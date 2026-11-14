"""Read/write orchestration for points and roads.

Concurrency model
-----------------
A batch is *planned* from a WAL read snapshot and the (potentially slow)
assembly runs with **no database lock at all**.  The plan is then committed in
one short ``BEGIN IMMEDIATE`` transaction that first re-checks the store
generation.  If any other batch committed in the meantime the plan is
discarded, nothing is written, and the batch is recomputed from a fresh
snapshot.

Because ``BEGIN IMMEDIATE`` excludes every other writer until COMMIT, whoever
commits second is guaranteed to have observed the first writer's rows.  So a
road can never be written back from stale coordinates, and a road created
concurrently can never be missed.  The recomputation is *bounded*: an
unchanged road is never rewritten, and all touched roads are recomputed in the
one transaction that changed their inputs.

Withdrawal reuses the same plan-then-verify flow.  On top of the generation
check, the committing transaction re-reads every recorded point's version
counter, so a concurrent correction/delete/re-create is refused (409) rather
than overwritten.  The version counters are maintained per point in every
point-mutating transaction; they advance even when a write restores the old
coordinate, which is what makes "changed back" and "deleted then re-created"
detectable.
"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Sequence

from .config import settings
from .db import read_tx, write_tx
from .geometry import assemble as _assemble
from .schemas import (
    BatchIn,
    BatchResult,
    PointIn,
    RoadOut,
    RollbackPointOut,
    RollbackRecordOut,
    WithdrawResult,
)

CHUNK = 400


class RetryPlanned(Exception):
    """Snapshot generation changed between planning and committing."""


class ConcurrentConflict(Exception):
    """Gave up after too many optimistic retries."""


class DemoFailure(Exception):
    """Injected failure used to demonstrate transactional rollback."""


class BatchNotWithdrawable(Exception):
    """No rollback record exists for that batch id (404)."""


class WithdrawConflict(Exception):
    """A point in the batch was corrected/deleted/recreated since (409)."""

    def __init__(self, batch_id: str, conflicts: list[dict]):
        self.batch_id = batch_id
        self.conflicts = conflicts
        super().__init__(f"batch {batch_id!r} is no longer withdrawable")


class DuplicateBatchId(Exception):
    """The caller-supplied batch id already identifies a recorded batch."""


def _chunks(ids: Iterable[str], size: int = CHUNK) -> Iterable[Sequence[str]]:
    buf: list[str] = []
    for i in ids:
        buf.append(i)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def _now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------


@dataclass
class Snapshot:
    generation: int
    old_points: dict[str, tuple[float, float]]
    old_refs: dict[str, list[str]]
    roads: dict[str, dict]
    coords: dict[str, tuple[float, float]]


@dataclass
class PlannedRoad:
    road_id: str
    point_ids: list[str]
    geometry: dict | None
    missing: list[str]
    existed: bool
    changed: bool


@dataclass
class RollbackEntry:
    """One point corrected by a withdrawable batch."""

    point_id: str
    old_lon: float
    old_lat: float


@dataclass
class Plan:
    generation: int
    point_writes: list[tuple[str, float, float]]
    point_deletes: list[str]
    road_deletes: list[str]
    batch_id: str
    roads: list[PlannedRoad] = field(default_factory=list)
    # Present only when the batch solely corrected the coordinates of points
    # that already existed: the record that makes the batch withdrawable.
    rollback: list[RollbackEntry] | None = None


def _take_snapshot(conn, batch: BatchIn) -> Snapshot:
    generation = int(
        conn.execute("SELECT generation FROM store_meta WHERE id = 1").fetchone()[0]
    )

    touched = {p.id for p in batch.points} | set(batch.delete_points)
    candidates = {r.id for r in batch.roads} | set(batch.delete_roads)

    old_points: dict[str, tuple[float, float]] = {}
    if touched:
        for chunk in _chunks(sorted(touched)):
            marks = ",".join("?" * len(chunk))
            for row in conn.execute(
                f"SELECT id, lon, lat FROM points WHERE id IN ({marks})", chunk
            ):
                old_points[row["id"]] = (row["lon"], row["lat"])

    if touched:
        for chunk in _chunks(sorted(touched)):
            marks = ",".join("?" * len(chunk))
            for row in conn.execute(
                f"SELECT DISTINCT road_id FROM road_points WHERE point_id IN ({marks})",
                chunk,
            ):
                candidates.add(row["road_id"])

    old_refs: dict[str, list[str]] = {rid: [] for rid in candidates}
    for chunk in _chunks(sorted(candidates)):
        marks = ",".join("?" * len(chunk))
        for row in conn.execute(
            f"SELECT road_id, point_id FROM road_points WHERE road_id IN ({marks}) "
            "ORDER BY road_id, seq",
            chunk,
        ):
            old_refs[row["road_id"]].append(row["point_id"])

    roads: dict[str, dict] = {}
    for chunk in _chunks(sorted(candidates)):
        marks = ",".join("?" * len(chunk))
        for row in conn.execute(
            f"SELECT id, geometry, missing_point_ids, revision, updated_at FROM roads "
            f"WHERE id IN ({marks})",
            chunk,
        ):
            roads[row["id"]] = {
                "geometry": json.loads(row["geometry"]) if row["geometry"] else None,
                "missing": json.loads(row["missing_point_ids"]),
                "revision": int(row["revision"]),
                "updated_at": row["updated_at"],
            }

    # Coordinates for every point an affected road may reference.  This maps
    # directly onto the post-batch point state.
    needed = set(touched)
    for refs in old_refs.values():
        needed.update(refs)
    for road in batch.roads:
        needed.update(road.point_ids)
    coords: dict[str, tuple[float, float]] = {}
    for chunk in _chunks(sorted(needed)):
        marks = ",".join("?" * len(chunk))
        for row in conn.execute(
            f"SELECT id, lon, lat FROM points WHERE id IN ({marks})", chunk
        ):
            coords[row["id"]] = (row["lon"], row["lat"])

    return Snapshot(
        generation=generation,
        old_points=old_points,
        old_refs=old_refs,
        roads=roads,
        coords=coords,
    )


def _build_plan(batch: BatchIn, snap: Snapshot) -> Plan:
    points = dict(snap.coords)
    for p in batch.points:
        points[p.id] = (p.lon, p.lat)
    for pid in batch.delete_points:
        points.pop(pid, None)

    refs: dict[str, list[str]] = {rid: list(v) for rid, v in snap.old_refs.items()}
    for road in batch.roads:
        refs[road.id] = list(road.point_ids)
    for rid in batch.delete_roads:
        refs.pop(rid, None)

    point_writes: list[tuple[str, float, float]] = []
    for p in batch.points:
        if snap.old_points.get(p.id) != (p.lon, p.lat):
            point_writes.append((p.id, p.lon, p.lat))
    point_deletes = [pid for pid in batch.delete_points if pid in snap.old_points]
    road_deletes = [rid for rid in batch.delete_roads if rid in snap.roads]

    # Only a batch that exclusively corrects the coordinates of points that
    # already existed is withdrawable.  Anything else (new points, any delete,
    # any road entry) changes structure that restoring coordinates alone could
    # not undo, so no rollback record is kept and the batch can never be
    # withdrawn.  Every point named by such a batch is recorded — including one
    # whose coordinate happened to be unchanged — so that a later edit to *any*
    # of them blocks the withdrawal (a replayed no-op is still an edit).
    rollback: list[RollbackEntry] | None = None
    only_corrections = (
        bool(batch.points)
        and not batch.roads
        and not batch.delete_points
        and not batch.delete_roads
        and all(p.id in snap.old_points for p in batch.points)
    )
    if only_corrections and point_writes:
        rollback = [
            RollbackEntry(p.id, *snap.old_points[p.id]) for p in batch.points
        ]

    planned: list[PlannedRoad] = []
    for rid in sorted(refs):
        road_refs = refs[rid]
        geometry, missing = _assemble(road_refs, points)
        meta = snap.roads.get(rid)
        existed = meta is not None
        if not existed:
            changed = True
        else:
            changed = (
                road_refs != snap.old_refs.get(rid, [])
                or geometry != meta["geometry"]
                or missing != meta["missing"]
            )
        planned.append(
            PlannedRoad(
                road_id=rid,
                point_ids=road_refs,
                geometry=geometry,
                missing=missing,
                existed=existed,
                changed=changed,
            )
        )

    return Plan(
        generation=snap.generation,
        point_writes=point_writes,
        point_deletes=point_deletes,
        road_deletes=road_deletes,
        # Caller-supplied id, or a placeholder the retry loop replaces with a
        # stable generated one.  Only recorded batches can be withdrawn.
        batch_id=batch.batch_id or uuid.uuid4().hex,
        roads=planned,
        rollback=rollback,
    )


# ---------------------------------------------------------------------------
# Commit
# ---------------------------------------------------------------------------


def write_refs(conn, road_id: str, current: list[str], wanted: list[str]) -> None:
    """Replace a road's reference list only where it actually differs."""
    if current == wanted:
        return
    conn.execute("DELETE FROM road_points WHERE road_id = ?", (road_id,))
    rows = [(road_id, seq, pid) for seq, pid in enumerate(wanted)]
    conn.executemany(
        "INSERT INTO road_points(road_id, seq, point_id) VALUES (?, ?, ?)", rows
    )


def _version_of(conn, point_id: str) -> int:
    row = conn.execute(
        "SELECT version FROM point_versions WHERE point_id = ?", (point_id,)
    ).fetchone()
    return int(row[0]) if row is not None else 0


def _apply(
    conn, plan: Plan, stamp: str, inject_failure: bool
) -> tuple[list[RoadOut], int]:
    written = [r for r in plan.roads if r.changed]

    for pid, lon, lat in plan.point_writes:
        conn.execute(
            "INSERT INTO points(id, lon, lat) VALUES (?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET lon = excluded.lon, lat = excluded.lat",
            (pid, lon, lat),
        )
    if plan.point_deletes:
        conn.executemany(
            "DELETE FROM points WHERE id = ?", [(pid,) for pid in plan.point_deletes]
        )
    # Version every point row mutation, even a re-supply that restores an
    # earlier coordinate, so a later withdrawal can prove that nobody else
    # has touched the point since (and that it was not deleted/recreated).
    for pid in [pid for pid, _, _ in plan.point_writes] + plan.point_deletes:
        conn.execute(
            "INSERT INTO point_versions(point_id, version) VALUES (?, 1) "
            "ON CONFLICT(point_id) DO UPDATE SET version = version + 1",
            (pid,),
        )
    if plan.rollback is not None:
        # Same transaction as the coordinates: the record and the values it
        # would restore can never disagree.
        versions = {
            pid: _version_of(conn, pid)
            for pid in (e.point_id for e in plan.rollback)
        }
        conn.execute(
            "INSERT INTO point_rollbacks(batch_id, created_at, withdrawn_at, points) "
            "VALUES (?, ?, NULL, ?)",
            (
                plan.batch_id,
                stamp,
                json.dumps(
                    [
                        {
                            "id": e.point_id,
                            "lon": e.old_lon,
                            "lat": e.old_lat,
                            "version": versions[e.point_id],
                        }
                        for e in plan.rollback
                    ],
                    separators=(",", ":"),
                ),
            ),
        )
    if plan.road_deletes:
        # road_points rows go with the road via ON DELETE CASCADE
        conn.executemany(
            "DELETE FROM roads WHERE id = ?", [(rid,) for rid in plan.road_deletes]
        )

    new_roads = 0
    out: list[RoadOut] = []
    for road in written:
        geom_text = (
            json.dumps(road.geometry, separators=(",", ":")) if road.geometry else None
        )
        missing_text = json.dumps(road.missing, separators=(",", ":"))
        if road.existed:
            revision = int(
                conn.execute(
                    "SELECT revision FROM roads WHERE id = ?", (road.road_id,)
                ).fetchone()[0]
            ) + 1
            conn.execute(
                "UPDATE roads SET geometry = ?, missing_point_ids = ?, revision = ?, "
                "updated_at = ? WHERE id = ?",
                (geom_text, missing_text, revision, stamp, road.road_id),
            )
            stored_refs = [
                r["point_id"]
                for r in conn.execute(
                    "SELECT point_id FROM road_points WHERE road_id = ? ORDER BY seq",
                    (road.road_id,),
                )
            ]
            write_refs(conn, road.road_id, stored_refs, road.point_ids)
        else:
            new_roads += 1
            revision = 1
            conn.execute(
                "INSERT INTO roads(id, geometry, missing_point_ids, revision, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (road.road_id, geom_text, missing_text, revision, stamp),
            )
            write_refs(conn, road.road_id, [], road.point_ids)
        out.append(
            RoadOut(
                id=road.road_id,
                point_ids=road.point_ids,
                status="ok" if road.geometry else "incomplete",
                geometry=road.geometry,
                missing_point_ids=road.missing,
                revision=revision,
                updated_at=stamp,
            )
        )

    if inject_failure:
        # Issued after every write, immediately before COMMIT: proves the
        # whole batch (points, references and geometry) is rolled back.
        raise DemoFailure("injected failure before commit; transaction rolled back")

    if written or plan.point_writes or plan.point_deletes or plan.road_deletes:
        # Bumping the generation invalidates any concurrent plan that was
        # computed from the pre-commit state, forcing it to recompute instead
        # of writing stale geometry back.
        conn.execute("UPDATE store_meta SET generation = generation + 1 WHERE id = 1")
    return out, new_roads


def execute_batch(
    batch: BatchIn, *, pause_ms: int = 0, inject_failure: bool = False
) -> tuple[BatchResult, int]:
    """Plan, verify and commit.  Returns (result, commit_attempts)."""
    max_attempts = max(1, settings.max_commit_attempts)
    # Stable across retries so a re-planned batch keeps the same identity.
    batch_id = batch.batch_id or uuid.uuid4().hex
    for attempt in range(1, max_attempts + 1):
        with read_tx() as conn:
            snap = _take_snapshot(conn, batch)

        if attempt == 1 and pause_ms > 0:
            # Demo hook: the pause happens after the snapshot is released and
            # before any write lock is taken.  Other batches commit freely.
            time.sleep(pause_ms / 1000.0)

        plan = _build_plan(batch, snap)
        plan.batch_id = batch_id
        stamp = _now()
        try:
            with write_tx() as conn:
                current = int(
                    conn.execute(
                        "SELECT generation FROM store_meta WHERE id = 1"
                    ).fetchone()[0]
                )
                if current != plan.generation:
                    raise RetryPlanned()
                if batch.batch_id is not None:
                    # An explicit id must not shadow an existing rollback
                    # record, or a later withdrawal could hit the wrong batch.
                    taken = conn.execute(
                        "SELECT 1 FROM point_rollbacks WHERE batch_id = ?",
                        (batch.batch_id,),
                    ).fetchone()
                    if taken is not None:
                        raise DuplicateBatchId(batch.batch_id)
                roads_out, new_roads = _apply(conn, plan, stamp, inject_failure)
        except RetryPlanned:
            # small jittered backoff before replanning from a fresh snapshot
            time.sleep(min(0.02 * attempt, 0.2))
            continue

        result = BatchResult(
            batch_id=plan.batch_id,
            withdrawable=plan.rollback is not None,
            points_upserted=len(plan.point_writes),
            roads_upserted=new_roads,
            roads_updated=len([r for r in plan.roads if r.changed and r.existed]),
            points_deleted=len(plan.point_deletes),
            roads_deleted=len(plan.road_deletes),
            affected_road_ids=sorted(r.road_id for r in plan.roads if r.changed),
            roads=sorted(roads_out, key=lambda r: r.id),
            commit_attempts=attempt,
        )
        return result, attempt
    raise ConcurrentConflict(
        f"could not commit after {max_attempts} attempts due to concurrent writes"
    )


# ---------------------------------------------------------------------------
# Withdrawal of point-coordinate corrections
# ---------------------------------------------------------------------------


def _load_rollback(conn, batch_id: str):
    return conn.execute(
        "SELECT batch_id, created_at, withdrawn_at, affected_road_ids, points "
        "FROM point_rollbacks WHERE batch_id = ?",
        (batch_id,),
    ).fetchone()


def _conflicts(conn, entries: list[dict]) -> list[dict]:
    """Points that no longer stand exactly as the batch left them.

    A point matches only when it still exists *and* its version counter equals
    the value captured at commit time.  The counter is bumped by every point
    write or delete, including a later correction that writes the original
    coordinates back and a delete followed by a re-create, so those all
    conflict even though the coordinates may look unchanged.
    """
    conflicts: list[dict] = []
    for entry in entries:
        pid = entry["id"]
        row = conn.execute(
            "SELECT lon, lat FROM points WHERE id = ?", (pid,)
        ).fetchone()
        version = _version_of(conn, pid)
        if row is None:
            conflicts.append({"point_id": pid, "reason": "deleted_or_missing"})
        elif entry["version"] != version:
            conflicts.append(
                {
                    "point_id": pid,
                    "reason": "modified_since_batch",
                    "current_lon": row["lon"],
                    "current_lat": row["lat"],
                }
            )
    return conflicts


def _withdraw_batch(
    conn, snap: Snapshot, plan: Plan, batch_id: str, stamp: str, inject_failure: bool
) -> list[RoadOut]:
    """Apply the restore inside the caller's write transaction.

    ``plan`` was built from the post-batch snapshot but with the recorded
    originals as the new point values, so exactly the roads that reference a
    restored point at commit time are recomputed.  Roads created or repointed
    by other batches since the correction are therefore brought up to date.
    The withdrawal record is flipped in the same transaction as the
    coordinates, the references and the geometry.
    """
    plan.rollback = None  # a withdrawal never records a new rollback
    roads_out, _ = _apply(conn, plan, stamp, inject_failure)
    by_id = {r.id: r for r in roads_out}
    # Roads that were examined and came out byte-identical keep their stored
    # revision/timestamp; the snapshot still holds those persisted values.
    for road in plan.roads:
        if road.road_id in by_id:
            continue
        meta = snap.roads.get(road.road_id)
        by_id[road.road_id] = RoadOut(
            id=road.road_id,
            point_ids=road.point_ids,
            status="ok" if road.geometry else "incomplete",
            geometry=road.geometry,
            missing_point_ids=road.missing,
            revision=meta["revision"] if meta else 0,
            updated_at=meta["updated_at"] if meta else stamp,
        )
    out = [by_id[road.road_id] for road in plan.roads]
    affected = sorted(r.road_id for r in plan.roads)
    conn.execute(
        "UPDATE point_rollbacks SET withdrawn_at = ?, affected_road_ids = ? "
        "WHERE batch_id = ?",
        (stamp, json.dumps(affected, separators=(",", ":")), batch_id),
    )
    return out


def _rollback_roads(conn, ids: Sequence[str]) -> list[RoadOut]:
    if not ids:
        return []
    out: list[RoadOut] = []
    for chunk in _chunks(ids):
        marks = ",".join("?" * len(chunk))
        for row in conn.execute(
            f"SELECT id FROM roads WHERE id IN ({marks})", chunk
        ).fetchall():
            out.extend(list_roads(conn, road_id=row["id"]))
    return out


def withdraw_batch(
    batch_id: str, *, inject_failure: bool = False
) -> tuple[WithdrawResult, int]:
    """Restore the original coordinates recorded for ``batch_id``.

    * 404 (:class:`BatchNotWithdrawable`) when no rollback record exists.
    * 409 (:class:`WithdrawConflict`) when any recorded point has been
      modified, deleted or re-created since — nothing is written then.
    * Repeating a successful withdrawal returns the recorded result and writes
      nothing (the coordinates are not restored twice, revisions do not move).
    """
    max_attempts = max(1, settings.max_commit_attempts)
    for attempt in range(1, max_attempts + 1):
        with read_tx() as conn:
            rec = _load_rollback(conn, batch_id)
            if rec is None:
                raise BatchNotWithdrawable(batch_id)
            if rec["withdrawn_at"] is not None:
                ids = json.loads(rec["affected_road_ids"] or "[]")
                roads = _rollback_roads(conn, ids)
                return (
                    WithdrawResult(
                        batch_id=batch_id,
                        withdrawn_at=rec["withdrawn_at"],
                        points_restored=0,
                        affected_road_ids=ids,
                        roads=sorted(roads, key=lambda r: r.id),
                        already_withdrawn=True,
                        commit_attempts=1,
                    ),
                    1,
                )
            entries = json.loads(rec["points"])
            conflicts = _conflicts(conn, entries)
            if conflicts:
                raise WithdrawConflict(batch_id, conflicts)
            batch = BatchIn(
                points=[
                    PointIn(id=e["id"], lon=e["lon"], lat=e["lat"]) for e in entries
                ]
            )
            snap = _take_snapshot(conn, batch)

        plan = _build_plan(batch, snap)
        plan.rollback = None
        stamp = _now()
        try:
            with write_tx() as conn:
                if get_generation(conn) != plan.generation:
                    raise RetryPlanned()
                fresh = _load_rollback(conn, batch_id)
                if fresh is None:  # pragma: no cover - rows are never deleted
                    raise BatchNotWithdrawable(batch_id)
                if fresh["withdrawn_at"] is not None:
                    # Another process withdrew it while we were planning.
                    raise RetryPlanned()
                conflicts = _conflicts(conn, json.loads(fresh["points"]))
                if conflicts:
                    raise WithdrawConflict(batch_id, conflicts)
                roads_out = _withdraw_batch(
                    conn, snap, plan, batch_id, stamp, inject_failure
                )
        except RetryPlanned:
            time.sleep(min(0.02 * attempt, 0.2))
            continue

        return (
            WithdrawResult(
                batch_id=batch_id,
                withdrawn_at=stamp,
                # Only points whose stored coordinate actually differed are
                # rewritten; a recorded-but-unchanged point is a no-op.
                points_restored=len(plan.point_writes),
                affected_road_ids=sorted(r.id for r in roads_out),
                roads=sorted(roads_out, key=lambda r: r.id),
                already_withdrawn=False,
                commit_attempts=attempt,
            ),
            attempt,
        )
    raise ConcurrentConflict(
        f"could not commit after {max_attempts} attempts due to concurrent writes"
    )


def get_rollback(conn, batch_id: str) -> RollbackRecordOut | None:
    rec = _load_rollback(conn, batch_id)
    if rec is None:
        return None
    points: list[RollbackPointOut] = []
    already = rec["withdrawn_at"] is not None
    for entry in json.loads(rec["points"]):
        row = conn.execute(
            "SELECT lon, lat FROM points WHERE id = ?", (entry["id"],)
        ).fetchone()
        ok = (
            not already
            and row is not None
            and _version_of(conn, entry["id"]) == entry["version"]
        )
        points.append(
            RollbackPointOut(
                id=entry["id"],
                lon=entry["lon"],
                lat=entry["lat"],
                current_lon=None if row is None else row["lon"],
                current_lat=None if row is None else row["lat"],
                withdrawable=ok,
            )
        )
    return RollbackRecordOut(
        batch_id=rec["batch_id"],
        created_at=rec["created_at"],
        withdrawn_at=rec["withdrawn_at"],
        affected_road_ids=json.loads(rec["affected_road_ids"] or "[]"),
        points=points,
    )


def get_generation(conn) -> int:
    return int(conn.execute("SELECT generation FROM store_meta WHERE id = 1").fetchone()[0])


def list_points(conn) -> list[PointIn]:
    return [
        PointIn(id=r["id"], lon=r["lon"], lat=r["lat"])
        for r in conn.execute("SELECT id, lon, lat FROM points ORDER BY id")
    ]


def get_point(conn, point_id: str) -> PointIn | None:
    row = conn.execute(
        "SELECT id, lon, lat FROM points WHERE id = ?", (point_id,)
    ).fetchone()
    return None if row is None else PointIn(id=row["id"], lon=row["lon"], lat=row["lat"])


def list_roads(conn, road_id: str | None = None) -> list[RoadOut]:
    if road_id is None:
        rows = conn.execute(
            "SELECT id, geometry, missing_point_ids, revision, updated_at FROM roads "
            "ORDER BY id"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, geometry, missing_point_ids, revision, updated_at FROM roads "
            "WHERE id = ?",
            (road_id,),
        ).fetchall()
    if not rows:
        return []
    refs_by_road: dict[str, list[str]] = {}
    for chunk in _chunks([r["id"] for r in rows]):
        marks = ",".join("?" * len(chunk))
        for row in conn.execute(
            f"SELECT road_id, point_id FROM road_points WHERE road_id IN ({marks}) "
            "ORDER BY road_id, seq",
            chunk,
        ):
            refs_by_road.setdefault(row["road_id"], []).append(row["point_id"])
    out: list[RoadOut] = []
    for row in rows:
        geometry = json.loads(row["geometry"]) if row["geometry"] else None
        out.append(
            RoadOut(
                id=row["id"],
                point_ids=refs_by_road.get(row["id"], []),
                status="ok" if geometry else "incomplete",
                geometry=geometry,
                missing_point_ids=json.loads(row["missing_point_ids"]),
                revision=row["revision"],
                updated_at=row["updated_at"],
            )
        )
    return out
