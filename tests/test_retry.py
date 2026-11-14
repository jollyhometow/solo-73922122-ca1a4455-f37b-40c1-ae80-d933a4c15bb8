"""Deterministic test of the optimistic commit guard.

Simulates "another API process committed while this batch was assembling" by
bumping the store generation from a second connection between the snapshot and
the commit.  The stale plan must be discarded, not written.
"""
from __future__ import annotations

import json
import sqlite3

import pytest

from app import config, store
from app.db import init_db
from app.schemas import BatchIn


@pytest.fixture()
def db(tmp_path, monkeypatch):
    path = str(tmp_path / "gis.db")
    monkeypatch.setattr(config.settings, "db_path", path)
    init_db()
    return path


def _batch():
    return BatchIn.model_validate(
        {
            "points": [
                {"id": "A", "lon": 1.0, "lat": 1.0},
                {"id": "B", "lon": 2.0, "lat": 2.0},
            ],
            "roads": [{"id": "R", "point_ids": ["A", "B"]}],
        }
    )


def test_stale_plan_is_recomputed_not_written(db, monkeypatch):
    # seed a road that references A (which will move concurrently)
    store.execute_batch(_batch())

    real_build = store._build_plan
    state = {"calls": 0}

    def build_with_concurrent_write(batch, snap):
        state["calls"] += 1
        if state["calls"] == 1:
            # another process commits A = (9,9) while we are still planning
            conn = sqlite3.connect(db, isolation_level=None)
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE points SET lon = 9, lat = 9 WHERE id = 'A'")
            conn.execute("UPDATE store_meta SET generation = generation + 1")
            conn.execute("COMMIT")
            conn.close()
        return real_build(batch, snap)

    monkeypatch.setattr(store, "_build_plan", build_with_concurrent_write)

    result, attempts = store.execute_batch(
        BatchIn.model_validate({"roads": [{"id": "R", "point_ids": ["A", "B"]}]})
    )
    assert attempts >= 2, "stale plan was not retried"
    # the committed geometry must use the OTHER process's coordinate, not the
    # coordinate this batch saw in its first snapshot
    assert result.roads[0].geometry["coordinates"][0] == [9.0, 9.0]
    assert result.roads[0].point_ids == ["A", "B"]


def test_failed_commit_does_not_bump_generation(db):
    conn = sqlite3.connect(db, isolation_level=None)
    before = conn.execute("SELECT generation FROM store_meta").fetchone()[0]
    conn.close()

    with pytest.raises(store.DemoFailure):
        store.execute_batch(
            BatchIn.model_validate({"points": [{"id": "C", "lon": 3, "lat": 3}]}),
            inject_failure=True,
        )

    conn = sqlite3.connect(db, isolation_level=None)
    after = conn.execute("SELECT generation FROM store_meta").fetchone()[0]
    has_c = conn.execute("SELECT count(*) FROM points WHERE id='C'").fetchone()[0]
    conn.close()
    assert after == before
    assert has_c == 0


def _correction(db):
    """Seed points and a road, then commit a correction batch; return its id."""
    store.execute_batch(
        BatchIn.model_validate(
            {
                "points": [
                    {"id": "A", "lon": 1.0, "lat": 1.0},
                    {"id": "B", "lon": 2.0, "lat": 2.0},
                    {"id": "C", "lon": 3.0, "lat": 3.0},
                ],
                "roads": [{"id": "R", "point_ids": ["A", "B"]}],
            }
        )
    )
    result, _ = store.execute_batch(
        BatchIn.model_validate({"points": [{"id": "B", "lon": 9.0, "lat": 9.0}]})
    )
    assert result.withdrawable
    return result.batch_id


def test_withdrawal_replans_around_a_road_created_meanwhile(db, monkeypatch):
    """A road committed between withdrawal planning and commit is picked up."""
    batch_id = _correction(db)

    real_build = store._build_plan
    done = {"flag": False}

    def build_with_concurrent_road(batch, snap):
        if not done["flag"]:
            done["flag"] = True
            conn = sqlite3.connect(db, isolation_level=None)
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO roads(id, geometry, missing_point_ids, revision, updated_at) "
                "VALUES ('R2', NULL, '[]', 1, 'x')"
            )
            conn.execute(
                "INSERT INTO road_points(road_id, seq, point_id) VALUES ('R2', 0, 'B')"
            )
            conn.execute(
                "INSERT INTO road_points(road_id, seq, point_id) VALUES ('R2', 1, 'C')"
            )
            conn.execute("UPDATE store_meta SET generation = generation + 1")
            conn.execute("COMMIT")
            conn.close()
        return real_build(batch, snap)

    monkeypatch.setattr(store, "_build_plan", build_with_concurrent_road)
    result, attempts = store.withdraw_batch(batch_id)
    assert attempts >= 2, "stale withdrawal plan was not retried"
    # The road created while planning is recomputed with the restored value.
    assert "R2" in result.affected_road_ids
    r2 = next(r for r in result.roads if r.id == "R2")
    assert r2.geometry["coordinates"][0] == [2.0, 2.0]
    assert result.points_restored == 1


def test_withdrawal_rejects_a_coordinate_restored_meanwhile(db, monkeypatch):
    """Another process writing the original value back still bumps the version."""
    batch_id = _correction(db)

    real_build = store._build_plan
    done = {"flag": False}

    def build_with_concurrent_write_back(batch, snap):
        if not done["flag"]:
            done["flag"] = True
            # Another process submits a real correction batch that writes the
            # very same coordinates the withdrawal wants to restore.
            store.execute_batch(
                BatchIn.model_validate(
                    {"points": [{"id": "B", "lon": 2.0, "lat": 2.0}]}
                )
            )
        return real_build(batch, snap)

    monkeypatch.setattr(store, "_build_plan", build_with_concurrent_write_back)
    with pytest.raises(store.WithdrawConflict) as exc:
        store.withdraw_batch(batch_id)
    assert exc.value.conflicts[0]["point_id"] == "B"
    # Nothing was restored: the other process's value stands.
    with store.read_tx() as conn:
        row = conn.execute("SELECT lon, lat FROM points WHERE id='B'").fetchone()
    assert (row[0], row[1]) == (2.0, 2.0)


def test_withdraw_bumps_generation_and_keeps_record_consistent(db):
    batch_id = _correction(db)
    with store.read_tx() as conn:
        before = store.get_generation(conn)
    result, _ = store.withdraw_batch(batch_id)
    with store.read_tx() as conn:
        after = store.get_generation(conn)
        rec = store._load_rollback(conn, batch_id)
    assert after > before
    assert rec["withdrawn_at"] == result.withdrawn_at
    assert json.loads(rec["affected_road_ids"]) == result.affected_road_ids


def test_legacy_database_is_migrated(tmp_path, monkeypatch):
    """A DB created before rollback support gains the tables on startup."""
    path = str(tmp_path / "legacy.db")
    monkeypatch.setattr(config.settings, "db_path", path)
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE points (id TEXT PRIMARY KEY, lon REAL NOT NULL, lat REAL NOT NULL);
        CREATE TABLE roads (id TEXT PRIMARY KEY, geometry TEXT,
            missing_point_ids TEXT NOT NULL DEFAULT '[]',
            revision INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL);
        CREATE TABLE road_points (road_id TEXT NOT NULL, seq INTEGER NOT NULL,
            point_id TEXT NOT NULL, PRIMARY KEY (road_id, seq));
        CREATE TABLE store_meta (id INTEGER PRIMARY KEY CHECK (id = 1),
            generation INTEGER NOT NULL);
        INSERT INTO store_meta VALUES (1, 1);
        INSERT INTO points VALUES ('L1', 10.0, 20.0);
        """
    )
    conn.commit()
    conn.close()

    init_db()
    with store.read_tx() as c:
        assert store.get_generation(c) == 1
        row = c.execute(
            "SELECT version FROM point_versions WHERE point_id='L1'"
        ).fetchone()
        assert row is not None and row[0] == 1
        assert c.execute("SELECT count(*) FROM point_rollbacks").fetchone()[0] == 0

    # A correction to the pre-existing point is now withdrawable.
    result, _ = store.execute_batch(
        BatchIn.model_validate({"points": [{"id": "L1", "lon": 11.0, "lat": 21.0}]})
    )
    assert result.withdrawable
    store.withdraw_batch(result.batch_id)
    with store.read_tx() as c:
        assert store.get_point(c, "L1").lon == 10.0
