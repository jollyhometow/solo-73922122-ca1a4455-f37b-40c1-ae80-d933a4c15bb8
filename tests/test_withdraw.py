"""Withdrawal of point-coordinate correction batches."""
from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.main import app

PTS = [
    {"id": "A", "lon": 1.0, "lat": 1.0},
    {"id": "B", "lon": 2.0, "lat": 2.0},
    {"id": "C", "lon": 3.0, "lat": 3.0},
]

ROADS = [
    {"id": "R1", "point_ids": ["A", "B"]},
    {"id": "R2", "point_ids": ["B", "C"]},
]


@pytest.fixture()
def client(tmp_path, monkeypatch):
    from app import config

    monkeypatch.setattr(config.settings, "db_path", str(tmp_path / "gis.db"))
    with TestClient(app) as c:
        yield c


def post(client, body, **params):
    return client.post("/api/v1/batches", json=body, params=params)


def setup(client):
    r = post(client, {"points": PTS, "roads": ROADS})
    assert r.status_code == 201, r.text


def withdraw(client, batch_id, **params):
    return client.post(f"/api/v1/batches/{batch_id}/withdraw", params=params)


def state(client):
    return client.get("/api/v1/state").json()


def point_map(s):
    return {p["id"]: [p["lon"], p["lat"]] for p in s["points"]}


# ---------------------------------------------------------------------------
# Recording
# ---------------------------------------------------------------------------


def test_correction_batch_is_recorded_and_withdrawable(client):
    setup(client)
    r = post(client, {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}]})
    body = r.json()
    assert body["withdrawable"] is True
    assert body["batch_id"]

    record = client.get(f"/api/v1/batches/{body['batch_id']}/rollback").json()
    assert record["withdrawn_at"] is None
    assert record["points"] == [
        {
            "id": "B",
            "lon": 2.0,
            "lat": 2.0,
            "current_lon": 9.0,
            "current_lat": 9.0,
            "withdrawable": True,
        }
    ]


@pytest.mark.parametrize(
    "body",
    [
        # creates a new point
        {"points": [{"id": "NEW", "lon": 5.0, "lat": 5.0}]},
        # mixes a correction with a new point
        {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}, {"id": "NEW", "lon": 5, "lat": 5}]},
        # deletes a point
        {"delete_points": ["C"]},
        # deletes a road
        {"delete_roads": ["R1"]},
        # new / repointed road alongside a correction
        {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}], "roads": [{"id": "R3", "point_ids": ["A", "C"]}]},
        # no actual coordinate change
        {"points": [{"id": "B", "lon": 2.0, "lat": 2.0}]},
    ],
)
def test_non_pure_correction_batches_are_not_withdrawable(client, body):
    setup(client)
    r = post(client, body)
    assert r.status_code == 201, r.text
    assert r.json()["withdrawable"] is False
    assert withdraw(client, r.json()["batch_id"]).status_code == 404


def test_unknown_batch_id_is_404(client):
    setup(client)
    assert withdraw(client, "no-such-batch").status_code == 404
    assert client.get("/api/v1/batches/no-such-batch/rollback").status_code == 404


def test_caller_supplied_batch_id(client):
    setup(client)
    r = post(
        client,
        {"batch_id": "fix-2026-11-14", "points": [{"id": "B", "lon": 9.0, "lat": 9.0}]},
    )
    assert r.json()["batch_id"] == "fix-2026-11-14"
    assert withdraw(client, "fix-2026-11-14").status_code == 200


def test_duplicate_caller_batch_id_rejected(client):
    setup(client)
    assert post(client, {"batch_id": "dup", "points": [{"id": "B", "lon": 9.0, "lat": 9.0}]}).status_code == 201
    r = post(client, {"batch_id": "dup", "points": [{"id": "B", "lon": 8.0, "lat": 8.0}]})
    assert r.status_code == 409
    assert client.get("/api/v1/points/B").json()["lon"] == 9.0


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_withdraw_restores_coordinates_and_recomputes_roads(client):
    setup(client)
    before = state(client)
    bid = post(client, {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}]}).json()["batch_id"]

    r = withdraw(client, bid)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["already_withdrawn"] is False
    assert body["points_restored"] == 1
    assert body["affected_road_ids"] == ["R1", "R2"]
    for road in body["roads"]:
        assert [2.0, 2.0] in road["geometry"]["coordinates"]

    after = state(client)
    assert point_map(after) == point_map(before)
    for road in after["roads"]:
        coords = point_map(after)
        for pid, pos in zip(road["point_ids"], road["geometry"]["coordinates"]):
            assert pos == coords[pid]

    # record reflects the withdrawal
    record = client.get(f"/api/v1/batches/{bid}/rollback").json()
    assert record["withdrawn_at"] == body["withdrawn_at"]
    assert record["affected_road_ids"] == ["R1", "R2"]


def test_withdraw_persists_geometry_in_the_same_transaction(client, tmp_path):
    setup(client)
    bid = post(client, {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}]}).json()["batch_id"]
    withdraw(client, bid)
    conn = sqlite3.connect(tmp_path / "gis.db")
    geom, withdrawn = conn.execute(
        "SELECT r.geometry, p.withdrawn_at FROM roads r, point_rollbacks p "
        "WHERE r.id = 'R1'"
    ).fetchone()
    conn.close()
    assert "2.0" in geom
    assert withdrawn is not None


def test_withdraw_reports_all_roads_that_reference_the_points_now(client):
    setup(client)
    bid = post(client, {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}]}).json()["batch_id"]
    # Later batches add a road referencing B and repoint an existing one.
    post(client, {"roads": [{"id": "R3", "point_ids": ["B", "A"]}]})
    post(client, {"roads": [{"id": "R1", "point_ids": ["C", "B"]}]})

    body = withdraw(client, bid).json()
    assert body["affected_road_ids"] == ["R1", "R2", "R3"]
    after = state(client)
    coords = point_map(after)
    for road in after["roads"]:
        for pid, pos in zip(road["point_ids"], road["geometry"]["coordinates"]):
            assert pos == coords[pid]
        idx = road["point_ids"].index("B")
        assert road["geometry"]["coordinates"][idx] == [2.0, 2.0]


def test_withdraw_does_not_touch_roads_that_no_longer_reference_the_point(client):
    setup(client)
    bid = post(client, {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}]}).json()["batch_id"]
    # R1 no longer references B at withdrawal time.
    post(client, {"roads": [{"id": "R1", "point_ids": ["A", "C"]}]})
    r1_before = client.get("/api/v1/roads/R1").json()

    body = withdraw(client, bid).json()
    assert body["affected_road_ids"] == ["R2"]
    r1_after = client.get("/api/v1/roads/R1").json()
    assert r1_after == r1_before, "R1 does not reference B and must not be rewritten"
    # R2 does reference B, so its persisted line follows the restored value.
    assert client.get("/api/v1/roads/R2").json()["geometry"]["coordinates"][0] == [2.0, 2.0]


def test_withdraw_handles_missing_referenced_point(client):
    setup(client)
    bid = post(client, {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}]}).json()["batch_id"]
    # An unrelated batch deletes C: R2 becomes incomplete, R1 unaffected.
    post(client, {"delete_points": ["C"]})
    body = withdraw(client, bid).json()
    roads = {r["id"]: r for r in body["roads"]}
    assert roads["R1"]["status"] == "ok"
    assert roads["R2"]["status"] == "incomplete"
    assert roads["R2"]["missing_point_ids"] == ["C"]
    # Restoring C later brings R2 back without another withdrawal.
    post(client, {"points": [PTS[2]]})
    r2 = client.get("/api/v1/roads/R2").json()
    assert r2["status"] == "ok"
    assert r2["geometry"]["coordinates"] == [[2.0, 2.0], [3.0, 3.0]]


def test_multiple_points_withdrawn_together(client):
    setup(client)
    bid = post(
        client,
        {"points": [{"id": "A", "lon": 7.0, "lat": 7.0}, {"id": "C", "lon": 8.0, "lat": 8.0}]},
    ).json()["batch_id"]
    body = withdraw(client, bid).json()
    assert body["points_restored"] == 2
    assert sorted(body["affected_road_ids"]) == ["R1", "R2"]
    coords = point_map(state(client))
    assert coords["A"] == [1.0, 1.0]
    assert coords["C"] == [3.0, 3.0]


def test_unchanged_stated_point_is_still_guarded(client):
    """Every point named by a withdrawable batch is protected by the 409 check."""
    setup(client)
    bid = post(
        client,
        {"points": [{"id": "A", "lon": 7.0, "lat": 7.0}, {"id": "B", "lon": 2.0, "lat": 2.0}]},
    ).json()["batch_id"]
    record = client.get(f"/api/v1/batches/{bid}/rollback").json()
    assert {p["id"] for p in record["points"]} == {"A", "B"}
    # B was stated with an unchanged coordinate, but a later edit to it must
    # still refuse the whole batch rather than silently keeping that edit.
    post(client, {"points": [{"id": "B", "lon": 2.5, "lat": 2.5}]})
    before = state(client)
    r = withdraw(client, bid)
    assert r.status_code == 409
    assert {c["point_id"] for c in r.json()["conflicts"]} == {"B"}
    assert state(client) == before


def test_unchanged_stated_point_is_not_rewritten(client):
    """Only points whose value actually differs are restored / counted."""
    setup(client)
    bid = post(
        client,
        {"points": [{"id": "A", "lon": 7.0, "lat": 7.0}, {"id": "B", "lon": 2.0, "lat": 2.0}]},
    ).json()["batch_id"]
    body = withdraw(client, bid).json()
    assert body["points_restored"] == 1  # A only; B already held its old value
    assert client.get("/api/v1/points/B").json()["lon"] == 2.0


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_repeated_withdrawal_does_not_modify_again(client):
    setup(client)
    bid = post(client, {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}]}).json()["batch_id"]

    first = withdraw(client, bid).json()
    snap = state(client)
    second = withdraw(client, bid)
    assert second.status_code == 200
    assert second.json()["already_withdrawn"] is True
    assert second.json()["points_restored"] == 0
    assert second.json()["withdrawn_at"] == first["withdrawn_at"]
    assert second.json()["affected_road_ids"] == first["affected_road_ids"]
    assert state(client) == snap  # generation and revisions unchanged


def test_withdrawal_after_a_later_correction_is_rejected(client):
    setup(client)
    bid = post(client, {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}]}).json()["batch_id"]
    withdraw(client, bid)
    # Somebody moves B again after the withdrawal; replay must not clobber it.
    post(client, {"points": [{"id": "B", "lon": 4.0, "lat": 4.0}]})
    snap = state(client)
    r = withdraw(client, bid)
    assert r.status_code == 200  # idempotent replay, not a restore
    assert r.json()["already_withdrawn"] is True
    assert state(client) == snap


# ---------------------------------------------------------------------------
# Conflicts
# ---------------------------------------------------------------------------


def _conflict_after(client, mutate):
    setup(client)
    bid = post(client, {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}]}).json()["batch_id"]
    mutate(client)
    before = state(client)
    r = withdraw(client, bid)
    assert r.status_code == 409, r.text
    assert r.json()["batch_id"] == bid
    assert r.json()["conflicts"], r.json()
    assert state(client) == before, "409 must not modify any data"
    record = client.get(f"/api/v1/batches/{bid}/rollback").json()
    assert record["withdrawn_at"] is None
    return r


def test_conflict_when_point_corrected_again(client):
    r = _conflict_after(
        client, lambda c: post(c, {"points": [{"id": "B", "lon": 5.0, "lat": 5.0}]})
    )
    assert r.json()["conflicts"][0]["point_id"] == "B"
    assert r.json()["conflicts"][0]["reason"] == "modified_since_batch"


def test_conflict_when_coordinate_was_changed_and_changed_back(client):
    def mutate(c):
        post(c, {"points": [{"id": "B", "lon": 6.0, "lat": 6.0}]})
        post(c, {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}]})

    _conflict_after(client, mutate)


def test_conflict_when_point_deleted(client):
    r = _conflict_after(client, lambda c: post(c, {"delete_points": ["B"]}))
    assert r.json()["conflicts"][0]["reason"] == "deleted_or_missing"


def test_conflict_when_point_deleted_then_recreated_with_same_coordinates(client):
    def mutate(c):
        post(c, {"delete_points": ["B"]})
        post(c, {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}]})

    _conflict_after(client, mutate)


def test_one_conflicting_point_rejects_the_whole_batch(client):
    setup(client)
    bid = post(
        client,
        {"points": [{"id": "A", "lon": 7.0, "lat": 7.0}, {"id": "B", "lon": 9.0, "lat": 9.0}]},
    ).json()["batch_id"]
    post(client, {"points": [{"id": "B", "lon": 5.0, "lat": 5.0}]})
    before = state(client)
    r = withdraw(client, bid)
    assert r.status_code == 409
    conflicts = {c["point_id"] for c in r.json()["conflicts"]}
    assert conflicts == {"B"}
    # A is still at its corrected value - the batch was not partially applied.
    assert state(client) == before
    assert point_map(before)["A"] == [7.0, 7.0]


def test_other_batches_touching_unrecorded_points_do_not_block(client):
    setup(client)
    bid = post(client, {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}]}).json()["batch_id"]
    post(client, {"points": [{"id": "A", "lon": 4.0, "lat": 4.0}]})
    post(client, {"delete_points": ["C"]})
    r = withdraw(client, bid)
    assert r.status_code == 200
    assert point_map(state(client))["A"] == [4.0, 4.0]


# ---------------------------------------------------------------------------
# Atomicity
# ---------------------------------------------------------------------------


def test_failed_withdrawal_leaves_everything_untouched(client):
    setup(client)
    bid = post(client, {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}]}).json()["batch_id"]
    before = state(client)
    r = withdraw(client, bid, fail=True)
    assert r.status_code == 500
    assert state(client) == before
    record = client.get(f"/api/v1/batches/{bid}/rollback").json()
    assert record["withdrawn_at"] is None
    assert record["points"][0]["current_lon"] == 9.0
    # A later normal withdrawal still works.
    assert withdraw(client, bid).status_code == 200
    assert point_map(state(client))["B"] == [2.0, 2.0]


def test_failed_withdrawal_after_a_later_edit_does_not_disturb_that_edit(client):
    setup(client)
    bid = post(client, {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}]}).json()["batch_id"]
    post(client, {"points": [{"id": "B", "lon": 5.0, "lat": 5.0}]})
    before = state(client)
    assert withdraw(client, bid, fail=True).status_code == 409
    assert state(client) == before


# ---------------------------------------------------------------------------
# State queries unchanged
# ---------------------------------------------------------------------------


def test_state_after_withdrawal_is_one_consistent_snapshot(client):
    setup(client)
    bid = post(client, {"points": [{"id": "B", "lon": 9.0, "lat": 9.0}]}).json()["batch_id"]
    withdraw(client, bid)
    snap = state(client)
    coords = point_map(snap)
    for road in snap["roads"]:
        for pid, pos in zip(road["point_ids"], road["geometry"]["coordinates"]):
            assert pos == coords[pid]
