"""Black-box tests against TWO real API processes sharing one SQLite file."""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager

import httpx
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@contextmanager
def two_apis(db_path):
    procs = []
    ports = []
    env = dict(os.environ, GIS_DB_PATH=str(db_path), GIS_BUSY_TIMEOUT_S="30")
    stop_file = str(db_path) + ".stop"
    try:
        for _ in range(2):
            port = _free_port()
            ports.append(port)
            procs.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        os.path.join(os.path.dirname(__file__), "server_runner.py"),
                        str(port),
                        stop_file,
                    ],
                    cwd=ROOT,
                    env=env,
                )
            )
        bases = [f"http://127.0.0.1:{p}" for p in ports]
        deadline = time.time() + 30
        for base in bases:
            while True:
                try:
                    if httpx.get(base + "/health", timeout=1).status_code == 200:
                        break
                except Exception:
                    if time.time() > deadline:
                        raise RuntimeError(f"{base} did not start")
                    time.sleep(0.1)
        yield bases
    finally:
        open(stop_file, "w").close()
        for p in procs:
            try:
                p.wait(timeout=15)
            except subprocess.TimeoutExpired:  # pragma: no cover
                p.kill()
                p.wait(timeout=5)


@pytest.fixture(scope="module")
def apis(tmp_path_factory):
    db = tmp_path_factory.mktemp("shared") / "gis.db"
    with two_apis(db) as bases:
        yield bases


PTS = [
    {"id": "P1", "lon": 116.100, "lat": 39.800},
    {"id": "P2", "lon": 116.200, "lat": 39.850},
    {"id": "P3", "lon": 116.300, "lat": 39.900},
]


def test_both_processes_share_the_same_database(apis):
    a, b = apis
    r = httpx.post(
        a + "/api/v1/batches",
        json={"points": PTS, "roads": [{"id": "R_SHARED", "point_ids": ["P1", "P2", "P3"]}]},
        timeout=10,
    )
    assert r.status_code == 201, r.text
    got = httpx.get(b + "/api/v1/roads/R_SHARED", timeout=10).json()
    assert got["geometry"]["coordinates"] == [[116.1, 39.8], [116.2, 39.85], [116.3, 39.9]]

    r = httpx.post(
        b + "/api/v1/batches",
        json={"points": [{"id": "P1", "lon": 116.150, "lat": 39.810}]},
        timeout=10,
    )
    assert r.status_code == 201
    assert r.json()["affected_road_ids"] == ["R_SHARED"]
    got = httpx.get(a + "/api/v1/roads/R_SHARED", timeout=10).json()
    assert got["geometry"]["coordinates"][0] == [116.15, 39.81]


def test_paused_assembly_does_not_hold_write_lock(apis):
    """A slow assembly on A must not block an ingest on B."""
    a, b = apis
    result = {}

    def slow_batch():
        result["r"] = httpx.post(
            a + "/api/v1/batches",
            params={"pause_ms": 1500},
            json={"points": [{"id": "P4", "lon": 116.400, "lat": 39.950}]},
            timeout=20,
        )

    t = threading.Thread(target=slow_batch)
    t.start()
    time.sleep(0.15)  # let the paused batch enter its assembly pause

    t0 = time.time()
    r = httpx.post(
        b + "/api/v1/batches",
        json={"roads": [{"id": "R_CONCURRENT", "point_ids": ["P4", "P2"]}]},
        timeout=10,
    )
    elapsed = time.time() - t0
    assert r.status_code == 201, r.text
    assert elapsed < 1.2, f"ingest was blocked by the paused batch ({elapsed:.2f}s)"

    t.join(timeout=20)
    assert result["r"].status_code == 201
    # R_CONCURRENT was created while P4 did not exist yet at A's snapshot
    road = httpx.get(a + "/api/v1/roads/R_CONCURRENT", timeout=10).json()
    assert road["status"] == "ok", road
    assert road["geometry"]["coordinates"][0] == [116.4, 39.95]

    # ... and A's batch, which committed later, still saw the new road
    assert "R_CONCURRENT" in result["r"].json()["affected_road_ids"]


def test_concurrent_reference_and_correction_no_lost_update(apis):
    """Race point corrections against road creation/repointing on both APIs."""
    a, b = apis
    httpx.post(
        a + "/api/v1/batches",
        json={"points": [{"id": f"Q{i}", "lon": 1.0 + i, "lat": 2.0 + i} for i in range(4)]},
        timeout=10,
    )

    errors: list[str] = []
    lock = threading.Lock()

    def worker(base, i):
        try:
            if i % 3 == 0:
                # correct a referenced point
                r = httpx.post(
                    base + "/api/v1/batches",
                    params={"pause_ms": 0},
                    json={"points": [{"id": "Q0", "lon": 1.0 + i / 100.0, "lat": 2.0}]},
                    timeout=30,
                )
            elif i % 3 == 1:
                # add / repoint a road that references Q0
                r = httpx.post(
                    base + "/api/v1/batches",
                    json={
                        "roads": [
                            {"id": f"RQ{i}", "point_ids": ["Q0", f"Q{1 + i % 3}"]}
                        ]
                    },
                    timeout=30,
                )
            else:
                # touch an unrelated point + delete a referenced one and restore
                r = httpx.post(
                    base + "/api/v1/batches",
                    json={"points": [{"id": f"Q{i % 4}", "lon": 1.0 + i, "lat": 2.0 + i}]},
                    timeout=30,
                )
            if r.status_code != 201:
                raise AssertionError(f"{r.status_code} {r.text}")
        except Exception as exc:  # pragma: no cover - failure path
            with lock:
                errors.append(f"{base}: {exc}")

    threads = [
        threading.Thread(target=worker, args=(base, i))
        for i, base in enumerate([a, b] * 8)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors

    state = httpx.get(a + "/api/v1/state", timeout=10).json()
    points = {p["id"]: [p["lon"], p["lat"]] for p in state["points"]}
    for road in state["roads"]:
        assert road["status"] == "ok", road
        for pid, pos in zip(road["point_ids"], road["geometry"]["coordinates"]):
            assert pos == points[pid], f"{road['id']}: stale coordinate for {pid}"

    # every road referencing Q0 must carry the FINAL committed coordinate
    final_q0 = points["Q0"]
    for road in state["roads"]:
        if "Q0" in road["point_ids"]:
            idx = road["point_ids"].index("Q0")
            assert road["geometry"]["coordinates"][idx] == final_q0


def test_state_endpoint_is_consistent_under_writes(apis):
    a, b = apis
    stop = threading.Event()
    failures: list[str] = []

    def mutate():
        n = 0
        while not stop.is_set():
            try:
                r = httpx.post(
                    a + "/api/v1/batches",
                    json={
                        "points": [{"id": "Q1", "lon": 10.0 + (n % 50) / 100.0, "lat": 20.0}],
                        "roads": [{"id": "RQ1", "point_ids": ["Q0", "Q1"]}],
                    },
                    timeout=30,
                )
                if r.status_code != 201:
                    failures.append(f"write {r.status_code} {r.text}")
            except Exception as exc:  # pragma: no cover
                failures.append(str(exc))
            n += 1

    t = threading.Thread(target=mutate)
    t.start()
    try:
        for _ in range(40):
            state = httpx.get(b + "/api/v1/state", timeout=10).json()
            coords = {p["id"]: [p["lon"], p["lat"]] for p in state["points"]}
            for road in state["roads"]:
                if road["geometry"] is None:
                    continue
                for pid, pos in zip(road["point_ids"], road["geometry"]["coordinates"]):
                    assert pos == coords[pid], f"mixed snapshot for {road['id']}/{pid}"
    finally:
        stop.set()
        t.join(timeout=20)
    assert not failures, failures


def test_withdraw_via_other_process_updates_all_roads(apis):
    """A batch committed on A is withdrawn on B; A immediately sees the result."""
    a, b = apis
    prefix = "W1"
    pts = [
        {"id": f"{prefix}P1", "lon": 30.0, "lat": 40.0},
        {"id": f"{prefix}P2", "lon": 31.0, "lat": 41.0},
        {"id": f"{prefix}P3", "lon": 32.0, "lat": 42.0},
    ]
    r = httpx.post(
        a + "/api/v1/batches",
        json={
            "points": pts,
            "roads": [
                {"id": f"{prefix}R1", "point_ids": [f"{prefix}P1", f"{prefix}P2"]},
                {"id": f"{prefix}R2", "point_ids": [f"{prefix}P2", f"{prefix}P3"]},
            ],
        },
        timeout=10,
    )
    assert r.status_code == 201, r.text

    r = httpx.post(
        a + "/api/v1/batches",
        json={"points": [{"id": f"{prefix}P2", "lon": 39.5, "lat": 49.5}]},
        timeout=10,
    )
    assert r.status_code == 201, r.text
    bid = r.json()["batch_id"]
    assert r.json()["withdrawable"] is True

    # A road created through A *after* the correction must also be recomputed.
    r = httpx.post(
        a + "/api/v1/batches",
        json={"roads": [{"id": f"{prefix}R3", "point_ids": [f"{prefix}P3", f"{prefix}P2"]}]},
        timeout=10,
    )
    assert r.status_code == 201
    assert httpx.get(a + f"/api/v1/roads/{prefix}R3", timeout=10).json()["geometry"][
        "coordinates"
    ][1] == [39.5, 49.5]

    w = httpx.post(b + f"/api/v1/batches/{bid}/withdraw", timeout=10)
    assert w.status_code == 200, w.text
    body = w.json()
    assert body["affected_road_ids"] == [f"{prefix}R1", f"{prefix}R2", f"{prefix}R3"]

    state = httpx.get(a + "/api/v1/state", timeout=10).json()
    coords = {p["id"]: [p["lon"], p["lat"]] for p in state["points"]}
    assert coords[f"{prefix}P2"] == [31.0, 41.0]
    for road in state["roads"]:
        if not road["id"].startswith(prefix):
            continue
        for pid, pos in zip(road["point_ids"], road["geometry"]["coordinates"]):
            assert pos == coords[pid], f"{road['id']}/{pid} stale after withdrawal"


def test_withdraw_races_a_correction_without_lost_update(apis):
    """Withdrawal on one process, correction of the same point on the other.

    Exactly one of the following must hold afterwards:
    * the withdrawal won first and the later correction then makes it 409 on a
      retry, or
    * the correction won first and the withdrawal is refused with 409.
    Either way the point must never end up at a value that disagrees with the
    persisted geometry, and a 409 must leave the correction intact.
    """
    a, b = apis
    prefix = "WR"
    pts = [{"id": f"{prefix}P1", "lon": 50.0, "lat": 60.0},
           {"id": f"{prefix}P2", "lon": 51.0, "lat": 61.0}]
    httpx.post(
        a + "/api/v1/batches",
        json={"points": pts, "roads": [{"id": f"{prefix}R", "point_ids": [f"{prefix}P1", f"{prefix}P2"]}]},
        timeout=10,
    )
    r = httpx.post(
        a + "/api/v1/batches",
        json={"points": [{"id": f"{prefix}P1", "lon": 55.0, "lat": 65.0}]},
        timeout=10,
    )
    bid = r.json()["batch_id"]

    barrier = threading.Barrier(2)
    results: dict[str, httpx.Response] = {}

    def do_withdraw():
        barrier.wait()
        results["withdraw"] = httpx.post(
            a + f"/api/v1/batches/{bid}/withdraw", timeout=30
        )

    def do_correct():
        barrier.wait()
        results["correct"] = httpx.post(
            b + "/api/v1/batches",
            json={"points": [{"id": f"{prefix}P1", "lon": 57.0, "lat": 67.0}]},
            timeout=30,
        )

    threads = [threading.Thread(target=do_withdraw), threading.Thread(target=do_correct)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=40)

    assert results["correct"].status_code == 201, results["correct"].text
    assert results["withdraw"].status_code in (200, 409), results["withdraw"].text

    state = httpx.get(a + "/api/v1/state", timeout=10).json()
    coords = {p["id"]: [p["lon"], p["lat"]] for p in state["points"]}
    road = next(r for r in state["roads"] if r["id"] == f"{prefix}R")
    assert road["geometry"]["coordinates"][0] == coords[f"{prefix}P1"]

    if results["withdraw"].status_code == 409:
        # The correction was applied first and must not have been rolled back.
        assert coords[f"{prefix}P1"] == [55.0, 65.0] or coords[f"{prefix}P1"] == [57.0, 67.0]


def test_concurrent_double_withdrawal_modifies_once(apis):
    a, b = apis
    prefix = "WD"
    httpx.post(
        a + "/api/v1/batches",
        json={
            "points": [{"id": f"{prefix}P1", "lon": 70.0, "lat": 80.0},
                       {"id": f"{prefix}P2", "lon": 71.0, "lat": 81.0}],
            "roads": [{"id": f"{prefix}R", "point_ids": [f"{prefix}P1", f"{prefix}P2"]}],
        },
        timeout=10,
    )
    r = httpx.post(
        a + "/api/v1/batches",
        json={"points": [{"id": f"{prefix}P1", "lon": 75.0, "lat": 85.0}]},
        timeout=10,
    )
    bid = r.json()["batch_id"]

    barrier = threading.Barrier(2)
    results: dict[str, httpx.Response] = {}

    def call(name, base):
        barrier.wait()
        results[name] = httpx.post(base + f"/api/v1/batches/{bid}/withdraw", timeout=30)

    threads = [threading.Thread(target=call, args=("a", a)),
               threading.Thread(target=call, args=("b", b))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=40)

    assert results["a"].status_code == 200, results["a"].text
    assert results["b"].status_code == 200, results["b"].text
    restored = [r.json() for r in results.values() if not r.json()["already_withdrawn"]]
    replays = [r.json() for r in results.values() if r.json()["already_withdrawn"]]
    assert len(restored) == 1 and len(replays) == 1, results
    assert restored[0]["points_restored"] == 1
    assert replays[0]["points_restored"] == 0

    road = httpx.get(b + f"/api/v1/roads/{prefix}R", timeout=10).json()
    assert road["geometry"]["coordinates"][0] == [70.0, 80.0]
    assert road["revision"] == 3  # create -> correction -> one withdrawal
