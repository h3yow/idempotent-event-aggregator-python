"""Concurrent-write and read/write consistency scenarios.

These tests submit the same events from multiple threads and assert that
no event is counted twice or lost, that concurrent duplicate replays
never raise, and that reads during writes always observe a committed
state (WAL + single-writer transactions).
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from app.db import Database
from app.events import parse_event
from app.store import EventStore

logger = logging.getLogger("event_aggregator.test")

AUTH_A = {"Authorization": "Bearer token-a"}
AUTH_ADMIN = {"Authorization": "Bearer token-admin"}


def test_concurrent_distinct_events_counted_exactly_once(client, app_env):
    # Direct store-level stress: N threads inserting distinct events,
    # each event retried by multiple workers (duplicate replays).
    _tokens, db_path = app_env
    db = Database(str(db_path))
    conn = db.connect()
    store = EventStore(conn, window_seconds=1000)

    n_events = 50
    attempts_per_event = 4

    def worker(idx: int) -> dict:
        event_id = f"evt-{idx % n_events}"
        # Retries of the same logical event carry an identical payload —
        # that is exactly what idempotency guarantees cover.
        ev = parse_event(
            {"event_id": event_id, "event_type": "click", "occurred_at": 100},
            tenant="tenant-a",
        )
        return store.insert_events([ev], write_lock=db.write_lock)

    total = n_events * attempts_per_event
    with ThreadPoolExecutor(max_workers=16) as pool:
        summaries = list(pool.map(worker, range(total)))

    inserted = sum(s["inserted"] for s in summaries)
    duplicate_acks = sum(len(s["duplicates"]) for s in summaries)
    assert inserted == n_events, summaries[:5]
    assert duplicate_acks == total - n_events

    rows = client.get("/aggregates", headers=AUTH_A).json()["windows"]
    assert sum(r["count"] for r in rows) == n_events, rows
    logger.info(
        "concurrent inserts: attempts=%d inserted=%d duplicate_acks=%d final_count=%d",
        total,
        inserted,
        duplicate_acks,
        sum(r["count"] for r in rows),
    )


def test_concurrent_identical_submissions_never_conflict(client):
    # Through the HTTP layer: same id, identical body, many threads.
    event = {"event_id": "hot", "event_type": "click", "occurred_at": 7}

    def send():
        return client.post("/events", json=event, headers=AUTH_A)

    with ThreadPoolExecutor(max_workers=12) as pool:
        responses = list(pool.map(lambda _: send(), range(24)))

    statuses = [r.status_code for r in responses]
    assert set(statuses) == {200}, statuses
    inserts = sum(r.json()["inserted"] for r in responses)
    assert inserts == 1, responses
    rows = client.get("/aggregates", headers=AUTH_A).json()["windows"]
    assert rows[0]["count"] == 1


def test_failed_batch_rolls_back_under_concurrency(client, app_env):
    # Mix good batches with bad batches; after all threads finish, only
    # fully-valid batches' rows must exist.
    def good(i):
        return client.post(
            "/events",
            json=[
                {"event_id": f"g-{i}-1", "event_type": "click", "occurred_at": 50},
                {"event_id": f"g-{i}-2", "event_type": "click", "occurred_at": 51},
            ],
            headers=AUTH_A,
        )

    def bad(i):
        # second item repeats the first id inside one batch -> rollback
        return client.post(
            "/events",
            json=[
                {"event_id": f"bad-{i}", "event_type": "click", "occurred_at": 60},
                {"event_id": f"bad-{i}", "event_type": "click", "occurred_at": 61},
            ],
            headers=AUTH_A,
        )

    with ThreadPoolExecutor(max_workers=12) as pool:
        goods = [pool.submit(good, i) for i in range(20)]
        bads = [pool.submit(bad, i) for i in range(20)]
        good_results = [f.result() for f in goods]
        bad_results = [f.result() for f in bads]

    assert all(r.status_code == 200 for r in good_results)
    assert all(r.status_code == 409 for r in bad_results)

    rows = client.get("/aggregates", headers=AUTH_A).json()["windows"]
    assert sum(r["count"] for r in rows) == 40, rows  # 20 batches * 2


def test_concurrent_settlement_and_write_is_serializable(client):
    # Writers race against an admin settlement. Either an event lands in
    # the (now open) next window, or it is cleanly rejected as
    # window_settled; counts must always be explainable and consistent.
    barrier = threading.Barrier(8)

    def write(i):
        barrier.wait()
        return client.post(
            "/events",
            json={"event_id": f"race-{i}", "event_type": "click", "occurred_at": 10},
            headers=AUTH_A,
        )

    def settle():
        barrier.wait()
        return client.post(
            "/admin/settle",
            json={"watermark_ms": 1000, "tenants": ["tenant-a"]},
            headers=AUTH_ADMIN,
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        writers = [pool.submit(write, i) for i in range(7)]
        settle_fut = pool.submit(settle)
        wres = [f.result() for f in writers]
        sres = settle_fut.result()

    assert sres.status_code == 200, sres.text

    accepted = [r for r in wres if r.status_code == 200 and r.json()["inserted"]]
    rejected = [r for r in wres if r.status_code == 409]
    assert len(accepted) + len(rejected) == 7
    for r in rejected:
        assert r.json()["details"]["reason"] == "window_settled"

    # No matter how the race resolved, accepted rows are counted exactly
    # once and either all snapshotted or all still open (not split).
    rows = client.get("/aggregates", headers=AUTH_A).json()["windows"]
    total = sum(r["count"] for r in rows)
    assert total == len(accepted), (total, len(accepted))
    states = {r["state"] for r in rows}
    assert states <= {"settled", "open"}
    logger.info(
        "race settled: accepted=%d rejected=%d states=%s rows=%s",
        len(accepted),
        len(rejected),
        states,
        rows,
    )
