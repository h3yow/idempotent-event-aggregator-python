"""Out-of-order / late-arrival and window settlement scenarios."""

from __future__ import annotations

import logging

import pytest

from tests.conftest import log_case

logger = logging.getLogger("event_aggregator.test")

AUTH_A = {"Authorization": "Bearer token-a"}
AUTH_ADMIN = {"Authorization": "Bearer token-admin"}


def settle(client, watermark, tenants=None):
    body = {"watermark_ms": watermark}
    if tenants is not None:
        body["tenants"] = tenants
    return client.post("/admin/settle", json=body, headers=AUTH_ADMIN)


def send(client, eid, ts, *, value=None, version=None, event_type="click"):
    payload = {"event_id": eid, "event_type": event_type, "occurred_at": ts}
    if version is not None:
        payload["schema_version"] = version
    if value is not None:
        payload["value"] = value
    return client.post("/events", json=payload, headers=AUTH_A)


def windows(client, **params):
    r = client.get("/aggregates", headers=AUTH_A, params=params)
    assert r.status_code == 200, r.text
    return r.json()["windows"]


def test_late_events_backfill_open_window(client, caplog):
    # Events arrive out of order inside window [0, 1000).
    send(client, "later", 800)
    send(client, "earlier", 100)
    send(client, "middle", 500)
    log_case(
        caplog,
        "out-of-order arrivals",
        [800, 100, 500],
        "all accepted into the same open window",
    )

    w = windows(client)
    assert len(w) == 1
    assert w[0]["window_start"] == 0
    assert w[0]["window_end"] == 1000
    assert w[0]["count"] == 3
    assert w[0]["state"] == "open"


def test_settlement_freezes_window_and_blocks_rewrites(client, caplog):
    send(client, "a", 10)
    send(client, "b", 20)

    r = settle(client, 1000)
    assert r.status_code == 200, r.text
    assert r.json()["tenants"]["tenant-a"]["frozen_windows"] == [
        {
            "event_type": "click",
            "window_start": 0,
            "count": 2,
            "sum_value": 0.0,
        }
    ]

    # Query now returns the immutable settled snapshot.
    w = windows(client)
    assert w[0]["state"] == "settled"
    assert w[0]["count"] == 2
    settled_at_1 = w[0]["settled_at"]

    # A genuinely new event for the settled window is rejected, with a
    # reason distinct from duplicate replay.
    r = send(client, "late-new", 500)
    assert r.status_code == 409
    err = r.json()
    assert err["error"] == "window_settled"
    assert err["details"]["reason"] == "window_settled", err
    log_case(
        caplog,
        "late event after settle",
        {"event_id": "late-new", "occurred_at": 500},
        f"rejected window_settled: {err['details']}",
    )

    # Identical replay of an already-accepted event stays idempotent and
    # must NOT be mis-classified as window_settled.
    r = send(client, "a", 10)
    assert r.status_code == 200
    assert r.json()["duplicates"][0]["reason"] == "already_stored"

    # Snapshot is bit-stable across repeated queries.
    w2 = windows(client)
    assert w2 == w
    assert w2[0]["settled_at"] == settled_at_1


def test_settlement_is_idempotent_and_watermark_monotonic(client):
    send(client, "a", 10)
    assert settle(client, 1000).status_code == 200

    # Re-settling at the same watermark freezes nothing new.
    again = settle(client, 1000).json()["tenants"]["tenant-a"]
    assert again["frozen_windows"] == []

    # Watermark cannot move backwards.
    r = settle(client, 999)
    assert r.status_code == 409
    assert r.json()["details"]["reason"] == "watermark_regression"


def test_new_window_remains_open_after_settlement(client):
    # Window [0,1000)
    send(client, "old", 100)
    settle(client, 1000)

    # Events at exactly the watermark belong to the next, open window.
    assert send(client, "boundary", 1000).status_code == 200
    assert send(client, "in-next", 1500).status_code == 200

    w = windows(client)
    by_start = {row["window_start"]: row for row in w}
    assert set(by_start) == {0, 1000}
    assert by_start[0]["state"] == "settled"
    assert by_start[0]["count"] == 1
    assert by_start[1000]["state"] == "open"
    assert by_start[1000]["count"] == 2

    # Settle the second window too; late arrivals are then blocked there.
    settle(client, 2000)
    r = send(client, "late-second", 1999)
    assert r.status_code == 409
    assert r.json()["details"]["reason"] == "window_settled"


def test_partial_window_not_early_settled(client):
    # Watermark 500 does not fully close the [0,1000) window, so no
    # snapshot is produced and the window is still reported as open for
    # queries. But the watermark itself is the no-rewrite frontier: an
    # event behind it is refused even though the window is unsnapshotted.
    send(client, "x", 100)
    result = settle(client, 500).json()["tenants"]["tenant-a"]
    assert result["frozen_windows"] == []

    w = windows(client)
    assert w[0]["state"] == "open"
    assert w[0]["count"] == 1

    # An event behind the advanced watermark is rejected.
    r = send(client, "late-behind-wm", 400)
    assert r.status_code == 409
    assert r.json()["details"]["reason"] == "window_settled"

    # An event at/after the watermark backfills the same open window.
    assert send(client, "still-open", 600).status_code == 200
    w2 = windows(client)
    assert w2[0]["state"] == "open"
    assert w2[0]["count"] == 2

    # Closing the window later snapshots it exactly as-is.
    settle(client, 1000)
    w3 = windows(client)
    assert w3[0]["state"] == "settled"
    assert w3[0]["count"] == 2
