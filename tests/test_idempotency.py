"""Duplicate submission and error classification scenarios."""

from __future__ import annotations

import logging

import pytest

from tests.conftest import log_case

logger = logging.getLogger("event_aggregator.test")

AUTH_A = {"Authorization": "Bearer token-a"}
AUTH_B = {"Authorization": "Bearer token-b"}
AUTH_ADMIN = {"Authorization": "Bearer token-admin"}


def post_event(client, payload, headers=AUTH_A):
    return client.post("/events", json=payload, headers=headers)


def test_identical_resubmission_is_not_counted_twice(client, caplog):
    event = {"event_id": "e-1", "event_type": "click", "occurred_at": 100}

    r1 = post_event(client, event)
    assert r1.status_code == 200, r1.text
    assert r1.json()["inserted"] == 1
    log_case(caplog, "first submit", event, f"status={r1.status_code} inserted=1")

    # Same event_id with byte-identical payload -> idempotent duplicate.
    r2 = post_event(client, event)
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["inserted"] == 0
    assert body2["duplicates"] == [
        {"event_id": "e-1", "reason": "already_stored"}
    ]
    log_case(
        caplog,
        "identical resubmit",
        event,
        f"status={r2.status_code} inserted=0 duplicate_reason=already_stored",
    )

    agg = client.get("/aggregates", headers=AUTH_A).json()["windows"]
    assert len(agg) == 1
    assert agg[0]["count"] == 1, agg
    logger.info("aggregate after duplicate: %s", agg)


def test_same_id_different_payload_conflict(client):
    r1 = post_event(
        client,
        {"event_id": "e-2", "event_type": "click", "occurred_at": 100},
    )
    assert r1.status_code == 200

    # Same id, different body (conflicting truth) -> distinct rejection.
    r2 = post_event(
        client,
        {"event_id": "e-2", "event_type": "click", "occurred_at": 999},
    )
    assert r2.status_code == 409
    err = r2.json()
    assert err["error"] == "duplicate_event"
    assert err["details"]["reason"] == "event_id_conflict", err


def test_duplicate_id_within_batch_rolls_back_whole_batch(client, caplog):
    batch = [
        {"event_id": "b1", "event_type": "click", "occurred_at": 10},
        {"event_id": "b2", "event_type": "click", "occurred_at": 20},
        {"event_id": "b2", "event_type": "click", "occurred_at": 21},
    ]
    r = client.post("/events", json=batch, headers=AUTH_A)
    assert r.status_code == 409
    err = r.json()
    assert err["details"]["reason"] == "duplicate_in_batch", err
    log_case(
        caplog,
        "in-batch duplicate",
        batch,
        f"rolled back all rows: {err['error']}/{err['details']['reason']}",
    )

    # Atomicity: not even the first, valid rows survived.
    agg = client.get("/aggregates", headers=AUTH_A).json()["windows"]
    assert agg == [], agg


def test_batch_with_replayed_duplicates_succeeds(client):
    post_event(client, {"event_id": "old", "event_type": "click", "occurred_at": 5})
    batch = [
        {"event_id": "old", "event_type": "click", "occurred_at": 5},
        {"event_id": "new1", "event_type": "click", "occurred_at": 6},
        {"event_id": "new2", "event_type": "click", "occurred_at": 7},
    ]
    r = client.post("/events", json=batch, headers=AUTH_A)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["inserted"] == 2
    assert [d["event_id"] for d in body["duplicates"]] == ["old"]


def test_tenant_isolation_same_event_id(client):
    # The same event_id for two tenants are independent events.
    payload = {"event_id": "shared", "event_type": "click", "occurred_at": 1}
    assert post_event(client, payload, AUTH_A).status_code == 200
    assert post_event(client, payload, AUTH_B).status_code == 200

    a = client.get("/aggregates", headers=AUTH_A).json()["windows"]
    b = client.get("/aggregates", headers=AUTH_B).json()["windows"]
    assert a[0]["count"] == 1
    assert b[0]["count"] == 1


def test_auth_failures_are_distinguished(client):
    # No token
    r = client.post(
        "/events",
        json={"event_id": "x", "event_type": "t", "occurred_at": 1},
    )
    assert r.status_code == 401
    assert r.json()["details"]["reason"] == "missing_token"

    # Unknown token
    r = client.post(
        "/events",
        json={"event_id": "x", "event_type": "t", "occurred_at": 1},
        headers={"Authorization": "Bearer nope"},
    )
    assert r.status_code == 401
    assert r.json()["details"]["reason"] == "unknown_token"

    # Non-admin cannot settle
    r = client.post("/admin/settle", json={"watermark_ms": 1000}, headers=AUTH_A)
    assert r.status_code == 403
    assert r.json()["details"]["reason"] == "admin_required"


@pytest.mark.parametrize(
    "payload,reason",
    [
        ([1, 2, 3], "not_object"),
        ({"event_type": "click", "occurred_at": 1}, "missing_field"),
        ({"event_id": "x", "event_type": "click", "occurred_at": "1"}, "invalid_type"),
        ({"event_id": "x", "event_type": 3, "occurred_at": 1}, "invalid_type"),
        (
            {"event_id": "x", "event_type": "t", "occurred_at": 1, "schema_version": 7},
            "invalid_schema_version",
        ),
        (
            {"event_id": "x", "event_type": "t", "occurred_at": 1, "value": "heavy"},
            "invalid_type",
        ),
        (
            {"event_id": "x", "event_type": "t", "occurred_at": -1},
            "invalid_timestamp",
        ),
    ],
)
def test_invalid_payloads_rejected_with_reasons(client, caplog, payload, reason):
    r = post_event(client, payload)
    assert r.status_code == 422, r.text
    err = r.json()
    assert err["error"] == "invalid_event"
    assert err["details"]["reason"] == reason, (payload, err)
    log_case(
        caplog,
        f"invalid payload expecting {reason}",
        payload,
        f"rejected invalid_event/{reason}",
    )
