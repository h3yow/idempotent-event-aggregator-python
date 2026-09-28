"""Schema evolution: v1/v2 mixed storage and stable aggregates."""

from __future__ import annotations

import logging

logger = logging.getLogger("event_aggregator.test")

AUTH_A = {"Authorization": "Bearer token-a"}
AUTH_ADMIN = {"Authorization": "Bearer token-admin"}


def v1(eid, ts, **extra):
    payload = {"event_id": eid, "event_type": "spend", "occurred_at": ts}
    payload.update(extra)
    return payload


def v2(eid, ts, value=None, **extra):
    payload = {
        "event_id": eid,
        "event_type": "spend",
        "occurred_at": ts,
        "schema_version": 2,
    }
    if value is not None:
        payload["value"] = value
    payload.update(extra)
    return payload


def aggregates(client):
    r = client.get(
        "/aggregates", headers=AUTH_A, params={"event_type": "spend"}
    )
    assert r.status_code == 200
    return r.json()["windows"]


def test_v1_and_v2_mixed_storage_aggregates_stably(client, caplog):
    events = [
        v1("old-1", 100),                 # pre-upgrade record
        v1("old-2", 200),
        v2("new-1", 300, value=10.0),     # post-upgrade, with value
        v2("new-2", 400, value=2.5),
        v2("new-3", 500),                 # v2 but new field omitted
        v2("new-4", 600, value=None),     # explicit null -> treated absent
    ]
    for e in events:
        r = client.post("/events", json=e, headers=AUTH_A)
        assert r.status_code == 200, (e, r.text)
    logger.info("mixed-version inputs accepted: %s", events)

    wins = aggregates(client)
    assert len(wins) == 1
    w = wins[0]
    assert w["count"] == 6, w
    # v1 and missing-value v2 records still count; sum ignores absent
    # values rather than failing or dropping records.
    assert w["sum_value"] == 12.5, w
    assert w["schema_versions"] == [1, 2], w
    caplog.records and None

    # Snapshot the window and confirm the conclusion is frozen/reproducible.
    r = client.post(
        "/admin/settle",
        json={"watermark_ms": 1000, "tenants": ["tenant-a"]},
        headers=AUTH_ADMIN,
    )
    assert r.status_code == 200
    frozen = aggregates(client)
    assert frozen[0]["state"] == "settled"
    assert frozen[0]["count"] == 6
    assert frozen[0]["sum_value"] == 12.5
    assert frozen[0]["schema_versions"] == [1, 2]
    logger.info("settled mixed-version window: %s", frozen)


def test_unknown_fields_are_kept_not_rejected(client):
    payload = v1("future", 10, new_fancy_field={"nested": [1, 2]})
    r = client.post("/events", json=payload, headers=AUTH_A)
    assert r.status_code == 200, r.text
    assert aggregates(client)[0]["count"] == 1


def test_explicit_schema_version_one_still_valid(client):
    r = client.post(
        "/events",
        json={
            "event_id": "x",
            "event_type": "spend",
            "occurred_at": 1,
            "schema_version": 1,
        },
        headers=AUTH_A,
    )
    assert r.status_code == 200
    assert aggregates(client)[0]["schema_versions"] == [1]


def test_missing_new_field_is_not_an_error(client, caplog):
    # v2 without 'value' must be distinguished from 'value' present with
    # a wrong type: the former is accepted, the latter rejected.
    missing = v2("ok-missing", 5)
    r1 = client.post("/events", json=missing, headers=AUTH_A)
    assert r1.status_code == 200, r1.text

    wrong_type = v2("bad-type", 6, value="10")
    # explicit kw would cast to string at construction; build directly
    wrong_type["value"] = "10"
    r2 = client.post("/events", json=wrong_type, headers=AUTH_A)
    assert r2.status_code == 422
    assert r2.json()["details"]["reason"] == "invalid_type"
    logger.info(
        "evolution decision input_missing=%s -> accepted; "
        "input_wrong_type=%s -> rejected invalid_event/invalid_type",
        missing,
        wrong_type,
    )

    # Rejected record was never stored; only the accepted one counts.
    assert aggregates(client)[0]["count"] == 1
