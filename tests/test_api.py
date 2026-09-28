"""HTTP 端到端：状态码、错误分类、鉴权、查询。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.service import EventService


@pytest.fixture
def client(service) -> TestClient:
    return TestClient(create_app(service))


@pytest.fixture
def authed_client(tmp_path, clock) -> TestClient:
    svc = EventService(tmp_path / "auth.db", write_token="tok-123", clock=clock)
    return TestClient(create_app(svc))


def _ev(eid: str, ts: int = 10_000, amount: float = 1.0) -> dict:
    return {"event_id": eid, "event_type": "purchase", "subject": "u1",
            "ts_ms": ts, "payload": {"amount": amount}}


def test_ingest_and_duplicate_returns_200(client: TestClient) -> None:
    r1 = client.post("/events", json=_ev("e1"))
    r2 = client.post("/events", json=_ev("e1"))
    assert r1.status_code == 200 and r1.json()["deduped"] is False
    assert r2.status_code == 200 and r2.json()["deduped"] is True
    print(f"[输入] POST /events 两次相同 body；[判定] 200 deduped 分别 False/True")


def test_settle_then_reject_409(client: TestClient, clock) -> None:
    client.post("/events", json=_ev("e1"))
    clock.advance_to(70_000)
    assert client.post("/settle").status_code == 200
    r = client.post("/events", json=_ev("e2"))
    assert r.status_code == 409
    assert r.json()["reason"] == "WINDOW_SETTLED"
    print(f"[输入] 结算后再写同窗口事件；[判定] 409 {r.json()}")


def test_validation_errors_422(client: TestClient) -> None:
    r = client.post("/events", json={"event_id": "x"})
    assert r.status_code == 422 and r.json()["reason"] == "MISSING_FIELD"
    r2 = client.post("/events", json={
        "event_id": "x", "event_type": "purchase", "subject": "u",
        "ts_ms": 1, "payload": {"amount": "bad"},
    })
    assert r2.status_code == 422 and r2.json()["reason"] == "TYPE_MISMATCH"
    r3 = client.post("/events", json={
        "event_id": "x", "event_type": "nope", "subject": "u",
        "ts_ms": 1, "payload": {},
    })
    assert r3.status_code == 422 and r3.json()["reason"] == "UNKNOWN_EVENT_TYPE"
    print("[输入] 三类非法请求；[判定依据] 均 422 但 reason 互不相同")


def test_auth_required(authed_client: TestClient) -> None:
    r = authed_client.post("/events", json=_ev("e1"))
    assert r.status_code == 401 and r.json()["reason"] == "UNAUTHORIZED"
    r2 = authed_client.post("/events", json=_ev("e1"), headers={"X-Write-Token": "wrong"})
    assert r2.status_code == 401
    r3 = authed_client.post("/events", json=_ev("e1"), headers={"X-Write-Token": "tok-123"})
    assert r3.status_code == 200 and r3.json()["deduped"] is False
    print("[输入] 无/错/对 token 三次写入；[判定依据] 401/401/200")


def test_aggregates_endpoint(client: TestClient, clock) -> None:
    client.post("/events", json=_ev("a", amount=3.0))
    client.post("/events", json=_ev("b", amount=4.0))
    clock.advance_to(70_000)
    client.post("/settle")
    r = client.get("/aggregates", params={"settled_only": "true"})
    assert r.status_code == 200
    body = r.json()
    assert body["settled"] is True
    assert body["rows"][0]["count"] == 2
    assert body["rows"][0]["amount_sum"] == 7.0
    print(f"[输入] 结算后 GET /aggregates；[判定依据] {body}")


def test_settle_endpoint_advances_with_clock(client: TestClient, clock) -> None:
    client.post("/events", json=_ev("old", ts=1_000))
    # 当前 30s，窗口尚未越过水位线 -> 无结算
    assert client.post("/settle").json()["settled_buckets"] == []
    clock.advance_to(70_000)
    r = client.post("/settle")
    assert r.status_code == 200
    assert "0/60000" in r.json()["settled_buckets"]
    print(f"[输入] ts=1s 的事件后在 now=30s/70s 两次调用 /settle；[判定依据] {r.json()}")
