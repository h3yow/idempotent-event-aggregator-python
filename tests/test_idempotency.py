"""重复提交幂等场景。"""
from __future__ import annotations

from app.errors import RejectReason


def test_duplicate_submission_not_double_counted(service: EventService) -> None:
    event = {
        "event_id": "evt-1",
        "event_type": "purchase",
        "subject": "user-a",
        "ts_ms": 10_000,
        "payload": {"amount": 99.5},
    }
    first = service.ingest(event, now_ms=11_000)
    second = service.ingest(event, now_ms=12_000)
    third = service.ingest(event, now_ms=13_000)

    assert first.deduped is False
    assert second.deduped is True
    assert third.deduped is True
    assert second.bucket == first.bucket

    rows = service.aggregates(event_type="purchase", settled_only=False).rows
    assert len(rows) == 1
    assert rows[0].count == 1
    assert rows[0].amount_sum == 99.5
    print(f"[输入] 同一事件提交3次: {event}")
    print(f"[判定依据] event_id 主键去重，计数 rows={rows}")


def test_duplicate_with_different_body_treated_as_dedup(service: EventService) -> None:
    first_event = {
        "event_id": "evt-2", "event_type": "purchase", "subject": "user-a",
        "ts_ms": 20_000, "payload": {"amount": 10},
    }
    tampered = {
        "event_id": "evt-2", "event_type": "purchase", "subject": "user-b",
        "ts_ms": 20_000, "payload": {"amount": 999},
    }
    service.ingest(first_event, now_ms=21_000)
    result = service.ingest(tampered, now_ms=22_000)

    assert result.deduped is True
    rows = service.aggregates(settled_only=False).rows
    assert len(rows) == 1
    assert rows[0].subject == "user-a"
    assert rows[0].count == 1
    assert rows[0].amount_sum == 10.0
    print(f"[输入] 首次={first_event} 重复但内容被篡改={tampered}")
    print("[判定依据] 同 event_id 一律按首次结果去重，不覆盖原记录")
