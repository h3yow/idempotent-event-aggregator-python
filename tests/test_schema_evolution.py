"""结构兼容演进与可区分拒绝原因场景。"""
from __future__ import annotations

import json

import pytest

from app.errors import RejectError, RejectReason


def test_old_producer_missing_new_field_gets_default(service) -> None:
    # 模拟 v1 生产者（无 coupon 字段）与 v2 生产者混存
    old_style = {
        "event_id": "old-1", "event_type": "purchase", "subject": "u1",
        "ts_ms": 10_000, "payload": {"amount": 10.0},
    }
    new_style = {
        "event_id": "new-1", "event_type": "purchase", "subject": "u1",
        "ts_ms": 11_000, "payload": {"amount": 5.0, "coupon": "SAVE10"},
    }
    service.ingest(old_style, now_ms=12_000)
    service.ingest(new_style, now_ms=13_000)

    rows = service.aggregates(event_type="purchase", settled_only=False).rows
    assert len(rows) == 1
    assert rows[0].count == 2
    assert rows[0].amount_sum == 15.0

    # 旧记录在存储中被补齐了默认值，重读结论稳定
    stored = service._conn.execute(
        "SELECT payload, schema_version FROM events WHERE event_id='old-1'"
    ).fetchone()
    payload = json.loads(stored["payload"])
    assert payload["values"]["coupon"] == ""
    print(f"[输入] 旧生产者={old_style['payload']}，新生产者={new_style['payload']}")
    print(f"[判定依据] 缺省 coupon 补默认''，聚合 amount_sum=15，旧记录={payload}")


def test_click_default_amount_does_not_move_sum(service) -> None:
    # click v1 无 amount，默认 0.0：新老数据混存 sum 仍可复现
    service.ingest({
        "event_id": "c1", "event_type": "click", "subject": "u1",
        "ts_ms": 10_000, "payload": {"target": "banner"},
    }, now_ms=11_000)
    service.ingest({
        "event_id": "c2", "event_type": "click", "subject": "u1",
        "ts_ms": 10_000, "payload": {"target": "banner", "amount": 2.5},
    }, now_ms=12_000)
    rows = service.aggregates(event_type="click", settled_only=False).rows
    assert rows[0].count == 2
    assert rows[0].amount_sum == 2.5
    print("[输入] 两条 click：一条无 amount，一条 amount=2.5")
    print("[判定依据] 缺失 amount 默认 0.0，count=2、sum=2.5，结论稳定")


def test_repeated_aggregation_is_reproducible(service) -> None:
    for i in range(5):
        service.ingest({
            "event_id": f"p{i}", "event_type": "purchase", "subject": "u1",
            "ts_ms": 10_000, "payload": {"amount": i},
        }, now_ms=11_000)
    service.settle(now_ms=70_000)
    first = service.aggregates(settled_only=True).model_dump()
    second = service.aggregates(settled_only=True).model_dump()
    assert first == second
    print(f"[输入] 5 条 purchase amount=0..4，结算后两次查询")
    print(f"[判定依据] 已结算窗口不可变，两次结果一致: {first}")


@pytest.mark.parametrize(
    "raw,reason",
    [
        ("not-a-dict", RejectReason.BAD_PAYLOAD),
        ({"event_type": "purchase", "subject": "u", "ts_ms": 1,
          "payload": {"amount": 1}}, RejectReason.MISSING_FIELD),
        ({"event_id": "x", "event_type": "purchase", "subject": "u",
          "ts_ms": 1, "payload": {}}, RejectReason.MISSING_FIELD),
        ({"event_id": "x", "event_type": "purchase", "subject": "u",
          "ts_ms": "1", "payload": {"amount": 1}}, RejectReason.TYPE_MISMATCH),
        ({"event_id": "x", "event_type": "purchase", "subject": "u",
          "ts_ms": 1, "payload": {"amount": "1"}}, RejectReason.TYPE_MISMATCH),
        ({"event_id": 1, "event_type": "purchase", "subject": "u",
          "ts_ms": 1, "payload": {}}, RejectReason.TYPE_MISMATCH),
        ({"event_id": "x", "event_type": "unknown-x", "subject": "u",
          "ts_ms": 1, "payload": {}}, RejectReason.UNKNOWN_EVENT_TYPE),
        ({"event_id": "x", "event_type": "purchase", "subject": "u",
          "ts_ms": -1, "payload": {"amount": 1}}, RejectReason.BAD_PAYLOAD),
    ],
)
def test_rejection_reasons_are_distinct(service, raw, reason) -> None:
    with pytest.raises(RejectError) as exc:
        service.ingest(raw, now_ms=2_000)
    assert exc.value.reason == reason
    print(f"[输入] {raw!r:.200}")
    print(f"[判定依据] 拒绝原因={reason.value}，消息={exc.value.message}")
