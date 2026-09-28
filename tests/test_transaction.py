"""写入事务原子性：计数更新失败时事件插入也必须回滚。"""
from __future__ import annotations

from unittest.mock import patch

from app import repository


def test_counter_failure_rolls_back_event(service) -> None:
    event = {
        "event_id": "tx-1", "event_type": "purchase", "subject": "u1",
        "ts_ms": 10_000, "payload": {"amount": 10.0},
    }

    # 让“计数更新”这一步必然失败，模拟写入中途异常
    with patch.object(
        repository, "upsert_counter",
        side_effect=RuntimeError("simulated mid-write failure"),
    ):
        try:
            service.ingest(event, now_ms=11_000)
        except RuntimeError:
            pass

    # 事件行不得残留，计数也不得变化：整体回滚
    stored = service._conn.execute(
        "SELECT COUNT(*) AS c FROM events WHERE event_id='tx-1'"
    ).fetchone()["c"]
    assert stored == 0
    rows = service.aggregates(settled_only=False).rows
    assert rows == []

    # 同一事件随后可以正常重放（没有半完成状态阻碍重试）
    r = service.ingest(event, now_ms=12_000)
    assert r.deduped is False
    rows = service.aggregates(settled_only=False).rows
    assert rows[0].count == 1
    print("[输入] 事件写入过程中计数步骤注入 RuntimeError，随后重放同一事件")
    print("[判定依据] 事务 ROLLBACK 后 events/counters 均无残留，重放成功且仅计 1 次")
