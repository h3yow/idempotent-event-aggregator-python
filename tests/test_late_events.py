"""乱序到达 / 迟到事件 / 窗口结算场景。"""
from __future__ import annotations

import pytest

from app.errors import RejectError, RejectReason


def _purchase(event_id: str, ts_ms: int, amount: float, subject: str = "u1") -> dict:
    return {
        "event_id": event_id, "event_type": "purchase", "subject": subject,
        "ts_ms": ts_ms, "payload": {"amount": amount},
    }


def test_late_event_backfills_open_window(service) -> None:
    # 事件时间 10s，在窗口结束+滞后(65s)之前的 40s 才到达 -> 允许回补
    service.ingest(_purchase("a", 10_000, 10.0), now_ms=20_000)
    service.ingest(_purchase("b", 20_000, 20.0), now_ms=40_000)

    rows = service.aggregates(event_type="purchase", settled_only=False).rows
    assert len(rows) == 1
    assert rows[0].count == 2
    assert rows[0].amount_sum == 30.0
    print("[输入] 先到 a(ts=10s)@20s，迟到 b(ts=20s)@40s，窗口 0/60000 未关闭")
    print(f"[判定依据] 水位线未越过窗口，回补成功 rows={rows}")


def test_out_of_order_across_two_windows(service) -> None:
    # 先写入第二个窗口的事件，再补第一个窗口的迟到事件
    service.ingest(_purchase("later", 70_000, 5.0), now_ms=71_000)
    service.ingest(_purchase("late", 50_000, 7.0), now_ms=64_000)

    rows = {r.bucket: r for r in service.aggregates(settled_only=False).rows}
    assert rows["0/60000"].count == 1
    assert rows["0/60000"].amount_sum == 7.0
    assert rows["60000/60000"].count == 1
    assert rows["60000/60000"].amount_sum == 5.0
    print("[输入] 先写 ts=70s 事件，再乱序补 ts=50s 事件（到达时间 64s）")
    print(f"[判定依据] 按事件时间分桶而非到达时间 rows={sorted(rows)}")


def test_settled_window_rejects_late_writes(service) -> None:
    service.ingest(_purchase("a", 10_000, 10.0), now_ms=11_000)
    settled = service.settle(now_ms=70_000)  # 水位线=65s，0 窗口已关闭
    assert settled == ["0/60000"]

    with pytest.raises(RejectError) as exc:
        service.ingest(_purchase("too-late", 10_000, 1.0), now_ms=70_000)
    assert exc.value.reason == RejectReason.WINDOW_SETTLED

    # 结算后的聚合结论稳定不变
    rows = service.aggregates(settled_only=True).rows
    assert rows[0].count == 1
    assert rows[0].amount_sum == 10.0
    print("[输入] 窗口 0/60000 在 now=70s 结算后，再来同窗口事件 too-late")
    print("[判定依据] 已登记 settled_windows，拒绝原因 WINDOW_SETTLED，聚合不变")


def test_watermark_boundary(service) -> None:
    # 窗口结束 60s + 滞后 5s = 65s：到达时间 64999 仍可写入，65000 起拒绝
    ok = service.ingest(_purchase("edge-ok", 59_000, 1.0), now_ms=64_999)
    assert ok.deduped is False
    with pytest.raises(RejectError) as exc:
        service.ingest(_purchase("edge-no", 59_000, 1.0), now_ms=65_000)
    assert exc.value.reason == RejectReason.WINDOW_SETTLED
    print("[输入] 同窗口事件分别在到达时间 64999 / 65000 提交")
    print("[判定依据] watermark=now-5000，65000 时 start+60000<=watermark，拒绝")
