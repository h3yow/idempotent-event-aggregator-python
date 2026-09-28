"""窗口分桶与结算语义。

- 采用固定窗口（tumbling window），窗口大小由 WINDOW_SIZE_MS 决定。
- 事件按其事件时间 ts_ms 归入对应窗口，迟到事件允许回补到尚未结算的旧窗口。
- 水位线 = 当前结算推进时间 - SETTLE_LAG_MS：起始时刻早于水位线的窗口视为已结算，
  之后拒绝写入，保证“已经结算的窗口不能被随意改写”。
"""
from __future__ import annotations

import sqlite3

WINDOW_SIZE_MS = 60_000  # 1 分钟固定窗口
SETTLE_LAG_MS = 5_000    # 水位线滞后 5 秒，窗口结束后 5 秒内仍可接收迟到事件


def bucket_of(ts_ms: int) -> str:
    """返回事件时间对应的窗口标签，例如 2026-01-01T00:00Z/60000。"""
    start = (ts_ms // WINDOW_SIZE_MS) * WINDOW_SIZE_MS
    return f"{start}/{WINDOW_SIZE_MS}"


def bucket_start(bucket: str) -> int:
    return int(bucket.split("/", 1)[0])


def _settled_exists(conn: sqlite3.Connection, bucket: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM settled_windows WHERE bucket = ?", (bucket,)
    ).fetchone()
    return row is not None


def is_settled(conn: sqlite3.Connection, bucket: str, now_ms: int) -> bool:
    """窗口已结算的两种等价判据：
    1. 已显式记录在 settled_windows 中；
    2. 窗口结束时刻 + 滞后时间已过（水位线已越过该窗口）。
    """
    if _settled_exists(conn, bucket):
        return True
    start = bucket_start(bucket)
    watermark = now_ms - SETTLE_LAG_MS
    return start + WINDOW_SIZE_MS <= watermark


def settle_pending(conn: sqlite3.Connection, now_ms: int) -> list[str]:
    """把所有“已越过水位线但尚未登记”的窗口标记为已结算。

    返回本次新结算的窗口标签列表。已登记的窗口不会重复处理。
    """
    watermark = now_ms - SETTLE_LAG_MS
    # 找出已有事件、窗口已关闭且尚未登记的窗口
    rows = conn.execute(
        """
        SELECT DISTINCT bucket FROM events
        WHERE bucket NOT IN (SELECT bucket FROM settled_windows)
        """
    ).fetchall()
    settled: list[str] = []
    for row in rows:
        bucket = row["bucket"]
        if bucket_start(bucket) + WINDOW_SIZE_MS <= watermark:
            conn.execute(
                "INSERT OR IGNORE INTO settled_windows (bucket, settled_at_ms) "
                "VALUES (?, ?)",
                (bucket, now_ms),
            )
            settled.append(bucket)
    return sorted(settled)
