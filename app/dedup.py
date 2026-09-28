"""幂等去重辅助。

最终的“不重复计数”保障来自 events.event_id 主键上的 INSERT 冲突
（见 repository.insert_event）。本模块提供只读查询，便于在重复提交时
取回首次写入的记录用于响应与日志，使判定依据可解释、可复现。
"""
from __future__ import annotations

import sqlite3
from typing import Any


def find_event(conn: sqlite3.Connection, event_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT event_id, event_type, subject, ts_ms, bucket, schema_version "
        "FROM events WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    if row is None:
        return None
    return dict(row)


def same_fingerprint(existing: dict[str, Any], *, event_type: str,
                     subject: str, ts_ms: int) -> bool:
    """同一 event_id 再次提交但内容不同：按重复提交处理（返回首次结果），
    但该差异会体现在日志中，调用方据此给出可解释判定。
    """
    return (
        existing["event_type"] == event_type
        and existing["subject"] == subject
        and existing["ts_ms"] == ts_ms
    )
