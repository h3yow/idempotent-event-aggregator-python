"""事件写入与聚合读取的数据访问层。

所有函数都假设运行在调用方开启的事务中（见 service.EventService）：
- 事件写入与计数更新在同一事务内原子提交，中途失败整体回滚。
- 计数更新使用 ``INSERT ... ON CONFLICT DO UPDATE SET cnt = cnt + 1``，
  以数据库行级原子加法完成，配合 BEGIN IMMEDIATE 的单写者锁，
  并发写入下既不会重复计数也不会漏计。
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any

from app.models import AggregateRow


def insert_event(
    conn: sqlite3.Connection,
    *,
    event_id: str,
    event_type: str,
    subject: str,
    ts_ms: int,
    bucket: str,
    normalized: Any,
    created_at_ms: int,
) -> bool:
    """插入一条已归一化事件。返回 False 表示 event_id 冲突（重复提交）。"""
    try:
        conn.execute(
            """
            INSERT INTO events
                (event_id, event_type, subject, ts_ms, bucket,
                 payload, schema_version, created_at_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                event_type,
                subject,
                ts_ms,
                bucket,
                json.dumps(
                    {"values": normalized.values, "extras": normalized.extras},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                normalized.schema_version,
                created_at_ms,
            ),
        )
    except sqlite3.IntegrityError:
        return False
    return True


def upsert_counter(
    conn: sqlite3.Connection,
    *,
    bucket: str,
    event_type: str,
    subject: str,
    amount: float,
) -> None:
    conn.execute(
        """
        INSERT INTO counters (bucket, event_type, subject, cnt, amount_sum)
        VALUES (?, ?, ?, 1, ?)
        ON CONFLICT(bucket, event_type, subject) DO UPDATE SET
            cnt = cnt + 1,
            amount_sum = amount_sum + excluded.amount_sum
        """,
        (bucket, event_type, subject, float(amount)),
    )


def query_aggregates(
    conn: sqlite3.Connection,
    *,
    event_type: str | None,
    subject: str | None,
    settled_only: bool,
) -> list[AggregateRow]:
    sql = (
        "SELECT c.bucket, c.event_type, c.subject, c.cnt, c.amount_sum, "
        "       CASE WHEN s.bucket IS NULL THEN 0 ELSE 1 END AS settled "
        "FROM counters c LEFT JOIN settled_windows s ON s.bucket = c.bucket"
    )
    where: list[str] = []
    params: list[Any] = []
    if event_type is not None:
        where.append("c.event_type = ?")
        params.append(event_type)
    if subject is not None:
        where.append("c.subject = ?")
        params.append(subject)
    if settled_only:
        where.append("s.bucket IS NOT NULL")
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY c.bucket, c.event_type, c.subject"
    return [
        AggregateRow(
            bucket=r["bucket"],
            event_type=r["event_type"],
            subject=r["subject"],
            count=r["cnt"],
            amount_sum=r["amount_sum"],
        )
        for r in conn.execute(sql, params).fetchall()
    ]
