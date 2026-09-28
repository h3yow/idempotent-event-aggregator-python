"""应用服务：编排鉴权、信封校验、结构校验、去重、窗口与事务写入。

并发一致性说明（可解释的并发与顺序）：
- 每次写入使用 ``BEGIN IMMEDIATE`` 立即获取 SQLite 保留锁：并发写请求被
  串行化为确定的全序，后到者在 busy_timeout 内等待，不会出现丢失更新。
- “去重检查 + 插入事件 + 更新计数”在同一事务内完成：任何一步失败整体回滚，
  不会出现事件已存但计数未加、或计数多加的中间态。
- 读取（聚合）走 WAL 的快照读，不加写锁，可与写入并发；读到的始终是某个
  已提交快照，绝不会读到半个事务。
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Callable

from app import dedup, repository, windows
from app.errors import RejectError, RejectReason
from app.models import AggregateResponse, AggregateRow, IngestResponse
from app.schema import init_db, open_db
from app.validation import validate_event

logger = logging.getLogger("event_aggregator")


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def validate_envelope(raw: Any) -> dict[str, Any]:
    """校验事件信封的必填字段及类型，拒绝原因可区分。"""
    if not isinstance(raw, dict):
        raise RejectError(RejectReason.BAD_PAYLOAD, f"请求体必须是对象，实际为 {type(raw).__name__}")

    required = ("event_id", "event_type", "subject", "ts_ms")
    for key in required:
        if key not in raw or raw[key] is None:
            raise RejectError(RejectReason.MISSING_FIELD, f"缺少必填字段 {key!r}")

    event_id = raw["event_id"]
    event_type = raw["event_type"]
    subject = raw["subject"]
    ts_ms = raw["ts_ms"]

    if not isinstance(event_id, str) or not event_id.strip():
        raise RejectError(RejectReason.TYPE_MISMATCH, "event_id 必须是非空字符串")
    if not isinstance(event_type, str) or not event_type.strip():
        raise RejectError(RejectReason.TYPE_MISMATCH, "event_type 必须是非空字符串")
    if not isinstance(subject, str) or not subject.strip():
        raise RejectError(RejectReason.TYPE_MISMATCH, "subject 必须是非空字符串")
    # bool 是 int 的子类型，显式排除
    if isinstance(ts_ms, bool) or not isinstance(ts_ms, int):
        raise RejectError(RejectReason.TYPE_MISMATCH, f"ts_ms 必须是整数，实际为 {type(ts_ms).__name__}")
    if ts_ms < 0:
        raise RejectError(RejectReason.BAD_PAYLOAD, "ts_ms 不能为负数")

    payload = raw.get("payload", {})
    if not isinstance(payload, dict):
        raise RejectError(RejectReason.TYPE_MISMATCH, f"payload 必须是对象，实际为 {type(payload).__name__}")

    return {
        "event_id": event_id,
        "event_type": event_type,
        "subject": subject,
        "ts_ms": ts_ms,
        "payload": payload,
    }


class EventService:
    def __init__(
        self,
        db_path: str | Path = ":memory:",
        *,
        write_token: str | None = None,
        clock: "Callable[[], int] | None" = None,
    ):
        self._lock = threading.RLock()
        self._conn = open_db(db_path)
        init_db(self._conn)
        self._write_token = write_token
        self._clock = clock if clock is not None else _now_ms

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _authorize(self, token: str | None) -> None:
        if self._write_token is not None and token != self._write_token:
            raise RejectError(RejectReason.UNAUTHORIZED, "缺少或错误的 X-Write-Token")

    def ingest(self, raw: Any, *, token: str | None = None, now_ms: int | None = None) -> IngestResponse:
        env = validate_envelope(raw)
        self._authorize(token)

        event_id = env["event_id"]
        event_type = env["event_type"]
        subject = env["subject"]
        ts_ms = env["ts_ms"]
        payload = env["payload"]

        # 结构演进校验：缺省补默认值、类型不符显式拒绝
        normalized = validate_event(event_type, payload)
        amount = float(normalized.values.get("amount", 0.0) or 0.0)
        bucket = windows.bucket_of(ts_ms)
        current = now_ms if now_ms is not None else self._clock()

        with self._lock:
            conn = self._conn
            try:
                conn.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:
                raise RejectError(RejectReason.BAD_PAYLOAD, f"获取写锁失败: {exc}") from exc
            try:
                # 先查重，使判定依据可解释（最终仍以主键冲突兜底）
                existing = dedup.find_event(conn, event_id)
                if existing is not None:
                    same = dedup.same_fingerprint(
                        existing, event_type=event_type, subject=subject, ts_ms=ts_ms
                    )
                    conn.execute("COMMIT")
                    logger.info(
                        "ingest reject input=%r reason=%s fingerprint_match=%s existing=%r",
                        env, RejectReason.DUPLICATE.value, same, existing,
                    )
                    return IngestResponse(
                        accepted=True, event_id=event_id, deduped=True,
                        bucket=existing["bucket"],
                    )

                # 已结算窗口拒绝改写（迟到事件只能回补未结算窗口）
                if windows.is_settled(conn, bucket, current):
                    conn.execute("ROLLBACK")
                    logger.info(
                        "ingest reject input=%r reason=%s bucket=%s now_ms=%s",
                        env, RejectReason.WINDOW_SETTLED.value, bucket, current,
                    )
                    raise RejectError(
                        RejectReason.WINDOW_SETTLED,
                        f"窗口 {bucket} 已结算，拒绝写入事件 {event_id}",
                    )

                inserted = repository.insert_event(
                    conn,
                    event_id=event_id,
                    event_type=event_type,
                    subject=subject,
                    ts_ms=ts_ms,
                    bucket=bucket,
                    normalized=normalized,
                    created_at_ms=current,
                )
                if not inserted:  # 极端并发下的主键冲突兜底
                    conn.execute("ROLLBACK")
                    raise RejectError(RejectReason.DUPLICATE, f"事件 {event_id} 已存在")

                repository.upsert_counter(
                    conn,
                    bucket=bucket,
                    event_type=event_type,
                    subject=subject,
                    amount=amount,
                )
                conn.execute("COMMIT")
            except Exception:
                # 任何失败整体回滚，绝不留下半完成状态
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise

        logger.info("ingest accept input=%r bucket=%s schema_version=%s amount=%s",
                    env, bucket, normalized.schema_version, amount)
        return IngestResponse(accepted=True, event_id=event_id, deduped=False, bucket=bucket)

    def settle(self, *, now_ms: int | None = None) -> list[str]:
        current = now_ms if now_ms is not None else self._clock()
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                settled = windows.settle_pending(conn, current)
                conn.execute("COMMIT")
            except Exception:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
        if settled:
            logger.info("settle now_ms=%s buckets=%r", current, settled)
        return settled

    def aggregates(
        self,
        *,
        event_type: str | None = None,
        subject: str | None = None,
        settled_only: bool = True,
    ) -> AggregateResponse:
        with self._lock:
            rows = repository.query_aggregates(
                self._conn,
                event_type=event_type,
                subject=subject,
                settled_only=settled_only,
            )
        logger.info(
            "aggregates query event_type=%r subject=%r settled_only=%s rows=%d",
            event_type, subject, settled_only, len(rows),
        )
        return AggregateResponse(settled=settled_only, rows=rows)
