"""Transactional event store: idempotent writes and window settlement.

Concurrency model
-----------------
* Every write transaction is opened with ``BEGIN IMMEDIATE``, which
  acquires SQLite's reserved (write) lock up front; combined with the
  process-wide ``RLock`` this serializes writers in a deterministic
  arrival order. Readers (WAL) never block writers.
* Duplicate detection (``SELECT``) and ``INSERT`` happen inside the
  same locked transaction, so two concurrent submissions of the same
  ``event_id`` can never both pass the check.
* Batch writes are all-or-nothing: any rejected event aborts the
  transaction (``ROLLBACK``), leaving zero rows inserted.

Window semantics
----------------
* Windows are fixed-size, aligned to epoch:
  ``window_start = occurred_at // window_seconds * window_seconds``.
* An admin ``settle`` call freezes every strictly-closed window before
  a watermark: aggregates are snapshotted into ``window_aggregates`` and
  the tenant's watermark advances. Events arriving afterwards for a
  frozen window are rejected with ``window_settled``; an *identical*
  re-submission of an already-stored event stays idempotent
  (``duplicate_event``) and never mutates the frozen snapshot.
"""

from __future__ import annotations

import json
import sqlite3
import time

from app.errors import DuplicateEventError, WindowSettledError
from app.events import ValidatedEvent, serialize_raw


class EventStore:
    def __init__(self, conn: sqlite3.Connection, window_seconds: int) -> None:
        self._conn = conn
        self._window_seconds = window_seconds

    def window_start(self, occurred_at: int) -> int:
        return (occurred_at // self._window_seconds) * self._window_seconds

    # ------------------------------------------------------------------
    # writes
    # ------------------------------------------------------------------
    def insert_events(
        self,
        events: list[ValidatedEvent],
        *,
        write_lock,
    ) -> dict:
        """Insert events idempotently in one transaction.

        Returns a summary ``{"inserted": n, "duplicates": [...]}``.

        Raises on the first bad event; the whole batch is rolled back.
        """
        if not events:
            return {"inserted": 0, "duplicates": []}

        tenants = {e.tenant for e in events}
        if len(tenants) != 1:
            # Authorization is per-request; a batch never mixes tenants.
            raise ValueError("all events in a batch must share one tenant")
        tenant = tenants.pop()

        inserted: list[str] = []
        duplicates: list[dict] = []
        # Duplicate within the same batch must also be rejected: the
        # whole batch is ambiguous otherwise.
        seen_in_batch: set[str] = set()

        with write_lock:
            conn = self._conn
            try:
                conn.execute("BEGIN IMMEDIATE")

                watermark_row = conn.execute(
                    "SELECT watermark_ms FROM settlement_watermarks WHERE tenant = ?",
                    (tenant,),
                ).fetchone()
                watermark = watermark_row[0] if watermark_row else -1

                for event in events:
                    if event.event_id in seen_in_batch:
                        raise DuplicateEventError(
                            f"event_id '{event.event_id}' submitted twice "
                            "within the same batch",
                            details={
                                "reason": "duplicate_in_batch",
                                "event_id": event.event_id,
                                "tenant": tenant,
                            },
                        )
                    seen_in_batch.add(event.event_id)

                    window_start = self.window_start(event.occurred_at)
                    existing = conn.execute(
                        "SELECT raw_json FROM events "
                        "WHERE tenant = ? AND event_id = ?",
                        (tenant, event.event_id),
                    ).fetchone()
                    if existing is not None:
                        # Idempotency: a byte-identical re-submission is
                        # acknowledged as a duplicate and never counted
                        # twice — even when its window has been settled.
                        if existing[0] != serialize_raw(event):
                            raise DuplicateEventError(
                                f"event_id '{event.event_id}' already exists "
                                "with a different payload",
                                details={
                                    "reason": "event_id_conflict",
                                    "event_id": event.event_id,
                                    "tenant": tenant,
                                },
                            )
                        duplicates.append(
                            {"event_id": event.event_id, "reason": "already_stored"}
                        )
                        continue

                    # New event id — but the event timestamp may fall in
                    # the already-settled region. Settlement freezes
                    # exactly the events with occurred_at < watermark, so
                    # that is the precise rejection boundary (window-
                    # aligned and independent of batch window algebra).
                    if watermark >= 0 and event.occurred_at < watermark:
                        raise WindowSettledError(
                            f"window starting at {window_start} is already "
                            "settled; late events cannot rewrite it",
                            details={
                                "reason": "window_settled",
                                "event_id": event.event_id,
                                "window_start": window_start,
                                "watermark_ms": watermark,
                            },
                        )

                    conn.execute(
                        "INSERT INTO events (event_id, tenant, event_type, "
                        "occurred_at, schema_version, value, raw_json) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            event.event_id,
                            tenant,
                            event.event_type,
                            event.occurred_at,
                            event.schema_version,
                            event.value,
                            serialize_raw(event),
                        ),
                    )
                    inserted.append(event.event_id)

                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

        return {
            "inserted": len(inserted),
            "inserted_ids": inserted,
            "duplicates": duplicates,
        }

    # ------------------------------------------------------------------
    # settlement
    # ------------------------------------------------------------------
    def settle_windows(
        self,
        tenant: str,
        watermark_ms: int,
        *,
        write_lock,
    ) -> dict:
        """Freeze every fully-closed window ending before ``watermark_ms``.

        Windows where ``window_start + window_seconds <= watermark_ms``
        are snapshotted. Already-snapshotted windows are left untouched,
        so settlement is itself idempotent.
        """
        now_ms = int(time.time() * 1000)
        frozen: list[dict] = []

        with write_lock:
            conn = self._conn
            try:
                conn.execute("BEGIN IMMEDIATE")

                row = conn.execute(
                    "SELECT watermark_ms, snapshot_frontier "
                    "FROM settlement_watermarks WHERE tenant = ?",
                    (tenant,),
                ).fetchone()
                current = row[0] if row else -1
                frontier = row[1] if row else 0
                if watermark_ms < current:
                    raise WindowSettledError(
                        "watermark cannot move backwards",
                        details={
                            "reason": "watermark_regression",
                            "current_watermark_ms": current,
                            "requested_watermark_ms": watermark_ms,
                        },
                    )

                # Only windows fully closed at *some past* frontier are
                # snapshotted. We advance the snapshot frontier in whole
                # window increments up to (at most) the new watermark,
                # so a partially-open window is never frozen early.
                new_frontier = min(
                    watermark_ms,
                    (watermark_ms // self._window_seconds)
                    * self._window_seconds,
                )

                agg_rows = conn.execute(
                    "SELECT event_type, "
                    "(occurred_at / ?) * ? AS window_start, "
                    "COUNT(*) AS cnt, "
                    "COALESCE(SUM(COALESCE(value, 0.0)), 0.0) AS sum_value, "
                    "GROUP_CONCAT(DISTINCT schema_version) AS versions "
                    "FROM events WHERE tenant = ? "
                    "AND occurred_at >= ? AND occurred_at < ? "
                    "GROUP BY event_type, window_start",
                    (
                        self._window_seconds,
                        self._window_seconds,
                        tenant,
                        frontier,
                        new_frontier,
                    ),
                ).fetchall()

                for r in agg_rows:
                    existing = conn.execute(
                        "SELECT 1 FROM window_aggregates "
                        "WHERE tenant = ? AND event_type = ? AND window_start = ?",
                        (tenant, r["event_type"], r["window_start"]),
                    ).fetchone()
                    if existing is not None:
                        # Frozen already; the snapshot is immutable.
                        continue
                    versions = sorted(
                        int(v) for v in (r["versions"] or "").split(",") if v
                    )
                    conn.execute(
                        "INSERT INTO window_aggregates (tenant, event_type, "
                        "window_start, count, sum_value, versions, settled_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            tenant,
                            r["event_type"],
                            r["window_start"],
                            r["cnt"],
                            r["sum_value"],
                            json.dumps(versions),
                            now_ms,
                        ),
                    )
                    frozen.append(
                        {
                            "event_type": r["event_type"],
                            "window_start": r["window_start"],
                            "count": r["cnt"],
                            "sum_value": r["sum_value"],
                        }
                    )

                conn.execute(
                    "INSERT INTO settlement_watermarks "
                    "(tenant, watermark_ms, snapshot_frontier) "
                    "VALUES (?, ?, ?) "
                    "ON CONFLICT(tenant) DO UPDATE SET "
                    "watermark_ms = excluded.watermark_ms, "
                    "snapshot_frontier = excluded.snapshot_frontier",
                    (tenant, watermark_ms, new_frontier),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

        return {
            "watermark_ms": watermark_ms,
            "snapshot_frontier_ms": new_frontier,
            "frozen_windows": frozen,
        }
