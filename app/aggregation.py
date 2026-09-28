"""Aggregation queries over frozen snapshots and live events.

For every queried window exactly one source answers:

* ``settled`` windows are served from the immutable
  ``window_aggregates`` snapshot, so repeated queries return bit-stable
  results even though the underlying ``events`` rows remain for audit.
* ``open`` windows are aggregated live from ``events`` (they accept
  late arrivals up to the next settlement).

The two sources can never overlap for a tenant, because snapshots are
only taken for windows fully before the settlement watermark while live
aggregation only counts events at/after it.
"""

from __future__ import annotations

import json
import sqlite3


def _frontiers(conn: sqlite3.Connection, tenant: str) -> tuple[int, int]:
    """Return (write_watermark, snapshot_frontier) for a tenant."""
    row = conn.execute(
        "SELECT watermark_ms, snapshot_frontier "
        "FROM settlement_watermarks WHERE tenant = ?",
        (tenant,),
    ).fetchone()
    if row is None:
        return 0, 0
    return row[0], row[1]


def query_aggregates(
    conn: sqlite3.Connection,
    *,
    tenant: str,
    window_seconds: int,
    event_type: str | None = None,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> list[dict]:
    """Return per-(event_type, window) aggregates for ``tenant``.

    Time filters operate on window starts:
    ``start_ms <= window_start < end_ms``.
    """
    results: list[dict] = []
    _wm, snapshot_frontier = _frontiers(conn, tenant)
    start_ms = -1 if start_ms is None else start_ms
    # SQLite signed INTEGER max is 2**63-1; stay one below it.
    end_ms = 2**63 - 1 if end_ms is None else end_ms

    type_clause = "AND event_type = ?" if event_type is not None else ""
    type_params: tuple = (event_type,) if event_type is not None else ()

    # ---- settled: read-only snapshots --------------------------------
    settled_rows = conn.execute(
        "SELECT event_type, window_start, count, sum_value, versions, settled_at "
        "FROM window_aggregates "
        "WHERE tenant = ? "
        + type_clause
        + " AND window_start >= ? AND window_start < ? "
        "ORDER BY event_type, window_start",
        (tenant, *type_params, start_ms, end_ms),
    ).fetchall()
    for r in settled_rows:
        results.append(
            {
                "event_type": r["event_type"],
                "window_start": r["window_start"],
                "window_end": r["window_start"] + window_seconds,
                "count": r["count"],
                "sum_value": r["sum_value"],
                "schema_versions": json.loads(r["versions"]),
                "state": "settled",
                "settled_at": r["settled_at"],
            }
        )

    # ---- open: live aggregation of post-watermark events --------------
    open_rows = conn.execute(
        "SELECT event_type, "
        "(occurred_at / ?) * ? AS window_start, "
        "COUNT(*) AS cnt, "
        "COALESCE(SUM(COALESCE(value, 0.0)), 0.0) AS sum_value, "
        "GROUP_CONCAT(DISTINCT schema_version) AS versions "
        "FROM events "
        "WHERE tenant = ? AND occurred_at >= ? "
        + type_clause
        + " GROUP BY event_type, window_start "
        "HAVING window_start >= ? AND window_start < ? "
        "ORDER BY event_type, window_start",
        (
            window_seconds,
            window_seconds,
            tenant,
            snapshot_frontier,
            *type_params,
            start_ms,
            end_ms,
        ),
    ).fetchall()
    for r in open_rows:
        versions = sorted(int(v) for v in (r["versions"] or "").split(",") if v)
        results.append(
            {
                "event_type": r["event_type"],
                "window_start": r["window_start"],
                "window_end": r["window_start"] + window_seconds,
                "count": r["cnt"],
                "sum_value": r["sum_value"],
                "schema_versions": versions,
                "state": "open",
                "settled_at": None,
            }
        )

    results.sort(key=lambda d: (d["event_type"], d["window_start"]))
    return results
