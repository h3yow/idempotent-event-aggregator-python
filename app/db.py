"""SQLite connection management and schema bootstrap.

The database is a plain local SQLite file (``events.db`` by default).
WAL plus a process-wide write lock make concurrent readers and writers
behave deterministically: readers never block, writers are serialized
in a well-defined order (see ``app/store.py``).
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

SCHEMA_VERSION = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id       TEXT    NOT NULL,
    tenant         TEXT    NOT NULL,
    event_type     TEXT    NOT NULL,
    occurred_at    INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    value          REAL,
    raw_json       TEXT    NOT NULL,
    PRIMARY KEY (tenant, event_id)
);

CREATE INDEX IF NOT EXISTS idx_events_window
    ON events (tenant, event_type, occurred_at);

CREATE TABLE IF NOT EXISTS window_aggregates (
    tenant       TEXT    NOT NULL,
    event_type   TEXT    NOT NULL,
    window_start INTEGER NOT NULL,
    count        INTEGER NOT NULL,
    sum_value    REAL    NOT NULL,
    versions     TEXT    NOT NULL,
    settled_at   INTEGER NOT NULL,
    PRIMARY KEY (tenant, event_type, window_start)
);

CREATE TABLE IF NOT EXISTS settlement_watermarks (
    tenant             TEXT PRIMARY KEY,
    watermark_ms       INTEGER NOT NULL,
    snapshot_frontier  INTEGER NOT NULL DEFAULT 0
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    """Open a configured SQLite connection.

    ``WAL`` allows concurrent readers while a write transaction is open.
    ``busy_timeout`` makes lock contention wait instead of failing hard;
    the explicit write lock in the store prevents duplicate-key races
    from reaching SQL in the first place.
    """
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        db_path,
        timeout=5.0,
        isolation_level=None,  # explicit transaction control
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables if missing (idempotent across restarts)."""
    conn.executescript(_SCHEMA)
    conn.execute(
        "PRAGMA user_version = %d" % SCHEMA_VERSION
    ) if conn.execute("PRAGMA user_version").fetchone()[0] == 0 else None


class Database:
    """Owns the file path and the process-wide writer lock."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.write_lock = threading.RLock()

    def connect(self) -> sqlite3.Connection:
        conn = connect(self.db_path)
        init_db(conn)
        return conn
