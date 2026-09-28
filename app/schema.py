"""本地 SQLite 的建表语句与连接管理。

设计要点（只依赖标准库 sqlite3，无外部服务）：
- events.event_id 为主键：重复提交在数据库层即被拒绝，是幂等的最终保障。
- counters 按 (bucket, event_type, subject) 维护计数，与事件写入在同一事务内更新。
- settled_windows 记录已结算窗口，已结算窗口拒绝后续改写。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS events (
    event_id       TEXT PRIMARY KEY,
    event_type     TEXT NOT NULL,
    subject        TEXT NOT NULL,
    ts_ms          INTEGER NOT NULL,
    bucket         TEXT NOT NULL,
    payload        TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    created_at_ms  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_bucket
    ON events (bucket, event_type, subject);

CREATE TABLE IF NOT EXISTS counters (
    bucket     TEXT NOT NULL,
    event_type TEXT NOT NULL,
    subject    TEXT NOT NULL,
    cnt        INTEGER NOT NULL DEFAULT 0,
    amount_sum REAL    NOT NULL DEFAULT 0.0,
    PRIMARY KEY (bucket, event_type, subject)
);

CREATE TABLE IF NOT EXISTS settled_windows (
    bucket        TEXT PRIMARY KEY,
    settled_at_ms INTEGER NOT NULL
);
"""


def open_db(path: str | Path) -> sqlite3.Connection:
    """打开（必要时创建）本地数据库并设置并发相关 PRAGMA。

    - WAL：允许一个写者与多个读者并发，读写互不长期阻塞。
    - busy_timeout：锁竞争时等待而不是立即报错。
    - foreign_keys：开启外键约束。
    """
    # check_same_thread=False：服务层以 RLock 串行化所有连接使用，
    # 允许线程池中的工作线程复用同一连接。
    conn = sqlite3.connect(
        str(path), timeout=30.0, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """幂等建表，可重复执行。"""
    conn.executescript(SCHEMA_SQL)
