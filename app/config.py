"""本地配置：数据库路径、窗口参数、写入令牌。

全部来自环境变量，带本地默认值；不依赖任何外部服务或新凭据。
- EVENT_DB_PATH：SQLite 文件路径，默认 events.db；:memory: 仅用于测试。
- EVENT_WINDOW_MS / EVENT_SETTLE_LAG_MS：窗口与水位线参数。
- EVENT_WRITE_TOKEN：若设置，则写接口必须携带 X-Write-Token 与之匹配。
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    db_path: str = "events.db"
    window_size_ms: int = 60_000
    settle_lag_ms: int = 5_000
    write_token: str | None = None


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def load_settings() -> Settings:
    token = os.environ.get("EVENT_WRITE_TOKEN") or None
    return Settings(
        db_path=os.environ.get("EVENT_DB_PATH", "events.db"),
        window_size_ms=_int_env("EVENT_WINDOW_MS", 60_000),
        settle_lag_ms=_int_env("EVENT_SETTLE_LAG_MS", 5_000),
        write_token=token,
    )
