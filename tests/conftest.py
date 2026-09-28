"""Shared pytest fixtures.

Each test run uses an isolated temporary SQLite file and a fixed local
token file — no external services, no real credentials.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def app_env(tmp_path: Path) -> tuple[Path, Path]:
    tokens_path = tmp_path / "tokens.json"
    tokens_path.write_text(
        json.dumps(
            {
                "token-a": "tenant-a",
                "token-b": "tenant-b",
                "token-admin": "tenant-admin",
            }
        ),
        encoding="utf-8",
    )
    db_path = tmp_path / "events.db"
    os.environ["EVENT_TOKENS_PATH"] = str(tokens_path)
    os.environ["EVENT_DB_PATH"] = str(db_path)
    os.environ["EVENT_WINDOW_SECONDS"] = "1000"

    # Reload modules that captured settings at import/create time.
    import importlib

    import app.config
    import app.api

    importlib.reload(app.config)
    importlib.reload(app.api)

    yield tokens_path, db_path


@pytest.fixture()
def client(app_env):
    from app.api import create_app

    app = create_app(str(app_env[1]))
    with TestClient(app) as c:
        c.app = app  # type: ignore[attr-defined]
        yield c


def log_case(caplog, title: str, payload: object, decision: str) -> None:
    """Print the inputs and the decision basis into test logs."""
    logging.getLogger("event_aggregator.test").info(
        "CASE %s | input=%s | decision=%s", title, payload, decision
    )
    caplog.records and None  # keep caplog referenced for fixtures using it
