"""Local configuration: static tenant tokens and window settings.

No external services or new credentials are involved. Tokens are read
from a local JSON file (shipped at ``app/tokens.json``) and can be
overridden with the ``EVENT_TOKENS_PATH`` environment variable. The
window length defaults to 3600s and can be overridden with
``EVENT_WINDOW_SECONDS``.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

DEFAULT_TOKENS_PATH = os.environ.get(
    "EVENT_TOKENS_PATH",
    os.path.join(os.path.dirname(__file__), "tokens.json"),
)
DEFAULT_WINDOW_SECONDS = 3600


@dataclass(frozen=True)
class Settings:
    window_seconds: int
    tokens_path: str


def get_settings() -> Settings:
    raw = os.environ.get("EVENT_WINDOW_SECONDS", str(DEFAULT_WINDOW_SECONDS))
    window_seconds = int(raw)
    if window_seconds <= 0:
        raise ValueError("EVENT_WINDOW_SECONDS must be positive")
    return Settings(
        window_seconds=window_seconds,
        tokens_path=os.environ.get("EVENT_TOKENS_PATH", DEFAULT_TOKENS_PATH),
    )


def load_tokens(path: str) -> dict[str, str]:
    """Return a mapping of bearer token -> tenant_id.

    The file format is a flat ``{token: tenant_id}`` object.
    """
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"tokens file {path} must contain a JSON object")
    tokens: dict[str, str] = {}
    for token, tenant in data.items():
        if not isinstance(token, str) or not isinstance(tenant, str):
            raise ValueError("token keys and tenant values must be strings")
        tokens[token] = tenant
    return tokens
