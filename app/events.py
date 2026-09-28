"""Event payload validation with compatible schema evolution.

Two schema versions are understood:

* ``v1`` (implicit when ``schema_version`` is absent): fields
  ``event_id``, ``event_type``, ``occurred_at`` are required.
* ``v2``: adds the optional numeric ``value`` field used by metric
  aggregates.

Evolution is *additive*: v1 records continue to validate forever, and
missing new fields are represented as ``None`` rather than dropped.
Unknown fields are tolerated (forward compatibility) but retained in
the raw payload. Malformed records are rejected with a precise,
distinct reason — never silently discarded.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from app.errors import InvalidEventError

REQUIRED_FIELDS = ("event_id", "event_type", "occurred_at")
SUPPORTED_VERSIONS = (1, 2)


@dataclass(frozen=True)
class ValidatedEvent:
    event_id: str
    tenant: str
    event_type: str
    occurred_at: int
    schema_version: int
    value: float | None
    raw: dict


def _reject(reason: str, message: str, **details: object) -> None:
    raise InvalidEventError(message, details={"reason": reason, **details})


def _check_type(value: object, expected: type, field: str, reason: str) -> None:
    # bool is a subtype of int in Python; reject it explicitly for
    # numeric/string fields so type mismatches stay unambiguous.
    if expected in (int, float) and isinstance(value, bool):
        _reject(
            reason,
            f"field '{field}' has wrong type: expected {expected.__name__}, "
            "got boolean",
            field=field,
            actual=type(value).__name__,
        )
    if expected is int and isinstance(value, int):
        return
    if expected is float and isinstance(value, (int, float)):
        return
    if expected is str and isinstance(value, str):
        return
    _reject(
        reason,
        f"field '{field}' has wrong type: expected {getattr(expected, '__name__', expected)}",
        field=field,
        actual=type(value).__name__,
    )


def parse_event(payload: object, *, tenant: str) -> ValidatedEvent:
    """Validate and normalize one event payload for ``tenant``.

    Raises :class:`InvalidEventError` whose ``details['reason']`` is one
    of: ``not_object``, ``missing_field``, ``invalid_type``,
    ``invalid_schema_version``, ``invalid_timestamp``.
    """
    if not isinstance(payload, dict):
        _reject(
            "not_object",
            "event payload must be a JSON object",
            actual=type(payload).__name__,
        )

    for field in REQUIRED_FIELDS:
        if field not in payload:
            _reject(
                "missing_field",
                f"required field '{field}' is missing",
                field=field,
            )

    event_id = payload["event_id"]
    _check_type(event_id, str, "event_id", "invalid_type")
    if not event_id.strip():
        _reject("invalid_type", "field 'event_id' must be non-empty", field="event_id")

    event_type = payload["event_type"]
    _check_type(event_type, str, "event_type", "invalid_type")
    if not event_type.strip():
        _reject("invalid_type", "field 'event_type' must be non-empty", field="event_type")

    occurred_at = payload["occurred_at"]
    _check_type(occurred_at, int, "occurred_at", "invalid_type")
    if occurred_at < 0:
        _reject(
            "invalid_timestamp",
            "field 'occurred_at' must be a non-negative unix-ms timestamp",
            occurred_at=occurred_at,
        )

    if "schema_version" in payload and payload["schema_version"] is not None:
        version = payload["schema_version"]
        if isinstance(version, bool) or not isinstance(version, int):
            _reject(
                "invalid_schema_version",
                "field 'schema_version' must be an integer",
                actual=type(version).__name__,
            )
        if version not in SUPPORTED_VERSIONS:
            _reject(
                "invalid_schema_version",
                f"unsupported schema_version {version}; "
                f"supported: {list(SUPPORTED_VERSIONS)}",
                schema_version=version,
            )
        schema_version = version
    else:
        # Absent (or explicit null) means the original v1 schema.
        schema_version = 1

    value: float | None = None
    if "value" in payload:
        raw_value = payload["value"]
        if raw_value is None:
            value = None
        else:
            _check_type(raw_value, float, "value", "invalid_type")
            value = float(raw_value)
    elif schema_version >= 2:
        # v2 producers may omit the new additive field: it defaults to
        # absent (None) and the record still counts normally.
        value = None

    normalized = dict(payload)
    normalized["schema_version"] = schema_version

    return ValidatedEvent(
        event_id=event_id,
        tenant=tenant,
        event_type=event_type,
        occurred_at=int(occurred_at),
        schema_version=schema_version,
        value=value,
        raw=normalized,
    )


def serialize_raw(event: ValidatedEvent) -> str:
    return json.dumps(event.raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
