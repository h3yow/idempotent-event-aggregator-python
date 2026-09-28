"""Error taxonomy for event ingestion.

Every rejected write is classified by a stable ``code`` so callers can
distinguish failure causes without parsing messages.
"""

from __future__ import annotations


class IngestionError(Exception):
    """Base class for all classified ingestion rejections."""

    code: str = "ingestion_error"
    http_status: int = 400

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        http_status: int | None = None,
        details: dict | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code
        if http_status is not None:
            self.http_status = http_status
        self.details = details or {}


class UnauthorizedError(IngestionError):
    code = "unauthorized"
    http_status = 401


class ForbiddenError(IngestionError):
    code = "forbidden"
    http_status = 403


class InvalidEventError(IngestionError):
    code = "invalid_event"
    http_status = 422


class DuplicateEventError(IngestionError):
    code = "duplicate_event"
    http_status = 409


class WindowSettledError(IngestionError):
    code = "window_settled"
    http_status = 409
