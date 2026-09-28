"""可区分的错误分类。"""
from __future__ import annotations

from enum import Enum


class RejectReason(str, Enum):
    DUPLICATE = "DUPLICATE"
    WINDOW_SETTLED = "WINDOW_SETTLED"
    UNAUTHORIZED = "UNAUTHORIZED"
    BAD_PAYLOAD = "BAD_PAYLOAD"
    MISSING_FIELD = "MISSING_FIELD"
    TYPE_MISMATCH = "TYPE_MISMATCH"
    UNKNOWN_EVENT_TYPE = "UNKNOWN_EVENT_TYPE"


class RejectError(Exception):
    def __init__(self, reason: RejectReason, message: str):
        self.reason = reason
        self.message = message
        super().__init__(f"{reason.value}: {message}")


class DuplicateEventError(RejectError):
    pass
