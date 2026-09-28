"""请求/响应与事件的数据模型。"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class IngestRequest(BaseModel):
    event_id: str = ""
    event_type: str = ""
    subject: str = ""
    ts_ms: int = 0
    payload: dict[str, Any] = Field(default_factory=dict)


class IngestResponse(BaseModel):
    accepted: bool = False
    event_id: str = ""
    deduped: bool = False
    bucket: str = ""


class AggregateRow(BaseModel):
    bucket: str = ""
    event_type: str = ""
    subject: str = ""
    count: int = 0
    amount_sum: float | None = None


class AggregateResponse(BaseModel):
    settled: bool = False
    rows: list[AggregateRow] = []
