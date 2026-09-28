"""FastAPI 路由：把错误分类映射为可区分的 HTTP 响应。

映射约定（错误分类规则）：
- DUPLICATE         -> 200（成功，deduped=true），重复提交不是错误
- WINDOW_SETTLED    -> 409
- UNAUTHORIZED      -> 401
- MISSING_FIELD / TYPE_MISMATCH / BAD_PAYLOAD / UNKNOWN_EVENT_TYPE -> 422
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

from app.config import load_settings
from app.errors import RejectError, RejectReason
from app.models import AggregateResponse, IngestResponse
from app.service import EventService

logger = logging.getLogger("event_aggregator")

_STATUS = {
    RejectReason.WINDOW_SETTLED: 409,
    RejectReason.UNAUTHORIZED: 401,
    RejectReason.BAD_PAYLOAD: 422,
    RejectReason.MISSING_FIELD: 422,
    RejectReason.TYPE_MISMATCH: 422,
    RejectReason.UNKNOWN_EVENT_TYPE: 422,
}


def create_app(service: EventService | None = None) -> FastAPI:
    settings = load_settings()
    if service is None:
        service = EventService(settings.db_path, write_token=settings.write_token)

    app = FastAPI(title="idempotent-event-aggregator")
    app.state.service = service

    @app.exception_handler(RejectError)
    async def reject_handler(_: Request, exc: RejectError) -> JSONResponse:
        logger.warning("request rejected reason=%s message=%s", exc.reason.value, exc.message)
        return JSONResponse(
            status_code=_STATUS[exc.reason],
            content={"accepted": False, "reason": exc.reason.value, "message": exc.message},
        )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/events", response_model=IngestResponse)
    async def ingest(
        request: Request,
        x_write_token: str | None = Header(default=None),
    ) -> IngestResponse:
        raw: Any = await request.json()
        return service.ingest(raw, token=x_write_token)

    @app.post("/settle")
    def settle() -> dict[str, Any]:
        return {"settled_buckets": service.settle()}

    @app.get("/aggregates", response_model=AggregateResponse)
    def aggregates(
        event_type: str | None = None,
        subject: str | None = None,
        settled_only: bool = True,
    ) -> AggregateResponse:
        return service.aggregates(
            event_type=event_type, subject=subject, settled_only=settled_only
        )

    return app
