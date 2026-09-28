"""FastAPI application wiring and HTTP layer.

Endpoints
---------
* ``POST /events``          ingest one event or a JSON array (atomic).
* ``POST /admin/settle``    advance the settlement watermark (admin).
* ``GET  /aggregates``      query window aggregates.

Tenant identity comes only from the local bearer token; payloads
themselves are never trusted for tenant/authorization decisions.
"""

from __future__ import annotations

import logging
import os

from fastapi import Depends, FastAPI, Header, Request
from fastapi.responses import JSONResponse

from app.aggregation import query_aggregates
from app.config import Settings, get_settings, load_tokens
from app.db import Database
from app.errors import (
    ForbiddenError,
    IngestionError,
    UnauthorizedError,
)
from app.events import parse_event
from app.store import EventStore

logger = logging.getLogger("event_aggregator")

ADMIN_TENANT = "tenant-admin"


def create_app(db_path: str | None = None) -> FastAPI:
    settings: Settings = get_settings()
    tokens = load_tokens(settings.tokens_path)
    db = Database(db_path or os.environ.get("EVENT_DB_PATH", "events.db"))

    app = FastAPI(title="Idempotent Event Aggregator", version="0.1.0")
    app.state.settings = settings
    app.state.db = db
    app.state.tokens = tokens

    # A single shared connection is safe here: writers are serialized
    # by the RLock and WAL permits concurrent reads on the same handle.
    app.state.conn = db.connect()

    @app.exception_handler(IngestionError)
    async def _ingestion_error_handler(_: Request, exc: IngestionError) -> JSONResponse:
        logger.warning("rejected request code=%s details=%s", exc.code, exc.details)
        return JSONResponse(
            status_code=exc.http_status,
            content={
                "error": exc.code,
                "message": exc.message,
                "details": exc.details,
            },
        )

    def authenticate(
        authorization: str | None = Header(default=None),
    ) -> str:
        if not authorization or not authorization.startswith("Bearer "):
            raise UnauthorizedError(
                "missing bearer token", details={"reason": "missing_token"}
            )
        token = authorization.removeprefix("Bearer ").strip()
        tenant = tokens.get(token)
        if tenant is None:
            raise UnauthorizedError(
                "unknown token", details={"reason": "unknown_token"}
            )
        return tenant

    def require_admin(
        tenant: str = Depends(authenticate),
    ) -> str:
        if tenant != ADMIN_TENANT:
            raise ForbiddenError(
                "settlement requires the admin tenant",
                details={"reason": "admin_required", "tenant": tenant},
            )
        return tenant

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "window_seconds": settings.window_seconds}

    @app.post("/events")
    async def ingest(
        request: Request,
        tenant: str = Depends(authenticate),
    ) -> dict:
        try:
            raw_body = await request.json()
        except Exception:  # covers JSONDecodeError and empty body
            raise IngestionError(
                "request body must be a JSON object or array",
                code="invalid_event",
                http_status=422,
                details={"reason": "not_json"},
            )
        payloads = raw_body if isinstance(raw_body, list) else [raw_body]

        logger.info(
            "ingest tenant=%s count=%d payloads=%s",
            tenant,
            len(payloads),
            payloads,
        )

        events = [parse_event(item, tenant=tenant) for item in payloads]

        store = EventStore(app.state.conn, settings.window_seconds)
        summary = store.insert_events(events, write_lock=db.write_lock)
        logger.info(
            "ingest decision tenant=%s summary=%s (idempotent acceptance; "
            "duplicates are not re-counted)",
            tenant,
            summary,
        )
        return {"status": "accepted", **summary}

    @app.post("/admin/settle")
    async def settle(
        request: Request,
        admin: str = Depends(require_admin),
    ) -> dict:
        body = await request.json()
        if not isinstance(body, dict) or "watermark_ms" not in body:
            raise IngestionError(
                "request body must contain watermark_ms",
                code="invalid_event",
                details={"reason": "missing_field", "field": "watermark_ms"},
            )
        watermark_ms = body["watermark_ms"]
        if isinstance(watermark_ms, bool) or not isinstance(watermark_ms, int):
            raise IngestionError(
                "watermark_ms must be an integer unix-ms timestamp",
                code="invalid_event",
                details={
                    "reason": "invalid_type",
                    "field": "watermark_ms",
                    "actual": type(watermark_ms).__name__,
                },
            )

        # Admin settles *all* non-admin tenants it names in scope; by
        # default the admin operates on every tenant. Keep the endpoint
        # explicit: body may carry "tenants": ["tenant-a", ...].
        target_tenants = body.get("tenants")
        if target_tenants is None:
            target_tenants = sorted(
                {t for t in tokens.values() if t != ADMIN_TENANT}
            )
        if not isinstance(target_tenants, list) or not all(
            isinstance(t, str) for t in target_tenants
        ):
            raise IngestionError(
                "tenants must be a list of strings",
                code="invalid_event",
                details={"reason": "invalid_type", "field": "tenants"},
            )

        store = EventStore(app.state.conn, settings.window_seconds)
        results = {}
        for target in target_tenants:
            results[target] = store.settle_windows(
                target, watermark_ms, write_lock=db.write_lock
            )
        logger.info("settle by=%s watermark_ms=%d results=%s", admin, watermark_ms, results)
        return {"status": "settled", "watermark_ms": watermark_ms, "tenants": results}

    @app.get("/aggregates")
    def aggregates(
        tenant: str = Depends(authenticate),
        event_type: str | None = None,
        start_ms: int | None = None,
        end_ms: int | None = None,
    ) -> dict:
        rows = query_aggregates(
            app.state.conn,
            tenant=tenant,
            window_seconds=settings.window_seconds,
            event_type=event_type,
            start_ms=start_ms,
            end_ms=end_ms,
        )
        logger.info(
            "aggregates tenant=%s event_type=%s range=(%s,%s) rows=%d",
            tenant,
            event_type,
            start_ms,
            end_ms,
            len(rows),
        )
        return {
            "tenant": tenant,
            "window_seconds": settings.window_seconds,
            "windows": rows,
        }

    return app
