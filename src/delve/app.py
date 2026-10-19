"""FastAPI application factory."""

from __future__ import annotations

import asyncio
import logging
import platform
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from delve import __version__
from delve.config import Settings, get_settings
from delve.db.session import make_engine, make_sessionmaker, ping

log = logging.getLogger("delve.app")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = make_engine(settings.database_url, connect_timeout_s=settings.db_connect_timeout_s)
        app.state.engine = engine
        app.state.sessionmaker = make_sessionmaker(engine)
        log.info("startup", extra={"version": __version__, "bind": settings.bind})
        try:
            yield
        finally:
            await engine.dispose()
            log.info("shutdown")

    app = FastAPI(
        title="Delve backend",
        version=__version__,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.settings = settings

    @app.get("/api/v1/health")
    async def health(request: Request) -> JSONResponse:
        checks: dict[str, Any] = {}
        try:
            async with asyncio.timeout(settings.db_connect_timeout_s + 1):
                await ping(request.app.state.engine)
            checks["db"] = {"ok": True}
        except Exception as exc:  # report, never raise from a health probe
            checks["db"] = {"ok": False, "error": type(exc).__name__}
        ok = all(c["ok"] for c in checks.values())
        return JSONResponse(
            {"status": "ok" if ok else "degraded", "checks": checks},
            status_code=200 if ok else 503,
        )

    @app.get("/api/v1/meta")
    async def meta() -> dict[str, Any]:
        return {
            "server_version": __version__,
            "python": platform.python_version(),
            "hermes_expected_version": settings.hermes_expected_version,
            "approval_timeout_s": settings.hermes_approval_timeout_s,
        }

    return app


def app_factory() -> FastAPI:
    """Entry point for `uvicorn --factory delve.app:app_factory`."""
    return create_app()
