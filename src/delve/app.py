"""FastAPI application factory."""

from __future__ import annotations

import asyncio
import logging
import platform
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from delve import __version__
from delve.api import rest, ws
from delve.config import Settings, get_settings
from delve.db.session import make_engine, make_sessionmaker, ping
from delve.events import EventHub
from delve.hermes.client import HermesClient, HermesError, HermesUnavailable, ThinkingRoute
from delve.runs.approvals import ApprovalService
from delve.runs.manager import RunManager, Timing

log = logging.getLogger("delve.app")


def create_app(
    settings: Settings | None = None,
    *,
    hermes_transport: httpx.AsyncBaseTransport | None = None,
    run_worker: bool = True,
    timing: Timing | None = None,
) -> FastAPI:
    """`hermes_transport`, `run_worker` and `timing` exist for tests."""
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = make_engine(settings.database_url, connect_timeout_s=settings.db_connect_timeout_s)
        sessionmaker = make_sessionmaker(engine)
        hermes = HermesClient(
            settings.hermes_base_url,
            settings.hermes_api_key.get_secret_value(),
            ThinkingRoute(settings.hermes_think_provider, settings.hermes_think_model),
            transport=hermes_transport,
        )
        hub = EventHub()
        manager = RunManager(
            sessionmaker,
            hermes,
            hub,
            timing=timing,
            approval_timeout_s=settings.hermes_approval_timeout_s,
        )
        app.state.engine, app.state.sessionmaker = engine, sessionmaker
        app.state.hermes, app.state.hub, app.state.run_manager = hermes, hub, manager
        app.state.approvals = ApprovalService(sessionmaker, hermes, hub)
        app.state.hermes_version = None
        probe = asyncio.create_task(_probe_hermes(app, hermes))
        if run_worker:
            await manager.start()
        log.info("startup", extra={"version": __version__, "bind": settings.bind})
        try:
            yield
        finally:
            probe.cancel()
            await manager.close()
            await hermes.aclose()
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
    app.include_router(rest.router)
    app.include_router(ws.router)

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


async def _probe_hermes(app: FastAPI, hermes: HermesClient) -> None:
    """Record the Hermes version for hello.ok; the health monitor (plan 6.9) comes later."""
    delay = 1.0
    while True:
        try:
            health = await hermes.health()
            app.state.hermes_version = health.version
            expected = app.state.settings.hermes_expected_version
            if health.version != expected:
                log.warning(
                    "unexpected hermes version",
                    extra={"version": health.version, "expected": expected},
                )
            return
        except (HermesError, HermesUnavailable, ValueError) as exc:
            log.info("hermes not reachable yet", extra={"error": str(exc)})
        await asyncio.sleep(delay)
        delay = min(delay * 2, 60.0)


def app_factory() -> FastAPI:
    """Entry point for `uvicorn --factory delve.app:app_factory`."""
    return create_app()
