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

from delve import API_REVISION, REVISION, __version__
from delve.api import devices, rest, ws
from delve.api import schedules as schedules_api
from delve.api import stats as stats_api
from delve.api import system as system_api
from delve.brief import BriefService
from delve.cards.ingest import CardIngester
from delve.config import Settings, get_settings
from delve.db.session import make_engine, make_sessionmaker, ping
from delve.events import EventHub
from delve.hermes.client import HermesClient, ThinkingRoute
from delve.llama import LlamaClient
from delve.push import Presence, build_push
from delve.runs.approvals import ApprovalService
from delve.runs.manager import RunManager, Timing
from delve.schedules import ScheduleService
from delve.stats import StatsService
from delve.system import HelperClient, SystemService

log = logging.getLogger("delve.app")


def create_app(
    settings: Settings | None = None,
    *,
    hermes_transport: httpx.AsyncBaseTransport | None = None,
    llama_transport: httpx.AsyncBaseTransport | None = None,
    run_worker: bool = True,
    timing: Timing | None = None,
    push_transport: httpx.AsyncBaseTransport | None = None,
    system_options: dict[str, float] | None = None,
) -> FastAPI:
    """`hermes_transport`, `llama_transport`, `run_worker`, `timing`, `push_transport` and
    `system_options` (SystemService timings) exist for tests."""
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
        llama = LlamaClient(settings.llama_base_url, transport=llama_transport)
        hub = EventHub(validate=True)
        manager = RunManager(
            sessionmaker,
            hermes,
            hub,
            timing=timing,
            approval_timeout_s=settings.hermes_approval_timeout_s,
            expected_version=settings.hermes_expected_version,
            llama=llama,
        )
        app.state.engine, app.state.sessionmaker = engine, sessionmaker
        app.state.hermes, app.state.hub, app.state.run_manager = hermes, hub, manager
        app.state.approvals = ApprovalService(sessionmaker, hermes, hub, manager)
        push = build_push(
            sessionmaker,
            app.state.presence,
            service_account_file=settings.fcm_service_account_file,
            project_id=settings.fcm_project_id,
            run_done_min_s=settings.push_run_done_min_s,
            transport=push_transport,
        )
        hub.add_listener(push.listener)
        app.state.push = push
        await push.start()
        ingester = CardIngester(
            sessionmaker, hub, settings.cards_path, poll_s=settings.cards_poll_s
        )
        app.state.card_ingester = ingester
        helper = HelperClient(settings.helper_socket)
        system = SystemService(
            sessionmaker,
            manager,
            hub,
            hermes,
            llama,
            helper,
            start_wait_s=settings.llama_start_wait_s,
            **(system_options or {}),
        )
        app.state.system = system
        schedules = ScheduleService(
            sessionmaker,
            manager,
            hub,
            BriefService(sessionmaker, hub, helper, system),
            tick_s=settings.schedule_tick_s,
            catchup_s=settings.schedule_catchup_s,
        )
        app.state.schedules = schedules
        stats = StatsService(
            sessionmaker,
            helper,
            sample_s=settings.stats_sample_s,
            gpu_retention_days=settings.gpu_retention_days,
            push_log_retention_days=settings.push_log_retention_days,
        )
        app.state.stats = stats
        if run_worker:
            await system.start()  # before the worker: a held queue must not start a run
            await manager.start()
            await ingester.start()
            await schedules.start()
            await stats.start()
        log.info(
            "startup",
            extra={"version": __version__, "bind": settings.bind, "push": push.enabled},
        )
        try:
            yield
        finally:
            await stats.close()
            await schedules.close()
            await ingester.close()
            await system.close()
            await manager.close()
            await push.close()
            await hermes.aclose()
            await llama.aclose()
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
    app.state.pairing_limiter = devices.FailureLimiter()
    app.state.presence = Presence()
    app.include_router(rest.router)
    app.include_router(devices.router)
    app.include_router(system_api.router)
    app.include_router(schedules_api.router)
    app.include_router(stats_api.router)
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
            "api_revision": API_REVISION,
            "revision": REVISION,
            "python": platform.python_version(),
            "hermes_expected_version": settings.hermes_expected_version,
            "approval_timeout_s": settings.hermes_approval_timeout_s,
        }

    return app


def app_factory() -> FastAPI:
    """Entry point for `uvicorn --factory delve.app:app_factory`."""
    return create_app()
