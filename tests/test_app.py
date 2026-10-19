from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import pytest
from fastapi import FastAPI

from delve import __version__
from delve.app import create_app
from delve.config import Settings
from delve.log import JsonFormatter


@asynccontextmanager
async def client_for(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


async def test_health_degraded_without_db(env: pytest.MonkeyPatch) -> None:
    app = create_app(Settings())  # type: ignore[call-arg]
    async with client_for(app) as client:
        r = await client.get("/api/v1/health")
    assert r.status_code == 503
    body = r.json()
    assert body["status"] == "degraded"
    assert body["checks"]["db"]["ok"] is False


async def test_meta(env: pytest.MonkeyPatch) -> None:
    app = create_app(Settings())  # type: ignore[call-arg]
    async with client_for(app) as client:
        r = await client.get("/api/v1/meta")
    assert r.status_code == 200
    body = r.json()
    assert body["server_version"] == __version__
    assert body["hermes_expected_version"] == "0.22.2"
    assert body["approval_timeout_s"] == 300
    assert "test-key" not in r.text


async def test_no_docs_exposed(env: pytest.MonkeyPatch) -> None:
    app = create_app(Settings())  # type: ignore[call-arg]
    async with client_for(app) as client:
        for path in ("/docs", "/redoc", "/openapi.json"):
            assert (await client.get(path)).status_code == 404


@pytest.mark.pg
async def test_health_ok_with_db(env: pytest.MonkeyPatch, pg_url: str) -> None:
    env.setenv("DELVE_DATABASE_URL", pg_url)
    app = create_app(Settings())  # type: ignore[call-arg]
    async with client_for(app) as client:
        r = await client.get("/api/v1/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "checks": {"db": {"ok": True}}}


def test_json_log_formatter_includes_extra() -> None:
    record = logging.LogRecord("delve.x", logging.INFO, __file__, 1, "hello %s", ("w",), None)
    record.run_id = "run_1"
    out = json.loads(JsonFormatter().format(record))
    assert out["msg"] == "hello w"
    assert out["level"] == "INFO"
    assert out["logger"] == "delve.x"
    assert out["run_id"] == "run_1"
    assert out["ts"].endswith("+00:00")
