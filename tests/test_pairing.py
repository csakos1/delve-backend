"""Pairing with a one-time code, the device's FCM token, status (plan 12/8 part, 12/12)."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.api.devices import FailureLimiter
from delve.auth import (
    authenticate,
    create_device,
    create_pairing_code,
    format_code,
    normalize_code,
    redeem_pairing_code,
    revoke_device,
    set_fcm_token,
)
from delve.db.models import Device, PairingCode, SystemState

from .conftest import ROOT
from .test_api import auth, hello


def test_code_format() -> None:
    assert normalize_code("1234-5678") == "12345678"
    assert normalize_code(" 1234 5678 ") == "12345678"
    for bad in ("1234567", "123456789", "abcd-efgh", "1" * 8 + " " * 30):
        assert normalize_code(bad) is None
    assert format_code("12345678") == "1234-5678"


@pytest.mark.pg
async def test_code_is_single_use(db: async_sessionmaker[AsyncSession]) -> None:
    async with db() as s:
        code, expires = await create_pairing_code(s, name="pixel")
        await s.commit()
    assert len(code) == 8 and code.isdigit()
    async with db() as s:
        stored = (await s.execute(select(PairingCode))).scalar_one()
        assert code not in stored.code_sha256 and stored.expires_at == expires

    async with db() as s:
        paired = await redeem_pairing_code(s, format_code(code), "ignored, the CLI named it")
        assert paired is not None
        device, token = paired
        await s.commit()
    assert device.name == "pixel"
    async with db() as s:
        ident = await authenticate(s, token)
        assert ident is not None and ident.id == device.id
        used = (await s.execute(select(PairingCode))).scalar_one()
        assert used.used_at is not None and used.device_id == device.id
        assert await redeem_pairing_code(s, code, None) is None  # used


@pytest.mark.pg
async def test_new_code_replaces_old_and_expiry(db: async_sessionmaker[AsyncSession]) -> None:
    async with db() as s:
        old, _ = await create_pairing_code(s)
        new, _ = await create_pairing_code(s)
        expired, _ = await create_pairing_code(s, ttl=timedelta(seconds=-1))
        await s.commit()
    async with db() as s:
        assert await redeem_pairing_code(s, old, None) is None
        assert await redeem_pairing_code(s, expired, None) is None
        await s.rollback()
    async with db() as s:
        # `expired` replaced `new` too: only the newest code is ever valid
        assert await redeem_pairing_code(s, new, None) is None
        fresh, _ = await create_pairing_code(s)
        paired = await redeem_pairing_code(s, fresh, "  my phone ")
        assert paired is not None and paired[0].name == "my phone"
        fresh2, _ = await create_pairing_code(s)
        paired2 = await redeem_pairing_code(s, fresh2, None)
        assert paired2 is not None and paired2[0].name == "phone"
        with pytest.raises(ValueError):
            await create_pairing_code(s, name=" ")


@pytest.mark.pg
async def test_fcm_token_and_revoke(db: async_sessionmaker[AsyncSession]) -> None:
    async with db() as s:
        device, _ = await create_device(s, "pixel")
        await set_fcm_token(s, device.id, "fcm-abc")
        with pytest.raises(ValueError):
            await set_fcm_token(s, device.id, " ")
        await s.commit()
    async with db() as s:
        assert (await s.get(Device, device.id)).fcm_token == "fcm-abc"  # type: ignore[union-attr]
        assert await revoke_device(s, device.id)
        await s.commit()
    async with db() as s:
        row = await s.get(Device, device.id)
        assert row is not None and row.revoked and row.fcm_token is None


@pytest.mark.pg
async def test_cli_pair(db: async_sessionmaker[AsyncSession], migrated_url: str) -> None:
    env = {**os.environ, "DELVE_DATABASE_URL": migrated_url}

    def cli(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603  # fixed argv, test-only
            [sys.executable, "-m", "delve", *args],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    r = cli("pair", "--name", "pixel", "--ttl", "5")
    assert r.returncode == 0, r.stderr
    code = r.stdout.strip()
    assert len(code) == 9 and code[4] == "-" and "valid until" in r.stderr
    assert cli("pair", "--ttl", "0").returncode == 2
    assert cli("pair", "--name", " ").returncode == 2
    async with db() as s:
        # the second `pair` (the failing ones never got that far) is the newest code
        assert await redeem_pairing_code(s, code, None) is not None


def test_limiter() -> None:
    lim = FailureLimiter(limit=2, window_s=60)
    assert lim.blocked() is None
    lim.fail()
    assert lim.blocked() is None
    lim.fail()
    wait = lim.blocked()
    assert wait is not None and 59 <= wait <= 60
    lim.window_s = 0.0
    assert lim.blocked() is None


# --- through the app ------------------------------------------------------------------


async def _code(db: async_sessionmaker[AsyncSession], name: str | None = None) -> str:
    async with db() as s:
        code, _ = await create_pairing_code(s, name=name)
        await s.commit()
    return code


@pytest.mark.pg
async def test_register_me_fcm_status(
    client: TestClient, db: async_sessionmaker[AsyncSession]
) -> None:
    code = await _code(db)
    reg = {"code": format_code(code), "name": "Pixel 9", "app_version": "0.1.0"}
    r = client.post("/api/v1/devices/register", json=reg)
    assert r.status_code == 201, r.text
    body = r.json()
    tok = body["token"]
    assert body["name"] == "Pixel 9" and tok.startswith("dlv_")
    again = client.post("/api/v1/devices/register", json={"code": code})
    assert again.status_code == 400 and again.json()["detail"]["code"] == "invalid_code"

    me = client.get("/api/v1/devices/me", headers=auth(tok)).json()
    assert me["id"] == body["device_id"] and me["fcm"] is False
    r = client.put("/api/v1/devices/me/fcm", json={"token": "fcm-xyz"}, headers=auth(tok))
    assert r.status_code == 204
    assert client.get("/api/v1/devices/me", headers=auth(tok)).json()["fcm"] is True
    assert client.put("/api/v1/devices/me/fcm", json={"token": None},
                      headers=auth(tok)).status_code == 204  # fmt: skip
    assert client.get("/api/v1/devices/me", headers=auth(tok)).json()["fcm"] is False
    for bad in ({"token": ""}, {}, {"token": "x", "extra": 1}):
        assert client.put("/api/v1/devices/me/fcm", json=bad,
                          headers=auth(tok)).status_code == 422  # fmt: skip
    assert client.put("/api/v1/devices/me/fcm", json={"token": "x"}).status_code == 401
    assert client.get("/api/v1/devices/me").status_code == 401

    st = client.get("/api/v1/status", headers=auth(tok)).json()
    assert set(st) == {"server_version", "hermes", "hermes_version", "thinking_route"}
    assert st["thinking_route"] is None
    assert client.get("/api/v1/status").status_code == 401


@pytest.mark.pg
async def test_register_rate_limit(
    client: TestClient, db: async_sessionmaker[AsyncSession]
) -> None:
    for _ in range(10):
        r = client.post("/api/v1/devices/register", json={"code": "0000-0000"})
        assert r.status_code == 400
    code = await _code(db)
    r = client.post("/api/v1/devices/register", json={"code": code})
    assert r.status_code == 429 and int(r.headers["retry-after"]) >= 1
    assert r.json()["detail"]["code"] == "too_many_attempts"
    client.app.state.pairing_limiter.window_s = 0.0  # type: ignore[attr-defined]
    assert client.post("/api/v1/devices/register", json={"code": code}).status_code == 201
    assert client.post("/api/v1/devices/register",
                       json={"code": "1" * 40}).status_code == 422  # fmt: skip


@pytest.mark.pg
async def test_hello_ok_carries_status(
    client: TestClient, token: str, db: async_sessionmaker[AsyncSession]
) -> None:
    with client.websocket_connect("/api/v1/ws") as ws:
        data = hello(ws, token)["data"]
    assert data["thinking_route"] is None and "hermes" in data and data["server_version"]
    async with db() as s:
        await s.execute(
            insert(SystemState).values(
                key="thinking_route",
                value={"ok": False, "run_id": "run_x", "at": "2026-10-19T20:00:00+00:00",
                       "runtime": {"provider": "custom"}},
            )
        )  # fmt: skip
        await s.commit()
    with client.websocket_connect("/api/v1/ws") as ws:
        data = hello(ws, token)["data"]
    assert data["thinking_route"] == {
        "ok": False, "run_id": "run_x", "at": "2026-10-19T20:00:00+00:00"
    }  # fmt: skip
