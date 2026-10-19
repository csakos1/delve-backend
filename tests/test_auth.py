from __future__ import annotations

import os
import subprocess
import sys

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.auth import (
    TOKEN_PREFIX,
    authenticate,
    create_device,
    new_token,
    revoke_device,
    token_hash,
)
from delve.db.models import Device

from .conftest import ROOT

pytestmark = pytest.mark.pg


def test_token_format() -> None:
    t = new_token()
    assert t.startswith(TOKEN_PREFIX)
    assert len(t) > 40
    assert token_hash(t) == token_hash(t) != token_hash(new_token())


async def test_create_authenticate_revoke(db: async_sessionmaker[AsyncSession]) -> None:
    async with db() as s:
        device, token = await create_device(s, " pixel ")
        await s.commit()
    async with db() as s:
        stored = (await s.execute(select(Device))).scalar_one()
        assert stored.name == "pixel"
        assert stored.token_sha256 == token_hash(token)
        assert token not in stored.token_sha256  # only the hash is stored

    async with db() as s:
        ident = await authenticate(s, token)
        assert ident is not None and ident.id == device.id
        assert await authenticate(s, token + "x") is None
        assert await authenticate(s, None) is None
        assert await authenticate(s, "Bearer " + token) is None
    async with db() as s:
        assert (await s.get(Device, device.id)) is not None
        seen = (await s.get(Device, device.id)).last_seen_at  # type: ignore[union-attr]
        assert seen is not None

    async with db() as s:
        assert await revoke_device(s, device.id)
        await s.commit()
        assert not await revoke_device(s, device.id)
    async with db() as s:
        assert await authenticate(s, token) is None


async def test_device_name_validation(db: async_sessionmaker[AsyncSession]) -> None:
    async with db() as s:
        for bad in ("", "   ", "x" * 65):
            with pytest.raises(ValueError):
                await create_device(s, bad)


async def test_cli_token_roundtrip(db: async_sessionmaker[AsyncSession], migrated_url: str) -> None:
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

    r = cli("token", "create", "--name", "pixel")
    assert r.returncode == 0, r.stderr
    token = r.stdout.strip()
    assert token.startswith(TOKEN_PREFIX)
    assert "shown once" in r.stderr

    r = cli("token", "list")
    assert r.returncode == 0 and "active" in r.stdout and "pixel" in r.stdout
    device_id = r.stdout.split()[0]

    async with db() as s:
        assert await authenticate(s, token) is not None

    assert cli("token", "revoke", device_id).returncode == 0
    assert cli("token", "revoke", device_id).returncode == 1
    assert cli("token", "revoke", "not-a-uuid").returncode == 2
    async with db() as s:
        assert await authenticate(s, token) is None
