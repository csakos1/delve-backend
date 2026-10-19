from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from delve.app import create_app
from delve.auth import create_device
from delve.config import Settings

from .fake_hermes import FakeHermes, StreamingASGITransport
from .fake_llama import FakeLlama
from .harness import FAST, Harness, make_harness

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).resolve().parent / "fixtures"

# Minimal valid environment; individual tests override what they exercise.
BASE_ENV = {
    "HERMES_API_KEY": "test-key",
    "HERMES_THINK_MODEL": "/home/<operator>/models/test.gguf",
    # unreachable on purpose: unit tests must not depend on a database
    "DELVE_DATABASE_URL": "postgresql+asyncpg://nobody@127.0.0.1:1/none",
    "DELVE_DB_CONNECT_TIMEOUT_S": "0.5",
}


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> Iterator[pytest.MonkeyPatch]:
    for key in list(os.environ):
        if key.startswith(("DELVE_", "HERMES_", "LLAMA_")) and not key.startswith("DELVE_TEST_"):
            monkeypatch.delenv(key)
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    yield monkeypatch


@pytest.fixture
def pg_url() -> str:
    url = os.environ.get("DELVE_TEST_DATABASE_URL")
    if not url:
        pytest.skip("DELVE_TEST_DATABASE_URL not set")
    return url


# --- database fixtures for the run manager / API tests --------------------------------

TABLES = (
    "push_log, devices, system_state, schedules, card_ingest_offsets, cards, approvals, "
    "tool_steps, run_events, runs, attachments, messages, conversations"
)


@pytest.fixture(scope="session")
def migrated_url() -> str:
    """DELVE_TEST_DATABASE_URL upgraded to head once per session."""
    import subprocess
    import sys

    url = os.environ.get("DELVE_TEST_DATABASE_URL")
    if not url:
        pytest.skip("DELVE_TEST_DATABASE_URL not set")
    r = subprocess.run(  # fixed argv, test-only
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        env={**os.environ, "DELVE_DATABASE_URL": url},
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode == 0, r.stderr
    return url


@pytest.fixture
async def db(migrated_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """A sessionmaker on an emptied database."""
    from sqlalchemy import text

    from delve.db.session import make_engine, make_sessionmaker

    engine = make_engine(migrated_url)
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE"))
    try:
        yield make_sessionmaker(engine)
    finally:
        await engine.dispose()


@pytest.fixture
async def h(db: async_sessionmaker[AsyncSession]) -> AsyncIterator[Harness]:
    """RunManager + EventHub + fake Hermes (streaming transport) on an empty database."""
    async with make_harness(db) as harness:
        yield harness


# --- app-level fixtures (test_api, test_pairing) ------------------------------------


@pytest.fixture
async def token(db: async_sessionmaker[AsyncSession]) -> str:
    async with db() as s:
        _, tok = await create_device(s, "pixel")
        await s.commit()
    return tok


@pytest.fixture
def fake() -> FakeHermes:
    return FakeHermes()


@pytest.fixture
def llama() -> FakeLlama:
    fl = FakeLlama()
    fl.set(task=100, processing=False, tokens=300)
    return fl


@pytest.fixture
def client(
    env: pytest.MonkeyPatch,
    migrated_url: str,
    fake: FakeHermes,
    llama: FakeLlama,
    db: async_sessionmaker[AsyncSession],
) -> Iterator[TestClient]:
    env.setenv("DELVE_DATABASE_URL", migrated_url)
    env.setenv("HERMES_API_KEY", "fake-key")
    app = create_app(
        Settings(),  # type: ignore[call-arg]
        hermes_transport=StreamingASGITransport(fake.app),
        llama_transport=StreamingASGITransport(llama.app),
        timing=FAST,
    )
    with TestClient(app) as c:
        yield c
