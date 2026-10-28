"""Schema tests. The `pg` ones create and drop the whole schema in the target database."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

from delve.db.models import Base

from .conftest import ROOT

EXPECTED_TABLES = {
    "conversations",
    "messages",
    "attachments",
    "runs",
    "run_events",
    "tool_steps",
    "approvals",
    "cards",
    "card_ingest_offsets",
    "schedules",
    "system_state",
    "devices",
    "pairing_codes",
    "push_log",
    "gpu_samples",  # B5
    "watches",  # W2
    "watch_items",
}


def test_metadata_tables() -> None:
    assert set(Base.metadata.tables) == EXPECTED_TABLES


def test_plan_1_2_changes() -> None:
    conv = Base.metadata.tables["conversations"].c
    assert "hermes_provider" not in conv
    assert "model_locked_at" not in conv
    assert "thinking" in conv
    msgs = Base.metadata.tables["messages"]
    assert "hermes_message_id" in msgs.c
    idx = {i.name: i for i in msgs.indexes}["uq_messages_conversation_id_hermes_message_id"]
    assert idx.unique
    assert "hermes_message_id IS NOT NULL" in str(idx.dialect_options["postgresql"]["where"])


def _alembic(url: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603  # fixed argv, test-only
        [sys.executable, "-m", "alembic", *args],
        cwd=ROOT,
        env={**os.environ, "DELVE_DATABASE_URL": url},
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def migrated(pg_url: str) -> str:
    for step in (("downgrade", "base"), ("upgrade", "head")):
        r = _alembic(pg_url, *step)
        assert r.returncode == 0, r.stderr
    return pg_url


@pytest.mark.pg
def test_migrations_match_models_and_roundtrip(migrated: str) -> None:
    r = _alembic(migrated, "check")
    assert r.returncode == 0, r.stdout + r.stderr
    for step in (("downgrade", "base"), ("upgrade", "head")):
        r = _alembic(migrated, *step)
        assert r.returncode == 0, r.stderr


@pytest.mark.pg
async def test_constraints(migrated: str) -> None:
    engine = create_async_engine(migrated)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("INSERT INTO conversations (id) VALUES ('c_1')"))
            for hid in ("h1", None, None):
                await conn.execute(
                    text(
                        "INSERT INTO messages (conversation_id, role, hermes_message_id) "
                        "VALUES ('c_1', 'assistant', :h)"
                    ),
                    {"h": hid},
                )
            row = (
                await conn.execute(
                    text("SELECT thinking, context_max, origin FROM conversations WHERE id='c_1'")
                )
            ).one()
            assert tuple(row) == (False, 65536, "app")

        bad = [
            "INSERT INTO messages (conversation_id, role, hermes_message_id) "
            "VALUES ('c_1', 'assistant', 'h1')",
            "INSERT INTO messages (conversation_id, role) VALUES ('c_1', 'bot')",
            "INSERT INTO messages (conversation_id, role) VALUES ('nope', 'user')",
            "INSERT INTO schedules (id, name, cron, prompt, conversation_mode) "
            "VALUES ('00000000-0000-4000-8000-000000000001', 'n', '0 7 * * *', 'p', 'fixed')",
            "INSERT INTO cards (id, v, type, severity, ts, origin, payload) "
            "VALUES ('x', 1, 'water', 'critical', now(), 'script', '{}')",
        ]
        for stmt in bad:
            with pytest.raises(IntegrityError):
                async with engine.begin() as conn:
                    await conn.execute(text(stmt))
    finally:
        async with engine.begin() as conn:
            await conn.execute(text("DELETE FROM messages"))
            await conn.execute(text("DELETE FROM conversations"))
        await engine.dispose()
