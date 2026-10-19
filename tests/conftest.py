from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

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
