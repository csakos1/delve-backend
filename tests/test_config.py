from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from delve.config import Settings


def test_defaults(env: pytest.MonkeyPatch) -> None:
    env.delenv("DELVE_DATABASE_URL")
    s = Settings()  # type: ignore[call-arg]
    assert s.database_url == "postgresql+asyncpg:///delve?host=/run/postgresql"
    assert s.hermes_base_url == "http://127.0.0.1:8642"
    assert s.hermes_expected_version == "0.22.2"
    assert s.hermes_think_provider == "custom:llama-qwen38-think"
    assert s.hermes_approval_timeout_s == 300
    assert s.cards_path == Path("/run/delve/cards/events.jsonl")
    assert (s.bind_host, s.bind_port) == ("127.0.0.1", 8700)
    assert s.hermes_api_key.get_secret_value() == "test-key"
    assert "test-key" not in repr(s)


@pytest.mark.parametrize("missing", ["HERMES_API_KEY", "HERMES_THINK_MODEL"])
def test_required(env: pytest.MonkeyPatch, missing: str) -> None:
    env.delenv(missing)
    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]


@pytest.mark.parametrize(
    ("var", "value"),
    [
        ("HERMES_THINK_PROVIDER", "llama-qwen38-think"),
        ("HERMES_THINK_PROVIDER", "custom:"),
        ("HERMES_THINK_MODEL", "  "),
        ("HERMES_BASE_URL", "127.0.0.1:8642"),
        ("HERMES_APPROVAL_TIMEOUT_S", "0"),
        ("DELVE_DATABASE_URL", "postgresql://delve"),
        ("DELVE_BIND", "8700"),
        ("DELVE_BIND", "127.0.0.1:99999"),
        ("DELVE_LOG_LEVEL", "TRACE"),
    ],
)
def test_rejects(env: pytest.MonkeyPatch, var: str, value: str) -> None:
    env.setenv(var, value)
    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]


def test_normalizes_urls_and_ipv6_bind(env: pytest.MonkeyPatch) -> None:
    env.setenv("HERMES_BASE_URL", "http://127.0.0.1:8642/")
    env.setenv("DELVE_BIND", "[::1]:8700")
    s = Settings()  # type: ignore[call-arg]
    assert s.hermes_base_url == "http://127.0.0.1:8642"
    assert (s.bind_host, s.bind_port) == ("::1", 8700)
