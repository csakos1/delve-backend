"""Runtime configuration, read from the environment (/etc/delve/env in production).

Variable names are fixed and unprefixed per field (see deploy/delve.env.example), so the
same file can be shared with operator scripts.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Annotated, Literal

from pydantic import AliasChoices, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _env(name: str) -> AliasChoices:
    return AliasChoices(name)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=None,
        extra="ignore",
        populate_by_name=False,
        frozen=True,
    )

    # --- database (peer auth over the unix socket by default) ---
    database_url: str = Field(
        default="postgresql+asyncpg:///delve?host=/run/postgresql",
        validation_alias=_env("DELVE_DATABASE_URL"),
    )
    db_connect_timeout_s: Annotated[float, Field(gt=0)] = Field(
        default=3.0, validation_alias=_env("DELVE_DB_CONNECT_TIMEOUT_S")
    )

    # --- Hermes API server ---
    hermes_base_url: str = Field(
        default="http://127.0.0.1:8642", validation_alias=_env("HERMES_BASE_URL")
    )
    hermes_api_key: SecretStr = Field(validation_alias=_env("HERMES_API_KEY"))
    hermes_expected_version: str = Field(
        default="0.22.2", validation_alias=_env("HERMES_EXPECTED_VERSION")
    )
    # Per-run thinking route (plan 1.2, decision 121): added to the run body of
    # thinking conversations only.
    hermes_think_provider: str = Field(
        default="custom:llama-qwen38-think", validation_alias=_env("HERMES_THINK_PROVIDER")
    )
    hermes_think_model: str = Field(validation_alias=_env("HERMES_THINK_MODEL"))
    # Mirrors Hermes' approvals.timeout; the backend user cannot read the agent config.
    hermes_approval_timeout_s: Annotated[int, Field(gt=0, le=86_400)] = Field(
        default=300, validation_alias=_env("HERMES_APPROVAL_TIMEOUT_S")
    )

    # --- model server ---
    llama_base_url: str = Field(
        default="http://127.0.0.1:8080", validation_alias=_env("LLAMA_BASE_URL")
    )

    # --- push (FCM HTTP v1; off without a service account key) ---
    fcm_service_account_file: Path | None = Field(
        default=None, validation_alias=_env("FCM_SERVICE_ACCOUNT_FILE")
    )
    # defaults to the key's project_id
    fcm_project_id: str | None = Field(default=None, validation_alias=_env("FCM_PROJECT_ID"))
    push_run_done_min_s: Annotated[float, Field(ge=0)] = Field(
        default=20.0, validation_alias=_env("PUSH_RUN_DONE_MIN_S")
    )

    # --- local files ---
    cards_path: Path = Field(
        default=Path("/run/delve/cards/events.jsonl"), validation_alias=_env("DELVE_CARDS_PATH")
    )
    cards_poll_s: Annotated[float, Field(gt=0)] = Field(
        default=2.0, validation_alias=_env("DELVE_CARDS_POLL_S")
    )
    inbox_dir: Path = Field(
        default=Path("/var/lib/delve-inbox"), validation_alias=_env("DELVE_INBOX_DIR")
    )

    # --- HTTP server ---
    # B3: privileged helper (delve-infra bin/delve-helper, delve-helper.socket)
    helper_socket: Path = Field(
        default=Path("/run/delve-helper.sock"), validation_alias=_env("DELVE_HELPER_SOCKET")
    )
    llama_start_wait_s: Annotated[float, Field(gt=0, le=1800)] = Field(
        default=180.0, validation_alias=_env("DELVE_LLAMA_START_WAIT_S")
    )

    # B4: schedules (the morning brief is one of them, its time is in the database)
    schedule_tick_s: Annotated[float, Field(gt=0, le=300)] = Field(
        default=30.0, validation_alias=_env("DELVE_SCHEDULE_TICK_S")
    )
    schedule_catchup_s: Annotated[float, Field(ge=0, le=86_400)] = Field(
        default=3600.0, validation_alias=_env("DELVE_SCHEDULE_CATCHUP_S")
    )
    # B5: Observability. GPU samples for the temperature/power series (0 turns sampling
    # off); the cleanup drops samples and push_log rows past their retention.
    stats_sample_s: Annotated[float, Field(ge=0, le=3600)] = Field(
        default=60.0, validation_alias=_env("DELVE_STATS_SAMPLE_S")
    )
    gpu_retention_days: Annotated[int, Field(ge=1, le=3650)] = Field(
        default=30, validation_alias=_env("DELVE_GPU_RETENTION_DAYS")
    )
    push_log_retention_days: Annotated[int, Field(ge=1, le=3650)] = Field(
        default=90, validation_alias=_env("DELVE_PUSH_LOG_RETENTION_DAYS")
    )
    bind: str = Field(default="127.0.0.1:8700", validation_alias=_env("DELVE_BIND"))
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO", validation_alias=_env("DELVE_LOG_LEVEL")
    )

    @field_validator("hermes_think_provider")
    @classmethod
    def _custom_provider(cls, v: str) -> str:
        if not v.startswith("custom:") or len(v) <= len("custom:"):
            raise ValueError("must be 'custom:<name>' (a custom_providers entry in Hermes)")
        return v

    @field_validator("hermes_think_model")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("must not be empty")
        return v

    @field_validator("hermes_base_url", "llama_base_url")
    @classmethod
    def _http_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("must be an http(s) URL")
        return v.rstrip("/")

    @field_validator("database_url")
    @classmethod
    def _asyncpg_url(cls, v: str) -> str:
        if not v.startswith("postgresql+asyncpg://"):
            raise ValueError("must use the postgresql+asyncpg:// driver")
        return v

    @field_validator("bind")
    @classmethod
    def _bind(cls, v: str) -> str:
        host, sep, port = v.rpartition(":")
        if not sep or not host or not port.isdigit() or not 0 < int(port) < 65536:
            raise ValueError("must be HOST:PORT")
        return v

    @property
    def bind_host(self) -> str:
        return self.bind.rpartition(":")[0].strip("[]")

    @property
    def bind_port(self) -> int:
        return int(self.bind.rpartition(":")[2])


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    # required fields come from the environment
    return Settings()
