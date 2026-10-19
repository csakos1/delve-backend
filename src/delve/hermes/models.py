"""Response models. Unknown fields are kept (`extra="allow"`) so nothing is lost when
Hermes adds fields; only what the backend reads is typed."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

ApprovalChoice = Literal["once", "session", "deny"]
ALLOWED_CHOICES: frozenset[str] = frozenset({"once", "session", "deny"})


class _Model(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)


class Health(_Model):
    status: str
    platform: str | None = None
    version: str | None = None


class RunCreated(_Model):
    run_id: str
    status: str
    replayed: bool = False


class Usage(_Model):
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    cache_read_tokens: int | None = None
    cache_write_tokens: int | None = None
    reasoning_tokens: int | None = None


class PendingApproval(_Model):
    request_id: str
    command: str
    description: str | None = None
    pattern_key: str | None = None
    pattern_keys: list[str] = Field(default_factory=list)
    allow_permanent: bool | None = None
    allow_session: bool | None = None
    choices: list[str] = Field(default_factory=list)


class RunStatus(_Model):
    run_id: str
    status: str
    session_id: str | None = None
    last_event: str | None = None
    created_at: float | None = None
    updated_at: float | None = None
    output: str | None = None
    usage: Usage | None = None
    approval: PendingApproval | None = None
    runtime: dict[str, Any] | None = None
    completed: bool | None = None
    interrupted: bool | None = None
    turn_exit_reason: str | None = None

    @property
    def terminal(self) -> bool:
        return self.status in {"completed", "cancelled", "failed", "interrupted"}


class ApprovalResponse(_Model):
    run_id: str
    request_id: str
    choice: str
    resolved: int


class SessionInfo(_Model):
    id: str | None = None
    title: str | None = None


class SessionMessage(_Model):
    id: int | str
    session_id: str | None = None
    role: str
    content: str | None = None
    reasoning: str | None = None
    reasoning_content: str | None = None
    tool_calls: list[dict[str, Any]] | None = None
    tool_call_id: str | None = None
    tool_name: str | None = None
    finish_reason: str | None = None
    display_kind: str | None = None
    token_count: int | None = None
    timestamp: float | None = None

    @property
    def thinking_text(self) -> str | None:
        """The reasoning text, if any (`reasoning` and `reasoning_content` are equal)."""
        text = self.reasoning or self.reasoning_content
        return text if text and text.strip() else None


class Toolset(_Model):
    name: str
    enabled: bool = False
    configured: bool | None = None
    tools: list[str] = Field(default_factory=list)
