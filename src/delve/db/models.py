"""ORM models: the schema of plan 1.2, section 6.3.

The initial Alembic migration is the source of truth for the database; CI checks that
these models and the migrations do not drift (`alembic check`).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    REAL,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

TZ = DateTime(timezone=True)

RUN_STATUSES = (
    "queued",
    "starting",
    "running",
    "waiting_for_approval",
    "stopping",
    "reconnecting",
    "completed",
    "cancelled",
    "failed",
)
ACTIVE_RUN_STATUSES = ("starting", "running", "waiting_for_approval", "stopping", "reconnecting")
TERMINAL_RUN_STATUSES = ("completed", "cancelled", "failed")
MESSAGE_STATUSES = ("streaming", "final", "cancelled", "failed")
# plan 6.5; late_approved .. superseded belong to late approval (plan 12/5)
APPROVAL_STATUSES = (
    "pending",
    "approved_once",
    "approved_session",
    "denied",
    "expired_in_hermes",
    "late_approved",
    "followup_started",
    "executed",
    "superseded",
    "expired",
    "cancelled",
)


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({','.join(repr(v) for v in values)})"


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def _now() -> Any:
    return mapped_column(TZ, nullable=False, server_default=func.now())


class Conversation(Base):
    __tablename__ = "conversations"

    id: Mapped[str] = mapped_column(Text, primary_key=True)  # = Hermes session_id
    title: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = _now()
    updated_at: Mapped[datetime] = _now()
    archived: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    last_run_id: Mapped[str | None] = mapped_column(Text)
    # feature 16: per-conversation thinking (per-run route, no session lock)
    thinking: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    # feature 17: context fill, from /slots
    context_tokens: Mapped[int | None] = mapped_column(Integer)
    context_max: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("65536"))
    context_measured_at: Mapped[datetime | None] = mapped_column(TZ)
    # running totals of the runs' Hermes usage (decision 160)
    tokens_in: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    tokens_out: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    tokens_cache_read: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    origin: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'app'"))
    # the latest non-empty user/assistant text, for the chat list (N40, migration 0006)
    preview: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (CheckConstraint("origin IN ('app','schedule')", name="origin"),)


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    conversation_id: Mapped[str] = mapped_column(
        Text, ForeignKey("conversations.id"), nullable=False
    )
    run_id: Mapped[str | None] = mapped_column(Text)
    role: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("''"))
    # Hermes `message.interim` text(s) of the run; shown apart from the answer (N27)
    interim: Mapped[str | None] = mapped_column(Text)
    reasoning: Mapped[str | None] = mapped_column(Text)  # Hermes /messages 'reasoning'
    hermes_message_id: Mapped[str | None] = mapped_column(Text)
    client_msg_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), unique=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'final'"))
    created_at: Mapped[datetime] = _now()

    __table_args__ = (
        CheckConstraint("role IN ('user','assistant','tool','system')", name="role"),
        CheckConstraint(_in("status", MESSAGE_STATUSES), name="status"),
        Index(None, "conversation_id", "id"),
        Index(
            "uq_messages_conversation_id_hermes_message_id",
            "conversation_id",
            "hermes_message_id",
            unique=True,
            postgresql_where=text("hermes_message_id IS NOT NULL"),
        ),
    )


class Attachment(Base):
    __tablename__ = "attachments"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    conversation_id: Mapped[str] = mapped_column(
        Text, ForeignKey("conversations.id"), nullable=False
    )
    message_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("messages.id"))
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    mime: Mapped[str] = mapped_column(Text, nullable=False)
    filename: Mapped[str] = mapped_column(Text, nullable=False)  # sanitized
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sha256: Mapped[str] = mapped_column(Text, nullable=False)
    inbox_path: Mapped[str | None] = mapped_column(Text)  # only for kind='file'
    image_tokens: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = _now()
    deleted_at: Mapped[datetime | None] = mapped_column(TZ)

    __table_args__ = (CheckConstraint("kind IN ('image','file')", name="kind"),)


class Run(Base):
    __tablename__ = "runs"

    run_id: Mapped[str] = mapped_column(Text, primary_key=True)  # 'run_<hex>' or 'q_<uuid>'
    conversation_id: Mapped[str] = mapped_column(
        Text, ForeignKey("conversations.id"), nullable=False
    )
    idempotency_key: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, unique=True
    )
    status: Mapped[str] = mapped_column(Text, nullable=False)
    input: Mapped[str] = mapped_column(Text, nullable=False)
    instructions: Mapped[str | None] = mapped_column(Text)
    # state at run start; decides whether provider/model go into the run body
    thinking: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    queued_at: Mapped[datetime] = _now()
    started_at: Mapped[datetime | None] = mapped_column(TZ)
    ended_at: Mapped[datetime | None] = mapped_column(TZ)
    usage: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    perf: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    error: Mapped[str | None] = mapped_column(Text)
    origin: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'app'"))
    schedule_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))

    __table_args__ = (
        CheckConstraint("origin IN ('app','late_approval','schedule','system')", name="origin"),
        CheckConstraint(_in("status", RUN_STATUSES), name="status"),
        Index(None, "status"),
        Index(None, "schedule_id"),
    )


class RunEvent(Base):
    __tablename__ = "run_events"

    run_id: Mapped[str] = mapped_column(Text, primary_key=True)
    seq: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    event: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    received_at: Mapped[datetime] = _now()


class ToolStep(Base):
    __tablename__ = "tool_steps"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(Text, nullable=False)
    seq_started: Mapped[int | None] = mapped_column(Integer)
    seq_completed: Mapped[int | None] = mapped_column(Integer)
    tool: Mapped[str] = mapped_column(Text, nullable=False)
    preview_start: Mapped[str | None] = mapped_column(Text)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    result_raw: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    user_summary: Mapped[str | None] = mapped_column(Text)
    duration_s: Mapped[float | None] = mapped_column(REAL)
    # the approval Hermes asked for while this step was running
    approval_request_id: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint(
            "status IN ('running','ok','error','blocked','denied','timeout','cancelled')",
            name="status",
        ),
        Index(None, "run_id"),
    )


class Approval(Base):
    __tablename__ = "approvals"

    request_id: Mapped[str] = mapped_column(Text, primary_key=True)
    run_id: Mapped[str] = mapped_column(Text, nullable=False)
    conversation_id: Mapped[str] = mapped_column(
        Text, ForeignKey("conversations.id"), nullable=False
    )
    command: Mapped[str] = mapped_column(Text, nullable=False)
    command_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    pattern_keys: Mapped[list[str] | None] = mapped_column(ARRAY(Text))
    action_class: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    choice: Mapped[str | None] = mapped_column(Text)
    decided_by: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = _now()
    hermes_deadline: Mapped[datetime] = mapped_column(TZ, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TZ, nullable=False)
    responded_at: Mapped[datetime | None] = mapped_column(TZ)
    followup_run_id: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint(
            "action_class IN ('pkg_upgrade','service_restart','delete_kill','other')",
            name="action_class",
        ),
        CheckConstraint("choice IS NULL OR choice IN ('once','session','deny')", name="choice"),
        CheckConstraint(_in("status", APPROVAL_STATUSES), name="status"),
        Index(None, "status", "expires_at"),
    )


class Card(Base):
    __tablename__ = "cards"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    v: Mapped[int] = mapped_column(Integer, nullable=False)
    type: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(Text, nullable=False)
    ts: Mapped[datetime] = mapped_column(TZ, nullable=False)
    origin: Mapped[str] = mapped_column(Text, nullable=False)
    session_id: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    received_at: Mapped[datetime] = _now()
    read_at: Mapped[datetime | None] = mapped_column(TZ)

    __table_args__ = (
        CheckConstraint("severity IN ('info','warn','alert')", name="severity"),
        Index("ix_cards_type_ts", "type", text("ts DESC")),
    )


class CardIngestOffset(Base):
    __tablename__ = "card_ingest_offsets"

    path: Mapped[str] = mapped_column(Text, primary_key=True)
    inode: Mapped[int] = mapped_column(BigInteger, nullable=False)
    offset_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at: Mapped[datetime] = _now()


class Schedule(Base):
    __tablename__ = "schedules"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    # 'prompt': a prompt sent as a run; 'brief': the built-in morning brief (one row, B4)
    kind: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'prompt'"))
    name: Mapped[str] = mapped_column(Text, nullable=False)
    cron: Mapped[str] = mapped_column(Text, nullable=False)  # 5 fields, Europe/Budapest
    prompt: Mapped[str | None] = mapped_column(Text)
    conversation_mode: Mapped[str] = mapped_column(Text, nullable=False)
    conversation_id: Mapped[str | None] = mapped_column(Text, ForeignKey("conversations.id"))
    thinking: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    last_run_at: Mapped[datetime | None] = mapped_column(TZ)
    next_run_at: Mapped[datetime | None] = mapped_column(TZ)
    # the brief's own outcome ('ok' | 'failed'); a prompt schedule's comes from its last run
    last_status: Mapped[str | None] = mapped_column(Text)
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = _now()
    updated_at: Mapped[datetime] = _now()

    __table_args__ = (
        CheckConstraint("conversation_mode IN ('new','fixed')", name="conversation_mode"),
        CheckConstraint(
            "conversation_mode = 'new' OR conversation_id IS NOT NULL", name="fixed_has_conv"
        ),
        CheckConstraint("kind IN ('prompt','brief')", name="kind"),
        CheckConstraint("kind = 'brief' OR prompt IS NOT NULL", name="prompt_has_text"),
        Index(
            "uq_schedules_brief",
            "kind",
            unique=True,
            postgresql_where=text("kind = 'brief'"),
        ),
    )


class SystemState(Base):
    __tablename__ = "system_state"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = _now()


class Device(Base):
    __tablename__ = "devices"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    token_sha256: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    fcm_token: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = _now()
    last_seen_at: Mapped[datetime | None] = mapped_column(TZ)
    revoked: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    # which pushes this device wants (decision 260); keys: delve.push.NOTIFY_DEFAULTS
    notify: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text(
            """'{"approvals": true, "run_done": true, "water": false, "brief": false}'::jsonb"""
        ),
    )


class PairingCode(Base):
    """One-time code for pairing a phone (plan 12/8, decision 161); only its hash is kept."""

    __tablename__ = "pairing_codes"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    code_sha256: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    name: Mapped[str | None] = mapped_column(Text)  # device name set by the CLI, if any
    created_at: Mapped[datetime] = _now()
    expires_at: Mapped[datetime] = mapped_column(TZ, nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(TZ)
    device_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("devices.id")
    )


class PushLog(Base):
    __tablename__ = "push_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    device_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("devices.id")
    )
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    ref_id: Mapped[str | None] = mapped_column(Text)
    sent_at: Mapped[datetime] = _now()
    ok: Mapped[bool] = mapped_column(Boolean, nullable=False)
    error: Mapped[str | None] = mapped_column(Text)
