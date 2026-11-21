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
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy import text as sql_text  # NewsItem has a `text` column
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
    # P1: the personal profile this chat started with (the head of every run's
    # instructions). NULL: not started yet; '': started without one.
    persona: Mapped[str | None] = mapped_column(Text)
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
    # both NULL until chat.send binds the upload to its message (B6, migration 0009)
    conversation_id: Mapped[str | None] = mapped_column(Text, ForeignKey("conversations.id"))
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

    __table_args__ = (
        CheckConstraint("kind IN ('image','file')", name="kind"),
        Index(None, "message_id"),
        Index(None, "conversation_id"),
    )


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
    # M: the model loaded when the run started, as shown ("Qwen3.8-27B · IQ4_XS")
    model: Mapped[str | None] = mapped_column(Text)

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
    # 'app' (a phone) or 'desk' (the desktop agent, DA2): what the token opens (delve.auth)
    role: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'app'"))
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


class GpuSample(Base):
    """One `gpu.status` reading per GPU, every DELVE_STATS_SAMPLE_S (B5, Observability).

    Kept for DELVE_GPU_RETENTION_DAYS; the primary key leads with `ts`, so the range
    reads and the cleanup use it.
    """

    __tablename__ = "gpu_samples"

    ts: Mapped[datetime] = mapped_column(TZ, primary_key=True)
    gpu_index: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    name: Mapped[str | None] = mapped_column(Text)
    temp_c: Mapped[float | None] = mapped_column(REAL)
    power_w: Mapped[float | None] = mapped_column(REAL)
    util_pct: Mapped[float | None] = mapped_column(REAL)
    mem_used_mib: Mapped[int | None] = mapped_column(Integer)


class Watch(Base):
    """A watcher (W): defined by the agent's `watch_manage` tool, run by a no-agent Hermes
    cron job (`watch:<slug>`), known here from its `watch_def` / `watch_run` cards and the
    helper's `agent.cron.list`. Deleting it happens in the chat (the files are the agent's);
    a watcher whose cron job is gone is marked `deleted_at`.
    """

    __tablename__ = "watches"

    slug: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    criteria: Mapped[str | None] = mapped_column(Text)
    # include / any / exclude / min_price / max_price / fetch_details / notify_existing
    filters: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    stop_after_hit: Mapped[bool] = mapped_column(Boolean, nullable=False)
    schedule: Mapped[str] = mapped_column(Text, nullable=False)
    def_hash: Mapped[str] = mapped_column(Text, nullable=False)
    defined_at: Mapped[datetime] = mapped_column(TZ, nullable=False)
    # from agent.cron.list: the job id, 'scheduled' | 'paused', next run, when last seen
    job_id: Mapped[str | None] = mapped_column(Text)
    job_state: Mapped[str | None] = mapped_column(Text)
    next_run_at: Mapped[datetime | None] = mapped_column(TZ)
    job_seen_at: Mapped[datetime | None] = mapped_column(TZ)
    # the last watch_run card
    last_check_at: Mapped[datetime | None] = mapped_column(TZ)
    last_status: Mapped[str | None] = mapped_column(Text)
    last_error_code: Mapped[str | None] = mapped_column(Text)
    last_error: Mapped[str | None] = mapped_column(Text)
    last_items: Mapped[int | None] = mapped_column(Integer)
    last_matched: Mapped[int | None] = mapped_column(Integer)
    errors_in_row: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    # set when stop_after_hit paused the job after a hit
    stopped_at: Mapped[datetime | None] = mapped_column(TZ)
    created_at: Mapped[datetime] = _now()
    updated_at: Mapped[datetime] = _now()
    deleted_at: Mapped[datetime | None] = mapped_column(TZ)

    __table_args__ = (
        CheckConstraint("last_status IN ('ok','error')", name="last_status"),
        CheckConstraint("job_state IN ('scheduled','paused')", name="job_state"),
    )


class WatchItem(Base):
    """One listing a watcher reported (a `watch_candidate` card), with the verdict.

    verdict: 'pending' (waits for the judge), 'match' / 'reject' (the judge's answer
    against the watcher's criteria), 'mechanical' (no criteria: the filters decided),
    'error' (the judge failed `attempts` times). `notify` is false for items read from
    history (older than the push window) - they are judged but not pushed.
    """

    __tablename__ = "watch_items"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    slug: Mapped[str] = mapped_column(Text, nullable=False)
    item_id: Mapped[str] = mapped_column(Text, nullable=False)
    card_id: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    price: Mapped[str | None] = mapped_column(Text)
    price_value: Mapped[int | None] = mapped_column(BigInteger)
    location: Mapped[str | None] = mapped_column(Text)
    posted: Mapped[str | None] = mapped_column(Text)
    excerpt: Mapped[str | None] = mapped_column(Text)
    reserved: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    verdict: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str | None] = mapped_column(Text)
    notify: Mapped[bool] = mapped_column(Boolean, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    judge: Mapped[dict[str, Any] | None] = mapped_column(JSONB)  # ms, tokens
    seen_at: Mapped[datetime] = mapped_column(TZ, nullable=False)  # the card's ts
    judged_at: Mapped[datetime | None] = mapped_column(TZ)
    notified_at: Mapped[datetime | None] = mapped_column(TZ)
    available_at: Mapped[datetime | None] = mapped_column(TZ)  # "jegelve" lifted
    created_at: Mapped[datetime] = _now()

    __table_args__ = (
        UniqueConstraint("slug", "item_id"),
        CheckConstraint(
            "verdict IN ('pending','match','reject','mechanical','error')", name="verdict"
        ),
        Index("ix_watch_items_slug_id", "slug", text("id DESC")),
        Index("ix_watch_items_pending", "id", postgresql_where=text("verdict = 'pending'")),
    )


class NewsTopic(Base):
    """A News topic (N1): a name and a description the model scores against. Imported
    (`delve-backend news import`) or edited in the app; the list says what the operator
    reads, so it lives only in the database (D-58 in delve-infra)."""

    __tablename__ = "news_topics"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    position: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    created_at: Mapped[datetime] = _now()
    updated_at: Mapped[datetime] = _now()


class NewsSource(Base):
    """A feed the agent's collector reads (N1), with what its last fetch said."""

    __tablename__ = "news_sources"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    topic_id: Mapped[str] = mapped_column(
        Text, ForeignKey("news_topics.id", ondelete="CASCADE"), nullable=False
    )
    url: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    kind: Mapped[str] = mapped_column(Text, nullable=False, server_default="feed")
    window_h: Mapped[int] = mapped_column(Integer, nullable=False, server_default="48")
    every_min: Mapped[int] = mapped_column(Integer, nullable=False, server_default="60")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    origin: Mapped[str] = mapped_column(Text, nullable=False, server_default="import")
    # the collector's last report on it
    last_at: Mapped[datetime | None] = mapped_column(TZ)
    last_ok_at: Mapped[datetime | None] = mapped_column(TZ)
    last_status: Mapped[str | None] = mapped_column(Text)
    last_http: Mapped[int | None] = mapped_column(Integer)
    last_error_code: Mapped[str | None] = mapped_column(Text)
    last_error: Mapped[str | None] = mapped_column(Text)
    last_items: Mapped[int | None] = mapped_column(Integer)
    last_new: Mapped[int | None] = mapped_column(Integer)
    errors_in_row: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    created_at: Mapped[datetime] = _now()
    updated_at: Mapped[datetime] = _now()

    __table_args__ = (
        CheckConstraint("kind IN ('feed','release')", name="kind"),
        CheckConstraint("origin IN ('import','app','agent')", name="origin"),
        CheckConstraint(
            "last_status IN ('ok','not_modified','skipped','error')", name="last_status"
        ),
    )


class NewsItem(Base):
    """One news item (N1): a feed entry the collector found, or one day's releases of a
    `release` source merged into one row. `dup_of` points to the first item with the same
    title (another source of the same story); the lists show the first one only. The model's
    fields (score .. summary_hu) are filled in N1b, the article's text in N1c."""

    __tablename__ = "news_items"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    source_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("news_sources.id", ondelete="SET NULL")
    )
    topic_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("news_topics.id", ondelete="SET NULL")
    )
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    publisher: Mapped[str | None] = mapped_column(Text)
    excerpt: Mapped[str | None] = mapped_column(Text)  # the feed's own summary
    published_at: Mapped[datetime | None] = mapped_column(TZ)
    seen_at: Mapped[datetime] = mapped_column(TZ, nullable=False)  # the batch's time
    gn: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    norm_title: Mapped[str] = mapped_column(Text, nullable=False)
    dup_of: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("news_items.id", ondelete="SET NULL")
    )
    # a `release` row: [{tag, url, published_at}], newest first
    releases: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB)
    score: Mapped[int | None] = mapped_column(Integer)
    why: Mapped[str | None] = mapped_column(Text)
    setup: Mapped[bool | None] = mapped_column(Boolean)
    summary_hu: Mapped[str | None] = mapped_column(Text)
    scored_at: Mapped[datetime | None] = mapped_column(TZ)
    # N1b: tries of the score and why steps, when the story was compared with the others
    # of its days (grouping), when it was pushed
    score_attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    why_attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    grouped_at: Mapped[datetime | None] = mapped_column(TZ)
    pushed_at: Mapped[datetime | None] = mapped_column(TZ)
    # N1c: the article's text (the agent's collector fetches it when the story is wanted):
    # text_state queued (in wanted.json, try text_try) | ok | failed (text_error: the
    # collector's status); text_url the publisher's address (a Google News link resolved);
    # text_at when it was queued or answered; text_requested_at the user asked for it
    text: Mapped[str | None] = mapped_column(Text)
    text_state: Mapped[str | None] = mapped_column(Text)
    text_error: Mapped[str | None] = mapped_column(Text)
    text_url: Mapped[str | None] = mapped_column(Text)
    text_at: Mapped[datetime | None] = mapped_column(TZ)
    text_try: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    text_requested_at: Mapped[datetime | None] = mapped_column(TZ)
    # N4b: the merged item whose article is fetched when the story's own could not be
    # (null: its own), and that article's publisher once its text came
    text_from: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("news_items.id", ondelete="SET NULL")
    )
    text_publisher: Mapped[str | None] = mapped_column(Text)
    summary_attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    # N4b: 'excerpt' - the summary was written from the feed's excerpt (no article came)
    summary_basis: Mapped[str | None] = mapped_column(Text)
    read_at: Mapped[datetime | None] = mapped_column(TZ)
    saved_at: Mapped[datetime | None] = mapped_column(TZ)
    feedback: Mapped[str | None] = mapped_column(Text)
    feedback_at: Mapped[datetime | None] = mapped_column(TZ)
    created_at: Mapped[datetime] = _now()

    __table_args__ = (
        CheckConstraint("kind IN ('feed','release')", name="kind"),
        CheckConstraint("feedback IN ('more','less')", name="feedback"),
        CheckConstraint("text_state IN ('queued','ok','failed')", name="text_state"),
        CheckConstraint("summary_basis IN ('excerpt')", name="summary_basis"),
        Index(
            "ix_news_items_text_state",
            "text_state",
            postgresql_where=sql_text("text_state IS NOT NULL"),
        ),
        Index(
            "ix_news_items_lead", sql_text("id DESC"), postgresql_where=sql_text("dup_of IS NULL")
        ),
        Index("ix_news_items_dup_of", "dup_of"),
        Index("ix_news_items_norm_title", "norm_title"),
    )


class NewsBatch(Base):
    """A batch file of the collector that was read (or refused), so it is read once."""

    __tablename__ = "news_batches"

    name: Mapped[str] = mapped_column(Text, primary_key=True)
    at: Mapped[datetime | None] = mapped_column(TZ)
    items: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    new: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    error: Mapped[str | None] = mapped_column(Text)
    ingested_at: Mapped[datetime] = _now()


class InboxDone(Base):
    """An Inbox item the reader cleared (I0). The Inbox itself is read from the other
    tables; this says what not to show: `kind` and `ref` name the item, `mark` the state it
    was cleared in (a notice - a failing feed, a failed switch or schedule - comes back
    when it fails anew), `item` is the item as it was shown (the day's Done list)."""

    __tablename__ = "inbox_done"

    kind: Mapped[str] = mapped_column(Text, primary_key=True)
    ref: Mapped[str] = mapped_column(Text, primary_key=True)
    mark: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    item: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    done_at: Mapped[datetime] = mapped_column(TZ, nullable=False)

    __table_args__ = (Index(None, "done_at"),)
