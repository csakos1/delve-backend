"""Initial schema: plan 1.2, section 6.3.

Revision ID: 0001
Revises:
Create Date: 2026-10-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "card_ingest_offsets",
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("inode", sa.BigInteger(), nullable=False),
        sa.Column("offset_bytes", sa.BigInteger(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("path", name=op.f("pk_card_ingest_offsets")),
    )
    op.create_table(
        "cards",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("v", sa.Integer(), nullable=False),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("origin", sa.Text(), nullable=False),
        sa.Column("session_id", sa.Text(), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("severity IN ('info','warn','alert')", name=op.f("ck_cards_severity")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_cards")),
    )
    op.create_index(
        "ix_cards_type_ts", "cards", ["type", sa.literal_column("ts DESC")], unique=False
    )
    op.create_table(
        "conversations",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("archived", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("last_run_id", sa.Text(), nullable=True),
        sa.Column("thinking", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("context_tokens", sa.Integer(), nullable=True),
        sa.Column("context_max", sa.Integer(), server_default=sa.text("65536"), nullable=False),
        sa.Column("context_measured_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("origin", sa.Text(), server_default=sa.text("'app'"), nullable=False),
        sa.CheckConstraint("origin IN ('app','schedule')", name=op.f("ck_conversations_origin")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_conversations")),
    )
    op.create_table(
        "devices",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("token_sha256", sa.Text(), nullable=False),
        sa.Column("fcm_token", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_devices")),
        sa.UniqueConstraint("token_sha256", name=op.f("uq_devices_token_sha256")),
    )
    op.create_table(
        "run_events",
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("seq", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("event", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("run_id", "seq", name=op.f("pk_run_events")),
    )
    op.create_table(
        "system_state",
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("value", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("key", name=op.f("pk_system_state")),
    )
    op.create_table(
        "tool_steps",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("seq_started", sa.Integer(), nullable=True),
        sa.Column("seq_completed", sa.Integer(), nullable=True),
        sa.Column("tool", sa.Text(), nullable=False),
        sa.Column("preview_start", sa.Text(), nullable=True),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("result_raw", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("user_summary", sa.Text(), nullable=True),
        sa.Column("duration_s", sa.REAL(), nullable=True),
        sa.CheckConstraint(
            "status IN ('running','ok','error','blocked','denied','timeout','cancelled')",
            name=op.f("ck_tool_steps_status"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_tool_steps")),
    )
    op.create_index(op.f("ix_tool_steps_run_id"), "tool_steps", ["run_id"], unique=False)
    op.create_table(
        "approvals",
        sa.Column("request_id", sa.Text(), nullable=False),
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("conversation_id", sa.Text(), nullable=False),
        sa.Column("command", sa.Text(), nullable=False),
        sa.Column("command_sha256", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("pattern_keys", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("action_class", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("choice", sa.Text(), nullable=True),
        sa.Column("decided_by", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("hermes_deadline", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("responded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("followup_run_id", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "action_class IN ('pkg_upgrade','service_restart','delete_kill','other')",
            name=op.f("ck_approvals_action_class"),
        ),
        sa.CheckConstraint(
            "choice IS NULL OR choice IN ('once','session','deny')",
            name=op.f("ck_approvals_choice"),
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name=op.f("fk_approvals_conversation_id_conversations"),
        ),
        sa.PrimaryKeyConstraint("request_id", name=op.f("pk_approvals")),
    )
    op.create_index(
        op.f("ix_approvals_status_expires_at"), "approvals", ["status", "expires_at"], unique=False
    )
    op.create_table(
        "messages",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("conversation_id", sa.Text(), nullable=False),
        sa.Column("run_id", sa.Text(), nullable=True),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("content", sa.Text(), server_default=sa.text("''"), nullable=False),
        sa.Column("reasoning", sa.Text(), nullable=True),
        sa.Column("hermes_message_id", sa.Text(), nullable=True),
        sa.Column("client_msg_id", sa.UUID(), nullable=True),
        sa.Column("status", sa.Text(), server_default=sa.text("'final'"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "role IN ('user','assistant','tool','system')", name=op.f("ck_messages_role")
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name=op.f("fk_messages_conversation_id_conversations"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_messages")),
        sa.UniqueConstraint("client_msg_id", name=op.f("uq_messages_client_msg_id")),
    )
    op.create_index(
        op.f("ix_messages_conversation_id_id"), "messages", ["conversation_id", "id"], unique=False
    )
    op.create_index(
        "uq_messages_conversation_id_hermes_message_id",
        "messages",
        ["conversation_id", "hermes_message_id"],
        unique=True,
        postgresql_where=sa.text("hermes_message_id IS NOT NULL"),
    )
    op.create_table(
        "push_log",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.UUID(), nullable=True),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("ref_id", sa.Text(), nullable=True),
        sa.Column(
            "sent_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("ok", sa.Boolean(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["device_id"], ["devices.id"], name=op.f("fk_push_log_device_id_devices")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_push_log")),
    )
    op.create_table(
        "runs",
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("conversation_id", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.UUID(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("input", sa.Text(), nullable=False),
        sa.Column("instructions", sa.Text(), nullable=True),
        sa.Column("thinking", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column(
            "queued_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("usage", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("perf", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("origin", sa.Text(), server_default=sa.text("'app'"), nullable=False),
        sa.Column("schedule_id", sa.UUID(), nullable=True),
        sa.CheckConstraint(
            "origin IN ('app','late_approval','schedule','system')", name=op.f("ck_runs_origin")
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name=op.f("fk_runs_conversation_id_conversations"),
        ),
        sa.PrimaryKeyConstraint("run_id", name=op.f("pk_runs")),
        sa.UniqueConstraint("idempotency_key", name=op.f("uq_runs_idempotency_key")),
    )
    op.create_index(op.f("ix_runs_status"), "runs", ["status"], unique=False)
    op.create_table(
        "schedules",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("cron", sa.Text(), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("conversation_mode", sa.Text(), nullable=False),
        sa.Column("conversation_id", sa.Text(), nullable=True),
        sa.Column("thinking", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "conversation_mode = 'new' OR conversation_id IS NOT NULL",
            name=op.f("ck_schedules_fixed_has_conv"),
        ),
        sa.CheckConstraint(
            "conversation_mode IN ('new','fixed')", name=op.f("ck_schedules_conversation_mode")
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name=op.f("fk_schedules_conversation_id_conversations"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_schedules")),
    )
    op.create_table(
        "attachments",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("conversation_id", sa.Text(), nullable=False),
        sa.Column("message_id", sa.BigInteger(), nullable=True),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("mime", sa.Text(), nullable=False),
        sa.Column("filename", sa.Text(), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("sha256", sa.Text(), nullable=False),
        sa.Column("inbox_path", sa.Text(), nullable=True),
        sa.Column("image_tokens", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("kind IN ('image','file')", name=op.f("ck_attachments_kind")),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name=op.f("fk_attachments_conversation_id_conversations"),
        ),
        sa.ForeignKeyConstraint(
            ["message_id"], ["messages.id"], name=op.f("fk_attachments_message_id_messages")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_attachments")),
    )


def downgrade() -> None:
    op.drop_table("attachments")
    op.drop_table("schedules")
    op.drop_index(op.f("ix_runs_status"), table_name="runs")
    op.drop_table("runs")
    op.drop_table("push_log")
    op.drop_index(
        "uq_messages_conversation_id_hermes_message_id",
        table_name="messages",
        postgresql_where=sa.text("hermes_message_id IS NOT NULL"),
    )
    op.drop_index(op.f("ix_messages_conversation_id_id"), table_name="messages")
    op.drop_table("messages")
    op.drop_index(op.f("ix_approvals_status_expires_at"), table_name="approvals")
    op.drop_table("approvals")
    op.drop_index(op.f("ix_tool_steps_run_id"), table_name="tool_steps")
    op.drop_table("tool_steps")
    op.drop_table("system_state")
    op.drop_table("run_events")
    op.drop_table("devices")
    op.drop_table("conversations")
    op.drop_index("ix_cards_type_ts", table_name="cards")
    op.drop_table("cards")
    op.drop_table("card_ingest_offsets")
