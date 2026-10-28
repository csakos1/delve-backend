"""Watchers (W2): watches and the items they reported; re-read the cards file once.

The watch_* cards written before this revision were skipped by the ingest as unknown
types; dropping the stored offset makes the ingest read the file again from the start
(cards it already has are kept by their id, the watch_* ones come in now).

Revision ID: 0010
Revises: 0009
Create Date: 2026-10-28
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TZ = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "watches",
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("criteria", sa.Text(), nullable=True),
        sa.Column("filters", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("stop_after_hit", sa.Boolean(), nullable=False),
        sa.Column("schedule", sa.Text(), nullable=False),
        sa.Column("def_hash", sa.Text(), nullable=False),
        sa.Column("defined_at", TZ, nullable=False),
        sa.Column("job_id", sa.Text(), nullable=True),
        sa.Column("job_state", sa.Text(), nullable=True),
        sa.Column("next_run_at", TZ, nullable=True),
        sa.Column("job_seen_at", TZ, nullable=True),
        sa.Column("last_check_at", TZ, nullable=True),
        sa.Column("last_status", sa.Text(), nullable=True),
        sa.Column("last_error_code", sa.Text(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_items", sa.Integer(), nullable=True),
        sa.Column("last_matched", sa.Integer(), nullable=True),
        sa.Column("errors_in_row", sa.Integer(), server_default="0", nullable=False),
        sa.Column("stopped_at", TZ, nullable=True),
        sa.Column("created_at", TZ, server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", TZ, server_default=sa.text("now()"), nullable=False),
        sa.Column("deleted_at", TZ, nullable=True),
        sa.CheckConstraint("last_status IN ('ok','error')", name=op.f("ck_watches_last_status")),
        sa.CheckConstraint(
            "job_state IN ('scheduled','paused')", name=op.f("ck_watches_job_state")
        ),
        sa.PrimaryKeyConstraint("slug", name=op.f("pk_watches")),
    )
    op.create_table(
        "watch_items",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("item_id", sa.Text(), nullable=False),
        sa.Column("card_id", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("price", sa.Text(), nullable=True),
        sa.Column("price_value", sa.BigInteger(), nullable=True),
        sa.Column("location", sa.Text(), nullable=True),
        sa.Column("posted", sa.Text(), nullable=True),
        sa.Column("excerpt", sa.Text(), nullable=True),
        sa.Column("reserved", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("verdict", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("notify", sa.Boolean(), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("judge", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("seen_at", TZ, nullable=False),
        sa.Column("judged_at", TZ, nullable=True),
        sa.Column("notified_at", TZ, nullable=True),
        sa.Column("available_at", TZ, nullable=True),
        sa.Column("created_at", TZ, server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "verdict IN ('pending','match','reject','mechanical','error')",
            name=op.f("ck_watch_items_verdict"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_watch_items")),
        sa.UniqueConstraint("slug", "item_id", name=op.f("uq_watch_items_slug_item_id")),
    )
    op.create_index(
        "ix_watch_items_slug_id", "watch_items", ["slug", sa.text("id DESC")], unique=False
    )
    op.create_index(
        "ix_watch_items_pending",
        "watch_items",
        ["id"],
        unique=False,
        postgresql_where=sa.text("verdict = 'pending'"),
    )
    op.execute("DELETE FROM card_ingest_offsets")


def downgrade() -> None:
    op.drop_index("ix_watch_items_pending", table_name="watch_items")
    op.drop_index("ix_watch_items_slug_id", table_name="watch_items")
    op.drop_table("watch_items")
    op.drop_table("watches")
    op.execute("DELETE FROM cards WHERE type LIKE 'watch\\_%'")
