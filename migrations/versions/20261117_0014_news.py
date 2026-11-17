"""News (N1): topics, the feeds the agent's collector reads, the items it found, and the
batch files already read.

Revision ID: 0014
Revises: 0013
Create Date: 2026-11-17
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0014"
down_revision: str | None = "0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TZ = sa.DateTime(timezone=True)


def _now(name: str) -> sa.Column:  # type: ignore[type-arg]
    return sa.Column(name, TZ, server_default=sa.text("now()"), nullable=False)


def upgrade() -> None:
    op.create_table(
        "news_topics",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), server_default="", nullable=False),
        sa.Column("position", sa.Integer(), server_default="0", nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default="true", nullable=False),
        _now("created_at"),
        _now("updated_at"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_news_topics")),
    )
    op.create_table(
        "news_sources",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("topic_id", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("kind", sa.Text(), server_default="feed", nullable=False),
        sa.Column("window_h", sa.Integer(), server_default="48", nullable=False),
        sa.Column("every_min", sa.Integer(), server_default="60", nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default="true", nullable=False),
        sa.Column("origin", sa.Text(), server_default="import", nullable=False),
        sa.Column("last_at", TZ, nullable=True),
        sa.Column("last_ok_at", TZ, nullable=True),
        sa.Column("last_status", sa.Text(), nullable=True),
        sa.Column("last_http", sa.Integer(), nullable=True),
        sa.Column("last_error_code", sa.Text(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_items", sa.Integer(), nullable=True),
        sa.Column("last_new", sa.Integer(), nullable=True),
        sa.Column("errors_in_row", sa.Integer(), server_default="0", nullable=False),
        _now("created_at"),
        _now("updated_at"),
        sa.CheckConstraint("kind IN ('feed','release')", name=op.f("ck_news_sources_kind")),
        sa.CheckConstraint(
            "origin IN ('import','app','agent')", name=op.f("ck_news_sources_origin")
        ),
        sa.CheckConstraint(
            "last_status IN ('ok','not_modified','skipped','error')",
            name=op.f("ck_news_sources_last_status"),
        ),
        sa.ForeignKeyConstraint(
            ["topic_id"],
            ["news_topics.id"],
            name=op.f("fk_news_sources_topic_id_news_topics"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_news_sources")),
        sa.UniqueConstraint("url", name=op.f("uq_news_sources_url")),
    )
    op.create_table(
        "news_items",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("source_id", sa.Text(), nullable=True),
        sa.Column("topic_id", sa.Text(), nullable=True),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("publisher", sa.Text(), nullable=True),
        sa.Column("excerpt", sa.Text(), nullable=True),
        sa.Column("published_at", TZ, nullable=True),
        sa.Column("seen_at", TZ, nullable=False),
        sa.Column("gn", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("norm_title", sa.Text(), nullable=False),
        sa.Column("dup_of", sa.BigInteger(), nullable=True),
        sa.Column("releases", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("score", sa.Integer(), nullable=True),
        sa.Column("why", sa.Text(), nullable=True),
        sa.Column("setup", sa.Boolean(), nullable=True),
        sa.Column("summary_hu", sa.Text(), nullable=True),
        sa.Column("scored_at", TZ, nullable=True),
        sa.Column("read_at", TZ, nullable=True),
        sa.Column("saved_at", TZ, nullable=True),
        sa.Column("feedback", sa.Text(), nullable=True),
        sa.Column("feedback_at", TZ, nullable=True),
        _now("created_at"),
        sa.CheckConstraint("kind IN ('feed','release')", name=op.f("ck_news_items_kind")),
        sa.CheckConstraint("feedback IN ('more','less')", name=op.f("ck_news_items_feedback")),
        sa.ForeignKeyConstraint(
            ["dup_of"],
            ["news_items.id"],
            name=op.f("fk_news_items_dup_of_news_items"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["news_sources.id"],
            name=op.f("fk_news_items_source_id_news_sources"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["topic_id"],
            ["news_topics.id"],
            name=op.f("fk_news_items_topic_id_news_topics"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_news_items")),
        sa.UniqueConstraint("key", name=op.f("uq_news_items_key")),
    )
    op.create_index(
        "ix_news_items_lead",
        "news_items",
        [sa.text("id DESC")],
        unique=False,
        postgresql_where=sa.text("dup_of IS NULL"),
    )
    op.create_index("ix_news_items_dup_of", "news_items", ["dup_of"], unique=False)
    op.create_index("ix_news_items_norm_title", "news_items", ["norm_title"], unique=False)
    op.create_table(
        "news_batches",
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("at", TZ, nullable=True),
        sa.Column("items", sa.Integer(), server_default="0", nullable=False),
        sa.Column("new", sa.Integer(), server_default="0", nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        _now("ingested_at"),
        sa.PrimaryKeyConstraint("name", name=op.f("pk_news_batches")),
    )


def downgrade() -> None:
    op.drop_table("news_batches")
    op.drop_index("ix_news_items_norm_title", table_name="news_items")
    op.drop_index("ix_news_items_dup_of", table_name="news_items")
    op.drop_index("ix_news_items_lead", table_name="news_items")
    op.drop_table("news_items")
    op.drop_table("news_sources")
    op.drop_table("news_topics")
    op.execute("DELETE FROM system_state WHERE key = 'news'")
