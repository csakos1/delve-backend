"""News (N1c): the article's text on the item - its state (queued for the agent's
collector, ok, failed), the publisher's address, when, which try, when the user asked for
it - and the summary step's tries.

Revision ID: 0017
Revises: 0016
Create Date: 2026-11-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0017"
down_revision: str | None = "0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TZ = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.add_column("news_items", sa.Column("text", sa.Text(), nullable=True))
    op.add_column("news_items", sa.Column("text_state", sa.Text(), nullable=True))
    op.add_column("news_items", sa.Column("text_error", sa.Text(), nullable=True))
    op.add_column("news_items", sa.Column("text_url", sa.Text(), nullable=True))
    op.add_column("news_items", sa.Column("text_at", TZ, nullable=True))
    op.add_column(
        "news_items", sa.Column("text_try", sa.Integer(), server_default="0", nullable=False)
    )
    op.add_column("news_items", sa.Column("text_requested_at", TZ, nullable=True))
    op.add_column(
        "news_items",
        sa.Column("summary_attempts", sa.Integer(), server_default="0", nullable=False),
    )
    op.create_check_constraint(
        op.f("ck_news_items_text_state"), "news_items", "text_state IN ('queued','ok','failed')"
    )
    op.create_index(
        "ix_news_items_text_state",
        "news_items",
        ["text_state"],
        unique=False,
        postgresql_where=sa.text("text_state IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_news_items_text_state", table_name="news_items")
    op.drop_constraint(op.f("ck_news_items_text_state"), "news_items", type_="check")
    for col in ("summary_attempts", "text_requested_at", "text_try", "text_at", "text_url",
                "text_error", "text_state", "text"):  # fmt: skip
        op.drop_column("news_items", col)
