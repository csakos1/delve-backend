"""News texts for more stories (N4b): the merged item whose article is fetched for a story
when its own could not be (`text_from`), that article's publisher once it came
(`text_publisher`), and a summary written from the feed's excerpt when no article could be
fetched at all (`summary_basis` 'excerpt').

Revision ID: 0019
Revises: 0018
Create Date: 2026-11-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0019"
down_revision: str | None = "0018"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("news_items", sa.Column("text_from", sa.BigInteger(), nullable=True))
    op.create_foreign_key(
        op.f("fk_news_items_text_from_news_items"),
        "news_items",
        "news_items",
        ["text_from"],
        ["id"],
        ondelete="SET NULL",
    )
    op.add_column("news_items", sa.Column("text_publisher", sa.Text(), nullable=True))
    op.add_column("news_items", sa.Column("summary_basis", sa.Text(), nullable=True))
    op.create_check_constraint(
        op.f("ck_news_items_summary_basis"), "news_items", "summary_basis IN ('excerpt')"
    )


def downgrade() -> None:
    op.drop_constraint(op.f("ck_news_items_summary_basis"), "news_items", type_="check")
    op.drop_column("news_items", "summary_basis")
    op.drop_column("news_items", "text_publisher")
    op.drop_constraint(op.f("fk_news_items_text_from_news_items"), "news_items", type_="foreignkey")
    op.drop_column("news_items", "text_from")
