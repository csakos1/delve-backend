"""News (N1b): the model steps' bookkeeping on the items (tries, grouping, push), and the
long feed titles some publishers carry cut at their first ": " (the publisher's name).

Revision ID: 0015
Revises: 0014
Create Date: 2026-11-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0015"
down_revision: str | None = "0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TZ = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.add_column(
        "news_items", sa.Column("score_attempts", sa.Integer(), server_default="0", nullable=False)
    )
    op.add_column(
        "news_items", sa.Column("why_attempts", sa.Integer(), server_default="0", nullable=False)
    )
    op.add_column("news_items", sa.Column("grouped_at", TZ, nullable=True))
    op.add_column("news_items", sa.Column("pushed_at", TZ, nullable=True))
    op.execute(
        "UPDATE news_items SET publisher = split_part(publisher, ': ', 1) "
        "WHERE length(publisher) > 30 AND position(': ' in publisher) > 3"
    )


def downgrade() -> None:
    op.drop_column("news_items", "pushed_at")
    op.drop_column("news_items", "grouped_at")
    op.drop_column("news_items", "why_attempts")
    op.drop_column("news_items", "score_attempts")
