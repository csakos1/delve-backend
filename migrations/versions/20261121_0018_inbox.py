"""Inbox (I0): what the reader cleared - the item's kind and reference, the mark of the
state it was cleared in (a notice comes back when its cause returns), the item as it was
shown, and when.

Revision ID: 0018
Revises: 0017
Create Date: 2026-11-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0018"
down_revision: str | None = "0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TZ = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "inbox_done",
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("ref", sa.Text(), nullable=False),
        sa.Column("mark", sa.Text(), server_default="", nullable=False),
        sa.Column("item", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("done_at", TZ, nullable=False),
        sa.PrimaryKeyConstraint("kind", "ref", name=op.f("pk_inbox_done")),
    )
    op.create_index(op.f("ix_inbox_done_done_at"), "inbox_done", ["done_at"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_inbox_done_done_at"), table_name="inbox_done")
    op.drop_table("inbox_done")
