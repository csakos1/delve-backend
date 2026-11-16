"""Personal profile (P1): the profile text a conversation started with.

Conversations that exist already started without one ('') and stay that way; NULL is a
conversation whose first run has not started yet.

Revision ID: 0013
Revises: 0012
Create Date: 2026-11-17
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0013"
down_revision: str | None = "0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("conversations", sa.Column("persona", sa.Text(), nullable=True))
    op.execute("UPDATE conversations SET persona = ''")


def downgrade() -> None:
    op.drop_column("conversations", "persona")
