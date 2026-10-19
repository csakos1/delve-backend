"""Run and message status checks; messages.interim (N27).

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

RUN_STATUSES = (
    "'queued','starting','running','waiting_for_approval','stopping','reconnecting',"
    "'completed','cancelled','failed'"
)
MESSAGE_STATUSES = "'streaming','final','cancelled','failed'"


def upgrade() -> None:
    op.add_column("messages", sa.Column("interim", sa.Text(), nullable=True))
    op.create_check_constraint("status", "messages", f"status IN ({MESSAGE_STATUSES})")
    op.create_check_constraint("status", "runs", f"status IN ({RUN_STATUSES})")


def downgrade() -> None:
    op.drop_constraint("status", "runs", type_="check")
    op.drop_constraint("status", "messages", type_="check")
    op.drop_column("messages", "interim")
