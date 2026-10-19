"""tool_steps.approval_request_id; approval status check (plan 6.5).

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

APPROVAL_STATUSES = (
    "'pending','approved_once','approved_session','denied','expired_in_hermes',"
    "'late_approved','followup_started','executed','superseded','expired','cancelled'"
)


def upgrade() -> None:
    op.add_column("tool_steps", sa.Column("approval_request_id", sa.Text(), nullable=True))
    op.create_check_constraint("status", "approvals", f"status IN ({APPROVAL_STATUSES})")


def downgrade() -> None:
    op.drop_constraint("status", "approvals", type_="check")
    op.drop_column("tool_steps", "approval_request_id")
