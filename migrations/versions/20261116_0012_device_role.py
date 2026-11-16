"""Notification mirroring (DA2): a device's role - the app, or the desktop agent.

Revision ID: 0012
Revises: 0011
Create Date: 2026-11-16
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "devices", sa.Column("role", sa.Text(), nullable=False, server_default=sa.text("'app'"))
    )
    op.create_check_constraint("devices_role_check", "devices", "role IN ('app', 'desk')")


def downgrade() -> None:
    op.drop_constraint("devices_role_check", "devices", type_="check")
    op.drop_column("devices", "role")
