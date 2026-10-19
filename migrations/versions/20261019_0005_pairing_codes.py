"""pairing_codes: one-time codes for pairing a phone (plan 12/8).

Revision ID: 0005
Revises: 0004
Create Date: 2026-10-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "pairing_codes",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("code_sha256", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("device_id", sa.UUID(), nullable=True),
        sa.ForeignKeyConstraint(
            ["device_id"], ["devices.id"], name=op.f("fk_pairing_codes_device_id_devices")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_pairing_codes")),
        sa.UniqueConstraint("code_sha256", name=op.f("uq_pairing_codes_code_sha256")),
    )


def downgrade() -> None:
    op.drop_table("pairing_codes")
