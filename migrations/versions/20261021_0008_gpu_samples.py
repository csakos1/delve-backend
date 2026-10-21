"""GPU samples for B5 (Observability): one row per GPU per sampling interval.

Revision ID: 0008
Revises: 0007
Create Date: 2026-10-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "gpu_samples",
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("gpu_index", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("name", sa.Text(), nullable=True),
        sa.Column("temp_c", sa.REAL(), nullable=True),
        sa.Column("power_w", sa.REAL(), nullable=True),
        sa.Column("util_pct", sa.REAL(), nullable=True),
        sa.Column("mem_used_mib", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("ts", "gpu_index", name=op.f("pk_gpu_samples")),
    )


def downgrade() -> None:
    op.drop_table("gpu_samples")
