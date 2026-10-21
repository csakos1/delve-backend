"""Schedules for B4: the morning brief as a built-in schedule, last outcome, runs index.

Revision ID: 0007
Revises: 0006
Create Date: 2026-10-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "schedules",
        sa.Column("kind", sa.Text(), server_default=sa.text("'prompt'"), nullable=False),
    )
    op.alter_column("schedules", "prompt", existing_type=sa.Text(), nullable=True)
    op.add_column("schedules", sa.Column("last_status", sa.Text(), nullable=True))
    op.add_column("schedules", sa.Column("last_error", sa.Text(), nullable=True))
    op.add_column(
        "schedules",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_check_constraint(op.f("ck_schedules_kind"), "schedules", "kind IN ('prompt','brief')")
    op.create_check_constraint(
        op.f("ck_schedules_prompt_has_text"), "schedules", "kind = 'brief' OR prompt IS NOT NULL"
    )
    op.create_index(
        "uq_schedules_brief",
        "schedules",
        ["kind"],
        unique=True,
        postgresql_where=sa.text("kind = 'brief'"),
    )
    op.create_index(op.f("ix_runs_schedule_id"), "runs", ["schedule_id"], unique=False)
    # the morning brief: daily 07:30 (Europe/Budapest), enabled; the scheduler sets next_run_at
    op.execute(
        """
        INSERT INTO schedules (id, kind, name, cron, conversation_mode, enabled)
        VALUES (gen_random_uuid(), 'brief', 'Morning brief', '30 7 * * *', 'new', true)
        """
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_runs_schedule_id"), table_name="runs")
    op.execute("DELETE FROM schedules WHERE kind = 'brief'")
    op.drop_index("uq_schedules_brief", table_name="schedules")
    op.drop_constraint(op.f("ck_schedules_prompt_has_text"), "schedules", type_="check")
    op.drop_constraint(op.f("ck_schedules_kind"), "schedules", type_="check")
    op.drop_column("schedules", "updated_at")
    op.drop_column("schedules", "last_error")
    op.drop_column("schedules", "last_status")
    op.alter_column("schedules", "prompt", existing_type=sa.Text(), nullable=False)
    op.drop_column("schedules", "kind")
