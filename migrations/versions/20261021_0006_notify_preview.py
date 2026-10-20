"""devices.notify (per-device notification settings) and conversations.preview.

Revision ID: 0006
Revises: 0005
Create Date: 2026-10-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

NOTIFY_DEFAULT = (
    """'{"approvals": true, "run_done": true, "water": false, "brief": false}'::jsonb"""
)


def upgrade() -> None:
    op.add_column(
        "devices",
        sa.Column(
            "notify",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text(NOTIFY_DEFAULT),
            nullable=False,
        ),
    )
    op.add_column("conversations", sa.Column("preview", sa.Text(), nullable=True))
    # the latest non-empty user or assistant text, whitespace collapsed, 200 characters
    op.execute(
        """
        UPDATE conversations c SET preview = (
            SELECT left(btrim(regexp_replace(m.content, '\\s+', ' ', 'g')), 200)
            FROM messages m
            WHERE m.conversation_id = c.id
              AND m.role IN ('user', 'assistant')
              AND btrim(m.content) <> ''
            ORDER BY m.id DESC
            LIMIT 1
        )
        """
    )


def downgrade() -> None:
    op.drop_column("conversations", "preview")
    op.drop_column("devices", "notify")
