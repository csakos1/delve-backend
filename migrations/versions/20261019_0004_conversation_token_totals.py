"""conversations.tokens_in/out/cache_read: running usage totals (plan 6.13).

Revision ID: 0004
Revises: 0003
Create Date: 2026-10-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

COLUMNS = ("tokens_in", "tokens_out", "tokens_cache_read")


def upgrade() -> None:
    for name in COLUMNS:
        op.add_column(
            "conversations",
            sa.Column(name, sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        )
    # backfill from runs finished before this migration
    op.execute(
        """
        UPDATE conversations c SET
            tokens_in = t.i, tokens_out = t.o, tokens_cache_read = t.cr
        FROM (
            SELECT conversation_id,
                   coalesce(sum((usage->>'input_tokens')::bigint), 0) AS i,
                   coalesce(sum((usage->>'output_tokens')::bigint), 0) AS o,
                   coalesce(sum((usage->>'cache_read_tokens')::bigint), 0) AS cr
            FROM runs
            WHERE jsonb_typeof(usage) = 'object'
            GROUP BY conversation_id
        ) t
        WHERE t.conversation_id = c.id
        """
    )


def downgrade() -> None:
    for name in reversed(COLUMNS):
        op.drop_column("conversations", name)
