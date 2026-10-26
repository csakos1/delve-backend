"""Attachments for B6: an upload is unbound until chat.send binds it to its message.

Revision ID: 0009
Revises: 0008
Create Date: 2026-10-26
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column("attachments", "conversation_id", existing_type=sa.Text(), nullable=True)
    op.create_index(op.f("ix_attachments_message_id"), "attachments", ["message_id"])
    op.create_index(op.f("ix_attachments_conversation_id"), "attachments", ["conversation_id"])


def downgrade() -> None:
    op.drop_index(op.f("ix_attachments_conversation_id"), table_name="attachments")
    op.drop_index(op.f("ix_attachments_message_id"), table_name="attachments")
    op.execute("DELETE FROM attachments WHERE conversation_id IS NULL")
    op.alter_column("attachments", "conversation_id", existing_type=sa.Text(), nullable=False)
