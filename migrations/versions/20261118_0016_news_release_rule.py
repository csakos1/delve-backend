"""News (N1b fix): a release row (one day of one feed's releases) is scored by rule (6, good
to know) and never grouped; the release rows the model scored, moved to another topic or
merged with other rows go back to their feed's topic, with 6, on their own.

Revision ID: 0016
Revises: 0015
Create Date: 2026-11-18
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0016"
down_revision: str | None = "0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "UPDATE news_items SET dup_of = NULL WHERE kind = 'release' "
        "OR dup_of IN (SELECT id FROM news_items WHERE kind = 'release')"
    )
    op.execute(
        "UPDATE news_items AS i SET score = 6, "
        "scored_at = coalesce(i.scored_at, i.seen_at), "
        "grouped_at = coalesce(i.grouped_at, i.seen_at), "
        "topic_id = coalesce("
        "(SELECT s.topic_id FROM news_sources AS s WHERE s.id = i.source_id), i.topic_id) "
        "WHERE i.kind = 'release'"
    )


def downgrade() -> None:
    pass  # the scores and groups before it are not kept
