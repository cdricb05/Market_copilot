"""Add review_reason_code and review_note to candidate_reviews.

Revision ID: 0003
Revises: 0002
Create Date: 2026-06-10
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003"
down_revision: str | None = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "candidate_reviews",
        sa.Column(
            "review_reason_code",
            sa.String(50),
            nullable=True,
            comment="Reason code for review action, e.g. STRONG_MODEL_SIGNAL.",
        ),
    )
    op.add_column(
        "candidate_reviews",
        sa.Column(
            "review_note",
            sa.String(500),
            nullable=True,
            comment="Optional free-text review note. Not used for trading.",
        ),
    )


def downgrade() -> None:
    op.drop_column("candidate_reviews", "review_note")
    op.drop_column("candidate_reviews", "review_reason_code")
