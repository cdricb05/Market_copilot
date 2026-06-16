"""Add daily-review session linkage to prediction_runs.

Revision ID: 0005
Revises: 0004
Create Date: 2026-06-15

Adds two nullable columns so a captured prediction run can be tied back to the
Daily Review session that dispatched it, without timestamp guessing:

    daily_session_id  - the Daily Review session identifier (same value the UI
                        uses as the CandidateReview idempotency_key for that
                        session). Null for runs captured before this column or
                        for ad-hoc dispatches with no session.
    source            - capture context: DAILY_REVIEW | PREDICTION_PREVIEW |
                        MARKET_SCAN. Null for legacy rows.

Still observational only: writing these columns never creates a signal,
decision, order, trade, fill, or broker action. No FKs (the table stays
decoupled from the trading lifecycle on purpose). No secrets are stored.
See docs/prediction_service_audit_v1.md for the motivating gap.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0005"
down_revision: str | None = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "prediction_runs",
        sa.Column(
            "daily_session_id",
            sa.String(100),
            nullable=True,
            comment=(
                "Daily Review session identifier this run was dispatched under "
                "(matches the CandidateReview idempotency_key for the session). "
                "Null for legacy rows or ad-hoc dispatches."
            ),
        ),
    )
    op.add_column(
        "prediction_runs",
        sa.Column(
            "source",
            sa.String(30),
            nullable=True,
            comment=(
                "Capture context: DAILY_REVIEW | PREDICTION_PREVIEW | "
                "MARKET_SCAN. Null for legacy rows."
            ),
        ),
    )
    op.create_index(
        "ix_prediction_runs_daily_session_id",
        "prediction_runs",
        ["daily_session_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_prediction_runs_daily_session_id",
        table_name="prediction_runs",
    )
    op.drop_column("prediction_runs", "source")
    op.drop_column("prediction_runs", "daily_session_id")
