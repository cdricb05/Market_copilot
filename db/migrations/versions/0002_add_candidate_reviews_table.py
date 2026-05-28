"""Add candidate_reviews table for review queue.

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-27
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: str | None = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # candidate_reviews
    # Review queue for candidate predictions awaiting manual approval.
    # No FKs. UUID PK.
    # ------------------------------------------------------------------
    op.create_table(
        "candidate_reviews",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "idempotency_key",
            sa.String(255),
            nullable=False,
            comment="Caller-provided key for deduplication. E.g. 'review-save-20260527-001'.",
        ),
        sa.Column(
            "ticker",
            sa.String(20),
            nullable=False,
            comment="Stock ticker symbol, uppercase.",
        ),
        # --- Scan metrics (from market scan) ---
        sa.Column(
            "scan_rank",
            sa.String(50),
            nullable=True,
            comment="Rank in market scan results. Stored as string for consistency.",
        ),
        sa.Column(
            "scan_score",
            sa.String(50),
            nullable=True,
            comment="Market scan composite score. Decimal string, e.g. '15.60'. Null if failed.",
        ),
        sa.Column(
            "latest_price",
            sa.String(50),
            nullable=True,
            comment="Latest price at scan time. Decimal string, e.g. '446.27'. Null if failed.",
        ),
        sa.Column(
            "momentum_5d_pct",
            sa.String(50),
            nullable=True,
            comment="5-day momentum percentage. Decimal string, e.g. '1.23'. Null if failed.",
        ),
        sa.Column(
            "momentum_20d_pct",
            sa.String(50),
            nullable=True,
            comment="20-day momentum percentage. Decimal string, e.g. '19.51'. Null if failed.",
        ),
        sa.Column(
            "relative_strength_vs_spy_20d",
            sa.String(50),
            nullable=True,
            comment="Relative Strength vs SPY (20-day). Decimal string, e.g. '14.04'. Null if failed.",
        ),
        sa.Column(
            "scan_reason_codes",
            postgresql.JSONB(),
            nullable=True,
            comment="Array of reason codes from market scan, e.g. ['POSITIVE_20D_MOMENTUM', 'OUTPERFORMING_SPY'].",
        ),
        # --- Prediction metrics (from GCP prediction API) ---
        sa.Column(
            "prediction_recommendation",
            sa.String(20),
            nullable=True,
            comment="BUY | SELL | HOLD. Null if prediction failed.",
        ),
        sa.Column(
            "prediction_confidence",
            sa.String(50),
            nullable=True,
            comment="Confidence score (0-1). Decimal string, e.g. '0.99'. Null if prediction failed.",
        ),
        sa.Column(
            "forecast_price_5d",
            sa.String(50),
            nullable=True,
            comment="5-day forecast price. Decimal string, e.g. '453.98'. Null if prediction failed.",
        ),
        sa.Column(
            "expected_return_pct",
            sa.String(50),
            nullable=True,
            comment="Expected return percentage. Decimal string, e.g. '1.73'. Null if prediction failed.",
        ),
        sa.Column(
            "market_context",
            sa.String(20),
            nullable=True,
            comment="bullish | bearish | neutral. Null if prediction failed.",
        ),
        # --- Preview decision summary ---
        sa.Column(
            "preview_decision",
            sa.String(20),
            nullable=False,
            comment="CONSIDER | WATCH | REJECT (from candidate preview scoring).",
        ),
        sa.Column(
            "preview_score",
            sa.String(50),
            nullable=False,
            comment="Preview decision score (0-100). Decimal string, e.g. '86.6'.",
        ),
        sa.Column(
            "preview_reasons",
            postgresql.JSONB(),
            nullable=True,
            comment="Array of reason strings explaining preview decision.",
        ),
        # --- Review status (UI-editable) ---
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default=sa.text("'OK'"),
            comment="OK | ERROR. Indicates whether the candidate was successfully saved.",
        ),
        sa.Column(
            "review_status",
            sa.String(20),
            nullable=False,
            server_default=sa.text("'NEW'"),
            comment="NEW | WATCHING | REJECTED | APPROVED_FOR_SIGNAL. User-editable review state.",
        ),
        # --- Timestamps ---
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("idempotency_key", "ticker", name="uq_candidate_reviews_key_ticker"),
    )
    op.create_index(
        "ix_candidate_reviews_review_status",
        "candidate_reviews",
        ["review_status"],
    )
    op.create_index(
        "ix_candidate_reviews_ticker",
        "candidate_reviews",
        ["ticker"],
    )
    op.create_index(
        "ix_candidate_reviews_created_at",
        "candidate_reviews",
        ["created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_candidate_reviews_created_at", table_name="candidate_reviews")
    op.drop_index("ix_candidate_reviews_ticker", table_name="candidate_reviews")
    op.drop_index("ix_candidate_reviews_review_status", table_name="candidate_reviews")
    op.drop_table("candidate_reviews")
