"""Add prediction_runs table for local prediction-run capture.

Revision ID: 0004
Revises: 0003
Create Date: 2026-06-15

Local-only evidence store of every call Paper Trader makes to the remote GCP
prediction service. Append-only, no FKs, observational only — writing a row
never creates a signal, decision, order, trade, fill, or broker action.
See docs/prediction_service_audit_v1.md for the motivating gap.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0004"
down_revision: str | None = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "prediction_runs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "ticker",
            sa.String(20),
            nullable=False,
            comment="Stock ticker symbol Paper Trader requested a prediction for.",
        ),
        # --- Request / response timing ---
        sa.Column(
            "request_ts",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="When Paper Trader issued the prediction request (UTC).",
        ),
        sa.Column(
            "response_ts",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="When the response (or failure) was observed. Null if never observed.",
        ),
        sa.Column(
            "latency_ms",
            sa.Integer(),
            nullable=True,
            comment="Round-trip latency in milliseconds. Null if not measured.",
        ),
        # --- Request provenance (no secrets) ---
        sa.Column(
            "prediction_service_url",
            sa.String(500),
            nullable=True,
            comment="Endpoint URL used. URL only; never contains API keys or credentials.",
        ),
        sa.Column(
            "request_payload",
            postgresql.JSONB(),
            nullable=True,
            comment="Exact JSON body Paper Trader sent, e.g. {'ticker': 'AAPL'}.",
        ),
        sa.Column(
            "http_status",
            sa.Integer(),
            nullable=True,
            comment="HTTP status code observed, when available.",
        ),
        # --- Raw response evidence ---
        sa.Column(
            "raw_response",
            postgresql.JSONB(),
            nullable=True,
            comment="Full raw JSON response from the prediction service. Null on hard fetch failure.",
        ),
        # --- Normalized fields (Paper Trader prediction contract) ---
        sa.Column(
            "normalized_recommendation",
            sa.String(20),
            nullable=True,
            comment="BUY | SELL | HOLD after normalization. Null if normalization failed.",
        ),
        sa.Column(
            "normalized_confidence",
            sa.String(50),
            nullable=True,
            comment="Confidence (0-1) as Decimal string. Null if normalization failed.",
        ),
        sa.Column(
            "normalized_expected_return_pct",
            sa.String(50),
            nullable=True,
            comment="Expected 5-day return percent as Decimal string. Null if unavailable.",
        ),
        sa.Column(
            "normalized_forecast_price_5d",
            sa.String(50),
            nullable=True,
            comment="5-day forecast price as Decimal string. Null if unavailable.",
        ),
        sa.Column(
            "model_consensus",
            postgresql.JSONB(),
            nullable=True,
            comment="Normalized per-model votes derived from per_model_summary, e.g. {'Drift': 'BUY'}.",
        ),
        # --- Remote execution diagnostics (if the service reports them) ---
        sa.Column(
            "ran_models",
            postgresql.JSONB(),
            nullable=True,
            comment="ran_models from the raw response, if present. Else null.",
        ),
        sa.Column(
            "skipped_models",
            postgresql.JSONB(),
            nullable=True,
            comment="skipped_models from the raw response, if present. Else null.",
        ),
        sa.Column(
            "model_errors",
            postgresql.JSONB(),
            nullable=True,
            comment="model_errors from the raw response, if present. Else null.",
        ),
        sa.Column(
            "service_version",
            sa.String(100),
            nullable=True,
            comment="Remote model/service version if exposed. Null if missing (the GCP service does not expose it in the response today).",
        ),
        # --- Failure flags ---
        sa.Column(
            "error",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
            comment="True if the prediction failed (fetch error or service-reported error key).",
        ),
        sa.Column(
            "error_message",
            sa.Text(),
            nullable=True,
            comment="Human-readable failure reason. Null on success.",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "ix_prediction_runs_ticker",
        "prediction_runs",
        ["ticker"],
    )
    op.create_index(
        "ix_prediction_runs_created_at",
        "prediction_runs",
        ["created_at"],
    )
    op.create_index(
        "ix_prediction_runs_ticker_created_at",
        "prediction_runs",
        ["ticker", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_prediction_runs_ticker_created_at", table_name="prediction_runs")
    op.drop_index("ix_prediction_runs_created_at", table_name="prediction_runs")
    op.drop_index("ix_prediction_runs_ticker", table_name="prediction_runs")
    op.drop_table("prediction_runs")
