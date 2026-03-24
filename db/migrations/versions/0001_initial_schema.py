"""Initial schema — all Phase 1 tables.

Revision ID: 0001
Revises:
Create Date: 2026-03-23
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: str | None = None
branch_labels = None
depends_on = None


def upgrade() -> None:

    # ------------------------------------------------------------------
    # portfolio
    # No FKs. Integer PK (single-row table).
    # ------------------------------------------------------------------
    op.create_table(
        "portfolio",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column(
            "initial_capital",
            sa.Numeric(18, 2),
            nullable=False,
            comment="Set at seed time. Never modified after inception.",
        ),
        sa.Column(
            "inception_date",
            sa.Date(),
            nullable=False,
            comment="US Eastern trading date on which the portfolio was seeded.",
        ),
        sa.Column(
            "cached_cash",
            sa.Numeric(18, 2),
            nullable=False,
            comment="Cache of SUM(cash_ledger.amount). Refreshed by reconciler after each fill cycle.",
        ),
        sa.Column(
            "cached_total_value",
            sa.Numeric(18, 2),
            nullable=False,
            comment="Cache of cash + mark-to-market positions value. Refreshed by reconciler.",
        ),
        sa.Column(
            "cached_as_of_ts",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Timestamp of the last reconciler refresh. None until first fill cycle.",
        ),
        sa.Column(
            "strategy_enabled",
            sa.Boolean(),
            nullable=False,
            comment="Master switch. False = signals are received but not processed.",
        ),
        sa.Column(
            "trading_enabled",
            sa.Boolean(),
            nullable=False,
            comment="False = no new orders are generated regardless of signals.",
        ),
        sa.Column(
            "allow_new_positions",
            sa.Boolean(),
            nullable=False,
            comment="False = only closes allowed; no BUY orders for tickers not already held.",
        ),
        sa.Column(
            "pause_reason",
            sa.Text(),
            nullable=True,
            comment="Human-readable explanation for any kill switch being disabled.",
        ),
        sa.Column(
            "config",
            postgresql.JSONB(),
            nullable=False,
            comment="Risk and execution parameters. See Portfolio docstring for expected keys.",
        ),
        sa.CheckConstraint(
            "initial_capital > 0",
            name="ck_portfolio_initial_capital_positive",
        ),
        sa.CheckConstraint(
            "cached_cash >= 0",
            name="ck_portfolio_cached_cash_nonneg",
        ),
    )

    # ------------------------------------------------------------------
    # job_runs
    # No FKs. Idempotency anchor for all workflow runs.
    # ------------------------------------------------------------------
    op.create_table(
        "job_runs",
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
            comment="Caller-provided key. Typically: 'pre_market_20260322'.",
        ),
        sa.Column(
            "workflow_type",
            sa.String(50),
            nullable=False,
            comment="PRE_MARKET | MIDDAY | POST_MARKET",
        ),
        sa.Column(
            "market_date",
            sa.Date(),
            nullable=False,
            comment="US Eastern trading date for this run.",
        ),
        sa.Column(
            "status",
            sa.String(50),
            nullable=False,
            comment="RUNNING | COMPLETED | FAILED | SKIPPED",
        ),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "result_summary",
            postgresql.JSONB(),
            nullable=True,
            comment=(
                "Populated on COMPLETED. Keys: signals_ingested, decisions_made, "
                "orders_created, fills_executed, positions_opened, positions_closed."
            ),
        ),
        sa.Column(
            "error_detail",
            sa.Text(),
            nullable=True,
            comment="First error message if status=FAILED.",
        ),
        sa.UniqueConstraint("idempotency_key", name="uq_job_runs_idempotency_key"),
    )
    op.create_index(
        "ix_job_runs_market_date_workflow",
        "job_runs",
        ["market_date", "workflow_type"],
    )
    op.create_index(
        "ix_job_runs_status_running",
        "job_runs",
        ["status"],
        postgresql_where=sa.text("status = 'RUNNING'"),
    )

    # ------------------------------------------------------------------
    # signals
    # FK → job_runs
    # ------------------------------------------------------------------
    op.create_table(
        "signals",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "job_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job_runs.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("ticker", sa.String(20), nullable=False),
        sa.Column(
            "direction",
            sa.String(10),
            nullable=False,
            comment="BUY | SELL | HOLD",
        ),
        sa.Column("confidence", sa.Numeric(5, 4), nullable=False),
        sa.Column(
            "signal_ts",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="When the prediction engine generated this signal.",
        ),
        sa.Column(
            "market_date",
            sa.Date(),
            nullable=False,
            comment="US Eastern trading date.",
        ),
        sa.Column(
            "source_run",
            sa.String(255),
            nullable=False,
            comment="Label from the prediction engine run, e.g. 'pre_market_20260322'.",
        ),
        sa.Column(
            "status",
            sa.String(50),
            nullable=False,
            comment="RECEIVED | PROCESSING | DECISION_MADE | EXPIRED | ERROR",
        ),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column(
            "raw_payload",
            postgresql.JSONB(),
            nullable=True,
            comment="Full prediction engine output for this signal.",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "confidence >= 0 AND confidence <= 1",
            name="ck_signals_confidence_range",
        ),
        sa.UniqueConstraint(
            "source_run",
            "ticker",
            "direction",
            name="uq_signals_source_run_ticker_direction",
        ),
    )
    op.create_index(
        "ix_signals_market_date_status", "signals", ["market_date", "status"]
    )
    op.create_index("ix_signals_job_run_id", "signals", ["job_run_id"])
    op.create_index(
        "ix_signals_ticker_market_date", "signals", ["ticker", "market_date"]
    )

    # ------------------------------------------------------------------
    # trade_decisions
    # FK → signals, job_runs
    # No order_id FK — navigate via SELECT … WHERE trade_decision_id = :id
    # ------------------------------------------------------------------
    op.create_table(
        "trade_decisions",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "signal_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("signals.id", ondelete="RESTRICT"),
            nullable=False,
            comment="One-to-one with signals. A signal gets exactly one decision.",
        ),
        sa.Column(
            "job_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job_runs.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("ticker", sa.String(20), nullable=False),
        sa.Column(
            "signal_direction",
            sa.String(10),
            nullable=False,
            comment="What the signal said: BUY | SELL | HOLD",
        ),
        sa.Column(
            "decision",
            sa.String(20),
            nullable=False,
            comment="BUY | SELL | HOLD | REJECTED",
        ),
        sa.Column(
            "reason_code",
            sa.String(60),
            nullable=True,
            comment="Populated for all outcomes. See RejectionReason constants for valid values.",
        ),
        sa.Column(
            "requested_notional",
            sa.Numeric(18, 2),
            nullable=True,
            comment="Target dollar amount before risk sizing clamps.",
        ),
        sa.Column(
            "approved_notional",
            sa.Numeric(18, 2),
            nullable=True,
            comment="Dollar amount after all sizing clamps. Null if rejected before sizing.",
        ),
        sa.Column("requested_qty", sa.Numeric(18, 8), nullable=True),
        sa.Column(
            "approved_qty",
            sa.Numeric(18, 8),
            nullable=True,
            comment="floor(approved_notional / snapshot_price). Null if rejected.",
        ),
        sa.Column(
            "risk_snapshot",
            postgresql.JSONB(),
            nullable=True,
            comment=(
                "Portfolio state at decision time: "
                "{cash, total_value, open_position_count, "
                "daily_exposure_used, daily_exposure_limit}"
            ),
        ),
        sa.Column(
            "sizing_adjustments",
            postgresql.JSONB(),
            nullable=True,
            comment=(
                "Log of sizing clamps applied, e.g. "
                "[{rule: 'concentration_cap', before: '2500.00', after: '2000.00'}]"
            ),
        ),
        sa.Column(
            "decided_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "market_date",
            sa.Date(),
            nullable=False,
            comment="Denormalised from job_run for efficient daily exposure queries.",
        ),
        sa.UniqueConstraint("signal_id", name="uq_trade_decisions_signal_id"),
    )
    op.create_index(
        "ix_trade_decisions_ticker_market_date",
        "trade_decisions",
        ["ticker", "market_date"],
    )
    op.create_index(
        "ix_trade_decisions_decision_reason",
        "trade_decisions",
        ["decision", "reason_code"],
    )
    op.create_index(
        "ix_trade_decisions_job_run_id", "trade_decisions", ["job_run_id"]
    )
    op.create_index(
        "ix_trade_decisions_market_date", "trade_decisions", ["market_date"]
    )

    # ------------------------------------------------------------------
    # orders
    # FK → trade_decisions (NOT NULL), job_runs (x2: creation + fill run)
    # ------------------------------------------------------------------
    op.create_table(
        "orders",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "trade_decision_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("trade_decisions.id", ondelete="RESTRICT"),
            nullable=False,
            comment="The trade_decision that produced this order.",
        ),
        sa.Column(
            "job_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job_runs.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "fill_job_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job_runs.id", ondelete="SET NULL"),
            nullable=True,
            comment="Which fill-cycle run filled this order. Null until filled.",
        ),
        sa.Column("ticker", sa.String(20), nullable=False),
        sa.Column(
            "side",
            sa.String(10),
            nullable=False,
            comment="BUY | SELL",
        ),
        sa.Column(
            "order_type",
            sa.String(20),
            nullable=False,
            comment="MARKET only in v1.",
        ),
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            comment="PENDING | FILLED | CANCELLED | EXPIRED | FAILED",
        ),
        sa.Column(
            "market_date",
            sa.Date(),
            nullable=False,
            comment="US Eastern trading date of order creation.",
        ),
        sa.Column("requested_qty", sa.Numeric(18, 8), nullable=False),
        sa.Column(
            "filled_qty",
            sa.Numeric(18, 8),
            nullable=True,
            comment="Null until filled. Equals requested_qty in v1 (no partial fills).",
        ),
        sa.Column(
            "requested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "fill_price",
            sa.Numeric(18, 6),
            nullable=True,
            comment="Post-slippage fill price per share.",
        ),
        sa.Column("commission", sa.Numeric(18, 2), nullable=True),
        sa.Column(
            "slippage_cost",
            sa.Numeric(18, 2),
            nullable=True,
            comment="(fill_price - snapshot_price) * qty. Positive for BUY, negative for SELL.",
        ),
        sa.Column(
            "notes",
            sa.Text(),
            nullable=True,
            comment="Audit note, e.g. cancellation reason or TTL expiry detail.",
        ),
        sa.CheckConstraint(
            "requested_qty > 0", name="ck_orders_requested_qty_positive"
        ),
        sa.CheckConstraint(
            "filled_qty >= 0", name="ck_orders_filled_qty_nonneg"
        ),
    )
    op.create_index(
        "ix_orders_status_requested_at", "orders", ["status", "requested_at"]
    )
    op.create_index(
        "ix_orders_ticker_status_pending",
        "orders",
        ["ticker", "status"],
        postgresql_where=sa.text("status = 'PENDING'"),
    )
    op.create_index("ix_orders_job_run_id", "orders", ["job_run_id"])
    op.create_index("ix_orders_market_date", "orders", ["market_date"])

    # ------------------------------------------------------------------
    # positions
    # No FKs. At most max_positions rows (5 in v1).
    # ------------------------------------------------------------------
    op.create_table(
        "positions",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("ticker", sa.String(20), nullable=False),
        sa.Column(
            "qty",
            sa.Numeric(18, 8),
            nullable=False,
            comment="Shares held. Must be > 0; delete row on full close.",
        ),
        sa.Column(
            "avg_cost",
            sa.Numeric(18, 6),
            nullable=False,
            comment="Weighted-average cost per share (WAC basis).",
        ),
        sa.Column(
            "cost_basis",
            sa.Numeric(18, 2),
            nullable=False,
            comment="avg_cost * qty. Updated on every fill touching this position.",
        ),
        sa.Column(
            "opened_at",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="Timestamp of the first fill that opened this position.",
        ),
        sa.Column(
            "last_updated",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="Timestamp of the most recent fill that touched this position.",
        ),
        sa.UniqueConstraint("ticker", name="uq_positions_ticker"),
        sa.CheckConstraint("qty > 0", name="ck_positions_qty_positive"),
        sa.CheckConstraint("avg_cost > 0", name="ck_positions_avg_cost_positive"),
    )

    # ------------------------------------------------------------------
    # trades
    # FK → orders, job_runs. Immutable after creation.
    # ------------------------------------------------------------------
    op.create_table(
        "trades",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "order_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("orders.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "job_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job_runs.id", ondelete="RESTRICT"),
            nullable=False,
            comment="The fill-cycle run that produced this trade.",
        ),
        sa.Column("ticker", sa.String(20), nullable=False),
        sa.Column(
            "side",
            sa.String(10),
            nullable=False,
            comment="BUY | SELL",
        ),
        sa.Column("qty", sa.Numeric(18, 8), nullable=False),
        sa.Column(
            "snapshot_price",
            sa.Numeric(18, 6),
            nullable=False,
            comment="Raw price from price_snapshots before slippage is applied.",
        ),
        sa.Column(
            "fill_price",
            sa.Numeric(18, 6),
            nullable=False,
            comment="Post-slippage fill price per share.",
        ),
        sa.Column(
            "gross_value",
            sa.Numeric(18, 2),
            nullable=False,
            comment="qty * fill_price",
        ),
        sa.Column("commission", sa.Numeric(18, 2), nullable=False),
        sa.Column(
            "net_value",
            sa.Numeric(18, 2),
            nullable=False,
            comment="gross_value - commission (same sign convention for BUY and SELL).",
        ),
        sa.Column(
            "cost_basis_per_share",
            sa.Numeric(18, 6),
            nullable=False,
            comment=(
                "Position avg_cost at fill time. Captured permanently here because "
                "the Position row is deleted on full close."
            ),
        ),
        sa.Column(
            "realized_pnl",
            sa.Numeric(18, 2),
            nullable=True,
            comment="Non-null for SELL only. (fill_price - cost_basis_per_share) * qty",
        ),
        sa.Column(
            "trade_ts",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "market_date",
            sa.Date(),
            nullable=False,
            comment="US Eastern trading date of this fill.",
        ),
        sa.CheckConstraint("qty > 0", name="ck_trades_qty_positive"),
        sa.CheckConstraint("fill_price > 0", name="ck_trades_fill_price_positive"),
        sa.CheckConstraint(
            "snapshot_price > 0", name="ck_trades_snapshot_price_positive"
        ),
    )
    op.create_index(
        "ix_trades_ticker_trade_ts", "trades", ["ticker", "trade_ts"]
    )
    op.create_index(
        "ix_trades_market_date_side", "trades", ["market_date", "side"]
    )
    op.create_index("ix_trades_job_run_id", "trades", ["job_run_id"])

    # ------------------------------------------------------------------
    # cash_ledger
    # FK → portfolio, trades (nullable), orders (nullable), job_runs (nullable)
    # Append-only ledger. Source of truth for cash balance.
    # ------------------------------------------------------------------
    op.create_table(
        "cash_ledger",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "portfolio_id",
            sa.Integer(),
            sa.ForeignKey("portfolio.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "entry_type",
            sa.String(50),
            nullable=False,
            comment=(
                "INITIAL_CAPITAL | BUY_DEBIT | SELL_CREDIT | "
                "COMMISSION_DEBIT | DIVIDEND_CREDIT | ADJUSTMENT"
            ),
        ),
        sa.Column(
            "amount",
            sa.Numeric(18, 2),
            nullable=False,
            comment="Signed and non-zero. Positive = cash in, negative = cash out.",
        ),
        sa.Column(
            "trade_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("trades.id", ondelete="RESTRICT"),
            nullable=True,
        ),
        sa.Column(
            "order_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("orders.id", ondelete="RESTRICT"),
            nullable=True,
        ),
        sa.Column(
            "job_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job_runs.id", ondelete="RESTRICT"),
            nullable=True,
        ),
        sa.Column(
            "description",
            sa.Text(),
            nullable=True,
            comment="Human-readable audit note, e.g. 'Initial capital seed 2026-03-22'.",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("amount <> 0", name="ck_cash_ledger_amount_nonzero"),
    )
    op.create_index(
        "ix_cash_ledger_portfolio_created",
        "cash_ledger",
        ["portfolio_id", "created_at"],
    )
    op.create_index("ix_cash_ledger_job_run_id", "cash_ledger", ["job_run_id"])
    op.create_index("ix_cash_ledger_entry_type", "cash_ledger", ["entry_type"])

    # ------------------------------------------------------------------
    # price_snapshots
    # FK → job_runs (SET NULL — manually ingested prices have no job_run)
    # ------------------------------------------------------------------
    op.create_table(
        "price_snapshots",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("ticker", sa.String(20), nullable=False),
        sa.Column("price", sa.Numeric(18, 6), nullable=False),
        sa.Column(
            "session_type",
            sa.String(20),
            nullable=False,
            comment="PREMARKET | REGULAR | POSTMARKET | EXTENDED | MANUAL",
        ),
        sa.Column(
            "price_type",
            sa.String(20),
            nullable=False,
            comment="OPEN | CLOSE | LAST | BID | ASK | MID | VWAP",
        ),
        sa.Column("exchange", sa.String(20), nullable=True),
        sa.Column(
            "data_source",
            sa.String(100),
            nullable=True,
            comment="e.g. 'brave_scrape', 'yahoo_finance', 'manual'",
        ),
        sa.Column("snapshot_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("market_date", sa.Date(), nullable=False),
        sa.Column(
            "job_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job_runs.id", ondelete="SET NULL"),
            nullable=True,
            comment="Null for manually ingested prices outside a workflow run.",
        ),
        sa.CheckConstraint(
            "price > 0", name="ck_price_snapshots_price_positive"
        ),
    )
    op.create_index(
        "ix_price_snapshots_ticker_ts", "price_snapshots", ["ticker", "snapshot_ts"]
    )
    op.create_index(
        "ix_price_snapshots_market_date_session",
        "price_snapshots",
        ["market_date", "session_type"],
    )
    op.create_index(
        "ix_price_snapshots_job_run_id", "price_snapshots", ["job_run_id"]
    )

    # ------------------------------------------------------------------
    # benchmark_prices
    # FK → job_runs (SET NULL). Benchmark tickers are never traded.
    # ------------------------------------------------------------------
    op.create_table(
        "benchmark_prices",
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
            comment="e.g. 'SPY', 'QQQ'",
        ),
        sa.Column("price", sa.Numeric(18, 6), nullable=False),
        sa.Column(
            "session_type",
            sa.String(20),
            nullable=False,
            comment="PREMARKET | REGULAR | POSTMARKET | EXTENDED | MANUAL",
        ),
        sa.Column("snapshot_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("market_date", sa.Date(), nullable=False),
        sa.Column(
            "job_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job_runs.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.CheckConstraint(
            "price > 0", name="ck_benchmark_prices_price_positive"
        ),
    )
    op.create_index(
        "ix_benchmark_prices_ticker_ts",
        "benchmark_prices",
        ["ticker", "snapshot_ts"],
    )
    op.create_index(
        "ix_benchmark_prices_market_date", "benchmark_prices", ["market_date"]
    )
    op.create_index(
        "ix_benchmark_prices_job_run_id", "benchmark_prices", ["job_run_id"]
    )

    # ------------------------------------------------------------------
    # portfolio_snapshots
    # FK → job_runs. One row per post-market run per trading day.
    # ------------------------------------------------------------------
    op.create_table(
        "portfolio_snapshots",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "job_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job_runs.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "snapshot_ts",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("market_date", sa.Date(), nullable=False),
        sa.Column(
            "cash",
            sa.Numeric(18, 2),
            nullable=False,
            comment="Computed from cash_ledger SUM at snapshot time, not from cache.",
        ),
        sa.Column("positions_value", sa.Numeric(18, 2), nullable=False),
        sa.Column("total_value", sa.Numeric(18, 2), nullable=False),
        sa.Column("unrealized_pnl", sa.Numeric(18, 2), nullable=False),
        sa.Column(
            "realized_pnl_cumulative",
            sa.Numeric(18, 2),
            nullable=False,
            comment="SUM of all realized_pnl from trades at snapshot time.",
        ),
        sa.Column("open_position_count", sa.Integer(), nullable=False),
        sa.Column(
            "daily_new_exposure",
            sa.Numeric(18, 2),
            nullable=True,
            comment="Total BUY notional (PENDING + FILLED) deployed on this market_date.",
        ),
        sa.Column("benchmark_ticker", sa.String(20), nullable=True),
        sa.Column("benchmark_price", sa.Numeric(18, 6), nullable=True),
        sa.Column(
            "benchmark_inception_price",
            sa.Numeric(18, 6),
            nullable=True,
            comment="Benchmark price on portfolio inception_date. Basis for alpha calculation.",
        ),
        sa.Column(
            "benchmark_value",
            sa.Numeric(18, 2),
            nullable=True,
            comment="initial_capital / benchmark_inception_price * benchmark_price",
        ),
        sa.Column(
            "portfolio_vs_benchmark",
            sa.Numeric(18, 2),
            nullable=True,
            comment="total_value - benchmark_value. Positive = outperforming benchmark.",
        ),
        sa.Column(
            "positions_detail",
            postgresql.JSONB(),
            nullable=True,
            comment=(
                "Array of {ticker, qty, avg_cost, current_price, market_value, unrealized_pnl} "
                "at snapshot time. Enables historical charts without joins."
            ),
        ),
        sa.UniqueConstraint(
            "job_run_id", name="uq_portfolio_snapshots_job_run_id"
        ),
        sa.UniqueConstraint(
            "market_date", name="uq_portfolio_snapshots_market_date"
        ),
    )
    op.create_index(
        "ix_portfolio_snapshots_market_date_desc",
        "portfolio_snapshots",
        ["market_date"],
    )


def downgrade() -> None:
    # Drop in reverse FK dependency order.
    # PostgreSQL automatically drops associated indexes when a table is dropped.
    op.drop_table("portfolio_snapshots")
    op.drop_table("benchmark_prices")
    op.drop_table("price_snapshots")
    op.drop_table("cash_ledger")
    op.drop_table("trades")
    op.drop_table("positions")
    op.drop_table("orders")
    op.drop_table("trade_decisions")
    op.drop_table("signals")
    op.drop_table("job_runs")
    op.drop_table("portfolio")
