"""
db/models.py — SQLAlchemy 2.x ORM models for the paper_trader system.

Numeric precision conventions:
    Numeric(18, 2)  — dollar amounts (cash, P&L, notional)
    Numeric(18, 6)  — per-share prices (allows sub-cent precision, handles penny stocks)
    Numeric(18, 8)  — share quantities (whole shares in v1, future-proofs fractional)
    Numeric(5, 4)   — confidence scores (0.0000–1.0000)

Cash accounting:
    portfolio.cached_cash and portfolio.cached_total_value are read-optimised caches.
    Source of truth for cash: SUM(cash_ledger.amount).
    The reconciler refreshes the cache after every fill cycle.

FK on-delete policy:
    RESTRICT is the default — audit trail rows are never silently removed.
    SET NULL is used only on genuinely optional FKs (job_run linkage on price/benchmark
    tables for manually ingested rows; fill_job_run_id on orders).

Circular FK eliminated:
    trade_decisions does NOT have an order_id FK back to orders.
    Navigate decision → order with: SELECT * FROM orders WHERE trade_decision_id = :id.
    This removes the circular dependency and the ALTER TABLE complexity entirely.

market_date convention:
    Always the US/Eastern calendar date of the run (not UTC date).
    Computed as: (timestamp AT TIME ZONE 'America/New_York')::date
    Stored as Date type. Used for: daily exposure cap, cooldown checks, reporting.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Shared declarative base for all paper_trader ORM models."""
    pass


# ---------------------------------------------------------------------------
# portfolio
# ---------------------------------------------------------------------------

class Portfolio(Base):
    """
    Single-row table. Holds immutable inception config and a reconciler-maintained
    cache of derived state.

    Do not read cached_cash or cached_total_value as accounting truth.
    Always use engine.portfolio.compute_cash(session) for the authoritative balance.

    portfolio.config JSONB expected keys:
        slippage_bps                int     — e.g. 10 (applied to fill price)
        commission_flat             str     — Decimal-safe, e.g. "1.00"
        max_positions               int     — e.g. 5
        max_concentration_pct       str     — e.g. "0.20"
        min_cash_pct                str     — e.g. "0.10"
        confidence_threshold        str     — e.g. "0.55"
        order_ttl_hours             int     — e.g. 24
        min_order_notional          str     — e.g. "50.00"
        max_daily_new_exposure_pct  str     — e.g. "0.40"
        ticker_cooldown_hours       int     — e.g. 48
        benchmark_ticker            str     — e.g. "SPY"
        allow_averaging_down        bool    — e.g. false
    """

    __tablename__ = "portfolio"
    __table_args__ = (
        CheckConstraint(
            "initial_capital > 0",
            name="ck_portfolio_initial_capital_positive",
        ),
        CheckConstraint(
            "cached_cash >= 0",
            name="ck_portfolio_cached_cash_nonneg",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # --- Immutable inception fields ---
    initial_capital: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="Set at seed time. Never modified after inception.",
    )
    inception_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="US Eastern trading date on which the portfolio was seeded.",
    )

    # --- Reconciler-maintained cache (not source of truth) ---
    cached_cash: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="Cache of SUM(cash_ledger.amount). Refreshed by reconciler after each fill cycle.",
    )
    cached_total_value: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="Cache of cash + mark-to-market positions value. Refreshed by reconciler.",
    )
    cached_as_of_ts: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of the last reconciler refresh. None until first fill cycle.",
    )

    # --- Portfolio-level kill switches ---
    strategy_enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        comment="Master switch. False = signals are received but not processed.",
    )
    trading_enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        comment="False = no new orders are generated regardless of signals.",
    )
    allow_new_positions: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        comment="False = only closes allowed; no BUY orders for tickers not already held.",
    )
    pause_reason: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Human-readable explanation for any kill switch being disabled.",
    )

    # --- Runtime-editable risk/execution configuration ---
    config: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
        comment="Risk and execution parameters. See class docstring for expected keys.",
    )

    # Relationships
    cash_ledger_entries: Mapped[list[CashLedger]] = relationship(
        "CashLedger",
        back_populates="portfolio",
        lazy="select",
    )


# ---------------------------------------------------------------------------
# cash_ledger
# ---------------------------------------------------------------------------

class CashLedger(Base):
    """
    Immutable append-only ledger. Source of truth for cash balance.

    current_cash = SUM(amount) across all rows for the portfolio.

    Positive amount = cash inflow  (INITIAL_CAPITAL, SELL_CREDIT, DIVIDEND_CREDIT)
    Negative amount = cash outflow (BUY_DEBIT, COMMISSION_DEBIT)

    Never update or delete rows. Every cash event produces a new row.
    The non-zero constraint prevents silent no-op entries that would corrupt audits.
    """

    __tablename__ = "cash_ledger"
    __table_args__ = (
        CheckConstraint(
            "amount <> 0",
            name="ck_cash_ledger_amount_nonzero",
        ),
        Index("ix_cash_ledger_portfolio_created", "portfolio_id", "created_at"),
        Index("ix_cash_ledger_job_run_id", "job_run_id"),
        Index("ix_cash_ledger_entry_type", "entry_type"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    portfolio_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("portfolio.id", ondelete="RESTRICT"),
        nullable=False,
    )
    entry_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment=(
            "INITIAL_CAPITAL | BUY_DEBIT | SELL_CREDIT | "
            "COMMISSION_DEBIT | DIVIDEND_CREDIT | ADJUSTMENT"
        ),
    )
    amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="Signed and non-zero. Positive = cash in, negative = cash out.",
    )

    # Optional back-links for audit trail — nullable because not every entry
    # is tied to a specific trade/order (e.g. INITIAL_CAPITAL, ADJUSTMENT).
    trade_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("trades.id", ondelete="RESTRICT"),
        nullable=True,
    )
    order_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("orders.id", ondelete="RESTRICT"),
        nullable=True,
    )
    job_run_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job_runs.id", ondelete="RESTRICT"),
        nullable=True,
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Human-readable audit note, e.g. 'Initial capital seed 2026-03-22'.",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # Relationships
    portfolio: Mapped[Portfolio] = relationship(
        "Portfolio",
        back_populates="cash_ledger_entries",
    )


# ---------------------------------------------------------------------------
# job_runs
# ---------------------------------------------------------------------------

class JobRun(Base):
    """
    One row per workflow execution attempt.

    Acts as the idempotency key holder and audit anchor for all child records
    produced by a workflow run (signals, orders, trades, snapshots, ledger entries).

    Idempotency contract:
        status=COMPLETED → return cached result_summary immediately.
        status=RUNNING   → return HTTP 423.
        status=FAILED    → retry permitted (configurable).
    """

    __tablename__ = "job_runs"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_job_runs_idempotency_key"),
        Index("ix_job_runs_market_date_workflow", "market_date", "workflow_type"),
        # Partial index: stale-RUNNING detection is a hot path at workflow start.
        Index(
            "ix_job_runs_status_running",
            "status",
            postgresql_where=text("status = 'RUNNING'"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    idempotency_key: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Caller-provided key. Typically: 'pre_market_20260322'.",
    )
    workflow_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="PRE_MARKET | MIDDAY | POST_MARKET",
    )
    market_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="US Eastern trading date for this run.",
    )
    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="RUNNING",
        comment="RUNNING | COMPLETED | FAILED | SKIPPED",
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    result_summary: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        nullable=True,
        comment=(
            "Populated on COMPLETED. Keys: signals_ingested, decisions_made, "
            "orders_created, fills_executed, positions_opened, positions_closed."
        ),
    )
    error_detail: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="First error message if status=FAILED.",
    )


# ---------------------------------------------------------------------------
# signals
# ---------------------------------------------------------------------------

class Signal(Base):
    """
    Raw output from the prediction engine, one row per signal ingested.

    Lifecycle: RECEIVED → PROCESSING → DECISION_MADE
                                     → EXPIRED (TTL passed before processing)
                                     → ERROR   (processing exception)

    The unique constraint on (source_run, ticker, direction) prevents the same
    prediction run from submitting duplicate signals for the same ticker+direction.
    """

    __tablename__ = "signals"
    __table_args__ = (
        CheckConstraint(
            "confidence >= 0 AND confidence <= 1",
            name="ck_signals_confidence_range",
        ),
        UniqueConstraint(
            "source_run",
            "ticker",
            "direction",
            name="uq_signals_source_run_ticker_direction",
        ),
        Index("ix_signals_market_date_status", "market_date", "status"),
        Index("ix_signals_job_run_id", "job_run_id"),
        Index("ix_signals_ticker_market_date", "ticker", "market_date"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    job_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job_runs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    ticker: Mapped[str] = mapped_column(String(20), nullable=False)
    direction: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        comment="BUY | SELL | HOLD",
    )
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    signal_ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="When the prediction engine generated this signal.",
    )
    market_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="US Eastern trading date.",
    )
    source_run: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Label from the prediction engine run, e.g. 'pre_market_20260322'.",
    )
    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="RECEIVED",
        comment="RECEIVED | PROCESSING | DECISION_MADE | EXPIRED | ERROR",
    )
    error_detail: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_payload: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        nullable=True,
        comment="Full prediction engine output for this signal.",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


# ---------------------------------------------------------------------------
# trade_decisions
# ---------------------------------------------------------------------------

class TradeDecision(Base):
    """
    One row per signal processed by the decision engine.

    Every signal that exits PROCESSING state produces exactly one TradeDecision.
    This table is the primary model evaluation dataset: it records why a signal
    became a BUY/SELL/HOLD/REJECTED with full risk state and sizing audit trail.

    Navigation from decision → order:
        SELECT * FROM orders WHERE trade_decision_id = :decision_id LIMIT 1
    There is no reverse FK from this table to orders. REJECTED and HOLD decisions
    produce no order (the query returns no rows).

    market_date is denormalised here (also on job_run) to allow efficient
    daily exposure queries without joining to job_runs.
    """

    __tablename__ = "trade_decisions"
    __table_args__ = (
        UniqueConstraint("signal_id", name="uq_trade_decisions_signal_id"),
        Index("ix_trade_decisions_ticker_market_date", "ticker", "market_date"),
        Index("ix_trade_decisions_decision_reason", "decision", "reason_code"),
        Index("ix_trade_decisions_job_run_id", "job_run_id"),
        Index("ix_trade_decisions_market_date", "market_date"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    signal_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("signals.id", ondelete="RESTRICT"),
        nullable=False,
        comment="One-to-one with signals. A signal gets exactly one decision.",
    )
    job_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job_runs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    ticker: Mapped[str] = mapped_column(String(20), nullable=False)
    signal_direction: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        comment="What the signal said: BUY | SELL | HOLD",
    )
    decision: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        comment="BUY | SELL | HOLD | REJECTED",
    )
    reason_code: Mapped[Optional[str]] = mapped_column(
        String(60),
        nullable=True,
        comment="Populated for all outcomes. See RejectionReason constants for valid values.",
    )

    # Sizing fields — null for HOLD and early REJECTED (before sizing is reached).
    requested_notional: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 2),
        nullable=True,
        comment="Target dollar amount before risk sizing clamps.",
    )
    approved_notional: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 2),
        nullable=True,
        comment="Dollar amount after all sizing clamps. Null if rejected before sizing.",
    )
    requested_qty: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 8),
        nullable=True,
    )
    approved_qty: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 8),
        nullable=True,
        comment="floor(approved_notional / snapshot_price). Null if rejected.",
    )

    # Audit fields
    risk_snapshot: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        nullable=True,
        comment=(
            "Portfolio state at decision time: "
            "{cash, total_value, open_position_count, "
            "daily_exposure_used, daily_exposure_limit}"
        ),
    )
    sizing_adjustments: Mapped[Optional[list]] = mapped_column(
        JSONB,
        nullable=True,
        comment=(
            "Log of sizing clamps applied, e.g. "
            "[{rule: 'concentration_cap', before: '2500.00', after: '2000.00'}]"
        ),
    )

    decided_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    market_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="Denormalised from job_run for efficient daily exposure queries.",
    )


# ---------------------------------------------------------------------------
# orders
# ---------------------------------------------------------------------------

class Order(Base):
    """
    Every order the system has ever generated. Orders are only created for
    approved (BUY or SELL) trade decisions. REJECTED and HOLD decisions produce
    no order row.

    trade_decision_id is NOT NULL in Phase 1: all orders originate from the
    decision engine. Manual admin orders are a Phase 2+ concern.

    State machine — valid transitions only:
        PENDING → FILLED    (fill cycle: price found, cash check passes)
        PENDING → CANCELLED (manual cancel via admin API)
        PENDING → EXPIRED   (fill cycle: order TTL exceeded)
        PENDING → FAILED    (fill cycle: unexpected runtime error)

    All states except PENDING are terminal. No transition back to PENDING.
    REJECTED is NOT an order status; it is a trade_decision-level outcome.
    """

    __tablename__ = "orders"
    __table_args__ = (
        CheckConstraint(
            "requested_qty > 0",
            name="ck_orders_requested_qty_positive",
        ),
        CheckConstraint(
            "filled_qty >= 0",
            name="ck_orders_filled_qty_nonneg",
        ),
        Index("ix_orders_status_requested_at", "status", "requested_at"),
        # Partial index: fill cycle only queries PENDING orders.
        Index(
            "ix_orders_ticker_status_pending",
            "ticker",
            "status",
            postgresql_where=text("status = 'PENDING'"),
        ),
        Index("ix_orders_job_run_id", "job_run_id"),
        Index("ix_orders_market_date", "market_date"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    # NOT NULL in Phase 1: every order must originate from a trade_decision.
    trade_decision_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("trade_decisions.id", ondelete="RESTRICT"),
        nullable=False,
        comment="The trade_decision that produced this order.",
    )
    job_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job_runs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    # SET NULL: fill run reference is informational; dev/test resets should not
    # cascade-delete orders when a job_run row is removed.
    fill_job_run_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job_runs.id", ondelete="SET NULL"),
        nullable=True,
        comment="Which fill-cycle run filled this order. Null until filled.",
    )

    ticker: Mapped[str] = mapped_column(String(20), nullable=False)
    side: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        comment="BUY | SELL",
    )
    order_type: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="MARKET",
        comment="MARKET only in v1.",
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="PENDING",
        comment="PENDING | FILLED | CANCELLED | EXPIRED | FAILED",
    )
    market_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="US Eastern trading date of order creation.",
    )

    requested_qty: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    filled_qty: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 8),
        nullable=True,
        comment="Null until filled. Equals requested_qty in v1 (no partial fills).",
    )

    requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    filled_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Fill execution details — null until filled.
    fill_price: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 6),
        nullable=True,
        comment="Post-slippage fill price per share.",
    )
    commission: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 2),
        nullable=True,
    )
    slippage_cost: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 2),
        nullable=True,
        comment="(fill_price - snapshot_price) * qty. Positive for BUY, negative for SELL.",
    )
    notes: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Audit note, e.g. cancellation reason or TTL expiry detail.",
    )


# ---------------------------------------------------------------------------
# positions
# ---------------------------------------------------------------------------

class Position(Base):
    """
    Current open positions. One row per ticker.

    The row is DELETED (not zeroed) when a position is fully closed. A zero-qty
    position must never exist; the positions table always reflects live holdings.

    avg_cost uses weighted-average cost (WAC) basis.

    WAC formula on BUY fill:
        new_avg_cost = (existing_qty * existing_avg_cost + fill_qty * fill_price)
                       / (existing_qty + fill_qty)

    cost_basis = avg_cost * qty. Stored for read efficiency; updated on every fill.

    No additional indexes beyond PK and the ticker unique constraint: the table
    holds at most max_positions rows (5 in v1). Full scans are faster than index
    lookups at this cardinality.
    """

    __tablename__ = "positions"
    __table_args__ = (
        # Single definition of ticker uniqueness — do not also use unique=True
        # on the column itself, which would create a duplicate constraint.
        UniqueConstraint("ticker", name="uq_positions_ticker"),
        CheckConstraint("qty > 0", name="ck_positions_qty_positive"),
        CheckConstraint("avg_cost > 0", name="ck_positions_avg_cost_positive"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    # unique=True omitted — uniqueness is enforced by uq_positions_ticker above.
    ticker: Mapped[str] = mapped_column(String(20), nullable=False)
    qty: Mapped[Decimal] = mapped_column(
        Numeric(18, 8),
        nullable=False,
        comment="Shares held. Must be > 0; delete row on full close.",
    )
    avg_cost: Mapped[Decimal] = mapped_column(
        Numeric(18, 6),
        nullable=False,
        comment="Weighted-average cost per share (WAC basis).",
    )
    cost_basis: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="avg_cost * qty. Updated on every fill touching this position.",
    )
    opened_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="Timestamp of the first fill that opened this position.",
    )
    last_updated: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="Timestamp of the most recent fill that touched this position.",
    )


# ---------------------------------------------------------------------------
# trades
# ---------------------------------------------------------------------------

class Trade(Base):
    """
    Immutable fill-level audit log. One row per executed fill.

    Every FILLED order produces exactly one Trade row. Trade rows are never
    updated after creation.

    realized_pnl is populated only for SELL trades:
        realized_pnl = (fill_price - cost_basis_per_share) * qty

    cost_basis_per_share captures the position's avg_cost at the moment of the
    SELL fill permanently — the Position row is deleted on full close, so this
    field is the only durable record of the cost basis used for that realisation.
    """

    __tablename__ = "trades"
    __table_args__ = (
        CheckConstraint("qty > 0", name="ck_trades_qty_positive"),
        CheckConstraint("fill_price > 0", name="ck_trades_fill_price_positive"),
        CheckConstraint(
            "snapshot_price > 0",
            name="ck_trades_snapshot_price_positive",
        ),
        Index("ix_trades_ticker_trade_ts", "ticker", "trade_ts"),
        Index("ix_trades_market_date_side", "market_date", "side"),
        Index("ix_trades_job_run_id", "job_run_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("orders.id", ondelete="RESTRICT"),
        nullable=False,
    )
    job_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job_runs.id", ondelete="RESTRICT"),
        nullable=False,
        comment="The fill-cycle run that produced this trade.",
    )

    ticker: Mapped[str] = mapped_column(String(20), nullable=False)
    side: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        comment="BUY | SELL",
    )
    qty: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)

    snapshot_price: Mapped[Decimal] = mapped_column(
        Numeric(18, 6),
        nullable=False,
        comment="Raw price from price_snapshots before slippage is applied.",
    )
    fill_price: Mapped[Decimal] = mapped_column(
        Numeric(18, 6),
        nullable=False,
        comment="Post-slippage fill price per share.",
    )
    gross_value: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="qty * fill_price",
    )
    commission: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    net_value: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="gross_value - commission (same sign convention for BUY and SELL).",
    )
    cost_basis_per_share: Mapped[Decimal] = mapped_column(
        Numeric(18, 6),
        nullable=False,
        comment=(
            "Position avg_cost at fill time. Captured permanently here because "
            "the Position row is deleted on full close."
        ),
    )
    realized_pnl: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 2),
        nullable=True,
        comment="Non-null for SELL only. (fill_price - cost_basis_per_share) * qty",
    )
    trade_ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    market_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="US Eastern trading date of this fill.",
    )


# ---------------------------------------------------------------------------
# price_snapshots
# ---------------------------------------------------------------------------

class PriceSnapshot(Base):
    """
    Price observations ingested at each workflow run.

    The fill cycle selects the most recent snapshot for each ticker:
        SELECT price FROM price_snapshots
        WHERE ticker = :ticker
        ORDER BY snapshot_ts DESC
        LIMIT 1

    session_type and price_type provide honest fill-context metadata for
    backtesting: a fill using a PREMARKET LAST price is materially different
    from a fill using a REGULAR CLOSE price.
    """

    __tablename__ = "price_snapshots"
    __table_args__ = (
        CheckConstraint("price > 0", name="ck_price_snapshots_price_positive"),
        Index("ix_price_snapshots_ticker_ts", "ticker", "snapshot_ts"),
        Index(
            "ix_price_snapshots_market_date_session",
            "market_date",
            "session_type",
        ),
        Index("ix_price_snapshots_job_run_id", "job_run_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    ticker: Mapped[str] = mapped_column(String(20), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    session_type: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        comment="PREMARKET | REGULAR | POSTMARKET | EXTENDED | MANUAL",
    )
    price_type: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        comment="OPEN | CLOSE | LAST | BID | ASK | MID | VWAP",
    )
    exchange: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    data_source: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        comment="e.g. 'brave_scrape', 'yahoo_finance', 'manual'",
    )
    snapshot_ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    market_date: Mapped[date] = mapped_column(Date, nullable=False)
    # SET NULL: manually ingested prices have no job_run.
    job_run_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job_runs.id", ondelete="SET NULL"),
        nullable=True,
        comment="Null for manually ingested prices outside a workflow run.",
    )


# ---------------------------------------------------------------------------
# benchmark_prices
# ---------------------------------------------------------------------------

class BenchmarkPrice(Base):
    """
    Price observations for benchmark tickers (SPY, QQQ, etc.).

    Kept separate from price_snapshots to avoid polluting the trading price
    dataset with benchmark tickers. Benchmark tickers are never traded.

    Benchmark value at any point in time:
        benchmark_value = initial_capital / benchmark_inception_price * current_price
    where benchmark_inception_price is stored in portfolio.config.
    """

    __tablename__ = "benchmark_prices"
    __table_args__ = (
        CheckConstraint("price > 0", name="ck_benchmark_prices_price_positive"),
        Index("ix_benchmark_prices_ticker_ts", "ticker", "snapshot_ts"),
        Index("ix_benchmark_prices_market_date", "market_date"),
        Index("ix_benchmark_prices_job_run_id", "job_run_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    ticker: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        comment="e.g. 'SPY', 'QQQ'",
    )
    price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    session_type: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        comment="PREMARKET | REGULAR | POSTMARKET | EXTENDED | MANUAL",
    )
    snapshot_ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    market_date: Mapped[date] = mapped_column(Date, nullable=False)
    job_run_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job_runs.id", ondelete="SET NULL"),
        nullable=True,
    )


# ---------------------------------------------------------------------------
# portfolio_snapshots
# ---------------------------------------------------------------------------

class PortfolioSnapshot(Base):
    """
    One authoritative snapshot per post-market workflow run.

    UNIQUE on both job_run_id and market_date enforces exactly one snapshot
    per trading day and per workflow run.

    positions_detail stores a point-in-time copy of all open positions with
    mark-to-market prices — used for historical P&L charts without requiring
    joins back to price_snapshots.

    benchmark_* fields allow computing alpha directly from this table without
    additional joins.
    """

    __tablename__ = "portfolio_snapshots"
    __table_args__ = (
        UniqueConstraint("job_run_id", name="uq_portfolio_snapshots_job_run_id"),
        UniqueConstraint("market_date", name="uq_portfolio_snapshots_market_date"),
        Index("ix_portfolio_snapshots_market_date_desc", "market_date"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    job_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job_runs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    snapshot_ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    market_date: Mapped[date] = mapped_column(Date, nullable=False)

    # --- Portfolio state at snapshot time ---
    cash: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="Computed from cash_ledger SUM at snapshot time, not from cache.",
    )
    positions_value: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    total_value: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    unrealized_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    realized_pnl_cumulative: Mapped[Decimal] = mapped_column(
        Numeric(18, 2),
        nullable=False,
        comment="SUM of all realized_pnl from trades at snapshot time.",
    )
    open_position_count: Mapped[int] = mapped_column(Integer, nullable=False)
    daily_new_exposure: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 2),
        nullable=True,
        comment="Total BUY notional (PENDING + FILLED) deployed on this market_date.",
    )

    # --- Benchmark comparison ---
    benchmark_ticker: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    benchmark_price: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 6),
        nullable=True,
    )
    benchmark_inception_price: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 6),
        nullable=True,
        comment="Benchmark price on portfolio inception_date. Basis for alpha calculation.",
    )
    benchmark_value: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 2),
        nullable=True,
        comment="initial_capital / benchmark_inception_price * benchmark_price",
    )
    portfolio_vs_benchmark: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 2),
        nullable=True,
        comment="total_value - benchmark_value. Positive = outperforming benchmark.",
    )

    # --- Point-in-time position detail ---
    positions_detail: Mapped[Optional[list]] = mapped_column(
        JSONB,
        nullable=True,
        comment=(
            "Array of {ticker, qty, avg_cost, current_price, market_value, unrealized_pnl} "
            "at snapshot time. Enables historical charts without joins."
        ),
    )
