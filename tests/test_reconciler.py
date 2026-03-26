"""
tests/test_reconciler.py — Integration tests for engine/reconciler.py.

Covers: empty cycle, TTL expiry, no-price skip, BUY fill (success + insufficient
cash), SELL fill (success + no position), and mixed-outcome counts.

run_fill_cycle() calls session.commit() / session.rollback() internally.
The conftest SAVEPOINT isolation pattern handles this: each internal commit
releases the current savepoint, _restart_savepoint opens a new one, and
outer_tx.rollback() in fixture teardown undoes everything.

Requires PAPER_TRADER_TEST_DATABASE_URL (auto-skipped when absent).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from paper_trader.constants import (
    CashEntryType,
    DecisionType,
    JobRunStatus,
    OrderSide,
    OrderStatus,
    SignalStatus,
    WorkflowType,
)
from paper_trader.db.models import (
    JobRun,
    Order,
    Portfolio,
    Position,
    PriceSnapshot,
    Signal,
    Trade,
    TradeDecision,
)
from paper_trader.engine.portfolio import append_cash_entry, compute_cash, open_position
from paper_trader.engine.reconciler import run_fill_cycle

_NOW  = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
_DATE = _NOW.date()
_OLD  = _NOW - timedelta(hours=25)   # 1 hour beyond the 24-hour TTL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def fill_portfolio(db_session: Session, seeded_portfolio: Portfolio) -> Portfolio:
    """Portfolio with explicit fill-cycle config (slippage=10bps, commission=$1)."""
    seeded_portfolio.config = {
        "slippage_bps":    10,
        "commission_flat": "1.00",
        "order_ttl_hours": 24,
    }
    db_session.flush()
    return seeded_portfolio


@pytest.fixture
def fill_job_run(db_session: Session) -> JobRun:
    """
    A JobRun row whose id is used both as the 'pre-market run that created the
    orders' and as the job_run_id passed to run_fill_cycle().

    Trade.job_run_id is a required FK, so this row must exist before fills run.
    """
    job_run = JobRun(
        idempotency_key=f"fill-test-{uuid.uuid4()}",
        workflow_type=WorkflowType.POST_MARKET,
        market_date=_DATE,
        status=JobRunStatus.COMPLETED,
        started_at=_NOW,
        completed_at=_NOW,
    )
    db_session.add(job_run)
    db_session.flush()
    return job_run


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pending_order(
    session: Session,
    job_run: JobRun,
    *,
    ticker: str,
    side: str,
    qty: Decimal,
    market_date=_DATE,
    requested_at: datetime = _NOW,
) -> Order:
    """Create Signal → TradeDecision → Order chain and return the Order."""
    signal = Signal(
        job_run_id=job_run.id,
        ticker=ticker,
        direction=side,
        confidence=Decimal("0.80"),
        signal_ts=requested_at,
        market_date=market_date,
        source_run="test",
        status=SignalStatus.DECISION_MADE,
    )
    session.add(signal)
    session.flush()

    td = TradeDecision(
        signal_id=signal.id,
        job_run_id=job_run.id,
        ticker=ticker,
        signal_direction=side,
        decision=side,
        reason_code=None,
        approved_qty=qty,
        approved_notional=(qty * Decimal("100")).quantize(Decimal("0.01")),
        market_date=market_date,
    )
    session.add(td)
    session.flush()

    order = Order(
        trade_decision_id=td.id,
        job_run_id=job_run.id,
        ticker=ticker,
        side=side,
        order_type="MARKET",
        status=OrderStatus.PENDING,
        market_date=market_date,
        requested_qty=qty,
        requested_at=requested_at,
    )
    session.add(order)
    session.flush()
    return order


def _add_snapshot(
    session: Session,
    ticker: str,
    price: Decimal,
    ts: datetime = _NOW,
) -> None:
    session.add(PriceSnapshot(
        ticker=ticker,
        price=price,
        session_type="REGULAR",
        price_type="CLOSE",
        snapshot_ts=ts,
        market_date=ts.date(),
        job_run_id=None,
    ))
    session.flush()


def _cycle(session, portfolio, job_run):
    """Thin wrapper to avoid repeating keyword args in every test."""
    return run_fill_cycle(
        session,
        portfolio=portfolio,
        job_run_id=job_run.id,
        now=_NOW,
        market_date=_DATE,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRunFillCycle:
    def test_empty_orders_all_zero_counts(
        self, db_session: Session, fill_portfolio: Portfolio, fill_job_run: JobRun
    ) -> None:
        counts = _cycle(db_session, fill_portfolio, fill_job_run)
        assert counts == {"filled": 0, "expired": 0, "failed": 0, "skipped": 0}

    def test_pending_order_no_price_skipped(
        self, db_session: Session, fill_portfolio: Portfolio, fill_job_run: JobRun
    ) -> None:
        """No PriceSnapshot for AAPL → order left PENDING, counted as skipped."""
        _make_pending_order(db_session, fill_job_run,
                            ticker="AAPL", side=OrderSide.BUY, qty=Decimal("5"))
        counts = _cycle(db_session, fill_portfolio, fill_job_run)
        assert counts["skipped"] == 1
        assert counts["filled"] == 0

    def test_pending_order_ttl_expired(
        self, db_session: Session, fill_portfolio: Portfolio, fill_job_run: JobRun
    ) -> None:
        """requested_at 25h ago with 24h TTL → order EXPIRED before price check."""
        order = _make_pending_order(
            db_session, fill_job_run,
            ticker="AAPL", side=OrderSide.BUY, qty=Decimal("5"),
            requested_at=_OLD,
        )
        _add_snapshot(db_session, "AAPL", Decimal("100"))
        counts = _cycle(db_session, fill_portfolio, fill_job_run)
        assert counts["expired"] == 1
        assert counts["filled"] == 0
        db_session.refresh(order)
        assert order.status == OrderStatus.EXPIRED

    def test_buy_fill_creates_position_and_debits_cash(
        self, db_session: Session, fill_portfolio: Portfolio, fill_job_run: JobRun
    ) -> None:
        """
        8 shares, snapshot=$100.
        fill_price = 100 * 1.001    = $100.100000
        fill_cost  = 8 * 100.100000 = $800.80
        commission = $1.00
        expected cash = 10000 - 800.80 - 1.00 = $9,198.20
        """
        order = _make_pending_order(db_session, fill_job_run,
                                    ticker="AAPL", side=OrderSide.BUY, qty=Decimal("8"))
        _add_snapshot(db_session, "AAPL", Decimal("100"))

        counts = _cycle(db_session, fill_portfolio, fill_job_run)
        assert counts == {"filled": 1, "expired": 0, "failed": 0, "skipped": 0}

        db_session.refresh(order)
        assert order.status     == OrderStatus.FILLED
        assert order.fill_price == Decimal("100.100000")
        assert order.filled_qty == Decimal("8.00000000")
        assert order.commission == Decimal("1.00")

        pos = db_session.execute(
            select(Position).where(Position.ticker == "AAPL")
        ).scalar_one()
        assert pos.qty        == Decimal("8.00000000")
        assert pos.avg_cost   == Decimal("100.100000")
        assert pos.cost_basis == Decimal("800.80")

        assert compute_cash(db_session) == Decimal("9198.20")

    def test_buy_fill_insufficient_cash_marks_failed(
        self, db_session: Session, fill_portfolio: Portfolio, fill_job_run: JobRun
    ) -> None:
        """Leave only $5 cash — far below the $801.80 required for 8 shares."""
        _make_pending_order(db_session, fill_job_run,
                            ticker="AAPL", side=OrderSide.BUY, qty=Decimal("8"))
        _add_snapshot(db_session, "AAPL", Decimal("100"))
        append_cash_entry(
            db_session,
            portfolio_id=fill_portfolio.id,
            entry_type=CashEntryType.BUY_DEBIT,
            amount=Decimal("-9995.00"),
        )
        db_session.flush()

        counts = _cycle(db_session, fill_portfolio, fill_job_run)
        assert counts["failed"] == 1
        assert counts["filled"] == 0
        assert db_session.execute(
            select(Position).where(Position.ticker == "AAPL")
        ).scalar_one_or_none() is None

    def test_sell_fill_closes_position_and_credits_cash(
        self, db_session: Session, fill_portfolio: Portfolio, fill_job_run: JobRun
    ) -> None:
        """
        10 shares held @ $100 avg_cost. Snapshot = $120.
        fill_price   = 120 * 0.999   = $119.880000
        gross_value  = 10 * 119.880000 = $1,198.80
        realized_pnl = (119.880000 - 100.000000) * 10 = $198.80
        expected cash = 10000 + 1198.80 - 1.00 = $11,197.80
        """
        open_position(db_session, ticker="AAPL", qty=Decimal("10"),
                      fill_price=Decimal("100.00"), now=_NOW)
        order = _make_pending_order(db_session, fill_job_run,
                                    ticker="AAPL", side=OrderSide.SELL, qty=Decimal("10"))
        _add_snapshot(db_session, "AAPL", Decimal("120"))

        counts = _cycle(db_session, fill_portfolio, fill_job_run)
        assert counts == {"filled": 1, "expired": 0, "failed": 0, "skipped": 0}

        db_session.refresh(order)
        assert order.status     == OrderStatus.FILLED
        assert order.fill_price == Decimal("119.880000")

        assert db_session.execute(
            select(Position).where(Position.ticker == "AAPL")
        ).scalar_one_or_none() is None

        trade = db_session.execute(
            select(Trade).where(Trade.ticker == "AAPL")
        ).scalar_one()
        assert trade.realized_pnl == Decimal("198.80")
        assert trade.gross_value  == Decimal("1198.80")

        assert compute_cash(db_session) == Decimal("11197.80")

    def test_sell_fill_no_position_marks_failed(
        self, db_session: Session, fill_portfolio: Portfolio, fill_job_run: JobRun
    ) -> None:
        """SELL with no open position → ValueError → order FAILED."""
        _make_pending_order(db_session, fill_job_run,
                            ticker="AAPL", side=OrderSide.SELL, qty=Decimal("5"))
        _add_snapshot(db_session, "AAPL", Decimal("100"))

        counts = _cycle(db_session, fill_portfolio, fill_job_run)
        assert counts["failed"] == 1
        assert counts["filled"] == 0

    def test_mixed_outcomes_correct_counts(
        self, db_session: Session, fill_portfolio: Portfolio, fill_job_run: JobRun
    ) -> None:
        """expired=1, skipped=1, filled=1, failed=0."""
        # TTL-expired BUY (AAPL — has a price but expires first)
        _make_pending_order(
            db_session, fill_job_run,
            ticker="AAPL", side=OrderSide.BUY, qty=Decimal("2"),
            requested_at=_OLD,
        )
        # No-price BUY (TSLA — skipped)
        _make_pending_order(db_session, fill_job_run,
                            ticker="TSLA", side=OrderSide.BUY, qty=Decimal("2"))
        # Successful BUY (MSFT)
        _make_pending_order(db_session, fill_job_run,
                            ticker="MSFT", side=OrderSide.BUY, qty=Decimal("2"))

        _add_snapshot(db_session, "AAPL", Decimal("100"))   # irrelevant — expires first
        _add_snapshot(db_session, "MSFT", Decimal("100"))   # used for fill + cache refresh

        counts = _cycle(db_session, fill_portfolio, fill_job_run)
        assert counts == {"filled": 1, "expired": 1, "failed": 0, "skipped": 1}
