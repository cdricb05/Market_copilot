"""
tests/test_decision.py — Integration tests for workflows/decision.py.

run_decision_workflow() opens its own sessions internally via get_dedicated_session(),
so all data must be committed. The conftest SAVEPOINT isolation pattern does not apply
here. The module-scoped committed-data pattern from test_api.py is used instead:
all rows are truncated in the decision_engine teardown.

Test ordering:
    TestIdempotencyGuards → TestDecisionOutcomes

Requires PAPER_TRADER_TEST_DATABASE_URL (entire module skipped when absent).
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from paper_trader.config import get_settings
from paper_trader.constants import (
    CashEntryType,
    DecisionType,
    JobRunStatus,
    OrderStatus,
    WorkflowType,
)
from paper_trader.db.models import Base, JobRun, Order, Portfolio, PriceSnapshot, TradeDecision
from paper_trader.db.session import reset_engine_state
from paper_trader.engine.portfolio import append_cash_entry
from paper_trader.workflows.decision import run_decision_workflow

_NOW  = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
_DATE = _NOW.date()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ikey() -> str:
    """Return a unique idempotency key for each workflow call."""
    return f"test-decision-{uuid.uuid4()}"


def _source() -> str:
    """Return a unique source_run string to avoid signals unique-constraint collisions."""
    return f"test-{uuid.uuid4()}"


def _buy_signal(*, ticker: str = "AAPL", source_run: str | None = None) -> dict:
    return {
        "ticker":     ticker,
        "direction":  "BUY",
        "confidence": Decimal("0.80"),
        "signal_ts":  _NOW,
        "source_run": source_run or _source(),
    }


def _sell_signal(*, ticker: str = "AAPL", source_run: str | None = None) -> dict:
    return {
        "ticker":     ticker,
        "direction":  "SELL",
        "confidence": Decimal("0.80"),
        "signal_ts":  _NOW,
        "source_run": source_run or _source(),
    }


def _hold_signal(*, ticker: str = "AAPL", source_run: str | None = None) -> dict:
    return {
        "ticker":     ticker,
        "direction":  "HOLD",
        "confidence": Decimal("0.80"),
        "signal_ts":  _NOW,
        "source_run": source_run or _source(),
    }


# ---------------------------------------------------------------------------
# Module-scoped fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def decision_engine():
    """
    Standalone engine for the test DB.

    create_all on entry. Truncates all rows on exit so the session-scoped
    db_engine.drop_all in conftest can finish cleanly.

    Skips the entire module when PAPER_TRADER_TEST_DATABASE_URL is absent.
    """
    url = os.environ.get("PAPER_TRADER_TEST_DATABASE_URL")
    if not url:
        pytest.skip("PAPER_TRADER_TEST_DATABASE_URL not set — skipping decision tests.")
    engine = create_engine(url, pool_pre_ping=True)
    Base.metadata.create_all(engine)
    yield engine
    with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            conn.execute(table.delete())
    engine.dispose()


@pytest.fixture(scope="module")
def seeded_db(decision_engine):
    """
    Redirect DATABASE_URL to the test DB and seed Portfolio + price data.

    Sets PAPER_TRADER_DATABASE_URL so that get_dedicated_session() inside
    run_decision_workflow() connects to the test DB. Calls reset_engine_state()
    so the engine singleton is rebuilt against the test URL.

    Commits a Portfolio row, an INITIAL_CAPITAL ledger entry, and a PriceSnapshot
    for AAPL so BUY-path tests have a price to work with.
    """
    db_url = decision_engine.url.render_as_string(hide_password=False)
    os.environ["PAPER_TRADER_DATABASE_URL"]    = db_url
    os.environ["PAPER_TRADER_SERVICE_API_KEY"] = "test-key"
    get_settings.cache_clear()
    reset_engine_state()

    with Session(decision_engine, autoflush=False, expire_on_commit=False) as session:
        portfolio = Portfolio(
            inception_date=_DATE,
            initial_capital=Decimal("10000.00"),
            strategy_enabled=True,
            trading_enabled=True,
            allow_new_positions=True,
            config={
                "max_positions":              5,
                "max_concentration_pct":      "0.20",
                "min_cash_pct":               "0.10",
                "max_daily_new_exposure_pct": "0.40",
                "confidence_threshold":       "0.55",
                "min_order_notional":         "50.00",
                "ticker_cooldown_hours":      0,
                "allow_averaging_down":       False,
            },
            cached_cash=Decimal("10000.00"),
            cached_total_value=Decimal("10000.00"),
            cached_as_of_ts=_NOW,
        )
        session.add(portfolio)
        session.flush()
        append_cash_entry(
            session,
            portfolio_id=portfolio.id,
            entry_type=CashEntryType.INITIAL_CAPITAL,
            amount=Decimal("10000.00"),
            description="Decision test initial capital",
        )
        session.add(PriceSnapshot(
            ticker="AAPL",
            price=Decimal("100.00"),
            session_type="REGULAR",
            price_type="CLOSE",
            snapshot_ts=_NOW,
            market_date=_DATE,
            job_run_id=None,
        ))
        session.commit()

    yield decision_engine

    get_settings.cache_clear()
    reset_engine_state()


# ---------------------------------------------------------------------------
# Idempotency guards
# ---------------------------------------------------------------------------

class TestIdempotencyGuards:
    def test_completed_run_returns_cached_summary(
        self, seeded_db
    ) -> None:
        """Calling run_decision_workflow twice with the same key returns the same dict."""
        key = _ikey()
        first = run_decision_workflow(
            idempotency_key=key,
            workflow_type=WorkflowType.PRE_MARKET,
            market_date=_DATE,
            signals=[],
            now=_NOW,
        )
        second = run_decision_workflow(
            idempotency_key=key,
            workflow_type=WorkflowType.PRE_MARKET,
            market_date=_DATE,
            signals=[_buy_signal()],   # extra signal — must be ignored
            now=_NOW,
        )
        assert second == first

    def test_running_run_raises_runtime_error(
        self, seeded_db
    ) -> None:
        """A RUNNING JobRun for the key causes an immediate RuntimeError."""
        key = _ikey()
        with Session(seeded_db, autoflush=False, expire_on_commit=False) as session:
            session.add(JobRun(
                idempotency_key=key,
                workflow_type=WorkflowType.PRE_MARKET,
                market_date=_DATE,
                status=JobRunStatus.RUNNING,
                started_at=_NOW,
            ))
            session.commit()

        with pytest.raises(RuntimeError, match="RUNNING"):
            run_decision_workflow(
                idempotency_key=key,
                workflow_type=WorkflowType.PRE_MARKET,
                market_date=_DATE,
                signals=[],
                now=_NOW,
            )

    def test_failed_run_raises_runtime_error(
        self, seeded_db
    ) -> None:
        """A FAILED JobRun for the key causes an immediate RuntimeError."""
        key = _ikey()
        with Session(seeded_db, autoflush=False, expire_on_commit=False) as session:
            session.add(JobRun(
                idempotency_key=key,
                workflow_type=WorkflowType.PRE_MARKET,
                market_date=_DATE,
                status=JobRunStatus.FAILED,
                started_at=_NOW,
                completed_at=_NOW,
                error_detail="synthetic failure",
            ))
            session.commit()

        with pytest.raises(RuntimeError, match="FAILED"):
            run_decision_workflow(
                idempotency_key=key,
                workflow_type=WorkflowType.PRE_MARKET,
                market_date=_DATE,
                signals=[],
                now=_NOW,
            )


# ---------------------------------------------------------------------------
# Decision outcomes — require seeded Portfolio and price data
# ---------------------------------------------------------------------------

class TestDecisionOutcomes:
    def test_empty_signals_creates_completed_job_run(
        self, seeded_db
    ) -> None:
        """Zero signals → all counts zero, JobRun ends COMPLETED."""
        key = _ikey()
        result = run_decision_workflow(
            idempotency_key=key,
            workflow_type=WorkflowType.PRE_MARKET,
            market_date=_DATE,
            signals=[],
            now=_NOW,
        )
        assert result == {
            "signals_ingested": 0,
            "decisions_made":   0,
            "orders_created":   0,
            "errors":           0,
        }
        with Session(seeded_db, autoflush=False, expire_on_commit=False) as session:
            job_run = session.execute(
                select(JobRun).where(JobRun.idempotency_key == key)
            ).scalar_one()
            assert job_run.status == JobRunStatus.COMPLETED
            assert job_run.result_summary == result

    def test_buy_signal_approved_creates_order(
        self, seeded_db
    ) -> None:
        """
        BUY AAPL with confidence=0.80, price=$100.
        Risk math: requested_notional = min(0.80 * 0.20 * 10000, 2000) = $1,600
                   approved_qty       = floor(1600 / 100) = 16 shares
        → TradeDecision BUY + PENDING Order, both scoped to this run's job_run_id.
        """
        key = _ikey()
        result = run_decision_workflow(
            idempotency_key=key,
            workflow_type=WorkflowType.PRE_MARKET,
            market_date=_DATE,
            signals=[_buy_signal()],
            now=_NOW,
        )
        assert result["signals_ingested"] == 1
        assert result["decisions_made"]   == 1
        assert result["orders_created"]   == 1
        assert result["errors"]           == 0

        with Session(seeded_db, autoflush=False, expire_on_commit=False) as session:
            job_run = session.execute(
                select(JobRun).where(JobRun.idempotency_key == key)
            ).scalar_one()

            td = session.execute(
                select(TradeDecision).where(TradeDecision.job_run_id == job_run.id)
            ).scalar_one()
            assert td.decision     == DecisionType.BUY
            assert td.approved_qty == Decimal("16")

            order = session.execute(
                select(Order).where(Order.job_run_id == job_run.id)
            ).scalar_one()
            assert order.side          == "BUY"
            assert order.status        == OrderStatus.PENDING
            assert order.requested_qty == Decimal("16")

    def test_sell_signal_no_position_rejected(
        self, seeded_db
    ) -> None:
        """SELL on a ticker with no open position → REJECTED, no Order created."""
        key = _ikey()
        result = run_decision_workflow(
            idempotency_key=key,
            workflow_type=WorkflowType.PRE_MARKET,
            market_date=_DATE,
            signals=[_sell_signal(ticker="MSFT")],
            now=_NOW,
        )
        assert result["signals_ingested"] == 1
        assert result["decisions_made"]   == 1
        assert result["orders_created"]   == 0
        assert result["errors"]           == 0

        with Session(seeded_db, autoflush=False, expire_on_commit=False) as session:
            job_run = session.execute(
                select(JobRun).where(JobRun.idempotency_key == key)
            ).scalar_one()

            td = session.execute(
                select(TradeDecision).where(TradeDecision.job_run_id == job_run.id)
            ).scalar_one()
            assert td.decision == DecisionType.REJECTED

            orders = session.execute(
                select(Order).where(Order.job_run_id == job_run.id)
            ).scalars().all()
            assert orders == []

    def test_hold_signal_creates_hold_decision_no_order(
        self, seeded_db
    ) -> None:
        """HOLD signal → HOLD TradeDecision, no Order row created."""
        key = _ikey()
        result = run_decision_workflow(
            idempotency_key=key,
            workflow_type=WorkflowType.PRE_MARKET,
            market_date=_DATE,
            signals=[_hold_signal(ticker="GOOG")],
            now=_NOW,
        )
        assert result["signals_ingested"] == 1
        assert result["decisions_made"]   == 1
        assert result["orders_created"]   == 0
        assert result["errors"]           == 0

        with Session(seeded_db, autoflush=False, expire_on_commit=False) as session:
            job_run = session.execute(
                select(JobRun).where(JobRun.idempotency_key == key)
            ).scalar_one()

            td = session.execute(
                select(TradeDecision).where(TradeDecision.job_run_id == job_run.id)
            ).scalar_one()
            assert td.decision == DecisionType.HOLD

            orders = session.execute(
                select(Order).where(Order.job_run_id == job_run.id)
            ).scalars().all()
            assert orders == []

    def test_per_signal_error_does_not_abort_batch(
        self, seeded_db
    ) -> None:
        """
        Two signals: one missing the required 'ticker' key (→ KeyError inside
        the savepoint), one valid HOLD.

        Expected: errors=1, signals_ingested=1, orders_created=0, decisions_made=1.
        The batch does not abort; the valid signal is processed normally.
        """
        bad_signal: dict = {
            # 'ticker' key intentionally absent — KeyError inside savepoint
            "direction":  "BUY",
            "confidence": Decimal("0.80"),
            "signal_ts":  _NOW,
            "source_run": _source(),
        }
        good_signal = _hold_signal(ticker="TSLA")

        result = run_decision_workflow(
            idempotency_key=_ikey(),
            workflow_type=WorkflowType.PRE_MARKET,
            market_date=_DATE,
            signals=[bad_signal, good_signal],
            now=_NOW,
        )
        assert result["errors"]           == 1
        assert result["signals_ingested"] == 1
        assert result["decisions_made"]   == 1
        assert result["orders_created"]   == 0
