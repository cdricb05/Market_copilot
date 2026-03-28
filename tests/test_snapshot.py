"""
tests/test_snapshot.py — Integration tests for workflows/snapshot.py.

run_snapshot_workflow() opens its own sessions internally via get_dedicated_session(),
so all data must be committed. The conftest SAVEPOINT isolation pattern does not apply
here. The module-scoped committed-data pattern from test_decision.py is used instead:
all rows are truncated in the snapshot_engine teardown.

A function-scoped autouse fixture cleans up per-test mutable state
(positions, price_snapshots, benchmark_prices) between tests so each test
starts from a consistent base.

Test classes:
    TestIdempotencyGuards → TestSnapshotCreation

Requires PAPER_TRADER_TEST_DATABASE_URL (entire module skipped when absent).
"""
from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from paper_trader.config import get_settings
from paper_trader.constants import (
    CashEntryType,
    JobRunStatus,
    WorkflowType,
)
from paper_trader.db.models import (
    Base,
    JobRun,
    Portfolio,
    PortfolioSnapshot,
    PriceSnapshot,
)
from paper_trader.db.session import reset_engine_state
from paper_trader.engine.portfolio import append_cash_entry, open_position
from paper_trader.workflows.snapshot import run_snapshot_workflow

_NOW = datetime(2025, 1, 15, 20, 0, 0, tzinfo=timezone.utc)

# Unique market dates per test to satisfy uq_portfolio_snapshots_market_date.
_DATE_EMPTY    = date(2025, 1, 10)   # no-positions test
_DATE_POSITION = date(2025, 1, 11)   # open-positions test
_DATE_MISSING  = date(2025, 1, 12)   # missing-price error test
_DATE_REPLAY   = date(2025, 1, 13)   # idempotent replay test
_DATE_RUNNING  = date(2025, 1, 14)   # RUNNING conflict test
_DATE_FAILED   = date(2025, 1, 15)   # FAILED conflict test
_DATE_BENCH    = date(2025, 1, 16)   # benchmark-null test


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ikey() -> str:
    """Return a unique idempotency key for each workflow call."""
    return f"test-snapshot-{uuid.uuid4()}"


def _run(market_date: date, idempotency_key: str | None = None) -> dict:
    """Invoke run_snapshot_workflow with a generated key unless one is given."""
    return run_snapshot_workflow(
        idempotency_key=idempotency_key or _ikey(),
        market_date=market_date,
        now=_NOW,
    )


# ---------------------------------------------------------------------------
# Module-scoped fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def snapshot_engine():
    """
    Standalone engine for the test DB.

    create_all on entry. Truncates all rows on exit so the session-scoped
    db_engine.drop_all in conftest can finish cleanly.

    Skips the entire module when PAPER_TRADER_TEST_DATABASE_URL is absent.
    """
    url = os.environ.get("PAPER_TRADER_TEST_DATABASE_URL")
    if not url:
        pytest.skip("PAPER_TRADER_TEST_DATABASE_URL not set — skipping snapshot tests.")
    engine = create_engine(url, pool_pre_ping=True)
    Base.metadata.create_all(engine)
    yield engine
    with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            conn.execute(table.delete())
    engine.dispose()


@pytest.fixture(scope="module")
def seeded_db(snapshot_engine):
    """
    Redirect DATABASE_URL to the test DB and seed Portfolio + initial capital.

    Sets PAPER_TRADER_DATABASE_URL so that get_dedicated_session() inside
    run_snapshot_workflow() connects to the test DB. Calls reset_engine_state()
    so the engine singleton is rebuilt against the test URL.

    No positions or price snapshots are seeded — each test manages its own
    mutable state, cleaned up by _clean_mutable_state.
    """
    db_url = snapshot_engine.url.render_as_string(hide_password=False)
    os.environ["PAPER_TRADER_DATABASE_URL"]    = db_url
    os.environ["PAPER_TRADER_SERVICE_API_KEY"] = "test-key"
    get_settings.cache_clear()
    reset_engine_state()

    with Session(snapshot_engine, autoflush=False, expire_on_commit=False) as session:
        portfolio = Portfolio(
            inception_date=date(2025, 1, 1),
            initial_capital=Decimal("10000.00"),
            strategy_enabled=True,
            trading_enabled=True,
            allow_new_positions=True,
            config={},
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
            description="Snapshot test initial capital",
        )
        session.commit()

    yield snapshot_engine

    get_settings.cache_clear()
    reset_engine_state()


# ---------------------------------------------------------------------------
# Per-test state cleanup
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clean_mutable_state(snapshot_engine):
    """
    Delete positions, price snapshots, and benchmark prices after each test.

    Prevents position and price state from leaking across tests.
    portfolio_snapshots and job_runs are intentionally left intact —
    idempotency tests depend on them persisting within the module.
    Portfolio config is reset to {} so benchmark tests don't contaminate others.
    """
    yield
    with snapshot_engine.begin() as conn:
        conn.execute(text("DELETE FROM positions"))
        conn.execute(text("DELETE FROM price_snapshots"))
        conn.execute(text("DELETE FROM benchmark_prices"))
        conn.execute(text("UPDATE portfolio SET config = '{}'"))


# ---------------------------------------------------------------------------
# Idempotency guards
# ---------------------------------------------------------------------------

class TestIdempotencyGuards:
    def test_completed_run_returns_cached_summary(self, seeded_db) -> None:
        """Calling run_snapshot_workflow twice with the same key returns the same dict."""
        key   = _ikey()
        first  = _run(_DATE_REPLAY, idempotency_key=key)
        second = _run(_DATE_REPLAY, idempotency_key=key)
        assert second == first

    def test_running_run_raises_runtime_error(self, seeded_db) -> None:
        """A RUNNING JobRun for the key causes an immediate RuntimeError."""
        key = _ikey()
        with Session(seeded_db, autoflush=False, expire_on_commit=False) as session:
            session.add(JobRun(
                idempotency_key=key,
                workflow_type=WorkflowType.POST_MARKET,
                market_date=_DATE_RUNNING,
                status=JobRunStatus.RUNNING,
                started_at=_NOW,
            ))
            session.commit()

        with pytest.raises(RuntimeError, match="RUNNING"):
            _run(_DATE_RUNNING, idempotency_key=key)

    def test_failed_run_raises_runtime_error(self, seeded_db) -> None:
        """A FAILED JobRun for the key causes an immediate RuntimeError."""
        key = _ikey()
        with Session(seeded_db, autoflush=False, expire_on_commit=False) as session:
            session.add(JobRun(
                idempotency_key=key,
                workflow_type=WorkflowType.POST_MARKET,
                market_date=_DATE_FAILED,
                status=JobRunStatus.FAILED,
                started_at=_NOW,
                completed_at=_NOW,
                error_detail="synthetic failure",
            ))
            session.commit()

        with pytest.raises(RuntimeError, match="FAILED"):
            _run(_DATE_FAILED, idempotency_key=key)


# ---------------------------------------------------------------------------
# Snapshot creation
# ---------------------------------------------------------------------------

class TestSnapshotCreation:
    def test_snapshot_no_positions(self, seeded_db) -> None:
        """
        Portfolio with $10,000 cash and no open positions.

        Expected:
            cash                    = $10,000.00
            positions_value         = $0.00
            total_value             = $10,000.00
            unrealized_pnl          = $0.00
            realized_pnl_cumulative = $0.00
            open_position_count     = 0
            positions_detail        = None
            all benchmark fields    = None (no benchmark_ticker configured)
        """
        key    = _ikey()
        result = _run(_DATE_EMPTY, idempotency_key=key)

        assert result["cash"]                    == "10000.00"
        assert result["positions_value"]         == "0.00"
        assert result["total_value"]             == "10000.00"
        assert result["unrealized_pnl"]          == "0.00"
        assert result["realized_pnl_cumulative"] == "0.00"
        assert result["open_position_count"]     == 0
        assert result["benchmark_ticker"]        is None
        assert result["portfolio_vs_benchmark"]  is None

        with Session(seeded_db, autoflush=False, expire_on_commit=False) as session:
            snap = session.execute(
                select(PortfolioSnapshot).where(
                    PortfolioSnapshot.market_date == _DATE_EMPTY
                )
            ).scalar_one()
            assert snap.cash                == Decimal("10000.00")
            assert snap.positions_value     == Decimal("0.00")
            assert snap.total_value         == Decimal("10000.00")
            assert snap.unrealized_pnl      == Decimal("0.00")
            assert snap.open_position_count == 0
            assert snap.positions_detail    is None
            assert snap.benchmark_ticker    is None

            job_run = session.get(JobRun, snap.job_run_id)
            assert job_run.status         == JobRunStatus.COMPLETED
            assert job_run.result_summary == result

    def test_snapshot_with_open_positions(self, seeded_db) -> None:
        """
        8 shares AAPL at avg_cost=$100, current snapshot price=$120.

        Math (cash = $10,000 from ledger only; open_position() does not debit):
            positions_value = 8 * 120         = $960.00
            total_value     = 10000 + 960     = $10,960.00
            unrealized_pnl  = (120-100) * 8   = $160.00
        """
        with Session(seeded_db, autoflush=False, expire_on_commit=False) as session:
            open_position(
                session,
                ticker="AAPL",
                qty=Decimal("8"),
                fill_price=Decimal("100.000000"),
                now=_NOW,
            )
            session.add(PriceSnapshot(
                ticker="AAPL",
                price=Decimal("120.000000"),
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=_NOW,
                market_date=_DATE_POSITION,
                job_run_id=None,
            ))
            session.commit()

        result = _run(_DATE_POSITION)

        assert result["cash"]                    == "10000.00"
        assert result["positions_value"]         == "960.00"
        assert result["total_value"]             == "10960.00"
        assert result["unrealized_pnl"]          == "160.00"
        assert result["realized_pnl_cumulative"] == "0.00"
        assert result["open_position_count"]     == 1

        with Session(seeded_db, autoflush=False, expire_on_commit=False) as session:
            snap = session.execute(
                select(PortfolioSnapshot).where(
                    PortfolioSnapshot.market_date == _DATE_POSITION
                )
            ).scalar_one()
            assert snap.positions_value     == Decimal("960.00")
            assert snap.total_value         == Decimal("10960.00")
            assert snap.unrealized_pnl      == Decimal("160.00")
            assert snap.open_position_count == 1

            assert snap.positions_detail is not None
            assert len(snap.positions_detail) == 1
            detail = snap.positions_detail[0]
            assert detail["ticker"]         == "AAPL"
            assert detail["qty"]            == "8.00000000"
            assert detail["avg_cost"]       == "100.000000"
            assert detail["current_price"]  == "120.000000"
            assert detail["market_value"]   == "960.00"
            assert detail["unrealized_pnl"] == "160.00"

    def test_missing_price_raises_value_error(self, seeded_db) -> None:
        """
        Open position with no PriceSnapshot raises ValueError.

        The JobRun row must be left FAILED with the missing ticker in error_detail.
        No PortfolioSnapshot row is created.
        """
        key = _ikey()
        with Session(seeded_db, autoflush=False, expire_on_commit=False) as session:
            open_position(
                session,
                ticker="TSLA",
                qty=Decimal("5"),
                fill_price=Decimal("200.000000"),
                now=_NOW,
            )
            session.commit()

        with pytest.raises(ValueError, match="TSLA"):
            _run(_DATE_MISSING, idempotency_key=key)

        with Session(seeded_db, autoflush=False, expire_on_commit=False) as session:
            job_run = session.execute(
                select(JobRun).where(JobRun.idempotency_key == key)
            ).scalar_one()
            assert job_run.status == JobRunStatus.FAILED
            assert "TSLA" in job_run.error_detail

            snap = session.execute(
                select(PortfolioSnapshot).where(
                    PortfolioSnapshot.market_date == _DATE_MISSING
                )
            ).scalar_one_or_none()
            assert snap is None

    def test_benchmark_fields_null_when_no_benchmark_data(self, seeded_db) -> None:
        """
        When benchmark_ticker is configured but no BenchmarkPrice rows exist,
        all four benchmark columns are NULL. This is not an error condition.

        The workflow succeeds and benchmark_ticker is reflected in both the
        result_summary and the PortfolioSnapshot row.
        """
        with Session(seeded_db, autoflush=False, expire_on_commit=False) as session:
            portfolio = session.execute(select(Portfolio)).scalar_one()
            portfolio.config = {"benchmark_ticker": "SPY"}
            session.commit()

        result = _run(_DATE_BENCH)

        assert result["benchmark_ticker"]       == "SPY"
        assert result["portfolio_vs_benchmark"] is None

        with Session(seeded_db, autoflush=False, expire_on_commit=False) as session:
            snap = session.execute(
                select(PortfolioSnapshot).where(
                    PortfolioSnapshot.market_date == _DATE_BENCH
                )
            ).scalar_one()
            assert snap.benchmark_ticker          == "SPY"
            assert snap.benchmark_price           is None
            assert snap.benchmark_inception_price is None
            assert snap.benchmark_value           is None
            assert snap.portfolio_vs_benchmark    is None
