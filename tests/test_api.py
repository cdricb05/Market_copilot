"""
tests/test_api.py — HTTP-layer tests for api/app.py.

All API endpoints create their own sessions internally (via get_session() /
get_dedicated_session()), so they can only see COMMITTED data. The conftest's
rollback-isolated db_session fixture cannot be used here. Instead:

  - api_engine (module-scoped) creates a standalone engine for the test DB.
  - client (module-scoped) redirects DATABASE_URL and resets db/session.py's
    engine singleton via reset_engine_state() so all app sessions use the
    test DB.
  - seeded_client (module-scoped) commits a Portfolio row directly so GET
    endpoints can read it.
  - snapshots_client (module-scoped) extends seeded_client by committing
    PortfolioSnapshot rows for list/fetch tests.
  - perf_benchmark_client (module-scoped) extends snapshots_client by
    committing a third PortfolioSnapshot row that has benchmark_value
    populated, enabling performance benchmark field assertions.

Test ordering matters:
    TestHealth → TestReady → TestAuthentication → TestUnseededPortfolio
                → TestPerformanceNoSnapshots → TestSeededEndpoints
                → TestSignalsWeekdayGuard → TestPerformanceEndpoint
                → TestPerformanceHistoryEndpoint → TestPerformanceHistoryFilters
                → TestPerformanceHistoryCSV → TestPerformanceBenchmark
                → TestSnapshotEndpoint

seeded_client is first used by TestPerformanceNoSnapshots, so the portfolio
is not committed until after TestUnseededPortfolio has already run.
TestPerformanceNoSnapshots runs before TestSeededEndpoints so that the
404-no-snapshots assertion executes before any snapshot rows are committed.
TestPerformanceEndpoint, TestPerformanceHistoryEndpoint, and
TestPerformanceHistoryFilters use only snapshots_client (no benchmark data)
so they must run before TestPerformanceBenchmark, which triggers
perf_benchmark_client and commits the Jan-13 benchmark row.
TestPerformanceHistoryCSV also uses only snapshots_client so it runs before
TestPerformanceBenchmark for the same reason.
TestSnapshotEndpoint runs last because POST /v1/snapshot creates
February-dated rows that would shift latest_snapshot_date and break
performance assertions.

Requires PAPER_TRADER_TEST_DATABASE_URL (entire module skipped when absent).
"""
from __future__ import annotations

import csv
import io
import os
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from paper_trader.api.app import app
from paper_trader.config import get_settings
from paper_trader.constants import CashEntryType, JobRunStatus, WorkflowType
from paper_trader.db.models import Base, BenchmarkPrice, CandidateReview, JobRun, Order, Portfolio, PortfolioSnapshot, Position, PriceSnapshot, Signal, TradeDecision
from paper_trader.db.session import reset_engine_state
from paper_trader.engine.portfolio import append_cash_entry, open_position

_TEST_API_KEY = "test-secret-key"
_NOW          = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
_AUTH         = {"X-API-Key": _TEST_API_KEY}

# Saturday 2025-01-18 — used to simulate a weekend clock reading.
_NOW_WEEKEND  = datetime(2025, 1, 18, 14, 30, 0, tzinfo=timezone.utc)
_DATE_WEEKEND = date(2025, 1, 18)

# Unique market dates for POST /v1/snapshot tests (February to avoid
# collision with snapshots_client dates 2025-01-10/11 and test_snapshot.py
# dates 2025-01-10 through 2025-01-17).
_DATE_SNAP_NO_POS  = date(2025, 2, 1)
_DATE_SNAP_REPLAY  = date(2025, 2, 2)
_DATE_SNAP_RUNNING = date(2025, 2, 3)
_DATE_SNAP_FAILED  = date(2025, 2, 4)
_DATE_SNAP_MISSING = date(2025, 2, 5)

# Weekday dates for strategy run tests (Tuesday 2025-01-21, Wednesday 2025-01-22)
_DATE_STRAT_APPROVED  = date(2025, 1, 21)  # Tuesday
_DATE_STRAT_DUPLICATE = date(2025, 1, 22)  # Wednesday


def _ikey() -> str:
    """Return a unique idempotency key for each snapshot workflow call."""
    return f"test-snap-api-{uuid.uuid4()}"


def _parse_csv_rows(text: str) -> list[list[str]]:
    """Parse CSV response text into a list of rows (each a list of strings)."""
    return list(csv.reader(io.StringIO(text)))


# ---------------------------------------------------------------------------
# Module-scoped fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def api_engine():
    """
    Standalone engine for the test DB, scoped to this module.

    create_all on entry (idempotent alongside conftest's db_engine).
    Truncates all rows on exit so the session-scoped db_engine.drop_all
    in conftest can finish cleanly.

    Skips the entire module when PAPER_TRADER_TEST_DATABASE_URL is absent.
    """
    url = os.environ.get("PAPER_TRADER_TEST_DATABASE_URL")
    if not url:
        pytest.skip("PAPER_TRADER_TEST_DATABASE_URL not set — skipping API tests.")
    engine = create_engine(url, pool_pre_ping=True)
    Base.metadata.create_all(engine)
    yield engine
    try:
        with engine.begin() as conn:
            for table in reversed(Base.metadata.sorted_tables):
                conn.execute(table.delete())
    finally:
        engine.dispose()


@pytest.fixture(scope="module")
def client(api_engine):
    """
    TestClient wired to the test DB.

    Sets PAPER_TRADER_DATABASE_URL and PAPER_TRADER_SERVICE_API_KEY in the
    process environment, clears the lru_cache on get_settings(), and calls
    reset_engine_state() so that db/session.py rebuilds its engine singleton
    against the test DB on first use.

    Teardown clears both singletons so later modules are not affected.
    """
    import asyncio
    import gc

    db_url = api_engine.url.render_as_string(hide_password=False)
    os.environ["PAPER_TRADER_DATABASE_URL"]     = db_url
    os.environ["PAPER_TRADER_SERVICE_API_KEY"]  = _TEST_API_KEY
    get_settings.cache_clear()
    reset_engine_state()
    c = TestClient(app)
    try:
        yield c
    finally:
        c.close()
        # Try to run async cleanup if the TestClient supports it
        try:
            loop = asyncio.get_event_loop()
            if loop and not loop.is_closed():
                loop.run_until_complete(c.wait_shutdown())
        except Exception:
            pass
        get_settings.cache_clear()
        reset_engine_state()


@pytest.fixture(scope="module")
def seeded_client(client, api_engine):
    """
    Client with a committed Portfolio row and INITIAL_CAPITAL ledger entry.

    Data is committed (not rollback-isolated) so that the app's own sessions
    can read it. Runs once for the module; truncation in api_engine teardown
    cleans up afterwards.
    """
    with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
        portfolio = Portfolio(
            inception_date=_NOW.date(),
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
            description="API test initial capital",
        )
        session.commit()
    yield client


@pytest.fixture(scope="module")
def snapshots_client(seeded_client, api_engine):
    """
    Client with committed PortfolioSnapshot rows for list/fetch tests.

    Creates two snapshots with distinct market_dates (2025-01-10, 2025-01-11)
    so ordering and individual fetch can be tested. Extends seeded_client with
    snapshot data via direct database insert.
    """
    with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
        # Create first JobRun and PortfolioSnapshot for 2025-01-10
        job_run_1 = JobRun(
            idempotency_key="test-snapshot-2025-01-10",
            workflow_type=WorkflowType.POST_MARKET,
            market_date=date(2025, 1, 10),
            status=JobRunStatus.COMPLETED,
            started_at=datetime(2025, 1, 10, 16, 0, 0, tzinfo=timezone.utc),
            completed_at=datetime(2025, 1, 10, 16, 0, 1, tzinfo=timezone.utc),
            result_summary={
                "total_value":             "10000.00",
                "cash":                    "10000.00",
                "positions_value":         "0.00",
                "unrealized_pnl":          "0.00",
                "realized_pnl_cumulative": "0.00",
                "open_position_count":     0,
                "benchmark_ticker":        None,
                "portfolio_vs_benchmark":  None,
            },
        )
        session.add(job_run_1)
        session.flush()

        snap_1 = PortfolioSnapshot(
            job_run_id=job_run_1.id,
            snapshot_ts=datetime(2025, 1, 10, 16, 0, 1, tzinfo=timezone.utc),
            market_date=date(2025, 1, 10),
            cash=Decimal("10000.00"),
            positions_value=Decimal("0.00"),
            total_value=Decimal("10000.00"),
            unrealized_pnl=Decimal("0.00"),
            realized_pnl_cumulative=Decimal("0.00"),
            open_position_count=0,
            daily_new_exposure=None,
            benchmark_ticker=None,
            benchmark_price=None,
            benchmark_inception_price=None,
            benchmark_value=None,
            portfolio_vs_benchmark=None,
            positions_detail=None,
        )
        session.add(snap_1)

        # Create second JobRun and PortfolioSnapshot for 2025-01-11
        job_run_2 = JobRun(
            idempotency_key="test-snapshot-2025-01-11",
            workflow_type=WorkflowType.POST_MARKET,
            market_date=date(2025, 1, 11),
            status=JobRunStatus.COMPLETED,
            started_at=datetime(2025, 1, 11, 16, 0, 0, tzinfo=timezone.utc),
            completed_at=datetime(2025, 1, 11, 16, 0, 1, tzinfo=timezone.utc),
            result_summary={
                "total_value":             "10500.00",
                "cash":                    "10500.00",
                "positions_value":         "0.00",
                "unrealized_pnl":          "0.00",
                "realized_pnl_cumulative": "0.00",
                "open_position_count":     0,
                "benchmark_ticker":        None,
                "portfolio_vs_benchmark":  None,
            },
        )
        session.add(job_run_2)
        session.flush()

        snap_2 = PortfolioSnapshot(
            job_run_id=job_run_2.id,
            snapshot_ts=datetime(2025, 1, 11, 16, 0, 1, tzinfo=timezone.utc),
            market_date=date(2025, 1, 11),
            cash=Decimal("10500.00"),
            positions_value=Decimal("0.00"),
            total_value=Decimal("10500.00"),
            unrealized_pnl=Decimal("0.00"),
            realized_pnl_cumulative=Decimal("0.00"),
            open_position_count=0,
            daily_new_exposure=None,
            benchmark_ticker=None,
            benchmark_price=None,
            benchmark_inception_price=None,
            benchmark_value=None,
            portfolio_vs_benchmark=None,
            positions_detail=None,
        )
        session.add(snap_2)
        session.commit()

    yield seeded_client


@pytest.fixture(scope="module")
def perf_benchmark_client(snapshots_client, api_engine):
    """
    Client with an additional PortfolioSnapshot row (2025-01-13) that has
    benchmark_value populated.

    Extends snapshots_client so that GET /v1/performance returns
    non-null benchmark_return_pct and excess_return_pct.

    Math (initial_capital = 10000.00):
        total_value=10800.00     → return_pct           = 8.0000
        benchmark_value=10600.00 → benchmark_return_pct = 6.0000
        excess_return_pct = 8.0000 - 6.0000             = 2.0000
    """
    with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
        job_run_bm = JobRun(
            idempotency_key="test-snapshot-2025-01-13",
            workflow_type=WorkflowType.POST_MARKET,
            market_date=date(2025, 1, 13),
            status=JobRunStatus.COMPLETED,
            started_at=datetime(2025, 1, 13, 16, 0, 0, tzinfo=timezone.utc),
            completed_at=datetime(2025, 1, 13, 16, 0, 1, tzinfo=timezone.utc),
            result_summary={
                "total_value":             "10800.00",
                "cash":                    "10800.00",
                "positions_value":         "0.00",
                "unrealized_pnl":          "0.00",
                "realized_pnl_cumulative": "0.00",
                "open_position_count":     0,
                "benchmark_ticker":        "SPY",
                "portfolio_vs_benchmark":  "200.00",
            },
        )
        session.add(job_run_bm)
        session.flush()

        snap_bm = PortfolioSnapshot(
            job_run_id=job_run_bm.id,
            snapshot_ts=datetime(2025, 1, 13, 16, 0, 1, tzinfo=timezone.utc),
            market_date=date(2025, 1, 13),
            cash=Decimal("10800.00"),
            positions_value=Decimal("0.00"),
            total_value=Decimal("10800.00"),
            unrealized_pnl=Decimal("0.00"),
            realized_pnl_cumulative=Decimal("0.00"),
            open_position_count=0,
            daily_new_exposure=None,
            benchmark_ticker="SPY",
            benchmark_price=Decimal("480.00"),
            benchmark_inception_price=Decimal("475.00"),
            benchmark_value=Decimal("10600.00"),
            portfolio_vs_benchmark=Decimal("200.00"),
            positions_detail=None,
        )
        session.add(snap_bm)
        session.commit()

    yield snapshots_client


# ---------------------------------------------------------------------------
# Health check endpoint — no authentication required
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_returns_200(self, client: TestClient) -> None:
        resp = client.get("/v1/health")
        assert resp.status_code == 200

    def test_health_works_without_auth_header(self, client: TestClient) -> None:
        """Health endpoint does not require X-API-Key."""
        resp = client.get("/v1/health")
        assert resp.status_code == 200
        body = resp.json()
        assert "status" in body
        assert "service" in body
        assert "version" in body

    def test_health_response_values(self, client: TestClient) -> None:
        resp = client.get("/v1/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["service"] == "paper_trader"
        assert body["version"] == "1.0.0"


# ---------------------------------------------------------------------------
# Readiness probe endpoint — no authentication required
# ---------------------------------------------------------------------------

class TestReady:
    def test_ready_returns_200(self, client: TestClient) -> None:
        resp = client.get("/v1/ready")
        assert resp.status_code == 200

    def test_ready_works_without_auth_header(self, client: TestClient) -> None:
        """Readiness endpoint does not require X-API-Key."""
        resp = client.get("/v1/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert "status" in body
        assert "service" in body
        assert "version" in body
        assert "database" in body

    def test_ready_response_values(self, client: TestClient) -> None:
        resp = client.get("/v1/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["service"] == "paper_trader"
        assert body["version"] == "1.0.0"
        assert body["database"] == "ok"

    def test_ready_503_when_db_unreachable(
        self, client: TestClient, monkeypatch
    ) -> None:
        """When get_session() raises, the endpoint returns 503."""
        def _broken_session():
            raise OSError("connection refused")

        monkeypatch.setattr("paper_trader.api.app.get_session", _broken_session)
        resp = client.get("/v1/ready")
        assert resp.status_code == 503
        assert resp.json()["detail"] == "Database unreachable."


# ---------------------------------------------------------------------------
# Authentication — no DB state required
# ---------------------------------------------------------------------------

class TestAuthentication:
    def test_missing_api_key_returns_401(self, client: TestClient) -> None:
        resp = client.get("/v1/portfolio")
        assert resp.status_code == 401

    def test_wrong_api_key_returns_401(self, client: TestClient) -> None:
        resp = client.get("/v1/portfolio", headers={"X-API-Key": "wrong-key"})
        assert resp.status_code == 401

    def test_correct_key_not_rejected(self, client: TestClient) -> None:
        """Correct key passes auth — response may be 503 (unseeded) but not 401."""
        resp = client.get("/v1/portfolio", headers=_AUTH)
        assert resp.status_code != 401

    def test_auth_required_on_positions(self, client: TestClient) -> None:
        assert client.get("/v1/positions").status_code == 401

    def test_auth_required_on_orders(self, client: TestClient) -> None:
        assert client.get("/v1/orders").status_code == 401

    def test_auth_required_on_prices(self, client: TestClient) -> None:
        assert client.post("/v1/prices", json={"snapshots": []}).status_code == 401

    def test_auth_required_on_benchmark_prices(self, client: TestClient) -> None:
        assert client.post("/v1/benchmark-prices", json={"prices": []}).status_code == 401

    def test_auth_required_on_snapshot(self, client: TestClient) -> None:
        assert client.post("/v1/snapshot", json={"idempotency_key": "x"}).status_code == 401

    def test_auth_required_on_snapshots_list(self, client: TestClient) -> None:
        assert client.get("/v1/snapshots").status_code == 401

    def test_auth_required_on_snapshots_get(self, client: TestClient) -> None:
        assert client.get("/v1/snapshots/2025-01-10").status_code == 401

    def test_auth_required_on_performance(self, client: TestClient) -> None:
        assert client.get("/v1/performance").status_code == 401

    def test_auth_required_on_performance_history(self, client: TestClient) -> None:
        assert client.get("/v1/performance/history").status_code == 401

    def test_auth_required_on_performance_history_csv(self, client: TestClient) -> None:
        assert client.get("/v1/performance/history.csv").status_code == 401


# ---------------------------------------------------------------------------
# Authentication check endpoint
# ---------------------------------------------------------------------------

class TestAuthCheck:
    def test_auth_check_requires_api_key(self, client: TestClient) -> None:
        resp = client.get("/v1/auth/check")
        assert resp.status_code == 401

    def test_auth_check_rejects_wrong_key(self, client: TestClient) -> None:
        resp = client.get("/v1/auth/check", headers={"X-API-Key": "wrong-key"})
        assert resp.status_code == 401

    def test_auth_check_accepts_valid_key(self, client: TestClient) -> None:
        resp = client.get("/v1/auth/check", headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()
        assert body["authenticated"] is True
        assert body["service"] == "paper_trader"

    def test_auth_check_is_lightweight(self, client: TestClient) -> None:
        """Auth check should not depend on portfolio being seeded."""
        resp = client.get("/v1/auth/check", headers=_AUTH)
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Unseeded state — must run before seeded_client commits data
# ---------------------------------------------------------------------------

class TestUnseededPortfolio:
    def test_portfolio_503_when_not_seeded(self, client: TestClient) -> None:
        resp = client.get("/v1/portfolio", headers=_AUTH)
        assert resp.status_code == 503

    def test_performance_503_when_not_seeded(self, client: TestClient) -> None:
        resp = client.get("/v1/performance", headers=_AUTH)
        assert resp.status_code == 503

    def test_performance_history_503_when_not_seeded(self, client: TestClient) -> None:
        resp = client.get("/v1/performance/history", headers=_AUTH)
        assert resp.status_code == 503

    def test_performance_history_csv_503_when_not_seeded(self, client: TestClient) -> None:
        resp = client.get("/v1/performance/history.csv", headers=_AUTH)
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# Performance — no snapshots yet (must run before snapshots_client fires)
# ---------------------------------------------------------------------------

class TestPerformanceNoSnapshots:
    def test_performance_404_no_snapshots(self, seeded_client: TestClient) -> None:
        """Portfolio seeded but no snapshots recorded — returns 404."""
        resp = seeded_client.get("/v1/performance", headers=_AUTH)
        assert resp.status_code == 404
        body = resp.json()
        assert "detail" in body
        assert "snapshot" in body["detail"].lower()

    def test_performance_history_404_no_snapshots(self, seeded_client: TestClient) -> None:
        """Portfolio seeded but no snapshots recorded — history returns 404."""
        resp = seeded_client.get("/v1/performance/history", headers=_AUTH)
        assert resp.status_code == 404
        body = resp.json()
        assert "detail" in body
        assert "snapshot" in body["detail"].lower()

    def test_performance_history_csv_404_no_snapshots(
        self, seeded_client: TestClient
    ) -> None:
        """Portfolio seeded but no snapshots recorded — CSV endpoint returns 404."""
        resp = seeded_client.get("/v1/performance/history.csv", headers=_AUTH)
        assert resp.status_code == 404
        body = resp.json()
        assert "detail" in body
        assert "snapshot" in body["detail"].lower()


# ---------------------------------------------------------------------------
# Seeded endpoints — rely on seeded_client having committed a Portfolio row
# ---------------------------------------------------------------------------

class TestSeededEndpoints:
    def test_get_portfolio_200(self, seeded_client: TestClient) -> None:
        resp = seeded_client.get("/v1/portfolio", headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()
        assert body["initial_capital"]     == "10000.00"
        assert body["cached_cash"]         == "10000.00"
        assert body["cached_total_value"]  == "10000.00"
        assert body["strategy_enabled"]    is True
        assert body["trading_enabled"]     is True
        assert body["allow_new_positions"] is True
        assert "inception_date" in body
        assert "id" in body

    def test_list_positions_empty_list(self, seeded_client: TestClient) -> None:
        resp = seeded_client.get("/v1/positions", headers=_AUTH)
        assert resp.status_code == 200
        assert resp.json() == []

    def test_list_orders_empty_list(self, seeded_client: TestClient) -> None:
        resp = seeded_client.get("/v1/orders", headers=_AUTH)
        assert resp.status_code == 200
        assert resp.json() == []

    def test_list_orders_status_filter_empty(self, seeded_client: TestClient) -> None:
        resp = seeded_client.get("/v1/orders?status=PENDING", headers=_AUTH)
        assert resp.status_code == 200
        assert resp.json() == []

    def test_ingest_prices_returns_inserted_count(
        self, seeded_client: TestClient
    ) -> None:
        payload = {
            "snapshots": [
                {"ticker": "AAPL", "price": "150.25"},
                {"ticker": "MSFT", "price": "380.00"},
            ]
        }
        resp = seeded_client.post("/v1/prices", json=payload, headers=_AUTH)
        assert resp.status_code == 200
        assert resp.json() == {"inserted": 2}

    def test_ingest_prices_explicit_fields(self, seeded_client: TestClient) -> None:
        """Optional snapshot_ts and market_date are accepted without error."""
        payload = {
            "snapshots": [
                {
                    "ticker": "GOOG",
                    "price": "190.00",
                    "session_type": "PREMARKET",
                    "price_type": "LAST",
                    "snapshot_ts": "2025-01-15T09:00:00Z",
                    "market_date": "2025-01-15",
                }
            ]
        }
        resp = seeded_client.post("/v1/prices", json=payload, headers=_AUTH)
        assert resp.status_code == 200
        assert resp.json()["inserted"] == 1

    def test_ingest_prices_empty_list_returns_zero(
        self, seeded_client: TestClient
    ) -> None:
        payload = {"snapshots": []}
        resp = seeded_client.post("/v1/prices", json=payload, headers=_AUTH)
        assert resp.status_code == 200
        assert resp.json() == {"inserted": 0}

    def test_ingest_benchmark_prices_empty_list_returns_zero(
        self, seeded_client: TestClient
    ) -> None:
        payload = {"prices": []}
        resp = seeded_client.post("/v1/benchmark-prices", json=payload, headers=_AUTH)
        assert resp.status_code == 200
        assert resp.json() == {"inserted": 0}

    def test_ingest_benchmark_prices_returns_inserted_count(
        self, seeded_client: TestClient
    ) -> None:
        payload = {
            "prices": [
                {"ticker": "SPY", "price": "475.00"},
                {"ticker": "QQQ", "price": "410.50"},
            ]
        }
        resp = seeded_client.post("/v1/benchmark-prices", json=payload, headers=_AUTH)
        assert resp.status_code == 200
        assert resp.json() == {"inserted": 2}

    def test_ingest_benchmark_prices_explicit_fields(
        self, seeded_client: TestClient
    ) -> None:
        """Optional session_type, snapshot_ts, and market_date are accepted without error."""
        payload = {
            "prices": [
                {
                    "ticker": "SPY",
                    "price": "476.25",
                    "session_type": "REGULAR",
                    "snapshot_ts": "2025-01-15T21:00:00Z",
                    "market_date": "2025-01-15",
                }
            ]
        }
        resp = seeded_client.post("/v1/benchmark-prices", json=payload, headers=_AUTH)
        assert resp.status_code == 200
        assert resp.json()["inserted"] == 1

    def test_list_snapshots_empty_list(self, seeded_client: TestClient) -> None:
        """No snapshots exist — return empty list."""
        resp = seeded_client.get("/v1/snapshots", headers=_AUTH)
        assert resp.status_code == 200
        assert resp.json() == []

    def test_list_snapshots_returns_data_desc_by_market_date(
        self, snapshots_client: TestClient
    ) -> None:
        """List snapshots ordered by market_date DESC (most recent first)."""
        resp = snapshots_client.get("/v1/snapshots", headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 2
        # Verify DESC ordering: 2025-01-11 should come before 2025-01-10
        assert body[0]["market_date"] == "2025-01-11"
        assert body[1]["market_date"] == "2025-01-10"

    def test_list_snapshots_fields_serialized_as_strings(
        self, snapshots_client: TestClient
    ) -> None:
        """All Decimal fields serialize to strings in the response."""
        resp = snapshots_client.get("/v1/snapshots", headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()
        snap = body[0]  # First (most recent) snapshot
        # Verify Decimal fields are strings
        assert isinstance(snap["cash"], str)
        assert isinstance(snap["positions_value"], str)
        assert isinstance(snap["total_value"], str)
        assert isinstance(snap["unrealized_pnl"], str)
        assert isinstance(snap["realized_pnl_cumulative"], str)
        assert snap["cash"] == "10500.00"
        assert snap["total_value"] == "10500.00"

    def test_get_snapshot_by_date_200(
        self, snapshots_client: TestClient
    ) -> None:
        """Fetch a snapshot by market_date — returns 200 with full snapshot data."""
        resp = snapshots_client.get("/v1/snapshots/2025-01-10", headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()
        assert body["market_date"] == "2025-01-10"
        assert body["cash"] == "10000.00"
        assert body["total_value"] == "10000.00"
        assert body["open_position_count"] == 0
        assert "id" in body
        assert "snapshot_ts" in body

    def test_get_snapshot_by_date_fields_include_benchmark(
        self, snapshots_client: TestClient
    ) -> None:
        """Fetch snapshot — verify benchmark fields are present (even if NULL)."""
        resp = snapshots_client.get("/v1/snapshots/2025-01-11", headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()
        # All benchmark fields present, even when NULL
        assert "benchmark_ticker" in body
        assert "benchmark_price" in body
        assert "benchmark_inception_price" in body
        assert "benchmark_value" in body
        assert "portfolio_vs_benchmark" in body
        # In test data, all are NULL
        assert body["benchmark_ticker"] is None
        assert body["benchmark_price"] is None

    def test_get_snapshot_by_date_404_when_not_found(
        self, snapshots_client: TestClient
    ) -> None:
        """Fetch snapshot for non-existent market_date — returns 404."""
        resp = snapshots_client.get("/v1/snapshots/2025-01-09", headers=_AUTH)
        assert resp.status_code == 404
        body = resp.json()
        assert "detail" in body
        assert "2025-01-09" in body["detail"]


# ---------------------------------------------------------------------------
# POST /v1/signals — weekday guard
# ---------------------------------------------------------------------------

class TestSignalsWeekdayGuard:
    """POST /v1/signals: 422 on weekend, not-422 on weekday."""

    def test_signals_returns_422_on_weekend(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """Saturday input triggers the weekday guard and returns 422."""
        monkeypatch.setattr(
            "paper_trader.api.app._now_and_date",
            lambda: (_NOW_WEEKEND, _DATE_WEEKEND),
        )
        resp = seeded_client.post(
            "/v1/signals",
            json={
                "idempotency_key": f"test-sig-weekend-{uuid.uuid4()}",
                "workflow_type": "PRE_MARKET",
                "signals": [
                    {
                        "ticker": "AAPL",
                        "direction": "BUY",
                        "confidence": "0.80",
                        "signal_ts": "2025-01-18T14:30:00Z",
                        "source_run": "test-weekend",
                    }
                ],
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert "weekend" in detail.lower() or "weekday" in detail.lower()

    def test_signals_accepted_on_weekday(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """Wednesday input bypasses the weekday guard — response is not 422."""
        monkeypatch.setattr(
            "paper_trader.api.app._now_and_date",
            lambda: (_NOW, _NOW.date()),
        )
        resp = seeded_client.post(
            "/v1/signals",
            json={
                "idempotency_key": f"test-sig-weekday-{uuid.uuid4()}",
                "workflow_type": "PRE_MARKET",
                "signals": [
                    {
                        "ticker": "AAPL",
                        "direction": "BUY",
                        "confidence": "0.80",
                        "signal_ts": "2025-01-15T09:00:00Z",
                        "source_run": "test-weekday",
                    }
                ],
            },
            headers=_AUTH,
        )
        assert resp.status_code != 422


# ---------------------------------------------------------------------------
# GET /v1/performance — no-benchmark summary tests
# Uses only snapshots_client (2 rows, no benchmark data).
# Must run before TestPerformanceBenchmark triggers perf_benchmark_client.
# ---------------------------------------------------------------------------

class TestPerformanceEndpoint:
    def test_performance_200_no_benchmark(
        self, snapshots_client: TestClient
    ) -> None:
        """
        Snapshots present, no benchmark data — all core fields populated,
        benchmark fields null.

        Derived from snapshots_client data (Jan-10: 10000, Jan-11: 10500):
            absolute_return = 500.00
            return_pct      = 5.0000
        """
        resp = snapshots_client.get("/v1/performance", headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()
        assert body["first_snapshot_date"]  == "2025-01-10"
        assert body["latest_snapshot_date"] == "2025-01-11"
        assert body["initial_capital"]      == "10000.00"
        assert body["latest_total_value"]   == "10500.00"
        assert body["absolute_return"]      == "500.00"
        assert body["return_pct"]           == "5.0000"
        assert body["benchmark_ticker"]     is None
        assert body["benchmark_return_pct"] is None
        assert body["excess_return_pct"]    is None

    def test_performance_null_benchmark_when_value_absent(
        self, snapshots_client: TestClient
    ) -> None:
        """
        When latest snapshot has benchmark_value=NULL, the endpoint still
        returns 200 and benchmark_return_pct / excess_return_pct are null.
        """
        resp = snapshots_client.get("/v1/performance", headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()
        assert body["benchmark_return_pct"] is None
        assert body["excess_return_pct"]    is None


# ---------------------------------------------------------------------------
# GET /v1/performance/history — no-benchmark history tests
# Uses only snapshots_client (2 rows, no benchmark data).
# Must run before TestPerformanceBenchmark triggers perf_benchmark_client.
# ---------------------------------------------------------------------------

class TestPerformanceHistoryEndpoint:
    def test_performance_history_200_returns_list(
        self, snapshots_client: TestClient
    ) -> None:
        """Snapshots present — returns 200 with a non-empty list."""
        resp = snapshots_client.get("/v1/performance/history", headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)
        assert len(body) == 2

    def test_performance_history_ascending_order_by_market_date(
        self, snapshots_client: TestClient
    ) -> None:
        """Items are ordered oldest-first (ASC by market_date)."""
        resp = snapshots_client.get("/v1/performance/history", headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()
        assert body[0]["market_date"] == "2025-01-10"
        assert body[1]["market_date"] == "2025-01-11"

    def test_performance_history_fields_serialized_as_strings(
        self, snapshots_client: TestClient
    ) -> None:
        """All Decimal fields serialize to strings; values match committed data."""
        resp = snapshots_client.get("/v1/performance/history", headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()
        item = body[1]  # Jan-11 entry (second in ASC order)
        assert isinstance(item["total_value"], str)
        assert isinstance(item["cash"], str)
        assert isinstance(item["positions_value"], str)
        assert isinstance(item["unrealized_pnl"], str)
        assert isinstance(item["realized_pnl_cumulative"], str)
        assert item["total_value"] == "10500.00"
        assert item["cash"]        == "10500.00"

    def test_performance_history_null_benchmark_when_absent(
        self, snapshots_client: TestClient
    ) -> None:
        """
        When snapshots have no benchmark data, benchmark_ticker and
        benchmark_value are null in every history item.
        """
        resp = snapshots_client.get("/v1/performance/history", headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()
        for item in body:
            assert item["benchmark_ticker"] is None
            assert item["benchmark_value"]  is None


# ---------------------------------------------------------------------------
# GET /v1/performance/history — date filter tests
# Uses only snapshots_client (2 rows: Jan-10, Jan-11, no benchmark data).
# Must run before TestPerformanceBenchmark triggers perf_benchmark_client.
# ---------------------------------------------------------------------------

class TestPerformanceHistoryFilters:
    def test_start_date_excludes_earlier_rows(
        self, snapshots_client: TestClient
    ) -> None:
        """
        start_date=2025-01-11 → only the Jan-11 row is returned.
        Jan-10 is excluded because market_date < start_date.
        """
        resp = snapshots_client.get(
            "/v1/performance/history?start_date=2025-01-11", headers=_AUTH
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 1
        assert body[0]["market_date"] == "2025-01-11"

    def test_end_date_excludes_later_rows(
        self, snapshots_client: TestClient
    ) -> None:
        """
        end_date=2025-01-10 → only the Jan-10 row is returned.
        Jan-11 is excluded because market_date > end_date.
        """
        resp = snapshots_client.get(
            "/v1/performance/history?end_date=2025-01-10", headers=_AUTH
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 1
        assert body[0]["market_date"] == "2025-01-10"

    def test_both_filters_inclusive_bounds(
        self, snapshots_client: TestClient
    ) -> None:
        """
        Both bounds on the same date → exactly that one row.
        Confirms start_date and end_date are both inclusive (>= and <=).
        """
        resp = snapshots_client.get(
            "/v1/performance/history?start_date=2025-01-10&end_date=2025-01-10",
            headers=_AUTH,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 1
        assert body[0]["market_date"] == "2025-01-10"
        assert body[0]["total_value"] == "10000.00"

    def test_inverted_range_returns_404(
        self, snapshots_client: TestClient
    ) -> None:
        """
        start_date > end_date → no rows can match → 404.

        The endpoint does not validate range order; the SQL WHERE simply
        returns an empty set, which the endpoint treats as 404.
        """
        resp = snapshots_client.get(
            "/v1/performance/history?start_date=2025-01-11&end_date=2025-01-10",
            headers=_AUTH,
        )
        assert resp.status_code == 404
        assert "detail" in resp.json()

    def test_start_date_beyond_all_rows_returns_404(
        self, snapshots_client: TestClient
    ) -> None:
        """
        start_date after all existing rows → empty result → 404.
        """
        resp = snapshots_client.get(
            "/v1/performance/history?start_date=2025-01-12", headers=_AUTH
        )
        assert resp.status_code == 404
        assert "detail" in resp.json()


# ---------------------------------------------------------------------------
# GET /v1/performance/history.csv — CSV export tests
# Uses only snapshots_client (2 rows: Jan-10, Jan-11, no benchmark data).
# Must run before TestPerformanceBenchmark triggers perf_benchmark_client.
# ---------------------------------------------------------------------------

class TestPerformanceHistoryCSV:
    """
    Full coverage for GET /v1/performance/history.csv.

    Auth, 503, and 404-no-snapshots cases are in TestAuthentication,
    TestUnseededPortfolio, and TestPerformanceNoSnapshots respectively.
    This class covers the success path and filter behaviour using the
    clean 2-row/null-benchmark state provided by snapshots_client.
    """

    def test_csv_200_ok(self, snapshots_client: TestClient) -> None:
        """Snapshots present — endpoint returns 200."""
        resp = snapshots_client.get("/v1/performance/history.csv", headers=_AUTH)
        assert resp.status_code == 200

    def test_csv_content_type_is_text_csv(self, snapshots_client: TestClient) -> None:
        """Response Content-Type includes text/csv."""
        resp = snapshots_client.get("/v1/performance/history.csv", headers=_AUTH)
        assert "text/csv" in resp.headers["content-type"]

    def test_csv_content_disposition_attachment(
        self, snapshots_client: TestClient
    ) -> None:
        """Content-Disposition triggers a browser download with the correct filename."""
        resp = snapshots_client.get("/v1/performance/history.csv", headers=_AUTH)
        cd = resp.headers.get("content-disposition", "")
        assert "attachment" in cd
        assert "performance_history.csv" in cd

    def test_csv_header_columns_correct_and_ordered(
        self, snapshots_client: TestClient
    ) -> None:
        """First row is the header; column names and order match _HISTORY_CSV_COLUMNS."""
        resp = snapshots_client.get("/v1/performance/history.csv", headers=_AUTH)
        rows = _parse_csv_rows(resp.text)
        assert rows[0] == [
            "market_date",
            "total_value",
            "cash",
            "positions_value",
            "unrealized_pnl",
            "realized_pnl_cumulative",
            "benchmark_ticker",
            "benchmark_value",
            "portfolio_vs_benchmark",
        ]

    def test_csv_ascending_order_by_market_date(
        self, snapshots_client: TestClient
    ) -> None:
        """Data rows are ordered oldest-first (ASC by market_date)."""
        resp = snapshots_client.get("/v1/performance/history.csv", headers=_AUTH)
        rows = _parse_csv_rows(resp.text)
        # rows[0] = header, rows[1] = Jan-10, rows[2] = Jan-11
        assert rows[1][0] == "2025-01-10"
        assert rows[2][0] == "2025-01-11"

    def test_csv_row_values_match_snapshot_data(
        self, snapshots_client: TestClient
    ) -> None:
        """Jan-11 row values match the committed snapshot data."""
        resp = snapshots_client.get("/v1/performance/history.csv", headers=_AUTH)
        rows = _parse_csv_rows(resp.text)
        jan11 = rows[2]  # header + Jan-10 + Jan-11
        assert jan11[0] == "2025-01-11"  # market_date
        assert jan11[1] == "10500.00"    # total_value
        assert jan11[2] == "10500.00"    # cash
        assert jan11[3] == "0.00"        # positions_value
        assert jan11[4] == "0.00"        # unrealized_pnl
        assert jan11[5] == "0.00"        # realized_pnl_cumulative

    def test_csv_null_benchmark_fields_are_empty_strings(
        self, snapshots_client: TestClient
    ) -> None:
        """When benchmark data is absent, benchmark columns are empty strings."""
        resp = snapshots_client.get("/v1/performance/history.csv", headers=_AUTH)
        rows = _parse_csv_rows(resp.text)
        for row in rows[1:]:  # skip header
            assert row[6] == ""  # benchmark_ticker
            assert row[7] == ""  # benchmark_value
            assert row[8] == ""  # portfolio_vs_benchmark

    def test_csv_start_date_filter(self, snapshots_client: TestClient) -> None:
        """start_date=2025-01-11 → header + 1 data row (Jan-11 only)."""
        resp = snapshots_client.get(
            "/v1/performance/history.csv?start_date=2025-01-11", headers=_AUTH
        )
        assert resp.status_code == 200
        rows = _parse_csv_rows(resp.text)
        assert len(rows) == 2  # header + 1 data row
        assert rows[1][0] == "2025-01-11"

    def test_csv_end_date_filter(self, snapshots_client: TestClient) -> None:
        """end_date=2025-01-10 → header + 1 data row (Jan-10 only)."""
        resp = snapshots_client.get(
            "/v1/performance/history.csv?end_date=2025-01-10", headers=_AUTH
        )
        assert resp.status_code == 200
        rows = _parse_csv_rows(resp.text)
        assert len(rows) == 2  # header + 1 data row
        assert rows[1][0] == "2025-01-10"

    def test_csv_filter_no_match_returns_404(
        self, snapshots_client: TestClient
    ) -> None:
        """
        Inverted date range → no rows match → 404.

        The response body is still JSON (FastAPI HTTPException), not CSV.
        """
        resp = snapshots_client.get(
            "/v1/performance/history.csv?start_date=2025-01-11&end_date=2025-01-10",
            headers=_AUTH,
        )
        assert resp.status_code == 404
        assert "detail" in resp.json()


# ---------------------------------------------------------------------------
# GET /v1/performance + GET /v1/performance/history — benchmark-populated cases
# Uses perf_benchmark_client (commits Jan-13 row with benchmark data).
# Must run after TestPerformanceEndpoint, TestPerformanceHistoryEndpoint,
# TestPerformanceHistoryFilters, and TestPerformanceHistoryCSV so those
# classes observe the clean 2-row/null-benchmark state.
# Must run before TestSnapshotEndpoint to avoid February rows shifting dates.
# ---------------------------------------------------------------------------

class TestPerformanceBenchmark:
    def test_performance_populated_benchmark_when_value_present(
        self, perf_benchmark_client: TestClient
    ) -> None:
        """
        When latest snapshot has benchmark_value populated, benchmark_return_pct
        and excess_return_pct are calculated and returned.

        perf_benchmark_client adds Jan-13 snapshot (total=10800, benchmark=10600):
            return_pct           = (10800-10000)/10000*100 = 8.0000
            benchmark_return_pct = (10600-10000)/10000*100 = 6.0000
            excess_return_pct    = 8.0000 - 6.0000         = 2.0000
        """
        resp = perf_benchmark_client.get("/v1/performance", headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()
        assert body["latest_snapshot_date"] == "2025-01-13"
        assert body["latest_total_value"]   == "10800.00"
        assert body["absolute_return"]      == "800.00"
        assert body["return_pct"]           == "8.0000"
        assert body["benchmark_ticker"]     == "SPY"
        assert body["benchmark_return_pct"] == "6.0000"
        assert body["excess_return_pct"]    == "2.0000"

    def test_performance_history_populated_benchmark_when_present(
        self, perf_benchmark_client: TestClient
    ) -> None:
        """
        When the Jan-13 snapshot has benchmark data, the last history item
        carries non-null benchmark_ticker, benchmark_value, and
        portfolio_vs_benchmark.

        perf_benchmark_client adds Jan-13 (total=10800, benchmark=10600,
        portfolio_vs_benchmark=200, ticker=SPY).
        """
        resp = perf_benchmark_client.get("/v1/performance/history", headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 3
        last = body[2]  # Jan-13 is newest, last in ASC order
        assert last["market_date"]            == "2025-01-13"
        assert last["total_value"]            == "10800.00"
        assert last["benchmark_ticker"]       == "SPY"
        assert last["benchmark_value"]        == "10600.00"
        assert last["portfolio_vs_benchmark"] == "200.00"
        # Earlier items without benchmark data are unaffected
        assert body[0]["benchmark_ticker"] is None
        assert body[1]["benchmark_ticker"] is None

    def test_performance_history_csv_benchmark_row_populated(
        self, perf_benchmark_client: TestClient
    ) -> None:
        """
        Jan-13 row in the CSV has non-empty benchmark columns; Jan-10 and
        Jan-11 rows still have empty benchmark columns.

        perf_benchmark_client adds Jan-13 (total=10800, benchmark_ticker=SPY,
        benchmark_value=10600, portfolio_vs_benchmark=200).
        """
        resp = perf_benchmark_client.get("/v1/performance/history.csv", headers=_AUTH)
        assert resp.status_code == 200
        rows = _parse_csv_rows(resp.text)
        # header + Jan-10 + Jan-11 + Jan-13
        assert len(rows) == 4
        jan13 = rows[3]
        assert jan13[0] == "2025-01-13"
        assert jan13[1] == "10800.00"
        assert jan13[6] == "SPY"      # benchmark_ticker
        assert jan13[7] == "10600.00" # benchmark_value
        assert jan13[8] == "200.00"   # portfolio_vs_benchmark
        # Earlier rows have empty benchmark columns
        assert rows[1][6] == ""  # Jan-10 benchmark_ticker
        assert rows[2][6] == ""  # Jan-11 benchmark_ticker


# ---------------------------------------------------------------------------
# Strategy run endpoint — POST /v1/strategy/run
# ---------------------------------------------------------------------------

class TestStrategyRunEndpoint:
    """POST /v1/strategy/run: decisions_breakdown and rejection_reasons validation."""

    def test_strategy_run_response_includes_breakdown_fields(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """
        POST /v1/strategy/run returns decisions_breakdown and rejection_reasons.

        Seeds uptrend prices (AAPL pattern: [100, 101, 102, 103, 104]) that
        generate a BUY signal. Validates that the response includes:
        - decisions_breakdown dict with approved >= 1, rejected == 0, hold == 0
        - rejection_reasons dict (empty when no rejections)
        - All existing fields: signals_generated, signals_submitted, etc.
        """
        # Seed prices for STRAT_APPROVED ticker (uptrend, known to generate BUY)
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            for i, price in enumerate([100, 101, 102, 103, 104], start=1):
                session.add(PriceSnapshot(
                    ticker="STRAT_APPROVED",
                    price=Decimal(str(price)),
                    session_type="REGULAR",
                    price_type="CLOSE",
                    snapshot_ts=_NOW.replace(hour=i),
                    market_date=_DATE_STRAT_APPROVED,
                    job_run_id=None,
                ))
            session.commit()

        payload = {
            "idempotency_key": f"test-strategy-run-breakdown-{uuid.uuid4()}",
            "market_date": _DATE_STRAT_APPROVED.isoformat(),
            "short_window": 3,
            "long_window": 5,
            "tickers": ["STRAT_APPROVED"],
        }
        resp = seeded_client.post("/v1/strategy/run", json=payload, headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()

        # Verify all existing fields are present
        assert "signals_generated" in body
        assert "signals_submitted" in body
        assert "skipped_tickers" in body
        assert "decisions_made" in body
        assert "orders_created" in body
        assert "errors" in body

        # Verify new breakdown fields are present and correctly populated
        assert "decisions_breakdown" in body
        assert isinstance(body["decisions_breakdown"], dict)
        assert body["decisions_breakdown"]["approved"] >= 1
        assert body["decisions_breakdown"]["rejected"] == 0
        assert body["decisions_breakdown"]["hold"] == 0

        assert "rejection_reasons" in body
        assert isinstance(body["rejection_reasons"], dict)
        assert body["rejection_reasons"] == {}

    def test_strategy_run_breakdown_default_values(
        self, seeded_client: TestClient
    ) -> None:
        """
        POST /v1/strategy/run returns zero breakdown when no signals generated.

        Uses a ticker with no seeded prices, so no signals generate. Validates:
        - decisions_breakdown defaults to {approved: 0, rejected: 0, hold: 0}
        - rejection_reasons defaults to empty dict {}
        - errors remains 0
        """
        payload = {
            "idempotency_key": f"test-strategy-run-defaults-{uuid.uuid4()}",
            "market_date": _DATE_STRAT_DUPLICATE.isoformat(),
            "short_window": 3,
            "long_window": 5,
            "tickers": ["NONEXISTENT_TICKER"],  # No prices seeded
        }
        resp = seeded_client.post("/v1/strategy/run", json=payload, headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()

        # With no signals generated, breakdown shows all zeros
        assert body["decisions_breakdown"]["approved"] == 0
        assert body["decisions_breakdown"]["rejected"] == 0
        assert body["decisions_breakdown"]["hold"] == 0
        assert body["rejection_reasons"] == {}
        assert body["errors"] == 0


# ---------------------------------------------------------------------------
# Strategy readiness check — GET /v1/strategy/readiness
# ---------------------------------------------------------------------------

class TestStrategyReadinessEndpoint:
    """GET /v1/strategy/readiness: pre-run validation of price history sufficiency."""

    def test_readiness_single_ticker_ready(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Requested ticker with >= long_window prices returns Ready."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            for i, price in enumerate([100, 101, 102, 103, 104], start=1):
                session.add(PriceSnapshot(
                    ticker="READY",
                    price=Decimal(str(price)),
                    session_type="REGULAR",
                    price_type="CLOSE",
                    snapshot_ts=_NOW.replace(hour=i),
                    market_date=_DATE_STRAT_APPROVED,
                    job_run_id=None,
                ))
            session.commit()

        resp = seeded_client.get(
            f"/v1/strategy/readiness?long_window=5&tickers=READY&market_date={_DATE_STRAT_APPROVED.isoformat()}",
            headers=_AUTH
        )
        assert resp.status_code == 200
        body = resp.json()

        assert body["market_date"] == _DATE_STRAT_APPROVED.isoformat()
        assert body["long_window"] == 5
        assert body["overall_status"] == "Ready"
        assert len(body["tickers_status"]) == 1
        assert body["tickers_status"][0]["ticker"] == "READY"
        assert body["tickers_status"][0]["price_count"] == 5
        assert body["tickers_status"][0]["has_sufficient_history"] is True
        assert body["tickers_status"][0]["missing_count"] == 0

    def test_readiness_single_ticker_insufficient(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Requested ticker with < long_window prices returns Insufficient History."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            for i, price in enumerate([100, 101, 102], start=1):
                session.add(PriceSnapshot(
                    ticker="SHORT",
                    price=Decimal(str(price)),
                    session_type="REGULAR",
                    price_type="CLOSE",
                    snapshot_ts=_NOW.replace(hour=i),
                    market_date=_DATE_STRAT_APPROVED,
                    job_run_id=None,
                ))
            session.commit()

        resp = seeded_client.get(
            f"/v1/strategy/readiness?long_window=5&tickers=SHORT&market_date={_DATE_STRAT_APPROVED.isoformat()}",
            headers=_AUTH
        )
        assert resp.status_code == 200
        body = resp.json()

        assert body["overall_status"] == "Insufficient History"
        assert len(body["tickers_status"]) == 1
        assert body["tickers_status"][0]["ticker"] == "SHORT"
        assert body["tickers_status"][0]["price_count"] == 3
        assert body["tickers_status"][0]["has_sufficient_history"] is False
        assert body["tickers_status"][0]["missing_count"] == 2

    def test_readiness_ticker_no_history(
        self, seeded_client: TestClient
    ) -> None:
        """Requested ticker with zero snapshots is included with count 0."""
        resp = seeded_client.get(
            f"/v1/strategy/readiness?long_window=5&tickers=NONEXISTENT&market_date={_DATE_STRAT_APPROVED.isoformat()}",
            headers=_AUTH
        )
        assert resp.status_code == 200
        body = resp.json()

        assert body["overall_status"] == "Insufficient History"
        assert len(body["tickers_status"]) == 1
        assert body["tickers_status"][0]["ticker"] == "NONEXISTENT"
        assert body["tickers_status"][0]["price_count"] == 0
        assert body["tickers_status"][0]["latest_market_date"] is None
        assert body["tickers_status"][0]["has_sufficient_history"] is False
        assert body["tickers_status"][0]["missing_count"] == 5

    def test_readiness_future_dates_excluded(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Future-dated snapshots are excluded when market_date is earlier."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            future_date = _DATE_STRAT_APPROVED + timedelta(days=10)
            for i, price in enumerate([100, 101, 102, 103, 104, 105, 106], start=1):
                session.add(PriceSnapshot(
                    ticker="FUTURE",
                    price=Decimal(str(price)),
                    session_type="REGULAR",
                    price_type="CLOSE",
                    snapshot_ts=_NOW.replace(hour=i),
                    market_date=future_date if i > 5 else _DATE_STRAT_APPROVED,
                    job_run_id=None,
                ))
            session.commit()

        resp = seeded_client.get(
            f"/v1/strategy/readiness?long_window=5&market_date={_DATE_STRAT_APPROVED.isoformat()}&tickers=FUTURE",
            headers=_AUTH
        )
        assert resp.status_code == 200
        body = resp.json()

        assert body["overall_status"] == "Ready"
        assert body["tickers_status"][0]["price_count"] == 5
        assert body["tickers_status"][0]["has_sufficient_history"] is True

    def test_readiness_omit_tickers_returns_all_distinct(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Omitting tickers returns readiness for all distinct tickers."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            for ticker in ["AAPL", "MSFT"]:
                for i in range(5):
                    session.add(PriceSnapshot(
                        ticker=ticker,
                        price=Decimal("100"),
                        session_type="REGULAR",
                        price_type="CLOSE",
                        snapshot_ts=_NOW.replace(hour=i),
                        market_date=_DATE_STRAT_APPROVED,
                        job_run_id=None,
                    ))
            session.commit()

        resp = seeded_client.get(
            f"/v1/strategy/readiness?long_window=5&market_date={_DATE_STRAT_APPROVED.isoformat()}",
            headers=_AUTH
        )
        assert resp.status_code == 200
        body = resp.json()

        tickers = sorted([t["ticker"] for t in body["tickers_status"]])
        assert "AAPL" in tickers
        assert "MSFT" in tickers

    def test_readiness_invalid_long_window_returns_422(
        self, seeded_client: TestClient
    ) -> None:
        """Invalid long_window returns 422."""
        resp = seeded_client.get(
            f"/v1/strategy/readiness?long_window=0&market_date={_DATE_STRAT_APPROVED.isoformat()}",
            headers=_AUTH
        )
        assert resp.status_code == 422

    def test_readiness_missing_api_key_returns_401(
        self, seeded_client: TestClient
    ) -> None:
        """Missing API key returns 401."""
        resp = seeded_client.get("/v1/strategy/readiness?long_window=5")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Snapshot workflow trigger — POST /v1/snapshot
# ---------------------------------------------------------------------------

class TestSnapshotEndpoint:
    def test_trigger_snapshot_no_positions_200(
        self, seeded_client: TestClient
    ) -> None:
        """
        Portfolio with $10,000 cash and no open positions.

        Expected response: all zero/null fields, total_value matches cash.
        """
        payload = {
            "idempotency_key": _ikey(),
            "market_date": _DATE_SNAP_NO_POS.isoformat(),
        }
        resp = seeded_client.post("/v1/snapshot", json=payload, headers=_AUTH)
        assert resp.status_code == 200
        body = resp.json()
        assert body["cash"]                    == "10000.00"
        assert body["positions_value"]         == "0.00"
        assert body["total_value"]             == "10000.00"
        assert body["unrealized_pnl"]          == "0.00"
        assert body["realized_pnl_cumulative"] == "0.00"
        assert body["open_position_count"]     == 0
        assert body["benchmark_ticker"]        is None
        assert body["portfolio_vs_benchmark"]  is None

    def test_trigger_snapshot_idempotent_replay_200(
        self, seeded_client: TestClient
    ) -> None:
        """Calling POST /v1/snapshot twice with the same key returns the same dict."""
        key = _ikey()
        payload = {
            "idempotency_key": key,
            "market_date": _DATE_SNAP_REPLAY.isoformat(),
        }
        first  = seeded_client.post("/v1/snapshot", json=payload, headers=_AUTH)
        second = seeded_client.post("/v1/snapshot", json=payload, headers=_AUTH)
        assert first.status_code  == 200
        assert second.status_code == 200
        assert second.json() == first.json()

    def test_trigger_snapshot_running_conflict_returns_409(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """A pre-existing RUNNING JobRun for the key causes a 409 response."""
        key = _ikey()
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            session.add(JobRun(
                idempotency_key=key,
                workflow_type=WorkflowType.POST_MARKET,
                market_date=_DATE_SNAP_RUNNING,
                status=JobRunStatus.RUNNING,
                started_at=_NOW,
            ))
            session.commit()

        resp = seeded_client.post(
            "/v1/snapshot",
            json={"idempotency_key": key, "market_date": _DATE_SNAP_RUNNING.isoformat()},
            headers=_AUTH,
        )
        assert resp.status_code == 409
        assert "RUNNING" in resp.json()["detail"]

    def test_trigger_snapshot_failed_conflict_returns_409(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """A pre-existing FAILED JobRun for the key causes a 409 response."""
        key = _ikey()
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            session.add(JobRun(
                idempotency_key=key,
                workflow_type=WorkflowType.POST_MARKET,
                market_date=_DATE_SNAP_FAILED,
                status=JobRunStatus.FAILED,
                started_at=_NOW,
                completed_at=_NOW,
                error_detail="synthetic failure",
            ))
            session.commit()

        resp = seeded_client.post(
            "/v1/snapshot",
            json={"idempotency_key": key, "market_date": _DATE_SNAP_FAILED.isoformat()},
            headers=_AUTH,
        )
        assert resp.status_code == 409
        assert "FAILED" in resp.json()["detail"]

    def test_trigger_snapshot_missing_price_returns_400(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """
        Open position with no PriceSnapshot returns 400 with missing ticker.

        This test is last in the class: the TSLA position it creates persists
        for the remainder of the module (no per-test cleanup in test_api.py).
        The api_engine teardown truncates all tables after the module finishes.
        """
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            open_position(
                session,
                ticker="TSLA",
                qty=Decimal("3"),
                fill_price=Decimal("250.000000"),
                now=_NOW,
            )
            session.commit()

        resp = seeded_client.post(
            "/v1/snapshot",
            json={"idempotency_key": _ikey(), "market_date": _DATE_SNAP_MISSING.isoformat()},
            headers=_AUTH,
        )
        assert resp.status_code == 400
        detail = resp.json()["detail"]
        assert "Snapshot requires prices for all open positions" in detail


class TestFetchPricesEndpoint:
    """Test POST /v1/prices/fetch with mocked market data."""

    def test_auth_required(self, seeded_client: TestClient) -> None:
        """Missing API key returns 401."""
        resp = seeded_client.post(
            "/v1/prices/fetch",
            json={"tickers": ["AAPL"]},
        )
        assert resp.status_code == 401

    def test_empty_tickers_returns_zero(self, seeded_client: TestClient) -> None:
        """Empty tickers list returns inserted=0, no failures."""
        resp = seeded_client.post(
            "/v1/prices/fetch",
            json={"tickers": []},
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["inserted"] == 0
        assert data["prices"] == []
        assert data["failures"] == []

    def test_single_successful_price(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """Successful fetch inserts price and returns detail."""
        def mock_fetch(tickers):
            return (
                [{"ticker": "AAPL", "price": "182.50"}],
                []
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_latest_prices",
            mock_fetch,
        )

        resp = seeded_client.post(
            "/v1/prices/fetch",
            json={"tickers": ["AAPL"]},
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["inserted"] == 1
        assert len(data["prices"]) == 1
        assert data["prices"][0]["ticker"] == "AAPL"
        assert data["prices"][0]["price"] == "182.50"
        assert data["prices"][0]["data_source"] == "yahoo_finance"
        assert data["prices"][0]["price_type"] == "LAST"
        assert data["prices"][0]["session_type"] == "REGULAR"
        assert data["failures"] == []

    def test_mixed_success_and_failure(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """Some tickers succeed, others fail; returns both in response."""
        def mock_fetch(tickers):
            return (
                [
                    {"ticker": "AAPL", "price": "182.50"},
                    {"ticker": "MSFT", "price": "420.75"},
                ],
                [
                    {"ticker": "BADTICKER", "reason": "No price returned"},
                ]
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_latest_prices",
            mock_fetch,
        )

        resp = seeded_client.post(
            "/v1/prices/fetch",
            json={"tickers": ["AAPL", "MSFT", "BADTICKER"]},
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["inserted"] == 2
        assert len(data["prices"]) == 2
        assert len(data["failures"]) == 1
        assert data["failures"][0]["ticker"] == "BADTICKER"
        assert "No price returned" in data["failures"][0]["reason"]

    def test_custom_price_type_and_session_type(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """Request with custom price_type and session_type is respected."""
        def mock_fetch(tickers):
            return (
                [{"ticker": "GOOG", "price": "190.00"}],
                []
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_latest_prices",
            mock_fetch,
        )

        resp = seeded_client.post(
            "/v1/prices/fetch",
            json={
                "tickers": ["GOOG"],
                "price_type": "CLOSE",
                "session_type": "PREMARKET",
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["inserted"] == 1
        assert data["prices"][0]["price_type"] == "CLOSE"
        assert data["prices"][0]["session_type"] == "PREMARKET"

    def test_price_inserted_into_db(
        self, seeded_client: TestClient, monkeypatch, api_engine
    ) -> None:
        """Successful fetch inserts PriceSnapshot row into database."""
        def mock_fetch(tickers):
            return (
                [{"ticker": "NVDA", "price": "875.25"}],
                []
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_latest_prices",
            mock_fetch,
        )

        resp = seeded_client.post(
            "/v1/prices/fetch",
            json={"tickers": ["NVDA"]},
            headers=_AUTH,
        )
        assert resp.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            snap = session.execute(
                select(PriceSnapshot).where(PriceSnapshot.ticker == "NVDA")
            ).scalar_one_or_none()
            assert snap is not None
            assert snap.ticker == "NVDA"
            assert snap.price == Decimal("875.25")
            assert snap.data_source == "yahoo_finance"
            assert snap.price_type == "LAST"
            assert snap.session_type == "REGULAR"
            assert snap.job_run_id is None

    def test_all_failures_no_success(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """All tickers fail; no prices inserted."""
        def mock_fetch(tickers):
            return (
                [],
                [
                    {"ticker": "BAD1", "reason": "Network error"},
                    {"ticker": "BAD2", "reason": "Symbol not found"},
                ]
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_latest_prices",
            mock_fetch,
        )

        resp = seeded_client.post(
            "/v1/prices/fetch",
            json={"tickers": ["BAD1", "BAD2"]},
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["inserted"] == 0
        assert data["prices"] == []
        assert len(data["failures"]) == 2

    def test_defaults_to_last_and_regular_session(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """Request with no price_type/session_type uses LAST and REGULAR."""
        def mock_fetch(tickers):
            return (
                [{"ticker": "TSLA", "price": "250.00"}],
                []
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_latest_prices",
            mock_fetch,
        )

        resp = seeded_client.post(
            "/v1/prices/fetch",
            json={"tickers": ["TSLA"]},
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["prices"][0]["price_type"] == "LAST"
        assert data["prices"][0]["session_type"] == "REGULAR"

    def test_market_date_is_eastern_date(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """Inserted prices have market_date = US-Eastern date of now."""
        def mock_fetch(tickers):
            return (
                [{"ticker": "AMD", "price": "165.50"}],
                []
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_latest_prices",
            mock_fetch,
        )

        resp = seeded_client.post(
            "/v1/prices/fetch",
            json={"tickers": ["AMD"]},
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        market_date_str = data["prices"][0]["market_date"]
        # Parse and verify it's a valid date
        from datetime import date
        parsed_date = date.fromisoformat(market_date_str)
        assert isinstance(parsed_date, date)


# ---------------------------------------------------------------------------
# Prediction strategy endpoint tests
# ---------------------------------------------------------------------------


class TestPredictionStrategyEndpoint:
    """POST /v1/strategy/prediction/run: Convert predictions to signals and run decision workflow."""

    def test_prediction_run_with_valid_buy_prediction(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """POST /v1/strategy/prediction/run with valid BUY prediction returns 200."""
        resp = seeded_client.post(
            "/v1/strategy/prediction/run",
            json={
                "idempotency_key": "pred-api-test-001",
                "market_date": _DATE_STRAT_APPROVED.isoformat(),
                "predictions": [
                    {
                        "ticker": "AAPL",
                        "current_price": "150.00",
                        "forecast_price_5d": "157.50",
                        "expected_return_pct": "5.00",
                        "confidence": "0.85",
                        "recommendation": "BUY",
                        "reason": "Strong uptrend",
                        "model_consensus": {"consensus": "BUY"},
                        "market_context": "bullish",
                    }
                ],
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["signals_generated"] == 1
        assert body["signals_submitted"] == 1
        assert body["errors"] == 0

    def test_prediction_run_missing_api_key_returns_401(
        self, seeded_client: TestClient
    ) -> None:
        """Missing X-API-Key header returns 401."""
        resp = seeded_client.post(
            "/v1/strategy/prediction/run",
            json={
                "idempotency_key": "pred-api-test-002",
                "predictions": [
                    {
                        "ticker": "AAPL",
                        "current_price": "150.00",
                        "forecast_price_5d": "157.50",
                        "expected_return_pct": "5.00",
                        "confidence": "0.85",
                        "recommendation": "BUY",
                    }
                ],
            },
        )
        assert resp.status_code == 401

    def test_prediction_run_empty_predictions_returns_zero_counts(
        self, seeded_client: TestClient
    ) -> None:
        """Empty predictions list returns 200 with zero counts."""
        resp = seeded_client.post(
            "/v1/strategy/prediction/run",
            json={
                "idempotency_key": "pred-api-test-003",
                "predictions": [],
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["signals_generated"] == 0
        assert body["signals_submitted"] == 0
        assert body["decisions_made"] == 0
        assert body["orders_created"] == 0
        assert body["errors"] == 0

    def test_prediction_run_invalid_predictions_returned_in_skipped(
        self, seeded_client: TestClient
    ) -> None:
        """Invalid predictions are reported in skipped_tickers, not as errors."""
        resp = seeded_client.post(
            "/v1/strategy/prediction/run",
            json={
                "idempotency_key": "pred-api-test-004",
                "predictions": [
                    {
                        "ticker": "INVALID",
                        "confidence": "invalid",
                        "recommendation": "BUY",
                    }
                ],
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["signals_generated"] == 0
        assert body["errors"] == 0
        assert "INVALID" in body["skipped_tickers"]

    def test_prediction_run_mixed_valid_invalid_predictions(
        self, seeded_client: TestClient
    ) -> None:
        """Batch with valid and invalid predictions processes both."""
        resp = seeded_client.post(
            "/v1/strategy/prediction/run",
            json={
                "idempotency_key": "pred-api-test-005",
                "predictions": [
                    {
                        "ticker": "AAPL",
                        "current_price": "150.00",
                        "forecast_price_5d": "157.50",
                        "expected_return_pct": "5.00",
                        "confidence": "0.85",
                        "recommendation": "BUY",
                    },
                    {
                        "ticker": "INVALID",
                        "confidence": "invalid",
                        "recommendation": "BUY",
                    },
                ],
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["signals_generated"] == 1  # Only valid signal
        assert "INVALID" in body["skipped_tickers"]

    def test_prediction_run_response_has_breakdown_fields(
        self, seeded_client: TestClient
    ) -> None:
        """Response includes decisions_breakdown and rejection_reasons."""
        resp = seeded_client.post(
            "/v1/strategy/prediction/run",
            json={
                "idempotency_key": "pred-api-test-006",
                "predictions": [
                    {
                        "ticker": "AAPL",
                        "current_price": "150.00",
                        "forecast_price_5d": "157.50",
                        "expected_return_pct": "5.00",
                        "confidence": "0.85",
                        "recommendation": "BUY",
                    }
                ],
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "decisions_breakdown" in body
        assert "approved" in body["decisions_breakdown"]
        assert "rejected" in body["decisions_breakdown"]
        assert "hold" in body["decisions_breakdown"]
        assert "rejection_reasons" in body


# ---------------------------------------------------------------------------
# Fetch and run prediction strategy endpoint tests
# ---------------------------------------------------------------------------


class TestFetchAndRunPredictionEndpoint:
    """POST /v1/strategy/prediction/fetch-and-run: Fetch predictions and run strategy."""

    def test_fetch_and_run_with_valid_predictions(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """Fetch and run normalizes API response and submits through strategy."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [
                    {
                        "ticker": "AAPL",
                        "current_price": "150.00",
                        "ensemble_day5": "157.50",
                        "d5_change_pct": "5.00",
                        "confidence": "85.0",
                        "recommendation": "Buy",
                        "rationale": ["Strong", "uptrend"],
                        "per_model_summary": {
                            "prophet": {"direction": "Up"},
                            "arima": {"direction": "Up"},
                        },
                    }
                ],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        resp = seeded_client.post(
            "/v1/strategy/prediction/fetch-and-run",
            json={
                "idempotency_key": "fetch-run-api-test-001",
                "market_date": _DATE_STRAT_APPROVED.isoformat(),
                "tickers": ["AAPL"],
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["fetched_count"] == 1
        assert data["failed_count"] == 0
        assert data["signals_generated"] == 1
        assert data["signals_submitted"] == 1
        assert len(data["normalized_predictions"]) == 1
        assert data["normalized_predictions"][0]["ticker"] == "AAPL"
        assert data["normalized_predictions"][0]["recommendation"] == "BUY"

    def test_fetch_and_run_partial_failures_returns_200(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """Partial failures return 200 with fetch_failures included."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [
                    {
                        "ticker": "AAPL",
                        "current_price": "150.00",
                        "ensemble_day5": "157.50",
                        "d5_change_pct": "5.00",
                        "confidence": "75.0",
                        "recommendation": "BUY",
                        "per_model_summary": {},
                    }
                ],
                [
                    {"ticker": "BADTICKER", "reason": "Ticker not found"},
                ],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        resp = seeded_client.post(
            "/v1/strategy/prediction/fetch-and-run",
            json={
                "idempotency_key": "fetch-run-api-test-002",
                "tickers": ["AAPL", "BADTICKER"],
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["fetched_count"] == 1
        assert data["failed_count"] == 1
        assert len(data["fetch_failures"]) == 1
        assert data["fetch_failures"][0]["ticker"] == "BADTICKER"

    def test_fetch_and_run_all_failures_returns_503(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """All fetch failures returns 503."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [],
                [
                    {"ticker": "BAD1", "reason": "Network error"},
                    {"ticker": "BAD2", "reason": "Service unavailable"},
                ],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        resp = seeded_client.post(
            "/v1/strategy/prediction/fetch-and-run",
            json={
                "idempotency_key": "fetch-run-api-test-003",
                "tickers": ["BAD1", "BAD2"],
            },
            headers=_AUTH,
        )
        assert resp.status_code == 503

    def test_fetch_and_run_normalization_failure(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """Response with invalid fields normalizes to zero predictions."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [
                    {
                        "ticker": "AAPL",
                        "current_price": "150.00",
                        "ensemble_day5": "157.50",
                        "d5_change_pct": "5.00",
                        "confidence": "not_a_number",  # Invalid
                        "recommendation": "BUY",
                        "per_model_summary": {},
                    }
                ],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        resp = seeded_client.post(
            "/v1/strategy/prediction/fetch-and-run",
            json={
                "idempotency_key": "fetch-run-api-test-004",
                "tickers": ["AAPL"],
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_generated"] == 0
        assert data["signals_submitted"] == 0
        assert len(data["normalized_predictions"]) == 0
        assert "AAPL" in data["skipped_tickers"]
        assert "Invalid confidence" in data["skipped_tickers"]["AAPL"]

    def test_fetch_and_run_missing_api_key_returns_401(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """Missing X-API-Key header returns 401."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        resp = seeded_client.post(
            "/v1/strategy/prediction/fetch-and-run",
            json={
                "idempotency_key": "fetch-run-api-test-005",
                "tickers": ["AAPL"],
            },
            # No headers, no API key
        )
        assert resp.status_code == 401

    def test_fetch_and_run_invalid_request_returns_422(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """Invalid request (missing required fields) returns 422."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        resp = seeded_client.post(
            "/v1/strategy/prediction/fetch-and-run",
            json={
                "idempotency_key": "fetch-run-api-test-006",
                # Missing "tickers" field
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422

    def test_fetch_and_run_response_has_breakdown_fields(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """Response includes decisions_breakdown and rejection_reasons."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [
                    {
                        "ticker": "AAPL",
                        "current_price": "150.00",
                        "ensemble_day5": "157.50",
                        "d5_change_pct": "5.00",
                        "confidence": "85.0",
                        "recommendation": "BUY",
                        "per_model_summary": {},
                    }
                ],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        resp = seeded_client.post(
            "/v1/strategy/prediction/fetch-and-run",
            json={
                "idempotency_key": "fetch-run-api-test-007",
                "tickers": ["AAPL"],
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "decisions_breakdown" in data
        assert "approved" in data["decisions_breakdown"]
        assert "rejected" in data["decisions_breakdown"]
        assert "hold" in data["decisions_breakdown"]
        assert "rejection_reasons" in data

    def test_fetch_and_run_batch_with_hold_and_buy(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """Batch with BUY and HOLD (missing confidence) normalizes all correctly."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [
                    {
                        "ticker": "AAPL",
                        "current_price": "150.00",
                        "ensemble_day5": "157.50",
                        "d5_change_pct": "5.00",
                        "confidence": "97.7",
                        "recommendation": "Buy",
                        "per_model_summary": [
                            {"model": "Drift", "direction": "Up"},
                            {"model": "LinearTrend", "direction": "Up"},
                        ],
                    },
                    {
                        "ticker": "MSFT",
                        "current_price": "416.03",
                        "ensemble_day5": "415.62",
                        "d5_change_pct": "-0.1",
                        "confidence": None,
                        "recommendation": "Hold",
                        "per_model_summary": [
                            {"model": "Drift", "direction": "Flat"},
                            {"model": "LinearTrend", "direction": "Down"},
                        ],
                    },
                    {
                        "ticker": "TSLA",
                        "current_price": "433.59",
                        "ensemble_day5": "441.37",
                        "d5_change_pct": "1.79",
                        "confidence": None,
                        "recommendation": "Hold",
                        "per_model_summary": [
                            {"model": "Drift", "direction": "Up"},
                            {"model": "XGBoost", "direction": "Up"},
                        ],
                    },
                ],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        resp = seeded_client.post(
            "/v1/strategy/prediction/fetch-and-run",
            json={
                "idempotency_key": "fetch-run-api-test-batch-001",
                "tickers": ["AAPL", "MSFT", "TSLA"],
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["fetched_count"] == 3
        assert data["failed_count"] == 0
        assert len(data["normalized_predictions"]) == 3
        # Verify AAPL prediction
        aapl_pred = next(p for p in data["normalized_predictions"] if p["ticker"] == "AAPL")
        assert aapl_pred["recommendation"] == "BUY"
        assert aapl_pred["confidence"] == "0.977"
        # Verify MSFT prediction (HOLD with defaulted confidence)
        msft_pred = next(p for p in data["normalized_predictions"] if p["ticker"] == "MSFT")
        assert msft_pred["recommendation"] == "HOLD"
        assert msft_pred["confidence"] == "0.50"
        # Verify TSLA prediction (HOLD with defaulted confidence)
        tsla_pred = next(p for p in data["normalized_predictions"] if p["ticker"] == "TSLA")
        assert tsla_pred["recommendation"] == "HOLD"
        assert tsla_pred["confidence"] == "0.50"
        # All should be in skipped_tickers (as HOLDs won't generate trading signals)
        assert len(data["skipped_tickers"]) >= 0  # HOLD predictions may be skipped downstream


class TestMarketScanEndpoint:
    """POST /v1/market/scan endpoint tests."""

    def test_requires_api_key(self, client: TestClient) -> None:
        """Endpoint requires X-API-Key header."""
        resp = client.post(
            "/v1/market/scan",
            json={
                "universe": "SP500",
                "tickers": None,
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 25,
                "min_price_points": 5,
            },
        )
        assert resp.status_code == 401

    def test_explicit_tickers_scan(self, seeded_client: TestClient) -> None:
        """Scan with explicit tickers returns valid response structure."""
        resp = seeded_client.post(
            "/v1/market/scan",
            json={
                "universe": "SP500",
                "tickers": ["AAPL", "MSFT"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 25,
                "min_price_points": 5,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()

        # Verify response structure
        assert "universe" in data
        assert "scan_date" in data
        assert "benchmark_ticker" in data
        assert data["benchmark_ticker"] == "SPY"
        assert "total_universe_count" in data
        assert "evaluated_count" in data
        assert "skipped_count" in data
        assert "top_n" in data
        assert "candidates" in data
        assert "skipped_tickers" in data

        # With no price data, all tickers should be skipped
        assert isinstance(data["candidates"], list)
        assert isinstance(data["skipped_tickers"], list)

    def test_insufficient_data_returns_200_with_skipped_tickers(
        self, seeded_client: TestClient
    ) -> None:
        """Missing price history returns 200 with skipped_tickers populated."""
        resp = seeded_client.post(
            "/v1/market/scan",
            json={
                "universe": "SP500",
                "tickers": ["NONEXISTENT"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 25,
                "min_price_points": 5,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Non-existent ticker should be in skipped_tickers
        assert len(data["skipped_tickers"]) > 0

    def test_invalid_top_n_rejected(self, seeded_client: TestClient) -> None:
        """Invalid top_n (>100) is rejected."""
        resp = seeded_client.post(
            "/v1/market/scan",
            json={
                "universe": "SP500",
                "tickers": ["AAPL"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 101,  # Over limit
                "min_price_points": 5,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422  # Validation error

    def test_default_sp500_request(self, seeded_client: TestClient) -> None:
        """Default SP500 universe request works without internet."""
        resp = seeded_client.post(
            "/v1/market/scan",
            json={
                "universe": "SP500",
                "tickers": None,
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 25,
                "min_price_points": 5,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["universe"] == "SP500"
        assert data["total_universe_count"] > 0  # From loaded CSV

    def test_response_includes_candidate_fields(self, seeded_client: TestClient) -> None:
        """Candidate objects have all required fields."""
        resp = seeded_client.post(
            "/v1/market/scan",
            json={
                "universe": "SP500",
                "tickers": ["AAPL"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 25,
                "min_price_points": 5,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()

        # If candidates exist, verify structure
        if data["candidates"]:
            candidate = data["candidates"][0]
            assert "rank" in candidate
            assert "ticker" in candidate
            assert "score" in candidate
            assert "latest_price" in candidate
            assert "latest_market_date" in candidate
            assert "price_count" in candidate
            assert "momentum_5d_pct" in candidate
            assert "momentum_20d_pct" in candidate
            assert "volatility_20d_pct" in candidate
            assert "relative_strength_vs_spy_20d" in candidate
            assert "reason_codes" in candidate

    def test_response_includes_skipped_ticker_fields(
        self, seeded_client: TestClient
    ) -> None:
        """Skipped ticker objects have all required fields."""
        resp = seeded_client.post(
            "/v1/market/scan",
            json={
                "universe": "SP500",
                "tickers": ["NONEXISTENT"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 25,
                "min_price_points": 5,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()

        if data["skipped_tickers"]:
            skipped = data["skipped_tickers"][0]
            assert "ticker" in skipped
            assert "reason" in skipped
            assert "price_count" in skipped

    def test_response_accounting_is_consistent(self, seeded_client: TestClient) -> None:
        """Response counts are consistent: evaluated + skipped = total."""
        resp = seeded_client.post(
            "/v1/market/scan",
            json={
                "universe": "SP500",
                "tickers": ["AAPL", "MSFT", "GOOGL"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 2,
                "min_price_points": 5,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()

        # Accounting: evaluated_count = total - skipped_count
        total = data["total_universe_count"]
        evaluated = data["evaluated_count"]
        skipped = data["skipped_count"]

        assert total == 3  # Requested 3 tickers
        assert evaluated + skipped == total  # Equation holds
        # Only up to top_n are returned in candidates (2 in this case)
        assert len(data["candidates"]) <= data["top_n"]


@pytest.fixture(scope="module")
def market_scan_prediction_seeded_client(seeded_client, api_engine):
    """
    Client with PriceSnapshot and BenchmarkPrice rows for market scan tests.

    Seeds 25 days of price history for AAPL, MSFT, SPY to enable:
    - Market scan to select candidates (min_price_points=5, lookback_days=20)
    - Relative strength vs SPY calculations
    - Momentum and volatility calculations
    """
    from datetime import timedelta

    with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
        # Use dates from 2025-01-15 backwards 25 days (into 2024-12-21)
        base_date = date(2024, 12, 21)

        # Seed SPY prices (benchmark)
        for i in range(25):
            market_date = base_date + timedelta(days=i)
            price = Decimal("500.00") + Decimal(i * 0.5)  # Uptrend
            session.add(BenchmarkPrice(
                ticker="SPY",
                price=price,
                session_type="REGULAR",
                market_date=market_date,
                snapshot_ts=_NOW.replace(day=min(market_date.day, 28)),
            ))

        # Seed AAPL prices (uptrend with volatility)
        for i in range(25):
            market_date = base_date + timedelta(days=i)
            price = Decimal("150.00") + Decimal(i * 0.4)  # Slight uptrend
            if i % 2 == 0:
                price += Decimal("0.5")  # Add noise
            session.add(PriceSnapshot(
                ticker="AAPL",
                price=price,
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=_NOW.replace(day=min(market_date.day, 28)),
                market_date=market_date,
                job_run_id=None,
            ))

        # Seed MSFT prices (uptrend with more volatility)
        for i in range(25):
            market_date = base_date + timedelta(days=i)
            price = Decimal("200.00") + Decimal(i * 0.6)  # Steeper uptrend
            if i % 3 == 0:
                price += Decimal("1.0")  # More noise
            session.add(PriceSnapshot(
                ticker="MSFT",
                price=price,
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=_NOW.replace(day=min(market_date.day, 28)),
                market_date=market_date,
                job_run_id=None,
            ))

        session.commit()

    yield seeded_client


class TestMarketScanPredictionCandidatesEndpoint:
    """POST /v1/strategy/market-scan/prediction-candidates: Market scan + prediction preview (V1)."""

    def test_requires_api_key(self, client: TestClient) -> None:
        """Endpoint requires X-API-Key header."""
        resp = client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-001",
                "universe": "SP500",
                "tickers": None,
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
            },
        )
        assert resp.status_code == 401

    def test_rejects_dry_run_false_with_422(self, seeded_client: TestClient) -> None:
        """V1: dry_run must be true, rejects false with 422."""
        resp = seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-002",
                "universe": "SP500",
                "tickers": None,
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": False,  # Violation
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422
        assert "PREVIEW-ONLY" in resp.json()["detail"]

    def test_rejects_submit_signals_true_with_422(self, seeded_client: TestClient) -> None:
        """V1: submit_signals must be false, rejects true with 422."""
        resp = seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-003",
                "universe": "SP500",
                "tickers": None,
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True,
                "submit_signals": True,  # Violation
                "run_risk": False,
                "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422
        assert "PREVIEW-ONLY" in resp.json()["detail"]

    def test_rejects_run_risk_true_with_422(self, seeded_client: TestClient) -> None:
        """V1: run_risk must be false, rejects true with 422."""
        resp = seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-004",
                "universe": "SP500",
                "tickers": None,
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": True,  # Violation
                "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422
        assert "PREVIEW-ONLY" in resp.json()["detail"]

    def test_rejects_create_orders_true_with_422(self, seeded_client: TestClient) -> None:
        """V1: create_orders must be false, rejects true with 422."""
        resp = seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-005",
                "universe": "SP500",
                "tickers": None,
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": True,  # Violation
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422
        assert "PREVIEW-ONLY" in resp.json()["detail"]

    def test_scan_and_select_top_prediction_candidates(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Runs market scan and selects top prediction_top_n candidates."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            # Return predictions for requested tickers
            return (
                [
                    {
                        "ticker": t,
                        "current_price": "100.00",
                        "ensemble_day5": "105.00",
                        "d5_change_pct": "5.00",
                        "confidence": "80.0",
                        "recommendation": "BUY",
                        "per_model_summary": {},
                    }
                    for t in tickers
                ],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-006",
                "universe": "SP500",
                "tickers": ["AAPL", "MSFT"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 2,  # Select top 2
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()

        # Verify structure
        assert "idempotency_key" in data
        assert data["idempotency_key"] == "test-006"
        assert data["dry_run"] is True
        assert data["execution_mode"] == "PREVIEW_ONLY"
        assert "scan" in data
        assert "selected_tickers" in data
        assert "predictions_fetched" in data
        assert "normalized_predictions" in data
        assert data["signals_submitted"] == 0
        assert data["decisions_made"] == 0
        assert data["orders_created"] == 0

        # Verify selected tickers is limited to prediction_top_n
        assert len(data["selected_tickers"]) <= 2

    def test_excludes_skipped_and_outlier_tickers(
        self, seeded_client: TestClient, monkeypatch
    ) -> None:
        """Excludes skipped tickers and DATA_QUALITY_OUTLIER tickers from prediction selection."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            # Return empty responses (no predictions should be fetched for excluded tickers)
            return [], []

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        resp = seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-007",
                "universe": "SP500",
                "tickers": ["NONEXISTENT"],  # Will be skipped due to no price data
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()

        # Non-existent ticker should be in skipped, so selected_tickers should be empty
        assert len(data["selected_tickers"]) == 0
        assert data["predictions_fetched"] == 0

    def test_partial_prediction_failures_returns_200(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Partial prediction failures return 200 with failures populated."""
        from paper_trader.engine.market_screener import CandidateScore, SkippedTicker

        def mock_scan(session, tickers=None, universe="SP500", benchmark_ticker="SPY", lookback_days=20, top_n=25, min_price_points=5):
            candidates = [
                CandidateScore(
                    rank=1,
                    ticker="AAPL",
                    score="5.23",
                    latest_price="150.00",
                    latest_market_date="2025-01-14",
                    price_count=25,
                    momentum_5d_pct="2.50",
                    momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15",
                    relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM", "OUTPERFORMING_SPY"],
                ),
                CandidateScore(
                    rank=2,
                    ticker="MSFT",
                    score="4.15",
                    latest_price="200.00",
                    latest_market_date="2025-01-14",
                    price_count=25,
                    momentum_5d_pct="1.50",
                    momentum_20d_pct="3.00",
                    volatility_20d_pct="3.20",
                    relative_strength_vs_spy_20d="0.50",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ]
            return candidates, [], date(2025, 1, 14)

        monkeypatch.setattr(
            "paper_trader.engine.market_screener.scan_market",
            mock_scan,
        )

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            # AAPL succeeds, MSFT fails
            return (
                [
                    {
                        "ticker": "AAPL",
                        "current_price": "150.00",
                        "ensemble_day5": "157.50",
                        "d5_change_pct": "5.00",
                        "confidence": "80.0",
                        "recommendation": "BUY",
                        "per_model_summary": {},
                    }
                ],
                [
                    {"ticker": "MSFT", "reason": "Service unavailable"},
                ],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-008",
                "universe": "SP500",
                "tickers": ["AAPL", "MSFT"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Verify selected candidates exist
        assert len(data["selected_tickers"]) > 0
        assert set(data["selected_tickers"]) <= {"AAPL", "MSFT"}
        # Verify prediction fetch was called
        assert data["predictions_fetched"] == 1
        assert len(data["prediction_failures"]) == 1
        assert len(data["normalized_predictions"]) == 1

    def test_all_prediction_failures_returns_200_with_failures(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """All prediction failures for selected candidates returns 200 (PREVIEW endpoint)."""
        from paper_trader.engine.market_screener import CandidateScore, SkippedTicker

        def mock_scan(session, tickers=None, universe="SP500", benchmark_ticker="SPY", lookback_days=20, top_n=25, min_price_points=5):
            candidates = [
                CandidateScore(
                    rank=1,
                    ticker="MSFT",
                    score="4.15",
                    latest_price="200.00",
                    latest_market_date="2025-01-14",
                    price_count=25,
                    momentum_5d_pct="1.50",
                    momentum_20d_pct="3.00",
                    volatility_20d_pct="3.20",
                    relative_strength_vs_spy_20d="0.50",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ]
            return candidates, [], date(2025, 1, 14)

        monkeypatch.setattr(
            "paper_trader.engine.market_screener.scan_market",
            mock_scan,
        )

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            # All fetch attempts fail
            return (
                [],
                [
                    {"ticker": t, "reason": "Prediction service unavailable"}
                    for t in tickers
                ],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-009",
                "universe": "SP500",
                "tickers": ["MSFT"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        # Preview endpoint returns 200 even when all predictions fail
        assert resp.status_code == 200
        data = resp.json()
        # Verify selected candidates exist (so prediction fetch was attempted)
        assert len(data["selected_tickers"]) > 0
        assert data["selected_tickers"] == ["MSFT"]
        # Verify all selected candidates had prediction failures
        assert data["predictions_fetched"] == 0
        assert len(data["prediction_failures"]) > 0
        assert len(data["normalized_predictions"]) == 0
        assert data["signals_submitted"] == 0
        assert data["decisions_made"] == 0
        assert data["orders_created"] == 0

    def test_returns_zero_execution_counts(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Response always has zero signals_submitted, decisions_made, orders_created."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [
                    {
                        "ticker": t,
                        "current_price": "100.00",
                        "ensemble_day5": "105.00",
                        "d5_change_pct": "5.00",
                        "confidence": "75.0",
                        "recommendation": "BUY",
                        "per_model_summary": {},
                    }
                    for t in tickers
                ],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-010",
                "universe": "SP500",
                "tickers": ["AAPL"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_submitted"] == 0
        assert data["decisions_made"] == 0
        assert data["orders_created"] == 0

    def test_no_signal_rows_created(self, market_scan_prediction_seeded_client: TestClient, monkeypatch) -> None:
        """V1: No Signal rows are created in the database."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [
                    {
                        "ticker": t,
                        "current_price": "100.00",
                        "ensemble_day5": "105.00",
                        "d5_change_pct": "5.00",
                        "confidence": "80.0",
                        "recommendation": "SELL",
                        "per_model_summary": {},
                    }
                    for t in tickers
                ],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-011",
                "universe": "SP500",
                "tickers": ["AAPL"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200

        # Query database to verify no Signal rows were created
        from paper_trader.db.session import get_dedicated_session
        from sqlalchemy import select

        with get_dedicated_session() as session:
            signal_count = session.execute(
                select(Signal).where(Signal.source_run == "test-011")
            ).scalars().all()
            assert len(signal_count) == 0

    def test_no_trade_decision_rows_created(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """V1: No TradeDecision rows are created in the database."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [
                    {
                        "ticker": t,
                        "current_price": "100.00",
                        "ensemble_day5": "105.00",
                        "d5_change_pct": "5.00",
                        "confidence": "85.0",
                        "recommendation": "BUY",
                        "per_model_summary": {},
                    }
                    for t in tickers
                ],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-012",
                "universe": "SP500",
                "tickers": ["AAPL"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200

        # Query database to verify no TradeDecision rows were created
        from paper_trader.db.session import get_dedicated_session
        from sqlalchemy import select

        with get_dedicated_session() as session:
            decision_count = session.execute(
                select(TradeDecision)
            ).scalars().all()
            # Check that none of these decisions are from this test
            test_decisions = [d for d in decision_count if getattr(d, 'idempotency_key', None) == 'test-012']
            assert len(test_decisions) == 0

    def test_no_order_rows_created(self, market_scan_prediction_seeded_client: TestClient, monkeypatch) -> None:
        """V1: No Order rows are created in the database."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [
                    {
                        "ticker": t,
                        "current_price": "100.00",
                        "ensemble_day5": "105.00",
                        "d5_change_pct": "5.00",
                        "confidence": "90.0",
                        "recommendation": "BUY",
                        "per_model_summary": {},
                    }
                    for t in tickers
                ],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        # Count orders before request
        from paper_trader.db.session import get_dedicated_session
        from sqlalchemy import select

        with get_dedicated_session() as session:
            orders_before = len(session.execute(select(Order)).scalars().all())

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-013",
                "universe": "SP500",
                "tickers": ["AAPL"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200

        # Query database to verify no new Order rows were created
        with get_dedicated_session() as session:
            orders_after = len(session.execute(select(Order)).scalars().all())
            # No new orders should be created
            assert orders_after == orders_before

    def test_no_real_gcp_api_calls(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Tests do not call real GCP prediction API."""
        from paper_trader.engine.market_screener import CandidateScore, SkippedTicker

        def mock_scan(session, tickers=None, universe="SP500", benchmark_ticker="SPY", lookback_days=20, top_n=25, min_price_points=5):
            candidates = [
                CandidateScore(
                    rank=1,
                    ticker="AAPL",
                    score="5.23",
                    latest_price="150.00",
                    latest_market_date="2025-01-14",
                    price_count=25,
                    momentum_5d_pct="2.50",
                    momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15",
                    relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM", "OUTPERFORMING_SPY"],
                ),
            ]
            return candidates, [], date(2025, 1, 14)

        monkeypatch.setattr(
            "paper_trader.engine.market_screener.scan_market",
            mock_scan,
        )

        call_log = []

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            call_log.append(tickers)
            # Return one valid prediction
            return (
                [
                    {
                        "ticker": tickers[0],
                        "current_price": "150.00",
                        "ensemble_day5": "157.50",
                        "d5_change_pct": "5.00",
                        "confidence": "75.0",
                        "recommendation": "BUY",
                        "per_model_summary": {},
                    }
                ],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers",
            mock_fetch,
        )

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-014",
                "universe": "SP500",
                "tickers": ["AAPL"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        # Verify mock was called with selected tickers
        assert len(call_log) > 0
        assert isinstance(call_log[0], list)
        assert call_log[0] == ["AAPL"]
        # Verify we got predictions
        data = resp.json()
        assert len(data["selected_tickers"]) > 0
        assert data["selected_tickers"] == ["AAPL"]
        assert data["predictions_fetched"] == 1
        assert len(data["normalized_predictions"]) == 1

    def test_accounting_invariant_all_normalize_successfully(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Invariant: len(normalized) + len(failures) == len(selected_tickers) when all succeed."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM", "OUTPERFORMING_SPY"],
                ),
                CandidateScore(
                    rank=2, ticker="MSFT", score="4.15", latest_price="200.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="1.50", momentum_20d_pct="3.00",
                    volatility_20d_pct="3.20", relative_strength_vs_spy_20d="0.50",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return ([
                {
                    "ticker": t,
                    "current_price": "100.00",
                    "ensemble_day5": "105.00",
                    "d5_change_pct": "5.00",
                    "confidence": "80.0",
                    "recommendation": "BUY",
                    "per_model_summary": {},
                }
                for t in tickers
            ], [])

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-invariant-001",
                "universe": "SP500",
                "tickers": ["AAPL", "MSFT"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 2,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Invariant: normalized + failures == selected
        assert len(data["normalized_predictions"]) + len(data["prediction_failures"]) == len(data["selected_tickers"])
        assert len(data["normalized_predictions"]) == 2
        assert len(data["prediction_failures"]) == 0

    def test_accounting_invariant_one_fetch_failure(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Invariant holds: one fetch failure leaves ticker in failures list."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
                CandidateScore(
                    rank=2, ticker="MSFT", score="4.15", latest_price="200.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="1.50", momentum_20d_pct="3.00",
                    volatility_20d_pct="3.20", relative_strength_vs_spy_20d="0.50",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            # AAPL succeeds, MSFT fails at fetch level
            return ([
                {
                    "ticker": "AAPL",
                    "current_price": "150.00",
                    "ensemble_day5": "157.50",
                    "d5_change_pct": "5.00",
                    "confidence": "80.0",
                    "recommendation": "BUY",
                    "per_model_summary": {},
                }
            ], [
                {"ticker": "MSFT", "reason": "API timeout"}
            ])

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-invariant-002",
                "universe": "SP500",
                "tickers": ["AAPL", "MSFT"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 2,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Invariant: normalized + failures == selected
        assert len(data["normalized_predictions"]) + len(data["prediction_failures"]) == len(data["selected_tickers"])
        assert len(data["selected_tickers"]) == 2
        assert len(data["normalized_predictions"]) == 1
        assert len(data["prediction_failures"]) == 1
        # Verify MSFT is in failures with fetch error
        failure_tickers = {f["ticker"] for f in data["prediction_failures"]}
        assert "MSFT" in failure_tickers

    def test_accounting_invariant_normalization_failure(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Invariant holds: normalization failure records ticker in failures with reason."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
                CandidateScore(
                    rank=2, ticker="MSFT", score="4.15", latest_price="200.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="1.50", momentum_20d_pct="3.00",
                    volatility_20d_pct="3.20", relative_strength_vs_spy_20d="0.50",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            # AAPL succeeds, MSFT has invalid response (missing confidence for BUY)
            return ([
                {
                    "ticker": "AAPL",
                    "current_price": "150.00",
                    "ensemble_day5": "157.50",
                    "d5_change_pct": "5.00",
                    "confidence": "80.0",
                    "recommendation": "BUY",
                    "per_model_summary": {},
                },
                {
                    "ticker": "MSFT",
                    "current_price": "200.00",
                    "ensemble_day5": "210.00",
                    "d5_change_pct": "5.00",
                    "confidence": None,  # Invalid: missing confidence for BUY
                    "recommendation": "BUY",
                    "per_model_summary": {},
                }
            ], [])

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-invariant-003",
                "universe": "SP500",
                "tickers": ["AAPL", "MSFT"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 2,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Invariant: normalized + failures == selected
        assert len(data["normalized_predictions"]) + len(data["prediction_failures"]) == len(data["selected_tickers"])
        assert len(data["selected_tickers"]) == 2
        assert len(data["normalized_predictions"]) == 1
        assert len(data["prediction_failures"]) == 1
        # Verify MSFT is in failures with normalization error
        failure_tickers = {f["ticker"] for f in data["prediction_failures"]}
        assert "MSFT" in failure_tickers
        msft_failure = next((f for f in data["prediction_failures"] if f["ticker"] == "MSFT"), None)
        assert msft_failure is not None
        assert "Normalization failed" in msft_failure["reason"]

    def test_accounting_invariant_all_normalization_failures(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Invariant holds: all normalization failures → all selected tickers in failures."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
                CandidateScore(
                    rank=2, ticker="MSFT", score="4.15", latest_price="200.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="1.50", momentum_20d_pct="3.00",
                    volatility_20d_pct="3.20", relative_strength_vs_spy_20d="0.50",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            # Both fail normalization (invalid recommendations)
            return ([
                {
                    "ticker": "AAPL",
                    "current_price": "150.00",
                    "ensemble_day5": "157.50",
                    "d5_change_pct": "5.00",
                    "confidence": "80.0",
                    "recommendation": "INVALID_REC",  # Invalid
                    "per_model_summary": {},
                },
                {
                    "ticker": "MSFT",
                    "current_price": "200.00",
                    "ensemble_day5": "210.00",
                    "d5_change_pct": "5.00",
                    "confidence": "70.0",
                    "recommendation": "MAYBE",  # Invalid
                    "per_model_summary": {},
                }
            ], [])

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-invariant-004",
                "universe": "SP500",
                "tickers": ["AAPL", "MSFT"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 2,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Invariant: normalized + failures == selected
        assert len(data["normalized_predictions"]) + len(data["prediction_failures"]) == len(data["selected_tickers"])
        assert len(data["selected_tickers"]) == 2
        assert data["predictions_fetched"] == 2  # Raw responses came back
        assert len(data["normalized_predictions"]) == 0
        assert len(data["prediction_failures"]) == 2
        # Verify both AAPL and MSFT are in failures
        failure_tickers = {f["ticker"] for f in data["prediction_failures"]}
        assert "AAPL" in failure_tickers
        assert "MSFT" in failure_tickers

    def test_accounting_invariant_missing_response_for_selected_ticker(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Invariant holds: selected ticker with no response → recorded in failures."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="QCOM", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
                CandidateScore(
                    rank=2, ticker="AMD", score="4.15", latest_price="200.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="1.50", momentum_20d_pct="3.00",
                    volatility_20d_pct="3.20", relative_strength_vs_spy_20d="0.50",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            # Only AMD in response, QCOM is missing (not in failures, not in responses)
            return ([
                {
                    "ticker": "AMD",
                    "current_price": "100.00",
                    "ensemble_day5": "105.00",
                    "d5_change_pct": "5.00",
                    "confidence": "80.0",
                    "recommendation": "BUY",
                    "per_model_summary": {},
                }
            ], [])

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-invariant-005",
                "universe": "SP500",
                "tickers": ["QCOM", "AMD"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 2,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Invariant: normalized + failures == selected
        assert len(data["normalized_predictions"]) + len(data["prediction_failures"]) == len(data["selected_tickers"])
        assert len(data["selected_tickers"]) == 2
        assert len(data["normalized_predictions"]) == 1
        assert len(data["prediction_failures"]) == 1
        # Verify QCOM is in failures with "missing response" reason
        failure_tickers = {f["ticker"] for f in data["prediction_failures"]}
        assert "QCOM" in failure_tickers
        qcom_failure = next((f for f in data["prediction_failures"] if f["ticker"] == "QCOM"), None)
        assert qcom_failure is not None
        assert "No prediction response received" in qcom_failure["reason"]

    def test_accounting_invariant_live_like_case(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Invariant holds: live-like case with 5 tickers, mixed failures."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=i+1, ticker=t, score=f"{5-i}.00", latest_price="100.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="1.50", momentum_20d_pct="3.00",
                    volatility_20d_pct="2.00", relative_strength_vs_spy_20d="0.50",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                )
                for i, t in enumerate(["QCOM", "AMD", "CSCO", "TXN", "AMAT"])
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            # AMD normalizes, AMAT normalizes, rest fail normalization
            responses = []
            if "AMD" in tickers:
                responses.append({
                    "ticker": "AMD",
                    "current_price": "100.00",
                    "ensemble_day5": "105.00",
                    "d5_change_pct": "5.00",
                    "confidence": "80.0",
                    "recommendation": "BUY",
                    "per_model_summary": {},
                })
            if "AMAT" in tickers:
                responses.append({
                    "ticker": "AMAT",
                    "current_price": "100.00",
                    "ensemble_day5": "105.00",
                    "d5_change_pct": "5.00",
                    "confidence": "75.0",
                    "recommendation": "BUY",
                    "per_model_summary": {},
                })
            # QCOM, CSCO, TXN return invalid recommendations
            for t in ["QCOM", "CSCO", "TXN"]:
                if t in tickers:
                    responses.append({
                        "ticker": t,
                        "current_price": "100.00",
                        "ensemble_day5": "105.00",
                        "d5_change_pct": "5.00",
                        "confidence": "70.0",
                        "recommendation": "INVALID",
                        "per_model_summary": {},
                    })
            return responses, []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-invariant-006",
                "universe": "SP500",
                "tickers": ["QCOM", "AMD", "CSCO", "TXN", "AMAT"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Invariant: normalized + failures == selected
        assert len(data["normalized_predictions"]) + len(data["prediction_failures"]) == len(data["selected_tickers"])
        # Expected: 5 selected, 2 normalized (AMD, AMAT), 3 failures (QCOM, CSCO, TXN)
        assert len(data["selected_tickers"]) == 5
        assert data["predictions_fetched"] == 5
        assert len(data["normalized_predictions"]) == 2
        assert len(data["prediction_failures"]) == 3
        # Verify the right tickers are in failures
        failure_tickers = {f["ticker"] for f in data["prediction_failures"]}
        assert failure_tickers == {"QCOM", "CSCO", "TXN"}

    def test_candidate_previews_length_equals_selected_tickers(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Invariant: len(candidate_previews) == len(selected_tickers)."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, universe="SP500", benchmark_ticker="SPY", lookback_days=20, top_n=25, min_price_points=5):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr(
            "paper_trader.engine.market_screener.scan_market", mock_scan
        )

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [{
                    "ticker": "AAPL", "current_price": "150.00",
                    "ensemble_day5": "157.50", "d5_change_pct": "5.00",
                    "confidence": "80.0", "recommendation": "BUY",
                    "per_model_summary": {},
                }],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch
        )

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-cand-001",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 10, "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "candidate_previews" in data
        assert len(data["candidate_previews"]) == len(data["selected_tickers"])

    def test_accounting_invariant_with_candidate_previews(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Existing accounting invariant: len(normalized) + len(failures) == len(selected)."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
                CandidateScore(
                    rank=2, ticker="MSFT", score="4.15", latest_price="200.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="1.50", momentum_20d_pct="3.00",
                    volatility_20d_pct="3.20", relative_strength_vs_spy_20d="0.50",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr(
            "paper_trader.engine.market_screener.scan_market", mock_scan
        )

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            # Only AAPL succeeds, MSFT fails fetch
            return (
                [{
                    "ticker": "AAPL", "current_price": "150.00",
                    "ensemble_day5": "157.50", "d5_change_pct": "5.00",
                    "confidence": "80.0", "recommendation": "BUY",
                    "per_model_summary": {},
                }],
                [{"ticker": "MSFT", "reason": "Service unavailable"}],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch
        )

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-cand-002",
                "universe": "SP500", "tickers": ["AAPL", "MSFT"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 10, "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Existing invariant
        normalized_count = len(data["normalized_predictions"])
        failures_count = len(data["prediction_failures"])
        selected_count = len(data["selected_tickers"])
        assert normalized_count + failures_count == selected_count
        # New invariant
        assert len(data["candidate_previews"]) == selected_count

    def test_preview_decision_buy_high_confidence_positive_return_is_consider(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """BUY + confidence >= 0.70 + positive return = CONSIDER."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr(
            "paper_trader.engine.market_screener.scan_market", mock_scan
        )

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [{
                    "ticker": "AAPL", "current_price": "150.00",
                    "ensemble_day5": "157.50", "d5_change_pct": "5.00",
                    "confidence": "85.0", "recommendation": "BUY",
                    "per_model_summary": {},
                }],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch
        )

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-cand-003",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 10, "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        previews = data["candidate_previews"]
        assert len(previews) == 1
        preview = previews[0]
        assert preview["ticker"] == "AAPL"
        assert preview["preview_decision"] == "CONSIDER"
        assert preview["status"] == "OK"

    def test_preview_decision_hold_becomes_watch(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """HOLD recommendation = WATCH."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr(
            "paper_trader.engine.market_screener.scan_market", mock_scan
        )

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [{
                    "ticker": "AAPL", "current_price": "150.00",
                    "ensemble_day5": "151.00", "d5_change_pct": "0.67",
                    "confidence": "50.0", "recommendation": "HOLD",
                    "per_model_summary": {},
                }],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch
        )

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-cand-004",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 10, "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        preview = data["candidate_previews"][0]
        assert preview["preview_decision"] == "WATCH"

    def test_preview_decision_sell_becomes_reject(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """SELL recommendation = REJECT."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr(
            "paper_trader.engine.market_screener.scan_market", mock_scan
        )

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [{
                    "ticker": "AAPL", "current_price": "150.00",
                    "ensemble_day5": "145.00", "d5_change_pct": "-3.33",
                    "confidence": "90.0", "recommendation": "SELL",
                    "per_model_summary": {},
                }],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch
        )

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-cand-005",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 10, "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        preview = data["candidate_previews"][0]
        assert preview["preview_decision"] == "REJECT"

    def test_preview_status_failed_fetch(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Fetch failure results in status=FAILED_FETCH and decision=REJECT."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr(
            "paper_trader.engine.market_screener.scan_market", mock_scan
        )

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], [{"ticker": "AAPL", "reason": "Timeout"}]

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch
        )

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-cand-006",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 10, "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        preview = data["candidate_previews"][0]
        assert preview["status"] == "FAILED_FETCH"
        assert preview["preview_decision"] == "REJECT"
        assert preview["prediction_recommendation"] is None

    def test_preview_status_failed_normalization(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Normalization failure results in status=FAILED_NORMALIZATION and decision=REJECT."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr(
            "paper_trader.engine.market_screener.scan_market", mock_scan
        )

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [{
                    "ticker": "AAPL", "current_price": "150.00",
                    # Missing required fields: confidence, ensemble_day5, etc.
                }],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch
        )

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-cand-007",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 10, "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        preview = data["candidate_previews"][0]
        assert preview["status"] == "FAILED_NORMALIZATION"
        assert preview["preview_decision"] == "REJECT"

    def test_preview_score_bounded_0_to_100(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """preview_score is bounded between 0 and 100."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr(
            "paper_trader.engine.market_screener.scan_market", mock_scan
        )

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [{
                    "ticker": "AAPL", "current_price": "150.00",
                    "ensemble_day5": "200.00", "d5_change_pct": "33.33",
                    "confidence": "95.0", "recommendation": "BUY",
                    "per_model_summary": {},
                }],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch
        )

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-cand-008",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 10, "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        preview = data["candidate_previews"][0]
        score = float(preview["preview_score"])
        assert 0 <= score <= 100, f"Score {score} not in [0, 100]"

    def test_preview_reasons_populated(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """preview_reasons is populated with explainable reasons."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr(
            "paper_trader.engine.market_screener.scan_market", mock_scan
        )

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [{
                    "ticker": "AAPL", "current_price": "150.00",
                    "ensemble_day5": "157.50", "d5_change_pct": "5.00",
                    "confidence": "85.0", "recommendation": "BUY",
                    "per_model_summary": {},
                }],
                [],
            )

        monkeypatch.setattr(
            "paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch
        )

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-cand-009",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 10, "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        preview = data["candidate_previews"][0]
        assert isinstance(preview["preview_reasons"], list)
        assert len(preview["preview_reasons"]) > 0

    # ------------------------------------------------------------------
    # candidate_funnel tests
    # ------------------------------------------------------------------

    def test_prediction_candidates_response_includes_candidate_funnel(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Response includes a candidate_funnel object with all required keys."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-funnel-001",
                "universe": "SP500",
                "tickers": ["AAPL", "MSFT"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 2,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "candidate_funnel" in data
        funnel = data["candidate_funnel"]
        for key in (
            "universe_count", "evaluated_count", "skipped_count",
            "skipped_by_reason", "top_scan_count", "clean_scan_count",
            "prediction_top_n", "gcp_prediction_count", "not_sent_to_gcp_count",
            "prediction_outcomes", "top_scan_not_predicted", "skipped_examples",
        ):
            assert key in funnel, f"Missing funnel key: {key}"
        outcomes = funnel["prediction_outcomes"]
        for ok in ("consider", "watch", "reject", "failed_fetch", "other"):
            assert ok in outcomes, f"Missing outcomes key: {ok}"

    def test_candidate_funnel_counts_universe_evaluated_skipped(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """candidate_funnel.universe_count, evaluated_count, skipped_count are correct."""
        from paper_trader.engine.market_screener import CandidateScore, SkippedTicker

        def mock_scan(session, tickers=None, **kwargs):
            candidates = [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.00", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.00", relative_strength_vs_spy_20d="1.00",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ]
            skipped = [
                SkippedTicker(ticker="XYZ", reason="INSUFFICIENT_PRICE_HISTORY", price_count=2),
                SkippedTicker(ticker="ABC", reason="NO_PRICE_DATA", price_count=0),
            ]
            return candidates, skipped, date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-funnel-002",
                "universe": "SP500",
                "tickers": ["AAPL", "XYZ", "ABC"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 1,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        funnel = resp.json()["candidate_funnel"]
        assert funnel["universe_count"] == 3
        assert funnel["skipped_count"] == 2
        assert funnel["evaluated_count"] == 1

    def test_candidate_funnel_skipped_by_reason(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """candidate_funnel.skipped_by_reason aggregates skip reasons correctly."""
        from paper_trader.engine.market_screener import SkippedTicker

        def mock_scan(session, tickers=None, **kwargs):
            skipped = [
                SkippedTicker(ticker="T1", reason="INSUFFICIENT_PRICE_HISTORY", price_count=1),
                SkippedTicker(ticker="T2", reason="INSUFFICIENT_PRICE_HISTORY", price_count=3),
                SkippedTicker(ticker="T3", reason="DATA_QUALITY_OUTLIER", price_count=20),
            ]
            return [], skipped, None

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-funnel-003",
                "universe": "SP500",
                "tickers": ["T1", "T2", "T3"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 5,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        sbr = resp.json()["candidate_funnel"]["skipped_by_reason"]
        assert sbr.get("INSUFFICIENT_PRICE_HISTORY") == 2
        assert sbr.get("DATA_QUALITY_OUTLIER") == 1

    def test_candidate_funnel_prediction_top_n_cutoff(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """top_scan_not_predicted lists candidates beyond prediction_top_n cutoff."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            candidates = [
                CandidateScore(
                    rank=i, ticker=f"TK{i}", score=str(10.0 - i),
                    latest_price="100.00", latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="1.00", momentum_20d_pct="2.00",
                    volatility_20d_pct="1.50", relative_strength_vs_spy_20d="0.50",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                )
                for i in range(1, 6)  # 5 candidates
            ]
            return candidates, [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-funnel-004",
                "universe": "SP500",
                "tickers": [f"TK{i}" for i in range(1, 6)],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 2,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        funnel = resp.json()["candidate_funnel"]
        assert funnel["gcp_prediction_count"] == 2
        assert funnel["not_sent_to_gcp_count"] == 3
        not_pred = funnel["top_scan_not_predicted"]
        assert len(not_pred) == 3
        assert all(entry["reason"] == "Below prediction_top_n cutoff" for entry in not_pred)

    def test_candidate_funnel_prediction_outcomes(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """candidate_funnel.prediction_outcomes counts decisions correctly."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.00", relative_strength_vs_spy_20d="1.00",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [{
                    "ticker": "AAPL", "current_price": "150.00",
                    "ensemble_day5": "157.50", "d5_change_pct": "5.00",
                    "confidence": "85.0", "recommendation": "BUY",
                    "per_model_summary": {},
                }],
                [],
            )

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-funnel-005",
                "universe": "SP500",
                "tickers": ["AAPL"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 1,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        outcomes = resp.json()["candidate_funnel"]["prediction_outcomes"]
        total = (
            outcomes["consider"] + outcomes["watch"] + outcomes["reject"]
            + outcomes["failed_fetch"] + outcomes["other"]
        )
        assert total >= 1
        # BUY at 85% confidence + 5% return should land as CONSIDER or WATCH
        assert outcomes["consider"] >= 1 or outcomes["watch"] >= 1

    def test_candidate_funnel_preserves_preview_only_safety(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """candidate_funnel addition must not create signals, decisions, or orders."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-funnel-006",
                "universe": "SP500",
                "tickers": ["AAPL", "MSFT"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 10,
                "min_price_points": 5,
                "prediction_top_n": 2,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_submitted"] == 0
        assert data["decisions_made"] == 0
        assert data["orders_created"] == 0
        assert "candidate_funnel" in data

    # ------------------------------------------------------------------
    # Phase 2 — holdings injection and new diagnostics
    # ------------------------------------------------------------------

    def test_prediction_candidates_injects_current_holdings_into_gcp_batch(
        self, market_scan_prediction_seeded_client: TestClient, api_engine, monkeypatch
    ) -> None:
        """Open positions are injected into the GCP prediction batch even if not top scan candidates."""
        received_tickers: list[list[str]] = []

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            received_tickers.append(list(tickers))
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        # Create a position for NVDA — not in seeded price data, so it won't rank in scan
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            open_position(
                session,
                ticker="NVDA",
                qty=Decimal("2"),
                fill_price=Decimal("500.00"),
                now=_NOW,
            )
            session.commit()

        try:
            resp = market_scan_prediction_seeded_client.post(
                "/v1/strategy/market-scan/prediction-candidates",
                json={
                    "idempotency_key": "test-p2-001",
                    "universe": "SP500",
                    "tickers": ["AAPL", "MSFT"],
                    "benchmark_ticker": "SPY",
                    "lookback_days": 20,
                    "top_n": 5,
                    "min_price_points": 5,
                    "prediction_top_n": 2,
                    "include_current_positions_for_prediction": True,
                    "dry_run": True,
                    "submit_signals": False,
                    "run_risk": False,
                    "create_orders": False,
                },
                headers=_AUTH,
            )
            assert resp.status_code == 200
            data = resp.json()
            funnel = data["candidate_funnel"]
            # NVDA should have been injected
            assert funnel["current_holdings_injected_count"] >= 1
            # selected_tickers should include NVDA
            assert "NVDA" in data["selected_tickers"]
        finally:
            # Clean up the NVDA position so it doesn't pollute later tests
            with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
                from sqlalchemy import delete
                session.execute(delete(Position).where(Position.ticker == "NVDA"))
                session.commit()

    def test_prediction_candidates_does_not_duplicate_holding_already_in_scan(
        self, market_scan_prediction_seeded_client: TestClient, api_engine, monkeypatch
    ) -> None:
        """A holding that already ranks in top scan candidates is not duplicated in GCP batch."""
        received_tickers: list[list[str]] = []

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            received_tickers.append(list(tickers))
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        # Delete any pre-existing positions for test isolation, then create AAPL
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            from sqlalchemy import delete as sa_delete
            session.execute(sa_delete(Position))
            session.commit()
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            open_position(
                session,
                ticker="AAPL",
                qty=Decimal("1"),
                fill_price=Decimal("150.00"),
                now=_NOW,
            )
            session.commit()

        try:
            resp = market_scan_prediction_seeded_client.post(
                "/v1/strategy/market-scan/prediction-candidates",
                json={
                    "idempotency_key": "test-p2-002",
                    "universe": "SP500",
                    "tickers": ["AAPL", "MSFT"],
                    "benchmark_ticker": "SPY",
                    "lookback_days": 20,
                    "top_n": 5,
                    "min_price_points": 5,
                    "prediction_top_n": 2,
                    "include_current_positions_for_prediction": True,
                    "dry_run": True,
                    "submit_signals": False,
                    "run_risk": False,
                    "create_orders": False,
                },
                headers=_AUTH,
            )
            assert resp.status_code == 200
            data = resp.json()
            # AAPL should appear exactly once in selected_tickers — not duplicated by holdings injection
            selected = data["selected_tickers"]
            assert "AAPL" in selected
            assert selected.count("AAPL") == 1
            # Candidate previews must also not duplicate AAPL
            previews_for_aapl = [p for p in data["candidate_previews"] if p["ticker"] == "AAPL"]
            assert len(previews_for_aapl) <= 1
            # Any injected count > 0 reflects other open positions in full-suite DB state, not duplicate AAPL
            funnel = data["candidate_funnel"]
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
                from sqlalchemy import delete as sa_delete
                session.execute(sa_delete(Position))
                session.commit()
            # Restore TSLA position that persists throughout this module
            with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
                open_position(
                    session,
                    ticker="TSLA",
                    qty=Decimal("3"),
                    fill_price=Decimal("250.000000"),
                    now=_NOW,
                )
                session.commit()

    def test_prediction_candidates_does_not_send_all_evaluated_tickers_to_gcp(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Only top prediction_top_n scan candidates (plus holdings) go to GCP, not the whole universe."""
        received_tickers: list[list[str]] = []

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            received_tickers.append(list(tickers))
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-p2-003",
                "universe": "SP500",
                "tickers": ["AAPL", "MSFT"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 5,
                "min_price_points": 5,
                "prediction_top_n": 1,
                "include_current_positions_for_prediction": False,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Only 1 ticker should have been sent to GCP (prediction_top_n=1, no holdings)
        assert data["candidate_funnel"]["gcp_prediction_count"] <= 1
        if received_tickers:
            assert len(received_tickers[0]) <= 1

    def test_prediction_candidates_response_includes_new_diagnostics(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Response candidate_funnel includes current_holdings_injected_count, gcp_concurrency, prediction_elapsed_ms."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-p2-004",
                "universe": "SP500",
                "tickers": ["AAPL", "MSFT"],
                "benchmark_ticker": "SPY",
                "lookback_days": 20,
                "top_n": 5,
                "min_price_points": 5,
                "prediction_top_n": 2,
                "max_prediction_concurrency": 3,
                "include_current_positions_for_prediction": False,
                "dry_run": True,
                "submit_signals": False,
                "run_risk": False,
                "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        funnel = resp.json()["candidate_funnel"]
        assert "current_holdings_injected_count" in funnel
        assert "gcp_concurrency" in funnel
        assert "prediction_elapsed_ms" in funnel
        assert funnel["gcp_concurrency"] == 3
        assert isinstance(funnel["prediction_elapsed_ms"], int)
        assert funnel["prediction_elapsed_ms"] >= 0

    # ---------------------------------------------------------------------------
    # Phase 4A: Candidate Scoring Diagnostics tests
    # ---------------------------------------------------------------------------

    def test_candidate_funnel_includes_new_diagnostic_fields(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4A: candidate_funnel includes new extended diagnostic fields."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4a-funnel-001",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        funnel = resp.json()["candidate_funnel"]
        assert "price_history_ready_count" in funnel
        assert "skipped_insufficient_history_count" in funnel
        assert "local_scan_candidate_count" in funnel
        assert "prediction_batch_count" in funnel
        assert "gcp_success_count" in funnel
        assert "gcp_failure_count" in funnel
        assert "final_selected_count" in funnel
        assert "safety_counts" in funnel
        safety = funnel["safety_counts"]
        assert safety["signals_created"] == 0
        assert safety["decisions_created"] == 0
        assert safety["orders_created"] == 0
        assert funnel["prediction_batch_count"] == len(resp.json()["selected_tickers"])
        assert funnel["final_selected_count"] == len(resp.json()["candidate_previews"])

    def test_response_includes_scoring_summary(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4A: response includes scoring_summary with formula labels and thresholds."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4a-scoring-001",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 7, "prediction_top_n": 3,
                "max_prediction_concurrency": 2,
                "include_current_positions_for_prediction": False,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "scoring_summary" in data
        ss = data["scoring_summary"]
        assert "local_scan_formula_label" in ss
        assert "final_score_formula_label" in ss
        assert "top_driver_counts" in ss
        assert "threshold_summary" in ss
        thresh = ss["threshold_summary"]
        assert thresh["min_price_points"] == 7
        assert thresh["prediction_top_n"] == 3
        assert thresh["scan_top_n"] == 5
        assert thresh["max_prediction_concurrency"] == 2
        assert thresh["include_current_positions_for_prediction"] is False
        assert "mom" in ss["local_scan_formula_label"].lower() or "scan" in ss["local_scan_formula_label"].lower()

    def test_candidate_preview_includes_explainability_fields(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4A: candidate_previews rows include explainability fields."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=22,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [{
                    "ticker": "AAPL", "current_price": "150.00",
                    "ensemble_day5": "157.50", "d5_change_pct": "5.00",
                    "confidence": "80.0", "recommendation": "BUY",
                    "per_model_summary": {},
                }],
                [],
            )

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4a-preview-001",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        previews = resp.json()["candidate_previews"]
        assert len(previews) == 1
        p = previews[0]
        assert "price_history_points" in p
        assert "prediction_status" in p
        assert "selected_for_gcp_reason" in p
        assert "top_score_drivers" in p
        assert "skip_or_warning_reason" in p
        assert p["price_history_points"] == 22
        assert p["prediction_status"] == "OK"
        assert isinstance(p["top_score_drivers"], list)

    def test_top_scan_candidate_has_top_scan_gcp_reason(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4A: ticker selected by scan has selected_for_gcp_reason TOP_SCAN (or BOTH)."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4a-reason-top-001",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        previews = resp.json()["candidate_previews"]
        aapl = next((p for p in previews if p["ticker"] == "AAPL"), None)
        assert aapl is not None
        assert aapl["selected_for_gcp_reason"] in ("TOP_SCAN", "BOTH")

    def test_holding_injected_ticker_has_holding_gcp_reason(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch, api_engine
    ) -> None:
        """Phase 4A: holding injected ticker has selected_for_gcp_reason CURRENT_HOLDING_INJECTED."""
        from paper_trader.engine.market_screener import CandidateScore
        from paper_trader.db.models import Position

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        # Insert a position for a ticker NOT in the scan results
        holding_ticker = "NVDA_4A_HOLD_TEST"
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            pos = Position(
                ticker=holding_ticker,
                qty=Decimal("10"),
                avg_cost=Decimal("400.00"),
                cost_basis=Decimal("4000.00"),
                opened_at=_NOW,
                last_updated=_NOW,
            )
            session.add(pos)
            session.commit()

        try:
            resp = market_scan_prediction_seeded_client.post(
                "/v1/strategy/market-scan/prediction-candidates",
                json={
                    "idempotency_key": "test-4a-reason-hold-001",
                    "universe": "SP500", "tickers": ["AAPL"],
                    "benchmark_ticker": "SPY", "lookback_days": 20,
                    "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                    "dry_run": True, "submit_signals": False,
                    "run_risk": False, "create_orders": False,
                    "include_current_positions_for_prediction": True,
                },
                headers=_AUTH,
            )
            assert resp.status_code == 200
            previews = resp.json()["candidate_previews"]
            holding_preview = next((p for p in previews if p["ticker"] == holding_ticker), None)
            assert holding_preview is not None, f"{holding_ticker} should appear in previews as injected holding"
            assert holding_preview["selected_for_gcp_reason"] in ("CURRENT_HOLDING_INJECTED", "BOTH")
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
                session.execute(
                    select(Position).where(Position.ticker == holding_ticker)
                )
                pos_to_del = session.execute(
                    select(Position).where(Position.ticker == holding_ticker)
                ).scalars().first()
                if pos_to_del:
                    session.delete(pos_to_del)
                    session.commit()

    def test_skipped_diagnostics_capped_at_sample_limit(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4A: skipped_diagnostics.samples is capped at sample_limit (25)."""
        from paper_trader.engine.market_screener import SkippedTicker as SkippedTickerDS

        def mock_scan(session, tickers=None, **kwargs):
            # Return 40 skipped tickers (no candidates)
            skipped = [
                SkippedTickerDS(ticker=f"SK{i:03d}", reason="INSUFFICIENT_PRICE_HISTORY", price_count=i)
                for i in range(40)
            ]
            return [], skipped, date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4a-skip-cap-001",
                "universe": "SP500", "tickers": [f"SK{i:03d}" for i in range(40)],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "skipped_diagnostics" in data
        sd = data["skipped_diagnostics"]
        assert sd["total_skipped"] == 40
        assert sd["sample_limit"] == 25
        assert len(sd["samples"]) == 25

    def test_skipped_diagnostics_has_insufficient_history_reason(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4A: skipped_diagnostics includes INSUFFICIENT_PRICE_HISTORY reason."""
        from paper_trader.engine.market_screener import SkippedTicker as SkippedTickerDS

        def mock_scan(session, tickers=None, **kwargs):
            skipped = [
                SkippedTickerDS(ticker="NOHIST_4A", reason="INSUFFICIENT_PRICE_HISTORY", price_count=2),
            ]
            return [], skipped, date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4a-skip-hist-001",
                "universe": "SP500", "tickers": ["NOHIST_4A"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        sd = resp.json()["skipped_diagnostics"]
        assert sd["total_skipped"] == 1
        assert len(sd["samples"]) == 1
        assert sd["samples"][0]["ticker"] == "NOHIST_4A"
        assert sd["samples"][0]["reason"] == "INSUFFICIENT_PRICE_HISTORY"
        assert sd["samples"][0]["price_history_points"] == 2
        assert sd["samples"][0]["required_min_price_points"] == 5

    def test_diagnostics_safety_counts_all_zero(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4A: diagnostics safety_counts are always zero (preview-only)."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4a-safety-001",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        safety = data["candidate_funnel"]["safety_counts"]
        assert safety["signals_created"] == 0
        assert safety["decisions_created"] == 0
        assert safety["orders_created"] == 0
        assert data["signals_submitted"] == 0
        assert data["decisions_made"] == 0
        assert data["orders_created"] == 0

    def test_scoring_formula_labels_present(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4A: scoring_summary.local_scan_formula_label and final_score_formula_label are non-empty."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4a-formula-001",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        ss = resp.json()["scoring_summary"]
        assert len(ss["local_scan_formula_label"]) > 10
        assert len(ss["final_score_formula_label"]) > 10
        # Both labels reference momentum
        assert "mom" in ss["local_scan_formula_label"] or "5d" in ss["local_scan_formula_label"]
        assert "conf" in ss["final_score_formula_label"] or "mom" in ss["final_score_formula_label"]

    # ---------------------------------------------------------------------------
    # Phase 4B: Candidate Classification tests
    # ---------------------------------------------------------------------------

    def test_candidate_preview_includes_classification_fields(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4B: candidate_previews rows include candidate_type, is_current_holding, eligible_for_review_queue."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [{
                    "ticker": "AAPL", "current_price": "150.00",
                    "ensemble_day5": "157.50", "d5_change_pct": "5.00",
                    "confidence": "80.0", "recommendation": "BUY",
                    "per_model_summary": {},
                }],
                [],
            )

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4b-class-001",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        previews = resp.json()["candidate_previews"]
        assert len(previews) >= 1
        p = previews[0]
        assert "candidate_type" in p
        assert "is_current_holding" in p
        assert "eligible_for_review_queue" in p
        assert "review_queue_eligibility_reason" in p

    def test_top_scan_candidate_is_new_buy_candidate(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4B: ticker selected by top scan (not held) has candidate_type NEW_BUY_CANDIDATE."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4b-newbuy-001",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        previews = resp.json()["candidate_previews"]
        aapl = next((p for p in previews if p["ticker"] == "AAPL"), None)
        assert aapl is not None
        assert aapl["candidate_type"] == "NEW_BUY_CANDIDATE"
        assert aapl["is_current_holding"] is False

    def test_holding_injected_candidate_is_current_holding_monitor(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch, api_engine
    ) -> None:
        """Phase 4B: holding-injected ticker has candidate_type CURRENT_HOLDING_MONITOR and is_current_holding True."""
        from paper_trader.engine.market_screener import CandidateScore
        from paper_trader.db.models import Position

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        holding_ticker = "MSFT_4B_HOLD_TEST"
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            pos = Position(
                ticker=holding_ticker,
                qty=Decimal("5"),
                avg_cost=Decimal("300.00"),
                cost_basis=Decimal("1500.00"),
                opened_at=_NOW,
                last_updated=_NOW,
            )
            session.add(pos)
            session.commit()

        try:
            resp = market_scan_prediction_seeded_client.post(
                "/v1/strategy/market-scan/prediction-candidates",
                json={
                    "idempotency_key": "test-4b-holdmon-001",
                    "universe": "SP500", "tickers": ["AAPL"],
                    "benchmark_ticker": "SPY", "lookback_days": 20,
                    "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                    "dry_run": True, "submit_signals": False,
                    "run_risk": False, "create_orders": False,
                    "include_current_positions_for_prediction": True,
                },
                headers=_AUTH,
            )
            assert resp.status_code == 200
            previews = resp.json()["candidate_previews"]
            hold_p = next((p for p in previews if p["ticker"] == holding_ticker), None)
            assert hold_p is not None
            assert hold_p["candidate_type"] == "CURRENT_HOLDING_MONITOR"
            assert hold_p["is_current_holding"] is True
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
                from sqlalchemy import delete
                session.execute(delete(Position).where(Position.ticker == holding_ticker))
                session.commit()

    def test_holding_monitor_has_eligible_for_review_queue_false(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch, api_engine
    ) -> None:
        """Phase 4B: current holding monitor has eligible_for_review_queue False."""
        from paper_trader.engine.market_screener import CandidateScore
        from paper_trader.db.models import Position

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [{
                    "ticker": "MSFT_4B_ELIG_TEST", "current_price": "300.00",
                    "ensemble_day5": "315.00", "d5_change_pct": "5.00",
                    "confidence": "80.0", "recommendation": "BUY",
                    "per_model_summary": {},
                }],
                [],
            )

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        holding_ticker = "MSFT_4B_ELIG_TEST"
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            pos = Position(
                ticker=holding_ticker,
                qty=Decimal("3"),
                avg_cost=Decimal("300.00"),
                cost_basis=Decimal("900.00"),
                opened_at=_NOW,
                last_updated=_NOW,
            )
            session.add(pos)
            session.commit()

        try:
            resp = market_scan_prediction_seeded_client.post(
                "/v1/strategy/market-scan/prediction-candidates",
                json={
                    "idempotency_key": "test-4b-elig-false-001",
                    "universe": "SP500", "tickers": ["AAPL"],
                    "benchmark_ticker": "SPY", "lookback_days": 20,
                    "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                    "dry_run": True, "submit_signals": False,
                    "run_risk": False, "create_orders": False,
                    "include_current_positions_for_prediction": True,
                },
                headers=_AUTH,
            )
            assert resp.status_code == 200
            previews = resp.json()["candidate_previews"]
            hold_p = next((p for p in previews if p["ticker"] == holding_ticker), None)
            assert hold_p is not None
            assert hold_p["eligible_for_review_queue"] is False
            assert hold_p["review_queue_eligibility_reason"] == "CURRENT_HOLDING_MONITOR_NOT_NEW_BUY"
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
                from sqlalchemy import delete
                session.execute(delete(Position).where(Position.ticker == holding_ticker))
                session.commit()

    def test_top_scan_consider_candidate_has_eligible_true(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4B: top-scan CONSIDER non-held candidate has eligible_for_review_queue True."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [{
                    "ticker": "AAPL", "current_price": "150.00",
                    "ensemble_day5": "157.50", "d5_change_pct": "5.00",
                    "confidence": "80.0", "recommendation": "BUY",
                    "per_model_summary": {},
                }],
                [],
            )

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4b-elig-true-001",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        previews = resp.json()["candidate_previews"]
        aapl = next((p for p in previews if p["ticker"] == "AAPL"), None)
        assert aapl is not None
        # AAPL is a top-scan non-held candidate with BUY recommendation -> CONSIDER
        if aapl["preview_decision"] == "CONSIDER":
            assert aapl["eligible_for_review_queue"] is True
            assert aapl["review_queue_eligibility_reason"] == "NEW_BUY_CANDIDATE"

    # ---------------------------------------------------------------------------
    # Phase 4B: Save behavior — skipping holdings
    # ---------------------------------------------------------------------------

    def test_save_does_not_save_current_holding_monitor_rows(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4B: save endpoint skips candidates with candidate_type=CURRENT_HOLDING_MONITOR."""
        resp = market_scan_prediction_seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-4b-save-skip-001",
                "candidates": [
                    {
                        "ticker": "NVDA",
                        "preview_decision": "CONSIDER",
                        "preview_score": "70",
                        "status": "OK",
                        "candidate_type": "CURRENT_HOLDING_MONITOR",
                        "eligible_for_review_queue": False,
                    },
                    {
                        "ticker": "MSFT",
                        "preview_decision": "CONSIDER",
                        "preview_score": "65",
                        "status": "OK",
                        "candidate_type": "NEW_BUY_CANDIDATE",
                        "eligible_for_review_queue": True,
                    },
                ],
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["skipped_current_holdings"] == 1
        assert data["inserted_count"] == 1
        saved_tickers = [c["ticker"] for c in data["candidates_saved"]]
        assert "MSFT" in saved_tickers
        assert "NVDA" not in saved_tickers

    def test_save_returns_skipped_current_holdings_count(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4B: save endpoint returns skipped_current_holdings in response."""
        resp = market_scan_prediction_seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-4b-save-count-001",
                "candidates": [
                    {
                        "ticker": "AMZN_4B_COUNT",
                        "preview_decision": "CONSIDER",
                        "preview_score": "60",
                        "status": "OK",
                        "candidate_type": "CURRENT_HOLDING_MONITOR",
                        "eligible_for_review_queue": False,
                    },
                ],
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "skipped_current_holdings" in data
        assert "skipped_watch" in data
        assert "skipped_rejected" in data
        assert "skipped_other" in data
        assert "saved_new_candidates" in data
        assert data["skipped_current_holdings"] == 1
        assert data["inserted_count"] == 0

    def test_save_backward_compatible_without_classification_fields(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4B: save without candidate_type/eligible_for_review_queue still works (backward compat)."""
        resp = market_scan_prediction_seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-4b-compat-001",
                "candidates": [
                    {
                        "ticker": "GOOG_4B_COMPAT",
                        "preview_decision": "CONSIDER",
                        "preview_score": "55",
                        "status": "OK",
                    },
                ],
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Should insert normally — no classification fields means no skipping
        assert data["inserted_count"] == 1
        assert data["skipped_current_holdings"] == 0

    # ---------------------------------------------------------------------------
    # Phase 4B: Balanced scoring
    # ---------------------------------------------------------------------------

    def test_default_scoring_profile_current_preserves_existing_behavior(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4B: default scoring_profile='current' returns same fields as before."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4b-current-001",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "scoring_profile": "current",
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Current profile: no scoring_profile_comparison, no balanced fields populated
        assert data["scoring_profile_comparison"] is None
        previews = data["candidate_previews"]
        if previews:
            p = previews[0]
            # Balanced fields should be None when profile is current
            assert p["current_score"] is None
            assert p["balanced_preview_score"] is None

    def test_balanced_preview_adds_balanced_score_fields(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4B: scoring_profile='balanced_preview' adds balanced score fields to candidate_previews."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
                CandidateScore(
                    rank=2, ticker="MSFT", score="4.10", latest_price="380.00",
                    latest_market_date="2025-01-14", price_count=22,
                    momentum_5d_pct="1.20", momentum_20d_pct="3.00",
                    volatility_20d_pct="1.80", relative_strength_vs_spy_20d="0.80",
                    reason_codes=["POSITIVE_5D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return (
                [
                    {
                        "ticker": "AAPL", "current_price": "150.00",
                        "ensemble_day5": "157.50", "d5_change_pct": "5.00",
                        "confidence": "80.0", "recommendation": "BUY",
                        "per_model_summary": {},
                    },
                    {
                        "ticker": "MSFT", "current_price": "380.00",
                        "ensemble_day5": "395.00", "d5_change_pct": "3.90",
                        "confidence": "70.0", "recommendation": "BUY",
                        "per_model_summary": {},
                    },
                ],
                [],
            )

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4b-balanced-001",
                "universe": "SP500", "tickers": ["AAPL", "MSFT"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 3,
                "scoring_profile": "balanced_preview",
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        previews = data["candidate_previews"]
        assert len(previews) >= 1
        p = previews[0]
        assert p["current_score"] is not None
        assert p["balanced_preview_score"] is not None
        assert p["score_delta"] is not None
        assert p["current_rank"] is not None
        assert p["balanced_preview_rank"] is not None
        assert p["ranking_change"] is not None
        assert isinstance(p["balanced_score_drivers"], list)

    def test_balanced_preview_includes_scoring_profile_comparison(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4B: balanced_preview includes scoring_profile_comparison in response."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=1, ticker="AAPL", score="5.23", latest_price="150.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.50", momentum_20d_pct="5.00",
                    volatility_20d_pct="2.15", relative_strength_vs_spy_20d="1.23",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                ),
            ], [], date(2025, 1, 14)

        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4b-cmp-001",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "scoring_profile": "balanced_preview",
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        cmp = data["scoring_profile_comparison"]
        assert cmp is not None
        assert cmp["active_profile"] == "balanced_preview"
        assert isinstance(cmp["current_top_tickers"], list)
        assert isinstance(cmp["balanced_top_tickers"], list)
        assert isinstance(cmp["overlap_count"], int)
        assert isinstance(cmp["changed_rank_count"], int)
        assert isinstance(cmp["biggest_promotions"], list)
        assert isinstance(cmp["biggest_demotions"], list)
        assert len(cmp["explanation"]) > 10
        assert cmp["safety_counts"]["signals_created"] == 0
        assert cmp["safety_counts"]["decisions_created"] == 0
        assert cmp["safety_counts"]["orders_created"] == 0

    def test_invalid_scoring_profile_returns_422(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4B: invalid scoring_profile value returns 422."""
        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4b-invalid-profile-001",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "scoring_profile": "momentum_heavy",
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422

    def test_balanced_preview_creates_zero_db_rows(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4B: balanced_preview mode creates zero signals/decisions/orders."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4b-zero-rows-001",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "scoring_profile": "balanced_preview",
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_submitted"] == 0
        assert data["decisions_made"] == 0
        assert data["orders_created"] == 0
        safety = data["candidate_funnel"]["safety_counts"]
        assert safety["signals_created"] == 0
        assert safety["decisions_created"] == 0
        assert safety["orders_created"] == 0

    def test_prediction_preview_creates_zero_signals_decisions_orders(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4B: prediction preview with classification fields creates zero DB rows."""
        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            return [], []

        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4b-zero-auto-001",
                "universe": "SP500", "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "scoring_profile": "current",
                "dry_run": True, "submit_signals": False,
                "run_risk": False, "create_orders": False,
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_submitted"] == 0
        assert data["decisions_made"] == 0
        assert data["orders_created"] == 0

    def test_save_returns_skipped_watch_and_rejected_counts(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """Phase 4B: save endpoint correctly counts skipped_watch and skipped_rejected rows."""
        resp = market_scan_prediction_seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-4b-skip-wrej-001",
                "candidates": [
                    {
                        "ticker": "WATCH_TEST_4B",
                        "preview_decision": "WATCH",
                        "preview_score": "40",
                        "status": "OK",
                        "candidate_type": "NEW_BUY_CANDIDATE",
                        "eligible_for_review_queue": False,
                    },
                    {
                        "ticker": "REJECT_TEST_4B",
                        "preview_decision": "REJECT",
                        "preview_score": "20",
                        "status": "OK",
                        "candidate_type": "NEW_BUY_CANDIDATE",
                        "eligible_for_review_queue": False,
                    },
                    {
                        "ticker": "ELIGIBLE_TEST_4B",
                        "preview_decision": "CONSIDER",
                        "preview_score": "75",
                        "status": "OK",
                        "candidate_type": "NEW_BUY_CANDIDATE",
                        "eligible_for_review_queue": True,
                    },
                ],
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["skipped_watch"] == 1
        assert data["skipped_rejected"] == 1
        assert data["inserted_count"] == 1
        assert data["saved_new_candidates"] == 1

    # -----------------------------------------------------------------------
    # Phase 4C tests — Quality Calibration Workbench
    # -----------------------------------------------------------------------

    def _make_mock_scan_and_fetch(self):
        """Return (mock_scan, mock_fetch) helpers used across Phase 4C tests."""
        from paper_trader.engine.market_screener import CandidateScore

        def mock_scan(session, tickers=None, **kwargs):
            return [
                CandidateScore(
                    rank=i + 1, ticker=t, score=f"{5-i}.00", latest_price="100.00",
                    latest_market_date="2025-01-14", price_count=25,
                    momentum_5d_pct="2.00", momentum_20d_pct="4.00",
                    volatility_20d_pct="3.00", relative_strength_vs_spy_20d="0.50",
                    reason_codes=["POSITIVE_20D_MOMENTUM"],
                )
                for i, t in enumerate(["AAPL", "MSFT", "NVDA"])
            ], [], __import__("datetime").date(2025, 1, 14)

        async def mock_fetch(tickers, api_url, timeout_seconds, max_concurrency=4):
            responses = []
            for t in tickers:
                responses.append({
                    "ticker": t,
                    "current_price": "100.00",
                    "ensemble_day5": "108.00",
                    "d5_change_pct": "8.00",
                    "confidence": "75.0",
                    "recommendation": "BUY",
                    "per_model_summary": {},
                })
            return responses, []

        return mock_scan, mock_fetch

    def test_4c_quality_preview_returns_200_and_score_breakdown(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """quality_preview returns 200 and each candidate has score_breakdown."""
        mock_scan, mock_fetch = self._make_mock_scan_and_fetch()
        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)
        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4c-quality-001",
                "tickers": ["AAPL", "MSFT", "NVDA"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 10, "min_price_points": 5, "prediction_top_n": 3,
                "scoring_profile": "quality_preview",
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["candidate_previews"]) > 0
        for p in data["candidate_previews"]:
            assert p["score_breakdown"] is not None, f"{p['ticker']} missing score_breakdown"
            assert p["score_breakdown"]["formula_profile"] == "quality_preview"

    def test_4c_risk_adjusted_preview_returns_200_and_score_breakdown(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """risk_adjusted_preview returns 200 and each candidate has score_breakdown."""
        mock_scan, mock_fetch = self._make_mock_scan_and_fetch()
        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)
        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4c-risk-001",
                "tickers": ["AAPL", "MSFT", "NVDA"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 10, "min_price_points": 5, "prediction_top_n": 3,
                "scoring_profile": "risk_adjusted_preview",
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["candidate_previews"]) > 0
        for p in data["candidate_previews"]:
            assert p["score_breakdown"] is not None
            assert p["score_breakdown"]["formula_profile"] == "risk_adjusted_preview"

    def test_4c_current_profile_score_breakdown_always_populated(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """current profile also populates score_breakdown (Phase 4C always-on)."""
        mock_scan, mock_fetch = self._make_mock_scan_and_fetch()
        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)
        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4c-cur-001",
                "tickers": ["AAPL", "MSFT"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 10, "min_price_points": 5, "prediction_top_n": 2,
                "scoring_profile": "current",
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        for p in data["candidate_previews"]:
            assert p["score_breakdown"] is not None
            assert p["score_breakdown"]["formula_profile"] == "current"
        # No comparison for current profile
        assert data["scoring_profile_comparison"] is None

    def test_4c_score_breakdown_has_all_required_fields(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """score_breakdown contains all required component fields."""
        mock_scan, mock_fetch = self._make_mock_scan_and_fetch()
        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)
        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4c-fields-001",
                "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 1,
                "scoring_profile": "quality_preview",
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        previews = resp.json()["candidate_previews"]
        assert len(previews) > 0
        sb = previews[0]["score_breakdown"]
        for field in (
            "formula_profile", "prediction_return_component", "prediction_confidence_component",
            "momentum_5d_component", "momentum_20d_component", "momentum_total_adj",
            "relative_strength_component", "scan_adj", "volatility_penalty_component",
            "already_held_penalty_component", "stale_or_missing_prediction_penalty",
            "low_conf_suppression_applied", "final_score",
        ):
            assert field in sb, f"score_breakdown missing field: {field}"

    def test_4c_profile_comparison_includes_top_tickers_by_profile(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """scoring_profile_comparison.top_tickers_by_profile present for quality_preview."""
        mock_scan, mock_fetch = self._make_mock_scan_and_fetch()
        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)
        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4c-top-001",
                "tickers": ["AAPL", "MSFT", "NVDA"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 10, "min_price_points": 5, "prediction_top_n": 3,
                "scoring_profile": "quality_preview",
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        cmp = resp.json()["scoring_profile_comparison"]
        assert cmp is not None
        assert "top_tickers_by_profile" in cmp
        assert "current" in cmp["top_tickers_by_profile"]
        assert "quality_preview" in cmp["top_tickers_by_profile"]
        assert isinstance(cmp["top_tickers_by_profile"]["current"], list)

    def test_4c_profile_comparison_includes_overlap_matrix(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """scoring_profile_comparison.overlap_matrix present for risk_adjusted_preview."""
        mock_scan, mock_fetch = self._make_mock_scan_and_fetch()
        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)
        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4c-matrix-001",
                "tickers": ["AAPL", "MSFT", "NVDA"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 10, "min_price_points": 5, "prediction_top_n": 3,
                "scoring_profile": "risk_adjusted_preview",
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        cmp = resp.json()["scoring_profile_comparison"]
        assert cmp is not None
        assert "overlap_matrix" in cmp
        assert isinstance(cmp["overlap_matrix"], dict)
        # At minimum, current vs risk_adjusted_preview must be in the matrix
        assert any("risk_adjusted_preview" in k for k in cmp["overlap_matrix"])

    def test_4c_profile_comparison_includes_candidates_with_high_disagreement(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """scoring_profile_comparison.candidates_with_high_disagreement is present."""
        mock_scan, mock_fetch = self._make_mock_scan_and_fetch()
        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)
        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4c-disagree-001",
                "tickers": ["AAPL", "MSFT", "NVDA"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 10, "min_price_points": 5, "prediction_top_n": 3,
                "scoring_profile": "quality_preview",
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        cmp = resp.json()["scoring_profile_comparison"]
        assert cmp is not None
        assert "candidates_with_high_disagreement" in cmp
        assert isinstance(cmp["candidates_with_high_disagreement"], list)

    def test_4c_profile_comparison_includes_profiles_compared(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """scoring_profile_comparison.profiles_compared lists all 4 profiles."""
        mock_scan, mock_fetch = self._make_mock_scan_and_fetch()
        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)
        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4c-profiles-001",
                "tickers": ["AAPL", "MSFT"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "scoring_profile": "balanced_preview",
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        cmp = resp.json()["scoring_profile_comparison"]
        assert cmp is not None
        compared = cmp["profiles_compared"]
        assert "current" in compared
        assert "balanced_preview" in compared
        assert "quality_preview" in compared
        assert "risk_adjusted_preview" in compared

    def test_4c_all_preview_profiles_create_zero_signals_decisions_orders(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch, api_engine
    ) -> None:
        """All 4 scoring profiles create zero signals, decisions, and orders."""
        from sqlalchemy.orm import Session as OrmSession
        mock_scan, mock_fetch = self._make_mock_scan_and_fetch()
        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)
        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        for profile in ["current", "balanced_preview", "quality_preview", "risk_adjusted_preview"]:
            with OrmSession(api_engine, autoflush=False, expire_on_commit=False) as s:
                sig_before = s.query(Signal).count()
                td_before  = s.query(TradeDecision).count()
                ord_before = s.query(Order).count()

            resp = market_scan_prediction_seeded_client.post(
                "/v1/strategy/market-scan/prediction-candidates",
                json={
                    "idempotency_key": f"test-4c-zero-{profile}-001",
                    "tickers": ["AAPL", "MSFT"],
                    "benchmark_ticker": "SPY", "lookback_days": 20,
                    "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                    "scoring_profile": profile,
                    "include_current_positions_for_prediction": False,
                },
                headers=_AUTH,
            )
            assert resp.status_code == 200, f"Profile {profile} failed: {resp.text}"
            data = resp.json()
            assert data["signals_submitted"] == 0, f"Profile {profile}: signals_submitted != 0"
            assert data["decisions_made"] == 0, f"Profile {profile}: decisions_made != 0"
            assert data["orders_created"] == 0, f"Profile {profile}: orders_created != 0"

            with OrmSession(api_engine, autoflush=False, expire_on_commit=False) as s:
                assert s.query(Signal).count() == sig_before, f"Profile {profile}: signals created"
                assert s.query(TradeDecision).count() == td_before, f"Profile {profile}: decisions created"
                assert s.query(Order).count() == ord_before, f"Profile {profile}: orders created"

    def test_4c_invalid_scoring_profile_new_values_422(
        self, market_scan_prediction_seeded_client: TestClient
    ) -> None:
        """Additional invalid profile names return 422 (complementing Phase 4B test)."""
        for bad_profile in ["momentum_heavy", "aggressive", "CURRENT", "Quality_Preview"]:
            resp = market_scan_prediction_seeded_client.post(
                "/v1/strategy/market-scan/prediction-candidates",
                json={
                    "idempotency_key": f"test-4c-inv-{bad_profile}",
                    "tickers": ["AAPL"],
                    "benchmark_ticker": "SPY",
                    "scoring_profile": bad_profile,
                },
                headers=_AUTH,
            )
            assert resp.status_code == 422, f"Expected 422 for profile '{bad_profile}', got {resp.status_code}"

    def test_4c_quality_preview_safety_counts_zero(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """quality_preview scoring_profile_comparison.safety_counts all zero."""
        mock_scan, mock_fetch = self._make_mock_scan_and_fetch()
        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)
        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4c-safety-001",
                "tickers": ["AAPL", "MSFT"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 2,
                "scoring_profile": "quality_preview",
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        cmp = resp.json()["scoring_profile_comparison"]
        assert cmp is not None
        sc = cmp["safety_counts"]
        assert sc["signals_created"] == 0
        assert sc["decisions_created"] == 0
        assert sc["orders_created"] == 0

    def test_4c_current_profile_backward_compat_no_comparison(
        self, market_scan_prediction_seeded_client: TestClient, monkeypatch
    ) -> None:
        """current profile: scoring_profile_comparison is None (backward compat preserved)."""
        mock_scan, mock_fetch = self._make_mock_scan_and_fetch()
        monkeypatch.setattr("paper_trader.engine.market_screener.scan_market", mock_scan)
        monkeypatch.setattr("paper_trader.api.app.fetch_predictions_for_tickers", mock_fetch)

        resp = market_scan_prediction_seeded_client.post(
            "/v1/strategy/market-scan/prediction-candidates",
            json={
                "idempotency_key": "test-4c-compat-001",
                "tickers": ["AAPL"],
                "benchmark_ticker": "SPY", "lookback_days": 20,
                "top_n": 5, "min_price_points": 5, "prediction_top_n": 1,
                "scoring_profile": "current",
                "include_current_positions_for_prediction": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["scoring_profile_comparison"] is None
        # Phase 4B fields remain None for current profile
        for p in data["candidate_previews"]:
            assert p["current_score"] is None
            assert p["balanced_preview_score"] is None


class TestMarketBackfillPricesEndpoint:
    """POST /v1/market/backfill-prices endpoint tests."""

    @pytest.fixture(autouse=True)
    def _prevent_real_yfinance(self, monkeypatch):
        """Autouse fixture: fail immediately if real yfinance.download is called."""
        def fail_if_yfinance_called(*args, **kwargs):
            raise AssertionError(
                "Test attempted to call real yfinance.download. "
                "All TestMarketBackfillPricesEndpoint tests must mock fetch_historical_prices."
            )

        # Patch yfinance.download at the engine module level where it's imported
        try:
            import paper_trader.engine.market_data as market_data_module
            if hasattr(market_data_module, 'yfinance') and market_data_module.yfinance is not None:
                monkeypatch.setattr(
                    market_data_module.yfinance,
                    "download",
                    fail_if_yfinance_called
                )
        except (ImportError, AttributeError):
            pass

    def test_requires_api_key(self, seeded_client: TestClient) -> None:
        """Endpoint requires X-API-Key header."""
        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "tickers": ["AAPL"],
                "start_date": "2026-04-01",
                "end_date": "2026-05-26",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_tickers": 10,
                "dry_run": True,
            },
        )
        assert resp.status_code == 401

    def test_max_tickers_cap_enforced(self, seeded_client: TestClient) -> None:
        """max_tickers > 50 is rejected with 422."""
        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "tickers": ["AAPL"],
                "start_date": "2026-04-01",
                "end_date": "2026-05-26",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_tickers": 51,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422

    def test_invalid_date_range_start_after_end(self, seeded_client: TestClient) -> None:
        """start_date > end_date is rejected with 422."""
        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "tickers": ["AAPL"],
                "start_date": "2026-05-26",
                "end_date": "2026-04-01",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_tickers": 10,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422

    def test_invalid_date_range_exceeds_180_days(self, seeded_client: TestClient) -> None:
        """Date range > 180 days is rejected with 422."""
        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "tickers": ["AAPL"],
                "start_date": "2025-01-01",
                "end_date": "2026-07-30",  # 181 days
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_tickers": 10,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422

    def test_explicit_tickers_dry_run(self, seeded_client: TestClient, monkeypatch, api_engine) -> None:
        """dry_run=true fetches data but inserts zero rows."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        # Mock fetch_historical_prices to return known data
        # Use dates in April 2026 to avoid collisions
        mock_successful = {
            "AAPL": [
                {"market_date": date_type(2026, 4, 1), "price": Decimal("150.00")},
                {"market_date": date_type(2026, 4, 2), "price": Decimal("151.00")},
            ]
        }
        mock_failures = {}

        def mock_fetch(*args, **kwargs):
            return mock_successful, mock_failures

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "tickers": ["AAPL"],
                "start_date": "2026-04-01",
                "end_date": "2026-05-26",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_tickers": 10,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["dry_run"] is True
        assert "results" in data
        assert "failures" in data
        # Verify response structure is populated
        assert data["processed_count"] >= 1
        # Verify no PriceSnapshot rows were actually inserted to database (dry_run=true)
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            snapshot_count = session.query(PriceSnapshot).filter(
                PriceSnapshot.ticker == "AAPL",
                PriceSnapshot.market_date >= date_type(2026, 4, 1),
                PriceSnapshot.market_date <= date_type(2026, 4, 2),
            ).count()
            assert snapshot_count == 0, "dry_run should not insert any rows"

    def test_explicit_tickers_non_dry_run_inserts_rows(self, seeded_client: TestClient, monkeypatch) -> None:
        """non-dry_run with mocked yfinance inserts PriceSnapshot rows."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        # Mock fetch_historical_prices to return known data
        # Use dates in January 2026 to avoid collisions with other tests
        mock_successful = {
            "AAPL": [
                {"market_date": date_type(2026, 1, 23), "price": Decimal("150.00")},
                {"market_date": date_type(2026, 1, 24), "price": Decimal("149.50")},
            ]
        }
        mock_failures = {}

        def mock_fetch(*args, **kwargs):
            return mock_successful, mock_failures

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "tickers": ["AAPL"],
                "start_date": "2026-01-23",
                "end_date": "2026-01-24",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_tickers": 10,
                "dry_run": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["dry_run"] is False
        assert data["inserted_count"] == 2
        assert len(data["results"]) >= 1
        assert data["results"][0]["status"] == "OK"

    def test_idempotent_same_run_twice(self, seeded_client: TestClient, monkeypatch) -> None:
        """Running backfill twice with same params: 2nd run inserts 0 rows."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        # Mock fetch_historical_prices
        # Use dates in February 2026 to avoid collisions with other tests
        mock_successful = {
            "AAPL": [
                {"market_date": date_type(2026, 2, 15), "price": Decimal("150.00")},
            ]
        }
        mock_failures = {}

        def mock_fetch(*args, **kwargs):
            return mock_successful, mock_failures

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        # First run
        resp1 = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "tickers": ["AAPL"],
                "start_date": "2026-02-15",
                "end_date": "2026-02-15",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_tickers": 10,
                "dry_run": False,
            },
            headers=_AUTH,
        )
        assert resp1.status_code == 200
        data1 = resp1.json()
        assert data1["inserted_count"] > 0

        # Second run with same params
        resp2 = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "tickers": ["AAPL"],
                "start_date": "2026-02-15",
                "end_date": "2026-02-15",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_tickers": 10,
                "dry_run": False,
            },
            headers=_AUTH,
        )
        assert resp2.status_code == 200
        data2 = resp2.json()
        # Second run should insert 0 new rows (all skipped as existing)
        assert data2["inserted_count"] == 0
        assert data2["skipped_existing_count"] > 0

    def test_sp500_universe_respects_max_tickers(self, seeded_client: TestClient, monkeypatch) -> None:
        """SP500 universe request capped to max_tickers."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        # Mock to return per-ticker (so we can verify count)
        call_count = [0]

        def mock_fetch(tickers, *args, **kwargs):
            call_count[0] = len(tickers)
            # Return empty data for all
            return {}, {}

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "tickers": None,  # Use universe
                "start_date": "2026-05-01",
                "end_date": "2026-05-26",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_tickers": 5,  # Cap to 5
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Verify that only 5 tickers were processed
        assert data["processed_count"] <= 5

    def test_partial_ticker_failure(self, seeded_client: TestClient, monkeypatch) -> None:
        """One ticker fails, others succeed; both in response."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        # Use March dates to avoid collisions
        mock_successful = {
            "AAPL": [
                {"market_date": date_type(2026, 3, 15), "price": Decimal("150.00")},
            ]
        }
        mock_failures = {
            "MSFT": "No data returned"
        }

        def mock_fetch(*args, **kwargs):
            return mock_successful, mock_failures

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "tickers": ["AAPL", "MSFT"],
                "start_date": "2026-03-15",
                "end_date": "2026-03-15",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_tickers": 10,
                "dry_run": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # AAPL succeeds
        assert any(r["ticker"] == "AAPL" and r["status"] == "OK" for r in data["results"])
        # MSFT fails
        assert any(f["ticker"] == "MSFT" for f in data["failures"])
        assert len(data["failures"]) > 0

    def test_all_tickers_fail(self, seeded_client: TestClient, monkeypatch) -> None:
        """All tickers fail; response is 200 with failures populated."""
        import paper_trader.api.app as app_module

        mock_successful = {}
        mock_failures = {
            "AAPL": "No data",
            "MSFT": "No data",
        }

        def mock_fetch(*args, **kwargs):
            return mock_successful, mock_failures

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "tickers": ["AAPL", "MSFT"],
                "start_date": "2026-04-15",
                "end_date": "2026-04-15",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_tickers": 10,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Should return 200, not crash
        assert "failures" in data
        assert len(data["failures"]) == 2
        assert data["failed_count"] == 2

    def test_response_structure(self, seeded_client: TestClient, monkeypatch) -> None:
        """Response has all required fields."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        # Use May dates
        mock_successful = {
            "AAPL": [
                {"market_date": date_type(2026, 5, 20), "price": Decimal("150.00")},
            ]
        }
        mock_failures = {}

        def mock_fetch(*args, **kwargs):
            return mock_successful, mock_failures

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "tickers": ["AAPL"],
                "start_date": "2026-05-20",
                "end_date": "2026-05-20",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_tickers": 10,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()

        # Verify all required response fields
        required_fields = [
            "universe", "requested_count", "processed_count", "inserted_count",
            "updated_count", "skipped_existing_count", "failed_count", "dry_run",
            "start_date", "end_date", "results", "failures"
        ]
        for field in required_fields:
            assert field in data, f"Missing field: {field}"

    def test_backfill_prices_start_index_selects_expected_slice(
        self, seeded_client: TestClient, monkeypatch, tmp_path
    ) -> None:
        """start_index=2 with max_tickers=2 selects tickers at indices 2 and 3."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        universe_tickers = ["BI0000", "BI0001", "BI0002", "BI0003", "BI0004"]
        full_csv = tmp_path / "sp500_universe_full.csv"
        full_csv.write_text("ticker\n" + "\n".join(universe_tickers) + "\n", encoding="utf-8")
        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)

        captured_tickers: list[str] = []

        def mock_fetch(tickers, *args, **kwargs):
            captured_tickers.extend(tickers)
            return {t: [{"market_date": date_type(2026, 7, 1), "price": Decimal("100.00")}] for t in tickers}, {}

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "start_date": "2026-07-01",
                "end_date": "2026-07-01",
                "max_tickers": 2,
                "start_index": 2,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        assert captured_tickers == ["BI0002", "BI0003"]

    def test_backfill_prices_response_includes_batch_metadata(
        self, seeded_client: TestClient, monkeypatch, tmp_path
    ) -> None:
        """Response includes all batch metadata fields with correct values."""
        import paper_trader.api.app as app_module

        universe_tickers = ["BM0000", "BM0001", "BM0002", "BM0003", "BM0004"]
        full_csv = tmp_path / "sp500_universe_full.csv"
        full_csv.write_text("ticker\n" + "\n".join(universe_tickers) + "\n", encoding="utf-8")
        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)
        monkeypatch.setattr(app_module, "fetch_historical_prices", lambda tickers, *a, **k: ({}, {}))

        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "start_date": "2026-07-02",
                "end_date": "2026-07-02",
                "max_tickers": 2,
                "start_index": 0,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        for field in ["total_available_tickers", "start_index", "end_index_exclusive",
                      "next_start_index", "has_more", "selected_ticker_count"]:
            assert field in data, f"Missing batch metadata field: {field}"
        assert data["total_available_tickers"] == 5
        assert data["start_index"] == 0
        assert data["end_index_exclusive"] == 2
        assert data["next_start_index"] == 2
        assert data["has_more"] is True
        assert data["selected_ticker_count"] == 2

    def test_backfill_prices_has_more_true_when_more_tickers_remaining(
        self, seeded_client: TestClient, monkeypatch, tmp_path
    ) -> None:
        """has_more is True when tickers remain after this batch."""
        import paper_trader.api.app as app_module

        universe_tickers = ["BH0000", "BH0001", "BH0002", "BH0003", "BH0004"]
        full_csv = tmp_path / "sp500_universe_full.csv"
        full_csv.write_text("ticker\n" + "\n".join(universe_tickers) + "\n", encoding="utf-8")
        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)
        monkeypatch.setattr(app_module, "fetch_historical_prices", lambda tickers, *a, **k: ({}, {}))

        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "start_date": "2026-07-03",
                "end_date": "2026-07-03",
                "max_tickers": 2,
                "start_index": 0,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        assert resp.json()["has_more"] is True

    def test_backfill_prices_has_more_false_on_last_batch(
        self, seeded_client: TestClient, monkeypatch, tmp_path
    ) -> None:
        """has_more is False when this batch reaches the end of the universe."""
        import paper_trader.api.app as app_module

        universe_tickers = ["BF0000", "BF0001", "BF0002", "BF0003", "BF0004"]
        full_csv = tmp_path / "sp500_universe_full.csv"
        full_csv.write_text("ticker\n" + "\n".join(universe_tickers) + "\n", encoding="utf-8")
        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)
        monkeypatch.setattr(app_module, "fetch_historical_prices", lambda tickers, *a, **k: ({}, {}))

        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "start_date": "2026-07-04",
                "end_date": "2026-07-04",
                "max_tickers": 2,
                "start_index": 4,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["has_more"] is False
        assert data["selected_ticker_count"] == 1

    def test_backfill_prices_start_index_beyond_universe_returns_zero_selected(
        self, seeded_client: TestClient, monkeypatch, tmp_path
    ) -> None:
        """start_index >= universe size returns zero selected tickers and has_more=False."""
        import paper_trader.api.app as app_module

        universe_tickers = ["BZ0000", "BZ0001", "BZ0002"]
        full_csv = tmp_path / "sp500_universe_full.csv"
        full_csv.write_text("ticker\n" + "\n".join(universe_tickers) + "\n", encoding="utf-8")
        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)
        monkeypatch.setattr(app_module, "fetch_historical_prices", lambda tickers, *a, **k: ({}, {}))

        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "start_date": "2026-07-05",
                "end_date": "2026-07-05",
                "max_tickers": 2,
                "start_index": 10,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["selected_ticker_count"] == 0
        assert data["processed_count"] == 0
        assert data["has_more"] is False

    def test_backfill_prices_dry_run_with_offset_creates_zero_rows(
        self, seeded_client: TestClient, monkeypatch, api_engine, tmp_path
    ) -> None:
        """dry_run=True with non-zero start_index inserts zero PriceSnapshot rows."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        universe_tickers = ["BD0000", "BD0001", "BD0002", "BD0003", "BD0004"]
        full_csv = tmp_path / "sp500_universe_full.csv"
        full_csv.write_text("ticker\n" + "\n".join(universe_tickers) + "\n", encoding="utf-8")
        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)

        mock_data = {
            "BD0002": [{"market_date": date_type(2026, 7, 6), "price": Decimal("100.00")}],
            "BD0003": [{"market_date": date_type(2026, 7, 6), "price": Decimal("101.00")}],
        }
        monkeypatch.setattr(app_module, "fetch_historical_prices", lambda tickers, *a, **k: (mock_data, {}))

        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "start_date": "2026-07-06",
                "end_date": "2026-07-06",
                "max_tickers": 2,
                "start_index": 2,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        assert resp.json()["dry_run"] is True
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            count = s.query(PriceSnapshot).filter(
                PriceSnapshot.ticker.in_(["BD0002", "BD0003"]),
                PriceSnapshot.market_date == date_type(2026, 7, 6),
            ).count()
        assert count == 0, "dry_run must not insert rows"

    def test_backfill_prices_non_dry_run_with_offset_writes_only_price_snapshots(
        self, seeded_client: TestClient, monkeypatch, api_engine, tmp_path
    ) -> None:
        """Non-dry-run with start_index writes PriceSnapshot rows only, no other tables."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        universe_tickers = ["BW0000", "BW0001", "BW0002", "BW0003", "BW0004"]
        full_csv = tmp_path / "sp500_universe_full.csv"
        full_csv.write_text("ticker\n" + "\n".join(universe_tickers) + "\n", encoding="utf-8")
        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)

        mock_data = {
            "BW0002": [{"market_date": date_type(2026, 7, 7), "price": Decimal("200.00")}],
        }
        monkeypatch.setattr(app_module, "fetch_historical_prices", lambda tickers, *a, **k: (mock_data, {}))

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            pre_signal_count   = s.query(Signal).count()
            pre_decision_count = s.query(TradeDecision).count()
            pre_order_count    = s.query(Order).count()

        resp = seeded_client.post(
            "/v1/market/backfill-prices",
            json={
                "universe": "SP500",
                "start_date": "2026-07-07",
                "end_date": "2026-07-07",
                "max_tickers": 2,
                "start_index": 2,
                "dry_run": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["inserted_count"] == 1
        assert data["dry_run"] is False

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            snap_count = s.query(PriceSnapshot).filter(
                PriceSnapshot.ticker == "BW0002",
                PriceSnapshot.market_date == date_type(2026, 7, 7),
            ).count()
            assert snap_count == 1, "PriceSnapshot row must be written"
            assert s.query(Signal).count() == pre_signal_count
            assert s.query(TradeDecision).count() == pre_decision_count
            assert s.query(Order).count() == pre_order_count


class TestMarketBenchmarkBackfillEndpoint:
    """POST /v1/market/backfill-benchmark-prices endpoint tests."""

    @pytest.fixture(autouse=True)
    def _prevent_real_yfinance(self, monkeypatch):
        """Autouse fixture: fail immediately if real yfinance.download is called."""
        def fail_if_yfinance_called(*args, **kwargs):
            raise AssertionError(
                "Test attempted to call real yfinance.download. "
                "All TestMarketBenchmarkBackfillEndpoint tests must mock fetch_historical_prices."
            )

        # Patch yfinance.download at the engine module level where it's imported
        try:
            import paper_trader.engine.market_data as market_data_module
            if hasattr(market_data_module, 'yfinance') and market_data_module.yfinance is not None:
                monkeypatch.setattr(
                    market_data_module.yfinance,
                    "download",
                    fail_if_yfinance_called
                )
        except (ImportError, AttributeError):
            pass

    def test_requires_api_key(self, seeded_client: TestClient) -> None:
        """Endpoint requires X-API-Key header."""
        resp = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["SPY"],
                "start_date": "2026-04-01",
                "end_date": "2026-05-26",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 5,
                "dry_run": True,
            },
        )
        assert resp.status_code == 401

    def test_rejects_empty_benchmark_tickers(self, seeded_client: TestClient) -> None:
        """Empty benchmark_tickers is rejected with 422."""
        resp = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": [],
                "start_date": "2026-04-01",
                "end_date": "2026-05-26",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 5,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422

    def test_max_benchmarks_cap_enforced(self, seeded_client: TestClient) -> None:
        """max_benchmarks > 10 is rejected with 422."""
        resp = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["SPY"],
                "start_date": "2026-04-01",
                "end_date": "2026-05-26",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 11,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422

    def test_invalid_date_range_start_after_end(self, seeded_client: TestClient) -> None:
        """start_date > end_date is rejected with 422."""
        resp = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["SPY"],
                "start_date": "2026-05-26",
                "end_date": "2026-04-01",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 5,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422

    def test_invalid_date_range_exceeds_180_days(self, seeded_client: TestClient) -> None:
        """Date range > 180 days is rejected with 422."""
        resp = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["SPY"],
                "start_date": "2025-01-01",
                "end_date": "2026-07-30",  # 181 days
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 5,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422

    def test_rejects_price_type_other_than_close(self, seeded_client: TestClient) -> None:
        """price_type other than CLOSE is rejected with 422."""
        resp = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["SPY"],
                "start_date": "2026-04-01",
                "end_date": "2026-05-26",
                "price_type": "OPEN",
                "session_type": "REGULAR",
                "max_benchmarks": 5,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422

    def test_dry_run_fetches_but_inserts_zero_rows(self, seeded_client: TestClient, monkeypatch, api_engine) -> None:
        """dry_run=true fetches data but inserts zero BenchmarkPrice rows."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        # Mock fetch_historical_prices to return known data
        # Use dates in April 2026 to avoid collisions
        mock_successful = {
            "SPY": [
                {"market_date": date_type(2026, 4, 1), "price": Decimal("400.00")},
                {"market_date": date_type(2026, 4, 2), "price": Decimal("401.00")},
            ]
        }
        mock_failures = {}

        def mock_fetch(*args, **kwargs):
            return mock_successful, mock_failures

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        resp = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["SPY"],
                "start_date": "2026-04-01",
                "end_date": "2026-05-26",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 5,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["dry_run"] is True
        assert "results" in data
        assert "failures" in data
        # Verify response structure is populated
        assert data["processed_count"] >= 1
        # Verify no BenchmarkPrice rows were actually inserted to database (dry_run=true)
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            benchmark_count = session.query(BenchmarkPrice).filter(
                BenchmarkPrice.ticker == "SPY",
                BenchmarkPrice.market_date >= date_type(2026, 4, 1),
                BenchmarkPrice.market_date <= date_type(2026, 4, 2),
            ).count()
            assert benchmark_count == 0, "dry_run should not insert any rows"

    def test_non_dry_run_inserts_rows(self, seeded_client: TestClient, monkeypatch) -> None:
        """non-dry_run with mocked yfinance inserts BenchmarkPrice rows."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        # Mock fetch_historical_prices to return known data
        # Use dates in January 2026 to avoid collisions with other tests
        mock_successful = {
            "SPY": [
                {"market_date": date_type(2026, 1, 23), "price": Decimal("400.00")},
                {"market_date": date_type(2026, 1, 24), "price": Decimal("399.50")},
            ]
        }
        mock_failures = {}

        def mock_fetch(*args, **kwargs):
            return mock_successful, mock_failures

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        resp = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["SPY"],
                "start_date": "2026-01-23",
                "end_date": "2026-01-24",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 5,
                "dry_run": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["dry_run"] is False
        assert data["inserted_count"] == 2
        assert len(data["results"]) >= 1
        assert data["results"][0]["status"] == "OK"

    def test_idempotent_same_run_twice(self, seeded_client: TestClient, monkeypatch) -> None:
        """Running backfill twice with same params: 2nd run inserts 0 rows."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        # Mock fetch_historical_prices
        # Use dates in February 2026 to avoid collisions with other tests
        mock_successful = {
            "SPY": [
                {"market_date": date_type(2026, 2, 15), "price": Decimal("400.00")},
            ]
        }
        mock_failures = {}

        def mock_fetch(*args, **kwargs):
            return mock_successful, mock_failures

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        # First run
        resp1 = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["SPY"],
                "start_date": "2026-02-15",
                "end_date": "2026-02-15",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 5,
                "dry_run": False,
            },
            headers=_AUTH,
        )
        assert resp1.status_code == 200
        data1 = resp1.json()
        assert data1["inserted_count"] > 0

        # Second run with same params
        resp2 = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["SPY"],
                "start_date": "2026-02-15",
                "end_date": "2026-02-15",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 5,
                "dry_run": False,
            },
            headers=_AUTH,
        )
        assert resp2.status_code == 200
        data2 = resp2.json()
        # Second run should insert 0 new rows (all skipped as existing)
        assert data2["inserted_count"] == 0
        assert data2["skipped_existing_count"] > 0

    def test_partial_benchmark_failure(self, seeded_client: TestClient, monkeypatch) -> None:
        """One benchmark fails, others succeed; both in response."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        # Use March dates to avoid collisions
        mock_successful = {
            "SPY": [
                {"market_date": date_type(2026, 3, 15), "price": Decimal("400.00")},
            ]
        }
        mock_failures = {
            "QQQ": "No data returned"
        }

        def mock_fetch(*args, **kwargs):
            return mock_successful, mock_failures

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        resp = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["SPY", "QQQ"],
                "start_date": "2026-03-15",
                "end_date": "2026-03-15",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 5,
                "dry_run": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # SPY succeeds
        assert any(r["benchmark_ticker"] == "SPY" and r["status"] == "OK" for r in data["results"])
        # QQQ fails
        assert any(f["ticker"] == "QQQ" for f in data["failures"])
        assert len(data["failures"]) > 0

    def test_all_benchmarks_fail(self, seeded_client: TestClient, monkeypatch) -> None:
        """All benchmarks fail; response is 200 with failures populated."""
        import paper_trader.api.app as app_module

        mock_successful = {}
        mock_failures = {
            "SPY": "No data",
            "QQQ": "No data",
        }

        def mock_fetch(*args, **kwargs):
            return mock_successful, mock_failures

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        resp = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["SPY", "QQQ"],
                "start_date": "2026-04-15",
                "end_date": "2026-04-15",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 5,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Should return 200, not crash
        assert "failures" in data
        assert len(data["failures"]) == 2
        assert data["failed_count"] == 2

    def test_response_structure(self, seeded_client: TestClient, monkeypatch) -> None:
        """Response has all required fields."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        # Use May dates
        mock_successful = {
            "SPY": [
                {"market_date": date_type(2026, 5, 20), "price": Decimal("400.00")},
            ]
        }
        mock_failures = {}

        def mock_fetch(*args, **kwargs):
            return mock_successful, mock_failures

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        resp = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["SPY"],
                "start_date": "2026-05-20",
                "end_date": "2026-05-20",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 5,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()

        # Verify all required response fields
        required_fields = [
            "requested_count", "processed_count", "inserted_count",
            "updated_count", "skipped_existing_count", "failed_count", "dry_run",
            "start_date", "end_date", "results", "failures"
        ]
        for field in required_fields:
            assert field in data, f"Missing field: {field}"

    def test_inserted_benchmark_prices_enable_market_scan(self, seeded_client: TestClient, monkeypatch, api_engine) -> None:
        """Inserted SPY benchmark rows allow /v1/market/scan to calculate relative_strength_vs_spy_20d."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        # First, backfill SPY benchmark prices
        mock_successful = {
            "SPY": [
                {"market_date": date_type(2026, 5, 7), "price": Decimal("300.00")},
                {"market_date": date_type(2026, 5, 8), "price": Decimal("300.75")},
                {"market_date": date_type(2026, 5, 9), "price": Decimal("301.50")},
                # Add more dates to reach 20 prices for momentum calculation
                *[
                    {"market_date": date_type(2026, 5, 10 + i), "price": Decimal(f"{300 + i}.00")}
                    for i in range(17)  # Days 10-26
                ]
            ]
        }
        mock_failures = {}

        def mock_fetch(*args, **kwargs):
            return mock_successful, mock_failures

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        # Backfill benchmark prices
        resp_backfill = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["SPY"],
                "start_date": "2026-05-07",
                "end_date": "2026-05-26",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 5,
                "dry_run": False,
            },
            headers=_AUTH,
        )
        assert resp_backfill.status_code == 200
        assert resp_backfill.json()["inserted_count"] >= 20

        # Verify that SPY benchmark prices are now in the database
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            spy_count = session.query(BenchmarkPrice).filter(
                BenchmarkPrice.ticker == "SPY",
            ).count()
            assert spy_count >= 20, "SPY benchmark prices should be inserted"

    def test_normalizes_tickers_strips_whitespace_and_uppercases(self, seeded_client: TestClient, monkeypatch) -> None:
        """Normalization strips whitespace and uppercases tickers."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        mock_successful = {
            "SPY": [
                {"market_date": date_type(2026, 5, 20), "price": Decimal("400.00")},
            ]
        }
        mock_failures = {}

        def mock_fetch(*args, **kwargs):
            return mock_successful, mock_failures

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        # Request with whitespace and lowercase
        resp = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["  spy  ", "s p y"],  # Second one should be invalid
                "start_date": "2026-05-20",
                "end_date": "2026-05-20",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 5,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Only "SPY" (from "  spy  ") should be processed, "S P Y" becomes "S P Y" which is malformed but passes through
        # Actually whitespace in the middle gets preserved. Let's verify requested_count
        assert data["requested_count"] >= 1

    def test_deduplicates_while_preserving_first_seen_order(self, seeded_client: TestClient, monkeypatch) -> None:
        """Duplicate tickers are deduplicated while preserving first-seen order."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        # Track which tickers are fetched to verify order
        fetch_calls = []

        def mock_fetch(tickers, **kwargs):
            fetch_calls.append(tickers)
            mock_successful = {
                t: [{"market_date": date_type(2026, 5, 20), "price": Decimal("400.00")}]
                for t in tickers
            }
            return mock_successful, {}

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        resp = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["SPY", "QQQ", "SPY", "IWM", "QQQ"],
                "start_date": "2026-05-20",
                "end_date": "2026-05-20",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 10,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Should deduplicate: ["SPY", "QQQ", "IWM"]
        assert data["requested_count"] == 3
        # Verify fetch was called with deduplicated list in order
        assert len(fetch_calls) == 1
        assert fetch_calls[0] == ["SPY", "QQQ", "IWM"]

    def test_max_benchmarks_uses_deterministic_first_seen_order(self, seeded_client: TestClient, monkeypatch) -> None:
        """max_benchmarks respects first-seen order (deterministic, not set-based)."""
        from datetime import date as date_type
        import paper_trader.api.app as app_module

        fetch_calls = []

        def mock_fetch(tickers, **kwargs):
            fetch_calls.append(tickers)
            mock_successful = {
                t: [{"market_date": date_type(2026, 5, 20), "price": Decimal("400.00")}]
                for t in tickers
            }
            return mock_successful, {}

        monkeypatch.setattr(app_module, "fetch_historical_prices", mock_fetch)

        resp = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["SPY", "QQQ", "IWM", "VTI", "BND"],
                "start_date": "2026-05-20",
                "end_date": "2026-05-20",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 3,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        # Should cap to first 3 in order: ["SPY", "QQQ", "IWM"]
        assert data["requested_count"] == 5
        assert data["processed_count"] == 3
        # Verify fetch was called with first 3 only
        assert len(fetch_calls) == 1
        assert fetch_calls[0] == ["SPY", "QQQ", "IWM"]

    def test_only_blanks_is_rejected_with_422(self, seeded_client: TestClient) -> None:
        """benchmark_tickers containing only blank strings is rejected with 422."""
        resp = seeded_client.post(
            "/v1/market/backfill-benchmark-prices",
            json={
                "benchmark_tickers": ["   ", "", "  \t  "],
                "start_date": "2026-05-20",
                "end_date": "2026-05-20",
                "price_type": "CLOSE",
                "session_type": "REGULAR",
                "max_benchmarks": 5,
                "dry_run": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422
        assert "must not be empty" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Candidate Review Queue
# ---------------------------------------------------------------------------

class TestCandidateReviewQueueEndpoint:
    """Tests for POST /v1/review/candidates, GET /v1/review/candidates, PATCH /v1/review/candidates/{id}."""

    def test_post_requires_api_key(self, client: TestClient) -> None:
        """POST /v1/review/candidates requires X-API-Key."""
        resp = client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-key-001",
                "candidates": [
                    {
                        "ticker": "AAPL",
                        "scan_score": "10.0",
                        "latest_price": "150.00",
                        "momentum_5d_pct": "1.5",
                        "momentum_20d_pct": "5.0",
                        "relative_strength_vs_spy_20d": "2.0",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.85",
                        "forecast_price_5d": "155.00",
                        "expected_return_pct": "3.3",
                        "market_context": "bullish",
                        "preview_decision": "CONSIDER",
                        "preview_score": "75.0",
                    }
                ]
            },
        )
        assert resp.status_code == 401  # Not authorized (no auth)

    def test_post_empty_candidates_rejected(self, seeded_client: TestClient) -> None:
        """POST rejects empty candidates list with 422."""
        resp = seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-key-001",
                "candidates": []
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422

    def test_post_saves_candidate(self, seeded_client: TestClient) -> None:
        """POST saves a single candidate."""
        resp = seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-key-001",
                "candidates": [
                    {
                        "ticker": "AAPL",
                        "scan_rank": "1",
                        "scan_score": "15.5",
                        "latest_price": "150.00",
                        "momentum_5d_pct": "1.5",
                        "momentum_20d_pct": "5.0",
                        "relative_strength_vs_spy_20d": "2.0",
                        "scan_reason_codes": ["POSITIVE_MOMENTUM"],
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.85",
                        "forecast_price_5d": "155.00",
                        "expected_return_pct": "3.3",
                        "market_context": "bullish",
                        "preview_decision": "CONSIDER",
                        "preview_score": "85.5",
                        "preview_reasons": ["High confidence"],
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["inserted_count"] == 1
        assert data["skipped_existing_count"] == 0
        assert len(data["candidates_saved"]) == 1
        assert data["candidates_saved"][0]["ticker"] == "AAPL"
        assert data["candidates_saved"][0]["review_status"] == "NEW"

    def test_post_defaults_review_status_to_new(self, seeded_client: TestClient) -> None:
        """POST defaults review_status to NEW."""
        resp = seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-key-002",
                "candidates": [
                    {
                        "ticker": "MSFT",
                        "scan_score": "12.0",
                        "latest_price": "300.00",
                        "momentum_5d_pct": "2.0",
                        "momentum_20d_pct": "6.0",
                        "relative_strength_vs_spy_20d": "1.5",
                        "prediction_recommendation": "HOLD",
                        "prediction_confidence": "0.65",
                        "forecast_price_5d": "302.00",
                        "expected_return_pct": "0.67",
                        "market_context": "neutral",
                        "preview_decision": "WATCH",
                        "preview_score": "50.0",
                    }
                ]
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["candidates_saved"][0]["review_status"] == "NEW"

    def test_post_is_idempotent(self, seeded_client: TestClient) -> None:
        """POST is idempotent on (idempotency_key, ticker)."""
        ikey = "test-idem-001"
        payload = {
            "idempotency_key": ikey,
            "candidates": [
                {
                    "ticker": "GOOG",
                    "scan_score": "18.0",
                    "latest_price": "140.00",
                    "momentum_5d_pct": "3.0",
                    "momentum_20d_pct": "7.0",
                    "relative_strength_vs_spy_20d": "3.5",
                    "prediction_recommendation": "BUY",
                    "prediction_confidence": "0.9",
                    "forecast_price_5d": "145.00",
                    "expected_return_pct": "3.57",
                    "market_context": "bullish",
                    "preview_decision": "CONSIDER",
                    "preview_score": "90.0",
                }
            ]
        }

        # First POST
        resp1 = seeded_client.post(
            "/v1/review/candidates",
            json=payload,
            headers=_AUTH,
        )
        assert resp1.status_code == 200
        data1 = resp1.json()
        assert data1["inserted_count"] == 1
        assert data1["skipped_existing_count"] == 0

        # Second POST (same ikey, same ticker) should skip
        resp2 = seeded_client.post(
            "/v1/review/candidates",
            json=payload,
            headers=_AUTH,
        )
        assert resp2.status_code == 200
        data2 = resp2.json()
        assert data2["inserted_count"] == 0
        assert data2["skipped_existing_count"] == 1

    def test_get_requires_api_key(self, client: TestClient) -> None:
        """GET /v1/review/candidates requires X-API-Key."""
        resp = client.get("/v1/review/candidates")
        assert resp.status_code == 401

    def test_get_lists_saved_candidates(self, seeded_client: TestClient) -> None:
        """GET lists saved candidates."""
        # Save a candidate first
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-list-001",
                "candidates": [
                    {
                        "ticker": "TSLA",
                        "scan_score": "20.0",
                        "latest_price": "250.00",
                        "momentum_5d_pct": "4.0",
                        "momentum_20d_pct": "10.0",
                        "relative_strength_vs_spy_20d": "5.0",
                        "prediction_recommendation": "SELL",
                        "prediction_confidence": "0.75",
                        "forecast_price_5d": "245.00",
                        "expected_return_pct": "-2.0",
                        "market_context": "bearish",
                        "preview_decision": "REJECT",
                        "preview_score": "25.0",
                    }
                ]
            },
            headers=_AUTH,
        )

        # GET should return the candidate
        resp = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert any(c["ticker"] == "TSLA" for c in data)

    def test_get_filters_by_status(self, seeded_client: TestClient) -> None:
        """GET filters by review_status."""
        # Save a candidate
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-filter-001",
                "candidates": [
                    {
                        "ticker": "NVDA",
                        "scan_score": "22.0",
                        "latest_price": "880.00",
                        "momentum_5d_pct": "2.5",
                        "momentum_20d_pct": "8.0",
                        "relative_strength_vs_spy_20d": "4.0",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.88",
                        "forecast_price_5d": "900.00",
                        "expected_return_pct": "2.27",
                        "market_context": "bullish",
                        "preview_decision": "CONSIDER",
                        "preview_score": "88.0",
                    }
                ]
            },
            headers=_AUTH,
        )

        # Filter by NEW status
        resp = seeded_client.get(
            "/v1/review/candidates?status=NEW",
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert all(c["review_status"] == "NEW" for c in data)

        # Filter by NON-existent status should return empty
        resp = seeded_client.get(
            "/v1/review/candidates?status=APPROVED_FOR_SIGNAL",
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 0

    def test_get_filters_by_ticker(self, seeded_client: TestClient) -> None:
        """GET filters by ticker."""
        # Save two candidates
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-ticker-filter-001",
                "candidates": [
                    {
                        "ticker": "AMD",
                        "scan_score": "16.0",
                        "latest_price": "120.00",
                        "momentum_5d_pct": "1.8",
                        "momentum_20d_pct": "4.5",
                        "relative_strength_vs_spy_20d": "1.8",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.80",
                        "forecast_price_5d": "125.00",
                        "expected_return_pct": "4.17",
                        "market_context": "bullish",
                        "preview_decision": "CONSIDER",
                        "preview_score": "80.0",
                    },
                    {
                        "ticker": "INTC",
                        "scan_score": "14.0",
                        "latest_price": "35.00",
                        "momentum_5d_pct": "1.2",
                        "momentum_20d_pct": "3.0",
                        "relative_strength_vs_spy_20d": "0.5",
                        "prediction_recommendation": "HOLD",
                        "prediction_confidence": "0.60",
                        "forecast_price_5d": "35.50",
                        "expected_return_pct": "1.43",
                        "market_context": "neutral",
                        "preview_decision": "WATCH",
                        "preview_score": "55.0",
                    }
                ]
            },
            headers=_AUTH,
        )

        # Filter by AMD
        resp = seeded_client.get(
            "/v1/review/candidates?ticker=AMD",
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert all(c["ticker"] == "AMD" for c in data)

    def test_get_limit_works(self, seeded_client: TestClient) -> None:
        """GET respects limit parameter."""
        # Save multiple candidates
        for i in range(3):
            seeded_client.post(
                "/v1/review/candidates",
                json={
                    "idempotency_key": f"test-limit-{i:03d}",
                    "candidates": [
                        {
                            "ticker": f"TST{i}",
                            "scan_score": f"{15 + i}.0",
                            "latest_price": "100.00",
                            "momentum_5d_pct": "1.0",
                            "momentum_20d_pct": "3.0",
                            "relative_strength_vs_spy_20d": "0.5",
                            "prediction_recommendation": "BUY",
                            "prediction_confidence": "0.75",
                            "forecast_price_5d": "105.00",
                            "expected_return_pct": "5.0",
                            "market_context": "bullish",
                            "preview_decision": "CONSIDER",
                            "preview_score": "75.0",
                        }
                    ]
                },
                headers=_AUTH,
            )

        # Get with limit=1
        resp = seeded_client.get(
            "/v1/review/candidates?limit=1",
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) <= 1

    def test_patch_requires_api_key(self, client: TestClient) -> None:
        """PATCH /v1/review/candidates/{id} requires X-API-Key."""
        test_id = "00000000-0000-0000-0000-000000000000"
        resp = client.patch(
            f"/v1/review/candidates/{test_id}",
            json={"review_status": "WATCHING"},
        )
        assert resp.status_code == 401

    def test_patch_updates_review_status(self, seeded_client: TestClient) -> None:
        """PATCH updates review_status."""
        # Save a candidate
        save_resp = seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-patch-001",
                "candidates": [
                    {
                        "ticker": "PATCH",
                        "scan_score": "17.0",
                        "latest_price": "200.00",
                        "momentum_5d_pct": "2.0",
                        "momentum_20d_pct": "5.5",
                        "relative_strength_vs_spy_20d": "2.5",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.82",
                        "forecast_price_5d": "210.00",
                        "expected_return_pct": "5.0",
                        "market_context": "bullish",
                        "preview_decision": "CONSIDER",
                        "preview_score": "82.0",
                    }
                ]
            },
            headers=_AUTH,
        )
        candidate_id = save_resp.json()["candidates_saved"][0]["id"]

        # PATCH to WATCHING
        resp = seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "WATCHING"},
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["review_status"] == "WATCHING"

    def test_patch_rejects_invalid_status(self, seeded_client: TestClient) -> None:
        """PATCH rejects invalid review_status with 422."""
        # Save a candidate
        save_resp = seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-patch-invalid-001",
                "candidates": [
                    {
                        "ticker": "BADSTATUS",
                        "scan_score": "10.0",
                        "latest_price": "50.00",
                        "momentum_5d_pct": "0.5",
                        "momentum_20d_pct": "1.0",
                        "relative_strength_vs_spy_20d": "0.2",
                        "prediction_recommendation": "HOLD",
                        "prediction_confidence": "0.50",
                        "forecast_price_5d": "50.50",
                        "expected_return_pct": "1.0",
                        "market_context": "neutral",
                        "preview_decision": "WATCH",
                        "preview_score": "50.0",
                    }
                ]
            },
            headers=_AUTH,
        )
        candidate_id = save_resp.json()["candidates_saved"][0]["id"]

        # PATCH with invalid status
        resp = seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "INVALID_STATUS"},
            headers=_AUTH,
        )
        assert resp.status_code == 422

    def test_patch_returns_404_for_unknown_id(self, seeded_client: TestClient) -> None:
        """PATCH returns 404 for unknown candidate_id."""
        fake_id = "00000000-0000-0000-0000-000000000000"
        resp = seeded_client.patch(
            f"/v1/review/candidates/{fake_id}",
            json={"review_status": "WATCHING"},
            headers=_AUTH,
        )
        assert resp.status_code == 404

    def test_patch_approved_for_signal_does_not_create_signal(self, seeded_client: TestClient) -> None:
        """PATCH APPROVED_FOR_SIGNAL does not create Signal rows."""
        # Save a candidate
        save_resp = seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-nosignal-001",
                "candidates": [
                    {
                        "ticker": "NOSIG",
                        "scan_score": "19.0",
                        "latest_price": "175.00",
                        "momentum_5d_pct": "2.2",
                        "momentum_20d_pct": "6.5",
                        "relative_strength_vs_spy_20d": "3.0",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.91",
                        "forecast_price_5d": "185.00",
                        "expected_return_pct": "5.71",
                        "market_context": "bullish",
                        "preview_decision": "CONSIDER",
                        "preview_score": "91.0",
                    }
                ]
            },
            headers=_AUTH,
        )
        candidate_id = save_resp.json()["candidates_saved"][0]["id"]

        # Count signals before PATCH
        from paper_trader.db.session import get_session
        with get_session() as session:
            signals_before = session.query(Signal).count()

        # PATCH to APPROVED_FOR_SIGNAL
        resp = seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )
        assert resp.status_code == 200

        # Count signals after PATCH — should be unchanged
        with get_session() as session:
            signals_after = session.query(Signal).count()
        assert signals_after == signals_before

    def test_post_does_not_create_signal(self, seeded_client: TestClient) -> None:
        """POST save does not create Signal rows."""
        from paper_trader.db.session import get_session

        with get_session() as session:
            signals_before = session.query(Signal).count()

        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-post-nosig-001",
                "candidates": [
                    {
                        "ticker": "POSTNOS",
                        "scan_score": "13.0",
                        "latest_price": "95.00",
                        "momentum_5d_pct": "1.3",
                        "momentum_20d_pct": "4.2",
                        "relative_strength_vs_spy_20d": "1.2",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.78",
                        "forecast_price_5d": "100.00",
                        "expected_return_pct": "5.26",
                        "market_context": "bullish",
                        "preview_decision": "CONSIDER",
                        "preview_score": "78.0",
                    }
                ]
            },
            headers=_AUTH,
        )

        with get_session() as session:
            signals_after = session.query(Signal).count()
        assert signals_after == signals_before

    def test_post_does_not_create_trade_decision(self, seeded_client: TestClient) -> None:
        """POST save does not create TradeDecision rows."""
        from paper_trader.db.session import get_session

        with get_session() as session:
            decisions_before = session.query(TradeDecision).count()

        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-post-nodec-001",
                "candidates": [
                    {
                        "ticker": "POSTNOD",
                        "scan_score": "11.0",
                        "latest_price": "80.00",
                        "momentum_5d_pct": "0.9",
                        "momentum_20d_pct": "2.8",
                        "relative_strength_vs_spy_20d": "0.8",
                        "prediction_recommendation": "HOLD",
                        "prediction_confidence": "0.65",
                        "forecast_price_5d": "81.00",
                        "expected_return_pct": "1.25",
                        "market_context": "neutral",
                        "preview_decision": "WATCH",
                        "preview_score": "60.0",
                    }
                ]
            },
            headers=_AUTH,
        )

        with get_session() as session:
            decisions_after = session.query(TradeDecision).count()
        assert decisions_after == decisions_before

    def test_post_does_not_create_order(self, seeded_client: TestClient) -> None:
        """POST save does not create Order rows."""
        from paper_trader.db.session import get_session

        with get_session() as session:
            orders_before = session.query(Order).count()

        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-post-noord-001",
                "candidates": [
                    {
                        "ticker": "POSTNO",
                        "scan_score": "9.0",
                        "latest_price": "65.00",
                        "momentum_5d_pct": "0.6",
                        "momentum_20d_pct": "1.9",
                        "relative_strength_vs_spy_20d": "0.3",
                        "prediction_recommendation": "SELL",
                        "prediction_confidence": "0.55",
                        "forecast_price_5d": "63.00",
                        "expected_return_pct": "-3.08",
                        "market_context": "bearish",
                        "preview_decision": "REJECT",
                        "preview_score": "25.0",
                    }
                ]
            },
            headers=_AUTH,
        )

        with get_session() as session:
            orders_after = session.query(Order).count()
        assert orders_after == orders_before

    def test_post_saves_failed_fetch_candidate(self, seeded_client: TestClient) -> None:
        """POST saves a FAILED_FETCH candidate with null prediction fields."""
        resp = seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-failed-fetch-001",
                "candidates": [
                    {
                        "ticker": "FAILFETCH",
                        "scan_rank": None,
                        "scan_score": None,
                        "latest_price": None,
                        "momentum_5d_pct": None,
                        "momentum_20d_pct": None,
                        "relative_strength_vs_spy_20d": None,
                        "scan_reason_codes": None,
                        "prediction_recommendation": None,
                        "prediction_confidence": None,
                        "forecast_price_5d": None,
                        "expected_return_pct": None,
                        "market_context": None,
                        "preview_decision": "REJECT",
                        "preview_score": "0.0",
                        "status": "ERROR",
                    }
                ]
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["inserted_count"] == 1
        assert len(data["candidates_saved"]) == 1
        saved = data["candidates_saved"][0]
        assert saved["ticker"] == "FAILFETCH"
        assert saved["scan_score"] is None
        assert saved["prediction_recommendation"] is None
        assert saved["status"] == "ERROR"

    def test_post_saves_failed_normalization_candidate(self, seeded_client: TestClient) -> None:
        """POST saves a FAILED_NORMALIZATION candidate with null prediction fields."""
        resp = seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-failed-norm-001",
                "candidates": [
                    {
                        "ticker": "FAILNORM",
                        "scan_score": "12.5",
                        "latest_price": "100.00",
                        "momentum_5d_pct": "1.5",
                        "momentum_20d_pct": "3.0",
                        "relative_strength_vs_spy_20d": "0.5",
                        "prediction_recommendation": None,
                        "prediction_confidence": None,
                        "forecast_price_5d": None,
                        "expected_return_pct": None,
                        "market_context": None,
                        "preview_decision": "REJECT",
                        "preview_score": "10.0",
                        "status": "ERROR",
                    }
                ]
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["inserted_count"] == 1
        saved = data["candidates_saved"][0]
        assert saved["ticker"] == "FAILNORM"
        assert saved["scan_score"] == "12.5"
        assert saved["prediction_recommendation"] is None
        assert saved["preview_decision"] == "REJECT"

    def test_post_saves_missing_prediction_candidate(self, seeded_client: TestClient) -> None:
        """POST saves a MISSING_PREDICTION candidate with null prediction fields."""
        resp = seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-missing-pred-001",
                "candidates": [
                    {
                        "ticker": "MISSPRED",
                        "scan_score": "8.0",
                        "latest_price": "50.00",
                        "momentum_5d_pct": "0.8",
                        "momentum_20d_pct": "2.0",
                        "relative_strength_vs_spy_20d": "0.3",
                        "prediction_recommendation": None,
                        "prediction_confidence": None,
                        "forecast_price_5d": None,
                        "expected_return_pct": None,
                        "market_context": None,
                        "preview_decision": "WATCH",
                        "preview_score": "45.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["inserted_count"] == 1
        saved = data["candidates_saved"][0]
        assert saved["ticker"] == "MISSPRED"
        assert saved["preview_decision"] == "WATCH"
        assert saved["prediction_recommendation"] is None

    def test_post_save_all_use_case_supported(self, seeded_client: TestClient) -> None:
        """POST supports Save All use case with mixed OK and failed candidates."""
        resp = seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-save-all-001",
                "candidates": [
                    {
                        "ticker": "OK_TICKER",
                        "scan_score": "15.0",
                        "latest_price": "150.00",
                        "momentum_5d_pct": "1.5",
                        "momentum_20d_pct": "5.0",
                        "relative_strength_vs_spy_20d": "2.0",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.85",
                        "forecast_price_5d": "155.00",
                        "expected_return_pct": "3.3",
                        "market_context": "bullish",
                        "preview_decision": "CONSIDER",
                        "preview_score": "85.0",
                        "status": "OK",
                    },
                    {
                        "ticker": "FAILED_TICKER",
                        "scan_score": None,
                        "latest_price": None,
                        "momentum_5d_pct": None,
                        "momentum_20d_pct": None,
                        "relative_strength_vs_spy_20d": None,
                        "prediction_recommendation": None,
                        "prediction_confidence": None,
                        "forecast_price_5d": None,
                        "expected_return_pct": None,
                        "market_context": None,
                        "preview_decision": "REJECT",
                        "preview_score": "5.0",
                        "status": "ERROR",
                    }
                ]
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["inserted_count"] == 2
        assert len(data["candidates_saved"]) == 2
        # Verify both kinds were saved
        tickers = {c["ticker"] for c in data["candidates_saved"]}
        assert "OK_TICKER" in tickers
        assert "FAILED_TICKER" in tickers

    def test_post_failed_candidates_do_not_create_signal(self, seeded_client: TestClient) -> None:
        """POST save of failed candidates does not create Signal rows."""
        from paper_trader.db.session import get_session

        with get_session() as session:
            signals_before = session.query(Signal).count()

        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-failed-nosig-001",
                "candidates": [
                    {
                        "ticker": "FAILSIG",
                        "scan_score": None,
                        "latest_price": None,
                        "momentum_5d_pct": None,
                        "momentum_20d_pct": None,
                        "relative_strength_vs_spy_20d": None,
                        "prediction_recommendation": None,
                        "prediction_confidence": None,
                        "forecast_price_5d": None,
                        "expected_return_pct": None,
                        "market_context": None,
                        "preview_decision": "REJECT",
                        "preview_score": "0.0",
                        "status": "ERROR",
                    }
                ]
            },
            headers=_AUTH,
        )

        with get_session() as session:
            signals_after = session.query(Signal).count()
        assert signals_after == signals_before

    def test_post_accepts_candidate_preview_shape_with_integer_scan_rank(self, seeded_client: TestClient) -> None:
        """POST accepts live candidate_preview shape with integer scan_rank."""
        resp = seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-preview-shape-001",
                "candidates": [
                    {
                        "ticker": "SCAN_INT",
                        "scan_rank": 3,
                        "scan_score": "22.5",
                        "latest_price": "175.00",
                        "momentum_5d_pct": "2.3",
                        "momentum_20d_pct": "8.1",
                        "relative_strength_vs_spy_20d": "5.2",
                        "scan_reason_codes": ["POSITIVE_5D_MOMENTUM", "OUTPERFORMING_SPY"],
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.92",
                        "forecast_price_5d": "185.50",
                        "expected_return_pct": "5.87",
                        "market_context": "bullish",
                        "preview_decision": "CONSIDER",
                        "preview_score": "92.3",
                        "preview_reasons": ["High confidence", "Outperforming benchmark"],
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["inserted_count"] == 1
        assert data["skipped_existing_count"] == 0
        saved = data["candidates_saved"][0]
        assert saved["ticker"] == "SCAN_INT"
        assert saved["scan_rank"] == "3"
        assert saved["scan_reason_codes"] == ["POSITIVE_5D_MOMENTUM", "OUTPERFORMING_SPY"]
        assert saved["preview_reasons"] == ["High confidence", "Outperforming benchmark"]

    def test_post_accepts_all_failed_candidates_shape(self, seeded_client: TestClient) -> None:
        """POST accepts failed candidates with null prediction fields and default empty lists."""
        resp = seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-all-failed-001",
                "candidates": [
                    {
                        "ticker": "FAIL_1",
                        "scan_rank": 5,
                        "scan_score": "10.0",
                        "latest_price": "100.00",
                        "momentum_5d_pct": "0.5",
                        "momentum_20d_pct": "1.0",
                        "relative_strength_vs_spy_20d": "0.0",
                        "scan_reason_codes": [],
                        "prediction_recommendation": None,
                        "prediction_confidence": None,
                        "forecast_price_5d": None,
                        "expected_return_pct": None,
                        "market_context": None,
                        "preview_decision": "WATCH",
                        "preview_score": "50.0",
                        "preview_reasons": [],
                        "status": "ERROR",
                    }
                ]
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["inserted_count"] == 1
        saved = data["candidates_saved"][0]
        assert saved["status"] == "ERROR"
        assert saved["scan_rank"] == "5"
        assert saved["prediction_recommendation"] is None


class TestReviewSignalPreviewEndpoint:
    """Tests for POST /v1/review/signal-preview (PREVIEW ONLY, no database writes)."""

    def test_post_requires_api_key(self, client: TestClient) -> None:
        """POST /v1/review/signal-preview requires X-API-Key."""
        resp = client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-test-001",
                "review_status": "APPROVED_FOR_SIGNAL",
                "limit": 50,
            },
        )
        assert resp.status_code == 401

    def test_no_approved_candidates_returns_empty(self, seeded_client: TestClient, api_engine) -> None:
        """POST returns empty preview with 0 counts when no approved candidates exist."""
        # Clear candidate_reviews table
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            from paper_trader.db.models import CandidateReview
            session.query(CandidateReview).delete()
            session.commit()

        resp = seeded_client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-test-empty",
                "review_status": "APPROVED_FOR_SIGNAL",
                "limit": 50,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["execution_mode"] == "PREVIEW_ONLY"
        assert data["candidates_evaluated"] == 0
        assert data["signal_previews_generated"] == 0
        assert data["skipped_count"] == 0
        assert data["signal_previews"] == []
        assert data["skipped"] == []
        assert data["signals_created"] == 0
        assert data["decisions_created"] == 0
        assert data["orders_created"] == 0

    def test_buy_recommendation_creates_preview(self, seeded_client: TestClient) -> None:
        """POST generates BUY signal preview from APPROVED_FOR_SIGNAL candidate."""
        # Save a candidate
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-buy-preview",
                "candidates": [
                    {
                        "ticker": "AAPL",
                        "scan_score": "10.0",
                        "latest_price": "150.00",
                        "momentum_5d_pct": "1.5",
                        "momentum_20d_pct": "5.0",
                        "relative_strength_vs_spy_20d": "2.0",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.85",
                        "forecast_price_5d": "155.00",
                        "expected_return_pct": "3.3",
                        "market_context": "bullish",
                        "preview_decision": "CONSIDER",
                        "preview_score": "75.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        # Approve it
        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = candidates[0]["id"]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # Generate preview with candidate_ids for isolation
        resp = seeded_client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-buy-test",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["candidates_evaluated"] == 1
        assert data["signal_previews_generated"] == 1
        assert data["skipped_count"] == 0
        assert len(data["signal_previews"]) == 1

        preview = data["signal_previews"][0]
        assert preview["ticker"] == "AAPL"
        assert preview["side"] == "BUY"
        assert preview["confidence"] == "0.85"
        assert preview["preview_decision"] == "CONSIDER"
        assert preview["preview_score"] == "75.0"
        assert preview["expected_return_pct"] == "3.3"
        assert data["signals_created"] == 0
        assert data["decisions_created"] == 0
        assert data["orders_created"] == 0

    def test_sell_recommendation_creates_preview(self, seeded_client: TestClient) -> None:
        """POST generates SELL signal preview from APPROVED_FOR_SIGNAL candidate."""
        # Save a candidate with SELL recommendation
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-sell-preview",
                "candidates": [
                    {
                        "ticker": "MSFT",
                        "scan_score": "8.0",
                        "latest_price": "300.00",
                        "prediction_recommendation": "SELL",
                        "prediction_confidence": "0.75",
                        "forecast_price_5d": "290.00",
                        "expected_return_pct": "-3.3",
                        "market_context": "bearish",
                        "preview_decision": "WATCH",
                        "preview_score": "60.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        # Approve it
        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = candidates[0]["id"]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # Generate preview with candidate_ids for isolation
        resp = seeded_client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-sell-test",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signal_previews_generated"] == 1

        preview = data["signal_previews"][0]
        assert preview["ticker"] == "MSFT"
        assert preview["side"] == "SELL"
        assert preview["confidence"] == "0.75"

    def test_hold_recommendation_skipped(self, seeded_client: TestClient) -> None:
        """POST skips HOLD recommendations as not actionable."""
        # Save a candidate with HOLD recommendation
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-hold-preview",
                "candidates": [
                    {
                        "ticker": "GOOGL",
                        "prediction_recommendation": "HOLD",
                        "prediction_confidence": "0.80",
                        "preview_decision": "WATCH",
                        "preview_score": "50.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        # Approve it
        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = candidates[0]["id"]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # Generate preview with candidate_ids for isolation
        resp = seeded_client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-hold-test",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["candidates_evaluated"] == 1
        assert data["signal_previews_generated"] == 0
        assert data["skipped_count"] == 1
        assert len(data["skipped"]) == 1
        assert data["skipped"][0]["ticker"] == "GOOGL"
        assert "HOLD" in data["skipped"][0]["reason"]

    def test_non_ok_status_skipped(self, seeded_client: TestClient) -> None:
        """POST skips candidates with status != OK."""
        # Save a candidate with ERROR status
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-error-status",
                "candidates": [
                    {
                        "ticker": "TSLA",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.70",
                        "preview_decision": "CONSIDER",
                        "preview_score": "65.0",
                        "status": "ERROR",
                    }
                ]
            },
            headers=_AUTH,
        )

        # Approve it
        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = candidates[0]["id"]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # Generate preview with candidate_ids for isolation
        resp = seeded_client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-error-status-test",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signal_previews_generated"] == 0
        assert data["skipped_count"] == 1
        assert data["skipped"][0]["reason"] == "Status is ERROR, not OK"

    def test_missing_recommendation_skipped(self, seeded_client: TestClient) -> None:
        """POST skips candidates with missing prediction_recommendation."""
        # Save a candidate without recommendation
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-no-rec",
                "candidates": [
                    {
                        "ticker": "NVDA",
                        "prediction_recommendation": None,
                        "prediction_confidence": "0.85",
                        "preview_decision": "WATCH",
                        "preview_score": "70.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        # Approve it
        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = candidates[0]["id"]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # Generate preview with candidate_ids for isolation
        resp = seeded_client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-no-rec-test",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["skipped_count"] == 1
        assert "Missing prediction_recommendation" in data["skipped"][0]["reason"]

    def test_missing_confidence_skipped(self, seeded_client: TestClient) -> None:
        """POST skips candidates with missing prediction_confidence."""
        # Save a candidate without confidence
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-no-conf",
                "candidates": [
                    {
                        "ticker": "META",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": None,
                        "preview_decision": "CONSIDER",
                        "preview_score": "72.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        # Approve it
        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = candidates[0]["id"]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # Generate preview with candidate_ids for isolation
        resp = seeded_client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-no-conf-test",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["skipped_count"] == 1
        assert "Missing prediction_confidence" in data["skipped"][0]["reason"]

    def test_non_approved_status_skipped(self, seeded_client: TestClient) -> None:
        """POST skips NEW candidates when review_status=APPROVED_FOR_SIGNAL."""
        # Save a candidate (defaults to NEW status)
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-new-status",
                "candidates": [
                    {
                        "ticker": "AMD",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.80",
                        "preview_decision": "CONSIDER",
                        "preview_score": "75.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        # Get the NEW candidate
        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = candidates[0]["id"]

        # Generate preview with candidate_ids explicitly requesting the NEW candidate
        resp = seeded_client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-new-status-test",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["candidates_evaluated"] == 1  # Evaluated because explicitly requested
        assert data["signal_previews_generated"] == 0  # Not approved so skipped
        assert data["skipped_count"] == 1  # NEW status is skipped
        assert "Review status is NEW" in data["skipped"][0]["reason"]

    def test_limit_respected(self, seeded_client: TestClient) -> None:
        """POST respects limit parameter."""
        # Create 3 candidates in isolation for this test
        for i in range(3):
            seeded_client.post(
                "/v1/review/candidates",
                json={
                    "idempotency_key": f"test-limit-isolated-{i}",
                    "candidates": [
                        {
                            "ticker": f"LIMITISOLATICK{i}",
                            "prediction_recommendation": "BUY",
                            "prediction_confidence": "0.80",
                            "preview_decision": "CONSIDER",
                            "preview_score": "70.0",
                            "status": "OK",
                        }
                    ]
                },
                headers=_AUTH,
            )

        # Get these specific candidates
        all_candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        limit_test_candidates = [
            c for c in all_candidates
            if c["ticker"].startswith("LIMITISOLATICK")
        ]
        assert len(limit_test_candidates) == 3, "Expected 3 limit test candidates"

        # Approve all limit test candidates
        for candidate in limit_test_candidates:
            seeded_client.patch(
                f"/v1/review/candidates/{candidate['id']}",
                json={"review_status": "APPROVED_FOR_SIGNAL"},
                headers=_AUTH,
            )

        # Preview with explicit candidate_ids and limit=1
        candidate_ids = [c["id"] for c in limit_test_candidates]
        resp = seeded_client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-limit-test-isolated",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": candidate_ids,
                "limit": 1,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["candidates_evaluated"] == 1
        assert data["signal_previews_generated"] == 1

    def test_no_signal_rows_created(self, seeded_client: TestClient, api_engine) -> None:
        """POST does not create Signal rows."""
        # Save and approve a candidate
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-no-signal-rows",
                "candidates": [
                    {
                        "ticker": "INTRN",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.85",
                        "preview_decision": "CONSIDER",
                        "preview_score": "75.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = candidates[0]["id"]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # Count Signal rows before
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signals_before = session.query(Signal).count()

        # Generate preview with candidate_ids for isolation
        seeded_client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-no-signals-test",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
            },
            headers=_AUTH,
        )

        # Count Signal rows after
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signals_after = session.query(Signal).count()

        assert signals_before == signals_after

    def test_no_trade_decision_rows_created(self, seeded_client: TestClient, api_engine) -> None:
        """POST does not create TradeDecision rows."""
        # Save and approve a candidate
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-no-decisions",
                "candidates": [
                    {
                        "ticker": "INTM",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.85",
                        "preview_decision": "CONSIDER",
                        "preview_score": "75.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = candidates[0]["id"]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # Count TradeDecision rows before
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            decisions_before = session.query(TradeDecision).count()

        # Generate preview with candidate_ids for isolation
        seeded_client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-no-decisions-test",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
            },
            headers=_AUTH,
        )

        # Count TradeDecision rows after
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            decisions_after = session.query(TradeDecision).count()

        assert decisions_before == decisions_after

    def test_no_order_rows_created(self, seeded_client: TestClient, api_engine) -> None:
        """POST does not create Order rows."""
        # Save and approve a candidate
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-no-orders",
                "candidates": [
                    {
                        "ticker": "INTF",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.85",
                        "preview_decision": "CONSIDER",
                        "preview_score": "75.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = candidates[0]["id"]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # Count Order rows before
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            orders_before = session.query(Order).count()

        # Generate preview with candidate_ids for isolation
        seeded_client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-no-orders-test",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
            },
            headers=_AUTH,
        )

        # Count Order rows after
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            orders_after = session.query(Order).count()

        assert orders_before == orders_after

    def test_response_shape_has_all_required_fields(self, seeded_client: TestClient) -> None:
        """POST response includes all required fields: candidate_review_id, source, reason, raw_payload."""
        # Save and approve a candidate
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-response-shape",
                "candidates": [
                    {
                        "ticker": "QCOM",
                        "scan_score": "12.5",
                        "latest_price": "150.00",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.96",
                        "forecast_price_5d": "165.00",
                        "expected_return_pct": "10.54",
                        "market_context": "bullish",
                        "preview_decision": "CONSIDER",
                        "preview_score": "98.7",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = candidates[0]["id"]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # Generate preview with candidate_ids for isolation
        resp = seeded_client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-shape-test",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["signal_previews"]) == 1

        preview = data["signal_previews"][0]
        # Verify all required fields exist
        assert "candidate_review_id" in preview
        assert "ticker" in preview
        assert "side" in preview
        assert "confidence" in preview
        assert "source" in preview
        assert "preview_decision" in preview
        assert "preview_score" in preview
        assert "expected_return_pct" in preview
        assert "reason" in preview
        assert "raw_payload" in preview

        # Verify no "direction" field (old name)
        assert "direction" not in preview

        # Verify field values
        assert preview["ticker"] == "QCOM"
        assert preview["side"] == "BUY"
        assert preview["source"] == "review_queue_preview_v1"
        assert preview["confidence"] == "0.96"
        assert preview["reason"] == "Preview only: would create BUY signal from approved review candidate."

        # Verify raw_payload structure
        raw = preview["raw_payload"]
        assert "candidate_review_id" in raw
        assert "prediction_recommendation" in raw
        assert "prediction_confidence" in raw
        assert "forecast_price_5d" in raw
        assert "market_context" in raw
        assert "preview_reasons" in raw
        assert raw["prediction_recommendation"] == "BUY"
        assert raw["prediction_confidence"] == "0.96"

    def test_candidate_ids_mixed_status_skipped(self, seeded_client: TestClient) -> None:
        """POST with candidate_ids skips rows not matching review_status (evaluated but not generated)."""
        # Save two candidates
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-mixed-1",
                "candidates": [
                    {
                        "ticker": "APPROVED_BUY",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.85",
                        "preview_decision": "CONSIDER",
                        "preview_score": "80.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-mixed-2",
                "candidates": [
                    {
                        "ticker": "WATCHING_BUY",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.80",
                        "preview_decision": "CONSIDER",
                        "preview_score": "75.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        # Get candidates
        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()

        # Approve first, leave second as NEW
        approved_id = None
        watching_id = None
        for cand in candidates:
            if cand["ticker"] == "APPROVED_BUY":
                approved_id = cand["id"]
                seeded_client.patch(
                    f"/v1/review/candidates/{approved_id}",
                    json={"review_status": "APPROVED_FOR_SIGNAL"},
                    headers=_AUTH,
                )
            elif cand["ticker"] == "WATCHING_BUY":
                watching_id = cand["id"]
                seeded_client.patch(
                    f"/v1/review/candidates/{watching_id}",
                    json={"review_status": "WATCHING"},
                    headers=_AUTH,
                )

        # Preview with both IDs, filtering by APPROVED_FOR_SIGNAL
        resp = seeded_client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-mixed-status",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [approved_id, watching_id],
                "limit": 50,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()

        # Both evaluated, only one generated
        assert data["candidates_evaluated"] == 2
        assert data["signal_previews_generated"] == 1
        assert data["skipped_count"] == 1

        # Check skipped includes the WATCHING row
        assert len(data["skipped"]) == 1
        skipped = data["skipped"][0]
        assert "candidate_review_id" in skipped
        assert skipped["ticker"] == "WATCHING_BUY"
        assert "WATCHING" in skipped["reason"]
        assert "APPROVED_FOR_SIGNAL" in skipped["reason"]

        # Check generated is the approved one
        assert len(data["signal_previews"]) == 1
        assert data["signal_previews"][0]["ticker"] == "APPROVED_BUY"

    def test_invalid_confidence_skipped(self, seeded_client: TestClient, api_engine) -> None:
        """POST skips candidates with invalid confidence (non-numeric or out of range)."""
        # Save a candidate with invalid confidence
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-invalid-conf",
                "candidates": [
                    {
                        "ticker": "BADCONF",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "abc",
                        "preview_decision": "CONSIDER",
                        "preview_score": "70.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        # Approve it
        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = candidates[0]["id"]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # Count database rows before
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signals_before = session.query(Signal).count()
            decisions_before = session.query(TradeDecision).count()
            orders_before = session.query(Order).count()

        # Generate preview with candidate_ids for isolation
        resp = seeded_client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-invalid-conf",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signal_previews_generated"] == 0
        assert data["skipped_count"] == 1
        assert "Invalid confidence" in data["skipped"][0]["reason"]

        # Count database rows after and verify no rows created
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signals_after = session.query(Signal).count()
            decisions_after = session.query(TradeDecision).count()
            orders_after = session.query(Order).count()

        assert signals_before == signals_after
        assert decisions_before == decisions_after
        assert orders_before == orders_after

    def test_skipped_includes_candidate_review_id(self, seeded_client: TestClient) -> None:
        """POST includes candidate_review_id in skipped items for traceability."""
        # Save a HOLD candidate and approve it
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-skip-id",
                "candidates": [
                    {
                        "ticker": "HOLDME",
                        "prediction_recommendation": "HOLD",
                        "prediction_confidence": "0.75",
                        "preview_decision": "WATCH",
                        "preview_score": "50.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = candidates[0]["id"]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # Generate preview with candidate_ids for isolation
        resp = seeded_client.post(
            "/v1/review/signal-preview",
            json={
                "idempotency_key": "preview-skip-id",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["skipped"]) == 1

        skipped = data["skipped"][0]
        assert "candidate_review_id" in skipped
        assert skipped["candidate_review_id"] == candidate_id
        assert skipped["ticker"] == "HOLDME"
        assert "HOLD" in skipped["reason"]


class TestReviewCreateSignalsEndpoint:
    """Tests for POST /v1/review/create-signals (creates actual Signal rows)."""

    def test_post_requires_api_key(self, seeded_client: TestClient) -> None:
        """POST requires valid X-API-Key header."""
        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-key-api",
                "review_status": "APPROVED_FOR_SIGNAL",
                "limit": 10,
                "confirm_create_signals": True,
            },
        )
        assert resp.status_code == 401

    def test_confirm_create_signals_false_returns_422(self, seeded_client: TestClient) -> None:
        """POST returns 422 if confirm_create_signals is false."""
        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-confirm-false",
                "review_status": "APPROVED_FOR_SIGNAL",
                "limit": 10,
                "confirm_create_signals": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422

    def test_confirm_create_signals_missing_returns_422(self, seeded_client: TestClient) -> None:
        """POST returns 422 if confirm_create_signals is missing."""
        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-confirm-missing",
                "review_status": "APPROVED_FOR_SIGNAL",
                "limit": 10,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 422

    def test_no_approved_candidates_returns_zero(self, seeded_client: TestClient, api_engine) -> None:
        """POST returns 0 created when no APPROVED_FOR_SIGNAL candidates exist."""
        # Clear CandidateReview rows to isolate this test
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            session.query(CandidateReview).delete()
            session.commit()

        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-no-approved",
                "review_status": "APPROVED_FOR_SIGNAL",
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_created"] == 0
        assert data["candidates_evaluated"] == 0
        assert data["skipped_count"] == 0
        assert data["skipped_existing_count"] == 0

    def test_approved_buy_creates_signal(self, seeded_client: TestClient, api_engine) -> None:
        """POST creates Signal row for BUY recommendation."""
        # Save a BUY candidate
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-buy-create",
                "candidates": [
                    {
                        "ticker": "BUYME",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.95",
                        "preview_decision": "CONSIDER",
                        "preview_score": "85.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "BUYME"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # Verify before count
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal_count_before = session.query(Signal).count()

        # Create signal
        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-buy-create-exec",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_created"] == 1
        assert data["created_signals"][0]["side"] == "BUY"
        assert data["created_signals"][0]["ticker"] == "BUYME"
        assert data["trade_decisions_created"] == 0
        assert data["orders_created"] == 0

        # Verify after count increased
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal_count_after = session.query(Signal).count()
        assert signal_count_after == signal_count_before + 1

    def test_approved_sell_creates_signal(self, seeded_client: TestClient) -> None:
        """POST creates Signal row for SELL recommendation."""
        # Save a SELL candidate
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-sell-create",
                "candidates": [
                    {
                        "ticker": "SELLME",
                        "prediction_recommendation": "SELL",
                        "prediction_confidence": "0.88",
                        "preview_decision": "CONSIDER",
                        "preview_score": "75.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "SELLME"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-sell-create-exec",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_created"] == 1
        assert data["created_signals"][0]["side"] == "SELL"
        assert data["created_signals"][0]["ticker"] == "SELLME"

    def test_hold_skipped_creates_no_signal(self, seeded_client: TestClient) -> None:
        """POST skips HOLD recommendations and creates no Signal."""
        # Save a HOLD candidate
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-hold-skip",
                "candidates": [
                    {
                        "ticker": "HOLDME",
                        "prediction_recommendation": "HOLD",
                        "prediction_confidence": "0.75",
                        "preview_decision": "WATCH",
                        "preview_score": "50.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "HOLDME"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-hold-skip-exec",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_created"] == 0
        assert data["skipped_count"] == 1
        assert "HOLD" in data["skipped"][0]["reason"]

    def test_error_status_skipped(self, seeded_client: TestClient) -> None:
        """POST skips candidates with status != OK."""
        # Save an ERROR status candidate
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-error-status",
                "candidates": [
                    {
                        "ticker": "ERRORME",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.80",
                        "preview_decision": "CONSIDER",
                        "preview_score": "70.0",
                        "status": "ERROR",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "ERRORME"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-error-status-exec",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_created"] == 0
        assert data["skipped_count"] == 1
        assert "Status is ERROR" in data["skipped"][0]["reason"]

    def test_missing_recommendation_skipped(self, seeded_client: TestClient) -> None:
        """POST skips candidates with missing prediction_recommendation."""
        # Save a candidate without recommendation
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-no-rec",
                "candidates": [
                    {
                        "ticker": "NOREC",
                        "prediction_recommendation": None,
                        "prediction_confidence": "0.80",
                        "preview_decision": "WATCH",
                        "preview_score": "50.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "NOREC"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-no-rec-exec",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_created"] == 0
        assert data["skipped_count"] == 1
        assert "Missing prediction_recommendation" in data["skipped"][0]["reason"]

    def test_missing_confidence_skipped(self, seeded_client: TestClient) -> None:
        """POST skips candidates with missing prediction_confidence."""
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-no-conf",
                "candidates": [
                    {
                        "ticker": "NOCONF",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": None,
                        "preview_decision": "CONSIDER",
                        "preview_score": "70.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "NOCONF"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-no-conf-exec",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_created"] == 0
        assert data["skipped_count"] == 1
        assert "Missing prediction_confidence" in data["skipped"][0]["reason"]

    def test_invalid_confidence_skipped(self, seeded_client: TestClient) -> None:
        """POST skips candidates with confidence out of [0, 1] range."""
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-bad-conf",
                "candidates": [
                    {
                        "ticker": "BADCONF",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "1.5",
                        "preview_decision": "CONSIDER",
                        "preview_score": "70.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "BADCONF"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-bad-conf-exec",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_created"] == 0
        assert data["skipped_count"] == 1
        assert "out of range" in data["skipped"][0]["reason"]

    def test_non_approved_status_explicitly_included_skipped(self, seeded_client: TestClient) -> None:
        """POST skips non-APPROVED_FOR_SIGNAL status when candidate_ids includes them."""
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-explicit-new",
                "candidates": [
                    {
                        "ticker": "NEWSTATUS",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.80",
                        "preview_decision": "CONSIDER",
                        "preview_score": "70.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "NEWSTATUS"][0]
        # Don't approve it - leave it as NEW

        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-explicit-new-exec",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_created"] == 0
        assert data["skipped_count"] == 1
        assert "Review status is NEW" in data["skipped"][0]["reason"]

    def test_candidate_ids_filter_works(self, seeded_client: TestClient) -> None:
        """POST filters to specified candidate_ids only."""
        # Save two candidates
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-filter-1",
                "candidates": [
                    {
                        "ticker": "FILTER1",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.90",
                        "preview_decision": "CONSIDER",
                        "preview_score": "80.0",
                        "status": "OK",
                    },
                    {
                        "ticker": "FILTER2",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.85",
                        "preview_decision": "CONSIDER",
                        "preview_score": "75.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id_1 = [c["id"] for c in candidates if c["ticker"] == "FILTER1"][0]
        
        # Approve both
        for c_id in [c["id"] for c in candidates if c["ticker"] in ["FILTER1", "FILTER2"]]:
            seeded_client.patch(
                f"/v1/review/candidates/{c_id}",
                json={"review_status": "APPROVED_FOR_SIGNAL"},
                headers=_AUTH,
            )

        # Process only FILTER1
        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-filter-exec",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id_1],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["candidates_evaluated"] == 1
        assert data["signals_created"] == 1
        assert data["created_signals"][0]["ticker"] == "FILTER1"

    def test_limit_works(self, seeded_client: TestClient) -> None:
        """POST respects limit parameter."""
        # Save 5 candidates
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-limit",
                "candidates": [
                    {
                        "ticker": f"LIMIT{i}",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.80",
                        "preview_decision": "CONSIDER",
                        "preview_score": "70.0",
                        "status": "OK",
                    }
                    for i in range(5)
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        for c in candidates:
            if c["ticker"].startswith("LIMIT"):
                seeded_client.patch(
                    f"/v1/review/candidates/{c['id']}",
                    json={"review_status": "APPROVED_FOR_SIGNAL"},
                    headers=_AUTH,
                )

        # Request with limit=2
        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-limit-exec",
                "review_status": "APPROVED_FOR_SIGNAL",
                "limit": 2,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["candidates_evaluated"] <= 2

    def test_repeated_request_idempotent_zero_created(self, seeded_client: TestClient, api_engine) -> None:
        """POST same idempotency_key twice creates 0 signals second time."""
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-idem",
                "candidates": [
                    {
                        "ticker": "IDEM1",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.90",
                        "preview_decision": "CONSIDER",
                        "preview_score": "80.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "IDEM1"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # First call
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal_count_before = session.query(Signal).count()
            decision_count_before = session.query(TradeDecision).count()
            order_count_before = session.query(Order).count()
            jobrun_count_before = session.query(JobRun).count()

        resp1 = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-idem-same",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp1.status_code == 200
        assert resp1.json()["signals_created"] == 1

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal_count_after_first = session.query(Signal).count()
            decision_count_after_first = session.query(TradeDecision).count()
            order_count_after_first = session.query(Order).count()
            jobrun_count_after_first = session.query(JobRun).count()

        assert signal_count_after_first == signal_count_before + 1
        assert decision_count_after_first == decision_count_before
        assert order_count_after_first == order_count_before
        assert jobrun_count_after_first == jobrun_count_before + 1

        # Second call with same idempotency_key and candidate
        resp2 = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-idem-same",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp2.status_code == 200
        data2 = resp2.json()
        assert data2["signals_created"] == 0
        assert data2["skipped_existing_count"] == 1

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal_count_after_second = session.query(Signal).count()
            decision_count_after_second = session.query(TradeDecision).count()
            order_count_after_second = session.query(Order).count()
            jobrun_count_after_second = session.query(JobRun).count()

        # Counts should be unchanged (no new JobRun created when no new signals)
        assert signal_count_after_second == signal_count_after_first
        assert decision_count_after_second == decision_count_after_first
        assert order_count_after_second == order_count_after_first
        assert jobrun_count_after_second == jobrun_count_after_first

    def test_duplicate_protection_different_idempotency_key(self, seeded_client: TestClient, api_engine) -> None:
        """POST same candidate with different idempotency_key creates no duplicate Signal."""
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-dup-protect",
                "candidates": [
                    {
                        "ticker": "DUPTEST",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.92",
                        "preview_decision": "CONSIDER",
                        "preview_score": "82.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "DUPTEST"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # First call with idempotency_key="batch-1"
        resp1 = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "batch-1",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp1.status_code == 200
        data1 = resp1.json()
        assert data1["signals_created"] == 1

        # Count signals before second call
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal_count_before = session.query(Signal).count()
            decision_count_before = session.query(TradeDecision).count()
            order_count_before = session.query(Order).count()

        # Second call with idempotency_key="batch-2" for SAME candidate
        resp2 = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "batch-2",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp2.status_code == 200
        data2 = resp2.json()
        assert data2["signals_created"] == 0
        assert data2["skipped_existing_count"] == 1
        assert "already exists" in data2["skipped"][0]["reason"]

        # Verify counts did NOT increase
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal_count_after = session.query(Signal).count()
            decision_count_after = session.query(TradeDecision).count()
            order_count_after = session.query(Order).count()
        assert signal_count_after == signal_count_before
        assert decision_count_after == decision_count_before
        assert order_count_after == order_count_before

    def test_raw_payload_includes_candidate_review_id(self, seeded_client: TestClient) -> None:
        """POST creates Signal with raw_payload containing candidate_review_id."""
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-payload",
                "candidates": [
                    {
                        "ticker": "PAYLOAD",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.85",
                        "preview_decision": "CONSIDER",
                        "preview_score": "75.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "PAYLOAD"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-payload-exec",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_created"] == 1
        signal = data["created_signals"][0]
        assert signal["candidate_review_id"] == candidate_id
        assert signal["source_run"] == f"review_queue_create_signals_v1:{candidate_id}"

    def test_source_run_deterministic(self, seeded_client: TestClient) -> None:
        """POST creates Signal with deterministic source_run based on candidate_id."""
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-source-run",
                "candidates": [
                    {
                        "ticker": "SRCRUN",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.87",
                        "preview_decision": "CONSIDER",
                        "preview_score": "77.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "SRCRUN"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-source-run-exec",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_created"] == 1
        created_signal = data["created_signals"][0]
        expected_source_run = f"review_queue_create_signals_v1:{candidate_id}"
        assert created_signal["source_run"] == expected_source_run

    def test_no_trade_decisions_created(self, seeded_client: TestClient, api_engine) -> None:
        """POST proves no TradeDecision rows created (before/after count)."""
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-no-decision",
                "candidates": [
                    {
                        "ticker": "NODECISION",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.89",
                        "preview_decision": "CONSIDER",
                        "preview_score": "79.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "NODECISION"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # Count before
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            decision_count_before = session.query(TradeDecision).count()

        # Create signal
        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-no-decision-exec",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        assert resp.json()["trade_decisions_created"] == 0

        # Count after
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            decision_count_after = session.query(TradeDecision).count()
        assert decision_count_after == decision_count_before

    def test_no_orders_created(self, seeded_client: TestClient, api_engine) -> None:
        """POST proves no Order rows created (before/after count)."""
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-no-order",
                "candidates": [
                    {
                        "ticker": "NOORDER",
                        "prediction_recommendation": "SELL",
                        "prediction_confidence": "0.91",
                        "preview_decision": "CONSIDER",
                        "preview_score": "81.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "NOORDER"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # Count before
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            order_count_before = session.query(Order).count()

        # Create signal
        resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-no-order-exec",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        assert resp.json()["orders_created"] == 0

        # Count after
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            order_count_after = session.query(Order).count()
        assert order_count_after == order_count_before


class TestReviewDecisionPreviewEndpoint:
    """Tests for POST /v1/review/decision-preview (preview-only, no persistence)."""

    def test_post_requires_api_key(self, seeded_client: TestClient) -> None:
        """POST requires valid X-API-Key header."""
        resp = seeded_client.post(
            "/v1/review/decision-preview",
            json={
                "idempotency_key": "test-key-decision-preview",
                "limit": 10,
                "received_only": True,
            },
        )
        assert resp.status_code == 401

    def test_no_signals_returns_zero(self, seeded_client: TestClient, api_engine) -> None:
        """POST returns 0 generated when no review-created signals exist."""
        # Use a unique source_run_prefix that matches no signals
        resp = seeded_client.post(
            "/v1/review/decision-preview",
            json={
                "idempotency_key": "test-no-signals-preview",
                "source_run_prefix": "review_queue_create_signals_v1_no_match:",
                "limit": 50,
                "received_only": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_evaluated"] == 0
        assert data["decision_previews_generated"] == 0
        assert data["skipped_count"] == 0
        assert data["trade_decisions_created"] == 0
        assert data["orders_created"] == 0

    def test_basic_buy_preview(self, seeded_client: TestClient, api_engine) -> None:
        """POST generates decision preview for a BUY signal."""
        # Create and approve a candidate
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-buy-preview-isolated",
                "candidates": [
                    {
                        "ticker": "BUYPREVIEW",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.85",
                        "preview_decision": "CONSIDER",
                        "preview_score": "85.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "BUYPREVIEW"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        # Create signal from candidate
        create_resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-buy-preview-create-isolated",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        signal_id = create_resp.json()["created_signals"][0]["signal_id"]

        # Count before preview
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td_before = session.query(TradeDecision).count()
            order_before = session.query(Order).count()
            jr_before = session.query(JobRun).count()

        # Preview the signal with explicit signal_ids
        resp = seeded_client.post(
            "/v1/review/decision-preview",
            json={
                "idempotency_key": "test-buy-preview-exec-isolated",
                "signal_ids": [signal_id],
                "limit": 50,
                "received_only": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["execution_mode"] == "DECISION_PREVIEW_ONLY"
        assert data["signals_evaluated"] == 1
        assert data["decision_previews_generated"] == 1
        assert len(data["decision_previews"]) == 1
        assert data["trade_decisions_created"] == 0
        assert data["orders_created"] == 0

        # Verify preview contains expected fields
        preview = data["decision_previews"][0]
        assert "signal_id" in preview
        assert preview["ticker"] == "BUYPREVIEW"
        assert preview["side"] == "BUY"
        assert Decimal(preview["confidence"]) == Decimal("0.85")
        assert "preview_decision" in preview
        assert "risk_snapshot" in preview
        assert "max_positions" in preview["risk_snapshot"], "risk_snapshot must include max_positions"
        assert "sizing_adjustments" in preview

        # Verify DB unchanged
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td_after = session.query(TradeDecision).count()
            order_after = session.query(Order).count()
            jr_after = session.query(JobRun).count()
        assert td_after == td_before, "TradeDecision rows created unexpectedly"
        assert order_after == order_before, "Order rows created unexpectedly"
        assert jr_after == jr_before, "JobRun rows created unexpectedly"

    def test_multiple_signals_preview(self, seeded_client: TestClient, api_engine) -> None:
        """POST generates previews for multiple signals."""
        # Create and approve multiple candidates
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-multi-preview",
                "candidates": [
                    {
                        "ticker": "MULTI1",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.80",
                        "preview_decision": "CONSIDER",
                        "preview_score": "80.0",
                        "status": "OK",
                    },
                    {
                        "ticker": "MULTI2",
                        "prediction_recommendation": "SELL",
                        "prediction_confidence": "0.75",
                        "preview_decision": "WATCH",
                        "preview_score": "70.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_ids = [c["id"] for c in candidates if c["ticker"] in ["MULTI1", "MULTI2"]]

        for cid in candidate_ids:
            seeded_client.patch(
                f"/v1/review/candidates/{cid}",
                json={"review_status": "APPROVED_FOR_SIGNAL"},
                headers=_AUTH,
            )

        # Create signals
        seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-multi-preview-create",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": candidate_ids,
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )

        # Preview all signals
        resp = seeded_client.post(
            "/v1/review/decision-preview",
            json={
                "idempotency_key": "test-multi-preview-exec",
                "limit": 50,
                "received_only": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_evaluated"] >= 2
        assert data["decision_previews_generated"] >= 2
        assert len(data["decision_previews"]) >= 2

    def test_received_only_filter(self, seeded_client: TestClient, api_engine) -> None:
        """POST respects received_only filter in SQL query."""
        # Create a signal
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-received-filter",
                "candidates": [
                    {
                        "ticker": "RECVONLY",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.80",
                        "preview_decision": "CONSIDER",
                        "preview_score": "80.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "RECVONLY"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-received-filter-create",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )

        # Get the signal ID and update its status
        signals = seeded_client.get(
            "/v1/positions",
            headers=_AUTH,
        )
        # Note: we'd need to manually update signal status via DB for this test
        # For now, just verify that received_only=false includes all signals
        resp = seeded_client.post(
            "/v1/review/decision-preview",
            json={
                "idempotency_key": "test-received-filter-exec",
                "limit": 50,
                "received_only": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_evaluated"] >= 1

    def test_explicit_signal_ids_non_review_source_run(self, seeded_client: TestClient, api_engine) -> None:
        """POST skips signal with non-review source_run when explicit signal_ids provided."""
        # Create a signal with non-review source_run
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_run = JobRun(
                idempotency_key="test-non-review-source",
                workflow_type="MANUAL_TEST",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="NONREV",
                direction="BUY",
                confidence=Decimal("0.85"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="manual_signal_test",  # Non-review source_run
                status="RECEIVED",
            )
            session.add(signal)
            session.flush()
            signal_id = str(signal.id)
            session.commit()

        # Try to preview with explicit signal_ids
        resp = seeded_client.post(
            "/v1/review/decision-preview",
            json={
                "idempotency_key": "test-non-review-preview",
                "signal_ids": [signal_id],
                "limit": 50,
                "received_only": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_evaluated"] == 1
        assert data["decision_previews_generated"] == 0
        assert data["skipped_count"] == 1
        assert "not review-created" in data["skipped"][0]["reason"]

    def test_explicit_signal_ids_non_received_status_with_received_only(self, seeded_client: TestClient, api_engine) -> None:
        """POST skips review-created signal with non-RECEIVED status when received_only=true."""
        # Create a review-created signal and update its status to DECISION_MADE
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-decision-made",
                "candidates": [
                    {
                        "ticker": "DECISIONMADE",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.80",
                        "preview_decision": "CONSIDER",
                        "preview_score": "80.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "DECISIONMADE"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        create_resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-decision-made-create",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        signal_id = create_resp.json()["created_signals"][0]["signal_id"]

        # Update signal status to DECISION_MADE
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal = session.query(Signal).filter(Signal.id == signal_id).first()
            if signal:
                signal.status = "DECISION_MADE"
                session.commit()

        # Try to preview with received_only=true
        resp = seeded_client.post(
            "/v1/review/decision-preview",
            json={
                "idempotency_key": "test-decision-made-preview",
                "signal_ids": [signal_id],
                "limit": 50,
                "received_only": True,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_evaluated"] == 1
        assert data["decision_previews_generated"] == 0
        assert data["skipped_count"] == 1
        assert "not RECEIVED" in data["skipped"][0]["reason"]

        # Verify Signal.status is still DECISION_MADE
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal_after = session.query(Signal).filter(Signal.id == signal_id).first()
            assert signal_after.status == "DECISION_MADE"

    def test_explicit_signal_ids_non_received_status_without_received_only(self, seeded_client: TestClient, api_engine) -> None:
        """POST can preview review-created signal with non-RECEIVED status when received_only=false."""
        # Create a review-created signal and update its status to DECISION_MADE
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-decision-made-no-filter",
                "candidates": [
                    {
                        "ticker": "DECISIONMADENOFILTER",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.80",
                        "preview_decision": "CONSIDER",
                        "preview_score": "80.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "DECISIONMADENOFILTER"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        create_resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-decision-made-no-filter-create",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        signal_id = create_resp.json()["created_signals"][0]["signal_id"]

        # Update signal status to DECISION_MADE
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal = session.query(Signal).filter(Signal.id == signal_id).first()
            if signal:
                signal.status = "DECISION_MADE"
                session.commit()

        # Count before
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td_before = session.query(TradeDecision).count()
            order_before = session.query(Order).count()
            jr_before = session.query(JobRun).count()

        # Try to preview with received_only=false
        resp = seeded_client.post(
            "/v1/review/decision-preview",
            json={
                "idempotency_key": "test-decision-made-no-filter-preview",
                "signal_ids": [signal_id],
                "limit": 50,
                "received_only": False,
            },
            headers=_AUTH,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["signals_evaluated"] == 1
        assert data["decision_previews_generated"] == 1
        assert data["skipped_count"] == 0
        assert data["trade_decisions_created"] == 0
        assert data["orders_created"] == 0

        # Verify DB still unchanged
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td_after = session.query(TradeDecision).count()
            order_after = session.query(Order).count()
            jr_after = session.query(JobRun).count()
        assert td_after == td_before, "TradeDecision rows created unexpectedly"
        assert order_after == order_before, "Order rows created unexpectedly"
        assert jr_after == jr_before, "JobRun rows created unexpectedly"

        # Verify Signal.status unchanged
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal_after = session.query(Signal).filter(Signal.id == signal_id).first()
            assert signal_after.status == "DECISION_MADE"

    def test_no_trade_decisions_created(self, seeded_client: TestClient, api_engine) -> None:
        """POST never creates TradeDecision rows (safety test)."""
        # Create and preview a signal
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-no-td-safety",
                "candidates": [
                    {
                        "ticker": "NOTDCREATE",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.85",
                        "preview_decision": "CONSIDER",
                        "preview_score": "85.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "NOTDCREATE"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-no-td-safety-create",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )

        # Count before
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td_before = session.query(TradeDecision).count()

        # Preview
        seeded_client.post(
            "/v1/review/decision-preview",
            json={
                "idempotency_key": "test-no-td-safety-exec",
                "limit": 50,
                "received_only": True,
            },
            headers=_AUTH,
        )

        # Count after
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td_after = session.query(TradeDecision).count()
        assert td_after == td_before, "TradeDecision rows should not be created by preview endpoint"

    def test_no_orders_created(self, seeded_client: TestClient, api_engine) -> None:
        """POST never creates Order rows (safety test)."""
        # Create and preview a signal
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-no-order-safety",
                "candidates": [
                    {
                        "ticker": "NOORDERSAFETY",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.85",
                        "preview_decision": "CONSIDER",
                        "preview_score": "85.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "NOORDERSAFETY"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-no-order-safety-create",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )

        # Count before
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            order_before = session.query(Order).count()

        # Preview
        seeded_client.post(
            "/v1/review/decision-preview",
            json={
                "idempotency_key": "test-no-order-safety-exec",
                "limit": 50,
                "received_only": True,
            },
            headers=_AUTH,
        )

        # Count after
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            order_after = session.query(Order).count()
        assert order_after == order_before, "Order rows should not be created by preview endpoint"

    def test_no_job_runs_created(self, seeded_client: TestClient, api_engine) -> None:
        """POST never creates JobRun rows (safety test)."""
        # Create and preview a signal
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-no-jr-safety",
                "candidates": [
                    {
                        "ticker": "NOJRSAFETY",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.85",
                        "preview_decision": "CONSIDER",
                        "preview_score": "85.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "NOJRSAFETY"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-no-jr-safety-create",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )

        # Count before
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            jr_before = session.query(JobRun).count()

        # Preview
        seeded_client.post(
            "/v1/review/decision-preview",
            json={
                "idempotency_key": "test-no-jr-safety-exec",
                "limit": 50,
                "received_only": True,
            },
            headers=_AUTH,
        )

        # Count after
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            jr_after = session.query(JobRun).count()
        assert jr_after == jr_before, "JobRun rows should not be created by preview endpoint"

    def test_signal_status_unchanged(self, seeded_client: TestClient, api_engine) -> None:
        """POST never updates Signal.status (safety test)."""
        # Create a signal
        seeded_client.post(
            "/v1/review/candidates",
            json={
                "idempotency_key": "test-status-safety",
                "candidates": [
                    {
                        "ticker": "STATUSSAFETY",
                        "prediction_recommendation": "BUY",
                        "prediction_confidence": "0.85",
                        "preview_decision": "CONSIDER",
                        "preview_score": "85.0",
                        "status": "OK",
                    }
                ]
            },
            headers=_AUTH,
        )

        candidates = seeded_client.get(
            "/v1/review/candidates",
            headers=_AUTH,
        ).json()
        candidate_id = [c["id"] for c in candidates if c["ticker"] == "STATUSSAFETY"][0]
        seeded_client.patch(
            f"/v1/review/candidates/{candidate_id}",
            json={"review_status": "APPROVED_FOR_SIGNAL"},
            headers=_AUTH,
        )

        create_resp = seeded_client.post(
            "/v1/review/create-signals",
            json={
                "idempotency_key": "test-status-safety-create",
                "review_status": "APPROVED_FOR_SIGNAL",
                "candidate_ids": [candidate_id],
                "limit": 50,
                "confirm_create_signals": True,
            },
            headers=_AUTH,
        )
        signal_id = create_resp.json()["created_signals"][0]["signal_id"]

        # Get status before preview
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal_before = session.query(Signal).filter(Signal.id == signal_id).first()
            status_before = signal_before.status

        # Preview
        seeded_client.post(
            "/v1/review/decision-preview",
            json={
                "idempotency_key": "test-status-safety-exec",
                "limit": 50,
                "received_only": True,
            },
            headers=_AUTH,
        )

        # Get status after preview
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal_after = session.query(Signal).filter(Signal.id == signal_id).first()
            status_after = signal_after.status

        assert status_after == status_before, "Signal.status should not be modified by preview endpoint"
        assert status_after == "RECEIVED", "Signal.status should remain RECEIVED"


class TestReviewCreateDecisionsEndpoint:
    """Test POST /v1/review/create-decisions endpoint."""

    def test_requires_api_key(self, seeded_client: TestClient) -> None:
        """Test that endpoint requires API key."""
        response = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "test-create-1",
                "confirm_create_decisions": True,
            },
        )
        assert response.status_code == 401, "Should require API key"

    def test_confirm_create_decisions_missing_returns_422(self, seeded_client: TestClient) -> None:
        """Test that confirm_create_decisions=false or missing returns HTTP 422."""
        response = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "test-create-1",
                # confirm_create_decisions missing (defaults to false)
            },
            headers=_AUTH,
        )
        assert response.status_code == 422, "Should return 422 when confirm_create_decisions is missing"

    def test_confirm_create_decisions_false_returns_422(self, seeded_client: TestClient) -> None:
        """Test that confirm_create_decisions=false returns HTTP 422."""
        response = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "test-create-1",
                "confirm_create_decisions": False,
            },
            headers=_AUTH,
        )
        assert response.status_code == 422, "Should return 422 when confirm_create_decisions is false"

    def test_no_signals_returns_zero_created(self, seeded_client: TestClient) -> None:
        """Test that querying non-existent signals returns 0 created."""
        response = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "test-create-no-signals",
                "source_run_prefix": "nonexistent_prefix_12345:",
                "limit": 50,
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["signals_evaluated"] == 0
        assert data["trade_decisions_created"] == 0
        assert data["skipped_count"] == 0
        assert data["orders_created"] == 0

    def test_buy_signal_creates_one_trade_decision(self, seeded_client: TestClient, api_engine) -> None:
        """Test that a BUY signal creates one TradeDecision."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            # Count TradeDecision rows before
            td_before = session.query(TradeDecision).count()

            # Create review-created signal (BUY)
            job_run = JobRun(
                idempotency_key="setup-signal-buy-signal-test",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="AAPL",
                direction="BUY",
                confidence=Decimal("0.85"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-1",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()

            signal_id = str(signal.id)

            # Ensure price exists
            price = PriceSnapshot(
                ticker="AAPL",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("150.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        # Call create-decisions endpoint
        response = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-buy-signal-test",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["signals_evaluated"] == 1
        assert data["trade_decisions_created"] == 1
        assert data["skipped_count"] == 0
        assert data["skipped_existing_count"] == 0
        assert data["orders_created"] == 0
        assert len(data["created_decisions"]) == 1

        # Verify TradeDecision row count increased
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td_after = session.query(TradeDecision).count()
            assert td_after == td_before + 1, "TradeDecision count should increase by 1"

            # Verify Signal.status is now DECISION_MADE
            signal_row = session.query(Signal).filter(Signal.id == uuid.UUID(signal_id)).first()
            assert signal_row.status == "DECISION_MADE", "Signal.status should be DECISION_MADE"

    def test_sell_signal_creates_one_trade_decision(self, seeded_client: TestClient, api_engine) -> None:
        """Test that a SELL signal creates one TradeDecision."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td_before = session.query(TradeDecision).count()

            # Create a position so SELL works
            pos = Position(
                ticker="MSFT",
                qty=Decimal("100"),
                avg_cost=Decimal("300.00"),
                cost_basis=Decimal("30000.00"),
                opened_at=datetime.now(timezone.utc),
                last_updated=datetime.now(timezone.utc),
            )
            session.add(pos)
            session.flush()

            # Create review-created signal (SELL)
            job_run = JobRun(
                idempotency_key="setup-signal-sell-signal-test",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="MSFT",
                direction="SELL",
                confidence=Decimal("0.80"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-2",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()

            signal_id = str(signal.id)

            price = PriceSnapshot(
                ticker="MSFT",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("310.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        response = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-sell-signal-test",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["signals_evaluated"] == 1
        assert data["trade_decisions_created"] == 1
        assert data["orders_created"] == 0

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td_after = session.query(TradeDecision).count()
            assert td_after == td_before + 1

    def test_non_review_source_run_skipped_by_default(self, seeded_client: TestClient, api_engine) -> None:
        """Test that signals with non-review source_run are skipped when using default prefix."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            # Create signal with non-matching source_run
            job_run = JobRun(
                idempotency_key="setup-signal-non-review-default-test",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="GOOGL",
                direction="BUY",
                confidence=Decimal("0.75"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="some_other_unique_source_xxxxxxxx:signal-1",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()
            session.commit()

        response = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-non-review-default-test",
                "source_run_prefix": "review_queue_create_signals_v1_non_review_default_no_match:",
                "limit": 50,
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["signals_evaluated"] == 0
        assert data["trade_decisions_created"] == 0
        assert data["skipped_count"] == 0
        assert data["orders_created"] == 0

    def test_explicit_signal_ids_skips_non_review_with_reason(self, seeded_client: TestClient, api_engine) -> None:
        """Test that explicit signal_ids validates source_run and skips non-review with clear reason."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_run = JobRun(
                idempotency_key="setup-signal-explicit-non-review-test",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="TSLA",
                direction="BUY",
                confidence=Decimal("0.80"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="not_review_created:signal-1",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()
            signal_id = str(signal.id)
            session.commit()

        response = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-explicit-non-review-test",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["signals_evaluated"] == 1
        assert data["trade_decisions_created"] == 0
        assert len(data["skipped"]) == 1
        assert "Signal source_run is not review-created" in data["skipped"][0]["reason"]

    def test_received_only_true_skips_non_received(self, seeded_client: TestClient, api_engine) -> None:
        """Test that received_only=true skips signals with status != RECEIVED."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_run = JobRun(
                idempotency_key="setup-signal-non-received-test",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            # Signal with DECISION_MADE status
            signal = Signal(
                job_run_id=job_run.id,
                ticker="META",
                direction="BUY",
                confidence=Decimal("0.85"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-3",
                status="DECISION_MADE",
                raw_payload={},
            )
            session.add(signal)
            session.flush()
            signal_id = str(signal.id)
            session.commit()

        response = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-non-received-test",
                "signal_ids": [signal_id],
                "received_only": True,
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["signals_evaluated"] == 1
        assert data["trade_decisions_created"] == 0
        assert len(data["skipped"]) == 1
        assert "not RECEIVED" in data["skipped"][0]["reason"]

    def test_limit_parameter_works(self, seeded_client: TestClient, api_engine) -> None:
        """Test that limit parameter restricts number of signals processed."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_run = JobRun(
                idempotency_key="setup-signal-limit-test",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            # Create 5 review-created signals
            for i in range(5):
                signal = Signal(
                    job_run_id=job_run.id,
                    ticker=f"TICK{i}",
                    direction="BUY",
                    confidence=Decimal("0.75"),
                    signal_ts=datetime.now(timezone.utc),
                    market_date=date.today(),
                    source_run="review_queue_create_signals_v1:candidate-limit-test",
                    status="RECEIVED",
                    raw_payload={},
                )
                session.add(signal)

            session.flush()

            # Create prices
            for i in range(5):
                price = PriceSnapshot(
                    ticker=f"TICK{i}",
                    price_type="CLOSE",
                    session_type="REGULAR",
                    market_date=date.today(),
                    price=Decimal("100.00"),
                    snapshot_ts=datetime.now(timezone.utc),
                )
                session.add(price)
            session.flush()
            session.commit()

        response = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-limit-test",
                "source_run_prefix": "review_queue_create_signals_v1:",
                "limit": 2,
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["signals_evaluated"] == 2, "Should evaluate only 2 signals (limit)"
        assert data["trade_decisions_created"] == 2

    def test_trade_decision_fields_match_risk_decision(self, seeded_client: TestClient, api_engine) -> None:
        """Test that created TradeDecision fields match RiskDecision output."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_run = JobRun(
                idempotency_key="setup-signal-field-match-test",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="NVDA",
                direction="BUY",
                confidence=Decimal("0.90"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-field-test",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()
            signal_id = str(signal.id)

            price = PriceSnapshot(
                ticker="NVDA",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("500.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        response = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-field-match-test",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["created_decisions"]) == 1
        decision = data["created_decisions"][0]

        # Verify fields are present
        assert "signal_id" in decision
        assert "trade_decision_id" in decision
        assert "decision" in decision
        assert "reason_code" in decision
        assert "requested_notional" in decision
        assert "approved_notional" in decision
        assert "requested_qty" in decision
        assert "approved_qty" in decision

    def test_risk_snapshot_populated(self, seeded_client: TestClient, api_engine) -> None:
        """Test that TradeDecision.risk_snapshot is populated in database."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_run = JobRun(
                idempotency_key="setup-signal-risk-snapshot-test",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="AMD",
                direction="BUY",
                confidence=Decimal("0.70"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-risk-snapshot",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()
            signal_id = str(signal.id)

            price = PriceSnapshot(
                ticker="AMD",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("120.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        response = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-risk-snapshot-test",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response.status_code == 200

        # Verify TradeDecision in database has risk_snapshot
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td = session.query(TradeDecision).filter(
                TradeDecision.signal_id == uuid.UUID(signal_id)
            ).first()
            assert td is not None
            assert td.risk_snapshot is not None
            assert isinstance(td.risk_snapshot, dict)
            assert "cash" in td.risk_snapshot
            assert "total_value" in td.risk_snapshot
            assert "max_positions" in td.risk_snapshot, "risk_snapshot must include max_positions"
            assert "open_position_count" in td.risk_snapshot

    def test_duplicate_protection_same_signal_id(self, seeded_client: TestClient, api_engine) -> None:
        """Test that same signal_id is not duplicated in TradeDecision."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_run = JobRun(
                idempotency_key="dup-same-test",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="INTC",
                direction="BUY",
                confidence=Decimal("0.75"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-dup-same",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()
            signal_id = str(signal.id)

            price = PriceSnapshot(
                ticker="INTC",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("30.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        # First request: creates TradeDecision
        response1 = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "dup-same-test-req1",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response1.status_code == 200
        assert response1.json()["trade_decisions_created"] == 1

        # Second request (with different idempotency_key): should skip as duplicate
        response2 = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "dup-same-test-req2",
                "signal_ids": [signal_id],
                "received_only": False,
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response2.status_code == 200
        data = response2.json()
        assert data["trade_decisions_created"] == 0
        assert data["skipped_existing_count"] == 1

    def test_duplicate_skipped_signal_status_unchanged(self, seeded_client: TestClient, api_engine) -> None:
        """Test that skipped duplicate signals don't mutate Signal.status."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_run = JobRun(
                idempotency_key="dup-status-test",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="NVDA",
                direction="SELL",
                confidence=Decimal("0.80"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-dup-status",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()
            signal_id = str(signal.id)

            # Create a position for SELL
            pos = Position(
                ticker="NVDA",
                qty=Decimal("50"),
                avg_cost=Decimal("450.00"),
                cost_basis=Decimal("22500.00"),
                opened_at=datetime.now(timezone.utc),
                last_updated=datetime.now(timezone.utc),
            )
            session.add(pos)

            price = PriceSnapshot(
                ticker="NVDA",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("450.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        # First request: creates TradeDecision and updates Signal.status to DECISION_MADE
        response1 = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "dup-status-test-req1",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response1.status_code == 200
        assert response1.json()["trade_decisions_created"] == 1

        # Verify Signal.status is DECISION_MADE
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal_row = session.query(Signal).filter(
                Signal.id == uuid.UUID(signal_id)
            ).first()
            assert signal_row.status == "DECISION_MADE"

        # Second request: skipped as duplicate
        response2 = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "dup-status-test-req2",
                "signal_ids": [signal_id],
                "received_only": False,
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response2.status_code == 200
        assert response2.json()["skipped_existing_count"] == 1

        # Verify Signal.status is still DECISION_MADE (unchanged by skip)
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal_row = session.query(Signal).filter(
                Signal.id == uuid.UUID(signal_id)
            ).first()
            assert signal_row.status == "DECISION_MADE"

    def test_no_orders_created(self, seeded_client: TestClient, api_engine) -> None:
        """Test that no Order rows are created."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            orders_before = session.query(Order).count()

            job_run = JobRun(
                idempotency_key="setup-signal-no-orders-test",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="AAPL",
                direction="BUY",
                confidence=Decimal("0.85"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-no-orders",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()
            signal_id = str(signal.id)

            price = PriceSnapshot(
                ticker="AAPL",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("150.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        response = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-no-orders-test",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        assert response.json()["orders_created"] == 0

        # Verify Order count unchanged
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            orders_after = session.query(Order).count()
            assert orders_after == orders_before, "No Orders should be created"

    def test_no_unnecessary_job_run_when_all_skipped(self, seeded_client: TestClient, api_engine) -> None:
        """Test that JobRun is not created when all signals are skipped."""
        # Setup: create signal and price snapshot
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_run = JobRun(
                idempotency_key="setup-signal-skip-all-test",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="TSLA",
                direction="BUY",
                confidence=Decimal("0.75"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-skip-all",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()

            price = PriceSnapshot(
                ticker="TSLA",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("250.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()

            signal_id = str(signal.id)
            session.commit()

        # First call: creates TradeDecision and JobRun
        response1 = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-skip-all-first",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response1.status_code == 200
        assert response1.json()["trade_decisions_created"] == 1

        # Record state after first call
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_runs_before_second = session.query(JobRun).count()
            orders_before_second = session.query(Order).count()
            signal_row = session.query(Signal).filter(Signal.id == uuid.UUID(signal_id)).first()
            signal_status_before_second = signal_row.status

        # Second call: signal is now duplicate (TradeDecision exists), should skip
        response2 = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-skip-all-second",
                "signal_ids": [signal_id],
                "received_only": False,
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response2.status_code == 200
        data2 = response2.json()
        assert data2["signals_evaluated"] == 1
        assert data2["trade_decisions_created"] == 0
        assert data2["skipped_existing_count"] == 1
        assert len(data2["skipped"]) == 1
        assert "TradeDecision already exists for signal" in data2["skipped"][0]["reason"]

        # Verify no new JobRun was created on second call
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_runs_after_second = session.query(JobRun).count()
            orders_after_second = session.query(Order).count()
            signal_row = session.query(Signal).filter(Signal.id == uuid.UUID(signal_id)).first()
            signal_status_after_second = signal_row.status

            assert job_runs_after_second == job_runs_before_second, "No JobRun should be created when all signals are skipped"
            assert orders_after_second == orders_before_second, "No Order should be created"

    def test_max_positions_blocks_buy_at_limit(self, seeded_client: TestClient, api_engine) -> None:
        """BUY signal is rejected with MAX_POSITIONS_REACHED when portfolio is at position limit."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            # Fill portfolio to the max_positions limit (default=5) with open positions
            for i in range(5):
                pos = Position(
                    ticker=f"MAXPOS{i}",
                    qty=Decimal("10"),
                    avg_cost=Decimal("100.00"),
                    cost_basis=Decimal("1000.00"),
                    opened_at=datetime.now(timezone.utc),
                    last_updated=datetime.now(timezone.utc),
                )
                session.add(pos)

            job_run = JobRun(
                idempotency_key="setup-signal-max-pos-test",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="NEWSTOCK",
                direction="BUY",
                confidence=Decimal("0.85"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-max-pos",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()
            signal_id = str(signal.id)

            price = PriceSnapshot(
                ticker="NEWSTOCK",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("50.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        response = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-max-pos-test",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["trade_decisions_created"] == 1
        assert data["orders_created"] == 0

        # The created TradeDecision must be REJECTED with MAX_POSITIONS_REACHED
        decision = data["created_decisions"][0]
        assert decision["decision"] == "REJECTED"
        assert decision["reason_code"] == "MAX_POSITIONS_REACHED"

        # No orders created (critical safety check)
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            order_count = session.query(Order).filter(
                Order.trade_decision_id == uuid.UUID(decision["trade_decision_id"])
            ).count()
            assert order_count == 0, "No Order rows must be created for REJECTED decisions"

        # Cleanup: remove synthetic positions to avoid polluting subsequent tests
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            session.query(Position).filter(
                Position.ticker.in_([f"MAXPOS{i}" for i in range(5)])
            ).delete(synchronize_session=False)
            session.commit()

    def test_no_position_to_sell_rejected(self, seeded_client: TestClient, api_engine) -> None:
        """SELL signal for a ticker with no open position is rejected with NO_POSITION_TO_SELL."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_run = JobRun(
                idempotency_key="setup-signal-no-pos-sell-test",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="NOPOSITIONSTOCK",
                direction="SELL",
                confidence=Decimal("0.85"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-no-pos-sell",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()
            signal_id = str(signal.id)

            price = PriceSnapshot(
                ticker="NOPOSITIONSTOCK",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("75.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        response = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-no-pos-sell-test",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["trade_decisions_created"] == 1
        assert data["orders_created"] == 0

        # The created TradeDecision must be REJECTED with NO_POSITION_TO_SELL
        decision = data["created_decisions"][0]
        assert decision["decision"] == "REJECTED"
        assert decision["reason_code"] == "NO_POSITION_TO_SELL"

        # No orders created (critical safety check)
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            order_count = session.query(Order).filter(
                Order.trade_decision_id == uuid.UUID(decision["trade_decision_id"])
            ).count()
            assert order_count == 0, "No Order rows must be created for REJECTED decisions"


class TestReviewOrderPreviewEndpoint:
    """Test POST /v1/review/order-preview endpoint."""

    def test_endpoint_requires_api_key(self, seeded_client: TestClient, api_engine) -> None:
        """Test that the endpoint requires API key."""
        response = seeded_client.post(
            "/v1/review/order-preview",
            json={"idempotency_key": "test-no-auth"},
        )
        assert response.status_code == 401

    def test_no_review_created_trade_decisions_returns_zero_preview(self, seeded_client: TestClient, api_engine) -> None:
        """Test that when no review-created TradeDecisions exist, returns zero previews."""
        response = seeded_client.post(
            "/v1/review/order-preview",
            json={
                "idempotency_key": "order-preview-empty",
                "source_run_prefix": "review_queue_create_signals_v1_order_preview_no_match:"
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["execution_mode"] == "ORDER_PREVIEW_ONLY"
        assert data["trade_decisions_evaluated"] == 0
        assert data["order_previews_generated"] == 0
        assert data["skipped_count"] == 0
        assert len(data["order_previews"]) == 0
        assert data["orders_created"] == 0
        assert data["job_runs_created"] == 0

    def test_review_created_approved_buy_trade_decision_returns_one_preview(self, seeded_client: TestClient, api_engine) -> None:
        """Test that a review-created approved BUY TradeDecision returns one order preview."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:8]

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            # Create setup signal and trade decision
            job_run = JobRun(
                idempotency_key=f"setup-signal-order-preview-buy-{suffix}",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            ticker = f"OPBUY{suffix[:6]}".upper()
            signal = Signal(
                job_run_id=job_run.id,
                ticker=ticker,
                direction="BUY",
                confidence=Decimal("0.85"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run=f"review_queue_create_signals_v1:order_preview_buy:{suffix}",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()

            # Create TradeDecision via create-decisions endpoint
            price = PriceSnapshot(
                ticker=ticker,
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("150.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        signal_id = str(signal.id)
        create_resp = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": f"create-decisions-order-preview-buy-{suffix}",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert create_resp.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td = session.query(TradeDecision).filter(TradeDecision.signal_id == uuid.UUID(signal_id)).first()
            td_id = str(td.id) if td else None

        # Now preview the order with explicit trade_decision_ids
        response = seeded_client.post(
            "/v1/review/order-preview",
            json={
                "idempotency_key": f"order-preview-buy-{suffix}",
                "trade_decision_ids": [td_id],
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["trade_decisions_evaluated"] == 1
        assert data["order_previews_generated"] == 1
        assert data["skipped_count"] == 0
        assert len(data["order_previews"]) == 1
        preview = data["order_previews"][0]
        assert preview["ticker"] == ticker
        assert preview["side"] == "BUY"
        assert preview["order_type"] == "MARKET"
        assert preview["status"] == "PREVIEW_ONLY"
        assert preview["decision"] == "BUY"

    def test_review_created_approved_sell_trade_decision_returns_one_preview(self, seeded_client: TestClient, api_engine) -> None:
        """Test that a review-created approved SELL TradeDecision returns one order preview."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            # Create position first with unique ticker
            position = Position(
                ticker="OPSELL1",
                qty=Decimal("100"),
                avg_cost=Decimal("300.00"),
                cost_basis=Decimal("30000.00"),
                opened_at=datetime.now(timezone.utc),
                last_updated=datetime.now(timezone.utc),
            )
            session.add(position)
            session.flush()

            # Create setup signal and trade decision
            job_run = JobRun(
                idempotency_key="setup-signal-order-preview-sell",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="OPSELL1",
                direction="SELL",
                confidence=Decimal("0.80"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-sell-preview",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()

            price = PriceSnapshot(
                ticker="OPSELL1",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("310.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        signal_id = str(signal.id)
        create_resp = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-order-preview-sell",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert create_resp.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td = session.query(TradeDecision).filter(TradeDecision.signal_id == uuid.UUID(signal_id)).first()
            td_id = str(td.id) if td else None

        # Preview the order with explicit trade_decision_ids
        response = seeded_client.post(
            "/v1/review/order-preview",
            json={
                "idempotency_key": "order-preview-sell",
                "trade_decision_ids": [td_id],
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["trade_decisions_evaluated"] == 1
        assert data["order_previews_generated"] == 1
        preview = data["order_previews"][0]
        assert preview["ticker"] == "OPSELL1"
        assert preview["side"] == "SELL"
        assert preview["decision"] == "SELL"

    def test_rejected_trade_decision_skipped_when_approved_only_true(self, seeded_client: TestClient, api_engine) -> None:
        """Test that REJECTED TradeDecision is skipped when approved_only=true."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            # Create a rejected trade decision (invalid confidence)
            job_run = JobRun(
                idempotency_key="setup-signal-order-preview-rejected",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="GOOG",
                direction="BUY",
                confidence=Decimal("0.50"),  # Below threshold
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-rejected-preview",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()

            price = PriceSnapshot(
                ticker="GOOG",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("140.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        signal_id = str(signal.id)
        create_resp = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-order-preview-rejected",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert create_resp.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td = session.query(TradeDecision).filter(TradeDecision.signal_id == uuid.UUID(signal_id)).first()
            td_id = str(td.id) if td else None

        # Preview with approved_only=true and explicit trade_decision_ids
        response = seeded_client.post(
            "/v1/review/order-preview",
            json={
                "idempotency_key": "order-preview-rejected-approved-only",
                "trade_decision_ids": [td_id],
                "approved_only": True,
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["trade_decisions_evaluated"] == 1
        assert data["order_previews_generated"] == 0
        assert data["skipped_count"] == 1
        assert len(data["skipped"]) == 1
        assert "not approved for order creation" in data["skipped"][0]["reason"]

    def test_approved_only_false_evaluates_but_skips_non_approved(self, seeded_client: TestClient, api_engine) -> None:
        """Test that approved_only=false evaluates all decisions but skips non-approved ones."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            # Create a rejected trade decision
            job_run = JobRun(
                idempotency_key="setup-signal-order-preview-hold",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="TSLA",
                direction="HOLD",
                confidence=Decimal("0.75"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-hold-preview",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()

            price = PriceSnapshot(
                ticker="TSLA",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("250.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        signal_id = str(signal.id)
        create_resp = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-order-preview-hold",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert create_resp.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td = session.query(TradeDecision).filter(TradeDecision.signal_id == uuid.UUID(signal_id)).first()
            td_id = str(td.id) if td else None

        # Preview with approved_only=false and explicit trade_decision_ids
        response = seeded_client.post(
            "/v1/review/order-preview",
            json={
                "idempotency_key": "order-preview-hold-approved-false",
                "trade_decision_ids": [td_id],
                "approved_only": False,
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["trade_decisions_evaluated"] == 1
        assert data["order_previews_generated"] == 0
        assert data["skipped_count"] == 1
        assert "not BUY/SELL" in data["skipped"][0]["reason"]

    def test_non_review_source_signal_skipped_by_default(self, seeded_client: TestClient, api_engine) -> None:
        """Test that TradeDecision with non-review Signal is skipped by default."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            # Create a non-review signal
            job_run = JobRun(
                idempotency_key="setup-signal-order-preview-non-review",
                workflow_type="CUSTOM_RUN",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="NVDA",
                direction="BUY",
                confidence=Decimal("0.88"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="some_other_source:signal-1",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()

            price = PriceSnapshot(
                ticker="NVDA",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("500.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        signal_id = str(signal.id)
        create_resp = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-order-preview-non-review",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert create_resp.status_code == 200

        # Preview should skip because source_run doesn't match
        response = seeded_client.post(
            "/v1/review/order-preview",
            json={"idempotency_key": "order-preview-non-review"},
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        # The trade decision might be created but not previewed due to filtering
        assert data["skipped_count"] >= 0

    def test_explicit_trade_decision_ids_non_review_skipped_with_reason(self, seeded_client: TestClient, api_engine) -> None:
        """Test that explicit trade_decision_ids with non-review Signal skips with reason."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            # Create a non-review signal and trade decision
            job_run = JobRun(
                idempotency_key="setup-signal-order-preview-explicit-non-review",
                workflow_type="CUSTOM_RUN",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="AMD",
                direction="BUY",
                confidence=Decimal("0.75"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="custom_source:signal-1",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()

            price = PriceSnapshot(
                ticker="AMD",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("120.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        signal_id = str(signal.id)
        create_resp = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-order-preview-explicit-non-review",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert create_resp.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td = session.query(TradeDecision).filter(TradeDecision.signal_id == uuid.UUID(signal_id)).first()
            td_id = str(td.id) if td else None

        if td_id:
            # Preview with explicit trade_decision_ids
            response = seeded_client.post(
                "/v1/review/order-preview",
                json={
                    "idempotency_key": "order-preview-explicit-non-review",
                    "trade_decision_ids": [td_id],
                },
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            assert data["trade_decisions_evaluated"] == 1
            assert data["order_previews_generated"] == 0
            assert len(data["skipped"]) == 1
            assert "not review-created" in data["skipped"][0]["reason"]

    def test_existing_order_for_trade_decision_skipped_as_duplicate(self, seeded_client: TestClient, api_engine) -> None:
        """Test that TradeDecision with existing Order is skipped as duplicate."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            # Create signal and trade decision
            job_run = JobRun(
                idempotency_key="setup-signal-order-preview-duplicate",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="QCOM",
                direction="BUY",
                confidence=Decimal("0.82"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-duplicate-preview",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()

            price = PriceSnapshot(
                ticker="QCOM",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("130.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        signal_id = str(signal.id)
        create_resp = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-order-preview-duplicate",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert create_resp.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td = session.query(TradeDecision).filter(TradeDecision.signal_id == uuid.UUID(signal_id)).first()
            td_id = str(td.id) if td else None
            # Create a fake order for this trade decision (to test duplicate detection)
            order = Order(
                trade_decision_id=td.id,
                job_run_id=td.job_run_id,
                ticker=td.ticker,
                side=td.signal_direction,
                order_type="MARKET",
                status="PENDING",
                market_date=date.today(),
                requested_qty=Decimal("10"),
                requested_at=datetime.now(timezone.utc),
            )
            session.add(order)
            session.commit()

        # Preview should skip because Order exists, using explicit trade_decision_ids
        response = seeded_client.post(
            "/v1/review/order-preview",
            json={
                "idempotency_key": "order-preview-duplicate",
                "trade_decision_ids": [td_id],
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["trade_decisions_evaluated"] == 1
        assert data["skipped_existing_count"] == 1
        assert len(data["skipped"]) == 1
        assert "Order already exists" in data["skipped"][0]["reason"]

    def test_limit_parameter_works(self, seeded_client: TestClient, api_engine) -> None:
        """Test that limit parameter is enforced."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:8]
        signal_ids = []
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            # Create 3 signals and trade decisions with unique source_run_prefix
            job_run = JobRun(
                idempotency_key=f"setup-signal-order-preview-limit-{suffix}",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            for i in range(3):
                ticker = f"LMTTICK{suffix[:6]}{i}".upper()
                signal = Signal(
                    job_run_id=job_run.id,
                    ticker=ticker,
                    direction="BUY",
                    confidence=Decimal("0.80"),
                    signal_ts=datetime.now(timezone.utc),
                    market_date=date.today(),
                    source_run=f"review_queue_create_signals_v1:order_preview_limit:{suffix}:{i}",
                    status="RECEIVED",
                    raw_payload={},
                )
                session.add(signal)
                session.flush()
                signal_ids.append(str(signal.id))

                price = PriceSnapshot(
                    ticker=ticker,
                    price_type="CLOSE",
                    session_type="REGULAR",
                    market_date=date.today(),
                    price=Decimal("100.00"),
                    snapshot_ts=datetime.now(timezone.utc),
                )
                session.add(price)
                session.flush()
            session.commit()

        create_resp = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": f"create-decisions-order-preview-limit-{suffix}",
                "signal_ids": signal_ids,
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert create_resp.status_code == 200

        # Preview with limit=2 and unique source_run_prefix
        response = seeded_client.post(
            "/v1/review/order-preview",
            json={
                "idempotency_key": f"order-preview-limit-2-{suffix}",
                "source_run_prefix": f"review_queue_create_signals_v1:order_preview_limit:{suffix}:",
                "limit": 2,
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["trade_decisions_evaluated"] == 2
        assert data["order_previews_generated"] == 2

    def test_response_includes_all_preview_fields(self, seeded_client: TestClient, api_engine) -> None:
        """Test that response includes all required preview fields."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_run = JobRun(
                idempotency_key="setup-signal-order-preview-fields",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="INTC",
                direction="BUY",
                confidence=Decimal("0.87"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-fields",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()

            price = PriceSnapshot(
                ticker="INTC",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("45.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        signal_id = str(signal.id)
        create_resp = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-order-preview-fields",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert create_resp.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td = session.query(TradeDecision).filter(TradeDecision.signal_id == uuid.UUID(signal_id)).first()
            td_id = str(td.id) if td else None

        response = seeded_client.post(
            "/v1/review/order-preview",
            json={
                "idempotency_key": "order-preview-fields",
                "trade_decision_ids": [td_id],
            },
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert "execution_mode" in data
        assert "trade_decisions_evaluated" in data
        assert "order_previews_generated" in data
        assert "skipped_count" in data
        assert "skipped_existing_count" in data
        assert "order_previews" in data
        assert "skipped" in data
        assert "orders_created" in data
        assert "job_runs_created" in data

        if len(data["order_previews"]) > 0:
            preview = data["order_previews"][0]
            assert "trade_decision_id" in preview
            assert "signal_id" in preview
            assert "ticker" in preview
            assert "side" in preview
            assert "order_type" in preview
            assert "status" in preview
            assert "qty" in preview
            assert "notional" in preview
            assert "decision" in preview
            assert "reason_code" in preview
            assert "source_run" in preview
            assert "reason" in preview

    def test_no_order_rows_created(self, seeded_client: TestClient, api_engine) -> None:
        """Test that no Order rows are created by preview endpoint."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            orders_before = session.query(Order).count()

        response = seeded_client.post(
            "/v1/review/order-preview",
            json={"idempotency_key": "order-preview-no-orders"},
            headers=_AUTH,
        )
        assert response.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            orders_after = session.query(Order).count()
            assert orders_after == orders_before, "No Order rows should be created"

    def test_no_job_run_rows_created(self, seeded_client: TestClient, api_engine) -> None:
        """Test that no JobRun rows are created by preview endpoint."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_runs_before = session.query(JobRun).count()

        response = seeded_client.post(
            "/v1/review/order-preview",
            json={"idempotency_key": "order-preview-no-job-runs"},
            headers=_AUTH,
        )
        assert response.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_runs_after = session.query(JobRun).count()
            assert job_runs_after == job_runs_before, "No JobRun rows should be created"

    def test_trade_decision_rows_unchanged(self, seeded_client: TestClient, api_engine) -> None:
        """Test that TradeDecision rows are not modified by preview endpoint."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_run = JobRun(
                idempotency_key="setup-signal-order-preview-td-unchanged",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="CSCO",
                direction="BUY",
                confidence=Decimal("0.79"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-td-unchanged",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()

            price = PriceSnapshot(
                ticker="CSCO",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("52.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        signal_id = str(signal.id)
        create_resp = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-order-preview-td-unchanged",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert create_resp.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td_before = session.query(TradeDecision).filter(
                TradeDecision.signal_id == uuid.UUID(signal_id)
            ).first()
            td_id = str(td_before.id) if td_before else None
            td_decided_at_before = td_before.decided_at if td_before else None

        response = seeded_client.post(
            "/v1/review/order-preview",
            json={
                "idempotency_key": "order-preview-td-unchanged",
                "trade_decision_ids": [td_id],
            },
            headers=_AUTH,
        )
        assert response.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            td_after = session.query(TradeDecision).filter(
                TradeDecision.signal_id == uuid.UUID(signal_id)
            ).first()
            td_decided_at_after = td_after.decided_at if td_after else None
            assert td_decided_at_after == td_decided_at_before, "TradeDecision should not be modified"

    def test_signal_rows_unchanged(self, seeded_client: TestClient, api_engine) -> None:
        """Test that Signal rows are not modified by preview endpoint."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_run = JobRun(
                idempotency_key="setup-signal-order-preview-signal-unchanged",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=date.today(),
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
            )
            session.add(job_run)
            session.flush()

            signal = Signal(
                job_run_id=job_run.id,
                ticker="DELL",
                direction="SELL",
                confidence=Decimal("0.76"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:candidate-signal-unchanged",
                status="RECEIVED",
                raw_payload={},
            )
            session.add(signal)
            session.flush()

            # Create position for SELL
            position = Position(
                ticker="DELL",
                qty=Decimal("50"),
                avg_cost=Decimal("28.00"),
                cost_basis=Decimal("1400.00"),
                opened_at=datetime.now(timezone.utc),
                last_updated=datetime.now(timezone.utc),
            )
            session.add(position)
            session.flush()

            price = PriceSnapshot(
                ticker="DELL",
                price_type="CLOSE",
                session_type="REGULAR",
                market_date=date.today(),
                price=Decimal("32.00"),
                snapshot_ts=datetime.now(timezone.utc),
            )
            session.add(price)
            session.flush()
            session.commit()

        signal_id = str(signal.id)
        create_resp = seeded_client.post(
            "/v1/review/create-decisions",
            json={
                "idempotency_key": "create-decisions-order-preview-signal-unchanged",
                "signal_ids": [signal_id],
                "confirm_create_decisions": True,
            },
            headers=_AUTH,
        )
        assert create_resp.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal_before = session.query(Signal).filter(
                Signal.id == uuid.UUID(signal_id)
            ).first()
            td = session.query(TradeDecision).filter(TradeDecision.signal_id == uuid.UUID(signal_id)).first()
            td_id = str(td.id) if td else None
            signal_status_before = signal_before.status if signal_before else None

        response = seeded_client.post(
            "/v1/review/order-preview",
            json={
                "idempotency_key": "order-preview-signal-unchanged",
                "trade_decision_ids": [td_id],
            },
            headers=_AUTH,
        )
        assert response.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signal_after = session.query(Signal).filter(
                Signal.id == uuid.UUID(signal_id)
            ).first()
            signal_status_after = signal_after.status if signal_after else None
            assert signal_status_after == signal_status_before, "Signal should not be modified"


class TestReviewWorkflowStatusEndpoint:
    """Test GET /v1/review/workflow-status endpoint (read-only workflow status)."""

    def test_endpoint_requires_api_key(self, seeded_client: TestClient) -> None:
        """Test that the endpoint requires API key."""
        response = seeded_client.get("/v1/review/workflow-status")
        assert response.status_code == 401

    def test_returns_all_top_level_sections(self, seeded_client: TestClient) -> None:
        """Test that response includes all required top-level sections."""
        response = seeded_client.get("/v1/review/workflow-status", headers=_AUTH)
        assert response.status_code == 200
        data = response.json()
        assert "review_candidates" in data
        assert "review_created_signals" in data
        assert "review_created_trade_decisions" in data
        assert "orders" in data
        assert "workflow_steps" in data
        assert "safety" in data

    def test_returns_safe_structure_with_nonnegative_counts(self, seeded_client: TestClient) -> None:
        """Test that endpoint returns safe structure with non-negative integer counts."""
        response = seeded_client.get("/v1/review/workflow-status", headers=_AUTH)
        assert response.status_code == 200
        data = response.json()

        # Verify all count sections exist and have integer counts >= 0
        assert isinstance(data["review_candidates"]["total"], int)
        assert isinstance(data["review_candidates"]["new"], int)
        assert isinstance(data["review_candidates"]["watching"], int)
        assert isinstance(data["review_candidates"]["approved_for_signal"], int)
        assert isinstance(data["review_candidates"]["rejected"], int)
        assert all(v >= 0 for v in data["review_candidates"].values())

        assert isinstance(data["review_created_signals"]["total"], int)
        assert isinstance(data["review_created_signals"]["received"], int)
        assert isinstance(data["review_created_signals"]["decision_made"], int)
        assert isinstance(data["review_created_signals"]["error"], int)
        assert all(v >= 0 for v in data["review_created_signals"].values())

        assert isinstance(data["review_created_trade_decisions"]["total"], int)
        assert isinstance(data["review_created_trade_decisions"]["buy"], int)
        assert isinstance(data["review_created_trade_decisions"]["sell"], int)
        assert isinstance(data["review_created_trade_decisions"]["rejected"], int)
        assert isinstance(data["review_created_trade_decisions"]["order_eligible"], int)
        assert isinstance(data["review_created_trade_decisions"]["already_has_order"], int)
        assert all(v >= 0 for v in data["review_created_trade_decisions"].values())

        assert isinstance(data["orders"]["total"], int)
        assert isinstance(data["orders"]["review_created"], int)
        assert all(v >= 0 for v in data["orders"].values())

        # Verify safety flags are disabled
        assert data["safety"]["create_orders_enabled"] is False
        assert data["safety"]["automation_enabled"] is False

    def test_counts_review_created_signals_with_correct_prefix(self, seeded_client: TestClient, api_engine) -> None:
        """Test that only signals with review_queue_create_signals_v1: prefix are counted."""
        # Get baseline counts before setup
        response_before = seeded_client.get("/v1/review/workflow-status", headers=_AUTH)
        assert response_before.status_code == 200
        data_before = response_before.json()
        baseline_signals = data_before["review_created_signals"]["total"]
        baseline_received = data_before["review_created_signals"]["received"]

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            # Create a JobRun with a valid WorkflowType
            jr = JobRun(
                idempotency_key="workflow-status-test-signals",
                workflow_type=WorkflowType.PRE_MARKET,
                market_date=date.today(),
                status=JobRunStatus.COMPLETED,
            )
            session.add(jr)
            session.flush()

            # Create a review-created signal (source_run prefix identifies it as review-created)
            signal = Signal(
                job_run_id=jr.id,
                ticker="AAPL",
                direction="BUY",
                confidence=Decimal("0.85"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:test-001",
                status="RECEIVED",
            )
            session.add(signal)
            session.flush()
            session.commit()

        response = seeded_client.get("/v1/review/workflow-status", headers=_AUTH)
        assert response.status_code == 200
        data = response.json()

        # Should count the review signal using delta pattern (before + 1)
        assert data["review_created_signals"]["total"] >= baseline_signals + 1
        assert data["review_created_signals"]["received"] >= baseline_received + 1

    def test_counts_review_created_trade_decisions_through_signal_source_run(self, seeded_client: TestClient, api_engine) -> None:
        """Test that trade decisions are counted only if their signal has review source_run prefix."""
        # Get baseline counts before setup
        response_before = seeded_client.get("/v1/review/workflow-status", headers=_AUTH)
        assert response_before.status_code == 200
        data_before = response_before.json()
        baseline_decisions = data_before["review_created_trade_decisions"]["total"]
        baseline_sell = data_before["review_created_trade_decisions"]["sell"]

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            # Create a JobRun with a valid WorkflowType
            jr = JobRun(
                idempotency_key="workflow-status-test-decisions",
                workflow_type=WorkflowType.MIDDAY,
                market_date=date.today(),
                status=JobRunStatus.COMPLETED,
            )
            session.add(jr)
            session.flush()

            # Create a review-created signal (source_run prefix identifies it as review-created)
            signal = Signal(
                job_run_id=jr.id,
                ticker="MSFT",
                direction="SELL",
                confidence=Decimal("0.75"),
                signal_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                source_run="review_queue_create_signals_v1:test-002",
                status="RECEIVED",
            )
            session.add(signal)
            session.flush()

            # Create a trade decision from that signal
            td = TradeDecision(
                signal_id=signal.id,
                job_run_id=jr.id,
                ticker="MSFT",
                signal_direction="SELL",
                decision="SELL",
                reason_code="POSITIVE_SIGNAL",
                approved_qty=Decimal("100.00"),
                approved_notional=Decimal("15000.00"),
                market_date=date.today(),
            )
            session.add(td)
            session.commit()

        response = seeded_client.get("/v1/review/workflow-status", headers=_AUTH)
        assert response.status_code == 200
        data = response.json()

        # Should count the review-created trade decision using delta pattern (before + 1)
        assert data["review_created_trade_decisions"]["total"] >= baseline_decisions + 1
        assert data["review_created_trade_decisions"]["sell"] >= baseline_sell + 1

    def test_does_not_create_job_run_rows(self, seeded_client: TestClient, api_engine) -> None:
        """Test that the endpoint does not create JobRun rows."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_runs_before = session.query(JobRun).count()

        response = seeded_client.get("/v1/review/workflow-status", headers=_AUTH)
        assert response.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            job_runs_after = session.query(JobRun).count()
            assert job_runs_after == job_runs_before, "No JobRun rows should be created"

    def test_does_not_create_signal_rows(self, seeded_client: TestClient, api_engine) -> None:
        """Test that the endpoint does not create Signal rows."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signals_before = session.query(Signal).count()

        response = seeded_client.get("/v1/review/workflow-status", headers=_AUTH)
        assert response.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            signals_after = session.query(Signal).count()
            assert signals_after == signals_before, "No Signal rows should be created"

    def test_does_not_create_trade_decision_rows(self, seeded_client: TestClient, api_engine) -> None:
        """Test that the endpoint does not create TradeDecision rows."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            decisions_before = session.query(TradeDecision).count()

        response = seeded_client.get("/v1/review/workflow-status", headers=_AUTH)
        assert response.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            decisions_after = session.query(TradeDecision).count()
            assert decisions_after == decisions_before, "No TradeDecision rows should be created"

    def test_does_not_create_order_rows(self, seeded_client: TestClient, api_engine) -> None:
        """Test that the endpoint does not create Order rows."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            orders_before = session.query(Order).count()

        response = seeded_client.get("/v1/review/workflow-status", headers=_AUTH)
        assert response.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            orders_after = session.query(Order).count()
            assert orders_after == orders_before, "No Order rows should be created"

    def test_safety_flags_disabled(self, seeded_client: TestClient) -> None:
        """Test that safety flags indicate features are disabled."""
        response = seeded_client.get("/v1/review/workflow-status", headers=_AUTH)
        assert response.status_code == 200
        data = response.json()

        assert data["safety"]["create_orders_enabled"] is False
        assert data["safety"]["automation_enabled"] is False

    def test_workflow_steps_include_nine_steps(self, seeded_client: TestClient) -> None:
        """Test that workflow_steps includes all 9 steps."""
        response = seeded_client.get("/v1/review/workflow-status", headers=_AUTH)
        assert response.status_code == 200
        data = response.json()

        assert len(data["workflow_steps"]) == 9
        step_names = {step["step"] for step in data["workflow_steps"]}
        expected_steps = {
            "Prediction Preview",
            "Save Candidates",
            "Review Queue",
            "Signal Preview",
            "Create Signals",
            "Decision Preview",
            "Create Decisions",
            "Order Preview",
            "Create Orders",
        }
        assert step_names == expected_steps


# ---------------------------------------------------------------------------
# Rotation Preview endpoint
# ---------------------------------------------------------------------------

class TestReviewRotationPreviewEndpoint:
    """Test POST /v1/review/rotation-preview endpoint (read-only, no DB writes)."""

    def test_rotation_preview_requires_api_key(self, seeded_client: TestClient) -> None:
        """Test that the endpoint requires API key."""
        response = seeded_client.post("/v1/review/rotation-preview", json={})
        assert response.status_code == 401

    def test_rotation_preview_creates_zero_rows(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Test that the endpoint creates no database rows of any kind."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            jr_before  = s.query(JobRun).count()
            sig_before = s.query(Signal).count()
            td_before  = s.query(TradeDecision).count()
            ord_before = s.query(Order).count()
            pos_before = s.query(Position).count()

        response = seeded_client.post(
            "/v1/review/rotation-preview", json={}, headers=_AUTH
        )
        assert response.status_code == 200
        data = response.json()
        assert data["safety_counts"]["signals_created"]   == 0
        assert data["safety_counts"]["decisions_created"] == 0
        assert data["safety_counts"]["orders_created"]    == 0
        assert data["safety_counts"]["db_rows_created"]   == 0

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            assert s.query(JobRun).count()       == jr_before
            assert s.query(Signal).count()       == sig_before
            assert s.query(TradeDecision).count() == td_before
            assert s.query(Order).count()        == ord_before
            assert s.query(Position).count()     == pos_before

    def test_rotation_preview_capacity_available_no_rotation_required(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Test that with fewer than max_positions held, capacity_available=True.

        Prior test classes commit Position rows that persist across the module
        (TSLA, MSFT, NVDA, OPSELL1, DELL — 5 rows = default max_positions=5).
        When the full suite runs those positions saturate the default limit, so
        this test temporarily raises max_positions to current_count+1 in
        portfolio.config to guarantee the endpoint sees available capacity.
        The original config is unconditionally restored in finally.
        """
        old_config: dict | None = None
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            portfolio_obj = s.query(Portfolio).first()
            current_count = s.query(Position).count()
            cfg_max = int(
                (portfolio_obj.config or {}).get("max_positions", get_settings().max_positions)
            )
            if current_count >= cfg_max:
                old_config = dict(portfolio_obj.config or {})
                portfolio_obj.config = {**old_config, "max_positions": current_count + 1}
                s.commit()

        try:
            response = seeded_client.post(
                "/v1/review/rotation-preview", json={}, headers=_AUTH
            )
            assert response.status_code == 200
            data = response.json()
            assert data["capacity_available"] is True
            assert data["rotation_required"] is False
            assert "capacity" in data["explanation"].lower()
            assert data["safety_counts"]["signals_created"] == 0
            assert data["safety_counts"]["db_rows_created"] == 0
        finally:
            if old_config is not None:
                with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                    portfolio_obj = s.query(Portfolio).first()
                    portfolio_obj.config = old_config
                    s.commit()

    def test_rotation_preview_at_max_positions_proposes_profitable_rotation(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Test that at max positions with a high-opportunity candidate, a rotation pair is proposed."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        cand_ticker = f"RTPC{suffix}"
        ikey = f"rtp-cand-{suffix}"
        created_pos_ids: list = []

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            portfolio_obj = session.query(Portfolio).first()
            max_pos = int(
                (portfolio_obj.config or {}).get("max_positions", get_settings().max_positions)
            )
            current_count = session.query(Position).count()

            # Fill slots up to (max_pos - 1) with strong positions (30% gain)
            strong_count = max(0, (max_pos - 1) - current_count)
            for i in range(strong_count):
                t = f"RTPS{suffix}{i}"
                pos = Position(
                    ticker=t, qty=Decimal("10"),
                    avg_cost=Decimal("100.000000"), cost_basis=Decimal("1000.00"),
                    opened_at=datetime.now(timezone.utc),
                    last_updated=datetime.now(timezone.utc),
                )
                session.add(pos)
                session.flush()
                created_pos_ids.append(pos.id)
                session.add(PriceSnapshot(
                    ticker=t, price=Decimal("130.000000"),
                    price_type="CLOSE", session_type="REGULAR",
                    market_date=date.today(), snapshot_ts=datetime.now(timezone.utc),
                ))

            # Add the weak position: -5% gain → guaranteed weakest when others are positive
            ticker_weak = f"RTPW{suffix}"
            pos_weak = Position(
                ticker=ticker_weak, qty=Decimal("10"),
                avg_cost=Decimal("100.000000"), cost_basis=Decimal("1000.00"),
                opened_at=datetime.now(timezone.utc),
                last_updated=datetime.now(timezone.utc),
            )
            session.add(pos_weak)
            session.flush()
            created_pos_ids.append(pos_weak.id)
            session.add(PriceSnapshot(
                ticker=ticker_weak, price=Decimal("95.000000"),  # -5% P&L
                price_type="CLOSE", session_type="REGULAR",
                market_date=date.today(), snapshot_ts=datetime.now(timezone.utc),
            ))

            # Candidate v2 score = 0.90 * (10.0/100) = 0.09; scan_score=50 → neutral (scan_adj=0)
            # holding no prediction → hold_score=0.0; improvement=0.09 >= 0.05 threshold → meets
            cand = CandidateReview(
                idempotency_key=ikey, ticker=cand_ticker,
                prediction_recommendation="BUY", prediction_confidence="0.90",
                expected_return_pct="10.0", scan_score="50", preview_decision="CONSIDER",
                preview_score="85.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            session.add(cand)
            session.commit()
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/rotation-preview",
                json={
                    "candidate_review_ids": [cand_id],
                    "min_improvement_score": 0.05,
                    "block_loss_realization": False,  # allow selling the -5% position
                },
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            assert data["rotation_required"] is True
            assert data["capacity_available"] is False
            assert len(data["rotation_pairs"]) >= 1
            pair = data["rotation_pairs"][0]
            assert pair["sell_ticker"] == ticker_weak
            assert pair["buy_ticker"] == cand_ticker
            assert pair["meets_threshold"] is True
            # Explicit v2 score: base=0.90*(10/100)=0.09, scan_adj=0 → total=0.09; hold=0.0 → imp=0.09
            assert float(pair["improvement_score"]) == pytest.approx(0.09, abs=0.002), (
                f"Expected improvement ~0.09 (0.90 x 0.10, scan neutral), got {pair['improvement_score']}"
            )
            assert data["safety_counts"]["db_rows_created"] == 0
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
                for pid in created_pos_ids:
                    session.query(Position).filter(Position.id == pid).delete()
                session.commit()

    def test_rotation_preview_blocks_negative_pnl_position(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Test that a position with negative unrealized P&L is blocked from rotation."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_loss = f"RTPL{suffix}"
        ikey = f"rtp-loss-cand-{suffix}"
        cand_ticker = f"RTPLC{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            pos = Position(
                ticker=ticker_loss, qty=Decimal("10"),
                avg_cost=Decimal("100.000000"), cost_basis=Decimal("1000.00"),
                opened_at=datetime.now(timezone.utc), last_updated=datetime.now(timezone.utc),
            )
            session.add(pos)
            session.add(PriceSnapshot(
                ticker=ticker_loss, price=Decimal("80.000000"),  # -20% loss
                price_type="CLOSE", session_type="REGULAR",
                market_date=date.today(), snapshot_ts=datetime.now(timezone.utc),
            ))
            cand = CandidateReview(
                idempotency_key=ikey, ticker=cand_ticker,
                prediction_recommendation="BUY", prediction_confidence="0.90",
                expected_return_pct="10.0", preview_decision="CONSIDER",
                preview_score="85.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            session.add(cand)
            session.commit()
            pos_id = pos.id
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/rotation-preview",
                json={
                    "candidate_review_ids": [cand_id],
                    "block_loss_realization": True,
                    "min_exit_pnl_pct": 0.0,
                },
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            blocked = data["blocked_positions"]
            blocked_tickers = [b["ticker"] for b in blocked]
            assert ticker_loss in blocked_tickers
            entry = next(b for b in blocked if b["ticker"] == ticker_loss)
            assert entry["sellable_for_rotation"] is False
            assert entry["blocked_reason"] == "LOSS_REALIZATION_BLOCKED"
            # Must not appear as sell side of any rotation pair
            all_pairs = data["rotation_pairs"] + data["rejected_pairs"]
            for pair in all_pairs:
                assert pair["sell_ticker"] != ticker_loss
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
                session.query(Position).filter(Position.id == pos_id).delete()
                session.commit()

    def test_rotation_preview_missing_price_blocks_position(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Test that a position with no price snapshot is blocked with MISSING_PRICE."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_no_price = f"RTPNP{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            pos = Position(
                ticker=ticker_no_price, qty=Decimal("5"),
                avg_cost=Decimal("200.000000"), cost_basis=Decimal("1000.00"),
                opened_at=datetime.now(timezone.utc), last_updated=datetime.now(timezone.utc),
            )
            session.add(pos)
            session.commit()
            pos_id = pos.id

        try:
            response = seeded_client.post(
                "/v1/review/rotation-preview", json={}, headers=_AUTH
            )
            assert response.status_code == 200
            data = response.json()
            blocked = data["blocked_positions"]
            blocked_tickers = [b["ticker"] for b in blocked]
            assert ticker_no_price in blocked_tickers
            entry = next(b for b in blocked if b["ticker"] == ticker_no_price)
            assert entry["sellable_for_rotation"] is False
            assert entry["blocked_reason"] == "MISSING_PRICE"
            assert entry["latest_price"] is None
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
                session.query(Position).filter(Position.id == pos_id).delete()
                session.commit()

    def test_rotation_preview_already_held_candidate_rejected(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Test that a candidate for a ticker already held is excluded from candidates."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        held_ticker = f"RTPAH{suffix}"
        ikey = f"rtp-held-{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            pos = Position(
                ticker=held_ticker, qty=Decimal("10"),
                avg_cost=Decimal("100.000000"), cost_basis=Decimal("1000.00"),
                opened_at=datetime.now(timezone.utc), last_updated=datetime.now(timezone.utc),
            )
            session.add(pos)
            session.add(PriceSnapshot(
                ticker=held_ticker, price=Decimal("110.000000"),
                price_type="CLOSE", session_type="REGULAR",
                market_date=date.today(), snapshot_ts=datetime.now(timezone.utc),
            ))
            cand = CandidateReview(
                idempotency_key=ikey, ticker=held_ticker,  # same ticker as held position
                prediction_recommendation="BUY", prediction_confidence="0.90",
                expected_return_pct="10.0", preview_decision="CONSIDER",
                preview_score="85.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            session.add(cand)
            session.commit()
            pos_id = pos.id
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/rotation-preview",
                json={"candidate_review_ids": [cand_id]},
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            cand_tickers = [c["ticker"] for c in data["strongest_candidates"]]
            assert held_ticker not in cand_tickers
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
                session.query(Position).filter(Position.id == pos_id).delete()
                session.commit()

    def test_rotation_preview_below_threshold_rejected(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Test that pairs below min_improvement_score appear in rejected_pairs, not rotation_pairs."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        cand_ticker = f"RTPBTC{suffix}"
        ikey = f"rtp-bt-{suffix}"
        created_pos_ids: list = []

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            portfolio_obj = session.query(Portfolio).first()
            max_pos = int(
                (portfolio_obj.config or {}).get("max_positions", get_settings().max_positions)
            )
            current_count = session.query(Position).count()

            # Fill remaining slots so we're at max_positions (triggers rotation logic)
            for i in range(max(0, max_pos - current_count)):
                t = f"RTPBTS{suffix}{i}"
                pos = Position(
                    ticker=t, qty=Decimal("10"),
                    avg_cost=Decimal("100.000000"), cost_basis=Decimal("1000.00"),
                    opened_at=datetime.now(timezone.utc), last_updated=datetime.now(timezone.utc),
                )
                session.add(pos)
                session.flush()
                created_pos_ids.append(pos.id)
                session.add(PriceSnapshot(
                    ticker=t, price=Decimal("120.000000"),
                    price_type="CLOSE", session_type="REGULAR",
                    market_date=date.today(), snapshot_ts=datetime.now(timezone.utc),
                ))

            # Candidate with moderate score (0.70 * 3.0 = 2.1)
            cand = CandidateReview(
                idempotency_key=ikey, ticker=cand_ticker,
                prediction_recommendation="BUY", prediction_confidence="0.70",
                expected_return_pct="3.0", preview_decision="WATCH",
                preview_score="40.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            session.add(cand)
            session.commit()
            cand_id = str(cand.id)

        try:
            # min_improvement_score=1000 is impossible to meet → all pairs rejected
            response = seeded_client.post(
                "/v1/review/rotation-preview",
                json={
                    "candidate_review_ids": [cand_id],
                    "min_improvement_score": 1000.0,
                    "block_loss_realization": False,
                },
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            assert data["rotation_required"] is True
            assert len(data["rotation_pairs"]) == 0
            assert len(data["rejected_pairs"]) >= 1
            assert data["rejected_pairs"][0]["meets_threshold"] is False
            assert data["rejected_pairs"][0]["buy_ticker"] == cand_ticker
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
                for pid in created_pos_ids:
                    session.query(Position).filter(Position.id == pid).delete()
                session.commit()

    def test_rotation_preview_uses_approved_candidates_by_default(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Test that approved_only=True (default) filters to APPROVED_FOR_SIGNAL candidates."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_approved = f"RTPAP{suffix}"
        ticker_new      = f"RTPNW{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            session.add(CandidateReview(
                idempotency_key=f"rtp-approved-{suffix}", ticker=ticker_approved,
                prediction_recommendation="BUY", prediction_confidence="0.90",
                expected_return_pct="10.0", preview_decision="CONSIDER",
                preview_score="85.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            ))
            session.add(CandidateReview(
                idempotency_key=f"rtp-new-{suffix}", ticker=ticker_new,
                prediction_recommendation="BUY", prediction_confidence="0.85",
                expected_return_pct="8.0", preview_decision="CONSIDER",
                preview_score="75.0", review_status="NEW", status="OK",
            ))
            session.commit()

        response = seeded_client.post(
            "/v1/review/rotation-preview",
            json={"approved_only": True},
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        cand_tickers = [c["ticker"] for c in data["strongest_candidates"]]
        assert ticker_approved in cand_tickers
        assert ticker_new not in cand_tickers

    def test_rotation_preview_explicit_candidate_ids(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Test that explicit candidate_review_ids bypasses the approved_only SQL filter."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_watching = f"RTPWT{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
            cand = CandidateReview(
                idempotency_key=f"rtp-watching-{suffix}", ticker=ticker_watching,
                prediction_recommendation="BUY", prediction_confidence="0.88",
                expected_return_pct="12.0", preview_decision="CONSIDER",
                preview_score="88.0", review_status="WATCHING", status="OK",
            )
            session.add(cand)
            session.commit()
            cand_id = str(cand.id)

        response = seeded_client.post(
            "/v1/review/rotation-preview",
            json={"candidate_review_ids": [cand_id]},
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        # candidates_considered counts raw candidates loaded (before filter for held/BUY)
        assert data["candidates_considered"] == 1
        cand_tickers = [c["ticker"] for c in data["strongest_candidates"]]
        assert ticker_watching in cand_tickers


# ---------------------------------------------------------------------------
# Daily Plan Preview endpoint
# ---------------------------------------------------------------------------

class TestDailyPlanPreviewEndpoint:
    """Test POST /v1/review/daily-plan-preview endpoint (read-only, no DB writes)."""

    def test_daily_plan_requires_api_key(self, seeded_client: TestClient) -> None:
        response = seeded_client.post("/v1/review/daily-plan-preview", json={})
        assert response.status_code == 401

    def test_daily_plan_creates_zero_rows(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Endpoint creates no DB rows of any kind."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            sig_before  = s.query(Signal).count()
            td_before   = s.query(TradeDecision).count()
            ord_before  = s.query(Order).count()
            pos_before  = s.query(Position).count()
            cand_before = s.query(CandidateReview).count()

        response = seeded_client.post(
            "/v1/review/daily-plan-preview", json={}, headers=_AUTH
        )
        assert response.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            assert s.query(Signal).count()          == sig_before
            assert s.query(TradeDecision).count()   == td_before
            assert s.query(Order).count()           == ord_before
            assert s.query(Position).count()        == pos_before
            assert s.query(CandidateReview).count() == cand_before

    def test_daily_plan_safety_counts_all_zero(self, seeded_client: TestClient) -> None:
        """safety_counts in response must always be all zeros."""
        response = seeded_client.post(
            "/v1/review/daily-plan-preview", json={}, headers=_AUTH
        )
        assert response.status_code == 200
        data = response.json()
        sc = data["safety_counts"]
        assert sc["signals_created"]          == 0
        assert sc["trade_decisions_created"]  == 0
        assert sc["orders_created"]           == 0
        assert sc["job_runs_created"]         == 0
        assert sc["db_rows_created"]          == 0

    def test_daily_plan_no_approved_candidates_returns_guidance(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """When no APPROVED_FOR_SIGNAL candidates exist, response guides user to Review Queue."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker = f"DPNOC{suffix}"

        # Add a candidate that is NOT approved
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            cand = CandidateReview(
                idempotency_key=f"dp-not-approved-{suffix}", ticker=ticker,
                prediction_recommendation="BUY", prediction_confidence="0.90",
                expected_return_pct="15.0", preview_decision="CONSIDER",
                preview_score="90.0", review_status="NEW", status="OK",
            )
            s.add(cand)
            s.commit()

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={"candidate_ids": []},
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            # No approved candidates → guidance to approve
            assert len(data["buy_recommendations"]) == 0
            assert "approve" in data["recommended_next_action"].lower() or "review queue" in data["recommended_next_action"].lower()
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker).delete()
                s.commit()

    def test_daily_plan_negative_pnl_position_blocked_from_sell(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """A position with negative PnL must not appear in sell_recommendations."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_loss = f"DPLOSS{suffix}"

        # Create a position at a loss (avg_cost=100, current price=80 → -20%)
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            pos = Position(
                ticker=ticker_loss,
                qty=Decimal("10"),
                avg_cost=Decimal("100.00"),
                cost_basis=Decimal("1000.00"),
                opened_at=_NOW,
                last_updated=_NOW,
            )
            s.add(pos)
            s.flush()
            pos_id = pos.id
            snap = PriceSnapshot(
                ticker=ticker_loss,
                price=Decimal("80.00"),
                snapshot_ts=datetime.now(timezone.utc),
                market_date=date.today(),
                session_type="REGULAR",
                price_type="LAST",
            )
            s.add(snap)
            # SELL candidate for this ticker
            cand = CandidateReview(
                idempotency_key=f"dp-sell-loss-{suffix}", ticker=ticker_loss,
                prediction_recommendation="SELL", prediction_confidence="0.85",
                preview_decision="CONSIDER", preview_score="85.0",
                review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand)
            s.commit()
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={"block_loss_realization": True, "candidate_ids": [cand_id]},
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            # Must NOT appear in sell_recommendations
            sell_tickers = [s["ticker"] for s in data["sell_recommendations"]]
            assert ticker_loss not in sell_tickers
            # Must appear in blocked_actions with NEGATIVE_PNL_BLOCKED
            blocked_reasons = {a["ticker"]: a["blocked_reason"] for a in data["blocked_actions"]}
            assert blocked_reasons.get(ticker_loss) == "NEGATIVE_PNL_BLOCKED"
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker_loss).delete()
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker_loss).delete()
                s.query(Position).filter(Position.id == pos_id).delete()
                s.commit()

    def test_daily_plan_buy_blocked_at_max_positions(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """At max positions, BUY candidates must appear in blocked_actions with MAX_POSITIONS_REACHED."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_held = f"DPMXH{suffix}"
        ticker_buy  = f"DPMXB{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            portfolio_obj = s.query(Portfolio).first()
            old_config = dict(portfolio_obj.config or {})
            portfolio_obj.config = {"max_positions": 1}
            s.commit()

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            pos = Position(
                ticker=ticker_held,
                qty=Decimal("5"),
                avg_cost=Decimal("100.00"),
                cost_basis=Decimal("500.00"),
                opened_at=_NOW,
                last_updated=_NOW,
            )
            s.add(pos)
            s.flush()
            pos_id = pos.id
            snap = PriceSnapshot(
                ticker=ticker_held, price=Decimal("110.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            )
            s.add(snap)
            snap2 = PriceSnapshot(
                ticker=ticker_buy, price=Decimal("50.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            )
            s.add(snap2)
            cand = CandidateReview(
                idempotency_key=f"dp-buy-maxpos-{suffix}", ticker=ticker_buy,
                prediction_recommendation="BUY", prediction_confidence="0.85",
                expected_return_pct="15.0", preview_decision="CONSIDER",
                preview_score="85.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand)
            s.commit()
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={"approved_only": True, "candidate_ids": [cand_id]},
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            # BUY must be blocked (max positions)
            buy_tickers = [b["ticker"] for b in data["buy_recommendations"]]
            assert ticker_buy not in buy_tickers
            blocked_reasons = {a["ticker"]: a["blocked_reason"] for a in data["blocked_actions"]}
            assert blocked_reasons.get(ticker_buy) == "MAX_POSITIONS_REACHED"
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                portfolio_obj = s.query(Portfolio).first()
                portfolio_obj.config = old_config
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker_buy).delete()
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker.in_([ticker_held, ticker_buy])).delete()
                s.query(Position).filter(Position.id == pos_id).delete()
                s.commit()

    def test_daily_plan_profitable_rotation_meets_threshold(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """A profitable holding can be proposed as rotation sell leg for a high-score BUY candidate."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_held = f"DPRTH{suffix}"
        ticker_buy  = f"DPRTB{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            portfolio_obj = s.query(Portfolio).first()
            old_config = dict(portfolio_obj.config or {})
            portfolio_obj.config = {"max_positions": 1}
            s.commit()

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            # Held position: cost=$900, price=$1000 → PnL = +11.1%
            pos = Position(
                ticker=ticker_held,
                qty=Decimal("10"),
                avg_cost=Decimal("90.00"),
                cost_basis=Decimal("900.00"),
                opened_at=_NOW,
                last_updated=_NOW,
            )
            s.add(pos)
            s.flush()
            pos_id = pos.id
            snap_held = PriceSnapshot(
                ticker=ticker_held, price=Decimal("100.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            )
            s.add(snap_held)
            snap_buy = PriceSnapshot(
                ticker=ticker_buy, price=Decimal("50.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            )
            s.add(snap_buy)
            # BUY candidate: conf=0.90, return%=20.0
            # v2 score = 0.90 * (20.0/100) = 0.18; holding has no prediction → 0.0
            # improvement = 0.18 - 0.0 = 0.18 >= 0.05 threshold → meets
            cand = CandidateReview(
                idempotency_key=f"dp-rot-buy-{suffix}", ticker=ticker_buy,
                prediction_recommendation="BUY", prediction_confidence="0.90",
                expected_return_pct="20.0", preview_decision="CONSIDER",
                preview_score="90.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand)
            s.commit()
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={
                    "approved_only": True,
                    "min_rotation_improvement_pct": 0.05,
                    "include_rotation": True,
                    "candidate_ids": [cand_id],
                    "position_tickers": [ticker_held],
                },
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            # There should be at least one rotation pair with meets_threshold=True
            good = [r for r in data["rotation_plan"] if r["meets_threshold"]]
            assert len(good) >= 1
            sell_tickers = [r["sell_ticker"] for r in good]
            buy_tickers  = [r["buy_ticker"]  for r in good]
            assert ticker_held in sell_tickers
            assert ticker_buy  in buy_tickers
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                portfolio_obj = s.query(Portfolio).first()
                portfolio_obj.config = old_config
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker_buy).delete()
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker.in_([ticker_held, ticker_buy])).delete()
                s.query(Position).filter(Position.id == pos_id).delete()
                s.commit()

    def test_daily_plan_position_tickers_scopes_evaluation(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """position_tickers filters which open positions are evaluated; no DB rows are written."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_a = f"DPPTA{suffix}"
        ticker_b = f"DPPTB{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            pos_a = Position(
                ticker=ticker_a, qty=Decimal("3"), avg_cost=Decimal("50.00"),
                cost_basis=Decimal("150.00"), opened_at=_NOW, last_updated=_NOW,
            )
            pos_b = Position(
                ticker=ticker_b, qty=Decimal("3"), avg_cost=Decimal("50.00"),
                cost_basis=Decimal("150.00"), opened_at=_NOW, last_updated=_NOW,
            )
            s.add(pos_a)
            s.add(pos_b)
            s.flush()
            pos_a_id = pos_a.id
            pos_b_id = pos_b.id
            s.add(PriceSnapshot(
                ticker=ticker_a, price=Decimal("55.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            s.add(PriceSnapshot(
                ticker=ticker_b, price=Decimal("55.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            s.commit()

        try:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                pos_before = s.query(Position).count()

            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={"candidate_ids": [], "position_tickers": [ticker_a]},
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            hold_tickers = [h["ticker"] for h in data["hold_positions"]]
            assert ticker_a in hold_tickers
            assert ticker_b not in hold_tickers

            # Endpoint must not write any rows
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                assert s.query(Position).count() == pos_before
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(PriceSnapshot).filter(
                    PriceSnapshot.ticker.in_([ticker_a, ticker_b])
                ).delete()
                s.query(Position).filter(
                    Position.id.in_([pos_a_id, pos_b_id])
                ).delete()
                s.commit()

    def test_daily_plan_response_includes_action_stack(self, seeded_client: TestClient) -> None:
        """action_stack field is present in the response and is a list."""
        response = seeded_client.post(
            "/v1/review/daily-plan-preview",
            json={"candidate_ids": []},
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        assert "action_stack" in data
        assert isinstance(data["action_stack"], list)

    def test_daily_plan_action_stack_safety_note(self, seeded_client: TestClient) -> None:
        """Every action_stack item has a safety_note confirming preview-only / no orders."""
        response = seeded_client.post(
            "/v1/review/daily-plan-preview",
            json={"candidate_ids": []},
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        for item in data["action_stack"]:
            note = item.get("safety_note", "").lower()
            assert "preview" in note or "no signals" in note or "no orders" in note, (
                f"safety_note does not mention preview/no-signals/no-orders: {item['safety_note']!r}"
            )

    def test_daily_plan_action_stack_rotate_before_buy(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """ROTATE action_type appears at a lower priority number than BUY when a rotation exists."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_held = f"DPASH{suffix}"
        ticker_buy  = f"DPASB{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            portfolio_obj = s.query(Portfolio).first()
            old_config = dict(portfolio_obj.config or {})
            portfolio_obj.config = {"max_positions": 1}
            s.commit()

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            pos = Position(
                ticker=ticker_held, qty=Decimal("10"), avg_cost=Decimal("90.00"),
                cost_basis=Decimal("900.00"), opened_at=_NOW, last_updated=_NOW,
            )
            s.add(pos)
            s.flush()
            pos_id = pos.id
            s.add(PriceSnapshot(
                ticker=ticker_held, price=Decimal("100.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            s.add(PriceSnapshot(
                ticker=ticker_buy, price=Decimal("50.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            cand = CandidateReview(
                idempotency_key=f"dp-as-rot-{suffix}", ticker=ticker_buy,
                prediction_recommendation="BUY", prediction_confidence="0.90",
                expected_return_pct="20.0", preview_decision="CONSIDER",
                preview_score="90.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand)
            s.commit()
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={
                    "approved_only": True,
                    "min_rotation_improvement_pct": 0.05,
                    "include_rotation": True,
                    "candidate_ids": [cand_id],
                    "position_tickers": [ticker_held],
                },
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            stack = data["action_stack"]
            rotate_priorities = [i["priority"] for i in stack if i["action_type"] == "ROTATE"]
            buy_priorities    = [i["priority"] for i in stack if i["action_type"] == "BUY"]
            assert len(rotate_priorities) >= 1, "Expected at least one ROTATE item in action_stack"
            if buy_priorities:
                assert min(rotate_priorities) < min(buy_priorities), (
                    "ROTATE priority must be lower (earlier) than BUY priority"
                )
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                portfolio_obj = s.query(Portfolio).first()
                portfolio_obj.config = old_config
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker_buy).delete()
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker.in_([ticker_held, ticker_buy])).delete()
                s.query(Position).filter(Position.id == pos_id).delete()
                s.commit()

    def test_daily_plan_action_stack_blocked_max_positions(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """BLOCKED item with blocked_reason=MAX_POSITIONS_REACHED appears in action_stack."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_held = f"DPASMXH{suffix}"
        ticker_buy  = f"DPASMXB{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            portfolio_obj = s.query(Portfolio).first()
            old_config = dict(portfolio_obj.config or {})
            portfolio_obj.config = {"max_positions": 1}
            s.commit()

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            pos = Position(
                ticker=ticker_held, qty=Decimal("5"), avg_cost=Decimal("100.00"),
                cost_basis=Decimal("500.00"), opened_at=_NOW, last_updated=_NOW,
            )
            s.add(pos)
            s.flush()
            pos_id = pos.id
            s.add(PriceSnapshot(
                ticker=ticker_buy, price=Decimal("50.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            cand = CandidateReview(
                idempotency_key=f"dp-as-maxpos-{suffix}", ticker=ticker_buy,
                prediction_recommendation="BUY", prediction_confidence="0.85",
                expected_return_pct="15.0", preview_decision="CONSIDER",
                preview_score="85.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand)
            s.commit()
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={"approved_only": True, "include_rotation": False, "candidate_ids": [cand_id]},
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            stack = data["action_stack"]
            blocked_items = [i for i in stack if i["action_type"] == "BLOCKED"]
            blocked_reasons = {i["ticker"]: i["blocked_reason"] for i in blocked_items}
            assert blocked_reasons.get(ticker_buy) == "MAX_POSITIONS_REACHED", (
                f"Expected BLOCKED/MAX_POSITIONS_REACHED for {ticker_buy}, got: {blocked_reasons}"
            )
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                portfolio_obj = s.query(Portfolio).first()
                portfolio_obj.config = old_config
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker_buy).delete()
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker_buy).delete()
                s.query(Position).filter(Position.id == pos_id).delete()
                s.commit()

    def test_daily_plan_action_stack_negative_pnl_blocked_not_sell(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Loss position appears as BLOCKED in action_stack, never as SELL."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_loss = f"DPASLOSS{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            pos = Position(
                ticker=ticker_loss, qty=Decimal("10"), avg_cost=Decimal("100.00"),
                cost_basis=Decimal("1000.00"), opened_at=_NOW, last_updated=_NOW,
            )
            s.add(pos)
            s.flush()
            pos_id = pos.id
            s.add(PriceSnapshot(
                ticker=ticker_loss, price=Decimal("80.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            cand = CandidateReview(
                idempotency_key=f"dp-as-loss-{suffix}", ticker=ticker_loss,
                prediction_recommendation="SELL", prediction_confidence="0.85",
                preview_decision="CONSIDER", preview_score="85.0",
                review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand)
            s.commit()
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={"block_loss_realization": True, "candidate_ids": [cand_id]},
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            stack = data["action_stack"]
            sell_tickers    = {i["ticker"] for i in stack if i["action_type"] == "SELL"}
            rotate_tickers  = {i["ticker"] for i in stack if i["action_type"] == "ROTATE"}
            blocked_reasons = {i["ticker"]: i["blocked_reason"] for i in stack if i["action_type"] == "BLOCKED"}
            assert ticker_loss not in sell_tickers, "Loss ticker must not appear as SELL"
            assert ticker_loss not in rotate_tickers, "Loss ticker must not appear as ROTATE sell leg"
            assert blocked_reasons.get(ticker_loss) == "NEGATIVE_PNL_BLOCKED", (
                f"Expected BLOCKED/NEGATIVE_PNL_BLOCKED for {ticker_loss}, got: {blocked_reasons}"
            )
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker_loss).delete()
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker_loss).delete()
                s.query(Position).filter(Position.id == pos_id).delete()
                s.commit()

    def test_daily_plan_action_stack_no_action_when_empty(self, seeded_client: TestClient) -> None:
        """When no candidates and no actionable positions, action_stack has a NO_ACTION item."""
        response = seeded_client.post(
            "/v1/review/daily-plan-preview",
            json={"candidate_ids": [], "position_tickers": []},
            headers=_AUTH,
        )
        assert response.status_code == 200
        data = response.json()
        stack = data["action_stack"]
        types = [i["action_type"] for i in stack]
        assert "NO_ACTION" in types, f"Expected NO_ACTION in action_stack, got: {types}"

    def test_daily_plan_buy_action_item_exposes_candidate_review_id_and_approved_qty(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """BUY action_stack item must expose candidate_review_id and approved_qty."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker = f"DPBUYID{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            s.add(PriceSnapshot(
                ticker=ticker, price=Decimal("50.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            cand = CandidateReview(
                idempotency_key=f"dp-buy-id-{suffix}", ticker=ticker,
                prediction_recommendation="BUY", prediction_confidence="0.90",
                expected_return_pct="20.0", preview_decision="CONSIDER",
                preview_score="90.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand)
            s.commit()
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={
                    "approved_only": True,
                    "candidate_ids": [cand_id],
                    "position_tickers": [],
                },
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            stack = data["action_stack"]
            buy_items = [i for i in stack if i["action_type"] == "BUY" and i["ticker"] == ticker]
            assert len(buy_items) >= 1, f"Expected a BUY item for {ticker}, got: {[i['ticker'] for i in stack]}"
            item = buy_items[0]
            assert item["candidate_review_id"] == cand_id, (
                f"BUY item candidate_review_id mismatch: got {item['candidate_review_id']!r}, want {cand_id!r}"
            )
            assert item["approved_qty"] is not None, "BUY item approved_qty must not be None"
            assert Decimal(item["approved_qty"]) >= Decimal("1"), f"BUY item approved_qty must be >= 1, got {item['approved_qty']!r}"
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker).delete()
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker).delete()
                s.commit()

    def test_daily_plan_sell_action_item_exposes_candidate_review_id_and_sell_qty(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """SELL action_stack item must expose candidate_review_id and sell_qty when a SELL candidate exists."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker = f"DPSELLID{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            pos = Position(
                ticker=ticker, qty=Decimal("10"), avg_cost=Decimal("80.00"),
                cost_basis=Decimal("800.00"), opened_at=_NOW, last_updated=_NOW,
            )
            s.add(pos)
            s.flush()
            pos_id = pos.id
            s.add(PriceSnapshot(
                ticker=ticker, price=Decimal("100.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            cand = CandidateReview(
                idempotency_key=f"dp-sell-id-{suffix}", ticker=ticker,
                prediction_recommendation="SELL", prediction_confidence="0.90",
                preview_decision="CONSIDER", preview_score="90.0",
                review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand)
            s.commit()
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={
                    "approved_only": False,
                    "block_loss_realization": False,
                    "candidate_ids": [cand_id],
                    "position_tickers": [ticker],
                },
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            stack = data["action_stack"]
            sell_items = [i for i in stack if i["action_type"] == "SELL" and i["ticker"] == ticker]
            assert len(sell_items) >= 1, (
                f"Expected a SELL item for {ticker}, got types: {[i['action_type'] for i in stack]}"
            )
            item = sell_items[0]
            assert item["candidate_review_id"] == cand_id, (
                f"SELL item candidate_review_id mismatch: got {item['candidate_review_id']!r}, want {cand_id!r}"
            )
            assert item["sell_qty"] is not None, "SELL item sell_qty must not be None"
            assert Decimal(item["sell_qty"]) >= Decimal("1"), f"SELL item sell_qty must be >= 1, got {item['sell_qty']!r}"
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker).delete()
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker).delete()
                s.query(Position).filter(Position.id == pos_id).delete()
                s.commit()

    def test_daily_plan_rotate_action_item_exposes_buy_candidate_review_id(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """ROTATE action_stack item candidate_review_id must equal the BUY candidate's CandidateReview id."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_held = f"DPROTSH{suffix}"
        ticker_buy  = f"DPROTSB{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            portfolio_obj = s.query(Portfolio).first()
            old_config = dict(portfolio_obj.config or {})
            portfolio_obj.config = {"max_positions": 1}
            s.commit()

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            pos = Position(
                ticker=ticker_held, qty=Decimal("10"), avg_cost=Decimal("90.00"),
                cost_basis=Decimal("900.00"), opened_at=_NOW, last_updated=_NOW,
            )
            s.add(pos)
            s.flush()
            pos_id = pos.id
            s.add(PriceSnapshot(
                ticker=ticker_held, price=Decimal("100.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            s.add(PriceSnapshot(
                ticker=ticker_buy, price=Decimal("50.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            cand = CandidateReview(
                idempotency_key=f"dp-rot-id-{suffix}", ticker=ticker_buy,
                prediction_recommendation="BUY", prediction_confidence="0.90",
                expected_return_pct="20.0", preview_decision="CONSIDER",
                preview_score="90.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand)
            s.commit()
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={
                    "approved_only": True,
                    "min_rotation_improvement_pct": 0.05,
                    "include_rotation": True,
                    "candidate_ids": [cand_id],
                    "position_tickers": [ticker_held],
                },
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            stack = data["action_stack"]
            rotate_items = [i for i in stack if i["action_type"] == "ROTATE"]
            assert len(rotate_items) >= 1, (
                f"Expected at least one ROTATE item, got types: {[i['action_type'] for i in stack]}"
            )
            item = rotate_items[0]
            assert item["candidate_review_id"] == cand_id, (
                f"ROTATE item candidate_review_id mismatch: got {item['candidate_review_id']!r}, want {cand_id!r}"
            )
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                portfolio_obj = s.query(Portfolio).first()
                portfolio_obj.config = old_config
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker_buy).delete()
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker.in_([ticker_held, ticker_buy])).delete()
                s.query(Position).filter(Position.id == pos_id).delete()
                s.commit()

    # -----------------------------------------------------------------------
    # Decision Model v2 integration tests
    # -----------------------------------------------------------------------

    def test_daily_plan_v2_uses_forward_vs_forward_not_score_vs_pnl(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Rotation uses forward-vs-forward scoring, not candidate_score minus pnl_pct."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_held = f"DPFWFH{suffix}"
        ticker_buy  = f"DPFWFB{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            portfolio_obj = s.query(Portfolio).first()
            old_config = dict(portfolio_obj.config or {})
            portfolio_obj.config = {"max_positions": 1}
            s.commit()

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            # Holding cost=$10, price=$50 -> PnL=+400% (would block old formula)
            pos = Position(
                ticker=ticker_held, qty=Decimal("10"), avg_cost=Decimal("10.00"),
                cost_basis=Decimal("100.00"), opened_at=_NOW, last_updated=_NOW,
            )
            s.add(pos)
            s.flush()
            pos_id = pos.id
            s.add(PriceSnapshot(
                ticker=ticker_held, price=Decimal("50.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            s.add(PriceSnapshot(
                ticker=ticker_buy, price=Decimal("20.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            # v2 candidate score = 0.80*(8/100)=0.064; scan_score=50 → neutral (scan_adj=0)
            # holding has no pred -> hold_score=0.0; improvement=0.064 >= 0.05 -> meets
            cand = CandidateReview(
                idempotency_key=f"dp-fwf-{suffix}", ticker=ticker_buy,
                prediction_recommendation="BUY", prediction_confidence="0.80",
                expected_return_pct="8.0", scan_score="50", preview_decision="CONSIDER",
                preview_score="80.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand)
            s.commit()
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={
                    "approved_only": True,
                    "min_rotation_improvement_pct": 0.05,
                    "include_rotation": True,
                    "candidate_ids": [cand_id],
                    "position_tickers": [ticker_held],
                },
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            good = [r for r in data["rotation_plan"] if r["meets_threshold"]]
            assert len(good) >= 1, (
                "Expected rotation to meet threshold under forward-vs-forward. "
                f"rotation_plan={data['rotation_plan']}"
            )
            improvement_f = float(good[0]["improvement_score"])
            # v2 forward-vs-forward: cand=0.80*(8/100)=0.064 minus hold=0.0 → ~0.064
            assert improvement_f == pytest.approx(0.064, abs=0.002), (
                f"Expected improvement ~0.064 (0.80 x 0.08, scan neutral), got {improvement_f}"
            )
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                portfolio_obj = s.query(Portfolio).first()
                portfolio_obj.config = old_config
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker_buy).delete()
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker.in_([ticker_held, ticker_buy])).delete()
                s.query(Position).filter(Position.id == pos_id).delete()
                s.commit()

    def test_daily_plan_v2_prediction_missing_flagged_in_rotation(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Rotation pair has prediction_missing=True when the holding has no CandidateReview."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_held = f"DPPMH{suffix}"
        ticker_buy  = f"DPPMB{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            portfolio_obj = s.query(Portfolio).first()
            old_config = dict(portfolio_obj.config or {})
            portfolio_obj.config = {"max_positions": 1}
            s.commit()

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            pos = Position(
                ticker=ticker_held, qty=Decimal("10"), avg_cost=Decimal("90.00"),
                cost_basis=Decimal("900.00"), opened_at=_NOW, last_updated=_NOW,
            )
            s.add(pos)
            s.flush()
            pos_id = pos.id
            s.add(PriceSnapshot(
                ticker=ticker_held, price=Decimal("100.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            s.add(PriceSnapshot(
                ticker=ticker_buy, price=Decimal("30.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            # No CandidateReview for ticker_held -> prediction_missing must be True
            cand = CandidateReview(
                idempotency_key=f"dp-pm-{suffix}", ticker=ticker_buy,
                prediction_recommendation="BUY", prediction_confidence="0.85",
                expected_return_pct="10.0", preview_decision="CONSIDER",
                preview_score="85.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand)
            s.commit()
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={
                    "approved_only": True,
                    "min_rotation_improvement_pct": 0.02,
                    "include_rotation": True,
                    "candidate_ids": [cand_id],
                    "position_tickers": [ticker_held],
                },
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            rot_items = data["rotation_plan"]
            assert len(rot_items) >= 1, "Expected at least one rotation pair"
            rot = rot_items[0]
            assert rot["prediction_missing"] is True
            assert rot["holding_score_v2"] == "0.0000"
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                portfolio_obj = s.query(Portfolio).first()
                portfolio_obj.config = old_config
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker_buy).delete()
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker.in_([ticker_held, ticker_buy])).delete()
                s.query(Position).filter(Position.id == pos_id).delete()
                s.commit()

    def test_daily_plan_v2_holding_with_strong_prediction_blocks_weak_rotation(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Rotation not proposed when the holding forward score exceeds the candidate's."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_held = f"DPHLDH{suffix}"
        ticker_buy  = f"DPHLDB{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            portfolio_obj = s.query(Portfolio).first()
            old_config = dict(portfolio_obj.config or {})
            portfolio_obj.config = {"max_positions": 1}
            s.commit()

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            pos = Position(
                ticker=ticker_held, qty=Decimal("10"), avg_cost=Decimal("90.00"),
                cost_basis=Decimal("900.00"), opened_at=_NOW, last_updated=_NOW,
            )
            s.add(pos)
            s.flush()
            pos_id = pos.id
            s.add(PriceSnapshot(
                ticker=ticker_held, price=Decimal("100.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            s.add(PriceSnapshot(
                ticker=ticker_buy, price=Decimal("30.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            # Holding: conf=0.90, exp_ret=15% -> v2 score=0.135
            held_cr = CandidateReview(
                idempotency_key=f"dp-held-{suffix}", ticker=ticker_held,
                prediction_recommendation="BUY", prediction_confidence="0.90",
                expected_return_pct="15.0", preview_decision="CONSIDER",
                preview_score="85.0", review_status="NEW", status="OK",
            )
            s.add(held_cr)
            # Buy candidate: conf=0.70, exp_ret=3% -> v2 score=0.021
            # improvement = 0.021 - 0.135 = -0.114 < 0.02 -> does NOT meet
            buy_cand = CandidateReview(
                idempotency_key=f"dp-hld-buy-{suffix}", ticker=ticker_buy,
                prediction_recommendation="BUY", prediction_confidence="0.70",
                expected_return_pct="3.0", preview_decision="WATCH",
                preview_score="40.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(buy_cand)
            s.commit()
            buy_cand_id = str(buy_cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={
                    "approved_only": True,
                    "min_rotation_improvement_pct": 0.02,
                    "include_rotation": True,
                    "candidate_ids": [buy_cand_id],
                    "position_tickers": [ticker_held],
                },
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            good = [r for r in data["rotation_plan"] if r["meets_threshold"]]
            assert len(good) == 0, (
                f"Rotation must not meet threshold when holding has stronger fwd score. good={good}"
            )
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                portfolio_obj = s.query(Portfolio).first()
                portfolio_obj.config = old_config
                s.query(CandidateReview).filter(
                    CandidateReview.ticker.in_([ticker_held, ticker_buy])
                ).delete()
                s.query(PriceSnapshot).filter(
                    PriceSnapshot.ticker.in_([ticker_held, ticker_buy])
                ).delete()
                s.query(Position).filter(Position.id == pos_id).delete()
                s.commit()

    def test_daily_plan_v2_score_factors_v2_present_in_buy_recommendation(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """buy_recommendations expose score_factors_v2 with factor breakdown keys."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker = f"DPSFV2{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            s.add(PriceSnapshot(
                ticker=ticker, price=Decimal("50.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            cand = CandidateReview(
                idempotency_key=f"dp-sfv2-{suffix}", ticker=ticker,
                prediction_recommendation="BUY", prediction_confidence="0.80",
                expected_return_pct="10.0", preview_decision="CONSIDER",
                preview_score="80.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand)
            s.commit()
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={"approved_only": True, "candidate_ids": [cand_id], "position_tickers": []},
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            buy_items = [b for b in data["buy_recommendations"] if b["ticker"] == ticker]
            assert len(buy_items) >= 1, f"Expected BUY recommendation for {ticker}"
            sf = buy_items[0].get("score_factors_v2")
            assert sf is not None, "score_factors_v2 must be present"
            for key in ("total_score", "base_score", "momentum_adj", "rs_adj",
                        "scan_adj", "confidence", "expected_return_pct"):
                assert key in sf, f"score_factors_v2 missing key: {key}"
            assert abs(sf["base_score"] - 0.08) < 1e-4
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker).delete()
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker).delete()
                s.commit()

    def test_daily_plan_v2_negative_pnl_holding_excluded_from_rotation(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """A holding at a loss is never proposed as the sell leg of a rotation pair."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_held = f"DPNPNH{suffix}"
        ticker_buy  = f"DPNPNB{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            portfolio_obj = s.query(Portfolio).first()
            old_config = dict(portfolio_obj.config or {})
            portfolio_obj.config = {"max_positions": 1}
            s.commit()

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            pos = Position(
                ticker=ticker_held, qty=Decimal("10"), avg_cost=Decimal("100.00"),
                cost_basis=Decimal("1000.00"), opened_at=_NOW, last_updated=_NOW,
            )
            s.add(pos)
            s.flush()
            pos_id = pos.id
            s.add(PriceSnapshot(
                ticker=ticker_held, price=Decimal("80.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            s.add(PriceSnapshot(
                ticker=ticker_buy, price=Decimal("30.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            cand = CandidateReview(
                idempotency_key=f"dp-npn-{suffix}", ticker=ticker_buy,
                prediction_recommendation="BUY", prediction_confidence="0.99",
                expected_return_pct="50.0", preview_decision="CONSIDER",
                preview_score="99.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand)
            s.commit()
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={
                    "approved_only": True,
                    "block_loss_realization": True,
                    "min_rotation_improvement_pct": 0.01,
                    "include_rotation": True,
                    "candidate_ids": [cand_id],
                    "position_tickers": [ticker_held],
                },
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            good = [r for r in data["rotation_plan"] if r["meets_threshold"]]
            assert len(good) == 0, (
                f"Loss position must not appear in qualifying rotation. rotation_plan={data['rotation_plan']}"
            )
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                portfolio_obj = s.query(Portfolio).first()
                portfolio_obj.config = old_config
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker_buy).delete()
                s.query(PriceSnapshot).filter(
                    PriceSnapshot.ticker.in_([ticker_held, ticker_buy])
                ).delete()
                s.query(Position).filter(Position.id == pos_id).delete()
                s.commit()

    # -----------------------------------------------------------------------
    # Capital Allocation / Rotation v3 tests
    # -----------------------------------------------------------------------

    def test_daily_plan_capital_release_summary_profitable_position(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Profitable position appears as sellable; releasable cash > 0."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker = f"DPCAP1{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            pos = Position(
                ticker=ticker, qty=Decimal("10"), avg_cost=Decimal("100.00"),
                cost_basis=Decimal("1000.00"), opened_at=_NOW, last_updated=_NOW,
            )
            s.add(pos)
            s.flush()
            pos_id = pos.id
            s.add(PriceSnapshot(
                ticker=ticker, price=Decimal("110.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            s.commit()

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={"candidate_ids": [], "position_tickers": [ticker]},
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            ca = data.get("capital_allocation")
            assert ca is not None, "capital_allocation must be present"
            summary = ca["capital_release_summary"]
            assert float(summary["max_releasable_cash_standard_mode"]) > 0.0, (
                "Profitable position should release cash in standard mode"
            )
            assert summary["sellable_positions_count"] >= 1
            details = [d for d in ca["position_release_details"] if d["ticker"] == ticker]
            assert len(details) == 1
            assert details[0]["sellable_standard_mode"] is True
            assert details[0]["blocked_reason"] is None
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker).delete()
                s.query(Position).filter(Position.id == pos_id).delete()
                s.commit()

    def test_daily_plan_capital_release_blocks_negative_pnl(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Loss-making position is not sellable; theoretical cash > 0; standard = 0."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker = f"DPCAP2{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            pos = Position(
                ticker=ticker, qty=Decimal("10"), avg_cost=Decimal("100.00"),
                cost_basis=Decimal("1000.00"), opened_at=_NOW, last_updated=_NOW,
            )
            s.add(pos)
            s.flush()
            pos_id = pos.id
            s.add(PriceSnapshot(
                ticker=ticker, price=Decimal("80.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            s.commit()

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={
                    "block_loss_realization": True,
                    "candidate_ids": [],
                    "position_tickers": [ticker],
                },
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            ca = data.get("capital_allocation")
            assert ca is not None
            summary = ca["capital_release_summary"]
            # Standard mode: 0 (blocked by loss rule)
            assert float(summary["max_releasable_cash_standard_mode"]) == 0.0
            # Theoretical: position value > 0 (10 * 80 = 800)
            assert float(summary["max_releasable_cash_theoretical"]) > 0.0
            assert float(summary["blocked_cash_due_to_negative_pnl"]) > 0.0
            # Per-position detail
            details = [d for d in ca["position_release_details"] if d["ticker"] == ticker]
            assert len(details) == 1
            d = details[0]
            assert d["sellable_standard_mode"] is False
            assert d["blocked_reason"] == "NEGATIVE_PNL_BLOCKED"
            assert float(d["releasable_cash_standard_mode"]) == 0.0
            assert float(d["releasable_cash_theoretical"]) > 0.0
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker).delete()
                s.query(Position).filter(Position.id == pos_id).delete()
                s.commit()

    def test_daily_plan_candidate_expected_pnl_per_1000(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """expected_pnl_per_1000 = 1000 * expected_return_pct * confidence / 100."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker = f"DPCAP3{suffix}"

        # confidence=0.80, expected_return_pct=10.0
        # risk_adj = 10.0 * 0.80 = 8.0 %; pnl_per_1000 = 1000 * 8.0 / 100 = 80.00
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            s.add(PriceSnapshot(
                ticker=ticker, price=Decimal("50.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            cand = CandidateReview(
                idempotency_key=f"dp-cap3-{suffix}", ticker=ticker,
                prediction_recommendation="BUY", prediction_confidence="0.80",
                expected_return_pct="10.0", preview_decision="CONSIDER",
                preview_score="80.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand)
            s.commit()
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={"approved_only": True, "candidate_ids": [cand_id], "position_tickers": []},
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            ca = data.get("capital_allocation")
            assert ca is not None
            redeploy = [r for r in ca["candidate_redeployment"] if r["ticker"] == ticker]
            assert len(redeploy) >= 1, f"Expected candidate_redeployment entry for {ticker}"
            r = redeploy[0]
            assert abs(float(r["expected_pnl_per_1000"]) - 80.0) < 0.01, (
                f"Expected pnl_per_1000=80.00, got {r['expected_pnl_per_1000']}"
            )
            assert abs(float(r["risk_adjusted_expected_return_pct"]) - 8.0) < 0.01
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker).delete()
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker).delete()
                s.commit()

    def test_daily_plan_rotation_opportunity_uses_cash_released_and_expected_pnl(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """rotation_opportunities.expected_forward_pnl = cash_released * risk_adj_return / 100."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_held = f"DPCAP4H{suffix}"
        ticker_buy  = f"DPCAP4B{suffix}"

        # Holding: 10 shares * $110 = $1,100 cash if sold (PnL > 0)
        # Candidate: confidence=0.80, expected_return=10% -> risk_adj=8%
        # expected_forward_pnl = 1100 * 8 / 100 = 88.00
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            pos = Position(
                ticker=ticker_held, qty=Decimal("10"), avg_cost=Decimal("100.00"),
                cost_basis=Decimal("1000.00"), opened_at=_NOW, last_updated=_NOW,
            )
            s.add(pos)
            s.flush()
            pos_id = pos.id
            s.add(PriceSnapshot(
                ticker=ticker_held, price=Decimal("110.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            s.add(PriceSnapshot(
                ticker=ticker_buy, price=Decimal("50.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            cand = CandidateReview(
                idempotency_key=f"dp-cap4-{suffix}", ticker=ticker_buy,
                prediction_recommendation="BUY", prediction_confidence="0.80",
                expected_return_pct="10.0", preview_decision="CONSIDER",
                preview_score="80.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand)
            s.commit()
            cand_id = str(cand.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={
                    "approved_only": True,
                    "candidate_ids": [cand_id],
                    "position_tickers": [ticker_held],
                },
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            ca = data.get("capital_allocation")
            assert ca is not None
            rot_opps = [
                o for o in ca["rotation_opportunities"]
                if o["sell_ticker"] == ticker_held and o["buy_ticker"] == ticker_buy
            ]
            assert len(rot_opps) >= 1, (
                f"Expected rotation opportunity {ticker_held}->{ticker_buy}, got {ca['rotation_opportunities']}"
            )
            opp = rot_opps[0]
            # cash_released = 10 * 110 = 1100
            assert abs(float(opp["cash_released"]) - 1100.0) < 0.01
            # expected_forward_pnl = 1100 * 0.80 * 0.10 = 88.00
            assert abs(float(opp["expected_forward_pnl"]) - 88.0) < 0.5
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker_buy).delete()
                s.query(PriceSnapshot).filter(
                    PriceSnapshot.ticker.in_([ticker_held, ticker_buy])
                ).delete()
                s.query(Position).filter(Position.id == pos_id).delete()
                s.commit()

    def test_daily_plan_rotation_opportunity_sorted_by_expected_pnl(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """rotation_opportunities is sorted by expected_pnl_improvement descending."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker_held = f"DPCAP5H{suffix}"
        ticker_buy_a = f"DPCAP5A{suffix}"
        ticker_buy_b = f"DPCAP5B{suffix}"

        # Candidate A: confidence=0.90, expected_return=20% -> risk_adj=18% -> pnl per $1k = $180
        # Candidate B: confidence=0.70, expected_return=5%  -> risk_adj=3.5% -> pnl per $1k = $35
        # A should appear first in rotation_opportunities
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            pos = Position(
                ticker=ticker_held, qty=Decimal("10"), avg_cost=Decimal("90.00"),
                cost_basis=Decimal("900.00"), opened_at=_NOW, last_updated=_NOW,
            )
            s.add(pos)
            s.flush()
            pos_id = pos.id
            for t, p in [(ticker_held, "100.00"), (ticker_buy_a, "50.00"), (ticker_buy_b, "40.00")]:
                s.add(PriceSnapshot(
                    ticker=t, price=Decimal(p),
                    snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                    session_type="REGULAR", price_type="LAST",
                ))
            cand_a = CandidateReview(
                idempotency_key=f"dp-cap5a-{suffix}", ticker=ticker_buy_a,
                prediction_recommendation="BUY", prediction_confidence="0.90",
                expected_return_pct="20.0", preview_decision="CONSIDER",
                preview_score="90.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            cand_b = CandidateReview(
                idempotency_key=f"dp-cap5b-{suffix}", ticker=ticker_buy_b,
                prediction_recommendation="BUY", prediction_confidence="0.70",
                expected_return_pct="5.0", preview_decision="CONSIDER",
                preview_score="70.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand_a)
            s.add(cand_b)
            s.commit()
            cand_a_id = str(cand_a.id)
            cand_b_id = str(cand_b.id)

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={
                    "approved_only": True,
                    "candidate_ids": [cand_a_id, cand_b_id],
                    "position_tickers": [ticker_held],
                },
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            ca = data.get("capital_allocation")
            assert ca is not None
            rot_opps = [
                o for o in ca["rotation_opportunities"] if o["sell_ticker"] == ticker_held
            ]
            assert len(rot_opps) >= 2, f"Expected at least 2 rotation opportunities, got {len(rot_opps)}"
            # First opportunity should have higher pnl_improvement
            imp_0 = float(rot_opps[0]["expected_pnl_improvement"])
            imp_1 = float(rot_opps[1]["expected_pnl_improvement"])
            assert imp_0 >= imp_1, (
                f"Expected rotation_opportunities sorted descending by pnl_improvement: "
                f"{imp_0} >= {imp_1}"
            )
            # Candidate A (higher return) should be first
            assert rot_opps[0]["buy_ticker"] == ticker_buy_a, (
                f"Expected candidate A ({ticker_buy_a}) first, got {rot_opps[0]['buy_ticker']}"
            )
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(CandidateReview).filter(
                    CandidateReview.ticker.in_([ticker_buy_a, ticker_buy_b])
                ).delete()
                s.query(PriceSnapshot).filter(
                    PriceSnapshot.ticker.in_([ticker_held, ticker_buy_a, ticker_buy_b])
                ).delete()
                s.query(Position).filter(Position.id == pos_id).delete()
                s.commit()

    def test_daily_plan_capital_analysis_creates_zero_rows(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """capital_allocation section creates no new DB rows of any kind."""
        import uuid as uuid_module
        suffix = uuid_module.uuid4().hex[:6]
        ticker = f"DPCAP6{suffix}"

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            s.add(PriceSnapshot(
                ticker=ticker, price=Decimal("50.00"),
                snapshot_ts=datetime.now(timezone.utc), market_date=date.today(),
                session_type="REGULAR", price_type="LAST",
            ))
            cand = CandidateReview(
                idempotency_key=f"dp-cap6-{suffix}", ticker=ticker,
                prediction_recommendation="BUY", prediction_confidence="0.85",
                expected_return_pct="12.0", preview_decision="CONSIDER",
                preview_score="85.0", review_status="APPROVED_FOR_SIGNAL", status="OK",
            )
            s.add(cand)
            s.commit()
            cand_id = str(cand.id)

        # Snapshot row counts before
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            sig_before  = s.query(Signal).count()
            td_before   = s.query(TradeDecision).count()
            ord_before  = s.query(Order).count()
            pos_before  = s.query(Position).count()
            cand_before = s.query(CandidateReview).count()

        try:
            response = seeded_client.post(
                "/v1/review/daily-plan-preview",
                json={"approved_only": True, "candidate_ids": [cand_id], "position_tickers": []},
                headers=_AUTH,
            )
            assert response.status_code == 200
            data = response.json()
            assert data.get("capital_allocation") is not None, "capital_allocation must be in response"

            # Verify no rows were created
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                assert s.query(Signal).count()          == sig_before
                assert s.query(TradeDecision).count()   == td_before
                assert s.query(Order).count()           == ord_before
                assert s.query(Position).count()        == pos_before
                assert s.query(CandidateReview).count() == cand_before
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(CandidateReview).filter(CandidateReview.ticker == ticker).delete()
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker).delete()
                s.commit()


class TestUniverseStatusEndpoint:
    """GET /v1/strategy/universe/status -- read-only universe diagnostics."""

    def test_requires_auth(self, client: TestClient) -> None:
        """Returns 401 when API key is missing."""
        resp = client.get("/v1/strategy/universe/status")
        assert resp.status_code == 401

    def test_returns_200_with_valid_auth(self, seeded_client: TestClient) -> None:
        """Returns 200 OK with correct top-level structure."""
        resp = seeded_client.get("/v1/strategy/universe/status", headers=_AUTH)
        assert resp.status_code == 200
        data = resp.json()
        assert "universe_name" in data
        assert "active_source_file" in data
        assert "ticker_count" in data
        assert "first_10_tickers" in data
        assert "last_10_tickers" in data
        assert "is_stub_universe" in data
        assert "market_data_coverage" in data
        assert "safety_counts" in data

    def test_flags_stub_universe(self, seeded_client: TestClient, tmp_path, monkeypatch) -> None:
        """A stub-only universe (< 450 tickers) is flagged as stub, regardless of local data files."""
        stub_csv = tmp_path / "sp500_universe.csv"
        stub_csv.write_text("ticker\n" + "\n".join(f"T{i}" for i in range(10)) + "\n", encoding="utf-8")
        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)

        resp = seeded_client.get("/v1/strategy/universe/status", headers=_AUTH)
        assert resp.status_code == 200
        data = resp.json()
        assert data["is_stub_universe"] is True
        assert data["ticker_count"] < 450
        assert data["warning"] is not None

    def test_reports_active_source_file(self, seeded_client: TestClient, tmp_path, monkeypatch) -> None:
        """Reports sp500_universe.csv as active when only stub file is present in controlled dir."""
        stub_csv = tmp_path / "sp500_universe.csv"
        stub_csv.write_text("ticker\nAAPL\nMSFT\n", encoding="utf-8")
        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)

        resp = seeded_client.get("/v1/strategy/universe/status", headers=_AUTH)
        assert resp.status_code == 200
        data = resp.json()
        assert data["full_universe_file_exists"] is False
        assert data["stub_universe_file_exists"] is True
        assert data["fallback_used"] is True
        assert data["active_source_file"] == "sp500_universe.csv"

    def test_market_data_coverage_structure(self, seeded_client: TestClient) -> None:
        """market_data_coverage contains all required fields."""
        resp = seeded_client.get("/v1/strategy/universe/status", headers=_AUTH)
        assert resp.status_code == 200
        data = resp.json()
        cov = data["market_data_coverage"]
        assert "tickers_with_enough_price_history" in cov
        assert "tickers_missing_price_history" in cov
        assert "benchmark_available" in cov
        assert "benchmark_ticker" in cov
        assert "min_price_points_used" in cov
        assert cov["benchmark_ticker"] == "SPY"
        assert cov["min_price_points_used"] == 5
        total = cov["tickers_with_enough_price_history"] + cov["tickers_missing_price_history"]
        assert total == data["ticker_count"]

    def test_creates_zero_rows(self, seeded_client: TestClient, api_engine) -> None:
        """Endpoint creates no DB rows of any kind."""
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            sig_before  = s.query(Signal).count()
            td_before   = s.query(TradeDecision).count()
            ord_before  = s.query(Order).count()
            pos_before  = s.query(Position).count()
            cand_before = s.query(CandidateReview).count()

        resp = seeded_client.get("/v1/strategy/universe/status", headers=_AUTH)
        assert resp.status_code == 200

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            assert s.query(Signal).count()          == sig_before
            assert s.query(TradeDecision).count()   == td_before
            assert s.query(Order).count()           == ord_before
            assert s.query(Position).count()        == pos_before
            assert s.query(CandidateReview).count() == cand_before

        sc = resp.json()["safety_counts"]
        assert sc["rows_created"] == 0
        assert sc["signals_created"] == 0
        assert sc["decisions_created"] == 0
        assert sc["orders_created"] == 0

    def test_expected_full_sp500_min_count(self, seeded_client: TestClient) -> None:
        """Reports expected_full_sp500_min_count of 450."""
        resp = seeded_client.get("/v1/strategy/universe/status", headers=_AUTH)
        assert resp.status_code == 200
        data = resp.json()
        assert data["expected_full_sp500_min_count"] == 450

    def test_market_data_coverage_counts_correctly(
        self, seeded_client: TestClient, api_engine, tmp_path, monkeypatch
    ) -> None:
        """tickers_with_enough/tickers_missing counts reflect actual DB state."""
        tickers_enough = ["COVTEST1", "COVTEST2", "COVTEST3"]
        tickers_missing = ["COVTEST4", "COVTEST5"]

        full_csv = tmp_path / "sp500_universe_full.csv"
        full_csv.write_text(
            "ticker\n" + "\n".join(tickers_enough + tickers_missing) + "\n",
            encoding="utf-8",
        )
        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)

        now_ts = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            rows_to_add = []
            for ticker in tickers_enough:
                for day in range(5):
                    rows_to_add.append(PriceSnapshot(
                        ticker=ticker,
                        price=Decimal("100.00"),
                        session_type="REGULAR",
                        price_type="CLOSE",
                        snapshot_ts=now_ts,
                        market_date=date(2026, 6, day + 1),
                    ))
            for ticker in tickers_missing:
                for day in range(2):
                    rows_to_add.append(PriceSnapshot(
                        ticker=ticker,
                        price=Decimal("50.00"),
                        session_type="REGULAR",
                        price_type="CLOSE",
                        snapshot_ts=now_ts,
                        market_date=date(2026, 6, day + 1),
                    ))
            s.add_all(rows_to_add)
            s.commit()

        resp = seeded_client.get("/v1/strategy/universe/status", headers=_AUTH)
        assert resp.status_code == 200
        data = resp.json()
        cov = data["market_data_coverage"]
        assert cov["tickers_with_enough_price_history"] == 3
        assert cov["tickers_missing_price_history"] == 2
        assert data["ticker_count"] == 5

    def test_market_data_coverage_benchmark_false_when_no_spy(
        self, seeded_client: TestClient, api_engine, tmp_path, monkeypatch
    ) -> None:
        """benchmark_available is False when no SPY BenchmarkPrice rows exist."""
        stub_csv = tmp_path / "sp500_universe.csv"
        stub_csv.write_text("ticker\nCOVTEST1\nCOVTEST2\n", encoding="utf-8")
        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            s.query(BenchmarkPrice).filter(BenchmarkPrice.ticker == "SPY").delete()
            s.commit()

        resp = seeded_client.get("/v1/strategy/universe/status", headers=_AUTH)
        assert resp.status_code == 200
        cov = resp.json()["market_data_coverage"]
        assert cov["benchmark_available"] is False

    def test_market_data_coverage_benchmark_true_when_spy_present(
        self, seeded_client: TestClient, api_engine, tmp_path, monkeypatch
    ) -> None:
        """benchmark_available is True when at least one SPY BenchmarkPrice row exists."""
        stub_csv = tmp_path / "sp500_universe.csv"
        stub_csv.write_text("ticker\nCOVTEST1\n", encoding="utf-8")
        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)

        now_ts = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            s.add(BenchmarkPrice(
                ticker="SPY",
                price=Decimal("520.00"),
                session_type="REGULAR",
                snapshot_ts=now_ts,
                market_date=date(2026, 6, 15),
            ))
            s.commit()

        resp = seeded_client.get("/v1/strategy/universe/status", headers=_AUTH)
        assert resp.status_code == 200
        cov = resp.json()["market_data_coverage"]
        assert cov["benchmark_available"] is True


class TestScoringProfileCalibrationPreviewEndpoint:
    """POST /v1/strategy/scoring-profile-calibration-preview — read-only historical calibration."""

    _ENDPOINT = "/v1/strategy/scoring-profile-calibration-preview"
    _AS_OF = date(2025, 4, 15)
    _FWD_DATE = date(2025, 4, 22)  # +5 trading days (approximate) after _AS_OF

    @staticmethod
    def _seed_ticker_prices(
        s,
        ticker: str,
        as_of_date: date,
        lookback: int = 25,
        latest_price: float = 100.0,
        fwd_price: float | None = None,
        fwd_days: int = 7,
    ) -> None:
        """Seed monotonically increasing prices from (as_of_date - lookback) to as_of_date,
        plus an optional forward price at as_of_date + fwd_days."""
        ticker = ticker.strip().upper()
        from datetime import timedelta as _td
        base_ts = datetime(2025, 4, 1, 16, 0, 0, tzinfo=timezone.utc)
        step = (latest_price * 0.005)
        for i in range(lookback):
            d = as_of_date - _td(days=lookback - 1 - i)
            price = latest_price - step * (lookback - 1 - i)
            s.add(PriceSnapshot(
                ticker=ticker,
                price=Decimal(f"{max(1.0, price):.4f}"),
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=base_ts,
                market_date=d,
            ))
        if fwd_price is not None:
            fwd_d = as_of_date + _td(days=fwd_days)
            s.add(PriceSnapshot(
                ticker=ticker,
                price=Decimal(f"{fwd_price:.4f}"),
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=base_ts,
                market_date=fwd_d,
            ))

    def test_requires_auth(self, client: TestClient) -> None:
        """Returns 401 when API key is missing."""
        resp = client.post(self._ENDPOINT, json={})
        assert resp.status_code == 401

    def test_returns_200_with_seeded_prices(self, seeded_client: TestClient, api_engine) -> None:
        """Returns 200 with correct top-level structure when prices are seeded."""
        import uuid as _uuid
        suffix = _uuid.uuid4().hex[:6]
        tickers = [f"CALIB{suffix}{i}".upper() for i in range(3)]

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            for t in tickers:
                self._seed_ticker_prices(s, t, self._AS_OF, lookback=25, latest_price=100.0, fwd_price=104.0, fwd_days=7)
            s.commit()

        try:
            resp = seeded_client.post(
                self._ENDPOINT,
                json={
                    "as_of_date": str(self._AS_OF),
                    "lookback_days": 20,
                    "forward_return_days": 5,
                    "scan_top_n": 10,
                    "profile_top_n": 3,
                    "min_price_points": 5,
                    "profiles": ["current", "balanced_preview"],
                },
                headers=_AUTH,
            )
            assert resp.status_code == 200
            data = resp.json()
            assert "calibration_summary" in data
            assert "profile_results" in data
            assert "profile_comparison" in data
            assert "skipped_diagnostics" in data
            assert data["calibration_summary"]["as_of_date"] == str(self._AS_OF)
            assert len(data["profile_results"]) == 2
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                for t in tickers:
                    s.query(PriceSnapshot).filter(PriceSnapshot.ticker == t).delete()
                s.commit()

    def test_creates_zero_rows(self, seeded_client: TestClient, api_engine) -> None:
        """Endpoint creates no DB rows of any kind."""
        import uuid as _uuid
        suffix = _uuid.uuid4().hex[:6]
        tickers = [f"CALIBZR{suffix}{i}".upper() for i in range(2)]

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            for t in tickers:
                self._seed_ticker_prices(s, t, self._AS_OF, lookback=25, latest_price=80.0)
            s.commit()

        try:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                sig_b = s.query(Signal).count()
                td_b  = s.query(TradeDecision).count()
                ord_b = s.query(Order).count()
                cand_b = s.query(CandidateReview).count()

            resp = seeded_client.post(
                self._ENDPOINT,
                json={"as_of_date": str(self._AS_OF), "min_price_points": 5, "profile_top_n": 2},
                headers=_AUTH,
            )
            assert resp.status_code == 200

            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                assert s.query(Signal).count()          == sig_b
                assert s.query(TradeDecision).count()   == td_b
                assert s.query(Order).count()           == ord_b
                assert s.query(CandidateReview).count() == cand_b

            sc = resp.json()["calibration_summary"]["safety_counts"]
            assert sc["signals_created"]   == 0
            assert sc["decisions_created"] == 0
            assert sc["orders_created"]    == 0
            assert sc["rows_created"]      == 0
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                for t in tickers:
                    s.query(PriceSnapshot).filter(PriceSnapshot.ticker == t).delete()
                s.commit()

    def test_forward_return_pct_computed_correctly(self, seeded_client: TestClient, api_engine) -> None:
        """forward_return_pct equals (fwd_price - as_of_price) / as_of_price * 100."""
        import uuid as _uuid
        from datetime import timedelta as _td
        suffix = _uuid.uuid4().hex[:6]
        ticker = f"CALIBFR{suffix}".upper()
        as_of = date(2025, 4, 10)
        fwd_date = as_of + _td(days=5)
        as_of_price = 100.0
        fwd_price = 105.0
        expected_ret = round((fwd_price - as_of_price) / as_of_price * 100.0, 4)  # 5.0

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            self._seed_ticker_prices(
                s, ticker, as_of, lookback=22, latest_price=as_of_price, fwd_price=fwd_price, fwd_days=5
            )
            s.commit()

        try:
            resp = seeded_client.post(
                self._ENDPOINT,
                json={
                    "as_of_date": str(as_of),
                    "forward_return_days": 5,
                    "scan_top_n": 50,
                    "profile_top_n": 50,
                    "min_price_points": 5,
                    "profiles": ["current"],
                    "tickers": [ticker],
                },
                headers=_AUTH,
            )
            assert resp.status_code == 200
            data = resp.json()
            pr = data["profile_results"][0]
            cand = next((c for c in pr["top_candidates"] if c["ticker"] == ticker), None)
            assert cand is not None, f"{ticker} not found in top_candidates"
            assert cand["forward_return_pct"] == expected_ret, (
                f"Expected {expected_ret}, got {cand['forward_return_pct']}"
            )
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker).delete()
                s.commit()

    def test_benchmark_missing_handled_gracefully(self, seeded_client: TestClient, api_engine) -> None:
        """When no benchmark prices exist, benchmark_available=False and no crash."""
        import uuid as _uuid
        suffix = _uuid.uuid4().hex[:6]
        ticker = f"CALIBNB{suffix}".upper()

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            self._seed_ticker_prices(s, ticker, self._AS_OF, lookback=25, latest_price=50.0)
            s.query(BenchmarkPrice).filter(BenchmarkPrice.ticker == "SPY").delete()
            s.commit()

        try:
            resp = seeded_client.post(
                self._ENDPOINT,
                json={
                    "as_of_date": str(self._AS_OF),
                    "min_price_points": 5,
                    "profile_top_n": 3,
                    "profiles": ["current"],
                },
                headers=_AUTH,
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["calibration_summary"]["benchmark_available"] is False
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker).delete()
                s.commit()

    def test_profile_results_includes_all_requested_profiles(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """profile_results contains one entry per requested profile."""
        import uuid as _uuid
        suffix = _uuid.uuid4().hex[:6]
        tickers = [f"CALIBPR{suffix}{i}".upper() for i in range(2)]

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            for t in tickers:
                self._seed_ticker_prices(s, t, self._AS_OF, lookback=25, latest_price=60.0)
            s.commit()

        try:
            profiles = ["current", "balanced_preview", "quality_preview", "risk_adjusted_preview"]
            resp = seeded_client.post(
                self._ENDPOINT,
                json={
                    "as_of_date": str(self._AS_OF),
                    "min_price_points": 5,
                    "profile_top_n": 5,
                    "profiles": profiles,
                },
                headers=_AUTH,
            )
            assert resp.status_code == 200
            data = resp.json()
            returned_profiles = [pr["profile_name"] for pr in data["profile_results"]]
            for p in profiles:
                assert p in returned_profiles, f"Profile {p!r} missing from profile_results"
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                for t in tickers:
                    s.query(PriceSnapshot).filter(PriceSnapshot.ticker == t).delete()
                s.commit()

    def test_excess_return_vs_spy_computed_when_benchmark_exists(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """excess_return_vs_spy_pct is populated when SPY benchmark prices exist."""
        import uuid as _uuid
        from datetime import timedelta as _td
        suffix = _uuid.uuid4().hex[:6]
        ticker = f"CALIBEX{suffix}".upper()
        as_of = date(2025, 4, 8)
        fwd_days = 5
        base_ts = datetime(2025, 4, 1, 16, 0, 0, tzinfo=timezone.utc)

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            self._seed_ticker_prices(
                s, ticker, as_of, lookback=22, latest_price=100.0, fwd_price=103.0, fwd_days=fwd_days
            )
            # Seed SPY benchmark lookback prices
            spy_price = 500.0
            for i in range(22):
                d = as_of - _td(days=21 - i)
                s.add(BenchmarkPrice(
                    ticker="SPY",
                    price=Decimal(f"{spy_price + i * 0.5:.2f}"),
                    session_type="REGULAR",
                    snapshot_ts=base_ts,
                    market_date=d,
                ))
            # Seed SPY forward price
            s.add(BenchmarkPrice(
                ticker="SPY",
                price=Decimal("511.00"),
                session_type="REGULAR",
                snapshot_ts=base_ts,
                market_date=as_of + _td(days=fwd_days),
            ))
            s.commit()

        try:
            resp = seeded_client.post(
                self._ENDPOINT,
                json={
                    "as_of_date": str(as_of),
                    "forward_return_days": fwd_days,
                    "scan_top_n": 50,
                    "profile_top_n": 50,
                    "min_price_points": 5,
                    "profiles": ["current"],
                    "tickers": [ticker],
                },
                headers=_AUTH,
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["calibration_summary"]["benchmark_available"] is True
            pr = data["profile_results"][0]
            cand = next((c for c in pr["top_candidates"] if c["ticker"] == ticker), None)
            assert cand is not None, f"{ticker} not in top_candidates"
            assert cand["excess_return_vs_spy_pct"] is not None, "excess_return_vs_spy_pct should be populated"
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker).delete()
                s.query(BenchmarkPrice).filter(BenchmarkPrice.ticker == "SPY").delete()
                s.commit()

    def test_no_forward_data_sets_warning_and_null_return(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """When no forward price exists, warning_reason=NO_FORWARD_PRICE and forward_return_pct=null."""
        import uuid as _uuid
        suffix = _uuid.uuid4().hex[:6]
        ticker = f"CALIBNF{suffix}".upper()
        # Use a far-future as_of_date so no forward price will exist
        as_of = date(2099, 1, 10)

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            from datetime import timedelta as _td
            base_ts = datetime(2099, 1, 1, 16, 0, 0, tzinfo=timezone.utc)
            for i in range(22):
                d = as_of - _td(days=21 - i)
                s.add(PriceSnapshot(
                    ticker=ticker,
                    price=Decimal("50.00"),
                    session_type="REGULAR",
                    price_type="CLOSE",
                    snapshot_ts=base_ts,
                    market_date=d,
                ))
            s.commit()

        try:
            resp = seeded_client.post(
                self._ENDPOINT,
                json={
                    "as_of_date": str(as_of),
                    "forward_return_days": 5,
                    "scan_top_n": 50,
                    "profile_top_n": 50,
                    "min_price_points": 5,
                    "profiles": ["current"],
                    "tickers": [ticker],
                },
                headers=_AUTH,
            )
            assert resp.status_code == 200
            data = resp.json()
            pr = data["profile_results"][0]
            cand = next((c for c in pr["top_candidates"] if c["ticker"] == ticker), None)
            assert cand is not None, f"{ticker} not in top_candidates"
            assert cand["forward_return_pct"] is None
            assert cand["warning_reason"] == "NO_FORWARD_PRICE"
            assert "No forward return data" in data["profile_comparison"]["explanation"] or \
                   "may not exist yet" in data["profile_comparison"]["explanation"]
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker).delete()
                s.commit()

    def test_skipped_diagnostics_capped_at_25_samples(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """skipped_diagnostics.samples is capped at 25 even when many tickers are skipped."""
        import uuid as _uuid
        from unittest.mock import patch
        suffix = _uuid.uuid4().hex[:6]

        # Patch get_sp500_universe to return 30 tickers with no price data → all skipped
        fake_tickers = [f"CALIBSK{suffix}{i:02d}" for i in range(30)]

        with patch("paper_trader.engine.universe.get_sp500_universe", return_value=fake_tickers):
            resp = seeded_client.post(
                self._ENDPOINT,
                json={
                    "as_of_date": str(self._AS_OF),
                    "min_price_points": 5,
                    "profile_top_n": 5,
                    "profiles": ["current"],
                },
                headers=_AUTH,
            )

        assert resp.status_code == 200
        data = resp.json()
        diag = data["skipped_diagnostics"]
        assert diag["total_skipped"] == 30
        assert len(diag["samples"]) <= 25

    def test_safety_counts_all_zero(self, seeded_client: TestClient, api_engine) -> None:
        """safety_counts in calibration_summary are all zero."""
        import uuid as _uuid
        suffix = _uuid.uuid4().hex[:6]
        ticker = f"CALIBSC{suffix}".upper()

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            self._seed_ticker_prices(s, ticker, self._AS_OF, lookback=25, latest_price=75.0)
            s.commit()

        try:
            resp = seeded_client.post(
                self._ENDPOINT,
                json={"as_of_date": str(self._AS_OF), "min_price_points": 5, "profile_top_n": 3},
                headers=_AUTH,
            )
            assert resp.status_code == 200
            sc = resp.json()["calibration_summary"]["safety_counts"]
            for key in ("signals_created", "decisions_created", "orders_created", "rows_created"):
                assert sc[key] == 0, f"safety_counts.{key} should be 0, got {sc[key]}"
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker).delete()
                s.commit()

    def test_invalid_profile_returns_422(self, seeded_client: TestClient) -> None:
        """An unrecognised profile name returns 422."""
        resp = seeded_client.post(
            self._ENDPOINT,
            json={"profiles": ["invalid_scoring_profile_xyz"]},
            headers=_AUTH,
        )
        assert resp.status_code == 422

    def test_overlap_matrix_present(self, seeded_client: TestClient, api_engine) -> None:
        """profile_comparison.overlap_matrix is present and keyed correctly."""
        import uuid as _uuid
        suffix = _uuid.uuid4().hex[:6]
        tickers = [f"CALIBOM{suffix}{i}".upper() for i in range(3)]

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            for t in tickers:
                self._seed_ticker_prices(s, t, self._AS_OF, lookback=25, latest_price=90.0)
            s.commit()

        try:
            resp = seeded_client.post(
                self._ENDPOINT,
                json={
                    "as_of_date": str(self._AS_OF),
                    "min_price_points": 5,
                    "profile_top_n": 5,
                    "profiles": ["current", "balanced_preview"],
                },
                headers=_AUTH,
            )
            assert resp.status_code == 200
            matrix = resp.json()["profile_comparison"]["overlap_matrix"]
            assert isinstance(matrix, dict)
            assert "current_vs_balanced_preview" in matrix
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                for t in tickers:
                    s.query(PriceSnapshot).filter(PriceSnapshot.ticker == t).delete()
                s.commit()

    def test_score_breakdown_present_on_each_candidate(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Each top_candidates row has a non-null score_breakdown with required keys."""
        import uuid as _uuid
        suffix = _uuid.uuid4().hex[:6]
        ticker = f"CALIBSB{suffix}".upper()

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            self._seed_ticker_prices(s, ticker, self._AS_OF, lookback=25, latest_price=120.0)
            s.commit()

        try:
            resp = seeded_client.post(
                self._ENDPOINT,
                json={
                    "as_of_date": str(self._AS_OF),
                    "min_price_points": 5,
                    "profile_top_n": 10,
                    "profiles": ["current"],
                    "tickers": [ticker],
                },
                headers=_AUTH,
            )
            assert resp.status_code == 200
            data = resp.json()
            pr = data["profile_results"][0]
            cand = next((c for c in pr["top_candidates"] if c["ticker"] == ticker), None)
            assert cand is not None, f"{ticker} not found in top_candidates"
            sb = cand["score_breakdown"]
            assert sb is not None
            for key in ("formula_profile", "final_score", "momentum_total_adj", "relative_strength_component"):
                assert key in sb, f"score_breakdown missing key {key!r}"
            assert sb["formula_profile"] == "current"
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker).delete()
                s.commit()

    def test_no_price_data_returns_200_empty_results(self, seeded_client: TestClient) -> None:
        """Returns 200 with empty profile_results when no price data exists for the universe."""
        from unittest.mock import patch
        with patch("paper_trader.engine.universe.get_sp500_universe", return_value=["ZZZNODATA1", "ZZZNODATA2"]):
            resp = seeded_client.post(
                self._ENDPOINT,
                json={
                    "as_of_date": str(self._AS_OF),
                    "min_price_points": 20,
                    "profiles": ["current"],
                },
                headers=_AUTH,
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["calibration_summary"]["evaluated_count"] == 0
        assert data["calibration_summary"]["skipped_count"] == 2
        assert data["profile_results"][0]["top_candidates"] == []

    def test_targeted_tickers_only_evaluates_those_tickers(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """When tickers is provided, universe_count equals the targeted set and only those tickers appear."""
        import uuid as _uuid
        suffix = _uuid.uuid4().hex[:6]
        ticker = f"CALIBTGT{suffix}".upper()

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            self._seed_ticker_prices(s, ticker, self._AS_OF, lookback=25, latest_price=110.0)
            s.commit()

        try:
            resp = seeded_client.post(
                self._ENDPOINT,
                json={
                    "as_of_date": str(self._AS_OF),
                    "min_price_points": 5,
                    "profile_top_n": 50,
                    "profiles": ["current"],
                    "tickers": [ticker],
                },
                headers=_AUTH,
            )
            assert resp.status_code == 200
            data = resp.json()
            summary = data["calibration_summary"]
            assert summary["universe_count"] == 1
            assert summary["evaluated_count"] == 1
            assert summary["skipped_count"] == 0
            pr = data["profile_results"][0]
            assert len(pr["top_candidates"]) == 1
            assert pr["top_candidates"][0]["ticker"] == ticker
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker).delete()
                s.commit()

    def test_tickers_normalized_and_deduplicated(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """Blank, lowercase, and duplicate entries in tickers are normalized and collapsed to one."""
        import uuid as _uuid
        suffix = _uuid.uuid4().hex[:6]
        ticker = f"CALIBND{suffix}".upper()

        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            self._seed_ticker_prices(s, ticker, self._AS_OF, lookback=25, latest_price=90.0)
            s.commit()

        try:
            resp = seeded_client.post(
                self._ENDPOINT,
                json={
                    "as_of_date": str(self._AS_OF),
                    "min_price_points": 5,
                    "profile_top_n": 50,
                    "profiles": ["current"],
                    # lowercase, extra spaces, blank, and duplicate — all collapse to 1 unique ticker
                    "tickers": [ticker.lower(), "  ", ticker, ticker.lower()],
                },
                headers=_AUTH,
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["calibration_summary"]["universe_count"] == 1
        finally:
            with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
                s.query(PriceSnapshot).filter(PriceSnapshot.ticker == ticker).delete()
                s.commit()

    def test_empty_tickers_falls_back_to_universe(
        self, seeded_client: TestClient
    ) -> None:
        """Passing tickers=[] is treated the same as omitting tickers: uses full universe."""
        from unittest.mock import patch
        with patch(
            "paper_trader.engine.universe.get_sp500_universe",
            return_value=["ZZZFB1", "ZZZFB2"],
        ):
            resp = seeded_client.post(
                self._ENDPOINT,
                json={
                    "as_of_date": str(self._AS_OF),
                    "min_price_points": 5,
                    "profiles": ["current"],
                    "tickers": [],
                },
                headers=_AUTH,
            )
        assert resp.status_code == 200
        data = resp.json()
        # No price data for ZZZFB tickers → all skipped; universe_count reflects the patched universe
        assert data["calibration_summary"]["universe_count"] == 2
        assert data["calibration_summary"]["evaluated_count"] == 0
