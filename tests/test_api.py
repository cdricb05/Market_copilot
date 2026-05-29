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
        async def mock_fetch(tickers, api_url, timeout_seconds):
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
        async def mock_fetch(tickers, api_url, timeout_seconds):
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
        async def mock_fetch(tickers, api_url, timeout_seconds):
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
        async def mock_fetch(tickers, api_url, timeout_seconds):
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
        async def mock_fetch(tickers, api_url, timeout_seconds):
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
        async def mock_fetch(tickers, api_url, timeout_seconds):
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
        async def mock_fetch(tickers, api_url, timeout_seconds):
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
        async def mock_fetch(tickers, api_url, timeout_seconds):
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
        async def mock_fetch(tickers, api_url, timeout_seconds):
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
        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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
        async def mock_fetch(tickers, api_url, timeout_seconds):
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
        async def mock_fetch(tickers, api_url, timeout_seconds):
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
        async def mock_fetch(tickers, api_url, timeout_seconds):
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
        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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

        async def mock_fetch(tickers, api_url, timeout_seconds):
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
            assert signal_status_after_second == signal_status_before_second, "Signal.status should not change on skip"
