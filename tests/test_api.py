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
    TestHealth → TestAuthentication → TestUnseededPortfolio
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
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from paper_trader.api.app import app
from paper_trader.config import get_settings
from paper_trader.constants import CashEntryType, JobRunStatus, WorkflowType
from paper_trader.db.models import Base, JobRun, Portfolio, PortfolioSnapshot
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
    with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            conn.execute(table.delete())
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
    db_url = api_engine.url.render_as_string(hide_password=False)
    os.environ["PAPER_TRADER_DATABASE_URL"]     = db_url
    os.environ["PAPER_TRADER_SERVICE_API_KEY"]  = _TEST_API_KEY
    get_settings.cache_clear()
    reset_engine_state()
    with TestClient(app) as c:
        yield c
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

    def test_trigger_snapshot_missing_price_returns_422(
        self, seeded_client: TestClient, api_engine
    ) -> None:
        """
        Open position with no PriceSnapshot returns 422.

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
        assert resp.status_code == 422
        assert "TSLA" in resp.json()["detail"]
