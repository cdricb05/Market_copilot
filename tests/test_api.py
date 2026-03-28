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

Test ordering matters:
    TestAuthentication → TestUnseededPortfolio → TestSeededEndpoints

seeded_client is first used by TestSeededEndpoints, so the portfolio is not
committed until after TestUnseededPortfolio has already run.

Requires PAPER_TRADER_TEST_DATABASE_URL (entire module skipped when absent).
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from paper_trader.api.app import app
from paper_trader.config import get_settings
from paper_trader.constants import CashEntryType
from paper_trader.db.models import Base, Portfolio
from paper_trader.db.session import reset_engine_state
from paper_trader.engine.portfolio import append_cash_entry

_TEST_API_KEY = "test-secret-key"
_NOW          = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
_AUTH         = {"X-API-Key": _TEST_API_KEY}


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


# ---------------------------------------------------------------------------
# Unseeded state — must run before seeded_client commits data
# ---------------------------------------------------------------------------

class TestUnseededPortfolio:
    def test_portfolio_503_when_not_seeded(self, client: TestClient) -> None:
        resp = client.get("/v1/portfolio", headers=_AUTH)
        assert resp.status_code == 503


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
