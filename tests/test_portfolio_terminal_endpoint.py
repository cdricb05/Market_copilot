"""
tests/test_portfolio_terminal_endpoint.py — Phase 14-B portfolio-terminal route.

Contract tests for the read-only aggregation route:

    GET /v1/dashboard/portfolio-terminal

DB-backed (mirrors test_api.py's engine/client fixtures) so the portfolio /
positions / orders slices exercise real queries — no research runner, no network,
no EODHD key, no prediction call. Verifies auth, the eleven sections, the seeded
capital summary, the REAL empty states (positions=[], pending/filled/history
order buckets all present and empty — never "Connect to Load"), that the
aggregation is read-only (no DB writes), and that the route is GET-only. Skipped
entirely without PAPER_TRADER_TEST_DATABASE_URL.
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
from paper_trader.api import portfolio_terminal as pt
from paper_trader.config import get_settings
from paper_trader.constants import CashEntryType
from paper_trader.db.models import Base, Order, Portfolio, Position
from paper_trader.db.session import reset_engine_state
from paper_trader.engine.portfolio import append_cash_entry

_PT = "/v1/dashboard/portfolio-terminal"
_TEST_API_KEY = "portfolio-terminal-test-key"
_AUTH = {"X-API-Key": _TEST_API_KEY}
_NOW = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)

_SECTIONS = ("summary", "positions", "paper_orders", "performance", "risk",
             "capacity", "alpha_context", "alerts", "warnings", "safety", "provenance")


@pytest.fixture(scope="module")
def api_engine():
    url = os.environ.get("PAPER_TRADER_TEST_DATABASE_URL")
    if not url:
        pytest.skip("PAPER_TRADER_TEST_DATABASE_URL not set — skipping portfolio-terminal endpoint tests.")
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
    db_url = api_engine.url.render_as_string(hide_password=False)
    os.environ["PAPER_TRADER_DATABASE_URL"] = db_url
    os.environ["PAPER_TRADER_SERVICE_API_KEY"] = _TEST_API_KEY
    get_settings.cache_clear()
    reset_engine_state()
    c = TestClient(app)
    try:
        yield c
    finally:
        c.close()
        get_settings.cache_clear()
        reset_engine_state()


@pytest.fixture(scope="module")
def seeded_client(client, api_engine):
    with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
        if session.query(Portfolio).first() is None:
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
                description="Portfolio-terminal test initial capital",
            )
            session.commit()
    yield client


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #

def test_requires_api_key(seeded_client):
    assert seeded_client.get(_PT).status_code in (401, 403)


def test_returns_all_sections(seeded_client):
    body = seeded_client.get(_PT, headers=_AUTH).json()
    for section in _SECTIONS:
        assert section in body, f"missing section: {section}"
    assert body["status"] in ("OK", "DEGRADED")


def test_summary_is_seeded(seeded_client):
    s = seeded_client.get(_PT, headers=_AUTH).json()["summary"]
    assert s["seeded"] is True
    assert s["total_value"] is not None
    assert s["cash"] is not None
    assert s["max_positions"] >= 1


def test_real_empty_states(seeded_client):
    body = seeded_client.get(_PT, headers=_AUTH).json()
    # Positions is a real (possibly empty) list — never a "Connect to Load" string.
    assert isinstance(body["positions"], list)
    orders = body["paper_orders"]
    # All three buckets exist and are lists — pending is never conflated with history.
    for bucket in ("pending", "filled", "history"):
        assert isinstance(orders[bucket], list)
    assert "pending_count" in orders and "filled_count" in orders and "history_count" in orders


def test_positions_have_status_when_present(seeded_client):
    positions = seeded_client.get(_PT, headers=_AUTH).json()["positions"]
    for p in positions:
        assert p["status"] in (pt.POS_HOLD, pt.POS_WATCH, pt.POS_REVIEW_FOR_EXIT, pt.POS_PRICE_UNAVAILABLE)
        assert "reason" in p


def test_capacity_risk_and_alpha_context_present(seeded_client):
    body = seeded_client.get(_PT, headers=_AUTH).json()
    cap = body["capacity"]
    assert cap["capacity_state"] in ("NO_OPEN_POSITIONS", "CAPACITY_AVAILABLE", "MAX_POSITIONS_REACHED")
    risk = body["risk"]
    assert "risk_message" in risk and "risk_engine_explanation" in risk
    # The risk-engine explanation must frame itself as paper-only and never a
    # prompt to place a live order (it may mention it "never ... a live order").
    expl = risk["risk_engine_explanation"].lower()
    assert "paper only" in expl and "never" in expl
    assert "available" in body["alpha_context"]


def test_safety_and_read_only_provenance(seeded_client):
    body = seeded_client.get(_PT, headers=_AUTH).json()
    assert body["safety"]["paper_only"] is True
    prov = body["provenance"]
    assert prov["read_only"] is True
    assert prov["wrote_to_database"] is False
    assert prov["created_orders"] is False
    assert prov["called_prediction_service"] is False
    assert prov["made_loopback_http_calls"] is False


def test_aggregation_writes_nothing(seeded_client, api_engine):
    def _counts():
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            return (s.query(Position).count(), s.query(Order).count())

    before = _counts()
    seeded_client.get(_PT, headers=_AUTH)
    seeded_client.get(_PT, headers=_AUTH)
    assert _counts() == before


def test_route_is_get_only(seeded_client):
    assert seeded_client.post(_PT, headers=_AUTH, json={}).status_code == 405


def test_app_wires_portfolio_terminal_route():
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "api" / "app.py").read_text(encoding="utf-8")
    assert '@app.get(\n    "/v1/dashboard/portfolio-terminal"' in src
