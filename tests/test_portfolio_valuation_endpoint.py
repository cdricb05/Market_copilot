"""
tests/test_portfolio_valuation_endpoint.py — Phase 14-C portfolio-valuation route.

Contract tests for the canonical read-only valuation route:

    GET /v1/dashboard/portfolio-valuation

DB-backed (mirrors the other dashboard endpoint fixtures) so the current mark,
snapshot separation, reconciliation invariant, and missing-price coverage are
exercised against real queries — no research runner, no network, no EODHD key,
no prediction call. Also verifies the CROSS-ENDPOINT invariant: the current
portfolio fields returned by /v1/dashboard/command-center and
/v1/dashboard/portfolio-terminal match the canonical valuation exactly.

Seeded scenario (initial_capital 10000, cached_cash 8000, cached_total_value
9960 — deliberately different from the re-marked total):
    AAA  qty 10  cost 1000  latest 110 -> mv 1100  (+10%  HOLD)
    BBB  qty  5  cost 1000  latest 170 -> mv  850  (-15%  REVIEW_FOR_EXIT)
    current_positions_value = 1950 ; current_total_value = 8000 + 1950 = 9950
    current_total_return_pct = (9950 - 10000)/10000 * 100 = -0.5

Skipped entirely without PAPER_TRADER_TEST_DATABASE_URL.
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
from paper_trader.db.models import (
    Base, JobRun, Portfolio, PortfolioSnapshot, Position, PriceSnapshot,
)
from paper_trader.db.session import reset_engine_state
from paper_trader.engine.portfolio import append_cash_entry

_PV = "/v1/dashboard/portfolio-valuation"
_CC = "/v1/dashboard/command-center"
_PT = "/v1/dashboard/portfolio-terminal"
_TEST_API_KEY = "portfolio-valuation-test-key"
_AUTH = {"X-API-Key": _TEST_API_KEY}
_NOW = datetime(2026, 7, 16, 20, 0, 0, tzinfo=timezone.utc)
_TODAY = _NOW.date()

_SECTIONS = ("status", "current_mark", "latest_snapshot", "reconciliation",
             "positions", "warnings", "safety", "provenance")


@pytest.fixture(scope="module")
def api_engine():
    url = os.environ.get("PAPER_TRADER_TEST_DATABASE_URL")
    if not url:
        pytest.skip("PAPER_TRADER_TEST_DATABASE_URL not set — skipping portfolio-valuation endpoint tests.")
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


def _price(session, ticker, px, *, source="test_source"):
    session.add(PriceSnapshot(
        ticker=ticker, price=Decimal(px), session_type="REGULAR", price_type="CLOSE",
        data_source=source, snapshot_ts=_NOW, market_date=_TODAY, job_run_id=None,
    ))


@pytest.fixture(scope="module")
def seeded_client(client, api_engine):
    with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
        if session.query(Portfolio).first() is None:
            portfolio = Portfolio(
                inception_date=_TODAY,
                initial_capital=Decimal("10000.00"),
                strategy_enabled=True, trading_enabled=True, allow_new_positions=True,
                config={"max_positions": 5},
                cached_cash=Decimal("8000.00"),
                cached_total_value=Decimal("9960.00"),   # intentionally != re-marked total
                cached_as_of_ts=_NOW,
            )
            session.add(portfolio)
            session.flush()
            append_cash_entry(
                session, portfolio_id=portfolio.id,
                entry_type=CashEntryType.INITIAL_CAPITAL, amount=Decimal("10000.00"),
                description="Portfolio-valuation test initial capital",
            )
            # Two covered positions.
            session.add(Position(ticker="AAA", qty=Decimal("10"), avg_cost=Decimal("100.00"),
                                 cost_basis=Decimal("1000.00"), opened_at=_NOW, last_updated=_NOW))
            session.add(Position(ticker="BBB", qty=Decimal("5"), avg_cost=Decimal("200.00"),
                                 cost_basis=Decimal("1000.00"), opened_at=_NOW, last_updated=_NOW))
            _price(session, "AAA", "110.000000")
            _price(session, "BBB", "170.000000")
            # One official snapshot (separate valuation timestamp/value).
            jr = JobRun(idempotency_key="pv-test-snap-1", workflow_type="POST_MARKET",
                        market_date=_TODAY, status="COMPLETED", started_at=_NOW)
            session.add(jr)
            session.flush()
            session.add(PortfolioSnapshot(
                job_run_id=jr.id, snapshot_ts=_NOW, market_date=_TODAY,
                cash=Decimal("8000.00"), positions_value=Decimal("1900.00"),
                total_value=Decimal("9900.00"), unrealized_pnl=Decimal("-100.00"),
                realized_pnl_cumulative=Decimal("0.00"), open_position_count=2,
            ))
            session.commit()
    yield client


def _mark(seeded_client):
    return seeded_client.get(_PV, headers=_AUTH).json()["current_mark"]


# --------------------------------------------------------------------------- #
# Auth + shape
# --------------------------------------------------------------------------- #

def test_requires_api_key(seeded_client):
    assert seeded_client.get(_PV).status_code in (401, 403)


def test_route_is_get_only(seeded_client):
    assert seeded_client.post(_PV, headers=_AUTH, json={}).status_code == 405


def test_returns_all_sections(seeded_client):
    body = seeded_client.get(_PV, headers=_AUTH).json()
    for section in _SECTIONS:
        assert section in body, f"missing section: {section}"
    assert body["status"] in ("OK", "DEGRADED")


def test_app_wires_portfolio_valuation_route():
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "api" / "app.py").read_text(encoding="utf-8")
    assert '@app.get(\n    "/v1/dashboard/portfolio-valuation"' in src


# --------------------------------------------------------------------------- #
# Current mark contract
# --------------------------------------------------------------------------- #

def test_cash_plus_positions_equals_total(seeded_client):
    cm = _mark(seeded_client)
    cash = Decimal(cm["current_cash"])
    pos = Decimal(cm["current_positions_value"])
    total = Decimal(cm["current_total_value"])
    assert cash == Decimal("8000.00")
    assert pos == Decimal("1950.00")
    assert cash + pos == total == Decimal("9950.00")


def test_current_return_formula(seeded_client):
    cm = _mark(seeded_client)
    # (9950 - 10000)/10000 * 100 = -0.5
    assert cm["current_total_return_pct"] == -0.5


def test_current_mark_does_not_use_cached_total(seeded_client):
    cm = _mark(seeded_client)
    # cached_total_value was 9960; the current mark must be the re-marked 9950.
    assert cm["current_total_value"] == "9950.00"
    assert cm["current_total_value"] != "9960.00"


def test_current_mark_metadata_present(seeded_client):
    cm = _mark(seeded_client)
    assert cm["valuation_type"] == "CURRENT_MARKED_EOD"
    assert cm["as_of_market_date"] == _TODAY.isoformat()
    assert cm["price_source"] == "test_source"
    assert cm["freshness_status"] == "FRESH"
    assert cm["calculated_at"]
    assert cm["covered_position_count"] == 2 and cm["total_position_count"] == 2
    assert cm["valuation_complete"] is True


def test_position_statuses(seeded_client):
    positions = seeded_client.get(_PV, headers=_AUTH).json()["positions"]
    by = {p["ticker"]: p for p in positions}
    assert by["AAA"]["status"] == "HOLD"
    assert by["BBB"]["status"] == "REVIEW_FOR_EXIT"


# --------------------------------------------------------------------------- #
# Snapshot separation + reconciliation
# --------------------------------------------------------------------------- #

def test_latest_snapshot_is_separate(seeded_client):
    snap = seeded_client.get(_PV, headers=_AUTH).json()["latest_snapshot"]
    assert snap is not None
    assert snap["valuation_type"] == "OFFICIAL_PORTFOLIO_SNAPSHOT"
    assert snap["total_value"] == "9900.00"
    # The snapshot total must NOT equal the current mark total (different as-of).
    assert snap["total_value"] != _mark(seeded_client)["current_total_value"]


def test_reconciliation(seeded_client):
    rec = seeded_client.get(_PV, headers=_AUTH).json()["reconciliation"]
    assert rec["cash_plus_positions"] == "9950.00"
    assert rec["reported_current_total"] == "9950.00"
    assert Decimal(rec["reconciliation_delta"]) == Decimal("0.00")
    assert rec["reconciled"] is True
    # Comparisons (not interchangeable values).
    assert rec["vs_cached_total_value"]["cached_total_value"] == "9960.00"
    assert Decimal(rec["vs_cached_total_value"]["delta"]) == Decimal("-10.00")
    assert rec["vs_latest_snapshot_total"]["snapshot_total_value"] == "9900.00"
    assert Decimal(rec["vs_latest_snapshot_total"]["delta"]) == Decimal("50.00")


# --------------------------------------------------------------------------- #
# Missing-price behaviour (coverage + partial mark)
# --------------------------------------------------------------------------- #

def test_missing_price_behaviour(seeded_client, api_engine):
    with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
        s.add(Position(ticker="CCC", qty=Decimal("2"), avg_cost=Decimal("300.00"),
                       cost_basis=Decimal("600.00"), opened_at=_NOW, last_updated=_NOW))
        s.commit()
    try:
        body = seeded_client.get(_PV, headers=_AUTH).json()
        cm = body["current_mark"]
        assert cm["valuation_complete"] is False
        assert cm["total_position_count"] == 3
        assert cm["covered_position_count"] == 2
        assert cm["missing_price_count"] == 1
        # A partial mark must NOT be marked reconciled even if the delta is zero.
        assert body["reconciliation"]["reconciled"] is False
        assert any("no current owned price" in w or "partial" in w for w in body["warnings"])
        ccc = next(p for p in body["positions"] if p["ticker"] == "CCC")
        assert ccc["status"] == "PRICE_UNAVAILABLE"
        # covered value is still exposed (never silently substituted)
        assert cm["current_positions_value"] == "1950.00"
    finally:
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            s.query(Position).filter(Position.ticker == "CCC").delete()
            s.commit()


# --------------------------------------------------------------------------- #
# Read-only provenance + no writes
# --------------------------------------------------------------------------- #

def test_read_only_provenance(seeded_client):
    prov = seeded_client.get(_PV, headers=_AUTH).json()["provenance"]
    assert prov["read_only"] is True
    for k in ("wrote_to_database", "created_orders", "created_signals",
              "created_trade_decisions", "invoked_daily_refresh",
              "called_prediction_service", "called_external_provider",
              "made_loopback_http_calls"):
        assert prov[k] is False


def test_aggregation_writes_nothing(seeded_client, api_engine):
    def _counts():
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            return (s.query(Position).count(), s.query(PortfolioSnapshot).count(),
                    s.query(PriceSnapshot).count())
    before = _counts()
    seeded_client.get(_PV, headers=_AUTH)
    seeded_client.get(_PV, headers=_AUTH)
    assert _counts() == before


def test_controlled_partial_failure(seeded_client, monkeypatch):
    # A failing snapshot dependency must degrade to a warning, never a 500.
    import paper_trader.api.portfolio_valuation as pv

    def _boom(*a, **k):
        raise RuntimeError("snapshot boom")

    monkeypatch.setattr(pv, "_latest_snapshot", _boom)
    resp = seeded_client.get(_PV, headers=_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["latest_snapshot"] is None
    assert any("snapshot" in w.lower() for w in body["warnings"])
    # the current mark is still fully computed
    assert body["current_mark"]["current_total_value"] == "9950.00"


# --------------------------------------------------------------------------- #
# Cross-endpoint consistency (valuation == command-center == portfolio-terminal)
# --------------------------------------------------------------------------- #

def test_cross_endpoint_current_fields_match(seeded_client):
    cm = seeded_client.get(_PV, headers=_AUTH).json()["current_mark"]
    ccpf = seeded_client.get(_CC, headers=_AUTH).json()["portfolio"]
    pts = seeded_client.get(_PT, headers=_AUTH).json()["summary"]

    # total value
    assert cm["current_total_value"] == ccpf["total_value"] == pts["total_value"] == "9950.00"
    # cash
    assert cm["current_cash"] == ccpf["cash"] == pts["cash"] == "8000.00"
    # invested value
    assert cm["current_positions_value"] == ccpf["invested_value"] == pts["invested_value"] == "1950.00"
    # current return
    assert cm["current_total_return_pct"] == ccpf["total_return_pct"] == pts["total_return_pct"] == -0.5
    # open positions + capacity
    assert ccpf["open_positions"] == 2 and pts["open_positions"] == 2
    assert ccpf["max_positions"] == 5 and pts["max_positions"] == 5


def test_command_center_keeps_snapshot_return_separate(seeded_client):
    ccpf = seeded_client.get(_CC, headers=_AUTH).json()["portfolio"]
    # current mark return (-0.5) is distinct from the official snapshot return (-1.0)
    assert ccpf["total_return_pct"] == -0.5
    assert ccpf["latest_performance_return_pct"] == -1.0
