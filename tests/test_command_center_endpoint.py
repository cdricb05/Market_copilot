"""
tests/test_command_center_endpoint.py — Phase 14-A command-center route.

Contract tests for the read-only aggregation route:

    GET /v1/dashboard/command-center

DB-backed (mirrors test_api.py's engine/client fixtures) so the workflow +
portfolio slices exercise real queries, while the current-alpha slice is fed by
a synthetic Phase 13-I backfill artifact via PAPER_TRADER_CURRENT_ALPHA_BACKFILL_DIR
— no research runner, no network, no EODHD key, no prediction call. Verifies
auth, the eight sections, that the aggregation is read-only (no DB writes, no
prediction probe), that the recommended next action is one labelled enum value,
and that the route is GET-only. Skipped entirely without PAPER_TRADER_TEST_DATABASE_URL.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from paper_trader.api.app import app
from paper_trader.api import command_center as cc
from paper_trader.config import get_settings
from paper_trader.constants import CashEntryType
from paper_trader.db.models import Base, Order, Portfolio, Position
from paper_trader.db.session import reset_engine_state
from paper_trader.engine.portfolio import append_cash_entry

from tests.test_current_alpha_decision_gate import _write_backfill

_CC = "/v1/dashboard/command-center"
_TEST_API_KEY = "command-center-test-key"
_AUTH = {"X-API-Key": _TEST_API_KEY}
_NOW = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)


# --------------------------------------------------------------------------- #
# DB-backed fixtures (mirror tests/test_api.py)
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="module")
def api_engine():
    url = os.environ.get("PAPER_TRADER_TEST_DATABASE_URL")
    if not url:
        pytest.skip("PAPER_TRADER_TEST_DATABASE_URL not set — skipping command-center endpoint tests.")
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
            description="Command-center test initial capital",
        )
        session.commit()
    yield client


@pytest.fixture
def fresh_backfill(tmp_path, monkeypatch) -> Path:
    """A DECISION_READY Phase 13-I artifact ending today -> a fresh (non-stale) mark."""
    d = tmp_path / "backfill"
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_BACKFILL_DIR", str(d))
    _write_backfill(d, end=date.today().isoformat())
    return d


_SECTIONS = ("system", "alpha", "workflow", "portfolio", "safety",
             "next_action", "warnings", "provenance")


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #

def test_requires_api_key(seeded_client):
    assert seeded_client.get(_CC).status_code in (401, 403)


def test_returns_all_sections(seeded_client, fresh_backfill):
    body = seeded_client.get(_CC, headers=_AUTH).json()
    for section in _SECTIONS:
        assert section in body, f"missing section: {section}"
    assert body["status"] in ("OK", "DEGRADED")


def test_alpha_primary_top50(seeded_client, fresh_backfill):
    body = seeded_client.get(_CC, headers=_AUTH).json()
    alpha = body["alpha"]
    assert alpha["available"] is True
    assert alpha["primary_paper_book"]["book"] == "TOP50"
    assert alpha["challenger_paper_book"]["label"] == "TOP25"
    assert alpha["top50"]["current_return_pct"] is not None
    assert alpha["top50"]["spy_cumulative_return_pct"] is not None


def test_next_action_is_labelled_enum(seeded_client, fresh_backfill):
    na = seeded_client.get(_CC, headers=_AUTH).json()["next_action"]
    valid = {
        cc.NA_RUN_REFRESH, cc.NA_LOAD_ALPHA, cc.NA_REVIEW_CANDIDATES,
        cc.NA_CREATE_SIGNALS, cc.NA_REVIEW_DECISIONS, cc.NA_MONITOR,
        cc.NA_RESOLVE_CAPACITY, cc.NA_REFRESH_APP, cc.NA_NONE,
    }
    assert na["action"] in valid
    assert na["action_label"]
    assert na["explanation"]
    assert na["ui_target"] in ("command-center", "daily-workflow", "portfolio", "research-audit")


def test_safety_and_read_only_provenance(seeded_client, fresh_backfill):
    body = seeded_client.get(_CC, headers=_AUTH).json()
    safety = body["safety"]
    assert safety["paper_only"] is True
    assert safety["no_broker_execution"] is True
    assert safety["automation_off"] is True
    assert safety["is_live_trading_approval"] is False
    prov = body["provenance"]
    assert prov["read_only"] is True
    assert prov["wrote_to_database"] is False
    assert prov["created_orders"] is False
    assert prov["created_signals"] is False
    assert prov["created_trade_decisions"] is False
    assert prov["invoked_daily_refresh"] is False
    assert prov["called_prediction_service"] is False
    assert prov["called_external_provider"] is False
    assert prov["made_loopback_http_calls"] is False


def test_prediction_tunnel_reported_not_probed(seeded_client, fresh_backfill):
    tunnel = seeded_client.get(_CC, headers=_AUTH).json()["system"]["prediction_tunnel"]
    assert tunnel["probed"] is False
    assert tunnel["status"] == "NOT_PROBED_READ_ONLY"


def test_aggregation_writes_nothing(seeded_client, api_engine, fresh_backfill):
    def _counts():
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            return (
                s.query(Position).count(),
                s.query(Order).count(),
            )

    before = _counts()
    seeded_client.get(_CC, headers=_AUTH)
    seeded_client.get(_CC, headers=_AUTH)
    assert _counts() == before


def test_portfolio_capacity_present(seeded_client, fresh_backfill):
    pf = seeded_client.get(_CC, headers=_AUTH).json()["portfolio"]
    assert pf["capacity_state"] in (cc.CAP_EMPTY, cc.CAP_AVAILABLE, cc.CAP_FULL)
    assert "max_positions" in pf
    assert "capacity_explanation" in pf


def test_no_backfill_is_graceful(client, tmp_path, monkeypatch):
    # No current-alpha backfill artifact -> the alpha slice degrades to unavailable
    # but the endpoint still returns HTTP 200 with a controlled, actionable body
    # (never a stack trace). Order-independent: does not depend on portfolio state.
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_BACKFILL_DIR", str(tmp_path / "empty"))
    resp = client.get(_CC, headers=_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["alpha"]["available"] is False
    assert body["next_action"]["action"]  # a concrete next action is still selected
    assert "portfolio" in body


def test_route_is_get_only(seeded_client):
    assert seeded_client.post(_CC, headers=_AUTH, json={}).status_code == 405


def test_app_wires_command_center_route():
    src = (Path(__file__).resolve().parents[1] / "api" / "app.py").read_text(encoding="utf-8")
    assert '@app.get(\n    "/v1/dashboard/command-center"' in src
