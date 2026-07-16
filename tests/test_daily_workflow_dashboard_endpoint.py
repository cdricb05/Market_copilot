"""
tests/test_daily_workflow_dashboard_endpoint.py — Phase 14-B daily-workflow route.

Contract tests for the read-only aggregation route:

    GET /v1/dashboard/daily-workflow

DB-backed (mirrors test_api.py's engine/client fixtures) so the workflow / review
slices exercise real queries — no research runner, no network, no EODHD key, no
prediction call. Verifies auth, the eleven sections, that exactly one stage is
active, that the review queue separates active (NEW) work from rejected / completed
history (grouped by stable candidate identity), that the aggregation is read-only
(no DB writes), and that the route is GET-only. Skipped entirely without
PAPER_TRADER_TEST_DATABASE_URL.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from paper_trader.api.app import app
from paper_trader.api import daily_workflow_dashboard as dw
from paper_trader.config import get_settings
from paper_trader.constants import CashEntryType
from paper_trader.db.models import Base, CandidateReview, Order, Portfolio, Position
from paper_trader.db.session import reset_engine_state
from paper_trader.engine.portfolio import append_cash_entry

_DW = "/v1/dashboard/daily-workflow"
_TEST_API_KEY = "daily-workflow-test-key"
_AUTH = {"X-API-Key": _TEST_API_KEY}
_NOW = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)

_SECTIONS = ("summary", "stages", "candidates", "review", "signals",
             "decisions", "capacity", "next_action", "warnings", "safety", "provenance")


@pytest.fixture(scope="module")
def api_engine():
    url = os.environ.get("PAPER_TRADER_TEST_DATABASE_URL")
    if not url:
        pytest.skip("PAPER_TRADER_TEST_DATABASE_URL not set — skipping daily-workflow endpoint tests.")
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
                description="Daily-workflow test initial capital",
            )
            session.commit()
    yield client


@pytest.fixture
def candidates(api_engine):
    """Insert one NEW (today) and one REJECTED candidate, then clean up.

    Exercises the active-queue vs history separation with real rows.
    """
    key = f"dw-test-{uuid.uuid4().hex[:8]}"
    ids: list = []
    with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
        for ticker, status in (("AAAT", "NEW"), ("BBBT", "REJECTED")):
            cr = CandidateReview(
                idempotency_key=key, ticker=ticker,
                preview_decision="CONSIDER", preview_score="80.0",
                status="OK", review_status=status,
                prediction_recommendation="BUY", prediction_confidence="0.90",
            )
            session.add(cr)
            session.flush()
            ids.append(cr.id)
        session.commit()
    yield {"new": "AAAT", "rejected": "BBBT"}
    with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
        for cid in ids:
            obj = session.get(CandidateReview, cid)
            if obj is not None:
                session.delete(obj)
        session.commit()


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #

def test_requires_api_key(seeded_client):
    assert seeded_client.get(_DW).status_code in (401, 403)


def test_returns_all_sections(seeded_client):
    body = seeded_client.get(_DW, headers=_AUTH).json()
    for section in _SECTIONS:
        assert section in body, f"missing section: {section}"
    assert body["status"] in ("OK", "DEGRADED")


def test_six_stages_and_exactly_one_active(seeded_client):
    body = seeded_client.get(_DW, headers=_AUTH).json()
    stages = body["stages"]
    assert [s["stage"] for s in stages] == list(dw.STAGE_ORDER)
    actives = [s for s in stages if s["is_active"]]
    assert len(actives) == 1
    assert body["summary"]["active_stage"] == actives[0]["stage"]


def test_review_separates_active_from_history(seeded_client, candidates):
    body = seeded_client.get(_DW, headers=_AUTH).json()
    review = body["review"]
    active_tickers = {r["ticker"] for r in review["active_review_queue"]}
    history_tickers = {r["ticker"] for r in review["recent_review_history"]}
    # The NEW candidate is actionable; the REJECTED one is history, never pending.
    assert candidates["new"] in active_tickers
    assert candidates["rejected"] not in active_tickers
    assert candidates["rejected"] in history_tickers
    assert review["grouped_by_identity"] is True


def test_next_action_is_labelled(seeded_client):
    na = seeded_client.get(_DW, headers=_AUTH).json()["next_action"]
    assert na["action"]
    assert na["action_label"]
    assert na["action_target"]
    assert na["safety_context"]


def test_capacity_and_safety_and_provenance(seeded_client):
    body = seeded_client.get(_DW, headers=_AUTH).json()
    cap = body["capacity"]
    assert cap["capacity_state"] in ("NO_OPEN_POSITIONS", "CAPACITY_AVAILABLE", "MAX_POSITIONS_REACHED")
    assert body["safety"]["paper_only"] is True
    prov = body["provenance"]
    assert prov["read_only"] is True
    assert prov["wrote_to_database"] is False
    assert prov["created_orders"] is False
    assert prov["called_prediction_service"] is False
    assert prov["made_loopback_http_calls"] is False


def test_aggregation_writes_nothing(seeded_client, api_engine, candidates):
    def _counts():
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            return (s.query(Position).count(), s.query(Order).count(),
                    s.query(CandidateReview).count())

    before = _counts()
    seeded_client.get(_DW, headers=_AUTH)
    seeded_client.get(_DW, headers=_AUTH)
    assert _counts() == before


def test_route_is_get_only(seeded_client):
    assert seeded_client.post(_DW, headers=_AUTH, json={}).status_code == 405


def test_app_wires_daily_workflow_route():
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "api" / "app.py").read_text(encoding="utf-8")
    assert '@app.get(\n    "/v1/dashboard/daily-workflow"' in src
