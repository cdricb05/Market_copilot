"""
tests/test_phase29j3_portfolio_analytics.py — Phase 29J.3 read-only Portfolio
Analytics aggregator + route.

Contract tests for:

    api.portfolio_analytics.build_portfolio_analytics(...)   (unit, injected loaders)
    GET /v1/portfolio/analytics                              (endpoint, monkeypatched loaders)

Network-free and DB-free. The four canonical read-only sources
(paper_trading_desk.load_performance, forward_evidence.load_attribution_history,
operational_book.load_operational_book, forward_evidence.load_holding_contributions)
are injected / monkeypatched with deterministic synthetic payloads, so the tests
exercise the aggregator's series shaping, contribution-aware winners/losers, the
unrealized fallback, the concentration descriptors, graceful degradation, auth, HTTP
method restriction and the READ-ONLY / paper-only safety posture — without any
provider call, ledger, DB, or prediction service.

This aggregator is a chart-data surface only. It must never create an order, signal,
trade decision, or model, and must never mutate any ledger / portfolio / target.
"""
from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

import paper_trader.api.app as appmod
import paper_trader.api.portfolio_analytics as pamod
from paper_trader.api.app import app
from paper_trader.config import get_settings

_ROUTE = "/v1/portfolio/chart-analytics"
_KEY = "portfolio-analytics-test-key"
_AUTH = {"X-API-Key": _KEY}


# --------------------------------------------------------------------------- #
# Deterministic synthetic source payloads
# --------------------------------------------------------------------------- #
def _perf():
    return {"status": "OK", "rows": [
        {"date": "2026-07-01", "nav": 100000, "cumulative_return_pct": 0.0,
         "benchmark_cumulative_return_pct": 0.0, "drawdown_pct": 0.0},
        {"date": "2026-07-02", "nav": 101000, "cumulative_return_pct": 1.0,
         "benchmark_cumulative_return_pct": 0.5, "drawdown_pct": 0.0},
        {"date": "2026-07-03", "nav": 100500, "cumulative_return_pct": 0.5,
         "benchmark_cumulative_return_pct": 0.7, "drawdown_pct": -0.49},
    ]}


def _attr():
    # attribution-history returns most-recent-first; the aggregator must re-sort ascending.
    return {"count": 2, "rows": [
        {"market_date": "2026-07-03", "daily_pnl": -500, "cumulative_pnl": 500, "daily_return_pct": -0.49},
        {"market_date": "2026-07-02", "daily_pnl": 1000, "cumulative_pnl": 1000, "daily_return_pct": 1.0},
    ]}


def _ob():
    return {"status": "OPERATIONAL_BOOK_OK", "operational_book": {
        "book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1", "nav": 100500,
        "desk_mark_date": "2026-07-03", "target_market_date": "2026-06-30",
        "current_target": {"alpha_market_date": "2026-07-03"},
        "portfolio_summary": {
            "cash": 500.0, "cash_weight": 0.005, "invested_value": 100000.0, "invested_weight": 0.995,
            "sector_exposure": [{"sector": "Tech", "weight": 0.4}, {"sector": "Health", "weight": 0.3}],
            "best_performers": [{"ticker": "AAA", "sector": "Tech", "unrealized_pnl": 200, "unrealized_pnl_pct": 0.02}],
            "worst_performers": [{"ticker": "BBB", "sector": "Health", "unrealized_pnl": -150, "unrealized_pnl_pct": -0.015}],
        },
        "holdings_detail": [
            {"ticker": "AAA", "sector": "Tech", "current_weight": 0.06, "target_weight": 0.04,
             "weight_drift": 0.02, "market_value": 6000},
            {"ticker": "BBB", "sector": "Health", "current_weight": 0.03, "target_weight": 0.05,
             "weight_drift": -0.02, "market_value": 3000},
        ],
        "canonical_state": {"nav": 100500, "valuation_date": "2026-07-03",
                            "target_date": "2026-06-30", "cash": 500.0},
    }}


def _contrib():
    return {"available": True, "market_date": "2026-07-03", "prior_market_date": "2026-07-02", "holdings": [
        {"ticker": "AAA", "sector": "Tech", "pnl_contribution": 120.0, "contribution_pp": 0.12, "daily_return_pct": 2.0},
        {"ticker": "BBB", "sector": "Health", "pnl_contribution": -620.0, "contribution_pp": -0.62, "daily_return_pct": -3.0},
    ]}


def _build_full():
    return pamod.build_portfolio_analytics(
        performance_loader=_perf, attribution_loader=_attr,
        operational_book_loader=_ob, contributions_loader=_contrib)


# --------------------------------------------------------------------------- #
# Unit tests — composition
# --------------------------------------------------------------------------- #
def test_status_and_top_level_shape():
    d = _build_full()
    assert d["status"] == pamod.STATUS_OK
    assert d["canonical_owner"] == pamod.CANONICAL_OWNER
    assert d["book_id"] == "alpha_paper_book_1"
    assert set(d["chart_keys"]) == set(pamod.CHART_KEYS)
    for sec in ("performance", "pnl", "allocation", "contributors", "drift", "freshness"):
        assert sec in d


def test_performance_series_ascending_with_excess_and_drawdown():
    p = _build_full()["performance"]
    assert p["available"] is True
    assert p["n_sessions"] == 3
    dates = [pt["date"] for pt in p["points"]]
    assert dates == sorted(dates)
    last = p["last"]
    assert last["date"] == "2026-07-03"
    # excess_pp = cum_return_pct - spy_cum_return_pct = 0.5 - 0.7
    assert last["excess_pp"] == pytest.approx(-0.2, abs=1e-9)
    assert last["drawdown_pct"] == pytest.approx(-0.49, abs=1e-9)


def test_pnl_series_reordered_ascending():
    p = _build_full()["pnl"]
    assert p["available"] is True
    assert [pt["date"] for pt in p["points"]] == ["2026-07-02", "2026-07-03"]
    assert p["points"][0]["daily_pnl"] == 1000.0
    assert p["last"]["date"] == "2026-07-03"


def test_allocation_concentration_descriptors():
    a = _build_full()["allocation"]
    assert a["available"] is True
    assert a["cash_weight"] == pytest.approx(0.005)
    assert a["invested_weight"] == pytest.approx(0.995)
    assert len(a["top_holdings"]) == 2
    # HHI = 0.06^2 + 0.03^2 ; top5 weight = 0.06 + 0.03
    assert a["hhi"] == pytest.approx(0.0045, abs=1e-9)
    assert a["concentration_top5"] == pytest.approx(0.09, abs=1e-9)
    assert a["n_holdings"] == 2
    # sectors sorted by descending weight
    assert [s["sector"] for s in a["sectors"]] == ["Tech", "Health"]


def test_contributors_contribution_aware():
    c = _build_full()["contributors"]
    assert c["available"] is True
    assert c["source"] == "holding_contributions"
    assert c["metric"] == "daily_pnl_contribution"
    assert c["as_of"] == "2026-07-03"
    assert c["top"][0]["ticker"] == "AAA"
    assert c["top"][0]["contribution_usd"] == pytest.approx(120.0)
    assert c["bottom"][0]["ticker"] == "BBB"
    assert c["bottom"][0]["contribution_usd"] == pytest.approx(-620.0)


def test_contributors_unrealized_fallback_when_no_processed_close():
    d = pamod.build_portfolio_analytics(
        performance_loader=_perf, attribution_loader=_attr, operational_book_loader=_ob,
        contributions_loader=lambda: {"available": False, "holdings": []})
    c = d["contributors"]
    assert c["source"] == "unrealized_fallback"
    assert c["metric"] == "unrealized_return"
    assert c["top"][0]["ticker"] == "AAA"
    assert c["bottom"][0]["ticker"] == "BBB"


def test_drift_sorted_by_abs_drift():
    dr = _build_full()["drift"]
    assert dr["available"] is True
    assert dr["max_abs_drift"] == pytest.approx(0.02)
    assert dr["names_off_target"] == 2
    # sorted by |drift| desc — both are 0.02, but each row carries current/target/drift
    for h in dr["holdings"]:
        assert h["current_weight"] is not None
        assert h["target_weight"] is not None
        assert h["weight_drift"] is not None


def test_freshness_and_safety_posture():
    d = _build_full()
    fr = d["freshness"]
    assert fr["valuation_date"] == "2026-07-03"
    assert fr["target_date"] == "2026-06-30"
    assert fr["performance_through"] == "2026-07-03"
    assert len(fr["sources"]) == 4
    # read-only / paper-only posture — never an order or automation surface.
    assert d["read_only"] is True
    assert d["paper_only"] is True
    assert d["orders_enabled"] is False
    assert d["broker_enabled"] is False
    assert d["automation_enabled"] is False
    assert d["live_orders_enabled"] is False
    assert d["performed_write"] is False


def test_graceful_all_empty():
    d = pamod.build_portfolio_analytics(
        performance_loader=lambda: {}, attribution_loader=lambda: {},
        operational_book_loader=lambda: {}, contributions_loader=lambda: {})
    assert d["status"] == pamod.STATUS_EMPTY
    for sec in ("performance", "pnl", "allocation", "contributors", "drift"):
        assert d[sec]["available"] is False
    # still carries the read-only safety posture and never raises
    assert d["read_only"] is True
    assert d["performed_write"] is False


def test_loader_failure_degrades_not_raises():
    def boom():
        raise RuntimeError("source down")
    d = pamod.build_portfolio_analytics(
        performance_loader=boom, attribution_loader=boom,
        operational_book_loader=boom, contributions_loader=boom)
    assert d["status"] == pamod.STATUS_EMPTY
    assert d["performance"]["available"] is False


# --------------------------------------------------------------------------- #
# Endpoint tests
# --------------------------------------------------------------------------- #
@pytest.fixture()
def client(monkeypatch):
    os.environ["PAPER_TRADER_SERVICE_API_KEY"] = _KEY
    os.environ.setdefault(
        "PAPER_TRADER_DATABASE_URL",
        "postgresql+psycopg2://u:p@localhost:5432/paper_trader_test_unused",
    )
    os.environ.setdefault("PAPER_TRADER_STOCK_PREDICTION_API_URL", "http://127.0.0.1:9000")
    get_settings.cache_clear()
    c = TestClient(app)
    try:
        yield c
    finally:
        c.close()
        get_settings.cache_clear()


def _patch_sources(monkeypatch):
    """Patch the four canonical loaders the aggregator calls so the endpoint is
    deterministic and never touches a ledger, DB, or provider."""
    monkeypatch.setattr(pamod._desk, "load_performance", lambda **k: _perf())
    monkeypatch.setattr(pamod._fe, "load_attribution_history", lambda **k: _attr())
    monkeypatch.setattr(pamod._opbook, "load_operational_book", lambda **k: _ob())
    monkeypatch.setattr(pamod._fe, "load_holding_contributions", lambda **k: _contrib())


def test_auth_required(client):
    assert client.get(_ROUTE).status_code in (401, 403)


def test_post_not_allowed(client):
    assert client.post(_ROUTE, headers=_AUTH).status_code == 405


def test_endpoint_full_payload(client, monkeypatch):
    _patch_sources(monkeypatch)
    r = client.get(_ROUTE, headers=_AUTH)
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == pamod.STATUS_OK
    assert d["performance"]["available"] is True
    assert d["pnl"]["available"] is True
    assert d["allocation"]["available"] is True
    assert d["contributors"]["available"] is True
    assert d["drift"]["available"] is True
    assert d["read_only"] is True and d["orders_enabled"] is False


def test_endpoint_graceful_when_sources_fail(client, monkeypatch):
    def boom(**k):
        raise RuntimeError("down")
    monkeypatch.setattr(pamod._desk, "load_performance", boom)
    monkeypatch.setattr(pamod._fe, "load_attribution_history", boom)
    monkeypatch.setattr(pamod._opbook, "load_operational_book", boom)
    monkeypatch.setattr(pamod._fe, "load_holding_contributions", boom)
    r = client.get(_ROUTE, headers=_AUTH)
    assert r.status_code == 200
    assert r.json()["status"] == pamod.STATUS_EMPTY
