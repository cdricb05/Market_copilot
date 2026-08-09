"""tests/test_market_history_endpoint.py — read-only per-instrument market HISTORY route.

Contract tests for the shared Market Detail drill-down owner:

    GET /v1/market/history?key=<instrument>&window=30D|90D|1Y

Network-free and DB-free. The yfinance history owner (`fetch_historical_prices`) and the
FRED range owner (`_fred_history`) are monkeypatched with deterministic synthetic series,
so the test exercises the endpoint's instrument registry, window clamping, series shaping,
graceful degradation, auth, GET-only posture and the read-only safety contract — without any
provider call, API key, DB, or prediction service.

This endpoint reuses the SAME authoritative owners already behind /v1/market/indicators and
/v1/market/context. Every value is an observed price/rate; it is reference/context only and
must never present itself as a signal, forecast, recommendation, order, or automation surface.
"""
from __future__ import annotations

import os
from datetime import date, timedelta

import pytest
from fastapi.testclient import TestClient

import paper_trader.api.app as appmod
from paper_trader.api.app import app
from paper_trader.config import get_settings

_ROUTE = "/v1/market/history"
_KEY = "market-history-test-key"
_AUTH = {"X-API-Key": _KEY}


def _series(symbol: str, n: int = 40) -> list[dict]:
    """A rising 1-per-day close series ending today, so it lands inside every window."""
    from decimal import Decimal
    end = date.today()
    base = {"^GSPC": 6000.0, "CL=F": 75.0, "GC=F": 4000.0}.get(symbol, 100.0)
    return [
        {"market_date": end - timedelta(days=(n - 1 - i)),
         "price": Decimal(str(round(base * (1 + 0.001 * i), 4)))}
        for i in range(n)
    ]


@pytest.fixture()
def client(monkeypatch):
    os.environ["PAPER_TRADER_SERVICE_API_KEY"] = _KEY
    os.environ.setdefault(
        "PAPER_TRADER_DATABASE_URL",
        "postgresql+psycopg2://u:p@localhost:5432/paper_trader_test_unused",
    )
    os.environ.setdefault("PAPER_TRADER_STOCK_PREDICTION_API_URL", "http://127.0.0.1:9000")
    os.environ.pop("PAPER_TRADER_FRED_API_KEY", None)
    get_settings.cache_clear()
    appmod._MARKET_HISTORY_CACHE.clear()
    c = TestClient(app)
    try:
        yield c
    finally:
        c.close()
        appmod._MARKET_HISTORY_CACHE.clear()
        get_settings.cache_clear()


# --------------------------------------------------------------------------- #
# Auth + method posture
# --------------------------------------------------------------------------- #
def test_auth_required(client):
    r = client.get(_ROUTE, params={"key": "sp500"})
    assert r.status_code in (401, 403)


def test_post_not_allowed(client):
    r = client.post(_ROUTE, headers=_AUTH, params={"key": "sp500"})
    assert r.status_code == 405


# --------------------------------------------------------------------------- #
# yfinance instruments
# --------------------------------------------------------------------------- #
def test_yfinance_series_shape_and_safety(client, monkeypatch):
    def fake_hist(tickers, start_date, end_date):
        return {sym: _series(sym) for sym in tickers}, {}
    monkeypatch.setattr(appmod, "fetch_historical_prices", fake_hist)

    r = client.get(_ROUTE, headers=_AUTH, params={"key": "sp500", "window": "90D"})
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "ok"
    assert d["key"] == "sp500"
    assert d["label"] == "S&P 500"
    assert d["symbol"] == "^GSPC"
    assert d["source"] == "yfinance"
    assert d["available"] is True
    assert d["window"] == "90D" and d["lookback_days"] == 90
    assert len(d["points"]) >= 2
    assert d["points"][0]["date"] < d["points"][-1]["date"]
    assert d["last_close"] is not None and d["first_close"] is not None
    # rising series → positive last-step change.
    assert d["change"] > 0 and d["change_pct"] > 0
    assert d["as_of"] == d["points"][-1]["date"]
    # read-only safety posture — context, never a signal / order / automation.
    for flag in ("read_only", "reference_only"):
        assert d[flag] is True
    for flag in ("is_signal", "is_prediction", "is_recommendation", "creates_orders",
                 "creates_signals", "mutates_state", "automation_enabled"):
        assert d[flag] is False


@pytest.mark.parametrize("win,expected", [("30D", 30), ("90D", 90), ("1Y", 365), ("bogus", 90)])
def test_window_clamping(client, monkeypatch, win, expected):
    monkeypatch.setattr(appmod, "fetch_historical_prices",
                        lambda t, s, e: ({sym: _series(sym) for sym in t}, {}))
    d = client.get(_ROUTE, headers=_AUTH, params={"key": "wti", "window": win}).json()
    assert d["lookback_days"] == expected


def test_yfinance_unavailable_is_honest(client, monkeypatch):
    monkeypatch.setattr(appmod, "fetch_historical_prices", lambda t, s, e: ({}, {}))
    d = client.get(_ROUTE, headers=_AUTH, params={"key": "gold", "window": "1Y"}).json()
    assert d["status"] == "unavailable"
    assert d["available"] is False
    assert d["points"] == []
    assert "unavailable" in d["reason"].lower()
    # honest, not fabricated: no numbers invented.
    assert d["last_close"] is None


def test_provider_exception_degrades(client, monkeypatch):
    def boom(tickers, start_date, end_date):
        raise RuntimeError("provider down")
    monkeypatch.setattr(appmod, "fetch_historical_prices", boom)
    r = client.get(_ROUTE, headers=_AUTH, params={"key": "sp500"})
    assert r.status_code == 200
    assert r.json()["available"] is False


# --------------------------------------------------------------------------- #
# FRED instruments (rates + USD Broad)
# --------------------------------------------------------------------------- #
def test_fred_series_available(client, monkeypatch):
    def fake_fred(series_id, start_date, end_date, api_key):
        return [{"date": (date.today() - timedelta(days=2)).isoformat(), "close": 4.10},
                {"date": (date.today() - timedelta(days=1)).isoformat(), "close": 4.20}]
    monkeypatch.setattr(appmod, "_fred_history", fake_fred)
    d = client.get(_ROUTE, headers=_AUTH, params={"key": "us10y", "window": "1Y"}).json()
    assert d["status"] == "ok"
    assert d["source"] == "fred"
    assert d["label"] == "US 10Y"
    assert d["symbol"] == "DGS10"
    assert d["available"] is True
    assert d["last_close"] == 4.20
    assert d["change"] is not None


def test_fred_key_missing_is_honest(client, monkeypatch):
    # No FRED key in the environment (fixture pops it) and the range helper returns nothing.
    monkeypatch.setattr(appmod, "_fred_history", lambda *a, **k: [])
    d = client.get(_ROUTE, headers=_AUTH, params={"key": "usd_broad", "window": "30D"}).json()
    assert d["status"] == "unavailable"
    assert d["available"] is False
    assert "FRED API key missing" in d["reason"]


# --------------------------------------------------------------------------- #
# Unsupported instrument + supported registry
# --------------------------------------------------------------------------- #
def test_unsupported_instrument(client):
    d = client.get(_ROUTE, headers=_AUTH, params={"key": "btc", "window": "30D"}).json()
    assert d["status"] == "unavailable"
    assert d["available"] is False
    assert "Unsupported" in d["reason"]


def test_all_market_context_instruments_supported():
    # Every Market Context tile instrument has a canonical history owner (yfinance or FRED).
    supported = set(appmod._MARKET_HISTORY_YF) | set(appmod._MARKET_HISTORY_FRED)
    tiles = {"sp500", "nasdaq", "dow", "vix", "eurusd", "gold", "wti", "brent",
             "us10y", "us2y", "usd_broad"}
    assert tiles <= supported


def test_route_registered_get_only():
    routes = [r for r in app.routes if getattr(r, "path", None) == _ROUTE]
    assert routes, "history route not registered"
    methods = set()
    for r in routes:
        methods |= (r.methods or set())
    assert "GET" in methods
    assert "POST" not in methods and "PUT" not in methods and "DELETE" not in methods
