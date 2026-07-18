"""
tests/test_current_alpha_revalidation_endpoint.py — Phase 17 revalidation route.

Contract tests for the read-only route:
    GET /v1/research/current-alpha/revalidation

Fully offline: synthetic Phase 17-A + 17-B artifact dirs are written to tmp and the loader is pointed at
them via PAPER_TRADER_CURRENT_ALPHA_REVALIDATION_DIR / _CHALLENGER_DIR. The composed daily-status
dependency degrades to a controlled state without a live database. Verifies auth, HTTP 200 controlled
decision, that the response is GET-only, carries the no-live-trading block, never replaces the champion,
and adds no write route.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paper_trader.api.app import app
from paper_trader.config import get_settings

from tests.test_current_alpha_revalidation import _write_reval_artifacts

_ROUTE = "/v1/research/current-alpha/revalidation"
_TEST_API_KEY = "current-alpha-revalidation-test-key"
_AUTH = {"X-API-Key": _TEST_API_KEY}


@pytest.fixture
def client(monkeypatch, tmp_path) -> TestClient:
    monkeypatch.setenv("PAPER_TRADER_SERVICE_API_KEY", _TEST_API_KEY)
    monkeypatch.setenv(
        "PAPER_TRADER_DATABASE_URL",
        "postgresql+psycopg2://unused:unused@localhost:5432/unused",
    )
    reval = tmp_path / "reval"
    challenger = tmp_path / "challenger"
    _write_reval_artifacts(reval, challenger, decision="PAPER_CHALLENGER_ELIGIBLE", created=True)
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_REVALIDATION_DIR", str(reval))
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_CHALLENGER_DIR", str(challenger))
    # keep the composed daily-status offline (nonexistent backfill dir -> controlled degrade)
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_BACKFILL_DIR", str(tmp_path / "no_backfill"))
    get_settings.cache_clear()
    with TestClient(app) as test_client:
        yield test_client


def test_requires_api_key(client: TestClient):
    assert client.get(_ROUTE).status_code in (401, 403)


def test_returns_controlled_200(client: TestClient):
    resp = client.get(_ROUTE, headers=_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["phase"] == "17"
    assert body["decision"] == "PAPER_CHALLENGER_ELIGIBLE"
    assert body["current_paper_champion"]["signal"] == "composite_sn"
    assert body["sector_repaired_candidate"]["signal"] == "composite_sn_repaired"


def test_surfaces_side_by_side_and_challenger(client: TestClient):
    body = client.get(_ROUTE, headers=_AUTH).json()
    m = body["original_vs_repaired_metrics"]
    assert "ic_t_stat" in m and "net25_spread" in m
    assert m["ic_t_stat"]["champion"] is not None and m["ic_t_stat"]["repaired_candidate"] is not None
    assert body["challenger_package"]["created"] is True
    assert body["top_book_overlap"]["top25_overlap"] is not None


def test_no_live_trading_and_read_only(client: TestClient):
    body = client.get(_ROUTE, headers=_AUTH).json()
    assert body["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"
    assert body["no_decision_approves_live_trading"] is True
    assert body["decision_approves_live_trading"] is False
    assert body["read_only"] is True and body["creates_orders"] is False
    assert body["champion_replaced"] is False and body["replaces_champion"] is False


def test_route_is_get_only(client: TestClient):
    assert client.post(_ROUTE, headers=_AUTH).status_code == 405
    assert client.put(_ROUTE, headers=_AUTH).status_code == 405
    assert client.delete(_ROUTE, headers=_AUTH).status_code == 405


def test_no_new_write_route_added():
    """The only new route for this phase is a read-only GET."""
    app_src = (Path(__file__).parent.parent / "api" / "app.py").read_text(encoding="utf-8")
    assert '"/v1/research/current-alpha/revalidation"' in app_src
    seg = app_src[app_src.index('"/v1/research/current-alpha/revalidation"') - 220:
                  app_src.index('"/v1/research/current-alpha/revalidation"')]
    assert "@app.get(" in seg
    fn_idx = app_src.index("def research_current_alpha_revalidation")
    header = app_src[fn_idx - 320:fn_idx]
    assert "@app.post(" not in header and "@app.put(" not in header and "@app.delete(" not in header
