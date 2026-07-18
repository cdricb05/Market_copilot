"""
tests/test_current_alpha_integrity_gate_endpoint.py — Phase 16-A integrity-gate route.

Contract tests for the read-only route:
    GET /v1/research/current-alpha/integrity-gate

Fully offline: a synthetic Phase 16-A artifact dir is written to tmp and
PAPER_TRADER_CURRENT_ALPHA_INTEGRITY_DIR points the loader at it. The composed decision-gate /
daily-status / operating-state dependencies degrade to controlled states without a backfill dir or a
live database. Verifies auth, HTTP 200 controlled status, that the response is GET-only, carries the
no-live-trading safety block, and adds no write route.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paper_trader.api.app import app
from paper_trader.config import get_settings

from tests.test_current_alpha_integrity_gate import _write_artifacts

_GATE = "/v1/research/current-alpha/integrity-gate"
_TEST_API_KEY = "current-alpha-integrity-gate-test-key"
_AUTH = {"X-API-Key": _TEST_API_KEY}


@pytest.fixture
def client(monkeypatch, tmp_path) -> TestClient:
    monkeypatch.setenv("PAPER_TRADER_SERVICE_API_KEY", _TEST_API_KEY)
    monkeypatch.setenv(
        "PAPER_TRADER_DATABASE_URL",
        "postgresql+psycopg2://unused:unused@localhost:5432/unused",
    )
    idir = tmp_path / "phase16a"
    _write_artifacts(idir)
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_INTEGRITY_DIR", str(idir))
    # Keep the daily-refresh subprocess disabled and give a nonexistent backfill dir so the
    # composed loaders degrade cleanly (no DB / no network).
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_BACKFILL_DIR", str(tmp_path / "no_backfill"))
    get_settings.cache_clear()
    with TestClient(app) as test_client:
        yield test_client


def test_requires_api_key(client: TestClient):
    assert client.get(_GATE).status_code in (401, 403)


def test_returns_controlled_200(client: TestClient):
    resp = client.get(_GATE, headers=_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["phase"] == "16-A"
    assert body["champion"] == "composite_sn"
    assert body["status"] in ("PAPER_TEST_CONTINUE", "PAPER_TEST_CHECKPOINT_DUE",
                              "RESEARCH_REVALIDATION_REQUIRED", "DATA_INTEGRITY_BLOCKED")


def test_surfaces_sector_shadow_decision(client: TestClient):
    body = client.get(_GATE, headers=_AUTH).json()
    # synthetic shadow artifact carries RESEARCH_REVALIDATION_REQUIRED
    assert body["sector_shadow_decision"] == "RESEARCH_REVALIDATION_REQUIRED"
    assert body["status"] == "RESEARCH_REVALIDATION_REQUIRED"
    m = body["rank_correlation_and_overlap_metrics"]
    assert "full_panel_rank_spearman" in m and "top25_overlap" in m


def test_no_live_trading_and_read_only(client: TestClient):
    body = client.get(_GATE, headers=_AUTH).json()
    assert body["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"
    assert body["status_approves_live_trading"] is False
    assert body["no_live_trading"] is True
    assert body["read_only"] is True and body["creates_orders"] is False


def test_route_is_get_only(client: TestClient):
    # POST / PUT / DELETE are not allowed on the read-only gate.
    assert client.post(_GATE, headers=_AUTH).status_code == 405
    assert client.put(_GATE, headers=_AUTH).status_code == 405
    assert client.delete(_GATE, headers=_AUTH).status_code == 405


def test_coverage_kept_separate(client: TestClient):
    body = client.get(_GATE, headers=_AUTH).json()
    assert body["current_daily_mark_coverage"]["label"] == "CURRENT DAILY-MARK COVERAGE"
    assert body["initial_entry_price_coverage"]["label"] == "INITIAL ENTRY-PRICE COVERAGE"


def test_no_new_write_route_added():
    """The only new route for this phase is a read-only GET."""
    app_src = (Path(__file__).parent.parent / "api" / "app.py").read_text(encoding="utf-8")
    assert '"/v1/research/current-alpha/integrity-gate"' in app_src
    seg = app_src[app_src.index('"/v1/research/current-alpha/integrity-gate"') - 220:
                  app_src.index('"/v1/research/current-alpha/integrity-gate"')]
    assert "@app.get(" in seg
    # no @app.post/put/delete decorates the integrity-gate handler
    fn_idx = app_src.index("def research_current_alpha_integrity_gate")
    header = app_src[fn_idx - 320:fn_idx]
    assert "@app.post(" not in header and "@app.put(" not in header and "@app.delete(" not in header
