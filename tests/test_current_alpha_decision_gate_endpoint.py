"""
tests/test_current_alpha_decision_gate_endpoint.py — Phase 13-J decision-gate route.

Contract tests for the read-only route:
    GET /v1/research/current-alpha/decision-gate

Fully offline: a synthetic Phase 13-I backfill artifact is written to a tmp dir and
PAPER_TRADER_CURRENT_ALPHA_BACKFILL_DIR points the loader at it — no research runner,
no network, no EODHD key, no database. Verifies auth, the controlled statuses, that
Top-25 and Top-50 stay separate, that the response carries the provisional-book /
not-live-approval safety block, and that the route is GET-only.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paper_trader.api.app import app
from paper_trader.config import get_settings

from tests.test_current_alpha_decision_gate import _write_backfill

_GATE = "/v1/research/current-alpha/decision-gate"
_TEST_API_KEY = "current-alpha-decision-gate-test-key"
_AUTH = {"X-API-Key": _TEST_API_KEY}


@pytest.fixture
def client(monkeypatch) -> TestClient:
    monkeypatch.setenv("PAPER_TRADER_SERVICE_API_KEY", _TEST_API_KEY)
    monkeypatch.setenv(
        "PAPER_TRADER_DATABASE_URL",
        "postgresql+psycopg2://unused:unused@localhost:5432/unused",
    )
    get_settings.cache_clear()
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def bdir(tmp_path, monkeypatch) -> Path:
    d = tmp_path / "backfill"
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_BACKFILL_DIR", str(d))
    return d


def test_requires_api_key(client: TestClient, bdir: Path):
    resp = client.get(_GATE)
    assert resp.status_code in (401, 403)


def test_no_backfill_yet_is_controlled_200(client: TestClient, bdir: Path):
    resp = client.get(_GATE, headers=_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "NO_BACKFILL_YET"
    assert body["decision"] == "INSUFFICIENT_FORWARD_HISTORY"
    assert body["no_orders"] is True
    assert body["wrote_to_paper_trader"] is False


def test_decision_ready(client: TestClient, bdir: Path):
    # The endpoint uses the real current date for mark-freshness, so end the artifact
    # today -> a fresh mark (no stale-mark risk trigger), independent of run date.
    _write_backfill(bdir, end=date.today().isoformat())
    resp = client.get(_GATE, headers=_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "DECISION_READY"
    assert body["decision"] == "PROVISIONAL_TOP50_PRIMARY"
    assert body["book_role_status"] == "PROVISIONAL_PRIMARY_TOP50"
    # Top-25 and Top-50 are separate scorecards, never merged.
    assert body["top25"]["book_id"].endswith("top25")
    assert body["top50"]["book_id"].endswith("top50")
    assert body["top25"]["book_id"] != body["top50"]["book_id"]
    assert body["primary_paper_book"]["book"] == "TOP50"
    assert body["challenger_paper_book"]["label"] == "TOP25"
    assert body["quarterly_rebalance_readiness"]["target_holding_period_trading_days"] == 63


def test_rejected_publishes_no_decision(client: TestClient, bdir: Path):
    _write_backfill(bdir, decision="BACKFILL_REJECTED_INTEGRITY_FAILURE")
    resp = client.get(_GATE, headers=_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "BACKFILL_NOT_PUBLISHED"
    assert body["decision"] == "INSUFFICIENT_FORWARD_HISTORY"
    assert body["primary_paper_book"]["status"] == "NO_PRIMARY_BOOK_YET"


def test_safety_flags_in_response(client: TestClient, bdir: Path):
    _write_backfill(bdir)
    body = client.get(_GATE, headers=_AUTH).json()
    assert body["promotes_to_live"] is False
    assert body["is_live_trading_approval"] is False
    assert body["daily_rebalancing"] is False
    assert body["reranking"] is False
    assert body["order_action_all"] == "NO_ORDER"
    for badge in ("PROVISIONAL PAPER BOOK ONLY", "NOT LIVE-TRADING APPROVAL",
                  "NO ORDERS", "NO BROKER", "NO AUTOMATION", "MANUAL REVIEW REQUIRED"):
        assert badge in body["safety_badges"]


def test_route_is_get_only(client: TestClient, bdir: Path):
    resp = client.post(_GATE, headers=_AUTH, json={})
    assert resp.status_code == 405


def test_app_wires_decision_gate_route():
    src = (Path(__file__).resolve().parents[1] / "api" / "app.py").read_text(encoding="utf-8")
    assert '@app.get(\n    "/v1/research/current-alpha/decision-gate"' in src
