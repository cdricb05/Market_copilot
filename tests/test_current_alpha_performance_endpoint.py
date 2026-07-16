"""
tests/test_current_alpha_performance_endpoint.py — Phase 13-I performance route.

Contract tests for the read-only route:
    GET /v1/research/current-alpha/performance

Fully offline: a synthetic Phase 13-I backfill artifact is written to a tmp dir and
PAPER_TRADER_CURRENT_ALPHA_BACKFILL_DIR points the loader at it — no research runner,
no network, no EODHD key, no database. Verifies auth, the controlled statuses, that
Top-25 and Top-50 stay separate, and that the response carries the frozen-holdings
safety block (no orders / no DB writes / promotes no book to live trading).
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paper_trader.api.app import app
from paper_trader.config import get_settings

from tests.test_current_alpha_performance import _write_backfill

_PERF = "/v1/research/current-alpha/performance"
_TEST_API_KEY = "current-alpha-performance-test-key"
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
    resp = client.get(_PERF)
    assert resp.status_code in (401, 403)


def test_no_backfill_yet_is_controlled_200(client: TestClient, bdir: Path):
    resp = client.get(_PERF, headers=_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "NO_BACKFILL_YET"
    assert body["no_orders"] is True
    assert body["wrote_to_paper_trader"] is False


def test_performance_ready(client: TestClient, bdir: Path):
    _write_backfill(bdir)
    resp = client.get(_PERF, headers=_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "PERFORMANCE_READY"
    assert body["backfill_decision"] == "BACKFILL_RECONCILED"
    assert body["reconciliation_status"] == "BACKFILL_RECONCILED"
    assert body["latest_mark_date"] == "2026-05-27"
    assert body["observation_count"] == 3
    # Top-25 and Top-50 are separate objects, never merged
    assert body["top25_analytics"]["book_id"].endswith("top25")
    assert body["top50_analytics"]["book_id"].endswith("top50")
    assert body["top25_analytics"]["book_id"] != body["top50_analytics"]["book_id"]
    # curves present and keyed by financial mark date
    assert body["top25_curves"]["n_marks"] == 3
    assert body["spy_curve"][-1]["mark_date"] == "2026-05-27"


def test_rejected_publishes_no_analytics(client: TestClient, bdir: Path):
    _write_backfill(bdir, decision="BACKFILL_REJECTED_INTEGRITY_FAILURE")
    resp = client.get(_PERF, headers=_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "BACKFILL_REJECTED"
    assert "top25_analytics" not in body
    assert body["reconciliation_status"]


def test_safety_flags_in_response(client: TestClient, bdir: Path):
    _write_backfill(bdir)
    body = client.get(_PERF, headers=_AUTH).json()
    assert body["frozen_holdings"] is True
    assert body["daily_rebalancing"] is False
    assert body["promotes_to_live"] is False
    assert body["order_action_all"] == "NO_ORDER"
    for badge in ("HISTORICAL PAPER MARK RECONSTRUCTION", "FROZEN HOLDINGS",
                  "NO DAILY REBALANCING", "DOES NOT EXECUTE TRADES"):
        assert badge in body["safety_badges"]


def test_route_is_get_only(client: TestClient, bdir: Path):
    # The performance route is read-only: POST is not allowed.
    resp = client.post(_PERF, headers=_AUTH, json={})
    assert resp.status_code == 405


def test_app_wires_performance_route():
    src = (Path(__file__).resolve().parents[1] / "api" / "app.py").read_text(encoding="utf-8")
    assert '@app.get(\n    "/v1/research/current-alpha/performance"' in src
