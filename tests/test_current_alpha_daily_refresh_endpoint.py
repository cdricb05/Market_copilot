"""
tests/test_current_alpha_daily_refresh_endpoint.py — Phase 13-G/H daily-refresh routes.

Contract + behavioural tests for the two manual-daily routes:
    POST /v1/research/current-alpha/daily-refresh   (manual, user-triggered)
    GET  /v1/research/current-alpha/daily-status     (read-only status)

Fully offline: the subprocess launch is disabled via PAPER_TRADER_DAILY_REFRESH_LAUNCH=0
and a synthetic Phase 13-G mark artifact is pre-written, so no research runner, no
network, and no EODHD key are exercised. The routes read/write only the local paper
store + the local mark artifact.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paper_trader.api.app import app
from paper_trader.config import get_settings

from tests.test_current_alpha_operations import _write_ops_fixture
from tests.test_current_alpha_daily_refresh import _write_daily_artifact, _write_audit

_REPO_ROOT = Path(__file__).resolve().parents[1]
_APP_PATH = _REPO_ROOT / "api" / "app.py"

_REFRESH = "/v1/research/current-alpha/daily-refresh"
_STATUS = "/v1/research/current-alpha/daily-status"

_TEST_API_KEY = "current-alpha-daily-refresh-test-key"
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
def env(tmp_path: Path, monkeypatch) -> dict:
    pkg = _write_ops_fixture(tmp_path / "pkg")
    store = tmp_path / "store"
    marks = tmp_path / "marks"
    repo = tmp_path / "repo"
    _write_daily_artifact(marks)   # pre-write a fresh mark artifact (launch disabled)
    _write_audit(repo)
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR", str(pkg))
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_BOOK_DIR", str(store))
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_DAILY_MARK_DIR", str(marks))
    monkeypatch.setenv("PAPER_TRADER_RESEARCH_REPO_DIR", str(repo))
    monkeypatch.setenv("PAPER_TRADER_DAILY_REFRESH_LAUNCH", "0")   # skip the subprocess
    return {"pkg": pkg, "store": store, "marks": marks, "repo": repo}


# --------------------------------------------------------------------------- #
# Auth
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("method,endpoint", [("post", _REFRESH), ("get", _STATUS)])
def test_endpoints_require_api_key(client: TestClient, env: dict, method: str, endpoint: str):
    resp = getattr(client, method)(endpoint)
    assert resp.status_code in (401, 403)


# --------------------------------------------------------------------------- #
# POST daily-refresh
# --------------------------------------------------------------------------- #
def test_daily_refresh_snapshots_both_books(client: TestClient, env: dict):
    resp = client.post(_REFRESH, headers=_AUTH, json={"commit": True})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "DAILY_REFRESH_COMPLETE"
    assert body["mark_date"] == "2026-07-14"
    t25, t50 = body["snapshots"]["top25"], body["snapshots"]["top50"]
    assert t25["action"] == "SNAPSHOT_WRITTEN" and t50["action"] == "SNAPSHOT_WRITTEN"
    assert t25["book_id"].endswith("top25") and t50["book_id"].endswith("top50")
    assert t25["mark_source"] == "PHASE13G_DAILY_REFRESH"
    assert t25["average_return_pct"] == pytest.approx(5.0)
    # safety
    assert body["no_orders"] is True
    assert body["wrote_to_paper_trader"] is False
    assert body["order_action_all"] == "NO_ORDER"
    assert body["refresh"]["shell"] is False
    assert body["refresh"]["api_key_in_command_line"] is False


def test_daily_refresh_same_price_date_no_duplicate(client: TestClient, env: dict):
    client.post(_REFRESH, headers=_AUTH, json={"commit": True})
    again = client.post(_REFRESH, headers=_AUTH, json={"commit": True}).json()
    assert again["snapshots"]["top25"]["action"] == "SNAPSHOT_SKIPPED_NO_NEW_PRICE_DATE"
    assert again["snapshots"]["top25"]["n_snapshots_after"] == 1


def test_daily_refresh_preview_default(client: TestClient, env: dict):
    # no body -> preview (commit defaults False); no paper-store snapshot file written
    resp = client.post(_REFRESH, headers=_AUTH)
    assert resp.status_code == 200
    assert resp.json()["committed"] is False
    assert not (env["store"] / "pnl_snapshots.json").is_file()


# --------------------------------------------------------------------------- #
# GET daily-status
# --------------------------------------------------------------------------- #
def test_daily_status_benchmark_coverage_and_universe(client: TestClient, env: dict):
    resp = client.get(_STATUS, headers=_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "DAILY_STATUS_READY"
    assert body["latest_completed_eod_date"] == "2026-07-14"
    assert body["data_source"] == "PHASE13G_DAILY_REFRESH"
    # correct benchmark + coverage
    assert body["top25"]["coverage_status"] == "FULL_COVERAGE"
    assert body["spy_benchmark"]["return_since_signal_pct"] == pytest.approx(1.0)
    assert body["top25"]["excess_return_vs_spy_pct_points"] == pytest.approx(4.0)
    # universe identity: actual audited universe, NOT strict S&P 500
    ui = body["universe_identity"]
    assert ui["is_strict_sp500"] is False
    assert "phase8v" in ui["current_champion_universe"]
    assert ui["sp500_shadow_decision"] == "SP500_SHADOW_REJECTED_WEAKER"
    # safety
    assert body["no_orders"] is True
    assert body["wrote_to_paper_trader"] is False


def test_daily_status_after_refresh_shows_history(client: TestClient, env: dict):
    client.post(_REFRESH, headers=_AUTH, json={"commit": True})
    body = client.get(_STATUS, headers=_AUTH).json()
    assert body["top25_history"]["n_snapshots"] == 1
    assert body["top50_history"]["n_snapshots"] == 1
    assert body["top25_history"]["selected_book_id"] != body["top50_history"]["selected_book_id"]


# --------------------------------------------------------------------------- #
# Static route/handler guards
# --------------------------------------------------------------------------- #
def test_app_wires_daily_routes():
    src = _APP_PATH.read_text(encoding="utf-8")
    assert '@app.post(\n    "/v1/research/current-alpha/daily-refresh"' in src
    assert '@app.get(\n    "/v1/research/current-alpha/daily-status"' in src


def test_daily_refresh_handler_has_no_forbidden_surface():
    # scan the two handler bodies for order/DB tokens (docstrings are prose, so scan
    # the whole app slice around the routes for concrete dangerous CALL tokens).
    src = _APP_PATH.read_text(encoding="utf-8")
    i = src.index("/v1/research/current-alpha/daily-refresh")
    j = src.index("def research_current_alpha_daily_status")
    handler = src[i:j].lower()
    for needle in ("create_order", "place_order", ".add(", ".commit(", "insert into"):
        assert needle not in handler, f"forbidden token near daily-refresh handler: {needle!r}"
