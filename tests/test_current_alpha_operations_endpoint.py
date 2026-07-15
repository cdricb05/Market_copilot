"""
tests/test_current_alpha_operations_endpoint.py — Phase 13-C/D/E endpoints.

Behavioural + contract tests for the three read-only GET routes added in
api/app.py:
    GET /v1/research/current-alpha/pnl                  (Phase 13-C)
    GET /v1/research/current-alpha/actions-preview      (Phase 13-D)
    GET /v1/research/current-alpha/rebalance-simulator  (Phase 13-E)

DB-free: each route reads only the local Phase 13-A package (and, for the
simulator, a frozen panel), pointed at self-contained tmp fixtures via the
``PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR`` / ``PAPER_TRADER_CURRENT_ALPHA_PANEL_PATH``
env vars. Static handler guards assert each route stays read-only and paper-only.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paper_trader.api.app import app
from paper_trader.config import get_settings

from tests.test_current_alpha_operations import _write_ops_fixture, _panel_csv

_REPO_ROOT = Path(__file__).resolve().parents[1]
_APP_PATH = _REPO_ROOT / "api" / "app.py"

_PNL = "/v1/research/current-alpha/pnl"
_ACTIONS = "/v1/research/current-alpha/actions-preview"
_SIM = "/v1/research/current-alpha/rebalance-simulator"
_TEST_API_KEY = "current-alpha-ops-endpoint-test-key"
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
def valid_env(tmp_path: Path, monkeypatch) -> Path:
    pkg = _write_ops_fixture(tmp_path / "pkg")
    panel = tmp_path / "panel.csv"
    panel.write_text(_panel_csv(), encoding="utf-8")
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR", str(pkg))
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_PANEL_PATH", str(panel))
    return pkg


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("endpoint", [_PNL, _ACTIONS, _SIM])
def test_endpoints_require_api_key(client: TestClient, valid_env: Path, endpoint: str) -> None:
    assert client.get(endpoint).status_code in (401, 403)


@pytest.mark.parametrize("endpoint", [_PNL, _ACTIONS, _SIM])
def test_endpoints_reject_wrong_key(client: TestClient, valid_env: Path, endpoint: str) -> None:
    assert client.get(endpoint, headers={"X-API-Key": "wrong"}).status_code == 401


# ---------------------------------------------------------------------------
# PnL (13-C)
# ---------------------------------------------------------------------------

def test_pnl_200_and_math(client: TestClient, valid_env: Path) -> None:
    body = client.get(_PNL, headers=_AUTH).json()
    assert body["alpha_name"] == "composite_sn"
    assert body["signal_date"] == "2026-05-22"
    assert body["top25"]["covered_count"] == 3
    assert body["top25"]["missing_count"] == 1
    assert body["top25"]["average_paper_return_pct"] == pytest.approx(4.0)
    assert body["top25"]["best_performers"][0]["ticker"] == "AAA"
    assert body["top25"]["worst_performers"][0]["ticker"] == "BBB"


def test_pnl_badges_and_flags(client: TestClient, valid_env: Path) -> None:
    body = client.get(_PNL, headers=_AUTH).json()
    for badge in ("PREVIEW ONLY", "NO ORDERS", "NO BROKER", "NO AUTOMATION",
                  "MANUAL REVIEW ONLY", "PAPER TEST ONLY"):
        assert badge in body["safety_badges"]
    assert body["no_orders"] is True
    assert body["wrote_to_paper_trader"] is False
    assert body["calls_prediction_service"] is False
    assert body["calls_external_providers"] is False


def test_pnl_checkpoints(client: TestClient, valid_env: Path) -> None:
    body = client.get(_PNL, headers=_AUTH).json()
    labels = [c["label"] for c in body["checkpoint_plan"]]
    assert labels == ["1 week", "1 month", "2 months", "63 trading days"]


# ---------------------------------------------------------------------------
# Actions (13-D)
# ---------------------------------------------------------------------------

def test_actions_200_plans_and_no_order(client: TestClient, valid_env: Path) -> None:
    body = client.get(_ACTIONS, headers=_AUTH).json()
    assert body["counts_by_action_type"]["ADD_PREVIEW"] == 3
    assert body["counts_by_action_type"]["WAIT_FOR_PRICE_PREVIEW"] == 1
    assert body["counts_by_action_type"]["AVOID_PREVIEW"] == 1
    rows = body["top25_action_plan"] + body["top50_action_plan"] + body["avoid_list"]
    assert rows and all(r["order_action"] == "NO_ORDER" for r in rows)


def test_actions_wait_and_avoid_mapping(client: TestClient, valid_env: Path) -> None:
    body = client.get(_ACTIONS, headers=_AUTH).json()
    waits = [r["ticker"] for r in body["top25_action_plan"] if r["action_type"] == "WAIT_FOR_PRICE_PREVIEW"]
    assert waits == ["CCC"]
    assert all(r["action_type"] == "AVOID_PREVIEW" for r in body["avoid_list"])


def test_actions_explicit_notice(client: TestClient, valid_env: Path) -> None:
    body = client.get(_ACTIONS, headers=_AUTH).json()
    assert body["order_action_all"] == "NO_ORDER"
    assert "No order is created" in body["explicit_notice"]
    assert "Manual review required" in body["explicit_notice"]


# ---------------------------------------------------------------------------
# Rebalance simulator (13-E)
# ---------------------------------------------------------------------------

def test_simulator_200_quarterly_and_daily_rejected(client: TestClient, valid_env: Path) -> None:
    body = client.get(_SIM, headers=_AUTH).json()
    assert body["simulation_status"] == "SIMULATED"
    assert body["recommendation"] == "QUARTERLY_REBALANCE_CANDIDATE"
    assert body["frequencies"]["daily"]["verdict"] == "DAILY_REBALANCE_REJECTED"
    assert body["frequencies"]["quarterly"]["top25"]["n_periods"] == 4
    assert body["daily_trading_recommended"] is False
    assert body["daily_monitoring_valid"] is True


def test_simulator_missing_panel_controlled(client: TestClient, tmp_path: Path, monkeypatch) -> None:
    pkg = _write_ops_fixture(tmp_path / "pkg")
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR", str(pkg))
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_PANEL_PATH", str(tmp_path / "absent.csv"))
    resp = client.get(_SIM, headers=_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["simulation_status"] == "SIMULATION_INSUFFICIENT_DATA"
    assert body["warnings"]


# ---------------------------------------------------------------------------
# Missing package -> controlled 503 (all three)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("endpoint,needle", [
    (_PNL, "Current alpha PnL unavailable"),
    (_ACTIONS, "Current alpha action preview unavailable"),
    (_SIM, "Current alpha rebalance simulation unavailable"),
])
def test_missing_package_maps_to_503(client: TestClient, tmp_path: Path, monkeypatch,
                                     endpoint: str, needle: str) -> None:
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR", str(tmp_path / "does_not_exist"))
    resp = client.get(endpoint, headers=_AUTH)
    assert resp.status_code == 503
    detail = resp.json()["detail"]
    assert needle in detail
    assert "Phase 13-A package not found" in detail
    assert "Traceback" not in detail and 'File "' not in detail


# ---------------------------------------------------------------------------
# Static handler guards
# ---------------------------------------------------------------------------

def _handler_source(func_name: str) -> str:
    src = _APP_PATH.read_text(encoding="utf-8")
    start = src.index(f"def {func_name}(")
    tail = src[start:]
    m = re.search(r"\n(?:@app\.|def )", tail[1:])
    end = (m.start() + 1) if m else len(tail)
    func = tail[:end]
    doc = re.search(r'"""', func)
    if doc:
        close = func.index('"""', doc.end())
        func = func[: doc.start()] + func[close + 3:]
    return func


@pytest.mark.parametrize("func", [
    "research_current_alpha_pnl",
    "research_current_alpha_actions_preview",
    "research_current_alpha_rebalance_simulator",
])
def test_handlers_read_only_no_db_or_action(func: str) -> None:
    handler = _handler_source(func)
    low = handler.lower()
    for needle in (".commit(", ".add(", "insert into", "update ", "delete from",
                   "get_session", "session.add"):
        assert needle not in low, f"{func} uses DB-write token: {needle!r}"
    for needle in ("order", "signal", "tradedecision", "trade_decision",
                   "automation", "prediction_client", "fetch_predictions",
                   "nasdaq", "intrinio", "fmp", "requests.get", "httpx"):
        assert needle not in low, f"{func} references forbidden token: {needle!r}"


@pytest.mark.parametrize("endpoint", [_PNL, _ACTIONS, _SIM])
def test_routes_are_get_not_post(endpoint: str) -> None:
    src = _APP_PATH.read_text(encoding="utf-8")
    assert f'@app.get(\n    "{endpoint}"' in src
    assert f'@app.post(\n    "{endpoint}"' not in src
