"""
tests/test_current_alpha_preview_endpoint.py — Phase 13-B endpoint.

Behavioural + contract tests for the read-only GET
``/v1/research/current-alpha/preview`` route added in api/app.py. The route
exposes the Phase 13-B ``load_current_alpha_preview`` payload behind the standard
X-API-Key auth used by every other protected v1 endpoint.

These tests are deliberately DB-free: the endpoint reads only the local Phase
13-A package (pointed at a self-contained tmp fixture via the
``PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR`` env var), so the suite runs without
``PAPER_TRADER_TEST_DATABASE_URL``. Static source-scan guards assert the route
stays read-only: no DB write, no order/signal/trade-decision call, no
automation, no prediction-service/provider call, and no UI modification.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paper_trader.api.app import app
from paper_trader.config import get_settings

# Reuse the Phase 13-B fixture-package builder so the two suites cannot drift.
from tests.test_current_alpha_preview import _write_fixture_package

_REPO_ROOT = Path(__file__).resolve().parents[1]
_APP_PATH = _REPO_ROOT / "api" / "app.py"
_UI_PATH = _REPO_ROOT / "api" / "ui" / "index.html"

_ENDPOINT = "/v1/research/current-alpha/preview"
_TEST_API_KEY = "current-alpha-endpoint-test-key"
_AUTH = {"X-API-Key": _TEST_API_KEY}


# ---------------------------------------------------------------------------
# Fixtures (no database required)
# ---------------------------------------------------------------------------

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
def valid_package(tmp_path: Path, monkeypatch) -> Path:
    pkg = _write_fixture_package(tmp_path / "pkg")
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR", str(pkg))
    return pkg


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def test_endpoint_requires_api_key(client: TestClient, valid_package: Path) -> None:
    resp = client.get(_ENDPOINT)
    assert resp.status_code in (401, 403)
    assert resp.status_code != 200


def test_endpoint_rejects_wrong_api_key(client: TestClient, valid_package: Path) -> None:
    resp = client.get(_ENDPOINT, headers={"X-API-Key": "wrong-key"})
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_endpoint_returns_200_with_payload(client: TestClient, valid_package: Path) -> None:
    resp = client.get(_ENDPOINT, headers=_AUTH)
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)


def test_endpoint_payload_core_fields(client: TestClient, valid_package: Path) -> None:
    body = client.get(_ENDPOINT, headers=_AUTH).json()
    assert body["alpha_name"] == "composite_sn"
    assert body["decision"] == "CURRENT_ALPHA_READY_FOR_PAPER_TEST"
    assert body["go_no_go"] == "GO_PAPER_ONLY_WITH_CAVEATS_NOT_LIVE"
    assert body["signal_date"] == "2026-05-22"
    assert body["cross_section_month"] == "2026-05"
    assert body["n_ranked"] == 234


def test_endpoint_payload_top_level_safety_flags(client: TestClient, valid_package: Path) -> None:
    body = client.get(_ENDPOINT, headers=_AUTH).json()
    for flag in ("preview_only", "paper_test_only", "manual_review_only",
                 "no_orders", "no_broker", "no_automation", "read_only"):
        assert body.get(flag) is True, f"missing/false safety guarantee: {flag}"
    for flag in ("creates_signals", "creates_trade_decisions",
                 "wrote_to_paper_trader", "calls_prediction_service",
                 "calls_external_providers", "live_trading"):
        assert body.get(flag) is False, f"action flag must be False: {flag}"


def test_endpoint_payload_includes_six_safety_badges(client: TestClient, valid_package: Path) -> None:
    body = client.get(_ENDPOINT, headers=_AUTH).json()
    badges = body["safety_badges"]
    assert isinstance(badges, list)
    for badge in ("PREVIEW ONLY", "NO ORDERS", "NO BROKER", "NO AUTOMATION",
                  "MANUAL REVIEW ONLY", "PAPER TEST ONLY"):
        assert badge in badges, f"missing safety badge: {badge}"


def test_endpoint_payload_includes_top_candidates(client: TestClient, valid_package: Path) -> None:
    body = client.get(_ENDPOINT, headers=_AUTH).json()
    assert body["top25_candidates"] and isinstance(body["top25_candidates"], list)
    assert body["top50_candidates"] and isinstance(body["top50_candidates"], list)
    assert body["top10_tickers"][0] == "EXPE"
    assert body["top25_candidates"][0]["ticker"] == "EXPE"


def test_endpoint_payload_includes_books_and_provenance(client: TestClient, valid_package: Path) -> None:
    body = client.get(_ENDPOINT, headers=_AUTH).json()
    for key in ("bottom25_avoid", "sector_exposure", "risk_limits",
                "go_no_go_scorecard", "caveats", "source_file_paths"):
        assert key in body, f"missing payload section: {key}"
        assert isinstance(body[key], list)
    assert body["caveats"], "caveats should be non-empty for the fixture package"


# ---------------------------------------------------------------------------
# Error mapping — missing package returns a controlled 503
# ---------------------------------------------------------------------------

def test_missing_package_maps_to_503(client: TestClient, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv(
        "PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR", str(tmp_path / "does_not_exist")
    )
    resp = client.get(_ENDPOINT, headers=_AUTH)
    assert resp.status_code == 503
    detail = resp.json()["detail"]
    assert "Current alpha preview unavailable" in detail
    assert "Phase 13-A package not found" in detail
    # No stack trace leaks to the client.
    assert "Traceback" not in detail
    assert 'File "' not in detail


# ---------------------------------------------------------------------------
# Static read-only / safety guards on the route handler in app.py
# ---------------------------------------------------------------------------

def _handler_source() -> str:
    """Extract the executable body of research_current_alpha_preview (no docstring)."""
    src = _APP_PATH.read_text(encoding="utf-8")
    start = src.index("def research_current_alpha_preview(")
    tail = src[start:]
    m = re.search(r"\n(?:@app\.|def )", tail[1:])
    end = (m.start() + 1) if m else len(tail)
    func = tail[:end]
    doc = re.search(r'"""', func)
    if doc:
        close = func.index('"""', doc.end())
        func = func[: doc.start()] + func[close + 3:]
    return func


def test_endpoint_exists_in_app_source() -> None:
    src = _APP_PATH.read_text(encoding="utf-8")
    assert _ENDPOINT in src
    assert "load_current_alpha_preview" in src
    assert f'@app.get(\n    "{_ENDPOINT}"' in src
    assert f'@app.post(\n    "{_ENDPOINT}"' not in src


def test_handler_is_read_only_no_db_write() -> None:
    handler = _handler_source()
    for needle in (".commit(", ".add(", "INSERT INTO", "UPDATE ", "DELETE FROM",
                   "get_session", "get_dedicated_session", "db.add", "session.add"):
        assert needle not in handler, f"read-only handler uses DB write token: {needle!r}"


def test_handler_creates_no_orders_signals_or_decisions() -> None:
    handler = _handler_source().lower()
    for needle in ("order", "signal", "tradedecision", "trade_decision",
                   "create_decision", "run_decision", "fill_order"):
        assert needle not in handler, f"handler references forbidden action: {needle!r}"


def test_handler_has_no_automation_or_prediction_or_provider_call() -> None:
    handler = _handler_source().lower()
    for needle in ("automation", "fetch_predictions", "prediction_client",
                   "fetch_latest_prices", "fetch_historical_prices", "schedule",
                   "nasdaq", "intrinio", "fmp", "requests.get", "httpx"):
        assert needle not in handler, f"handler references forbidden call: {needle!r}"


def test_handler_does_not_touch_ui() -> None:
    handler = _handler_source().lower()
    assert "index.html" not in handler
    assert "/ui/" not in handler
    assert _UI_PATH.is_file()
