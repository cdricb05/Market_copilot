"""
tests/test_research_candidate_preview_endpoint.py — Phase 4-E endpoint.

Behavioural + contract tests for the read-only GET
``/v1/research/candidate-preview`` route added in api/app.py. The route exposes
the Phase 4-D ``load_candidate_preview`` payload behind the standard X-API-Key
auth used by every other protected v1 endpoint.

These tests are deliberately DB-free: the endpoint reads only the local Phase
4-B candidate package (pointed at a self-contained tmp fixture via the
``PAPER_TRADER_CANDIDATE_PACKAGE_DIR`` env var), so the suite runs without
``PAPER_TRADER_TEST_DATABASE_URL``. A set of static source-scan guards assert
the route stays read-only: no DB write, no order/signal/trade-decision call, no
automation, no prediction-service call, and no UI modification.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paper_trader.api.app import app
from paper_trader.config import get_settings

# Reuse the Phase 4-D fixture-package builder so the two suites cannot drift.
from tests.test_research_candidate_preview import _write_fixture_package

_REPO_ROOT = Path(__file__).resolve().parents[1]
_APP_PATH = _REPO_ROOT / "api" / "app.py"
_UI_PATH = _REPO_ROOT / "api" / "ui" / "index.html"

_ENDPOINT = "/v1/research/candidate-preview"
_TEST_API_KEY = "endpoint-test-key"
_AUTH = {"X-API-Key": _TEST_API_KEY}


# ---------------------------------------------------------------------------
# Fixtures (no database required)
# ---------------------------------------------------------------------------

@pytest.fixture
def client(monkeypatch) -> TestClient:
    """A TestClient with auth configured but no database wiring.

    The candidate-preview route performs no database access, so we only need a
    valid service API key. ``conftest`` clears the settings lru_cache around
    every test, and we clear it again here after setting the key so the app
    picks it up.
    """
    monkeypatch.setenv("PAPER_TRADER_SERVICE_API_KEY", _TEST_API_KEY)
    # A syntactically-valid DSN so Settings validates; the route never connects.
    monkeypatch.setenv(
        "PAPER_TRADER_DATABASE_URL",
        "postgresql+psycopg2://unused:unused@localhost:5432/unused",
    )
    get_settings.cache_clear()
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def valid_package(tmp_path: Path, monkeypatch) -> Path:
    """Point the loader at a valid, self-contained Phase 4-B fixture package."""
    pkg = _write_fixture_package(tmp_path / "pkg")
    monkeypatch.setenv("PAPER_TRADER_CANDIDATE_PACKAGE_DIR", str(pkg))
    return pkg


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def test_endpoint_requires_api_key(client: TestClient, valid_package: Path) -> None:
    """Without a key the endpoint refuses (no payload leaks)."""
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
    body = resp.json()
    assert isinstance(body, dict)


def test_endpoint_payload_core_identity_fields(client: TestClient, valid_package: Path) -> None:
    body = client.get(_ENDPOINT, headers=_AUTH).json()
    assert body["candidate_id"] == "NPC-RIDGE-CRI-126D-TOP10EW-25BPS"
    assert body["model_name"] == "ridge_combined_regime_interactions"
    assert body["horizon"] == "126d"
    assert body["strategy_name"] == "top_10_equal_weight"


def test_endpoint_payload_top_level_safety_flags(client: TestClient, valid_package: Path) -> None:
    body = client.get(_ENDPOINT, headers=_AUTH).json()
    for flag in (
        "preview_only",
        "no_orders",
        "no_automation",
        "no_live_portfolio_weights",
        "manual_review_required",
    ):
        assert body.get(flag) is True, f"missing/false top-level safety flag: {flag}"


def test_endpoint_payload_includes_safety_badges(client: TestClient, valid_package: Path) -> None:
    body = client.get(_ENDPOINT, headers=_AUTH).json()
    badges = body["safety_badges"]
    assert isinstance(badges, list) and badges
    for badge in ("PREVIEW ONLY", "NO ORDERS", "NO AUTOMATION", "MANUAL REVIEW REQUIRED"):
        assert badge in badges, f"missing safety badge: {badge}"


def test_endpoint_payload_includes_no_go_items(client: TestClient, valid_package: Path) -> None:
    body = client.get(_ENDPOINT, headers=_AUTH).json()
    no_go = body["no_go_items"]
    assert isinstance(no_go, list) and no_go, "no_go_items must be a non-empty list"


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------

def test_candidate_preview_error_maps_to_503(client: TestClient, tmp_path: Path, monkeypatch) -> None:
    """A missing/invalid package yields a controlled 503 with a clear detail."""
    monkeypatch.setenv(
        "PAPER_TRADER_CANDIDATE_PACKAGE_DIR", str(tmp_path / "does_not_exist")
    )
    resp = client.get(_ENDPOINT, headers=_AUTH)
    assert resp.status_code == 503
    detail = resp.json()["detail"]
    assert "Candidate preview unavailable" in detail
    # No stack trace is leaked to the client.
    assert "Traceback" not in detail
    assert 'File "' not in detail


# ---------------------------------------------------------------------------
# Static read-only / safety guards on the route handler in app.py
# ---------------------------------------------------------------------------

def _handler_source() -> str:
    """Extract the *executable* body of the research_candidate_preview handler.

    The docstring is stripped so that prose describing what the route must NOT
    do (e.g. "creates no orders") does not trip the forbidden-token scans below
    — only the actual code is inspected.
    """
    src = _APP_PATH.read_text(encoding="utf-8")
    start = src.index("def research_candidate_preview(")
    tail = src[start:]
    m = re.search(r"\n(?:@app\.|def )", tail[1:])
    end = (m.start() + 1) if m else len(tail)
    func = tail[:end]
    # Drop the triple-quoted docstring if present.
    doc = re.search(r'"""', func)
    if doc:
        close = func.index('"""', doc.end())
        func = func[: doc.start()] + func[close + 3:]
    return func


def test_endpoint_exists_in_app_source() -> None:
    src = _APP_PATH.read_text(encoding="utf-8")
    assert _ENDPOINT in src
    assert "load_candidate_preview" in src
    # Declared as a GET (read-only), never a POST.
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


def test_handler_has_no_automation_or_prediction_call() -> None:
    handler = _handler_source().lower()
    for needle in ("automation", "fetch_predictions", "prediction_client",
                   "fetch_latest_prices", "fetch_historical_prices", "schedule"):
        assert needle not in handler, f"handler references forbidden call: {needle!r}"


def test_handler_does_not_touch_ui() -> None:
    handler = _handler_source().lower()
    assert "index.html" not in handler
    assert "/ui/" not in handler
    # The UI file is present and is not referenced by this route.
    assert _UI_PATH.is_file()
