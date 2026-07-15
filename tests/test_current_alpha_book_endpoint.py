"""
tests/test_current_alpha_book_endpoint.py — Phase 13-F paper-book routes.

Behavioural + contract tests for the four current-alpha paper-book routes:
    GET  /v1/research/current-alpha/book                  (read persisted book)
    POST /v1/research/current-alpha/book/preview-create   (preview / save)
    GET  /v1/research/current-alpha/book/pnl-history       (PnL over time)
    POST /v1/research/current-alpha/book/snapshot-preview  (daily PnL snapshot)

DB-free: each route reads/writes only a local JSON paper-tracking store pointed
at a tmp dir via PAPER_TRADER_CURRENT_ALPHA_BOOK_DIR, and reads the Phase 13-A
package via PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR. Static handler guards assert
the routes create no orders / signals / decisions and touch no DB / broker /
provider; only the two POST routes write, and only to the local JSON store.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paper_trader.api.app import app
from paper_trader.config import get_settings

from tests.test_current_alpha_operations import (
    _PORTFOLIO_COLS,
    _prow,
    _write_ops_fixture,
)


def _distinct_top50_csv() -> str:
    """A TOP 50 portfolio with tickers disjoint from the TOP 25 AAA..DDD set."""
    header = ",".join(_PORTFOLIO_COLS)
    rows = [
        _prow(ticker="EEE", side="LONG", target_weight=0.02, sector="Health Care",
              signal_composite_sn=7.0, signal_date="2026-05-22", price_source="EODHD",
              entry_reference_date="2026-05-22", entry_price=100, current_price=120,
              current_price_date="2026-06-26", paper_return_pct=20.0, price_status="MARKED",
              order_action="NO_ORDER", review_status="PAPER_REVIEW_ONLY"),
        _prow(ticker="FFF", side="LONG", target_weight=0.02, sector="Unknown",
              signal_composite_sn=6.0, signal_date="2026-05-22", price_source="EODHD",
              entry_reference_date="2026-05-22", entry_price=50, current_price=46,
              current_price_date="2026-06-26", paper_return_pct=-8.0, price_status="MARKED",
              order_action="NO_ORDER", review_status="PAPER_REVIEW_ONLY"),
    ]
    return header + "\n" + "\n".join(rows) + "\n"

_REPO_ROOT = Path(__file__).resolve().parents[1]
_APP_PATH = _REPO_ROOT / "api" / "app.py"

_BOOK = "/v1/research/current-alpha/book"
_CREATE = "/v1/research/current-alpha/book/preview-create"
_HISTORY = "/v1/research/current-alpha/book/pnl-history"
_SNAPSHOT = "/v1/research/current-alpha/book/snapshot-preview"

_TEST_API_KEY = "current-alpha-book-endpoint-test-key"
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
def env(tmp_path: Path, monkeypatch) -> Path:
    pkg = _write_ops_fixture(tmp_path / "pkg")
    store = tmp_path / "store"
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR", str(pkg))
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_BOOK_DIR", str(store))
    return store


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("method,endpoint", [
    ("get", _BOOK), ("post", _CREATE), ("get", _HISTORY), ("post", _SNAPSHOT),
])
def test_endpoints_require_api_key(client: TestClient, env: Path, method: str, endpoint: str) -> None:
    resp = getattr(client, method)(endpoint)
    assert resp.status_code in (401, 403)


# ---------------------------------------------------------------------------
# GET book — empty then active
# ---------------------------------------------------------------------------

def test_book_empty_then_saved(client: TestClient, env: Path) -> None:
    empty = client.get(_BOOK, headers=_AUTH).json()
    assert empty["status"] == "NO_PAPER_BOOK_YET"
    assert empty["book"] is None

    saved = client.post(_CREATE, headers=_AUTH, json={"commit": True}).json()
    assert saved["wrote_to_local_paper_store"] is True
    assert (env / "paper_book.json").is_file()

    got = client.get(_BOOK, headers=_AUTH).json()
    assert got["status"] == "ACTIVE_PAPER_BOOK"
    assert got["book"]["n_positions"] == 4
    assert all(p["order_action"] == "NO_ORDER" for p in got["book"]["positions"])


# ---------------------------------------------------------------------------
# preview-create — preview writes nothing, commit writes book only
# ---------------------------------------------------------------------------

def test_preview_create_no_write(client: TestClient, env: Path) -> None:
    body = client.post(_CREATE, headers=_AUTH, json={"commit": False}).json()
    assert body["mode"] == "PREVIEW"
    assert body["wrote_to_local_paper_store"] is False
    assert not (env / "paper_book.json").exists()
    assert body["book"]["priced_count"] == 3


def test_preview_create_no_body_defaults_to_preview(client: TestClient, env: Path) -> None:
    body = client.post(_CREATE, headers=_AUTH).json()
    assert body["mode"] == "PREVIEW"
    assert body["wrote_to_local_paper_store"] is False


def test_save_writes_book_only(client: TestClient, env: Path) -> None:
    body = client.post(_CREATE, headers=_AUTH, json={"commit": True}).json()
    assert body["action"] == "SAVED_PAPER_BOOK"
    assert (env / "paper_book.json").is_file()
    assert not (env / "pnl_snapshots.json").exists()
    assert body["wrote_to_paper_trader"] is False


# ---------------------------------------------------------------------------
# snapshot — no book, then after save
# ---------------------------------------------------------------------------

def test_snapshot_without_book_controlled(client: TestClient, env: Path) -> None:
    body = client.post(_SNAPSHOT, headers=_AUTH, json={"commit": True}).json()
    assert body["status"] == "NO_PAPER_BOOK_YET"
    assert body["wrote_to_local_paper_store"] is False
    assert not (env / "pnl_snapshots.json").exists()


def test_snapshot_writes_snapshot_only(client: TestClient, env: Path) -> None:
    client.post(_CREATE, headers=_AUTH, json={"commit": True})
    body = client.post(_SNAPSHOT, headers=_AUTH, json={"commit": True}).json()
    assert body["action"] == "SNAPSHOT_WRITTEN"
    assert body["wrote_to_local_paper_store"] is True
    assert (env / "pnl_snapshots.json").is_file()
    assert body["snapshot"]["average_return_pct"] == pytest.approx(4.0)


def test_pnl_history_after_snapshot(client: TestClient, env: Path) -> None:
    client.post(_CREATE, headers=_AUTH, json={"commit": True})
    client.post(_SNAPSHOT, headers=_AUTH, json={"commit": True})
    hist = client.get(_HISTORY, headers=_AUTH).json()
    assert hist["status"] == "PNL_HISTORY_READY"
    assert hist["n_snapshots"] == 1
    assert hist["series"][0]["average_return_pct"] == pytest.approx(4.0)
    assert hist["series"][0]["as_of_price_date"] == "2026-06-26"
    assert "selected_book_id" in hist and "available_book_ids" in hist
    assert hist["excluded_snapshot_count"] == 0


# ---------------------------------------------------------------------------
# ISSUE 1 (route) — stale-mark de-duplication
# ---------------------------------------------------------------------------

def test_snapshot_dedup_same_price_date(client: TestClient, env: Path) -> None:
    client.post(_CREATE, headers=_AUTH, json={"commit": True})
    first = client.post(_SNAPSHOT, headers=_AUTH, json={"commit": True}).json()
    assert first["action"] == "SNAPSHOT_WRITTEN"
    second = client.post(_SNAPSHOT, headers=_AUTH, json={"commit": True}).json()
    assert second["action"] == "SNAPSHOT_SKIPPED_NO_NEW_PRICE_DATE"
    assert second["wrote_to_local_paper_store"] is False
    hist = client.get(_HISTORY, headers=_AUTH).json()
    assert hist["n_snapshots"] == 1


def test_snapshot_reports_freshness_fields(client: TestClient, env: Path) -> None:
    client.post(_CREATE, headers=_AUTH, json={"commit": True})
    body = client.post(_SNAPSHOT, headers=_AUTH, json={"commit": True}).json()
    assert body["as_of_price_date"] == "2026-06-26"
    assert body["observation_date"] != body["as_of_price_date"]
    assert body["mark_freshness_status"] in {
        "FRESH_MARK", "STALE_MARK_WARNING", "STALE_MARK_REJECT", "UNKNOWN_MARK_AGE"}


# ---------------------------------------------------------------------------
# ISSUE 2 (route) — TOP 25 / TOP 50 history isolation via ?book_size=
# ---------------------------------------------------------------------------

def test_pnl_history_book_size_isolation(client: TestClient, tmp_path: Path, monkeypatch) -> None:
    pkg = _write_ops_fixture(tmp_path / "pkg")
    (pkg / "current_alpha_paper_portfolio_top50.csv").write_text(
        _distinct_top50_csv(), encoding="utf-8")
    store = tmp_path / "store"
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR", str(pkg))
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_BOOK_DIR", str(store))

    client.post(_CREATE, headers=_AUTH, json={"commit": True, "book_size": 25})
    client.post(_SNAPSHOT, headers=_AUTH, json={"commit": True})
    client.post(_CREATE, headers=_AUTH, json={"commit": True, "book_size": 50})
    client.post(_SNAPSHOT, headers=_AUTH, json={"commit": True})

    h25 = client.get(_HISTORY + "?book_size=25", headers=_AUTH).json()
    assert h25["n_snapshots"] == 1
    assert "top25" in h25["selected_book_id"]
    assert h25["excluded_snapshot_count"] == 1
    assert all("top25" in s["book_id"] for s in h25["series"])

    h50 = client.get(_HISTORY + "?book_size=50", headers=_AUTH).json()
    assert h50["n_snapshots"] == 1
    assert "top50" in h50["selected_book_id"]
    assert h50["excluded_snapshot_count"] == 1
    assert all("top50" in s["book_id"] for s in h50["series"])
    # Both books persist; saving TOP 50 did not discard TOP 25.
    book = client.get(_BOOK, headers=_AUTH).json()
    ids = book["available_book_ids"]
    assert any("top25" in i for i in ids) and any("top50" in i for i in ids)


# ---------------------------------------------------------------------------
# Missing package — POST routes 503, GET routes still 200 (read store only)
# ---------------------------------------------------------------------------

def test_missing_package_post_create_503(client: TestClient, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR", str(tmp_path / "nope"))
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_BOOK_DIR", str(tmp_path / "store"))
    resp = client.post(_CREATE, headers=_AUTH, json={"commit": False})
    assert resp.status_code == 503
    detail = resp.json()["detail"]
    assert "Current alpha paper book unavailable" in detail
    assert "Phase 13-A package not found" in detail
    assert "Traceback" not in detail


def test_missing_package_get_book_still_ok(client: TestClient, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR", str(tmp_path / "nope"))
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_BOOK_DIR", str(tmp_path / "store"))
    assert client.get(_BOOK, headers=_AUTH).status_code == 200
    assert client.get(_HISTORY, headers=_AUTH).status_code == 200


def test_snapshot_missing_package_503_when_book_exists(client: TestClient, env: Path, monkeypatch, tmp_path: Path) -> None:
    client.post(_CREATE, headers=_AUTH, json={"commit": True})
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR", str(tmp_path / "nope"))
    resp = client.post(_SNAPSHOT, headers=_AUTH, json={"commit": True})
    assert resp.status_code == 503
    assert "Current alpha paper snapshot unavailable" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Safety surface
# ---------------------------------------------------------------------------

def test_safety_badges_and_flags(client: TestClient, env: Path) -> None:
    body = client.post(_CREATE, headers=_AUTH, json={"commit": True}).json()
    for badge in ("PREVIEW ONLY", "NO ORDERS", "NO BROKER", "NO AUTOMATION",
                  "MANUAL REVIEW ONLY", "PAPER TEST ONLY"):
        assert badge in body["safety_badges"]
    assert body["no_orders"] is True
    assert body["order_action_all"] == "NO_ORDER"
    assert body["calls_prediction_service"] is False
    assert body["calls_external_providers"] is False


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
    "research_current_alpha_book",
    "research_current_alpha_book_preview_create",
    "research_current_alpha_book_pnl_history",
    "research_current_alpha_book_snapshot_preview",
])
def test_handlers_no_db_or_action_tokens(func: str) -> None:
    handler = _handler_source(func)
    low = handler.lower()
    for needle in (".commit(", ".add(", "insert into", "update ", "delete from",
                   "get_session", "session.add"):
        assert needle not in low, f"{func} uses DB-write token: {needle!r}"
    for needle in ("order", "signal", "tradedecision", "trade_decision",
                   "automation", "prediction_client", "fetch_predictions",
                   "nasdaq", "intrinio", "fmp", "requests.get", "httpx", "broker"):
        assert needle not in low, f"{func} references forbidden token: {needle!r}"


def test_route_methods_are_correct() -> None:
    src = _APP_PATH.read_text(encoding="utf-8")
    # Reads are GET, writes are POST — and never the other way round.
    assert f'@app.get(\n    "{_BOOK}"' in src
    assert f'@app.get(\n    "{_HISTORY}"' in src
    assert f'@app.post(\n    "{_CREATE}"' in src
    assert f'@app.post(\n    "{_SNAPSHOT}"' in src
    assert f'@app.post(\n    "{_BOOK}"' not in src
    assert f'@app.get(\n    "{_CREATE}"' not in src
