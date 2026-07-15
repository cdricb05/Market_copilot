"""
tests/test_current_alpha_book.py — Phase 13-F persistent paper book + PnL history.

Self-contained: every test builds a temporary Phase 13-A fixture package (full
paper-portfolio schema, reused from the Phase 13-C/D/E ops fixture) and points
the paper-book store at a tmp directory. Behavioural tests assert the store
round-trips (preview writes nothing; save writes only paper_book.json; snapshot
writes only pnl_snapshots.json) and the computed PnL math; static source-scan
guards assert the paper-only, no-DB / no-order / no-broker / no-provider contract
and that the module only ever writes through its atomic JSON writer.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from paper_trader.api.current_alpha_book import (
    BOOK_FILE,
    SNAPSHOTS_FILE,
    CurrentAlphaPreviewError,
    load_current_alpha_book,
    load_current_alpha_pnl_history,
    preview_or_create_current_alpha_book,
    snapshot_current_alpha_book,
)

# Reuse the full-schema Phase 13-A fixture package builder from the ops tests.
from tests.test_current_alpha_operations import _write_ops_fixture

_REPO_ROOT = Path(__file__).resolve().parents[1]
_MODULE_PATH = _REPO_ROOT / "api" / "current_alpha_book.py"
_APP_PATH = _REPO_ROOT / "api" / "app.py"

_SAFETY_BADGES = ("PREVIEW ONLY", "NO ORDERS", "NO BROKER", "NO AUTOMATION",
                  "MANUAL REVIEW ONLY", "PAPER TEST ONLY")


@pytest.fixture
def pkg(tmp_path: Path) -> Path:
    return _write_ops_fixture(tmp_path / "pkg")


@pytest.fixture
def store(tmp_path: Path) -> Path:
    return tmp_path / "store"


# ---------------------------------------------------------------------------
# GET book — empty store
# ---------------------------------------------------------------------------

def test_book_empty_store_is_no_book(store: Path) -> None:
    b = load_current_alpha_book(book_dir=store)
    assert b["status"] == "NO_PAPER_BOOK_YET"
    assert b["book"] is None
    assert "Save Paper Book" in b["guidance"]
    assert not (store / BOOK_FILE).exists()  # reading never creates the store
    for badge in _SAFETY_BADGES:
        assert badge in b["safety_badges"]
    assert b["wrote_to_local_paper_store"] is False


# ---------------------------------------------------------------------------
# preview-create — preview writes nothing
# ---------------------------------------------------------------------------

def test_preview_create_writes_nothing(pkg: Path, store: Path) -> None:
    p = preview_or_create_current_alpha_book(pkg, commit=False, book_dir=store)
    assert p["mode"] == "PREVIEW"
    assert p["action"] == "PREVIEW_ONLY_NOT_WRITTEN"
    assert p["wrote_to_local_paper_store"] is False
    assert not (store / BOOK_FILE).exists()
    # Proposed book: AAA/BBB/CCC/DDD; CCC has no local price.
    book = p["book"]
    assert book["n_positions"] == 4
    assert book["priced_count"] == 3
    assert book["unpriced_count"] == 1
    assert book["book_size"] == 25
    assert book["alpha_name"] == "composite_sn"
    assert all(pos["order_action"] == "NO_ORDER" for pos in book["positions"])


def test_preview_create_missing_package_raises(tmp_path: Path, store: Path) -> None:
    with pytest.raises(CurrentAlphaPreviewError, match="Phase 13-A package not found"):
        preview_or_create_current_alpha_book(tmp_path / "nope", commit=False, book_dir=store)


# ---------------------------------------------------------------------------
# save (commit) — writes only paper_book.json
# ---------------------------------------------------------------------------

def test_save_writes_only_book_file(pkg: Path, store: Path) -> None:
    p = preview_or_create_current_alpha_book(pkg, commit=True, book_dir=store)
    assert p["mode"] == "COMMIT"
    assert p["action"] == "SAVED_PAPER_BOOK"
    assert p["wrote_to_local_paper_store"] is True
    assert p["local_paper_store_write_kind"] == "PAPER_BOOK"
    # The book file exists; no snapshots file was created by a save.
    assert (store / BOOK_FILE).is_file()
    assert not (store / SNAPSHOTS_FILE).exists()
    # The paper trader DB is never touched.
    assert p["wrote_to_paper_trader"] is False
    # Round-trips through GET book.
    got = load_current_alpha_book(book_dir=store)
    assert got["status"] == "ACTIVE_PAPER_BOOK"
    assert got["book"]["book_id"] == p["book"]["book_id"]
    assert got["book"]["n_positions"] == 4


def test_save_book_size_50_uses_top50(pkg: Path, store: Path) -> None:
    p = preview_or_create_current_alpha_book(pkg, commit=True, book_size=50, book_dir=store)
    assert p["book"]["book_size"] == 50
    assert "top50" in p["book"]["book_id"]


def test_save_invalid_book_size_defaults_with_warning(pkg: Path, store: Path) -> None:
    p = preview_or_create_current_alpha_book(pkg, commit=True, book_size=7, book_dir=store)
    assert p["book"]["book_size"] == 25
    assert any("not supported" in w for w in p["warnings"])


# ---------------------------------------------------------------------------
# snapshot — preview vs commit
# ---------------------------------------------------------------------------

def test_snapshot_no_book_is_controlled(pkg: Path, store: Path) -> None:
    s = snapshot_current_alpha_book(pkg, commit=True, book_dir=store)
    assert s["status"] == "NO_PAPER_BOOK_YET"
    assert s["snapshot"] is None
    assert s["wrote_to_local_paper_store"] is False
    assert not (store / SNAPSHOTS_FILE).exists()


def test_snapshot_preview_writes_nothing(pkg: Path, store: Path) -> None:
    preview_or_create_current_alpha_book(pkg, commit=True, book_dir=store)
    s = snapshot_current_alpha_book(pkg, commit=False, book_dir=store)
    assert s["mode"] == "PREVIEW"
    assert s["wrote_to_local_paper_store"] is False
    assert not (store / SNAPSHOTS_FILE).exists()
    snap = s["snapshot"]
    assert snap["coverage"]["covered_count"] == 3
    assert snap["average_return_pct"] == pytest.approx(4.0)  # (10 - 4 + 6)/3
    assert snap["median_return_pct"] == pytest.approx(6.0)
    assert snap["hit_rate_pct"] == pytest.approx(66.67, abs=0.01)
    assert snap["best_contributors"][0]["ticker"] == "AAA"
    assert snap["worst_contributors"][0]["ticker"] == "BBB"
    assert all(pos["order_action"] == "NO_ORDER" for pos in snap["positions"])


def test_snapshot_commit_writes_only_snapshots_file(pkg: Path, store: Path) -> None:
    preview_or_create_current_alpha_book(pkg, commit=True, book_dir=store)
    s = snapshot_current_alpha_book(pkg, commit=True, book_dir=store)
    assert s["action"] == "SNAPSHOT_WRITTEN"
    assert s["wrote_to_local_paper_store"] is True
    assert s["local_paper_store_write_kind"] == "PNL_SNAPSHOT"
    assert (store / SNAPSHOTS_FILE).is_file()
    payload = json.loads((store / SNAPSHOTS_FILE).read_text(encoding="utf-8"))
    assert len(payload["snapshots"]) == 1
    assert s["wrote_to_paper_trader"] is False


def test_snapshot_missing_package_raises_when_book_exists(pkg: Path, tmp_path: Path, store: Path) -> None:
    preview_or_create_current_alpha_book(pkg, commit=True, book_dir=store)
    with pytest.raises(CurrentAlphaPreviewError, match="Phase 13-A package not found"):
        snapshot_current_alpha_book(tmp_path / "nope", commit=True, book_dir=store)


# ---------------------------------------------------------------------------
# pnl-history
# ---------------------------------------------------------------------------

def test_pnl_history_empty_is_no_book(store: Path) -> None:
    h = load_current_alpha_pnl_history(book_dir=store)
    assert h["status"] == "NO_PAPER_BOOK_YET"
    assert h["series"] == []


def test_pnl_history_after_snapshots(pkg: Path, store: Path) -> None:
    preview_or_create_current_alpha_book(pkg, commit=True, book_dir=store)
    snapshot_current_alpha_book(pkg, commit=True, book_dir=store)
    snapshot_current_alpha_book(pkg, commit=True, book_dir=store)
    h = load_current_alpha_pnl_history(book_dir=store)
    assert h["status"] == "PNL_HISTORY_READY"
    assert h["n_snapshots"] == 2
    assert len(h["series"]) == 2
    assert h["series"][0]["average_return_pct"] == pytest.approx(4.0)
    assert h["latest_snapshot"]["average_return_pct"] == pytest.approx(4.0)
    # AAA is the best mean contributor across snapshots; BBB the worst.
    assert h["best_contributors_over_time"][0]["ticker"] == "AAA"
    assert h["worst_contributors_over_time"][0]["ticker"] == "BBB"
    assert h["benchmark_status"] is not None


def test_pnl_history_safety_surface(pkg: Path, store: Path) -> None:
    preview_or_create_current_alpha_book(pkg, commit=True, book_dir=store)
    snapshot_current_alpha_book(pkg, commit=True, book_dir=store)
    h = load_current_alpha_pnl_history(book_dir=store)
    for badge in _SAFETY_BADGES:
        assert badge in h["safety_badges"]
    assert h["no_orders"] is True and h["no_broker"] is True and h["no_automation"] is True
    assert h["wrote_to_paper_trader"] is False and h["live_trading"] is False


# ---------------------------------------------------------------------------
# Static safety-contract guards (source scans)
# ---------------------------------------------------------------------------

def _module_source() -> str:
    return _MODULE_PATH.read_text(encoding="utf-8")


def _import_targets(source: str) -> list[str]:
    targets: list[str] = []
    for line in source.splitlines():
        m = re.match(r"\s*(?:from|import)\s+([\w\.]+)", line)
        if m:
            targets.append(m.group(1))
    return targets


def test_no_database_or_network_imports() -> None:
    for t in _import_targets(_module_source()):
        root = t.split(".")[0]
        assert root not in {"sqlalchemy", "psycopg2", "alembic", "requests", "httpx",
                            "urllib", "http", "socket", "aiohttp"}, f"forbidden import: {t}"
        assert "session" not in t.lower(), f"session import: {t}"
        assert not t.startswith("paper_trader.db"), f"db import: {t}"


def test_no_db_write_or_forbidden_action_tokens() -> None:
    # Scan CODE only — the module docstring and safety-message string literals
    # legitimately say "no broker", "no nasdaq / intrinio / fmp", etc., so a raw
    # whole-source scan for those bare words would false-positive. Import safety
    # is covered separately; here we forbid concrete dangerous call/URL patterns.
    src = _module_source()
    low = src.lower()
    # No DB writes (the store is plain JSON files, never the trading DB).
    for needle in (".commit(", ".add(", "insert into", "update ", "delete from",
                   "get_session", "session.add", "to_csv", "engine.connect"):
        assert needle not in low, f"forbidden DB token: {needle!r}"
    # No order / prediction / paid-provider CALL or URL surface (patterns that
    # would never appear in the safety prose).
    for needle in ("create_order", "place_order", "submit_order",
                   "prediction_client", "fetch_predictions", "requests.get",
                   "httpx.", "/v1/orders", "/v1/signals", "/v1/decisions",
                   "boto3", "smtplib"):
        assert needle not in low, f"forbidden action token: {needle!r}"


def test_writes_only_through_atomic_json_writer() -> None:
    src = _module_source()
    # The only file-opening in the module is the JSON reader/writer helpers.
    assert "_atomic_write_json" in src
    # No FastAPI route is declared in the data module.
    assert "@app." not in src and "@router." not in src
    assert "apirouter" not in src.lower() and "add_api_route" not in src


def test_app_wires_book_routes() -> None:
    app_src = _APP_PATH.read_text(encoding="utf-8")
    assert '@app.get(\n    "/v1/research/current-alpha/book"' in app_src
    assert '@app.get(\n    "/v1/research/current-alpha/book/pnl-history"' in app_src
    assert '@app.post(\n    "/v1/research/current-alpha/book/preview-create"' in app_src
    assert '@app.post(\n    "/v1/research/current-alpha/book/snapshot-preview"' in app_src
