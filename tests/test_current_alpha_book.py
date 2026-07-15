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
    BOOKS_FILE,
    SNAPSHOTS_FILE,
    CurrentAlphaPreviewError,
    _mark_freshness,
    load_current_alpha_book,
    load_current_alpha_pnl_history,
    preview_or_create_current_alpha_book,
    snapshot_current_alpha_book,
)

# Reuse the full-schema Phase 13-A fixture package builder from the ops tests.
from tests.test_current_alpha_operations import (
    _PORTFOLIO_COLS,
    _prow,
    _write_ops_fixture,
)


def _repackage_date(pkg: Path, new_date: str) -> None:
    """Rewrite the fixture package_date (the owned price-mark date) in place."""
    jp = pkg / "phase13a_current_champion_alpha_paper_test_package.json"
    data = json.loads(jp.read_text(encoding="utf-8"))
    data["package_date"] = new_date
    jp.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _custom_portfolio(rows: list[dict]) -> str:
    header = ",".join(_PORTFOLIO_COLS)
    body = "\n".join(_prow(**r) for r in rows)
    return header + "\n" + body + "\n"


def _set_top50_portfolio(pkg: Path, rows: list[dict]) -> None:
    """Overwrite the TOP 50 portfolio side-car with distinct tickers so book
    isolation (contributors must not cross book_id) is provable."""
    (pkg / "current_alpha_paper_portfolio_top50.csv").write_text(
        _custom_portfolio(rows), encoding="utf-8"
    )


# Distinct TOP 50 names (EEE best, FFF worst) — disjoint from the TOP 25 AAA..DDD.
_TOP50_ROWS = [
    dict(ticker="EEE", side="LONG", target_weight=0.02, sector="Health Care",
         signal_composite_sn=7.0, signal_date="2026-05-22", price_source="EODHD",
         entry_reference_date="2026-05-22", entry_price=100, current_price=120,
         current_price_date="2026-06-26", paper_return_pct=20.0, price_status="MARKED",
         order_action="NO_ORDER", review_status="PAPER_REVIEW_ONLY"),
    dict(ticker="FFF", side="LONG", target_weight=0.02, sector="Unknown",
         signal_composite_sn=6.0, signal_date="2026-05-22", price_source="EODHD",
         entry_reference_date="2026-05-22", entry_price=50, current_price=46,
         current_price_date="2026-06-26", paper_return_pct=-8.0, price_status="MARKED",
         order_action="NO_ORDER", review_status="PAPER_REVIEW_ONLY"),
]

_REPO_ROOT = Path(__file__).resolve().parents[1]
_MODULE_PATH = _REPO_ROOT / "api" / "current_alpha_book.py"
_APP_PATH = _REPO_ROOT / "api" / "app.py"

_SAFETY_BADGES = ("PREVIEW ONLY", "NO ORDERS", "NO BROKER", "NO AUTOMATION",
                  "MANUAL REVIEW ONLY", "PAPER TEST ONLY")


@pytest.fixture(autouse=True)
def _isolate_daily_marks(tmp_path: Path, monkeypatch) -> None:
    # Point the Phase 13-G daily-mark resolver at an empty dir so this 13-F suite
    # deterministically exercises the PHASE13A_STALE_FALLBACK package-mark path and
    # never picks up a real daily mark artifact present on the host.
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_DAILY_MARK_DIR",
                       str(tmp_path / "no_daily_marks"))


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
    # One committed snapshot for one price date -> a single-point history (a second
    # click at the same price date is de-duplicated; see the dedup test below).
    preview_or_create_current_alpha_book(pkg, commit=True, book_dir=store)
    snapshot_current_alpha_book(pkg, commit=True, book_dir=store)
    h = load_current_alpha_pnl_history(book_dir=store)
    assert h["status"] == "PNL_HISTORY_READY"
    assert h["n_snapshots"] == 1
    assert len(h["series"]) == 1
    assert h["series"][0]["average_return_pct"] == pytest.approx(4.0)
    assert h["series"][0]["as_of_price_date"] == "2026-06-26"
    assert h["latest_snapshot"]["average_return_pct"] == pytest.approx(4.0)
    # AAA is the best mean contributor across snapshots; BBB the worst.
    assert h["best_contributors_over_time"][0]["ticker"] == "AAA"
    assert h["worst_contributors_over_time"][0]["ticker"] == "BBB"
    assert h["benchmark_status"] is not None


# ---------------------------------------------------------------------------
# ISSUE 1 — stale-mark de-duplication + freshness
# ---------------------------------------------------------------------------

def test_snapshot_dedup_same_book_same_price_date(pkg: Path, store: Path) -> None:
    """Same book_id + same as_of_price_date: first commit writes, second is skipped
    and does NOT advance the daily curve (n_snapshots stays 1)."""
    preview_or_create_current_alpha_book(pkg, commit=True, book_dir=store)
    s1 = snapshot_current_alpha_book(pkg, commit=True, book_dir=store)
    assert s1["action"] == "SNAPSHOT_WRITTEN"
    assert s1["wrote_to_local_paper_store"] is True

    s2 = snapshot_current_alpha_book(pkg, commit=True, book_dir=store)
    assert s2["action"] == "SNAPSHOT_SKIPPED_NO_NEW_PRICE_DATE"
    assert s2["wrote_to_local_paper_store"] is False
    assert s2["n_snapshots_after"] == 1
    assert any("No new owned price date" in w for w in s2["warnings"])
    assert s2["snapshot"] is not None  # the already-recorded snapshot is returned

    payload = json.loads((store / SNAPSHOTS_FILE).read_text(encoding="utf-8"))
    assert len(payload["snapshots"]) == 1
    h = load_current_alpha_pnl_history(book_dir=store)
    assert h["n_snapshots"] == 1


def test_snapshot_new_price_date_advances_history(pkg: Path, store: Path, tmp_path: Path) -> None:
    """A genuinely new owned price date DOES advance the daily curve, and the
    financial x-axis is the price-mark date (ordered, distinct)."""
    preview_or_create_current_alpha_book(pkg, commit=True, book_dir=store)
    snapshot_current_alpha_book(pkg, commit=True, book_dir=store)  # 2026-06-26

    pkg2 = _write_ops_fixture(tmp_path / "pkg2")  # same signal_date -> same book_id
    _repackage_date(pkg2, "2026-07-10")
    s2 = snapshot_current_alpha_book(pkg2, commit=True, book_dir=store)
    assert s2["action"] == "SNAPSHOT_WRITTEN"
    assert s2["as_of_price_date"] == "2026-07-10"

    h = load_current_alpha_pnl_history(book_dir=store)
    assert h["n_snapshots"] == 2
    assert [s["as_of_price_date"] for s in h["series"]] == ["2026-06-26", "2026-07-10"]


def test_snapshot_records_separate_observation_and_price_dates(pkg: Path, store: Path) -> None:
    preview_or_create_current_alpha_book(pkg, commit=True, book_dir=store)
    s = snapshot_current_alpha_book(pkg, commit=True, book_dir=store)
    snap = s["snapshot"]
    assert snap["as_of_price_date"] == "2026-06-26"
    assert snap["observation_date"]  # today (UTC), non-empty
    # Observation date (today) and the owned price-mark date are distinct fields.
    assert snap["observation_date"] != snap["as_of_price_date"]
    assert "mark_age_calendar_days" in snap
    assert snap["mark_freshness_status"] in {
        "FRESH_MARK", "STALE_MARK_WARNING", "STALE_MARK_REJECT", "UNKNOWN_MARK_AGE"}
    assert s["mark_freshness_status"] == snap["mark_freshness_status"]


def test_mark_freshness_thresholds() -> None:
    # warn > 3 calendar days, reject > 7 (boundaries are NOT stale).
    assert _mark_freshness("2026-06-26", "2026-06-27") == (1, "FRESH_MARK")
    assert _mark_freshness("2026-06-26", "2026-06-29") == (3, "FRESH_MARK")
    assert _mark_freshness("2026-06-26", "2026-06-30") == (4, "STALE_MARK_WARNING")
    assert _mark_freshness("2026-06-26", "2026-07-03") == (7, "STALE_MARK_WARNING")
    assert _mark_freshness("2026-06-26", "2026-07-04") == (8, "STALE_MARK_REJECT")
    assert _mark_freshness("2026-06-26", "2026-07-20") == (24, "STALE_MARK_REJECT")
    age, status = _mark_freshness(None, "2026-07-01")
    assert age is None and status == "UNKNOWN_MARK_AGE"


# ---------------------------------------------------------------------------
# ISSUE 2 — book-history isolation (TOP 25 vs TOP 50)
# ---------------------------------------------------------------------------

def _save_and_snap_both_books(pkg: Path, store: Path) -> None:
    """Save + snapshot a TOP 25 book (AAA..DDD), then a TOP 50 book (EEE/FFF)."""
    _set_top50_portfolio(pkg, _TOP50_ROWS)
    preview_or_create_current_alpha_book(pkg, commit=True, book_size=25, book_dir=store)
    snapshot_current_alpha_book(pkg, commit=True, book_dir=store)   # marks active = top25
    preview_or_create_current_alpha_book(pkg, commit=True, book_size=50, book_dir=store)
    snapshot_current_alpha_book(pkg, commit=True, book_dir=store)   # marks active = top50


def test_top25_and_top50_coexist(pkg: Path, store: Path) -> None:
    _save_and_snap_both_books(pkg, store)
    book = load_current_alpha_book(book_dir=store)
    ids = book["available_book_ids"]
    assert any("top25" in i for i in ids)
    assert any("top50" in i for i in ids)
    # Saving TOP 50 did not discard the TOP 25 book.
    multi = json.loads((store / BOOKS_FILE).read_text(encoding="utf-8"))
    assert len(multi["books"]) == 2


def test_top25_history_contains_only_top25(pkg: Path, store: Path) -> None:
    _save_and_snap_both_books(pkg, store)
    h = load_current_alpha_pnl_history(book_dir=store, book_size=25)
    assert h["n_snapshots"] == 1
    assert h["excluded_snapshot_count"] == 1
    assert all("top25" in s["book_id"] for s in h["series"])
    assert "top25" in h["selected_book_id"]


def test_top50_history_contains_only_top50(pkg: Path, store: Path) -> None:
    _save_and_snap_both_books(pkg, store)
    h = load_current_alpha_pnl_history(book_dir=store, book_size=50)
    assert h["n_snapshots"] == 1
    assert h["excluded_snapshot_count"] == 1
    assert all("top50" in s["book_id"] for s in h["series"])
    assert "top50" in h["selected_book_id"]


def test_contributors_never_cross_book_id(pkg: Path, store: Path) -> None:
    _save_and_snap_both_books(pkg, store)
    h25 = load_current_alpha_pnl_history(book_dir=store, book_size=25)
    names25 = {c["ticker"] for c in
               h25["best_contributors_over_time"] + h25["worst_contributors_over_time"]}
    assert h25["best_contributors_over_time"][0]["ticker"] == "AAA"
    assert h25["worst_contributors_over_time"][0]["ticker"] == "BBB"
    assert "EEE" not in names25 and "FFF" not in names25

    h50 = load_current_alpha_pnl_history(book_dir=store, book_size=50)
    names50 = {c["ticker"] for c in
               h50["best_contributors_over_time"] + h50["worst_contributors_over_time"]}
    assert h50["best_contributors_over_time"][0]["ticker"] == "EEE"
    assert h50["worst_contributors_over_time"][0]["ticker"] == "FFF"
    assert "AAA" not in names50 and "BBB" not in names50


def test_pnl_history_default_is_active_book_only(pkg: Path, store: Path) -> None:
    _save_and_snap_both_books(pkg, store)  # active ends as top50
    h = load_current_alpha_pnl_history(book_dir=store)  # no selector -> active
    assert h["selected_book_id"].endswith("top50")
    assert h["active_book_id"].endswith("top50")
    assert h["n_snapshots"] == 1
    assert h["excluded_snapshot_count"] == 1
    assert all("top50" in s["book_id"] for s in h["series"])


# ---------------------------------------------------------------------------
# Legacy read / migration (no DB migration; the local book is never lost)
# ---------------------------------------------------------------------------

def test_legacy_single_book_read_and_migration(pkg: Path, store: Path) -> None:
    # Simulate a pre-hardening store: only the legacy single-book paper_book.json.
    proposed = preview_or_create_current_alpha_book(pkg, commit=False, book_dir=store)["book"]
    store.mkdir(parents=True, exist_ok=True)
    (store / BOOK_FILE).write_text(json.dumps(proposed), encoding="utf-8")
    assert not (store / BOOKS_FILE).exists()

    got = load_current_alpha_book(book_dir=store)
    assert got["status"] == "ACTIVE_PAPER_BOOK"
    assert got["book"]["book_id"] == proposed["book_id"]
    assert any("legacy" in w.lower() for w in got["warnings"])

    # A later save migrates it into the multi-book store WITHOUT losing the book.
    preview_or_create_current_alpha_book(pkg, commit=True, book_size=50, book_dir=store)
    assert (store / BOOKS_FILE).is_file()
    multi = json.loads((store / BOOKS_FILE).read_text(encoding="utf-8"))
    ids = set(multi["books"].keys())
    assert proposed["book_id"] in ids            # legacy TOP 25 preserved
    assert any("top50" in i for i in ids)        # new TOP 50 added


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
