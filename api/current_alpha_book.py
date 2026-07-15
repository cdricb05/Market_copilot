"""
api/current_alpha_book.py — Phase 13-F persistent current-alpha paper book.

This module turns the previously stateless Phase 13-B/C/D/E current-alpha
preview into a real *paper-only* monitoring workflow: it persists selected paper
books for the champion alpha (``composite_sn``) and a growing history of daily
paper PnL snapshots, and computes PnL-over-time views for the UI.

Four data services back four Paper Trader routes:

    load_current_alpha_book(...)             -> GET  /v1/research/current-alpha/book
    preview_or_create_current_alpha_book(...)-> POST /v1/research/current-alpha/book/preview-create
    load_current_alpha_pnl_history(...)      -> GET  /v1/research/current-alpha/book/pnl-history
    snapshot_current_alpha_book(...)         -> POST /v1/research/current-alpha/book/snapshot-preview

Storage (no DB migration — deliberately a safe local JSON tracking store):
    A directory resolved from ``PAPER_TRADER_CURRENT_ALPHA_BOOK_DIR`` (default
    ``~/.paper_trader/current_alpha_paper_book``) holds:
        paper_books.json    — the multi-book store: {"books": {book_id: book},
                              "active_book_id": ...}. TOP 25 and TOP 50 coexist as
                              distinct persistent books; saving one never discards
                              the other.
        paper_book.json     — a legacy single-book mirror of the active book, kept
                              for backward-compatible reads (older shape / tooling).
        pnl_snapshots.json  — an append-only list of paper PnL snapshots, each
                              keyed by its book_id.
    The store lives OUTSIDE the git working tree by default so the repo status
    stays clean, and it is env-overridable so tests point it at a tmp dir.

Two paper-PnL integrity guarantees (Phase 13-F hardening):
    1. Stale-mark de-duplication. A snapshot's financial identity is
       (book_id, as_of_price_date). If a committed snapshot already exists for the
       active book at the same owned price date, Snapshot Today does NOT append a
       second one — it returns SNAPSHOT_SKIPPED_NO_NEW_PRICE_DATE so repeated
       clicks before an owned-price refresh cannot fabricate a fake daily curve.
       The observation date (when it was taken) and the price-mark date (what it
       reflects) are recorded separately, with a mark-freshness label.
    2. Book-history isolation. Every snapshot carries its book_id; PnL history is
       always filtered to a single selected book (active by default, or by
       book_id / book_size), so TOP 25 and TOP 50 returns are never mixed and
       best/worst contributors are computed only within the selected book.

Scope (paper-only, preview-only — enforced):
    - The ONLY things this module ever writes are local JSON files, and only when
      a caller explicitly passes ``commit=True``. Every position and every payload
      is marked ``NO_ORDER`` / paper-only.
    - It NEVER touches the Paper Trader database (``wrote_to_paper_trader`` stays
      False), creates no signals / trade decisions / orders, runs no automation,
      connects to no broker, and calls neither the prediction service nor any
      external / paid market-data provider (no Nasdaq / Intrinio / FMP).
    - Marks come only from the committed Phase 13-A package (already priced from
      owned local EOD). Nothing is faked; a missing package / book yields a
      controlled status, never a crash.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

from paper_trader.api.current_alpha_preview import (
    CurrentAlphaPreviewError,
    PACKAGE_JSON_NAME,
    SAFETY_BADGES,
    SAFETY_FLAGS,
    _read_csv_rows,
    _resolve_package_dir,
    load_current_alpha_preview,
)

# ---------------------------------------------------------------------------
# Store location + constants
# ---------------------------------------------------------------------------

#: Environment variable that overrides the default paper-book store directory.
BOOK_DIR_ENV_VAR = "PAPER_TRADER_CURRENT_ALPHA_BOOK_DIR"

#: Default store directory — OUTSIDE the git working tree (keeps repo clean).
DEFAULT_BOOK_DIR = Path.home() / ".paper_trader" / "current_alpha_paper_book"

#: Store file names.
BOOKS_FILE = "paper_books.json"      # authoritative multi-book store
BOOK_FILE = "paper_book.json"        # legacy single-book mirror (back-compat read)
SNAPSHOTS_FILE = "pnl_snapshots.json"

#: Portfolio side-cars in the Phase 13-A package (source of positions + marks).
_PORTFOLIO_TOP25 = "current_alpha_paper_portfolio_top25.csv"
_PORTFOLIO_TOP50 = "current_alpha_paper_portfolio_top50.csv"

#: Every persisted position / payload carries this — nothing implies execution.
ORDER_ACTION_NONE = "NO_ORDER"

#: Supported paper-book sizes (top-N long-only).
BOOK_SIZES = (25, 50)
DEFAULT_BOOK_SIZE = 25

#: Mark-freshness thresholds (calendar days between the price-mark date and the
#: observation date). These bound the CLAIM that a mark is current daily PnL —
#: they never reject the alpha signal itself.
MARK_FRESHNESS_WARN_DAYS = 3
MARK_FRESHNESS_REJECT_DAYS = 7

_PAPER_ONLY_NOTICE = (
    "This is a paper-only, preview-only book. No order is created. No signal is "
    "created. No trade decision is created. Nothing is sent to a broker and no "
    "automation is scheduled. Writes go only to a local paper-tracking JSON "
    "store. Manual review required."
)


class CurrentAlphaBookError(RuntimeError):
    """Raised for a bad paper-book request (never for a missing package)."""


# ---------------------------------------------------------------------------
# Small helpers (pure)
# ---------------------------------------------------------------------------

def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _round(value: Optional[float], digits: int = 4) -> Optional[float]:
    return round(value, digits) if isinstance(value, (int, float)) else None


def _median(values: list[float]) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _ordered_unique(items: list[Any]) -> list[Any]:
    """Stable de-dup that preserves first-seen order (no set — keeps the source
    scan clean and the ordering deterministic)."""
    seen: list[Any] = []
    for x in items:
        if x is not None and x not in seen:
            seen.append(x)
    return seen


def _parse_iso_date(value: Any) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _calendar_age_days(observation_date: Any, as_of_price_date: Any) -> Optional[int]:
    """Calendar days between the price-mark date and the observation date."""
    obs = _parse_iso_date(observation_date)
    priced = _parse_iso_date(as_of_price_date)
    if obs is None or priced is None:
        return None
    return (obs - priced).days


def _mark_freshness(
    as_of_price_date: Any, observation_date: Any
) -> tuple[Optional[int], str]:
    """Classify how stale a price mark is relative to the observation date.

    Returns ``(mark_age_calendar_days, status)`` where status is one of
    FRESH_MARK / STALE_MARK_WARNING / STALE_MARK_REJECT / UNKNOWN_MARK_AGE. This
    bounds the claim that the mark is *current daily PnL* — it does not reject the
    alpha signal.
    """
    age = _calendar_age_days(observation_date, as_of_price_date)
    if age is None:
        return None, "UNKNOWN_MARK_AGE"
    if age > MARK_FRESHNESS_REJECT_DAYS:
        return age, "STALE_MARK_REJECT"
    if age > MARK_FRESHNESS_WARN_DAYS:
        return age, "STALE_MARK_WARNING"
    return age, "FRESH_MARK"


def _normalize_book_size(book_size: Any) -> tuple[int, Optional[str]]:
    """Coerce a requested book size to a supported value (25 or 50) + a warning."""
    parsed: Optional[int]
    try:
        parsed = int(book_size)
    except (TypeError, ValueError):
        parsed = None
    if parsed in BOOK_SIZES:
        return parsed, None
    return (
        DEFAULT_BOOK_SIZE,
        f"Requested book_size {book_size!r} is not supported; using "
        f"{DEFAULT_BOOK_SIZE} (allowed: {', '.join(str(s) for s in BOOK_SIZES)}).",
    )


def _safety_block(*, wrote_store: bool = False, store_kind: Optional[str] = None) -> dict[str, Any]:
    """Enforced safety surface. ``wrote_to_paper_trader`` always stays False; a
    local paper-store write is reported separately and explicitly."""
    return {
        "safety_badges": list(SAFETY_BADGES),
        "safety": dict(SAFETY_FLAGS),
        **dict(SAFETY_FLAGS),
        "order_action_all": ORDER_ACTION_NONE,
        # Honest, separate reporting of the local JSON store write (never the DB).
        "wrote_to_local_paper_store": bool(wrote_store),
        "local_paper_store_write_kind": store_kind,
    }


# ---------------------------------------------------------------------------
# Store I/O (the ONLY place this module writes — local JSON only)
# ---------------------------------------------------------------------------

def _resolve_book_dir(book_dir: Optional[Union[str, Path]]) -> Path:
    if book_dir is not None:
        return Path(book_dir)
    env_value = os.environ.get(BOOK_DIR_ENV_VAR)
    if env_value:
        return Path(env_value)
    return DEFAULT_BOOK_DIR


def _read_json_file(path: Path) -> tuple[Any, Optional[str]]:
    """Read a JSON file. Returns (data, error). Missing file -> (None, None)."""
    if not path.is_file():
        return None, None
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle), None
    except (OSError, ValueError) as exc:
        return None, f"could not read {path.name}: {exc}"


def _atomic_write_json(path: Path, data: Any) -> None:
    """Write JSON atomically into the store dir (temp file + os.replace)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, sort_keys=False)
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.remove(tmp_name)
            except OSError:
                pass


def _read_legacy_book(book_dir: Path) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    """Read the legacy single-book file (paper_book.json), if present."""
    data, err = _read_json_file(book_dir / BOOK_FILE)
    if err:
        return None, err
    return (data if isinstance(data, dict) else None), None


def _latest_book_id(books: dict[str, Any]) -> Optional[str]:
    """Pick the most-recently-created book id (fallback: any key)."""
    if not books:
        return None
    ordered = sorted(
        books.items(), key=lambda kv: str((kv[1] or {}).get("created_at") or "")
    )
    return ordered[-1][0]


def _read_books_store(book_dir: Path) -> tuple[dict[str, Any], list[str]]:
    """Load the multi-book store, migrating a legacy single-book file on read.

    Returns ``({"books": {book_id: book}, "active_book_id": ..., "from_legacy":
    bool}, warnings)``. Read-only — a legacy paper_book.json is surfaced as the
    active book but is not rewritten here (it is preserved on the next save).
    """
    warnings: list[str] = []
    data, err = _read_json_file(book_dir / BOOKS_FILE)
    if err:
        warnings.append(err)
    if isinstance(data, dict) and isinstance(data.get("books"), dict):
        books = {k: v for k, v in data["books"].items() if isinstance(v, dict)}
        active = data.get("active_book_id")
        if active not in books:
            active = _latest_book_id(books)
        return {"books": books, "active_book_id": active, "from_legacy": False}, warnings

    # Backward-compatible read: a pre-hardening store had only paper_book.json.
    legacy, lerr = _read_legacy_book(book_dir)
    if lerr:
        warnings.append(lerr)
    if isinstance(legacy, dict) and legacy.get("book_id"):
        bid = str(legacy["book_id"])
        warnings.append(
            "Loaded a legacy single-book paper store (paper_book.json); it is "
            "preserved and will migrate into the multi-book store on the next save."
        )
        return {"books": {bid: legacy}, "active_book_id": bid, "from_legacy": True}, warnings

    return {"books": {}, "active_book_id": None, "from_legacy": False}, warnings


def _active_book(store: dict[str, Any]) -> Optional[dict[str, Any]]:
    active = store.get("active_book_id")
    books = store.get("books") or {}
    if active and active in books:
        return books[active]
    return None


def _available_book_ids(store: dict[str, Any], snapshots: list[dict[str, Any]]) -> list[str]:
    ids = list((store.get("books") or {}).keys())
    ids += [s.get("book_id") for s in snapshots if s.get("book_id")]
    return _ordered_unique(ids)


def _write_books_store(book_dir: Path, store: dict[str, Any]) -> None:
    """Persist the multi-book store, plus a legacy single-book mirror.

    The authoritative file is paper_books.json. paper_book.json is refreshed as a
    mirror of the active book so any older reader still sees the current book — a
    live paper book is never dropped silently.
    """
    books = store.get("books") or {}
    active = store.get("active_book_id")
    _atomic_write_json(
        book_dir / BOOKS_FILE, {"books": books, "active_book_id": active}
    )
    active_book = books.get(active) if active else None
    if isinstance(active_book, dict):
        _atomic_write_json(book_dir / BOOK_FILE, active_book)


def _read_snapshots(book_dir: Path) -> tuple[list[dict[str, Any]], Optional[str]]:
    data, err = _read_json_file(book_dir / SNAPSHOTS_FILE)
    if err:
        return [], err
    if isinstance(data, dict):
        data = data.get("snapshots")
    if not isinstance(data, list):
        return [], None
    return [r for r in data if isinstance(r, dict)], None


# ---------------------------------------------------------------------------
# Building a proposed book from the Phase 13-A package (read-only)
# ---------------------------------------------------------------------------

def _portfolio_file(book_size: int) -> str:
    return _PORTFOLIO_TOP50 if book_size == 50 else _PORTFOLIO_TOP25


def _read_portfolio_rows(base: Path, book_size: int) -> list[dict[str, str]]:
    """Read the package portfolio CSV, skipping blanks / aggregate ``_`` rows."""
    rows = _read_csv_rows(base / _portfolio_file(book_size))
    out: list[dict[str, str]] = []
    for r in rows:
        ticker = str(r.get("ticker", "")).strip()
        if not ticker or ticker.startswith("_"):
            continue
        out.append(r)
    return out


def _position_from_row(rank: int, r: dict[str, str]) -> dict[str, Any]:
    status = str(r.get("price_status", "")).strip()
    return {
        "source_rank": rank,
        "ticker": str(r.get("ticker", "")).strip(),
        "sector": (r.get("sector") or None),
        "side": (str(r.get("side", "")).strip() or "LONG"),
        "target_weight": _to_float(r.get("target_weight")),
        "signal_composite_sn": _to_float(r.get("signal_composite_sn")),
        "entry_price": _to_float(r.get("entry_price")),
        "entry_reference_date": (str(r.get("entry_reference_date", "")).strip() or None),
        "price_status_at_creation": (status or None),
        "order_action": ORDER_ACTION_NONE,
    }


def _book_id(alpha_name: str, signal_date: Optional[str], book_size: int) -> str:
    return f"{alpha_name}__{signal_date or 'unknown'}__top{book_size}"


def _benchmark_status(preview: dict[str, Any]) -> dict[str, Any]:
    available = preview.get("spy_benchmark_available_locally")
    expected = preview.get("expected_benchmark") or {}
    return {
        "spy_available_locally": available,
        "reference": (
            "SPY (owned local EOD)" if available
            else "equal-weight-universe reference (SPY not available locally)"
        ),
        "expected_benchmark_signal": expected.get("benchmark_signal"),
        "expected_ic_t_63d": expected.get("ic_t_63d"),
        "expected_quarterly_net_25bps": expected.get("quarterly_net_25bps"),
        "note": (
            "The 10-D composite_sn benchmark is a full-rank long/short backtest; a "
            "long-only paper book only approximates it."
        ),
    }


def _build_book(preview: dict[str, Any], base: Path, book_size: int) -> dict[str, Any]:
    rows = _read_portfolio_rows(base, book_size)
    positions = [_position_from_row(i + 1, r) for i, r in enumerate(rows)]
    priced = sum(1 for p in positions if p["price_status_at_creation"] == "MARKED")
    alpha_name = preview.get("alpha_name") or "composite_sn"
    return {
        "book_id": _book_id(alpha_name, preview.get("signal_date"), book_size),
        "status": "ACTIVE",
        "alpha_name": alpha_name,
        "champion_definition": preview.get("champion_definition"),
        "decision": preview.get("decision"),
        "go_no_go": preview.get("go_no_go"),
        "signal_date": preview.get("signal_date"),
        "cross_section_month": preview.get("cross_section_month"),
        "package_date": preview.get("package_date"),
        "book_size": book_size,
        "weighting": preview.get("weighting") or "EQUAL_WEIGHT_LONG_ONLY",
        "holding_horizon_trading_days": preview.get("holding_horizon_trading_days"),
        "rebalance_cadence": preview.get("rebalance_cadence"),
        "next_rebalance_target": preview.get("next_rebalance_target"),
        "n_positions": len(positions),
        "priced_count": priced,
        "unpriced_count": len(positions) - priced,
        "positions": positions,
        "benchmark_status": _benchmark_status(preview),
        "caveats": preview.get("caveats") or [],
        "provenance": {
            "package_dir": str(base),
            "package_json": str(base / PACKAGE_JSON_NAME),
            "portfolio_csv": str(base / _portfolio_file(book_size)),
            "built_from_phase": "13-A",
        },
        "order_action": ORDER_ACTION_NONE,
    }


# ---------------------------------------------------------------------------
# Marking a book against the latest package prices (read-only)
# ---------------------------------------------------------------------------

def _current_marks(base: Path, book_size: int) -> dict[str, dict[str, Any]]:
    """{ticker: {current_price, current_price_date, paper_return_pct, price_status}}."""
    marks: dict[str, dict[str, Any]] = {}
    for r in _read_portfolio_rows(base, book_size):
        ticker = str(r.get("ticker", "")).strip()
        if not ticker:
            continue
        marks[ticker] = {
            "current_price": _to_float(r.get("current_price")),
            "current_price_date": (str(r.get("current_price_date", "")).strip() or None),
            "paper_return_pct": _to_float(r.get("paper_return_pct")),
            "price_status": (str(r.get("price_status", "")).strip() or None),
        }
    return marks


def _mark_positions(book: dict[str, Any], marks: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    marked: list[dict[str, Any]] = []
    for p in book.get("positions", []):
        ticker = p.get("ticker")
        m = marks.get(ticker, {})
        ret = m.get("paper_return_pct")
        covered = (m.get("price_status") == "MARKED") and (ret is not None)
        marked.append({
            "ticker": ticker,
            "sector": p.get("sector"),
            "source_rank": p.get("source_rank"),
            "entry_price": p.get("entry_price"),
            "current_price": m.get("current_price"),
            "current_price_date": m.get("current_price_date"),
            "paper_return_pct": _round(ret, 4) if ret is not None else None,
            "price_status": m.get("price_status"),
            "covered": bool(covered),
            "order_action": ORDER_ACTION_NONE,
        })
    return marked


def _summarize_marks(marked: list[dict[str, Any]]) -> dict[str, Any]:
    covered = [p for p in marked if p["covered"] and p["paper_return_pct"] is not None]
    returns = [p["paper_return_pct"] for p in covered]
    n_up = sum(1 for x in returns if x > 0)
    n_down = sum(1 for x in returns if x < 0)
    avg = (sum(returns) / len(returns)) if returns else None
    ranked = sorted(covered, key=lambda p: p["paper_return_pct"], reverse=True)
    cov_n = len(covered)

    def _slim(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [{
            "ticker": p["ticker"],
            "paper_return_pct": p["paper_return_pct"],
            "contribution_to_avg_pct": (
                _round(p["paper_return_pct"] / cov_n, 4) if cov_n else None
            ),
        } for p in items]

    return {
        "covered_count": cov_n,
        "missing_count": len(marked) - cov_n,
        "total_count": len(marked),
        "average_return_pct": _round(avg, 4),
        "median_return_pct": _round(_median(returns), 4) if returns else None,
        "min_return_pct": (min(returns) if returns else None),
        "max_return_pct": (max(returns) if returns else None),
        "n_up": n_up,
        "n_down": n_down,
        "hit_rate_pct": (_round(100.0 * n_up / len(returns), 2) if returns else None),
        "best_contributors": _slim(ranked[:5]),
        "worst_contributors": _slim(list(reversed(ranked))[:5]),
    }


# ---------------------------------------------------------------------------
# Public service 1 — GET book (read-only)
# ---------------------------------------------------------------------------

def load_current_alpha_book(
    book_dir: Optional[Union[str, Path]] = None,
) -> dict[str, Any]:
    """Return the persisted active paper book, or a NO_PAPER_BOOK_YET status.

    Read-only: reads only the local JSON store. Never raises for a missing book
    or a missing package — a missing book is a normal, reported state. Also reports
    every saved book id (TOP 25 / TOP 50 coexist) and the active book's own
    snapshot count.
    """
    store_dir = _resolve_book_dir(book_dir)
    store, warnings = _read_books_store(store_dir)
    snapshots, snap_err = _read_snapshots(store_dir)
    if snap_err:
        warnings.append(snap_err)

    active = _active_book(store)
    available = list((store.get("books") or {}).keys())

    if active is None:
        payload: dict[str, Any] = {
            "status": "NO_PAPER_BOOK_YET",
            "book": None,
            "active_book_id": None,
            "available_book_ids": available,
            "guidance": (
                "No paper book has been saved yet. Choose TOP 25 or TOP 50, use "
                "Preview Create Paper Book to review the proposed book, then Save "
                "Paper Book to persist it to the local paper-tracking store."
            ),
            "store_dir": str(store_dir),
            "books_file": str(store_dir / BOOKS_FILE),
            "book_file": str(store_dir / BOOK_FILE),
            "warnings": warnings,
            "loaded_at": _iso_now(),
        }
        payload.update(_safety_block())
        return payload

    book_id = active.get("book_id")
    book_snaps = [s for s in snapshots if s.get("book_id") == book_id]

    payload = {
        "status": "ACTIVE_PAPER_BOOK",
        "book": active,
        "active_book_id": book_id,
        "available_book_ids": available,
        "n_snapshots": len(book_snaps),  # this book only
        "n_snapshots_all_books": len(snapshots),
        "last_snapshot_at": (book_snaps[-1].get("snapshot_taken_at") if book_snaps else None),
        "store_dir": str(store_dir),
        "books_file": str(store_dir / BOOKS_FILE),
        "book_file": str(store_dir / BOOK_FILE),
        "warnings": warnings,
        "explicit_notice": _PAPER_ONLY_NOTICE,
        "loaded_at": _iso_now(),
    }
    payload.update(_safety_block())
    return payload


# ---------------------------------------------------------------------------
# Public service 2 — POST preview-create (preview = no write, commit = save)
# ---------------------------------------------------------------------------

def preview_or_create_current_alpha_book(
    package_dir: Optional[Union[str, Path]] = None,
    *,
    book_size: int = DEFAULT_BOOK_SIZE,
    commit: bool = False,
    book_dir: Optional[Union[str, Path]] = None,
) -> dict[str, Any]:
    """Build the proposed paper book from the Phase 13-A package.

    ``commit=False`` (default) is a pure preview and writes NOTHING. ``commit=True``
    merges the book into the multi-book store (paper_books.json) as the active book
    and refreshes the legacy mirror — a book of the OTHER size that was saved
    earlier is kept, not discarded. Raises :class:`CurrentAlphaPreviewError`
    (mapped to 503) if the Phase 13-A package is missing.
    """
    preview = load_current_alpha_preview(package_dir)  # validates -> may raise (503)
    base = _resolve_package_dir(package_dir)
    store_dir = _resolve_book_dir(book_dir)

    size, size_warning = _normalize_book_size(book_size)
    warnings: list[str] = []
    if size_warning:
        warnings.append(size_warning)

    book = _build_book(preview, base, size)
    book["created_at"] = _iso_now()

    store, serr = _read_books_store(store_dir)
    warnings.extend(serr)
    existing_same_id = store["books"].get(book["book_id"]) is not None
    other_ids = [b for b in store["books"] if b != book["book_id"]]

    wrote = False
    if commit:
        store["books"][book["book_id"]] = book
        store["active_book_id"] = book["book_id"]
        _write_books_store(store_dir, store)
        wrote = True
        if other_ids:
            warnings.append(
                "Other paper books remain saved: "
                + ", ".join(sorted(other_ids))
                + ". Each book keeps its own separate PnL history."
            )
        if existing_same_id:
            warnings.append(
                "A paper book with this id was refreshed; its PnL snapshots "
                "reference the same book_id and are retained."
            )

    if book["unpriced_count"]:
        warnings.append(
            f"{book['unpriced_count']} of {book['n_positions']} positions have no "
            "local entry price yet; they are held in the book but excluded from PnL "
            "until an owned price refresh marks them."
        )
    warnings.extend(book.get("caveats") or [])

    available = list((store.get("books") or {}).keys())
    payload: dict[str, Any] = {
        "mode": "COMMIT" if commit else "PREVIEW",
        "action": "SAVED_PAPER_BOOK" if commit else "PREVIEW_ONLY_NOT_WRITTEN",
        "book": book,
        "replaced_existing_book": bool(existing_same_id and commit),
        "active_book_id": (store.get("active_book_id") if commit else None),
        "available_book_ids": available,
        "store_dir": str(store_dir),
        "books_file": str(store_dir / BOOKS_FILE),
        "book_file": str(store_dir / BOOK_FILE),
        "explicit_notice": _PAPER_ONLY_NOTICE,
        "warnings": warnings,
        "loaded_at": _iso_now(),
    }
    payload.update(_safety_block(
        wrote_store=wrote,
        store_kind="PAPER_BOOK" if wrote else None,
    ))
    return payload


# ---------------------------------------------------------------------------
# Public service 3 — POST snapshot-preview (commit = write one PnL snapshot)
# ---------------------------------------------------------------------------

def _build_snapshot(
    book: dict[str, Any],
    marked: list[dict[str, Any]],
    summary: dict[str, Any],
    *,
    sequence: int,
    as_of_price_date: Optional[str],
    observation_date: str,
    mark_age: Optional[int],
    mark_freshness: str,
) -> dict[str, Any]:
    book_id = book.get("book_id")
    return {
        "snapshot_id": f"{book_id}#{as_of_price_date or 'no-date'}",
        "sequence": sequence,
        "book_id": book_id,
        "book_size": int(book.get("book_size") or DEFAULT_BOOK_SIZE),
        # Observation (when taken) and price-mark date (what it reflects) are
        # deliberately separate so a stale mark cannot masquerade as today's PnL.
        "snapshot_date": observation_date,
        "observation_date": observation_date,
        "snapshot_taken_at": _iso_now(),
        "as_of_price_date": as_of_price_date,
        "mark_age_calendar_days": mark_age,
        "mark_freshness_status": mark_freshness,
        "coverage": {
            "covered_count": summary["covered_count"],
            "missing_count": summary["missing_count"],
            "total_count": summary["total_count"],
        },
        "average_return_pct": summary["average_return_pct"],
        "median_return_pct": summary["median_return_pct"],
        "hit_rate_pct": summary["hit_rate_pct"],
        "n_up": summary["n_up"],
        "n_down": summary["n_down"],
        "best_contributors": summary["best_contributors"],
        "worst_contributors": summary["worst_contributors"],
        "positions": marked,
        "benchmark_status": book.get("benchmark_status"),
        "price_source": "EODHD_LOCAL_EOD(adjusted_close)",
        "mark_method": "package paper_return_pct from owned local EOD; no live market call",
        "order_action": ORDER_ACTION_NONE,
    }


def snapshot_current_alpha_book(
    package_dir: Optional[Union[str, Path]] = None,
    *,
    commit: bool = False,
    book_dir: Optional[Union[str, Path]] = None,
) -> dict[str, Any]:
    """Compute today's paper PnL snapshot for the active saved book.

    Marks the ACTIVE book (choose which book is active by saving TOP 25 or TOP 50).
    ``commit=False`` previews the snapshot without writing. ``commit=True`` appends
    it to ``pnl_snapshots.json`` (and only that file).

    Stale-mark de-dup: a snapshot is identified by (book_id, as_of_price_date). If a
    committed snapshot already exists for the active book at the same owned price
    date, this does NOT append another — it returns
    SNAPSHOT_SKIPPED_NO_NEW_PRICE_DATE and writes nothing, so repeated clicks before
    an owned-price refresh cannot fabricate a fake daily curve.

    Requires a saved book; if none exists returns a controlled NO_PAPER_BOOK_YET
    status (HTTP 200, no crash). Raises :class:`CurrentAlphaPreviewError` (503) only
    if the package is missing.
    """
    store_dir = _resolve_book_dir(book_dir)
    store, warnings = _read_books_store(store_dir)
    book = _active_book(store)

    if book is None:
        payload: dict[str, Any] = {
            "mode": "COMMIT" if commit else "PREVIEW",
            "status": "NO_PAPER_BOOK_YET",
            "action": "NO_PAPER_BOOK_YET",
            "snapshot": None,
            "guidance": (
                "Save a paper book first (choose TOP 25 or TOP 50, Preview Create "
                "Paper Book -> Save Paper Book); a PnL snapshot marks the active "
                "book's positions."
            ),
            "store_dir": str(store_dir),
            "warnings": warnings,
            "loaded_at": _iso_now(),
        }
        payload.update(_safety_block())
        return payload

    preview = load_current_alpha_preview(package_dir)  # validates -> may raise (503)
    base = _resolve_package_dir(package_dir)
    book_id = book.get("book_id")
    book_size = int(book.get("book_size") or DEFAULT_BOOK_SIZE)

    marks = _current_marks(base, book_size)
    marked = _mark_positions(book, marks)
    summary = _summarize_marks(marked)

    snapshots, snap_err = _read_snapshots(store_dir)
    if snap_err:
        warnings.append(snap_err)
    book_snaps = [s for s in snapshots if s.get("book_id") == book_id]

    as_of_price_date = preview.get("package_date")
    observation_date = _today_iso()
    mark_age, mark_freshness = _mark_freshness(as_of_price_date, observation_date)

    if mark_freshness == "STALE_MARK_REJECT":
        warnings.append(
            f"Price mark is {mark_age} calendar days old (> "
            f"{MARK_FRESHNESS_REJECT_DAYS}): this is NOT current daily PnL; it "
            f"reflects the last owned price date {as_of_price_date}. The alpha "
            "signal is not rejected — only the claim that this is today's market PnL."
        )
    elif mark_freshness == "STALE_MARK_WARNING":
        warnings.append(
            f"Price mark is {mark_age} calendar days old (> "
            f"{MARK_FRESHNESS_WARN_DAYS}): treat as a monitoring mark, not current "
            "daily PnL, until an owned price refresh."
        )
    if summary["covered_count"] == 0:
        warnings.append(
            "No positions are currently priced from owned local EOD; the snapshot "
            "records zero covered names until a price refresh."
        )

    # Stale-mark de-dup: one committed snapshot per (book_id, as_of_price_date).
    existing = next(
        (s for s in book_snaps if s.get("as_of_price_date") == as_of_price_date), None
    )
    if existing is not None:
        warnings.append(
            "No new owned price date is available. Daily PnL was not advanced."
        )
        payload = {
            "mode": "COMMIT" if commit else "PREVIEW",
            "action": "SNAPSHOT_SKIPPED_NO_NEW_PRICE_DATE",
            "status": "SNAPSHOT_SKIPPED_NO_NEW_PRICE_DATE",
            "book_id": book_id,
            "snapshot": existing,
            "as_of_price_date": as_of_price_date,
            "observation_date": observation_date,
            "mark_age_calendar_days": mark_age,
            "mark_freshness_status": mark_freshness,
            "n_snapshots_after": len(book_snaps),
            "store_dir": str(store_dir),
            "snapshots_file": str(store_dir / SNAPSHOTS_FILE),
            "explicit_notice": _PAPER_ONLY_NOTICE,
            "warnings": warnings,
            "loaded_at": _iso_now(),
        }
        payload.update(_safety_block())
        return payload

    snapshot = _build_snapshot(
        book, marked, summary,
        sequence=len(book_snaps) + 1,
        as_of_price_date=as_of_price_date,
        observation_date=observation_date,
        mark_age=mark_age,
        mark_freshness=mark_freshness,
    )

    wrote = False
    if commit:
        snapshots.append(snapshot)
        _atomic_write_json(store_dir / SNAPSHOTS_FILE, {"snapshots": snapshots})
        wrote = True

    payload = {
        "mode": "COMMIT" if commit else "PREVIEW",
        "action": "SNAPSHOT_WRITTEN" if commit else "PREVIEW_ONLY_NOT_WRITTEN",
        "status": "SNAPSHOT_READY",
        "book_id": book_id,
        "snapshot": snapshot,
        "as_of_price_date": as_of_price_date,
        "observation_date": observation_date,
        "mark_age_calendar_days": mark_age,
        "mark_freshness_status": mark_freshness,
        "n_snapshots_after": len(book_snaps) + (1 if commit else 0),
        "store_dir": str(store_dir),
        "snapshots_file": str(store_dir / SNAPSHOTS_FILE),
        "explicit_notice": _PAPER_ONLY_NOTICE,
        "warnings": warnings,
        "loaded_at": _iso_now(),
    }
    payload.update(_safety_block(
        wrote_store=wrote,
        store_kind="PNL_SNAPSHOT" if wrote else None,
    ))
    return payload


# ---------------------------------------------------------------------------
# Public service 4 — GET pnl-history (read-only, isolated per book)
# ---------------------------------------------------------------------------

def _contributors_over_time(snapshots: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Rank tickers by mean paper return across the GIVEN snapshots where covered.

    Callers pass only the selected book's snapshots, so contribution aggregation
    never crosses book_id.
    """
    agg: dict[str, dict[str, Any]] = {}
    for snap in snapshots:
        for p in snap.get("positions", []):
            if not p.get("covered") or p.get("paper_return_pct") is None:
                continue
            ticker = p.get("ticker")
            entry = agg.setdefault(ticker, {"ticker": ticker, "sum": 0.0, "n": 0})
            entry["sum"] += float(p["paper_return_pct"])
            entry["n"] += 1
    scored = [{
        "ticker": v["ticker"],
        "mean_return_pct": _round(v["sum"] / v["n"], 4),
        "appearances": v["n"],
    } for v in agg.values() if v["n"]]
    ranked = sorted(scored, key=lambda x: x["mean_return_pct"], reverse=True)
    return ranked[:5], list(reversed(ranked))[:5]


def _resolve_history_book_id(
    store: dict[str, Any],
    snapshots: list[dict[str, Any]],
    *,
    book_id: Optional[str],
    book_size: Optional[int],
) -> Optional[str]:
    """Pick exactly one book to report history for (never aggregate across books).

    Priority: explicit book_id > explicit book_size > the active book > the most
    recent snapshot's book. Returns None if the requested selection has no data.
    """
    books = store.get("books") or {}
    active = store.get("active_book_id")
    snap_ids = [s.get("book_id") for s in snapshots if s.get("book_id")]
    all_ids = _ordered_unique(list(books.keys()) + snap_ids)

    if book_id and book_id in all_ids:
        return book_id

    size: Optional[int] = None
    if book_size is not None:
        try:
            size = int(book_size)
        except (TypeError, ValueError):
            size = None
    if size in BOOK_SIZES:
        active_book = books.get(active) if active else None
        if isinstance(active_book, dict) and int(active_book.get("book_size") or 0) == size:
            return active
        for bid, bk in books.items():
            if int((bk or {}).get("book_size") or 0) == size:
                return bid
        for s in reversed(snapshots):
            if int(s.get("book_size") or 0) == size:
                return s.get("book_id")
        return None  # requested size has no saved book or snapshots

    if active and active in books:
        return active
    if snap_ids:
        return snap_ids[-1]
    if all_ids:
        return all_ids[0]
    return None


def load_current_alpha_pnl_history(
    book_dir: Optional[Union[str, Path]] = None,
    *,
    book_id: Optional[str] = None,
    book_size: Optional[int] = None,
) -> dict[str, Any]:
    """Return one paper book's PnL history over time (read-only, local store).

    History is ALWAYS isolated to a single selected book (active by default, or by
    ``book_id`` / ``book_size``); TOP 25 and TOP 50 series are never combined, and
    best/worst contributors are computed only within the selected book. The
    financial time-series x-axis is ``as_of_price_date`` (the owned price-mark
    date), not the observation date.
    """
    store_dir = _resolve_book_dir(book_dir)
    store, warnings = _read_books_store(store_dir)
    snapshots, snap_err = _read_snapshots(store_dir)
    if snap_err:
        warnings.append(snap_err)

    books = store.get("books") or {}
    available = _available_book_ids(store, snapshots)

    if not available:
        payload: dict[str, Any] = {
            "status": "NO_PAPER_BOOK_YET",
            "book": None,
            "selected_book_id": None,
            "active_book_id": store.get("active_book_id"),
            "available_book_ids": [],
            "excluded_snapshot_count": 0,
            "n_snapshots": 0,
            "series": [],
            "guidance": (
                "No paper book or snapshots yet. Save a paper book, then use Snapshot "
                "Today to begin building PnL history."
            ),
            "store_dir": str(store_dir),
            "warnings": warnings,
            "loaded_at": _iso_now(),
        }
        payload.update(_safety_block())
        return payload

    selected_id = _resolve_history_book_id(
        store, snapshots, book_id=book_id, book_size=book_size
    )
    selected = [s for s in snapshots if s.get("book_id") == selected_id]
    # Stable financial time-series ordering: by price-mark date, then sequence.
    selected.sort(key=lambda s: (str(s.get("as_of_price_date") or ""), s.get("sequence") or 0))
    excluded = len(snapshots) - len(selected)

    if selected_id is None:
        warnings.append(
            "No saved paper book or snapshots match the requested selection; "
            "nothing to show. Save / snapshot that book size first."
        )
    elif not selected:
        warnings.append(
            f"No PnL snapshots recorded yet for the selected book ({selected_id}). "
            "Use Snapshot Today to begin building its history."
        )
    if excluded:
        warnings.append(
            f"{excluded} snapshot(s) from other paper books are excluded — history "
            "is isolated to a single book and never combines TOP 25 with TOP 50."
        )

    series = [{
        "sequence": s.get("sequence"),
        "book_id": s.get("book_id"),
        # Financial x-axis: the owned price-mark date.
        "as_of_price_date": s.get("as_of_price_date"),
        "observation_date": s.get("observation_date") or s.get("snapshot_date"),
        "snapshot_date": s.get("snapshot_date"),
        "snapshot_taken_at": s.get("snapshot_taken_at"),
        "mark_age_calendar_days": s.get("mark_age_calendar_days"),
        "mark_freshness_status": s.get("mark_freshness_status"),
        "average_return_pct": s.get("average_return_pct"),
        "median_return_pct": s.get("median_return_pct"),
        "hit_rate_pct": s.get("hit_rate_pct"),
        "covered_count": (s.get("coverage") or {}).get("covered_count"),
        "missing_count": (s.get("coverage") or {}).get("missing_count"),
    } for s in selected]

    latest = selected[-1] if selected else None
    best_over_time, worst_over_time = _contributors_over_time(selected)

    if 0 < len(selected) < 2:
        warnings.append(
            "Fewer than two snapshots for this book: PnL history is a single point. "
            "Snapshot Today over multiple owned-price refreshes to build a trend."
        )

    selected_book = books.get(selected_id) if selected_id else None
    book_summary = None
    source_book = selected_book if isinstance(selected_book, dict) else (latest or {})
    if source_book:
        book_summary = {
            "book_id": (source_book.get("book_id") or selected_id),
            "alpha_name": source_book.get("alpha_name"),
            "signal_date": source_book.get("signal_date"),
            "book_size": source_book.get("book_size"),
            "n_positions": source_book.get("n_positions"),
            "status": source_book.get("status"),
            "created_at": source_book.get("created_at"),
        }

    payload = {
        "status": "PNL_HISTORY_READY",
        "book": book_summary,
        "selected_book_id": selected_id,
        "active_book_id": store.get("active_book_id"),
        "available_book_ids": available,
        "excluded_snapshot_count": excluded,
        "n_snapshots": len(selected),  # selected book only
        "series": series,
        "latest_snapshot": latest,
        "best_contributors_over_time": best_over_time,
        "worst_contributors_over_time": worst_over_time,
        "benchmark_status": (
            (latest or {}).get("benchmark_status")
            or (selected_book or {}).get("benchmark_status")
        ),
        "store_dir": str(store_dir),
        "snapshots_file": str(store_dir / SNAPSHOTS_FILE),
        "explicit_notice": _PAPER_ONLY_NOTICE,
        "warnings": warnings,
        "loaded_at": _iso_now(),
    }
    payload.update(_safety_block())
    return payload


__all__ = [
    "load_current_alpha_book",
    "preview_or_create_current_alpha_book",
    "snapshot_current_alpha_book",
    "load_current_alpha_pnl_history",
    "CurrentAlphaBookError",
    "CurrentAlphaPreviewError",
    "BOOK_DIR_ENV_VAR",
    "DEFAULT_BOOK_DIR",
    "BOOKS_FILE",
    "BOOK_FILE",
    "SNAPSHOTS_FILE",
    "ORDER_ACTION_NONE",
    "BOOK_SIZES",
    "DEFAULT_BOOK_SIZE",
    "MARK_FRESHNESS_WARN_DAYS",
    "MARK_FRESHNESS_REJECT_DAYS",
]
