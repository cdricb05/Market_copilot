"""api/paper_trading_desk.py - Phase 27A production-like PAPER trading workflow.

Extends the validated Phase 25/26 platform past "Portfolio Proposal" through the complete
operational lifecycle of a professional PAPER investment desk:

    Market Refresh -> Portfolio Proposal -> Manager Review -> Approve Proposal (paper-alpha
    snapshot confirm, Phase 25) -> Generate Paper Orders -> Preview Execution -> Manual
    Confirmation -> Paper Fills -> Holdings / Cash / NAV updated -> Forward Performance ->
    Attribution -> Decision Journal.

STRICT SAFETY CONTRACT (enforced, on every payload):
    * PAPER ONLY. No live orders, no broker, no automation, no background execution, no
      scheduled tasks. Every mutation requires an explicit manual confirmation token.
    * No model change, no model-weight change, no research promotion. The desk CONSUMES the
      already-confirmed paper-alpha snapshot (Phase 25 ledger); it never re-ranks or re-scores.
    * ALL HISTORY APPEND ONLY. Ledger rows are never edited or deleted; every row carries a
      sha256 chain hash so any rewrite of history is detectable (verify_ledger).
    * NO DATABASE TABLES. Workstream K allows ledgers "if required" - they are implemented as
      dedicated local append-only JSON ledgers (the established Phase 13-F / 25 architecture),
      OUTSIDE the git tree, and the existing PostgreSQL signal/decision/order/fill tables are
      never touched.

EXECUTION MODEL (Workstream B) - ONE deterministic model, default and only implemented mode:
    NEXT_CLOSE. An order approved on calendar date A fills at the FIRST completed owned EOD
    close with date >= A that was NOT yet present in the desk mark store at approval time
    (``marks_latest_at_approval`` is recorded on the SUBMITTED transition). This double guard
    makes fills provably hindsight-free: the fill price was unknowable to the desk when the
    order was confirmed. Fill prices are the owned-EODHD adjusted closes as recorded in the
    mark store at settlement time and are embedded immutably in the fill row - fills are never
    rewritten, never random, never backdated. NEXT_OPEN / NEXT_VWAP are declared but NOT
    implemented (owned data has EOD closes only) and cannot be selected. The execution model
    is frozen into the paper book record at creation.

MARK DATA: the desk keeps its own local mark store (a provider CACHE, not a ledger),
refreshed ONLY by an explicit manual confirmed refresh through the EXISTING owned-EODHD
transport (the same injectable client Phase 13-G / 19 reuse). A fixture env / injectable
downloader keeps every test offline. Only completed sessions (date < today) are ever stored.
Historical performance rows are appended once per completed date and NEVER recomputed; later
dividend re-adjustment of the provider series does not restate recorded history (the known,
benign Phase 19 drift). Dividends are not otherwise modeled.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import multi_horizon_ledger as mhz_ledger
from paper_trader.api import multi_horizon_registry as mreg
from paper_trader.api.current_alpha_tournament_sync import (
    TournamentSyncBlocked,
    _classify_provider_error,
    _clean_symbol,
    _completed_bars,
    _normalize_bars,
    _FATAL_BLOCKS,
)

PHASE = "27A"

# --------------------------------------------------------------------------- #
# Store location (env-overridable; outside the git tree by default)
# --------------------------------------------------------------------------- #
DESK_DIR_ENV = "PAPER_TRADER_DESK_DIR"
DEFAULT_DESK_DIR = Path.home() / ".paper_trader" / "paper_trading_desk"

#: Test seam: a JSON of {clean_symbol: [{"date":..,"adjusted_close":..}, ...]} used as an
#: OFFLINE downloader so tests never touch the network / never need a key.
MARKS_FIXTURE_ENV = "PAPER_TRADER_DESK_MARKS_FIXTURE"

BOOKS_FILE = "paper_books.json"
ORDERS_FILE = "paper_orders.json"
FILLS_FILE = "paper_fills.json"
JOURNAL_FILE = "decision_journal.json"
PERFORMANCE_FILE = "forward_performance.json"
TIMELINE_FILE = "execution_timeline.json"
MARKS_FILE = "desk_marks.json"          # provider CACHE - documented non-ledger

LEDGER_FILES = (BOOKS_FILE, ORDERS_FILE, FILLS_FILE, JOURNAL_FILE, PERFORMANCE_FILE,
                TIMELINE_FILE)

# Explicit manual confirmation tokens - one per mutating desk action.
GEN_CONFIRM_TOKEN = "CONFIRM_PAPER_DESK_CREATE_ORDERS"
EXEC_CONFIRM_TOKEN = "CONFIRM_PAPER_DESK_SUBMIT_ORDERS"
REFRESH_CONFIRM_TOKEN = "CONFIRM_PAPER_DESK_REFRESH"
CANCEL_CONFIRM_TOKEN = "CONFIRM_PAPER_DESK_CANCEL"

# Order lifecycle (Workstream A). Transitions are append-only events; records are immutable.
ST_PROPOSED = "PROPOSED"
ST_APPROVED = "APPROVED"
ST_SUBMITTED = "SUBMITTED"
ST_FILLED = "FILLED"
ST_CANCELLED = "CANCELLED"
ST_EXPIRED = "EXPIRED"
ORDER_STATUSES = (ST_PROPOSED, ST_APPROVED, ST_SUBMITTED, ST_FILLED, ST_CANCELLED, ST_EXPIRED)
_TERMINAL = {ST_FILLED, ST_CANCELLED, ST_EXPIRED}
_ALLOWED_TRANSITIONS = {
    ST_PROPOSED: {ST_APPROVED, ST_CANCELLED},
    ST_APPROVED: {ST_SUBMITTED, ST_CANCELLED},
    ST_SUBMITTED: {ST_FILLED, ST_CANCELLED, ST_EXPIRED},
}

# Paper order sides - never plain BUY/SELL (these are simulated paper instructions).
SIDE_BUY = "PAPER_BUY"
SIDE_SELL = "PAPER_SELL"

# Execution model (Workstream B): one deterministic implemented mode, frozen per book.
EXECUTION_MODEL_DEFAULT = "NEXT_CLOSE"
EXECUTION_MODELS = {
    "NEXT_CLOSE": "IMPLEMENTED_DEFAULT",
    "NEXT_OPEN": "NOT_IMPLEMENTED_NO_OWNED_OPEN_MARKS",
    "NEXT_VWAP": "NOT_IMPLEMENTED_NO_OWNED_INTRADAY_MARKS",
}
EXECUTION_MODEL_DOC = (
    "NEXT_CLOSE: an order approved on calendar date A fills at the first completed owned EOD "
    "close with date >= A that was not yet in the desk mark store at approval time. No "
    "hindsight, no random fills, fills never rewritten.")

# Frozen book economics (Workstream D) - the platform's existing 25 bps round-trip assumption.
COST_BPS_PER_SIDE = 12.5
COST_RATE_PER_SIDE = COST_BPS_PER_SIDE / 10000.0
DEFAULT_INITIAL_CAPITAL = 1_000_000.0
BOOK_CURRENCY = "USD_PAPER"
REVIEW_CADENCE = "monthly"
BENCHMARK_TICKER = "SPY"
#: A SUBMITTED order expires after this many completed benchmark sessions without a fill
#: (its ticker has no owned mark) - it is never silently retried forever.
EXPIRY_TRADING_DAYS = 5
#: Mark-fetch lookback when no book exists yet (calendar days before today).
DEFAULT_MARK_LOOKBACK_DAYS = 45
#: Phase 27B.1: the refresh fetch window always reaches at least this many calendar
#: days before the required completed date. Root cause of the "marks through None"
#: false success: a fetch that started AT the snapshot market date on the refresh
#: day contained zero completed sessions, so the store was written with a null
#: latest date and an empty series while the refresh still reported success.
MIN_MARK_WINDOW_DAYS = 14

PRIMARY_MODEL_ID = "fundamental_momentum_50_50_v1"

# Statuses returned by mutating endpoints.
S_OK = "PAPER_DESK_OK"
S_CONFIRM_REQUIRED = "PAPER_DESK_CONFIRM_REQUIRED"
S_NO_PROPOSAL = "NO_CONFIRMED_PROPOSAL"
S_MARKS_REQUIRED = "DESK_MARKS_REQUIRED"
S_DUPLICATE = "ORDERS_ALREADY_EXIST"
S_NO_CHANGES = "NO_CHANGES_REQUIRED"
S_NO_OPEN_ORDERS = "NO_OPEN_ORDERS"
S_BLOCKED = "DESK_PROVIDER_BLOCKED"
#: Phase 27B.1: the refresh completed but did NOT produce valid sizing marks
#: (null resulting date, nothing priced, or the mark date is behind the required
#: latest completed market date). Never reported as success.
S_MARKS_BLOCKED = "DESK_MARK_REFRESH_BLOCKED"
NEXT_ACTION_REPAIR = "REPAIR_OR_REFRESH_MARK_SOURCE"

_today_override: Optional[str] = None   # test seam - monkeypatch for deterministic dates

SAFETY_BADGES = ["PAPER ORDERS ONLY", "NO LIVE ORDERS", "NO BROKER", "AUTOMATION OFF",
                 "MANUAL CONFIRMATION", "NO LIVE PROMOTION"]


def desk_safety(performed_write: bool = False) -> dict:
    """The Phase 27A safety block attached to every desk payload."""
    return {
        "paper_only": True,
        "paper_orders_only": True,
        "orders_enabled": False,            # LIVE orders remain disabled, always
        "live_orders_enabled": False,
        "broker_enabled": False,
        "automation_enabled": False,
        "background_execution": False,
        "scheduled_tasks": False,
        "champion_replaced": False,
        "model_weights_changed": False,
        "research_promoted": False,
        "history_rewritten": False,
        "append_only": True,
        "manual_confirmation_required": True,
        "performed_write": bool(performed_write),
        "no_broker": True,
        "no_live_orders": True,
        "no_automation": True,
        "safety_badges": list(SAFETY_BADGES),
    }


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today(today: Optional[str] = None) -> str:
    if today:
        return str(today)[:10]
    if _today_override:
        return _today_override
    return datetime.now(timezone.utc).date().isoformat()


def _f(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _r2(x):
    return None if x is None else round(float(x), 2)


def _r4(x):
    return None if x is None else round(float(x), 4)


def _r6(x):
    return None if x is None else round(float(x), 6)


def _desk_dir(desk_dir=None) -> Path:
    if desk_dir is not None:
        return Path(desk_dir)
    env = os.environ.get(DESK_DIR_ENV)
    return Path(env) if env else DEFAULT_DESK_DIR


def _atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, indent=1, sort_keys=False)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def _read_json(path: Path) -> Optional[Any]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# Append-only ledger primitives (chain-hashed; rewrites are detectable)
# --------------------------------------------------------------------------- #
def _row_hash(prev_hash: str, core: dict) -> str:
    blob = prev_hash + json.dumps(core, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:24]


def _read_ledger(sdir: Path, fname: str) -> list[dict]:
    obj = _read_json(sdir / fname)
    if isinstance(obj, dict) and isinstance(obj.get("rows"), list):
        return obj["rows"]
    return []


def _append_ledger(sdir: Path, fname: str, new_rows: list[dict]) -> list[dict]:
    """APPEND-ONLY: existing rows are re-written byte-identically; each new row gets a
    monotonically increasing seq and a sha256 chain hash over (prev_hash + row core)."""
    rows = _read_ledger(sdir, fname)
    prev = rows[-1]["chain_hash"] if rows else "GENESIS"
    seq = (rows[-1]["seq"] + 1) if rows else 1
    appended = []
    for core in new_rows:
        core = dict(core)
        core["seq"] = seq
        core["recorded_at"] = _iso_now()
        core["chain_hash"] = _row_hash(prev, {k: v for k, v in core.items() if k != "chain_hash"})
        prev = core["chain_hash"]
        seq += 1
        rows.append(core)
        appended.append(core)
    _atomic_write_json(sdir / fname, {"phase": PHASE, "ledger": fname, "append_only": True,
                                      "updated_at": _iso_now(), "rows": rows})
    return appended


def verify_ledger(sdir: Path, fname: str) -> dict:
    """Recompute the chain: any edited/removed historical row breaks it."""
    rows = _read_ledger(sdir, fname)
    prev = "GENESIS"
    for i, row in enumerate(rows):
        expect = _row_hash(prev, {k: v for k, v in row.items() if k != "chain_hash"})
        if row.get("chain_hash") != expect:
            return {"ledger": fname, "intact": False, "broken_at_seq": row.get("seq"),
                    "n_rows": len(rows)}
        prev = row["chain_hash"]
    return {"ledger": fname, "intact": True, "broken_at_seq": None, "n_rows": len(rows)}


def verify_all_ledgers(desk_dir=None) -> dict:
    sdir = _desk_dir(desk_dir)
    reports = [verify_ledger(sdir, f) for f in LEDGER_FILES]
    return {"all_intact": all(r["intact"] for r in reports), "ledgers": reports}


# --------------------------------------------------------------------------- #
# Mark store (provider CACHE - not a ledger; completed sessions only)
# --------------------------------------------------------------------------- #
Downloader = Callable[[str, str], Any]


def _fixture_downloader(fixture_path: Path) -> Downloader:
    data = _read_json(fixture_path)
    table = data if isinstance(data, dict) else {}

    def _get(symbol: str, _start: str) -> Any:
        return table.get(symbol, table.get(_clean_symbol(symbol), []))

    return _get


def _live_downloader() -> Downloader:
    """The EXISTING owned-EODHD transport (same client as Phase 13-G / 19). Lazily imported;
    the key is read from the environment by the transport itself - never handled here."""
    import sys
    from paper_trader.api.current_alpha_daily_refresh import _resolve_research_repo_dir
    repo = str(_resolve_research_repo_dir(None))
    if repo not in sys.path:
        sys.path.insert(0, repo)
    from research import run_phase8u_eodhd_price_universe_expansion as u8  # lazy
    return u8._eod_live_get


def _resolve_downloader(downloader: Optional[Downloader]) -> tuple[Downloader, str]:
    if downloader is not None:
        return downloader, "INJECTED"
    fixture = os.environ.get(MARKS_FIXTURE_ENV)
    if fixture:
        return _fixture_downloader(Path(fixture)), "FIXTURE"
    return _live_downloader(), "OWNED_EODHD_LIVE"


def read_marks(desk_dir=None) -> dict:
    sdir = _desk_dir(desk_dir)
    obj = _read_json(sdir / MARKS_FILE)
    if isinstance(obj, dict) and isinstance(obj.get("series"), dict):
        return obj
    return {"series": {}, "latest_completed_date": None, "updated_at": None, "source": None}


def marks_latest_date(marks: dict) -> Optional[str]:
    return marks.get("latest_completed_date")


def _series_price_at_or_before(series: list, as_of: str) -> Optional[tuple[str, float]]:
    best = None
    for d, v in series:
        if d <= as_of and v is not None and (best is None or d > best[0]):
            best = (d, v)
    return best


def _first_close_on_or_after(series: list, on_or_after: str,
                             strictly_after_store: Optional[str]) -> Optional[tuple[str, float]]:
    """NEXT_CLOSE fill resolution: first completed close with date >= approval date AND
    (no-hindsight guard) strictly after the store's latest date at approval time."""
    for d, v in series:  # series is date-sorted ascending
        if v is None:
            continue
        if d < on_or_after:
            continue
        if strictly_after_store is not None and d <= strictly_after_store:
            continue
        return (d, v)
    return None


def sync_marks(*, tickers: list[str], start: str, desk_dir=None,
               downloader: Optional[Downloader] = None, today: Optional[str] = None,
               completed_through: Optional[str] = None) -> dict:
    """Fetch completed EOD closes for the given tickers (+ SPY) into the desk mark store.
    When ``completed_through`` is given (the clock-resolved latest COMPLETED session, e.g.
    today after the US close), bars through that date are kept; otherwise only bars
    strictly before `today` are kept (a same-day bar may be incomplete). The store
    is a provider cache: series are replaced whole per sync; ledgers embed the prices they
    used, so recorded history is never restated by a later re-adjusted series.
    Phase 27B.1: if nothing was ever priced (empty resulting series) the store is NOT
    rewritten and ``store_written`` is False - an empty cache is never a success."""
    sdir = _desk_dir(desk_dir)
    tref = _today(today)
    tdate = date.fromisoformat(tref)
    if completed_through:
        # keep bars <= completed_through (equivalently: strictly before the next day)
        cutoff = date.fromisoformat(str(completed_through)[:10]) + timedelta(days=1)
    else:
        cutoff = tdate
    dl, source = _resolve_downloader(downloader)
    fetch = sorted(set([t.strip().upper() for t in tickers if t] + [BENCHMARK_TICKER]))
    prior = read_marks(desk_dir)
    series: dict[str, list] = {k: list(v) for k, v in (prior.get("series") or {}).items()}
    per_ticker, failed = [], []
    for tk in fetch:
        try:
            payload = dl(_clean_symbol(tk), start)
        except Exception as exc:  # noqa: BLE001 - sanitized taxonomy below
            enum = _classify_provider_error(exc)
            if enum in _FATAL_BLOCKS:
                raise TournamentSyncBlocked(enum, "provider stop: %s"
                                            % getattr(exc, "error_type", "error"))
            failed.append(tk)
            per_ticker.append({"ticker": tk, "status": "ERROR", "n_bars": 0})
            continue
        bars = _completed_bars(_normalize_bars(payload), cutoff)
        if bars:
            series[tk] = [[d, v] for d, v in bars]
        per_ticker.append({"ticker": tk, "status": "OK" if bars else "EMPTY",
                           "n_bars": len(bars),
                           "latest_completed_date": bars[-1][0] if bars else None})
    latest = None
    spy = series.get(BENCHMARK_TICKER) or []
    if spy:
        latest = spy[-1][0]
    else:
        for s in series.values():
            if s and (latest is None or s[-1][0] > latest):
                latest = s[-1][0]
    if not series:
        # Phase 27B.1: nothing priced and nothing carried over - do NOT write an
        # empty store (a null-date cache must never look like a completed sync).
        return {"synced": False, "store_written": False, "source": source,
                "n_tickers": len(fetch), "n_failed": len(failed),
                "failed_tickers": failed, "latest_completed_date": None,
                "per_ticker": per_ticker}
    store = {"phase": PHASE, "kind": "provider_cache_not_a_ledger", "source": source,
             "updated_at": _iso_now(), "reference_today": tref, "fetch_start": start,
             "completed_through": (cutoff - timedelta(days=1)).isoformat(),
             "latest_completed_date": latest, "series": series}
    _atomic_write_json(sdir / MARKS_FILE, store)
    return {"synced": True, "store_written": True, "source": source,
            "n_tickers": len(fetch), "n_failed": len(failed),
            "failed_tickers": failed, "latest_completed_date": latest,
            "per_ticker": per_ticker}


# --------------------------------------------------------------------------- #
# Book + order + fill state folds (state is DERIVED by replaying the ledgers)
# --------------------------------------------------------------------------- #
def _books(sdir: Path) -> list[dict]:
    out = []
    for row in _read_ledger(sdir, BOOKS_FILE):
        if row.get("event") == "BOOK_CREATED":
            out.append(dict(row["book"]))
        elif row.get("event") == "BOOK_STATUS":
            for b in out:
                if b["book_id"] == row.get("book_id"):
                    b["status"] = row.get("to_status", b["status"])
    return out


def open_book(sdir: Path) -> Optional[dict]:
    for b in _books(sdir):
        if b.get("status") == "OPEN":
            return b
    return None


def _orders_state(sdir: Path) -> dict[str, dict]:
    """Fold ORDER_CREATED + ORDER_TRANSITION events into current order records."""
    orders: dict[str, dict] = {}
    for row in _read_ledger(sdir, ORDERS_FILE):
        ev = row.get("event")
        if ev == "ORDER_CREATED":
            o = dict(row["order"])
            o["status"] = ST_PROPOSED
            o["history"] = [{"to_status": ST_PROPOSED, "at": row.get("recorded_at")}]
            orders[o["order_id"]] = o
        elif ev == "ORDER_TRANSITION":
            o = orders.get(row.get("order_id"))
            if o is None:
                continue
            o["status"] = row.get("to_status", o["status"])
            o["history"].append({"to_status": row.get("to_status"),
                                 "at": row.get("recorded_at"),
                                 "detail": row.get("detail")})
            if row.get("to_status") == ST_SUBMITTED:
                o["approval_date"] = row.get("approval_date")
                o["marks_latest_at_approval"] = row.get("marks_latest_at_approval")
    return orders


def _fills(sdir: Path) -> list[dict]:
    return [dict(r["fill"]) for r in _read_ledger(sdir, FILLS_FILE) if "fill" in r]


def book_cash_holdings(book: dict, fills: list[dict],
                       up_to_date: Optional[str] = None) -> tuple[float, dict[str, int]]:
    """Replay immutable fills -> (cash, holdings qty). Fully reproducible from the ledgers."""
    cash = float(book["initial_capital"])
    qty: dict[str, int] = {}
    for f in sorted(fills, key=lambda x: (x.get("fill_date") or "", x.get("fill_id") or "")):
        if f.get("book_id") != book["book_id"]:
            continue
        if up_to_date is not None and (f.get("fill_date") or "") > up_to_date:
            continue
        cash += float(f["net_cash_delta"])
        tk = f["ticker"]
        q = int(f["quantity"])
        qty[tk] = qty.get(tk, 0) + (q if f["side"] == SIDE_BUY else -q)
        if qty.get(tk) == 0:
            qty.pop(tk, None)
    return cash, qty


def book_nav(book: dict, fills: list[dict], marks: dict,
             as_of: Optional[str] = None) -> dict:
    latest = as_of or marks_latest_date(marks)
    cash, qty = book_cash_holdings(book, fills, up_to_date=latest)
    series = marks.get("series") or {}
    invested = 0.0
    missing = []
    for tk, q in sorted(qty.items()):
        at = _series_price_at_or_before(series.get(tk) or [], latest) if latest else None
        if at is None:
            missing.append(tk)
            continue
        invested += q * at[1]
    nav = cash + invested
    return {"as_of_date": latest, "cash": _r2(cash), "invested": _r2(invested),
            "nav": _r2(nav), "holdings": qty, "holdings_count": len(qty),
            "missing_marks": missing}


# --------------------------------------------------------------------------- #
# Proposal -> paper orders (Workstream A)
# --------------------------------------------------------------------------- #
def _latest_confirmed_snapshot(ledger_dir=None) -> Optional[dict]:
    prior = mhz_ledger.latest_confirmed_by_sleeve(ledger_dir)
    combined = prior.get(mreg.SLEEVE_COMBINED) or {}
    sid = combined.get("snapshot_id")
    return mhz_ledger.get_snapshot(sid, ledger_dir) if sid else None


def _snapshot_target(snap: dict) -> tuple[list[str], float, str]:
    blk = (snap.get("sleeves") or {}).get(mreg.SLEEVE_COMBINED) or {}
    cons = list(blk.get("constituents_top25") or [])
    w = _f((blk.get("target_weights") or {}).get("top25")) or (1.0 / len(cons) if cons else 0.0)
    return cons, w, snap.get("market_as_of_date") or ""


def _sector_lookup() -> dict[str, str]:
    """Best-effort sectors from the current engine build (display/attribution only)."""
    try:
        cur = eng.build_current()
        if cur.get("status") != eng.STATUS_READY:
            return {}
        book = cur["books"]["books"].get("fundamental_momentum_50_50_top50") or {}
        out = {c["ticker"]: c.get("sector") or "Unknown" for c in book.get("constituents", [])}
        for tk, sc in (cur.get("combined", {}).get("combined") or {}).items():
            out.setdefault(tk, sc.get("sector") or "Unknown")
        return out
    except Exception:  # noqa: BLE001 - sectors are cosmetic; never block the desk
        return {}


def generate_orders(*, confirm: Optional[str] = None, desk_dir=None, ledger_dir=None,
                    initial_capital: float = DEFAULT_INITIAL_CAPITAL,
                    today: Optional[str] = None) -> dict:
    """Generate PROPOSED paper orders from the latest CONFIRMED paper-alpha snapshot.
    Creates Paper Book #N on first use. Orders do NOT modify holdings (Workstream A)."""
    if confirm != GEN_CONFIRM_TOKEN:
        return {"status": S_CONFIRM_REQUIRED, "performed_write": False,
                "message": "Creating paper orders requires confirm='%s'." % GEN_CONFIRM_TOKEN,
                **desk_safety()}
    sdir = _desk_dir(desk_dir)
    snap = _latest_confirmed_snapshot(ledger_dir)
    if snap is None:
        return {"status": S_NO_PROPOSAL, "performed_write": False,
                "message": ("No confirmed paper-alpha snapshot exists. Approve the proposal "
                            "first (Portfolio Manager manual flow: preview + confirm the "
                            "paper-alpha snapshot)."), **desk_safety()}
    target, weight, snap_market_date = _snapshot_target(snap)
    if not target:
        return {"status": S_NO_PROPOSAL, "performed_write": False,
                "message": "The confirmed snapshot has no combined Top-25 constituents.",
                **desk_safety()}
    marks = read_marks(desk_dir)
    latest = marks_latest_date(marks)
    if latest is None:
        return {"status": S_MARKS_REQUIRED, "performed_write": False,
                "message": ("The desk mark store is empty. Run the manual desk data refresh "
                            "first (confirm='%s')." % REFRESH_CONFIRM_TOKEN), **desk_safety()}

    orders = _orders_state(sdir)
    open_non_terminal = [o for o in orders.values() if o["status"] not in _TERMINAL]
    if any(o.get("snapshot_id") == snap.get("snapshot_id") for o in open_non_terminal):
        return {"status": S_DUPLICATE, "performed_write": False,
                "snapshot_id": snap.get("snapshot_id"),
                "message": ("Open paper orders for this snapshot already exist. Preview and "
                            "confirm (or cancel) them instead of re-creating."),
                **desk_safety()}
    if open_non_terminal:
        return {"status": S_DUPLICATE, "performed_write": False,
                "message": ("Open paper orders from a prior proposal exist. Confirm or cancel "
                            "them before generating a new order set."), **desk_safety()}

    book = open_book(sdir)
    created_book = False
    if book is None:
        n = len(_books(sdir)) + 1
        book = {
            "book_id": "paper_book_%d" % n,
            "book_number": n,
            "display_name": "Paper Book #%d" % n,
            "creation_date": _today(today),
            "created_at": _iso_now(),
            "model_id": PRIMARY_MODEL_ID,
            "model_version": (mreg.model_by_id(PRIMARY_MODEL_ID) or {}).get("model_version", "v1"),
            "execution_model": EXECUTION_MODEL_DEFAULT,
            "execution_model_doc": EXECUTION_MODEL_DOC,
            "transaction_cost_bps_per_side": COST_BPS_PER_SIDE,
            "transaction_cost_bps_round_trip": 2 * COST_BPS_PER_SIDE,
            "review_cadence": REVIEW_CADENCE,
            "initial_capital": float(initial_capital),
            "currency": BOOK_CURRENCY,
            "frozen_target_weights": {tk: _r6(weight) for tk in target},
            "snapshot_id": snap.get("snapshot_id"),
            "snapshot_market_date": snap_market_date,
            "benchmark": BENCHMARK_TICKER,
            "status": "OPEN",
            "immutable_record": True,
        }
        _append_ledger(sdir, BOOKS_FILE, [{"event": "BOOK_CREATED", "book": book}])
        created_book = True

    fills = _fills(sdir)
    cash, held = book_cash_holdings(book, fills)
    nav_blk = book_nav(book, fills, marks)
    equity_basis = nav_blk["nav"] if nav_blk["nav"] is not None else float(book["initial_capital"])
    series = marks.get("series") or {}
    sectors = _sector_lookup()

    additions = [tk for tk in target if tk not in held]
    removals = [tk for tk in sorted(held) if tk not in target]
    retained = [tk for tk in target if tk in held]
    if not additions and not removals:
        return {"status": S_NO_CHANGES, "performed_write": created_book,
                "book_id": book["book_id"], "snapshot_id": snap.get("snapshot_id"),
                "message": ("Holdings already match the confirmed snapshot target. "
                            "No paper orders are required."), **desk_safety(created_book)}

    new_orders, blocked, journal_rows, timeline_rows = [], [], [], []
    seq = sum(1 for o in orders) + 1
    for tk in additions + removals:
        side = SIDE_BUY if tk in additions else SIDE_SELL
        ref = _series_price_at_or_before(series.get(tk) or [], latest)
        if side == SIDE_BUY:
            if ref is None:
                blocked.append({"ticker": tk, "side": side,
                                "reason": "NO_OWNED_MARK - no reference close in the desk store."})
                journal_rows.append(_journal_core(book, "ORDER_BLOCKED", tk,
                                                  "Paper order blocked for %s: no owned mark "
                                                  "in the desk store." % tk))
                continue
            qty = int(math.floor(weight * equity_basis / ref[1]))
            if qty < 1:
                blocked.append({"ticker": tk, "side": side,
                                "reason": "QTY_BELOW_ONE_SHARE at the reference close."})
                continue
            reason = "Added because it entered the combined Top-25."
        else:
            qty = int(held.get(tk, 0))
            if qty < 1:
                continue
            reason = "Removed because it exited the combined Top-25."
        order = {
            "order_id": "ord_%s_%03d_%s" % (book["book_id"], seq, tk),
            "book_id": book["book_id"],
            "snapshot_id": snap.get("snapshot_id"),
            "ticker": tk,
            "side": side,
            "quantity": qty,
            "target_weight": _r6(weight if side == SIDE_BUY else 0.0),
            "reference_close": _r6(ref[1]) if (side == SIDE_BUY and ref) else None,
            "reference_close_date": ref[0] if (side == SIDE_BUY and ref) else None,
            "sector": sectors.get(tk, "Unknown"),
            "execution_model": book["execution_model"],
            "reason": reason,
            "created_at": _iso_now(),
        }
        seq += 1
        new_orders.append({"event": "ORDER_CREATED", "order": order})
        journal_rows.append(_journal_core(book, "ORDER_CREATED", tk,
                                          "%s Paper order %s proposed: %s %d shares." %
                                          (reason, order["order_id"], side, qty)))
    for tk in retained:
        journal_rows.append(_journal_core(book, "HOLD", tk,
                                          "Held because it remains inside the hold buffer."))

    appended = _append_ledger(sdir, ORDERS_FILE, new_orders) if new_orders else []
    timeline_rows.append(_timeline_core(book, "ORDERS_PROPOSED",
                                        "%d paper order(s) proposed from snapshot %s "
                                        "(%d blocked). Manual preview + confirmation required."
                                        % (len(appended), snap.get("snapshot_id"), len(blocked))))
    if created_book:
        timeline_rows.insert(0, _timeline_core(book, "BOOK_CREATED",
                                               "%s created (model %s, %s, %.1f bps/side, "
                                               "initial paper capital %.0f %s)."
                                               % (book["display_name"], book["model_id"],
                                                  book["execution_model"], COST_BPS_PER_SIDE,
                                                  book["initial_capital"], BOOK_CURRENCY)))
        journal_rows.insert(0, _journal_core(book, "BOOK_CREATED", None,
                                             "%s created from confirmed snapshot %s."
                                             % (book["display_name"], snap.get("snapshot_id"))))
    _append_ledger(sdir, JOURNAL_FILE, journal_rows)
    _append_ledger(sdir, TIMELINE_FILE, timeline_rows)
    return {"status": S_OK, "performed_write": True, "wrote_to_desk_ledgers_only": True,
            "book_id": book["book_id"], "book_created": created_book,
            "snapshot_id": snap.get("snapshot_id"),
            "orders_created": [r["order"]["order_id"] for r in new_orders],
            "n_orders_created": len(new_orders), "n_blocked": len(blocked), "blocked": blocked,
            "additions": additions, "removals": removals, "retained": retained,
            "equity_basis": _r2(equity_basis), "cash": _r2(cash),
            "reference_marks_date": latest,
            "message": ("%d paper order(s) created in PROPOSED. Nothing fills until the "
                        "separate manual order confirmation." % len(new_orders)),
            **desk_safety(True)}


def _journal_core(book: dict, category: str, ticker: Optional[str], text: str) -> dict:
    return {"entry": {"book_id": book["book_id"], "category": category, "ticker": ticker,
                      "text": text, "rule_based": True, "llm_generated": False}}


def _timeline_core(book: dict, kind: str, summary: str) -> dict:
    return {"event": {"book_id": book["book_id"], "kind": kind, "summary": summary}}


# --------------------------------------------------------------------------- #
# Manual order confirmation (Workstream C) - PROPOSED -> APPROVED -> SUBMITTED
# --------------------------------------------------------------------------- #
def confirm_orders(*, confirm: Optional[str] = None, desk_dir=None,
                   today: Optional[str] = None) -> dict:
    if confirm != EXEC_CONFIRM_TOKEN:
        return {"status": S_CONFIRM_REQUIRED, "performed_write": False,
                "message": "Submitting paper orders requires confirm='%s'." % EXEC_CONFIRM_TOKEN,
                **desk_safety()}
    sdir = _desk_dir(desk_dir)
    book = open_book(sdir)
    orders = _orders_state(sdir)
    proposed = [o for o in orders.values() if o["status"] == ST_PROPOSED]
    if book is None or not proposed:
        return {"status": S_NO_OPEN_ORDERS, "performed_write": False,
                "message": "No PROPOSED paper orders exist to confirm.", **desk_safety()}
    approval_date = _today(today)
    marks_latest = marks_latest_date(read_marks(desk_dir))
    events, journal_rows = [], []
    for o in sorted(proposed, key=lambda x: x["order_id"]):
        events.append({"event": "ORDER_TRANSITION", "order_id": o["order_id"],
                       "from_status": ST_PROPOSED, "to_status": ST_APPROVED,
                       "detail": "Manually approved."})
        events.append({"event": "ORDER_TRANSITION", "order_id": o["order_id"],
                       "from_status": ST_APPROVED, "to_status": ST_SUBMITTED,
                       "approval_date": approval_date,
                       "marks_latest_at_approval": marks_latest,
                       "detail": ("Submitted for NEXT_CLOSE paper execution: fills at the "
                                  "first completed owned close on/after %s not yet in the "
                                  "desk store." % approval_date)})
        journal_rows.append(_journal_core(book, "ORDER_SUBMITTED", o["ticker"],
                                          "Paper order %s submitted (%s %d %s). Awaiting the "
                                          "next completed owned close." %
                                          (o["order_id"], o["side"], o["quantity"], o["ticker"])))
    _append_ledger(sdir, ORDERS_FILE, events)
    _append_ledger(sdir, JOURNAL_FILE, journal_rows)
    _append_ledger(sdir, TIMELINE_FILE,
                   [_timeline_core(book, "ORDERS_SUBMITTED",
                                   "%d paper order(s) manually confirmed and SUBMITTED on %s "
                                   "(NEXT_CLOSE). No fill has occurred yet."
                                   % (len(proposed), approval_date))])
    settle = settle_due_orders(desk_dir=desk_dir, today=today)
    return {"status": S_OK, "performed_write": True, "wrote_to_desk_ledgers_only": True,
            "n_submitted": len(proposed), "approval_date": approval_date,
            "marks_latest_at_approval": marks_latest,
            "settlement": settle,
            "message": ("%d paper order(s) SUBMITTED. Fills occur at the first completed "
                        "owned close on/after %s once a manual desk refresh records it."
                        % (len(proposed), approval_date)),
            **desk_safety(True)}


def cancel_orders(*, confirm: Optional[str] = None, order_ids: Optional[list[str]] = None,
                  desk_dir=None) -> dict:
    if confirm != CANCEL_CONFIRM_TOKEN:
        return {"status": S_CONFIRM_REQUIRED, "performed_write": False,
                "message": "Cancelling paper orders requires confirm='%s'." % CANCEL_CONFIRM_TOKEN,
                **desk_safety()}
    sdir = _desk_dir(desk_dir)
    book = open_book(sdir)
    orders = _orders_state(sdir)
    open_orders = [o for o in orders.values() if o["status"] not in _TERMINAL]
    if order_ids:
        open_orders = [o for o in open_orders if o["order_id"] in set(order_ids)]
    if book is None or not open_orders:
        return {"status": S_NO_OPEN_ORDERS, "performed_write": False,
                "message": "No open paper orders match.", **desk_safety()}
    events, journal_rows = [], []
    for o in sorted(open_orders, key=lambda x: x["order_id"]):
        events.append({"event": "ORDER_TRANSITION", "order_id": o["order_id"],
                       "from_status": o["status"], "to_status": ST_CANCELLED,
                       "detail": "Manually cancelled before any fill."})
        journal_rows.append(_journal_core(book, "ORDER_CANCELLED", o["ticker"],
                                          "Paper order %s cancelled manually; no fill, no "
                                          "position change." % o["order_id"]))
    _append_ledger(sdir, ORDERS_FILE, events)
    _append_ledger(sdir, JOURNAL_FILE, journal_rows)
    _append_ledger(sdir, TIMELINE_FILE,
                   [_timeline_core(book, "ORDERS_CANCELLED",
                                   "%d open paper order(s) manually cancelled." % len(events))])
    return {"status": S_OK, "performed_write": True, "n_cancelled": len(events),
            "cancelled_order_ids": [o["order_id"] for o in open_orders], **desk_safety(True)}


# --------------------------------------------------------------------------- #
# Deterministic settlement (NEXT_CLOSE) + expiry - called ONLY from manual actions
# --------------------------------------------------------------------------- #
def settle_due_orders(*, desk_dir=None, today: Optional[str] = None) -> dict:
    """Fill SUBMITTED orders whose NEXT_CLOSE mark now exists; expire stale ones.
    Deterministic and reproducible: same ledgers + same mark store => same fills."""
    sdir = _desk_dir(desk_dir)
    book = open_book(sdir)
    if book is None:
        return {"n_filled": 0, "n_expired": 0, "fills": [], "expired": []}
    marks = read_marks(desk_dir)
    series = marks.get("series") or {}
    spy_dates = [d for d, _v in (series.get(BENCHMARK_TICKER) or [])]
    orders = _orders_state(sdir)
    submitted = sorted((o for o in orders.values() if o["status"] == ST_SUBMITTED),
                       key=lambda x: x["order_id"])
    fills_rows, order_events, journal_rows, timeline_rows, expired = [], [], [], [], []
    n_fill = 0
    for o in submitted:
        appr = o.get("approval_date") or _today(today)
        guard = o.get("marks_latest_at_approval")
        hit = _first_close_on_or_after(series.get(o["ticker"]) or [], appr, guard)
        if hit is not None:
            fill_date, price = hit
            qty = int(o["quantity"])
            gross = qty * price
            cost = gross * COST_RATE_PER_SIDE
            delta = -(gross + cost) if o["side"] == SIDE_BUY else (gross - cost)
            fill = {
                "fill_id": "fill_%s" % o["order_id"][4:],
                "order_id": o["order_id"], "book_id": o["book_id"],
                "ticker": o["ticker"], "side": o["side"], "quantity": qty,
                "fill_date": fill_date, "fill_price": _r6(price),
                "gross_value": _r2(gross), "transaction_cost": _r4(cost),
                "cost_bps_per_side": COST_BPS_PER_SIDE,
                "net_cash_delta": _r4(delta),
                "execution_model": book["execution_model"],
                "price_source": "OWNED_EODHD_ADJUSTED_CLOSE_AS_RECORDED",
                "no_hindsight_guard": {"approval_date": appr,
                                       "marks_latest_at_approval": guard},
                "immutable": True,
            }
            n_fill += 1
            fills_rows.append({"event": "PAPER_FILL", "fill": fill})
            order_events.append({"event": "ORDER_TRANSITION", "order_id": o["order_id"],
                                 "from_status": ST_SUBMITTED, "to_status": ST_FILLED,
                                 "detail": "Paper fill %s at %s close %.4f."
                                           % (fill["fill_id"], fill_date, price)})
            journal_rows.append(_journal_core(book, "PAPER_FILL", o["ticker"],
                                              "Paper fill: %s %d %s at the %s owned close "
                                              "%.4f (cost %.2f). Holdings and cash updated."
                                              % (o["side"], qty, o["ticker"], fill_date,
                                                 price, cost)))
            continue
        # expiry: too many completed benchmark sessions elapsed with no ticker mark
        elapsed = sum(1 for d in spy_dates if d > (o.get("approval_date") or ""))
        if o.get("approval_date") and elapsed > EXPIRY_TRADING_DAYS:
            order_events.append({"event": "ORDER_TRANSITION", "order_id": o["order_id"],
                                 "from_status": ST_SUBMITTED, "to_status": ST_EXPIRED,
                                 "detail": ("Expired: no owned mark for %s within %d completed "
                                            "sessions after approval."
                                            % (o["ticker"], EXPIRY_TRADING_DAYS))})
            journal_rows.append(_journal_core(book, "ORDER_EXPIRED", o["ticker"],
                                              "Paper order %s expired without a fill (no owned "
                                              "mark within %d completed sessions)."
                                              % (o["order_id"], EXPIRY_TRADING_DAYS)))
            expired.append(o["order_id"])
    if fills_rows:
        _append_ledger(sdir, FILLS_FILE, fills_rows)
    if order_events:
        _append_ledger(sdir, ORDERS_FILE, order_events)
    if journal_rows:
        _append_ledger(sdir, JOURNAL_FILE, journal_rows)
    if n_fill or expired:
        timeline_rows.append(_timeline_core(book, "SETTLEMENT",
                                            "%d paper fill(s) recorded, %d order(s) expired. "
                                            "Holdings, cash and NAV updated (paper only)."
                                            % (n_fill, len(expired))))
        _append_ledger(sdir, TIMELINE_FILE, timeline_rows)
    return {"n_filled": n_fill, "n_expired": len(expired),
            "fills": [r["fill"]["fill_id"] for r in fills_rows], "expired": expired}


# --------------------------------------------------------------------------- #
# Forward performance (Workstream E) - append-only daily rows, never recomputed
# --------------------------------------------------------------------------- #
def append_performance(*, desk_dir=None) -> dict:
    sdir = _desk_dir(desk_dir)
    book = open_book(sdir)
    if book is None:
        return {"n_appended": 0, "dates": []}
    marks = read_marks(desk_dir)
    series = marks.get("series") or {}
    fills = [f for f in _fills(sdir) if f["book_id"] == book["book_id"]]
    if not fills:
        return {"n_appended": 0, "dates": []}
    first_fill = min(f["fill_date"] for f in fills)
    latest = marks_latest_date(marks)
    existing = [r["row"] for r in _read_ledger(sdir, PERFORMANCE_FILE)
                if r.get("row", {}).get("book_id") == book["book_id"]]
    have = {r["date"] for r in existing}
    spy_series = series.get(BENCHMARK_TICKER) or []
    dates = [d for d, _v in spy_series
             if first_fill <= d <= (latest or "") and d not in have]
    if not dates:
        return {"n_appended": 0, "dates": []}
    # rolling context from EXISTING rows (read-only; historical rows never recomputed)
    prev_nav = existing[-1]["nav"] if existing else float(book["initial_capital"])
    peak_nav = max([r["nav"] for r in existing] + [float(book["initial_capital"])])
    bench_base = existing[0]["benchmark_close"] if existing else None
    rows = []
    for d in sorted(dates):
        blk = book_nav(book, fills, marks, as_of=d)
        nav = blk["nav"]
        if nav is None:
            continue
        day_fills = [f for f in fills if f["fill_date"] == d]
        traded = sum(abs(float(f["gross_value"])) for f in day_fills)
        cost = sum(float(f["transaction_cost"]) for f in day_fills)
        spy_at = _series_price_at_or_before(spy_series, d)
        bench_close = _r4(spy_at[1]) if spy_at else None
        if bench_base is None and bench_close is not None:
            bench_base = bench_close
        peak_nav = max(peak_nav, nav)
        row = {
            "book_id": book["book_id"], "date": d,
            "nav": _r2(nav), "cash": blk["cash"], "invested": blk["invested"],
            "holdings": blk["holdings"], "holdings_count": blk["holdings_count"],
            "missing_marks": blk["missing_marks"],
            "benchmark_ticker": BENCHMARK_TICKER, "benchmark_close": bench_close,
            "benchmark_cumulative_return_pct":
                _r4(100.0 * (bench_close / bench_base - 1.0))
                if (bench_close is not None and bench_base) else None,
            "daily_return_pct": _r4(100.0 * (nav / prev_nav - 1.0)) if prev_nav else None,
            "cumulative_return_pct":
                _r4(100.0 * (nav / float(book["initial_capital"]) - 1.0)),
            "drawdown_pct": _r4(100.0 * (nav / peak_nav - 1.0)) if peak_nav else None,
            "turnover_pct": _r4(100.0 * traded / nav) if nav else None,
            "transaction_cost": _r4(cost),
        }
        prev_nav = nav
        rows.append({"row": row})
    if rows:
        _append_ledger(sdir, PERFORMANCE_FILE, rows)
        _append_ledger(sdir, TIMELINE_FILE,
                       [_timeline_core(book, "PERFORMANCE_APPENDED",
                                       "%d forward performance row(s) appended (through %s). "
                                       "Historical rows are never recalculated."
                                       % (len(rows), rows[-1]["row"]["date"]))])
    return {"n_appended": len(rows), "dates": [r["row"]["date"] for r in rows]}


# --------------------------------------------------------------------------- #
# One manual refresh (Workstream I step 1): marks sync + settle + expire + perf
# --------------------------------------------------------------------------- #
def _required_mark_date(*, completed_through: Optional[str] = None,
                        today: Optional[str] = None) -> str:
    """Phase 27B.1: the latest completed owned market date the refresh must reach.

    An explicit ``completed_through`` wins (callers/tests with their own clock).
    With an explicit ``today`` (or the module override) the deterministic legacy
    rule applies: the latest weekday strictly before the reference day. Live (no
    seams) the platform's ET-close clock rule resolves it - the SAME rule the
    alpha-target readiness uses, so the desk mark date and the target market
    date can genuinely align on the refresh evening."""
    if completed_through:
        return str(completed_through)[:10]
    if today is None and _today_override is None:
        from paper_trader.api import alpha_target as at  # lazy: at imports this module
        return at.latest_completed()
    d = date.fromisoformat(_today(today)) - timedelta(days=1)
    while d.weekday() >= 5:  # walk back over the weekend
        d -= timedelta(days=1)
    return d.isoformat()


def refresh_desk(*, confirm: Optional[str] = None, desk_dir=None, ledger_dir=None,
                 downloader: Optional[Downloader] = None,
                 today: Optional[str] = None,
                 completed_through: Optional[str] = None) -> dict:
    if confirm != REFRESH_CONFIRM_TOKEN:
        return {"status": S_CONFIRM_REQUIRED, "performed_write": False,
                "message": "The manual desk refresh requires confirm='%s'." % REFRESH_CONFIRM_TOKEN,
                **desk_safety()}
    sdir = _desk_dir(desk_dir)
    book = open_book(sdir)
    tickers: set[str] = set()
    start = None
    snap = _latest_confirmed_snapshot(ledger_dir)
    if snap is not None:
        cons, _w, snap_date = _snapshot_target(snap)
        tickers.update(cons)
        start = snap_date or None
    if book is not None:
        _cash, held = book_cash_holdings(book, _fills(sdir))
        tickers.update(held)
        orders = _orders_state(sdir)
        tickers.update(o["ticker"] for o in orders.values() if o["status"] not in _TERMINAL)
        start = min(x for x in [start, book.get("snapshot_market_date")] if x) if \
            (start or book.get("snapshot_market_date")) else None
    if not tickers and snap is None:
        return {"status": S_NO_PROPOSAL, "performed_write": False,
                "message": ("Nothing to refresh: no confirmed snapshot and no paper book. "
                            "Approve a proposal first."), **desk_safety()}
    required = _required_mark_date(completed_through=completed_through, today=today)
    # Phase 27B.1 window guard: the fetch always reaches back far enough to contain
    # completed sessions (a start AT the snapshot market date on the refresh day
    # yields an empty window - the exact root cause of the null-date store).
    floor_start = (date.fromisoformat(required)
                   - timedelta(days=MIN_MARK_WINDOW_DAYS)).isoformat()
    if start is None:
        start = (date.fromisoformat(required)
                 - timedelta(days=DEFAULT_MARK_LOOKBACK_DAYS)).isoformat()
    elif start > floor_start:
        start = floor_start
    try:
        sync = sync_marks(tickers=sorted(tickers), start=start, desk_dir=desk_dir,
                          downloader=downloader, today=today,
                          completed_through=required)
    except TournamentSyncBlocked as blocked:
        return {"status": S_BLOCKED, "performed_write": False,
                "blocked_reason": blocked.result_enum,
                "requested_ticker_count": len(tickers),
                "priced_ticker_count": 0, "missing_ticker_count": len(tickers),
                "missing_tickers": sorted(tickers),
                "latest_completed_market_date": required,
                "resulting_desk_mark_date": marks_latest_date(read_marks(desk_dir)),
                "blockers": ["PROVIDER_BLOCKED: %s - the owned-EODHD transport refused the "
                             "sync; no data was written." % blocked.result_enum],
                "next_action": NEXT_ACTION_REPAIR,
                "message": ("The owned-EODHD refresh was blocked (%s). The mark store and all "
                            "ledgers are unchanged." % blocked.result_enum), **desk_safety()}
    # ---- Phase 27B.1 coverage reconciliation (from the FINAL persisted store) ---- #
    marks = read_marks(desk_dir)
    series = marks.get("series") or {}
    resulting = marks_latest_date(marks)
    requested = sorted(tickers)
    missing = [tk for tk in requested if resulting is None or
               _series_price_at_or_before(series.get(tk) or [], resulting) is None]
    priced = len(requested) - len(missing)
    benchmark_priced = bool(resulting and _series_price_at_or_before(
        series.get(BENCHMARK_TICKER) or [], resulting) is not None)
    contract = {
        "requested_ticker_count": len(requested),
        "priced_ticker_count": priced,
        "missing_ticker_count": len(missing),
        "missing_tickers": missing,
        "latest_completed_market_date": required,
        "resulting_desk_mark_date": resulting,
        "benchmark_priced": benchmark_priced,
        "mark_coverage": sync.get("per_ticker") or [],
        "coverage_complete": not missing,
    }
    blockers: list[str] = []
    if resulting is None:
        blockers.append("NO_COMPLETED_MARKS_RECORDED: the owned provider returned no "
                        "completed close on or before %s for any requested ticker "
                        "(fetch window %s..%s)." % (required, start, required))
    elif priced == 0:
        blockers.append("NO_REQUESTED_TICKER_PRICED: the store has a completed date (%s) "
                        "but none of the %d requested tickers has a usable close."
                        % (resulting, len(requested)))
    elif resulting < required:
        blockers.append("DESK_MARK_DATE_BEHIND_REQUIRED: the freshest completed owned "
                        "close (%s) is behind the required latest completed market date "
                        "(%s) - the provider may not have published that session yet; "
                        "retry the refresh later." % (resulting, required))
    if resulting is not None:
        for tk in missing:
            blockers.append("TICKER_MARKS_MISSING: %s has no completed owned close at or "
                            "before %s; its allocation can only be held as cash."
                            % (tk, resulting))
    hard_blocked = resulting is None or priced == 0 or resulting < required
    if hard_blocked:
        wrote = bool(sync.get("store_written"))
        return {"status": S_MARKS_BLOCKED, "performed_write": wrote,
                "write_note": ("The desk mark store (a provider cache, not a ledger) was "
                               "rewritten with the fetched completed closes, but the sizing "
                               "requirements are unmet; no ledger row, order, fill or "
                               "performance row was created." if wrote else
                               "Nothing was written: no completed close was available to "
                               "record."),
                "marks": sync, **contract, "blockers": blockers,
                "next_action": NEXT_ACTION_REPAIR,
                "message": ("Desk mark refresh BLOCKED - no valid sizing marks. %s"
                            % blockers[0]),
                **desk_safety(wrote)}
    settle = settle_due_orders(desk_dir=desk_dir, today=today)
    perf = append_performance(desk_dir=desk_dir)
    if book is not None:
        _append_ledger(sdir, TIMELINE_FILE,
                       [_timeline_core(book, "DESK_REFRESH",
                                       "Manual desk refresh: marks through %s (%d tickers), "
                                       "%d fill(s), %d expiry(ies), %d performance row(s)."
                                       % (sync.get("latest_completed_date"),
                                          sync.get("n_tickers", 0), settle["n_filled"],
                                          settle["n_expired"], perf["n_appended"]))])
    open_after = [o for o in _orders_state(sdir).values() if o["status"] not in _TERMINAL]
    next_action = ("MONITOR_FILLS_AND_PERFORMANCE" if (open_after or settle["n_filled"])
                   else "GENERATE_ORDER_PLAN" if book is not None
                   else "REVIEW_DESK_STATUS")
    return {"status": S_OK, "performed_write": True, "wrote_to_desk_store_only": True,
            "marks": sync, "settlement": settle, "performance": perf,
            **contract, "blockers": blockers, "next_action": next_action,
            "message": ("Manual refresh complete: marks through %s; %d paper fill(s); %d "
                        "performance row(s) appended. %d of %d requested tickers priced."
                        % (sync.get("latest_completed_date"), settle["n_filled"],
                           perf["n_appended"], priced, len(requested))),
            **desk_safety(True)}


# --------------------------------------------------------------------------- #
# Read-only views (Workstream H backing)
# --------------------------------------------------------------------------- #
def load_books(desk_dir=None) -> dict:
    sdir = _desk_dir(desk_dir)
    books = _books(sdir)
    fills = _fills(sdir)
    marks = read_marks(desk_dir)
    out = []
    for b in books:
        nav = book_nav(b, fills, marks)
        out.append({**b, "valuation": nav})
    return {"status": S_OK, "n_books": len(books), "books": out,
            "store_dir": str(sdir), "ledger_integrity": verify_all_ledgers(desk_dir),
            **desk_safety()}


def load_orders(desk_dir=None, status_filter: Optional[str] = None) -> dict:
    sdir = _desk_dir(desk_dir)
    orders = sorted(_orders_state(sdir).values(), key=lambda o: o["order_id"])
    if status_filter:
        orders = [o for o in orders if o["status"] == status_filter.upper()]
    counts: dict[str, int] = {s: 0 for s in ORDER_STATUSES}
    for o in _orders_state(sdir).values():
        counts[o["status"]] = counts.get(o["status"], 0) + 1
    return {"status": S_OK, "n_orders": len(orders), "orders": orders,
            "counts_by_status": counts, "statuses": list(ORDER_STATUSES), **desk_safety()}


def load_fills(desk_dir=None) -> dict:
    fills = _fills(_desk_dir(desk_dir))
    return {"status": S_OK, "n_fills": len(fills),
            "fills": sorted(fills, key=lambda f: (f["fill_date"], f["fill_id"])),
            **desk_safety()}


def load_journal(desk_dir=None, limit: int = 200) -> dict:
    rows = [dict(r["entry"], seq=r["seq"], recorded_at=r["recorded_at"])
            for r in _read_ledger(_desk_dir(desk_dir), JOURNAL_FILE) if "entry" in r]
    return {"status": S_OK, "n_entries": len(rows), "entries": rows[-limit:][::-1],
            "rule_based_only": True, "llm_generated": False, **desk_safety()}


def load_timeline(desk_dir=None, limit: int = 200) -> dict:
    rows = [dict(r["event"], seq=r["seq"], recorded_at=r["recorded_at"])
            for r in _read_ledger(_desk_dir(desk_dir), TIMELINE_FILE) if "event" in r]
    return {"status": S_OK, "n_events": len(rows), "events": rows[-limit:][::-1],
            **desk_safety()}


def load_performance(desk_dir=None) -> dict:
    sdir = _desk_dir(desk_dir)
    book = open_book(sdir)
    rows = [r["row"] for r in _read_ledger(sdir, PERFORMANCE_FILE) if "row" in r]
    if book is not None:
        rows = [r for r in rows if r["book_id"] == book["book_id"]]
    summary = None
    if rows:
        last = rows[-1]
        summary = {"start_date": rows[0]["date"], "end_date": last["date"],
                   "n_rows": len(rows), "nav": last["nav"],
                   "cumulative_return_pct": last["cumulative_return_pct"],
                   "benchmark_cumulative_return_pct": last["benchmark_cumulative_return_pct"],
                   "excess_vs_benchmark_pct_points":
                       _r4(last["cumulative_return_pct"] - last["benchmark_cumulative_return_pct"])
                       if (last["cumulative_return_pct"] is not None
                           and last["benchmark_cumulative_return_pct"] is not None) else None,
                   "max_drawdown_pct": min((r["drawdown_pct"] for r in rows
                                            if r["drawdown_pct"] is not None), default=None),
                   "total_transaction_cost": _r4(sum(r["transaction_cost"] or 0 for r in rows))}
    return {"status": S_OK, "book_id": book["book_id"] if book else None,
            "n_rows": len(rows), "rows": rows, "summary": summary,
            "append_only": True, "historical_rows_never_recomputed": True, **desk_safety()}


def load_execution_preview(desk_dir=None) -> dict:
    """Read-only preview of what confirming / settling would do. Writes nothing."""
    sdir = _desk_dir(desk_dir)
    book = open_book(sdir)
    marks = read_marks(desk_dir)
    series = marks.get("series") or {}
    latest = marks_latest_date(marks)
    orders = [o for o in _orders_state(sdir).values() if o["status"] not in _TERMINAL]
    rows, est_cash_delta, est_cost = [], 0.0, 0.0
    for o in sorted(orders, key=lambda x: x["order_id"]):
        ind = _series_price_at_or_before(series.get(o["ticker"]) or [], latest) if latest else None
        gross = (o["quantity"] * ind[1]) if ind else None
        cost = gross * COST_RATE_PER_SIDE if gross is not None else None
        delta = None
        if gross is not None:
            delta = -(gross + cost) if o["side"] == SIDE_BUY else (gross - cost)
            est_cash_delta += delta
            est_cost += cost
        rows.append({**o,
                     "indicative_close": _r6(ind[1]) if ind else None,
                     "indicative_close_date": ind[0] if ind else None,
                     "indicative_gross_value": _r2(gross),
                     "indicative_transaction_cost": _r4(cost),
                     "indicative_net_cash_delta": _r4(delta)})
    val = book_nav(book, _fills(sdir), marks) if book else None
    return {"status": S_OK, "performed_write": False, "read_only_preview": True,
            "n_open_orders": len(rows), "orders": rows,
            "indicative_marks_date": latest,
            "estimated_total_transaction_cost": _r4(est_cost),
            "estimated_net_cash_delta": _r4(est_cash_delta),
            "book_valuation_before": val,
            "note": ("INDICATIVE at the latest completed owned close. The actual paper fill "
                     "is the first completed close on/after the approval date that was not "
                     "yet in the desk store at approval time (NEXT_CLOSE, no hindsight)."),
            "confirm_required_token": EXEC_CONFIRM_TOKEN, **desk_safety()}


def load_status(desk_dir=None, ledger_dir=None) -> dict:
    """The desk overview + the ONE deterministic next required manual action."""
    sdir = _desk_dir(desk_dir)
    book = open_book(sdir)
    marks = read_marks(desk_dir)
    fills = _fills(sdir)
    orders = _orders_state(sdir)
    counts: dict[str, int] = {s: 0 for s in ORDER_STATUSES}
    for o in orders.values():
        counts[o["status"]] += 1
    snap = _latest_confirmed_snapshot(ledger_dir)
    snap_id = snap.get("snapshot_id") if snap else None
    nav = book_nav(book, fills, marks) if book else None
    perf_rows = [r["row"] for r in _read_ledger(sdir, PERFORMANCE_FILE) if "row" in r]

    if snap is None:
        next_action = ("APPROVE_PROPOSAL: confirm the paper-alpha snapshot (Portfolio "
                       "Manager manual flow) - the desk has no approved proposal yet.")
    elif counts[ST_PROPOSED] > 0:
        next_action = ("CONFIRM_ORDERS: preview execution, then manually confirm the "
                       "PROPOSED paper orders.")
    elif counts[ST_SUBMITTED] > 0:
        next_action = ("REFRESH_DESK: SUBMITTED paper orders await the next completed owned "
                       "close - run the manual desk refresh.")
    elif book is None or not any(o.get("snapshot_id") == snap_id for o in orders.values()):
        if marks_latest_date(marks) is None:
            next_action = ("REFRESH_DESK: run the manual desk data refresh to record owned "
                           "marks, then create paper orders from the approved proposal.")
        else:
            next_action = ("CREATE_ORDERS: generate paper orders from confirmed snapshot "
                           "%s." % snap_id)
    else:
        next_action = "MONITOR: no pending paper orders. Monitor NAV, fills and attribution."

    return {"status": S_OK, "phase": PHASE,
            "book": book, "book_valuation": nav,
            "latest_desk_mark_date": marks_latest_date(marks),
            "marks_source": marks.get("source"),
            "order_counts": counts, "n_fills": len(fills),
            "n_performance_rows": len(perf_rows),
            "latest_confirmed_snapshot_id": snap_id,
            "snapshot_market_date": (snap or {}).get("market_as_of_date"),
            "execution_model": (book or {}).get("execution_model", EXECUTION_MODEL_DEFAULT),
            "execution_models": EXECUTION_MODELS,
            "execution_model_doc": EXECUTION_MODEL_DOC,
            "cost_bps_per_side": COST_BPS_PER_SIDE,
            "next_required_action": next_action,
            "confirm_tokens": {"create_orders": GEN_CONFIRM_TOKEN,
                               "submit_orders": EXEC_CONFIRM_TOKEN,
                               "refresh": REFRESH_CONFIRM_TOKEN,
                               "cancel": CANCEL_CONFIRM_TOKEN},
            "ledger_integrity": verify_all_ledgers(desk_dir),
            "store_dir": str(sdir), **desk_safety()}


# --------------------------------------------------------------------------- #
# Attribution (Workstream F) - read-only view over ledgers + marks
# --------------------------------------------------------------------------- #
_WINDOW_ROWS = {"daily": 1, "weekly": 5, "monthly": 21}


def load_attribution(desk_dir=None, window: str = "daily") -> dict:
    window = (window or "daily").lower()
    n_back = _WINDOW_ROWS.get(window, 1)
    sdir = _desk_dir(desk_dir)
    book = open_book(sdir)
    rows = [r["row"] for r in _read_ledger(sdir, PERFORMANCE_FILE) if "row" in r]
    if book is not None:
        rows = [r for r in rows if r["book_id"] == book["book_id"]]
    if book is None or len(rows) < 1:
        return {"status": "ATTRIBUTION_UNAVAILABLE", "window": window,
                "message": "Attribution requires at least one forward performance row.",
                **desk_safety()}
    end = rows[-1]
    start_idx = max(0, len(rows) - 1 - n_back)
    startr = rows[start_idx]
    if startr["date"] == end["date"] and len(rows) == 1:
        startr = {"date": None, "nav": float(book["initial_capital"]),
                  "cash": float(book["initial_capital"]), "holdings": {},
                  "benchmark_close": end["benchmark_close"]}
    marks = read_marks(desk_dir)
    series = marks.get("series") or {}
    fills = [f for f in _fills(sdir) if f["book_id"] == book["book_id"]]
    d0, d1 = startr["date"], end["date"]
    nav0 = float(startr["nav"]) or 1.0

    def _val(holdings: dict, d: Optional[str]) -> dict[str, float]:
        out = {}
        for tk, q in (holdings or {}).items():
            at = _series_price_at_or_before(series.get(tk) or [], d) if d else None
            out[tk] = q * at[1] if at else 0.0
        return out

    v0, v1 = _val(startr.get("holdings"), d0), _val(end.get("holdings"), d1)
    flows: dict[str, float] = {}
    win_cost = 0.0
    for f in fills:
        fd = f["fill_date"]
        if (d0 is None or fd > d0) and fd <= d1:
            sign = 1.0 if f["side"] == SIDE_BUY else -1.0
            flows[f["ticker"]] = flows.get(f["ticker"], 0.0) + sign * float(f["gross_value"])
            win_cost += float(f["transaction_cost"])
    sectors = {o["ticker"]: o.get("sector", "Unknown")
               for o in _orders_state(sdir).values()}
    contribs = []
    for tk in sorted(set(v0) | set(v1) | set(flows)):
        pnl = v1.get(tk, 0.0) - v0.get(tk, 0.0) - flows.get(tk, 0.0)
        contribs.append({"ticker": tk, "sector": sectors.get(tk, "Unknown"),
                         "pnl": _r2(pnl),
                         "contribution_pct_points": _r4(100.0 * pnl / nav0)})
    contribs.sort(key=lambda c: -(c["pnl"] or 0.0))
    sector_rows: dict[str, float] = {}
    for c in contribs:
        sector_rows[c["sector"]] = sector_rows.get(c["sector"], 0.0) + (c["pnl"] or 0.0)
    b0, b1 = _f(startr.get("benchmark_close")), _f(end.get("benchmark_close"))
    bench_ret = 100.0 * (b1 / b0 - 1.0) if (b0 and b1) else None
    cash_w = (float(startr.get("cash") or 0.0) / nav0) if nav0 else None
    cash_drag = (_r4(-cash_w * bench_ret) if (cash_w is not None and bench_ret is not None)
                 else None)
    total_position_contrib = sum(c["contribution_pct_points"] or 0.0 for c in contribs)
    return {
        "status": S_OK, "window": window, "windows": list(_WINDOW_ROWS),
        "book_id": book["book_id"], "start_date": d0, "end_date": d1,
        "nav_start": _r2(nav0), "nav_end": end["nav"],
        "portfolio_return_pct": _r4(100.0 * (float(end["nav"]) / nav0 - 1.0)),
        "benchmark_return_pct": _r4(bench_ret),
        "top_contributors": contribs[:5],
        "worst_contributors": contribs[-5:][::-1],
        "all_contributors": contribs,
        "sector_contribution": [{"sector": s, "pnl": _r2(p),
                                 "contribution_pct_points": _r4(100.0 * p / nav0)}
                                for s, p in sorted(sector_rows.items(),
                                                   key=lambda kv: -kv[1])],
        "cash_drag_pct_points": cash_drag,
        "cash_drag_formula": ("-(cash weight at window start) x benchmark window return; "
                              "the benchmark return the cash share did not earn."),
        "transaction_cost_in_window": _r4(win_cost),
        "model_contribution": {
            "convention": ("The operational blend is FIXED 50/50; per-leg attribution "
                           "follows the frozen blend weights (0.5 x each position's "
                           "contribution per leg). No loading is re-estimated."),
            "fundamental_contribution_pct_points": _r4(0.5 * total_position_contrib),
            "momentum_contribution_pct_points": _r4(0.5 * total_position_contrib),
        },
        "risk_overlay_effect": {"applied": False, "effect_pct_points": 0.0,
                                "note": ("The low-volatility overlay is diagnostic only and "
                                         "is never applied to the operational paper book.")},
        **desk_safety()}


__all__ = [
    "PHASE", "DESK_DIR_ENV", "MARKS_FIXTURE_ENV", "DEFAULT_DESK_DIR",
    "GEN_CONFIRM_TOKEN", "EXEC_CONFIRM_TOKEN", "REFRESH_CONFIRM_TOKEN", "CANCEL_CONFIRM_TOKEN",
    "ORDER_STATUSES", "SIDE_BUY", "SIDE_SELL",
    "EXECUTION_MODEL_DEFAULT", "EXECUTION_MODELS", "EXECUTION_MODEL_DOC",
    "COST_BPS_PER_SIDE", "EXPIRY_TRADING_DAYS", "BENCHMARK_TICKER",
    "desk_safety", "verify_ledger", "verify_all_ledgers",
    "sync_marks", "read_marks", "generate_orders", "confirm_orders", "cancel_orders",
    "settle_due_orders", "append_performance", "refresh_desk",
    "S_MARKS_BLOCKED", "NEXT_ACTION_REPAIR", "MIN_MARK_WINDOW_DAYS",
    "load_status", "load_books", "load_orders", "load_fills", "load_journal",
    "load_timeline", "load_performance", "load_execution_preview", "load_attribution",
    "open_book", "book_cash_holdings", "book_nav",
]
