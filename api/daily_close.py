"""
api/daily_close.py — Phase 27E/27F: the EXPLICIT DAILY CLOSE for Alpha Paper Book #1.

Before Phase 27E the operator UI was technically consistent but operationally
*passive*: it showed "NO ACTION TODAY / CURRENT — ALIGNED / Monitor Holdings"
without ever making the operator run, mark and record a daily close. A no-trade
day is only a valid *recorded decision* AFTER the latest eligible completed close
has been processed — not "doing nothing".

Phase 27F fixes three remaining defects:

  1. INITIAL BASELINE SEMANTICS — the very first operational close has no prior
     completed NAV. It is not an ordinary "daily review complete / HOLD"; it
     RECORDS THE INITIAL BASELINE (establishes the starting operational NAV).
     Daily P&L only begins with the next eligible completed close.
  2. SAME-DAY EOD READINESS — eligibility is a TWO-PART decision: (A) an EXPECTED
     SESSION from the US/Eastern clock with a configurable post-close safety
     cutoff (17:30 ET) reusing the existing market-hours logic, and (B) PROVIDER
     CONFIRMATION — the expected session is eligible only once the owned EOD
     transport actually returns that completed date. Before the cutoff ->
     AWAITING_MARKET_CLOSE; after the cutoff but provider not yet published ->
     WAITING_FOR_MARKET_DATA (no write in either).
  3. EXPLICIT MARKET-DATA SCOPE — the payload separates the VALUATION scope
     (every holding + non-terminal order ticker + SPY) from the DECISION scope
     (the full dynamic scoring universe from the frozen model). If only the
     holdings were refreshed the daily gate must NOT claim a fresh target-
     membership evaluation.

This module is the ONE canonical daily-close service. It does NOT re-implement
marking, P&L or the event gate — it COMPOSES the existing operational services:

    * ``paper_trading_desk.refresh_desk``  — sync owned completed EOD closes into
      the desk mark store, settle due NEXT_CLOSE paper orders, and append exactly
      one immutable forward-performance row per completed date (the P&L record);
    * ``daily_action_gate`` (Phase 27C/27D) — recompute the frozen-model target,
      compare it against the actual holdings and run the 13 daily risk / control
      checks, returning HOLD (NO_ACTION_TODAY) or a rebalance PROPOSAL;
    * ``operational_book``                 — the single read model for the book
      (holdings, NAV, cash, valuation date, review clock, lifecycle, pending
      orders);
    * ``multi_horizon_engine.build_current`` — the frozen model's current
      cross-section, used to derive the DYNAMIC decision scoring universe.

Canonical statuses (every eligible completed market date resolves to exactly one):

    INITIAL_BASELINE_DUE       active book, no prior completed row, close available
    INITIAL_BASELINE_RECORDED  the baseline NAV was recorded; daily P&L begins next
    AWAITING_MARKET_CLOSE      the expected session has not passed the safety cutoff
    WAITING_FOR_MARKET_DATA    session complete but the provider has not published
    DAILY_CLOSE_DUE            a new eligible close needs processing
    DAILY_CLOSE_COMPLETE_HOLD  processed; documented HOLD (no change)
    REBALANCE_PROPOSAL_READY   processed; a material trigger fired -> proposal
    PAPER_ORDERS_SUBMITTED     paper orders from a prior proposal are working
    DATA_BLOCKED               owned data cannot reach the required close
    ALREADY_PROCESSED          re-run of an already-closed date (POST only)
    AWAITING_ELIGIBLE_CLOSE    the book is not an active forward-tracking book yet

Two public entry points, mirroring the platform's read/execute split:

    load_daily_close(...)   — GET  /v1/operations/daily-close          (read-only)
    run_daily_close(...)    — POST /v1/operations/daily-close/execute  (manual)

STRICT SAFETY CONTRACT (enforced): the GET writes nothing (a live provider probe
is a read). The POST is the ONLY write and requires the explicit token
``CONFIRM_ALPHA_DAILY_CLOSE``; it revalidates readiness server-side and never
relies on a previously loaded GET. Its permitted writes are exactly the desk mark
cache, the settled paper fills / forward performance rows produced by the existing
manual desk refresh, and ONE row in a dedicated append-only, chain-hashed daily-
close decision journal. It NEVER creates paper orders (order creation stays a
separate token-gated manual action), never touches a broker, never runs
automation, never retrains / reweights / replaces the model, champion or sleeve,
and never writes a Paper Trader database row. Idempotent on
(operational_book_id, market_date): re-running a processed date returns
ALREADY_PROCESSED and writes nothing. A provider key is never returned or logged.
"""
from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable, Optional

from paper_trader.api import alpha_book as ab
from paper_trader.api import daily_action_gate as dag
from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import operational_book as ob
from paper_trader.api import paper_trading_desk as desk
from paper_trader.engine import market_hours as mh

PHASE = "27F"

# --------------------------------------------------------------------------- #
# Explicit manual confirmation token (the ONLY write path).
# --------------------------------------------------------------------------- #
EXECUTE_CONFIRMATION = "CONFIRM_ALPHA_DAILY_CLOSE"

# --------------------------------------------------------------------------- #
# The dedicated append-only, chain-hashed daily-close decision journal. It lives
# in the desk store (outside the git tree) alongside the other desk ledgers and
# uses the SAME append-only primitives, so a rewrite of a recorded daily close is
# detectable. It holds exactly ONE row per closed (book_id, market_date) and is
# the durable idempotency + decision record.
# --------------------------------------------------------------------------- #
DAILY_CLOSE_JOURNAL_FILE = "daily_close_journal.json"
DAILY_CLOSE_EVENT = "DAILY_CLOSE"

# --------------------------------------------------------------------------- #
# Post-close data-readiness safety cutoff (US/Eastern). Regular NYSE close is
# 16:00 ET; owned EOD data is only reliably published well after the close, so
# the expected session does not become "today" until this cutoff has passed.
# --------------------------------------------------------------------------- #
POST_CLOSE_CUTOFF_ET = time(17, 30)
_ET = mh._ET
BENCHMARK_TICKER = desk.BENCHMARK_TICKER  # "SPY"

# Deterministic clock seams (tests / explicit callers). An explicit ``now``
# datetime always wins; otherwise the env override, otherwise real UTC now.
NOW_ENV = "PAPER_TRADER_DAILY_CLOSE_NOW"
_now_override: Optional[datetime] = None

# --------------------------------------------------------------------------- #
# Canonical daily-close statuses.
# --------------------------------------------------------------------------- #
INITIAL_BASELINE_DUE = "INITIAL_BASELINE_DUE"
INITIAL_BASELINE_RECORDED = "INITIAL_BASELINE_RECORDED"
AWAITING_MARKET_CLOSE = "AWAITING_MARKET_CLOSE"
WAITING_FOR_MARKET_DATA = "WAITING_FOR_MARKET_DATA"
CLOSE_DUE = "DAILY_CLOSE_DUE"
CLOSE_COMPLETE_HOLD = "DAILY_CLOSE_COMPLETE_HOLD"
REBALANCE_PROPOSAL_READY = "REBALANCE_PROPOSAL_READY"
PAPER_ORDERS_SUBMITTED = "PAPER_ORDERS_SUBMITTED"
DATA_BLOCKED = "DATA_BLOCKED"
ALREADY_PROCESSED = "ALREADY_PROCESSED"
AWAITING_ELIGIBLE_CLOSE = "AWAITING_ELIGIBLE_CLOSE"

ALL_CLOSE_STATUSES = (INITIAL_BASELINE_DUE, INITIAL_BASELINE_RECORDED,
                      AWAITING_MARKET_CLOSE, WAITING_FOR_MARKET_DATA,
                      CLOSE_DUE, CLOSE_COMPLETE_HOLD, REBALANCE_PROPOSAL_READY,
                      PAPER_ORDERS_SUBMITTED, DATA_BLOCKED, ALREADY_PROCESSED,
                      AWAITING_ELIGIBLE_CLOSE)

# --------------------------------------------------------------------------- #
# Canonical daily decision-journal results (persisted per closed date).
# --------------------------------------------------------------------------- #
DECISION_HOLD = "HOLD_CURRENT_PORTFOLIO"
DECISION_REBALANCE = "REBALANCE_PROPOSAL_READY"
DECISION_DATA_BLOCKED = "DATA_BLOCKED"
DECISION_ORDERS_PENDING = "ORDERS_ALREADY_PENDING"
DECISION_BASELINE = "INITIAL_BASELINE_RECORDED"

# --------------------------------------------------------------------------- #
# Presentation (ONE operator vocabulary per status — every surface renders these).
# --------------------------------------------------------------------------- #
SEV_GREEN = "green"
SEV_AMBER = "amber"
SEV_RED = "red"

_PRESENTATION = {
    INITIAL_BASELINE_DUE: {
        "label": "INITIAL BASELINE DUE",
        "headline": "RECORD INITIAL BASELINE",
        "severity": SEV_AMBER,
        "primary_action_label": "Record Initial Baseline",
        "primary_action_kind": "RUN_DAILY_CLOSE",
        "current_task": "Record the initial operational baseline",
        "next_action": ("This first run establishes the starting operational NAV. Daily P&L "
                        "begins with the next eligible completed close."),
        "cycle_label": "INITIAL BASELINE DUE",
    },
    INITIAL_BASELINE_RECORDED: {
        "label": "INITIAL BASELINE RECORDED",
        "headline": "BASELINE RECORDED",
        "severity": SEV_GREEN,
        "primary_action_label": "View Baseline & Performance",
        "primary_action_kind": "VIEW_REVIEW",
        "current_task": "Await the next eligible completed close",
        "next_action": ("The starting operational NAV is recorded. Daily P&L will become "
                        "available after the next eligible completed close."),
        "cycle_label": "BASELINE RECORDED",
    },
    AWAITING_MARKET_CLOSE: {
        "label": "AWAITING MARKET CLOSE",
        "headline": "WAITING FOR TODAY'S MARKET CLOSE",
        "severity": SEV_GREEN,
        "primary_action_label": "Await Today's Market Close",
        "primary_action_kind": "AWAIT",
        "current_task": "Await today's market close",
        "next_action": ("Today's session has not passed the post-close data cutoff yet. The "
                        "daily close runs after the market closes and owned EOD data is "
                        "published."),
        "cycle_label": "AWAITING MARKET CLOSE",
    },
    WAITING_FOR_MARKET_DATA: {
        "label": "WAITING FOR MARKET DATA",
        "headline": "MARKET CLOSED — WAITING FOR COMPLETE EOD DATA",
        "severity": SEV_AMBER,
        "primary_action_label": "Refresh Status",
        "primary_action_kind": "REFRESH_STATUS",
        "current_task": "Wait for complete owned EOD data",
        "next_action": ("The exchange session should be complete, but the owned provider has "
                        "not yet published the required completed EOD data. Refresh status "
                        "shortly — no write occurs until the data is confirmed."),
        "cycle_label": "WAITING FOR MARKET DATA",
    },
    CLOSE_DUE: {
        "label": "DAILY CLOSE DUE",
        "headline": "RUN TODAY'S DAILY CLOSE",
        "severity": SEV_AMBER,
        "primary_action_label": "Run Daily Close",
        "primary_action_kind": "RUN_DAILY_CLOSE",
        "current_task": "Run Daily Close",
        "next_action": ("Process the latest completed EOD close, mark the book, update "
                        "P&L and evaluate the portfolio."),
        "cycle_label": "DAILY CLOSE DUE",
    },
    CLOSE_COMPLETE_HOLD: {
        "label": "DAILY REVIEW COMPLETE — HOLD CURRENT PORTFOLIO",
        "headline": "DAILY REVIEW COMPLETE — HOLD CURRENT PORTFOLIO",
        "severity": SEV_GREEN,
        "primary_action_label": "View Today's Daily Review",
        "primary_action_kind": "VIEW_REVIEW",
        "current_task": "Daily Review Complete",
        "next_action": ("Hold the current portfolio and monitor until the next eligible "
                        "close."),
        "cycle_label": "DAILY CLOSE COMPLETE — HOLD",
    },
    REBALANCE_PROPOSAL_READY: {
        "label": "REBALANCE PROPOSAL READY — MANUAL REVIEW REQUIRED",
        "headline": "REBALANCE PROPOSAL READY — MANUAL REVIEW REQUIRED",
        "severity": SEV_AMBER,
        "primary_action_label": "Review Rebalance Proposal",
        "primary_action_kind": "REVIEW_PROPOSAL",
        "current_task": "Review Rebalance Proposal",
        "next_action": ("Review the proposed portfolio changes; paper orders are created "
                        "only by a separate explicit confirmation."),
        "cycle_label": "PROPOSAL READY",
    },
    PAPER_ORDERS_SUBMITTED: {
        "label": "PAPER ORDERS PENDING",
        "headline": "PAPER ORDERS IN PROGRESS",
        "severity": SEV_AMBER,
        "primary_action_label": "Monitor Pending Paper Orders",
        "primary_action_kind": "MONITOR_ORDERS",
        "current_task": "Monitor Pending Paper Orders",
        "next_action": ("Paper orders from a prior proposal are working; refresh after the "
                        "next eligible close to settle them."),
        "cycle_label": "PAPER ORDERS PENDING",
    },
    DATA_BLOCKED: {
        "label": "DATA REFRESH REQUIRED",
        "headline": "DAILY CLOSE BLOCKED — OWNED DATA NOT AVAILABLE",
        "severity": SEV_RED,
        "primary_action_label": "Review Data Blocker",
        "primary_action_kind": "REVIEW_BLOCKER",
        "current_task": "Resolve the daily-close data blocker",
        "next_action": ("The owned completed EOD close required for the daily close is not "
                        "yet available. Retry the daily close later."),
        "cycle_label": "DAILY CLOSE BLOCKED",
    },
    ALREADY_PROCESSED: {
        "label": "ALREADY PROCESSED",
        "headline": "DAILY CLOSE ALREADY PROCESSED FOR THIS DATE",
        "severity": SEV_GREEN,
        "primary_action_label": "View Today's Daily Review",
        "primary_action_kind": "VIEW_REVIEW",
        "current_task": "Daily Review Complete",
        "next_action": ("This eligible close was already processed; the existing daily "
                        "review and mark are shown. No duplicate record was created."),
        "cycle_label": "DAILY CLOSE COMPLETE",
    },
    AWAITING_ELIGIBLE_CLOSE: {
        "label": "AWAITING ELIGIBLE CLOSE",
        "headline": "AWAITING THE NEXT ELIGIBLE COMPLETED CLOSE",
        "severity": SEV_GREEN,
        "primary_action_label": "Await Next Completed Close",
        "primary_action_kind": "AWAIT",
        "current_task": "Await the next eligible completed close",
        "next_action": ("No new eligible completed market close is available to process "
                        "yet. Monitor holdings until the next close completes."),
        "cycle_label": "FORWARD TRACKING",
    },
}

# Statuses whose primary action RUNS the daily close (write).
_RUNNABLE = (CLOSE_DUE, INITIAL_BASELINE_DUE)
# Statuses whose primary action is a disabled/await affordance.
_DISABLED_PRIMARY = (AWAITING_ELIGIBLE_CLOSE, AWAITING_MARKET_CLOSE)

_FIRST_MARK_NOTE = (
    "First daily mark after the initial baseline: there is no prior completed "
    "operational NAV, so daily P&L is unavailable for this date. Cumulative P&L "
    "and cumulative return are shown and reflect the modeled 12.5 bps/side paper "
    "execution cost embedded at fill — that cost is never charged again during "
    "daily marking.")
_BASELINE_PNL_DISPLAY = "Not available — baseline mark"

_MONTHS = ["", "JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY",
           "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER"]


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(str(x))
    except (TypeError, ValueError):
        return None


def _r2(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), 2)


def _fmt_md(iso: Optional[str]) -> str:
    """Human month + day (e.g. '2026-07-22' -> 'JULY 22'). Falls back to the ISO."""
    try:
        d = date.fromisoformat(str(iso)[:10])
        return "%s %d" % (_MONTHS[d.month], d.day)
    except (TypeError, ValueError, IndexError):
        return str(iso) if iso else "—"


def _safety(performed_write: bool = False) -> dict:
    return {
        "paper_only": True,
        "paper_orders_only": True,
        "read_only": not performed_write,
        "performed_write": bool(performed_write),
        "creates_orders": False,
        "auto_order_creation": False,
        "broker_enabled": False,
        "live_orders_enabled": False,
        "automation_enabled": False,
        "background_execution": False,
        "scheduled_tasks": False,
        "model_parameters_changed": False,
        "champion_replaced": False,
        "fast_sleeve_active": False,
        "manual_confirmation_required": True,
        "confirmation_token": EXECUTE_CONFIRMATION,
        "safety_badges": ["PAPER ONLY", "MANUAL REVIEW", "NO BROKER", "AUTOMATION OFF",
                          "NO LIVE ORDERS", "NO AUTO ORDER CREATION"],
    }


# --------------------------------------------------------------------------- #
# Expected-session clock (Phase 27F part A). Pure; reuses the existing US/Eastern
# market-hours logic and adds the post-close data-readiness safety cutoff.
# --------------------------------------------------------------------------- #
def _clock_now(now: Optional[datetime] = None) -> datetime:
    if now is not None:
        return now
    if _now_override is not None:
        return _now_override
    raw = os.environ.get(NOW_ENV)
    if raw:
        try:
            parsed = datetime.fromisoformat(raw)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            pass
    return datetime.now(tz=timezone.utc)


def _walk_back_weekend(d: date) -> date:
    while d.weekday() >= 5:  # Sat/Sun -> previous Friday
        d -= timedelta(days=1)
    return d


def _expected_session(now_et: datetime) -> tuple[date, bool, bool]:
    """The clock's latest EXPECTED completed trading session, whether the post-close
    safety cutoff has passed, and whether we are inside a trading day still forming
    today's session (a weekday before the cutoff). Weekends resolve back to the
    latest weekday; a holiday only makes the expected date one session too new —
    provider confirmation (part B) then resolves it to the latest actual session."""
    is_weekday = now_et.weekday() < 5
    cutoff_passed = is_weekday and now_et.timetz().replace(tzinfo=None) >= POST_CLOSE_CUTOFF_ET
    candidate = now_et.date() if cutoff_passed else now_et.date() - timedelta(days=1)
    within_trading_day = bool(is_weekday and not cutoff_passed)
    return _walk_back_weekend(candidate), cutoff_passed, within_trading_day


def _resolve_clock(today: Optional[str] = None, now: Optional[datetime] = None) -> dict:
    """Resolve the expected completed session + clock metadata.

    A deterministic ``today`` date string (no explicit ``now``) uses the legacy
    weekday-before rule (the SAME rule ``paper_trading_desk._required_mark_date``
    uses for an injected date) and treats the session as safely closed — so the
    offline harness and the alpha-target readiness stay aligned. The live path
    (today is None) uses the real US/Eastern clock with the 17:30 ET cutoff."""
    base = {
        "timezone": "America/New_York",
        "post_close_cutoff_et": POST_CLOSE_CUTOFF_ET.strftime("%H:%M"),
        "market_open": "09:30",
        "market_close": "16:00",
    }
    if now is None and _now_override is None and os.environ.get(NOW_ENV) is None \
            and today is not None:
        d = date.fromisoformat(str(today)[:10])
        expected = _walk_back_weekend(d - timedelta(days=1))
        base.update({
            "now_et": None, "cutoff_passed": True, "within_trading_day": False,
            "expected_market_date": expected.isoformat(),
            "reference_today": d.isoformat(), "clock_source": "INJECTED_DATE",
        })
        return base
    et = _clock_now(now).astimezone(_ET)
    expected, cutoff_passed, within_trading_day = _expected_session(et)
    base.update({
        "now_et": et.isoformat(), "cutoff_passed": bool(cutoff_passed),
        "within_trading_day": bool(within_trading_day),
        "expected_market_date": expected.isoformat(),
        "reference_today": et.date().isoformat(), "clock_source": "LIVE_ET",
    })
    return base


def _latest_eligible_market_date(today: Optional[str] = None,
                                 now: Optional[datetime] = None) -> str:
    """The latest COMPLETED owned market date the close targets (clock-resolved)."""
    return _resolve_clock(today=today, now=now)["expected_market_date"]


# --------------------------------------------------------------------------- #
# Provider confirmation (Phase 27F part B). Read-only owned-EOD transport probe:
# inspect the actual latest completed date the provider returns. Never writes,
# never returns or logs the provider key.
# --------------------------------------------------------------------------- #
def _default_provider_probe(*, expected_market_date: Optional[str], tickers: list,
                            downloader=None, ref_today: Optional[str] = None) -> dict:
    """Query the EXISTING owned EOD transport for the given tickers and return the
    actual latest completed date + the priced set. Read-only (no store write).
    Degrades to an empty result on any provider/transport error — never raises."""
    try:
        dl, source = desk._resolve_downloader(downloader)
    except Exception as exc:  # noqa: BLE001
        return {"provider_latest_date": None, "priced": [], "source": None,
                "error": str(exc)[:120], "queried": False}
    try:
        exp = date.fromisoformat(str(expected_market_date)[:10])
    except (TypeError, ValueError):
        exp = None
    ref = None
    try:
        ref = date.fromisoformat(str(ref_today)[:10]) if ref_today else None
    except (TypeError, ValueError):
        ref = None
    anchor = ref or exp or datetime.now(tz=timezone.utc).date()
    start = (anchor - timedelta(days=12)).isoformat()
    cutoff = anchor + timedelta(days=2)  # include today's completed bar if published
    latest: Optional[str] = None
    priced: set[str] = set()
    for tk in (tickers or [BENCHMARK_TICKER]):
        try:
            payload = dl(desk._clean_symbol(tk), start)
            bars = desk._completed_bars(desk._normalize_bars(payload), cutoff)
        except Exception:  # noqa: BLE001 — per-ticker isolation; the key is never handled here
            continue
        if bars:
            priced.add(tk)
            d = bars[-1][0]
            if latest is None or d > latest:
                latest = d
    return {"provider_latest_date": latest, "priced": sorted(priced),
            "source": source, "queried": True}


# The probe seam (tests inject a fully offline probe). The GET probes ONLY the
# benchmark (one read) to establish the provider's latest session; per-holding
# valuation coverage is reported from the read-only desk mark cache.
_PROVIDER_PROBE: Callable = _default_provider_probe
_ENGINE_LOADER: Callable = eng.build_current


def _provider_readiness(*, expected_market_date: Optional[str], probe_result: Optional[dict],
                        mark_cache_date: Optional[str]) -> dict:
    """Assemble the provider-readiness block from a (read-only) probe result,
    falling back to the desk mark cache's latest completed date."""
    pr = probe_result or {}
    latest = pr.get("provider_latest_date") or mark_cache_date
    source = pr.get("source") or ("desk_mark_cache" if mark_cache_date else None)
    ready = bool(latest and expected_market_date and latest >= expected_market_date)
    if latest is None:
        status, code, msg = ("PROVIDER_UNAVAILABLE", "PROVIDER_UNAVAILABLE",
                             "The owned EOD transport did not return a completed date.")
    elif ready:
        status, code, msg = ("READY", None,
                             "The owned provider has published the expected completed close.")
    else:
        status, code, msg = ("BEHIND", "PROVIDER_BEHIND_EXPECTED",
                             "The owned provider's latest completed date (%s) is behind the "
                             "expected session (%s) — the session has not been published yet."
                             % (latest, expected_market_date))
    return {
        "provider_name": source,
        "provider_latest_date": latest,
        "expected_market_date": expected_market_date,
        "ready": ready,
        "status": status,
        "checked_at": _now_iso(),
        "blocker_code": code,
        "blocker_message": (None if ready else msg),
        "queried_provider": bool(pr.get("queried")),
    }


# --------------------------------------------------------------------------- #
# Market-data scope (Phase 27F part 3). Two explicit scopes.
# --------------------------------------------------------------------------- #
def _holding_tickers(ops: dict) -> list:
    cs = (ops or {}).get("canonical_state") or {}
    ob_book = (ops or {}).get("operational_book") or {}
    out: set[str] = set()
    for r in (cs.get("holdings_detail") or ob_book.get("holdings_detail") or []):
        tk = r.get("ticker")
        if tk:
            out.add(str(tk).upper())
    for tk in (ob_book.get("holdings") or {}):
        out.add(str(tk).upper())
    return sorted(out)


def _open_order_tickers(desk_dir) -> list:
    """Non-terminal (open) paper-order tickers for the operational book (read-only)."""
    try:
        sdir = desk._desk_dir(desk_dir)
        orders = desk._orders_state(sdir).values()
        return sorted({str(o["ticker"]).upper() for o in orders
                       if o.get("book_id") == ob.OPERATIONAL_BOOK_ID
                       and o.get("status") not in desk._TERMINAL and o.get("ticker")})
    except Exception:  # noqa: BLE001
        return []


def _decision_universe(cur: Optional[dict]) -> list:
    """The DYNAMIC scoring universe from the frozen model's current cross-section
    (never hard-coded to 25 / 199 / 500 / 1009 / 1419)."""
    if not cur or cur.get("status") != eng.STATUS_READY:
        return []
    combined = cur.get("combined") or {}
    uni = combined.get("common_universe")
    if uni:
        return sorted({str(t).upper() for t in uni})
    # fall back to the scored combined map keys
    cmap = combined.get("combined") or {}
    return sorted({str(t).upper() for t in cmap})


def _market_data_scope(*, ops: dict, cur: Optional[dict], desk_dir,
                       mark_cache_date: Optional[str], benchmark_ready: bool,
                       gate: dict) -> dict:
    holdings = _holding_tickers(ops)
    orders = _open_order_tickers(desk_dir)
    valuation = sorted(set(holdings) | set(orders) | {BENCHMARK_TICKER})

    # Valuation coverage from the READ-ONLY desk mark cache at its latest date.
    priced_val: list[str] = []
    missing_val: list[str] = []
    try:
        marks = desk.read_marks(desk_dir)
        series = marks.get("series") or {}
        for tk in valuation:
            hit = (desk._series_price_at_or_before(series.get(tk) or [], mark_cache_date)
                   if mark_cache_date else None)
            (priced_val if hit is not None else missing_val).append(tk)
    except Exception:  # noqa: BLE001
        missing_val = list(valuation)
    holdings_priced = [tk for tk in holdings if tk in priced_val]
    complete_valuation = bool(mark_cache_date and not missing_val
                              and len(holdings_priced) == len(holdings)
                              and BENCHMARK_TICKER in priced_val)

    # Decision scope from the DYNAMIC model universe.
    universe = _decision_universe(cur)
    decision_scope = sorted(set(universe) | set(holdings) | set(orders) | {BENCHMARK_TICKER})
    engine_ready = bool(cur and cur.get("status") == eng.STATUS_READY)
    gate_data_ready = bool((gate or {}).get("data_ready"))
    decision_priced = len(universe) if engine_ready else 0
    # Held / open-order names that the current scoring universe does not contain
    # (still in scope — a current holding remains valued even if it left the universe).
    decision_missing = sorted((set(holdings) | set(orders)) - set(universe)) if engine_ready else []
    complete_decision = bool(engine_ready and gate_data_ready and universe)

    return {
        "valuation_tickers": valuation,
        "valuation_ticker_count": len(valuation),
        "valuation_priced_count": len(priced_val),
        "valuation_missing_tickers": sorted(missing_val),
        "benchmark_ticker": BENCHMARK_TICKER,
        "benchmark_ready": bool(benchmark_ready or (BENCHMARK_TICKER in priced_val)),
        "decision_universe_count": len(universe),
        "decision_scope_count": len(decision_scope),
        "decision_priced_count": decision_priced,
        "decision_missing_tickers": decision_missing,
        "current_holding_count": len(holdings),
        "open_order_ticker_count": len(orders),
        "complete_for_valuation": complete_valuation,
        "complete_for_decision": complete_decision,
        "decision_scope_note": (
            "The decision scope is the full dynamic scoring universe of the frozen model "
            "plus current holdings, open-order tickers and SPY. A fresh target-membership "
            "evaluation requires the complete decision scope — refreshing only the current "
            "holdings is not sufficient."),
    }


# --------------------------------------------------------------------------- #
# Daily-close decision journal (append-only, chain-hashed) — idempotency + record
# --------------------------------------------------------------------------- #
def _journal_rows(sdir) -> list[dict]:
    return [r for r in desk._read_ledger(sdir, DAILY_CLOSE_JOURNAL_FILE)
            if r.get("event") == DAILY_CLOSE_EVENT]


def _processed_row(sdir, book_id: str, market_date: str) -> Optional[dict]:
    """The recorded daily-close row for exactly this (book, date), or None."""
    match = None
    for r in _journal_rows(sdir):
        if r.get("book_id") == book_id and r.get("market_date") == market_date:
            match = r  # last write wins (there can only be one under the guard)
    return match


def _last_processed_date(sdir, book_id: str) -> Optional[str]:
    dates = [r.get("market_date") for r in _journal_rows(sdir)
             if r.get("book_id") == book_id and r.get("market_date")]
    return max(dates) if dates else None


def _decision_history(sdir, book_id: str, limit: int = 30) -> list[dict]:
    rows = [r for r in _journal_rows(sdir) if r.get("book_id") == book_id]
    rows = sorted(rows, key=lambda r: (r.get("market_date") or "", r.get("seq") or 0))
    out = [{"market_date": r.get("market_date"), "decision": r.get("decision"),
            "close_status": r.get("close_status"), "nav": r.get("nav"),
            "daily_pnl": r.get("daily_pnl"), "cumulative_pnl": r.get("cumulative_pnl"),
            "proposed_change_count": r.get("proposed_change_count"),
            "is_baseline": bool(r.get("is_baseline")),
            "evaluation_date": r.get("evaluation_date"),
            "recorded_at": r.get("recorded_at")} for r in rows]
    return out[-limit:][::-1]


# --------------------------------------------------------------------------- #
# P&L accounting — derived from the EXISTING immutable desk performance rows.
# --------------------------------------------------------------------------- #
def _sorted_perf_rows(perf: dict) -> list[dict]:
    rows = [r for r in (perf.get("rows") or []) if _f(r.get("nav")) is not None]
    return sorted(rows, key=lambda r: r.get("date") or "")


def _pnl_block(perf: dict, *, starting_capital: Optional[float],
               cash: Optional[float]) -> Optional[dict]:
    rows = _sorted_perf_rows(perf)
    if not rows:
        return None
    last = rows[-1]
    prev = rows[-2] if len(rows) >= 2 else None
    nav = _f(last.get("nav"))
    invested = _f(last.get("invested"))
    row_cash = _f(last.get("cash"))
    sc = _f(starting_capital)
    cum_pnl = (nav - sc) if (nav is not None and sc is not None) else None
    cum_ret = (nav / sc - 1.0) if (nav is not None and sc) else None
    if prev is not None:
        prev_nav = _f(prev.get("nav"))
        daily_pnl = (nav - prev_nav) if (nav is not None and prev_nav is not None) else None
        daily_ret = (daily_pnl / prev_nav) if (daily_pnl is not None and prev_nav) else None
        daily_available = daily_pnl is not None
        note = None
        display = None
        basis_date = prev.get("date")
    else:
        daily_pnl = daily_ret = None
        daily_available = False
        note = _FIRST_MARK_NOTE
        display = _BASELINE_PNL_DISPLAY
        basis_date = None
    spy_cum = _f(last.get("benchmark_cumulative_return_pct"))
    excess = ((cum_ret * 100.0) - spy_cum) if (cum_ret is not None and spy_cum is not None) else None
    return {
        "valuation_date": last.get("date"),
        "starting_capital": _r2(sc),
        "nav": _r2(nav),
        "cash": _r2(row_cash if row_cash is not None else cash),
        "invested_value": _r2(invested),
        "daily_pnl": _r2(daily_pnl),
        "daily_return_pct": (round(daily_ret * 100.0, 4) if daily_ret is not None else None),
        "daily_pnl_available": bool(daily_available),
        "daily_pnl_display": display,
        "daily_pnl_basis_date": basis_date,
        "daily_pnl_note": note,
        "baseline_nav": _r2(_f(rows[0].get("nav"))),
        "baseline_date": rows[0].get("date"),
        "cumulative_pnl": _r2(cum_pnl),
        "cumulative_return_pct": (round(cum_ret * 100.0, 4) if cum_ret is not None else None),
        "spy_cumulative_return_pct": spy_cum,
        "excess_return_pct": (round(excess, 4) if excess is not None else None),
        "drawdown_pct": _f(last.get("drawdown_pct")),
        "n_marks": len(rows),
    }


def _perf_history(perf: dict, *, starting_capital: Optional[float],
                  limit: int = 60) -> list[dict]:
    rows = _sorted_perf_rows(perf)
    sc = _f(starting_capital)
    out: list[dict] = []
    prev_nav: Optional[float] = None
    for i, r in enumerate(rows):
        nav = _f(r.get("nav"))
        dpnl = (nav - prev_nav) if (nav is not None and prev_nav is not None) else None
        dret = (dpnl / prev_nav) if (dpnl is not None and prev_nav) else None
        cpnl = (nav - sc) if (nav is not None and sc is not None) else None
        cret = _f(r.get("cumulative_return_pct"))
        spy_cum = _f(r.get("benchmark_cumulative_return_pct"))
        excess = (cret - spy_cum) if (cret is not None and spy_cum is not None) else None
        out.append({
            "market_date": r.get("date"),
            "row_type": ("INITIAL_BASELINE" if i == 0 else "DAILY_CLOSE"),
            "nav": _r2(nav),
            "daily_pnl": _r2(dpnl),
            "daily_return_pct": (round(dret * 100.0, 4) if dret is not None else None),
            "cumulative_pnl": _r2(cpnl),
            "cumulative_return_pct": cret,
            "spy_cumulative_return_pct": spy_cum,
            "excess_return_pct": (round(excess, 4) if excess is not None else None),
            "drawdown_pct": _f(r.get("drawdown_pct")),
        })
        prev_nav = nav
    return out[-limit:]


def _baseline_block(*, perf: dict, pnl: Optional[dict], baseline_recorded: bool,
                    baseline_required: bool) -> dict:
    rows = _sorted_perf_rows(perf)
    n = len(rows)
    baseline_nav = _r2(_f(rows[0].get("nav"))) if rows else None
    baseline_date = rows[0].get("date") if rows else None
    prior_nav_available = n >= 2
    if baseline_required:
        explanation = ("No prior completed operational NAV exists. The first run records the "
                       "initial baseline NAV; daily P&L begins with the next eligible close.")
    elif not prior_nav_available:
        explanation = ("The initial baseline NAV is recorded. Daily P&L is unavailable until "
                       "the next eligible completed close establishes a prior NAV.")
    else:
        explanation = "Daily P&L is computed against the prior completed operational NAV."
    return {
        "required": bool(baseline_required),
        "recorded": bool(baseline_recorded),
        "baseline_date": baseline_date,
        "baseline_nav": baseline_nav,
        "prior_completed_nav_available": bool(prior_nav_available),
        "daily_pnl_available": bool((pnl or {}).get("daily_pnl_available")),
        "explanation": explanation,
    }


# --------------------------------------------------------------------------- #
# Pure status resolver (fully deterministic; unit-testable)
# --------------------------------------------------------------------------- #
def resolve_daily_close_status(
    *,
    initialized: bool,
    book_active: bool,
    pending_orders: int,
    latest_eligible: Optional[str],
    last_processed_date: Optional[str],
    processed_decision_for_latest: Optional[str],
    baseline_required: bool = False,
    provider_ready: bool = True,
    cutoff_passed: bool = True,
    valuation_complete: bool = True,
    within_trading_day: bool = False,
) -> str:
    """Resolve the ONE canonical daily-close status from the current book state.

    ``processed_decision_for_latest`` is the recorded daily-close decision for the
    latest eligible market date (or None if that date has never been closed). The
    27F readiness inputs (baseline / provider / clock cutoff / valuation coverage)
    default to the legacy "ready" values so pre-27F callers keep their behavior."""
    if pending_orders:
        return PAPER_ORDERS_SUBMITTED
    if not initialized or not book_active:
        return AWAITING_ELIGIBLE_CLOSE
    if processed_decision_for_latest is not None:
        d = processed_decision_for_latest
        # An actionable recorded decision is never hidden behind a "waiting" state.
        if d == DECISION_REBALANCE:
            return REBALANCE_PROPOSAL_READY
        if d == DECISION_DATA_BLOCKED:
            return DATA_BLOCKED
        if d == DECISION_ORDERS_PENDING:
            return PAPER_ORDERS_SUBMITTED
        # A completed no-action decision (HOLD / BASELINE): inside a trading day
        # still forming today's session, surface AWAITING_MARKET_CLOSE so the
        # operator sees the next close is pending — not a stale prior HOLD.
        if within_trading_day:
            return AWAITING_MARKET_CLOSE
        if d == DECISION_BASELINE:
            return INITIAL_BASELINE_RECORDED
        return CLOSE_COMPLETE_HOLD  # HOLD_CURRENT_PORTFOLIO
    new_session = (last_processed_date is None
                   or (latest_eligible and last_processed_date < latest_eligible))
    if not new_session:
        # The latest final session is processed. Inside a trading day still forming
        # today's session (a weekday before the post-close cutoff) we are AWAITING
        # today's MARKET CLOSE; otherwise simply awaiting the next eligible close.
        return AWAITING_MARKET_CLOSE if within_trading_day else AWAITING_ELIGIBLE_CLOSE
    if not provider_ready:
        return AWAITING_MARKET_CLOSE if not cutoff_passed else WAITING_FOR_MARKET_DATA
    if not valuation_complete:
        return DATA_BLOCKED
    if baseline_required:
        return INITIAL_BASELINE_DUE
    return CLOSE_DUE


def _primary_action(close_status: str, *, book_active: bool) -> dict:
    pres = _PRESENTATION[close_status]
    kind = pres["primary_action_kind"]
    enabled = close_status not in _DISABLED_PRIMARY
    route = {
        "RUN_DAILY_CLOSE": "#daily-workflow",
        "REFRESH_STATUS": "#daily-workflow",
        "VIEW_REVIEW": "#portfolio-manager",
        "REVIEW_PROPOSAL": "#portfolio-manager",
        "MONITOR_ORDERS": "#portfolio-manager/pd-band",
        "REVIEW_BLOCKER": "#daily-workflow",
        "AWAIT": "#portfolio",
    }.get(kind, "#command-center")
    return {
        "label": pres["primary_action_label"],
        "kind": kind,
        "enabled": bool(enabled),
        "runs_daily_close": close_status in _RUNNABLE,
        "refreshes_status": kind == "REFRESH_STATUS",
        "route": route,
    }


def _daily_cycle_stages(close_status: str) -> list[dict]:
    """The explicit five-stage daily operating cycle. Stage statuses derive from
    the ONE canonical close status."""
    C, N, A, P, B = "COMPLETE", "NEEDS_ACTION", "ACTIVE", "PENDING", "BLOCKED"
    if close_status in (CLOSE_DUE, INITIAL_BASELINE_DUE):
        s = [N, P, P, P, P]
    elif close_status == DATA_BLOCKED:
        s = [B, P, P, P, P]
    elif close_status == WAITING_FOR_MARKET_DATA:
        s = [P, P, P, P, P]
    elif close_status == REBALANCE_PROPOSAL_READY:
        s = [C, C, C, N, P]
    elif close_status == PAPER_ORDERS_SUBMITTED:
        s = [C, C, C, A, P]
    elif close_status in (CLOSE_COMPLETE_HOLD, ALREADY_PROCESSED, INITIAL_BASELINE_RECORDED):
        s = [C, C, C, C, A]
    else:  # AWAITING_ELIGIBLE_CLOSE / AWAITING_MARKET_CLOSE
        s = [C, C, C, C, A]
    labels = [
        ("RUN_DAILY_CLOSE", "Run Daily Close",
         "Refresh the latest eligible owned EOD data, mark holdings, append daily performance."),
        ("RECALCULATE_TARGET_RISK", "Recalculate Target & Risk",
         "Recompute current ranks, eligibility and risk from the frozen model (no retraining)."),
        ("COMPARE_BUILD_DECISION", "Compare Holdings & Build Decision",
         "Compare the target against actual holdings and record HOLD or a rebalance proposal."),
        ("MANUAL_REVIEW_ORDERS", "Manual Review & Paper Orders",
         "Only when a proposal exists — explicit manual confirmation; no broker."),
        ("MONITOR_PERFORMANCE", "Monitor Performance",
         "NAV, P&L, benchmark, drawdown and forward history."),
    ]
    return [{"stage": i + 1, "code": code, "label": lbl, "status": s[i], "detail": det}
            for i, (code, lbl, det) in enumerate(labels)]


# --------------------------------------------------------------------------- #
# Composition helpers
# --------------------------------------------------------------------------- #
def _book_state(ops: dict) -> dict:
    cs = (ops or {}).get("canonical_state") or {}
    ob_book = (ops or {}).get("operational_book") or {}
    pending = int(cs.get("pending_order_count") or ob_book.get("pending_order_count") or 0)
    fills = int(cs.get("fill_count") or ob_book.get("fill_count") or 0)
    lifecycle = cs.get("lifecycle_stage")
    initialized = bool(ob_book.get("initialized"))
    book_active = bool((lifecycle == ob.LIFECYCLE_FILLED or fills) and not pending)
    return {
        "book_id": ob_book.get("book_id") or ab.ALPHA_BOOK_ID,
        "book_label": ob_book.get("book_label") or ob.OPERATIONAL_BOOK_LABEL,
        "initialized": initialized,
        "pending_orders": pending,
        "fills_count": fills,
        "lifecycle_stage": lifecycle,
        "book_active": book_active,
        "starting_capital": _f(ob_book.get("starting_capital")
                               or ob_book.get("initial_capital")),
        "nav": _f(cs.get("nav")),
        "cash": _f(cs.get("cash")),
        "holdings_count": int(cs.get("holdings_count") or ob_book.get("holdings_count") or 0),
        "valuation_date": cs.get("valuation_date"),
        "desk_mark_date": cs.get("desk_mark_date") or cs.get("valuation_date"),
        "next_scheduled_full_review": cs.get("next_review_date"),
        "scheduled_review_due": bool(cs.get("review_due")),
        "review_cadence": cs.get("review_cadence") or "MONTHLY",
    }


def _gate_slim(gate: dict) -> dict:
    """The gate fields the daily-close surfaces render (never re-derived in JS)."""
    g = gate or {}
    return {
        "gate_outcome": g.get("outcome"),
        "gate_outcome_label": g.get("outcome_label"),
        "target_state": g.get("target_state"),
        "target_state_label": g.get("target_state_label"),
        "checks_performed": g.get("checks_performed") or [],
        "checks_summary": g.get("checks_summary") or {},
        "proposed_additions": g.get("proposed_additions") or [],
        "proposed_removals": g.get("proposed_removals") or [],
        "proposed_resizes": g.get("proposed_resizes") or [],
        "blocked_changes": g.get("blocked_changes") or [],
        "proposed_change_count": int(g.get("proposed_change_count") or 0),
        "estimated_turnover": g.get("estimated_turnover"),
        "estimated_cost": g.get("estimated_cost"),
        "trigger_categories": g.get("trigger_categories") or [],
        "trigger_reasons": g.get("trigger_reasons") or [],
        "target_actual_match": bool(g.get("target_actual_match")),
        "operational_dates": g.get("operational_dates") or {},
        "data_ready": bool(g.get("data_ready")),
    }


def _assemble(*, close_status: str, book: dict, gate: dict, pnl: Optional[dict],
              history: list, processed_row: Optional[dict], last_processed_date: Optional[str],
              latest_eligible: Optional[str], decision_history: list, warnings: list,
              performed_write: bool, message: Optional[str] = None,
              blocker: Optional[dict] = None, evaluation_date: Optional[str] = None,
              payload_status: str = "DAILY_CLOSE_OK", context: Optional[dict] = None,
              headline_override: Optional[str] = None) -> dict:
    pres = _PRESENTATION[close_status]
    gslim = _gate_slim(gate)
    recorded_decision = (processed_row or {}).get("decision")
    # Estimated cash after a proposed implementation (indicative only).
    expected_cash_after = None
    if close_status == REBALANCE_PROPOSAL_READY and book.get("cash") is not None:
        cost = _f(gslim.get("estimated_cost")) or 0.0
        nav = book.get("nav") or 0.0
        expected_cash_after = _r2(book.get("cash") - cost * nav)
    proposal = None
    if close_status == REBALANCE_PROPOSAL_READY or gslim["proposed_change_count"]:
        proposal = {
            "proposed_additions": gslim["proposed_additions"],
            "proposed_removals": gslim["proposed_removals"],
            "proposed_resizes": gslim["proposed_resizes"],
            "blocked_changes": gslim["blocked_changes"],
            "proposed_change_count": gslim["proposed_change_count"],
            "estimated_turnover": gslim["estimated_turnover"],
            "estimated_cost": gslim["estimated_cost"],
            "expected_cash_after_implementation_indicative": expected_cash_after,
            "trigger_categories": gslim["trigger_categories"],
            "trigger_reasons": gslim["trigger_reasons"],
            "manual_review_required": True,
            "creates_orders": False,
            "note": ("Manual review required. Paper orders are created only by a separate "
                     "explicit token-gated confirmation — never by the daily close."),
        }
    ctx = context or {}
    out = {
        "status": payload_status,
        "phase": PHASE,
        "generated_at": _now_iso(),
        # -- the ONE canonical daily-close contract -------------------------- #
        "close_status": close_status,
        "close_status_label": pres["label"],
        "headline": headline_override or pres["headline"],
        "explanation": message or pres["next_action"],
        "severity": pres["severity"],
        "daily_cycle_label": pres["cycle_label"],
        "current_task": pres["current_task"],
        "next_action": pres["next_action"],
        "primary_action": _primary_action(close_status, book_active=book["book_active"]),
        "requires_close_run": close_status in _RUNNABLE,
        # -- book + dates ---------------------------------------------------- #
        "operational_book_id": book["book_id"],
        "operational_book_label": book["book_label"],
        "initialized": book["initialized"],
        "book_active": book["book_active"],
        "holdings_count": book["holdings_count"],
        "pending_order_count": book["pending_orders"],
        "fill_count": book["fills_count"],
        "latest_eligible_market_date": latest_eligible,
        "last_processed_market_date": last_processed_date,
        "current_valuation_date": book["valuation_date"],
        "desk_mark_date": book["desk_mark_date"],
        "next_scheduled_full_review": book["next_scheduled_full_review"],
        "scheduled_review_due": book["scheduled_review_due"],
        "review_cadence": book["review_cadence"],
        "operational_dates": {
            "evaluation_date": evaluation_date,
            "latest_eligible_market_date": latest_eligible,
            "last_processed_market_date": last_processed_date,
            "desk_mark_date": book["desk_mark_date"],
            "book_valuation_date": book["valuation_date"],
            "next_scheduled_full_review": book["next_scheduled_full_review"],
        },
        # -- decision + P&L -------------------------------------------------- #
        "decision": recorded_decision,
        "decision_recorded": processed_row is not None,
        "recorded_close": (None if processed_row is None else {
            "market_date": processed_row.get("market_date"),
            "decision": processed_row.get("decision"),
            "close_status": processed_row.get("close_status"),
            "is_baseline": bool(processed_row.get("is_baseline")),
            "evaluation_date": processed_row.get("evaluation_date"),
            "recorded_at": processed_row.get("recorded_at"),
            "nav": processed_row.get("nav"),
            "daily_pnl": processed_row.get("daily_pnl"),
            "cumulative_pnl": processed_row.get("cumulative_pnl"),
            "proposed_change_count": processed_row.get("proposed_change_count"),
        }),
        "pnl": pnl,
        "performance_history": history,
        "decision_history": decision_history,
        # -- gate passthrough (target vs actual + 13 checks) ----------------- #
        "gate_outcome": gslim["gate_outcome"],
        "gate_outcome_label": gslim["gate_outcome_label"],
        "target_state": gslim["target_state"],
        "target_state_label": gslim["target_state_label"],
        "target_actual_match": gslim["target_actual_match"],
        "checks_performed": gslim["checks_performed"],
        "checks_summary": gslim["checks_summary"],
        "proposal": proposal,
        "proposed_change_count": gslim["proposed_change_count"],
        # -- workflow + blockers -------------------------------------------- #
        "daily_cycle_stages": _daily_cycle_stages(close_status),
        "data_blocker": blocker,
        "confirmation_required": EXECUTE_CONFIRMATION,
        "close_status_vocabulary": list(ALL_CLOSE_STATUSES),
        "warnings": warnings,
        # -- Phase 27F readiness blocks (clock / provider / scope / baseline) - #
        "clock": ctx.get("clock"),
        "provider_readiness": ctx.get("provider_readiness"),
        "market_data_scope": ctx.get("market_data_scope"),
        "baseline": ctx.get("baseline"),
        **_safety(performed_write),
    }
    return out


# --------------------------------------------------------------------------- #
# Injectable seams (tests swap these to run fully offline).
# --------------------------------------------------------------------------- #
def _default_operational(today: Optional[str] = None) -> dict:
    return ob.load_operational_book(today=today)


def _default_gate(today: Optional[str] = None, operational: Optional[dict] = None,
                  current: Optional[dict] = None) -> dict:
    return dag.load_daily_action_gate(today=today, operational=operational, current=current)


def _safe_engine(engine_loader: Optional[Callable], warnings: list) -> Optional[dict]:
    try:
        return (engine_loader or _ENGINE_LOADER)()
    except Exception as exc:  # noqa: BLE001 — decision scope simply degrades
        warnings.append("Model current unavailable: %s" % str(exc)[:160])
        return None


def _run_probe(*, expected: Optional[str], ops: dict, desk_dir, downloader,
               provider_probe: Optional[Callable], ref_today: Optional[str],
               warnings: list, active: bool) -> Optional[dict]:
    """Run the read-only benchmark probe (only when it can change the decision)."""
    if not active:
        return None
    probe = provider_probe or _PROVIDER_PROBE
    try:
        return probe(expected_market_date=expected, tickers=[BENCHMARK_TICKER],
                     downloader=downloader, ref_today=ref_today)
    except Exception as exc:  # noqa: BLE001
        warnings.append("Provider probe failed: %s" % str(exc)[:160])
        return None


# --------------------------------------------------------------------------- #
# Public — GET (read-only status)
# --------------------------------------------------------------------------- #
def load_daily_close(
    *,
    today: Optional[str] = None,
    now: Optional[datetime] = None,
    desk_dir=None,
    ledger_dir=None,
    operational: Optional[dict] = None,
    gate: Optional[dict] = None,
    operational_loader: Optional[Callable] = None,
    gate_loader: Optional[Callable] = None,
    engine_loader: Optional[Callable] = None,
    provider_probe: Optional[Callable] = None,
    downloader=None,
) -> dict:
    """Read-only canonical daily-close status for Alpha Paper Book #1. Writes
    nothing (a live provider probe is a read); degrades to a controlled status
    (never a stack trace)."""
    warnings: list[str] = []
    sdir = desk._desk_dir(desk_dir)
    op_loader = operational_loader or _default_operational
    g_loader = gate_loader or _default_gate

    try:
        ops = operational if operational is not None else op_loader(today)
    except Exception as exc:  # noqa: BLE001
        ops = {}
        warnings.append("Operational book unavailable: %s" % str(exc)[:160])
    book = _book_state(ops)

    # Frozen-model current cross-section (decision scope + gate input; cache-backed).
    cur = None if gate is not None else _safe_engine(engine_loader, warnings)
    try:
        if gate is not None:
            pass
        elif g_loader is _default_gate:
            gate = g_loader(today, ops, cur)
        else:
            gate = g_loader(today, ops)
    except Exception as exc:  # noqa: BLE001
        gate = {}
        warnings.append("Daily action gate unavailable: %s" % str(exc)[:160])
    for w in (gate.get("warnings") or []):
        warnings.append("gate: %s" % w)

    clock = _resolve_clock(today=today, now=now)
    latest_eligible = clock["expected_market_date"]
    book_id = book["book_id"]
    last_processed = _last_processed_date(sdir, book_id)
    processed_row = _processed_row(sdir, book_id, latest_eligible) if latest_eligible else None
    baseline_recorded = last_processed is not None
    baseline_required = bool(book["book_active"] and not baseline_recorded)

    # Provider confirmation (part B) — read-only benchmark probe (only if active &
    # the latest eligible date has not already been processed).
    probe_needed = bool(book["book_active"] and processed_row is None)
    probe_result = _run_probe(expected=latest_eligible, ops=ops, desk_dir=desk_dir,
                              downloader=downloader, provider_probe=provider_probe,
                              ref_today=clock.get("reference_today"), warnings=warnings,
                              active=probe_needed)
    mark_cache_date = None
    try:
        mark_cache_date = desk.marks_latest_date(desk.read_marks(desk_dir))
    except Exception:  # noqa: BLE001
        pass
    provider = _provider_readiness(expected_market_date=latest_eligible,
                                   probe_result=probe_result, mark_cache_date=mark_cache_date)
    # If we did not probe (already processed / inactive), treat provider as ready
    # for resolution — the recorded decision or inactive branch governs the status.
    provider_ready = provider["ready"] if probe_needed else True

    scope = _market_data_scope(ops=ops, cur=cur, desk_dir=desk_dir,
                               mark_cache_date=mark_cache_date,
                               benchmark_ready=provider["ready"], gate=gate)

    close_status = resolve_daily_close_status(
        initialized=book["initialized"], book_active=book["book_active"],
        pending_orders=book["pending_orders"], latest_eligible=latest_eligible,
        last_processed_date=last_processed,
        processed_decision_for_latest=(processed_row or {}).get("decision")
        if processed_row else None,
        baseline_required=baseline_required, provider_ready=provider_ready,
        cutoff_passed=bool(clock.get("cutoff_passed")), valuation_complete=True,
        within_trading_day=bool(clock.get("within_trading_day")))

    try:
        perf = desk.load_performance(desk_dir)
    except Exception as exc:  # noqa: BLE001
        perf = {"rows": []}
        warnings.append("Performance history unavailable: %s" % str(exc)[:160])
    pnl = _pnl_block(perf, starting_capital=book["starting_capital"], cash=book["cash"])
    history = _perf_history(perf, starting_capital=book["starting_capital"])
    baseline = _baseline_block(perf=perf, pnl=pnl, baseline_recorded=baseline_recorded,
                               baseline_required=baseline_required)
    context = {"clock": clock, "provider_readiness": provider,
               "market_data_scope": scope, "baseline": baseline}

    return _assemble(
        close_status=close_status, book=book, gate=gate, pnl=pnl, history=history,
        processed_row=processed_row, last_processed_date=last_processed,
        latest_eligible=latest_eligible,
        decision_history=_decision_history(sdir, book_id), warnings=warnings,
        performed_write=False, evaluation_date=(today or date.today().isoformat()),
        context=context,
        headline_override=_headline_for(close_status, latest_eligible, pnl))


def _headline_for(close_status: str, market_date: Optional[str],
                  pnl: Optional[dict]) -> Optional[str]:
    """Date-bearing operator headline for the readiness states (Phase 27F)."""
    md = _fmt_md(market_date)
    if close_status == INITIAL_BASELINE_DUE:
        return "RECORD INITIAL BASELINE FOR %s" % md
    if close_status == INITIAL_BASELINE_RECORDED:
        nav = (pnl or {}).get("baseline_nav")
        if nav is None:
            nav = (pnl or {}).get("nav")
        base = "BASELINE RECORDED FOR %s" % md
        return base
    if close_status == CLOSE_DUE:
        return "%s EOD DATA READY — RUN DAILY CLOSE" % md
    return None


# --------------------------------------------------------------------------- #
# Public — POST (explicit manual daily close; the ONLY write path)
# --------------------------------------------------------------------------- #
def run_daily_close(
    *,
    confirm: Optional[str] = None,
    requested_by: str = "manual_ui",
    today: Optional[str] = None,
    now: Optional[datetime] = None,
    desk_dir=None,
    ledger_dir=None,
    downloader=None,
    refresh_fn: Optional[Callable] = None,
    operational_loader: Optional[Callable] = None,
    gate_loader: Optional[Callable] = None,
    engine_loader: Optional[Callable] = None,
    provider_probe: Optional[Callable] = None,
) -> dict:
    """Execute ONE explicit, manual daily close for Alpha Paper Book #1.

    Revalidates readiness SERVER-SIDE (never relies on a previously loaded GET):
    an unprocessed date before the post-close cutoff -> AWAITING_MARKET_CLOSE, and
    a provider that is affirmatively behind the expected session ->
    WAITING_FOR_MARKET_DATA — both perform no write. The first ever close for an
    active book RECORDS THE INITIAL BASELINE (establishes the starting operational
    NAV; daily P&L begins next close). Idempotent on (operational_book_id,
    market_date). Never creates a paper order, never touches a broker, never runs
    automation, never changes a model / champion / weight / sleeve."""
    warnings: list[str] = []
    evaluation_date = today or date.today().isoformat()
    sdir = desk._desk_dir(desk_dir)
    op_loader = operational_loader or _default_operational
    g_loader = gate_loader or _default_gate

    if confirm != EXECUTE_CONFIRMATION:
        return {"status": "DAILY_CLOSE_CONFIRM_REQUIRED", "phase": PHASE,
                "close_status": None, "performed_write": False,
                "confirmation_required": EXECUTE_CONFIRMATION,
                "message": ("Running the daily close requires confirm='%s'."
                            % EXECUTE_CONFIRMATION),
                **_safety(False)}

    # 1. resolve book + clock + latest eligible completed market date.
    try:
        ops = op_loader(today)
    except Exception as exc:  # noqa: BLE001
        ops = {}
        warnings.append("Operational book unavailable: %s" % str(exc)[:160])
    book = _book_state(ops)
    book_id = book["book_id"]
    clock = _resolve_clock(today=today, now=now)
    latest_eligible = clock["expected_market_date"]

    # 2. idempotency — an already-processed date performs no write.
    existing = _processed_row(sdir, book_id, latest_eligible) if latest_eligible else None
    if existing is not None:
        gate = {}
        try:
            gate = g_loader(today, ops)
        except Exception as exc:  # noqa: BLE001
            warnings.append("Gate unavailable: %s" % str(exc)[:160])
        perf = _safe_perf(desk_dir, warnings)
        pnl = _pnl_block(perf, starting_capital=book["starting_capital"], cash=book["cash"])
        return _assemble(
            close_status=ALREADY_PROCESSED, book=book, gate=gate, pnl=pnl,
            history=_perf_history(perf, starting_capital=book["starting_capital"]),
            processed_row=existing, last_processed_date=_last_processed_date(sdir, book_id),
            latest_eligible=latest_eligible,
            decision_history=_decision_history(sdir, book_id), warnings=warnings,
            performed_write=False, evaluation_date=evaluation_date,
            context=_min_context(clock),
            message=("The daily close for %s was already processed for %s — the existing "
                     "review and mark are shown. No duplicate record was created."
                     % (latest_eligible, book["book_label"])))

    # A non-active / uninitialized book (or one with pending orders) cannot run a
    # fresh close — surface the state, write nothing.
    if book["pending_orders"]:
        return _no_write_state(PAPER_ORDERS_SUBMITTED, book, ops, g_loader, today, sdir,
                               latest_eligible, warnings, evaluation_date, desk_dir, clock)
    if not book["initialized"] or not book["book_active"]:
        return _no_write_state(AWAITING_ELIGIBLE_CLOSE, book, ops, g_loader, today, sdir,
                               latest_eligible, warnings, evaluation_date, desk_dir, clock,
                               message=("Alpha Paper Book #1 is not an active forward-tracking "
                                        "book yet — the daily close begins after the initial "
                                        "implementation is filled."))

    last_processed = _last_processed_date(sdir, book_id)
    baseline_required = last_processed is None

    # 3. SERVER-SIDE readiness revalidation (never trust a stale GET). The expected
    #    session is always a FINAL completed session (yesterday before today's cutoff,
    #    today after it) — the wall clock is a GET-display concern, not a POST gate.
    #    If a probe/downloader is available and the provider is affirmatively behind
    #    the expected session -> WAITING_FOR_MARKET_DATA (no write).
    if provider_probe is not None or downloader is not None:
        probe_result = _run_probe(expected=latest_eligible, ops=ops, desk_dir=desk_dir,
                                  downloader=downloader, provider_probe=provider_probe,
                                  ref_today=clock.get("reference_today"), warnings=warnings,
                                  active=True)
        plat = (probe_result or {}).get("provider_latest_date")
        if plat is not None and latest_eligible and plat < latest_eligible:
            return _no_write_state(WAITING_FOR_MARKET_DATA, book, ops, g_loader, today, sdir,
                                   latest_eligible, warnings, evaluation_date, desk_dir, clock,
                                   provider=_provider_readiness(
                                       expected_market_date=latest_eligible,
                                       probe_result=probe_result, mark_cache_date=None))

    # 4. refresh owned completed EOD marks + settle fills + append performance,
    #    targeting the clock-resolved required completed date.
    refresh: dict = {}
    try:
        refresh = (refresh_fn or desk.refresh_desk)(
            confirm=desk.REFRESH_CONFIRM_TOKEN, desk_dir=desk_dir, ledger_dir=ledger_dir,
            downloader=downloader, today=today, completed_through=latest_eligible)
    except TypeError:
        # A fake refresh seam without ``completed_through`` — call without it.
        refresh = (refresh_fn or desk.refresh_desk)(
            confirm=desk.REFRESH_CONFIRM_TOKEN, desk_dir=desk_dir, ledger_dir=ledger_dir,
            downloader=downloader, today=today)
    except Exception as exc:  # noqa: BLE001 — degrade to a controlled DATA_BLOCKED
        warnings.append("Desk refresh failed: %s" % str(exc)[:160])
        refresh = {"status": desk.S_MARKS_BLOCKED, "performed_write": False,
                   "message": "Desk refresh raised: %s" % str(exc)[:160]}
    wrote = bool(refresh.get("performed_write"))
    resulting = (refresh.get("resulting_desk_mark_date")
                 or refresh.get("latest_completed_market_date"))

    # 5. blocked owned data -> DATA_BLOCKED (retryable; no decision-journal row).
    reached = bool(resulting and latest_eligible and resulting >= latest_eligible)
    if refresh.get("status") != desk.S_OK or not reached:
        blocker = {
            "refresh_status": refresh.get("status"),
            "required_market_date": latest_eligible,
            "resulting_desk_mark_date": resulting,
            "blockers": refresh.get("blockers") or [],
            "message": refresh.get("message"),
        }
        book2 = _book_state(_safe_ops(op_loader, today, warnings))
        perf_b = _safe_perf(desk_dir, warnings)
        return _assemble(
            close_status=DATA_BLOCKED, book=book2, gate={},
            pnl=_pnl_block(perf_b, starting_capital=book2["starting_capital"], cash=book2["cash"]),
            history=_perf_history(perf_b, starting_capital=book2["starting_capital"]),
            processed_row=None, last_processed_date=_last_processed_date(sdir, book_id),
            latest_eligible=latest_eligible,
            decision_history=_decision_history(sdir, book_id), warnings=warnings,
            performed_write=wrote, blocker=blocker, evaluation_date=evaluation_date,
            context=_min_context(clock),
            message=("The daily close could not reach the required completed close (%s). "
                     "The owned market data is not yet available; no decision was recorded — "
                     "retry the daily close later." % latest_eligible))
    closed_date = resulting

    # 6. recompute the frozen-model target + checks against fresh marks.
    ops2 = _safe_ops(op_loader, today, warnings)
    book2 = _book_state(ops2)
    cur2 = None if gate_loader is not None else _safe_engine(engine_loader, warnings)
    gate = {}
    try:
        if gate_loader is not None:
            gate = g_loader(today, ops2)
        else:
            gate = g_loader(today, ops2, cur2)
    except Exception as exc:  # noqa: BLE001
        warnings.append("Gate evaluation failed: %s" % str(exc)[:160])
    outcome = gate.get("outcome")
    pcount = int(gate.get("proposed_change_count") or 0)
    pending_after = int((ops2.get("canonical_state") or {}).get("pending_order_count") or 0)

    if outcome == dag.OUTCOME_DATA_NOT_READY:
        # Marks refreshed but the model target is not evaluable — the DECISION
        # scope is incomplete. Record NO decision; do not claim NO PORTFOLIO CHANGE.
        perf_b = _safe_perf(desk_dir, warnings)
        return _assemble(
            close_status=DATA_BLOCKED, book=book2, gate=gate,
            pnl=_pnl_block(perf_b, starting_capital=book2["starting_capital"], cash=book2["cash"]),
            history=_perf_history(perf_b, starting_capital=book2["starting_capital"]),
            processed_row=None, last_processed_date=_last_processed_date(sdir, book_id),
            latest_eligible=latest_eligible,
            decision_history=_decision_history(sdir, book_id), warnings=warnings,
            performed_write=wrote,
            blocker={"refresh_status": refresh.get("status"),
                     "required_market_date": latest_eligible,
                     "resulting_desk_mark_date": resulting,
                     "decision_scope_incomplete": True,
                     "message": ("Valuation marks refreshed, but the full decision scope "
                                 "(the frozen-model scoring universe) is not evaluable this "
                                 "session — no target-membership decision was recorded.")},
            context=_min_context(clock),
            message=("Owned valuation marks refreshed, but the frozen-model decision scope is "
                     "not evaluable this session; no portfolio decision was recorded — retry "
                     "later. Portfolio valuation and P&L are still available."))

    # 7. decision + P&L, then persist EXACTLY ONE daily-close journal row.
    is_baseline = False
    if pending_after or outcome == dag.OUTCOME_ORDERS_SUBMITTED:
        decision, close_status = DECISION_ORDERS_PENDING, PAPER_ORDERS_SUBMITTED
    elif baseline_required:
        decision, close_status, is_baseline = DECISION_BASELINE, INITIAL_BASELINE_RECORDED, True
    elif pcount > 0 or outcome in (dag.OUTCOME_PROPOSAL_READY, dag.OUTCOME_APPROVAL_REQUIRED):
        decision, close_status = DECISION_REBALANCE, REBALANCE_PROPOSAL_READY
    else:
        decision, close_status = DECISION_HOLD, CLOSE_COMPLETE_HOLD

    perf = _safe_perf(desk_dir, warnings)
    pnl = _pnl_block(perf, starting_capital=book2["starting_capital"], cash=book2["cash"])

    journal_row = {
        "event": DAILY_CLOSE_EVENT,
        "book_id": book_id,
        "market_date": closed_date,
        "decision": decision,
        "close_status": close_status,
        "is_baseline": is_baseline,
        "evaluation_date": evaluation_date,
        "requested_by": requested_by,
        "proposed_change_count": pcount,
        "gate_outcome": outcome,
        "checks_summary_line": (gate.get("checks_summary") or {}).get("line"),
        "nav": (pnl or {}).get("nav"),
        "daily_pnl": (pnl or {}).get("daily_pnl"),
        "daily_pnl_available": bool((pnl or {}).get("daily_pnl_available")),
        "cumulative_pnl": (pnl or {}).get("cumulative_pnl"),
        "cumulative_return_pct": (pnl or {}).get("cumulative_return_pct"),
        "settlement_fills": (refresh.get("settlement") or {}).get("n_filled"),
        "performance_rows_appended": (refresh.get("performance") or {}).get("n_appended"),
    }
    try:
        desk._append_ledger(sdir, DAILY_CLOSE_JOURNAL_FILE, [journal_row])
    except Exception as exc:  # noqa: BLE001 — never lose the completed marks/fills
        warnings.append("Daily-close journal append failed: %s" % str(exc)[:160])

    processed_row = _processed_row(sdir, book_id, closed_date)
    baseline = _baseline_block(perf=perf, pnl=pnl, baseline_recorded=True,
                               baseline_required=False)
    context = {"clock": clock, "provider_readiness": None,
               "market_data_scope": None, "baseline": baseline}
    return _assemble(
        close_status=close_status, book=book2, gate=gate, pnl=pnl,
        history=_perf_history(perf, starting_capital=book2["starting_capital"]),
        processed_row=processed_row, last_processed_date=closed_date,
        latest_eligible=latest_eligible,
        decision_history=_decision_history(sdir, book_id), warnings=warnings,
        performed_write=True, evaluation_date=evaluation_date, context=context,
        headline_override=_headline_for(close_status, closed_date, pnl),
        message=_completed_message(close_status, closed_date, pcount, pnl))


# --------------------------------------------------------------------------- #
# Internal — degrade-safe loaders / no-write state builder
# --------------------------------------------------------------------------- #
def _min_context(clock: dict) -> dict:
    return {"clock": clock, "provider_readiness": None,
            "market_data_scope": None, "baseline": None}


def _safe_ops(op_loader: Callable, today: Optional[str], warnings: list) -> dict:
    try:
        return op_loader(today)
    except Exception as exc:  # noqa: BLE001
        warnings.append("Operational book reload failed: %s" % str(exc)[:160])
        return {}


def _safe_perf(desk_dir, warnings: list) -> dict:
    try:
        return desk.load_performance(desk_dir)
    except Exception as exc:  # noqa: BLE001
        warnings.append("Performance history unavailable: %s" % str(exc)[:160])
        return {"rows": []}


def _no_write_state(close_status: str, book: dict, ops: dict, g_loader: Callable,
                    today: Optional[str], sdir, latest_eligible: Optional[str],
                    warnings: list, evaluation_date: Optional[str], desk_dir,
                    clock: Optional[dict] = None, message: Optional[str] = None,
                    provider: Optional[dict] = None) -> dict:
    gate = {}
    try:
        gate = g_loader(today, ops)
    except Exception as exc:  # noqa: BLE001
        warnings.append("Gate unavailable: %s" % str(exc)[:160])
    perf = _safe_perf(desk_dir, warnings)
    context = {"clock": clock, "provider_readiness": provider,
               "market_data_scope": None, "baseline": None}
    return _assemble(
        close_status=close_status, book=book, gate=gate,
        pnl=_pnl_block(perf, starting_capital=book["starting_capital"], cash=book["cash"]),
        history=_perf_history(perf, starting_capital=book["starting_capital"]),
        processed_row=None, last_processed_date=_last_processed_date(sdir, book["book_id"]),
        latest_eligible=latest_eligible,
        decision_history=_decision_history(sdir, book["book_id"]), warnings=warnings,
        performed_write=False, evaluation_date=evaluation_date, context=context,
        headline_override=_headline_for(close_status, latest_eligible, None),
        message=message)


def _completed_message(close_status: str, closed_date: str, pcount: int,
                       pnl: Optional[dict] = None) -> str:
    if close_status == INITIAL_BASELINE_RECORDED:
        nav = (pnl or {}).get("nav")
        nav_txt = ("$%s" % format(nav, ",.2f")) if isinstance(nav, (int, float)) else "the current mark"
        return ("Initial baseline recorded for %s. Starting NAV: %s. Daily P&L will become "
                "available after the next eligible completed close. This first run establishes "
                "the operational baseline — it is not an ordinary daily HOLD." % (closed_date, nav_txt))
    if close_status == CLOSE_COMPLETE_HOLD:
        return ("Daily close complete for %s. Documented decision: HOLD CURRENT PORTFOLIO — "
                "target and holdings remain aligned; no paper orders. This is a recorded "
                "decision, not inaction." % closed_date)
    if close_status == REBALANCE_PROPOSAL_READY:
        return ("Daily close complete for %s. A material trigger produced %d proposed change(s) "
                "— manual review required. No paper orders were created." % (closed_date, pcount))
    if close_status == PAPER_ORDERS_SUBMITTED:
        return ("Daily close complete for %s. Paper orders from a prior proposal are still "
                "working — monitor pending paper orders." % closed_date)
    return "Daily close complete for %s." % closed_date


__all__ = [
    "PHASE", "EXECUTE_CONFIRMATION", "DAILY_CLOSE_JOURNAL_FILE", "DAILY_CLOSE_EVENT",
    "POST_CLOSE_CUTOFF_ET", "NOW_ENV",
    "INITIAL_BASELINE_DUE", "INITIAL_BASELINE_RECORDED", "AWAITING_MARKET_CLOSE",
    "WAITING_FOR_MARKET_DATA",
    "CLOSE_DUE", "CLOSE_COMPLETE_HOLD", "REBALANCE_PROPOSAL_READY",
    "PAPER_ORDERS_SUBMITTED", "DATA_BLOCKED", "ALREADY_PROCESSED",
    "AWAITING_ELIGIBLE_CLOSE", "ALL_CLOSE_STATUSES",
    "DECISION_HOLD", "DECISION_REBALANCE", "DECISION_DATA_BLOCKED",
    "DECISION_ORDERS_PENDING", "DECISION_BASELINE",
    "resolve_daily_close_status", "load_daily_close", "run_daily_close",
    "_latest_eligible_market_date", "_resolve_clock",
]
