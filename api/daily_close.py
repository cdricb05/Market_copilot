"""
api/daily_close.py — Phase 27E: the EXPLICIT DAILY CLOSE for Alpha Paper Book #1.

Before this phase the operator UI was technically consistent but operationally
*passive*: it showed "NO ACTION TODAY / CURRENT — ALIGNED / Monitor Holdings"
without ever making the operator run, mark and record a daily close. A no-trade
day is only a valid *recorded decision* AFTER the latest eligible completed close
has been processed — not "doing nothing".

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
      orders).

Every eligible completed market date resolves to exactly ONE canonical status:

    DAILY_CLOSE_DUE            a new eligible close needs processing
    DAILY_CLOSE_COMPLETE_HOLD  processed; documented HOLD (no change)
    REBALANCE_PROPOSAL_READY   processed; a material trigger fired -> proposal
    PAPER_ORDERS_SUBMITTED     paper orders from a prior proposal are working
    DATA_BLOCKED               owned data cannot reach the required close
    ALREADY_PROCESSED          re-run of an already-closed date (POST only)
    AWAITING_ELIGIBLE_CLOSE    no new eligible completed close to process yet

Two public entry points, mirroring the platform's read/execute split:

    load_daily_close(...)   — GET  /v1/operations/daily-close          (read-only)
    run_daily_close(...)    — POST /v1/operations/daily-close/execute  (manual)

STRICT SAFETY CONTRACT (enforced): the GET writes nothing. The POST is the ONLY
write and requires the explicit token ``CONFIRM_ALPHA_DAILY_CLOSE``; its permitted
writes are exactly the desk mark cache, the settled paper fills / forward
performance rows produced by the existing manual desk refresh, and ONE row in a
dedicated append-only, chain-hashed daily-close decision journal. It NEVER creates
paper orders (order creation stays a separate token-gated manual action), never
touches a broker, never runs automation, never retrains / reweights / replaces the
model, champion or sleeve, and never writes a Paper Trader database row. Idempotent
on (operational_book_id, market_date): re-running a processed date returns
ALREADY_PROCESSED and writes nothing.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Callable, Optional

from paper_trader.api import alpha_book as ab
from paper_trader.api import daily_action_gate as dag
from paper_trader.api import operational_book as ob
from paper_trader.api import paper_trading_desk as desk

PHASE = "27E"

# --------------------------------------------------------------------------- #
# Explicit manual confirmation token (the ONLY write path).
# --------------------------------------------------------------------------- #
EXECUTE_CONFIRMATION = "CONFIRM_ALPHA_DAILY_CLOSE"

# --------------------------------------------------------------------------- #
# The dedicated append-only, chain-hashed daily-close decision journal. It lives
# in the desk store (outside the git tree) alongside the other desk ledgers and
# uses the SAME append-only primitives, so a rewrite of a recorded daily close is
# detectable. It is distinct from the desk's own decision_journal.json (which
# carries many rows per day); this ledger holds exactly ONE row per closed
# (book_id, market_date) and is the durable idempotency + decision record.
# --------------------------------------------------------------------------- #
DAILY_CLOSE_JOURNAL_FILE = "daily_close_journal.json"
DAILY_CLOSE_EVENT = "DAILY_CLOSE"

# --------------------------------------------------------------------------- #
# Canonical daily-close statuses.
# --------------------------------------------------------------------------- #
CLOSE_DUE = "DAILY_CLOSE_DUE"
CLOSE_COMPLETE_HOLD = "DAILY_CLOSE_COMPLETE_HOLD"
REBALANCE_PROPOSAL_READY = "REBALANCE_PROPOSAL_READY"
PAPER_ORDERS_SUBMITTED = "PAPER_ORDERS_SUBMITTED"
DATA_BLOCKED = "DATA_BLOCKED"
ALREADY_PROCESSED = "ALREADY_PROCESSED"
AWAITING_ELIGIBLE_CLOSE = "AWAITING_ELIGIBLE_CLOSE"

ALL_CLOSE_STATUSES = (CLOSE_DUE, CLOSE_COMPLETE_HOLD, REBALANCE_PROPOSAL_READY,
                      PAPER_ORDERS_SUBMITTED, DATA_BLOCKED, ALREADY_PROCESSED,
                      AWAITING_ELIGIBLE_CLOSE)

# --------------------------------------------------------------------------- #
# Canonical daily decision-journal results (persisted per closed date).
# --------------------------------------------------------------------------- #
DECISION_HOLD = "HOLD_CURRENT_PORTFOLIO"
DECISION_REBALANCE = "REBALANCE_PROPOSAL_READY"
DECISION_DATA_BLOCKED = "DATA_BLOCKED"
DECISION_ORDERS_PENDING = "ORDERS_ALREADY_PENDING"

# --------------------------------------------------------------------------- #
# Presentation (ONE operator vocabulary per status — every surface renders these).
# --------------------------------------------------------------------------- #
SEV_GREEN = "green"
SEV_AMBER = "amber"
SEV_RED = "red"

_PRESENTATION = {
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

# Statuses whose primary action RUNS the daily close (write). Only DUE.
_RUNNABLE = (CLOSE_DUE,)
# Statuses whose primary action is a disabled/await affordance.
_DISABLED_PRIMARY = (AWAITING_ELIGIBLE_CLOSE,)

_FIRST_MARK_NOTE = (
    "First daily mark after initial implementation: there is no prior completed "
    "operational NAV, so daily P&L is unavailable for this date. Cumulative P&L "
    "and cumulative return are shown and reflect the modeled 12.5 bps/side paper "
    "execution cost embedded at fill — that cost is never charged again during "
    "daily marking.")


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


def _r6(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), 6)


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


def _latest_eligible_market_date(today: Optional[str] = None) -> str:
    """The latest COMPLETED owned market date the close must reach — the SAME clock
    rule the alpha-target readiness + desk refresh use (so the dates align)."""
    return desk._required_mark_date(today=today)


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
            "evaluation_date": r.get("evaluation_date"),
            "recorded_at": r.get("recorded_at")} for r in rows]
    return out[-limit:][::-1]


# --------------------------------------------------------------------------- #
# P&L accounting — derived from the EXISTING immutable desk performance rows.
#
# NAV, cash, invested and cost are already embedded in each forward-performance
# row (cost is inside the fill cost basis, never re-charged here). We only add the
# honest daily-P&L rule: daily P&L exists ONLY when a PRIOR completed operational
# NAV exists; on the first mark after implementation it is unavailable while the
# cumulative figures are still shown.
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
        basis_date = prev.get("date")
    else:
        daily_pnl = daily_ret = None
        daily_available = False
        note = _FIRST_MARK_NOTE
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
        "daily_pnl_basis_date": basis_date,
        "daily_pnl_note": note,
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
    for r in rows:
        nav = _f(r.get("nav"))
        dpnl = (nav - prev_nav) if (nav is not None and prev_nav is not None) else None
        dret = (dpnl / prev_nav) if (dpnl is not None and prev_nav) else None
        cpnl = (nav - sc) if (nav is not None and sc is not None) else None
        cret = _f(r.get("cumulative_return_pct"))
        spy_cum = _f(r.get("benchmark_cumulative_return_pct"))
        excess = (cret - spy_cum) if (cret is not None and spy_cum is not None) else None
        out.append({
            "market_date": r.get("date"),
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
) -> str:
    """Resolve the ONE canonical daily-close status from the current book state.

    ``processed_decision_for_latest`` is the recorded daily-close decision for the
    latest eligible market date (or None if that date has never been closed).
    """
    if pending_orders:
        return PAPER_ORDERS_SUBMITTED
    if not initialized or not book_active:
        return AWAITING_ELIGIBLE_CLOSE
    if processed_decision_for_latest is not None:
        d = processed_decision_for_latest
        if d == DECISION_REBALANCE:
            return REBALANCE_PROPOSAL_READY
        if d == DECISION_DATA_BLOCKED:
            return DATA_BLOCKED
        if d == DECISION_ORDERS_PENDING:
            return PAPER_ORDERS_SUBMITTED
        return CLOSE_COMPLETE_HOLD  # HOLD_CURRENT_PORTFOLIO
    if last_processed_date is None or (latest_eligible and last_processed_date < latest_eligible):
        return CLOSE_DUE
    return AWAITING_ELIGIBLE_CLOSE


def _primary_action(close_status: str, *, book_active: bool) -> dict:
    pres = _PRESENTATION[close_status]
    kind = pres["primary_action_kind"]
    enabled = close_status not in _DISABLED_PRIMARY
    route = {
        "RUN_DAILY_CLOSE": "#daily-workflow",
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
        "route": route,
    }


def _daily_cycle_stages(close_status: str) -> list[dict]:
    """The explicit five-stage daily operating cycle (Phase 27E section G). Stage
    statuses derive from the ONE canonical close status."""
    C, N, A, P, B = "COMPLETE", "NEEDS_ACTION", "ACTIVE", "PENDING", "BLOCKED"
    if close_status == CLOSE_DUE:
        s = [N, P, P, P, P]
    elif close_status == DATA_BLOCKED:
        s = [B, P, P, P, P]
    elif close_status == REBALANCE_PROPOSAL_READY:
        s = [C, C, C, N, P]
    elif close_status == PAPER_ORDERS_SUBMITTED:
        s = [C, C, C, A, P]
    elif close_status in (CLOSE_COMPLETE_HOLD, ALREADY_PROCESSED):
        s = [C, C, C, C, A]
    else:  # AWAITING_ELIGIBLE_CLOSE
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
    }


def _assemble(*, close_status: str, book: dict, gate: dict, pnl: Optional[dict],
              history: list, processed_row: Optional[dict], last_processed_date: Optional[str],
              latest_eligible: Optional[str], decision_history: list, warnings: list,
              performed_write: bool, message: Optional[str] = None,
              blocker: Optional[dict] = None, evaluation_date: Optional[str] = None,
              payload_status: str = "DAILY_CLOSE_OK") -> dict:
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
    return {
        "status": payload_status,
        "phase": PHASE,
        "generated_at": _now_iso(),
        # -- the ONE canonical daily-close contract -------------------------- #
        "close_status": close_status,
        "close_status_label": pres["label"],
        "headline": pres["headline"],
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
        **_safety(performed_write),
    }


# --------------------------------------------------------------------------- #
# Injectable seams (tests swap these to run fully offline).
# --------------------------------------------------------------------------- #
def _default_operational(today: Optional[str] = None) -> dict:
    return ob.load_operational_book(today=today)


def _default_gate(today: Optional[str] = None, operational: Optional[dict] = None) -> dict:
    return dag.load_daily_action_gate(today=today, operational=operational)


# --------------------------------------------------------------------------- #
# Public — GET (read-only status)
# --------------------------------------------------------------------------- #
def load_daily_close(
    *,
    today: Optional[str] = None,
    desk_dir=None,
    ledger_dir=None,
    operational: Optional[dict] = None,
    gate: Optional[dict] = None,
    operational_loader: Optional[Callable] = None,
    gate_loader: Optional[Callable] = None,
) -> dict:
    """Read-only canonical daily-close status for Alpha Paper Book #1. Writes
    nothing; degrades to a controlled status (never a stack trace)."""
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

    try:
        gate = gate if gate is not None else g_loader(today, ops)
    except Exception as exc:  # noqa: BLE001
        gate = {}
        warnings.append("Daily action gate unavailable: %s" % str(exc)[:160])
    for w in (gate.get("warnings") or []):
        warnings.append("gate: %s" % w)

    try:
        latest_eligible = _latest_eligible_market_date(today)
    except Exception as exc:  # noqa: BLE001
        latest_eligible = book["desk_mark_date"]
        warnings.append("Latest completed market date unresolved: %s" % str(exc)[:160])

    book_id = book["book_id"]
    processed_row = _processed_row(sdir, book_id, latest_eligible) if latest_eligible else None
    last_processed = _last_processed_date(sdir, book_id)

    close_status = resolve_daily_close_status(
        initialized=book["initialized"], book_active=book["book_active"],
        pending_orders=book["pending_orders"], latest_eligible=latest_eligible,
        last_processed_date=last_processed,
        processed_decision_for_latest=(processed_row or {}).get("decision")
        if processed_row else None)

    try:
        perf = desk.load_performance(desk_dir)
    except Exception as exc:  # noqa: BLE001
        perf = {"rows": []}
        warnings.append("Performance history unavailable: %s" % str(exc)[:160])
    pnl = _pnl_block(perf, starting_capital=book["starting_capital"], cash=book["cash"])
    history = _perf_history(perf, starting_capital=book["starting_capital"])

    return _assemble(
        close_status=close_status, book=book, gate=gate, pnl=pnl, history=history,
        processed_row=processed_row, last_processed_date=last_processed,
        latest_eligible=latest_eligible,
        decision_history=_decision_history(sdir, book_id), warnings=warnings,
        performed_write=False,
        evaluation_date=(today or date.today().isoformat()))


# --------------------------------------------------------------------------- #
# Public — POST (explicit manual daily close; the ONLY write path)
# --------------------------------------------------------------------------- #
def run_daily_close(
    *,
    confirm: Optional[str] = None,
    requested_by: str = "manual_ui",
    today: Optional[str] = None,
    desk_dir=None,
    ledger_dir=None,
    downloader=None,
    refresh_fn: Optional[Callable] = None,
    operational_loader: Optional[Callable] = None,
    gate_loader: Optional[Callable] = None,
) -> dict:
    """Execute ONE explicit, manual daily close for Alpha Paper Book #1.

    Deterministic sequence (idempotent on operational_book_id + market_date):
      1. resolve the latest eligible completed market date;
      2. if already processed -> ALREADY_PROCESSED (no write, no duplicate row);
      3. refresh owned completed EOD marks + settle NEXT_CLOSE fills + append the
         immutable daily performance row (the existing manual desk refresh);
      4. if the owned data cannot reach the required close -> DATA_BLOCKED (no
         decision-journal row, retryable — never a partial performance record);
      5. recompute the frozen-model target + risk + the 13 daily checks and compare
         with the actual holdings (the Phase 27D daily action gate);
      6. persist exactly ONE daily decision-journal row (HOLD / REBALANCE /
         ORDERS_ALREADY_PENDING);
      7. return the complete daily-close result.

    Never creates a paper order, never touches a broker, never runs automation,
    never changes a model / champion / weight / sleeve.
    """
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

    # 1. resolve book + latest eligible completed market date.
    try:
        ops = op_loader(today)
    except Exception as exc:  # noqa: BLE001
        ops = {}
        warnings.append("Operational book unavailable: %s" % str(exc)[:160])
    book = _book_state(ops)
    book_id = book["book_id"]
    try:
        latest_eligible = _latest_eligible_market_date(today)
    except Exception as exc:  # noqa: BLE001
        latest_eligible = book["desk_mark_date"]
        warnings.append("Latest completed market date unresolved: %s" % str(exc)[:160])

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
            message=("The daily close for %s was already processed for %s — the existing "
                     "review and mark are shown. No duplicate record was created."
                     % (latest_eligible, book["book_label"])))

    # A non-active / uninitialized book (or one with pending orders) cannot run a
    # fresh close — surface the state, write nothing.
    if book["pending_orders"]:
        return _no_write_state(PAPER_ORDERS_SUBMITTED, book, ops, g_loader, today, sdir,
                               latest_eligible, warnings, evaluation_date, desk_dir)
    if not book["initialized"] or not book["book_active"]:
        return _no_write_state(AWAITING_ELIGIBLE_CLOSE, book, ops, g_loader, today, sdir,
                               latest_eligible, warnings, evaluation_date, desk_dir,
                               message=("Alpha Paper Book #1 is not an active forward-tracking "
                                        "book yet — the daily close begins after the initial "
                                        "implementation is filled."))

    # 3. refresh owned completed EOD marks + settle fills + append performance.
    refresh: dict = {}
    try:
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

    # 4. blocked owned data -> DATA_BLOCKED (retryable; no decision-journal row).
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
        return _assemble(
            close_status=DATA_BLOCKED, book=book2, gate={},
            pnl=_pnl_block(_safe_perf(desk_dir, warnings),
                           starting_capital=book2["starting_capital"], cash=book2["cash"]),
            history=_perf_history(_safe_perf(desk_dir, warnings),
                                  starting_capital=book2["starting_capital"]),
            processed_row=None, last_processed_date=_last_processed_date(sdir, book_id),
            latest_eligible=latest_eligible,
            decision_history=_decision_history(sdir, book_id), warnings=warnings,
            performed_write=wrote, blocker=blocker, evaluation_date=evaluation_date,
            message=("The daily close could not reach the required completed close (%s). "
                     "The owned market data is not yet available; no decision was recorded — "
                     "retry the daily close later." % latest_eligible))
    closed_date = resulting

    # 5. recompute the frozen-model target + checks against fresh marks.
    ops2 = _safe_ops(op_loader, today, warnings)
    book2 = _book_state(ops2)
    gate = {}
    try:
        gate = g_loader(today, ops2)
    except Exception as exc:  # noqa: BLE001
        warnings.append("Gate evaluation failed: %s" % str(exc)[:160])
    outcome = gate.get("outcome")
    pcount = int(gate.get("proposed_change_count") or 0)
    pending_after = int((ops2.get("canonical_state") or {}).get("pending_order_count") or 0)

    if outcome == dag.OUTCOME_DATA_NOT_READY:
        # Marks refreshed but the model target is not evaluable — retryable block.
        return _assemble(
            close_status=DATA_BLOCKED, book=book2, gate=gate,
            pnl=_pnl_block(_safe_perf(desk_dir, warnings),
                           starting_capital=book2["starting_capital"], cash=book2["cash"]),
            history=_perf_history(_safe_perf(desk_dir, warnings),
                                  starting_capital=book2["starting_capital"]),
            processed_row=None, last_processed_date=_last_processed_date(sdir, book_id),
            latest_eligible=latest_eligible,
            decision_history=_decision_history(sdir, book_id), warnings=warnings,
            performed_write=wrote,
            blocker={"refresh_status": refresh.get("status"),
                     "required_market_date": latest_eligible,
                     "resulting_desk_mark_date": resulting,
                     "message": "Model target/scores are not evaluable this session."},
            evaluation_date=evaluation_date,
            message=("Owned marks refreshed, but the frozen-model target is not evaluable "
                     "this session; no decision was recorded — retry later."))

    # 6. decision + P&L, then persist EXACTLY ONE daily-close journal row.
    if pending_after or outcome == dag.OUTCOME_ORDERS_SUBMITTED:
        decision, close_status = DECISION_ORDERS_PENDING, PAPER_ORDERS_SUBMITTED
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
    return _assemble(
        close_status=close_status, book=book2, gate=gate, pnl=pnl,
        history=_perf_history(perf, starting_capital=book2["starting_capital"]),
        processed_row=processed_row, last_processed_date=closed_date,
        latest_eligible=latest_eligible,
        decision_history=_decision_history(sdir, book_id), warnings=warnings,
        performed_write=True, evaluation_date=evaluation_date,
        message=_completed_message(close_status, closed_date, pcount))


# --------------------------------------------------------------------------- #
# Internal — degrade-safe loaders / no-write state builder
# --------------------------------------------------------------------------- #
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
                    message: Optional[str] = None) -> dict:
    gate = {}
    try:
        gate = g_loader(today, ops)
    except Exception as exc:  # noqa: BLE001
        warnings.append("Gate unavailable: %s" % str(exc)[:160])
    perf = _safe_perf(desk_dir, warnings)
    return _assemble(
        close_status=close_status, book=book, gate=gate,
        pnl=_pnl_block(perf, starting_capital=book["starting_capital"], cash=book["cash"]),
        history=_perf_history(perf, starting_capital=book["starting_capital"]),
        processed_row=None, last_processed_date=_last_processed_date(sdir, book["book_id"]),
        latest_eligible=latest_eligible,
        decision_history=_decision_history(sdir, book["book_id"]), warnings=warnings,
        performed_write=False, evaluation_date=evaluation_date, message=message)


def _completed_message(close_status: str, closed_date: str, pcount: int) -> str:
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
    "CLOSE_DUE", "CLOSE_COMPLETE_HOLD", "REBALANCE_PROPOSAL_READY",
    "PAPER_ORDERS_SUBMITTED", "DATA_BLOCKED", "ALREADY_PROCESSED",
    "AWAITING_ELIGIBLE_CLOSE", "ALL_CLOSE_STATUSES",
    "DECISION_HOLD", "DECISION_REBALANCE", "DECISION_DATA_BLOCKED", "DECISION_ORDERS_PENDING",
    "resolve_daily_close_status", "load_daily_close", "run_daily_close",
    "_latest_eligible_market_date",
]
