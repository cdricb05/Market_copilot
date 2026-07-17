"""
api/daily_workflow_dashboard.py — Phase 14-B read-only Daily Workflow view model.

Aggregates the state the daily operating terminal needs into ONE read-only view
model organised around the six operating stages:

    DATA -> CANDIDATES -> REVIEW -> SIGNALS -> DECISIONS -> PORTFOLIO

Exactly one stage is marked active, derived from current backend data (never a
stale browser variable). The review slice separates the ACTIVE REVIEW QUEUE
(today's currently-actionable candidates) from RECENT REVIEW HISTORY (rejected /
approved / older records), grouped by stable candidate identity (ticker) with a
history count — so completed or rejected work is never presented as pending.

It composes EXISTING internal service state only: the same read-only counts as
/v1/review/current-workflow-state, the review-created Signal / TradeDecision
rows, and the shared capacity / safety helpers from the command centre. It makes
NO loopback HTTP calls, performs NO database writes, invokes NO daily refresh,
calls NO prediction provider, and creates NO signals / decisions / orders. Every
section is isolated so a single failing dependency degrades to a ``warnings[]``
entry instead of failing the whole endpoint.

Sections returned:
    summary, stages, candidates, review, signals, decisions, capacity,
    next_action, warnings, safety, provenance
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from paper_trader.config import get_settings
from paper_trader.db.models import (
    CandidateReview,
    Order,
    Portfolio,
    Position,
    Signal,
    TradeDecision,
)
from paper_trader.db.session import get_session

from paper_trader.api import command_center as cc
from paper_trader.api import daily_operating_run as dor

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

PHASE = "14-B"

_REVIEW_SOURCE_PREFIX = cc._REVIEW_SOURCE_PREFIX

# Six operating stages (in order).
ST_DATA = "DATA"
ST_CANDIDATES = "CANDIDATES"
ST_REVIEW = "REVIEW"
ST_SIGNALS = "SIGNALS"
ST_DECISIONS = "DECISIONS"
ST_PORTFOLIO = "PORTFOLIO"
STAGE_ORDER = [ST_DATA, ST_CANDIDATES, ST_REVIEW, ST_SIGNALS, ST_DECISIONS, ST_PORTFOLIO]

# Stage status enum.
S_COMPLETE = "COMPLETE"
S_READY = "READY"
S_NEEDS_ACTION = "NEEDS_ACTION"
S_BLOCKED = "BLOCKED"
S_NOT_AVAILABLE = "NOT_AVAILABLE"

_STAGE_LABEL = {
    ST_DATA: "Data",
    ST_CANDIDATES: "Candidates",
    ST_REVIEW: "Review",
    ST_SIGNALS: "Signals",
    ST_DECISIONS: "Decisions",
    ST_PORTFOLIO: "Portfolio",
}

# Row display caps (bounded previews — never a giant raw table).
_MAX_QUEUE_ROWS = 25
_MAX_HISTORY_ROWS = 25
_MAX_SIGNAL_ROWS = 25
_MAX_DECISION_ROWS = 25

_CAPACITY_BLOCK_EXPLANATION = (
    "Portfolio capacity is full. The risk engine prevents additional paper "
    "exposure. Review current positions before advancing new paper-trade "
    "decisions."
)


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #

def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _iso(ts: Any) -> Optional[str]:
    if ts is None:
        return None
    try:
        return ts.isoformat()
    except Exception:  # noqa: BLE001
        return str(ts)


def _source_tail(source_run: Any) -> Optional[str]:
    """Human-friendly source-candidate reference from a review source_run label."""
    if not isinstance(source_run, str):
        return None
    if source_run.startswith(_REVIEW_SOURCE_PREFIX):
        return source_run[len(_REVIEW_SOURCE_PREFIX):] or source_run
    return source_run


# --------------------------------------------------------------------------- #
# Active-stage derivation (exactly one active stage)
# --------------------------------------------------------------------------- #

def _market_data_needs_action(market_data: Optional[dict[str, Any]]) -> bool:
    """True when the alignment layer is available and NOT aligned/complete."""
    if not market_data:
        return False
    if market_data.get("status") in (None, "UNAVAILABLE"):
        return False
    if market_data.get("aligned") and market_data.get("coverage_complete") is not False:
        return False
    return True


def _derive_active_stage(counts: dict[str, int], *, capacity_full: bool,
                         market_data: Optional[dict[str, Any]] = None) -> str:
    """Pure single active-stage derivation from live counts.

    Stale / misaligned market data takes precedence: downstream review cannot be
    trusted against mixed market dates, so DATA is surfaced first (Preview Daily
    Run). Otherwise priority mirrors the canonical daily workflow: a pending
    review is first, then approved candidates awaiting signal creation, then
    order-eligible or pending paper decisions, then open-position monitoring. Only
    when nothing is actionable do we fall back to CANDIDATES (a scan exists) or
    DATA. Older candidates never select a stage.
    """
    if _market_data_needs_action(market_data):
        return ST_DATA
    if counts["today_pending"] > 0:
        return ST_REVIEW
    if counts["today_approved"] > 0:
        return ST_SIGNALS
    if counts["order_eligible"] > 0 or counts["pending_orders"] > 0:
        return ST_DECISIONS
    if counts["open_positions"] > 0:
        return ST_PORTFOLIO
    if counts["today_total"] > 0:
        return ST_CANDIDATES
    return ST_DATA


def _data_stage_status(
    market_data: Optional[dict[str, Any]], *, has_data: bool
) -> tuple[str, Optional[str], Optional[str]]:
    """DATA stage status driven by the Phase 15-A market-date alignment.

    When market data is stale or misaligned the DATA stage becomes NEEDS_ACTION
    (or BLOCKED if a completed mark cannot be produced) and explains exactly which
    market dates differ; the single action is Preview Daily Run. When aligned it is
    COMPLETE with the completed date and source. Falls back to the legacy
    has-candidate-data check when the alignment layer is unavailable.
    """
    if not market_data:
        return (S_COMPLETE if has_data else S_READY, None, None)
    status = market_data.get("status")
    aligned = bool(market_data.get("aligned"))
    if status == "UNAVAILABLE":
        return (S_COMPLETE if has_data else S_READY, None, None)
    if aligned and market_data.get("coverage_complete") is not False:
        return (S_COMPLETE, None, None)
    blockers = market_data.get("blockers") or []
    mismatches = market_data.get("mismatches") or []
    req = market_data.get("required_market_date")
    port = market_data.get("portfolio_mark_market_date")
    alpha = market_data.get("alpha_top25_market_date")
    detail = (
        f"Market dates differ: portfolio mark {port or 'none'} vs alpha "
        f"{alpha or 'none'}; latest completed {req or 'unknown'}. Run the daily "
        f"operating run to align them."
    )
    if status == "BLOCKED" or (blockers and any("blocked" in str(b).lower() for b in blockers)):
        return (S_BLOCKED, "MARKET_DATA_BLOCKED", detail)
    if status == "PARTIAL_COVERAGE":
        return (S_NEEDS_ACTION, "PARTIAL_COVERAGE",
                "Some open positions have no completed EOD price for the required "
                "market date. Run the daily operating run to complete coverage.")
    if mismatches or status == "STALE":
        return (S_NEEDS_ACTION, "MARKET_DATA_STALE", detail)
    return (S_NEEDS_ACTION, "MARKET_DATA_STALE", detail)


def _stage_status(
    stage: str, counts: dict[str, int], *, active_stage: str, capacity_full: bool,
    market_data: Optional[dict[str, Any]] = None,
) -> tuple[str, Optional[str], Optional[str]]:
    """Return (status, blocker_code, blocker_explanation) for one stage."""
    today_total = counts["today_total"]
    today_pending = counts["today_pending"]
    today_approved = counts["today_approved"]
    order_eligible = counts["order_eligible"]
    pending_orders = counts["pending_orders"]
    signal_count = counts["signal_count"]
    decision_count = counts["decision_count"]
    open_positions = counts["open_positions"]
    has_data = today_total > 0 or open_positions > 0 or counts["total_candidates"] > 0

    if stage == ST_DATA:
        return _data_stage_status(market_data, has_data=has_data)

    if stage == ST_CANDIDATES:
        if today_total > 0:
            return (S_COMPLETE, None, None)
        return (S_READY, None, None)

    if stage == ST_REVIEW:
        if today_pending > 0:
            return (S_NEEDS_ACTION, None, None)
        if today_total > 0:
            return (S_COMPLETE, None, None)
        return (S_READY, None, None)

    if stage == ST_SIGNALS:
        if today_approved > 0:
            return (S_NEEDS_ACTION, None, None)
        if signal_count > 0:
            return (S_COMPLETE, None, None)
        return (S_READY, None, None)

    if stage == ST_DECISIONS:
        if capacity_full and (order_eligible > 0 or today_approved > 0):
            return (S_BLOCKED, cc.CAP_FULL, _CAPACITY_BLOCK_EXPLANATION)
        if order_eligible > 0 or pending_orders > 0:
            return (S_NEEDS_ACTION, None, None)
        if decision_count > 0:
            return (S_COMPLETE, None, None)
        return (S_READY, None, None)

    if stage == ST_PORTFOLIO:
        if open_positions > 0:
            return (S_COMPLETE, None, None)
        return (S_READY, None, None)

    return (S_NOT_AVAILABLE, None, None)


# Per-stage action metadata (deep-link targets align with the UI hash routes).
_STAGE_ACTION = {
    ST_DATA: ("Start Daily Review", "daily-workflow/review"),
    ST_CANDIDATES: ("View Candidates", "research-audit"),
    ST_REVIEW: ("Review Candidates", "daily-workflow/review"),
    ST_SIGNALS: ("Create Paper Signals", "daily-workflow/signals"),
    ST_DECISIONS: ("Review Decisions", "daily-workflow/decisions"),
    ST_PORTFOLIO: ("Open Portfolio", "portfolio/positions"),
}


def _stage_count(stage: str, counts: dict[str, int]) -> int:
    return {
        ST_DATA: counts["today_total"],
        ST_CANDIDATES: counts["today_total"],
        ST_REVIEW: counts["today_pending"],
        ST_SIGNALS: counts["today_approved"],
        ST_DECISIONS: counts["order_eligible"] + counts["pending_orders"],
        ST_PORTFOLIO: counts["open_positions"],
    }.get(stage, 0)


def _stage_enabled(stage: str, counts: dict[str, int]) -> tuple[bool, Optional[str]]:
    """Return (enabled, disabled_reason) for the stage's single action."""
    if stage == ST_DATA:
        return (True, None)
    if stage == ST_CANDIDATES:
        if counts["today_total"] > 0:
            return (True, None)
        return (False, "No candidates have been scanned yet today.")
    if stage == ST_REVIEW:
        if counts["today_pending"] > 0:
            return (True, None)
        return (False, "No candidates are pending review.")
    if stage == ST_SIGNALS:
        if counts["today_approved"] > 0:
            return (True, None)
        return (False, "No approved candidates are ready for signals.")
    if stage == ST_DECISIONS:
        if counts["order_eligible"] > 0 or counts["pending_orders"] > 0:
            return (True, None)
        return (False, "No paper trade decisions are ready to review.")
    if stage == ST_PORTFOLIO:
        if counts["open_positions"] > 0:
            return (True, None)
        return (False, "No open paper positions to monitor.")
    return (False, "Stage unavailable.")


def _build_stages(
    counts: dict[str, int], *, active_stage: str, capacity_full: bool,
    last_signal_date: Optional[str], market_data: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    stages: list[dict[str, Any]] = []
    data_needs_action = _market_data_needs_action(market_data)
    for stage in STAGE_ORDER:
        status, blocker_code, blocker_expl = _stage_status(
            stage, counts, active_stage=active_stage, capacity_full=capacity_full,
            market_data=market_data,
        )
        action_label, action_target = _STAGE_ACTION[stage]
        enabled, disabled_reason = _stage_enabled(stage, counts)
        # Phase 15-A: when market data is stale/misaligned, the DATA stage's single
        # action is Preview Daily Run (routed to the Command Center run card).
        if stage == ST_DATA and data_needs_action:
            action_label, action_target = ("Preview Daily Run", "command-center")
            enabled, disabled_reason = (True, None)
        elif stage == ST_DATA and market_data and market_data.get("status") not in (
                None, "UNAVAILABLE") and status == S_COMPLETE:
            # Phase 15-B: when aligned, the DATA action is non-corrective — it opens the
            # already-complete daily run rather than implying corrective work is needed.
            action_label, action_target = ("View Daily Run", "command-center")
        last_completed = None
        if stage in (ST_DATA, ST_CANDIDATES, ST_REVIEW) and status == S_COMPLETE:
            last_completed = last_signal_date
        if stage == ST_DATA and status == S_COMPLETE and market_data:
            last_completed = market_data.get("required_market_date") or last_completed
        stages.append({
            "stage": stage,
            "label": _STAGE_LABEL[stage],
            "status": status,
            "count": _stage_count(stage, counts),
            "last_completed_at": last_completed,
            "blocker_code": blocker_code,
            "blocker_explanation": blocker_expl,
            "action_label": action_label,
            "action_target": action_target,
            "enabled": enabled,
            "disabled_reason": disabled_reason,
            "is_active": stage == active_stage,
        })
    return stages


# --------------------------------------------------------------------------- #
# Candidate / review display (active queue vs recent history)
# --------------------------------------------------------------------------- #

def _candidate_view(cr: CandidateReview) -> dict[str, Any]:
    return {
        "candidate_id": str(cr.id),
        "ticker": cr.ticker,
        "review_status": cr.review_status,
        "preview_decision": cr.preview_decision,
        "preview_score": cc._num(cr.preview_score),
        "prediction_recommendation": cr.prediction_recommendation,
        "prediction_confidence": cc._num(cr.prediction_confidence),
        "expected_return_pct": cc._num(cr.expected_return_pct),
        "scan_score": cc._num(cr.scan_score),
        "created_at": _iso(cr.created_at),
        "paper_only": True,
    }


def _group_by_ticker(rows: list[CandidateReview]) -> list[dict[str, Any]]:
    """Group by stable identity (ticker); keep the latest record, count history.

    Deterministic display only — no database deletion or mutation.
    """
    by_ticker: dict[str, list[CandidateReview]] = {}
    for cr in rows:
        by_ticker.setdefault(cr.ticker, []).append(cr)
    grouped: list[dict[str, Any]] = []
    for ticker, crs in by_ticker.items():
        crs_sorted = sorted(crs, key=lambda c: (c.created_at or datetime.min), reverse=True)
        latest = _candidate_view(crs_sorted[0])
        latest["history_count"] = len(crs_sorted) - 1
        grouped.append(latest)
    grouped.sort(key=lambda d: (d.get("created_at") or ""), reverse=True)
    return grouped


def _collect_review(session, *, start_of_day: datetime) -> dict[str, Any]:
    """Split candidates into ACTIVE REVIEW QUEUE vs RECENT REVIEW HISTORY.

    Active queue = today's NEW (currently actionable) candidates only. History =
    rejected / watching / approved / older records. Rejected and completed rows
    are never shown as pending work.
    """
    # Active queue: today + review_status NEW.
    active_rows = (
        session.query(CandidateReview)
        .filter(CandidateReview.created_at >= start_of_day)
        .filter(CandidateReview.review_status == "NEW")
        .order_by(CandidateReview.created_at.desc())
        .all()
    )
    # History: everything else (non-NEW today, or anything older), most recent first.
    history_rows = (
        session.query(CandidateReview)
        .filter(
            (CandidateReview.review_status != "NEW")
            | (CandidateReview.created_at < start_of_day)
        )
        .order_by(CandidateReview.created_at.desc())
        .limit(200)
        .all()
    )

    active_grouped = _group_by_ticker(active_rows)
    history_grouped = _group_by_ticker(history_rows)

    return {
        "grouped_by_identity": True,
        "active_count": len(active_grouped),
        "active_review_queue": active_grouped[:_MAX_QUEUE_ROWS],
        "history_count": len(history_grouped),
        "recent_review_history": history_grouped[:_MAX_HISTORY_ROWS],
        "active_truncated": len(active_grouped) > _MAX_QUEUE_ROWS,
        "history_truncated": len(history_grouped) > _MAX_HISTORY_ROWS,
    }


# --------------------------------------------------------------------------- #
# Signals & decisions preview rows
# --------------------------------------------------------------------------- #

def _collect_signals(session) -> dict[str, Any]:
    rows_q = (
        session.query(Signal)
        .filter(Signal.source_run.startswith(_REVIEW_SOURCE_PREFIX))
        .order_by(Signal.created_at.desc())
        .limit(_MAX_SIGNAL_ROWS)
        .all()
    )
    total = (
        session.query(Signal)
        .filter(Signal.source_run.startswith(_REVIEW_SOURCE_PREFIX))
        .count()
    )
    rows = [{
        "ticker": s.ticker,
        "direction": s.direction,
        "confidence": cc._num(s.confidence),
        "source_candidate": _source_tail(s.source_run),
        "status": s.status,
        "blocker": None,
        "paper_only": True,
    } for s in rows_q]
    return {"count": total, "rows": rows}


def _collect_decisions(session, *, capacity_full: bool) -> dict[str, Any]:
    decisions_q = (
        session.query(TradeDecision, Signal)
        .join(Signal, Signal.id == TradeDecision.signal_id)
        .filter(Signal.source_run.startswith(_REVIEW_SOURCE_PREFIX))
        .order_by(TradeDecision.id.desc())
        .limit(_MAX_DECISION_ROWS)
        .all()
    )
    total = (
        session.query(TradeDecision)
        .join(Signal, Signal.id == TradeDecision.signal_id)
        .filter(Signal.source_run.startswith(_REVIEW_SOURCE_PREFIX))
        .count()
    )

    rows: list[dict[str, Any]] = []
    order_eligible = 0
    for td, sig in decisions_q:
        existing_order = (
            session.query(Order).filter(Order.trade_decision_id == td.id).first()
        )
        is_eligible = (
            td.decision in ("BUY", "SELL")
            and td.approved_qty is not None
            and Decimal(str(td.approved_qty)) > Decimal("0")
            and existing_order is None
        )
        if is_eligible:
            order_eligible += 1

        status = existing_order.status if existing_order is not None else td.decision
        blocker = None
        blocker_explanation = None
        if is_eligible and capacity_full:
            blocker = cc.CAP_FULL
            blocker_explanation = _CAPACITY_BLOCK_EXPLANATION

        rows.append({
            "ticker": td.ticker,
            "direction": td.decision,
            "confidence": cc._num(sig.confidence),
            "source_candidate": _source_tail(sig.source_run),
            "status": status,
            "blocker": blocker,
            "blocker_explanation": blocker_explanation,
            "paper_only": True,
        })

    return {"count": total, "order_eligible_count": order_eligible, "rows": rows}


# --------------------------------------------------------------------------- #
# Next action (stage-scoped, reuses command-centre enums/copy)
# --------------------------------------------------------------------------- #

def _next_action(active_stage: str, counts: dict[str, int], *, capacity_full: bool,
                 market_data: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    if active_stage == ST_DATA and _market_data_needs_action(market_data):
        req = (market_data or {}).get("required_market_date")
        return {
            "action": cc.NA_RUN_REFRESH,
            "title": "Align market data",
            "explanation": (
                "The portfolio mark, portfolio snapshot and current-alpha marks do not "
                f"all share the latest completed market date ({req or 'unknown'}). "
                "Preview the daily operating run to align them — this creates no orders "
                "and no trades."
            ),
            "action_label": "Preview Daily Run",
            "action_target": "command-center",
            "ui_target": "command-center",
            "stage": ST_DATA,
            "safety_context": "Paper preview only — manual review required. No orders, no broker, no automation.",
            "requires_user_action": True,
        }
    if active_stage == ST_REVIEW:
        action = cc.NA_REVIEW_CANDIDATES
        explanation = (
            f"{counts['today_pending']} trade idea(s) from today's scan need manual "
            f"review. Approve, watch, or reject each one — this records your review only."
        )
    elif active_stage == ST_SIGNALS:
        action = cc.NA_CREATE_SIGNALS
        explanation = (
            f"{counts['today_approved']} approved candidate(s) are ready. Create their "
            f"paper signals in preview — no orders and no broker execution."
        )
    elif active_stage == ST_DECISIONS and capacity_full and counts["order_eligible"] > 0:
        action = cc.NA_RESOLVE_CAPACITY
        explanation = _CAPACITY_BLOCK_EXPLANATION
    elif active_stage == ST_DECISIONS:
        action = cc.NA_REVIEW_DECISIONS
        explanation = (
            f"{counts['order_eligible']} paper trade decision(s) are ready to review. "
            f"This stays in the paper portfolio — no live trade is placed."
        )
    elif active_stage == ST_PORTFOLIO:
        action = cc.NA_MONITOR
        explanation = (
            "No trade ideas need action right now. Monitor the open paper positions "
            "for any review-for-exit conditions."
        )
    else:  # DATA / CANDIDATES
        action = cc.NA_RUN_REFRESH
        explanation = (
            "No current trade ideas are pending. Start a new daily review to scan for "
            "fresh candidates — this creates no orders and no trades."
        )

    copy = cc._NEXT_ACTION_COPY.get(action, {})
    stage_target = _STAGE_ACTION.get(active_stage, ("Open", "daily-workflow"))[1]
    return {
        "action": action,
        "title": copy.get("title", "Next action"),
        "explanation": explanation,
        "action_label": copy.get("action_label", "Open"),
        "action_target": stage_target,
        "ui_target": copy.get("ui_target", "daily-workflow"),
        "stage": active_stage,
        "safety_context": "Paper preview only — manual review required. No orders, no broker, no automation.",
        "requires_user_action": action != cc.NA_NONE,
    }


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #

def _degraded(reason: str) -> dict[str, Any]:
    settings = get_settings()
    counts = {
        "total_candidates": 0, "today_total": 0, "today_pending": 0, "today_approved": 0,
        "older_count": 0, "decision_count": 0, "signal_count": 0, "order_eligible": 0,
        "pending_orders": 0, "filled_orders": 0, "open_positions": 0,
    }
    capacity = {
        "open_positions": 0, "max_positions": settings.max_positions, "available_slots": 0,
        "capacity_state": cc.CAP_EMPTY, "capacity_explanation": "Workflow state unavailable.",
        "pending_paper_orders": 0,
    }
    stages = _build_stages(counts, active_stage=ST_DATA, capacity_full=False, last_signal_date=None)
    return {
        "status": "DEGRADED",
        "summary": {
            "active_stage": ST_DATA, "active_stage_label": _STAGE_LABEL[ST_DATA],
            "stage_count": len(STAGE_ORDER), "review_queue_count": 0, "approved_count": 0,
            "order_eligible_count": 0, "pending_order_count": 0, "open_position_count": 0,
            "actionable_count": 0, "blocked_count": 0, "signal_date": None,
            "generated_at": _now_iso(), "unavailable_reason": reason,
        },
        "stages": stages,
        "candidates": {"today_total": 0, "today_pending": 0, "today_approved": 0,
                       "older_count": 0, "active_count": 0},
        "review": {"grouped_by_identity": True, "active_count": 0, "active_review_queue": [],
                   "history_count": 0, "recent_review_history": [],
                   "active_truncated": False, "history_truncated": False},
        "signals": {"count": 0, "rows": []},
        "decisions": {"count": 0, "order_eligible_count": 0, "rows": []},
        "capacity": capacity,
        "market_data": None,
        "next_action": _next_action(ST_DATA, counts, capacity_full=False),
        "warnings": [reason],
        "safety": cc._safety_block(),
        "provenance": _provenance(),
    }


def _provenance() -> dict[str, Any]:
    return {
        "phase": PHASE,
        "generated_at": _now_iso(),
        "read_only": True,
        "wrote_to_database": False,
        "created_orders": False,
        "created_signals": False,
        "created_trade_decisions": False,
        "invoked_daily_refresh": False,
        "called_prediction_service": False,
        "called_external_provider": False,
        "made_loopback_http_calls": False,
        "sources": [
            "db:current_workflow_state_counts",
            "db:candidate_reviews",
            "db:review_created_signals",
            "db:review_created_trade_decisions",
            "helper:command_center._capacity_state",
        ],
    }


def _market_data(warnings: list[str]) -> Optional[dict[str, Any]]:
    """Read-only Phase 15-A market-date alignment slice for the DATA stage."""
    try:
        dr = dor.load_daily_operating_run_status()
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Market-date alignment unavailable: {str(exc)[:160]}")
        return None
    alignment = dr.get("alignment") or {}
    return {
        "status": dr.get("status"),
        "aligned": bool(alignment.get("aligned")),
        "required_market_date": dr.get("required_market_date"),
        "portfolio_mark_market_date": alignment.get("price_snapshot_market_date"),
        "portfolio_snapshot_market_date": alignment.get("portfolio_snapshot_market_date"),
        "alpha_top25_market_date": alignment.get("alpha_top25_market_date"),
        "alpha_top50_market_date": alignment.get("alpha_top50_market_date"),
        "spy_market_date": alignment.get("spy_market_date"),
        "coverage_complete": dr.get("coverage_complete"),
        "freshness_status": dr.get("freshness_status"),
        "mismatches": alignment.get("mismatches") or [],
        "blockers": dr.get("blockers") or [],
        "confirmation_required": dr.get("confirmation_required"),
        "prediction_checked": False,
    }


def load_daily_workflow_dashboard() -> dict[str, Any]:
    """Aggregate the read-only Daily Workflow view model.

    Never raises: each dependency is isolated so a single failure becomes a
    warning and a degraded section rather than an HTTP 500. Performs no writes,
    no prediction call, and no external provider call.
    """
    warnings: list[str] = []
    today = date.today()
    start_of_day = datetime(today.year, today.month, today.day)

    counts: Optional[dict[str, int]] = None
    portfolio_data: Optional[dict[str, Any]] = None
    review: Optional[dict[str, Any]] = None
    signals: Optional[dict[str, Any]] = None
    decisions: Optional[dict[str, Any]] = None

    try:
        with get_session() as session:
            try:
                counts = cc._collect_workflow_counts(session)
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"Workflow counts unavailable: {str(exc)[:160]}")
            try:
                pend = (counts or {}).get("pending_orders", 0)
                portfolio_data = cc._collect_portfolio(session, pending_orders=pend)
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"Portfolio/capacity unavailable: {str(exc)[:160]}")

            capacity_full = (portfolio_data or {}).get("capacity_state") == cc.CAP_FULL
            try:
                review = _collect_review(session, start_of_day=start_of_day)
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"Review queue unavailable: {str(exc)[:160]}")
            try:
                signals = _collect_signals(session)
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"Signals preview unavailable: {str(exc)[:160]}")
            try:
                decisions = _collect_decisions(session, capacity_full=capacity_full)
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"Decisions preview unavailable: {str(exc)[:160]}")
    except Exception as exc:  # noqa: BLE001
        return _degraded(f"Backend/database unavailable: {str(exc)[:160]}")

    if counts is None:
        return _degraded("Workflow counts unavailable.")

    settings = get_settings()
    if portfolio_data is None:
        portfolio_data = {
            "open_positions": 0, "max_positions": settings.max_positions,
            "available_slots": 0, "capacity_state": cc.CAP_EMPTY,
            "capacity_explanation": "Portfolio state unavailable.",
            "pending_paper_orders": counts.get("pending_orders", 0),
        }

    capacity_full = portfolio_data.get("capacity_state") == cc.CAP_FULL

    # Phase 15-A: market-date alignment drives the DATA stage (read-only).
    market_data = _market_data(warnings)

    active_stage = _derive_active_stage(
        counts, capacity_full=capacity_full, market_data=market_data)

    # Latest review-created signal market_date (best-effort completion timestamp).
    last_signal_date: Optional[str] = None

    stages = _build_stages(
        counts, active_stage=active_stage, capacity_full=capacity_full,
        last_signal_date=last_signal_date, market_data=market_data,
    )

    if review is None:
        review = {"grouped_by_identity": True, "active_count": 0, "active_review_queue": [],
                  "history_count": 0, "recent_review_history": [],
                  "active_truncated": False, "history_truncated": False}
    if signals is None:
        signals = {"count": 0, "rows": []}
    if decisions is None:
        decisions = {"count": 0, "order_eligible_count": 0, "rows": []}

    actionable = counts["today_pending"] + counts["today_approved"] + counts["order_eligible"]
    blocked = (counts["today_approved"] + counts["order_eligible"]) if capacity_full else 0

    capacity = {
        "open_positions": portfolio_data.get("open_positions", 0),
        "max_positions": portfolio_data.get("max_positions", settings.max_positions),
        "available_slots": portfolio_data.get("available_slots", 0),
        "capacity_state": portfolio_data.get("capacity_state", cc.CAP_EMPTY),
        "capacity_explanation": portfolio_data.get("capacity_explanation", ""),
        "pending_paper_orders": portfolio_data.get("pending_paper_orders", counts.get("pending_orders", 0)),
    }

    summary = {
        "active_stage": active_stage,
        "active_stage_label": _STAGE_LABEL[active_stage],
        "stage_count": len(STAGE_ORDER),
        "review_queue_count": counts["today_pending"],
        "approved_count": counts["today_approved"],
        "order_eligible_count": counts["order_eligible"],
        "pending_order_count": counts["pending_orders"],
        "open_position_count": counts["open_positions"],
        "actionable_count": actionable,
        "blocked_count": blocked,
        "signal_date": last_signal_date,
        "generated_at": _now_iso(),
    }

    if capacity_full and blocked > 0:
        warnings.append(
            "Portfolio at MAX_POSITIONS_REACHED — pending paper decisions are blocked."
        )

    candidates = {
        "today_total": counts["today_total"],
        "today_pending": counts["today_pending"],
        "today_approved": counts["today_approved"],
        "older_count": counts["older_count"],
        "active_count": review.get("active_count", 0),
    }

    return {
        "status": "OK",
        "summary": summary,
        "stages": stages,
        "candidates": candidates,
        "review": review,
        "signals": signals,
        "decisions": decisions,
        "capacity": capacity,
        "market_data": market_data,
        "next_action": _next_action(active_stage, counts, capacity_full=capacity_full,
                                    market_data=market_data),
        "warnings": warnings,
        "safety": cc._safety_block(),
        "provenance": _provenance(),
    }


__all__ = [
    "load_daily_workflow_dashboard",
    "_derive_active_stage",
    "_build_stages",
    "_collect_review",
    "_next_action",
    "STAGE_ORDER",
    "ST_DATA", "ST_CANDIDATES", "ST_REVIEW", "ST_SIGNALS", "ST_DECISIONS", "ST_PORTFOLIO",
    "S_COMPLETE", "S_READY", "S_NEEDS_ACTION", "S_BLOCKED", "S_NOT_AVAILABLE",
]
