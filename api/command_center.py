"""
api/command_center.py — Phase 14-A read-only Trading Command Center view model.

Aggregates the state a trader needs to answer, within five seconds, the daily
operating questions:

    Is the system ready?  Is the market data fresh?  Which alpha book is primary?
    How is it performing?  What is the current risk / capacity state?  What needs
    to be done next?  Is anything blocked?  Which action should be clicked?

It composes EXISTING internal service functions only. It makes NO loopback HTTP
calls to Paper Trader itself, performs NO database writes, invokes NO daily
refresh, calls NO prediction provider, and creates NO signals / decisions /
orders. Every section is wrapped so a single failing dependency degrades to a
``warnings[]`` entry instead of failing the whole endpoint.

Sections returned:
    system, alpha, workflow, portfolio, safety, next_action, warnings, provenance

The prediction tunnel is REPORTED (configured target) but never probed — the
aggregation stays fully offline and test-safe. Every value here is preview /
paper-only and promotes nothing to live trading.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from paper_trader.config import get_settings
from paper_trader.db.models import (
    CandidateReview,
    Order,
    Portfolio,
    PortfolioSnapshot,
    Position,
    Signal,
    TradeDecision,
)
from paper_trader.db.session import get_session
from paper_trader.api.current_alpha_decision_gate import load_current_alpha_decision_gate
from paper_trader.api.portfolio_valuation import load_portfolio_valuation
from paper_trader.workflows.decision import _latest_price

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

PHASE = "14-A"

# Review-created signal source prefix (mirrors app.get_current_workflow_state).
_REVIEW_SOURCE_PREFIX = "review_queue_create_signals_v1:"

# Next-action enum (exactly one is selected).
NA_RUN_REFRESH = "RUN_DAILY_ALPHA_REFRESH"
NA_LOAD_ALPHA = "LOAD_CURRENT_ALPHA"
NA_REVIEW_CANDIDATES = "REVIEW_CANDIDATES"
NA_CREATE_SIGNALS = "CREATE_SIGNALS_PREVIEW"
NA_REVIEW_DECISIONS = "REVIEW_DECISIONS"
NA_MONITOR = "MONITOR_PORTFOLIO"
NA_RESOLVE_CAPACITY = "RESOLVE_CAPACITY_BLOCK"
NA_REFRESH_APP = "REFRESH_APPLICATION_DATA"
NA_NONE = "NO_ACTION_REQUIRED"

# Portfolio capacity state enum.
CAP_EMPTY = "NO_OPEN_POSITIONS"
CAP_AVAILABLE = "CAPACITY_AVAILABLE"
CAP_FULL = "MAX_POSITIONS_REACHED"

# Current-alpha gate status that means "usable decision available".
_ALPHA_READY_STATUS = "DECISION_READY"

# Safety — always-on, paper-only.
SAFETY_BADGES = [
    "MANUAL REVIEW",
    "PAPER ONLY",
    "NO BROKER EXECUTION",
    "AUTOMATION OFF",
    "NO LIVE ORDERS",
]
SAFETY_MODE = "PAPER_PREVIEW_MANUAL_REVIEW"


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #

def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _num(x: Any) -> Optional[float]:
    if isinstance(x, bool) or x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(str(x))
    except (TypeError, ValueError):
        return None


def _safety_block() -> dict[str, Any]:
    """The always-on paper-only safety contract for the command center."""
    return {
        "manual_review": True,
        "paper_only": True,
        "no_broker_execution": True,
        "automation_off": True,
        "no_live_orders": True,
        "creates_orders": False,
        "creates_signals": False,
        "creates_trade_decisions": False,
        "is_live_trading_approval": False,
        "safety_mode": SAFETY_MODE,
        "safety_badges": list(SAFETY_BADGES),
    }


# --------------------------------------------------------------------------- #
# Capacity
# --------------------------------------------------------------------------- #

def _capacity_state(open_positions: int, max_positions: int) -> tuple[str, str]:
    """Return (capacity_state, plain-language explanation).

    MAX_POSITIONS_REACHED is explained as a risk-engine guard, never as a prompt
    to place a live order.
    """
    if max_positions <= 0:
        return CAP_FULL, (
            "Portfolio capacity is full. The risk engine is preventing additional "
            "exposure. Review current positions before creating new paper-trade "
            "decisions."
        )
    if open_positions <= 0:
        return CAP_EMPTY, (
            f"No open positions. Capacity for up to {max_positions} paper "
            f"position(s)."
        )
    available = max(0, max_positions - open_positions)
    if available <= 0:
        return CAP_FULL, (
            "Portfolio capacity is full. The risk engine is preventing additional "
            "exposure. Review current positions before creating new paper-trade "
            "decisions."
        )
    return CAP_AVAILABLE, (
        f"{available} of {max_positions} paper position slot(s) free. New paper "
        f"trades are possible (capacity only — manual review still required)."
    )


# --------------------------------------------------------------------------- #
# Alpha section (from the read-only Phase 13-J decision gate)
# --------------------------------------------------------------------------- #

def _book_summary(book: dict[str, Any] | None) -> dict[str, Any]:
    book = book or {}
    return {
        "label": book.get("label"),
        "book_id": book.get("book_id"),
        "current_return_pct": _num(book.get("current_return_pct")),
        "current_excess_return_pct_points": _num(
            book.get("current_excess_return_pct_points")
        ),
        "spy_cumulative_return_pct": _num(book.get("spy_cumulative_return_pct")),
        "current_drawdown_pct": _num(book.get("current_drawdown_pct")),
        "max_drawdown_pct": _num(book.get("max_drawdown_pct")),
        "contributor_concentration_top5_pct": _num(
            book.get("contributor_concentration_top5_pct")
        ),
        "latest_coverage_pct": _num(book.get("latest_coverage_pct")),
        "risk_trigger_count": len(book.get("risk_triggers") or []),
    }


def _alpha_name_from_book_id(book_id: Any) -> Optional[str]:
    if isinstance(book_id, str) and "__" in book_id:
        return book_id.split("__", 1)[0]
    return None


def _alpha_section(gate: dict[str, Any]) -> dict[str, Any]:
    """Normalize the current-alpha slice from the decision-gate payload."""
    status = gate.get("status")
    available = status == _ALPHA_READY_STATUS

    mf = gate.get("mark_freshness") or {}
    freshness_status = mf.get("mark_freshness_status")
    mark_stale = isinstance(freshness_status, str) and freshness_status.startswith("STALE")

    primary = gate.get("primary_paper_book") or {}
    challenger = gate.get("challenger_paper_book") or {}
    top25 = gate.get("top25") or {}
    top50 = gate.get("top50") or {}
    readiness = gate.get("quarterly_rebalance_readiness") or {}
    risk_review = gate.get("risk_review") or {}

    alpha_name = (
        _alpha_name_from_book_id(primary.get("book_id"))
        or _alpha_name_from_book_id(top50.get("book_id"))
        or _alpha_name_from_book_id(top25.get("book_id"))
    )

    return {
        "available": available,
        "gate_status": status,
        "alpha_name": alpha_name,
        "signal_date": gate.get("signal_date"),
        "latest_mark_date": gate.get("latest_mark_date"),
        "mark_freshness_status": freshness_status,
        "mark_age_calendar_days": mf.get("mark_age_calendar_days"),
        "mark_stale": mark_stale,
        "decision": gate.get("decision"),
        "decision_label": gate.get("decision_label"),
        "book_role_status": gate.get("book_role_status"),
        "primary_paper_book": {
            "status": primary.get("status"),
            "book": primary.get("book"),
            "book_id": primary.get("book_id"),
        },
        "challenger_paper_book": (
            {"label": challenger.get("label"), "book_id": challenger.get("book_id")}
            if challenger else None
        ),
        "top25": _book_summary(top25),
        "top50": _book_summary(top50),
        "risk_any_breach": bool(risk_review.get("any_breach")),
        "rebalance_readiness_status": readiness.get("readiness_status"),
        "remaining_trading_days": readiness.get("remaining_trading_days"),
        "observation_count": gate.get("observation_count"),
    }


def _degraded_alpha(reason: str) -> dict[str, Any]:
    return {
        "available": False,
        "gate_status": "UNAVAILABLE",
        "alpha_name": None,
        "signal_date": None,
        "latest_mark_date": None,
        "mark_freshness_status": None,
        "mark_age_calendar_days": None,
        "mark_stale": False,
        "decision": None,
        "decision_label": None,
        "book_role_status": None,
        "primary_paper_book": {"status": "NO_PRIMARY_BOOK_YET", "book": None, "book_id": None},
        "challenger_paper_book": None,
        "top25": _book_summary(None),
        "top50": _book_summary(None),
        "risk_any_breach": False,
        "rebalance_readiness_status": None,
        "remaining_trading_days": None,
        "observation_count": None,
        "unavailable_reason": reason,
    }


# --------------------------------------------------------------------------- #
# Workflow section (read-only counts — mirrors get_current_workflow_state)
# --------------------------------------------------------------------------- #

def _derive_stage(
    *,
    today_pending: int,
    today_approved: int,
    order_eligible: int,
    pending_orders: int,
    filled_orders: int,
    open_positions: int,
    today_total: int,
) -> str:
    """Pure single-stage derivation (kept local to avoid an app.py import cycle;
    priority mirrors app._derive_workflow_stage)."""
    if today_pending > 0:
        return "REVIEW_CANDIDATES"
    if pending_orders > 0:
        return "FILL_PAPER_ORDER"
    if order_eligible > 0:
        return "CREATE_PAPER_ORDER"
    if today_approved > 0:
        return "GENERATE_TRADE_PLAN"
    if filled_orders > 0 and open_positions > 0:
        return "PAPER_TRADE_COMPLETED"
    if today_total > 0:
        return "NO_TRADE_PLAN"
    if open_positions > 0:
        return "MONITOR_PORTFOLIO"
    return "NEEDS_DAILY_REVIEW"


_STAGE_FLOW = {
    "NEEDS_DAILY_REVIEW": ("None", "DATA_REFRESH"),
    "REVIEW_CANDIDATES": ("CANDIDATES", "REVIEW"),
    "GENERATE_TRADE_PLAN": ("REVIEW", "SIGNALS"),
    "CREATE_PAPER_ORDER": ("SIGNALS", "DECISIONS"),
    "FILL_PAPER_ORDER": ("DECISIONS", "PORTFOLIO"),
    "PAPER_TRADE_COMPLETED": ("PORTFOLIO", "PORTFOLIO"),
    "NO_TRADE_PLAN": ("REVIEW", "PORTFOLIO"),
    "MONITOR_PORTFOLIO": ("REVIEW", "PORTFOLIO"),
}


def _workflow_section(counts: dict[str, int], capacity_full: bool) -> dict[str, Any]:
    """Assemble the daily-workflow slice from raw live counts (pure)."""
    today_pending = counts["today_pending"]
    today_approved = counts["today_approved"]
    order_eligible = counts["order_eligible"]
    pending_orders = counts["pending_orders"]
    filled_orders = counts["filled_orders"]
    open_positions = counts["open_positions"]
    today_total = counts["today_total"]

    stage = _derive_stage(
        today_pending=today_pending,
        today_approved=today_approved,
        order_eligible=order_eligible,
        pending_orders=pending_orders,
        filled_orders=filled_orders,
        open_positions=open_positions,
        today_total=today_total,
    )
    last_completed, next_required = _STAGE_FLOW.get(stage, ("None", "DATA_REFRESH"))

    actionable = today_pending + today_approved + order_eligible
    # Work that cannot become a new paper position because capacity is full.
    blocked = (today_approved + order_eligible) if capacity_full else 0
    blocker = None
    if capacity_full and blocked > 0:
        blocker = (
            "Portfolio capacity is full (MAX_POSITIONS_REACHED). New paper "
            "positions are blocked by the risk engine until a slot is freed."
        )

    return {
        "candidate_count": counts["total_candidates"],
        "today_candidate_count": today_total,
        "review_queue_count": today_pending,
        "approved_count": today_approved,
        "signal_count": counts["signal_count"],
        "decision_count": counts["decision_count"],
        "order_eligible_count": order_eligible,
        "pending_order_count": pending_orders,
        "actionable_count": actionable,
        "blocked_count": blocked,
        "older_candidate_count": counts["older_count"],
        "stage": stage,
        "last_completed_stage": last_completed,
        "next_required_stage": next_required,
        "current_blocker": blocker,
    }


def _degraded_workflow(reason: str) -> dict[str, Any]:
    return {
        "candidate_count": None,
        "today_candidate_count": None,
        "review_queue_count": None,
        "approved_count": None,
        "signal_count": None,
        "decision_count": None,
        "order_eligible_count": None,
        "pending_order_count": None,
        "actionable_count": None,
        "blocked_count": None,
        "older_candidate_count": None,
        "stage": "UNAVAILABLE",
        "last_completed_stage": None,
        "next_required_stage": None,
        "current_blocker": None,
        "unavailable_reason": reason,
    }


# --------------------------------------------------------------------------- #
# DB collectors (READ ONLY)
# --------------------------------------------------------------------------- #

def _collect_workflow_counts(session) -> dict[str, int]:
    """Replicate the read-only counts used by /v1/review/current-workflow-state."""
    from datetime import date, datetime as _dt

    today = date.today()
    start_of_day = _dt(today.year, today.month, today.day)

    total_candidates = session.query(CandidateReview).count()
    today_q = session.query(CandidateReview).filter(
        CandidateReview.created_at >= start_of_day
    )
    today_total = today_q.count()
    today_pending = today_q.filter(CandidateReview.review_status == "NEW").count()
    today_approved = today_q.filter(
        CandidateReview.review_status == "APPROVED_FOR_SIGNAL"
    ).count()
    older_count = total_candidates - today_total

    review_decisions = (
        session.query(TradeDecision)
        .join(Signal, Signal.id == TradeDecision.signal_id)
        .filter(Signal.source_run.startswith(_REVIEW_SOURCE_PREFIX))
    )
    decision_count = review_decisions.count()
    signal_count = (
        session.query(Signal)
        .filter(Signal.source_run.startswith(_REVIEW_SOURCE_PREFIX))
        .count()
    )

    order_eligible = 0
    for td in review_decisions.filter(
        TradeDecision.decision.in_(["BUY", "SELL"]),
        TradeDecision.approved_qty > Decimal("0"),
    ).all():
        existing = session.query(Order).filter(Order.trade_decision_id == td.id).first()
        if existing is None:
            order_eligible += 1

    pending_orders = session.query(Order).filter(Order.status == "PENDING").count()
    filled_orders = session.query(Order).filter(Order.status == "FILLED").count()
    open_positions = session.query(Position).count()

    return {
        "total_candidates": total_candidates,
        "today_total": today_total,
        "today_pending": today_pending,
        "today_approved": today_approved,
        "older_count": older_count,
        "decision_count": decision_count,
        "signal_count": signal_count,
        "order_eligible": order_eligible,
        "pending_orders": pending_orders,
        "filled_orders": filled_orders,
        "open_positions": open_positions,
    }


def _collect_portfolio(session, *, pending_orders: int,
                       valuation: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """Read-only portfolio + capacity + risk roll-up.

    All CURRENT valuation fields (total value, cash, invested value, current
    return, current unrealized P&L, per-position status) come from the canonical
    ``portfolio_valuation`` result so Command Center and Portfolio Terminal show
    the same numbers. ``latest_performance_return_pct`` remains the SEPARATE
    latest-official-snapshot return — never mixed into the current mark.
    """
    valuation = valuation or {}
    cm = valuation.get("current_mark") or {}
    vpos = valuation.get("positions") or []
    snapshot = valuation.get("latest_snapshot") or None

    portfolio = session.query(Portfolio).first()
    if portfolio is None:
        return {
            "seeded": False,
            "open_positions": 0,
            "max_positions": get_settings().max_positions,
            "available_slots": 0,
            "capacity_state": CAP_EMPTY,
            "capacity_explanation": "Portfolio not seeded.",
            "pending_paper_orders": pending_orders,
            "hold_count": 0,
            "watch_count": 0,
            "review_for_exit_count": 0,
            "risk_message": "Portfolio not seeded.",
            "total_value": None,
            "cash": None,
            "invested_value": None,
            "total_return_pct": None,
            "unrealized_pnl": None,
            "as_of_market_date": None,
            "price_source": None,
            "freshness_status": None,
            "valuation_complete": None,
            "latest_performance_return_pct": None,
        }

    cfg_max = int(
        (portfolio.config or {}).get("max_positions", get_settings().max_positions)
    )
    # Status counts from the CANONICAL positions (a PRICE_UNAVAILABLE position is
    # counted as WATCH here, matching the prior command-center behaviour).
    open_count = cm.get("total_position_count")
    if open_count is None:
        open_count = session.query(Position).count()
    hold = watch = review_for_exit = 0
    for r in vpos:
        s = r.get("status")
        if s == "HOLD":
            hold += 1
        elif s == "REVIEW_FOR_EXIT":
            review_for_exit += 1
        else:  # WATCH or PRICE_UNAVAILABLE
            watch += 1

    capacity_state, capacity_explanation = _capacity_state(open_count, cfg_max)
    available_slots = max(0, cfg_max - open_count)

    if review_for_exit > 0:
        risk_message = f"Exit review required for {review_for_exit} position(s)."
    elif watch > 0:
        risk_message = f"Watch required for {watch} position(s)."
    elif open_count > 0:
        risk_message = "Portfolio healthy. Monitor only."
    else:
        risk_message = "No open positions."

    # SEPARATE latest official snapshot return (never the current mark).
    latest_return_pct: Optional[float] = None
    if snapshot is not None:
        latest_return_pct = _num(snapshot.get("cumulative_return_pct"))

    return {
        "seeded": True,
        "open_positions": open_count,
        "max_positions": cfg_max,
        "available_slots": available_slots,
        "capacity_state": capacity_state,
        "capacity_explanation": capacity_explanation,
        "pending_paper_orders": pending_orders,
        "hold_count": hold,
        "watch_count": watch,
        "review_for_exit_count": review_for_exit,
        "risk_message": risk_message,
        # canonical CURRENT mark (CURRENT_MARKED_EOD)
        "total_value": cm.get("current_total_value"),
        "cash": cm.get("current_cash"),
        "invested_value": cm.get("current_positions_value"),
        "total_return_pct": cm.get("current_total_return_pct"),
        "unrealized_pnl": cm.get("current_unrealized_pnl"),
        "as_of_market_date": cm.get("as_of_market_date"),
        "price_source": cm.get("price_source"),
        "freshness_status": cm.get("freshness_status"),
        "valuation_complete": cm.get("valuation_complete"),
        # SEPARATE official snapshot return
        "latest_performance_return_pct": latest_return_pct,
    }


# --------------------------------------------------------------------------- #
# Next action (single primary action)
# --------------------------------------------------------------------------- #

_NEXT_ACTION_COPY: dict[str, dict[str, str]] = {
    NA_REFRESH_APP: {
        "title": "Refresh application data",
        "action_label": "Refresh Application Data",
        "ui_target": "command-center",
    },
    NA_REVIEW_CANDIDATES: {
        "title": "Review today's trade ideas",
        "action_label": "Review Candidates",
        "ui_target": "daily-workflow",
    },
    NA_CREATE_SIGNALS: {
        "title": "Create paper signals from approved candidates",
        "action_label": "Create Signals (Preview)",
        "ui_target": "daily-workflow",
    },
    NA_REVIEW_DECISIONS: {
        "title": "Review paper trade decisions",
        "action_label": "Review Decisions",
        "ui_target": "daily-workflow",
    },
    NA_RESOLVE_CAPACITY: {
        "title": "Resolve portfolio capacity block",
        "action_label": "Review Open Positions",
        "ui_target": "portfolio",
    },
    NA_RUN_REFRESH: {
        "title": "Run the daily alpha mark refresh",
        "action_label": "Run Daily Alpha Refresh",
        "ui_target": "research-audit",
    },
    NA_LOAD_ALPHA: {
        "title": "Load the current alpha paper test",
        "action_label": "Load Current Alpha",
        "ui_target": "research-audit",
    },
    NA_MONITOR: {
        "title": "Monitor the paper portfolio",
        "action_label": "Monitor Portfolio",
        "ui_target": "portfolio",
    },
    NA_NONE: {
        "title": "No action required",
        "action_label": "Everything is up to date",
        "ui_target": "command-center",
    },
}


def _select_next_action(
    *,
    system: dict[str, Any],
    alpha: dict[str, Any],
    workflow: dict[str, Any],
    portfolio: dict[str, Any],
) -> dict[str, Any]:
    """Choose exactly ONE primary next action and explain it in plain language.

    Never implies a live order. Priority: system health -> capacity block ->
    workflow actions -> alpha freshness -> monitoring -> idle.
    """
    capacity_full = portfolio.get("capacity_state") == CAP_FULL
    review_queue = workflow.get("review_queue_count") or 0
    approved = workflow.get("approved_count") or 0
    order_eligible = workflow.get("order_eligible_count") or 0
    open_positions = portfolio.get("open_positions") or 0

    if not system.get("backend_ready", False):
        action = NA_REFRESH_APP
        explanation = (
            "The backend is not fully ready. Refresh application data and "
            "reconnect before continuing the daily workflow."
        )
    elif capacity_full and (approved > 0 or order_eligible > 0):
        action = NA_RESOLVE_CAPACITY
        explanation = (
            "Portfolio capacity is full. The risk engine is preventing additional "
            "exposure. Review current positions before creating new paper-trade "
            "decisions."
        )
    elif review_queue > 0:
        action = NA_REVIEW_CANDIDATES
        explanation = (
            f"{review_queue} trade idea(s) from today's scan need manual review. "
            f"Approve, watch, or reject each one — this records your review only."
        )
    elif approved > 0:
        action = NA_CREATE_SIGNALS
        explanation = (
            f"{approved} approved candidate(s) are ready. Create their paper "
            f"signals in preview — no orders and no broker execution."
        )
    elif order_eligible > 0:
        action = NA_REVIEW_DECISIONS
        explanation = (
            f"{order_eligible} paper trade decision(s) are ready to review. This "
            f"stays in the paper portfolio — no live trade is placed."
        )
    elif alpha.get("available") and alpha.get("mark_stale"):
        action = NA_RUN_REFRESH
        age = alpha.get("mark_age_calendar_days")
        age_txt = f" ({age} calendar days old)" if age is not None else ""
        explanation = (
            f"The latest financial mark{age_txt} is stale. Run the manual daily "
            f"alpha mark refresh to bring the paper books up to date."
        )
    elif not alpha.get("available"):
        action = NA_LOAD_ALPHA
        explanation = (
            "No current-alpha decision is available yet. Load the current alpha "
            "paper test to establish the provisional primary paper book."
        )
    elif open_positions > 0:
        action = NA_MONITOR
        explanation = (
            "No trade ideas need action right now. Monitor the open paper "
            "positions for any review-for-exit conditions."
        )
    else:
        action = NA_NONE
        explanation = (
            "The system is ready, the market mark is fresh, and nothing is "
            "pending. No action is required right now."
        )

    copy = _NEXT_ACTION_COPY[action]
    return {
        "action": action,
        "title": copy["title"],
        "explanation": explanation,
        "action_label": copy["action_label"],
        "ui_target": copy["ui_target"],
        "safety_context": "Paper preview only — manual review required. No orders, no broker, no automation.",
        "capacity_context": portfolio.get("capacity_explanation"),
        "requires_user_action": action != NA_NONE,
    }


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #

def load_command_center() -> dict[str, Any]:
    """Aggregate the read-only Trading Command Center view model.

    Never raises: each dependency is isolated so a single failure becomes a
    warning and a degraded section rather than an HTTP 500. Performs no writes,
    no prediction call, and no external provider call.
    """
    warnings: list[str] = []
    settings = get_settings()

    # --- System + DB-backed sections (workflow + portfolio) ---------------- #
    backend_ready = False
    workflow_counts: Optional[dict[str, int]] = None
    portfolio_data: Optional[dict[str, Any]] = None

    # --- Canonical current valuation (single source of truth) -------------- #
    try:
        valuation = load_portfolio_valuation()
    except Exception as exc:  # noqa: BLE001
        valuation = {"current_mark": {}, "positions": [], "latest_snapshot": None}
        warnings.append(f"Canonical valuation unavailable: {str(exc)[:160]}")

    try:
        with get_session() as session:
            session.query(Portfolio).first()  # touch DB -> readiness
            backend_ready = True
            try:
                workflow_counts = _collect_workflow_counts(session)
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"Workflow state unavailable: {str(exc)[:160]}")
            try:
                pend = (workflow_counts or {}).get("pending_orders", 0)
                portfolio_data = _collect_portfolio(session, pending_orders=pend,
                                                    valuation=valuation)
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"Portfolio state unavailable: {str(exc)[:160]}")
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Backend/database unavailable: {str(exc)[:160]}")

    # --- Current alpha (read-only decision gate) --------------------------- #
    try:
        gate = load_current_alpha_decision_gate()
        alpha = _alpha_section(gate)
    except Exception as exc:  # noqa: BLE001
        alpha = _degraded_alpha(f"decision-gate unavailable: {str(exc)[:160]}")
        warnings.append(f"Current alpha unavailable: {str(exc)[:160]}")

    # --- Portfolio fallback ------------------------------------------------ #
    if portfolio_data is None:
        portfolio_data = {
            "seeded": False,
            "open_positions": 0,
            "max_positions": settings.max_positions,
            "available_slots": 0,
            "capacity_state": CAP_EMPTY,
            "capacity_explanation": "Portfolio state unavailable.",
            "pending_paper_orders": 0,
            "hold_count": 0,
            "watch_count": 0,
            "review_for_exit_count": 0,
            "risk_message": "Portfolio state unavailable.",
            "total_value": None,
            "cash": None,
            "invested_value": None,
            "total_return_pct": None,
            "unrealized_pnl": None,
            "as_of_market_date": None,
            "price_source": None,
            "freshness_status": None,
            "valuation_complete": None,
            "latest_performance_return_pct": None,
        }

    capacity_full = portfolio_data.get("capacity_state") == CAP_FULL

    # --- Workflow section -------------------------------------------------- #
    if workflow_counts is not None:
        workflow = _workflow_section(workflow_counts, capacity_full)
    else:
        workflow = _degraded_workflow("workflow counts unavailable")

    # --- System section ---------------------------------------------------- #
    system = {
        "health": "ok",
        "service": "paper_trader",
        "version": "1.0.0",
        "backend_ready": backend_ready,
        "backend_status": "READY" if backend_ready else "DEGRADED",
        "authenticated": True,
        "connected": True,
        "prediction_tunnel": {
            "target": settings.stock_prediction_api_url,
            "probed": False,
            "status": "NOT_PROBED_READ_ONLY",
            "detail": "Reported only — the command center never initiates a prediction.",
        },
        "last_refresh": _now_iso(),
    }

    # --- Warnings from the alpha layer ------------------------------------- #
    if alpha.get("available") and alpha.get("mark_stale"):
        warnings.append(
            "Latest financial mark is stale — run the daily alpha refresh before "
            "relying on the current decision."
        )
    if alpha.get("risk_any_breach"):
        warnings.append("Paper-review risk threshold breached — see the decision gate.")
    if capacity_full and (workflow.get("blocked_count") or 0) > 0:
        warnings.append(
            "Portfolio at MAX_POSITIONS_REACHED — pending paper work is blocked."
        )

    # --- Next action ------------------------------------------------------- #
    next_action = _select_next_action(
        system=system, alpha=alpha, workflow=workflow, portfolio=portfolio_data
    )

    provenance = {
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
            "db:portfolio_analytics_rollup",
            "loader:load_current_alpha_decision_gate",
            "config:stock_prediction_api_url",
        ],
    }

    return {
        "status": "OK" if backend_ready else "DEGRADED",
        "system": system,
        "alpha": alpha,
        "workflow": workflow,
        "portfolio": portfolio_data,
        "safety": _safety_block(),
        "next_action": next_action,
        "warnings": warnings,
        "provenance": provenance,
    }


__all__ = [
    "load_command_center",
    "_select_next_action",
    "_capacity_state",
    "_alpha_section",
    "_workflow_section",
    "_safety_block",
    "NA_RUN_REFRESH",
    "NA_LOAD_ALPHA",
    "NA_REVIEW_CANDIDATES",
    "NA_CREATE_SIGNALS",
    "NA_REVIEW_DECISIONS",
    "NA_MONITOR",
    "NA_RESOLVE_CAPACITY",
    "NA_REFRESH_APP",
    "NA_NONE",
    "CAP_EMPTY",
    "CAP_AVAILABLE",
    "CAP_FULL",
]
