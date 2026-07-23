"""
api/operational_book.py — Phase 27B: ONE operational portfolio, everywhere.

The application exposes three different portfolio concepts:

    1. Alpha Paper Book #1        (desk ledgers, book_id ``alpha_paper_book_1``)
    2. the legacy paper portfolio (DB-backed signal workflow: CDW/HUM, fills,
       snapshots — ARCHIVED, read-only, never the default portfolio again)
    3. research / multi-horizon books (Top25 / Top50 / champion / challenger /
       composite — evidence, never the user's operational holdings)

This module is the single READ-ONLY aggregation that every operational screen
(Command Center, Portfolio, Daily Workflow, Paper Desk, Portfolio Manager)
uses to describe the ONE operational book: **Alpha Paper Book #1**.

It composes EXISTING service functions only (alpha_book status + desk NAV
replay + alpha-target readiness). It performs NO writes, creates NO orders /
signals / decisions / fills, never initializes the book, never confirms a
snapshot, and never calls a prediction service. Cash / NAV / holdings /
pending orders each have exactly ONE producer (the desk ledger replay), so no
two pages can ever disagree about the operational book.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from paper_trader.api import alpha_book as ab
from paper_trader.api import alpha_target as at
from paper_trader.api import paper_trading_desk as desk

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

PHASE = "27B"

OPERATIONAL_BOOK_ID = ab.ALPHA_BOOK_ID              # "alpha_paper_book_1"
OPERATIONAL_BOOK_LABEL = "Alpha Paper Book #1"

ROLE_OPERATIONAL = "OPERATIONAL_BOOK"
ROLE_HISTORICAL = "HISTORICAL_BOOK"
ROLE_RESEARCH = "RESEARCH_BOOK"

STATUS_OK = "OPERATIONAL_BOOK_OK"
ARCHIVE_STATUS_OK = "HISTORICAL_BOOKS_OK"

LEGACY_BOOK_ID = "legacy_paper_portfolio"
LEGACY_BOOK_LABEL = "Legacy Paper Portfolio (Archived)"

# Non-terminal desk order statuses = "pending" from the operator's viewpoint.
_PENDING_ORDER_STATUSES = (desk.ST_PROPOSED, desk.ST_APPROVED, desk.ST_SUBMITTED)

SINGLE_SOURCE_NOTE = (
    "Alpha Paper Book #1 is the ONE operational portfolio. Cash, holdings, "
    "pending orders and NAV are produced exactly once, by replaying the "
    "append-only desk ledgers, and every operational page reads this same "
    "endpoint. The legacy paper portfolio is a read-only historical archive; "
    "research books are evidence and are never the user's holdings."
)

# Research books (identity metadata only — the numbers live in Research & Audit).
RESEARCH_BOOKS = [
    {"book_id": "current_alpha_top25", "label": "Champion composite_sn — Top 25",
     "classification": ROLE_RESEARCH, "route": "research-audit/paper-books"},
    {"book_id": "current_alpha_top50", "label": "Champion composite_sn — Top 50",
     "classification": ROLE_RESEARCH, "route": "research-audit/paper-books"},
    {"book_id": "challenger_top25", "label": "Challenger composite_sn_repaired — Top 25",
     "classification": ROLE_RESEARCH, "route": "research-audit/tournament"},
    {"book_id": "challenger_top50", "label": "Challenger composite_sn_repaired — Top 50",
     "classification": ROLE_RESEARCH, "route": "research-audit/tournament"},
    {"book_id": "fundamental_momentum_50_50_top25",
     "label": "Multi-horizon 50/50 ensemble model book (target source)",
     "classification": ROLE_RESEARCH, "route": "multi-horizon"},
]

RESEARCH_BOOKS_NOTE = (
    "Research books are frozen paper evidence (champion / challenger / model "
    "books). They inform the operational target but are never shown as the "
    "user's operational holdings."
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _default_valuation_loader():
    from paper_trader.api.portfolio_valuation import load_portfolio_valuation
    return load_portfolio_valuation()


# Injectable seam (mirrors alpha_book._VALUATION_LOADER) so tests never need a DB.
_VALUATION_LOADER = _default_valuation_loader


def _safety() -> dict:
    """Read-only safety contract carried by every GET in this module."""
    return {
        "paper_only": True,
        "read_only": True,
        "broker_enabled": False,
        "automation_enabled": False,
        "live_orders_enabled": False,
        "performed_write": False,
        "safety_badges": ["OPERATIONAL BOOK", "PAPER ONLY", "MANUAL REVIEW",
                          "NO ORDERS CREATED", "NO BROKER", "AUTOMATION OFF"],
    }


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(str(x))
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# Phase 27B.9 — scheduled-review cadence (the canonical review clock).
#
# The operational book rebalances on a MONTHLY combined-sleeve cadence. The next
# scheduled review is the first calendar day of the month AFTER the confirmed
# target's market month. `review_due` is True only once that date is reached.
# A newer/unconfirmed alpha target BEFORE that date is informational, never an
# urgent operator action, so a fully-implemented active book is not flagged for
# "verify target" just because fresher research data exists.
# --------------------------------------------------------------------------- #

REVIEW_CADENCE = "MONTHLY"


def _parse_iso_date(s: Any):
    from datetime import date
    if not s:
        return None
    try:
        return date.fromisoformat(str(s)[:10])
    except (TypeError, ValueError):
        return None


def _next_month_first(d):
    from datetime import date
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)


def _derive_review(target_date: Any, valuation_date: Any,
                   today: Optional[str] = None) -> tuple[Optional[str], bool]:
    """Monthly review clock anchored to the confirmed target's market month.

    Returns ``(next_review_date_iso, review_due)``. Pure; degrades to
    ``(None, False)`` when no anchor date is available.
    """
    from datetime import date
    anchor = _parse_iso_date(target_date) or _parse_iso_date(valuation_date)
    if anchor is None:
        return None, False
    nrd = _next_month_first(anchor)
    tdy = _parse_iso_date(today) or date.today()
    return nrd.isoformat(), bool(tdy >= nrd)


def _pending_orders(desk_dir=None) -> dict:
    """Fold the desk order ledger for the operational book (read-only replay)."""
    sdir = desk._desk_dir(desk_dir)
    orders = [o for o in desk._orders_state(sdir).values()
              if o.get("book_id") == OPERATIONAL_BOOK_ID]
    by_status: dict[str, int] = {}
    for o in orders:
        by_status[o["status"]] = by_status.get(o["status"], 0) + 1
    pending = [o for o in orders if o.get("status") in _PENDING_ORDER_STATUSES]
    submission_dates = [o.get("approval_date") for o in orders
                        if o.get("approval_date")
                        and o.get("status") in (desk.ST_APPROVED, desk.ST_SUBMITTED,
                                                desk.ST_FILLED)]
    return {
        "pending_count": len(pending),
        "awaiting_manual_confirmation": by_status.get(desk.ST_PROPOSED, 0),
        "awaiting_fill": (by_status.get(desk.ST_APPROVED, 0)
                          + by_status.get(desk.ST_SUBMITTED, 0)),
        "filled_count": by_status.get(desk.ST_FILLED, 0),
        "total_orders": len(orders),
        "by_status": by_status,
        "latest_submission_date": max(submission_dates) if submission_dates else None,
    }


# --------------------------------------------------------------------------- #
# Phase 27B.8 - per-holding valuation for the operational holdings dashboard.
#
# Composes ONLY data the canonical read model already trusts (ledger-replayed
# holdings + BUY-fill average cost + desk-mark latest prices + confirmed-plan
# sectors + frozen target weights). No schema, no writes, no engine rebuild, no
# prediction service. Degrades to [] on any inconsistency so the canonical
# payload is never broken.
# --------------------------------------------------------------------------- #

def _holding_status(pnl_pct: Optional[float], drift: Optional[float]) -> str:
    """Operator-friendly monitoring status (never a raw internal code). Pure."""
    adr = abs(drift) if drift is not None else None
    if (pnl_pct is not None and pnl_pct <= -0.10) or (adr is not None and adr >= 0.02):
        return "REVIEW"
    if (pnl_pct is not None and pnl_pct <= -0.05) or (adr is not None and adr >= 0.01):
        return "WATCH"
    return "HOLD"


def _prev_common_mark_date(series_map: dict, held: list, as_of: Optional[str]) -> Optional[str]:
    """Latest owned-close strictly before `as_of` that EVERY held name has a price
    at-or-before — so a daily delta is honest (never a partial / fabricated 0)."""
    if not as_of or not held:
        return None
    cand = None
    for tk in held:
        for d, v in (series_map.get(tk) or []):
            if v is not None and d < as_of and (cand is None or d > cand):
                cand = d
    if cand is None:
        return None
    for tk in held:
        if desk._series_price_at_or_before(series_map.get(tk) or [], cand) is None:
            return None
    return cand


def _plan_orders(desk_dir=None) -> list[dict]:
    """The per-name rows of the latest CONFIRMED order plan (sectors + sizing).
    Read-only; degrades to []."""
    try:
        sdir = desk._desk_dir(desk_dir)
        rows = desk._read_ledger(sdir, ab.PLANS_FILE)
        confirmed = [r for r in rows if r.get("event") == "ORDER_PLAN_CONFIRMED"]
        return list(confirmed[-1].get("orders") or []) if confirmed else []
    except Exception:  # noqa: BLE001 - the canonical payload must always load
        return []


def build_holdings_detail(*, book: dict, valuation: dict, fills: list,
                          marks: dict, plan_orders: list,
                          target_weights: dict) -> tuple[list, Optional[str]]:
    """Read-only per-holding valuation of the operational book (Phase 27B.8)."""
    book_id = book.get("book_id")
    qty_map = valuation.get("holdings") or {}
    as_of = valuation.get("as_of_date")
    nav = _f(valuation.get("nav"))
    series = marks.get("series") or {}

    # BUY-fill average cost + first-open date per ticker (append-only replay).
    cost: dict[str, dict] = {}
    for f in fills:
        if f.get("book_id") != book_id:
            continue
        tk = f.get("ticker")
        try:
            q = int(f.get("quantity"))
        except (TypeError, ValueError):
            continue
        c = cost.setdefault(tk, {"buy_qty": 0, "buy_cost": 0.0, "opened": None})
        if f.get("side") == desk.SIDE_BUY:
            c["buy_qty"] += q
            ncd = _f(f.get("net_cash_delta"))
            if ncd is not None:
                c["buy_cost"] += -ncd            # gross + transaction cost actually paid
            fd = f.get("fill_date")
            if fd and (c["opened"] is None or fd < c["opened"]):
                c["opened"] = fd

    sector_map = {o.get("ticker"): o.get("sector") for o in (plan_orders or [])}
    prev_date = _prev_common_mark_date(series, list(qty_map.keys()), as_of)

    rows: list[dict] = []
    for tk, q in qty_map.items():
        c = cost.get(tk) or {}
        buy_qty = c.get("buy_qty") or 0
        avg_cost = (c["buy_cost"] / buy_qty) if buy_qty else None
        cost_basis = (avg_cost * q) if avg_cost is not None else None
        hit = (desk._series_price_at_or_before(series.get(tk) or [], as_of)
               if as_of else None)
        price = hit[1] if hit else None
        mv = (q * price) if price is not None else None
        upnl = (mv - cost_basis) if (mv is not None and cost_basis is not None) else None
        upnl_pct = (upnl / cost_basis) if (upnl is not None and cost_basis) else None
        cw = (mv / nav) if (mv is not None and nav) else None
        tw = _f(target_weights.get(tk))
        drift = (cw - tw) if (cw is not None and tw is not None) else None
        # Daily P&L is only honest once the name was HELD through the prior close
        # (opened strictly before the valuation date) — never fabricate a same-day 0.
        opened = c.get("opened")
        dpnl = None
        if prev_date and price is not None and opened is not None and opened < as_of:
            ph = desk._series_price_at_or_before(series.get(tk) or [], prev_date)
            if ph is not None:
                dpnl = q * (price - ph[1])
        rows.append({
            "ticker": tk,
            "name": None,               # company name not stored on the desk ledgers
            "sector": sector_map.get(tk) or "Unknown",
            "quantity": q,
            "average_cost": (desk._r2(avg_cost) if avg_cost is not None else None),
            "latest_price": (desk._r2(price) if price is not None else None),
            "cost_basis": (desk._r2(cost_basis) if cost_basis is not None else None),
            "market_value": (desk._r2(mv) if mv is not None else None),
            "unrealized_pnl": (desk._r2(upnl) if upnl is not None else None),
            "unrealized_pnl_pct": (round(upnl_pct, 6) if upnl_pct is not None else None),
            "current_weight": (round(cw, 6) if cw is not None else None),
            "target_weight": (round(tw, 6) if tw is not None else None),
            "weight_drift": (round(drift, 6) if drift is not None else None),
            "daily_pnl": (desk._r2(dpnl) if dpnl is not None else None),
            "status": _holding_status(upnl_pct, drift),
            "opened_date": opened,
            "valuation_date": as_of,
        })
    rows.sort(key=lambda r: (r["current_weight"] is None,
                             -(r["current_weight"] or 0.0), r["ticker"]))
    return rows, prev_date


def _portfolio_summary(rows: list, *, cash: Optional[float], nav: Optional[float],
                       prev_date: Optional[str], target_count: Optional[int],
                       implementation_count: int) -> dict:
    """Compact operational summaries for the holdings dashboard. Pure; no I/O."""
    invested = sum(r["market_value"] for r in rows if r["market_value"] is not None)
    cost_basis_total = sum(r["cost_basis"] for r in rows if r["cost_basis"] is not None)
    upnl_total = (invested - cost_basis_total) if rows else None
    uret = (upnl_total / cost_basis_total) if cost_basis_total else None
    # Whole-book daily P&L only when EVERY holding has an honest daily figure.
    daily_ok = bool(rows) and prev_date is not None and all(
        r["daily_pnl"] is not None for r in rows)
    daily_pnl = sum(r["daily_pnl"] for r in rows) if daily_ok else None
    prev_nav = (((cash or 0.0) + invested - daily_pnl)
                if daily_pnl is not None else None)
    daily_pnl_pct = ((daily_pnl / prev_nav)
                     if (daily_pnl is not None and prev_nav) else None)
    cash_weight = (cash / nav) if (cash is not None and nav) else None

    sect: dict[str, float] = {}
    for r in rows:
        if r["current_weight"] is not None:
            sect[r["sector"]] = sect.get(r["sector"], 0.0) + r["current_weight"]
    sector_exposure = [{"sector": s, "weight": round(w, 6)}
                       for s, w in sorted(sect.items(), key=lambda kv: -kv[1])]

    def _mini(r: dict) -> dict:
        return {"ticker": r["ticker"], "current_weight": r["current_weight"],
                "market_value": r["market_value"],
                "unrealized_pnl": r["unrealized_pnl"],
                "unrealized_pnl_pct": r["unrealized_pnl_pct"]}

    ranked_w = [r for r in rows if r["current_weight"] is not None]
    ranked_p = [r for r in rows if r["unrealized_pnl_pct"] is not None]
    largest = [_mini(r) for r in ranked_w[:5]]
    best = [_mini(r) for r in sorted(ranked_p, key=lambda r: -r["unrealized_pnl_pct"])[:3]]
    worst = [_mini(r) for r in sorted(ranked_p, key=lambda r: r["unrealized_pnl_pct"])[:3]]
    drifts = [r for r in rows if r["weight_drift"] is not None]
    max_drift = max((abs(r["weight_drift"]) for r in drifts), default=None)
    off_target = sum(1 for r in drifts if abs(r["weight_drift"]) >= 0.01)

    return {
        "invested_value": desk._r2(invested),
        "cost_basis_total": desk._r2(cost_basis_total),
        "unrealized_pnl": (desk._r2(upnl_total) if upnl_total is not None else None),
        "unrealized_return": (round(uret, 6) if uret is not None else None),
        "daily_pnl": (desk._r2(daily_pnl) if daily_pnl is not None else None),
        "daily_pnl_pct": (round(daily_pnl_pct, 6) if daily_pnl_pct is not None else None),
        "daily_pnl_available": bool(daily_ok),
        "daily_pnl_basis_date": prev_date if daily_ok else None,
        "cash": (desk._r2(cash) if cash is not None else None),
        "cash_weight": (round(cash_weight, 6) if cash_weight is not None else None),
        "invested_weight": (round(1.0 - cash_weight, 6)
                            if cash_weight is not None else None),
        "holdings_count": len(rows),
        "sector_exposure": sector_exposure,
        "largest_positions": largest,
        "best_performers": best,
        "worst_performers": worst,
        "drift_summary": {
            "max_abs_drift": (round(max_drift, 6) if max_drift is not None else None),
            "names_off_target": off_target,
            "implemented_count": implementation_count,
            "target_count": target_count,
        },
    }


# --------------------------------------------------------------------------- #
# Phase 27B.1 - canonical workflow view (stages, header status, next action)
# --------------------------------------------------------------------------- #

#: Operational five-stage workflow (Workstream E). The stage statuses are derived
#: HERE (server-side, from the one canonical payload) so no page invents its own.
STAGE_REFRESH_DESK_MARKS = "REFRESH_DESK_MARKS"
STAGE_VERIFY_ALPHA_TARGET = "VERIFY_ALPHA_TARGET"
STAGE_GENERATE_ORDER_PLAN = "GENERATE_ORDER_PLAN"
STAGE_CONFIRM_PAPER_ORDERS = "CONFIRM_PAPER_ORDERS"
STAGE_MONITOR = "MONITOR"

ST_COMPLETE = "COMPLETE"
ST_NEEDS_ACTION = "NEEDS_ACTION"
ST_BLOCKED = "BLOCKED"
ST_PENDING = "PENDING"
ST_ACTIVE = "ACTIVE"

#: Operationally precise header vocabulary (Workstream G) - replaces the generic
#: "MARKET DATA: STALE / MISALIGNED" on operational pages.
HEADER_TARGET_REFRESH_REQUIRED = "TARGET_REFRESH_REQUIRED"
HEADER_DESK_MARK_REQUIRED = "DESK_MARK_REQUIRED"
HEADER_ORDER_PLAN_READY = "ORDER_PLAN_READY"
HEADER_ORDERS_PENDING = "ORDERS_PENDING"
HEADER_FORWARD_TRACKING_ACTIVE = "FORWARD_TRACKING_ACTIVE"
HEADER_DESK_MARK_READY = "DESK_MARK_READY"

_TRACKING_STATES = ("ORDERS_CONFIRMED", "WAITING_FOR_ELIGIBLE_CLOSE",
                    "PARTIALLY_FILLED", "FULLY_FILLED", "FORWARD_TRACKING_ACTIVE")

#: Phase 27B.2 — the ONE canonical next required operational action once the
#: deterministic executable order plan exists. Every operator page must agree.
NEXT_ACTION_REVIEW_AND_CONFIRM = "REVIEW_AND_CONFIRM_ORDER_PLAN"

#: Research champion terminology (Research & Audit) — kept separate from the
#: operational strategy/target names so no page can conflate the two.
RESEARCH_CHAMPION_NAME = "composite_sn"

#: Next-action codes that resolve to the order-plan review workspace.
_PLAN_REVIEW_CODES = ("REVIEW_AND_CONFIRM_ORDER_PLAN", "REVIEW_ORDER_PLAN",
                      "CONFIRM_ORDER_PLAN", "GENERATE_ORDER_PLAN",
                      "CONFIRM_PAPER_ORDERS")

#: ONE canonical navigation label per next-action code (final 27B.2 cutover).
#: Every operator CTA renders these verbatim — pages never invent labels.
NEXT_ACTION_LABELS = {
    "REVIEW_AND_CONFIRM_ORDER_PLAN": "Review Order Plan",
    "REVIEW_ORDER_PLAN": "Review Order Plan",
    "CONFIRM_ORDER_PLAN": "Review Order Plan",
    "GENERATE_ORDER_PLAN": "Review Order Plan",
    "CONFIRM_PAPER_ORDERS": "Review Order Plan",
    "REFRESH_DESK": "Refresh Desk Marks",
    "REFRESH_ALPHA_TARGET": "Refresh Alpha Target",
    "CONFIRM_TARGET_SNAPSHOT": "Confirm Target Snapshot",
    "INITIALIZE_ALPHA_BOOK": "Initialize Alpha Book",
    "MONITOR": "Monitor Fills & Holdings",
    "BLOCKED": "Resolve Ledger Block",
}

#: ONE final token-gated confirmation label (the only write CTA wording).
CONFIRM_ACTION_LABEL = "Confirm and Create Proposed Paper Orders"

# --------------------------------------------------------------------------- #
# Phase 27B.5 — canonical paper-order LIFECYCLE (one state-driven operator view)
# --------------------------------------------------------------------------- #

#: The six operator-facing lifecycle stages of the paper-order workflow. Every
#: operational surface (Command Center, Portfolio Manager, Daily Workflow,
#: Portfolio, right panel) renders THESE labels — no page invents its own.
LIFECYCLE_PLAN_NOT_CREATED = "PLAN_NOT_CREATED"
LIFECYCLE_PLAN_READY = "PLAN_READY"
LIFECYCLE_PROPOSED = "PROPOSED"
LIFECYCLE_SUBMITTED = "SUBMITTED"
LIFECYCLE_PARTIALLY_FILLED = "PARTIALLY_FILLED"
LIFECYCLE_FILLED = "FILLED"

LIFECYCLE_STAGES = (LIFECYCLE_PLAN_NOT_CREATED, LIFECYCLE_PLAN_READY,
                    LIFECYCLE_PROPOSED, LIFECYCLE_SUBMITTED,
                    LIFECYCLE_PARTIALLY_FILLED, LIFECYCLE_FILLED)

LIFECYCLE_LABELS = {
    LIFECYCLE_PLAN_NOT_CREATED: "Order Plan Not Created",
    LIFECYCLE_PLAN_READY: "Order Plan Ready For Review",
    LIFECYCLE_PROPOSED: "Paper Orders Awaiting Confirmation",
    LIFECYCLE_SUBMITTED: "Paper Orders Submitted — Awaiting Next Eligible Close",
    LIFECYCLE_PARTIALLY_FILLED: "Paper Execution In Progress",
    LIFECYCLE_FILLED: "Alpha Paper Book Active",
}

#: Lifecycle stages in which paper orders exist (the "tracking" half of the flow).
_LIFECYCLE_ORDER_STAGES = (LIFECYCLE_PROPOSED, LIFECYCLE_SUBMITTED,
                           LIFECYCLE_PARTIALLY_FILLED, LIFECYCLE_FILLED)


def derive_lifecycle_view(*, initialized: bool, orders: dict, fills_count: int,
                          plan_exists: bool, submitted_date: Optional[str] = None,
                          execution_model: Optional[str] = None) -> dict:
    """Pure derivation of the canonical operator lifecycle from the existing
    truth (desk order fold + ledger fill count + stateless plan existence).
    Never hard-codes counts or dates; performs no I/O and never writes."""
    by = orders.get("by_status") or {}
    proposed = by.get("PROPOSED", 0)
    submitted = by.get("APPROVED", 0) + by.get("SUBMITTED", 0)
    filled_orders = by.get("FILLED", 0)
    cancelled = by.get("CANCELLED", 0)
    expired = by.get("EXPIRED", 0)
    open_orders = proposed + submitted

    if proposed:
        stage = LIFECYCLE_PROPOSED
    elif submitted and not fills_count:
        stage = LIFECYCLE_SUBMITTED
    elif open_orders and fills_count:
        stage = LIFECYCLE_PARTIALLY_FILLED
    elif plan_exists:
        # Includes the monthly-rebalance re-emergence: every prior order is
        # terminal and a NEWER confirmed target produced a fresh plan cycle.
        stage = LIFECYCLE_PLAN_READY
    elif fills_count and not open_orders:
        stage = LIFECYCLE_FILLED
    else:
        stage = LIFECYCLE_PLAN_NOT_CREATED

    primary_action_label: Optional[str] = None
    secondary_action_label: Optional[str] = None
    current_task_label: Optional[str] = None
    next_eligible_fill_explanation: Optional[str] = None

    if stage == LIFECYCLE_PLAN_NOT_CREATED:
        headline = "ORDER PLAN NOT CREATED"
        explanation = ("No executable paper-order plan exists yet. Complete the "
                       "prerequisite step first — no paper orders have been "
                       "created." if initialized else
                       "Alpha Paper Book #1 is not initialized yet — no order "
                       "plan and no paper orders exist.")
    elif stage == LIFECYCLE_PLAN_READY:
        headline = "ORDER PLAN READY FOR REVIEW"
        explanation = ("The executable paper-order plan is ready. Review the "
                       "proposed orders and confirm them manually.")
        primary_action_label = "Review Order Plan"
        current_task_label = "Review Order Plan"
    elif stage == LIFECYCLE_PROPOSED:
        headline = "PAPER ORDERS AWAITING CONFIRMATION"
        explanation = ("%d proposed paper order%s await your manual confirmation. "
                       "Nothing fills until you confirm and submit them — paper "
                       "only, no broker."
                       % (proposed, "" if proposed == 1 else "s"))
        primary_action_label = ("Confirm and Submit %d Paper Order%s"
                                % (proposed, "" if proposed == 1 else "s"))
        secondary_action_label = "Cancel Proposed Paper Orders"
        current_task_label = "Confirm and Submit Paper Orders"
        next_eligible_fill_explanation = (
            "Nothing fills while the orders are PROPOSED. After you confirm and "
            "submit them, fills wait for the first eligible completed owned close "
            "recorded by a later manual desk refresh.")
    elif stage == LIFECYCLE_SUBMITTED:
        headline = ("%d PAPER ORDERS SUBMITTED — AWAITING NEXT ELIGIBLE CLOSE"
                    % submitted)
        explanation = ("The paper orders were created successfully. No further "
                       "confirmation is required. They will fill only after a "
                       "later eligible completed close is recorded by a manual "
                       "desk refresh.")
        primary_action_label = "Refresh After Market Close"
        secondary_action_label = "Cancel Submitted Orders"
        current_task_label = "Await Next Eligible Close"
        next_eligible_fill_explanation = (
            "Paper fills occur only at the first eligible completed owned close "
            "on or after %s (the submission date) that a LATER manual desk "
            "refresh records. A refresh that finds no newer completed close "
            "records 0 fills and the orders simply remain SUBMITTED — that is "
            "expected, not a failure." % (submitted_date or "the submission date"))
    elif stage == LIFECYCLE_PARTIALLY_FILLED:
        headline = "PAPER EXECUTION IN PROGRESS"
        explanation = ("%d paper order%s filled; %d still await%s an eligible "
                       "completed close. Run the manual desk refresh on a later "
                       "day to settle the rest — no further confirmation is "
                       "required."
                       % (filled_orders, "" if filled_orders == 1 else "s",
                          open_orders, "s" if open_orders == 1 else ""))
        primary_action_label = "Refresh After Market Close"
        secondary_action_label = "Cancel Submitted Orders"
        current_task_label = "Await Remaining Fills"
        next_eligible_fill_explanation = (
            "The remaining paper orders fill at the first eligible completed "
            "owned close on or after %s (the submission date) recorded by a "
            "later manual desk refresh." % (submitted_date or "their submission"))
    else:  # LIFECYCLE_FILLED
        headline = "ALPHA PAPER BOOK ACTIVE"
        explanation = ("All paper orders are settled and the alpha paper book is "
                       "live. Monitor holdings, NAV and forward performance; each "
                       "manual desk refresh appends new marks and performance "
                       "rows.")
        primary_action_label = "Monitor Holdings and Performance"
        current_task_label = "Monitor Holdings and Performance"

    return {
        "lifecycle_stage": stage,
        "lifecycle_stage_label": LIFECYCLE_LABELS[stage],
        "primary_headline": headline,
        "primary_explanation": explanation,
        "primary_action_label": primary_action_label,
        "secondary_action_label": secondary_action_label,
        "current_task_label": current_task_label,
        "proposed_count": proposed,
        "submitted_count": submitted,
        "filled_count": filled_orders,
        "cancelled_count": cancelled,
        "expired_count": expired,
        "open_order_count": open_orders,
        "submitted_date": submitted_date,
        "next_eligible_fill_explanation": next_eligible_fill_explanation,
        "no_further_confirmation_required": stage in (
            LIFECYCLE_SUBMITTED, LIFECYCLE_PARTIALLY_FILLED, LIFECYCLE_FILLED),
        "orders_exist": stage in _LIFECYCLE_ORDER_STAGES,
        "execution_model": execution_model,
    }


def _workflow_view(*, current_status: str, target: Optional[dict],
                   readiness: Optional[dict], initialized: bool,
                   orders: dict, fills_count: int,
                   review_due: bool = False) -> dict:
    """Derive the five-stage operational workflow + precise header status + the
    one next action from the canonical inputs. Pure; no I/O."""
    t_state = (target or {}).get("state")
    target_confirmed = t_state == "CONFIRMED"
    rd = readiness or {}
    marks_ready = (rd.get("desk_mark_status") == "DESK_MARK_READY"
                   and not rd.get("missing_ticker_count"))
    proposed = orders.get("awaiting_manual_confirmation") or 0
    awaiting_fill = orders.get("awaiting_fill") or 0
    tracking = current_status in _TRACKING_STATES
    # Phase 27B.9 — the operational book is ACTIVE when it holds executed positions
    # and no paper orders are open (fully implemented / forward tracking). An active
    # book whose scheduled review is not yet due is not flagged for target work.
    book_active = ((tracking or bool(fills_count))
                   and not (proposed or awaiting_fill))
    # The deterministic executable order plan EXISTS (buildable now) — generation
    # is complete and the workflow moves to review & confirm (Phase 27B.2).
    plan_exists = bool(initialized and target_confirmed and marks_ready
                       and current_status in ("ORDER_PLAN_READY",
                                              "ORDER_PLAN_REVIEW_REQUIRED"))

    # -- stage statuses ---------------------------------------------------- #
    s1 = (ST_COMPLETE if marks_ready else
          ST_NEEDS_ACTION if (initialized and target is not None) else ST_PENDING)
    # Stage 2 (Verify Alpha Target): a STALE / READY_TO_CONFIRM target is only
    # operator-actionable when the book is NOT yet active, or the scheduled review
    # is due. An active, fully-implemented book whose review is not due keeps the
    # current target and treats a fresher one as informational (Phase 27B.9).
    if target_confirmed:
        s2 = ST_COMPLETE
    elif t_state == "BLOCKED":
        s2 = ST_BLOCKED
    elif t_state in ("STALE_TARGET", "READY_TO_CONFIRM"):
        s2 = (ST_NEEDS_ACTION if (not book_active or review_due) else ST_COMPLETE)
    else:
        s2 = ST_PENDING
    # A COMPLETE stage-2 on an active/not-due book reads "Current — review not due"
    # rather than echoing the raw (still-fresh) readiness state.
    s2_current_not_due = (s2 == ST_COMPLETE and not target_confirmed and book_active)
    if proposed or awaiting_fill or fills_count or tracking or plan_exists:
        s3 = ST_COMPLETE
    elif initialized and target_confirmed:
        s3 = ST_NEEDS_ACTION if marks_ready else ST_BLOCKED
    else:
        s3 = ST_PENDING
    s4 = (ST_NEEDS_ACTION if proposed else
          ST_COMPLETE if (awaiting_fill or fills_count or tracking) else
          ST_NEEDS_ACTION if plan_exists else ST_PENDING)
    s5 = ST_ACTIVE if tracking else ST_PENDING
    stages = [
        {"stage": 1, "code": STAGE_REFRESH_DESK_MARKS, "label": "Refresh Desk Marks",
         "status": s1, "detail": (rd.get("desk_mark_date") and
                                  ("Desk mark %s" % rd.get("desk_mark_date"))) or
         "No desk mark recorded yet"},
        {"stage": 2, "code": STAGE_VERIFY_ALPHA_TARGET, "label": "Verify Alpha Target",
         "status": s2, "detail": ("Current — review not due" if s2_current_not_due else
         (t_state or "UNAVAILABLE") +
         ((" · " + str((target or {}).get("alpha_market_date")))
          if (target or {}).get("alpha_market_date") else ""))},
        {"stage": 3, "code": STAGE_GENERATE_ORDER_PLAN, "label": "Generate Order Plan",
         "status": s3, "detail": ("Order plan ready" if plan_exists else
                                  "Blocked until valid desk marks exist"
                                  if s3 == ST_BLOCKED else "")},
        {"stage": 4, "code": STAGE_CONFIRM_PAPER_ORDERS,
         "label": "Review & Confirm Paper Orders", "status": s4,
         "detail": (("%d proposed order(s) await manual confirmation" % proposed)
                    if proposed else
                    "Review and confirm the executable paper-order plan"
                    if (plan_exists and s4 == ST_NEEDS_ACTION) else "")},
        {"stage": 5, "code": STAGE_MONITOR,
         "label": "Monitor Fills, Holdings & Performance", "status": s5, "detail": ""},
    ]
    current_stage = next((s["code"] for s in stages
                          if s["status"] in (ST_NEEDS_ACTION, ST_BLOCKED)),
                         STAGE_MONITOR if tracking else STAGE_GENERATE_ORDER_PLAN)

    # -- precise header status (one value; never legacy-derived) ------------ #
    # Phase 27B.9 — an ACTIVE book whose review is not due is FORWARD TRACKING,
    # even if a fresher (unconfirmed) target exists. This guard MUST precede the
    # target-confirmation check so a monitoring book is never labelled
    # "TARGET REFRESH REQUIRED".
    if book_active and not review_due:
        header = {"code": HEADER_FORWARD_TRACKING_ACTIVE, "label": "FORWARD TRACKING ACTIVE"}
    elif target is None or not target_confirmed:
        header = {"code": HEADER_TARGET_REFRESH_REQUIRED, "label": "TARGET REFRESH REQUIRED"}
    elif not marks_ready:
        header = {"code": HEADER_DESK_MARK_REQUIRED, "label": "DESK MARK REQUIRED"}
    elif proposed or awaiting_fill:
        header = {"code": HEADER_ORDERS_PENDING, "label": "ORDERS PENDING"}
    elif current_status in ("FORWARD_TRACKING_ACTIVE", "FULLY_FILLED", "PARTIALLY_FILLED",
                            "WAITING_FOR_ELIGIBLE_CLOSE", "ORDERS_CONFIRMED"):
        header = {"code": HEADER_FORWARD_TRACKING_ACTIVE, "label": "FORWARD TRACKING ACTIVE"}
    elif current_status in ("BOOK_INITIALIZED", "ORDER_PLAN_READY",
                            "ORDER_PLAN_REVIEW_REQUIRED"):
        header = {"code": HEADER_ORDER_PLAN_READY, "label": "ORDER PLAN READY"}
    else:
        header = {"code": HEADER_DESK_MARK_READY,
                  "label": "DESK MARK READY — %s" % rd.get("desk_mark_date")}
    return {"stages": stages, "current_stage": current_stage, "header": header,
            "marks_ready": marks_ready, "plan_exists": plan_exists,
            "book_active": book_active, "review_due": bool(review_due)}


# --------------------------------------------------------------------------- #
# The operational book (single source of truth)
# --------------------------------------------------------------------------- #

def load_operational_book(*, desk_dir=None, ledger_dir=None, today: Optional[str] = None,
                          panel_path=None, inputs_dir=None) -> dict:
    """ONE payload describing Alpha Paper Book #1 for every operational page."""
    warnings: list[str] = []

    # 1. Alpha-book workflow status + ledger-replayed valuation (the ONE producer).
    status: dict = {}
    try:
        status = ab.load_alpha_status(desk_dir=desk_dir, ledger_dir=ledger_dir,
                                      today=today)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Alpha book status unavailable: {str(exc)[:160]}")

    book = status.get("book")
    valuation = status.get("book_valuation") or {}
    initialized = book is not None

    # 2. Pending paper orders for this book (same ledgers, read-only fold).
    orders = {"pending_count": 0, "awaiting_manual_confirmation": 0,
              "awaiting_fill": 0, "filled_count": 0, "total_orders": 0,
              "by_status": {}}
    try:
        orders = _pending_orders(desk_dir)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Desk order ledger unavailable: {str(exc)[:160]}")

    # 3. Current target readiness (six-date contract from Phase 27A.2).
    target: Optional[dict] = None
    try:
        r = at.load_readiness(panel_path=panel_path, inputs_dir=inputs_dir,
                              ledger_dir=ledger_dir)
        dates = r.get("dates") or {}
        target = {
            "state": r.get("state"),
            "alpha_market_date": dates.get("alpha_market_date"),
            "latest_completed_market_date": dates.get("latest_completed_market_date"),
            "alpha_market_aligned": r.get("alpha_market_aligned"),
            "snapshot_confirmation_allowed": r.get("snapshot_confirmation_allowed"),
            "confirmation_blockers": r.get("confirmation_blockers") or [],
            "required_next_action": r.get("required_next_action"),
        }
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Alpha target readiness unavailable: {str(exc)[:160]}")

    # 3b. Desk-mark / sizing readiness (Phase 27B.1) - can the target be sized?
    readiness: Optional[dict] = None
    try:
        readiness = ab.load_desk_mark_readiness(desk_dir=desk_dir, ledger_dir=ledger_dir)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Desk mark readiness unavailable: {str(exc)[:160]}")

    current_status = status.get("current_state") or "UNAVAILABLE"
    fills_count = status.get("n_alpha_fills") or 0
    policy = status.get("policy") or {}
    rd = readiness or {}

    # Implementation state (Workstream A): a confirmed target weight is NOT a
    # current executed position - implemented = target names actually HELD.
    holdings = valuation.get("holdings") or {}
    target_tickers = rd.get("target_tickers") or []
    target_count = (rd.get("confirmed_target_ticker_count")
                    or policy.get("target_position_count") or 0)
    implementation_count = sum(1 for tk in target_tickers if holdings.get(tk))
    implementation_percentage = (round(100.0 * implementation_count / target_count, 2)
                                 if target_count else None)

    # Phase 27B.9 — the canonical scheduled-review clock (monthly cadence). Anchored
    # to the CONFIRMED target's market month, not the fresher readiness date, so a
    # newer unconfirmed target does not move the next review earlier.
    confirmed_target_date = (rd.get("target_market_date")
                             or (target or {}).get("alpha_market_date"))
    next_review_date, review_due = _derive_review(
        target_date=confirmed_target_date,
        valuation_date=rd.get("desk_mark_date"), today=today)

    wf = _workflow_view(current_status=current_status, target=target,
                        readiness=readiness, initialized=initialized,
                        orders=orders, fills_count=fills_count,
                        review_due=review_due)

    # The ONE next action. When the book is initialized but the sizing marks are
    # missing/behind, the desk refresh outranks the plan states.
    next_action = status.get("next_required_action") or (
        "Backend unavailable — reconnect, then reload the operational book.")
    if (initialized and not wf["marks_ready"]
            and current_status in ("BOOK_INITIALIZED", "ORDER_PLAN_READY",
                                   "ORDER_PLAN_REVIEW_REQUIRED")
            and not (orders.get("pending_count") or 0)):
        next_action = ("REFRESH_DESK: run the manual desk data refresh (paper desk) - "
                       "the mark store must reach the latest completed owned market "
                       "date (%s) before the executable order plan."
                       % (rd.get("latest_completed_market_date") or "latest completed"))
    elif (wf["plan_exists"] and not (orders.get("pending_count") or 0)
            and not fills_count):
        # Phase 27B.2: the ONE canonical next action once the deterministic plan
        # exists — every operator page renders exactly this code.
        next_action = (NEXT_ACTION_REVIEW_AND_CONFIRM + ": review the executable "
                       "paper-order plan (read-only), then confirm it manually to "
                       "create the dedicated alpha paper orders (PROPOSED; paper "
                       "only, no broker, nothing fills yet).")
    next_action_code = str(next_action).split(":", 1)[0].strip() or None
    next_action_label = (str(next_action).split(":", 1)[1].strip()
                         if ":" in str(next_action) else str(next_action))

    # Phase 27B.2: an already-confirmed target whose only "blocker" is that a
    # re-confirmation would DUPLICATE the latest confirmed snapshot is NOT an
    # operational blocker — it is informational (nothing is actionable).
    blockers: list[str] = []
    informational: list[str] = []
    t_state = (target or {}).get("state")
    if target and target.get("confirmation_blockers"):
        for b in target["confirmation_blockers"]:
            sb = str(b)
            if (t_state == "CONFIRMED"
                    and sb.startswith("DUPLICATE_OF_LATEST_CONFIRMED_SNAPSHOT")):
                informational.append(
                    "TARGET_ALREADY_CONFIRMED: Target already confirmed — no further "
                    "confirmation required (re-confirming would duplicate the latest "
                    "confirmed snapshot).")
            else:
                blockers.append(sb)
    blockers.extend(str(b) for b in (rd.get("blockers") or []))
    # Phase 27B.9 — a newer/unconfirmed target on an ACTIVE book whose scheduled
    # review is not due is INFORMATIONAL, never a blocker and never urgent.
    if (wf.get("book_active") and not review_due
            and t_state in ("READY_TO_CONFIRM", "STALE_TARGET")):
        informational.append(
            "NEXT_CYCLE_TARGET_AVAILABLE: A newer model target is available, but the "
            "scheduled monthly review is not due until %s. The active paper holdings "
            "remain valid; no target action is required now."
            % (next_review_date or "the next review date"))
    integrity = status.get("ledger_integrity") or {}
    if integrity and not integrity.get("all_intact", True):
        blockers.append("LEDGER_INTEGRITY_BROKEN: an append-only desk ledger failed its "
                        "chain-hash verification.")

    operational_book = {
        "book_id": OPERATIONAL_BOOK_ID,
        "book_label": OPERATIONAL_BOOK_LABEL,
        "book_type": ROLE_OPERATIONAL,
        "classification": ROLE_OPERATIONAL,
        "initialized": initialized,
        "policy_version": status.get("policy_version"),
        "strategy_name": policy.get("strategy") or "fundamental_momentum_50_50_v1",
        "target_name": policy.get("target_book") or "fundamental_momentum_50_50_top25",
        "initial_capital": (_f(book.get("initial_capital")) if book else
                            _f(policy.get("starting_virtual_capital"))),
        "starting_capital": (_f(book.get("initial_capital")) if book else
                             _f(policy.get("starting_virtual_capital"))),
        "currency": (book or {}).get("currency") or desk.BOOK_CURRENCY,
        # The ONE producer of these numbers is desk.book_nav (ledger replay).
        "cash": _f(valuation.get("cash")),
        "invested": _f(valuation.get("invested")),
        "nav": _f(valuation.get("nav")),
        "nav_as_of_date": valuation.get("as_of_date"),
        "holdings_count": valuation.get("holdings_count") or 0,
        "holdings": holdings,
        "pending_orders": orders,
        "pending_order_count": orders.get("pending_count") or 0,
        "fills_count": fills_count,
        "fill_count": fills_count,
        "current_target": target,
        "target_count": target_count or None,
        "target_market_date": (rd.get("target_market_date")
                               or (target or {}).get("alpha_market_date")),
        "target_confirmation_status": (target or {}).get("state"),
        "desk_mark_date": rd.get("desk_mark_date"),
        "desk_mark_status": rd.get("desk_mark_status") or "UNAVAILABLE",
        "desk_mark_required_date": rd.get("latest_completed_market_date"),
        "desk_mark_priced_count": rd.get("priced_ticker_count"),
        "desk_mark_missing_tickers": rd.get("missing_tickers") or [],
        "implementation_count": implementation_count,
        "implementation_percentage": implementation_percentage,
        "implementation_note": ("A confirmed target weight is NOT a current executed "
                                "position. Implementation counts only names the alpha "
                                "book actually holds (ledger-replayed fills)."),
        "order_plan_ready": bool(rd.get("order_plan_ready")),
        "execution_model": (book or {}).get("execution_model"),
        "workflow_stage": wf["current_stage"],
        "workflow_stages": wf["stages"],
        "header_status": wf["header"],
        "current_status": current_status,
        "next_action": next_action,
        "next_action_code": next_action_code,
        "next_action_label": next_action_label,
        "blockers": blockers,
        "informational": informational,
        "ledger_integrity_ok": bool(integrity.get("all_intact", False)) if integrity else None,
        "latest_desk_mark_date": status.get("latest_desk_mark_date"),
        "initialization": status.get("initialization"),
        "not_initialized_note": (None if initialized else
                                 "Alpha Paper Book #1 is not initialized yet: cash, "
                                 "NAV and holdings are honestly empty until the first "
                                 "manual initialization (token-gated, user approval)."),
    }

    # ------------------------------------------------------------------ #
    # Phase 27B.8 — per-holding valuation for the operational holdings
    # dashboard. Read-only; degrades to [] so the canonical payload always
    # loads. Sourced ONLY from the ledger-replayed holdings + BUY-fill cost +
    # desk-mark prices + confirmed-plan sectors + frozen target weights.
    # ------------------------------------------------------------------ #
    holdings_detail: list = []
    portfolio_summary: Optional[dict] = None
    try:
        if book is not None and holdings:
            sdir = desk._desk_dir(desk_dir)
            fills = [f for f in desk._fills(sdir)
                     if f.get("book_id") == OPERATIONAL_BOOK_ID]
            marks = desk.read_marks(desk_dir)
            target_weights = book.get("frozen_target_weights") or {}
            holdings_detail, prev_date = build_holdings_detail(
                book=book, valuation=valuation, fills=fills, marks=marks,
                plan_orders=_plan_orders(desk_dir), target_weights=target_weights)
            portfolio_summary = _portfolio_summary(
                holdings_detail, cash=_f(valuation.get("cash")),
                nav=_f(valuation.get("nav")), prev_date=prev_date,
                target_count=target_count or None,
                implementation_count=implementation_count)
    except Exception as exc:  # noqa: BLE001 - never break the canonical payload
        warnings.append(f"Holdings detail unavailable: {str(exc)[:160]}")
        holdings_detail, portfolio_summary = [], None
    operational_book["holdings_detail"] = holdings_detail
    operational_book["portfolio_summary"] = portfolio_summary

    # ------------------------------------------------------------------ #
    # Phase 27B.2 — the CANONICAL OPERATIONAL STATE: one flat, explicitly
    # namespaced contract every operator surface renders verbatim.
    # ------------------------------------------------------------------ #
    if current_status in _TRACKING_STATES:
        order_plan_status = "ORDERS_CONFIRMED"
    elif orders.get("awaiting_manual_confirmation"):
        order_plan_status = "ORDERS_PROPOSED"
    elif wf["plan_exists"]:
        order_plan_status = "ORDER_PLAN_READY"
    elif initialized and not wf["marks_ready"]:
        order_plan_status = "BLOCKED_DESK_MARKS_REQUIRED"
    else:
        order_plan_status = "NOT_GENERATED"

    legacy_summary: dict = {"available": False, "book_id": LEGACY_BOOK_ID,
                            "label": LEGACY_BOOK_LABEL, "positions_count": None,
                            "tickers": [],
                            "line": "Legacy paper book archive (valuation unavailable)"}
    try:
        entry = _legacy_archive_entry()
        n_legacy = entry.get("positions_count")
        legacy_summary = {
            "available": bool(entry.get("available")),
            "book_id": LEGACY_BOOK_ID,
            "label": LEGACY_BOOK_LABEL,
            "positions_count": n_legacy,
            "tickers": entry.get("tickers") or [],
            "as_of_market_date": entry.get("as_of_market_date"),
            "line": (("Legacy paper book archive: %d historical position%s"
                      % (n_legacy, "" if n_legacy == 1 else "s"))
                     if n_legacy is not None else
                     "Legacy paper book archive (valuation unavailable)"),
        }
    except Exception:  # noqa: BLE001 - the canonical state must always load
        pass

    # Presentation resolution (final 27B.2): ONE label, ONE route, ONE enabled
    # flag per next-action code — derived HERE so no page invents its own CTA.
    plan_summary = status.get("plan_summary") or {}
    next_action_label_canonical = NEXT_ACTION_LABELS.get(
        next_action_code or "",
        (next_action_code or "OPEN_PORTFOLIO_MANAGER").replace("_", " ").title())

    # Phase 27B.5 — the canonical paper-order lifecycle. Once paper orders exist
    # the lifecycle owns the operator wording (headline, CTA label, explanation);
    # before that the 27B.2 next-action label map stays authoritative.
    lifecycle = derive_lifecycle_view(
        initialized=initialized, orders=orders, fills_count=fills_count,
        plan_exists=bool(wf["plan_exists"]),
        submitted_date=orders.get("latest_submission_date"),
        execution_model=(book or {}).get("execution_model"))
    if lifecycle["primary_action_label"]:
        next_action_label_canonical = lifecycle["primary_action_label"]
    next_action_route = ("#portfolio-manager/pd-band"
                         if lifecycle["orders_exist"]
                         else "#portfolio-manager/ab-band"
                         if next_action_code in _PLAN_REVIEW_CODES
                         else "#portfolio-manager")

    # Phase 27B.9 — target-freshness classification + the monitoring next-action
    # line. On an ACTIVE book, a fresher unconfirmed target that is not yet due
    # reads "CURRENT TARGET ACTIVE / NEXT-CYCLE TARGET AVAILABLE — REVIEW NOT DUE",
    # never "NEEDS ACTION". The classification is the ONE source every surface uses.
    _book_active = bool(wf.get("book_active"))
    _tstate = (target or {}).get("state")
    if _book_active and review_due:
        target_freshness = {
            "code": "MODEL_REVIEW_DUE", "label": "MODEL REVIEW DUE",
            "line": ("The scheduled model review is due (%s). Review the alpha target."
                     % (next_review_date or "now"))}
    elif _book_active and _tstate in ("READY_TO_CONFIRM", "STALE_TARGET"):
        target_freshness = {
            "code": "NEXT_CYCLE_TARGET_AVAILABLE_REVIEW_NOT_DUE",
            "label": "NEXT-CYCLE TARGET AVAILABLE — REVIEW NOT DUE",
            "line": ("A newer model target is available; the next scheduled review is "
                     "%s. The current active paper holdings remain valid."
                     % (next_review_date or "pending"))}
    elif _book_active:
        target_freshness = {
            "code": "CURRENT_TARGET_ACTIVE", "label": "CURRENT TARGET ACTIVE",
            "line": ("Current target active. Next scheduled review: %s."
                     % (next_review_date or "pending"))}
    else:
        target_freshness = {
            "code": "TARGET_%s" % (_tstate or "UNAVAILABLE"),
            "label": str(_tstate or "UNAVAILABLE").replace("_", " "),
            "line": (lifecycle["primary_explanation"] or "")}
    monitor_next_action_line = (
        ("Monitor holdings, NAV, drift and forward performance."
         + (" Next model review: %s." % next_review_date if next_review_date else ""))
        if lifecycle["lifecycle_stage"] == LIFECYCLE_FILLED else None)

    canonical_state = {
        "operational_book_id": OPERATIONAL_BOOK_ID,
        "operational_book_name": OPERATIONAL_BOOK_LABEL,
        "operational_book_status": current_status,
        # -- presentation contract (exact operator wording, one source) ----- #
        "workflow_state": current_status,
        "workflow_state_label": str(current_status).replace("_", " "),
        "next_action_code": next_action_code,
        "next_action_label": next_action_label_canonical,
        "next_action_description": next_action_label,
        "next_action_route_or_anchor": next_action_route,
        "next_action_enabled": bool(next_action_code and initialized),
        "confirm_action_label": CONFIRM_ACTION_LABEL,
        # -- Phase 27B.5: canonical paper-order lifecycle (one operator view) -- #
        "lifecycle_stage": lifecycle["lifecycle_stage"],
        "lifecycle_stage_label": lifecycle["lifecycle_stage_label"],
        "primary_headline": lifecycle["primary_headline"],
        "primary_explanation": lifecycle["primary_explanation"],
        "next_action_explanation": lifecycle["primary_explanation"],
        "current_task_label": (lifecycle["current_task_label"]
                               or next_action_label_canonical),
        "primary_action_enabled": bool(next_action_code and initialized),
        "primary_action_disabled_reason": (
            None if (next_action_code and initialized) else
            "Alpha Paper Book #1 is not initialized yet." if not initialized else
            "Backend unavailable — reconnect, then reload the operational book."),
        "secondary_action_label": lifecycle["secondary_action_label"],
        "proposed_count": lifecycle["proposed_count"],
        "submitted_count": lifecycle["submitted_count"],
        "filled_count": lifecycle["filled_count"],
        "cancelled_count": lifecycle["cancelled_count"],
        "expired_count": lifecycle["expired_count"],
        "open_order_count": lifecycle["open_order_count"],
        "submitted_date": lifecycle["submitted_date"],
        "next_eligible_fill_explanation": lifecycle["next_eligible_fill_explanation"],
        "no_further_confirmation_required": lifecycle["no_further_confirmation_required"],
        "execution_model": lifecycle["execution_model"],
        "implementation_count": implementation_count,
        "target_status": (target or {}).get("state"),
        "target_date": operational_book["target_market_date"],
        "target_count": operational_book["target_count"],
        # -- Phase 27B.9: the canonical scheduled-review clock (one source) ---- #
        "next_review_date": next_review_date,
        "review_due": bool(review_due),
        "review_cadence": REVIEW_CADENCE,
        "active_target_date": operational_book["target_market_date"],
        "desk_valuation_date": operational_book["desk_mark_date"],
        "target_freshness": target_freshness,
        "monitor_next_action_line": monitor_next_action_line,
        "plan_status": order_plan_status,
        "planned_position_count": plan_summary.get("executable_count"),
        "planned_blocked_count": plan_summary.get("blocked_count"),
        "implemented_position_count": implementation_count,
        "pending_order_count": operational_book["pending_order_count"],
        "fill_count": fills_count,
        "holdings_count": operational_book["holdings_count"],
        "nav": operational_book["nav"],
        "cash": operational_book["cash"],
        # -- Phase 27B.8: holdings dashboard KPIs (one canonical source) ------ #
        "invested_value": (portfolio_summary or {}).get("invested_value"),
        "cost_basis": (portfolio_summary or {}).get("cost_basis_total"),
        "unrealized_pnl": (portfolio_summary or {}).get("unrealized_pnl"),
        "unrealized_pnl_pct": (portfolio_summary or {}).get("unrealized_return"),
        "daily_pnl": (portfolio_summary or {}).get("daily_pnl"),
        "daily_pnl_pct": (portfolio_summary or {}).get("daily_pnl_pct"),
        "daily_pnl_available": bool((portfolio_summary or {}).get("daily_pnl_available")),
        "cash_weight": (portfolio_summary or {}).get("cash_weight"),
        "invested_weight": (portfolio_summary or {}).get("invested_weight"),
        "valuation_date": operational_book["desk_mark_date"],
        "holdings_detail": holdings_detail,
        "portfolio_summary": portfolio_summary,
        "informational_notices": list(informational),
        "research_summary": {
            "research_champion": RESEARCH_CHAMPION_NAME,
            "operational_strategy": operational_book["strategy_name"],
            "operational_target": operational_book["target_name"],
            "note": ("RESEARCH ONLY — research evidence informs the target but "
                     "never drives the operational next action."),
        },
        "operational_nav": operational_book["nav"],
        "operational_cash": operational_book["cash"],
        "operational_holdings_count": operational_book["holdings_count"],
        "operational_pending_order_count": operational_book["pending_order_count"],
        "operational_fill_count": fills_count,
        "confirmed_target_name": operational_book["target_name"],
        "confirmed_target_date": operational_book["target_market_date"],
        "confirmed_target_count": operational_book["target_count"],
        "target_confirmation_status": (target or {}).get("state"),
        "implemented_target_count": implementation_count,
        "implementation_percentage": implementation_percentage,
        "desk_mark_status": operational_book["desk_mark_status"],
        "desk_mark_date": operational_book["desk_mark_date"],
        "order_plan_status": order_plan_status,
        "next_required_action": next_action_code,
        "next_required_action_label": next_action_label,
        "header_status": wf["header"],
        "blockers": list(blockers),
        "informational": list(informational),
        "safety_mode": {
            "manual_review": True,
            "paper_orders_only": True,
            "broker_execution": False,
            "automation": False,
            "line": ("Manual review · Paper orders only · No broker execution · "
                     "Automation off"),
        },
        "legacy_archive_summary": legacy_summary,
    }
    operational_book["canonical_state"] = canonical_state

    return {
        "status": STATUS_OK,
        "phase": PHASE,
        "generated_at": _now_iso(),
        "operational_book": operational_book,
        "canonical_state": canonical_state,
        "single_source_of_truth": {
            "endpoint": "/v1/operational-book",
            "value_producer": "paper_trading_desk.book_nav (append-only ledger replay)",
            "note": SINGLE_SOURCE_NOTE,
        },
        "other_books": {
            "legacy_paper_portfolio": {
                "book_id": LEGACY_BOOK_ID,
                "label": LEGACY_BOOK_LABEL,
                "classification": ROLE_HISTORICAL,
                "read_only": True,
                "archived": True,
                "route": "portfolio/archive",
            },
            "research_books_note": RESEARCH_BOOKS_NOTE,
        },
        "warnings": warnings,
        **_safety(),
    }


# --------------------------------------------------------------------------- #
# Historical Paper Books (archive)
# --------------------------------------------------------------------------- #

def _legacy_archive_entry() -> dict:
    """Read-only snapshot of the ARCHIVED legacy paper portfolio. Degrades."""
    entry = {
        "book_id": LEGACY_BOOK_ID,
        "label": LEGACY_BOOK_LABEL,
        "classification": ROLE_HISTORICAL,
        "read_only": True,
        "archived": True,
        "available": False,
        "positions_count": None,
        "tickers": [],
        "cash": None,
        "total_value": None,
        "as_of_market_date": None,
        "note": ("The legacy signal-workflow paper portfolio (existing positions, "
                 "fills, snapshots and P&L). Preserved read-only in the archive — "
                 "never shown as the default portfolio, never modified by the "
                 "operational Alpha Paper Book #1 workflow."),
    }
    try:
        v = _VALUATION_LOADER()
        mark = v.get("current_mark") or {}
        positions = [p.get("ticker") for p in (v.get("positions") or [])
                     if p.get("ticker")]
        entry.update({
            "available": True,
            "positions_count": len(positions),
            "tickers": positions,
            "cash": _f(mark.get("current_cash")),
            "total_value": _f(mark.get("current_total_value")),
            "as_of_market_date": mark.get("as_of_market_date"),
        })
    except Exception as exc:  # noqa: BLE001
        entry["note"] += f" (valuation currently unavailable: {str(exc)[:120]})"
    return entry


def _completed_desk_books(desk_dir=None) -> list[dict]:
    """Any non-operational or closed desk paper books (future completed books)."""
    out: list[dict] = []
    try:
        sdir = desk._desk_dir(desk_dir)
        for b in desk._books(sdir):
            closed = b.get("status") not in (None, "OPEN")
            if b.get("book_id") == OPERATIONAL_BOOK_ID and not closed:
                continue  # the ACTIVE operational book is not an archive entry
            out.append({
                "book_id": b.get("book_id"),
                "label": b.get("label") or b.get("book_id"),
                "classification": ROLE_HISTORICAL,
                "read_only": True,
                "archived": True,
                "status": b.get("status"),
                "initial_capital": _f(b.get("initial_capital")),
                "note": "Completed / non-operational desk paper book (archived).",
            })
    except Exception:  # noqa: BLE001
        pass
    return out


def load_historical_books(*, desk_dir=None) -> dict:
    """Historical Paper Books: the legacy archive + past/completed paper books."""
    historical = [_legacy_archive_entry()] + _completed_desk_books(desk_dir)
    return {
        "status": ARCHIVE_STATUS_OK,
        "phase": PHASE,
        "generated_at": _now_iso(),
        "operational_book_id": OPERATIONAL_BOOK_ID,
        "operational_book_label": OPERATIONAL_BOOK_LABEL,
        "historical_books": historical,
        "research_books": [dict(b) for b in RESEARCH_BOOKS],
        "research_books_note": RESEARCH_BOOKS_NOTE,
        "note": ("Historical Paper Books are read-only. The single operational "
                 "portfolio is Alpha Paper Book #1 (see /v1/operational-book)."),
        **_safety(),
    }


__all__ = [
    "PHASE", "OPERATIONAL_BOOK_ID", "OPERATIONAL_BOOK_LABEL",
    "ROLE_OPERATIONAL", "ROLE_HISTORICAL", "ROLE_RESEARCH",
    "STATUS_OK", "ARCHIVE_STATUS_OK", "LEGACY_BOOK_ID", "LEGACY_BOOK_LABEL",
    "SINGLE_SOURCE_NOTE", "RESEARCH_BOOKS",
    "STAGE_REFRESH_DESK_MARKS", "STAGE_VERIFY_ALPHA_TARGET",
    "STAGE_GENERATE_ORDER_PLAN", "STAGE_CONFIRM_PAPER_ORDERS", "STAGE_MONITOR",
    "HEADER_TARGET_REFRESH_REQUIRED", "HEADER_DESK_MARK_REQUIRED",
    "HEADER_ORDER_PLAN_READY", "HEADER_ORDERS_PENDING",
    "HEADER_FORWARD_TRACKING_ACTIVE", "HEADER_DESK_MARK_READY",
    "NEXT_ACTION_REVIEW_AND_CONFIRM", "NEXT_ACTION_LABELS",
    "CONFIRM_ACTION_LABEL", "RESEARCH_CHAMPION_NAME",
    "LIFECYCLE_PLAN_NOT_CREATED", "LIFECYCLE_PLAN_READY", "LIFECYCLE_PROPOSED",
    "LIFECYCLE_SUBMITTED", "LIFECYCLE_PARTIALLY_FILLED", "LIFECYCLE_FILLED",
    "LIFECYCLE_STAGES", "LIFECYCLE_LABELS", "derive_lifecycle_view",
    "load_operational_book", "load_historical_books",
]
