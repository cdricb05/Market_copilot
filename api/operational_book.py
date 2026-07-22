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


def _pending_orders(desk_dir=None) -> dict:
    """Fold the desk order ledger for the operational book (read-only replay)."""
    sdir = desk._desk_dir(desk_dir)
    orders = [o for o in desk._orders_state(sdir).values()
              if o.get("book_id") == OPERATIONAL_BOOK_ID]
    by_status: dict[str, int] = {}
    for o in orders:
        by_status[o["status"]] = by_status.get(o["status"], 0) + 1
    pending = [o for o in orders if o.get("status") in _PENDING_ORDER_STATUSES]
    return {
        "pending_count": len(pending),
        "awaiting_manual_confirmation": by_status.get(desk.ST_PROPOSED, 0),
        "awaiting_fill": (by_status.get(desk.ST_APPROVED, 0)
                          + by_status.get(desk.ST_SUBMITTED, 0)),
        "filled_count": by_status.get(desk.ST_FILLED, 0),
        "total_orders": len(orders),
        "by_status": by_status,
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


def _workflow_view(*, current_status: str, target: Optional[dict],
                   readiness: Optional[dict], initialized: bool,
                   orders: dict, fills_count: int) -> dict:
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
    # The deterministic executable order plan EXISTS (buildable now) — generation
    # is complete and the workflow moves to review & confirm (Phase 27B.2).
    plan_exists = bool(initialized and target_confirmed and marks_ready
                       and current_status in ("ORDER_PLAN_READY",
                                              "ORDER_PLAN_REVIEW_REQUIRED"))

    # -- stage statuses ---------------------------------------------------- #
    s1 = (ST_COMPLETE if marks_ready else
          ST_NEEDS_ACTION if (initialized and target is not None) else ST_PENDING)
    s2 = (ST_COMPLETE if target_confirmed else
          ST_BLOCKED if t_state == "BLOCKED" else
          ST_NEEDS_ACTION if t_state in ("STALE_TARGET", "READY_TO_CONFIRM") else ST_PENDING)
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
         "status": s2, "detail": (t_state or "UNAVAILABLE") +
         ((" · " + str((target or {}).get("alpha_market_date")))
          if (target or {}).get("alpha_market_date") else "")},
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
    if target is None or not target_confirmed:
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
            "marks_ready": marks_ready, "plan_exists": plan_exists}


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

    wf = _workflow_view(current_status=current_status, target=target,
                        readiness=readiness, initialized=initialized,
                        orders=orders, fills_count=fills_count)

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
    next_action_route = ("#portfolio-manager/ab-band"
                         if next_action_code in _PLAN_REVIEW_CODES
                         else "#portfolio-manager")

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
        "target_status": (target or {}).get("state"),
        "target_date": operational_book["target_market_date"],
        "target_count": operational_book["target_count"],
        "plan_status": order_plan_status,
        "planned_position_count": plan_summary.get("executable_count"),
        "planned_blocked_count": plan_summary.get("blocked_count"),
        "implemented_position_count": implementation_count,
        "pending_order_count": operational_book["pending_order_count"],
        "fill_count": fills_count,
        "holdings_count": operational_book["holdings_count"],
        "nav": operational_book["nav"],
        "cash": operational_book["cash"],
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
    "load_operational_book", "load_historical_books",
]
