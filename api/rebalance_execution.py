r"""Stage 19 — Controlled paper-rebalance execution (APPROVED proposal -> paper orders).

This is the bridge that closes the active portfolio-management loop:

    RESEARCH CYCLE -> HOC -> REALLOCATION PROPOSAL -> MANUAL DECISION (Stage 18)
        -> CONTROLLED ORDER PLAN (this module)
        -> SECOND MANUAL CONFIRMATION (this module)
        -> PAPER ORDERS (existing desk lifecycle, api.paper_trading_desk)
        -> EXISTING NEXT_CLOSE EXECUTION (desk.settle_due_orders)
        -> RECONCILIATION -> UPDATED ACTIVE PAPER BOOK.

Structural gap it fills
-----------------------
``api.alpha_book`` can only plan orders against the confirmed MHZ *alpha-target* snapshot;
it cannot consume the Stage-18 *Reallocation Proposal*. This module lets the EXISTING
order-planning + execution primitives operate on an APPROVED, immutable reallocation
target — WITHOUT a second fill simulator, a second order ledger, or a second NAV owner.
Orders are written into the desk's own ``ORDERS_FILE`` and settled by the desk's own
``confirm_orders``/``settle_due_orders`` (NEXT_CLOSE, no-hindsight). The old alpha-target
workflow is untouched.

Two independent manual gates protect execution
----------------------------------------------
  1. Stage-18 portfolio approval (``api.portfolio_decision``: ``APPROVE_FOR_PAPER_REBALANCE``
     + ``CONFIRM_PORTFOLIO_REBALANCE_DECISION``) — is this reallocation the right call?
  2. Stage-19 order-plan confirmation (this module: :data:`CONFIRM_TOKEN`) — are THESE
     concrete whole-share orders, reconciled against the CURRENT desk, correct?
Only the second gate creates paper orders. ``REJECT`` / ``HOLD`` and every stale-evidence
condition create nothing. Backend-enforced: a UI button is never the security boundary.

Everything here is paper-only. No broker. No live order. No automatic approval. No
automatic rebalance. No model change.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from paper_trader.api import corporate_actions as ca
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import reallocation_proposal as realloc

PHASE = "STAGE19"
OWNER = "api.rebalance_execution"

# --- The SECOND explicit confirmation (distinct from Stage-18's decision token) ---- #
CONFIRM_TOKEN = "CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN"

# --- Rebalance-lifecycle vocabulary (Workstream H) --------------------------------- #
RB_NO_ACTIVE_BOOK = "REBALANCE_NO_ACTIVE_BOOK"
RB_NO_PROPOSAL = "REBALANCE_NO_PROPOSAL"
RB_PROPOSAL_REVIEW_REQUIRED = "PROPOSAL_REVIEW_REQUIRED"          # Stage-18 not yet approved
RB_STALE = "STALE_PROPOSAL_REVIEW_REQUIRED"
RB_PLAN_REVIEW_REQUIRED = "PROPOSAL_APPROVED_ORDER_PLAN_REVIEW_REQUIRED"
RB_PLAN_CONFIRMED = "ORDER_PLAN_CONFIRMED_PAPER_EXECUTION_PENDING"
RB_EXECUTED = "PAPER_EXECUTED_RECONCILED"
RB_NO_CHANGES = "REBALANCE_NO_ORDERS_REQUIRED"
RB_UNAVAILABLE = "REBALANCE_UNAVAILABLE"
STATE_VOCAB = (RB_NO_ACTIVE_BOOK, RB_NO_PROPOSAL, RB_PROPOSAL_REVIEW_REQUIRED, RB_STALE,
               RB_PLAN_REVIEW_REQUIRED, RB_PLAN_CONFIRMED, RB_EXECUTED, RB_NO_CHANGES,
               RB_UNAVAILABLE)

# --- Confirm-status codes returned by confirm_rebalance_order_plan ----------------- #
C_CONFIRM_REQUIRED = "ORDER_PLAN_CONFIRMATION_REQUIRED"
C_NOT_APPROVED = "PORTFOLIO_APPROVAL_REQUIRED"
C_STALE = "STALE_PLAN_REVIEW_REQUIRED"
C_NO_CHANGES = "NO_ORDERS_REQUIRED"
C_CREATED = "PAPER_ORDERS_CREATED"
C_REUSED = "REUSED_EXISTING_NO_DUPLICATE"

# --- Plan-evidence root (its OWN root, NEVER the desk ledger root) ------------------ #
PLAN_DIR_ENV = "PAPER_TRADER_REBALANCE_PLAN_DIR"
_DEFAULT_PLAN_DIR = Path(r"D:\Stock_Prediction_app_data\rebalance_order_plans")
_PLANS_FILE = "order_plans.json"

# --- Reconciliation policy (reuse the desk / engine constants; no new numbers) ----- #
COST_RATE_PER_SIDE = desk.COST_RATE_PER_SIDE
COST_BPS_PER_SIDE = desk.COST_BPS_PER_SIDE
MAX_INDIVIDUAL_WEIGHT = 0.10          # mirrors engine.reallocation_proposal.default_policy
SECTOR_CAP_FRACTION = 0.25
MIN_ORDER_SHARES = 1                  # whole-share, minimum one share
EXECUTION_MODEL = desk.EXECUTION_MODEL_DEFAULT


# --------------------------------------------------------------------------- #
# io helpers
# --------------------------------------------------------------------------- #
def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _plan_dir(plan_dir=None) -> Path:
    if plan_dir is not None:
        return Path(plan_dir)
    env = os.environ.get(PLAN_DIR_ENV)
    return Path(env) if env else _DEFAULT_PLAN_DIR


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(blob)
        os.replace(tmp, str(path))
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _stable_hash(obj: Any) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _r2(x):
    return None if x is None else round(float(x), 2)


def _r6(x):
    return None if x is None else round(float(x), 6)


def _safety() -> dict:
    return {
        "paper_only": True, "manual_review": True, "automation_off": True,
        "broker_enabled": False, "live_orders_enabled": False,
        "automatic_approval_allowed": False, "automatic_rebalance_allowed": False,
        "second_confirmation_required": True, "execution_model": EXECUTION_MODEL,
        "promoted_model": False, "recalibrated_model": False, "changed_cadence": False,
        "safety_badges": ["PAPER ONLY", "MANUAL REVIEW", "NO BROKER", "NO LIVE ORDERS",
                          "SECOND CONFIRMATION REQUIRED", "AUTOMATION OFF"],
    }


# --------------------------------------------------------------------------- #
# Default loaders (injectable seams for hermetic tests)
# --------------------------------------------------------------------------- #
def _default_portfolio_state_loader() -> dict:
    from paper_trader.api import portfolio_state as ps
    return ps.load_portfolio_state()


def _resolve_book_and_date(active_book_id, eligible_market_date, portfolio_state,
                           portfolio_state_loader, desk_dir):
    """Resolve (active_book_id, eligible_market_date). Explicit args win; else the operating
    portfolio-state; else the desk's open book id + the latest proposal date for it."""
    if active_book_id and eligible_market_date:
        return active_book_id, eligible_market_date
    ps = portfolio_state
    if ps is None:
        try:
            ps = (portfolio_state_loader or _default_portfolio_state_loader)()
        except Exception:  # noqa: BLE001 - degrade to desk-derived resolution
            ps = None
    if ps:
        active_book_id = active_book_id or (ps.get("active_book") or {}).get("book_id")
        eligible_market_date = eligible_market_date or (
            ps.get("dates") or {}).get("eligible_market_date")
    if not active_book_id:
        b = desk.open_book(desk._desk_dir(desk_dir))
        active_book_id = (b or {}).get("book_id")
    return active_book_id, eligible_market_date


# --------------------------------------------------------------------------- #
# Deterministic order-plan construction (READ-ONLY; Workstream C)
# --------------------------------------------------------------------------- #
def _current_desk_view(desk_dir, actions_dir, corporate_actions):
    """Current book, corrected holdings, cash, marks + a desk state hash. The holdings are
    corporate-action corrected (Stage-19 split repair) so an EXIT sells the true share
    count and NAV is economically correct."""
    sdir = desk._desk_dir(desk_dir)
    book = desk.open_book(sdir)
    marks = desk.read_marks(desk_dir)
    marks_date = desk.marks_latest_date(marks)
    if corporate_actions is None:
        corporate_actions = ca.load_actions(actions_dir=actions_dir)
    fills = desk._fills(sdir)
    if book is None:
        return {"book": None, "holdings": {}, "cash": None, "nav": None,
                "marks": marks, "marks_date": marks_date, "series": (marks.get("series") or {}),
                "desk_state_hash": None, "corporate_actions": corporate_actions}
    nav_blk = desk.book_nav(book, fills, marks, corporate_actions=corporate_actions)
    holdings = dict(nav_blk["holdings"])
    state_key = {"book_id": book["book_id"], "cash": nav_blk["cash"],
                 "holdings": {k: holdings[k] for k in sorted(holdings)},
                 "marks_date": marks_date}
    return {"book": book, "holdings": holdings, "cash": nav_blk["cash"], "nav": nav_blk["nav"],
            "marks": marks, "marks_date": marks_date, "series": (marks.get("series") or {}),
            "desk_state_hash": _stable_hash(state_key), "corporate_actions": corporate_actions}


def _price(series: dict, tk: str, as_of: str) -> Optional[float]:
    hit = desk._series_price_at_or_before(series.get(tk) or [], as_of) if as_of else None
    return hit[1] if hit else None


def _base_plan(*, decision_dir, reallocation_dir, desk_dir, actions_dir,
               active_book_id, eligible_market_date, portfolio_state,
               portfolio_state_loader, artifact, decision_record, corporate_actions) -> dict:
    """Shared plan derivation used by both the read contract and the confirm gate. Returns
    a dict with ``state`` (one of :data:`STATE_VOCAB`) and, when the proposal is APPROVED
    and unchanged, the full deterministic order plan (never writes)."""
    active_book_id, eligible_market_date = _resolve_book_and_date(
        active_book_id, eligible_market_date, portfolio_state, portfolio_state_loader, desk_dir)
    if not active_book_id:
        return {"state": RB_NO_ACTIVE_BOOK, "message": "No active operational book."}

    if decision_record is None:
        decision_record = pdec.load_decision_record(
            active_book_id=active_book_id, eligible_market_date=eligible_market_date,
            decision_dir=decision_dir)
    if artifact is None:
        artifact = realloc.load_latest_artifact(
            active_book_id=active_book_id, eligible_market_date=eligible_market_date,
            reallocation_dir=reallocation_dir)
    if not artifact:
        return {"state": RB_NO_PROPOSAL, "active_book_id": active_book_id,
                "eligible_market_date": eligible_market_date,
                "message": "No reallocation proposal exists for the active book / session."}

    ident = artifact.get("identity") or {}
    prop = artifact.get("proposal") or {}
    current_hash = ident.get("proposal_hash") or prop.get("proposal_hash")
    bound = {"proposal_id": artifact.get("proposal_id"), "proposal_hash": current_hash,
             "eligible_market_date": ident.get("eligible_market_date") or eligible_market_date,
             "active_book_id": ident.get("active_book_id") or active_book_id,
             "portfolio_state_hash": ident.get("portfolio_state_hash"),
             "hoc_assessment_hash": ident.get("hoc_assessment_hash"),
             "universe_scoring_hash": ident.get("universe_scoring_hash"),
             "allocation_policy_version": ident.get("allocation_policy_version")}

    # Gate 1: the Stage-18 portfolio decision must be an APPROVE bound to THIS proposal.
    if not decision_record or decision_record.get("decision") != pdec.DECISION_APPROVE:
        return {"state": RB_PROPOSAL_REVIEW_REQUIRED, "bound": bound,
                "decision": (decision_record or {}).get("decision"),
                "message": ("The reallocation proposal has not been APPROVED for paper "
                            "rebalance. Approve it (Stage-18) before an order plan exists.")}
    # Gate 2: the approved decision must bind the CURRENT proposal (no stale evidence).
    if decision_record.get("proposal_hash") != current_hash:
        return {"state": RB_STALE, "bound": bound,
                "approved_proposal_hash": decision_record.get("proposal_hash"),
                "current_proposal_hash": current_hash,
                "message": ("The proposal changed since it was approved; a fresh review + "
                            "approval is required before an order plan can be built.")}

    # Current desk state (corporate-action corrected holdings / cash / NAV).
    view = _current_desk_view(desk_dir, actions_dir, corporate_actions)
    if view["book"] is None or view["marks_date"] is None or view["nav"] is None:
        return {"state": RB_UNAVAILABLE, "bound": bound,
                "message": "The desk is not ready (no open book or no owned marks)."}

    plan = _reconcile_order_plan(artifact=artifact, bound=bound, view=view)
    return {"state": (RB_NO_CHANGES if not plan["orders"] else RB_PLAN_REVIEW_REQUIRED),
            "bound": bound, "plan": plan, "desk_view": view,
            "decision_record": decision_record}


def _reconcile_order_plan(*, artifact: dict, bound: dict, view: dict) -> dict:
    """The deterministic, read-only reconciliation of the APPROVED proposed weights against
    the CURRENT desk (shares / prices / cash), producing whole-share SELL/BUY orders,
    estimated proceeds/purchases/costs, residual cash, before/after weights and the target
    tracking error. Pure given (artifact, desk view)."""
    prop = artifact.get("proposal") or {}
    allocs = prop.get("allocations") or []
    series = view["series"]
    as_of = view["marks_date"]
    nav = float(view["nav"])
    cash = float(view["cash"])
    held = dict(view["holdings"])

    # --- target shares per name from the APPROVED proposed weight x current corrected NAV.
    rows = []
    sector_of = {}
    proposed_w = {}
    for a in allocs:
        tk = a.get("ticker")
        if not tk:
            continue
        sector_of[tk] = a.get("sector") or "Unknown"
        pw = float(a.get("proposed_weight") or 0.0)
        proposed_w[tk] = pw
        px = _price(series, tk, as_of)
        cur_sh = int(held.get(tk, 0))
        if px is None or px <= 0:
            rows.append({"ticker": tk, "action": a.get("action"), "sector": sector_of[tk],
                         "current_shares": cur_sh, "target_shares": None, "order_shares": 0,
                         "side": None, "price": None, "proposed_weight": _r6(pw),
                         "blocked": True, "block_reason": "NO_OWNED_MARK",
                         "current_market_value": None, "target_market_value": None})
            continue
        # position cap: never target more than MAX_INDIVIDUAL_WEIGHT of NAV
        capped_w = min(pw, MAX_INDIVIDUAL_WEIGHT)
        target_dollars = capped_w * nav
        target_sh = int(math.floor(target_dollars / px)) if target_dollars > 0 else 0
        rows.append({"ticker": tk, "action": a.get("action"), "sector": sector_of[tk],
                     "current_shares": cur_sh, "target_shares": target_sh,
                     "order_shares": target_sh - cur_sh, "price": _r6(px),
                     "proposed_weight": _r6(pw), "capped_weight": _r6(capped_w),
                     "blocked": False, "block_reason": None,
                     "current_market_value": _r2(cur_sh * px),
                     "target_market_value": _r2(target_sh * px)})
    # Names currently held but ABSENT from the proposal allocations -> full EXIT (sell all).
    alloc_tickers = {r["ticker"] for r in rows}
    for tk, cur_sh in held.items():
        if tk in alloc_tickers or int(cur_sh) == 0:
            continue
        px = _price(series, tk, as_of)
        sector_of[tk] = sector_of.get(tk, "Unknown")
        proposed_w[tk] = 0.0
        rows.append({"ticker": tk, "action": "EXIT", "sector": sector_of[tk],
                     "current_shares": int(cur_sh), "target_shares": 0,
                     "order_shares": -int(cur_sh), "price": _r6(px),
                     "proposed_weight": 0.0, "capped_weight": 0.0,
                     "blocked": px is None, "block_reason": ("NO_OWNED_MARK" if px is None else None),
                     "current_market_value": _r2(int(cur_sh) * px) if px is not None else None,
                     "target_market_value": 0.0 if px is not None else None})

    # --- capital reconciliation: buys may not exceed cash + estimated sell proceeds.
    def _sell_proceeds(rs):
        tot = 0.0
        for r in rs_active(rs):
            if r["order_shares"] < 0:
                gross = -r["order_shares"] * float(r["price"])
                tot += gross - gross * COST_RATE_PER_SIDE
        return tot

    def _buy_outflow(rs):
        tot = 0.0
        for r in rs_active(rs):
            if r["order_shares"] > 0:
                gross = r["order_shares"] * float(r["price"])
                tot += gross + gross * COST_RATE_PER_SIDE
        return tot

    def rs_active(rs):
        return [r for r in rs if not r["blocked"] and r["order_shares"] != 0 and r["price"]]

    available = cash + _sell_proceeds(rows)
    trimmed = []
    # deterministically trim the largest remaining BUY by one share until feasible
    guard = 0
    while _buy_outflow(rows) > available + 1e-6 and guard < 10_000_000:
        guard += 1
        buys = [r for r in rs_active(rows) if r["order_shares"] > 0]
        if not buys:
            break
        buys.sort(key=lambda r: (-(r["order_shares"] * float(r["price"])), r["ticker"]))
        top = buys[0]
        top["order_shares"] -= 1
        top["target_shares"] = top["current_shares"] + top["order_shares"]
        top["target_market_value"] = _r2(top["target_shares"] * float(top["price"]))
        trimmed.append(top["ticker"])

    # --- classify + build order list (whole-share, min-order enforced).
    orders, blocked = [], []
    gross_sells = gross_buys = est_cost = 0.0
    for r in rows:
        if r["blocked"]:
            blocked.append({"ticker": r["ticker"], "reason": r["block_reason"]})
            continue
        n = r["order_shares"]
        if n == 0 or abs(n) < MIN_ORDER_SHARES:
            continue
        px = float(r["price"])
        gross = abs(n) * px
        cost = gross * COST_RATE_PER_SIDE
        est_cost += cost
        if n < 0:
            side = desk.SIDE_SELL
            gross_sells += gross
            kind = "EXIT" if r["target_shares"] == 0 else "REDUCE"
        else:
            side = desk.SIDE_BUY
            gross_buys += gross
            kind = "ADD" if r["current_shares"] == 0 else "INCREASE"
        orders.append({
            "ticker": r["ticker"], "side": side, "order_kind": kind,
            "quantity": int(abs(n)), "price_used_for_sizing": _r6(px),
            "price_date": as_of, "sector": r["sector"],
            "current_shares": r["current_shares"], "target_shares": r["target_shares"],
            "gross_notional": _r2(gross), "estimated_transaction_cost": round(cost, 4),
            "current_weight": _r6((r["current_shares"] * px) / nav) if nav else None,
            "proposed_weight": r["proposed_weight"],
        })
    orders.sort(key=lambda o: (0 if o["side"] == desk.SIDE_SELL else 1, o["ticker"]))

    residual_cash = cash + (gross_sells - gross_sells * COST_RATE_PER_SIDE) \
        - (gross_buys + gross_buys * COST_RATE_PER_SIDE)

    # --- before/after weights + target tracking error (one-sided active weight).
    before_w, after_w = {}, {}
    achieved_sh = dict(held)
    for r in rows:
        if not r["blocked"]:
            achieved_sh[r["ticker"]] = r["target_shares"]
    for tk in sorted(set(list(held) + list(proposed_w))):
        px = _price(series, tk, as_of)
        if px is None:
            continue
        before_w[tk] = round((int(held.get(tk, 0)) * px) / nav, 6) if nav else None
        after_w[tk] = round((int(achieved_sh.get(tk, 0)) * px) / nav, 6) if nav else None
    te = 0.0
    for tk in set(list(proposed_w) + list(after_w)):
        te += abs(float(after_w.get(tk, 0.0) or 0.0) - float(proposed_w.get(tk, 0.0)))
    tracking_error = round(0.5 * te, 6)

    plan_core = {
        "orders": orders, "blocked": blocked,
        "n_sell": sum(1 for o in orders if o["side"] == desk.SIDE_SELL),
        "n_buy": sum(1 for o in orders if o["side"] == desk.SIDE_BUY),
        "estimated_sell_proceeds": _r2(gross_sells - gross_sells * COST_RATE_PER_SIDE),
        "estimated_buy_cost": _r2(gross_buys + gross_buys * COST_RATE_PER_SIDE),
        "estimated_transaction_cost": round(est_cost, 2),
        "gross_sells": _r2(gross_sells), "gross_buys": _r2(gross_buys),
        "residual_cash": _r2(residual_cash),
        "one_way_turnover": _r6((gross_sells + gross_buys) / 2.0 / nav) if nav else None,
        "target_tracking_error": tracking_error,
        "trimmed_for_capital": sorted(set(trimmed)),
        "before_weights": before_w, "after_weights": after_w,
        "sizing_nav_basis": _r2(nav), "sizing_cash": _r2(cash), "marks_date": as_of,
        "policy": {"max_individual_weight": MAX_INDIVIDUAL_WEIGHT,
                   "sector_cap_fraction": SECTOR_CAP_FRACTION,
                   "min_order_shares": MIN_ORDER_SHARES,
                   "cost_bps_per_side": COST_BPS_PER_SIDE, "whole_shares_only": True,
                   "execution_model": EXECUTION_MODEL, "long_only": True},
    }
    # order_plan_hash binds the entire plan + the bound proposal identity + desk state.
    plan_identity = {"bound": bound, "desk_state_hash": view["desk_state_hash"],
                     "marks_date": as_of, "orders": orders, "policy": plan_core["policy"]}
    order_plan_hash = _stable_hash(plan_identity)
    plan_core["order_plan_hash"] = order_plan_hash
    plan_core["desk_state_hash"] = view["desk_state_hash"]
    plan_core["order_plan_id"] = "rbop_%s_%s_%s" % (
        bound.get("eligible_market_date") or "nodate",
        bound.get("active_book_id") or "book", order_plan_hash[:12])
    return plan_core


# --------------------------------------------------------------------------- #
# Read contract (Workstream H) — one primary action per state; writes nothing
# --------------------------------------------------------------------------- #
_PRIMARY_ACTION = {
    RB_NO_ACTIVE_BOOK: None,
    RB_NO_PROPOSAL: {"label": "Run the Daily Research Cycle",
                     "path": "POST /v1/operations/daily-research-cycle/run"},
    RB_PROPOSAL_REVIEW_REQUIRED: {"label": "Review & approve the reallocation proposal",
                                  "path": "POST /v1/operations/portfolio-decision/record"},
    RB_STALE: {"label": "Re-review the changed proposal",
               "path": "POST /v1/operations/portfolio-decision/record"},
    RB_PLAN_REVIEW_REQUIRED: {"label": "Confirm the paper rebalance order plan",
                              "path": "POST /v1/operations/rebalance/confirm-order-plan"},
    RB_PLAN_CONFIRMED: {"label": "Awaiting the next owned close (paper execution pending)",
                        "path": None},
    RB_EXECUTED: {"label": "Paper rebalance executed & reconciled", "path": None},
    RB_NO_CHANGES: None,
    RB_UNAVAILABLE: None,
}


def _executed_orders_for_plan(sdir, order_plan_id: str) -> list[dict]:
    """All desk orders whose lineage carries this exact ``order_plan_id`` (idempotency)."""
    out = []
    for o in desk._orders_state(sdir).values():
        lin = o.get("rebalance_lineage") or {}
        if lin.get("order_plan_id") == order_plan_id:
            out.append(o)
    return out


def _rebalance_orders_for_proposal(sdir, proposal_hash: str) -> list[dict]:
    """All desk rebalance orders bound to this proposal hash (execution-status detection).
    Proposal-level (not plan-id) so that once fills settle and the desk state moves, the
    executed orders are still recognised."""
    if not proposal_hash:
        return []
    out = []
    for o in desk._orders_state(sdir).values():
        lin = o.get("rebalance_lineage") or {}
        if lin.get("proposal_hash") == proposal_hash:
            out.append(o)
    return out


def load_rebalance_state(*, decision_dir=None, reallocation_dir=None, desk_dir=None,
                         actions_dir=None, plan_dir=None, active_book_id=None,
                         eligible_market_date=None, portfolio_state=None,
                         portfolio_state_loader=None, artifact=None, decision_record=None,
                         corporate_actions=None) -> dict:
    """READ-ONLY rebalance-lifecycle contract. Composes the state, the review-screen order
    plan (when approved) and the single primary action. Degrade-safe; never writes."""
    generated_at = _iso_now()
    try:
        base = _base_plan(decision_dir=decision_dir, reallocation_dir=reallocation_dir,
                          desk_dir=desk_dir, actions_dir=actions_dir,
                          active_book_id=active_book_id, eligible_market_date=eligible_market_date,
                          portfolio_state=portfolio_state,
                          portfolio_state_loader=portfolio_state_loader,
                          artifact=artifact, decision_record=decision_record,
                          corporate_actions=corporate_actions)
    except Exception as exc:  # noqa: BLE001 - a pure read must never crash the caller
        return {"phase": PHASE, "owner": OWNER, "status": "OK",
                "generated_at": generated_at, "rebalance_state": RB_UNAVAILABLE,
                "message": "Rebalance state unavailable: %s" % str(exc)[:160], **_safety()}

    state = base["state"]
    plan = base.get("plan")
    bound = base.get("bound") or {}
    sdir = desk._desk_dir(desk_dir)
    # Execution status is judged at the PROPOSAL level: once the confirmed orders settle,
    # the desk state moves and the re-derived plan id changes, but the orders remain bound
    # to the same proposal hash.
    executed = [o for o in _rebalance_orders_for_proposal(sdir, bound.get("proposal_hash"))
                if o["status"] not in (desk.ST_CANCELLED, desk.ST_EXPIRED)]
    if executed:
        all_filled = all(o["status"] == desk.ST_FILLED for o in executed)
        state = RB_EXECUTED if all_filled else RB_PLAN_CONFIRMED

    label = {RB_NO_ACTIVE_BOOK: "No active book", RB_NO_PROPOSAL: "No proposal yet",
             RB_PROPOSAL_REVIEW_REQUIRED: "Proposal awaiting manual review",
             RB_STALE: "Proposal changed since approval",
             RB_PLAN_REVIEW_REQUIRED: "Order plan review required",
             RB_PLAN_CONFIRMED: "Paper execution pending (NEXT_CLOSE)",
             RB_EXECUTED: "Paper executed & reconciled",
             RB_NO_CHANGES: "Holdings already match the approved target",
             RB_UNAVAILABLE: "Rebalance state unavailable"}.get(state, state)

    out = {
        "phase": PHASE, "owner": OWNER, "status": "OK", "generated_at": generated_at,
        "rebalance_state": state, "state_vocabulary": list(STATE_VOCAB), "label": label,
        "bound": base.get("bound"), "active_book_id": base.get("active_book_id"),
        "message": base.get("message"),
        "primary_action": _PRIMARY_ACTION.get(state),
        "confirm_required_token": CONFIRM_TOKEN,
        "order_plan": plan,
        "executed_order_ids": [o["order_id"] for o in executed],
        "executed_order_status": {o["order_id"]: o["status"] for o in executed},
        **_safety(),
    }
    return out


# --------------------------------------------------------------------------- #
# Second confirmation + paper-order write (Workstreams D + E) — the ONLY writer
# --------------------------------------------------------------------------- #
def confirm_rebalance_order_plan(*, confirm: Optional[str] = None,
                                 expected_order_plan_hash: Optional[str] = None,
                                 decision_dir=None, reallocation_dir=None, desk_dir=None,
                                 actions_dir=None, plan_dir=None, active_book_id=None,
                                 eligible_market_date=None, portfolio_state=None,
                                 portfolio_state_loader=None, artifact=None,
                                 decision_record=None, corporate_actions=None,
                                 actor: Optional[str] = None, today: Optional[str] = None) -> dict:
    """SECOND explicit confirmation. Only ``confirm == CONFIRM_TOKEN`` on an APPROVED,
    unchanged proposal writes PAPER orders (into the EXISTING desk lifecycle, submitted for
    NEXT_CLOSE). REJECT/HOLD, an un-approved or stale proposal, and a wrong token all write
    NOTHING. Idempotent: confirming the exact same order plan twice creates ZERO duplicate
    paper orders."""
    base_safety = {"owner": OWNER, "phase": PHASE, "performed_write": False,
                   "created_orders": False, "created_fills": False, "changed_holdings": False,
                   "changed_cash": False, "changed_nav": False, **_safety()}

    if confirm != CONFIRM_TOKEN:
        return {**base_safety, "status": C_CONFIRM_REQUIRED,
                "message": "Creating paper rebalance orders requires confirm='%s'." % CONFIRM_TOKEN,
                "confirm_required_token": CONFIRM_TOKEN}

    base = _base_plan(decision_dir=decision_dir, reallocation_dir=reallocation_dir,
                      desk_dir=desk_dir, actions_dir=actions_dir,
                      active_book_id=active_book_id, eligible_market_date=eligible_market_date,
                      portfolio_state=portfolio_state,
                      portfolio_state_loader=portfolio_state_loader,
                      artifact=artifact, decision_record=decision_record,
                      corporate_actions=corporate_actions)
    state = base["state"]
    if state in (RB_PROPOSAL_REVIEW_REQUIRED, RB_NO_ACTIVE_BOOK, RB_NO_PROPOSAL):
        return {**base_safety, "status": C_NOT_APPROVED, "rebalance_state": state,
                "message": base.get("message"), "bound": base.get("bound")}
    if state == RB_STALE:
        return {**base_safety, "status": C_STALE, "rebalance_state": state,
                "message": base.get("message"), "bound": base.get("bound"),
                "approved_proposal_hash": base.get("approved_proposal_hash"),
                "current_proposal_hash": base.get("current_proposal_hash")}
    if state == RB_UNAVAILABLE:
        return {**base_safety, "status": RB_UNAVAILABLE, "message": base.get("message")}
    plan = base.get("plan") or {}
    if not plan.get("orders"):
        return {**base_safety, "status": C_NO_CHANGES, "rebalance_state": RB_NO_CHANGES,
                "message": "The approved target already matches the current desk; no order "
                           "is required.", "order_plan_id": plan.get("order_plan_id")}

    # Stale-plan guard: if the operator reviewed a different plan (desk moved), re-review.
    if expected_order_plan_hash is not None and expected_order_plan_hash != plan["order_plan_hash"]:
        return {**base_safety, "status": C_STALE, "rebalance_state": RB_STALE,
                "message": ("The desk state changed since the order plan was reviewed; a "
                            "fresh order-plan review is required."),
                "expected_order_plan_hash": expected_order_plan_hash,
                "current_order_plan_hash": plan["order_plan_hash"]}

    sdir = desk._desk_dir(desk_dir)
    # Idempotency: the exact plan already executed -> reuse, ZERO duplicate orders.
    already = _executed_orders_for_plan(sdir, plan["order_plan_id"])
    if already:
        return {**base_safety, "status": C_REUSED, "reused": True,
                "rebalance_state": RB_PLAN_CONFIRMED,
                "order_plan_id": plan["order_plan_id"],
                "order_plan_hash": plan["order_plan_hash"],
                "existing_order_ids": [o["order_id"] for o in already],
                "message": "This exact order plan was already confirmed; no duplicate orders."}

    book = base["desk_view"]["book"]
    bound = base["bound"]
    resolved_decision = base.get("decision_record") or decision_record or {}
    approval_date = desk._today(today)
    marks_latest = base["desk_view"]["marks_date"]
    created_at = _iso_now()
    lineage = {
        "decision_id": resolved_decision.get("record_id"),
        "proposal_id": bound.get("proposal_id"), "proposal_hash": bound.get("proposal_hash"),
        "order_plan_id": plan["order_plan_id"], "order_plan_hash": plan["order_plan_hash"],
        "eligible_market_date": bound.get("eligible_market_date"),
        "paper_book_id": book["book_id"], "created_at": created_at,
        "execution_model": EXECUTION_MODEL,
    }
    # Persist the plan as an append-only correction/plan artifact (never the desk ledger).
    _persist_plan(plan_dir, plan, bound, lineage, approval_date)

    order_events, submit_events, journal_rows = [], [], []
    seq = len(desk._orders_state(sdir)) + 1
    for o in plan["orders"]:
        order_id = "ord_%s_%03d_%s" % (book["book_id"], seq, o["ticker"])
        seq += 1
        order = {
            "order_id": order_id, "book_id": book["book_id"], "ticker": o["ticker"],
            "side": o["side"], "quantity": int(o["quantity"]),
            "order_kind": o["order_kind"], "target_weight": o.get("proposed_weight"),
            "reference_close": o["price_used_for_sizing"],
            "reference_close_date": o["price_date"], "sector": o.get("sector", "Unknown"),
            "execution_model": EXECUTION_MODEL,
            "reason": "Stage-19 controlled paper rebalance (%s) from approved proposal %s."
                      % (o["order_kind"], bound.get("proposal_id")),
            "rebalance_lineage": lineage, "created_at": created_at,
        }
        order_events.append({"event": "ORDER_CREATED", "order": order})
        submit_events.append({"event": "ORDER_TRANSITION", "order_id": order_id,
                              "from_status": desk.ST_PROPOSED, "to_status": desk.ST_APPROVED,
                              "detail": "Second-confirmation approved (Stage 19)."})
        submit_events.append({"event": "ORDER_TRANSITION", "order_id": order_id,
                              "from_status": desk.ST_APPROVED, "to_status": desk.ST_SUBMITTED,
                              "approval_date": approval_date,
                              "marks_latest_at_approval": marks_latest,
                              "detail": ("Submitted for NEXT_CLOSE paper execution: fills at "
                                         "the first completed owned close strictly after %s."
                                         % marks_latest)})
        journal_rows.append(desk._journal_core(
            book, "ORDER_SUBMITTED", o["ticker"],
            "Stage-19 rebalance paper order %s submitted (%s %d %s). Awaiting the first "
            "completed owned close after %s." % (order_id, o["side"], int(o["quantity"]),
                                                 o["ticker"], marks_latest)))

    desk._append_ledger(sdir, desk.ORDERS_FILE, order_events)
    desk._append_ledger(sdir, desk.ORDERS_FILE, submit_events)
    desk._append_ledger(sdir, desk.JOURNAL_FILE, journal_rows)
    desk._append_ledger(sdir, desk.TIMELINE_FILE, [desk._timeline_core(
        book, "REBALANCE_ORDERS_SUBMITTED",
        "Stage-19 paper rebalance confirmed on %s: %d order(s) SUBMITTED for NEXT_CLOSE "
        "(order plan %s). No same-close hindsight fill." % (
            approval_date, len(order_events), plan["order_plan_id"]))])
    # Settle now (no-hindsight: nothing fills at a close already known at approval).
    settle = desk.settle_due_orders(desk_dir=desk_dir, today=today)
    order_ids = [e["order"]["order_id"] for e in order_events]
    return {**base_safety, "status": C_CREATED, "performed_write": True, "created_orders": True,
            "wrote_to_desk_ledgers_only": True, "rebalance_state": RB_PLAN_CONFIRMED,
            "order_plan_id": plan["order_plan_id"], "order_plan_hash": plan["order_plan_hash"],
            "n_orders_created": len(order_ids), "orders_created": order_ids,
            "approval_date": approval_date, "marks_latest_at_approval": marks_latest,
            "no_hindsight_note": ("Orders are SUBMITTED; they can only fill at the first "
                                  "completed owned close strictly after %s — never the "
                                  "already-known close." % marks_latest),
            "settlement": settle, "lineage": lineage,
            "message": ("%d paper rebalance order(s) SUBMITTED for NEXT_CLOSE. Fills occur at "
                        "the next completed owned close via the existing desk settlement."
                        % len(order_ids))}


def _persist_plan(plan_dir, plan: dict, bound: dict, lineage: dict, plan_date: str) -> None:
    path = _plan_dir(plan_dir) / _PLANS_FILE
    rows = _load_json(path)
    if not isinstance(rows, list):
        rows = []
    if any(r.get("order_plan_id") == plan["order_plan_id"] for r in rows):
        return
    rows.append({"order_plan_id": plan["order_plan_id"],
                 "order_plan_hash": plan["order_plan_hash"], "bound": bound,
                 "lineage": lineage, "plan_date": plan_date, "confirmed_at": _iso_now(),
                 "orders": plan["orders"], "reconciliation": {
                     k: plan[k] for k in ("estimated_sell_proceeds", "estimated_buy_cost",
                                          "estimated_transaction_cost", "residual_cash",
                                          "target_tracking_error", "one_way_turnover")},
                 "immutable": True})
    _atomic_write_json(path, rows)


__all__ = [
    "PHASE", "OWNER", "CONFIRM_TOKEN", "STATE_VOCAB", "PLAN_DIR_ENV",
    "RB_NO_ACTIVE_BOOK", "RB_NO_PROPOSAL", "RB_PROPOSAL_REVIEW_REQUIRED", "RB_STALE",
    "RB_PLAN_REVIEW_REQUIRED", "RB_PLAN_CONFIRMED", "RB_EXECUTED", "RB_NO_CHANGES",
    "RB_UNAVAILABLE", "C_CONFIRM_REQUIRED", "C_NOT_APPROVED", "C_STALE", "C_NO_CHANGES",
    "C_CREATED", "C_REUSED", "load_rebalance_state", "confirm_rebalance_order_plan",
]
