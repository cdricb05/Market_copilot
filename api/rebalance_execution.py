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
from paper_trader.api import portfolio_decision_outcome as pdo
from paper_trader.api import reallocation_proposal as realloc

PHASE = "STAGE19"
OWNER = "api.rebalance_execution"

# --- The SECOND explicit confirmation (distinct from Stage-18's decision token) ---- #
CONFIRM_TOKEN = "CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN"

#: Stage 19.2 — the explicit manual token that hydrates owned marks for the APPROVED target
#: universe. Distinct from the two execution gates: it creates no order and no fill, it only
#: delegates to the canonical desk mark owner. A GET never triggers it.
HYDRATE_CONFIRM_TOKEN = "CONFIRM_REBALANCE_TARGET_MARK_REFRESH"

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
#: Stage 19.2 FAIL-CLOSED states. An APPROVED proposal whose executable plan cannot
#: faithfully implement the target lands HERE, never in RB_PLAN_REVIEW_REQUIRED.
RB_BLOCKED_MARKS = "ORDER_PLAN_BLOCKED_MISSING_OWNED_MARKS"
RB_BLOCKED_INCOMPLETE = "ORDER_PLAN_BLOCKED_INCOMPLETE_TARGET"
STATE_VOCAB = (RB_NO_ACTIVE_BOOK, RB_NO_PROPOSAL, RB_PROPOSAL_REVIEW_REQUIRED, RB_STALE,
               RB_PLAN_REVIEW_REQUIRED, RB_PLAN_CONFIRMED, RB_EXECUTED, RB_NO_CHANGES,
               RB_UNAVAILABLE, RB_BLOCKED_MARKS, RB_BLOCKED_INCOMPLETE)
#: The states in which NO order plan may ever be confirmed.
NON_CONFIRMABLE_STATES = (RB_NO_ACTIVE_BOOK, RB_NO_PROPOSAL, RB_PROPOSAL_REVIEW_REQUIRED,
                          RB_STALE, RB_UNAVAILABLE, RB_BLOCKED_MARKS, RB_BLOCKED_INCOMPLETE)

# --- Confirm-status codes returned by confirm_rebalance_order_plan ----------------- #
C_CONFIRM_REQUIRED = "ORDER_PLAN_CONFIRMATION_REQUIRED"
C_NOT_APPROVED = "PORTFOLIO_APPROVAL_REQUIRED"
C_STALE = "STALE_PLAN_REVIEW_REQUIRED"
C_NO_CHANGES = "NO_ORDERS_REQUIRED"
C_CREATED = "PAPER_ORDERS_CREATED"
C_REUSED = "REUSED_EXISTING_NO_DUPLICATE"
#: Stage 19.2 — the confirmation refused because the plan is not faithfully executable.
C_BLOCKED = "ORDER_PLAN_BLOCKED"
#: Stage 19.2 hydration statuses.
H_CONFIRM_REQUIRED = "TARGET_MARK_REFRESH_CONFIRMATION_REQUIRED"
H_NOT_APPROVED = "PORTFOLIO_APPROVAL_REQUIRED"
H_DONE = "TARGET_MARKS_REFRESHED"
H_INCOMPLETE = "TARGET_MARKS_STILL_INCOMPLETE"

# --- Block-reason vocabulary (structured, never free text) -------------------------- #
BR_NO_OWNED_MARK = "NO_OWNED_MARK"
BR_TARGET_OMITTED = "TARGET_ACTION_OMITTED"
BR_TRACKING_ERROR = "TARGET_TRACKING_ERROR_EXCEEDS_ENVELOPE"
BR_TURNOVER_GAP = "TURNOVER_GAP_EXCEEDS_ENVELOPE"
BR_RECONCILIATION = "TARGET_RECONCILIATION_FAILED"
BLOCK_REASON_VOCAB = (BR_NO_OWNED_MARK, BR_TARGET_OMITTED, BR_TRACKING_ERROR,
                      BR_TURNOVER_GAP, BR_RECONCILIATION)

#: Omission reasons that the deterministic execution envelope EXPLICITLY supports. An
#: omission for any other reason is a defect and fails the plan closed.
OMIT_WHOLE_SHARE = "WHOLE_SHARE_ROUNDING"
OMIT_MIN_ORDER = "MIN_ORDER_SIZE"
OMIT_CAPITAL = "AVAILABLE_CAPITAL_TRIM"
OMIT_ALREADY_AT_TARGET = "ALREADY_AT_TARGET"
SUPPORTED_OMISSIONS = (OMIT_WHOLE_SHARE, OMIT_MIN_ORDER, OMIT_CAPITAL, OMIT_ALREADY_AT_TARGET)

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


#: Proposal actions that REQUIRE a trade. RETAIN does not.
_TRADING_ACTIONS = ("ADD", "INCREASE", "REDUCE", "EXIT", "REPLACE_IN", "REPLACE_OUT")


def _implies_trade(row: dict) -> bool:
    return str(row.get("action") or "").upper() in _TRADING_ACTIONS


def _proposal_one_way_turnover(artifact: Optional[dict]) -> Optional[float]:
    """The APPROVED proposal's own one-way turnover — read from the immutable artifact the
    proposal engine produced. If an older artifact carries no turnover block it is derived
    from the artifact's own weights (0.5 * sum |proposed - current|), which is the same
    definition; this module never re-derives allocation math, only reads the target."""
    prop = (artifact or {}).get("proposal") or {}
    blk = prop.get("turnover") or {}
    if blk.get("one_way_turnover") is not None:
        try:
            return round(float(blk["one_way_turnover"]), 6)
        except (TypeError, ValueError):
            pass
    allocs = prop.get("allocations") or []
    if not allocs:
        return None
    total = 0.0
    for a in allocs:
        try:
            total += abs(float(a.get("proposed_weight") or 0.0)
                         - float(a.get("current_weight") or 0.0))
        except (TypeError, ValueError):
            return None
    return round(0.5 * total, 6)


# --------------------------------------------------------------------------- #
# Stage 19.2 — the EXECUTION MARK UNIVERSE of an approved proposal
#
# The August-12 incident: eight ADD names of an APPROVED proposal had no owned mark, the
# plan silently omitted all eight, and the system still reported the plan buildable. The
# root cause is that the required mark universe was never named anywhere: the desk mark
# refresh derives its ticker set from the confirmed alpha snapshot + currently held names +
# open orders, none of which contains a not-yet-held reallocation target. So the target
# universe is stated ONCE here and used by BOTH the fail-closed gate and the hydration.
# --------------------------------------------------------------------------- #
def target_mark_universe(*, artifact: Optional[dict], holdings: Optional[dict] = None) -> dict:
    """Every ticker that must carry an owned execution mark before the APPROVED proposal can
    be turned into a faithful order plan: every proposed POSITIVE-weight constituent (BUY /
    ADD / INCREASE and any retained name that still needs sizing), every currently held name
    (SELL / REDUCE / EXIT sizing), and the desk's benchmark (existing desk accounting).
    Pure and read-only."""
    prop = (artifact or {}).get("proposal") or {}
    target, allocation = [], []
    for a in prop.get("allocations") or []:
        tk = a.get("ticker")
        if not tk:
            continue
        allocation.append(tk)
        if float(a.get("proposed_weight") or 0.0) > 0.0:
            target.append(tk)
    held = sorted(tk for tk, q in (holdings or {}).items() if tk and int(q or 0) != 0)
    benchmark = desk.BENCHMARK_TICKER
    required = sorted(set(target) | set(held) | {benchmark})
    return {"target_tickers": sorted(set(target)), "allocation_tickers": sorted(set(allocation)),
            "held_tickers": held, "benchmark": benchmark, "required": required,
            "n_target": len(set(target)), "n_held": len(held), "n_required": len(required)}


def mark_coverage(*, required: list[str], series: dict, as_of: Optional[str]) -> dict:
    """Which of the REQUIRED execution marks the owned desk mark store can actually price at
    or before ``as_of``. Read-only; never calls a provider."""
    req = sorted(set(required or []))
    available = [tk for tk in req if as_of and _price(series or {}, tk, as_of) is not None]
    missing = [tk for tk in req if tk not in set(available)]
    return {"required_mark_count": len(req), "available_mark_count": len(available),
            "missing_mark_count": len(missing), "missing_marks": missing,
            "available_marks": available, "marks_date": as_of,
            "coverage_complete": not missing}


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
             "corporate_actions_hash": ident.get("corporate_actions_hash"),
             "hoc_assessment_hash": ident.get("hoc_assessment_hash"),
             "universe_scoring_hash": ident.get("universe_scoring_hash"),
             "allocation_policy_version": ident.get("allocation_policy_version")}

    # Gate 0 (Stage 19.1): the proposal must have been computed against the CURRENT
    # corporate-action registry. A split registered after the proposal was produced changes
    # the economic share counts an order plan would reconcile against, while leaving the
    # immutable proposal artifact (and therefore its proposal_hash) untouched — so only the
    # registry fingerprint can detect it. No order plan is ever built from a stale proposal.
    ca_stale = realloc.corporate_action_staleness(
        artifact=artifact, portfolio_state=portfolio_state,
        active_book_id=bound["active_book_id"])
    if ca_stale.get("stale"):
        return {"state": RB_STALE, "bound": bound,
                "stale_reason": ca_stale.get("reason"),
                "corporate_action_staleness": ca_stale,
                "message": ("A corporate action has been registered since this proposal was "
                            "produced; the economic holdings it targets no longer describe "
                            "the current portfolio. No order plan can be built. Run the "
                            "Daily Research Cycle for a fresh proposal, then re-approve.")}

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
    # Stage 19.2 GATE 3 — FAIL CLOSED. A plan that cannot faithfully implement the approved
    # target NEVER reaches the confirmable review state. The partial plan is still returned
    # (it is the explanation the operator needs) but the state itself is non-confirmable.
    if not plan.get("order_plan_buildable"):
        reasons = [b["reason"] for b in plan.get("blocked_reasons") or []]
        marks_only = bool(plan.get("missing_marks"))
        state = RB_BLOCKED_MARKS if marks_only else RB_BLOCKED_INCOMPLETE
        return {"state": state, "bound": bound, "plan": plan, "desk_view": view,
                "decision_record": decision_record, "block_reason_codes": sorted(set(reasons)),
                "message": _blocked_message(plan, state)}
    return {"state": (RB_NO_CHANGES if not plan["orders"] else RB_PLAN_REVIEW_REQUIRED),
            "bound": bound, "plan": plan, "desk_view": view,
            # Release 47: the immutable proposal travels with the plan so the
            # execution boundary can freeze its economics as decision evidence
            # without re-reading (and possibly re-resolving) a different artifact.
            "artifact": artifact,
            "decision_record": decision_record}


def _blocked_message(plan: dict, state: str) -> str:
    """One operator-readable sentence per blocked state. No arithmetic here — every number
    is read verbatim from the plan the backend already computed."""
    if state == RB_BLOCKED_MARKS:
        missing = plan.get("missing_marks") or []
        return ("ORDER PLAN BLOCKED - OWNED MARKS REQUIRED. %d approved target name(s) have "
                "no owned execution mark at or before %s: %s. A missing mark is never "
                "permission to omit a holding, so no order plan can be confirmed. Run the "
                "explicit target-mark refresh, then reload."
                % (len(missing), plan.get("marks_date"), ", ".join(missing)))
    details = [b.get("detail") for b in (plan.get("blocked_reasons") or []) if b.get("detail")]
    return ("ORDER PLAN BLOCKED - the executable plan is not a faithful implementation of "
            "the approved proposal. " + " ".join(details))


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
    policy_w = {}          # proposed weight AFTER the explicitly supported concentration cap
    for a in allocs:
        tk = a.get("ticker")
        if not tk:
            continue
        sector_of[tk] = a.get("sector") or "Unknown"
        pw = float(a.get("proposed_weight") or 0.0)
        proposed_w[tk] = pw
        policy_w[tk] = min(pw, MAX_INDIVIDUAL_WEIGHT)
        px = _price(series, tk, as_of)
        cur_sh = int(held.get(tk, 0))
        if px is None or px <= 0:
            rows.append({"ticker": tk, "action": a.get("action"), "sector": sector_of[tk],
                         "current_shares": cur_sh, "target_shares": None, "order_shares": 0,
                         "side": None, "price": None, "proposed_weight": _r6(pw),
                         "blocked": True, "block_reason": BR_NO_OWNED_MARK,
                         "current_market_value": None, "target_market_value": None})
            continue
        # position cap: never target more than MAX_INDIVIDUAL_WEIGHT of NAV
        capped_w = policy_w[tk]
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
        policy_w[tk] = 0.0
        rows.append({"ticker": tk, "action": "EXIT", "sector": sector_of[tk],
                     "current_shares": int(cur_sh), "target_shares": 0,
                     "order_shares": -int(cur_sh), "price": _r6(px),
                     "proposed_weight": 0.0, "capped_weight": 0.0,
                     "blocked": px is None, "block_reason": (BR_NO_OWNED_MARK if px is None else None),
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
    # The UNTRIMMED buy requirement, captured BEFORE the capital trim: the difference
    # between it and the available capital is exactly the deterministic cash shortfall the
    # executability envelope below is allowed to absorb.
    required_buy_outflow = _buy_outflow(rows)
    capital_shortfall = max(0.0, required_buy_outflow - available)
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
    orders, blocked, omitted = [], [], []
    gross_sells = gross_buys = est_cost = 0.0
    for r in rows:
        if r["blocked"]:
            blocked.append({"ticker": r["ticker"], "reason": r["block_reason"],
                            "action": r.get("action"),
                            "proposed_weight": r.get("proposed_weight"),
                            "current_shares": r.get("current_shares")})
            continue
        n = r["order_shares"]
        if n == 0 or abs(n) < MIN_ORDER_SHARES:
            # A target action that produced NO order. Stage 19.2: never silent — name the
            # exact deterministic mechanic that consumed it, so an unexplained omission
            # cannot hide as "no order required".
            if _implies_trade(r):
                if r["ticker"] in set(trimmed) and n == 0:
                    why = OMIT_CAPITAL
                elif n == 0 and int(r["current_shares"]) == int(r["target_shares"] or 0):
                    why = OMIT_WHOLE_SHARE if r.get("action") != "RETAIN" else OMIT_ALREADY_AT_TARGET
                else:
                    why = OMIT_MIN_ORDER
                omitted.append({"ticker": r["ticker"], "action": r.get("action"),
                                "reason": why,
                                "proposed_weight": r.get("proposed_weight"),
                                "current_shares": r["current_shares"],
                                "target_shares": r["target_shares"],
                                "supported": why in SUPPORTED_OMISSIONS})
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
    # The same measure against the POLICY-ADJUSTED target (the concentration cap is an
    # explicitly supported deviation, so it must not be charged against executability).
    policy_te = 0.0
    for tk in set(list(policy_w) + list(after_w)):
        policy_te += abs(float(after_w.get(tk, 0.0) or 0.0) - float(policy_w.get(tk, 0.0)))
    policy_tracking_error = round(0.5 * policy_te, 6)

    planned_turnover = _r6((gross_sells + gross_buys) / 2.0 / nav) if nav else None

    # ----------------------------------------------------------------------- #
    # Stage 19.2 — THE FAIL-CLOSED EXECUTABILITY CONTRACT
    #
    # An approved proposal may only become an executable plan when the ONLY differences
    # between the approved target and the plan come from the explicitly supported
    # mechanics: whole shares, transaction cost, available cash, the concentration cap
    # and the minimum order size. Their combined dollar effect is BOUNDED here, and any
    # deviation larger than that bound is a defect, not an approximation.
    #
    # The August-12 defect was exactly this: eight ADD names were silently dropped for a
    # reason (no owned mark) that is NOT an execution mechanic at all, and the resulting
    # 19.33% plan was still offered for confirmation against a 35.55% approved proposal.
    # ----------------------------------------------------------------------- #
    universe = target_mark_universe(artifact=artifact, holdings=held)
    coverage = mark_coverage(required=universe["required"], series=series, as_of=as_of)
    # The benchmark is desk accounting, not a tradable target: report it, but only the
    # tradable names can block an order plan.
    tradable_missing = [t for t in coverage["missing_marks"] if t != universe["benchmark"]]

    priced_rows = [r for r in rows if r.get("price")]
    trim_counts: dict[str, int] = {}
    for tk in trimmed:
        trim_counts[tk] = trim_counts.get(tk, 0) + 1
    # ONE share of slack per name (floor rounding) + the transaction cost + the genuine
    # cash shortfall. Deliberately NOT proportional to how much was trimmed: an envelope
    # that grew with the trim would absorb its own error and could never block.
    share_slack = sum(float(r["price"]) for r in priced_rows)
    envelope_dollars = share_slack + est_cost + capital_shortfall
    executability_envelope = _r6(0.5 * envelope_dollars / nav) if nav else None

    proposal_turnover = _proposal_one_way_turnover(artifact)
    turnover_gap = (None if (proposal_turnover is None or planned_turnover is None)
                    else _r6(float(planned_turnover) - float(proposal_turnover)))

    proposal_action_count = sum(1 for a in allocs if _implies_trade(a)) + sum(
        1 for r in rows if r["ticker"] not in {a.get("ticker") for a in allocs}
        and _implies_trade(r))
    planned_action_count = len(orders)
    unsupported_omissions = [o for o in omitted if not o["supported"]]

    block_reasons: list[dict] = []
    if tradable_missing:
        block_reasons.append({
            "reason": BR_NO_OWNED_MARK, "tickers": tradable_missing,
            "detail": ("%d approved target / held name(s) have no owned execution mark at or "
                       "before %s. A missing mark is NOT permission to omit a holding: the "
                       "order plan fails closed until the marks are hydrated."
                       % (len(tradable_missing), as_of))})
    if unsupported_omissions:
        block_reasons.append({
            "reason": BR_TARGET_OMITTED,
            "tickers": sorted({o["ticker"] for o in unsupported_omissions}),
            "detail": ("%d approved target action(s) produced no order for a reason outside "
                       "the supported whole-share / minimum-order / capital envelope."
                       % len(unsupported_omissions))})
    if (executability_envelope is not None
            and policy_tracking_error > float(executability_envelope) + 1e-9):
        block_reasons.append({
            "reason": BR_TRACKING_ERROR, "tickers": [],
            "detail": ("The executable plan tracks the approved target to %.6f, outside the "
                       "deterministic whole-share / cost / cash envelope of %.6f. The plan is "
                       "not an acceptable implementation of the approved proposal."
                       % (policy_tracking_error, float(executability_envelope)))})
    if (turnover_gap is not None and executability_envelope is not None
            and abs(float(turnover_gap)) > float(executability_envelope) + 1e-9):
        block_reasons.append({
            "reason": BR_TURNOVER_GAP, "tickers": [],
            "detail": ("Executable one-way turnover %.6f vs approved proposal turnover %.6f "
                       "(gap %.6f) exceeds the deterministic envelope %.6f."
                       % (float(planned_turnover or 0.0), float(proposal_turnover or 0.0),
                          float(turnover_gap), float(executability_envelope)))})
    # Target reconciliation: every proposed positive-weight constituent must end up either
    # HELD at its target share count or explicitly, supportedly omitted. Nothing may vanish.
    unrepresented = []
    for tk in universe["target_tickers"]:
        if int(achieved_sh.get(tk, 0) or 0) > 0:
            continue
        if any(o["ticker"] == tk and o["supported"] for o in omitted):
            continue
        unrepresented.append(tk)
    if unrepresented:
        block_reasons.append({
            "reason": BR_RECONCILIATION, "tickers": sorted(unrepresented),
            "detail": ("%d approved positive-weight target constituent(s) are absent from the "
                       "resulting portfolio and are not explained by a supported deterministic "
                       "policy." % len(unrepresented))})

    blocked_tickers = sorted({t for b in block_reasons for t in b["tickers"]}
                             | {b["ticker"] for b in blocked})
    buildable = not block_reasons

    plan_core = {
        "orders": orders, "blocked": blocked,
        "n_sell": sum(1 for o in orders if o["side"] == desk.SIDE_SELL),
        "n_buy": sum(1 for o in orders if o["side"] == desk.SIDE_BUY),
        "estimated_sell_proceeds": _r2(gross_sells - gross_sells * COST_RATE_PER_SIDE),
        "estimated_buy_cost": _r2(gross_buys + gross_buys * COST_RATE_PER_SIDE),
        "estimated_transaction_cost": round(est_cost, 2),
        "gross_sells": _r2(gross_sells), "gross_buys": _r2(gross_buys),
        "residual_cash": _r2(residual_cash),
        "one_way_turnover": planned_turnover,
        "target_tracking_error": tracking_error,
        "trimmed_for_capital": sorted(set(trimmed)),
        "before_weights": before_w, "after_weights": after_w,
        "sizing_nav_basis": _r2(nav), "sizing_cash": _r2(cash), "marks_date": as_of,
        "policy": {"max_individual_weight": MAX_INDIVIDUAL_WEIGHT,
                   "sector_cap_fraction": SECTOR_CAP_FRACTION,
                   "min_order_shares": MIN_ORDER_SHARES,
                   "cost_bps_per_side": COST_BPS_PER_SIDE, "whole_shares_only": True,
                   "execution_model": EXECUTION_MODEL, "long_only": True},
        # --- the Stage 19.2 fail-closed contract (structured, never free text) --- #
        "order_plan_buildable": buildable,
        "blocked_tickers": blocked_tickers,
        "blocked_count": len(blocked_tickers),
        "blocked_reasons": block_reasons,
        "block_reason_vocabulary": list(BLOCK_REASON_VOCAB),
        "required_mark_count": coverage["required_mark_count"],
        "available_mark_count": coverage["available_mark_count"],
        "missing_mark_count": len(tradable_missing),
        "missing_marks": tradable_missing,
        "mark_coverage": coverage,
        "target_mark_universe": universe,
        "proposal_action_count": proposal_action_count,
        "planned_action_count": planned_action_count,
        "omitted_actions": omitted,
        "unsupported_omission_count": len(unsupported_omissions),
        "proposal_one_way_turnover": proposal_turnover,
        "planned_one_way_turnover": planned_turnover,
        "turnover_gap": turnover_gap,
        "policy_target_tracking_error": policy_tracking_error,
        "executability_envelope": executability_envelope,
        "envelope_components": {
            "whole_share_slack": _r2(share_slack),
            "transaction_cost": _r2(est_cost),
            "capital_shortfall": _r2(capital_shortfall),
            "nav_basis": _r2(nav)},
        "supported_execution_mechanics": ["WHOLE_SHARES", "TRANSACTION_COST",
                                          "AVAILABLE_CASH", "CONCENTRATION_POLICY",
                                          "MIN_ORDER_POLICY"],
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
    # Stage 19.2 — exactly ONE next action out of each blocked state.
    RB_BLOCKED_MARKS: {"label": "Refresh owned marks for the approved target",
                       "path": "POST /v1/operations/rebalance/refresh-target-marks"},
    RB_BLOCKED_INCOMPLETE: {"label": "Run the Daily Research Cycle for a fresh proposal",
                            "path": "POST /v1/operations/daily-research-cycle/run"},
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


# --------------------------------------------------------------------------- #
# Stage 19.3 (Workstreams H + I) — the CURRENT-rebalance execution summary.
#
# The August-13 live surface showed "Submitted 29" next to "Filled 25", which reads
# as a partially-filled current rebalance. It was not: the 29 belong to the repaired
# plan (0 filled), the 25 are the book's historical initial implementation, and a
# further 22 belong to the CANCELLED defective plan. Every count below is filtered by
# the CURRENT proposal/order-plan lineage; the other cohorts are reported separately
# and stay fully auditable. Pure read over the desk fold — nothing is recomputed.
# --------------------------------------------------------------------------- #
EXECUTION_STAGES = (RB_PROPOSAL_REVIEW_REQUIRED, RB_PLAN_REVIEW_REQUIRED,
                    RB_PLAN_CONFIRMED, RB_EXECUTED)
_EXECUTION_STAGE_LABELS = {
    RB_PROPOSAL_REVIEW_REQUIRED: "PROPOSAL REVIEW",
    RB_PLAN_REVIEW_REQUIRED: "ORDER PLAN REVIEW",
    RB_PLAN_CONFIRMED: "PAPER EXECUTION PENDING",
    RB_EXECUTED: "PAPER EXECUTED / RECONCILED",
}


def build_execution_summary(sdir, *, bound: Optional[dict], state: str) -> dict:
    """Lineage-scoped execution counts + the four-stage lifecycle position."""
    bound = bound or {}
    cohort = _rebalance_orders_for_proposal(sdir, bound.get("proposal_hash"))
    # Stage 21 (Workstream 0A) — CHRONOLOGICAL plan selection.
    #
    # This previously used ``sorted(plan_ids)[-1]``. A plan id ends in a HASH, so that
    # ordering is arbitrary: on the live book it ranks the DEFECTIVE, fully cancelled
    # plan ``..._5bf9c6c20f8a`` above the EXECUTED plan ``..._1a198f560cca`` purely
    # because "5" sorts after "1". Whenever the live cohort was empty the read model
    # therefore presented the defective plan as the current rebalance and described the
    # 29-order executed plan as superseded.
    #
    # Selection is now: among the plans that are NOT fully cancelled (i.e. at least one
    # order filled or is still live), the newest wins. Only when every plan is dead does
    # the newest dead plan surface. "Newest" is the lineage's own recorded ``created_at``
    # — never an id, never a hash.
    def _created_at(pid: str) -> str:
        for o in cohort:
            lin = o.get("rebalance_lineage") or {}
            if lin.get("order_plan_id") == pid:
                return str(lin.get("created_at") or "")
        return ""

    def _newest(orders_subset) -> Optional[str]:
        ids = {(o.get("rebalance_lineage") or {}).get("order_plan_id")
               for o in orders_subset} - {None}
        return max(ids, key=lambda p: (_created_at(p), p)) if ids else None

    live = [o for o in cohort if o["status"] not in (desk.ST_CANCELLED, desk.ST_EXPIRED)]
    current_plan_id = _newest(live) or _newest(cohort)
    current = [o for o in cohort
               if (o.get("rebalance_lineage") or {}).get("order_plan_id") == current_plan_id]

    def _n(*statuses) -> int:
        return sum(1 for o in current if o["status"] in statuses)

    submitted = _n(desk.ST_APPROVED, desk.ST_SUBMITTED)
    filled = _n(desk.ST_FILLED)
    cancelled = _n(desk.ST_CANCELLED)
    superseded = [o for o in cohort
                  if (o.get("rebalance_lineage") or {}).get("order_plan_id") != current_plan_id]
    approvals = sorted({str(o.get("approval_date") or "") for o in current} - {""})
    # Book-wide fills that carry NO rebalance lineage: the historical initial
    # implementation. Reported so the operator can see it is a DIFFERENT thing.
    historical_fills = sum(
        1 for o in desk._orders_state(sdir).values()
        if o["status"] == desk.ST_FILLED and not (o.get("rebalance_lineage") or {})
        .get("order_plan_id"))
    return {
        "lifecycle_stage": state if state in EXECUTION_STAGES else None,
        "lifecycle_stage_label": _EXECUTION_STAGE_LABELS.get(state),
        "lifecycle_stages": [
            {"stage": i + 1, "code": c, "label": _EXECUTION_STAGE_LABELS[c],
             "current": c == state}
            for i, c in enumerate(EXECUTION_STAGES)],
        "order_plan_id": current_plan_id,
        "order_plan_id_short": (current_plan_id or "")[-12:] or None,
        # The hash is read from the orders' own recorded lineage (``bound`` describes the
        # proposal, not the plan), so it is the hash the operator actually confirmed.
        "order_plan_hash": ((current[0].get("rebalance_lineage") or {}).get("order_plan_hash")
                            if current else None),
        "proposal_id": bound.get("proposal_id"),
        "approval_date": approvals[-1] if approvals else None,
        "execution_model": desk.EXECUTION_MODEL_DEFAULT,
        "order_count": len(current),
        "submitted_count": submitted,
        "filled_count": filled,
        "cancelled_count": cancelled,
        "buy_count": sum(1 for o in current if o.get("side") == desk.SIDE_BUY),
        "sell_count": sum(1 for o in current if o.get("side") == desk.SIDE_SELL),
        "further_confirmation_required": False,
        "expected_next_execution_event": (
            "Fills at the first eligible completed owned close on or after %s, settled by "
            "that session's Daily Close." % (approvals[-1] if approvals else "approval")
            if submitted else None),
        # -- explicitly separated cohorts (auditable, never mixed above) ------- #
        "superseded_plan_order_count": len(superseded),
        "superseded_plan_ids": sorted({(o.get("rebalance_lineage") or {}).get("order_plan_id")
                                       for o in superseded} - {None}),
        "historical_implementation_fill_count": historical_fills,
        "counts_are_lineage_scoped": True,
        "current_rebalance_label": (
            "Current rebalance: %d submitted / %d filled" % (submitted, filled)),
        "historical_label": ("Existing operational holdings from the initial "
                             "implementation: %d filled order(s)" % historical_fills),
    }


def _latest_completed_rebalance(*, desk_dir=None) -> Optional[dict]:
    """Delegate to the ONE Stage-21 execution-lineage owner. Degrade-safe: a lineage
    failure must never break the rebalance read (it is evidence, not a gate)."""
    try:
        from paper_trader.api import execution_lineage as el
        return el.load_execution_lineage(desk_dir=desk_dir).get(
            "latest_completed_rebalance")
    except Exception:  # noqa: BLE001
        return None


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
                "state_vocabulary": list(STATE_VOCAB),
                "order_plan_buildable": False, "confirmation_available": False,
                "blocked_tickers": [], "blocked_count": 0, "blocked_reasons": [],
                "missing_marks": [], "order_plan": None,
                "latest_completed_rebalance": _latest_completed_rebalance(desk_dir=desk_dir),
                "confirm_required_token": CONFIRM_TOKEN,
                "target_mark_refresh_token": HYDRATE_CONFIRM_TOKEN,
                "provider_called": False, "performed_write": False, "created_orders": False,
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
             RB_STALE: "Proposal superseded — fresh review required",
             RB_PLAN_REVIEW_REQUIRED: "Order plan review required",
             RB_PLAN_CONFIRMED: "Paper execution pending (NEXT_CLOSE)",
             RB_EXECUTED: "Paper executed & reconciled",
             RB_NO_CHANGES: "Holdings already match the approved target",
             RB_UNAVAILABLE: "Rebalance state unavailable",
             RB_BLOCKED_MARKS: "ORDER PLAN BLOCKED — owned marks required",
             RB_BLOCKED_INCOMPLETE: "ORDER PLAN BLOCKED — incomplete target"}.get(state, state)

    # Stage 19.2: executability is a property of the PLAN, never of the state name alone.
    # The August-12 defect was precisely a state-derived `True` sitting on top of a plan
    # that had already recorded eight blocked names.
    buildable = bool(plan.get("order_plan_buildable")) if plan else False
    if state in NON_CONFIRMABLE_STATES:
        buildable = False
    if state in (RB_PLAN_CONFIRMED, RB_EXECUTED, RB_NO_CHANGES):
        buildable = False

    out = {
        "phase": PHASE, "owner": OWNER, "status": "OK", "generated_at": generated_at,
        "rebalance_state": state, "state_vocabulary": list(STATE_VOCAB), "label": label,
        "bound": base.get("bound"), "active_book_id": base.get("active_book_id"),
        "message": base.get("message"),
        "primary_action": _PRIMARY_ACTION.get(state),
        "confirm_required_token": CONFIRM_TOKEN,
        "target_mark_refresh_token": HYDRATE_CONFIRM_TOKEN,
        "order_plan": plan,
        "executed_order_ids": [o["order_id"] for o in executed],
        "executed_order_status": {o["order_id"]: o["status"] for o in executed},
        # -- Stage 19.3 (Workstreams H + I): the compact, LINEAGE-SCOPED execution
        # summary every operator surface renders for the CURRENT rebalance. It never
        # mixes the historical initial-implementation fills or a superseded/cancelled
        # plan into a current-state count. --------------------------------------- #
        "execution_summary": build_execution_summary(sdir, bound=bound, state=state),
        # --- Stage 21 (Workstream 0A): the COMPLETED rebalance, recovered from the
        # immutable desk ledger by the ONE execution-lineage owner. The block above is
        # scoped to the CURRENT proposal and therefore vanishes once the eligible
        # session advances past it — which is exactly how a fully executed 29-order
        # rebalance became undiscoverable while the read model reported
        # REBALANCE_NO_PROPOSAL. This block does not depend on the current proposal at
        # all, so settlement can never erase the evidence of what was executed. ------ #
        "latest_completed_rebalance": _latest_completed_rebalance(desk_dir=desk_dir),
        # Stage 19.1 — why a proposal is stale, and the explicit executability contract.
        "stale_reason": base.get("stale_reason"),
        "corporate_action_staleness": base.get("corporate_action_staleness"),
        "order_plan_buildable": buildable,
        "confirmation_available": buildable and bool((plan or {}).get("orders")),
        # --- Stage 19.2 fail-closed contract, surfaced at the TOP level so no operator
        # surface has to dig into diagnostics to discover that names were dropped. --- #
        "blocked_tickers": (plan or {}).get("blocked_tickers") or [],
        "blocked_count": (plan or {}).get("blocked_count") or 0,
        "blocked_reasons": (plan or {}).get("blocked_reasons") or [],
        "block_reason_codes": base.get("block_reason_codes") or [],
        "required_mark_count": (plan or {}).get("required_mark_count"),
        "available_mark_count": (plan or {}).get("available_mark_count"),
        "missing_mark_count": (plan or {}).get("missing_mark_count"),
        "missing_marks": (plan or {}).get("missing_marks") or [],
        "proposal_action_count": (plan or {}).get("proposal_action_count"),
        "planned_action_count": (plan or {}).get("planned_action_count"),
        "proposal_one_way_turnover": (plan or {}).get("proposal_one_way_turnover"),
        "planned_one_way_turnover": (plan or {}).get("planned_one_way_turnover"),
        "turnover_gap": (plan or {}).get("turnover_gap"),
        "target_tracking_error": (plan or {}).get("target_tracking_error"),
        "executability_envelope": (plan or {}).get("executability_envelope"),
        "residual_cash": (plan or {}).get("residual_cash"),
        "marks_date": (plan or {}).get("marks_date"),
        "provider_called": False, "performed_write": False, "created_orders": False,
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
                                 outcome_dir=None,
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
                "stale_reason": base.get("stale_reason"),
                "corporate_action_staleness": base.get("corporate_action_staleness"),
                "approved_proposal_hash": base.get("approved_proposal_hash"),
                "current_proposal_hash": base.get("current_proposal_hash")}
    if state == RB_UNAVAILABLE:
        return {**base_safety, "status": RB_UNAVAILABLE, "message": base.get("message")}
    plan = base.get("plan") or {}

    # ----------------------------------------------------------------------- #
    # Stage 19.2 — INDEPENDENT SERVER-SIDE REVALIDATION, before the first write.
    #
    # `base` above was rebuilt from the CURRENT stores at this instant: the proposal, the
    # decision, the corporate-action registry, the desk holdings and the owned marks are
    # all re-read here, so a plan that was executable when the operator reviewed it but is
    # not executable NOW is refused. A browser boolean is never trusted; the UI cannot
    # reach this branch at all, because the refusal is decided from the freshly rebuilt
    # plan. The refusal happens BEFORE any ledger append, so it is atomic: zero orders,
    # zero fills, zero holding / cash / NAV change.
    # ----------------------------------------------------------------------- #
    if state in NON_CONFIRMABLE_STATES or not plan.get("order_plan_buildable"):
        return {**base_safety, "status": C_BLOCKED, "rebalance_state": state,
                "revalidated_server_side": True, "refused_before_any_write": True,
                "message": base.get("message") or _blocked_message(plan, state),
                "bound": base.get("bound"),
                "order_plan_buildable": False,
                "blocked_tickers": plan.get("blocked_tickers") or [],
                "blocked_count": plan.get("blocked_count") or 0,
                "blocked_reasons": plan.get("blocked_reasons") or [],
                "block_reason_codes": base.get("block_reason_codes") or [],
                "required_mark_count": plan.get("required_mark_count"),
                "available_mark_count": plan.get("available_mark_count"),
                "missing_mark_count": plan.get("missing_mark_count"),
                "missing_marks": plan.get("missing_marks") or [],
                "proposal_action_count": plan.get("proposal_action_count"),
                "planned_action_count": plan.get("planned_action_count"),
                "proposal_one_way_turnover": plan.get("proposal_one_way_turnover"),
                "planned_one_way_turnover": plan.get("planned_one_way_turnover"),
                "turnover_gap": plan.get("turnover_gap"),
                "target_tracking_error": plan.get("target_tracking_error"),
                "executability_envelope": plan.get("executability_envelope"),
                "order_plan_id": plan.get("order_plan_id"),
                "order_plan_hash": plan.get("order_plan_hash"),
                "target_mark_refresh_token": HYDRATE_CONFIRM_TOKEN,
                "next_action": _PRIMARY_ACTION.get(state)}

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
    # Idempotency: the exact plan already has a LIVE order set -> reuse, ZERO duplicates.
    #
    # Stage 19.2 (Workstream F): terminal CANCELLED / EXPIRED orders are immutable evidence,
    # not a live lineage. A defective plan that the operator cancelled must therefore NOT
    # permanently veto a later, repaired plan for the same proposal — while a plan with any
    # live or filled order stays strictly idempotent. Order ids are sequenced from the total
    # ledger length, so a recovery set can never collide with the cancelled one.
    lineage_orders = _executed_orders_for_plan(sdir, plan["order_plan_id"])
    already = [o for o in lineage_orders if o["status"] not in (desk.ST_CANCELLED,
                                                                desk.ST_EXPIRED)]
    cancelled_prior = [o for o in lineage_orders if o["status"] in (desk.ST_CANCELLED,
                                                                    desk.ST_EXPIRED)]
    if already:
        return {**base_safety, "status": C_REUSED, "reused": True,
                "rebalance_state": RB_PLAN_CONFIRMED,
                "order_plan_id": plan["order_plan_id"],
                "order_plan_hash": plan["order_plan_hash"],
                "existing_order_ids": [o["order_id"] for o in already],
                "cancelled_prior_order_ids": [o["order_id"] for o in cancelled_prior],
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
    order_ids = [e["order"]["order_id"] for e in order_events]
    # ----------------------------------------------------------------------- #
    # Release 47 — freeze the portfolio-decision forward evidence HERE.
    #
    # This is the only moment at which an honest counterfactual can be created: the
    # capital decision has just been made and not one forward price exists yet. Both
    # paths (the executed paper portfolio and the hold portfolio we are giving up)
    # are frozen together, with the decision session's own marks. Freezing is
    # idempotent on the exact order plan, so an approval replay records nothing new,
    # and it writes ONLY to its own evidence root — never a desk ledger.
    # ----------------------------------------------------------------------- #
    decision_evidence = _freeze_decision_evidence(
        plan=plan, base=base, bound=bound, artifact=base.get("artifact"),
        n_orders=len(order_ids),
        outcome_dir=_evidence_dir(outcome_dir, desk_dir))
    # Settle now (no-hindsight: nothing fills at a close already known at approval).
    settle = desk.settle_due_orders(desk_dir=desk_dir, today=today)
    return {**base_safety, "status": C_CREATED, "performed_write": True, "created_orders": True,
            "wrote_to_desk_ledgers_only": True, "rebalance_state": RB_PLAN_CONFIRMED,
            "revalidated_server_side": True,
            "order_plan_id": plan["order_plan_id"], "order_plan_hash": plan["order_plan_hash"],
            "order_plan_buildable": True,
            "blocked_count": plan.get("blocked_count") or 0,
            "blocked_tickers": plan.get("blocked_tickers") or [],
            "proposal_one_way_turnover": plan.get("proposal_one_way_turnover"),
            "planned_one_way_turnover": plan.get("planned_one_way_turnover"),
            "turnover_gap": plan.get("turnover_gap"),
            "target_tracking_error": plan.get("target_tracking_error"),
            "executability_envelope": plan.get("executability_envelope"),
            "residual_cash": plan.get("residual_cash"),
            "recovered_from_cancelled_plan": bool(cancelled_prior),
            "cancelled_prior_order_ids": [o["order_id"] for o in cancelled_prior],
            "n_orders_created": len(order_ids), "orders_created": order_ids,
            "approval_date": approval_date, "marks_latest_at_approval": marks_latest,
            "no_hindsight_note": ("Orders are SUBMITTED; they can only fill at the first "
                                  "completed owned close strictly after %s — never the "
                                  "already-known close." % marks_latest),
            "settlement": settle, "lineage": lineage,
            "decision_evidence": decision_evidence,
            "message": ("%d paper rebalance order(s) SUBMITTED for NEXT_CLOSE. Fills occur at "
                        "the next completed owned close via the existing desk settlement."
                        % len(order_ids))}


# --------------------------------------------------------------------------- #
# Release 47 — the decision-evidence freeze (composition only; no calculation)
# --------------------------------------------------------------------------- #
def _evidence_dir(outcome_dir, desk_dir):
    """Where this execution's decision evidence belongs.

    A decision record describes ONE desk, so it lives beside that desk. Production
    calls inject neither root and land on the evidence owner's own default; an
    injected (hermetic) desk therefore keeps its evidence hermetic BY CONSTRUCTION,
    rather than by every caller remembering to redirect a second root. That
    distinction is the difference between a test that is isolated and a test that
    quietly appends to the production ledger.
    """
    if outcome_dir is not None:
        return outcome_dir
    if desk_dir is not None:
        return Path(desk_dir).parent / "portfolio_decision_outcomes"
    return None


def _freeze_decision_evidence(*, plan: dict, base: dict, bound: dict,
                              artifact: Optional[dict], n_orders: int,
                              outcome_dir=None) -> dict:
    """Hand the executed decision to its evidence owner. Never raises.

    Everything passed here is already owned by somebody else: the before/after
    weights and the cost come from the reconciled order plan, the reference prices
    from the desk's own marks, the expected improvement and the constraint state
    from the immutable proposal. This function computes none of them — a failure to
    record evidence must never undo a completed, correct paper execution, so it
    degrades to a reported error instead of propagating.
    """
    try:
        prop = (artifact or {}).get("proposal") or {}
        view = base.get("desk_view") or {}
        series = view.get("series") or {}
        as_of = plan.get("marks_date")
        before_w = {k: v for k, v in (plan.get("before_weights") or {}).items()
                    if (v or 0) > 0}
        after_w = {k: v for k, v in (plan.get("after_weights") or {}).items()
                   if (v or 0) > 0}
        proposed_w = {a.get("ticker"): a.get("proposed_weight")
                      for a in (prop.get("allocations") or [])
                      if a.get("ticker") and (a.get("proposed_weight") or 0) > 0}
        prices = {}
        for tk in sorted(set(before_w) | set(after_w) | set(proposed_w)):
            p = _price(series, tk, as_of) if as_of else None
            if p is not None:
                prices[tk] = p
        return pdo.freeze_executed_decision(
            proposal_hash=bound.get("proposal_hash"),
            order_plan_id=plan.get("order_plan_id"),
            eligible_market_date=bound.get("eligible_market_date"),
            active_book_id=bound.get("active_book_id"),
            previous_weights=before_w, proposed_weights=proposed_w,
            executed_weights=after_w, reference_prices=prices,
            nav=plan.get("sizing_nav_basis"),
            transaction_cost=plan.get("estimated_transaction_cost"),
            orders_created=int(n_orders or 0),
            decision_reasons={
                "outcome": prop.get("outcome"),
                "reallocation_outcome": prop.get("reallocation_outcome") or {},
                "action_counts": prop.get("action_counts") or {},
                "constraint_reoptimization_applied": bool(
                    (prop.get("constraint_reoptimization") or {}).get("applied")),
                "constraints_that_reshaped": list(
                    (prop.get("constraint_reoptimization") or {}).get(
                        "constraints_that_reshaped") or [])},
            expected_improvement={
                "switching_economics": prop.get("switching_economics") or {},
                "signal": prop.get("signal") or {},
                "turnover": prop.get("turnover") or {}},
            risk_at_decision=prop.get("risk") or {},
            constraints_at_decision={
                "constraints": prop.get("constraints") or {},
                "complete_target_limits": prop.get("complete_target_limits") or {},
                "policy": prop.get("policy") or {}},
            model_state={
                "allocation_policy_version": prop.get("policy_version"),
                "proposal_schema_version": prop.get("schema_version"),
                "proposal_id": bound.get("proposal_id"),
                "universe_scoring_hash": bound.get("universe_scoring_hash"),
                "hoc_assessment_hash": bound.get("hoc_assessment_hash"),
                "portfolio_state_hash": bound.get("portfolio_state_hash"),
                "corporate_actions_hash": bound.get("corporate_actions_hash")},
            provenance={"execution_owner": OWNER,
                        "order_plan_hash": plan.get("order_plan_hash"),
                        "marks_date": as_of,
                        "execution_model": EXECUTION_MODEL},
            outcome_dir=outcome_dir)
    except Exception as exc:  # noqa: BLE001 - evidence must never break execution
        return {"owner": pdo.OWNER, "frozen": False,
                "status": "DECISION_EVIDENCE_FREEZE_ERROR",
                "message": str(exc)[:200]}


# --------------------------------------------------------------------------- #
# Stage 19.2 — explicit target-mark hydration (Workstream B)
#
# This is NOT a second mark writer and NOT a second EODHD client. It resolves the required
# execution universe of the APPROVED proposal and hands it to the ONE canonical desk mark
# owner (``desk.refresh_desk``), which keeps sole ownership of the transport, the
# normalization, the completed-session rule, the store write and the coverage taxonomy.
# --------------------------------------------------------------------------- #
def refresh_target_marks(*, confirm: Optional[str] = None, decision_dir=None,
                         reallocation_dir=None, desk_dir=None, actions_dir=None,
                         ledger_dir=None, active_book_id=None, eligible_market_date=None,
                         portfolio_state=None, portfolio_state_loader=None, artifact=None,
                         decision_record=None, corporate_actions=None, downloader=None,
                         today: Optional[str] = None,
                         completed_through: Optional[str] = None) -> dict:
    """Hydrate the owned execution marks the APPROVED reallocation target needs.

    Explicit and manual: it requires ``confirm == HYDRATE_CONFIRM_TOKEN``. A GET never
    reaches it, so no page load can call the provider or move a mark. It creates no order,
    no fill and no decision; the only store it can change is the desk mark cache, and only
    through the canonical owner."""
    base_safety = {"owner": OWNER, "phase": PHASE, "performed_write": False,
                   "created_orders": False, "created_fills": False,
                   "changed_holdings": False, "changed_cash": False, "changed_nav": False,
                   "delegated_to_mark_owner": "api.paper_trading_desk.refresh_desk",
                   **_safety()}
    if confirm != HYDRATE_CONFIRM_TOKEN:
        return {**base_safety, "status": H_CONFIRM_REQUIRED,
                "message": ("Refreshing the approved target's owned marks requires "
                            "confirm='%s'." % HYDRATE_CONFIRM_TOKEN),
                "confirm_required_token": HYDRATE_CONFIRM_TOKEN}

    before = load_rebalance_state(
        decision_dir=decision_dir, reallocation_dir=reallocation_dir, desk_dir=desk_dir,
        actions_dir=actions_dir, active_book_id=active_book_id,
        eligible_market_date=eligible_market_date, portfolio_state=portfolio_state,
        portfolio_state_loader=portfolio_state_loader, artifact=artifact,
        decision_record=decision_record, corporate_actions=corporate_actions)
    plan_before = before.get("order_plan") or {}
    universe = plan_before.get("target_mark_universe")
    if universe is None:
        # No executable plan context (no approved proposal / no desk): resolve the universe
        # directly from the artifact so an operator is never left without a next step.
        art = artifact
        if art is None:
            book_id, elig = _resolve_book_and_date(
                active_book_id, eligible_market_date, portfolio_state,
                portfolio_state_loader, desk_dir)
            art = realloc.load_latest_artifact(active_book_id=book_id,
                                               eligible_market_date=elig,
                                               reallocation_dir=reallocation_dir)
        view = _current_desk_view(desk_dir, actions_dir, corporate_actions)
        universe = target_mark_universe(artifact=art, holdings=view.get("holdings") or {})
    if before.get("rebalance_state") in (RB_PROPOSAL_REVIEW_REQUIRED, RB_NO_PROPOSAL,
                                         RB_NO_ACTIVE_BOOK, RB_STALE):
        return {**base_safety, "status": H_NOT_APPROVED,
                "rebalance_state": before.get("rebalance_state"),
                "target_mark_universe": universe,
                "message": ("Owned marks are hydrated for an APPROVED reallocation target. "
                            "%s" % (before.get("message") or ""))}

    refresh = desk.refresh_desk(confirm=desk.REFRESH_CONFIRM_TOKEN, desk_dir=desk_dir,
                               ledger_dir=ledger_dir, downloader=downloader, today=today,
                               completed_through=completed_through,
                               extra_tickers=list(universe["required"]))
    after = load_rebalance_state(
        decision_dir=decision_dir, reallocation_dir=reallocation_dir, desk_dir=desk_dir,
        actions_dir=actions_dir, active_book_id=active_book_id,
        eligible_market_date=eligible_market_date, portfolio_state=portfolio_state,
        portfolio_state_loader=portfolio_state_loader, artifact=artifact,
        decision_record=decision_record, corporate_actions=corporate_actions)
    plan_after = after.get("order_plan") or {}
    still_missing = plan_after.get("missing_marks") or []
    status = H_DONE if not still_missing else H_INCOMPLETE
    return {**base_safety, "status": status,
            "performed_write": bool(refresh.get("performed_write")),
            "wrote_to_desk_mark_store_only": True,
            "target_mark_universe": universe,
            "requested_ticker_count": len(universe["required"]),
            "missing_marks_before": plan_before.get("missing_marks") or [],
            "missing_marks_after": still_missing,
            "missing_mark_count_before": plan_before.get("missing_mark_count"),
            "missing_mark_count_after": plan_after.get("missing_mark_count"),
            "rebalance_state_before": before.get("rebalance_state"),
            "rebalance_state_after": after.get("rebalance_state"),
            "order_plan_buildable_before": before.get("order_plan_buildable"),
            "order_plan_buildable_after": after.get("order_plan_buildable"),
            "desk_refresh": refresh,
            "next_action": after.get("primary_action"),
            "message": ("Owned marks refreshed for the approved target universe (%d name(s)) "
                        "through the canonical desk mark owner. %s"
                        % (len(universe["required"]),
                           ("Every required execution mark is now present; reload the order "
                            "plan for review." if not still_missing else
                            "%d name(s) still have no owned mark: %s."
                            % (len(still_missing), ", ".join(still_missing)))))}


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
    "PHASE", "OWNER", "CONFIRM_TOKEN", "HYDRATE_CONFIRM_TOKEN", "STATE_VOCAB",
    "NON_CONFIRMABLE_STATES", "PLAN_DIR_ENV",
    "RB_NO_ACTIVE_BOOK", "RB_NO_PROPOSAL", "RB_PROPOSAL_REVIEW_REQUIRED", "RB_STALE",
    "RB_PLAN_REVIEW_REQUIRED", "RB_PLAN_CONFIRMED", "RB_EXECUTED", "RB_NO_CHANGES",
    "RB_UNAVAILABLE", "RB_BLOCKED_MARKS", "RB_BLOCKED_INCOMPLETE",
    "C_CONFIRM_REQUIRED", "C_NOT_APPROVED", "C_STALE", "C_NO_CHANGES",
    "C_CREATED", "C_REUSED", "C_BLOCKED",
    "H_CONFIRM_REQUIRED", "H_NOT_APPROVED", "H_DONE", "H_INCOMPLETE",
    "BR_NO_OWNED_MARK", "BR_TARGET_OMITTED", "BR_TRACKING_ERROR", "BR_TURNOVER_GAP",
    "BR_RECONCILIATION", "BLOCK_REASON_VOCAB", "SUPPORTED_OMISSIONS",
    "target_mark_universe", "mark_coverage",
    "load_rebalance_state", "confirm_rebalance_order_plan", "refresh_target_marks",
]
