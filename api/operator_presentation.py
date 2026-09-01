"""Release 49 — the ONE reconciled operator presentation.

The normal operator UI used to render many raw subsystem states independently
(``MANUAL_REVIEW_REQUIRED`` beside ``PORTFOLIO CONSTRAINT BREACH`` beside
``STATE NOT_RUN`` beside ``NO PROPOSAL YET - RUN THE DAILY RESEARCH CYCLE``
beside ``REBALANCE_NO_PROPOSAL``), each true of its own owner and none of them an
operator answer. This module is the ONE read-only place where those authoritative
states are RECONCILED into one operator truth:

    WHAT IS HAPPENING?      -> system_readiness
    WHAT SHOULD WE OWN?     -> portfolio_decision + decision_summary
    WHAT DO I NEED TO DO?   -> next_action (at most ONE)

It is a PRESENTATION owner and nothing else. It CONSUMES the canonical owners —
``api.workflow_state`` (overall state, operator command, canonical portfolio
decision, portfolio-decision lane, reassessment lane, operational state),
``api.reallocation_proposal.load_constrained_reallocation`` (Release-47 outcome,
best feasible target, switching economics, approval + execution state),
``api.daily_close`` (the P&L block), ``api.material_information`` (what arrived),
``api.portfolio_decision_outcome`` (forward evidence of executed decisions) and
``api.information_collection`` (collection lifecycle) — and it RECOMPUTES NONE of
them. No NAV, no target, no portfolio decision, no constraint, no HOC, no proposal,
no execution state and no research verdict is derived here: every number and every
verdict is read verbatim from the owner that decided it, and this module only
chooses which of the already-decided facts the operator sees first and in what
words.

Two hard rules the reconciliation obeys:

* A HISTORICAL session is never rerun, backfilled or rewritten. When the latest
  completed session was decided under the prior workflow (a per-holding cap breach
  recorded as a manual-review blocker, no governed Release-47 target for the
  session), the presentation says so — ``historical_context.historical`` — and
  names the next ELIGIBLE action (the next portfolio cycle after the next market
  close). It never fabricates a target and never tells the operator to rerun an
  immutable session.
* A collection-infrastructure problem is stated as DEGRADED unless the owners say
  it blocks the portfolio decision. A red infrastructure chip is not a portfolio
  blocker.

Read-only. No write, no provider call, no prediction call, no order, no approval.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Optional

PHASE = "R49"
OWNER = "api.operator_presentation"
SCHEMA_VERSION = "operator_presentation.v1"
ROUTE = "/v1/operations/operator-presentation"

# --------------------------------------------------------------------------- #
# Vocabularies (frozen; every consumer renders these words and nothing else).
# --------------------------------------------------------------------------- #
SYSTEM_READY = "READY"
SYSTEM_DEGRADED = "DEGRADED"
SYSTEM_BLOCKED = "BLOCKED"
SYSTEM_READINESS_VOCABULARY = (SYSTEM_READY, SYSTEM_DEGRADED, SYSTEM_BLOCKED)

PD_CYCLE_REQUIRED = "CYCLE_REQUIRED"
PD_REALLOCATE = "REALLOCATE"
PD_HOLD = "HOLD"
PD_BLOCKED = "BLOCKED"
PD_AWAITING_APPROVAL = "AWAITING_APPROVAL"
PD_AWAITING_CONFIRMATION = "AWAITING_CONFIRMATION"
PD_AWAITING_NEXT_CLOSE = "AWAITING_NEXT_CLOSE"
PD_OUTCOME_ACCRUING = "OUTCOME_ACCRUING"
PORTFOLIO_DECISION_VOCABULARY = (
    PD_CYCLE_REQUIRED, PD_REALLOCATE, PD_HOLD, PD_BLOCKED, PD_AWAITING_APPROVAL,
    PD_AWAITING_CONFIRMATION, PD_AWAITING_NEXT_CLOSE, PD_OUTCOME_ACCRUING)
PORTFOLIO_DECISION_LABELS = {
    PD_CYCLE_REQUIRED: "Cycle required",
    PD_REALLOCATE: "Reallocate",
    PD_HOLD: "Hold",
    PD_BLOCKED: "Blocked",
    PD_AWAITING_APPROVAL: "Awaiting approval",
    PD_AWAITING_CONFIRMATION: "Awaiting confirmation",
    PD_AWAITING_NEXT_CLOSE: "Awaiting next close",
    PD_OUTCOME_ACCRUING: "Outcome accruing",
}

NA_PORTFOLIO_CYCLE = "PORTFOLIO_CYCLE"        # executes through the ONE dispatcher
NA_REVIEW_REALLOCATION = "REVIEW_REALLOCATION"
NA_REVIEW_ORDER_PLAN = "REVIEW_ORDER_PLAN"
NA_REVIEW_BLOCKER = "REVIEW_BLOCKER"
NA_WAIT = "WAIT"
NA_NONE = "NONE"
NEXT_ACTION_VOCABULARY = (NA_PORTFOLIO_CYCLE, NA_REVIEW_REALLOCATION,
                          NA_REVIEW_ORDER_PLAN, NA_REVIEW_BLOCKER, NA_WAIT, NA_NONE)
NEXT_ACTION_LABELS = {
    NA_PORTFOLIO_CYCLE: "Run portfolio cycle",
    NA_REVIEW_REALLOCATION: "Review reallocation",
    NA_REVIEW_ORDER_PLAN: "Review order plan",
    NA_REVIEW_BLOCKER: "Review blocker",
}
#: The only next-action kind that EXECUTES anything, and it executes through the
#: Release-48 orchestration owner's own contract, supplied verbatim by the workflow
#: owner. Every other kind is navigation.
EXECUTING_NEXT_ACTION_KINDS = frozenset({NA_PORTFOLIO_CYCLE})

GOV_REVIEW = "REVIEW"
GOV_APPROVE = "APPROVE"
GOV_CONFIRM = "CONFIRM"
GOV_AWAIT_NEXT_CLOSE = "AWAIT_NEXT_CLOSE"
GOV_EXECUTED = "EXECUTED"
GOV_OUTCOME_ACCRUING = "OUTCOME_ACCRUING"
GOVERNANCE_SEQUENCE = (GOV_REVIEW, GOV_APPROVE, GOV_CONFIRM, GOV_AWAIT_NEXT_CLOSE,
                       GOV_EXECUTED, GOV_OUTCOME_ACCRUING)
GOVERNANCE_LABELS = {
    GOV_REVIEW: "Review", GOV_APPROVE: "Approve", GOV_CONFIRM: "Confirm order plan",
    GOV_AWAIT_NEXT_CLOSE: "Await next close", GOV_EXECUTED: "Executed",
    GOV_OUTCOME_ACCRUING: "Outcome accruing",
}
GOVERNANCE_OWNERS = {
    GOV_REVIEW: "api.reallocation_proposal",
    GOV_APPROVE: "api.portfolio_decision",
    GOV_CONFIRM: "api.rebalance_execution",
    GOV_AWAIT_NEXT_CLOSE: "api.daily_close (NEXT_CLOSE settlement through the desk owner)",
    GOV_EXECUTED: "api.rebalance_execution",
    GOV_OUTCOME_ACCRUING: "api.portfolio_decision_outcome",
}
STEP_DONE, STEP_CURRENT, STEP_UPCOMING = "DONE", "CURRENT", "UPCOMING"

SAFETY_MODE_LINE = "PAPER · MANUAL APPROVAL · AUTOMATION OFF"

#: Owner state strings this module READS (never writes, never re-derives). They are
#: named here once so a renamed owner constant fails loudly in the tests.
_CPD_NOT_RUN = "NOT_RUN"
_CPD_NO_CHANGE = "NO_CHANGE"
_CPD_WITHHELD = "CHANGE_CANDIDATE_WITHHELD"
_CPD_HOLD = "HOLD_CURRENT_BOOK"
_CPD_REVIEW = "PROPOSAL_REVIEW_REQUIRED"
_CPD_RECORDED = "DECISION_RECORDED"
_CPD_BLOCKED = "BLOCKED"
_PDS_APPROVED = "PROPOSAL_APPROVED"
_PDS_REJECTED = "PROPOSAL_REJECTED"
_PDS_HELD = "PROPOSAL_HELD"
_PDS_STALE = "STALE_PROPOSAL_REVIEW_REQUIRED"
_PDS_HOLD = "HOLD_CURRENT_BOOK"
_RB_PLAN_REVIEW_REQUIRED = "PROPOSAL_APPROVED_ORDER_PLAN_REVIEW_REQUIRED"
_RB_PLAN_CONFIRMED = "ORDER_PLAN_CONFIRMED_PAPER_EXECUTION_PENDING"
_RB_EXECUTED = "PAPER_EXECUTED_RECONCILED"
_RB_BLOCKED = frozenset({"ORDER_PLAN_BLOCKED_MISSING_OWNED_MARKS",
                         "ORDER_PLAN_BLOCKED_INCOMPLETE_TARGET"})
_RO_PROPOSAL_READY = "PROPOSAL_READY"
_RO_HOLD = "HOLD_CURRENT_BOOK"
_RO_TRUE_BLOCKER = "TRUE_BLOCKER"
_WF_INCONSISTENT = "INCONSISTENT_STATE"
_WF_WAITING_CLOSE = "WAITING_FOR_SESSION_CLOSE"
_WF_CYCLE_RUNNING = "RESEARCH_CYCLE_RUNNING"
_WF_CYCLE_BLOCKED = "RESEARCH_CYCLE_BLOCKED"
_COLLECTION_RUNNING = "RUNNING"

SOURCE_OWNERS = {
    "workflow_state": "api.workflow_state",
    "constrained_reallocation": "api.reallocation_proposal",
    "daily_close": "api.daily_close",
    "material_information": "api.material_information",
    "decision_outcomes": "api.portfolio_decision_outcome",
    "information_collection": "api.information_collection",
    # Release 50 - the ONE capital pool (allocation by asset class, verbatim).
    "capital_pool": "api.capital_pool",
}


def _now_iso(now: Optional[datetime] = None) -> str:
    return (now or datetime.now(timezone.utc)).isoformat()


def _d(x: Any) -> dict:
    return x if isinstance(x, dict) else {}


def _l(x: Any) -> list:
    return list(x) if isinstance(x, (list, tuple)) else []


def _num(x: Any) -> Optional[float]:
    try:
        if x is None or isinstance(x, bool):
            return None
        v = float(x)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def _int(x: Any) -> int:
    v = _num(x)
    return int(v) if v is not None else 0


def _count_actions(action_counts: dict, allocations: list) -> dict:
    """The owner's own action tally when it published one; otherwise the plain
    count of the owner's allocation rows by their published action label. A count
    of published rows is presentation, not a portfolio decision."""
    counts = {k: _int(v) for k, v in _d(action_counts).items()}
    if not counts and allocations:
        for row in allocations:
            act = str(_d(row).get("action") or "")
            if act:
                counts[act] = counts.get(act, 0) + 1
    return counts


# --------------------------------------------------------------------------- #
# System readiness — READY / DEGRADED / BLOCKED, with every degraded item saying
# whether it blocks the portfolio decision (the owners decide; this reads).
# --------------------------------------------------------------------------- #
def _system_readiness(wf: dict, collection: Optional[dict]) -> dict:
    os_ = _d(wf.get("operational_state"))
    ev = _d(wf.get("evidence_state"))
    evc = _d(wf.get("evidence_classification"))
    gaps = _d(wf.get("data_gap_taxonomy"))
    mrev = _d(wf.get("model_review"))
    overall = wf.get("overall_state")
    blockers = _l(wf.get("blockers"))
    items: list[dict] = []
    blocking: list[str] = []
    degraded: list[str] = []

    def item(key, label, value, state, detail=None, blocks=False):
        items.append({"key": key, "label": label, "value": value, "state": state,
                      "detail": detail, "blocks_portfolio_decision": bool(blocks)})

    workflow_available = bool(wf.get("status") == "OK" and overall)
    item("service", "Service", "ready" if workflow_available else "unavailable",
         "ok" if workflow_available else "blocked",
         None if workflow_available else "The canonical workflow state did not load.",
         blocks=not workflow_available)
    if not workflow_available:
        blocking.append("workflow state unavailable")

    item("eligible_session", "Eligible session",
         os_.get("eligible_market_date") or wf.get("eligible_market_date"), "ok")
    close_valid = bool(os_.get("operational_close_valid"))
    item("operational_mark", "Operational mark", os_.get("valuation_date"),
         "ok" if close_valid else "degraded",
         None if close_valid else "The latest operational close is not marked valid.")
    if not close_valid and workflow_available:
        degraded.append("operational close not valid")
    item("nav", "NAV", _num(os_.get("nav")), "ok")

    if overall == _WF_INCONSISTENT:
        blocking.append("authoritative surfaces disagree")
    for b in blockers:
        blocking.append(str(b))
    if bool(gaps.get("has_blocking_gap")):
        blocking.append("blocking data gap: %s"
                        % ", ".join(str(t) for t in _l(gaps.get("affected_tickers"))))
    if bool(evc.get("is_operational_incident")) or bool(evc.get("blocks_portfolio_action")):
        blocking.append("operational evidence incident")

    # Collection — decided by api.information_collection; presented as DEGRADED and
    # explicitly NON-BLOCKING unless the workflow owner itself named it a blocker.
    if collection is not None:
        svc = _d(collection.get("service"))
        rec = _d(collection.get("recovery"))
        hd = _d(collection.get("headline"))
        svc_state = svc.get("service_state")
        running = svc_state == _COLLECTION_RUNNING
        item("collection", "Collection",
             "running" if running else str(svc_state or "unknown").lower(),
             "ok" if running else "degraded",
             None if running else (
                 (rec.get("why") or hd.get("detail") or "Collection is not running.")
                 + " The portfolio decision is unaffected; recovery is an explicit "
                   "operator action under System · Audit."),
             blocks=False)
        if not running:
            degraded.append("collection %s" % str(svc_state or "unknown").lower())
    else:
        item("collection", "Collection", "not checked", "degraded",
             "The collection state did not load.", blocks=False)
        degraded.append("collection state unavailable")

    # Research / evidence health — a documented gap is attention, never a blocker.
    gap = bool(ev.get("documented_gap"))
    review_due = bool(mrev.get("model_review_required"))
    if gap:
        r_state, r_value, r_detail = "degraded", "evidence gap", ev.get("recovery_classification")
        degraded.append("forward-evidence gap documented")
    elif review_due:
        r_state, r_value, r_detail = "degraded", "model review due", \
            "A model-recalibration review threshold has been met (review only)."
        degraded.append("model review due")
    else:
        r_state, r_value, r_detail = "ok", "healthy", None
    item("research", "Research", r_value, r_state, r_detail, blocks=False)

    if blocking:
        state = SYSTEM_BLOCKED
    elif degraded:
        state = SYSTEM_DEGRADED
    else:
        state = SYSTEM_READY
    if state == SYSTEM_BLOCKED:
        summary = "Blocked — " + "; ".join(blocking[:3])
    elif state == SYSTEM_DEGRADED:
        summary = ("Degraded — " + "; ".join(degraded[:3])
                   + ". The portfolio decision remains valid.")
    else:
        summary = "Ready"
    return {
        "state": state,
        "state_vocabulary": list(SYSTEM_READINESS_VOCABULARY),
        "summary": summary,
        "items": items,
        "blocking_reasons": blocking,
        "degraded_reasons": degraded,
        "degraded_blocks_portfolio_decision": False if not blocking else True,
        "portfolio_decision_remains_valid": not blocking,
    }


# --------------------------------------------------------------------------- #
# Historical / pre-R47 reconciliation.
# --------------------------------------------------------------------------- #
def _historical_context(wf: dict, constrained: dict) -> dict:
    cpd = _d(wf.get("canonical_portfolio_decision"))
    rpp = _d(wf.get("reallocation_proposal_presentation"))
    inv = _d(constrained.get("constraint_inventory"))
    session = cpd.get("eligible_market_date") or wf.get("eligible_market_date") \
        or _d(wf.get("operational_state")).get("eligible_market_date")
    blockers = [str(b) for b in _l(cpd.get("withheld_reasons"))]
    true_blocker_codes = {str(c) for c in _l(inv.get("true_blocker_codes"))}
    codes = {b.split(":")[-1] for b in blockers}
    governed_cycle_complete = bool(rpp.get("governed_cycle_complete_for_session"))
    gate_withheld = bool(rpp.get("economic_gate_withheld_the_proposal"))
    feasible = bool(cpd.get("feasible_target_exists")) or bool(
        constrained.get("feasible_target_exists"))
    outcome = constrained.get("outcome")
    no_governed_target = (outcome is None) and not feasible
    # The prior workflow recorded a per-holding CAP breach as a manual-review
    # blocker. Under the current workflow (Release 47 §7b) such a code is a
    # reshaping constraint, never a true blocker. A blocked decision whose every
    # blocker is outside the owner's OWN true-blocker vocabulary, on a session the
    # governed cycle completed without a target, is therefore a historical artifact.
    blockers_are_prior_workflow = bool(codes) and bool(true_blocker_codes) \
        and not (codes & true_blocker_codes)
    historical = (cpd.get("state") == _CPD_BLOCKED and governed_cycle_complete
                  and gate_withheld and no_governed_target
                  and blockers_are_prior_workflow)
    tickers = sorted({b.split(":")[0] for b in blockers if ":" in b})
    if historical:
        explanation = (
            "The %s session was completed under the prior decision workflow. A "
            "portfolio constraint breach was recorded (%d holding%s: %s). No governed "
            "reallocation target exists for that historical session, and the "
            "historical record will not be rewritten."
            % (session or "latest completed", len(tickers), "" if len(tickers) == 1 else "s",
               ", ".join(tickers) or "none named"))
    else:
        explanation = None
    return {
        "historical": bool(historical),
        "session_date": session,
        "recorded_under": ("PRIOR_DECISION_WORKFLOW" if historical
                           else "CURRENT_GOVERNED_WORKFLOW"),
        "explanation": explanation,
        "governed_target_exists": bool(feasible),
        "governed_cycle_complete_for_session": governed_cycle_complete,
        "breach_tickers": tickers,
        "blocker_codes": sorted(codes),
        "blockers_outside_true_blocker_vocabulary": blockers_are_prior_workflow,
        "next_eligible_action": (
            "Run the portfolio cycle after the next eligible market close."),
        "history_rewritten": False,
        "proposal_fabricated": False,
        "rerun_of_historical_session_instructed": False,
    }


# --------------------------------------------------------------------------- #
# The portfolio decision — ONE state, ONE headline, ONE explanation, ONE action.
# --------------------------------------------------------------------------- #
def _next_action(kind: str, *, available: bool, label: Optional[str] = None,
                 destination: Optional[str] = None, execution_contract=None,
                 confirmation: Optional[str] = None, owner: Optional[str] = None,
                 note: Optional[str] = None) -> dict:
    executes = kind in EXECUTING_NEXT_ACTION_KINDS and bool(available)
    return {
        "kind": kind,
        "kind_vocabulary": list(NEXT_ACTION_VOCABULARY),
        "label": label if label is not None else NEXT_ACTION_LABELS.get(kind),
        "available": bool(available),
        "executes": executes,
        "navigates": bool(available) and not executes and kind not in (NA_WAIT, NA_NONE),
        "destination": destination,
        "execution_contract": (dict(execution_contract)
                               if executes and isinstance(execution_contract, dict)
                               else None),
        "confirmation_required": confirmation if executes else None,
        "owner": owner,
        "note": note,
        "creates_orders": False,
        "approves_anything": False,
    }


def _governance(decision: str) -> dict:
    current = {
        PD_REALLOCATE: GOV_REVIEW,
        PD_AWAITING_APPROVAL: GOV_APPROVE,
        PD_AWAITING_CONFIRMATION: GOV_CONFIRM,
        PD_AWAITING_NEXT_CLOSE: GOV_AWAIT_NEXT_CLOSE,
        PD_OUTCOME_ACCRUING: GOV_OUTCOME_ACCRUING,
    }.get(decision)
    steps = []
    passed = current is not None
    for s in GOVERNANCE_SEQUENCE:
        if current is None:
            status = STEP_UPCOMING
        elif s == current:
            status = STEP_CURRENT
            passed = False
        elif passed:
            status = STEP_DONE
        else:
            status = STEP_UPCOMING
        steps.append({"step": s, "label": GOVERNANCE_LABELS[s], "status": status,
                      "owner": GOVERNANCE_OWNERS[s], "manual": s in (GOV_REVIEW, GOV_APPROVE, GOV_CONFIRM)})
    # OUTCOME_ACCRUING implies EXECUTED is done.
    if current == GOV_OUTCOME_ACCRUING:
        for st in steps:
            if st["step"] == GOV_EXECUTED:
                st["status"] = STEP_DONE
    return {
        "sequence": list(GOVERNANCE_SEQUENCE),
        "current_step": current,
        "steps": steps,
        "manual_gates": [GOV_APPROVE, GOV_CONFIRM],
        "note": ("Two independent manual gates, both backend-enforced. Nothing on "
                 "the presentation surface approves, confirms, executes or creates "
                 "an order."),
    }


def _portfolio_decision(wf: dict, constrained: dict, outcomes: dict,
                        historical: dict) -> dict:
    cmd = _d(wf.get("operator_command"))
    pa = _d(wf.get("primary_action"))
    cpd = _d(wf.get("canonical_portfolio_decision"))
    lane = _d(wf.get("portfolio_decision_state"))
    prs = _d(wf.get("portfolio_reassessment"))
    exec_prec = _d(wf.get("portfolio_reassessment_execution_precedence"))
    os_ = _d(wf.get("operational_state"))
    execution = _d(constrained.get("execution"))
    sw = _d(constrained.get("switching_economics"))
    overall = wf.get("overall_state")
    cpd_state = cpd.get("state")
    pd_state = lane.get("portfolio_decision_state")
    rb_state = execution.get("rebalance_state") or exec_prec.get("rebalance_state")
    outcome = constrained.get("outcome")
    pending = _int(os_.get("pending_orders")) or _int(exec_prec.get("pending_orders"))
    session = cpd.get("eligible_market_date") or os_.get("eligible_market_date")
    counts = _count_actions(_d(_d(lane.get("materiality")).get("action_counts")),
                            _l(_d(constrained.get("best_feasible_target")).get("allocations")))
    changing = sum(v for k, v in counts.items() if k != "RETAIN")

    eyebrow = None
    tone = "info"
    blocked_detail = None
    reason_codes: list = []

    # 1. An execution in flight outranks every decision surface (Stage 19 precedence).
    if rb_state == _RB_PLAN_CONFIRMED or (bool(exec_prec.get("execution_active")) and pending > 0):
        state = PD_AWAITING_NEXT_CLOSE
        headline = "AWAITING NEXT CLOSE" + (" — %d PAPER ORDER%s WORKING" % (pending, "" if pending == 1 else "S")
                                             if pending else "")
        explanation = ("The approved order plan is confirmed. Paper orders fill at the first "
                       "completed owned close strictly after approval (NEXT_CLOSE — no "
                       "same-close hindsight fill); the next Daily Close settles them.")
        action = _next_action(NA_WAIT, available=False,
                              label="Wait for the next completed close")
        tone = "info"
    elif rb_state == _RB_EXECUTED:
        state = PD_OUTCOME_ACCRUING
        headline = "EXECUTED — OUTCOME ACCRUING"
        explanation = ("The governed rebalance executed and reconciled. Its forward "
                       "evidence against the frozen hold counterfactual accrues at each "
                       "completed close; nothing further is required.")
        action = _next_action(NA_NONE, available=False)
        tone = "ok"
    elif rb_state in _RB_BLOCKED:
        state = PD_BLOCKED
        headline = "BLOCKED — ORDER PLAN CANNOT BE BUILT"
        explanation = execution.get("message") or "The approved target cannot be faithfully executed."
        reason_codes = [str(r) for r in _l(execution.get("blocked_reasons"))]
        blocked_detail = {
            "why": explanation,
            "cannot_trust": "The order plan for the approved target; no paper order may be created from it.",
            "resolves": ("Restore the missing owned marks / complete the target, then review "
                         "the order plan again. Nothing is executed automatically."),
        }
        action = _next_action(NA_REVIEW_ORDER_PLAN, available=True,
                              destination="portfolio-manager/reallocation")
        tone = "bad"
    elif rb_state == _RB_PLAN_REVIEW_REQUIRED or pd_state == _PDS_APPROVED:
        state = PD_AWAITING_CONFIRMATION
        headline = "APPROVED — REVIEW ORDER PLAN"
        explanation = ("The reallocation is approved (manual gate 1). Its deterministic "
                       "order plan awaits the second manual confirmation before any paper "
                       "order is created.")
        action = _next_action(NA_REVIEW_ORDER_PLAN, available=True,
                              destination="portfolio-manager/reallocation")
        tone = "warn"
    # 2. A normal-path mutation is due: the ONE Release-48 concept, verbatim.
    elif bool(cmd.get("primary_action_available")) and cmd.get("primary_action_kind") == NA_PORTFOLIO_CYCLE:
        state = PD_CYCLE_REQUIRED
        headline = "RUN THE PORTFOLIO CYCLE"
        explanation = cmd.get("supporting_text") or cmd.get("why") or pa.get("explanation") \
            or "The eligible session is ready to be processed."
        action = _next_action(
            NA_PORTFOLIO_CYCLE, available=True,
            label=cmd.get("primary_action_label") or NEXT_ACTION_LABELS[NA_PORTFOLIO_CYCLE],
            execution_contract=cmd.get("primary_action_execution_contract"),
            confirmation=cmd.get("confirmation_required"),
            owner=cmd.get("primary_action_owner"),
            note="Executes through the canonical dispatcher; stops at the governed portfolio decision.")
        tone = "info"
    elif overall == _WF_CYCLE_RUNNING:
        state = PD_CYCLE_REQUIRED
        headline = "PORTFOLIO CYCLE RUNNING"
        explanation = pa.get("explanation") or "A Daily Research Cycle run is in progress."
        action = _next_action(NA_WAIT, available=False, label="Wait for the running cycle")
        tone = "info"
    elif overall in (_WF_CYCLE_BLOCKED, _WF_INCONSISTENT):
        state = PD_BLOCKED
        headline = "BLOCKED"
        explanation = pa.get("explanation") or cmd.get("why") or "Recovery is required."
        reason_codes = [str(b) for b in _l(wf.get("blockers"))] or \
            [str(c) for c in _l(_d(wf.get("evidence_classification")).get("blocker_codes"))]
        blocked_detail = {
            "why": explanation,
            "cannot_trust": ("The daily cycle cannot advance, so no portfolio decision for "
                             "this session can be trusted."),
            "resolves": pa.get("label") or "Resolve the named blocker.",
        }
        action = _next_action(NA_REVIEW_BLOCKER, available=True,
                              destination="system-audit/diagnostics")
        tone = "bad"
    # 3. The governed decision, read from its owners in their own precedence.
    #    Track B (decision consistency): the constrained owner's HOLD_CURRENT_BOOK
    #    outcome is read FIRST. It is the highest decision authority in this region
    #    (constrained outcome -> decision lane -> composed workflow object), and a
    #    review state any downstream surface reconstructed from "a proposal exists"
    #    must never outrank it — on 2026-08-31 exactly that reconstruction presented
    #    a governed economic HOLD as "REALLOCATE — 27 POSITIONS CHANGE".
    elif cpd_state == _CPD_HOLD or outcome == _RO_HOLD or pd_state == _PDS_HOLD:
        state = PD_HOLD
        headline = "HOLD CURRENT PORTFOLIO"
        explanation = cpd.get("no_proposal_reason") or (
            "A feasible alternative exists, but the expected improvement after "
            "transaction costs and turnover does not clear the switching hurdle.")
        action = _next_action(NA_NONE, available=False)
        tone = "ok"
    elif cpd_state == _CPD_REVIEW or (outcome == _RO_PROPOSAL_READY and bool(lane.get("requires_manual_review"))):
        state = PD_REALLOCATE
        headline = "REALLOCATE" + (" — %d POSITION%s CHANGE" % (changing, "" if changing == 1 else "S")
                                   if changing else "")
        explanation = cpd.get("no_proposal_reason") or cpd.get("explanation") or (
            "A feasible target exists and its expected improvement clears the "
            "switching hurdle after cost, risk, liquidity and turnover.")
        action = _next_action(NA_REVIEW_REALLOCATION, available=True,
                              destination="portfolio-manager/reallocation")
        tone = "warn"
    elif pd_state in (_PDS_HELD, _PDS_STALE):
        state = PD_AWAITING_APPROVAL
        headline = ("PROPOSAL HELD — DECIDE TO PROCEED" if pd_state == _PDS_HELD
                    else "PROPOSAL CHANGED — RE-REVIEW REQUIRED")
        explanation = cpd.get("explanation") or (
            "The proposal awaits the manual approval decision.")
        action = _next_action(NA_REVIEW_REALLOCATION, available=True,
                              destination="portfolio-manager/reallocation")
        tone = "warn"
    elif cpd_state in (_CPD_NO_CHANGE, _CPD_WITHHELD) or (cpd_state == _CPD_RECORDED and pd_state == _PDS_REJECTED):
        state = PD_HOLD
        headline = "HOLD CURRENT PORTFOLIO"
        if cpd_state == _CPD_WITHHELD:
            explanation = cpd.get("no_proposal_reason") or (
                "Deterioration was found, but no change cleared the economic hurdle.")
        elif cpd_state == _CPD_RECORDED:
            explanation = "The proposal was rejected on manual review; the current book stands."
        else:
            explanation = cpd.get("no_proposal_reason") or (
                "The reassessment found the current holdings remain the best available use of capital.")
        action = _next_action(NA_NONE, available=False)
        tone = "ok"
    elif cpd_state == _CPD_BLOCKED or outcome == _RO_TRUE_BLOCKER:
        state = PD_BLOCKED
        reason_codes = [str(r) for r in _l(cpd.get("withheld_reasons"))] or \
            [str(r) for r in _l(_d(constrained.get("reallocation_outcome")).get("reason_codes"))]
        if historical.get("historical"):
            eyebrow = "HISTORICAL DECISION"
            headline = "HISTORICAL DECISION — %s" % (historical.get("session_date") or session or "")
            explanation = historical.get("explanation")
            blocked_detail = {
                "why": explanation,
                "cannot_trust": ("The recorded breach list is a per-holding review signal from "
                                 "the prior workflow; it is not an executable target, and no "
                                 "target was solved for this session."),
                "resolves": historical.get("next_eligible_action"),
            }
            action = _next_action(NA_WAIT, available=False,
                                  label=historical.get("next_eligible_action"),
                                  note="A historical session is never rerun.")
            tone = "warn"
        else:
            headline = "BLOCKED"
            explanation = cpd.get("explanation") or cpd.get("no_proposal_reason") or (
                "No trustworthy portfolio decision can be made.")
            blocked_detail = {
                "why": explanation,
                "cannot_trust": ("The portfolio decision for %s; no target can be reviewed or "
                                 "approved from it." % (session or "this session")),
                "resolves": ("Resolve the named blocker; the next portfolio cycle re-solves the "
                             "target under the constraint-respecting workflow."),
            }
            action = _next_action(NA_REVIEW_BLOCKER, available=True,
                                  destination="portfolio-manager/overview")
            tone = "bad"
    elif cpd_state == _CPD_NOT_RUN or cpd_state is None:
        state = PD_CYCLE_REQUIRED
        if overall == _WF_WAITING_CLOSE:
            headline = "WAITING FOR THE SESSION TO CLOSE"
            explanation = pa.get("explanation") or (
                "The latest eligible session is fully processed; the current session is still open.")
            action = _next_action(NA_WAIT, available=False,
                                  label="Run the portfolio cycle after the session closes")
        else:
            headline = "NO PORTFOLIO DECISION YET"
            explanation = cmd.get("why") or pa.get("explanation") or (
                "No governed portfolio decision exists for the eligible session.")
            action = _next_action(NA_WAIT, available=False,
                                  label=cmd.get("next_text") or "Wait for the portfolio cycle")
        tone = "info"
    else:
        # Fail closed: an unrecognised owner state is never presented as HOLD.
        state = PD_BLOCKED
        headline = "BLOCKED — STATE NOT RECOGNISED"
        explanation = ("The canonical portfolio decision reported a state this presentation "
                       "does not recognise; see the raw states under Audit.")
        blocked_detail = {"why": explanation,
                          "cannot_trust": "The reconciled presentation.",
                          "resolves": "Inspect the raw owner states under System · Audit."}
        action = _next_action(NA_REVIEW_BLOCKER, available=True,
                              destination="system-audit/diagnostics")
        tone = "bad"

    return {
        "state": state,
        "state_vocabulary": list(PORTFOLIO_DECISION_VOCABULARY),
        "state_label": PORTFOLIO_DECISION_LABELS[state],
        "eyebrow": eyebrow,
        "headline": headline,
        "explanation": explanation,
        "tone": tone,
        "eligible_market_date": session,
        "next_action": action,
        "blocked_detail": blocked_detail,
        "reason_codes": reason_codes,
        "positions_changing": changing,
        "governance": _governance(state),
        "manual_review_only": True,
        "creates_orders": False,
        "automation_off": True,
        "owners": {
            "overall_state": "api.workflow_state",
            "operator_command": "api.workflow_state.build_operator_command",
            "canonical_portfolio_decision": "api.workflow_state.build_canonical_portfolio_decision",
            "portfolio_decision_lane": "api.portfolio_decision",
            "reallocation_outcome": "engine.constrained_reallocation via api.reallocation_proposal",
            "execution_state": "api.rebalance_execution",
            "decision_outcomes": "api.portfolio_decision_outcome",
        },
    }


# --------------------------------------------------------------------------- #
# Snapshot, economics, attention, outcome — every number read verbatim.
# --------------------------------------------------------------------------- #
def _portfolio_snapshot(wf: dict, daily_close: dict, capital_pool: Optional[dict] = None) -> dict:
    os_ = _d(wf.get("operational_state"))
    pnl = _d(daily_close.get("pnl"))
    cpool = _d(capital_pool)
    # Release 50 - the allocation by asset class is the capital-pool owner's, read
    # verbatim; only asset classes that carry weight are present (never "FX 0%").
    allocation = [{"asset_class": k, "label": _d(cpool.get("allocation_labels")).get(k, k),
                   "weight": _num(v)}
                  for k, v in _d(cpool.get("allocation")).items() if _num(v) is not None]
    return {
        "allocation": allocation,
        "allocation_available": bool(allocation),
        "asset_classes_present": [a["asset_class"] for a in allocation],
        "collateral": _num(cpool.get("collateral")),
        "available_capital": _num(cpool.get("available_capital")),
        "gross_exposure": _num(cpool.get("gross_exposure")),
        "non_equity_positions": _int(cpool.get("non_equity_position_count")),
        "capital_pool_owner": cpool.get("owner") or SOURCE_OWNERS["capital_pool"],
        "book_id": os_.get("active_book_id"),
        "book_label": os_.get("active_book_name"),
        "book_status": os_.get("book_status"),
        "valuation_date": os_.get("valuation_date") or pnl.get("valuation_date"),
        "nav": _num(os_.get("nav")) if _num(os_.get("nav")) is not None else _num(pnl.get("nav")),
        "cash": _num(os_.get("cash")) if _num(os_.get("cash")) is not None else _num(pnl.get("cash")),
        "invested_value": _num(pnl.get("invested_value")),
        "positions": _int(os_.get("holdings_count")) if os_.get("holdings_count") is not None else None,
        "pending_orders": _int(os_.get("pending_orders")),
        "daily_pnl": _num(pnl.get("daily_pnl")),
        "daily_return_pct": _num(pnl.get("daily_return_pct")),
        "daily_pnl_available": bool(pnl.get("daily_pnl_available", pnl.get("daily_pnl") is not None)),
        "daily_pnl_note": pnl.get("daily_pnl_note"),
        "cumulative_pnl": _num(pnl.get("cumulative_pnl")),
        "cumulative_return_pct": _num(pnl.get("cumulative_return_pct")),
        "benchmark_cumulative_return_pct": _num(pnl.get("spy_cumulative_return_pct")),
        "excess_return_pct": _num(pnl.get("excess_return_pct")),
        "drawdown_pct": _num(pnl.get("drawdown_pct")),
        "starting_capital": _num(pnl.get("starting_capital")),
        "basis": pnl.get("basis"),
        "basis_label": pnl.get("label"),
        "benchmark": "SPY",
        "owners": {"nav_cash_positions": "api.operational_book via api.workflow_state",
                   "pnl": "api.daily_close"},
    }


def _decision_summary(wf: dict, constrained: dict) -> dict:
    cpd = _d(wf.get("canonical_portfolio_decision"))
    lane = _d(wf.get("portfolio_decision_state"))
    best = _d(constrained.get("best_feasible_target"))
    sw = _d(constrained.get("switching_economics"))
    trn = _d(constrained.get("turnover"))
    allocations = _l(best.get("allocations"))
    counts = _count_actions(_d(_d(lane.get("materiality")).get("action_counts")), allocations)
    feasible = bool(cpd.get("feasible_target_exists")) or bool(constrained.get("feasible_target_exists"))
    net = _num(sw.get("score_improvement_net_of_cost"))
    if net is None:
        net = _num(cpd.get("expected_net_improvement"))
    hurdle = _num(sw.get("switching_hurdle"))
    if hurdle is None:
        hurdle = _num(cpd.get("net_improvement_hurdle"))
    turnover = _num(sw.get("one_way_turnover"))
    if turnover is None:
        turnover = _num(trn.get("one_way_turnover"))
    if turnover is None:
        turnover = _num(cpd.get("expected_one_way_turnover"))
    cost = _num(sw.get("estimated_transaction_cost"))
    if cost is None:
        cost = _num(trn.get("estimated_transaction_cost"))
    if cost is None:
        cost = _num(cpd.get("expected_transaction_cost_usd"))
    replacements = counts.get("REPLACE_OUT", 0) or counts.get("REPLACE", 0)
    return {
        "available": feasible,
        "feasible_target_exists": feasible,
        "outcome": constrained.get("outcome"),
        "outcome_vocabulary": list(_l(constrained.get("outcome_vocabulary"))),
        "exits": counts.get("EXIT", 0),
        "reductions": counts.get("REDUCE", 0),
        "replacements": replacements,
        "additions": counts.get("ADD", 0),
        "increases": counts.get("INCREASE", 0),
        "retained": counts.get("RETAIN", 0),
        "positions_changing": sum(v for k, v in counts.items() if k != "RETAIN"),
        "action_counts": counts,
        "turnover": turnover,
        "turnover_budget": _num(cpd.get("turnover_budget")),
        "estimated_cost": cost,
        "net_improvement": net,
        "switching_hurdle": hurdle,
        "clears_switching_hurdle": (sw.get("clears_switching_hurdle")
                                    if sw.get("clears_switching_hurdle") is not None
                                    else cpd.get("clears_switching_hurdle")),
        "score_before": _num(sw.get("score_before")),
        "score_after": _num(sw.get("score_after")),
        "risk_before": _num(sw.get("portfolio_volatility_before")),
        "risk_after": _num(sw.get("portfolio_volatility_after")),
        "concentration_before": _num(sw.get("concentration_before")),
        "concentration_after": _num(sw.get("concentration_after")),
        "cash_before": _num(sw.get("cash_weight_before")),
        "cash_after": _num(sw.get("cash_weight_after")),
        "positions_before": sw.get("position_count_before"),
        "positions_after": sw.get("position_count_after"),
        "target_position_count": best.get("position_count"),
        "target_cash_weight": _num(best.get("cash_weight")),
        "constraints_satisfied": _d(best.get("constraints")).get("all_ok"),
        "constraints_that_reshaped": [str(c) for c in _l(cpd.get("constraints_that_reshaped"))
                                      or _l(constrained.get("constraints_that_reshaped"))],
        "constraint_reoptimized": bool(cpd.get("constraint_reoptimized")
                                       or constrained.get("constraint_reoptimization_applied")),
        "mandatory_exits": [str(t) for t in _l(sw.get("mandatory_exits"))
                            or _l(cpd.get("mandatory_exit_tickers"))],
        "expected_return_state": sw.get("expected_return_state") or "NOT_CALIBRATED",
        "improvement_units": "combined percentile score points (never a dollar forecast)",
        "owners": {"counts": "api.portfolio_decision (materiality) / api.reallocation_proposal (allocations)",
                   "economics": "engine.constrained_reallocation via api.reallocation_proposal",
                   "hurdle_and_budget": "api.workflow_state.canonical_portfolio_decision"},
    }


_RELEVANCE = {
    "OPERATIONAL_ALPHA": "operational signal input",
    "OPERATIONAL_RISK": "operational risk input",
    "EVENT_TRIGGER_ONLY": "reassessment trigger only",
    "RESEARCH_ALPHA": "research evidence only",
    "OBSERVABILITY_ONLY": "observability only",
    "BLOCKED": "blocked source",
}


def _alerts_summary(mi: Optional[dict], top_n: int = 3) -> dict:
    mi = _d(mi)
    rows = [_d(r) for r in _l(mi.get("rows"))]

    def rank(r: dict):
        return (0 if r.get("held") else 1,
                0 if r.get("hoc_affected") else 1,
                0 if r.get("risk_affected") else 1)

    ordered = sorted(rows, key=rank)   # stable: the owner's recency order survives
    items = []
    for r in ordered[:top_n]:
        held = bool(r.get("held"))
        rec = r.get("hoc_recommendation")
        auth = str(r.get("signal_authority") or "")
        if held and rec:
            relevance = "Held · review signal %s" % rec
        elif held:
            relevance = "Held · no signal change"
        elif r.get("ticker"):
            relevance = "Not held · " + _RELEVANCE.get(auth, "event")
        else:
            relevance = "Market-wide · " + _RELEVANCE.get(auth, "event")
        desc = str(r.get("what_changed") or r.get("source_title") or r.get("event_type") or "")
        items.append({
            "event_id": r.get("event_id"),
            "ticker": r.get("ticker"),
            "held": held,
            "description": desc[:120],
            "relevance": relevance,
            "hoc_recommendation": rec,
            "event_type": r.get("event_type"),
            "signal_authority": auth or None,     # raw, for audit — never rendered as prose
            "timestamp": r.get("timestamp"),
            "source_url": (r.get("source_url")
                           if r.get("source_url_state") == "CANONICAL_SOURCE_URL" else None),
        })
    return {
        "available": bool(mi) and mi.get("state") in ("READY", "NO_MATERIAL_INFORMATION"),
        "state": mi.get("state"),
        "count": _int(mi.get("total_material_events")),
        "portfolio_relevant_count": _int(mi.get("material_events_affecting_holdings")),
        "affected_holdings": [str(t) for t in _l(mi.get("affected_holdings"))],
        "rows_available": len(rows),
        "top_items": items,
        "top_n": top_n,
        "detail_location": "system-audit/diagnostics",
        "owner": mi.get("composition_owner") or SOURCE_OWNERS["material_information"],
        "authority_owner": mi.get("authority_policy_owner"),
    }


def _decision_outcome(outcomes: Optional[dict]) -> dict:
    o = _d(outcomes)
    measured = _int(o.get("measured_count"))
    return {
        "available": measured > 0,
        "decision_count": _int(o.get("decision_count")),
        "measured_count": measured,
        "pending_count": _int(o.get("pending_count")),
        "cumulative_incremental_pnl": _num(o.get("cumulative_incremental_pnl")),
        "cumulative_transaction_cost": _num(o.get("cumulative_transaction_cost")),
        "verdict_counts": _d(o.get("verdict_counts")),
        "improvement_basis": o.get("improvement_basis"),
        "owner": o.get("owner") or SOURCE_OWNERS["decision_outcomes"],
    }


def _research_health(wf: dict) -> dict:
    ev = _d(wf.get("evidence_state"))
    mrev = _d(wf.get("model_review"))
    gap = bool(ev.get("documented_gap"))
    return {
        "evidence_status": ev.get("evidence_status"),
        "documented_gap": gap,
        "gap_severity": ev.get("gap_severity"),
        "model_review_state": mrev.get("model_review_state"),
        "model_review_required": bool(mrev.get("model_review_required")),
        "label": ("Evidence gap" if gap else
                  ("Model review due" if mrev.get("model_review_required") else "Healthy")),
        "destination": "research",
    }


# --------------------------------------------------------------------------- #
# The builder (pure) and the loader (composition, parallel, degrade-safe).
# --------------------------------------------------------------------------- #
def build_operator_presentation(*, workflow: Optional[dict],
                                constrained: Optional[dict] = None,
                                daily_close: Optional[dict] = None,
                                material_information: Optional[dict] = None,
                                decision_outcomes: Optional[dict] = None,
                                information_collection: Optional[dict] = None,
                                warnings: Optional[list] = None,
                                now: Optional[datetime] = None,
                                capital_pool: Optional[dict] = None) -> dict:
    """Reconcile the authoritative owner payloads into ONE operator presentation.

    Pure: no I/O, no owner call, no recomputation. A missing owner degrades to an
    honest gap (``sources[<name>].available = False``) and never to a fabricated value.
    """
    wf = _d(workflow)
    cn = _d(constrained)
    dc = _d(daily_close)
    warns = list(warnings or [])
    historical = _historical_context(wf, cn)
    decision = _portfolio_decision(wf, cn, _d(decision_outcomes), historical)
    system = _system_readiness(wf, information_collection if isinstance(information_collection, dict) else None)
    snapshot = _portfolio_snapshot(wf, dc, capital_pool)
    summary = _decision_summary(wf, cn)
    alerts = _alerts_summary(material_information)
    outcome = _decision_outcome(decision_outcomes)
    cmd = _d(wf.get("operator_command"))
    os_ = _d(wf.get("operational_state"))
    raw_states = {
        "overall_state": wf.get("overall_state"),
        "operator_command_state": cmd.get("state"),
        "canonical_portfolio_decision_state": _d(wf.get("canonical_portfolio_decision")).get("state"),
        "reassessment_state": _d(wf.get("portfolio_reassessment")).get("state"),
        "proposal_state": _d(wf.get("reallocation_proposal_presentation")).get("state"),
        "reallocation_operator_state": wf.get("reallocation_operator_state"),
        "portfolio_decision_state": _d(wf.get("portfolio_decision_state")).get("portfolio_decision_state"),
        "reallocation_outcome": cn.get("outcome"),
        "constrained_state": cn.get("state"),
        "rebalance_state": _d(cn.get("execution")).get("rebalance_state"),
        "close_status": os_.get("latest_close_status"),
        "collection_service_state": _d(_d(information_collection).get("service")).get("service_state"),
        "note": "Raw owner states, for Audit / Advanced only. Normal surfaces render the reconciled fields.",
    }
    sources = {
        "workflow_state": {"available": bool(wf), "owner": SOURCE_OWNERS["workflow_state"]},
        "constrained_reallocation": {"available": bool(cn), "owner": SOURCE_OWNERS["constrained_reallocation"]},
        "daily_close": {"available": bool(dc), "owner": SOURCE_OWNERS["daily_close"]},
        "material_information": {"available": bool(material_information), "owner": SOURCE_OWNERS["material_information"]},
        "decision_outcomes": {"available": bool(decision_outcomes), "owner": SOURCE_OWNERS["decision_outcomes"]},
        "information_collection": {"available": bool(information_collection), "owner": SOURCE_OWNERS["information_collection"]},
        "capital_pool": {"available": bool(capital_pool), "owner": SOURCE_OWNERS["capital_pool"]},
    }
    safety_badges: list[str] = []
    for src in (wf, cn, dc):
        for b in _l(_d(src.get("safety")).get("safety_badges")) + _l(src.get("safety_badges")):
            if b not in safety_badges:
                safety_badges.append(str(b))
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "owner": OWNER,
        "status": "OK" if wf else "DEGRADED",
        "as_of": _now_iso(now),
        "eligible_market_date": (_d(wf.get("canonical_portfolio_decision")).get("eligible_market_date")
                                 or os_.get("eligible_market_date") or cmd.get("eligible_market_date")),
        "latest_completed_close_date": os_.get("latest_completed_close_date")
                                       or cmd.get("latest_completed_close_date"),
        "system_readiness": system,
        "portfolio_decision": decision,
        "headline": decision["headline"],
        "explanation": decision["explanation"],
        "next_action": decision["next_action"],
        "portfolio_snapshot": snapshot,
        "decision_summary": summary,
        "alerts_summary": alerts,
        "decision_outcome": outcome,
        "research_health": _research_health(wf),
        "historical_context": historical,
        "safety": {
            "mode_line": SAFETY_MODE_LINE,
            "paper_only": True,
            "manual_approval": True,
            "automation_off": True,
            "creates_orders": False,
            "creates_fills": False,
            "approves_proposals": False,
            "confirms_order_plans": False,
            "broker": "NONE",
            "badges": safety_badges,
            "detail_location": "Portfolio › Audit & Details and System · Audit",
        },
        "raw_states": raw_states,
        "sources": sources,
        "warnings": warns,
        "recomputes_nothing": True,
        "recomputed_concepts": [],
        "consumed_owners": dict(SOURCE_OWNERS),
        "read_only": True,
        "wrote_to_database": False,
        "wrote_to_ledger": False,
        "called_provider": False,
        "called_prediction": False,
        "note": ("ONE reconciled operator presentation. It consumes the authoritative "
                 "owners and translates their already-decided states into one operator "
                 "truth; it never recomputes NAV, a target, a portfolio decision, a "
                 "constraint, an HOC assessment, a proposal, an execution state or a "
                 "research verdict, never rewrites history and never fabricates a proposal."),
    }


def _accepts(fn: Callable, name: str) -> bool:
    try:
        import inspect
        return name in inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False


def owner_loaders(*, portfolio_state: Optional[dict] = None) -> dict[str, Callable[[], dict]]:
    """The owners, composed — never forked.

    Every owner that accepts an injected ``portfolio_state`` receives the ONE
    portfolio state loaded here, so the presentation costs one portfolio-state
    read instead of five (the read models are otherwise identical). The
    collection lifecycle is read through the owner's own
    ``resolve_service_lifecycle`` verdict (RUNNING / STOPPED / DEGRADED /
    NEVER_STARTED) — the same function the full collection read model uses —
    without the per-source health scan the presentation does not render.

    Release 50 - the decision snapshot passes the ONE portfolio state it already
    composed (``portfolio_state=``) and reuses these loaders for the owners it
    fans out, so no second read-model composition exists.
    """
    # Imported lazily so the pure builder stays importable without the owners.
    from paper_trader.api import workflow_state as _ws
    from paper_trader.api import reallocation_proposal as _rp
    from paper_trader.api import daily_close as _dc
    from paper_trader.api import material_information as _mi
    from paper_trader.api import portfolio_decision_outcome as _pdo
    from paper_trader.api import information_collection as _ic
    from paper_trader.api import portfolio_state as _pst
    from paper_trader.api import portfolio_decision as _pdm
    from paper_trader.api import holding_opportunity_cost as _hoc
    from paper_trader.api import event_signal_refresh as _esr
    from paper_trader.api import return_forecast as _rfc
    from paper_trader.api import capital_pool as _cpool

    cache: dict[str, Any] = {}
    if portfolio_state is not None:
        cache["ps"] = portfolio_state

    def _ps() -> Optional[dict]:
        if "ps" not in cache:
            try:
                cache["ps"] = _pst.load_portfolio_state()
            except Exception:  # noqa: BLE001 - owners fall back to their own read
                cache["ps"] = None
        return cache["ps"]

    def _with_ps(fn: Callable, **kw):
        ps = _ps()
        if ps is not None and _accepts(fn, "portfolio_state"):
            kw["portfolio_state"] = ps
        return fn(**kw)

    def _material() -> dict:
        # The owner's own composition (``load_material_information``) re-reads the
        # portfolio state inside each composed owner; this passes the ONE state in
        # and calls the owner's own pure ``build`` — identical output, one read.
        try:
            ev = _with_ps(_esr.load_event_signal_refresh_status)
            hoc = _with_ps(_hoc.load_holding_opportunity_cost)
            dec = _with_ps(_pdm.load_portfolio_decision)
            fsum = _rfc.summary()
            return _mi.build(event_refresh=ev, hoc=hoc, decision=dec,
                             forecast_summary=fsum, limit=12)
        except Exception:  # noqa: BLE001 - the owner's own degrade path
            return _mi.load_material_information(limit=12)

    def _collection() -> dict:
        state = _ic.load_service_state()
        try:
            lock = _ic._read_json(_ic._lock_path()) or None
        except Exception:  # noqa: BLE001
            lock = None
        lc = _ic.resolve_service_lifecycle(state, lock, datetime.now(timezone.utc))
        return {"service": lc,
                "recovery": {"why": lc.get("reason")},
                "headline": {"detail": lc.get("reason")},
                "lifecycle_owner": "api.information_collection.resolve_service_lifecycle",
                "scope": "SERVICE_LIFECYCLE_ONLY"}

    return {
        "workflow": lambda: _ws.load_workflow_state(),
        "constrained": lambda: _with_ps(_rp.load_constrained_reallocation),
        "daily_close": lambda: _dc.load_daily_close(),
        "material_information": _material,
        "decision_outcomes": lambda: _pdo.load_portfolio_decision_outcomes(),
        "information_collection": _collection,
        "capital_pool": lambda: _cpool.load_capital_pool(portfolio_state=_ps()),
    }


def _default_loaders() -> dict[str, Callable[[], dict]]:
    return owner_loaders()


def load_operator_presentation(*, workflow: Optional[dict] = None,
                               constrained: Optional[dict] = None,
                               daily_close: Optional[dict] = None,
                               material_information: Optional[dict] = None,
                               decision_outcomes: Optional[dict] = None,
                               information_collection: Optional[dict] = None,
                               loaders: Optional[dict] = None,
                               now: Optional[datetime] = None,
                               capital_pool: Optional[dict] = None) -> dict:
    """Compose the owners and build.

    READ-ONLY. Every owner is a GET-level read model; a failing owner degrades to a
    warning and an honest ``sources[<name>].available = False`` — never a crash and
    never a fabricated value.
    """
    supplied = {
        "workflow": workflow, "constrained": constrained, "daily_close": daily_close,
        "material_information": material_information,
        "decision_outcomes": decision_outcomes,
        "information_collection": information_collection,
        "capital_pool": capital_pool,
    }
    warnings: list[str] = []
    missing = [k for k, v in supplied.items() if v is None]
    if missing:
        try:
            lds = dict(loaders) if loaders else _default_loaders()
        except Exception as exc:  # noqa: BLE001 - owners unavailable: degrade honestly
            lds = {}
            warnings.append("owner loaders unavailable: %s" % str(exc)[:160])
        # Serial on purpose: the read models are CPU-bound under the interpreter
        # lock, so threads buy nothing; the ONE shared portfolio-state read (see
        # ``_default_loaders``) is what makes the composition cheap.
        for k in missing:
            fn = lds.get(k)
            if fn is None:
                warnings.append("%s: no loader" % k)
                continue
            try:
                supplied[k] = fn()
            except Exception as exc:  # noqa: BLE001
                supplied[k] = None
                warnings.append("%s unavailable: %s" % (k, str(exc)[:160]))
    return build_operator_presentation(
        workflow=supplied["workflow"], constrained=supplied["constrained"],
        daily_close=supplied["daily_close"],
        material_information=supplied["material_information"],
        decision_outcomes=supplied["decision_outcomes"],
        information_collection=supplied["information_collection"],
        capital_pool=supplied["capital_pool"],
        warnings=warnings, now=now)
