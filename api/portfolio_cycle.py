"""api/portfolio_cycle.py — Release 48: the ONE canonical PORTFOLIO CYCLE
orchestration entrypoint.

WHY THIS EXISTS
---------------
Before Release 48 the normal daily workflow was correct but exposed as a hidden
button order: the operator had to know that the Daily Close comes first, that the
Daily Research Cycle comes second, and that the portfolio decision appears only
after both — three concepts, two confirmation tokens, and a sequence the operator
had to carry in their head. The workflow owner (``api.workflow_state``) always
knew the sequence; the operator was still the one walking it.

Release 48 gives the operator ONE concept:

    RUN PORTFOLIO CYCLE

This module is the ONE orchestration path for it. It is a SEQUENCER, not a second
decision engine and not a second state machine:

  * WHAT to do next is decided solely by ``api.workflow_state`` (the one
    next-action owner). This module reads that decision verbatim between steps
    and never re-derives a workflow priority of its own.
  * HOW each step runs is owned solely by the existing authoritative execution
    owners — ``api.daily_close.run_daily_close`` (the ONLY close write path) and
    ``api.daily_research_cycle.run_daily_research_cycle`` (the sole research
    execution path). This module holds no write path of its own: no desk write,
    no ledger write, no order, no fill, no approval, no store of any kind.
  * WHERE the cycle stops is where governance begins: the run always halts at the
    governed portfolio decision (PROPOSAL_READY / HOLD_CURRENT_BOOK /
    TRUE_BLOCKER via the canonical decision object). It never reviews, approves,
    confirms or executes a proposal — those remain the two existing manual gates
    owned by ``api.portfolio_decision`` and ``api.rebalance_execution``.

GOVERNANCE BOUNDARIES (unchanged by this module)
------------------------------------------------
Model recalibration NEVER runs here (the Daily Research Cycle's frozen monthly
input has no safe automatic emitter and BLOCKS at the month boundary — this
module surfaces that blocker, it does not resolve it). R46 research promotion
never runs here. Paper execution never runs here. Proposal generation is
automatic; portfolio mutation is not.

Idempotency: both composed owners are idempotent for a processed session
(ALREADY_PROCESSED / existing run manifest), each owner is invoked AT MOST once
per run, and a run that cannot advance stops and says why rather than retrying.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Optional

PHASE = "R48"
#: This module — the ONE canonical portfolio-cycle orchestration owner.
ORCHESTRATION_OWNER = "api.portfolio_cycle"
#: The one operator confirmation token for the one operator concept.
EXECUTE_CONFIRMATION = "RUN_PORTFOLIO_CYCLE"
RUN_ROUTE = "/v1/operations/portfolio-cycle/run"
READ_ROUTE = "/v1/operations/portfolio-cycle"

#: The only two steps this orchestrator may take, each delegated verbatim to its
#: existing authoritative owner. There is deliberately no third step: review,
#: approval, order-plan confirmation and execution are manual gates, not steps.
STEP_DAILY_CLOSE = "DAILY_CLOSE"
STEP_DAILY_RESEARCH_CYCLE = "DAILY_RESEARCH_CYCLE"
STEP_VOCABULARY = (STEP_DAILY_CLOSE, STEP_DAILY_RESEARCH_CYCLE)
STEP_OWNERS = {
    STEP_DAILY_CLOSE: "api.daily_close",
    STEP_DAILY_RESEARCH_CYCLE: "api.daily_research_cycle",
}
#: Each composed owner keeps its OWN confirmation token; the orchestrator supplies
#: it because the operator already confirmed the one cycle token. These literals
#: are asserted equal to the owners' constants by the release tests.
_STEP_CONFIRMATIONS = {
    STEP_DAILY_CLOSE: "CONFIRM_ALPHA_DAILY_CLOSE",
    STEP_DAILY_RESEARCH_CYCLE: "RUN_DAILY_RESEARCH_CYCLE",
}

#: Why a run stopped (frozen vocabulary; every stop names its reason).
STOP_DECISION_PRESENTED = "DECISION_PRESENTED"
STOP_WAITING_FOR_SESSION_CLOSE = "WAITING_FOR_SESSION_CLOSE"
STOP_CYCLE_ALREADY_RUNNING = "CYCLE_ALREADY_RUNNING"
STOP_RECOVERY_REQUIRED = "RECOVERY_REQUIRED"
STOP_STATE_DID_NOT_ADVANCE = "STATE_DID_NOT_ADVANCE"
STOP_OWNER_REPORTED_BLOCKER = "OWNER_REPORTED_BLOCKER"
STOP_VOCABULARY = (
    STOP_DECISION_PRESENTED, STOP_WAITING_FOR_SESSION_CLOSE,
    STOP_CYCLE_ALREADY_RUNNING, STOP_RECOVERY_REQUIRED,
    STOP_STATE_DID_NOT_ADVANCE, STOP_OWNER_REPORTED_BLOCKER)

#: Owner result statuses that are a named blocker (the run stops and reports the
#: owner's own words; nothing is retried and nothing is inferred from them).
_CLOSE_BLOCKED_STATUSES = frozenset({
    "DATA_BLOCKED", "EXECUTION_ERROR", "DAILY_CLOSE_CONFIRM_REQUIRED",
    "DAILY_CLOSE_IN_PROGRESS", "NOT_A_TRADING_DAY", "SESSION_NOT_CLOSED"})
_DRC_BLOCKED_STATES = frozenset({"BLOCKED", "FAILED", "REFUSED"})

_MAX_OWNER_INVOCATIONS = 2  # each step at most once; the vocabulary has two.


def _safety(performed_write: bool) -> dict:
    return {
        "safety": {
            "paper_only": True,
            "orchestrates_existing_owners_only": True,
            "owns_no_store": True,
            "performed_write": bool(performed_write),
            "writes_delegated_to": sorted(STEP_OWNERS.values()),
            "creates_orders": False,
            "creates_fills": False,
            "approves_proposals": False,
            "confirms_order_plans": False,
            "executes_rebalance": False,
            "promotes_models": False,
            "recalibrates_models": False,
            "touches_r46_research": False,
            "automation": "OFF",
            "manual_review_required_for_portfolio_mutation": True,
        }
    }


def _workflow_loader_default() -> dict:
    from paper_trader.api import workflow_state as ws
    return ws.load_workflow_state()


def _close_runner_default(*, requested_by: str) -> dict:
    from paper_trader.api import daily_close as dclose
    return dclose.run_daily_close(
        confirm=_STEP_CONFIRMATIONS[STEP_DAILY_CLOSE], requested_by=requested_by)


def _drc_runner_default(*, requested_by: str) -> dict:
    from paper_trader.api import daily_research_cycle as drc
    return drc.run_daily_research_cycle(
        confirm=_STEP_CONFIRMATIONS[STEP_DAILY_RESEARCH_CYCLE],
        requested_by=requested_by)


# --------------------------------------------------------------------------- #
# PURE planning: read the workflow owner's decision verbatim; derive nothing.
# --------------------------------------------------------------------------- #
def plan_next_step(workflow: Optional[dict]) -> dict[str, Any]:
    """The next orchestration step, read from the ONE decided workflow state.

    Pure function of the payload. It maps the workflow owner's OWN primary action
    to a delegated step and otherwise stops with a named reason. It never invents
    a step the workflow owner did not ask for, and an unknown state STOPS the run
    (fail closed) instead of guessing.
    """
    wf = workflow or {}
    overall = str(wf.get("overall_state") or "")
    primary = wf.get("primary_action") or {}
    kind = primary.get("execution_kind")
    action_code = str(primary.get("action_code") or "")

    if kind == "DAILY_CLOSE" and primary.get("execution_available"):
        return {"step": STEP_DAILY_CLOSE, "owner": STEP_OWNERS[STEP_DAILY_CLOSE],
                "stop_reason": None,
                "reason": ("The workflow owner's primary action is the Daily "
                           "Close (%s)." % action_code)}
    if kind == "DAILY_RESEARCH_CYCLE" and primary.get("execution_available"):
        return {"step": STEP_DAILY_RESEARCH_CYCLE,
                "owner": STEP_OWNERS[STEP_DAILY_RESEARCH_CYCLE],
                "stop_reason": None,
                "reason": ("The workflow owner's primary action is the Daily "
                           "Research Cycle (%s)." % action_code)}
    if overall == "PORTFOLIO_REASSESSMENT_REQUIRED":
        # The workflow owner's own action text declares the Daily Research Cycle
        # the SOLE execution path for a due reassessment (its
        # ASSESS_HOLDING_OPPORTUNITY_COST step). The orchestrator follows that
        # declaration; it does not invent a separate reassessment executor.
        return {"step": STEP_DAILY_RESEARCH_CYCLE,
                "owner": STEP_OWNERS[STEP_DAILY_RESEARCH_CYCLE],
                "stop_reason": None,
                "reason": ("Portfolio reassessment is due and the workflow owner "
                           "declares the Daily Research Cycle its sole execution "
                           "path (%s)." % action_code)}
    if overall == "RESEARCH_CYCLE_RUNNING":
        return {"step": None, "owner": None,
                "stop_reason": STOP_CYCLE_ALREADY_RUNNING,
                "reason": ("A Daily Research Cycle run is already in progress; "
                           "starting another is refused.")}
    if overall == "WAITING_FOR_SESSION_CLOSE":
        return {"step": None, "owner": None,
                "stop_reason": STOP_WAITING_FOR_SESSION_CLOSE,
                "reason": ("The current market session is still open; the latest "
                           "eligible completed session is fully processed.")}
    if overall in ("INCONSISTENT_STATE", "RESEARCH_CYCLE_BLOCKED"):
        return {"step": None, "owner": None,
                "stop_reason": STOP_RECOVERY_REQUIRED,
                "reason": ("The workflow owner reports %s — the named blocker or "
                           "inconsistency must be resolved before the cycle "
                           "continues; nothing is run over it." % overall)}
    if overall in ("MANUAL_REVIEW_REQUIRED", "DAILY_CYCLE_COMPLETE",
                   "DAILY_CYCLE_COMPLETE_EVIDENCE_GAP"):
        return {"step": None, "owner": None,
                "stop_reason": STOP_DECISION_PRESENTED,
                "reason": ("No cycle step is required in state %s; the governed "
                           "portfolio decision is what the workflow presents."
                           % overall)}
    # An unknown or future state is NEVER treated as runnable or as a presented
    # decision — the run stops, fail closed, and names the state.
    return {"step": None, "owner": None,
            "stop_reason": STOP_RECOVERY_REQUIRED,
            "reason": ("State %s is not one the portfolio-cycle orchestrator "
                       "recognises; nothing is run over an unrecognised state."
                       % (overall or "UNKNOWN"))}


def _owner_blocked(step: str, result: Optional[dict]) -> Optional[str]:
    """The owner's OWN blocking status, if it reported one (else None)."""
    r = result or {}
    if step == STEP_DAILY_CLOSE:
        s = str(r.get("status") or "")
        if s in _CLOSE_BLOCKED_STATUSES:
            return s
    if step == STEP_DAILY_RESEARCH_CYCLE:
        s = str(r.get("state") or r.get("status") or "")
        if s in _DRC_BLOCKED_STATES:
            return s
    return None


def _step_summary(step: str, result: Optional[dict]) -> dict:
    """A compact, verbatim summary of an owner's result (no re-interpretation)."""
    r = result or {}
    return {
        "step": step,
        "owner": STEP_OWNERS[step],
        "status": r.get("status") or r.get("state"),
        "close_status": r.get("close_status"),
        "run_id": r.get("run_id"),
        "market_date": r.get("market_date") or r.get("eligible_market_date"),
        "performed_write": r.get("performed_write"),
        "message": r.get("message"),
    }


def _operator_projection(wf: dict) -> dict:
    """The operator-facing slice of the ONE workflow payload, passed through
    verbatim. This is a projection (field selection), never a derivation."""
    wf = wf or {}
    cmd = wf.get("operator_command") or {}
    return {
        "overall_state": wf.get("overall_state"),
        "operator_command": cmd,
        "canonical_portfolio_decision": wf.get("canonical_portfolio_decision"),
        "portfolio_attention_kind": wf.get("portfolio_attention_kind"),
        "eligible_market_date": cmd.get("eligible_market_date"),
        "latest_completed_close_date": cmd.get("latest_completed_close_date"),
    }


# --------------------------------------------------------------------------- #
# READ-ONLY status: what a run would do right now (no write, no owner call).
# --------------------------------------------------------------------------- #
def load_portfolio_cycle(*, workflow: Optional[dict] = None,
                         workflow_loader: Optional[Callable] = None) -> dict:
    """Read-only: the one operator cycle status + what RUN PORTFOLIO CYCLE would
    do right now. Composes the ONE workflow owner; decides nothing itself."""
    loader = workflow_loader or _workflow_loader_default
    wf = workflow if workflow is not None else loader()
    plan = plan_next_step(wf)
    planned_steps: list[dict] = []
    if plan["step"] == STEP_DAILY_CLOSE:
        planned_steps.append({"step": STEP_DAILY_CLOSE,
                              "owner": STEP_OWNERS[STEP_DAILY_CLOSE]})
        # After a close the workflow owner routinely requires the research cycle;
        # the run re-reads the decided state between steps, so the preview names
        # the possible follow-up without promising it.
        planned_steps.append({"step": STEP_DAILY_RESEARCH_CYCLE,
                              "owner": STEP_OWNERS[STEP_DAILY_RESEARCH_CYCLE],
                              "conditional": True})
    elif plan["step"] == STEP_DAILY_RESEARCH_CYCLE:
        planned_steps.append({"step": STEP_DAILY_RESEARCH_CYCLE,
                              "owner": STEP_OWNERS[STEP_DAILY_RESEARCH_CYCLE]})
    return {
        "phase": PHASE,
        "orchestration_owner": ORCHESTRATION_OWNER,
        "status": "PORTFOLIO_CYCLE_STATUS",
        "cycle_run_available": bool(plan["step"]),
        "planned_steps": planned_steps,
        "plan_reason": plan["reason"],
        "stop_reason": plan["stop_reason"],
        "step_vocabulary": list(STEP_VOCABULARY),
        "stop_vocabulary": list(STOP_VOCABULARY),
        "execution_contract": {
            "method": "POST", "path": RUN_ROUTE,
            "confirmation_field": "confirmation",
            "confirmation_token": EXECUTE_CONFIRMATION},
        # Track B (decision consistency §7) — client-timeout recovery, stated on the
        # read route so an operator whose synchronous POST timed out never has to
        # guess. The safe recovery is READ STATUS (this route): a completed run shows
        # cycle_run_available=false / stop_reason=DECISION_PRESENTED with the
        # canonical decision beside it. A repeated POST cannot duplicate work — both
        # composed owners are idempotent for a processed session and a completed
        # state stops at DECISION_PRESENTED before any owner is invoked — but a
        # blind rerun is still the wrong recovery: read first.
        "timeout_recovery": {
            "safe_recovery": "GET %s" % READ_ROUTE,
            "guidance": ("If the POST to %s timed out at the client, the backend "
                         "run continues and completes independently. Do NOT rerun "
                         "blindly: read this route first. cycle_run_available="
                         "false with stop_reason=DECISION_PRESENTED means the run "
                         "completed and the governed decision is presented in "
                         "canonical_portfolio_decision." % RUN_ROUTE),
            "repeated_post_is_idempotent": True,
            "idempotency_basis": ("Each composed owner refuses/no-ops a session it "
                                  "already processed, and a completed workflow "
                                  "state stops the orchestrator at "
                                  "DECISION_PRESENTED before any owner runs."),
            "single_orchestration_path": RUN_ROUTE,
        },
        **_operator_projection(wf),
        **_safety(False),
    }


# --------------------------------------------------------------------------- #
# The ONE orchestrated run.
# --------------------------------------------------------------------------- #
def run_portfolio_cycle(*, confirm: Optional[str] = None,
                        requested_by: str = "manual_ui",
                        workflow_loader: Optional[Callable] = None,
                        close_runner: Optional[Callable] = None,
                        drc_runner: Optional[Callable] = None) -> dict:
    """Run the canonical portfolio cycle: sequence the EXISTING owners until the
    governed portfolio decision is presented, then stop.

    Token-gated (``RUN_PORTFOLIO_CYCLE``). Each composed owner runs AT MOST once,
    with its own confirmation supplied by this orchestrator and attributed
    ``portfolio_cycle:<requested_by>``. Between steps the ONE workflow owner is
    re-read and its decision followed verbatim. The run never approves, confirms
    or executes anything: it stops at the decision boundary in every path.
    """
    if confirm != EXECUTE_CONFIRMATION:
        return {"phase": PHASE, "orchestration_owner": ORCHESTRATION_OWNER,
                "status": "PORTFOLIO_CYCLE_CONFIRM_REQUIRED",
                "performed_write": False,
                "confirmation_required": EXECUTE_CONFIRMATION,
                "message": ("Running the portfolio cycle requires "
                            "confirmation='%s'." % EXECUTE_CONFIRMATION),
                "steps": [], **_safety(False)}

    loader = workflow_loader or _workflow_loader_default
    runners = {
        STEP_DAILY_CLOSE: close_runner or _close_runner_default,
        STEP_DAILY_RESEARCH_CYCLE: drc_runner or _drc_runner_default,
    }
    attributed = "portfolio_cycle:%s" % (requested_by or "manual_ui")

    steps: list[dict] = []
    ran: set[str] = set()
    stop_reason: Optional[str] = None
    stop_detail: Optional[str] = None
    wf = loader() or {}

    for _ in range(_MAX_OWNER_INVOCATIONS + 1):
        plan = plan_next_step(wf)
        if plan["step"] is None:
            stop_reason = plan["stop_reason"]
            stop_detail = plan["reason"]
            break
        step = plan["step"]
        if step in ran:
            # The owner ran once and the workflow owner still asks for it: the
            # state did not advance. Stop and say so — never run an owner twice
            # in one operator action.
            stop_reason = STOP_STATE_DID_NOT_ADVANCE
            stop_detail = ("The workflow state still requires %s after its owner "
                           "ran once; the run stops rather than repeating an "
                           "owner. Review the owner's own result below." % step)
            break
        ran.add(step)
        result = runners[step](requested_by=attributed)
        summary = _step_summary(step, result)
        summary["plan_reason"] = plan["reason"]
        steps.append(summary)
        blocked = _owner_blocked(step, result)
        if blocked:
            stop_reason = STOP_OWNER_REPORTED_BLOCKER
            stop_detail = ("%s reported %s — the owner's own message: %s"
                           % (STEP_OWNERS[step], blocked,
                              summary.get("message") or "(none)"))
            wf = loader() or {}
            break
        wf = loader() or {}
    else:  # pragma: no cover - unreachable with the two-step vocabulary
        stop_reason = STOP_STATE_DID_NOT_ADVANCE
        stop_detail = "Step budget exhausted."

    performed_write = any(bool(s.get("performed_write")) for s in steps) or bool(
        steps)
    return {
        "phase": PHASE,
        "orchestration_owner": ORCHESTRATION_OWNER,
        "status": "PORTFOLIO_CYCLE_COMPLETE" if stop_reason in (
            STOP_DECISION_PRESENTED, STOP_WAITING_FOR_SESSION_CLOSE)
        else "PORTFOLIO_CYCLE_STOPPED",
        "requested_by": requested_by,
        "steps": steps,
        "steps_taken": [s["step"] for s in steps],
        "stop_reason": stop_reason,
        "stop_detail": stop_detail,
        "stopped_at_decision_boundary": True,
        "performed_write": performed_write,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        **_operator_projection(wf),
        **_safety(performed_write),
    }


__all__ = [
    "PHASE", "ORCHESTRATION_OWNER", "EXECUTE_CONFIRMATION",
    "RUN_ROUTE", "READ_ROUTE",
    "STEP_DAILY_CLOSE", "STEP_DAILY_RESEARCH_CYCLE", "STEP_VOCABULARY",
    "STEP_OWNERS", "STOP_VOCABULARY",
    "STOP_DECISION_PRESENTED", "STOP_WAITING_FOR_SESSION_CLOSE",
    "STOP_CYCLE_ALREADY_RUNNING", "STOP_RECOVERY_REQUIRED",
    "STOP_STATE_DID_NOT_ADVANCE", "STOP_OWNER_REPORTED_BLOCKER",
    "plan_next_step", "load_portfolio_cycle", "run_portfolio_cycle",
]
