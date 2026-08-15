r"""Stage 22 — the canonical NORMAL DAILY PORTFOLIO CYCLE contract (PURE kernel).

WHY THIS EXISTS
---------------
Nine surfaces could each answer "what does the operator do next?" — the Daily Close
panel, the Daily Research Cycle panel, the Holding Opportunity-Cost card, the
Reallocation Proposal card, the Active Portfolio Assessment card, the Stage-19
rebalance lifecycle, the right action rail, the Today hero and the operator command
bar. Each one was correct about its OWN domain, and each one was free to imply an
action. Stage 19.3 reduced the *promoted* action to one; Stage 22 removes the
remaining ambiguity by making the SEQUENCE itself canonical:

    1. WAIT_FOR_SESSION_CLOSE   the session is still open — nothing to do
    2. DAILY_CLOSE              marks / NAV / NEXT_CLOSE settlement / forward evidence
    3. DAILY_RESEARCH_CYCLE     signal refresh -> HOC -> reassessment -> proposal
    4. PORTFOLIO_DECISION       monitor, or manual proposal review
    5. CONTROLLED_REBALANCE     gate 1 -> order plan -> gate 2 -> desk -> reconcile

There is exactly ONE path through those stages. A surface no longer decides whether
it may offer an action: it reads the stage gate this kernel produced from the ONE
already-decided overall state.

PURITY
------
This module is a pure function of its arguments. It performs NO IO, reads no clock,
opens no store, calls no provider or prediction service, writes nothing, and imports
no ``api.*`` module. It DECIDES nothing about the world — the overall workflow state
is decided by ``api.workflow_state`` from the authoritative domain owners, and this
kernel only projects that decision onto the canonical sequence. It never creates an
order, never approves a proposal and never enables automation.
"""
from __future__ import annotations

from typing import Any, Optional

CALCULATION_OWNER = "engine.normal_cycle"
CYCLE_ID = "PAPER_TRADER_NORMAL_DAILY_CYCLE"
CYCLE_POLICY_VERSION = "normal_cycle.v1"
PHASE = "STAGE22"

# --------------------------------------------------------------------------- #
# Frozen stage vocabulary (the canonical normal cycle). The ORDER is the contract.
# --------------------------------------------------------------------------- #
STAGE_WAIT_FOR_SESSION_CLOSE = "WAIT_FOR_SESSION_CLOSE"
STAGE_DAILY_CLOSE = "DAILY_CLOSE"
STAGE_DAILY_RESEARCH_CYCLE = "DAILY_RESEARCH_CYCLE"
STAGE_PORTFOLIO_DECISION = "PORTFOLIO_DECISION"
STAGE_CONTROLLED_REBALANCE = "CONTROLLED_REBALANCE"
#: Not part of the normal cycle: an inconsistency or a named blocker must be resolved
#: before the cycle can continue. It is reported as RECOVERY, never as a cycle stage.
STAGE_RECOVERY = "RECOVERY"

STAGE_SEQUENCE = (
    STAGE_WAIT_FOR_SESSION_CLOSE,
    STAGE_DAILY_CLOSE,
    STAGE_DAILY_RESEARCH_CYCLE,
    STAGE_PORTFOLIO_DECISION,
    STAGE_CONTROLLED_REBALANCE,
)
STAGE_VOCABULARY = STAGE_SEQUENCE + (STAGE_RECOVERY,)

#: Stage status vocabulary (per-stage position relative to the current stage).
ST_DONE = "DONE"
ST_CURRENT = "CURRENT"
ST_UPCOMING = "UPCOMING"
STAGE_STATUS_VOCABULARY = (ST_DONE, ST_CURRENT, ST_UPCOMING)

#: The canonical description of every stage: what happens in it, who owns it and what
#: the operator's role is. Rendered VERBATIM by every surface — no UI copy of its own.
STAGE_CONTRACT = {
    STAGE_WAIT_FOR_SESSION_CLOSE: {
        "ordinal": 1,
        "label": "Wait for the session close",
        "happens": ("The current market session is still open. The latest eligible "
                    "completed session is already fully processed."),
        "produces": None,
        "owner": "engine.market_session",
        "operator_role": "NONE",
    },
    STAGE_DAILY_CLOSE: {
        "ordinal": 2,
        "label": "Daily Close",
        "happens": ("The Daily Close refreshes owned marks through the Paper Desk, "
                    "settles eligible NEXT_CLOSE paper orders, records NAV and "
                    "performance, and captures forward evidence."),
        "produces": ("operational marks, NAV, NEXT_CLOSE settlement, forward evidence"),
        "owner": "api.daily_close",
        "operator_role": "CONFIRM_AND_RUN",
    },
    STAGE_DAILY_RESEARCH_CYCLE: {
        "ordinal": 3,
        "label": "Daily Research Cycle",
        "happens": ("The Daily Research Cycle refreshes every required signal input, "
                    "scores the universe, produces the Holding Opportunity-Cost "
                    "assessment, reassesses the portfolio, builds a reallocation "
                    "proposal when one is justified, and records persistent "
                    "research-agent observations."),
        "produces": ("signal refresh, opportunity-cost assessment, portfolio "
                     "reassessment, reallocation proposal, research observations"),
        "owner": "api.daily_research_cycle",
        "operator_role": "CONFIRM_AND_RUN",
    },
    STAGE_PORTFOLIO_DECISION: {
        "ordinal": 4,
        "label": "Portfolio decision",
        "happens": ("The portfolio decision is presented: either no change is "
                    "economically justified (monitor), or a proposal requires manual "
                    "review. Reviewing creates no order."),
        "produces": "MONITOR / NO CHANGE, or MANUAL PROPOSAL REVIEW",
        "owner": "api.portfolio_decision",
        "operator_role": "REVIEW",
    },
    STAGE_CONTROLLED_REBALANCE: {
        "ordinal": 5,
        "label": "Controlled rebalance",
        "happens": ("An approved proposal becomes a deterministic order plan behind "
                    "two manual gates, executes as NEXT_CLOSE paper orders on the "
                    "Paper Desk, and is reconciled at the next completed close."),
        "produces": "manual gate 1, order plan, manual gate 2, paper fills, reconciliation",
        "owner": "api.rebalance_execution",
        "operator_role": "REVIEW_AND_CONFIRM",
    },
    STAGE_RECOVERY: {
        "ordinal": 0,
        "label": "Recovery",
        "happens": ("The normal cycle is suspended: authoritative surfaces disagree, "
                    "or a named blocker must be resolved before the cycle continues."),
        "produces": None,
        "owner": "api.workflow_state",
        "operator_role": "RESOLVE",
    },
}

# --------------------------------------------------------------------------- #
# overall_state -> canonical cycle stage. This is the WHOLE mapping: every state in
# api.workflow_state.OVERALL_STATES appears exactly once, so a new state can never
# silently fall outside the cycle (``stage_for_overall_state`` fails CLOSED).
# --------------------------------------------------------------------------- #
STAGE_FOR_OVERALL_STATE = {
    "INCONSISTENT_STATE": STAGE_RECOVERY,
    "WAITING_FOR_SESSION_CLOSE": STAGE_WAIT_FOR_SESSION_CLOSE,
    "WAITING_FOR_OWNED_DATA": STAGE_DAILY_CLOSE,
    "READY_FOR_DAILY_CLOSE": STAGE_DAILY_CLOSE,
    "RESEARCH_CYCLE_REQUIRED": STAGE_DAILY_RESEARCH_CYCLE,
    "RESEARCH_CYCLE_RUNNING": STAGE_DAILY_RESEARCH_CYCLE,
    "RESEARCH_CYCLE_BLOCKED": STAGE_RECOVERY,
    "PORTFOLIO_REASSESSMENT_REQUIRED": STAGE_DAILY_RESEARCH_CYCLE,
    "MANUAL_REVIEW_REQUIRED": STAGE_PORTFOLIO_DECISION,
    "DAILY_CYCLE_COMPLETE": STAGE_PORTFOLIO_DECISION,
    "DAILY_CYCLE_COMPLETE_EVIDENCE_GAP": STAGE_PORTFOLIO_DECISION,
}

#: What becomes the operator's concern once the CURRENT stage completes. This is the
#: "WHAT HAPPENS AFTER THAT" answer, owned here rather than written into UI copy.
_AFTER_TEXT = {
    STAGE_WAIT_FOR_SESSION_CLOSE: (
        "When the session closes, the Daily Close becomes the one required action."),
    STAGE_DAILY_CLOSE: (
        "When the Daily Close completes, the Daily Research Cycle becomes the one "
        "required action. No separate desk, target, evidence or mark refresh is "
        "needed in between."),
    STAGE_DAILY_RESEARCH_CYCLE: (
        "When the Daily Research Cycle completes, the portfolio decision is "
        "presented: monitor if no change is justified, or a proposal for manual "
        "review. Nothing is approved or executed automatically."),
    STAGE_PORTFOLIO_DECISION: (
        "If you approve a proposal, the controlled rebalance runs behind two manual "
        "gates. Otherwise nothing happens until the next session closes."),
    STAGE_CONTROLLED_REBALANCE: (
        "Confirmed paper orders settle at the next eligible completed close and are "
        "reconciled there. Until then this is monitoring only."),
    STAGE_RECOVERY: (
        "Once the named blocker or inconsistency is resolved, the normal cycle "
        "resumes at its current stage."),
}


def stage_for_overall_state(overall: Any) -> str:
    """Map an ``api.workflow_state`` overall state to its canonical cycle stage.

    FAILS CLOSED: an unknown state is RECOVERY, never silently treated as part of the
    normal cycle.
    """
    return STAGE_FOR_OVERALL_STATE.get(str(overall), STAGE_RECOVERY)


def stage_ordinal(stage: Any) -> int:
    return int((STAGE_CONTRACT.get(str(stage)) or {}).get("ordinal") or 0)


def next_stage(stage: Any) -> Optional[str]:
    """The stage that follows ``stage`` in the canonical sequence (None at the end)."""
    s = str(stage)
    if s == STAGE_RECOVERY or s not in STAGE_SEQUENCE:
        return None
    i = STAGE_SEQUENCE.index(s)
    return STAGE_SEQUENCE[i + 1] if i + 1 < len(STAGE_SEQUENCE) else None


# --------------------------------------------------------------------------- #
# Stage gates — the ONE verdict every surface obeys (Workstream A).
#
# A surface never decides for itself whether it may offer its action. It reads the
# gate this kernel produced from the ONE already-decided overall state, so the Daily
# Close panel, the Daily Research panel, the portfolio-decision card and the Stage-19
# rebalance lifecycle can never disagree about what the operator must do next.
# --------------------------------------------------------------------------- #
#: Stages whose MUTATION gate may be OPEN, keyed by the overall states that open them.
#: Only the two stages that actually write are here. The portfolio decision is a REVIEW
#: (it records a human decision through its own owner and creates no order), and the
#: controlled rebalance runs behind its own two manual gates — neither is a normal-path
#: mutation, so neither can ever open this gate. That is what keeps "at most one
#: mutation" a statement about writes rather than about buttons.
_GATE_OPEN_STATES = {
    STAGE_DAILY_CLOSE: frozenset({"READY_FOR_DAILY_CLOSE", "WAITING_FOR_OWNED_DATA"}),
    STAGE_DAILY_RESEARCH_CYCLE: frozenset({"RESEARCH_CYCLE_REQUIRED"}),
}

#: Stages whose REVIEW is required, keyed by the overall states that require it. A review
#: is read-only: it approves nothing, writes no order and enables no automation.
_REVIEW_REQUIRED_STATES = {
    STAGE_PORTFOLIO_DECISION: frozenset({"MANUAL_REVIEW_REQUIRED"}),
}

#: Passive wording per (stage, current stage relationship). Deterministic, never a
#: disabled-looking execute control.
def _gate_passive_text(stage: str, *, current: str, overall: str,
                       eligible: Optional[str], closed: Optional[str]) -> str:
    cur_ord, my_ord = stage_ordinal(current), stage_ordinal(stage)
    if current == STAGE_RECOVERY:
        return ("Unavailable — resolve the named blocker or state inconsistency "
                "first.")
    if stage == STAGE_DAILY_CLOSE and my_ord < cur_ord:
        return "Daily Close complete for %s." % (closed or eligible or "the eligible session")
    if stage == STAGE_DAILY_CLOSE and my_ord > cur_ord:
        return "Daily Close waiting for the current market session to close."
    if stage == STAGE_DAILY_RESEARCH_CYCLE and my_ord > cur_ord:
        return "Daily Research Cycle waiting for the Daily Close."
    if stage == STAGE_DAILY_RESEARCH_CYCLE and my_ord < cur_ord:
        return "Daily Research Cycle complete for %s." % (eligible or "the eligible session")
    if stage == STAGE_DAILY_RESEARCH_CYCLE and overall == "RESEARCH_CYCLE_RUNNING":
        return "A Daily Research Cycle run is in progress — do not start it again."
    if stage == STAGE_DAILY_RESEARCH_CYCLE:
        return "Daily Research Cycle waiting for the reassessment step to complete."
    if stage == STAGE_PORTFOLIO_DECISION and my_ord > cur_ord:
        return "The portfolio decision is presented after the Daily Research Cycle."
    if stage == STAGE_PORTFOLIO_DECISION:
        return "No portfolio change requires review."
    if stage == STAGE_CONTROLLED_REBALANCE:
        return ("A controlled rebalance runs only from an approved proposal, behind "
                "its own two manual gates.")
    if stage == STAGE_WAIT_FOR_SESSION_CLOSE:
        # This stage never has an action; the wording only has to be TRUE. While it is
        # the current stage the session is still open — saying it had already closed
        # would contradict the state that put us here.
        return ("The current market session is still open."
                if my_ord == cur_ord
                else "The market session has already closed for the eligible session.")
    return "No action available in this stage."


def build_stage_gates(*, overall: Any, current_stage: Any,
                      eligible_market_date: Optional[str] = None,
                      latest_completed_close_date: Optional[str] = None,
                      execution_active: bool = False) -> dict[str, dict]:
    """The per-stage verdict every surface obeys verbatim.

    ``execution_allowed`` is True for AT MOST ONE stage — the current stage, and only
    when the already-decided overall state opens its gate. ``review_required`` marks the
    read-only portfolio-decision review, which is never a mutation and so never counts
    toward that limit. Every stage that offers neither carries a passive status string
    and no execute affordance. An unknown/recovery state opens NOTHING (fail closed).
    """
    ov = str(overall)
    cur = str(current_stage)
    gates: dict[str, dict] = {}
    for stage in STAGE_SEQUENCE:
        open_states = _GATE_OPEN_STATES.get(stage) or frozenset()
        allowed = bool(stage == cur and ov in open_states)
        review_states = _REVIEW_REQUIRED_STATES.get(stage) or frozenset()
        review = bool(stage == cur and ov in review_states)
        # Stage-19 precedence: while a confirmed order plan is still executing, the
        # portfolio-decision review stays closed (the execution lifecycle keeps the
        # operator's attention) — the reassessment remains readable as evidence.
        if review and execution_active:
            review = False
        gates[stage] = {
            "stage": stage,
            "ordinal": stage_ordinal(stage),
            "execution_allowed": allowed,
            "review_required": review,
            "creates_orders": False,
            "is_current_stage": stage == cur,
            "owner": (STAGE_CONTRACT.get(stage) or {}).get("owner"),
            "passive_status": (None if (allowed or review) else _gate_passive_text(
                stage, current=cur, overall=ov,
                eligible=eligible_market_date, closed=latest_completed_close_date)),
        }
    return gates


# --------------------------------------------------------------------------- #
# Single-primary-mutation invariant (Workstream A / H).
# --------------------------------------------------------------------------- #
class MultiplePrimaryMutationError(AssertionError):
    """Raised when more than one normal-path mutation is offered at once."""


def assert_single_primary_mutation(offers: Any) -> list[str]:
    """FAIL CLOSED unless AT MOST ONE normal-path mutation is offered.

    ``offers`` is any iterable of ``(label, allowed)`` pairs or of dicts carrying
    ``stage``/``execution_allowed``. Returns the (0 or 1) allowed labels.
    """
    allowed: list[str] = []
    for o in (offers or []):
        if isinstance(o, dict):
            if o.get("execution_allowed"):
                allowed.append(str(o.get("stage") or o.get("label") or "?"))
        else:
            label, ok = o
            if ok:
                allowed.append(str(label))
    if len(allowed) > 1:
        raise MultiplePrimaryMutationError(
            "%d normal-path mutations offered at once (%s); exactly one canonical "
            "primary action or none is permitted."
            % (len(allowed), ", ".join(sorted(allowed))))
    return allowed


# --------------------------------------------------------------------------- #
# The operator-facing cycle view (Workstream G): the four questions, answered by the
# backend so no surface writes its own copy.
# --------------------------------------------------------------------------- #
def build_cycle_view(*, overall: Any, current_task: Any = None,
                     why: Any = None, action_available: bool = False,
                     action_label: Any = None, no_action_text: Any = None,
                     eligible_market_date: Optional[str] = None,
                     latest_completed_close_date: Optional[str] = None,
                     execution_active: bool = False,
                     blockers: Any = None) -> dict[str, Any]:
    """Project the ONE decided workflow state onto the canonical normal cycle.

    Returns the four operator answers (now / do / why / after), the ordered stage
    list with each stage's status, and the per-stage gates. It decides nothing: the
    overall state and the primary action were already resolved by the workflow owner.
    """
    ov = str(overall)
    current = stage_for_overall_state(ov)
    # A confirmed order plan still awaiting NEXT_CLOSE settlement places the operator
    # in the controlled-rebalance stage of the cycle even though the operational lane
    # has nothing to run — the cycle position is where the CAPITAL is, not where the
    # buttons are.
    if execution_active and current == STAGE_PORTFOLIO_DECISION:
        current = STAGE_CONTROLLED_REBALANCE
    cur_ord = stage_ordinal(current)
    recovery = (current == STAGE_RECOVERY)

    stages = []
    for s in STAGE_SEQUENCE:
        o = stage_ordinal(s)
        status = (ST_CURRENT if s == current
                  else (ST_DONE if (not recovery and o < cur_ord) else ST_UPCOMING))
        c = STAGE_CONTRACT[s]
        stages.append({
            "stage": s, "ordinal": o, "label": c["label"], "happens": c["happens"],
            "produces": c["produces"], "owner": c["owner"],
            "operator_role": c["operator_role"], "status": status,
        })

    gates = build_stage_gates(
        overall=ov, current_stage=current, eligible_market_date=eligible_market_date,
        latest_completed_close_date=latest_completed_close_date,
        execution_active=execution_active)
    allowed = assert_single_primary_mutation(list(gates.values()))

    nxt = next_stage(current)
    contract = STAGE_CONTRACT[current]
    now_text = contract["happens"]
    if bool(action_available) and action_label:
        do_text = str(action_label)
    elif recovery:
        do_text = "Resolve the named blocker before the cycle continues."
    else:
        do_text = str(no_action_text or "No action required right now")

    return {
        "cycle_id": CYCLE_ID,
        "calculation_owner": CALCULATION_OWNER,
        "policy_version": CYCLE_POLICY_VERSION,
        "stage_vocabulary": list(STAGE_VOCABULARY),
        "stage_sequence": list(STAGE_SEQUENCE),
        "stage_status_vocabulary": list(STAGE_STATUS_VOCABULARY),
        "current_stage": current,
        "current_stage_ordinal": cur_ord,
        "current_stage_label": contract["label"],
        "next_stage": nxt,
        "next_stage_label": (STAGE_CONTRACT[nxt]["label"] if nxt else None),
        "stages": stages,
        "stage_gates": gates,
        # --- the four operator answers (Workstream G) ------------------------ #
        "now_text": now_text,
        "do_text": do_text,
        "why_text": (str(why) if why else None),
        "after_text": _AFTER_TEXT[current],
        "current_task": (str(current_task) if current_task else None),
        # --- hard invariants -------------------------------------------------- #
        "action_required": bool(action_available),
        "no_action_required": not bool(action_available),
        "in_recovery": recovery,
        "executable_stages": allowed,
        "executable_stage_count": len(allowed),
        "single_primary_mutation_enforced": True,
        "alternate_path_exists": False,
        "blockers": list(blockers or []),
        "creates_orders": False,
        "automatic_execution": False,
        "note": ("One canonical daily cycle. Every surface reads this stage contract "
                 "and its gates; no surface derives a workflow priority of its own, "
                 "and at most one normal-path mutation is ever offered."),
    }


__all__ = [
    "PHASE", "CALCULATION_OWNER", "CYCLE_ID", "CYCLE_POLICY_VERSION",
    "STAGE_WAIT_FOR_SESSION_CLOSE", "STAGE_DAILY_CLOSE", "STAGE_DAILY_RESEARCH_CYCLE",
    "STAGE_PORTFOLIO_DECISION", "STAGE_CONTROLLED_REBALANCE", "STAGE_RECOVERY",
    "STAGE_SEQUENCE", "STAGE_VOCABULARY", "STAGE_CONTRACT", "STAGE_FOR_OVERALL_STATE",
    "ST_DONE", "ST_CURRENT", "ST_UPCOMING", "STAGE_STATUS_VOCABULARY",
    "MultiplePrimaryMutationError",
    "stage_for_overall_state", "stage_ordinal", "next_stage", "build_stage_gates",
    "assert_single_primary_mutation", "build_cycle_view",
]
