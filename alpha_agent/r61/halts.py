r"""alpha_agent.r61.halts - THE pre-measurement halt record (Workstream E).

RESEARCH ONLY. NO ORDERS, NO FILLS, NO PROMOTION. This module settles
experiments that stopped BEFORE any return existed. It never settles one that
produced a return, and it never makes a statement about alpha.

The gap R60 exposed
-------------------
``AgentPipeline.reveal_stage`` was the only path that could settle an
experiment. It settles by judging a MEASURED layer. So a cell that halted
before any layer existed - on a data-coverage floor, or on a pre-registered
economic constraint - had nowhere to go. It appeared in the campaign artifacts
as a halt and stayed OPEN in the research memory forever.

Four of R60's eight cells did exactly that, and the consequence was not
cosmetic. ``ResearchMemory.burden`` counts rows ``WHERE outcome IS NOT NULL``,
so four cells the director believed were charged to the search burden were not
charged at all. The estate's own denominator was understated by every cell that
halted early. Recording them corrects it upward, which is the safe direction:
a larger denominator only ever moves a future candidate further from
significance.

NO_ALPHA_EVIDENCE IS NOT AVAILABLE HERE, AND THAT IS THE POINT
--------------------------------------------------------------
A cell that never computed a return has said NOTHING about alpha. Settling it
as NO_ALPHA_EVIDENCE would file silence as evidence of absence - the exact
mislabelling the R60 director refused for basis momentum ("the graveyard entry
is currently mislabelled evidence"). :func:`outcome_for` therefore maps halt
reasons onto DATA_HOLD or REJECTED only, and :func:`build` refuses any caller
that tries to force NO_ALPHA_EVIDENCE through.

    DATA_COVERAGE_FLOOR           DATA_HOLD   the data was not there yet
    INSTRUMENT_COVERAGE_FLOOR     DATA_HOLD   the cross-section was too thin
    SUBSTRATE_FROZEN_PENDING_AUDIT DATA_HOLD  governance froze the substrate
    COST_BUDGET_EXCEEDED          REJECTED    a frozen economic constraint
                                              refused the construction
    TURNOVER_CEILING_EXCEEDED     REJECTED    the RETIRED scalar (R61 keeps it
                                              so R60's halts can be recorded
                                              under the threshold that was
                                              actually frozen at the time)

THE THRESHOLD RECORDED IS THE ONE THAT WAS FROZEN WHEN THE HALT HAPPENED.
Recording an R60 halt against R61's new cost budget would be rewriting a
measured past. The three R60 turnover halts stand exactly as they fired, under
0.40, and carry a ``superseded_by`` note rather than a new verdict.

ONE REGISTRY. This module builds and validates a record; the single write goes
through ``AgentPipeline.record_pre_measurement_halt`` into the existing
ResearchMemory event stream. There is no second registry, no second queue and
no second clock.
"""
from __future__ import annotations

from typing import Optional

from .. import r59
from . import now_iso, stable_hash

HALT_OWNER = "alpha_agent.agents_v2.pipeline.AgentPipeline"
HALT_BUILDER = "alpha_agent.r61.halts"
HALT_VERSION = "R61_PRE_MEASUREMENT_HALT_V1"

#: The event kind written into the ONE research memory.
EV_PREMEASUREMENT_HALT = "AGENTS_V2_PRE_MEASUREMENT_HALT"

#: Halt reason -> settled outcome. Declared, closed, and tested.
HALT_REASON_OUTCOMES = {
    "DATA_COVERAGE_FLOOR": r59.HO_DATA_HOLD,
    "INSTRUMENT_COVERAGE_FLOOR": r59.HO_DATA_HOLD,
    "SUBSTRATE_FROZEN_PENDING_AUDIT": r59.HO_DATA_HOLD,
    "COST_BUDGET_EXCEEDED": r59.HO_REJECTED,
    "TURNOVER_CEILING_EXCEEDED": r59.HO_REJECTED,
}
HALT_REASONS = tuple(sorted(HALT_REASON_OUTCOMES))

#: Outcomes this path may NEVER write. A halt before measurement is not a
#: measurement, and QUALIFIED/FORWARD_FROZEN would be an adoption claim.
FORBIDDEN_OUTCOMES = frozenset({
    r59.HO_NO_ALPHA_EVIDENCE, r59.HO_QUALIFIED, r59.HO_FORWARD_FROZEN,
    r59.HO_NEEDS_MORE_EVIDENCE})

#: Constant fields. A pre-measurement halt consumed no alpha layer and no
#: lockbox, by definition - if it had, it would not be pre-measurement.
ALPHA_LAYER_CONSUMED = "NONE"
LOCKBOX_CONSUMED = "NO"

#: A halted cell still consumed a pre-registration and a look at the data, so
#: it IS charged. This is the same treatment a measured halt receives.
BURDEN_CHARGED = "CHARGED"


class HaltRefusal(RuntimeError):
    """The halt record is not admissible."""


def outcome_for(halt_reason: str) -> str:
    """The settled outcome a halt reason maps to. Unknown reasons are refused."""
    try:
        return HALT_REASON_OUTCOMES[str(halt_reason)]
    except KeyError:
        raise HaltRefusal(
            "UNKNOWN_HALT_REASON: %r is not one of %s. A new halt reason is a "
            "governance decision and must be declared here with the outcome "
            "it maps to, never chosen at the call site."
            % (halt_reason, list(HALT_REASONS))) from None


def build(*, experiment_id: str, halt_reason: str, measured_metric: str,
          measured_value, frozen_threshold, frozen_threshold_owner: str = "",
          detail: str = "", superseded_by: str = "",
          outcome: Optional[str] = None) -> dict:
    """The canonical PRE_MEASUREMENT_HALT record. Pure; writes nothing."""
    if not str(experiment_id or "").strip():
        raise HaltRefusal("experiment_id is required")
    mapped = outcome_for(halt_reason)
    if outcome is not None and str(outcome) != mapped:
        raise HaltRefusal(
            "OUTCOME_NOT_THE_MAPPED_ONE: %s maps to %s, not %r. The mapping "
            "is the governance decision; a caller does not get to choose."
            % (halt_reason, mapped, outcome))
    if mapped in FORBIDDEN_OUTCOMES:                        # defensive
        raise HaltRefusal("FORBIDDEN_OUTCOME: %s" % mapped)
    if not str(measured_metric or "").strip():
        raise HaltRefusal(
            "measured_metric is required: a halt that cannot name what it "
            "measured is an assertion, not a governed record")
    if measured_value is None or frozen_threshold is None:
        raise HaltRefusal(
            "MEASURED_VALUE_AND_FROZEN_THRESHOLD_ARE_BOTH_REQUIRED: a halt is "
            "only auditable when both the number and the line it crossed are "
            "recorded")
    body = {
        "record": "PRE_MEASUREMENT_HALT",
        "record_version": HALT_VERSION,
        "builder": HALT_BUILDER,
        "experiment_id": str(experiment_id),
        "halt_reason": str(halt_reason),
        "measured_metric": str(measured_metric),
        "measured_value": measured_value,
        "frozen_threshold": frozen_threshold,
        "frozen_threshold_owner": str(frozen_threshold_owner or ""),
        "timestamp": now_iso(),
        "alpha_layer_consumed": ALPHA_LAYER_CONSUMED,
        "lockbox_consumed": LOCKBOX_CONSUMED,
        "outcome": mapped,
        "burden_treatment": BURDEN_CHARGED,
        "detail": str(detail or "")[:1000],
        "superseded_by": str(superseded_by or ""),
        "reopenable": mapped == r59.HO_DATA_HOLD,
    }
    body["record_hash"] = stable_hash(
        {k: v for k, v in body.items() if k not in ("timestamp",
                                                    "record_hash")})
    return body


def reason_for_memory(record: dict) -> str:
    """The one-line reason string stored on the settled hypothesis row."""
    return ("PRE_MEASUREMENT_HALT/%s: %s %s against the frozen threshold %s; "
            "no return was ever scored, so no statement is made about alpha"
            % (record["halt_reason"], record["measured_metric"],
               record["measured_value"], record["frozen_threshold"]))


def reopen_condition_for(record: dict) -> str:
    """What would legitimately reopen this halted cell."""
    if record["halt_reason"] in ("DATA_COVERAGE_FLOOR",
                                 "INSTRUMENT_COVERAGE_FLOOR"):
        return ("COVERAGE_CLEARS_THE_FROZEN_FLOOR: a NEW pre-registration "
                "whose sample start is fixed from the coverage series BEFORE "
                "any return is computed")
    if record["halt_reason"] == "SUBSTRATE_FROZEN_PENDING_AUDIT":
        return "SUBSTRATE_AUDIT_PASSES_ITS_PRE_REGISTERED_THRESHOLDS"
    return ("NEW_PRE_REGISTRATION_UNDER_THE_CORRECTED_CONSTRAINT: a new "
            "experiment id, charged to the burden. This cell is NOT re-run "
            "under a changed ceiling; a ceiling chosen after a measurement is "
            "outcome-directed tuning however it is documented")
