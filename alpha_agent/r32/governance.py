"""alpha_agent.r32.governance - the Daily Multi-Asset Governance CONTRACT.

This module declares how a future multi-asset portfolio would be governed. It
declares; it does not run. Release 32 is production read-only, so nothing here
schedules a review, emits an event, computes a target or touches the operational
book. Release 33 implements against this contract.

The four rules that make daily reassessment safe:

**Daily reassessment is not daily trading.** A difference between the current
portfolio and the target is a reason to EVALUATE a change, not to make one. The
portfolio moves only when

    ExpectedUtility(target) - ExpectedUtility(current) - TransitionCosts
        > GovernanceHurdle

Most days that quantity is negative, and the correct action is none. Without the
hurdle, a system that reassesses every day trades every day and pays for the
privilege of restating its own noise.

**Closed markets do not cancel decisions.** Assets keep different calendars. A
target delta for an instrument whose market is shut stays PENDING - it is
neither executed nor silently dropped, and the distinction between the IDEAL
target and the CURRENTLY EXECUTABLE one is explicit.

**Stale data fails closed.** An input past its freshness budget blocks the
reassessment rather than being carried forward. A forward-filled price is a
guess wearing the costume of an observation.

**One NAV.** Multi-asset NAV gets exactly one authoritative owner, declared here
before any second implementation can appear.

**Concepts without values.** Turnover budgets are declared as concepts with a
named future owner and NO numbers. Release 32 measured nothing that could
calibrate them, and a serialised guess is indistinguishable from a calibrated
limit by the time Release 33 reads it. An uncalibrated budget is undecidable -
neither zero nor unlimited - and consumers get that state, not a float.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Optional

from .. import r32
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r32.governance"
GOVERNANCE_SCHEMA = "r32_daily_multi_asset_governance_contract/2"
ARTIFACT_NAME = "daily_multi_asset_governance_contract_v2.json"

#: Schema 1 declared INVENTED turnover budget values. It stays on disk, frozen,
#: because a campaign that can rewrite its own record is not a record - the same
#: rule that made v1/v2/v3 of this campaign supersessions rather than edits. The
#: research result is unaffected: no verdict, frontier or sleeve number reads
#: this artifact, which is why the correction is an artifact supersession and
#: not a new campaign.
SUPERSEDED_ARTIFACT = {
    "artifact": "daily_multi_asset_governance_contract.json",
    "schema": "r32_daily_multi_asset_governance_contract/1",
    "state": "SUPERSEDED_UNAUTHORISED_VALUES",
    "defects": [
        "turnover budgets were serialised with invented numeric values "
        "(daily/weekly/monthly) although Release 32 measured no multi-asset "
        "trading behaviour that could calibrate them and named no owner who "
        "had chosen them. Release 33 would have inherited them as settled "
        "limits, indistinguishable from calibrated ones",
    ],
    "correction": "schema 2 declares the three budget CONCEPTS with null "
                  "values, a NOT_CALIBRATED value state, an explicit future "
                  "value owner, and an undecidable - not zero, not unlimited - "
                  "answer for any consumer that asks",
    "research_result_affected": False,
}

#: The existing event system. Release 32 declares REUSE, not a second fabric.
#: Two event systems means two answers to "did this happen", and the operator
#: has no way to know which one the portfolio believed.
EVENT_FABRIC_OWNER = "engine.event_fabric"
SECOND_EVENT_SYSTEM_ALLOWED = False

#: The single future owner of multi-asset NAV, named now so it cannot be
#: quietly duplicated later. Principle 1 in its most literal form.
MULTI_ASSET_NAV_OWNER = "api.portfolio_valuation"
NAV_OWNER_STATE = "DECLARED_FOR_RELEASE_33_NOT_YET_EXTENDED"

REVIEW_SCHEDULED = "SCHEDULED_DAILY_REVIEW"
REVIEW_EVENT_DRIVEN = "EVENT_DRIVEN_REVIEW"
REVIEW_MODES = (REVIEW_SCHEDULED, REVIEW_EVENT_DRIVEN)

#: Both modes produce the same object through the same path. A scheduled review
#: and an event-driven one that disagree about what a reassessment IS would give
#: the operator two portfolios.
ORCHESTRATION_CONTRACT = "ONE_REASSESSMENT_CONTRACT_FOR_BOTH_MODES"

MARKET_OPEN = "OPEN"
MARKET_CLOSED = "CLOSED"
MARKET_HOLIDAY = "HOLIDAY"
MARKET_UNKNOWN = "CALENDAR_UNKNOWN"
MARKET_STATES = (MARKET_OPEN, MARKET_CLOSED, MARKET_HOLIDAY, MARKET_UNKNOWN)

DELTA_EXECUTABLE = "EXECUTABLE_NOW"
DELTA_PENDING_MARKET_CLOSED = "PENDING_MARKET_CLOSED"
DELTA_PENDING_STALE_DATA = "PENDING_STALE_DATA"
DELTA_BLOCKED = "BLOCKED"
DELTA_STATES = (DELTA_EXECUTABLE, DELTA_PENDING_MARKET_CLOSED,
                DELTA_PENDING_STALE_DATA, DELTA_BLOCKED)

#: Turnover budgets. Release 32 declares the CONCEPTS - that a daily, a weekly
#: and a monthly budget exist and that some owner sets them. It does not declare
#: VALUES, because no owner and no measured behaviour exist to set them against;
#: an invented number here would be indistinguishable from a calibrated one the
#: moment it was serialised, and Release 33 would inherit a limit nobody chose.
TURNOVER_BUDGET_PERIODS = ("daily", "weekly", "monthly")
TURNOVER_BUDGETS = {p: None for p in TURNOVER_BUDGET_PERIODS}

TURNOVER_BUDGET_VALUE_STATE = "NOT_CALIBRATED"
TURNOVER_BUDGET_VALUE_OWNER = (
    "RELEASE_33_MULTI_ASSET_TARGET_GOVERNANCE_CALIBRATION_OWNER")

#: An uncalibrated budget is UNDECIDABLE, and that is a third answer - not a
#: budget of zero (which would forbid every trade) and not a budget of infinity
#: (which would permit every trade). Both of those are decisions this release is
#: not entitled to make, and both look identical to a calibrated limit once they
#: reach a consumer as a plain float.
TURNOVER_WITHIN_BUDGET = "WITHIN_TURNOVER_BUDGET"
TURNOVER_OVER_BUDGET = "OVER_TURNOVER_BUDGET"
TURNOVER_BUDGET_UNDECIDABLE = "TURNOVER_BUDGET_NOT_CALIBRATED"
TURNOVER_BUDGET_STATES = (TURNOVER_WITHIN_BUDGET, TURNOVER_OVER_BUDGET,
                          TURNOVER_BUDGET_UNDECIDABLE)

#: Hedging. Substituting a correlated instrument for the one actually intended
#: is a modelling assumption with its own basis risk, and it needs its own
#: validated policy before it is ever automatic.
UNRELATED_INSTRUMENT_HEDGE_SUBSTITUTION_ALLOWED = False
HEDGE_POLICY_STATE = "NO_VALIDATED_HEDGE_POLICY_EXISTS"


def hysteresis_decision(*, expected_utility_target: float,
                        expected_utility_current: float,
                        transition_costs: float,
                        governance_hurdle: float) -> dict:
    """The no-churn rule, as arithmetic. Pure; declares nothing operational."""
    improvement = (float(expected_utility_target)
                   - float(expected_utility_current)
                   - float(transition_costs))
    act = improvement > float(governance_hurdle)
    return {"improvement_after_costs": improvement,
            "governance_hurdle": float(governance_hurdle),
            "action": "EVALUATE_CHANGE" if act else "NO_CHANGE",
            "trades": bool(act),
            "reassessment_happened": True,
            "note": "reassessment always happens; trading rarely should"}


def classify_delta(*, market_state: str, data_is_stale: bool) -> str:
    """What happens to one target delta given market and data state."""
    if data_is_stale:
        return DELTA_PENDING_STALE_DATA
    if market_state == MARKET_OPEN:
        return DELTA_EXECUTABLE
    if market_state in (MARKET_CLOSED, MARKET_HOLIDAY):
        return DELTA_PENDING_MARKET_CLOSED
    return DELTA_BLOCKED


def check_turnover_budget(*, period: str, proposed_turnover: float) -> dict:
    """Would this turnover fit the budget for ``period``?

    While the values are uncalibrated the honest answer is "unknown", and this
    returns it as a state rather than as a number a caller could compare. The
    failure this prevents is the one-liner ``if turnover > (budget or 0.0)``,
    which silently converts "nobody has set this" into "nothing may trade".
    """
    if period not in TURNOVER_BUDGETS:
        raise KeyError(f"unknown turnover budget period: {period!r}")
    limit = TURNOVER_BUDGETS[period]
    if limit is None:
        return {"period": period,
                "limit": None,
                "limit_state": TURNOVER_BUDGET_VALUE_STATE,
                "state": TURNOVER_BUDGET_UNDECIDABLE,
                "decidable": False,
                "within_budget": None,
                "means_zero_turnover": False,
                "means_unlimited_turnover": False,
                "value_owner": TURNOVER_BUDGET_VALUE_OWNER}
    within = float(proposed_turnover) <= float(limit)
    return {"period": period,
            "limit": float(limit),
            "limit_state": "CALIBRATED",
            "state": TURNOVER_WITHIN_BUDGET if within else TURNOVER_OVER_BUDGET,
            "decidable": True,
            "within_budget": within,
            "means_zero_turnover": False,
            "means_unlimited_turnover": False,
            "value_owner": TURNOVER_BUDGET_VALUE_OWNER}


def build_contract(*, campaign_id: str = _contract.CAMPAIGN_ID,
                   created_at: Optional[str] = None) -> dict:
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "created_at": created_at or _dt.datetime.now().isoformat(timespec="seconds"),
        "state": "DECLARED_FOR_RELEASE_33",
        "supersedes": dict(SUPERSEDED_ARTIFACT),
        "implemented_in_release_32": False,
        "runs_anything": False,
        "daily_reassessment_implies_daily_trading": False,
        "no_churn_rule": ("ExpectedUtility(target) - ExpectedUtility(current) "
                          "- TransitionCosts > GovernanceHurdle"),
        "review_modes": list(REVIEW_MODES),
        "orchestration_contract": ORCHESTRATION_CONTRACT,
        "event_fabric_owner": EVENT_FABRIC_OWNER,
        "second_event_system_allowed": SECOND_EVENT_SYSTEM_ALLOWED,
        "market_states": list(MARKET_STATES),
        "delta_states": list(DELTA_STATES),
        "closed_market_delta_remains_pending": True,
        "ideal_vs_currently_executable_target_is_explicit": True,
        "stale_data_fails_closed": True,
        "turnover_budget_periods": list(TURNOVER_BUDGET_PERIODS),
        "turnover_budgets": dict(TURNOVER_BUDGETS),
        "turnover_budgets_are_future_governance_concepts": True,
        "turnover_budget_concepts_declared": True,
        "turnover_budget_values_calibrated": False,
        "turnover_budget_value_state": TURNOVER_BUDGET_VALUE_STATE,
        "turnover_budget_value_owner": TURNOVER_BUDGET_VALUE_OWNER,
        "turnover_budget_states": list(TURNOVER_BUDGET_STATES),
        "uncalibrated_turnover_budget_means_zero_turnover": False,
        "uncalibrated_turnover_budget_means_unlimited_turnover": False,
        "uncalibrated_turnover_budget_is_undecidable": True,
        "risk_driven_reduction": {
            "may_reduce_without_a_new_opportunity": True,
            "rationale": "a risk limit breach is a reason to reduce even when "
                         "no sleeve has changed its opinion",
        },
        "unrelated_instrument_hedge_substitution_allowed":
            UNRELATED_INSTRUMENT_HEDGE_SUBSTITUTION_ALLOWED,
        "hedge_policy_state": HEDGE_POLICY_STATE,
        "multi_asset_nav_owner": MULTI_ASSET_NAV_OWNER,
        "multi_asset_nav_owner_state": NAV_OWNER_STATE,
        "asset_count_is_not_diversification": True,
        "diversification_measured_by": "RISK_FACTOR_AND_CORRELATION_CLUSTER",
        "sleeves_own_capital": False,
        "allocator_owns_capital": True,
    }
    body = r32.artifact_body(GOVERNANCE_SCHEMA, payload)
    body["governance_hash"] = r32.sha(payload)
    return body


def path_for(campaign_id: str = _contract.CAMPAIGN_ID) -> Path:
    return r32.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> Path:
    return r32.write_json(path_for(body["campaign_id"]), body)
