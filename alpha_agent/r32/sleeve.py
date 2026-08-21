"""alpha_agent.r32.sleeve - the ONE Strategy Sleeve contract owner.

A sleeve GENERATES OPPORTUNITIES. A sleeve does not own capital.

That boundary is the load-bearing rule of Release 32 and it is enforced here in
structure, not in prose. A sleeve returns a :class:`StrategyOpportunity`: a
direction, a conviction, and a RECOMMENDED exposure expressed in the sleeve's own
terms. It is an opinion. It is not an allocation, not a proposal, not a decision
and not an order.

The distinction is easy to lose, so it is worth being concrete about why it
matters. Six sleeves that each size their own book are six portfolio managers
who cannot see each other's exposures. Two of them can be long the same factor
through different instruments and believe they are diversified; one can be
hedging a risk another has already removed; and nothing in the system knows the
total. Sizing is a GLOBAL decision because risk is a global property. So the
global allocator owns capital, and Release 33 is where that allocator learns to
consume these opportunities. Release 32 only measures them.

To measure a sleeve's economics at all, something must turn an opinion into a
return path. That something is the JUDGE, never the sleeve, and what it builds
is a *research book* - a measurement device, explicitly not a portfolio target.
``StrategyOpportunity.research_book_is_not_a_portfolio_target`` is True on every
opportunity this module produces, and the audit asserts it.
"""
from __future__ import annotations

from typing import Callable, Optional

import numpy as np

from .. import r32
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r32.sleeve"
CONTRACT_SCHEMA = "r32_strategy_sleeve_contract/1"
ARTIFACT_NAME = "strategy_sleeve_contract.json"

# --------------------------------------------------------------------------- #
# Direction
# --------------------------------------------------------------------------- #
DIRECTION_LONG = "LONG"
DIRECTION_REDUCE = "REDUCE"
DIRECTION_FLAT = "FLAT"
DIRECTION_ROTATE = "ROTATE"
DIRECTIONS = (DIRECTION_LONG, DIRECTION_REDUCE, DIRECTION_FLAT, DIRECTION_ROTATE)

# --------------------------------------------------------------------------- #
# Sleeve states
# --------------------------------------------------------------------------- #
STATE_PROPOSED = "PROPOSED"
STATE_RESEARCH_ONLY = "RESEARCH_ONLY"
STATE_QUALIFIED = "QUALIFIED_NOT_ACTIVATED"
STATE_REJECTED = "REJECTED"
STATE_DATA_BLOCKED = "DATA_COVERAGE_INSUFFICIENT"
STATE_PIT_BLOCKED = "POINT_IN_TIME_EVIDENCE_BLOCKED"
STATE_NOT_ADMISSIBLE = "RESEARCH_ONLY_NOT_OPERATIONALLY_ADMISSIBLE"
SLEEVE_STATES = (STATE_PROPOSED, STATE_RESEARCH_ONLY, STATE_QUALIFIED,
                 STATE_REJECTED, STATE_DATA_BLOCKED, STATE_PIT_BLOCKED,
                 STATE_NOT_ADMISSIBLE)

#: No state in this package permits capital. ``QUALIFIED_NOT_ACTIVATED`` is
#: deliberately named so that "qualified" can never be misread as "live".
STATES_THAT_OWN_CAPITAL = ()

#: Actions a sleeve may never take. Asserted by the architecture audit against
#: every module under ``alpha_agent.r32.sleeves``.
FORBIDDEN_SLEEVE_ACTIONS = (
    "write a capital allocation",
    "size a book",
    "create a portfolio target",
    "create a reallocation proposal",
    "create a portfolio decision",
    "create an order",
    "promote a model",
    "activate itself",
    "mutate holdings or cash",
    "write any operational store",
)


class SleeveViolation(RuntimeError):
    """Raised when a sleeve tries to behave like a portfolio manager."""


# --------------------------------------------------------------------------- #
# The opportunity
# --------------------------------------------------------------------------- #
class StrategyOpportunity:
    """One sleeve's opinion at one decision date.

    ``recommended_exposure`` maps instrument -> weight in the sleeve's OWN
    terms, and must sum to at most 1.0 with a non-negative cash remainder. It is
    a shape, not a size: the global allocator decides how much capital, if any,
    stands behind it.
    """

    __slots__ = ("sleeve", "decision_date", "direction", "conviction",
                 "recommended_exposure", "cash_weight", "rationale",
                 "state_variables", "model_spec_hash")

    def __init__(self, *, sleeve: str, decision_date: str, direction: str,
                 conviction: float, recommended_exposure: dict,
                 rationale: str = "", state_variables: Optional[dict] = None,
                 model_spec_hash: Optional[str] = None):
        if sleeve not in _contract.SLEEVES:
            raise SleeveViolation(f"unknown sleeve: {sleeve}")
        if direction not in DIRECTIONS:
            raise SleeveViolation(f"unknown direction: {direction}")
        exposure = {k: float(v) for k, v in dict(recommended_exposure).items()}
        gross = sum(abs(v) for v in exposure.values())
        if gross > 1.0 + 1e-9:
            raise SleeveViolation(
                f"{sleeve} recommended gross exposure {gross:.4f} > 1.0; a "
                "sleeve may not lever, and may not size a book")
        self.sleeve = sleeve
        self.decision_date = str(decision_date)
        self.direction = direction
        self.conviction = float(conviction)
        self.recommended_exposure = exposure
        self.cash_weight = float(max(0.0, 1.0 - gross))
        self.rationale = str(rationale)
        self.state_variables = dict(state_variables or {})
        self.model_spec_hash = model_spec_hash

    # A sleeve is an opinion. These constants exist so that the property is
    # visible in every serialised artifact, not only in this docstring.
    owns_capital = False
    creates_portfolio_target = False
    creates_proposal = False
    creates_order = False
    research_book_is_not_a_portfolio_target = True

    def as_dict(self) -> dict:
        return {"sleeve": self.sleeve, "decision_date": self.decision_date,
                "direction": self.direction, "conviction": self.conviction,
                "recommended_exposure": dict(self.recommended_exposure),
                "cash_weight": self.cash_weight, "rationale": self.rationale,
                "state_variables": dict(self.state_variables),
                "model_spec_hash": self.model_spec_hash,
                "owns_capital": self.owns_capital,
                "creates_portfolio_target": self.creates_portfolio_target,
                "creates_proposal": self.creates_proposal,
                "creates_order": self.creates_order,
                "research_book_is_not_a_portfolio_target":
                    self.research_book_is_not_a_portfolio_target}


# --------------------------------------------------------------------------- #
# Sleeve specification
# --------------------------------------------------------------------------- #
class SleeveSpec:
    """One testable configuration of one sleeve.

    ``spec_hash`` binds the sleeve, the family, the parameters AND the judge's
    behaviour hash. Release 31 learned why the last part matters: binding a
    schema NAME let a corrected cost model silently reuse candidates measured
    under the old one, so a leaderboard mixed two judges without saying so.
    """

    __slots__ = ("sleeve", "family", "params", "stage", "depth", "generate",
                 "is_control")

    def __init__(self, *, sleeve: str, family: str, params: dict,
                 generate: Callable, stage: str = _contract.STAGE_SCREENING,
                 depth: int = 0, is_control: bool = False):
        self.sleeve = sleeve
        self.family = family
        self.params = dict(params)
        self.stage = stage
        self.depth = int(depth)
        self.generate = generate
        # A CONTROL is executed and counted, but may never become a finalist or
        # qualify its own sleeve. Release 32 v1 let the "hold the index every
        # session" control reach the lockbox, where beating cash would have
        # qualified EVENT_DRIVEN for discovering buy-and-hold.
        self.is_control = bool(is_control)

    def spec_hash(self, judge_behaviour_hash: str) -> str:
        return r32.sha({"sleeve": self.sleeve, "family": self.family,
                        "params": self.params,
                        "judge_behaviour_hash": judge_behaviour_hash})

    def label(self) -> str:
        parts = ",".join(f"{k}={self.params[k]}" for k in sorted(self.params))
        return f"{self.sleeve}:{self.family}:{parts}"

    def as_dict(self, judge_behaviour_hash: str) -> dict:
        return {"sleeve": self.sleeve, "family": self.family,
                "params": dict(self.params), "stage": self.stage,
                "depth": self.depth, "label": self.label(),
                "is_control": self.is_control,
                "spec_hash": self.spec_hash(judge_behaviour_hash)}


def normalise_exposure(raw: dict, *, max_gross: float = 1.0) -> dict:
    """Scale an exposure so gross <= ``max_gross``, preserving its shape.

    Scaling down is legitimate - it is still the same opinion, held smaller. A
    sleeve is never scaled UP to fill the book: an opinion that only wants 30 %
    invested is expressing a real preference for cash, and inflating it to 100 %
    would be the judge inventing conviction the sleeve did not have. Release 31
    made exactly that mistake in campaign v2, where cash was pinned to zero and
    a model that found nothing worth owning was still made to own 25 names.
    """
    gross = sum(abs(float(v)) for v in raw.values())
    if gross <= max_gross or gross <= 0.0:
        return {k: float(v) for k, v in raw.items()}
    scale = max_gross / gross
    return {k: float(v) * scale for k, v in raw.items()}


def top_k_long_only(scores: dict, *, k: int, gross: float = 1.0) -> dict:
    """Equal-weight the top ``k`` positive scores; the rest is cash."""
    ranked = sorted(((v, s) for s, v in scores.items()
                     if v is not None and np.isfinite(v)), reverse=True)
    chosen = [s for v, s in ranked[:max(0, int(k))] if v > 0]
    if not chosen:
        return {}
    w = float(gross) / float(len(chosen))
    return {s: w for s in chosen}


def build_contract(*, campaign_id: str = _contract.CAMPAIGN_ID) -> dict:
    """The frozen sleeve contract artifact."""
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "principle": "A sleeve generates opportunities. A sleeve does not own "
                     "capital.",
        "derived_from": "Release-32 Multi-Asset Design Rules A and B, which are "
                        "derived from Principle 1 (one canonical calculation "
                        "per business concept) and Principle 3 (research does "
                        "not create orders).",
        "directions": list(DIRECTIONS),
        "sleeve_states": list(SLEEVE_STATES),
        "states_that_own_capital": list(STATES_THAT_OWN_CAPITAL),
        "forbidden_sleeve_actions": list(FORBIDDEN_SLEEVE_ACTIONS),
        "opportunity_fields": [
            "sleeve", "decision_date", "direction", "conviction",
            "recommended_exposure", "cash_weight", "rationale",
            "state_variables", "model_spec_hash"],
        "max_gross_exposure": 1.0,
        "levered_sleeve_state": STATE_NOT_ADMISSIBLE,
        "zero_exposure_is_legitimate": True,
        "spec_hash_binds_judge_behaviour": True,
    }
    body = r32.artifact_body(CONTRACT_SCHEMA, payload)
    body["sleeve_contract_hash"] = r32.sha(payload)
    return body


def path_for(campaign_id: str = _contract.CAMPAIGN_ID):
    return r32.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    return r32.write_json(path_for(body["campaign_id"]), body)
