"""alpha_agent.r37.scoring - the ONE Release-37 purchase-value scorer.

Turns "how many cells, how good, how much?" into four cost metrics and one
transparent score. Every factor is declared in :mod:`alpha_agent.r37.contract`
BEFORE any provider is scored, every factor is emitted alongside the result, and
the formula is a sentence rather than a fitted model.

The score is a **ranking aid**. ``SCORE_MAY_OVERRIDE_A_HARD_GATE`` is False, so
a dataset that fails point-in-time, survivorship, history or licensing does not
climb the ranking by being cheap. That is the mistake the score is shaped to
prevent: the cheapest dataset in this release's long list is free, survivor-only
crypto, and a naive value-per-dollar metric would have put it first.
"""
from __future__ import annotations

from typing import Optional

from .. import r37
from . import contract as _contract
from . import providers as _providers

CALCULATION_OWNER = "alpha_agent.r37.scoring"
SCHEMA = "r37_dataset_purchase_scorecard/1"
ARTIFACT_NAME = "dataset_purchase_scorecard.json"

C = _contract

#: Gate states that mean the candidate has failed a HARD condition. Such a row
#: is scored - so the reader can see what it would have been worth - and is
#: excluded from the recommendation ranking regardless of its score.
HARD_FAIL_STATES = (
    C.STATE_NO_PIT, C.STATE_NO_SURVIVORSHIP, C.STATE_NO_HISTORY,
    C.STATE_NO_LICENCE, C.STATE_NO_COST_VALUE, C.STATE_NO_LOW_VALUE,
    C.STATE_SAMPLE_FAILED,
)

#: Gate states that are eligible to be ranked as a data INVESTMENT.
INVESTABLE_STATES = (C.STATE_BUY_RECOMMENDED, C.STATE_SAMPLE_PASSED)


def annualised_cost(row: dict) -> dict:
    """One comparable annual number for a subscription and for a perpetual dump."""
    annual = row.get("annual_cost_usd")
    one_time = row.get("one_time_cost_usd")
    known = annual is not None or one_time is not None
    total = float(annual or 0.0) + (
        float(one_time or 0.0) / C.COST_AMORTISATION_YEARS)
    return {"annual_cost_usd": annual,
            "one_time_cost_usd": one_time,
            "amortisation_years": C.COST_AMORTISATION_YEARS,
            "annualised_cost_usd": round(total, 2) if known else None,
            "cost_known": bool(known),
            "is_free": bool(known and total == 0.0)}


def integrity_factors(row: dict) -> dict:
    """Every multiplier applied to the raw cell count, and why."""
    level = row.get("implementation_level")
    structure = C.NATIVE_STRUCTURE_FACTOR.get(level, 0.05)
    dated = row.get("dated_contracts_available")
    continuous_penalty = (C.CONTINUOUS_ONLY_FACTOR if dated is False else 1.0)
    history = float(row.get("history_years") or 0.0)
    history_factor = min(1.0, history / C.HISTORY_REFERENCE_YEARS)
    return {
        "native_structure_factor": structure,
        "continuous_only_factor": continuous_penalty,
        "point_in_time_factor": C.PIT_FACTOR.get(row.get("pit_class"), 0.30),
        "survivorship_factor": C.SURVIVORSHIP_FACTOR.get(
            row.get("survivorship_class"), 0.40),
        "licence_factor": C.LICENCE_FACTOR.get(row.get("licence_class"), 0.50),
        "identity_factor": C.IDENTITY_FACTOR.get(row.get("identity_class"),
                                                 0.50),
        "opacity_factor": C.OPACITY_FACTOR.get(row.get("opacity_class"), 0.60),
        "history_years": history,
        "history_factor": round(history_factor, 4),
    }


def breadth_factor(n_asset_classes: int) -> float:
    """Sub-linear reward for spanning economically distinct asset classes."""
    return C.BREADTH_BASE + C.BREADTH_PER_ASSET_CLASS * max(0, int(n_asset_classes))


def score_row(row: dict, unlock_row: dict) -> dict:
    """Score one candidate. Pure arithmetic over declared factors."""
    factors = integrity_factors(row)
    cost = annualised_cost(row)
    cells_full = int(unlock_row.get("cells_unlocked_full") or 0)
    cells_ceiling = int(unlock_row.get("cells_unlocked_ceiling") or 0)
    n_classes = int(unlock_row.get("n_asset_classes_unlocked_full") or 0)
    markets_full = int(unlock_row.get("native_markets_unlocked_full") or 0)
    breadth = breadth_factor(n_classes)

    multiplier = (factors["native_structure_factor"]
                  * factors["continuous_only_factor"]
                  * factors["point_in_time_factor"]
                  * factors["survivorship_factor"]
                  * factors["licence_factor"]
                  * factors["identity_factor"]
                  * factors["opacity_factor"]
                  * factors["history_factor"]
                  * breadth)
    points = cells_full * multiplier

    annualised = cost["annualised_cost_usd"]
    if annualised is None:
        score = None
        denominator = None
    else:
        denominator = max(float(annualised), C.FREE_COST_FLOOR_USD)
        score = points / denominator

    def per(numerator: Optional[float]) -> Optional[float]:
        if annualised is None or not numerator:
            return None
        return round(float(annualised) / float(numerator), 2)

    hard_fail = row.get("gate_state") in HARD_FAIL_STATES
    return {
        "dataset_id": row["dataset_id"],
        "provider": row["provider"],
        "lane": row["lane"],
        "gate_state": row["gate_state"],
        "cells_unlocked_full": cells_full,
        "cells_unlocked_ceiling": cells_ceiling,
        "native_markets_unlocked": markets_full,
        "asset_classes_unlocked": n_classes,
        "factors": factors,
        "breadth_factor": round(breadth, 4),
        "integrity_multiplier": round(multiplier, 6),
        "research_value_points": round(points, 4),
        "cost": cost,
        "score_denominator_usd": denominator,
        "cost_floor_applied": bool(
            annualised is not None and float(annualised) < C.FREE_COST_FLOOR_USD),
        "research_value_per_dollar_score": (None if score is None
                                            else round(score, 6)),
        "cost_per_r36_cell_unlocked": per(cells_full),
        "cost_per_native_market_unlocked": per(markets_full),
        "cost_per_year_of_history": per(factors["history_years"]),
        "cost_per_distinct_asset_class_unlocked": per(n_classes),
        "hard_fail": hard_fail,
        "rankable_as_investment": bool(
            row.get("gate_state") in INVESTABLE_STATES and cells_full > 0),
        "unrankable_reason": (
            None if row.get("gate_state") in INVESTABLE_STATES
            else "gate state %s is not an investable state"
                 % row.get("gate_state")),
    }


def build(unlock_map: dict) -> dict:
    """Score every candidate and rank the investable ones."""
    by_id = {u["dataset_id"]: u for u in unlock_map["rows"]}
    scored = [score_row(row, by_id.get(row["dataset_id"], {}))
              for row in _providers.rows()]

    investable = [s for s in scored if s["rankable_as_investment"]]
    ranked = sorted(investable,
                    key=lambda s: (-(s["research_value_per_dollar_score"] or 0.0),
                                   -s["research_value_points"],
                                   s["dataset_id"]))
    #: The honest counterfactual: what the ranking would look like if the score
    #: were allowed to override a hard gate. It is reported and never used.
    naive = sorted(scored,
                   key=lambda s: (-(s["research_value_per_dollar_score"] or 0.0),
                                  s["dataset_id"]))
    return {
        "rows": scored,
        "ranked_investable": [s["dataset_id"] for s in ranked],
        "ranked_investable_detail": ranked,
        "best": ranked[0]["dataset_id"] if ranked else None,
        "second": ranked[1]["dataset_id"] if len(ranked) > 1 else None,
        "third": ranked[2]["dataset_id"] if len(ranked) > 2 else None,
        "naive_ranking_ignoring_hard_gates": [s["dataset_id"] for s in naive],
        "hard_failed": sorted(s["dataset_id"] for s in scored if s["hard_fail"]),
        "cost_unknown": sorted(s["dataset_id"] for s in scored
                               if not s["cost"]["cost_known"]),
        "score_may_override_a_hard_gate": C.SCORE_MAY_OVERRIDE_A_HARD_GATE,
    }


def artifact(built: dict, *, campaign_id: str, created_at: str) -> dict:
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "formula": C.SCORE_FORMULA,
        "declared_factors": {
            "native_structure_factor": dict(C.NATIVE_STRUCTURE_FACTOR),
            "continuous_only_factor": C.CONTINUOUS_ONLY_FACTOR,
            "point_in_time_factor": dict(C.PIT_FACTOR),
            "survivorship_factor": dict(C.SURVIVORSHIP_FACTOR),
            "licence_factor": dict(C.LICENCE_FACTOR),
            "identity_factor": dict(C.IDENTITY_FACTOR),
            "opacity_factor": dict(C.OPACITY_FACTOR),
            "history_reference_years": C.HISTORY_REFERENCE_YEARS,
            "breadth_base": C.BREADTH_BASE,
            "breadth_per_asset_class": C.BREADTH_PER_ASSET_CLASS,
            "cost_amortisation_years": C.COST_AMORTISATION_YEARS,
            "free_cost_floor_usd": C.FREE_COST_FLOOR_USD,
        },
        "hard_fail_states": list(HARD_FAIL_STATES),
        "investable_states": list(INVESTABLE_STATES),
        "rows": built["rows"],
        "ranked_investable": built["ranked_investable"],
        "best": built["best"],
        "second": built["second"],
        "third": built["third"],
        "naive_ranking_ignoring_hard_gates":
            built["naive_ranking_ignoring_hard_gates"],
        "hard_failed": built["hard_failed"],
        "cost_unknown": built["cost_unknown"],
        "score_is_a_ranking_aid_not_an_optimiser":
            C.SCORE_IS_A_RANKING_AID_NOT_AN_OPTIMISER,
        "score_may_override_a_hard_gate": C.SCORE_MAY_OVERRIDE_A_HARD_GATE,
        "money_spent_usd": 0.0,
    }
    return r37.artifact_body(SCHEMA, payload)


def path_for(campaign_id: str = C.CAMPAIGN_ID):
    return r37.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    return r37.write_json(path_for(body["campaign_id"]), body)


def load(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    return r37.read_json(path_for(campaign_id))


__all__ = ["CALCULATION_OWNER", "HARD_FAIL_STATES", "INVESTABLE_STATES",
           "annualised_cost", "integrity_factors", "breadth_factor",
           "score_row", "build", "artifact", "freeze", "load", "path_for"]
