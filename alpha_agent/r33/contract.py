"""alpha_agent.r33.contract - the ONE Release 33 campaign contract owner.

Every budget, split date, cost rate, primary metric and qualification condition
below is a NUMBER in this module, frozen BEFORE any result is observed and
enforced by :mod:`alpha_agent.r33.registry` and :mod:`alpha_agent.r33.campaign`.

The single most important thing this file pins down is WHICH METRIC DECIDES.
A campaign that picks its primary metric after seeing the results has not run an
experiment; it has gone shopping. Each forecast target therefore names its
primary metric here, and the qualification gate reads that name rather than
choosing the flattering one.

If a material term changes, that is a NEW campaign with a NEW id, never an edit.
"""
from __future__ import annotations

import platform
import sys
from pathlib import Path
from typing import Optional

from .. import r33
from ..r31.contract import git_head  # ONE git-head owner, reused

CALCULATION_OWNER = "alpha_agent.r33.contract"
CONTRACT_SCHEMA = "r33_predictive_edge_contract/1"
ARTIFACT_NAME = "research_contract.json"

#: The campaign this release runs. A new id is the ONLY way to change a term.
CAMPAIGN_ID = "r33_predictive_edge_v2"

SUPERSEDED_GATE_DEFECT = "SUPERSEDED_GATE_DID_NOT_ENFORCE_A_FROZEN_TERM"

#: v1 is SUPERSEDED, not deleted. Its artifacts stay on disk as history.
#:
#: Nothing about the MEASUREMENTS changed between v1 and v2: same contract
#: terms, same seeds, same configurations, same selection rule, same judge. Only
#: the QUALIFICATION GATE changed, and it changed in the direction that can only
#: remove a qualification. That is why v2 is required to reproduce every v1
#: per-candidate number exactly - and why it is asserted rather than assumed.
SUPERSEDED_CAMPAIGNS = {
    "r33_predictive_edge_v1": {
        "state": SUPERSEDED_GATE_DEFECT,
        "produced_a_verdict": True,
        "verdict_reproduced_in_v2": "R33_NO_PREDICTIVE_EDGE",
        "defects": [
            "the contract declares that a candidate with fewer than "
            "MIN_SCORED_FORECAST_DATES scored dates in a segment cannot carry "
            "a verdict for that segment. The gate never read that frozen term, "
            "so every lockbox result - all of which rested on 23 scored dates "
            "against a declared minimum of 24 - was allowed to satisfy the two "
            "predictive conditions",
            "subperiod stability returned no flag when it had too few "
            "observations to measure, so 'not dependent on a single subperiod' "
            "passed VACUOUSLY for exactly the candidates whose stability could "
            "not be checked",
        ],
        "correction": "v2 enforces the frozen minimum at the qualification "
                      "gate and makes an unmeasurable stability check FAIL "
                      "CLOSED",
        "gate_change_direction": "STRICTLY_TIGHTENING",
        "measurements_unchanged": True,
        "measurements_must_reproduce_exactly": True,
        "selection_rule_unchanged": True,
        "lockbox_reopened": False,
        "note": "the finalist selection rule, the seeds and the judge are "
                "identical, so v2 selects the SAME finalists and gives each "
                "the same single lockbox execution. No candidate was revised "
                "after seeing a lockbox result and none was resubmitted.",
    },
}

#: Superseded evidence may be READ as history and may never select anything.
SUPERSEDED_EVIDENCE_RULES = {
    "may_select_hyperparameters": False,
    "may_select_finalists": False,
    "may_influence_the_lockbox": False,
    "may_contribute_to_a_qualification_verdict": False,
    "may_reduce_the_multiple_testing_denominator": False,
    "is_preserved_on_disk": True,
}

# --------------------------------------------------------------------------- #
# Inherited state
# --------------------------------------------------------------------------- #
R32_CAMPAIGN_ID = "r32_pnl_opportunity_frontier_v4"
R32_VERDICT = "R32_ZERO_COST_OPPORTUNITY_FRONTIER_EXHAUSTED"
R32_SYSTEM_RESULT = "PASS"
R32_ALPHA_RESULT = "FAIL"
R32_EXECUTED_HYPOTHESES = 104
R32_QUALIFIED_SLEEVES = 0

#: Release 32's evidence may be READ as history and may never select anything
#: here. Its hypotheses do NOT reduce this campaign's denominator either: a
#: prior campaign's failures neither excuse nor pay for this campaign's tests.
INHERITED_EVIDENCE_RULES = {
    "may_select_r33_hyperparameters": False,
    "may_select_r33_finalists": False,
    "may_influence_the_lockbox": False,
    "may_contribute_to_a_qualification_verdict": False,
    "may_reduce_the_multiple_testing_denominator": False,
    "is_preserved_on_disk": True,
}

# --------------------------------------------------------------------------- #
# The two lanes
# --------------------------------------------------------------------------- #
LANE_A = "BROAD_CROSS_MARKET_PREDICTION"
LANE_B = "PIT_MACRO_POSITIONING_REGIME"
LANE_C = "EXPECTATION_CHANGE_ANALYST_REVISIONS"
LANES = (LANE_A, LANE_B, LANE_C)

#: Lane C may not block A or B. If no genuine provider sample exists it persists
#: a readiness package and the campaign continues.
LANE_C_MAY_BLOCK_CAMPAIGN = False
LANE_C_SYNTHETIC_DATA_ADMISSIBLE = False

# --------------------------------------------------------------------------- #
# Forecast horizons and implementation timing
# --------------------------------------------------------------------------- #
HORIZONS = (5, 20, 60)

#: Decide from information through the close of session t; ENTER at the close of
#: session t+1; measure the return from t+1 to t+1+h.
#:
#: This one session of slack is not decoration. The universe spans Tokyo, London
#: and New York closes stamped with the same calendar date, so a signal built
#: from "day t closes" across markets would otherwise be able to trade on moves
#: that happened after some of those markets had already shut. That is a real
#: lead-lag effect and it is NOT capturable - nobody can buy the Nikkei at
#: yesterday's Tokyo close. It also covers same-day publication ambiguity in the
#: daily yield series, which deliver one session later than the price series.
IMPLEMENTATION_LAG_SESSIONS = 1

#: Forecast dates are struck every ``h`` sessions, so successive observations of
#: an ``h``-session return DO NOT OVERLAP. Overlapping forecasts inflate the
#: effective sample by a factor of ``h`` and make every t-statistic a fiction.
NON_OVERLAPPING_FORECAST_DATES = True

#: Minimum trailing sessions before any feature is computed for a market.
MIN_HISTORY_SESSIONS = 252

#: A candidate with fewer scored forecast dates than this in a segment cannot
#: carry a verdict for that segment.
MIN_SCORED_FORECAST_DATES = 24

#: A market must clear this many usable sessions to enter the universe.
MIN_MARKET_SESSIONS = 2000

# --------------------------------------------------------------------------- #
# Chronological partition
# --------------------------------------------------------------------------- #
#: No random split, ever. Three contiguous blocks in time, with an embargo
#: between them of ``horizon + EMBARGO_EXTRA_SESSIONS`` sessions so that the
#: last training label cannot overlap the first evaluation window.
PANEL_START = "1995-01-03"
DISCOVERY_END = "2012-12-31"
VALIDATION_START = "2013-01-01"
VALIDATION_END = "2020-12-31"
LOCKBOX_START = "2021-01-01"

EMBARGO_EXTRA_SESSIONS = 5

SEGMENT_DISCOVERY = "DISCOVERY"
SEGMENT_VALIDATION = "VALIDATION"
SEGMENT_LOCKBOX = "LOCKBOX"
SEGMENTS = (SEGMENT_DISCOVERY, SEGMENT_VALIDATION, SEGMENT_LOCKBOX)

#: TRUE_FORWARD is the operational forward-evidence store. It is SEPARATE,
#: immutable and is not read or written here.
TRUE_FORWARD_IS_SEPARATE = True
TRUE_FORWARD_CONTAMINATED = False

# --------------------------------------------------------------------------- #
# Forecast targets and their PRIMARY metrics - declared before validation
# --------------------------------------------------------------------------- #
TARGET_RETURN = "EXCESS_RETURN"
TARGET_SIGN = "POSITIVE_RETURN_PROBABILITY"
TARGET_VOLATILITY = "REALISED_VOLATILITY"
TARGET_CROSS_SECTION = "CROSS_SECTIONAL_RANK"
TARGETS = (TARGET_RETURN, TARGET_SIGN, TARGET_VOLATILITY, TARGET_CROSS_SECTION)

#: The metric that DECIDES, per target. Others are reported as diagnostics and
#: may never promote a candidate that its primary metric rejects.
PRIMARY_METRIC = {
    TARGET_RETURN: "OOS_R2",
    TARGET_SIGN: "LOG_LOSS_SKILL",
    TARGET_VOLATILITY: "QLIKE_SKILL",
    TARGET_CROSS_SECTION: "RANK_IC",
}

#: The pre-registered forecast baseline each target is scored against.
FORECAST_BASELINE = {
    TARGET_RETURN: "UNCONDITIONAL_TRAINING_MEAN",
    TARGET_SIGN: "TRAINING_BASE_RATE",
    TARGET_VOLATILITY: "TRAILING_REALISED_VOLATILITY",
    TARGET_CROSS_SECTION: "ZERO_FORECAST",
}

# --------------------------------------------------------------------------- #
# Model families and configuration budgets - CEILINGS, not targets
# --------------------------------------------------------------------------- #
FAMILY_BASELINE = "BASELINE_TRANSPARENT"
FAMILY_POOLED = "POOLED_STATISTICAL"
FAMILY_REGIME = "REGIME"
FAMILY_COMBINED = "COMBINED_REFINEMENT"
FAMILIES = (FAMILY_BASELINE, FAMILY_POOLED, FAMILY_REGIME, FAMILY_COMBINED)

MAX_CONFIGS = {
    FAMILY_BASELINE: 40,
    FAMILY_POOLED: 60,
    FAMILY_REGIME: 30,
    FAMILY_COMBINED: 40,
}
MAX_CONFIGS_TOTAL = 170

#: No adaptive unlimited hyperparameter search, and no N+1 campaign after
#: exhaustion. Every configuration is enumerated from a frozen grid.
ADAPTIVE_SEARCH_ALLOWED = False

#: Deep networks and transformers are deliberately OUT OF SCOPE. The question
#: this release answers first is whether structured point-in-time tabular
#: information contains signal that strong lower-complexity models can find.
DEEP_LEARNING_IN_SCOPE = False

# --------------------------------------------------------------------------- #
# Pooling - the core Lane A hypothesis
# --------------------------------------------------------------------------- #
POOLING_INDEPENDENT = "INDEPENDENT_PER_MARKET"
POOLING_FULL = "FULLY_POOLED"
POOLING_GROUPED = "ECONOMIC_GROUP_POOLED"
POOLING_HIERARCHICAL = "HIERARCHICAL_SHRINKAGE"
POOLINGS = (POOLING_INDEPENDENT, POOLING_FULL, POOLING_GROUPED,
            POOLING_HIERARCHICAL)

#: A result that depends entirely on one market is not broad predictive edge,
#: so leave-market-out is a REQUIRED diagnostic rather than an optional extra.
LEAVE_MARKET_OUT_REQUIRED = True
LEAVE_ASSET_CLASS_OUT_REQUIRED = True

# --------------------------------------------------------------------------- #
# Multiple testing
# --------------------------------------------------------------------------- #
FDR_Q = 0.10
BOOTSTRAP_RESAMPLES = 2000
BOOTSTRAP_BLOCK_MEAN = 10.0
BOOTSTRAP_SEED = 20330821
MODEL_SEED = 33

#: EVERY executed configuration enters the denominator, including the ones that
#: failed, were abandoned or were resource-infeasible. A denominator that counts
#: only survivors is not a correction, it is a second selection.
DENOMINATOR_COUNTS_ALL_EXECUTED = True

# --------------------------------------------------------------------------- #
# Economics
# --------------------------------------------------------------------------- #
#: Cost is charged on TRADED NOTIONAL (``sum |dw|``, sells AND buys), never on
#: one-way turnover. Release 31 shipped that bug and understated every cost by
#: roughly half.
COST_BASE = "TRADED_NOTIONAL"

#: One-way cost per side, in basis points, by asset class. These are the CENTRAL
#: assumption; the campaign additionally reports every candidate at each
#: multiplier in ``COST_SENSITIVITY_MULTIPLIERS``, because a result that only
#: survives its most optimistic cost assumption has not survived.
COST_PER_SIDE_BPS = {
    "EQUITY_INDEX": 3.0,
    "GOVERNMENT_BOND": 2.0,
    "CREDIT_BOND": 5.0,
    "COMMODITY": 8.0,
    "PRECIOUS_METAL": 5.0,
    "FX": 3.0,
}
COST_SENSITIVITY_MULTIPLIERS = (0.5, 1.0, 2.0, 4.0)

#: The risk-free leg. Cash is a real asset choice and must earn an observable
#: point-in-time yield: ``%IRX`` is the 13-week T-bill yield, quoted daily.
CASH_YIELD_SYMBOL = "%IRX"

#: The benchmark for the risk-matched control.
BENCHMARK_SYMBOL = "$SPXTR"

#: The control that decides economic skill. R32 proved the alternatives fail:
#: excess over CASH measures exposure, not skill - every one of its six sleeves
#: beat cash and not one beat a volatility-matched mix.
ECONOMIC_CONTROL = "VOLATILITY_MATCHED_BENCHMARK_CASH_MIX"
CONTROLS = ("CASH", "BENCHMARK_BUY_AND_HOLD", ECONOMIC_CONTROL,
            "CANONICAL_TIME_SERIES_MOMENTUM", "EQUAL_RISK_CROSS_MARKET")

#: Annualised volatility the research book is scaled to, so that candidates are
#: compared at comparable risk rather than by who levered hardest.
VOLATILITY_TARGET_ANNUAL = 0.10
VOLATILITY_TARGET_SENSITIVITY = (0.05, 0.10, 0.15)

#: No leverage is available to this paper book, so the control's benchmark
#: weight is capped at 1.0 and the research book's gross exposure is capped.
MAX_GROSS_EXPOSURE = 2.0
LEVERAGE_AVAILABLE = False

# --------------------------------------------------------------------------- #
# Return definition - declared, because the panel is heterogeneous
# --------------------------------------------------------------------------- #
#: Equity indices are PRICE indices: they exclude dividends. Bond indices are
#: TOTAL RETURN: they include coupon. FX spot excludes the interest
#: differential. These differences are real, they are NOT corrected here, and
#: they bias a naive long-only cross-market comparison towards bonds.
#:
#: The campaign handles this by making the primary economic construction
#: cross-sectional and zero-mean within asset class, where a constant per-market
#: drift offset largely cancels, and by MEASURING the equity dividend gap from
#: the owned ``$SPX`` / ``$SPXTR`` pair rather than assuming a number for it.
RETURN_DEFINITION_HETEROGENEOUS = True
EQUITY_INDICES_EXCLUDE_DIVIDENDS = True
BOND_INDICES_INCLUDE_COUPON = True
FX_SPOT_EXCLUDES_CARRY = True
DIVIDEND_GAP_MEASURED_FROM = ("$SPX", "$SPXTR")

#: Every non-USD market is converted to USD with the owned Forex Spot series
#: using the vendor's authoritative currency field, because the paper book is
#: USD-denominated and an unhedged foreign holding earns the currency move too.
RETURNS_ARE_USD_CONVERTED = True

# --------------------------------------------------------------------------- #
# Implementability - the distinction Release 33 may not blur
# --------------------------------------------------------------------------- #
SIGNAL_RESEARCH_VALID = "SIGNAL_RESEARCH_VALID"
FUTURES_IMPLEMENTABILITY_PROVEN = "FUTURES_IMPLEMENTABILITY_PROVEN"

#: The owned Continuous Futures entitlement is ONE market. Roll yield, contract
#: selection, execution price and futures transaction-cost semantics are NOT
#: supported by the owned data for the rest of the universe, so no result in
#: this release may claim futures implementability.
FUTURES_IMPLEMENTABILITY_CLAIMABLE = False
UNIVERSE_IMPLEMENTABILITY_STATE = SIGNAL_RESEARCH_VALID

# --------------------------------------------------------------------------- #
# Lockbox
# --------------------------------------------------------------------------- #
MAX_LOCKBOX_FINALISTS = 8
MAX_LOCKBOX_PER_FAMILY = 3
MAX_LOCKBOX_ACCESSES_PER_FINALIST = 1
RETUNING_AFTER_LOCKBOX_ALLOWED = False

# --------------------------------------------------------------------------- #
# The qualification gate
# --------------------------------------------------------------------------- #
#: ALL of these must hold on frozen evidence. They are evaluated by
#: ``alpha_agent.r33.campaign.qualify`` and each one is reported with its own
#: boolean so a reader can see exactly which condition failed.
QUALIFICATION_CONDITIONS = (
    "positive_oos_predictive_improvement_vs_baseline",
    "predictive_improvement_same_sign_in_validation_and_lockbox",
    "positive_after_cost_excess_vs_risk_matched_control",
    "positive_after_cost_utility_improvement",
    "survives_multiple_testing_procedure",
    "no_severe_parameter_cliff",
    "not_dependent_on_a_single_market",
    "not_dependent_on_a_single_subperiod",
    "acceptable_cost_sensitivity",
    "point_in_time_integrity_pass",
    "lockbox_accessed_exactly_once",
)

#: A candidate with attractive PnL but no measurable predictive improvement is
#: NOT predictive alpha. A candidate that predicts better but cannot produce
#: superior after-cost economics is NOT investable alpha. Both are required.
PREDICTIVE_IMPROVEMENT_REQUIRED = True
ECONOMIC_IMPROVEMENT_REQUIRED = True

#: Thresholds for the two "not dependent on one thing" conditions.
MAX_SINGLE_MARKET_CONTRIBUTION = 0.50
MAX_SINGLE_SUBPERIOD_CONTRIBUTION = 0.75
#: A parameter cliff: the median neighbouring configuration must retain this
#: fraction of the candidate's economic result.
MIN_NEIGHBOUR_RETENTION = 0.30

# --------------------------------------------------------------------------- #
# Terminal verdicts
# --------------------------------------------------------------------------- #
VERDICT_QUALIFIED = "R33_ALPHA_QUALIFIED"
VERDICT_NO_EDGE = "R33_NO_PREDICTIVE_EDGE"
VERDICT_PIT_BLOCKED = "R33_PIT_INFORMATION_BLOCKED"
VERDICT_DATA_BLOCKED = "R33_DATA_SOURCE_BLOCKED"
VERDICT_BUDGET = "R33_RESOURCE_BUDGET_EXHAUSTED"
PRIMARY_VERDICTS = (VERDICT_QUALIFIED, VERDICT_NO_EDGE, VERDICT_PIT_BLOCKED,
                    VERDICT_DATA_BLOCKED, VERDICT_BUDGET)

RESULT_PASS = "PASS"
RESULT_FAIL = "FAIL"

#: ALPHA_RESULT may be PASS only with R33_ALPHA_QUALIFIED. Enforced in
#: ``campaign.build_verdict`` and asserted by the release tests.
ALPHA_PASS_REQUIRES = VERDICT_QUALIFIED


# --------------------------------------------------------------------------- #
# Contract construction
# --------------------------------------------------------------------------- #
def build(*, campaign_id: str = CAMPAIGN_ID, created_at: str,
          repo: Optional[Path] = None) -> dict:
    """Build the immutable campaign contract body."""
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "created_at": created_at,
        "mission": (
            "find measurable out-of-sample predictive edge; infrastructure, "
            "documentation and a completed campaign are NOT an alpha success"),
        "inherited_release32": {
            "campaign_id": R32_CAMPAIGN_ID,
            "verdict": R32_VERDICT,
            "system_result": R32_SYSTEM_RESULT,
            "alpha_result": R32_ALPHA_RESULT,
            "executed_hypotheses": R32_EXECUTED_HYPOTHESES,
            "qualified_sleeves": R32_QUALIFIED_SLEEVES,
            "rerun": False,
        },
        "inherited_evidence_rules": INHERITED_EVIDENCE_RULES,
        "superseded_campaigns": SUPERSEDED_CAMPAIGNS,
        "superseded_evidence_rules": SUPERSEDED_EVIDENCE_RULES,
        "lanes": {
            "lane_a": LANE_A,
            "lane_b": LANE_B,
            "lane_c": LANE_C,
            "lane_c_may_block_campaign": LANE_C_MAY_BLOCK_CAMPAIGN,
            "lane_c_synthetic_data_admissible":
                LANE_C_SYNTHETIC_DATA_ADMISSIBLE,
        },
        "forecast": {
            "horizons_sessions": list(HORIZONS),
            "implementation_lag_sessions": IMPLEMENTATION_LAG_SESSIONS,
            "non_overlapping_forecast_dates": NON_OVERLAPPING_FORECAST_DATES,
            "min_history_sessions": MIN_HISTORY_SESSIONS,
            "min_scored_forecast_dates": MIN_SCORED_FORECAST_DATES,
            "min_market_sessions": MIN_MARKET_SESSIONS,
            "targets": list(TARGETS),
            "primary_metric": dict(PRIMARY_METRIC),
            "forecast_baseline": dict(FORECAST_BASELINE),
        },
        "partition": {
            "panel_start": PANEL_START,
            "discovery_end": DISCOVERY_END,
            "validation_start": VALIDATION_START,
            "validation_end": VALIDATION_END,
            "lockbox_start": LOCKBOX_START,
            "embargo_extra_sessions": EMBARGO_EXTRA_SESSIONS,
            "segments": list(SEGMENTS),
            "random_split_allowed": False,
            "true_forward_is_separate": TRUE_FORWARD_IS_SEPARATE,
            "true_forward_contaminated": TRUE_FORWARD_CONTAMINATED,
        },
        "models": {
            "families": list(FAMILIES),
            "max_configs": dict(MAX_CONFIGS),
            "max_configs_total": MAX_CONFIGS_TOTAL,
            "adaptive_search_allowed": ADAPTIVE_SEARCH_ALLOWED,
            "deep_learning_in_scope": DEEP_LEARNING_IN_SCOPE,
            "poolings": list(POOLINGS),
            "leave_market_out_required": LEAVE_MARKET_OUT_REQUIRED,
            "leave_asset_class_out_required": LEAVE_ASSET_CLASS_OUT_REQUIRED,
            "model_seed": MODEL_SEED,
        },
        "multiple_testing": {
            "fdr_q": FDR_Q,
            "bootstrap_resamples": BOOTSTRAP_RESAMPLES,
            "bootstrap_block_mean": BOOTSTRAP_BLOCK_MEAN,
            "bootstrap_seed": BOOTSTRAP_SEED,
            "denominator_counts_all_executed": DENOMINATOR_COUNTS_ALL_EXECUTED,
        },
        "economics": {
            "cost_base": COST_BASE,
            "cost_per_side_bps": dict(COST_PER_SIDE_BPS),
            "cost_sensitivity_multipliers": list(COST_SENSITIVITY_MULTIPLIERS),
            "cash_yield_symbol": CASH_YIELD_SYMBOL,
            "benchmark_symbol": BENCHMARK_SYMBOL,
            "economic_control": ECONOMIC_CONTROL,
            "controls": list(CONTROLS),
            "volatility_target_annual": VOLATILITY_TARGET_ANNUAL,
            "volatility_target_sensitivity": list(VOLATILITY_TARGET_SENSITIVITY),
            "max_gross_exposure": MAX_GROSS_EXPOSURE,
            "leverage_available": LEVERAGE_AVAILABLE,
        },
        "return_definition": {
            "heterogeneous": RETURN_DEFINITION_HETEROGENEOUS,
            "equity_indices_exclude_dividends":
                EQUITY_INDICES_EXCLUDE_DIVIDENDS,
            "bond_indices_include_coupon": BOND_INDICES_INCLUDE_COUPON,
            "fx_spot_excludes_carry": FX_SPOT_EXCLUDES_CARRY,
            "dividend_gap_measured_from": list(DIVIDEND_GAP_MEASURED_FROM),
            "returns_are_usd_converted": RETURNS_ARE_USD_CONVERTED,
        },
        "implementability": {
            "universe_state": UNIVERSE_IMPLEMENTABILITY_STATE,
            "futures_implementability_claimable":
                FUTURES_IMPLEMENTABILITY_CLAIMABLE,
            "signal_research_valid": SIGNAL_RESEARCH_VALID,
            "futures_implementability_proven": FUTURES_IMPLEMENTABILITY_PROVEN,
        },
        "lockbox": {
            "max_finalists": MAX_LOCKBOX_FINALISTS,
            "max_per_family": MAX_LOCKBOX_PER_FAMILY,
            "max_accesses_per_finalist": MAX_LOCKBOX_ACCESSES_PER_FINALIST,
            "retuning_after_lockbox_allowed": RETUNING_AFTER_LOCKBOX_ALLOWED,
        },
        "qualification": {
            "conditions": list(QUALIFICATION_CONDITIONS),
            "predictive_improvement_required": PREDICTIVE_IMPROVEMENT_REQUIRED,
            "economic_improvement_required": ECONOMIC_IMPROVEMENT_REQUIRED,
            "max_single_market_contribution": MAX_SINGLE_MARKET_CONTRIBUTION,
            "max_single_subperiod_contribution":
                MAX_SINGLE_SUBPERIOD_CONTRIBUTION,
            "min_neighbour_retention": MIN_NEIGHBOUR_RETENTION,
        },
        "verdicts": {
            "primary": list(PRIMARY_VERDICTS),
            "alpha_pass_requires": ALPHA_PASS_REQUIRES,
            "reports_system_result_and_alpha_result_separately": True,
        },
        "environment": {
            "git_head": git_head(repo),
            "python": sys.version.split()[0],
            "platform": platform.platform(),
        },
        "research_root": str(r33.research_root()),
    }
    body = r33.artifact_body(CONTRACT_SCHEMA, payload)
    body["contract_hash"] = r33.sha(
        {k: v for k, v in payload.items() if k != "environment"})
    return body


def path_for(campaign_id: str = CAMPAIGN_ID) -> Path:
    return r33.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(contract: dict) -> Path:
    return r33.write_json(path_for(contract["campaign_id"]), contract)


def load(campaign_id: str = CAMPAIGN_ID) -> Optional[dict]:
    return r33.read_json(path_for(campaign_id))


def verify(contract: dict) -> dict:
    """Recompute the contract hash and report drift."""
    payload = {k: v for k, v in contract.items()
               if k not in ("schema", "release", "safety_block",
                            "contract_hash", "environment")}
    recomputed = r33.sha(payload)
    return {"declared": contract.get("contract_hash"),
            "recomputed": recomputed,
            "stable": recomputed == contract.get("contract_hash")}
