"""alpha_agent.r34.contract - the ONE Release 34 campaign contract owner.

Every budget, split date, cost rate, calibration family, sizing rule, gate
threshold and qualification condition below is a NUMBER in this module, frozen
BEFORE any economic result is observed.

Three terms in this file exist specifically because Release 33 got them wrong,
and each is written down here so it cannot be chosen after the fact:

**The horizon-normalised evidence score.** R33 ranked its finalists on raw
primary-metric magnitude, and a 60-session rank IC is mechanically larger than a
5-session one while resting on a twelfth as many observations. Every R33
finalist was therefore biased toward ``h=60`` by arithmetic rather than by
evidence. :data:`HNES_FORMULA` is declared here, before evaluation, and
:mod:`alpha_agent.r34.horizon` is the only module allowed to compute it.

**The concentration gate.** R33's five lockbox finalists showed positive
after-cost excess and leave-one-market-out attributed all of it to ``TRYUSD``:
removing one market moved mean excess from ``+0.0041`` to ``-0.0069``. The
thresholds in :data:`MAX_SINGLE_INSTRUMENT_PNL_SHARE` and its neighbours are
frozen now so that no threshold can be invented once a finalist's concentration
is known.

**The absence of a fresh lockbox.** R31, R32 and R33 have all used evidence
through 2026 for selection. There is therefore NO untouched historical block
left, and :data:`FRESH_UNSEEN_EVIDENCE_EXISTS` says so before the campaign runs
rather than after it produces a flattering number. That fact CAPS the reachable
verdict at :data:`VERDICT_NEEDS_FORWARD` and makes ``ALPHA_RESULT = PASS``
unreachable in this release by construction. That is the honest position, and it
is declared rather than discovered.

If a material term changes, that is a NEW campaign with a NEW id, never an edit.
"""
from __future__ import annotations

import platform
import sys
from pathlib import Path
from typing import Optional

from .. import r34
from ..r31.contract import git_head  # ONE git-head owner, reused
from ..r33 import contract as _r33

CALCULATION_OWNER = "alpha_agent.r34.contract"
CONTRACT_SCHEMA = "r34_prediction_to_pnl_contract/1"
ARTIFACT_NAME = "research_contract.json"

#: The campaign this release runs. A new id is the ONLY way to change a term.
CAMPAIGN_ID = "r34_prediction_to_pnl_v2"

SUPERSEDED_GATE_DEFECT = "SUPERSEDED_GUARD_COULD_NOT_FAIL"

#: v1 is SUPERSEDED, not deleted. Its artifacts stay on disk as history.
#:
#: The defect was found by reading v1's own finalist table: three finalists that
#: differ in their calibration and their sizing rule reported IDENTICAL
#: economics to seven significant figures. That cannot happen if the lanes do
#: anything, and it did not - the multi-horizon conviction override was built
#: ONCE from the winning lane settings and then reused for every neighbour, so
#: the calibration and sizing neighbours were the same book under a different
#: name. The parameter-cliff gate was therefore VACUOUS for two of the four
#: lanes it is supposed to probe, in exactly the way Release 33's v1 stability
#: check passed vacuously for the candidates it could not measure.
#:
#: v2 also closes a hole that v1 did not happen to fall into: a book that holds
#: almost nothing and trades almost never has an after-cost excess of
#: approximately zero, which beats every genuinely negative candidate. Cash is a
#: legitimate ALLOCATION and abstention is a legitimate ANSWER, but a book that
#: takes no positions has converted no prediction and may not qualify as
#: conversion alpha.
#:
#: Both changes can only REMOVE a qualification. No measurement changed, no
#: threshold was loosened, and the universe, the frozen models, the partition
#: and the seeds are identical.
SUPERSEDED_CAMPAIGNS = {
    "r34_prediction_to_pnl_v1": {
        "state": SUPERSEDED_GATE_DEFECT,
        "produced_a_verdict": True,
        "verdict": "R34_PREDICTION_DOES_NOT_CONVERT",
        "defects": [
            "the multi-horizon conviction override was built once from the "
            "winning lane settings and reused for every finalist neighbour, so "
            "the calibration and sizing neighbours produced a byte-identical "
            "book and the parameter-cliff gate could not fail on those lanes",
            "a book that takes almost no position and almost never trades has "
            "an after-cost excess of approximately zero, which ranks above "
            "every genuinely negative candidate; nothing barred such a book "
            "from being selected as the best finalist",
        ],
        "correction": "v2 rebuilds the conviction override per finalist, marks "
                      "any neighbour whose weights do not actually differ as "
                      "NO_EFFECT and excludes it from the retention median, and "
                      "adds a frozen minimum-engagement condition",
        "gate_change_direction": "STRICTLY_TIGHTENING",
        "measurements_unchanged": True,
        "universe_unchanged": True,
        "models_unchanged": True,
        "partition_unchanged": True,
        "seeds_unchanged": True,
        "is_preserved_on_disk": True,
    },
}

#: Superseded evidence may be READ as history and may never select anything.
SUPERSEDED_EVIDENCE_RULES = {
    "may_select_hyperparameters": False,
    "may_select_finalists": False,
    "may_contribute_to_a_qualification_verdict": False,
    "may_reduce_the_multiple_testing_denominator": False,
    "is_preserved_on_disk": True,
}

# --------------------------------------------------------------------------- #
# Inherited state - Release 33 is HYPOTHESIS-GENERATING EVIDENCE ONLY
# --------------------------------------------------------------------------- #
R33_CAMPAIGN_ID = _r33.CAMPAIGN_ID
R33_VERDICT = _r33.VERDICT_NO_EDGE
R33_SYSTEM_RESULT = "PASS"
R33_ALPHA_RESULT = "FAIL"
R33_DENOMINATOR = 105
R33_LOCKBOX_ACCESSES = 8
R33_QUALIFIED = 0

#: R33 is frozen. It is not rerun, its lockbox is not reopened, its candidates
#: are not retuned and its verdict is not revisited. It may motivate a
#: hypothesis here and may never select anything here.
R33_EVIDENCE_RULES = {
    "may_be_rerun": False,
    "lockbox_may_be_reopened": False,
    "candidates_may_be_retuned": False,
    "may_select_r34_hyperparameters": False,
    "may_select_r34_finalists": False,
    "may_contribute_to_a_qualification_verdict": False,
    "may_reduce_the_multiple_testing_denominator": False,
    "may_generate_hypotheses": True,
    "is_preserved_on_disk": True,
}

#: The eight R33 observations that motivate this release. Recorded so a reader
#: can check that the design answers them rather than repeating them.
R33_MOTIVATING_OBSERVATIONS = (
    "predictive rank structure exists",
    "volatility is materially forecastable",
    "naive forecast-to-position conversion failed",
    "the strongest apparent result concentrated in TRYUSD",
    "broad continuous-futures implementability did not exist",
    "the owned Norgate Continuous Futures entitlement contains only &ES",
    "the 66-market research panel was SIGNAL_RESEARCH_VALID, not "
    "FUTURES_IMPLEMENTABILITY_PROVEN",
    "raw primary-metric magnitude biased all R33 finalists toward h=60",
)

# --------------------------------------------------------------------------- #
# Lane A - the implementable universe
# --------------------------------------------------------------------------- #
IMPLEMENTABLE_RESEARCH_UNIVERSE = "IMPLEMENTABLE_RESEARCH_UNIVERSE"
SIGNAL_RESEARCH_VALID = "SIGNAL_RESEARCH_VALID"
UNIVERSE_BLOCKED = "IMPLEMENTABLE_UNIVERSE_BLOCKED"

#: The universe may carry the IMPLEMENTABLE label ONLY if every admitted
#: instrument is an exchange-listed security with total-return adjusted prices.
#: An economic index, a spot FX rate or a non-investable series may never enter
#: the primary economic portfolio; R33's apparent edge was a spot-FX trade.
IMPLEMENTABLE_REQUIRES_EXCHANGE_TRADED_SECURITY = True
IMPLEMENTABLE_REQUIRES_TOTAL_RETURN_PRICES = True
NON_INVESTABLE_SERIES_MAY_ENTER_PORTFOLIO = False
#: Explicitly barred from the primary economic portfolio. They may remain
#: EXPLANATORY FEATURES (global state) and nothing else.
BARRED_FROM_PORTFOLIO = ("TRYUSD", "RAW_FX_SPOT", "NON_INVESTABLE_INDEX")

TARGET_INSTRUMENT_COUNT = (20, 50)
MIN_INSTRUMENT_COUNT = 20
MIN_ASSET_CLASS_COUNT = 6

#: Instrument admission rules. Every one is MEASURED from delivered data and
#: every candidate records the rule that decided it.
MIN_INSTRUMENT_SESSIONS = 1000
#: Median dollar volume, in USD, required at admission - and, separately and
#: bindingly, on a TRAILING basis at every individual decision date.
#:
#: The floor is set from what this book could actually trade, and the number is
#: fixed here before any economic result exists. The research book is $100,000
#: with a 20 % single-instrument cap, so its largest possible position is
#: $20,000: against a $5m daily volume that is 0.4 % of a day's trading, which
#: the spread-plus-impact cost tiers below price rather than forbid. A $50m
#: floor was the first draft and it deleted every commodity, every currency,
#: emerging-market debt and international treasuries from the universe - it
#: would have narrowed a multi-asset question to an equity one by an
#: administrative choice rather than by evidence.
#:
#: Illiquidity is therefore PRICED, not pretended away: the thin tiers below pay
#: 15 and 30 basis points a side, ten and twenty times the mega-cap tier.
MIN_MEDIAN_DOLLAR_VOLUME = 5e6
LIQUIDITY_WINDOW_SESSIONS = 252
#: Admission uses the full-history median, which is generous. The BINDING
#: constraint is per-date: an instrument is tradable on a date only if its
#: trailing-window median dollar volume clears the floor on THAT date, so an
#: instrument that was liquid for only part of its life is traded only then.
LIQUIDITY_IS_POINT_IN_TIME = True
#: A price that repeats more than this fraction of sessions is not trading.
MAX_ZERO_RETURN_FRACTION = 0.20
MAX_STALE_SESSIONS = 10

#: Exchange-traded NOTES are excluded from the primary universe: an ETN is an
#: unsecured obligation of its issuer, so its return carries issuer credit risk
#: that the price series does not show and that this campaign does not model.
EXCLUDE_EXCHANGE_TRADED_NOTES = True
#: Leveraged and inverse products are excluded: their compounding path makes a
#: daily-rebalanced product a different economic object from the exposure it
#: names, and the campaign's sizing layer already controls leverage explicitly.
EXCLUDE_LEVERAGED_AND_INVERSE = True
#: Currency-HEDGED share classes are excluded as duplicates of the unhedged
#: exposure, which is the one the paper book would actually hold.
EXCLUDE_CURRENCY_HEDGED = True

#: Survivorship. The candidate pool is enumerated from the live AND the
#: DELISTED vendor databases. A universe assembled from products that happen to
#: exist today is a hindsight portfolio, and this estate has measured that bias
#: at 2.74x and 3.42x in two earlier releases.
UNIVERSE_INCLUDES_DELISTED_CANDIDATES = True
DELISTED_INSTRUMENT_IS_FORCED_TO_CASH = True

# --------------------------------------------------------------------------- #
# Lane B - the FROZEN predictive families
# --------------------------------------------------------------------------- #
#: Carried forward from Release 33 and refit on the new instrument returns
#: because the instrument domain changed. NO new predictor search runs here.
NEW_PREDICTOR_SEARCH_ALLOWED = False
FEATURE_FAMILIES_FROZEN_FROM = _r33.CAMPAIGN_ID

MODEL_TSMOM = "TRANSPARENT_TIME_SERIES_MOMENTUM"
MODEL_RIDGE = "POOLED_RIDGE"
MODEL_ELASTIC_NET = "POOLED_ELASTIC_NET"
MODEL_HIERARCHICAL = "HIERARCHICAL_SHRINKAGE"
MODEL_VOLATILITY = "VOLATILITY_FORECAST"
FORECAST_MODELS = (MODEL_TSMOM, MODEL_RIDGE, MODEL_ELASTIC_NET,
                   MODEL_HIERARCHICAL)
#: The volatility forecast is a RISK input to sizing, not a return candidate.
#: R33 measured real QLIKE skill and none of it was economically convertible on
#: its own; here it earns its keep by sizing positions or not at all.
VOLATILITY_MODEL_IS_A_RISK_INPUT = True

RIDGE_ALPHAS = (1.0, 10.0)
ELASTIC_NET_ALPHAS = (0.01, 0.1)
ELASTIC_NET_L1_RATIO = 0.5
HIERARCHICAL_SHRINK = (0.5,)
HIERARCHICAL_GROUP = "ASSET_CLASS"

HORIZONS = (5, 20, 60)
#: The horizon the conversion lanes are tested at, declared A PRIORI and NOT by
#: metric magnitude: 20 sessions is the middle of the declared set, it is the
#: conventional monthly cadence for multi-asset allocation, and it leaves enough
#: forecast dates for a calibration to be estimable. Lane E then tests horizon
#: choice and combination on its own, scored by HNES rather than by raw metric.
PRIMARY_CONVERSION_HORIZON = 20
HORIZON_CHOSEN_BY_RAW_METRIC_MAGNITUDE = False

#: Reused from Release 33 rather than redeclared, and ASSERTED equal at import
#: so a silent divergence in either release is a test failure rather than a
#: quiet inconsistency.
IMPLEMENTATION_LAG_SESSIONS = _r33.IMPLEMENTATION_LAG_SESSIONS
MIN_HISTORY_SESSIONS = _r33.MIN_HISTORY_SESSIONS
NON_OVERLAPPING_FORECAST_DATES = True
MIN_SCORED_FORECAST_DATES = 24

# --------------------------------------------------------------------------- #
# Lane C - expected-return calibration
# --------------------------------------------------------------------------- #
CAL_LINEAR = "LINEAR"
CAL_RIDGE_SHRUNK = "RIDGE_SHRUNK"
CAL_ISOTONIC = "ISOTONIC"
CAL_RANK_BUCKET = "RANK_BUCKET_EMPIRICAL"
CAL_BAYES = "BAYESIAN_SHRINKAGE_TO_ZERO"
CALIBRATIONS = (CAL_LINEAR, CAL_RIDGE_SHRUNK, CAL_ISOTONIC, CAL_RANK_BUCKET,
                CAL_BAYES)

#: Isotonic regression is admitted ONLY where the training block is large
#: enough to support a monotone step function without fitting noise.
MIN_ISOTONIC_TRAINING_ROWS = 2000
RANK_BUCKET_COUNT = 10
CALIBRATION_RIDGE_SHRINK = 0.5
CALIBRATION_BAYES_PRIOR_STRENGTH = 1.0

#: Every calibration is fitted on TRAINING rows only. A calibration fitted on
#: the block it is scored on is not a calibration, it is a fit.
CALIBRATION_FITTED_ON_TRAINING_ONLY = True
FUTURE_PERIOD_CALIBRATION_ALLOWED = False

#: The declared test of whether forecast MAGNITUDE carries information beyond
#: RANK: the same book is built from the calibrated expected return and from a
#: pure rank transform of the same forecast, and the difference in after-cost
#: excess utility is reported. A positive difference is magnitude value; a
#: non-positive one means only rank survived, and that is admitted rather than
#: dressed up.
MAGNITUDE_BEYOND_RANK_TEST = "CALIBRATED_MINUS_RANK_ONLY_UTILITY_DELTA"

# --------------------------------------------------------------------------- #
# Lane D - uncertainty-aware position sizing
# --------------------------------------------------------------------------- #
SIZE_RANK_WEIGHT = "RANK_WEIGHTED"
SIZE_SIGNAL_OVER_VOL = "SIGNAL_OVER_FORECAST_VOLATILITY"
SIZE_ER_OVER_VAR = "EXPECTED_RETURN_OVER_PREDICTED_VARIANCE"
SIZE_ER_OVER_UNCERTAINTY = "EXPECTED_RETURN_OVER_UNCERTAINTY"
SIZE_BAYES_POSTERIOR = "BAYESIAN_POSTERIOR_MEAN"
SIZE_CLIPPED_Z = "CLIPPED_Z_SCORE"
SIZINGS = (SIZE_RANK_WEIGHT, SIZE_SIGNAL_OVER_VOL, SIZE_ER_OVER_VAR,
           SIZE_ER_OVER_UNCERTAINTY, SIZE_BAYES_POSTERIOR, SIZE_CLIPPED_Z)

#: No unconstrained optimiser and no leverage in the PRIMARY campaign.
UNCONSTRAINED_OPTIMISER_ALLOWED = False
LEVERAGE_AVAILABLE = False
MAX_GROSS_EXPOSURE = 1.0
#: Cash is a real asset choice. A book that holds nothing is a valid answer.
MIN_CASH_WEIGHT = 0.0
MAX_CASH_WEIGHT = 1.0
CLIPPED_Z_LIMIT = 2.0

# --------------------------------------------------------------------------- #
# Lane E - horizon combination
# --------------------------------------------------------------------------- #
HORIZON_SETS = ((5,), (20,), (60,), (5, 20), (20, 60), (5, 20, 60))

#: The horizon-normalised evidence score, declared BEFORE evaluation.
#:
#:     HNES(h) = IR_ann(h) * shrink(n_h) * stability(h)
#:
#:     IR_ann(h)   = mean(g_h) / sd(g_h) * sqrt(252 / h)
#:     shrink(n_h) = n_h / (n_h + HNES_SHRINK_N0)
#:     stability(h)= fraction of training sub-blocks with mean(g_h) > 0
#:
#: ``g_h`` is the per-forecast-date after-cost economic gain of the candidate
#: over the risk-matched control at horizon ``h``, computed inside the TRAINING
#: partition only. Annualising removes the mechanical horizon scaling that
#: biased every R33 finalist; the shrink term charges for the smaller
#: observation count a long horizon necessarily has; the stability term refuses
#: to reward an effect that lives in one sub-block.
HNES_FORMULA = ("IR_ann(h) * shrink(n_h) * stability(h); "
                "IR_ann = mean(g)/sd(g)*sqrt(252/h); "
                "shrink = n/(n+HNES_SHRINK_N0); "
                "stability = fraction of training sub-blocks with mean(g)>0")
HNES_SHRINK_N0 = 20.0
HNES_STABILITY_BLOCKS = 4
HNES_COMPUTED_ON_TRAINING_ONLY = True

COMBINE_EQUAL = "EQUAL_WEIGHT_A_PRIORI"
COMBINE_HNES = "HNES_PROPORTIONAL_TRAINED_IN_TRAINING"
COMBINATION_WEIGHTS = (COMBINE_EQUAL, COMBINE_HNES)
POST_RESULT_HAND_TUNING_ALLOWED = False

# --------------------------------------------------------------------------- #
# Lane F - cost-aware turnover control
# --------------------------------------------------------------------------- #
TURN_IMMEDIATE = "IMMEDIATE_TARGET"
TURN_NO_TRADE_BAND = "NO_TRADE_BAND"
TURN_FORECAST_CHANGE = "FORECAST_CHANGE_THRESHOLD"
TURN_PARTIAL = "PARTIAL_ADJUSTMENT"
TURN_PENALISED = "TURNOVER_PENALISED_TARGET"
TURNOVER_RULES = (TURN_IMMEDIATE, TURN_NO_TRADE_BAND, TURN_FORECAST_CHANGE,
                  TURN_PARTIAL, TURN_PENALISED)

NO_TRADE_BAND_GRID = (0.01, 0.02, 0.05)
PARTIAL_ADJUSTMENT_GRID = (0.25, 0.50, 0.75)
FORECAST_CHANGE_GRID = (0.25, 0.50)
TURNOVER_PENALTY_GRID = (1.0, 5.0)

#: The objective is NOT to minimise turnover. A book that never trades has zero
#: turnover and zero edge.
TURNOVER_OBJECTIVE = "MAXIMISE_EXPECTED_AFTER_COST_UTILITY"

# --------------------------------------------------------------------------- #
# Lane G - portfolio construction
# --------------------------------------------------------------------------- #
PORT_LONG_CASH_RANKED = "LONG_CASH_RANKED"
PORT_LONG_SCORE_WEIGHTED = "LONG_ONLY_SCORE_WEIGHTED"
PORT_VOL_SCALED_LONG_CASH = "VOLATILITY_SCALED_LONG_CASH"
PORT_MEAN_VARIANCE = "SHRUNK_MEAN_VARIANCE"
PORT_RISK_BUDGET_TILT = "RISK_BUDGETED_TILT_AROUND_NEUTRAL"
PORTFOLIOS = (PORT_LONG_CASH_RANKED, PORT_LONG_SCORE_WEIGHTED,
              PORT_VOL_SCALED_LONG_CASH, PORT_MEAN_VARIANCE,
              PORT_RISK_BUDGET_TILT)

#: A long-short book is SECONDARY research. It may not qualify the primary
#: release: shortability, borrow cost and recall risk are not modelled from
#: owned data, so a long-short result is not implementability-proven.
LONG_SHORT_IS_SECONDARY_ONLY = True
LONG_SHORT_MAY_QUALIFY_PRIMARY = False

LONG_CASH_TOP_K = 8
MAX_INSTRUMENT_WEIGHT = 0.20
MAX_ASSET_CLASS_WEIGHT = 0.40
MEAN_VARIANCE_SHRINKAGE = 0.80
RISK_BUDGET_NEUTRAL = "EQUAL_RISK_ACROSS_ASSET_CLASSES"
RISK_AVERSION = 2.0

# --------------------------------------------------------------------------- #
# Economics
# --------------------------------------------------------------------------- #
#: Charged on TRADED NOTIONAL (``sum |dw|``, sells AND buys), never on one-way
#: turnover. Release 31 shipped that bug and understated every cost by half.
COST_BASE = "TRADED_NOTIONAL"

#: One-way cost per side in basis points, by MEASURED liquidity tier rather
#: than by asset class. These are ETF costs - half-spread plus a small impact
#: allowance for a paper book of this size - and they are deliberately NOT the
#: futures numbers Release 33 used.
COST_TIER_BPS = {
    "TIER_1_MEGA": 1.5,        # median ADV >= $1bn
    "TIER_2_LARGE": 3.0,       # $200m - $1bn
    "TIER_3_MID": 6.0,         # $50m - $200m
    "TIER_4_THIN": 15.0,       # $10m - $50m
    "TIER_5_VERY_THIN": 30.0,  # $5m - $10m
    "TIER_6_UNTRADABLE": 100.0,  # below the liquidity floor; never admitted
}
COST_TIER_BOUNDS = ((1e9, "TIER_1_MEGA"), (200e6, "TIER_2_LARGE"),
                    (50e6, "TIER_3_MID"), (10e6, "TIER_4_THIN"),
                    (5e6, "TIER_5_VERY_THIN"), (0.0, "TIER_6_UNTRADABLE"))
COST_SCENARIOS = {"OPTIMISTIC": 0.5, "BASE": 1.0, "STRESSED": 2.0,
                  "SEVERE_STRESS": 4.0}
COST_SCENARIO_PRIMARY = "BASE"
#: A result that survives only its most optimistic cost assumption has not
#: survived. The candidate must remain positive at the STRESSED multiplier.
COST_SENSITIVITY_REQUIRED_THROUGH = "STRESSED"

CASH_YIELD_SYMBOL = "%IRX"
BENCHMARK_SYMBOL = "SPY"
BENCHMARK_IS_AN_INSTRUMENT_OF_THE_UNIVERSE = True

CONTROL_CASH = "CASH"
CONTROL_EQUAL_WEIGHT = "EQUAL_WEIGHT_IMPLEMENTABLE_UNIVERSE"
CONTROL_VOL_MATCHED = "VOLATILITY_MATCHED_BENCHMARK_CASH_MIX"
CONTROL_BUY_AND_HOLD = "BENCHMARK_BUY_AND_HOLD"
CONTROL_TREND = "CANONICAL_TRANSPARENT_TREND"
CONTROL_SIXTY_FORTY = "SIXTY_FORTY_EQUITY_BOND"
CONTROLS = (CONTROL_CASH, CONTROL_EQUAL_WEIGHT, CONTROL_VOL_MATCHED,
            CONTROL_BUY_AND_HOLD, CONTROL_TREND, CONTROL_SIXTY_FORTY)

#: The control that DECIDES. Release 32 measured six sleeves against cash and
#: all six beat it; not one beat a volatility-matched mix. Over a long window
#: anything holding risk beats bills, so excess over cash measures EXPOSURE and
#: a campaign that ranks on it will promote beta and call it alpha.
ECONOMIC_CONTROL = CONTROL_VOL_MATCHED
EXCESS_OVER_CASH_MAY_RANK = False

#: The PRIMARY economic decision statistic, declared before evaluation:
#:
#:     AFTER_COST_EXCESS_UTILITY
#:       = U(book_net) - U(control_net)
#:       = [mu_book - (gamma/2) var_book] - [mu_ctrl - (gamma/2) var_ctrl]
#:
#: annualised, with ``gamma = RISK_AVERSION`` and both legs measured on the SAME
#: forecast dates. Utility rather than raw excess, because a book can raise its
#: mean purely by carrying more risk and the control is volatility-matched only
#: to the book's REALISED risk, not to its risk at every moment.
PRIMARY_DECISION_STATISTIC = "AFTER_COST_EXCESS_UTILITY"
PRIMARY_DECISION_FORMULA = ("U(book_net) - U(control_net); "
                            "U(x) = mean_ann(x) - 0.5 * RISK_AVERSION * "
                            "var_ann(x); same dates for both legs")

# --------------------------------------------------------------------------- #
# Temporal design - nested chronological walk-forward
# --------------------------------------------------------------------------- #
#: The panel begins when the implementable universe has enough breadth to be a
#: cross-section rather than a handful of products.
PANEL_START = "1999-01-04"
RANDOM_SPLIT_ALLOWED = False

#: Nested chronological walk-forward. Each fold trains on everything strictly
#: before its validation block, minus an embargo, and the INNER selection of
#: calibration and turnover parameters happens inside the training block only.
WALK_FORWARD_FOLDS = (
    ("2008-01-01", "2010-12-31"),
    ("2011-01-01", "2013-12-31"),
    ("2014-01-01", "2016-12-31"),
    ("2017-01-01", "2019-12-31"),
    ("2020-01-01", "2022-12-31"),
    ("2023-01-01", "2026-08-20"),
)
MIN_TRAIN_SESSIONS = 1260
EMBARGO_EXTRA_SESSIONS = 5
INNER_VALIDATION_FRACTION = 0.25
NESTED_SELECTION_INSIDE_TRAINING_ONLY = True

HISTORICAL_WALK_FORWARD_EVIDENCE = "HISTORICAL_WALK_FORWARD_EVIDENCE"
FRESH_UNSEEN_FORWARD_EVIDENCE = "FRESH_UNSEEN_FORWARD_EVIDENCE"

#: THE decisive temporal fact, declared before the campaign runs.
#:
#: Releases 31, 32 and 33 have all used evidence through 2026 to select. R33's
#: lockbox was 2021-01-01 onward and it was opened eight times. There is
#: therefore no historical block left that is genuinely untouched by prior
#: selection, and calling the walk-forward's last fold a "fresh lockbox" would
#: be a fiction. This campaign does not manufacture one.
FRESH_UNSEEN_EVIDENCE_EXISTS = False
FRESH_UNSEEN_EVIDENCE_REASON = (
    "R31, R32 and R33 all used evidence through 2026 for selection; R33's "
    "lockbox opened 2021-01-01 onward and was accessed 8 times. No untouched "
    "historical block remains, so no fold of this walk-forward may be called a "
    "fresh lockbox")
EVIDENCE_USED_BY_PRIOR_CAMPAIGNS = ("r31_mathematical_alpha_frontier",
                                    "r32_pnl_opportunity_frontier_v4",
                                    "r33_predictive_edge_v2")

# --------------------------------------------------------------------------- #
# Multiple testing
# --------------------------------------------------------------------------- #
#: CEILING, not a target. Every executed conversion configuration counts,
#: including the ones that failed or were abandoned. A denominator that counts
#: only survivors is not a correction, it is a second selection.
MAX_PRIMARY_CONFIGS = 80
DENOMINATOR_COUNTS_ALL_EXECUTED = True
ADAPTIVE_SEARCH_ALLOWED = False

#: Pre-registered families and their counts. The sum is the campaign's planned
#: denominator and is asserted against the executed count.
#: DERIVED from the frozen grids above rather than typed as a separate number.
#: The first version typed "12" for a forecast family that the frozen grid
#: actually enumerates 18 of, so the plan and the enumeration disagreed by six
#: and the assertion that compares them was checking one hand-written number
#: against another. A planned count that cannot drift from what will actually
#: run is the only kind worth asserting.
_FORECAST_MODEL_CONFIGS = (1                              # transparent TSMOM
                           + len(RIDGE_ALPHAS)
                           + len(ELASTIC_NET_ALPHAS)
                           + len(HIERARCHICAL_SHRINK))
_HORIZON_CONFIGS = sum(1 if len(s) == 1 else len(COMBINATION_WEIGHTS)
                       for s in HORIZON_SETS)
#: The finalist set is the combined best plus one neighbour on each of the four
#: lanes that can move a book, plus two structural probes.
FINALIST_CONFIGS = 7

CONFIG_FAMILIES = {
    "FORECAST": _FORECAST_MODEL_CONFIGS * len(HORIZONS),
    "CALIBRATION": len(CALIBRATIONS),
    "SIZING": len(SIZINGS),
    "HORIZON": _HORIZON_CONFIGS,
    "TURNOVER": len(TURNOVER_RULES),
    "PORTFOLIO": len(PORTFOLIOS),
    "FINALIST": FINALIST_CONFIGS,
}
PLANNED_CONFIG_TOTAL = sum(CONFIG_FAMILIES.values())

#: The DEFAULT held constant while one lane is varied, declared before
#: evaluation. A coordinate-wise design like this cannot see every interaction,
#: and that limitation is accepted deliberately: the alternative is a
#: 4,500-cell grid, which would blow through the frozen ceiling of 80 and turn
#: a conversion study into exactly the unbounded search Release 33 concluded was
#: the wrong thing to buy.
#:
#: Each default is the conservative or textbook choice for its lane, not the
#: one that happened to look good - none of them can have, because no result
#: exists when this file is written.
DEFAULT_CALIBRATION = CAL_RIDGE_SHRUNK
DEFAULT_SIZING = SIZE_ER_OVER_VAR
DEFAULT_HORIZON_SET = (20,)
DEFAULT_TURNOVER = TURN_IMMEDIATE
DEFAULT_PORTFOLIO = PORT_LONG_SCORE_WEIGHTED
#: The forecast model is NOT defaulted. It is selected per fold on the INNER
#: VALIDATION block by rank IC, inside the training partition.
DEFAULT_MODEL_SELECTION = "INNER_VALIDATION_RANK_IC"

#: How the nested selection is actually arranged, declared so the one honest
#: limitation is on the record rather than discovered by a reader.
#:
#: Per fold: the model is fitted on the INNER-FIT rows and its forecasts on the
#: INNER-VALIDATION rows are used both to fit calibrations and to select
#: parameters. Reusing one inner block for both can overfit the SELECTION; it
#: cannot contaminate the EVALUATION, which no fitting or selection step ever
#: touches. The evaluation forecasts come from a model refitted on the whole
#: training block, and the resulting score-scale difference is MEASURED and
#: reported rather than assumed negligible.
NESTED_SELECTION_ARRANGEMENT = (
    "model fitted on INNER_FIT; calibration and parameter selection on "
    "INNER_VALIDATION; evaluation forecasts from a refit on the full training "
    "block; score-scale ratio between the two fits measured and reported")

#: Controls are pre-registered REFERENCE objects, not searched hypotheses, so
#: they do not enter the denominator. They are enumerated in ``CONTROLS`` and
#: none of them can qualify.
CONTROLS_ENTER_DENOMINATOR = False

FDR_Q = 0.10
BOOTSTRAP_RESAMPLES = 2000
BOOTSTRAP_BLOCK_MEAN = 10.0
BOOTSTRAP_SEED = 20340821
MODEL_SEED = 34

# --------------------------------------------------------------------------- #
# Concentration gates - frozen BEFORE any economic result
# --------------------------------------------------------------------------- #
#: No finalist qualifies if removing ONE instrument reverses the sign of its
#: after-cost excess economics. R33's entire positive lockbox result was one
#: currency, and it was only visible because leave-one-market-out was required.
SIGN_REVERSAL_ON_LEAVE_ONE_OUT_DISQUALIFIES = True
MAX_SINGLE_INSTRUMENT_PNL_SHARE = 0.40
MAX_SINGLE_ASSET_CLASS_PNL_SHARE = 0.60
MAX_MEAN_SINGLE_INSTRUMENT_EXPOSURE = 0.25
MIN_EFFECTIVE_INSTRUMENTS = 5.0
LEAVE_ONE_INSTRUMENT_OUT_REQUIRED = True
LEAVE_ONE_ASSET_CLASS_OUT_REQUIRED = True
CONCENTRATION_GATE_FROZEN_BEFORE_EVALUATION = True

#: Other robustness thresholds, frozen with the rest.
MAX_ANNUAL_TURNOVER = 12.0
MIN_NEIGHBOUR_RETENTION = 0.30
MIN_SAME_SIGN_FOLD_FRACTION = 0.60

#: Minimum ENGAGEMENT. Cash is a legitimate allocation and abstention is a
#: legitimate answer, but a book that holds almost nothing and almost never
#: trades has converted no prediction into anything - and because its
#: after-cost excess over a volatility-matched control is approximately zero, it
#: would otherwise outrank every genuinely negative candidate and be reported as
#: the campaign's best result. A book must actually take positions before it can
#: be called conversion alpha.
MIN_MEAN_GROSS_EXPOSURE = 0.20
MIN_ANNUAL_TURNOVER = 0.10

#: A neighbour that produces the SAME book is not a probe of anything. The
#: parameter-cliff gate excludes neighbours whose weights are identical to the
#: base and records them as NO_EFFECT, and requires at least this many
#: neighbours that actually moved before it will report a retention figure.
MIN_EFFECTIVE_NEIGHBOURS = 2
NEIGHBOUR_WEIGHT_DIFFERENCE_EPSILON = 1e-9

# --------------------------------------------------------------------------- #
# Terminal verdicts
# --------------------------------------------------------------------------- #
VERDICT_QUALIFIED = "R34_ALPHA_QUALIFIED"
VERDICT_NEEDS_FORWARD = "R34_CONVERSION_EDGE_REQUIRES_FRESH_FORWARD_CONFIRMATION"
VERDICT_NO_CONVERSION = "R34_PREDICTION_DOES_NOT_CONVERT"
VERDICT_UNIVERSE_BLOCKED = "R34_IMPLEMENTABLE_UNIVERSE_BLOCKED"
VERDICT_DATA_BLOCKED = "R34_DATA_INTEGRITY_BLOCKED"
PRIMARY_VERDICTS = (VERDICT_QUALIFIED, VERDICT_NEEDS_FORWARD,
                    VERDICT_NO_CONVERSION, VERDICT_UNIVERSE_BLOCKED,
                    VERDICT_DATA_BLOCKED)

RESULT_PASS = "PASS"
RESULT_FAIL = "FAIL"

#: ALPHA_RESULT may be PASS only with R34_ALPHA_QUALIFIED. Enforced in
#: ``campaign.build_verdict`` and asserted by the release tests.
ALPHA_PASS_REQUIRES = VERDICT_QUALIFIED

#: R34_CONVERSION_EDGE_REQUIRES_FRESH_FORWARD_CONFIRMATION means mathematically
#: interesting, potentially economically useful, and NOT yet qualified alpha.
VERDICTS_WITH_ALPHA_FAIL = (VERDICT_NEEDS_FORWARD, VERDICT_NO_CONVERSION,
                            VERDICT_UNIVERSE_BLOCKED, VERDICT_DATA_BLOCKED)

#: ALL of these must hold for R34_ALPHA_QUALIFIED. Each is reported with its own
#: boolean so a reader sees exactly which condition failed.
QUALIFICATION_CONDITIONS = (
    "predictive_skill_remains_positive",
    "positive_after_cost_excess_economics",
    "positive_after_cost_utility",
    "beats_proper_investable_risk_matched_control",
    "same_sign_result_across_walk_forward_folds",
    "survives_multiple_testing_procedure",
    "no_single_instrument_dependency",
    "no_single_asset_class_dependency",
    "acceptable_turnover",
    "acceptable_cost_sensitivity",
    "no_severe_parameter_cliff",
    "book_actually_takes_positions",
    "implementability_proven",
    "genuinely_independent_evidence_exists",
)

#: SYSTEM_RESULT and ALPHA_RESULT are separate, permanently. If the software
#: works and no economic edge qualifies, that is PASS/FAIL and it is not an
#: alpha success.
SYSTEM_AND_ALPHA_RESULTS_ARE_SEPARATE = True

# --------------------------------------------------------------------------- #
# Scope - research only
# --------------------------------------------------------------------------- #
OUT_OF_SCOPE = (
    "production paper portfolio", "holdings mutation", "proposal mutation",
    "decision mutation", "order creation", "paper execution",
    "broker integration", "champion promotion", "model activation",
    "scheduler changes", "production restart", "UI work", "API endpoint",
    "allocator integration", "portfolio activation", "Telegram changes",
    "execution changes", "workflow-state changes",
)


def _assert_reused_terms() -> None:
    """The terms reused from Release 33 must still be what this release says.

    A reused constant that silently changes in its owner is the quiet failure
    this check exists to make loud.
    """
    if IMPLEMENTATION_LAG_SESSIONS != 1:
        raise RuntimeError(
            "R34 requires an implementation lag of exactly one session; the "
            "reused R33 term is %r" % (IMPLEMENTATION_LAG_SESSIONS,))
    if MIN_HISTORY_SESSIONS != 252:
        raise RuntimeError(
            "R34 requires 252 sessions of minimum history; the reused R33 term "
            "is %r" % (MIN_HISTORY_SESSIONS,))
    if PRIMARY_CONVERSION_HORIZON not in HORIZONS:
        raise RuntimeError(
            "the primary conversion horizon must be one of the declared "
            "horizons")
    if PLANNED_CONFIG_TOTAL > MAX_PRIMARY_CONFIGS:
        raise RuntimeError(
            "planned configuration total %d exceeds the frozen ceiling %d"
            % (PLANNED_CONFIG_TOTAL, MAX_PRIMARY_CONFIGS))


_assert_reused_terms()


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
            "can verified predictive information be converted into robust, "
            "implementable, after-cost excess PnL? Infrastructure, "
            "documentation and a completed campaign are NOT an alpha success"),
        "inherited_release33": {
            "campaign_id": R33_CAMPAIGN_ID,
            "verdict": R33_VERDICT,
            "system_result": R33_SYSTEM_RESULT,
            "alpha_result": R33_ALPHA_RESULT,
            "denominator": R33_DENOMINATOR,
            "lockbox_accesses": R33_LOCKBOX_ACCESSES,
            "qualified": R33_QUALIFIED,
            "rerun": False,
            "evidence_rules": R33_EVIDENCE_RULES,
            "motivating_observations": list(R33_MOTIVATING_OBSERVATIONS),
        },
        "lane_a_universe": {
            "implementable_label": IMPLEMENTABLE_RESEARCH_UNIVERSE,
            "requires_exchange_traded_security":
                IMPLEMENTABLE_REQUIRES_EXCHANGE_TRADED_SECURITY,
            "requires_total_return_prices":
                IMPLEMENTABLE_REQUIRES_TOTAL_RETURN_PRICES,
            "non_investable_series_may_enter_portfolio":
                NON_INVESTABLE_SERIES_MAY_ENTER_PORTFOLIO,
            "barred_from_portfolio": list(BARRED_FROM_PORTFOLIO),
            "target_instrument_count": list(TARGET_INSTRUMENT_COUNT),
            "min_instrument_count": MIN_INSTRUMENT_COUNT,
            "min_asset_class_count": MIN_ASSET_CLASS_COUNT,
            "min_instrument_sessions": MIN_INSTRUMENT_SESSIONS,
            "min_median_dollar_volume": MIN_MEDIAN_DOLLAR_VOLUME,
            "liquidity_window_sessions": LIQUIDITY_WINDOW_SESSIONS,
            "max_zero_return_fraction": MAX_ZERO_RETURN_FRACTION,
            "exclude_exchange_traded_notes": EXCLUDE_EXCHANGE_TRADED_NOTES,
            "exclude_leveraged_and_inverse": EXCLUDE_LEVERAGED_AND_INVERSE,
            "exclude_currency_hedged": EXCLUDE_CURRENCY_HEDGED,
            "includes_delisted_candidates":
                UNIVERSE_INCLUDES_DELISTED_CANDIDATES,
            "delisted_instrument_is_forced_to_cash":
                DELISTED_INSTRUMENT_IS_FORCED_TO_CASH,
        },
        "lane_b_forecast": {
            "new_predictor_search_allowed": NEW_PREDICTOR_SEARCH_ALLOWED,
            "feature_families_frozen_from": FEATURE_FAMILIES_FROZEN_FROM,
            "models": list(FORECAST_MODELS),
            "volatility_model_is_a_risk_input":
                VOLATILITY_MODEL_IS_A_RISK_INPUT,
            "horizons": list(HORIZONS),
            "primary_conversion_horizon": PRIMARY_CONVERSION_HORIZON,
            "horizon_chosen_by_raw_metric_magnitude":
                HORIZON_CHOSEN_BY_RAW_METRIC_MAGNITUDE,
            "implementation_lag_sessions": IMPLEMENTATION_LAG_SESSIONS,
            "non_overlapping_forecast_dates": NON_OVERLAPPING_FORECAST_DATES,
            "min_scored_forecast_dates": MIN_SCORED_FORECAST_DATES,
        },
        "lane_c_calibration": {
            "calibrations": list(CALIBRATIONS),
            "min_isotonic_training_rows": MIN_ISOTONIC_TRAINING_ROWS,
            "rank_bucket_count": RANK_BUCKET_COUNT,
            "fitted_on_training_only": CALIBRATION_FITTED_ON_TRAINING_ONLY,
            "future_period_calibration_allowed":
                FUTURE_PERIOD_CALIBRATION_ALLOWED,
            "magnitude_beyond_rank_test": MAGNITUDE_BEYOND_RANK_TEST,
        },
        "lane_d_sizing": {
            "sizings": list(SIZINGS),
            "unconstrained_optimiser_allowed":
                UNCONSTRAINED_OPTIMISER_ALLOWED,
            "leverage_available": LEVERAGE_AVAILABLE,
            "max_gross_exposure": MAX_GROSS_EXPOSURE,
            "cash_weight_range": [MIN_CASH_WEIGHT, MAX_CASH_WEIGHT],
        },
        "lane_e_horizon": {
            "horizon_sets": [list(h) for h in HORIZON_SETS],
            "hnes_formula": HNES_FORMULA,
            "hnes_shrink_n0": HNES_SHRINK_N0,
            "hnes_stability_blocks": HNES_STABILITY_BLOCKS,
            "hnes_computed_on_training_only": HNES_COMPUTED_ON_TRAINING_ONLY,
            "combination_weights": list(COMBINATION_WEIGHTS),
            "post_result_hand_tuning_allowed":
                POST_RESULT_HAND_TUNING_ALLOWED,
        },
        "lane_f_turnover": {
            "rules": list(TURNOVER_RULES),
            "no_trade_band_grid": list(NO_TRADE_BAND_GRID),
            "partial_adjustment_grid": list(PARTIAL_ADJUSTMENT_GRID),
            "forecast_change_grid": list(FORECAST_CHANGE_GRID),
            "turnover_penalty_grid": list(TURNOVER_PENALTY_GRID),
            "objective": TURNOVER_OBJECTIVE,
        },
        "lane_g_portfolio": {
            "portfolios": list(PORTFOLIOS),
            "long_short_is_secondary_only": LONG_SHORT_IS_SECONDARY_ONLY,
            "long_short_may_qualify_primary": LONG_SHORT_MAY_QUALIFY_PRIMARY,
            "long_cash_top_k": LONG_CASH_TOP_K,
            "max_instrument_weight": MAX_INSTRUMENT_WEIGHT,
            "max_asset_class_weight": MAX_ASSET_CLASS_WEIGHT,
            "mean_variance_shrinkage": MEAN_VARIANCE_SHRINKAGE,
            "risk_budget_neutral": RISK_BUDGET_NEUTRAL,
            "risk_aversion": RISK_AVERSION,
        },
        "economics": {
            "cost_base": COST_BASE,
            "cost_tier_bps": dict(COST_TIER_BPS),
            "cost_tier_bounds": [list(b) for b in COST_TIER_BOUNDS],
            "cost_scenarios": dict(COST_SCENARIOS),
            "cost_scenario_primary": COST_SCENARIO_PRIMARY,
            "cost_sensitivity_required_through":
                COST_SENSITIVITY_REQUIRED_THROUGH,
            "cash_yield_symbol": CASH_YIELD_SYMBOL,
            "benchmark_symbol": BENCHMARK_SYMBOL,
            "controls": list(CONTROLS),
            "economic_control": ECONOMIC_CONTROL,
            "excess_over_cash_may_rank": EXCESS_OVER_CASH_MAY_RANK,
            "primary_decision_statistic": PRIMARY_DECISION_STATISTIC,
            "primary_decision_formula": PRIMARY_DECISION_FORMULA,
        },
        "temporal": {
            "panel_start": PANEL_START,
            "random_split_allowed": RANDOM_SPLIT_ALLOWED,
            "walk_forward_folds": [list(f) for f in WALK_FORWARD_FOLDS],
            "min_train_sessions": MIN_TRAIN_SESSIONS,
            "embargo_extra_sessions": EMBARGO_EXTRA_SESSIONS,
            "inner_validation_fraction": INNER_VALIDATION_FRACTION,
            "nested_selection_inside_training_only":
                NESTED_SELECTION_INSIDE_TRAINING_ONLY,
            "historical_walk_forward_evidence":
                HISTORICAL_WALK_FORWARD_EVIDENCE,
            "fresh_unseen_forward_evidence": FRESH_UNSEEN_FORWARD_EVIDENCE,
            "fresh_unseen_evidence_exists": FRESH_UNSEEN_EVIDENCE_EXISTS,
            "fresh_unseen_evidence_reason": FRESH_UNSEEN_EVIDENCE_REASON,
            "evidence_used_by_prior_campaigns":
                list(EVIDENCE_USED_BY_PRIOR_CAMPAIGNS),
        },
        "multiple_testing": {
            "max_primary_configs": MAX_PRIMARY_CONFIGS,
            "config_families": dict(CONFIG_FAMILIES),
            "planned_config_total": PLANNED_CONFIG_TOTAL,
            "denominator_counts_all_executed": DENOMINATOR_COUNTS_ALL_EXECUTED,
            "controls_enter_denominator": CONTROLS_ENTER_DENOMINATOR,
            "adaptive_search_allowed": ADAPTIVE_SEARCH_ALLOWED,
            "defaults_held_while_one_lane_varies": {
                "calibration": DEFAULT_CALIBRATION,
                "sizing": DEFAULT_SIZING,
                "horizon_set": list(DEFAULT_HORIZON_SET),
                "turnover": DEFAULT_TURNOVER,
                "portfolio": DEFAULT_PORTFOLIO,
                "model_selection": DEFAULT_MODEL_SELECTION,
            },
            "nested_selection_arrangement": NESTED_SELECTION_ARRANGEMENT,
            "fdr_q": FDR_Q,
            "bootstrap_resamples": BOOTSTRAP_RESAMPLES,
            "bootstrap_block_mean": BOOTSTRAP_BLOCK_MEAN,
            "bootstrap_seed": BOOTSTRAP_SEED,
            "model_seed": MODEL_SEED,
        },
        "concentration": {
            "sign_reversal_on_leave_one_out_disqualifies":
                SIGN_REVERSAL_ON_LEAVE_ONE_OUT_DISQUALIFIES,
            "max_single_instrument_pnl_share":
                MAX_SINGLE_INSTRUMENT_PNL_SHARE,
            "max_single_asset_class_pnl_share":
                MAX_SINGLE_ASSET_CLASS_PNL_SHARE,
            "max_mean_single_instrument_exposure":
                MAX_MEAN_SINGLE_INSTRUMENT_EXPOSURE,
            "min_effective_instruments": MIN_EFFECTIVE_INSTRUMENTS,
            "leave_one_instrument_out_required":
                LEAVE_ONE_INSTRUMENT_OUT_REQUIRED,
            "leave_one_asset_class_out_required":
                LEAVE_ONE_ASSET_CLASS_OUT_REQUIRED,
            "frozen_before_evaluation":
                CONCENTRATION_GATE_FROZEN_BEFORE_EVALUATION,
            "max_annual_turnover": MAX_ANNUAL_TURNOVER,
            "min_neighbour_retention": MIN_NEIGHBOUR_RETENTION,
            "min_same_sign_fold_fraction": MIN_SAME_SIGN_FOLD_FRACTION,
            "min_mean_gross_exposure": MIN_MEAN_GROSS_EXPOSURE,
            "min_annual_turnover": MIN_ANNUAL_TURNOVER,
            "min_effective_neighbours": MIN_EFFECTIVE_NEIGHBOURS,
        },
        "superseded_campaigns": SUPERSEDED_CAMPAIGNS,
        "superseded_evidence_rules": SUPERSEDED_EVIDENCE_RULES,
        "qualification": {
            "conditions": list(QUALIFICATION_CONDITIONS),
            "alpha_pass_requires": ALPHA_PASS_REQUIRES,
            "verdicts_with_alpha_fail": list(VERDICTS_WITH_ALPHA_FAIL),
            "system_and_alpha_results_are_separate":
                SYSTEM_AND_ALPHA_RESULTS_ARE_SEPARATE,
        },
        "verdicts": {"primary": list(PRIMARY_VERDICTS)},
        "out_of_scope": list(OUT_OF_SCOPE),
        "environment": {
            "git_head": git_head(repo),
            "python": sys.version.split()[0],
            "platform": platform.platform(),
        },
        "research_root": str(r34.research_root()),
    }
    body = r34.artifact_body(CONTRACT_SCHEMA, payload)
    body["contract_hash"] = r34.sha(
        {k: v for k, v in payload.items() if k != "environment"})
    return body


def path_for(campaign_id: str = CAMPAIGN_ID) -> Path:
    return r34.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(contract: dict) -> Path:
    return r34.write_json(path_for(contract["campaign_id"]), contract)


def load(campaign_id: str = CAMPAIGN_ID) -> Optional[dict]:
    return r34.read_json(path_for(campaign_id))


def verify(contract: dict) -> dict:
    """Recompute the contract hash and report drift."""
    payload = {k: v for k, v in contract.items()
               if k not in ("schema", "release", "safety_block",
                            "contract_hash", "environment")}
    recomputed = r34.sha(payload)
    return {"declared": contract.get("contract_hash"),
            "recomputed": recomputed,
            "stable": recomputed == contract.get("contract_hash")}


def cost_tier(median_dollar_volume: float) -> str:
    """The MEASURED liquidity tier one instrument's cost is charged at."""
    v = float(median_dollar_volume or 0.0)
    for bound, tier in COST_TIER_BOUNDS:
        if v >= float(bound):
            return tier
    return "TIER_4_THIN"
