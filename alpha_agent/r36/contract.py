"""alpha_agent.r36.contract - the ONE Release 36 campaign contract owner.

Every lane, instrument, strategy rule, decision cadence, control, cost tier,
threshold and terminal state below is a value in this module, frozen BEFORE any
result is observed. If a material term changes, that is a NEW campaign with a
NEW id, never an edit.

Three things this contract pins down that no earlier release had to.

**A decision cadence per lane, justified economically.** Release 34 ran one
monthly cadence because it held one cross-section of funds. A commodity curve
rolls monthly, an FX forward is a one-month instrument, a volatility term
structure moves in days. Forcing all of them onto R34's cadence would not be
consistency, it would be a modelling error. Each lane's cadence is declared here
with its reason and may not be chosen after seeing a result.

**A control per lane.** ``SPY + cash`` is the wrong control for a currency
book, a curve trade or a volatility sleeve; measured against it, any of them
would look like skill for holding a different risk. Each lane names the passive
exposure that its strategies must beat - the FX dollar basket, the passive
front-contract commodity roll, duration-matched Treasuries, duration-hedged
credit, a passive long-volatility position, 60/40, passive crypto - and the
primary statistic is after-cost excess UTILITY over a volatility-matched mix of
THAT control and cash.

**An implementation level per cell.** LEVEL 1 SIGNAL, LEVEL 2 PROXY, LEVEL 3
NATIVE. A proxy result never closes a native frontier, and this contract makes
that a recorded property of every experiment rather than a sentence in a
document.
"""
from __future__ import annotations

import platform
import sys
from pathlib import Path
from typing import Optional

from .. import r36
from ..r31.contract import git_head  # ONE git-head owner, reused
from ..r33 import contract as _r33_contract
from ..r33 import universe as _r33_universe
from ..r35 import contract as _r35_contract

CALCULATION_OWNER = "alpha_agent.r36.contract"
CONTRACT_SCHEMA = "r36_global_multi_asset_frontier_contract/1"
ARTIFACT_NAME = "research_contract.json"

#: The campaign this release runs. A new id is the ONLY way to change a term.
CAMPAIGN_ID = "r36_global_multi_asset_frontier_v3"

SUPERSEDED_CONTROL_DEFECT = "SUPERSEDED_CONTROL_DID_NOT_MATCH_WHAT_WAS_TRADED"
SUPERSEDED_WINDOW_DEFECT = "SUPERSEDED_CONTROL_WAS_NOT_OBSERVABLE_THROUGHOUT"

#: v1 is SUPERSEDED, not deleted. Its artifacts stay on disk as history.
#:
#: v1 gave every configuration in a lane the SAME control. That is right when a
#: lane's configurations trade the same thing and wrong when they do not, and
#: the volatility lane does not: ``VOL_TERM_LONG_TIMING`` trades a long
#: volatility product and ``VOL_TERM_EQUITY_TIMING`` trades equity. Measured
#: against a volatility-matched slice of a passive long volatility position -
#: which loses roughly sixty per cent a year to contango - an equity book
#: returned ``+10.5 %/yr`` of "excess" at ``t = 2.81`` and would have been
#: reported as a qualified native candidate. It was not skill; it was a control
#: that had nothing to do with what the book held.
#:
#: v2 declares a control leg per configuration where the lane's configurations
#: trade different instruments. The change can only REMOVE a qualification,
#: never add one, because the defect flattered every book whose control was
#: more negative than its own passive alternative.
SUPERSEDED_CAMPAIGNS = {
    "r36_global_multi_asset_frontier_v1": {
        "state": SUPERSEDED_CONTROL_DEFECT,
        "produced_a_verdict": True,
        "defect": "the volatility lane's control was a volatility-matched mix "
                  "of PASSIVE LONG VOLATILITY and cash, and it was applied to "
                  "a configuration that holds EQUITY. A decaying benchmark is "
                  "not a control, it is a handicap awarded to the candidate",
        "flattered_configuration": "VOL_TERM_EQUITY_TIMING",
        "flattered_reading": "+10.5 %/yr after-cost excess at t = 2.81",
        "correction": "v2 declares STRATEGY_CONTROL_LEG so a configuration is "
                      "measured against the passive buy-and-hold of what it "
                      "actually trades",
        "gate_change_direction": "STRICTLY_TIGHTENING",
        "measurements_unchanged_for_other_lanes": True,
        "is_preserved_on_disk": True,
    },
    "r36_global_multi_asset_frontier_v2": {
        "state": SUPERSEDED_WINDOW_DEFECT,
        "produced_a_verdict": True,
        "defect": "the cross-asset lane's 60/40 control filled its missing "
                  "bond leg with zero, so for every decision before the "
                  "Treasury total-return index begins in December 2004 the "
                  "'60/40 benchmark' was really sixty per cent equity and "
                  "forty per cent nothing. Fifteen years of configurations "
                  "were scored against a portfolio nobody could have held",
        "flattered_configuration": "XA_GOLD_VS_REAL_YIELD and "
                                   "XA_CURVE_SLOPE_EQUITY_BOND, both reported "
                                   "as significant Benjamini-Hochberg "
                                   "rejections against that fabricated control",
        "flattered_reading": "-6.87 %/yr at t = -3.03 and -6.73 %/yr at "
                             "t = -4.23",
        "correction": "v3 lets the missing leg propagate as missing and trims "
                      "EVERY lane to the window in which its own control is "
                      "observable, uniformly and before any strategy runs",
        "gate_change_direction": "NEUTRAL_CORRECTS_A_FABRICATED_BENCHMARK",
        "measurements_unchanged_for_other_lanes": True,
        "is_preserved_on_disk": True,
    },
}

# --------------------------------------------------------------------------- #
# Inherited state - read as history, never used to select anything here
# --------------------------------------------------------------------------- #
INHERITED = {
    "R31": {"verdict": "R31_NO_MATHEMATICAL_ALPHA",
            "scope": "mathematical / price / fundamental frontier, US equities",
            "rerun": False},
    "R32": {"verdict": "R32_ZERO_COST_OPPORTUNITY_FRONTIER_EXHAUSTED",
            "scope": "six simple multi-asset strategy sleeves",
            "rerun": False},
    "R33": {"verdict": "R33_NO_PREDICTIVE_EDGE",
            "scope": "66 markets, 6 asset classes, SIGNAL_RESEARCH_VALID only",
            "rerun": False},
    "R34": {"verdict": "R34_PREDICTION_DOES_NOT_CONVERT",
            "scope": "47 US-listed ETFs, monthly, PROXY implementation",
            "rerun": False},
    "R35": {"verdict": "R35_NO_INCREMENTAL_INFORMATION_EDGE",
            "scope": "six free information families on the R34 decision problem",
            "rerun": False},
}

#: What R34 and R35 did NOT prove. Written down because the temptation to read
#: "an ETF proxy failed" as "the asset class has no alpha" is exactly the error
#: this release exists to correct.
NOT_PROVEN_BY_R34_R35 = (
    "FX has no alpha - a three-currency ETF subset is not the FX market",
    "commodities have no alpha - USO is not the WTI futures curve",
    "credit has no alpha - HYG is not the credit market",
    "rates have no alpha - TLT is not the Treasury curve",
    "volatility has no alpha - VIX as a feature is not a volatility sleeve",
)

INHERITED_EVIDENCE_RULES = {
    "may_select_r36_strategies": False,
    "may_select_r36_parameters": False,
    "may_contribute_to_a_qualification_verdict": False,
    "may_reduce_the_multiple_testing_denominator": False,
    "is_preserved_on_disk": True,
}

# --------------------------------------------------------------------------- #
# The three implementation levels
# --------------------------------------------------------------------------- #
LEVEL_SIGNAL = "LEVEL_1_SIGNAL_SERIES"
LEVEL_PROXY = "LEVEL_2_PROXY_INSTRUMENT"
LEVEL_NATIVE = "LEVEL_3_NATIVE_IMPLEMENTATION"
LEVELS = (LEVEL_SIGNAL, LEVEL_PROXY, LEVEL_NATIVE)

#: The rule this release is built to enforce. A proxy test does NOT close a
#: native-market frontier unless the research proves the proxy preserves the
#: economically relevant opportunity, and no experiment here claims that.
PROXY_MAY_CLOSE_A_NATIVE_FRONTIER = False
PROXY_CLOSURE_REQUIRES_PROVEN_STRUCTURE_PRESERVATION = True

# --------------------------------------------------------------------------- #
# Terminal coverage states
# --------------------------------------------------------------------------- #
STATE_TESTED_NATIVE_REJECTED = "TESTED_NATIVE_REJECTED"
STATE_TESTED_NATIVE_SURVIVOR = "TESTED_NATIVE_SURVIVOR"
STATE_TESTED_PROXY_ONLY = "TESTED_PROXY_ONLY"
STATE_TESTED_SIGNAL_ONLY = "TESTED_SIGNAL_ONLY_NON_IMPLEMENTABLE"
STATE_NOT_TESTED_AVAILABLE = "NOT_TESTED_DATA_AVAILABLE"
STATE_BLOCKED_HISTORY = "BLOCKED_INSUFFICIENT_HISTORY"
STATE_BLOCKED_PIT = "BLOCKED_POINT_IN_TIME"
STATE_BLOCKED_SURVIVORSHIP = "BLOCKED_SURVIVORSHIP"
STATE_BLOCKED_MAPPING = "BLOCKED_INSTRUMENT_MAPPING"
STATE_BLOCKED_ENTITLEMENT = "BLOCKED_ENTITLEMENT"
STATE_BLOCKED_LICENSING = "BLOCKED_LICENSING"
STATE_BLOCKED_COST = "BLOCKED_COST"
STATE_NOT_DISTINCT = "NOT_ECONOMICALLY_DISTINCT"
STATE_NOT_APPLICABLE = "NOT_ECONOMICALLY_APPLICABLE"
STATE_OUT_OF_SCOPE = "OUT_OF_SCOPE_WITH_EXPLICIT_REASON"

TERMINAL_STATES = (
    STATE_TESTED_NATIVE_REJECTED, STATE_TESTED_NATIVE_SURVIVOR,
    STATE_TESTED_PROXY_ONLY, STATE_TESTED_SIGNAL_ONLY,
    STATE_NOT_TESTED_AVAILABLE, STATE_BLOCKED_HISTORY, STATE_BLOCKED_PIT,
    STATE_BLOCKED_SURVIVORSHIP, STATE_BLOCKED_MAPPING,
    STATE_BLOCKED_ENTITLEMENT, STATE_BLOCKED_LICENSING, STATE_BLOCKED_COST,
    STATE_NOT_DISTINCT, STATE_NOT_APPLICABLE, STATE_OUT_OF_SCOPE)

#: A cell may not leave this release without one of the states above. There is
#: no "interesting", no "future work", no "could investigate".
EVERY_CELL_MUST_BE_TERMINAL = True
AMBIGUOUS_CELL_STATES_ALLOWED = False

#: Only these states mean "this cell still has executable work left in it".
#: The verdict rule reads this set rather than a hand-maintained list.
EXECUTABLE_REMAINING_STATES = (STATE_NOT_TESTED_AVAILABLE,)

# --------------------------------------------------------------------------- #
# Strategy families - the columns of the coverage matrix
# --------------------------------------------------------------------------- #
SF_TREND = "TREND"
SF_MOMENTUM = "MOMENTUM"
SF_CARRY = "CARRY"
SF_ROLL = "ROLL"
SF_VALUE = "VALUE"
SF_MEAN_REVERSION = "MEAN_REVERSION"
SF_RELATIVE_VALUE = "RELATIVE_VALUE"
SF_EVENT_DRIVEN = "EVENT_DRIVEN"
SF_POSITIONING = "POSITIONING"
SF_VRP = "VOLATILITY_RISK_PREMIUM"
SF_CURVE = "CURVE_TERM_STRUCTURE"
SF_SEASONALITY = "SEASONALITY"
SF_SUPPLY_DEMAND = "FUNDAMENTAL_SUPPLY_DEMAND"
SF_CROSS_SECTIONAL = "CROSS_SECTIONAL"
SF_MACRO_CONDITIONAL = "MACRO_CONDITIONAL"
SF_LIQUIDITY = "LIQUIDITY_PREMIUM"

STRATEGY_FAMILIES = (
    SF_TREND, SF_MOMENTUM, SF_CARRY, SF_ROLL, SF_VALUE, SF_MEAN_REVERSION,
    SF_RELATIVE_VALUE, SF_EVENT_DRIVEN, SF_POSITIONING, SF_VRP, SF_CURVE,
    SF_SEASONALITY, SF_SUPPLY_DEMAND, SF_CROSS_SECTIONAL, SF_MACRO_CONDITIONAL,
    SF_LIQUIDITY)

# --------------------------------------------------------------------------- #
# Lanes
# --------------------------------------------------------------------------- #
LANE_FX = "FX_NATIVE"
LANE_COMMODITY = "COMMODITY_CURVE_NATIVE"
LANE_RATES = "RATES_CURVE_NATIVE"
LANE_CREDIT = "CREDIT_INDEX"
LANE_VOL = "VOLATILITY_TERM_STRUCTURE"
LANE_CROSS_ASSET = "CROSS_ASSET_RELATIVE_VALUE"
LANE_CRYPTO = "CRYPTO_NATIVE"
EXECUTED_LANES = (LANE_FX, LANE_COMMODITY, LANE_RATES, LANE_CREDIT, LANE_VOL,
                  LANE_CROSS_ASSET, LANE_CRYPTO)

#: Priority order, declared before execution. Measurement may override it, and
#: the artifact records whether it did.
LANE_PRIORITY = (LANE_COMMODITY, LANE_FX, LANE_RATES, LANE_VOL, LANE_CREDIT,
                 LANE_CROSS_ASSET, LANE_CRYPTO)

# --------------------------------------------------------------------------- #
# Decision cadence - frozen per lane WITH its economic reason
# --------------------------------------------------------------------------- #
SESSIONS_PER_YEAR = 252.0
CADENCE_MONTHLY = 21
CADENCE_WEEKLY = 5

LANE_CADENCE = {
    LANE_FX: CADENCE_MONTHLY,
    LANE_COMMODITY: CADENCE_MONTHLY,
    LANE_RATES: CADENCE_MONTHLY,
    LANE_CREDIT: CADENCE_MONTHLY,
    LANE_VOL: CADENCE_WEEKLY,
    LANE_CROSS_ASSET: CADENCE_MONTHLY,
    LANE_CRYPTO: CADENCE_WEEKLY,
}

LANE_CADENCE_REASON = {
    LANE_FX: "the deliverable forward that implements a currency position is a "
             "one-month instrument and the interest differential that drives "
             "carry is a one-month rate; a shorter cadence would pay spread "
             "without changing the information",
    LANE_COMMODITY: "the contracts are monthly and the return being measured is "
                    "the return of holding the second-nearest contract until it "
                    "becomes the nearest, which is exactly one month",
    LANE_RATES: "curve carry and roll-down accrue over months and the tradable "
                "legs are duration-bucket indices whose relative value moves "
                "slowly; a weekly cadence would charge cost against noise",
    LANE_CREDIT: "credit spreads mean-revert over months and the instrument is "
                 "a broad index; credit is the slowest lane in the release",
    LANE_VOL: "the volatility term structure inverts and re-steepens within "
              "days, and a monthly cadence would miss every episode the sleeve "
              "exists to capture",
    LANE_CROSS_ASSET: "the state variables that drive these relationships - "
                      "the real yield, the curve slope, the credit spread - "
                      "move over months, and the legs are whole asset classes "
                      "rather than instruments, so a faster cadence would "
                      "charge cost against macro noise",
    LANE_CRYPTO: "the market trades continuously and its trend horizon is "
                 "measured in weeks, not months",
}

#: Decide from information observable through session t, enter at the close of
#: session t+1, measure the return from t+1 forward. One session of slack, the
#: same convention Release 33 froze and for the same reason.
IMPLEMENTATION_LAG_SESSIONS = _r33_contract.IMPLEMENTATION_LAG_SESSIONS

#: Successive decisions do not overlap: a monthly strategy is observed monthly.
#: Overlapping observations inflate the effective sample and make every
#: t-statistic a fiction.
NON_OVERLAPPING_DECISIONS = True

# --------------------------------------------------------------------------- #
# Point-in-time publication lags - ONE convention, reused, never re-invented
# --------------------------------------------------------------------------- #
#: Reused from the Release-35 contract so that the estate has exactly one
#: publication-lag convention per source rather than one per release.
COT_PUBLICATION_LAG_DAYS = _r35_contract.COT_PUBLICATION_LAG_DAYS
OECD_RATE_PUBLICATION_LAG_MONTHS = \
    _r35_contract.OECD_RATE_PUBLICATION_LAG_MONTHS
BROADCAST_LAG_SESSIONS = _r35_contract.BROADCAST_LAG_SESSIONS

#: EIA republishes the prior session's NYMEX settlements the next business
#: morning, so a settlement dated t is observable for a decision struck at t+1.
EIA_SETTLEMENT_LAG_SESSIONS = 1

#: National CPI is published two to six weeks after the month it measures. Two
#: months is the conservative choice and matches the OECD rate convention.
CPI_PUBLICATION_LAG_MONTHS = 2

#: Quarterly CPI (Australia, New Zealand) is stamped on the FIRST month of the
#: quarter it measures and published about four weeks after the quarter ENDS, so
#: three months of lag would still be a look-ahead of several weeks. Four is the
#: conservative choice.
QUARTERLY_CPI_PUBLICATION_LAG_MONTHS = 4

PROHIBITED_SUBSTITUTIONS = (
    "a current index or universe membership written back onto historical dates",
    "a current crypto survivor list written back onto historical dates",
    "a revised macro value treated as its original print without vintage proof",
    "a fiscal-period date substituted for a publication date",
    "a forward or carry leg manufactured from spot price momentum",
    "a historical option surface manufactured from current implied volatility",
    "a futures roll that uses a contract identity known only after the fact",
    "a bond or index membership assigned retrospectively",
)

# --------------------------------------------------------------------------- #
# Market admissibility - REUSED from Release 33, not re-invented
# --------------------------------------------------------------------------- #
#: A currency whose spot repeats its previous close more than this fraction of
#: sessions is ADMINISTERED: its "return" is an announcement, not a price.
MAX_ZERO_RETURN_FRACTION = _r33_universe.MAX_ZERO_RETURN_FRACTION
#: A currency below this annualised volatility is PEGGED. A carry book that
#: loads on a hard peg is being paid for a tail that did not occur in sample,
#: which is the most flattering thing a currency lane could do to itself.
MIN_ANNUAL_VOLATILITY = _r33_universe.MIN_ANNUAL_VOLATILITY
MAX_DUPLICATE_CORRELATION = _r33_universe.MAX_DUPLICATE_CORRELATION

ADMISSIBILITY_RULES_ARE_REUSED_FROM_R33 = True

#: Minimum trailing observations before an instrument may enter a decision.
MIN_TRAILING_OBSERVATIONS = 60
#: Minimum instruments in a cross-section for a cross-sectional strategy to be
#: scored on a date.
MIN_CROSS_SECTION = 4
#: Crypto is the one lane where the honest cross-section is TWO, because two is
#: how many major crypto assets have a survivorship-safe history. The override
#: is declared here rather than taken quietly inside a strategy, and the
#: resulting long/short pair is reported as the thin test it is.
LANE_MIN_CROSS_SECTION = {"CRYPTO_NATIVE": 2}
#: A configuration with fewer scored decisions than this cannot carry a verdict.
MIN_DECISION_PERIODS = 60

# --------------------------------------------------------------------------- #
# Costs - charged on TRADED NOTIONAL, by lane, with a declared range
# --------------------------------------------------------------------------- #
COST_BASE = "TRADED_NOTIONAL"

#: One-way cost per side in basis points. These are ASSUMPTIONS about execution
#: in markets this estate does not trade, so every configuration is additionally
#: reported at each multiplier below. A result that survives only its most
#: optimistic cost assumption has not survived.
COST_BPS_PER_SIDE = {
    "FX_G10": 2.0,
    "FX_EMERGING": 8.0,
    "ENERGY_FUTURE": 5.0,
    "TREASURY_INDEX": 2.0,
    "CREDIT_INDEX": 5.0,
    "VOLATILITY_ETP": 15.0,
    "EQUITY_INDEX": 3.0,
    "PRECIOUS_METAL": 5.0,
    "CRYPTO": 25.0,
}
COST_SENSITIVITY_MULTIPLIERS = _r33_contract.COST_SENSITIVITY_MULTIPLIERS
#: The multiplier a qualifying configuration must still be positive at.
COST_STRESS_MULTIPLIER = 2.0

# --------------------------------------------------------------------------- #
# Controls - one per lane, and NEVER "SPY plus cash" for all of them
# --------------------------------------------------------------------------- #
CONTROL_VOL_MATCHED = "VOLATILITY_MATCHED_LANE_BENCHMARK_CASH_MIX"

LANE_CONTROL = {
    LANE_FX: "PASSIVE_EQUAL_WEIGHT_LONG_FOREIGN_CURRENCY_BASKET",
    LANE_COMMODITY: "PASSIVE_EQUAL_WEIGHT_FRONT_CONTRACT_ROLL",
    LANE_RATES: "DURATION_MATCHED_PASSIVE_TREASURY_EXPOSURE",
    LANE_CREDIT: "PASSIVE_DURATION_HEDGED_CREDIT_EXPOSURE",
    LANE_VOL: "PASSIVE_LONG_VOLATILITY_EXPOSURE",
    LANE_CROSS_ASSET: "SIXTY_FORTY_EQUITY_BOND",
    LANE_CRYPTO: "PASSIVE_EQUAL_WEIGHT_CRYPTO",
}

LANE_CONTROL_REASON = (
    "a currency book measured against SPY plus cash would be paid for holding "
    "a different risk; each lane's control is the passive exposure a person "
    "could have held in THAT market with none of the timing, and the primary "
    "statistic is after-cost excess utility over a volatility-matched mix of "
    "that control and cash")

UNIVERSAL_SPY_CASH_CONTROL_ALLOWED = False
EXCESS_OVER_CASH_MAY_RANK = False

#: The rule, and it is the rule v1 got wrong: a configuration is measured
#: against the passive buy-and-hold of WHAT IT TRADES. A lane-level basket is
#: the right control only when every configuration in the lane trades that
#: basket's constituents. Where a lane holds configurations that trade
#: different instruments, the control leg is named here - once, before any
#: result exists - and it names a COLUMN of the lane's own return panel, so a
#: control can never be a series chosen to make a number look better.
CONTROL_IS_THE_PASSIVE_HOLD_OF_WHAT_IS_TRADED = True
STRATEGY_CONTROL_LEG = {
    "VOL_TERM_LONG_TIMING": "VIXY",
    "VOL_TERM_EQUITY_TIMING": "SPY",
}
STRATEGY_CONTROL_LEG_REASON = (
    "the volatility lane runs one configuration that holds a long volatility "
    "product and one that holds equity; a single lane control would measure "
    "the equity book against a benchmark that loses about sixty per cent a "
    "year to contango, which is how Release 36 v1 produced a spurious "
    "qualified candidate")

#: The risk-free leg. Cash is a real asset choice and earns an observable
#: point-in-time yield.
CASH_YIELD_SERIES = "DTB3"
RISK_AVERSION = 2.0
PRIMARY_DECISION_STATISTIC = "AFTER_COST_EXCESS_UTILITY_VS_LANE_CONTROL"
PRIMARY_DECISION_FORMULA = (
    "dU = [mu_book - (gamma/2) var_book] - [mu_control - (gamma/2) "
    "var_control], annualised, both legs on the same decision dates")

# --------------------------------------------------------------------------- #
# Position construction - rank based, zero free parameters
# --------------------------------------------------------------------------- #
#: Cross-sectional strategies hold equal-weighted extreme terciles, long the top
#: and short the bottom, gross exposure 1.0. Rank weighting rather than
#: volatility scaling is deliberate: an inverse-volatility book in a currency
#: cross-section puts its largest weight on the most tightly managed currency,
#: which is the one whose realised volatility least reflects its risk.
CONSTRUCTION_CROSS_SECTIONAL = "EQUAL_WEIGHT_EXTREME_TERCILES_GROSS_ONE"
CONSTRUCTION_DIRECTIONAL = "SIGN_OF_SIGNAL_EQUAL_WEIGHT_GROSS_ONE"
CONSTRUCTIONS = (CONSTRUCTION_CROSS_SECTIONAL, CONSTRUCTION_DIRECTIONAL)
TERCILE_FRACTION = 1.0 / 3.0
MAX_GROSS_EXPOSURE = 1.0
LEVERAGE_AVAILABLE = False

#: Every parameter in every rule is PRE-DECLARED at its canonical value. No
#: parameter is selected, tuned or chosen after seeing a result; neighbouring
#: values are reported as a parameter-cliff DIAGNOSTIC and may never be
#: promoted, so they do not enter the multiple-testing denominator.
PARAMETERS_ARE_PRE_DECLARED = True
PARAMETER_SEARCH_ALLOWED = False
ADAPTIVE_SEARCH_ALLOWED = False
MODEL_ARCHITECTURE_SEARCH_ALLOWED = False
DEEP_LEARNING_IN_SCOPE = False
NEIGHBOUR_VALUES_MAY_BE_PROMOTED = False

TREND_LOOKBACK_MONTHS = 12
TREND_SKIP_MONTHS = 1
REVERSAL_LOOKBACK_MONTHS = 1
VALUE_LOOKBACK_MONTHS = 60
SPREAD_REVERSION_LOOKBACK_MONTHS = 60
VOL_TERM_LOOKBACK_WEEKS = 4
CRYPTO_TREND_LOOKBACK_WEEKS = 12
SEASONALITY_MIN_TRAILING_YEARS = 10

#: Every normalisation, ranking and conditioning statistic is computed from
#: TRAILING data only. There is no full-sample mean, no full-sample standard
#: deviation and no full-sample median anywhere in this release.
NORMALISATION_IS_TRAILING_ONLY = True
FULL_SAMPLE_STATISTICS_ALLOWED = False

# --------------------------------------------------------------------------- #
# The frozen experiment grid
# --------------------------------------------------------------------------- #
#: name -> (lane, strategy families, implementation level, construction)
STRATEGIES = {
    # ---- FX, LEVEL 3 NATIVE: a deliverable one-month forward -------------- #
    "FX_CARRY": (LANE_FX, (SF_CARRY, SF_CROSS_SECTIONAL), LEVEL_NATIVE,
                 CONSTRUCTION_CROSS_SECTIONAL),
    "FX_TREND_12_1": (LANE_FX, (SF_TREND, SF_MOMENTUM), LEVEL_NATIVE,
                      CONSTRUCTION_CROSS_SECTIONAL),
    "FX_REVERSAL_1M": (LANE_FX, (SF_MEAN_REVERSION,), LEVEL_NATIVE,
                       CONSTRUCTION_CROSS_SECTIONAL),
    "FX_VALUE_REAL_RATE": (LANE_FX, (SF_VALUE,), LEVEL_NATIVE,
                           CONSTRUCTION_CROSS_SECTIONAL),
    "FX_CARRY_TREND": (LANE_FX, (SF_CARRY, SF_TREND), LEVEL_NATIVE,
                       CONSTRUCTION_CROSS_SECTIONAL),
    "FX_CARRY_CRASH_CONDITIONED": (LANE_FX, (SF_CARRY, SF_MACRO_CONDITIONAL),
                                   LEVEL_NATIVE, CONSTRUCTION_CROSS_SECTIONAL),
    "FX_CARRY_POSITIONING": (LANE_FX, (SF_CARRY, SF_POSITIONING), LEVEL_NATIVE,
                             CONSTRUCTION_CROSS_SECTIONAL),
    "FX_DOLLAR_FACTOR_TIMING": (LANE_FX, (SF_CARRY, SF_MACRO_CONDITIONAL),
                                LEVEL_NATIVE, CONSTRUCTION_DIRECTIONAL),

    # ---- Commodity curve, LEVEL 3 NATIVE: dated NYMEX settlements --------- #
    "CMDTY_CURVE_CARRY": (LANE_COMMODITY, (SF_CARRY, SF_ROLL, SF_CURVE),
                          LEVEL_NATIVE, CONSTRUCTION_DIRECTIONAL),
    "CMDTY_TREND_12M": (LANE_COMMODITY, (SF_TREND,), LEVEL_NATIVE,
                        CONSTRUCTION_DIRECTIONAL),
    "CMDTY_CARRY_TREND": (LANE_COMMODITY, (SF_CARRY, SF_TREND), LEVEL_NATIVE,
                          CONSTRUCTION_DIRECTIONAL),
    "CMDTY_CROSS_SECTIONAL_CARRY": (LANE_COMMODITY,
                                    (SF_CARRY, SF_CROSS_SECTIONAL),
                                    LEVEL_NATIVE,
                                    CONSTRUCTION_CROSS_SECTIONAL),
    "CMDTY_CALENDAR_SPREAD": (LANE_COMMODITY, (SF_CURVE, SF_RELATIVE_VALUE),
                              LEVEL_NATIVE, CONSTRUCTION_DIRECTIONAL),
    "CMDTY_CARRY_POSITIONING": (LANE_COMMODITY, (SF_CARRY, SF_POSITIONING),
                                LEVEL_NATIVE, CONSTRUCTION_DIRECTIONAL),
    "CMDTY_SEASONALITY": (LANE_COMMODITY, (SF_SEASONALITY,), LEVEL_NATIVE,
                          CONSTRUCTION_DIRECTIONAL),

    # ---- Rates curve, LEVEL 2 PROXY legs driven by LEVEL 1 curve signals -- #
    "RATES_CARRY_ROLLDOWN": (LANE_RATES, (SF_CARRY, SF_ROLL, SF_CURVE),
                             LEVEL_PROXY, CONSTRUCTION_CROSS_SECTIONAL),
    "RATES_LEVEL_TREND": (LANE_RATES, (SF_TREND,), LEVEL_PROXY,
                          CONSTRUCTION_DIRECTIONAL),
    "RATES_CURVE_VALUE": (LANE_RATES, (SF_VALUE, SF_MEAN_REVERSION, SF_CURVE),
                          LEVEL_PROXY, CONSTRUCTION_DIRECTIONAL),
    "RATES_STEEPENER_CONDITIONAL": (LANE_RATES,
                                    (SF_CURVE, SF_MACRO_CONDITIONAL),
                                    LEVEL_PROXY, CONSTRUCTION_DIRECTIONAL),
    "RATES_BUTTERFLY": (LANE_RATES, (SF_CURVE, SF_RELATIVE_VALUE), LEVEL_PROXY,
                        CONSTRUCTION_DIRECTIONAL),
    "RATES_BREAKEVEN_RV": (LANE_RATES, (SF_RELATIVE_VALUE, SF_VALUE),
                           LEVEL_PROXY, CONSTRUCTION_DIRECTIONAL),

    # ---- Credit, LEVEL 1 index signal on a LEVEL 2 index leg -------------- #
    "CREDIT_SPREAD_CARRY": (LANE_CREDIT, (SF_CARRY,), LEVEL_PROXY,
                            CONSTRUCTION_DIRECTIONAL),
    "CREDIT_SPREAD_MOMENTUM": (LANE_CREDIT, (SF_MOMENTUM, SF_TREND),
                               LEVEL_PROXY, CONSTRUCTION_DIRECTIONAL),
    "CREDIT_SPREAD_REVERSION": (LANE_CREDIT, (SF_MEAN_REVERSION,), LEVEL_PROXY,
                                CONSTRUCTION_DIRECTIONAL),
    "CREDIT_VS_RATES_RV": (LANE_CREDIT, (SF_RELATIVE_VALUE,), LEVEL_PROXY,
                           CONSTRUCTION_DIRECTIONAL),

    # ---- Volatility, LEVEL 2 PROXY: the native curve is not entitled ------ #
    "VOL_TERM_LONG_TIMING": (LANE_VOL, (SF_CURVE, SF_VRP), LEVEL_PROXY,
                             CONSTRUCTION_DIRECTIONAL),
    "VOL_TERM_EQUITY_TIMING": (LANE_VOL, (SF_VRP, SF_MACRO_CONDITIONAL),
                               LEVEL_PROXY, CONSTRUCTION_DIRECTIONAL),

    # ---- Cross-asset relative value -------------------------------------- #
    "XA_GOLD_VS_REAL_YIELD": (LANE_CROSS_ASSET,
                              (SF_RELATIVE_VALUE, SF_MACRO_CONDITIONAL),
                              LEVEL_PROXY, CONSTRUCTION_DIRECTIONAL),
    "XA_COPPER_GOLD_TO_RATES": (LANE_CROSS_ASSET,
                                (SF_RELATIVE_VALUE, SF_MACRO_CONDITIONAL),
                                LEVEL_PROXY, CONSTRUCTION_DIRECTIONAL),
    "XA_EQUITY_VS_CREDIT": (LANE_CROSS_ASSET, (SF_RELATIVE_VALUE,),
                            LEVEL_PROXY, CONSTRUCTION_DIRECTIONAL),
    "XA_CURVE_SLOPE_EQUITY_BOND": (LANE_CROSS_ASSET,
                                   (SF_MACRO_CONDITIONAL, SF_CURVE),
                                   LEVEL_PROXY, CONSTRUCTION_DIRECTIONAL),
    "XA_FX_CARRY_VS_EQUITY": (LANE_CROSS_ASSET,
                              (SF_RELATIVE_VALUE, SF_CARRY), LEVEL_NATIVE,
                              CONSTRUCTION_DIRECTIONAL),

    # ---- Crypto ---------------------------------------------------------- #
    "CRYPTO_TREND_TS": (LANE_CRYPTO, (SF_TREND,), LEVEL_PROXY,
                        CONSTRUCTION_DIRECTIONAL),
    "CRYPTO_CROSS_SECTIONAL": (LANE_CRYPTO, (SF_MOMENTUM, SF_CROSS_SECTIONAL),
                               LEVEL_PROXY, CONSTRUCTION_CROSS_SECTIONAL),
}

#: Ceiling on executed PRIMARY configurations. Controls do not enter the
#: denominator: a control is not a candidate and cannot be promoted.
MAX_PRIMARY_CONFIGS = 80
CONTROLS_ENTER_DENOMINATOR = False
DENOMINATOR_COUNTS_ALL_EXECUTED = True
PLANNED_CONFIG_TOTAL = len(STRATEGIES)


def strategies_for_lane(lane: str) -> tuple:
    return tuple(sorted(n for n, spec in STRATEGIES.items() if spec[0] == lane))


def lane_config_counts() -> dict:
    return {lane: len(strategies_for_lane(lane)) for lane in EXECUTED_LANES}


# --------------------------------------------------------------------------- #
# Instrument declarations
# --------------------------------------------------------------------------- #
#: FX: currency code -> (economic group, cost tier). The XXXUSD series and its
#: quote direction are resolved by ``alpha_agent.r33.universe.resolve_fx``; the
#: short-rate leg is the OECD three-month interbank rate on FRED.
FX_UNIVERSE = {
    "EUR": ("FX_MAJOR", "FX_G10", "IR3TIB01EZM156N", "CP0000EZ19M086NEST"),
    "JPY": ("FX_MAJOR", "FX_G10", "IR3TIB01JPM156N", "JPNCPIALLMINMEI"),
    "GBP": ("FX_MAJOR", "FX_G10", "IR3TIB01GBM156N", "GBRCPIALLMINMEI"),
    "CHF": ("FX_MAJOR", "FX_G10", "IR3TIB01CHM156N", "CHECPIALLMINMEI"),
    "AUD": ("FX_COMMODITY_BLOC", "FX_G10", "IR3TIB01AUM156N",
            "AUSCPIALLQINMEI"),
    "CAD": ("FX_COMMODITY_BLOC", "FX_G10", "IR3TIB01CAM156N",
            "CANCPIALLMINMEI"),
    "NZD": ("FX_COMMODITY_BLOC", "FX_G10", "IR3TIB01NZM156N",
            "NZLCPIALLQINMEI"),
    "NOK": ("FX_COMMODITY_BLOC", "FX_G10", "IR3TIB01NOM156N",
            "NORCPIALLMINMEI"),
    "SEK": ("FX_EUROPE_MINOR", "FX_G10", "IR3TIB01SEM156N", "SWECPIALLMINMEI"),
    "DKK": ("FX_EUROPE_MINOR", "FX_G10", "IR3TIB01DKM156N", "DNKCPIALLMINMEI"),
    "CZK": ("FX_EUROPE_MINOR", "FX_EMERGING", "IR3TIB01CZM156N",
            "CZECPIALLMINMEI"),
    "HUF": ("FX_EUROPE_MINOR", "FX_EMERGING", "IR3TIB01HUM156N",
            "HUNCPIALLMINMEI"),
    "PLN": ("FX_EUROPE_MINOR", "FX_EMERGING", "IR3TIB01PLM156N",
            "POLCPIALLMINMEI"),
    "TRY": ("FX_EMERGING", "FX_EMERGING", "IR3TIB01TRM156N",
            "TURCPIALLMINMEI"),
    "ZAR": ("FX_EMERGING", "FX_EMERGING", "IR3TIB01ZAM156N",
            "ZAFCPIALLMINMEI"),
    "MXN": ("FX_EMERGING", "FX_EMERGING", "IR3TIB01MXM156N",
            "MEXCPIALLMINMEI"),
    "CLP": ("FX_EMERGING", "FX_EMERGING", "IR3TIB01CLM156N",
            "CHLCPIALLMINMEI"),
    "ILS": ("FX_EMERGING", "FX_EMERGING", "IR3TIB01ILM156N",
            "ISRCPIALLMINMEI"),
    "KRW": ("FX_ASIA", "FX_EMERGING", "IR3TIB01KRM156N", "KORCPIALLMINMEI"),
    "RUB": ("FX_EMERGING", "FX_EMERGING", "IR3TIB01RUM156N",
            "RUSCPIALLMINMEI"),
}
FX_BASE_SHORT_RATE = "IR3TIB01USM156N"
FX_BASE_CPI = "USACPIALLMINMEI"
#: CPI published quarterly rather than monthly; carried forward with the longer
#: publication lag rather than interpolated.
FX_QUARTERLY_CPI = ("AUSCPIALLQINMEI", "NZLCPIALLQINMEI")

#: Currencies deliberately NOT admitted, each with its MEASURED reason. Two
#: different kinds of exclusion live here and conflating them would misdescribe
#: the universe: three currencies fail Release 33's admissibility rules on the
#: numbers, and four have no comparable interest rate at all.
#:
#: The rate exclusions are the more interesting ones. Brazil and India DO have
#: FRED short-rate series - an overnight call rate and a discount rate - and
#: using them would have added two high-carry currencies to the cross-section.
#: They are refused because a carry ranking built from mixed tenors ranks rate
#: DEFINITIONS rather than carry, and the two currencies it would have added
#: are precisely the ones whose apparent carry is largest.
FX_EXCLUDED_BY_MEASUREMENT = {
    "CNY": "ADMINISTERED: spot repeats its previous close on 14.5 % of "
           "sessions, above the 10 % rule reused from Release 33",
    "MYR": "ADMINISTERED: spot repeats its previous close on 17.9 % of "
           "sessions",
    "HKD": "PEGGED: annualised volatility 0.6 %, below the 2 % floor; a carry "
           "book would load on a peg whose tail has not occurred in sample",
    "TWD": "NO_COMPARABLE_SHORT_RATE: Taiwan is not in the OECD "
           "three-month interbank series",
    "SGD": "NO_COMPARABLE_SHORT_RATE: Singapore is not in the OECD "
           "three-month interbank series",
    "BRL": "TENOR_MISMATCH: only an overnight/discount rate is published, and "
           "mixing tenors across the cross-section would rank rate "
           "definitions rather than carry",
    "INR": "TENOR_MISMATCH: only an overnight/discount rate is published",
}

#: Commodity curves: market -> (EIA series stem per contract, group, first
#: contract available). PROPANE TERMINATED in 2009 and is included precisely
#: because it did: a commodity panel built only from contracts that still trade
#: is survivorship-biased in the direction that flatters carry.
COMMODITY_CURVES = {
    "WTI_CRUDE": (("PET.RCLC1.D", "PET.RCLC2.D", "PET.RCLC3.D",
                   "PET.RCLC4.D"), "ENERGY_CRUDE", False),
    "HEATING_OIL": (("PET.EER_EPD2F_PE1_Y35NY_DPG.D",
                     "PET.EER_EPD2F_PE2_Y35NY_DPG.D",
                     "PET.EER_EPD2F_PE3_Y35NY_DPG.D",
                     "PET.EER_EPD2F_PE4_Y35NY_DPG.D"), "ENERGY_DISTILLATE",
                    False),
    "RBOB_GASOLINE": (("PET.EER_EPMRR_PE1_Y35NY_DPG.D",
                       "PET.EER_EPMRR_PE2_Y35NY_DPG.D",
                       "PET.EER_EPMRR_PE3_Y35NY_DPG.D",
                       "PET.EER_EPMRR_PE4_Y35NY_DPG.D"), "ENERGY_GASOLINE",
                      False),
    "NATURAL_GAS": (("NG.RNGC1.D", "NG.RNGC2.D", "NG.RNGC3.D", "NG.RNGC4.D"),
                    "ENERGY_GAS", False),
    "PROPANE": (("PET.EER_EPLLPA_PE1_Y44MB_DPG.D",
                 "PET.EER_EPLLPA_PE2_Y44MB_DPG.D",
                 "PET.EER_EPLLPA_PE3_Y44MB_DPG.D",
                 "PET.EER_EPLLPA_PE4_Y44MB_DPG.D"), "ENERGY_NGL", True),
}
COMMODITY_TERMINATED_MARKETS = ("PROPANE",)

#: The conventional gasoline contract (EPMR) was replaced by RBOB (EPMRR) when
#: NYMEX changed the deliverable in 2006. Splicing them would create a single
#: series spanning two different physical commodities, so they are NOT spliced
#: and only RBOB is admitted.
GASOLINE_CONTRACT_SPLICE_ALLOWED = False
GASOLINE_SPLICE_REASON = (
    "NYMEX replaced the conventional unleaded gasoline deliverable with RBOB "
    "in 2006; a spliced series would join two different physical contracts and "
    "the join would sit in the middle of the sample")

#: CFTC contract market codes for the positioning leg of the commodity and FX
#: lanes. Codes, never names: 067651 is 'CRUDE OIL, LIGHT SWEET' in 1995 and
#: 'WTI-PHYSICAL' in 2026.
CFTC_CODES = {
    "WTI_CRUDE": ("067651", "06765A", "06765T"),
    "HEATING_OIL": ("022651", "02265B"),
    "RBOB_GASOLINE": ("111659", "11165B"),
    "NATURAL_GAS": ("023651", "0233AT", "023391"),
    "EUR": ("099741",), "JPY": ("097741",), "GBP": ("096742",),
    "CHF": ("092741",), "AUD": ("232741",), "CAD": ("090741",),
    "NZD": ("112741",), "MXN": ("095741",), "BRL": ("102741",),
    "RUB": ("089741",), "ZAR": ("122741",),
}

#: Rates: the tradable duration-bucket legs (ICE BofA US Treasury total-return
#: indices) and the curve signal series (constant-maturity yields).
RATES_LEGS = {
    "UST_1_3Y": ("$IDCOT1TR", "%2YTCM"),
    "UST_3_7Y": ("$IDCOT3TR", "%5YTCM"),
    "UST_7_10Y": ("$IDCOT7TR", "%10YTCM"),
    "UST_10_20Y": ("$IDCOT10TR", "%10YTCM"),
    "UST_20Y_PLUS": ("$IDCOT20TR", "%30YTCM"),
}
RATES_CURVE_POINTS = ("%3MTCM", "%1YTCM", "%2YTCM", "%5YTCM", "%10YTCM",
                      "%30YTCM")
RATES_CONTROL_LEG = "$IDCOT7TR"
RATES_BREAKEVEN_LEGS = ("TIP", "IEF")
RATES_BREAKEVEN_SIGNAL = ("T10YIE", "DFII10")

#: Credit: the tradable index leg and the spread signals.
CREDIT_LEG = "$USBIGCORP"
CREDIT_HEDGE_LEGS = ("$IDCOT3TR", "$IDCOT7TR")
CREDIT_SPREAD_SIGNALS = ("BAA10Y", "%CCCHYS")

#: Volatility: the index term structure is free and long; the FUTURES curve
#: that would implement it is not entitled, and the short-volatility ETPs that
#: terminated are absent from the owned delisted database.
VOL_INDEX_LEGS = ("VIX", "VIX3M")
VOL_TRADABLE_LEG = "VIXY"
VOL_EQUITY_LEG = "SPY"
VIX_FUTURES_ENTITLED = False
SHORT_VOLATILITY_DIRECTION_TESTABLE = False
SHORT_VOLATILITY_BLOCK_REASON = (
    "the short-volatility exchange-traded products that terminated - XIV, "
    "TVIX, ZIV, VIIX - are absent from the owned delisted database, and SVXY "
    "was structurally re-levered from -1x to -0.5x in February 2018, so a "
    "short-volatility book built from what survives would be backfilled with "
    "exactly the instruments that did not blow up")

#: Cross-asset legs.
CROSS_ASSET_LEGS = {
    "EQUITY": "SPY", "TREASURY": "$IDCOT7TR", "GOLD": "XAUUSD",
    "COPPER_GOLD": "#CUGC", "CREDIT": "$USBIGCORP",
}

#: Crypto. Two assets, both priced from the Coinbase series the St. Louis Fed
#: republishes. BTC and ETH were the two largest crypto assets by market
#: capitalisation continuously over the sample, so admitting exactly them is
#: not a survivor selection - but any BROADER crypto cross-section would be.
CRYPTO_LEGS = ("CBBTCUSD", "CBETHUSD")
CRYPTO_BROAD_UNIVERSE_ADMISSIBLE = False
CRYPTO_BROAD_UNIVERSE_BLOCK_REASON = (
    "a current list of surviving tokens cannot be written back into history, "
    "and no free point-in-time listing/delisting record exists for the "
    "exchanges that would have to have traded them")

# --------------------------------------------------------------------------- #
# Free / owned sources and the money boundary
# --------------------------------------------------------------------------- #
MAY_SPEND_MONEY = False
MAY_START_PROVIDER_TRIAL = False
MAY_CREATE_PROVIDER_ACCOUNT = False
MAY_CHANGE_SUBSCRIPTION_TIER = False
MAY_ACQUIRE_FREE_PUBLIC_DATA = True
MAY_USE_EXISTING_ENTITLEMENTS = True

#: A configured API key is NOT an entitlement. Every source below is MEASURED by
#: :mod:`alpha_agent.r36.entitlements` against a real endpoint before this
#: release claims it has anything.
API_KEY_IMPLIES_ENTITLEMENT = False

EIA_NATURAL_GAS_BULK_URL = "https://www.eia.gov/opendata/bulk/NG.zip"
FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_API_KEY_ENV = ("FRED_API_KEY", "FRED_KEY", "ALFRED_API_KEY")
CBOE_INDEX_URL = ("https://cdn.cboe.com/api/global/us_indices/daily_prices/"
                  "%s_History.csv")

#: Routes probed for the native VIX futures settlement history, all of which
#: answered 403 or 404. Recorded so the entitlement block is a measurement.
CBOE_VX_FUTURES_ROUTES = (
    "https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/"
    "VX/VX_2020-01-02.csv",
    "https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/"
    "VX_2020-01-02.csv",
    "https://cdn.cboe.com/api/global/delayed_quotes/term_structure/VX.json",
    "https://cdn.cboe.com/data/us/futures/market_statistics/settlement/"
    "VX_2020-01-02.csv",
    "https://cdn.cboe.com/resources/futures/VX_2020-01-02.csv",
)

HTTP_TIMEOUT_SECONDS = _r35_contract.HTTP_TIMEOUT_SECONDS
HTTP_MIN_INTERVAL_SECONDS = _r35_contract.HTTP_MIN_INTERVAL_SECONDS
HTTP_USER_AGENT = _r35_contract.HTTP_USER_AGENT

#: Payloads Release 35 already acquired. They are LOCATED, not downloaded
#: again: a raw vendor archive is an input, and re-fetching it would change
#: bytes an earlier artifact was hashed against.
REUSED_R35_SOURCES = ("CFTC_COMMITMENTS_OF_TRADERS", "EIA_PETROLEUM_BULK")

# --------------------------------------------------------------------------- #
# Qualification gate - frozen before any economic result exists
# --------------------------------------------------------------------------- #
MIN_EXCESS_T_STAT = 2.0
MAX_SINGLE_SUBPERIOD_PNL_SHARE = _r33_contract.MAX_SINGLE_SUBPERIOD_CONTRIBUTION

QUALIFICATION_CONDITIONS = (
    "enough_decision_periods",
    "positive_after_cost_excess_vs_lane_control",
    "significant_after_cost_excess",
    "positive_after_cost_utility_improvement",
    "same_sign_in_both_chronological_halves",
    "survives_multiple_testing_procedure",
    "survives_cost_stress",
    "not_dependent_on_a_single_instrument",
    "not_dependent_on_a_single_subperiod",
    "point_in_time_integrity_pass",
)

#: A strategy that beats its lane control is a RESEARCH CANDIDATE. It is not
#: alpha, because every outcome period in this release has already been read by
#: Releases 31 to 35.
PREDICTIVE_DIAGNOSTIC_REQUIRED = True
ECONOMIC_IMPROVEMENT_REQUIRED = True

FDR_Q = _r33_contract.FDR_Q
BOOTSTRAP_RESAMPLES = _r33_contract.BOOTSTRAP_RESAMPLES
BOOTSTRAP_BLOCK_MEAN = _r33_contract.BOOTSTRAP_BLOCK_MEAN
BOOTSTRAP_SEED = 20360822
ONLY_POSITIVE_REJECTIONS_MAY_QUALIFY = True

# --------------------------------------------------------------------------- #
# Evidence honesty
# --------------------------------------------------------------------------- #
#: Historical outcomes through August 2026 have been read by Releases 31-35.
#: Opening a NEW MARKET does not make an already-consumed OUTCOME PERIOD unseen:
#: the researcher choosing which market to open has already seen what happened.
FRESH_UNSEEN_EVIDENCE_EXISTS = False
FRESH_UNSEEN_EVIDENCE_REASON = (
    "every historical outcome period this release scores has been available to "
    "the researcher throughout Releases 31 to 35; acquiring a new INSTRUMENT "
    "does not make an old OUTCOME unseen, and a lane chosen after five "
    "failures is not chosen independently of them")
A_FOLD_MAY_BE_CALLED_A_LOCKBOX = False
CHRONOLOGICAL_HALVES_ARE_A_STABILITY_CHECK_NOT_A_LOCKBOX = True

RESULT_PASS = "PASS"
RESULT_FAIL = "FAIL"
RESULT_NAMES = ("SYSTEM_RESULT", "RESEARCH_CANDIDATE_RESULT", "ALPHA_RESULT")
SYSTEM_AND_ALPHA_RESULTS_ARE_SEPARATE = True

VERDICT_EDGE_FOUND = "R36_NATIVE_MULTI_ASSET_EDGE_FOUND"
VERDICT_NO_EDGE = "R36_NO_NATIVE_MULTI_ASSET_EDGE"
VERDICT_PARTIAL = "R36_FRONTIER_PARTIALLY_CLOSED"
#: Alias so a reading table cannot drift from the verdict constant it explains.
VERDICT_PARTIALLY_CLOSED_READING = VERDICT_PARTIAL
VERDICT_BLOCKED_DATA = "R36_NATIVE_RESEARCH_BLOCKED_BY_DATA"
VERDICT_BLOCKED_ENTITLEMENT = "R36_NATIVE_RESEARCH_BLOCKED_BY_ENTITLEMENT"
VERDICT_INTEGRITY = "R36_DATA_INTEGRITY_BLOCKED"
PRIMARY_VERDICTS = (VERDICT_EDGE_FOUND, VERDICT_NO_EDGE, VERDICT_PARTIAL,
                    VERDICT_BLOCKED_DATA, VERDICT_BLOCKED_ENTITLEMENT,
                    VERDICT_INTEGRITY)

#: RESEARCH_CANDIDATE_RESULT may be PASS on historical evidence.
#: ALPHA_RESULT may not.
RESEARCH_CANDIDATE_PASS_REQUIRES = VERDICT_EDGE_FOUND
ALPHA_PASS_REQUIRES = VERDICT_EDGE_FOUND
ALPHA_PASS_ALSO_REQUIRES_INDEPENDENT_EVIDENCE = True

MAY_REGISTER_FORWARD_CANDIDATE = False
MAY_CREATE_SECOND_TRUE_FORWARD_STORE = False
MAY_PROMOTE_MODEL = False
MAY_ACTIVATE_SLEEVE = False
FORWARD_EVIDENCE_OWNER = "api/forward_evidence.py"
FORWARD_PREDICTION_OWNER = "api/forward_prediction_skill.py"


def genuinely_independent_evidence_exists() -> bool:
    """The single gate ``ALPHA_RESULT = PASS`` must pass through.

    It returns ``FRESH_UNSEEN_EVIDENCE_EXISTS``, which is False and is asserted
    False by the architecture audit and by the release tests. Making
    ``ALPHA_RESULT`` reachable therefore requires changing a declared contract
    term in a reviewed commit, which is the point.
    """
    return bool(FRESH_UNSEEN_EVIDENCE_EXISTS)


def verdict_ceiling_without_fresh_evidence() -> str:
    """The best terminal RESULT this release can reach, and why."""
    return RESULT_FAIL if not genuinely_independent_evidence_exists() \
        else RESULT_PASS


# --------------------------------------------------------------------------- #
# Contract construction
# --------------------------------------------------------------------------- #
def build(*, campaign_id: str = CAMPAIGN_ID, created_at: str,
          repo: Optional[Path] = None) -> dict:
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "created_at": created_at,
        "mission": (
            "close the global asset-class x strategy frontier with terminal "
            "states, and execute every native lane owned or free "
            "point-in-time data actually supports; an inventory is not a "
            "release and a coverage matrix is not an alpha success"),
        "inherited": dict(INHERITED),
        "inherited_evidence_rules": dict(INHERITED_EVIDENCE_RULES),
        "superseded_campaigns": dict(SUPERSEDED_CAMPAIGNS),
        "not_proven_by_r34_r35": list(NOT_PROVEN_BY_R34_R35),
        "implementation_levels": {
            "levels": list(LEVELS),
            "proxy_may_close_a_native_frontier":
                PROXY_MAY_CLOSE_A_NATIVE_FRONTIER,
            "proxy_closure_requires_proven_structure_preservation":
                PROXY_CLOSURE_REQUIRES_PROVEN_STRUCTURE_PRESERVATION,
        },
        "coverage": {
            "terminal_states": list(TERMINAL_STATES),
            "every_cell_must_be_terminal": EVERY_CELL_MUST_BE_TERMINAL,
            "ambiguous_cell_states_allowed": AMBIGUOUS_CELL_STATES_ALLOWED,
            "executable_remaining_states": list(EXECUTABLE_REMAINING_STATES),
            "strategy_families": list(STRATEGY_FAMILIES),
        },
        "lanes": {
            "executed_lanes": list(EXECUTED_LANES),
            "priority": list(LANE_PRIORITY),
            "cadence_sessions": dict(LANE_CADENCE),
            "cadence_reason": dict(LANE_CADENCE_REASON),
            "control": dict(LANE_CONTROL),
            "control_reason": LANE_CONTROL_REASON,
            "control_is_the_passive_hold_of_what_is_traded":
                CONTROL_IS_THE_PASSIVE_HOLD_OF_WHAT_IS_TRADED,
            "strategy_control_leg": dict(STRATEGY_CONTROL_LEG),
            "strategy_control_leg_reason": STRATEGY_CONTROL_LEG_REASON,
            "universal_spy_cash_control_allowed":
                UNIVERSAL_SPY_CASH_CONTROL_ALLOWED,
            "config_counts": lane_config_counts(),
        },
        "timing": {
            "implementation_lag_sessions": IMPLEMENTATION_LAG_SESSIONS,
            "non_overlapping_decisions": NON_OVERLAPPING_DECISIONS,
            "cot_publication_lag_days": COT_PUBLICATION_LAG_DAYS,
            "oecd_rate_publication_lag_months":
                OECD_RATE_PUBLICATION_LAG_MONTHS,
            "cpi_publication_lag_months": CPI_PUBLICATION_LAG_MONTHS,
            "quarterly_cpi_publication_lag_months":
                QUARTERLY_CPI_PUBLICATION_LAG_MONTHS,
            "eia_settlement_lag_sessions": EIA_SETTLEMENT_LAG_SESSIONS,
            "broadcast_lag_sessions": BROADCAST_LAG_SESSIONS,
            "prohibited_substitutions": list(PROHIBITED_SUBSTITUTIONS),
        },
        "admissibility": {
            "rules_are_reused_from_r33": ADMISSIBILITY_RULES_ARE_REUSED_FROM_R33,
            "max_zero_return_fraction": MAX_ZERO_RETURN_FRACTION,
            "min_annual_volatility": MIN_ANNUAL_VOLATILITY,
            "max_duplicate_correlation": MAX_DUPLICATE_CORRELATION,
            "min_trailing_observations": MIN_TRAILING_OBSERVATIONS,
            "min_cross_section": MIN_CROSS_SECTION,
            "lane_min_cross_section": dict(LANE_MIN_CROSS_SECTION),
            "min_decision_periods": MIN_DECISION_PERIODS,
        },
        "construction": {
            "constructions": list(CONSTRUCTIONS),
            "tercile_fraction": TERCILE_FRACTION,
            "max_gross_exposure": MAX_GROSS_EXPOSURE,
            "leverage_available": LEVERAGE_AVAILABLE,
            "parameters_are_pre_declared": PARAMETERS_ARE_PRE_DECLARED,
            "parameter_search_allowed": PARAMETER_SEARCH_ALLOWED,
            "adaptive_search_allowed": ADAPTIVE_SEARCH_ALLOWED,
            "deep_learning_in_scope": DEEP_LEARNING_IN_SCOPE,
            "neighbour_values_may_be_promoted": NEIGHBOUR_VALUES_MAY_BE_PROMOTED,
            "normalisation_is_trailing_only": NORMALISATION_IS_TRAILING_ONLY,
            "full_sample_statistics_allowed": FULL_SAMPLE_STATISTICS_ALLOWED,
            "trend_lookback_months": TREND_LOOKBACK_MONTHS,
            "trend_skip_months": TREND_SKIP_MONTHS,
            "reversal_lookback_months": REVERSAL_LOOKBACK_MONTHS,
            "value_lookback_months": VALUE_LOOKBACK_MONTHS,
            "vol_term_lookback_weeks": VOL_TERM_LOOKBACK_WEEKS,
            "crypto_trend_lookback_weeks": CRYPTO_TREND_LOOKBACK_WEEKS,
        },
        "economics": {
            "cost_base": COST_BASE,
            "cost_bps_per_side": dict(COST_BPS_PER_SIDE),
            "cost_sensitivity_multipliers": list(COST_SENSITIVITY_MULTIPLIERS),
            "cost_stress_multiplier": COST_STRESS_MULTIPLIER,
            "cash_yield_series": CASH_YIELD_SERIES,
            "risk_aversion": RISK_AVERSION,
            "primary_decision_statistic": PRIMARY_DECISION_STATISTIC,
            "primary_decision_formula": PRIMARY_DECISION_FORMULA,
            "excess_over_cash_may_rank": EXCESS_OVER_CASH_MAY_RANK,
            "control": CONTROL_VOL_MATCHED,
        },
        "strategies": {name: {"lane": spec[0],
                              "families": list(spec[1]),
                              "implementation_level": spec[2],
                              "construction": spec[3]}
                       for name, spec in sorted(STRATEGIES.items())},
        "budget": {
            "planned_config_total": PLANNED_CONFIG_TOTAL,
            "max_primary_configs": MAX_PRIMARY_CONFIGS,
            "denominator_counts_all_executed": DENOMINATOR_COUNTS_ALL_EXECUTED,
            "controls_enter_denominator": CONTROLS_ENTER_DENOMINATOR,
            "fdr_q": FDR_Q,
            "bootstrap_resamples": BOOTSTRAP_RESAMPLES,
            "bootstrap_block_mean": BOOTSTRAP_BLOCK_MEAN,
            "bootstrap_seed": BOOTSTRAP_SEED,
            "only_positive_rejections_may_qualify":
                ONLY_POSITIVE_REJECTIONS_MAY_QUALIFY,
        },
        "instruments": {
            "fx_universe": {k: list(v) for k, v in sorted(FX_UNIVERSE.items())},
            "fx_base_short_rate": FX_BASE_SHORT_RATE,
            "fx_base_cpi": FX_BASE_CPI,
            "fx_excluded_by_measurement": dict(FX_EXCLUDED_BY_MEASUREMENT),
            "commodity_curves": {k: list(v[0])
                                 for k, v in sorted(COMMODITY_CURVES.items())},
            "commodity_terminated_markets": list(COMMODITY_TERMINATED_MARKETS),
            "gasoline_contract_splice_allowed":
                GASOLINE_CONTRACT_SPLICE_ALLOWED,
            "gasoline_splice_reason": GASOLINE_SPLICE_REASON,
            "cftc_codes": {k: list(v) for k, v in sorted(CFTC_CODES.items())},
            "rates_legs": {k: list(v) for k, v in sorted(RATES_LEGS.items())},
            "credit_leg": CREDIT_LEG,
            "vol_index_legs": list(VOL_INDEX_LEGS),
            "vol_tradable_leg": VOL_TRADABLE_LEG,
            "vix_futures_entitled": VIX_FUTURES_ENTITLED,
            "short_volatility_direction_testable":
                SHORT_VOLATILITY_DIRECTION_TESTABLE,
            "short_volatility_block_reason": SHORT_VOLATILITY_BLOCK_REASON,
            "crypto_legs": list(CRYPTO_LEGS),
            "crypto_broad_universe_admissible":
                CRYPTO_BROAD_UNIVERSE_ADMISSIBLE,
            "crypto_broad_universe_block_reason":
                CRYPTO_BROAD_UNIVERSE_BLOCK_REASON,
        },
        "money": {
            "may_spend_money": MAY_SPEND_MONEY,
            "may_start_provider_trial": MAY_START_PROVIDER_TRIAL,
            "may_create_provider_account": MAY_CREATE_PROVIDER_ACCOUNT,
            "may_change_subscription_tier": MAY_CHANGE_SUBSCRIPTION_TIER,
            "may_acquire_free_public_data": MAY_ACQUIRE_FREE_PUBLIC_DATA,
            "may_use_existing_entitlements": MAY_USE_EXISTING_ENTITLEMENTS,
            "api_key_implies_entitlement": API_KEY_IMPLIES_ENTITLEMENT,
            "reused_r35_sources": list(REUSED_R35_SOURCES),
        },
        "qualification": {
            "conditions": list(QUALIFICATION_CONDITIONS),
            "min_excess_t_stat": MIN_EXCESS_T_STAT,
            "min_decision_periods": MIN_DECISION_PERIODS,
            "max_single_subperiod_pnl_share": MAX_SINGLE_SUBPERIOD_PNL_SHARE,
            "predictive_diagnostic_required": PREDICTIVE_DIAGNOSTIC_REQUIRED,
            "economic_improvement_required": ECONOMIC_IMPROVEMENT_REQUIRED,
        },
        "evidence_honesty": {
            "fresh_unseen_evidence_exists": FRESH_UNSEEN_EVIDENCE_EXISTS,
            "fresh_unseen_evidence_reason": FRESH_UNSEEN_EVIDENCE_REASON,
            "a_fold_may_be_called_a_lockbox": A_FOLD_MAY_BE_CALLED_A_LOCKBOX,
            "chronological_halves_are_a_stability_check_not_a_lockbox":
                CHRONOLOGICAL_HALVES_ARE_A_STABILITY_CHECK_NOT_A_LOCKBOX,
            "result_names": list(RESULT_NAMES),
            "system_and_alpha_results_are_separate":
                SYSTEM_AND_ALPHA_RESULTS_ARE_SEPARATE,
            "research_candidate_pass_requires": RESEARCH_CANDIDATE_PASS_REQUIRES,
            "alpha_pass_requires": ALPHA_PASS_REQUIRES,
            "alpha_pass_also_requires_independent_evidence":
                ALPHA_PASS_ALSO_REQUIRES_INDEPENDENT_EVIDENCE,
            "verdict_ceiling_without_fresh_evidence":
                verdict_ceiling_without_fresh_evidence(),
        },
        "forward": {
            "may_register_forward_candidate": MAY_REGISTER_FORWARD_CANDIDATE,
            "may_create_second_true_forward_store":
                MAY_CREATE_SECOND_TRUE_FORWARD_STORE,
            "may_promote_model": MAY_PROMOTE_MODEL,
            "may_activate_sleeve": MAY_ACTIVATE_SLEEVE,
            "forward_evidence_owner": FORWARD_EVIDENCE_OWNER,
            "forward_prediction_owner": FORWARD_PREDICTION_OWNER,
        },
        "verdicts": {"primary": list(PRIMARY_VERDICTS)},
        "environment": {
            "git_head": git_head(repo),
            "python": sys.version.split()[0],
            "platform": platform.platform(),
        },
        "research_root": str(r36.research_root()),
    }
    body = r36.artifact_body(CONTRACT_SCHEMA, payload)
    body["contract_hash"] = r36.sha(
        {k: v for k, v in payload.items() if k != "environment"})
    return body


def path_for(campaign_id: str = CAMPAIGN_ID) -> Path:
    return r36.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(contract: dict) -> Path:
    return r36.write_json(path_for(contract["campaign_id"]), contract)


def load(campaign_id: str = CAMPAIGN_ID) -> Optional[dict]:
    return r36.read_json(path_for(campaign_id))


def verify(contract: dict) -> dict:
    """Recompute the contract hash and report drift."""
    payload = {k: v for k, v in contract.items()
               if k not in ("schema", "release", "safety_block",
                            "contract_hash", "environment")}
    recomputed = r36.sha(payload)
    return {"declared": contract.get("contract_hash"),
            "recomputed": recomputed,
            "stable": recomputed == contract.get("contract_hash")}
