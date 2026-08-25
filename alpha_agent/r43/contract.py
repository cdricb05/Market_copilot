"""alpha_agent.r43.contract - the FROZEN Release-43 contract.

Everything a later result could be accused of having been chosen to fit is
declared HERE, and this module is hashed into
``r43_frozen_contract.json`` before the first Release-43 number exists.
Nothing in this file may be edited once that artifact is written; the audit
guard and the regression both re-derive the hash from this source.

The single most important declaration in the file is
:data:`COLLATERAL_CLASSES`. Release 42 proved that a book's denominator is
part of its claim. It did NOT prove that carry is uneconomic, and reading it
that way would be a second error as large as the first one. The distinction
is the REMUNERATION OF COLLATERAL:

  * ``UNREMUNERATED_FULLY_FUNDED`` - coin and stablecoin collateral pays
    nothing. Committed capital forgoes the entire risk-free rate, so the
    control is the risk-free rate and the correction is a SUBTRACTION.
    (Crypto cash-and-carry: the R42 case.)
  * ``REMUNERATED_MARGIN`` - exchange-traded futures margin is posted in
    T-bills at an FCM and PAYS the risk-free rate. Committed capital forgoes
    nothing, so the control is zero and the correction is a RESCALE of the
    return from traded notional onto committed capital.
  * ``FUNDED_LONG_SHORT_EQUITY`` - a cash-neutral equity long/short posts
    Reg-T/portfolio margin, earns a short rebate below the risk-free rate
    and pays stock borrow. The control is the shortfall, not the whole rate.

Declaring this in the contract, before any number, is what stops Release 43
from selecting the collateral story that makes its own results look best.
"""
from __future__ import annotations

from ..r39 import contract as _c39
from ..r41 import contract as _c41

CALCULATION_OWNER = "alpha_agent.r43.contract"

RELEASE = "release43"
CAMPAIGN_ID = "r43_global_alpha_offensive_v1"
MISSION = ("locate the strongest ECONOMICALLY IMPLEMENTABLE alpha available "
           "to the estate today, across information families, markets, "
           "economic expressions and horizons, and identify exactly which "
           "new information would most raise the probability of finding "
           "stronger alpha tomorrow")

RESEARCH_ONLY = True
IS_ANOTHER_CRYPTO_RELEASE = False
IS_ANOTHER_CARRY_RELEASE = False
CENTRED_ON_ONE_STRATEGY_FAMILY = False

# --------------------------------------------------------------------------- #
# Search burden - inherited, verified from the artifact, never reset
# --------------------------------------------------------------------------- #
#: 230 pre-R41 effective trials + 59 distinct R41 ZONE_B candidates. Both
#: halves are VERIFIED at import time by :mod:`alpha_agent.r43.burden`
#: against the R41 ledger on disk; a mismatch is a hard failure, never a
#: silently accepted number.
GLOBAL_INHERITED_EFFECTIVE_TRIALS = 289
INHERITED_PRE_R41 = 230
INHERITED_R41_DISTINCT = 59
BURDEN_NEVER_RESETS = True
NO_CAMPAIGN_ID_LAUNDERING = True
R41_LEDGER_IS_READ_ONLY = True

#: Family denominators. The first ten are R41's, carried forward unchanged so
#: a family's history remains visible across releases; the last four are
#: scientifically distinct families opened by this release.
BURDEN_FAMILIES = tuple(_c41.BURDEN_FAMILIES) + (
    "EVENT_DRIVEN", "CROSS_ASSET", "TECHNICAL_STRUCTURE", "EQUITY_RESIDUAL",
)
INHERITED_FAMILY_COUNTS = {
    "RATES_RV": 14, "CRYPTO": 10, "FX": 11, "MICROSTRUCTURE": 12,
    "VOLATILITY_OPTIONS": 4, "COMMODITY_CURVE": 6, "CREDIT": 1,
    "MODEL_FAMILY": 1, "EQUITY_REVISIONS": 0, "HORIZON_FAMILY": 0,
    "EVENT_DRIVEN": 0, "CROSS_ASSET": 0, "TECHNICAL_STRUCTURE": 0,
    "EQUITY_RESIDUAL": 0,
}
LINEAGE_FIELDS = tuple(_c41.LINEAGE_FIELDS)

# --------------------------------------------------------------------------- #
# THE COLLATERAL DECLARATION - the heart of this release
# --------------------------------------------------------------------------- #
#: ``committed_capital`` is expressed per unit of ONE LEG's traded notional
#: for outright/funded books and per unit of GROSS traded notional for
#: margin books; ``collateral_earns_rf`` decides whether the risk-free rate
#: is a control (subtract) or already earned (rescale only).
COLLATERAL_CLASSES = {
    "UNREMUNERATED_FULLY_FUNDED": {
        "committed_capital": 1.35,
        "collateral_earns_rf": 0.0,
        "control": "RISK_FREE_RATE",
        "correction": "SUBTRACT_RISK_FREE_RATE_AND_RESCALE",
        "applies_to": ("CRYPTO_CASH_AND_CARRY", "CRYPTO_SPOT"),
        "evidence": "alpha_agent.r42.capital / R42 CAPITAL_EFFICIENCY_REPORT "
                    "- spot coin and stablecoin margin pay nothing",
    },
    "REMUNERATED_MARGIN": {
        "committed_capital": None,     # measured: SPAN-like margin + buffer
        "collateral_earns_rf": 1.0,
        "control": "ZERO",
        "correction": "RESCALE_ONTO_COMMITTED_MARGIN_ONLY",
        "applies_to": ("FUTURES_OUTRIGHT", "FUTURES_CURVE_SPREAD",
                       "FUTURES_CROSS_MARKET_RV", "FX_FUTURES_CARRY",
                       "VX_TERM_STRUCTURE"),
        "evidence": "exchange-traded futures margin is posted in T-bills at "
                    "an FCM and is remunerated; the futures price already "
                    "embeds cost of carry",
    },
    "FUNDED_LONG_SHORT_EQUITY": {
        "committed_capital": 1.00,     # Reg-T style: 100% of gross/2 per side
        "collateral_earns_rf": 0.60,   # short rebate net of borrow, declared
        "control": "RISK_FREE_SHORTFALL",
        "correction": "SUBTRACT_UNREMUNERATED_FRACTION",
        "applies_to": ("EQUITY_MARKET_NEUTRAL", "EQUITY_RESIDUAL"),
        "evidence": "a cash-neutral equity long/short earns a short rebate "
                    "below the risk-free rate and pays general-collateral "
                    "borrow; the shortfall is the honest control",
    },
}
PRIMARY_COLLATERAL_BY_EXPRESSION = {
    "FUTURES_CURVE_SPREAD": "REMUNERATED_MARGIN",
    "FUTURES_CROSS_MARKET_RV": "REMUNERATED_MARGIN",
    "FUTURES_OUTRIGHT": "REMUNERATED_MARGIN",
    "FX_FUTURES_CARRY": "REMUNERATED_MARGIN",
    "VX_TERM_STRUCTURE": "REMUNERATED_MARGIN",
    "CRYPTO_CASH_AND_CARRY": "UNREMUNERATED_FULLY_FUNDED",
    "EQUITY_MARKET_NEUTRAL": "FUNDED_LONG_SHORT_EQUITY",
    "EQUITY_RESIDUAL": "FUNDED_LONG_SHORT_EQUITY",
}
COLLATERAL_CHOICE_IS_PREDECLARED = True
COLLATERAL_MAY_NOT_BE_CHOSEN_AFTER_SEEING_RESULTS = True

#: Conservative futures margin, as a fraction of ONE LEG's notional, by cost
#: group. Declared as an initial-margin-plus-buffer bound rather than read
#: from a live exchange table (which is not point-in-time). A book's
#: committed capital is the SUM over legs plus the declared stress buffer.
FUTURES_MARGIN_FRACTION = {
    "TREASURY_FUTURES": 0.020, "INTERNATIONAL_GOVERNMENT": 0.025,
    "SHORT_RATE_FUTURES": 0.004, "INTERNATIONAL_SHORT_RATE": 0.006,
    "ENERGY": 0.100, "GRAINS_AND_OILSEEDS": 0.070, "SOFTS": 0.090,
    "LIVESTOCK": 0.060, "PRECIOUS_METALS": 0.070,
    "INDUSTRIAL_METALS": 0.080, "FX_FUTURES": 0.030,
    "US_INDEX_FUTURES": 0.070, "INTL_INDEX_FUTURES": 0.080,
    "VIX_FUTURES_TERM_STRUCTURE": 0.180, "CRYPTO_FUTURES": 0.400,
    "EMISSIONS": 0.120, "COMMODITY_INDEX": 0.060,
}
MARGIN_STRESS_BUFFER_MULTIPLIER = 2.0   # capital = 2x initial margin
MARGIN_FLOOR_FRACTION_OF_GROSS = 0.02   # never claim less than 2% of gross
CAPITAL_MODELS_REPORTED = ("TRADED_NOTIONAL", "COMMITTED_MARGIN",
                           "COMMITTED_MARGIN_X2", "GROSS_EXPOSURE")
PRIMARY_CAPITAL_MODEL = "COMMITTED_MARGIN_X2"

#: Every candidate is quoted on BOTH conventions so R41/R42 comparisons stay
#: like-for-like and neither convention can be selected after the fact.
DUAL_QUOTATION_REQUIRED = True

# --------------------------------------------------------------------------- #
# Costs and controls - inherited unchanged from the R41 owner
# --------------------------------------------------------------------------- #
COST_BPS_PER_SIDE = dict(_c41.COST_BPS_PER_SIDE)
COST_STRESS_MULTIPLIERS = tuple(_c41.COST_STRESS_MULTIPLIERS)
COST_BASE_IS_TRADED_NOTIONAL = True
CONTROLS = dict(_c41.CONTROLS)
CONTROLS.update({
    "FUNDED_CARRY": "the risk-free rate the committed capital forgoes",
    "MARGIN_FINANCED_RV": "zero - margin is remunerated; capital is the "
                          "denominator, not a charge",
    "EQUITY_MARKET_NEUTRAL": "the unremunerated fraction of committed "
                             "capital (short-rebate shortfall + borrow)",
})
NO_UNIVERSAL_SPY_CONTROL = True
A_POSITIVE_NOMINAL_RETURN_IS_NOT_ALPHA = True
A_HIGH_SHARPE_IS_NOT_ALPHA = True
PRIMARY_METRIC = ("IMPLEMENTABLE RESIDUAL RETURN ON CONSERVATIVE COMMITTED "
                  "CAPITAL AGAINST THE CORRECT ECONOMIC CONTROL")

# --------------------------------------------------------------------------- #
# Zones, horizons, gates - inherited conventions, unchanged
# --------------------------------------------------------------------------- #
ZONE_SPLIT = dict(_c41.ZONE_SPLIT)
ZONE_EMBARGO_RULE = _c41.ZONE_EMBARGO_RULE
ZONE_C_PREGATE_T = _c41.ZONE_C_PREGATE_T
ZONE_C_ONE_ACCESS_PER_LINEAGE = True
ZONE_C_NEVER_READ_FOR_SELECTION = True

HORIZON_SESSIONS = dict(_c41.HORIZON_SESSIONS)
HORIZON_MINUTES = dict(_c41.HORIZON_MINUTES)
SYSTEM_IS_NOT_MONTHLY = True
DECISION_CADENCE_IS_A_CANDIDATE_PROPERTY = True
NO_UPSAMPLING_OF_SLOW_INFORMATION = True
NO_DOWNSAMPLING_AWAY_SHORT_LIVED_INFORMATION = True
HORIZON_REQUIRES_NATIVE_SOURCE_FREQUENCY = True

RESEARCH_CANDIDATE_GATE = dict(_c41.RESEARCH_CANDIDATE_GATE)
QUALIFIED_ALPHA_GATE = dict(_c41.QUALIFIED_ALPHA_GATE)
QUALIFIED_ALPHA_GATE["net_residual_on_committed_capital_positive"] = True
HISTORICAL_PASS_REQUIRES = ("QUALIFIED_ALPHA_GATE ON ZONE_C AT FAMILY "
                            "BURDEN, ON COMMITTED CAPITAL, AGAINST THE "
                            "PREDECLARED CONTROL")
PROSPECTIVE_PASS_REQUIRES = _c41.PROSPECTIVE_PASS_REQUIRES
RESEARCH_CANDIDATE_IS_NOT_ALPHA = True

# --------------------------------------------------------------------------- #
# Lane declarations - predeclared search budget per lane
# --------------------------------------------------------------------------- #
#: ``advance_t`` is the ZONE_A screening bar; ``cap`` is the MAXIMUM number
#: of candidates that lane may ever score on ZONE_B (each one a burden
#: trial). Both are frozen here; a lane that wants more must fail instead.
LANES = {
    "A_CARRY_REJUDGMENT": {
        "track": "A", "family": "FX",
        "question": "which historical carry survivors, if any, beat cash on "
                    "the capital they immobilise",
        "advance_t": 1.5, "cap": 10, "expressions": ("FUTURES_CURVE_SPREAD",
                                                     "FX_FUTURES_CARRY",
                                                     "VX_TERM_STRUCTURE")},
    "B_OPTIONS_VOL_SURFACE": {
        "track": "B", "family": "VOLATILITY_OPTIONS",
        "question": "does a $0-accessible option surface exist that supports "
                    "a variance-risk-premium-controlled candidate",
        "advance_t": 1.5, "cap": 6, "expressions": ("VOL_PREMIUM",)},
    "C_ANALYST_REVISIONS": {
        "track": "C", "family": "EQUITY_REVISIONS",
        "question": "are point-in-time analyst expectation vintages testable",
        "advance_t": 1.5, "cap": 6, "expressions": ("EQUITY_MARKET_NEUTRAL",)},
    "D_NATIVE_INTRADAY_FUTURES": {
        "track": "D", "family": "HORIZON_FAMILY",
        "question": "does native intraday futures history change any prior "
                    "conclusion",
        "advance_t": 1.5, "cap": 6, "expressions": ("FUTURES_OUTRIGHT",)},
    "E_RATES_RV": {
        "track": "E", "family": "RATES_RV",
        "question": "after-cost capital-adjusted residual international "
                    "rates relative value",
        "advance_t": 1.5, "cap": 14, "expressions": ("FUTURES_CURVE_SPREAD",
                                                     "FUTURES_CROSS_MARKET_RV")},
    "F_COMMODITY_CURVES": {
        "track": "F", "family": "COMMODITY_CURVE",
        "question": "do SPARSE, high-conviction, slow-turnover curve books "
                    "survive the cost that killed R41's diversified book",
        "advance_t": 1.5, "cap": 12, "expressions": ("FUTURES_CURVE_SPREAD",)},
    "G_MICROSTRUCTURE": {
        "track": "G", "family": "MICROSTRUCTURE",
        "question": "can maker execution be modelled without fabricating a "
                    "fill",
        "advance_t": 1.5, "cap": 4, "expressions": ("INTRADAY_TS",)},
    "H_EVENT_DRIVEN": {
        "track": "H", "family": "EVENT_DRIVEN",
        "question": "which leg becomes temporarily mispriced RELATIVE to "
                    "related markets around a scheduled macro release",
        "advance_t": 1.5, "cap": 10, "expressions": ("FUTURES_CROSS_MARKET_RV",
                                                     "FUTURES_CURVE_SPREAD")},
    "I_CROSS_ASSET": {
        "track": "I", "family": "CROSS_ASSET",
        "question": "does a sparse, economically motivated cross-asset "
                    "relation convert into a tradeable residual",
        "advance_t": 1.5, "cap": 12, "expressions": ("FUTURES_CROSS_MARKET_RV",)},
    "J_TECHNICAL_STRUCTURE": {
        "track": "J", "family": "TECHNICAL_STRUCTURE",
        "question": "do named technical levels beat PREDECLARED placebo "
                    "levels on causal pivots",
        "advance_t": 1.5, "cap": 8, "expressions": ("FUTURES_OUTRIGHT",)},
    "L_EQUITY_NEUTRAL": {
        "track": "L", "family": "EQUITY_RESIDUAL",
        "question": "does a sector/beta-neutral residual equity expression "
                    "survive on funded long/short capital",
        "advance_t": 1.5, "cap": 10, "expressions": ("EQUITY_MARKET_NEUTRAL",)},
    "M_CREDIT": {
        "track": "M", "family": "CREDIT",
        "question": "is native credit information reachable at $0",
        "advance_t": 1.5, "cap": 4, "expressions": ("CREDIT_RV",)},
    "N_CRYPTO_NONCARRY": {
        "track": "N", "family": "CRYPTO",
        "question": "does any NON-funding-carry crypto hypothesis survive "
                    "the R42 committed-capital treatment",
        "advance_t": 1.5, "cap": 6, "expressions": ("CRYPTO_CASH_AND_CARRY",)},
}
TOTAL_ZONE_B_BUDGET = sum(v["cap"] for v in LANES.values())
LANE_CAP_IS_A_CEILING_NOT_A_TARGET = True
A_FAILED_LANE_IS_A_ROUTING_EVENT = True
ONE_LANE_MAY_NOT_HALT_ANOTHER = True

# --------------------------------------------------------------------------- #
# Track Q - the kill battery
# --------------------------------------------------------------------------- #
ALPHA_KILLER_TESTS = tuple(_c41.ALPHA_KILLER_TESTS) + (
    "ALTERNATIVE_ECONOMIC_CONTROL", "CAPITAL_HURDLE_X2",
    "COLLATERAL_REMUNERATION_ZERO", "PARAMETER_NEIGHBOURHOOD",
    "LEAVE_ONE_ASSET_OUT", "SIGNAL_LAG_PERTURBATION",
)
KILL_TESTS_ARE_CHOSEN_BEFORE_RESULTS = True
DO_NOT_PROTECT_PROMISING_RESULTS = True

#: A candidate is DESTROYED if any of these hold on its judged zone.
KILL_CRITERIA = {
    "sign_flip_on_any_leave_one_out": True,
    "negative_at_2x_cost": True,
    "negative_under_alternative_control": True,
    "placebo_indistinguishable": "placebo |t| >= 0.8 * candidate |t|",
    "parameter_neighbourhood_median_t_below": 1.0,
}

# --------------------------------------------------------------------------- #
# Track J - placebo levels (predeclared, never re-chosen)
# --------------------------------------------------------------------------- #
FIB_NAMED_LEVELS = tuple(_c41.FIB_NAMED_LEVELS)
FIB_PLACEBO_LEVELS = tuple(_c41.FIB_PLACEBO_LEVELS)
PIVOT_CONFIRMATION_RULE = _c41.PIVOT_CONFIRMATION_RULE
NO_HINDSIGHT_EXTREMA = True
NO_HUMAN_VISUAL_CONFIRMATION = True

# --------------------------------------------------------------------------- #
# Track R - prospective freeze
# --------------------------------------------------------------------------- #
MAX_NEW_SHADOWS = 4
SHADOW_STATE = "RESEARCH_SHADOW_ONLY"
PROMOTION_ALLOWED = False
NEVER_BACKFILL_PROSPECTIVE_ROWS = True
NEVER_REWRITE_FROZEN_PREDICTIONS = True
NEVER_CHANGE_FROZEN_CANDIDATE_PARAMETERS = True
PRIOR_SHADOWS_ARE_IMMUTABLE = True
FREEZE_REQUIRES = {
    "zone_b_excess_t_hac_min": 2.0,
    "positive_on_committed_capital": True,
    "positive_at_2x_cost": True,
    "survives_full_kill_battery": True,
    "pit_status": "PIT_TRUE",
}

# --------------------------------------------------------------------------- #
# Data / PIT integrity
# --------------------------------------------------------------------------- #
PIT_CHECKS = ("source identity", "timestamp semantics",
              "availability timestamp", "revision policy",
              "corporate-action handling", "contract identity",
              "symbol mapping", "delisted/inactive coverage", "timezone",
              "holiday/session alignment", "licensing/provenance")
NO_FABRICATED_EVIDENCE = True
NO_HINDSIGHT_RECONSTRUCTION = True
NO_CURRENT_SNAPSHOT_AS_HISTORICAL_VINTAGE = True
NO_HIDDEN_SURVIVORSHIP = True
NO_INTERPOLATED_INTRADAY = True

MAY_DOWNLOAD_FREE_PUBLIC_SAMPLES = True
SAMPLE_ACQUISITION_CONDITIONS = tuple(_c41.SAMPLE_ACQUISITION_CONDITIONS)
MAY_PURCHASE_DATA = False
MAY_CREATE_PROVIDER_ACCOUNT = False
MAY_ACCEPT_LICENCE_AGREEMENT = False
MAY_SUBMIT_PAYMENT_DETAILS = False
MAY_START_PROVIDER_TRIAL = False
MAY_SEND_VENDOR_EMAIL = False
MAY_PURCHASE_COMPUTE = False
MAY_PURCHASE_CLOUD_COMPUTE = False

BLOCKER_VOCAB = (
    "PAYMENT_REQUIRED", "ACCOUNT_REQUIRED", "LICENCE_REQUIRED",
    "HISTORICAL_DATA_UNAVAILABLE", "PIT_INTEGRITY_FAILURE",
    "SURVIVORSHIP_FAILURE", "IDENTITY_FAILURE", "COMPUTE_SPEND_REQUIRED",
    "FUTURE_TIME_REQUIRED", "SAFETY_BLOCKER",
    "IRREPARABLE_TECHNICAL_FAILURE",
)
BRANCH_STATES = ("EXECUTED",) + BLOCKER_VOCAB
NO_ALPHA_FOUND_IS_NOT_A_GLOBAL_STOP = True
GLOBAL_STOP_REQUIRES = ("every material $0 executable lane has either "
                        "produced evidence or reached a specific external "
                        "blocker from BLOCKER_VOCAB")

# --------------------------------------------------------------------------- #
# Safety
# --------------------------------------------------------------------------- #
MAY_CREATE_ORDER = False
MAY_CREATE_PAPER_ORDER = False
MAY_CHANGE_HOLDINGS = False
MAY_PROMOTE_MODEL = False
MAY_ACTIVATE_SLEEVE = False
MAY_MODIFY_PRODUCTION_SCHEDULER = False
MAY_RESTART_PRODUCTION = False
MAY_CONNECT_BROKER = False
MAY_SPEND_MONEY = _c39.MAY_SPEND_MONEY
MAY_MUTATE_OPERATIONAL_STORE = False
MAY_MUTATE_PRIOR_RELEASE_ARTIFACT = False

WINDOWS_POWERSHELL_ONLY = True
SHELL_POLICY = ("PowerShell only. NO Bash, WSL, Git Bash or sh - including "
                "any Unix shell hidden inside a monitor or background tool. "
                "Prior releases' disclosures are never erased.")
SHELL_POLICY_MEASURED_BY = ("DISTINCT tool-use identity and timestamp, not "
                            "naive transcript character position")
INHERITED_SHELL_POLICY_DISCLOSURES = (
    {"release": "release42",
     "event": "one read-only grep through the Bash tool during initial R41 "
              "reconnaissance, before any R42 code existed",
     "wrote_anything": False, "erased": False},
)

# --------------------------------------------------------------------------- #
# Result axes and terminal states
# --------------------------------------------------------------------------- #
RESULT_AXES = ("SYSTEM_RESULT", "CAPITAL_TREATMENT_RESULT",
               "CARRY_REJUDGMENT_RESULT", "RATES_RV_RESULT",
               "COMMODITY_CURVE_RESULT", "EVENT_DRIVEN_RESULT",
               "CROSS_ASSET_RESULT", "TECHNICAL_STRUCTURE_RESULT",
               "EQUITY_RESIDUAL_RESULT", "OPTIONS_DATA_RESULT",
               "ANALYST_REVISION_DATA_RESULT", "NATIVE_INTRADAY_DATA_RESULT",
               "MICROSTRUCTURE_RESULT", "CREDIT_DATA_RESULT",
               "SEARCH_ADJUSTED_RESULT", "HISTORICAL_ALPHA_RESULT",
               "TRUE_FORWARD_RESULT")
NEVER_COLLAPSE_RESULT_AXES = True

TERMINAL_STATES = (
    "R43_QUALIFIED_ALPHA_FOUND",
    "R43_STRONG_CANDIDATE_FORWARD_PENDING",
    "R43_MULTIPLE_RESEARCH_CANDIDATES_FOUND",
    "R43_NEW_INFORMATION_IMPROVES_FRONTIER",
    "R43_RELATIVE_VALUE_DOMINATES_DIRECTION",
    "R43_OPTIONS_DATA_WALL_BINDING",
    "R43_ANALYST_REVISION_DATA_WALL_BINDING",
    "R43_NATIVE_INTRADAY_DATA_WALL_BINDING",
    "R43_OWNED_INFORMATION_FRONTIER_EXHAUSTED",
    "R43_NO_QUALIFIED_ALPHA_AFTER_GLOBAL_OFFENSIVE",
)
DO_NOT_FORCE_A_SUCCESS_STATE = True

TWENTY_QUESTIONS = (
    "1 DID WE FIND QUALIFIED ALPHA?",
    "2 WHAT IS THE STRONGEST IMPLEMENTABLE CANDIDATE?",
    "3 WHAT IS THE STRONGEST HISTORICAL CANDIDATE THAT STILL NEEDS FORWARD "
    "CONFIRMATION?",
    "4 WHICH INFORMATION FAMILY ADDED THE MOST INCREMENTAL VALUE?",
    "5 WHICH ASSET CLASS HAS THE HIGHEST ALPHA DENSITY?",
    "6 WHICH HORIZON HAS THE HIGHEST ALPHA DENSITY?",
    "7 DID RELATIVE-VALUE EXPRESSIONS OUTPERFORM OUTRIGHT DIRECTION?",
    "8 DID ANY COMPLEX MODEL ADD MATERIAL ECONOMIC VALUE ABOVE SIMPLE RULES?",
    "9 DID ANY OLD CARRY SURVIVE THE R42 CAPITAL/CASH JUDGE?",
    "10 DID OPTIONS DATA PRODUCE A SERIOUS CANDIDATE?",
    "11 DID ANALYST REVISION DATA BECOME TESTABLE?",
    "12 DID NATIVE INTRADAY FUTURES CHANGE ANY PRIOR CONCLUSION?",
    "13 DID MICROSTRUCTURE SURVIVE REALISTIC EXECUTION?",
    "14 DID EVENT-DRIVEN RESEARCH PRODUCE A SURVIVOR?",
    "15 DID CROSS-ASSET RELATIONAL RESEARCH PRODUCE A TRADEABLE RESIDUAL?",
    "16 WHAT NEW CANDIDATES WERE FROZEN AS RESEARCH SHADOWS?",
    "17 WHAT EXACT DATASET PURCHASE HAS THE HIGHEST EXPECTED INFORMATION "
    "VALUE?",
    "18 WHAT MATERIAL BRANCH IS STILL UNTESTED?",
    "19 WHY IS IT UNTESTED?",
    "20 WHAT IS THE SINGLE HIGHEST-VALUE RELEASE 44?",
)

#: Fields every row of the final cross-family frontier must carry.
FRONTIER_FIELDS = (
    "RANK", "CANDIDATE_ID", "ASSET", "ASSET_CLASS", "HORIZON",
    "INFORMATION_FAMILY", "ECONOMIC_EXPRESSION", "MODEL", "GROSS_RETURN",
    "FULL_COST", "COMMITTED_CAPITAL", "CASH_HURDLE", "NET_RESIDUAL_ALPHA",
    "VOLATILITY", "SHARPE", "T_STAT", "SEARCH_ADJUSTMENT", "ROBUSTNESS",
    "CAPACITY", "PIT_STATUS", "FORWARD_READY", "QUALIFICATION_STATE",
)
RANK_BY = "evidence-weighted implementable economic value, NEVER Sharpe alone"


def frozen_body() -> dict:
    """The exact dictionary that is hashed into the frozen contract."""
    import inspect
    import sys
    mod = sys.modules[__name__]
    out = {}
    for name, val in inspect.getmembers(mod):
        if name.startswith("_") or inspect.ismodule(val) \
                or inspect.isfunction(val) or inspect.isclass(val):
            continue
        if name.isupper():
            out[name] = val
    return out
