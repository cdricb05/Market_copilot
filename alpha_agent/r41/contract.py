"""alpha_agent.r41.contract - the frozen Release-41 contract.

Everything here is declared BEFORE any Release-41 outcome is computed and
hashed into the first artifact the campaign writes (``r40_closeout_import``),
so no rule below can be tuned after a result is seen:

* the R40 facts this release inherits and must VERIFY from the immutable
  artifacts (never trusted from a prompt);
* the THREE clocks and the multi-horizon target contract (a horizon exists
  only where genuine source frequency exists - no interpolated intraday);
* the MATERIAL_UPDATE definitions per strategy family;
* the evidence zones, the research-candidate gate and the qualified-alpha
  gate, the family-level and global search burdens (never reset: 230
  inherited);
* the cost model per lane, the control per expression, the alpha-killer
  battery and the blocker vocabulary;
* the free-sample acquisition conditions and the commercial refusals.
"""
from __future__ import annotations

from .. import r39 as _r39
from ..r39 import contract as _c39
from ..r40 import contract as _c40

CALCULATION_OWNER = "alpha_agent.r41.contract"

# --------------------------------------------------------------------------- #
# Inherited R40 facts - VERIFIED by closeout_import, never assumed
# --------------------------------------------------------------------------- #
R40_CAMPAIGN_ID = "r40_prospective_alpha_acceleration_v1"
R40_CLOSEOUT_COMMIT = "5f27ba4b0417032d84cb9503bbc18a2569235fbc"
R40_HANDOFF_DIR = r"D:\Temp\paper_trader_release40_prospective_alpha_handoff"
R40_RESEARCH_ROOT = r"D:\Stock_Prediction_app_data\prospective_alpha_r40"
R39_CONTINUATION_CAMPAIGN_ID = _c40.R39_CONTINUATION_CAMPAIGN_ID

R40_EXPECTED = {
    "cumulative_effective_trials": 230,
    "r39_inherited_effective_trials": 194,
    "r40_new_effective_trials": 36,
    "n_research_shadows": 5,
    "shadow_ids": ["shadow_wide_xs", "shadow_carry_rule_xs",
                   "shadow_vx_carry_ts", "shadow_intl_rates_carry_rv",
                   "shadow_slot5_c39_fad367467c79"],
    "shadow_candidates": ["c39_c9233eccaa74", "c39_8278ddd2d3b9",
                          "c39_0574796699fa", "c39_1a0105dd2f0c",
                          "c39_fad367467c79"],
    "terminal_states": ["R40_PROSPECTIVE_ENGINE_READY_WAITING_FOR_TIME",
                        "R40_NO_INCREMENTAL_EDGE_FOUND",
                        "R40_COMPUTE_LIMIT_BINDING"],
    "true_forward_rows_at_r40_close": 0,
    "first_eligible_forward_date": "2026-08-31",
}

BURDEN_NEVER_RESETS = True
GLOBAL_INHERITED_EFFECTIVE_TRIALS = 230
NO_CAMPAIGN_ID_LAUNDERING = True

# --------------------------------------------------------------------------- #
# THE CORRECTION - three clocks, cadence belongs to the candidate
# --------------------------------------------------------------------------- #
SYSTEM_CLOCKS = {
    "SIGNAL_REFRESH": "as frequently as the underlying information supports",
    "PORTFOLIO_REASSESSMENT": "after every MATERIAL signal/information "
                              "refresh (possibly several times per session)",
    "MODEL_RECALIBRATION": "only under controlled evidence gates",
}
SYSTEM_IS_NOT_MONTHLY = True
DECISION_CADENCE_IS_A_CANDIDATE_PROPERTY = True
MONTHLY_SHADOWS_DO_NOT_DEFINE_SYSTEM_CADENCE = True
NO_UPSAMPLING_OF_SLOW_STRATEGIES = True   # marks are not trades

# --------------------------------------------------------------------------- #
# Track 1 - multi-horizon target contract
# --------------------------------------------------------------------------- #
HORIZON_CLASSES = {
    "INTRADAY": ["1m", "5m", "15m", "30m", "60m", "2h", "4h"],
    "DAILY_SWING": ["1s", "2s", "5s", "10s", "21s"],
    "MEDIUM": ["42s", "63s"],
    "EVENT": ["NEXT_EVENT", "NEXT_VOL_SHOCK", "MACRO_ANNOUNCEMENT_WINDOW",
              "EARNINGS_REVISION_EVENT", "CURVE_DISLOCATION",
              "BREAKOUT_RETEST", "CONFIRMED_PIVOT_RETRACEMENT"],
}
#: sessions per horizon label (intraday in bars of the source frequency)
HORIZON_SESSIONS = {"1s": 1, "2s": 2, "5s": 5, "10s": 10, "21s": 21,
                    "42s": 42, "63s": 63}
HORIZON_MINUTES = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "60m": 60,
                   "2h": 120, "4h": 240}
#: A horizon exists for a family ONLY where actual history at that source
#: frequency exists. Daily prices interpolated to minutes are not data.
NO_INTERPOLATED_INTRADAY = True
HORIZON_REQUIRES_NATIVE_SOURCE_FREQUENCY = True
#: An intraday horizon needs at least this many bars of genuine history.
MIN_INTRADAY_BARS_FOR_RESEARCH = 50_000
#: A daily horizon needs at least this many decision dates in the fit zone.
MIN_DAILY_DECISIONS_FIT_ZONE = 250
#: Every family reports these eight facts (Track 1).
FAMILY_FREQUENCY_FACTS = ("SOURCE_FREQUENCY", "OBSERVABLE_LATENCY",
                          "EARLIEST_HISTORY", "LATEST_HISTORY",
                          "TARGET_HORIZONS_SUPPORTED",
                          "DECISION_HORIZONS_SUPPORTED",
                          "IMPLEMENTATION_LATENCY", "PIT_STATE")

# --------------------------------------------------------------------------- #
# Track 2 - MATERIAL_UPDATE per strategy family (research definitions)
# --------------------------------------------------------------------------- #
MATERIAL_UPDATE = {
    "RATES_RV": {
        "triggers": ["new settlement of any leg", "curve z-score break "
                     "(|z| crosses 2.0 on the residual)", "policy/macro "
                     "release inside the family's calendar", "hedge-ratio "
                     "drift beyond 10 % of the frozen ratio"],
        "refresh_interval": "1 session (daily settlements); intraday only "
                            "with native intraday futures",
    },
    "COMMODITY_CURVE": {
        "triggers": ["new settlement", "EIA weekly balance (Wed 10:30 ET) "
                     "/ CFTC COT (Fri 15:30 ET) publication", "front-to-"
                     "second slope change beyond 1 rolling SD",
                     "volume/OI migration between tenors (> 20 % in a "
                     "session)", "roll-exit date reached"],
        "refresh_interval": "1 session",
    },
    "VOLATILITY_OPTIONS": {
        "triggers": ["VIX / VX settlement", "term-structure slope sign "
                     "change", "realised-vs-implied gap beyond 1 SD",
                     "scheduled macro release window", "VX roll"],
        "refresh_interval": "1 session (daily indices) / weekly (VX)",
    },
    "INTRADAY_STRUCTURE": {
        "triggers": ["confirmed causal pivot", "price touches a declared "
                     "level band (ATR-scaled)", "realised-volatility "
                     "regime change", "session open / close windows"],
        "refresh_interval": "per bar (1-60 minutes) where native bars exist",
    },
    "EQUITY_EVENTS": {
        "triggers": ["earnings release", "consensus revision publication",
                     "filing", "index rebalance"],
        "refresh_interval": "event-driven; daily between events",
    },
    "FX": {"triggers": ["new fixing / settlement", "rate decision",
                        "macro surprise", "forward-points change"],
           "refresh_interval": "1 session (daily) / per bar (ticks)"},
    "CREDIT": {"triggers": ["new OAS print", "rating action", "equity/vol "
                            "shock beyond 2 SD"],
               "refresh_interval": "1 session"},
    "CRYPTO": {"triggers": ["funding settlement (8h)", "basis break",
                            "OI/liquidation shock", "listing/delisting"],
               "refresh_interval": "per bar / per funding interval"},
}
OPERATING_SEQUENCE = ("UPDATE", "INCREMENTAL_FEATURE_REFRESH",
                      "AFFECTED_SIGNAL_RESCORE", "FULL_OPPORTUNITY_FRONTIER",
                      "PORTFOLIO_REASSESSMENT")
R41_IS_RESEARCH_ONLY = True
MUTATES_OPERATIONAL_PORTFOLIO = False

# --------------------------------------------------------------------------- #
# Evidence zones (Track 12/13) - declared once, per family, by RULE
# --------------------------------------------------------------------------- #
#: Per family, decision dates are split chronologically A/B/C = 50/30/20.
#: ZONE_A: fit + screening; ZONE_B: selection (every distinct candidate
#: scored here is a burden trial); ZONE_C: lockbox, one access per candidate
#: lineage, opened only after the pre-gate. An embargo of the longest target
#: horizon separates the zones.
ZONE_SPLIT = {"ZONE_A": 0.50, "ZONE_B": 0.30, "ZONE_C": 0.20}
ZONE_EMBARGO_RULE = "max target horizon of the family, in sessions/bars"
ZONE_C_PREGATE_T = 2.5
ZONE_C_ONE_ACCESS_PER_LINEAGE = True
ZONE_C_NEVER_READ_FOR_SELECTION = True

#: Research-candidate gate (freeze-worthy; NOT Alpha) - all must hold on
#: ZONE_B with Zone-A-only fitting.
RESEARCH_CANDIDATE_GATE = {
    "after_cost_excess_t_hac_min": 2.0,
    "hac_lags": "max(4, horizon_sessions) Bartlett; XS rows clustered by "
                "timestamp",
    "same_sign_halves": True,
    "positive_at_2x_cost": True,
    "min_effective_decisions": 60,
    "kill_tests_no_sign_flip": True,
    "factor_residual_t_min": 1.5,
    "family_bh_q": 0.10,
}
#: Qualified historical alpha (HISTORICAL_ALPHA_RESULT = PASS) requires, in
#: addition, the lockbox confirmation and burden-adjusted survival.
QUALIFIED_ALPHA_GATE = {
    "zone_c_after_cost_excess_t_min": 2.0,
    "zone_c_same_sign_as_zone_b": True,
    "deflated_sharpe_at_family_burden_min": 0.95,
    "deflated_sharpe_at_global_burden_reported": True,
    "bh_survivor_within_family": True,
    "factor_residual_t_min": 2.0,
    "positive_at_3x_cost": True,
}
HISTORICAL_PASS_REQUIRES = "QUALIFIED_ALPHA_GATE_ON_ZONE_C_AT_FAMILY_BURDEN"
PROSPECTIVE_PASS_REQUIRES = _c40.PROSPECTIVE_PASS_REQUIRES
RESEARCH_CANDIDATE_IS_NOT_ALPHA = True

#: Search-burden families (Track 12). GLOBAL = 230 + every new ZONE_B
#: evaluation in any family; FAMILY = the family's own ZONE_B evaluations.
BURDEN_FAMILIES = ("RATES_RV", "COMMODITY_CURVE", "VOLATILITY_OPTIONS",
                   "EQUITY_REVISIONS", "MICROSTRUCTURE", "FX", "CRYPTO",
                   "CREDIT", "MODEL_FAMILY", "HORIZON_FAMILY")
LINEAGE_FIELDS = ("information_family", "asset_family", "horizon",
                  "economic_expression", "representation", "model",
                  "hyperparameter_budget", "parent_hypotheses",
                  "validation_touches")

# --------------------------------------------------------------------------- #
# Costs (bps per side on TRADED NOTIONAL - modelled, labelled) and controls
# --------------------------------------------------------------------------- #
COST_BPS_PER_SIDE = {
    "TREASURY_FUTURES": 0.75, "INTERNATIONAL_GOVERNMENT": 1.5,
    "SHORT_RATE_FUTURES": 0.5, "INTERNATIONAL_SHORT_RATE": 1.0,
    "ENERGY": 3.0, "GRAINS_AND_OILSEEDS": 4.0, "SOFTS": 6.0,
    "LIVESTOCK": 6.0, "PRECIOUS_METALS": 2.0, "INDUSTRIAL_METALS": 3.0,
    "FX_FUTURES": 1.0, "US_INDEX_FUTURES": 0.75, "INTL_INDEX_FUTURES": 2.0,
    "VIX_FUTURES_TERM_STRUCTURE": 5.0, "CRYPTO_FUTURES": 8.0,
    "EMISSIONS": 5.0, "COMMODITY_INDEX": 3.0,
    "FX_SPOT_INTRADAY": "observed half-spread from tick bid/ask + 0.2",
    "INDEX_CFD_INTRADAY": "observed half-spread from tick bid/ask + 0.5",
    "CRYPTO_SPOT_INTRADAY": 5.0, "CREDIT_ETF": 2.0, "EQUITY_ETF": 1.0,
    "US_EQUITY_SINGLE_NAME": 5.0,
}
COST_STRESS_MULTIPLIERS = (1.0, 2.0, 3.0)
COST_BASE_IS_TRADED_NOTIONAL = True
CONTROLS = {
    "RV_SELF_FINANCED": "zero (cash) - a duration/curve-neutral spread "
                        "book is self-financing",
    "XS_LONG_SHORT": "risk-matched cash",
    "TS_DIRECTIONAL": "volatility-matched passive long of the same "
                      "instrument set",
    "CURVE_SPREAD": "zero after duration/roll neutralisation",
    "VOL_PREMIUM": "volatility-matched short-VX passive",
    "INTRADAY_TS": "zero (flat) - with the intraday passive drift reported",
    "CREDIT_RV": "duration-matched Treasury",
}
NO_UNIVERSAL_SPY_CONTROL = True

# --------------------------------------------------------------------------- #
# Track 15 - alpha killer battery (applied where applicable)
# --------------------------------------------------------------------------- #
ALPHA_KILLER_TESTS = (
    "LEAVE_ONE_MARKET_OUT", "LEAVE_ONE_COUNTRY_OUT", "LEAVE_ONE_YEAR_BLOCK_OUT",
    "LEAVE_ONE_REGIME_OUT", "COST_X2", "COST_X3", "LATENCY_ONE_BAR",
    "SPREAD_X2", "DATE_PERTURBATION", "PLACEBO_FEATURE", "PLACEBO_LEVEL",
    "ALTERNATIVE_ROLL_RULE", "FACTOR_RESIDUALISATION", "CLUSTER_BOOTSTRAP",
    "FEATURE_FAMILY_ABLATION",
)
RESULT_STRENGTHENS_ONLY_BY_SURVIVING_ATTEMPTS_TO_DESTROY_IT = True

# --------------------------------------------------------------------------- #
# Fibonacci / market structure (Track 6)
# --------------------------------------------------------------------------- #
FIB_NAMED_LEVELS = (0.236, 0.382, 0.500, 0.618, 0.786, 1.272, 1.618)
FIB_PLACEBO_LEVELS = (0.300, 0.450, 0.550, 0.700, 0.850, 1.150, 1.450)
PIVOT_CONFIRMATION_RULE = "causal: a swing extreme is confirmed only after "\
                          "K bars have closed beyond it by >= 1 ATR; the "\
                          "event is stamped at the CONFIRMATION bar"
NO_HINDSIGHT_EXTREMA = True
NO_HUMAN_VISUAL_CONFIRMATION = True
FIB_QUESTION = ("conditional on the volatility regime, does price reaction "
                "near a NAMED level contain information beyond generic "
                "pullback geometry (the placebo levels)?")

# --------------------------------------------------------------------------- #
# Free sample acquisition (Track 3) - EIGHT conditions, all must hold
# --------------------------------------------------------------------------- #
MAY_DOWNLOAD_FREE_PUBLIC_SAMPLES = True
SAMPLE_ACQUISITION_CONDITIONS = (
    "ZERO_MONETARY_COST",
    "NO_ACCOUNT_CREATION",
    "NO_PAYMENT_DETAIL",
    "NO_LICENCE_CLICK_THROUGH_ON_OPERATORS_BEHALF",
    "PUBLIC_TERMS_PERMIT_RESEARCH_USE",
    "PROVIDER_RATE_LIMITS_RESPECTED",
    "STORAGE_ON_RESEARCH_DRIVE",
    "PROVENANCE_URL_TIME_HASH_RECORDED",
)
#: Existing entitlements whose keys are present in the operator's shell may
#: be used READ-ONLY within their free/paid tier; no tier may be changed.
EXISTING_ENTITLEMENT_KEYS = ("NORGATE (local NDU)", "FRED_API_KEY",
                             "EODHD_API_KEY", "POLYGON_API_KEY",
                             "TIINGO_API_KEY", "FINNHUB_API_KEY",
                             "FMP_API_KEY", "ALPHAVANTAGE_API_KEY",
                             "NASDAQ_DATA_LINK_API_KEY")
MAY_CHANGE_ENTITLEMENT_TIER = False

BLOCKER_VOCAB = (
    "PAYMENT_REQUIRED", "ACCOUNT_REQUIRED", "LICENCE_REQUIRED",
    "HISTORICAL_DATA_UNAVAILABLE", "PIT_INTEGRITY_FAILURE",
    "SURVIVORSHIP_FAILURE", "IDENTITY_FAILURE",
    "COMPUTE_REQUIRES_OPERATOR_SPEND", "FUTURE_TIME_REQUIRED",
    "SAFETY_BLOCKER", "IRREPARABLE_TECHNICAL_FAILURE",
)
BRANCH_STATES = ("EXECUTED",) + BLOCKER_VOCAB
A_FAILED_CANDIDATE_IS_A_ROUTING_EVENT = True

# --------------------------------------------------------------------------- #
# Compute (Track 11)
# --------------------------------------------------------------------------- #
MAY_PURCHASE_COMPUTE = False
MAY_PURCHASE_CLOUD_COMPUTE = False
MAY_INSTALL_CUDA = False
R40_GPU_ESTIMATE_IS_NOT_AUTHORITY_TO_SPEND = True
LOCAL_COMPUTE = "i3-10105F 8 threads, 68.6 GB RAM, torch CPU on D:"

# --------------------------------------------------------------------------- #
# Automation / commercial refusals (inherited verbatim)
# --------------------------------------------------------------------------- #
MAY_ENABLE_SCHEDULED_TASK = False
MAY_MODIFY_PRODUCTION_SCHEDULER = False
MAY_RESTART_PRODUCTION = False
MAY_CREATE_ORDER = False
MAY_CHANGE_HOLDINGS = False
MAY_PROMOTE_MODEL = False
MAY_SPEND_MONEY = _c39.MAY_SPEND_MONEY
MAY_START_PROVIDER_TRIAL = _c39.MAY_START_PROVIDER_TRIAL
MAY_CREATE_PROVIDER_ACCOUNT = _c39.MAY_CREATE_PROVIDER_ACCOUNT
MAY_ACCEPT_LICENCE_AGREEMENT = _c39.MAY_ACCEPT_LICENCE_AGREEMENT
MAY_SUBMIT_PAYMENT_DETAILS = _c39.MAY_SUBMIT_PAYMENT_DETAILS
MAY_PURCHASE_DATA = False
MAY_SEND_VENDOR_EMAIL = False

# --------------------------------------------------------------------------- #
# Result axes (never collapsed)
# --------------------------------------------------------------------------- #
RESULT_AXES = ("SYSTEM_RESULT", "DATA_FRONTIER_RESULT",
               "RESEARCH_CANDIDATE_RESULT", "HISTORICAL_ALPHA_RESULT",
               "PROSPECTIVE_ALPHA_RESULT", "INFORMATION_RESULT",
               "CADENCE_RESULT", "EXPRESSION_RESULT", "MODEL_RESULT",
               "PURCHASE_RESULT")
TERMINAL_STATES = ("R41_ALPHA_CANDIDATE_FOUND", "R41_NO_QUALIFIED_ALPHA_YET",
                   "R41_INFORMATION_LIMIT_BINDING",
                   "R41_COMPUTE_LIMIT_BINDING", "R41_TIME_LIMIT_BINDING")
DO_NOT_FORCE_A_SUCCESS_STATE = True
NO_QUALIFIED_ALPHA_YET_REQUIRES_BRANCH_MATRIX = True

TWENTY_TWO_QUESTIONS = (
    "Which assets can currently be researched intraday?",
    "Which need new data?",
    "Which provider/dataset best unlocks intraday futures?",
    "Which provider/dataset best unlocks options surfaces?",
    "Has the Steele analyst-revision sample been received/tested?",
    "What is the strongest rates-RV result at EACH supported horizon?",
    "What is the strongest commodity-curve result at EACH supported horizon?",
    "What is the strongest volatility/options result at EACH supported "
    "horizon?",
    "Was intraday Fibonacci tested against placebo where genuine data "
    "permitted?",
    "Did Fibonacci itself add information beyond generic market structure?",
    "Which relative-value structures beat outright prediction?",
    "Which model families add incremental value?",
    "Did larger temporal models improve TCN?",
    "Which microstructure features add incremental value?",
    "What survives factor adjustment?",
    "What survives cost/latency stress?",
    "What survives search-burden adjustment?",
    "Which candidates are ready for prospective shadow evidence?",
    "What frequency will their forward evidence accumulate at?",
    "What remaining blocker is INFORMATION vs MODEL vs COMPUTE vs TIME?",
    "What exact data purchase has the highest expected research value per "
    "dollar?",
    "Prove that no material $0 executable research branch was left undone.",
)

WINDOWS_POWERSHELL_ONLY = True
SHELL_POLICY_EVENTS = {"bash_tool_invocations_by_claude": 0,
                       "monitor_tool_invocations": 0,
                       "note": "counts are EVIDENCE from the transcript; the "
                               "handoff validator recomputes them"}


def contract_hash() -> str:
    """Hash of every frozen rule above."""
    return _r39.sha({
        "R40_EXPECTED": R40_EXPECTED,
        "HORIZON_CLASSES": HORIZON_CLASSES,
        "MATERIAL_UPDATE": MATERIAL_UPDATE,
        "ZONE_SPLIT": ZONE_SPLIT,
        "ZONE_C_PREGATE_T": ZONE_C_PREGATE_T,
        "RESEARCH_CANDIDATE_GATE": RESEARCH_CANDIDATE_GATE,
        "QUALIFIED_ALPHA_GATE": QUALIFIED_ALPHA_GATE,
        "BURDEN_FAMILIES": BURDEN_FAMILIES,
        "COST_BPS_PER_SIDE": COST_BPS_PER_SIDE,
        "CONTROLS": CONTROLS,
        "ALPHA_KILLER_TESTS": ALPHA_KILLER_TESTS,
        "FIB_NAMED_LEVELS": FIB_NAMED_LEVELS,
        "FIB_PLACEBO_LEVELS": FIB_PLACEBO_LEVELS,
        "SAMPLE_ACQUISITION_CONDITIONS": SAMPLE_ACQUISITION_CONDITIONS,
        "BLOCKER_VOCAB": BLOCKER_VOCAB,
        "GLOBAL_INHERITED_EFFECTIVE_TRIALS": GLOBAL_INHERITED_EFFECTIVE_TRIALS,
    })
