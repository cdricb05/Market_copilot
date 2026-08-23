"""alpha_agent.r39.contract - the frozen Release 39 campaign contract.

Everything that changes what a Release-39 number MEANS is declared here,
before any outcome is viewed: the evidence zones and their honest labels, the
staged search budget, the multiple-discovery policy, the verdict rules, and
every commercial/safety refusal. Modules read these constants; none may widen
them.

The operating principle, stated as constants rather than prose: there is NO
artificial limit on ideas (asset class, model family, representation, trade
structure) and a HARD limit on evidence (leakage, hindsight, survivorship,
uncontrolled search, test-set reuse, wrong benchmarks, fake significance,
unauthorized spend, production mutation).
"""
from __future__ import annotations

from ..r31 import contract as _r31c

CAMPAIGN_ID = "r39_universal_alpha_discovery_v1"
SUPERSEDED_CAMPAIGNS: dict = {}

# --------------------------------------------------------------------------- #
# The idea boundary is open; the evidence boundary is closed
# --------------------------------------------------------------------------- #
NO_ARTIFICIAL_ASSET_BOUNDARY = True
NO_ARTIFICIAL_MODEL_FAMILY_BOUNDARY = True
NO_HUMAN_HYPOTHESIS_BOUNDARY = True
SEARCH_CAPACITY_IS_NOT_EVIDENCE = True

NO_LEAKAGE = True
NO_HINDSIGHT = True
NO_SURVIVORSHIP_BIAS_UNDECLARED = True
NO_UNCONTROLLED_MULTIPLE_SEARCH = True
NO_TEST_SET_REUSE = True
NO_FAKE_SIGNIFICANCE = True
NO_UNAUTHORIZED_SPEND = True
NO_PRODUCTION_MUTATION = True

# --------------------------------------------------------------------------- #
# Evidence zones (all lanes share ONE calendar, inherited from the R38
# ML-ready partition so no lane can shop for a friendlier chronology)
# --------------------------------------------------------------------------- #
ZONE_A_DISCOVERY_END = "2007-02-23"       # inclusive last decision date
ZONE_B_VALIDATION_START = "2007-04-24"
ZONE_B_VALIDATION_END = "2016-11-01"
ZONE_C_CONFIRMATION_START = "2016-12-29"
ZONE_C_CONFIRMATION_END = "2026-07-09"
EMBARGO_SESSIONS = 21

#: Releases 31-38 have already read history through 2026 at the FAMILY level
#: (trend, carry, momentum, seasonality economics were computed full-sample by
#: R38 over these same markets). No model was ever fitted or selected on
#: Zone C in this estate, but the chronology is not virgin, and the label
#: refuses to pretend otherwise.
ZONE_C_EVIDENCE_LABEL = "HISTORICAL_CONFIRMATION_EVIDENCE"
ZONE_C_IS_FRESH_UNSEEN_EVIDENCE = False
TRUE_FORWARD_BEGINS_ONLY_AFTER_FREEZE = True
HISTORICAL_ALPHA_IS_NOT_TRUE_FORWARD_EVIDENCE = True

#: Zone-B evaluations are tracked per candidate and campaign-wide; the
#: search-burden correction consumes these counters as its denominator.
ZONE_B_REUSE_IS_TRACKED = True

# --------------------------------------------------------------------------- #
# Locked confirmation budget - the BUDGET constants are the Release-31
# lockbox discipline, imported so no release can quietly run a looser lockbox
# --------------------------------------------------------------------------- #
MAX_LOCKBOX_CANDIDATES = _r31c.MAX_LOCKBOX_CANDIDATES
MAX_LOCKBOX_PER_FAMILY = _r31c.MAX_LOCKBOX_PER_FAMILY
ONE_EXECUTION_PER_FINALIST = True
NO_REDESIGN_AND_RETRY = True

# --------------------------------------------------------------------------- #
# Staged search budget (Track 9) - hierarchical, ledgered, never brute force
# --------------------------------------------------------------------------- #
STAGE1_MAX_CANDIDATES = 2000          # cheap Zone-A screening ceiling
STAGE2_MAX_CANDIDATES = 160           # Zone-B economic validation ceiling
STAGE3_MAX_CANDIDATES = 40            # specialist/ensemble refinement ceiling
STAGE1_PROMOTION_FRACTION = 0.15      # top share promoted, subject to caps
FAMILY_DIVERSITY_FLOOR = 2            # best-k of EVERY family survive stage 1
OPTIMIZATION_ZONES = ("ZONE_A", "ZONE_B")
NEVER_OPTIMIZE_AGAINST_ZONE_C = True

SEARCH_BUDGET_COUNTERS = (
    "CANDIDATES_GENERATED", "CANDIDATES_SCREENED",
    "CANDIDATES_PROMOTED_TO_STAGE2", "CANDIDATES_PROMOTED_TO_STAGE3",
    "FINALISTS_FROZEN", "LOCKED_CONFIRMATIONS_RUN")

# --------------------------------------------------------------------------- #
# Multiple-discovery policy (Track 10)
# --------------------------------------------------------------------------- #
FDR_Q = _r31c.FDR_Q
BOOTSTRAP_RESAMPLES = _r31c.BOOTSTRAP_RESAMPLES
BOOTSTRAP_BLOCK_MEAN = _r31c.BOOTSTRAP_BLOCK_MEAN
SEEDS = {"bootstrap": _r31c.SEEDS["bootstrap"], "director": 3901,
         "representation": 3902, "models": 3903}

MULTIPLE_TESTING_POLICY = {
    "benjamini_hochberg": "q=%s over every LOCKED confirmation p-value; owner "
                          "alpha_agent.r31.multiple_testing" % FDR_Q,
    "superior_predictive_ability": "Hansen SPA over the finalist excess "
                                   "series; owner alpha_agent.r31."
                                   "multiple_testing",
    "deflated_sharpe": "Bailey & Lopez de Prado deflated Sharpe with the "
                       "effective trial count taken from the Zone-B reuse "
                       "ledger, never from the finalist count",
    "denominator": "every candidate that touched Zone B stays in the "
                   "denominator; rejected candidates are never deleted",
    "placebo": "the Fibonacci family is judged against placebo levels, and "
               "a level family that ties its placebo is PULLBACK STRUCTURE, "
               "not FIBONACCI",
}
EFFECTIVE_SEARCH_BURDEN_IS_REPORTED = True
T_ABOVE_2_IS_NOT_QUALIFICATION = True

# --------------------------------------------------------------------------- #
# Targets and horizons (Track 2)
# --------------------------------------------------------------------------- #
TARGETS = ("EXCESS_VS_CONTROL", "XS_RANK", "SIGN_AFTER_COST",
           "REALISED_VOL", "LOWER_TAIL_Q10")
PRIMARY_HORIZON_SESSIONS = 21
SECONDARY_HORIZON_SESSIONS = 63
VX_HORIZON_SESSIONS = 5
NO_COMBINATORIAL_TARGET_EXPLOSION = True

# --------------------------------------------------------------------------- #
# Trade expressions (Track 3)
# --------------------------------------------------------------------------- #
TRADE_EXPRESSIONS = ("TS_OUTRIGHT", "XS_LONG_SHORT", "CALENDAR_CURVE",
                     "GROUP_SPREAD", "REGIME_CONDITIONAL", "ABSTAIN_OVERLAY")
EVERY_EXPRESSION_IS_COSTED = True
EVERY_EXPRESSION_HAS_A_CONTROL = True
NO_UNCONSTRAINED_PAIR_EXPLOSION = True

# --------------------------------------------------------------------------- #
# Representation families (Track 4/5)
# --------------------------------------------------------------------------- #
REPRESENTATION_FAMILIES = (
    "RAW", "CLASSICAL", "AUTO_TRANSFORM", "SPECTRAL", "LATENT_PCA",
    "GRAPH_LEAD_LAG", "SYMBOLIC", "MARKET_STRUCTURE", "FIBONACCI",
    "MACRO_OVERLAY", "POSITIONING")
DEFERRED_REPRESENTATION_FAMILIES = ("SELF_SUPERVISED", "VISUAL_CHART")

FIB_LEVELS = (0.236, 0.382, 0.500, 0.618, 0.786)
PLACEBO_LEVELS = (0.330, 0.420, 0.550, 0.580, 0.650, 0.710)
FIB_REQUIRES_CONFIRMED_PIVOTS = True
FIB_TIE_WITH_PLACEBO_MEANS = "PULLBACK_STRUCTURE_MAY_MATTER"

# --------------------------------------------------------------------------- #
# Cost model - reused, labelled, stressed
# --------------------------------------------------------------------------- #
COST_BASE = "TRADED_NOTIONAL"           # Release 31 lesson: sells AND buys
COST_MODEL_STATE = "MODELLED_NOT_OBSERVED"
COST_STRESS_MULTIPLIER = 2.0
#: Futures rows carry their own per-market cost from the R38 panel; equity and
#: ETF lanes use the Release-34 measured-liquidity tiers via
#: alpha_agent.r34.economics. No third cost model exists.
FUTURES_COST_SOURCE = "r38_ml_ready_native_futures_panel.cost_bps_per_side"
EQUITY_COST_BPS_PER_SIDE = 10.0
ETF_COST_BPS_PER_SIDE = 5.0

# --------------------------------------------------------------------------- #
# Verdict rules
# --------------------------------------------------------------------------- #
SUCCESS_STATES = (
    "R39_AUTONOMOUS_ALPHA_DISCOVERED",
    "R39_HISTORICAL_ALPHA_CANDIDATES_DISCOVERED",
    "R39_INFORMATION_LIMIT_STILL_BINDING",
    "R39_COMPUTE_LIMIT_BINDING",
    "R39_NO_ROBUST_ALPHA_DESPITE_UNIVERSAL_SEARCH")
ALPHA_PASS_REQUIRES_VERDICT = "R39_AUTONOMOUS_ALPHA_DISCOVERED"
RESULT_AXES = ("SYSTEM_RESULT", "DATA_RESULT", "DISCOVERY_RESULT",
               "HISTORICAL_ALPHA_RESULT", "FORWARD_CANDIDATE_RESULT")
DO_NOT_FORCE_A_SUCCESS_STATE = True

# --------------------------------------------------------------------------- #
# Qualification - what a candidate must survive to be called historical Alpha
# --------------------------------------------------------------------------- #
QUALIFICATION_CONDITIONS = {
    "bh_survivor": "rejected by Benjamini-Hochberg (q=%s) over every locked "
                   "confirmation" % FDR_Q,
    "deflated_sharpe": "DSR >= %s with the Zone-B effective trial count"
                       % 0.95,
    "sign_stability": "same-sign after-control excess in halves of Zone C",
    "cost_stress": "after-cost excess stays positive at 2x modelled costs",
    "concentration": "no leave-one-market-out sign flip where applicable",
    "positive_excess": "the confirmed excess itself is positive",
}
DSR_QUALIFICATION_THRESHOLD = 0.95

# --------------------------------------------------------------------------- #
# Named exclusion reasons (Track 15) - a dataset may be excluded only for one
# --------------------------------------------------------------------------- #
EXCLUSION_REASONS = (
    "PIT_FAILURE", "SURVIVORSHIP_FAILURE", "INSUFFICIENT_HISTORY",
    "IDENTITY_FAILURE", "LICENSING", "NO_TRADABLE_IMPLEMENTATION",
    "INSUFFICIENT_SAMPLE", "UNAVAILABLE")

#: R38 cell/experiment integrity vocabulary (predecessor repair, Track 0).
RESEARCH_STATUSES = (
    "DIRECTLY_TESTED", "VALIDLY_REPRESENTED_BY_GROUP_TEST",
    "DATA_AVAILABLE_BUT_NOT_TESTED", "MISSING_REQUIRED_INFORMATION_LEG",
    "STILL_BLOCKED")
DATA_AVAILABLE_BUT_NOT_TESTED_IS_NOT_A_REJECTED_HYPOTHESIS = True

# --------------------------------------------------------------------------- #
# Compute strategy (Track 16)
# --------------------------------------------------------------------------- #
COMPUTE_CLASSES = ("CPU_FEASIBLE_NOW", "LOCAL_GPU_FEASIBLE",
                   "LOCAL_GPU_SLOW_BUT_POSSIBLE", "EXTERNAL_GPU_HIGH_VALUE",
                   "NOT_WORTH_COMPUTE")
MAY_PURCHASE_COMPUTE = False
COMPUTE_ESCALATION_STOPS_ONLY_THAT_BRANCH = True

# --------------------------------------------------------------------------- #
# Commercial and production refusals - every one is False and stays False
# --------------------------------------------------------------------------- #
MAY_SPEND_MONEY = False
MAY_START_PROVIDER_TRIAL = False
MAY_CREATE_PROVIDER_ACCOUNT = False
MAY_CHANGE_SUBSCRIPTION_TIER = False
MAY_RENEW_SUBSCRIPTION = False
MAY_ACCEPT_LICENCE_AGREEMENT = False
MAY_SUBMIT_PAYMENT_DETAILS = False
MAY_PURCHASE_CLOUD_COMPUTE = False
MAY_INSTALL_CUDA = False
MAY_DOWNLOAD_MODEL_WEIGHTS = False
MAY_UPGRADE_NORGATE_PACKAGES = False
MONEY_SPENT_BY_R39_USD = 0.0

PURCHASE_AUTHORITY_GRANTED_BY_THIS_RELEASE = False
RENEWAL_AUTHORITY_GRANTED_BY_THIS_RELEASE = False
MODEL_PROMOTED_BY_THIS_RELEASE = False
OPERATIONAL_WRITES_BY_THIS_RELEASE = 0

#: Free, licence-compatible OPEN-SOURCE packages installed into the project
#: venv for this release (Track 16 permits exactly this). Recorded here so the
#: dependency surface is a contract fact, not an accident.
PACKAGES_INSTALLED_FOR_R39 = (
    ("scikit-learn", "BSD-3-Clause"), ("scipy", "BSD-3-Clause"),
    ("xgboost", "Apache-2.0"), ("lightgbm", "MIT"),
    ("statsmodels", "BSD-3-Clause"))


def purchase_authority() -> dict:
    """Release 39 grants nothing commercial, ever."""
    return {"purchase_authorised": False,
            "renewal_authorised": False,
            "granted_by": None,
            "reason": "Release 39 is research-only; purchase and renewal "
                      "authority belong to the operator through the ONE "
                      "canonical gate (engine.data_expansion_gate)."}
