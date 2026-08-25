"""alpha_agent.r44.contract - the FROZEN Release-44 contract.

Everything in this module is declared BEFORE any Release-44 number exists.
It is hashed by :func:`alpha_agent.r44.closeout.contract_hash` and the hash
is written into every artifact, so a reader can prove that the rules were
not chosen after the results.

The single most important thing this file does is defend Engine 2 against
itself. A "portfolio of weak edges" is trivially easy to fake: score fifty
streams, keep the twelve that worked, weight them by something that looks
principled, and publish the aggregate. That is not a portfolio test - it is
the same search with an extra step. This contract therefore fixes, in
advance:

  * WHICH streams exist, by economics, including the ones R43 already
    killed - :data:`STREAMS`;
  * that every stream uses the CONTINUOUS expression, so not one threshold
    is chosen anywhere in the release - :data:`NO_THRESHOLD_IS_CHOSEN`;
  * which streams are STRUCTURAL PREMIA and therefore belong to the
    control rather than to the alpha claim - the ``role`` field;
  * the eight combination rules and WHICH ONE IS PRIMARY -
    :data:`COMBINATION_RULES`, :data:`PRIMARY_COMBINATION_RULE`;
  * that weights are fitted on ZONE_A+ZONE_B and the lockbox is opened once.
"""
from __future__ import annotations

CALCULATION_OWNER = "alpha_agent.r44.contract"
RELEASE = "release44"
CAMPAIGN_ID = "r44_orthogonal_portfolio_alpha_v1"

MISSION = (
    "Determine whether Alpha is hiding in NEW INFORMATION, in the "
    "DIVERSIFICATION OF WEAK EDGES, in LESS-EFFICIENT MARKETS, or nowhere "
    "accessible to this research estate - and prove whichever answer is true."
)

# --------------------------------------------------------------------------- #
# Safety - identical in force to R43, restated so this release cannot drift
# --------------------------------------------------------------------------- #
RESEARCH_ONLY = True
MAY_SPEND_MONEY = False
MAY_PURCHASE_DATA = False
MAY_START_PROVIDER_TRIAL = False
MAY_CREATE_PROVIDER_ACCOUNT = False
MAY_SUBMIT_PAYMENT_DETAILS = False
MAY_ACCEPT_LICENCE_AGREEMENT = False
MAY_SEND_VENDOR_EMAIL = False
MAY_PURCHASE_COMPUTE = False
MAY_PURCHASE_CLOUD_COMPUTE = False
MAY_CREATE_ORDER = False
MAY_CREATE_PAPER_ORDER = False
MAY_CONNECT_BROKER = False
MAY_CHANGE_HOLDINGS = False
MAY_CREATE_CAPITAL_ALLOCATION = False
MAY_ACTIVATE_SLEEVE = False
MAY_PROMOTE_MODEL = False
MAY_RESTART_PRODUCTION = False
MAY_MODIFY_PRODUCTION_SCHEDULER = False
MAY_MUTATE_OPERATIONAL_STORE = False
MAY_MUTATE_PRIOR_RELEASE_ARTIFACT = False
MAY_DOWNLOAD_FREE_PUBLIC_SAMPLES = True
DEFAULT_AUTHORIZED_SPEND_USD = 0.0

WINDOWS_POWERSHELL_ONLY = True
SHELL_POLICY = (
    "Windows PowerShell only. No Bash, WSL, Git Bash, sh, or any Unix shell "
    "hidden inside background or monitor tooling. The canonical Python "
    "interpreter may be invoked FROM PowerShell."
)
SHELL_POLICY_MEASURED_BY = (
    "distinct tool-use identity and timestamp, never naive transcript "
    "position"
)
#: R42 disclosed exactly one read-only Bash event during R41 reconnaissance.
#: It is inherited as a historical disclosure and is NOT erased.
INHERITED_SHELL_POLICY_DISCLOSURES = 1

# --------------------------------------------------------------------------- #
# Inherited search burden - read from R43's own artifact, never retyped
# --------------------------------------------------------------------------- #
#: The value R43 finalised. :mod:`alpha_agent.r44.burden` re-derives this from
#: the R43 ledger's own bytes and REFUSES to run if the ledger disagrees.
GLOBAL_INHERITED_EFFECTIVE_TRIALS = 302
INHERITED_PRE_R41 = 230
INHERITED_R41_DISTINCT = 59
INHERITED_R43_DISTINCT = 13
BURDEN_NEVER_RESETS = True
NO_CAMPAIGN_ID_LAUNDERING = True
PRIOR_LEDGERS_ARE_READ_ONLY = True

#: R43's fourteen families plus the four this release opens.
BURDEN_FAMILIES = [
    "RATES_RV", "COMMODITY_CURVE", "VOLATILITY_OPTIONS", "EQUITY_REVISIONS",
    "MICROSTRUCTURE", "FX", "CRYPTO", "CREDIT", "MODEL_FAMILY",
    "HORIZON_FAMILY", "EVENT_DRIVEN", "CROSS_ASSET", "TECHNICAL_STRUCTURE",
    "EQUITY_RESIDUAL",
    # opened by Release 44
    "OPTIONS_VOL", "INTRADAY_EVENT", "NATIVE_CREDIT",
    "LESS_EFFICIENT_MARKETS", "PORTFOLIO_SYNTHESIS",
]

INHERITED_FAMILY_COUNTS = {
    "RATES_RV": 19, "COMMODITY_CURVE": 6, "VOLATILITY_OPTIONS": 4,
    "EQUITY_REVISIONS": 0, "MICROSTRUCTURE": 12, "FX": 13, "CRYPTO": 10,
    "CREDIT": 1, "MODEL_FAMILY": 1, "HORIZON_FAMILY": 0, "EVENT_DRIVEN": 0,
    "CROSS_ASSET": 2, "TECHNICAL_STRUCTURE": 2, "EQUITY_RESIDUAL": 2,
    "OPTIONS_VOL": 0, "INTRADAY_EVENT": 0, "NATIVE_CREDIT": 0,
    "LESS_EFFICIENT_MARKETS": 0, "PORTFOLIO_SYNTHESIS": 0,
}

LINEAGE_FIELDS = [
    "information_family", "asset_family", "horizon", "economic_expression",
    "representation", "model", "hyperparameter_budget", "parent_hypotheses",
    "validation_touches",
]

#: Combination is a searched family. Pretending otherwise is how a
#: "portfolio result" launders a hundred implicit trials into one headline.
PORTFOLIO_SYNTHESIS_IS_A_SEARCHED_FAMILY = True

# --------------------------------------------------------------------------- #
# Zones - fitted on A+B, the lockbox opened once
# --------------------------------------------------------------------------- #
ZONE_SPLIT = {"ZONE_A": 0.5, "ZONE_B": 0.3, "ZONE_C": 0.2}
FIT_ZONES = ("ZONE_A", "ZONE_B")
LOCK_ZONE = "ZONE_C"
ZONE_EMBARGO_SESSIONS = 21
ZONE_C_NEVER_READ_FOR_SELECTION = True
ZONE_C_ONE_ACCESS_PER_LINEAGE = True
WEIGHTS_ARE_FITTED_ON_FIT_ZONES_ONLY = True
NO_OPTIMISATION_ON_THE_HOLDOUT = True

# --------------------------------------------------------------------------- #
# ENGINE 2A - the residual stream inventory, declared by ECONOMICS
# --------------------------------------------------------------------------- #
#: Not one threshold, band, lookback or hyper-parameter is chosen anywhere in
#: Release 44. Every stream uses its family's CONTINUOUS expression with the
#: window constants its R43 owner already froze. A release that tunes nothing
#: cannot be accused of tuning its way to a portfolio.
NO_THRESHOLD_IS_CHOSEN = True
STREAM_EXPRESSION = "CONTINUOUS"

#: Losers are INCLUDED. R43 killed several of these on their own merits;
#: excluding them because they failed is precisely the selection bias that
#: makes a portfolio result meaningless.
LOSERS_ARE_INCLUDED = True
SELECTION_ON_MEASURED_PERFORMANCE_IS_FORBIDDEN = True

#: role = PREMIUM  -> belongs to the STRUCTURAL-PREMIUM CONTROL portfolio
#: role = RESIDUAL -> belongs to the PORTFOLIO-ALPHA claim
#: A stream may not change role after its numbers are seen.
STREAMS = (
    # ---- rates relative value: duration-neutral SPREAD books, self-financing
    {"id": "S01_RATES_RV_CARRY", "role": "RESIDUAL", "family": "RATES_RV",
     "asset_class": "RATES", "owner": "alpha_agent.r43.rv",
     "build": ("rv", {"kind": "RATES", "signal": "CARRY_XS"}),
     "expression": "FUTURES_CROSS_MARKET_RV",
     "why": "cross-sectional carry across duration-neutral international "
            "rates spreads - a spread book, not an outright carry harvest"},
    {"id": "S02_RATES_RV_VALUE", "role": "RESIDUAL", "family": "RATES_RV",
     "asset_class": "RATES", "owner": "alpha_agent.r43.rv",
     "build": ("rv", {"kind": "RATES", "signal": "VALUE"}),
     "expression": "FUTURES_CROSS_MARKET_RV",
     "why": "spread level relative to its own history"},
    {"id": "S03_RATES_RV_MOMENTUM", "role": "RESIDUAL", "family": "RATES_RV",
     "asset_class": "RATES", "owner": "alpha_agent.r43.rv",
     "build": ("rv", {"kind": "RATES", "signal": "MOMENTUM"}),
     "expression": "FUTURES_CROSS_MARKET_RV",
     "why": "spread trend"},
    # ---- commodity curve relative value
    {"id": "S04_COMMODITY_RV_CARRY", "role": "RESIDUAL",
     "family": "COMMODITY_CURVE", "asset_class": "COMMODITY",
     "owner": "alpha_agent.r43.rv",
     "build": ("rv", {"kind": "COMMODITY", "signal": "CARRY_XS"}),
     "expression": "FUTURES_CURVE_SPREAD",
     "why": "calendar and inter-commodity spread carry"},
    {"id": "S05_COMMODITY_RV_VALUE", "role": "RESIDUAL",
     "family": "COMMODITY_CURVE", "asset_class": "COMMODITY",
     "owner": "alpha_agent.r43.rv",
     "build": ("rv", {"kind": "COMMODITY", "signal": "VALUE"}),
     "expression": "FUTURES_CURVE_SPREAD", "why": "spread value"},
    {"id": "S06_COMMODITY_RV_MOMENTUM", "role": "RESIDUAL",
     "family": "COMMODITY_CURVE", "asset_class": "COMMODITY",
     "owner": "alpha_agent.r43.rv",
     "build": ("rv", {"kind": "COMMODITY", "signal": "MOMENTUM"}),
     "expression": "FUTURES_CURVE_SPREAD", "why": "spread trend"},
    # ---- cross-asset relations and scheduled events
    {"id": "S07_CROSS_ASSET_RELATIONS", "role": "RESIDUAL",
     "family": "CROSS_ASSET", "asset_class": "CROSS_ASSET",
     "owner": "alpha_agent.r43.crossasset",
     "build": ("relations", {}),
     "expression": "FUTURES_OUTRIGHT",
     "why": "twelve sign-predeclared economic relations between markets"},
    {"id": "S08_MACRO_EVENT_REVERSAL", "role": "RESIDUAL",
     "family": "EVENT_DRIVEN", "asset_class": "CROSS_ASSET",
     "owner": "alpha_agent.r43.crossasset",
     "build": ("event", {"rule": "REVERSAL", "horizon": 5}),
     "expression": "FUTURES_CROSS_MARKET_RV",
     "why": "relative dislocation around scheduled macro releases, daily"},
    # ---- equity residuals, sector/beta neutral, funded long/short
    {"id": "S09_EQUITY_RESIDUAL_REVERSAL", "role": "RESIDUAL",
     "family": "EQUITY_RESIDUAL", "asset_class": "US_EQUITY",
     "owner": "alpha_agent.r43.equity",
     "build": ("equity", {"signal": "RESIDUAL_REVERSAL"}),
     "expression": "EQUITY_MARKET_NEUTRAL", "why": "short-horizon reversal"},
    {"id": "S10_EQUITY_RESIDUAL_MOMENTUM", "role": "RESIDUAL",
     "family": "EQUITY_RESIDUAL", "asset_class": "US_EQUITY",
     "owner": "alpha_agent.r43.equity",
     "build": ("equity", {"signal": "RESIDUAL_MOMENTUM"}),
     "expression": "EQUITY_MARKET_NEUTRAL", "why": "residual momentum"},
    {"id": "S11_EQUITY_LOW_RESIDUAL_VOL", "role": "RESIDUAL",
     "family": "EQUITY_RESIDUAL", "asset_class": "US_EQUITY",
     "owner": "alpha_agent.r43.equity",
     "build": ("equity", {"signal": "LOW_RESIDUAL_VOL"}),
     "expression": "EQUITY_MARKET_NEUTRAL", "why": "low residual volatility"},
    {"id": "S12_EQUITY_ILLIQUIDITY", "role": "RESIDUAL",
     "family": "EQUITY_RESIDUAL", "asset_class": "US_EQUITY",
     "owner": "alpha_agent.r43.equity",
     "build": ("equity", {"signal": "ILLIQUIDITY"}),
     "expression": "EQUITY_MARKET_NEUTRAL",
     "why": "Amihud illiquidity - the closest thing the owned equity panel "
            "has to a less-efficient-market exposure"},
    # ---- structural premia: the CONTROL
    {"id": "P01_FX_CARRY", "role": "PREMIUM", "family": "FX",
     "asset_class": "FX", "owner": "alpha_agent.r43.carry",
     "build": ("carry", {"group": "FX_CARRY", "mode": "XS"}),
     "expression": "FX_FUTURES_CARRY",
     "why": "the R36 survivor, rank IC 0.155 at t 7.97 - a textbook premium"},
    {"id": "P02_RATES_CARRY", "role": "PREMIUM", "family": "RATES_RV",
     "asset_class": "RATES", "owner": "alpha_agent.r43.carry",
     "build": ("carry", {"group": "RATES_CARRY", "mode": "XS"}),
     "expression": "FUTURES_OUTRIGHT", "why": "international term premium"},
    {"id": "P03_COMMODITY_CARRY", "role": "PREMIUM",
     "family": "COMMODITY_CURVE", "asset_class": "COMMODITY",
     "owner": "alpha_agent.r43.carry",
     "build": ("carry", {"group": "COMMODITY_CARRY", "mode": "XS"}),
     "expression": "FUTURES_OUTRIGHT", "why": "backwardation premium"},
    {"id": "P04_EQUITY_INDEX_CARRY", "role": "PREMIUM",
     "family": "CROSS_ASSET", "asset_class": "EQUITY_INDEX",
     "owner": "alpha_agent.r43.carry",
     "build": ("carry", {"group": "EQUITY_INDEX_CARRY", "mode": "XS"}),
     "expression": "FUTURES_OUTRIGHT",
     "why": "index dividend/financing carry"},
    {"id": "P05_VOL_TERM_PREMIUM", "role": "PREMIUM",
     "family": "VOLATILITY_OPTIONS", "asset_class": "VOLATILITY",
     "owner": "alpha_agent.r44.streams",
     "build": ("vx", {}),
     "expression": "VX_TERM_STRUCTURE",
     "why": "the variance risk premium as the owned VX curve expresses it: "
            "an unconditional short-front / long-second calendar spread"},
    {"id": "P06_CRYPTO_FUNDING_CARRY", "role": "PREMIUM", "family": "CRYPTO",
     "asset_class": "CRYPTO", "owner": "alpha_agent.r42.capital",
     "build": ("crypto", {"symbol": "BTCUSDT"}),
     "expression": "CRYPTO_CASH_AND_CARRY",
     "why": "R41's strongest measured stream, priced by R42 on unremunerated "
            "collateral where it fell BELOW cash - included at its true "
            "price, not at R41's"},
)

STREAM_IDS = tuple(s["id"] for s in STREAMS)
RESIDUAL_STREAM_IDS = tuple(s["id"] for s in STREAMS if s["role"] == "RESIDUAL")
PREMIUM_STREAM_IDS = tuple(s["id"] for s in STREAMS if s["role"] == "PREMIUM")

#: Streams Engine 1 and Engine 3 may ADD if - and only if - they produce a
#: PIT-clean, cost-charged, capital-priced daily excess series. They are
#: declared here so that adding one is a contractual event, not an
#: opportunistic one.
CONDITIONAL_STREAMS = (
    {"id": "X01_INTRADAY_MACRO_EVENT_FX", "role": "RESIDUAL",
     "family": "INTRADAY_EVENT", "asset_class": "FX",
     "owner": "alpha_agent.r44.intraday",
     "requires": "owned native minute bars with observed bid/ask spread, "
                 "aligned to the PIT FRED release calendar"},
    {"id": "X02_OPTION_SURFACE_RESIDUAL", "role": "RESIDUAL",
     "family": "OPTIONS_VOL", "asset_class": "EQUITY_INDEX",
     "owner": "alpha_agent.r44.options",
     "requires": "an option surface deep enough for a fitting zone of at "
                 "least 250 sessions PLUS a judged zone"},
    {"id": "X03_NICHE_MARKET_RESIDUAL", "role": "RESIDUAL",
     "family": "LESS_EFFICIENT_MARKETS", "asset_class": "MIXED",
     "owner": "alpha_agent.r44.niche",
     "requires": "a capacity estimate and a liquidity-scaled cost that the "
                 "market's own volume supports"},
)

#: Two streams are duplicates when they share an economic lineage and differ
#: only in a parameter. The inventory above contains no such pair BY
#: CONSTRUCTION - one stream per (family x economic idea), one expression.
DUPLICATE_RULE = (
    "same information family AND same economic expression AND same "
    "underlying market set => duplicate; only one may enter the portfolio"
)
MAX_CORRELATION_FOR_INDEPENDENCE = 0.60

# --------------------------------------------------------------------------- #
# ENGINE 2B - the combination rules, predeclared, primary named in advance
# --------------------------------------------------------------------------- #
COMBINATION_RULES = (
    "EQUAL_WEIGHT",
    "INVERSE_VOL",
    "EQUAL_RISK_CONTRIBUTION",
    "CAPPED_EQUAL_RISK_CONTRIBUTION",
    "FAMILY_BALANCED_ERC",
    "HIERARCHICAL_RISK_PARITY",
    "MIN_CORRELATION_SHRINKAGE",
    "BAYESIAN_SHRINKAGE",
)

#: Named BEFORE the lockbox is opened. Every other rule is reported on the
#: FIT zones and none of them may replace this one after the fact.
PRIMARY_COMBINATION_RULE = "FAMILY_BALANCED_ERC"
PRIMARY_RULE_RATIONALE = (
    "it estimates no expected return, it cannot concentrate into whichever "
    "family happened to work, and it is the only rule in the list that is "
    "invariant to how many streams a family happens to contain"
)

#: Forbidden outright. Both would let the holdout choose the weights.
UNCONSTRAINED_MEAN_VARIANCE_IS_FORBIDDEN = True
MAXIMISING_HISTORICAL_SHARPE_IS_FORBIDDEN = True

#: Constraints applied to EVERY rule.
PORTFOLIO_CONSTRAINTS = {
    "long_only_in_stream_space": True,
    "max_single_stream_weight": 0.25,
    "max_family_weight": 0.40,
    "max_asset_class_weight": 0.50,
    "max_single_lineage_contribution_to_excess": 0.50,
    "weights_sum_to_one": True,
    "rebalance": "MONTHLY_TO_TARGET",
    "overlay_cost_bps_per_unit_turnover": 1.0,
}
#: A stream may never be shorted. Choosing a stream's sign from its measured
#: history is a fitted decision and would import the whole search.
SHORTING_A_STREAM_IS_FORBIDDEN = True

# --------------------------------------------------------------------------- #
# POST-FREEZE AMENDMENTS - disclosed, never silent
# --------------------------------------------------------------------------- #
#: The contract is frozen before the first number. Exactly one amendment was
#: made after the inventory was first built, and it is recorded here in full
#: rather than folded into a new hash as though it had always been there.
#:
#: The rule that makes this admissible, and that constrains every future
#: amendment: an amendment may only make a stream that FAILED TO BUILD
#: buildable. It may never change a stream that produced a number, never
#: change a role, a weight rule, a control, a cap or a gate, and never be
#: made after the lockbox is opened.
POST_FREEZE_AMENDMENTS = (
    {"id": "A1_P05_VOL_TERM_PREMIUM_SOURCE",
     "when": "after the first inventory build, before any artifact was "
             "written and before the lockbox was opened",
     "what": "P05_VOL_TERM_PREMIUM's builder changed from "
             "alpha_agent.r43.carry group VX_TERM_CARRY (which returned no "
             "markets - the futures store holds ONE volatility market, below "
             "that module's MIN_MARKETS) to a direct short-front/long-second "
             "VX calendar spread on the same owned VX curve",
     "state_before": "HISTORICAL_DATA_UNAVAILABLE - the stream had no number",
     "admissible_because": "it makes an UNBUILT stream buildable and changes "
                           "no stream that had produced a number, no role, "
                           "no weighting rule, no control and no gate",
     "affects_a_measured_result": False},
)
AMENDMENTS_MAY_ONLY_MAKE_AN_UNBUILT_STREAM_BUILDABLE = True
AMENDMENTS_AFTER_THE_LOCKBOX_ARE_FORBIDDEN = True

#: A DIAGNOSTIC, explicitly non-qualifying, declared here so that it cannot
#: later be quoted as a result. The contract forbids shorting a stream
#: because choosing a sign from measured history is fitting. But there is a
#: real scientific difference between "this stream carries no information"
#: and "this stream carries information and our predeclared economic sign
#: was backwards", and only a sign-selected variant can tell them apart.
#:
#: So it is run, on the FIT ZONES ONLY, and its lockbox number is reported
#: as a DIAGNOSTIC that may never be promoted to a qualification, may never
#: be frozen as a shadow, and is charged its own search burden.
SIGN_SELECTED_DIAGNOSTIC = {
    "runs": True,
    "sign_chosen_on": "FIT_ZONES_ONLY",
    "may_qualify": False,
    "may_be_frozen_as_a_shadow": False,
    "charged_burden": True,
    "why": "it separates 'no information' from 'wrong predeclared sign'",
}

#: The crypto sleeve's round-trip execution cost, inherited from R42's real
#: Binance fee schedule plus spread. An always-on book pays it once.
CRYPTO_ROUND_TRIP_BPS = 8.0
CRYPTO_COMMITTED_CAPITAL = 1.35

COVARIANCE_ESTIMATOR = "LEDOIT_WOLF_SHRINKAGE_TO_DIAGONAL"
COVARIANCE_FITTED_ON = "FIT_ZONES_ONLY"

# --------------------------------------------------------------------------- #
# ENGINE 2C - the controls
# --------------------------------------------------------------------------- #
CONTROLS = {
    "STRUCTURAL_PREMIUM_PORTFOLIO": (
        "the SAME primary combination rule applied to the PREMIUM-role "
        "streams only, priced with their own capital models"),
    "VOLATILITY_MATCHED_PASSIVE": (
        "an always-long equal-risk book over the same markets, volatility "
        "matched to the candidate - R43's control, inherited unchanged"),
    "CASH": "the risk-free rate the committed capital forgoes",
}
PRIMARY_PORTFOLIO_CONTROL = "STRUCTURAL_PREMIUM_PORTFOLIO"
INCREMENT_IS_VOLATILITY_MATCHED = True
A_SMOOTHER_PACKAGE_OF_PREMIA_IS_NOT_ALPHA = True

# --------------------------------------------------------------------------- #
# Capital, cost and the judge - inherited from R43 unchanged
# --------------------------------------------------------------------------- #
JUDGE_OWNER = "alpha_agent.r43.judge"
CAPITAL_AND_COLLATERAL_OWNER = "alpha_agent.r43.contract"
PRIMARY_CAPITAL_MODEL = "COMMITTED_MARGIN_X2"
COST_STRESS_MULTIPLIERS = [1.0, 2.0, 3.0]
COST_BASE_IS_TRADED_NOTIONAL = True
PRIMARY_METRIC = (
    "IMPLEMENTABLE RESIDUAL RETURN ON CONSERVATIVE COMMITTED CAPITAL "
    "AGAINST THE CORRECT ECONOMIC CONTROL"
)

#: Option capital treatments R43 did not need and R44 declares in advance so
#: that an option result can never be quoted on a flattering denominator.
OPTION_COLLATERAL_CLASSES = {
    "OPTION_PREMIUM_PAID": {
        "committed_capital": 1.0, "collateral_earns_rf": 0.0,
        "control": "RISK_FREE_RATE",
        "note": "premium paid is spent, earns nothing, and can go to zero"},
    "OPTION_MARGINED_SHORT": {
        "committed_capital": None, "collateral_earns_rf": 1.0,
        "control": "ZERO",
        "note": "short option margin is posted at the clearing house and is "
                "remunerated; capital is the denominator, not a charge"},
}

# --------------------------------------------------------------------------- #
# Qualification - three separate words, never collapsed
# --------------------------------------------------------------------------- #
QUALIFICATION_LEVELS = (
    "STANDALONE_ALPHA", "PORTFOLIO_ALPHA", "STRUCTURAL_PREMIUM",
    "RESEARCH_CANDIDATE", "FORWARD_PENDING", "DATA_BLOCKED",
)
ALPHA_IS_NOT_A_LOOSE_WORD = True
A_POSITIVE_NOMINAL_RETURN_IS_NOT_ALPHA = True
A_HIGH_SHARPE_IS_NOT_ALPHA = True

STANDALONE_ALPHA_GATE = {
    "positive_after_cost_residual_return": True,
    "correct_economic_control": True,
    "same_sign_fit_and_lock": True,
    "min_effective_decisions": 250,
    "t_min_lock": 2.0,
    "positive_at_2x_cost": True,
    "survives_search_adjustment": "BENJAMINI_HOCHBERG_WITHIN_FAMILY_Q_0.10",
    "no_pit_or_survivorship_defect": True,
    "not_one_market": True,
    "factor_residual_evidence": True,
    "implementable": True,
}

PORTFOLIO_ALPHA_GATE = {
    "streams_independently_discovered": True,
    "weighting_rule_predeclared": True,
    "no_holdout_optimisation": True,
    "positive_lock_excess": True,
    "t_min_lock": 2.0,
    "same_sign_fit_and_lock": True,
    "positive_at_2x_cost": True,
    "beats_structural_premium_control": True,
    "control_increment_t_min": 2.0,
    "no_single_stream_above_fraction_of_excess": 0.50,
    "leave_one_family_out_preserves_sign": True,
    "leave_one_stream_out_preserves_sign": True,
    "leave_one_asset_class_out_preserves_sign": True,
    "survives_search_adjustment": "BENJAMINI_HOCHBERG_WITHIN_"
                                  "PORTFOLIO_SYNTHESIS_FAMILY_Q_0.10",
    "min_lock_days": 250,
}

STRUCTURAL_PREMIUM_LABEL = (
    "economically useful and NOT Alpha. It is labelled honestly and is never "
    "promoted to Alpha because it is profitable."
)

# --------------------------------------------------------------------------- #
# The kill battery - chosen before results, applied to portfolios too
# --------------------------------------------------------------------------- #
KILL_TESTS_ARE_CHOSEN_BEFORE_RESULTS = True
PORTFOLIO_KILL_TESTS = (
    "LEAVE_ONE_STREAM_OUT",
    "LEAVE_ONE_FAMILY_OUT",
    "LEAVE_ONE_ASSET_CLASS_OUT",
    "LEAVE_ONE_YEAR_OUT",
    "WEIGHT_PERTURBATION",
    "COST_X2",
    "COST_X3",
    "CORRELATION_STRESS",
    "BLOCK_BOOTSTRAP",
    "CONCENTRATION",
    "STRUCTURAL_PREMIUM_CONTROL_INCREMENT",
    "VOLATILITY_MATCHED_PASSIVE_INCREMENT",
    "EQUAL_WEIGHT_DEGRADATION",
    "PBO_COMBINATORIAL_SPLIT",
)
WEIGHT_PERTURBATION_SIGMA = 0.25
BOOTSTRAP_DRAWS = 2000
BOOTSTRAP_BLOCK = 21

# --------------------------------------------------------------------------- #
# ENGINE 1 and ENGINE 3 lanes
# --------------------------------------------------------------------------- #
LANES = {
    "E1A_OPTIONS_SURFACE": "does a $0 option surface deep enough to test "
                           "variance-premium and skew hypotheses exist",
    "E1B_INTRADAY_EVENT": "does R43's real-but-cost-dominated macro event "
                          "effect become tradable in event TIME",
    "E1C_ANALYST_REVISIONS": "are point-in-time analyst vintages testable",
    "E1D_NATIVE_CREDIT": "is native credit information reachable at $0",
    "E1E_MICROSTRUCTURE": "is there a NEW microstructure question the owned "
                          "archives can answer without fabricating a fill",
    "E2_PORTFOLIO_SYNTHESIS": "do weak independent edges combine into "
                              "portfolio alpha that beats the premia",
    "E3_LESS_EFFICIENT_MARKETS": "do lower-capacity markets carry a better "
                                 "frontier than the liquid ones",
}

#: The FROZEN ZONE_B ceiling per lane. A lane that wants more candidates than
#: its cap must fail instead of borrowing budget from another lane.
LANE_CAPS = {
    "E1A_OPTIONS_SURFACE": 6,
    "E1B_INTRADAY_EVENT": 8,
    "E1C_ANALYST_REVISIONS": 4,
    "E1D_NATIVE_CREDIT": 4,
    "E1E_MICROSTRUCTURE": 4,
    "E2_PORTFOLIO_SYNTHESIS": 8,
    "E3_LESS_EFFICIENT_MARKETS": 12,
}
TOTAL_ZONE_B_BUDGET = sum(LANE_CAPS.values())
LANE_CAP_IS_A_CEILING_NOT_A_TARGET = True

# --- Engine 1B: intraday event time, declared before the first bar is read --
#: Instruments the estate OWNS at native minute resolution with an observed
#: bid/ask spread. Dukascopy's index and Bund symbols are CFDs and are
#: excluded from any futures hypothesis by NO_CFD_PROXY_FOR_A_FUTURES_
#: HYPOTHESIS; spot FX and spot gold are real OTC instruments quoted by a
#: real broker, so they are admissible in their own right.
INTRADAY_INSTRUMENTS = ("EURUSD", "USDJPY", "XAUUSD")
INTRADAY_EXCLUDED_AS_CFD = ("USA500IDXUSD", "DEUIDXEUR", "BUNDTREUR",
                            "LIGHTCMDUSD")
#: Minutes AFTER the release stamp at which a position may first be taken.
#: Zero is forbidden: the estate has no fill at the print.
INTRADAY_ENTRY_DELAYS_MIN = (1, 5)
#: Holding periods, in minutes, from entry.
INTRADAY_HOLD_MINUTES = (5, 15, 30, 60, 120)
#: The cost model is the OBSERVED half-spread on the entry and exit bars,
#: plus a declared slippage allowance. No assumed fill, no mid-price fill.
INTRADAY_COST_MODEL = "OBSERVED_HALF_SPREAD_BOTH_SIDES_PLUS_SLIPPAGE"
INTRADAY_SLIPPAGE_BPS_PER_SIDE = 0.2
INTRADAY_PLACEBO = "the SAME rule at the SAME clock time on non-release days"
INTRADAY_REQUIRES_PLACEBO_SEPARATION = True

# --- Engine 1A: option surface -------------------------------------------- #
OPTION_MIN_FIT_SESSIONS = 250
OPTION_MIN_JUDGED_SESSIONS = 250
OPTION_MONEYNESS_BUCKETS = ((0.80, 0.90), (0.90, 0.97), (0.97, 1.03),
                            (1.03, 1.10), (1.10, 1.25))
OPTION_DTE_BUCKETS = ((7, 30), (30, 60), (60, 120), (120, 400))
OPTION_IV_METHOD = "BLACK_SCHOLES_BISECTION_ON_OWNED_UNDERLYING_AND_RATE"
OPTION_VENDOR_GREEKS_REQUIRED = False
A_SHORT_WINDOW_MAY_DIAGNOSE_AND_MAY_NOT_QUALIFY = True

# --- Engine 3: less-efficient markets ------------------------------------- #
NICHE_LIQUIDITY_SOURCE = "the market's OWN dollar volume in the owned store"
NICHE_COST_IS_LIQUIDITY_SCALED = True
CAPACITY_IS_A_RESULT_NOT_A_FILTER = True

BLOCKER_VOCAB = (
    "EXECUTED", "PAYMENT_REQUIRED", "ACCOUNT_REQUIRED", "LICENCE_REQUIRED",
    "HISTORICAL_DATA_UNAVAILABLE", "PIT_INTEGRITY_FAILURE",
    "SURVIVORSHIP_FAILURE", "IDENTITY_FAILURE", "COMPUTE_SPEND_REQUIRED",
    "FUTURE_TIME_REQUIRED", "SAFETY_BLOCKER", "IRREPARABLE_TECHNICAL_FAILURE",
)
A_FAILED_LANE_IS_A_ROUTING_EVENT = True
ONE_LANE_MAY_NOT_HALT_ANOTHER = True
NO_BROAD_EXECUTABLE_ZERO_COST_BRANCH_MAY_BE_DEFERRED = True

#: Engine 3 - capacity is a RESULT, not a filter. A credible $500k book is a
#: legitimate answer for this estate.
CAPACITY_TIERS_USD = [100_000, 500_000, 1_000_000, 5_000_000, 25_000_000]
LOWER_CAPACITY_IS_ACCEPTABLE = True
FANTASY_EXECUTION_IS_NOT = True
PARTICIPATION_CAP_OF_DAILY_VOLUME = 0.01

# --------------------------------------------------------------------------- #
# Data integrity - inherited, restated
# --------------------------------------------------------------------------- #
PIT_CHECKS = (
    "no future information in any signal",
    "no current snapshot substituted for a historical vintage",
    "no reconstructed vintage",
    "no interpolated intraday bar",
    "no fabricated fill",
    "no hindsight extrema",
    "survivorship-safe universes only",
)
NO_CURRENT_SNAPSHOT_AS_HISTORICAL_VINTAGE = True
NO_INTERPOLATED_INTRADAY = True
NO_FABRICATED_EVIDENCE = True
NO_CFD_PROXY_FOR_A_FUTURES_HYPOTHESIS = True
#: Release times of US macro releases are a DECLARED CONSTANT, not data. They
#: are stable, published and stated here so the reader can check them.
MACRO_RELEASE_TIMES_ET = {
    "EMPLOYMENT_SITUATION": "08:30", "CPI": "08:30", "PPI": "08:30",
    "GDP": "08:30", "RETAIL_SALES": "08:30", "PERSONAL_INCOME_PCE": "08:30",
    "DURABLE_GOODS": "08:30", "HOUSING_STARTS": "08:30",
    "INDUSTRIAL_PRODUCTION": "09:15",
}
MACRO_RELEASE_TIMES_ARE_A_DECLARED_CONSTANT = True

# --------------------------------------------------------------------------- #
# Forward evidence
# --------------------------------------------------------------------------- #
PRIOR_SHADOWS_ARE_IMMUTABLE = True
NEVER_BACKFILL_PROSPECTIVE_ROWS = True
PROMOTION_ALLOWED = False
MAX_NEW_SHADOWS = 3
FREEZE_REQUIRES = (
    "a qualification state of RESEARCH_CANDIDATE or better",
    "a positive lock-zone result that the kill battery did not overturn",
    "a predeclared capture cadence and capital model",
    "PROMOTION_ALLOWED = False on the frozen row",
)
DO_NOT_FREEZE_MEDIOCRE_CANDIDATES_TO_CREATE_ACTIVITY = True

# --------------------------------------------------------------------------- #
# Terminal states
# --------------------------------------------------------------------------- #
TERMINAL_STATES = (
    "R44_STANDALONE_ALPHA_FOUND",
    "R44_PORTFOLIO_ALPHA_FOUND",
    "R44_MULTIPLE_ALPHA_CANDIDATES_FORWARD_PENDING",
    "R44_NEW_INFORMATION_IMPROVES_FRONTIER",
    "R44_LESS_EFFICIENT_MARKET_EDGE_FOUND",
    "R44_STRUCTURAL_PREMIA_ONLY",
    "R44_DATA_WALL_REMAINS_BINDING",
    "R44_NO_ALPHA_AFTER_ORTHOGONAL_AND_PORTFOLIO_SYNTHESIS",
)
NO_ALPHA_TERMINAL_REQUIRES_EVERY_ZERO_COST_BRANCH_EXECUTED = True
DO_NOT_FORCE_A_SUCCESS_STATE = True
DO_NOT_PROTECT_PROMISING_RESULTS = True
NEVER_COLLAPSE_RESULT_AXES = True

RESULT_AXES = (
    "SYSTEM_RESULT", "STANDALONE_ALPHA_RESULT", "PORTFOLIO_ALPHA_RESULT",
    "STRUCTURAL_PREMIUM_RESULT", "OPTIONS_DATA_RESULT",
    "INTRADAY_DATA_RESULT", "ANALYST_REVISION_RESULT", "CREDIT_DATA_RESULT",
    "MICROSTRUCTURE_DATA_RESULT", "LESS_EFFICIENT_MARKET_RESULT",
    "SEARCH_ADJUSTED_RESULT", "TRUE_FORWARD_RESULT",
)

FIFTEEN_QUESTIONS = (
    "Did we find standalone Alpha?",
    "Did combining weak independent edges create portfolio-level Alpha?",
    "Did the portfolio beat a structural-premium control?",
    "If yes, where did the incremental Alpha come from?",
    "If no, was the portfolio simply diversified beta/premia?",
    "Did options add genuinely new information?",
    "Did native intraday event data turn R43's cost-killed macro effect "
    "into a tradable effect?",
    "Did analyst revisions become testable?",
    "Did less-efficient markets outperform the major-liquid-market frontier?",
    "Which candidate has the highest evidence-weighted expected value?",
    "Which candidate deserves prospective freezing?",
    "Which complex model materially beat its simple baseline?",
    "What information source appears most valuable now?",
    "What exact data purchase, if any, has earned authorization "
    "consideration?",
    "What is the single highest-value Release 45?",
)

# --------------------------------------------------------------------------- #
# Machine learning policy
# --------------------------------------------------------------------------- #
ML_IS_ALLOWED_WHERE_NEW_INFORMATION_JUSTIFIES_IT = True
EVERY_COMPLEX_MODEL_MUST_BEAT_A_SIMPLE_BASELINE = True
REQUIRED_MODEL_COMPARISON = ("COMPLEX", "SIMPLE_TRANSPARENT_BASELINE",
                             "PASSIVE_STRUCTURAL_CONTROL")
NO_GPU_SPEND_WITHOUT_PROOF_CAPACITY_BINDS = True


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
