r"""api/alpha_opportunity_registry.py - Release 56: the ONE canonical registry of
ECONOMICALLY DISTINCT alpha opportunities.

WHY IT EXISTS
-------------
This project has run twenty-six alpha campaigns. Their results are real, they
are hashed, and they are scattered across twenty-six release documents, six
research packages and three live read models. Nothing has ever answered the one
question that decides what to research next:

    which economically distinct ideas have we tested, what did each one
    conclude, and which of them could receive a dollar today?

Without that, a release rediscovers a closed frontier. Release 27 measured 49
hypotheses across 8 families and produced 0 survivors; Release 39 searched
universally and produced 0 that survived its own search-burden gate. Re-running
either without new information is not research, it is arithmetic with a
different seed.

WHAT IT IS
----------
Two halves, permanently distinguishable:

* the FROZEN CATALOGUE - one row per economically distinct family, each
  carrying the release that judged it, the document that records it, the
  numbers that release published and the named condition under which it may be
  re-opened. These are CITATIONS. Nothing here is recomputed, and no number is
  invented; if a release did not publish a figure, the field is null.

* the LIVE COMPOSITION - the same families as the operational and research
  owners see them RIGHT NOW: the champion sleeves (``api.multi_horizon_*``
  through ``api.universe_scoring``), the Release-46 prospective tournament
  (``api.prospective_tournament``), the Release-32 sleeve frontier
  (``api.pnl_opportunity_frontier``) and the capital-eligibility registry
  (``api.investability_registry`` through ``api.opportunity_frontier``).

A family's published STATUS is the live one when a live owner has an opinion,
and the frozen one otherwise - and the payload always shows both, so a reader
can see when the two disagree.

THE EXPERIMENT QUEUE
--------------------
The registry's forward agenda, ranked by EXPECTED INFORMATION VALUE rather than
by how interesting the hypothesis sounds. An experiment that would re-test an
EXHAUSTED family without new information scores zero and is REJECTED by name,
which is the whole point.

Read-only. It creates no signal, target, proposal, decision or order; it
promotes no model, activates no sleeve and enables no automation.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

SCHEMA_VERSION = "alpha_opportunity_registry.v1"
COMPOSITION_OWNER = "api.alpha_opportunity_registry"
CALCULATION_OWNER = "api.alpha_opportunity_registry"
PHASE = "R56"
ROUTE = "/v1/research/alpha-opportunity-registry"

# --------------------------------------------------------------------------- #
# Lifecycle vocabulary
# --------------------------------------------------------------------------- #
ST_ACTIVE = "ACTIVE"                       # carries operational capital today
ST_CHALLENGER = "CHALLENGER"               # frozen, competing forward, no capital
ST_PROMISING = "PROMISING"                 # measured edge, not yet forward-proven
ST_FORWARD_EVIDENCE_NEEDED = "FORWARD_EVIDENCE_NEEDED"
ST_EXHAUSTED = "EXHAUSTED"                 # searched out on the information we own
ST_REJECTED = "REJECTED"                   # tested and failed on its own merits
ST_BLOCKED = "BLOCKED"                     # data / entitlement / venue wall
ST_UNTESTED = "UNTESTED"                   # data exists, no experiment run yet
STATUS_VOCAB = (ST_ACTIVE, ST_CHALLENGER, ST_PROMISING,
                ST_FORWARD_EVIDENCE_NEEDED, ST_EXHAUSTED, ST_REJECTED,
                ST_BLOCKED, ST_UNTESTED)

STATUS_CLASS = {ST_ACTIVE: "safe", ST_CHALLENGER: "manual", ST_PROMISING: "warn",
                ST_FORWARD_EVIDENCE_NEEDED: "warn", ST_EXHAUSTED: "muted",
                ST_REJECTED: "danger", ST_BLOCKED: "danger",
                ST_UNTESTED: "muted"}

#: Whether a family could receive an operational dollar TODAY. Distinct from
#: status: a family can be a strong CHALLENGER and still be unable to hold
#: capital, because capital eligibility belongs to the investability registry
#: and to a human, never to a research result.
CAP_ELIGIBLE = "CAPITAL_ELIGIBLE_TODAY"
CAP_RESEARCH_ONLY = "RESEARCH_ONLY_NO_CAPITAL"
CAP_BLOCKED = "CAPITAL_BLOCKED"
CAPITAL_VOCAB = (CAP_ELIGIBLE, CAP_RESEARCH_ONLY, CAP_BLOCKED)

# --------------------------------------------------------------------------- #
# Asset-class readiness
# --------------------------------------------------------------------------- #
AR_DATA_READY = "DATA_READY"
AR_RESEARCH_READY = "RESEARCH_READY"
AR_FORWARD_SHADOW_READY = "FORWARD_SHADOW_READY"
AR_NOT_READY = "NOT_READY"
AR_BLOCKED = "BLOCKED"
READINESS_VOCAB = (AR_DATA_READY, AR_RESEARCH_READY, AR_FORWARD_SHADOW_READY,
                   AR_NOT_READY, AR_BLOCKED)

SAFETY_BADGES = ["RESEARCH ONLY", "READ ONLY", "PAPER ONLY", "NO ORDERS",
                 "NO LIVE BROKER ORDERS", "AUTOMATION OFF", "MANUAL REVIEW",
                 "NO MODEL PROMOTION", "NO SLEEVE ACTIVATION"]

STATE_READY = "READY"
STATE_DEGRADED = "DEGRADED"
STATE_UNAVAILABLE = "UNAVAILABLE"
READ_STATE_VOCAB = (STATE_READY, STATE_DEGRADED, STATE_UNAVAILABLE)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# THE FROZEN CATALOGUE
#
# One row per economically distinct family. Every `evidence` figure is quoted
# from the named release document; a figure that release did not publish is
# null rather than estimated. `reopen_condition` is the specific NEW thing that
# would make re-running the family research rather than repetition.
# --------------------------------------------------------------------------- #
FROZEN_CATALOGUE = (
    # ---------------------- US equity cross-section ------------------------ #
    {
        "family_id": "EQ_XS_FUNDAMENTAL_QUALITY",
        "label": "Equity cross-sectional fundamental / quality",
        "economic_family": "CROSS_SECTIONAL_FUNDAMENTAL_COMPOSITE",
        "information_family": "FUNDAMENTALS",
        "asset_classes": ["US_EQUITY"], "horizons_sessions": [63],
        "status": ST_ACTIVE,
        "status_reason": ("one of the two legs of the live champion "
                          "fundamental_momentum_50_50_v1"),
        "capital_state": CAP_ELIGIBLE,
        "pit_integrity": "PIT_SAFE_OWNED_PANEL",
        "effective_history": "owned frozen fundamental panel; quarterly cadence",
        "turnover": "LOW (quarterly)", "cost_sensitivity": "LOW",
        "evidence": {"release": "Stage 23 / Stage 24", "verdict": "RETAINED",
                     "doc": "docs/STAGE24_PIT_FUNDAMENTAL_ALPHA.md",
                     "numbers": {"note": "the fundamental leg carries the RANK; "
                                         "momentum buys regime insurance"}},
        "reopen_condition": "a new PIT fundamental vendor with pre-2015 coverage",
    },
    {
        "family_id": "EQ_XS_MOMENTUM",
        "label": "Equity cross-sectional momentum (12-1 / 6-1)",
        "economic_family": "CROSS_SECTIONAL_MOMENTUM",
        "information_family": "PRICE_STATE",
        "asset_classes": ["US_EQUITY"], "horizons_sessions": [20, 63],
        "status": ST_ACTIVE,
        "status_reason": "the second leg of the live champion; also an R46 challenger",
        "capital_state": CAP_ELIGIBLE,
        "pit_integrity": "PIT_SAFE_OWNED_PANEL",
        "effective_history": "owned daily price panel",
        "turnover": "MEDIUM (monthly)", "cost_sensitivity": "MEDIUM",
        "evidence": {"release": "Stage 23 / R46 / R57", "verdict": "ACTIVE_PLUS_CHALLENGER",
                     "doc": "docs/STAGE23_UNIFIED_ALPHA_RESEARCH.md",
                     "numbers": {"note": "momentum buys REGIME INSURANCE rather "
                                         "than rank",
                                 "r57_lockbox_ann_net_excess": 0.0927,
                                 "r57_lockbox_t": 1.47,
                                 "r57_validation_ann_net_excess": -0.0019,
                                 "r57_verdict": ("NO_ALPHA_EVIDENCE - lockbox "
                                                 "positive but validation-"
                                                 "negative (sign flip) and "
                                                 "fails BH q=0.10"),
                                 "r57_doc": "docs/RELEASE57_ALPHA_DISCOVERY_OFFENSIVE.md"}},
        "reopen_condition": "n/a - it is live and measured every session",
    },
    {
        "family_id": "EQ_XS_REVERSAL",
        "label": "Equity short-horizon cross-sectional reversal",
        "economic_family": "CROSS_SECTIONAL_REVERSAL",
        "information_family": "PRICE_STATE",
        "asset_classes": ["US_EQUITY"], "horizons_sessions": [1, 5],
        "status": ST_CHALLENGER,
        "status_reason": "R46 forward challenger with the most matured observations",
        "capital_state": CAP_RESEARCH_ONLY,
        "pit_integrity": "PIT_SAFE_OWNED_PANEL",
        "effective_history": "owned daily price panel",
        "turnover": "VERY HIGH (daily)", "cost_sensitivity": "VERY HIGH",
        "evidence": {"release": "R46 / R57", "verdict": "EARLY_FORWARD_EVIDENCE",
                     "doc": "docs/RELEASE46_PROSPECTIVE_ALPHA_TOURNAMENT.md",
                     "numbers": {"note": "live figures come from the tournament "
                                         "owner, not from this catalogue",
                                 "r57_lockbox_ann_net_excess": -0.0116,
                                 "r57_verdict": ("NO_ALPHA_EVIDENCE long-only "
                                                 "top-50 at 12.5bp/side on the "
                                                 "survivorship-safe panel"),
                                 "r57_doc": "docs/RELEASE57_ALPHA_DISCOVERY_OFFENSIVE.md"}},
        "reopen_condition": "n/a - it is accruing forward evidence now",
    },
    {
        "family_id": "EQ_XS_VALUE",
        "label": "Equity cross-sectional value (price-based multiples)",
        "economic_family": "CROSS_SECTIONAL_VALUE",
        "information_family": "FUNDAMENTALS",
        "asset_classes": ["US_EQUITY"], "horizons_sessions": [20, 63],
        "status": ST_CHALLENGER,
        "status_reason": "R46 forward challenger, distinct from the champion's own composite",
        "capital_state": CAP_RESEARCH_ONLY, "pit_integrity": "PIT_SAFE_OWNED_PANEL",
        "effective_history": "owned fundamental + price panel",
        "turnover": "LOW", "cost_sensitivity": "LOW",
        "evidence": {"release": "R46", "verdict": "FORWARD_PENDING",
                     "doc": "docs/RELEASE46_PROSPECTIVE_ALPHA_TOURNAMENT.md",
                     "numbers": {}},
        "reopen_condition": "n/a - forward evidence pending",
    },
    {
        "family_id": "EQ_LOW_RISK_ANOMALY",
        "label": "Equity low-volatility / low-beta anomaly",
        "economic_family": "LOW_RISK_ANOMALY", "information_family": "PRICE_STATE",
        "asset_classes": ["US_EQUITY"], "horizons_sessions": [20],
        "status": ST_CHALLENGER, "status_reason": "R46 forward challenger, pending",
        "capital_state": CAP_RESEARCH_ONLY, "pit_integrity": "PIT_SAFE_OWNED_PANEL",
        "effective_history": "owned daily price panel",
        "turnover": "LOW", "cost_sensitivity": "LOW",
        "evidence": {"release": "R46 / R57", "verdict": "FORWARD_PENDING",
                     "doc": "docs/RELEASE46_PROSPECTIVE_ALPHA_TOURNAMENT.md",
                     "numbers": {"r57_lockbox_ann_net_excess": -0.0490,
                                 "r57_validation_ann_net_excess": 0.0116,
                                 "r57_verdict": ("NO_ALPHA_EVIDENCE - "
                                                 "validation-positive but "
                                                 "lockbox-negative (sign flip)"),
                                 "r57_doc": "docs/RELEASE57_ALPHA_DISCOVERY_OFFENSIVE.md"}},
        "reopen_condition": "n/a - forward evidence pending",
    },
    {
        "family_id": "EQ_RESIDUAL_MOMENTUM",
        "label": "Equity factor-residual momentum",
        "economic_family": "RESIDUAL_MOMENTUM", "information_family": "PRICE_STATE",
        "asset_classes": ["US_EQUITY"], "horizons_sessions": [20],
        "status": ST_CHALLENGER,
        "status_reason": ("the R39 WIDE representation survived a factor-residual "
                          "control (t 2.58) and then failed the search-burden gate"),
        "capital_state": CAP_RESEARCH_ONLY, "pit_integrity": "PIT_SAFE_OWNED_PANEL",
        "effective_history": "owned daily price panel",
        "turnover": "MEDIUM", "cost_sensitivity": "MEDIUM",
        "evidence": {"release": "R39 / R46 / R57",
                     "verdict": "HISTORICAL_QUALIFICATION_FAIL_FORWARD_PENDING",
                     "doc": "docs/RELEASE39_AUTONOMOUS_UNIVERSAL_ALPHA_DISCOVERY.md",
                     "numbers": {"factor_residual_t": 2.58,
                                 "survivors_after_deflated_sharpe": 0,
                                 "cumulative_search_burden": 230,
                                 "r57_lockbox_ann_net_excess": 0.0721,
                                 "r57_validation_ann_net_excess": -0.0101,
                                 "r57_verdict": ("NO_ALPHA_EVIDENCE - "
                                                 "validation/lockbox sign flip, "
                                                 "turnover cap breach, BH fail"),
                                 "r57_doc": "docs/RELEASE57_ALPHA_DISCOVERY_OFFENSIVE.md"}},
        "reopen_condition": ("forward evidence, not another historical search: the "
                             "search burden is already 230 and deflates any new "
                             "in-sample t"),
    },
    {
        "family_id": "EQ_PEAD",
        "label": "Post-earnings announcement drift (price-based)",
        "economic_family": "POST_EARNINGS_ANNOUNCEMENT_DRIFT",
        "information_family": "EARNINGS_EVENTS",
        "asset_classes": ["US_EQUITY"], "horizons_sessions": [5, 20],
        "status": ST_CHALLENGER,
        "status_reason": "R46 forward challengers exist on the price-based variant",
        "capital_state": CAP_RESEARCH_ONLY,
        "pit_integrity": "PIT_SAFE_SCHEDULED_EVENT_CALENDAR",
        "effective_history": "owned earnings calendar + price panel",
        "turnover": "HIGH (event-driven)", "cost_sensitivity": "HIGH",
        "evidence": {"release": "Stage 13B / R46", "verdict": "FORWARD_PENDING",
                     "doc": "docs/RELEASE46_PROSPECTIVE_ALPHA_TOURNAMENT.md",
                     "numbers": {"stage13b_sales_pead_63d_t": 2.27}},
        "reopen_condition": "n/a - forward evidence pending",
    },
    {
        "family_id": "EQ_ANALYST_REVISIONS",
        "label": "Analyst estimate revisions / expectation drift",
        "economic_family": "ANALYST_EXPECTATION_REVISION",
        "information_family": "ANALYST_ESTIMATES",
        "asset_classes": ["US_EQUITY"], "horizons_sessions": [20, 63],
        "status": ST_REJECTED,
        "status_reason": ("the in-sample sales-revision effect did not replicate "
                          "out of sample, and every vendor evaluated failed the "
                          "survivorship-safe test or the purchase gate"),
        "capital_state": CAP_RESEARCH_ONLY,
        "pit_integrity": "VENDOR_DEPENDENT_SURVIVORSHIP_RISK",
        "effective_history": "prospective revision ledger only (no PIT history owned)",
        "turnover": "MEDIUM", "cost_sensitivity": "MEDIUM",
        "evidence": {"release": "Stage 13B / 13C / Intrinio trial",
                     "verdict": "OOS_DID_NOT_REPLICATE_DO_NOT_BUY",
                     "doc": "docs/INTRINIO_TRIAL_READINESS.md",
                     "numbers": {"stage13b_in_sample_t": 2.27,
                                 "stage13c_out_of_sample_t": -0.29}},
        "reopen_condition": ("a vendor with a survivorship-safe POINT-IN-TIME "
                             "estimate history, evaluated through the "
                             "Information Purchase Gate"),
    },
    {
        "family_id": "EQ_INSIDER_FLOW",
        "label": "Insider (Form 4) cluster buying / net purchase ratio",
        "economic_family": "INSIDER_CLUSTER_BUYING",
        "information_family": "INSIDER_FLOW",
        "asset_classes": ["US_EQUITY"], "horizons_sessions": [20],
        "status": ST_CHALLENGER,
        "status_reason": "three R46 forward challengers on owned SEC Form 4 data",
        "capital_state": CAP_RESEARCH_ONLY,
        "pit_integrity": "PIT_SAFE_FILING_TIMESTAMPED",
        "effective_history": "owned SEC filing archive",
        "turnover": "MEDIUM", "cost_sensitivity": "MEDIUM",
        "evidence": {"release": "R46", "verdict": "FORWARD_PENDING",
                     "doc": "docs/RELEASE46_PROSPECTIVE_ALPHA_TOURNAMENT.md",
                     "numbers": {}},
        "reopen_condition": "n/a - forward evidence pending",
    },
    {
        "family_id": "EQ_SECTOR_ROTATION",
        "label": "Sector / industry rotation",
        "economic_family": "SECTOR_ROTATION", "information_family": "PRICE_STATE",
        "asset_classes": ["US_EQUITY", "EQUITY_INDEX"], "horizons_sessions": [20, 63],
        "status": ST_EXHAUSTED,
        "status_reason": ("beat cash but lost to a volatility-matched mix of the "
                          "benchmark and cash after costs"),
        "capital_state": CAP_RESEARCH_ONLY, "pit_integrity": "PIT_SAFE_OWNED_PANEL",
        "effective_history": "R32 cross-asset panel",
        "turnover": "MEDIUM", "cost_sensitivity": "MEDIUM",
        "evidence": {"release": "R32", "verdict": "FAILED_GATE:beats_volatility_matched_control",
                     "doc": "docs/PNL_OPPORTUNITY_FRONTIER.md",
                     "numbers": {"net_annual_return": 0.129012,
                                 "net_sharpe": 0.814631,
                                 "excess_vs_cash": 0.098782,
                                 "excess_vs_benchmark": -0.032275,
                                 "t_vs_cash": 2.343878}},
        "reopen_condition": ("a control the sleeve can actually beat - i.e. new "
                             "information, not a weaker benchmark"),
    },
    {
        "family_id": "EQ_BETA_TIMING",
        "label": "Equity market beta timing",
        "economic_family": "INDEX_TREND_TIMING", "information_family": "PRICE_STATE",
        "asset_classes": ["US_EQUITY", "EQUITY_INDEX"], "horizons_sessions": [20],
        "status": ST_EXHAUSTED,
        "status_reason": "same volatility-matched control failure as sector rotation",
        "capital_state": CAP_RESEARCH_ONLY, "pit_integrity": "PIT_SAFE_OWNED_PANEL",
        "effective_history": "R32 cross-asset panel",
        "turnover": "MEDIUM", "cost_sensitivity": "MEDIUM",
        "evidence": {"release": "R32", "verdict": "FAILED_GATE:beats_volatility_matched_control",
                     "doc": "docs/PNL_OPPORTUNITY_FRONTIER.md",
                     "numbers": {"net_annual_return": 0.104194,
                                 "net_sharpe": 0.844196,
                                 "excess_vs_cash": 0.080558,
                                 "excess_vs_benchmark": -0.049730,
                                 "t_vs_cash": 2.642281}},
        "reopen_condition": "new information about the market's own level",
    },
    {
        "family_id": "EQ_VOL_RISK_REGIME",
        "label": "Volatility / risk-regime overlay",
        "economic_family": "REGIME_GATED_ENSEMBLE",
        "information_family": "PRICE_STATE",
        "asset_classes": ["US_EQUITY", "VOLATILITY"], "horizons_sessions": [20],
        "status": ST_EXHAUSTED,
        "status_reason": "beat cash (t 2.70) and still lost to the matched control",
        "capital_state": CAP_RESEARCH_ONLY, "pit_integrity": "PIT_SAFE_OWNED_PANEL",
        "effective_history": "R32 cross-asset panel",
        "turnover": "MEDIUM", "cost_sensitivity": "MEDIUM",
        "evidence": {"release": "R32", "verdict": "FAILED_GATE:beats_volatility_matched_control",
                     "doc": "docs/PNL_OPPORTUNITY_FRONTIER.md",
                     "numbers": {"net_annual_return": 0.117450,
                                 "net_sharpe": 0.821386,
                                 "excess_vs_cash": 0.094025,
                                 "excess_vs_benchmark": -0.024227,
                                 "t_vs_cash": 2.703165}},
        "reopen_condition": "an owned forward-looking volatility surface",
    },
    {
        "family_id": "EQ_EVENT_DRIVEN",
        "label": "Equity event-driven sleeve",
        "economic_family": "EVENT_DRIVEN", "information_family": "EARNINGS_EVENTS",
        "asset_classes": ["US_EQUITY"], "horizons_sessions": [5, 20],
        "status": ST_EXHAUSTED,
        "status_reason": "best Sharpe of the R32 sleeves and still lost to the control",
        "capital_state": CAP_RESEARCH_ONLY,
        "pit_integrity": "PIT_SAFE_SCHEDULED_EVENT_CALENDAR",
        "effective_history": "R32 cross-asset panel",
        "turnover": "HIGH", "cost_sensitivity": "HIGH",
        "evidence": {"release": "R32", "verdict": "FAILED_GATE:beats_volatility_matched_control",
                     "doc": "docs/PNL_OPPORTUNITY_FRONTIER.md",
                     "numbers": {"net_annual_return": 0.045716,
                                 "net_sharpe": 1.198385,
                                 "excess_vs_cash": 0.017702,
                                 "excess_vs_benchmark": -0.125418,
                                 "t_vs_cash": 1.625295}},
        "reopen_condition": "an owned intraday event tape",
    },
    {
        "family_id": "EQ_MACRO_CROSS_SECTIONAL_BETA",
        "label": "Macro-factor cross-sectional betas on equities",
        "economic_family": "MACRO_FACTOR_BETA",
        "information_family": "MACRO_RATES_LEVELS",
        "asset_classes": ["US_EQUITY"], "horizons_sessions": [20, 63],
        "status": ST_REJECTED,
        "status_reason": "no hypothesis survived false-discovery control",
        "capital_state": CAP_RESEARCH_ONLY,
        "pit_integrity": "PIT_SAFE_ALFRED_VINTAGES",
        "effective_history": "ALFRED vintages capped at 2000",
        "turnover": "LOW", "cost_sensitivity": "LOW",
        "evidence": {"release": "Stage 15", "verdict": "NO_DEFENSIBLE_ALPHA_ON_MERITS",
                     "doc": "docs/ARCHITECTURE_DECISIONS.md",
                     "numbers": {"fdr_survivors": 0}},
        "reopen_condition": "macro vintages before 2000, or a different transmission channel",
    },
    {
        "family_id": "EQ_ML_CROSS_SECTIONAL",
        "label": "Machine-learned equity cross-section (linear and non-linear)",
        "economic_family": "ML_CROSS_SECTIONAL",
        "information_family": "PRICE_STATE",
        "asset_classes": ["US_EQUITY"], "horizons_sessions": [5, 20],
        "status": ST_CHALLENGER,
        "status_reason": ("R46 forward challengers exist; every historical "
                          "qualification failed the deflated-Sharpe burden gate"),
        "capital_state": CAP_RESEARCH_ONLY, "pit_integrity": "PIT_SAFE_OWNED_PANEL",
        "effective_history": "owned daily price panel",
        "turnover": "HIGH", "cost_sensitivity": "HIGH",
        "evidence": {"release": "R39 / R40 / R41",
                     "verdict": "HISTORICAL_ALPHA_RESULT_FAIL",
                     "doc": "docs/RELEASE41_MULTI_HORIZON_ALPHA_BREAKTHROUGH.md",
                     "numbers": {"cumulative_search_burden": 230}},
        "reopen_condition": ("forward evidence. Another historical fit is deflated "
                             "by a burden of 230 before it starts."),
    },
    # --------------------------- Cross-asset ------------------------------- #
    {
        "family_id": "FX_CARRY",
        "label": "FX carry (cross-sectional and CIP-adjusted)",
        "economic_family": "FX_CARRY", "information_family": "MACRO_RATES_LEVELS",
        "asset_classes": ["FX"], "horizons_sessions": [20, 63],
        "status": ST_FORWARD_EVIDENCE_NEEDED,
        "status_reason": ("strongly predictive in-sample and economically flat "
                          "once the correct capital denominator is applied"),
        "capital_state": CAP_RESEARCH_ONLY,
        "pit_integrity": "PIT_SAFE_OWNED_RATE_AND_FUTURES_SERIES",
        "effective_history": "Norgate world futures + owned rate series",
        "turnover": "LOW", "cost_sensitivity": "LOW",
        "evidence": {"release": "R36 / R43 / R51",
                     "verdict": "PREMIUM_REAL_TIMING_SIGNAL_ZERO",
                     "doc": "docs/RELEASE43_GLOBAL_ALPHA_OFFENSIVE.md",
                     "numbers": {"r36_rank_ic": 0.155, "r36_rank_ic_t": 7.97,
                                 "r43_continuous_gross_annual": 0.1544,
                                 "r43_continuous_sharpe": 0.76,
                                 "r43_after_denominator_annual": 0.0117,
                                 "r43_after_denominator_t": 0.06}},
        "reopen_condition": ("forward evidence on the frozen R51 challenger; the "
                             "historical question is answered"),
    },
    {
        "family_id": "RATES_TERM_PREMIUM_CARRY",
        "label": "Rates / term-premium carry and relative value",
        "economic_family": "TERM_PREMIUM_CARRY",
        "information_family": "FUTURES_CURVE",
        "asset_classes": ["RATES"], "horizons_sessions": [20, 63],
        "status": ST_REJECTED,
        "status_reason": ("the gross carry is real; on the correct denominator it "
                          "is a significant LOSS, not an edge"),
        "capital_state": CAP_RESEARCH_ONLY,
        "pit_integrity": "PIT_SAFE_NATIVE_FUTURES",
        "effective_history": "Norgate world futures (purchased R37, delivered R38)",
        "turnover": "LOW", "cost_sensitivity": "LOW",
        "evidence": {"release": "R43", "verdict": "COLLATERAL_REMUNERATION_IS_THE_DECIDING_TERM",
                     "doc": "docs/RELEASE43_GLOBAL_ALPHA_OFFENSIVE.md",
                     "numbers": {"continuous_gross_annual": 0.3099,
                                 "continuous_sharpe": 2.07,
                                 "after_denominator_annual": -0.1875,
                                 "after_denominator_t": -1.74}},
        "reopen_condition": ("a funding arrangement in which collateral is "
                             "remunerated - i.e. a real prime broker, not a "
                             "research assumption"),
    },
    {
        "family_id": "COMMODITY_CURVE_CARRY",
        "label": "Commodity futures curve carry / roll yield",
        "economic_family": "FUTURES_CURVE_CARRY",
        "information_family": "FUTURES_CURVE",
        "asset_classes": ["COMMODITY"], "horizons_sessions": [5, 20],
        "status": ST_CHALLENGER,
        "status_reason": "R46 forward challenger with its first matured observation",
        "capital_state": CAP_RESEARCH_ONLY,
        "pit_integrity": "PIT_SAFE_NATIVE_FUTURES",
        "effective_history": "Norgate world futures, 105 markets / 23,805 contracts",
        "turnover": "LOW", "cost_sensitivity": "LOW",
        "evidence": {"release": "R38 / R46", "verdict": "FORWARD_PENDING",
                     "doc": "docs/RELEASE38_NATIVE_FUTURES_INFORMATION_FRONTIER.md",
                     "numbers": {}},
        "reopen_condition": "n/a - forward evidence pending",
    },
    {
        "family_id": "VOLATILITY_TERM_CARRY",
        "label": "Volatility term-structure carry (VX)",
        "economic_family": "VOLATILITY_TERM_CARRY",
        "information_family": "FUTURES_CURVE",
        "asset_classes": ["VOLATILITY"], "horizons_sessions": [1, 5],
        "status": ST_FORWARD_EVIDENCE_NEEDED,
        "status_reason": ("the strongest measured forward net alpha in the live "
                          "tournament, on far too few independent observations"),
        "capital_state": CAP_RESEARCH_ONLY,
        "pit_integrity": "PIT_SAFE_NATIVE_FUTURES",
        "effective_history": "Norgate VX continuous contracts",
        "turnover": "HIGH", "cost_sensitivity": "HIGH",
        "evidence": {"release": "R46", "verdict": "EARLY_FORWARD_EVIDENCE",
                     "doc": "docs/RELEASE46_PROSPECTIVE_ALPHA_TOURNAMENT.md",
                     "numbers": {"note": "live figures come from the tournament owner"}},
        "reopen_condition": "n/a - it is the fastest-maturing cell in the tournament",
    },
    {
        "family_id": "MULTI_ASSET_TIME_SERIES_TREND",
        "label": "Multi-asset time-series trend (managed futures)",
        "economic_family": "TIME_SERIES_TREND", "information_family": "PRICE_STATE",
        "asset_classes": ["MULTI_ASSET_FUTURES"], "horizons_sessions": [20, 63],
        "status": ST_CHALLENGER,
        "status_reason": ("R46 forward challenger; the R32 sleeve version lost to "
                          "the volatility-matched control by 12.6 points"),
        "capital_state": CAP_RESEARCH_ONLY,
        "pit_integrity": "PIT_SAFE_NATIVE_FUTURES",
        "effective_history": "Norgate world futures",
        "turnover": "MEDIUM", "cost_sensitivity": "MEDIUM",
        "evidence": {"release": "R32 / R38 / R46 / R57",
                     "verdict": "R32_REJECTED_R46_FORWARD_PENDING",
                     "doc": "docs/PNL_OPPORTUNITY_FRONTIER.md",
                     "numbers": {"r32_net_annual_return": 0.040345,
                                 "r32_net_sharpe": 0.414415,
                                 "r32_excess_vs_benchmark": -0.126222,
                                 "r32_t_vs_cash": 0.337152,
                                 "r57_tsmom_lockbox_net_sharpe": 0.218,
                                 "r57_breakout_lockbox_net_sharpe": -0.394,
                                 "r57_xsmom_lockbox_net_sharpe": 0.438,
                                 "r57_verdict": ("NO_ALPHA_EVIDENCE on 103 "
                                                 "NATIVE continuous markets at "
                                                 "2bp/side, both roll "
                                                 "methodologies; best family "
                                                 "(cross-market momentum, 0.44) "
                                                 "fails materiality-with-"
                                                 "robustness and BH"),
                                 "r57_doc": "docs/RELEASE57_ALPHA_DISCOVERY_OFFENSIVE.md"}},
        "reopen_condition": ("R32 used index-level proxies; R38 delivered NATIVE "
                             "futures; R57 prosecuted the native panel "
                             "historically and found no qualifying family - a "
                             "re-open now requires a pre-registered v2 protocol "
                             "or the R46 forward evidence maturing"),
    },
    {
        "family_id": "CREDIT_SPREAD_MOMENTUM",
        "label": "Credit spread momentum / credit regime timing",
        "economic_family": "CREDIT_SPREAD_MOMENTUM",
        "information_family": "CREDIT_SPREADS",
        "asset_classes": ["CREDIT"], "horizons_sessions": [5, 20],
        "status": ST_CHALLENGER,
        "status_reason": "three R46 forward challengers on owned spread proxies",
        "capital_state": CAP_RESEARCH_ONLY,
        "pit_integrity": "PIT_SAFE_PUBLISHED_SPREAD_SERIES",
        "effective_history": "owned FRED / ICE spread series",
        "turnover": "LOW", "cost_sensitivity": "LOW",
        "evidence": {"release": "R46", "verdict": "FORWARD_PENDING",
                     "doc": "docs/RELEASE46_PROSPECTIVE_ALPHA_TOURNAMENT.md",
                     "numbers": {}},
        "reopen_condition": "n/a - forward evidence pending",
    },
    {
        "family_id": "CRYPTO_FUNDING_CARRY",
        "label": "Delta-neutral crypto perpetual funding carry",
        "economic_family": "FUNDING_CARRY", "information_family": "FUNDING_RATES",
        "asset_classes": ["CRYPTO"], "horizons_sessions": [1, 5],
        "status": ST_BLOCKED,
        "status_reason": ("the strongest historical result this project has ever "
                          "produced, and it is not implementable: the venue "
                          "requires an exchange account and an API key, both "
                          "forbidden by the safety boundary"),
        "capital_state": CAP_BLOCKED,
        "pit_integrity": "PIT_SAFE_EXCHANGE_ARCHIVE",
        "effective_history": "Binance archive: spot 2017-08+, perp 2019-09+",
        "turnover": "VERY HIGH", "cost_sensitivity": "VERY HIGH",
        "evidence": {"release": "R41 / R42 / R51",
                     "verdict": "STRUCTURAL_PREMIUM_CONFIRMED_NOT_TIMING_ALPHA",
                     "doc": "docs/RELEASE42_CRYPTO_BASIS_ALPHA_VALIDATION.md",
                     "numbers": {"r41_variant_b_t": 10.2, "r41_variant_c_t": 6.9,
                                 "r41_placebo_t": 4.45,
                                 "r42_implementability": "HISTORICALLY_NON_IMPLEMENTABLE"}},
        "reopen_condition": ("a venue relationship the safety boundary permits. "
                             "The research question is answered; the wall is "
                             "operational."),
    },
    {
        "family_id": "MACRO_EVENT_SURPRISE",
        "label": "Scheduled macro release surprise / pre-FOMC drift",
        "economic_family": "MACRO_RELEASE_SURPRISE",
        "information_family": "MACRO_RELEASE_SURPRISE",
        "asset_classes": ["RATES", "COMMODITY", "EQUITY_INDEX"],
        "horizons_sessions": [1, 5],
        "status": ST_REJECTED,
        "status_reason": ("the R44 gold effect did not survive the 370 events R44 "
                          "never scored"),
        "capital_state": CAP_RESEARCH_ONLY,
        "pit_integrity": "PIT_SAFE_RELEASE_TIMESTAMPED",
        "effective_history": "owned macro release calendar + native futures",
        "turnover": "HIGH", "cost_sensitivity": "HIGH",
        "evidence": {"release": "R44 / R45",
                     "verdict": "R45_R44_MACRO_EFFECT_REFUTED_IN_NATIVE_MARKETS",
                     "doc": "docs/RELEASE45_MACRO_EVENT_ALPHA.md",
                     "numbers": {"events_r44_did_not_score": 370}},
        "reopen_condition": "an intraday tape around the release, which we do not own",
    },
    {
        "family_id": "INTRADAY_ORDER_FLOW",
        "label": "Signed intraday order flow / microstructure",
        "economic_family": "POSITIONING_FLOW", "information_family": "PRICE_VOLUME",
        "asset_classes": ["CRYPTO", "FX"], "horizons_sessions": [1],
        "status": ST_REJECTED,
        "status_reason": "a real effect that the taker cost consumes entirely",
        "capital_state": CAP_RESEARCH_ONLY,
        "pit_integrity": "PIT_SAFE_TICK_ARCHIVE",
        "effective_history": "Dukascopy tick / Binance 1m klines",
        "turnover": "EXTREME", "cost_sensitivity": "EXTREME",
        "evidence": {"release": "R41", "verdict": "REAL_BUT_TAKER_COST_KILLED",
                     "doc": "docs/RELEASE41_MULTI_HORIZON_ALPHA_BREAKTHROUGH.md",
                     "numbers": {}},
        "reopen_condition": "maker-side execution, which paper trading cannot claim",
    },
    {
        "family_id": "POSITIONING_COT",
        "label": "Commercial hedger positioning (CFTC COT)",
        "economic_family": "COMMERCIAL_HEDGER_POSITIONING",
        "information_family": "POSITIONING",
        "asset_classes": ["COMMODITY", "RATES", "FX"], "horizons_sessions": [20],
        "status": ST_CHALLENGER,
        "status_reason": "R46 forward challengers on owned CFTC data with its publication lag applied",
        "capital_state": CAP_RESEARCH_ONLY,
        "pit_integrity": "PIT_SAFE_PUBLICATION_LAG_APPLIED",
        "effective_history": "owned CFTC COT archive",
        "turnover": "LOW", "cost_sensitivity": "LOW",
        "evidence": {"release": "R46", "verdict": "FORWARD_PENDING",
                     "doc": "docs/RELEASE46_PROSPECTIVE_ALPHA_TOURNAMENT.md",
                     "numbers": {}},
        "reopen_condition": "n/a - forward evidence pending",
    },
    {
        "family_id": "INDEX_CALENDAR_SEASONALITY",
        "label": "Index calendar seasonality (turn of month)",
        "economic_family": "CALENDAR_SEASONALITY",
        "information_family": "CALENDAR_STRUCTURE",
        "asset_classes": ["EQUITY_INDEX"], "horizons_sessions": [1],
        "status": ST_CHALLENGER,
        "status_reason": "R46 forward challenger with three matured observations",
        "capital_state": CAP_RESEARCH_ONLY,
        "pit_integrity": "PIT_SAFE_CALENDAR_ONLY",
        "effective_history": "owned index futures history",
        "turnover": "LOW", "cost_sensitivity": "LOW",
        "evidence": {"release": "R46", "verdict": "FORWARD_PENDING",
                     "doc": "docs/RELEASE46_PROSPECTIVE_ALPHA_TOURNAMENT.md",
                     "numbers": {}},
        "reopen_condition": "n/a - forward evidence pending",
    },
    {
        "family_id": "OPTIONS_SURFACE",
        "label": "Options surface / variance risk premium",
        "economic_family": "VARIANCE_RISK_PREMIUM",
        "information_family": "OPTIONS_SURFACE",
        "asset_classes": ["OPTIONS"], "horizons_sessions": [5, 20],
        "status": ST_BLOCKED,
        "status_reason": "no owned point-in-time options surface exists",
        "capital_state": CAP_BLOCKED,
        "pit_integrity": "NOT_OWNED",
        "effective_history": "none",
        "turnover": "MEDIUM", "cost_sensitivity": "HIGH",
        "evidence": {"release": "R44 / R45", "verdict": "DATA_WALL",
                     "doc": "docs/RELEASE44_ORTHOGONAL_PORTFOLIO_ALPHA.md",
                     "numbers": {}},
        "reopen_condition": ("a surface vendor evaluated through the Information "
                             "Purchase Gate with demonstrated incremental value"),
    },
    # --------------------- Frontier-level closed results ------------------- #
    {
        "family_id": "OWNED_INFORMATION_FRONTIER",
        "label": "The owned-information frontier as a whole",
        "economic_family": "META_FRONTIER", "information_family": "ALL_OWNED",
        "asset_classes": ["US_EQUITY"], "horizons_sessions": [20, 63],
        "status": ST_EXHAUSTED,
        "status_reason": ("four independent campaigns searched the information we "
                          "own and produced zero survivors between them"),
        "capital_state": CAP_RESEARCH_ONLY, "pit_integrity": "PIT_SAFE",
        "effective_history": "every owned panel",
        "turnover": "n/a", "cost_sensitivity": "n/a",
        "evidence": {"release": "Stage 12 / R27 / R31 / R33 / R34 / R35 / R39 / R57",
                     "verdict": "NEW_ORTHOGONAL_INFORMATION_REQUIRED",
                     "doc": "docs/MATHEMATICAL_ALPHA_FRONTIER.md",
                     "numbers": {"r27_families": 8, "r27_hypotheses": 49,
                                 "r27_survivors": 0,
                                 "r39_bh_survivors": 2,
                                 "r39_deflated_sharpe_survivors": 0,
                                 "r57_families": 12, "r57_survivors": 0,
                                 "r57_note": ("pre-registered protocol, "
                                              "survivorship-safe 20-year panel, "
                                              "untouched lockbox, BH q=0.10: "
                                              "12/12 NO_ALPHA_EVIDENCE"),
                                 "r57_doc": "docs/RELEASE57_ALPHA_DISCOVERY_OFFENSIVE.md"}},
        "reopen_condition": ("genuinely orthogonal NEW information. Not a new "
                             "model on the same panel."),
    },
)

FROZEN_CATALOGUE_VERSION = "r56_frozen_alpha_catalogue.v1"

#: Asset-class readiness. Every row states what the class can do TODAY and what
#: the next state would require. `capital_eligible` is never a research result:
#: it is read from the investability registry at composition time.
ASSET_CLASS_READINESS = (
    {"asset_class": "US_EQUITY", "readiness": AR_FORWARD_SHADOW_READY,
     "data_owner": "api.price_panel + owned fundamental panel",
     "detail": "the only class that carries operational capital today",
     "next_state_requires": None},
    {"asset_class": "EQUITY_INDEX", "readiness": AR_FORWARD_SHADOW_READY,
     "data_owner": "Norgate world futures (purchased)",
     "detail": "R46 challengers emit forward predictions on it",
     "next_state_requires": "a capital-eligible sleeve, which is a human decision"},
    {"asset_class": "RATES", "readiness": AR_FORWARD_SHADOW_READY,
     "data_owner": "Norgate world futures + owned curve series",
     "detail": "carry is measurable and, on the correct denominator, negative",
     "next_state_requires": "remunerated collateral"},
    {"asset_class": "COMMODITY", "readiness": AR_FORWARD_SHADOW_READY,
     "data_owner": "Norgate world futures",
     "detail": "curve carry challengers are accruing forward evidence",
     "next_state_requires": "forward evidence at the declared floor"},
    {"asset_class": "FX", "readiness": AR_FORWARD_SHADOW_READY,
     "data_owner": "Norgate world futures + owned rate series",
     "detail": "predictive in-sample, economically flat after the correct denominator",
     "next_state_requires": "forward evidence on the frozen R51 challenger"},
    {"asset_class": "VOLATILITY", "readiness": AR_FORWARD_SHADOW_READY,
     "data_owner": "Norgate VX continuous",
     "detail": "the fastest-maturing forward cell in the tournament",
     "next_state_requires": "55 more effective independent observations"},
    {"asset_class": "CREDIT", "readiness": AR_RESEARCH_READY,
     "data_owner": "owned FRED / ICE spread series",
     "detail": "proxies only; no tradable credit instrument is owned",
     "next_state_requires": "a tradable credit instrument, not another proxy"},
    {"asset_class": "MULTI_ASSET_FUTURES", "readiness": AR_FORWARD_SHADOW_READY,
     "data_owner": "Norgate world futures, 105 markets",
     "detail": "trend challengers frozen; R32's proxy version was rejected",
     "next_state_requires": "forward evidence"},
    {"asset_class": "CRYPTO", "readiness": AR_BLOCKED,
     "data_owner": "Binance public archive",
     "detail": ("data is complete and the historical result is the strongest we "
                "have ever measured; execution requires an exchange account and "
                "an API key, which the safety boundary forbids"),
     "next_state_requires": "a permitted venue relationship"},
    {"asset_class": "OPTIONS", "readiness": AR_NOT_READY,
     "data_owner": None,
     "detail": "no owned point-in-time surface",
     "next_state_requires": "a surface vendor through the Information Purchase Gate"},
)


def frozen_catalogue() -> list:
    return [dict(x) for x in FROZEN_CATALOGUE]


def asset_class_readiness() -> list:
    return [dict(x) for x in ASSET_CLASS_READINESS]


# --------------------------------------------------------------------------- #
# Expected information value
# --------------------------------------------------------------------------- #
EIV_WEIGHTS = {"orthogonality": 0.35, "evidence_gain": 0.30,
               "prior_plausibility": 0.20, "implementability": 0.15}

EIV_REJECT_EXHAUSTED = "REJECTED_RETESTS_AN_EXHAUSTED_FAMILY_WITH_NO_NEW_INFORMATION"
EIV_REJECT_BLOCKED = "REJECTED_BLOCKED_BY_A_WALL_THE_EXPERIMENT_CANNOT_MOVE"
EIV_REJECT_LOW = "REJECTED_EXPECTED_INFORMATION_VALUE_BELOW_FLOOR"
EIV_ACCEPTED = "QUEUED"
EIV_VOCAB = (EIV_ACCEPTED, EIV_REJECT_EXHAUSTED, EIV_REJECT_BLOCKED, EIV_REJECT_LOW)

#: Below this an experiment costs more attention than the information it can
#: return. Declared, not tuned: a floor that moves to admit a favourite
#: hypothesis is not a floor.
EIV_FLOOR = 0.35


def expected_information_value(experiment: dict) -> dict:
    """Score ONE experiment by what it can TEACH, not by what it might find.

    Four declared components, each in [0, 1]:

    * orthogonality      - how little it overlaps what has already been tested
    * evidence_gain      - how far it moves a family's evidence state
    * prior_plausibility - the economic reason to expect an effect at all
    * implementability   - whether a positive result could ever hold capital

    A high-plausibility idea that duplicates a tested family scores low, and it
    should: we would learn nothing we do not already know.
    """
    comp = {k: max(0.0, min(1.0, float(experiment.get(k) or 0.0)))
            for k in EIV_WEIGHTS}
    score = sum(EIV_WEIGHTS[k] * comp[k] for k in EIV_WEIGHTS)
    status = experiment.get("family_status")
    verdict = EIV_ACCEPTED
    if status == ST_EXHAUSTED and not experiment.get("brings_new_information"):
        verdict = EIV_REJECT_EXHAUSTED
    elif status == ST_BLOCKED and not experiment.get("moves_the_wall"):
        verdict = EIV_REJECT_BLOCKED
    elif score < EIV_FLOOR:
        verdict = EIV_REJECT_LOW
    return {"expected_information_value": round(score, 4),
            "components": {k: round(v, 4) for k, v in comp.items()},
            "weights": dict(EIV_WEIGHTS), "floor": EIV_FLOOR,
            "verdict": verdict, "vocabulary": list(EIV_VOCAB),
            "queued": verdict == EIV_ACCEPTED}


#: The Release-56 experiment agenda. Each entry names its hypothesis, the
#: economic reason to believe it, the distinct information it uses, and - the
#: field that stops a queue becoming a wish list - whether it can produce
#: PROSPECTIVE evidence starting now.
EXPERIMENT_QUEUE_SPEC = (
    {
        "experiment_id": "r56_x1_turnover_penalty",
        "hypothesis": ("Raising the turnover penalty inside the operational "
                       "construction improves net-of-cost forward performance "
                       "without material rank-IC loss."),
        "economic_rationale": ("the live book pays 5.17% average daily turnover "
                               "while its forward decile spread is negative; a "
                               "cost it is not being paid to incur"),
        "family_id": "EQ_XS_MOMENTUM", "family_status": ST_ACTIVE,
        "information_source": "OWNED_FORWARD_EVIDENCE + OWNED_HOC_HISTORY",
        "target_horizon_sessions": 20, "pit_validity": "PIT_SAFE_OWNED_ONLY",
        "cost": "ZERO_MARGINAL", "runtime": "BOUNDED_SINGLE_STUDY",
        "success_criterion": ("net-of-cost forward return improves with rank-IC "
                              "loss under one standard error"),
        "rejection_criterion": "rank IC degrades materially, or net return does not improve",
        "prospective_evidence_now": True,
        "orthogonality": 0.55, "evidence_gain": 0.85, "prior_plausibility": 0.80,
        "implementability": 1.00, "brings_new_information": True,
    },
    {
        "experiment_id": "r56_x2_holding_period_hurdle",
        "hypothesis": ("Requiring a REPLACE to clear its own payback horizon - "
                       "not merely a score improvement - reduces churn without "
                       "losing selection quality."),
        "economic_rationale": ("Release 56 measures that the full zero-base "
                               "rotation needs 29 sessions to repay its switch "
                               "cost while the policy horizon is 20; the "
                               "operational hurdle is expressed in score units "
                               "and cannot see that"),
        "family_id": "EQ_XS_FUNDAMENTAL_QUALITY", "family_status": ST_ACTIVE,
        "information_source": "OWNED_REALLOCATION_HISTORY + OWNED_PRICE_PANEL",
        "target_horizon_sessions": 20, "pit_validity": "PIT_SAFE_OWNED_ONLY",
        "cost": "ZERO_MARGINAL", "runtime": "BOUNDED_SINGLE_STUDY",
        "success_criterion": "fewer replacements and equal or better net forward return",
        "rejection_criterion": "net forward return falls",
        "prospective_evidence_now": True,
        "orthogonality": 0.90, "evidence_gain": 0.80, "prior_plausibility": 0.75,
        "implementability": 1.00, "brings_new_information": True,
    },
    {
        "experiment_id": "r56_x3_forecast_activation_gate",
        "hypothesis": ("The Release-30 forecast's rank ordering, measured "
                       "forward against the approved model's, is good enough to "
                       "justify an activation review."),
        "economic_rationale": ("the entire capital hurdle is unevidenced because "
                               "expected return is NOT_CALIBRATED; activating a "
                               "calibrated forecast is the single change that "
                               "would make an economic hurdle possible"),
        "family_id": "EQ_ML_CROSS_SECTIONAL", "family_status": ST_CHALLENGER,
        "information_source": "OWNED_FORWARD_EVIDENCE + api.return_forecast",
        "target_horizon_sessions": 20, "pit_validity": "PIT_SAFE_OWNED_ONLY",
        "cost": "ZERO_MARGINAL", "runtime": "BOUNDED_ROLLING_COMPARISON",
        "success_criterion": ("forward rank IC of the research forecast exceeds "
                              "the approved model's on a common window"),
        "rejection_criterion": "it does not, on the common window",
        "prospective_evidence_now": True,
        "orthogonality": 0.70, "evidence_gain": 0.95, "prior_plausibility": 0.55,
        "implementability": 0.90, "brings_new_information": True,
    },
    {
        "experiment_id": "r56_x4_portfolio_challenger_forward",
        "hypothesis": ("A zero-base constructed portfolio beats the incumbent "
                       "book forward, net of its switching cost."),
        "economic_rationale": ("every prior comparison has been a SIGNAL "
                               "comparison; the portfolio is what actually holds "
                               "the capital, and it has never been raced"),
        "family_id": "EQ_XS_FUNDAMENTAL_QUALITY", "family_status": ST_ACTIVE,
        "information_source": "api.shadow_portfolio_evidence (frozen R56 records)",
        "target_horizon_sessions": 20, "pit_validity": "PIT_SAFE_FROZEN_AT_INCEPTION",
        "cost": "ZERO_MARGINAL", "runtime": "CONTINUOUS_FORWARD_ACCRUAL",
        "success_criterion": "positive excess over the incumbent on equal windows",
        "rejection_criterion": "negative excess once the evidence floor is reached",
        "prospective_evidence_now": True,
        "orthogonality": 0.95, "evidence_gain": 0.90, "prior_plausibility": 0.60,
        "implementability": 1.00, "brings_new_information": True,
    },
    {
        "experiment_id": "r56_x5_vx_term_carry_maturity",
        "hypothesis": ("The volatility term-carry cell reaches its declared "
                       "evidence floor with its measured edge intact."),
        "economic_rationale": ("it is the only non-equity cell with a positive "
                               "measured forward net alpha and the fastest "
                               "maturity schedule in the tournament"),
        "family_id": "VOLATILITY_TERM_CARRY",
        "family_status": ST_FORWARD_EVIDENCE_NEEDED,
        "information_source": "alpha_agent.r46 tournament (already running)",
        "target_horizon_sessions": 1, "pit_validity": "PIT_SAFE_TRUE_FORWARD",
        "cost": "ZERO_MARGINAL", "runtime": "CONTINUOUS_DAILY_CYCLE",
        "success_criterion": "the declared effective-independent floor with t above its gate",
        "rejection_criterion": "the edge decays as observations accumulate",
        "prospective_evidence_now": True,
        "orthogonality": 0.85, "evidence_gain": 0.70, "prior_plausibility": 0.65,
        "implementability": 0.55, "brings_new_information": True,
    },
    {
        "experiment_id": "r56_x6_cash_drag_attribution",
        "hypothesis": ("The book's realised underperformance is name selection, "
                       "not cash drag, cost or beta."),
        "economic_rationale": ("without decomposing it, every proposed remedy is "
                               "a guess; the decomposition decides whether to "
                               "fix the model or the construction"),
        "family_id": "EQ_XS_FUNDAMENTAL_QUALITY", "family_status": ST_ACTIVE,
        "information_source": "api.paper_trading_desk attribution + forward evidence",
        "target_horizon_sessions": 20, "pit_validity": "PIT_SAFE_REALISED_ONLY",
        "cost": "ZERO_MARGINAL", "runtime": "BOUNDED_SINGLE_STUDY",
        "success_criterion": "an attributed decomposition that sums to the realised excess",
        "rejection_criterion": "the residual dominates the attributed terms",
        "prospective_evidence_now": True,
        "orthogonality": 0.80, "evidence_gain": 0.85, "prior_plausibility": 0.90,
        "implementability": 1.00, "brings_new_information": True,
    },
    {
        "experiment_id": "r56_x7_rerun_r32_sleeves",
        "hypothesis": "One of the six R32 sleeves beats its control if re-run.",
        "economic_rationale": "none - the information set has not changed",
        "family_id": "EQ_SECTOR_ROTATION", "family_status": ST_EXHAUSTED,
        "information_source": "the same R32 panel",
        "target_horizon_sessions": 20, "pit_validity": "PIT_SAFE_OWNED_ONLY",
        "cost": "HIGH_COMPUTE", "runtime": "FULL_CAMPAIGN",
        "success_criterion": "n/a",
        "rejection_criterion": "n/a",
        "prospective_evidence_now": False,
        "orthogonality": 0.05, "evidence_gain": 0.05, "prior_plausibility": 0.30,
        "implementability": 0.80, "brings_new_information": False,
        "included_to_show_the_queue_rejects": True,
    },
    {
        "experiment_id": "r56_x8_crypto_funding_carry_live",
        "hypothesis": "Run the R41 funding-carry book forward on a live venue.",
        "economic_rationale": "the historical evidence is the strongest we hold",
        "family_id": "CRYPTO_FUNDING_CARRY", "family_status": ST_BLOCKED,
        "information_source": "Binance archive + a live exchange account",
        "target_horizon_sessions": 1, "pit_validity": "PIT_SAFE_EXCHANGE_ARCHIVE",
        "cost": "REQUIRES_VENUE_RELATIONSHIP", "runtime": "CONTINUOUS",
        "success_criterion": "n/a",
        "rejection_criterion": "n/a",
        "prospective_evidence_now": False,
        "orthogonality": 0.90, "evidence_gain": 0.90, "prior_plausibility": 0.85,
        "implementability": 0.00, "brings_new_information": False,
        "moves_the_wall": False,
        "included_to_show_the_queue_rejects": True,
    },
)


def experiment_queue() -> dict:
    """The agenda, scored and ranked by expected information value."""
    rows = []
    for spec in EXPERIMENT_QUEUE_SPEC:
        row = dict(spec)
        row.update(expected_information_value(spec))
        rows.append(row)
    queued = [r for r in rows if r["queued"]]
    rejected = [r for r in rows if not r["queued"]]
    queued.sort(key=lambda r: -r["expected_information_value"])
    rejected.sort(key=lambda r: -r["expected_information_value"])
    for i, r in enumerate(queued, 1):
        r["queue_position"] = i
    return {
        "calculation_owner": CALCULATION_OWNER,
        "scoring_method": "EXPECTED_INFORMATION_VALUE_v1",
        "weights": dict(EIV_WEIGHTS), "floor": EIV_FLOOR,
        "n_total": len(rows), "n_queued": len(queued), "n_rejected": len(rejected),
        "queued": queued, "rejected": rejected,
        "all_experiments_require_manual_approval": True,
        "automatic_execution_allowed": False,
        "doc": ("an experiment that re-tests an EXHAUSTED family without new "
                "information is rejected by name rather than quietly ranked "
                "last, because a queue that never says no is a wish list"),
    }


# --------------------------------------------------------------------------- #
# Live composition
# --------------------------------------------------------------------------- #
def _live_from_tournament(tournament: Optional[dict]) -> dict:
    """Live per-economic-family facts from the Release-46 forward tournament.

    Aggregated by the tournament's OWN family label. Nothing is recomputed: the
    counts and the best measured cell are read as published.
    """
    out: dict = {}
    for row in (tournament or {}).get("leaderboard") or []:
        fam = row.get("family")
        if not fam:
            continue
        blk = out.setdefault(fam, {
            "economic_family": fam, "n_challengers": 0, "asset_classes": set(),
            "horizons": set(), "states": {}, "raw_matured": 0,
            "effective_independent": 0, "best_net_alpha_bps": None,
            "best_challenger_id": None, "best_t_stat": None,
            "promotion_allowed": False})
        blk["n_challengers"] += 1
        if row.get("asset_class"):
            blk["asset_classes"].add(row["asset_class"])
        if row.get("horizon") is not None:
            blk["horizons"].add(row["horizon"])
        st = row.get("state")
        blk["states"][st] = blk["states"].get(st, 0) + 1
        blk["raw_matured"] += int(row.get("raw_matured") or 0)
        blk["effective_independent"] += int(row.get("effective_independent") or 0)
        na = row.get("net_alpha_bps")
        if na is not None and (blk["best_net_alpha_bps"] is None
                               or na > blk["best_net_alpha_bps"]):
            blk["best_net_alpha_bps"] = na
            blk["best_challenger_id"] = row.get("challenger_id")
            blk["best_t_stat"] = row.get("t_stat")
    for blk in out.values():
        blk["asset_classes"] = sorted(blk["asset_classes"])
        blk["horizons"] = sorted(blk["horizons"])
    return out


def _live_from_r32(r32: Optional[dict]) -> dict:
    out: dict = {}
    for s in (r32 or {}).get("sleeves") or []:
        name = s.get("sleeve")
        if name:
            out[name] = dict(s)
    return out


def _capital_eligible_classes(frontier: Optional[dict]) -> list:
    rows = (frontier or {}).get("rows") or []
    return sorted({r.get("asset_class") for r in rows
                   if r.get("eligible") and r.get("asset_class")})


def load_alpha_opportunity_registry(*, tournament: Optional[dict] = None,
                                    r32_frontier: Optional[dict] = None,
                                    opportunity_frontier: Optional[dict] = None,
                                    sleeves: Optional[dict] = None,
                                    scoring: Optional[dict] = None) -> dict:
    """The GET read model. Read-only and degrade-safe."""
    degraded = []
    if tournament is None:
        try:
            from paper_trader.api import prospective_tournament as pt
            tournament = pt.load_prospective_tournament()
        except Exception as exc:                                   # noqa: BLE001
            tournament, _ = {}, degraded.append(
                {"source": "api.prospective_tournament", "detail": type(exc).__name__})
    if r32_frontier is None:
        try:
            from paper_trader.api import pnl_opportunity_frontier as pf
            r32_frontier = pf.load_frontier()
        except Exception as exc:                                   # noqa: BLE001
            r32_frontier, _ = {}, degraded.append(
                {"source": "api.pnl_opportunity_frontier", "detail": type(exc).__name__})
    if opportunity_frontier is None:
        try:
            from paper_trader.api import opportunity_frontier as of
            opportunity_frontier = of.load_opportunity_frontier()
        except Exception as exc:                                   # noqa: BLE001
            opportunity_frontier, _ = {}, degraded.append(
                {"source": "api.opportunity_frontier", "detail": type(exc).__name__})
    if scoring is None:
        try:
            from paper_trader.api import universe_scoring as us
            scoring = us.load_universe_scoring()
        except Exception as exc:                                   # noqa: BLE001
            scoring, _ = {}, degraded.append(
                {"source": "api.universe_scoring", "detail": type(exc).__name__})

    live_t = _live_from_tournament(tournament)
    live_32 = _live_from_r32(r32_frontier)
    cap_classes = _capital_eligible_classes(opportunity_frontier)

    families = []
    for row in frozen_catalogue():
        fam = dict(row)
        fam["status_class"] = STATUS_CLASS.get(fam["status"], "muted")
        fam["frozen_status"] = fam["status"]
        lt = live_t.get(fam["economic_family"])
        fam["live_tournament"] = lt
        fam["live_status"] = None
        if lt:
            states = lt.get("states") or {}
            if states.get("EARLY_FORWARD_EVIDENCE"):
                fam["live_status"] = ST_FORWARD_EVIDENCE_NEEDED
            elif states.get("FORWARD_CONFIRMED"):
                fam["live_status"] = ST_PROMISING
            elif states.get("DATA_BLOCKED") and len(states) == 1:
                fam["live_status"] = ST_BLOCKED
            else:
                fam["live_status"] = ST_CHALLENGER
        # A live tournament challenger inside a family that is ACTIVE is
        # EXPECTED, not a contradiction, and a live challenger inside a CLOSED
        # family is the informative case: it usually means a later release
        # re-opened the question on BETTER DATA. Both are annotated; neither is
        # allowed to silently rewrite the frozen verdict.
        fam["status_disagreement"] = bool(
            fam["live_status"] and fam["frozen_status"] in
            (ST_EXHAUSTED, ST_REJECTED, ST_BLOCKED))
        fam["live_status_note"] = (
            ("a later release froze a forward challenger in this family; the "
             "frozen verdict describes the EARLIER experiment and still stands "
             "for it")
            if fam["status_disagreement"] else None)
        fam["capital_eligible_today"] = bool(
            fam["capital_state"] == CAP_ELIGIBLE
            and any(ac in cap_classes for ac in fam["asset_classes"]))
        fam["forward_evidence_observations"] = (lt or {}).get("raw_matured", 0)
        fam["effective_independent_observations"] = (lt or {}).get(
            "effective_independent", 0)
        fam["best_measured_net_alpha_bps"] = (lt or {}).get("best_net_alpha_bps")
        fam["promotion_allowed"] = False
        families.append(fam)

    counts: dict = {}
    for f in families:
        counts[f["status"]] = counts.get(f["status"], 0) + 1
    by_class: dict = {}
    for f in families:
        for ac in f["asset_classes"]:
            by_class.setdefault(ac, []).append(f["family_id"])

    readiness = []
    for r in asset_class_readiness():
        r["capital_eligible_today"] = r["asset_class"] in cap_classes
        readiness.append(r)

    actionable = [f for f in families
                  if f["status"] in (ST_ACTIVE, ST_CHALLENGER, ST_PROMISING,
                                     ST_FORWARD_EVIDENCE_NEEDED)]
    closed = [f for f in families
              if f["status"] in (ST_EXHAUSTED, ST_REJECTED, ST_BLOCKED)]

    return {
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": CALCULATION_OWNER,
        "phase": PHASE, "route": ROUTE,
        "state": STATE_DEGRADED if degraded else STATE_READY,
        "state_vocabulary": list(READ_STATE_VOCAB),
        "generated_at": _now_iso(),
        "frozen_catalogue_version": FROZEN_CATALOGUE_VERSION,
        "status_vocabulary": list(STATUS_VOCAB),
        "capital_vocabulary": list(CAPITAL_VOCAB),
        "readiness_vocabulary": list(READINESS_VOCAB),
        "n_families": len(families),
        "n_actionable": len(actionable),
        "n_closed": len(closed),
        "status_counts": counts,
        "families": families,
        "families_by_asset_class": {k: sorted(v) for k, v in sorted(by_class.items())},
        "asset_class_readiness": readiness,
        "capital_eligible_asset_classes": cap_classes,
        "experiment_queue": experiment_queue(),
        "live_sources": {
            "tournament_state": (tournament or {}).get("state"),
            "tournament_challengers": (tournament or {}).get("how_many_are_active"),
            "tournament_forward_predictions":
                (tournament or {}).get("how_many_real_forward_predictions_exist"),
            "tournament_matured": (tournament or {}).get("how_many_have_matured"),
            "r32_primary_verdict": (r32_frontier or {}).get("primary_verdict"),
            "r32_qualified": (r32_frontier or {}).get("n_qualified"),
            "opportunity_frontier_expected_return_state":
                (opportunity_frontier or {}).get("expected_return_state"),
            "champion_model_id": (scoring or {}).get("primary_model_id"),
            "r32_sleeve_states": {k: v.get("state") for k, v in live_32.items()},
        },
        "degraded_sources": degraded,
        "headline": _headline(families, counts),
        "safety": {
            "badges": list(SAFETY_BADGES), "research_only": True,
            "read_only": True, "creates_signals": False,
            "creates_trade_decisions": False, "creates_orders": False,
            "creates_proposal": False, "mutates_holdings": False,
            "promotes_model": False, "activates_sleeve": False,
            "enables_automation": False, "writes_operational_store": False,
            "automatic_promotion_allowed": False,
        },
    }


def _headline(families: list, counts: dict) -> str:
    live = counts.get(ST_ACTIVE, 0)
    ch = counts.get(ST_CHALLENGER, 0) + counts.get(ST_FORWARD_EVIDENCE_NEEDED, 0)
    closed = (counts.get(ST_EXHAUSTED, 0) + counts.get(ST_REJECTED, 0)
              + counts.get(ST_BLOCKED, 0))
    return ("%d economically distinct alpha families: %d carry capital today, "
            "%d are competing forward without capital, and %d are closed "
            "(exhausted, rejected on their merits, or walled off by data or "
            "venue). Re-opening a closed family requires the named condition on "
            "its row, not a new random seed."
            % (len(families), live, ch, closed))


def summary(payload: Optional[dict] = None, **kwargs) -> dict:
    p = payload if payload is not None else load_alpha_opportunity_registry(**kwargs)
    q = p.get("experiment_queue") or {}
    return {
        "state": p.get("state"),
        "n_families": p.get("n_families"),
        "n_actionable": p.get("n_actionable"),
        "n_closed": p.get("n_closed"),
        "status_counts": p.get("status_counts"),
        "capital_eligible_asset_classes": p.get("capital_eligible_asset_classes"),
        "n_experiments_queued": q.get("n_queued"),
        "n_experiments_rejected": q.get("n_rejected"),
        "next_experiment": (q.get("queued") or [{}])[0].get("experiment_id"),
        "headline": p.get("headline"),
    }


__all__ = [
    "SCHEMA_VERSION", "COMPOSITION_OWNER", "CALCULATION_OWNER", "PHASE", "ROUTE",
    "STATUS_VOCAB", "ST_ACTIVE", "ST_CHALLENGER", "ST_PROMISING",
    "ST_FORWARD_EVIDENCE_NEEDED", "ST_EXHAUSTED", "ST_REJECTED", "ST_BLOCKED",
    "ST_UNTESTED", "STATUS_CLASS", "CAPITAL_VOCAB", "CAP_ELIGIBLE",
    "CAP_RESEARCH_ONLY", "CAP_BLOCKED", "READINESS_VOCAB", "AR_DATA_READY",
    "AR_RESEARCH_READY", "AR_FORWARD_SHADOW_READY", "AR_NOT_READY", "AR_BLOCKED",
    "FROZEN_CATALOGUE", "FROZEN_CATALOGUE_VERSION", "ASSET_CLASS_READINESS",
    "EIV_WEIGHTS", "EIV_FLOOR", "EIV_VOCAB", "EXPERIMENT_QUEUE_SPEC",
    "SAFETY_BADGES", "STATE_READY", "STATE_DEGRADED", "STATE_UNAVAILABLE",
    "frozen_catalogue", "asset_class_readiness", "expected_information_value",
    "experiment_queue", "load_alpha_opportunity_registry", "summary",
]
