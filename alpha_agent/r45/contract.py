"""alpha_agent.r45.contract - frozen BEFORE any Release-45 bar is scored.

Everything in this file is a DECLARATION. The single most important one is
:data:`FROZEN_RULE`: it is copied verbatim out of Release 44's screened cell
and Release 45 is forbidden to change any of its numbers before the first
independent replication result has been recorded.

Why that matters. R44 chose XAUUSD / REVERSAL / +5 / +120 by screening 60
cells (3 instruments x 2 entry delays x 5 holds x 2 rules) on the FIRST 50 %
of its events. The remaining 50 % - 370 gold events between 2018-12-14 and
2026-08-18 - were never scored by anything. They are a clean holdout for a
rule whose parameters cannot leak into them, and the estate already owns
every one of their bars. That test is free, it is decisive, and it is
Release 45's first act.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

RELEASE = "R45"
CAMPAIGN_ID = "r45_macro_event_alpha_v1"
CALCULATION_OWNER = "alpha_agent.r45.contract"
RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\macro_event_alpha_r45")
ARTIFACT_DIR = RESEARCH_ROOT / CAMPAIGN_ID

OBJECTIVE = (
    "decide whether the scheduled-US-macro-release reversal Release 44 "
    "measured in gold is a real, implementable, repeatable mispricing - by "
    "replicating the frozen rule, unchanged, in the events R44 never scored "
    "and in the markets where the release is actually price-discovered"
)

# --------------------------------------------------------------------------- #
# THE FROZEN RULE - inherited verbatim, not re-derived
# --------------------------------------------------------------------------- #
#: R44's screened cell. Every number here was chosen by Release 44 on Release
#: 44's zone A and is FIXED for the whole of Release 45's first replication.
FROZEN_RULE = {
    "source_release": "R44",
    "source_lane": "E1B_INTRADAY_EVENT",
    "source_artifact": (
        r"D:\Stock_Prediction_app_data\orthogonal_portfolio_alpha_r44"
        r"\r44_orthogonal_portfolio_alpha_v1\R44_LANE_RESULTS.json"
        "::engine1.intraday_prosecution"),
    "instrument_of_origin": "XAUUSD",
    "rule": "REVERSAL",
    "entry_delay_min": 5,
    "hold_min": 120,
    "shock_window_min": 1,
    "bar_tolerance_min": 3,
    "position": "-sign(shock) * 1 unit of notional",
    "shock": "close of the entry bar / close of the last bar before the "
             "release stamp - 1",
    "forward": "close of the exit bar / close of the entry bar - 1",
    "event_family": "scheduled US macro releases with a declared ET time",
    "cost": "half-spread paid on BOTH legs plus a declared slippage "
            "allowance, charged on EVERY event whether it won or lost",
}

#: What R44 recorded for that cell, on its zone A only. Release 45 verifies
#: these against the frozen artifact before it is allowed to run.
R44_ZONE_A_REFERENCE = {
    "symbol": "XAUUSD", "zone": "A", "n_events": 386,
    "gross_bps_per_event": 6.978859540689271,
    "gross_t": 2.614921750533075,
    "cost_bps_per_event": 2.560701337669724,
    "net_bps_per_event": 4.418158203019549,
    "net_t": 1.6572127682028737,
    "hit_rate": 0.5544041450777202,
}
R44_REFERENCE_TOLERANCE = 1e-6

#: R44's own cross-instrument result, also inherited as a fact to beat.
R44_CROSS_INSTRUMENT = {
    "EURUSD": {"n_events": 405, "net_bps_per_event": -0.5646686960924525,
               "net_t": -0.3585744146116955},
    "USDJPY": {"n_events": 435, "net_bps_per_event": 0.17752828643361818,
               "net_t": 0.1198066830402702},
}

NO_PARAMETER_SEARCH_BEFORE_FIRST_REPLICATION = True
RETUNING_AFTER_A_FAILED_FROZEN_TEST_IS_NOT_A_REPLICATION = True

# --------------------------------------------------------------------------- #
# Event calendar - point in time
# --------------------------------------------------------------------------- #
#: Inherited unchanged from R44. The DATES are point-in-time (published in
#: advance); the TIMES are a declared constant, stated so a reader can check
#: them, and every result is reported against a timing perturbation sweep.
MACRO_RELEASE_TIMES_ET = {
    "EMPLOYMENT_SITUATION": "08:30", "CPI": "08:30", "PPI": "08:30",
    "GDP": "08:30", "RETAIL_SALES": "08:30", "PERSONAL_INCOME_PCE": "08:30",
    "DURABLE_GOODS": "08:30", "HOUSING_STARTS": "08:30",
    "INDUSTRIAL_PRODUCTION": "09:15",
}
MACRO_RELEASE_TIMES_ARE_A_DECLARED_CONSTANT = True
RELEASE_CALENDAR_IS_POINT_IN_TIME = True
MIN_CALENDAR_YEAR = 2012

# --------------------------------------------------------------------------- #
# Instrument classes - the honesty layer
# --------------------------------------------------------------------------- #
#: An instrument's CLASS decides what a result about it is allowed to be
#: called. Nothing here is a proxy standing in for something it is not.
INSTRUMENT_CLASS = {
    "NATIVE_FUTURES": "a dated or continuous contract quoted by the exchange "
                      "that lists it. A replication here answers the R45 "
                      "question directly.",
    "LISTED_ETF": "an exchange-listed fund tracking the same underlying. A "
                  "real, tradable instrument in its own right - NOT a "
                  "futures contract, and never reported as one.",
    "OTC_SPOT": "spot FX and spot metal quoted by a real broker with an "
                "observed bid/ask. Real instruments; this is what R44 used.",
    "CFD": "a contract for difference referencing an index or a future. "
           "Tradable, but it is NOT the future, and a CFD result may never "
           "be reported as a native futures replication.",
}
NO_CFD_PROXY_FOR_A_FUTURES_HYPOTHESIS = True
NO_ETF_PROXY_FOR_A_FUTURES_HYPOTHESIS = True
AN_INSTRUMENT_MAY_BE_TESTED_AS_ITSELF = True

#: Owned Dukascopy minute bars, with the broker's observed spread on every
#: row. Class is stated per symbol and is not negotiable.
OWNED_MINUTE_INSTRUMENTS = {
    "XAUUSD": {"class": "OTC_SPOT", "underlying": "GOLD"},
    "EURUSD": {"class": "OTC_SPOT", "underlying": "EURUSD"},
    "USDJPY": {"class": "OTC_SPOT", "underlying": "USDJPY"},
    "USA500IDXUSD": {"class": "CFD", "underlying": "SP500"},
    "DEUIDXEUR": {"class": "CFD", "underlying": "DAX"},
    "BUNDTREUR": {"class": "CFD", "underlying": "EURO_BUND"},
    "LIGHTCMDUSD": {"class": "CFD", "underlying": "WTI"},
}

#: US-listed instruments acquired at minute resolution from an entitlement
#: the estate already holds. These are the US rates and equity exposures the
#: R45 question is about - as ETFs, labelled as ETFs.
LISTED_MINUTE_INSTRUMENTS = {
    "SHY": {"class": "LISTED_ETF", "underlying": "UST_1_3Y", "sleeve": "RATES"},
    "IEF": {"class": "LISTED_ETF", "underlying": "UST_7_10Y", "sleeve": "RATES"},
    "TLT": {"class": "LISTED_ETF", "underlying": "UST_20Y+", "sleeve": "RATES"},
    "SPY": {"class": "LISTED_ETF", "underlying": "SP500", "sleeve": "EQUITY"},
    "QQQ": {"class": "LISTED_ETF", "underlying": "NASDAQ100",
            "sleeve": "EQUITY"},
    "GLD": {"class": "LISTED_ETF", "underlying": "GOLD", "sleeve": "GOLD"},
    "UUP": {"class": "LISTED_ETF", "underlying": "USD_INDEX", "sleeve": "FX"},
}

#: Native exchange-listed futures, acquired at native resolution. The window
#: is short and that is reported, not hidden.
NATIVE_FUTURES_INSTRUMENTS = {
    "ZT=F": {"class": "NATIVE_FUTURES", "exchange": "CBOT",
             "underlying": "UST_2Y", "sleeve": "RATES"},
    "ZF=F": {"class": "NATIVE_FUTURES", "exchange": "CBOT",
             "underlying": "UST_5Y", "sleeve": "RATES"},
    "ZN=F": {"class": "NATIVE_FUTURES", "exchange": "CBOT",
             "underlying": "UST_10Y", "sleeve": "RATES"},
    "ZB=F": {"class": "NATIVE_FUTURES", "exchange": "CBOT",
             "underlying": "UST_30Y", "sleeve": "RATES"},
    "ES=F": {"class": "NATIVE_FUTURES", "exchange": "CME",
             "underlying": "SP500", "sleeve": "EQUITY"},
    "NQ=F": {"class": "NATIVE_FUTURES", "exchange": "CME",
             "underlying": "NASDAQ100", "sleeve": "EQUITY"},
    "GC=F": {"class": "NATIVE_FUTURES", "exchange": "COMEX",
             "underlying": "GOLD", "sleeve": "GOLD"},
    "6E=F": {"class": "NATIVE_FUTURES", "exchange": "CME",
             "underlying": "EURUSD", "sleeve": "FX"},
    "6J=F": {"class": "NATIVE_FUTURES", "exchange": "CME",
             "underlying": "USDJPY", "sleeve": "FX"},
    "CL=F": {"class": "NATIVE_FUTURES", "exchange": "NYMEX",
             "underlying": "WTI", "sleeve": "ENERGY"},
}

# --------------------------------------------------------------------------- #
# Cost - what may be charged, and what it must be called
# --------------------------------------------------------------------------- #
#: An OBSERVED spread comes off the bar. An ESTIMATED spread is computed from
#: the bar's own high/low by Corwin-Schultz and is labelled ESTIMATED on
#: every row, every card and every artifact it ever touches.
COST_SOURCE_OBSERVED = "OBSERVED_HALF_SPREAD_FROM_THE_BAR"
COST_SOURCE_ESTIMATED = "ESTIMATED_HALF_SPREAD_CORWIN_SCHULTZ_FROM_OWN_BARS"
COST_SOURCE_MUST_BE_LABELLED = True
SLIPPAGE_BPS_PER_SIDE = 0.2
#: A floor under the estimated spread so a quiet bar cannot print free
#: execution. Stated per instrument class, in basis points per side.
ESTIMATED_HALF_SPREAD_FLOOR_BPS = {
    "LISTED_ETF": 0.25, "NATIVE_FUTURES": 0.25, "CFD": 0.5, "OTC_SPOT": 0.5,
}
COST_STRESS_MULTIPLIERS = (1.0, 2.0, 3.0)
LATENCY_STRESS_EXTRA_MINUTES = (0, 1, 2, 5)
NO_MIDPOINT_FILL_WITHOUT_A_QUOTE = True
NO_FABRICATED_FILL = True

# --------------------------------------------------------------------------- #
# Lanes
# --------------------------------------------------------------------------- #
LANES = {
    "L1_GOLD_HOLDOUT":
        "does the frozen rule survive the 370 gold events R44 never scored?",
    "L2_LISTED_US":
        "does the frozen rule appear in US rates and equities on listed "
        "instruments the estate can already reach?",
    "L3_NATIVE_FUTURES":
        "does the frozen rule appear in NATIVE CBOT/CME/COMEX futures?",
    "L4_OWNED_BREADTH":
        "does the frozen rule appear anywhere else the estate owns at "
        "minute resolution, over the full 2012-2026 window?",
    "L5_CAUSAL":
        "is whatever survives actually about the release?",
    "L6_FAMILY":
        "which economically distinct releases drive it?",
    "L7_DISCOVERY":
        "which market prices US macro information first?",
    "L8_RV":
        "is there a temporary RELATIVE mispricing after the shock?",
    "L9_SURPRISE":
        "does the response scale with a PIT-safe measure of surprise?",
    "L10_STATE":
        "does the effect live only in a particular pre-event state?",
    "L11_ML":
        "does any richer model beat the frozen transparent rule?",
    "L12_KILL":
        "what kills the strongest candidate?",
    "L13_OPTIONS":
        "did the free option surface deepen toward a judgeable sample?",
    "L14_ANALYST":
        "did the prospective analyst-revision ledger deepen?",
}
#: Lanes that MUST report before any lane is allowed to change a parameter.
REPLICATION_LANES_FIRST = ("L1_GOLD_HOLDOUT", "L2_LISTED_US",
                           "L3_NATIVE_FUTURES", "L4_OWNED_BREADTH")

REPLICATION_STATES = ("REPLICATES", "DOES_NOT_REPLICATE", "DATA_INSUFFICIENT")
#: Below this, a lane may not be called a replication either way.
MIN_EVENTS_TO_JUDGE_REPLICATION = 60
#: Below this, no candidate may be called qualified, whatever its t.
MIN_EVENTS_TO_QUALIFY = 100
#: A market REPLICATES only with a positive net return of the SAME SIGN as
#: R44's and a clustered t at or above this. Anything positive but weaker is
#: reported with its numbers and is still DOES_NOT_REPLICATE.
REPLICATION_NET_T_MIN = 2.0
#: The frozen tolerance is 3 minutes. A panel whose bars are coarser than
#: that cannot express the rule at all unless the tolerance widens to exactly
#: one bar - the smallest possible change - and every result computed that
#: way is labelled resolution-degraded on the card and in the artifact.
TOLERANCE_SCALES_WITH_BAR_INTERVAL = True
RESOLUTION_DEGRADED_RESULTS_ARE_LABELLED = True

# --------------------------------------------------------------------------- #
# Qualification - deliberately stricter than "t > 2 once"
# --------------------------------------------------------------------------- #
QUALIFICATION = {
    "min_events": MIN_EVENTS_TO_QUALIFY,
    "net_bps_per_event_gt": 0.0,
    "net_t_cluster_ge": 2.5,
    "must_survive_cost_x2": True,
    "must_survive_latency_plus_1min": True,
    "must_beat_its_placebo": True,
    "must_be_release_locked": True,
    "min_independent_positive_windows": 2,
    "no_single_event_above_share_of_pnl": 0.25,
    "no_single_year_above_share_of_pnl": 0.60,
    "must_survive_leave_one_year_out": True,
    "must_survive_leave_one_family_out": True,
    "must_survive_search_adjustment": True,
}
#: Inference is clustered by EVENT DATE, because several releases share a
#: stamp and their event returns are not independent.
CLUSTER_INFERENCE_BY = "EVENT_DATE"
HAC_LAGS = 5
BOOTSTRAP_DRAWS = 5000
A_SINGLE_T_ABOVE_2_IS_NOT_A_QUALIFICATION = True

QUALIFICATION_STATES = (
    "QUALIFIED_ALPHA", "RESEARCH_CANDIDATE", "WEAK_EVIDENCE",
    "NOT_A_CANDIDATE", "REFUTED", "DATA_INSUFFICIENT",
)

# --------------------------------------------------------------------------- #
# Search burden
# --------------------------------------------------------------------------- #
INHERITED_GLOBAL_BURDEN = 310
INHERITED_GLOBAL_BURDEN_CONSERVATIVE = 312
BURDEN_MAY_NEVER_BE_RESET = True
BURDEN_FAMILIES = (
    "FROZEN_MACRO_REPLICATION", "EVENT_FAMILY", "EVENT_RELATIVE_VALUE",
    "EVENT_STATE_CONDITIONING", "EVENT_ML", "OPTIONS_VOL",
    "ANALYST_REVISIONS",
)
#: One predeclared mechanism tested in many markets is ONE confirmation
#: programme, not one trial per market - but the moment a parameter moves,
#: every cell is charged.
FROZEN_REPLICATION_IS_ONE_TRIAL = True
FROZEN_REPLICATION_TRIAL_COUNT = 1
POST_REPLICATION_EXPLORATION_IS_CHARGED_PER_CELL = True

# --------------------------------------------------------------------------- #
# Safety
# --------------------------------------------------------------------------- #
AUTHORIZED_SPEND_USD = 0.0
RESEARCH_ONLY = True
PROMOTION_ALLOWED = False
FORBIDDEN = (
    "orders", "paper orders", "broker connections", "broker authentication",
    "exchange account creation", "capital deposit", "withdrawal",
    "portfolio mutation", "cash mutation", "operational target portfolio",
    "operational proposal", "operational decision", "model promotion",
    "sleeve activation", "scheduler activation", "production restart",
    "subscription purchase", "paid trial", "payment details",
    "licence acceptance on the operator's behalf", "cloud/GPU spending",
)
BLOCKER_VOCAB = (
    "EXECUTED", "PAYMENT_REQUIRED", "ACCOUNT_REQUIRED", "LICENCE_REQUIRED",
    "HISTORICAL_DATA_UNAVAILABLE", "PIT_INTEGRITY_FAILURE",
    "SURVIVORSHIP_FAILURE", "IDENTITY_FAILURE", "COMPUTE_SPEND_REQUIRED",
    "FUTURE_TIME_REQUIRED", "SAFETY_BLOCKER", "IRREPARABLE_TECHNICAL_FAILURE",
)
A_FAILED_LANE_IS_A_ROUTING_EVENT = True
ONE_LANE_MAY_NOT_HALT_ANOTHER = True
NO_BROAD_EXECUTABLE_ZERO_COST_BRANCH_MAY_BE_DEFERRED = True

PIT_CHECKS = (
    "no future information in any signal",
    "no current snapshot substituted for a historical vintage",
    "no reconstructed vintage",
    "no interpolated intraday bar",
    "no fabricated fill",
    "no hindsight extrema",
    "no hedge ratio estimated on the window it is applied to",
    "survivorship-safe universes only",
)
NO_INTERPOLATED_INTRADAY = True
NO_CURRENT_SNAPSHOT_AS_HISTORICAL_VINTAGE = True
HEDGE_RATIOS_ARE_FITTED_ON_TRAINING_EVENTS_ONLY = True

# --------------------------------------------------------------------------- #
# Forward freeze
# --------------------------------------------------------------------------- #
PRIOR_SHADOWS_ARE_IMMUTABLE = True
NEVER_BACKFILL_PROSPECTIVE_ROWS = True
MAX_NEW_SHADOWS = 3
FREEZE_REQUIRES = (
    "a qualification state of RESEARCH_CANDIDATE or better",
    "a positive result on events the parameters were not chosen on",
    "a kill battery that did not overturn it",
    "an economic expression that can be written down completely",
)
DO_NOT_FREEZE_MEDIOCRE_CANDIDATES_TO_CREATE_A_SHADOW = True

# --------------------------------------------------------------------------- #
# Shell policy
# --------------------------------------------------------------------------- #
SHELL_POLICY = "WINDOWS_POWERSHELL_ONLY"
FORBIDDEN_SHELLS = ("bash", "wsl", "git-bash", "sh", "zsh")
SHELL_POLICY_WAIVERS_ARE_NOT_AVAILABLE = True
INHERITED_SHELL_DISCLOSURES = (
    {"release": "R42", "events": 1,
     "what": "a single read-only Bash invocation, disclosed and preserved"},
    {"release": "R44", "events": 1,
     "what": "a single no-op 'sleep 1; echo waiting' through the Bash tool "
             "while waiting on a background acquisition; read no file, wrote "
             "no file, and no result depends on it"},
)

# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #
REQUIRED_VERDICT_KEYS = (
    "QUALIFIED_ALPHA_RESULT", "FROZEN_R44_RULE_NATIVE_REPLICATION_RESULT",
    "BEST_CANDIDATE", "BEST_INSTRUMENT", "BEST_EVENT_FAMILY", "BEST_HORIZON",
    "BEST_ECONOMIC_EXPRESSION", "BEST_MODEL", "BEST_NET_BPS_PER_EVENT",
    "BEST_NET_T", "BEST_HIT_RATE", "BEST_CAPACITY",
    "RATES_REPLICATION_RESULT", "EQUITY_INDEX_REPLICATION_RESULT",
    "GOLD_REPLICATION_RESULT", "FX_REPLICATION_RESULT",
    "RELATIVE_VALUE_RESULT", "EVENT_CAUSALITY_RESULT", "PLACEBO_RESULT",
    "TIMING_PERTURBATION_RESULT", "COST_STRESS_RESULT", "LATENCY_RESULT",
    "SEARCH_ADJUSTED_RESULT", "FORWARD_SHADOWS_ADDED",
    "GLOBAL_SEARCH_BURDEN", "NEW_R45_EFFECTIVE_TRIALS",
    "OPTIONS_PROGRESS", "ANALYST_REVISION_PROGRESS",
    "NATIVE_FUTURES_DATA_RESULT", "TOP_DATA_PURCHASE_RECOMMENDATION",
    "EXACT_PRICE_IF_ANY", "ACCOUNT_REQUIRED", "PAYMENT_REQUIRED",
    "EXTERNAL_BLOCKERS", "MONEY_SPENT", "NEW_ACCOUNTS", "LICENCES_ACCEPTED",
    "OPERATIONAL_WRITES", "PORTFOLIO_MUTATIONS", "ORDERS",
    "MODEL_PROMOTIONS", "SCHEDULER_CHANGES", "SHELL_POLICY_VIOLATION",
)

TERMINAL_STATES = (
    "R45_QUALIFIED_EVENT_ALPHA_FOUND",
    "R45_NATIVE_FUTURES_EVENT_ALPHA_CANDIDATE_FOUND",
    "R45_EVENT_RELATIVE_VALUE_ALPHA_CANDIDATE_FOUND",
    "R45_STRONG_EVENT_CANDIDATE_FORWARD_PENDING",
    "R45_GOLD_SPECIFIC_EFFECT_NOT_GENERAL_MACRO_ALPHA",
    "R45_R44_MACRO_EFFECT_REFUTED_IN_NATIVE_MARKETS",
    "R45_NATIVE_FUTURES_DATA_WALL_BINDING",
    "R45_NO_QUALIFIED_EVENT_ALPHA",
)


# --------------------------------------------------------------------------- #
def _stable(obj):
    if isinstance(obj, dict):
        return {k: _stable(obj[k]) for k in sorted(obj)}
    if isinstance(obj, (list, tuple)):
        return [_stable(v) for v in obj]
    return obj


def frozen_contract() -> dict:
    """The declaration, plus a hash of it, for the artifact record."""
    body = {
        "schema": "r45_frozen_contract/1",
        "release": RELEASE, "campaign_id": CAMPAIGN_ID,
        "calculation_owner": CALCULATION_OWNER,
        "objective": OBJECTIVE,
        "frozen_rule": FROZEN_RULE,
        "r44_zone_a_reference": R44_ZONE_A_REFERENCE,
        "r44_cross_instrument": R44_CROSS_INSTRUMENT,
        "no_parameter_search_before_first_replication":
            NO_PARAMETER_SEARCH_BEFORE_FIRST_REPLICATION,
        "macro_release_times_et": MACRO_RELEASE_TIMES_ET,
        "release_times_are_a_declared_constant":
            MACRO_RELEASE_TIMES_ARE_A_DECLARED_CONSTANT,
        "instrument_class": INSTRUMENT_CLASS,
        "owned_minute_instruments": OWNED_MINUTE_INSTRUMENTS,
        "listed_minute_instruments": LISTED_MINUTE_INSTRUMENTS,
        "native_futures_instruments": NATIVE_FUTURES_INSTRUMENTS,
        "no_cfd_proxy_for_a_futures_hypothesis":
            NO_CFD_PROXY_FOR_A_FUTURES_HYPOTHESIS,
        "no_etf_proxy_for_a_futures_hypothesis":
            NO_ETF_PROXY_FOR_A_FUTURES_HYPOTHESIS,
        "cost_source_observed": COST_SOURCE_OBSERVED,
        "cost_source_estimated": COST_SOURCE_ESTIMATED,
        "slippage_bps_per_side": SLIPPAGE_BPS_PER_SIDE,
        "estimated_half_spread_floor_bps": ESTIMATED_HALF_SPREAD_FLOOR_BPS,
        "cost_stress_multipliers": list(COST_STRESS_MULTIPLIERS),
        "latency_stress_extra_minutes": list(LATENCY_STRESS_EXTRA_MINUTES),
        "lanes": LANES,
        "replication_lanes_first": list(REPLICATION_LANES_FIRST),
        "min_events_to_judge_replication": MIN_EVENTS_TO_JUDGE_REPLICATION,
        "min_events_to_qualify": MIN_EVENTS_TO_QUALIFY,
        "replication_net_t_min": REPLICATION_NET_T_MIN,
        "tolerance_scales_with_bar_interval":
            TOLERANCE_SCALES_WITH_BAR_INTERVAL,
        "qualification": QUALIFICATION,
        "cluster_inference_by": CLUSTER_INFERENCE_BY,
        "inherited_global_burden": INHERITED_GLOBAL_BURDEN,
        "inherited_global_burden_conservative":
            INHERITED_GLOBAL_BURDEN_CONSERVATIVE,
        "burden_families": list(BURDEN_FAMILIES),
        "authorized_spend_usd": AUTHORIZED_SPEND_USD,
        "forbidden": list(FORBIDDEN),
        "blocker_vocab": list(BLOCKER_VOCAB),
        "pit_checks": list(PIT_CHECKS),
        "freeze_requires": list(FREEZE_REQUIRES),
        "max_new_shadows": MAX_NEW_SHADOWS,
        "shell_policy": SHELL_POLICY,
        "inherited_shell_disclosures": list(INHERITED_SHELL_DISCLOSURES),
        "terminal_states": list(TERMINAL_STATES),
    }
    body["contract_hash"] = hashlib.sha256(
        json.dumps(_stable(body), sort_keys=True,
                   default=str).encode("utf-8")).hexdigest()[:16]
    return body
