"""alpha_agent.r46.contract - frozen BEFORE any Release-46 prediction exists.

Everything here is a DECLARATION made before a bar was read. The three that
decide whether this release is honest:

:data:`ENTRY_RULE`
    When a prediction's outcome window opens. Deliberately conservative: the
    NEXT calendar trading day after the emission's Eastern date, for every
    instrument, whatever its own session end. FX spot and CME futures still
    have hours left to trade when the US cash equity market closes; a rule
    that tried to exploit that would have to defend, per instrument and per
    day, that the mark it entered on was genuinely undetermined at emission.
    R46 declines the argument and gives the hours up. A few basis points of
    forgone edge is a cheap price for a rule an auditor can check with a
    calendar.

:data:`FORWARD_EVIDENCE_GATES`
    What it takes to be believed. Not "t > 2 once".

:data:`SEED_PARAMETERS_WERE_NOT_SEARCHED`
    The reason this release charges almost no search burden. Every seed
    challenger's parameters are textbook constants - 12-1 momentum, 5-day
    reversal, 60-day volatility, 252-day trend - fixed here, in this file,
    before :mod:`alpha_agent.r46.marketdata` was ever called. Nothing was
    swept on this estate's data to choose them. R45 measured what happens
    when you let a screen pick the cell: the winner moves every time you
    move the sample.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from . import CAMPAIGN_ID, RESEARCH_ROOT

RELEASE = "R46"
CALCULATION_OWNER = "alpha_agent.r46.contract"
ARTIFACT_DIR = RESEARCH_ROOT / CAMPAIGN_ID

OBJECTIVE = (
    "build and START a persistent prospective alpha tournament in which "
    "frozen, versioned challengers emit timestamped predictions BEFORE the "
    "market outcome exists, and are later scored against realised outcomes "
    "and the correct control, after costs, without retuning or backfill"
)

THE_QUESTION = (
    "what did the model predict before the market moved, and did that "
    "prediction actually make money after costs against the correct control?"
)

# --------------------------------------------------------------------------- #
# Evidence classes - never conflated, never silently mixed
# --------------------------------------------------------------------------- #
TRUE_FORWARD = "TRUE_FORWARD"
HISTORICAL_SIMULATION = "HISTORICAL_SIMULATION"

EVIDENCE_CLASSES = {
    TRUE_FORWARD: (
        "a prediction emitted strictly before its outcome window opened, "
        "from a specification frozen strictly before emission, using only "
        "features that existed at its declared data cutoff"),
    HISTORICAL_SIMULATION: (
        "any replay, backtest, reconstruction or re-scoring of dates that "
        "had already happened when the calculation ran - useful to NOMINATE "
        "a challenger, never to crown one"),
}

#: A prediction is TRUE_FORWARD only when every one of these holds.
TRUE_FORWARD_CONDITIONS = (
    "emitted_at_utc < outcome_window_start_utc",
    "every feature used existed by data_cutoff_utc",
    "data_cutoff_utc <= emitted_at_utc",
    "challenger spec_hash was registered before emitted_at_utc",
    "the outcome was unknown at emission",
)

FORBIDDEN_FOREVER = (
    "backdating a prediction",
    "reconstructing a prediction from current parameters",
    "labelling a retrospective calculation TRUE_FORWARD",
    "overwriting or revising a forecast after it was emitted",
    "rewriting a prediction after its outcome is known",
    "substituting a current snapshot into historical evidence",
    "silently retuning a losing challenger in place",
)

# --------------------------------------------------------------------------- #
# THE ENTRY RULE - frozen
# --------------------------------------------------------------------------- #
ENTRY_RULE = {
    "id": "R46_NEXT_TRADING_DAY_CLOSE",
    "statement": (
        "a prediction emitted at instant T enters at the CLOSE of the first "
        "trading day whose calendar date, in America/New_York, is strictly "
        "GREATER than the Eastern calendar date of T; its horizon is then "
        "measured in eligible sessions of that instrument's own realised bar "
        "calendar, counted from the entry close"),
    "why_conservative": (
        "at 16:05 ET the US cash equity session is closed but FX spot and "
        "several futures still have hours to run. Entering on any of those "
        "same-day closes would require a per-instrument, per-day argument "
        "that the mark was undetermined at emission. R46 gives those hours "
        "up so the rule can be checked with a calendar."),
    "eligible_session": (
        "a date on which the instrument actually printed a bar in the owned "
        "store - no holiday calendar is assumed, and no bar is interpolated"),
    "applies_to": "every instrument and every asset class, uniformly",
}

#: Horizons, in eligible sessions after the entry close.
HORIZONS = (1, 5, 20)

# --------------------------------------------------------------------------- #
# The immutable prediction record
# --------------------------------------------------------------------------- #
#: Every field a prospective prediction must carry to be prosecutable later.
#: :mod:`alpha_agent.r46.ledger` refuses to append a row missing any of them.
PREDICTION_RECORD_FIELDS = (
    "prediction_id", "batch_id",
    "challenger_id", "challenger_version", "challenger_spec_hash",
    "emitted_at_utc", "emitted_market_timestamp", "effective_as_of",
    "data_cutoff_utc", "data_cutoff_session",
    "asset_class", "instrument", "instrument_identity", "venue",
    "prediction_type", "horizon", "horizon_unit", "horizon_end_expected",
    "direction", "expected_return", "expected_residual_return",
    "probability", "confidence",
    "benchmark", "control", "hedge_definition",
    "expected_cost", "expected_financing", "expected_slippage",
    "expected_net_return",
    "position_expression", "n_legs", "gross_notional", "max_notional",
    "model_id", "model_family", "model_parameters_hash",
    "feature_set_hash", "training_data_cutoff",
    "market_state_snapshot_hash", "input_evidence_hash",
    "point_in_time_status", "forward_evidence_type", "status", "provenance",
)

#: The idempotency key. The same tuple may never produce two prediction rows.
PREDICTION_IDENTITY_KEY = (
    "challenger_id", "challenger_version", "instrument", "effective_as_of",
    "horizon",
)

#: The idempotency key for an outcome row.
OUTCOME_IDENTITY_KEY = ("prediction_id",)

STATUS_PENDING = "PENDING"
STATUS_MATURED = "MATURED"
STATUS_SCORED = "SCORED"
STATUS_INVALIDATED = "INVALIDATED_DATA_ERROR"
PREDICTION_STATUSES = (STATUS_PENDING, STATUS_MATURED, STATUS_SCORED,
                       STATUS_INVALIDATED)

PIT_OK = "PIT_OK"
PIT_VIOLATION = "PIT_VIOLATION"
PIT_UNVERIFIABLE = "PIT_UNVERIFIABLE"

# --------------------------------------------------------------------------- #
# Challenger lifecycle states
# --------------------------------------------------------------------------- #
HISTORICAL_ONLY = "HISTORICAL_ONLY"
FORWARD_PENDING = "FORWARD_PENDING"
EARLY_FORWARD_EVIDENCE = "EARLY_FORWARD_EVIDENCE"
FORWARD_CANDIDATE = "FORWARD_CANDIDATE"
FORWARD_CONFIRMED = "FORWARD_CONFIRMED"
FORWARD_REJECTED = "FORWARD_REJECTED"
DATA_BLOCKED = "DATA_BLOCKED"

CHALLENGER_STATES = (HISTORICAL_ONLY, FORWARD_PENDING, EARLY_FORWARD_EVIDENCE,
                     FORWARD_CANDIDATE, FORWARD_CONFIRMED, FORWARD_REJECTED,
                     DATA_BLOCKED)

#: No state in this release means "proven". PROVEN_ALPHA is not a state a
#: challenger can reach here; FORWARD_CONFIRMED is the strongest, and it
#: still confers no capital and no promotion.
PROVEN_ALPHA_IS_NOT_A_STATE = True

# --------------------------------------------------------------------------- #
# Forward evidence gates
# --------------------------------------------------------------------------- #
#: Horizon-appropriate evidence requirements. ``min_effective`` counts
#: EFFECTIVE INDEPENDENT decisions, not raw rows: a 20-session horizon emitted
#: daily overlaps twenty ways and is discounted accordingly.
FORWARD_EVIDENCE_GATES = {
    "min_effective_independent": {1: 60, 5: 40, 20: 24},
    "min_calendar_days": {1: 90, 5: 180, 20: 365},
    "min_raw_matured": {1: 60, 5: 60, 20: 60},
    "min_net_edge_bps_per_decision": 1.0,
    "min_t_stat_net_vs_control": 2.5,
    "max_single_day_share_of_pnl": 0.35,
    "max_single_leg_share_of_pnl": 0.40,
    "require_positive_at_2x_costs": True,
    "require_same_sign_halves": True,
    "require_confidence_interval_excludes_zero": True,
    "require_no_pit_violation": True,
    "require_no_retune_since_freeze": True,
    "multiple_testing_control": "Benjamini-Hochberg over all registered "
                                "challenger-horizon cells, FDR 0.10",
}

#: A challenger is killed - permanently, at this version - when any holds.
FORWARD_REJECTION_RULES = {
    "min_raw_matured_before_rejection": 40,
    "reject_if_net_t_below": -2.0,
    "reject_if_net_edge_below_bps_after_min_raw": -1.0,
    "reject_on_pit_violation": True,
    "reject_on_spec_hash_mismatch": True,
}

EARLY_EVIDENCE_MIN_MATURED = 5
CANDIDATE_MIN_EFFECTIVE_SHARE = 0.5   # half the gate's effective evidence

# --------------------------------------------------------------------------- #
# Cost model - charged on TRADED NOTIONAL (Release 31's correction)
# --------------------------------------------------------------------------- #
#: One side of a round trip, in basis points of traded notional. Deliberately
#: conservative; a challenger that only works at optimistic costs is not a
#: challenger.
COST_BPS_PER_SIDE = {
    "US_EQUITY": 5.0,
    "US_ETF": 2.0,
    "EQUITY_INDEX_FUTURES": 1.0,
    "RATES_FUTURES": 0.75,
    "COMMODITY_FUTURES": 2.5,
    "FX_FUTURES": 1.0,
    "FX_SPOT": 1.5,
    "VOLATILITY_FUTURES": 12.0,
    "CRYPTO": 5.0,
}

#: Slippage allowance on top of the half-spread, same units, same base.
SLIPPAGE_BPS_PER_SIDE = 1.0

CONTROL_CASH = "CASH_COLLATERAL_AT_RISK_FREE"
CONTROL_BENCHMARK = "BENCHMARK_BUY_AND_HOLD"
CONTROLS = (CONTROL_CASH, CONTROL_BENCHMARK)

#: R42's lesson, promoted to a contract clause: a dollar-neutral book still
#: consumes collateral, and collateral earns the risk-free rate. Beating zero
#: is not beating cash.
COLLATERAL_IS_REMUNERATED = True
RISK_FREE_SERIES = "DGS3MO"
RISK_FREE_SOURCE = "FRED (free, owned key, no purchase)"
RISK_FREE_FALLBACK_ANNUAL = None   # no silent constant; a miss is disclosed

# --------------------------------------------------------------------------- #
# Seed parameterisation - declared, not searched
# --------------------------------------------------------------------------- #
SEED_PARAMETERS_WERE_NOT_SEARCHED = {
    "statement": (
        "every seed challenger's parameters are canonical constants from the "
        "published asset-pricing literature, written into this contract "
        "before alpha_agent.r46.marketdata was first called. No sweep, no "
        "screen, no ranking on this estate's data selected any of them."),
    "consequence_for_burden": (
        "R46 charges ZERO new historical search trials for the seed cohort. "
        "A trial is charged when a release CHOOSES a parameter by looking at "
        "data; R46 chose none."),
    "canonical_constants": {
        "momentum_formation_days": 252,
        "momentum_skip_days": 21,
        "reversal_days": 5,
        "volatility_days": 60,
        "trend_days": 252,
        "beta_days": 60,
        "decile_fraction": 0.10,
        "trend_filter_days": 200,
        "spread_z_days": 60,
    },
    "why_it_matters": (
        "R45 re-ran R44's 60-cell screen separately on three event zones and "
        "got a different winner every time, the last one LARGER than the "
        "published headline. A maximum found by screening always looks "
        "locally peaked. The only defence is to not screen."),
}

#: Choosing a threshold, a parameter or a challenger AFTER seeing forward
#: results is forward p-hacking, and it is charged to the prospective ledger
#: as a decision, not hidden.
FORWARD_SELECTION_MUST_BE_RECORDED = True

# --------------------------------------------------------------------------- #
# Prior-release prospective shadows - adopted BY REFERENCE, never mutated
# --------------------------------------------------------------------------- #
ADOPTED_REGISTRY_SOURCES = (
    {"release": "R39", "campaign_id": "r39_universal_alpha_continuation_v2",
     "path": r"D:\Stock_Prediction_app_data\universal_alpha_r39"
             r"\r39_universal_alpha_continuation_v2"
             r"\research_shadow_registry.json",
     "owner": "alpha_agent.r39.research_shadow"},
    {"release": "R40", "campaign_id": "r40_prospective_alpha_acceleration_v1",
     "path": r"D:\Stock_Prediction_app_data\prospective_alpha_r40"
             r"\r40_prospective_alpha_acceleration_v1"
             r"\shadow_registry_v2.json",
     "owner": "alpha_agent.r40.shadow_registry"},
    {"release": "R41", "campaign_id": "r41_multi_horizon_alpha_breakthrough_v1",
     "path": r"D:\Stock_Prediction_app_data\multi_horizon_alpha_r41"
             r"\r41_multi_horizon_alpha_breakthrough_v1"
             r"\r41_shadow_registry.json",
     "owner": "alpha_agent.r41.forward_freeze"},
    {"release": "R42", "campaign_id": "r42_crypto_basis_alpha_validation_v1",
     "path": r"D:\Stock_Prediction_app_data\crypto_basis_r42"
             r"\r42_crypto_basis_alpha_validation_v1"
             r"\r42_shadow_registry.json",
     "owner": "alpha_agent.r42.forward"},
    {"release": "R43", "campaign_id": "r43_global_alpha_offensive_v1",
     "path": r"D:\Stock_Prediction_app_data\global_alpha_offensive_r43"
             r"\r43_global_alpha_offensive_v1\r43_shadow_registry.json",
     "owner": "alpha_agent.r43.frontier"},
    {"release": "R45", "campaign_id": "r45_macro_event_alpha_v1",
     "path": r"D:\Stock_Prediction_app_data\macro_event_alpha_r45"
             r"\r45_macro_event_alpha_v1\R45_SHADOW_REGISTRY.json",
     "owner": "alpha_agent.r45.frontier"},
)

ADOPTION_RULES = {
    "prior_registries_are_read_only": True,
    "prior_registry_bytes_must_be_unchanged": True,
    "adopted_shadows_keep_their_own_spec_hash": True,
    "adopted_shadows_keep_their_own_freeze_timestamp": True,
    "adopted_shadows_keep_their_own_ledger_owner": True,
    "r46_never_writes_a_forward_row_for_an_adopted_shadow": True,
    "reason": (
        "adoption exists so ONE leaderboard can show an operator that seven "
        "frozen shadows hold zero forward observations. It does not move "
        "their evidence, their ledgers or their ownership into R46."),
}

# --------------------------------------------------------------------------- #
# Feasibility - the gate R42 discovered and nobody enforced
# --------------------------------------------------------------------------- #
FEASIBILITY_STATES = ("CAN_ACCRUE", "DATA_STALE", "NO_DATA", "VENUE_BLOCKED",
                      "NOT_PROBED")

FEASIBILITY_RULE = {
    "statement": (
        "a challenger may not be registered ACTIVE unless its declared data "
        "path was probed in this run and demonstrably carries an observation "
        "recent enough to decide on"),
    "max_data_lag_sessions": 3,
    "why": (
        "R42 wrote down that the R41 BTC shadow could not accrue - monthly "
        "archive, 24-day lag, venue HTTP 451 - and the shadow stayed nominally "
        "live for three releases producing nothing. A registry that cannot "
        "tell an operator a stream is dead is worse than no registry."),
}

# --------------------------------------------------------------------------- #
# Relation to the portfolio manager
# --------------------------------------------------------------------------- #
PORTFOLIO_BOUNDARY = {
    "FORWARD_CANDIDATE_is_an_order": False,
    "FORWARD_CONFIRMED_is_an_automatic_holding": False,
    "tournament_may_expose_to_the_opportunity_frontier": (
        "candidate expected return, expected residual return, confidence, "
        "forward evidence state, risk characteristics"),
    "who_still_decides": (
        "the canonical portfolio manager, manually, subject to risk, "
        "concentration, switching cost, liquidity, settlement, capital and "
        "governance"),
    "manual_review_remains_mandatory": True,
}

# --------------------------------------------------------------------------- #
# Failure escalation (section 23)
# --------------------------------------------------------------------------- #
INFORMATION_SET_INSUFFICIENT = "INFORMATION_SET_INSUFFICIENT"
ESCALATION_RULE = {
    "trigger": (
        "a broad prospective tournament accumulates enough independent "
        "forward evidence across economically distinct families and none "
        "produces implementable residual alpha"),
    "min_families_for_escalation": 6,
    "min_effective_evidence_for_escalation": 200,
    "response": INFORMATION_SET_INSUFFICIENT,
    "forbidden_response": (
        "automatically running a bigger generic model search over the same "
        "information"),
    "required_response": (
        "rank the highest-value ORTHOGONAL information sources and price "
        "them per effective independent observation unlocked"),
}

# --------------------------------------------------------------------------- #
# Safety
# --------------------------------------------------------------------------- #
SAFETY_BLOCK = {
    "safety": ["RESEARCH ONLY", "READ ONLY", "PREVIEW ONLY",
               "NO LIVE BROKER ORDERS", "AUTOMATION OFF", "MANUAL REVIEW",
               "NO OPERATIONAL WRITE", "NO MODEL PROMOTION",
               "NO SLEEVE ACTIVATION", "NO PORTFOLIO ACTIVATION",
               "NO PURCHASE", "NO RENEWAL", "NO TRIAL", "NO NEW ACCOUNT",
               "NO SUBSCRIPTION CHANGE", "NO SCHEDULER CHANGE",
               "NO BACKDATED FORWARD ROW", "CREATES NO SIGNALS",
               "CREATES NO TRADE DECISIONS"],
    "accepts_licence_agreement": False,
    "activates_portfolio": False,
    "activates_sleeve": False,
    "automatic_promotion_allowed": False,
    "automatic_sleeve_activation_allowed": False,
    "backdates_forward_rows": False,
    "changes_scheduler": False,
    "changes_subscription_tier": False,
    "creates_capital_allocation": False,
    "creates_decision": False,
    "creates_order": False,
    "creates_paper_order": False,
    "creates_portfolio_target": False,
    "creates_proposal": False,
    "creates_provider_account": False,
    "creates_signal_authority": False,
    "enables_automation": False,
    "integrates_broker": False,
    "may_mutate_production": False,
    "may_spend_money": False,
    "mutates_cash": False,
    "mutates_holdings": False,
    "mutates_prior_release_artifacts": False,
    "promotes_model": False,
    "purchases_data": False,
    "restarts_production": False,
    "starts_provider_trial": False,
    "submits_payment_details": False,
    "trains_a_model": False,
    "writes_operational_store": False,
}

# --------------------------------------------------------------------------- #
# Shell policy
# --------------------------------------------------------------------------- #
SHELL_POLICY = "WINDOWS_POWERSHELL_ONLY"
FORBIDDEN_SHELLS = ("bash", "wsl", "git-bash", "sh", "zsh", "dash",
                    "posix shell wrapper")
SHELL_POLICY_WAIVERS_ARE_NOT_AVAILABLE = True

#: Prior releases' disclosed events. HISTORY - never rewritten, never erased.
INHERITED_SHELL_DISCLOSURES = (
    {"release": "R42", "events": 1,
     "note": "one disclosed Bash event, no waiver offered"},
    {"release": "R44", "events": 1,
     "note": "one disclosed Bash event; the clean-recovery handoff re-verified "
             "every artifact hash three ways and offered no waiver"},
    {"release": "R45", "events": 0,
     "note": "zero; the session was instructed by its harness to prefer a "
             "POSIX shell and declined"},
)

# --------------------------------------------------------------------------- #
# Terminal states
# --------------------------------------------------------------------------- #
TERMINAL_STATES = (
    "R46_PROSPECTIVE_ALPHA_TOURNAMENT_LIVE",
    "R46_FORWARD_PREDICTIONS_EMITTED",
    "R46_FORWARD_EVIDENCE_ALREADY_MATURING",
    "R46_TOURNAMENT_READY_NEXT_FORWARD_WINDOW",
    "R46_NO_VALID_FORWARD_WINDOW_TODAY",
    "R46_BLOCKED_BY_AUTHORITATIVE_DATA_FRESHNESS",
    "R46_FORWARD_INFRASTRUCTURE_INCOMPLETE",
)

#: Never invented unless genuine R46 forward observations have matured AND
#: passed the declared gate.
ALPHA_CONFIRMED_REQUIRES_MATURED_FORWARD_EVIDENCE = True

# --------------------------------------------------------------------------- #
# Inherited burden
# --------------------------------------------------------------------------- #
INHERITED_GLOBAL_BURDEN = 353
INHERITED_GLOBAL_BURDEN_CONSERVATIVE = 355
INHERITED_BURDEN_SOURCE = (
    r"D:\Stock_Prediction_app_data\macro_event_alpha_r45"
    r"\r45_macro_event_alpha_v1\search_burden.json")
BURDEN_MAY_NEVER_BE_RESET = True

#: Prospective forward evidence is NOT a historical search trial and is
#: ledgered separately. The two may never be netted against each other.
PROSPECTIVE_EVIDENCE_IS_NOT_SEARCH_BURDEN = True


# --------------------------------------------------------------------------- #
def contract_body() -> dict:
    return {
        "schema": "r46_frozen_contract/1",
        "release": RELEASE,
        "campaign_id": CAMPAIGN_ID,
        "calculation_owner": CALCULATION_OWNER,
        "objective": OBJECTIVE,
        "the_question": THE_QUESTION,
        "evidence_classes": EVIDENCE_CLASSES,
        "true_forward_conditions": list(TRUE_FORWARD_CONDITIONS),
        "forbidden_forever": list(FORBIDDEN_FOREVER),
        "entry_rule": ENTRY_RULE,
        "horizons": list(HORIZONS),
        "prediction_record_fields": list(PREDICTION_RECORD_FIELDS),
        "prediction_identity_key": list(PREDICTION_IDENTITY_KEY),
        "outcome_identity_key": list(OUTCOME_IDENTITY_KEY),
        "prediction_statuses": list(PREDICTION_STATUSES),
        "challenger_states": list(CHALLENGER_STATES),
        "proven_alpha_is_not_a_state": PROVEN_ALPHA_IS_NOT_A_STATE,
        "forward_evidence_gates": FORWARD_EVIDENCE_GATES,
        "forward_rejection_rules": FORWARD_REJECTION_RULES,
        "cost_bps_per_side": COST_BPS_PER_SIDE,
        "slippage_bps_per_side": SLIPPAGE_BPS_PER_SIDE,
        "controls": list(CONTROLS),
        "collateral_is_remunerated": COLLATERAL_IS_REMUNERATED,
        "risk_free_series": RISK_FREE_SERIES,
        "risk_free_source": RISK_FREE_SOURCE,
        "seed_parameters_were_not_searched": SEED_PARAMETERS_WERE_NOT_SEARCHED,
        "forward_selection_must_be_recorded": FORWARD_SELECTION_MUST_BE_RECORDED,
        "adopted_registry_sources": [dict(a) for a in ADOPTED_REGISTRY_SOURCES],
        "adoption_rules": ADOPTION_RULES,
        "feasibility_states": list(FEASIBILITY_STATES),
        "feasibility_rule": FEASIBILITY_RULE,
        "portfolio_boundary": PORTFOLIO_BOUNDARY,
        "escalation_rule": ESCALATION_RULE,
        "safety_block": SAFETY_BLOCK,
        "shell_policy": SHELL_POLICY,
        "forbidden_shells": list(FORBIDDEN_SHELLS),
        "inherited_shell_disclosures": [dict(d) for d in
                                        INHERITED_SHELL_DISCLOSURES],
        "terminal_states": list(TERMINAL_STATES),
        "inherited_global_burden": INHERITED_GLOBAL_BURDEN,
        "inherited_global_burden_conservative":
            INHERITED_GLOBAL_BURDEN_CONSERVATIVE,
        "burden_may_never_be_reset": BURDEN_MAY_NEVER_BE_RESET,
        "prospective_evidence_is_not_search_burden":
            PROSPECTIVE_EVIDENCE_IS_NOT_SEARCH_BURDEN,
    }


def contract_hash() -> str:
    body = contract_body()
    blob = json.dumps(body, sort_keys=True, separators=(",", ":"),
                      default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def write(path: Path = None) -> dict:
    from . import write_json
    body = contract_body()
    body["contract_hash"] = contract_hash()
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    write_json(path or (ARTIFACT_DIR / "r46_frozen_contract.json"), body)
    return body
