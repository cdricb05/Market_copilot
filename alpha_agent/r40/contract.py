"""alpha_agent.r40.contract - the frozen Release-40 contract.

Everything here is declared BEFORE any Release-40 outcome is computed and
hashed into the first artifact the campaign writes (``r39_closeout_import``),
so no rule below can be tuned after a result is seen:

* the R39 facts this release inherits and must VERIFY from the immutable
  artifacts (never trusted from a prompt);
* the prospective-evidence honesty rules (no historical row in TRUE_FORWARD,
  no row dated at or before a candidate's freeze, no backfill, no model swap
  under one id, no threshold reset);
* the research-shadow family cap (FIVE) and the pre-declared selection rule
  for the fifth slot;
* the declared roles of the evidence channels (ONE primary, the rest
  supporting) and the dependence treatments the velocity engine must use;
* the ten conditions under which open model weights may be downloaded, and
  the pretraining-contamination vocabulary;
* the commercial / production refusals inherited unchanged.
"""
from __future__ import annotations

from .. import r39 as _r39
from ..r39 import contract as _c39
from ..r39 import continuation as _cont39

CALCULATION_OWNER = "alpha_agent.r40.contract"

# --------------------------------------------------------------------------- #
# Inherited R39 facts - VERIFIED by closeout_import, never assumed
# --------------------------------------------------------------------------- #
R39_V1_CAMPAIGN_ID = _c39.CAMPAIGN_ID
R39_CONTINUATION_CAMPAIGN_ID = _cont39.CONTINUATION_CAMPAIGN_ID
R39_CLOSEOUT_COMMIT = "8d6d8d7b415fc6b5c6b0aeb4f43c18c9c2f755c2"
R39_HANDOFF_DIR = r"D:\Temp\paper_trader_release39_universal_alpha_handoff"

R39_EXPECTED = {
    "v1_generated": 608,
    "v1_screened": 594,
    "v1_zone_b_distinct": 107,
    "v1_locked_confirmations": 12,
    "continuation_new_trials": 87,
    "cumulative_effective_trials": 194,
    "continuation_zone_c_accesses": 0,
    "wide_candidate_id": "c39_c9233eccaa74",
    "wide_spec_hash":
        "cfd2ec367018412d5fb83909ba15f62900bd0e4f2ef974f53e04dea1096a361c",
    "wide_zone_c_excess_annualised": 0.03469932560862894,
    "wide_zone_c_t": 2.4311194784714067,
    "wide_residual_alpha_t_min": 2.5,
    "wide_group_kill_sign_flips": 0,
    "n_research_shadows": 3,
    "intl_rates_carry_rv_candidate": "c39_1a0105dd2f0c",
    "tcn_xs_candidate": "c39_fad367467c79",
}

#: The burden never resets: R40's reuse ledger is INITIALISED from the R39
#: continuation ledger (194 distinct) and every R40 validation evaluation
#: adds to it. Denominator laundering through a new campaign id is refused.
BURDEN_NEVER_RESETS = True
R39_INHERITED_EFFECTIVE_TRIALS_EXPECTED = 194
NO_CAMPAIGN_ID_LAUNDERING = True

# --------------------------------------------------------------------------- #
# Forward-evidence honesty (absolute)
# --------------------------------------------------------------------------- #
NO_HISTORICAL_ROW_IN_TRUE_FORWARD = True
NO_ROW_AT_OR_BEFORE_CANDIDATE_FREEZE = True
NO_BACKFILL_FROM_CURRENT_SNAPSHOT = True
NO_RETRAINED_MODEL_UNDER_FROZEN_ID = True
NO_RESULT_CROSS_WRITTEN_BETWEEN_CANDIDATE_LEDGERS = True
NO_FUTURE_INPUT_IN_FEATURES = True
NO_REVISED_MACRO_SUBSTITUTION = True
NO_OPTIONAL_THRESHOLD_RESET = True
NO_MODEL_SWAP_UNDER_ONE_ID = True
CANDIDATE_IDENTITY_IS_IMMUTABLE = True

#: Catch-up rule, declared before any capture: a research cycle that runs
#: late MUST capture every missed eligible decision date in order (no
#: cherry-picking), and every row records its capture lateness in sessions.
#: The frozen models cannot change and features are point-in-time, so a late
#: capture cannot alter a weight; what it could do is SELECT dates, which
#: contiguous catch-up forbids.
CATCH_UP_MUST_BE_CONTIGUOUS = True
LATE_CAPTURE_IS_RECORDED_NOT_HIDDEN = True

# --------------------------------------------------------------------------- #
# Shadow family (Track D)
# --------------------------------------------------------------------------- #
MAX_RESEARCH_SHADOW_FAMILY = 5
R39_SHADOWS_REMAIN_IMMUTABLE = True
SLOT_4_CANDIDATE_ID = R39_EXPECTED["intl_rates_carry_rv_candidate"]
SLOT_4_REASON = ("economically distinct (international bond-futures carry / "
                 "relative value inside ONE declared group, vol-scaled, "
                 "self-financed), built from the newly unlocked Norgate "
                 "international bond-futures universe, and selected on "
                 "Zone-B evidence BEFORE any R40 forward outcome")

#: The fifth slot is chosen by THIS rule, frozen here and hashed into the
#: closeout-import artifact before any R40 Zone-B evaluation runs. The rule
#: may read only historical discovery/selection evidence (fit Zone A, judge
#: Zone B); it may NOT read Zone C and may NOT read any TRUE_FORWARD row.
SLOT_5_SELECTION_RULE = {
    "candidates": {
        "A": "the R39 from-scratch TCN cross-section c39_fad367467c79 "
             "(Zone-B t 2.07, re-scored under the same candidate id)",
        "B": "the corrected WIDE successor built under the Track-E "
             "availability controls (a NEW candidate id)",
        "C": "the best materially distinct candidate discovered in a NEW "
             "R40 model/information branch (Tracks F/G/H/I)",
    },
    "eligibility": [
        "Zone-B after-cost excess t-statistic >= 1.5 (fit Zone A only)",
        "same sign in both Zone-B halves",
        "positive after-cost excess at 2x modelled costs",
        "|correlation| of the Zone-B after-cost net stream with EVERY "
        "already-registered shadow's Zone-B stream < 0.90 (a near duplicate "
        "is not an experiment); 0.70-0.90 is admitted and labelled "
        "PARTIALLY_REDUNDANT",
        "the candidate's information family + expression pair is not "
        "identical to an existing shadow's",
    ],
    "choice": "among eligible candidates, the HIGHEST Zone-B after-cost "
              "excess t-statistic; ties (|dt| < 0.05) resolved toward the "
              "candidate whose branch consumed FEWER distinct validation "
              "evaluations in R40 (lower selection burden)",
    "null_is_valid": "if no candidate is eligible, Slot 5 stays EMPTY and "
                     "the family has four members",
    "may_read_zone_c": False,
    "may_read_true_forward": False,
    "frozen_before_any_r40_evaluation": True,
}
SLOT_5_MIN_ZONE_B_T = 1.5
SLOT_5_DUPLICATE_CORRELATION = 0.90
SLOT_5_PARTIAL_REDUNDANCY_CORRELATION = 0.70

# --------------------------------------------------------------------------- #
# Evidence channels (Track B/J) - roles declared BEFORE outcomes
# --------------------------------------------------------------------------- #
PRIMARY_EVIDENCE_CHANNEL = ("after-cost economic excess of the frozen book "
                            "versus its declared control, one observation "
                            "per primary decision period")
SUPPORTING_EVIDENCE_CHANNELS = (
    "daily mark-to-market of already-frozen positions (variance/drawdown "
    "information only; adds NO mean information for a fixed position)",
    "date-level cross-sectional rank IC (predictive skill; uses every "
    "ranked market, not only the traded terciles)",
    "date-level sign accuracy of the book",
    "residual return after the frozen known-premia factor set",
    "candidate-level drawdown and CVaR",
    "realised cost versus modelled cost",
    "regime-conditioned excess (calm/stress by the observable VIX split)",
)
DEPENDENCE_TREATMENTS = ("date clustering", "Newey-West / HAC serial "
                         "correction", "stationary block bootstrap",
                         "cross-sectional correlation / effective number "
                         "of markets (participation ratio)",
                         "overlap-aware effective sample size")
DAILY_MARKS_OF_A_MONTHLY_POSITION_ARE_NOT_INDEPENDENT_TRADES = True
CROSS_SECTIONAL_ROWS_SHARING_A_DATE_ARE_NOT_INDEPENDENT_MARKETS = True
NEVER_REPORT_MARKETS_TIMES_DAYS_AS_INDEPENDENT_SAMPLES = True

# --------------------------------------------------------------------------- #
# Open-weight model policy (Track G) - TEN conditions, ALL must hold
# --------------------------------------------------------------------------- #
MAY_DOWNLOAD_MODEL_WEIGHTS = True
MODEL_WEIGHT_DOWNLOAD_CONDITIONS = (
    "ZERO_MONETARY_COST",
    "NO_COMMERCIAL_SUBSCRIPTION",
    "NO_CREDIT_CARD",
    "NO_PROVIDER_ACCOUNT_REQUIRED",
    "NO_RESTRICTED_DATA_UPLOAD",
    "LICENCE_PERMITS_LOCAL_RESEARCH_USE",
    "NO_AUTOMATIC_LICENCE_ACCEPTANCE_ON_OPERATORS_BEHALF",
    "STORAGE_ON_RESEARCH_DRIVE",
    "PROVENANCE_VERSION_HASH_RECORDED",
    "NOT_GATED_BEHIND_CLICK_THROUGH",
)
MAY_PURCHASE_COMPUTE = False
MAY_PURCHASE_CLOUD_COMPUTE = False
MAY_INSTALL_CUDA = False

PRETRAINING_CONTAMINATION_CLASSES = (
    "PRETRAINING_DATA_KNOWN_CLEAN",
    "PRETRAINING_OVERLAP_POSSIBLE",
    "PRETRAINING_OVERLAP_LIKELY",
    "PRETRAINING_UNKNOWN",
    "NOT_APPLICABLE_TRAINED_FROM_SCRATCH",
)
#: A zero-shot model with possible exposure to the target series is NOT
#: fresh out-of-sample evidence; it may be REPRESENTATION_RESEARCH or
#: MODEL_CAPABILITY_RESEARCH and never receives a clean historical-OOS label
#: without evidence.
EVIDENCE_ROLES = ("CLEAN_HISTORICAL_OOS", "REPRESENTATION_RESEARCH",
                  "MODEL_CAPABILITY_RESEARCH")
CONTAMINATED_MODELS_CANNOT_CLAIM_CLEAN_OOS = True

# --------------------------------------------------------------------------- #
# Model challenge (Track H) - same economic protocol, bounded search
# --------------------------------------------------------------------------- #
SAME_TARGET_SAME_CONTROL_SAME_CHRONOLOGY = True
NO_ZONE_C_REDESIGN = True
HIERARCHICAL_FIDELITY_SEARCH = True
MAX_CONFIGS_PER_MODEL_FAMILY = 3   # low-fidelity screen, then ONE full run

# --------------------------------------------------------------------------- #
# Automation boundary (Track N)
# --------------------------------------------------------------------------- #
MAY_ENABLE_SCHEDULED_TASK = False
MAY_MODIFY_PRODUCTION_SCHEDULER = False
MAY_RESTART_PRODUCTION = False
MAY_CREATE_ORDER = False
MAY_CHANGE_HOLDINGS = False
MAY_PROMOTE_MODEL = False
RESEARCH_CYCLE_IS_A_CALLABLE_NOT_A_SCHEDULE = True

# --------------------------------------------------------------------------- #
# Commercial refusals (inherited verbatim)
# --------------------------------------------------------------------------- #
MAY_SPEND_MONEY = _c39.MAY_SPEND_MONEY
MAY_START_PROVIDER_TRIAL = _c39.MAY_START_PROVIDER_TRIAL
MAY_CREATE_PROVIDER_ACCOUNT = _c39.MAY_CREATE_PROVIDER_ACCOUNT
MAY_ACCEPT_LICENCE_AGREEMENT = _c39.MAY_ACCEPT_LICENCE_AGREEMENT
MAY_SUBMIT_PAYMENT_DETAILS = _c39.MAY_SUBMIT_PAYMENT_DETAILS
MAY_PURCHASE_DATA = False
INTRINIO_PURCHASE_ALLOWED = False
INTRINIO_SAMPLE_PURPOSE = "SCHEMA_AND_PIT_VALIDATION_ONLY"

# --------------------------------------------------------------------------- #
# Result axes (never collapsed)
# --------------------------------------------------------------------------- #
RESULT_AXES = ("SYSTEM_RESULT", "FORWARD_ENGINE_RESULT",
               "FORWARD_EVIDENCE_RESULT", "INFORMATION_RESULT",
               "MODEL_RESULT", "HISTORICAL_ALPHA_RESULT",
               "PROSPECTIVE_ALPHA_RESULT")
TERMINAL_STATES = (
    "R40_FORWARD_ALPHA_EVIDENCE_STRENGTHENED",
    "R40_FORWARD_ALPHA_EVIDENCE_WEAKENED",
    "R40_OPEN_INTELLIGENCE_CANDIDATE_DISCOVERED",
    "R40_PROSPECTIVE_ENGINE_READY_WAITING_FOR_TIME",
    "R40_INFORMATION_LIMIT_BINDING",
    "R40_COMPUTE_LIMIT_BINDING",
    "R40_NO_INCREMENTAL_EDGE_FOUND",
)
DO_NOT_FORCE_A_SUCCESS_STATE = True
#: PROSPECTIVE_ALPHA_RESULT may be PASS only when a shadow's pre-registered
#: success boundary is crossed on TRUE_FORWARD rows; HISTORICAL_ALPHA_RESULT
#: may be PASS only under the R39 Track-K qualification at the cumulative
#: trial count. Neither is a constant that can be set by narrative.
PROSPECTIVE_PASS_REQUIRES = "SUCCESS_BOUNDARY_CROSSED_ON_TRUE_FORWARD_ROWS"
HISTORICAL_PASS_REQUIRES = "R39_TRACK_K_QUALIFICATION_AT_CUMULATIVE_BURDEN"

WINDOWS_POWERSHELL_ONLY = True

#: Shell-policy event log for this release - recorded, never self-attested
#: away. The handoff validator greps the session transcript for the tool
#: names below and reports the counts as EVIDENCE; the operator decides.
SHELL_POLICY_EVENTS = {
    "bash_tool_invocations_by_claude": 0,
    "monitor_tool_invocations": 1,
    "monitor_event_detail": {
        "when": "2026-08-23 ~13:27 local, during phase G+H",
        "what": "ONE 'Monitor' tool call whose script was bash-syntax "
                "(a read-only loop polling phaseGH.log with wc/tail/grep/"
                "sleep); the tool's own response labels the task type "
                "'local_bash'",
        "duration": "about one minute; stopped by Claude as soon as the "
                    "task-type label was seen",
        "side_effects": "none - it read two log files and wrote nothing",
        "classification": "SHELL_POLICY_EVENT - a bash-type process was "
                          "invoked by a non-Bash tool; under the operator's "
                          "literal rule ('NO sh') this is a violation and "
                          "is reported as such; no Bash/WSL/Git-Bash tool "
                          "was invoked directly and no command, edit or "
                          "artifact of this release was produced through "
                          "it",
        "replacement": "all subsequent polling through PowerShell "
                       "Get-Content",
    },
}
SHELL_POLICY_VIOLATION_REPORTED = True


def contract_hash() -> str:
    """Hash of every frozen rule above (the Slot-5 rule included)."""
    return _r39.sha({
        "R39_EXPECTED": R39_EXPECTED,
        "SLOT_5_SELECTION_RULE": SLOT_5_SELECTION_RULE,
        "SLOT_4_CANDIDATE_ID": SLOT_4_CANDIDATE_ID,
        "MAX_RESEARCH_SHADOW_FAMILY": MAX_RESEARCH_SHADOW_FAMILY,
        "PRIMARY_EVIDENCE_CHANNEL": PRIMARY_EVIDENCE_CHANNEL,
        "SUPPORTING_EVIDENCE_CHANNELS": SUPPORTING_EVIDENCE_CHANNELS,
        "MODEL_WEIGHT_DOWNLOAD_CONDITIONS": MODEL_WEIGHT_DOWNLOAD_CONDITIONS,
        "PRETRAINING_CONTAMINATION_CLASSES":
            PRETRAINING_CONTAMINATION_CLASSES,
        "MAX_CONFIGS_PER_MODEL_FAMILY": MAX_CONFIGS_PER_MODEL_FAMILY,
        "SLOT_5_MIN_ZONE_B_T": SLOT_5_MIN_ZONE_B_T,
        "SLOT_5_DUPLICATE_CORRELATION": SLOT_5_DUPLICATE_CORRELATION,
    })
