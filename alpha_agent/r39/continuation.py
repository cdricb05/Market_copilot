"""alpha_agent.r39.continuation - the Release-39 CONTINUATION contract (v2).

Release 39 v1 ended with a near miss (the WIDE machine book) and a list of
executable work it had not run: four owned-but-unconsumed information
families, twelve DATA_AVAILABLE_BUT_NOT_TESTED cells, unexecuted $0 model
families, and no prospective evidence stream. The operator ordered the
campaign CONTINUED - not closed, not relaunched with a clean denominator.

This module is the continuation's frozen contract. Everything here is
declared BEFORE any continuation outcome is viewed:

* a NEW immutable campaign id (v1 artifacts stay byte-identical, valid, and
  citable; nothing is superseded except narrative text corrected elsewhere);
* the search burden NEVER resets: the v2 Zone-B reuse ledger is INITIALISED
  from the v1 ledger's 107 distinct candidates, every continuation
  evaluation adds to it, and the deflated-Sharpe denominator is the
  CUMULATIVE count (``BURDEN_NEVER_RESETS``);
* the evidence rules of v1 stay in force verbatim: no redesign against the
  already-accessed Zone C, Zone C stays ``HISTORICAL_CONFIRMATION_EVIDENCE``,
  diagnostics of already-executed Zone-C streams (factor residualisation,
  group kill tests, fixed-coefficient attribution) may not upgrade
  qualification;
* a DECLARED Zone-C pre-gate for v2: a continuation candidate may be frozen
  as a finalist only if its Zone-B after-cost excess t-statistic clears
  ``ZONE_C_PREGATE_MIN_ZONE_B_T`` - at the cumulative trial count, anything
  weaker cannot clear the DSR bar even if its Zone-C draw were perfect, so
  executing it would burn confirmation budget for arithmetic-certain FAIL;
* qualification is STRICTER than v1 (Track K): every v1 condition, plus
  positive residual alpha after the known-premia factor model.

Commercial and production refusals are inherited from :mod:`contract`
unchanged: $0, no accounts, no trials, no operational writes, no promotion,
no scheduler change. ``MAY_DOWNLOAD_MODEL_WEIGHTS`` REMAINS ``False`` - the
deep-sequence lane executed in this continuation trains small nets FROM
RANDOM INITIALISATION with a CPU-only torch build (BSD-3-Clause, installed
to the research drive); no pretrained weights enter the estate.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from .. import r39
from . import contract as C
from . import target_factory as TF
from . import universal_state as US
from . import zones

CALCULATION_OWNER = "alpha_agent.r39.continuation"

CONTINUATION_CAMPAIGN_ID = "r39_universal_alpha_continuation_v2"
V1_CAMPAIGN_ID = C.CAMPAIGN_ID
V1_ARTIFACTS_REMAIN_VALID = True
V1_ARTIFACTS_REMAIN_IMMUTABLE = True

# --------------------------------------------------------------------------- #
# Search burden - cumulative, never laundered
# --------------------------------------------------------------------------- #
BURDEN_NEVER_RESETS = True
NO_CAMPAIGN_ID_LAUNDERING = True
V1_EFFECTIVE_TRIALS_EXPECTED = 107
INHERITED_STAGE_MARKER = "V1_INHERITED"

# --------------------------------------------------------------------------- #
# Evidence rules carried forward, verbatim in force
# --------------------------------------------------------------------------- #
NO_REDESIGN_AGAINST_ACCESSED_ZONE_C = True
ZONE_C_DIAGNOSTICS_CANNOT_UPGRADE_QUALIFICATION = True
ZONE_C_LABEL_UNCHANGED = C.ZONE_C_EVIDENCE_LABEL

#: Declared BEFORE any v2 outcome: the Zone-C pre-gate. With the cumulative
#: trial count already >= 107, the DSR-implied qualification bar sits above
#: any Zone-B t-statistic materially below this; freezing weaker candidates
#: would spend single-execution confirmation budget on certain failures.
ZONE_C_PREGATE_MIN_ZONE_B_T = 3.0

#: Track K - the continuation's stricter qualification: all v1 conditions
#: (BH survivor, DSR >= 0.95 at the CUMULATIVE trial count, sign stability,
#: 2x cost stress, no concentration flip, positive excess) PLUS a positive
#: residual alpha after the known-premia factor model at |t| >= this.
QUALIFICATION_ADDS_RESIDUAL_ALPHA = True
RESIDUAL_ALPHA_T_MIN = 2.0
HISTORICAL_ALPHA_CANDIDATE_IS_NOT_PROVEN_ALPHA = True

# --------------------------------------------------------------------------- #
# Continuation compute surface - $0, from-scratch training only
# --------------------------------------------------------------------------- #
#: Free, licence-compatible open-source PACKAGES installed for the
#: continuation (the Track-16 rule that admitted scikit-learn et al. in v1).
#: torch is installed CPU-only to the RESEARCH DRIVE (D:), not the venv
#: drive, so the v1 disk constraint is solved rather than ignored.
PACKAGES_INSTALLED_FOR_CONTINUATION = (
    ("torch (CPU-only wheel, --target on the research drive)",
     "BSD-3-Clause"),)
TORCH_LIB_DIR = Path(r"D:\Stock_Prediction_app_data\universal_alpha_r39"
                     r"\_torch_cpu_lib")

#: Deep models in this continuation are trained FROM RANDOM INITIALISATION.
#: No pretrained weights are downloaded; the v1 contract flag stands.
MAY_DOWNLOAD_MODEL_WEIGHTS_STILL_FALSE = True
DEEP_MODELS_TRAINED_FROM_SCRATCH = True

#: Foundation-model branches that remain blocked, each for a NAMED reason
#: (not silence). These stay in the compute-escalation request.
FOUNDATION_MODEL_BLOCKERS = {
    "TABULAR_FOUNDATION (TabPFN-v2/TabICL-class)":
        "weights are gated behind a provider account and a custom licence "
        "acceptance; MAY_CREATE_PROVIDER_ACCOUNT and "
        "MAY_ACCEPT_LICENCE_AGREEMENT are False",
    "FOUNDATION_TIME_SERIES (Chronos/TimesFM-class zero-shot)":
        "pretraining corpora plausibly contain the same public financial "
        "series (rates, index levels); zero-shot output on this estate "
        "cannot be defended as out-of-sample, so the evidence could never "
        "qualify - and the transformers dependency stack does not fit the "
        "venv drive",
    "VISION_CHART (pretrained image encoders)":
        "the testable hypothesis is that PRETRAINED visual features "
        "transfer to charts; a random-init toy CNN tests a different, "
        "weaker hypothesis - deferred with the escalation request, with "
        "the market-structure family carrying the geometric hypothesis "
        "numerically",
}

# --------------------------------------------------------------------------- #
# Shell policy - the v1 attestation, audited instead of asserted
# --------------------------------------------------------------------------- #
WINDOWS_POWERSHELL_ONLY = True

#: The operator asserted the v1 transcript contains Bash-tool invocations
#: and that the v1 handoff attestation is therefore false. The transcript
#: was audited before any continuation work. Both the assertion and the
#: audit result are recorded; neither is rewritten to match the other.
SHELL_POLICY_AUDIT = {
    "operator_assertion": "the prior transcript contains Bash-tool "
                          "invocations; the v1 attestation is false",
    "transcript": r"C:\Users\binis\.claude\projects"
                  r"\c--Users-binis-paper-trader"
                  r"\c0ca2206-ba81-4d07-b25b-080b2bef0bf3.jsonl",
    "audit_method": "ripgrep over the full session transcript",
    "tool_invocation_pattern": "\"name\":\\s*\"Bash\"",
    "tool_invocation_matches": 0,
    "prose_token_pattern": "Bash",
    "prose_token_matches": "29 occurrences, every one inside prose: the "
                           "operator's own policy text, a skill "
                           "description, the validate.ps1 comment block, "
                           "the final report sentence, the compaction "
                           "summary, and the continuation directive "
                           "itself",
    "audit_verdict": "NO_BASH_TOOL_INVOCATION_FOUND_IN_TRANSCRIPT",
    "v1_violation_state": "DISPUTED_BY_TRANSCRIPT_AUDIT - not confirmed; "
                          "the v1 handoff's defect was CERTIFYING "
                          "compliance by self-declaration, and the "
                          "superseding validator replaces the declaration "
                          "with this evidence record",
    "continuation_rule": "WINDOWS POWERSHELL ONLY; any Bash/WSL/Git-Bash "
                         "use fails the handoff closed",
}

# --------------------------------------------------------------------------- #
# Ledger inheritance
# --------------------------------------------------------------------------- #
def inherit_reuse_ledger(campaign_id: str = CONTINUATION_CAMPAIGN_ID,
                         v1_campaign_id: str = V1_CAMPAIGN_ID) -> dict:
    """Initialise the v2 Zone-B reuse ledger FROM the v1 ledger.

    Every v1 candidate stays in the denominator with an explicit inherited
    marker; the call is idempotent so re-running a phase cannot double-count
    the inheritance. Returns the cumulative summary.
    """
    v2_path = r39.campaign_dir(campaign_id) / zones.REUSE_NAME
    if not v2_path.exists():
        v1 = r39.read_json(r39.campaign_dir(v1_campaign_id)
                           / zones.REUSE_NAME)
        if not v1:
            raise FileNotFoundError(
                "v1 reuse ledger is required to inherit the burden")
        if int(v1["distinct_candidates"]) != V1_EFFECTIVE_TRIALS_EXPECTED:
            raise ValueError(
                "v1 ledger holds %s distinct candidates; the continuation "
                "contract expected %d - refusing to guess"
                % (v1["distinct_candidates"], V1_EFFECTIVE_TRIALS_EXPECTED))
        evaluations = {}
        for cid, entry in v1["evaluations"].items():
            evaluations[cid] = {
                "count": int(entry["count"]),
                "stages": [INHERITED_STAGE_MARKER] +
                          [str(s) for s in entry.get("stages", [])]}
        body = {"contract": zones.REUSE_SCHEMA, "campaign_id": campaign_id,
                "calculation_owner": CALCULATION_OWNER,
                "inherited_from_campaign": v1_campaign_id,
                "inherited_distinct_candidates":
                    int(v1["distinct_candidates"]),
                "burden_never_resets": BURDEN_NEVER_RESETS,
                "evaluations": evaluations,
                "total_evaluations": int(v1["total_evaluations"]),
                "distinct_candidates": len(evaluations)}
        r39.campaign_dir(campaign_id).mkdir(parents=True, exist_ok=True)
        r39.write_json(v2_path, body, immutable=False)
    return cumulative_burden(campaign_id)


def cumulative_burden(campaign_id: str = CONTINUATION_CAMPAIGN_ID) -> dict:
    """The cumulative search burden: v1-inherited plus continuation trials."""
    led = r39.read_json(r39.campaign_dir(campaign_id) / zones.REUSE_NAME)
    if not led:
        raise FileNotFoundError("inherit_reuse_ledger() must run first")
    inherited = int(led.get("inherited_distinct_candidates") or 0)
    distinct = int(led["distinct_candidates"])
    return {
        "R39_V1_EFFECTIVE_TRIALS": inherited,
        "R39_CONTINUATION_NEW_TRIALS": distinct - inherited,
        "R39_CUMULATIVE_EFFECTIVE_TRIALS": distinct,
        "total_zone_b_evaluations": int(led["total_evaluations"]),
        "burden_never_resets": BURDEN_NEVER_RESETS,
    }


# --------------------------------------------------------------------------- #
# Frozen-state loading (byte-identical inputs, no rebuild drift)
# --------------------------------------------------------------------------- #
def load_frozen_state(v1_campaign_id: str = V1_CAMPAIGN_ID) -> dict:
    """Reload the v1 universal state from its FROZEN lane CSVs.

    The panels are read back from the hashed v1 artifacts (not rebuilt from
    providers), targets are re-materialised exactly as the v1 run did after
    persisting, and the macro overlay / daily layer are rebuilt from the
    same frozen source files the v1 contract fingerprinted. This is the
    reconstruction path Track A verifies against the frozen spec hash.
    """
    sdir = r39.state_dir(v1_campaign_id)
    lanes = {}
    for name in ("fut_monthly", "vx_weekly", "eq_monthly", "etf_monthly"):
        p = sdir / (name + ".csv")
        lanes[name] = pd.read_csv(p, parse_dates=["decision_date"]) \
            if p.exists() else pd.DataFrame()
    macro = US.build_macro_overlay()
    layer = US.load_daily_layer()
    fut = TF.materialise(lanes["fut_monthly"], scope_col="asset_class")
    vx = TF.materialise(lanes["vx_weekly"], scope_col="asset_class")
    eq = TF.materialise(lanes["eq_monthly"], scope_col="economic_group")
    etf = lanes["etf_monthly"]
    if etf is not None and not etf.empty:
        etf = TF.materialise(etf, scope_col="asset_class")
    return {"fut": fut, "vx": vx, "eq": eq, "etf": etf,
            "macro": macro, "layer": layer}
