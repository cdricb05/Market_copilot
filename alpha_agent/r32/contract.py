"""alpha_agent.r32.contract - the ONE Release 32 campaign contract owner.

The contract is frozen BEFORE any sleeve result is observed and is immutable
afterwards. Every budget below is a NUMBER in this module, enforced by
:mod:`alpha_agent.r32.funnel`, not a sentence in a document. A campaign that may
widen its budget or relax its bar after seeing a disappointing sleeve is not
running an experiment, it is searching until it finds something.

If a material term changes, that is a NEW campaign with a NEW id, never an edit.
"""
from __future__ import annotations

import platform
import sys
from pathlib import Path
from typing import Optional

from .. import r32
from ..r31.contract import git_head  # ONE git-head owner, reused

CALCULATION_OWNER = "alpha_agent.r32.contract"
CONTRACT_SCHEMA = "r32_pnl_opportunity_frontier_contract/1"
ARTIFACT_NAME = "research_campaign_contract.json"

#: The campaign this release runs. A new id is the ONLY way to change a term.
CAMPAIGN_ID = "r32_pnl_opportunity_frontier_v4"

SUPERSEDED_EXPERIMENTAL_DESIGN = "SUPERSEDED_EXPERIMENTAL_DESIGN"
SUPERSEDED_INCOMPLETE_REPORTING = "SUPERSEDED_INCOMPLETE_REPORTING"

#: v1 is SUPERSEDED, not deleted. Its artifacts stay on disk as history and
#: cannot enter v2: the judge's behaviour hash is bound into every specification
#: hash, so a result measured under v1's judge can never appear in a v2
#: leaderboard, lockbox or multiple-testing denominator.
SUPERSEDED_CAMPAIGNS = {
    "r32_pnl_opportunity_frontier_v1": {
        "state": SUPERSEDED_EXPERIMENTAL_DESIGN,
        "produced_a_verdict": True,
        "verdict_withdrawn": "R32_SINGLE_SLEEVE_QUALIFIED",
        "defects": [
            "the primary selection and multiple-testing statistic was excess "
            "over CASH. Over a long window every strategy with equity exposure "
            "beats bills, so that statistic measures exposure, not skill: all "
            "ten v1 lockbox results had NEGATIVE excess over the benchmark "
            "while one was reported as qualified",
            "the EVENT_DRIVEN 'hold the index every session' CONTROL was "
            "allowed to become a lockbox finalist, where beating cash would "
            "have qualified the sleeve for rediscovering buy-and-hold",
            "the inherited Release-31 control verdict was read from a key that "
            "does not exist ('verdict' rather than 'primary_verdict'), so the "
            "equity-selection baseline entered the frontier as null",
        ],
        "correction": "v2 judges every candidate against a VOLATILITY-MATCHED "
                      "mix of the benchmark and cash, excludes controls from "
                      "finalist selection, and reads the inherited verdict by "
                      "its real key",
    },
    "r32_pnl_opportunity_frontier_v2": {
        "state": SUPERSEDED_INCOMPLETE_REPORTING,
        "produced_a_verdict": True,
        "verdict_reproduced_in_v3": "R32_ZERO_COST_OPPORTUNITY_FRONTIER_EXHAUSTED",
        "defects": [
            "the candidate registry stripped private keys before returning a "
            "recorded row, so the sleeve net return paths never reached the "
            "frontier. The correlation map and the latent risk clusters were "
            "therefore EMPTY, which reads as 'no sleeve is related to any "
            "other' rather than 'the relationship was never measured'",
        ],
        "correction": "v3 returns the return paths to the caller while keeping "
                      "them out of the frozen artifact",
        "judge_behaviour_unchanged": True,
        "measurements_must_reproduce_exactly": True,
        "note": "v2 measured correctly and reported incompletely. Because the "
                "judge behaviour hash is identical, every later per-candidate "
                "number must equal v2's - which is asserted rather than "
                "assumed, and is the reason this was superseded rather than "
                "quietly rewritten in place.",
    },
    "r32_pnl_opportunity_frontier_v3": {
        "state": SUPERSEDED_INCOMPLETE_REPORTING,
        "produced_a_verdict": True,
        "verdict_reproduced_in_v4": "R32_ZERO_COST_OPPORTUNITY_FRONTIER_EXHAUSTED",
        "defects": [
            "every panel generated its OWN decision calendar, so no two "
            "sleeves shared a single decision date. The cross-sleeve "
            "correlation map was therefore empty and the latent risk clusters "
            "were empty with it - which reads as 'no sleeve is related to any "
            "other' when the truth is that the relationship was never "
            "measurable. A ranking across sleeves measured on disjoint "
            "calendars compares eras, not strategies",
        ],
        "correction": "v4 derives a SHARED decision calendar from the sessions "
                      "every panel covers and re-scores each sleeve's finalist "
                      "on it, as a reporting-only view that cannot qualify "
                      "anything and does not enter the denominator",
        "judge_behaviour_unchanged": True,
        "measurements_must_reproduce_exactly": True,
    },
}

#: v1 evidence may be READ as history and may never influence v2.
SUPERSEDED_EVIDENCE_RULES = {
    "may_select_v2_hyperparameters": False,
    "may_select_v2_finalists": False,
    "may_influence_the_lockbox": False,
    "may_contribute_to_a_qualification_verdict": False,
    "may_reduce_the_multiple_testing_denominator": False,
    "is_preserved_on_disk": True,
}

# --------------------------------------------------------------------------- #
# The question
# --------------------------------------------------------------------------- #
QUESTION = (
    "If every investable dollar were cash right now, given everything "
    "legitimately observable right now, where should capital be deployed to "
    "maximise expected after-cost, risk-adjusted paper portfolio PnL?"
)

#: Three consequences that are binding, and are each enforced somewhere.
NOT_REQUIRED_TO_ALLOCATE_EVERYWHERE = True
NULL_RESULT_IS_VALID = True
CASH_IS_A_REAL_ASSET_CHOICE = True

# --------------------------------------------------------------------------- #
# The six sleeves
# --------------------------------------------------------------------------- #
SLEEVE_EQUITY_SELECTION = "EQUITY_SELECTION"
SLEEVE_EQUITY_BETA_TIMING = "EQUITY_BETA_TIMING"
SLEEVE_SECTOR_ROTATION = "SECTOR_ROTATION"
SLEEVE_CROSS_ASSET_TREND = "CROSS_ASSET_TREND"
SLEEVE_VOLATILITY_RISK_REGIME = "VOLATILITY_RISK_REGIME"
SLEEVE_EVENT_DRIVEN = "EVENT_DRIVEN"

SLEEVES = (
    SLEEVE_EQUITY_SELECTION,
    SLEEVE_EQUITY_BETA_TIMING,
    SLEEVE_SECTOR_ROTATION,
    SLEEVE_CROSS_ASSET_TREND,
    SLEEVE_VOLATILITY_RISK_REGIME,
    SLEEVE_EVENT_DRIVEN,
)

#: EQUITY_SELECTION is the CONTROL. It is not researched again: it carries
#: Release 31's frozen terminal evidence into the frontier. Release 31 proved
#: the binding constraint there is INFORMATION, not METHOD, so re-running the
#: same search over the same information would add data-mining risk and no
#: knowledge. Enforced by ``funnel.assert_control_not_researched``.
CONTROL_SLEEVE = SLEEVE_EQUITY_SELECTION
NEW_SLEEVES = tuple(s for s in SLEEVES if s != CONTROL_SLEEVE)

R31_CAMPAIGN_ID = "r31_mathematical_alpha_frontier_v3"
R31_VERDICT = "R31_CURRENT_INFORMATION_MODEL_FRONTIER_EXHAUSTED"
R31_DOMINANT_CONSTRAINT = "INFORMATION_NOT_METHOD"

# --------------------------------------------------------------------------- #
# Research funnel budgets - CEILINGS, not targets
# --------------------------------------------------------------------------- #
STAGE_SCREENING = "SCREENING"
STAGE_QUALIFICATION = "QUALIFICATION"
STAGE_NOVEL = "NOVEL_REFINEMENT"
STAGE_LOCKBOX = "LOCKBOX"
STAGES = (STAGE_SCREENING, STAGE_QUALIFICATION, STAGE_NOVEL, STAGE_LOCKBOX)

#: Screening: cheap, directional, DISCOVERY layer only.
SCREENING_MAX_PER_SLEEVE = 8

#: Qualification: at most 3 families x 8 configurations for each sleeve.
QUALIFICATION_MAX_FAMILIES_PER_SLEEVE = 3
QUALIFICATION_MAX_CONFIGS_PER_FAMILY = 8
QUALIFICATION_MAX_PER_SLEEVE = (QUALIFICATION_MAX_FAMILIES_PER_SLEEVE
                                * QUALIFICATION_MAX_CONFIGS_PER_FAMILY)   # 24
QUALIFICATION_MAX_TOTAL = 120

#: Novel / refinement: bounded depth, so a refinement cannot become a new search.
NOVEL_MAX_PER_SLEEVE = 12
NOVEL_MAX_TOTAL = 60
NOVEL_MAX_DEPTH = 2

#: Lockbox: two finalists per sleeve, ONE access each, no retuning afterwards.
LOCKBOX_MAX_FINALISTS_PER_SLEEVE = 2
LOCKBOX_MAX_FINALISTS_TOTAL = 12
LOCKBOX_MAX_ACCESSES_PER_FINALIST = 1
RETUNING_AFTER_LOCKBOX_ALLOWED = False

BUDGETS = {
    STAGE_SCREENING: {"max_per_sleeve": SCREENING_MAX_PER_SLEEVE,
                      "max_total": SCREENING_MAX_PER_SLEEVE * len(NEW_SLEEVES)},
    STAGE_QUALIFICATION: {
        "max_families_per_sleeve": QUALIFICATION_MAX_FAMILIES_PER_SLEEVE,
        "max_configs_per_family": QUALIFICATION_MAX_CONFIGS_PER_FAMILY,
        "max_per_sleeve": QUALIFICATION_MAX_PER_SLEEVE,
        "max_total": QUALIFICATION_MAX_TOTAL},
    STAGE_NOVEL: {"max_per_sleeve": NOVEL_MAX_PER_SLEEVE,
                  "max_total": NOVEL_MAX_TOTAL,
                  "max_depth": NOVEL_MAX_DEPTH},
    STAGE_LOCKBOX: {"max_finalists_per_sleeve": LOCKBOX_MAX_FINALISTS_PER_SLEEVE,
                    "max_finalists_total": LOCKBOX_MAX_FINALISTS_TOTAL,
                    "max_accesses_per_finalist": LOCKBOX_MAX_ACCESSES_PER_FINALIST,
                    "retuning_allowed": RETUNING_AFTER_LOCKBOX_ALLOWED},
}

# --------------------------------------------------------------------------- #
# Sample geometry
# --------------------------------------------------------------------------- #
#: Decision dates are struck every N trading sessions. Sleeves whose economics
#: are monthly do not get to claim daily independence.
STEP_SESSIONS = 21

#: Minimum trailing sessions before any feature is computed.
MIN_HISTORY = 252

#: A sleeve with fewer scored decision dates than this cannot carry a verdict:
#: it has not been observed often enough for a t-statistic to mean anything.
MIN_SCORED_DECISIONS = 60

#: Hold horizon in trading sessions for the common judge.
HOLD_SESSIONS = 21

# --------------------------------------------------------------------------- #
# Multiple testing
# --------------------------------------------------------------------------- #
FDR_Q = 0.10
SPA_BLOCK_MEAN = 10.0
SPA_RESAMPLES = 2000
BOOTSTRAP_SEED = 20320820

#: EVERY executed hypothesis enters the denominator, including the ones that
#: failed, were resource-infeasible, or were abandoned. A denominator that
#: counts only the survivors is not a correction, it is a second selection.
DENOMINATOR_COUNTS_ALL_EXECUTED = True

# --------------------------------------------------------------------------- #
# The common economic judge
# --------------------------------------------------------------------------- #
#: One-way transaction cost charged PER SIDE. Release 31 learned this the hard
#: way: a rebalance pays on the sell AND the buy, so the cost base is TRADED
#: NOTIONAL (sum |dw|), never one-way turnover (sum |dw| / 2).
COST_BASE = "TRADED_NOTIONAL"

#: Per-sleeve cost rates differ because the instruments differ. Declared here so
#: no sleeve can quietly award itself a cheaper market.
COST_RATE_PER_SIDE_BPS = {
    SLEEVE_EQUITY_SELECTION: 10.0,
    SLEEVE_EQUITY_BETA_TIMING: 3.0,
    SLEEVE_SECTOR_ROTATION: 5.0,
    SLEEVE_CROSS_ASSET_TREND: 5.0,
    SLEEVE_VOLATILITY_RISK_REGIME: 15.0,
    SLEEVE_EVENT_DRIVEN: 15.0,
}

#: The risk-free leg. Cash is a real asset choice, so it must earn something and
#: that something must be observable point-in-time. ``%IRX`` is the 13-week
#: T-bill yield, a market observable, quoted daily since 1960.
CASH_YIELD_SYMBOL = "%IRX"

#: The common benchmark every sleeve is measured against, plus cash.
BENCHMARK_TOTAL_RETURN = "$SPXTR"
BENCHMARK_CASH = "CASH"

#: A sleeve qualifies only if it beats BOTH, after cost, on the common overlap.
MUST_BEAT_CASH = True
MUST_BEAT_BENCHMARK_OR_ADD_MARGINAL_VALUE = True

# --------------------------------------------------------------------------- #
# Terminal verdicts
# --------------------------------------------------------------------------- #
VERDICT_MULTIPLE = "R32_MULTIPLE_SLEEVES_QUALIFIED"
VERDICT_SINGLE = "R32_SINGLE_SLEEVE_QUALIFIED"
VERDICT_EXHAUSTED = "R32_ZERO_COST_OPPORTUNITY_FRONTIER_EXHAUSTED"
VERDICT_PIT_BLOCKED = "R32_POINT_IN_TIME_EVIDENCE_BLOCKED"
VERDICT_BUDGET = "R32_RESOURCE_BUDGET_EXHAUSTED"
VERDICT_DATA_BLOCKED = "R32_DATA_SOURCE_BLOCKED"
PRIMARY_VERDICTS = (VERDICT_MULTIPLE, VERDICT_SINGLE, VERDICT_EXHAUSTED,
                    VERDICT_PIT_BLOCKED, VERDICT_BUDGET, VERDICT_DATA_BLOCKED)

SECONDARY_READY = "READY_FOR_R33_MULTI_ASSET_INTEGRATION"
SECONDARY_SAMPLE = "INFORMATION_SAMPLE_PRIORITY_IDENTIFIED"
SECONDARY_VERDICTS = (SECONDARY_READY, SECONDARY_SAMPLE)

# --------------------------------------------------------------------------- #
# Contract construction
# --------------------------------------------------------------------------- #
def build(*, campaign_id: str = CAMPAIGN_ID, created_at: str,
          repo: Optional[Path] = None) -> dict:
    """Build the immutable campaign contract body."""
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "created_at": created_at,
        "question": QUESTION,
        "binding_consequences": {
            "not_required_to_allocate_to_every_asset_class":
                NOT_REQUIRED_TO_ALLOCATE_EVERYWHERE,
            "null_result_is_valid": NULL_RESULT_IS_VALID,
            "cash_is_a_real_asset_choice": CASH_IS_A_REAL_ASSET_CHOICE,
        },
        "sleeves": list(SLEEVES),
        "control_sleeve": CONTROL_SLEEVE,
        "new_sleeves": list(NEW_SLEEVES),
        "superseded_campaigns": SUPERSEDED_CAMPAIGNS,
        "superseded_evidence_rules": SUPERSEDED_EVIDENCE_RULES,
        "inherited_release31": {
            "campaign_id": R31_CAMPAIGN_ID,
            "verdict": R31_VERDICT,
            "dominant_constraint": R31_DOMINANT_CONSTRAINT,
            "rerun": False,
            "reused_as_control": True,
        },
        "budgets": BUDGETS,
        "sample_geometry": {
            "step_sessions": STEP_SESSIONS,
            "min_history_sessions": MIN_HISTORY,
            "min_scored_decisions": MIN_SCORED_DECISIONS,
            "hold_sessions": HOLD_SESSIONS,
        },
        "multiple_testing": {
            "fdr_q": FDR_Q,
            "spa_block_mean": SPA_BLOCK_MEAN,
            "spa_resamples": SPA_RESAMPLES,
            "bootstrap_seed": BOOTSTRAP_SEED,
            "denominator_counts_all_executed": DENOMINATOR_COUNTS_ALL_EXECUTED,
        },
        "economics": {
            "cost_base": COST_BASE,
            "cost_rate_per_side_bps": dict(COST_RATE_PER_SIDE_BPS),
            "cash_yield_symbol": CASH_YIELD_SYMBOL,
            "benchmark_total_return": BENCHMARK_TOTAL_RETURN,
            "must_beat_cash": MUST_BEAT_CASH,
            "must_beat_benchmark_or_add_marginal_value":
                MUST_BEAT_BENCHMARK_OR_ADD_MARGINAL_VALUE,
        },
        "verdicts": {"primary": list(PRIMARY_VERDICTS),
                     "secondary": list(SECONDARY_VERDICTS)},
        "environment": {
            "git_head": git_head(repo),
            "python": sys.version.split()[0],
            "platform": platform.platform(),
        },
        "research_root": str(r32.research_root()),
    }
    body = r32.artifact_body(CONTRACT_SCHEMA, payload)
    body["contract_hash"] = r32.sha(
        {k: v for k, v in payload.items() if k != "environment"})
    return body


def path_for(campaign_id: str = CAMPAIGN_ID) -> Path:
    return r32.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(contract: dict) -> Path:
    return r32.write_json(path_for(contract["campaign_id"]), contract)


def load(campaign_id: str = CAMPAIGN_ID) -> Optional[dict]:
    return r32.read_json(path_for(campaign_id))


def verify(contract: dict) -> dict:
    """Recompute the contract hash and report drift."""
    payload = {k: v for k, v in contract.items()
               if k not in ("schema", "release", "safety_block",
                            "contract_hash", "environment")}
    recomputed = r32.sha(payload)
    return {"declared": contract.get("contract_hash"),
            "recomputed": recomputed,
            "stable": recomputed == contract.get("contract_hash")}
