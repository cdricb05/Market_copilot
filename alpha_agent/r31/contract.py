"""alpha_agent.r31.contract - the ONE Release 31 campaign contract owner.

The contract is frozen BEFORE any candidate result is observed and is immutable
afterwards. That ordering is the whole point: a campaign that may widen its
budget, move its lockbox boundary or relax its superiority bar after seeing a
disappointing result is not running an experiment, it is searching until it finds
something. Every budget below is therefore a NUMBER in this module, enforced by
:mod:`alpha_agent.r31.registry`, not a sentence in a document.

If a material term changes, that is a NEW campaign with a NEW id - never an edit.
"""
from __future__ import annotations

import platform
import subprocess
import sys
from pathlib import Path
from typing import Optional

from .. import r31
from . import _versions

CALCULATION_OWNER = "alpha_agent.r31.contract"
CONTRACT_SCHEMA = "r31_research_campaign_contract/2"

#: The campaign this release runs. A new id is the ONLY way to change any term.
CAMPAIGN_ID = "r31_mathematical_alpha_frontier_v3"

#: Both predecessors are SUPERSEDED, not deleted. Their artifacts stay on disk as
#: history and are unreadable by v3: the judge's behaviour hash, the investment
#: universe hash and the benchmark hash are all bound into every v3 candidate's
#: specification hash, so a result measured under an earlier judge cannot enter a
#: v3 leaderboard, a v3 lockbox or the v3 multiple-testing denominator.
SUPERSEDED_EXPERIMENTAL_DESIGN = "SUPERSEDED_EXPERIMENTAL_DESIGN"
SUPERSEDED_CAMPAIGNS = {
    "r31_mathematical_alpha_frontier_v1": {
        "state": SUPERSEDED_EXPERIMENTAL_DESIGN,
        "defects": [
            "the judge charged the canonical per-side cost on ONE-WAY turnover "
            "while reporting the drag on both sides, so every net return it "
            "measured understated transaction cost by roughly half",
        ],
        "produced_a_verdict": False,
    },
    "r31_mathematical_alpha_frontier_v2": {
        "state": SUPERSEDED_EXPERIMENTAL_DESIGN,
        "defects": [
            "the primary portfolio EVALUATION universe was the Russell 1000 "
            "panel, not the point-in-time S&P 500 investment universe the "
            "business objective names",
            "the primary economic judge built top-N approximately-equal-weight "
            "books with cash pinned to zero, so a candidate that should have "
            "held cash was forced to own 25 names and cash was never a decision",
            "the direct-portfolio (Track B) learner compared consecutive weight "
            "vectors POSITIONALLY whenever their lengths matched, so turnover was "
            "not measured by security identity",
            "the investable S&P 500 total-return comparison was silently replaced "
            "by the eligible-universe equal-weight return",
        ],
        "produced_a_verdict": False,
    },
}

#: v2 evidence may be READ as history and may never influence v3. Stated as an
#: enforced list rather than a paragraph, because the registry and the multiple
#: testing owner both assert it.
SUPERSEDED_EVIDENCE_RULES = {
    "may_select_v3_hyperparameters": False,
    "may_select_v3_finalists": False,
    "may_influence_the_lockbox": False,
    "may_contribute_to_a_superiority_verdict": False,
    "may_reduce_the_multiple_testing_denominator": False,
    "may_be_reused_as_v3_validation_evidence": False,
    "is_preserved_on_disk": True,
}

ARTIFACT_NAME = "research_campaign_contract.json"

# --------------------------------------------------------------------------- #
# Sample geometry
# --------------------------------------------------------------------------- #
#: Trading-session forecast horizons. Identical to ``engine.return_forecast``.
HORIZONS = (5, 20, 60)

#: Decision dates are struck every N trading sessions (~1 month). This mirrors
#: the cadence at which the operational monthly momentum input actually changes,
#: so the campaign never claims more independent decisions than the owned inputs
#: support.
STEP_SESSIONS = 21

#: Minimum trailing sessions before any feature is computed.
MIN_HISTORY = 252

#: A date with fewer eligible names is not a cross-section.
MIN_CROSS_SECTION = 50

#: The two declared sample geometries. The campaign's PRIMARY evidence is the
#: survivorship-free price sample; the fundamental sample is admissible but
#: carries a measured survivorship limitation and can never, on its own, produce
#: a superiority verdict. See ``snapshot.survivorship_report``.
SAMPLE_PRICE_FULL = "PRICE_FULL_SURVIVORSHIP_FREE"
SAMPLE_FUND_MATCHED = "FUNDAMENTAL_MATCHED_SURVIVORSHIP_LIMITED"
SAMPLES = (SAMPLE_PRICE_FULL, SAMPLE_FUND_MATCHED)
PRIMARY_SAMPLE = SAMPLE_PRICE_FULL

#: The label. Forward EXCESS return over the cross-sectional equal-weight mean,
#: measured from ONE common decision timestamp for the whole cross-section.
TARGET = "FORWARD_EXCESS_RETURN_VS_CROSS_SECTIONAL_MEAN_COMMON_TIMESTAMP"

# --------------------------------------------------------------------------- #
# CORRECTION 1 - training universe is not the investment universe
# --------------------------------------------------------------------------- #
#: What a candidate may LEARN from. Declared by ``alpha_agent.r31.universe``.
#: Which one a candidate used is part of its specification hash and therefore
#: part of the multiple-testing denominator: "train broad, invest narrow" is a
#: hypothesis this campaign TESTS, not an accident of which panel was loaded.
TRAIN_INVESTMENT_ONLY = "TRAIN_S_AND_P_500_ONLY"
TRAIN_BROAD_PIT = "TRAIN_RUSSELL1000_PIT"
TRAINING_UNIVERSES = (TRAIN_INVESTMENT_ONLY, TRAIN_BROAD_PIT)

#: What the PRIMARY judge may let a candidate OWN. Never widened by a broader
#: training choice.
EVALUATION_UNIVERSE = "EVALUATE_S_AND_P_500_PIT_MEMBERS_ONLY"

# --------------------------------------------------------------------------- #
# CORRECTION 2 - the primary judge is zero-base economics
# --------------------------------------------------------------------------- #
#: The two architectures under comparison. Neither is privileged beforehand.
TRACK_A = "FORECAST_THEN_ALLOCATE"
TRACK_B = "DIRECT_PORTFOLIO_DECISION"
TRACKS = (TRACK_A, TRACK_B)

#: The pre-registered risk frontier, expressed as MULTIPLES of the canonical
#: ``risk_aversion_gamma``. This replaces v2's top-N book sizes, which varied
#: CONCENTRATION rather than risk appetite and could not express a cash decision
#: at all. Frozen here before any candidate return exists.
RISK_FRONTIER_GAMMA_MULTIPLIERS = (0.5, 1.0, 2.0)

#: Selection ALWAYS uses the canonical 1.0x point. The other two characterise a
#: finalist's sensitivity to risk appetite; they never choose between candidates,
#: so a candidate cannot win by being best at one convenient gamma.
PRIMARY_GAMMA_MULTIPLIER = 1.0
FRONTIER_SCOPE = "FINALISTS_AND_LOCKBOX_ONLY"

#: Retained from v2 and DEMOTED. A top-N equal-weight book is a legitimate
#: diagnostic - it answers "which names does this model like?" - and it is no
#: longer permitted to carry the economic verdict.
SECONDARY_DIAGNOSTIC_BOOK_SIZE = 25
TOP_N_MAY_CARRY_PRIMARY_VERDICT = False

# --------------------------------------------------------------------------- #
# CORRECTION 4 - two benchmarks, neither substitutable
# --------------------------------------------------------------------------- #
BENCH_EQUAL_WEIGHT = "SP500_PIT_EQUAL_WEIGHT"
BENCH_SPY = "SPY_TOTAL_RETURN"
BENCHMARKS_REPORTED = (BENCH_EQUAL_WEIGHT, BENCH_SPY)
BENCHMARK_SUBSTITUTION_PERMITTED = False

# --------------------------------------------------------------------------- #
# Evidence partition (Phase 3) - chosen before any candidate result
# --------------------------------------------------------------------------- #
#: Fractions of the ordered decision-date axis. LOCKBOX is the LATEST contiguous
#: block, VALIDATION precedes it, DISCOVERY precedes that.
DISCOVERY_FRACTION = 0.55
VALIDATION_FRACTION = 0.25
#: remainder (0.20) is LOCKBOX

#: Minimum decision dates each layer must retain for the partition to be honest.
MIN_DISCOVERY_DATES = 60
MIN_VALIDATION_DATES = 30
MIN_LOCKBOX_DATES = 24

# --------------------------------------------------------------------------- #
# Budgets - HARD, encoded, enforced by the registry
# --------------------------------------------------------------------------- #
MAX_KNOWN_METHOD_FAMILIES = 12
MAX_KNOWN_METHOD_CONFIGS = 240
MAX_CONFIGS_PER_KNOWN_FAMILY = 40

MAX_NOVEL_FAMILIES = 6
MAX_NOVEL_CAMPAIGNS = 2
MAX_NOVEL_CANDIDATES_PER_CAMPAIGN = 150
MAX_NOVEL_CANDIDATES_TOTAL = 300
MAX_NOVEL_REFINEMENT_DEPTH = 3

MAX_LOCKBOX_CANDIDATES = 12
MAX_LOCKBOX_PER_FAMILY = 2

#: Literature exhaustion contract (Phase 5).
MAX_PAPERS_SCREENED = 60
MAX_METHODS_EXTRACTED = 24
LITERATURE_DRY_EXPANSIONS = 2

#: The budgets above are CEILINGS, not targets, and the executed grid is
#: deliberately smaller than every one of them.
#:
#: The v3 judge allocates capital through the canonical zero-base optimiser at
#: every decision date. Measured on this machine at an S&P-500-scale
#: cross-section, one canonical solve costs 4-12 seconds depending on how strong
#: the forecast is relative to risk - a weak forecast leaves the optimum interior
#: and Frank-Wolfe runs to its iteration cap. That is 5-15 minutes of judging per
#: candidate per evidence layer, against roughly a second per candidate under v2's
#: top-N book. Executing 240 known configurations plus 300 novel ones would take
#: ~90 core-hours of the JUDGE alone.
#:
#: The response is a smaller, materially-distinct pre-registered grid rather than
#: a cheaper judge. That trade is not merely pragmatic: a smaller pre-registered
#: search has a SMALLER multiple-testing denominator, so each surviving candidate
#: carries more evidence, and the contract's own rule already forbids executing
#: configurations simply to consume a budget. Hyperparameter points were dropped
#: where the grid was dense in a direction the family is known to be insensitive
#: to, never where two configurations express materially different hypotheses.
EXECUTED_GRID_POLICY = "MATERIALLY_DISTINCT_SPECS_ONLY_NEVER_BUDGET_FILLING"

# --------------------------------------------------------------------------- #
# Economics - delegated, never restated
# --------------------------------------------------------------------------- #
#: The campaign does NOT own cost, risk prices or portfolio constraints. It reads
#: ``engine.zero_base_allocator.default_policy()``. These names record WHICH keys
#: the judge consumes so a policy change is visible in the contract hash.
CANONICAL_POLICY_OWNER = "engine.zero_base_allocator.default_policy"
CANONICAL_ALLOCATOR_OWNER = "engine.zero_base_allocator.optimise"
CANONICAL_COVARIANCE_OWNER = "engine.holding_opportunity_cost.build_covariance"
CANONICAL_POLICY_KEYS = (
    "max_name_weight", "sector_cap_fraction", "min_adv_dollar",
    "cost_rate_per_side", "cost_bps_per_side", "covariance_lookback",
    "min_covariance_obs", "risk_aversion_gamma", "min_position_weight",
    "uncertainty_aversion_phi", "max_adv_participation",
)

#: Cash is an asset choice; the paper assumption is a zero return, owned by the
#: allocator and restated here only as a recorded term.
CASH_RETURN_POLICY = "ZERO_RETURN_PAPER_ASSUMPTION"

#: Historical sector classification is not reconstructable point-in-time from the
#: owned estate. The campaign records that as a measurement state and never
#: substitutes current sector metadata backwards.
HISTORICAL_SECTOR_CONSTRAINT = "UNMEASURABLE_PIT"

#: Transition cost is charged on TRADED NOTIONAL - sells plus buys - because the
#: canonical rate is quoted per side. Aligned by SECURITY IDENTITY, never by
#: array position.
COST_BASE = "TRADED_NOTIONAL_SELLS_PLUS_BUYS_TIMES_RATE_PER_SIDE"
TURNOVER_ALIGNMENT = "BY_SECURITY_IDENTITY_NEVER_BY_ARRAY_POSITION"

# --------------------------------------------------------------------------- #
# Statistics
# --------------------------------------------------------------------------- #
MULTIPLE_TESTING_POLICY = "BENJAMINI_HOCHBERG_FDR_PLUS_STATIONARY_BOOTSTRAP_SPA"
FDR_Q = 0.10
BOOTSTRAP_RESAMPLES = 2000
BOOTSTRAP_BLOCK_MEAN = 6
SEEDS = {"learner": 31, "bootstrap": 3101, "novel_search": 3102}

#: Superiority bar. All of these must hold for MODEL_READY_FOR_MANUAL_PAPER_REVIEW.
SUPERIORITY = {
    "must_pass_pit_integrity": True,
    "must_pass_survivorship_controls": True,
    "must_survive_multiple_testing": True,
    "must_evaluate_on_investment_universe_only": True,
    "min_net_annualised_excess_vs_incumbent": 0.0075,
    "max_drawdown_deterioration": 0.05,
    "max_turnover_ratio_vs_incumbent": 2.0,
    "min_subperiod_win_fraction": 0.60,
    "min_spa_p_value_reject": 0.10,
    "requires_lockbox_pass_without_redesign": True,
    "requires_frozen_spec_hash": True,
}

TERMINAL_STATES = (
    "R31_KNOWN_METHOD_SUPERIOR_MODEL_FOUND",
    "R31_NOVEL_ALPHA_SUPERIOR_MODEL_FOUND",
    "R31_CURRENT_INFORMATION_MODEL_FRONTIER_EXHAUSTED",
    "R31_NEW_ORTHOGONAL_DATA_REQUIRED",
    "R31_POINT_IN_TIME_EVIDENCE_BLOCKED",
    "R31_RESOURCE_BUDGET_EXHAUSTED",
)

#: Information families the campaign REFUSES as predictors, and why. Release 30.1
#: exposed a live GDELT feed associating plainly irrelevant articles with CAT;
#: the campaign does not repair a weak model by feeding mislabelled text to a
#: learner.
INADMISSIBLE_INFORMATION = {
    "gdelt_news_text": "EVENT_TRIGGER_ONLY; entity resolution quality unproven",
    "external_reference_links": "OPERATOR REFERENCE ONLY; never an input",
    "current_analyst_snapshots": "no point-in-time history; substituting a "
                                 "current snapshot backwards fabricates evidence",
    "entity_sic_snapshot_sector": "canonical PIT sector owner classifies the "
                                  "owned snapshot as inadmissible for signal "
                                  "construction",
    "current_index_membership_applied_backwards": "hindsight membership is the "
                                                  "single most effective way to "
                                                  "manufacture a backtest",
}


def git_head(repo: Optional[Path] = None) -> Optional[str]:
    root = Path(repo or Path(__file__).resolve().parents[2])
    try:
        out = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(root),
                             capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None
    head = (out.stdout or "").strip()
    return head or None


def build(*, campaign_id: str = CAMPAIGN_ID, created_at: str,
          data_sources: dict, feature_spec: dict,
          universe_hash: str, benchmark_hash: str, judge_hash: str,
          calibration_owner: str, allocation_owner: str,
          covariance_cache_key: str, executed_grid: dict) -> dict:
    """The frozen campaign contract.

    ``created_at`` is supplied by the caller rather than read from the clock so
    the artifact is reproducible and a resumed run re-derives the SAME hash.

    The universe, benchmark, judge, calibration and covariance-cache identities
    are arguments rather than imports: the contract must BIND the exact evidence
    semantics a candidate will be measured under, and a term the contract merely
    describes is a term that can drift away from the code silently.
    """
    body = {
        "contract": CONTRACT_SCHEMA,
        "campaign_id": str(campaign_id),
        "release": r31.RELEASE,
        "campaign_family": r31.CAMPAIGN_FAMILY,
        "calculation_owner": CALCULATION_OWNER,
        "created_at": str(created_at),
        "git_head": git_head(),
        "research_root": str(r31.research_root()),

        "objective": (
            "maximise strictly out-of-sample, after-transaction-cost, "
            "risk-adjusted paper portfolio P&L over HISTORICALLY ELIGIBLE S&P 500 "
            "equities and CASH, using only information legitimately available at "
            "each historical decision time"),
        "canonical_question": (
            "if every investable dollar were cash right now, given everything "
            "legitimately known right now, what portfolio should we own?"),
        "existing_holdings_have_no_intrinsic_privilege": True,
        "primary_selection_principle": (
            "IMPLEMENTABLE NET PORTFOLIO ECONOMICS AT COMPARABLE RISK - never "
            "MSE, never IC alone, never gross return alone"),

        "data_sources": data_sources,
        "feature_specification": feature_spec,

        "sample_geometry": {
            "samples": list(SAMPLES),
            "primary_sample": PRIMARY_SAMPLE,
            "decision_date_definition": (
                "every %d trading sessions on the owned daily panel, starting "
                "after %d trailing sessions of history" % (STEP_SESSIONS, MIN_HISTORY)),
            "step_sessions": STEP_SESSIONS,
            "min_history_sessions": MIN_HISTORY,
            "min_cross_section": MIN_CROSS_SECTION,
            "label_horizons_sessions": list(HORIZONS),
            "target": TARGET,
        },

        "universe_policy": {
            "training_universes": list(TRAINING_UNIVERSES),
            "evaluation_universe": EVALUATION_UNIVERSE,
            "training_choice_is_part_of_candidate_specification": True,
            "broader_training_never_widens_evaluation": True,
            "current_membership_applied_backwards": False,
            "universe_hash": str(universe_hash),
            "owner": "alpha_agent.r31.universe",
        },

        "benchmark_policy": {
            "benchmarks_reported": list(BENCHMARKS_REPORTED),
            "both_always_reported": True,
            "substitution_permitted": BENCHMARK_SUBSTITUTION_PERMITTED,
            "benchmark_hash": str(benchmark_hash),
            "owner": "alpha_agent.r31.benchmarks",
        },

        "architecture_policy": {
            "tracks": list(TRACKS),
            "neither_track_privileged": True,
            "track_a": "information -> expected return (native units or an "
                       "accepted monotonic calibration) -> canonical zero-base "
                       "allocator -> stocks + cash",
            "track_b": "information -> proposed portfolio weights -> the SAME "
                       "canonical feasibility, covariance and transition owners "
                       "-> stocks + cash",
            "calibration_owner": str(calibration_owner),
            "allocation_owner": str(allocation_owner),
        },

        "evidence_partition_policy": {
            "layers": ["DISCOVERY", "VALIDATION", "LOCKBOX", "TRUE_FORWARD"],
            "discovery_fraction": DISCOVERY_FRACTION,
            "validation_fraction": VALIDATION_FRACTION,
            "lockbox_is_latest_contiguous_block": True,
            "purge_embargo_rule": "ceil(horizon / step_sessions) decision dates "
                                  "between adjacent layers",
            "min_dates": {"discovery": MIN_DISCOVERY_DATES,
                          "validation": MIN_VALIDATION_DATES,
                          "lockbox": MIN_LOCKBOX_DATES},
            "no_random_split": True,
            "normalisation_is_per_date_cross_sectional_only": True,
            "calibration_may_read": "DISCOVERY_AND_VALIDATION_ONLY",
        },

        "economics_policy": {
            "policy_owner": CANONICAL_POLICY_OWNER,
            "allocator_owner": CANONICAL_ALLOCATOR_OWNER,
            "covariance_owner": CANONICAL_COVARIANCE_OWNER,
            "covariance_cache_key": str(covariance_cache_key),
            "consumed_keys": list(CANONICAL_POLICY_KEYS),
            "cash_return_policy": CASH_RETURN_POLICY,
            "cash_is_a_real_allocation_choice_zero_to_one_hundred_percent": True,
            "cost_base": COST_BASE,
            "turnover_alignment": TURNOVER_ALIGNMENT,
            "historical_sector_constraint": HISTORICAL_SECTOR_CONSTRAINT,
            "risk_frontier_gamma_multipliers": list(RISK_FRONTIER_GAMMA_MULTIPLIERS),
            "primary_gamma_multiplier": PRIMARY_GAMMA_MULTIPLIER,
            "frontier_scope": FRONTIER_SCOPE,
            "frontier_frozen_before_results": True,
            "secondary_diagnostic_book_size": SECONDARY_DIAGNOSTIC_BOOK_SIZE,
            "top_n_may_carry_primary_verdict": TOP_N_MAY_CARRY_PRIMARY_VERDICT,
            "campaign_owns_no_cost_or_risk_calculation": True,
            "second_portfolio_optimiser_exists": False,
        },

        "budgets": {
            "known_method_families": MAX_KNOWN_METHOD_FAMILIES,
            "known_method_configs": MAX_KNOWN_METHOD_CONFIGS,
            "configs_per_known_family": MAX_CONFIGS_PER_KNOWN_FAMILY,
            "novel_families": MAX_NOVEL_FAMILIES,
            "novel_campaigns": MAX_NOVEL_CAMPAIGNS,
            "novel_candidates_per_campaign": MAX_NOVEL_CANDIDATES_PER_CAMPAIGN,
            "novel_candidates_total": MAX_NOVEL_CANDIDATES_TOTAL,
            "novel_refinement_depth": MAX_NOVEL_REFINEMENT_DEPTH,
            "lockbox_candidates": MAX_LOCKBOX_CANDIDATES,
            "lockbox_per_family": MAX_LOCKBOX_PER_FAMILY,
            "papers_screened": MAX_PAPERS_SCREENED,
            "methods_extracted": MAX_METHODS_EXTRACTED,
            "literature_dry_expansions": LITERATURE_DRY_EXPANSIONS,
            "budgets_are_ceilings_not_targets": True,
            "executed_grid_policy": EXECUTED_GRID_POLICY,
            "executed_grid": dict(executed_grid),
        },

        "lockbox_policy": {
            "touched_once_per_candidate": True,
            "finalists_frozen_before_execution": True,
            "no_redesign_and_retry": True,
            "invisible_to_training_and_selection": True,
            "invisible_to_calibration": True,
        },

        "multiple_testing_policy": {
            "policy": MULTIPLE_TESTING_POLICY,
            "fdr_q": FDR_Q,
            "bootstrap_resamples": BOOTSTRAP_RESAMPLES,
            "bootstrap_block_mean": BOOTSTRAP_BLOCK_MEAN,
            "denominator_is_every_executed_candidate": True,
            "rejected_candidates_stay_in_denominator": True,
            "superseded_campaign_results_are_not_in_the_denominator": True,
        },

        "exhaustion_policy": {
            "two_null_novel_campaigns_terminate": True,
            "terminal_states": list(TERMINAL_STATES),
            "no_budget_extension_after_a_poor_result": True,
            "no_third_novel_campaign": True,
            "no_grammar_expansion_after_failure": True,
        },

        "superseded_campaigns": dict(SUPERSEDED_CAMPAIGNS),
        "superseded_evidence_rules": dict(SUPERSEDED_EVIDENCE_RULES),
        "superiority_contract": dict(SUPERIORITY),
        "inadmissible_information": dict(INADMISSIBLE_INFORMATION),
        "seeds": dict(SEEDS),
        "judge_hash": str(judge_hash),
        "software": _versions.software_versions(),
        "platform": {"python": sys.version.split()[0],
                     "system": platform.system(),
                     "release": platform.release()},
    }
    body["contract_hash"] = r31.sha(body)
    body.update(r31.safety_block())
    return body


def path_for(campaign_id: str = CAMPAIGN_ID) -> Path:
    return r31.campaign_dir(campaign_id) / ARTIFACT_NAME


def load(campaign_id: str = CAMPAIGN_ID) -> Optional[dict]:
    return r31.read_json(path_for(campaign_id))


def freeze(contract: dict) -> Path:
    """Persist the contract. Immutable: a differing rewrite is refused."""
    return r31.write_json(path_for(contract["campaign_id"]), contract)


def verify(contract: dict) -> dict:
    """Re-derive the contract hash and report whether the artifact still binds."""
    body = {k: v for k, v in contract.items()
            if k not in ("contract_hash",) and k not in r31.safety_block()}
    recomputed = r31.sha(body)
    ok = recomputed == contract.get("contract_hash")
    return {"contract_hash": contract.get("contract_hash"),
            "recomputed": recomputed, "intact": bool(ok)}
