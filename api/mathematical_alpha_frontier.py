"""api/mathematical_alpha_frontier.py - the Release 31 research read model.

Read-only. This module answers ONE operator question - "what is the mathematical
alpha frontier campaign doing, and what did it conclude?" - by reading the
campaign's own immutable artifacts and restating nothing.

Why it computes no research mathematics
---------------------------------------
Every number this read model publishes was produced by
``alpha_agent.r31`` and written into a hashed artifact. If this module
recomputed a metric it would immediately become a second owner of that metric,
and the two would drift the day someone changed a budget or a judge constant.
So it loads, it selects, and it reports the artifacts' own hashes so the operator
can tell WHICH campaign state they are looking at.

It creates no signal authority, no target, no proposal, no decision and no order,
exposes no model-activation control, and writes nothing. The campaign it reports
on cannot promote a model either - ``AUTOMATIC_PROMOTION_ALLOWED`` is False in
the research package and asserted by the architecture audit.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

SCHEMA_VERSION = "mathematical_alpha_frontier.v1"
COMPOSITION_OWNER = "api.mathematical_alpha_frontier"
PHASE = "R31"

STATE_READY = "READY"
STATE_RUNNING = "RUNNING"
STATE_NOT_STARTED = "NOT_STARTED"
STATE_UNAVAILABLE = "UNAVAILABLE"
STATE_VOCAB = (STATE_READY, STATE_RUNNING, STATE_NOT_STARTED, STATE_UNAVAILABLE)

#: Mirrors ``alpha_agent.r31`` so the read model never imports the research
#: package into the API process. The campaign root is research-owned; the API
#: only reads finished artifacts from it.
RESEARCH_ROOT_ENV = "PAPER_TRADER_R31_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\mathematical_alpha_frontier")
DEFAULT_CAMPAIGN_ID = "r31_mathematical_alpha_frontier_v3"

#: Campaigns whose artifacts remain on disk as history and may never be presented
#: as the campaign's state. Shown to the operator as SUPERSEDED with the reason,
#: because a research surface that silently drops a prior result invites the
#: question "what happened to v2?" and answers it nowhere.
SUPERSEDED_CAMPAIGN_IDS = ("r31_mathematical_alpha_frontier_v1",
                           "r31_mathematical_alpha_frontier_v2")

SAFETY_BADGES = ["RESEARCH ONLY", "READ ONLY", "PREVIEW ONLY", "NO ORDERS",
                 "NO LIVE ORDERS", "AUTOMATION OFF", "MANUAL REVIEW",
                 "NO MODEL PROMOTION"]

ARTIFACTS = {
    "contract": "research_campaign_contract.json",
    "universe": "investment_universe_manifest.json",
    "benchmarks": "benchmark_manifest.json",
    "covariance_cache": "covariance_cache_manifest.json",
    "manifest": "data_snapshot_manifest.json",
    "partition": "evidence_partition_contract.json",
    "judge": "research_judge_contract.json",
    "literature": "literature_method_registry.json",
    "known_registry": "known_method_registry.json",
    "known_results": "known_method_results.json",
    "novel_contract": "novel_discovery_contract.json",
    "novel_results": "novel_discovery_results.json",
    "candidates": "candidate_registry.json",
    "lockbox_access": "lockbox_access_log.json",
    "lockbox_results": "lockbox_results.json",
    "multiple_testing": "multiple_testing_results.json",
    "frontier": "economic_frontier_results.json",
    "verdict": "final_verdict.json",
}


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def campaign_dir(campaign_id: str = DEFAULT_CAMPAIGN_ID) -> Path:
    return research_root() / str(campaign_id)


def _read(path: Path) -> Optional[dict]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _pct(part: Optional[int], whole: Optional[int]) -> Optional[float]:
    try:
        p, w = float(part), float(whole)
    except (TypeError, ValueError):
        return None
    return round(100.0 * p / w, 1) if w > 0 else None


def load_frontier(campaign_id: str = DEFAULT_CAMPAIGN_ID) -> dict:
    """The compact read model behind ``GET /v1/research/mathematical-alpha-frontier``."""
    root = campaign_dir(campaign_id)
    art = {k: _read(root / name) for k, name in ARTIFACTS.items()}
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    if art["contract"] is None:
        return {
            "schema_version": SCHEMA_VERSION, "composition_owner": COMPOSITION_OWNER,
            "phase": PHASE, "generated_at": generated_at,
            "campaign_id": campaign_id, "state": STATE_NOT_STARTED,
            "blocker": "NO_CAMPAIGN_CONTRACT",
            "headline": "No Release 31 campaign contract has been frozen.",
            "safety_badges": list(SAFETY_BADGES),
            "creates_orders": False, "allows_model_activation": False,
        }

    con = art["contract"]
    man = art["manifest"] or {}
    ver = art["verdict"]
    cands = art["candidates"] or {}
    known = art["known_results"] or {}
    novel = art["novel_results"] or {}
    mt = art["multiple_testing"] or {}
    lit = art["literature"] or {}
    access = art["lockbox_access"] or {}
    uni = art["universe"] or {}
    inv = uni.get("investment_universe") or {}
    usurv = uni.get("survivorship") or {}
    bench = art["benchmarks"] or {}
    covc = art["covariance_cache"] or {}
    econ = con.get("economics_policy") or {}

    state = STATE_READY if ver else (STATE_RUNNING if cands else STATE_READY)

    samples = []
    for name, s in sorted((man.get("samples") or {}).items()):
        samples.append({
            "sample": name, "cross_sections": s.get("cross_sections"),
            "first_date": s.get("first_date"), "last_date": s.get("last_date"),
            "survivorship": s.get("survivorship"),
            "may_carry_verdict": s.get("may_carry_verdict")})

    partition_rows = []
    for name, blk in sorted((art["partition"] or {}).get("samples", {}).items()):
        for h, p in sorted((blk.get("horizons") or {}).items(),
                           key=lambda kv: int(kv[0])):
            c = p.get("counts") or {}
            d = p.get("dates") or {}
            partition_rows.append({
                "sample": name, "horizon_sessions": int(h),
                "discovery": c.get("discovery"), "validation": c.get("validation"),
                "lockbox": c.get("lockbox"), "embargoed": c.get("embargoed"),
                "lockbox_first": (d.get("lockbox") or {}).get("first"),
                "lockbox_last": (d.get("lockbox") or {}).get("last"),
                "state": p.get("state")})

    km = (cands.get("known_method") or {})
    nv = (cands.get("novel") or {})
    budgets = {
        "known_families": {"used": km.get("family_count"),
                           "budget": km.get("family_budget"),
                           "pct": _pct(km.get("family_count"), km.get("family_budget"))},
        "known_configs": {"used": km.get("configs"), "budget": km.get("config_budget"),
                          "pct": _pct(km.get("configs"), km.get("config_budget"))},
        "novel_families": {"used": nv.get("family_count"),
                           "budget": nv.get("family_budget"),
                           "pct": _pct(nv.get("family_count"), nv.get("family_budget"))},
        "novel_candidates": {"used": nv.get("candidates"),
                             "budget": nv.get("candidate_budget"),
                             "pct": _pct(nv.get("candidates"), nv.get("candidate_budget"))},
        "lockbox_accesses": {"used": access.get("access_count"),
                             "budget": access.get("budget"),
                             "pct": _pct(access.get("access_count"), access.get("budget"))},
    }

    best_validation = (known.get("leaderboard") or [None])[0]
    best_novel = (novel.get("leaderboard") or [None])[0]
    incumbent = next((b for b in (known.get("benchmarks") or [])
                      if b.get("family") == "incumbent_momentum_leg"), None)

    improvement = None
    if best_validation and incumbent:
        a = best_validation.get("net_excess_annualised")
        b = incumbent.get("net_excess_annualised")
        if a is not None and b is not None:
            improvement = round(float(a) - float(b), 6)

    return {
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "phase": PHASE,
        "generated_at": generated_at,
        "campaign_id": con.get("campaign_id"),
        "state": state,
        "campaign_state": ("TERMINAL" if ver else "IN_PROGRESS"),

        "contract_hash": con.get("contract_hash"),
        "git_head": con.get("git_head"),
        "created_at": con.get("created_at"),
        "objective": con.get("objective"),
        "canonical_question": con.get("canonical_question"),
        "primary_selection_principle": con.get("primary_selection_principle"),

        # --- Campaign v3 corrections, as operator-visible state -------------- #
        "superseded_campaigns": [
            {"campaign_id": cid,
             "state": (con.get("superseded_campaigns") or {}).get(cid, {}).get("state"),
             "defects": (con.get("superseded_campaigns") or {}).get(cid, {}).get("defects") or [],
             "artifacts_preserved": (campaign_dir(cid)).exists()}
            for cid in SUPERSEDED_CAMPAIGN_IDS],
        "superseded_evidence_rules": con.get("superseded_evidence_rules") or {},

        "universe": {
            "training_universes": (con.get("universe_policy") or {}).get("training_universes") or [],
            "evaluation_universe": (con.get("universe_policy") or {}).get("evaluation_universe"),
            "universe_hash": (uni.get("universe_hash")
                              or (con.get("universe_policy") or {}).get("universe_hash")),
            "median_members_per_session": inv.get("median_members_per_session"),
            "min_members_per_session": inv.get("min_members_per_session"),
            "max_members_per_session": inv.get("max_members_per_session"),
            "point_in_time": inv.get("point_in_time"),
            "current_membership_applied_backwards":
                inv.get("current_membership_applied_backwards"),
            "missing_member_day_fraction": usurv.get("missing_fraction"),
            "completeness_verdict": usurv.get("verdict"),
            "dominant_structural_cause": usurv.get("dominant_structural_cause"),
            "broader_training_never_widens_evaluation":
                (con.get("universe_policy") or {}).get("broader_training_never_widens_evaluation"),
        },
        "training_universe_comparison": known.get("training_universe_comparison"),

        "benchmarks": {
            "reported": (con.get("benchmark_policy") or {}).get("benchmarks_reported") or [],
            "benchmark_hash": bench.get("benchmark_hash"),
            "equal_weight": (bench.get("equal_weight") or {}).get("name"),
            "investable": (bench.get("investable") or {}).get("name"),
            "investable_source": (bench.get("investable") or {}).get("source_symbol"),
            "investable_state": (bench.get("investable") or {}).get("state"),
            "investable_coverage": (bench.get("investable") or {}).get("coverage_fraction"),
            "substitution_permitted":
                (con.get("benchmark_policy") or {}).get("substitution_permitted"),
        },

        "economics": {
            "primary_construction": "CANONICAL_ZERO_BASE_ALLOCATION_STOCKS_PLUS_CASH",
            "allocator_owner": econ.get("allocator_owner"),
            "covariance_owner": econ.get("covariance_owner"),
            "cash_is_a_real_allocation_choice":
                econ.get("cash_is_a_real_allocation_choice_zero_to_one_hundred_percent"),
            "risk_frontier_gamma_multipliers":
                econ.get("risk_frontier_gamma_multipliers") or [],
            "primary_gamma_multiplier": econ.get("primary_gamma_multiplier"),
            "frontier_scope": econ.get("frontier_scope"),
            "top_n_may_carry_primary_verdict":
                econ.get("top_n_may_carry_primary_verdict"),
            "cost_base": econ.get("cost_base"),
            "turnover_alignment": econ.get("turnover_alignment"),
            "historical_sector_constraint": econ.get("historical_sector_constraint"),
            "second_portfolio_optimiser_exists":
                econ.get("second_portfolio_optimiser_exists"),
            "covariance_cache_sections": covc.get("sections_cached"),
            "covariance_cache_reused_by_every_candidate":
                covc.get("reused_by_every_candidate"),
        },

        "tracks": (con.get("architecture_policy") or {}).get("tracks") or [],
        "calibration_rejections": known.get("calibration_rejections") or {},

        "snapshot": {
            "hash": (man.get("snapshot_cache") or {}).get("sha256"),
            "cross_sections": man.get("cross_sections_total"),
            "feature_count": len(man.get("feature_order") or []),
            "samples": samples,
            "survivorship": man.get("survivorship_measurement"),
        },
        "evidence_partition": partition_rows,

        "literature": {
            "papers_screened": lit.get("papers_screened"),
            "papers_screened_budget": lit.get("papers_screened_budget"),
            "methods_extracted": lit.get("methods_deeply_extracted"),
            "methods_extracted_budget": lit.get("methods_extracted_budget"),
            "families_implemented": lit.get("implemented_family_count"),
            "family_budget": lit.get("family_budget"),
            "stopping_rule_state": (lit.get("stopping_rule") or {}).get("state"),
            "excluded_methods": lit.get("excluded_methods") or [],
        },

        "budgets": budgets,
        "known_method_families": known.get("families_executed") or [],
        "novel_families": novel.get("families_executed") or [],
        "novel_campaigns": novel.get("campaigns") or {},
        "novel_exhaustion_triggered": novel.get("exhaustion_triggered"),

        "best_validation_candidate": best_validation,
        "best_novel_candidate": best_novel,
        "incumbent_benchmark": incumbent,
        "net_economic_improvement_vs_incumbent": improvement,

        "multiple_testing": {
            "denominator": mt.get("denominator_executed_candidates"),
            "bh_rejected": (mt.get("benjamini_hochberg") or {}).get("n_rejected"),
            "bh_m": (mt.get("benjamini_hochberg") or {}).get("m"),
            "spa_p_value": (mt.get("superior_predictive_ability") or {}).get("p_value"),
            "paired_p_value": (mt.get("paired_vs_incumbent") or {}).get("p_value"),
            "policy": mt.get("policy"),
        },

        "lockbox": {
            "access_count": access.get("access_count"),
            "budget": access.get("budget"),
            "results": (ver or {}).get("lockbox", {}).get("results") or [],
        },

        "verdict": {
            "primary": (ver or {}).get("primary_verdict"),
            "secondary": (ver or {}).get("secondary_verdict"),
            "all_superiority_checks_passed": ((ver or {}).get("superiority") or {})
            .get("all_passed"),
            "checks": ((ver or {}).get("superiority") or {}).get("checks") or {},
            "information_gap": (ver or {}).get("information_gap"),
            "hash": (ver or {}).get("final_verdict_hash"),
        },
        "current_blocker": _blocker(ver, cands, novel),
        "headline": _headline(ver, best_validation, incumbent),

        "architecture_comparison": (ver or {}).get("architecture_comparison"),
        "operational_model_comparison": (ver or {}).get("operational_model_comparison"),
        "intrinio_extension_readiness": (ver or {}).get("intrinio_extension_readiness"),
        "event_news_decision": (ver or {}).get("event_news_decision"),

        "artifacts_present": sorted(k for k, v in art.items() if v is not None),
        "artifacts_missing": sorted(k for k, v in art.items() if v is None),
        "research_root": str(campaign_dir(campaign_id)),

        "safety_badges": list(SAFETY_BADGES),
        "creates_orders": False,
        "allows_model_activation": False,
        "automatic_promotion_allowed": False,
        "operational_model_unchanged": True,
        "read_only": True,
    }


def _blocker(verdict, candidates, novel) -> Optional[str]:
    if verdict:
        p = verdict.get("primary_verdict") or ""
        if p.endswith("SUPERIOR_MODEL_FOUND"):
            return "AWAITING_MANUAL_PAPER_REVIEW"
        if "EXHAUSTED" in p:
            return "NEW_ORTHOGONAL_INFORMATION_REQUIRED"
        if "BLOCKED" in p:
            return "POINT_IN_TIME_EVIDENCE_BLOCKED"
        return None
    if not candidates:
        return "NO_CANDIDATE_EXECUTED_YET"
    if not novel:
        return "NOVEL_DISCOVERY_NOT_RUN"
    return "LOCKBOX_NOT_EXECUTED"


def _headline(verdict, best, incumbent) -> str:
    if verdict:
        p = verdict.get("primary_verdict") or ""
        if p.endswith("SUPERIOR_MODEL_FOUND"):
            return ("A candidate passed every frozen superiority check on the "
                    "lockbox. It is READY FOR MANUAL PAPER REVIEW and is NOT "
                    "activated.")
        if "EXHAUSTED" in p:
            return ("No decision function beat the incumbent's net implementable "
                    "economics at comparable risk on evidence that played no "
                    "part in selecting it. Further search over the same "
                    "information would add data-mining risk, not knowledge.")
        return p
    if best and incumbent:
        return ("Campaign in progress. Best validation candidate is %s."
                % best.get("candidate_id"))
    return "Campaign in progress."
