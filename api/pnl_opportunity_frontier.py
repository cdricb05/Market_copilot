"""api/pnl_opportunity_frontier.py - the Release 32 research read model.

Read-only. This module answers ONE operator question - "given everything we
legitimately observe, where should capital be deployed, and what did the
opportunity research conclude?" - by reading the campaign's own immutable
artifacts and restating nothing.

It computes no research mathematics. Every number it publishes was produced by
``alpha_agent.r32`` and written into a hashed artifact; recomputing one here
would make this module a second owner of that number, and the two would drift
the first time a budget or a judge constant changed. So it loads, it selects,
and it reports the artifacts' own hashes so the operator can tell WHICH campaign
state they are looking at.

It creates no signal authority, no target, no capital allocation, no proposal,
no decision and no order; it exposes no sleeve-activation and no
model-activation control; and it writes nothing. The campaign it reports on
cannot activate a sleeve either - ``AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED`` is
False in the research package and the architecture audit asserts it.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

SCHEMA_VERSION = "pnl_opportunity_frontier.v1"
COMPOSITION_OWNER = "api.pnl_opportunity_frontier"
PHASE = "R32"

STATE_READY = "READY"
STATE_RUNNING = "RUNNING"
STATE_NOT_STARTED = "NOT_STARTED"
STATE_UNAVAILABLE = "UNAVAILABLE"
STATE_VOCAB = (STATE_READY, STATE_RUNNING, STATE_NOT_STARTED, STATE_UNAVAILABLE)

#: Mirrors ``alpha_agent.r32`` so the read model never imports the research
#: package into the API process.
RESEARCH_ROOT_ENV = "PAPER_TRADER_R32_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\pnl_opportunity_frontier")
DEFAULT_CAMPAIGN_ID = "r32_pnl_opportunity_frontier_v4"

#: Campaigns whose artifacts remain on disk as history and may never be
#: presented as the campaign's state. Shown to the operator as SUPERSEDED with
#: the reason, because a research surface that silently drops a prior result
#: invites the question "what happened to v1?" and answers it nowhere.
SUPERSEDED_CAMPAIGN_IDS = ("r32_pnl_opportunity_frontier_v1",
                           "r32_pnl_opportunity_frontier_v2",
                           "r32_pnl_opportunity_frontier_v3")

#: ``NO LIVE BROKER ORDERS`` is the canonical Phase 27B.6 wording. Paper orders
#: are real and exist in the operational book under a governed, manually
#: reviewed workflow; only live brokerage orders are structurally disabled.
SAFETY_BADGES = ["RESEARCH ONLY", "READ ONLY", "PREVIEW ONLY",
                 "NO LIVE BROKER ORDERS", "AUTOMATION OFF", "MANUAL REVIEW",
                 "NO MODEL PROMOTION", "NO SLEEVE ACTIVATION"]

ARTIFACTS = {
    "contract": "research_campaign_contract.json",
    "sources": "data_source_registry.json",
    "information_state": "information_state_contract.json",
    "panels": "cross_asset_panel_manifest.json",
    "sleeve_contract": "strategy_sleeve_contract.json",
    "judge": "common_economic_judge.json",
    # Schema 2. Schema 1 declared invented turnover budget values and stays on
    # disk, frozen and superseded, rather than being rewritten.
    "governance": "daily_multi_asset_governance_contract_v2.json",
    "registry": "sleeve_candidate_registry.json",
    "sleeve_results": "sleeve_results.json",
    "purchase": "information_purchase_frontier.json",
    "frontier": "pnl_opportunity_frontier.json",
    "verdict": "final_verdict.json",
}

#: Artifacts that must all exist before the campaign may be called READY. A
#: guard that checks each artifact only "if present" reports OK for work it
#: never did - Release 31 shipped exactly that and passed with 8 of 15 missing.
REQUIRED_FOR_READY = ("contract", "sources", "panels", "judge",
                      "sleeve_contract", "registry", "frontier", "verdict")


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def campaign_dir(campaign_id: str = DEFAULT_CAMPAIGN_ID) -> Path:
    return research_root() / str(campaign_id)


def _read(path: Path) -> Optional[dict]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _g(obj: Optional[dict], *keys, default=None):
    cur = obj
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return default if cur is None else cur


def load_frontier(campaign_id: str = DEFAULT_CAMPAIGN_ID) -> dict:
    """Compose the Release-32 read model from frozen artifacts."""
    root = campaign_dir(campaign_id)
    loaded = {k: _read(root / name) for k, name in ARTIFACTS.items()}
    missing = [ARTIFACTS[k] for k in ARTIFACTS if loaded.get(k) is None]
    present_required = [k for k in REQUIRED_FOR_READY if loaded.get(k) is not None]

    if not root.exists():
        state = STATE_NOT_STARTED
    elif len(present_required) == len(REQUIRED_FOR_READY):
        state = STATE_READY
    elif present_required:
        state = STATE_RUNNING
    else:
        state = STATE_UNAVAILABLE

    verdict = loaded.get("verdict") or {}
    frontier = loaded.get("frontier") or {}
    contract = loaded.get("contract") or {}
    sources = loaded.get("sources") or {}
    panels = loaded.get("panels") or {}
    judge = loaded.get("judge") or {}
    purchase = loaded.get("purchase") or {}
    governance = loaded.get("governance") or {}
    registry = loaded.get("registry") or {}

    sleeves = []
    for row in (frontier.get("rows") or []):
        sleeves.append({
            "sleeve": row.get("sleeve"),
            "state": row.get("state"),
            "is_control": bool(row.get("is_control")),
            "owns_capital": False,
            "activated": False,
            "best_configuration": row.get("best_configuration"),
            "net_annual_return": row.get("net_annual_return"),
            "net_sharpe": row.get("net_sharpe"),
            "max_drawdown": row.get("max_drawdown"),
            "mean_cash_weight": row.get("mean_cash_weight"),
            "annual_cost_drag": row.get("annual_cost_drag"),
            "excess_vs_cash": row.get("excess_vs_cash"),
            "excess_vs_benchmark": row.get("excess_vs_benchmark"),
            "t_vs_cash": row.get("t_vs_cash"),
            "qualifies": bool(row.get("qualifies")),
            "rejection_reason": row.get("rejection_reason"),
            "window": row.get("window"),
        })

    source_counts = _source_admissibility_counts(sources)

    return {
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "phase": PHASE,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "campaign_id": campaign_id,
        "state": state,
        "research_root": str(research_root()),
        "missing_artifacts": missing,
        "artifact_count": len(ARTIFACTS) - len(missing),
        "artifact_total": len(ARTIFACTS),

        "question": contract.get("question") or _g(frontier, "question"),
        "primary_verdict": verdict.get("primary_verdict"),
        "secondary_verdict": verdict.get("secondary_verdict"),
        "qualified_sleeves": verdict.get("qualified_sleeves") or [],
        "n_qualified": verdict.get("n_qualified"),
        "headline": _headline(verdict, frontier),

        "sleeves": sleeves,
        "strongest_sleeve": frontier.get("strongest_sleeve"),
        "second_strongest_sleeve": frontier.get("second_strongest_sleeve"),
        "ranked_sleeves": frontier.get("ranked_by_excess_over_cash") or [],
        "cash_row": frontier.get("cash_row") or {},
        "latent_risk_clusters": frontier.get("latent_risk_clusters") or [],
        "correlation_map": frontier.get("correlation_map") or {},
        "cluster_threshold": frontier.get("cluster_threshold"),
        "common_overlap": frontier.get("common_overlap") or {},

        "control_sleeve": contract.get("control_sleeve"),
        "inherited_equity_selection": frontier.get("inherited_equity_selection")
                                       or {},
        "superseded_campaigns": list(SUPERSEDED_CAMPAIGN_IDS),
        "superseded_detail": contract.get("superseded_campaigns") or {},

        "budgets": contract.get("budgets") or {},
        "funnel": verdict.get("funnel") or _g(registry, "summary", default={}),
        "multiple_testing": verdict.get("multiple_testing") or {},

        "sources": {
            "source_count": sources.get("source_count"),
            "total_marginal_cost_usd": sources.get("total_marginal_cost_usd"),
            "admissibility_counts": source_counts,
            "classifier": sources.get("classifier") or {},
            "prohibited_substitutions": sources.get("prohibited_substitutions")
                                        or [],
        },
        "panel_limitations": panels.get("point_in_time_limitations") or [],
        "panels": panels.get("panels") or {},

        "information_gaps": frontier.get("information_gaps") or [],
        "purchase_frontier": {
            "n_gaps": purchase.get("n_gaps"),
            "purchase_candidates": purchase.get("purchase_candidates") or [],
            "highest_value_sample_request":
                purchase.get("highest_value_sample_request"),
            "prior_evaluations": purchase.get("prior_evaluations") or [],
            "total_spent_usd": purchase.get("total_spent_usd"),
            "may_spend_money": purchase.get("release32_may_spend_money"),
        },
        "governance": {
            "state": governance.get("state"),
            "implemented_in_release_32":
                governance.get("implemented_in_release_32"),
            "daily_reassessment_implies_daily_trading":
                governance.get("daily_reassessment_implies_daily_trading"),
            "no_churn_rule": governance.get("no_churn_rule"),
            "event_fabric_owner": governance.get("event_fabric_owner"),
            "multi_asset_nav_owner": governance.get("multi_asset_nav_owner"),
            "turnover_budgets": governance.get("turnover_budgets") or {},
            # The concepts exist; the numbers do not. Both halves are reported,
            # because a budget map of nulls with no state beside it reads as a
            # budget of zero to anyone skimming it.
            "turnover_budget_concepts_declared":
                governance.get("turnover_budget_concepts_declared"),
            "turnover_budget_values_calibrated":
                governance.get("turnover_budget_values_calibrated"),
            "turnover_budget_value_state":
                governance.get("turnover_budget_value_state"),
            "turnover_budget_value_owner":
                governance.get("turnover_budget_value_owner"),
            "stale_data_fails_closed": governance.get("stale_data_fails_closed"),
        },

        "hashes": {
            "contract": contract.get("contract_hash"),
            "sources": sources.get("registry_hash"),
            "panels": panels.get("manifest_hash"),
            "judge": judge.get("judge_hash"),
            "judge_behaviour": _g(judge, "behaviour_hash"),
            "sleeve_contract": _g(loaded.get("sleeve_contract"),
                                  "sleeve_contract_hash"),
            "frontier": frontier.get("frontier_hash"),
            "verdict": verdict.get("verdict_hash"),
            "governance": governance.get("governance_hash"),
            "purchase": purchase.get("purchase_frontier_hash"),
        },

        "safety_badges": list(SAFETY_BADGES),
        "creates_signal_authority": False,
        "creates_portfolio_target": False,
        "creates_capital_allocation": False,
        "creates_proposal": False,
        "creates_decision": False,
        "creates_order": False,
        "activates_sleeve": False,
        "promotes_model": False,
        "mutates_holdings": False,
        "mutates_cash": False,
        "enables_automation": False,
        "writes_operational_store": False,
        "production_read_only": True,
        "sleeves_own_capital": False,
        "is_research_comparison_not_an_allocator": True,
    }


def _source_admissibility_counts(sources: dict) -> dict:
    """Total each admissibility class across every measured database."""
    out: dict = {}
    for src in (sources.get("sources") or []):
        by = _g(src, "measured", "by_admissibility", default={}) or {}
        for k, v in by.items():
            out[str(k)] = out.get(str(k), 0) + int(v or 0)
    return out


def _headline(verdict: dict, frontier: dict) -> str:
    """One sentence an operator can act on, or decline to act on."""
    primary = verdict.get("primary_verdict")
    n = verdict.get("n_qualified")
    if not primary:
        return "Release 32 opportunity research has not produced a verdict yet."
    if primary == "R32_ZERO_COST_OPPORTUNITY_FRONTIER_EXHAUSTED":
        return ("No sleeve beat a volatility-matched mix of the benchmark and "
                "cash after costs, so no zero-cost opportunity qualified. "
                "Cash and the existing book remain the standing answer.")
    if primary == "R32_SINGLE_SLEEVE_QUALIFIED":
        qs = (verdict.get("qualified_sleeves") or ["one sleeve"])[0]
        return f"{qs} qualified for research review. Nothing is activated."
    if primary == "R32_MULTIPLE_SLEEVES_QUALIFIED":
        return (f"{n} sleeves qualified for research review. Nothing is "
                "activated.")
    return f"Release 32 terminated with {primary}."
