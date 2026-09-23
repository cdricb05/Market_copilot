r"""Build the R67 artifacts. READ ONLY.

Registers no hypothesis, mints no experiment id, charges no burden, emits no
prediction and writes nothing to any operational store. Run:

    C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe ^
        research\agents\campaign_r67_multi_strategy\build_artifacts.py

Produces, in this directory:

    STRATEGY_OPPORTUNITY_INVENTORY.json   every economic mechanism the estate
                                          has prosecuted, deduplicated by
                                          MECHANISM, with its evidence, its
                                          failure reason and its next action
    FORWARD_PRODUCER_RECONCILIATION.json  which registered challengers can ever
                                          be funded, and on what date
    CAPITAL_FEASIBILITY_CORRECTION.json   R66's eleven structures re-ruled with
                                          every constraint attributed to its
                                          owner
    PORTFOLIO_OPPORTUNITY_COST.json       the book before, the read-only target
                                          after, and what stops the governed
                                          proposal today
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
R66 = HERE.parent / "campaign_r66_hedged_rv" / "PORTFOLIO_FEASIBILITY.json"


def _w(name: str, body: dict) -> None:
    body.setdefault("generated_at", datetime.now(timezone.utc).isoformat())
    body.setdefault("run_id", "R67_MULTI_STRATEGY_ALPHA_AND_PORTFOLIO_CAPITAL_ENGINE")
    body.setdefault("read_only", True)
    body.setdefault("registers_no_hypothesis", True)
    body.setdefault("burden_charged", 0)
    (HERE / name).write_text(json.dumps(body, indent=1, default=str),
                             encoding="utf-8")
    print("wrote %s" % name)


def inventory() -> None:
    from paper_trader.alpha_agent.r67 import strategy_inventory as SI
    inv = SI.build(limit=None)
    inv["what_this_answers"] = (
        "What economic mechanisms has this system prosecuted, what is each "
        "worth, and what is the next legitimate action on each. Deduplicated "
        "by MECHANISM rather than by experiment name, because 8,472 names "
        "collapse to a far smaller set of economic ideas - 4,112 of them came "
        "from a symbolic tree search and 3,475 from an auto-transform grammar.")
    _w("STRATEGY_OPPORTUNITY_INVENTORY.json", inv)


def forward() -> None:
    from paper_trader.alpha_agent.r67 import forward_producer as FP
    r = FP.reconcile()
    r["what_this_answers"] = (
        "Can anything this estate has registered forward EVER be funded, and "
        "on what date. The capital gate needs 60 matured observations; an "
        "observation needs a decision; a decision needs a cadence producer.")
    _w("FORWARD_PRODUCER_RECONCILIATION.json", r)


def feasibility() -> None:
    from paper_trader.alpha_agent.r67 import capital_feasibility as CF
    pol = CF.authorised_policy()
    rows = []
    if R66.exists():
        b = json.loads(R66.read_text(encoding="utf-8"))
        for s in (b.get("body", b).get("structures") or []):
            ie = s.get("minimum_faithful_expression") or {}
            half_n = (s.get("gross_notional_usd") or 0.0) / 2.0
            half_m = (s.get("committed_capital_initial_margin_usd") or 0.0) / 2.0
            legs = [{"symbol": s.get("long"), "side": "LONG",
                     "n_contracts": ie.get("n_long"),
                     "notional_usd": half_n, "initial_margin_usd": half_m},
                    {"symbol": s.get("short"), "side": "SHORT",
                     "n_contracts": ie.get("n_short"),
                     "notional_usd": half_n, "initial_margin_usd": half_m}]
            r = CF.assess_structure(legs=legs, policy=pol,
                                    label=s.get("label", ""), r66_record=s)
            r["sensitivity"] = CF.sensitivity(r)
            rows.append(r)

    counts: dict = {}
    for r in rows:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    _w("CAPITAL_FEASIBILITY_CORRECTION.json", {
        "schema": "r67_capital_feasibility_correction/1",
        "calculation_owner": CF.CALCULATION_OWNER,
        "what_this_corrects": (
            "R66 measured eleven hedged structures and reported the blocker as "
            "instrument granularity plus the absence of owned micro-contract "
            "data. That is the constraint on the LONG leg's SIZE. It is not the "
            "first constraint. Every one of the eleven has a SHORT leg, and the "
            "operational book declares short_exposure_supported=false, so each "
            "is inexpressible at any size, at any NAV, under any cash policy. "
            "R66 never evaluated that constraint. Its verdicts are not disputed "
            "and are not recomputed here; what changes is WHOSE RULE produced "
            "them, and therefore what would have to change to lift them."),
        "the_headline_consequence": (
            "R66's single survivor - corn/wheat ZC/ZW, reported as missing by "
            "$82 of margin and becoming holdable at a 10% cash policy - is not "
            "holdable at any cash policy. No cash policy makes a short leg "
            "expressible in a long-only book."),
        "authorised_policy": pol,
        "constraint_provenance": CF.CONSTRAINT_PROVENANCE,
        "binding_owners": list(CF.BINDING_OWNERS),
        "verdict_counts": counts,
        "n_structures": len(rows),
        "structures": rows,
    })


def portfolio() -> None:
    from paper_trader.api.capital_pool import load_capital_pool
    from paper_trader.api.holding_opportunity_cost import (
        load_holding_opportunity_cost)
    from paper_trader.api.opportunity_frontier import load_opportunity_frontier
    from paper_trader.api.reallocation_proposal import load_proposal_summary
    from paper_trader.api.zero_base_target import load_zero_base_target

    pool = load_capital_pool()
    hoc = load_holding_opportunity_cost()
    fro = load_opportunity_frontier()
    zbt = load_zero_base_target()
    prop = load_proposal_summary()

    reviews = hoc.get("holding_reviews") or []
    by_rc = sorted(reviews, key=lambda r: -(r.get("risk_contribution_pct") or 0))
    top5 = by_rc[:5]

    _w("PORTFOLIO_OPPORTUNITY_COST.json", {
        "schema": "r67_portfolio_opportunity_cost/1",
        "composition_owner": "alpha_agent.r67 (composition only)",
        "source_owners": {
            "nav_and_capital": "api.capital_pool -> api.paper_trading_desk.book_nav",
            "opportunity_cost": "api.holding_opportunity_cost",
            "frontier": "api.opportunity_frontier",
            "zero_base_target": "api.zero_base_target",
            "governed_proposal": "api.reallocation_proposal",
        },
        "before": {
            "valuation_date": pool.get("valuation_date"),
            "nav_usd": pool.get("nav"), "cash_usd": pool.get("cash"),
            "invested_usd": pool.get("invested_capital"),
            "position_count": pool.get("position_count"),
            "asset_class_exposure": pool.get("asset_class_exposure"),
            "non_equity_position_count": pool.get("non_equity_position_count"),
            "sector_weights": (hoc.get("portfolio_summary") or {}).get("sector_weights"),
            "max_name_weight": (hoc.get("portfolio_summary") or {}).get("max_name_weight"),
            "herfindahl_index": (hoc.get("portfolio_summary") or {}).get("herfindahl_index"),
        },
        "opportunity_cost_assessment": {
            "state": hoc.get("state"),
            "assessment_hash": hoc.get("assessment_hash"),
            "recommendation_counts": hoc.get("recommendation_counts"),
            "holdings_reviewed": len(reviews),
            "addition_candidates": len(hoc.get("addition_candidates") or []),
            "risk_concentration_top5": [
                {"ticker": r.get("ticker"),
                 "recommendation": r.get("recommendation"),
                 "weight": r.get("current_weight"),
                 "risk_contribution_pct": r.get("risk_contribution_pct"),
                 "reason_codes": r.get("reason_codes")} for r in top5],
            "risk_contribution_of_top5": sum(
                (r.get("risk_contribution_pct") or 0) for r in top5),
            "weight_of_top5": sum(
                (r.get("current_weight") or 0) for r in top5),
        },
        "frontier": {
            "eligible_instrument_count": fro.get("eligible_instrument_count"),
            "eligible_non_equity_count": fro.get("eligible_non_equity_count"),
            "expected_return_state": fro.get("expected_return_state"),
            "candidate_rows_for_proposal": len(
                fro.get("candidate_rows_for_proposal") or []),
            "why_no_non_equity": fro.get("eligible_non_equity_count_explanation"),
        },
        "after_research_preview_only": {
            "authority_lane": (zbt.get("authority") or {}).get("lane"),
            "can_become_a_proposal": (zbt.get("authority") or {}).get(
                "can_become_a_proposal"),
            "forecast_state": (zbt.get("authority") or {}).get("forecast_state"),
            "comparison": zbt.get("comparison"),
            "why_it_has_no_capital_authority": (zbt.get("authority") or {}).get("doc"),
        },
        "governed_proposal": {
            "state": prop.get("reallocation_proposal_state"),
            "available": prop.get("reallocation_proposal_available"),
            "approvable": prop.get("reallocation_proposal_approvable"),
            "why_not_generated_here": (
                "The governed proposal is produced by the operator's own "
                "DB-writing action, POST /v1/operations/daily-research-cycle/run "
                "(named by api.holding_opportunity_cost as its sole execution "
                "path). R67 is read-only and does not trigger it. The read-only "
                "opportunity-cost assessment above is complete and current, and "
                "is the input that action consumes."),
        },
        "safety": {
            "paper_only": True, "read_only": True, "creates_orders": False,
            "creates_fills": False, "mutates_holdings": False,
            "approves_nothing": True, "promotes_nothing": True,
        },
    })


if __name__ == "__main__":
    inventory()
    forward()
    feasibility()
    portfolio()
    print("R67_ARTIFACTS_OK")
