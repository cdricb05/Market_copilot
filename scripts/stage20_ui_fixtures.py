"""scripts/stage20_ui_fixtures.py — Stage 20 hermetic UI-acceptance fixtures.

Generates the EXACT canonical backend payload the browser acceptance run renders, by
calling the REAL contract builders (``api.portfolio_reassessment`` over the pure
``engine.portfolio_reassessment`` kernel) with synthetic inputs.

Strictly offline and read-only: it opens no live store, calls no provider, contacts no
prediction service, creates no order and writes only the fixture file it is given. It
exists so the Playwright acceptance run never touches live operational state — in
particular the 29 SUBMITTED NEXT_CLOSE paper orders of the current real rebalance.

Usage:
    python scripts/stage20_ui_fixtures.py --out <path.json>
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from paper_trader.api import portfolio_reassessment as prs
from paper_trader.engine import portfolio_reassessment as kernel

BOOK = "alpha_paper_book_1"
DATE = "2026-08-12"
PREV = "2026-08-11"
NAV = 100000.0

# The live Stage-19 execution facts (SHAPE only — no live store is read).
LIVE_PLAN = "rbop_2026-08-12_alpha_paper_book_1_1a198f560cca"
LIVE_SUBMITTED = 29


# --------------------------------------------------------------------------- #
# Synthetic Slice-6 assessment inputs (structurally faithful, deterministic).
# --------------------------------------------------------------------------- #
def _review(ticker, **kw):
    base = {
        "ticker": ticker, "sector": "Tech", "current_quantity": 100,
        "current_weight": 0.04, "market_value": 0.04 * NAV,
        "current_rank": 10, "previous_rank": 10, "rank_change": 0,
        "current_score": 0.80, "score_components": {}, "signal_strength": 0.80,
        "deterioration_state": "STABLE", "deterioration_reason_codes": [],
        "return_5d": 0.01, "return_20d": 0.02, "return_60d": 0.05,
        "volatility_20d": 0.20, "volatility_60d": 0.22, "drawdown_60d": -0.05,
        "risk_contribution_pct": 0.04, "concentration_contribution": 0.04,
        "median_dollar_volume_20d": 5.0e7, "estimated_days_to_liquidate": 0.1,
        "liquidity_state": "LIQUID",
        "strongest_replacement_ticker": None, "replacement_rank": None,
        "replacement_score": None, "replacement_sector": None,
        "gross_score_improvement": None, "risk_adjusted_improvement": None,
        "switching_cost_bps": 25.0, "switching_cost_usd": 100.0,
        "net_improvement": None, "recommendation": "HOLD",
        "recommendation_confidence": "HIGH", "reason_codes": [], "explanation": "",
        "required_data_complete": True,
    }
    base.update(kw)
    return base


def _replace(ticker, *, gross, net, rank=62, prev=18, sector="Fin", weight=0.04):
    return _review(ticker, sector=sector, current_weight=weight,
                   market_value=weight * NAV, current_rank=rank, previous_rank=prev,
                   rank_change=prev - rank, recommendation="REPLACE",
                   deterioration_state="DETERIORATING",
                   deterioration_reason_codes=["RANK_WORSENED"],
                   drawdown_60d=-0.184, strongest_replacement_ticker="CVS",
                   replacement_rank=9, replacement_score=0.95,
                   replacement_sector="Health", gross_score_improvement=gross,
                   risk_adjusted_improvement=gross, net_improvement=net,
                   switching_cost_usd=weight * NAV * 0.0025)


#: A realistic sector spread so the concentration / sector-cap arithmetic is meaningful.
_SECTORS = ("Tech", "Fin", "Health", "Ind", "Energy")


def _filler(i):
    """One quiet, retained holding with a realistic sector."""
    return _review("H%02d" % i, sector=_SECTORS[i % len(_SECTORS)], current_rank=i + 1)


def _hoc(reviews, *, state="READY", gaps=None, eligible=DATE):
    invested = sum(r["market_value"] for r in reviews)
    sw: dict[str, float] = {}
    for r in reviews:
        sw[r["sector"]] = sw.get(r["sector"], 0.0) + r["current_weight"]
    return {
        "schema_version": "holding_opportunity_cost.v1",
        "eligible_market_date": eligible, "active_book_id": BOOK,
        "assessment_state": state, "assessment_hash": "hoc_fixture_%s" % state.lower(),
        "policy": {"policy_version": "hoc_decision_policy.v1"},
        "portfolio_summary": {
            "nav": NAV, "cash": NAV - invested, "invested_value": invested,
            "holdings_count": len(reviews),
            "max_name_weight": max(r["current_weight"] for r in reviews),
            "max_name_ticker": reviews[0]["ticker"],
            "max_sector_weight": max(sw.values()), "max_sector": max(sw, key=sw.get),
            "sector_weights": sw,
            "herfindahl_index": sum(r["current_weight"] ** 2 for r in reviews),
            "portfolio_variance_daily": 0.0001,
            "risk_contribution_state": "AVAILABLE"},
        "recommendation_counts": {"HOLD": 0, "REDUCE": 0, "EXIT": 0, "REPLACE": 0,
                                  "ADD": 0},
        "holding_reviews": reviews,
        "addition_candidates": [
            {"ticker": "CVS", "rank": 9, "score": 0.95, "combined_score": 0.95,
             "sector": "Health", "recommendation": "ADD"},
            {"ticker": "ADBE", "rank": 12, "score": 0.93, "combined_score": 0.93,
             "sector": "Tech", "recommendation": "ADD"},
            {"ticker": "LIN", "rank": 17, "score": 0.91, "combined_score": 0.91,
             "sector": "Materials", "recommendation": "ADD"}],
        "diagnostics": {"eligible_universe_size": 503},
        "data_quality": {"data_gaps": list(gaps or [])},
        "provenance": {"portfolio_state_hash": "ps_fixture",
                       "corporate_actions_hash": None,
                       "universe_scoring_hash": "us_fixture"},
    }


def _ps(*, eligible=DATE):
    # The corporate-action registry is supplied EXPLICITLY as the empty registry, so the
    # read path never falls back to reading the live registry file. Hermetic by
    # construction: this generator opens no operational store.
    from paper_trader.api import corporate_actions as ca
    return {"dates": {"eligible_market_date": eligible, "valuation_date": eligible},
            "active_book": {"book_id": BOOK, "book_label": "Alpha Paper Book #1",
                            "status": "ACTIVE", "initialized": True,
                            "holdings_count": 25},
            "capital": {"nav": NAV, "cash": 0.0},
            "state_hash": "ps_fixture",
            "corporate_actions": {
                "registry_fingerprint": ca.EMPTY_REGISTRY_FINGERPRINT, "actions": []}}


def _scoring():
    return {"output_hash": "us_fixture", "input_contract_hash": "uic_fixture",
            "strategy_id": "fundamental_momentum_50_50_v1", "strategy_version": "v1",
            "primary_model_id": "fundamental_momentum_50_50_v1",
            "champion_model_id": "composite_sn", "model_registry_version": "29",
            "universe_id": "phase8v_combined_eodhd_price_fundamentals_universe"}


def _fresh(rows=None):
    default = [
        {"source_id": "owned_daily_prices", "status": "FRESH", "as_of_date": DATE,
         "cadence": "DAILY", "required_for_portfolio_reassessment": True,
         "authoritative_owner": "api.operational_book",
         "reason": "Current through the eligible session %s." % DATE,
         "expected_through_date": DATE},
        {"source_id": "price_score_refresh", "status": "FRESH", "as_of_date": DATE,
         "cadence": "DAILY", "required_for_portfolio_reassessment": True,
         "authoritative_owner": "api.multi_horizon_engine",
         "reason": "Current through the eligible session %s." % DATE,
         "expected_through_date": DATE},
        {"source_id": "fundamental_quarterly", "status": "NOT_DUE",
         "as_of_date": "2026-05-22", "cadence": "QUARTERLY",
         "required_for_portfolio_reassessment": False,
         "authoritative_owner": "api.multi_horizon_engine",
         "reason": "Current under its own quarterly cadence.",
         "expected_through_date": "2026-05-22"},
    ]
    return {"eligible_market_date": DATE, "source_freshness": rows or default}


#: Every artifact built during this run, keyed by the scenario payload identity, so the
#: SEED mode can persist the exact same immutable artifact the fixture describes.
_ARTIFACTS: list[dict] = []


def _payload(reviews, *, freshness=None, hoc_state="READY", history=None,
             execution=None, stale=None):
    """Build the REAL read-contract payload the UI renders."""
    hoc = _hoc(reviews, state=hoc_state)
    run = prs.run_reassessment(input_contract=prs.build_input_contract(
        portfolio_state=_ps(), scoring=_scoring(), hoc_assessment=hoc,
        freshness=freshness or _fresh(), recent_change_history=history or [],
        corporate_action_stale=stale, policy=prs.resolve_policy()))
    artifact = {
        "reassessment_id": "prs_%s_%s_fixture" % (DATE, BOOK),
        "schema_version": prs.SCHEMA_VERSION,
        "composition_owner": prs.COMPOSITION_OWNER,
        "calculation_owner": prs.CALCULATION_OWNER,
        "generated_at": "%sT21:05:00Z" % DATE,
        "identity": prs.artifact_identity(input_contract=run["input_contract"],
                                          result=run["reassessment"]),
        "input_contract": {}, "reassessment": run["reassessment"]}
    _ARTIFACTS.append({"artifact": artifact, "execution": execution or {}})
    return prs.load_portfolio_reassessment(
        portfolio_state=_ps(), artifact=artifact, rebalance_state=execution or {})


def _quiet_book():
    return [_filler(i) for i in range(25)]


def build() -> dict:
    # --- 1. PORTFOLIO CURRENT — nothing needs attention -------------------- #
    s1 = _payload(_quiet_book())

    # --- 2. Two holdings deteriorating but NO economic action -------------- #
    #     Attractive per-name replacements whose portfolio economics do not clear
    #     the hurdle -> CHANGE_CANDIDATE, no proposal, no action.
    two = [_replace("MRNA", gross=0.30, net=0.28, rank=58, prev=14),
           _replace("PARA", gross=0.28, net=0.26, rank=71, prev=22)]
    s2 = _payload(two + [_filler(i) for i in range(23)],
                  history=None)

    # --- 3. Strong replacement -> PROPOSAL READY --------------------------- #
    strong = [_replace("MRNA", gross=1.20, net=1.15, rank=58, prev=14),
              _replace("PARA", gross=1.10, net=1.05, rank=71, prev=22),
              _replace("WBA", gross=1.05, net=1.00, rank=88, prev=30),
              _replace("ENPH", gross=1.00, net=0.95, rank=95, prev=33)]
    s3 = _payload(strong + [_filler(i) for i in range(21)])

    # --- 4. Data-blocked reassessment -------------------------------------- #
    blocked_rows = [
        {"source_id": "price_score_refresh", "status": "MISSING", "as_of_date": None,
         "cadence": "DAILY", "required_for_portfolio_reassessment": True,
         "authoritative_owner": "api.multi_horizon_engine",
         "reason": "Source date is absent.", "expected_through_date": DATE},
        {"source_id": "owned_daily_prices", "status": "FRESH", "as_of_date": DATE,
         "cadence": "DAILY", "required_for_portfolio_reassessment": True,
         "authoritative_owner": "api.operational_book", "reason": "current",
         "expected_through_date": DATE}]
    s4 = _payload(_quiet_book(), freshness={"eligible_market_date": DATE,
                                            "source_freshness": blocked_rows})

    # --- 5. Stage-19 execution pending — it keeps operator precedence ------ #
    live = {"rebalance_state": "ORDER_PLAN_CONFIRMED_PAPER_EXECUTION_PENDING",
            "execution_summary": {"order_plan_id": LIVE_PLAN,
                                  "submitted_count": LIVE_SUBMITTED,
                                  "filled_count": 0, "cancelled_count": 0}}
    s5 = _payload(strong + [_filler(i) for i in range(21)],
                  execution=live)

    # --- 6. Executed / reconciled, then the NEXT reassessment -------------- #
    #     The churn controls now protect the names that just changed, so the very next
    #     reassessment does NOT immediately reverse them.
    hist = [{"eligible_market_date": PREV, "ticker": t, "direction": "IN"}
            for t in ("MRNA", "PARA", "WBA", "ENPH")]
    s6 = _payload(strong + [_filler(i) for i in range(21)],
                  history=hist,
                  execution={"rebalance_state": "PAPER_EXECUTED_RECONCILED",
                             "execution_summary": {"submitted_count": 0,
                                                   "filled_count": 29}})

    return {
        "generated_by": "scripts/stage20_ui_fixtures.py",
        "owner": prs.COMPOSITION_OWNER,
        "calculation_owner": kernel.CALCULATION_OWNER,
        "hermetic": True, "live_store_read": False, "provider_called": False,
        "scenarios": {
            "scenario_1_portfolio_current": {
                "title": "Portfolio current — no change is economically justified",
                "expect_state": kernel.STATE_NO_CHANGE,
                "expect_primary_action": None,
                "expect_attention": 0,
                "payload": s1},
            "scenario_2_deteriorating_no_action": {
                "title": "Two holdings deteriorating but no economic action",
                "expect_state": kernel.STATE_CHANGE_CANDIDATE,
                "expect_primary_action": None,
                "expect_attention": 2,
                "payload": s2},
            "scenario_3_proposal_review": {
                "title": "Strong replacement — one proposal review action",
                "expect_state": kernel.STATE_PROPOSAL_READY,
                "expect_primary_action": "REVIEW_PORTFOLIO_PROPOSAL",
                "expect_attention": 4,
                "payload": s3},
            "scenario_4_data_blocked": {
                "title": "Data-blocked reassessment — named blocker, no action",
                "expect_state": kernel.STATE_BLOCKED_DATA,
                "expect_primary_action": None,
                "expect_attention": 0,
                "payload": s4},
            "scenario_5_execution_pending": {
                "title": "Stage-19 execution pending — it keeps operator precedence",
                "expect_state": kernel.STATE_PROPOSAL_READY,
                "expect_primary_action": None,
                "expect_execution_precedence": True,
                "expect_attention": 4,
                "payload": s5},
            "scenario_6_reconciled_then_next": {
                "title": "Executed/reconciled, then the next reassessment (churn holds)",
                "expect_state": kernel.STATE_NO_CHANGE,
                "expect_primary_action": None,
                "expect_attention": 0,
                "payload": s6},
        },
    }


def seed(*, reassessment_dir, scenario: str, book_id: str, eligible) -> dict:
    """Persist ONE scenario's immutable artifact into a HERMETIC reassessment root, keyed
    to the (book, eligible session) the hermetic backend actually resolves.

    This is how the browser acceptance drives the REAL read route end to end instead of
    stubbing the renderer. It writes only inside the throwaway acceptance root supplied by
    the caller; it can never reach an operational store.
    """
    data = build()
    sc = data["scenarios"][scenario]
    idx = list(data["scenarios"]).index(scenario)
    art = dict(_ARTIFACTS[idx]["artifact"])
    art["reassessment_id"] = "prs_%s_%s_%s" % (eligible or "nodate", book_id, scenario[:8])
    art["identity"] = dict(art["identity"], active_book_id=book_id,
                           eligible_market_date=eligible)
    art["reassessment"] = dict(art["reassessment"], active_book_id=book_id,
                               eligible_market_date=eligible)
    root = Path(reassessment_dir)
    (root / "artifacts").mkdir(parents=True, exist_ok=True)
    path = root / "artifacts" / ("%s.json" % art["reassessment_id"])
    path.write_text(json.dumps(art, indent=2, sort_keys=True, default=str),
                    encoding="utf-8")
    key = "%s|%s" % (book_id or "?", eligible or "?")
    index = {}
    ipath = root / "index.json"
    if ipath.exists():
        try:
            index = json.loads(ipath.read_text(encoding="utf-8"))
        except ValueError:
            index = {}
    index[key] = {"artifact_id": art["reassessment_id"], "path": str(path),
                  "reassessment_hash": art["identity"]["reassessment_hash"],
                  "eligible_market_date": eligible, "active_book_id": book_id,
                  "generated_at": art["generated_at"]}
    ipath.write_text(json.dumps(index, indent=2, sort_keys=True), encoding="utf-8")
    return {"scenario": scenario, "title": sc["title"], "index_key": key,
            "artifact_path": str(path),
            "expect_state": sc["expect_state"],
            "expect_primary_action": sc["expect_primary_action"],
            "expect_attention": sc["expect_attention"],
            "execution": _ARTIFACTS[idx]["execution"]}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Stage 20 hermetic UI fixtures (read-only).")
    ap.add_argument("--out", help="Fixture JSON output path.")
    ap.add_argument("--seed-dir", help="HERMETIC reassessment root to seed (acceptance).")
    ap.add_argument("--scenario", help="Scenario key to seed.")
    ap.add_argument("--book-id", default=BOOK)
    ap.add_argument("--eligible", default=None)
    args = ap.parse_args(argv)
    if args.seed_dir:
        if not args.scenario:
            ap.error("--scenario is required with --seed-dir")
        info = seed(reassessment_dir=args.seed_dir, scenario=args.scenario,
                    book_id=args.book_id, eligible=args.eligible)
        print(json.dumps(info, indent=2, sort_keys=True, default=str))
        return 0
    if not args.out:
        ap.error("--out is required")
    data = build()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2, sort_keys=True, default=str) + "\n",
                   encoding="utf-8")
    for k, v in data["scenarios"].items():
        print("%-38s state=%-28s action=%s attention=%s"
              % (k, v["payload"]["state"],
                 v["payload"]["presentation"].get("primary_action"),
                 (v["payload"].get("attention") or {}).get("count")))
    print("fixtures written to: %s" % out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
