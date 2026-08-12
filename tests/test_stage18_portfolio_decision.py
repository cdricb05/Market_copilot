"""Stage 18 — Active Portfolio Manager: manual portfolio-decision spine + post-close
semantic separation + model-recalibration REVIEW trigger.

Deterministic and hermetic: every test drives the pure module functions with explicit
proposal-summary / artifact dicts and a TEMPORARY decision-ledger root. No live desk,
no real orders/fills, no operational ledger, no provider / prediction, no DB. It proves
the required Stage-18 acceptance matrix:

  * completed operational close + unreviewed MATERIAL proposal must NOT read
    "no action required" (the Today hero surfaces the portfolio review lane);
  * the completed close stays VALID (the hero never mutates overall_state / the close);
  * an IMMATERIAL proposal may read monitor / no-portfolio-action;
  * a MATERIAL proposal requires manual review;
  * an approval binds the EXACT immutable proposal hashes;
  * a STALE proposal is rejected (no write);
  * REJECT / HOLD create no orders; an approval alone creates no fills;
  * duplicate confirmation creates ZERO duplicate records;
  * the order-plan PREVIEW reconciles and creates no order;
  * the model-recalibration REVIEW is deterministic and SEPARATE from portfolio review;
  * negative P&L alone NEVER forces model recalibration.
"""
from __future__ import annotations

import json
from pathlib import Path

from paper_trader.api import portfolio_decision as pd
from paper_trader.api import workflow_state as ws

ROOT = Path(__file__).resolve().parent.parent
PD_SRC = (ROOT / "api" / "portfolio_decision.py").read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Deterministic builders (no live reads, no RNG)
# --------------------------------------------------------------------------- #
def _summary(*, counts=None, phash="hash_current", turnover=0.3679, available=True,
             pid="reap_2026-08-11_book_hash_current", proposed=25):
    return {
        "reallocation_proposal_available": available,
        "reallocation_proposal_state": "DEGRADED",
        "reallocation_proposal_hash": phash,
        "reallocation_proposal_id": pid,
        "reallocation_action_counts": counts if counts is not None else {
            "ADD": 8, "EXIT": 8, "INCREASE": 10, "REDUCE": 7,
            "REPLACE_IN": 0, "REPLACE_OUT": 0, "RETAIN": 0},
        "reallocation_proposed_holding_count": proposed,
        "reallocation_one_way_turnover": turnover,
        "reallocation_estimated_transaction_cost": 89.63,
        "reallocation_score_improvement_net_of_cost": 0.0235,
        "reallocation_data_gaps": ["EXPECTED_RETURN_NOT_CALIBRATED"],
    }


def _artifact(*, phash="hash_current", pid="reap_2026-08-11_book_hash_current",
              allocations=None):
    allocs = allocations if allocations is not None else [
        {"ticker": "APD", "action": "EXIT", "sector": "Materials",
         "current_weight": 0.04, "proposed_weight": 0.0, "delta_weight": -0.04,
         "capital_change": -3900.0, "current_market_value": 3900.0,
         "proposed_market_value": 0.0, "rank": 40},
        {"ticker": "ABNB", "action": "ADD", "sector": "Consumer Discretionary",
         "current_weight": 0.0, "proposed_weight": 0.04, "delta_weight": 0.04,
         "capital_change": 3899.19, "current_market_value": 0.0,
         "proposed_market_value": 3899.19, "rank": 14},
        {"ticker": "ANET", "action": "INCREASE", "sector": "Information Technology",
         "current_weight": 0.044, "proposed_weight": 0.04, "delta_weight": -0.004,
         "capital_change": -390.0, "current_market_value": 4352.0,
         "proposed_market_value": 3962.0, "rank": 4},
    ]
    return {
        "proposal_id": pid,
        "identity": {
            "eligible_market_date": "2026-08-11", "active_book_id": "alpha_paper_book_1",
            "portfolio_state_hash": "psh_1", "universe_scoring_hash": "ush_1",
            "hoc_assessment_hash": "hoc_1", "proposal_hash": phash,
            "allocation_policy_version": "reallocation_allocation_policy.v1"},
        "input_contract": {
            "eligible_market_date": "2026-08-11", "active_book_id": "alpha_paper_book_1",
            "portfolio_state_hash": "psh_1", "universe_scoring_hash": "ush_1",
            "hoc_assessment_hash": "hoc_1",
            "universe_input_contract_hash": "uich_1"},
        "proposal": {
            "proposal_hash": phash, "proposal_state": "DEGRADED",
            "allocations": allocs,
            "turnover": {"one_way_turnover": 0.3679, "gross_sells": 4290.0,
                         "gross_buys": 3899.19, "estimated_transaction_cost": 89.63},
            "portfolio": {"current_cash": 4630.31, "proposed_cash": 2027.24}},
    }


def _flag(flag, actionable, metric="m", value=-0.03, threshold="t"):
    return {"flag": flag, "actionable": actionable, "observation_only": not actionable,
            "metric": metric, "value": value, "threshold": threshold}


def _primary(action_code="MONITOR_PORTFOLIO"):
    return {"action_code": action_code, "label": "Monitor the portfolio",
            "explanation": "The daily cycle is complete ... No action is required ...",
            "severity": "SUCCESS", "destination": "portfolio", "focus": None,
            "headline": "Daily cycle complete for 2026-08-11."}


# --------------------------------------------------------------------------- #
# A. Materiality (from proposal semantics, never P&L)
# --------------------------------------------------------------------------- #
def test_materiality_membership_change_is_material():
    m = pd.assess_materiality(_summary())
    assert m["material"] is True
    assert m["membership_change_count"] == 16 and m["resize_change_count"] == 17
    assert m["basis"] == "PROPOSAL_ACTION_SEMANTICS"


def test_materiality_resize_only_is_material():
    m = pd.assess_materiality(_summary(counts={"ADD": 0, "EXIT": 0, "REPLACE_IN": 0,
                                               "REPLACE_OUT": 0, "INCREASE": 2,
                                               "REDUCE": 1, "RETAIN": 22}))
    assert m["material"] is True and m["membership_change_count"] == 0


def test_materiality_empty_is_immaterial():
    m = pd.assess_materiality(_summary(counts={"ADD": 0, "EXIT": 0, "REPLACE_IN": 0,
                                               "REPLACE_OUT": 0, "INCREASE": 0,
                                               "REDUCE": 0, "RETAIN": 25}))
    assert m["material"] is False


def test_materiality_ignores_pnl():
    s = _summary(counts={"ADD": 0, "EXIT": 0, "REPLACE_IN": 0, "REPLACE_OUT": 0,
                         "INCREASE": 0, "REDUCE": 0, "RETAIN": 25})
    s["daily_pnl"] = -1847.26  # a P&L field must not make an immaterial proposal material
    s["cumulative_pnl"] = -2520.21
    assert pd.assess_materiality(s)["material"] is False


# --------------------------------------------------------------------------- #
# B. Decision-state derivation (the separate portfolio-decision review lane)
# --------------------------------------------------------------------------- #
def test_state_no_active_book():
    lane = pd.derive_decision_state(has_active_book=False, proposal_summary=_summary(),
                                    decision_record=None)
    assert lane["portfolio_decision_state"] == pd.PDS_NO_ACTIVE_BOOK


def test_state_no_proposal():
    lane = pd.derive_decision_state(has_active_book=True,
                                    proposal_summary=_summary(available=False),
                                    decision_record=None)
    assert lane["portfolio_decision_state"] == pd.PDS_NO_PROPOSAL


def test_state_no_material_change():
    lane = pd.derive_decision_state(
        has_active_book=True, decision_record=None,
        proposal_summary=_summary(counts={"ADD": 0, "EXIT": 0, "REPLACE_IN": 0,
                                          "REPLACE_OUT": 0, "INCREASE": 0, "REDUCE": 0,
                                          "RETAIN": 25}))
    assert lane["portfolio_decision_state"] == pd.PDS_NO_MATERIAL_CHANGE
    assert lane["requires_manual_review"] is False


def test_state_review_required_when_material_and_no_decision():
    lane = pd.derive_decision_state(has_active_book=True, proposal_summary=_summary(),
                                    decision_record=None)
    assert lane["portfolio_decision_state"] == pd.PDS_REVIEW_REQUIRED
    assert lane["requires_manual_review"] is True and lane["material"] is True


def test_state_approved_when_decision_bound_to_current_hash():
    rec = {"decision": pd.DECISION_APPROVE, "proposal_hash": "hash_current"}
    lane = pd.derive_decision_state(has_active_book=True, proposal_summary=_summary(),
                                    decision_record=rec)
    assert lane["portfolio_decision_state"] == pd.PDS_APPROVED
    assert lane["decision_is_current"] is True


def test_state_stale_when_decision_bound_to_old_hash():
    rec = {"decision": pd.DECISION_APPROVE, "proposal_hash": "hash_OLD"}
    lane = pd.derive_decision_state(has_active_book=True,
                                    proposal_summary=_summary(phash="hash_current"),
                                    decision_record=rec)
    assert lane["portfolio_decision_state"] == pd.PDS_STALE
    assert lane["requires_manual_review"] is True


# --------------------------------------------------------------------------- #
# C. record_decision — binding, idempotency, staleness, safety (temp ledger root)
# --------------------------------------------------------------------------- #
def test_record_requires_confirmation(tmp_path):
    art = _artifact()
    r = pd.record_decision(decision=pd.DECISION_APPROVE, confirm=None, artifact=art,
                           proposal_summary=_summary(), decision_dir=str(tmp_path),
                           expected_proposal_hash="hash_current")
    assert r["status"] == "DECISION_CONFIRMATION_REQUIRED" and r["recorded"] is False
    assert not (tmp_path / "decisions.json").exists()


def test_record_binds_all_immutable_hashes(tmp_path):
    art = _artifact()
    r = pd.record_decision(decision=pd.DECISION_APPROVE, confirm=pd.CONFIRM_TOKEN,
                           artifact=art, proposal_summary=_summary(),
                           decision_dir=str(tmp_path), expected_proposal_hash="hash_current")
    assert r["status"] == "CREATED" and r["recorded"] is True
    b = r["record"]["binding"]
    for key in ("proposal_id", "proposal_hash", "eligible_market_date", "active_book_id",
                "portfolio_state_hash", "hoc_assessment_hash", "universe_scoring_hash",
                "universe_input_contract_hash"):
        assert b.get(key), "binding missing %s" % key
    assert b["proposal_hash"] == "hash_current"


def test_record_idempotent_no_duplicate(tmp_path):
    art, s = _artifact(), _summary()
    kw = dict(decision=pd.DECISION_APPROVE, confirm=pd.CONFIRM_TOKEN, artifact=art,
              proposal_summary=s, decision_dir=str(tmp_path),
              expected_proposal_hash="hash_current")
    r1 = pd.record_decision(**kw)
    r2 = pd.record_decision(**kw)
    assert r1["status"] == "CREATED" and r2["status"] == "REUSED_EXISTING"
    records = json.loads((tmp_path / "decisions.json").read_text(encoding="utf-8"))
    assert len(records) == 1, "duplicate confirmation must not create a second record"


def test_record_stale_rejected_no_write(tmp_path):
    r = pd.record_decision(decision=pd.DECISION_APPROVE, confirm=pd.CONFIRM_TOKEN,
                           artifact=_artifact(phash="hash_current"),
                           proposal_summary=_summary(phash="hash_current"),
                           decision_dir=str(tmp_path),
                           expected_proposal_hash="hash_STALE_the_operator_reviewed")
    assert r["status"] == pd.PDS_STALE and r["recorded"] is False
    assert not (tmp_path / "decisions.json").exists()


def test_record_reject_and_hold_create_no_orders(tmp_path):
    for dec in (pd.DECISION_REJECT, pd.DECISION_HOLD):
        r = pd.record_decision(decision=dec, confirm=pd.CONFIRM_TOKEN, artifact=_artifact(),
                               proposal_summary=_summary(), decision_dir=str(tmp_path),
                               expected_proposal_hash="hash_current")
        assert r["recorded"] is True
        assert r["created_orders"] is False and r["created_fills"] is False
        assert r["changed_holdings"] is False and r["changed_nav"] is False


def test_record_revision_preserved(tmp_path):
    kw = dict(confirm=pd.CONFIRM_TOKEN, artifact=_artifact(), proposal_summary=_summary(),
              decision_dir=str(tmp_path), expected_proposal_hash="hash_current")
    pd.record_decision(decision=pd.DECISION_APPROVE, **kw)
    r2 = pd.record_decision(decision=pd.DECISION_HOLD, **kw)
    assert r2["status"] == "REVISED" and r2["revised"] is True
    records = json.loads((tmp_path / "decisions.json").read_text(encoding="utf-8"))
    assert len(records) == 2  # both immutable records preserved
    latest = pd.load_decision_record(active_book_id="alpha_paper_book_1",
                                     eligible_market_date="2026-08-11",
                                     decision_dir=str(tmp_path))
    assert latest["decision"] == pd.DECISION_HOLD


def test_record_immaterial_proposal_not_written(tmp_path):
    flat = _summary(counts={"ADD": 0, "EXIT": 0, "REPLACE_IN": 0, "REPLACE_OUT": 0,
                            "INCREASE": 0, "REDUCE": 0, "RETAIN": 25})
    r = pd.record_decision(decision=pd.DECISION_APPROVE, confirm=pd.CONFIRM_TOKEN,
                           artifact=_artifact(), proposal_summary=flat,
                           decision_dir=str(tmp_path), expected_proposal_hash="hash_current")
    assert r["status"] == pd.PDS_NO_MATERIAL_CHANGE and r["recorded"] is False
    assert not (tmp_path / "decisions.json").exists()


# --------------------------------------------------------------------------- #
# D. Read-only order-plan PREVIEW (approval alone creates no orders/fills)
# --------------------------------------------------------------------------- #
def test_order_plan_preview_reconciles_and_creates_no_orders():
    art = _artifact()
    prev = pd.build_order_plan_preview(artifact=art)
    assert prev["creates_orders"] is False and prev["wrote_to_ledger"] is False
    # every allocation with a non-zero capital change appears exactly once as SELL or BUY
    allocs = [a for a in art["proposal"]["allocations"] if a.get("capital_change")]
    assert prev["sell_count"] + prev["buy_count"] == len(allocs)
    assert prev["estimated_proceeds"] == art["proposal"]["turnover"]["gross_sells"]
    assert prev["estimated_purchases"] == art["proposal"]["turnover"]["gross_buys"]
    # SELLs are the exits/reduces (negative capital change)
    assert all(r["capital_change"] < 0 for r in prev["sell_orders"])
    assert all(r["capital_change"] > 0 for r in prev["buy_orders"])
    assert "NEXT_CLOSE" in prev["execution_note"]


def test_load_portfolio_decision_exposes_preview_only_when_approved(tmp_path):
    art = _artifact()
    ps = {"active_book": {"book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1"},
          "dates": {"eligible_market_date": "2026-08-11"}}
    # before any decision -> review required, no preview
    lane0 = pd.load_portfolio_decision(portfolio_state=ps, proposal_summary=_summary(),
                                       artifact=art, decision_dir=str(tmp_path))
    assert lane0["portfolio_decision_state"] == pd.PDS_REVIEW_REQUIRED
    assert lane0["order_plan_preview"] is None
    # record approval, then read -> approved + read-only preview
    pd.record_decision(decision=pd.DECISION_APPROVE, confirm=pd.CONFIRM_TOKEN, artifact=art,
                       proposal_summary=_summary(), decision_dir=str(tmp_path),
                       expected_proposal_hash="hash_current")
    lane1 = pd.load_portfolio_decision(portfolio_state=ps, proposal_summary=_summary(),
                                       artifact=art, decision_dir=str(tmp_path))
    assert lane1["portfolio_decision_state"] == pd.PDS_APPROVED
    assert lane1["order_plan_preview"]["creates_orders"] is False
    assert lane1["read_only"] is True and lane1["created_orders"] is False


# --------------------------------------------------------------------------- #
# E. Model-recalibration REVIEW trigger (Workstream J) — separate + deterministic
# --------------------------------------------------------------------------- #
def test_model_review_healthy_when_signal_flag_observation_only():
    fs = {"research_flags": [_flag("INSUFFICIENT_SAMPLE", False),
                             _flag("RANK_IC_DEGRADATION", False)]}
    mv = ws._derive_model_review(fs)
    assert mv["model_review_state"] == ws.MODEL_HEALTHY
    assert mv["model_review_required"] is False and mv["missing_evidence"]


def test_model_review_triggers_on_actionable_signal_flag():
    fs = {"research_flags": [_flag("RANK_IC_DEGRADATION", True, value=-0.05)]}
    mv = ws._derive_model_review(fs)
    assert mv["model_review_state"] == ws.MODEL_RECALIBRATION_REVIEW
    assert mv["model_review_required"] is True and mv["triggering_flags"]
    # even a triggered review never retrains or promotes automatically
    assert mv["automatic_retraining_allowed"] is False
    assert mv["automatic_promotion_allowed"] is False and mv["review_only"] is True


def test_model_review_negative_pnl_symptoms_never_trigger():
    # actionable performance / drawdown symptoms alone must NOT trigger a model review
    fs = {"research_flags": [_flag("PERSISTENT_NEGATIVE_EXCESS", True, value=-5.6),
                             _flag("DRAWDOWN_REVIEW", True, value=-4.66),
                             _flag("COST_ADJUSTED_ALPHA_NEGATIVE", True)]}
    mv = ws._derive_model_review(fs)
    assert mv["model_review_state"] == ws.MODEL_HEALTHY
    assert mv["model_review_required"] is False


# --------------------------------------------------------------------------- #
# F. Today hero (Workstream C/K) — the post-close semantic separation
# --------------------------------------------------------------------------- #
def _lane(state_summary, decision_record=None):
    return pd.derive_decision_state(has_active_book=True, proposal_summary=state_summary,
                                    decision_record=decision_record)


def test_hero_complete_with_material_unreviewed_says_review_not_all_set():
    hero = ws._build_today_hero(
        overall=ws.DAILY_CYCLE_COMPLETE, primary=_primary(), eligible_date="2026-08-11",
        operational_close_valid=True, latest_close_date="2026-08-11",
        portfolio_decision_lane=_lane(_summary()),
        model_review=ws._derive_model_review({"research_flags": []}))
    assert hero["focus_lane"] == "PORTFOLIO"
    assert hero["severity"] == "ATTENTION"
    assert "manual review" in hero["headline"].lower()
    assert "no action" not in hero["headline"].lower()
    assert hero["cta_action_code"] == ws.ACTION_REVIEW_PROPOSED_PORTFOLIO
    assert hero["cta_creates_orders"] is False
    # the operational lane is STILL reported complete (the close is not invalidated)
    assert hero["operational_lane"]["complete"] is True


def test_hero_complete_no_material_change_says_monitor():
    flat = _summary(counts={"ADD": 0, "EXIT": 0, "REPLACE_IN": 0, "REPLACE_OUT": 0,
                            "INCREASE": 0, "REDUCE": 0, "RETAIN": 25})
    hero = ws._build_today_hero(
        overall=ws.DAILY_CYCLE_COMPLETE, primary=_primary(), eligible_date="2026-08-11",
        operational_close_valid=True, latest_close_date="2026-08-11",
        portfolio_decision_lane=_lane(flat),
        model_review=ws._derive_model_review({"research_flags": []}))
    assert hero["focus_lane"] == "OPERATIONAL"
    assert hero["cta_action_code"] == "MONITOR_PORTFOLIO"


def test_hero_operational_incomplete_mirrors_primary():
    primary = {"action_code": "RUN_DAILY_CLOSE", "label": "Run the Daily Close",
               "explanation": "...", "severity": "ATTENTION", "destination": "daily-workflow",
               "focus": None, "headline": "Ready for the Daily Close."}
    hero = ws._build_today_hero(
        overall=ws.READY_FOR_DAILY_CLOSE, primary=primary, eligible_date="2026-08-11",
        operational_close_valid=False, latest_close_date=None,
        portfolio_decision_lane=_lane(_summary()),  # even a material proposal must not
        model_review=ws._derive_model_review({"research_flags": []}))  # outrank an open cycle
    assert hero["focus_lane"] == "OPERATIONAL"
    assert hero["cta_action_code"] == "RUN_DAILY_CLOSE"


def test_hero_model_review_surfaces_only_when_no_portfolio_action():
    flat = _summary(counts={"ADD": 0, "EXIT": 0, "REPLACE_IN": 0, "REPLACE_OUT": 0,
                            "INCREASE": 0, "REDUCE": 0, "RETAIN": 25})
    hero = ws._build_today_hero(
        overall=ws.DAILY_CYCLE_COMPLETE, primary=_primary(), eligible_date="2026-08-11",
        operational_close_valid=True, latest_close_date="2026-08-11",
        portfolio_decision_lane=_lane(flat),
        model_review=ws._derive_model_review(
            {"research_flags": [_flag("RANK_IC_DEGRADATION", True)]}))
    assert hero["focus_lane"] == "MODEL"
    assert hero["cta_action_code"] == "REVIEW_MODEL_RECALIBRATION"
    # portfolio and model are independent lanes, both present
    assert hero["portfolio_lane"]["state"] == pd.PDS_NO_MATERIAL_CHANGE
    assert hero["model_lane"]["review_required"] is True


def test_hero_lanes_are_independent():
    hero = ws._build_today_hero(
        overall=ws.DAILY_CYCLE_COMPLETE, primary=_primary(), eligible_date="2026-08-11",
        operational_close_valid=True, latest_close_date="2026-08-11",
        portfolio_decision_lane=_lane(_summary()),
        model_review=ws._derive_model_review({"research_flags": []}))
    assert hero["lanes_are_independent"] is True
    assert {hero["operational_lane"]["lane"], hero["portfolio_lane"]["lane"],
            hero["model_lane"]["lane"]} == {"OPERATIONAL", "PORTFOLIO", "MODEL"}


# --------------------------------------------------------------------------- #
# G. Architecture / safety invariants
# --------------------------------------------------------------------------- #
def test_load_reports_full_safety_block():
    ps = {"active_book": {"book_id": "alpha_paper_book_1"},
          "dates": {"eligible_market_date": "2026-08-11"}}
    out = pd.load_portfolio_decision(portfolio_state=ps, proposal_summary=_summary(),
                                     artifact=_artifact(), decision_dir="/nonexistent_ro")
    for k in ("read_only", "preview_only", "manual_review", "paper_only", "automation_off"):
        assert out[k] is True
    for k in ("created_orders", "created_fills", "changed_holdings", "changed_nav",
              "broker_enabled", "live_orders_enabled", "automatic_promotion_allowed"):
        assert out[k] is False


def test_owner_source_creates_no_orders_or_broker_path():
    # the decision owner must never import/call the desk order/fill WRITERS (execution
    # stays the existing paper-desk owner; this module only records a decision + previews)
    for writer in ("generate_orders", "confirm_orders", "settle_due_orders",
                   "confirm_order_plan", "import paper_trading_desk"):
        assert writer not in PD_SRC, "decision owner must not call %s" % writer
    # 'broker' must appear ONLY in negative safety context (broker_enabled: False / NO BROKER)
    import re
    for m in re.finditer(r"broker", PD_SRC.lower()):
        ctx = PD_SRC.lower()[max(0, m.start() - 16):m.start() + 16]
        assert ("broker_enabled" in ctx or "no broker" in ctx), ctx


def test_decision_vocabulary_frozen():
    assert pd.DECISION_VOCAB == ("APPROVE_FOR_PAPER_REBALANCE", "REJECT", "HOLD")
    assert pd.CONFIRM_TOKEN == "CONFIRM_PORTFOLIO_REBALANCE_DECISION"
    assert set(pd.DECISION_STATE_VOCAB) >= {
        pd.PDS_REVIEW_REQUIRED, pd.PDS_APPROVED, pd.PDS_REJECTED, pd.PDS_HELD,
        pd.PDS_STALE, pd.PDS_NO_MATERIAL_CHANGE}
