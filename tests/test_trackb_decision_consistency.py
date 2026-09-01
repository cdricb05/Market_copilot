r"""Track B — DECISION CONSISTENCY: a canonical HOLD is presented as HOLD everywhere.

What this proves (mandate scenarios A-K, 2026-08-31)
----------------------------------------------------
On the first real post-R50/51/52 Portfolio Cycle the constrained owner concluded
HOLD_CURRENT_BOOK (net improvement below the switching hurdle) and the direct
decision endpoint agreed — while the workflow said MANUAL_REVIEW_REQUIRED /
"a portfolio change is economically justified", the embedded canonical decision
said PROPOSAL_REVIEW_REQUIRED / approvable=true, and the operator presentation +
decision snapshot said REALLOCATE. Two reconstructions caused it:

  1. api.daily_action_gate never forwarded the Release-47 outcome fields from the
     canonical proposal summary, so every workflow consumer read outcome=None and
     the decision lane fell through to PROPOSAL_REVIEW_REQUIRED.
  2. api.workflow_state.portfolio_attention keyed its proposal-review verdict off
     the reassessment's UPSTREAM ``proposal_required`` request alone — proposal
     existence was read as "proposal economically qualifies for review".

The ONE canonical decision chain is: engine.constrained_reallocation outcome ->
api.reallocation_proposal summary -> api.portfolio_decision lane -> every
downstream surface (workflow / normal cycle / snapshot / presentation) CONSUMES
that decision and re-derives nothing.

A. proposal READY + constrained HOLD -> workflow never says MANUAL_REVIEW_REQUIRED.
B. same state -> the presentation the snapshot serves never says REALLOCATE.
C. same state -> operator presentation says HOLD, no action.
D. same state -> approvable=false on every surface (and invariant I8 is ARMED).
E. same state -> no execution action offered anywhere.
F. a genuinely qualifying PROPOSAL_READY still raises manual review.
G. a TRUE_BLOCKER / blocked reassessment remains blocked.
H. the immutable proposal artifact stays visible, auditable evidence under HOLD.
I. completed Daily Close / DRC artifacts are never rerun by the orchestrator.
J. nothing here mutates orders / plans / fills / holdings / cash / NAV.
K. a client timeout is recovered by READING status; a repeated POST is idempotent.

Everything is hermetic and read-only: every load_workflow_state seam is injected,
no provider, no prediction, no store, no order, no close, no research run.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path

from paper_trader.api import daily_action_gate as dag
from paper_trader.api import operator_presentation as op
from paper_trader.api import portfolio_cycle as pc
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import portfolio_reassessment as prs_api
from paper_trader.api import workflow_state as ws

REPO = Path(__file__).resolve().parents[1]

# --------------------------------------------------------------------------- #
# Hermetic fixtures — the exact live 2026-08-31 shape, every seam injected.
# --------------------------------------------------------------------------- #
_OPB = {"operational_book": {
    "book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1",
    "current_status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
    "nav_as_of_date": "2026-08-31", "desk_mark_date": "2026-08-31",
    "latest_desk_mark_date": "2026-08-31", "nav": 99113.22, "cash": 4482.71,
    "holdings_count": 25, "pending_order_count": 0,
    "current_target": {"alpha_market_date": "2026-08-31",
                       "latest_completed_market_date": "2026-08-31"}}}
_INPUTS = {"market_as_of_date": "2026-08-31", "momentum_month": "2026-08",
           "fundamental_as_of_date": "2026-05-22"}
_DESK = {"series": {"SPY": [["2026-08-28", 770.0], ["2026-08-31", 772.0]]},
         "latest_completed_date": "2026-08-31"}
_FWD = {"latest_snapshot_date": "2026-08-31", "snapshot_count": 1,
        "evidence_state": "FORWARD_EVIDENCE_OK", "active_book": {},
        "shadow_books": []}
_CLOSE_DONE = {"market_date": "2026-08-31", "done": True,
               "final_close_status": "DAILY_CLOSE_COMPLETE_HOLD"}
_TR = {"dates": {"alpha_market_date": "2026-08-31"}}

#: The Release-47 fields the fixed gate forwards verbatim from the canonical
#: proposal summary (the exact live HOLD economics).
_HOLD_OUTCOME_FIELDS = {
    "reallocation_proposal_withheld": False,
    "reallocation_withheld_reasons": [],
    "reallocation_proposal_approvable": False,
    "reallocation_outcome": "HOLD_CURRENT_BOOK",
    "reallocation_outcome_headline": "HOLD THE CURRENT BOOK",
    "reallocation_outcome_reason_codes": ["NET_IMPROVEMENT_BELOW_SWITCHING_HURDLE"],
    "reallocation_constraints_reshaped": [],
    "reallocation_constraint_reoptimized": False,
    "reallocation_feasible_target_exists": True,
    "reallocation_switching_hurdle": 0.05,
    "reallocation_clears_switching_hurdle": False,
}
_READY_OUTCOME_FIELDS = {
    **_HOLD_OUTCOME_FIELDS,
    "reallocation_proposal_approvable": True,
    "reallocation_outcome": "PROPOSAL_READY",
    "reallocation_outcome_headline": "PORTFOLIO PROPOSAL READY",
    "reallocation_outcome_reason_codes": [],
    "reallocation_clears_switching_hurdle": True,
}


def _gate(**kw):
    g = {"latest_completed_market_date": "2026-08-31", "outcome": "NO_ACTION_TODAY",
         "target_state": "CURRENT_ALIGNED",
         "next_scheduled_full_review": "2026-09-15", "scheduled_review_due": False,
         "opportunity_cost_available": True, "opportunity_cost_state": "READY",
         "opportunity_cost_assessment_hash": "H1",
         "opportunity_cost_recommendation_counts": {"HOLD": 25},
         "opportunity_cost_data_gaps": [],
         "opportunity_cost_bound_eligible_market_date": "2026-08-31",
         "opportunity_cost_bound_active_book_id": "alpha_paper_book_1",
         "reallocation_proposal_available": True,
         "reallocation_proposal_state": "READY",
         "reallocation_proposal_hash": "P1",
         "reallocation_proposal_id": "prop_2026-08-31",
         "reallocation_action_counts": {"RETAIN": 10, "EXIT": 7, "ADD": 8,
                                        "REDUCE": 6, "INCREASE": 6},
         "reallocation_proposed_holding_count": 26,
         "reallocation_one_way_turnover": 0.121918,
         "reallocation_estimated_transaction_cost": 30.21,
         "reallocation_score_improvement_net_of_cost": -0.00699,
         "reallocation_data_gaps": [],
         "reallocation_bound_hoc_assessment_hash": "H1",
         "reallocation_bound_eligible_market_date": "2026-08-31",
         "reallocation_bound_active_book_id": "alpha_paper_book_1",
         "reallocation_proposal_stale": False,
         "reallocation_proposal_stale_reason": None,
         **_HOLD_OUTCOME_FIELDS}
    g.update(kw)
    return g


_PRS = {"reassessment_available": True, "reassessment_state": "PROPOSAL_READY",
        "proposal_required": True, "reassessment_hash": "R1",
        "hoc_assessment_hash": "H1", "holdings_evaluated": 25,
        "attention_count": 3, "blockers": [],
        "expected_net_improvement": -0.000894,
        "expected_one_way_turnover": 0.121918,
        "expected_transaction_cost_usd": 30.21,
        "explanation": "hermetic: the reassessment asked for a target"}


def _load(**kw):
    args = dict(reference_today="2026-09-01",
                operational=copy.deepcopy(_OPB), inputs=dict(_INPUTS),
                daily_status={"latest_valid_mark_date": "2026-08-31"},
                desk_marks=copy.deepcopy(_DESK), close_progress=dict(_CLOSE_DONE),
                forward_status=copy.deepcopy(_FWD), gate=_gate(),
                target_readiness=copy.deepcopy(_TR),
                research_cycle={"state": "COMPLETE", "blockers": [],
                                "governed_research_evidence_current": True,
                                "run_id": "drc_2026-08-31_2995fdc283b9"},
                reassessment_summary=dict(_PRS), decision_record=None)
    args.update(kw)
    return ws.load_workflow_state(**args)


_CONSTRAINED_HOLD = {
    "outcome": "HOLD_CURRENT_BOOK",
    "execution": {"rebalance_state": "NO_PLAN"},
    "switching_economics": {"clears_switching_hurdle": False,
                            "switching_hurdle": 0.05},
    "best_feasible_target": {"allocations": []},
    "reallocation_outcome": {
        "reason_codes": ["NET_IMPROVEMENT_BELOW_SWITCHING_HURDLE"],
        "feasible_target_exists": True}}


# =========================================================================== #
# A. HOLD_CURRENT_BOOK never raises a rebalance manual review
# =========================================================================== #
def test_a1_hold_outcome_is_not_manual_review_required():
    wf = _load()
    assert wf["overall_state"] == "DAILY_CYCLE_COMPLETE"
    att = wf["portfolio_reassessment"]["portfolio_attention"]
    assert att["review_required"] is False
    assert att["review_reason"] is None
    assert att["proposal_review_settled_by_decision"] is True
    assert att["decision_lane_state"] == "HOLD_CURRENT_BOOK"
    cmd = wf["operator_command"]
    assert cmd["task"] == "Monitor the portfolio."
    # The exact false sentence the live payload showed must be gone everywhere.
    blob = json.dumps(wf, default=str)
    assert "economically justified — review it" not in blob


def test_a2_the_attention_verdict_consumes_the_canonical_decision_lane():
    # Settled lane states suppress the proposal review; unknown lane keeps the
    # pre-existing fail-toward-review behaviour; a constraint breach is NEVER
    # suppressed by a settled proposal decision.
    for settled in ("HOLD_CURRENT_BOOK", "CHANGE_CANDIDATE_WITHHELD",
                    "NO_MATERIAL_CHANGE"):
        v = ws.portfolio_attention(
            reassessment_state="PROPOSAL_READY", proposal_required=True,
            decision_lane_state=settled)
        assert v["review_required"] is False, settled
        assert v["proposal_review_settled_by_decision"] is True, settled
    for open_state in (None, "PROPOSAL_REVIEW_REQUIRED",
                       "STALE_PROPOSAL_REVIEW_REQUIRED", "PROPOSAL_HELD",
                       "PORTFOLIO_DECISION_NO_PROPOSAL",
                       "PORTFOLIO_DECISION_UNAVAILABLE"):
        v = ws.portfolio_attention(
            reassessment_state="PROPOSAL_READY", proposal_required=True,
            decision_lane_state=open_state)
        assert v["review_required"] is True, open_state
    breach = ws.portfolio_attention(
        reassessment_state="MANUAL_REVIEW_REQUIRED", proposal_required=False,
        held_name_breaches=["AMD:RISK_CONTRIBUTION_BREACH"],
        decision_lane_state="HOLD_CURRENT_BOOK")
    assert breach["review_required"] is True
    assert breach["review_reason"] == ws.REVIEW_REASON_CONSTRAINT_BREACH


def test_a3_the_gate_forwards_the_release47_outcome_verbatim():
    g = dag.load_daily_action_gate(
        today="2026-08-31", current={"status": "MHZ_INPUTS_UNAVAILABLE"},
        operational={},
        opportunity_cost={"opportunity_cost_available": True,
                          "opportunity_cost_state": "READY"},
        reallocation={"reallocation_proposal_available": True,
                      "reallocation_proposal_state": "READY",
                      **_HOLD_OUTCOME_FIELDS})
    for k, v in _HOLD_OUTCOME_FIELDS.items():
        assert g[k] == v, k


# =========================================================================== #
# B. The presentation the decision snapshot serves never says REALLOCATE
# =========================================================================== #
def test_b_snapshot_presentation_decision_is_hold_not_reallocate():
    wf = _load()
    # api.decision_snapshot serves references.presentation_decision verbatim from
    # presentation.portfolio_decision.state — this is that exact object.
    pd = op._portfolio_decision(wf, _CONSTRAINED_HOLD, {}, {"historical": False})
    assert pd["state"] == "HOLD"
    assert pd["state"] != "REALLOCATE"
    assert pd["headline"] == "HOLD CURRENT PORTFOLIO"
    cpd = wf["canonical_portfolio_decision"]
    assert cpd["state"] == "HOLD_CURRENT_BOOK"
    assert cpd["headline"] == "HOLD THE CURRENT BOOK"
    assert cpd["hold_current_book"] is True
    assert cpd["reallocation_outcome"] == "HOLD_CURRENT_BOOK"


def test_b2_owner_precedence_defends_against_a_reconstructed_review_state():
    # Even a composed object that still (defectively) claims PROPOSAL_REVIEW_REQUIRED
    # cannot outrank the constrained owner's HOLD outcome in the presentation.
    wf = _load()
    wf_defective = dict(wf, canonical_portfolio_decision=dict(
        wf["canonical_portfolio_decision"], state="PROPOSAL_REVIEW_REQUIRED"))
    pd = op._portfolio_decision(wf_defective, _CONSTRAINED_HOLD, {},
                                {"historical": False})
    assert pd["state"] == "HOLD"


# =========================================================================== #
# C. The operator presentation says HOLD / no change
# =========================================================================== #
def test_c_operator_presentation_holds_with_no_action():
    wf = _load()
    pd = op._portfolio_decision(wf, _CONSTRAINED_HOLD, {}, {"historical": False})
    assert pd["state"] == "HOLD" and pd["tone"] == "ok"
    na = pd["next_action"] or {}
    assert na.get("available") is False
    assert pd["creates_orders"] is False and pd["automation_off"] is True
    # The reassessment's own card no longer raises its review CTA over the
    # settled decision — it names the decision instead.
    pres = wf["portfolio_reassessment_presentation"]
    assert pres["primary_action"] is None
    assert pres["decision_settled"] is True
    assert pres["settled_decision_state"] == "HOLD_CURRENT_BOOK"
    assert pres["operator_state"] == "PORTFOLIO_DECISION_SETTLED"
    assert "Review the proposed portfolio change" not in (pres["task"] or "")


# =========================================================================== #
# D. approvable=false everywhere, and invariant I8 is ARMED
# =========================================================================== #
def test_d1_nothing_is_approvable_under_hold():
    wf = _load()
    lane = wf["portfolio_decision_state"]
    assert lane["portfolio_decision_state"] == "HOLD_CURRENT_BOOK"
    assert lane["approvable"] is False
    assert lane["requires_manual_review"] is False
    cpd = wf["canonical_portfolio_decision"]
    assert cpd["approvable"] is False
    assert cpd["operator_action_available"] is False
    assert wf["consistency_status"] == "CONSISTENT"
    assert [v for v in (wf.get("consistency_violations") or [])] == []


def test_d2_invariant_i8_fires_when_a_hold_is_claimed_approvable():
    # Before this correction I8 was blind: the gate never delivered the outcome, so
    # reallocation_outcome=None could never equal HOLD. Prove the invariant is armed.
    v = ws.check_decision_semantics(
        reallocation_operator_state="REALLOCATION_PROPOSAL_READY",
        reallocation_approvable=True,
        reassessment_state="PROPOSAL_READY", reassessment_proposal_required=True,
        portfolio_decision_state="HOLD_CURRENT_BOOK",
        portfolio_decision_requires_review=False,
        portfolio_decision_approvable=False,
        proposal_bound_reassessment_hash="H1", current_reassessment_hash="H1",
        mandatory_exit_tickers=[], mandatory_exit_obligation="NONE",
        reallocation_outcome="HOLD_CURRENT_BOOK", feasible_target_exists=True)
    assert "HOLD_CURRENT_BOOK_EXPOSED_AS_APPROVABLE" in [x["code"] for x in v]


def test_d3_the_lane_owner_ranks_hold_above_review():
    summ = {"reallocation_proposal_available": True,
            "reallocation_proposal_hash": "P1",
            "reallocation_action_counts": {"EXIT": 7, "ADD": 8},
            "reallocation_outcome": "HOLD_CURRENT_BOOK",
            "reallocation_switching_hurdle": 0.05,
            "reallocation_clears_switching_hurdle": False,
            "reallocation_feasible_target_exists": True}
    lane = pdec.derive_decision_state(has_active_book=True,
                                      proposal_summary=summ, decision_record=None)
    assert lane["portfolio_decision_state"] == pdec.PDS_HOLD_CURRENT_BOOK
    assert lane["approvable"] is False and lane["requires_manual_review"] is False


# =========================================================================== #
# E. No execution action is offered
# =========================================================================== #
def test_e_no_execution_action_offered_under_hold():
    wf = _load()
    cmd = wf["operator_command"]
    assert cmd["primary_action_available"] is False
    assert cmd["mutation_controls_allowed"] is False
    primary = wf["primary_action"]
    assert primary["execution_available"] is False
    codes = [q["action_code"] for q in (wf.get("queued_actions") or [])]
    assert "REVIEW_PORTFOLIO_PROPOSAL" not in codes
    assert all(q.get("execution_available") is False
               for q in (wf.get("queued_actions") or []))
    hero = wf["today_hero"]
    assert hero["cta_execution_available"] is False
    assert hero["focus_lane"] != "PORTFOLIO"


# =========================================================================== #
# F. A genuinely qualifying proposal STILL requires manual review
# =========================================================================== #
def test_f_qualifying_proposal_ready_still_raises_manual_review():
    wf = _load(gate=_gate(**_READY_OUTCOME_FIELDS))
    assert wf["overall_state"] == "MANUAL_REVIEW_REQUIRED"
    att = wf["portfolio_reassessment"]["portfolio_attention"]
    assert att["review_required"] is True
    assert att["review_reason"] == ws.REVIEW_REASON_PROPOSAL
    lane = wf["portfolio_decision_state"]
    assert lane["portfolio_decision_state"] == "PROPOSAL_REVIEW_REQUIRED"
    assert lane["approvable"] is True
    cpd = wf["canonical_portfolio_decision"]
    assert cpd["state"] == "PROPOSAL_REVIEW_REQUIRED"
    constrained_ready = dict(_CONSTRAINED_HOLD, outcome="PROPOSAL_READY")
    pd = op._portfolio_decision(wf, constrained_ready, {}, {"historical": False})
    assert pd["state"] == "REALLOCATE"
    nc = wf["normal_cycle"]
    assert nc["review_required"] is True


# =========================================================================== #
# G. A true blocker remains blocked
# =========================================================================== #
def test_g_a_blocked_reassessment_remains_blocked():
    blocked_prs = dict(_PRS, reassessment_state="BLOCKED_DATA",
                       proposal_required=False,
                       blockers=["CRITICAL_STALE_OR_MISSING_MARKET_DATA"])
    blocked_gate = _gate(reallocation_proposal_available=False,
                         reallocation_proposal_state="NOT_RUN",
                         reallocation_proposal_hash=None,
                         reallocation_outcome="TRUE_BLOCKER",
                         reallocation_outcome_headline="TRUE BLOCKER",
                         reallocation_outcome_reason_codes=[
                             "CRITICAL_STALE_OR_MISSING_MARKET_DATA"],
                         reallocation_feasible_target_exists=False,
                         reallocation_proposal_approvable=False)
    wf = _load(reassessment_summary=blocked_prs, gate=blocked_gate)
    assert wf["overall_state"] == "RESEARCH_CYCLE_BLOCKED"
    cpd = wf["canonical_portfolio_decision"]
    assert cpd["state"] == "BLOCKED"
    assert cpd["approvable"] is False
    constrained_blocked = dict(_CONSTRAINED_HOLD, outcome="TRUE_BLOCKER")
    pd = op._portfolio_decision(wf, constrained_blocked, {}, {"historical": False})
    assert pd["state"] == "BLOCKED"


# =========================================================================== #
# H. The immutable proposal stays visible, auditable evidence under HOLD
# =========================================================================== #
def test_h_hold_preserves_the_proposal_as_evidence():
    gate = _gate()
    prs = dict(_PRS)
    gate_blob = json.dumps(gate, sort_keys=True, default=str)
    prs_blob = json.dumps(prs, sort_keys=True, default=str)
    wf = _load(gate=gate, reassessment_summary=prs)
    # Composition mutated none of its inputs (the artifact stays what it was).
    assert json.dumps(gate, sort_keys=True, default=str) == gate_blob
    assert json.dumps(prs, sort_keys=True, default=str) == prs_blob
    lane = wf["portfolio_decision_state"]
    # The considered-and-declined alternative remains fully identified.
    assert lane["proposal_available"] is True
    assert lane["proposal_hash"] == "P1"
    assert lane["proposal_id"] == "prop_2026-08-31"
    pres = wf["reallocation_proposal_presentation"]
    assert pres["has_proposal"] is True
    assert pres["proposal_hash"] == "P1"


# =========================================================================== #
# I / K. The orchestrator never reruns completed work; timeout recovery reads
# =========================================================================== #
def _no_runner(**_kw):
    raise AssertionError("an owner was invoked for an already-completed session")


def test_i_completed_close_and_drc_are_never_rerun():
    wf = _load()
    assert wf["overall_state"] == "DAILY_CYCLE_COMPLETE"
    plan = pc.plan_next_step(wf)
    assert plan["step"] is None
    assert plan["stop_reason"] == pc.STOP_DECISION_PRESENTED
    for _ in range(2):  # a repeated POST cannot duplicate the Daily Close / DRC
        out = pc.run_portfolio_cycle(confirm=pc.EXECUTE_CONFIRMATION,
                                     workflow_loader=lambda: wf,
                                     close_runner=_no_runner,
                                     drc_runner=_no_runner)
        assert out["status"] == "PORTFOLIO_CYCLE_COMPLETE"
        assert out["steps"] == [] and out["performed_write"] is False
        assert out["stop_reason"] == pc.STOP_DECISION_PRESENTED
        assert out["canonical_portfolio_decision"]["state"] == "HOLD_CURRENT_BOOK"


def test_k_timeout_recovery_is_read_status_not_rerun():
    wf = _load()
    status = pc.load_portfolio_cycle(workflow=wf)
    assert status["cycle_run_available"] is False
    assert status["stop_reason"] == pc.STOP_DECISION_PRESENTED
    tr = status["timeout_recovery"]
    assert tr["repeated_post_is_idempotent"] is True
    assert tr["safe_recovery"] == "GET %s" % pc.READ_ROUTE
    assert "Do NOT rerun blindly" in tr["guidance"]
    assert tr["single_orchestration_path"] == pc.RUN_ROUTE
    assert status["canonical_portfolio_decision"]["state"] == "HOLD_CURRENT_BOOK"


# =========================================================================== #
# J. Nothing mutates: no order, no plan, no fill, no holding/cash/NAV
# =========================================================================== #
def test_j_no_mutation_anywhere_in_the_read_models():
    wf = _load()
    att = wf["portfolio_reassessment"]["portfolio_attention"]
    assert att["creates_orders"] is False and att["mutates_portfolio"] is False
    assert att["requires_rebalance_approval"] is False
    cpd = wf["canonical_portfolio_decision"]
    assert cpd["creates_orders"] is False and cpd["automation_off"] is True
    status = pc.load_portfolio_cycle(workflow=wf)
    assert status["safety"]["performed_write"] is False
    assert status["safety"]["creates_orders"] is False
    pres = wf["portfolio_reassessment_presentation"]
    assert pres["primary_action"] is None
    lane = wf["portfolio_decision_state"]
    assert lane.get("order_plan_preview") is None or "order_plan_preview" not in lane


# =========================================================================== #
# The stored sentence: a breach-override target request never claims economics
# =========================================================================== #
def test_l_breach_override_sentence_states_the_constraint_not_economics():
    from paper_trader.engine import portfolio_reassessment as eng
    base_decision = {
        "actionable_holding_count": 0, "expected_net_improvement": -0.000894,
        "expected_one_way_turnover": 0.121918,
        "expected_transaction_cost_usd": 30.21,
        "mandatory_exit_tickers": [],
        "held_name_constraint_breaches": ["AMD:RISK_CONTRIBUTION_BREACH"],
        "reason_codes": ["HELD_NAME_CONSTRAINT_BREACH_REQUIRES_TARGET",
                         "NO_ACTIONABLE_HOLDING"],
        "blockers": []}
    pol = {"min_portfolio_net_improvement": 0.05}
    s = eng.explain_portfolio(
        {"reassessment_state": eng.STATE_PROPOSAL_READY, "decision": base_decision},
        pol)
    assert "economically justified" not in s
    assert "AMD:RISK_CONTRIBUTION_BREACH" in s
    assert "constraint fact, not an economic verdict" in s
    # A target request whose economics GENUINELY cleared keeps the classic sentence.
    cleared = dict(base_decision, expected_net_improvement=0.09,
                   held_name_constraint_breaches=[], reason_codes=[])
    s2 = eng.explain_portfolio(
        {"reassessment_state": eng.STATE_PROPOSAL_READY, "decision": cleared}, pol)
    assert "A portfolio change is economically justified" in s2


# =========================================================================== #
# The reassessment card suppression is surgical: old callers are untouched
# =========================================================================== #
def test_m_presentation_without_decision_lane_keeps_old_behaviour():
    p = prs_api.build_presentation(state=prs_api.STATE_PROPOSAL_READY,
                                   reassessment=None)
    assert p["operator_state"] == "MANUAL_REVIEW_REQUIRED"
    assert p["primary_action"] == "REVIEW_PORTFOLIO_PROPOSAL"
    assert p["decision_settled"] is False
    # An OPEN lane (genuine review) keeps the CTA too.
    p2 = prs_api.build_presentation(
        state=prs_api.STATE_PROPOSAL_READY, reassessment=None,
        decision_lane={"portfolio_decision_state": "PROPOSAL_REVIEW_REQUIRED"})
    assert p2["primary_action"] == "REVIEW_PORTFOLIO_PROPOSAL"
    # Execution precedence still outranks everything, settled or not.
    p3 = prs_api.build_presentation(
        state=prs_api.STATE_PROPOSAL_READY, reassessment=None,
        execution={"execution_active": True, "reason": "orders working"},
        decision_lane={"portfolio_decision_state": "HOLD_CURRENT_BOOK"})
    assert p3["primary_action"] is None
