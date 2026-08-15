r"""Stage 22 — NORMAL-CYCLE RELIABILITY & DECISION READINESS.

What this proves
----------------
A. ONE canonical normal cycle. Every workflow state maps to exactly one stage, the
   stages are ordered, and AT MOST ONE normal-path mutation gate is ever open. No
   surface can disagree about what the operator must do next because no surface
   decides it.
B. Stale evidence is DEMOTED, not reinterpreted. BLOCKED_EVIDENCE still blocks a
   portfolio change; what changes is whether it competes for attention and whether
   it reads as an operational incident.
C. Every data gap is machine-readable (ticker / metric / expected + available as-of /
   owner / reason / blocking / effect / safe fallback), an unknown code fails CLOSED,
   and no missing value is ever converted to zero or to current data.
D. The post-close handoff is deterministic: a completed Daily Close makes the Daily
   Research Cycle the ONE required action, with no hidden desk / target / evidence /
   mark refresh in between.
E. A fresh assessment and any proposal derived from it are provably bound to the
   session and portfolio they describe, and a broken binding fails closed exactly once.
F. The full cycle runs end to end in the hermetic acceptance scenarios.
H. The architecture guard reports every Stage-22 invariant and --strict exits zero.

Everything here is offline and read-only: no provider, no prediction service, no live
store, no order, no close, no research run, no promotion.
"""
from __future__ import annotations

import copy
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from paper_trader.api import daily_action_gate as dag
from paper_trader.api import holding_opportunity_cost as hoc
from paper_trader.api import reallocation_proposal as ralloc
from paper_trader.api import workflow_state as ws
from paper_trader.engine import data_gap_taxonomy as gaptax
from paper_trader.engine import normal_cycle as nc

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

ET = timezone(timedelta(hours=-4))


# =========================================================================== #
# A. THE CANONICAL NORMAL-CYCLE STATE CONTRACT (1-14)
# =========================================================================== #
def test_01_the_cycle_declares_five_ordered_stages():
    assert nc.STAGE_SEQUENCE == (
        "WAIT_FOR_SESSION_CLOSE", "DAILY_CLOSE", "DAILY_RESEARCH_CYCLE",
        "PORTFOLIO_DECISION", "CONTROLLED_REBALANCE")
    assert [nc.stage_ordinal(s) for s in nc.STAGE_SEQUENCE] == [1, 2, 3, 4, 5]


def test_02_every_workflow_state_maps_to_exactly_one_stage():
    # No overall state may fall outside the cycle contract.
    for state in ws.OVERALL_STATES:
        assert state in nc.STAGE_FOR_OVERALL_STATE, state
        assert nc.STAGE_FOR_OVERALL_STATE[state] in nc.STAGE_VOCABULARY


def test_03_an_unknown_state_fails_closed_into_recovery():
    assert nc.stage_for_overall_state("SOMETHING_NEW") == nc.STAGE_RECOVERY
    assert nc.stage_for_overall_state(None) == nc.STAGE_RECOVERY


def test_04_next_stage_follows_the_declared_order():
    assert nc.next_stage(nc.STAGE_WAIT_FOR_SESSION_CLOSE) == nc.STAGE_DAILY_CLOSE
    assert nc.next_stage(nc.STAGE_DAILY_CLOSE) == nc.STAGE_DAILY_RESEARCH_CYCLE
    assert nc.next_stage(nc.STAGE_DAILY_RESEARCH_CYCLE) == nc.STAGE_PORTFOLIO_DECISION
    assert nc.next_stage(nc.STAGE_PORTFOLIO_DECISION) == nc.STAGE_CONTROLLED_REBALANCE
    assert nc.next_stage(nc.STAGE_CONTROLLED_REBALANCE) is None
    assert nc.next_stage(nc.STAGE_RECOVERY) is None


@pytest.mark.parametrize("state", list(ws.OVERALL_STATES))
def test_05_at_most_one_mutation_gate_is_open_in_every_state(state):
    gates = nc.build_stage_gates(overall=state, current_stage=nc.stage_for_overall_state(state))
    open_gates = [k for k, v in gates.items() if v["execution_allowed"]]
    assert len(open_gates) <= 1, (state, open_gates)
    # ...and the open gate is always the CURRENT stage.
    if open_gates:
        assert open_gates[0] == nc.stage_for_overall_state(state)


def test_06_the_only_mutation_stages_are_close_and_research():
    opened = set()
    for state in ws.OVERALL_STATES:
        gates = nc.build_stage_gates(
            overall=state, current_stage=nc.stage_for_overall_state(state))
        opened |= {k for k, v in gates.items() if v["execution_allowed"]}
    assert opened == {nc.STAGE_DAILY_CLOSE, nc.STAGE_DAILY_RESEARCH_CYCLE}


def test_07_the_portfolio_decision_is_a_review_never_a_mutation():
    gates = nc.build_stage_gates(overall="MANUAL_REVIEW_REQUIRED",
                                 current_stage=nc.STAGE_PORTFOLIO_DECISION)
    g = gates[nc.STAGE_PORTFOLIO_DECISION]
    assert g["review_required"] is True
    assert g["execution_allowed"] is False
    assert g["creates_orders"] is False


def test_08_stage19_execution_precedence_suppresses_the_review():
    gates = nc.build_stage_gates(overall="MANUAL_REVIEW_REQUIRED",
                                 current_stage=nc.STAGE_PORTFOLIO_DECISION,
                                 execution_active=True)
    assert gates[nc.STAGE_PORTFOLIO_DECISION]["review_required"] is False
    assert gates[nc.STAGE_PORTFOLIO_DECISION]["passive_status"]


def test_09_every_closed_gate_carries_a_passive_status_never_a_control():
    for state in ws.OVERALL_STATES:
        gates = nc.build_stage_gates(
            overall=state, current_stage=nc.stage_for_overall_state(state))
        for stage, g in gates.items():
            if not g["execution_allowed"] and not g["review_required"]:
                assert g["passive_status"], (state, stage)


def test_09b_the_passive_wording_never_contradicts_the_state():
    # While WAITING for the session to close, the wait stage must not claim the session
    # has already closed — a gate string that contradicts its own state is worse than no
    # string at all.
    waiting = nc.build_stage_gates(
        overall=ws.WAITING_FOR_SESSION_CLOSE,
        current_stage=nc.STAGE_WAIT_FOR_SESSION_CLOSE)
    assert ("still open"
            in waiting[nc.STAGE_WAIT_FOR_SESSION_CLOSE]["passive_status"])
    # Past it, the same stage correctly reports the session as closed.
    past = nc.build_stage_gates(overall=ws.RESEARCH_CYCLE_REQUIRED,
                                current_stage=nc.STAGE_DAILY_RESEARCH_CYCLE)
    assert "already closed" in past[nc.STAGE_WAIT_FOR_SESSION_CLOSE]["passive_status"]


def test_10_two_open_mutations_fail_closed():
    with pytest.raises(nc.MultiplePrimaryMutationError):
        nc.assert_single_primary_mutation([
            {"stage": "DAILY_CLOSE", "execution_allowed": True},
            {"stage": "DAILY_RESEARCH_CYCLE", "execution_allowed": True}])
    assert nc.assert_single_primary_mutation([]) == []
    assert nc.assert_single_primary_mutation(
        [{"stage": "DAILY_CLOSE", "execution_allowed": True}]) == ["DAILY_CLOSE"]


def test_11_the_cycle_view_answers_the_four_operator_questions():
    v = nc.build_cycle_view(overall="READY_FOR_DAILY_CLOSE",
                            current_task="Run the operational Daily Close.",
                            why="because", action_available=True,
                            action_label="Run the Daily Close")
    assert v["now_text"] and v["do_text"] == "Run the Daily Close"
    assert v["why_text"] == "because"
    assert "Daily Research Cycle becomes the one required action" in v["after_text"]
    assert v["action_required"] is True and v["no_action_required"] is False


def test_12_a_passive_state_says_no_action_required_explicitly():
    v = nc.build_cycle_view(overall="WAITING_FOR_SESSION_CLOSE",
                            action_available=False,
                            no_action_text=ws.NO_ACTION_TEXT)
    assert v["do_text"] == ws.NO_ACTION_TEXT
    assert v["no_action_required"] is True
    assert v["executable_stage_count"] == 0


def test_13_stage_status_is_done_current_upcoming():
    v = nc.build_cycle_view(overall="RESEARCH_CYCLE_REQUIRED")
    by = {s["stage"]: s["status"] for s in v["stages"]}
    assert by["WAIT_FOR_SESSION_CLOSE"] == nc.ST_DONE
    assert by["DAILY_CLOSE"] == nc.ST_DONE
    assert by["DAILY_RESEARCH_CYCLE"] == nc.ST_CURRENT
    assert by["PORTFOLIO_DECISION"] == nc.ST_UPCOMING
    assert by["CONTROLLED_REBALANCE"] == nc.ST_UPCOMING


def test_14_the_kernel_declares_no_alternate_path_and_no_automation():
    v = nc.build_cycle_view(overall="DAILY_CYCLE_COMPLETE")
    assert v["alternate_path_exists"] is False
    assert v["creates_orders"] is False and v["automatic_execution"] is False
    assert v["single_primary_mutation_enforced"] is True


# =========================================================================== #
# A/D. THE PRIORITY POLICY: close before research, research after the close (15-21)
# =========================================================================== #
_BASE = dict(inconsistent=False, session_status="EXPECTED_SESSION_COMPLETE",
             has_confirmed_eligible=True, eligible_session_closed=True,
             owned_data_lag=False, research_current=True,
             assessment_status=ws.ASSESS_CURRENT, manual_review_required=False,
             evidence_gap=False)


def _decide(**kw):
    return ws._decide_overall(**{**_BASE, **kw})   # noqa: SLF001 - the frozen policy


def test_15_an_unclosed_eligible_session_is_always_the_daily_close():
    # THE alternate path Stage 22 removes: a stale research input used to promote the
    # research cycle ahead of a session whose close had not run.
    assert _decide(eligible_session_closed=False,
                   research_current=False) == ws.READY_FOR_DAILY_CLOSE
    assert _decide(eligible_session_closed=False,
                   assessment_status=ws.ASSESS_STALE) == ws.READY_FOR_DAILY_CLOSE
    assert _decide(eligible_session_closed=False,
                   manual_review_required=True) == ws.READY_FOR_DAILY_CLOSE


def test_16_a_running_or_blocked_cycle_still_outranks_the_close():
    # A run in flight is never interrupted, and a blocked one names a required fix.
    assert _decide(eligible_session_closed=False,
                   cycle_running=True) == ws.RESEARCH_CYCLE_RUNNING
    assert _decide(eligible_session_closed=False,
                   cycle_blocked=True) == ws.RESEARCH_CYCLE_BLOCKED


def test_17_unconfirmed_owned_data_still_outranks_everything_below_it():
    assert _decide(owned_data_lag=True) == ws.WAITING_FOR_OWNED_DATA
    assert _decide(has_confirmed_eligible=False) == ws.WAITING_FOR_OWNED_DATA


def test_18_a_completed_close_makes_the_research_cycle_due():
    # Workstream D: every required signal input can be CURRENT and the legacy gate can
    # be CURRENT while the session that was just closed has never been reassessed.
    assert _decide(research_cycle_due_after_close=True) == ws.RESEARCH_CYCLE_REQUIRED
    # ...and it does not fire before the close is complete (the close comes first).
    assert _decide(eligible_session_closed=False,
                   research_cycle_due_after_close=True) == ws.READY_FOR_DAILY_CLOSE


def test_19_the_post_close_requirement_is_never_inferred_from_a_missing_contract():
    # An absent HOC contract is UNVERIFIABLE, not "not run": inferring the requirement
    # from a missing key would fabricate work for the operator.
    assert _decide(research_cycle_due_after_close=False) == ws.DAILY_CYCLE_COMPLETE


def test_20_the_post_close_action_explains_the_real_reason():
    a = ws._primary_action(ws.RESEARCH_CYCLE_REQUIRED,   # noqa: SLF001
                           {"eligible_date": "2026-08-14",
                            "research_cycle_due_after_close": True})
    assert a["action_code"] == ws.ACTION_RUN_RESEARCH_CYCLE
    assert "no Holding Opportunity-Cost assessment" in a["explanation"]
    assert "stale" not in a["explanation"].lower()
    b = ws._primary_action(ws.RESEARCH_CYCLE_REQUIRED,   # noqa: SLF001
                           {"eligible_date": "2026-08-14"})
    assert "stale or missing" in b["explanation"]


def test_21_the_terminal_region_is_unchanged():
    assert _decide() == ws.DAILY_CYCLE_COMPLETE
    assert _decide(evidence_gap=True) == ws.DAILY_CYCLE_COMPLETE_EVIDENCE_GAP
    assert _decide(manual_review_required=True) == ws.MANUAL_REVIEW_REQUIRED
    assert _decide(inconsistent=True) == ws.INCONSISTENT_STATE


# =========================================================================== #
# B. STALE-EVIDENCE PRESENTATION (22-30)
# =========================================================================== #
_SUPERSEDABLE = [{"code": "STALE_CORPORATE_ACTION_EVIDENCE"},
                 {"code": "PORTFOLIO_STATE_CHANGED_SINCE_ASSESSMENT"}]


def test_22_expected_stale_evidence_is_demoted_but_still_blocks():
    ec = ws.build_evidence_classification(
        reassessment_state="BLOCKED_EVIDENCE", overall=ws.WAITING_FOR_SESSION_CLOSE,
        blockers=_SUPERSEDABLE, eligible_date="2026-08-13")
    assert ec["classification"] == ws.EVIDENCE_EXPECTED_STALE
    assert ec["demoted"] is True
    assert ec["competes_with_primary_action"] is False
    assert ec["is_operational_incident"] is False
    assert ec["requires_operator_fix"] is False
    # THE fail-closed rule is untouched.
    assert ec["blocks_portfolio_action"] is True
    assert ec["validity_rules_changed"] is False
    assert ec["history_rewritten"] is False
    assert ec["audit_visible"] is True


def test_23_before_the_close_it_is_demoted_all_the_way_to_history():
    ec = ws.build_evidence_classification(
        reassessment_state="BLOCKED_EVIDENCE", overall=ws.WAITING_FOR_SESSION_CLOSE,
        blockers=_SUPERSEDABLE)
    assert ec["presentation_class"] == ws.PRESENT_HISTORY
    assert ec["severity"] == ws.SEV_INFO


def test_24_mid_cycle_it_is_evidence_not_history():
    ec = ws.build_evidence_classification(
        reassessment_state="BLOCKED_EVIDENCE", overall=ws.READY_FOR_DAILY_CLOSE,
        blockers=_SUPERSEDABLE)
    assert ec["classification"] == ws.EVIDENCE_EXPECTED_STALE
    assert ec["presentation_class"] == ws.PRESENT_EVIDENCE


def test_25_a_data_block_is_a_system_blocker_the_operator_must_fix():
    ec = ws.build_evidence_classification(
        reassessment_state="BLOCKED_DATA", overall=ws.READY_FOR_DAILY_CLOSE,
        blockers=[{"code": "price_score_refresh_UNAVAILABLE"}])
    assert ec["classification"] == ws.EVIDENCE_SYSTEM_BLOCKER
    assert ec["demoted"] is False and ec["requires_operator_fix"] is True
    assert ec["presentation_class"] == ws.PRESENT_PRIMARY
    assert ec["severity"] == ws.SEV_BLOCKED


def test_26_an_unrecognised_blocker_is_never_assumed_expected():
    ec = ws.build_evidence_classification(
        reassessment_state="BLOCKED_EVIDENCE", overall=ws.READY_FOR_DAILY_CLOSE,
        blockers=[{"code": "SOMETHING_NOBODY_CLASSIFIED"}])
    assert ec["classification"] == ws.EVIDENCE_SYSTEM_BLOCKER


def test_27_no_blockers_at_all_is_never_assumed_expected():
    ec = ws.build_evidence_classification(
        reassessment_state="BLOCKED_EVIDENCE", overall=ws.READY_FOR_DAILY_CLOSE,
        blockers=[])
    assert ec["classification"] == ws.EVIDENCE_SYSTEM_BLOCKER


def test_28_an_inconsistent_workflow_is_never_expected_stale():
    ec = ws.build_evidence_classification(
        reassessment_state="BLOCKED_EVIDENCE", overall=ws.INCONSISTENT_STATE,
        blockers=_SUPERSEDABLE)
    assert ec["classification"] == ws.EVIDENCE_SYSTEM_BLOCKER


def test_29_an_unblocked_assessment_is_current_evidence():
    ec = ws.build_evidence_classification(
        reassessment_state="CURRENT_NO_CHANGE", overall=ws.DAILY_CYCLE_COMPLETE)
    assert ec["classification"] == ws.EVIDENCE_CURRENT
    assert ec["blocks_portfolio_action"] is False


def test_30_the_corporate_action_stale_state_is_also_classified():
    ec = ws.build_evidence_classification(
        reassessment_state="STALE_CORPORATE_ACTION_REVIEW_REQUIRED",
        overall=ws.WAITING_FOR_SESSION_CLOSE,
        blockers=[{"code": "STALE_CORPORATE_ACTION_EVIDENCE"}])
    assert ec["classification"] == ws.EVIDENCE_EXPECTED_STALE
    assert ec["blocks_portfolio_action"] is True


# =========================================================================== #
# C. DATA-GAP TAXONOMY (31-42)
# =========================================================================== #
_REQUIRED_GAP_FIELDS = ("ticker", "metric", "expected_as_of_date",
                        "available_as_of_date", "source_owner", "reason", "blocking",
                        "effect_on_recommendation", "safe_fallback")


def test_31_every_gap_record_carries_the_full_required_contract():
    r = gaptax.classify_code("LIQUIDITY_UNAVAILABLE", ticker="MNST",
                             expected_as_of_date="2026-08-13")
    for f in _REQUIRED_GAP_FIELDS:
        assert f in r, f
    assert r["ticker"] == "MNST" and r["expected_as_of_date"] == "2026-08-13"
    assert r["available_as_of_date"] is None
    assert r["source_owner"] and r["reason"] and r["effect_on_recommendation"]


def test_32_severity_is_a_property_not_a_string_to_parse():
    assert gaptax.classify_code("PRIOR_RANK_UNAVAILABLE")["blocking"] is False
    assert gaptax.classify_code("NO_HOLDINGS")["blocking"] is True
    assert gaptax.classify_code("PRIOR_RANK_UNAVAILABLE")["severity"] == gaptax.NON_BLOCKING
    assert gaptax.classify_code("NO_HOLDINGS")["severity"] == gaptax.BLOCKING


def test_33_an_unknown_gap_code_fails_closed():
    r = gaptax.classify_code("A_CODE_NOBODY_CLASSIFIED")
    assert r["blocking"] is True and r["known_code"] is False
    assert r["safe_fallback"] is None


def test_34_a_gap_with_no_genuine_substitute_reports_none_not_zero():
    for code in ("PRIOR_RANK_UNAVAILABLE", "LIQUIDITY_UNAVAILABLE",
                 "STALE_CORPORATE_ACTION_EVIDENCE"):
        r = gaptax.classify_code(code)
        assert r["safe_fallback"] is None
        assert "None" in r["fallback_note"] or "none" in r["fallback_note"]
        assert r["silently_substituted"] is False


def test_35_the_one_named_fallback_is_one_the_owner_actually_implements():
    r = gaptax.classify_code("RISK_CONTRIBUTION_UNAVAILABLE")
    assert r["safe_fallback"] == "gross_score_improvement"
    assert r["blocking"] is False


def test_36_blocking_and_non_blocking_reach_different_conclusions():
    blocking = gaptax.summarize([gaptax.classify_code("NO_HOLDINGS")])
    assert blocking["conclusion"] == "PORTFOLIO_CONCLUSION_NOT_SAFE"
    assert blocking["has_blocking_gap"] is True
    soft = gaptax.summarize([gaptax.classify_code("PRIOR_RANK_UNAVAILABLE")])
    assert soft["conclusion"] == "PORTFOLIO_CONCLUSION_WITH_DISCLOSED_UNCERTAINTY"
    assert soft["has_blocking_gap"] is False
    none = gaptax.summarize([])
    assert none["conclusion"] == "PORTFOLIO_CONCLUSION_COMPLETE"


def test_37_the_summary_never_claims_a_silent_substitution():
    s = gaptax.summarize([gaptax.classify_code("LIQUIDITY_UNAVAILABLE", ticker="X")])
    assert s["missing_data_converted_to_zero"] is False
    assert s["missing_data_converted_to_current"] is False


def _assessment(**kw):
    base = {
        "eligible_market_date": "2026-08-13",
        "data_quality": {"data_gaps": ["PRIOR_RANK_UNAVAILABLE"]},
        "holding_reviews": [
            {"ticker": "AAA", "liquidity_state": "LIQUID", "required_data_complete": True},
            {"ticker": "BBB", "liquidity_state": "UNAVAILABLE",
             "required_data_complete": True},
            {"ticker": "CCC", "liquidity_state": "LIQUID", "required_data_complete": False,
             "current_rank": None, "current_score": 1.0, "return_20d": 0.1,
             "volatility_60d": 0.2},
        ],
    }
    base.update(kw)
    return base


def test_38_a_holding_level_gap_is_attributed_to_its_ticker():
    s = gaptax.classify_assessment_gaps(assessment=_assessment())
    liq = [g for g in s["gaps"] if g["code"] == "LIQUIDITY_UNAVAILABLE"]
    assert len(liq) == 1 and liq[0]["ticker"] == "BBB"
    assert liq[0]["scope"] == gaptax.SCOPE_HOLDING
    inc = [g for g in s["gaps"] if g["code"] == "REQUIRED_HOLDING_INPUT_INCOMPLETE"]
    assert len(inc) == 1 and inc[0]["ticker"] == "CCC"
    assert "current rank" in inc[0]["reason"]
    assert s["affected_tickers"] == ["BBB", "CCC"]


def test_39_a_portfolio_level_gap_carries_no_ticker():
    s = gaptax.classify_assessment_gaps(assessment=_assessment())
    prior = [g for g in s["gaps"] if g["code"] == "PRIOR_RANK_UNAVAILABLE"]
    assert len(prior) == 1 and prior[0]["ticker"] is None
    assert prior[0]["scope"] == gaptax.SCOPE_PORTFOLIO


def test_40_the_expected_as_of_date_for_prior_rank_is_the_previous_session():
    s = gaptax.classify_assessment_gaps(
        assessment=_assessment(), eligible_market_date="2026-08-13",
        previous_eligible_market_date="2026-08-12")
    prior = [g for g in s["gaps"] if g["code"] == "PRIOR_RANK_UNAVAILABLE"][0]
    assert prior["expected_as_of_date"] == "2026-08-12"


def test_41_classification_never_perturbs_the_immutable_assessment():
    a = _assessment()
    before = json.dumps(a, sort_keys=True)
    gaptax.classify_assessment_gaps(assessment=a)
    assert json.dumps(a, sort_keys=True) == before


def test_42_the_hoc_read_owner_exposes_the_taxonomy_verbatim():
    s = hoc.classify_data_gaps(assessment=_assessment())
    assert s["taxonomy_version"] == gaptax.TAXONOMY_VERSION
    assert s["gap_count"] == 3 and s["blocking_gap_count"] == 0


# =========================================================================== #
# E. FRESH ASSESSMENT / PROPOSAL BINDING (43-50)
# =========================================================================== #
def _binding(**kw):
    base = dict(eligible_date="2026-08-13", active_book_id="alpha_paper_book_1",
                hoc_available=True, hoc_bound_date="2026-08-13",
                hoc_bound_book="alpha_paper_book_1", hoc_assessment_hash="H1",
                hoc_stale=False, hoc_stale_reason=None,
                proposal_available=False, proposal_bound_assessment_hash=None,
                proposal_bound_date=None, proposal_hash=None)
    base.update(kw)
    return ws.build_assessment_binding(**base)


def test_43_a_fresh_bound_assessment_is_current():
    b = _binding()
    assert b["state"] == ws.BINDING_CURRENT and b["bound_and_current"] is True
    assert b["failed_checks"] == [] and b["unverifiable_checks"] == []


def test_44_an_assessment_for_another_session_fails_closed():
    b = _binding(hoc_bound_date="2026-08-12")
    assert b["state"] == ws.BINDING_STALE and b["bound_and_current"] is False
    assert "assessment_market_date_equals_eligible_session" in b["failed_checks"]


def test_45_an_assessment_for_another_book_fails_closed():
    b = _binding(hoc_bound_book="some_other_book")
    assert b["state"] == ws.BINDING_STALE
    assert "assessment_bound_to_active_book" in b["failed_checks"]


def test_46_a_registered_corporate_action_fails_closed():
    b = _binding(hoc_stale=True, hoc_stale_reason="CORPORATE_ACTION_REGISTERED")
    assert b["state"] == ws.BINDING_STALE
    assert "corporate_action_registry_unchanged" in b["failed_checks"]


def test_47_a_proposal_must_bind_to_the_exact_current_assessment():
    ok = _binding(proposal_available=True, proposal_bound_assessment_hash="H1",
                  proposal_bound_date="2026-08-13", proposal_hash="P1")
    assert ok["state"] == ws.BINDING_CURRENT
    assert ok["proposal_bound_to_current_assessment"] is True
    bad = _binding(proposal_available=True, proposal_bound_assessment_hash="H_OLD",
                   proposal_bound_date="2026-08-13", proposal_hash="P1")
    assert bad["state"] == ws.BINDING_STALE
    assert "proposal_binds_to_current_assessment" in bad["failed_checks"]
    assert bad["proposal_bound_to_current_assessment"] is False


def test_48_an_unverifiable_binding_is_not_treated_as_broken():
    b = _binding(hoc_bound_date=None)
    assert b["state"] == ws.BINDING_UNVERIFIABLE
    assert b["failed_checks"] == []
    assert "cannot be proven either way" in b["explanation"]


def test_49_no_assessment_is_its_own_state_not_a_failure():
    b = _binding(hoc_available=False)
    assert b["state"] == ws.BINDING_ABSENT
    assert b["reason"] == "NO_ASSESSMENT_FOR_ELIGIBLE_SESSION"


def test_50_the_binding_verdict_is_stated_exactly_once():
    # One verdict object, one reason — never four surfaces each raising their own.
    b = _binding(hoc_bound_date="2026-08-12", hoc_stale=True)
    assert b["stated_once"] is True and b["fails_closed"] is True
    assert isinstance(b["reason"], str)
    assert b["reason"] == b["failed_checks"][0]


# =========================================================================== #
# A/E. THE COMPOSED WORKFLOW CONTRACT (51-60)
# =========================================================================== #
_OPB = {"operational_book": {
    "book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1",
    "current_status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
    "nav_as_of_date": "2026-08-13", "desk_mark_date": "2026-08-13",
    "latest_desk_mark_date": "2026-08-13", "nav": 100463.92, "cash": 4482.71,
    "holdings_count": 25, "pending_order_count": 0,
    "current_target": {"alpha_market_date": "2026-08-13",
                       "latest_completed_market_date": "2026-08-13"}}}
_INPUTS = {"market_as_of_date": "2026-08-13", "momentum_month": "2026-08",
           "fundamental_as_of_date": "2026-05-22"}
_DESK = {"series": {"SPY": [["2026-08-12", 770.0], ["2026-08-13", 772.0]]},
         "latest_completed_date": "2026-08-13"}
_FWD = {"latest_snapshot_date": "2026-08-13", "snapshot_count": 1,
        "evidence_state": "FORWARD_EVIDENCE_OK", "active_book": {}, "shadow_books": []}
_CLOSE_DONE = {"market_date": "2026-08-13", "done": True,
               "final_close_status": "DAILY_CLOSE_COMPLETE_HOLD"}
_TR = {"dates": {"alpha_market_date": "2026-08-13"}}


def _gate(*, hoc_available=True, **kw):
    g = {"latest_completed_market_date": "2026-08-13", "outcome": "NO_ACTION_TODAY",
         "target_state": "CURRENT_ALIGNED", "next_scheduled_full_review": "2026-09-01",
         "scheduled_review_due": False,
         "opportunity_cost_available": hoc_available,
         "opportunity_cost_state": "READY" if hoc_available else "NOT_RUN",
         "opportunity_cost_assessment_hash": "H1" if hoc_available else None,
         "opportunity_cost_recommendation_counts": {"HOLD": 25},
         "opportunity_cost_data_gaps": [],
         "opportunity_cost_bound_eligible_market_date": ("2026-08-13" if hoc_available
                                                         else None),
         "opportunity_cost_bound_active_book_id": ("alpha_paper_book_1" if hoc_available
                                                   else None)}
    g.update(kw)
    return g


def _load(**kw):
    args = dict(reference_today="2026-08-14", operational=copy.deepcopy(_OPB),
                inputs=dict(_INPUTS), daily_status={"latest_valid_mark_date": "2026-08-13"},
                desk_marks=copy.deepcopy(_DESK), close_progress=dict(_CLOSE_DONE),
                forward_status=copy.deepcopy(_FWD), gate=_gate(),
                target_readiness=copy.deepcopy(_TR),
                research_cycle={"state": "COMPLETE", "blockers": []},
                reassessment_summary={}, decision_record=None)
    args.update(kw)
    return ws.load_workflow_state(**args)


def test_51_the_payload_carries_the_canonical_cycle():
    r = _load()
    cyc = r["normal_cycle"]
    assert cyc["cycle_id"] == nc.CYCLE_ID
    assert cyc["stage_sequence"] == list(nc.STAGE_SEQUENCE)
    assert r["normal_cycle_owner"] == "engine.normal_cycle"
    assert r["normal_cycle_stage"] == cyc["current_stage"]


def test_52_a_completed_close_without_an_assessment_demands_the_research_cycle():
    r = _load(gate=_gate(hoc_available=False))
    assert r["overall_state"] == ws.RESEARCH_CYCLE_REQUIRED
    assert r["normal_cycle"]["current_stage"] == nc.STAGE_DAILY_RESEARCH_CYCLE
    assert r["primary_action"]["execution_kind"] == ws.EXEC_DAILY_RESEARCH_CYCLE
    assert r["operator_command"]["primary_action_available"] is True


def test_53_no_hidden_desk_refresh_stands_between_the_close_and_research():
    # Standing at the close, the contract PROMISES the handoff needs nothing in between.
    at_close = _load(gate=_gate(hoc_available=False),
                     close_progress={"market_date": "2026-08-12", "done": True,
                                     "final_close_status": "DAILY_CLOSE_COMPLETE_HOLD"})
    assert at_close["normal_cycle"]["current_stage"] == nc.STAGE_DAILY_CLOSE
    after_close = at_close["normal_cycle"]["after_text"].lower()
    assert "no separate desk, target, evidence or mark refresh" in after_close
    # Standing after it, the research cycle is the ONE open gate — the promise held.
    after = _load(gate=_gate(hoc_available=False))
    gates = after["normal_cycle"]["stage_gates"]
    open_gates = [k for k, v in gates.items() if v["execution_allowed"]]
    assert open_gates == [nc.STAGE_DAILY_RESEARCH_CYCLE]
    # The maintenance desk refresh can never be the promoted action in either state.
    for r in (at_close, after):
        assert r["primary_action"]["execution_kind"] not in ws.MAINTENANCE_EXECUTION_KINDS
        assert r["operator_command"]["primary_action_kind"] in (
            ws.EXEC_DAILY_CLOSE, ws.EXEC_DAILY_RESEARCH_CYCLE)


def test_54_with_a_current_assessment_the_cycle_is_complete_and_quiet():
    r = _load()
    assert r["overall_state"] == ws.DAILY_CYCLE_COMPLETE
    assert r["normal_cycle"]["current_stage"] == nc.STAGE_PORTFOLIO_DECISION
    assert r["operator_command"]["primary_action_available"] is False
    assert r["operator_command"]["next_text"] == ws.NO_ACTION_TEXT
    assert r["normal_cycle"]["executable_stage_count"] == 0


def test_55_the_operator_command_answers_what_happens_after_that():
    r = _load()
    c = r["operator_command"]
    assert c["after_text"] and c["cycle_stage"] == r["normal_cycle"]["current_stage"]
    assert c["cycle_next_stage"] == r["normal_cycle"]["next_stage"]
    assert c["cycle_stage_count"] == 5


def test_56_exactly_one_mutation_is_offered_in_the_composed_payload():
    for kw in ({}, {"gate": _gate(hoc_available=False)},
               {"close_progress": {"market_date": "2026-08-12", "done": True,
                                   "final_close_status": "DAILY_CLOSE_COMPLETE_HOLD"}}):
        r = _load(**kw)
        gates = r["normal_cycle"]["stage_gates"]
        assert sum(1 for g in gates.values() if g["execution_allowed"]) <= 1


def test_57_the_binding_verdict_is_present_and_bound():
    r = _load()
    assert r["assessment_binding"]["state"] == ws.BINDING_CURRENT
    assert r["assessment_binding_state_vocabulary"] == list(ws.BINDING_STATES)


def test_58_the_gap_taxonomy_is_present_and_consumed_not_inferred():
    r = _load()
    assert r["data_gap_taxonomy_owner"] == "engine.data_gap_taxonomy"
    assert r["blocking_data_gap_count"] == 0
    assert "severity_vocabulary" in r["data_gap_taxonomy"]


def test_59_the_workflow_owner_stays_read_only():
    s = _load()["safety"]
    for flag in ("wrote_to_database", "wrote_to_ledger", "called_provider",
                 "called_prediction", "ran_daily_close", "ran_research_refresh",
                 "created_orders", "promoted_model", "automatic_promotion_allowed"):
        assert s[flag] is False, flag
    assert s["read_only"] is True and s["automation_off"] is True


def test_60_an_unclosed_session_promotes_the_close_over_a_missing_assessment():
    r = _load(gate=_gate(hoc_available=False),
              close_progress={"market_date": "2026-08-12", "done": True,
                              "final_close_status": "DAILY_CLOSE_COMPLETE_HOLD"})
    assert r["overall_state"] == ws.READY_FOR_DAILY_CLOSE
    assert r["normal_cycle"]["current_stage"] == nc.STAGE_DAILY_CLOSE
    assert r["normal_cycle"]["next_stage"] == nc.STAGE_DAILY_RESEARCH_CYCLE


# =========================================================================== #
# C/E. THE SHARED READ PATH CARRIES THE NEW CONTRACTS (61-64)
# =========================================================================== #
_ART = {"artifact_id": "hoc_x", "identity": {
    "active_book_id": "alpha_paper_book_1", "eligible_market_date": "2026-08-13",
    "assessment_hash": "H1", "corporate_actions_hash": None},
    "input_contract": {"eligible_market_date": "2026-08-13",
                       "economic_state_hash": "E1"},
    "assessment": _assessment(assessment_hash="H1", assessment_state="DEGRADED",
                              recommendation_counts={"HOLD": 3})}


def test_61_the_hoc_summary_carries_the_taxonomy_and_the_binding(tmp_path):
    s = hoc.load_assessment_summary(
        active_book_id="alpha_paper_book_1", eligible_market_date="2026-08-13",
        artifact=_ART, hoc_dir=tmp_path, actions_dir=tmp_path / "ca")
    assert s["opportunity_cost_blocking_gap_count"] == 0
    assert s["opportunity_cost_data_gap_taxonomy"]["gap_count"] == 3
    assert s["opportunity_cost_bound_eligible_market_date"] == "2026-08-13"
    assert s["opportunity_cost_bound_active_book_id"] == "alpha_paper_book_1"
    assert s["opportunity_cost_bound_economic_state_hash"] == "E1"


def test_62_the_hoc_read_contract_carries_the_taxonomy(tmp_path):
    ps = {"dates": {"eligible_market_date": "2026-08-13"},
          "active_book": {"book_id": "alpha_paper_book_1", "initialized": True},
          "corporate_actions": {"registry_fingerprint": None, "actions": []}}
    r = hoc.load_holding_opportunity_cost(portfolio_state=ps, artifact=_ART,
                                          hoc_dir=tmp_path)
    assert r["data_gap_taxonomy"]["gap_count"] == 3


def test_63_the_reallocation_summary_carries_its_binding(tmp_path):
    art = {"proposal_id": "P1", "identity": {
        "active_book_id": "alpha_paper_book_1", "eligible_market_date": "2026-08-13",
        "hoc_assessment_hash": "H1"},
        "proposal": {"proposal_state": "READY", "proposal_hash": "PH1",
                     "action_counts": {}, "data_gaps": [], "portfolio": {},
                     "signal": {}, "turnover": {}}}
    s = ralloc.load_proposal_summary(
        active_book_id="alpha_paper_book_1", eligible_market_date="2026-08-13",
        artifact=art, reallocation_dir=tmp_path, actions_dir=tmp_path / "ca")
    assert s["reallocation_bound_hoc_assessment_hash"] == "H1"
    assert s["reallocation_bound_eligible_market_date"] == "2026-08-13"


def test_64_the_gate_carries_both_bindings_through_the_one_shared_path(tmp_path):
    g = dag.load_daily_action_gate(
        today="2026-08-14", operational=copy.deepcopy(_OPB),
        current={"status": "MHZ_READY", "market_as_of_date": "2026-08-13"},
        opportunity_cost=hoc.load_assessment_summary(
            active_book_id="alpha_paper_book_1", eligible_market_date="2026-08-13",
            artifact=_ART, hoc_dir=tmp_path, actions_dir=tmp_path / "ca"),
        reallocation={"reallocation_proposal_available": False,
                      "reallocation_bound_hoc_assessment_hash": None})
    assert g["opportunity_cost_bound_eligible_market_date"] == "2026-08-13"
    assert g["opportunity_cost_data_gap_taxonomy"]["gap_count"] == 3
    assert "reallocation_bound_hoc_assessment_hash" in g


# =========================================================================== #
# F. FULL NORMAL-CYCLE HERMETIC ACCEPTANCE (65-72)
# =========================================================================== #
@pytest.fixture(scope="module")
def fx():
    import stage20_ui_fixtures as _fx
    return _fx


@pytest.fixture(scope="module")
def composed(fx):
    return {k: fx.compose(k) for k in fx.SCENARIO_KEYS}


def test_65_the_three_normal_cycle_scenarios_exist(fx):
    for k in ("scenario_7_pre_close_expected_stale_evidence",
              "scenario_8_session_complete_close_due",
              "scenario_9_post_close_research_due"):
        assert k in fx.SCENARIO_KEYS


def test_66_every_scenario_is_cross_panel_consistent(composed):
    bad = {k: c["consistency"]["violations"] for k, c in composed.items()
           if not c["consistency"]["consistent"]}
    assert bad == {}


def test_67_pre_close_requires_nothing_and_demotes_the_stale_assessment(composed):
    c = composed["scenario_7_pre_close_expected_stale_evidence"]
    w = c["panels"]["workflow_state"]
    assert w["overall_state"] == ws.WAITING_FOR_SESSION_CLOSE
    assert w["normal_cycle"]["current_stage"] == nc.STAGE_WAIT_FOR_SESSION_CLOSE
    assert w["operator_command"]["primary_action_available"] is False
    assert w["operator_command"]["next_text"] == ws.NO_ACTION_TEXT
    # The prior assessment is genuinely BLOCKED_EVIDENCE...
    assert c["panels"]["portfolio_reassessment"]["state"] == "BLOCKED_EVIDENCE"
    # ...and it is demoted to history while STILL blocking a portfolio change.
    ec = w["evidence_classification"]
    assert ec["classification"] == ws.EVIDENCE_EXPECTED_STALE
    assert ec["presentation_class"] == ws.PRESENT_HISTORY
    assert ec["blocks_portfolio_action"] is True
    assert ec["is_operational_incident"] is False
    assert c["consistency"]["mutation_action_count"] == 0


def test_68_a_completed_session_makes_the_close_the_one_action(composed):
    c = composed["scenario_8_session_complete_close_due"]
    w = c["panels"]["workflow_state"]
    assert w["overall_state"] == ws.READY_FOR_DAILY_CLOSE
    assert w["normal_cycle"]["current_stage"] == nc.STAGE_DAILY_CLOSE
    assert w["primary_action"]["execution_kind"] == ws.EXEC_DAILY_CLOSE
    assert c["consistency"]["mutation_action_count"] == 1
    assert c["consistency"]["open_stage_gates"] == [nc.STAGE_DAILY_CLOSE]


def test_69_a_completed_close_makes_the_research_cycle_the_one_action(composed):
    c = composed["scenario_9_post_close_research_due"]
    w = c["panels"]["workflow_state"]
    assert w["overall_state"] == ws.RESEARCH_CYCLE_REQUIRED
    assert w["normal_cycle"]["current_stage"] == nc.STAGE_DAILY_RESEARCH_CYCLE
    assert w["primary_action"]["execution_kind"] == ws.EXEC_DAILY_RESEARCH_CYCLE
    assert c["consistency"]["open_stage_gates"] == [nc.STAGE_DAILY_RESEARCH_CYCLE]
    # No assessment exists for the just-closed session — stated once, not four times.
    assert w["assessment_binding"]["state"] == ws.BINDING_ABSENT


def test_70_the_two_legitimate_endings_are_monitor_and_manual_review(composed):
    monitor = composed["scenario_1_portfolio_current"]["panels"]["workflow_state"]
    assert monitor["overall_state"] == ws.DAILY_CYCLE_COMPLETE
    assert monitor["operator_command"]["primary_action_available"] is False
    review = composed["scenario_3_proposal_review"]
    assert review["consistency"]["mutation_action_count"] == 1
    assert (review["panels"]["portfolio_reassessment"]["presentation"]["primary_action"]
            == "REVIEW_PORTFOLIO_PROPOSAL")


def test_71_no_scenario_creates_an_order_promotes_a_model_or_enables_automation(composed):
    for k, c in composed.items():
        w = c["panels"]["workflow_state"]
        s = w["safety"]
        assert s["created_orders"] is False, k
        assert s["promoted_model"] is False, k
        assert s["automatic_promotion_allowed"] is False, k
        assert s["automation_off"] is True, k
        assert w["normal_cycle"]["creates_orders"] is False, k


def test_72_the_pre_close_scenario_uses_a_real_open_session_not_a_forced_status(composed):
    w = composed["scenario_7_pre_close_expected_stale_evidence"]["panels"]["workflow_state"]
    # The status comes from the REAL market-session owner under a live pre-cutoff clock.
    assert w["current_session"]["session_status"] == "BEFORE_SESSION_CLOSE"


# --------------------------------------------------------------------------- #
# F2. ONE CLOCK PER SCENARIO — the last live-world read in the operational book.
#
# The operational panel publishes the latest completed session TWICE: the alpha-target
# readiness contract states it, and the desk-mark readiness states it again as the session
# the marks are required for. In production they are the same call, so they cannot
# disagree. Stage 21 (Workstream 0F) froze the first through `target_readiness` and left
# the second resolving `alpha_target.latest_completed()` — the live clock. Every frozen
# scenario therefore degraded to DESK_MARK_BEHIND / REFRESH_DESK_MARKS with a
# DESK_MARK_DATE_BEHIND_REQUIRED blocker as soon as the wall clock passed the frozen
# session, and published a real-calendar date inside a synthetic world. Bound at the read
# seam, not at the rendered value: the harness supplies no clock of its own.
# --------------------------------------------------------------------------- #
def test_72b_the_desk_mark_readiness_clock_is_an_additive_injection_seam():
    import inspect

    from paper_trader.api import alpha_book as ab
    from paper_trader.api import operational_book as ob
    sig = inspect.signature(ab.load_desk_mark_readiness)
    assert "latest_completed" in sig.parameters
    assert sig.parameters["latest_completed"].default is None
    # The operational book grows NO new public parameter: the session it already receives
    # through `target_readiness` is the one it forwards, so a caller cannot declare two.
    assert "latest_completed" not in inspect.signature(ob.load_operational_book).parameters


def test_72c_omitting_the_clock_seam_leaves_production_resolving_it_live(tmp_path):
    from paper_trader.api import alpha_book as ab
    from paper_trader.api import alpha_target as at
    rd = ab.load_desk_mark_readiness(desk_dir=tmp_path / "d", ledger_dir=tmp_path / "l")
    assert rd["latest_completed_market_date"] == at.latest_completed()


def test_72d_a_frozen_world_freezes_both_published_required_sessions(composed):
    for k, c in composed.items():
        opb = c["panels"]["operational_book"]["operational_book"]
        eligible = c["world"]["eligible_market_date"]
        assert opb["desk_mark_required_date"] == eligible, k
        assert (opb["current_target"] or {}).get(
            "latest_completed_market_date") == eligible, k
        # The seeded desk marks ARE at that session, so the world's own verdict shows.
        assert opb["desk_mark_status"] == "DESK_MARK_READY", k
        assert not [b for b in (opb["blockers"] or [])
                    if b.startswith("DESK_MARK_DATE_BEHIND_REQUIRED")], k


def test_72e_no_composed_panel_carries_a_date_after_the_harness_reference(fx, composed):
    """The Stage-21 decay signature, asserted from inside the Stage-22 suite too: a date
    later than the harness's own reference day can only have come from the real world."""
    def observed(node, out, key=""):
        if isinstance(node, str):
            if key.endswith(("_at",)) or "next_" in key or "scheduled" in key:
                return out
            if len(node) >= 10 and node[4] == "-" and node[7] == "-":
                out.add(node[:10])
        elif isinstance(node, dict):
            if str(node.get("code") or "") == "SCHEDULED_FULL_REVIEW":
                return out
            for k, v in node.items():
                observed(v, out, str(k))
        elif isinstance(node, (list, tuple)):
            for v in node:
                observed(v, out, key)
        return out

    for k, c in composed.items():
        future = sorted(d for d in observed(c["panels"], set()) if d > fx.NEXT)
        assert not future, (k, future)


# =========================================================================== #
# G. OPERATOR COCKPIT — the UI mirrors, never derives (73-78)
# =========================================================================== #
@pytest.fixture(scope="module")
def ui():
    return (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8")


def test_73_the_command_bar_renders_the_cycle_from_the_backend(ui):
    assert 'id="opc-cycle"' in ui
    assert "d.normal_cycle" in ui
    assert "cyc.stages" in ui and "s.status === 'CURRENT'" in ui


def test_74_the_ui_owns_no_stage_list_and_no_next_step_rule(ui):
    for forbidden in ("function normalCycleStage", "function nextCycleStage",
                      "function decideCycleStage"):
        assert forbidden not in ui
    # The stage labels are read from the payload, never hard-coded as a sequence.
    assert "WAIT_FOR_SESSION_CLOSE'," not in ui


def test_75_the_command_bar_answers_all_four_questions(ui):
    for token in ('class="opc-task"', 'class="opc-why"', 'id="opc-after"',
                  'class="opc-cycle-lab"'):
        assert token in ui, token
    assert "c.after_text" in ui


def test_75b_the_right_rail_never_duplicates_the_one_execution_control(ui):
    # The command bar owns execution; the rail (like the Today hero) becomes purely
    # navigational when it does, so one canonical action is never two live buttons.
    assert "var railOwnsExecution" in ui and "_wsCommandOwnsExecution()" in ui
    start = ui.find("var railOwnsExecution")
    region = ui[start:start + 1600]
    assert "navigateToRoute(r)" in region
    assert "Navigation only" in region


def test_76_the_evidence_hierarchy_is_applied_from_the_canonical_payload(ui):
    assert "function _wsApplyEvidenceHierarchy(" in ui
    assert ui.count("function _wsApplyEvidenceHierarchy(") == 1
    assert "ec.presentation_class" in ui and "ec.demoted" in ui
    assert 'id="reassess-evidence-class"' in ui


def test_77_the_data_gap_taxonomy_is_rendered_field_by_field(ui):
    assert 'id="hoc-gaps"' in ui
    for token in ("g.expected_as_of_date", "g.available_as_of_date", "g.source_owner",
                  "g.effect_on_recommendation", "g.safe_fallback", "g.severity"):
        assert token in ui, token


def test_78_the_stage22_surfaces_add_no_dialog_and_no_order_control(ui):
    import re
    assert not re.search(r"(?<![A-Za-z0-9_.])alert\(", ui)
    assert not re.search(r"(?<![A-Za-z0-9_.])confirm\(", ui)
    assert not re.search(r"(?<![A-Za-z0-9_.])prompt\(", ui)
    # Scope: what Stage 22 ADDED. The legacy DB review workflow's own order-ticket
    # controls are pre-existing and out of this stage's scope; what must be true is that
    # the cycle strip, the evidence hierarchy and the gap block introduce no execution
    # control of any kind — they are orientation and evidence, never a place to act.
    start = ui.find('id="opc-cycle"')
    end = ui.find("window.renderOperatorCommand")
    strip = ui[start:end] if (start != -1 and end > start) else ""
    assert strip, "the cycle strip must render inside renderOperatorCommand"
    for forbidden in ("<button", "onclick=", "createOrders", "confirm_", "POST"):
        assert forbidden not in strip, forbidden
    hier_start = ui.find("function _wsApplyEvidenceHierarchy(")
    hier_end = ui.find("window._wsApplyEvidenceHierarchy")
    hier = ui[hier_start:hier_end]
    for forbidden in ("<button", "onclick=", "createOrders", "POST"):
        assert forbidden not in hier, forbidden


# =========================================================================== #
# H. ARCHITECTURE GUARDS (79-84)
# =========================================================================== #
@pytest.fixture(scope="module")
def audit():
    sys.path.insert(0, str(REPO / "scripts"))
    import audit_architecture as A
    return A


@pytest.fixture(scope="module")
def report(audit):
    return audit.run_audit()


def test_79_the_guard_reports_every_stage22_invariant(report):
    nc_rep = report["normal_cycle_ownership"]
    assert nc_rep["kernels_present"] is True
    assert nc_rep["kernel_impurity"] == [] and nc_rep["gap_kernel_impurity"] == []
    assert nc_rep["sequence_declared"] and nc_rep["sequence_ordered"]
    assert nc_rep["second_cycle_owner_modules"] == []
    assert nc_rep["second_gap_owner_modules"] == []
    assert nc_rep["missing_owner_tokens"] == []
    assert nc_rep["single_mutation_enforced"] is True
    assert nc_rep["post_close_research_required"] is True
    assert nc_rep["close_outranks_research"] is True
    assert nc_rep["no_standalone_desk_refresh_required"] is True
    assert nc_rep["missing_evidence_tokens"] == []
    assert nc_rep["evidence_still_fails_closed"] is True
    assert nc_rep["missing_binding_tokens"] == []
    assert nc_rep["missing_gap_tokens"] == []
    assert nc_rep["unknown_gap_fails_closed"] is True
    assert nc_rep["no_silent_substitution"] is True
    assert nc_rep["gap_severity_consumed_not_inferred"] is True
    assert nc_rep["missing_ui_tokens"] == []
    assert nc_rep["ui_cycle_derivation"] == []


def test_80_every_stage22_invariant_is_blocking(audit):
    keys = {f for k, f, _ in audit.BLOCKING_INVARIANTS if k == "normal_cycle_ownership"}
    for required in ("kernels_present", "kernel_impurity", "sequence_ordered",
                     "second_cycle_owner_modules", "single_mutation_enforced",
                     "post_close_research_required", "close_outranks_research",
                     "no_standalone_desk_refresh_required", "evidence_still_fails_closed",
                     "missing_binding_tokens", "unknown_gap_fails_closed",
                     "no_silent_substitution", "ui_cycle_derivation"):
        assert required in keys, required


def test_81_no_blocking_invariant_fails(audit, report):
    assert audit._blocking_invariant_failures(report) == []   # noqa: SLF001


def test_82_the_new_modules_are_in_the_inventory(audit, report):
    d = report["inventory_drift"]
    assert d["on_disk_not_in_inventory"] == []
    assert d["in_inventory_not_on_disk"] == []
    inv = json.loads((REPO / "docs" / "architecture" / "system_inventory.json")
                     .read_text(encoding="utf-8"))
    paths = {m["path"] for m in inv["modules"]}
    assert "engine/normal_cycle.py" in paths
    assert "engine/data_gap_taxonomy.py" in paths


def test_83_the_canonical_restart_guard_is_preserved(report):
    br = report["backend_restart_ownership"]
    assert br["owner_present"] and br["owner_declares_ownership"]
    assert br["noncanonical_health_probes"] == []
    assert br["reimplementing_scripts"] == []
    assert br["probed_routes_not_declared"] == []
    assert br["live_smoke_emitting_scripts"] == [
        "scripts/restart_paper_trader_backend.ps1"]


def test_84_strict_audit_exit_zero():
    p = subprocess.run([sys.executable, str(REPO / "scripts" / "audit_architecture.py"),
                        "--strict", "--json-only"],
                       capture_output=True, text=True, cwd=str(REPO), timeout=900)
    assert p.returncode == 0, p.stdout[-4000:] + p.stderr[-2000:]


# =========================================================================== #
# SAFETY BOUNDARIES (85-88)
# =========================================================================== #
def test_85_the_cycle_kernel_is_pure():
    src = (REPO / "engine" / "normal_cycle.py").read_text(encoding="utf-8")
    for forbidden in ("import os", "open(", "requests.", "httpx.", "datetime.now(",
                      "from paper_trader.api"):
        assert forbidden not in src, forbidden


def test_86_the_gap_kernel_is_pure():
    src = (REPO / "engine" / "data_gap_taxonomy.py").read_text(encoding="utf-8")
    for forbidden in ("import os", "open(", "requests.", "httpx.", "datetime.now(",
                      "from paper_trader.api"):
        assert forbidden not in src, forbidden


def test_87_no_new_write_route_was_introduced(audit, report):
    routes = report["routes"]["routes"]
    for r in routes:
        if "normal-cycle" in r["path"] or "data-gap" in r["path"]:
            assert r["method"] == "GET", r


def test_88_the_stage22_contract_never_creates_an_order_or_promotes_a_model():
    v = nc.build_cycle_view(overall="MANUAL_REVIEW_REQUIRED")
    assert v["creates_orders"] is False and v["automatic_execution"] is False
    r = _load()
    assert r["safety"]["created_orders"] is False
    assert r["safety"]["promoted_model"] is False
    assert r["model_governance_state"]["automatic_promotion_allowed"] is False
