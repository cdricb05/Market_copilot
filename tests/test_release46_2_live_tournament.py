"""Release 46.2 - portfolio-attention consistency, and a tournament that advances.

Two defects are locked shut here.

**The operator-state contradiction.** The governed 2026-08-25 Daily Research Cycle
produced a portfolio reassessment of MANUAL_REVIEW_REQUIRED naming seven hard
constraint breaches on retained holdings, a canonical portfolio decision of
BLOCKED, and an explanation that said in so many words that a human had to
adjudicate it. The SAME payload reported DAILY_CYCLE_COMPLETE, "Monitor the
portfolio." and ``no_action_required = true``. The tests below prove the three
things that had to become true for that to be impossible:

* a constraint breach is a REVIEW, and a review is neither a mutation nor nothing;
* the overall state may not be a completion state while a review is required;
* and the invariant that says so is checked on the composed payload, so a
  regression fails here rather than in a browser.

**A tournament nobody advanced.** Release 46 put eleven TRUE_FORWARD predictions on
the record and could only be advanced by re-running the whole campaign by hand -
the same shape as the five earlier releases that each froze a shadow registry and
never called its capture owner again. The remaining tests prove that the daily
cycle now scores what genuinely matured, emits idempotently, isolates a broken
challenger from the healthy ones, and cannot rewrite a forecast it has already
made.
"""
from __future__ import annotations

import copy
import datetime as dt
import json

import pytest

from alpha_agent import r46 as R46
from alpha_agent.r46 import advance as AD
from alpha_agent.r46 import clock as CK
from alpha_agent.r46 import contract as C
from alpha_agent.r46 import emit as EM
from alpha_agent.r46 import judge as JD
from alpha_agent.r46 import leaderboard as LB
from alpha_agent.r46 import ledger as LG
from alpha_agent.r46 import registry as RG
from api import daily_research_cycle as DRC
from api import prospective_tournament as PT
from api import workflow_state as WS
from engine import normal_cycle as NC

TEST_CAMPAIGN = "r46_2_pytest_campaign"

#: The exact breach set the live 2026-08-25 payload carried.
LIVE_BREACHES = ["ABNB:SECTOR_WEIGHT_BREACH", "AMD:RISK_CONTRIBUTION_BREACH",
                 "CVS:SECTOR_WEIGHT_BREACH", "DXCM:SECTOR_WEIGHT_BREACH",
                 "EXPE:SECTOR_WEIGHT_BREACH", "ITW:SECTOR_WEIGHT_BREACH",
                 "LH:SECTOR_WEIGHT_BREACH"]


@pytest.fixture()
def sandbox(tmp_path, monkeypatch):
    """Point every R46 write at a temp root. The real ledger is never touched."""
    monkeypatch.setattr(R46, "RESEARCH_ROOT", tmp_path / "r46root")
    monkeypatch.setattr(C, "ARTIFACT_DIR", tmp_path / "r46root" / TEST_CAMPAIGN)
    return tmp_path


# =========================================================================== #
# PART A - the portfolio-attention semantics (Objective A)
# =========================================================================== #
def test_constraint_breach_is_a_review_not_a_mutation_and_not_nothing():
    v = WS.portfolio_attention(reassessment_state="MANUAL_REVIEW_REQUIRED",
                               proposal_required=False, blockers=LIVE_BREACHES)
    assert v["attention_kind"] == WS.ATTENTION_REVIEW
    assert v["review_required"] is True
    assert v["review_reason"] == WS.REVIEW_REASON_CONSTRAINT_BREACH
    assert v["operator_action"] == WS.ACTION_REVIEW_PORTFOLIO_CONSTRAINT_BREACH
    assert v["constraint_breaches"] == sorted(LIVE_BREACHES)
    assert v["constraint_breach_count"] == 7


def test_a_review_creates_nothing():
    v = WS.portfolio_attention(reassessment_state="MANUAL_REVIEW_REQUIRED",
                               proposal_required=False, blockers=LIVE_BREACHES)
    for flag in ("creates_orders", "creates_proposal", "mutates_portfolio",
                 "requires_rebalance_approval", "is_a_mutation"):
        assert v[flag] is False, flag
    assert v["automation_off"] is True
    assert v["paper_only"] is True


def test_an_economically_justified_proposal_is_the_other_review_reason():
    v = WS.portfolio_attention(reassessment_state="PROPOSAL_READY",
                               proposal_required=True, blockers=[])
    assert v["review_reason"] == WS.REVIEW_REASON_PROPOSAL
    assert v["operator_action"] == WS.ACTION_REVIEW_PORTFOLIO_PROPOSAL


def test_stage19_precedence_still_suppresses_a_proposal_during_execution():
    """A commitment outranks a competing proposal. That rule is unchanged."""
    v = WS.portfolio_attention(reassessment_state="PROPOSAL_READY",
                               proposal_required=True, blockers=[],
                               execution_active=True)
    assert v["review_required"] is False
    assert v["proposal_review_suppressed_by_execution"] is True


def test_a_constraint_breach_is_NOT_suppressed_by_an_in_flight_execution():
    """It is not a proposal. It is a condition of the book a person must settle."""
    v = WS.portfolio_attention(reassessment_state="MANUAL_REVIEW_REQUIRED",
                               proposal_required=False, blockers=LIVE_BREACHES,
                               execution_active=True)
    assert v["review_required"] is True
    assert v["review_reason"] == WS.REVIEW_REASON_CONSTRAINT_BREACH


def test_a_clean_reassessment_requires_no_attention():
    v = WS.portfolio_attention(reassessment_state="CURRENT_NO_CHANGE",
                               proposal_required=False, blockers=[])
    assert v["attention_kind"] == WS.ATTENTION_NONE
    assert v["review_required"] is False
    assert v["operator_action"] is None


def test_constraint_breach_codes_are_recognised_individually():
    for code in WS.CONSTRAINT_BREACH_CODES:
        assert WS.constraint_breaches_of(["XYZ:%s" % code]) == ["XYZ:%s" % code]
    assert WS.constraint_breaches_of(["HOLDING_OPPORTUNITY_COST_NOT_RUN"]) == []


def test_blocked_reassessment_states_are_untouched_by_the_new_rule():
    """Stage 22.1's blocked-cycle rule answers a DIFFERENT question and still does."""
    for state in ("BLOCKED_DATA", "BLOCKED_EVIDENCE"):
        assert WS.portfolio_attention(reassessment_state=state,
                                      proposal_required=False,
                                      blockers=[])["review_required"] is False
    assert WS.reassessment_blocks_cycle(reassessment_state="BLOCKED_DATA",
                                        blockers=[]) is True
    # ...and a constraint breach reached a verdict, so it never blocks the cycle.
    assert WS.reassessment_blocks_cycle(
        reassessment_state="MANUAL_REVIEW_REQUIRED",
        blockers=LIVE_BREACHES) is False


# --------------------------------------------------------------------------- #
# The overall-state precedence
# --------------------------------------------------------------------------- #
def _decide(**over):
    kw = dict(inconsistent=False, session_status="AFTER_SESSION_CLOSE",
              has_confirmed_eligible=True, eligible_session_closed=True,
              owned_data_lag=False, research_current=True,
              assessment_status=WS.ASSESS_CURRENT, manual_review_required=False,
              evidence_gap=False, cycle_complete=True, hoc_current=True)
    kw.update(over)
    return WS._decide_overall(**kw)


def test_a_review_requirement_outranks_daily_cycle_complete():
    assert _decide(manual_review_required=False) == WS.DAILY_CYCLE_COMPLETE
    assert _decide(manual_review_required=True) == WS.MANUAL_REVIEW_REQUIRED


def test_a_review_requirement_outranks_an_evidence_gap_too():
    assert _decide(manual_review_required=True,
                   evidence_gap=True) == WS.MANUAL_REVIEW_REQUIRED


def test_a_named_blocker_still_outranks_a_review():
    """An input the operator has to restore names an EARLIER fix. Unchanged."""
    assert _decide(manual_review_required=True,
                   reassessment_blocked=True) == WS.RESEARCH_CYCLE_BLOCKED


# --------------------------------------------------------------------------- #
# The pure cycle kernel
# --------------------------------------------------------------------------- #
def test_manual_review_opens_a_review_gate_and_no_mutation_gate():
    view = NC.build_cycle_view(overall="MANUAL_REVIEW_REQUIRED",
                               action_available=False,
                               action_label="Review the portfolio constraint breach")
    gates = view["stage_gates"]
    assert gates[NC.STAGE_PORTFOLIO_DECISION]["review_required"] is True
    assert view["executable_stages"] == []
    assert view["executable_stage_count"] == 0
    assert all(not g["execution_allowed"] for g in gates.values())
    assert all(g["creates_orders"] is False for g in gates.values())


def test_an_open_review_gate_can_never_report_no_action_required():
    """THE regression. This exact combination shipped on 2026-08-25."""
    view = NC.build_cycle_view(overall="MANUAL_REVIEW_REQUIRED",
                               action_available=False,
                               action_label="Review the portfolio constraint breach")
    assert view["no_action_required"] is False
    assert view["action_required"] is True
    assert view["attention_kind"] == NC.ATTENTION_REVIEW
    assert view["review_required"] is True
    assert view["review_stages"] == [NC.STAGE_PORTFOLIO_DECISION]
    assert view["do_text"] == "Review the portfolio constraint breach"
    assert "No action required" not in view["do_text"]


def test_a_review_is_declared_to_be_no_kind_of_write():
    view = NC.build_cycle_view(overall="MANUAL_REVIEW_REQUIRED",
                               action_available=False, action_label="Review")
    for flag in ("review_creates_orders", "review_creates_proposal",
                 "review_mutates_portfolio", "review_is_a_mutation"):
        assert view[flag] is False, flag
    assert view["creates_orders"] is False
    assert view["automatic_execution"] is False


def test_a_genuinely_quiet_cycle_still_says_no_action_required():
    view = NC.build_cycle_view(overall="DAILY_CYCLE_COMPLETE",
                               action_available=False)
    assert view["no_action_required"] is True
    assert view["attention_kind"] == NC.ATTENTION_NONE
    assert view["review_required"] is False
    assert view["do_text"] == "No action required right now"


def test_a_mutation_still_outranks_a_review_in_the_attention_kind():
    view = NC.build_cycle_view(overall="READY_FOR_DAILY_CLOSE",
                               action_available=True,
                               action_label="Run the Daily Close")
    assert view["attention_kind"] == NC.ATTENTION_MUTATION
    assert view["executable_stages"] == [NC.STAGE_DAILY_CLOSE]
    assert view["no_action_required"] is False


def test_recovery_is_still_its_own_attention_kind():
    view = NC.build_cycle_view(overall="RESEARCH_CYCLE_BLOCKED",
                               action_available=False)
    assert view["attention_kind"] == NC.ATTENTION_RECOVERY
    assert view["no_action_required"] is False


def test_stage19_execution_closes_the_review_gate_but_not_the_cycle_position():
    view = NC.build_cycle_view(overall="MANUAL_REVIEW_REQUIRED",
                               action_available=False, execution_active=True)
    assert view["current_stage"] == NC.STAGE_CONTROLLED_REBALANCE
    assert view["review_required"] is False


# --------------------------------------------------------------------------- #
# The kernel reads the canonical verdict; it does not make a second one.
# --------------------------------------------------------------------------- #
def test_a_stated_review_verdict_is_obeyed_wherever_the_cycle_stands():
    """The defect: the review gate needed the review to BE the overall state."""
    derived = NC.build_cycle_view(overall="WAITING_FOR_SESSION_CLOSE",
                                  action_available=False)
    assert derived["review_required"] is False
    assert derived["review_verdict_source"] == "DERIVED"

    stated = NC.build_cycle_view(overall="WAITING_FOR_SESSION_CLOSE",
                                 action_available=False, review_required=True)
    assert stated["review_required"] is True
    assert stated["review_stages"] == [NC.REVIEW_STAGE]
    assert stated["review_verdict_source"] == "STATED"
    assert stated["no_action_required"] is False
    assert stated["attention_kind"] == NC.ATTENTION_REVIEW


def test_a_stated_review_never_becomes_a_mutation():
    stated = NC.build_cycle_view(overall="READY_FOR_DAILY_CLOSE",
                                 action_available=True,
                                 action_label="Run the Daily Close",
                                 review_required=True)
    # The mutation still wins the headline and the executable lane...
    assert stated["attention_kind"] == NC.ATTENTION_MUTATION
    assert stated["executable_stages"] == [NC.STAGE_DAILY_CLOSE]
    assert stated["executable_stage_count"] == 1
    # ...and the review is still reported, creating nothing.
    assert stated["review_required"] is True
    assert stated["review_creates_orders"] is False
    assert stated["review_is_a_mutation"] is False
    assert stated["stage_gates"][NC.REVIEW_STAGE]["execution_allowed"] is False


def test_a_stated_verdict_is_not_re_suppressed_by_an_in_flight_execution():
    """The caller already applied Stage-19 precedence. Applying it twice is how two
    owners disagree — and it is exactly what would re-hide a constraint breach."""
    stated = NC.build_cycle_view(overall="WAITING_FOR_SESSION_CLOSE",
                                 action_available=False, execution_active=True,
                                 review_required=True)
    assert stated["review_required"] is True
    # ...while the LEGACY derivation keeps its unchanged suppression.
    legacy = NC.build_cycle_view(overall="MANUAL_REVIEW_REQUIRED",
                                 action_available=False, execution_active=True)
    assert legacy["review_required"] is False


def test_a_stated_false_verdict_closes_the_gate_the_overall_state_would_open():
    stated = NC.build_cycle_view(overall="MANUAL_REVIEW_REQUIRED",
                                 action_available=False, review_required=False)
    assert stated["review_required"] is False
    assert stated["no_action_required"] is True


# --------------------------------------------------------------------------- #
# The strict cross-surface invariant
# --------------------------------------------------------------------------- #
def _attention():
    return WS.portfolio_attention(reassessment_state="MANUAL_REVIEW_REQUIRED",
                                  proposal_required=False, blockers=LIVE_BREACHES)


def test_the_invariant_fires_on_the_exact_shipped_contradiction():
    v = WS.portfolio_attention_violations(
        attention=_attention(), overall=WS.DAILY_CYCLE_COMPLETE,
        cycle_no_action_required=True, cycle_review_required=False)
    codes = {x["code"] for x in v}
    assert codes == {"PORTFOLIO_ATTENTION_CONTRADICTION",
                     "PORTFOLIO_ATTENTION_NO_ACTION_CONTRADICTION",
                     "PORTFOLIO_ATTENTION_REVIEW_GATE_CLOSED"}
    assert v[0]["constraint_breaches"] == sorted(LIVE_BREACHES)


def test_the_invariant_is_silent_once_the_state_is_correct():
    assert WS.portfolio_attention_violations(
        attention=_attention(), overall=WS.MANUAL_REVIEW_REQUIRED,
        cycle_no_action_required=False, cycle_review_required=True) == []


def test_the_invariant_says_nothing_when_no_review_is_required():
    quiet = WS.portfolio_attention(reassessment_state="CURRENT_NO_CHANGE",
                                   proposal_required=False, blockers=[])
    assert WS.portfolio_attention_violations(
        attention=quiet, overall=WS.DAILY_CYCLE_COMPLETE,
        cycle_no_action_required=True, cycle_review_required=False) == []


@pytest.mark.parametrize("state", ["DAILY_CYCLE_COMPLETE",
                                   "DAILY_CYCLE_COMPLETE_EVIDENCE_GAP",
                                   "WAITING_FOR_SESSION_CLOSE"])
def test_no_completion_state_may_coexist_with_a_required_review(state):
    v = WS.portfolio_attention_violations(
        attention=_attention(), overall=state,
        cycle_no_action_required=False, cycle_review_required=True)
    assert [x["code"] for x in v] == ["PORTFOLIO_ATTENTION_CONTRADICTION"]


# --------------------------------------------------------------------------- #
# Cross-surface: the whole composed payload
# --------------------------------------------------------------------------- #
#: The composed-payload cases run on an INJECTED clock and INJECTED read models.
#:
#: They originally ran against the live store and the real wall clock, which made them
#: pass in the evening and fail in the morning: before 16:00 ET the session gate
#: answered WAITING_FOR_SESSION_CLOSE and the assertions collapsed. That flakiness was
#: worth something exactly once — it exposed a real precedence defect (a required review
#: was ranked below "wait for the close") — and is worth nothing afterwards. Both clocks
#: are now covered deterministically instead, so the two situations are PROVEN rather
#: than sampled by whatever hour the suite happens to run at.
_ET = dt.timezone(dt.timedelta(hours=-4))          # America/New_York, August (EDT)
_ELIG = "2026-08-05"                               # the eligible completed session
#: 09:48 ET the NEXT morning — the exact live situation that broke: the eligible session
#: is fully processed, today's session is open, and the breach is still outstanding.
NOW_SESSION_OPEN = dt.datetime(2026, 8, 6, 9, 48, tzinfo=_ET)
#: 17:30 ET on the eligible session's own evening — the original 2026-08-25 situation.
NOW_AFTER_CLOSE = dt.datetime(2026, 8, 5, 17, 30, tzinfo=_ET)


def _seams(elig=_ELIG) -> dict:
    """Every read model the workflow owner composes, pinned. No live store, no clock."""
    return {
        "operational": {"operational_book": {
            "book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1",
            "current_status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
            "nav_as_of_date": elig, "desk_mark_date": elig,
            "latest_desk_mark_date": elig, "nav": 102241.79, "cash": 1500.0,
            "holdings_count": 25, "pending_order_count": 0,
            "current_target": {"alpha_market_date": elig,
                               "latest_completed_market_date": elig}}},
        # The HOC contract is OBSERVABLE and satisfied, so the post-close research
        # requirement is met and the state machine reaches the terminal region.
        "gate": {"latest_completed_market_date": elig, "outcome": "NO_ACTION_TODAY",
                 "headline": "No portfolio change required.",
                 "target_state": "CURRENT_ALIGNED", "action_required": False,
                 "next_scheduled_full_review": "2026-09-01",
                 "scheduled_review_due": False, "review_cadence": "MONTHLY",
                 "opportunity_cost_available": True,
                 "opportunity_cost_state": "ASSESSED",
                 "opportunity_cost_assessment_hash": "hoc_hash",
                 "opportunity_cost_market_date": elig,
                 "opportunity_cost_recommendation_counts": {"HOLD": 25}},
        "inputs": {"market_as_of_date": elig, "momentum_month": elig[:7],
                   "fundamental_as_of_date": "2026-05-22"},
        "daily_status": {"status": "DAILY_STATUS_READY",
                         "latest_valid_mark_date": elig},
        "desk_marks": {"series": {"SPY": [["2026-08-03", 757.67], [elig, 771.33]]},
                       "latest_completed_date": elig},
        "close_progress": {"market_date": elig, "done": True,
                           "final_close_status": "DAILY_CLOSE_COMPLETE_HOLD",
                           "status": "CLOSE_FINISHED"},
        "forward_status": {"latest_snapshot_date": elig, "snapshot_count": 5,
                           "evidence_state": "FORWARD_EVIDENCE_CURRENT",
                           "interpretation": "current",
                           "active_book": {"model_id": "m", "book_id": "x"},
                           "shadow_books": []},
        "target_readiness": {"dates": {"alpha_market_date": elig}},
        "research_cycle": {"state": "COMPLETE", "eligible_market_date": elig,
                           "governed_research_evidence_current": True},
    }


def _live_reassessment_summary(elig=_ELIG, **over):
    """The 2026-08-25 reassessment, as its canonical owner reported it."""
    summary = {
        "reassessment_available": True,
        "reassessment_state": "MANUAL_REVIEW_REQUIRED",
        "decision": "MANUAL_REVIEW_REQUIRED",
        "reassessment_id": "prs_live", "reassessment_hash": "h",
        "reassessment_date": elig,
        "proposal_required": False, "attention_count": 7,
        "holdings_evaluated": 25,
        "expected_net_improvement": None, "expected_one_way_turnover": None,
        "expected_transaction_cost_usd": None,
        "blockers": list(LIVE_BREACHES), "reason_codes": [],
        "explanation": ("A holding breaches a hard portfolio constraint that "
                        "requires human adjudication (%s)." % ", ".join(LIVE_BREACHES)),
        "policy_version": "x", "owner": "api.portfolio_reassessment",
    }
    summary.update(over)
    return summary


def _compose(*, now, summary=None):
    return WS.load_workflow_state(
        now=now, reassessment_summary=summary or _live_reassessment_summary(),
        **copy.deepcopy(_seams()))


@pytest.fixture(params=[NOW_SESSION_OPEN, NOW_AFTER_CLOSE],
                ids=["session_open", "after_close"])
def composed(request):
    """The breach payload, proven at BOTH hours it can be read at."""
    return _compose(now=request.param)


def test_the_composed_payload_no_longer_says_the_cycle_is_complete(composed):
    assert composed["overall_state"] == WS.MANUAL_REVIEW_REQUIRED
    assert composed["current_task"] == "Review the portfolio constraint breach."
    assert "Monitor the portfolio" not in composed["current_task"]


def test_the_composed_payload_names_the_breach_the_operator_must_adjudicate(composed):
    att = composed["portfolio_attention"]
    assert att["review_reason"] == WS.REVIEW_REASON_CONSTRAINT_BREACH
    assert att["constraint_breaches"] == sorted(LIVE_BREACHES)
    assert composed["portfolio_review_required"] is True
    assert composed["portfolio_attention_kind"] == WS.ATTENTION_REVIEW


def test_every_surface_reads_the_same_verdict(composed):
    """Top level, the reassessment lane and the cycle view cannot disagree."""
    top = composed["portfolio_attention"]
    lane = composed["portfolio_reassessment"]["portfolio_attention"]
    assert top == lane
    assert composed["normal_cycle"]["review_required"] is True
    assert composed["normal_cycle"]["no_action_required"] is False
    assert composed["normal_cycle"]["attention_kind"] == NC.ATTENTION_REVIEW


def test_the_composed_payload_is_internally_consistent(composed):
    codes = {v.get("code") for v in composed["consistency_violations"]}
    assert "PORTFOLIO_ATTENTION_CONTRADICTION" not in codes
    assert "PORTFOLIO_ATTENTION_NO_ACTION_CONTRADICTION" not in codes
    assert "PORTFOLIO_ATTENTION_REVIEW_GATE_CLOSED" not in codes


def test_reviewing_the_breach_offers_no_execution_and_creates_no_order(composed):
    pa = composed["primary_action"]
    assert pa["execution_available"] is False
    assert pa["execution_kind"] is None
    assert pa["execution_contract"] is None
    assert composed["normal_cycle"]["executable_stages"] == []
    assert composed["normal_cycle"]["creates_orders"] is False


def test_reviewing_the_breach_creates_no_proposal_and_no_rebalance(composed):
    cpd = composed["canonical_portfolio_decision"]
    assert cpd["creates_orders"] is False
    assert cpd["manual_review_only"] is True
    assert composed["portfolio_reassessment"]["proposal_required"] is False
    assert composed["portfolio_attention"]["creates_proposal"] is False
    assert composed["portfolio_attention"]["requires_rebalance_approval"] is False
    assert composed["normal_cycle"]["stage_gates"][
        NC.STAGE_CONTROLLED_REBALANCE]["execution_allowed"] is False


def _clean_summary():
    return _live_reassessment_summary(
        reassessment_state="CURRENT_NO_CHANGE", decision="CURRENT_NO_CHANGE",
        blockers=[], attention_count=0,
        explanation="No portfolio change is economically justified.")


@pytest.mark.parametrize("now,expected",
                         [(NOW_AFTER_CLOSE, WS.DAILY_CYCLE_COMPLETE),
                          (NOW_SESSION_OPEN, WS.WAITING_FOR_SESSION_CLOSE)])
def test_a_clean_reassessment_still_reaches_a_quiet_terminal_state(now, expected):
    """Nothing outstanding ⇒ the workflow is FREE to say nothing is outstanding.

    Both quiet answers are legitimate and hour-dependent: after the close the eligible
    session is complete; while the session is open the honest answer is that there is
    nothing to do but wait. The point of the release is not that either state is
    forbidden — it is that neither may be said over a live adjudication.
    """
    st = _compose(now=now, summary=_clean_summary())
    assert st["overall_state"] == expected
    assert st["overall_state"] in WS._NO_ATTENTION_OVERALL_STATES
    assert st["portfolio_review_required"] is False
    assert st["portfolio_attention_kind"] == WS.ATTENTION_NONE
    assert st["normal_cycle"]["no_action_required"] is True
    assert st["consistency_status"] != WS.INCONSISTENT


# --------------------------------------------------------------------------- #
# The R46.2 regression repair: the SAME contradiction one gate higher up.
#
# R46.2 lifted a required review above the two COMPLETION states and stopped there, so
# the identical defect survived at a different hour. At 09:48 the next morning the live
# payload read WAITING_FOR_SESSION_CLOSE / "Wait for the current market session to
# close" / no_action_required=true with all seven breaches outstanding — and the R46.2
# invariant fired on it, which is the payload declaring ITSELF inconsistent every
# morning. These tests pin the repair from both directions.
# --------------------------------------------------------------------------- #
def test_a_review_outranks_waiting_for_the_session_to_close():
    assert _decide(session_status="BEFORE_SESSION_CLOSE",
                   manual_review_required=False) == WS.WAITING_FOR_SESSION_CLOSE
    assert _decide(session_status="BEFORE_SESSION_CLOSE",
                   manual_review_required=True) == WS.MANUAL_REVIEW_REQUIRED


def test_a_review_outranks_every_state_that_claims_nothing_is_outstanding():
    """...and exactly those. The set is the invariant's own, so they cannot drift."""
    for state in WS._NO_ATTENTION_OVERALL_STATES:
        assert state != _decide(
            session_status=("BEFORE_SESSION_CLOSE"
                            if state == WS.WAITING_FOR_SESSION_CLOSE
                            else "AFTER_SESSION_CLOSE"),
            manual_review_required=True,
            evidence_gap=(state == WS.DAILY_CYCLE_COMPLETE_EVIDENCE_GAP))


def test_a_review_does_NOT_outrank_a_state_that_names_real_work():
    """A review is not more urgent than the close; it is only more urgent than idle."""
    assert _decide(session_status="BEFORE_SESSION_CLOSE",
                   manual_review_required=True,
                   eligible_session_closed=False) == WS.READY_FOR_DAILY_CLOSE
    assert _decide(manual_review_required=True,
                   research_current=False) == WS.RESEARCH_CYCLE_REQUIRED
    assert _decide(manual_review_required=True,
                   owned_data_lag=True) == WS.WAITING_FOR_OWNED_DATA


def test_the_open_session_no_longer_hides_the_breach():
    """The exact live 09:48 payload, end to end."""
    st = _compose(now=NOW_SESSION_OPEN)
    assert st["overall_state"] == WS.MANUAL_REVIEW_REQUIRED
    assert st["current_task"] == "Review the portfolio constraint breach."
    assert "Wait for the current market session" not in st["current_task"]
    assert st["consistency_status"] != WS.INCONSISTENT
    assert st["normal_cycle"]["executable_stages"] == []


def test_an_outranked_review_is_still_reported_as_outstanding():
    """A due Daily Close wins the headline; it does not make the breach disappear.

    This is the half the kernel could not express before: it derived the review from the
    overall state, so a review that anything outranked read as "no review". The payload
    then had to choose between naming the close and naming the breach, and whichever it
    named, the other silently became false.
    """
    st = _compose(now=NOW_SESSION_OPEN,
                  summary=_live_reassessment_summary())
    nc = st["normal_cycle"]
    assert nc["review_verdict_source"] == "STATED"
    st2 = WS.load_workflow_state(
        now=NOW_SESSION_OPEN, reassessment_summary=_live_reassessment_summary(),
        **dict(copy.deepcopy(_seams()),
               close_progress={"market_date": "2026-08-04", "done": False,
                               "status": "CLOSE_NOT_STARTED"}))
    assert st2["overall_state"] != WS.MANUAL_REVIEW_REQUIRED   # the close outranks it
    assert st2["portfolio_review_required"] is True            # ...and it is still true
    assert st2["normal_cycle"]["review_required"] is True
    assert st2["normal_cycle"]["no_action_required"] is False
    assert st2["consistency_status"] != WS.INCONSISTENT
    assert nc["review_required"] is True


# =========================================================================== #
# PART B - the live prospective tournament (Objective B)
# =========================================================================== #
def _valid_row(**over) -> dict:
    row = {f: None for f in C.PREDICTION_RECORD_FIELDS}
    row.update({
        "prediction_id": "p1", "batch_id": "b1",
        "challenger_id": "c", "challenger_version": "v1",
        "challenger_spec_hash": "h",
        "emitted_at_utc": "2026-08-25T20:00:00Z",
        "outcome_window_start_utc": "2026-08-26T00:00:00Z",
        "data_cutoff_utc": "2026-08-25T20:00:00Z",
        "data_cutoff_session": "2026-08-24",
        "effective_as_of": "2026-08-26",
        "asset_class": "US_EQUITY", "instrument": "BOOK:X",
        "horizon": 2, "horizon_unit": "ELIGIBLE_SESSIONS",
        "benchmark": "CASH", "control": C.CONTROL_CASH,
        "cost_class": "US_EQUITY",
        "position_expression": {"legs": [
            {"instrument": "AAA", "weight": 0.5, "score": 1.0,
             "side": "LONG", "cost_class": "US_EQUITY"},
            {"instrument": "BBB", "weight": -0.5, "score": -1.0,
             "side": "SHORT", "cost_class": "US_EQUITY"}]},
        "point_in_time_status": C.PIT_OK,
        "forward_evidence_type": C.TRUE_FORWARD,
        "status": C.STATUS_PENDING,
    })
    row.update(over)
    return row


def _fake_series(monkeypatch, table):
    import pandas as pd

    def fake(sym):
        data = table.get(sym)
        if data is None:
            return None
        idx = pd.DatetimeIndex([pd.Timestamp(d) for d, _ in data])
        return pd.Series([float(v) for _, v in data], index=idx)

    monkeypatch.setattr(JD, "_series", fake)


def _matured_market(monkeypatch):
    """Three realised sessions: entry, +1, +2. A horizon-2 prediction matures."""
    _fake_series(monkeypatch, {
        "AAA": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 110.0)],
        "BBB": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 90.0)]})
    monkeypatch.setattr(JD.MD, "risk_free_per_session", lambda h: 0.0)
    monkeypatch.setattr(JD.MD, "risk_free_annual", lambda: {"state": "OK"})


# --------------------------------------------------------------------------- #
# Maturity, scoring and immutability
# --------------------------------------------------------------------------- #
def test_a_prediction_stays_pending_until_its_horizon_genuinely_matures(
        sandbox, monkeypatch):
    _fake_series(monkeypatch, {
        "AAA": [("2026-08-26", 100.0), ("2026-08-27", 101.0)],
        "BBB": [("2026-08-26", 100.0), ("2026-08-27", 99.0)]})
    LG.append_predictions([_valid_row()], TEST_CAMPAIGN)
    run = JD.score_pending(TEST_CAMPAIGN)
    assert run["n_newly_scored"] == 0
    assert run["n_still_pending"] == 1
    assert LG.outcomes(TEST_CAMPAIGN) == []


def test_maturity_counts_realised_sessions_not_calendar_days(sandbox, monkeypatch):
    """Two weekdays elapsed, but only ONE session printed. Nothing may score."""
    _fake_series(monkeypatch, {
        "AAA": [("2026-08-26", 100.0), ("2026-08-27", 101.0)],
        "BBB": [("2026-08-26", 100.0), ("2026-08-27", 99.0)]})
    # The naive calendar estimate says it should be ripe...
    assert CK.expected_maturity_date(dt.date(2026, 8, 26), 2) == dt.date(2026, 8, 28)
    # ...and the judge, which counts realised bars, refuses.
    LG.append_predictions([_valid_row()], TEST_CAMPAIGN)
    assert JD.score_pending(TEST_CAMPAIGN)["n_newly_scored"] == 0


def test_a_matured_prediction_is_scored_against_its_declared_control(
        sandbox, monkeypatch):
    _matured_market(monkeypatch)
    LG.append_predictions([_valid_row()], TEST_CAMPAIGN)
    run = JD.score_pending(TEST_CAMPAIGN)
    assert run["n_newly_scored"] == 1
    out = LG.outcomes(TEST_CAMPAIGN)[0]
    # +10% long half, -10% short half, each at half weight = +10% gross.
    assert out["realised_gross_return"] == pytest.approx(0.10, abs=1e-9)
    assert out["realised_cost"] > 0
    assert out["realised_net_return"] == pytest.approx(
        out["realised_gross_return"] - out["realised_cost"], abs=1e-12)
    assert out["control"] == C.CONTROL_CASH
    assert out["net_alpha_vs_control"] == pytest.approx(
        out["realised_net_return"] - out["control_return"], abs=1e-12)
    assert out["status"] == C.STATUS_SCORED
    assert out["forward_evidence_type"] == C.TRUE_FORWARD


def test_cost_is_charged_on_both_sides_of_the_round_trip(sandbox, monkeypatch):
    _matured_market(monkeypatch)
    LG.append_predictions([_valid_row()], TEST_CAMPAIGN)
    JD.score_pending(TEST_CAMPAIGN)
    out = LG.outcomes(TEST_CAMPAIGN)[0]
    assert out["realised_cost_entry_side"] == pytest.approx(
        out["realised_cost_exit_side"], abs=1e-12)
    assert out["realised_cost"] == pytest.approx(
        out["realised_cost_entry_side"] * 2, abs=1e-12)


def test_scoring_is_idempotent(sandbox, monkeypatch):
    _matured_market(monkeypatch)
    LG.append_predictions([_valid_row()], TEST_CAMPAIGN)
    assert JD.score_pending(TEST_CAMPAIGN)["n_newly_scored"] == 1
    assert JD.score_pending(TEST_CAMPAIGN)["n_newly_scored"] == 0
    assert len(LG.outcomes(TEST_CAMPAIGN)) == 1


def test_scoring_never_rewrites_the_forecast_it_is_judging(sandbox, monkeypatch):
    _matured_market(monkeypatch)
    row = _valid_row()
    LG.append_predictions([row], TEST_CAMPAIGN)
    before = json.dumps(LG.predictions(TEST_CAMPAIGN), sort_keys=True)
    JD.score_pending(TEST_CAMPAIGN)
    after = json.dumps(LG.predictions(TEST_CAMPAIGN), sort_keys=True)
    assert before == after
    assert LG.verify(TEST_CAMPAIGN)["all_intact"] is True


def test_a_benchmark_relative_challenger_is_scored_net_of_the_benchmark(
        sandbox, monkeypatch):
    _fake_series(monkeypatch, {
        "AAA": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 110.0)],
        "BBB": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 100.0)],
        "SPY": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 105.0)]})
    monkeypatch.setattr(JD.MD, "risk_free_per_session", lambda h: 0.0)
    monkeypatch.setattr(JD.MD, "risk_free_annual", lambda: {"state": "OK"})
    out = JD.resolve(_valid_row(benchmark="SPY", control=C.CONTROL_BENCHMARK))
    assert out["realised_benchmark_return"] == pytest.approx(0.05, abs=1e-9)
    assert out["control_return"] == pytest.approx(0.05, abs=1e-9)
    assert out["realised_residual_return"] == pytest.approx(
        out["realised_gross_return"] - 0.05, abs=1e-9)


# --------------------------------------------------------------------------- #
# The advance step
# --------------------------------------------------------------------------- #
#: A freeze instant strictly BEFORE every emission instant used in these tests, so
#: "the specification was frozen before the prediction was emitted" is a real
#: comparison rather than an accident of when the suite happened to run.
FROZEN_AT = "2026-08-25T19:00:00Z"


def _register(sandbox_path, monkeypatch, specs=None, frozen_at=FROZEN_AT):
    # R52 fix: freshness is derived from the canonical clock rather than a
    # pinned 2026-08-25, which expired once the calendar moved more than the
    # feasibility MAX_LAG past it and turned every registration DATA_STALE.
    monkeypatch.setattr(EM.MD, "last_session",
                        lambda s: CK.eastern_date(CK.now_utc()))
    return RG.register(TEST_CAMPAIGN, specs=specs, frozen_at=frozen_at)


def test_advance_reports_not_registered_on_an_empty_research_root(sandbox):
    res = AD.advance(TEST_CAMPAIGN)
    assert res["state"] == AD.STATE_NOT_REGISTERED
    assert res["available"] is False


def test_advance_scores_before_it_emits(sandbox, monkeypatch):
    """The ordering the whole release rests on, proved rather than asserted."""
    _matured_market(monkeypatch)
    calls: list = []
    monkeypatch.setattr(RG, "load", lambda cid=TEST_CAMPAIGN: {
        "challengers": [{"challenger_id": "c", "state": C.FORWARD_PENDING,
                         "challenger_version": "v1", "family": "F",
                         "asset_class": "US_EQUITY", "horizons": [2]}]})
    real_score, real_emit = JD.score_pending, EM.emit
    monkeypatch.setattr(JD, "score_pending",
                        lambda *a, **k: calls.append("score") or real_score(*a, **k))
    monkeypatch.setattr(EM, "emit",
                        lambda *a, **k: calls.append("emit") or {"n_appended": 0})
    monkeypatch.setattr(LB, "build", lambda *a, **k: {"rows": []})
    LG.append_predictions([_valid_row()], TEST_CAMPAIGN)
    AD.advance(TEST_CAMPAIGN)
    assert calls == ["score", "emit"]


def test_advance_is_idempotent(sandbox, monkeypatch):
    _matured_market(monkeypatch)
    monkeypatch.setattr(RG, "load", lambda cid=TEST_CAMPAIGN: {
        "challengers": [{"challenger_id": "c", "state": C.FORWARD_PENDING,
                         "challenger_version": "v1", "family": "F",
                         "asset_class": "US_EQUITY", "horizons": [2]}]})
    monkeypatch.setattr(EM, "emit", lambda *a, **k: {"n_appended": 0})
    monkeypatch.setattr(LB, "build", lambda *a, **k: {"rows": []})
    LG.append_predictions([_valid_row()], TEST_CAMPAIGN)
    first = AD.advance(TEST_CAMPAIGN)
    second = AD.advance(TEST_CAMPAIGN)
    assert first["tournament_outcomes_scored"] == 1
    assert second["tournament_outcomes_scored"] == 0
    assert first["state"] == AD.STATE_ADVANCED
    assert second["state"] == AD.STATE_NOTHING_DUE
    assert len(LG.outcomes(TEST_CAMPAIGN)) == 1


def test_advance_reports_the_six_manifest_facts(sandbox, monkeypatch):
    _matured_market(monkeypatch)
    monkeypatch.setattr(RG, "load", lambda cid=TEST_CAMPAIGN: {
        "challengers": [{"challenger_id": "c", "state": C.FORWARD_PENDING,
                         "challenger_version": "v1", "family": "F",
                         "asset_class": "US_EQUITY", "horizons": [2]}]})
    monkeypatch.setattr(EM, "emit", lambda *a, **k: {"n_appended": 0})
    monkeypatch.setattr(LB, "build", lambda *a, **k: {"rows": []})
    LG.append_predictions([_valid_row()], TEST_CAMPAIGN)
    res = AD.advance(TEST_CAMPAIGN)
    for f in ("tournament_predictions_matured", "tournament_outcomes_scored",
              "tournament_predictions_emitted", "tournament_challengers_active",
              "tournament_forward_evidence_count", "tournament_next_maturity"):
        assert f in res, f
    assert res["tournament_forward_evidence_count"] == 1
    assert res["tournament_predictions_matured"] == 1
    assert res["tournament_challengers_active"] == 1


def test_advance_writes_only_into_the_research_root(sandbox, monkeypatch):
    monkeypatch.setattr(RG, "load", lambda cid=TEST_CAMPAIGN: {
        "challengers": [{"challenger_id": "c", "state": C.FORWARD_PENDING}]})
    monkeypatch.setattr(EM, "emit", lambda *a, **k: {"n_appended": 0})
    monkeypatch.setattr(LB, "build", lambda *a, **k: {"rows": []})
    res = AD.advance(TEST_CAMPAIGN)
    assert res["operational_writes"] == 0
    assert res["orders_created"] == 0
    assert res["portfolio_mutations"] == 0
    assert res["promoted_models"] == 0
    assert res["money_spent_usd"] == 0.0
    assert str(sandbox) in res["writes_only_into"]


def test_a_failing_stage_never_propagates_out_of_the_advance(sandbox, monkeypatch):
    monkeypatch.setattr(RG, "load", lambda cid=TEST_CAMPAIGN: {
        "challengers": [{"challenger_id": "c", "state": C.FORWARD_PENDING}]})

    def boom(*a, **k):
        raise RuntimeError("provider exploded")

    monkeypatch.setattr(EM, "emit", boom)
    monkeypatch.setattr(LB, "build", lambda *a, **k: {"rows": []})
    res = AD.advance(TEST_CAMPAIGN)          # must not raise
    assert res["n_stage_failures"] == 1
    assert res["stage_failures"][0]["stage"] == "emit_batch"
    assert res["one_blocked_challenger_never_blocks_the_tournament"] is True


def test_advance_records_every_cycle(sandbox, monkeypatch):
    monkeypatch.setattr(RG, "load", lambda cid=TEST_CAMPAIGN: {
        "challengers": [{"challenger_id": "c", "state": C.FORWARD_PENDING}]})
    monkeypatch.setattr(EM, "emit", lambda *a, **k: {"n_appended": 0})
    monkeypatch.setattr(LB, "build", lambda *a, **k: {"rows": []})
    AD.advance(TEST_CAMPAIGN)
    AD.advance(TEST_CAMPAIGN)
    body = R46.read_json(R46.campaign_dir(TEST_CAMPAIGN) / AD.CYCLE_ARTIFACT)
    assert body["n_cycles_total"] == 2
    assert len(body["cycles"]) == 2


def test_forward_confirmation_never_confers_capital(sandbox, monkeypatch):
    monkeypatch.setattr(RG, "load", lambda cid=TEST_CAMPAIGN: {
        "challengers": [{"challenger_id": "c", "state": C.FORWARD_PENDING}]})
    monkeypatch.setattr(EM, "emit", lambda *a, **k: {"n_appended": 0})
    monkeypatch.setattr(LB, "build", lambda *a, **k: {"rows": []})
    res = AD.advance(TEST_CAMPAIGN)
    assert res["forward_confirmed_is_research_evidence_only"] is True
    assert res["promotion_requires_manual_governance"] is True


# --------------------------------------------------------------------------- #
# Emission, isolation and the leaderboard
# --------------------------------------------------------------------------- #
def test_emission_is_idempotent_within_a_session(sandbox, monkeypatch):
    reg = _register(sandbox, monkeypatch)
    now = dt.datetime(2026, 8, 25, 20, 0, tzinfo=dt.timezone.utc)
    first = EM.emit(TEST_CAMPAIGN, reg, now)
    second = EM.emit(TEST_CAMPAIGN, reg, now)
    assert second["n_appended"] == 0
    assert second["n_duplicates_skipped"] == first["n_appended"]


def test_one_broken_challenger_never_blocks_the_healthy_ones(sandbox, monkeypatch):
    """Before R46.2 a single raising rule aborted the WHOLE batch."""
    reg = _register(sandbox, monkeypatch)
    specs = list(RG.active_specs(reg))
    assert len(specs) >= 2
    broken_id = specs[0]["challenger_id"]
    real_build = EM.CH.build

    def flaky(spec):
        if spec["challenger_id"] == broken_id:
            raise ZeroDivisionError("empty window")
        return real_build(spec)

    monkeypatch.setattr(EM.CH, "build", flaky)
    batch = EM.build_batch(TEST_CAMPAIGN, reg,
                           dt.datetime(2026, 8, 25, 20, 0, tzinfo=dt.timezone.utc))
    reasons = {s["challenger_id"]: s["reason"] for s in batch["skipped"]}
    assert reasons[broken_id] == EM.REASON_BUILD_FAILED
    assert batch["n_predictions"] > 0
    assert broken_id not in {r["challenger_id"] for r in batch["rows"]}


def test_a_non_emitting_challenger_is_named_with_a_stable_reason(sandbox, monkeypatch):
    reg = _register(sandbox, monkeypatch)
    monkeypatch.setattr(EM.CH, "build",
                        lambda spec: {"state": "OK", "legs": [], "n_legs": 0})
    batch = EM.build_batch(TEST_CAMPAIGN, reg,
                           dt.datetime(2026, 8, 25, 20, 0, tzinfo=dt.timezone.utc))
    assert batch["n_predictions"] == 0
    assert {s["reason"] for s in batch["skipped"]} == {EM.REASON_FLAT}
    for s in batch["skipped"]:
        assert s["reason"] in EM.NON_EMISSION_REASONS


def test_a_registry_blocked_challenger_is_reported_not_silently_dropped(
        sandbox, monkeypatch):
    reg = _register(sandbox, monkeypatch)
    reg["challengers"][0]["state"] = C.DATA_BLOCKED
    reg["challengers"][0]["blocked_reason"] = "stream cannot accrue here"
    blocked_id = reg["challengers"][0]["challenger_id"]
    batch = EM.build_batch(TEST_CAMPAIGN, reg,
                           dt.datetime(2026, 8, 25, 20, 0, tzinfo=dt.timezone.utc))
    entry = next(s for s in batch["skipped"] if s["challenger_id"] == blocked_id)
    assert entry["reason"] == EM.REASON_DATA_BLOCKED
    assert entry["detail"] == "stream cannot accrue here"


def test_the_leaderboard_reflects_a_newly_scored_outcome(sandbox, monkeypatch):
    _matured_market(monkeypatch)
    reg = {"challengers": [{
        "challenger_id": "c", "challenger_version": "v1", "family": "F",
        "asset_class": "US_EQUITY", "horizons": [2], "state": C.FORWARD_PENDING,
        "point_in_time_status": C.PIT_OK, "control": C.CONTROL_CASH,
        "benchmark": "CASH"}]}
    LG.append_predictions([_valid_row()], TEST_CAMPAIGN)
    before = LB.build(TEST_CAMPAIGN, reg)
    assert before["total_forward_predictions_matured"] == 0
    JD.score_pending(TEST_CAMPAIGN)
    after = LB.build(TEST_CAMPAIGN, reg)
    assert after["total_forward_predictions_matured"] == 1
    row = next(r for r in after["rows"] if r["challenger_id"] == "c")
    assert row["raw_matured"] == 1
    assert row["promotion_allowed"] is False


def test_no_leaderboard_row_may_ever_read_proven(sandbox, monkeypatch):
    reg = {"challengers": [{
        "challenger_id": "c", "challenger_version": "v1", "family": "F",
        "asset_class": "US_EQUITY", "horizons": [2], "state": C.FORWARD_PENDING,
        "point_in_time_status": C.PIT_OK, "control": C.CONTROL_CASH,
        "benchmark": "CASH"}]}
    board = LB.build(TEST_CAMPAIGN, reg)
    assert board["no_row_may_read_proven"] is True
    assert "PROVEN" not in json.dumps(board["rows"]).upper()


# --------------------------------------------------------------------------- #
# Timestamp precision (forward-only)
# --------------------------------------------------------------------------- #
def test_the_frozen_whole_second_stamp_is_unchanged():
    t = dt.datetime(2026, 8, 25, 20, 24, 42, 123456, tzinfo=dt.timezone.utc)
    assert CK.iso(t) == "2026-08-25T20:24:42Z"


def test_the_precise_stamp_carries_microseconds():
    t = dt.datetime(2026, 8, 25, 20, 24, 42, 123456, tzinfo=dt.timezone.utc)
    assert CK.iso_precise(t) == "2026-08-25T20:24:42.123456Z"


def test_the_r46_first_batch_ambiguity_is_reported_honestly():
    """Two whole-second stamps in the SAME second cannot be ordered numerically."""
    ev = CK.ordering_evidence("2026-08-25T20:24:42Z", "2026-08-25T20:24:42Z")
    assert ev["decidable"] is False
    assert ev["resolution"] == "WHOLE_SECOND"


def test_two_precise_stamps_in_one_second_are_decidable():
    ev = CK.ordering_evidence("2026-08-25T20:24:42.100000Z",
                              "2026-08-25T20:24:42.900000Z")
    assert ev["decidable"] is True
    assert ev["strictly_ordered"] is True
    assert ev["resolution"] == "MICROSECOND"
    assert ev["delta_seconds"] == pytest.approx(0.8, abs=1e-6)


def test_whole_second_stamps_in_different_seconds_are_still_decidable():
    ev = CK.ordering_evidence("2026-08-25T20:24:42Z", "2026-08-25T20:24:43Z")
    assert ev["decidable"] is True
    assert ev["strictly_ordered"] is True


def test_new_rows_carry_the_precise_stamp(sandbox, monkeypatch):
    reg = _register(sandbox, monkeypatch)
    batch = EM.build_batch(TEST_CAMPAIGN, reg,
                           dt.datetime(2026, 8, 25, 20, 0, 1, 500000,
                                       tzinfo=dt.timezone.utc))
    assert batch["rows"], "the seed cohort must emit something to test"
    row = batch["rows"][0]
    assert row["emitted_at_utc"] == "2026-08-25T20:00:01Z"
    assert row["emitted_at_utc_precise"] == "2026-08-25T20:00:01.500000Z"
    assert row["timestamp_precision"] == "MICROSECOND"
    assert row["timestamp_precision_contract"] == EM.TIMESTAMP_PRECISION_CONTRACT
    assert row["freeze_before_emission_evidence"]["strictly_ordered"] is True


def test_a_legacy_row_without_precise_stamps_is_still_valid(sandbox):
    """Backward compatibility: the original eleven rows must keep validating."""
    legacy = _valid_row()
    assert "emitted_at_utc_precise" not in legacy
    LG.validate_prediction(legacy)
    assert LG.append_predictions([legacy], TEST_CAMPAIGN)["n_appended"] == 1


def test_a_registry_entry_frozen_before_r46_2_is_not_back_stamped(
        sandbox, monkeypatch):
    reg = _register(sandbox, monkeypatch)
    # Simulate the R46 cohort: registered, with no precise stamp on the record.
    for c in reg["challengers"]:
        c.pop("frozen_at_precise", None)
    R46.write_json(RG.registry_path(TEST_CAMPAIGN), reg)
    again = _register(sandbox, monkeypatch)
    for c in again["challengers"]:
        assert c["frozen_at_precise"] is None, (
            "back-stamping today's microseconds onto an older freeze would "
            "manufacture precision the record never had")


# --------------------------------------------------------------------------- #
# Daily Research Cycle integration
# --------------------------------------------------------------------------- #
def test_the_tournament_step_is_in_the_frozen_sequence():
    assert DRC.STEP_ADVANCE_TOURNAMENT in DRC.STEP_SEQUENCE


def test_the_tournament_is_scored_before_the_portfolio_lane_is_decided():
    """Ordering: inputs refreshed -> tournament -> opportunity cost -> reassess."""
    seq = list(DRC.STEP_SEQUENCE)
    assert seq.index(DRC.STEP_REFRESH_INPUTS) < seq.index(DRC.STEP_ADVANCE_TOURNAMENT)
    assert seq.index(DRC.STEP_CAPTURE_EVIDENCE) < seq.index(DRC.STEP_ADVANCE_TOURNAMENT)
    assert seq.index(DRC.STEP_ADVANCE_TOURNAMENT) < seq.index(DRC.STEP_HOLDING_OPP_COST)
    assert seq.index(DRC.STEP_ADVANCE_TOURNAMENT) < seq.index(DRC.STEP_REASSESS_PORTFOLIO)
    assert seq.index(DRC.STEP_ADVANCE_TOURNAMENT) < seq.index(DRC.STEP_RUN_RESEARCH_AGENT)


def _advance_result(**over) -> dict:
    body = {
        "available": True, "state": AD.STATE_ADVANCED,
        "state_vocabulary": list(AD.STATES),
        "calculation_owner": AD.CALCULATION_OWNER, "campaign_id": "cid",
        "eligible_market_date": "2026-08-25",
        "tournament_predictions_matured": 3, "tournament_outcomes_scored": 2,
        "tournament_predictions_emitted": 11, "tournament_challengers_active": 10,
        "tournament_forward_evidence_count": 22,
        "tournament_next_maturity": "2026-09-02",
        "pending_predictions": 19, "challengers_registered": 10,
        "challengers_blocked": 0, "ledger_chain_intact": True,
        "stage_failures": [],
        "emission": {"non_emission_reasons": ["FLAT_NO_POSITION"], "skipped": []},
        "leaderboard": {"top_forward_challenger": "r46_fx_xs_mom_252",
                        "top_forward_state": "EARLY_FORWARD_EVIDENCE",
                        "best_net_alpha_bps": 12.5},
    }
    body.update(over)
    return body


def test_the_manifest_reports_the_tournament_activity():
    rec = DRC._contract(
        state=DRC.COMPLETE, facts={"eligible": "2026-08-25"},
        prospective_tournament=DRC._extract_tournament(_advance_result(),
                                                       "2026-08-25"))
    assert rec["prospective_tournament_state"] == AD.STATE_ADVANCED
    assert rec["tournament_predictions_matured"] == 3
    assert rec["tournament_outcomes_scored"] == 2
    assert rec["tournament_predictions_emitted"] == 11
    assert rec["tournament_challengers_active"] == 10
    assert rec["tournament_forward_evidence_count"] == 22
    assert rec["tournament_next_maturity"] == "2026-09-02"
    assert rec["prospective_tournament_owner"] == "alpha_agent.r46.advance"
    assert rec["tournament_promotes_models"] is False


def test_the_manifest_states_why_challengers_did_not_emit():
    rec = DRC._contract(
        state=DRC.COMPLETE, facts={"eligible": "2026-08-25"},
        prospective_tournament=DRC._extract_tournament(_advance_result(),
                                                       "2026-08-25"))
    assert rec["tournament_non_emission_reasons"] == ["FLAT_NO_POSITION"]


def test_an_unavailable_tournament_does_not_break_the_manifest():
    rec = DRC._contract(state=DRC.COMPLETE, facts={"eligible": "2026-08-25"},
                        prospective_tournament=DRC._extract_tournament(None, "x"))
    assert rec["prospective_tournament_state"] == "UNAVAILABLE"
    assert rec["tournament_forward_evidence_count"] == 0


def test_a_terminal_manifest_must_state_what_the_tournament_did():
    rec = DRC._contract(
        state=DRC.COMPLETE, facts={"eligible": "2026-08-25"},
        step_results=[{"step_id": DRC.STEP_ADVANCE_TOURNAMENT,
                       "status": DRC.S_OK}])
    problems = DRC._validate_terminal_manifest(rec)
    assert any("prospective-tournament step OK but state missing" in p
               for p in problems)


def test_a_skipped_tournament_step_is_exempt_from_that_check():
    rec = DRC._contract(
        state=DRC.COMPLETE, facts={"eligible": "2026-08-25"},
        step_results=[{"step_id": DRC.STEP_ADVANCE_TOURNAMENT,
                       "status": DRC.S_SKIPPED}])
    assert not any("prospective-tournament" in p
                   for p in DRC._validate_terminal_manifest(rec))


def test_an_unregistered_tournament_is_skipped_not_failed():
    """A research root with no registry has nothing to advance. That is a quiet
    tournament, not a broken daily cycle — and the two must not read the same."""
    assert DRC.TOURNAMENT_NOT_REGISTERED == AD.STATE_NOT_REGISTERED
    body = AD._body("c", AD.STATE_NOT_REGISTERED, CK.now_utc(), [])
    assert body["available"] is False
    t = DRC._extract_tournament(body, "2026-08-25")
    assert t["state"] == DRC.TOURNAMENT_NOT_REGISTERED


# --------------------------------------------------------------------------- #
# The orchestration contract: no canonical step may go unreported.
# --------------------------------------------------------------------------- #
def _all_steps(**over):
    return [{"step_id": s, "status": over.get(s, DRC.S_OK)}
            for s in DRC.STEP_SEQUENCE]


def test_a_complete_manifest_accounts_for_every_canonical_step():
    rec = DRC._contract(state=DRC.COMPLETE, facts={"eligible": "2026-08-25"},
                        step_results=_all_steps())
    assert rec["accounted_steps"] == list(DRC.STEP_SEQUENCE)
    assert rec["unaccounted_steps"] == []
    assert rec["all_steps_accounted_for"] is True


def test_a_deliberately_skipped_step_is_still_accounted_for():
    """Skipping is legal. Staying silent is not — and the two looked identical."""
    rec = DRC._contract(
        state=DRC.COMPLETE, facts={"eligible": "2026-08-25"},
        step_results=_all_steps(**{DRC.STEP_ADVANCE_TOURNAMENT: DRC.S_SKIPPED}))
    assert DRC.STEP_ADVANCE_TOURNAMENT not in rec["completed_steps"]
    assert DRC.STEP_ADVANCE_TOURNAMENT in rec["skipped_steps"]
    assert rec["all_steps_accounted_for"] is True
    assert not any("never reported" in p
                   for p in DRC._validate_terminal_manifest(rec))


def test_a_silently_omitted_step_fails_terminal_validation():
    """The failure mode a new canonical step introduces: added to the sequence and
    never reached, while the manifest still calls the run COMPLETE."""
    partial = [s for s in _all_steps()
               if s["step_id"] != DRC.STEP_ADVANCE_TOURNAMENT]
    rec = DRC._contract(state=DRC.COMPLETE, facts={"eligible": "2026-08-25"},
                        step_results=partial)
    assert rec["unaccounted_steps"] == [DRC.STEP_ADVANCE_TOURNAMENT]
    assert rec["all_steps_accounted_for"] is False
    assert any(DRC.STEP_ADVANCE_TOURNAMENT in p and "never reported" in p
               for p in DRC._validate_terminal_manifest(rec))


# --------------------------------------------------------------------------- #
# A completed run is not erased by the clock.
#
# Removing the portfolio-attention mask exposed a second one underneath it: the cycle
# owner's own status returned "today's session is still open" AHEAD of reflecting the
# terminal manifest it had persisted the previous evening, publishing
# governed_research_evidence_current=False for a session whose governed cycle had
# demonstrably completed. The workflow owner consumes that fact, so the operator was
# told to run a cycle that had already run.
# --------------------------------------------------------------------------- #
_DRC_D = "2026-08-25"


def _drc_freshness(session_status="BEFORE_SESSION_CLOSE", *, owned=True):
    return {"market_session": {"session_status": session_status,
                               "latest_confirmed_owned_data_date": (_DRC_D if owned
                                                                    else None),
                               "operator_action": "WAIT"},
            "eligible_market_date": _DRC_D,
            "expected_completed_market_date": _DRC_D,
            "active_book": {"active_book_id": "b1", "active_book_name": "Book"},
            "consistency_status": "CONSISTENT", "consistency_violations": [],
            "source_freshness": []}


def _persist_complete_run(tmp, fresh):
    facts = DRC._facts(fresh)
    rec = DRC._contract(
        state=DRC.COMPLETE, facts=facts, run_id="drc_test_run",
        started_at="2026-08-25T20:00:00+00:00",
        completed_at="2026-08-25T20:25:00+00:00",
        step_results=_all_steps(), executable=False)
    rec["run_id"] = "drc_test_run"
    DRC._save_run(rec, str(tmp))
    DRC._update_index(eligible_date=_DRC_D, idempotency_key=facts["idempotency_key"],
                      input_contract_hash=facts["input_contract_hash"],
                      run_id="drc_test_run", state=DRC.COMPLETE, drc_dir=str(tmp))
    return rec


def test_an_open_session_no_longer_erases_yesterdays_completed_cycle(tmp_path):
    fresh = _drc_freshness()
    _persist_complete_run(tmp_path, fresh)
    s = DRC.load_daily_research_cycle_status(drc_dir=str(tmp_path), freshness=fresh)
    assert s["state"] == DRC.COMPLETE
    assert s["eligible_market_date"] == _DRC_D
    assert s["completed_steps"] == list(DRC.STEP_SEQUENCE)
    # ...and reporting it opens nothing.
    assert s["executable"] is False


def test_the_session_wait_still_stands_when_no_run_was_ever_completed(tmp_path):
    s = DRC.load_daily_research_cycle_status(drc_dir=str(tmp_path),
                                             freshness=_drc_freshness())
    assert s["state"] == DRC.WAITING_FOR_SESSION_CLOSE
    assert s["executable"] is False


def test_an_untrustworthy_input_still_outranks_a_completed_run(tmp_path):
    """A finished run does not answer "can these inputs be trusted?", so the two
    gates that ask that question keep their precedence."""
    for status, expected in (("WAITING_FOR_OWNED_DATA", DRC.WAITING_FOR_OWNED_DATA),
                             ("INCONSISTENT_FUTURE_DATA", DRC.INCONSISTENT)):
        fresh = _drc_freshness(status, owned=False)
        _persist_complete_run(tmp_path, fresh)
        s = DRC.load_daily_research_cycle_status(drc_dir=str(tmp_path),
                                                 freshness=fresh)
        assert s["state"] == expected


def test_the_cycle_still_refuses_to_RUN_before_the_close(tmp_path):
    """Reporting what finished is not permission to start something new."""
    fresh = _drc_freshness()
    _persist_complete_run(tmp_path, fresh)
    r = DRC.run_daily_research_cycle(confirm=DRC.EXECUTE_CONFIRMATION,
                                     drc_dir=str(tmp_path), freshness=fresh)
    assert r["state"] == DRC.WAITING_FOR_SESSION_CLOSE
    assert r["safety"]["performed_research_write"] is False


def test_the_extracted_tournament_declares_what_it_never_does():
    t = DRC._extract_tournament(_advance_result(), "2026-08-25")
    assert t["creates_orders"] is False
    assert t["mutates_portfolio"] is False
    assert t["promotes_models"] is False
    assert t["research_root_only"] is True


# --------------------------------------------------------------------------- #
# The operator surface
# --------------------------------------------------------------------------- #
def test_the_tournament_surface_separates_history_from_forward_proof(monkeypatch):
    monkeypatch.setattr(PT, "_read", lambda p: None)
    view = PT.load_prospective_tournament()
    hv = view["historical_qualification_vs_forward_proof"]
    assert hv["historical_qualification_is_not_proof"] is True
    assert hv["forward_proof_requires_matured_true_forward_rows"] is True
    assert view["proven_alpha_is_not_a_state"] is True


def test_the_missing_cycles_artifact_is_not_an_operator_warning(monkeypatch):
    monkeypatch.setattr(PT, "_read", lambda p: None)
    view = PT.load_prospective_tournament()
    assert not any("R46_TOURNAMENT_CYCLES" in w for w in view["warnings"])
    assert view["tournament_advance"]["has_ever_advanced"] is False


def test_the_next_maturity_comes_from_the_live_ledger_not_a_cached_verdict():
    """A cached date keeps advertising evidence that has already landed."""
    assert PT._next_maturity({"NEXT_MATERIAL_EVIDENCE_TIME": "2026-08-27"},
                             []) is None
    assert PT._next_maturity(
        {"NEXT_MATERIAL_EVIDENCE_TIME": "2026-08-27"},
        [{"horizon_end_expected": "2026-09-02"}]) == "2026-09-02"


def test_the_surface_reports_scored_outcomes_against_their_control():
    outs = [{"prediction_id": "p", "challenger_id": "c", "horizon": 5,
             "realised_gross_return": 0.02, "realised_cost": 0.001,
             "realised_net_return": 0.019, "control": C.CONTROL_CASH,
             "control_return": 0.0004, "net_alpha_vs_control": 0.0186,
             "realised_residual_return": None, "hit": True}]
    rows = PT._scored_outcomes(outs)
    assert rows[0]["net_alpha_vs_control"] == 0.0186
    assert rows[0]["one_outcome_is_not_alpha"] is True


def test_the_surface_never_calls_one_outcome_alpha():
    state = PT._maturity_state([{"a": 1}], [{"b": 1}], [{"forward_evidence_score": 0.02}])
    assert state == PT.MATURITY_ACCRUING
    assert PT._maturity_state([{"a": 1}], [], []) == PT.MATURITY_AWAITING_FIRST
    assert PT._maturity_state([], [], []) == PT.MATURITY_NO_FORWARD_EVIDENCE


def test_the_surface_still_creates_nothing(monkeypatch):
    monkeypatch.setattr(PT, "_read", lambda p: None)
    view = PT.load_prospective_tournament()
    for flag, expected in view["no_live_trading"].items():
        if isinstance(expected, bool):
            assert expected is False, flag
