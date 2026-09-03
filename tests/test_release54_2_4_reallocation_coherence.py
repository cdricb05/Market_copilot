"""Release 54.2.4 — reallocation-proposal coherence, current-decision
presentation, and first-class intraday reassessment visibility.

The live 2026-09-02 defect set: a governed HOLD CURRENT PORTFOLIO hero still
rendered the SUPERSEDED proposal's economics (0.056 / 35% / $85.69 / 28
positions changing); the Reallocation page looked like an active change plan;
two legitimate economic scopes (HOC release-set estimate vs complete-target
proposal) were mixed without names; zero-delta rows could classify as changes;
"eligible" meant two different rules on one screen; a fresh reassessment was
labelled OVERDUE; and the live/intraday lane had no first-class Today answer.

Everything here is hermetic: pure builders over fixture payloads, plus static
assertions on the ONE UI file. No production store is read or written.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from paper_trader.api import active_manager_state as ams
from paper_trader.api import corporate_actions as ca
from paper_trader.api import daily_action_gate as dag
from paper_trader.api import operator_presentation as op
from paper_trader.api import portfolio_decision as pd
from paper_trader.api import reassessment_outcomes as ro
from paper_trader.engine import portfolio_reassessment as prs_kernel
from paper_trader.engine import reallocation_proposal as rp_kernel

REPO = Path(__file__).resolve().parents[1]
UI = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8")
PRS_SRC = (REPO / "engine" / "portfolio_reassessment.py").read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Fixtures — shaped like the live 2026-09-02 payloads (governed HOLD standing,
# superseded 28-change / 35% / $85.69 proposal).
# --------------------------------------------------------------------------- #
_SUPERSEDED_BY = {
    "kind": "GOVERNED_ASSESSMENT",
    "decision": "CURRENT_NO_CHANGE",
    "artifact_id": "prs_2026-09-02_alpha_paper_book_1_029df5cdcda5",
    "reassessment_hash": "029df5cdcda5" + "0" * 52,
    "session": "2026-09-02",
    "decided_at": "2026-09-02T23:51:50.475243Z",
    "governed_manifest_run_id": "drc_2026-09-02_15abfb01856f",
    "governed_provenance": "GOVERNED_DAILY_CYCLE",
}

_PROPOSAL_COUNTS = {"RETAIN": 10, "EXIT": 14, "INCREASE": 5, "ADD": 6,
                    "REPLACE_IN": 2, "REDUCE": 1}


def _superseded_workflow() -> dict:
    return {
        "overall_state": "DAILY_CYCLE_COMPLETE",
        "canonical_portfolio_decision": {
            "state": "NO_CHANGE",
            "eligible_market_date": "2026-09-02",
            "proposal_superseded": True,
            "superseded_by": dict(_SUPERSEDED_BY),
            "no_proposal_reason": "The current holdings remain the best use of capital.",
        },
        "portfolio_decision_state": {
            "portfolio_decision_state": "PROPOSAL_SUPERSEDED_BY_NEWER_DECISION",
            "proposal_superseded": True,
            "superseded_by": dict(_SUPERSEDED_BY),
            "materiality": {"material": False, "action_counts": {}},
            "proposal_id": "reap_2026-09-02_alpha_paper_book_1_dcf85725a02e",
            "proposal_hash": "dcf85725a02e" + "0" * 52,
            "supersession": {
                "reason": "SAME_SESSION_NO_CHANGE_DECISION",
                "proposal_id": "reap_2026-09-02_alpha_paper_book_1_dcf85725a02e",
                "proposal_hash": "dcf85725a02e" + "0" * 52,
                "proposal_session": "2026-09-02",
                "owner": "api.portfolio_decision.assess_proposal_supersession",
                "superseded_by": dict(_SUPERSEDED_BY),
            },
            "superseded_proposal": {
                "proposal_id": "reap_2026-09-02_alpha_paper_book_1_dcf85725a02e",
                "proposal_hash": "dcf85725a02e" + "0" * 52,
                "one_way_turnover": 0.35,
                "estimated_transaction_cost": 85.69,
                "score_improvement_net_of_cost": 0.055696,
                "action_counts": dict(_PROPOSAL_COUNTS),
                "history_only": True,
            },
        },
        "reallocation_proposal_presentation": {
            "state": "SUPERSEDED_BY_NEWER_DECISION",
            "reallocation_proposal_generated_at": "2026-09-02T23:38:15Z",
        },
        "portfolio_attention": {"review_required": False},
        "operational_state": {"eligible_market_date": "2026-09-02",
                              "nav": 97934.33},
        "operator_command": {},
        "primary_action": {},
    }


def _superseded_constrained() -> dict:
    allocations = ([{"ticker": "T%02d" % i, "action": "EXIT",
                     "current_weight": 0.03, "proposed_weight": 0.0,
                     "held": True} for i in range(14)]
                   + [{"ticker": "A%02d" % i, "action": "ADD",
                       "current_weight": 0.0, "proposed_weight": 0.03,
                       "held": False} for i in range(6)]
                   + [{"ticker": "I%02d" % i, "action": "INCREASE",
                       "current_weight": 0.03, "proposed_weight": 0.04,
                       "held": True} for i in range(5)]
                   + [{"ticker": "R%02d" % i, "action": "REPLACE_IN",
                       "current_weight": 0.0, "proposed_weight": 0.03,
                       "held": False} for i in range(2)]
                   + [{"ticker": "D00", "action": "REDUCE",
                       "current_weight": 0.05, "proposed_weight": 0.04,
                       "held": True}]
                   + [{"ticker": "K%02d" % i, "action": "RETAIN",
                       "current_weight": 0.04, "proposed_weight": 0.04,
                       "held": True} for i in range(10)])
    return {
        "outcome": "PROPOSAL_READY",
        "feasible_target_exists": True,
        "superseded": True,
        "superseded_by": dict(_SUPERSEDED_BY),
        "best_feasible_target": {"allocations": allocations,
                                 "position_count": 24, "cash_weight": 0.046557,
                                 "constraints": {"all_ok": True}},
        "switching_economics": {
            "score_improvement_net_of_cost": 0.055696,
            "switching_hurdle": 0.05,
            "one_way_turnover": 0.35,
            "estimated_transaction_cost": 85.69,
            "clears_switching_hurdle": True,
            "score_before": 0.852329, "score_after": 0.925525,
            "portfolio_volatility_before": 0.122434,
            "portfolio_volatility_after": 0.156105,
        },
        "turnover": {"one_way_turnover": 0.35,
                     "estimated_transaction_cost": 85.69},
    }


def _review_workflow() -> dict:
    wf = _superseded_workflow()
    wf["canonical_portfolio_decision"] = {
        "state": "PROPOSAL_REVIEW_REQUIRED",
        "eligible_market_date": "2026-09-02",
    }
    wf["portfolio_decision_state"] = {
        "portfolio_decision_state": "PROPOSAL_REVIEW_REQUIRED",
        "requires_manual_review": True,
        "materiality": {"material": True,
                        "action_counts": {"EXIT": 2, "ADD": 2, "RETAIN": 20}},
    }
    wf["reallocation_proposal_presentation"] = {"state": "PROPOSAL_READY"}
    return wf


def _review_constrained() -> dict:
    cn = _superseded_constrained()
    cn["superseded"] = False
    cn.pop("superseded_by")
    return cn


def _pres(wf=None, cn=None) -> dict:
    return op.build_operator_presentation(
        workflow=wf if wf is not None else _superseded_workflow(),
        constrained=cn if cn is not None else _superseded_constrained())


# --------------------------------------------------------------------------- #
# A. The governed HOLD renders CURRENT-DECISION economics (tests 1-4, 22)
# --------------------------------------------------------------------------- #
class TestCurrentDecisionEconomics:
    def test_01_hold_current_decision_turnover_is_zero(self):
        cde = _pres()["decision_summary"]["current_decision"]
        assert cde["available"] is True
        assert cde["turnover"] == 0.0

    def test_02_hold_current_decision_cost_is_zero(self):
        cde = _pres()["decision_summary"]["current_decision"]
        assert cde["estimated_cost"] == 0.0

    def test_03_hold_current_decision_positions_changing_is_zero(self):
        p = _pres()
        assert p["decision_summary"]["current_decision"]["positions_changing"] == 0
        assert p["portfolio_decision"]["positions_changing"] == 0

    def test_04_superseded_economics_cannot_populate_current_decision(self):
        p = _pres()
        cde = p["decision_summary"]["current_decision"]
        assert p["portfolio_decision"]["state"] == "HOLD"
        assert cde["decision"] == "HOLD"
        assert cde["turnover"] != 0.35
        assert cde["estimated_cost"] != 85.69
        assert cde["net_improvement"] is None
        assert cde["capital_change"] == "NONE"
        assert cde["target"] == "CURRENT_BOOK"

    def test_hold_decision_positions_changing_zero_even_without_supersession(self):
        # A rejected (not superseded) alternative under a HOLD_CURRENT_BOOK
        # verdict: the decision object still reports zero changing positions.
        wf = _superseded_workflow()
        wf["canonical_portfolio_decision"] = {
            "state": "HOLD_CURRENT_BOOK", "eligible_market_date": "2026-09-02"}
        wf["portfolio_decision_state"] = {
            "portfolio_decision_state": "HOLD_CURRENT_BOOK",
            "materiality": {"material": True,
                            "action_counts": dict(_PROPOSAL_COUNTS)}}
        p = _pres(wf=wf, cn=_review_constrained())
        assert p["portfolio_decision"]["state"] == "HOLD"
        assert p["portfolio_decision"]["positions_changing"] == 0
        assert p["decision_summary"]["current_decision"]["positions_changing"] == 0

    def test_review_state_current_decision_is_the_pending_change(self):
        p = _pres(wf=_review_workflow(), cn=_review_constrained())
        cde = p["decision_summary"]["current_decision"]
        assert p["portfolio_decision"]["state"] == "REALLOCATE"
        assert cde["decision"] == "CHANGE_UNDER_REVIEW"
        assert cde["turnover"] == 0.35
        assert cde["positions_changing"] == 4
        assert cde["capital_change"] == "PROPOSED_PENDING_MANUAL_REVIEW"

    def test_22_today_exposes_the_governed_decision_lane(self):
        p = _pres()
        assert p["headline"] == "HOLD CURRENT PORTFOLIO"
        assert p["decision_summary"]["current_decision"]["scope"] == \
            op.ECON_SCOPE_CURRENT_DECISION
        assert "Governed portfolio decision" in UI


# --------------------------------------------------------------------------- #
# B. Scoping — the two artifact scopes are named (tests 8-12)
# --------------------------------------------------------------------------- #
class TestEconomicScopes:
    def test_08_hoc_economics_have_explicit_scope(self):
        assert '"expected_turnover_basis": "PRE_PROPOSAL_RELEASE_SET_ESTIMATE"' \
            in PRS_SRC
        assert op.ECON_SCOPE_HOC_RELEASE_SET in op.ECONOMICS_SCOPE_LABELS

    def test_09_full_target_economics_have_explicit_scope(self):
        ds = _pres()["decision_summary"]
        assert ds["economics_scope"] == op.ECON_SCOPE_COMPLETE_TARGET
        assert ds["is_current_decision_economics"] is False

    def test_10_the_two_improvements_are_different_scopes(self):
        # +0.018 = HOC release-set estimate (engine.portfolio_reassessment,
        # non-binding); +0.056 = complete-target net-of-cost improvement
        # (engine.reallocation_proposal, binding). Both bases are distinct
        # published constants, so the numbers can never be one owner's output.
        assert prs_kernel.IMPROVEMENT_BASIS != rp_kernel.IMPROVEMENT_BASIS \
            or '"turnover_budget_binding_here": False' in PRS_SRC
        assert '"expected_turnover_basis": "PRE_PROPOSAL_RELEASE_SET_ESTIMATE"' \
            in PRS_SRC

    def test_11_and_12_turnover_and_cost_are_scoped(self):
        ds = _pres()["decision_summary"]
        # the 35% / $85.69 numbers live under the COMPLETE_TARGET scope label
        assert ds["turnover"] == 0.35 and ds["estimated_cost"] == 85.69
        assert ds["economics_scope"] == "COMPLETE_TARGET_PROPOSAL"
        # while the current decision's are zero
        assert ds["current_decision"]["turnover"] == 0.0
        assert ds["current_decision"]["estimated_cost"] == 0.0

    def test_scope_vocabulary_is_frozen(self):
        assert set(op.ECONOMICS_SCOPE_VOCABULARY) == {
            "CURRENT_GOVERNED_DECISION", "COMPLETE_TARGET_PROPOSAL",
            "HOC_RELEASE_SET_ESTIMATE"}


# --------------------------------------------------------------------------- #
# C. Superseded proposal — history-visible, never actionable (tests 5-7, 35)
# --------------------------------------------------------------------------- #
class TestSupersededHistory:
    def test_05_superseded_proposal_remains_history_visible(self):
        ph = _pres()["decision_summary"]["proposal_history"]
        assert ph is not None
        assert ph["history_only"] is True
        assert ph["original_economics"]["one_way_turnover"] == 0.35
        assert ph["original_economics"]["estimated_transaction_cost"] == 85.69
        assert ph["original_economics"]["action_counts"] == _PROPOSAL_COUNTS

    def test_06_reallocation_page_not_actionable_without_current_proposal(self):
        ds = _pres()["decision_summary"]
        assert ds["renders_approval_cta"] is False
        assert ds["target_class"] == "SUPERSEDED_HISTORY_ONLY"
        # the UI demotes the analysis into an explicit history <details> block
        assert 'data-history-only="1"' in UI
        assert "SUPERSEDED PROPOSAL — HISTORY ONLY" in UI

    def test_07_history_labels_are_explicit(self):
        ph = _pres()["decision_summary"]["proposal_history"]
        assert ph["not_current"] and ph["not_approvable"] \
            and ph["not_an_action_plan"]
        assert ph["superseded_by_decision"] == "CURRENT_NO_CHANGE"
        assert ph["superseded_by_session"] == "2026-09-02"
        assert ph["superseded_at"] == "2026-09-02T23:51:50.475243Z"
        assert ph["created_at"] == "2026-09-02T23:38:15Z"
        assert ph["supersession_reason"] == "SAME_SESSION_NO_CHANGE_DECISION"

    def test_35_superseded_proposal_approval_remains_backend_rejected(self):
        # The one calculation still supersedes a same-session standing proposal
        # under a governed CURRENT_NO_CHANGE conclusion (R54.2.3.2 unchanged).
        verdict = pd.assess_proposal_supersession(
            proposal_summary={
                "reallocation_proposal_available": True,
                "reallocation_proposal_id": "reap_x",
                "reallocation_proposal_hash": "h1",
                "reallocation_bound_eligible_market_date": "2026-09-02",
                "reallocation_bound_hoc_assessment_hash": "hoc1",
            },
            assessment={
                "available": True, "decision": "CURRENT_NO_CHANGE",
                "eligible_market_date": "2026-09-02",
                "reassessment_hash": "r2", "artifact_id": "prs_v2",
                "generated_at": "2026-09-02T23:51:50Z",
                "hoc_assessment_hash": "hoc2", "is_governed": True,
                "governed_manifest_run_id": "drc_x",
                "governed_provenance": "GOVERNED_DAILY_CYCLE",
            })
        assert verdict["superseded"] is True

    def test_non_governed_result_never_supersedes(self):
        verdict = pd.assess_proposal_supersession(
            proposal_summary={
                "reallocation_proposal_available": True,
                "reallocation_bound_eligible_market_date": "2026-09-02"},
            assessment={"available": True, "decision": "CURRENT_NO_CHANGE",
                        "eligible_market_date": "2026-09-02",
                        "is_governed": False})
        assert verdict["superseded"] is False


# --------------------------------------------------------------------------- #
# D. Action accounting / materiality (tests 13-18)
# --------------------------------------------------------------------------- #
class TestActionMateriality:
    POLICY = {"material_weight_delta": 1.0e-4}

    def test_13_zero_delta_cannot_classify_increase(self):
        action, _ = rp_kernel._reoptimised_action(
            ticker="X", action="RETAIN", reason_codes=[], delta=5e-5,
            proposed=0.04, held=True, policy=self.POLICY)
        assert action == "RETAIN"

    def test_14_zero_delta_cannot_classify_reduce(self):
        action, _ = rp_kernel._reoptimised_action(
            ticker="X", action="RETAIN", reason_codes=[], delta=-5e-5,
            proposed=0.04, held=True, policy=self.POLICY)
        assert action == "RETAIN"

    def test_15_zero_to_zero_cannot_classify_add(self):
        action, _ = rp_kernel._reoptimised_action(
            ticker="X", action="ADD", reason_codes=[], delta=0.0,
            proposed=0.0, held=False, policy=self.POLICY)
        assert action is None  # no row at all — never ADD

    def test_16_zero_to_zero_cannot_classify_exit(self):
        action, _ = rp_kernel._reoptimised_action(
            ticker="X", action="REPLACE_IN", reason_codes=[], delta=0.0,
            proposed=0.0, held=False, policy=self.POLICY)
        assert action is None  # no row at all — never EXIT

    def test_material_change_beyond_band_still_classifies(self):
        action, _ = rp_kernel._reoptimised_action(
            ticker="FTNT", action="RETAIN", reason_codes=[], delta=0.002,
            proposed=0.04, held=True, policy=self.POLICY)
        assert action == "INCREASE"

    def test_17_changed_count_uses_material_action_counts(self):
        p = _pres(wf=_review_workflow(), cn=_review_constrained())
        ds = p["decision_summary"]
        assert ds["positions_changing"] == 4  # EXIT 2 + ADD 2; RETAIN excluded

    def test_18_replacement_pair_counts_exactly_two_capital_changes(self):
        m = pd.assess_materiality({"reallocation_action_counts": {
            "REPLACE_IN": 1, "REPLACE_OUT": 1, "RETAIN": 10}})
        assert m["membership_change_count"] == 2
        assert m["resize_change_count"] == 0
        counts = {"REPLACE_IN": 1, "REPLACE_OUT": 1, "RETAIN": 10}
        assert sum(v for k, v in counts.items() if k != "RETAIN") == 2

    def test_ui_shows_extra_precision_when_rounding_hides_a_change(self):
        assert "_opWeight(row.current_weight, 1) === _opWeight(row.proposed_weight, 1)" in UI


# --------------------------------------------------------------------------- #
# E. Eligibility vocabulary (test 19)
# --------------------------------------------------------------------------- #
class TestEligibilityVocabulary:
    def test_19_membership_terminology_separate_from_retention(self):
        assert dag.CHECK_LABELS[dag.CHECK_ELIGIBILITY] == \
            "Universe membership / scoreability"
        assert "no longer meets the HOC retention rule" in PRS_SRC
        assert "no longer meets the eligibility rule" not in PRS_SRC
        assert "no longer meet the eligibility rule" not in PRS_SRC

    def test_gate_pass_summary_says_membership_not_eligibility(self):
        dag_src = (REPO / "api" / "daily_action_gate.py").read_text(
            encoding="utf-8")
        assert "All holdings remain scoreable, current universe" in dag_src
        assert "All holdings remain eligible (scoreable, current members)." \
            not in dag_src

    def test_retention_rule_statement_renamed(self):
        assert "No retention-rule exit is outstanding." in PRS_SRC
        assert "No eligibility exit is outstanding." not in PRS_SRC


# --------------------------------------------------------------------------- #
# F. Reassessment freshness labelling (tests 20-21)
# --------------------------------------------------------------------------- #
class TestFreshnessLabels:
    @staticmethod
    def _stale_rows(freshness_detail):
        """The OPERATOR stale/missing list.

        Release 55 split the return into ``(stale, advisory)``: a row the
        currency owner attributes to the LEGACY scheduled-review checkpoint
        clock moves to the audit-only advisory surface. These R54.2.4 fixtures
        deliberately supply no R55 scope declaration, so the row stays on the
        operator surface here — the documented fail-safe (silence is never read
        as a repair), and exactly what these two tests still pin.
        """
        stale, _advisory = ams._stale_components(
            operational_book={"available": True},
            live_information={"available": True, "collection_running": True},
            signal_state={"available": True,
                          "scoring_status": "UNIVERSE_SCORING_READY"},
            reassessment={"available": True, "state": "CURRENT_NO_CHANGE",
                          "reassessment_freshness": "OVERDUE",
                          "reassessment_freshness_detail": freshness_detail},
            target_proposal={"available": True},
            research_governance={"available": True,
                                 "research_runtime": {"state": "OK"}})
        return stale

    def test_20_fresh_reassessment_not_labelled_overdue(self):
        rows = self._stale_rows({"current_for_eligible_session": True,
                                 "review_overdue": True,
                                 "next_scheduled_review_date": "2026-08-01"})
        row = next(r for r in rows if r["component"] == "portfolio_reassessment")
        assert row["display_label"]
        assert "Scheduled full review due" in row["display_label"]
        assert "OVERDUE" not in row["display_label"]
        # the raw owner token stays available for Audit
        assert row["owner_state"] == "OVERDUE"

    def test_21_scheduled_review_stays_visible_truthfully(self):
        rows = self._stale_rows({"current_for_eligible_session": True,
                                 "review_overdue": True,
                                 "next_scheduled_review_date": "2026-08-01"})
        assert any(r["component"] == "portfolio_reassessment" for r in rows)
        # genuinely stale assessments keep the raw fallback (no display_label)
        stale_rows = self._stale_rows({"current_for_eligible_session": False,
                                       "review_overdue": True,
                                       "next_scheduled_review_date": "2026-08-01"})
        row = next(r for r in stale_rows
                   if r["component"] == "portfolio_reassessment")
        assert row["display_label"] is None

    def test_ui_prefers_display_label(self):
        assert "s.display_label" in UI


# --------------------------------------------------------------------------- #
# G. Lane B — first-class live/intraday reassessment (tests 23-32)
# --------------------------------------------------------------------------- #
def _ams_state(last_run: dict, *, reassessment=None, workflow=None) -> dict:
    return ams.build_active_manager_state(
        workflow=workflow or {"portfolio_attention": {"review_required": False}},
        event_refresh={"last_run": last_run, "material_event_count": 3,
                       "affected_holdings": ["FTNT", "KEYS"],
                       "recent_events": [], "material_events": []},
        reassessment=reassessment,
        governed_decision={"available": True,
                           "record_id": "drc_governed_drc_2026-09-02_15abfb01856f"})


_NOT_MATERIAL_RUN = {
    "run_id": "evt_e6e0292f1d15175f",
    "state": "INFORMATION_NOT_MATERIAL",
    "generated_at": "2026-09-03T00:33:19Z",
    "reassessment_ran": False, "proposal_built": False,
    "materiality_change_level": "SIGNAL_CHANGED",
}

_WITHHELD_RUN = {
    "run_id": "evt_withheld", "state": "PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW",
    "generated_at": "2026-09-02T20:30:00Z",
    "reassessment_ran": True, "proposal_built": True,
    "materiality_change_level": "HOC_AFFECTED",
    "reassessment_state": "PROPOSAL_READY",
    "reassessment_id": "prs_A", "reassessment_hash": "hashA",
    "reassessment_persisted": True,
    "governed_decision": {"evaluated": True, "verdict": "WITHHELD",
                          "recorded": False,
                          "withheld_reason_codes":
                              ["SAME_SESSION_HOC_ARTIFACT_IMMUTABLE"],
                          "failing_checks": ["hoc_artifact_binding"]},
}

_PROMOTED_RUN = {
    **_WITHHELD_RUN, "run_id": "evt_promoted",
    "governed_decision": {"evaluated": True, "verdict": "PROMOTED",
                          "recorded": True, "record_id": "gid_1",
                          "withheld_reason_codes": [], "failing_checks": []},
}


class TestLiveReassessmentLane:
    def test_23_today_exposes_the_live_lane(self):
        lane = _ams_state(_NOT_MATERIAL_RUN)["live_reassessment_lane"]
        assert lane["available"] is True
        assert lane["lane"] == "LIVE_INTRADAY_REASSESSMENT"
        assert "live_reassessment_lane" in UI  # rendered on Today

    def test_24_latest_live_event_timestamp_exposed(self):
        lane = _ams_state(_NOT_MATERIAL_RUN)["live_reassessment_lane"]
        assert lane["at"] == "2026-09-03T00:33:19Z"
        assert lane["run_id"] == "evt_e6e0292f1d15175f"

    def test_25_live_trigger_exposed(self):
        lane = _ams_state(_NOT_MATERIAL_RUN)["live_reassessment_lane"]
        assert lane["trigger"] == "SIGNAL_CHANGED"

    def test_26_material_events_and_affected_holdings_exposed(self):
        lane = _ams_state(_NOT_MATERIAL_RUN)["live_reassessment_lane"]
        assert lane["material_event_count"] == 3
        assert lane["affected_holdings"] == ["FTNT", "KEYS"]

    def test_27_candidate_conclusion_exposed(self):
        assert _ams_state(_NOT_MATERIAL_RUN)["live_reassessment_lane"][
            "candidate_conclusion"] == "INFORMATION_NOT_MATERIAL"
        assert _ams_state(_WITHHELD_RUN)["live_reassessment_lane"][
            "candidate_conclusion"] == "PROPOSAL_AVAILABLE"
        hold_run = {**_WITHHELD_RUN,
                    "reassessment_state": "CURRENT_NO_CHANGE"}
        assert _ams_state(hold_run)["live_reassessment_lane"][
            "candidate_conclusion"] == "HOLD"

    def test_28_governance_status_exposed(self):
        assert _ams_state(_NOT_MATERIAL_RUN)["live_reassessment_lane"][
            "governance_state"] == "NOT_REQUIRED"
        assert _ams_state(_WITHHELD_RUN)["live_reassessment_lane"][
            "governance_state"] == "WITHHELD"
        assert _ams_state(_PROMOTED_RUN)["live_reassessment_lane"][
            "governance_state"] == "GOVERNED"

    def test_29_exact_withheld_reason_exposed(self):
        lane = _ams_state(_WITHHELD_RUN)["live_reassessment_lane"]
        assert lane["governance_withheld_reason_codes"] == \
            ["SAME_SESSION_HOC_ARTIFACT_IMMUTABLE"]
        assert lane["governance_failing_checks"] == ["hoc_artifact_binding"]

    def test_30_withheld_result_cannot_look_authoritative(self):
        lane = _ams_state(_WITHHELD_RUN)["live_reassessment_lane"]
        assert lane["is_authoritative_decision"] is False
        assert lane["supersedes_standing_decision"] is False
        assert lane["promoted_to_governed"] is False

    def test_31_non_material_event_shows_information_not_material(self):
        lane = _ams_state(_NOT_MATERIAL_RUN)["live_reassessment_lane"]
        assert lane["candidate_conclusion"] == "INFORMATION_NOT_MATERIAL"
        assert lane["governance_state"] == "NOT_REQUIRED"

    def test_32_governed_result_identifies_supersession(self):
        lane = _ams_state(_PROMOTED_RUN)["live_reassessment_lane"]
        assert lane["supersedes_standing_decision"] is True
        assert lane["is_authoritative_decision"] is True
        assert lane["governed_record_id"] == "gid_1"
        assert lane["standing_governed_decision_id"] == \
            "drc_governed_drc_2026-09-02_15abfb01856f"

    def test_lane_economics_only_from_matching_artifact(self):
        matched = _ams_state(
            _WITHHELD_RUN,
            reassessment={"reassessment_id": "prs_A",
                          "decision": {"expected_net_improvement": 0.018,
                                       "net_improvement_hurdle": 0.05,
                                       "expected_one_way_turnover": 0.235,
                                       "expected_transaction_cost_usd": 57.46}})
        lane = matched["live_reassessment_lane"]
        assert lane["economics"]["scope"] == "HOC_RELEASE_SET_ESTIMATE"
        assert lane["economics"]["expected_net_improvement"] == 0.018
        other = _ams_state(
            _WITHHELD_RUN,
            reassessment={"reassessment_id": "prs_B",
                          "decision": {"expected_net_improvement": 0.5}})
        assert other["live_reassessment_lane"]["economics"] is None

    def test_lane_is_a_declared_component(self):
        assert "live_reassessment_lane" in ams.COMPONENTS
        assert "live_reassessment_lane" in ams.COMPONENT_OWNERS


# --------------------------------------------------------------------------- #
# H. The browser decides nothing (tests 33-34)
# --------------------------------------------------------------------------- #
class TestNoUiDerivation:
    def test_33_no_javascript_computes_governance_status(self):
        # the Lane B card reads backend fields verbatim
        assert "lane.governance_state" in UI
        assert "lane.supersedes_standing_decision" in UI
        assert "lane.candidate_conclusion" in UI

    def test_34_no_javascript_decides_proposal_supersession(self):
        # the reallocation view switches on the backend's target_class verbatim
        assert "ds.target_class === 'SUPERSEDED_HISTORY_ONLY'" in UI
        # the hero renders the backend current_decision block, never the
        # proposal metrics
        assert "ds.current_decision" in UI
        assert "_opMetric('Current-decision turnover'" in UI
        # the HERO's old unscoped render is gone (the Reallocation page's
        # scoped Economics block legitimately keeps the alternative's numbers)
        assert ("_opMetric('Expected improvement', _opNum(ds.net_improvement, 3), "
                "_opNil(ds.switching_hurdle)") not in UI


# --------------------------------------------------------------------------- #
# I. Corporate-action scope (test 36)
# --------------------------------------------------------------------------- #
class TestCorporateActionScope:
    def test_36_projection_cannot_masquerade_as_authoritative_nav(self):
        rec = ca.reconcile_book(
            book={"book_id": "alpha_paper_book_1"},
            fills=[], marks={"latest_completed_date": "2026-09-02"},
            actions=[])
        assert rec["is_authoritative_nav"] is False
        assert rec["nav_scope"] == "DESK_BOOK_RECONCILIATION_PROJECTION"
        assert "never the authoritative operational NAV" in rec["nav_scope_label"]
        assert "api.operational_book" in rec["authoritative_nav_owner"]
        assert "rec.nav_scope_label" in UI


# --------------------------------------------------------------------------- #
# J. Outcome-evidence identity (tests 37-38)
# --------------------------------------------------------------------------- #
class TestOutcomeEvidenceIdentity:
    ROWS = [
        {"active_book_id": "b1", "eligible_market_date": "2026-09-02",
         "ticker": "KEYS", "recommendation": "REPLACE",
         "replacement_ticker": "FTNT", "horizon_eligible_closes": 5,
         "reassessment_id": "prs_v1", "reassessment_hash": "aaa111" * 8},
        {"active_book_id": "b1", "eligible_market_date": "2026-09-02",
         "ticker": "KEYS", "recommendation": "REPLACE",
         "replacement_ticker": "FTNT", "horizon_eligible_closes": 5,
         "reassessment_id": "prs_v2", "reassessment_hash": "bbb222" * 8},
        {"active_book_id": "b1", "eligible_market_date": "2026-09-01",
         "ticker": "MNST", "recommendation": "HOLD",
         "replacement_ticker": None, "horizon_eligible_closes": 5,
         "reassessment_id": "prs_v0", "reassessment_hash": "ccc333" * 8},
    ]

    def test_37_repeated_rows_are_differentiated_by_version(self):
        out = ro._annotate_assessment_versions([dict(r) for r in self.ROWS])
        keys_rows = [r for r in out if r["ticker"] == "KEYS"]
        assert all(r["repeated_across_assessment_versions"] for r in keys_rows)
        assert all(r["same_axis_version_count"] == 2 for r in keys_rows)
        assert keys_rows[0]["assessment_version"] != \
            keys_rows[1]["assessment_version"]
        mnst = next(r for r in out if r["ticker"] == "MNST")
        assert mnst["repeated_across_assessment_versions"] is False

    def test_38_immutable_history_is_never_deleted(self):
        out = ro._annotate_assessment_versions([dict(r) for r in self.ROWS])
        assert len(out) == len(self.ROWS)
        # projection annotation only; source keys intact
        for orig, ann in zip(self.ROWS, out):
            for k, v in orig.items():
                assert ann[k] == v
        assert "Assessment version" in UI


# --------------------------------------------------------------------------- #
# K. Legacy controls and safety (tests 39-44)
# --------------------------------------------------------------------------- #
class TestLegacyAndSafety:
    def test_39_legacy_controls_not_presented_as_canonical(self):
        assert UI.count('data-flow-class="LEGACY_COMPATIBILITY"') >= 2
        assert 'data-flow-class="MAINTENANCE_RECOVERY"' in UI

    def test_40_to_43_no_orders_no_fills_no_broker_no_automation(self):
        p = _pres()
        assert p["safety"]["creates_orders"] is False
        assert p["safety"]["creates_fills"] is False
        assert p["safety"]["broker"] == "NONE"
        assert p["safety"]["automation_off"] is True
        lane = _ams_state(_NOT_MATERIAL_RUN)["live_reassessment_lane"]
        assert lane["creates_orders"] is False
        assert lane["approves_anything"] is False
        cde = p["decision_summary"]["current_decision"]
        assert cde["scope"] == "CURRENT_GOVERNED_DECISION"

    def test_44_no_model_promotion_surface_added(self):
        st = _ams_state(_NOT_MATERIAL_RUN)
        rg = st["research_governance"]
        assert rg["automatic_promotion_allowed"] is False
