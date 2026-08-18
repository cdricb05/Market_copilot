r"""Release 29.4 — NORMAL-CYCLE SESSION AUTHORITY + CLOSE VALIDITY.

THE LIVE CONTRADICTION THIS SUITE LOCKS OUT
-------------------------------------------
On 2026-08-18 at 08:31 ET — with the market session still open — the operator screen
offered RUN DAILY CLOSE for a session that had already been closed the previous evening:

    engine.market_session   session_status              BEFORE_SESSION_CLOSE
                            eligible completed session  2026-08-17
    api.daily_close         close_status                AWAITING_MARKET_CLOSE
                            requires_close_run          False
                            recorded_close.market_date  2026-08-17
                            recorded_close.close_status DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT
                            forward evidence            6 / 6 snapshots present
    api.workflow_state      overall_state               READY_FOR_DAILY_CLOSE   <-- wrong
                            daily_close_gate            execution_allowed=True  <-- wrong
                            operational_close_valid     False                   <-- wrong
                            evidence_presentation       "No completed operational
                                                         close has been recorded yet."

ROOT CAUSE
----------
`api.workflow_state` did not ASK the Daily Close owner whether the close completed. It
kept a private LITERAL COPY of the owner's completed-close vocabulary:

    _CLOSE_COMPLETE_STATUSES = {"DAILY_CLOSE_COMPLETE_HOLD", "REBALANCE_PROPOSAL_READY",
                                "PAPER_ORDERS_SUBMITTED", "INITIAL_BASELINE_RECORDED",
                                "ALREADY_PROCESSED"}

Release 29.3 renamed REBALANCE_PROPOSAL_READY to DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT
and migrated the token on READ. The copy kept the old spelling. From that moment the
real Aug-17 close normalised on read to a token the copy did not contain, so
`operational_close_valid` went False, `eligible_session_closed` went False, and the
priority policy's P3.7 ("an unclosed eligible session must be closed") fired for a
session that was already closed.

Note what the defect is NOT: nothing here was about membership drift being "bad". A
PORTFOLIO finding never had any business deciding whether an OPERATIONAL close happened
— and after this release it structurally cannot, because the predicate that answers
that question accepts no portfolio input at all.
"""
from __future__ import annotations

import copy
import re
from datetime import datetime
from pathlib import Path

import pytest

from paper_trader.api import daily_close as dc
from paper_trader.api import portfolio_state as pstate
from paper_trader.api import workflow_state as ws
from paper_trader.engine import market_hours as mh
from paper_trader.engine import market_session as msession
from paper_trader.engine import normal_cycle as nc

UI_FILE = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"


def _ui() -> str:
    return UI_FILE.read_text(encoding="utf-8")


def _src(*parts) -> str:
    return Path(__file__).resolve().parents[1].joinpath(*parts).read_text(encoding="utf-8")


# =========================================================================== #
# THE REAL AUG-18 08:31 ET WORLD, as constants. Nothing below re-derives them.
# =========================================================================== #
AUG17 = "2026-08-17"
AUG18 = "2026-08-18"
#: 08:31 ET on Tuesday 2026-08-18 — before the 17:30 post-close cutoff.
NOW_PRE_CLOSE = datetime(2026, 8, 18, 8, 31, 0, tzinfo=mh._ET)
#: 18:05 ET the same day — after the cutoff, so 2026-08-18 is itself expected complete.
NOW_POST_CLOSE = datetime(2026, 8, 18, 18, 5, 0, tzinfo=mh._ET)

#: The close the operator really ran on the evening of 2026-08-17.
AUG17_CLOSE_PROGRESS = {"market_date": AUG17, "done": True,
                        "final_close_status": dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT}

_OPB = {"operational_book": {
    "book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1",
    "current_status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
    "nav_as_of_date": AUG17, "desk_mark_date": AUG17, "latest_desk_mark_date": AUG17,
    "nav": 100303.50, "cash": 4482.71, "holdings_count": 25, "pending_order_count": 0,
    "current_target": {"alpha_market_date": AUG17,
                       "latest_completed_market_date": AUG17}}}
_INPUTS = {"market_as_of_date": AUG17, "momentum_month": "2026-08",
           "fundamental_as_of_date": "2026-05-22"}
_DESK = {"series": {"SPY": [["2026-08-14", 770.0], [AUG17, 772.0]]},
         "latest_completed_date": AUG17}
_FWD = {"latest_snapshot_date": AUG17, "snapshot_count": 6,
        "evidence_state": "FORWARD_EVIDENCE_OK", "active_book": {}, "shadow_books": []}
_TR = {"dates": {"alpha_market_date": AUG17}}


def _gate(**kw):
    """The LEGACY rank-membership comparison, current for the eligible session.

    Release 29.3 established this is compatibility only: its outcome describes a
    membership diff, never a portfolio proposal.
    """
    g = {"latest_completed_market_date": AUG17,
         "outcome": "MEMBERSHIP_DRIFT_DETECTED",
         "target_state": "MEMBERSHIP_DRIFT",
         "next_scheduled_full_review": "2026-09-01", "scheduled_review_due": False,
         "opportunity_cost_available": True, "opportunity_cost_state": "READY",
         "opportunity_cost_assessment_hash": "7a96efc2f95e",
         "opportunity_cost_recommendation_counts": {"HOLD": 23, "REVIEW": 2},
         "opportunity_cost_data_gaps": [],
         "opportunity_cost_bound_eligible_market_date": AUG17,
         "opportunity_cost_bound_active_book_id": "alpha_paper_book_1"}
    g.update(kw)
    return g


def _reassessment(**kw):
    """The canonical CHANGE_CANDIDATE / withheld reassessment recorded for Aug-17.

    Field names are the ones ``api.portfolio_reassessment.load_reassessment_summary``
    actually publishes — the workflow owner reads them verbatim and computes no
    economics of its own.
    """
    r = {"reassessment_available": True,
         "reassessment_state": "CHANGE_CANDIDATE",
         "decision": "CHANGE_CANDIDATE",
         "proposal_required": False,
         "reassessment_id": "prs_2026-08-17_alpha_paper_book_1_7edb4353341f",
         "reassessment_hash": "7edb4353341f",
         "reassessment_date": AUG17,
         "hoc_assessment_hash": "7a96efc2f95e",
         "eligible_market_date": AUG17,
         "active_book_id": "alpha_paper_book_1",
         "holdings_evaluated": 25, "attention_count": 2,
         "expected_net_improvement": 0.028081,
         "expected_one_way_turnover": 0.080000,
         "expected_transaction_cost_usd": 61.24,
         "blockers": [], "reason_codes": [],
         "mandatory_exit_tickers": ["AIZ", "SPG"],
         "mandatory_exit_policy": {"obligation": "REQUIRED_IF_REALLOCATION_PROCEEDS"}}
    r.update(kw)
    return r


def _load(**kw):
    """Compose the REAL workflow state with every read seam bound (no store, no probe)."""
    args = dict(
        now=NOW_PRE_CLOSE,
        operational=copy.deepcopy(_OPB), inputs=dict(_INPUTS),
        daily_status={"latest_valid_mark_date": AUG17},
        desk_marks=copy.deepcopy(_DESK),
        close_progress=dict(AUG17_CLOSE_PROGRESS),
        forward_status=copy.deepcopy(_FWD), gate=_gate(),
        target_readiness=copy.deepcopy(_TR),
        research_cycle={"state": "COMPLETE", "blockers": []},
        reassessment_summary=_reassessment(), decision_record={})
    args.update(kw)
    return ws.load_workflow_state(**args)


# =========================================================================== #
# 1. CLOSE VALIDITY IS OPERATIONAL COMPLETION — AND HAS EXACTLY ONE OWNER
# =========================================================================== #
class TestCloseValidityOwnership:

    def test_01_the_daily_close_owner_publishes_the_predicate(self):
        assert dc.CLOSE_VALIDITY_OWNER == "api.daily_close"
        assert dc.CLOSE_VALIDITY_POLICY == "OPERATIONAL_COMPLETION_ONLY"
        assert callable(dc.completed_close_statuses)
        assert callable(dc.is_completed_close_status)
        assert callable(dc.is_operational_close_complete)

    def test_02_membership_drift_is_a_COMPLETED_operational_close(self):
        # The exact status the real Aug-17 close recorded.
        assert dc.is_completed_close_status(dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT) is True
        assert dc.is_operational_close_complete(AUG17_CLOSE_PROGRESS) is True

    def test_03_hold_is_a_COMPLETED_operational_close(self):
        assert dc.is_completed_close_status(dc.CLOSE_COMPLETE_HOLD) is True
        assert dc.is_operational_close_complete(
            {"done": True, "final_close_status": dc.CLOSE_COMPLETE_HOLD}) is True

    def test_04_every_processed_status_is_complete(self):
        for status in dc._CLOSE_PROCESSED_STATUSES:
            assert dc.is_completed_close_status(status) is True, status

    def test_05_a_legacy_byte_on_disk_still_reads_as_complete(self):
        # History is never rewritten; it is migrated on READ.
        assert dc.is_completed_close_status(dc.LEGACY_REBALANCE_PROPOSAL_READY) is True

    def test_06_a_failed_or_unfinished_close_is_not_complete(self):
        assert dc.is_operational_close_complete(
            {"done": True, "final_close_status": dc.DATA_BLOCKED}) is False
        assert dc.is_operational_close_complete(
            {"done": False, "final_close_status": dc.CLOSE_COMPLETE_HOLD}) is False
        assert dc.is_operational_close_complete({}) is False
        assert dc.is_operational_close_complete(None) is False

    def test_07_the_predicate_ACCEPTS_no_portfolio_input(self):
        # THE STRUCTURAL GUARANTEE. A portfolio finding cannot invalidate a close
        # because there is no parameter through which one could arrive.
        import inspect
        params = list(inspect.signature(dc.is_operational_close_complete).parameters)
        assert params == ["progress"]
        for excluded in dc.CLOSE_VALIDITY_EXCLUDED_INPUTS:
            assert excluded not in params

    def test_08_the_excluded_inputs_are_named_explicitly(self):
        for name in ("membership_drift", "reallocation_proposal",
                     "portfolio_reassessment", "holding_opportunity_cost",
                     "portfolio_decision"):
            assert name in dc.CLOSE_VALIDITY_EXCLUDED_INPUTS

    def test_09_the_workflow_owner_DELEGATES_and_never_mirrors(self):
        assert ws.CLOSE_VALIDITY_OWNER == "api.daily_close"
        # The stale literal is gone by NAME, so nothing can read it again.
        assert not hasattr(ws, "_CLOSE_COMPLETE_STATUSES")
        assert ws._is_operational_close_complete(AUG17_CLOSE_PROGRESS) is True

    def test_10_the_audited_fallback_equals_the_owners_set(self):
        # The fallback exists only for a pure import context. If it ever stops
        # matching the owner, THIS is the drift that caused Release 29.4.
        assert ws._CLOSE_COMPLETE_FALLBACK == dc.completed_close_statuses()

    def test_11_portfolio_state_no_longer_carries_a_duplicate(self):
        assert not hasattr(pstate, "_CLOSE_COMPLETE_STATUSES")
        src = _src("api", "portfolio_state.py")
        assert "REBALANCE_PROPOSAL_READY" not in src

    def test_12_no_module_outside_the_owner_defines_the_vocabulary(self):
        # A second literal definition is exactly how Release 29.3's rename went unseen.
        for mod in ("api/workflow_state.py", "api/portfolio_state.py"):
            src = _src(*mod.split("/"))
            assert "_CLOSE_COMPLETE_STATUSES = frozenset" not in src, mod


# =========================================================================== #
# 2. THE AUG-18 08:31 ET PRE-CLOSE FIXTURE — THE LIVE DEFECT
# =========================================================================== #
class TestAug18PreCloseSessionAuthority:

    def test_20_the_market_session_owner_says_the_session_is_still_open(self):
        s = msession.evaluate_session(
            now=NOW_PRE_CLOSE, latest_confirmed_owned_data_date=AUG17,
            latest_benchmark_date=AUG17)
        assert s.session_status == msession.BEFORE_SESSION_CLOSE
        assert s.expected_completed_market_date == AUG17
        assert s.eligible_market_date == AUG17
        assert s.close_cutoff_et == "17:30"

    def test_21_the_overall_state_is_WAITING_FOR_SESSION_CLOSE(self):
        r = _load()
        assert r["overall_state"] == ws.WAITING_FOR_SESSION_CLOSE

    def test_22_the_cycle_stage_is_WAIT_FOR_SESSION_CLOSE(self):
        cyc = _load()["normal_cycle"]
        assert cyc["current_stage"] == nc.STAGE_WAIT_FOR_SESSION_CLOSE
        gates = cyc["stage_gates"]
        assert gates[nc.STAGE_WAIT_FOR_SESSION_CLOSE]["is_current_stage"] is True

    def test_23_the_daily_close_stage_gate_is_CLOSED(self):
        gates = _load()["normal_cycle"]["stage_gates"]
        assert gates[nc.STAGE_DAILY_CLOSE]["execution_allowed"] is False

    def test_24_the_canonical_daily_close_gate_is_CLOSED(self):
        assert _load()["daily_close_gate"]["execution_allowed"] is False

    def test_25_NO_normal_path_mutation_is_offered_at_all(self):
        cyc = _load()["normal_cycle"]
        assert cyc["executable_stages"] == []
        assert cyc["executable_stage_count"] == 0
        assert cyc["no_action_required"] is True
        assert cyc["in_recovery"] is False

    def test_26_the_primary_action_is_passive_and_not_executable(self):
        r = _load()
        pa = r["primary_action"]
        assert pa["action_code"] != "RUN_DAILY_CLOSE"
        assert pa["action_code"] == ws.ACTION_WAIT_FOR_SESSION_CLOSE
        assert pa["execution_available"] is False
        assert r["operator_command"]["primary_action_available"] is False
        assert r["operator_command"]["passive"] is True

    def test_27_the_current_task_says_wait_for_the_session_to_complete(self):
        r = _load()
        text = " ".join(str(x).lower() for x in (
            r["current_task"], r["normal_cycle"]["now_text"],
            r["normal_cycle"]["do_text"]))
        assert "still open" in text or "wait" in text

    def test_28_the_AUG17_close_remains_VALID(self):
        os_ = _load()["operational_state"]
        assert os_["operational_close_valid"] is True
        assert os_["latest_completed_close_date"] == AUG17
        assert os_["latest_close_status"] == dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT
        assert os_["eligible_session_already_processed"] is True

    def test_29_the_two_owners_are_NAMED_in_the_payload(self):
        os_ = _load()["operational_state"]
        assert os_["close_validity_owner"] == "api.daily_close"
        assert os_["session_eligibility_owner"] == "engine.market_session"
        assert os_["close_validity_independent_of_portfolio_outcome"] is True

    def test_30_the_dates_come_from_the_market_session_owner(self):
        cur = _load()["current_session"]
        assert cur["session_status"] == msession.BEFORE_SESSION_CLOSE
        assert cur["latest_eligible_completed_market_date"] == AUG17
        assert cur["calendar_date"] == AUG18
        assert cur["session_close_cutoff"] == "17:30"

    def test_31_the_payload_is_CONSISTENT(self):
        r = _load()
        assert r["consistency_status"] == ws.CONSISTENT
        assert r["consistency_violations"] == []

    def test_32_a_portfolio_CHANGE_CANDIDATE_does_not_reopen_the_close(self):
        # The whole point: the portfolio lane is loud, and the close stays closed.
        r = _load()
        assert r["operational_state"]["operational_close_valid"] is True
        assert r["overall_state"] == ws.WAITING_FOR_SESSION_CLOSE
        assert r["daily_close_gate"]["execution_allowed"] is False

    def test_33_a_HOLD_close_behaves_identically(self):
        r = _load(close_progress={"market_date": AUG17, "done": True,
                                  "final_close_status": dc.CLOSE_COMPLETE_HOLD})
        assert r["overall_state"] == ws.WAITING_FOR_SESSION_CLOSE
        assert r["operational_state"]["operational_close_valid"] is True

    def test_34_a_legacy_journal_byte_behaves_identically(self):
        r = _load(close_progress={
            "market_date": AUG17, "done": True,
            "final_close_status": dc.LEGACY_REBALANCE_PROPOSAL_READY})
        assert r["overall_state"] == ws.WAITING_FOR_SESSION_CLOSE
        assert r["operational_state"]["operational_close_valid"] is True
        # Normalised on READ; the stored byte is never rewritten.
        assert (r["operational_state"]["latest_close_status"]
                == dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT)

    def test_35_the_exact_pre_repair_payload_would_now_be_INCONSISTENT(self):
        # Replay the LIVE 2026-08-18 08:31 ET field set through the invariant.
        v = ws.check_session_authority(
            session_status="BEFORE_SESSION_CLOSE", eligible_market_date=AUG17,
            expected_completed_market_date=AUG17,
            latest_completed_close_date=AUG17, operational_close_valid=False,
            latest_close_status=dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT,
            daily_close_execution_allowed=True,
            evidence_completed_close_state="NONE")
        assert [x["code"] for x in v] == ["COMPLETED_CLOSE_REPORTED_INVALID"]


# =========================================================================== #
# 3. THE POST-CLOSE FIXTURE — THE REPAIR MUST NOT SUPPRESS A LEGITIMATE CLOSE
# =========================================================================== #
class TestAug18PostCloseStillRunsTheClose:

    def _post(self, **kw):
        """18:05 ET on 2026-08-18: the cutoff has passed and the owned provider has
        published 2026-08-18, but the Daily Close for it has NOT been run — the latest
        completed close is still 2026-08-17."""
        opb = copy.deepcopy(_OPB)
        # Owned-data confirmation for the market session comes from the ACTIVE book's
        # owned desk mark (api.data_freshness), so this is what "the provider confirms
        # 2026-08-18" means to the session owner.
        opb["operational_book"].update(
            desk_mark_date=AUG18, latest_desk_mark_date=AUG18)
        args = dict(now=NOW_POST_CLOSE, operational=opb,
                    desk_marks={"series": {"SPY": [[AUG17, 772.0], [AUG18, 774.0]]},
                                "latest_completed_date": AUG18},
                    inputs={"market_as_of_date": AUG18, "momentum_month": "2026-08",
                            "fundamental_as_of_date": "2026-05-22"},
                    daily_status={"latest_valid_mark_date": AUG18},
                    close_progress=dict(AUG17_CLOSE_PROGRESS))
        args.update(kw)
        return _load(**args)

    def test_39_the_fixture_really_is_post_cutoff_with_AUG18_confirmed(self):
        cur = self._post()["current_session"]
        assert cur["calendar_date"] == AUG18
        assert cur["expected_completed_market_date"] == AUG18
        assert cur["latest_eligible_completed_market_date"] == AUG18
        assert cur["session_status"] != msession.BEFORE_SESSION_CLOSE

    def test_40_the_market_session_owner_advances_to_AUG18(self):
        s = msession.evaluate_session(
            now=NOW_POST_CLOSE, latest_confirmed_owned_data_date=AUG18,
            latest_benchmark_date=AUG18)
        assert s.expected_completed_market_date == AUG18
        assert s.eligible_market_date == AUG18
        assert s.session_status != msession.BEFORE_SESSION_CLOSE

    def test_41_the_overall_state_is_READY_FOR_DAILY_CLOSE(self):
        assert self._post()["overall_state"] == ws.READY_FOR_DAILY_CLOSE

    def test_42_the_cycle_stage_is_DAILY_CLOSE(self):
        cyc = self._post()["normal_cycle"]
        assert cyc["current_stage"] == nc.STAGE_DAILY_CLOSE
        assert cyc["stage_gates"][nc.STAGE_DAILY_CLOSE]["execution_allowed"] is True

    def test_43_exactly_ONE_normal_mutation_is_offered(self):
        cyc = self._post()["normal_cycle"]
        assert cyc["executable_stages"] == [nc.STAGE_DAILY_CLOSE]
        assert cyc["executable_stage_count"] == 1

    def test_44_the_primary_action_is_RUN_DAILY_CLOSE(self):
        r = self._post()
        assert r["primary_action"]["action_code"] == ws.ACTION_RUN_DAILY_CLOSE
        assert r["primary_action"]["execution_available"] is True
        assert r["daily_close_gate"]["execution_allowed"] is True

    def test_45_the_AUG17_close_is_still_valid_while_AUG18_is_due(self):
        # A newer session becoming eligible never retroactively invalidates the old one.
        os_ = self._post()["operational_state"]
        assert os_["operational_close_valid"] is True
        assert os_["latest_completed_close_date"] == AUG17
        assert os_["eligible_session_already_processed"] is False

    def test_46_a_legitimate_next_close_is_CONSISTENT(self):
        r = self._post()
        assert r["consistency_status"] == ws.CONSISTENT
        assert r["consistency_violations"] == []

    def test_47_the_repair_did_not_simply_disable_the_close(self):
        # The two fixtures differ ONLY in the clock and the owned-data date, and they
        # produce opposite verdicts. That is what proves session authority, rather
        # than a blanket suppression.
        assert _load()["daily_close_gate"]["execution_allowed"] is False
        assert self._post()["daily_close_gate"]["execution_allowed"] is True


# =========================================================================== #
# 4. THE SESSION-AUTHORITY INVARIANTS (runtime, fail-closed)
# =========================================================================== #
class TestSessionAuthorityInvariants:

    def test_50_the_violation_codes_are_frozen(self):
        assert ws.SESSION_AUTHORITY_VIOLATION_CODES == (
            "DAILY_CLOSE_OFFERED_FOR_ALREADY_PROCESSED_SESSION",
            "COMPLETED_CLOSE_REPORTED_INVALID",
            "COMPLETED_CLOSE_HIDDEN_FROM_EVIDENCE")

    def test_51_S1_a_close_offered_for_a_processed_session_is_a_violation(self):
        v = ws.check_session_authority(
            session_status="BEFORE_SESSION_CLOSE", eligible_market_date=AUG17,
            expected_completed_market_date=AUG17,
            latest_completed_close_date=AUG17, operational_close_valid=True,
            latest_close_status=dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT,
            daily_close_execution_allowed=True, evidence_completed_close_state="VALID")
        assert [x["code"] for x in v] == [
            "DAILY_CLOSE_OFFERED_FOR_ALREADY_PROCESSED_SESSION"]
        assert v[0]["authoritative_owners"] == ["engine.market_session", "api.daily_close"]

    def test_52_S2_a_completed_close_reported_invalid_is_a_violation(self):
        v = ws.check_session_authority(
            session_status="BEFORE_SESSION_CLOSE", eligible_market_date=AUG17,
            expected_completed_market_date=AUG17,
            latest_completed_close_date=AUG17, operational_close_valid=False,
            latest_close_status=dc.CLOSE_COMPLETE_HOLD,
            daily_close_execution_allowed=False, evidence_completed_close_state="NONE")
        assert [x["code"] for x in v] == ["COMPLETED_CLOSE_REPORTED_INVALID"]

    def test_53_S3_a_valid_close_hidden_from_evidence_is_a_violation(self):
        v = ws.check_session_authority(
            session_status="BEFORE_SESSION_CLOSE", eligible_market_date=AUG17,
            expected_completed_market_date=AUG17,
            latest_completed_close_date=AUG17, operational_close_valid=True,
            latest_close_status=dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT,
            daily_close_execution_allowed=False, evidence_completed_close_state="NONE")
        assert [x["code"] for x in v] == ["COMPLETED_CLOSE_HIDDEN_FROM_EVIDENCE"]

    def test_54_a_correct_composition_raises_nothing(self):
        assert ws.check_session_authority(
            session_status="BEFORE_SESSION_CLOSE", eligible_market_date=AUG17,
            expected_completed_market_date=AUG17,
            latest_completed_close_date=AUG17, operational_close_valid=True,
            latest_close_status=dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT,
            daily_close_execution_allowed=False,
            evidence_completed_close_state="VALID") == []

    def test_55_a_genuinely_due_next_close_raises_nothing(self):
        assert ws.check_session_authority(
            session_status="SESSION_READY", eligible_market_date=AUG18,
            expected_completed_market_date=AUG18,
            latest_completed_close_date=AUG17, operational_close_valid=True,
            latest_close_status=dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT,
            daily_close_execution_allowed=True,
            evidence_completed_close_state="VALID") == []

    def test_56_the_check_recomputes_no_owner_economics(self):
        # It compares published answers. It must never import a kernel or a store.
        src = _src("api", "workflow_state.py")
        body = src.split("def check_session_authority(")[1].split("\ndef ")[0]
        for forbidden in ("load_", "import ", "open(", "Path("):
            assert forbidden not in body, forbidden

    def test_57_the_violations_reach_the_composed_payload(self):
        # A regression must surface as INCONSISTENT rather than a silent mutation.
        r = _load()
        assert r["consistency_status"] == ws.CONSISTENT
        merged = _src("api", "workflow_state.py")
        assert "session_violations = check_session_authority(" in merged
        assert "consistency_violations = list(consistency_violations) + session_violations" \
            in merged


# =========================================================================== #
# 5. EVIDENCE PRESENTATION — A COMPLETED CLOSE IS NEVER ERASED
# =========================================================================== #
class TestEvidencePresentation:

    def test_60_the_completed_AUG17_close_stays_visible_while_AUG18_is_open(self):
        ep = _load()["evidence_presentation"]
        assert ep["latest_completed_close"]["state"] == "VALID"
        assert AUG17 in ep["latest_completed_close"]["explanation"]

    def test_61_the_open_session_still_says_no_result_yet(self):
        # Both facts are true at once and neither may overwrite the other.
        ep = _load()["evidence_presentation"]
        assert ep["current_session"]["state"] == "NO_RESULT_YET"
        assert "still-open current session" in ep["current_session"]["explanation"]

    def test_62_it_never_claims_no_completed_close_exists(self):
        ep = _load()["evidence_presentation"]
        assert "No completed operational close has been recorded yet." \
            not in ep["explanation"]

    def test_63_completed_summary_and_evidence_presentation_agree(self):
        r = _load()
        summary = (r["completed_summary"] or {}).get("latest_completed_close") or {}
        assert summary.get("market_date") == AUG17
        assert r["evidence_presentation"]["latest_completed_close"]["state"] == "VALID"

    def test_64_an_incomplete_attempt_is_named_honestly_not_erased(self):
        ep = ws.build_evidence_presentation(
            operational_close_valid=False, latest_close_date=AUG18,
            evidence_gap=False, active_book_snapshot_present=True,
            current_session_open=False)
        assert ep["latest_completed_close"]["state"] == "NOT_COMPLETED"
        assert AUG18 in ep["latest_completed_close"]["explanation"]

    def test_65_a_genuinely_empty_history_still_says_NONE(self):
        ep = ws.build_evidence_presentation(
            operational_close_valid=False, latest_close_date=None,
            evidence_gap=False, active_book_snapshot_present=False,
            current_session_open=True)
        assert ep["latest_completed_close"]["state"] == "NONE"

    def test_66_a_documented_forward_evidence_gap_is_still_a_VALID_close(self):
        ep = ws.build_evidence_presentation(
            operational_close_valid=True, latest_close_date=AUG17,
            evidence_gap=True, active_book_snapshot_present=False,
            current_session_open=True)
        assert ep["latest_completed_close"]["state"] == "VALID_WITH_DOCUMENTED_GAP"
        assert ep["severity"] == ws.SEV_ATTENTION


# =========================================================================== #
# 6. LANE SEPARATION — PORTFOLIO FINDINGS NEVER TOUCH OPERATIONAL COMPLETION
# =========================================================================== #
class TestLaneSeparation:

    @pytest.mark.parametrize("summary", [
        {"state": "CHANGE_CANDIDATE", "proposal_required": False},
        {"state": "PROPOSAL_READY", "proposal_required": True},
        {"state": "NO_CHANGE", "proposal_required": False},
        {"state": "BLOCKED_EVIDENCE", "proposal_required": False},
    ])
    def test_70_no_reassessment_verdict_can_invalidate_the_close(self, summary):
        r = _load(reassessment_summary=_reassessment(**summary))
        assert r["operational_state"]["operational_close_valid"] is True

    @pytest.mark.parametrize("outcome", [
        "MEMBERSHIP_DRIFT_DETECTED", "NO_ACTION_TODAY", "DATA_NOT_READY"])
    def test_71_no_membership_outcome_can_invalidate_the_close(self, outcome):
        r = _load(gate=_gate(outcome=outcome))
        assert r["operational_state"]["operational_close_valid"] is True

    def test_72_the_close_owner_still_reports_the_drift_it_observed(self):
        # Separating the lanes does not hide the finding — it just stops it deciding
        # something it never owned.
        r = _load()
        assert (r["operational_state"]["latest_close_status"]
                == dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT)

    def test_73_the_close_status_still_carries_no_proposal_claim(self):
        # The Release 29.3 guarantee must survive Release 29.4.
        for token in (dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT, dc.DECISION_MEMBERSHIP_DRIFT):
            assert "PROPOSAL" not in token
            assert "REBALANCE" not in token


# =========================================================================== #
# 7. TODAY IS THE SOLE NORMAL-PATH EXECUTION SURFACE
# =========================================================================== #
class TestTodayIsTheSoleActionSurface:

    def test_80_the_execute_CTA_is_hidden_off_today(self):
        ui = _ui()
        block = ui.split("R29.4 - TODAY IS THE SOLE NORMAL-PATH EXECUTION SURFACE")[1] \
                  .split("</style>")[0]
        for route in ("portfolio-manager", "holding-review", "proposed-portfolio",
                      "markets", "system-audit"):
            assert 'body[data-route="%s"] #operator-command .opc-cta' % route in block

    def test_81_a_routing_notice_replaces_it(self):
        ui = _ui()
        assert 'id="opc-go-today"' in ui
        assert "Open Today to act" in ui

    def test_82_the_dispatcher_refuses_to_execute_off_today(self):
        ui = _ui()
        fn = ui.split("function dispatchCanonicalPrimaryAction(")[1].split("\nwindow.")[0]
        assert "_wsIsTodayRoute()" in fn
        assert "navigateToRoute('command-center')" in fn

    def test_83_the_today_route_predicate_names_both_today_ids(self):
        ui = _ui()
        fn = ui.split("function _wsIsTodayRoute(")[1].split("\n}")[0]
        assert "command-center" in fn and "today" in fn

    def test_84_today_still_carries_the_executable_CTA(self):
        ui = _ui()
        block = ui.split("R29.4 - TODAY IS THE SOLE NORMAL-PATH EXECUTION SURFACE")[1] \
                  .split("</style>")[0]
        assert 'body[data-route="command-center"] #operator-command .opc-cta' not in block
        assert 'body[data-route="today"] #operator-command .opc-cta' not in block

    def test_85_no_alert_or_confirm_was_introduced(self):
        ui = _ui()
        assert not re.search(r"(?<![\w.])alert\s*\(", ui)
        assert not re.search(r"(?<![\w.])confirm\s*\(", ui)


# =========================================================================== #
# 8. THE MODEL-TARGET SNAPSHOT LANE IS NOT PORTFOLIO CAPITAL ALLOCATION
# =========================================================================== #
class TestModelTargetSnapshotLane:

    def test_90_the_band_states_its_scope_explicitly(self):
        ui = _ui()
        assert "MODEL TARGET SNAPSHOT REVIEW" in ui
        assert 'id="otr-scope"' in ui

    def test_91_it_says_it_is_not_a_reallocation_proposal(self):
        ui = _ui()
        scope = ui.split('id="otr-scope"')[1].split("</div>")[0]
        low = scope.lower()
        assert "not a portfolio reallocation proposal" in low
        assert "no capital" in low or "creates no order" in low

    def test_92_its_ready_state_names_the_snapshot(self):
        ui = _ui()
        assert "'READY_TO_CONFIRM': 'READY TO CONFIRM SNAPSHOT'" in ui

    def test_93_it_is_not_an_input_to_the_canonical_portfolio_decision(self):
        # The Release 30 contract reads the portfolio owners ONLY.
        src = _src("api", "workflow_state.py")
        fn = src.split("def build_canonical_portfolio_decision(")[1].split("\ndef ")[0]
        for forbidden in ("alpha_target", "target_readiness", "otr", "snapshot"):
            assert forbidden not in fn, forbidden

    def test_94_the_canonical_decision_is_unaffected_by_target_readiness(self):
        a = _load()["canonical_portfolio_decision"]
        b = _load(target_readiness={"dates": {"alpha_market_date": "2026-01-02"}})[
            "canonical_portfolio_decision"]
        assert a["state"] == b["state"]


# =========================================================================== #
# 9. STRUCTURAL ARCHITECTURE CONTRACTS (the audit's own invariants)
# =========================================================================== #
class TestStrictArchitectureAudit:

    def test_100_the_audit_registers_the_release29_4_check(self):
        src = _src("scripts", "audit_architecture.py")
        assert "def check_release29_4_session_authority(" in src
        assert "release29_4_session_authority" in src

    def test_101_every_invariant_is_blocking(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "_audit_r294",
            Path(__file__).resolve().parents[1] / "scripts" / "audit_architecture.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        blocking = {(section, key) for section, key, _ in mod.BLOCKING_INVARIANTS}
        required = {
            "close_validity_owned_by_daily_close",
            "no_duplicate_close_vocabulary",
            "workflow_delegates_close_validity",
            "close_validity_excludes_portfolio_inputs",
            "session_eligibility_owned_by_market_session",
            "session_authority_codes_frozen",
            "session_check_wired",
            "today_is_sole_execution_surface",
            "model_target_lane_scoped",
        }
        assert {("release29_4_session_authority", k) for k in required} <= blocking

    def test_102_workflow_state_does_not_recompute_market_dates(self):
        # Invariant 5: eligibility arrives from engine.market_session via
        # api.data_freshness. The workflow owner runs no calendar arithmetic of its own.
        src = _src("api", "workflow_state.py")
        assert ws.SESSION_ELIGIBILITY_OWNER == "engine.market_session"
        for forbidden in ("walk_back_to_trading_day(", "previous_trading_day(",
                          "resolve_expected_session(", "expected_from_reference_date("):
            assert forbidden not in src, forbidden

    def test_103_the_normal_cycle_kernel_stays_a_pure_projection(self):
        # No second state machine was introduced.
        src = _src("engine", "normal_cycle.py")
        assert "import" not in src.split('"""', 2)[2].split("CALCULATION_OWNER")[0] \
            .replace("from __future__ import annotations", "") \
            .replace("from typing import Any, Optional", "")
        assert set(nc.STAGE_SEQUENCE) == {
            "WAIT_FOR_SESSION_CLOSE", "DAILY_CLOSE", "DAILY_RESEARCH_CYCLE",
            "PORTFOLIO_DECISION", "CONTROLLED_REBALANCE"}

    def test_104_the_release29_3_decision_contract_is_untouched(self):
        r = _load()
        cpd = r["canonical_portfolio_decision"]
        assert cpd["state"] in ws.CANONICAL_PORTFOLIO_DECISION_STATES
        assert cpd["approvable"] is False
        # The Aug-17 verdict still reads from its own owners, unchanged by 29.4.
        assert cpd["reassessment_state"] == "CHANGE_CANDIDATE"
        assert cpd["proposal_state"] == "REALLOCATION_PROPOSAL_NOT_RUN"
        assert cpd["mandatory_exit_obligation"] == "REQUIRED_IF_REALLOCATION_PROCEEDS"
        assert r["primary_action"]["execution_available"] is False

    def test_105_nothing_here_creates_an_order_or_enables_automation(self):
        r = _load()
        assert r["normal_cycle"]["creates_orders"] is False
        assert r["normal_cycle"]["automatic_execution"] is False
        for gate in r["normal_cycle"]["stage_gates"].values():
            assert gate["creates_orders"] is False
