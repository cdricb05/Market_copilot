r"""RELEASE 55.1 — INTRADAY GOVERNANCE COMPLETION & NO-OP SEMANTIC CLARITY.

WHAT R55.1 REPAIRS
------------------
R55 shipped an acceptance contract that reported GOVERNANCE = MISSING on a
chain that had in fact completed correctly. Three defects sat behind that:

1.  THE GATE'S SILENCE WAS AMBIGUOUS. ``api.event_signal_refresh`` invokes the
    R54.1 gate if and only if the cycle produced a reassessment candidate, so a
    cycle that terminated at NO_NEW_INFORMATION required no verdict. Nothing
    said so. "The gate declined to promote", "the gate was never invoked" and
    "no verdict was required" all reached the operator as one absence.

2.  A NO-OP CYCLE PRESENTED AS AN UNREADABLE REASSESSMENT. The live lane fell
    through to ``UNKNOWN`` whenever the cycle state was not one it enumerated,
    so a healthy NO_NEW_INFORMATION cycle displayed as
    "Latest reassessment -> UNKNOWN": a reassessment that never ran, shown as
    one whose conclusion could not be read.

3.  ``persisted: false`` SAT BESIDE A REAL RECORD ID. Truthful — the Sep-2
    session closed before R54.4 made the daily cycle delegate its governed
    write, so it is served by the read-time legacy projection — but it read as
    a contradiction, because nothing said which of the two live states it was.

THE CONTRACT
------------
``api.portfolio_decision`` remains the ONE governance authority. R55.1 adds no
second gate, no second decision writer, no second ledger and no second event
path: it adds a PURE CLASSIFIER inside that owner
(:func:`classify_intraday_governance`) which reads facts the gate and the cycle
owner already recorded and issues ONE terminal disposition per cycle.

NOT_REQUIRED is a valid terminal disposition. MISSING means the system cannot
prove what happened. The two are never merged, and no governed row is ever
written to turn an acceptance row green.

Every test here is hermetic: fixtures only, no store, no route, no clock, no
network, no process.
"""
from __future__ import annotations

import inspect
from pathlib import Path

from paper_trader.api import active_manager_state as ams
from paper_trader.api import event_signal_refresh as esr
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import workflow_state as ws

REPO = Path(__file__).resolve().parents[1]
UI = (REPO / "api" / "ui" / "index.html").read_text(
    encoding="utf-8", errors="replace")

# --------------------------------------------------------------------------- #
# The five cycle shapes, written exactly as the owners record them.
# --------------------------------------------------------------------------- #
#: The real 2026-09-03 17:47 UTC cycle: a signal refresh that found nothing.
NO_NEW = {
    "run_id": "evt_9dc69133094e9b27", "state": "NO_NEW_INFORMATION",
    "generated_at": "2026-09-03T17:47:48.896626+00:00",
    "reassessment_ran": False, "proposal_built": False,
    "governance_gate_invoked": False,
    "materiality_change_level": "SIGNAL_CHANGED",
}
#: The real 2026-09-03 17:29 UTC cycle: it reassessed, and never reached the gate.
REASSESSED_NO_GATE = {
    "run_id": "evt_32f3d95a34c68b61", "state": "REASSESSED_NO_CHANGE",
    "generated_at": "2026-09-03T17:29:42.227509+00:00",
    "reassessment_ran": True, "proposal_built": False,
    "governance_gate_invoked": False,
    "reassessment_state": "CURRENT_NO_CHANGE",
    "reassessment_id": "prs_live", "reassessment_hash": "hash_live",
}
#: The gate ran and did not promote.
EVALUATED = {
    **REASSESSED_NO_GATE, "run_id": "evt_evaluated",
    "governance_gate_invoked": True,
    "governed_decision": {"evaluated": True, "verdict": "ELIGIBLE",
                          "recorded": False, "decision": "HOLD_CURRENT_BOOK",
                          "candidate_identity_hash": "cid_eval",
                          "withheld_reason_codes": [], "failing_checks": []},
}
#: The gate ran and withheld.
WITHHELD = {
    **REASSESSED_NO_GATE, "run_id": "evt_withheld",
    "governance_gate_invoked": True,
    "governed_decision": {
        "evaluated": True, "verdict": "WITHHELD", "recorded": False,
        "candidate_identity_hash": "cid_wh",
        "withheld_reason_codes": ["SAME_SESSION_HOC_ARTIFACT_IMMUTABLE"],
        "failing_checks": ["hoc_artifact_binding"]},
}
#: The gate promoted through the ONE writer.
PROMOTED = {
    **REASSESSED_NO_GATE, "run_id": "evt_promoted", "proposal_built": True,
    "reassessment_state": "PROPOSAL_READY", "governance_gate_invoked": True,
    "governed_decision": {"evaluated": True, "verdict": "PROMOTED",
                          "recorded": True, "record_id": "gid_intraday_1",
                          "provenance": "GOVERNED_INTRADAY",
                          "decision": "CHANGE_RECOMMENDED",
                          "supersedes_decision_id": "drc_governed_prior",
                          "candidate_identity_hash": "cid_pr"},
}
#: A cycle that reached the gate and recorded nothing at all.
GATE_SILENT = {**REASSESSED_NO_GATE, "run_id": "evt_silent",
               "governance_gate_invoked": True}

STANDING = "drc_governed_drc_2026-09-02_15abfb01856f"


def _state(cycle: dict, *, governed: dict = None) -> dict:
    """A composed active-manager state over ONE cycle. Hermetic: every owner
    view is injected, so nothing here reads a store or a clock."""
    return ams.build_active_manager_state(
        workflow={"portfolio_attention": {"review_required": False}},
        event_refresh={"last_run": cycle, "material_event_count": 24,
                       "affected_holdings": ["AMD", "ANET"],
                       "recent_events": [], "material_events": []},
        governed_decision=(governed if governed is not None else {
            "available": True, "decision": "CURRENT_NO_CHANGE",
            "record_id": STANDING, "persisted": False,
            "eligible_market_session": "2026-09-02",
            "timestamp": "2026-09-02T23:51:50.475243Z",
            "provenance": "GOVERNED_DAILY_CYCLE"}))


def _gov_row(state: dict) -> dict:
    return next(r for r in ams.build_acceptance_contract(state)["rows"]
                if r["row"] == "GOVERNANCE")


# =========================================================================== #
# SCENARIO 1 — NO_NEW_INFORMATION: a successful terminal no-op.
# =========================================================================== #
class TestScenario1NoNewInformation:
    def test_the_owner_issues_a_terminal_not_required_disposition(self):
        d = pdec.classify_intraday_governance(event_cycle=NO_NEW)
        assert d["disposition"] == pdec.GOV_DISP_NOT_REQUIRED
        assert d["terminal"] is True
        assert d["required"] is False
        assert d["reason"] == "NO_REASSESSMENT_CANDIDATE_PRODUCED"
        assert d["owner"] == "api.portfolio_decision"

    def test_hoc_and_reassessment_were_not_required(self):
        assert NO_NEW["reassessment_ran"] is False
        assert esr.stages_not_required(NO_NEW) == [
            "hoc_completed_at", "reassessment_completed_at",
            "target_completed_at", "governance_gate_completed_at",
            "governed_decision_persisted_at"]

    def test_acceptance_governance_is_present(self):
        row = _gov_row(_state(NO_NEW))
        assert row["status"] == ams.ACCEPTANCE_PRESENT
        assert row["disposition"] == pdec.GOV_DISP_NOT_REQUIRED
        assert row["required"] is False

    def test_no_fake_reassessment_is_manufactured(self):
        lane = _state(NO_NEW)["live_reassessment_lane"]
        assert lane["reassessment_ran"] is False
        assert lane["candidate_conclusion"] == "NO_REASSESSMENT_REQUIRED"
        assert lane["candidate_conclusion"] != "UNKNOWN"
        assert lane["reassessment_id"] is None
        assert lane["reassessment_hash"] is None

    def test_no_governed_decision_is_created(self):
        state = _state(NO_NEW)
        gov = state["intraday_governance"]
        assert gov["promoted_to_governed"] is False
        assert gov["governed_record_id"] is None
        d = pdec.classify_intraday_governance(event_cycle=NO_NEW)
        assert d["writes_nothing"] is True
        assert d["creates_no_governed_row"] is True
        assert d["promotion_decision_id"] is None

    def test_the_standing_governed_decision_is_unchanged(self):
        lane = _state(NO_NEW)["live_reassessment_lane"]
        assert lane["supersedes_standing_decision"] is False
        assert lane["standing_governed_decision_id"] == STANDING
        assert lane["governed_summary"] == "Standing governed decision unchanged"


# =========================================================================== #
# SCENARIO 2 — MATERIAL INFORMATION -> HOC -> REASSESSMENT -> HOLD.
# =========================================================================== #
class TestScenario2EvaluatedNoPromotion:
    def test_governance_was_evaluated(self):
        d = pdec.classify_intraday_governance(event_cycle=EVALUATED)
        assert d["evaluated"] is True
        assert d["required"] is True
        assert d["gate_invoked_by_cycle"] is True

    def test_the_disposition_is_explicitly_evaluated_no_promotion(self):
        d = pdec.classify_intraday_governance(event_cycle=EVALUATED)
        assert d["disposition"] == pdec.GOV_DISP_EVALUATED_NO_PROMOTION
        assert d["terminal"] is True
        assert d["reason"] == "GATE_EVALUATED_AND_DID_NOT_PROMOTE"

    def test_the_exact_hoc_and_reassessment_identities_are_bound(self):
        d = pdec.classify_intraday_governance(event_cycle=EVALUATED)
        assert d["candidate_reassessment_id"] == "prs_live"
        assert d["candidate_reassessment_hash"] == "hash_live"
        assert d["candidate_identity_hash"] == "cid_eval"
        assert d["event_cycle_run_id"] == "evt_evaluated"

    def test_acceptance_governance_is_present(self):
        assert _gov_row(_state(EVALUATED))["status"] == ams.ACCEPTANCE_PRESENT

    def test_the_standing_governed_decision_is_unchanged(self):
        lane = _state(EVALUATED)["live_reassessment_lane"]
        assert lane["promoted_to_governed"] is False
        assert lane["is_authoritative_decision"] is False
        assert lane["governance_state"] == "EVALUATED_NO_PROMOTION"

    def test_evaluated_no_promotion_is_not_the_old_eligible_word(self):
        """ELIGIBLE now means only 'a candidate exists and the gate has not
        spoken' — an unproven state. A gate that ran must not borrow it."""
        lane = _state(EVALUATED)["live_reassessment_lane"]
        assert lane["governance_state"] != "ELIGIBLE"


# =========================================================================== #
# SCENARIO 3 — the gate WITHHELD, with the exact reason.
# =========================================================================== #
class TestScenario3Withheld:
    def test_the_disposition_is_withheld_with_the_exact_reason(self):
        d = pdec.classify_intraday_governance(event_cycle=WITHHELD)
        assert d["disposition"] == pdec.GOV_DISP_WITHHELD
        assert d["terminal"] is True
        assert d["withheld_reason_codes"] == [
            "SAME_SESSION_HOC_ARTIFACT_IMMUTABLE"]
        assert d["failing_checks"] == ["hoc_artifact_binding"]

    def test_no_promotion_occurred(self):
        d = pdec.classify_intraday_governance(event_cycle=WITHHELD)
        assert d["promoted_to_governed"] is False
        assert d["promotion_decision_id"] is None

    def test_acceptance_is_present_and_carries_the_reason(self):
        row = _gov_row(_state(WITHHELD))
        assert row["status"] == ams.ACCEPTANCE_PRESENT
        assert row["withheld_reason_codes"] == [
            "SAME_SESSION_HOC_ARTIFACT_IMMUTABLE"]
        assert row["failing_checks"] == ["hoc_artifact_binding"]

    def test_a_withheld_result_is_never_authoritative(self):
        lane = _state(WITHHELD)["live_reassessment_lane"]
        assert lane["governance_state"] == "WITHHELD"
        assert lane["is_authoritative_decision"] is False
        assert lane["supersedes_standing_decision"] is False


# =========================================================================== #
# SCENARIO 4 — an eligible intraday PROMOTION.
# =========================================================================== #
class TestScenario4Promotion:
    def test_the_disposition_is_promoted(self):
        d = pdec.classify_intraday_governance(event_cycle=PROMOTED)
        assert d["disposition"] == pdec.GOV_DISP_PROMOTED
        assert d["terminal"] is True
        assert d["promoted_to_governed"] is True
        assert d["promotion_decision_id"] == "gid_intraday_1"

    def test_the_governed_write_stays_with_the_one_writer(self):
        """R55.1 records the disposition; it never writes the decision. The
        ONE writer remains :func:`record_governed_decision`."""
        src = inspect.getsource(pdec.classify_intraday_governance)
        for banned in ("record_governed_decision(", "_atomic_write_json(",
                       "open(", "govern_latest_intraday_assessment("):
            assert banned not in src

    def test_provenance_is_governed_intraday(self):
        lane = _state(PROMOTED)["live_reassessment_lane"]
        assert lane["governance_state"] == "GOVERNED"
        assert lane["promoted_to_governed"] is True
        assert lane["is_authoritative_decision"] is True

    def test_supersession_ordering_is_recorded_not_recomputed(self):
        lane = _state(PROMOTED)["live_reassessment_lane"]
        assert lane["supersedes_standing_decision"] is True
        assert lane["governed_record_id"] == "gid_intraday_1"
        assert lane["governed_summary"].startswith(
            "This cycle became the latest governed portfolio decision")

    def test_acceptance_governance_is_present(self):
        row = _gov_row(_state(PROMOTED))
        assert row["status"] == ams.ACCEPTANCE_PRESENT
        assert row["promotion_decision_id"] == "gid_intraday_1"


# =========================================================================== #
# SCENARIO 5 — an ambiguous / incomplete cycle. Nothing is inferred.
# =========================================================================== #
class TestScenario5Incomplete:
    def test_a_reassessment_without_a_gate_verdict_is_incomplete(self):
        d = pdec.classify_intraday_governance(event_cycle=REASSESSED_NO_GATE)
        assert d["disposition"] == pdec.GOV_DISP_INCOMPLETE
        assert d["terminal"] is False
        assert d["required"] is True

    def test_the_two_provable_causes_are_named_separately(self):
        """'Never invoked' and 'invoked but silent' are different failures and
        must not be reported as one shrug."""
        assert pdec.classify_intraday_governance(
            event_cycle=REASSESSED_NO_GATE)["reason"] == \
            "GATE_NOT_INVOKED_AFTER_REASSESSMENT"
        assert pdec.classify_intraday_governance(
            event_cycle=GATE_SILENT)["reason"] == \
            "GATE_INVOKED_WITHOUT_RECORDED_VERDICT"

    def test_acceptance_governance_stays_missing(self):
        for cyc in (REASSESSED_NO_GATE, GATE_SILENT):
            assert _gov_row(_state(cyc))["status"] == ams.ACCEPTANCE_MISSING

    def test_an_empty_cycle_proves_nothing(self):
        d = pdec.classify_intraday_governance(event_cycle={})
        assert d["disposition"] == pdec.GOV_DISP_INCOMPLETE
        assert d["reason"] == "NO_EVENT_CYCLE_RECORDED"
        assert d["required"] is None
        assert d["at"] is None

    def test_a_blocked_cycle_is_never_excused(self):
        """BLOCKED is deliberately absent from the no-candidate states: a
        blocked cycle proves nothing about whether governance was required."""
        blocked = {"run_id": "evt_b", "state": "BLOCKED",
                   "reassessment_ran": False}
        assert "BLOCKED" not in esr.NO_CANDIDATE_CYCLE_STATES
        d = pdec.classify_intraday_governance(event_cycle=blocked)
        assert d["disposition"] == pdec.GOV_DISP_INCOMPLETE
        assert esr.stages_not_required(blocked) == []

    def test_an_unrecorded_reassessment_flag_is_not_read_as_false(self):
        """Fail-closed: only an EXPLICIT False excuses the downstream stages."""
        vague = {"run_id": "evt_v", "state": "NO_NEW_INFORMATION"}
        assert pdec.classify_intraday_governance(
            event_cycle=vague)["disposition"] == pdec.GOV_DISP_INCOMPLETE
        assert esr.stages_not_required(vague) == []

    def test_an_unavailable_owner_never_fails_open(self):
        assert ams._classify_governance(None) == {} or \
            ams._classify_governance(None).get("disposition") == \
            pdec.GOV_DISP_INCOMPLETE


# =========================================================================== #
# SCENARIO 6 — governed decision RETRIEVABILITY, truthfully reported.
# =========================================================================== #
class TestScenario6DecisionPersistence:
    def test_a_ledger_row_is_reported_as_a_ledger_row(self):
        p = pdec.classify_decision_persistence(
            record={"decision": "CURRENT_NO_CHANGE", "persisted": True},
            available=True)
        assert p["persistence_status"] == pdec.DECISION_PERSISTENCE_LEDGER_ROW
        assert p["is_ledger_row"] is True
        assert p["retrievable_through_owner"] is True

    def test_a_pre_r54_4_session_is_a_legacy_projection_not_a_defect(self):
        p = pdec.classify_decision_persistence(
            record={"decision": "CURRENT_NO_CHANGE",
                    "legacy_compatibility_projection": True, "persisted": False},
            available=True)
        assert p["persistence_status"] == \
            pdec.DECISION_PERSISTENCE_LEGACY_PROJECTION
        assert p["is_ledger_row"] is False
        # It is still retrievable and authoritative through the ONE owner.
        assert p["retrievable_through_owner"] is True
        assert "before R54.4" in p["persistence_detail"]

    def test_a_missing_row_is_never_claimed_as_persisted(self):
        p = pdec.classify_decision_persistence(record=None, available=False)
        assert p["persistence_status"] == pdec.DECISION_PERSISTENCE_ABSENT
        assert p["is_ledger_row"] is False
        assert p["retrievable_through_owner"] is False

    def test_nothing_is_ever_backfilled(self):
        for rec in ({"decision": "X"}, {"decision": "X", "projected": True},
                    None):
            p = pdec.classify_decision_persistence(record=rec)
            assert p["backfilled"] is False
            assert p["history_rewritten"] is False

    def test_retrievability_is_derived_from_the_owner_not_a_browser(self):
        p = pdec.classify_decision_persistence(record={"decision": "X"})
        assert p["retrievability_owner"] == "api.portfolio_decision"

    def test_the_acceptance_row_explains_the_persistence_status(self):
        state = _state(NO_NEW, governed={
            "available": True, "decision": "CURRENT_NO_CHANGE",
            "record_id": STANDING, "persisted": False,
            **pdec.classify_decision_persistence(
                record={"decision": "CURRENT_NO_CHANGE",
                        "legacy_compatibility_projection": True},
                available=True)})
        row = next(r for r in ams.build_acceptance_contract(state)["rows"]
                   if r["row"] == "GOVERNED_DECISION")
        assert row["status"] == ams.ACCEPTANCE_PRESENT
        assert row["persistence_status"] == \
            pdec.DECISION_PERSISTENCE_LEGACY_PROJECTION
        assert row["is_ledger_row"] is False
        assert row["retrievable_through_owner"] is True


# =========================================================================== #
# SCENARIO 7 — UI semantics. The browser interprets nothing.
# =========================================================================== #
class TestScenario7UiSemantics:
    def test_no_new_information_never_renders_an_unknown_reassessment(self):
        chg = _state(NO_NEW)["operator_answer"]["what_changed_since"]
        assert chg["reassessment_ran"] is False
        assert chg["latest_reassessment_at"] is None
        assert chg["latest_reassessment_conclusion"] is None
        assert chg["headline"] == "Latest signal refresh: no new information"
        assert chg["reassessment_summary"] == \
            "No new portfolio reassessment was required"
        assert chg["governed_summary"] == "Standing governed decision unchanged"

    def test_the_cycle_still_has_a_time_under_its_own_name(self):
        chg = _state(NO_NEW)["operator_answer"]["what_changed_since"]
        assert chg["latest_cycle_at"] == NO_NEW["generated_at"]
        assert chg["latest_cycle_display"]

    def test_the_ui_reads_the_flag_and_does_not_derive_it(self):
        assert "chg.reassessment_ran" in UI
        assert "data-reassessment-ran=" in UI
        assert "lane.reassessment_ran === false" in UI
        # An EXPLICIT false takes the no-op branch. A pre-R55.1 payload that
        # publishes no flag keeps the previous rendering rather than silently
        # claiming that no reassessment ran.
        assert "chg.reassessment_ran === false" in UI
        # The backend's sentences are rendered verbatim.
        for field in ("chg.headline", "chg.reassessment_summary",
                      "chg.governed_summary", "lane.reassessment_summary"):
            assert field in UI

    def test_the_ui_never_recomputes_whether_a_reassessment_happened(self):
        """No browser-side inference from cycle state tokens."""
        for banned in ("=== 'NO_NEW_INFORMATION'", "=== 'REASSESSED_NO_CHANGE'",
                       "=== 'DUPLICATE_TRIGGER_SUPPRESSED'"):
            assert banned not in UI

    def test_the_governed_and_research_lanes_stay_distinct(self):
        state = _state(NO_NEW)
        assert state["live_reassessment_lane"]["is_authoritative_decision"] \
            is False
        assert state["operator_answer"]["what_changed_since"][
            "is_authoritative"] is False
        assert state["operator_answer"]["current_decision"][
            "is_authoritative"] is True
        assert "RESEARCH LANE &middot; NOT THE GOVERNED DECISION" in UI \
            or "RESEARCH LANE · NOT THE GOVERNED DECISION" in UI

    def test_the_acceptance_grid_shows_the_disposition(self):
        assert "data-disposition=" in UI
        assert "not_required_measurements" in UI


# =========================================================================== #
# STAGE-AWARE LATENCY — NOT_REQUIRED is never MISSING and never zero.
# =========================================================================== #
class TestStageAwareLatency:
    _STAMPS = {"signal_refresh_completed_at": "2026-09-03T17:53:12.838902+00:00",
               "scoring_completed_at": "2026-09-03T17:53:12.842559+00:00",
               "hoc_completed_at": None, "reassessment_completed_at": None,
               "target_completed_at": None}

    def _measure(self, cycle):
        return esr.measure_decision_latency(
            stage_timestamps=dict(self._STAMPS),
            event_cycle_started_at=cycle.get("generated_at"),
            observation_received_at="2026-09-03T17:14:03.777478+00:00",
            not_required_stages=esr.stages_not_required(cycle))

    def test_a_terminal_no_op_cycle_is_complete_not_broken(self):
        lat = self._measure(NO_NEW)
        assert lat["observation_to_signal_seconds"] == 2349.1
        assert lat["missing_measurements"] == []
        assert lat["latency_measurement_complete"] is True
        assert "hoc_completed_at" in lat["not_required_measurements"]

    def test_not_required_is_distinguished_from_missing(self):
        lat = self._measure(NO_NEW)
        assert lat["stage_dispositions"]["hoc_completed_at"] == "NOT_REQUIRED"
        assert lat["stage_dispositions"]["signal_refresh_completed_at"] == \
            "MEASURED"
        assert lat["interval_dispositions"][
            "signal_to_reassessment_seconds"] == "NOT_REQUIRED"

    def test_an_unexecuted_stage_is_never_zero_seconds(self):
        lat = self._measure(NO_NEW)
        for interval in ("signal_to_reassessment_seconds",
                         "reassessment_to_governed_seconds",
                         "observation_to_governed_seconds"):
            assert lat[interval] is None
        assert lat["never_zero_fills_an_unexecuted_stage"] is True

    def test_a_cycle_that_should_have_governed_stays_incomplete(self):
        lat = self._measure(REASSESSED_NO_GATE)
        assert "reassessment_completed_at" in lat["missing_measurements"]
        assert "governed_decision_persisted_at" in lat["missing_measurements"]
        assert lat["latency_measurement_complete"] is False
        # Only the target stage was excused: its proposal gate declined to build.
        assert lat["not_required_measurements"] == ["target_completed_at"]

    def test_a_stamped_stage_is_measured_even_if_a_caller_excuses_it(self):
        lat = esr.measure_decision_latency(
            stage_timestamps={"signal_refresh_completed_at": "2026-09-03T17:53:12Z"},
            observation_received_at="2026-09-03T17:14:03Z",
            not_required_stages=["signal_refresh_completed_at"])
        assert lat["stage_dispositions"]["signal_refresh_completed_at"] == \
            "MEASURED"
        assert lat["observation_to_signal_seconds"] is not None

    def test_the_composition_layer_asks_the_cycle_owner(self):
        assert ams._stages_not_required(NO_NEW) == esr.stages_not_required(NO_NEW)
        assert ams._stages_not_required(None) == []


# =========================================================================== #
# ARCHITECTURE — one authority, one writer, one event path.
# =========================================================================== #
class TestArchitecturalBoundaries:
    def test_the_governance_owner_is_unchanged(self):
        d = pdec.classify_intraday_governance(event_cycle=NO_NEW)
        assert d["owner"] == "api.portfolio_decision"
        assert d["gate_version"] == pdec.GOVERNANCE_GATE_VERSION
        assert ams.COMPONENT_OWNERS["intraday_governance"] == \
            "api.portfolio_decision"

    def test_the_classifier_reaches_no_verdict_of_its_own(self):
        """It classifies recorded facts. It runs no gate rule and no threshold."""
        src = inspect.getsource(pdec.classify_intraday_governance)
        body = src.replace(inspect.getdoc(pdec.classify_intraday_governance) or "", "")
        for banned in ("evaluate_intraday_governance", "build_intraday_candidate",
                       "hurdle", "threshold", "datetime.now", "_now("):
            assert banned not in body
        assert d_writes_nothing(d=pdec.classify_intraday_governance(
            event_cycle=PROMOTED))

    def test_the_composition_layer_holds_no_governance_rule(self):
        """R55.1 moved the NOT_REQUIRED inference OUT of the presentation owner."""
        src = inspect.getsource(ams._live_reassessment_lane_block)
        assert "GOVERNANCE_DISPOSITION_TO_LANE" in src
        # No re-derivation of the governance word from cycle facts.
        assert "LANE_GOV_NOT_REQUIRED\n" not in src

    def test_no_second_ledger_or_scheduler_is_introduced(self):
        src = inspect.getsource(pdec)
        assert src.count("_GOVERNED_RECORDS_FILE") >= 1
        for banned in ("governance_dispositions.json", "disposition_ledger",
                       "schedule.every", "CronTab"):
            assert banned not in src

    def test_not_required_is_a_terminal_disposition_and_missing_is_not(self):
        assert pdec.GOV_DISP_NOT_REQUIRED in \
            pdec.GOVERNANCE_TERMINAL_DISPOSITIONS
        assert pdec.GOV_DISP_INCOMPLETE not in \
            pdec.GOVERNANCE_TERMINAL_DISPOSITIONS

    def test_the_disposition_vocabulary_is_frozen(self):
        assert pdec.GOVERNANCE_DISPOSITION_VOCAB == (
            "NOT_REQUIRED_NO_NEW_INFORMATION", "EVALUATED_NO_PROMOTION",
            "WITHHELD", "PROMOTED", "INCOMPLETE")

    def test_every_disposition_maps_to_a_lane_word(self):
        for token in pdec.GOVERNANCE_DISPOSITION_VOCAB:
            assert token in ams.GOVERNANCE_DISPOSITION_TO_LANE
            assert ams.GOVERNANCE_DISPOSITION_TO_LANE[token] in \
                ams.LANE_GOVERNANCE_VOCAB

    def test_the_invocation_contract_is_stated_by_the_gate_owner(self):
        assert "if and only if" in pdec.INTRADAY_GATE_INVOCATION_CONTRACT
        d = pdec.classify_intraday_governance(event_cycle=NO_NEW)
        assert d["invocation_contract"] == pdec.INTRADAY_GATE_INVOCATION_CONTRACT

    def test_the_cycle_owner_proves_gate_invocation_from_its_own_steps(self):
        summary = esr.build_last_run_summary(
            {"run_id": "evt_x", "state": "REASSESSED_NO_CHANGE",
             "steps": [{"step": "MATERIALITY_GATE"},
                       {"step": esr.GOVERNANCE_GATE_STEP}]})
        assert summary["governance_gate_invoked"] is True
        assert esr.build_last_run_summary(
            {"run_id": "evt_y", "steps": [{"step": "MATERIALITY_GATE"}]}
        )["governance_gate_invoked"] is False

    def test_safety_is_unchanged_by_this_release(self):
        state = _state(PROMOTED)
        for block in (state["intraday_governance"],
                      state["live_reassessment_lane"]):
            assert block.get("created_orders", block.get("creates_orders")) \
                is False
            assert block.get("approved_anything",
                             block.get("approves_anything")) is False
            assert block["advances_operational_mark"] is False


def d_writes_nothing(*, d: dict) -> bool:
    """The disposition asserts, on every path, that it changed nothing."""
    return bool(d.get("writes_nothing") and d.get("recomputes_nothing")
                and d.get("creates_no_governed_row")
                and d.get("decided_here") is False)


# =========================================================================== #
# THE OPERATOR ACTION is untouched: R55.1 changes no priority policy.
# =========================================================================== #
def test_r55_1_changes_no_operator_action_policy():
    action = ws.build_operator_action(overall=ws.WAITING_FOR_SESSION_CLOSE)
    assert action["action"] == ws.OP_ACTION_WAIT_SESSION_CLOSE
    assert action["requires_operator_work"] is False
    # The one priority order is still _decide_overall's, and R55.1 touched
    # neither the vocabulary nor its rank order.
    assert ws.OPERATOR_ACTION_PRIORITY[0] == ws.OP_ACTION_BLOCKED
    assert ws.OPERATOR_ACTION_PRIORITY[-1] == ws.OP_ACTION_MONITOR
    assert len(ws.OPERATOR_ACTIONS) == 7


def test_the_acceptance_row_vocabulary_is_unchanged():
    """R55.1 completes a row; it does not add one."""
    assert ams.ACCEPTANCE_ROWS == (
        "COLLECTION", "SIGNAL", "SCORING", "HOC", "REASSESSMENT", "GOVERNANCE",
        "GOVERNED_DECISION", "OPERATIONAL_BOOK", "NEXT_ACTION", "LATENCY")
