r"""Release 29.5 — PRE-DRC PROVENANCE vs GOVERNED DRC TERMINAL EVIDENCE.

THE LIVE DEADLOCK THIS SUITE LOCKS OUT
--------------------------------------
On 2026-08-18, after a SUCCESSFUL Daily Close, the operator had no executable stage at
all — the normal cycle had suspended itself into RECOVERY over a blocker that only
running the suspended stage could have cleared:

    engine.market_session   session_status                 SESSION_READY
                            eligible completed session     2026-08-18
    api.daily_close         latest_completed_close_date    2026-08-18
                            operational_close_valid        True
    api.daily_research_cycle state                         INCONSISTENT   <-- wrong
                            blocker  TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST
    api.workflow_state      overall_state                  INCONSISTENT_STATE
                            normal_cycle.current_stage     RECOVERY       <-- wrong
                            executable_stage_count         0              <-- deadlock

    1. a downstream HOC artifact existed for 2026-08-18
    2. no DRC run manifest existed for 2026-08-18
    3. (1) + (2) was read as corruption  -> RECOVERY
    4. RECOVERY opens no stage gate      -> the DRC cannot be run
    5. the DRC is the only thing that writes the manifest whose absence caused (3)

WHO ACTUALLY WROTE THE ARTIFACT (proven, not assumed)
-----------------------------------------------------
Not the Daily Close, and not a half-finished DRC. The Release 28 event cycle, triggered
by Release 29 continuous collection:

    event_fabric/runs/evt_b91704271fb7a992/event_signal_refresh_status.json
        composition_owner       api.event_signal_refresh
        requested_by            PAPER_TRADER_INFORMATION_COLLECTION:6ed66ead-...
        eligible_market_date    2026-08-18
        completed_at            2026-08-18T22:07:52Z   (HOC artifact: 22:07:50Z)
        state                   REASSESSED_NO_CHANGE

That is the system working as designed. Continuous collection is SUPPOSED to refresh
signal state between governed cycles.

THE ROOT CAUSE
--------------
Two legitimate producers wrote indistinguishable artifacts. The guard inferred
provenance from EXISTENCE, and existence stopped being evidence of the governed cycle
the moment Release 28/29 gave a second owner the same canonical entry point.

THE DISTINCTION
---------------
An artifact is GOVERNED_DRC_TERMINAL only when it CLAIMS to be — i.e. carries a
``drc_run_id``. Everything else is LIVE_PRE_DRC_SIGNAL. Absence of a claim is not a
broken claim; a BROKEN claim is the corruption case, and it still fails closed.
"""
from __future__ import annotations

import copy
import inspect
from datetime import datetime
from pathlib import Path

import pytest

from paper_trader.api import daily_close as dc
from paper_trader.api import daily_research_cycle as drc
from paper_trader.api import event_signal_refresh as esr
from paper_trader.api import holding_opportunity_cost as hoc
from paper_trader.api import workflow_state as ws
from paper_trader.engine import market_hours as mh
from paper_trader.engine import market_session as msession
from paper_trader.engine import normal_cycle as nc

ROOT = Path(__file__).resolve().parents[1]
UI_FILE = ROOT / "api" / "ui" / "index.html"


def _ui() -> str:
    return UI_FILE.read_text(encoding="utf-8")


def _src(*parts) -> str:
    return ROOT.joinpath(*parts).read_text(encoding="utf-8")


# =========================================================================== #
# THE REAL AUG-18 POST-CLOSE WORLD, as constants. Nothing below re-derives them.
# =========================================================================== #
AUG17 = "2026-08-17"
AUG18 = "2026-08-18"
#: 18:05 ET on Tuesday 2026-08-18 — after the 17:30 cutoff, so AUG18 is itself the
#: expected completed session, and owned data has confirmed it.
NOW = datetime(2026, 8, 18, 18, 5, 0, tzinfo=mh._ET)

#: The Aug-18 close the operator really ran, and which really succeeded.
AUG18_CLOSE_PROGRESS = {"market_date": AUG18, "done": True,
                        "final_close_status": dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT}

#: The live artifact's real assessment hash and the run that really produced it.
LIVE_HOC_HASH = "76b92f028a0a5b1f24c7ca65f87b810902df94d1e67a344a84d1bef072170eab"
LIVE_EVENT_RUN = "evt_b91704271fb7a992"

_OPB = {"operational_book": {
    "book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1",
    "current_status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
    "nav_as_of_date": AUG18, "desk_mark_date": AUG18, "latest_desk_mark_date": AUG18,
    "nav": 99913.25, "cash": 4482.71, "holdings_count": 25, "pending_order_count": 0,
    "current_target": {"alpha_market_date": AUG18,
                       "latest_completed_market_date": AUG18}}}
_INPUTS = {"market_as_of_date": AUG18, "momentum_month": "2026-08",
           "fundamental_as_of_date": "2026-05-22"}
_DESK = {"series": {"SPY": [[AUG17, 772.0], [AUG18, 774.0]]},
         "latest_completed_date": AUG18}
_FWD = {"latest_snapshot_date": AUG18, "snapshot_count": 6,
        "evidence_state": "FORWARD_EVIDENCE_OK", "active_book": {}, "shadow_books": []}
_TR = {"dates": {"alpha_market_date": AUG18}}


def _gate(**kw):
    """The Daily Action Gate as it really reads on Aug-18: the LIVE opportunity-cost
    artifact is present, and it carries the artifact owner's provenance classification."""
    g = {"latest_completed_market_date": AUG18,
         "outcome": "MEMBERSHIP_DRIFT_DETECTED",
         "target_state": "MEMBERSHIP_DRIFT",
         "next_scheduled_full_review": "2026-09-01", "scheduled_review_due": False,
         "opportunity_cost_available": True, "opportunity_cost_state": "READY",
         "opportunity_cost_assessment_hash": LIVE_HOC_HASH,
         "opportunity_cost_recommendation_counts": {"HOLD": 9, "REDUCE": 8, "EXIT": 2,
                                                    "REPLACE": 6, "ADD": 6},
         "opportunity_cost_data_gaps": [],
         "opportunity_cost_bound_eligible_market_date": AUG18,
         "opportunity_cost_bound_active_book_id": "alpha_paper_book_1",
         # Release 29.5 — Class 1: real, current, and claiming nothing about a manifest.
         "opportunity_cost_artifact_class": hoc.ARTIFACT_CLASS_LIVE_PRE_DRC,
         "opportunity_cost_producer_owner": hoc.PRODUCER_EVENT_SIGNAL_REFRESH,
         "opportunity_cost_claims_drc_terminal": False,
         "opportunity_cost_drc_run_id": None,
         "opportunity_cost_proves_drc_complete": False}
    g.update(kw)
    return g


def _reassessment(**kw):
    """The live pre-DRC reassessment recorded for Aug-18 by the event cycle."""
    r = {"reassessment_available": True,
         "reassessment_state": "CHANGE_CANDIDATE",
         "decision": "CHANGE_CANDIDATE",
         "proposal_required": False,
         "reassessment_id": "prs_2026-08-18_alpha_paper_book_1_cc0e6cedf338",
         "reassessment_hash": "cc0e6cedf338",
         "reassessment_date": AUG18,
         "hoc_assessment_hash": LIVE_HOC_HASH,
         "eligible_market_date": AUG18,
         "active_book_id": "alpha_paper_book_1",
         "holdings_evaluated": 25, "attention_count": 1,
         "expected_net_improvement": -0.000853,
         "net_improvement_hurdle": 0.050,
         "expected_one_way_turnover": 0.019697,
         "turnover_budget": 0.35,
         "expected_transaction_cost_usd": 4.92,
         "blockers": [], "reason_codes": ["SECTOR_CAP_BREACH_BLOCKS_CHANGE"],
         "mandatory_exit_tickers": [],
         "mandatory_exit_policy": {"obligation": "NONE"}}
    r.update(kw)
    return r


#: The DRC status as the repaired owner reports it for this world: the cycle has NOT run.
DRC_NOT_STARTED = {"state": drc.NOT_STARTED, "run_id": None, "blockers": [],
                   "completed_steps": [], "executable": True,
                   "governed_research_evidence_current": False,
                   "governed_manifest_run_id": None,
                   "opportunity_cost_selected": True,
                   "opportunity_cost_artifact_class": hoc.ARTIFACT_CLASS_LIVE_PRE_DRC,
                   "opportunity_cost_producer_owner": hoc.PRODUCER_EVENT_SIGNAL_REFRESH,
                   "opportunity_cost_claims_drc_terminal": False,
                   "opportunity_cost_proves_drc_complete": False}

#: The DRC status after a REAL governed run for the same session.
DRC_COMPLETE = {"state": drc.COMPLETE, "run_id": "drc_2026-08-18_abc123def456",
                "blockers": [], "executable": False,
                "governed_research_evidence_current": True,
                "governed_manifest_run_id": "drc_2026-08-18_abc123def456",
                "opportunity_cost_selected": True,
                "opportunity_cost_artifact_class":
                    hoc.ARTIFACT_CLASS_GOVERNED_DRC_TERMINAL,
                "opportunity_cost_producer_owner": hoc.PRODUCER_DAILY_RESEARCH_CYCLE,
                "opportunity_cost_claims_drc_terminal": True,
                "opportunity_cost_proves_drc_complete": False}

#: The CORRUPTED state: the DRC owner adjudicated a claim it could not validate.
DRC_INCONSISTENT = {
    "state": drc.INCONSISTENT, "run_id": None, "executable": True,
    "governed_research_evidence_current": False,
    "blockers": [{"code": drc.TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST,
                  "claimed_drc_run_id": "drc_2026-08-18_orphaned000"}]}


def _load(**kw):
    """Compose the REAL workflow state with every read seam bound (no store, no probe)."""
    args = dict(
        now=NOW,
        operational=copy.deepcopy(_OPB), inputs=dict(_INPUTS),
        daily_status={"latest_valid_mark_date": AUG18},
        desk_marks=copy.deepcopy(_DESK),
        close_progress=dict(AUG18_CLOSE_PROGRESS),
        forward_status=copy.deepcopy(_FWD), gate=_gate(),
        target_readiness=copy.deepcopy(_TR),
        research_cycle=copy.deepcopy(DRC_NOT_STARTED),
        reassessment_summary=_reassessment(), decision_record={})
    args.update(kw)
    return ws.load_workflow_state(**args)


def _artifact(*, produced_by=None, drc_run_id=None, with_block=True):
    """An artifact shaped like the real one on disk."""
    art = {"artifact_id": "hoc_2026-08-18_alpha_paper_book_1_76b92f028a0a",
           "schema_version": hoc.SCHEMA_VERSION,
           "composition_owner": hoc.COMPOSITION_OWNER,
           "generated_at": "2026-08-18T22:07:50.469083+00:00",
           "identity": {"active_book_id": "alpha_paper_book_1",
                        "assessment_hash": LIVE_HOC_HASH,
                        "eligible_market_date": AUG18},
           "input_contract": {"eligible_market_date": AUG18},
           "assessment": {"assessment_state": "READY", "assessment_hash": LIVE_HOC_HASH,
                          "recommendation_counts": {"HOLD": 9}}}
    if with_block:
        art[hoc.PROVENANCE_KEY] = hoc.build_provenance(producer_owner=produced_by,
                                                       drc_run_id=drc_run_id)
    return art


# =========================================================================== #
# 1. THE PROVENANCE DISTINCTION HAS EXACTLY ONE OWNER
# =========================================================================== #
class TestProvenanceOwnership:

    def test_01_the_artifact_owner_publishes_the_vocabulary(self):
        assert hoc.PROVENANCE_OWNER == "api.holding_opportunity_cost"
        assert hoc.ARTIFACT_CLASS_LIVE_PRE_DRC == "LIVE_PRE_DRC_SIGNAL"
        assert hoc.ARTIFACT_CLASS_GOVERNED_DRC_TERMINAL == "GOVERNED_DRC_TERMINAL"
        assert set(hoc.ARTIFACT_CLASS_VOCABULARY) == {
            hoc.ARTIFACT_CLASS_LIVE_PRE_DRC, hoc.ARTIFACT_CLASS_GOVERNED_DRC_TERMINAL}
        assert callable(hoc.classify_artifact_provenance)
        assert callable(hoc.build_provenance)

    def test_02_the_classifier_is_PURE_it_opens_no_manifest(self):
        # THE STRUCTURAL GUARANTEE. The artifact owner states a CLAIM; only the manifest
        # owner may adjudicate it. There is no parameter through which a store arrives.
        params = list(inspect.signature(hoc.classify_artifact_provenance).parameters)
        assert params == ["artifact"]
        for forbidden in ("drc_dir", "manifest", "index", "run", "hoc_dir"):
            assert forbidden not in params

    def test_03_an_event_cycle_artifact_is_CLASS_1(self):
        p = hoc.classify_artifact_provenance(
            _artifact(produced_by=hoc.PRODUCER_EVENT_SIGNAL_REFRESH))
        assert p["artifact_class"] == hoc.ARTIFACT_CLASS_LIVE_PRE_DRC
        assert p["claims_drc_terminal"] is False
        assert p["producer_owner"] == hoc.PRODUCER_EVENT_SIGNAL_REFRESH
        assert p["drc_run_id"] is None

    def test_04_a_governed_cycle_artifact_is_CLASS_2(self):
        p = hoc.classify_artifact_provenance(
            _artifact(produced_by=hoc.PRODUCER_DAILY_RESEARCH_CYCLE,
                      drc_run_id="drc_2026-08-18_abc123def456"))
        assert p["artifact_class"] == hoc.ARTIFACT_CLASS_GOVERNED_DRC_TERMINAL
        assert p["claims_drc_terminal"] is True
        assert p["drc_run_id"] == "drc_2026-08-18_abc123def456"

    def test_05_a_LEGACY_artifact_with_no_block_is_CLASS_1(self):
        # The exact artifact sitting on disk right now. Absence of a claim is not a
        # broken claim — history is read, never rewritten, and never retro-accused.
        p = hoc.classify_artifact_provenance(_artifact(with_block=False))
        assert p["artifact_class"] == hoc.ARTIFACT_CLASS_LIVE_PRE_DRC
        assert p["claims_drc_terminal"] is False
        assert p["producer_owner"] == hoc.PRODUCER_UNRECORDED

    def test_06_no_artifact_EVER_proves_the_cycle_ran(self):
        for art in (_artifact(with_block=False),
                    _artifact(produced_by=hoc.PRODUCER_EVENT_SIGNAL_REFRESH),
                    _artifact(produced_by=hoc.PRODUCER_DAILY_RESEARCH_CYCLE,
                              drc_run_id="drc_x"),
                    None, {}, "not-a-dict"):
            assert hoc.classify_artifact_provenance(art)["proves_drc_complete"] is False

    def test_07_a_claim_needs_a_NAMED_run_to_be_adjudicable(self):
        # A bare boolean names nothing a manifest owner could look up, so it would be
        # permanently unresolvable rather than fail-closed. Only an ID makes a claim.
        art = _artifact(with_block=False)
        art[hoc.PROVENANCE_KEY] = {"claims_drc_terminal": True, "drc_run_id": None}
        p = hoc.classify_artifact_provenance(art)
        assert p["claims_drc_terminal"] is False
        assert p["artifact_class"] == hoc.ARTIFACT_CLASS_LIVE_PRE_DRC

    def test_08_the_provenance_block_cannot_change_identity_or_hash(self):
        # It lives at the TOP LEVEL, outside identity and assessment, so every existing
        # artifact stays byte-valid and no stamp can invalidate recorded evidence.
        assert hoc.PROVENANCE_KEY == "produced_by"
        a = _artifact(with_block=False)
        b = _artifact(produced_by=hoc.PRODUCER_DAILY_RESEARCH_CYCLE, drc_run_id="drc_x")
        assert a["identity"] == b["identity"]
        assert a["assessment"] == b["assessment"]
        assert hoc.PROVENANCE_KEY not in a["identity"]
        assert hoc.PROVENANCE_KEY not in a["assessment"]

    def test_09_both_canonical_producers_identify_themselves(self):
        drc_src = _src("api", "daily_research_cycle.py")
        esr_src = _src("api", "event_signal_refresh.py")
        assert "PRODUCER_DAILY_RESEARCH_CYCLE" in drc_src
        assert "drc_run_id=drc_run_id" in drc_src
        assert "PRODUCER_EVENT_SIGNAL_REFRESH" in esr_src
        # The event cycle must NEVER stamp a run id — it owns no manifest. Asserted on
        # the CALL, not on prose: the docstring explains precisely why it does not.
        assert "drc_run_id=" not in esr_src

    def test_10_the_event_cycle_still_uses_the_SAME_canonical_owner(self):
        # The fix must not fork a calculation: both modes still call one owner.
        assert (esr.CANONICAL_CALCULATION_DELEGATES[
            esr.ek.CALC_HOLDING_OPPORTUNITY_COST] == "api.holding_opportunity_cost")

    def test_11_persist_records_the_producer(self):
        assert "produced_by" in inspect.signature(hoc.persist_assessment).parameters
        assert "drc_run_id" in inspect.signature(hoc.persist_assessment).parameters
        assert "produced_by" in inspect.signature(hoc.run_and_persist).parameters


# =========================================================================== #
# 2. THE AUG-18 PRE-DRC FIXTURE — THE LIVE DEADLOCK
# =========================================================================== #
class TestAug18PreDrcIsDueNotRecovery:

    def test_20_the_close_really_did_complete(self):
        assert dc.is_operational_close_complete(AUG18_CLOSE_PROGRESS) is True
        d = _load()
        assert d["operational_state"]["latest_completed_close_date"] == AUG18
        assert d["operational_state"]["operational_close_valid"] is True

    def test_21_the_session_is_ready_and_eligible_is_AUG18(self):
        d = _load()
        assert d["current_session"]["latest_eligible_completed_market_date"] == AUG18
        assert d["current_session"]["session_status"] == msession.SESSION_READY

    def test_22_the_overall_state_is_NOT_inconsistent(self):
        d = _load()
        assert d["overall_state"] != ws.INCONSISTENT_STATE
        assert d["overall_state"] == ws.RESEARCH_CYCLE_REQUIRED

    def test_23_the_cycle_is_NOT_in_recovery(self):
        d = _load()
        cyc = d["normal_cycle"]
        assert cyc["current_stage"] == nc.STAGE_DAILY_RESEARCH_CYCLE
        assert cyc["in_recovery"] is False

    def test_24_the_stage_ladder_reads_exactly_as_required(self):
        stages = {s["stage"]: s["status"] for s in _load()["normal_cycle"]["stages"]}
        assert stages[nc.STAGE_DAILY_CLOSE] == nc.ST_DONE
        assert stages[nc.STAGE_DAILY_RESEARCH_CYCLE] == nc.ST_CURRENT
        assert stages[nc.STAGE_PORTFOLIO_DECISION] == nc.ST_UPCOMING
        assert stages[nc.STAGE_CONTROLLED_REBALANCE] == nc.ST_UPCOMING

    def test_25_exactly_one_executable_normal_mutation(self):
        cyc = _load()["normal_cycle"]
        assert cyc["executable_stage_count"] == 1
        assert cyc["executable_stages"] == [nc.STAGE_DAILY_RESEARCH_CYCLE]

    def test_26_the_DRC_gate_is_the_one_that_opens(self):
        gates = _load()["normal_cycle"]["stage_gates"]
        assert gates[nc.STAGE_DAILY_RESEARCH_CYCLE]["execution_allowed"] is True
        for stage in (nc.STAGE_DAILY_CLOSE, nc.STAGE_PORTFOLIO_DECISION,
                      nc.STAGE_CONTROLLED_REBALANCE, nc.STAGE_WAIT_FOR_SESSION_CLOSE):
            assert gates[stage]["execution_allowed"] is False, stage

    def test_27_the_primary_action_is_RUN_DAILY_RESEARCH_CYCLE(self):
        p = _load()["primary_action"]
        assert p["action_code"] == ws.ACTION_RUN_RESEARCH_CYCLE
        assert p["execution_available"] is True
        assert p["confirmation_required"] == "RUN_DAILY_RESEARCH_CYCLE"
        assert p["manual_confirmation_required"] is True

    def test_27b_the_reason_given_matches_what_is_on_screen(self):
        # The pre-29.5 wording said "no Holding Opportunity-Cost assessment has been
        # produced for that session yet" — while the live assessment's counts were
        # rendered directly beside it. Naming a cause the operator can see is false
        # sends them hunting for something that is not missing.
        p = _load()["primary_action"]
        assert "no Holding Opportunity-Cost assessment has been produced" \
            not in p["explanation"]
        assert "continuous information collection" in p["explanation"]
        assert "governed" in p["explanation"]

    def test_27bb_the_task_line_names_a_reason_the_operator_can_act_on(self):
        # "Refresh the stale inputs" for a session whose inputs are all current sends
        # the operator looking for something that does not exist.
        # `current_task` / `headline` reach the operator through the cycle view and the
        # operator-command presentation, not through primary_action itself.
        d = _load()
        task = d["normal_cycle"]["current_task"]
        headline = d["today_hero"]["headline"]
        assert "stale" not in task
        assert "stale" not in headline
        assert "Daily Close complete" in headline
        assert "governed" in task

    def test_27c_the_ordinary_post_close_wording_is_unchanged(self):
        # With NO live artifact the original sentence is still the accurate one.
        d = _load(gate=_gate(opportunity_cost_available=False,
                             opportunity_cost_state="NOT_RUN"))
        p = d["primary_action"]
        assert p["action_code"] == ws.ACTION_RUN_RESEARCH_CYCLE
        assert "no Holding Opportunity-Cost assessment has been produced" \
            in p["explanation"]

    def test_28_no_daily_close_is_offered_again(self):
        d = _load()
        assert d["daily_close_gate"]["execution_allowed"] is False
        assert d["primary_action"]["action_code"] != ws.ACTION_RUN_DAILY_CLOSE

    def test_29_the_live_HOC_stays_VISIBLE(self):
        # It is real information. Suppressing it would be as wrong as trusting it.
        rc = _load()["research_cycle_state"]
        assert rc["live_pre_drc_signal_present"] is True
        assert rc["opportunity_cost_artifact_class"] == hoc.ARTIFACT_CLASS_LIVE_PRE_DRC
        assert rc["opportunity_cost_producer_owner"] == hoc.PRODUCER_EVENT_SIGNAL_REFRESH

    def test_30_the_DRC_is_NOT_falsely_complete(self):
        rc = _load()["research_cycle_state"]
        assert rc["cycle_complete"] is False
        assert rc["governed_research_evidence_current"] is False
        assert rc["governed_manifest_run_id"] is None
        assert rc["opportunity_cost_proves_drc_complete"] is False

    def test_31_the_portfolio_decision_is_NOT_falsely_governed(self):
        cd = _load()["canonical_portfolio_decision"]
        # The verdict itself is still the truthful reading of the live reassessment...
        assert cd["state"] == ws.CPD_CHANGE_WITHHELD
        # ...but it is labelled as live signal, not the governed daily-cycle decision.
        assert cd["decision_provenance"] == ws.DECISION_PROVENANCE_LIVE_PRE_DRC
        assert cd["is_governed_daily_cycle_decision"] is False
        assert cd["provenance_label"]

    def test_32_no_portfolio_mutation_is_available_pre_cycle(self):
        cd = _load()["canonical_portfolio_decision"]
        assert cd["operator_action_available"] is False
        assert cd["approvable"] is False
        assert cd["creates_orders"] is False
        assert cd["proposal_state"] == "REALLOCATION_PROPOSAL_NOT_RUN"

    def test_33_the_state_is_CONSISTENT(self):
        d = _load()
        assert d["consistency_status"] == ws.CONSISTENT
        assert d["consistency_violations"] == []


# =========================================================================== #
# 3. THE CORRUPTED TERMINAL FIXTURE STILL FAILS CLOSED
# =========================================================================== #
class TestFalselyTerminalArtifactStillRecovers:

    def test_40_the_probe_claim_drives_the_verdict(self, tmp_path):
        from tests.test_slice3_daily_research_cycle import _inputs, _status
        claim = lambda **kw: {"present": True, "assessment_hash": "H",  # noqa: E731
                              "recommendation_counts": {}, "data_gaps": [],
                              "claims_drc_terminal": True,
                              "drc_run_id": "drc_2026-08-03_orphaned00"}
        s = _status(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"),
                    downstream_artifacts_fn=claim)
        assert s["state"] == drc.INCONSISTENT
        assert any(b["code"] == drc.TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST
                   for b in s["blockers"])

    def test_41_the_reason_code_is_NOT_deleted(self):
        # The fail-closed contract survives the repair: only its TRIGGER narrowed.
        assert drc.TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST in drc.__all__
        assert "TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST" in _src(
            "api", "daily_research_cycle.py")

    def test_42_a_broken_claim_suspends_the_cycle(self):
        d = _load(research_cycle=copy.deepcopy(DRC_INCONSISTENT))
        assert d["overall_state"] == ws.INCONSISTENT_STATE
        assert d["normal_cycle"]["current_stage"] == nc.STAGE_RECOVERY
        assert d["normal_cycle"]["in_recovery"] is True

    def test_43_a_broken_claim_offers_NO_normal_mutation(self):
        cyc = _load(research_cycle=copy.deepcopy(DRC_INCONSISTENT))["normal_cycle"]
        assert cyc["executable_stage_count"] == 0
        for g in cyc["stage_gates"].values():
            assert g["execution_allowed"] is False

    def test_44_the_completed_close_survives_the_suspension(self):
        # Stage 22.1 — RECOVERY suspends the cycle; it never un-records finished work.
        d = _load(research_cycle=copy.deepcopy(DRC_INCONSISTENT))
        assert d["operational_state"]["operational_close_valid"] is True
        stages = {s["stage"]: s["status"] for s in d["normal_cycle"]["stages"]}
        assert stages[nc.STAGE_DAILY_CLOSE] == nc.ST_DONE


# =========================================================================== #
# 4. AFTER A SUCCESSFUL GOVERNED RUN
# =========================================================================== #
class TestAfterASuccessfulGovernedRun:

    def test_50_the_cycle_advances_to_the_portfolio_decision(self):
        d = _load(research_cycle=copy.deepcopy(DRC_COMPLETE))
        assert d["overall_state"] != ws.RESEARCH_CYCLE_REQUIRED
        stages = {s["stage"]: s["status"] for s in d["normal_cycle"]["stages"]}
        assert stages[nc.STAGE_DAILY_RESEARCH_CYCLE] == nc.ST_DONE
        assert d["normal_cycle"]["current_stage"] == nc.STAGE_PORTFOLIO_DECISION

    def test_51_governed_evidence_is_now_current(self):
        rc = _load(research_cycle=copy.deepcopy(DRC_COMPLETE))["research_cycle_state"]
        assert rc["governed_research_evidence_current"] is True
        assert rc["governed_manifest_run_id"] == "drc_2026-08-18_abc123def456"
        assert rc["live_pre_drc_signal_present"] is False

    def test_52_the_decision_becomes_the_GOVERNED_one(self):
        cd = _load(research_cycle=copy.deepcopy(DRC_COMPLETE))["canonical_portfolio_decision"]
        assert cd["decision_provenance"] == ws.DECISION_PROVENANCE_GOVERNED
        assert cd["is_governed_daily_cycle_decision"] is True
        assert cd["provenance_label"] is None

    def test_53_a_withheld_governed_decision_offers_NO_rebalance(self):
        d = _load(research_cycle=copy.deepcopy(DRC_COMPLETE))
        cd = d["canonical_portfolio_decision"]
        assert cd["state"] == ws.CPD_CHANGE_WITHHELD
        assert cd["operator_action_available"] is False
        gates = d["normal_cycle"]["stage_gates"]
        assert gates[nc.STAGE_CONTROLLED_REBALANCE]["execution_allowed"] is False
        assert d["normal_cycle"]["executable_stage_count"] == 0

    def test_54_a_review_required_decision_reviews_without_creating_an_order(self):
        d = _load(research_cycle=copy.deepcopy(DRC_COMPLETE),
                  reassessment_summary=_reassessment(
                      reassessment_state="PROPOSAL_READY", decision="PROPOSAL_READY",
                      proposal_required=True))
        gates = d["normal_cycle"]["stage_gates"]
        # A review is never a mutation, so the single-mutation contract still holds.
        assert d["normal_cycle"]["executable_stage_count"] == 0
        assert gates[nc.STAGE_CONTROLLED_REBALANCE]["execution_allowed"] is False
        for g in gates.values():
            assert g["creates_orders"] is False
        assert d["canonical_portfolio_decision"]["creates_orders"] is False

    def test_55_no_stage_ever_creates_orders_or_automates(self):
        for rc in (DRC_NOT_STARTED, DRC_COMPLETE, DRC_INCONSISTENT):
            cyc = _load(research_cycle=copy.deepcopy(rc))["normal_cycle"]
            assert cyc["creates_orders"] is False
            assert cyc["automatic_execution"] is False


# =========================================================================== #
# 5. REPLAY / IDEMPOTENCY — A REPLAY DUPLICATES NOTHING
# =========================================================================== #
class TestReplayIdempotency:

    def test_60_a_replayed_run_reuses_the_manifest(self, tmp_path):
        from tests.test_slice3_daily_research_cycle import _inputs, _run
        r1, _ = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
        r2, _ = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
        assert r1["run_id"] == r2["run_id"]
        assert r2["reused_existing_run"] is True
        runs = list((tmp_path / "runs").glob("drc_*.json"))
        assert len(runs) == 1

    def test_61_a_replay_creates_no_duplicate_downstream_artifact(self, tmp_path):
        from tests.test_slice3_daily_research_cycle import _inputs, _run
        r1, _ = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
        r2, _ = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
        assert (r1.get("opportunity_cost_assessment_hash")
                == r2.get("opportunity_cost_assessment_hash"))
        assert (r1.get("portfolio_reassessment_hash")
                == r2.get("portfolio_reassessment_hash"))
        assert (r1.get("reallocation_proposal_hash")
                == r2.get("reallocation_proposal_hash"))
        assert r1.get("snapshot_bundle_id") == r2.get("snapshot_bundle_id")

    def test_62_the_artifact_owner_reuses_rather_than_rewrites(self, tmp_path):
        # An identical identity REUSES. The immutable artifact is never overwritten, so
        # a replay cannot acquire, change or lose a provenance claim.
        ic = {"active_book_id": "b1", "eligible_market_date": AUG18,
              "portfolio_state_hash": "P", "universe_scoring_hash": "U",
              "decision_policy_version": hoc.DECISION_POLICY_VERSION,
              "corporate_actions_hash": "C"}
        res = {"assessment_state": "READY", "assessment_hash": "H",
               "recommendation_counts": {"HOLD": 1}, "holding_reviews": [{"ticker": "A"}]}
        a = hoc.persist_assessment(result=res, input_contract=ic, hoc_dir=str(tmp_path),
                                   produced_by=hoc.PRODUCER_EVENT_SIGNAL_REFRESH)
        b = hoc.persist_assessment(result=res, input_contract=ic, hoc_dir=str(tmp_path),
                                   produced_by=hoc.PRODUCER_DAILY_RESEARCH_CYCLE,
                                   drc_run_id="drc_late_claim")
        assert a["status"] == "CREATED" and b["status"] == "REUSED_EXISTING"
        assert len(list((tmp_path / "artifacts").glob("*.json"))) == 1
        art = hoc.load_latest_artifact(active_book_id="b1",
                                       eligible_market_date=AUG18, hoc_dir=str(tmp_path))
        # The FIRST producer's statement stands. Adoption is proven by the adopter's
        # manifest, never by retro-stamping evidence it did not produce.
        p = hoc.classify_artifact_provenance(art)
        assert p["producer_owner"] == hoc.PRODUCER_EVENT_SIGNAL_REFRESH
        assert p["claims_drc_terminal"] is False

    def test_63_a_replayed_workflow_read_is_stable(self):
        a, b = _load(), _load()
        for key in ("overall_state", "consistency_status"):
            assert a[key] == b[key]
        assert (a["normal_cycle"]["executable_stages"]
                == b["normal_cycle"]["executable_stages"])
        assert (a["canonical_portfolio_decision"]["state"]
                == b["canonical_portfolio_decision"]["state"])


# =========================================================================== #
# 6. LANE / OWNERSHIP INVARIANTS
# =========================================================================== #
class TestOwnershipInvariants:

    def test_70_workflow_state_invents_NO_provenance(self):
        # It reads the classification through the shared gate path; it never derives one.
        src = _src("api", "workflow_state.py")
        assert "classify_artifact_provenance" not in src
        assert "LIVE_PRE_DRC_SIGNAL" not in src.split("DECISION_PROVENANCE_LIVE_PRE_DRC")[0]

    def test_71_the_manifest_has_exactly_one_owner(self):
        for mod in ("api/holding_opportunity_cost.py", "api/portfolio_reassessment.py",
                    "api/reallocation_proposal.py", "api/workflow_state.py",
                    "api/event_signal_refresh.py"):
            src = _src(*mod.split("/"))
            assert "_save_run(" not in src, mod
            assert "TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST" not in src, mod

    def test_72_no_second_state_machine_was_introduced(self):
        src = _src("engine", "normal_cycle.py")
        assert set(nc.STAGE_SEQUENCE) == {
            nc.STAGE_WAIT_FOR_SESSION_CLOSE, nc.STAGE_DAILY_CLOSE,
            nc.STAGE_DAILY_RESEARCH_CYCLE, nc.STAGE_PORTFOLIO_DECISION,
            nc.STAGE_CONTROLLED_REBALANCE}
        # The kernel stays a PURE projection: no store, no clock, no api import.
        assert "import" in src and "from paper_trader.api" not in src

    def test_73_the_event_cycle_creates_no_orders_and_no_manifest(self):
        src = _src("api", "event_signal_refresh.py")
        assert "created_orders" not in src or "created_orders=True" not in src
        assert "drc_" not in src.replace("drc_run_id", "")

    def test_74_close_validity_is_still_independent_of_all_of_this(self):
        # Release 29.4's guarantee must survive Release 29.5 untouched.
        params = list(inspect.signature(dc.is_operational_close_complete).parameters)
        assert params == ["progress"]
        assert dc.is_operational_close_complete(AUG18_CLOSE_PROGRESS) is True


# =========================================================================== #
# 7. UI — NO SURFACE INFERS CYCLE COMPLETION FROM AN ARTIFACT
# =========================================================================== #
class TestUiStatesProvenance:

    def test_80_the_verdict_renders_the_backend_label_verbatim(self):
        ui = _ui()
        assert "cd.provenance_label" in ui
        assert "cd.provenance_detail" in ui
        assert "r29-verdict-prov" in ui

    def test_81_the_UI_derives_no_provenance_of_its_own(self):
        ui = _ui()
        # No client-side reconstruction of the distinction the backend owns.
        assert "LIVE_PRE_DRC_SIGNAL" not in ui
        assert "GOVERNED_DRC_TERMINAL" not in ui
        assert "classify_artifact_provenance" not in ui

    def test_82_the_provenance_chip_carries_no_action(self):
        ui = _ui()
        block = ui.split("r29-verdict-prov")[2]
        head = block[:400]
        for forbidden in ("onclick", "navigateToRoute", "execute", "confirm("):
            assert forbidden not in head, forbidden

    def test_83_no_dialogs_and_no_order_creation_were_added(self):
        ui = _ui()
        # The app NEUTRALISES the native dialogs rather than merely avoiding them: the
        # override returns false so a legacy synchronous call can never auto-proceed.
        assert "window.confirm = function" in ui
        assert "native confirm dialog is disabled" in ui
        # Release 29.5 adds a LABEL, not a control: the chip introduces no dialog, no
        # order path and no execution affordance of any kind. (The legacy advanced-actions
        # list that mentions order creation predates this release and is untouched here;
        # the canonical decision and every stage gate still report creates_orders False,
        # which tests 54/55 assert on the backend contract rather than on UI strings.)
        chip = ui.split("r29-verdict-prov")[2][:400]
        for forbidden in ("alert(", "confirm(", "createOrders", "fetch(", "POST"):
            assert forbidden not in chip, forbidden
