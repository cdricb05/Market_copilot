r"""RELEASE 55.2.1 — RUNTIME ALIGNMENT RECONCILIATION, LEGACY GOVERNED-DECISION
CONTINUITY AND THE WINDOWS PROCESS-TOPOLOGY TRUTH MODEL.

THREE DEFECTS, THREE ROOT CAUSES, ONE THEME
-------------------------------------------
Each of these is a case of a reader treating "I cannot see it" as "it is not
there", and each is repaired at the OWNER of the fact rather than at the surface
that displayed it.

1. RUNTIME ALIGNMENT SAID UNKNOWN WHILE THE COLLECTION OWNER SAID ALIGNED.
   R55.2 published the worker's captured release only on the FULL collection
   payload. The Active Manager reads the canonical LIFECYCLE view instead (via
   ``api.operator_presentation.owner_loaders`` -> ``api.decision_snapshot``), and
   ``resolve_service_lifecycle`` did not carry the identity forward — so the
   composition received ``loaded=None`` and fell to UNKNOWN for a worker that
   had proven itself aligned. The repair puts the worker's own identity facts on
   the lifecycle verdict, where ``worker_pid`` already lived. No second
   alignment calculation exists: the verdict is still ``api.runtime_identity``'s.

2. THE SEP-2 GOVERNED DECISION DISAPPEARED.
   NOT an R55.2 regression — R55.2 touched neither ``api.daily_research_cycle``
   nor the projection. R46.2 repaired ONE of the two ways the clock erases a
   finished run (WAITING_FOR_SESSION_CLOSE) and left the other open. When the
   Sep-3 session closed without publishing owned data, ``_pre_run_state`` moved
   from WAITING_FOR_SESSION_CLOSE to WAITING_FOR_OWNED_DATA, the reflection
   branch stopped matching, the COMPLETE Sep-2 manifest vanished from the status,
   ``governed_research_evidence_current`` went false, and the Release-29.5
   compatibility projection of the Sep-2 governed decision went dark with it.
   A wait for a LATER session's data says nothing about the eligible session's
   finished research.

3. THE MANAGER REPORTED NO_LOGICAL_WORKER FOR A HEALTHY WORKER.
   The collector matched processes by command line and silently dropped every row
   whose command line this shell may not read — which, for a Task-Scheduler-owned
   worker, is all of them. The snapshot arrived EMPTY and the verdict was "No
   process on this machine is running the collection worker" while pid 1976 was
   alive, heartbeating and holding the singleton lock. That is not cosmetic:
   ``resolve_abandoned_lock`` treats that verdict as PROOF the machine is empty
   and would have authorised clearing a live worker's lock.

Every test here is hermetic: tmp_path stores, injected freshness, injected
identities, synthetic process snapshots. Nothing reads the production collection
root, starts/stops/restarts a process, runs a close or a cycle, creates an order
or a fill, promotes a model or activates a sleeve.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from paper_trader.api import active_manager_state as ams
from paper_trader.api import daily_research_cycle as drc
from paper_trader.api import information_collection as ic
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import runtime_identity as rid
from paper_trader.engine import market_session as msession

REPO = Path(__file__).resolve().parents[1]

REV_A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
REV_B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

BOOK = "alpha_paper_book_1"
SESSION = "2026-09-02"
NEXT_SESSION = "2026-09-03"
RUN_ID = "drc_2026-09-02_15abfb01856f"
HOC_AID = "hoc_2026-09-02_alpha_paper_book_1_a162fca9"

T_WORKER_START = datetime(2026, 9, 4, 0, 19, 37, tzinfo=timezone.utc)
T_NOW = datetime(2026, 9, 4, 0, 32, 42, tzinfo=timezone.utc)
STAMP_LATE = "2026-09-02T23:51:52+00:00"
STAMP_REASSESS = "2026-09-03T20:56:44.616804Z"

IC_SRC = Path(ic.__file__).read_text(encoding="utf-8")
DRC_SRC = Path(drc.__file__).read_text(encoding="utf-8")
AMS_SRC = Path(ams.__file__).read_text(encoding="utf-8")
MANAGE_PS1 = (REPO / "scripts" / "manage_information_collection.ps1").read_text(
    encoding="utf-8")
CONTROL_SRC = (REPO / "scripts" / "collection_service_control.py").read_text(
    encoding="utf-8")


# =========================================================================== #
# Fixtures
# =========================================================================== #
def _identity(commit, *, captured_at=T_WORKER_START, pid=1976):
    """A frozen loaded-identity capture, shaped exactly as the owner emits it."""
    return {
        "identity_kind": "LOADED_RUNTIME_IDENTITY", "owner": rid.OWNER,
        "schema_version": rid.SCHEMA_VERSION, "contract_id": rid.CONTRACT_ID,
        "repo_root": str(REPO), "commit": commit,
        "commit_short": (commit[:12] if commit else None),
        "branch": "stage19-controlled-rebalance", "dirty_at_capture": False,
        "resolved_from": rid.RESOLVED_FROM_GIT_DIR,
        "captured_at": captured_at.isoformat(), "pid": pid,
        "captured_once_per_process": True, "changes_when_source_changes": False,
    }


def _source(commit):
    return {
        "identity_kind": "SOURCE_REPOSITORY_IDENTITY", "owner": rid.OWNER,
        "repo_root": str(REPO), "commit": commit,
        "commit_short": (commit[:12] if commit else None),
        "branch": "stage19-controlled-rebalance", "dirty": False,
        "resolved_from": rid.RESOLVED_FROM_GIT_DIR, "read_at": T_NOW.isoformat(),
        "is_a_loaded_runtime_identity": False,
    }


def _service_state(*, commit=REV_B, pid=1976, instance="inst-1",
                   heartbeat=T_NOW, started=T_WORKER_START):
    """The persisted service state a live worker keeps."""
    st = ic._blank_service_state()
    st.update({
        "instance_id": instance, "pid": pid, "host": "test-host",
        "started_at": started.isoformat(),
        "heartbeat_at": (heartbeat.isoformat() if heartbeat else None),
        "progress_at": (heartbeat.isoformat() if heartbeat else None),
        "progress_seq": 181, "progress_step": "ITERATION_END",
        "iteration_in_flight": False,
        "loaded_release": (_identity(commit, pid=pid) if commit else None),
        "collection_automation_enabled": True,
    })
    return st


def _lock(*, pid=1976, instance="inst-1"):
    return {"pid": pid, "instance_id": instance, "host": "test-host",
            "acquired_at": T_WORKER_START.isoformat(),
            "heartbeat_at": T_NOW.isoformat()}


def _blind_snapshot():
    """What the operator's unelevated shell actually collected on 2026-09-03:
    six python.exe rows, four of them with an unreadable command line, and NOT
    ONE match — because the worker is among the four it may not read."""
    return {"rows": [],
            "introspection": {"scanned_count": 6, "matched_count": 0,
                              "unreadable_command_line_count": 4,
                              "query_failed": False}}


def _readable_empty_snapshot():
    """A COMPLETE snapshot that genuinely found no worker."""
    return {"rows": [],
            "introspection": {"scanned_count": 3, "matched_count": 0,
                              "unreadable_command_line_count": 0,
                              "query_failed": False}}


def _lineage_snapshot(root_pid=61108, leaf_pid=1976):
    """The venv redirector + the base interpreter it launched: ONE worker."""
    cmd = r'"C:\repo\.venv-win\Scripts\python.exe" scripts\run_information_collection_service.py'
    return {"rows": [
        {"pid": root_pid, "parent_pid": 2056, "command_line": cmd,
         "executable_path": r"C:\repo\.venv-win\Scripts\python.exe",
         "created_at": "2026-09-04T00:19:36.000Z"},
        {"pid": leaf_pid, "parent_pid": root_pid, "command_line": cmd,
         "executable_path": r"C:\Python313\python.exe",
         "created_at": "2026-09-04T00:19:37.000Z"}],
        "introspection": {"scanned_count": 6, "matched_count": 2,
                          "unreadable_command_line_count": 0,
                          "query_failed": False}}


def _two_lineage_snapshot():
    cmd = r'"C:\repo\.venv-win\Scripts\python.exe" scripts\run_information_collection_service.py'
    rows = _lineage_snapshot()["rows"] + [
        {"pid": 4242, "parent_pid": 9, "command_line": cmd,
         "executable_path": r"C:\Python313\python.exe",
         "created_at": "2026-09-04T00:20:00.000Z"}]
    return {"rows": rows,
            "introspection": {"scanned_count": 7, "matched_count": 3,
                              "unreadable_command_line_count": 0,
                              "query_failed": False}}


def _presence(snapshot, *, state=None, lock=None, alive=True):
    topo = ic.resolve_worker_topology(snapshot, lock=lock)
    return ic.resolve_worker_presence(
        topology=topo, state=state, lock=lock, now=T_NOW,
        pid_alive=lambda p: alive)


# --- DRC / decision fixtures ------------------------------------------------ #
def _freshness(*, session_status, eligible=SESSION, expected=NEXT_SESSION,
               owned_confirmed=SESSION, consistency="CONSISTENT"):
    return {
        "eligible_market_date": eligible,
        "expected_completed_market_date": expected,
        "consistency_status": consistency, "consistency_violations": [],
        "active_book": {"active_book_id": BOOK, "active_book_name": "Book #1"},
        "market_session": {
            "session_status": session_status,
            "latest_confirmed_owned_data_date": owned_confirmed,
            "operator_action": ("Wait for owned market data to publish the %s "
                                "session." % expected)},
        "source_freshness": [
            {"source_id": "owned_daily_prices", "as_of_date": eligible},
            {"source_id": "desk_marks", "as_of_date": eligible}],
        "warnings": [],
    }


def _manifest(**kw):
    d = {"run_id": RUN_ID, "state": "COMPLETE", "active_book_id": BOOK,
         "eligible_market_date": SESSION, "completed_at": STAMP_LATE,
         "session_contract_hash": "SCH1", "input_contract_hash": "ICH1",
         "portfolio_reassessment_id": "ra_1", "portfolio_reassessment_hash": "RA1",
         "portfolio_reassessment_state": "CURRENT_NO_CHANGE",
         "opportunity_cost_artifact_id": HOC_AID,
         "opportunity_cost_assessment_hash": "HOC1",
         # Written by the cycle's own contract at persist time and reflected
         # verbatim — this is the Release-29.5 fact the projection gate reads.
         "governed_evidence_owner": "api.daily_research_cycle",
         "governed_manifest_run_id": RUN_ID,
         "governed_research_evidence_current": True,
         "warnings": [], "required_actions": []}
    d.update(kw)
    return d


def _seed_drc(tmp_path, manifest=None):
    """Write ONE completed manifest into a hermetic DRC store."""
    man = manifest if manifest is not None else _manifest()
    runs = tmp_path / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    (runs / ("%s.json" % man["run_id"])).write_text(
        json.dumps(man), encoding="utf-8")
    (tmp_path / "index.json").write_text(json.dumps(
        {man["eligible_market_date"]: {"run_id": man["run_id"],
                                       "state": man["state"],
                                       "idempotency_key": "IK1",
                                       "input_contract_hash": "ICH1"}}),
        encoding="utf-8")
    return man


def _status(tmp_path, *, session_status, **kw):
    return drc.load_daily_research_cycle_status(
        drc_dir=tmp_path, freshness=_freshness(session_status=session_status, **kw),
        monthly_emitter_available=True)


def _workflow(*, governed_current, run_id=RUN_ID):
    return {"research_cycle_state": {
        "governed_research_evidence_current": bool(governed_current),
        "governed_manifest_run_id": (run_id if governed_current else None),
        "eligible_market_date": SESSION}}


def _reassessment(state="CURRENT_NO_CHANGE"):
    return {"state": state, "eligible_market_date": SESSION,
            "reassessment_hash": "RA1",
            "active_book": {"book_id": BOOK},
            "artifact": {"reassessment_id": "ra_1",
                         "generated_at": STAMP_REASSESS,
                         "identity": {"hoc_assessment_hash": "HOC1",
                                      "economic_state_hash": "ESH1"}}}


def _read_decision(tmp_path, *, governed_current=True, reassessment=None,
                   summary=None, constrained=None):
    return pdec.load_governed_portfolio_decision(
        workflow=_workflow(governed_current=governed_current),
        reassessment=(reassessment if reassessment is not None
                      else _reassessment()),
        proposal_summary=summary, constrained=constrained,
        active_book_id=BOOK, decision_dir=tmp_path)


# =========================================================================== #
# 1-7. RUNTIME ALIGNMENT
# =========================================================================== #
class TestRuntimeAlignmentReconciliation:

    def _row(self, loaded, source=REV_B):
        composed = rid.build_runtime_alignment(
            source=_source(source),
            runtimes=[{"runtime": rid.RUNTIME_COLLECTION, "loaded": loaded}])
        return composed, composed["runtimes"][0]

    def test_01_collection_loaded_a_source_a_is_aligned(self):
        composed, row = self._row(_identity(REV_A), source=REV_A)
        assert row["verdict"] == rid.ALIGNMENT_ALIGNED
        assert row["reason"] == rid.REASON_SAME_COMMIT
        assert composed["verdict"] == rid.ALIGNMENT_ALIGNED
        assert composed["proven"] is True

    def test_02_collection_loaded_a_source_b_is_stale(self):
        composed, row = self._row(_identity(REV_A), source=REV_B)
        assert row["verdict"] == rid.ALIGNMENT_STALE
        assert row["reason"] == rid.REASON_DIFFERENT_COMMIT
        assert composed["verdict"] == rid.ALIGNMENT_STALE

    def test_03_collection_loaded_unknown_is_unknown_never_aligned(self):
        composed, row = self._row(None)
        assert row["verdict"] == rid.ALIGNMENT_UNKNOWN
        assert row["reason"] == rid.REASON_LOADED_UNKNOWN
        assert composed["proven"] is False

    def test_04_the_lifecycle_owner_carries_the_workers_identity(self):
        """THE DEFECT-1 REPAIR. The Active Manager reads this view, so the fact
        has to be ON it — otherwise a proven-aligned worker reports UNKNOWN."""
        lc = ic.resolve_service_lifecycle(_service_state(commit=REV_B),
                                          _lock(), T_NOW)
        assert lc["service_state"] == ic.SVC_RUNNING
        assert (lc["loaded_release"] or {})["commit"] == REV_B
        assert lc["instance_id"] == "inst-1"
        assert lc["started_at"] == T_WORKER_START.isoformat()
        assert lc["loaded_release_owner"] == "api.runtime_identity"

    def test_05_active_manager_consumes_the_canonical_collection_identity(self):
        lc = ic.resolve_service_lifecycle(_service_state(commit=REV_B),
                                          _lock(), T_NOW)
        block = ams._runtime_alignment_block({}, {"service": lc})
        row = [r for r in block["runtimes"]
               if r["runtime"] == rid.RUNTIME_COLLECTION][0]
        assert row["loaded_commit"] == REV_B
        assert row["verdict"] in rid.ALIGNMENT_VERDICTS
        assert row["owner"] == "api.runtime_identity"
        assert row["process"]["instance_id"] == "inst-1"

    def test_06_active_manager_cannot_disagree_with_the_owner(self):
        """Identical evidence, two callers, one verdict. There is exactly ONE
        alignment calculation and both surfaces call it."""
        lc = ic.resolve_service_lifecycle(_service_state(commit=REV_A),
                                          _lock(), T_NOW)
        direct = rid.build_runtime_alignment(
            source=_source(REV_A),
            runtimes=[{"runtime": rid.RUNTIME_COLLECTION,
                       "loaded": lc["loaded_release"]}])["runtimes"][0]
        via_ams = [r for r in ams._runtime_alignment_block(
            {}, {"service": lc},
            rid.build_runtime_alignment(
                source=_source(REV_A),
                runtimes=[{"runtime": rid.RUNTIME_COLLECTION,
                           "loaded": lc["loaded_release"]}]))["runtimes"]
            if r["runtime"] == rid.RUNTIME_COLLECTION][0]
        assert direct["verdict"] == via_ams["verdict"]
        assert direct["reason"] == via_ams["reason"]

    def test_07_heartbeat_alone_never_proves_aligned(self):
        """A perfectly healthy worker that recorded NO identity stays UNKNOWN."""
        st = _service_state(commit=None)
        lc = ic.resolve_service_lifecycle(st, _lock(), T_NOW)
        assert lc["service_state"] == ic.SVC_RUNNING       # alive and healthy
        assert lc["heartbeat_age_seconds"] is not None
        row = ams._runtime_alignment_block({}, {"service": lc})["runtimes"]
        collection = [r for r in row if r["runtime"] == rid.RUNTIME_COLLECTION][0]
        assert collection["verdict"] == rid.ALIGNMENT_UNKNOWN

    def test_08_git_head_alone_is_not_a_loaded_identity(self):
        src = rid.read_source_identity(repo_root=REPO)
        assert src["is_a_loaded_runtime_identity"] is False
        # The source read may not masquerade as a loaded identity anywhere.
        assert "rev-parse" not in AMS_SRC
        assert "read_source_identity(" not in AMS_SRC


# =========================================================================== #
# 9-17. LEGACY GOVERNED-DECISION CONTINUITY
# =========================================================================== #
class TestLegacyGovernedDecisionContinuity:

    def test_09_a_completed_run_survives_a_wait_for_a_LATER_sessions_data(
            self, tmp_path):
        """THE DEFECT-2 REPAIR, reproduced exactly: eligible session Sep-2 is
        COMPLETE and owned-confirmed; Sep-3 has closed without publishing."""
        _seed_drc(tmp_path)
        st = _status(tmp_path, session_status=msession.WAITING_FOR_OWNED_DATA)
        assert st["state"] == drc.COMPLETE
        assert st["run_id"] == RUN_ID
        assert st["governed_research_evidence_current"] is True
        assert st["governed_manifest_run_id"] == RUN_ID

    def test_10_the_next_sessions_gate_is_not_lost_by_reflecting_the_run(
            self, tmp_path):
        _seed_drc(tmp_path)
        st = _status(tmp_path, session_status=msession.WAITING_FOR_OWNED_DATA)
        gate = st["pending_session_gate"]
        assert gate["expected_completed_market_date"] == NEXT_SESSION
        assert gate["session_status"] == msession.WAITING_FOR_OWNED_DATA
        assert st["required_actions"][0]["gate"] == "market_session"
        assert st["executable"] is False          # nothing new became runnable

    def test_11_reflecting_a_run_never_makes_a_cycle_executable(self, tmp_path):
        _seed_drc(tmp_path)
        for status in (msession.WAITING_FOR_OWNED_DATA,
                       msession.BEFORE_SESSION_CLOSE):
            assert _status(tmp_path, session_status=status)["executable"] is False

    def test_12_an_unconfirmed_eligible_session_reflects_nothing(self, tmp_path):
        """Case (a): the ELIGIBLE session's own data is unconfirmed. There is no
        eligible session to reflect, and the waiting contract is unchanged."""
        _seed_drc(tmp_path)
        st = drc.load_daily_research_cycle_status(
            drc_dir=tmp_path, monthly_emitter_available=True,
            freshness=_freshness(session_status=msession.WAITING_FOR_OWNED_DATA,
                                 eligible=None, owned_confirmed=None))
        assert st["state"] == drc.WAITING_FOR_OWNED_DATA

    def test_13_inconsistent_still_outranks_a_completed_run(self, tmp_path):
        _seed_drc(tmp_path)
        st = drc.load_daily_research_cycle_status(
            drc_dir=tmp_path, monthly_emitter_available=True,
            freshness=_freshness(session_status=msession.WAITING_FOR_OWNED_DATA,
                                 consistency="INCONSISTENT"))
        assert st["state"] == drc.INCONSISTENT

    def test_14_pre_r54_4_evidence_without_a_ledger_row_is_a_projection(
            self, tmp_path):
        got = _read_decision(tmp_path)
        assert got["available"] is True
        assert got["decision"] == pdec.GD_NO_CHANGE
        assert got["provenance"] == pdec.PROV_GOVERNED_DAILY_CYCLE
        assert got["eligible_market_session"] == SESSION
        assert got["persistence_status"] == (
            pdec.DECISION_PERSISTENCE_LEGACY_PROJECTION)
        assert got["is_ledger_row"] is False
        assert got["retrievable_through_owner"] is True

    def test_15_the_projection_is_retrievable_through_the_decision_owner(
            self, tmp_path):
        got = _read_decision(tmp_path)
        assert got["owner"] == pdec.GOVERNANCE_GATE_OWNER
        assert got["retrievability_owner"] == "api.portfolio_decision"

    def test_16_the_projection_creates_no_ledger_row(self, tmp_path):
        _read_decision(tmp_path)
        assert not (tmp_path / "governed_decisions.json").exists()
        assert list(tmp_path.iterdir()) == []

    def test_17_the_projection_rewrites_no_history_and_backfills_nothing(
            self, tmp_path):
        got = _read_decision(tmp_path)
        assert got["backfilled"] is False
        assert got["persisted"] is False
        assert got["persisted_record_present"] is False
        assert got["projected_daily_cycle_present"] is True

    def test_18_a_later_ledger_row_supersedes_the_projection(self, tmp_path):
        """The forward-going state: once the daily cycle delegates a real write,
        the row IS the decision and the compatibility shim retires."""
        before = _read_decision(tmp_path)
        assert before["persistence_status"] == (
            pdec.DECISION_PERSISTENCE_LEGACY_PROJECTION)
        cand = pdec.build_daily_cycle_candidate(
            portfolio_state={"active_book": {"book_id": BOOK},
                             "dates": {"eligible_market_date": SESSION}},
            drc_manifest=_manifest(), reassessment=_reassessment(),
            proposal_summary={"reallocation_outcome": None},
            constrained={"outcome": None}, scoring_identity={},
            hoc_binding={"hoc_artifact_id": HOC_AID, "hoc_persisted": True})
        pdec.record_governed_decision(
            candidate=cand,
            gate={"eligible": True, "verdict": pdec.GATE_ELIGIBLE,
                  "checks_passed": 1, "checks_total": 1,
                  "evaluated_at": STAMP_LATE},
            provenance=pdec.PROV_GOVERNED_DAILY_CYCLE,
            confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
            decision_dir=tmp_path)
        after = _read_decision(tmp_path)
        assert after["persistence_status"] == pdec.DECISION_PERSISTENCE_LEDGER_ROW
        assert after["is_ledger_row"] is True
        assert after["legacy_daily_projection_suppressed"] is True

    def test_19_no_governed_evidence_still_means_absent(self, tmp_path):
        """The repair restores a REAL fact; it does not invent one. With no
        governed manifest there is still no governed decision."""
        got = _read_decision(tmp_path, governed_current=False)
        assert got["available"] is False
        assert got["persistence_status"] == pdec.DECISION_PERSISTENCE_ABSENT
        assert got["retrievable_through_owner"] is False


# =========================================================================== #
# 20-24. A WITHHELD CANDIDATE MUST NOT ERASE STANDING AUTHORITY
# =========================================================================== #
class TestWithheldCandidateNeverErasesStandingAuthority:

    def _govern(self, tmp_path, *, hoc_matches):
        """Run the ONE governed-intraday path with every owner injected."""
        ev = {"run_id": "evt_1", "state": "REASSESSED_NO_CHANGE",
              "eligible_market_date": SESSION, "active_book_id": BOOK,
              "reassessment_hash": "RA1", "hoc_artifact_id": HOC_AID,
              "hoc_assessment_hash": "HOC1", "hoc_persisted": True,
              "hoc_persistence_status": "REUSED_EXISTING"}
        return pdec.govern_latest_intraday_assessment(
            confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
            portfolio_state={"active_book": {"book_id": BOOK},
                             "dates": {"eligible_market_date": SESSION}},
            workflow=_workflow(governed_current=True),
            event_cycle=ev, reassessment=_reassessment(),
            proposal_summary={"reallocation_outcome": None},
            constrained={"outcome": None}, scoring_identity={},
            hoc_binding={"hoc_artifact_id": HOC_AID, "hoc_persisted": True,
                         "hoc_artifact_retrievable": True,
                         "hoc_artifact_identity_matches": bool(hoc_matches),
                         "hoc_binding_detail": (
                             "stored artifact carries a DIFFERENT assessment hash"
                             if not hoc_matches else "artifact opened by id")},
            decision_dir=tmp_path)

    def test_20_a_withheld_intraday_candidate_leaves_the_standing_decision(
            self, tmp_path):
        standing_before = _read_decision(tmp_path)
        assert standing_before["available"] is True
        out = self._govern(tmp_path, hoc_matches=False)
        assert out["recorded"] is False
        standing_after = _read_decision(tmp_path)
        assert standing_after["available"] is True
        assert standing_after["decision"] == standing_before["decision"]
        assert standing_after["record_id"] == standing_before["record_id"]
        assert standing_after["persistence_status"] == (
            pdec.DECISION_PERSISTENCE_LEGACY_PROJECTION)

    def test_21_the_withheld_candidate_writes_no_ledger_row(self, tmp_path):
        self._govern(tmp_path, hoc_matches=False)
        rows = tmp_path / "governed_decisions.json"
        assert not rows.exists() or json.loads(rows.read_text()) == []

    def test_22_the_gate_still_reports_the_candidate_and_its_blockers(
            self, tmp_path):
        out = self._govern(tmp_path, hoc_matches=False)
        gate = out.get("gate") or {}
        assert gate.get("eligible") is False
        assert gate.get("withheld_reason_codes")
        assert pdec.WR_HOC_ARTIFACT_MISMATCH in gate["withheld_reason_codes"]

    def test_23_a_research_only_candidate_does_not_become_the_decision(
            self, tmp_path):
        """A LIVE_PRE_DRC_SIGNAL never enters this lane without the gate."""
        got = _read_decision(tmp_path)
        assert got["provenance"] == pdec.PROV_GOVERNED_DAILY_CYCLE
        assert got["provenance"] != "LIVE_PRE_DRC_SIGNAL"

    def test_24_the_standing_decision_is_named_for_the_withheld_candidate(
            self, tmp_path):
        """The withheld verdict is reported BESIDE the standing authority, so a
        reader can never mistake "not promoted" for "no governed decision"."""
        out = self._govern(tmp_path, hoc_matches=False)
        assert out["standing_decision_id"] is not None
        assert out["standing_decision_id"] == _read_decision(
            tmp_path)["record_id"]


# =========================================================================== #
# 25-29. TODAY / OPERATOR COHERENCE
# =========================================================================== #
class TestOperatorCoherence:

    def test_25_a_standing_hold_and_a_catch_up_action_are_not_contradictory(
            self, tmp_path):
        """Two different questions: WHAT SHOULD THE PORTFOLIO BE (standing HOLD,
        for Sep-2) and WHAT MUST THE OPERATOR DO NEXT (process Sep-3)."""
        got = _read_decision(tmp_path)
        assert got["decision"] == pdec.GD_NO_CHANGE
        assert got["manual_review_required"] is False
        # The standing decision is bound to the session it was decided for. It
        # says nothing about the LATER session the operator still has to process.
        assert got["eligible_market_session"] == SESSION
        assert got["safety"]["advances_operational_mark"] is False

    def test_26_a_no_change_decision_never_requires_manual_review(self, tmp_path):
        got = _read_decision(tmp_path)
        assert not (got["decision"] == pdec.GD_NO_CHANGE
                    and got["manual_review_required"])

    def test_27_the_browser_derives_no_governed_authority(self):
        ui = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8")
        for forbidden in ("LEGACY_COMPATIBILITY_PROJECTION ===",
                          "persistence_status =="):
            assert forbidden not in ui

    def test_28_the_browser_derives_no_runtime_alignment(self):
        ui = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8")
        assert "loaded_commit ===" not in ui
        assert "STALE_RUNTIME'" not in ui.replace('"STALE_RUNTIME"', "")

    def test_29_the_decision_owner_is_the_only_retrievability_authority(self):
        got_owner = pdec.classify_decision_persistence(
            record=None, available=False)["retrievability_owner"]
        assert got_owner == "api.portfolio_decision"


# =========================================================================== #
# 30-40. WINDOWS WORKER TOPOLOGY / PRESENCE
# =========================================================================== #
class TestWindowsWorkerPresence:

    def test_30_authoritative_runtime_state_proves_a_singleton_without_cim(self):
        """THE DEFECT-3 REPAIR. The exact live shape: a blind snapshot beside a
        live pid, a held lock, a matching instance and a fresh heartbeat."""
        got = _presence(_blind_snapshot(), state=_service_state(), lock=_lock())
        assert got["verdict"] == ic.WORKER_PRESENCE_CONFIRMED_NO_OS
        assert got["singleton_proven"] is True
        assert got["decided_on"] == "AUTHORITATIVE_RUNTIME_STATE"
        assert got["os_metadata_available"] is False
        assert "unavailable" in (got["advisory"] or "")

    def test_31_an_unreadable_snapshot_is_never_NO_LOGICAL_WORKER(self):
        """The verdict that authorised clearing a live worker's singleton lock."""
        topo = ic.resolve_worker_topology(_blind_snapshot(), lock=_lock())
        assert topo["verdict"] == ic.WORKER_TOPOLOGY_AMBIGUOUS
        assert topo["verdict"] != ic.WORKER_TOPOLOGY_NONE
        assert topo["os_metadata_available"] is False

    def test_32_an_unreadable_snapshot_cannot_clear_a_live_workers_lock(self):
        """The safety consequence, asserted at the lock owner itself."""
        topo = ic.resolve_worker_topology(_blind_snapshot(), lock=_lock())
        decision = ic.resolve_abandoned_lock(lock=_lock(), topology=topo,
                                             now=T_NOW)
        assert decision["may_clear"] is False
        assert decision["state"] == ic.LOCK_REFUSED_TOPOLOGY_UNKNOWN

    def test_33_a_readable_empty_snapshot_still_means_no_worker(self):
        topo = ic.resolve_worker_topology(_readable_empty_snapshot(), lock=None)
        assert topo["verdict"] == ic.WORKER_TOPOLOGY_NONE
        assert topo["os_metadata_available"] is True

    def test_34_a_corroborated_singleton_is_CONFIRMED(self):
        got = _presence(_lineage_snapshot(), state=_service_state(), lock=_lock())
        assert got["verdict"] == ic.WORKER_PRESENCE_CONFIRMED
        assert got["singleton_proven"] is True

    def test_35_two_proven_workers_fail_closed_even_though_one_is_healthy(self):
        """Unreadable OS metadata may never manufacture a worker — and a readable
        snapshot proving TWO may never be softened by healthy runtime state."""
        got = _presence(_two_lineage_snapshot(), state=_service_state(),
                        lock=_lock())
        assert got["verdict"] == ic.WORKER_PRESENCE_MULTIPLE
        assert got["singleton_proven"] is False
        assert got["decided_on"] == "PROVEN_MULTIPLE_LINEAGES"

    def test_36_a_stale_heartbeat_and_a_dead_pid_is_no_worker(self):
        dead = _service_state(heartbeat=datetime(2026, 9, 3, 1, 0, 0,
                                                 tzinfo=timezone.utc))
        got = _presence(_blind_snapshot(), state=dead, lock=_lock(), alive=False)
        assert got["verdict"] == ic.WORKER_PRESENCE_NONE
        assert got["singleton_proven"] is False

    def test_37_a_heartbeat_pid_that_is_not_the_lock_owner_is_inconsistent(self):
        got = _presence(_blind_snapshot(), state=_service_state(pid=1976),
                        lock=_lock(pid=4242))
        assert got["verdict"] == ic.WORKER_PRESENCE_INCONSISTENT
        assert got["decided_on"] == "CONFLICTING_RUNTIME_EVIDENCE"
        assert got["singleton_proven"] is False

    def test_38_a_mismatched_instance_id_is_inconsistent(self):
        got = _presence(_blind_snapshot(),
                        state=_service_state(instance="inst-1"),
                        lock=_lock(instance="inst-2"))
        assert got["verdict"] == ic.WORKER_PRESENCE_INCONSISTENT
        assert got["singleton_proven"] is False

    def test_39_a_complete_snapshot_that_contradicts_live_state_is_inconsistent(
            self):
        """Both could see this fact and they disagree — that is reported, not
        resolved by preferring the more comfortable answer."""
        got = _presence(_readable_empty_snapshot(), state=_service_state(),
                        lock=_lock())
        assert got["verdict"] == ic.WORKER_PRESENCE_INCONSISTENT
        assert got["decided_on"] == "OS_PROCESS_CORRELATION"

    def test_40_presence_explains_its_evidence_and_its_ladder(self):
        got = _presence(_blind_snapshot(), state=_service_state(), lock=_lock())
        assert got["evidence_order"] == list(ic.WORKER_PRESENCE_EVIDENCE_ORDER)
        assert got["worker_pid"] == 1976 and got["lock_pid"] == 1976
        assert got["heartbeat_fresh"] is True
        assert got["read_only"] is True and got["writes_nothing"] is True
        assert got["owner"] == ic.PRESENCE_OWNER

    def test_41_a_legacy_bare_list_snapshot_still_works(self):
        """Every pre-R55.2.1 caller passes a plain list and is taken at its word."""
        topo = ic.resolve_worker_topology([], lock=None)
        assert topo["verdict"] == ic.WORKER_TOPOLOGY_NONE
        assert topo["snapshot_authoritative"] is True

    def test_42_a_failed_process_query_is_not_an_empty_machine(self):
        topo = ic.resolve_worker_topology(
            {"rows": [], "introspection": {"scanned_count": 0,
                                           "unreadable_command_line_count": 0,
                                           "query_failed": True}}, lock=_lock())
        assert topo["verdict"] == ic.WORKER_TOPOLOGY_AMBIGUOUS


# =========================================================================== #
# 43-47. THE RESTART CONTRACT
# =========================================================================== #
class TestRestartContract:

    def test_43_restart_succeeds_when_only_os_metadata_is_missing(self):
        """The exact live-shaped condition: new instance, live pid, fresh
        heartbeat, lock held by the same pid, CIM command line unavailable."""
        got = _presence(_blind_snapshot(),
                        state=_service_state(instance="64258bb3"),
                        lock=_lock(instance="64258bb3"))
        assert got["singleton_proven"] is True
        # ...and the script gates on exactly that flag, nothing else.
        assert "function Test-SingletonProven" in MANAGE_PS1
        assert "return [bool]$Topology.singleton_proven" in MANAGE_PS1
        assert "if (-not (Test-SingletonProven $topology))" in MANAGE_PS1

    def test_44_restart_blocks_on_real_singleton_ambiguity(self):
        for state, lock in ((_service_state(pid=1976), _lock(pid=4242)),
                            (_service_state(instance="a"), _lock(instance="b"))):
            assert _presence(_blind_snapshot(), state=state,
                             lock=lock)["singleton_proven"] is False
        assert _presence(_two_lineage_snapshot(), state=_service_state(),
                         lock=_lock())["singleton_proven"] is False

    def test_45_the_script_no_longer_gates_on_the_cim_only_verdicts(self):
        assert 'verdict -ne "NO_LOGICAL_WORKER"' not in MANAGE_PS1
        assert "-not $topology.healthy" not in MANAGE_PS1

    def test_46_the_collector_reports_what_it_could_not_read(self):
        assert "unreadable_command_line_count" in MANAGE_PS1
        assert "function Get-WorkerScan" in MANAGE_PS1
        assert "$unreadable++" in MANAGE_PS1

    def test_47_stop_does_not_claim_success_from_a_blind_snapshot(self):
        assert "AN EMPTY SNAPSHOT IS NOT PROOF OF A STOP" in MANAGE_PS1
        assert "$verdict.presence.worker_pid" in MANAGE_PS1


# =========================================================================== #
# 48-56. ARCHITECTURAL BOUNDARIES AND SAFETY
# =========================================================================== #
class TestBoundariesAndSafety:

    def test_48_one_presence_owner_and_one_topology_owner(self):
        assert IC_SRC.count("def resolve_worker_presence(") == 1
        assert IC_SRC.count("def resolve_worker_topology(") == 1
        assert CONTROL_SRC.count("def resolve_worker_presence(") == 0
        assert CONTROL_SRC.count("def resolve_worker_topology(") == 0

    def test_49_powershell_decides_no_verdict_of_its_own(self):
        for invented in ("CONFIRMED_SINGLETON =", "$presence = if",
                         "NO_WORKER'"):
            assert invented not in MANAGE_PS1

    def test_50_the_lifecycle_owner_compares_no_commits(self):
        body = IC_SRC.split("def resolve_service_lifecycle(")[1].split(
            "\ndef ")[0]
        for forbidden in ("ALIGNED", "STALE_RUNTIME", "read_source_identity",
                          "rev-parse", "classify_alignment"):
            assert forbidden not in body

    def test_51_the_presence_owner_never_starts_or_kills_anything(self):
        body = IC_SRC.split("def resolve_worker_presence(")[1].split("\ndef ")[0]
        for forbidden in ("subprocess", "Popen", "terminate(", "kill(",
                          "save_service_state", "unlink("):
            assert forbidden not in body

    def test_52_the_drc_reflection_writes_nothing(self, tmp_path):
        _seed_drc(tmp_path)
        before = sorted(p.name for p in tmp_path.rglob("*"))
        _status(tmp_path, session_status=msession.WAITING_FOR_OWNED_DATA)
        assert sorted(p.name for p in tmp_path.rglob("*")) == before

    def test_53_no_governed_ledger_row_is_ever_backfilled(self, tmp_path):
        for src in (DRC_SRC,):
            assert "backfill" not in src.lower() or "no historical" in src.lower()
        got = _read_decision(tmp_path)
        assert got["backfilled"] is False
        assert not (tmp_path / "governed_decisions.json").exists()

    def test_54_nothing_here_creates_an_order_a_fill_or_an_approval(self,
                                                                   tmp_path):
        out = TestWithheldCandidateNeverErasesStandingAuthority()._govern(
            tmp_path, hoc_matches=False)
        assert out["recorded"] is False
        safety = _read_decision(tmp_path)["safety"]
        for flag in ("created_orders", "created_order_plan", "created_fills",
                     "approved_anything", "automatic_approval_allowed",
                     "promoted_model", "activated_sleeve", "changed_holdings",
                     "changed_cash", "changed_nav", "ran_daily_close",
                     "advances_operational_mark", "broker_enabled",
                     "automation_enabled"):
            assert safety[flag] is False, flag
        assert safety["manual_review_required_for_change"] is True

    def test_55_execution_automation_stays_off(self, tmp_path):
        got = _read_decision(tmp_path)
        assert got["approval_required_token"] == (
            "CONFIRM_PORTFOLIO_REBALANCE_DECISION")
        assert "Start-ScheduledTask" not in IC_SRC

    def test_56_no_model_promotion_or_sleeve_activation_anywhere_here(self):
        for src in (IC_SRC, CONTROL_SRC):
            assert "promote_model" not in src
            assert "activate_sleeve" not in src

    def test_57_no_portfolio_cycle_is_run_by_any_repaired_path(self):
        for src in (IC_SRC, CONTROL_SRC, MANAGE_PS1):
            assert "run_daily_close" not in src
            assert "run_portfolio_cycle" not in src
        body = DRC_SRC.split("def _reflect_completed_run(")[1].split("\ndef ")[0]
        for forbidden in ("_save_run", "_atomic_write_json", "run_daily_research"):
            assert forbidden not in body
