r"""RELEASE 55.2 — RUNTIME RELEASE IDENTITY, STALE-WORKER DETECTION AND
DECISION-LATENCY SEMANTIC HARDENING.

THE INCIDENT, REPRODUCED
------------------------
The information-collection worker started at 2026-09-01 14:12:09. The R54.1
governance gate was committed at 2026-09-01 23:46:16 — nine and a half hours
later. The worker held its pre-R54.1 module graph for life, so every intraday
cycle it produced silently skipped the governance path while the heartbeat
stayed fresh, progress kept advancing and the service reported RUNNING. Nothing
in the system could say "this process is running old code".

``TestTheStaleWorkerIncident`` reproduces exactly that shape: source at
revision B, a healthy busy worker that started on revision A, and the assertion
that the verdict is STALE_RUNTIME — never ALIGNED merely because git HEAD is B.

WHAT ELSE IS PROVEN HERE
------------------------
    * a loaded identity is captured ONCE and a later source change cannot move
      it (the property that makes staleness detectable at all);
    * ALIGNED is unreachable from liveness — a fresh heartbeat, a live pid and
      an advancing iteration never produce it;
    * a stale runtime degrades the LIVE lane and leaves the governed decision,
      the operational close and the primary operator action untouched;
    * a scheduled runtime that exits between invocations is NOT_APPLICABLE, not
      a permanent UNKNOWN;
    * restart returns the runtime to ALIGNED while durable watermarks stay
      independent of process release identity;
    * ``observation_to_signal_seconds`` is labelled an OBSERVATION AGE whenever
      the observation was not admitted by the cycle that produced the signal
      stamp — and a NOT_REQUIRED stage is still never zero-filled.

Every test is hermetic: tmp_path stores, injected identities, injected git
runners. Nothing reads the production collection root, starts or stops a
process, restarts a service, or writes to the real repository.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from paper_trader.api import active_manager_state as ams
from paper_trader.api import event_signal_refresh as esr
from paper_trader.api import information_collection as ic
from paper_trader.api import runtime_identity as rid

REPO = Path(__file__).resolve().parents[1]

# The two revisions of the incident: A is what the worker loaded, B is deployed.
REV_A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
REV_B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

T_WORKER_START = datetime(2026, 9, 1, 14, 12, 9, tzinfo=timezone.utc)
T_GATE_COMMIT = datetime(2026, 9, 1, 23, 46, 16, tzinfo=timezone.utc)
T_NOW = datetime(2026, 9, 3, 19, 0, 0, tzinfo=timezone.utc)


def _identity(commit: str, *, captured_at: datetime = T_WORKER_START,
              pid: int = 19228, dirty: bool = False) -> dict:
    """A frozen loaded-identity capture, shaped exactly as the owner emits it."""
    return {
        "identity_kind": "LOADED_RUNTIME_IDENTITY",
        "owner": rid.OWNER,
        "schema_version": rid.SCHEMA_VERSION,
        "contract_id": rid.CONTRACT_ID,
        "repo_root": str(REPO),
        "commit": commit,
        "commit_short": commit[:12],
        "branch": "stage19-controlled-rebalance",
        "dirty_at_capture": dirty,
        "resolved_from": rid.RESOLVED_FROM_GIT_DIR,
        "captured_at": captured_at.isoformat(),
        "pid": pid,
        "captured_once_per_process": True,
        "changes_when_source_changes": False,
    }


def _source(commit: str, *, dirty: bool = False) -> dict:
    return {
        "identity_kind": "SOURCE_REPOSITORY_IDENTITY",
        "owner": rid.OWNER,
        "repo_root": str(REPO),
        "commit": commit,
        "commit_short": (commit[:12] if commit else None),
        "branch": "stage19-controlled-rebalance",
        "dirty": dirty,
        "resolved_from": rid.RESOLVED_FROM_GIT_DIR,
        "read_at": T_NOW.isoformat(),
        "is_a_loaded_runtime_identity": False,
    }


def _healthy_worker_process() -> dict:
    """Everything an operator would read as "this worker is fine"."""
    return {"pid": 19228, "instance_id": "f4dae865-28a3-4d1d-a0b2-ee312a670fa5",
            "started_at": T_WORKER_START.isoformat(),
            "service_state": "RUNNING", "worker_activity": "BUSY",
            "heartbeat_age_seconds": 6.1, "progress_age_seconds": 2.0}


# =========================================================================== #
# PHASE G — THE INCIDENT
# =========================================================================== #
class TestTheStaleWorkerIncident:
    """Source at B, a healthy worker that started on A. The verdict must be
    STALE_RUNTIME, and it must not be reachable from health."""

    def _composed(self, *, worker_commit=REV_A, source_commit=REV_B):
        return rid.build_runtime_alignment(
            source=_source(source_commit),
            runtimes=[
                {"runtime": rid.RUNTIME_BACKEND,
                 "loaded": _identity(source_commit)},
                {"runtime": rid.RUNTIME_COLLECTION,
                 "loaded": (_identity(worker_commit) if worker_commit else None),
                 "process": _healthy_worker_process()},
            ])

    def test_a_worker_started_on_an_older_revision_is_stale(self):
        row = [r for r in self._composed()["runtimes"]
               if r["runtime"] == rid.RUNTIME_COLLECTION][0]
        assert row["verdict"] == rid.ALIGNMENT_STALE
        assert row["reason"] == rid.REASON_DIFFERENT_COMMIT
        assert row["loaded_commit"] == REV_A
        assert row["source_commit"] == REV_B

    def test_it_is_never_aligned_merely_because_git_head_is_b(self):
        composed = self._composed()
        assert composed["verdict"] == rid.ALIGNMENT_STALE
        assert composed["aligned"] is False
        assert rid.RUNTIME_COLLECTION in composed["stale_runtimes"]

    def test_a_fresh_heartbeat_and_a_busy_worker_never_produce_aligned(self):
        # Every liveness signal is present and maximally healthy.
        row = [r for r in self._composed()["runtimes"]
               if r["runtime"] == rid.RUNTIME_COLLECTION][0]
        assert row["process"]["service_state"] == "RUNNING"
        assert row["process"]["worker_activity"] == "BUSY"
        assert row["verdict"] != rid.ALIGNMENT_ALIGNED
        assert row["inferred_from_process_health"] is False
        assert self._composed()["infers_alignment_from_process_health"] is False

    def test_the_worker_is_still_processing_iterations_and_still_stale(self):
        composed = self._composed()
        # Health facts travel BESIDE the verdict; they never reach it.
        row = [r for r in composed["runtimes"]
               if r["runtime"] == rid.RUNTIME_COLLECTION][0]
        assert row["process"]["progress_age_seconds"] == 2.0
        assert row["verdict"] == rid.ALIGNMENT_STALE

    def test_the_verdict_names_the_exact_remediation_and_restarts_nothing(self):
        composed = self._composed()
        assert composed["restarts_nothing"] is True
        assert composed["restarts_no_process_automatically"] is True
        assert any("manage_information_collection.ps1" in r
                   for r in composed["remediation"])

    def test_a_backend_on_the_deployed_revision_is_aligned(self):
        row = [r for r in self._composed()["runtimes"]
               if r["runtime"] == rid.RUNTIME_BACKEND][0]
        assert row["verdict"] == rid.ALIGNMENT_ALIGNED
        assert row["reason"] == rid.REASON_SAME_COMMIT

    def test_the_incident_timeline_is_what_the_capture_records(self):
        # The worker's capture predates the commit that introduced the gate.
        loaded = _identity(REV_A, captured_at=T_WORKER_START)
        assert datetime.fromisoformat(loaded["captured_at"]) < T_GATE_COMMIT

    def test_a_worker_restart_on_revision_b_becomes_aligned(self):
        composed = self._composed(worker_commit=REV_B)
        row = [r for r in composed["runtimes"]
               if r["runtime"] == rid.RUNTIME_COLLECTION][0]
        assert row["verdict"] == rid.ALIGNMENT_ALIGNED
        assert composed["verdict"] == rid.ALIGNMENT_ALIGNED
        assert composed["aligned"] is True
        assert composed["proven"] is True
        assert composed["stale_runtimes"] == []
        assert row["remediation"] is None


class TestWatermarksAreIndependentOfReleaseIdentity:
    """A restart changes the loaded release. It must not touch the durable
    market/event watermarks, which belong to the collection owner."""

    def test_registering_a_new_worker_keeps_the_durable_watermarks(self, tmp_path):
        ic.register_worker_start(root=tmp_path, instance_id="first", pid=101,
                                 now=T_WORKER_START,
                                 loaded_release=_identity(REV_A))
        state = ic.load_service_state(root=tmp_path)
        state.update({"last_collection_success_at": "2026-09-02T18:00:00+00:00",
                      "last_material_information_at": "2026-09-02T18:30:00+00:00",
                      "last_event_cycle_at": "2026-09-02T18:31:00+00:00"})
        ic.save_service_state(state, root=tmp_path)

        ic.register_worker_start(root=tmp_path, instance_id="second", pid=202,
                                 now=T_NOW, loaded_release=_identity(REV_B))
        after = ic.load_service_state(root=tmp_path)
        assert after["last_collection_success_at"] == "2026-09-02T18:00:00+00:00"
        assert after["last_material_information_at"] == "2026-09-02T18:30:00+00:00"
        assert after["last_event_cycle_at"] == "2026-09-02T18:31:00+00:00"
        # Only the identity moved, and it moved because a NEW process started.
        assert after["loaded_release"]["commit"] == REV_B
        assert after["restart_count"] == 1

    def test_a_pre_r55_2_state_file_reads_back_with_no_identity(self, tmp_path):
        # A worker that started before this release recorded nothing. The state
        # must load cleanly and report None rather than inventing an identity.
        path = tmp_path / "collection_service_state.json"
        path.write_text(json.dumps({"service_id": ic.SERVICE_ID,
                                    "started_at": T_WORKER_START.isoformat(),
                                    "instance_id": "legacy", "pid": 19228}),
                        encoding="utf-8")
        state = ic.load_service_state(root=tmp_path)
        assert state["loaded_release"] is None
        assert state["started_at"] == T_WORKER_START.isoformat()


# =========================================================================== #
# PHASE B — CAPTURE ONCE, THEN FREEZE
# =========================================================================== #
class TestLoadedIdentityIsCapturedOnceAndFrozen:

    @pytest.fixture(autouse=True)
    def _clean(self):
        rid.reset_loaded_identity_for_tests()
        yield
        rid.reset_loaded_identity_for_tests()

    def test_a_later_source_change_does_not_move_the_loaded_identity(self):
        first = rid.capture_loaded_identity(runner=lambda a: "")
        # The source tree "advances" — every later call must be unmoved.
        later = rid.capture_loaded_identity(runner=lambda a: REV_B)
        assert later is first
        assert later["commit"] == first["commit"]
        assert later["captured_at"] == first["captured_at"]

    def test_loaded_identity_returns_the_same_frozen_mapping(self):
        a = rid.capture_loaded_identity()
        b = rid.loaded_identity()
        assert a is b
        assert a["captured_once_per_process"] is True
        assert a["changes_when_source_changes"] is False

    def test_the_source_read_is_deliberately_dynamic(self):
        # The SOURCE side must re-read; only the LOADED side is frozen. Two
        # reads carry two timestamps, which is what makes them different facts.
        one = rid.read_source_identity()
        two = rid.read_source_identity()
        assert one["is_a_loaded_runtime_identity"] is False
        assert one["commit"] == two["commit"]
        assert "read_at" in one and "read_at" in two

    def test_the_real_repository_resolves_its_commit_without_a_subprocess(self):
        src = rid.read_source_identity(runner=lambda a: None)
        assert src["commit"], "the repo's own .git must resolve a commit"
        assert src["resolved_from"] == rid.RESOLVED_FROM_GIT_DIR
        assert len(src["commit"]) == 40

    def test_an_unresolvable_repository_degrades_to_unknown_not_to_a_guess(
            self, tmp_path):
        src = rid.read_source_identity(repo_root=tmp_path,
                                       runner=lambda a: None)
        assert src["commit"] is None
        assert src["resolved_from"] == rid.RESOLVED_UNRESOLVED
        assert src["dirty"] is None

    def test_dirtiness_is_unknown_rather_than_clean_when_git_cannot_answer(
            self, tmp_path):
        src = rid.read_source_identity(repo_root=tmp_path,
                                       runner=lambda a: None)
        assert src["dirty"] is not False


# =========================================================================== #
# PHASE E — THE ALIGNMENT CONTRACT, FAIL-CLOSED
# =========================================================================== #
class TestAlignmentFailsClosed:

    def test_an_unrecorded_loaded_identity_is_unknown_never_aligned(self):
        row = rid.classify_alignment(loaded=None, source=_source(REV_B))
        assert row["verdict"] == rid.ALIGNMENT_UNKNOWN
        assert row["reason"] == rid.REASON_LOADED_UNKNOWN

    def test_an_unresolvable_source_is_unknown_never_aligned(self):
        row = rid.classify_alignment(loaded=_identity(REV_A),
                                     source=_source(None))
        assert row["verdict"] == rid.ALIGNMENT_UNKNOWN
        assert row["reason"] == rid.REASON_SOURCE_UNKNOWN

    def test_two_empty_identities_are_unknown_not_equal(self):
        row = rid.classify_alignment(loaded=_source(None), source=_source(None))
        assert row["verdict"] == rid.ALIGNMENT_UNKNOWN

    def test_a_scheduled_runtime_is_not_applicable_not_unknown(self):
        composed = rid.build_runtime_alignment(
            source=_source(REV_B),
            runtimes=[{"runtime": rid.RUNTIME_RESEARCH, "loaded": None},
                      {"runtime": rid.RUNTIME_INTRADAY_EMISSION,
                       "loaded": None}])
        for row in composed["runtimes"]:
            assert row["verdict"] == rid.ALIGNMENT_NOT_APPLICABLE
            assert row["reason"] == rid.REASON_NOT_REQUIRED
        assert composed["unknown_runtimes"] == []

    def test_proven_is_false_while_any_required_runtime_is_unknown(self):
        composed = rid.build_runtime_alignment(
            source=_source(REV_B),
            runtimes=[{"runtime": rid.RUNTIME_BACKEND,
                       "loaded": _identity(REV_B)},
                      {"runtime": rid.RUNTIME_COLLECTION, "loaded": None}])
        assert composed["proven"] is False
        assert composed["aligned"] is False
        assert composed["verdict"] == rid.ALIGNMENT_UNKNOWN

    def test_a_proven_stale_runtime_outranks_an_unprovable_one(self):
        composed = rid.build_runtime_alignment(
            source=_source(REV_B),
            runtimes=[{"runtime": rid.RUNTIME_BACKEND, "loaded": None},
                      {"runtime": rid.RUNTIME_COLLECTION,
                       "loaded": _identity(REV_A)}])
        assert composed["verdict"] == rid.ALIGNMENT_STALE

    def test_an_aligned_runtime_never_masks_a_stale_one(self):
        composed = rid.build_runtime_alignment(
            source=_source(REV_B),
            runtimes=[{"runtime": rid.RUNTIME_BACKEND,
                       "loaded": _identity(REV_B)},
                      {"runtime": rid.RUNTIME_COLLECTION,
                       "loaded": _identity(REV_A)}])
        assert composed["verdict"] == rid.ALIGNMENT_STALE

    def test_a_dirty_working_tree_is_a_caveat_and_not_a_stale_verdict(self):
        row = rid.classify_alignment(loaded=_identity(REV_B),
                                     source=_source(REV_B, dirty=True))
        assert row["verdict"] == rid.ALIGNMENT_ALIGNED
        assert row["verdict_decided_on"] == "COMMIT_IDENTITY"
        assert row["working_tree_caveat"]

    def test_every_verdict_carries_a_named_reason(self):
        for loaded, source in ((None, _source(REV_B)),
                               (_identity(REV_A), _source(None)),
                               (_identity(REV_A), _source(REV_B)),
                               (_identity(REV_B), _source(REV_B))):
            row = rid.classify_alignment(loaded=loaded, source=source)
            assert row["reason"] in rid.ALIGNMENT_REASONS
            assert row["verdict"] in rid.ALIGNMENT_VERDICTS

    def test_the_classifier_writes_nothing_and_restarts_nothing(self):
        row = rid.classify_alignment(loaded=_identity(REV_A),
                                     source=_source(REV_B))
        assert row["writes_nothing"] is True
        assert row["restarts_nothing"] is True


# =========================================================================== #
# PHASE F — WHAT A STALE RUNTIME MAY AND MAY NOT DO
# =========================================================================== #
def _live_information(*, service_state="RUNNING") -> dict:
    return {
        "available": True,
        "collection_running": service_state == "RUNNING",
        "collection_service_state": service_state,
        "worker_activity": "BUSY",
        "last_observation_at": "2026-09-03T18:50:00+00:00",
        "last_material_event_at": "2026-09-03T17:00:00+00:00",
        "last_event_cycle": {
            "run_id": "evt_stale", "state": "NO_NEW_INFORMATION",
            "generated_at": "2026-09-03T18:55:00+00:00",
            "reassessment_ran": False, "proposal_built": False,
            "events_admitted": 0, "cycle_duration_seconds": 12.5,
            "stage_timestamps": {
                "signal_refresh_completed_at": "2026-09-03T18:55:20+00:00",
                "scoring_completed_at": None, "hoc_completed_at": None,
                "reassessment_completed_at": None, "target_completed_at": None},
        },
    }


def _alignment(verdict_commit=REV_A) -> dict:
    composed = rid.build_runtime_alignment(
        source=_source(REV_B),
        runtimes=[{"runtime": rid.RUNTIME_BACKEND, "loaded": _identity(REV_B)},
                  {"runtime": rid.RUNTIME_COLLECTION,
                   "loaded": (_identity(verdict_commit) if verdict_commit
                              else None),
                   "process": _healthy_worker_process()}])
    return dict(composed, available=True)


class TestStaleRuntimeDegradesResearchWithoutInvalidatingTheBook:

    def _state(self, alignment):
        return ams.build_active_manager_state(
            workflow={"canonical_portfolio_decision": {}},
            information_collection={"service": {"service_state": "RUNNING"}},
            runtime_alignment=alignment)

    def test_a_stale_runtime_degrades_the_live_lane(self):
        state = self._state(_alignment(REV_A))
        lane = state["live_reassessment_lane"]
        assert lane["runtime_degraded"] is True
        assert lane["runtime_alignment"] == rid.ALIGNMENT_STALE
        assert "older application release" in lane[
            "runtime_degradation_statement"]

    def test_an_unknown_runtime_also_degrades_the_live_lane(self):
        lane = self._state(_alignment(None))["live_reassessment_lane"]
        assert lane["runtime_degraded"] is True
        assert lane["runtime_alignment"] == rid.ALIGNMENT_UNKNOWN

    def test_an_aligned_runtime_adds_no_degradation_at_all(self):
        lane = self._state(_alignment(REV_B))["live_reassessment_lane"]
        assert lane["runtime_degraded"] is False
        assert lane["runtime_degradation_statement"] is None
        assert lane["runtime_remediation"] is None

    def test_it_never_invalidates_the_governed_decision_or_the_close(self):
        lane = self._state(_alignment(REV_A))["live_reassessment_lane"]
        assert lane["invalidates_governed_decision"] is False
        assert lane["invalidates_operational_close"] is False
        ra = self._state(_alignment(REV_A))["runtime_alignment"]
        assert ra["invalidates_operational_close"] is False
        assert ra["invalidates_governed_decision"] is False

    def test_it_never_manufactures_a_portfolio_change(self):
        stale = self._state(_alignment(REV_A))
        aligned = self._state(_alignment(REV_B))
        assert (stale["latest_governed_portfolio_decision"]
                == aligned["latest_governed_portfolio_decision"])
        assert stale["operational_book"] == aligned["operational_book"]

    def test_it_never_alters_the_primary_operator_action(self):
        stale = self._state(_alignment(REV_A))
        aligned = self._state(_alignment(REV_B))
        assert stale["operator_guidance"] == aligned["operator_guidance"]
        assert (stale["operator_answer"]["what_to_do_now"]
                == aligned["operator_answer"]["what_to_do_now"])
        assert stale["runtime_alignment"]["alters_primary_operator_action"] is False

    def test_the_operator_action_owner_is_unchanged(self):
        # The one priority owner still decides; nothing here competes with it.
        assert ams.COMPONENT_OWNERS["operator_guidance"] == "api.workflow_state"
        assert "api.workflow_state" in ams.RUNTIME_STALENESS_POLICY

    def test_a_stale_runtime_is_a_named_operator_problem_with_remediation(self):
        state = self._state(_alignment(REV_A))
        rows = [r for r in state["stale_components"]
                if r["component"] == "runtime_alignment"]
        assert len(rows) == 1
        assert rows[0]["owner_state"] == rid.ALIGNMENT_STALE
        assert rows[0]["is_operator_problem"] is True
        assert rows[0]["owner"] == "api.runtime_identity"

    def test_an_aligned_runtime_adds_no_row_to_the_operator_list(self):
        state = self._state(_alignment(REV_B))
        assert not [r for r in state["stale_components"]
                    if r["component"] == "runtime_alignment"]

    def test_the_answer_block_carries_the_backend_composed_sentences(self):
        chg = self._state(_alignment(REV_A))["operator_answer"][
            "what_changed_since"]
        assert chg["runtime_degraded"] is True
        assert chg["runtime_degradation_statement"]
        assert "manage_information_collection.ps1" in chg["runtime_remediation"]

    def test_an_unavailable_identity_owner_degrades_the_block_not_the_state(self):
        state = self._state({"available": False, "verdict": None,
                             "unavailable_reason": "boom"})
        assert state["runtime_alignment"]["available"] is False
        # An unavailable owner must not manufacture an operator problem either.
        assert not [r for r in state["stale_components"]
                    if r["component"] == "runtime_alignment"]


# =========================================================================== #
# PHASE J — ACCEPTANCE CARRIES THE FACTS WITHOUT CHANGING ITS VOCABULARY
# =========================================================================== #
class TestAcceptanceCarriesRuntimeFacts:

    def _acc(self, alignment):
        state = ams.build_active_manager_state(
            information_collection={"service": {"service_state": "RUNNING"}},
            runtime_alignment=alignment)
        return state, ams.build_acceptance_contract(state)

    def test_the_ten_row_vocabulary_is_unchanged(self):
        assert ams.ACCEPTANCE_ROWS == (
            "COLLECTION", "SIGNAL", "SCORING", "HOC", "REASSESSMENT",
            "GOVERNANCE", "GOVERNED_DECISION", "OPERATIONAL_BOOK",
            "NEXT_ACTION", "LATENCY")
        _, acc = self._acc(_alignment(REV_A))
        assert [r["row"] for r in acc["rows"]] == list(ams.ACCEPTANCE_ROWS)

    def test_the_collection_row_reports_the_alignment_as_a_value(self):
        _, acc = self._acc(_alignment(REV_A))
        row = [r for r in acc["rows"] if r["row"] == "COLLECTION"][0]
        assert row["runtime_alignment"] == rid.ALIGNMENT_STALE
        assert row["loaded_release"] == REV_A[:12]
        assert row["deployed_release"] == REV_B[:12]

    def test_a_stale_runtime_does_not_silently_flip_a_row_to_present(self):
        # The row's key fact is unchanged, so its status is decided by the same
        # evidence as before; the runtime facts are additional, never load-bearing.
        _, stale = self._acc(_alignment(REV_A))
        _, ok = self._acc(_alignment(REV_B))
        assert ([r["status"] for r in stale["rows"]]
                == [r["status"] for r in ok["rows"]])

    def test_the_diagnostic_contract_is_named_and_separate(self):
        state, acc = self._acc(_alignment(None))
        assert state["runtime_alignment"]["contract_id"] == rid.CONTRACT_ID
        assert state["runtime_alignment"]["verdict"] == rid.ALIGNMENT_UNKNOWN
        # It fails closed on its own terms without touching acceptance rows.
        assert state["runtime_alignment"]["proven"] is False
        assert "RUNTIME_ALIGNMENT" not in acc["row_vocabulary"]

    def test_runtime_alignment_is_a_declared_component_with_an_owner(self):
        assert "runtime_alignment" in ams.COMPONENTS
        assert ams.COMPONENT_OWNERS["runtime_alignment"] == "api.runtime_identity"


# =========================================================================== #
# PHASE H — LATENCY SEMANTICS
# =========================================================================== #
class TestLatencySemantics:

    def _measure(self, **kw):
        base = dict(
            stage_timestamps={
                "signal_refresh_completed_at": "2026-09-03T18:55:20+00:00",
                "scoring_completed_at": "2026-09-03T18:55:25+00:00",
                "hoc_completed_at": None, "reassessment_completed_at": None,
                "target_completed_at": None},
            event_cycle_started_at="2026-09-03T18:55:00+00:00",
            observation_received_at="2026-09-03T17:13:29+00:00")
        base.update(kw)
        return esr.measure_decision_latency(**base)

    def test_an_unadmitted_observation_is_labelled_an_age_not_a_latency(self):
        out = self._measure(
            observation_provenance=esr.OBS_PREDATES_THIS_CYCLE)
        sem = out["interval_semantics"]["observation_to_signal_seconds"]
        assert sem["kind"] == esr.INTERVAL_KIND_OBSERVATION_AGE
        assert "Age of the newest observation" in sem["label"]
        assert "NOT processing time" in sem["means"]
        # The measured value itself is untouched: 6111 seconds stays 6111.
        assert out["observation_to_signal_seconds"] == pytest.approx(6111.0)

    def test_an_undeclared_provenance_fails_closed_to_an_age(self):
        out = self._measure()
        assert out["observation_provenance"] == esr.OBS_PROVENANCE_UNKNOWN
        assert (out["interval_semantics"]["observation_to_signal_seconds"]["kind"]
                == esr.INTERVAL_KIND_OBSERVATION_AGE)

    def test_an_unknown_token_is_not_accepted_as_a_pipeline_claim(self):
        out = self._measure(observation_provenance="TOTALLY_MADE_UP")
        assert out["observation_provenance"] == esr.OBS_PROVENANCE_UNKNOWN
        assert (out["interval_semantics"]["observation_to_signal_seconds"]["kind"]
                == esr.INTERVAL_KIND_OBSERVATION_AGE)

    def test_an_admitted_observation_really_is_a_pipeline_latency(self):
        out = self._measure(
            observation_provenance=esr.OBS_ADMITTED_BY_THIS_CYCLE)
        sem = out["interval_semantics"]["observation_to_signal_seconds"]
        assert sem["kind"] == esr.INTERVAL_KIND_PIPELINE
        assert sem["label"] == "Observation → signal refresh"

    def test_a_negative_value_is_named_as_cross_cycle_proof_not_hidden(self):
        out = self._measure(
            observation_received_at="2026-09-03T19:07:50+00:00",
            observation_provenance=esr.OBS_PREDATES_THIS_CYCLE)
        assert out["observation_to_signal_seconds"] < 0
        sem = out["interval_semantics"]["observation_to_signal_seconds"]
        assert "arrived AFTER" in sem["label"]
        assert sem["negative_is_proof_of_cross_cycle_endpoints"] is True

    def test_the_engines_own_processing_duration_is_reported_separately(self):
        out = self._measure(event_cycle_processing_seconds=12.53)
        assert out["event_cycle_processing_seconds"] == 12.5

    def test_an_absent_processing_duration_is_none_never_zero(self):
        assert self._measure()["event_cycle_processing_seconds"] is None
        assert self._measure(
            event_cycle_processing_seconds="nonsense")[
                "event_cycle_processing_seconds"] is None

    def test_the_middle_intervals_keep_their_pipeline_meaning(self):
        out = self._measure(
            observation_provenance=esr.OBS_PREDATES_THIS_CYCLE)
        assert (out["interval_semantics"]["signal_to_reassessment_seconds"]["kind"]
                == esr.INTERVAL_KIND_PIPELINE)

    def test_not_required_stays_not_required_and_is_never_zero_filled(self):
        cycle = {"state": esr.ST_NO_NEW_INFORMATION, "reassessment_ran": False}
        out = self._measure(
            observation_provenance=esr.OBS_PREDATES_THIS_CYCLE,
            not_required_stages=esr.stages_not_required(cycle))
        assert out["interval_dispositions"][
            "reassessment_to_governed_seconds"] == esr.LAT_NOT_REQUIRED
        assert out["reassessment_to_governed_seconds"] is None
        assert out["never_zero_fills_an_unexecuted_stage"] is True
        assert out["latency_measurement_complete"] is True

    def test_no_timestamp_is_manufactured_by_the_labelling(self):
        before = self._measure()["timestamps"]
        after = self._measure(
            observation_provenance=esr.OBS_ADMITTED_BY_THIS_CYCLE)["timestamps"]
        assert before == after

    def test_a_no_admission_cycle_proves_the_observation_predates_it(self):
        cycle = {"state": esr.ST_NO_NEW_INFORMATION, "events_admitted": 0}
        assert ams._observation_provenance(cycle) == esr.OBS_PREDATES_THIS_CYCLE

    def test_an_unstated_admission_count_is_not_claimed_as_this_cycles(self):
        assert ams._observation_provenance({}) == esr.OBS_PROVENANCE_UNKNOWN
        assert (ams._observation_provenance({"events_admitted": 4})
                == esr.OBS_PROVENANCE_UNKNOWN)

    def test_the_composed_state_publishes_the_labels(self):
        state = ams.build_active_manager_state(
            event_refresh={"last_run_summary": {
                "run_id": "evt_x", "state": esr.ST_NO_NEW_INFORMATION,
                "generated_at": "2026-09-03T18:55:00+00:00",
                "reassessment_ran": False, "events_admitted": 0,
                "cycle_duration_seconds": 12.5,
                "stage_timestamps": {
                    "signal_refresh_completed_at": "2026-09-03T18:55:20+00:00"}}},
            information_collection={"service": {"service_state": "RUNNING"}},
            runtime_alignment=_alignment(REV_B))
        lat = state["decision_latency"]
        assert lat["event_cycle_processing_seconds"] == 12.5
        assert lat["observation_provenance"] == esr.OBS_PREDATES_THIS_CYCLE
        assert lat["interval_labels"]["observation_to_signal_seconds"]


# =========================================================================== #
# PHASE D — THE WORKER'S OWN CONTRACT
# =========================================================================== #
class TestCollectionWorkerIdentityContract:

    def test_a_worker_start_records_the_identity_it_was_given(self, tmp_path):
        ic.register_worker_start(root=tmp_path, instance_id="w1", pid=19228,
                                 now=T_NOW, loaded_release=_identity(REV_B))
        state = ic.load_service_state(root=tmp_path)
        assert state["loaded_release"]["commit"] == REV_B
        assert state["loaded_release"]["pid"] == 19228

    def test_the_identity_survives_heartbeats_and_progress(self, tmp_path):
        ic.register_worker_start(root=tmp_path, instance_id="w1", pid=19228,
                                 now=T_NOW, loaded_release=_identity(REV_B))
        for i in range(3):
            ic.heartbeat(root=tmp_path, instance_id="w1", loop_count=i,
                         now=T_NOW + timedelta(seconds=30 * i))
        ic.record_progress(root=tmp_path, step="SOURCE", instance_id="w1",
                           iteration_id="it1", in_flight=True, now=T_NOW)
        state = ic.load_service_state(root=tmp_path)
        assert state["loaded_release"]["commit"] == REV_B

    def test_a_new_worker_process_replaces_the_identity(self, tmp_path):
        ic.register_worker_start(root=tmp_path, instance_id="w1", pid=1,
                                 now=T_WORKER_START,
                                 loaded_release=_identity(REV_A))
        ic.register_worker_start(root=tmp_path, instance_id="w2", pid=2,
                                 now=T_NOW, loaded_release=_identity(REV_B))
        assert ic.load_service_state(
            root=tmp_path)["loaded_release"]["commit"] == REV_B

    def test_the_status_composition_publishes_it(self, tmp_path):
        ic.register_worker_start(root=tmp_path, instance_id="w1", pid=19228,
                                 now=T_NOW, loaded_release=_identity(REV_B))
        payload = ic.load_information_collection(root=tmp_path, event_status={},
                                                 now=T_NOW)
        assert payload["service"]["loaded_release"]["commit"] == REV_B
        assert payload["service"]["loaded_release_owner"] == "api.runtime_identity"

    def test_the_collection_owner_publishes_the_fact_and_no_verdict(self, tmp_path):
        # ONE owner decides alignment. The collection payload carries the
        # worker's recorded fact and must not restate a comparison of its own.
        ic.register_worker_start(root=tmp_path, instance_id="w1", pid=19228,
                                 now=T_NOW, loaded_release=_identity(REV_A))
        payload = ic.load_information_collection(root=tmp_path, event_status={},
                                                 now=T_NOW)
        assert "runtime_alignment" not in payload["service"]
        assert "STALE_RUNTIME" not in json.dumps(payload, default=str)

    def test_release_identity_never_blocks_a_worker_start(self, tmp_path,
                                                          monkeypatch):
        monkeypatch.setattr(rid, "loaded_identity",
                            lambda: (_ for _ in ()).throw(RuntimeError("no git")))
        state = ic.register_worker_start(root=tmp_path, instance_id="w1",
                                         pid=19228, now=T_NOW)
        assert state["started_at"] == T_NOW.isoformat()
        assert state["loaded_release"] is None


# =========================================================================== #
# ARCHITECTURAL BOUNDARIES
# =========================================================================== #
class TestArchitecturalBoundaries:

    def _src(self, rel: str) -> str:
        return (REPO / rel).read_text(encoding="utf-8", errors="replace")

    def test_the_identity_owner_starts_stops_and_signals_nothing(self):
        src = self._src("api/runtime_identity.py")
        for forbidden in ("Popen", "taskkill", "Stop-Process", "os.kill",
                          "terminate(", "Start-Process", "os.system"):
            assert forbidden not in src, forbidden

    def test_the_identity_owner_writes_nothing(self):
        src = self._src("api/runtime_identity.py")
        for forbidden in ("open(", "write_text(", "mkdir(", "unlink("):
            assert forbidden not in src, forbidden

    def test_only_the_owner_defines_the_alignment_classification(self):
        for rel in ("api/active_manager_state.py", "api/information_collection.py",
                    "scripts/collection_service_control.py",
                    "scripts/run_information_collection_service.py"):
            assert "def classify_alignment(" not in self._src(rel), rel
            assert "def build_runtime_alignment(" not in self._src(rel), rel

    def test_no_surface_re_reads_head_and_calls_it_a_loaded_identity(self):
        # The exact bug this release exists to prevent: resolving HEAD at read
        # time and presenting it as what a running process loaded.
        for rel in ("api/active_manager_state.py", "api/information_collection.py",
                    "scripts/collection_service_control.py"):
            src = self._src(rel)
            assert "rev-parse" not in src, rel
            assert "read_source_identity(" not in src or rel.startswith("scripts/")

    def test_the_ui_computes_no_alignment(self):
        ui = self._src("api/ui/index.html")
        assert "_r55RuntimeAlignmentHtml" in ui
        for forbidden in ("STALE_RUNTIME'", 'loaded_commit ===',
                          "rev-parse", "source_commit ==="):
            assert forbidden not in ui, forbidden

    def test_the_ui_renders_the_backend_latency_labels(self):
        ui = self._src("api/ui/index.html")
        assert "lat.interval_labels" in ui
        assert "event_cycle_processing_seconds" in ui

    def test_the_reset_hook_is_used_only_by_tests(self):
        for rel in ("api/active_manager_state.py", "api/information_collection.py",
                    "api/app.py", "scripts/collection_service_control.py",
                    "scripts/run_information_collection_service.py"):
            assert "reset_loaded_identity_for_tests" not in self._src(rel), rel

    def test_the_backend_captures_its_identity_at_import(self):
        src = self._src("api/app.py")
        assert "_runtime_identity.capture_loaded_identity()" in src

    def test_the_worker_captures_its_identity_before_the_loop(self):
        src = self._src("scripts/run_information_collection_service.py")
        assert "rid.capture_loaded_identity(pid=pid)" in src
        assert src.index("capture_loaded_identity") < src.index("while True:")

    def test_the_release_introduces_no_second_scheduler_or_restarter(self):
        src = self._src("api/runtime_identity.py")
        assert "restarts_nothing" in src
        # Scan for scheduler/lifecycle CALLS, not for the word in the prose that
        # explains why there are none.
        for forbidden in ("Register-ScheduledTask", "Get-ScheduledTask",
                          "sleep(", "threading.", "Timer(", "asyncio"):
            assert forbidden not in src, forbidden

    def test_the_staleness_policy_is_stated_once_and_is_explicit(self):
        policy = ams.RUNTIME_STALENESS_POLICY
        assert "never changes the primary operator action" in policy
        assert "api.workflow_state" in policy
        assert "Nothing restarts a process automatically." in policy
