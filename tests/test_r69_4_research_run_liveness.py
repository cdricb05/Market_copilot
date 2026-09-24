"""tests/test_r69_4_research_run_liveness.py — R69.4.

Two defects, both found by mechanical reconciliation of the September 23 cycle,
both reproduced here hermetically.

1.  THE RESEARCH RUN THAT WAS DECLARED ABANDONED WHILE IT WORKED.

    ``drc_2026-09-23_12bb76e7b466`` started 2026-09-24T16:12:34Z and completed
    2026-09-24T17:05:13Z — 52.7 minutes, of which 31.3 were CAPTURE_FORWARD_EVIDENCE
    and 20.8 ADVANCE_PROSPECTIVE_TOURNAMENT, both bound to a degraded provider. The
    32 runs before it took 4.5–23.2 minutes, so the 45-minute lock constant had never
    once been reached and nothing measured what it would do when it was.

    At +45:00 ``_lock_is_stale`` answered "how OLD is this run?" to the question "is
    this process ALIVE?" and declared the working run abandoned. The status owner then
    skipped the lock branch, found no COMPLETE manifest (it is written at the END), and
    published NOT_STARTED / run_id=null / executable=True — inviting the operator to
    start a governed research cycle that was 46 minutes into executing.

2.  THE 90-SECOND workflow-state READ.

    Measured on the live store: a snapshot identity HIT costs 0.005s and a full
    ``_compose`` costs 9.2–12.0s. The identity is a stat fingerprint over the desk and
    DRC run stores — exactly what a running cycle rewrites continuously — so during a
    cycle every request misses. ``_compose`` runs outside the memo lock and nothing
    recorded that a build for the same identity was ALREADY RUNNING, so one page load
    fanned out across eleven snapshot-backed routes and composed the whole world eleven
    times at once, competing with the cycle for one GIL.

Every test is hermetic: run locks are written under ``tmp_path``, the snapshot identity
and composition are injected, and NO provider, prediction, network, database, real
cycle, Daily Close, approval, order or promotion occurs.
"""
from __future__ import annotations

import copy
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from paper_trader.api import daily_research_cycle as drc
from paper_trader.api import data_freshness as df
from paper_trader.api import decision_snapshot as ds

ROOT = Path(__file__).resolve().parent.parent
DRC_SRC = (ROOT / "api" / "daily_research_cycle.py").read_text(encoding="utf-8")
SNAP_SRC = (ROOT / "api" / "decision_snapshot.py").read_text(encoding="utf-8")

#: The observed run, to the second.
RUN_ID = "drc_2026-09-23_12bb76e7b466"
RUN_MINUTES = 52.7


def _utc(offset_seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)).isoformat()


def _lock(*, run_age_minutes: float, heartbeat_age_seconds=None, run_id=RUN_ID,
          key="12bb76e7b466e9e3c8b3bd91", ich="0a02b2a7aaa4b471e678a37c") -> dict:
    """A run lock of a given age. ``heartbeat_age_seconds=None`` writes a LEGACY lock
    (pre-R69.4) that declares no heartbeat at all."""
    doc = {"idempotency_key": key, "eligible_date": "2026-09-23", "run_id": run_id,
           "input_contract_hash": ich, "started_at": _utc(-run_age_minutes * 60.0),
           "pid": 30800}
    if heartbeat_age_seconds is not None:
        doc["heartbeat_at"] = _utc(-heartbeat_age_seconds)
        doc["heartbeat_interval_seconds"] = drc._HEARTBEAT_SECONDS
    return doc


def _put(tmp_path: Path, doc: dict) -> Path:
    p = Path(tmp_path) / drc._LOCK_FILE
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(doc), encoding="utf-8")
    return p


# =========================================================================== #
# 1. THE SEPTEMBER 23 SHAPE — a long run is not an abandoned run
# =========================================================================== #
class TestTheRunThatWasDeclaredAbandoned:

    def test_the_observed_52_minute_run_is_live_while_it_heartbeats(self):
        """The exact shape of 13:03 ET: 52 minutes in flight, beating 5s ago."""
        live = _lock(run_age_minutes=RUN_MINUTES, heartbeat_age_seconds=5)
        assert drc._lock_is_stale(live) is False, \
            "a run that is still stamping its heartbeat is working, not abandoned"

    def test_the_rule_it_replaced_would_have_condemned_that_same_run(self):
        """Without this repair the defect is reproducible: the legacy rule judges the
        SAME healthy run purely on age and calls it dead."""
        legacy = _lock(run_age_minutes=RUN_MINUTES, heartbeat_age_seconds=None)
        assert drc._lock_is_stale(legacy) is True, \
            "the legacy age rule is what mislabelled the September 23 run"
        assert RUN_MINUTES > drc._LOCK_STALE_MINUTES, \
            "the run genuinely outlived the constant — that is the whole defect"

    def test_duration_alone_never_decides_liveness_any_more(self):
        """Even at four hours, a beating run is live. Duration is reported, not judged."""
        marathon = _lock(run_age_minutes=240, heartbeat_age_seconds=10)
        assert drc._lock_is_stale(marathon) is False
        facts = drc.lock_liveness(marathon)
        assert facts["live"] is True
        assert facts["basis"] == "HEARTBEAT"
        assert facts["long_run_is_not_a_failure"] is True
        assert facts["run_age_seconds"] > drc._LOCK_STALE_MINUTES * 60

    def test_the_status_names_the_running_run_instead_of_saying_not_started(self, tmp_path):
        """The operator-visible defect: NOT_STARTED / run_id=null / executable=True
        published while the run executed."""
        _put(tmp_path, _lock(run_age_minutes=RUN_MINUTES, heartbeat_age_seconds=5))
        (Path(tmp_path) / drc._RUNS_SUBDIR).mkdir(parents=True, exist_ok=True)

        out = drc.load_daily_research_cycle_status(
            freshness=_freshness(), drc_dir=tmp_path, monthly_emitter_available=True)

        assert out["state"] != drc.NOT_STARTED, \
            "a running cycle must never be reported as one that never started"
        assert out["state"] in drc._RUNNING
        assert out["run_id"] == RUN_ID, "the operator must be told WHICH run holds the cycle"
        assert out["executable"] is False, \
            "the screen must not invite a second governed research cycle"
        assert out["run_in_flight"] is True
        assert out["run_liveness"]["basis"] == "HEARTBEAT"
        assert any("is in progress" in w for w in out.get("warnings") or [])
        assert any("not a failed run" in w for w in out.get("warnings") or [])

    def test_a_genuinely_abandoned_run_is_still_reclaimable(self, tmp_path):
        """The repair must not wedge the cycle shut: a lock whose process died stops
        beating, and after the liveness window the cycle is runnable again."""
        _put(tmp_path, _lock(run_age_minutes=RUN_MINUTES, heartbeat_age_seconds=600))
        (Path(tmp_path) / drc._RUNS_SUBDIR).mkdir(parents=True, exist_ok=True)

        out = drc.load_daily_research_cycle_status(
            freshness=_freshness(), drc_dir=tmp_path, monthly_emitter_available=True)
        assert out.get("run_in_flight") is not True
        assert out["state"] == drc.NOT_STARTED


# =========================================================================== #
# 2. LIVENESS IS A HEARTBEAT; the legacy rule survives for legacy locks
# =========================================================================== #
class TestLivenessVocabulary:

    @pytest.mark.parametrize("age_s,expected_live", [(0, True), (30, True), (179, True),
                                                     (181, False), (600, False)])
    def test_the_heartbeat_window_is_what_decides(self, age_s, expected_live):
        doc = _lock(run_age_minutes=90, heartbeat_age_seconds=age_s)
        assert drc.lock_liveness(doc)["live"] is expected_live

    def test_a_legacy_lock_keeps_the_age_rule_in_both_directions(self):
        young = _lock(run_age_minutes=5, heartbeat_age_seconds=None)
        old = _lock(run_age_minutes=RUN_MINUTES, heartbeat_age_seconds=None)
        assert drc._lock_is_stale(young) is False
        assert drc._lock_is_stale(old) is True
        assert drc.lock_liveness(young)["basis"] == "LEGACY_LOCK_AGE"
        assert drc.lock_liveness(young)["long_run_is_not_a_failure"] is False

    def test_an_unreadable_or_absent_lock_is_never_called_live(self):
        assert drc.lock_liveness(None)["held"] is False
        assert drc.lock_liveness(None)["live"] is False
        assert drc._lock_is_stale({"heartbeat_interval_seconds": 30}) is True
        assert drc._lock_is_stale({}) is True

    def test_a_corrupt_heartbeat_interval_fails_closed_to_the_default_window(self):
        doc = _lock(run_age_minutes=90, heartbeat_age_seconds=5)
        doc["heartbeat_interval_seconds"] = "not-a-number"
        assert drc._lock_is_stale(doc) is False           # 5s old is live either way
        doc["heartbeat_at"] = _utc(-100000)
        assert drc._lock_is_stale(doc) is True


# =========================================================================== #
# 3. THE HEARTBEAT'S LIFETIME — acquired with the lock, stopped before release
# =========================================================================== #
class TestHeartbeatLifetime:

    def test_write_lock_declares_a_heartbeat_and_clear_lock_stops_it(self, tmp_path):
        drc._write_lock(idempotency_key="k", eligible_date="2026-09-23", run_id=RUN_ID,
                        input_contract_hash="h", drc_dir=tmp_path)
        try:
            doc = json.loads((Path(tmp_path) / drc._LOCK_FILE).read_text(encoding="utf-8"))
            assert doc["heartbeat_interval_seconds"] == drc._HEARTBEAT_SECONDS
            assert doc["heartbeat_at"]
            assert doc["pid"]
            assert drc._lock_is_stale(doc) is False
            assert drc._BEAT_THREAD is not None and drc._BEAT_THREAD.is_alive()
        finally:
            drc._clear_lock(tmp_path)
        assert drc._BEAT_THREAD is None, "the pulse must not outlive the lock"

    def test_a_late_beat_cannot_resurrect_a_finished_runs_lock(self, tmp_path):
        """R69.3 learned this on the close: stop the pulse BEFORE the terminal write,
        or a beat lands after the file is gone and recreates it."""
        drc._write_lock(idempotency_key="k", eligible_date="2026-09-23", run_id=RUN_ID,
                        input_contract_hash="h", drc_dir=tmp_path)
        drc._clear_lock(tmp_path)
        lock_path = Path(tmp_path) / drc._LOCK_FILE
        assert not lock_path.exists()
        drc._stamp_heartbeat(tmp_path)          # a beat arriving after release
        assert not lock_path.exists(), \
            "stamping must never recreate the lock of a run that has finished"

    def test_stamping_preserves_every_other_field_of_the_lock(self, tmp_path):
        drc._write_lock(idempotency_key="key-1", eligible_date="2026-09-23",
                        run_id=RUN_ID, input_contract_hash="hash-1", drc_dir=tmp_path)
        try:
            before = json.loads((Path(tmp_path) / drc._LOCK_FILE).read_text(encoding="utf-8"))
            drc._stamp_heartbeat(tmp_path)
            after = json.loads((Path(tmp_path) / drc._LOCK_FILE).read_text(encoding="utf-8"))
            for field in ("idempotency_key", "eligible_date", "run_id",
                          "input_contract_hash", "started_at", "pid"):
                assert after[field] == before[field], "%s was rewritten by a heartbeat" % field
        finally:
            drc._clear_lock(tmp_path)


# =========================================================================== #
# 4. THE RUN PATH — a live long run holds the concurrency gate
# =========================================================================== #
class TestRunPathConcurrency:

    def test_a_live_long_run_is_refused_not_resumed(self, tmp_path):
        """Before the repair the guard fell through at +45:00 and the next caller
        RESUMED the same run_id, writing the same manifest concurrently. Only the
        in-process _RUN_LOCK stood between that and two writers of one manifest."""
        fresh = _freshness()
        facts = drc._facts(fresh)
        _put(tmp_path, _lock(run_age_minutes=RUN_MINUTES, heartbeat_age_seconds=5,
                             run_id=RUN_ID, key=facts["idempotency_key"],
                             ich=facts["input_contract_hash"]))
        (Path(tmp_path) / drc._RUNS_SUBDIR).mkdir(parents=True, exist_ok=True)

        out = drc.run_daily_research_cycle(
            confirm=drc.EXECUTE_CONFIRMATION, requested_by="test", freshness=fresh,
            drc_dir=tmp_path, refresh_confirm_token=None, monthly_emitter_fn=None)

        assert out["state"] == drc.RUN_IN_PROGRESS
        assert out["executable"] is False
        assert out["run_id"] == RUN_ID
        assert not (Path(tmp_path) / drc._RUNS_SUBDIR / ("%s.json" % RUN_ID)).exists(), \
            "a refused run must write no manifest"

    def test_the_in_process_guard_is_still_there(self):
        """The cross-process file lock is the outer guard; the in-process lock is the
        one that actually held on September 23. Neither may be removed."""
        assert "_RUN_LOCK = threading.Lock()" in DRC_SRC
        assert "_RUN_LOCK.acquire(blocking=False)" in DRC_SRC

    def test_the_explicit_confirmation_gate_still_refuses(self, tmp_path):
        """Nothing here weakens the manual-confirmation boundary."""
        out = drc.run_daily_research_cycle(
            confirm="", requested_by="test", freshness=_freshness(), drc_dir=tmp_path)
        assert out["state"] != drc.RUN_IN_PROGRESS
        assert not list((Path(tmp_path) / drc._RUNS_SUBDIR).glob("*.json")) \
            if (Path(tmp_path) / drc._RUNS_SUBDIR).exists() else True


# =========================================================================== #
# 4b. THE OPERATOR'S SCREEN — a running cycle is work in progress, not an
#     instruction to start one.
# =========================================================================== #
class TestWhatTheOperatorIsTold:

    def test_the_workflow_owner_says_monitor_not_run(self, tmp_path):
        """End to end: a live research run must compose RESEARCH_CYCLE_RUNNING and a
        MONITOR action. That vocabulary already existed — only the DRC owner's report
        was wrong, which is why no new state and no UI change is needed here."""
        from paper_trader.api import workflow_state as ws

        _put(tmp_path, _lock(run_age_minutes=RUN_MINUTES, heartbeat_age_seconds=5))
        (Path(tmp_path) / drc._RUNS_SUBDIR).mkdir(parents=True, exist_ok=True)
        running = drc.load_daily_research_cycle_status(
            freshness=_freshness(), drc_dir=tmp_path, monthly_emitter_available=True)

        assert running["state"] in ws._DRC_RUNNING_STATES, \
            "the state the DRC publishes must be one the workflow owner calls RUNNING"
        assert ws.OPERATOR_ACTION_BY_OVERALL[ws.RESEARCH_CYCLE_RUNNING] == ws.OP_ACTION_MONITOR
        assert ws.OP_ACTION_MONITOR in ws.OPERATOR_ACTIONS
        assert ws.OPERATOR_ACTION_BY_OVERALL[ws.RESEARCH_CYCLE_RUNNING] != "RUN_PORTFOLIO_CYCLE", \
            "a cycle already in flight is work in progress, not a new instruction"

    def test_not_started_is_what_the_defect_published_and_is_not_a_running_state(self):
        from paper_trader.api import workflow_state as ws
        assert drc.NOT_STARTED not in ws._DRC_RUNNING_STATES, \
            "NOT_STARTED routed the operator to RUN — that is why the mislabel mattered"

    def test_the_orchestrator_refuses_by_name_and_writes_nothing(self):
        """'Run the Portfolio Cycle' must never be offered against a live run without
        naming it. The close branch already did; the research branch said nothing."""
        from paper_trader.api import portfolio_cycle as pc

        out = pc.plan_next_step({
            "overall_state": "RESEARCH_CYCLE_RUNNING",
            "research_cycle_state": {"run_id": RUN_ID,
                                     "eligible_market_date": "2026-09-23",
                                     "current_step": "CAPTURE_FORWARD_EVIDENCE"}})
        assert out["step"] is None, "a refused cycle must delegate no step"
        assert out["owner"] is None
        assert out["stop_reason"] == pc.STOP_CYCLE_ALREADY_RUNNING
        assert RUN_ID in out["reason"], "the operator must be shown the actual RUN_ID"
        assert "CAPTURE_FORWARD_EVIDENCE" in out["reason"], "…and where it has got to"
        assert "2026-09-23" in out["reason"]

    def test_the_orchestrator_refusal_survives_a_payload_with_no_run_identity(self):
        """Degrade-safe: a missing identity block must still refuse, never crash."""
        from paper_trader.api import portfolio_cycle as pc

        out = pc.plan_next_step({"overall_state": "RESEARCH_CYCLE_RUNNING"})
        assert out["stop_reason"] == pc.STOP_CYCLE_ALREADY_RUNNING
        assert out["step"] is None


# =========================================================================== #
# 5. THE 90-SECOND READ — one composition per identity, not one per caller
# =========================================================================== #
class _FakeCompose:
    """A composition that is slow enough to overlap and counts how often it runs."""

    def __init__(self, *, seconds=0.30, raises=False):
        self.calls = 0
        self.seconds = seconds
        self.raises = raises
        self._lock = threading.Lock()

    def __call__(self, identity):
        with self._lock:
            self.calls += 1
        time.sleep(self.seconds)
        if self.raises:
            raise RuntimeError("composition failed")
        return {"computed_at": "2026-09-24T17:00:00+00:00",
                "identity": {"identity_hash": identity["identity_hash"]},
                "sections": {"workflow": {"overall_state": "RESEARCH_CYCLE_RUNNING"}}}


@pytest.fixture
def snap(monkeypatch):
    """A hermetic snapshot: a fixed identity, an injected composition, clean memo."""
    ident = {"schema_version": ds.SCHEMA_VERSION, "eligible_market_date": "2026-09-23",
             "identity_hash": "a" * 64}
    monkeypatch.setattr(ds, "snapshot_identity", lambda: dict(ident))
    ds.reset()
    yield ident
    ds.reset()


class TestSnapshotSingleFlight:

    def test_concurrent_misses_compose_exactly_once(self, snap, monkeypatch):
        """The measured defect: eleven snapshot-backed routes, eleven compositions."""
        fake = _FakeCompose(seconds=0.35)
        monkeypatch.setattr(ds, "_compose", fake)

        results, errors = [], []

        def _go():
            try:
                results.append(ds.load_decision_snapshot())
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=_go) for _ in range(11)]
        started = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        elapsed = time.perf_counter() - started

        assert not errors, errors
        assert len(results) == 11
        assert fake.calls == 1, \
            "eleven concurrent readers must share ONE composition, not run eleven"
        assert elapsed < 0.35 * 5, \
            "the readers must overlap, not queue behind one another"
        assert any(r["served_from"] == "SNAPSHOT_COALESCED_WITH_IN_FLIGHT_BUILD"
                   for r in results)

    def test_a_coalesced_reader_gets_the_identity_it_asked_for(self, snap, monkeypatch):
        """Coalescing must not serve anything staler: the payload a waiter receives was
        composed under the very identity that waiter computed."""
        monkeypatch.setattr(ds, "_compose", _FakeCompose(seconds=0.25))
        out = []
        threads = [threading.Thread(target=lambda: out.append(ds.load_decision_snapshot()))
                   for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert len(out) == 6
        for payload in out:
            assert payload["identity"]["identity_hash"] == snap["identity_hash"]

    def test_a_failing_build_releases_every_waiter(self, snap, monkeypatch):
        """A build that raises must never leave readers blocked until their timeout."""
        monkeypatch.setattr(ds, "_compose", _FakeCompose(seconds=0.20, raises=True))
        monkeypatch.setattr(ds, "SINGLE_FLIGHT_WAIT_SECONDS", 20.0)
        errors = []

        def _go():
            try:
                ds.load_decision_snapshot()
            except Exception as exc:  # noqa: BLE001
                errors.append(type(exc).__name__)

        threads = [threading.Thread(target=_go) for _ in range(4)]
        started = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        elapsed = time.perf_counter() - started

        assert len(errors) == 4, "every caller must learn the build failed"
        assert elapsed < 10, "waiters must be released on failure, not left to time out"
        assert ds._INFLIGHT == {}, "a failed build must leave no ticket behind"

    def test_force_never_serves_or_publishes_a_shared_build(self, snap, monkeypatch):
        """An explicit refresh must compose for itself."""
        fake = _FakeCompose(seconds=0.05)
        monkeypatch.setattr(ds, "_compose", fake)
        first = ds.load_decision_snapshot()
        assert first["served_from"] == "REGENERATED_FROM_CANONICAL_OWNERS"
        forced = ds.load_decision_snapshot(force=True)
        assert forced["served_from"] == "REGENERATED_FROM_CANONICAL_OWNERS"
        assert fake.calls == 2
        assert ds._INFLIGHT == {}

    def test_the_fast_hit_path_is_unchanged(self, snap, monkeypatch):
        fake = _FakeCompose(seconds=0.02)
        monkeypatch.setattr(ds, "_compose", fake)
        ds.load_decision_snapshot()
        hit = ds.load_decision_snapshot()
        assert hit["served_from"] == "SNAPSHOT_IDENTITY_MATCH"
        assert fake.calls == 1

    def test_a_changed_identity_still_regenerates(self, monkeypatch):
        """The invalidation RULE is untouched — nothing whose inputs moved is served."""
        ds.reset()
        try:
            state = {"h": "1" * 64}
            monkeypatch.setattr(ds, "snapshot_identity",
                                lambda: {"schema_version": ds.SCHEMA_VERSION,
                                         "eligible_market_date": "2026-09-23",
                                         "identity_hash": state["h"]})
            fake = _FakeCompose(seconds=0.02)
            monkeypatch.setattr(ds, "_compose", fake)
            ds.load_decision_snapshot()
            state["h"] = "2" * 64                      # a store moved
            out = ds.load_decision_snapshot()
            assert fake.calls == 2
            assert out["served_from"] == "REGENERATED_FROM_CANONICAL_OWNERS"
            assert out["identity"]["identity_hash"] == "2" * 64
        finally:
            ds.reset()

    def test_reset_releases_stranded_waiters(self, snap):
        ticket = {"event": threading.Event(), "payload": None, "waiters": 1}
        ds._INFLIGHT["zzz"] = ticket
        ds.reset()
        assert ds._INFLIGHT == {}
        assert ticket["event"].is_set(), "a reset must not strand a blocked reader"


# =========================================================================== #
# 6. SAFETY — the repair moves no decision authority
# =========================================================================== #
class TestSafety:

    def test_neither_repair_executes_approves_or_orders(self):
        for name, src in (("daily_research_cycle", DRC_SRC), ("decision_snapshot", SNAP_SRC)):
            low = src.lower()
            for forbidden in ("create_order", "submit_order", "place_order",
                              "execute_order", "auto_approve", "automation_enabled"):
                assert forbidden not in low, "%s introduced %s" % (name, forbidden)

    def test_the_snapshot_remains_a_read_model(self):
        assert '"read_only": True' in SNAP_SRC
        assert '"recomputes_nothing": True' in SNAP_SRC
        assert "_compose" in SNAP_SRC

    def test_the_r69_3_running_close_projection_is_untouched(self):
        """R69.4 must not disturb the running-close repair released alongside it."""
        close_src = (ROOT / "api" / "daily_close.py").read_text(encoding="utf-8")
        assert "def active_close_run(" in close_src
        assert 'CLOSE_RUNNING = "DAILY_CLOSE_RUNNING"' in close_src

    def test_the_status_read_declares_no_new_write(self):
        assert "def load_daily_research_cycle_status(" in DRC_SRC
        # the status branch publishes liveness; it must not acquire, stamp or clear
        branch = DRC_SRC.split("def load_daily_research_cycle_status(")[1].split("\ndef ")[0]
        for forbidden in ("_write_lock(", "_clear_lock(", "_start_heartbeat(",
                          "_stamp_heartbeat("):
            assert forbidden not in branch, \
                "the read path must never touch the lock (%s)" % forbidden


# --------------------------------------------------------------------------- #
# Hermetic freshness (mirrors tests/test_slice3_daily_research_cycle.py).
# --------------------------------------------------------------------------- #
_OP = {"operational_book": {
    "book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1",
    "current_status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
    "nav_as_of_date": "2026-08-04", "desk_mark_date": "2026-08-04",
    "latest_desk_mark_date": "2026-08-04", "nav": 102000.0, "cash": 1500.0,
    "holdings_count": 25, "pending_order_count": 0,
    "current_target": {"alpha_market_date": "2026-08-04",
                       "latest_completed_market_date": "2026-08-04"}}}
_DESK = {"series": {"SPY": [["2026-07-31", 747.0], ["2026-08-04", 771.3]]},
         "latest_completed_date": "2026-08-04"}
_CLOSE = {"market_date": "2026-08-04", "done": True,
          "final_close_status": "DAILY_CLOSE_COMPLETE_HOLD", "status": "CLOSE_FINISHED"}
_FWD = {"latest_snapshot_date": "2026-08-04", "snapshot_count": 6, "evidence_state": "X"}
_DAILY = {"status": "DAILY_STATUS_READY", "latest_valid_mark_date": "2026-08-04"}


def _freshness(reference_today="2026-08-05"):
    return df.load_data_freshness(
        reference_today=reference_today, operational=copy.deepcopy(_OP),
        inputs={"market_as_of_date": "2026-08-04", "momentum_month": "2026-08",
                "fundamental_as_of_date": "2026-05-22"},
        daily_status=dict(_DAILY), desk_marks=copy.deepcopy(_DESK),
        daily_close_status=dict(_CLOSE), forward_status=copy.deepcopy(_FWD))
