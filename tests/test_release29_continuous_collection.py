"""Release 29 — continuous governed information collection.

WHAT THESE TESTS PROTECT
------------------------
Release 28 could react to an event. It could not keep events arriving, and the
health it reported was measured against a denominator that made a *monthly*
series and a market feed *on a Sunday* both read "degraded". Release 29 runs the
sources at their own cadence and reports health against the sources that should
actually be current now. Each test below is one way that could quietly become
wrong again:

* a 15-minute lane re-collected every wake, or a monthly lane probed hourly;
* a market feed on a weekend reported STALE instead of NOT_DUE;
* a provider 429 answered with an immediate retry;
* one failing source stopping the others;
* a second worker started by hand, running provider calls concurrently;
* a wake-from-sleep turning into a request storm;
* a delayed quote counted as a "material company event" every 15 minutes;
* a same-session collapse waiting for tomorrow's bar to reach the review list;
* a read surface counting "material events" by a definition the gate does not use;
* collection automation drifting into execution automation.

HERMETIC
--------
Every test runs against ``tmp_path`` roots and an injected clock. Nothing here
opens a production store, calls a provider, starts a worker, touches the Windows
Scheduled Task, creates an order or mutates operational state.
"""
from __future__ import annotations

import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

import pytest

from paper_trader.api import collection_replay as creplay
from paper_trader.api import event_fabric as fabric
from paper_trader.api import event_replay as replay
from paper_trader.api import event_signal_refresh as esr
from paper_trader.api import information_collection as ic
from paper_trader.engine import collection_cadence as cad
from paper_trader.engine import event_fabric as ek
from paper_trader.engine import event_materiality as emat
from paper_trader.engine import market_hours as mh

REPO = __import__("pathlib").Path(__file__).resolve().parents[1]

# A Monday inside the regular session, and the Sunday before it.
OPEN_UTC = datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc)     # 10:00 ET Mon
WEEKEND_UTC = datetime(2026, 8, 16, 16, 0, tzinfo=timezone.utc)  # 12:00 ET Sun
EVENING_UTC = datetime(2026, 8, 17, 23, 0, tzinfo=timezone.utc)  # 19:00 ET Mon


@pytest.fixture()
def root(tmp_path):
    out = tmp_path / "collection"
    out.mkdir(parents=True, exist_ok=True)
    return out


def _policy(source_id):
    return cad.CADENCE_POLICY_BY_ID[source_id]


# --------------------------------------------------------------------------- #
# 1. Cadence policy — every interval is declared and justified
# --------------------------------------------------------------------------- #
class TestCadencePolicy:

    def test_every_source_declares_a_kind_a_session_and_a_reason(self):
        for sid, policy in cad.CADENCE_POLICY_BY_ID.items():
            assert policy["cadence_kind"] in cad.CADENCE_KINDS, sid
            assert policy["market_session_requirement"] in cad.SESSION_REQUIREMENTS, sid
            # A cadence without a stated reason is a number someone liked.
            assert str(policy.get("why") or "").strip(), sid

    def test_a_collected_source_declares_an_interval_and_a_budget(self):
        for sid, policy in cad.CADENCE_POLICY_BY_ID.items():
            if not policy["collection_enabled"]:
                continue
            assert policy["normal_interval_seconds"] > 0, sid
            budget = policy["request_budget"]
            assert budget["max_calls_per_iteration"] >= 1, sid
            assert budget["timeout_seconds"] > 0, sid
            assert budget["max_retries"] >= 0, sid

    def test_blocked_and_redundant_sources_are_not_collected(self):
        for sid in ("analyst_revision_vendor", "options_iv", "bls", "bea",
                    "prediction_service"):
            assert cad.CADENCE_POLICY_BY_ID[sid]["collection_enabled"] is False, sid

    def test_the_policy_contract_is_serialisable_evidence(self):
        contract = cad.policy_contract()
        assert contract["contract_id"] == cad.CADENCE_CONTRACT_ID
        assert contract["policy_version"] == cad.CADENCE_POLICY_VERSION
        assert set(contract["runtime_states"]) == set(cad.RUNTIME_STATES)


# --------------------------------------------------------------------------- #
# 2. The fixed freshness denominator
# --------------------------------------------------------------------------- #
class TestFreshnessDenominator:

    def test_a_market_feed_is_not_due_on_a_weekend_not_stale(self):
        row = cad.resolve_source_runtime(
            policy=_policy("yahoo_delayed_quote"), state={}, now=WEEKEND_UTC)
        assert row["runtime_state"] == cad.RS_NOT_DUE
        assert row["due_window_active"] is False
        assert row["session_phase"] == mh.PHASE_WEEKEND
        assert row["collect_now"] is False

    def test_the_same_market_feed_is_due_once_the_session_opens(self):
        row = cad.resolve_source_runtime(
            policy=_policy("yahoo_delayed_quote"), state={}, now=OPEN_UTC)
        assert row["due_window_active"] is True
        assert row["collect_now"] is True

    def test_a_publisher_driven_lane_is_due_on_a_sunday(self):
        row = cad.resolve_source_runtime(policy=_policy("news_rss"), state={},
                                         now=WEEKEND_UTC)
        assert row["due_window_active"] is True

    def test_a_daily_lane_is_not_due_before_its_publication_window(self):
        morning = cad.resolve_source_runtime(policy=_policy("finra"), state={},
                                             now=OPEN_UTC)
        evening = cad.resolve_source_runtime(policy=_policy("finra"), state={},
                                             now=EVENING_UTC)
        assert morning["runtime_state"] == cad.RS_NOT_DUE
        assert evening["due_window_active"] is True

    def test_the_denominator_is_the_sources_that_should_be_current_now(self):
        rows = [cad.resolve_source_runtime(policy=p, state={}, now=WEEKEND_UTC)
                for p in cad.CADENCE_POLICY_BY_ID.values()]
        summary = cad.summarize_runtime(rows)
        assert summary["due_now"] == summary["healthy_due"]
        assert summary["due_now"] < summary["total_sources"]
        assert "should be current now" in summary["headline"]
        # Nothing blocked or disabled may enter the operator's denominator.
        assert summary["blocked"] >= 1 and summary["disabled"] >= 1

    def test_a_collected_source_is_fresh_not_due_and_never_stale_mid_interval(self):
        state = {"last_attempt_at": OPEN_UTC.isoformat(),
                 "last_success_at": OPEN_UTC.isoformat()}
        row = cad.resolve_source_runtime(policy=_policy("sec_edgar"), state=state,
                                         now=OPEN_UTC + timedelta(minutes=5))
        assert row["collect_now"] is False
        assert row["runtime_state"] in cad.HEALTHY_STATES

    def test_the_next_wake_is_bounded_at_both_ends(self):
        rows = [cad.resolve_source_runtime(policy=p, state={}, now=WEEKEND_UTC)
                for p in cad.CADENCE_POLICY_BY_ID.values()]
        wake = cad.next_wake_seconds(rows, now=WEEKEND_UTC)
        assert cad.MIN_ITERATION_INTERVAL_SECONDS <= wake <= cad.MAX_WAKE_SECONDS


# --------------------------------------------------------------------------- #
# 3. Backoff and the circuit breaker — a 429 means STOP ASKING
# --------------------------------------------------------------------------- #
class TestBackoffAndCircuit:

    def test_http_status_maps_to_an_explicit_error_category(self):
        assert cad.classify_http_error(status=429) == cad.ERR_RATE_LIMIT
        assert cad.classify_http_error(status=503) == cad.ERR_SERVER
        assert cad.classify_http_error(status=403) == cad.ERR_AUTH
        assert cad.classify_http_error(status=404) == cad.ERR_CLIENT
        assert cad.classify_http_error(detail="timed out") == cad.ERR_TIMEOUT

    def test_a_rate_limit_opens_a_long_backoff_never_an_immediate_retry(self):
        first = cad.backoff_seconds(category=cad.ERR_RATE_LIMIT,
                                    consecutive_failures=1)
        assert first >= 900.0

    def test_backoff_doubles_and_is_clamped_at_the_ceiling(self):
        seq = [cad.backoff_seconds(category=cad.ERR_SERVER, consecutive_failures=n)
               for n in range(1, 12)]
        assert seq[1] > seq[0]
        assert max(seq) <= cad.BACKOFF_CEILING_SECONDS[cad.ERR_SERVER]

    def test_an_entitlement_failure_backs_off_for_hours_not_minutes(self):
        assert cad.backoff_seconds(category=cad.ERR_AUTH,
                                   consecutive_failures=1) >= 6 * 3600.0

    def test_the_circuit_opens_after_the_threshold_and_probes_once(self):
        now = OPEN_UTC
        assert cad.circuit_state_for(consecutive_failures=1, backoff_until=None,
                                     now=now) == cad.CIRCUIT_CLOSED
        assert cad.circuit_state_for(
            consecutive_failures=cad.CIRCUIT_OPEN_THRESHOLD,
            backoff_until=now + timedelta(minutes=5), now=now) == cad.CIRCUIT_OPEN
        assert cad.circuit_state_for(
            consecutive_failures=cad.CIRCUIT_OPEN_THRESHOLD,
            backoff_until=now - timedelta(minutes=5), now=now) == cad.CIRCUIT_HALF_OPEN

    def test_a_source_in_backoff_is_not_called(self):
        state = {"last_attempt_at": OPEN_UTC.isoformat(),
                 "consecutive_failures": 3,
                 "backoff_until": (OPEN_UTC + timedelta(hours=1)).isoformat()}
        row = cad.resolve_source_runtime(policy=_policy("gdelt"), state=state,
                                         now=OPEN_UTC + timedelta(minutes=30))
        assert row["runtime_state"] == cad.RS_BACKOFF
        assert row["collect_now"] is False
        # The operator is told WHEN it will be retried, while the service still runs.
        assert row["backoff_until"]


# --------------------------------------------------------------------------- #
# 4. Provider budgets
# --------------------------------------------------------------------------- #
class TestProviderBudget:

    def test_a_source_over_its_daily_budget_is_skipped_with_a_reason(self):
        policy = _policy("gdelt")
        cap = policy["request_budget"]["max_calls_per_day"]
        if cap is None:
            pytest.skip("gdelt declares no daily cap")
        verdict = ic.budget_verdict(policy=policy,
                                    row={"request_count_today": cap}, now=OPEN_UTC)
        assert verdict["allowed"] is False
        assert "budget" in verdict["reason"].lower()

    def test_an_hourly_budget_counts_only_the_last_hour(self):
        policy = _policy("gdelt")
        cap = policy["request_budget"]["max_calls_per_hour"]
        if cap is None:
            pytest.skip("gdelt declares no hourly cap")
        stale = [(OPEN_UTC - timedelta(hours=3)).isoformat()] * (cap + 2)
        assert ic.budget_verdict(policy=policy,
                                 row={"recent_call_times": stale},
                                 now=OPEN_UTC)["allowed"] is True
        fresh = [(OPEN_UTC - timedelta(minutes=5)).isoformat()] * cap
        assert ic.budget_verdict(policy=policy,
                                 row={"recent_call_times": fresh},
                                 now=OPEN_UTC)["allowed"] is False

    def test_daily_counters_roll_on_the_market_day_not_the_utc_day(self):
        row = {"counter_et_date": "2026-08-14", "request_count_today": 40}
        rolled = ic._reset_daily_counters(row, OPEN_UTC)
        assert rolled["request_count_today"] == 0
        assert rolled["counter_et_date"] == mh.session_state(OPEN_UTC)["et_date"]


# --------------------------------------------------------------------------- #
# 5. Singleton, heartbeat and restart
# --------------------------------------------------------------------------- #
class TestSingletonAndLifecycle:

    def test_a_second_worker_is_refused_while_the_first_is_alive(self, root):
        first = ic.acquire_service_lock(root=root, instance_id="w1")
        second = ic.acquire_service_lock(root=root, instance_id="w2", pid=1)
        assert first["acquired"] is True
        assert second["acquired"] is False
        assert second["reason"] == "SINGLE_FLIGHT_LOCK_HELD"

    def test_an_abandoned_lock_is_reclaimed_only_after_silence_and_a_dead_pid(
            self, root):
        now = OPEN_UTC
        ic.acquire_service_lock(root=root, instance_id="dead", pid=999_999_999,
                                now=now)
        # Still inside the takeover window: refused even though the pid is gone.
        soon = ic.acquire_service_lock(
            root=root, instance_id="new", pid=4242,
            now=now + timedelta(seconds=ic.LOCK_TAKEOVER_SECONDS / 2))
        assert soon["acquired"] is False
        later = ic.acquire_service_lock(
            root=root, instance_id="new", pid=4242,
            now=now + timedelta(seconds=ic.LOCK_TAKEOVER_SECONDS + 60))
        assert later["acquired"] is True
        assert later["reclaimed"]["prior_instance_id"] == "dead"

    def test_lifecycle_never_started_running_degraded_stopped(self, root):
        now = OPEN_UTC
        blank = ic.load_service_state(root=root)
        assert ic.resolve_service_lifecycle(blank, None, now)["service_state"] == \
            ic.SVC_NEVER_STARTED

        # A RUNNING verdict means a live worker, so the pid has to be a live
        # one. A made-up pid only ever read as "running" because the liveness
        # probe could not answer on Windows.
        ic.register_worker_start(root=root, instance_id="w1", pid=os.getpid(),
                                 now=now)
        ic.heartbeat(root=root, instance_id="w1", now=now)
        state = ic.load_service_state(root=root)
        lock = {"pid": os.getpid()}
        assert ic.resolve_service_lifecycle(state, lock, now)["service_state"] == \
            ic.SVC_RUNNING

        stale = now + timedelta(seconds=ic.HEARTBEAT_STALE_SECONDS + 60)
        assert ic.resolve_service_lifecycle(state, lock, stale)["service_state"] == \
            ic.SVC_DEGRADED

        ic.release_service_lock(root=root, instance_id="w1")
        assert ic.resolve_service_lifecycle(
            ic.load_service_state(root=root), None, now)["service_state"] == \
            ic.SVC_STOPPED

    def test_a_restart_counts_itself_without_losing_prior_evidence(self, root):
        ic.register_worker_start(root=root, instance_id="w1", pid=1, now=OPEN_UTC)
        ic.register_worker_start(root=root, instance_id="w2", pid=2,
                                 now=OPEN_UTC + timedelta(minutes=5))
        state = ic.load_service_state(root=root)
        assert state["restart_count"] == 1
        assert state["instance_id"] == "w2"


# --------------------------------------------------------------------------- #
# 5a. Release 29.2 — HEALTHY/BUSY is not the same thing as STALLED
# --------------------------------------------------------------------------- #
class TestBusyWorkerIsNotAStalledWorker:
    """Regression for a healthy worker reported DEGRADED while it was working.

    Measured in the Release-29.1 acceptance run: one collection iteration took
    341.85 seconds (245.41s of it inside a single ``INGEST_SINCE_WATERMARK``
    step), the heartbeat was stamped ONCE before the iteration began, and health
    read DEGRADED at a heartbeat age of ~309 seconds while the worker was alive
    and legitimately busy.

    The repair is not a wider tolerance — ``PROGRESS_STALL_SECONDS`` is
    deliberately equal to ``HEARTBEAT_STALE_SECONDS``. What changed is what the
    clock measures: an OPEN iteration is judged on progress evidence that has to
    keep advancing, and anything unprovable still fails closed.
    """

    def _running_worker(self, root, now):
        ic.register_worker_start(root=root, instance_id="w1", pid=os.getpid(),
                                 now=now)
        ic.heartbeat(root=root, instance_id="w1", now=now)
        return {"pid": os.getpid()}

    def test_the_stall_budget_was_not_widened(self):
        assert ic.PROGRESS_STALL_SECONDS == ic.HEARTBEAT_STALE_SECONDS

    def test_a_long_iteration_that_keeps_advancing_stays_healthy_and_busy(self, root):
        now = OPEN_UTC
        lock = self._running_worker(root, now)
        ic.record_progress(root=root, step="ITERATION_BEGIN", detail="it1",
                           iteration_id="it1", instance_id="w1",
                           in_flight=True, now=now)
        # 341.85s of real work, reporting progress as it goes.
        ic.record_progress(root=root, step="EVENT_CYCLE",
                           detail="INGEST_SINCE_WATERMARK file 214",
                           iteration_id="it1", instance_id="w1",
                           now=now + timedelta(seconds=300))
        state = ic.load_service_state(root=root)
        # The EXACT moment Release 29.1 reported DEGRADED.
        verdict = ic.resolve_service_lifecycle(state, lock,
                                               now + timedelta(seconds=309))
        assert verdict["heartbeat_age_seconds"] > ic.HEARTBEAT_STALE_SECONDS
        assert verdict["service_state"] == ic.SVC_RUNNING
        assert verdict["worker_activity"] == ic.ACT_BUSY
        assert verdict["iteration_in_flight"] is True

    def test_progress_evidence_actually_advances(self, root):
        now = OPEN_UTC
        self._running_worker(root, now)
        seqs, stamps = [], []
        for n in range(4):
            ic.record_progress(root=root, step="COLLECT_STAGE2",
                               detail="source %d" % n, iteration_id="it1",
                               now=now + timedelta(seconds=30 * n))
            state = ic.load_service_state(root=root)
            seqs.append(state["progress_seq"])
            stamps.append(state["progress_at"])
        assert seqs == sorted(seqs) and len(set(seqs)) == 4
        assert stamps == sorted(stamps) and len(set(stamps)) == 4

    def test_the_sequence_never_steps_backwards_across_iterations(self, root,
                                                                  tmp_path):
        """Two counters write this field; only one of them may decide it.

        The worker's reporter is long-lived, so its count is a LIFETIME total,
        while the orchestrator increments the document once when it opens an
        iteration and once when it closes one. Letting the reporter's number win
        outright made the sequence step BACKWARDS by one at every iteration
        boundary — falsifying the single property the sequence exists to prove.
        """
        ic.set_collection_automation(enabled=True, confirm=ic.ENABLE_CONFIRM_TOKEN,
                                     root=root)
        ic.register_worker_start(root=root, instance_id="w1", pid=os.getpid(),
                                 now=OPEN_UTC)
        trail: list = []

        def tracing_writer(**kw):
            ic.record_progress(**kw)
            trail.append(ic.load_service_state(root=root)["progress_seq"])

        reporter = ic.ProgressReporter(root=root, instance_id="w1",
                                       min_write_seconds=0.0,
                                       writer=tracing_writer)
        for n in range(3):
            ic.run_collection_iteration(
                root=root, instance_id="w1",
                now=OPEN_UTC + timedelta(minutes=20 * n), progress_fn=reporter,
                attention_universe={"tiers": {}, "holdings": [], "candidates": [],
                                    "warnings": []},
                stage2_fn=lambda **kw: {"status": "OK", "counts": {},
                                        "source_states": {}},
                news_fn=lambda **kw: {"status": "OK", "counts": {}},
                event_cycle_fn=lambda **kw: {"state": "NO_NEW_INFORMATION"},
                ingestion_root=tmp_path / "ing", news_root=tmp_path / "news")
            trail.append(ic.load_service_state(root=root)["progress_seq"])
        assert len(trail) >= 9, trail
        assert all(b > a for a, b in zip(trail, trail[1:])), trail

    def test_an_ordinary_checkpoint_never_closes_an_open_iteration(self, root):
        """Regression for a defect the first draft of this release shipped.

        ``record_progress`` defaulted ``in_flight`` to the caller's flag, so every
        checkpoint re-answered a question it does not own: the orchestrator
        opened the iteration and the very next HTTP checkpoint closed it again.
        The live worker then read RUNNING/IDLE for the whole of a real
        316-second pass — the BUSY verdict existed but could never be reached.
        Whether an iteration is open is the ORCHESTRATOR'S fact.
        """
        now = OPEN_UTC
        self._running_worker(root, now)
        ic.record_progress(root=root, step="ITERATION_BEGIN", detail="it1",
                           iteration_id="it1", in_flight=True, now=now)
        for step in ("COLLECT_STAGE2", "EVENT_CYCLE", "CORPUS_SCAN"):
            ic.record_progress(root=root, step=step, detail="unit of work",
                               now=now + timedelta(seconds=1))
            state = ic.load_service_state(root=root)
            assert state["iteration_in_flight"] is True, step
            assert state["current_iteration_id"] == "it1", step
        ic.record_progress(root=root, step="ITERATION_END", detail="it1",
                           in_flight=False, now=now + timedelta(seconds=2))
        closed = ic.load_service_state(root=root)
        assert closed["iteration_in_flight"] is False
        assert closed["current_iteration_id"] is None

    def test_work_that_stops_progressing_becomes_degraded_and_stalled(self, root):
        now = OPEN_UTC
        lock = self._running_worker(root, now)
        ic.record_progress(root=root, step="COLLECT_STAGE2", detail="sec_edgar",
                           iteration_id="it1", in_flight=True, now=now)
        state = ic.load_service_state(root=root)
        late = now + timedelta(seconds=ic.PROGRESS_STALL_SECONDS + 1)
        verdict = ic.resolve_service_lifecycle(state, lock, late)
        assert verdict["service_state"] == ic.SVC_DEGRADED
        assert verdict["worker_activity"] == ic.ACT_STALLED
        assert "COLLECT_STAGE2" in verdict["reason"]

    def test_an_open_iteration_with_no_progress_evidence_fails_closed(self, root):
        now = OPEN_UTC
        lock = self._running_worker(root, now)
        state = ic.load_service_state(root=root)
        state["iteration_in_flight"] = True
        state["progress_at"] = None
        verdict = ic.resolve_service_lifecycle(state, lock, now)
        assert verdict["service_state"] == ic.SVC_DEGRADED
        assert verdict["worker_activity"] == ic.ACT_UNKNOWN

    def test_a_dead_worker_is_still_detected_while_an_iteration_is_open(self, root):
        now = OPEN_UTC
        self._running_worker(root, now)
        ic.record_progress(root=root, step="EVENT_CYCLE", detail="mid-cycle",
                           iteration_id="it1", in_flight=True, now=now)
        state = ic.load_service_state(root=root)
        # Fresh progress, but the process is gone: DEAD outranks BUSY.
        verdict = ic.resolve_service_lifecycle(state, {"pid": 999_999_999}, now)
        assert verdict["service_state"] == ic.SVC_DEGRADED
        assert verdict["worker_activity"] == ic.ACT_DEAD

    def test_a_completed_iteration_returns_to_healthy_and_idle(self, root):
        now = OPEN_UTC
        lock = self._running_worker(root, now)
        ic.record_progress(root=root, step="COLLECT_STAGE2", detail="sec_edgar",
                           iteration_id="it1", in_flight=True, now=now)
        done = now + timedelta(seconds=120)
        ic.record_progress(root=root, step="ITERATION_END", detail="it1",
                           iteration_id="it1", in_flight=False, now=done)
        ic.heartbeat(root=root, instance_id="w1", now=done)
        state = ic.load_service_state(root=root)
        verdict = ic.resolve_service_lifecycle(state, lock,
                                               done + timedelta(seconds=5))
        assert verdict["service_state"] == ic.SVC_RUNNING
        assert verdict["worker_activity"] == ic.ACT_IDLE
        assert verdict["iteration_in_flight"] is False

    def test_a_worker_between_iterations_that_stops_waking_is_still_caught(
            self, root):
        now = OPEN_UTC
        lock = self._running_worker(root, now)
        state = ic.load_service_state(root=root)
        stale = now + timedelta(seconds=ic.HEARTBEAT_STALE_SECONDS + 1)
        verdict = ic.resolve_service_lifecycle(state, lock, stale)
        assert verdict["service_state"] == ic.SVC_DEGRADED
        assert verdict["worker_activity"] == ic.ACT_STALLED

    def test_a_restart_never_inherits_the_dead_workers_open_iteration(self, root):
        now = OPEN_UTC
        self._running_worker(root, now)
        ic.record_progress(root=root, step="EVENT_CYCLE", detail="mid-cycle",
                           iteration_id="it1", in_flight=True, now=now)
        assert ic.load_service_state(root=root)["iteration_in_flight"] is True
        ic.register_worker_start(root=root, instance_id="w2", pid=os.getpid(),
                                 now=now + timedelta(minutes=1))
        fresh = ic.load_service_state(root=root)
        assert fresh["iteration_in_flight"] is False
        assert fresh["progress_at"] is None and fresh["progress_seq"] == 0
        assert fresh["restart_count"] == 1

    def test_a_graceful_stop_closes_the_iteration(self, root):
        now = OPEN_UTC
        ic.acquire_service_lock(root=root, instance_id="w1", now=now)
        self._running_worker(root, now)
        ic.record_progress(root=root, step="EVENT_CYCLE", iteration_id="it1",
                           in_flight=True, now=now)
        ic.release_service_lock(root=root, instance_id="w1")
        state = ic.load_service_state(root=root)
        assert state["iteration_in_flight"] is False
        assert ic.resolve_service_lifecycle(state, None, now)["service_state"] == \
            ic.SVC_STOPPED

    def test_the_reporter_advances_every_checkpoint_and_throttles_the_write(self):
        writes: list = []
        ticks = iter([0.0, 0.0, 1.0, 2.0, 99.0])
        rep = ic.ProgressReporter(root=None, instance_id="w1",
                                  min_write_seconds=10.0,
                                  clock=lambda: next(ticks),
                                  writer=lambda **kw: writes.append(kw))
        rep("CORPUS_SCAN", detail="file 1")   # first write
        rep("CORPUS_SCAN", detail="file 2")   # throttled
        rep("CORPUS_SCAN", detail="file 3")   # throttled
        rep("EVENT_CYCLE", detail="step")     # step CHANGED -> written
        rep("EVENT_CYCLE", detail="step 2")   # 96s later -> written
        assert rep.seq == 5, "every checkpoint advances the sequence"
        assert [w["step"] for w in writes] == ["CORPUS_SCAN", "EVENT_CYCLE",
                                               "EVENT_CYCLE"]
        assert [w["seq"] for w in writes] == [1, 4, 5]

    def test_a_raising_progress_callback_never_breaks_collection(self):
        def boom(step, detail=None):
            raise RuntimeError("observer exploded")
        safe = ic._safe_progress(boom)
        safe("COLLECT_STAGE2", detail="sec_edgar")   # must not raise
        fabric.emit_progress(boom, "CORPUS_SCAN", "file 1")

    def test_the_canonical_path_reports_progress_from_inside_the_iteration(
            self, root, tmp_path):
        """The callback is called by the ORCHESTRATOR, not by a timer."""
        seen: list = []
        rep = ic.ProgressReporter(root=root, instance_id="w1",
                                  min_write_seconds=0.0,
                                  writer=lambda **kw: seen.append(kw))
        ic.set_collection_automation(enabled=True, confirm=ic.ENABLE_CONFIRM_TOKEN,
                                     root=root)
        ic.register_worker_start(root=root, instance_id="w1", pid=os.getpid(),
                                 now=OPEN_UTC)
        rep.begin("it1")
        receipt = ic.run_collection_iteration(
            root=root, instance_id="w1", now=OPEN_UTC, progress_fn=rep,
            attention_universe={"tiers": {}, "holdings": [], "candidates": [],
                                "warnings": []},
            stage2_fn=lambda **kw: {"status": "OK", "counts": {},
                                    "source_states": {}},
            news_fn=lambda **kw: {"status": "OK", "counts": {}},
            event_cycle_fn=lambda **kw: {"state": "NO_NEW_INFORMATION"},
            ingestion_root=tmp_path / "ing", news_root=tmp_path / "news")
        assert receipt["ran"] is True
        assert receipt["progress_checkpoints"] >= 3
        assert receipt["progress_stall_after_seconds"] == ic.PROGRESS_STALL_SECONDS
        steps = [row["step"] for row in seen]
        assert "DUE_PLAN" in steps and "PERSIST_ITERATION" in steps
        # The iteration CLOSED itself, and the final state write ADVANCED the
        # evidence instead of rolling it back to the copy loaded at the top of
        # the pass. (The recording writer above never persists, so the durable
        # sequence here is the orchestrator's own open/close pair.)
        state = ic.load_service_state(root=root)
        assert state["iteration_in_flight"] is False
        assert state["current_iteration_id"] is None
        assert state["progress_step"] == "ITERATION_END"
        assert state["progress_seq"] >= 2

    def test_health_reads_busy_from_INSIDE_a_running_iteration(self, root, tmp_path):
        """Sample the live health surface from the middle of a real iteration.

        This is the end-to-end version of the Release-29.1 failure: the health a
        DIFFERENT process would read, taken while the orchestrator is still
        working, at a moment when the heartbeat is already older than its budget.
        """
        ic.set_collection_automation(enabled=True, confirm=ic.ENABLE_CONFIRM_TOKEN,
                                     root=root)
        ic.register_worker_start(root=root, instance_id="w1", pid=os.getpid(),
                                 now=OPEN_UTC)
        ic.heartbeat(root=root, instance_id="w1", now=OPEN_UTC)
        reporter = ic.ProgressReporter(root=root, instance_id="w1",
                                       min_write_seconds=0.0)
        seen: list = []

        def slow_cycle(**kwargs):
            # Three units of real work, sampled from outside as they complete.
            for n in range(3):
                reporter("EVENT_CYCLE", detail="INGEST_SINCE_WATERMARK file %d" % n)
                state = ic.load_service_state(root=root)
                # Two full stale budgets after the iteration started.
                late = OPEN_UTC + timedelta(
                    seconds=ic.HEARTBEAT_STALE_SECONDS * 2 + n)
                seen.append(ic.resolve_service_lifecycle(
                    state, {"pid": os.getpid()}, late))
            return {"state": "NO_NEW_INFORMATION"}

        ic.run_collection_iteration(
            root=root, instance_id="w1", now=OPEN_UTC, progress_fn=reporter,
            force_event_cycle=True,
            attention_universe={"tiers": {}, "holdings": [], "candidates": [],
                                "warnings": []},
            stage2_fn=lambda **kw: {"status": "OK", "counts": {},
                                    "source_states": {}},
            news_fn=lambda **kw: {"status": "OK", "counts": {}},
            event_cycle_fn=slow_cycle,
            ingestion_root=tmp_path / "ing", news_root=tmp_path / "news")

        assert len(seen) == 3
        for verdict in seen:
            assert verdict["iteration_in_flight"] is True
            assert verdict["heartbeat_age_seconds"] > ic.HEARTBEAT_STALE_SECONDS
            assert verdict["service_state"] == ic.SVC_RUNNING
            assert verdict["worker_activity"] == ic.ACT_BUSY
        assert [v["progress_seq"] for v in seen] == sorted(
            v["progress_seq"] for v in seen)
        # And the moment it finishes, it is idle again.
        after = ic.resolve_service_lifecycle(
            ic.load_service_state(root=root), {"pid": os.getpid()}, OPEN_UTC)
        assert after["worker_activity"] == ic.ACT_IDLE

    def test_the_event_orchestrator_reports_every_step(self, tmp_path):
        seen: list = []
        esr.run_event_signal_refresh(
            confirm=esr.EXECUTE_CONFIRM_TOKEN,
            fabric_dir=tmp_path / "fab", hoc_dir=tmp_path / "hoc",
            reassessment_dir=tmp_path / "rea", reallocation_dir=tmp_path / "rel",
            ingestion_root=tmp_path / "ing", news_root=tmp_path / "news",
            portfolio_state={"positions": [], "dates": {}, "active_book": {}},
            scoring={"rankings": []}, price_panel=None, corpus_events=[],
            progress_fn=lambda step, detail=None: seen.append((step, detail)))
        assert seen, "the cycle emitted no progress at all"
        assert {s for s, _ in seen} == {"EVENT_CYCLE"}
        named = " ".join(str(d) for _, d in seen)
        for step_id in ("LOAD_PORTFOLIO_CONTEXT", "INGEST_SINCE_WATERMARK",
                        "DEDUPLICATE_AND_PERSIST", "MATERIALITY_GATE"):
            assert step_id in named, step_id

    def test_the_corpus_scan_reports_per_file_not_only_per_lane(self, tmp_path):
        """The 245-second step must not be one opaque unit of progress."""
        tree = (tmp_path / "ing" / "normalized" / "FILING_EVENT"
                / "2026" / "08" / "17")
        tree.mkdir(parents=True, exist_ok=True)
        for n in range(3):
            (tree / ("part-%d.jsonl" % n)).write_text("\n", encoding="utf-8")
        seen: list = []
        fabric.ingest_corpus_lane(
            tickers=["AAA"], ingestion_root=tmp_path / "ing",
            news_root=tmp_path / "news",
            progress_fn=lambda step, detail=None: seen.append((step, detail)))
        files = [d for s, d in seen if s == "CORPUS_SCAN" and "file " in str(d)]
        assert len(files) == 3, seen

    def test_the_read_contract_publishes_the_activity_verdict(self, root):
        ic.register_worker_start(root=root, instance_id="w1", pid=os.getpid(),
                                 now=OPEN_UTC)
        ic.record_progress(root=root, step="EVENT_CYCLE", detail="mid-cycle",
                           iteration_id="it1", in_flight=True, now=OPEN_UTC)
        payload = ic.load_information_collection(root=root, now=OPEN_UTC)
        svc = payload["service"]
        # Every value the browser shows is decided here, not in the browser.
        for key in ("worker_activity", "worker_activity_reason",
                    "progress_at", "progress_age_seconds", "progress_seq",
                    "progress_step", "iteration_in_flight",
                    "progress_stall_after_seconds"):
            assert key in svc, key
        assert svc["worker_activity"] == ic.ACT_BUSY
        assert svc["worker_activity_vocabulary"] == list(ic.WORKER_ACTIVITY_STATES)
        assert payload["headline"]["title"] == "COLLECTION RUNNING"
        assert payload["safety"]["execution_automation_enabled"] is False

    def test_the_worker_hands_the_orchestrator_its_progress_callback(self):
        worker = (REPO / "scripts" / "run_information_collection_service.py"
                  ).read_text(encoding="utf-8", errors="replace")
        assert "ic.ProgressReporter(" in worker
        assert "progress_fn=progress" in worker
        # No second heartbeat authority: nothing here may spawn a timer thread.
        for forbidden in ("threading.Thread", "threading.Timer", "Thread(",
                          "asyncio."):
            assert forbidden not in worker, forbidden

    def test_progress_evidence_manufactures_no_duplicate_artifacts(
            self, root, tmp_path):
        """Adding checkpoints must not add a single artifact.

        The checkpoints write to ONE mutable state document; nothing about them
        touches the immutable event log, the iteration receipts or a decision
        artifact. Three real iterations, all reporting progress, must still
        produce three distinct iteration ids and no duplicate event id.
        """
        ic.set_collection_automation(enabled=True, confirm=ic.ENABLE_CONFIRM_TOKEN,
                                     root=root)
        ic.register_worker_start(root=root, instance_id="w1", pid=os.getpid(),
                                 now=OPEN_UTC)
        reporter = ic.ProgressReporter(root=root, instance_id="w1",
                                       min_write_seconds=0.0)
        for n in range(3):
            ic.run_collection_iteration(
                root=root, instance_id="w1",
                now=OPEN_UTC + timedelta(minutes=20 * n), progress_fn=reporter,
                attention_universe={"tiers": {}, "holdings": [], "candidates": [],
                                    "warnings": []},
                stage2_fn=lambda **kw: {"status": "OK", "counts": {},
                                        "source_states": {}},
                news_fn=lambda **kw: {"status": "OK", "counts": {}},
                event_cycle_fn=lambda **kw: {"state": "NO_NEW_INFORMATION"},
                ingestion_root=tmp_path / "ing", news_root=tmp_path / "news")
        history = ic.read_iteration_history(root=root, limit=50)
        ids = [r["iteration_id"] for r in history]
        assert len(ids) == 3
        assert len(set(ids)) == 3, "duplicate iteration id"
        assert reporter.seq > 3, "the pass emitted no progress at all"
        state = ic.load_service_state(root=root)
        assert state["loop_count"] == 3
        assert state["iteration_in_flight"] is False

    def test_progress_reporting_reaches_no_execution_path(self, root):
        """The new evidence is evidence. It authorises nothing."""
        ic.record_progress(root=root, step="COLLECT_STAGE2", detail="sec_edgar",
                           in_flight=True, now=OPEN_UTC)
        contract = ic.collection_safety_contract()
        assert contract["execution_automation_enabled"] is False
        assert contract["broker_execution_enabled"] is False
        for verb in ("creates_orders", "confirms_orders", "fills_orders",
                     "cancels_orders", "approves_proposals", "confirms_targets",
                     "runs_controlled_rebalance", "runs_daily_close",
                     "promotes_models"):
            assert contract[verb] is False, verb
        assert contract["manual_portfolio_review"] == "REQUIRED"
        # Nothing in the progress vocabulary is an execution verb.
        source = (REPO / "api" / "information_collection.py").read_text(
            encoding="utf-8", errors="replace")
        window = source[source.find("def record_progress("):
                        source.find("def release_service_lock(")]
        for forbidden in ("order", "broker", "confirm_target", "approve"):
            assert forbidden not in window.lower(), forbidden

    def test_progress_has_exactly_one_writer(self):
        strays = []
        for path in list((REPO / "api").glob("*.py")) + \
                list((REPO / "scripts").glob("*.py")) + \
                list((REPO / "engine").glob("*.py")):
            if path.name in ("information_collection.py",
                             "audit_architecture.py"):
                continue
            if "def record_progress(" in path.read_text(encoding="utf-8",
                                                        errors="replace"):
                strays.append(path.name)
        assert strays == []


# --------------------------------------------------------------------------- #
# 5b. ONE LOGICAL WORKER is one LAUNCH LINEAGE, not one physical process
# --------------------------------------------------------------------------- #
class TestWindowsLaunchLineageIdentity:
    """Regression for a production Start that could never succeed on Windows.

    ``.venv-win\\Scripts\\python.exe`` is not CPython. It is the venv REDIRECTOR
    (version resource: OriginalFilename ``py.exe``) that CreateProcess-es the
    base interpreter from ``pyvenv.cfg`` with a BYTE-IDENTICAL command line and
    waits on it. Every clean Start therefore produces two physical processes for
    one worker, and the manager - which counted command-line matches - called
    that ``singleton violated: 2 worker processes`` every single time. Measured
    on this machine with a bare ``Start-Process``, so it is the launcher, not
    Task Scheduler.

    The fix must not soften the guarantee: two independent lineages, a lineage
    with two executing processes, or an unreadable snapshot must still fail.
    """

    VENV = r"C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe"
    BASE = r"C:\Python313\python.exe"
    CMD = ('"%s" C:\\Users\\binis\\paper_trader\\scripts\\'
           'run_information_collection_service.py --interval-seconds 60' % VENV)

    def _row(self, pid, parent_pid, *, image, created="2026-08-17T14:00:00Z",
             command=None):
        return {"pid": pid, "parent_pid": parent_pid,
                "command_line": command if command is not None else self.CMD,
                "executable_path": image, "created_at": created}

    def _lineage(self, launcher_pid, worker_pid, *, base_offset_seconds=1):
        created = "2026-08-17T14:00:0%dZ" % base_offset_seconds
        return [self._row(launcher_pid, 9760, image=self.VENV,
                          created="2026-08-17T14:00:00Z"),
                self._row(worker_pid, launcher_pid, image=self.BASE,
                          created=created)]

    def test_the_venv_redirector_and_its_worker_are_one_logical_worker(self):
        # The exact observed topology: 31080 -> 33008, identical command lines.
        topology = ic.resolve_worker_topology(self._lineage(31080, 33008))
        assert topology["verdict"] == ic.WORKER_TOPOLOGY_SINGLE
        assert topology["logical_worker_count"] == 1
        assert topology["physical_process_count"] == 2
        assert topology["executing_process_count"] == 1
        # The CHILD runs main(), so the child is the executing process.
        assert topology["executing_pid"] == 33008
        assert topology["lineages"][0]["root_pid"] == 31080
        # Root-first, so the printed chain reads in the direction it launched.
        assert topology["lineages"][0]["pids"] == [31080, 33008]
        assert topology["lineages"][0]["executable_paths"] == [self.VENV, self.BASE]

    def test_the_lineage_reads_in_launch_order_even_when_the_child_pid_is_lower(self):
        # Observed live: redirector pid 18204 launched worker pid 8660. Sorting
        # the members would print "8660 -> 18204" and state the topology
        # backwards to the operator.
        rows = [self._row(18204, 9760, image=self.VENV,
                          created="2026-08-17T12:05:20.000Z"),
                self._row(8660, 18204, image=self.BASE,
                          created="2026-08-17T12:05:21.000Z")]
        topology = ic.resolve_worker_topology(rows, lock={"pid": 8660})
        assert topology["lineages"][0]["pids"] == [18204, 8660]
        assert topology["lineages"][0]["root_pid"] == 18204
        assert topology["executing_pid"] == 8660
        assert "18204 -> 8660" in topology["reason"]
        assert topology["healthy"] is True

    def test_two_independent_lineages_are_still_a_singleton_violation(self):
        rows = self._lineage(31080, 33008) + self._lineage(41000, 42000)
        topology = ic.resolve_worker_topology(rows)
        assert topology["verdict"] == ic.WORKER_TOPOLOGY_VIOLATED
        assert topology["logical_worker_count"] == 2
        assert topology["singleton_ok"] is False
        assert topology["healthy"] is False

    def test_one_launcher_that_spawned_two_workers_is_a_violation(self):
        # One root, but TWO processes actually executing the application.
        rows = [self._row(31080, 9760, image=self.VENV),
                self._row(33008, 31080, image=self.BASE),
                self._row(33009, 31080, image=self.BASE)]
        topology = ic.resolve_worker_topology(rows)
        assert topology["verdict"] == ic.WORKER_TOPOLOGY_VIOLATED
        assert topology["executing_process_count"] == 2

    def test_a_lone_worker_without_a_redirector_is_one_logical_worker(self):
        topology = ic.resolve_worker_topology(
            [self._row(4242, 9760, image=self.BASE)])
        assert topology["verdict"] == ic.WORKER_TOPOLOGY_SINGLE
        assert topology["executing_pid"] == 4242

    def test_no_worker_is_not_a_violation(self):
        empty = ic.resolve_worker_topology([])
        assert empty["verdict"] == ic.WORKER_TOPOLOGY_NONE
        assert empty["logical_worker_count"] == 0
        # A process that is not the collection worker is never counted.
        other = ic.resolve_worker_topology(
            [self._row(11, 1, image=self.BASE, command='"python.exe" -m http.server')])
        assert other["verdict"] == ic.WORKER_TOPOLOGY_NONE

    def test_the_lock_owner_must_be_the_executing_process_of_the_lineage(self):
        rows = self._lineage(31080, 33008)
        held = ic.resolve_worker_topology(rows, lock={"pid": 33008,
                                                      "instance_id": "w1"})
        assert held["lock_correlated"] is True
        assert held["healthy"] is True
        assert held["lineages"][0]["owns_lock"] is True

        # The redirector never calls os.getpid(); a lock naming IT is wrong.
        wrong = ic.resolve_worker_topology(rows, lock={"pid": 31080})
        assert wrong["verdict"] == ic.WORKER_TOPOLOGY_SINGLE
        assert wrong["lock_correlated"] is False
        assert wrong["healthy"] is False

        # A lock left behind by a worker that is not in this lineage fails too.
        stale = ic.resolve_worker_topology(rows, lock={"pid": 999_999})
        assert stale["lock_correlated"] is False
        assert stale["healthy"] is False

        # One logical worker with NO lock at all is not healthy either.
        unlocked = ic.resolve_worker_topology(rows, lock=None)
        assert unlocked["singleton_ok"] is True
        assert unlocked["healthy"] is False

    def test_an_unreadable_or_ambiguous_snapshot_fails_closed(self):
        # A row whose pid cannot be read.
        blind = ic.resolve_worker_topology(
            [{"pid": None, "parent_pid": 1, "command_line": self.CMD}])
        assert blind["verdict"] == ic.WORKER_TOPOLOGY_AMBIGUOUS
        assert blind["healthy"] is False

        # A parent cycle is a malformed snapshot, never a lineage.
        cycle = ic.resolve_worker_topology(
            [self._row(100, 200, image=self.VENV),
             self._row(200, 100, image=self.BASE)])
        assert cycle["verdict"] == ic.WORKER_TOPOLOGY_AMBIGUOUS
        assert cycle["healthy"] is False

    def test_a_recycled_parent_id_cannot_adopt_an_older_child(self):
        # Windows reuses pids. A "parent" created AFTER its supposed child is a
        # recycled id: the child must stay its own root, which fails SAFE by
        # producing more lineages, never fewer.
        rows = [self._row(500, 9760, image=self.VENV,
                          created="2026-08-17T15:00:00Z"),
                self._row(600, 500, image=self.BASE,
                          created="2026-08-17T14:00:00Z")]
        topology = ic.resolve_worker_topology(rows)
        assert topology["verdict"] == ic.WORKER_TOPOLOGY_VIOLATED
        assert topology["logical_worker_count"] == 2

    def test_win32_key_names_are_accepted_verbatim(self):
        # PowerShell hands over Win32_Process rows; no renaming layer may sit in
        # between and silently drop the parent edge.
        rows = [{"ProcessId": 31080, "ParentProcessId": 9760,
                 "CommandLine": self.CMD, "ExecutablePath": self.VENV},
                {"ProcessId": 33008, "ParentProcessId": 31080,
                 "CommandLine": self.CMD, "ExecutablePath": self.BASE}]
        topology = ic.resolve_worker_topology(rows)
        assert topology["verdict"] == ic.WORKER_TOPOLOGY_SINGLE
        assert topology["executing_pid"] == 33008

    def test_the_manager_delegates_the_count_and_never_counts_processes(self):
        manage = (REPO / "scripts" / "manage_information_collection.ps1").read_text(
            encoding="utf-8", errors="replace")
        # The physical-process count may never be the singleton verdict again.
        assert "singleton violated: $($procs.Count) worker processes" not in manage
        assert "--action worker-topology" in manage
        assert "function Get-WorkerTopology(" in manage
        assert "SINGLE_LOGICAL_WORKER" in manage
        # Stop must still remove the WHOLE lineage, deepest member first.
        assert "$parentPids" in manage
        assert "NO_LOGICAL_WORKER" in manage

    def test_the_control_helper_exposes_the_one_definition(self):
        control = (REPO / "scripts" / "collection_service_control.py").read_text(
            encoding="utf-8", errors="replace")
        assert "def action_worker_topology(" in control
        assert "ic.resolve_worker_topology(" in control
        assert '"worker-topology": action_worker_topology' in control
        # There is exactly ONE implementation of the definition in the tree.
        owner = (REPO / "api" / "information_collection.py").read_text(
            encoding="utf-8", errors="replace")
        assert owner.count("def resolve_worker_topology(") == 1
        assert control.count("def resolve_worker_topology(") == 0
        # And the strict architecture audit fails the build if that drifts back.
        audit = (REPO / "scripts" / "audit_architecture.py").read_text(
            encoding="utf-8", errors="replace")
        assert "IC_MANAGE_RAW_PROCESS_COUNT" in audit
        assert 'icx["manage_counts_raw_processes"]' in audit
        assert 'icx["second_topology_owner_modules"]' in audit

    @pytest.mark.parametrize("bom,shape,expected_physical", [
        # PowerShell 5.1 writes a UTF-8 BOM ahead of anything it pipes into a
        # native command; a BOM must never read as "ambiguous".
        (True, "array", 1),
        (False, "array", 1),
        # ... and it unwraps a single-element array into a bare object.
        (False, "object", 1),
        (True, "empty", 0),
        (False, "empty", 0),
    ])
    def test_the_control_helper_survives_how_powershell_serialises(
            self, tmp_path, bom, shape, expected_physical):
        import json as _json
        import subprocess
        row = {"pid": 4242, "parent_pid": 9760, "command_line": self.CMD,
               "executable_path": self.BASE}
        body = {"array": [row], "object": row, "empty": []}[shape]
        stdin = (b"\xef\xbb\xbf" if bom else b"") + _json.dumps(body).encode("utf-8")
        proc = subprocess.run(
            [sys.executable, str(REPO / "scripts" / "collection_service_control.py"),
             "--action", "worker-topology", "--root", str(tmp_path)],
            input=stdin, capture_output=True, timeout=180)
        assert proc.returncode == 0, proc.stdout + proc.stderr
        out = _json.loads(proc.stdout.decode("utf-8"))
        assert out["physical_process_count"] == expected_physical
        assert out["verdict"] == (ic.WORKER_TOPOLOGY_SINGLE if expected_physical
                                  else ic.WORKER_TOPOLOGY_NONE)
        # The read contract must never write into the state root it was given.
        assert list(tmp_path.iterdir()) == []

    def test_the_lineage_verdict_never_authorises_execution(self):
        topology = ic.resolve_worker_topology(
            self._lineage(31080, 33008), lock={"pid": 33008})
        assert topology["healthy"] is True
        safety = ic.collection_safety_contract()
        assert safety["execution_automation_enabled"] is False
        assert safety["broker_execution_enabled"] is False
        assert safety["creates_orders"] is False


# --------------------------------------------------------------------------- #
# 5a. The liveness probe is a QUESTION, never a SIGNAL
# --------------------------------------------------------------------------- #
class TestLivenessProbeIsNeverASignal:
    """Regression for an interrupt that killed three complete test runs.

    On Windows, CPython's ``os.kill`` dispatches on the SIGNAL value, and
    signal 0 IS ``CTRL_C_EVENT``. ``os.kill(pid, 0)`` therefore probes nothing:
    it calls ``GenerateConsoleCtrlEvent`` and delivers a REAL console Ctrl+C to
    the process group named by ``pid``. ``acquire_service_lock`` records
    ``os.getpid()``, so the next liveness check interrupted its own console.

    The interrupt is asynchronous - measured at ~11 ms - so the resulting
    ``KeyboardInterrupt`` surfaced in the test AFTER the one that caused it.
    Three full-suite runs died at this file, at 73%, with an interrupt nobody
    typed. These tests fail if that probe is ever expressed as a signal again.
    """

    def test_the_probe_never_routes_through_os_kill_on_windows(self, monkeypatch):
        if sys.platform != "win32":
            pytest.skip("signal 0 is a genuine liveness probe on POSIX")
        calls = []

        def spy(pid, sig, *args, **kwargs):
            calls.append((pid, sig))
            raise AssertionError(
                "the liveness probe called os.kill(%r, %r); on Windows signal 0 "
                "is CTRL_C_EVENT and that delivers a console interrupt" % (pid, sig))

        monkeypatch.setattr(os, "kill", spy)
        assert ic._pid_alive(os.getpid()) is True
        assert calls == []

    def test_probing_this_very_process_delivers_no_interrupt(self):
        received = []
        previous = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, lambda signum, frame: received.append(signum))
        try:
            for _ in range(5):
                assert ic._pid_alive(os.getpid()) is True
            # The console control event was delivered by an injected thread
            # ~11 ms after the call returned, so settle before concluding.
            time.sleep(0.35)
        finally:
            signal.signal(signal.SIGINT, previous)
        assert received == [], (
            "the liveness probe delivered SIGINT to its own process: it is "
            "generating a console control event instead of asking a question")

    def test_the_probe_answers_accurately_and_never_raises(self):
        assert ic._pid_alive(os.getpid()) is True
        assert ic._pid_alive(999_999_999) is False
        assert ic._pid_alive(0) is False
        assert ic._pid_alive(-1) is False
        assert ic._pid_alive("not-a-pid") is None
        assert ic._pid_alive(None) is None

    def test_a_live_holder_keeps_the_slot_even_when_its_heartbeat_is_stale(self, root):
        """Reclaim needs silence AND a dead pid - never silence alone."""
        first = ic.acquire_service_lock(root=root, instance_id="live",
                                        pid=os.getpid(), now=OPEN_UTC)
        assert first["acquired"] is True
        intruder = ic.acquire_service_lock(
            root=root, instance_id="intruder", pid=os.getpid() + 1,
            now=OPEN_UTC + timedelta(seconds=ic.LOCK_TAKEOVER_SECONDS * 4))
        assert intruder["acquired"] is False
        assert intruder["reason"] == "SINGLE_FLIGHT_LOCK_HELD"


# --------------------------------------------------------------------------- #
# 6. Governance — collection automation is NOT execution automation
# --------------------------------------------------------------------------- #
class TestGovernance:

    def test_enabling_collection_requires_the_explicit_confirm_token(self, root):
        refused = ic.set_collection_automation(enabled=True, root=root)
        assert refused["ok"] is False
        assert refused["confirm_required"] == ic.ENABLE_CONFIRM_TOKEN
        assert ic.load_service_state(
            root=root)["collection_automation_enabled"] is False

        armed = ic.set_collection_automation(
            enabled=True, confirm=ic.ENABLE_CONFIRM_TOKEN, root=root)
        assert armed["collection_automation_enabled"] is True
        assert armed["execution_automation_enabled"] is False

    def test_an_unauthorised_iteration_calls_nothing(self, root):
        called = []
        receipt = ic.run_collection_iteration(
            root=root, instance_id="w1", now=OPEN_UTC,
            attention_universe={"tiers": {}, "holdings": [], "candidates": []},
            stage2_fn=lambda **kw: called.append("stage2"),
            news_fn=lambda **kw: called.append("news"),
            event_cycle_fn=lambda **kw: called.append("cycle"))
        assert receipt["ran"] is False
        assert receipt["state"] == "COLLECTION_AUTOMATION_DISABLED"
        assert called == []

    def test_the_safety_contract_forbids_every_execution_verb(self):
        safety = ic.collection_safety_contract()
        for flag in ("execution_automation_enabled", "broker_execution_enabled",
                     "creates_orders", "confirms_orders", "fills_orders",
                     "cancels_orders", "approves_proposals", "confirms_targets",
                     "runs_controlled_rebalance", "runs_daily_close",
                     "runs_daily_research_cycle", "promotes_models",
                     "mutates_model_weights"):
            assert safety[flag] is False, flag
        assert safety["manual_portfolio_review"] == "REQUIRED"

    def test_credentials_are_reported_by_presence_never_by_value(self):
        creds = ic.credential_availability(env={"EODHD_API_KEY": "super-secret"})
        blob = repr(creds)
        assert "super-secret" not in blob


# --------------------------------------------------------------------------- #
# 7. Attention universe — read from the owners, never hardcoded
# --------------------------------------------------------------------------- #
class TestAttentionUniverse:

    def test_tiers_are_derived_from_the_authoritative_owners(self):
        universe = ic.build_attention_universe(
            portfolio_state={"positions": [{"ticker": "AAA"}, {"ticker": "BBB"}]},
            scoring={"rankings": [{"ticker": "BBB"}, {"ticker": "CCC"},
                                  {"ticker": "DDD"}]})
        assert universe["holdings"] == ["AAA", "BBB"]
        # A held name is never offered back as its own replacement candidate.
        assert "BBB" not in universe["candidates"]
        assert universe["candidates"][:2] == ["CCC", "DDD"]
        assert universe["authoritative_sources"] == ["api.portfolio_state",
                                                     "api.universe_scoring"]

    def test_a_missing_owner_degrades_honestly_and_does_not_crash(self):
        def boom():
            raise RuntimeError("portfolio store unavailable")
        universe = ic.build_attention_universe(portfolio_state_loader=boom,
                                               scoring={"rankings": []})
        assert universe["holdings"] == []
        assert any("portfolio state unavailable" in w
                   for w in universe["warnings"])


# --------------------------------------------------------------------------- #
# 8. Release-28 integration — the quote lane speaks through RISK, not by arriving
# --------------------------------------------------------------------------- #
class TestQuoteLaneIntegration:

    def test_a_quote_identifies_the_days_mark_so_a_repeat_read_is_a_no_op(self):
        first = fabric.capture_market_quotes(
            ["AAA"], fetcher=lambda t: ([{"ticker": "AAA", "price": 100.0}], []),
            now_iso="2026-08-17T14:00:00+00:00")
        later = fabric.capture_market_quotes(
            ["AAA"], fetcher=lambda t: ([{"ticker": "AAA", "price": 100.0}], []),
            now_iso="2026-08-17T14:45:00+00:00")
        assert first["events"][0]["source_event_id"] == \
            later["events"][0]["source_event_id"]
        # Same identity AND same payload -> the same idempotency key -> a duplicate.
        assert first["events"][0]["idempotency_key"] == \
            later["events"][0]["idempotency_key"]

    def test_a_changed_price_supersedes_the_days_mark_instead_of_duplicating_it(self):
        moved = fabric.capture_market_quotes(
            ["AAA"], fetcher=lambda t: ([{"ticker": "AAA", "price": 111.0}], []),
            now_iso="2026-08-17T14:45:00+00:00")
        flat = fabric.capture_market_quotes(
            ["AAA"], fetcher=lambda t: ([{"ticker": "AAA", "price": 100.0}], []),
            now_iso="2026-08-17T14:00:00+00:00")
        assert moved["events"][0]["idempotency_key"] != \
            flat["events"][0]["idempotency_key"]

    def test_event_identity_comes_from_the_callers_clock(self):
        events = fabric.capture_market_quotes(
            ["AAA"], fetcher=lambda t: ([{"ticker": "AAA", "price": 100.0}], []),
            now_iso="2026-08-17T14:00:00+00:00")["events"]
        assert events[0]["source_event_id"].endswith("2026-08-17")
        assert events[0]["effective_at"] == "2026-08-17"

    def test_a_market_observation_is_not_material_merely_by_arriving(self):
        quote = fabric.capture_market_quotes(
            ["AAA"], fetcher=lambda t: ([{"ticker": "AAA", "price": 100.0}], []),
            now_iso="2026-08-17T14:00:00+00:00")["events"][0]
        verdict = emat.assess_materiality(events=[quote], holdings=["AAA"])
        assert verdict["material_signal_changed"] is False
        assert verdict["reassessment_required"] is False
        codes = {s["code"] for s in verdict["suppressed"]}
        assert emat.S_OBSERVATION_ON_ARRIVAL in codes

    def test_an_intraday_move_beyond_the_threshold_is_material(self):
        threshold = emat.DEFAULT_POLICY["abs_return_1d"]
        risk = {"AAA": {"ret_intraday": -(threshold + 0.05)},
                "BBB": {"ret_intraday": 0.001}}
        verdict = emat.assess_materiality(risk_state=risk, holdings=["AAA", "BBB"])
        assert verdict["reassessment_required"] is True
        assert verdict["affected_entities"] == ["AAA"]
        assert emat.T_HOLDING_PRICE_SHOCK in verdict["trigger_codes"]

    def test_the_risk_owner_measures_the_quote_against_the_owned_close(self):
        world = replay.build_world()
        panel = world["price_panel"]
        ticker = sorted(panel["series"].keys())[0]
        close = float(panel["series"][ticker]["adj"][-1])
        state = esr.build_market_risk_state(
            price_panel=panel, tickers=[ticker], eligible=world["eligible"],
            latest_quotes={ticker: close * 0.90})
        row = state["rows"][ticker]
        assert row["intraday_reference_close"] == pytest.approx(close)
        assert row["ret_intraday"] == pytest.approx(-0.10, abs=1e-6)
        assert state["intraday_quoted"] == 1

    def test_a_holding_without_a_quote_reports_no_intraday_move(self):
        world = replay.build_world()
        state = esr.build_market_risk_state(
            price_panel=world["price_panel"],
            tickers=sorted(world["price_panel"]["series"].keys())[:2],
            eligible=world["eligible"], latest_quotes={})
        assert state["intraday_quoted"] == 0
        assert all(r["ret_intraday"] is None for r in state["rows"].values())

    def test_the_read_surface_counts_material_by_the_gates_own_rule(self):
        # The status view must not call a price observation a material event.
        assert ek.F_MARKET_QUOTE in emat.MARKET_OBSERVATION_FAMILIES
        assert ek.F_MARKET_BAR in emat.MARKET_OBSERVATION_FAMILIES

    def test_the_event_orchestrator_accepts_the_service_clock(self, tmp_path):
        roots = {n: tmp_path / n for n in ("fabric", "hoc", "reassess", "realloc")}
        for p in roots.values():
            p.mkdir(parents=True, exist_ok=True)
        world = replay.build_world()
        cycle = replay.run_cycle(
            world=world, records=[], roots=roots, include_market_quotes=True,
            quote_fetcher=creplay.world_quote_fetcher(world),
            entity_index=creplay._entity_index(world),
            now_iso="2026-08-17T14:00:00+00:00")
        assert cycle["events_admitted"] >= 1
        # An ordinary session poll of an unmoved book decides nothing.
        assert cycle["materiality"]["material_signal_changed"] is False


# --------------------------------------------------------------------------- #
# 9. The read contract — GET /v1/operations/information-collection
# --------------------------------------------------------------------------- #
class TestReadContract:

    def test_the_payload_is_readable_before_the_service_has_ever_run(self, root):
        payload = ic.load_information_collection(root=root, now=WEEKEND_UTC)
        assert payload["service"]["service_state"] == ic.SVC_NEVER_STARTED
        assert payload["headline"]["title"] == "COLLECTION NOT INSTALLED"
        assert payload["automation"]["execution_automation_enabled"] is False
        assert payload["service"]["single_flight_lock"]["held"] is False

    def test_the_headline_answers_is_it_running_before_any_counter(self, root):
        ic.set_collection_automation(enabled=True, confirm=ic.ENABLE_CONFIRM_TOKEN,
                                     root=root)
        ic.register_worker_start(root=root, instance_id="w1", pid=os.getpid())
        ic.heartbeat(root=root, instance_id="w1")
        payload = ic.load_information_collection(root=root)
        assert payload["headline"]["title"] in ("COLLECTION RUNNING",
                                                "MATERIAL INFORMATION RECEIVED")
        assert payload["headline"]["detail"]

    def test_the_kpi_row_is_a_due_denominator_not_a_registry_count(self, root):
        payload = ic.load_information_collection(root=root, now=WEEKEND_UTC)
        summary = payload["source_health_summary"]
        for key in ("due_now", "healthy_due", "not_due", "backoff", "blocked",
                    "disabled"):
            assert key in summary, key
        assert summary["due_now"] <= summary["total_sources"]

    def test_every_rendered_value_is_computed_by_the_backend(self, root):
        payload = ic.load_information_collection(root=root, now=WEEKEND_UTC)
        # The browser must never classify health or do date arithmetic: the row
        # arrives already resolved, with the reason that produced it.
        row = payload["source_health"]["sources"][0]
        for key in ("runtime_state", "health_reason", "due_window_reason",
                    "due_window_active", "collect_now", "next_due_at",
                    "session_phase", "cadence_kind", "why_this_cadence",
                    "circuit_state", "backoff_until"):
            assert key in row, key
        # Every state carries the sentence that produced it.
        assert str(row["health_reason"]).strip()

    def test_the_read_contract_writes_nothing(self, root):
        before = sorted(p.name for p in root.iterdir())
        ic.load_information_collection(root=root, now=WEEKEND_UTC)
        assert sorted(p.name for p in root.iterdir()) == before


# --------------------------------------------------------------------------- #
# 10. Ownership — exactly one worker, one orchestrator, one manager script
# --------------------------------------------------------------------------- #
class TestOwnership:

    def test_there_is_exactly_one_long_lived_worker_script(self):
        assert (REPO / "scripts" / "run_information_collection_service.py").exists()
        strays = [p.name for p in (REPO / "scripts").glob("*.py")
                  if "collection" in p.name.lower()
                  and p.name not in ("run_information_collection_service.py",
                                     "collection_service_control.py")]
        assert strays == []

    def test_the_service_is_managed_by_exactly_one_powershell_script(self):
        manage = REPO / "scripts" / "manage_information_collection.ps1"
        assert manage.exists()
        text = manage.read_text(encoding="utf-8", errors="replace")
        # Read-only Status must not require -Execute; every mutation must.
        assert "-Execute" in text
        assert "Install" in text and "Uninstall" in text and "Status" in text

    def test_every_action_that_kills_the_worker_records_the_stop(self):
        """A terminated worker cannot release its own singleton lock.

        Stop-Worker uses Stop-Process, so the worker's graceful release never
        runs and the lock is left naming a dead pid. Every action that calls it
        must record the clean-shutdown marker, or the NEXT Start is refused by
        the single-flight gate for the whole 15-minute takeover window against a
        holder that no longer exists. Measured: Uninstall -> Install -> Start
        failed exactly that way.
        """
        text = (REPO / "scripts" / "manage_information_collection.ps1").read_text(
            encoding="utf-8", errors="replace")
        for action in ('"Stop" {', '"Restart" {', '"Uninstall" {'):
            start = text.find(action)
            assert start > 0, action
            # Bound the search to this action's own switch arm.
            nxt = min([p for p in
                       (text.find('"Stop" {', start + 1),
                        text.find('"Restart" {', start + 1),
                        text.find('"Uninstall" {', start + 1),
                        len(text)) if p > start])
            arm = text[start:nxt]
            assert "Stop-Worker" in arm, action
            assert 'Invoke-Control @("--action", "mark-stopped")' in arm, action

    def test_the_operator_sees_collection_state_without_scrolling(self):
        ui = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8",
                                                            errors="replace")
        # The card is a screen below the fold; the header chip is not.
        assert 'id="ic-header-badge"' in ui
        assert "_icSetHeaderBadge(" in ui
        # ONE loader still owns it — the chip is not a second status source.
        assert ui.count("function loadInformationCollection(") == 1
        assert ui.count("_mhzGet('/v1/operations/information-collection')") == 1

    def test_the_architecture_audit_guards_this_release(self):
        audit = (REPO / "scripts" / "audit_architecture.py").read_text(
            encoding="utf-8", errors="replace")
        assert "def check_information_collection_ownership(" in audit
        # Wired into the report AND into the strict gate — a guard nobody runs is
        # documentation, not enforcement.
        assert '"information_collection_ownership": check_information_collection_ownership(' \
            in audit
        assert 'icx = rep["information_collection_ownership"]' in audit
        assert 'icx["kernel_impurity"]' in audit
        # Release 29.2: one progress writer, no timer authority in the worker,
        # and a stall budget nobody can quietly widen back out of the problem.
        for token in ('icx["second_progress_owner_modules"]',
                      'icx["worker_timer_authorities"]',
                      'icx["stall_budget_not_widened"]'):
            assert token in audit, token

    def test_collection_composes_the_release28_orchestrator_and_does_not_fork_it(self):
        payload = ic.collection_safety_contract()
        assert payload["runs_daily_research_cycle"] is False
        assert esr.COMPOSITION_OWNER == "api.event_signal_refresh"
        source = (REPO / "api" / "information_collection.py").read_text(
            encoding="utf-8", errors="replace")
        # No second opportunity cost, reassessment or proposal builder may appear here.
        for forbidden in ("def assess_holding_opportunity_cost",
                          "def run_portfolio_reassessment",
                          "def build_reallocation_proposal"):
            assert forbidden not in source, forbidden


# --------------------------------------------------------------------------- #
# 11. The hermetic acceptance suite itself
# --------------------------------------------------------------------------- #
class TestHermeticAcceptance:

    def test_the_scenario_registry_covers_every_release29_claim(self):
        assert len(creplay.SCENARIOS) >= 22
        for key in ("S1", "S4", "S6", "S12", "S15", "S18", "S19", "S20", "S21",
                    "S22"):
            assert key in creplay.SCENARIOS, key

    @pytest.mark.parametrize("scenario", ["S12", "S21", "S22"])
    def test_a_representative_scenario_passes_hermetically(self, scenario, tmp_path):
        result = creplay.run_simulation(base_dir=tmp_path, scenarios=[scenario],
                                        timeout_seconds=120)
        assert result["blocked_connection_attempts"] == []
        assert result["passed"] is True, result["scenarios"]
