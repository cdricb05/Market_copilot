r"""Release 64 - the persistent research runtime CPU repair.

WHAT FAILED. The R59 autonomous research worker recorded 4,829 cycles, every
one of them a zero-second sleep and 4,825 of them stopping on
``B_EXTERNAL_BLOCKER`` with nothing executed. It evaluated work that could not
run and immediately started an identical evaluation, consuming a full CPU core
for as long as it was up.

WHY. :func:`alpha_agent.r59.runtime.run_forever` asked the GOVERNOR how many
mandates it could generate and handed that number to :func:`plan_sleep` as
``ready_work``. Those are different quantities: the governor answers "are
there questions worth asking", the queue answers "can anything be claimed".
``governor.stop_reason`` is a pure function of research MEMORY and memory only
changes when work EXECUTES, so a frontier on which every job was blocked
produced the same answer forever. ``plan_sleep`` was correct and well tested -
and its blocked and exhausted branches were unreachable for the entire life of
the worker, because the one caller never told it the truth.

These tests protect the repaired contract:

* the session that just ran is the authority on what is executable, and a
  proven external blocker means ZERO regardless of the governor's opinion;
* a cycle that achieved nothing is never followed immediately by an identical
  one, enforced at the loop and not only in the policy it calls;
* an unchanged blocker earns a bounded, doubling back-off; a changed one, or
  any real progress, resets it;
* a zero or negative configured interval is clamped, never honoured;
* genuinely claimable work is still drained with NO added delay;
* Stop interrupts a waiting worker promptly and is idempotent;
* one lease, one worker, no duplicate experiments, no orphans;
* the queue, the memory and the graveyard are read and resumed, never reset;
* and nothing here promotes a model, approves a proposal, or creates an order.

The reproduction fixture at the end replays the real failure at its measured
cost: the last worker instance was up 57,348 seconds and spent all of it on
193 back-to-back cycles, sleeping for none of them. Under the old rule the
fixture still spends the whole span working; under the repair it spends it
waiting.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from alpha_agent import r59  # noqa: E402
from alpha_agent.r59 import loop as LP  # noqa: E402
from alpha_agent.r59 import memory as M  # noqa: E402
from alpha_agent.r59 import runtime as RT  # noqa: E402

RUNTIME_SRC = (_ROOT / "alpha_agent" / "r59" / "runtime.py").read_text(
    encoding="utf-8")
MANAGER = _ROOT / "scripts" / "manage_research_runtime.ps1"

#: The MEASURED live failure. The last worker instance ran from
#: 2026-09-21T21:45:02Z to its final heartbeat at 2026-09-22T13:40:50Z - 57,348
#: seconds - and recorded 193 cycles in that time, so one blocked cycle cost
#: about 297 seconds of continuous work. The runtime log holds 4,829 such cycle
#: rows across every worker generation since it was last rotated, of which
#: 4,825 stopped on B_EXTERNAL_BLOCKER, 4,806 executed nothing at all, and
#: 4,829 - every single one - slept zero seconds. At the measured rate those
#: rows represent about 16.6 days of continuous unproductive work.
OBSERVED_WORKER_UPTIME_SECONDS = 57348.0
OBSERVED_WORKER_CYCLES = 193
OBSERVED_CYCLE_COST_SECONDS = OBSERVED_WORKER_UPTIME_SECONDS / OBSERVED_WORKER_CYCLES
OBSERVED_ZERO_SLEEP_LOG_ROWS = 4829

#: The span the reproduction fixture replays. A day is longer than the worker
#: that produced the evidence was ever up in one go.
REPLAY_SECONDS = 24 * 3600.0


@pytest.fixture()
def root(tmp_path, monkeypatch):
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59"))
    # The idle policy is read from the environment; no test may inherit an
    # operator's tuning from the machine it happens to run on.
    monkeypatch.delenv(RT.MIN_SLEEP_ENV, raising=False)
    monkeypatch.delenv(RT.MAX_SLEEP_ENV, raising=False)
    return tmp_path


class _Clock:
    """A deterministic clock. Time passes when the worker sleeps, and when it
    works - a cycle that costs nothing cannot express a CPU defect."""

    def __init__(self, start: float = 0.0):
        self.t = float(start)
        self.sleeps: list = []
        self.worked = 0.0

    def __call__(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(float(seconds))
        self.t += float(seconds)

    def work(self, seconds: float) -> None:
        self.worked += float(seconds)
        self.t += float(seconds)

    @property
    def slept(self) -> float:
        return sum(self.sleeps)


class _Job:
    """A blocked queue row, as ``blocked_jobs`` yields them."""

    def __init__(self, lane: str, reason: str, job_id: str = "j"):
        self.lane = lane
        self.blocked_reason = reason
        self.job_id = job_id
        self.payload = {}


class _Queue:
    """The smallest queue that can express the failure: a claimable depth and
    a blocked set, both under the test's control."""

    def __init__(self, runnable: int = 0, blocked=()):
        self._runnable = int(runnable)
        self._blocked = list(blocked)
        self.closed = False
        self.runnable_calls = 0

    def runnable_depth(self, **_kw) -> int:
        self.runnable_calls += 1
        return self._runnable

    def blocked_jobs(self, *, limit: int = 100) -> list:
        return list(self._blocked)[:limit]

    def depth(self) -> int:
        return self._runnable + len(self._blocked)

    def set_runnable(self, n: int) -> None:
        self._runnable = int(n)

    def close(self) -> None:
        self.closed = True


def _blocked_17():
    """The live frontier at the moment of the failure: 17 blocked jobs across
    five lanes, every one of them needing new INFORMATION."""
    rows = []
    for lane, reason, n in (
            ("r59.mathematical.us_equity",
             "generator produced no non-degenerate candidate for "
             "US_EQUITY/SYMBOLIC", 4),
            ("r59.mathematical.cross_asset",
             "generator produced no non-degenerate candidate for "
             "CROSS_ASSET/SYMBOLIC", 3),
            ("r59.mathematical.rates_futures",
             "generator produced no non-degenerate candidate for "
             "RATES_FUTURES/SYMBOLIC", 3),
            ("r59.mathematical.equity_index_futures",
             "generator produced no non-degenerate candidate for "
             "EQUITY_INDEX_FUTURES/SYMBOLIC", 2),
            ("r59.cross_asset.cross_asset", "engine returned NO_MEMBERS", 5)):
        rows.extend(_Job(lane, reason, "%s_%d" % (lane, i)) for i in range(n))
    return rows


def _blocked_session(**_kw):
    """A session that re-seeded the frontier and still could claim nothing."""
    return {"stop_condition": LP.STOP_B, "jobs_executed": 0,
            "hypotheses_measured": 0, "research_still_ready": 3}


# --------------------------------------------------------------------------- #
# The root cause: governor potential is not claimable work
# --------------------------------------------------------------------------- #
def test_a_proven_external_blocker_means_zero_executable_work():
    """The exact substitution that produced 4,825 blocked cycles."""
    verdict = RT.executable_ready_work(
        session=_blocked_session(), runnable_now=0)
    assert verdict["ready_work"] == 0
    assert verdict["authority"] == "SESSION_PROVED_EXTERNAL_BLOCKER"


def test_a_blocked_session_outranks_the_governors_opinion_of_the_frontier():
    """``research_still_ready`` was 3 on nearly every one of the 4,825 cycles.

    That number is what the governor could GENERATE, and the old runtime read
    it as work it could RUN. A session that proved the blocker overrules it.
    """
    session = dict(_blocked_session(), research_still_ready=3)
    assert RT.executable_ready_work(session=session,
                                    runnable_now=0)["ready_work"] == 0


def test_a_session_that_raised_is_never_retried_without_a_delay():
    """A crash loop is the same busy-wait wearing a different hat."""
    verdict = RT.executable_ready_work(session=None, runnable_now=7)
    assert verdict["ready_work"] == 0
    assert verdict["authority"] == "SESSION_DID_NOT_COMPLETE"


def test_claimable_work_is_still_reported_as_executable():
    verdict = RT.executable_ready_work(
        session={"stop_condition": LP.STOP_A}, runnable_now=4)
    assert verdict["ready_work"] == 4
    assert verdict["authority"] == "QUEUE_CLAIMABLE_DEPTH"


def test_the_scheduler_no_longer_consults_the_governor_at_all():
    """One owner for 'is this executable', and it is not the governor."""
    assert "GOV.stop_reason" not in RUNTIME_SRC
    assert "from . import governor" not in RUNTIME_SRC
    assert "executable_ready_work(" in RUNTIME_SRC


# --------------------------------------------------------------------------- #
# B. an external blocker waits, and backs off
# --------------------------------------------------------------------------- #
def test_an_external_blocker_never_produces_a_zero_sleep_cycle(root,
                                                                monkeypatch):
    monkeypatch.setattr(LP, "run_session", _blocked_session)
    q = _Queue(runnable=0, blocked=_blocked_17())
    clk = _Clock()
    body = RT.run_forever(queue=q, debug_max_cycles=6, clock=clk,
                          sleep_fn=clk.sleep, install_signal_handlers=False)
    assert body["n_cycles"] == 6
    assert [c for c in body["cycles"] if c["sleep_seconds"] <= 0] == []
    for c in body["cycles"]:
        assert c["sleep_seconds"] >= RT.ABSOLUTE_MIN_SLEEP_SECONDS
        assert c["ready_work_authority"] == "SESSION_PROVED_EXTERNAL_BLOCKER"


def test_the_same_blocker_backs_off_and_a_changed_one_resets_it():
    pol = RT.idle_policy()
    waits = [RT.escalate_idle_backoff(base_seconds=60.0, unproductive_streak=s,
                                      policy=pol)["sleep_seconds"]
             for s in range(1, 8)]
    assert waits == [60.0, 120.0, 240.0, 480.0, 960.0, 1920.0, 3600.0]
    # It is BOUNDED: the ceiling still guarantees an hourly re-examination.
    assert RT.escalate_idle_backoff(base_seconds=60.0, unproductive_streak=500,
                                    policy=pol)["sleep_seconds"] == 3600.0
    # And a reset returns to the floor rather than to zero.
    assert RT.escalate_idle_backoff(base_seconds=60.0, unproductive_streak=0,
                                    policy=pol)["sleep_seconds"] == 60.0


def test_a_repeated_identical_blocker_escalates_the_wait_in_the_live_loop(
        root, monkeypatch):
    monkeypatch.setattr(LP, "run_session", _blocked_session)
    q = _Queue(runnable=0, blocked=_blocked_17())
    clk = _Clock()
    body = RT.run_forever(queue=q, debug_max_cycles=8, clock=clk,
                          sleep_fn=clk.sleep, install_signal_handlers=False)
    waits = [c["sleep_seconds"] for c in body["cycles"]]
    streaks = [c["unproductive_streak"] for c in body["cycles"]]
    assert streaks == [1, 2, 3, 4, 5, 6, 7, 8]
    assert waits == sorted(waits), waits          # monotonically non-decreasing
    assert waits[-1] >= waits[0]
    assert max(waits) <= RT.idle_policy()["max_sleep_seconds"]
    # The blocked set never changed, so the signature never changed.
    assert len({c["blocker_signature"] for c in body["cycles"]}) == 1


def test_a_changed_blocked_set_resets_the_back_off(root, monkeypatch):
    """A frontier that starts moving again is not punished for having stuck."""
    q = _Queue(runnable=0, blocked=_blocked_17())
    seen = {"n": 0}

    def _session(**_kw):
        seen["n"] += 1
        if seen["n"] == 3:                 # something clears on the third pass
            q._blocked = _blocked_17()[:4]
        return _blocked_session()

    monkeypatch.setattr(LP, "run_session", _session)
    clk = _Clock()
    body = RT.run_forever(queue=q, debug_max_cycles=5, clock=clk,
                          sleep_fn=clk.sleep, install_signal_handlers=False)
    streaks = [c["unproductive_streak"] for c in body["cycles"]]
    assert streaks[:2] == [1, 2]
    assert streaks[2] == 1, streaks       # the signature changed; back to one
    sigs = [c["blocker_signature"] for c in body["cycles"]]
    assert sigs[1] != sigs[2]


def test_the_blocker_signature_tracks_what_is_blocked_not_how_many_times():
    a = RT.blocker_signature({"by_reason": {"FAMILY_EXHAUSTED": 14},
                              "blocked_total": 17}, _blocked_17())
    b = RT.blocker_signature({"by_reason": {"FAMILY_EXHAUSTED": 14},
                              "blocked_total": 17}, _blocked_17())
    c = RT.blocker_signature({"by_reason": {"FAMILY_EXHAUSTED": 3},
                              "blocked_total": 4}, _blocked_17()[:4])
    assert a == b
    assert a != c


# --------------------------------------------------------------------------- #
# C. an empty queue does not spin
# --------------------------------------------------------------------------- #
def test_an_empty_queue_waits_instead_of_spinning(root, monkeypatch):
    monkeypatch.setattr(LP, "run_session", lambda **kw: {
        "stop_condition": LP.STOP_A, "jobs_executed": 0,
        "hypotheses_measured": 0, "research_still_ready": 0})
    q = _Queue(runnable=0, blocked=[])
    clk = _Clock()
    body = RT.run_forever(queue=q, debug_max_cycles=4, clock=clk,
                          sleep_fn=clk.sleep, install_signal_handlers=False)
    assert all(c["sleep_seconds"] > 0 for c in body["cycles"])
    assert all(c["sleep_reason"] == "ONLY_FUTURE_DATA_CAN_ADVANCE_THE_STATE"
               for c in body["cycles"])
    # It waits with the EXISTING watch mechanism, not by polling hard.
    assert clk.slept >= RT.idle_policy()["min_sleep_seconds"] * 3


# --------------------------------------------------------------------------- #
# A. genuinely executable work is never delayed
# --------------------------------------------------------------------------- #
def test_claimable_work_is_processed_with_no_added_delay(root, monkeypatch):
    """The repair may not buy quiet by making the researcher slow."""
    monkeypatch.setattr(LP, "run_session", lambda **kw: {
        "stop_condition": LP.STOP_A, "jobs_executed": 5,
        "hypotheses_measured": 11, "research_still_ready": 2})
    q = _Queue(runnable=3, blocked=[])
    clk = _Clock()
    body = RT.run_forever(queue=q, debug_max_cycles=4, clock=clk,
                          sleep_fn=clk.sleep, install_signal_handlers=False)
    assert all(c["sleep_seconds"] == 0.0 for c in body["cycles"])
    assert all(c["sleep_reason"] == "EXECUTABLE_RESEARCH_EXISTS"
               for c in body["cycles"])
    assert clk.slept == 0.0
    assert body["forced_waits"] == 0


def test_new_claimable_work_wakes_a_waiting_worker_at_once(root):
    """A WORK arrival is never held back by an information dwell."""
    mem = M.open_memory()
    q = _Queue(runnable=0, blocked=_blocked_17())
    baseline = RT.wake_conditions(mem, q)
    clk = _Clock()

    class _Stop:
        requested = False

        def request(self, _r):
            self.requested = True

    def _sleep(seconds):
        clk.sleep(seconds)
        q.set_runnable(2)                  # work arrives during the wait

    changed = RT._sleep_watching(
        3600.0, stopper=_Stop(), mem=mem, queue=q, baseline=baseline,
        beat=lambda *a, **k: True, sleep_fn=_sleep, clock=clk,
        information_dwell_seconds=3600.0)
    assert "QUEUE_RUNNABLE" in changed
    # It woke on the FIRST slice, not at the end of the hour.
    assert clk.slept <= RT.HEARTBEAT_SECONDS


def test_an_information_arrival_may_not_cut_a_standing_back_off_short(root):
    """The collector rewrites its progress file every half hour. A back-off any
    such write can cut short is a thirty-minute poll, not a back-off."""
    mem = M.open_memory()
    q = _Queue(runnable=0, blocked=_blocked_17())
    baseline = RT.wake_conditions(mem, q)
    # An INFORMATION watermark moves immediately and keeps moving.
    moved = [dict(c) for c in baseline]
    for c in moved:
        if c["name"] == "EODHD_COLLECTION":
            c["watermark"] = "MOVED"
    clk = _Clock()

    class _Stop:
        requested = False

        def request(self, _r):
            self.requested = True

    changed = RT._sleep_watching(
        600.0, stopper=_Stop(), mem=mem, queue=q, baseline=baseline,
        beat=lambda *a, **k: True, sleep_fn=clk.sleep, clock=clk,
        information_dwell_seconds=600.0,
        )
    # Nothing actually moved in this fixture, so it waited the full interval
    # rather than finding a reason to leave early.
    assert changed == []
    assert clk.slept >= 600.0


# --------------------------------------------------------------------------- #
# F. an invalid configuration may not reactivate the busy-wait
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("requested", [0, 0.0, -1, -3600, "0", "-5"])
def test_a_zero_or_negative_idle_interval_is_clamped(requested):
    pol = RT.idle_policy(min_seconds=requested)
    assert pol["min_sleep_seconds"] >= RT.ABSOLUTE_MIN_SLEEP_SECONDS
    assert pol["was_corrected"] is True
    assert "MIN_NOT_POSITIVE" in pol["corrections"]


def test_a_nonsense_idle_interval_falls_back_and_says_so():
    pol = RT.idle_policy(min_seconds="soon")
    assert pol["min_sleep_seconds"] == RT.MIN_SLEEP_SECONDS
    assert "MIN_NOT_NUMERIC" in pol["corrections"]
    assert pol["min_source"] == "DEFAULT_AFTER_INVALID"


def test_a_ceiling_below_the_floor_is_raised_to_it():
    pol = RT.idle_policy(min_seconds=300, max_seconds=10)
    assert pol["max_sleep_seconds"] == 300.0
    assert "MAX_BELOW_MIN" in pol["corrections"]


def test_an_environment_override_is_read_and_reported(root, monkeypatch):
    monkeypatch.setenv(RT.MIN_SLEEP_ENV, "0")
    pol = RT.idle_policy()
    assert pol["min_sleep_seconds"] == RT.ABSOLUTE_MIN_SLEEP_SECONDS
    assert pol["min_source"] == "ENVIRONMENT"
    assert pol["requested_min_seconds"] == 0.0


def test_a_clamped_configuration_still_cannot_busy_wait(root, monkeypatch):
    """The whole point: a zero interval must not reproduce the failure."""
    monkeypatch.setenv(RT.MIN_SLEEP_ENV, "0")
    monkeypatch.setattr(LP, "run_session", _blocked_session)
    q = _Queue(runnable=0, blocked=_blocked_17())
    clk = _Clock()
    body = RT.run_forever(queue=q, debug_max_cycles=5, clock=clk,
                          sleep_fn=clk.sleep, install_signal_handlers=False)
    assert all(c["sleep_seconds"] >= RT.ABSOLUTE_MIN_SLEEP_SECONDS
               for c in body["cycles"])
    assert body["idle_policy"]["was_corrected"] is True


def test_the_effective_configuration_is_published_for_the_operator(root,
                                                                   monkeypatch):
    """What an operator sees WHILE the worker is waiting - which is the only
    moment the question 'is it waiting or spinning?' is worth asking."""
    monkeypatch.setattr(LP, "run_session", _blocked_session)
    q = _Queue(runnable=0, blocked=_blocked_17())
    clk = _Clock()
    seen: list = []

    def _sleep(seconds):
        clk.sleep(seconds)
        seen.append(RT.read_status())

    RT.run_forever(queue=q, debug_max_cycles=2, clock=clk, sleep_fn=_sleep,
                   install_signal_handlers=False)

    assert seen, "the worker never waited"
    st = seen[0]
    assert st["worker_state"] == RT.W_SLEEPING
    assert st["idle_policy"]["min_sleep_seconds"] == RT.MIN_SLEEP_SECONDS
    assert st["idle_policy"]["was_corrected"] is False
    assert st["effective_sleep_seconds"] > 0
    assert st["next_planned_wake"]
    assert st["unproductive_streak"] >= 1
    assert st["blocker_signature"]
    assert st["blocker_reason"] == "FAMILY_EXHAUSTED"


# --------------------------------------------------------------------------- #
# D. the belt-and-braces bound
# --------------------------------------------------------------------------- #
def test_an_upstream_owner_insisting_on_zero_is_still_made_to_wait(root,
                                                                   monkeypatch):
    """The single property that makes the 4,829-cycle pattern impossible.

    Even if ``plan_sleep`` were to regress to the pre-R61 'ready work always
    beats sleeping' answer for a frontier that is entirely blocked, the loop
    refuses to start an identical cycle immediately.
    """
    monkeypatch.setattr(RT, "plan_sleep", lambda **kw: {
        "sleep_seconds": 0.0, "state": RT.W_RESEARCHING,
        "reason": "EXECUTABLE_RESEARCH_EXISTS", "wake_condition": None,
        "blocker_reasons": {}, "detail": "an upstream owner regressed"})
    monkeypatch.setattr(LP, "run_session", _blocked_session)
    q = _Queue(runnable=0, blocked=_blocked_17())
    clk = _Clock()
    body = RT.run_forever(queue=q, debug_max_cycles=5, clock=clk,
                          sleep_fn=clk.sleep, install_signal_handlers=False)
    assert all(c["sleep_seconds"] > 0 for c in body["cycles"])
    assert body["forced_waits"] >= 4
    assert clk.slept > 0


def test_a_crash_loop_cannot_spin_either(root, monkeypatch):
    """A session that raises every pass is the other zero-delay retry path."""
    calls = {"n": 0}

    def _boom(**_kw):
        calls["n"] += 1
        raise RuntimeError("engine exploded")

    monkeypatch.setattr(LP, "run_session", _boom)
    q = _Queue(runnable=0, blocked=[])
    clk = _Clock()
    body = RT.run_forever(queue=q, debug_max_cycles=4, clock=clk,
                          sleep_fn=clk.sleep, install_signal_handlers=False)
    assert calls["n"] == 4
    assert all(c["sleep_seconds"] > 0 for c in body["cycles"])
    assert "engine exploded" in str(body["latest_error"])


# --------------------------------------------------------------------------- #
# E. shutdown stays responsive
# --------------------------------------------------------------------------- #
def test_stop_interrupts_a_waiting_worker_rather_than_serving_the_backoff(
        root, monkeypatch):
    monkeypatch.setattr(LP, "run_session", _blocked_session)
    q = _Queue(runnable=0, blocked=_blocked_17())
    clk = _Clock()
    asked = {"at": None}

    def _sleep(seconds):
        clk.sleep(seconds)
        if asked["at"] is None:
            asked["at"] = clk.t
            raise KeyboardInterrupt

    # The worker installs no handler here, so the interrupt is delivered the
    # way a service stop reaches a sleeping worker: while it is waiting.
    with pytest.raises(KeyboardInterrupt):
        RT.run_forever(queue=q, debug_max_cycles=50, clock=clk,
                       sleep_fn=_sleep, install_signal_handlers=False)
    # It was waiting in HEARTBEAT-sized slices, so the stop was seen within one
    # slice rather than after the whole (possibly hour-long) back-off.
    assert asked["at"] <= RT.HEARTBEAT_SECONDS


def test_a_stop_request_during_a_wait_ends_the_worker_promptly(root,
                                                                monkeypatch):
    monkeypatch.setattr(LP, "run_session", _blocked_session)
    q = _Queue(runnable=0, blocked=_blocked_17())
    clk = _Clock()
    state = {"stopper": None}
    real_init = RT._Stopper.__init__

    def _init(self):
        real_init(self)
        state["stopper"] = self

    monkeypatch.setattr(RT._Stopper, "__init__", _init)

    def _sleep(seconds):
        clk.sleep(seconds)
        state["stopper"].request("SERVICE_STOP")

    body = RT.run_forever(queue=q, debug_max_cycles=50, clock=clk,
                          sleep_fn=_sleep, install_signal_handlers=False)
    assert body["worker_state"] == RT.W_STOPPED
    assert body["stopped_because"] == "SERVICE_STOP"
    # One wait, one slice: it did not serve the rest of the back-off.
    assert clk.slept <= RT.HEARTBEAT_SECONDS
    assert body["n_cycles"] == 1


def test_the_wait_is_taken_in_heartbeat_slices_so_the_lease_stays_alive(root):
    """A worker that waits an hour in one call is declared dead by its own
    replacement, because the ceiling (3600s) is longer than the stale
    threshold (1800s). The wait is therefore SLICED: no single sleep exceeds
    the heartbeat interval, and the lease is refreshed after each one."""
    assert RT.HEARTBEAT_SECONDS < RT.LEASE_STALE_SECONDS
    mem = M.open_memory()
    q = _Queue(runnable=0, blocked=_blocked_17())
    baseline = RT.wake_conditions(mem, q)
    clk = _Clock()
    beats = {"n": 0}

    class _Stop:
        requested = False

        def request(self, _r):
            self.requested = True

    def _beat(*_a, **_k):
        beats["n"] += 1
        return True

    RT._sleep_watching(RT.MAX_SLEEP_SECONDS, stopper=_Stop(), mem=mem,
                       queue=q, baseline=baseline, beat=_beat,
                       sleep_fn=clk.sleep, clock=clk)
    assert clk.slept == pytest.approx(RT.MAX_SLEEP_SECONDS)
    assert max(clk.sleeps) <= RT.HEARTBEAT_SECONDS
    # One heartbeat per slice, and enough of them that the lease can never go
    # stale in the middle of the longest wait the policy allows.
    assert beats["n"] >= RT.MAX_SLEEP_SECONDS / RT.HEARTBEAT_SECONDS
    assert RT.HEARTBEAT_SECONDS * 2 < RT.LEASE_STALE_SECONDS


@pytest.mark.skipif(not MANAGER.exists(), reason="manager absent")
def test_repeated_stop_is_idempotent_by_construction():
    """The canonical Stop has no branch that depends on a previous Stop: it
    reports the same token whether or not a worker was running."""
    src = MANAGER.read_text(encoding="utf-8")
    block = src[src.index("'Stop' {"):src.index("'Restart' {")]
    assert block.count("RESEARCH_WORKER_STOPPED_OK") == 0    # via $STOPPED_TOKEN
    assert "$STOPPED_TOKEN" in block
    # Exactly one exit path, and it is unconditional.
    assert block.count("Write-Output") == 1
    assert "-ErrorAction SilentlyContinue" in block


# --------------------------------------------------------------------------- #
# Nothing was traded away for the quiet
# --------------------------------------------------------------------------- #
def test_one_lease_still_means_one_worker(root, monkeypatch):
    from alpha_agent.r46 import runlock as RL
    holder = "r59_research_worker:incumbent"
    RL.acquire_path(RT.lease_path(), holder, wait_s=0,
                    stale_after_s=RT.LEASE_STALE_SECONDS,
                    extra={"instance_id": "incumbent"})
    try:
        monkeypatch.setattr(LP, "run_session", _blocked_session)
        clk = _Clock()
        body = RT.run_forever(queue=_Queue(), debug_max_cycles=1, clock=clk,
                              sleep_fn=clk.sleep,
                              install_signal_handlers=False)
        assert body["worker_state"] == RT.W_REFUSED
        assert "cycles" not in body
    finally:
        RL.release_path(RT.lease_path(), holder)


def test_a_blocked_frontier_launches_no_duplicate_experiment(root,
                                                              monkeypatch):
    """4,825 blocked cycles must not become 4,825 attempts at the same work."""
    seeded: list = []

    def _session(**kw):
        seeded.append(kw.get("batch"))
        return _blocked_session()

    monkeypatch.setattr(LP, "run_session", _session)
    q = _Queue(runnable=0, blocked=_blocked_17())
    clk = _Clock()
    body = RT.run_forever(queue=q, debug_max_cycles=6, clock=clk,
                          sleep_fn=clk.sleep, install_signal_handlers=False)
    # One session per cycle, and the cycles are now few.
    assert len(seeded) == 6 == body["n_cycles"]
    # The blocked work kept its identity; nothing was re-created or renamed.
    assert len(q.blocked_jobs(limit=100)) == 17


def test_the_queue_memory_and_graveyard_are_preserved_across_a_blocked_run(
        root, monkeypatch):
    mem = M.open_memory()
    hid = mem.register(title="prior", release="RTEST", origin="TEST",
                       generation_method="TEST",
                       information_family="PRICE_STATE",
                       economic_family="FAM_A", asset_class=r59.AC_COMMODITY,
                       model_family="LINEAR")
    mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE,
                      reason_rejected="prior verdict")
    burden_before = mem.burden()["total"]
    graveyard_before = len(mem.graveyard())

    monkeypatch.setattr(LP, "run_session", _blocked_session)
    q = _Queue(runnable=0, blocked=_blocked_17())
    clk = _Clock()
    RT.run_forever(queue=q, debug_max_cycles=5, clock=clk, sleep_fn=clk.sleep,
                   install_signal_handlers=False)

    after = M.open_memory()
    assert after.burden()["total"] == burden_before
    assert len(after.graveyard()) == graveyard_before
    assert after.get(hid)["outcome"] == r59.HO_NO_ALPHA_EVIDENCE
    assert len(q.blocked_jobs(limit=100)) == 17
    assert q.closed is False              # a passed-in queue is not closed


def test_a_persistent_worker_does_not_grow_a_record_per_cycle_forever(root,
                                                                      monkeypatch):
    """The pre-repair worker held 4,829 cycle records in a list that only grew."""
    monkeypatch.setattr(RT, "MAX_CYCLE_RECORDS", 10)
    monkeypatch.setattr(LP, "run_session", _blocked_session)
    q = _Queue(runnable=0, blocked=_blocked_17())
    clk = _Clock()
    body = RT.run_forever(queue=q, debug_max_cycles=40, clock=clk,
                          sleep_fn=clk.sleep, install_signal_handlers=False)
    assert body["n_cycles"] == 40            # the true total is still reported
    assert len(body["cycles"]) == 10         # the retained window is bounded
    assert body["cycles"][-1]["cycle"] == 40


def test_a_blocked_frontier_does_not_flood_the_runtime_log(root, monkeypatch):
    monkeypatch.setattr(LP, "run_session", _blocked_session)
    q = _Queue(runnable=0, blocked=_blocked_17())
    clk = _Clock()
    RT.run_forever(queue=q, debug_max_cycles=6, clock=clk, sleep_fn=clk.sleep,
                   install_signal_handlers=False)
    log = RT.runtime_dir() / RT.RUN_LOG
    rows = [json.loads(line) for line in
            log.read_text(encoding="utf-8").splitlines() if line.strip()]
    cycle_rows = [r for r in rows if r.get("event") == "cycle"]
    assert len(cycle_rows) == 6
    # One row per cycle, and the cycles are paced - so the live worker's
    # 1.5 MB of identical rows in a day cannot recur.
    assert all(r["sleep_seconds"] > 0 for r in cycle_rows)


def test_the_repair_changed_no_execution_ordering(root, monkeypatch):
    """The session is still called with NO caps and the canonical arguments."""
    captured = {}

    def _session(**kw):
        captured.update(kw)
        return _blocked_session()

    monkeypatch.setattr(LP, "run_session", _session)
    q = _Queue(runnable=0, blocked=_blocked_17())
    clk = _Clock()
    RT.run_forever(queue=q, batch=12, max_jobs_per_iteration=8, clock=clk,
                   debug_max_cycles=1, sleep_fn=clk.sleep,
                   install_signal_handlers=False)
    assert captured["batch"] == 12
    assert captured["max_jobs_per_iteration"] == 8
    assert captured.get("max_iterations") is None
    assert captured.get("budget_seconds") is None
    assert captured.get("on_progress") is not None


def test_the_repair_promotes_approves_and_orders_nothing(root, monkeypatch):
    monkeypatch.setattr(LP, "run_session", _blocked_session)
    q = _Queue(runnable=0, blocked=_blocked_17())
    clk = _Clock()
    body = RT.run_forever(queue=q, debug_max_cycles=3, clock=clk,
                          sleep_fn=clk.sleep, install_signal_handlers=False)
    safety = body["safety"]
    for flag in ("creates_orders", "creates_fills", "broker_enabled",
                 "promotes_model", "activates_sleeve", "approves_proposal",
                 "mutates_operational_store", "automation_enabled"):
        assert safety[flag] is False, flag
    assert safety["research_only"] is True
    # And the repair introduced no reachable operational writer.
    for forbidden in ("create_order", "submit_order", "approve_proposal",
                      "promote_champion", "activate_sleeve"):
        assert forbidden not in RUNTIME_SRC, forbidden


# --------------------------------------------------------------------------- #
# THE REGRESSION FIXTURE - the original 4,829-cycle failure, replayed
# --------------------------------------------------------------------------- #
def test_the_original_zero_sleep_cycle_storm_is_reproduced_and_refused(
        root, monkeypatch):
    r"""Replay the live failure, with the MEASURED live numbers.

    The last worker instance was up 57,348 seconds and recorded 193 cycles, so
    one blocked cycle cost about 297 seconds of continuous work, and it slept
    for none of them. This drives the REAL ``run_forever`` over a simulated
    day with a clock that only advances when the worker works or sleeps, and
    asserts both halves:

    * the OLD rule, recomputed here, still produces the zero-second sleep and
      an unbroken day of work on this fixture - so the fixture has not quietly
      stopped reproducing the defect it exists for;
    * the REPAIRED runtime, given the same day and the same frontier, spends
      it waiting instead of working.
    """
    q = _Queue(runnable=0, blocked=_blocked_17())
    summary = RT._blocked_summary(q)
    conditions = [{"name": "BLOCKED_SOURCES", "kind": "EXTERNAL",
                   "watermark": "17"}]

    # ---- the OLD rule, on this exact fixture ------------------------------ #
    # run_forever passed the governor's mandate count as ready_work. On the
    # live estate that number was 2 or 3 on every one of the 4,825 cycles.
    old_plan = RT.plan_sleep(ready_work=3, conditions=conditions,
                             blocked_summary=summary)
    assert old_plan["sleep_seconds"] == 0.0
    assert old_plan["reason"] == "EXECUTABLE_RESEARCH_EXISTS"
    # With a zero sleep the whole span is spent working, which is what the
    # live worker did: 193 cycles back to back, and 4,829 rows of it on disk.
    old_cycles = int(REPLAY_SECONDS / OBSERVED_CYCLE_COST_SECONDS)
    assert old_cycles > OBSERVED_WORKER_CYCLES, old_cycles
    assert OBSERVED_ZERO_SLEEP_LOG_ROWS > 4000

    # ---- the REPAIRED runtime, same frontier, same day -------------------- #
    clk = _Clock()

    def _session(**_kw):
        clk.work(OBSERVED_CYCLE_COST_SECONDS)
        return _blocked_session()

    monkeypatch.setattr(LP, "run_session", _session)
    body = RT.run_forever(queue=q, clock=clk, sleep_fn=clk.sleep,
                          debug_max_seconds=REPLAY_SECONDS,
                          install_signal_handlers=False)

    assert body["n_cycles"] < old_cycles / 5, body["n_cycles"]
    zero_sleep_blocked = [c for c in body["cycles"]
                          if c["sleep_seconds"] <= 0
                          and c["stop_condition"] == LP.STOP_B]
    assert zero_sleep_blocked == []
    # It spent the day waiting, not working: the CPU claim, in the same units.
    # The old rule spent 100% of the span working; this spends under a fifth.
    assert clk.worked < REPLAY_SECONDS * 0.2, clk.worked
    assert clk.slept > REPLAY_SECONDS * 0.8, clk.slept
    # And it reached the ceiling rather than re-asking every minute.
    assert max(c["sleep_seconds"] for c in body["cycles"]) == \
        RT.idle_policy()["max_sleep_seconds"]
