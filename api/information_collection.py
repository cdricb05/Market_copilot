"""
api/information_collection.py — Release 29 CANONICAL CONTINUOUS COLLECTION OWNER.

THE ONE composition owner for continuous, governed information acquisition.

Release 28 built the processing path and proved one event can flow through it.
It left every collector operator-initiated, so in production the path was starved:
the live surface read ``NOT_RUN / Last cycle never / Sources fresh 1 of 17``.
Release 29 keeps information FLOWING INTO that path.

WHAT THIS OWNER DOES
--------------------
    LOAD SERVICE STATE
        -> LOAD PORTFOLIO / ATTENTION UNIVERSE   (api.portfolio_state, api.universe_scoring)
        -> RESOLVE MARKET SESSION                (engine.market_hours)
        -> BUILD DUE PLAN                        (engine.collection_cadence)
        -> FOR EACH DUE SOURCE: circuit -> budget -> COLLECT via its EXISTING owner
        -> ONE consolidated Release-28 event pass (api.event_signal_refresh)
        -> PERSIST ITERATION RECEIPT
        -> CALCULATE NEXT WAKE

WHAT THIS OWNER DOES NOT DO
---------------------------
It owns no provider client, no scoring, no opportunity cost, no reassessment, no
proposal, no market-session arithmetic and no freshness vocabulary. Every one of
those already has a canonical owner and this module CALLS it:

    alpha_agent.ingestion.run_ingestion        Stage-2 corpus collectors
    alpha_agent.feed_registry.run_news_rss     Stage-3.5 official RSS/Atom
    api.event_fabric.capture_market_quotes     delayed-quote live adapter
    api.event_fabric.capture_gdelt_news        GDELT live adapter
    api.event_signal_refresh                   THE Release-28 event orchestrator
    engine.collection_cadence                  cadence / due / backoff policy
    engine.market_hours                        market-clock facts

There is exactly ONE collection orchestrator (this module) and exactly ONE event
orchestrator (Release 28's). This module never forks either.

SAFETY — COLLECTION AUTOMATION IS NOT EXECUTION AUTOMATION
----------------------------------------------------------
Once the operator explicitly installs and starts the service, it may continuously
collect information, persist immutable events, refresh signals and risk, run
holding opportunity cost, run portfolio reassessment and build review-only
proposals. It may NEVER approve a proposal, confirm a target, create/confirm/fill/
cancel an order, run a controlled rebalance, call a broker, run Daily Close, run
the full Daily Research Cycle, or promote a model. Those invariants are asserted
by ``collection_safety_contract()`` and enforced by the architecture audit.
"""
from __future__ import annotations

import json
import os
import socket
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from ..engine import collection_cadence as cad
from ..engine import market_hours as mh
from . import event_fabric as fabric
from . import event_signal_refresh as esr
from . import source_capability as scap

PHASE = "RELEASE29"
COMPOSITION_OWNER = "api.information_collection"
SCHEMA_VERSION = "1.0.0"
SERVICE_ID = "PAPER_TRADER_INFORMATION_COLLECTION"
SERVICE_CONTRACT_ID = "paper_trader.information_collection_service/1"

#: The Windows Scheduled Task identity. Follows the existing AlphaAgent-* naming
#: convention already established by scripts/install_alpha_agent_tasks.ps1.
SCHEDULED_TASK_NAME = "PaperTrader-InformationCollection"

#: Enabling collection automation is an explicit operator act, exactly once.
ENABLE_CONFIRM_TOKEN = "CONFIRM_ENABLE_INFORMATION_COLLECTION"

COLLECTION_DIR_ENV = "PAPER_TRADER_COLLECTION_DIR"
_DEFAULT_COLLECTION_DIR = Path(r"D:\Stock_Prediction_app_data\information_collection")

NOW_ENV = "PAPER_TRADER_COLLECTION_NOW"

# Service lifecycle states.
SVC_RUNNING = "RUNNING"
SVC_STOPPED = "STOPPED"
SVC_DEGRADED = "DEGRADED"
SVC_NEVER_STARTED = "NEVER_STARTED"
SERVICE_STATES = (SVC_RUNNING, SVC_STOPPED, SVC_DEGRADED, SVC_NEVER_STARTED)

#: R55.2.1 — the ONE module that decides what a loaded release identity IS and
#: whether it matches the deployed source. This module records and republishes
#: the worker's capture; it never compares two commits itself.
_RELEASE_IDENTITY_OWNER = "api.runtime_identity"

#: A heartbeat older than this means the worker is not alive even if a lock file
#: is still on disk (a hard kill leaves the lock behind).
HEARTBEAT_STALE_SECONDS = 300.0
#: PROGRESS evidence older than this means WORK has stopped advancing.
#:
#: Release 29.1 measured health from a heartbeat stamped ONCE, before the
#: iteration began, so a real 5.5-minute collection pass reported DEGRADED at
#: ~309 seconds while the worker was alive and legitimately busy. The repair is
#: NOT a wider tolerance — this budget is deliberately IDENTICAL to
#: ``HEARTBEAT_STALE_SECONDS``. What changed is WHAT the clock measures: no
#: longer "seconds since the iteration started" but "seconds since the canonical
#: collection path last finished a bounded unit of work". A hung worker still
#: trips at the same 300 seconds, now counted from its last proven progress,
#: which is never later than the old iteration-start stamp. Detection is
#: therefore at least as strong, and no longer fires on healthy work.
PROGRESS_STALL_SECONDS = 300.0
#: A progress checkpoint costs one counter increment; PERSISTING it costs a file
#: write. Checkpoints fire per scanned file and per HTTP attempt, so the stamp is
#: written at most this often. ``progress_seq`` still advances on every single
#: checkpoint — the throttle bounds I/O, never the evidence.
PROGRESS_WRITE_MIN_SECONDS = 5.0
#: An abandoned lock older than this may be reclaimed by a new worker.
LOCK_TAKEOVER_SECONDS = 900.0

# What the worker is DOING, as distinct from whether the service is running.
# RUNNING/IDLE and RUNNING/BUSY are both healthy; STALLED and DEAD are not.
ACT_IDLE = "IDLE"
ACT_BUSY = "BUSY"
ACT_STALLED = "STALLED"
ACT_DEAD = "DEAD"
ACT_UNKNOWN = "UNKNOWN"
ACT_NOT_RUNNING = "NOT_RUNNING"
WORKER_ACTIVITY_STATES = (ACT_IDLE, ACT_BUSY, ACT_STALLED, ACT_DEAD,
                          ACT_UNKNOWN, ACT_NOT_RUNNING)
#: Activity verdicts that mean the worker is healthy.
HEALTHY_ACTIVITY_STATES = (ACT_IDLE, ACT_BUSY)

#: Bounded catch-up: however long the machine slept, ONE iteration collects at
#: most this many sources so a wake-from-sleep is a pass, never a request storm.
MAX_SOURCES_PER_ITERATION = 6
#: Bounded iteration history kept on disk.
MAX_ITERATION_RECEIPTS = 500

_REPO_ROOT = Path(__file__).resolve().parents[1]
_STAGE2_CONFIG = _REPO_ROOT / "configs" / "alpha_agent" / "stage2_ingestion.json"
_STAGE35_CONFIG = _REPO_ROOT / "configs" / "alpha_agent" / "stage3_5_news_rss.json"
_NEWS_FEEDS_CONFIG = _REPO_ROOT / "configs" / "alpha_agent" / "news_rss_feeds.json"

#: Which lane collects each source. The service dispatches to the EXISTING owner;
#: it never re-implements a provider client.
LANE_STAGE2 = "STAGE2_INGESTION"
LANE_NEWS_RSS = "STAGE35_NEWS_RSS"
LANE_LIVE_ADAPTER = "RELEASE28_LIVE_ADAPTER"
COLLECTOR_LANE = {
    "norgate_local": LANE_STAGE2, "eodhd": LANE_STAGE2,
    "eodhd_analyst": LANE_STAGE2, "sec_edgar": LANE_STAGE2,
    "finra": LANE_STAGE2, "nasdaq_trader": LANE_STAGE2,
    "fred_alfred": LANE_STAGE2, "us_treasury": LANE_STAGE2,
    "news_rss": LANE_NEWS_RSS,
    "yahoo_delayed_quote": LANE_LIVE_ADAPTER, "gdelt": LANE_LIVE_ADAPTER,
}


# --------------------------------------------------------------------------- #
# Clock + small helpers
# --------------------------------------------------------------------------- #
def _now() -> datetime:
    """UTC now, overridable by NOW_ENV so replay and tests are hermetic."""
    override = os.environ.get(NOW_ENV)
    if override:
        try:
            dt = datetime.fromisoformat(override.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


def _parse(value: Any) -> Optional[datetime]:
    return mh.parse_et(value)


def _age(value: Any, now: datetime) -> Optional[float]:
    dt = _parse(value)
    if dt is None:
        return None
    try:
        return max(0.0, (now - dt).total_seconds())
    except TypeError:
        return None


def now_utc() -> datetime:
    """The service clock (public seam). Overridable by NOW_ENV for hermetic runs."""
    return _now()


def utc_iso(dt: Optional[datetime] = None) -> Optional[str]:
    """ISO-8601 stamp for a datetime, defaulting to the service clock."""
    return _iso(dt if dt is not None else _now())


def collection_root(root=None) -> Path:
    if root is not None:
        return Path(root)
    env = os.environ.get(COLLECTION_DIR_ENV)
    return Path(env) if env else _DEFAULT_COLLECTION_DIR


def _state_path(root=None) -> Path:
    return collection_root(root) / "collection_service_state.json"


def _runtime_path(root=None) -> Path:
    return collection_root(root) / "source_runtime_health.json"


def _iterations_path(root=None) -> Path:
    return collection_root(root) / "collection_iteration_history.json"


def _lock_path(root=None) -> Path:
    return collection_root(root) / "collection_service.lock"


def logs_root(root=None) -> Path:
    return collection_root(root) / "logs"


def _read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=1, sort_keys=True, default=str),
                   encoding="utf-8")
    os.replace(tmp, path)


# --------------------------------------------------------------------------- #
# Durable service state
# --------------------------------------------------------------------------- #
def _blank_service_state() -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "service_id": SERVICE_ID,
        "collection_automation_enabled": False,
        "enabled_at": None,
        "enabled_by": None,
        "instance_id": None,
        "pid": None,
        "host": None,
        "started_at": None,
        # R55.2 — WHICH APPLICATION RELEASE THIS WORKER LOADED.
        #
        # A long-lived Python process resolves its imports once and holds them
        # for life. On 2026-09-01 this worker started 9h34m BEFORE the R54.1
        # governance gate was committed and went on executing the older module
        # graph while the repository, the backend and every test reported the
        # newer code — invisibly, because a heartbeat proves a process is alive
        # and says nothing about which code it is running. The identity is
        # captured ONCE by ``api.runtime_identity`` at worker start and is
        # replaced only when a genuinely new worker process registers.
        "loaded_release": None,
        "heartbeat_at": None,
        "stopped_at": None,
        "graceful_shutdown": None,
        "loop_count": 0,
        "restart_count": 0,
        "last_iteration_started_at": None,
        "last_iteration_finished_at": None,
        "last_iteration_id": None,
        "current_source": None,
        # PROGRESS EVIDENCE — the proof that a BUSY worker is a healthy one.
        # ``iteration_in_flight`` says an iteration is open; the progress stamp
        # and the monotonic sequence say the canonical path is still finishing
        # bounded units of work inside it.
        "iteration_in_flight": False,
        "current_iteration_id": None,
        "progress_at": None,
        "progress_seq": 0,
        "progress_step": None,
        "progress_detail": None,
        "progress_iteration_id": None,
        "next_wake_at": None,
        "last_collection_success_at": None,
        "last_new_information_at": None,
        "last_material_information_at": None,
        "last_event_cycle_at": None,
        "last_event_cycle_state": None,
        "last_reassessment_at": None,
        "last_target_change_at": None,
        "last_error": None,
    }


def load_service_state(*, root=None) -> dict:
    state = _read_json(_state_path(root)) or {}
    merged = _blank_service_state()
    merged.update({k: v for k, v in state.items() if k in merged})
    return merged


def save_service_state(state: dict, *, root=None) -> None:
    _atomic_write_json(_state_path(root), state)


def load_source_runtime_state(*, root=None) -> dict:
    return (_read_json(_runtime_path(root)) or {}).get("sources") or {}


def save_source_runtime_state(sources: dict, *, root=None) -> None:
    _atomic_write_json(_runtime_path(root), {
        "schema_version": SCHEMA_VERSION, "service_id": SERVICE_ID,
        "updated_at": _iso(_now()), "sources": sources})


def set_collection_automation(*, enabled: bool, confirm: Optional[str] = None,
                              requested_by: Optional[str] = None, root=None) -> dict:
    """Arm or disarm COLLECTION automation. Never touches execution automation.

    Enabling is token-gated because it is the ONE governance boundary of this
    release: the operator authorises continuous collection once, and after that
    the service may keep the Release-28 path fed without another button press.
    """
    if enabled and str(confirm or "") != ENABLE_CONFIRM_TOKEN:
        return {"ok": False, "collection_automation_enabled": False,
                "confirm_required": ENABLE_CONFIRM_TOKEN,
                "message": ("Enabling continuous collection is an explicit operator "
                            "act. Pass confirm=%r." % ENABLE_CONFIRM_TOKEN)}
    state = load_service_state(root=root)
    state["collection_automation_enabled"] = bool(enabled)
    stamp = _iso(_now())
    if enabled:
        state["enabled_at"] = stamp
        state["enabled_by"] = str(requested_by or "operator")
    else:
        state["enabled_at"] = None
    save_service_state(state, root=root)
    return {"ok": True, "collection_automation_enabled": bool(enabled),
            "enabled_at": state["enabled_at"],
            "execution_automation_enabled": False,
            "safety": collection_safety_contract()}


# --------------------------------------------------------------------------- #
# Singleton / service identity — starting a second worker FAILS CLOSED
# --------------------------------------------------------------------------- #
# Liveness is a QUERY. On Windows it must never be expressed as os.kill(pid, 0).
#
# CPython's os.kill dispatches on Windows by SIGNAL VALUE, and signal 0 IS
# CTRL_C_EVENT (1 is CTRL_BREAK_EVENT). os.kill(pid, 0) therefore probes
# nothing: it calls GenerateConsoleCtrlEvent(CTRL_C_EVENT, pid) and delivers a
# REAL console Ctrl+C to the process group named by pid. acquire_service_lock
# records os.getpid() as the holder, so the very next liveness check sent a
# Ctrl+C to this process's own console group. Measured: the call succeeds, and
# ~11 ms later SIGINT is delivered asynchronously - which is why the resulting
# KeyboardInterrupt surfaced in the test AFTER the one that caused it, and why
# three full pytest runs died at the same node with an interrupt nobody typed.
_WIN_PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
_WIN_SYNCHRONIZE = 0x00100000
_WIN_WAIT_OBJECT_0 = 0x00000000
_WIN_WAIT_TIMEOUT = 0x00000102
_WIN_ERROR_ACCESS_DENIED = 5
_WIN_ERROR_INVALID_PARAMETER = 87


def _pid_alive_windows(n: int) -> Optional[bool]:
    """Ask Windows whether a pid is running. No signal, no console event."""
    import ctypes
    from ctypes import wintypes

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    k32.OpenProcess.restype = wintypes.HANDLE
    k32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    k32.WaitForSingleObject.restype = wintypes.DWORD
    k32.CloseHandle.argtypes = [wintypes.HANDLE]
    k32.CloseHandle.restype = wintypes.BOOL

    handle = k32.OpenProcess(
        _WIN_PROCESS_QUERY_LIMITED_INFORMATION | _WIN_SYNCHRONIZE, False, n)
    if not handle:
        err = ctypes.get_last_error()
        if err == _WIN_ERROR_INVALID_PARAMETER:
            return False   # there is no process with that id
        if err == _WIN_ERROR_ACCESS_DENIED:
            return True    # it exists; this account simply may not open it
        return None
    try:
        rc = k32.WaitForSingleObject(handle, 0)
        if rc == _WIN_WAIT_TIMEOUT:
            return True    # not signalled == still running
        if rc == _WIN_WAIT_OBJECT_0:
            return False   # signalled == the process has exited
        return None
    finally:
        k32.CloseHandle(handle)


def _pid_alive(pid: Any) -> Optional[bool]:
    """True/False if determinable, None if the platform will not say. Never raises."""
    try:
        n = int(pid)
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return False
    try:
        if sys.platform == "win32":
            return _pid_alive_windows(n)
        os.kill(n, 0)  # POSIX only: there, signal 0 really is a liveness probe.
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except (OSError, AttributeError, ValueError):
        return None


def read_service_lock(*, root=None) -> Optional[dict]:
    return _read_json(_lock_path(root))


def acquire_service_lock(*, root=None, instance_id: Optional[str] = None,
                         pid: Optional[int] = None, now: Optional[datetime] = None
                         ) -> dict:
    """Acquire the SINGLE production collection worker slot. Fail-closed.

    A second worker is rejected while the incumbent's heartbeat is live. A lock
    left behind by a hard kill is reclaimed only after the heartbeat has been
    silent longer than the takeover window AND the recorded PID is gone.
    """
    now = now or _now()
    iid = str(instance_id or uuid.uuid4())
    mypid = int(pid if pid is not None else os.getpid())
    path = _lock_path(root)
    existing = _read_json(path)
    if existing:
        holder_pid = existing.get("pid")
        alive = _pid_alive(holder_pid)
        age = _age(existing.get("heartbeat_at") or existing.get("acquired_at"), now)
        same_process = (holder_pid == mypid)
        # Reclaim requires BOTH conditions in the docstring to be satisfied:
        # the heartbeat silent past the takeover window AND the holder's pid
        # gone. So REFUSE whenever either one still holds - the holder may be
        # alive (or its liveness is not determinable, which fails closed), or
        # the takeover window has not elapsed. Expressing this as one `and`
        # chain instead let a stale-but-LIVE holder lose its slot, which is the
        # concurrent-worker outcome the single-flight gate exists to prevent.
        if not same_process and (alive is not False
                                 or age is None
                                 or age < LOCK_TAKEOVER_SECONDS):
            return {
                "acquired": False, "reason": "SINGLE_FLIGHT_LOCK_HELD",
                "holder": {k: existing.get(k) for k in
                           ("instance_id", "pid", "host", "acquired_at",
                            "heartbeat_at")},
                "heartbeat_age_seconds": (None if age is None else round(age, 1)),
                "message": ("Another collection worker (pid %s) already holds the "
                            "single production slot. Overlapping provider "
                            "iterations are refused by design."
                            % existing.get("pid")),
            }
        reclaimed = {"prior_instance_id": existing.get("instance_id"),
                     "prior_pid": holder_pid, "prior_pid_alive": alive,
                     "prior_heartbeat_age_seconds":
                         (None if age is None else round(age, 1))}
    else:
        reclaimed = None
    payload = {"instance_id": iid, "pid": mypid, "host": socket.gethostname(),
               "service_id": SERVICE_ID, "acquired_at": _iso(now),
               "heartbeat_at": _iso(now), "reclaimed": reclaimed}
    _atomic_write_json(path, payload)
    return {"acquired": True, "instance_id": iid, "pid": mypid,
            "reclaimed": reclaimed, "lock_path": str(path)}


def acquire_service_lock_with_wait(*, root=None,
                                   instance_id: Optional[str] = None,
                                   pid: Optional[int] = None,
                                   max_wait_seconds: Optional[float] = None,
                                   poll_seconds: float = 10.0,
                                   should_stop=None,
                                   _sleep=None) -> dict:
    """Acquire the single worker slot, WAITING OUT a dead holder's takeover
    window instead of failing terminally.

    Release 53, after the 2026-08-28 outage: at a logon the Scheduled Task
    relaunches the worker while the previous session's worker has just been
    killed. Its lock heartbeat is seconds old, so the strict acquire refuses
    (correctly - freshness alone cannot prove death is permanent), the task
    exits 3, and the LogonTrigger never fires again: continuous collection
    dies silently until the next logon. The fix is NOT to weaken the
    single-flight rule - it is to keep the refused starter ALIVE while the
    evidence resolves:

    * holder pid provably ALIVE (or liveness undeterminable) -> refuse
      immediately, exactly as before. Two live workers must never overlap and
      an unknown holder fails closed.
    * holder pid provably DEAD -> wait in bounded slices and re-attempt; the
      strict acquire itself grants the slot once the heartbeat has been
      silent past ``LOCK_TAKEOVER_SECONDS``. The reclaim rule is unchanged -
      this wrapper only refuses to treat "not yet" as "never".

    ``should_stop`` lets the caller honour a shutdown signal during the wait;
    ``_sleep`` is injected by tests. Bounded: at most the takeover window
    plus one poll interval, then one final verdict is returned.
    """
    import time as _time
    sleep = _sleep or _time.sleep
    deadline = float(max_wait_seconds if max_wait_seconds is not None
                     else LOCK_TAKEOVER_SECONDS + poll_seconds)
    waited = 0.0
    attempts = 0
    while True:
        res = acquire_service_lock(root=root, instance_id=instance_id, pid=pid)
        attempts += 1
        res["acquire_attempts"] = attempts
        res["waited_seconds"] = round(waited, 1)
        if res.get("acquired"):
            return res
        holder_pid = (res.get("holder") or {}).get("pid")
        if _pid_alive(holder_pid) is not False:
            res["wait_refused_reason"] = (
                "holder pid %s is alive or undeterminable; failing closed "
                "immediately (single-flight)" % holder_pid)
            return res
        if callable(should_stop) and should_stop():
            res["wait_refused_reason"] = "stop requested during lock wait"
            return res
        if waited >= deadline:
            res["wait_refused_reason"] = (
                "takeover window did not resolve within %.0fs" % deadline)
            return res
        step = min(float(poll_seconds), deadline - waited)
        sleep(step)
        waited += step


def heartbeat(*, root=None, instance_id: str, now: Optional[datetime] = None,
              loop_count: Optional[int] = None, current_source: Any = None,
              next_wake_at: Any = None) -> dict:
    """Stamp the worker heartbeat into BOTH the lock and the service state."""
    now = now or _now()
    lock = _read_json(_lock_path(root)) or {}
    if lock.get("instance_id") == instance_id:
        lock["heartbeat_at"] = _iso(now)
        _atomic_write_json(_lock_path(root), lock)
    state = load_service_state(root=root)
    state["heartbeat_at"] = _iso(now)
    state["instance_id"] = instance_id
    if loop_count is not None:
        state["loop_count"] = int(loop_count)
    state["current_source"] = current_source
    if next_wake_at is not None:
        state["next_wake_at"] = next_wake_at
    save_service_state(state, root=root)
    return {"heartbeat_at": state["heartbeat_at"], "instance_id": instance_id}


def record_progress(*, root=None, step: str, detail: Any = None,
                    iteration_id: Optional[str] = None,
                    instance_id: Optional[str] = None,
                    seq: Optional[int] = None,
                    in_flight: Optional[bool] = None,
                    now: Optional[datetime] = None) -> dict:
    """Stamp PROOF THAT WORK ADVANCED. The ONE writer of progress evidence.

    This is not a second heartbeat and not a timer: it is only ever called by the
    canonical collection path when a bounded unit of work — an HTTP attempt, a
    source, a scanned partition file, one orchestration step — has actually
    happened. ``progress_seq`` is monotonic, so a reader can prove the evidence
    is ADVANCING rather than merely being rewritten.

    ``in_flight`` defaults to None meaning LEAVE IT ALONE. Whether an iteration
    is open is the ORCHESTRATOR'S fact, not a checkpoint's: the first version of
    this function defaulted it, and every checkpoint then quietly re-answered a
    question it does not own — the flag was cleared microseconds after the
    iteration opened it, and a busy worker read IDLE all the way through a real
    316-second pass.
    """
    now = now or _now()
    state = load_service_state(root=root)
    state["progress_at"] = _iso(now)
    state["progress_step"] = (str(step)[:120] if step is not None else None)
    state["progress_detail"] = (str(detail)[:300] if detail is not None else None)
    # THE DOCUMENT owns the sequence, and it only ever goes up. Two counters
    # write this field — the orchestrator's open/close pair and the worker's
    # long-lived reporter, whose count is a LIFETIME total — so a caller-supplied
    # value is a floor, never an assignment. Letting the reporter's number win
    # outright made the sequence step BACKWARDS by one at every iteration
    # boundary, which would falsify the one thing it exists to prove.
    current = int(state.get("progress_seq") or 0)
    proposed = int(seq) if seq is not None else current + 1
    state["progress_seq"] = proposed if proposed > current else current + 1
    if in_flight is not None:
        state["iteration_in_flight"] = bool(in_flight)
        if not in_flight:
            state["current_iteration_id"] = None
    if iteration_id is not None:
        state["progress_iteration_id"] = iteration_id
        if state.get("iteration_in_flight"):
            state["current_iteration_id"] = iteration_id
    if instance_id is not None:
        state["instance_id"] = instance_id
    save_service_state(state, root=root)
    return {"progress_at": state["progress_at"], "progress_seq":
            state["progress_seq"], "progress_step": state["progress_step"],
            "iteration_in_flight": state["iteration_in_flight"]}


class ProgressReporter:
    """The progress callback the worker hands to ``run_collection_iteration``.

    NOT a thread. Nothing here wakes up on its own, so it can never report a
    hung process as healthy: every stamp is the trailing edge of work the
    canonical path really performed. The sequence advances on EVERY checkpoint;
    the disk write is throttled to ``min_write_seconds`` for repeats of the same
    named step, and always forced when the step changes, so a per-file
    checkpoint is free while a lane transition is immediately visible.
    """

    def __init__(self, *, root=None, instance_id: Optional[str] = None,
                 min_write_seconds: float = PROGRESS_WRITE_MIN_SECONDS,
                 clock: Optional[Callable[[], float]] = None,
                 writer: Optional[Callable] = None) -> None:
        self.root = root
        self.instance_id = instance_id
        self.min_write_seconds = float(min_write_seconds)
        self._clock = clock or time.monotonic
        self._writer = writer or record_progress
        self.seq = 0
        self.writes = 0
        self.iteration_id: Optional[str] = None
        #: None means "an ordinary checkpoint does not answer this question".
        #: Only begin()/finish() ever assert it, and the orchestrator asserts it
        #: directly — a checkpoint reports progress, never lifecycle.
        self.in_flight: Optional[bool] = None
        self._last_write = None
        self._last_step = None

    # -- lifecycle ---------------------------------------------------------- #
    def begin(self, iteration_id: str) -> None:
        """Open an iteration. The in-flight flag is what makes BUSY provable."""
        self.iteration_id = iteration_id
        self.in_flight = True
        self("ITERATION_BEGIN", detail=iteration_id, force=True)
        self.in_flight = None

    def finish(self, detail: Any = None) -> None:
        """Close the iteration. A closed iteration is IDLE, never BUSY."""
        self.in_flight = False
        self("ITERATION_END", detail=detail, force=True)
        self.in_flight = None
        self.iteration_id = None

    # -- the callback ------------------------------------------------------- #
    def __call__(self, step: str, detail: Any = None, force: bool = False) -> None:
        self.seq += 1
        now = self._clock()
        changed = (step != self._last_step)
        due = (self._last_write is None
               or (now - self._last_write) >= self.min_write_seconds)
        self._last_step = step
        if not (force or changed or due):
            return
        try:
            self._writer(root=self.root, step=step, detail=detail,
                         iteration_id=self.iteration_id,
                         instance_id=self.instance_id, seq=self.seq,
                         in_flight=self.in_flight)
        except Exception:  # noqa: BLE001 - evidence must never break collection
            return
        self._last_write = now
        self.writes += 1


def _safe_progress(progress_fn: Optional[Callable]) -> Callable:
    """Adapt any caller-supplied callback into one that can never raise."""
    if progress_fn is None:
        return lambda step, detail=None: None

    def _emit(step: str, detail: Any = None) -> None:
        try:
            progress_fn(step, detail=detail)
        except Exception:  # noqa: BLE001 - progress is evidence, not control flow
            pass
    return _emit


def _progress_transport(inner: Callable, progress: Callable, *, lane: str
                        ) -> Callable:
    """Wrap an HTTP transport so every ATTEMPT is a progress checkpoint.

    ``run_ingestion`` and ``run_news_rss`` already take an injectable transport,
    so the finest-grained blocking unit in the collection lanes reports progress
    without either collector owner learning what a heartbeat is. One attempt is
    bounded by its own socket timeout, so this is what keeps a multi-minute lane
    provably alive between sources.
    """
    def _transport(request, timeout):
        url = str((request or {}).get("url") or "")
        progress(lane, detail="request %s" % url[:200])
        try:
            return inner(request, timeout)
        finally:
            progress(lane, detail="completed %s" % url[:200])
    return _transport


def _mark_iteration_in_flight(*, root=None, iteration_id: str,
                              instance_id: Optional[str],
                              started_at: Optional[str],
                              now: Optional[datetime] = None) -> None:
    state = load_service_state(root=root)
    state["iteration_in_flight"] = True
    state["current_iteration_id"] = iteration_id
    state["last_iteration_started_at"] = started_at
    state["progress_at"] = _iso(now or _now())
    state["progress_step"] = "ITERATION_BEGIN"
    state["progress_detail"] = iteration_id
    state["progress_iteration_id"] = iteration_id
    state["progress_seq"] = int(state.get("progress_seq") or 0) + 1
    if instance_id:
        state["instance_id"] = instance_id
    save_service_state(state, root=root)


def clear_iteration_in_flight(*, root=None) -> None:
    """Close the iteration even when it FAILED. A crash must read IDLE-with-an-
    error, never BUSY forever."""
    try:
        state = load_service_state(root=root)
        state["iteration_in_flight"] = False
        state["current_iteration_id"] = None
        save_service_state(state, root=root)
    except OSError:
        pass


def release_service_lock(*, root=None, instance_id: str,
                         graceful: bool = True) -> dict:
    """Release the worker slot and record a clean shutdown."""
    path = _lock_path(root)
    lock = _read_json(path)
    released = False
    if lock and lock.get("instance_id") == instance_id:
        try:
            path.unlink()
            released = True
        except OSError:
            pass
    state = load_service_state(root=root)
    state["stopped_at"] = _iso(_now())
    state["graceful_shutdown"] = bool(graceful)
    state["current_source"] = None
    state["iteration_in_flight"] = False
    state["current_iteration_id"] = None
    save_service_state(state, root=root)
    return {"released": released, "graceful": bool(graceful)}


def register_worker_start(*, root=None, instance_id: str, pid: int,
                          now: Optional[datetime] = None,
                          loaded_release: Optional[dict] = None) -> dict:
    """Record a worker start, counting restarts without losing prior evidence.

    R55.2 — a start is also the ONE moment at which this worker's loaded release
    identity can be recorded truthfully, so it is stamped here and replaced only
    by the next genuinely new worker process. ``loaded_release`` is injectable
    for hermetic tests; the default asks the ONE identity owner for the frozen
    capture this process already made.
    """
    now = now or _now()
    state = load_service_state(root=root)
    if state.get("started_at"):
        state["restart_count"] = int(state.get("restart_count") or 0) + 1
    if loaded_release is None:
        try:
            from . import runtime_identity as _rid
            loaded_release = _rid.loaded_identity()
        except Exception:  # noqa: BLE001 - identity never blocks a worker start
            loaded_release = None
    state.update({"instance_id": instance_id, "pid": int(pid),
                  "host": socket.gethostname(), "started_at": _iso(now),
                  "loaded_release": loaded_release,
                  "heartbeat_at": _iso(now), "stopped_at": None,
                  "graceful_shutdown": None, "current_source": None,
                  # A NEW worker has proven no work yet. Inheriting the dead
                  # worker's in-flight flag would report the fresh process as
                  # BUSY (or, once its stamp aged out, STALLED) before it had
                  # done anything at all.
                  "iteration_in_flight": False, "current_iteration_id": None,
                  "progress_at": None, "progress_seq": 0,
                  "progress_step": None, "progress_detail": None,
                  "progress_iteration_id": None})
    save_service_state(state, root=root)
    return state


def resolve_service_lifecycle(state: dict, lock: Optional[dict],
                              now: datetime) -> dict:
    """ONE authoritative RUNNING / STOPPED / DEGRADED / NEVER_STARTED verdict,
    and ONE authoritative IDLE / BUSY / STALLED / DEAD activity verdict.

    A worker is healthy in two different ways and unhealthy in two others, and
    Release 29.1 could not tell them apart:

        RUNNING + IDLE     between iterations, heartbeat fresh
        RUNNING + BUSY     inside a long iteration that is STILL ADVANCING
        DEGRADED + STALLED the process exists but work stopped advancing
        DEGRADED + DEAD    the process is gone

    The distinction is evidence, not tolerance. BUSY requires an OPEN iteration
    whose progress stamp advanced within ``PROGRESS_STALL_SECONDS`` — the same
    budget the heartbeat has always had. Anything that cannot be proven (no
    heartbeat, an open iteration with no progress stamp) fails CLOSED to
    DEGRADED.
    """
    if not state.get("started_at"):
        return {"service_state": SVC_NEVER_STARTED,
                "worker_activity": ACT_NOT_RUNNING,
                "activity_reason": "No worker has ever run on this machine.",
                "reason": ("The collection service has never been started on this "
                           "machine. Install and start it to begin continuous "
                           "collection."),
                "heartbeat_age_seconds": None, "progress_age_seconds": None,
                "progress_seq": None, "progress_step": None,
                "iteration_in_flight": False,
                "progress_stall_after_seconds": PROGRESS_STALL_SECONDS,
                "worker_pid": None, "worker_alive": None,
                "instance_id": None, "started_at": None,
                "loaded_release": None,
                "loaded_release_owner": _RELEASE_IDENTITY_OWNER}
    hb_age = _age(state.get("heartbeat_at"), now)
    pg_age = _age(state.get("progress_at"), now)
    in_flight = bool(state.get("iteration_in_flight"))
    pid = (lock or {}).get("pid") or state.get("pid")
    alive = _pid_alive(pid)

    def _verdict(service_state: str, activity: str, reason: str,
                 activity_reason: str) -> dict:
        return {"service_state": service_state, "worker_activity": activity,
                "reason": reason, "activity_reason": activity_reason,
                "heartbeat_age_seconds": (None if hb_age is None
                                          else round(hb_age, 1)),
                "progress_age_seconds": (None if pg_age is None
                                         else round(pg_age, 1)),
                "progress_seq": state.get("progress_seq"),
                "progress_step": state.get("progress_step"),
                "iteration_in_flight": in_flight,
                "progress_stall_after_seconds": PROGRESS_STALL_SECONDS,
                "worker_pid": pid, "worker_alive": alive,
                # R55.2.1 — WHICH WORKER, AND WHICH RELEASE IT LOADED.
                #
                # These are properties of the worker PROCESS, exactly like
                # ``worker_pid``, and they are carried here VERBATIM out of the
                # state this owner was already handed. R55.2 published the
                # capture only on the full collection payload, so every consumer
                # that reads the canonical lifecycle view instead — the operator
                # presentation, the decision snapshot and through it the Active
                # Manager — saw no identity at all and reported a proven-aligned
                # worker as UNKNOWN. One lifecycle owner, one set of worker
                # facts. This owner still compares nothing: the alignment verdict
                # belongs to ``api.runtime_identity`` and to nothing else.
                "instance_id": state.get("instance_id"),
                "started_at": state.get("started_at"),
                "loaded_release": state.get("loaded_release"),
                "loaded_release_owner": _RELEASE_IDENTITY_OWNER}

    if state.get("stopped_at") and not lock:
        return _verdict(SVC_STOPPED, ACT_NOT_RUNNING,
                        ("Stopped %s. Collection is not running; execution "
                         "safety is unchanged."
                         % ("cleanly" if state.get("graceful_shutdown")
                            else "without a clean shutdown marker")),
                        "The worker is stopped.")
    if alive is False:
        return _verdict(SVC_DEGRADED, ACT_DEAD,
                        ("The worker process (pid %s) is gone. Collection has "
                         "stopped; execution safety is unchanged." % pid),
                        "The recorded worker process no longer exists.")
    if hb_age is None:
        return _verdict(SVC_DEGRADED, ACT_UNKNOWN,
                        "No heartbeat has ever been recorded by this worker.",
                        "Nothing this worker has done can be proven.")
    if in_flight:
        # An OPEN iteration is judged on PROGRESS, never on the wake stamp the
        # heartbeat records. This is the whole Release 29.2 repair: a 5.5-minute
        # collection pass that is still finishing bounded units of work is BUSY,
        # not degraded.
        if pg_age is None:
            return _verdict(SVC_DEGRADED, ACT_UNKNOWN,
                            ("A collection iteration is open but has emitted no "
                             "progress evidence. Health fails closed when work "
                             "cannot be proven."),
                            "An open iteration with no progress stamp.")
        if pg_age <= PROGRESS_STALL_SECONDS:
            return _verdict(SVC_RUNNING, ACT_BUSY,
                            ("Collecting: %s. Progress advanced %.0f seconds ago "
                             "(step %s of iteration %s)."
                             % (state.get("progress_step") or "work in flight",
                                pg_age, state.get("progress_seq"),
                                state.get("current_iteration_id") or "?")),
                            ("The worker is busy and still advancing through the "
                             "collection path."))
        return _verdict(SVC_DEGRADED, ACT_STALLED,
                        ("A collection iteration has been open with NO progress "
                         "for %.0f seconds (stalled beyond %.0f) at step %r."
                         % (pg_age, PROGRESS_STALL_SECONDS,
                            state.get("progress_step"))),
                        "The worker is alive but its work stopped advancing.")
    if hb_age > HEARTBEAT_STALE_SECONDS:
        return _verdict(SVC_DEGRADED, ACT_STALLED,
                        ("The worker heartbeat is %.0f seconds old (stale beyond "
                         "%.0f) and no iteration is open — the process is not "
                         "iterating." % (hb_age, HEARTBEAT_STALE_SECONDS)),
                        "The worker is between iterations and stopped waking up.")
    return _verdict(SVC_RUNNING, ACT_IDLE,
                    "Heartbeat is %.0f seconds old." % hb_age,
                    "The worker is idle between iterations.")


# --------------------------------------------------------------------------- #
# Windows launch lineage — what ONE LOGICAL WORKER actually is
# --------------------------------------------------------------------------- #
# A clean canonical Start reproducibly produced TWO physical python.exe
# processes with a BYTE-IDENTICAL command line, and the manager called that a
# singleton violation. It is not one. Measured on this machine:
#
#   pid   940  ExecutablePath C:\...\.venv-win\Scripts\python.exe  parent 9760
#   pid 29108  ExecutablePath C:\Python313\python.exe              parent  940
#   both CommandLine: "C:\...\.venv-win\Scripts\python.exe" <script>
#
# ``.venv-win\Scripts\python.exe`` is NOT CPython. Its version resource reads
# InternalName "Python Launcher", OriginalFilename "py.exe": it is the venv
# REDIRECTOR that CPython's ``venv`` copies into Scripts. It CreateProcess-es the
# base interpreter named by pyvenv.cfg (``executable = C:\Python313\python.exe``)
# passing its own command line verbatim, then waits on it. The duplicate command
# line is therefore structural and permanent, and has nothing to do with Task
# Scheduler - a bare Start-Process reproduces the same pair exactly. Inside
# Python, ``sys.executable`` reports the VENV path in BOTH processes, so no
# in-process check can tell them apart; only ExecutablePath and the parent edge
# can. The CHILD runs main(), so the CHILD is what calls os.getpid() and owns the
# singleton lock.
#
# ONE LOGICAL WORKER is therefore ONE LAUNCH LINEAGE: the matched processes
# joined by parent->child edges *within the matched set*. A lineage LEAF - a
# matched process with no matched child - is the process that actually executes
# the application. Counting LEAVES keeps the guarantee intact in both
# directions: redirector+worker is ONE, while two independent lineages (or one
# launcher that somehow spawned two workers) is a violation. Nothing here is
# allowed to soften that: an unreadable pid, a cycle, or a lineage that does not
# own the lock fails CLOSED.

#: The command-line token that makes a process a candidate collection worker.
CANONICAL_WORKER_SCRIPT = "run_information_collection_service.py"

WORKER_TOPOLOGY_NONE = "NO_LOGICAL_WORKER"
WORKER_TOPOLOGY_SINGLE = "SINGLE_LOGICAL_WORKER"
WORKER_TOPOLOGY_VIOLATED = "SINGLETON_VIOLATED"
WORKER_TOPOLOGY_AMBIGUOUS = "AMBIGUOUS_WORKER_TOPOLOGY"
WORKER_TOPOLOGY_VERDICTS = (WORKER_TOPOLOGY_NONE, WORKER_TOPOLOGY_SINGLE,
                            WORKER_TOPOLOGY_VIOLATED, WORKER_TOPOLOGY_AMBIGUOUS)

# --------------------------------------------------------------------------- #
# RELEASE 55.2.1 — "I LOOKED AND SAW NOTHING" IS NOT "I COULD NOT LOOK"
# --------------------------------------------------------------------------- #
# Measured on this machine on 2026-09-03, in an unelevated operator shell:
#
#   Get-CimInstance Win32_Process -Filter "Name='python.exe'"  ->  6 rows
#     pid  1888 / 2264   CommandLine READABLE     (this shell's own lineage)
#     pid  3208 / 58768  CommandLine NULL         (backend, Task Scheduler)
#     pid 61108 /  1976  CommandLine NULL         (COLLECTION WORKER, alive)
#
# The collector selects candidates by ``CommandLine -like "*<worker script>*"``,
# so a row whose command line the shell may not read is not merely unmatched —
# it never reaches this function at all. The snapshot arrived EMPTY while pid
# 1976 was alive, holding the singleton lock and heartbeating, and the verdict
# was NO_LOGICAL_WORKER: "No process on this machine is running the collection
# worker." Restart then declared COLLECTION_SERVICE_BLOCKED on a healthy service.
#
# That is not a cosmetic warning. ``resolve_abandoned_lock`` treats
# NO_LOGICAL_WORKER as PROOF that the machine is empty and returns
# ``may_clear: True`` — so the same blindness would have authorised clearing the
# singleton lock of a RUNNING worker and permitted a second one. An unreadable
# snapshot must therefore never reach that verdict; it fails closed to
# AMBIGUOUS_WORKER_TOPOLOGY, which every safety consumer already refuses.
#
# The introspection envelope is what lets this function tell the two cases
# apart. It is OPTIONAL: a caller that supplies none is taken at its word, which
# keeps every existing snapshot contract working unchanged.
#: Counters a snapshot collector may report about its OWN ability to see.
WORKER_SNAPSHOT_INTROSPECTION_KEYS = ("scanned_count", "matched_count",
                                      "unreadable_command_line_count",
                                      "query_failed")

# --------------------------------------------------------------------------- #
# WORKER PRESENCE — the evidence hierarchy, ordered strongest first
# --------------------------------------------------------------------------- #
#: One live worker, corroborated by a readable process snapshot.
WORKER_PRESENCE_CONFIRMED = "CONFIRMED_SINGLETON"
#: One live worker proven by AUTHORITATIVE runtime state, while the OPTIONAL OS
#: process-metadata correlation was unavailable. Strong evidence, one advisory.
WORKER_PRESENCE_CONFIRMED_NO_OS = "CONFIRMED_SINGLETON_OS_METADATA_UNAVAILABLE"
#: No authoritative evidence of any worker.
WORKER_PRESENCE_NONE = "NO_WORKER"
#: More than one worker is PROVEN. Never softened by anything below it.
WORKER_PRESENCE_MULTIPLE = "MULTIPLE_WORKERS"
#: The evidence conflicts (heartbeat pid vs lock owner vs snapshot). Fails closed.
WORKER_PRESENCE_INCONSISTENT = "INCONSISTENT_TOPOLOGY"
WORKER_PRESENCE_VERDICTS = (WORKER_PRESENCE_CONFIRMED,
                            WORKER_PRESENCE_CONFIRMED_NO_OS,
                            WORKER_PRESENCE_NONE, WORKER_PRESENCE_MULTIPLE,
                            WORKER_PRESENCE_INCONSISTENT)
#: The verdicts under which an operator lifecycle action may report success.
WORKER_PRESENCE_SINGLETON_PROVEN = (WORKER_PRESENCE_CONFIRMED,
                                    WORKER_PRESENCE_CONFIRMED_NO_OS)

#: The evidence ladder, named once so no surface invents its own ordering. Read
#: top to bottom: the first rung that decides, decides.
WORKER_PRESENCE_EVIDENCE_ORDER = (
    "PROVEN_MULTIPLE_LINEAGES",       # the snapshot proves two workers
    "CONFLICTING_RUNTIME_EVIDENCE",   # heartbeat pid vs lock owner vs instance
    "AUTHORITATIVE_RUNTIME_STATE",    # live pid + lock owner + instance + beat
    "OS_PROCESS_CORRELATION",         # optional; corroborates, never overrides
)
#: The ONE owner of the presence verdict. No script re-derives it.
PRESENCE_OWNER = "api.information_collection.resolve_worker_presence"

#: Bound on the ancestor walk. A lineage is a redirector plus a worker; anything
#: deeper than this is a malformed snapshot, not a launch chain.
_MAX_LINEAGE_DEPTH = 16


def _as_pid(value: Any) -> Optional[int]:
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _process_row(raw: Any) -> dict:
    """Normalise ONE process snapshot row. Win32 or snake_case keys both work."""
    raw = raw if isinstance(raw, dict) else {}

    def pick(*names):
        for name in names:
            if name in raw and raw[name] not in (None, ""):
                return raw[name]
        return None

    return {
        "pid": _as_pid(pick("pid", "ProcessId")),
        "parent_pid": _as_pid(pick("parent_pid", "ParentProcessId")),
        "command_line": str(pick("command_line", "CommandLine") or ""),
        "executable_path": (str(pick("executable_path", "ExecutablePath"))
                            if pick("executable_path", "ExecutablePath") else None),
        "created_at": pick("created_at", "CreationDate"),
    }


def _created_before(parent: dict, child: dict) -> bool:
    """Is ``parent`` old enough to really be ``child``'s parent?

    Windows recycles process ids. A recorded parent id that names a process
    created AFTER its supposed child is a RECYCLED id, not an ancestor. Refusing
    that edge fails safe: it produces MORE lineage roots, never fewer.
    """
    p_at, c_at = _parse(parent.get("created_at")), _parse(child.get("created_at"))
    if p_at is None or c_at is None:
        return True  # unknown creation times cannot disprove the edge
    try:
        return p_at <= c_at
    except TypeError:  # naive vs aware — undecidable, so do not disprove it
        return True


def _split_snapshot(processes: Any, introspection: Optional[dict]) -> tuple:
    """``(rows, introspection)`` from either a bare list or an envelope.

    A collector may hand over ``[row, ...]`` (every pre-R55.2.1 caller) or
    ``{"rows": [...], "introspection": {...}}``. Both are accepted so the
    envelope can be adopted one collector at a time.
    """
    if isinstance(processes, dict):
        intro = introspection or processes.get("introspection") or None
        return list(processes.get("rows") or []), intro
    return list(processes or []), introspection


def _snapshot_authority(introspection: Optional[dict]) -> dict:
    """Could this snapshot SEE everything it needed to see?

    No envelope means the collector made no claim, and a caller is taken at its
    word — that is what every existing snapshot contract already assumed.
    """
    if not isinstance(introspection, dict) or not introspection:
        return {"os_metadata_available": True, "snapshot_authoritative": True,
                "introspection": None,
                "authority_detail": ("No introspection envelope was supplied; the "
                                     "snapshot is taken as complete.")}
    unreadable = introspection.get("unreadable_command_line_count")
    try:
        unreadable = int(unreadable) if unreadable is not None else 0
    except (TypeError, ValueError):
        unreadable = 0
    failed = bool(introspection.get("query_failed"))
    available = not failed and unreadable == 0
    if failed:
        detail = ("The process query itself failed, so this machine's process "
                  "table was never read.")
    elif unreadable:
        detail = ("%d process(es) were visible but their command lines could not "
                  "be read by this shell, so a worker among them could not be "
                  "recognised." % unreadable)
    else:
        detail = ("Every visible process exposed its command line; the snapshot "
                  "is complete.")
    return {"os_metadata_available": available,
            "snapshot_authoritative": available,
            "introspection": {k: introspection.get(k)
                              for k in WORKER_SNAPSHOT_INTROSPECTION_KEYS},
            "authority_detail": detail}


def resolve_worker_topology(processes: Any, *, lock: Optional[dict] = None,
                            worker_script: str = CANONICAL_WORKER_SCRIPT,
                            introspection: Optional[dict] = None) -> dict:
    """ONE authoritative logical-worker verdict over a process snapshot.

    ``processes`` is a snapshot of candidate processes (Win32_Process rows or the
    snake_case equivalent), optionally wrapped in an envelope carrying what the
    collector could SEE. Membership is the command line naming the canonical
    worker script — the same test the manager has always used — but the COUNT is
    of launch lineages, not of physical processes.

    R55.2.1: an EMPTY snapshot now means "no worker" only when the collector
    could actually read the process table. When command lines were unreadable the
    verdict fails closed to AMBIGUOUS_WORKER_TOPOLOGY instead, because
    :func:`resolve_abandoned_lock` treats NO_LOGICAL_WORKER as proof that the
    machine is empty and would otherwise authorise clearing a live worker's lock.
    """
    processes, introspection = _split_snapshot(processes, introspection)
    authority = _snapshot_authority(introspection)
    rows = [_process_row(r) for r in (processes or [])]
    token = str(worker_script or "").lower()
    candidates = [r for r in rows if token and token in r["command_line"].lower()]
    unreadable = [r for r in candidates if r["pid"] is None]

    by_pid: dict = {}
    for row in candidates:
        if row["pid"] is not None:
            by_pid.setdefault(row["pid"], row)

    # Effective parent = the recorded parent, but only when it is itself a
    # matched process and old enough to be an ancestor.
    parent_of: dict = {}
    for pid, row in by_pid.items():
        ppid = row["parent_pid"]
        parent = by_pid.get(ppid) if ppid is not None and ppid != pid else None
        parent_of[pid] = ppid if (parent is not None
                                  and _created_before(parent, row)) else None

    children_of: dict = {pid: [] for pid in by_pid}
    for pid, ppid in parent_of.items():
        if ppid is not None:
            children_of[ppid].append(pid)

    # A parent chain that revisits a pid is a malformed snapshot, not a lineage.
    cyclic: set = set()
    for pid in by_pid:
        seen, cursor, steps = {pid}, parent_of[pid], 0
        while cursor is not None and steps < _MAX_LINEAGE_DEPTH:
            if cursor in seen:
                cyclic.add(pid)
                break
            seen.add(cursor)
            cursor, steps = parent_of[cursor], steps + 1
        else:
            if cursor is not None:
                cyclic.add(pid)

    roots = sorted(pid for pid, ppid in parent_of.items()
                   if ppid is None and pid not in cyclic)
    lock_pid = _as_pid((lock or {}).get("pid"))

    lineages: list = []
    reached: set = set()
    for root in roots:
        members, queue, guard = [], [root], 0
        while queue and guard <= len(by_pid):
            pid = queue.pop(0)
            if pid in reached:
                continue
            reached.add(pid)
            members.append(pid)
            queue.extend(sorted(children_of.get(pid, ())))
            guard += 1
        leaves = sorted(p for p in members if not children_of.get(p))
        executing = leaves[0] if len(leaves) == 1 else None
        # ``members`` is in BFS order from the root, so the list reads as the
        # launch chain itself. Sorting it here would print "8660 -> 18204" for a
        # lineage whose parent is 18204 - an operator sentence that states the
        # topology backwards is worse than no sentence.
        lineages.append({
            "root_pid": root,
            "pids": list(members),
            "leaf_pids": leaves,
            "executing_pid": executing,
            "owns_lock": bool(lock_pid is not None and lock_pid in members),
            "executable_paths": [by_pid[p]["executable_path"] for p in members],
        })

    orphaned = sorted(set(by_pid) - reached)
    leaf_total = sum(len(l["leaf_pids"]) for l in lineages)

    if not candidates and not authority["snapshot_authoritative"]:
        # THE R55.2.1 REPAIR. Absence of evidence is not evidence of absence when
        # the shell was not permitted to look. Failing closed here is what keeps
        # ``resolve_abandoned_lock`` from clearing a live worker's singleton lock.
        verdict = WORKER_TOPOLOGY_AMBIGUOUS
        reason = ("The process snapshot is not authoritative, so it cannot show "
                  "whether a worker is running. %s Independent runtime evidence "
                  "(service state, heartbeat, singleton lock, pid liveness) "
                  "decides worker presence instead."
                  % authority["authority_detail"])
    elif not candidates:
        verdict = WORKER_TOPOLOGY_NONE
        reason = "No process on this machine is running the collection worker."
    elif unreadable or cyclic or orphaned:
        verdict = WORKER_TOPOLOGY_AMBIGUOUS
        reason = ("The process snapshot cannot be resolved into launch lineages "
                  "(unreadable pids: %d; cyclic: %d; unreachable: %d). Failing "
                  "closed rather than guessing how many workers are running."
                  % (len(unreadable), len(cyclic), len(orphaned)))
    elif len(lineages) == 1 and leaf_total == 1:
        verdict = WORKER_TOPOLOGY_SINGLE
        reason = ("One launch lineage: %s. The venv redirector and the base "
                  "interpreter it launched are ONE logical worker."
                  % " -> ".join(str(p) for p in lineages[0]["pids"]))
    elif len(lineages) > 1 or leaf_total > 1:
        verdict = WORKER_TOPOLOGY_VIOLATED
        reason = ("%d independent launch lineage(s) and %d executing process(es) "
                  "are running the collection worker. Overlapping provider "
                  "iterations are refused by design."
                  % (len(lineages), leaf_total))
    else:
        verdict = WORKER_TOPOLOGY_AMBIGUOUS
        reason = ("A worker process exists but no launch lineage could be "
                  "resolved from it.")

    single = lineages[0] if verdict == WORKER_TOPOLOGY_SINGLE else None
    executing_pid = single["executing_pid"] if single else None
    if verdict != WORKER_TOPOLOGY_SINGLE:
        lock_correlated, lock_reason = False, "No single logical worker to correlate."
    elif lock_pid is None:
        lock_correlated, lock_reason = False, (
            "One logical worker is running but no singleton lock names its pid.")
    elif lock_pid == executing_pid:
        lock_correlated, lock_reason = True, (
            "The singleton lock is held by pid %s, the executing process of the "
            "one launch lineage." % lock_pid)
    elif lock_pid in single["pids"]:
        lock_correlated, lock_reason = False, (
            "The singleton lock names pid %s, which belongs to the lineage but is "
            "not its executing process (%s)." % (lock_pid, executing_pid))
    else:
        lock_correlated, lock_reason = False, (
            "The singleton lock names pid %s, which is not part of the running "
            "launch lineage." % lock_pid)

    return {
        "verdict": verdict,
        "reason": reason,
        "logical_worker_count": len(lineages),
        "physical_process_count": len(candidates),
        "executing_process_count": leaf_total,
        "executing_pid": executing_pid,
        "lineages": lineages,
        "matched_pids": sorted(by_pid),
        "unreadable_rows": len(unreadable),
        "cyclic_pids": sorted(cyclic),
        "unreachable_pids": orphaned,
        "lock_pid": lock_pid,
        "lock_instance_id": (lock or {}).get("instance_id"),
        "lock_correlated": bool(lock_correlated),
        "lock_correlation_reason": lock_reason,
        "singleton_ok": verdict == WORKER_TOPOLOGY_SINGLE,
        "healthy": bool(verdict == WORKER_TOPOLOGY_SINGLE and lock_correlated),
        "worker_script": worker_script,
        # R55.2.1 — what the COLLECTOR could see, carried beside what it found.
        # A consumer must be able to tell a complete snapshot that found nothing
        # from a snapshot that could not be taken.
        "os_metadata_available": authority["os_metadata_available"],
        "snapshot_authoritative": authority["snapshot_authoritative"],
        "snapshot_authority_detail": authority["authority_detail"],
        "introspection": authority["introspection"],
    }


def resolve_worker_presence(*, topology: Optional[dict],
                            state: Optional[dict] = None,
                            lock: Optional[dict] = None,
                            now: Optional[datetime] = None,
                            pid_alive: Optional[Callable] = None) -> dict:
    """R55.2.1 — IS EXACTLY ONE COLLECTION WORKER RUNNING, and how do we know?

    ONE verdict over TWO independent bodies of evidence, ranked by strength and
    resolved in the fixed order of :data:`WORKER_PRESENCE_EVIDENCE_ORDER`:

      1. PROVEN_MULTIPLE_LINEAGES — the process snapshot resolved two or more
         workers. This outranks everything below it and is never softened,
         because the singleton guarantee is the one thing that must fail closed
         in both directions: unreadable OS metadata may not manufacture a worker
         out of nothing, and it may not hide one either.
      2. CONFLICTING_RUNTIME_EVIDENCE — the heartbeat's pid, the lock owner and
         the instance id disagree. Undecidable is not healthy.
      3. AUTHORITATIVE_RUNTIME_STATE — the worker's own durable evidence: a live
         pid, a singleton lock naming that pid, the same instance id in both, and
         a heartbeat (or an advancing iteration) inside the stall budget. This is
         STRONGER than a process-table correlation, because the worker wrote it.
      4. OS_PROCESS_CORRELATION — the optional command-line snapshot. It can
         CORROBORATE rung 3 and it can prove rung 1. It can never, on its own,
         refute rung 3: a shell that may not read a Task-Scheduler process's
         command line has learned nothing about whether that process exists.

    Rung 4 does retain one refutation: when the snapshot WAS authoritative and
    found no worker while rung 3 says one is live, the two disagree about a fact
    both could see, and that is INCONSISTENT_TOPOLOGY — reported, never resolved
    by preferring the more comfortable answer.

    Read-only. Starts, stops, restarts and signals nothing.
    """
    now = now or _now()
    alive_fn = pid_alive or _pid_alive
    topo = topology or {}
    st = state or {}
    verdict_topo = topo.get("verdict")
    os_available = topo.get("os_metadata_available")
    if os_available is None:
        os_available = True

    lock_pid = _as_pid((lock or {}).get("pid"))
    state_pid = _as_pid(st.get("pid"))
    lock_instance = (lock or {}).get("instance_id")
    state_instance = st.get("instance_id")
    hb_age = _age(st.get("heartbeat_at"), now)
    pg_age = _age(st.get("progress_at"), now)
    in_flight = bool(st.get("iteration_in_flight"))
    # A worker proves it is awake either by heartbeating between iterations or by
    # advancing progress inside an open one — the same two proofs the lifecycle
    # owner already uses. Nothing here invents a third.
    beating = bool(hb_age is not None and hb_age <= HEARTBEAT_STALE_SECONDS)
    advancing = bool(in_flight and pg_age is not None
                     and pg_age <= PROGRESS_STALL_SECONDS)
    pid_for_liveness = state_pid or lock_pid
    live = alive_fn(pid_for_liveness) if pid_for_liveness else None

    evidence = {
        "worker_pid": state_pid, "lock_pid": lock_pid,
        "pid_alive": live,
        "state_instance_id": state_instance, "lock_instance_id": lock_instance,
        "heartbeat_age_seconds": (None if hb_age is None else round(hb_age, 1)),
        "heartbeat_fresh": beating,
        "progress_age_seconds": (None if pg_age is None else round(pg_age, 1)),
        "iteration_in_flight": in_flight, "iteration_advancing": advancing,
        "heartbeat_stale_after_seconds": HEARTBEAT_STALE_SECONDS,
        "progress_stall_after_seconds": PROGRESS_STALL_SECONDS,
        "topology_verdict": verdict_topo,
        "topology_reason": topo.get("reason"),
        "topology_executing_pid": topo.get("executing_pid"),
        "os_metadata_available": bool(os_available),
        "snapshot_authoritative": bool(topo.get("snapshot_authoritative", True)),
        "snapshot_authority_detail": topo.get("snapshot_authority_detail"),
        "evidence_order": list(WORKER_PRESENCE_EVIDENCE_ORDER),
    }

    def _out(verdict: str, rung: str, reason: str, advisory=None) -> dict:
        return dict(evidence, verdict=verdict, verdict_vocabulary=list(
            WORKER_PRESENCE_VERDICTS), decided_on=rung, reason=reason,
            advisory=advisory,
            singleton_proven=verdict in WORKER_PRESENCE_SINGLETON_PROVEN,
            read_only=True, writes_nothing=True, restarts_nothing=True,
            owner=PRESENCE_OWNER)

    # ---- 1. A proven violation outranks every other consideration. --------- #
    if verdict_topo == WORKER_TOPOLOGY_VIOLATED:
        return _out(WORKER_PRESENCE_MULTIPLE, "PROVEN_MULTIPLE_LINEAGES",
                    topo.get("reason") or "More than one collection worker is "
                                          "running.")

    # ---- 2. Runtime evidence that contradicts itself is never healthy. ----- #
    if (lock_pid is not None and state_pid is not None and lock_pid != state_pid
            and live is not False):
        return _out(WORKER_PRESENCE_INCONSISTENT, "CONFLICTING_RUNTIME_EVIDENCE",
                    ("The service state records worker pid %s while the singleton "
                     "lock is held by pid %s. Two different processes cannot both "
                     "be the one worker." % (state_pid, lock_pid)))
    if (lock_instance and state_instance and lock_instance != state_instance):
        return _out(WORKER_PRESENCE_INCONSISTENT, "CONFLICTING_RUNTIME_EVIDENCE",
                    ("The singleton lock names instance %s while the service state "
                     "records instance %s." % (lock_instance, state_instance)))

    # ---- 3. Does the worker's OWN durable evidence prove one live worker? --- #
    proven = bool(lock_pid is not None and live is True and (beating or advancing)
                  and (state_pid is None or state_pid == lock_pid))
    if proven:
        detail = ("pid %s is alive, holds the singleton lock, and %s"
                  % (lock_pid,
                     ("last heartbeat %.0fs ago" % hb_age) if beating
                     else ("its open iteration advanced %.0fs ago" % pg_age)))
        # ---- 4. The optional OS correlation: corroborate, or disagree. ------ #
        if verdict_topo == WORKER_TOPOLOGY_SINGLE:
            return _out(WORKER_PRESENCE_CONFIRMED, "AUTHORITATIVE_RUNTIME_STATE",
                        ("Exactly one collection worker is running: %s. The "
                         "process snapshot resolves the same single launch "
                         "lineage." % detail))
        if not os_available:
            return _out(
                WORKER_PRESENCE_CONFIRMED_NO_OS, "AUTHORITATIVE_RUNTIME_STATE",
                ("Exactly one collection worker is running: %s." % detail),
                advisory=("OS process-metadata correlation is unavailable — %s "
                          "Worker presence is proven by the worker's own durable "
                          "runtime state instead, which is stronger evidence than "
                          "a command-line match. Nothing is wrong with the service."
                          % (topo.get("snapshot_authority_detail") or
                             "this shell could not read the process command "
                             "lines.")))
        if verdict_topo == WORKER_TOPOLOGY_NONE:
            return _out(WORKER_PRESENCE_INCONSISTENT, "OS_PROCESS_CORRELATION",
                        ("The runtime state proves a live worker (%s) but a "
                         "COMPLETE process snapshot found no process running the "
                         "collection worker. The two disagree about a fact both "
                         "could observe." % detail))
        return _out(
            WORKER_PRESENCE_CONFIRMED_NO_OS, "AUTHORITATIVE_RUNTIME_STATE",
            ("Exactly one collection worker is running: %s." % detail),
            advisory=("The process snapshot could not be resolved into launch "
                      "lineages (%s), so it neither corroborates nor refutes the "
                      "runtime evidence." % (topo.get("reason") or "unresolved")))

    # ---- 5. Nothing proves a worker. --------------------------------------- #
    if verdict_topo == WORKER_TOPOLOGY_SINGLE:
        return _out(WORKER_PRESENCE_INCONSISTENT, "OS_PROCESS_CORRELATION",
                    ("A single worker lineage is running (executing pid %s) but "
                     "its durable runtime evidence does not confirm it: %s."
                     % (topo.get("executing_pid"),
                        topo.get("lock_correlation_reason") or
                        "the singleton lock does not name a live heartbeating "
                        "process")))
    why = []
    if lock_pid is None:
        why.append("no singleton lock is held")
    if live is False:
        why.append("the recorded worker pid %s is gone" % pid_for_liveness)
    elif live is None:
        why.append("no worker pid has been recorded")
    if not (beating or advancing):
        why.append("no fresh heartbeat or advancing iteration")
    return _out(WORKER_PRESENCE_NONE, "AUTHORITATIVE_RUNTIME_STATE",
                "No collection worker can be proven: %s."
                % ("; ".join(why) or "no runtime evidence of a worker"))


# --------------------------------------------------------------------------- #
# Credentials — PRESENCE only, never a value
# --------------------------------------------------------------------------- #
def credential_availability(*, env: Optional[dict] = None) -> dict:
    """Which configured credential NAMES are visible to THIS process.

    A worker launched by Task Scheduler does not necessarily inherit an
    interactive shell's environment. Reporting presence honestly is the whole
    point: a source whose key is invisible becomes DEGRADED_CREDENTIAL and the
    other sources continue, instead of silently pretending to be active.
    """
    env_map = dict(os.environ if env is None else env)
    rows = {}
    for policy in cad.CADENCE_POLICY_TABLE:
        names = list(policy["credential_env"])
        if not names:
            rows[policy["source_id"]] = {"required": False, "available": None,
                                         "names": [], "visible_names": []}
            continue
        visible = [n for n in names if str(env_map.get(n) or "").strip()]
        rows[policy["source_id"]] = {
            "required": True, "available": bool(visible), "names": names,
            "visible_names": visible,
            "detail": ("present in the worker environment" if visible else
                       "NOT visible to the collection worker"),
        }
    return {"contract_id": "paper_trader.collection_credential_presence/1",
            "checked_names_only": True, "values_never_read": True,
            "sources": rows}


# --------------------------------------------------------------------------- #
# The live attention universe
# --------------------------------------------------------------------------- #
def _default_portfolio_state_loader():
    from . import portfolio_state as ps
    return ps.load_portfolio_state()


def _default_scoring_loader():
    from . import universe_scoring as us
    return us.build_universe_scoring()


def build_attention_universe(*, portfolio_state: Optional[dict] = None,
                             scoring: Optional[dict] = None,
                             portfolio_state_loader: Optional[Callable] = None,
                             scoring_loader: Optional[Callable] = None,
                             candidate_depth: int = 60,
                             universe_depth: int = 250) -> dict:
    """THE live attention universe, read from the AUTHORITATIVE owners.

    Tier 0 is every current holding. Tier 1 is the strongest eligible
    replacement candidates from the canonical ranking. Tier 2 is the broader
    eligible universe for lanes where per-symbol querying is cheap enough. No
    ticker is hardcoded, the UI holds no universe of its own, and this module
    performs NO ranking — it reads the one the scoring owner already produced.
    """
    warnings: list[str] = []
    ps = portfolio_state
    if ps is None:
        try:
            ps = (portfolio_state_loader or _default_portfolio_state_loader)()
        except Exception as exc:  # noqa: BLE001 - a missing read is not a crash
            ps = {}
            warnings.append("portfolio state unavailable: %s" % str(exc)[:160])
    sc = scoring
    if sc is None:
        try:
            sc = (scoring_loader or _default_scoring_loader)()
        except Exception as exc:  # noqa: BLE001
            sc = {}
            warnings.append("universe scoring unavailable: %s" % str(exc)[:160])

    held = sorted({str(p.get("ticker")).upper()
                   for p in ((ps or {}).get("positions") or []) if p.get("ticker")})
    ranked = [str(r.get("ticker")).upper()
              for r in ((sc or {}).get("rankings") or []) if r.get("ticker")]
    held_set = set(held)
    candidates = [t for t in ranked if t not in held_set][:int(candidate_depth)]
    universe = ranked[:int(universe_depth)]
    return {
        "contract_id": "paper_trader.live_attention_universe/1",
        "composition_owner": COMPOSITION_OWNER,
        "generated_at": _iso(_now()),
        "authoritative_sources": ["api.portfolio_state", "api.universe_scoring"],
        "active_book_id": ((ps or {}).get("active_book") or {}).get("book_id"),
        "eligible_market_date": ((ps or {}).get("dates") or {}).get(
            "eligible_market_date"),
        "tiers": {
            cad.TIER_HOLDINGS: {"tickers": held, "count": len(held),
                                "why": "Every current operational holding. "
                                       "Highest collection priority."},
            cad.TIER_CANDIDATES: {"tickers": candidates, "count": len(candidates),
                                  "why": "The strongest eligible alternatives from "
                                         "the canonical ranking — enough names to "
                                         "support opportunity-cost discovery."},
            cad.TIER_UNIVERSE: {"tickers": universe, "count": len(universe),
                                "why": "The broader eligible universe, collected at "
                                       "a lower frequency where per-symbol querying "
                                       "is expensive."},
            cad.TIER_GLOBAL: {"tickers": [], "count": 0,
                              "why": "SEC index, halt feeds and public broad feeds "
                                     "cover the universe without a per-ticker "
                                     "query."},
        },
        "holdings": held, "candidates": candidates,
        "warnings": warnings,
    }


def tickers_for_tier(universe: dict, tier: str, *, limit: int = 0) -> list[str]:
    row = ((universe or {}).get("tiers") or {}).get(tier) or {}
    out = list(row.get("tickers") or [])
    if tier == cad.TIER_CANDIDATES:
        out = list((universe or {}).get("holdings") or []) + out
    if tier == cad.TIER_UNIVERSE:
        out = list((universe or {}).get("holdings") or []) + out
    seen, uniq = set(), []
    for t in out:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return uniq[:limit] if limit else uniq


# --------------------------------------------------------------------------- #
# Provider budget
# --------------------------------------------------------------------------- #
def _reset_daily_counters(row: dict, now: datetime) -> dict:
    """Roll per-day counters on an ET date change (the market's day, not UTC's)."""
    et_date = mh.session_state(now)["et_date"]
    if row.get("counter_et_date") != et_date:
        row = dict(row)
        row["counter_et_date"] = et_date
        row["request_count_today"] = 0
        row["new_event_count_today"] = 0
        row["duplicate_count_today"] = 0
    return row


def budget_verdict(*, policy: dict, row: dict, now: datetime) -> dict:
    """May this source be called right now under its OWN request budget?"""
    budget = policy["request_budget"]
    today = int(row.get("request_count_today") or 0)
    per_day = budget.get("max_calls_per_day")
    if per_day is not None and today >= per_day:
        return {"allowed": False, "reason":
                "Daily request budget exhausted (%d/%d calls)." % (today, per_day)}
    per_hour = budget.get("max_calls_per_hour")
    if per_hour is not None:
        window = [t for t in (row.get("recent_call_times") or [])
                  if (_age(t, now) or 1e9) < 3600.0]
        if len(window) >= per_hour:
            return {"allowed": False, "reason":
                    "Hourly request budget exhausted (%d/%d calls in the last hour)."
                    % (len(window), per_hour)}
    return {"allowed": True, "reason": "Within this source's request budget."}


def _record_call(row: dict, now: datetime) -> dict:
    row = dict(row)
    # The ATTEMPT stamp is what the cadence gate reads. Without it every source
    # would look "never attempted" and be re-collected on the very next wake.
    row["last_attempt_at"] = _iso(now)
    row["request_count_today"] = int(row.get("request_count_today") or 0) + 1
    recent = [t for t in (row.get("recent_call_times") or [])
              if (_age(t, now) or 1e9) < 3600.0]
    recent.append(_iso(now))
    row["recent_call_times"] = recent[-64:]
    return row


def _record_success(row: dict, now: datetime, *, new_records: int,
                    watermark: Any = None) -> dict:
    row = dict(row)
    row["last_success_at"] = _iso(now)
    row["consecutive_failures"] = 0
    row["backoff_until"] = None
    row["last_error"] = None
    row["last_error_category"] = None
    row["last_http_status"] = None
    if watermark:
        row["watermark"] = watermark
    if int(new_records or 0) > 0:
        row["last_new_information_at"] = _iso(now)
        row["new_event_count_today"] = (int(row.get("new_event_count_today") or 0)
                                        + int(new_records))
    return row


def _record_failure(row: dict, now: datetime, *, status: Any, detail: Any) -> dict:
    row = dict(row)
    category = cad.classify_http_error(status=status, detail=detail)
    failures = int(row.get("consecutive_failures") or 0) + 1
    wait = cad.backoff_seconds(category=category, consecutive_failures=failures)
    row["consecutive_failures"] = failures
    row["backoff_until"] = _iso(now + timedelta(seconds=wait))
    row["last_error"] = str(detail or "")[:300]
    row["last_error_category"] = category
    row["last_http_status"] = status
    row["backoff_seconds"] = round(wait, 1)
    if category == cad.ERR_RATE_LIMIT:
        row["rate_limit_count"] = int(row.get("rate_limit_count") or 0) + 1
    return row


# --------------------------------------------------------------------------- #
# Collector dispatch — every lane delegates to its EXISTING canonical owner
# --------------------------------------------------------------------------- #
def _git_contact_email() -> Optional[str]:
    """The SEC fair-access contact, from the existing safe configuration."""
    import subprocess
    try:
        out = subprocess.run(["git", "-C", str(_REPO_ROOT), "config", "user.email"],
                             capture_output=True, text=True, timeout=10)
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except Exception:  # noqa: BLE001
        pass
    return None


def _stage2_config_for(source_ids: Iterable[str]) -> Optional[dict]:
    """The canonical Stage-2 config with ONLY the due sources enabled.

    This is how the service runs a subset of sources without forking the
    collection owner: ``run_ingestion`` still reads its own configuration
    contract; we only narrow which sources it visits this pass.
    """
    try:
        cfg = json.loads(_STAGE2_CONFIG.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return None
    wanted = {str(s) for s in source_ids}
    for sid, scfg in (cfg.get("sources") or {}).items():
        if sid not in wanted:
            scfg["enabled"] = False
    return cfg


def collect_stage2(*, source_ids: list[str], ingestion_root: Any,
                   run_fn: Optional[Callable] = None,
                   progress_fn: Optional[Callable] = None) -> dict:
    """Run the canonical Stage-2 collectors for the due sources only."""
    if not source_ids:
        return {"ran": False, "reason": "no Stage-2 source due"}
    cfg = _stage2_config_for(source_ids)
    if cfg is None:
        return {"ran": False, "ok": False,
                "reason": "Stage-2 configuration could not be read"}
    fn = run_fn
    if fn is None:
        from ..alpha_agent import ingestion as ing
        fn = ing.run_ingestion
    progress = _safe_progress(progress_fn)
    extra: dict = {}
    if progress_fn is not None and run_fn is None:
        # Only the REAL collector owner is instrumented — its signature is the
        # one this module knows. An injected run_fn (replay, tests) keeps its own
        # hermetic wiring and reports progress at the lane boundary instead. The
        # transport seam is what matters here: ONE HTTP attempt is the finest
        # blocking unit in this lane, so wrapping it proves a multi-minute lane
        # is advancing without either collector learning what a heartbeat is.
        from ..alpha_agent.collectors import default_transport
        extra["transport"] = _progress_transport(default_transport, progress,
                                                 lane="COLLECT_STAGE2")
        extra["progress_fn"] = lambda source_id: progress(
            "COLLECT_STAGE2", detail="source %s" % source_id)
    try:
        result = fn(config=cfg, output_root=str(ingestion_root), mode="incremental",
                    as_of="latest", contact_email=_git_contact_email(),
                    config_path=str(_STAGE2_CONFIG), **extra)
    except Exception as exc:  # noqa: BLE001 - one lane must never kill the iteration
        return {"ran": True, "ok": False, "reason": "%s: %s"
                % (type(exc).__name__, str(exc)[:200]), "per_source": {}}
    counts = result.get("counts") or {}
    per_source: dict[str, dict] = {}
    for sid, health in (result.get("source_states") or {}).items():
        if sid not in set(source_ids):
            continue
        per_source[sid] = health if isinstance(health, dict) else {"state": health}
    return {"ran": True, "ok": result.get("status") not in ("ALPHA_AGENT_STAGE2_BLOCKED",),
            "status": result.get("status"), "run_id": result.get("run_id"),
            "records_new": int(counts.get("records_new") or 0),
            "raw_new": int(counts.get("raw_new") or 0),
            "blocked_sources": result.get("blocked_sources") or [],
            "per_source": per_source, "reason": result.get("reason")}


def collect_news_rss(*, news_root: Any, run_fn: Optional[Callable] = None,
                     progress_fn: Optional[Callable] = None) -> dict:
    """Run the canonical Stage-3.5 official RSS/Atom collector."""
    fn = run_fn
    if fn is None:
        from ..alpha_agent import feed_registry as fr
        fn = fr.run_news_rss
    try:
        cfg = json.loads(_STAGE35_CONFIG.read_text(encoding="utf-8-sig"))
        feeds = json.loads(_NEWS_FEEDS_CONFIG.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        return {"ran": False, "ok": False,
                "reason": "news configuration unreadable: %s" % str(exc)[:160]}
    extra: dict = {}
    if progress_fn is not None and run_fn is None:
        from ..alpha_agent.collectors import default_transport
        extra["transport"] = _progress_transport(
            default_transport, _safe_progress(progress_fn), lane="COLLECT_NEWS_RSS")
    try:
        result = fn(config=cfg, feeds_config=feeds, output_root=str(news_root),
                    mode="incremental", as_of="latest",
                    contact_email=_git_contact_email(), **extra)
    except Exception as exc:  # noqa: BLE001
        return {"ran": True, "ok": False,
                "reason": "%s: %s" % (type(exc).__name__, str(exc)[:200])}
    counts = result.get("counts") or {}
    return {"ran": True, "ok": "BLOCKED" not in str(result.get("status") or ""),
            "status": result.get("status"),
            "records_new": int(counts.get("records_new")
                               or counts.get("normalized_new") or 0),
            "reason": result.get("reason")}


def collection_safety_contract() -> dict:
    """The invariants the continuous service must satisfy. Architecture-tested."""
    return {
        "information_collection_automation": "ON_WHEN_OPERATOR_STARTS_SERVICE",
        "portfolio_reassessment_automation": "ON_PAPER_RESEARCH_ARTIFACTS_ONLY",
        "manual_portfolio_review": "REQUIRED",
        "execution_automation_enabled": False,
        "broker_execution_enabled": False,
        "model_promotion_automation_enabled": False,
        "creates_orders": False,
        "confirms_orders": False,
        "fills_orders": False,
        "cancels_orders": False,
        "approves_proposals": False,
        "confirms_targets": False,
        "runs_controlled_rebalance": False,
        "runs_daily_close": False,
        "runs_daily_research_cycle": False,
        "promotes_models": False,
        "mutates_model_weights": False,
        "may_create": ["immutable source records", "normalized PIT event evidence",
                       "source runtime state", "Release-28 event-cycle artifacts",
                       "holding opportunity-cost assessments",
                       "portfolio reassessments",
                       "review-only reallocation proposals"],
        "badges": ["INFORMATION COLLECTION: ON", "PAPER MODE", "MANUAL REVIEW",
                   "EXECUTION AUTOMATION: OFF", "NO BROKER EXECUTION",
                   "NO LIVE ORDERS"],
        "note": ("Collection automation and execution automation are DIFFERENT "
                 "switches. This release turns the first one on and leaves the "
                 "second one off, unreachable and architecture-tested."),
    }


# --------------------------------------------------------------------------- #
# THE authoritative per-source runtime health (the fixed freshness denominator)
# --------------------------------------------------------------------------- #
def _terminal_states(*, ingestion_root=None, news_root=None,
                     capability: Optional[dict] = None) -> dict:
    cap = capability
    if cap is None:
        try:
            cap = scap.build_capability_matrix(ingestion_root=ingestion_root,
                                               news_root=news_root)
        except Exception:  # noqa: BLE001 - a read contract must never crash
            cap = {"sources": []}
    return {s["source_id"]: s.get("terminal_state")
            for s in (cap.get("sources") or [])}


def build_source_runtime_health(*, root=None, now: Optional[datetime] = None,
                                runtime_state: Optional[dict] = None,
                                credentials: Optional[dict] = None,
                                terminal_states: Optional[dict] = None,
                                running_source: Any = None,
                                env: Optional[dict] = None,
                                capability: Optional[dict] = None,
                                ingestion_root=None, news_root=None) -> dict:
    """ONE authoritative runtime row per source, plus the honest denominator.

    This REPLACES the misleading "fresh N of 17" reading with the question an
    operator actually needs answered: of the sources whose own publication window
    is open right now, how many are healthy? Every value is computed here; the
    browser derives none of it.
    """
    now = now or _now()
    rt = runtime_state if runtime_state is not None else load_source_runtime_state(
        root=root)
    creds = credentials if credentials is not None else credential_availability(
        env=env)
    terms = terminal_states if terminal_states is not None else _terminal_states(
        ingestion_root=ingestion_root, news_root=news_root, capability=capability)
    session = mh.session_state(now)
    rows = []
    for policy in cad.CADENCE_POLICY_TABLE:
        sid = policy["source_id"]
        cred = (creds.get("sources") or {}).get(sid) or {}
        available = cred.get("available") if cred.get("required") else None
        row = cad.resolve_source_runtime(
            policy=policy, state=_reset_daily_counters(dict(rt.get(sid) or {}), now),
            now=now, session=session, credential_available=available,
            terminal_state=terms.get(sid),
            running=(str(running_source or "") == sid))
        src = scap.SOURCE_BY_ID.get(sid) or {}
        row["label"] = src.get("label") or sid
        row["lane"] = COLLECTOR_LANE.get(sid, "NOT_SCHEDULED")
        row["decision_authorities"] = src.get("decision_authorities")
        row["expected_cadence_text"] = src.get("available_frequency")
        rows.append(row)
    summary = cad.summarize_runtime(rows)
    return {
        "contract_id": "paper_trader.source_runtime_health/1",
        "composition_owner": COMPOSITION_OWNER,
        "cadence_owner": cad.CALCULATION_OWNER,
        "market_session_owner": "engine.market_hours",
        "generated_at": _iso(now),
        "market_session": session,
        "sources": rows,
        "summary": summary,
        "next_wake_seconds": cad.next_wake_seconds(rows, now=now),
        "note": ("A source that is not expected to publish now reads NOT_DUE, not "
                 "STALE. The KPI denominator is the set of sources whose own "
                 "window is open at this moment."),
    }


# --------------------------------------------------------------------------- #
# THE collection iteration
# --------------------------------------------------------------------------- #
_KIND_URGENCY = {
    cad.K_INTRADAY_MARKET: 0, cad.K_CONTINUOUS_EVENT: 1, cad.K_SESSION_END: 2,
    cad.K_LOCAL_FILE_WATCH: 3, cad.K_DAILY_PUBLICATION: 4,
    cad.K_MONTHLY_RELEASE: 5, cad.K_QUARTERLY_RELEASE: 6,
}


def _due_priority(row: dict) -> tuple:
    """Holdings-facing operational lanes first, then the most overdue."""
    tier_rank = {cad.TIER_HOLDINGS: 0, cad.TIER_CANDIDATES: 1,
                 cad.TIER_GLOBAL: 2, cad.TIER_UNIVERSE: 3}
    overdue = -(row.get("seconds_since_last_success") or 0.0)
    return (0 if row.get("operational") else 1,
            _KIND_URGENCY.get(row.get("cadence_kind"), 9),
            tier_rank.get(row.get("attention_tier"), 9),
            overdue)


def run_collection_iteration(
        *, root=None, instance_id: Optional[str] = None,
        now: Optional[datetime] = None, env: Optional[dict] = None,
        require_enabled: bool = True,
        max_sources: int = MAX_SOURCES_PER_ITERATION,
        attention_universe: Optional[dict] = None,
        portfolio_state: Optional[dict] = None, scoring: Optional[dict] = None,
        portfolio_state_loader: Optional[Callable] = None,
        scoring_loader: Optional[Callable] = None,
        stage2_fn: Optional[Callable] = None, news_fn: Optional[Callable] = None,
        event_cycle_fn: Optional[Callable] = None,
        quote_fetcher: Optional[Callable] = None,
        gdelt_fetcher: Optional[Callable] = None,
        ingestion_root=None, news_root=None, fabric_dir=None, hoc_dir=None,
        reassessment_dir=None, reallocation_dir=None,
        capability: Optional[dict] = None,
        progress_fn: Optional[Callable] = None,
        force_event_cycle: bool = False) -> dict:
    """ONE canonical collection iteration. Bounded, restart-safe, idempotent.

    Collects only the sources that are ACTUALLY due, under their own budgets and
    circuits, then hands whatever arrived to the Release-28 event orchestrator in
    ONE consolidated pass. When nothing new arrived it advances health state and
    stops — no opportunity cost, no reassessment, no proposal.

    ``progress_fn(step, detail=None)`` is the worker's progress callback. This
    function marks the iteration IN FLIGHT for its whole duration and calls the
    callback at every bounded unit of work — including inside the collector
    lanes and inside the Release-28 cycle — so a legitimately long pass proves
    it is still advancing instead of ageing out of a start-of-iteration stamp.
    """
    now = now or _now()
    progress = _safe_progress(progress_fn)
    # The worker's reporter lives as long as the worker, so its sequence is a
    # LIFETIME counter. Take a baseline to report this pass's own checkpoints.
    checkpoints_at_start = int(getattr(progress_fn, "seq", 0) or 0)
    started_iso = _iso(now)
    iteration_id = "collect_%s_%s" % (now.strftime("%Y%m%dT%H%M%S"),
                                      uuid.uuid4().hex[:8])
    service = load_service_state(root=root)
    warnings: list[str] = []

    if require_enabled and not service.get("collection_automation_enabled"):
        return {"schema_version": SCHEMA_VERSION, "iteration_id": iteration_id,
                "started_at": started_iso, "finished_at": _iso(_now()),
                "ran": False, "state": "COLLECTION_AUTOMATION_DISABLED",
                "reason": ("Continuous collection has not been authorised by the "
                           "operator. Nothing was collected and no provider was "
                           "called."),
                "confirm_required": ENABLE_CONFIRM_TOKEN,
                "safety": collection_safety_contract()}

    # From here the iteration is OPEN. Health now judges this worker on PROGRESS
    # rather than on the wake stamp, and the flag is cleared on the way out —
    # normally by the final state write below, and by the worker's own error
    # branch when the iteration raises.
    _mark_iteration_in_flight(root=root, iteration_id=iteration_id,
                              instance_id=instance_id, started_at=started_iso,
                              now=now)

    ing_root = scap.ingestion_root(ingestion_root)
    nws_root = scap.news_root(news_root)

    # ---- 1. attention universe (authoritative owners) ---------------------- #
    progress("ATTENTION_UNIVERSE", detail="reading holdings and candidates")
    universe = attention_universe
    if universe is None:
        universe = build_attention_universe(
            portfolio_state=portfolio_state, scoring=scoring,
            portfolio_state_loader=portfolio_state_loader,
            scoring_loader=scoring_loader)
    warnings.extend(universe.get("warnings") or [])

    # ---- 2. market session + due plan -------------------------------------- #
    progress("DUE_PLAN", detail="resolving which sources are due now")
    creds = credential_availability(env=env)
    terms = _terminal_states(ingestion_root=ing_root, news_root=nws_root,
                             capability=capability)
    rt_state = load_source_runtime_state(root=root)
    rt_state = {sid: _reset_daily_counters(dict(row or {}), now)
                for sid, row in rt_state.items()}
    health_before = build_source_runtime_health(
        root=root, now=now, runtime_state=rt_state, credentials=creds,
        terminal_states=terms)
    session = health_before["market_session"]

    due_rows = [r for r in health_before["sources"] if r["collect_now"]]
    budget_blocked: list[dict] = []
    admitted_due: list[dict] = []
    for row in sorted(due_rows, key=_due_priority):
        sid = row["source_id"]
        policy = cad.CADENCE_POLICY_BY_ID[sid]
        verdict = budget_verdict(policy=policy,
                                 row=rt_state.get(sid) or {}, now=now)
        if not verdict["allowed"]:
            budget_blocked.append({"source_id": sid, "reason": verdict["reason"]})
            continue
        admitted_due.append(row)
    deferred_for_cap = [r["source_id"] for r in admitted_due[int(max_sources):]]
    admitted_due = admitted_due[:int(max_sources)]
    if deferred_for_cap:
        warnings.append(
            "Bounded catch-up: %d due source(s) deferred to the next iteration "
            "(%s) so a wake-from-sleep is a pass, not a request storm."
            % (len(deferred_for_cap), ", ".join(deferred_for_cap)))

    due_ids = [r["source_id"] for r in admitted_due]
    stage2_due = [s for s in due_ids if COLLECTOR_LANE.get(s) == LANE_STAGE2]
    news_due = [s for s in due_ids if COLLECTOR_LANE.get(s) == LANE_NEWS_RSS]
    quotes_due = "yahoo_delayed_quote" in due_ids
    gdelt_due = "gdelt" in due_ids

    # ---- 3. collect, one lane at a time; a failure never blocks the rest ---- #
    collected: list[dict] = []
    corpus_new_records = 0
    for sid in due_ids:
        rt_state[sid] = _record_call(dict(rt_state.get(sid) or {}), now)

    if stage2_due:
        t0 = time.time()
        progress("COLLECT_STAGE2", detail="due: %s" % ", ".join(stage2_due))
        res = collect_stage2(source_ids=stage2_due, ingestion_root=ing_root,
                             run_fn=stage2_fn, progress_fn=progress_fn)
        elapsed = round(time.time() - t0, 2)
        progress("COLLECT_STAGE2", detail="lane finished in %.1fs" % elapsed)
        per_source = res.get("per_source") or {}
        lane_ok = bool(res.get("ok"))
        lane_new = int(res.get("records_new") or 0)
        corpus_new_records += lane_new
        for sid in stage2_due:
            detail = per_source.get(sid) or {}
            state_text = str(detail.get("overall_state") or detail.get("state") or "")
            src_ok = lane_ok and state_text not in ("FAILED", "BLOCKED_CREDENTIAL",
                                                    "BLOCKED_CONFIGURATION")
            if src_ok:
                rt_state[sid] = _record_success(
                    rt_state[sid], now,
                    new_records=int(detail.get("records_normalized_new") or 0),
                    watermark=detail.get("newest_source_event_time"))
            else:
                rt_state[sid] = _record_failure(
                    rt_state[sid], now, status=None,
                    detail=(state_text or res.get("reason")
                            or "Stage-2 lane did not complete"))
            collected.append({
                "source_id": sid, "lane": LANE_STAGE2, "ok": src_ok,
                "duration_seconds": elapsed, "status": res.get("status"),
                "new_records": int(detail.get("records_normalized_new") or 0),
                "duplicates_prevented": int(detail.get("duplicates_prevented") or 0),
                "error_category": rt_state[sid].get("last_error_category"),
                "detail": (None if src_ok else
                           (state_text or res.get("reason"))),
            })

    if news_due:
        t0 = time.time()
        progress("COLLECT_NEWS_RSS", detail="official RSS/Atom feeds")
        res = collect_news_rss(news_root=nws_root, run_fn=news_fn,
                               progress_fn=progress_fn)
        elapsed = round(time.time() - t0, 2)
        progress("COLLECT_NEWS_RSS", detail="lane finished in %.1fs" % elapsed)
        sid = "news_rss"
        ok = bool(res.get("ok"))
        lane_new = int(res.get("records_new") or 0)
        corpus_new_records += lane_new
        rt_state[sid] = (_record_success(rt_state[sid], now, new_records=lane_new)
                         if ok else
                         _record_failure(rt_state[sid], now, status=None,
                                         detail=res.get("reason")
                                         or "news lane did not complete"))
        collected.append({"source_id": sid, "lane": LANE_NEWS_RSS, "ok": ok,
                          "duration_seconds": elapsed, "status": res.get("status"),
                          "new_records": lane_new,
                          "error_category": rt_state[sid].get("last_error_category"),
                          "detail": (None if ok else res.get("reason"))})

    # ---- 4. ONE consolidated Release-28 event pass ------------------------- #
    bootstrap = not service.get("last_event_cycle_at")
    should_run_cycle = bool(force_event_cycle or corpus_new_records > 0
                            or quotes_due or gdelt_due or bootstrap)
    authorization = {
        "collection_service_started_by_operator": bool(service.get("started_at")),
        "collection_automation_enabled": bool(
            service.get("collection_automation_enabled")),
        "execution_automation_enabled": False,
        "broker_path_exists": False,
        "single_flight_gate_passed": True,
        "within_released_authority": True,
    }
    cycle: Optional[dict] = None
    cycle_reason: str
    if not should_run_cycle:
        cycle_reason = ("No source produced new information and no live adapter was "
                        "due, so no opportunity cost, reassessment or proposal work "
                        "was performed.")
    elif not all(authorization[k] for k in
                 ("collection_service_started_by_operator",
                  "collection_automation_enabled")) and require_enabled:
        cycle_reason = ("The event cycle is authorised only while the operator has "
                        "started and enabled the collection service.")
        should_run_cycle = False
    else:
        fn = event_cycle_fn or esr.run_event_signal_refresh
        progress("EVENT_CYCLE", detail="Release-28 incremental dependency refresh")
        try:
            cycle = fn(confirm=esr.EXECUTE_CONFIRM_TOKEN,
                       requested_by="%s:%s" % (SERVICE_ID, instance_id or "adhoc"),
                       fabric_dir=fabric_dir, hoc_dir=hoc_dir,
                       reassessment_dir=reassessment_dir,
                       reallocation_dir=reallocation_dir,
                       ingestion_root=ing_root, news_root=nws_root,
                       include_market_quotes=quotes_due, include_gdelt=gdelt_due,
                       quote_fetcher=quote_fetcher, gdelt_fetcher=gdelt_fetcher,
                       # The event orchestrator is the LONGEST single leg of an
                       # iteration (measured: 269s of a 342s pass, 245s of it in
                       # ONE step). It reports its own step and per-file progress
                       # through this same callback, so BUSY stays provable there.
                       progress_fn=progress_fn,
                       # ONE clock: the iteration's own. The live adapters stamp
                       # event identity with it instead of taking a second ambient
                       # reading, so an iteration is reproducible at any moment.
                       now_iso=started_iso)
            cycle_reason = ("New information arrived, so the Release-28 event "
                            "orchestrator ran ONE incremental dependency refresh.")
        except Exception as exc:  # noqa: BLE001 - the loop must survive
            cycle = None
            cycle_reason = "event cycle failed: %s: %s" % (type(exc).__name__,
                                                           str(exc)[:200])
            warnings.append(cycle_reason)

    # Live-adapter outcomes are reported BY the cycle; fold them into runtime state.
    if cycle:
        adapters = cycle.get("adapters") or {}
        if quotes_due:
            mq = adapters.get("market_quotes") or {}
            ok = bool(mq.get("ok", False))
            rt_state["yahoo_delayed_quote"] = (
                _record_success(rt_state.get("yahoo_delayed_quote") or {}, now,
                                new_records=int(mq.get("fetched") or 0))
                if ok else
                _record_failure(rt_state.get("yahoo_delayed_quote") or {}, now,
                                status=None, detail=mq.get("detail")
                                or "delayed quote adapter degraded"))
            collected.append({"source_id": "yahoo_delayed_quote",
                              "lane": LANE_LIVE_ADAPTER, "ok": ok,
                              "new_records": int(mq.get("fetched") or 0),
                              "error_category":
                                  rt_state["yahoo_delayed_quote"].get(
                                      "last_error_category"),
                              "detail": (None if ok else mq.get("detail"))})
        if gdelt_due:
            gd = adapters.get("gdelt") or {}
            ok = bool(gd.get("ok", False))
            probes = gd.get("probes") or []
            status = next((p.get("status") for p in probes if p.get("status")), None)
            rt_state["gdelt"] = (
                _record_success(rt_state.get("gdelt") or {}, now,
                                new_records=int(gd.get("event_count") or 0))
                if ok else
                _record_failure(rt_state.get("gdelt") or {}, now, status=status,
                                detail=gd.get("detail") or "GDELT adapter degraded"))
            collected.append({"source_id": "gdelt", "lane": LANE_LIVE_ADAPTER,
                              "ok": ok,
                              "new_records": int(gd.get("event_count") or 0),
                              "http_status": status,
                              "error_category":
                                  rt_state["gdelt"].get("last_error_category"),
                              "detail": (None if ok else gd.get("detail"))})

    # ---- 5. persist state, receipt and next wake --------------------------- #
    progress("PERSIST_ITERATION", detail="writing runtime state and receipt")
    save_source_runtime_state(rt_state, root=root)
    finished = _now()
    health_after = build_source_runtime_health(
        root=root, now=finished, runtime_state=rt_state, credentials=creds,
        terminal_states=terms)
    wake_seconds = cad.next_wake_seconds(health_after["sources"], now=finished)
    next_wake_at = _iso(finished + timedelta(seconds=wake_seconds))

    admitted = int((cycle or {}).get("events_admitted") or 0)
    cycle_state = (cycle or {}).get("state")
    material = bool((cycle or {}).get("materiality", {}).get("reassessment_required"))
    reassessed = bool((cycle or {}).get("reassessment_ran"))
    proposal = bool((cycle or {}).get("proposal_built"))

    # Re-read before the final write. The progress callback has been writing to
    # this same document throughout the iteration, so saving the copy loaded at
    # the top would silently roll the progress evidence back to the start stamp —
    # exactly the bug this release exists to remove.
    service = load_service_state(root=root)
    service.update({
        "loop_count": int(service.get("loop_count") or 0) + 1,
        "last_iteration_started_at": started_iso,
        "last_iteration_finished_at": _iso(finished),
        "last_iteration_id": iteration_id,
        "current_source": None,
        # The iteration is CLOSED: the worker returns to healthy/idle and health
        # goes back to judging it on the heartbeat.
        "iteration_in_flight": False,
        "current_iteration_id": None,
        "progress_at": _iso(finished),
        "progress_step": "ITERATION_END",
        "progress_detail": iteration_id,
        "progress_seq": int(service.get("progress_seq") or 0) + 1,
        "progress_iteration_id": iteration_id,
        "next_wake_at": next_wake_at,
        "heartbeat_at": _iso(finished),
    })
    if any(c.get("ok") for c in collected):
        service["last_collection_success_at"] = _iso(finished)
    if admitted > 0 or corpus_new_records > 0:
        service["last_new_information_at"] = _iso(finished)
    if cycle_state:
        service["last_event_cycle_at"] = _iso(finished)
        service["last_event_cycle_state"] = cycle_state
    if material:
        service["last_material_information_at"] = _iso(finished)
    if reassessed:
        service["last_reassessment_at"] = _iso(finished)
    if proposal:
        service["last_target_change_at"] = _iso(finished)
    save_service_state(service, root=root)

    receipt = {
        "schema_version": SCHEMA_VERSION,
        "iteration_id": iteration_id,
        "service_id": SERVICE_ID,
        "instance_id": instance_id,
        "pid": os.getpid(),
        "started_at": started_iso,
        "finished_at": _iso(finished),
        "duration_seconds": round((finished - now).total_seconds(), 2),
        "ran": True,
        "state": ("MATERIAL_INFORMATION" if material else
                  ("NEW_INFORMATION" if (admitted or corpus_new_records) else
                   "NO_NEW_INFORMATION")),
        "session_phase": session["phase"],
        "market_session": session,
        "attention_universe": {
            "active_book_id": universe.get("active_book_id"),
            "eligible_market_date": universe.get("eligible_market_date"),
            "tier_counts": {k: v["count"]
                            for k, v in (universe.get("tiers") or {}).items()},
        },
        "due_plan": due_ids,
        "due_deferred_for_cap": deferred_for_cap,
        "budget_blocked": budget_blocked,
        "collected": collected,
        "corpus_new_records": corpus_new_records,
        "event_cycle_ran": cycle is not None,
        "event_cycle_reason": cycle_reason,
        "event_cycle_authorization": authorization,
        "event_cycle": ({
            "state": cycle_state,
            "events_built": cycle.get("events_built"),
            "events_admitted": admitted,
            "duplicates_suppressed": cycle.get("duplicates_suppressed"),
            # Two different questions, deliberately both recorded: which holdings had
            # an INPUT invalidated (refresh scope — a delayed quote invalidates risk
            # for its name), and which holdings a MATERIAL fact actually named (the
            # operator's attention list).
            "affected_holdings": cycle.get("affected_holdings"),
            "holdings_named_by_material_information": sorted(
                set((cycle.get("materiality") or {}).get("affected_entities") or [])
                & set(tickers_for_tier(universe, cad.TIER_HOLDINGS))),
            "reassessment_ran": reassessed,
            "reassessment_reason": cycle.get("reassessment_reason"),
            "proposal_built": proposal,
            "manual_review_required": cycle.get("manual_review_required"),
        } if cycle else None),
        "source_health_summary": health_after["summary"],
        "next_wake_seconds": round(wake_seconds, 1),
        "next_wake_at": next_wake_at,
        # How much progress evidence THIS pass emitted. A long iteration with a
        # high checkpoint count is the positive proof that BUSY was earned.
        "progress_checkpoints": max(
            0, int(getattr(progress_fn, "seq", 0) or 0) - checkpoints_at_start),
        "progress_stall_after_seconds": PROGRESS_STALL_SECONDS,
        "warnings": warnings,
        "safety": collection_safety_contract(),
    }
    _append_iteration_receipt(receipt, root=root)
    return receipt


def _append_iteration_receipt(receipt: dict, *, root=None) -> None:
    """Bounded iteration history — a continuously running service must not grow
    an unbounded evidence file."""
    path = _iterations_path(root)
    doc = _read_json(path) or {}
    history = list(doc.get("iterations") or [])
    history.append(receipt)
    if len(history) > MAX_ITERATION_RECEIPTS:
        history = history[-MAX_ITERATION_RECEIPTS:]
    _atomic_write_json(path, {
        "schema_version": SCHEMA_VERSION, "service_id": SERVICE_ID,
        "bounded_to": MAX_ITERATION_RECEIPTS, "updated_at": _iso(_now()),
        "iterations": history})


def read_iteration_history(*, root=None, limit: int = 20) -> list[dict]:
    doc = _read_json(_iterations_path(root)) or {}
    history = list(doc.get("iterations") or [])
    return history[-int(limit):][::-1]


# --------------------------------------------------------------------------- #
# Release 46.6.2 — RECOVERY: a dead collector must never be able to stay dead
# QUIETLY
# --------------------------------------------------------------------------- #
# What actually happened on 2026-08-28, reconstructed from the durable state,
# the singleton lock and the Windows Task Scheduler operational log:
#
#   13:51:14 ET  iteration 2512 finished cleanly; the worker slept, due to wake
#                at 13:51:44.
#   13:51:44 ET  the interactive logon session that owned the worker ended and
#                Task Scheduler terminated the action (event 201/102, instance
#                {2ce838cf…}). The process never ran its ``finally``, so
#                ``release_service_lock`` never ran and the singleton lock was
#                left on disk with a heartbeat THIRTY SECONDS old.
#   13:52:36 ET  a NEW logon fired the task's only trigger. The new worker
#                called :func:`acquire_service_lock`, found a lock whose
#                heartbeat age was 82 s — far inside ``LOCK_TAKEOVER_SECONDS``
#                — correctly refused to become a second worker, printed
#                ``COLLECTION_SERVICE_BLOCKED`` and exited 3.
#   13:53:01 ET  nothing ran again. For ~6 hours.
#
# Three things are true at once and only the third is a defect:
#
# * the singleton gate was RIGHT. It cannot know that a pid it can no longer
#   see is not about to reappear, and a second concurrent collector is a worse
#   outcome than a paused one. That rule is unchanged here.
# * an intentional stop and this hard kill are INDISTINGUISHABLE from the
#   state file, because whoever stops the worker owes it a clean-shutdown
#   marker and Windows owes nothing. ``-Action Stop`` writes one; a session
#   ending does not.
# * NOTHING WOULD EVER HAVE TRIED AGAIN. The scheduled task carries exactly one
#   trigger — ``LogonTrigger`` — and its ``NextRunTime`` is empty. Collection
#   could have stayed dead until the next logon, and the read model said only
#   "COLLECTION DEGRADED / RESTART_COLLECTION_SERVICE", which does not tell an
#   operator that nothing else is coming.
#
# So the repair is not a wider tolerance and not a hidden daemon. It is: say
# the recoverable truth, and give the recovery ONE authorised, idempotent owner
# that can clear a lock only when NO worker exists anywhere on this machine.

#: Recovery vocabulary. Every value is DERIVED from the two verdicts
#: :func:`resolve_service_lifecycle` already produces — this is not a second
#: state machine and it never overrides ``service_state``.
REC_RUNNING_HEALTHY = "RUNNING_HEALTHY"
REC_STARTING = "STARTING"
REC_DEGRADED_RESTARTABLE = "DEGRADED_RESTARTABLE"
REC_STOPPED_INTENTIONALLY = "STOPPED_INTENTIONALLY"
REC_NEVER_INSTALLED = "NEVER_INSTALLED"
REC_AUTOMATION_DISABLED = "AUTOMATION_DISABLED"
RECOVERY_STATES = (REC_RUNNING_HEALTHY, REC_STARTING, REC_DEGRADED_RESTARTABLE,
                   REC_STOPPED_INTENTIONALLY, REC_NEVER_INSTALLED,
                   REC_AUTOMATION_DISABLED)

#: A worker that has registered a start but has not yet proven a heartbeat this
#: recently is STARTING rather than degraded.
STARTING_GRACE_SECONDS = 90.0

#: THE recovery command. One action, named in the payload so the operator never
#: has to know which of six scripts owns the worker.
RECOVERY_COMMAND = (
    r"powershell -File C:\Users\binis\paper_trader\scripts"
    r"\manage_information_collection.ps1 "
    r"-RepoRoot C:\Users\binis\paper_trader -Action Recover -Execute")

#: Outcomes of asking whether an ABANDONED lock may be cleared. Fail closed.
LOCK_NOTHING_TO_CLEAR = "NOTHING_TO_CLEAR"
LOCK_CLEARABLE = "ABANDONED_CLEARABLE"
LOCK_REFUSED_WORKER_RUNNING = "REFUSED_A_WORKER_IS_RUNNING"
LOCK_REFUSED_TOPOLOGY_UNKNOWN = "REFUSED_WORKER_TOPOLOGY_UNRESOLVED"


def resolve_recovery(state: dict, lifecycle: dict, *,
                     now: Optional[datetime] = None) -> dict:
    """Is recovery required, is it possible, and will anything else try?

    Pure derivation from the existing verdicts. It invents no state the owner
    does not already hold and it never contradicts ``service_state``.
    """
    svc = lifecycle.get("service_state")
    act = lifecycle.get("worker_activity")
    hb = lifecycle.get("heartbeat_age_seconds")

    if not state.get("collection_automation_enabled"):
        rec, required = REC_AUTOMATION_DISABLED, False
        why = ("Information-collection automation has not been authorised on "
               "this machine, so no worker is expected to be running.")
    elif svc == SVC_NEVER_STARTED:
        rec, required = REC_NEVER_INSTALLED, True
        why = "No worker has ever run on this machine."
    elif svc == SVC_RUNNING:
        if (hb is not None and hb <= STARTING_GRACE_SECONDS
                and not state.get("loop_count")):
            rec, required, why = (REC_STARTING, False,
                                  "A worker has started and has not completed "
                                  "its first iteration yet.")
        else:
            rec, required, why = (REC_RUNNING_HEALTHY, False,
                                  "The worker is alive and %s."
                                  % ("busy" if act == ACT_BUSY else "idle"))
    elif svc == SVC_STOPPED and state.get("graceful_shutdown"):
        rec, required = REC_STOPPED_INTENTIONALLY, False
        why = ("The worker was stopped cleanly by an operator action, which "
               "recorded the shutdown marker.")
    else:
        rec, required = REC_DEGRADED_RESTARTABLE, True
        why = lifecycle.get("reason") or "The worker is not collecting."

    # An UNOWNED stop looks exactly like a crash, and saying which one it was
    # is the difference between "somebody meant this" and "nobody noticed".
    unowned = bool(state.get("started_at")
                   and not state.get("stopped_at")
                   and act in (ACT_DEAD, ACT_STALLED, ACT_UNKNOWN))
    return {
        "recovery_state": rec,
        "recovery_state_vocabulary": list(RECOVERY_STATES),
        "recovery_required": required,
        "recovery_is_a_derivation_of":
            "service_state x worker_activity (api.information_collection."
            "resolve_service_lifecycle) - never a second state machine",
        "why": why,
        "recovery_action": ("RECOVER_COLLECTION_SERVICE" if required
                            else "NONE_REQUIRED"),
        "recovery_command": (RECOVERY_COMMAND if required else None),
        "recovery_owner": "scripts/manage_information_collection.ps1",
        "recovery_is_idempotent": True,
        "recovery_creates_no_second_worker": True,
        "stop_was_unowned": unowned,
        "stop_was_unowned_note": (
            "The worker stopped without writing a clean-shutdown marker, so "
            "an intentional stop and a hard kill are indistinguishable from "
            "the durable state alone. Whoever stops a worker owes it that "
            "marker; Windows ending a logon session does not write one."
            if unowned else None),
        # The whole point of this block: nothing on this machine restarts a
        # dead collector by itself, so a degraded state that nobody looks at
        # is permanent. That sentence must appear in the payload, not in a
        # release note nobody reads.
        "nothing_restarts_it_automatically": True,
        "can_silently_remain_dead": bool(required and rec !=
                                         REC_AUTOMATION_DISABLED),
        "scheduled_task_trigger": "AT_LOGON_ONLY",
        "scheduled_task_trigger_note": (
            "The %s task carries one trigger - a logon trigger - and no "
            "repetition, so after a failed relaunch it has no future run time. "
            "Recovery is an explicit operator action by design; execution "
            "automation stays OFF and no scheduler change is made here."
            % SCHEDULED_TASK_NAME),
        "collection_automation_is_not_execution_automation": True,
    }


def resolve_abandoned_lock(*, lock: Optional[dict], topology: Optional[dict],
                           now: Optional[datetime] = None) -> dict:
    """May an OPERATOR-authorised recovery clear this singleton lock?

    The pid probe alone is not enough authority to take a slot early - that is
    why :func:`acquire_service_lock` waits out ``LOCK_TAKEOVER_SECONDS`` and
    why that rule is unchanged. The launch TOPOLOGY is stronger evidence: if no
    process anywhere on this machine is running the canonical worker script,
    there is no worker to collide with and the lock is an artefact of a kill
    that never ran its ``finally``. Anything less certain fails CLOSED.
    """
    now = now or _now()
    if not lock:
        return {"state": LOCK_NOTHING_TO_CLEAR, "may_clear": False,
                "reason": "no singleton lock is present"}
    verdict = (topology or {}).get("verdict")
    holder = {k: (lock or {}).get(k) for k in
              ("instance_id", "pid", "host", "acquired_at", "heartbeat_at")}
    evidence = {
        "holder": holder,
        "holder_pid_alive": _pid_alive(holder.get("pid")),
        "heartbeat_age_seconds": (
            None if _age(lock.get("heartbeat_at"), now) is None
            else round(_age(lock.get("heartbeat_at"), now), 1)),
        "worker_topology_verdict": verdict,
        "worker_topology_reason": (topology or {}).get("reason"),
    }
    if verdict == WORKER_TOPOLOGY_NONE:
        return dict(evidence, state=LOCK_CLEARABLE, may_clear=True,
                    reason=("no process on this machine is running %s, so the "
                            "lock names a worker that does not exist"
                            % CANONICAL_WORKER_SCRIPT))
    if verdict in (WORKER_TOPOLOGY_SINGLE, WORKER_TOPOLOGY_VIOLATED):
        return dict(evidence, state=LOCK_REFUSED_WORKER_RUNNING,
                    may_clear=False,
                    reason=("a collection worker IS running; clearing the "
                            "singleton lock would permit a second one"))
    return dict(evidence, state=LOCK_REFUSED_TOPOLOGY_UNKNOWN, may_clear=False,
                reason=("the worker topology could not be resolved, and a "
                        "recovery that cannot prove the machine is empty "
                        "fails closed"))


def clear_abandoned_lock(*, root=None, lock: Optional[dict] = None,
                         topology: Optional[dict] = None,
                         now: Optional[datetime] = None) -> dict:
    """Clear a PROVABLY abandoned singleton lock. Idempotent, attributed.

    Writes the clean-shutdown marker the killed worker never wrote, so the
    next reader sees an owned stop rather than a silent gap. Never touches a
    lock while any worker process exists.
    """
    now = now or _now()
    lock = lock if lock is not None else read_service_lock(root=root)
    decision = resolve_abandoned_lock(lock=lock, topology=topology, now=now)
    if not decision.get("may_clear"):
        return dict(decision, cleared=False)
    path = _lock_path(root)
    try:
        path.unlink()
        cleared = True
    except OSError:
        cleared = False
    state = load_service_state(root=root)
    state["stopped_at"] = _iso(now)
    state["graceful_shutdown"] = False
    state["current_source"] = None
    state["iteration_in_flight"] = False
    state["current_iteration_id"] = None
    state["last_error"] = (
        "worker pid %s vanished without releasing the singleton lock; the "
        "lock was cleared by an operator recovery after the process table "
        "proved no collection worker was running"
        % (lock or {}).get("pid"))
    save_service_state(state, root=root)
    return dict(decision, cleared=cleared,
                write_owner="api.information_collection.clear_abandoned_lock",
                wrote_clean_shutdown_marker=True)


# --------------------------------------------------------------------------- #
# READ CONTRACT — GET /v1/operations/information-collection
# --------------------------------------------------------------------------- #
def _headline(*, lifecycle: dict, service: dict, summary: dict,
              last_receipt: Optional[dict], now: datetime,
              recovery: Optional[dict] = None) -> dict:
    """The operator sentence, in the order an active manager actually asks:
    is collection running, did anything important arrive, and what must I review?
    """
    svc = lifecycle["service_state"]
    rec = recovery or {}
    if svc == SVC_NEVER_STARTED:
        return {"title": "COLLECTION NOT INSTALLED",
                "tone": "idle",
                "detail": ("Continuous collection has never been started on this "
                           "machine. Install and start the collection service to "
                           "begin acquiring information automatically."),
                "operator_action": "INSTALL_AND_START_COLLECTION_SERVICE"}
    if svc == SVC_STOPPED:
        return {"title": "COLLECTION STOPPED", "tone": "warn",
                "detail": ("The collection service is stopped, so no new "
                           "information is arriving. Execution safety is "
                           "unchanged: no orders, no broker, manual review."),
                "operator_action": "START_COLLECTION_SERVICE"}
    if svc == SVC_DEGRADED:
        # Release 46.6.2: "DEGRADED" alone let a dead collector look like
        # something that might come back. Nothing restarts it, so the headline
        # says so and names the one command that does.
        return {"title": "COLLECTION DEGRADED", "tone": "warn",
                "detail": ("%s Nothing will restart it automatically - "
                           "recovery is an explicit operator action."
                           % lifecycle["reason"]
                           if rec.get("can_silently_remain_dead")
                           else lifecycle["reason"]),
                "operator_action": ("RECOVER_COLLECTION_SERVICE"
                                    if rec.get("recovery_required")
                                    else "RESTART_COLLECTION_SERVICE"),
                "operator_command": rec.get("recovery_command"),
                "recovery_state": rec.get("recovery_state")}
    material_age = _age(service.get("last_material_information_at"), now)
    if (last_receipt or {}).get("state") == "MATERIAL_INFORMATION" or (
            material_age is not None and material_age < 86400):
        return {"title": "MATERIAL INFORMATION RECEIVED", "tone": "attention",
                "detail": ("New information arrived that was material enough to "
                           "re-ask the portfolio question."),
                "operator_action": "REVIEW_PORTFOLIO_DECISION"}
    if lifecycle.get("worker_activity") == ACT_BUSY:
        # A worker in the middle of a long, advancing pass is healthy. Saying so
        # explicitly is what stops an operator restarting a working service.
        return {"title": "COLLECTION RUNNING", "tone": "ok",
                "detail": ("Collecting now — %s. %s."
                           % (lifecycle["reason"], summary.get("headline") or "")),
                "operator_action": "NONE_REQUIRED"}
    return {"title": "COLLECTION RUNNING", "tone": "ok",
            "detail": ("No material new information since %s. %s."
                       % (str(service.get("last_material_information_at")
                              or service.get("last_collection_success_at")
                              or service.get("started_at"))[:19] or "startup",
                          summary.get("headline") or "")),
            "operator_action": "NONE_REQUIRED"}


def load_information_collection(*, root=None, limit: int = 12,
                                fabric_dir=None, ingestion_root=None,
                                news_root=None, env: Optional[dict] = None,
                                event_status: Optional[dict] = None,
                                now: Optional[datetime] = None) -> dict:
    """READ-ONLY live collection status. Calls no provider and writes nothing.

    ONE payload answers, in order: is collection running, did anything important
    arrive, which holdings are affected, was the portfolio reassessed, did the
    recommendation change, and what must the operator review.
    """
    now = now or _now()
    warnings: list[str] = []
    service = load_service_state(root=root)
    lock = read_service_lock(root=root)
    lifecycle = resolve_service_lifecycle(service, lock, now)
    health = build_source_runtime_health(root=root, now=now, env=env,
                                         ingestion_root=ingestion_root,
                                         news_root=news_root)
    history = read_iteration_history(root=root, limit=int(limit))
    last_receipt = history[0] if history else None

    evt = event_status
    if evt is None:
        try:
            evt = esr.load_event_signal_refresh_status(
                fabric_dir=fabric_dir, ingestion_root=ingestion_root,
                news_root=news_root, limit=40)
        except Exception as exc:  # noqa: BLE001 - a read contract must never crash
            evt = {}
            warnings.append("event status unavailable: %s" % str(exc)[:160])
    last_run = (evt or {}).get("last_run") or {}

    try:
        universe = build_attention_universe()
    except Exception as exc:  # noqa: BLE001
        universe = {"tiers": {}, "holdings": [], "candidates": []}
        warnings.append("attention universe unavailable: %s" % str(exc)[:160])

    affected = []
    held = set(universe.get("holdings") or [])
    for ev in ((evt or {}).get("events_affecting_holdings") or [])[:25]:
        ticker = str(ev.get("primary_ticker") or "").upper()
        affected.append({
            "ticker": ticker,
            "event": ev.get("family"),
            "event_type": ev.get("event_type"),
            "event_time": ev.get("published_at") or ev.get("effective_at"),
            "authority": ev.get("decision_authority"),
            "novelty": ev.get("novelty"),
            "source_id": ev.get("source_id"),
            "title": (ev.get("materiality_inputs") or {}).get("title"),
            "is_current_holding": ticker in held,
        })

    summary = health["summary"]
    recovery = resolve_recovery(service, lifecycle, now=now)
    headline = _headline(lifecycle=lifecycle, service=service, summary=summary,
                         last_receipt=last_receipt, now=now,
                         recovery=recovery)
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "composition_owner": COMPOSITION_OWNER,
        "contract_id": SERVICE_CONTRACT_ID,
        "generated_at": _iso(now),
        "headline": headline,
        # Release 46.6.2 - whether this worker can come back, who brings it
        # back, and whether anything else would. Derived from the two verdicts
        # below; never a competing state machine.
        "recovery": recovery,
        "service": {
            "service_id": SERVICE_ID,
            "scheduled_task_name": SCHEDULED_TASK_NAME,
            "service_state": lifecycle["service_state"],
            "service_state_vocabulary": list(SERVICE_STATES),
            "reason": lifecycle["reason"],
            # IS IT WORKING, or merely running? Computed here, rendered verbatim.
            "worker_activity": lifecycle["worker_activity"],
            "worker_activity_vocabulary": list(WORKER_ACTIVITY_STATES),
            "worker_activity_reason": lifecycle["activity_reason"],
            "iteration_in_flight": lifecycle["iteration_in_flight"],
            "current_iteration_id": service.get("current_iteration_id"),
            "progress_at": service.get("progress_at"),
            "progress_age_seconds": lifecycle["progress_age_seconds"],
            "progress_seq": service.get("progress_seq"),
            "progress_step": service.get("progress_step"),
            "progress_detail": service.get("progress_detail"),
            "progress_stall_after_seconds": PROGRESS_STALL_SECONDS,
            "instance_id": service.get("instance_id"),
            "worker_pid": lifecycle["worker_pid"],
            "worker_alive": lifecycle["worker_alive"],
            "host": service.get("host"),
            "started_at": service.get("started_at"),
            # R55.2 — the release THIS worker process loaded when it started,
            # published verbatim. Whether it matches the deployed source is a
            # comparison, and the comparison belongs to api.runtime_identity;
            # this block only carries the worker's own recorded fact.
            "loaded_release": service.get("loaded_release"),
            "loaded_release_owner": _RELEASE_IDENTITY_OWNER,
            "heartbeat_at": service.get("heartbeat_at"),
            "heartbeat_age_seconds": lifecycle["heartbeat_age_seconds"],
            "heartbeat_stale_after_seconds": HEARTBEAT_STALE_SECONDS,
            "stopped_at": service.get("stopped_at"),
            "graceful_shutdown": service.get("graceful_shutdown"),
            "loop_count": service.get("loop_count"),
            "restart_count": service.get("restart_count"),
            "last_iteration_started_at": service.get("last_iteration_started_at"),
            "last_iteration_finished_at": service.get("last_iteration_finished_at"),
            "current_source": service.get("current_source"),
            "next_wake_at": service.get("next_wake_at"),
            "single_flight_lock": ({"held": bool(lock),
                                    "instance_id": (lock or {}).get("instance_id"),
                                    "pid": (lock or {}).get("pid"),
                                    "acquired_at": (lock or {}).get("acquired_at")}),
        },
        "automation": {
            "information_collection_enabled": bool(
                service.get("collection_automation_enabled")),
            "information_collection_enabled_at": service.get("enabled_at"),
            "portfolio_reassessment_automation": "ON_PAPER_RESEARCH_ARTIFACTS_ONLY",
            "execution_automation_enabled": False,
            "broker_execution_enabled": False,
            "model_promotion_automation_enabled": False,
            "manual_review_required": True,
            "distinction": ("Collection automation and execution automation are "
                            "different switches. Information may flow "
                            "continuously; nothing may execute."),
        },
        "activity": {
            "last_collection_success_at": service.get("last_collection_success_at"),
            "last_new_information_at": service.get("last_new_information_at"),
            "last_material_information_at": service.get(
                "last_material_information_at"),
            "last_event_cycle_at": service.get("last_event_cycle_at"),
            "last_event_cycle_state": service.get("last_event_cycle_state")
                                      or (evt or {}).get("state"),
            "last_reassessment_at": service.get("last_reassessment_at"),
            "last_target_change_at": service.get("last_target_change_at"),
            "seconds_since_last_collection": _age(
                service.get("last_collection_success_at"), now),
            "seconds_since_material_information": _age(
                service.get("last_material_information_at"), now),
        },
        "source_health": health,
        "source_health_summary": summary,
        "market_session": health["market_session"],
        "attention_universe": universe,
        "cadence_policy": cad.policy_contract(),
        "credential_presence": credential_availability(env=env),
        "recent_iterations": history,
        "last_iteration": last_receipt,
        "recent_successful_collections": [
            {"source_id": c["source_id"], "new_records": c.get("new_records"),
             "at": (last_receipt or {}).get("finished_at")}
            for c in ((last_receipt or {}).get("collected") or []) if c.get("ok")],
        "recent_provider_failures": [
            {"source_id": r["source_id"], "runtime_state": r["runtime_state"],
             "error_category": r.get("last_error_category"),
             "last_error": r.get("last_error"),
             "backoff_until": r.get("backoff_until"),
             "circuit_state": r.get("circuit_state"),
             "next_due_at": r.get("next_due_at")}
            for r in health["sources"]
            if str(r["runtime_state"]) in cad.UNHEALTHY_STATES],
        "affected_holdings": affected,
        "portfolio_decision": {
            "event_cycle_state": (evt or {}).get("state"),
            "reassessment_ran": last_run.get("reassessment_ran"),
            "reassessment_reason": last_run.get("reassessment_reason"),
            "portfolio_reassessment": last_run.get("portfolio_reassessment"),
            "holding_opportunity_cost": last_run.get("holding_opportunity_cost"),
            "target_portfolio": last_run.get("target_portfolio"),
            "manual_review_required": bool(last_run.get("manual_review_required")),
            "proposal_built": bool(last_run.get("proposal_built")),
        },
        "event_counts": {
            "recent_events": (evt or {}).get("recent_event_count"),
            "material_events": (evt or {}).get("material_event_count"),
            "events_naming_a_holding": len(
                (evt or {}).get("events_affecting_holdings") or []),
        },
        "release28_integration": {
            "event_orchestrator": esr.COMPOSITION_OWNER,
            "cycle_id": esr.CYCLE_ID,
            "forked": False,
            "note": ("Continuous collection invokes the EXISTING Release-28 event "
                     "orchestrator; it does not contain a second one."),
        },
        "store_root": str(collection_root(root)),
        "store_root_env": COLLECTION_DIR_ENV,
        "warnings": warnings,
        "safety": dict(collection_safety_contract(),
                       read_only=True, performed_write=False),
    }


__all__ = [
    "PHASE", "COMPOSITION_OWNER", "SCHEMA_VERSION", "SERVICE_ID",
    "SERVICE_CONTRACT_ID", "SCHEDULED_TASK_NAME", "ENABLE_CONFIRM_TOKEN",
    "COLLECTION_DIR_ENV", "NOW_ENV", "SERVICE_STATES",
    "SVC_RUNNING", "SVC_STOPPED", "SVC_DEGRADED", "SVC_NEVER_STARTED",
    "HEARTBEAT_STALE_SECONDS", "LOCK_TAKEOVER_SECONDS",
    "PROGRESS_STALL_SECONDS", "PROGRESS_WRITE_MIN_SECONDS",
    "WORKER_ACTIVITY_STATES", "HEALTHY_ACTIVITY_STATES",
    "ACT_IDLE", "ACT_BUSY", "ACT_STALLED", "ACT_DEAD", "ACT_UNKNOWN",
    "ACT_NOT_RUNNING",
    "MAX_SOURCES_PER_ITERATION", "MAX_ITERATION_RECEIPTS",
    "COLLECTOR_LANE", "LANE_STAGE2", "LANE_NEWS_RSS", "LANE_LIVE_ADAPTER",
    "collection_root", "logs_root", "now_utc", "utc_iso",
    "load_service_state", "save_service_state", "set_collection_automation",
    "load_source_runtime_state", "save_source_runtime_state",
    "acquire_service_lock", "acquire_service_lock_with_wait",
    "release_service_lock", "read_service_lock",
    "heartbeat", "record_progress", "ProgressReporter",
    "clear_iteration_in_flight",
    "register_worker_start", "resolve_service_lifecycle",
    "CANONICAL_WORKER_SCRIPT", "WORKER_TOPOLOGY_VERDICTS",
    "WORKER_TOPOLOGY_NONE", "WORKER_TOPOLOGY_SINGLE",
    "WORKER_TOPOLOGY_VIOLATED", "WORKER_TOPOLOGY_AMBIGUOUS",
    "resolve_worker_topology",
    "credential_availability", "build_attention_universe", "tickers_for_tier",
    "budget_verdict", "collect_stage2", "collect_news_rss",
    "collection_safety_contract", "build_source_runtime_health",
    "run_collection_iteration", "read_iteration_history",
    "load_information_collection",
]
