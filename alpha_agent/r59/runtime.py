r"""alpha_agent.r59.runtime - the ONE persistent autonomous research runtime.

R59 restored the autonomous loop but left its LIFETIME bound to an interactive
Claude Code session: the loop only ever ran inside
``scripts/run_r59_autonomous_engine.py``, invoked by hand, so when the session
ended the researcher ended with it. The state survived - the queue and the
memory are SQLite - but nothing ever started them again. That is the whole
defect this module closes.

WHAT IT OWNS: process lifecycle. Acquire single-worker ownership, load the
persisted state, run the autonomous loop, heartbeat, decide between working
and sleeping, recover, stop cleanly, resume exactly.

WHAT IT DOES NOT OWN, and must never re-implement:

    hypothesis mathematics      alpha_agent.r39 / r57 kernels
    research economics          alpha_agent.r59.engines
    queue semantics             alpha_agent.autonomous_research (Stage 8)
    graveyard / burden          alpha_agent.r59.memory
    what to research next       alpha_agent.r59.governor
    alpha qualification         alpha_agent.r59.engines gate
    forward evidence            alpha_agent.r52.runtime (called, not copied)
    portfolio decisions         the operational decision owner, which is NOT
                                importable from here and never will be

There is no second scheduler, no second lock subsystem and no second timing
authority: the worker lease is :mod:`alpha_agent.r46.runlock` (extended with a
heartbeat, because a lease that only records an acquisition time evicts a
healthy long-running worker), and forward-evidence maturation is R52's
``research_runtime_cycle`` called verbatim.

RESEARCH ONLY. Nothing reachable from here can create an order or a fill,
enable a broker, promote a model, activate a sleeve, approve a proposal or
write an operational store. A forward-confirmed challenger raises
``CHALLENGER_WARRANTS_GOVERNED_REVIEW`` for a human and stops there.
"""
from __future__ import annotations

import os
import signal
import socket
import time
import uuid
from pathlib import Path
from typing import Optional

from .. import r59
from ..r46 import runlock as RL
from . import frontier as FR
from . import governor as GOV
from . import loop as LP
from . import memory as M
from . import steele as ST

CALCULATION_OWNER = "alpha_agent.r59.runtime"

RUNTIME_SUBDIR = "runtime"
WORKER_LEASE_NAME = "r59_research_worker.lease"
STATUS_ARTIFACT = "runtime_status.json"
RUN_LOG = "autonomous_runtime.log"

#: A lease whose heartbeat is older than this is abandoned and may be taken
#: over. It must be comfortably longer than the heartbeat interval AND than
#: the longest single research iteration, or a healthy worker deep in a drain
#: would be declared dead by its own replacement.
LEASE_STALE_SECONDS = 1800.0
HEARTBEAT_SECONDS = 60.0

#: Sleep bounds. The floor stops a pathological wake-work-sleep spin; the
#: ceiling guarantees the runtime re-examines the world at least hourly even
#: if every watched source looks quiet.
MIN_SLEEP_SECONDS = 60.0
MAX_SLEEP_SECONDS = 3600.0

#: Optional deployment pin. A deployment path is a property of the DEPLOYMENT,
#: not of the research package, so it is read from the environment and never
#: written here: an R59 module that names the live checkout has already
#: coupled research to one machine's layout.
DEPLOYED_ROOT_ENV = "PAPER_TRADER_DEPLOYED_ROOT"

# --- worker states --------------------------------------------------------- #
W_STARTING = "STARTING"
W_RESEARCHING = "RESEARCHING"
W_MATURING = "MATURING_FORWARD_EVIDENCE"
W_SLEEPING = "SLEEPING"
W_STOPPED = "STOPPED"
W_REFUSED = "REFUSED_ANOTHER_WORKER_HOLDS_THE_LEASE"
W_LEASE_LOST = "STOPPED_LEASE_LOST"

WORKER_STATES = (W_STARTING, W_RESEARCHING, W_MATURING, W_SLEEPING,
                 W_STOPPED, W_REFUSED, W_LEASE_LOST)

#: The ONE signal a forward-confirmed challenger may raise. It is addressed to
#: a human governance review and is not an instruction to anything.
CHALLENGER_REVIEW = "CHALLENGER_WARRANTS_GOVERNED_REVIEW"


# --------------------------------------------------------------------------- #
# Paths and identity
# --------------------------------------------------------------------------- #
def runtime_dir() -> Path:
    d = r59.research_root() / RUNTIME_SUBDIR
    d.mkdir(parents=True, exist_ok=True)
    return d


def lease_path() -> Path:
    return runtime_dir() / WORKER_LEASE_NAME


def _close(queue) -> None:
    """Release a queue this call opened. The Stage-8 queue is connection-per-
    operation, so ``close`` is optional there and must stay optional here."""
    closer = getattr(queue, "close", None)
    if callable(closer):
        closer()


def source_identity(reader=None) -> dict:
    """Which checkout this worker is running, and whether it is committed.

    A pid is not an identity. Two workers on one machine can differ only in
    the code they loaded, and the difference that matters most - committed
    versus uncommitted - is invisible from the process table.

    ``reader`` is INJECTED rather than imported: the canonical revision
    reader lives in the application's api layer, and an R59 research module
    that imports it makes the research package depend on the application.
    The entrypoint, which is allowed to know about both, supplies it. With
    no reader the revision is simply UNRESOLVED, and every decision that
    depends on it fails closed.
    """
    repo = Path(__file__).resolve().parents[2]
    row = {"repo_root": str(repo), "release": r59.RELEASE, "commit": None,
           "commit_short": None, "branch": None, "dirty": None,
           "resolved_from": "UNRESOLVED"}
    if reader is not None:
        try:
            src = reader(repo_root=repo) or {}
            row.update({"commit": src.get("commit"),
                        "commit_short": src.get("commit_short"),
                        "branch": src.get("branch"),
                        "dirty": src.get("dirty"),
                        "resolved_from": src.get("resolved_from")
                        or "UNRESOLVED"})
        except Exception:                                # noqa: BLE001
            pass
    pin = os.environ.get(DEPLOYED_ROOT_ENV)
    row["deployment_pin"] = pin
    row["is_deployed_source"] = (None if not pin
                                 else Path(pin) == repo)
    return row


def worker_identity(instance_id: Optional[str] = None, *, reader=None) -> dict:
    return {
        "instance_id": instance_id or uuid.uuid4().hex,
        "pid": os.getpid(),
        "host": socket.gethostname(),
        "started_at": r59.now_iso(),
        "owner": CALCULATION_OWNER,
        "source": source_identity(reader),
    }


def maturation_policy(identity: Optional[dict] = None) -> dict:
    """May this worker advance PROSPECTIVE evidence?

    Only a source that is KNOWN to be committed and clean may. Forward
    evidence is the one thing in this estate that cannot be recomputed
    later - it is a record of what was known at a moment - so writing it
    from an uncommitted worktree would permanently attribute rows to code
    that never existed in the history.

    Note what is NOT the test: a path. The question is whether the code is
    committed, and an unresolved answer counts as no. Historical research
    continues either way; only prospective writes are gated.
    """
    ident = identity or worker_identity()
    src = ident.get("source") or {}
    if not src.get("commit") or src.get("dirty") is None:
        return {"allowed": False,
                "reason": "SOURCE_REVISION_UNRESOLVED",
                "detail": "the running revision could not be established "
                          "(%s); prospective evidence is refused rather than "
                          "attributed to unknown code"
                          % src.get("resolved_from")}
    if src.get("dirty"):
        return {"allowed": False,
                "reason": "SOURCE_HAS_UNCOMMITTED_CHANGES",
                "detail": "the checkout at %s has uncommitted changes; "
                          "prospective evidence is refused until it is clean"
                          % src.get("repo_root")}
    if src.get("is_deployed_source") is False:
        return {"allowed": False,
                "reason": "SOURCE_IS_NOT_THE_DEPLOYED_CHECKOUT",
                "detail": "running from %s but %s pins the deployment to %s"
                          % (src.get("repo_root"), DEPLOYED_ROOT_ENV,
                             src.get("deployment_pin"))}
    return {"allowed": True, "reason": "COMMITTED_CLEAN_SOURCE",
            "detail": None}


# --------------------------------------------------------------------------- #
# Status read model (section L). No dashboard - one JSON document.
# --------------------------------------------------------------------------- #
def read_status() -> dict:
    return r59.read_json(runtime_dir() / STATUS_ARTIFACT) or {}


def write_status(body: dict) -> Path:
    return r59.write_artifact(STATUS_ARTIFACT, body, subdir=RUNTIME_SUBDIR)


def _log(row: dict) -> None:
    try:
        import json
        p = runtime_dir() / RUN_LOG
        if p.exists() and p.stat().st_size > 4 * 1024 * 1024:
            prev = p.with_suffix(".log.1")
            if prev.exists():
                prev.unlink()
            os.replace(p, prev)
        with p.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, default=str)[:4000] + "\n")
    except OSError:
        pass


def status(mem: Optional[M.ResearchMemory] = None,
           queue=None) -> dict:
    """Read-only runtime status. Safe to call while a worker is running."""
    mem = mem or M.open_memory()
    close_queue = queue is None
    queue = queue or LP.open_queue()
    try:
        counts = queue.counts_by_state()
        burden = mem.burden()
        summary = mem.summary()
        persisted = read_status()
        lease = RL.state_path(lease_path())
        return {
            "calculation_owner": CALCULATION_OWNER,
            "observed_at": r59.now_iso(),
            "worker_state": persisted.get("worker_state") or "NEVER_STARTED",
            "worker_identity": persisted.get("worker_identity"),
            "source_identity": persisted.get("source_identity"),
            "started_at": persisted.get("started_at"),
            "last_heartbeat": lease.get("heartbeat_at_utc")
            or persisted.get("last_heartbeat"),
            "lease": lease,
            "lease_is_live": bool(lease.get("held")
                                  and lease.get("pid_alive") is not False),
            "current_lane": persisted.get("current_lane"),
            "queue_ready": int(counts.get("QUEUED", 0)
                               + counts.get("RETRYABLE", 0)),
            "queue_runnable_now": queue.runnable_depth(),
            "queue_running": int(counts.get("RUNNING", 0)),
            "queue_blocked": int(counts.get("BLOCKED_SPECIFIC", 0)),
            "queue_completed": int(counts.get("COMPLETED", 0)),
            "cumulative_hypotheses": summary.get("n_hypotheses")
            or sum((summary.get("by_outcome") or {}).values()),
            "cumulative_search_burden": burden.get("total"),
            "distinct_families": burden.get("distinct_families"),
            "last_completed_experiment": persisted.get(
                "last_completed_experiment"),
            "last_challenger_freeze": persisted.get("last_challenger_freeze"),
            "stop_or_sleep_reason": persisted.get("stop_or_sleep_reason"),
            "next_planned_wake": persisted.get("next_planned_wake"),
            "data_frontier": persisted.get("data_frontier"),
            "capacity": persisted.get("capacity"),
            "maturation": persisted.get("maturation"),
            "latest_error": persisted.get("latest_error"),
            "status_path": str(runtime_dir() / STATUS_ARTIFACT),
            "safety": dict(r59.SAFETY),
        }
    finally:
        if close_queue:
            _close(queue)


# --------------------------------------------------------------------------- #
# Wake conditions (section H). Cheap, read-only watermarks.
# --------------------------------------------------------------------------- #
COLLECTION_ROOT = Path(r"D:\Stock_Prediction_app_data\information_collection")
INGESTION_ROOT = Path(r"D:\Stock_Prediction_app_data\alpha_agent\ingestion")
R52_ROOT = Path(r"D:\Stock_Prediction_app_data\research_runtime_r52")


def _mtime_mark(path: Path) -> Optional[str]:
    try:
        st = path.stat()
        return "%d:%d" % (int(st.st_mtime), int(st.st_size))
    except OSError:
        return None


def _newest_child_mark(path: Path) -> Optional[str]:
    try:
        best = 0.0
        n = 0
        for child in path.iterdir():
            n += 1
            try:
                best = max(best, child.stat().st_mtime)
            except OSError:
                continue
        return "%d:%d" % (int(best), n)
    except OSError:
        return None


def wake_conditions(mem: Optional[M.ResearchMemory] = None,
                    queue=None) -> list:
    """Everything that could make more research possible than a moment ago.

    Each condition reports a WATERMARK. A watermark that differs from the one
    recorded at the last check means new information arrived; the runtime does
    not have to guess, and it does not poll anything expensive.
    """
    mem = mem or M.open_memory()
    out: list = []

    if queue is not None:
        out.append({"name": "QUEUE_RUNNABLE", "kind": "WORK",
                    "watermark": str(queue.runnable_depth()),
                    "detail": "jobs claimable right now"})
        blocked = queue.blocked_jobs(limit=50)
        out.append({"name": "BLOCKED_SOURCES", "kind": "EXTERNAL",
                    "watermark": str(len(blocked)),
                    "detail": "jobs held on a named external condition"})

    out.append({"name": "MARKET_DATA_PANEL", "kind": "MARKET_SESSION",
                "watermark": _mtime_mark(r59.FUTURES_PANEL_META),
                "detail": str(r59.FUTURES_PANEL_META)})
    out.append({"name": "EQUITY_PANEL", "kind": "MARKET_SESSION",
                "watermark": _mtime_mark(r59.EQUITY_PANEL_META),
                "detail": str(r59.EQUITY_PANEL_META)})
    out.append({"name": "EODHD_COLLECTION", "kind": "COLLECTION",
                "watermark": _mtime_mark(
                    COLLECTION_ROOT / "collection_iteration_history.json"),
                "detail": "continuous information-collection progress"})
    out.append({"name": "INGESTION_STORE", "kind": "COLLECTION",
                "watermark": _newest_child_mark(INGESTION_ROOT),
                "detail": "collector output (macro vintages, filings, news)"})
    out.append({"name": "FORM4_HISTORY", "kind": "EVENT",
                "watermark": _newest_child_mark(r59.FORM4_RAW_DIR),
                "detail": "insider-transaction sessions accumulated"})
    out.append({"name": "FORWARD_EVIDENCE", "kind": "PROSPECTIVE",
                "watermark": _mtime_mark(R52_ROOT / "runtime_health.json"),
                "detail": "R46/R52 prospective tournament state"})

    sample = None
    try:
        sample = ST.sample_path()
    except Exception:                                    # noqa: BLE001
        sample = None
    out.append({"name": "STEELE_ANALYST_SAMPLE", "kind": "DATA_OPPORTUNITY",
                "watermark": ("PRESENT" if (sample and Path(sample).exists())
                              else "ABSENT"),
                "detail": "sample gate is armed; nothing is purchased"})

    opp_state = sorted((o.get("opportunity_id"), o.get("state"))
                       for o in mem.opportunities())
    out.append({"name": "DATA_OPPORTUNITY_FRONTIER", "kind": "DATA_OPPORTUNITY",
                "watermark": r59.short_hash(opp_state, 16),
                "detail": "%d opportunities tracked" % len(opp_state)})
    return out


def wake_delta(previous: Optional[list], current: list) -> dict:
    """Which watched sources moved since the last check."""
    prev = {c["name"]: c.get("watermark") for c in (previous or [])}
    changed = [c["name"] for c in current
               if c["name"] in prev and prev[c["name"]] != c.get("watermark")]
    unseen = [c["name"] for c in current if c["name"] not in prev]
    return {"changed": changed, "first_seen": unseen,
            "any_change": bool(changed)}


def plan_sleep(*, ready_work: int, conditions: list,
               max_sleep: float = MAX_SLEEP_SECONDS) -> dict:
    """How long to sleep, and why. READY work always beats sleeping.

    The project rule is that usable time is never left idle, and the
    distinction that makes it operable is between "nothing to do right now"
    and "nothing until new data exists". Only the second one may sleep.
    """
    if ready_work > 0:
        return {"sleep_seconds": 0.0, "state": W_RESEARCHING,
                "reason": "EXECUTABLE_RESEARCH_EXISTS",
                "detail": "%d job(s) claimable now" % ready_work}
    blocked = next((c for c in conditions
                    if c["name"] == "BLOCKED_SOURCES"), None)
    if blocked and str(blocked.get("watermark") or "0") != "0":
        return {"sleep_seconds": float(MIN_SLEEP_SECONDS), "state": W_SLEEPING,
                "reason": "WAITING_ON_A_BLOCKED_EXTERNAL_SOURCE",
                "detail": "%s job(s) blocked on a named external condition"
                          % blocked.get("watermark")}
    return {"sleep_seconds": float(max(MIN_SLEEP_SECONDS, max_sleep)),
            "state": W_SLEEPING,
            "reason": "ONLY_FUTURE_DATA_CAN_ADVANCE_THE_STATE",
            "detail": "no executable research and no mandate the governor can "
                      "issue from the current information set"}


# --------------------------------------------------------------------------- #
# The persistent runtime
# --------------------------------------------------------------------------- #
class _Stopper:
    """Cooperative shutdown. A signal never interrupts a research iteration."""

    def __init__(self):
        self.requested = False
        self.reason = None
        self._prev = {}

    def install(self) -> None:
        for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None),
                    getattr(signal, "SIGBREAK", None)):
            if sig is None:
                continue
            try:
                self._prev[sig] = signal.signal(sig, self._handle)
            except (ValueError, OSError, RuntimeError):
                continue

    def restore(self) -> None:
        for sig, prev in self._prev.items():
            try:
                signal.signal(sig, prev)
            except (ValueError, OSError, RuntimeError):
                continue

    def request(self, reason: str) -> None:
        self.requested = True
        self.reason = self.reason or reason

    def _handle(self, signum, frame):                    # noqa: ARG002
        self.request("SIGNAL_%s" % signum)


def run_forever(*, mem: Optional[M.ResearchMemory] = None,
                queue=None,
                batch: int = 12,
                max_jobs_per_iteration: int = 8,
                allow_maturation: Optional[bool] = None,
                identity_reader=None,
                debug_max_seconds: Optional[float] = None,
                debug_max_cycles: Optional[int] = None,
                sleep_fn=time.sleep,
                install_signal_handlers: bool = True) -> dict:
    """Run the autonomous researcher until asked to stop.

    PRODUCTION HAS NO ITERATION LIMIT. ``debug_max_seconds`` and
    ``debug_max_cycles`` default to None and exist only for the engineering
    smoke; when either binds it is reported as ``operator_override`` so a
    debugging run can never be mistaken for a research conclusion.

    One cycle is: heartbeat -> research until exhausted -> mature forward
    evidence if allowed -> measure the world -> work again or sleep. The
    research stage is :func:`alpha_agent.r59.loop.run_session` with NO caps;
    it returns only when the governor is out of mandates or an external
    blocker holds every path.
    """
    started = time.monotonic()
    mem = mem or M.open_memory()
    close_queue = queue is None
    queue = queue or LP.open_queue()

    identity = worker_identity(reader=identity_reader)
    maturation = maturation_policy(identity)
    if allow_maturation is not None:
        maturation = dict(maturation)
        maturation["allowed"] = bool(allow_maturation) and maturation["allowed"]
        if not allow_maturation:
            maturation["reason"] = "DISABLED_BY_OPERATOR"

    holder = "r59_research_worker:%s" % identity["instance_id"]
    stopper = _Stopper()

    # --- single-worker safety: fail CLOSED against a healthy holder --------- #
    try:
        lease = RL.acquire_path(lease_path(), holder, wait_s=0,
                                stale_after_s=LEASE_STALE_SECONDS,
                                extra=identity)
    except RL.AdvanceLockBusy as exc:
        held = RL.state_path(lease_path())
        body = {"calculation_owner": CALCULATION_OWNER,
                "worker_state": W_REFUSED,
                "refused_because": str(exc)[:400],
                "existing_lease": held,
                "started_at": identity["started_at"],
                "worker_identity": identity,
                "safety": dict(r59.SAFETY)}
        _log({"event": "start_refused", "holder": held.get("holder")})
        if close_queue:
            _close(queue)
        return body

    if install_signal_handlers:
        stopper.install()

    cycles: list = []
    last_conditions: Optional[list] = None
    last_experiment = None
    last_freeze = None
    latest_error = None
    state = W_STARTING
    sleep_plan = {"reason": "STARTING", "sleep_seconds": 0.0}

    def _beat(lane: Optional[str] = None, *, worker_state: str = None,
              extra: dict = None) -> bool:
        """Refresh the lease and republish status. False = ownership lost."""
        alive = RL.heartbeat_path(lease_path(), holder,
                                  {"last_lane": lane} if lane else None)
        body = {
            "calculation_owner": CALCULATION_OWNER,
            "worker_state": worker_state or state,
            "worker_identity": identity,
            "source_identity": identity["source"],
            "started_at": identity["started_at"],
            "last_heartbeat": r59.now_iso(),
            "current_lane": lane,
            "lease_held": bool(alive),
            "maturation": maturation,
            "last_completed_experiment": last_experiment,
            "last_challenger_freeze": last_freeze,
            "stop_or_sleep_reason": sleep_plan.get("reason"),
            "next_planned_wake": sleep_plan.get("next_wake"),
            "latest_error": latest_error,
            "cycles_completed": len(cycles),
            "production_iteration_limit": None,
            "safety": dict(r59.SAFETY),
        }
        if extra:
            body.update(extra)
        try:
            write_status(body)
        except OSError:
            pass
        return bool(alive)

    def _progress(iteration: dict) -> bool:
        """Called by the research loop between iterations.

        This is also where an OPERATOR debug clock binds. It has to be here
        rather than only at the cycle boundary, because one research cycle
        has no cap by design and can legitimately run for hours - an
        engineering smoke that could only stop between cycles would not be
        an engineering smoke. Production passes None and this never fires.
        """
        nonlocal last_experiment
        last_experiment = {
            "iteration": iteration.get("iteration"),
            "jobs_completed": iteration.get("jobs_completed"),
            "hypotheses_measured": iteration.get("hypotheses_measured"),
            "at": r59.now_iso()}
        if not _beat("r59.research", worker_state=W_RESEARCHING):
            stopper.request("LEASE_LOST")
            return False
        if debug_max_seconds is not None and \
                (time.monotonic() - started) >= float(debug_max_seconds):
            stopper.request("OPERATOR_DEBUG_TIME_LIMIT")
            return False
        return not stopper.requested

    try:
        _beat(worker_state=W_STARTING)
        cycle = 0
        while True:
            cycle += 1
            if stopper.requested:
                break
            if debug_max_cycles is not None and cycle > int(debug_max_cycles):
                sleep_plan = {"reason": "OPERATOR_DEBUG_CYCLE_LIMIT",
                              "sleep_seconds": 0.0, "operator_override": True}
                break
            if debug_max_seconds is not None and \
                    (time.monotonic() - started) >= float(debug_max_seconds):
                sleep_plan = {"reason": "OPERATOR_DEBUG_TIME_LIMIT",
                              "sleep_seconds": 0.0, "operator_override": True}
                break

            # ---- 1. RESEARCH. No cap; returns when genuinely out of work --- #
            state = W_RESEARCHING
            _beat("r59.research", worker_state=state)
            session = None
            try:
                session = LP.run_session(
                    mem=mem, queue=queue, batch=batch,
                    max_jobs_per_iteration=max_jobs_per_iteration,
                    on_progress=_progress)
            except Exception as exc:                     # noqa: BLE001
                latest_error = "%s: %s" % (type(exc).__name__, str(exc)[:300])
                _log({"event": "research_failed", "error": latest_error})

            # ---- 2. FORWARD EVIDENCE. Reused, never re-implemented --------- #
            mat_result = {"state": "SKIPPED", "reason": maturation["reason"]}
            if maturation.get("allowed") and not stopper.requested:
                state = W_MATURING
                _beat("r52.forward_evidence", worker_state=state)
                mat_result = _mature_forward_evidence()
                if mat_result.get("challenger_review"):
                    last_freeze = mat_result.get("challenger_review")

            # ---- 3. MEASURE THE WORLD, then work again or sleep ------------ #
            conditions = wake_conditions(mem, queue)
            delta = wake_delta(last_conditions, conditions)
            last_conditions = conditions
            runnable = queue.runnable_depth()
            if runnable == 0:
                # A drained queue is not an empty frontier: ask the governor.
                try:
                    decision = GOV.stop_reason(mem)
                    if not decision.get("stop"):
                        runnable = int(decision.get("next_mandates") or 0)
                except Exception as exc:                 # noqa: BLE001
                    latest_error = "%s: %s" % (type(exc).__name__,
                                               str(exc)[:300])

            sleep_plan = plan_sleep(ready_work=runnable, conditions=conditions)
            cycles.append({
                "cycle": cycle,
                "stop_condition": (session or {}).get("stop_condition"),
                "jobs_executed": (session or {}).get("jobs_executed"),
                "hypotheses_measured": (session or {}).get(
                    "hypotheses_measured"),
                "still_ready": (session or {}).get("research_still_ready"),
                "maturation": mat_result.get("state"),
                "watched_sources_changed": delta["changed"],
                "sleep_reason": sleep_plan["reason"],
                "sleep_seconds": sleep_plan["sleep_seconds"],
            })
            _log({"event": "cycle", **cycles[-1]})

            if stopper.requested:
                break
            if sleep_plan["sleep_seconds"] <= 0:
                continue

            # ---- 4. SLEEP in heartbeat slices, waking on change ------------ #
            state = W_SLEEPING
            sleep_plan["next_wake"] = _iso_in(sleep_plan["sleep_seconds"])
            _beat(None, worker_state=state)
            woke_early = _sleep_watching(
                sleep_plan["sleep_seconds"], stopper=stopper, mem=mem,
                queue=queue, baseline=conditions, beat=_beat,
                sleep_fn=sleep_fn)
            if woke_early:
                _log({"event": "woke_early", "changed": woke_early})
    finally:
        stopper.restore()
        final_state = (W_LEASE_LOST if stopper.reason == "LEASE_LOST"
                       else W_STOPPED)
        state = final_state
        _beat(None, worker_state=final_state)
        RL.release_path(lease_path(), holder)
        if close_queue:
            _close(queue)

    return {
        "calculation_owner": CALCULATION_OWNER,
        "worker_state": state,
        "worker_identity": identity,
        "stopped_because": stopper.reason or sleep_plan.get("reason"),
        "operator_override": bool(sleep_plan.get("operator_override")),
        "production_iteration_limit": None,
        "cycles": cycles,
        "n_cycles": len(cycles),
        "elapsed_seconds": round(time.monotonic() - started, 1),
        "maturation": maturation,
        "latest_error": latest_error,
        "resumable": True,
        "safety": dict(r59.SAFETY),
    }


def _iso_in(seconds: float) -> str:
    import datetime as _dt
    return (_dt.datetime.now(_dt.timezone.utc)
            + _dt.timedelta(seconds=float(seconds))).replace(
                microsecond=0).isoformat()


def _sleep_watching(seconds: float, *, stopper, mem, queue, baseline, beat,
                    sleep_fn) -> list:
    """Sleep in heartbeat slices; return early when a watched source moves."""
    deadline = time.monotonic() + float(seconds)
    while time.monotonic() < deadline:
        if stopper.requested:
            return []
        slice_s = min(HEARTBEAT_SECONDS, max(0.0, deadline - time.monotonic()))
        if slice_s > 0:
            sleep_fn(slice_s)
        if stopper.requested:
            return []
        if not beat(None, worker_state=W_SLEEPING):
            stopper.request("LEASE_LOST")
            return []
        now = wake_conditions(mem, queue)
        delta = wake_delta(baseline, now)
        if delta["any_change"]:
            return delta["changed"]
    return []


def _mature_forward_evidence() -> dict:
    """Advance prospective evidence through the CANONICAL R52 owner.

    This function contains no timing rule, no scoring and no emission policy.
    It calls ``research_runtime_cycle`` - the same entrypoint the scheduled
    task has always called - and translates its result. A challenger that
    reaches a confirming observation raises a REVIEW signal for a human and
    changes nothing else: no promotion, no holding, no proposal, no order.
    """
    try:
        from ..r52 import runtime as R52
    except Exception as exc:                             # noqa: BLE001
        return {"state": "UNAVAILABLE",
                "reason": "%s: %s" % (type(exc).__name__, str(exc)[:200])}
    try:
        body = R52.research_runtime_cycle(trigger="R59_PERSISTENT_RUNTIME")
    except Exception as exc:                             # noqa: BLE001
        return {"state": "FAILED",
                "reason": "%s: %s" % (type(exc).__name__, str(exc)[:200])}
    adv = body.get("advance") or {}
    ready = body.get("promotion_ready_count")
    out = {"state": body.get("state"),
           "run_id": body.get("run_id"),
           "outcomes_scored": adv.get("tournament_outcomes_scored"),
           "forward_evidence_count": adv.get(
               "tournament_forward_evidence_count"),
           "promotion_ready_count": ready,
           "promotes_model": False,
           "mutates_holdings": False}
    if ready:
        out["challenger_review"] = {
            "signal": CHALLENGER_REVIEW,
            "promotion_ready_count": ready,
            "at": r59.now_iso(),
            "note": "a human governance review is required; this runtime "
                    "cannot promote, activate, approve or order"}
    return out


__all__ = ["CALCULATION_OWNER", "WORKER_LEASE_NAME", "STATUS_ARTIFACT",
           "LEASE_STALE_SECONDS", "HEARTBEAT_SECONDS", "MIN_SLEEP_SECONDS",
           "MAX_SLEEP_SECONDS", "DEPLOYED_ROOT_ENV", "WORKER_STATES",
           "W_STARTING", "W_RESEARCHING", "W_MATURING", "W_SLEEPING",
           "W_STOPPED", "W_REFUSED", "W_LEASE_LOST", "CHALLENGER_REVIEW",
           "runtime_dir", "lease_path", "source_identity", "worker_identity",
           "maturation_policy", "read_status", "write_status", "status",
           "wake_conditions", "wake_delta", "plan_sleep", "run_forever"]
