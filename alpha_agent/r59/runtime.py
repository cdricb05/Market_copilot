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

#: R64 - the floor no CONFIGURATION may go below. ``MIN_SLEEP_SECONDS`` is a
#: policy an operator may tune; this is a safety property and they may not. A
#: zero or negative idle interval - from an environment override, a future
#: config file or an arithmetic slip in an upstream owner - is the exact input
#: that turns a scheduler into a busy-wait, and R64 measured what that costs:
#: 4,829 consecutive cycles, every one of them a zero-second sleep.
ABSOLUTE_MIN_SLEEP_SECONDS = 5.0

#: Operator overrides for the idle policy, read from the environment for the
#: same reason ``DEPLOYED_ROOT_ENV`` is: a cadence is a property of the
#: DEPLOYMENT, not of the research package.
MIN_SLEEP_ENV = "PAPER_TRADER_R59_MIN_SLEEP_SECONDS"
MAX_SLEEP_ENV = "PAPER_TRADER_R59_MAX_SLEEP_SECONDS"

#: R64 - how an UNCHANGED blocker is paced. Re-asking a question the estate has
#: already asked N times, and been refused N times, is not research; it is
#: polling. The wait doubles per consecutive unproductive cycle whose blocked
#: set is byte-identical, and is capped by the idle ceiling, so the runtime
#: still re-examines the world at least hourly.
BACKOFF_MULTIPLIER = 2.0

#: R64 - the belt-and-braces bound. Even if every upstream owner were to
#: regress at once and keep insisting that executable work exists, this many
#: consecutive cycles that executed nothing and measured nothing force a wait.
UNPRODUCTIVE_CYCLES_BEFORE_FORCED_WAIT = 3

#: R64 - a persistent worker may not accumulate one record per cycle forever.
#: The pre-repair worker held 4,829 of them in a list that only ever grew.
MAX_CYCLE_RECORDS = 500

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

#: R61 - a TRUTHFUL waiting state. ``SLEEPING`` says only that the process is
#: not busy; it does not say whether the estate is waiting for a session, for
#: data it does not own, or has genuinely run out of questions. Those need
#: different operator actions, so they get different words.
W_WAITING_MARKET = "WAITING_FOR_MARKET_DATA"
W_WAITING_FORWARD = "WAITING_FOR_FORWARD_EVIDENCE"
W_WAITING_SAMPLE = "WAITING_FOR_EXTERNAL_SAMPLE"
W_FRONTIER_EXHAUSTED = "FRONTIER_EXHAUSTED_UNTIL_NEW_INFORMATION"

WAITING_STATES = (W_WAITING_MARKET, W_WAITING_FORWARD, W_WAITING_SAMPLE,
                  W_FRONTIER_EXHAUSTED)

WORKER_STATES = (W_STARTING, W_RESEARCHING, W_MATURING, W_SLEEPING,
                 W_STOPPED, W_REFUSED, W_LEASE_LOST) + WAITING_STATES

#: The reason published while a research cycle is actually executing jobs. A
#: busy worker is not waiting for anything, and saying so is not the same as
#: saying nothing.
RESEARCH_IN_PROGRESS = "EXECUTING_RESEARCH"

#: How a canonical blocker reason maps to the state the operator should see.
#: One mapping, so the runtime and the read model cannot disagree about what a
#: blocked frontier means.
_WAIT_STATE_BY_BLOCKER: dict[str, str] = {
    "WAITING_FOR_MARKET_SESSION": W_WAITING_MARKET,
    "WAITING_FOR_FORWARD_EVIDENCE": W_WAITING_FORWARD,
    "WAITING_FOR_SAMPLE": W_WAITING_SAMPLE,
    "WAITING_FOR_EXTERNAL_ENTITLEMENT": W_WAITING_SAMPLE,
    "WAITING_FOR_PROVIDER_DATA": W_WAITING_MARKET,
    "FAMILY_EXHAUSTED": W_FRONTIER_EXHAUSTED,
    "DEPENDENCY_BLOCKED": W_FRONTIER_EXHAUSTED,
    "COMPUTE_GATE": W_SLEEPING,
    "INVALIDATED": W_FRONTIER_EXHAUSTED,
    "SUPERSEDED": W_FRONTIER_EXHAUSTED,
    "UNCLASSIFIED_BLOCKER": W_SLEEPING,
}


def waiting_state_for(blocker_reason: Optional[str]) -> str:
    """The worker state that truthfully describes waiting on this blocker."""
    return _WAIT_STATE_BY_BLOCKER.get(str(blocker_reason or ""),
                                      W_FRONTIER_EXHAUSTED)

#: The ONE signal a forward-confirmed challenger may raise. It is addressed to
#: a human governance review and is not an instruction to anything.
CHALLENGER_REVIEW = "CHALLENGER_WARRANTS_GOVERNED_REVIEW"


# --------------------------------------------------------------------------- #
# Paths and identity
# --------------------------------------------------------------------------- #
def runtime_dir(*, create: bool = True) -> Path:
    """The runtime artifact directory.

    ``create=False`` (R60) is how a READ path asks where the artifacts are
    without bringing the directory into existence. A GET that creates a
    research directory as a side effect is not a read.
    """
    d = r59.research_root() / RUNTIME_SUBDIR
    if create:
        d.mkdir(parents=True, exist_ok=True)
    return d


def lease_path(*, create: bool = True) -> Path:
    return runtime_dir(create=create) / WORKER_LEASE_NAME


def status_path(*, create: bool = True) -> Path:
    return runtime_dir(create=create) / STATUS_ARTIFACT


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
    return r59.read_json(status_path(create=False)) or {}


def write_status(body: dict) -> Path:
    return r59.write_artifact(STATUS_ARTIFACT, body, subdir=RUNTIME_SUBDIR)


def _log(row: dict) -> None:
    try:
        import json
        p = runtime_dir(create=True) / RUN_LOG
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
           queue=None, *, read_only: bool = False) -> dict:
    """Read-only runtime status. Safe to call while a worker is running.

    ``read_only`` (R60) makes the STORE ACCESS read-only too, not merely the
    intent: the memory and the queue are opened as observer handles and the
    lease path is resolved without creating the runtime directory. Without it
    this function still opened writer handles, so asking for status ran the
    schema scripts of both SQLite stores it was reporting on.
    """
    mem = mem or (M.open_memory_readonly() if read_only else M.open_memory())
    close_queue = queue is None
    queue = queue or LP.open_queue(read_only=read_only)
    try:
        counts = queue.counts_by_state()
        burden = mem.burden()
        summary = mem.summary()
        persisted = read_status()
        lease = RL.state_path(lease_path(create=not read_only))
        # ``hypotheses_total`` is the memory summary's own name for every
        # registered identity. The earlier key (``n_hypotheses``) has never
        # existed in that contract, so this always fell through to the sum of
        # by_outcome - the SETTLED count - and reported it under a name that
        # promised the total. Both are now reported, each under its own name.
        settled = sum((summary.get("by_outcome") or {}).values())
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
            "cumulative_hypotheses": summary.get("hypotheses_total", settled),
            "cumulative_hypotheses_settled": summary.get(
                "hypotheses_settled", settled),
            "cumulative_search_burden": burden.get("total"),
            "distinct_families": burden.get("distinct_families"),
            "last_completed_experiment": persisted.get(
                "last_completed_experiment"),
            "last_challenger_freeze": persisted.get("last_challenger_freeze"),
            "stop_or_sleep_reason": persisted.get("stop_or_sleep_reason"),
            "next_planned_wake": persisted.get("next_planned_wake"),
            "wake_condition": persisted.get("wake_condition"),
            "blocker_reason": persisted.get("blocker_reason"),
            "blocker_reasons": dict(persisted.get("blocker_reasons") or {}),
            "wait_detail": persisted.get("wait_detail"),
            # R64 - the cadence actually in force, and the pacing state behind
            # it. An operator who can see only "SLEEPING" cannot tell a worker
            # that is waiting from one that is spinning.
            "idle_policy": persisted.get("idle_policy"),
            "effective_sleep_seconds": persisted.get("effective_sleep_seconds"),
            "unproductive_streak": persisted.get("unproductive_streak"),
            "blocker_signature": persisted.get("blocker_signature"),
            "forced_waits": persisted.get("forced_waits"),
            "cycles_completed": persisted.get("cycles_completed"),
            "data_frontier": persisted.get("data_frontier"),
            "capacity": persisted.get("capacity"),
            "maturation": persisted.get("maturation"),
            "latest_error": persisted.get("latest_error"),
            "status_path": str(status_path(create=not read_only)),
            "read_only": bool(read_only),
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


def _blocked_rows(queue) -> list:
    """The blocked jobs as (lane, reason) rows, for the R64 signature. A read;
    never raises, because observability may not break a research cycle."""
    try:
        return [{"lane": j.lane, "reason": j.blocked_reason}
                for j in queue.blocked_jobs(limit=500)]
    except Exception:                                    # noqa: BLE001
        return []


def _blocked_summary(queue) -> dict:
    """The canonical classification of everything the queue currently holds
    blocked (R61). A read; never raises, because observability may not break a
    research cycle."""
    try:
        from . import blockers as BLK
        rows = [BLK.classify_job(j) for j in queue.blocked_jobs(limit=500)]
        return BLK.summarise(rows)
    except Exception:                                    # noqa: BLE001
        return {}


def idle_policy(min_seconds=None, max_seconds=None) -> dict:
    """The EFFECTIVE idle configuration, and everything it had to correct.

    R64. The runtime may be tuned, but it may not be tuned into a busy-wait.
    A requested floor that is zero, negative, non-numeric or simply absurd is
    CLAMPED to :data:`ABSOLUTE_MIN_SLEEP_SECONDS` and the correction is
    reported by name, because a silently corrected configuration is how an
    operator comes to believe a cadence that was never in force.
    """
    notes: list = []

    def _read(explicit, env_name, default, label):
        raw, source = explicit, "ARGUMENT"
        if raw is None:
            raw, source = os.environ.get(env_name), "ENVIRONMENT"
        if raw is None or (isinstance(raw, str) and not str(raw).strip()):
            return float(default), "DEFAULT"
        try:
            return float(raw), source
        except (TypeError, ValueError):
            notes.append("%s_NOT_NUMERIC" % label)
            return float(default), "DEFAULT_AFTER_INVALID"

    requested_min, min_source = _read(min_seconds, MIN_SLEEP_ENV,
                                      MIN_SLEEP_SECONDS, "MIN")
    requested_max, max_source = _read(max_seconds, MAX_SLEEP_ENV,
                                      MAX_SLEEP_SECONDS, "MAX")

    effective_min = requested_min
    if not (effective_min > 0):
        notes.append("MIN_NOT_POSITIVE")
        effective_min = ABSOLUTE_MIN_SLEEP_SECONDS
    elif effective_min < ABSOLUTE_MIN_SLEEP_SECONDS:
        notes.append("MIN_BELOW_SAFE_FLOOR")
        effective_min = ABSOLUTE_MIN_SLEEP_SECONDS

    effective_max = requested_max
    if not (effective_max > 0):
        notes.append("MAX_NOT_POSITIVE")
        effective_max = effective_min
    if effective_max < effective_min:
        notes.append("MAX_BELOW_MIN")
        effective_max = effective_min

    return {"calculation_owner": CALCULATION_OWNER,
            "min_sleep_seconds": float(effective_min),
            "max_sleep_seconds": float(effective_max),
            "absolute_floor_seconds": float(ABSOLUTE_MIN_SLEEP_SECONDS),
            "requested_min_seconds": float(requested_min),
            "requested_max_seconds": float(requested_max),
            "min_source": min_source, "max_source": max_source,
            "corrections": notes, "was_corrected": bool(notes)}


def executable_ready_work(*, session: Optional[dict], runnable_now: int) -> dict:
    """How much research is ACTUALLY executable, on the evidence of the session
    that just ran. The R64 repair, and the one rule this module got wrong.

    The pre-repair runtime asked the GOVERNOR how many mandates it could
    generate and passed that to :func:`plan_sleep` as ``ready_work``. Those are
    not the same quantity and were never interchangeable:

        the governor answers "are there questions worth asking",
        the queue answers  "can anything be claimed right now".

    ``governor.stop_reason`` is a pure function of research MEMORY, and memory
    only changes when work EXECUTES. So once every queued job was held on an
    external blocker the governor kept returning the same two or three
    mandates, the runtime kept reading that as "executable research exists",
    slept zero seconds and started another identical cycle - 4,825 times,
    every one of them stopping on ``B_EXTERNAL_BLOCKER`` and 4,806 of them
    executing nothing at all. R61's carefully-written back-off branches were
    unreachable for the entire life of the worker.

    The session itself is the authority, and it is a trustworthy one:
    :func:`alpha_agent.r59.loop.run_session` re-measures the frontier,
    re-generates mandates and re-seeds the queue after EVERY iteration and
    does not return while anything is claimable. So when it returns:

        ``B_EXTERNAL_BLOCKER``  nothing was claimable and re-seeding did not
                                help. Executable work is ZERO, whatever the
                                governor's opinion of the frontier.
        ``None`` (it raised)    nothing was proven to have executed, and a
                                crash that is retried with no delay is the
                                same busy-wait wearing a different hat.
        anything else           the queue's own claimable depth stands.

    This never delays genuinely executable work: work that is genuinely
    executable is drained INSIDE the session, with no cycle boundary and no
    sleep between items.
    """
    stop = (session or {}).get("stop_condition") if session is not None else None
    runnable = max(0, int(runnable_now or 0))
    if session is None:
        return {"ready_work": 0, "authority": "SESSION_DID_NOT_COMPLETE",
                "stop_condition": None, "queue_runnable": runnable,
                "detail": "the research session raised; nothing was proven to "
                          "have executed and an undelayed retry is a spin"}
    if stop == LP.STOP_B:
        return {"ready_work": 0, "authority": "SESSION_PROVED_EXTERNAL_BLOCKER",
                "stop_condition": stop, "queue_runnable": runnable,
                "detail": "the session re-seeded the frontier and still could "
                          "claim nothing; no mandate the governor can generate "
                          "is executable until the blocker clears"}
    return {"ready_work": runnable, "authority": "QUEUE_CLAIMABLE_DEPTH",
            "stop_condition": stop, "queue_runnable": runnable,
            "detail": "%d job(s) claimable now" % runnable}


def blocker_signature(blocked_summary: Optional[dict] = None,
                      blocked_rows: Optional[list] = None) -> str:
    """A stable fingerprint of WHAT is currently blocked.

    Two consecutive cycles with the same signature saw the same wall. That is
    the only thing that justifies waiting longer the second time, and the only
    thing that must RESET the wait when it changes.
    """
    def _field(row, *names):
        for name in names:
            value = (row.get(name) if isinstance(row, dict)
                     else getattr(row, name, None))
            if value:
                return str(value)
        return ""

    summary = blocked_summary or {}
    # Accepts either the ``_blocked_rows`` mappings or the ``ResearchJob``
    # objects ``blocked_jobs`` yields, so a caller cannot get a different
    # signature for the same wall by reading it through a different handle.
    rows = sorted((_field(r, "reason", "blocked_reason"), _field(r, "lane"))
                  for r in (blocked_rows or []))
    return r59.short_hash({"by_reason": dict(summary.get("by_reason") or {}),
                           "total": summary.get("blocked_total"),
                           "rows": rows}, 16)


def escalate_idle_backoff(*, base_seconds: float, unproductive_streak: int,
                          policy: Optional[dict] = None) -> dict:
    """Bounded exponential back-off over an UNCHANGED blocker (R64).

    The first unproductive cycle waits the planned interval. Each further
    consecutive cycle that executed nothing, measured nothing and saw the same
    blocked set doubles it, up to the idle ceiling. A productive cycle or a
    changed blocker resets the streak, so a frontier that starts moving again
    is not punished for having once been stuck.
    """
    pol = policy or idle_policy()
    floor = float(pol["min_sleep_seconds"])
    ceiling = float(pol["max_sleep_seconds"])
    base = max(float(base_seconds or 0.0), floor)
    steps = max(0, int(unproductive_streak or 0) - 1)
    # Bound the exponent before it is applied; 2**streak on a worker that has
    # been up for a month is an overflow, not a policy.
    steps = min(steps, 32)
    seconds = min(ceiling, base * (BACKOFF_MULTIPLIER ** steps))
    seconds = max(floor, min(ceiling, seconds))
    return {"sleep_seconds": float(seconds), "base_seconds": float(base),
            "streak": int(unproductive_streak or 0), "steps_applied": steps,
            "at_ceiling": bool(seconds >= ceiling),
            "floor_seconds": floor, "ceiling_seconds": ceiling}


def plan_sleep(*, ready_work: int, conditions: list,
               max_sleep: float = MAX_SLEEP_SECONDS,
               blocked_summary: Optional[dict] = None) -> dict:
    """How long to sleep, why, and WHAT WOULD END THE WAIT. READY work always
    beats sleeping.

    The project rule is that usable time is never left idle, and the
    distinction that makes it operable is between "nothing to do right now"
    and "nothing until new data exists". Only the second one may sleep.

    RELEASE 61 - the wait is now NAMED. ``blocked_summary`` is
    ``alpha_agent.r59.blockers.summarise`` over the queue's blocked rows, so
    the state the operator sees ("waiting for market data", "frontier
    exhausted until new information") is derived from the canonical reason the
    blocked work actually carries rather than from the single word "sleeping".
    A blocker only TIME can clear justifies a short re-check; one that time can
    never clear does not, and sleeping the ceiling on it is the honest answer.
    """
    if ready_work > 0:
        return {"sleep_seconds": 0.0, "state": W_RESEARCHING,
                "reason": "EXECUTABLE_RESEARCH_EXISTS",
                "wake_condition": None, "blocker_reasons": {},
                "detail": "%d job(s) claimable now" % ready_work}
    summary = blocked_summary or {}
    by_reason = dict(summary.get("by_reason") or {})
    blocked = next((c for c in conditions
                    if c["name"] == "BLOCKED_SOURCES"), None)
    n_blocked = 0
    try:
        n_blocked = int(str(blocked.get("watermark") or "0")) if blocked else 0
    except (TypeError, ValueError):
        n_blocked = 0
    if by_reason or n_blocked:
        # The DOMINANT canonical reason decides the word; every reason is
        # published beside it so a mixed frontier is never reduced to one.
        dominant = (max(by_reason.items(), key=lambda kv: kv[1])[0]
                    if by_reason else "UNCLASSIFIED_BLOCKER")
        time_clears = int(summary.get("time_will_clear") or 0)
        # A blocker time CAN clear earns a short re-check. So does an
        # UNCLASSIFIED one: the R59 back-off is the safe answer when the estate
        # cannot say what it is waiting for, and sleeping the ceiling on an
        # unknown blocker would trade responsiveness for nothing. Only a
        # classification that says no amount of time will help sleeps long.
        classified = bool(by_reason)
        seconds = (float(MIN_SLEEP_SECONDS)
                   if (time_clears or not classified)
                   else float(max(MIN_SLEEP_SECONDS, max_sleep)))
        return {"sleep_seconds": seconds,
                "state": waiting_state_for(dominant),
                "reason": "WAITING_ON_A_BLOCKED_EXTERNAL_SOURCE",
                "blocker_reason": dominant,
                "blocker_reasons": by_reason,
                "wake_condition": (
                    "AN_ELAPSED_MARKET_SESSION" if time_clears else
                    "A_RE_CHECK_OF_AN_UNCLASSIFIED_BLOCKER" if not classified
                    else "NEW_INFORMATION_OR_AN_OPERATOR_DECISION"),
                "detail": "%s job(s) blocked; dominant canonical reason %s"
                          % (summary.get("blocked_total", n_blocked), dominant)}
    return {"sleep_seconds": float(max(MIN_SLEEP_SECONDS, max_sleep)),
            "state": W_FRONTIER_EXHAUSTED,
            "reason": "ONLY_FUTURE_DATA_CAN_ADVANCE_THE_STATE",
            "blocker_reason": None, "blocker_reasons": {},
            "wake_condition": "NEW_INFORMATION_OR_AN_OPERATOR_DECISION",
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
                adopt_forward=None,
                debug_max_seconds: Optional[float] = None,
                debug_max_cycles: Optional[int] = None,
                sleep_fn=time.sleep,
                clock=time.monotonic,
                min_sleep_seconds: Optional[float] = None,
                max_sleep_seconds: Optional[float] = None,
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

    ``clock`` and ``sleep_fn`` are injectable together so a test can drive the
    wait deterministically. They must agree: a ``sleep_fn`` that does not
    advance ``clock`` describes a machine on which time does not pass, and the
    runtime would wait in it forever.
    """
    started = clock()
    policy = idle_policy(min_sleep_seconds, max_sleep_seconds)
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
    n_cycles_total = 0
    last_conditions: Optional[list] = None
    last_experiment = None
    last_freeze = None
    latest_error = None
    state = W_STARTING
    sleep_plan = {"reason": "STARTING", "sleep_seconds": 0.0}
    # R64 - the pacing state for a repeated, unchanged blocker.
    unproductive_streak = 0
    last_blocker_signature = None
    forced_waits = 0

    def _researching_plan(iteration: Optional[dict] = None) -> dict:
        """R61 — the reason to publish WHILE a research cycle is running.

        ``sleep_plan`` was only recomputed at the END of a cycle, and a research
        cycle has no cap by design. The live worker therefore published
        ``stop_or_sleep_reason: STARTING`` and ``next_planned_wake: null`` for
        its entire life while reporting RESEARCHING - so the one field that says
        what the agent is waiting for said nothing, for hours. A worker that IS
        executing jobs has no wake condition because it is not waiting, and that
        is what this says, with the iteration it is on as the evidence.
        """
        it = iteration or {}
        return {"reason": RESEARCH_IN_PROGRESS, "sleep_seconds": 0.0,
                "next_wake": None,
                "detail": ("executing research; iteration %s, %s job(s) "
                           "completed in the last iteration"
                           % (it.get("iteration"), it.get("jobs_completed")))
                          if it else "executing research"}

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
            # R61 - WHAT would end the wait, and the canonical blocker reasons
            # behind it. A worker that reports RESEARCHING with no wake
            # condition is either busy (and says so) or lying.
            "wake_condition": sleep_plan.get("wake_condition"),
            "blocker_reason": sleep_plan.get("blocker_reason"),
            "blocker_reasons": dict(sleep_plan.get("blocker_reasons") or {}),
            "wait_detail": sleep_plan.get("detail"),
            "latest_error": latest_error,
            "cycles_completed": n_cycles_total,
            "production_iteration_limit": None,
            # R64 - the EFFECTIVE idle configuration and the pacing state. An
            # operator who cannot see the cadence in force cannot tell a
            # healthy waiting worker from the busy-wait this release removed.
            "idle_policy": policy,
            "unproductive_streak": unproductive_streak,
            "blocker_signature": last_blocker_signature,
            "forced_waits": forced_waits,
            "effective_sleep_seconds": sleep_plan.get("sleep_seconds"),
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
        nonlocal last_experiment, sleep_plan
        last_experiment = {
            "iteration": iteration.get("iteration"),
            "jobs_completed": iteration.get("jobs_completed"),
            "hypotheses_measured": iteration.get("hypotheses_measured"),
            "at": r59.now_iso()}
        # R61 - the published reason tracks the work actually in flight.
        sleep_plan = _researching_plan(iteration)
        if not _beat("r59.research", worker_state=W_RESEARCHING):
            stopper.request("LEASE_LOST")
            return False
        if debug_max_seconds is not None and \
                (clock() - started) >= float(debug_max_seconds):
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
                    (clock() - started) >= float(debug_max_seconds):
                sleep_plan = {"reason": "OPERATOR_DEBUG_TIME_LIMIT",
                              "sleep_seconds": 0.0, "operator_override": True}
                break

            # ---- 1. RESEARCH. No cap; returns when genuinely out of work --- #
            state = W_RESEARCHING
            sleep_plan = _researching_plan()
            _beat("r59.research", worker_state=state)
            session = None
            try:
                session = LP.run_session(
                    mem=mem, queue=queue, batch=batch,
                    max_jobs_per_iteration=max_jobs_per_iteration,
                    # R61 - carried through, never imported here.
                    adopt_forward=adopt_forward,
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
            blocked_summary = _blocked_summary(queue)

            # R64 - the SESSION decides whether executable research exists, not
            # the governor's opinion of the frontier. See
            # :func:`executable_ready_work` for the 4,829-cycle reason why.
            verdict = executable_ready_work(session=session,
                                            runnable_now=queue.runnable_depth())
            runnable = verdict["ready_work"]

            # A cycle that executed nothing and measured nothing produced no
            # new information, so nothing it could ask next has changed.
            jobs_executed = int((session or {}).get("jobs_executed") or 0)
            measured = int((session or {}).get("hypotheses_measured") or 0)
            productive = bool(jobs_executed or measured)
            signature = blocker_signature(blocked_summary,
                                          _blocked_rows(queue))
            if productive or signature != last_blocker_signature:
                unproductive_streak = 0 if productive else 1
            else:
                unproductive_streak += 1
            last_blocker_signature = signature

            sleep_plan = plan_sleep(ready_work=runnable, conditions=conditions,
                                    max_sleep=policy["max_sleep_seconds"],
                                    blocked_summary=blocked_summary)
            sleep_plan["ready_work_authority"] = verdict["authority"]
            sleep_plan["queue_runnable"] = verdict["queue_runnable"]

            # R64 - the NONZERO WAIT, enforced here and not only upstream.
            # plan_sleep is the policy owner and this does not second-guess its
            # state, reason or wake condition; it guarantees the one property
            # that an upstream regression took away, namely that a cycle which
            # achieved nothing is never followed immediately by an identical
            # one. Genuinely claimable work still proceeds with no delay.
            forced = (not productive and verdict["queue_runnable"] == 0) or \
                     (unproductive_streak >= UNPRODUCTIVE_CYCLES_BEFORE_FORCED_WAIT)
            if forced and sleep_plan["sleep_seconds"] <= 0:
                sleep_plan["sleep_seconds"] = policy["min_sleep_seconds"]
                sleep_plan["forced_nonzero_wait"] = True
                forced_waits += 1
            if sleep_plan["sleep_seconds"] > 0:
                backoff = escalate_idle_backoff(
                    base_seconds=sleep_plan["sleep_seconds"],
                    unproductive_streak=unproductive_streak, policy=policy)
                sleep_plan["sleep_seconds"] = backoff["sleep_seconds"]
                sleep_plan["backoff"] = backoff

            n_cycles_total += 1
            record = {
                "cycle": cycle,
                "stop_condition": (session or {}).get("stop_condition"),
                "jobs_executed": jobs_executed,
                "hypotheses_measured": measured,
                "still_ready": (session or {}).get("research_still_ready"),
                "maturation": mat_result.get("state"),
                "watched_sources_changed": delta["changed"],
                "sleep_reason": sleep_plan["reason"],
                "sleep_seconds": sleep_plan["sleep_seconds"],
                "ready_work_authority": verdict["authority"],
                "unproductive_streak": unproductive_streak,
                "blocker_signature": signature,
            }
            cycles.append(record)
            # A worker that never ends may not hold a record per cycle forever.
            if len(cycles) > MAX_CYCLE_RECORDS:
                del cycles[:len(cycles) - MAX_CYCLE_RECORDS]
            _log({"event": "cycle", **record})

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
                sleep_fn=sleep_fn, clock=clock,
                # R64 - while the SAME wall stands, an information arrival may
                # not cut the back-off short: that is how a 30-minute collector
                # tick turns an hourly re-examination back into a poll. New
                # claimable WORK always wakes immediately; see _sleep_watching.
                information_dwell_seconds=(sleep_plan["sleep_seconds"]
                                           if unproductive_streak > 1 else 0.0))
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
        # The most recent MAX_CYCLE_RECORDS cycles; n_cycles is the true total,
        # which is why they are separate fields.
        "cycles": cycles,
        "n_cycles": n_cycles_total,
        "elapsed_seconds": round(clock() - started, 1),
        "idle_policy": policy,
        "unproductive_streak": unproductive_streak,
        "forced_waits": forced_waits,
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
                    sleep_fn, clock=time.monotonic,
                    information_dwell_seconds: float = 0.0) -> list:
    """Sleep in heartbeat slices; return early when a watched source moves.

    R64 separates the two kinds of arrival, because they deserve different
    answers while the same blocker stands:

        WORK         new CLAIMABLE work. Always wakes the worker at once -
                     nothing in this release may delay executable research.
        everything   new INFORMATION. Honoured immediately by default, but
        else         held until ``information_dwell_seconds`` has elapsed once
                     the same wall has already refused the estate twice. The
                     continuous collector rewrites its progress file every
                     half hour, and a back-off that any such write could cut
                     short is not a back-off - it is a thirty-minute poll.

    The dwell never loses an arrival: the change is remembered, the worker
    keeps its lease alive throughout, and it acts the moment the dwell ends.
    """
    started = clock()
    deadline = started + float(seconds)
    dwell = max(0.0, float(information_dwell_seconds or 0.0))
    kinds = {c.get("name"): c.get("kind") for c in (baseline or [])}
    pending: list = []
    while clock() < deadline:
        if stopper.requested:
            return []
        slice_s = min(HEARTBEAT_SECONDS, max(0.0, deadline - clock()))
        if slice_s > 0:
            sleep_fn(slice_s)
        if stopper.requested:
            return []
        if not beat(None, worker_state=W_SLEEPING):
            stopper.request("LEASE_LOST")
            return []
        now = wake_conditions(mem, queue)
        delta = wake_delta(baseline, now)
        changed = delta["changed"]
        if changed:
            kinds.update({c.get("name"): c.get("kind") for c in now})
            if any(kinds.get(name) == "WORK" for name in changed):
                return changed
            pending = changed
            if (clock() - started) >= dwell:
                return changed
    return pending


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
           # R61 - truthful waiting vocabulary + the blocker mapping.
           "WAITING_STATES", "W_WAITING_MARKET", "W_WAITING_FORWARD",
           "W_WAITING_SAMPLE", "W_FRONTIER_EXHAUSTED", "RESEARCH_IN_PROGRESS",
           "waiting_state_for",
           "W_STARTING", "W_RESEARCHING", "W_MATURING", "W_SLEEPING",
           "W_STOPPED", "W_REFUSED", "W_LEASE_LOST", "CHALLENGER_REVIEW",
           "runtime_dir", "lease_path", "status_path", "source_identity",
           "worker_identity",
           "maturation_policy", "read_status", "write_status", "status",
           "wake_conditions", "wake_delta", "plan_sleep", "run_forever",
           # R64 - the repaired scheduling contract.
           "ABSOLUTE_MIN_SLEEP_SECONDS", "MIN_SLEEP_ENV", "MAX_SLEEP_ENV",
           "BACKOFF_MULTIPLIER", "UNPRODUCTIVE_CYCLES_BEFORE_FORCED_WAIT",
           "MAX_CYCLE_RECORDS", "idle_policy", "executable_ready_work",
           "blocker_signature", "escalate_idle_backoff"]
