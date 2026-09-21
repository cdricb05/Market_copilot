r"""alpha_agent.r61.workers - THE local research worker registry (Workstream G).

RESEARCH ONLY. READ-ONLY WITH RESPECT TO PROCESSES: nothing here terminates
anything, ever. There is no kill verb, by design.

The problem this owns
---------------------
Local research work degraded the operator's Windows machine because a job that
finished, crashed or was interrupted left no trace that anyone could read. The
operator was left inspecting ``python.exe`` by NAME, which is exactly how an
unrelated process - or Claude Code itself - gets terminated by mistake.

So every heavy local Paper Trader research process registers itself HERE, with

    RUN_ID  WORKER_ID  PID  PARENT_PID  COMMAND  STARTED_AT  LOG_PATH  STATE

and STATE is one of RUNNING / COMPLETED / FAILED / INTERRUPTED.

No duplicate worker state
-------------------------
One WORKER_ID is exactly one file. A worker rewrites its own file and no
other, so two concurrent workers never contend and the registry can never hold
two disagreeing rows for one worker. That is cheaper and safer than a second
SQLite database, and a second database is precisely what this estate refuses.

PID REUSE IS A REAL HAZARD AND IS DEFEATED HERE. Windows recycles process ids
freely, so "is pid 62804 alive" is not the question - "is the process now
holding pid 62804 the SAME process that registered" is. Every row therefore
carries the registering process's own creation time, read from the OS, and
liveness is judged on the (pid, creation time) pair. A row whose pid is alive
but whose creation time differs is reported as INTERRUPTED, never as RUNNING,
because the worker it described is gone.
"""
from __future__ import annotations

import ctypes
import json
import os
import sys
from ctypes import wintypes
from pathlib import Path
from typing import Optional

from . import RUN_ID, WORKER_ROOT, now_iso, write_artifact

REGISTRY_OWNER = "alpha_agent.r61.workers"
REGISTRY_VERSION = "R61_LOCAL_WORKER_REGISTRY_V1"

STATE_RUNNING = "RUNNING"
STATE_COMPLETED = "COMPLETED"
STATE_FAILED = "FAILED"
STATE_INTERRUPTED = "INTERRUPTED"
STATES = (STATE_RUNNING, STATE_COMPLETED, STATE_FAILED, STATE_INTERRUPTED)

#: Terminal states. A worker in one of these is not consuming the machine.
TERMINAL_STATES = (STATE_COMPLETED, STATE_FAILED, STATE_INTERRUPTED)

#: The pre-declared ceiling on concurrent CPU-heavy Paper Trader research
#: jobs. Declared here so a launcher can refuse rather than discover.
MAX_CONCURRENT_HEAVY_JOBS = 2

KIND_HEAVY = "HEAVY"
KIND_LIGHT = "LIGHT"


class WorkerRefusal(RuntimeError):
    """The registry refused to record a worker."""


# --------------------------------------------------------------------------- #
# Process identity - (pid, creation time), never pid alone
# --------------------------------------------------------------------------- #
_PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
_STILL_ACTIVE = 259


def _filetime_to_int(ft) -> int:
    return (int(ft.dwHighDateTime) << 32) | int(ft.dwLowDateTime)


def process_created_at(pid: int) -> Optional[int]:
    """The OS creation time of ``pid`` as a 64-bit FILETIME, or None.

    None means "this process cannot be identified" - either it is gone, or
    this account may not query it. Both are reported as absence; neither is
    reported as a live match, because a row that cannot be identified must
    never be presented as RUNNING.
    """
    if not sys.platform.startswith("win"):
        return None
    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    handle = k32.OpenProcess(_PROCESS_QUERY_LIMITED_INFORMATION, False,
                             int(pid))
    if not handle:
        return None
    try:
        creation = wintypes.FILETIME()
        exit_t = wintypes.FILETIME()
        kernel = wintypes.FILETIME()
        user = wintypes.FILETIME()
        ok = k32.GetProcessTimes(handle, ctypes.byref(creation),
                                 ctypes.byref(exit_t), ctypes.byref(kernel),
                                 ctypes.byref(user))
        if not ok:
            return None
        return _filetime_to_int(creation)
    finally:
        k32.CloseHandle(handle)


def process_is_alive(pid: int) -> bool:
    if not sys.platform.startswith("win"):
        return False
    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    handle = k32.OpenProcess(_PROCESS_QUERY_LIMITED_INFORMATION, False,
                             int(pid))
    if not handle:
        return False
    try:
        code = wintypes.DWORD()
        if not k32.GetExitCodeProcess(handle, ctypes.byref(code)):
            return False
        return int(code.value) == _STILL_ACTIVE
    finally:
        k32.CloseHandle(handle)


def same_process(row: dict) -> bool:
    """Is the process this row describes still the one holding its pid?"""
    pid = row.get("pid")
    if not pid:
        return False
    if not process_is_alive(int(pid)):
        return False
    recorded = row.get("process_created_filetime")
    if recorded is None:
        # An unidentifiable row is never claimed as live.
        return False
    return process_created_at(int(pid)) == int(recorded)


# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #
def run_dir(run_id: str = RUN_ID, root: Optional[Path] = None) -> Path:
    return Path(root or WORKER_ROOT) / str(run_id)


def worker_path(worker_id: str, run_id: str = RUN_ID,
                root: Optional[Path] = None) -> Path:
    safe = "".join(c if (c.isalnum() or c in "-_.") else "_"
                   for c in str(worker_id))
    if not safe:
        raise WorkerRefusal("worker_id must not be empty")
    return run_dir(run_id, root) / ("%s.json" % safe)


# --------------------------------------------------------------------------- #
# Registering
# --------------------------------------------------------------------------- #
def register(worker_id: str, *, command: Optional[list] = None,
             log_path: Optional[str] = None, run_id: str = RUN_ID,
             kind: str = KIND_HEAVY, root: Optional[Path] = None,
             pid: Optional[int] = None) -> dict:
    """Record this process as RUNNING. Returns the written row."""
    pid = int(os.getpid() if pid is None else pid)
    row = {
        "registry_owner": REGISTRY_OWNER,
        "registry_version": REGISTRY_VERSION,
        "run_id": str(run_id),
        "worker_id": str(worker_id),
        "kind": str(kind),
        "pid": pid,
        "parent_pid": int(os.getppid()),
        "process_created_filetime": process_created_at(pid),
        "command": list(command if command is not None else sys.argv),
        "started_at": now_iso(),
        "log_path": str(log_path) if log_path else None,
        "state": STATE_RUNNING,
        "finished_at": None,
        "exit_detail": None,
    }
    write_artifact(worker_path(worker_id, run_id, root), row)
    return row


def finish(worker_id: str, *, state: str = STATE_COMPLETED,
           detail: Optional[str] = None, run_id: str = RUN_ID,
           root: Optional[Path] = None) -> dict:
    """Record a terminal state for a worker this process registered."""
    if state not in TERMINAL_STATES:
        raise WorkerRefusal(
            "%r is not a terminal state; finish() records %s"
            % (state, list(TERMINAL_STATES)))
    p = worker_path(worker_id, run_id, root)
    if not p.exists():
        raise WorkerRefusal("no registered worker %s in run %s"
                            % (worker_id, run_id))
    row = json.loads(p.read_text(encoding="utf-8-sig"))
    row["state"] = state
    row["finished_at"] = now_iso()
    row["exit_detail"] = (str(detail)[:500] if detail else None)
    write_artifact(p, row)
    return row


class worker:                                          # noqa: N801 - a verb
    """Context manager: RUNNING on entry, COMPLETED or FAILED on exit.

    A worker that dies without unwinding leaves its row RUNNING, and
    :func:`report` reclassifies it as INTERRUPTED once its process is gone.
    That asymmetry is deliberate: a crash must look different from a clean
    finish, and only the READER may decide a vanished process was interrupted.
    """

    def __init__(self, worker_id: str, *, command: Optional[list] = None,
                 log_path: Optional[str] = None, run_id: str = RUN_ID,
                 kind: str = KIND_HEAVY, root: Optional[Path] = None):
        self.worker_id = worker_id
        self.run_id = run_id
        self.root = root
        self.row: dict = {}
        self._kw = {"command": command, "log_path": log_path, "kind": kind}

    def __enter__(self) -> dict:
        self.row = register(self.worker_id, run_id=self.run_id,
                            root=self.root, **self._kw)
        return self.row

    def __exit__(self, exc_type, exc, tb) -> bool:
        finish(self.worker_id,
               state=STATE_COMPLETED if exc_type is None else STATE_FAILED,
               detail=None if exc_type is None
               else "%s: %s" % (exc_type.__name__, exc),
               run_id=self.run_id, root=self.root)
        return False


# --------------------------------------------------------------------------- #
# Reading - the operator surface. READ ONLY.
# --------------------------------------------------------------------------- #
def rows(run_id: Optional[str] = None, root: Optional[Path] = None) -> list:
    """Every registered worker row, newest first. Nothing is mutated."""
    base = Path(root or WORKER_ROOT)
    if not base.exists():
        return []
    dirs = ([base / str(run_id)] if run_id
            else [d for d in sorted(base.iterdir()) if d.is_dir()])
    out = []
    for d in dirs:
        if not d.exists():
            continue
        for p in sorted(d.glob("*.json")):
            try:
                out.append(json.loads(p.read_text(encoding="utf-8-sig")))
            except (OSError, ValueError):
                out.append({"worker_id": p.stem, "run_id": d.name,
                            "state": "UNREADABLE", "path": str(p)})
    out.sort(key=lambda r: str(r.get("started_at") or ""), reverse=True)
    return out


def observed_state(row: dict) -> str:
    """The state the MACHINE says this worker is in, right now.

    A row still claiming RUNNING whose process is gone (or whose pid has been
    recycled by a different process) is INTERRUPTED. The stored row is not
    rewritten: the registry records what the worker said, and this function
    records what the machine shows.
    """
    stored = str(row.get("state") or "")
    if stored != STATE_RUNNING:
        return stored
    return STATE_RUNNING if same_process(row) else STATE_INTERRUPTED


def report(run_id: Optional[str] = None, root: Optional[Path] = None) -> dict:
    """What the operator reads. Never terminates anything."""
    rs = rows(run_id, root)
    enriched = []
    for r in rs:
        obs = observed_state(r)
        enriched.append({**r, "observed_state": obs,
                         "stale": bool(str(r.get("state")) == STATE_RUNNING
                                       and obs != STATE_RUNNING)})
    running = [r for r in enriched if r["observed_state"] == STATE_RUNNING]
    stale = [r for r in enriched if r["stale"]]
    heavy_running = [r for r in running if str(r.get("kind")) == KIND_HEAVY]
    return {
        "registry_owner": REGISTRY_OWNER,
        "registry_version": REGISTRY_VERSION,
        "generated_at": now_iso(),
        "run_id": run_id,
        "workers": enriched,
        "running": [r["worker_id"] for r in running],
        "running_count": len(running),
        "heavy_running_count": len(heavy_running),
        "max_concurrent_heavy_jobs": MAX_CONCURRENT_HEAVY_JOBS,
        "heavy_over_budget": len(heavy_running) > MAX_CONCURRENT_HEAVY_JOBS,
        "stale": [r["worker_id"] for r in stale],
        "stale_count": len(stale),
        "by_state": {s: sum(1 for r in enriched
                            if r["observed_state"] == s) for s in STATES},
        "orphaned_workers": [r["worker_id"] for r in stale],
        "advice": ("This report never terminates a process. A stale row means "
                   "the worker died without unwinding; inspect it before "
                   "acting, and never terminate a process chosen by name."),
    }


def assert_none_running(run_id: Optional[str] = None,
                        root: Optional[Path] = None) -> dict:
    """Prove no worker of ``run_id`` is still consuming the machine.

    Returns the report. Raises if any worker is genuinely still RUNNING, so a
    release cannot claim a clean finish it did not have.
    """
    rep = report(run_id, root)
    if rep["running_count"]:
        raise WorkerRefusal(
            "%d worker(s) still RUNNING in run %s: %s"
            % (rep["running_count"], run_id, ", ".join(rep["running"])))
    return rep
