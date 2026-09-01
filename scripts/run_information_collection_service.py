"""
scripts/run_information_collection_service.py — Release 29 COLLECTION WORKER.

The ONE long-lived process that keeps information flowing into the Release-28
active-manager path. It owns no policy and no collection logic of its own: every
decision belongs to ``api.information_collection`` (composition) and
``engine.collection_cadence`` (cadence / backoff), and every provider call
belongs to the collector that already owned it.

WHAT THIS PROCESS DOES
    acquire the SINGLE production worker slot (fail closed)
    loop:
        heartbeat                       (I am awake)
        run ONE bounded collection iteration, reporting PROGRESS throughout
                                        (I am still advancing)
        sleep until the next MEANINGFUL due check, heartbeating in slices
    on SIGINT/SIGTERM: finish the current iteration, release the lock, exit 0

TWO DIFFERENT PROOFS, ON PURPOSE
    The heartbeat says the LOOP is awake and is stamped between iterations. It
    cannot be stamped during one, because a timer that fires regardless of what
    the worker is doing would report a hung process as healthy. So a long
    iteration is judged instead on PROGRESS: the callback below is handed to the
    canonical collection path, which calls it whenever a bounded unit of work —
    an HTTP attempt, a source, a scanned partition file, one orchestration step
    — has actually completed. Health reads that evidence and reports BUSY.
    Neither proof is a second scheduler: nothing here wakes up on its own.

It wakes frequently and cheaply. Provider calls happen only when a source is
ACTUALLY due, so a quiet Sunday night costs one due-check every few minutes
rather than seventeen provider calls a minute.

SAFETY
    Never creates, confirms, fills or cancels an order. Never approves a
    proposal or confirms a target. Never runs Daily Close, the full Daily
    Research Cycle or a controlled rebalance. Never promotes a model. It is a
    read-and-record process whose only writes are immutable evidence, service
    state and the review-only artifacts the Release-28 owners already produce.

Usage (Windows PowerShell; normally launched by the Scheduled Task):
    C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe `
      C:\\Users\\binis\\paper_trader\\scripts\\run_information_collection_service.py `
      --interval-seconds 60

Terminal lines (exactly one at exit):
    COLLECTION_SERVICE_STOPPED
    COLLECTION_SERVICE_BLOCKED - <exact reason>
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
import uuid
from datetime import timezone
from pathlib import Path

# Import root = parent of the repo dir (the repo is the package `paper_trader`).
_REPO = Path(__file__).resolve().parents[1]
_PKG_PARENT = str(_REPO.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from paper_trader.api import information_collection as ic  # noqa: E402
from paper_trader.engine import collection_cadence as cad  # noqa: E402

STOPPED = "COLLECTION_SERVICE_STOPPED"
BLOCKED = "COLLECTION_SERVICE_BLOCKED"

#: Bounded log: the file is rotated once it exceeds this size, and exactly one
#: previous generation is retained. A continuously running service must never
#: grow a multi-gigabyte log.
LOG_MAX_BYTES = 4 * 1024 * 1024
LOG_KEEP = 1

_STOP = {"requested": False, "signal": None}


def _install_signal_handlers() -> None:
    def _handler(signum, _frame):
        _STOP["requested"] = True
        _STOP["signal"] = int(signum)
    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is not None:
            try:
                signal.signal(sig, _handler)
            except (ValueError, OSError):  # not installable in this context
                pass


class _BoundedLog:
    """Size-bounded append log. No payload bodies, no secrets — one line per fact."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _rotate_if_needed(self) -> None:
        try:
            if self.path.exists() and self.path.stat().st_size > LOG_MAX_BYTES:
                previous = self.path.with_suffix(self.path.suffix + ".1")
                if previous.exists():
                    previous.unlink()
                os.replace(self.path, previous)
        except OSError:
            pass

    def write(self, event: str, **fields) -> None:
        self._rotate_if_needed()
        row = {"at": ic.utc_iso(), "event": event}
        row.update({k: v for k, v in fields.items() if v is not None})
        try:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row, default=str)[:4000] + "\n")
        except OSError:
            pass


def _iteration_log_fields(receipt: dict) -> dict:
    cycle = receipt.get("event_cycle") or {}
    summary = receipt.get("source_health_summary") or {}
    return {
        "iteration_id": receipt.get("iteration_id"),
        "state": receipt.get("state"),
        "duration_seconds": receipt.get("duration_seconds"),
        "session_phase": receipt.get("session_phase"),
        "due": receipt.get("due_plan"),
        "collected_ok": [c["source_id"] for c in (receipt.get("collected") or [])
                         if c.get("ok")],
        "collected_failed": [
            {"source_id": c["source_id"], "error_category": c.get("error_category")}
            for c in (receipt.get("collected") or []) if not c.get("ok")],
        "new_records": receipt.get("corpus_new_records"),
        "event_cycle_state": cycle.get("state"),
        "events_admitted": cycle.get("events_admitted"),
        "duplicates_suppressed": cycle.get("duplicates_suppressed"),
        "reassessment_ran": cycle.get("reassessment_ran"),
        "proposal_built": cycle.get("proposal_built"),
        "healthy_due": "%s/%s" % (summary.get("healthy_due"),
                                  summary.get("due_now")),
        "progress_checkpoints": receipt.get("progress_checkpoints"),
        "next_wake_seconds": receipt.get("next_wake_seconds"),
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Paper Trader continuous information-collection worker.")
    ap.add_argument("--interval-seconds", type=int,
                    default=cad.DEFAULT_ITERATION_INTERVAL_SECONDS,
                    help="Lightweight wake interval floor (seconds).")
    ap.add_argument("--max-iterations", type=int, default=0,
                    help="Stop after N iterations (0 = run until stopped).")
    ap.add_argument("--max-sources-per-iteration", type=int,
                    default=ic.MAX_SOURCES_PER_ITERATION)
    ap.add_argument("--root", default=None,
                    help="Collection state root (default: %s)."
                         % ic.COLLECTION_DIR_ENV)
    ap.add_argument("--once", action="store_true",
                    help="Run exactly one iteration and exit.")
    ap.add_argument("--allow-disabled", action="store_true",
                    help="Run even when collection automation is not enabled "
                         "(diagnostics only; performs no provider call).")
    args = ap.parse_args(argv)

    root = args.root
    floor = max(int(args.interval_seconds), cad.MIN_ITERATION_INTERVAL_SECONDS)
    instance_id = str(uuid.uuid4())
    pid = os.getpid()
    log = _BoundedLog(ic.logs_root(root) / "collection_service.log")

    state = ic.load_service_state(root=root)
    if not state.get("collection_automation_enabled") and not args.allow_disabled:
        print("%s - collection automation is not enabled. The operator must "
              "authorise it once (manage_information_collection.ps1 -Action "
              "Install -Execute)." % BLOCKED)
        log.write("start_refused", reason="COLLECTION_AUTOMATION_DISABLED")
        return 2

    # Signals are installed BEFORE the lock wait so a service stop during the
    # takeover-window wait is honoured promptly.
    _install_signal_handlers()

    # Release 53: a logon relaunch races the previous session's freshly-killed
    # worker. The bounded-wait acquire keeps single-flight (a LIVE holder is
    # refused immediately) but waits out a provably-dead holder's takeover
    # window instead of exiting terminally - the defect that silently stopped
    # collection from 2026-08-28 (start_refused / exit 3 / no retry trigger).
    lock = ic.acquire_service_lock_with_wait(
        root=root, instance_id=instance_id, pid=pid,
        should_stop=lambda: _STOP["requested"])
    if not lock.get("acquired"):
        print("%s - %s" % (BLOCKED, lock.get("message")))
        log.write("start_refused", reason=lock.get("reason"),
                  holder=lock.get("holder"),
                  waited_seconds=lock.get("waited_seconds"),
                  wait_refused_reason=lock.get("wait_refused_reason"))
        return 3

    ic.register_worker_start(root=root, instance_id=instance_id, pid=pid)
    log.write("worker_started", instance_id=instance_id, pid=pid,
              interval_floor_seconds=floor,
              reclaimed=lock.get("reclaimed"),
              waited_seconds=lock.get("waited_seconds"))

    # THE progress callback for this worker's whole life. Owned by the same
    # module that owns the heartbeat, so there is still exactly one authority
    # for "is this worker healthy".
    progress = ic.ProgressReporter(root=root, instance_id=instance_id)

    iterations = 0
    exit_code = 0
    try:
        while True:
            if _STOP["requested"]:
                break
            started = time.monotonic()
            ic.heartbeat(root=root, instance_id=instance_id,
                         loop_count=iterations)
            try:
                receipt = ic.run_collection_iteration(
                    root=root, instance_id=instance_id,
                    require_enabled=not args.allow_disabled,
                    progress_fn=progress,
                    max_sources=int(args.max_sources_per_iteration))
            except Exception as exc:  # noqa: BLE001 - the loop must survive anything
                log.write("iteration_failed", error="%s: %s"
                          % (type(exc).__name__, str(exc)[:300]))
                receipt = {"next_wake_seconds": floor, "state": "ITERATION_FAILED"}
                svc = ic.load_service_state(root=root)
                svc["last_error"] = "%s: %s" % (type(exc).__name__, str(exc)[:300])
                ic.save_service_state(svc, root=root)
                # A FAILED iteration is closed, not in flight. Leaving the flag
                # set would report the next quiet minute as a stall instead of
                # as the error it actually was.
                ic.clear_iteration_in_flight(root=root)
            else:
                log.write("iteration", **_iteration_log_fields(receipt))
            iterations += 1

            if args.once or (args.max_iterations
                             and iterations >= int(args.max_iterations)):
                break
            if _STOP["requested"]:
                break

            wake = float(receipt.get("next_wake_seconds") or floor)
            wake = max(wake, floor)
            ic.heartbeat(root=root, instance_id=instance_id, loop_count=iterations,
                         next_wake_at=receipt.get("next_wake_at"))
            # Sleep in short slices so a stop signal is honoured promptly and the
            # heartbeat never goes stale during a long quiet wait.
            elapsed = time.monotonic() - started
            remaining = max(0.0, wake - elapsed)
            slept = 0.0
            while slept < remaining and not _STOP["requested"]:
                slice_seconds = min(15.0, remaining - slept)
                time.sleep(slice_seconds)
                slept += slice_seconds
                if slept % 60 < 15:
                    ic.heartbeat(root=root, instance_id=instance_id,
                                 loop_count=iterations)
    except KeyboardInterrupt:
        _STOP["requested"] = True
    finally:
        ic.release_service_lock(root=root, instance_id=instance_id,
                                graceful=True)
        log.write("worker_stopped", instance_id=instance_id,
                  iterations=iterations, signal=_STOP["signal"])

    print("%s - %d iteration(s)" % (STOPPED, iterations))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
