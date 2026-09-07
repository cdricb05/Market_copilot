"""scripts/run_research_runtime.py - the ONE research runtime entrypoint.

The ONE action the ``PaperTrader-ResearchRuntime`` Windows task executes, and
the one a human runs for a manual one-shot. It parses arguments, sets the
import path, calls exactly one canonical runtime function, writes a bounded
log line, prints ONE terminal token and exits.

It owns NO timing rule, NO signal, NO calendar and NO retry loop: the
runtime decides what is due from the canonical timing owners, and the
scheduler provides the invocation instants. Running it twice is harmless by
construction (ledger identities, runtime lock, worker lease).

TWO MODES, ONE ENTRYPOINT. Release 59 added a persistent mode because the
autonomous researcher's lifetime was still bound to an interactive session:
the discovery loop only ever ran when a person started it, so when the
session ended the research ended too. A second scheduled task would have been
the wrong answer - two tasks means two owners - so the mode is a flag here:

    cycle       (default, unchanged) ONE bounded prospective-evidence
                invocation: alpha_agent.r52.runtime.research_runtime_cycle.
                This is exactly what the installed task has always run.
    persistent  the long-lived autonomous researcher:
                alpha_agent.r59.runtime.run_forever - one worker at a time,
                heartbeating, sleeping only when solely future data can
                advance the state, resuming the persisted queue on every
                start. It has NO production iteration limit.
    status      read-only: print the runtime status document and return.

Terminal tokens (exactly one):
    RESEARCH_RUNTIME_OK - <run state>
    RESEARCH_RUNTIME_REFUSED - <reason>          (exit 3: concurrent run)
    RESEARCH_RUNTIME_INTEGRITY_FAILED - <reason> (exit 4: fail-closed)
    RESEARCH_RUNTIME_FAILED - <reason>           (exit 1)

RESEARCH ONLY. This process never calls the backend, never runs the
portfolio cycle or the daily close, and never touches an operational store.
It cannot promote a model, activate a sleeve, approve a proposal or create
an order in either mode.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_PKG_PARENT = str(_REPO.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

OK = "RESEARCH_RUNTIME_OK"
REFUSED = "RESEARCH_RUNTIME_REFUSED"
INTEGRITY = "RESEARCH_RUNTIME_INTEGRITY_FAILED"
FAILED = "RESEARCH_RUNTIME_FAILED"

LOG_MAX_BYTES = 4 * 1024 * 1024


def _log(root: Path, row: dict) -> None:
    try:
        log_dir = root / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        p = log_dir / "research_runtime.log"
        if p.exists() and p.stat().st_size > LOG_MAX_BYTES:
            prev = p.with_suffix(".log.1")
            if prev.exists():
                prev.unlink()
            os.replace(p, prev)
        with p.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, default=str)[:4000] + "\n")
    except OSError:
        pass


def _persistent(args) -> int:
    """The long-lived autonomous researcher. Delegates; owns no research rule.

    Imported as top-level ``alpha_agent`` from THIS repository so the worker
    can never straddle two checkouts: the venv's editable finder maps
    ``paper_trader`` to the deployed tree, and a persistent process that
    resolved half its modules there and half here would be unattributable.
    """
    if str(_REPO) not in sys.path:
        sys.path.insert(0, str(_REPO))
    from alpha_agent import r59                          # type: ignore
    from alpha_agent.r59 import runtime as R59RT         # type: ignore

    r59.assert_worktree_import()

    # The CANONICAL revision reader, injected here rather than imported by
    # the research package: api.runtime_identity is the one function in this
    # application that answers "what revision is on disk", and a research
    # module that imports it would make research depend on the application.
    # If it cannot be loaded the worker still researches; it just refuses to
    # advance prospective evidence, because it cannot say what wrote it.
    # THIS repository's copy first: _REPO is at the front of sys.path, so
    # the reader comes from the same checkout as the worker it describes.
    try:
        from api import runtime_identity as rid           # type: ignore
        reader = rid.read_source_identity
    except Exception:                                     # noqa: BLE001
        reader = None

    body = R59RT.run_forever(
        batch=int(args.batch),
        max_jobs_per_iteration=int(args.jobs),
        allow_maturation=(False if args.no_maturation else None),
        identity_reader=reader,
        debug_max_seconds=args.debug_max_seconds,
        debug_max_cycles=args.debug_max_cycles)

    _log(R59RT.runtime_dir(), {"at": _dt.datetime.now(_dt.timezone.utc)
                               .isoformat(),
                               "mode": "persistent",
                               "state": body.get("worker_state"),
                               "cycles": body.get("n_cycles"),
                               "stopped_because": body.get("stopped_because")})
    if body.get("worker_state") == R59RT.W_REFUSED:
        print("%s - another autonomous research worker holds the lease "
              "(%s)" % (REFUSED, (body.get("existing_lease") or {})
                        .get("holder")))
        return 3
    print("%s - %s (%d cycle(s), stopped_because=%s)"
          % (OK, body.get("worker_state"), int(body.get("n_cycles") or 0),
             body.get("stopped_because")))
    return 0


def _status(args) -> int:                                # noqa: ARG001
    if str(_REPO) not in sys.path:
        sys.path.insert(0, str(_REPO))
    from alpha_agent.r59 import runtime as R59RT         # type: ignore
    print(json.dumps(R59RT.status(), indent=1, default=str))
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="The canonical research runtime entrypoint - one "
                    "bounded prospective invocation, or the persistent "
                    "autonomous researcher.")
    ap.add_argument("--mode", choices=("cycle", "persistent", "status"),
                    default="cycle",
                    help="cycle (default): ONE prospective-evidence "
                         "invocation. persistent: the long-lived autonomous "
                         "researcher. status: read-only report.")
    ap.add_argument("--trigger", default="MANUAL",
                    help="Label recorded on the run (the scheduled task "
                         "passes its trigger purpose).")
    ap.add_argument("--no-emit", action="store_true",
                    help="Sweep-only invocation: score, record forfeitures, "
                         "rebuild read models, but never emit a batch.")
    ap.add_argument("--batch", type=int, default=12,
                    help="persistent mode: mandates generated per iteration.")
    ap.add_argument("--jobs", type=int, default=8,
                    help="persistent mode: jobs drained per iteration.")
    ap.add_argument("--no-maturation", action="store_true",
                    help="persistent mode: never advance prospective "
                         "evidence (also refused automatically unless the "
                         "source is the deployed, committed checkout).")
    # OPERATOR/DEBUG ONLY. Both default to None: production persistent
    # operation has NO internal cycle or wall-clock limit, and a run that
    # ends on one of these is reported as an operator override rather than
    # as a research conclusion.
    ap.add_argument("--debug-max-seconds", type=float, default=None,
                    help="OPERATOR OVERRIDE (engineering smoke only).")
    ap.add_argument("--debug-max-cycles", type=int, default=None,
                    help="OPERATOR OVERRIDE (engineering smoke only).")
    args = ap.parse_args(argv)

    if args.mode == "status":
        return _status(args)
    if args.mode == "persistent":
        return _persistent(args)

    from paper_trader.alpha_agent import r52
    from paper_trader.alpha_agent.r52 import runtime as RT

    started = _dt.datetime.now(_dt.timezone.utc)
    try:
        body = RT.research_runtime_cycle(
            trigger=str(args.trigger),
            emit_override=("NEVER" if args.no_emit else None))
    except Exception as exc:              # noqa: BLE001 - one token, one exit
        _log(r52.runtime_dir(), {"at": started.isoformat(),
                                 "trigger": args.trigger,
                                 "state": "CRASHED",
                                 "error": "%s: %s" % (type(exc).__name__,
                                                      str(exc)[:400])})
        print("%s - %s: %s" % (FAILED, type(exc).__name__, str(exc)[:300]))
        return 1

    state = str(body.get("state"))
    _log(r52.runtime_dir(), {
        "at": started.isoformat(),
        "trigger": args.trigger,
        "run_id": body.get("run_id"),
        "state": state,
        "stages": [{"stage": s.get("stage"), "state": s.get("state")}
                   for s in (body.get("stages") or ())],
    })
    if state == RT.RUN_REFUSED_CONCURRENT:
        print("%s - another runtime instance holds the lock" % REFUSED)
        return 3
    if state == RT.RUN_FAILED_INTEGRITY:
        print("%s - an evidence chain failed verification; the run failed "
              "closed and wrote nothing" % INTEGRITY)
        return 4
    print("%s - %s (run %s)" % (OK, state, body.get("run_id")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
