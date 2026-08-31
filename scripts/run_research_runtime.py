"""scripts/run_research_runtime.py - Release 52 scheduled runtime entrypoint.

The ONE action the ``PaperTrader-ResearchRuntime`` Windows task executes, and
the one a human runs for a manual one-shot. It parses arguments, sets the
import path, calls :func:`alpha_agent.r52.runtime.research_runtime_cycle`
exactly once, writes a bounded log line, prints ONE terminal token and exits.

It owns NO timing rule, NO signal, NO calendar and NO retry loop: the
runtime decides what is due from the canonical timing owners, and the
scheduler provides the invocation instants. Running it twice is harmless by
construction (ledger identities, runtime lock).

Terminal tokens (exactly one):
    RESEARCH_RUNTIME_OK - <run state>
    RESEARCH_RUNTIME_REFUSED - <reason>          (exit 3: concurrent run)
    RESEARCH_RUNTIME_INTEGRITY_FAILED - <reason> (exit 4: fail-closed)
    RESEARCH_RUNTIME_FAILED - <reason>           (exit 1)

RESEARCH ONLY. This process never calls the backend, never runs the
portfolio cycle or the daily close, and never touches an operational store.
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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Release 52 persistent prospective research runtime - "
                    "one invocation.")
    ap.add_argument("--trigger", default="MANUAL",
                    help="Label recorded on the run (the scheduled task "
                         "passes its trigger purpose).")
    ap.add_argument("--no-emit", action="store_true",
                    help="Sweep-only invocation: score, record forfeitures, "
                         "rebuild read models, but never emit a batch.")
    args = ap.parse_args(argv)

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
