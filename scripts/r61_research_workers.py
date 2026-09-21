r"""scripts/r61_research_workers.py - what Paper Trader research is running NOW.

READ ONLY. THIS SCRIPT NEVER TERMINATES A PROCESS.

There is no kill verb here and there is not meant to be one. The operator's
machine was degraded by research jobs that left no trace, and the reflex that
follows - hunting ``python.exe`` by NAME in Task Manager - is how an unrelated
process, a scheduled task, or Claude Code itself gets killed by mistake.

What this reports instead:

    every registered Paper Trader research worker, with its RUN_ID, WORKER_ID,
    PID, PARENT_PID, COMMAND, STARTED_AT, LOG_PATH and STATE;

    the state the MACHINE shows, which is not always the state the worker
    recorded - a row still claiming RUNNING whose process is gone, or whose
    pid has since been recycled by a DIFFERENT process, is reported
    INTERRUPTED and flagged STALE;

    whether the concurrent heavy-job budget is exceeded.

PowerShell:

    & C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
        C:\Users\binis\paper_trader\scripts\r61_research_workers.py

    ... --run-id R61_RESEARCH_APPARATUS_CALIBRATION_AND_REPAIR
    ... --json
    ... --assert-none-running        exit 1 if any worker is still RUNNING
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from alpha_agent.r61 import workers as W                      # noqa: E402


def render(rep: dict) -> str:
    lines = ["PAPER TRADER RESEARCH WORKERS  (read-only; nothing is "
             "terminated by this report)",
             "generated_at: %s" % rep["generated_at"],
             "registry:     %s" % rep["registry_owner"], ""]
    if not rep["workers"]:
        lines.append("  no registered workers")
    for r in rep["workers"]:
        flag = "  <-- STALE" if r.get("stale") else ""
        lines.append("  [%s] %s / %s%s"
                     % (r.get("observed_state"), r.get("run_id"),
                        r.get("worker_id"), flag))
        lines.append("      pid %s (parent %s)  kind %s  started %s"
                     % (r.get("pid"), r.get("parent_pid"), r.get("kind"),
                        r.get("started_at")))
        if r.get("finished_at"):
            lines.append("      finished %s  %s"
                         % (r["finished_at"], r.get("exit_detail") or ""))
        if r.get("log_path"):
            lines.append("      log %s" % r["log_path"])
        cmd = r.get("command") or []
        lines.append("      cmd %s" % " ".join(str(c) for c in cmd)[:220])
    lines += [
        "",
        "PAPER_TRADER_BACKGROUND_PYTHON_PROCESSES = %d" % rep["running_count"],
        "ORPHANED_WORKERS = %s" % (", ".join(rep["orphaned_workers"])
                                   or "NONE"),
        "HEAVY_RUNNING = %d  (budget %d%s)"
        % (rep["heavy_running_count"], rep["max_concurrent_heavy_jobs"],
           ", OVER BUDGET" if rep["heavy_over_budget"] else ""),
        "BY_STATE = %s" % json.dumps(rep["by_state"]),
        "",
        rep["advice"],
    ]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--assert-none-running", action="store_true")
    args = ap.parse_args()

    rep = W.report(args.run_id)
    print(json.dumps(rep, indent=1) if args.json else render(rep))
    if args.assert_none_running and rep["running_count"]:
        print("\nRESEARCH_WORKERS_STILL_RUNNING - %d" % rep["running_count"])
        return 1
    print("\nRESEARCH_WORKERS_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
