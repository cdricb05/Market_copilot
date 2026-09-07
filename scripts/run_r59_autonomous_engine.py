r"""scripts/run_r59_autonomous_engine.py - drive the R59 autonomous alpha engine.

Usage (Windows PowerShell only)::

    & C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
        D:\paper_trader_r59_autonomous_alpha\scripts\run_r59_autonomous_engine.py `
        --iterations 12 --batch 12 --jobs 8 --budget 900

Modes::

    --import-only     ingest prior-release evidence into research memory, then
                      report; run no experiment
    --report-only     rebuild the state artifact from existing memory
    (default)         import, then run an autonomous research session, then
                      report

Every invocation is resumable: the queue and the memory are SQLite databases
under the R59 research root, so a session that stops on an environment limit is
continued - not restarted - by running this again.

RESEARCH ONLY. This script cannot create an order or a fill, enable a broker,
promote a model, activate a sleeve, approve a proposal or write an operational
store. It restarts no backend and changes no scheduled task.
"""
from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path

# --------------------------------------------------------------------------- #
# Worktree import integrity.
#
# The venv carries an editable install whose finder maps the ``paper_trader``
# package to C:\Users\binis\paper_trader. A worktree that imports through that
# finder silently executes the LIVE tree's code. R59 is therefore imported as
# top-level ``alpha_agent`` from THIS repository root, and the assertion below
# proves it rather than trusting it.
# --------------------------------------------------------------------------- #
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

warnings.filterwarnings("ignore", category=RuntimeWarning)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="R59 autonomous alpha engine")
    # DEFAULT = RUN_UNTIL_RESEARCH_EXHAUSTED_OR_REAL_EXTERNAL_BLOCKER.
    # Both caps default to None. They are explicit operator/debug overrides,
    # never production research behaviour: an earlier run stopped on a
    # self-imposed iteration counter with nineteen mandates still READY and
    # reported it as an environment limit, which it was not.
    ap.add_argument("--iterations", type=int, default=None,
                    help="OPERATOR OVERRIDE: cap iterations (debug only)")
    ap.add_argument("--batch", type=int, default=12)
    ap.add_argument("--jobs", type=int, default=8,
                    help="jobs drained per iteration")
    ap.add_argument("--budget", type=float, default=None,
                    help="OPERATOR OVERRIDE: wall-clock cap in seconds "
                         "(debug only)")
    ap.add_argument("--import-only", action="store_true")
    ap.add_argument("--report-only", action="store_true")
    ap.add_argument("--research-root", default=None)
    args = ap.parse_args(argv)

    if args.research_root:
        import os
        os.environ["PAPER_TRADER_R59_RESEARCH_ROOT"] = args.research_root

    from alpha_agent import r59
    from alpha_agent.r59 import importers as IMP
    from alpha_agent.r59 import loop as LP
    from alpha_agent.r59 import memory as M
    from alpha_agent.r59 import providers as PR
    from alpha_agent.r59 import report as RPT
    from alpha_agent.r59 import steele as ST

    print("R59 import integrity:", r59.assert_worktree_import())
    print("R59 research root   :", r59.research_root())

    mem = M.open_memory()

    if not args.report_only:
        imp = IMP.import_all(mem)
        print("\n== RESEARCH MEMORY IMPORT ==")
        for k, v in imp["sources"].items():
            print("  %-16s %-14s imported=%s"
                  % (k, v.get("state"), v.get("imported")))
        b = imp["summary"]["search_burden"]
        print("  counted search burden: %d across %d families"
              % (b["total"], b["distinct_families"]))

    session = None
    if not args.import_only and not args.report_only:
        print("\n== AUTONOMOUS SESSION ==")
        session = LP.run_session(max_iterations=args.iterations,
                                 batch=args.batch,
                                 max_jobs_per_iteration=args.jobs,
                                 budget_seconds=args.budget, mem=mem)
        for it in session["iterations"]:
            print("  iter %-3d ready=%-2d gen=%-3d enq=%-3d claimed=%-2d "
                  "done=%-2d blocked=%-2d err=%-2d measured=%-4d"
                  % (it["iteration"], len(it["frontier_ready"]),
                     it["mandates_generated"], it["mandates_enqueued"],
                     it["jobs_claimed"], it["jobs_completed"],
                     it["jobs_blocked"], it["handler_errors"],
                     it["hypotheses_measured"]))
        print("  stop: %s" % session["stop_condition"])
        print("  jobs executed: %d   hypotheses measured: %d   still ready: %d"
              % (session["jobs_executed"], session["hypotheses_measured"],
                 session["research_still_ready"]))

    print("\n== ARTIFACTS ==")
    ST.publish()
    PR.scoreboard(mem)
    doc = RPT.build(mem, session=session)
    print("  state report: %s" % doc.get("artifact_path"))

    res = doc["session_results"]
    print("\n== R59 RESULTS ==")
    print("  hypotheses measured this estate: %d"
          % res["r59_hypotheses_measured"])
    print("  qualified: %d" % res["n_qualified"])
    print("  by asset class: %s" % json.dumps(res["by_asset_class"]))
    print("  by generation method: %s" % json.dumps(res["by_generation_method"]))
    print("\n  strongest measured:")
    for h in res["strongest_overall"][:8]:
        print("    t=%-7s %-22s %-34s %s"
              % (round(h["lockbox_t"], 3) if h["lockbox_t"] is not None
                 else None, h["asset_class"], h["economic_family"][:32],
                 h["outcome"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
