r"""scripts/run_r59_autonomous_engine.py - drive the R59 autonomous alpha engine.

Usage (Windows PowerShell only)::

    & C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
        D:\paper_trader_alpha_recovery_offensive\scripts\run_r59_autonomous_engine.py `
        --continuous --skip-import --jobs 2

Modes::

    --import-only       ingest prior-release evidence into research memory, then
                        report; run no experiment
    --report-only       rebuild the state artifact from existing memory
    --mechanism-status  seed the closed-mechanism ledger, print the mechanism
                        frontier and write the agent checkpoint; run nothing
    --continuous        the ALPHA AGENT loop for a development checkout: run a
                        session; when it stops on A or B (every remaining path
                        needs a new executor, a human gate or new information)
                        wait until the mechanism frontier, an executor pin or
                        the runnable queue changes, then re-enter. It never
                        advances prospective evidence - that stays with the
                        canonical persistent runtime - and it stops only on the
                        agent STOP file, an interrupt or an explicit operator cap.
    (default)           import, then run one autonomous research session, then
                        report

Every invocation is resumable: the queue and the memory are SQLite databases
under the R59 research root and the agent checkpoint is rewritten every
iteration, so a session that stops is continued - not restarted - by running
this again.

RESEARCH ONLY. This script cannot create an order or a fill, enable a broker,
promote a model, activate a sleeve, approve a proposal, register a challenger or
write an operational store. It restarts no backend and changes no scheduled task.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
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

#: The canonical forward registry, READ ONLY. Its owner is the application's
#: ``api.forward_challenger_registry``; this script only reports what it holds.
REGISTRY_DIR_ENV = "PAPER_TRADER_FORWARD_CHALLENGER_REGISTRY_DIR"
DEFAULT_REGISTRY_DIR = Path(r"D:\Stock_Prediction_app_data\forward_challenger_registry")


def _latest_commits(n: int = 12):
    try:
        out = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "log", "-n", str(n),
             "--pretty=format:%h|%cI|%s"],
            capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None
    rows = []
    for line in (out.stdout or "").splitlines():
        parts = line.split("|", 2)
        if len(parts) == 3:
            rows.append({"commit": parts[0], "committed_at": parts[1],
                         "subject": parts[2]})
    return rows or None


def _forward_state(catalog):
    """What the canonical registry holds for each live candidate. Read only."""
    ids = {c.get("candidate_id") for c in (catalog or {}).get("live_candidates") or []}
    reg = Path(os.environ.get(REGISTRY_DIR_ENV) or DEFAULT_REGISTRY_DIR) / "registrations"
    rows = []
    for p in sorted(reg.glob("*.json")) if reg.exists() else []:
        try:
            body = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if body.get("challenger_id") not in ids:
            continue
        ident = body.get("identity") or {}
        rows.append({
            "candidate_id": body.get("challenger_id"),
            "identity_hash": ident.get("identity_hash"),
            "inception": ident.get("inception"),
            "horizon_sessions": body.get("horizon_sessions"),
            "effective_independent_observations":
                body.get("effective_independent_observations"),
            "backfilled": body.get("backfilled"),
            "automatic_promotion_allowed": body.get("automatic_promotion_allowed"),
            "evidence_accrual_owner": body.get("evidence_accrual_owner"),
            "registration_file": str(p),
            "state_source": "canonical forward registry (read only)"})
    return rows


def _context():
    from alpha_agent.r59 import mechanisms as MX
    return {"commits": _latest_commits(), "forward": _forward_state(MX.load_catalog())}


def _fingerprint(mem, queue) -> str:
    from alpha_agent import r59
    from alpha_agent.r59 import mechanisms as MX
    fr = MX.frontier(mem)
    return r59.short_hash([fr.get("catalog_hash"),
                           sorted((str(r["mechanism_id"]), r["status"], r["executor_state"])
                                  for r in fr.get("rows") or []),
                           queue.runnable_depth()], 16)


def _print_session(session: dict) -> None:
    for it in session["iterations"]:
        print("  iter %-3d ready=%-2d gen=%-3d enq=%-3d claimed=%-2d "
              "done=%-2d blocked=%-2d err=%-2d measured=%-4d"
              % (it["iteration"], len(it["frontier_ready"]),
                 it["mandates_generated"], it["mandates_enqueued"],
                 it["jobs_claimed"], it["jobs_completed"],
                 it["jobs_blocked"], it["handler_errors"],
                 it["hypotheses_measured"]), flush=True)
    print("  stop: %s" % session["stop_condition"], flush=True)
    print("  jobs executed: %d   hypotheses measured: %d   still ready: %d"
          % (session["jobs_executed"], session["hypotheses_measured"],
             session["research_still_ready"]), flush=True)


def _print_frontier(mem) -> None:
    from alpha_agent.r59 import mechanisms as MX
    fr = MX.frontier(mem)
    print("  catalog %s (%s)" % (fr.get("catalog_path"), fr.get("catalog_hash")))
    print("  by status: %s" % json.dumps(fr.get("by_status")))
    for r in sorted(fr.get("rows") or [], key=lambda r: -r["score"]["total"]):
        print("  %-48s %-36s %.4f executor=%s"
              % (r["mechanism_id"], r["status"], r["score"]["total"], r["executor_state"]))


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
    ap.add_argument("--skip-import", action="store_true",
                    help="do not re-run the prior-release evidence import")
    ap.add_argument("--mechanism-status", action="store_true")
    ap.add_argument("--continuous", action="store_true")
    ap.add_argument("--poll", type=float, default=30.0,
                    help="continuous mode: seconds between frontier checks")
    ap.add_argument("--idle-recheck", type=float, default=900.0,
                    help="continuous mode: re-enter a session at least this often")
    ap.add_argument("--research-root", default=None)
    args = ap.parse_args(argv)

    if args.research_root:
        os.environ["PAPER_TRADER_R59_RESEARCH_ROOT"] = args.research_root

    from alpha_agent import r59
    from alpha_agent.r59 import importers as IMP
    from alpha_agent.r59 import loop as LP
    from alpha_agent.r59 import mechanisms as MX
    from alpha_agent.r59 import memory as M
    from alpha_agent.r59 import providers as PR
    from alpha_agent.r59 import report as RPT
    from alpha_agent.r59 import steele as ST

    print("R59 import integrity:", r59.assert_worktree_import())
    print("R59 research root   :", r59.research_root())

    mem = M.open_memory()

    if args.mechanism_status:
        seeded = MX.seed_closed(mem)
        print("\n== MECHANISM FRONTIER ==")
        print("  closed ledger: added %d, already present %d"
              % (len(seeded["added"]), seeded["already_present"]))
        _print_frontier(mem)
        queue = LP.open_queue()
        path = MX.write_checkpoint(mem, queue, context=_context())
        body = json.loads(Path(path).read_text(encoding="utf-8"))
        print("  checkpoint: %s" % path)
        print("  CURRENT_RESEARCH_ACTION: %s"
              % json.dumps(body["CURRENT_RESEARCH_ACTION"])[:600])
        return 0

    if not args.report_only and not args.skip_import:
        imp = IMP.import_all(mem)
        print("\n== RESEARCH MEMORY IMPORT ==")
        for k, v in imp["sources"].items():
            print("  %-16s %-14s imported=%s"
                  % (k, v.get("state"), v.get("imported")))
        b = imp["summary"]["search_burden"]
        print("  counted search burden: %d across %d families"
              % (b["total"], b["distinct_families"]))

    if args.continuous:
        queue = LP.open_queue()
        stop_file = r59.research_root() / MX.CHECKPOINT_SUBDIR / "STOP"
        n = 0
        print("\n== ALPHA AGENT CONTINUOUS LOOP (stop file: %s) ==" % stop_file,
              flush=True)
        try:
            while not stop_file.exists():
                n += 1
                print("\n-- session %d --" % n, flush=True)
                session = LP.run_session(max_iterations=args.iterations,
                                         batch=args.batch,
                                         max_jobs_per_iteration=args.jobs,
                                         budget_seconds=args.budget, mem=mem,
                                         queue=queue, checkpoint_context=_context)
                _print_session(session)
                MX.write_checkpoint(mem, queue, context=_context())
                if session["stop_condition"] == LP.STOP_D:
                    print("  an explicit operator cap bound; exiting", flush=True)
                    break
                mark = _fingerprint(mem, queue)
                print("  waiting for the mechanism frontier, an executor pin or "
                      "runnable work to change (re-check every %.0fs)" % args.poll,
                      flush=True)
                waited = 0.0
                while not stop_file.exists():
                    time.sleep(args.poll)
                    waited += args.poll
                    if waited >= args.idle_recheck or _fingerprint(mem, queue) != mark:
                        break
        except KeyboardInterrupt:
            print("interrupted; the queue, memory and checkpoint are persisted")
        return 0

    session = None
    if not args.import_only and not args.report_only:
        print("\n== AUTONOMOUS SESSION ==")
        session = LP.run_session(max_iterations=args.iterations,
                                 batch=args.batch,
                                 max_jobs_per_iteration=args.jobs,
                                 budget_seconds=args.budget, mem=mem,
                                 checkpoint_context=_context)
        _print_session(session)

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
