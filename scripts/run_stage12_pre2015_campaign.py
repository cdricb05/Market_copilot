"""Bounded, resumable Stage 12 FINAL pre-2015 (2009-2014) confirmation campaign.

THIN MONITORED WRAPPER around the CANONICAL queue path
``alpha_agent.runtime.Runtime.run_stage12_pre2015_campaign`` -- it does NOT execute
lanes itself. The runtime drives the pre-2015 inventory + materialization THROUGH
the shared ResearchQueue (one live job at a time, origin=stage12-autopsy, lane
stage12.pre2015.*, category DATA_VALIDATION, claim/attempt/retry/stale-recovery),
under the collect lock, with the operational-ledger before/after fingerprint, then
finalises the frozen 3-hypothesis independent-time study in-process. Bounded by a
wall-clock budget and resumable across invocations (epoch-scoped lane flags + the
Stage 6 backfill cursor).

RESEARCH-ONLY: the inventory/materialize lanes write ONLY under a DEDICATED pre-2015
ingestion tree (the released 2015+ MARKET_BAR tree, Stage 11 caches and released
Stage 12 artifacts are byte-untouched); the study reads the shared identity +
companyfacts index read-only and writes only under the Stage 12 pre-2015 research
root. Never touches an operational ledger, an order/fill/holding, prediction, or
the Daily Close. No automatic promotion.

Usage (Windows PowerShell):
  C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe scripts\\run_stage12_pre2015_campaign.py \
      --config configs\\alpha_agent\\stage4_runtime.json --budget-seconds 3600 --json
  # offline/local (no queue; in-process lane loop) -- for tests / dev only:
  ... scripts\\run_stage12_pre2015_campaign.py --local --config configs\\alpha_agent\\stage8_autonomy.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))


def _run_canonical(config_path: str, budget_seconds: float, max_lanes):
    try:
        from alpha_agent import runtime as rt
        from alpha_agent import runtime_contracts as rcx
    except Exception:  # pragma: no cover
        from paper_trader.alpha_agent import runtime as rt  # type: ignore
        from paper_trader.alpha_agent import runtime_contracts as rcx  # type: ignore
    cfg = rcx.load_config(config_path)
    runtime = rt.Runtime(cfg, drivers=rt.RealStageDrivers(cfg, repo_root=str(_REPO)))
    return runtime.run_stage12_pre2015_campaign(budget_seconds=budget_seconds,
                                                max_lanes=max_lanes)


def _run_local(config_path: str, budget_seconds: float, max_lanes):
    try:
        from alpha_agent import stage12_pre2015 as jobs
    except Exception:  # pragma: no cover
        from paper_trader.alpha_agent import stage12_pre2015 as jobs  # type: ignore
    return jobs.run_local_campaign(config_path, budget_seconds=budget_seconds,
                                   max_lanes=max_lanes)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Bounded Stage 12 pre-2015 (2009-2014) confirmation campaign")
    ap.add_argument("--config", required=True,
                    help="runtime config (stage4_runtime.json) for the canonical "
                         "path, or stage8_autonomy.json for --local")
    ap.add_argument("--budget-seconds", type=float, default=3600.0,
                    help="wall-clock budget; stops cleanly + RESUMABLE when spent")
    ap.add_argument("--max-lanes", type=int, default=None,
                    help="optional cap on lanes executed this run")
    ap.add_argument("--local", action="store_true",
                    help="offline in-process lane loop (no queue); tests/dev only")
    ap.add_argument("--json", action="store_true", help="emit the report as JSON")
    args = ap.parse_args()

    if args.local:
        report = _run_local(args.config, args.budget_seconds, args.max_lanes)
        complete = bool(report.get("campaign_complete"))
    else:
        report = _run_canonical(args.config, args.budget_seconds, args.max_lanes)
        complete = report.get("terminal") not in (
            "STAGE12_PRE2015_CAMPAIGN_RESUMABLE", "LEDGER_MUTATION_DETECTED")

    if args.json:
        print(json.dumps(report, indent=1, default=str))
    else:
        print("Stage 12 pre-2015 campaign terminal:", report.get("terminal"))
        print("  ledgers_unchanged:", report.get("ledgers_unchanged",
                                                 report.get("campaign_complete")))
        for k in ("epoch", "lanes_planned", "lanes_completed_this_run",
                  "lanes_remaining"):
            if k in report:
                print("  %s: %s" % (k, report[k]))
    return 0 if complete else 3


if __name__ == "__main__":
    raise SystemExit(main())
