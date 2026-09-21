r"""scripts/r61_power_calibration.py - run the R61 power calibration sweep.

RESEARCH INFRASTRUCTURE ONLY. NOT AN ALPHA SEARCH.

Registers no hypothesis, charges no burden, consumes no lockbox, creates no
forward request, promotes nothing and rewrites no historical result.

PowerShell:

    & C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
        C:\Users\binis\paper_trader\scripts\r61_power_calibration.py `
        --out D:\Stock_Prediction_app_data\r61_apparatus_calibration\power

The process registers itself in the canonical worker registry
(``alpha_agent.r61.workers``) on entry and records a terminal state on exit,
so a run can never leave an unaccounted Python process on the operator's
machine. It writes the FROZEN pre-registration before it measures anything,
and one result file per panel as that panel completes, so an interrupted run
loses only the panel it was in.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from alpha_agent import r61                                   # noqa: E402
from alpha_agent.r61 import calibration_panels as CP          # noqa: E402
from alpha_agent.r61 import power as P                        # noqa: E402
from alpha_agent.r61 import workers as W                      # noqa: E402

WORKER_ID = "r61_power_calibration"


def run(out_dir: Path, *, panels=None, seeds: int = P.SEEDS_PER_POINT,
        rho_grid=None, verbose: bool = True) -> dict:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    grid = tuple(rho_grid or P.RHO_GRID)

    prereg = P.pre_registration()
    prereg["seeds_per_point"] = int(seeds)
    prereg["effect_grid"] = list(grid)
    prereg["pre_registration_hash"] = r61.stable_hash(
        {k: v for k, v in prereg.items() if k != "pre_registration_hash"})
    r61.write_artifact(out_dir / "PRE_REGISTRATION.json", prereg)
    if verbose:
        print("PRE_REGISTRATION frozen: %s"
              % prereg["pre_registration_hash"][:16], flush=True)

    panel_ids = list(panels or CP.PANEL_IDS)
    summaries = []
    for pid in panel_ids:
        t0 = time.time()
        spec = CP.load(pid)
        meta = CP.describe(pid)
        pre = P.precompute(spec, verbose=False)
        if verbose:
            print("[%s] %d instruments, %d decisions, median %d eligible "
                  "(precompute %.1fs)"
                  % (pid, meta["instrument_count"], pre["n_decisions"],
                     pre["median_eligible"], time.time() - t0), flush=True)
        cells = []
        for rho in grid:
            t1 = time.time()
            for seed in range(1, int(seeds) + 1):
                cells.append(P.run_cell(spec, pre, rho=float(rho), seed=seed))
            hits = sum(1 for c in cells[-int(seeds):] if c["detected"])
            if verbose:
                print("   rho=%-6.3f detections %3d/%-3d  (%.1fs)"
                      % (rho, hits, seeds, time.time() - t1), flush=True)
        # The realised IC, measured on one reference seed per effect, so the
        # injection's own claim is evidence rather than assertion.
        ic_check = []
        for rho in grid:
            eps = P.noise_matrix(spec, pre, 1)
            ic_check.append({"rho": float(rho),
                             **P.realised_ic(spec, pre, eps, float(rho))})
        summary = P.panel_summary(pid, cells, {
            "n_decisions": pre["n_decisions"],
            "median_eligible": pre["median_eligible"]}, meta)
        summary["injection_verification"] = ic_check
        summary["elapsed_seconds"] = round(time.time() - t0, 1)
        summary["pre_registration_hash"] = prereg["pre_registration_hash"]
        r61.write_artifact(out_dir / ("PANEL_%s.json" % pid), summary)
        cells_body = {"panel": pid, "cells": cells,
                      "pre_registration_hash": prereg["pre_registration_hash"]}
        r61.write_artifact(out_dir / ("CELLS_%s.json" % pid), cells_body)
        summaries.append(summary)
        if verbose:
            m80 = summary["mde"]["MDE_80"]
            print("[%s] DONE in %.1fs  MDE_80 rho=%s net=%s"
                  % (pid, summary["elapsed_seconds"], m80["mde_rho"],
                     m80["mde_ann_net_excess"]), flush=True)

    result = {
        "calibration": "R61_POWER_CALIBRATION",
        "power_calibration_version": P.POWER_CALIBRATION_VERSION,
        "pre_registration_hash": prereg["pre_registration_hash"],
        "panels": panel_ids,
        "seeds_per_point": int(seeds),
        "effect_grid": list(grid),
        "cells_run": sum(len(s["curve"]) * int(seeds) for s in summaries),
        "summaries": [{k: v for k, v in s.items() if k != "curve"}
                      for s in summaries],
        "curves": {s["panel"]: s["curve"] for s in summaries},
        "final_table": [
            {"PANEL": s["panel"],
             "INSTRUMENT_COUNT": s["instrument_count"],
             "DECISION_COUNT": s["decision_count"],
             "EFFECTIVE_OBSERVATIONS": s["effective_observations_lockbox"]
             or s["lockbox_periods"],
             "COST_MODEL": (s["cost_model"] or {}).get("cost_model_id"),
             "BURDEN_USED": s["burden_used"],
             "SEEDS": int(seeds),
             "FALSE_POSITIVE_RATE_AT_RHO_ZERO":
                 s["false_positive_rate_at_rho_zero"],
             "MDE_50": s["mde"]["MDE_50"],
             "MDE_80": s["mde"]["MDE_80"],
             "MDE_90": s["mde"]["MDE_90"]}
            for s in summaries],
    }
    r61.write_artifact(out_dir / "POWER_CALIBRATION_RESULT.json", result)
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=str(r61.POWER_ROOT))
    ap.add_argument("--panels", nargs="*", default=None)
    ap.add_argument("--seeds", type=int, default=P.SEEDS_PER_POINT)
    ap.add_argument("--log", default=None)
    args = ap.parse_args()

    with W.worker(WORKER_ID, command=[sys.executable] + sys.argv,
                  log_path=args.log, kind=W.KIND_HEAVY) as row:
        print("WORKER pid=%s parent=%s started=%s"
              % (row["pid"], row["parent_pid"], row["started_at"]), flush=True)
        res = run(Path(args.out), panels=args.panels, seeds=args.seeds)
    print(json.dumps({k: v for k, v in res.items()
                      if k in ("pre_registration_hash", "panels",
                               "seeds_per_point", "cells_run")}, indent=1))
    print("R61_POWER_CALIBRATION_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
