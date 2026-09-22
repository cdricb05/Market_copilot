r"""Measure the HORIZON-5 detection floor. The estate has never had one.

RESEARCH INFRASTRUCTURE ONLY. NOT AN ALPHA SEARCH. The injected signal is
synthetic and known to be synthetic. This registers no hypothesis, charges no
research-family burden, consumes no lockbox budget, creates no forward
candidate and alters no historical outcome.

    & .\.venv-win\Scripts\python.exe research\agents\campaign_r65_non_equity\h5_power.py

WHY
---
Every measured floor the estate owns was calibrated at CADENCE 21 / HORIZON 21:
CROSS_ASSET_FUTURES MDE_80 = 2.98%/yr net at burden 1, COMMODITY_FUTURES 6.56%,
EQUITY_INDEX_FUTURES 4.32%. There is no horizon-5 number anywhere.

That matters because a five-session book rebalances 50.4 times a year instead of
12, so it pays roughly four times the rebalance drag for the same turnover. A
short-horizon cell judged against the 21-session floor is being judged against a
number that was never measured on its geometry - which is precisely the defect
R61 was built to stop, and the reason the R65 director refused to register a
short-horizon cell that would have borrowed one.

H1-H5 is 0.45% of a 8,472-row estate. If the reason is that the apparatus cannot
see a short-horizon effect at any plausible size, that is worth knowing before
another campaign proposes one. If it can, the horizon is open.

WHAT IS FROZEN BEFORE MEASUREMENT
---------------------------------
The rho grid, the seed count, the detection rule, the burden grid and the power
levels are ``alpha_agent.r61.power``'s own frozen constants, unchanged. The ONLY
thing this run changes is cadence and horizon, 21 -> 5, and that change is
declared here before any cell is run. Nothing is re-parameterised afterwards.

TERMINAL TOKEN
    H5_POWER_OK <panels>
    H5_POWER_FAILED <reason>
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from alpha_agent import r59                              # noqa: E402
from alpha_agent.r61 import calibration_panels as CP     # noqa: E402
from alpha_agent.r61 import power as P                   # noqa: E402

HORIZON = 5
CADENCE = 5
RUN_ID = "R65_MULTI_ASSET_ALPHA_AND_PNL_OFFENSIVE"
CALIBRATION_ID = "R65_HORIZON5_DETECTION_FLOOR_V1"
DEFAULT_PANELS = (CP.CROSS_ASSET_FUTURES,)
OUT = _HERE / "H5_POWER_CALIBRATION.json"


def frozen_declaration() -> dict:
    """Declared BEFORE any cell is measured."""
    return {
        "calibration_id": CALIBRATION_ID,
        "run_id": RUN_ID,
        "owner": P.POWER_OWNER,
        "harness_version": P.POWER_CALIBRATION_VERSION,
        "is_alpha_search": False,
        "signal": "SYNTHETIC, known to be synthetic",
        "changed_from_the_canonical_sweep": {
            "cadence_sessions": [r59.CADENCE, CADENCE],
            "horizon_sessions": [r59.HORIZON, HORIZON],
        },
        "unchanged": {
            "rho_grid": list(P.RHO_GRID),
            "seeds_per_point": P.SEEDS_PER_POINT,
            "burden_denominator_grid": list(P.BURDEN_DENOMINATOR_GRID),
            "power_levels": list(P.POWER_LEVELS),
            "detection_rule": P.DETECTION_RULE,
            "strict_detection_rule": P.STRICT_DETECTION_RULE,
        },
        "reference_floors_at_h21": {
            "CROSS_ASSET_FUTURES_MDE_80_burden_1": 0.0298,
            "COMMODITY_FUTURES_MDE_80_burden_1": 0.0656,
            "EQUITY_INDEX_FUTURES_MDE_80_burden_1": 0.0432,
            "note": "quoted for contrast only; a h21 floor does not apply to "
                    "a h5 cell and that is the whole reason for this run",
        },
        "safety": ["RESEARCH ONLY", "PAPER ONLY", "NO ORDERS",
                   "NO PROMOTION", "NO ADOPTION", "PREVIEW ONLY"],
    }


def run_panel(panel_id: str, *, seeds: int) -> dict:
    spec = CP.load(panel_id)
    pre = P.precompute(spec, cadence=CADENCE, horizon=HORIZON)
    cells = []
    t0 = time.time()
    for rho in P.RHO_GRID:
        for k in range(int(seeds)):
            cells.append(P.run_cell(spec, pre, rho=float(rho),
                                    seed=1000 + k, cadence=CADENCE,
                                    horizon=HORIZON))
        print("   rho=%.3f done (%.0fs elapsed)" % (rho, time.time() - t0),
              flush=True)
    summary = P.panel_summary(panel_id, cells, pre, CP.describe(panel_id))
    summary["cadence_sessions"] = CADENCE
    summary["horizon_sessions"] = HORIZON
    summary["seeds_per_point"] = int(seeds)
    summary["wall_seconds"] = round(time.time() - t0, 1)
    return summary


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--panels", nargs="*", default=list(DEFAULT_PANELS))
    ap.add_argument("--seeds", type=int, default=P.SEEDS_PER_POINT)
    ap.add_argument("--out", default=str(OUT))
    args = ap.parse_args(argv)
    try:
        body = {"generated_at": datetime.now(timezone.utc)
                .strftime("%Y-%m-%dT%H:%M:%SZ"),
                "frozen_declaration": frozen_declaration(),
                "panels": {}}
        for pid in args.panels:
            print("panel %s ..." % pid, flush=True)
            body["panels"][pid] = run_panel(pid, seeds=args.seeds)
        Path(args.out).write_text(json.dumps(body, indent=1, default=str),
                                  encoding="utf-8")
        for pid, s in body["panels"].items():
            print("%s  decisions=%s  median_eligible=%s  fp_at_rho0=%s"
                  % (pid, s["decision_count"],
                     s["median_eligible_per_decision"],
                     s["false_positive_rate_at_rho_zero"]))
            for den in P.BURDEN_DENOMINATOR_GRID:
                m = s["mde_at_burden_%d" % den].get("MDE_80") or {}
                print("   burden %-5d MDE_80 rho=%s  net=%s"
                      % (den, m.get("rho"), m.get("ann_net_excess")))
        print("H5_POWER_OK %d" % len(body["panels"]))
        return 0
    except Exception as exc:                                  # noqa: BLE001
        print(json.dumps({"failed": type(exc).__name__,
                          "detail": str(exc)[:900]}, indent=1))
        print("H5_POWER_FAILED %s" % type(exc).__name__)
        return 1


if __name__ == "__main__":
    sys.exit(main())
