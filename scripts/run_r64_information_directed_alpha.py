"""scripts/run_r64_information_directed_alpha.py - the ONE R64 research runner.

WHAT THIS IS
    A research-only entrypoint that runs the R64 stages in order against the
    OWNED substrates and the persisted R63 artifacts (read only), writing every
    artifact under the R64 research root:

        handoff       r63_handoff_validation.json (the exact R63 FX-carry
                      artifact verified; with --reproduce, a fresh cell too)
        contracts     r64_distinct_contract_evidence.json (pseudo-curves refused)
        grid          cells/*.json (checkpointed, resumable)
        merge         r64_carry_matrix.json (campaign BH, inherited R63 FDR)
        family        r64_fx_carry_family.json (Holm within the FX CARRY family)
        challengers   r64_challenger_candidates.json + challengers/*.json
        frontier      r64_information_need_updates.json (an overlay, not a frontier)
        report        R64_REPORT.md
        all           every stage in that order

WHAT THIS IS NOT
    It does not restart, deploy, register, adopt, promote, approve, order,
    fill, purchase, subscribe or write any live store. The canonical checkout,
    the live desk ledger and the R63 research root are read only. There is no
    execute flag because there is nothing operational to execute.

TERMINAL TOKENS (exactly one on the last line)
    R64_STAGE_OK / R64_STAGE_FAILED - <reason>
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

import warnings  # noqa: E402

warnings.filterwarnings("ignore")

from alpha_agent import r64  # noqa: E402

ENTRYPOINT = "scripts/run_r64_information_directed_alpha.py"
OK = "R64_STAGE_OK"
FAILED = "R64_STAGE_FAILED"
STAGES = ("handoff", "contracts", "grid", "merge", "family", "challengers", "frontier",
          "report", "all")


def _cells() -> list:
    """The MERGED cells (campaign BH applied, R63 FDR inherited) when the
    matrix exists; the raw checkpoints carry no multiple-testing verdict and
    must never feed the family, challenger or frontier stages."""
    from alpha_agent.r64 import experiments as X
    matrix = r64.read_artifact(X.MATRIX_ARTIFACT)
    if matrix and matrix.get("cells"):
        return list(matrix["cells"])
    return X.merge()["cells"]


def stage_handoff(args) -> dict:
    from alpha_agent.r64 import handoff_validation as HV
    from alpha_agent.r64 import experiments as X
    fresh = None
    if getattr(args, "reproduce", False):
        fresh = X.measure_cell(X.spec(r64.AC_FX, "XS", 1, r64.DIM_CURVE_CARRY, "AUGMENTATION",
                                      X.TAG_REPRODUCTION))
    else:
        # the grid's own checkpoint of the same cell, when it exists
        fresh = next((c for c in X.load_cells() if c.get("cell_id") == HV.FX_CELL_ID
                      and c.get("conditional")), None)
    body = HV.validate(reproduced_cell=fresh)
    return {"verdict": body["verdict"], "failed": body["failed_checks"],
            "reproduction": (body.get("reproduction") or {}).get("matches")}


def stage_contracts(args) -> dict:
    from alpha_agent.r64 import carry as C
    body = C.distinct_contract_evidence(refuse=False)
    return {"verdict": body["verdict"], "refused": body["refused"], "n": body["n_markets"]}


def stage_grid(args) -> dict:
    from alpha_agent.r64 import experiments as X
    grid = X.default_grid()
    if args.scopes:
        scopes = tuple(s.strip() for s in args.scopes.split(","))
        grid = [g for g in grid if g["scope"] in scopes]
    if args.tags:
        tags = tuple(t.strip().upper() for t in args.tags.split(","))
        grid = [g for g in grid if g["tag"] in tags]
    if args.horizons:
        hs = tuple(int(h) for h in args.horizons.split(","))
        grid = [g for g in grid if g["horizon"] in hs]
    cells = X.run_grid(grid, verbose=True)
    return {"cells": len(cells)}


def stage_merge(args) -> dict:
    from alpha_agent.r64 import experiments as X
    body = X.merge()
    return {"cells": body["n_cells"], "summary": body["summary"]}


def stage_family(args) -> dict:
    from alpha_agent.r64 import family as FAM
    body = FAM.fx_carry_family(_cells())
    return {"verdict": body["verdict"], "family_p_conditional": body["family_p_conditional"],
            "family_p_economic": body["family_p_economic"], "n": body["n_cells"]}


def stage_challengers(args) -> dict:
    from alpha_agent.r64 import challenger as CH
    from alpha_agent.r64 import family as FAM
    fam = r64.read_artifact(FAM.ARTIFACT_NAME)
    body = CH.build(_cells(), fam)
    return body["counts"]


def stage_frontier(args) -> dict:
    from alpha_agent.r64 import frontier as FR
    body = FR.build(_cells())
    return {"updates": body["n_updates"]}


def stage_report(args) -> dict:
    from alpha_agent.r64 import report as R
    return {"report": str(R.write())}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog=ENTRYPOINT, description=__doc__.split("\n")[0])
    ap.add_argument("--stage", choices=STAGES, default="all")
    ap.add_argument("--scopes", default=None, help="comma-separated scopes for --stage grid")
    ap.add_argument("--tags", default=None, help="comma-separated cell tags for --stage grid")
    ap.add_argument("--horizons", default=None, help="comma-separated horizons for --stage grid")
    ap.add_argument("--reproduce", action="store_true",
                    help="handoff: also re-run the FX carry cell and compare")
    ap.add_argument("--research-root", default=None)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    if args.research_root:
        import os
        os.environ[r64.RESEARCH_ROOT_ENV] = args.research_root
    r64.assert_worktree_import()
    r64.assert_research_root_is_not_live()
    t0 = time.time()
    out = {"entrypoint": ENTRYPOINT, "stage": args.stage,
           "research_root": str(r64.research_root()),
           "r63_results_root": str(r64.r63_results_root()), "safety": r64.SAFETY}
    try:
        stages = STAGES[:-1] if args.stage == "all" else (args.stage,)
        for s in stages:
            out[s] = globals()["stage_%s" % s](args)
    except Exception as exc:                                  # noqa: BLE001
        out["error"] = "%s: %s" % (type(exc).__name__, exc)
        print(json.dumps(out, indent=1, default=str) if args.json else out["error"])
        print("%s - %s" % (FAILED, out["error"]))
        return 1
    out["seconds"] = round(time.time() - t0, 1)
    print(json.dumps(out, indent=1, default=str))
    print(OK)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
