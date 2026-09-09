"""scripts/run_r63_information_sensitivity.py - the ONE R63 research runner.

WHAT THIS IS
    A research-only entrypoint that runs the R63 stages in order against the
    OWNED substrates and writes every artifact under the R63 research root:

        ontology      information_ontology.json
        substrates    r63_substrate_report.json
        acquire       SEC submissions histories + EIA natural-gas archive
        grid          information_sensitivity_matrix.json (or a partial)
        merge         combine partials into the one matrix (BH across all)
        orthogonality orthogonality_matrix.json
        inventory     information_inventory.json (the certification)
        matrix        asset_horizon_information_matrix.json
        gaps          information_gap_frontier.json
        sourcing      sourcing_economics.json
        challengers   r63_challenger_candidates.json + challengers/*.json
        handoff       r63_alphaagent_frontier.json
        report        R63_REPORT.md
        all           every stage in that order (grid without --part)

WHAT THIS IS NOT
    It does not restart, deploy, register, adopt, promote, approve, order,
    fill, purchase, subscribe or write any live store. The canonical checkout
    and the live desk ledger are read only. There is no execute flag because
    there is nothing operational to execute.

TERMINAL TOKENS (exactly one on the last line)
    R63_STAGE_OK / R63_STAGE_FAILED - <reason>
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

from alpha_agent import r63  # noqa: E402
from alpha_agent.r63 import experiments as X  # noqa: E402

ENTRYPOINT = "scripts/run_r63_information_sensitivity.py"
OK = "R63_STAGE_OK"
FAILED = "R63_STAGE_FAILED"
STAGES = ("ontology", "substrates", "acquire", "grid", "merge", "orthogonality",
          "inventory", "matrix", "gaps", "sourcing", "challengers", "handoff",
          "report", "all")


def _cells() -> list:
    return (r63.read_artifact(X.MATRIX_ARTIFACT) or {}).get("cells") or []


def stage_ontology() -> dict:
    from alpha_agent.r63 import ontology as ONT
    body = ONT.build()
    r63.write_artifact(ONT.ARTIFACT_NAME, body)
    return {"n_dimensions": body["n_dimensions"], "hash": body["ontology_hash"]}


def stage_substrates() -> dict:
    from alpha_agent.r63 import panels as P
    rep = P.substrate_report()
    r63.write_artifact("r63_substrate_report.json", rep)
    return {"futures_admitted": rep["futures_store"]["admitted"],
            "equity": rep.get("equity", {}).get("present")}


def stage_acquire() -> dict:
    from alpha_agent.r63 import acquire as A
    ng = A.eia_natural_gas()
    sub = A.sec_submissions(verbose=True)
    return {"eia_ng": ng.get("state"), "sec_submissions": sub.get("state"),
            "ciks_ok": sub.get("ciks_ok"), "rows": sub.get("rows")}


def stage_grid(args) -> dict:
    grid = X.default_grid()
    if args.scopes:
        scopes = tuple(s.strip() for s in args.scopes.split(","))
        grid = [g for g in grid if g[0] in scopes]
    if args.horizons:
        hs = tuple(int(h) for h in args.horizons.split(","))
        grid = [g for g in grid if g[2] in hs]
    if args.modes:
        ms = tuple(m.strip().upper() for m in args.modes.split(","))
        grid = [g for g in grid if g[1] in ms]
    if args.dims:
        ds = tuple(d.strip().upper() for d in args.dims.split(","))
        grid = [g for g in grid if g[3] in ds]
    if args.dim_slice:
        # "k/n": the k-th of n contiguous slices of the dimension list, so a
        # scope's cells can be spread over several worker processes
        k, n = (int(x) for x in args.dim_slice.split("/"))
        dims = sorted({g[3] for g in grid})
        edges = [round(i * len(dims) / n) for i in range(n + 1)]
        mine = set(dims[edges[k - 1]:edges[k]])
        grid = [g for g in grid if g[3] in mine]
    cells = X.run_grid(grid, part=args.part, verbose=True)
    return {"cells": len(cells), "part": args.part}


def stage_merge() -> dict:
    cells = X.merge_partials()
    summary = X.summarise(cells)
    return {"cells": len(cells), "by_verdict": summary["by_verdict"],
            "fdr_conditional_survivors": summary["fdr"]["conditional"]["n_rejected"]}


def stage_orthogonality() -> dict:
    body = X.orthogonality()
    return {"scopes": sorted(body["scopes"])}


def stage_inventory() -> dict:
    from alpha_agent.r63 import inventory as INV
    body = INV.certify(_cells())
    return {"fields": body["n_fields"], "counts": body["counts"], "unknown": body["unknown_fields"]}


def stage_matrix() -> dict:
    from alpha_agent.r63 import asset_horizon as AH
    from alpha_agent.r63 import inventory as INV
    inv = r63.read_artifact(INV.ARTIFACT_NAME) or INV.certify(_cells())
    body = AH.classify(inv, _cells())
    return body["counts"]


def stage_gaps() -> dict:
    from alpha_agent.r63 import asset_horizon as AH
    from alpha_agent.r63 import gaps as G
    from alpha_agent.r63 import inventory as INV
    inv = r63.read_artifact(INV.ARTIFACT_NAME)
    mat = r63.read_artifact(AH.ARTIFACT_NAME)
    ortho = r63.read_artifact(X.ORTHO_ARTIFACT)
    body = G.build(inv, mat, _cells(), ortho)
    return {"needs": body["n_needs"], "top": [r["cell_key"] for r in body["top_gaps"][:5]]}


def stage_sourcing() -> dict:
    from alpha_agent.r63 import asset_horizon as AH
    from alpha_agent.r63 import gaps as G
    from alpha_agent.r63 import inventory as INV
    from alpha_agent.r63 import sourcing as SO
    body = SO.build(r63.read_artifact(G.ARTIFACT_NAME), r63.read_artifact(INV.ARTIFACT_NAME),
                    r63.read_artifact(AH.ARTIFACT_NAME), _cells())
    return {"needs": len(body["needs"]), "gate": len(body["paid_data_gate"]),
            "nav": body["authoritative_nav"].get("nav")}


def stage_challengers() -> dict:
    from alpha_agent.r63 import challengers as CH
    body = CH.build(_cells())
    return body["counts"]


def stage_handoff() -> dict:
    from alpha_agent.r63 import gaps as G
    from alpha_agent.r63 import handoff as H
    body = H.publish(r63.read_artifact(G.ARTIFACT_NAME), _cells())
    return {"ranked": len(body["ranked"])}


def stage_report() -> dict:
    from alpha_agent.r63 import report as R
    p = R.write()
    return {"report": str(p)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog=ENTRYPOINT, description=__doc__.split("\n")[0])
    ap.add_argument("--stage", choices=STAGES, default="all")
    ap.add_argument("--scopes", default=None, help="comma-separated scopes for --stage grid")
    ap.add_argument("--horizons", default=None, help="comma-separated horizons for --stage grid")
    ap.add_argument("--modes", default=None, help="comma-separated modes (XS,TS) for --stage grid")
    ap.add_argument("--dims", default=None, help="comma-separated dimension ids for --stage grid")
    ap.add_argument("--dim-slice", default=None, help="k/n contiguous slice of the dimension list")
    ap.add_argument("--part", default=None, help="partial name for a parallel grid slice")
    ap.add_argument("--research-root", default=None)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    if args.research_root:
        import os
        os.environ[r63.RESEARCH_ROOT_ENV] = args.research_root
    r63.assert_worktree_import()
    r63.assert_research_root_is_not_live()
    t0 = time.time()
    out = {"entrypoint": ENTRYPOINT, "stage": args.stage,
           "research_root": str(r63.research_root()), "safety": r63.SAFETY}
    try:
        if args.stage == "all":
            for s in STAGES[:-1]:
                if s in ("grid",):
                    out[s] = stage_grid(args)
                elif s == "merge":
                    if (r63.research_root() / "partials").exists():
                        out[s] = stage_merge()
                else:
                    out[s] = globals()["stage_%s" % s]() if s != "acquire" else stage_acquire()
        elif args.stage == "grid":
            out["grid"] = stage_grid(args)
        else:
            out[args.stage] = globals()["stage_%s" % args.stage]()
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
