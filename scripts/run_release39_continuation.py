"""Release 39 CONTINUATION runner (campaign r39_universal_alpha_continuation_v2).

Phases:
  A  - WIDE exact reconstruction + control reconciliation (Track A)
  B  - factor residualisation of the frozen WIDE stream (Track B)
  C  - group/cluster kill tests (Track C)
  D  - WIDE information attribution (Track D)
  E  - owned-information expansion + untested-cell execution (Tracks E/F)
  X  - trade-expression frontier extras (Track F)
  G  - $0 model-frontier completion (Track G)
  V  - continuation verdict + cumulative burden (Tracks J/K)

Usage (Windows PowerShell):
  & C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe `
      C:\\Users\\binis\\paper_trader\\scripts\\run_release39_continuation.py `
      --phase ABCDEXGV
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.alpha_agent import r39 as R39  # noqa: E402
from paper_trader.alpha_agent.r39 import continuation as CONT  # noqa: E402
from paper_trader.alpha_agent.r39 import (  # noqa: E402
    continuation_campaign as CC,
)
from paper_trader.alpha_agent.r39 import wide_prosecution as WP  # noqa: E402
from paper_trader.alpha_agent.r39.continuation_director import (  # noqa: E402
    Director2,
    build_intl_rates_extension,
    register_bundles,
)


def log(msg: str) -> None:
    print("[r39c %s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", default="ABCDEXGHV",
                    help="any subset of ABCDEXGHV, run in order")
    args = ap.parse_args()
    phases = args.phase.upper()

    log("inheriting the v1 search burden (never resets)")
    burden = CONT.inherit_reuse_ledger()
    log("cumulative burden: %s" % json.dumps(burden))

    log("loading frozen v1 universal state")
    state = CONT.load_frozen_state()
    log("fut=%d vx=%d eq=%d etf=%d rows"
        % (len(state["fut"]), len(state["vx"]), len(state["eq"]),
           0 if state["etf"] is None or state["etf"].empty
           else len(state["etf"])))

    log("regenerating representations (deterministic, same seeds)")
    director = Director2(state, CONT.CONTINUATION_CAMPAIGN_ID)
    director.prepare_representations()
    log("bundles=%d" % len(director.bundles))

    recon = None
    if "A" in phases:
        log("TRACK A: reconstructing %s" % WP.WIDE_ID)
        recon = WP.reconstruct(director)
        log("  proofs: %s" % json.dumps(
            {k: v for k, v in recon["proofs"].items()
             if not isinstance(v, dict)}))
        rec = WP.control_reconciliation(recon)
        log("  control reconciliation: %s" % rec["result_state"])

    if "B" in phases:
        if recon is None:
            raise SystemExit("phase B needs phase A in the same run")
        log("TRACK B: factor residualisation")
        fac = WP.factor_residualisation(director, recon)
        full = fac["full_model"]
        log("  residual alpha %.4f/yr t=%.2f R2=%.2f (n=%s months)"
            % (full.get("residual_alpha_annualised") or float("nan"),
               full.get("residual_alpha_t") or float("nan"),
               full.get("r_squared") or float("nan"),
               full.get("n_months")))

    if "C" in phases:
        log("TRACK C: group/cluster kill tests")
        kills = WP.group_kill_tests(director)
        log("  kills=%d sign_flips=%s"
            % (kills["n_kills"], kills["sign_flips"]))

    if "D" in phases:
        log("TRACK D: information attribution")
        att = WP.information_attribution(director)
        for fam, row in att["additive_increments"].items():
            inc = row["paired_increment_vs_base"]
            log("  BASE+%s: t=%.2f inc_t=%s"
                % (fam, row.get("zone_b_t") or float("nan"),
                   inc.get("incremental_t")))

    cont = CC.Continuation(director)
    if set("EXGV") & set(phases):
        log("attaching owned information families (Track E loaders)")
        CC.attach_information(state, cont)
        log("  provenance: %s" % json.dumps(
            {k: (v.get("state") or "OK") if isinstance(v, dict) else "OK"
             for k, v in cont.provenance.items()}))
        log("building the international-rates extension (canonical R38 "
            "builder)")
        intl_ext = build_intl_rates_extension(state)
        log("  intl rates: %s rows=%s"
            % (intl_ext.get("state"), intl_ext.get("rows")))
        register_bundles(director)

    if "E" in phases:
        log("PHASE E1: the 12 DATA_AVAILABLE_BUT_NOT_TESTED cells")
        cells = cont.run_cells(intl_ext)
        for row in cells["cells"]:
            log("  %s: %s best_t=%s"
                % (row["r36_cell"], row["execution_state"],
                   row.get("best_zone_b_t")))
        log("PHASE E2: paired information increments")
        info = cont.run_info()
        for fam, body in info["families"].items():
            log("  %s: protocol=%s" % (fam, body.get("protocol")))

    if "X" in phases:
        log("PHASE X: trade-expression frontier extras")
        expr = cont.run_expressions()
        for k, v in expr["results"].items():
            log("  %s: t=%s" % (k, v.get("after_cost_excess_t_stat")))
        log("PHASE X2: representation repair (latent/graph)")
        rep2 = cont.run_representation_repair()
        for k, v in rep2["results"].items():
            log("  %s: t=%s" % (k, v.get("after_cost_excess_t_stat")))

    if "G" in phases:
        log("PHASE G: $0 model-frontier completion")
        models = cont.run_models()
        log("  torch: %s" % json.dumps(models["torch"]))
        for k, v in sorted(models["results"].items()):
            log("  %s: t=%s" % (k, v.get("after_cost_excess_t_stat")))

    if "H" in phases:
        log("PHASE H: research-shadow registration + prospective design")
        from paper_trader.alpha_agent.r39 import (
            prospective_design as PD,
            research_shadow as RS,
        )
        reg = RS.register(director)
        log("  shadows frozen: %d at %s"
            % (reg["n_shadows"], reg["frozen_at"]))
        des = PD.freeze()
        log("  prospective design frozen: %s"
            % des["prospective_design_hash"][:16])

    if "V" in phases:
        log("PHASE V: registry, pre-gate, cumulative ledger, verdict")
        camp = R39.campaign_dir(CONT.CONTINUATION_CAMPAIGN_ID)
        wide_artifacts = {
            "recon": R39.read_json(camp / WP.RECON_NAME) or {},
            "factor": R39.read_json(camp / WP.FACTOR_NAME) or {},
            "kills": R39.read_json(camp / WP.KILL_NAME) or {},
        }
        verdict = cont.finalise(wide_artifacts=wide_artifacts)
        log("  verdict: %s" % verdict["verdict"])
        log("  pregate: %s" % verdict["pregate"]["decision"])

    log("cumulative burden now: %s"
        % json.dumps(CONT.cumulative_burden()))
    log("phases %s complete" % phases)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
