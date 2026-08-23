"""Release 40 runner - Prospective Alpha Acceleration (campaign
r40_prospective_alpha_acceleration_v1). RESEARCH ONLY.

Phases (run in the order given; each is idempotent through artifact
immutability or explicit cache hits):
  I  - R39 closeout import + burden inheritance (194, never reset)
  E  - availability integrity + corrected WIDE successor (Track E)
  F  - NY Fed legacy dealer-positioning bridge + paired increment (Track F)
  G  - open-weight model frontier: inventory, provenance, contamination
       (Track G)
  H  - temporal / relational model challenge under the same protocol
       (Track H)
  R  - cross-asset relational research (Track I)
  S  - Slot-4 / Slot-5 resolution + SHADOW_REGISTRY_V2 (Track D)
  Q  - always-valid prospective designs + family error budget (Track C)
  V  - evidence velocity registry + effective-sample analysis (Track B)
  P  - prospective research portfolio (Track K)
  L  - Intrinio sample readiness (Track L)
  M  - compute escalation request (Track M)
  Z  - cumulative ledger + joint evidence + final verdict

Usage (Windows PowerShell):
  & C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe `
      C:\\Users\\binis\\paper_trader\\scripts\\run_release40_prospective_alpha.py `
      --phase IE
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.alpha_agent import r40 as R40  # noqa: E402
from paper_trader.alpha_agent.r40 import burden_ledger as BL  # noqa: E402
from paper_trader.alpha_agent.r40 import closeout_import as CI  # noqa: E402


def log(msg: str) -> None:
    print("[r40 %s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", default="I")
    args = ap.parse_args()
    phases = args.phase.upper()
    log("campaign dir %s" % R40.campaign_dir())

    imp = CI.run()
    log("R39 closeout import: %s (mismatches=%d)"
        % (imp["state"], len(imp["mismatches"])))
    if imp["state"] != "R39_VERIFIED":
        log("DO_NOT_START_R40 - R39_NOT_FINALIZED")
        return 2
    log("burden: %s" % json.dumps(BL.inherit()))

    if "E" in phases:
        from paper_trader.alpha_agent.r40 import wide_successor as WS
        log("PHASE E: availability integrity + corrected WIDE successor")
        body = WS.build()
        for b, r in body["results"].items():
            log("  %s: t=%s exc=%s n_feat=%d"
                % (b, r["zone_b"].get("after_cost_excess_t_stat"),
                   r["zone_b"].get("after_cost_excess_annualised"),
                   r["n_features"]))
        log("  excluded by rule: %s" % body["excluded_by_rule"])
        log("  best: %s" % json.dumps(body["best_successor"]))

    if "F" in phases:
        from paper_trader.alpha_agent.r40 import nyfed_bridge as NY
        log("PHASE F: NY Fed legacy bridge")
        mp = NY.build_mapping()
        log("  mapping state: %s concepts=%d"
            % (mp["state"], len(mp["concepts"])))
        inc = NY.run_increment()
        log("  increment: %s" % json.dumps(inc.get("headline")))

    if "G" in phases:
        from paper_trader.alpha_agent.r40 import open_models as OM
        log("PHASE G: open-weight model frontier")
        reg = OM.build_registry()
        for k, v in reg["models"].items():
            log("  %s: %s / %s" % (k, v.get("decision"),
                                   v.get("contamination_class")))
        prov = OM.acquire()
        log("  acquired: %s" % json.dumps(
            {k: v.get("state") for k, v in prov["weights"].items()}))

    if "H" in phases:
        from paper_trader.alpha_agent.r40 import model_challenge as MC
        log("PHASE H: temporal / relational model challenge")
        res = MC.run()
        for k, v in res["results"].items():
            log("  %s: t=%s" % (k, (v.get("zone_b") or {}).get(
                "after_cost_excess_t_stat")))

    if "R" in phases:
        from paper_trader.alpha_agent.r40 import cross_asset as XA
        log("PHASE R: cross-asset relational research")
        res = XA.run()
        log("  edges screened=%s kept=%s; headline=%s"
            % (res["screen"]["n_pairs_screened"],
               res["screen"]["n_edges_kept"],
               json.dumps(res.get("headline"))))

    if "S" in phases:
        from paper_trader.alpha_agent.r40 import shadow_registry as SR
        log("PHASE S: slot resolution + SHADOW_REGISTRY_V2")
        reg = SR.freeze()
        log("  shadows=%d slot5=%s"
            % (reg["n_shadows"], json.dumps(reg["slot_5_resolution"].get(
                "winner"))))

    if "Q" in phases:
        from paper_trader.alpha_agent.r40 import sequential as SQ
        log("PHASE Q: prospective validation designs + error budget")
        des = SQ.freeze_designs()
        log("  designs=%d budget=%s"
            % (len(des["designs"]), json.dumps(des["family_error_budget"])))

    if "V" in phases:
        from paper_trader.alpha_agent.r40 import evidence_velocity as EV
        log("PHASE V: evidence velocity")
        ev = EV.build()
        for k, v in ev["registry"].items():
            log("  %s: months_to_success(point)=%s ess_ratio=%s"
                % (k, (v.get("time_to_decision") or {}).get(
                    "point_estimate"), v.get("effective_sample_ratio")))

    if "P" in phases:
        from paper_trader.alpha_agent.r40 import research_portfolio as RP
        log("PHASE P: prospective research portfolio")
        rp = RP.build()
        log("  redundancy: %s" % json.dumps(rp.get("redundant_pairs")))

    if "L" in phases:
        from paper_trader.alpha_agent.r40 import intrinio_readiness as IR
        log("PHASE L: Intrinio sample readiness")
        ir = IR.build()
        log("  state: %s" % ir["state"])

    if "M" in phases:
        from paper_trader.alpha_agent.r40 import compute_escalation as CE
        log("PHASE M: compute escalation")
        ce = CE.build()
        log("  requests: %d" % len(ce["requests"]))

    if "Z" in phases:
        from paper_trader.alpha_agent.r40 import campaign as CP
        log("PHASE Z: ledger + joint evidence + verdict")
        v = CP.finalise()
        log("  verdict: %s" % json.dumps(v["terminal_states"]))
        log("  burden: %s" % json.dumps(v["cumulative_burden"]))

    log("burden now: %s" % json.dumps(BL.summary()))
    log("phases %s complete" % phases)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
