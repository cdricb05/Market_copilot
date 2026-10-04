r"""pf1_rerun_r02_c02.py - re-evaluate the R61 cost budget of R99 R02 and C02 with the PF1 fix (READ ONLY).

No book is re-run and no return is read anew: the inputs are the lockbox layer stats R99 already
stored in results_w1.json, the frozen pre-registration payloads, and the canonical budget owner
(alpha_agent.r61.cost_budget.evaluate_frozen_spec) reached exactly as the pipeline reaches it.
Writes PF1_RERUN_R02_C02.json next to this file. Changes no verdict and no research-memory row.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(REPO))
R99 = REPO / "research" / "agents" / "campaign_r99_multi_asset_alpha_offensive"

from alpha_agent.agents_v2 import runner as RN          # noqa: E402
from alpha_agent.r61 import cost_budget as CB           # noqa: E402

TARGETS = {"H_d1181435_4f695f30a9e1": "R99_R02", "H_1b9c733e_787575045691": "R99_C02"}


def j(p: Path):
    return json.loads(p.read_text(encoding="utf-8-sig"))


def main() -> dict:
    results, source = {}, {}
    for p in sorted(R99.glob("results_*.json")):
        for r in j(p)["results"]:
            if r["experiment_id"] in TARGETS and (r.get("layers") or {}).get("L"):
                results[r["experiment_id"]] = r
                source[r["experiment_id"]] = {"file": p.name, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
    sk = j(R99 / "R99_SKEPTIC_RESULTS.json")["verdicts"]
    ceiling = float(j(REPO / "research" / "agents" / "validation_gate_schema.json")
                    ["canonical_statistical_gate"]["inherited_thresholds"]["max_annualized_cost_drag"])
    out = {"artifact": "PF1_RERUN_R02_C02", "read_only": True, "lockbox_sources": source,
           "ceiling": ceiling, "records": {}}
    for eid, cell in TARGETS.items():
        L = results[eid]["layers"]["L"]
        payload = j(R99 / "payloads" / ("%s__preregister.json" % cell))
        spec = payload.get("spec") or payload
        h = int(spec["horizon_sessions"])
        before_rate, roll = RN.effective_cost_inputs_of(L)                       # the R99 call: no horizon
        after_rate, _ = RN.effective_cost_inputs_of(L, horizon_sessions=h)
        kw = dict(one_way_turnover=L["mean_oneway_turnover_per_period"],
                  additional_ann_cost_drag=float(roll or 0.0), ceiling=ceiling)
        before = CB.evaluate_frozen_spec(spec, cost_per_side=before_rate, **kw)
        after = CB.evaluate_frozen_spec(spec, cost_per_side=after_rate, **kw)
        v = sk[eid]
        out["records"][cell] = {
            "experiment_id": eid, "horizon_sessions": h,
            "cadence_sessions": (spec.get("parameters") or {}).get("cadence_sessions"),
            "returns_unchanged": {k: L[k] for k in ("ann_gross_excess", "ann_net_excess", "t_net_excess", "p_one_sided")},
            "lockbox_measured_ann_cost_drag": L["ann_cost_drag"],
            "before_fix": {"effective_cost_per_side": before_rate, "state": before["state"], "passed": before["passed"]},
            "after_fix": {"effective_cost_per_side": after_rate, "state": after["state"], "passed": after["passed"],
                          "projected_ann_cost_drag": after.get("measured"),
                          "projected_equals_measured": (after.get("measured") is not None
                                                        and abs(after["measured"] - L["ann_cost_drag"]) < 1e-9)},
            "skeptic_verdict_recorded": v["verdict"],
            "independent_kill_reasons": v["canonical_gate"],
            "verdict_after_fix": v["verdict"],
            "verdict_changed": False,
            "why_unchanged": ("the canonical gate failed lockbox_material and burden_corrected_significant "
                              "independently of the cost budget; an evaluable budget can only add or remove "
                              "one machine check and cannot clear the statistical failures"),
        }
    (HERE / "PF1_RERUN_R02_C02.json").write_text(json.dumps(out, indent=1), encoding="utf-8")
    return out


if __name__ == "__main__":
    o = main()
    for c, r in o["records"].items():
        print(c, "before", r["before_fix"], "after", r["after_fix"], "verdict", r["verdict_after_fix"])
