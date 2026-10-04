r"""post_r99_errata.py - the R99 provenance errata, computed (READ ONLY on the R99 folder).

Writes R99_PROVENANCE_ERRATA.json next to this file. Never edits an R99 artifact: historical
pre-registrations, results and verdicts are left exactly as recorded; this file states what they
must be read with.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(REPO))
R99 = REPO / "research" / "agents" / "campaign_r99_multi_asset_alpha_offensive"

from alpha_agent.agents_v2 import provenance as PV        # noqa: E402


def j(p: Path):
    return json.loads(p.read_text(encoding="utf-8-sig"))


def wave_key(p: Path):
    # W1 < W1A < W2 < ... < W16; the AGENDA ruling carries no per-cell G7
    m = re.match(r"R99_DIRECTOR_RULING_W(\d+)([A-Z]?)", p.stem)
    return (int(m.group(1)), m.group(2))


def main() -> dict:
    rulings = sorted(R99.glob("R99_DIRECTOR_RULING_W*.json"), key=wave_key)
    spec = j(R99 / "campaign_spec.json")
    results = {}
    for f in sorted(R99.glob("results_*.json")):
        for r in j(f)["results"]:
            if r.get("layers"):
                results[r["experiment_id"]] = f.name
    measured_cells = [e["cell_id"] for e in spec["experiments"] if e["experiment_id"] in results]
    exceptions = PV.admission_exceptions(measured_cells, rulings)
    verify = j(HERE / "POST_R99_VERIFICATION.json")
    out = {
        "artifact": "R99_PROVENANCE_ERRATA",
        "rewrites_history": False,
        "ruling_order_used": [p.name for p in rulings],
        "C04_governance_exception": {
            "computed_by": "alpha_agent.agents_v2.provenance.admission_exceptions over R99's own ruling files",
            "exceptions": exceptions,
            "timeline_utc": {
                "W1_admits_C04_G7_PASS": "2026-10-04T15:40:57Z (file mtime)",
                "C04_preregistered": "2026-10-04T15:45:04Z (research memory event 193160)",
                "C04_power_assessed": "2026-10-04T15:45:05Z (event 193161)",
                "C04_D_layer_revealed": "2026-10-04T15:46:12Z (event 193186; halted at D)",
                "W1A_withdraws_C04_G1_FAIL_G7_FAIL": "2026-10-04T15:46:34Z (file mtime)"},
            "why_measured": ("W1 admitted C04 and the wave-1 batch was dispatched on that admission while the "
                             "director's W1A consistency amendment (prompted by the C01/C02 reopen evidence) was still "
                             "being written. The runner had no admission door, so nothing re-read the ruling at "
                             "measurement time; the withdrawal landed 22 s after C04's D layer was revealed."),
            "treatment": ("C04 remains in R99's MEASURED count as the guard computes it, but it is NOT a clean "
                          "admissible experiment. Its outcome (NO_ALPHA_EVIDENCE at D) is unaffected; the R99 "
                          "sensitivity without C04 (R99_PIPELINE_FINDINGS.json c04_sensitivity) changes no quota "
                          "PASS/FAIL."),
            "repair": "runner admission door + post-measurement exception list (alpha_agent/agents_v2/provenance.py)"},
        "PF4_hash_drift_corrected": verify["pf4_hash_drift"],
        "PF2_horizon": {
            "finding": "r99_cells.py CELLS literals show h=21 for wave-1 cells; an import-time daily-cadence override "
                       "set h=1 and every preregistration records horizon_sessions=1 / cadence_sessions=1",
            "historical_returns": "unchanged; measured at h=1, which is what the preregistrations state",
            "repair": "runner refuses any executed horizon/cadence that differs from the frozen pair, a frozen spec "
                      "that states no horizon, and a spec that states horizon/cadence twice with different values"},
        "F32_documentation_erratum": {
            "where": "research/agents/campaign_r99_multi_asset_alpha_offensive/r99_cells.py sig_F32 docstring",
            "states": "LONG an equal-vol basket of 6E 6J 6B 6C 6A 6S",
            "frozen_code": "legs={s: 1.0 / 6 for s in FX6} - EQUAL-NOTIONAL, 1/6 each",
            "authoritative_reading": "R99_F32 is an EQUAL-NOTIONAL G6 basket. The docstring is wrong; the frozen code is "
                                     "authoritative. R99_GATE_LEDGER.json already records the inconsistency in F32's G1 note.",
            "historical_result": "F32 was REFUSED before measurement (no return read); nothing changes",
            "not_edited_because": "editing the frozen cells file would add a further hash drift to a file 19 "
                                  "preregistrations name"},
        "X04_carry_forward": {
            "superseded": "~3.4 years (assumed 70% non-zero surprise share)",
            "authoritative": "~4.5 years (observed non-zero share 0.531 x 8 meetings/yr; (36-17)/4.25 = 4.47)",
            "stale_copy": "R99_NEXT_CAMPAIGN_DRAFT.json ready_designs[R99_X04].state",
            "replacement": "POST_R99_NEXT_CAMPAIGN_DRAFT.json, generated by scripts/build_next_campaign_draft.py from "
                           "R99_CARRY_FORWARD.json"},
    }
    (HERE / "R99_PROVENANCE_ERRATA.json").write_text(json.dumps(out, indent=1), encoding="utf-8")
    return out


if __name__ == "__main__":
    o = main()
    print(json.dumps(o["C04_governance_exception"]["exceptions"], indent=0))
