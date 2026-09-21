r"""scripts/r61_annotate_evidence_state.py - annotate what a null is WORTH.

RESEARCH GOVERNANCE ONLY. NO ORDERS, NO FILLS, NO PROMOTION. Re-runs nothing,
rewrites no measured return, and changes no outcome.

WHY
---
The R60 director ruled basis momentum UNDERPOWERED and said its graveyard
entry was MISLABELLED EVIDENCE: left as it stood, a later campaign would read
NO_ALPHA_EVIDENCE as "the estate looked and the effect is not there", when
what actually happened is that three reads produced |t| <= 0.68 and the panel
could not have resolved the effect either way.

The correction he asked for is a metadata correction, and that is all this
does. Every measured number - the layers, the statistics, the turnover, the
cost model, the outcome - is re-passed UNCHANGED. The only thing added is an
``evidence_state`` block on ``robustness`` recording what R61's power
calibration measured about the panel the null was produced on.

``ResearchMemory.record_result`` rewrites every evidence column, so each one
is read back and written back verbatim. A column that came back different
would be a defect, and the script prints the comparison so a reader can see
that none did.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from alpha_agent import r61                                   # noqa: E402
from alpha_agent.r59 import memory as M                       # noqa: E402
from alpha_agent.r61 import power as PW                       # noqa: E402

NOT_TESTABLE = "NOT_TESTABLE_WITH_CURRENT_PANEL"
DEAD_IN_ESTATE = "DEAD_IN_THIS_ESTATE"

#: The two basis-momentum cells R60 measured, and the panel each was run on.
TARGETS = [
    {"experiment_id": "H_7994380d_cf2e4efaaa5a", "executor": "r60_01",
     "label": "FUT_BASIS_MOMENTUM_COMMODITY",
     "panel": "COMMODITY_FUTURES",
     "measured": "D +1.09%/yr t 0.596, V -0.82%/yr t -0.283; halted at V"},
    {"experiment_id": "H_58d49e5c_079c8b4dbce5", "executor": "r60_06",
     "label": "XA_BASIS_MOMENTUM_REPLICATION",
     "panel": "CROSS_ASSET_FUTURES",
     "measured": "D -0.89%/yr t -0.682; halted at D"},
]


def _panel_mde(power_dir: Path, panel: str) -> dict:
    body = r61.read_artifact(power_dir / ("PANEL_%s.json" % panel))
    if not body:
        raise SystemExit("no calibration result for panel %s in %s"
                         % (panel, power_dir))
    return {"panel": panel,
            "mde_50": body["mde"]["MDE_50"],
            "mde_80": body["mde"]["MDE_80"],
            "mde_90": body["mde"]["MDE_90"],
            "mde_80_at_burden_10": body["mde_at_burden_10"]["MDE_80"],
            "false_positive_rate_at_rho_zero":
                body["false_positive_rate_at_rho_zero"],
            "instrument_count": body["instrument_count"],
            "decision_count": body["decision_count"],
            "effective_observations_lockbox":
                body["effective_observations_lockbox"],
            "pre_registration_hash": body["pre_registration_hash"]}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--power-dir", default=str(r61.POWER_ROOT))
    ap.add_argument("--out", default=str(r61.ARTIFACT_DIR))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    power_dir = Path(args.power_dir)
    mem = M.ResearchMemory(read_only=bool(args.dry_run))
    rows = []
    for t in TARGETS:
        eid = t["experiment_id"]
        row = mem.get(eid)
        if row is None:
            rows.append({**t, "state": "NOT_REGISTERED"})
            continue
        mde = _panel_mde(power_dir, t["panel"])
        annotation = {
            "evidence_state": NOT_TESTABLE,
            "annotated_by": "R61_RESEARCH_APPARATUS_CALIBRATION_AND_REPAIR",
            "power_calibration_version": PW.POWER_CALIBRATION_VERSION,
            "panel_minimum_detectable_effect": mde,
            "what_this_means": (
                "This null does NOT discriminate between 'no effect' and 'an "
                "effect up to the panel's minimum detectable size'. On this "
                "panel a signal needs a cross-sectional rank IC of %.4f to be "
                "detected 80%% of the time, delivering a median lockbox net "
                "excess of %.2f%%/yr. The measured reads were: %s. Anything "
                "smaller than the MDE would have produced exactly what was "
                "observed."
                % (mde["mde_80"]["mde_rho"] or float("nan"),
                   100 * (mde["mde_80"]["mde_ann_net_excess"] or 0.0),
                   t["measured"])),
            "do_not_read_as": "EVIDENCE_OF_ABSENCE",
            "nothing_was_rewritten": (
                "Every measured column - layers, statistics, turnover, cost "
                "model - and the OUTCOME are unchanged. This is a metadata "
                "correction only."),
            "reopen_rule": (
                "A re-parameterisation on the SAME panel remains prohibited. "
                "Only a materially deeper panel - longer history or a "
                "genuinely larger instrument count, not a re-weighting - "
                "changes the answer."),
        }
        before = {k: row.get(k) for k in ("outcome", "statistic", "economics",
                                          "turnover_cost", "reason_rejected",
                                          "evidence_maturity")}
        if args.dry_run:
            rows.append({**t, "state": "DRY_RUN",
                         "current_outcome": row.get("outcome"),
                         "would_annotate": NOT_TESTABLE})
            continue
        rob = dict(row.get("robustness") or {})
        rob["power_calibration_evidence_state"] = annotation
        mem.record_result(
            eid, outcome=row["outcome"],
            evidence_maturity=row.get("evidence_maturity") or "HISTORICAL",
            statistic=row.get("statistic"), economics=row.get("economics"),
            turnover_cost=row.get("turnover_cost"), robustness=rob,
            reason_rejected=row.get("reason_rejected"),
            reopen_condition=row.get("reopen_condition"))
        after_row = M.ResearchMemory(read_only=True).get(eid)
        after = {k: after_row.get(k) for k in before}
        rows.append({**t, "state": "ANNOTATED",
                     "outcome": after_row.get("outcome"),
                     "evidence_state": NOT_TESTABLE,
                     "measured_columns_unchanged": before == after,
                     "mde_80_rho": mde["mde_80"]["mde_rho"],
                     "mde_80_ann_net_excess":
                         mde["mde_80"]["mde_ann_net_excess"]})

    body = {
        "action": "R61_ANNOTATE_EVIDENCE_STATE",
        "dry_run": bool(args.dry_run),
        "basis_momentum_evidence_state": NOT_TESTABLE,
        "vocabulary": [NOT_TESTABLE, DEAD_IN_ESTATE],
        "results": rows,
        "rule": (
            "A null is evidence of absence only ABOVE the panel's measured "
            "minimum detectable effect. Below it the null is silence, and "
            "filing silence as a finding is the mislabelling this corrects."),
    }
    if not args.dry_run:
        r61.write_artifact(Path(args.out) / "EVIDENCE_STATE.json", body)
    print(json.dumps(body, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
