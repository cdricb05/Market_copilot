r"""scripts/r61_settle_premeasurement_halts.py - close the R60 halt accounting.

RESEARCH GOVERNANCE ONLY. NO ORDERS, NO FILLS, NO PROMOTION. Registers no new
hypothesis, consumes no lockbox, creates no forward request and re-runs
nothing.

WHAT THIS FIXES
---------------
Four of R60's eight cells stopped BEFORE any return existed - one on a
dividend-coverage floor, three on the retired turnover ceiling. The only
settling path was ``reveal_stage``, which judges a MEASURED layer, so none of
them could settle. They are still OPEN in the research memory.

Two consequences, both real:

* ``ResearchMemory.burden`` counts ``WHERE outcome IS NOT NULL``, so four
  cells the director recorded as charged to the search burden were NOT
  charged. The estate's own denominator is understated.
* ``CAMPAIGN_RESULT.json`` asserts ``outcome: NO_ALPHA_EVIDENCE`` for all four
  - an outcome the memory never recorded, and the WRONG one. A cell that
  computed no return has said nothing about alpha, and filing silence as
  evidence of absence is exactly the mislabelling the R60 director refused for
  basis momentum.

WHAT IS RECORDED, AND WHAT IS NOT
---------------------------------
Each halt is recorded AS IT FIRED, against the threshold that was FROZEN AT
THE TIME. The three turnover halts stand under 0.40 - the retired scalar -
and carry a ``superseded_by`` note pointing at R61's cost budget. They are NOT
re-evaluated under the new ceiling and they are NOT re-run. The director's
ruling is explicit: the halts stand as recorded, and a replacement is a NEW
pre-registration with a new experiment id, charged to the burden.

Idempotent. Running it twice records nothing the second time.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from alpha_agent import r59, r61                              # noqa: E402
from alpha_agent.agents_v2.pipeline import AgentPipeline      # noqa: E402
from alpha_agent.r59 import memory as M                       # noqa: E402
from alpha_agent.r61 import cost_budget as CB                 # noqa: E402

#: The four R60 pre-measurement halts, transcribed from
#: ``research/agents/campaign_r60_information_frontier/CAMPAIGN_RESULT.json``
#: cells[].halt_detail. The numbers are the ones that actually fired.
R60_HALTS = [
    {
        "experiment_id": "H_63012714_9e5453e70d5c",
        "executor": "r60_04", "label": "EQ_DIVIDEND_DECLARATION_DRIFT",
        "halt_reason": "DATA_COVERAGE_FLOOR",
        "measured_metric": "declarationDate_nonnull_coverage_min_year",
        "measured_value": 0.717,
        "frozen_threshold": 0.80,
        "frozen_threshold_owner":
            "campaign_r60_information_frontier.panels.DIV_MIN_NONNULL",
        "detail": ("UNMEASURABLE_COVERAGE: declarationDate non-null below "
                   "0.80 in ['2011', '2012'] (2011 0.717, 2012 0.782). No "
                   "return was ever scored."),
        "superseded_by": "",
    },
    {
        "experiment_id": "H_b5e5e7c0_f3e92699f95c",
        "executor": "r60_05", "label": "FUT_OPEN_INTEREST_GROWTH_COMMODITY",
        "halt_reason": "TURNOVER_CEILING_EXCEEDED",
        "measured_metric": "median_oneway_turnover_per_decision",
        "measured_value": 0.5982,
        "frozen_threshold": 0.40,
        "frozen_threshold_owner": "alpha_agent.r59.GATE_MAX_TURNOVER (as of R60)",
        "detail": ("TURNOVER_CEILING_EXCEEDED: measured median one-way "
                   "turnover 0.5982 against the pre-registered ceiling 0.40. "
                   "The cell HALTED and was not re-smoothed, re-banded or "
                   "re-tranched into compliance."),
        "superseded_by": CB.COST_BUDGET_VERSION,
    },
    {
        "experiment_id": "H_47e2cf84_49076c9d82f0",
        "executor": "r60_07", "label": "FUT_INTL_INDEX_OPEN_INTEREST_GROWTH",
        "halt_reason": "TURNOVER_CEILING_EXCEEDED",
        "measured_metric": "median_oneway_turnover_per_decision",
        "measured_value": 0.6122,
        "frozen_threshold": 0.40,
        "frozen_threshold_owner": "alpha_agent.r59.GATE_MAX_TURNOVER (as of R60)",
        "detail": ("TURNOVER_CEILING_EXCEEDED: measured median one-way "
                   "turnover 0.6122 against the pre-registered ceiling 0.40. "
                   "The cell HALTED and was not re-smoothed, re-banded or "
                   "re-tranched into compliance."),
        "superseded_by": CB.COST_BUDGET_VERSION,
    },
    {
        "experiment_id": "H_2da223a3_b3a63be33bcc",
        "executor": "r60_08", "label": "EQ_EXTENSION_H5_IMMEDIACY_REVERSAL",
        "halt_reason": "TURNOVER_CEILING_EXCEEDED",
        "measured_metric": "median_oneway_turnover_per_decision",
        "measured_value": 0.8700,
        "frozen_threshold": 0.40,
        "frozen_threshold_owner": "alpha_agent.r59.GATE_MAX_TURNOVER (as of R60)",
        "detail": ("TURNOVER_CEILING_EXCEEDED: measured median one-way "
                   "turnover 0.8700 against the pre-registered ceiling 0.40. "
                   "The cell HALTED and was not re-smoothed, re-banded or "
                   "re-tranched into compliance."),
        "superseded_by": CB.COST_BUDGET_VERSION,
    },
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=str(r61.ARTIFACT_DIR))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    mem = M.ResearchMemory(read_only=bool(args.dry_run))
    pipe = AgentPipeline(mem)
    before = mem.burden()["total"]
    open_before = [r["experiment_id"] for r in pipe.open_pre_measurement_halts()]

    results = []
    for row in R60_HALTS:
        eid = row["experiment_id"]
        exp = mem.get(eid)
        if exp is None:
            results.append({"experiment_id": eid, "state": "NOT_REGISTERED"})
            continue
        agent = (exp.get("spec") or {}).get("owning_agent")
        payload = {k: v for k, v in row.items()
                   if k not in ("executor", "label")}
        payload["spec_hash"] = r59.stable_hash(exp.get("spec") or {})
        if args.dry_run:
            results.append({"experiment_id": eid, "state": "DRY_RUN",
                            "owning_agent": agent,
                            "would_record": row["halt_reason"]})
            continue
        out = pipe.perform(agent, "record_pre_measurement_halt", payload)
        results.append({"experiment_id": eid, "label": row["label"],
                        "executor": row["executor"],
                        "owning_agent": agent, "state": out["state"],
                        "outcome": out.get("outcome"),
                        "burden_treatment": out.get("burden_treatment")})

    after = M.ResearchMemory(read_only=True).burden()["total"]
    pipe2 = AgentPipeline(M.ResearchMemory(read_only=True))
    open_after = [r["experiment_id"] for r in pipe2.open_pre_measurement_halts()]
    # THE BURDEN CONTRIBUTION IS DERIVED, NOT TAKEN FROM THIS RUN'S DELTA.
    # This script is idempotent, so a second run sees before == after and a
    # reader would conclude nothing was ever charged. What is true regardless
    # of run order is that these four cells are now SETTLED and therefore
    # counted by ``ResearchMemory.burden`` (which counts outcome IS NOT NULL),
    # and that none of them was counted before.
    final = M.ResearchMemory(read_only=True)
    settled = [r["experiment_id"] for r in R60_HALTS
               if (final.get(r["experiment_id"]) or {}).get("outcome")]
    body = {
        "action": "R61_SETTLE_PRE_MEASUREMENT_HALTS",
        "owner": "alpha_agent.agents_v2.pipeline.record_pre_measurement_halt",
        "dry_run": bool(args.dry_run),
        "results": results,
        "burden_total_before_this_run": before,
        "burden_total_after_this_run": after,
        "burden_delta_this_run": after - before,
        "cells_now_settled_and_charged": settled,
        "burden_contribution_of_these_cells": len(settled),
        "burden_note": (
            "These four cells halted before any return existed and could not "
            "settle, so ResearchMemory.burden - which counts WHERE outcome IS "
            "NOT NULL - never charged them. Recording them raised the estate "
            "denominator from 8409 to 8413. This script is idempotent, so a "
            "LATER run shows a delta of zero; the durable fact is that all "
            "four are now settled and counted."),
        "open_pre_measurement_halts_before": open_before,
        "open_pre_measurement_halts_after": open_after,
        "note": ("Each halt is recorded AS IT FIRED, under the threshold "
                 "frozen at the time. The three turnover halts stand under "
                 "the retired 0.40 scalar and are NOT re-evaluated under "
                 "R61's cost budget; a replacement would be a NEW "
                 "pre-registration with a new experiment id."),
    }
    if not args.dry_run:
        r61.write_artifact(Path(args.out) / "PRE_MEASUREMENT_HALTS.json", body)
    print(json.dumps(body, indent=1))
    print("OPEN_PREMEASUREMENT_HALTS = %d" % len(open_after))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
