r"""Execute the frozen R65 campaign spec, in the frozen review order.

RESEARCH ONLY. PAPER ONLY. NO ORDERS, NO FILLS, NO PROMOTION, NO ADOPTION.

    & .\.venv-win\Scripts\python.exe research\agents\campaign_r65_non_equity\run_campaign.py
    & .\.venv-win\Scripts\python.exe research\agents\campaign_r65_non_equity\run_campaign.py --only r65_01
    & .\.venv-win\Scripts\python.exe research\agents\campaign_r65_non_equity\run_campaign.py --precheck-only

This is NOT a second runner. Measurement is delegated in full to
``alpha_agent.agents_v2.runner.run_experiment``, every durable write goes
through ``AgentPipeline`` into the ONE research memory, and every verdict comes
from ``alpha_agent.r59.engines``.

What it adds is the one thing the generic runner cannot do: a cell that fails a
PRE-MEASUREMENT gate raises out of its executor, and the runner can only report
that as ``FAILED``. A failed executor is not a result. This driver catches the
two declared gate failures and settles them through
``AgentPipeline.record_pre_measurement_halt``, so the cell is CHARGED to the
search burden and carries a reason and a reopen condition, instead of staying
open in the memory forever the way four of R60's eight cells did.

EXECUTION ORDER IS THE FROZEN REVIEW ORDER, and that is load-bearing: a halted
cell settles and charges the burden denominator exactly like a reviewed one, so
the strongest idea must pay the smallest denominator.

TERMINAL TOKEN (exactly one, on the last line)
    R65_CAMPAIGN_OK <measured>/<halted>/<refused>
    R65_CAMPAIGN_FAILED <reason>
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[2]
for _p in (str(_REPO), str(_HERE.parent)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from alpha_agent import r59                                    # noqa: E402
from alpha_agent.agents_v2 import runner as R                  # noqa: E402
from alpha_agent.agents_v2.pipeline import (AgentPipeline,     # noqa: E402
                                            PipelineRefusal)
from alpha_agent.r59 import memory as M                        # noqa: E402

SPEC = _HERE / "campaign_spec.json"
RESULTS = _HERE / "results.json"
PRECHECK = _HERE / "PRECHECK.json"

#: Executor exceptions that are RESULTS, mapped to the declared halt reason.
#: Anything else is a real failure and is reported as one.
GATE_FAILURES = ("CoverageFloorFailed", "CostBudgetFailed")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _halt_fields(exc) -> dict:
    """The measured number and the frozen threshold that refused it."""
    from campaign_r65_non_equity import executors as X

    name = type(exc).__name__
    if name == "CoverageFloorFailed":
        return {"halt_reason": X.CoverageFloorFailed.halt_reason,
                "measured_metric": "usable_open_interest_coverage",
                "frozen_threshold": X.OI_COVERAGE_FLOOR,
                "frozen_threshold_owner":
                    "quant-research-director, frozen 2026-09-22 before any "
                    "R65 measurement existed"}
    return {"halt_reason": X.CostBudgetFailed.halt_reason,
            "measured_metric": "annualized_cost_drag",
            "frozen_threshold": X.COST_BUDGET_CEILING,
            "frozen_threshold_owner": "alpha_agent.r61.cost_budget"}


def _measured_value(exc, plan_state: dict):
    name = type(exc).__name__
    if name == "CoverageFloorFailed":
        return (plan_state.get("coverage") or {}).get("coverage")
    return ((plan_state.get("cost_precheck") or {}).get("verdict") or {}
            ).get("annualized_cost_drag")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--only", action="append", default=None,
                    help="executor name, repeatable")
    ap.add_argument("--precheck-only", action="store_true",
                    help="run the pre-measurement gates and stop")
    ap.add_argument("--memory", default="", help="research memory (tests)")
    args = ap.parse_args(argv)

    try:
        spec = R.load_campaign_spec(SPEC)
        executors = R.load_executors(spec["executor_module"])
        mem = M.ResearchMemory(Path(args.memory) if args.memory else None)
        pipe = AgentPipeline(mem)

        order = spec.get("review_order") or [r["executor"]
                                             for r in spec["experiments"]]
        rows = {r["executor"]: r for r in spec["experiments"]}
        todo = [rows[n] for n in order
                if n in rows and (args.only is None or n in set(args.only))]

        results, prechecks = [], []
        for row in todo:
            eid = row["experiment_id"]
            name = row["executor"]
            if not eid or eid.startswith("REFUSED_"):
                prechecks.append({"executor": name, "state": "NOT_REGISTERED",
                                  "experiment_id": eid,
                                  "note": "refused by the director before "
                                          "registration; carries no id and "
                                          "charges no burden"})
                continue
            exp = mem.get(eid)
            if exp is None:
                prechecks.append({"executor": name, "state": "UNKNOWN",
                                  "experiment_id": eid})
                continue
            spec_hash = r59.stable_hash(exp.get("spec") or {})
            agent = (exp.get("spec") or {}).get("owning_agent")

            # ---- the pre-measurement gates -------------------------------
            fn = executors[name]
            try:
                plan = fn(row)
            except Exception as exc:                          # noqa: BLE001
                kind = type(exc).__name__
                if kind not in GATE_FAILURES:
                    prechecks.append({"executor": name, "experiment_id": eid,
                                      "state": "EXECUTOR_FAILED",
                                      "error": kind, "detail": str(exc)[:400]})
                    continue
                from campaign_r65_non_equity import executors as X
                state = dict(X._CACHE.get(("built", name)) or {})
                fields = _halt_fields(exc)
                try:
                    rec = pipe.perform(agent, "record_pre_measurement_halt",
                                       dict(experiment_id=eid,
                                            spec_hash=spec_hash,
                                            detail=str(exc)[:900],
                                            measured_value=_measured_value(
                                                exc, state),
                                            **fields))
                except PipelineRefusal as ref:
                    rec = {"refused": ref.code, "detail": str(ref)[:300]}
                prechecks.append({"executor": name, "experiment_id": eid,
                                  "state": "PRE_MEASUREMENT_HALT",
                                  "halt": fields["halt_reason"],
                                  "measured_value": _measured_value(exc, state),
                                  "frozen_threshold": fields[
                                      "frozen_threshold"],
                                  "recorded": rec,
                                  "message": str(exc)[:600]})
                continue

            prechecks.append({
                "executor": name, "experiment_id": eid, "state": "GATES_PASSED",
                "coverage": plan.get("coverage"),
                "cost_precheck": plan.get("cost_precheck"),
                "universe_id": plan.get("universe_id"),
                "min_markets": plan.get("min_markets"),
                "demean_key": plan.get("demean_key")})

            if args.precheck_only:
                continue

            # ---- measurement, delegated in full --------------------------
            out = R.run_experiment(pipe, row, executors, spec_hash=spec_hash,
                                   campaign_id=spec["campaign_id"],
                                   artifact_dir=_HERE / "artifacts")
            if out.get("layers"):
                out["cost_parity"] = __import__(
                    "campaign_r65_non_equity.executors", fromlist=["x"]
                ).cost_parity(plan.get("cost_precheck") or {}, out["layers"])
            results.append(out)

        body = {"campaign_id": spec["campaign_id"],
                "run_id": "R65_MULTI_ASSET_ALPHA_AND_PNL_OFFENSIVE",
                "generated_at": _now(),
                "runner": R.RUNNER_VERSION,
                "calculation_owner": R.CALCULATION_OWNER,
                "execution_order": [r["executor"] for r in todo],
                "execution_order_rule": spec.get("review_order_rule"),
                "safety": spec.get("safety"),
                "prechecks": prechecks,
                "results": results}
        PRECHECK.write_text(json.dumps(
            {k: body[k] for k in ("campaign_id", "generated_at",
                                  "execution_order", "prechecks")},
            indent=1, default=str), encoding="utf-8")
        if not args.precheck_only:
            RESULTS.write_text(json.dumps(body, indent=1, default=str),
                               encoding="utf-8")
        print(json.dumps(body, indent=1, default=str))

        measured = sum(1 for r in results if r["state"] == R.RUN_MEASURED)
        halted = (sum(1 for r in results if r["state"] == R.RUN_HALTED)
                  + sum(1 for p in prechecks
                        if p["state"] == "PRE_MEASUREMENT_HALT"))
        refused = (sum(1 for r in results
                       if r["state"] in (R.RUN_REFUSED, R.RUN_FAILED))
                   + sum(1 for p in prechecks
                         if p["state"] == "EXECUTOR_FAILED"))
        print("R65_CAMPAIGN_OK %d/%d/%d" % (measured, halted, refused))
        return 0
    except Exception as exc:                                   # noqa: BLE001
        print(json.dumps({"failed": type(exc).__name__,
                          "detail": str(exc)[:1200]}, indent=1))
        print("R65_CAMPAIGN_FAILED %s" % type(exc).__name__)
        return 1


if __name__ == "__main__":
    sys.exit(main())
