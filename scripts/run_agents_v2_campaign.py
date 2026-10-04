r"""scripts/run_agents_v2_campaign.py - execute a FROZEN campaign spec locally.

The deterministic half of PAPER_TRADER_ALPHA_AGENTS_V2. The director freezes a
campaign spec; this command measures every experiment in it - universes,
features, books, sequential D/V/L slicing, cost arithmetic, cost-neutral
placebos, doubled cost, subperiods - and prints ONE compact machine-readable
summary per experiment for the agents to review.

    & .\.venv-win\Scripts\python.exe scripts\run_agents_v2_campaign.py `
        --spec research\agents\campaign_r57_wave2\campaign_spec.json `
        --out  research\agents\campaign_r57_wave2\results.json

    & .\.venv-win\Scripts\python.exe scripts\run_agents_v2_campaign.py `
        --spec <spec.json> --only H_abc123_def456

    & .\.venv-win\Scripts\python.exe scripts\run_agents_v2_campaign.py `
        --spec <spec.json> --plan-only

``--plan-only`` reads the spec and the memory and measures NOTHING: it reports
which experiments are pre-registered, which are already settled and which would
run. It is the safe way to check a spec before spending compute.

``--preregister <cell_id>`` (R100 repair, FULL_RECIPE_V1 campaigns only) is the
ONE pre-registration path of a full-recipe campaign. It reads the cell from the
spec's ``cells`` list, snapshots the source into the spec's
``frozen_source_store`` AUTOMATICALLY, builds the plan WITHOUT reading any
layer, freezes the eight-part recipe and pre-registers it through the
pipeline. It then appends the experiment row to the spec and records a work
event in the campaign work log (``agents_v2.campaign_clock``).

Every measurement run records a runner span in the campaign folder's
``WORK_LOG.jsonl``. That is real working time; wall-clock time is never counted.

IDEMPOTENT. An experiment that has already been measured is reported
ALREADY_MEASURED and is not measured again - one experiment, one result, and a
second draw needs a second pre-registration.

SEQUENTIAL. Discovery is measured first; validation only if discovery earned
it; the lockbox only if validation earned it. An experiment that halts settles
where it halted and its lockbox is NEVER computed.

RESEARCH ONLY. Every durable write goes through
``alpha_agent.agents_v2.pipeline``, which writes the one research memory. There
is no verb here that can create an order, a fill, a proposal approval, a model
promotion, a sleeve eligibility or a forward clock. Adoption remains the single
operator door, ``scripts/adopt_prospective_freeze.py``.

TERMINAL TOKENS (exactly one, on the last line)
    CAMPAIGN_OK <n measured>/<n experiments>
    CAMPAIGN_REFUSED <code>        the spec is not executable (exit 3)
    CAMPAIGN_FAILED <reason>       an unexpected failure (exit 1)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))


def _emit(body) -> None:
    print(json.dumps(body, indent=1, default=str))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--spec", required=True, help="frozen campaign spec JSON")
    ap.add_argument("--out", default="", help="write the full result bundle")
    ap.add_argument("--artifacts", default="",
                    help="directory for per-experiment artifacts")
    ap.add_argument("--only", nargs="*", default=None,
                    help="experiment ids to run (default: all)")
    ap.add_argument("--plan-only", action="store_true",
                    help="report what WOULD run; measure nothing")
    ap.add_argument("--memory", default="",
                    help="research memory path (tests); default = R59 root")
    ap.add_argument("--preregister", default="",
                    help="cell_id to pre-register under FULL_RECIPE_V1")
    args = ap.parse_args(argv)

    try:
        from alpha_agent import r59                          # type: ignore
        from alpha_agent.agents_v2 import campaign_clock as CK  # type: ignore
        from alpha_agent.agents_v2 import pipeline as P      # type: ignore
        from alpha_agent.agents_v2 import provenance as PV   # type: ignore
        from alpha_agent.agents_v2 import runner as R        # type: ignore
        from alpha_agent.r59 import memory as M              # type: ignore
        r59.assert_worktree_import()

        db = Path(args.memory) if args.memory else None
        spec_path = Path(args.spec)
        work_dir = spec_path.resolve().parent

        if args.preregister:
            raw = json.loads(spec_path.read_text(encoding="utf-8-sig"))
            if raw.get("provenance_contract") != PV.PROVENANCE_CONTRACT_FULL:
                raise R.CampaignRefusal(
                    "--preregister serves %s campaigns only"
                    % PV.PROVENANCE_CONTRACT_FULL)
            prov = R.campaign_provenance(raw)
            cell = next((c for c in raw.get("cells") or []
                         if c.get("cell_id") == args.preregister), None)
            if cell is None:
                raise R.CampaignRefusal("cell %s is not in the spec's cells"
                                        % args.preregister)
            if any(e.get("cell_id") == cell["cell_id"]
                   for e in raw.get("experiments") or []):
                raise R.CampaignRefusal("cell %s is already pre-registered"
                                        % cell["cell_id"])
            # The admission door binds pre-registration too: no G7 PASS, no id.
            PV.require_admission(cell["cell_id"], prov["admission_rulings"])
            payload = json.loads(R._campaign_path(cell["payload"]).read_text(
                encoding="utf-8-sig"))
            payload.setdefault("campaign_id", raw["campaign_id"])
            row = {"executor": cell["executor"], "book": cell["book"],
                   "cell_id": cell["cell_id"]}
            mem = M.ResearchMemory(db)
            pipe = P.AgentPipeline(mem)
            executors = R.load_executors(raw["executor_module"])
            with CK.span(work_dir, "preregister %s" % cell["cell_id"],
                         actor="quant-research-director"):
                res = R.preregister_frozen(
                    pipe, director="quant-research-director", row=row,
                    payload=payload, executors=executors,
                    store_dir=prov["frozen_source_store"],
                    source_paths=[raw["executor_module"]]
                    + list(cell.get("source_paths") or []))
            raw.setdefault("experiments", []).append(
                {"experiment_id": res["experiment_id"],
                 "executor": cell["executor"], "book": cell["book"],
                 "cell_id": cell["cell_id"],
                 "recipe_sha256": res["recipe_sha256"],
                 "frozen_source_manifest_sha256":
                     res["frozen_source_manifest_sha256"]})
            tmp = spec_path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(raw, indent=1), encoding="utf-8")
            tmp.replace(spec_path)
            CK.record_work(work_dir, activity="preregistered %s as %s"
                           % (cell["cell_id"], res["experiment_id"]),
                           artifact=spec_path, actor="quant-research-director")
            _emit(res)
            print("CAMPAIGN_OK preregistered %s" % res["experiment_id"])
            return 0

        spec = R.load_campaign_spec(args.spec)

        if args.plan_only:
            mem = M.open_memory_readonly(db)
            rows = []
            for row in spec["experiments"]:
                exp = mem.get(row["experiment_id"])
                rows.append({
                    "experiment_id": row["experiment_id"],
                    "executor": row["executor"], "book": row["book"],
                    "preregistered": exp is not None,
                    "already_settled": bool(exp and exp.get("outcome")),
                    "outcome": (exp or {}).get("outcome"),
                    "would_run": bool(exp and not exp.get("outcome"))})
            _emit({"campaign_id": spec["campaign_id"], "plan_only": True,
                   "measured": 0, "experiments": rows})
            print("CAMPAIGN_OK 0/%d" % len(rows))
            return 0

        mem = M.ResearchMemory(db)
        pipe = P.AgentPipeline(mem)
        out = R.run_campaign(
            pipe, spec, only=args.only,
            artifact_dir=Path(args.artifacts) if args.artifacts else None,
            work_dir=work_dir)
        if args.out:
            Path(args.out).write_text(
                json.dumps(R._clean(out), indent=1, default=str),
                encoding="utf-8")
            try:
                CK.record_work(work_dir, activity="campaign results %s"
                               % Path(args.out).name, artifact=args.out,
                               actor=R.CALCULATION_OWNER)
            except CK.ClockRefusal:
                pass        # an unchanged replay is no new work
        _emit({k: v for k, v in out.items() if k != "results"})
        for r in out["results"]:
            _emit(r)
        measured = out["by_state"].get(R.RUN_MEASURED, 0)
        print("CAMPAIGN_OK %d/%d" % (measured, out["experiments"]))
        return 0
    except Exception as exc:                                 # noqa: BLE001
        name = type(exc).__name__
        if name in ("CampaignRefusal", "PipelineRefusal", "ProvenanceRefusal",
                    "ClockRefusal"):
            _emit({"refused": name, "detail": str(exc)[:600]})
            print("CAMPAIGN_REFUSED %s" % name)
            return 3
        _emit({"failed": name, "detail": str(exc)[:600]})
        print("CAMPAIGN_FAILED %s" % name)
        return 1


if __name__ == "__main__":
    sys.exit(main())
