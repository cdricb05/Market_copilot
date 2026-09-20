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
    args = ap.parse_args(argv)

    try:
        from alpha_agent import r59                          # type: ignore
        from alpha_agent.agents_v2 import pipeline as P      # type: ignore
        from alpha_agent.agents_v2 import runner as R        # type: ignore
        from alpha_agent.r59 import memory as M              # type: ignore
        r59.assert_worktree_import()

        spec = R.load_campaign_spec(args.spec)
        db = Path(args.memory) if args.memory else None

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
            artifact_dir=Path(args.artifacts) if args.artifacts else None)
        if args.out:
            Path(args.out).write_text(
                json.dumps(R._clean(out), indent=1, default=str),
                encoding="utf-8")
        _emit({k: v for k, v in out.items() if k != "results"})
        for r in out["results"]:
            _emit(r)
        measured = out["by_state"].get(R.RUN_MEASURED, 0)
        print("CAMPAIGN_OK %d/%d" % (measured, out["experiments"]))
        return 0
    except Exception as exc:                                 # noqa: BLE001
        name = type(exc).__name__
        if name in ("CampaignRefusal", "PipelineRefusal"):
            _emit({"refused": name, "detail": str(exc)[:600]})
            print("CAMPAIGN_REFUSED %s" % name)
            return 3
        _emit({"failed": name, "detail": str(exc)[:600]})
        print("CAMPAIGN_FAILED %s" % name)
        return 1


if __name__ == "__main__":
    sys.exit(main())
