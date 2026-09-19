r"""scripts/alpha_agents_v2.py - THE PowerShell entrypoint of PAPER_TRADER_ALPHA_AGENTS_V2.

The twelve Claude research agents own no durable state. Every handoff they make
goes through this one command, which delegates to
``alpha_agent.agents_v2.pipeline`` - a governed projection over the ONE research
memory (``alpha_agent.r59.memory``). It owns no research rule, no gate and no
forward clock.

    & .\.venv-win\Scripts\python.exe scripts\alpha_agents_v2.py status
    & .\.venv-win\Scripts\python.exe scripts\alpha_agents_v2.py validate-contracts
    & .\.venv-win\Scripts\python.exe scripts\alpha_agents_v2.py <verb> --agent <name> --input <payload.json>
    & .\.venv-win\Scripts\python.exe scripts\alpha_agents_v2.py ledger
    & .\.venv-win\Scripts\python.exe scripts\alpha_agents_v2.py survivors | validated-survivors
    & .\.venv-win\Scripts\python.exe scripts\alpha_agents_v2.py census [--out <file.json>]

VERBS  certify_data define_universe publish_features preregister submit_candidate
       skeptic_review risk_review meta_review director_clear publish_candidate
       request_forward_registration

THIS COMMAND NEVER ADOPTS. ``request_forward_registration`` writes the prospective
freeze through ``alpha_agent.r59.handlers.freeze_qualified`` and stops at
``FROZEN_AWAITING_ADOPTION_OWNER``. Paper Trader has exactly ONE operator door for
starting a forward clock - ``scripts/adopt_prospective_freeze.py``, which resolves
an operator-named challenger id against the research memory and hands it to
``api.prospective_adoption`` - and this command is deliberately not a second one:
it composes no adoption owner, takes no confirmation token and has no execute
mode. The request artifact names that door and the challenger id for the human
operator. There is no date argument and there never may be.

``status``, ``validate-contracts``, ``ledger``, ``survivors``,
``validated-survivors`` and ``census`` are READ-ONLY: they open the memory
through its read-only handle and write nothing.

RESEARCH ONLY. Nothing here can create an order or a fill, promote a model,
approve a proposal, make a sleeve capital eligible or touch an operational store.

TERMINAL TOKENS (exactly one, on the last line)
    PIPELINE_OK <verb>            the verb completed
    PIPELINE_REFUSED <code>       a governed refusal (exit 3)
    PIPELINE_FAILED <reason>      an unexpected failure (exit 1)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

READ_ONLY_COMMANDS = ("status", "validate-contracts", "ledger", "survivors",
                      "validated-survivors", "census")


def _emit(body) -> None:
    print(json.dumps(body, indent=1, default=str))


def _census(mem, pipeline, manifest: dict) -> dict:
    """What the estate already knows, read-only. No experiment is run."""
    from alpha_agent import r59                            # type: ignore

    rows = mem.list_hypotheses(limit=1000000)
    by_class: dict = {}
    fam_stats: dict = {}
    for h in rows:
        ac, oc = h.get("asset_class"), h.get("outcome") or "UNSETTLED"
        by_class.setdefault(ac, {}).setdefault(oc, 0)
        by_class[ac][oc] += 1
        key = (ac, h.get("economic_family"))
        s = fam_stats.setdefault(key, {"settled": 0, "qualified": 0,
                                       "frozen": 0, "best_t": None,
                                       "information": set()})
        s["information"].add(h.get("information_family"))
        if h.get("outcome"):
            s["settled"] += 1
        if h.get("outcome") == r59.HO_QUALIFIED:
            s["qualified"] += 1
        if h.get("outcome") == r59.HO_FORWARD_FROZEN:
            s["frozen"] += 1
        t = (h.get("statistic") or {}).get("lockbox_t")
        if isinstance(t, (int, float)) and (s["best_t"] is None
                                            or t > s["best_t"]):
            s["best_t"] = float(t)
    families = sorted(
        ({"asset_class": k[0], "economic_family": k[1],
          "settled": v["settled"], "qualified": v["qualified"],
          "forward_frozen": v["frozen"], "best_lockbox_t": v["best_t"],
          "information_families": sorted(x for x in v["information"] if x)}
         for k, v in fam_stats.items()),
        key=lambda d: (-d["settled"], str(d["economic_family"])))
    data_roots = {k: {"path": v, "present": Path(v).exists()}
                  for k, v in (manifest.get("data_roots") or {}).items()
                  if k != "rule"}
    return {
        "kind": "PREPARATORY_ALPHA_CENSUS",
        "read_only": True, "experiments_run": 0,
        "evaluation_samples_read": 0,
        "memory": mem.summary(),
        "burden": {k: v for k, v in mem.burden().items()
                   if k != "by_family"},
        "by_asset_class_outcome": by_class,
        "frontier": mem.get_frontier(),
        "families_by_effort": families[:400],
        "do_not_repeat_families": [
            f for f in families
            if f["settled"] > 0 and f["qualified"] == 0
            and f["forward_frozen"] == 0][:400],
        "strongest_unqualified": mem.strongest_unqualified(limit=25),
        "forward_frozen": [
            {"hypothesis_id": h["hypothesis_id"], "title": h.get("title"),
             "asset_class": h.get("asset_class"),
             "invalidated": bool(h.get("invalidated_reason"))}
            for h in rows if h.get("outcome") == r59.HO_FORWARD_FROZEN],
        "data_opportunities": mem.opportunities(),
        "owned_data_roots": data_roots,
        "agent_ledger_rows": len(pipeline.ledger()),
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("command")
    ap.add_argument("--agent", default="")
    ap.add_argument("--input", default="")
    ap.add_argument("--out", default="")
    ap.add_argument("--memory", default="",
                    help="research memory path (tests); default = R59 root")
    args = ap.parse_args(argv)
    cmd = args.command.replace("-", "_") \
        if args.command not in READ_ONLY_COMMANDS else args.command

    try:
        from alpha_agent import agents_v2 as A               # type: ignore
        from alpha_agent import r59                          # type: ignore
        from alpha_agent.agents_v2 import contracts as C     # type: ignore
        from alpha_agent.agents_v2 import pipeline as P      # type: ignore
        from alpha_agent.r59 import memory as M              # type: ignore
        r59.assert_worktree_import()

        if cmd == "validate-contracts":
            problems = C.validate()
            _emit({"agent_system_version": A.AGENT_SYSTEM_VERSION,
                   "problems": problems})
            if problems:
                print("PIPELINE_REFUSED CONTRACTS_INCOHERENT")
                return 3
            print("PIPELINE_OK validate-contracts")
            return 0

        db = Path(args.memory) if args.memory else None
        if cmd in READ_ONLY_COMMANDS:
            if not M.memory_present(db):
                _emit({"agent_system_version": A.AGENT_SYSTEM_VERSION,
                       "memory": str(M.memory_db_path(db)),
                       "memory_present": False})
                print("PIPELINE_OK %s" % cmd)
                return 0
            mem = M.ResearchMemory(db, read_only=True)
        else:
            mem = M.ResearchMemory(db)
        pipe = P.AgentPipeline(mem)

        if cmd == "status":
            ledger = pipe.ledger()
            states: dict = {}
            for row in ledger:
                states[row["SURVIVOR_STATE"]] = \
                    states.get(row["SURVIVOR_STATE"], 0) + 1
            _emit({"agent_system_version": A.AGENT_SYSTEM_VERSION,
                   "roster": list(A.ROSTER),
                   "research_registry_owner": A.RESEARCH_REGISTRY_OWNER,
                   "forward_evidence_owner": A.FORWARD_EVIDENCE_OWNER,
                   "memory": str(mem.db_path),
                   "contract_problems": C.validate(),
                   "experiments": len(ledger), "by_state": states,
                   "safety": A.SAFETY})
        elif cmd == "ledger":
            _emit(pipe.ledger())
        elif cmd == "survivors":
            _emit(pipe.survivors())
        elif cmd == "validated-survivors":
            _emit(pipe.validated_survivors())
        elif cmd == "census":
            body = _census(mem, pipe, C.load_contract("agent_manifest.json"))
            if args.out:
                Path(args.out).write_text(
                    json.dumps(body, indent=1, default=str), encoding="utf-8")
                _emit({"census_written": args.out,
                       "memory": body["memory"]})
            else:
                _emit(body)
        else:
            if not args.agent:
                raise P.PipelineRefusal("AGENT_REQUIRED",
                                        "--agent names the calling agent")
            payload = {}
            if args.input:
                payload = json.loads(Path(args.input).read_text(
                    encoding="utf-8-sig"))
            # No adoption owner is ever composed here. A forward clock is
            # started by the ONE operator door, by a human.
            _emit(pipe.perform(args.agent, cmd, payload))
        print("PIPELINE_OK %s" % cmd)
        return 0
    except Exception as exc:                                 # noqa: BLE001
        if type(exc).__name__ == "PipelineRefusal":
            _emit({"refused": exc.code, "detail": str(exc)})
            print("PIPELINE_REFUSED %s" % exc.code)
            return 3
        _emit({"failed": type(exc).__name__, "detail": str(exc)[:400]})
        print("PIPELINE_FAILED %s" % type(exc).__name__)
        return 1


if __name__ == "__main__":
    sys.exit(main())
