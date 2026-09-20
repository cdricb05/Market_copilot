r"""scripts/agents_v2_brief.py - hand a role its brief; never the estate.

The control-plane half of PAPER_TRADER_ALPHA_AGENTS_V2. ``run_agents_v2_campaign``
executes the deterministic work; this command decides WHO should be spawned and
prints exactly what that role needs to decide.

    # who should run at all, given what the memory already knows
    & .\.venv-win\Scripts\python.exe scripts\agents_v2_brief.py `
        --spawn-plan --campaign R57_WAVE2 `
        --spec research\agents\campaign_r57_wave2\campaign_spec.json

    # one role's compact brief
    & .\.venv-win\Scripts\python.exe scripts\agents_v2_brief.py `
        --role validation-skeptic-agent --experiment H_973b93bc_61298a993f59 `
        --campaign R57_WAVE2 --spec <spec.json> --results <results.json>

    # resume after a context reset - the transcript is NOT the system of record
    & .\.venv-win\Scripts\python.exe scripts\agents_v2_brief.py `
        --resume --campaign R57_WAVE2 --spec <spec.json> `
        --out research\agents\campaign_r57_wave2\CAMPAIGN_RESUME_STATE.json

    # the committed model/effort routing, and whether the definitions match it
    & .\.venv-win\Scripts\python.exe scripts\agents_v2_brief.py --routing

    # BEFORE vs AFTER, measured on a real campaign directory
    & .\.venv-win\Scripts\python.exe scripts\agents_v2_brief.py `
        --benchmark --campaign R57_WAVE2 --spec <spec.json> `
        --campaign-dir research\agents\campaign_r57_wave2

READ-ONLY. Every command here opens the research memory through its read-only
handle. There is no verb in this file that can pre-register, measure, rule,
publish, adopt, create an order or a fill, approve a proposal, promote a model
or make a sleeve capital eligible.

TERMINAL TOKENS (exactly one, on the last line)
    BRIEF_OK <what>
    BRIEF_REFUSED <code>          the request is not answerable (exit 3)
    BRIEF_FAILED <reason>         an unexpected failure (exit 1)
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


def _load(path: str):
    if not path:
        return None
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--role", default="", help="agent to brief")
    ap.add_argument("--campaign", default="", help="campaign id")
    ap.add_argument("--experiment", default="", help="experiment id")
    ap.add_argument("--spec", default="", help="frozen campaign spec JSON")
    ap.add_argument("--results", default="",
                    help="runner result bundle, for the skeptic's attacks")
    ap.add_argument("--campaign-dir", default="",
                    help="campaign artifact directory (benchmark)")
    ap.add_argument("--run-id", default="AGENTS_V2", help="run id stamp")
    ap.add_argument("--out", default="", help="write the brief here")
    ap.add_argument("--memory", default="",
                    help="research memory path (tests); default = R59 root")
    ap.add_argument("--spawn-plan", action="store_true",
                    help="which roles should be launched, and why not")
    ap.add_argument("--resume", action="store_true",
                    help="emit CAMPAIGN_RESUME_STATE")
    ap.add_argument("--routing", action="store_true",
                    help="the model/effort routing and its verification")
    ap.add_argument("--benchmark", action="store_true",
                    help="BEFORE vs AFTER context proxy for one campaign")
    args = ap.parse_args(argv)

    try:
        from alpha_agent.agents_v2 import briefs as BR        # type: ignore
        from alpha_agent.agents_v2 import efficiency as EF    # type: ignore
        from alpha_agent.agents_v2 import pipeline as P       # type: ignore
        from alpha_agent.agents_v2 import routing as RT       # type: ignore
        from alpha_agent.r59 import memory as M               # type: ignore

        if args.routing:
            problems = RT.routing_problems()
            body = {"routing_owner": RT.ROUTING_OWNER,
                    "routing_version": RT.ROUTING_VERSION,
                    "supported_by_local_claude_code":
                        RT.MODEL_ROUTING_EVIDENCE,
                    "supported_model_aliases": list(RT.SUPPORTED_MODEL_ALIASES),
                    "supported_efforts": list(RT.SUPPORTED_EFFORTS),
                    "routing": RT.ROUTING,
                    "definitions_match_policy": not problems,
                    "problems": problems}
            _emit(body)
            if problems:
                print("BRIEF_REFUSED ROUTING_DRIFT")
                return 3
            print("BRIEF_OK routing")
            return 0

        spec = _load(args.spec)
        results = _load(args.results)
        campaign = args.campaign or (spec or {}).get("campaign_id") or ""
        if not campaign:
            _emit({"refused": "NO_CAMPAIGN",
                   "detail": "pass --campaign or a --spec that names one"})
            print("BRIEF_REFUSED NO_CAMPAIGN")
            return 3

        db = Path(args.memory) if args.memory else None
        mem = M.open_memory_readonly(db)
        pipe = P.AgentPipeline(mem)

        if args.spawn_plan:
            state = BR.campaign_state(pipe, campaign, spec)
            body = RT.spawn_plan(state)
            body["state"] = state
            _emit(body)
            if args.out:
                Path(args.out).write_text(json.dumps(body, indent=1,
                                                     default=str),
                                          encoding="utf-8")
            print("BRIEF_OK spawn-plan %d/%d"
                  % (body["agent_invocations_planned"], body["roles_available"]))
            return 0

        if args.resume:
            body = BR.resume_state(pipe, run_id=args.run_id,
                                   campaign_id=campaign, spec=spec)
            _emit(body)
            if args.out:
                Path(args.out).write_text(json.dumps(body, indent=1,
                                                     default=str),
                                          encoding="utf-8")
            print("BRIEF_OK resume")
            return 0

        if args.benchmark:
            cdir = Path(args.campaign_dir) if args.campaign_dir else None
            if cdir is None:
                _emit({"refused": "NO_CAMPAIGN_DIR"})
                print("BRIEF_REFUSED NO_CAMPAIGN_DIR")
                return 3
            body = EF.measure_campaign(pipe, run_id=args.run_id,
                                       campaign_id=campaign, repo=_REPO,
                                       campaign_dir=cdir, spec=spec)
            _emit(body)
            if args.out:
                Path(args.out).write_text(json.dumps(body, indent=1,
                                                     default=str),
                                          encoding="utf-8")
            print("BRIEF_OK benchmark")
            return 0

        if not args.role:
            _emit({"refused": "NO_ROLE",
                   "detail": "pass --role, or one of --spawn-plan/--resume/"
                             "--routing/--benchmark"})
            print("BRIEF_REFUSED NO_ROLE")
            return 3
        if args.role not in BR.BUILDERS:
            _emit({"refused": "UNKNOWN_ROLE", "detail": args.role,
                   "roster": sorted(BR.BUILDERS)})
            print("BRIEF_REFUSED UNKNOWN_ROLE")
            return 3

        state = BR.campaign_state(pipe, campaign, spec)
        decision = next(d for d in RT.spawn_plan(state)["roles"]
                        if d["role"] == args.role)
        if decision["decision"] == RT.SKIP:
            _emit({"refused": "ROLE_SHOULD_NOT_BE_SPAWNED",
                   "role": args.role, "reason": decision["reason"],
                   "facts": decision["facts"]})
            print("BRIEF_REFUSED ROLE_SHOULD_NOT_BE_SPAWNED")
            return 3

        kind = BR.BUILDERS[args.role]
        if kind == "skeptic_brief":
            eid = args.experiment or next(iter(state["awaiting_skeptic"]), "")
            if not eid:
                _emit({"refused": "NO_MEASURED_CANDIDATE"})
                print("BRIEF_REFUSED NO_MEASURED_CANDIDATE")
                return 3
            one = None
            for r in ((results or {}).get("results") or ()):
                if r.get("experiment_id") == eid:
                    one = r
                    break
            brief = BR.skeptic_brief(pipe, run_id=args.run_id,
                                     campaign_id=campaign, experiment_id=eid,
                                     result=one)
        elif kind == "risk_brief":
            eid = args.experiment or next(iter(state["awaiting_risk"]), "")
            if not eid:
                _emit({"refused": "NO_SKEPTIC_SURVIVOR"})
                print("BRIEF_REFUSED NO_SKEPTIC_SURVIVOR")
                return 3
            brief = BR.risk_brief(pipe, run_id=args.run_id,
                                  campaign_id=campaign, experiment_id=eid)
        elif kind == "meta_brief":
            brief = BR.meta_brief(pipe, run_id=args.run_id,
                                  campaign_id=campaign,
                                  experiment_ids=state["validated_survivors"])
        elif kind == "publishing_brief":
            eid = args.experiment or next(iter(state["director_cleared"]), "")
            brief = BR.publishing_brief(pipe, run_id=args.run_id,
                                        campaign_id=campaign,
                                        experiment_id=eid)
        elif kind == "director_brief":
            brief = BR.director_brief(pipe, run_id=args.run_id,
                                      campaign_id=campaign, spec=spec)
        elif kind == "signal_brief":
            brief = BR.signal_brief(pipe, run_id=args.run_id,
                                    campaign_id=campaign, target=args.role,
                                    spec=spec)
        else:
            brief = BR.foundation_brief(pipe, run_id=args.run_id,
                                        campaign_id=campaign,
                                        target=args.role, spec=spec)

        problems = BR.brief_problems(brief)
        _emit(brief)
        if args.out:
            Path(args.out).write_text(json.dumps(brief, indent=1, default=str),
                                      encoding="utf-8")
        if problems:
            _emit({"handoff_contract_problems": problems})
            print("BRIEF_REFUSED HANDOFF_CONTRACT_VIOLATION")
            return 3
        print("BRIEF_OK %s" % args.role)
        return 0
    except Exception as exc:                                 # noqa: BLE001
        name = type(exc).__name__
        _emit({"failed": name, "detail": str(exc)[:600]})
        print("BRIEF_FAILED %s" % name)
        return 1


if __name__ == "__main__":
    sys.exit(main())
