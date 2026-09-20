r"""alpha_agent.agents_v2.routing - WHICH model runs a role, and WHEN a role runs.

RESEARCH ONLY. PAPER ONLY. NO ORDERS, NO FILLS, NO PROMOTION, NO ADOPTION.

Why this module exists
----------------------
R57 moved the deterministic COMPUTATION out of the agents and into
``agents_v2.runner``. It did not touch the control plane, and the control
plane is where the remaining spend is:

    every role ran on the session's strongest model, including the roles whose
    whole job is to check a schema and format a handoff;

    every subagent inherited the project CLAUDE.md - eleven kilobytes of UI
    redesign workflow, browser acceptance criteria and backend restart rules -
    none of which a momentum researcher has ever needed;

    and a downstream agent was SPAWNED, loaded its estate and reasoned before
    ``pipeline`` refused its write. R57 Wave 2 measured ten experiments, nine
    of which halted at D or V. The pipeline correctly refused the downstream
    work. It refused it AFTER the context had been paid for.

So this module owns two policies, both mechanical, both testable:

    MODEL / EFFORT ROUTING   what each of the twelve roles costs to think with
    SPAWN DISCIPLINE         whether a role should be launched AT ALL, decided
                             from memory state BEFORE a subagent exists

It is pure stdlib and reads nothing durable. ``briefs`` projects the state;
this module rules on it.

What the LOCAL Claude Code actually supports
--------------------------------------------
Nothing here is guessed. ``MODEL_ROUTING_EVIDENCE`` records the agent-definition
frontmatter schema carried by the installed binary, which is the authority on
what a ``.claude/agents/*.md`` file may declare. The fields this module uses -
``model``, ``effort``, ``maxTurns``, ``omitClaudeMd`` - are all in that schema.
No unsupported field is written.

A model value is an ALIAS or a full id. Aliases resolve inside Claude Code, so
the routing table names aliases and never pins a dated model id that a later
installation would not serve.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from . import (AGENT_DEFINITION_DIR, DATA_FOUNDATION, DIRECTOR, FEATURES,
               META, MOMENTUM, PUBLISHING, REVERSAL, RISK, ROSTER, SIGNAL_AGENTS,
               SKEPTIC, TREND_BREADTH, UNIVERSE, VOL_LIQUIDITY)

ROUTING_OWNER = "alpha_agent.agents_v2.routing"
ROUTING_VERSION = "R58_AGENT_MODEL_ROUTING_V1"

# --------------------------------------------------------------------------- #
# What the installed Claude Code supports. Evidence, not belief.
# --------------------------------------------------------------------------- #
#: Observed in the agent-definition frontmatter schema of the installed
#: Claude Code binary (2.1.278). Recorded so a future reader can re-derive it
#: instead of trusting this file.
MODEL_ROUTING_EVIDENCE = {
    "claude_code_version": "2.1.278",
    "binary": r"C:\Users\binis\.local\bin\claude.exe",
    "agent_frontmatter_fields": (
        "name", "description", "model", "tools", "disallowedTools", "color",
        "effort", "permissionMode", "mcpServers", "hooks", "maxTurns",
        "skills", "initialPrompt", "memory", "background", "omitClaudeMd",
        "isolation", "observer", "observerMessage", "observeSubagents"),
    "model_field_doc": ("Model override for this agent. Use `inherit` to "
                        "match the spawning conversation."),
    "model_alias_doc": ("Model alias (e.g. 'fable', 'opus', 'sonnet', "
                        "'haiku') or full model ID (e.g. 'claude-fable-5')."),
    "effort_field_doc": "Thinking effort: `low`, `medium`, `high`, `max`, "
                        "or an integer.",
    "omit_claude_md_doc": (
        "If true, the agent runs without the user, project and local "
        "CLAUDE.md instruction files when spawned as a subagent; managed "
        "policy files are kept."),
}

#: Aliases the installed resolver accepts. A dated model id is deliberately
#: NOT used: an alias keeps working when the family rolls forward.
SUPPORTED_MODEL_ALIASES = ("haiku", "sonnet", "opus", "fable", "inherit")

#: Values the ``effort`` field accepts (an integer is also legal; the routing
#: table uses the named levels because they survive a model change).
SUPPORTED_EFFORTS = ("low", "medium", "high", "max")

# --------------------------------------------------------------------------- #
# The tiers
# --------------------------------------------------------------------------- #
TIER_HIGH = "HIGH_REASONING"
TIER_MEDIUM = "MEDIUM"
TIER_STRUCTURED = "LOW_STRUCTURED"

#: Judgment that materially changes what the estate spends or believes.
#: The director sets the agenda and rules the tournament; the skeptic is the
#: ONLY door downstream and rejects by default. Both interpret ambiguous
#: evidence, and a wrong call from either is expensive for many releases.
_HIGH = (DIRECTOR, SKEPTIC)

#: Reasoning over STRUCTURED inputs. These roles read a brief and a measured
#: result; they do not read the estate.
_MEDIUM = (MOMENTUM, REVERSAL, TREND_BREADTH, VOL_LIQUIDITY, RISK, META)

#: Contract checking, schema checking, mechanical preparation and handoff
#: formatting. Python already determined the answer; the role states it.
_STRUCTURED = (DATA_FOUNDATION, UNIVERSE, FEATURES, PUBLISHING)

TIERS = {TIER_HIGH: _HIGH, TIER_MEDIUM: _MEDIUM, TIER_STRUCTURED: _STRUCTURED}

#: model / effort / turn ceiling per tier.
TIER_POLICY = {
    TIER_HIGH: {"model": "opus", "effort": "high", "maxTurns": 40},
    TIER_MEDIUM: {"model": "sonnet", "effort": "medium", "maxTurns": 25},
    TIER_STRUCTURED: {"model": "haiku", "effort": "low", "maxTurns": 15},
}


def tier_of(agent: str) -> str:
    for tier, members in TIERS.items():
        if agent in members:
            return tier
    raise KeyError("%s is not a PAPER_TRADER_ALPHA_AGENTS_V2 role" % agent)


def _routing_table() -> dict:
    out = {}
    for name in ROSTER:
        policy = dict(TIER_POLICY[tier_of(name)])
        # Every research role takes everything it needs from its brief and its
        # own definition, so none of them inherits the project CLAUDE.md. The
        # non-negotiables it would have carried (PowerShell only, research
        # only, no orders, no automation) are REQUIRED SECTIONS of each
        # definition already - ``contracts.REQUIRED_SECTIONS`` enforces that -
        # so dropping the inherited copy removes duplication, not safety.
        policy["omitClaudeMd"] = True
        policy["tier"] = tier_of(name)
        out[name] = policy
    return out


#: The routing policy, one row per role. This is the OWNER of the answer to
#: "what model does this agent use"; the .md frontmatter is its rendering, and
#: ``routing_problems`` proves the two agree.
ROUTING = _routing_table()

#: Frontmatter keys this module owns in a definition file.
ROUTED_KEYS = ("model", "effort", "maxTurns", "omitClaudeMd")


def frontmatter_for(agent: str) -> dict:
    """The exact frontmatter values ``agent``'s definition must declare."""
    row = ROUTING[agent]
    return {"model": row["model"], "effort": row["effort"],
            "maxTurns": row["maxTurns"],
            "omitClaudeMd": "true" if row["omitClaudeMd"] else "false"}


def routing_problems(definition_dir: Optional[Path] = None) -> list:
    """Every disagreement between this policy and the committed definitions.

    Empty means the configured routing is the routing Claude Code will use.
    """
    from . import contracts as C                      # local: avoid a cycle

    ddir = Path(definition_dir or AGENT_DEFINITION_DIR)
    problems: list = []
    for name in ROSTER:
        p = ddir / ("%s.md" % name)
        if not p.exists():
            problems.append("missing agent definition: %s" % name)
            continue
        front = C.parse_agent_definition(p)["frontmatter"]
        want = frontmatter_for(name)
        for key, value in want.items():
            got = front.get(key)
            if got is None:
                problems.append("%s: frontmatter does not declare %s"
                                % (name, key))
            elif str(got).strip().lower() != str(value).strip().lower():
                problems.append("%s: %s is %r, routing policy says %r"
                                % (name, key, got, value))
        model = str(front.get("model", "")).strip()
        if model and model not in SUPPORTED_MODEL_ALIASES \
                and not model.startswith("claude-"):
            problems.append(
                "%s: model %r is neither a supported alias %s nor a full "
                "claude- id" % (name, model, list(SUPPORTED_MODEL_ALIASES)))
        effort = str(front.get("effort", "")).strip()
        if effort and effort not in SUPPORTED_EFFORTS and not effort.isdigit():
            problems.append("%s: effort %r is not one of %s nor an integer"
                            % (name, effort, list(SUPPORTED_EFFORTS)))
    return problems


# --------------------------------------------------------------------------- #
# Spawn discipline
# --------------------------------------------------------------------------- #
SPAWN = "SPAWN"
SKIP = "SKIP"

#: Reason codes. A skip is always EXPLAINED; silence is not a decision.
R_AGENDA = "DIRECTOR_OWNS_THE_AGENDA"
R_FOUNDATION_NEEDED = "FOUNDATION_STAGE_NOT_YET_SATISFIED"
R_FOUNDATION_DONE = "FOUNDATION_STAGE_ALREADY_SATISFIED"
R_HAS_WORK = "OWNS_UNMEASURED_ASSIGNED_EXPERIMENTS"
R_NOT_ASSIGNED = "NO_ASSIGNED_EXPERIMENTS_IN_THIS_CAMPAIGN"
R_ALL_MEASURED = "ALL_ASSIGNED_EXPERIMENTS_ALREADY_MEASURED"
R_HAS_CANDIDATE = "MEASURED_CANDIDATES_AWAIT_REVIEW"
R_NO_CANDIDATE = "NO_MEASURED_CANDIDATE_EVERY_EXPERIMENT_HALTED_OR_SETTLED"
R_HAS_SURVIVOR = "SKEPTIC_SURVIVORS_AWAIT_RISK_REVIEW"
R_NO_SURVIVOR = "SKEPTIC_KILLED_EVERY_CANDIDATE"
R_META_QUESTION = "TWO_OR_MORE_VALIDATED_SURVIVORS_CREATE_A_COMBINATION_QUESTION"
R_NO_META_QUESTION = "FEWER_THAN_TWO_VALIDATED_SURVIVORS_NOTHING_TO_COMBINE"
R_CLEARED = "DIRECTOR_CLEARED_A_CANDIDATE"
R_NOT_CLEARED = "NO_DIRECTOR_CLEARANCE"

#: A meta review is only a real question when there is something to combine.
META_MIN_SURVIVORS = 2

#: Deterministic work that must NEVER be done by a language model. Each entry
#: names the local owner that already does it. ``briefs`` renders this into
#: every brief so a role is told, in its own context, what not to recompute.
DETERMINISTIC_LOCAL_OWNERS = {
    "arithmetic": "alpha_agent.agents_v2.runner",
    "statistics": "alpha_agent.r59.engines.gate",
    "ranking": "alpha_agent.agents_v2.books",
    "backtests": "alpha_agent.agents_v2.books",
    "feature_computation": "campaign executor module",
    "transaction_costs": "alpha_agent.agents_v2.books",
    "universe_filtering": "campaign executor module",
    "date_slicing": "alpha_agent.agents_v2.runner",
    "placebo": "alpha_agent.agents_v2.runner.adversarial_pack",
    "doubled_cost": "alpha_agent.agents_v2.runner.adversarial_pack",
    "subperiod_stability": "alpha_agent.agents_v2.runner.adversarial_pack",
    "duplicate_identity": "alpha_agent.r59.memory.ResearchMemory.is_novel",
    "threshold_comparison": "alpha_agent.r59.engines.gate",
    "experiment_state_lookup": "alpha_agent.r59.memory.ResearchMemory",
    "burden_calculation": "alpha_agent.r59.handlers.search_denominator",
    "stage_advance_rule": "alpha_agent.r59.engines.stage_advance",
    "result_formatting": "alpha_agent.agents_v2.briefs",
}

#: Judgment that must STAY with an agent. Local code cannot settle these.
AGENT_JUDGMENT = (
    "economic hypothesis generation",
    "novelty judgment against the settled estate",
    "interpretation of an ambiguous result",
    "adversarial reasoning beyond the machine-checkable attacks",
    "agenda prioritisation and budget allocation",
    "portfolio research judgment",
)


def _decision(role: str, allow: bool, reason: str, **facts) -> dict:
    return {"role": role, "decision": SPAWN if allow else SKIP,
            "reason": reason, "model": ROUTING[role]["model"],
            "effort": ROUTING[role]["effort"], "facts": facts}


def spawn_plan(state: dict) -> dict:
    """Rule, for every role, whether a subagent should be launched at all.

    ``state`` is the compact campaign projection ``briefs.campaign_state``
    builds. This function is PURE so the discipline can be proven hermetically
    against any state, including states no live campaign has reached yet.

    The rules are the ones the pipeline already enforces at WRITE time. Moving
    them to SPAWN time is the whole point: a refusal after a subagent has
    loaded its context has already cost the context.
    """
    assigned = dict(state.get("assigned_by_agent") or {})
    unmeasured = dict(state.get("unmeasured_by_agent") or {})
    awaiting_skeptic = list(state.get("awaiting_skeptic") or ())
    awaiting_risk = list(state.get("awaiting_risk") or ())
    validated = list(state.get("validated_survivors") or ())
    cleared = list(state.get("director_cleared") or ())

    plan = [_decision(DIRECTOR, True, R_AGENDA,
                      campaign_id=state.get("campaign_id"))]

    for role, done_key in ((DATA_FOUNDATION, "data_certified"),
                           (UNIVERSE, "universe_defined"),
                           (FEATURES, "features_published")):
        done = bool(state.get(done_key))
        plan.append(_decision(role, not done,
                              R_FOUNDATION_DONE if done else R_FOUNDATION_NEEDED,
                              **{done_key: done}))

    for role in SIGNAL_AGENTS:
        n_assigned = int(assigned.get(role, 0))
        n_open = int(unmeasured.get(role, 0))
        if n_assigned == 0:
            reason = R_NOT_ASSIGNED
        elif n_open == 0:
            reason = R_ALL_MEASURED
        else:
            reason = R_HAS_WORK
        plan.append(_decision(role, n_open > 0, reason,
                              assigned=n_assigned, unmeasured=n_open))

    plan.append(_decision(
        SKEPTIC, bool(awaiting_skeptic),
        R_HAS_CANDIDATE if awaiting_skeptic else R_NO_CANDIDATE,
        awaiting_review=len(awaiting_skeptic),
        halted_before_lockbox=int(state.get("halted", 0))))

    plan.append(_decision(
        RISK, bool(awaiting_risk),
        R_HAS_SURVIVOR if awaiting_risk else R_NO_SURVIVOR,
        survivors_awaiting_risk=len(awaiting_risk)))

    enough = len(validated) >= META_MIN_SURVIVORS
    plan.append(_decision(
        META, enough, R_META_QUESTION if enough else R_NO_META_QUESTION,
        validated_survivors=len(validated), minimum=META_MIN_SURVIVORS))

    plan.append(_decision(
        PUBLISHING, bool(cleared), R_CLEARED if cleared else R_NOT_CLEARED,
        director_cleared=len(cleared)))

    spawn = [d["role"] for d in plan if d["decision"] == SPAWN]
    return {
        "routing_owner": ROUTING_OWNER, "routing_version": ROUTING_VERSION,
        "campaign_id": state.get("campaign_id"),
        "roles": plan,
        "spawn": spawn, "skip": [d["role"] for d in plan
                                 if d["decision"] == SKIP],
        "agent_invocations_planned": len(spawn),
        "roles_available": len(ROSTER),
        # A subagent cannot launch a subagent in Claude Code, and no research
        # definition is granted a delegation tool. The session orchestrator is
        # the ONLY dispatcher, and this plan is what it may dispatch.
        "spawned_by": "SESSION_ORCHESTRATOR_ONLY",
        "agent_may_spawn_agent": False,
    }


#: Tools that would let a research agent launch another agent. None of the
#: twelve may hold one; ``contracts.validate`` is where that is enforced.
DELEGATION_TOOLS = ("Agent", "Task", "Workflow")
