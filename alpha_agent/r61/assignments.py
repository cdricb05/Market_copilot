r"""alpha_agent.r61.assignments - bounded foundation assignments (Workstream F).

ORCHESTRATION ONLY. Produces assignment envelopes. Runs nothing, measures
nothing, writes nothing to the research memory.

What went wrong in R60, precisely
---------------------------------
Model routing was NOT the problem and is not touched here. The director stays
on Opus, the skeptic stays on Opus, data foundation stays on Haiku, and the
structured roles stay cheap. The problem was TASK SIZE.

``ASSIGNMENT_DF4_SEC_STORES`` carried two sources and
``ASSIGNMENT_DF5_LIGHT_PROBES`` carried three. Both went to a role whose turn
ceiling is 15. Both then had to produce one certification per source -
DF4A/DF4B/DF4C and DF5A/DF5B/DF5C in the committed artifacts - so a single
15-turn agent was asked for three complete dataset certifications, exhausted
its budget and had to be resumed. The fix is not a bigger budget. Raising
every ceiling pays more for the same overrun and hides it.

The fix is that a large certification plan is SPLIT MECHANICALLY, here, before
anything is spawned:

    ONE SOURCE PER ASSIGNMENT           a role that certifies one dataset can
                                        finish one dataset
    A BOUNDED QUESTION LIST             at most MAX_QUESTIONS_PER_ASSIGNMENT
    AN ARTIFACT WRITTEN EARLY           the assignment names the file and the
                                        turn by which a partial answer must
                                        already be on disk, so an agent that
                                        runs out of turns still leaves
                                        evidence instead of nothing
    A DECLARED TURN BUDGET              taken from the role's OWN routing row,
                                        never invented at the call site

The director keeps receiving compact measurements. Splitting an assignment
changes what a foundation agent is asked for; it changes nothing about what
the director reads.
"""
from __future__ import annotations

from typing import Optional

from ..agents_v2 import DATA_FOUNDATION
from ..agents_v2 import routing as RT
from . import stable_hash

ASSIGNMENT_OWNER = "alpha_agent.r61.assignments"
ASSIGNMENT_VERSION = "R61_BOUNDED_FOUNDATION_ASSIGNMENT_V1"

#: The whole point. A 15-turn role gets ONE dataset question.
MAX_SOURCES_PER_ASSIGNMENT = 1

#: A bounded question list. Four is what a certification actually needs -
#: coverage, availability instant, survivorship, and the corporate-action or
#: continuation convention - and a fifth is a second assignment.
MAX_QUESTIONS_PER_ASSIGNMENT = 4

#: The artifact must exist before the role is half-way through its budget.
#: An agent that runs out of turns with nothing on disk has produced nothing;
#: one that wrote a partial certification has produced evidence.
EARLY_ARTIFACT_TURN_FRACTION = 0.5

#: The four questions a dataset certification answers. Named so an assignment
#: cannot quietly become a research brief.
CANONICAL_QUESTIONS = (
    "COVERAGE: over what instruments and what date range is this source "
    "present, measured, not assumed?",
    "AVAILABILITY_INSTANT: at what timestamp did each value become "
    "observable, and is that the filing/publication instant rather than the "
    "period end?",
    "SURVIVORSHIP: are delisted, expired or terminated entities retained, "
    "and does each carry a terminal value?",
    "CONVENTION: what corporate-action, continuation or restatement "
    "convention applies, and is it point-in-time?",
)


class AssignmentRefusal(RuntimeError):
    """A foundation assignment is not bounded."""


def turn_budget(role: str = DATA_FOUNDATION) -> int:
    """The role's OWN turn ceiling, from the routing table that owns it.

    Read, never chosen here. ``contracts.validate`` already fails the build if
    a definition's frontmatter drifts from this table, so an assignment sized
    against it is sized against what the agent will really get.
    """
    return int(RT.ROUTING[role]["maxTurns"])


def early_artifact_turn(role: str = DATA_FOUNDATION) -> int:
    return max(1, int(turn_budget(role) * EARLY_ARTIFACT_TURN_FRACTION))


def _source_id(src) -> str:
    if isinstance(src, dict):
        for k in ("source_id", "id", "dataset_id", "name"):
            if src.get(k):
                return str(src[k])
        return stable_hash(src)[:12]
    return str(src)


def split_plan(plan: dict, *, role: str = DATA_FOUNDATION,
               questions: Optional[tuple] = None) -> list:
    """Turn ONE certification plan into N bounded assignments, one per source.

    ``plan`` is an assignment envelope in the R60 shape: a ``sources`` list
    plus whatever campaign identifiers it carries. The identifiers are
    preserved so the split assignments stay traceable to the plan they came
    from; the SOURCES are what gets divided.
    """
    sources = list((plan or {}).get("sources") or [])
    if not sources:
        raise AssignmentRefusal(
            "a certification plan with no sources cannot be assigned; an "
            "empty assignment is a spawn that produces nothing")
    qs = tuple(questions or CANONICAL_QUESTIONS)[:MAX_QUESTIONS_PER_ASSIGNMENT]
    base_id = str(plan.get("assignment_id") or "DF")
    budget = turn_budget(role)
    early = early_artifact_turn(role)
    out = []
    for i, src in enumerate(sources):
        sid = _source_id(src)
        suffix = chr(ord("A") + i) if len(sources) > 1 and i < 26 else str(i)
        aid = base_id if len(sources) == 1 else "%s%s" % (base_id, suffix)
        body = {
            "assignment_owner": ASSIGNMENT_OWNER,
            "assignment_version": ASSIGNMENT_VERSION,
            "assignment_id": aid,
            "split_from": base_id,
            "split_index": i,
            "split_of": len(sources),
            "run_id": plan.get("run_id"),
            "campaign_id": plan.get("campaign_id"),
            "role": plan.get("role") or role,
            "phase": plan.get("phase"),
            "source_id": sid,
            "sources": [src],
            "questions": list(qs),
            "turn_budget": budget,
            "write_artifact_by_turn": early,
            "artifact": "CERTIFICATION_%s.json" % aid,
            "artifact_rule": (
                "Write %s with whatever is established by turn %d, then keep "
                "refining it. An agent that exhausts its budget with nothing "
                "on disk has produced nothing; one that wrote a partial "
                "certification has produced evidence."
                % ("CERTIFICATION_%s.json" % aid, early)),
            "scope_rule": (
                "ONE source. If this source turns out to contain several "
                "distinct stores, report that and STOP - it is a new split, "
                "not extra work inside this assignment."),
        }
        body["assignment_hash"] = stable_hash(
            {k: v for k, v in body.items() if k != "assignment_hash"})
        out.append(body)
    return out


def problems(assignment: dict, *, role: str = DATA_FOUNDATION) -> list:
    """Every way this assignment is not bounded. Empty means it is."""
    a = assignment or {}
    out = []
    srcs = list(a.get("sources") or [])
    if len(srcs) > MAX_SOURCES_PER_ASSIGNMENT:
        out.append(
            "%s carries %d sources; the ceiling is %d. This is the R60 defect "
            "exactly: DF4 carried 2 and DF5 carried 3, each to a %d-turn role, "
            "and each had to produce one certification per source."
            % (a.get("assignment_id"), len(srcs), MAX_SOURCES_PER_ASSIGNMENT,
               turn_budget(role)))
    if not srcs:
        out.append("%s carries no source" % a.get("assignment_id"))
    qs = list(a.get("questions") or [])
    if len(qs) > MAX_QUESTIONS_PER_ASSIGNMENT:
        out.append("%s asks %d questions; the ceiling is %d"
                   % (a.get("assignment_id"), len(qs),
                      MAX_QUESTIONS_PER_ASSIGNMENT))
    if not qs:
        out.append("%s asks no bounded question" % a.get("assignment_id"))
    if not a.get("artifact"):
        out.append("%s names no artifact" % a.get("assignment_id"))
    by_turn = a.get("write_artifact_by_turn")
    budget = a.get("turn_budget") or turn_budget(role)
    if not isinstance(by_turn, int) or by_turn < 1 or by_turn > int(budget):
        out.append(
            "%s does not require an artifact EARLY (write_artifact_by_turn=%r "
            "against a budget of %s)"
            % (a.get("assignment_id"), by_turn, budget))
    return out


def audit_plan(plan: dict, *, role: str = DATA_FOUNDATION) -> dict:
    """Would this plan overrun, and what is the bounded version of it?"""
    sources = list((plan or {}).get("sources") or [])
    split = split_plan(plan, role=role) if sources else []
    return {
        "assignment_id": plan.get("assignment_id"),
        "sources": len(sources),
        "role": role,
        "turn_budget": turn_budget(role),
        "model": RT.ROUTING[role]["model"],
        "effort": RT.ROUTING[role]["effort"],
        "would_overrun": len(sources) > MAX_SOURCES_PER_ASSIGNMENT,
        "bounded_assignments": [a["assignment_id"] for a in split],
        "split": split,
        "problems_before": problems(plan, role=role),
        "problems_after": [p for a in split for p in problems(a, role=role)],
        "routing_unchanged": True,
        "routing_note": ("Model routing is NOT changed by this module. The "
                         "director stays on opus, the skeptic stays on opus "
                         "and data foundation stays on haiku; only the SIZE "
                         "of the task changes."),
    }
