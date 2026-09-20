r"""alpha_agent.agents_v2.efficiency - MEASURE the operational pass, honestly.

RESEARCH ONLY. PAPER ONLY. NO ORDERS, NO FILLS, NO PROMOTION, NO ADOPTION.

What this measures, and what it refuses to claim
------------------------------------------------
Claude Code does not expose a per-subagent token meter to a local process, so
this module does NOT report token savings. It reports a deterministic PROXY -
the bytes of context payload a role is handed - computed from real files on
disk, plus invocation counts computed from the same spawn rule in both arms.

    BEFORE   the payload the COMMITTED contract says a role reads:
             the inherited project CLAUDE.md, the contract set named in its
             ``input_artifacts``, the whole campaign agenda and the whole
             result bundle.
    AFTER    the payload the role is handed: its brief, and nothing else.

Both arms are measured the same way, from the same campaign, by the same
function. A reader who disagrees with the BEFORE model can re-run it against
a different one: ``measure_campaign`` takes the file set as data.

Invocation counts use one definition in both arms: how many of the twelve
roles a session would launch. BEFORE there was no spawn gate, so the answer is
the canonical flow's whole roster. AFTER it is ``routing.spawn_plan`` over the
campaign's real end state. Both are STRUCTURAL counts, and both are labelled
as such.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from . import (AGENT_SYSTEM_VERSION, CONTRACT_DIR, CONTRACT_FILES,
               DATA_FOUNDATION, DIRECTOR, FEATURES, META, PUBLISHING, RISK,
               ROSTER, SIGNAL_AGENTS, SKEPTIC, UNIVERSE)
from . import briefs as BR
from . import routing as RT

EFFICIENCY_OWNER = "alpha_agent.agents_v2.efficiency"
EFFICIENCY_VERSION = "R58_AGENT_EFFICIENCY_BENCHMARK_V1"

#: The proxy. Named so a report can never imply a measured token count.
PROXY = "CONTEXT_PAYLOAD_BYTES"
PROXY_NOTE = ("Deterministic byte proxy over real files. NOT a token count; "
              "Claude Code exposes no per-subagent token meter locally.")


def _size(p: Path) -> int:
    try:
        return p.stat().st_size
    except OSError:
        return 0


def _contract_bytes(contract_dir: Path) -> int:
    return sum(_size(contract_dir / n) for n in CONTRACT_FILES)


#: What each role reads BEFORE, per the committed contracts. The director also
#: reads the census and the three canonical docs; every role inherits
#: CLAUDE.md; every role that reasons about the campaign receives the whole
#: agenda and the whole result bundle.
_DIRECTOR_DOCS = ("docs/PROJECT_CHARTER.md", "docs/PNL_OPPORTUNITY_FRONTIER.md",
                  "docs/STRATEGY_SLEEVE_CONTRACT.md")
_RISK_DOCS = ("docs/STRATEGY_SLEEVE_CONTRACT.md",
              "docs/DAILY_MULTI_ASSET_GOVERNANCE.md")


def before_payload_bytes(role: str, *, repo: Path, contract_dir: Path,
                         campaign_dir: Path) -> dict:
    """The payload the committed contract hands ``role``, in bytes."""
    parts = {
        "inherited_claude_md": _size(repo / "CLAUDE.md"),
        "contract_set": _contract_bytes(contract_dir),
        "campaign_agenda": _size(campaign_dir / "campaign_agenda.json"),
        "result_bundle": _size(campaign_dir / "results.json"),
    }
    if role == DIRECTOR:
        parts["census"] = _size(contract_dir / BR.CENSUS_FILE)
        parts["canonical_docs"] = sum(_size(repo / d) for d in _DIRECTOR_DOCS)
    if role == SKEPTIC:
        parts["campaign_result"] = _size(campaign_dir / "CAMPAIGN_RESULT.json")
    if role == RISK:
        parts["canonical_docs"] = sum(_size(repo / d) for d in _RISK_DOCS)
    return {"role": role, "bytes": sum(parts.values()), "parts": parts}


def after_payload_bytes(role: str, brief: Optional[dict]) -> dict:
    """The payload the role is handed now: its brief, and nothing else."""
    if brief is None:
        return {"role": role, "bytes": 0, "parts": {"brief": 0},
                "not_spawned": True}
    n = len(json.dumps(brief, default=str).encode("utf-8"))
    return {"role": role, "bytes": n, "parts": {"brief": n}}


def build_brief(pipe, role: str, *, run_id: str, campaign_id: str,
                spec: Optional[dict] = None,
                state: Optional[dict] = None) -> Optional[dict]:
    """The brief a role would receive for this campaign, or None if the spawn
    rule says it should not be launched at all."""
    state = state or BR.campaign_state(pipe, campaign_id, spec)
    if role == DIRECTOR:
        return BR.director_brief(pipe, run_id=run_id, campaign_id=campaign_id,
                                 spec=spec)
    if role in (DATA_FOUNDATION, UNIVERSE, FEATURES):
        return BR.foundation_brief(pipe, run_id=run_id,
                                   campaign_id=campaign_id, target=role,
                                   spec=spec)
    if role in SIGNAL_AGENTS:
        return BR.signal_brief(pipe, run_id=run_id, campaign_id=campaign_id,
                               target=role, spec=spec)
    if role == SKEPTIC:
        pending = state.get("awaiting_skeptic") or state.get("measured") or []
        if not pending:
            return None
        return BR.skeptic_brief(pipe, run_id=run_id, campaign_id=campaign_id,
                                experiment_id=pending[0])
    if role == RISK:
        pending = state.get("awaiting_risk") or []
        if not pending:
            return None
        return BR.risk_brief(pipe, run_id=run_id, campaign_id=campaign_id,
                             experiment_id=pending[0])
    if role == META:
        valid = state.get("validated_survivors") or []
        if len(valid) < RT.META_MIN_SURVIVORS:
            return None
        return BR.meta_brief(pipe, run_id=run_id, campaign_id=campaign_id,
                             experiment_ids=valid)
    if role == PUBLISHING:
        cleared = state.get("director_cleared") or []
        if not cleared:
            return None
        return BR.publishing_brief(pipe, run_id=run_id,
                                   campaign_id=campaign_id,
                                   experiment_id=cleared[0])
    raise KeyError(role)


def measure_campaign(pipe, *, run_id: str, campaign_id: str,
                     repo: Path, campaign_dir: Path,
                     spec: Optional[dict] = None,
                     contract_dir: Optional[Path] = None) -> dict:
    """BEFORE vs AFTER over ONE campaign, both arms measured identically."""
    repo = Path(repo)
    cdir = Path(contract_dir or CONTRACT_DIR)
    campaign_dir = Path(campaign_dir)
    state = BR.campaign_state(pipe, campaign_id, spec)
    plan = RT.spawn_plan(state)
    spawn = set(plan["spawn"])

    before_rows, after_rows, all_rows = [], [], []
    for role in ROSTER:
        before_rows.append(before_payload_bytes(
            role, repo=repo, contract_dir=cdir, campaign_dir=campaign_dir))
        # Two effects are at work and they must not be conflated: briefs make
        # each spawn SMALLER, and the spawn gate makes there be FEWER spawns.
        # ``all_rows`` prices every role's brief as if all twelve ran, which
        # isolates the brief effect from the gate effect.
        every = build_brief(pipe, role, run_id=run_id,
                            campaign_id=campaign_id, spec=spec, state=state)
        all_rows.append(after_payload_bytes(role, every))
        after_rows.append(after_payload_bytes(
            role, every if role in spawn else None))

    before_total = sum(r["bytes"] for r in before_rows)
    after_total = sum(r["bytes"] for r in after_rows)
    all_total = sum(r["bytes"] for r in all_rows)
    before_handoff = sum(_size(campaign_dir / n) for n in
                         ("campaign_agenda.json", "results.json",
                          "CAMPAIGN_RESULT.json", "skeptic_review.json"))
    after_handoff = sum(r["bytes"] for r in after_rows)

    def _pct(before: int, after: int) -> Optional[float]:
        if before <= 0:
            return None
        return round(100.0 * (before - after) / before, 1)

    return {
        "kind": "AGENT_EFFICIENCY_BENCHMARK",
        "owner": EFFICIENCY_OWNER,
        "version": EFFICIENCY_VERSION,
        "agent_system_version": AGENT_SYSTEM_VERSION,
        "run_id": run_id,
        "campaign_id": campaign_id,
        "proxy": PROXY,
        "proxy_note": PROXY_NOTE,
        "token_counts_available": False,
        "invocations": {
            "definition": ("roles a session would launch for this campaign; "
                           "STRUCTURAL, not an observed transcript count"),
            "before_agent_invocations": len(ROSTER),
            "before_note": "no spawn gate existed; the whole roster was available",
            "after_agent_invocations": plan["agent_invocations_planned"],
            "after_spawn": plan["spawn"],
            "after_skip": [{"role": d["role"], "reason": d["reason"]}
                           for d in plan["roles"] if d["decision"] == RT.SKIP],
            "reduction_pct": _pct(len(ROSTER),
                                  plan["agent_invocations_planned"]),
        },
        "context_proxy": {
            "before_bytes": before_total,
            "after_bytes": after_total,
            "reduction_pct": _pct(before_total, after_total),
            # The two effects, priced separately so neither is overclaimed.
            "brief_effect_only": {
                "note": ("all twelve roles spawned in BOTH arms; the only "
                         "change priced here is estate payload -> brief"),
                "before_bytes": before_total,
                "after_bytes": all_total,
                "reduction_pct": _pct(before_total, all_total)},
            "spawn_gate_effect_only": {
                "note": ("briefs in BOTH arms; the only change priced here "
                         "is twelve spawns -> the spawn plan"),
                "before_bytes": all_total,
                "after_bytes": after_total,
                "reduction_pct": _pct(all_total, after_total)},
            "before_by_role": before_rows,
            "after_by_role": after_rows,
        },
        "handoff_bytes": {
            "before_bytes": before_handoff,
            "after_bytes": after_handoff,
            "reduction_pct": _pct(before_handoff, after_handoff),
        },
        "duplicate_context_removed": {
            "inherited_claude_md_per_spawn": _size(repo / "CLAUDE.md"),
            "spawns": len(ROSTER),
            "total_bytes": _size(repo / "CLAUDE.md") * len(ROSTER),
            "mechanism": "omitClaudeMd: true in every research agent definition",
        },
        "full_memory_reads_required_by_director": 0,
        "full_census_reads_required_by_director": 0,
        "skeptic_recomputes_machine_tests": False,
        "deterministic_tasks_done_locally": len(RT.DETERMINISTIC_LOCAL_OWNERS),
        "deterministic_tasks_done_by_agent": 0,
        "judgment_retained_by_agents": len(RT.AGENT_JUDGMENT),
        "campaign_state": state,
    }
