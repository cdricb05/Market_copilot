"""alpha_agent.r63.handoff - the machine-readable frontier for the AlphaAgent.

The persistent research agent (``alpha_agent.r59.governor``) asks "what
should we learn next?" and today answers from a frontier of PRICE families.
R63 gives it a second question it can ask:

    which asset class x horizon x information dimension has the highest
    REMAINING expected research value, and what is the next action there?

``next_best_information_research`` answers from the published gap frontier;
``publish`` writes ``r63_alphaagent_frontier.json`` with the ranked answer,
the per-scope STOP_TRANSFORMING flags (a scope whose baseline ablations show
the price block is fully exploited and whose remaining value sits in unseen
dimensions), and ``mandate_hints`` in the shape the R59 governor's
DATA_OPPORTUNITY mandate already uses.

This is an INTERFACE, implemented and tested in the R63 branch. It is NOT
wired into the live ResearchRuntime and deploys nothing tonight.
"""
from __future__ import annotations

from . import ASSET_CLASSES, read_artifact, write_artifact
from . import gaps as G
from . import sensitivity as S

CALCULATION_OWNER = "alpha_agent.r63.handoff"
SCHEMA = "r63_alphaagent_frontier/1"
ARTIFACT_NAME = "r63_alphaagent_frontier.json"
MANDATE_KIND = "DATA_OPPORTUNITY"


def next_best_information_research(n: int = 10, *, frontier: dict | None = None,
                                    asset_class: str | None = None) -> list:
    fr = frontier or read_artifact(G.ARTIFACT_NAME) or {}
    rows = fr.get("all_needs") or []
    if asset_class:
        rows = [r for r in rows if r["asset_class"] == asset_class]
    rows = [r for r in rows if r["remaining_research_value"] > 0]
    rows.sort(key=lambda r: (-r["remaining_research_value"], r["cell_key"]))
    return rows[:int(n)]


def stop_transforming_flags(cells: list | None) -> dict:
    """Per scope: are the baseline (price) ablations all NO_CONDITIONAL_VALUE
    or REDUNDANT (the price block is exploited) - if so, a new price
    transformation is not the next experiment."""
    out = {}
    for ac in ASSET_CLASSES:
        abl = [c for c in (cells or []) if c.get("scope") == ac and c.get("kind") == "ABLATION"
               and c.get("conditional")]
        if not abl:
            out[ac] = {"flag": None, "why": "no ablation measured"}
            continue
        valuable = [c["cell_id"] for c in abl if (c["conditional"].get("t") or 0) >= S.CONDITIONAL_T_FLOOR
                    and (c.get("economics") or {}).get("ann_net_increment", 0) and c["economics"]["ann_net_increment"] > 0]
        out[ac] = {"flag": len(valuable) == 0, "ablations": len(abl),
                   "baseline_dimensions_with_marginal_value": valuable,
                   "why": ("no baseline dimension shows marginal net value: stop generating price transformations here"
                           if not valuable else "the baseline still carries marginal value in the named dimensions")}
    return out


def mandate_hints(frontier: dict, n: int = 10) -> list:
    """Rows in the EXACT shape of ``alpha_agent.r59.governor._mandate``:
    kind / asset_class / family / expected_information_value / reason /
    payload / issued_by / issued_at. ``family`` follows the governor's
    ``DATA:<id>`` convention so a future consumer needs no adapter. Nothing
    here issues, enqueues or executes a mandate."""
    hints = []
    for r in next_best_information_research(n, frontier=frontier):
        hints.append({
            "kind": MANDATE_KIND, "asset_class": r["asset_class"],
            "family": "DATA:INFORMATION_NEED:%s" % r["cell_key"],
            "expected_information_value": r["remaining_research_value"],
            "reason": "R63 gap frontier: observation_state=%s best_verdict=%s next_action=%s"
                      % (r["observation_state"], r["best_verdict"], r["next_action"]),
            "payload": {"source": "R63_INFORMATION_GAP_FRONTIER", "cell_key": r["cell_key"],
                        "horizon_sessions": r["horizon"], "information_family": r["dimension"],
                        "economic_family": "INFORMATION_%s" % r["dimension"],
                        "next_action": r["next_action"], "sourcing_step": r["sourcing_step"]},
            "issued_by": CALCULATION_OWNER, "issued_at": None,
            "executes_nothing": True})
    return hints


def publish(frontier: dict, cells: list | None) -> dict:
    ranked = next_best_information_research(40, frontier=frontier)
    per_scope = {ac: next_best_information_research(5, frontier=frontier, asset_class=ac) for ac in ASSET_CLASSES}
    body = {"schema": SCHEMA, "calculation_owner": CALCULATION_OWNER,
            "question": "which asset class x horizon x information dimension has the highest remaining "
                        "expected research value?",
            "ranked": ranked, "per_scope": per_scope,
            "stop_transforming": stop_transforming_flags(cells),
            "mandate_hints": mandate_hints(frontier, 10),
            "integration_state": "INTERFACE_ONLY_NOT_WIRED_INTO_LIVE_RUNTIME",
            "consumer": "alpha_agent.r59.governor (future), through mandate_hints"}
    write_artifact(ARTIFACT_NAME, body)
    return body
