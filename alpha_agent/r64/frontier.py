"""alpha_agent.r64.frontier - an OVERLAY on the R63 information gap frontier.

The R63 ``information_gap_frontier.json`` is THE frontier of information needs
(asset class x horizon x dimension) and stays the one owner of that question.
R64 measured several of those needs with a proper book; this module publishes
what changed as an OVERLAY keyed by the frontier's own ``cell_key``:

    * a remaining-research-value MULTIPLIER (a need answered under controls
      has less remaining research value; a need R64 could not settle keeps it);
    * the next action R64's verdict implies;
    * the evidence (cell id, verdict, numbers).

``alpha_agent.r59.information_needs`` reads the frontier AND this overlay and
hands the R59 governor its information-need candidates. No second frontier,
no second ranking rule: the overlay only multiplies the frontier's own
remaining value and names the next action.
"""
from __future__ import annotations

from . import (CARRY_FAMILY, CARRY_VARIANTS, V_DATA_HOLD, V_ECON, V_NO_VALUE, V_NOT_ECON,
               V_NOT_FDR, V_REPRO_FAILED, V_UNSTABLE, read_r63_artifact, write_artifact)

CALCULATION_OWNER = "alpha_agent.r64.frontier"
ARTIFACT_NAME = "r64_information_need_updates.json"
SCHEMA = "r64_information_need_updates/1"

#: How much research value REMAINS after the R64 verdict, as a multiplier of
#: the frontier's own remaining_research_value. Fixed here; not tuned.
MULTIPLIER = {V_ECON: 0.0, V_NOT_ECON: 0.25, V_UNSTABLE: 0.50, V_NOT_FDR: 0.75,
              V_NO_VALUE: 0.10, V_DATA_HOLD: 1.0, V_REPRO_FAILED: 1.0}
NEXT_ACTION = {V_ECON: "FORWARD_QUALIFICATION_REVIEW",
               V_NOT_ECON: "REJECTED_UNDER_RISK_CONTROLS",
               V_UNSTABLE: "MORE_RESEARCH_STABILITY",
               V_NOT_FDR: "MORE_RESEARCH_SAMPLE",
               V_NO_VALUE: "NO_CONDITIONAL_VALUE",
               V_DATA_HOLD: "DATA_HOLD", V_REPRO_FAILED: "REPRODUCTION_FAILED"}
#: The most favourable verdict decides a need measured by several cells.
RANK = {V_ECON: 6, V_NOT_FDR: 5, V_UNSTABLE: 4, V_NOT_ECON: 3, V_NO_VALUE: 2,
        V_DATA_HOLD: 1, V_REPRO_FAILED: 0}


def need_key(cell: dict) -> str:
    dim = cell.get("dimension")
    fam = CARRY_FAMILY if dim in CARRY_VARIANTS else dim
    return "%s|%d|%s" % (cell.get("scope"), int(cell.get("horizon") or 0), fam)


def build(cells: list, *, frontier: dict | None = None, write: bool = True) -> dict:
    fr = frontier if frontier is not None else read_r63_artifact("information_gap_frontier.json")
    known = {r["cell_key"]: r for r in ((fr or {}).get("all_needs") or [])}
    best: dict = {}
    for c in cells:
        v = c.get("r64_verdict")
        if v is None:
            continue
        k = need_key(c)
        if k not in best or RANK.get(v, -1) > RANK.get(best[k]["r64_verdict"], -1):
            e = c.get("r64_economics") or {}
            best[k] = {"cell_key": k, "r64_verdict": v, "cell_id": c.get("cell_id"),
                       "construction_variant": c.get("dimension"),
                       "remaining_research_value_multiplier": MULTIPLIER.get(v, 1.0),
                       "next_action": NEXT_ACTION.get(v, "MORE_RESEARCH_SAME_INFORMATION"),
                       "in_r63_frontier": k in known,
                       "frontier_remaining_research_value": (known.get(k) or {}).get(
                           "remaining_research_value"),
                       "evidence": {"conditional_t": (c.get("conditional") or {}).get("t"),
                                    "ann_net_increment_under_controls": e.get("ann_net_increment"),
                                    "sharpe_increment_under_controls": e.get("sharpe_increment"),
                                    "augmented_max_dd": (e.get("augmented") or {}).get("max_dd"),
                                    "fdr_pass_conditional": c.get("fdr_pass_conditional")}}
    updates = [best[k] for k in sorted(best)]
    body = {"schema": SCHEMA, "calculation_owner": CALCULATION_OWNER,
            "not_a_frontier": True,
            "frontier_owner": "alpha_agent.r63.gaps (information_gap_frontier.json)",
            "source_frontier_artifact_hash": (fr or {}).get("artifact_hash"),
            "consumer": "alpha_agent.r59.information_needs (reads the frontier and this overlay)",
            "rule": ("each update multiplies the frontier's OWN remaining_research_value by the "
                     "verdict multiplier and names the next action; nothing here re-ranks or "
                     "re-scores a need"),
            "multiplier_by_verdict": MULTIPLIER, "next_action_by_verdict": NEXT_ACTION,
            "n_updates": len(updates), "updates": updates}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
