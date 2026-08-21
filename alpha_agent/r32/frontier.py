"""alpha_agent.r32.frontier - the ONE PnL Opportunity Frontier owner.

The frontier is a RESEARCH COMPARISON. It is not the production allocator, it
produces no target portfolio, and Release 33 is where an allocator learns to
consume it. Saying so here matters because a ranked table of strategies that
each report an annual return looks exactly like an allocation instruction.

What it assembles:

* every sleeve's standalone after-cost economics, on its own maximum legitimate
  history AND on the calendar every sleeve shares;
* the correlation map between sleeve return paths, which is the only honest way
  to talk about diversification - counting asset classes is not;
* latent risk clusters, so two sleeves expressing the same bet through different
  instruments are visible as one bet;
* marginal portfolio value, which is what actually decides whether an
  opportunity deserves capital;
* the information gaps that blocked a sleeve, handed to the purchase gate.

Cash appears as a real row, not as the leftover. A frontier on which cash wins
is a valid outcome and is reported as one.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Optional

import numpy as np

from .. import r32
from . import contract as _contract
from . import judge as _judge

CALCULATION_OWNER = "alpha_agent.r32.frontier"
FRONTIER_SCHEMA = "r32_pnl_opportunity_frontier/1"
ARTIFACT_NAME = "pnl_opportunity_frontier.json"

#: Two sleeves whose net return paths correlate above this are treated as one
#: latent bet until proven otherwise. Declared before results are seen.
CLUSTER_CORRELATION = 0.70


def correlation_map(paths: dict) -> dict:
    """Pairwise correlation of sleeve net return paths on their common dates."""
    names = sorted(paths)
    out = {}
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            da, db = paths[a], paths[b]
            common = sorted(set(da) & set(db))
            if len(common) < 8:
                out[f"{a}|{b}"] = {"n": len(common), "correlation": None,
                                   "note": "insufficient common decisions"}
                continue
            x = np.asarray([da[d] for d in common], dtype=float)
            y = np.asarray([db[d] for d in common], dtype=float)
            ok = np.isfinite(x) & np.isfinite(y)
            if ok.sum() < 8 or np.std(x[ok]) == 0 or np.std(y[ok]) == 0:
                out[f"{a}|{b}"] = {"n": int(ok.sum()), "correlation": None}
                continue
            out[f"{a}|{b}"] = {"n": int(ok.sum()),
                               "correlation": float(np.corrcoef(x[ok], y[ok])[0, 1]),
                               "first": common[0], "last": common[-1]}
    return out


def latent_clusters(corr: dict, *, threshold: float = CLUSTER_CORRELATION) -> list:
    """Group sleeves whose paths move together into single latent bets.

    Asset labels do not equal diversification. Two sleeves can hold entirely
    different instruments and still be the same trade - long equity beta wearing
    two hats - and a portfolio that owns both believes it is twice as
    diversified as it is.
    """
    edges = [k.split("|") for k, v in corr.items()
             if v.get("correlation") is not None
             and abs(float(v["correlation"])) >= threshold]
    parent: dict = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in edges:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra
    groups: dict = {}
    for node in parent:
        groups.setdefault(find(node), []).append(node)
    return [{"cluster": sorted(v), "size": len(v)}
            for v in groups.values() if len(v) > 1]


def build(*, campaign_id: str = _contract.CAMPAIGN_ID, verdict: dict,
          sleeve_paths: dict, overlap: dict, inherited: dict,
          information_gaps: list,
          created_at: Optional[str] = None) -> dict:
    """Assemble the frontier from an already-computed verdict."""
    sleeves = verdict.get("sleeves") or {}
    rows = []
    for name in _contract.SLEEVES:
        s = sleeves.get(name) or {}
        best = None
        for r in (s.get("lockbox_results") or []):
            if best is None or _score(r) > _score(best):
                best = r
        rows.append({
            "sleeve": name,
            "state": s.get("state"),
            "is_control": bool(s.get("is_control")),
            "owns_capital": False,
            "activated": False,
            "window": s.get("window"),
            "best_configuration": (best or {}).get("label"),
            "net_annual_return": (best or {}).get("net_annual_return"),
            "net_sharpe": (best or {}).get("net_sharpe"),
            "max_drawdown": (best or {}).get("max_drawdown"),
            "mean_cash_weight": (best or {}).get("mean_cash_weight"),
            "annual_cost_drag": (best or {}).get("annual_cost_drag"),
            "excess_vs_cash": ((best or {}).get("vs_cash") or {}).get("annual_excess"),
            "t_vs_cash": ((best or {}).get("vs_cash") or {}).get("t_stat"),
            "excess_vs_benchmark":
                ((best or {}).get("vs_benchmark") or {}).get("annual_excess"),
            "marginal_portfolio_value":
                (best or {}).get("marginal_portfolio_value"),
            "qualifies": bool((best or {}).get("qualifies")),
            "rejection_reason": s.get("rejection_reason"),
        })
    corr = correlation_map(sleeve_paths)
    clusters = latent_clusters(corr)
    ranked = sorted(
        [r for r in rows if r["t_vs_cash"] is not None],
        key=lambda r: -float(r["t_vs_cash"]))
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "created_at": created_at or _dt.datetime.now().isoformat(timespec="seconds"),
        "is_research_comparison_not_an_allocator": True,
        "produces_portfolio_target": False,
        "question": _contract.QUESTION,
        "rows": rows,
        "ranked_by_excess_over_cash": [r["sleeve"] for r in ranked],
        "strongest_sleeve": ranked[0]["sleeve"] if ranked else None,
        "second_strongest_sleeve": ranked[1]["sleeve"] if len(ranked) > 1 else None,
        "cash_row": {"asset": "CASH", "is_a_real_choice": True,
                     "beaten_by": [r["sleeve"] for r in rows
                                   if r.get("t_vs_cash") is not None
                                   and float(r["t_vs_cash"]) > 0.0]},
        "correlation_map": corr,
        "latent_risk_clusters": clusters,
        "cluster_threshold": CLUSTER_CORRELATION,
        "common_overlap": overlap,
        "inherited_equity_selection": inherited,
        "information_gaps": list(information_gaps),
        "judge_behaviour_hash": _judge.behaviour_hash(),
        "primary_verdict": verdict.get("primary_verdict"),
        "qualified_sleeves": verdict.get("qualified_sleeves", []),
    }
    body = r32.artifact_body(FRONTIER_SCHEMA, payload)
    body["frontier_hash"] = r32.sha(payload)
    return body


def _score(row: dict) -> float:
    t = (row.get("vs_cash") or {}).get("t_stat")
    return float(t) if t is not None and np.isfinite(float(t)) else float("-inf")


def path_for(campaign_id: str = _contract.CAMPAIGN_ID) -> Path:
    return r32.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> Path:
    return r32.write_json(path_for(body["campaign_id"]), body)
