"""alpha_agent.r59.report - the machine-readable R59 state report.

Section T is explicit: no dashboard, no diagnostic wall. This module assembles
what the estate now knows into ONE artifact that a person or a later run can
read, and writes nothing else.

It composes existing owners and calculates nothing of its own: the frontier
comes from :mod:`alpha_agent.r59.frontier`, the memory summary and search
burden from :mod:`alpha_agent.r59.memory`, the data frontier from
:mod:`alpha_agent.r59.opportunities`, the provider scoreboard from
:mod:`alpha_agent.r59.providers`, the analyst gate from
:mod:`alpha_agent.r59.steele` and the insider coverage from
:mod:`alpha_agent.r59.form4`.
"""
from __future__ import annotations

from typing import Optional

from .. import r59
from . import form4 as F4
from . import frontier as FR
from . import governor as GOV
from . import loop as LP
from . import memory as M
from . import opportunities as OPP
from . import providers as PR
from . import steele as ST

CALCULATION_OWNER = "alpha_agent.r59.report"
ARTIFACT = "r59_autonomous_engine_state.json"


def architecture_map() -> dict:
    """Who owns what, after reunification. Names are modules, not intentions."""
    return {
        "level_1_research_governor": {
            "discovery": "alpha_agent.r59.governor",
            "operational": "api.research_agent + engine.research_agent",
            "mandate_bridge": "api.research_bridge + engine.research_bridge",
            "owns": ["what to learn next", "expected information value",
                     "cross-asset fairness", "stop conditions"],
            "cannot": ["execute an experiment", "own capital",
                       "promote a model"],
        },
        "level_2_alphaagent": {
            "queue": "alpha_agent.autonomous_research.ResearchQueue",
            "lanes": "alpha_agent.r59.handlers (r59.* prefix routing)",
            "session": "alpha_agent.r59.loop",
            "memory": "alpha_agent.r59.memory",
            "novelty_and_graveyard": "alpha_agent.r59.memory",
            "search_burden": "alpha_agent.r59.memory.burden (COUNTED)",
            "owns": ["persistent queue", "experiment identity", "resumability",
                     "novelty", "graveyard", "burden", "next-job generation"],
        },
        "level_3_engines": {
            "economic": "alpha_agent.r59.engines.run_futures_hypothesis / "
                        "run_equity_hypothesis",
            "mathematical": "alpha_agent.r39.representation_factory "
                            "(auto-transform grammar + symbolic trees), driven "
                            "by alpha_agent.r59.engines",
            "cross_asset": "alpha_agent.r59.engines (CROSS_ASSET scope)",
            "data_opportunity": "alpha_agent.r59.opportunities",
            "information_change": "alpha_agent.r59.form4",
            "evaluation_kernel": "alpha_agent.r57.engine (Newey-West + "
                                 "Benjamini-Hochberg) and "
                                 "alpha_agent.r57.futures_tournament.simulate",
        },
        "forward_evidence": {
            "owner": "alpha_agent.r46 + alpha_agent.r52.runtime",
            "rule": "R59 references inception and record hashes only; it never "
                    "writes, backfills or rescores a forward challenger",
        },
        "not_created_by_r59": [
            "a second queue", "a second governor", "a second tournament",
            "a second evaluation kernel", "a second restart script",
        ],
    }


def bypassed_by_r56_r58() -> dict:
    """What the three preceding campaigns did not use, and what it cost."""
    return {
        "durable_queue": {
            "owner": "alpha_agent.autonomous_research",
            "bypassed": True,
            "consequence": "each campaign ran as a straight-line script, so a "
                           "finished batch was indistinguishable from finished "
                           "research and the run ended",
        },
        "mandate_bridge": {
            "owner": "api.research_bridge",
            "bypassed": True,
            "consequence": "the governor -> queue -> evidence -> gate path "
                           "existed and carried no R56-R58 traffic",
        },
        "machine_discovery": {
            "owner": "alpha_agent.r39",
            "bypassed": True,
            "consequence": "R57 and R58 enumerated 25 families by hand while a "
                           "608-candidate generative engine sat unused",
        },
        "search_burden_ledger": {
            "owner": "alpha_agent.r39.search_budget / r46.burden",
            "bypassed": True,
            "consequence": "R58 carried PRIOR_SEARCH_BURDEN = 302 as a "
                           "hand-copied constant; the counted burden across "
                           "the imported estate is materially larger",
        },
        "graveyard_and_reopen": {
            "owner": "none existed",
            "bypassed": True,
            "consequence": "no release could ask what had already been tried, "
                           "so novelty was a matter of memory rather than of "
                           "record",
        },
    }


def build(mem: Optional[M.ResearchMemory] = None, *,
          session: Optional[dict] = None, write: bool = True) -> dict:
    mem = mem or M.open_memory()
    view = FR.measure(mem)
    ready = GOV.generate_mandates(mem, limit=100, frontier_view=view)
    burden = mem.burden()

    body = {
        "calculation_owner": CALCULATION_OWNER,
        "release": r59.RELEASE,
        "campaign_id": r59.CAMPAIGN_ID,
        "architecture_map": architecture_map(),
        "bypassed_by_r56_r58": bypassed_by_r56_r58(),
        "research_memory": mem.summary(),
        "search_burden": burden,
        "multi_asset_frontier": {
            ac: {"state": r["state"], "reason": r["reason"],
                 "instruments": r["detail"].get("instruments"),
                 "burden": r["detail"].get("search_burden"),
                 "remaining_families": r["detail"].get("remaining_families"),
                 "forward_frozen": r["detail"].get("forward_frozen")}
            for ac, r in view["asset_classes"].items()},
        "substrates": view["substrates"],
        "research_still_ready": {
            "n_mandates": len(ready["mandates"]),
            "by_asset_class": LP.count_by_asset_class(ready["mandates"]),
            "top": [{"kind": m["kind"], "asset_class": m["asset_class"],
                     "family": m["family"],
                     "eiv": m["expected_information_value"]}
                    for m in ready["mandates"][:15]],
        },
        "session": session or {},
        "session_results": LP.session_results(mem),
        "data_opportunity_frontier": OPP.publish(mem),
        "provider_value_scoreboard": PR.scoreboard(mem, write=False),
        "analyst_sample_readiness": ST.readiness(),
        "insider_information": F4.coverage_report(),
        "forward_challengers": _forward(mem),
        "safety": dict(r59.SAFETY),
    }
    if write:
        p = r59.write_artifact(ARTIFACT, body)
        body["artifact_path"] = str(p)
    return body


def _forward(mem: M.ResearchMemory) -> dict:
    rows = mem.list_hypotheses(outcome=r59.HO_FORWARD_FROZEN, limit=500)
    by_release: dict = {}
    for h in rows:
        by_release.setdefault(h["release"], []).append({
            "hypothesis_id": h["hypothesis_id"],
            "challenger": (h.get("forward_challenger") or {}).get(
                "challenger_id"),
            "inception": (h.get("forward_challenger") or {}).get("inception"),
            "asset_class": h.get("asset_class"),
            "economic_family": h.get("economic_family"),
        })
    return {
        "n_frozen": len(rows),
        "by_release": {k: len(v) for k, v in by_release.items()},
        "detail": by_release,
        "rule": "inception and record hash only; scoring belongs to the "
                "R46/R52 prospective runtime and R59 never writes it",
        "r59_froze": [h["hypothesis_id"] for h in rows
                      if h["release"] == r59.RELEASE],
    }
