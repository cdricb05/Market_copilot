"""alpha_agent.r42.microstructure_check - Track R: a bounded check, then stop.

R41 found genuine information in BTC signed order flow (+21 %/yr gross at
the 5-minute grid, direction holding out of sample) and it was deeply
negative net of taker fees. That is an EXECUTION problem, not an
information problem - but converting it requires a maker execution model,
and the contract forbids assuming a posted limit order fills.

This is not the principal R42 mission. The check is bounded: can a
defensible post-only model be built from FREE data? Four components are
required (queue position or fill probability, maker fee/rebate, adverse
selection, latency). Whichever cannot be sourced is named, and the lane is
closed with an explicit blocker rather than a fabricated fill.
"""
from __future__ import annotations

from . import CAMPAIGN_ID, artifact_body, read_json, sha, write_artifact
from . import contract as C
from . import execution as EX
from . import r41_campaign_dir

CALCULATION_OWNER = "alpha_agent.r42.microstructure_check"
ARTIFACT = "MICROSTRUCTURE_EXECUTION_FEASIBILITY.json"


def component_availability() -> dict:
    """What the free frontier can and cannot supply for a maker model."""
    return {
        "queue_position_or_fill_probability": {
            "available": False,
            "requires": "L2/L3 order-book updates with per-order queue "
                        "identity, or at minimum full book snapshots at "
                        "the decision frequency, for the whole sample",
            "free_sources_examined": [
                "data.binance.vision bookTicker (best bid/ask only - gives "
                "the top of book, NOT queue position or depth ahead)",
                "Tardis.dev free tier (first day of each month only - "
                "~1.5% of the sample; cannot estimate a fill distribution "
                "conditional on the signal)",
            ],
            "blocker": "EXECUTION_MICROSTRUCTURE_DATA",
        },
        "maker_fee_or_rebate": {
            "available": True,
            "source": "published venue fee schedule (VIP0 maker)",
            "note": "the only one of the four that is free and certain",
        },
        "adverse_selection": {
            "available": False,
            "requires": "realised post-fill markout at the horizon of the "
                        "signal, conditional on having been filled - which "
                        "needs the fill model that is itself unavailable",
            "note": "this is the component that decides the answer: an "
                    "order-flow signal is exactly the signal most likely to "
                    "be filled when it is WRONG, because the flow that "
                    "moves the price is the flow that trades through the "
                    "resting order",
            "blocker": "EXECUTION_MICROSTRUCTURE_DATA",
        },
        "latency": {
            "available": False,
            "requires": "measured round-trip time from a co-located or "
                        "near-venue host, which needs an account and "
                        "infrastructure",
            "blocker": "ACCOUNT_REQUIRED",
        },
    }


def r41_microstructure_result() -> dict:
    m = read_json(r41_campaign_dir() / "crypto_micro_results.json") or {}
    body = m.get("results", m)
    rows = body.get("results") or []
    out = []
    for r in rows:
        zb = r.get("zone_b") or {}
        out.append({"symbol": r.get("symbol"), "hold_bars": r.get("hold_bars"),
                    "threshold": r.get("threshold"),
                    "zone_b_gross_ann": zb.get("gross_ann"),
                    "zone_b_cost_ann": zb.get("cost_ann"),
                    "zone_b_net_ann": zb.get("net_ann"),
                    "zone_b_t": zb.get("excess_t_hac")})
    best_gross = max((r["zone_b_gross_ann"] or -9e9) for r in out) \
        if out else None
    return {"source": "R41 artifact, unchanged", "n_candidates": len(out),
            "rows": out, "best_zone_b_gross_ann": best_gross,
            "n_net_positive": sum(1 for r in out
                                  if (r["zone_b_net_ann"] or 0) > 0)}


def run() -> dict:
    comp = component_availability()
    have = {k: v["available"] for k, v in comp.items()}
    adm = EX.maker_admissibility(have)
    body = artifact_body("r42_microstructure_execution_feasibility/1", {
        "calculation_owner": CALCULATION_OWNER,
        "track": "R - bounded execution-feasibility check on the "
                 "cost-killed signed-flow signal",
        "is_principal_mission": False,
        "required_components": list(C.MAKER_FILL_REQUIRES),
        "component_availability": comp,
        "maker_admissibility": adm,
        "r41_result_unchanged": r41_microstructure_result(),
        "assumed_limit_fill_is_forbidden": C.ASSUMED_LIMIT_FILL_IS_FORBIDDEN,
        "verdict": {
            "state": "BLOCKED_EXECUTION_MICROSTRUCTURE_DATA",
            "blocker": "EXECUTION_MICROSTRUCTURE_DATA",
            "components_missing": [k for k, v in have.items() if not v],
            "no_fills_were_fabricated": True,
            "what_would_unblock_it": "a full-sample L2 book archive at the "
                                     "decision frequency (Tardis paid tier "
                                     "or equivalent), which is a PURCHASE "
                                     "decision and is not authorised by "
                                     "this release",
            "note": "the honest position is unchanged from R41: the "
                    "information is real and the execution model that would "
                    "monetise it cannot be built from free data. Moving on.",
        },
    })
    body["microstructure_feasibility_hash"] = sha(body)
    write_artifact(ARTIFACT, body, CAMPAIGN_ID, overwrite=True)
    return body
