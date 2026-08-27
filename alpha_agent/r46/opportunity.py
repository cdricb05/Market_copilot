"""alpha_agent.r46.opportunity - research opportunity cost, and the READ-ONLY bridge.

After every eligible refresh the shadow portfolio asks, strategy by strategy,
the question the operational manager asks of a holding: is this still the
best use of its capital? The answer is a research recommendation -
HOLD / REDUCE / EXIT / REPLACE / ADD - computed from the same frozen
allocation scores the canonical policy uses, against the strongest eligible
alternative, and it changes nothing: not a shadow weight (the allocator owns
those), not an operational holding (nothing here can reach one).

The research-to-portfolio bridge (section 40) is the second artifact here. It
exposes, read-only, what the portfolio manager would need to consider a
strategy as a candidate sleeve. In this release it is structurally empty of
candidates - no strategy has reached FORWARD_CONFIRMED - and it says so with
every strategy's evidence state rather than with silence.
"""
from __future__ import annotations

import datetime as _dt

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, write_json
from . import allocation as AL
from . import challengers as CH
from . import clock as CK
from . import contract as C
from . import risk as RK
from . import strategy_pnl as SP

CALCULATION_OWNER = "alpha_agent.r46.opportunity"

ARTIFACT = "R46_4_OPPORTUNITY_COST.json"
BRIDGE_ARTIFACT = "R46_4_RESEARCH_BRIDGE.json"

HOLD, REDUCE, EXIT, REPLACE, ADD = "HOLD", "REDUCE", "EXIT", "REPLACE", "ADD"
RECOMMENDATIONS = (HOLD, REDUCE, EXIT, REPLACE, ADD)

#: Frozen decision thresholds on the canonical allocation score. Compared to
#: the MEDIAN allocated score (a single low-volatility outlier must not make
#: the whole field read REDUCE) and, for REPLACE, to the strongest
#: UNALLOCATED alternative outside the strategy's own dependence cluster.
HOLD_BAND = 0.10          # within 10% of the median -> HOLD
REDUCE_SHARE = 0.50       # below half the median allocated score -> REDUCE
REPLACE_RATIO = 2.0       # an unallocated alternative twice as strong -> REPLACE


def build(as_of: _dt.date, entries: dict, evidence: dict,
          campaign_id: str = CAMPAIGN_ID) -> dict:
    alloc = read_json(campaign_dir(campaign_id) / AL.ARTIFACT,
                      default=None) or {}
    detail = alloc.get("canonical_detail") or {}
    weights = alloc.get("canonical_weights") or {}
    spnl = read_json(campaign_dir(campaign_id) / SP.ARTIFACT,
                     default=None) or {}
    strat = {s["challenger_id"]: s for s in (spnl.get("strategies") or ())}
    risk_state = read_json(campaign_dir(campaign_id) / RK.ARTIFACT,
                           default=None) or {}
    rc = risk_state.get("risk_contribution") or {}
    md = risk_state.get("marginal_diversification") or {}
    scores = {cid: float(d.get("score") or 0.0) for cid, d in detail.items()}
    ineligible = ((alloc.get("current") or {}).get(AL.CANONICAL_POLICY) or {}
                  ).get("ineligible") or {}

    allocated_scores = sorted(sc for c, sc in scores.items()
                              if float(weights.get(c) or 0.0) > 0)
    median = (allocated_scores[len(allocated_scores) // 2]
              if allocated_scores else 0.0)
    rows = []
    for cid, e in entries.items():
        s = strat.get(cid, {})
        my = scores.get(cid, 0.0)
        w = float(weights.get(cid) or 0.0)
        my_cluster = CH.cluster_for(e)
        alts = [(c, sc) for c, sc in scores.items()
                if c != cid and CH.cluster_for(entries.get(c, {})) != my_cluster]
        best_alt = max(alts, key=lambda kv: kv[1], default=(None, 0.0))
        unalloc_alts = [(c, sc) for c, sc in alts
                        if float(weights.get(c) or 0.0) <= 0]
        best_unalloc = max(unalloc_alts, key=lambda kv: kv[1],
                           default=(None, 0.0))
        if cid in ineligible:
            rec = EXIT if w > 0 else HOLD
            why = "ineligible: %s" % ", ".join(ineligible[cid])
            if w <= 0:
                rec, why = EXIT, why + " (already unallocated)"
        elif w > 0:
            if best_unalloc[0] and my > 0 and \
                    best_unalloc[1] >= REPLACE_RATIO * my:
                rec, why = REPLACE, ("%s scores %.1fx higher and holds no "
                                     "shadow capital"
                                     % (best_unalloc[0], best_unalloc[1] / my))
            elif s.get("economic_state") == SP.ECON_WATCH:
                rec, why = REDUCE, "economic state WATCH (net negative early)"
            elif median > 0 and my < REDUCE_SHARE * median:
                rec, why = REDUCE, ("score is below half the median "
                                    "allocated score")
            else:
                rec, why = HOLD, "score within the hold band of the field"
        else:
            if my > 0 and my >= median and cid not in ineligible:
                rec, why = ADD, "eligible, unallocated, scores at or above the "\
                                "allocated median (capped out by concentration)"
            else:
                rec, why = HOLD, "unallocated and not stronger than the field"
        rows.append({
            "challenger_id": cid,
            "asset_class": e.get("asset_class"),
            "economic_family": e.get("family"),
            "dependence_cluster": my_cluster,
            "shadow_weight": w,
            "allocation_score": my,
            "forward_evidence": {
                "state": max(((v.get("state"), h) for h, v in
                              (evidence.get(cid, {}).get("cells") or {})
                              .items()), default=(C.FORWARD_PENDING, None))[0],
                "raw_matured": sum(int(v.get("raw_matured") or 0) for v in
                                   (evidence.get(cid, {}).get("cells") or {})
                                   .values()),
                "effective_independent": max(
                    (int(v.get("effective_independent") or 0) for v in
                     (evidence.get(cid, {}).get("cells") or {}).values()),
                    default=0),
                "mean_net_alpha_bps": evidence.get(cid, {}).get(
                    "mean_net_alpha_bps"),
            },
            "cum_net_return": s.get("cum_net_return"),
            "realised_net_return": s.get("realised_net_return"),
            "unrealised_net_return": s.get("unrealised_net_return"),
            "max_drawdown": s.get("max_drawdown"),
            "cum_cost_return": s.get("cum_cost_return"),
            "economic_state": s.get("economic_state"),
            "risk_contribution": rc.get(cid),
            "marginal_diversification": md.get(cid),
            "strongest_alternative": best_alt[0],
            "strongest_alternative_score": best_alt[1],
            "expected_incremental_benefit_of_switching": (
                round(best_alt[1] - my, 6) if best_alt[0] else None),
            "recommendation": rec,
            "why": why,
            "changes_nothing": True,
        })
    counts = {r: sum(1 for x in rows if x["recommendation"] == r)
              for r in RECOMMENDATIONS}
    body = artifact_body(
        "r46_4_opportunity_cost/1", CALCULATION_OWNER,
        as_of=str(as_of),
        built_at_utc=CK.iso(CK.now_utc()),
        question="is each strategy still the best use of its shadow capital?",
        thresholds={"hold_band": HOLD_BAND, "reduce_share": REDUCE_SHARE,
                    "replace_ratio": REPLACE_RATIO},
        counts=counts,
        recommendations=list(RECOMMENDATIONS),
        rows=sorted(rows, key=lambda r: -(r["shadow_weight"] or 0.0)),
        mirrors_milestone_2_but_touches_no_holding=True,
        research_only=True,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)

    # ---- the READ-ONLY research-to-portfolio bridge ------------------------ #
    vols = risk_state.get("volatility") or {}
    candidates, considered = [], []
    for cid, e in entries.items():
        s = strat.get(cid, {})
        ev = evidence.get(cid, {})
        states = {v.get("state") for v in (ev.get("cells") or {}).values()}
        confirmed = C.FORWARD_CONFIRMED in states
        row = {
            "challenger_id": cid,
            "candidate_operational_sleeve": ("%s|%s" % (e.get("asset_class"),
                                                       e.get("family"))),
            "evidence_state": sorted(states) or [C.FORWARD_PENDING],
            "sufficiently_supported": confirmed,
            "expected_marginal_return": "NOT_CALIBRATED",
            "expected_residual_alpha_bps": ev.get("mean_net_alpha_bps"),
            "expected_volatility_annual": (vols.get(cid) or {}).get(
                "annual_vol"),
            "expected_volatility_source": (vols.get(cid) or {}).get("source"),
            "expected_drawdown": s.get("max_drawdown"),
            "correlation_to_operational_portfolio":
                "NOT_MEASURED_RESEARCH_AND_OPERATIONS_ARE_SEPARATE",
            "liquidity_capacity": "DECLARED_LIQUID_UNIVERSE_ONLY",
            "confidence": AL.evidence_score(ev),
            "shadow_weight": float(weights.get(cid) or 0.0),
        }
        considered.append(row)
        if confirmed:
            candidates.append(row)
    bridge = artifact_body(
        "r46_4_research_bridge/1", CALCULATION_OWNER,
        as_of=str(as_of),
        read_only=True,
        n_candidates=len(candidates),
        candidates=candidates,
        n_considered=len(considered),
        considered=considered,
        who_decides="the canonical portfolio manager, manually",
        adds_to_portfolio=False, creates_orders=False,
        creates_targets=False, creates_proposals=False,
        note="a strategy becomes a candidate only at FORWARD_CONFIRMED, the "
             "strongest state the tournament has; none has reached it",
    )
    write_json(campaign_dir(campaign_id) / BRIDGE_ARTIFACT, bridge)
    return body


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "BRIDGE_ARTIFACT",
           "RECOMMENDATIONS", "HOLD", "REDUCE", "EXIT", "REPLACE", "ADD",
           "build"]
