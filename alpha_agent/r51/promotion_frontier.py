"""alpha_agent.r51.promotion_frontier - ONE ranking by distance to promotion.

The canonical Release-51 question:

    WHICH NON-EQUITY SLEEVE IS CLOSEST TO EARNING OPERATIONAL CAPITAL,
    WHAT EXACT EVIDENCE IS STILL MISSING, AND CAN WE CLOSE THAT GAP NOW?

This module answers it from evidence that already exists. It is a PURE
calculation owner:

* every input is injected (leaderboard, velocity, verdicts, continuation
  state, the R50 sleeve records, unit economics, NAV) - nothing is read from
  disk and nothing is written;
* the promotion-distance score PRIORITISES WORK and never replaces the real
  gates: ``alpha_agent.r46.contract.FORWARD_EVIDENCE_GATES`` remain the only
  thing that can make a sleeve PROMOTION_READY, and this module only reports
  a sleeve as such when the tournament's own leaderboard already carries a
  ``FORWARD_CONFIRMED`` cell for it AND no structural deficit stands;
* excitement is not a ranking. The primary key is the honest number of weeks
  of forward evidence still missing at the currently projected velocity, and
  a sleeve with no working data path ranks behind every sleeve with one.

Research only. Ranks nothing into capital. Promotes nothing.
"""
from __future__ import annotations

import math

from . import RELEASE
from ..r46 import contract as C46

CALCULATION_OWNER = "alpha_agent.r51.promotion_frontier"

SCHEMA = "r51_promotion_frontier/1"

#: The real gates, imported - never copied - so this module can never drift
#: from the contract that actually decides.
GATES = C46.FORWARD_EVIDENCE_GATES

#: The most permissive effective-independent floor across the declared
#: horizon menu. A sleeve's evidence distance is measured against the floor
#: of its FASTEST-accruing eligible cell, because promotion needs ONE cell
#: through the gate, not all of them.
MIN_EFFECTIVE_FLOOR = min(GATES["min_effective_independent"].values())

#: How a research challenger's asset class maps onto the R50 investability
#: registry's sleeve records. The mapping is DATA. ``None`` means the
#: expression has no operational sleeve of its own and is reported under the
#: sleeve that would execute it (a credit-ETF book executes as US cash
#: equities; an event lane is registered as the event/macro record).
ASSET_CLASS_TO_SLEEVE = {
    "US_EQUITY": "us_equity_fundamental_momentum_50_50_v1",
    "US_ETF": "us_equity_fundamental_momentum_50_50_v1",
    "EQUITY_INDEX": "sleeve_equity_index_futures",
    "RATES": "sleeve_rates_futures",
    "COMMODITY": "sleeve_commodity_futures",
    "VOLATILITY": "sleeve_volatility_futures",
    "FX": "sleeve_fx_futures",
    "MULTI_ASSET_FUTURES": "sleeve_multi_asset_futures_trend",
    "FUTURES": "sleeve_multi_asset_futures_trend",
    "CREDIT": "sleeve_event_macro",
    "CRYPTO_MARKET_STRUCTURE": "sleeve_crypto_futures",
    "UNDECLARED": None,
}

#: A synthetic reporting row for strategies that span several futures
#: sleeves at once (time-series trend, cross-sectional futures momentum, COT
#: positioning). Operationally each LEG would execute through its own R50
#: sleeve; scientifically the evidence belongs to one strategy, so it is
#: ranked once here rather than triple-counted.
MULTI_SLEEVE_ID = "sleeve_multi_asset_futures_trend"

#: Adopted prior-release shadows, keyed to their R46.6.1 continuation lanes.
#: Their leaderboard rows still carry the pre-continuation DATA_BLOCKED
#: statement; the CONTINUATION state is the current truth and this mapping is
#: how the frontier reads it. The two BTC shadows stay mapped to the crypto
#: sleeve because their retirement is a sleeve-level fact.
ADOPTED_CONTINUATION_LANES = {
    "shadow_wide_xs": "r39_fut_month_end",
    "shadow_carry_rule_xs": "r39_fut_month_end",
    "shadow_vx_carry_ts": "r39_vx_weekly",
    "shadow_intl_rates_carry_rv": "r40_fut_month_end",
    "shadow_slot5_c39_fad367467c79": "r40_fut_month_end",
}

#: Structural deficit vocabulary. Every deficit is a NAMED fact with an owner
#: somewhere else; this module only collects them.
DEFICIT_FORWARD_SAMPLE = "FORWARD_SAMPLE_DEFICIT"
DEFICIT_DATA_BLOCKED = "DATA_PATH_BLOCKED"
DEFICIT_EXECUTION_PATH = "NO_R50_EXECUTION_PATH"
DEFICIT_GRANULARITY = "UNIT_GRANULARITY_AT_NAV"
DEFICIT_LIQUIDITY = "LIQUIDITY_EVIDENCE_UNAVAILABLE"
DEFICIT_CONTROL = "DECLARED_CONTROL_UNAVAILABLE"
DEFICIT_STATISTICAL = "STATISTICAL_GATE_UNMET"
DEFICIT_COST = "COST_ROBUSTNESS_UNPROVEN"

#: Historical evidence citations, frozen prior releases only. These entries
#: NOMINATE - under the R46 contract they may never be netted against the
#: forward ledger, and the ranking never reads their numbers as evidence.
PRIOR_EVIDENCE = {
    "sleeve_fx_futures": [
        {"release": "R36", "finding": "cross-sectional FX carry rank IC "
                                      "0.155, t 7.97 - HISTORICAL ONLY, "
                                      "never frozen prospectively"},
        {"release": "R43", "finding": "carry premium real; timing signal "
                                      "zero (third sighting)"},
    ],
    "sleeve_fx_spot": [
        {"release": "R36", "finding": "same carry family as above, spot "
                                      "expression; no owned volume data for "
                                      "liquidity evidence"},
    ],
    "sleeve_volatility_futures": [
        {"release": "R38", "finding": "VX term carry looked strong against "
                                      "the WRONG comparator"},
        {"release": "R46.6.1", "finding": "comparator corrected: a "
                                          "collateralised short-vol book is "
                                          "judged against cash on the "
                                          "capital it ties up"},
        {"release": "R45", "finding": "variance risk premium 4.50 vol "
                                      "points, t 9.39 - real, insurance "
                                      "revenue, NOT alpha, excluded by name"},
    ],
    "sleeve_rates_futures": [
        {"release": "R33", "finding": "owned futures constitute ONE market; "
                                      "no predictive edge found"},
        {"release": "R50", "finding": "every full-size rates contract "
                                      "exceeds the per-name cap at the "
                                      "~$100k production NAV; micro yield "
                                      "futures are not in the owned "
                                      "entitlement"},
    ],
    "sleeve_commodity_futures": [
        {"release": "R38", "finding": "native futures frontier delivered "
                                      "105 markets; no confirmed commodity "
                                      "alpha"},
    ],
    "sleeve_crypto_futures": [
        {"release": "R41", "finding": "BTC funding carry survived "
                                      "historical prosecution"},
        {"release": "R42", "finding": "the premium prices BELOW remunerated "
                                      "cash collateral on the correct "
                                      "control - DO_NOT_BUY stands"},
    ],
    "sleeve_equity_index_futures": [
        {"release": "R39", "finding": "WIDE cross-section survived factor "
                                      "residualisation (t 2.58) - "
                                      "historical; its adopted shadow "
                                      "continues month-end via R46.6.1"},
    ],
    "sleeve_event_macro": [
        {"release": "R45", "finding": "R44's gold event effect died on the "
                                      "370 events it never scored"},
    ],
    MULTI_SLEEVE_ID: [
        {"release": "R33", "finding": "the whole owned futures panel is "
                                      "effectively ONE market in stress; "
                                      "diversification claims must be "
                                      "measured, not assumed"},
    ],
}


def _weeks(deficit_effective: float, per_week: float):
    if deficit_effective <= 0:
        return 0.0
    if per_week is None or per_week <= 0:
        return None                                    # honest: unknown/infinite
    return round(deficit_effective / per_week, 2)


def _cluster_velocity(velocity: dict) -> dict:
    """{cluster_name: projected effective per week} from the velocity artifact.

    Prefers the artifact's own per-cluster projection
    (``projections.per_cluster``); falls back to five eligible sessions of a
    cell's ``expected_effective_per_session``.
    """
    out = _plan_velocity(
        (velocity.get("projections") or {}).get("per_cluster"))
    if out:
        return out
    for row in (velocity.get("cells") or ()):
        cl = row.get("dependence_cluster")
        per_session = row.get("expected_effective_per_session")
        if cl and per_session is not None:
            out[cl] = max(out.get(cl, 0.0), 5.0 * float(per_session or 0.0))
    return out


def _plan_velocity(plan_per_cluster) -> dict:
    out = {}
    for row in (plan_per_cluster or ()):
        cl = row.get("cluster")
        if cl:
            out[cl] = float(row.get("projected_effective_per_week") or 0.0)
    return out


def build(*, leaderboard: dict, velocity: dict, verdicts: dict,
          continuation: dict, sleeves: list, unit_economics: dict,
          nav_usd: float, name_cap_fraction: float = 0.10,
          plan_per_cluster=None, as_of: str = None) -> dict:
    """Assemble the promotion frontier. Pure; mutates none of its inputs.

    ``unit_economics`` maps a sleeve_id to
    ``{"smallest_unit_notional_usd": float|None, "smallest_unit_symbol": str,
    "margin_usd": float|None}`` measured from owned reference data by the
    caller. ``sleeves`` is ``api.investability_registry.declared_sleeves()``
    (or a fixture with the same shape), injected so this research module
    never imports the operational package.
    """
    sleeve_by_id = {s.get("sleeve_id"): s for s in (sleeves or ())}
    vel = _plan_velocity(plan_per_cluster) or _cluster_velocity(velocity)

    # ---- group tournament rows by operational sleeve ----------------------- #
    lanes = (continuation or {}).get("lane_results") or {}
    rows_by_sleeve: dict = {}
    for row in (leaderboard.get("rows") or ()):
        cid = str(row.get("challenger_id"))
        if cid in ADOPTED_CONTINUATION_LANES:
            sid = "adopted_continuation::" + ADOPTED_CONTINUATION_LANES[cid]
        else:
            sid = ASSET_CLASS_TO_SLEEVE.get(str(row.get("asset_class")))
            if sid is None:
                sid = "UNMAPPED_" + cid
        rows_by_sleeve.setdefault(sid, []).append(row)

    frontier, ready = [], []
    for sid, rows in sorted(rows_by_sleeve.items()):
        rec = sleeve_by_id.get(sid) or {}
        is_equity = sid == ASSET_CLASS_TO_SLEEVE["US_EQUITY"]
        adopted_lane = (lanes.get(sid.split("::", 1)[1])
                        if sid.startswith("adopted_continuation::") else None)
        econ = unit_economics.get(sid) or {}

        active = [r for r in rows if r.get("state") != "DATA_BLOCKED"]
        best_eff = max((float(r.get("effective_independent") or 0.0)
                        for r in rows), default=0.0)
        emitted = sum(int(r.get("forward_predictions_emitted") or 0)
                      for r in rows)
        matured = sum(int(r.get("forward_predictions_matured") or 0)
                      for r in rows)
        confirmed = [r for r in rows if r.get("state") == "FORWARD_CONFIRMED"]

        # Fastest eligible horizon's effective floor for THIS sleeve's cells.
        floors = [GATES["min_effective_independent"].get(int(r["horizon"]))
                  for r in active
                  if r.get("horizon") is not None
                  and int(r["horizon"]) in GATES["min_effective_independent"]]
        floor = min([f for f in floors if f], default=MIN_EFFECTIVE_FLOOR)
        deficit = max(0.0, float(floor) - best_eff)

        clusters = sorted({str(r.get("challenger_id")) for r in rows})
        sleeve_velocity = max(
            (vel.get(str(cl), 0.0) for cl in
             {c for r in active
              for c in ([_cluster_of(r, verdicts)] if _cluster_of(r, verdicts)
                        else [])}),
            default=0.0)
        weeks = _weeks(deficit, sleeve_velocity)

        deficits = []
        if deficit > 0:
            deficits.append({
                "code": DEFICIT_FORWARD_SAMPLE,
                "detail": "needs %.0f more effective independent forward "
                          "observations against a floor of %d; best cell "
                          "holds %.0f" % (deficit, floor, best_eff)})
        if rows and not active:
            deficits.append({
                "code": DEFICIT_DATA_BLOCKED,
                "detail": "every mapped challenger is DATA_BLOCKED; no "
                          "forward evidence can accrue on this path"})
        if not rec and not is_equity:
            if sid == MULTI_SLEEVE_ID:
                deficits.append({
                    "code": "NO_SINGLE_SLEEVE_RECORD",
                    "detail": "a multi-asset futures basket spans several "
                              "R50 sleeves; each LEG is executable through "
                              "its own sleeve record, but an approval for "
                              "the strategy as one unit needs a registry "
                              "record of its own"})
            else:
                deficits.append({
                    "code": DEFICIT_EXECUTION_PATH,
                    "detail": "no R50 investability-registry record "
                              "represents this expression; approval would "
                              "have nowhere to land"})
        smallest = econ.get("smallest_unit_notional_usd")
        if smallest is not None and nav_usd and \
                smallest > float(nav_usd) * float(name_cap_fraction):
            deficits.append({
                "code": DEFICIT_GRANULARITY,
                "detail": "smallest owned unit (%s) is $%.0f notional; the "
                          "%.0f%% name cap at the $%.0f NAV allows $%.0f. "
                          "Minimum NAV for one unit: $%.0f. This constrains "
                          "IMPLEMENTATION, never the science"
                          % (econ.get("smallest_unit_symbol"), smallest,
                             100 * name_cap_fraction, nav_usd,
                             nav_usd * name_cap_fraction,
                             smallest / name_cap_fraction)})
        caps = rec.get("declared_capabilities") or {}
        if caps.get("LIQUIDITY_SUPPORTED") is False:
            deficits.append({
                "code": DEFICIT_LIQUIDITY,
                "detail": "the sleeve declares LIQUIDITY_SUPPORTED False "
                          "(no owned volume data); liquidity evidence "
                          "cannot be invented and only a purchase-gate "
                          "decision could close it"})
        if matured > 0 and not confirmed:
            deficits.append({
                "code": DEFICIT_STATISTICAL,
                "detail": "matured evidence exists but no cell meets the "
                          "frozen statistical gate (t >= %.1f vs control, "
                          "CI excluding zero, FDR 0.10)"
                          % GATES["min_t_stat_net_vs_control"]})
            deficits.append({
                "code": DEFICIT_COST,
                "detail": "positive net edge at 2x costs not yet shown on "
                          "matured forward rows"})

        registry_blocker = ((rec.get("r50_activation_attempt") or {})
                            .get("remaining_blocker")
                            or (rec.get("approval_evidence") or {})
                            .get("verdict"))

        if adopted_lane is not None:
            # The leaderboard's DATA_BLOCKED on these rows predates the
            # R46.6.1 continuation; the lane lifecycle is the current truth.
            deficits = [d for d in deficits
                        if d["code"] not in (DEFICIT_DATA_BLOCKED,
                                             DEFICIT_EXECUTION_PATH)]
            deficits.append({
                "code": "CONTINUATION_CLOCK",
                "detail": "accrues through the R46-owned continuation "
                          "ledger on its own decision calendar (lifecycle "
                          "%s, next decision %s); the operational "
                          "expression is declared at promotion time, "
                          "evidence first"
                          % (adopted_lane.get("lifecycle"),
                             adopted_lane.get("next_decision_date"))})

        is_ready = bool(confirmed) and not deficits
        state = ("ALREADY_OPERATIONAL" if is_equity and rec else
                 "PROMOTION_READY" if is_ready else
                 ("RETIRED_UNTIL_DATA_AVAILABLE"
                  if str(adopted_lane.get("lifecycle")) == "RETIRED"
                  else "CONTINUATION_ARMED") if adopted_lane is not None else
                 "ACCRUING" if active else
                 "BLOCKED")

        row_out = {
            "sleeve_id": sid,
            "registry_represented": bool(rec) or is_equity,
            "asset_classes": sorted({str(r.get("asset_class"))
                                     for r in rows}),
            "strategy_families": sorted({str(r.get("family"))
                                         for r in rows}),
            "challenger_ids": clusters,
            "n_challengers": len(rows),
            "n_active": len(active),
            "forward_predictions_emitted": emitted,
            "forward_predictions_matured": matured,
            "best_effective_independent": best_eff,
            "effective_floor_fastest_cell": floor,
            "effective_deficit": deficit,
            "projected_effective_per_week": sleeve_velocity,
            "weeks_to_evidence_floor": weeks,
            "structural_deficits": deficits,
            "registry_blocker": registry_blocker
            or ("APPROVED" if is_equity else "NO_APPROVED_OPERATIONAL_SIGNAL"),
            "prior_evidence_nominations": PRIOR_EVIDENCE.get(sid, []),
            "prior_evidence_confers_no_forward_credit": True,
            "state": state,
            "exact_missing_gate": _exact_missing_gate(rows, deficits),
            "unit_economics": econ or None,
        }
        if is_equity:
            row_out["note"] = (
                "the approved operational equity model is unaffected; the "
                "deficits above describe candidate REPLACEMENT research "
                "challengers competing in the tournament, not the model "
                "that already runs capital")
        frontier.append(row_out)
        if is_ready:
            ready.append(sid)

    # ---- ranking: honest weeks first, unknown-velocity last, blocked last - #
    def _key(r):
        if r["state"] == "ALREADY_OPERATIONAL":
            return (2, 0.0, r["sleeve_id"])
        if r["state"] in ("BLOCKED", "RETIRED_UNTIL_DATA_AVAILABLE"):
            return (3, math.inf, r["sleeve_id"])
        w = r["weeks_to_evidence_floor"]
        return (0 if r["state"] == "PROMOTION_READY" else 1,
                math.inf if w is None else w,
                r["sleeve_id"])

    ranked = sorted(frontier, key=_key)
    for i, r in enumerate(ranked, start=1):
        r["promotion_distance_rank"] = i

    return {
        "schema": SCHEMA,
        "release": RELEASE,
        "calculation_owner": CALCULATION_OWNER,
        "as_of": as_of,
        "question": ("which non-equity sleeve is closest to earning "
                     "operational capital, what exact evidence is still "
                     "missing, and can we close that gap now?"),
        "ranking_rule": ("weeks of forward evidence still missing at the "
                         "projected velocity, ascending; a sleeve with no "
                         "working data path ranks behind every sleeve with "
                         "one; excitement is not a key"),
        "the_score_never_replaces_the_gates": True,
        "gates_owner": "alpha_agent.r46.contract.FORWARD_EVIDENCE_GATES",
        "nav_usd": nav_usd,
        "name_cap_fraction": name_cap_fraction,
        "n_sleeves": len(ranked),
        "promotion_ready": ready,
        "promotion_ready_count": len(ready),
        "manual_approval_required": True,
        "automatic_promotion_performed": False,
        "rows": ranked,
        "continuation_note": _continuation_note(continuation),
        "research_only": True,
    }


def _cluster_of(row: dict, verdicts: dict):
    cid = str(row.get("challenger_id"))
    for v in (verdicts.get("rows") or ()):
        if str(v.get("challenger_id")) == cid:
            return v.get("dependence_cluster")
    return None


def _exact_missing_gate(rows, deficits):
    gates = sorted({str(r.get("next_evidence_gate"))
                    for r in rows if r.get("next_evidence_gate")})
    if deficits and deficits[0]["code"] == DEFICIT_DATA_BLOCKED:
        return "DATA_PATH_BLOCKED - " + (
            "; ".join(sorted({str(r.get("blocked_reason"))[:120]
                              for r in rows if r.get("blocked_reason")}))
            or "see blocked_reason")
    return gates[0] if gates else None


def _continuation_note(continuation: dict) -> dict:
    lanes = (continuation or {}).get("lane_results") or {}
    return {
        "owner": "alpha_agent.r46.adopted_forward",
        "lanes": {k: {"lifecycle": v.get("lifecycle"),
                      "next_decision_date": v.get("next_decision_date")}
                  for k, v in lanes.items()},
        "note": ("adopted prior-release shadows accrue through the R46-owned "
                 "continuation ledger only; prior-release artifacts are "
                 "never written"),
    }
