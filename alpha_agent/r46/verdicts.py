"""alpha_agent.r46.verdicts - ONE research assessment per challenger.

The tournament board says how much a challenger has PROVEN (evidence bands
that need sixty effective observations); the strategy-P&L owner says whether
its stream is economically OK, WATCH or a KILL candidate (rules that need
twenty closed trades). Between "nothing has matured" and "the gate is met"
lies the whole period Release 46.5 lives in, and an operator needs one honest
word for each strategy inside it. This module gives exactly one:

``TOO_EARLY``               fewer than ``min_closed_before_any_verdict``
                            matured trades - nothing may be said, in either
                            direction, however large the first outcome was
``POSITIVE_EARLY``          a small matured sample whose realised residual
                            alpha (net of cost, net of the declared control)
                            is positive - a sign, not a result
``NEGATIVE_EARLY``          the same sample, negative
``SHADOW_SCALE_CANDIDATE``  enough matured trades that the frozen SCALE rule
                            can fire: positive residual alpha with a t-stat
                            at least the declared floor, still positive at
                            2x costs, drawdown inside the band, hit rate at
                            least one half, no reconciliation mismatch, and
                            a non-negative marginal diversification
``SHADOW_REDUCE_CANDIDATE`` enough matured trades that the frozen REDUCE
                            rule can fire: persistently negative residual
                            alpha, or cost fragility, or a drawdown outside
                            the band - short of the kill threshold
``FORWARD_REJECTED``        the tournament's own rejection (forty matured,
                            t below -2) or the P&L owner's frozen kill rule
                            (twenty closed trades); permanent at this version
``FORWARD_CONFIRMED``       the tournament's full declared gate on at least
                            one cell; still confers no capital

Every threshold is declared here, before the first outcome exists, and the
verdict reads ONLY matured trades - a mark-to-market number can move a
strategy's NAV but never its verdict. Nothing is crowned: a
``SHADOW_SCALE_CANDIDATE`` is a research recommendation the allocator's
frozen policies may or may not act on at their next zero-base decision, and
``FORWARD_CONFIRMED`` remains the tournament's word, never this module's.
"""
from __future__ import annotations

import datetime as _dt

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, write_json
from . import allocation as AL
from . import clock as CK
from . import contract as C
from . import leaderboard as LB
from . import risk as RK
from . import strategy_pnl as SP

CALCULATION_OWNER = "alpha_agent.r46.verdicts"

ARTIFACT = "R46_5_STRATEGY_VERDICTS.json"

TOO_EARLY = "TOO_EARLY"
POSITIVE_EARLY = "POSITIVE_EARLY"
NEGATIVE_EARLY = "NEGATIVE_EARLY"
SCALE = "SHADOW_SCALE_CANDIDATE"
REDUCE = "SHADOW_REDUCE_CANDIDATE"
REJECTED = "FORWARD_REJECTED"
CONFIRMED = "FORWARD_CONFIRMED"
VERDICTS = (TOO_EARLY, POSITIVE_EARLY, NEGATIVE_EARLY, SCALE, REDUCE,
            REJECTED, CONFIRMED)

#: FROZEN. Declared before any Release-46 outcome existed (2026-08-26, with
#: zero matured rows on the record). A verdict reads matured trades only.
VERDICT_RULES = {
    "version": "R46_5_VERDICT_RULES_v1",
    "min_closed_before_any_verdict": 3,
    "min_closed_for_scale_or_reduce": 10,
    "scale_requires_all": {
        "realised_residual_alpha_positive": True,
        "t_residual_alpha_at_least": 1.0,
        "positive_at_2x_costs": True,
        "max_drawdown_not_below": -0.10,
        "hit_rate_at_least": 0.5,
        "no_reconciliation_mismatch": True,
        "marginal_diversification_not_negative": True,
    },
    "reduce_if_any": {
        "realised_residual_alpha_negative_and_t_at_most": -1.0,
        "net_positive_but_negative_at_2x_costs": True,
        "max_drawdown_below": -0.10,
    },
    "rejected_if": "tournament FORWARD_REJECTED on any cell, or the P&L "
                   "owner's frozen kill rule (needs %d closed trades)"
                   % SP.KILL_RULES["min_closed_trades_before_kill"],
    "confirmed_if": "tournament FORWARD_CONFIRMED on at least one cell (the "
                    "full declared evidence gate)",
    "mark_to_market_never_decides": True,
    "one_outcome_never_decides": True,
    "a_verdict_confers_no_capital": True,
}


def _cells(board: dict) -> dict:
    out: dict = {}
    for r in (board or {}).get("rows") or ():
        if r.get("origin") != "R46_SEED":
            continue
        out.setdefault(r["challenger_id"], []).append(r)
    return out


def verdict_for(*, n_closed: int, residual: float, t_residual, net_at_2x: float,
                max_drawdown, hit_rate, reconciliation_mismatches: int,
                marginal_diversification, tournament_states: set,
                economic_state: str) -> dict:
    """Apply the FROZEN rules to one strategy's MATURED record. Pure."""
    R = VERDICT_RULES
    reasons = []
    if C.FORWARD_CONFIRMED in tournament_states:
        return {"verdict": CONFIRMED,
                "reasons": ["tournament gate met on at least one cell"]}
    if C.FORWARD_REJECTED in tournament_states:
        return {"verdict": REJECTED,
                "reasons": ["tournament FORWARD_REJECTED"]}
    if economic_state == SP.ECON_KILL_CANDIDATE:
        return {"verdict": REJECTED,
                "reasons": ["frozen economic kill rule fired"]}
    if n_closed < R["min_closed_before_any_verdict"]:
        return {"verdict": TOO_EARLY,
                "reasons": ["%d matured trade(s); %d needed before any verdict"
                            % (n_closed, R["min_closed_before_any_verdict"])]}
    if n_closed >= R["min_closed_for_scale_or_reduce"]:
        red = R["reduce_if_any"]
        if residual < 0 and t_residual is not None and \
                t_residual <= red["realised_residual_alpha_negative_and_t_at_most"]:
            reasons.append("PERSISTENTLY_NEGATIVE_RESIDUAL_ALPHA")
        if red["net_positive_but_negative_at_2x_costs"] and residual > 0 \
                and net_at_2x < 0:
            reasons.append("COST_FRAGILE_AT_2X")
        if max_drawdown is not None and max_drawdown < red["max_drawdown_below"]:
            reasons.append("DRAWDOWN_OUTSIDE_BAND")
        if reasons:
            return {"verdict": REDUCE, "reasons": reasons}
        sc = R["scale_requires_all"]
        checks = {
            "residual_alpha_positive": residual > 0,
            "t_residual_alpha_at_least": (t_residual is not None
                                          and t_residual
                                          >= sc["t_residual_alpha_at_least"]),
            "positive_at_2x_costs": net_at_2x > 0,
            "drawdown_inside_band": (max_drawdown is None
                                     or max_drawdown
                                     >= sc["max_drawdown_not_below"]),
            "hit_rate_at_least_half": (hit_rate is not None
                                       and hit_rate >= sc["hit_rate_at_least"]),
            "no_reconciliation_mismatch": reconciliation_mismatches == 0,
            "marginal_diversification_not_negative": (
                marginal_diversification is None
                or marginal_diversification >= 0),
        }
        if all(checks.values()):
            return {"verdict": SCALE, "reasons": ["every frozen SCALE "
                                                  "condition holds"],
                    "checks": checks}
        return {"verdict": (POSITIVE_EARLY if residual > 0 else NEGATIVE_EARLY),
                "reasons": ["matured sample large enough to test, SCALE "
                            "conditions not all met"],
                "checks": checks}
    return {"verdict": (POSITIVE_EARLY if residual > 0 else NEGATIVE_EARLY),
            "reasons": ["%d matured trades: a sign, not a result"
                        % n_closed]}


def build(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID,
          registry: dict = None, board: dict = None,
          write: bool = True) -> dict:
    from . import registry as RG
    reg = registry if registry is not None else RG.load(campaign_id)
    b = board if board is not None else (
        read_json(campaign_dir(campaign_id) / LB.ARTIFACT, default=None)
        or {})
    cells = _cells(b)
    matured = SP.matured_summary(as_of, campaign_id)
    spnl = read_json(campaign_dir(campaign_id) / SP.ARTIFACT,
                     default=None) or {}
    econ = {s["challenger_id"]: s for s in (spnl.get("strategies") or ())}
    risk_state = read_json(campaign_dir(campaign_id) / RK.ARTIFACT,
                           default=None) or {}
    md = risk_state.get("marginal_diversification") or {}
    alloc = read_json(campaign_dir(campaign_id) / AL.ARTIFACT,
                      default=None) or {}
    weights = alloc.get("canonical_weights") or {}
    rows = []
    for c in (reg.get("challengers") or ()):
        cid = c["challenger_id"]
        m = matured.get(cid) or SP.empty_matured(cid)
        s = econ.get(cid, {})
        states = {r.get("state") for r in cells.get(cid, [])}
        v = verdict_for(
            n_closed=m["n_closed"], residual=m["cum_residual_alpha"],
            t_residual=m["t_residual_alpha"], net_at_2x=m["cum_net_at_2x"],
            max_drawdown=m["max_drawdown_realised"], hit_rate=m["hit_rate"],
            reconciliation_mismatches=m["reconciliation_mismatches"],
            marginal_diversification=md.get(cid),
            tournament_states=states,
            economic_state=s.get("economic_state", SP.ECON_TOO_EARLY))
        eff = max((int(r.get("effective_independent") or 0)
                   for r in cells.get(cid, [])), default=0)
        rows.append({
            "challenger_id": cid,
            "challenger_version": c.get("challenger_version"),
            "cohort": c.get("cohort"),
            "asset_class": c.get("asset_class"),
            "economic_family": c.get("family"),
            "information_family": c.get("information_family"),
            "dependence_cluster": c.get("dependence_cluster"),
            "verdict": v["verdict"],
            "reasons": v["reasons"],
            "scale_checks": v.get("checks"),
            "tournament_states": sorted(x for x in states if x),
            "economic_state": s.get("economic_state", SP.ECON_TOO_EARLY),
            # ---- the section-6 report, matured trades only ----------------- #
            "matured_observations": m["n_closed"],
            "effective_observations": eff,
            "net_pnl_unit": m["cum_net"],
            "residual_alpha_pnl_unit": m["cum_residual_alpha"],
            "gross_pnl_unit": m["cum_gross"],
            "return_on_capital": m["cum_net"],
            "cost_drag_unit": m["cum_cost"],
            "net_at_2x_costs_unit": m["cum_net_at_2x"],
            "survives_2x_costs": (None if m["n_closed"] == 0
                                  else bool(m["cum_net_at_2x"] > 0)),
            "max_drawdown_realised": m["max_drawdown_realised"],
            "hit_rate": m["hit_rate"],
            "t_residual_alpha": m["t_residual_alpha"],
            "calibration": {"directional_hit_rate": m["hit_rate"],
                            "magnitude": "NOT_CALIBRATED_BY_CONTRACT"},
            "diversification_contribution": md.get(cid),
            "shadow_weight": float(weights.get(cid) or 0.0),
            "net_pnl_usd_funded": m["usd_net"],
            "residual_alpha_pnl_usd_funded": m["usd_residual_alpha"],
            "mark_to_market_excluded_from_verdict": True,
        })
    order = {v: i for i, v in enumerate(
        (CONFIRMED, SCALE, POSITIVE_EARLY, TOO_EARLY, NEGATIVE_EARLY, REDUCE,
         REJECTED))}
    rows.sort(key=lambda r: (order.get(r["verdict"], 9),
                             -(r["residual_alpha_pnl_unit"] or 0.0)))
    counts = {v: sum(1 for r in rows if r["verdict"] == v) for v in VERDICTS}
    body = artifact_body(
        "r46_5_strategy_verdicts/1", CALCULATION_OWNER,
        as_of=str(as_of),
        built_at_utc=CK.iso(CK.now_utc()),
        vocabulary=list(VERDICTS),
        rules=dict(VERDICT_RULES),
        counts=counts,
        n_strategies=len(rows),
        positive_early=[r["challenger_id"] for r in rows
                        if r["verdict"] == POSITIVE_EARLY],
        negative_early=[r["challenger_id"] for r in rows
                        if r["verdict"] == NEGATIVE_EARLY],
        shadow_scale_candidates=[r["challenger_id"] for r in rows
                                 if r["verdict"] == SCALE],
        shadow_reduce_candidates=[r["challenger_id"] for r in rows
                                  if r["verdict"] == REDUCE],
        forward_rejected=[r["challenger_id"] for r in rows
                          if r["verdict"] == REJECTED],
        forward_confirmed=[r["challenger_id"] for r in rows
                           if r["verdict"] == CONFIRMED],
        best_by_residual_alpha=next((r["challenger_id"] for r in rows
                                     if r["matured_observations"]), None),
        worst_by_residual_alpha=next((r["challenger_id"] for r in reversed(rows)
                                      if r["matured_observations"]), None),
        no_false_winner=("a strategy is never crowned from a handful of "
                         "outcomes; FORWARD_CONFIRMED is the tournament's "
                         "word under its full gate"),
        rows=rows,
        research_only=True,
    )
    if write:
        write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "VERDICTS", "VERDICT_RULES",
           "TOO_EARLY", "POSITIVE_EARLY", "NEGATIVE_EARLY", "SCALE", "REDUCE",
           "REJECTED", "CONFIRMED", "verdict_for", "build"]
