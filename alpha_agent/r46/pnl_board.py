"""alpha_agent.r46.pnl_board - the P&L leaderboard. The evidence board, priced.

Release 46's board ranks by evidence maturity band first and measured edge
second, and that rule stands: two observations never outrank five hundred.
Release 46.4 extends every row with what the strategy has actually EARNED -
net forward P&L, residual alpha P&L, return on capital, realised Sharpe where
valid, drawdown, turnover, cost drag, hit rate, calibration, marginal
diversification value and the shadow weight it holds - and ranks INSIDE an
evidence band by net forward P&L rather than by a statistic.

The consequence the release asks for is enforced by the ranking key and by
the ``economic_state`` column together: a strong t-statistic with negative
economic P&L sorts below a weaker one that made money, and it reads
ECONOMIC_WATCH or ECONOMIC_KILL_CANDIDATE next to its t.

Read-only over the artifacts their owners persisted; computes no P&L.
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

CALCULATION_OWNER = "alpha_agent.r46.pnl_board"

ARTIFACT = "R46_4_PNL_LEADERBOARD.json"

_BAND = dict(LB.EVIDENCE_BANDS)

HEADLINE_FIELDS = ("net_forward_pnl", "residual_alpha_pnl", "return_on_capital",
                   "realised_sharpe", "max_drawdown", "turnover", "cost_drag",
                   "hit_rate", "calibration", "marginal_diversification",
                   "shadow_weight")


def _rank_key(row: dict):
    band = _BAND.get(row.get("state"), 7)
    eff = -int(row.get("effective_independent") or 0)
    econ_penalty = 1 if row.get("economic_state") in (
        SP.ECON_WATCH, SP.ECON_KILL_CANDIDATE) else 0
    net = row.get("net_forward_pnl")
    net = -float(net) if net is not None else 0.0
    return (band, econ_penalty, eff, net)


def build(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID,
          board: dict = None) -> dict:
    b = board if board is not None else (
        read_json(campaign_dir(campaign_id) / LB.ARTIFACT, default=None) or {})
    spnl = read_json(campaign_dir(campaign_id) / SP.ARTIFACT,
                     default=None) or {}
    strat = {s["challenger_id"]: s for s in (spnl.get("strategies") or ())}
    alloc = read_json(campaign_dir(campaign_id) / AL.ARTIFACT,
                      default=None) or {}
    weights = alloc.get("canonical_weights") or {}
    risk_state = read_json(campaign_dir(campaign_id) / RK.ARTIFACT,
                           default=None) or {}
    md = risk_state.get("marginal_diversification") or {}

    rows = []
    for r in (b.get("rows") or ()):
        cid = r.get("challenger_id")
        s = strat.get(cid, {})
        ce = s.get("capital_efficiency") or {}
        row = dict(r)
        row.update({
            "net_forward_pnl": s.get("cum_net_return"),
            "gross_forward_pnl": s.get("cum_gross_return"),
            "residual_alpha_pnl": s.get("cum_residual_alpha"),
            "realised_pnl": s.get("realised_net_return"),
            "unrealised_pnl": s.get("unrealised_net_return"),
            "return_on_capital": ce.get("net_return_on_capital"),
            "realised_sharpe": s.get("sharpe_annualised"),
            "max_drawdown_pnl": s.get("max_drawdown"),
            "turnover_per_unit_capital": ce.get("turnover_per_unit_capital"),
            "cost_drag": s.get("cum_cost_return"),
            "cost_drag_share_of_gross": s.get("cost_drag_share_of_gross"),
            "net_at_2x_costs_pnl": s.get("cum_net_return_at_2x"),
            "hit_rate_closed": s.get("hit_rate_closed"),
            "calibration": s.get("calibration"),
            "pnl_per_unit_volatility": ce.get("pnl_per_unit_volatility"),
            "pnl_per_unit_drawdown": ce.get("pnl_per_unit_drawdown"),
            "pnl_per_unit_turnover": ce.get("pnl_per_unit_turnover"),
            "pnl_per_unit_cost": ce.get("pnl_per_unit_cost"),
            "marginal_diversification": md.get(cid),
            "shadow_weight": float(weights.get(cid) or 0.0),
            "economic_state": s.get("economic_state", SP.ECON_TOO_EARLY),
            "economic_reasons": s.get("economic_reasons") or [],
            "n_trades_opened": s.get("n_trades_opened", 0),
            "n_trades_closed": s.get("n_trades_closed", 0),
            "pnl_unit": "per 1.0 of strategy capital (unit economics)",
            "strong_t_with_negative_pnl_cannot_rank_high": True,
        })
        rows.append(row)
    rows.sort(key=_rank_key)
    for i, r in enumerate(rows, start=1):
        r["pnl_rank"] = i
    active = [r for r in rows if r.get("origin") == "R46_SEED"
              and r.get("state") != C.DATA_BLOCKED]
    with_pnl = [r for r in active if r.get("n_trades_opened")]
    best = max(with_pnl, key=lambda r: r.get("net_forward_pnl") or 0.0,
               default=None)
    worst = min(with_pnl, key=lambda r: r.get("net_forward_pnl") or 0.0,
                default=None)
    best_resid = max(with_pnl, key=lambda r: r.get("residual_alpha_pnl")
                     or 0.0, default=None)
    best_eff = max((r for r in with_pnl
                    if r.get("pnl_per_unit_volatility") is not None),
                   key=lambda r: r["pnl_per_unit_volatility"], default=None)
    alpha_result = alpha_result_for(rows)
    body = artifact_body(
        "r46_4_pnl_leaderboard/1", CALCULATION_OWNER,
        as_of=str(as_of),
        built_at_utc=CK.iso(CK.now_utc()),
        headline_fields=list(HEADLINE_FIELDS),
        ranking_rule="evidence maturity band first; economic WATCH/KILL sorts "
                     "below OK inside a band; then effective independent "
                     "observations; then net forward P&L - never a t-stat "
                     "alone",
        n_rows=len(rows),
        n_with_pnl=len(with_pnl),
        best_net_pnl_strategy=(best or {}).get("challenger_id"),
        best_net_pnl=(best or {}).get("net_forward_pnl"),
        worst_net_pnl_strategy=(worst or {}).get("challenger_id"),
        worst_net_pnl=(worst or {}).get("net_forward_pnl"),
        best_residual_alpha_strategy=(best_resid or {}).get("challenger_id"),
        best_capital_efficiency_strategy=(best_eff or {}).get("challenger_id"),
        economic_state_counts=spnl.get("economic_state_counts"),
        evidence_counts={"forward_pending": b.get("n_forward_pending"),
                         "early": b.get("n_early"),
                         "candidate": b.get("n_candidate"),
                         "confirmed": b.get("n_confirmed"),
                         "rejected": b.get("n_rejected"),
                         "data_blocked": b.get("n_data_blocked")},
        multiple_testing=b.get("multiple_testing"),
        ALPHA_RESULT=alpha_result["result"],
        # Named distinctly from ALPHA_RESULT: PowerShell's JSON reader is
        # case-insensitive and a same-spelling pair breaks every operator
        # script that parses this artifact.
        alpha_result_detail=alpha_result,
        no_row_may_read_proven=True,
        proven_alpha_is_not_a_state=True,
        rows=rows,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


#: The ONLY three answers the release allows. PROVEN_ALPHA is not a state.
ALPHA_NOT_YET_JUDGED = "NOT_YET_JUDGED"
ALPHA_EARLY = "EARLY_FORWARD_PNL_EVIDENCE"
ALPHA_CONFIRMED_CANDIDATE = "FORWARD_CONFIRMED_CANDIDATE"
ALPHA_RESULTS = (ALPHA_NOT_YET_JUDGED, ALPHA_EARLY, ALPHA_CONFIRMED_CANDIDATE)


def alpha_result_for(rows: list) -> dict:
    """NOT_YET_JUDGED / EARLY_FORWARD_PNL_EVIDENCE / FORWARD_CONFIRMED_CANDIDATE.

    Confirmed requires EVERY declared forward gate to have passed on at least
    one cell (the tournament's own FORWARD_CONFIRMED state); early requires
    at least one closed research trade; otherwise nothing has been judged.
    """
    active = [r for r in rows if r.get("origin") == "R46_SEED"
              and r.get("state") != C.DATA_BLOCKED]
    n_closed = sum(int(r.get("n_trades_closed") or 0) for r in active)
    confirmed = [r["challenger_id"] for r in active
                 if r.get("state") == C.FORWARD_CONFIRMED]
    if confirmed:
        result = ALPHA_CONFIRMED_CANDIDATE
    elif n_closed > 0:
        result = ALPHA_EARLY
    else:
        result = ALPHA_NOT_YET_JUDGED
    return {"result": result, "vocabulary": list(ALPHA_RESULTS),
            "n_closed_research_trades": n_closed,
            "forward_confirmed_cells": sorted(set(confirmed)),
            "proven_alpha_is_not_a_state": True}


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "HEADLINE_FIELDS", "ALPHA_RESULTS",
           "ALPHA_NOT_YET_JUDGED", "ALPHA_EARLY", "ALPHA_CONFIRMED_CANDIDATE",
           "alpha_result_for", "build"]
