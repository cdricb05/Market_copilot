"""alpha_agent.r46.strategy_pnl - ONE owner for strategy P&L streams.

A strategy is a frozen challenger. Its P&L stream is the unit-capital return
series of every research trade it produced, funded or not, with each trade
weighted by its declared share of the strategy's capital (a cell's capital is
split across its overlapping cohorts, 1/horizon each, and a challenger's
capital across its cells equally). The stream is built from the trade
ledgers alone - opens, marks, closes - and it is a pure function of them.

Three things are kept apart and never summed into one headline:

* **EXPECTED** - what the model said it would earn. Every Release-46
  challenger emits ``expected_return = None`` (NOT_CALIBRATED) by contract,
  so expected P&L is reported as NOT_CALIBRATED rather than invented, and the
  forecast error that section 22 asks for is reported as UNAVAILABLE until a
  challenger version that states a magnitude exists;
* **UNREALISED** - open trades at their point-in-time mark;
* **REALISED** - closed trades at the judge's number.

Economic governance (sections 24-25) lives here as FROZEN rules, applied to
the stream, never to the specification: a strategy that fails them becomes
``ECONOMIC_KILL_CANDIDATE`` for the shadow allocator, which then gives it no
shadow capital. Its scientific state on the tournament board is untouched,
its predictions keep emitting and maturing, and nothing about it is edited -
a changed strategy is a new version with a new clock, by the R46 rule.
"""
from __future__ import annotations

import datetime as _dt
import math
from typing import Optional

import numpy as np

from . import CAMPAIGN_ID, artifact_body, campaign_dir, write_json
from . import clock as CK
from . import trades as TR

CALCULATION_OWNER = "alpha_agent.r46.strategy_pnl"

ARTIFACT = "R46_4_STRATEGY_PNL.json"

MIN_SESSIONS_FOR_SHARPE = 20

#: Economic state vocabulary.
ECON_TOO_EARLY = "ECONOMIC_TOO_EARLY"
ECON_OK = "ECONOMIC_OK"
ECON_WATCH = "ECONOMIC_WATCH"
ECON_KILL_CANDIDATE = "ECONOMIC_KILL_CANDIDATE"
ECON_STATES = (ECON_TOO_EARLY, ECON_OK, ECON_WATCH, ECON_KILL_CANDIDATE)

#: FROZEN economic kill rules. A kill needs a sample, never one trade.
KILL_RULES = {
    "min_closed_trades_before_kill": 20,
    "min_closed_trades_before_watch": 5,
    "kill_if_cum_residual_alpha_negative_and_t_below": -1.5,
    "kill_if_net_positive_but_negative_at_2x": True,        # cost fragility
    "kill_if_max_drawdown_below": -0.20,                    # of unit capital
    "kill_if_reconciliation_mismatches": True,              # implementation
    "kill_if_data_blocked_trades_at_least": 3,              # liquidity/data
    "catastrophic_single_trade_loss_below": -0.15,          # flagged, not kill
    "no_kill_from_one_unlucky_trade": True,
    "a_killed_strategy_is_never_retuned_in_place": True,
}

#: FROZEN shadow scaling rules, read by the allocator.
SCALING_RULES = {
    "evidence_floor_share": 0.10,
    "evidence_full_share_at_gate": 1.0,
    "edge_discount_when_early_evidence_negative": 0.5,
    "edge_discount_when_watch": 0.25,
    "weight_when_kill_candidate": 0.0,
    "never_scaled_on_yesterday_alone": True,
}


# --------------------------------------------------------------------------- #
# Streams
# --------------------------------------------------------------------------- #
def _g_prev(marks_sorted: list, session: str) -> float:
    g = 0.0
    for m in marks_sorted:
        if m["session"] < session:
            g = float(m.get("gross_return") or 0.0)
        else:
            break
    return g


def unit_streams(campaign_id: str = CAMPAIGN_ID,
                 as_of: _dt.date = None) -> dict:
    """{challenger_id: {session: {net, gross, cost, control, residual,
    n_open, exposure}}} from the trade ledgers, through ``as_of``."""
    limit = str(as_of) if as_of else "9999-12-31"
    opens = {r["research_trade_id"]: r for r in TR.opens(campaign_id)}
    marks: dict = {}
    for m in TR.marks(campaign_id):
        if m["session"] <= limit:
            marks.setdefault(m["research_trade_id"], []).append(m)
    for v in marks.values():
        v.sort(key=lambda m: m["session"])
    closes = {r["research_trade_id"]: r for r in TR.closes(campaign_id)
              if str(r.get("exit_session")) <= limit}

    streams: dict = {}

    def bump(cid, session, **parts):
        row = streams.setdefault(cid, {}).setdefault(session, {
            "net": 0.0, "gross": 0.0, "cost": 0.0, "control": 0.0,
            "residual": 0.0, "n_open": 0, "exposure": 0.0})
        for k, v in parts.items():
            row[k] = row.get(k, 0.0) + (v or 0.0)

    for tid, o in opens.items():
        if str(o["entry_session"]) > limit:
            continue
        cid = o["challenger_id"]
        share = float(o.get("weight_within_strategy") or 0.0)
        cost = float(o.get("cost_return") or 0.0)
        gross_exp = float(o.get("gross_exposure_per_unit_capital") or 0.0)
        bump(cid, str(o["entry_session"]), net=-share * cost,
             cost=share * cost)
        prev_g, prev_ctl = 0.0, 0.0
        for m in marks.get(tid, []):
            g = float(m.get("gross_return") or 0.0)
            ctl = float(m.get("control_return") or 0.0)
            bump(cid, m["session"], net=share * (g - prev_g),
                 gross=share * (g - prev_g),
                 control=share * (ctl - prev_ctl),
                 residual=share * ((g - prev_g) - (ctl - prev_ctl)),
                 n_open=1, exposure=share * gross_exp)
            prev_g, prev_ctl = g, ctl
        c = closes.get(tid)
        if c is not None:
            g = float(c.get("gross_return") or 0.0)
            ctl = float(c.get("control_return") or 0.0)
            bump(cid, str(c["exit_session"]), net=share * (g - prev_g),
                 gross=share * (g - prev_g),
                 control=share * (ctl - prev_ctl),
                 residual=share * ((g - prev_g) - (ctl - prev_ctl)))
    for cid in streams:
        streams[cid] = dict(sorted(streams[cid].items()))
    return streams


def net_series(streams: dict) -> dict:
    """{challenger_id: {session: net}} - what the risk owner consumes."""
    return {cid: {s: row["net"] for s, row in rows.items()}
            for cid, rows in streams.items()}


# --------------------------------------------------------------------------- #
# Summaries
# --------------------------------------------------------------------------- #
def _t(x: list) -> Optional[float]:
    a = [float(v) for v in x if v is not None and math.isfinite(float(v))]
    if len(a) < 2:
        return None
    sd = float(np.std(a, ddof=1))
    if sd <= 0:
        return None
    v = float(np.mean(a) / (sd / math.sqrt(len(a))))
    return v if math.isfinite(v) else None


def _max_dd(path: list) -> Optional[float]:
    if not path:
        return None
    peak, dd = 0.0, 0.0
    for v in path:
        peak = max(peak, v)
        dd = min(dd, v - peak)
    return float(dd)


def summarise_strategy(cid: str, entry: dict, stream: dict, opens: list,
                       marks: dict, closes: list, blocked_trades: int,
                       as_of: _dt.date) -> dict:
    sessions = sorted(stream)
    net = [stream[s]["net"] for s in sessions]
    cum = float(np.cumsum(net)[-1]) if net else 0.0
    path = list(np.cumsum(net)) if net else []
    gross = float(sum(stream[s]["gross"] for s in sessions))
    cost = float(sum(stream[s]["cost"] for s in sessions))
    control = float(sum(stream[s]["control"] for s in sessions))
    residual = float(sum(stream[s]["residual"] for s in sessions))

    my_closes = [c for c in closes if c["challenger_id"] == cid]
    my_opens = [o for o in opens if o["challenger_id"] == cid]
    closed_ids = {c["research_trade_id"] for c in my_closes}
    realised = float(sum(float(o.get("weight_within_strategy") or 0.0)
                         * float(c.get("net_return") or 0.0)
                         for c in my_closes
                         for o in my_opens
                         if o["research_trade_id"] == c["research_trade_id"]))
    unreal = 0.0
    n_open = 0
    for o in my_opens:
        if o["research_trade_id"] in closed_ids:
            continue
        n_open += 1
        mm = marks.get(o["research_trade_id"]) or []
        last = mm[-1] if mm else None
        share = float(o.get("weight_within_strategy") or 0.0)
        unreal += share * (float(last.get("net_return")) if last
                           else -float(o.get("cost_return") or 0.0))
    resid_closed = [c.get("residual_alpha_vs_control") for c in my_closes]
    hits = [1.0 if c.get("hit") else 0.0 for c in my_closes]
    turnover = float(sum(float(o.get("weight_within_strategy") or 0.0)
                         * 2.0 * float(o.get(
                             "gross_exposure_per_unit_capital") or 0.0)
                         for o in my_opens))
    exposures = [stream[s]["exposure"] for s in sessions
                 if stream[s]["n_open"]]
    avg_exposure = float(np.mean(exposures)) if exposures else 0.0
    sharpe = None
    vol = None
    if len(net) >= MIN_SESSIONS_FOR_SHARPE:
        sd = float(np.std(net, ddof=1))
        if sd > 0:
            vol = sd * math.sqrt(252.0)
            sharpe = float(np.mean(net) / sd * math.sqrt(252.0))
    dd = _max_dd(path)
    net_2x = cum - cost          # cost charged once more
    worst_trade = min((float(c.get("net_return") or 0.0) for c in my_closes),
                      default=None)
    recon_bad = sum(1 for c in my_closes
                    if (c.get("reconciliation") or {}).get("state")
                    == "RECONCILIATION_MISMATCH")

    econ = economic_state(n_closed=len(my_closes), cum_net=cum,
                          cum_residual=residual, t_residual=_t(resid_closed),
                          net_at_2x=net_2x, max_drawdown=dd,
                          reconciliation_mismatches=recon_bad,
                          data_blocked_trades=blocked_trades,
                          worst_trade=worst_trade)
    return {
        "challenger_id": cid,
        "challenger_version": entry.get("challenger_version"),
        "asset_class": entry.get("asset_class"),
        "economic_family": entry.get("family"),
        "information_family": entry.get("information_family"),
        "dependence_cluster": entry.get("dependence_cluster"),
        "horizons": entry.get("horizons"),
        "as_of": str(as_of),
        "n_sessions": len(sessions),
        "first_session": sessions[0] if sessions else None,
        "last_session": sessions[-1] if sessions else None,
        "n_trades_opened": len(my_opens),
        "n_trades_closed": len(my_closes),
        "n_trades_open": n_open,
        # ---- three concepts, never one headline ------------------------- #
        "expected_net_edge": None,
        "expected_state": "NOT_CALIBRATED",
        "forecast_error": "UNAVAILABLE_NO_MAGNITUDE_FORECAST",
        "unrealised_net_return": unreal,
        "realised_net_return": realised,
        # ---- the stream ------------------------------------------------- #
        "cum_net_return": cum,
        "cum_gross_return": gross,
        "cum_cost_return": cost,
        "cum_control_return": control,
        "cum_residual_alpha": residual,
        "cum_net_return_at_2x": net_2x,
        "cost_drag_share_of_gross": (cost / abs(gross) if gross else None),
        "annualised_vol": vol,
        "sharpe_annualised": sharpe,
        "max_drawdown": dd,
        "t_residual_alpha_closed": _t(resid_closed),
        "hit_rate_closed": (float(np.mean(hits)) if hits else None),
        # ---- capital efficiency (section 13) ---------------------------- #
        "capital_efficiency": {
            "net_return_on_capital": cum,
            "pnl_per_unit_volatility": (None if not vol else cum / vol),
            "pnl_per_unit_drawdown": (None if not dd else cum / abs(dd)),
            "pnl_per_unit_turnover": (None if not turnover else cum / turnover),
            "pnl_per_unit_cost": (None if not cost else cum / cost),
            "pnl_per_unit_gross_exposure": (None if not avg_exposure
                                            else cum / avg_exposure),
            "turnover_per_unit_capital": turnover,
            "average_gross_exposure": avg_exposure,
            "margin_usage": "NOT_MODELLED_FULLY_COLLATERALISED",
        },
        # ---- calibration (section 23) ----------------------------------- #
        "calibration": {
            "directional_skill_hit_rate": (float(np.mean(hits))
                                           if hits else None),
            "n_closed": len(my_closes),
            "magnitude_skill": "NOT_CALIBRATED_NO_MAGNITUDE_FORECAST",
            "economic_pnl_skill_cum_residual": residual,
        },
        "worst_closed_trade_net": worst_trade,
        "reconciliation_mismatches": recon_bad,
        "economic_state": econ["state"],
        "economic_reasons": econ["reasons"],
        "economic_flags": econ["flags"],
        "evidence_class": "TRUE_FORWARD_STREAM",
        "unit_capital": 1.0,
    }


def economic_state(*, n_closed: int, cum_net: float, cum_residual: float,
                   t_residual: Optional[float], net_at_2x: float,
                   max_drawdown: Optional[float],
                   reconciliation_mismatches: int, data_blocked_trades: int,
                   worst_trade: Optional[float]) -> dict:
    """Apply the FROZEN economic rules to one strategy's stream."""
    R = KILL_RULES
    reasons, flags = [], []
    if worst_trade is not None and \
            worst_trade < R["catastrophic_single_trade_loss_below"]:
        flags.append("CATASTROPHIC_SINGLE_TRADE_LOSS_FLAGGED_FOR_REVIEW")
    if reconciliation_mismatches and R["kill_if_reconciliation_mismatches"]:
        reasons.append("IMPLEMENTATION_INSTABILITY_RECONCILIATION_MISMATCH")
    if data_blocked_trades >= R["kill_if_data_blocked_trades_at_least"]:
        reasons.append("LIQUIDITY_OR_DATA_FAILURE")
    if n_closed >= R["min_closed_trades_before_kill"]:
        if cum_residual < 0 and t_residual is not None and \
                t_residual < R["kill_if_cum_residual_alpha_negative_and_t_below"]:
            reasons.append("PERSISTENTLY_NEGATIVE_RESIDUAL_PNL")
        if cum_net < 0 and t_residual is not None and \
                t_residual < R["kill_if_cum_residual_alpha_negative_and_t_below"]:
            reasons.append("PERSISTENTLY_NEGATIVE_NET_PNL")
        if R["kill_if_net_positive_but_negative_at_2x"] and cum_net > 0 \
                and net_at_2x < 0:
            reasons.append("SEVERE_COST_FRAGILITY")
        if max_drawdown is not None and \
                max_drawdown < R["kill_if_max_drawdown_below"]:
            reasons.append("UNACCEPTABLE_DRAWDOWN")
    if reasons:
        state = ECON_KILL_CANDIDATE
    elif n_closed >= R["min_closed_trades_before_watch"] and cum_net < 0:
        state = ECON_WATCH
    elif n_closed >= R["min_closed_trades_before_watch"]:
        state = ECON_OK
    else:
        state = ECON_TOO_EARLY
    return {"state": state, "reasons": reasons, "flags": flags}


def build(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID,
          registry: dict = None, write: bool = True) -> dict:
    from . import registry as RG
    reg = registry if registry is not None else RG.load(campaign_id)
    entries = {c["challenger_id"]: c for c in (reg.get("challengers") or ())}
    streams = unit_streams(campaign_id, as_of)
    opens = TR.opens(campaign_id)
    marks: dict = {}
    for m in TR.marks(campaign_id):
        if m["session"] <= str(as_of):
            marks.setdefault(m["research_trade_id"], []).append(m)
    for v in marks.values():
        v.sort(key=lambda m: m["session"])
    closes = [c for c in TR.closes(campaign_id)
              if str(c.get("exit_session")) <= str(as_of)]
    st = TR.states(as_of, campaign_id)
    blocked_by_cid: dict = {}
    for t in st["trades"]:
        if t["state"] == TR.DATA_BLOCKED:
            blocked_by_cid[t["challenger_id"]] = \
                blocked_by_cid.get(t["challenger_id"], 0) + 1

    strategies = []
    for cid, entry in entries.items():
        strategies.append(summarise_strategy(
            cid, entry, streams.get(cid, {}), opens, marks, closes,
            blocked_by_cid.get(cid, 0), as_of))
    strategies.sort(key=lambda s: -(s["cum_net_return"] or 0.0))

    body = artifact_body(
        "r46_4_strategy_pnl/1", CALCULATION_OWNER,
        as_of=str(as_of),
        built_at_utc=CK.iso(CK.now_utc()),
        n_strategies=len(strategies),
        n_with_any_trade=sum(1 for s in strategies if s["n_trades_opened"]),
        n_with_closed_trades=sum(1 for s in strategies
                                 if s["n_trades_closed"]),
        economic_state_counts={st_: sum(1 for s in strategies
                                        if s["economic_state"] == st_)
                               for st_ in ECON_STATES},
        kill_rules=dict(KILL_RULES),
        scaling_rules=dict(SCALING_RULES),
        expected_vs_unrealised_vs_realised_are_never_summed=True,
        expected_state="NOT_CALIBRATED_BY_CONTRACT",
        best_net=(strategies[0]["challenger_id"] if strategies
                  and strategies[0]["n_trades_opened"] else None),
        worst_net=(strategies[-1]["challenger_id"] if strategies
                   and strategies[-1]["n_trades_opened"] else None),
        strategies=strategies,
        streams={cid: {s: {k: round(v, 10) if isinstance(v, float) else v
                           for k, v in row.items()}
                       for s, row in rows.items()}
                 for cid, rows in streams.items()},
    )
    if write:
        write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "ECON_STATES", "ECON_TOO_EARLY",
           "ECON_OK", "ECON_WATCH", "ECON_KILL_CANDIDATE", "KILL_RULES",
           "SCALING_RULES", "unit_streams", "net_series",
           "summarise_strategy", "economic_state", "build"]
