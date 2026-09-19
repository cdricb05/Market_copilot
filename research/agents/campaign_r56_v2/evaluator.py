"""evaluator - the ONE shared book evaluator of campaign R56_V2.

RESEARCH ONLY. PAPER ONLY. NO ORDERS. This module simulates books on owned
historical data. It owns NO registry, gate, queue, verdict or forward clock:
the statistical kernel stays ``alpha_agent.r57.engine`` (``layer_of``,
``forward_return``, ``layer_stats``, ``nw_tstat``), the dated-contract book
accounting stays ``alpha_agent.r59.native`` (``_fwd``-equivalent compounding,
``_xs_weights``, ``_layer_stats``), and the verdict stays
``alpha_agent.r59.engines.gate`` reached through ``scripts/alpha_agents_v2.py``.

Why it exists (each item is a capability the owners do not have and the
campaign pre-registered):

* ``native.run_book`` charges NOTHING for rolls, ranks one cross-section only,
  and cannot express pre-declared pair weights (P3) or grouped sub-books (P4).
  ``run_futures_book`` reproduces its accounting exactly (parity-tested) and
  ADDS the campaign cost model FUTURES_PER_MARKET_R38_PLUS_ROLL_V1.
* ``r57.engine.run_topn`` hard-codes the large-cap eligibility (ADV >= 1e7).
  ``run_equity_topn`` is the same loop with the universe agent's
  point-in-time eligibility mask (parity-tested against ``run_topn``).
* No owner simulates an H5 next-open overlapping-tranche book.
  ``run_equity_daily_tranche_book`` is that daily-PnL book.

GATE TRAP. ``alpha_agent.r59.engines.gate`` reads materiality from
``net_sharpe`` when that key exists but picks the FLOOR from whether
``ann_net_excess`` exists. A layer carrying BOTH would test a Sharpe against
0.015. No layer produced here carries ``net_sharpe`` (``assert_gate_safe``).

Timing contracts the caller owns (the evaluator cannot see inside a callable):
* futures ``weights_fn(t, live)`` reads data through index t-1 only (open
  interest through t-2); the position is entered at the settlement of t.
* equity ``score_fn(panel, t)`` reads data through close t; the top-N book
  enters at close t+1 and the tranche book at the OPEN of t+1.
"""
from __future__ import annotations

import math
from typing import Callable, Optional

import numpy as np

from alpha_agent import r59
from alpha_agent.r57 import engine as K
from alpha_agent.r59 import native

FUT_COST_MODEL = "FUTURES_PER_MARKET_R38_PLUS_ROLL_V1"
EQ_COST_MODEL = "EQ_EXT_FLAT_25BP_V1"
EQ_EXT_COST_RATE = 0.0025

#: How a market with no session on the decision date is treated.
#: DROP                  owner behaviour (``native.run_book``): not live, flat.
#: ENTER_NEXT_OWN_SETTLE live if it traded within ``stale_slots`` grid slots;
#:                       entered at its OWN next settlement after t, so its
#:                       first own-session return after t is NOT earned.
CLOSED_RULES = ("DROP", "ENTER_NEXT_OWN_SETTLE")


def assert_gate_safe(layers: dict) -> None:
    for name, row in (layers or {}).items():
        if "net_sharpe" in row and "ann_net_excess" in row:
            raise AssertionError("layer %s carries net_sharpe AND ann_net_excess" % name)


# --------------------------------------------------------------------------- #
# Futures: dated-contract long/short book with roll cost
# --------------------------------------------------------------------------- #
def rank_weights(score: np.ndarray, eligible: np.ndarray, *,
                 min_markets: int = 12) -> np.ndarray:
    """``native._xs_weights`` (rank-centred, dollar-neutral, unit gross), flat
    when fewer than ``min_markets`` markets are scorable."""
    score = np.asarray(score, dtype=np.float64)
    sel = np.asarray(eligible, dtype=bool) & np.isfinite(score)
    if int(sel.sum()) < int(min_markets):
        return np.zeros(len(score), dtype=np.float64)
    return native._xs_weights(score, sel)


def live_markets(layer: dict, t: int, *, closed_rule: str = "DROP",
                 stale_slots: int = 5) -> np.ndarray:
    ret = layer["ret"]
    here = np.isfinite(ret[:, t])
    if closed_rule == "DROP":
        return here
    lo = max(0, t - int(stale_slots))
    return np.isfinite(ret[:, lo:t + 1]).any(axis=1)


def _window_returns(ret: np.ndarray, t: int, horizon: int,
                    skip_first_own: Optional[np.ndarray]) -> np.ndarray:
    """ret[t+1 .. t+horizon] with a missing session earning zero (the owner's
    ``_fwd`` rule). For rows flagged in ``skip_first_own`` the first OWN session
    in the window is zeroed: that settlement is the entry, not a return."""
    w = ret[:, t + 1:t + 1 + horizon]
    fin = np.isfinite(w)
    r0 = np.where(fin, w, 0.0)
    if skip_first_own is not None and skip_first_own.any() and r0.shape[1]:
        first = np.argmax(fin, axis=1)
        rows = np.where(skip_first_own & fin.any(axis=1))[0]
        r0[rows, first[rows]] = 0.0
    return r0


def run_futures_book(layer: dict, weights_fn: Callable, *, horizon: int,
                     cadence: int, label: str, cost_mult: float = 1.0,
                     charge_rolls: bool = True, closed_rule: str = "DROP",
                     stale_slots: int = 5, decision_idx=None) -> dict:
    """Simulate one dated-contract book. Control = cash; costs still charged.

    ``layer``       ``data_r38.load_certified_layer()`` (needs ``ret``, ``roll``,
                    ``dates``, ``cost_per_side``).
    ``weights_fn``  ``(t, live) -> float[n_markets]`` target weights (signed
                    notional per unit capital). NaN / not-live -> 0.
    Rebalance cost  ``|w - w_prev| @ cost``            (the owner's rule).
    Roll cost       interior: every change of ``held`` flagged at a session in
                    [t+2, t+horizon] costs ``2 x cost_i x |w_i|``;
                    at entry (flag at t+1, traded at the rebalance instant):
                    the extra notional over ``|w - w_prev|`` is
                    ``2 x min(|w_prev_i|, |w_i|)`` when both have one sign.
    """
    if closed_rule not in CLOSED_RULES:
        raise ValueError("closed_rule must be one of %s" % (CLOSED_RULES,))
    dates, ret, roll = layer["dates"], layer["ret"], layer["roll"]
    costs = np.asarray(layer["cost_per_side"], dtype=np.float64)
    n_m, n_d = ret.shape
    idx = (native.decision_indices(dates, cadence, horizon)
           if decision_idx is None else np.asarray(decision_idx, dtype=int))
    if len(idx) == 0:
        return {"state": "NO_DECISIONS"}
    lay = native.layers_of(dates, idx, cadence, horizon)
    tiled = int(cadence) == int(horizon)

    n_dec = len(idx)
    sg, sn = np.zeros(n_dec), np.zeros(n_dec)
    c_rebal, c_roll = np.zeros(n_dec), np.zeros(n_dec)
    to = np.zeros(n_dec)
    gross_notional = np.zeros(n_dec)
    n_live = np.zeros(n_dec, dtype=int)
    n_pos = np.zeros(n_dec, dtype=int)
    daily_gross = np.zeros(n_d)
    daily_cost = np.zeros(n_d)
    in_book = np.zeros(n_d, dtype=bool)
    weights = np.zeros((n_dec, n_m))
    prev = np.zeros(n_m)

    for j, t in enumerate(idx):
        t = int(t)
        live = live_markets(layer, t, closed_rule=closed_rule,
                            stale_slots=stale_slots)
        w = np.asarray(weights_fn(t, live), dtype=np.float64)
        w = np.where(np.isfinite(w) & live, w, 0.0)
        skip = (~np.isfinite(ret[:, t])) if closed_rule != "DROP" else None
        r0 = _window_returns(ret, t, horizon, skip)
        cum = np.cumprod(1.0 + r0, axis=1)
        f = cum[:, -1] - 1.0 if cum.shape[1] else np.zeros(n_m)
        gross = float(np.sum(w * f))
        d = np.abs(w - prev)
        rebal = float(d @ costs)
        rollc = 0.0
        interior = np.zeros((n_m, 0))
        if charge_rolls:
            lo, hi = min(t + 2, n_d), min(t + horizon + 1, n_d)
            interior = roll[:, lo:hi].astype(np.float64)
            at_entry = (roll[:, t + 1].astype(np.float64) if t + 1 < n_d
                        else np.zeros(n_m))
            same = (np.sign(w) == np.sign(prev)) & (w != 0.0)
            extra = np.where(same, 2.0 * np.minimum(np.abs(w), np.abs(prev)), 0.0)
            entry_cost = float((at_entry * extra) @ costs)
            per_day = (interior * (2.0 * np.abs(w) * costs)[:, None]).sum(axis=0)
            rollc = entry_cost + float(per_day.sum())
        sg[j] = gross
        sn[j] = gross - cost_mult * (rebal + rollc)
        c_rebal[j], c_roll[j] = cost_mult * rebal, cost_mult * rollc
        to[j] = float(d.sum()) / 2.0
        gross_notional[j] = float(np.abs(w).sum())
        n_live[j] = int(live.sum())
        n_pos[j] = int((w != 0.0).sum())
        weights[j] = w
        if tiled and cum.shape[1]:
            prevcum = np.concatenate([np.ones((n_m, 1)), cum[:, :-1]], axis=1)
            dg = (w[:, None] * (cum - prevcum)).sum(axis=0)
            sl = slice(t + 1, t + 1 + len(dg))
            daily_gross[sl] += dg
            in_book[sl] = True
            daily_cost[t + 1] += cost_mult * rebal
            if charge_rolls:
                daily_cost[t + 1] += cost_mult * entry_cost
                k = per_day.shape[0]
                daily_cost[t + 2:t + 2 + k] += cost_mult * per_day
        prev = w

    zeros = np.zeros(n_dec)
    res = {"idx": idx, "layers": lay, "dates": dates[idx],
           "strat_gross": sg, "strat_net": sn,
           "bench_gross": zeros, "bench_net": zeros,
           "turnover_oneway": to, "cadence": cadence, "horizon": horizon}
    ppy = 252.0 / float(horizon)
    layers = {}
    for name in ("D", "V", "L"):
        row = native._layer_stats(res, name)
        sel = lay == name
        if sel.sum():
            x = sn[sel]
            sd = float(x.std())
            row["ann_rebalance_cost_drag"] = float(c_rebal[sel].mean() * ppy)
            row["ann_roll_cost_drag"] = float(c_roll[sel].mean() * ppy)
            row["ann_vol_net"] = sd * math.sqrt(ppy)
            row["period_ir_ann"] = (float(x.mean()) / sd * math.sqrt(ppy)
                                    if sd > 0 else None)
            row["median_live_markets"] = float(np.median(n_live[sel]))
            row["median_positions"] = float(np.median(n_pos[sel]))
            row["flat_decisions"] = int((n_pos[sel] == 0).sum())
            row["mean_gross_notional"] = float(gross_notional[sel].mean())
        layers[name] = row
    assert_gate_safe(layers)
    out = {"state": "MEASURED", "label": label,
           "evaluator": "research.agents.campaign_r56_v2.evaluator.run_futures_book"
                        " (alpha_agent.r59.native accounting + roll cost)"
                        " + alpha_agent.r57.engine.nw_tstat",
           "cost_model": FUT_COST_MODEL, "cost_mult": cost_mult,
           "charge_rolls": bool(charge_rolls), "closed_rule": closed_rule,
           "horizon": horizon, "cadence": cadence, "layers": layers,
           "series": {"decision_dates": dates[idx], "decision_idx": idx,
                      "layer": lay, "gross": sg, "net": sn,
                      "rebalance_cost": c_rebal, "roll_cost": c_roll,
                      "turnover_oneway": to, "n_live": n_live,
                      "n_positions": n_pos, "weights": weights}}
    if tiled:
        out["daily"] = {"dates": dates, "in_book": in_book,
                        "gross": daily_gross, "cost": daily_cost,
                        "net": daily_gross - daily_cost}
    return out


# --------------------------------------------------------------------------- #
# Equity: long-only top-N with a universe eligibility mask
# --------------------------------------------------------------------------- #
def _elig_at(panel: dict, elig, t: int) -> np.ndarray:
    base = elig(panel, t) if callable(elig) else np.asarray(elig[:, t], dtype=bool)
    return base & np.isfinite(panel["tr"][:, t])


def run_equity_topn(panel: dict, score_fn: Callable, elig, *, label: str,
                    cadence: int = 21, horizon: int = 21, top_n: int = 100,
                    cost_rate: float = EQ_EXT_COST_RATE, cost_mult: float = 1.0,
                    first_date: str = r59.DISCOVERY_START) -> dict:
    """``alpha_agent.r57.engine.run_topn`` with the universe agent's eligibility.

    ``elig`` is a bool[n_names, n_sessions] mask or ``(panel, t) -> bool[]``.
    Same conventions as the owner: score from data through close t, NEXT_CLOSE
    entry (close t+1), equal weight, benchmark = equal-weight eligible universe
    charged the same way, cost on traded notional.
    """
    dates = panel["dates"]
    idx = K.decision_indices(dates, cadence, first_date, horizon)
    layers = K.layer_of(dates, idx, cadence, horizon)
    n_dec = len(idx)
    rate = float(cost_rate) * float(cost_mult)
    sg = np.zeros(n_dec); sn = np.zeros(n_dec)
    bg = np.zeros(n_dec); bn = np.zeros(n_dec)
    to = np.zeros(n_dec); bto = np.zeros(n_dec)
    n_held = np.zeros(n_dec, dtype=int)
    n_univ = np.zeros(n_dec, dtype=int)
    holdings = []
    prev_w: dict = {}
    prev_bw: dict = {}
    for j, t in enumerate(idx):
        t = int(t)
        elig_t = _elig_at(panel, elig, t)
        scores = score_fn(panel, t)
        s = np.where(elig_t & np.isfinite(scores), scores, -np.inf)
        k = min(top_n, int((s > -np.inf).sum()))
        if k == 0:
            prev_w = {}
            holdings.append((t, np.array([], dtype=int), np.array([]),
                             np.array([], dtype=int)))
            continue
        held = np.argpartition(-s, k - 1)[:k]
        held = held[np.argsort(-s[held])]
        w = np.full(k, 1.0 / k)
        wmap = {int(h): float(x) for h, x in zip(held, w)}
        traded = sum(abs(wmap.get(x, 0.0) - prev_w.get(x, 0.0))
                     for x in set(wmap) | set(prev_w))
        r = K.forward_return(panel, held, t, horizon)
        gross = float((w * r).sum())
        univ = np.where(elig_t)[0]
        bwmap = {int(x): 1.0 / len(univ) for x in univ}
        btraded = sum(abs(bwmap.get(x, 0.0) - prev_bw.get(x, 0.0))
                      for x in set(bwmap) | set(prev_bw))
        br = K.forward_return(panel, univ, t, horizon)
        bgross = float(br.mean())
        sg[j], sn[j] = gross, gross - traded * rate
        bg[j], bn[j] = bgross, bgross - btraded * rate
        to[j], bto[j] = traded / 2.0, btraded / 2.0
        n_held[j], n_univ[j] = k, len(univ)
        holdings.append((t, held, w, univ))
        prev_w, prev_bw = wmap, bwmap
    res = {"idx": idx, "layers": layers, "dates": dates[idx],
           "strat_gross": sg, "strat_net": sn, "bench_gross": bg, "bench_net": bn,
           "turnover_oneway": to, "bench_turnover_oneway": bto,
           "n_held": n_held, "n_universe": n_univ,
           "cadence": cadence, "horizon": horizon}
    rows = {}
    for name in ("D", "V", "L"):
        row = K.layer_stats(res, name)
        sel = layers == name
        if sel.sum():
            row["median_universe"] = float(np.median(n_univ[sel]))
            row["min_universe"] = int(n_univ[sel].min())
            row["median_held"] = float(np.median(n_held[sel]))
        rows[name] = row
    assert_gate_safe(rows)
    return {"state": "MEASURED", "label": label,
            "evaluator": "research.agents.campaign_r56_v2.evaluator.run_equity_topn"
                         " (alpha_agent.r57.engine.run_topn accounting + universe"
                         " mask) + alpha_agent.r57.engine.layer_stats",
            "cost_model": EQ_COST_MODEL, "cost_rate": rate,
            "horizon": horizon, "cadence": cadence, "top_n": top_n,
            "layers": rows, "res": res, "holdings": holdings}


def _value_paths(tr: np.ndarray, rows: np.ndarray, a: int, b: int) -> np.ndarray:
    """tr[rows, a..b] / tr[rows, a], forward-filled; an unpriced entry earns 0
    (the owner's ``forward_return`` convention)."""
    win = tr[rows, a:b + 1]
    fin = np.isfinite(win)
    pos = np.where(fin, np.arange(win.shape[1])[None, :], 0)
    pos = np.maximum.accumulate(pos, axis=1)
    filled = win[np.arange(len(rows))[:, None], pos]
    entry = win[:, 0]
    ok = np.isfinite(entry) & (entry > 0)
    v = np.where(ok[:, None], filled / np.where(ok, entry, 1.0)[:, None], 1.0)
    return np.where(np.isfinite(v), v, 1.0)


def equity_topn_daily(panel: dict, result: dict) -> dict:
    """Daily strategy / benchmark / excess series of a tiled top-N book (for the
    risk agent). Costs are booked on the first accrual day of each window. The
    per-window sums reproduce the per-decision returns."""
    res = result["res"]
    h = int(res["horizon"])
    if int(res["cadence"]) != h:
        raise ValueError("daily series needs cadence == horizon")
    tr, dates = panel["tr"], panel["dates"]
    n_d = tr.shape[1]
    sgd, scd = np.zeros(n_d), np.zeros(n_d)
    bgd, bcd = np.zeros(n_d), np.zeros(n_d)
    in_book = np.zeros(n_d, dtype=bool)
    for j, (t, held, w, univ) in enumerate(result["holdings"]):
        if len(held) == 0:
            continue
        a, b = t + 1, min(t + 1 + h, n_d - 1)
        v = _value_paths(tr, held, a, b)
        sgd[a + 1:b + 1] += (w[:, None] * np.diff(v, axis=1)).sum(axis=0)
        bv = _value_paths(tr, univ, a, b)
        bgd[a + 1:b + 1] += np.diff(bv, axis=1).mean(axis=0)
        scd[a + 1] += res["strat_gross"][j] - res["strat_net"][j]
        bcd[a + 1] += res["bench_gross"][j] - res["bench_net"][j]
        in_book[a + 1:b + 1] = True
    sn, bn = sgd - scd, bgd - bcd
    return {"dates": dates, "in_book": in_book, "strat_net": sn,
            "bench_net": bn, "excess_net": sn - bn,
            "strat_gross": sgd, "bench_gross": bgd}


# --------------------------------------------------------------------------- #
# Equity: H-session next-open overlapping-tranche daily book
# --------------------------------------------------------------------------- #
def open_marks(tr: np.ndarray, op: np.ndarray) -> np.ndarray:
    """Mark at the OPEN of session s: the open print when there is one, else the
    last total-return close strictly before s. ``op_tr`` and ``tr`` carry the
    same adjustment factor on a session, so their ratio is a return."""
    n_s, n_d = tr.shape
    fin = np.isfinite(tr)
    pos = np.where(fin, np.arange(n_d, dtype=np.int32)[None, :], -1)
    pos = np.maximum.accumulate(pos, axis=1)
    last_close = np.where(pos >= 0,
                          tr[np.arange(n_s)[:, None], np.clip(pos, 0, None)],
                          np.nan)
    prev_close = np.full_like(tr, np.nan)
    prev_close[:, 1:] = last_close[:, :-1]
    return np.where(np.isfinite(op) & (op > 0), op, prev_close)


def _daily_layer_stats(dates, x_net, x_gross, s_net, b_net, turnover, sl,
                       nw_lag: int) -> dict:
    r = x_net[sl]
    if len(r) < 30:
        return {"days": int(len(r))}
    st = K.nw_tstat(r, lag=nw_lag)
    nav = np.cumprod(1.0 + r)
    snav = np.cumprod(1.0 + s_net[sl])
    half = len(r) // 2
    return {
        "days": int(len(r)),
        "first": str(dates[sl][0]), "last": str(dates[sl][-1]),
        "ann_net_excess": float(r.mean() * 252.0),
        "ann_gross_excess": float(x_gross[sl].mean() * 252.0),
        "ann_cost_drag": float((x_gross[sl] - r).mean() * 252.0),
        "ann_strat_net": float(s_net[sl].mean() * 252.0),
        "ann_bench_net": float(b_net[sl].mean() * 252.0),
        "ann_vol_excess": float(r.std() * math.sqrt(252.0)),
        "mean_oneway_turnover_per_day": float(turnover[sl].mean()),
        "max_dd": float((nav / np.maximum.accumulate(nav) - 1.0).min()),
        "strat_max_dd": float((snav / np.maximum.accumulate(snav) - 1.0).min()),
        "hit_rate": float((r > 0).mean()),
        "t_net_excess": st["t"], "p_one_sided": st["p_one_sided"],
        "nw_lag": int(nw_lag),
        "halves_ann_net_excess": [float(r[:half].mean() * 252.0),
                                  float(r[half:].mean() * 252.0)],
    }


def _tranche_side(F, op, elig_fn, score_fn, panel, t0, n_d, hold, top_n,
                  net_overlap, rate):
    """One side (strategy when ``top_n`` is set, benchmark when None)."""
    n_s = F.shape[0]
    gross = np.zeros(n_d)
    cost = np.zeros(n_d)
    traded_day = np.zeros(n_d)
    unfilled = np.zeros(n_d)
    selected = {}
    book = {}           # formation t -> (names, entry_px, unit)
    for s in range(t0 + 1, n_d - 1):
        t = s - 1       # tranche formed at close t enters at the OPEN of s
        elig_t = elig_fn(t)
        if top_n is None:
            names = np.where(elig_t)[0]
        else:
            sc = score_fn(panel, t)
            v = np.where(elig_t & np.isfinite(sc), sc, -np.inf)
            k = min(int(top_n), int((v > -np.inf).sum()))
            names = (np.argpartition(-v, k - 1)[:k] if k else
                     np.array([], dtype=int))
        buy = np.zeros(n_s)
        if len(names):
            unit = (1.0 / hold) / len(names)
            px = op[names, s]
            fill = np.isfinite(px) & (px > 0)
            unfilled[s] = float((~fill).sum()) / len(names)
            book[t] = (names[fill], px[fill], unit)
            buy[names[fill]] = unit
            if top_n is not None:
                selected[t] = np.sort(names)
        sell = np.zeros(n_s)
        old = book.pop(t - hold, None)
        if old is not None and len(old[0]):
            sell[old[0]] = old[2] * F[old[0], s] / old[1]
        traded = (float(np.abs(buy - sell).sum()) if net_overlap
                  else float(buy.sum() + sell.sum()))
        cost[s] = rate * traded
        traded_day[s] = traded
        g = 0.0
        for names_k, px_k, unit_k in book.values():
            if len(names_k):
                g += unit_k * float(((F[names_k, s + 1] - F[names_k, s]) / px_k).sum())
        gross[s] = g
    return gross, cost, traded_day, unfilled, selected


def run_equity_daily_tranche_book(panel: dict, score_fn: Callable, elig, *,
                                  label: str, hold: int = 5, top_n: int = 100,
                                  cost_rate: float = EQ_EXT_COST_RATE,
                                  cost_mult: float = 1.0,
                                  net_overlap: bool = True,
                                  first_date: str = r59.DISCOVERY_START,
                                  embargo: int = 10, nw_lag: int = 5) -> dict:
    """Overlapping-tranche next-open book (Jegadeesh-Titman construction).

    Each session t one tranche (1/``hold`` of capital) buys the ``top_n``
    highest ``score_fn(panel, t)`` eligible names at the OPEN of t+1 and is
    liquidated at the OPEN of t+1+``hold``. A selected name with no open print
    at t+1 is NOT replaced: its slice stays in cash at no cost. A name with no
    open print at exit is marked at its last total-return close.

    Cost is charged on TRADED NOTIONAL. With ``net_overlap`` the expiring
    tranche is netted name by name against the new one (a name held by both is
    not sold and rebought); without it both legs are charged in full. The
    benchmark is the equal-weight eligible universe run through the SAME
    construction and the SAME cost rule. Day s is the interval open s -> open
    s+1 and is dated s.
    """
    tr, op, dates = panel["tr"], panel["op_tr"], panel["dates"]
    n_d = tr.shape[1]
    rate = float(cost_rate) * float(cost_mult)
    F = open_marks(tr, op)
    t0 = int(np.searchsorted(dates, first_date))

    def elig_fn(t):
        return _elig_at(panel, elig, t)

    sg, sc, s_tr, s_unf, selected = _tranche_side(
        F, op, elig_fn, score_fn, panel, t0, n_d, hold, top_n, net_overlap, rate)
    bg, bc, b_tr, _b_unf, _ = _tranche_side(
        F, op, elig_fn, score_fn, panel, t0, n_d, hold, None, net_overlap, rate)

    s_net, b_net = sg - sc, bg - bc
    x_net, x_gross = s_net - b_net, sg - bg
    v0 = int(np.searchsorted(dates, r59.VALIDATION_START))
    l0 = int(np.searchsorted(dates, r59.LOCKBOX_START))
    first = t0 + hold                      # fully invested from here
    slices = {"D": slice(first, v0 - embargo), "V": slice(v0, l0 - embargo),
              "L": slice(l0, n_d - 1)}
    layers = {}
    for name, sl in slices.items():
        row = _daily_layer_stats(dates, x_net, x_gross, s_net, b_net,
                                 s_tr / 2.0, sl, nw_lag)
        if row.get("days", 0) >= 30:
            row["mean_unfilled_share"] = float(s_unf[sl].mean())
            row["bench_mean_oneway_turnover_per_day"] = float((b_tr[sl] / 2.0).mean())
        layers[name] = row
    assert_gate_safe(layers)
    return {"state": "MEASURED", "label": label,
            "evaluator": "research.agents.campaign_r56_v2.evaluator."
                         "run_equity_daily_tranche_book"
                         " + alpha_agent.r57.engine.nw_tstat",
            "cost_model": EQ_COST_MODEL, "cost_rate": rate,
            "net_overlap": bool(net_overlap), "hold": hold, "top_n": top_n,
            "entry": "OPEN t+1", "exit": "OPEN t+1+hold", "layers": layers,
            "daily": {"dates": dates, "slices": slices, "strat_gross": sg,
                      "strat_net": s_net, "bench_gross": bg, "bench_net": b_net,
                      "excess_net": x_net, "excess_gross": x_gross,
                      "strat_traded": s_tr, "bench_traded": b_tr,
                      "unfilled_share": s_unf},
            "selected": selected}
