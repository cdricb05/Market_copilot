"""alpha_agent.r58.engine - the ONE R58 evaluation kernel.

Everything the protocol registered is implemented here once, so no family can
quietly run under different rules:

* the R58 eligible universe (R57's PIT membership / price / liquidity / history
  floors AND a PANEL-F core fundamental record available at t)
* ONE benchmark - the equal-weight R58 eligible universe - shared by every
  family, so families are comparable to each other and not just to themselves
* NEXT_CLOSE entry, 21-session cadence, 21-session horizon
* symmetric 12.5bp/side costs on strategy AND benchmark
* the DISCOVERY / VALIDATION / LOCKBOX partition with a purge embargo
* BUY-side and SELL-side decomposition, kept separate on purpose: a signal good
  at selling and bad at buying must never be reported as one blended score
* Newey-West one-sided tests and Benjamini-Hochberg across the campaign

Forward returns, the t-statistic and the FDR procedure are imported from the
R57 kernel rather than rewritten, so an R58 fundamental family and an R57 price
family are measured by literally the same code.
"""
from __future__ import annotations

import math

import numpy as np

from . import (CADENCE, EQ_COST_RATE_PER_SIDE, EQ_TOP_N, HORIZON,
               LOCKBOX_START, VALIDATION_START)
from ..r57.engine import bh_fdr, forward_return, nw_tstat  # noqa: F401  (re-exported)
from ..r57.engine import eligibility as _r57_eligibility

PPY = 252.0


# --------------------------------------------------------------------------- #
# Partition
# --------------------------------------------------------------------------- #
def layer_of(dec_dates: np.ndarray) -> np.ndarray:
    """'D' / 'V' / 'L' / '' (embargoed) per decision date.

    The embargo removes ceil(horizon/cadence) decision dates from the END of the
    earlier layer at each boundary, so no forward label resolves across one.
    """
    emb = int(math.ceil(HORIZON / float(CADENCE)))
    out = np.array(["D" if d < VALIDATION_START
                    else ("V" if d < LOCKBOX_START else "L")
                    for d in dec_dates], dtype="U1")
    for boundary in ("D", "V"):
        js = np.where(out == boundary)[0]
        if len(js):
            out[js[-emb:]] = ""
    return out


# --------------------------------------------------------------------------- #
# Eligibility
# --------------------------------------------------------------------------- #
def eligibility(pf: dict, j: int) -> np.ndarray:
    """The R58 eligible universe at decision slot j.

    R57's floors AND a PANEL-F core fundamental record: total assets plus at
    least one TTM flow, every term filed on or before the decision date. A name
    the fundamental store cannot see is not in the universe at all - it is not
    silently scored as zero, and it is not silently in the benchmark either.
    """
    t = int(pf["dec"][j])
    ok = _r57_eligibility(pf["price"], t)
    core = pf["cube"][:, j, pf["f_ix"]["has_core"]]
    return ok & np.isfinite(core) & (core > 0)


# --------------------------------------------------------------------------- #
# Cross-sectional helpers
# --------------------------------------------------------------------------- #
def xs_z(v: np.ndarray, mask: np.ndarray, wins=(0.01, 0.99)) -> np.ndarray:
    """Winsorised cross-sectional z within the eligible set. NaN stays NaN."""
    out = np.full(v.shape, np.nan)
    ok = mask & np.isfinite(v)
    if ok.sum() < 20:
        return out
    x = v[ok].astype(np.float64)
    lo, hi = np.quantile(x, wins[0]), np.quantile(x, wins[1])
    x = np.clip(x, lo, hi)
    sd = x.std()
    if not np.isfinite(sd) or sd < 1e-12:
        return out
    out[ok] = (x - x.mean()) / sd
    return out


def xs_rank01(v: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """Cross-sectional rank in [0, 1] within the eligible set."""
    out = np.full(v.shape, np.nan)
    ok = mask & np.isfinite(v)
    n = int(ok.sum())
    if n < 20:
        return out
    idx = np.where(ok)[0]
    order = np.argsort(v[idx], kind="mergesort")
    r = np.empty(n)
    r[order] = np.arange(n)
    out[idx] = r / max(n - 1, 1)
    return out


def spearman(a: np.ndarray, b: np.ndarray) -> float:
    ok = np.isfinite(a) & np.isfinite(b)
    if ok.sum() < 20:
        return float("nan")
    x, y = a[ok], b[ok]
    rx = np.argsort(np.argsort(x, kind="mergesort")).astype(float)
    ry = np.argsort(np.argsort(y, kind="mergesort")).astype(float)
    rx -= rx.mean(); ry -= ry.mean()
    d = math.sqrt(float((rx * rx).sum()) * float((ry * ry).sum()))
    return float((rx * ry).sum() / d) if d > 0 else float("nan")


# --------------------------------------------------------------------------- #
# Long-only top-N simulation with the shared benchmark
# --------------------------------------------------------------------------- #
def run_topn(pf: dict, score_fn, top_n: int = EQ_TOP_N,
             cost_rate: float = EQ_COST_RATE_PER_SIDE,
             hold_band: int = 0, benchmark_scope: str = "eligible") -> dict:
    """Simulate the long-only top-N book and its benchmark.

    ``score_fn(pf, j, elig) -> scores`` (np.nan = unscorable, -inf = vetoed).
    ``hold_band`` > 0 keeps an existing holding while its rank is <= hold_band,
    which is the pre-registered turnover-control construction.
    ``benchmark_scope`` is "eligible" for the shared campaign benchmark, or
    "scored" for the restricted benchmark the protocol permits ONLY as a
    labelled within-coverage diagnostic for a family the coverage gate blocked.
    """
    price = pf["price"]
    dec = pf["dec"]
    dec_dates = pf["dec_dates"]
    layers = layer_of(dec_dates)
    n = len(dec)
    sg = np.zeros(n); sn = np.zeros(n)
    bg = np.zeros(n); bn = np.zeros(n)
    to = np.zeros(n); n_held = np.zeros(n, dtype=int)
    n_elig = np.zeros(n, dtype=int); n_scored = np.zeros(n, dtype=int)
    buy = np.full(n, np.nan); sell = np.full(n, np.nan); ic = np.full(n, np.nan)
    prev_w: dict = {}
    prev_bw: dict = {}
    prev_held: list = []
    for j in range(n):
        t = int(dec[j])
        elig = eligibility(pf, j)
        n_elig[j] = int(elig.sum())
        scores = score_fn(pf, j, elig)
        scored = elig & np.isfinite(scores)
        n_scored[j] = int(scored.sum())
        s = np.where(scored, scores, -np.inf)
        k = min(top_n, int(np.isfinite(s[scored]).sum()) if scored.any() else 0)
        if k == 0 or n_elig[j] < 50:
            prev_w = {}; prev_held = []
            continue
        order = np.argsort(-s)
        ranked = [int(x) for x in order if np.isfinite(s[x])]
        if hold_band and prev_held:
            rank_of = {sym: r for r, sym in enumerate(ranked)}
            keep = [h for h in prev_held
                    if rank_of.get(h, 10 ** 9) < hold_band]
            held = list(keep)
            for sym in ranked:
                if len(held) >= k:
                    break
                if sym not in held:
                    held.append(sym)
            held = held[:k]
        else:
            held = ranked[:k]
        held_a = np.array(held, dtype=int)
        w = np.full(len(held_a), 1.0 / len(held_a))
        wmap = {int(h): float(x) for h, x in zip(held_a, w)}
        traded = sum(abs(wmap.get(x, 0.0) - prev_w.get(x, 0.0))
                     for x in set(wmap) | set(prev_w))
        r = forward_return(price, held_a, t, HORIZON)
        gross = float((w * r).sum())

        univ = np.where(scored if benchmark_scope == "scored" else elig)[0]
        if len(univ) == 0:
            prev_w = {}; prev_held = []
            continue
        bwmap = {int(x): 1.0 / len(univ) for x in univ}
        btraded = sum(abs(bwmap.get(x, 0.0) - prev_bw.get(x, 0.0))
                      for x in set(bwmap) | set(prev_bw))
        br = forward_return(price, univ, t, HORIZON)
        bgross = float(br.mean())

        sg[j], sn[j] = gross, gross - traded * cost_rate
        bg[j], bn[j] = bgross, bgross - btraded * cost_rate
        to[j] = traded / 2.0
        n_held[j] = len(held_a)

        # BUY / SELL decomposition on the scored subset, gross of costs
        sc_ix = np.where(scored)[0]
        if len(sc_ix) >= 50:
            sv = scores[sc_ix]
            rv = forward_return(price, sc_ix, t, HORIZON)
            mu = float(rv.mean())
            hi = sv >= np.quantile(sv, 0.90)
            lo = sv <= np.quantile(sv, 0.10)
            if hi.sum() and lo.sum():
                buy[j] = float(rv[hi].mean()) - mu
                sell[j] = float(rv[lo].mean()) - mu
            ic[j] = spearman(sv, rv)
        prev_w, prev_bw, prev_held = wmap, bwmap, held
    return {"dec": dec, "dates": dec_dates, "layers": layers,
            "strat_gross": sg, "strat_net": sn,
            "bench_gross": bg, "bench_net": bn,
            "turnover_oneway": to, "n_held": n_held,
            "n_eligible": n_elig, "n_scored": n_scored,
            "buy_excess": buy, "sell_excess": sell, "rank_ic": ic,
            "cadence": CADENCE, "horizon": HORIZON}


# --------------------------------------------------------------------------- #
# Layer statistics
# --------------------------------------------------------------------------- #
def layer_stats(res: dict, layer: str) -> dict:
    sel = res["layers"] == layer
    sel = sel & (res["n_held"] > 0)
    if sel.sum() == 0:
        return {"periods": 0}
    ex_net = res["strat_net"][sel] - res["bench_net"][sel]
    ex_gross = res["strat_gross"][sel] - res["bench_gross"][sel]
    ppy = PPY / res["cadence"]
    lag = max(0, int(math.ceil(res["horizon"] / res["cadence"])) - 1)
    st = nw_tstat(ex_net, lag=lag)

    def _dd(rets):
        nav = np.cumprod(1.0 + rets)
        peak = np.maximum.accumulate(nav)
        return float((nav / peak - 1.0).min()) if len(nav) else None

    def _m(a):
        v = a[sel]
        v = v[np.isfinite(v)]
        return float(v.mean()) if len(v) else None

    half = len(ex_net) // 2
    return {
        "periods": int(sel.sum()),
        "first": str(res["dates"][sel][0]), "last": str(res["dates"][sel][-1]),
        "ann_net_excess": float(ex_net.mean() * ppy),
        "ann_gross_excess": float(ex_gross.mean() * ppy),
        "ann_strat_net": float(res["strat_net"][sel].mean() * ppy),
        "ann_bench_net": float(res["bench_net"][sel].mean() * ppy),
        "ann_cost_drag": float((ex_gross - ex_net).mean() * ppy),
        "mean_oneway_turnover_per_period": float(res["turnover_oneway"][sel].mean()),
        "strat_max_dd": _dd(res["strat_net"][sel]),
        "bench_max_dd": _dd(res["bench_net"][sel]),
        "hit_rate": float((ex_net > 0).mean()),
        "t_net_excess": st["t"], "p_one_sided": st["p_one_sided"],
        "halves_ann_net_excess": [float(ex_net[:half].mean() * ppy),
                                  float(ex_net[half:].mean() * ppy)]
        if sel.sum() >= 4 else None,
        "buy_side_ann_excess": (_m(res["buy_excess"]) * ppy
                                if _m(res["buy_excess"]) is not None else None),
        "sell_side_ann_excess": (_m(res["sell_excess"]) * ppy
                                 if _m(res["sell_excess"]) is not None else None),
        "mean_rank_ic": _m(res["rank_ic"]),
        "mean_scored": _m(res["n_scored"].astype(float)),
        "mean_eligible": _m(res["n_eligible"].astype(float)),
        "scored_fraction": (_m(res["n_scored"].astype(float))
                            / max(_m(res["n_eligible"].astype(float)) or 1.0, 1.0)),
    }


def buy_sell_t(res: dict, layer: str) -> dict:
    """One-sided t on the BUY side and on SELL-side skill, reported separately."""
    sel = (res["layers"] == layer) & np.isfinite(res["buy_excess"])
    if sel.sum() < 4:
        return {"periods": int(sel.sum())}
    b = res["buy_excess"][sel]
    s = res["sell_excess"][sel]
    ics = res["rank_ic"][(res["layers"] == layer) & np.isfinite(res["rank_ic"])]
    return {
        "periods": int(sel.sum()),
        "buy_t": nw_tstat(b)["t"],
        "sell_skill_t": nw_tstat(-s)["t"],
        "buy_hit_rate": float((b > 0).mean()),
        "sell_hit_rate": float((s < 0).mean()),
        "mean_rank_ic": float(ics.mean()) if len(ics) else None,
        "rank_ic_t": nw_tstat(ics)["t"] if len(ics) >= 4 else None,
        "note": "sell-side skill means the BOTTOM decile UNDERPERFORMS; its t is "
                "for -excess > 0",
    }
