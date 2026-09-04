"""alpha_agent.r57.engine - the ONE R57 evaluation kernel.

Everything the protocol registered is implemented here once, so no family can
quietly run under different rules:

* the DISCOVERY / VALIDATION / LOCKBOX partition with purge embargo
* NEXT_CLOSE entry (signal at t close, position effective t+1 close)
* eligibility (PIT membership, $5 floor, $10M ADV, 260-session history)
* long-only equal-weight top-N simulation with symmetric costs on strategy
  AND benchmark
* Newey-West one-sided tests
* Benjamini-Hochberg across the campaign's lockbox tests

Pure numpy; deterministic; no I/O.
"""
from __future__ import annotations

import math

import numpy as np

from . import (DISCOVERY_START, EQ_COST_RATE_PER_SIDE, EQ_MIN_ADV,
               EQ_MIN_HISTORY, EQ_MIN_PRICE, EQ_TOP_N, LOCKBOX_START,
               VALIDATION_START)

PPY = 252.0


# --------------------------------------------------------------------------- #
# Partition
# --------------------------------------------------------------------------- #
def decision_indices(dates: np.ndarray, cadence: int, first_date: str,
                     horizon: int) -> np.ndarray:
    """Every cadence-th session index from the first date, leaving room for the
    NEXT_CLOSE entry and the full forward window."""
    start = int(np.searchsorted(dates, first_date))
    last = len(dates) - horizon - 2
    return np.arange(start, last + 1, cadence)


def layer_of(dates: np.ndarray, idx: np.ndarray, cadence: int,
             horizon: int) -> np.ndarray:
    """'D' / 'V' / 'L' / '' (embargoed) per decision index.

    The embargo removes ceil(horizon/cadence) decision dates from the END of
    the earlier layer at each boundary, so no forward label resolves across a
    boundary.
    """
    emb = int(math.ceil(horizon / float(cadence)))
    v0 = int(np.searchsorted(dates, VALIDATION_START))
    l0 = int(np.searchsorted(dates, LOCKBOX_START))
    out = np.empty(len(idx), dtype="U1")
    for j, t in enumerate(idx):
        out[j] = "D" if t < v0 else ("V" if t < l0 else "L")
    # purge: last `emb` D-decisions before V, last `emb` V-decisions before L
    for boundary in ("D", "V"):
        js = np.where(out == boundary)[0]
        if len(js):
            out[js[-emb:]] = ""
    return out


# --------------------------------------------------------------------------- #
# Eligibility
# --------------------------------------------------------------------------- #
def eligibility(panel: dict, t: int) -> np.ndarray:
    """Boolean mask over symbols at decision index t. Uses data <= t only."""
    tr, un, vol, mem = panel["tr"], panel["un"], panel["vol"], panel["mem"]
    ok = mem[:, t] > 0
    ok &= np.isfinite(un[:, t]) & (un[:, t] >= EQ_MIN_PRICE)
    lo = max(0, t - 62)
    dvol = un[:, lo:t + 1] * vol[:, lo:t + 1]
    med = np.nanmedian(np.where(np.isfinite(dvol), dvol, np.nan), axis=1)
    ok &= np.isfinite(med) & (med >= EQ_MIN_ADV)
    lo2 = max(0, t - EQ_MIN_HISTORY + 1)
    ok &= np.isfinite(tr[:, lo2:t + 1]).sum(axis=1) >= EQ_MIN_HISTORY * 0.9
    ok &= np.isfinite(tr[:, t])
    return ok


# --------------------------------------------------------------------------- #
# Forward window return  (NEXT_CLOSE: [t+1, t+1+h])
# --------------------------------------------------------------------------- #
def forward_return(panel: dict, held: np.ndarray, t: int, horizon: int) -> np.ndarray:
    """Per-name total return over the forward window for the held indices.

    A name whose series ends inside the window exits at its LAST available
    total-return close (the vendor series realises the delisting path), and
    the missing tail earns zero - never a fabricated price.
    """
    tr = panel["tr"]
    entry = tr[held, t + 1]
    window = tr[held, t + 1:t + 1 + horizon + 1]
    # last finite value in each row
    idx = np.where(np.isfinite(window), np.arange(window.shape[1])[None, :], -1)
    last_ix = idx.max(axis=1)
    exitp = window[np.arange(len(held)), np.clip(last_ix, 0, None)]
    r = np.where((last_ix > 0) & np.isfinite(entry) & (entry > 0),
                 exitp / entry - 1.0, 0.0)
    return np.where(np.isfinite(r), r, 0.0)


# --------------------------------------------------------------------------- #
# Long-only top-N simulation
# --------------------------------------------------------------------------- #
def run_topn(panel: dict, score_fn, cadence: int, horizon: int,
             top_n: int = EQ_TOP_N,
             cost_rate: float = EQ_COST_RATE_PER_SIDE,
             first_date: str = DISCOVERY_START,
             weights_fn=None) -> dict:
    """Simulate the long-only top-N strategy and its EW-universe benchmark.

    ``score_fn(panel, t) -> scores`` (np.nan = unscorable). Higher is better.
    ``weights_fn(panel, held, t) -> weights`` optionally replaces equal weight
    (the construction track); weights are normalised to 1.
    Returns per-decision arrays: strat/bench gross and net period returns,
    one-way turnover, layer labels.
    """
    dates = panel["dates"]
    idx = decision_indices(dates, cadence, first_date, horizon)
    layers = layer_of(dates, idx, cadence, horizon)
    n_dec = len(idx)
    sg = np.zeros(n_dec); sn = np.zeros(n_dec)
    bg = np.zeros(n_dec); bn = np.zeros(n_dec)
    to = np.zeros(n_dec); bto = np.zeros(n_dec)
    n_held = np.zeros(n_dec, dtype=int)
    prev_w: dict = {}
    prev_bw: dict = {}
    for j, t in enumerate(idx):
        elig = eligibility(panel, t)
        scores = score_fn(panel, t)
        s = np.where(elig & np.isfinite(scores), scores, -np.inf)
        k = min(top_n, int((s > -np.inf).sum()))
        if k == 0:
            prev_w = {}; continue
        held = np.argpartition(-s, k - 1)[:k]
        held = held[np.argsort(-s[held])]
        if weights_fn is None:
            w = np.full(k, 1.0 / k)
        else:
            w = np.asarray(weights_fn(panel, held, t), dtype=np.float64)
            w = np.clip(w, 0, None); w = w / w.sum() if w.sum() > 0 else np.full(k, 1.0 / k)
        wmap = {int(h): float(x) for h, x in zip(held, w)}
        traded = sum(abs(wmap.get(x, 0.0) - prev_w.get(x, 0.0))
                     for x in set(wmap) | set(prev_w))
        r = forward_return(panel, held, t, horizon)
        gross = float((w * r).sum())
        cost = traded * cost_rate
        # benchmark: EW of the whole eligible universe, same convention
        univ = np.where(elig)[0]
        bwmap = {int(x): 1.0 / len(univ) for x in univ}
        btraded = sum(abs(bwmap.get(x, 0.0) - prev_bw.get(x, 0.0))
                      for x in set(bwmap) | set(prev_bw))
        br = forward_return(panel, univ, t, horizon)
        bgross = float(br.mean())
        bcost = btraded * cost_rate
        sg[j], sn[j] = gross, gross - cost
        bg[j], bn[j] = bgross, bgross - bcost
        to[j], bto[j] = traded / 2.0, btraded / 2.0
        n_held[j] = k
        prev_w, prev_bw = wmap, bwmap
    return {"idx": idx, "layers": layers, "dates": dates[idx],
            "strat_gross": sg, "strat_net": sn,
            "bench_gross": bg, "bench_net": bn,
            "turnover_oneway": to, "bench_turnover_oneway": bto,
            "n_held": n_held, "cadence": cadence, "horizon": horizon}


# --------------------------------------------------------------------------- #
# Statistics
# --------------------------------------------------------------------------- #
def nw_tstat(x: np.ndarray, lag: int = 0) -> dict:
    """One-sided Newey-West t for mean(x) > 0."""
    x = np.asarray(x, dtype=np.float64)
    n = len(x)
    if n < 3:
        return {"n": n, "mean": None, "t": None, "p_one_sided": None}
    m = x.mean()
    e = x - m
    s2 = float((e * e).sum()) / n
    for k in range(1, min(lag, n - 1) + 1):
        wgt = 1.0 - k / (lag + 1.0)
        s2 += 2.0 * wgt * float((e[k:] * e[:-k]).sum()) / n
    se = math.sqrt(max(s2, 1e-18) / n)
    t = m / se
    # normal approximation for one-sided p (n typically >= 36)
    p = 0.5 * math.erfc(t / math.sqrt(2.0))
    return {"n": n, "mean": m, "t": t, "p_one_sided": p}


def layer_stats(res: dict, layer: str) -> dict:
    """Annualised net excess, gross excess, turnover, drawdowns for one layer."""
    sel = res["layers"] == layer
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
        "halves_ann_net_excess": [
            float(ex_net[:len(ex_net) // 2].mean() * ppy),
            float(ex_net[len(ex_net) // 2:].mean() * ppy)] if sel.sum() >= 4 else None,
    }


def bh_fdr(pvals: dict, q: float) -> dict:
    """Benjamini-Hochberg across named tests. Returns per-name pass/fail."""
    items = [(k, v) for k, v in pvals.items() if v is not None]
    items.sort(key=lambda kv: kv[1])
    m = len(items)
    passed = set()
    max_i = 0
    for i, (k, p) in enumerate(items, 1):
        if p <= q * i / m:
            max_i = i
    for i, (k, p) in enumerate(items, 1):
        if i <= max_i:
            passed.add(k)
    return {k: (k in passed) for k, _p in items}
