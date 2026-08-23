"""alpha_agent.r41.evidence - ONE multi-horizon inference owner.

Blocker 2 (decision cadence) is attacked honestly or not at all: a 5-session
target evaluated daily has 4/5 overlap, cross-sectional rows sharing a
timestamp are one observation, and a minute strategy's 100,000 bars are not
100,000 independent decisions. This module owns:

* chronological A/B/C zone splitting with an embargo (contract.ZONE_SPLIT);
* HAC (Newey-West) t-statistics with Bartlett lags >= the target overlap;
* effective sample size from the fitted autocorrelation of the judged
  stream;
* the economic scorecard every lab must produce (gross, cost, net vs its
  declared control, Sharpe / Sortino / drawdown / CVaR, cost-stress at the
  contract multipliers, halves sign stability);
* factor residualisation (OLS on declared factor streams, HAC t on the
  residual alpha);
* the RESEARCH_CANDIDATE gate and the family Benjamini-Hochberg, reusing
  the R31 multiple-testing owner and the R39 deflated Sharpe.

Deflated Sharpe and BH are IMPORTED (:mod:`alpha_agent.r31.multiple_testing`,
:mod:`alpha_agent.r39.burden`), never re-implemented.
"""
from __future__ import annotations

import math
from typing import Optional

import numpy as np
import pandas as pd

from ..r31 import multiple_testing as MT
from ..r39 import burden as R39B
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r41.evidence"


# --------------------------------------------------------------------------- #
# Zones
# --------------------------------------------------------------------------- #
def zone_split(dates, *, embargo: int = 0) -> dict:
    """Chronological A/B/C = 50/30/20 with ``embargo`` periods removed from
    the START of B and C (the fit side keeps its data; the judged side loses
    the overlap)."""
    idx = pd.DatetimeIndex(dates).unique().sort_values()
    n = len(idx)
    a_end = int(n * C.ZONE_SPLIT["ZONE_A"])
    b_end = int(n * (C.ZONE_SPLIT["ZONE_A"] + C.ZONE_SPLIT["ZONE_B"]))
    a = idx[:a_end]
    b = idx[a_end + embargo:b_end]
    c = idx[b_end + embargo:]
    return {"A": a, "B": b, "C": c, "n": n, "embargo": int(embargo),
            "a_range": (str(a[0])[:10], str(a[-1])[:10]) if len(a) else None,
            "b_range": (str(b[0])[:10], str(b[-1])[:10]) if len(b) else None,
            "c_range": (str(c[0])[:10], str(c[-1])[:10]) if len(c) else None}


# --------------------------------------------------------------------------- #
# Inference
# --------------------------------------------------------------------------- #
def hac_t(diff: np.ndarray, *, lags: int) -> dict:
    """Mean and Newey-West t of a (possibly overlapping) per-period stream."""
    d = np.asarray(diff, dtype=np.float64)
    d = d[np.isfinite(d)]
    n = d.size
    if n < 12:
        return {"n": int(n), "mean": None, "t": None}
    mu = float(d.mean())
    dev = d - mu
    L = max(1, min(int(lags), n - 2))
    var = float(dev @ dev / n)
    for k in range(1, L + 1):
        var += 2.0 * (1.0 - k / (L + 1.0)) * float(dev[k:] @ dev[:-k] / n)
    var = max(var, 1e-18)
    t = mu / math.sqrt(var / n)
    return {"n": int(n), "mean": mu, "t": float(t), "hac_lags": int(L)}


def effective_sample(diff: np.ndarray, *, max_lag: int = 20) -> dict:
    d = np.asarray(diff, dtype=np.float64)
    d = d[np.isfinite(d)]
    n = d.size
    if n < 24:
        return {"n": int(n), "ess": int(n), "ratio": 1.0}
    dev = d - d.mean()
    v0 = float(dev @ dev / n)
    if v0 <= 0:
        return {"n": int(n), "ess": int(n), "ratio": 1.0}
    s = 0.0
    for k in range(1, min(max_lag, n - 2) + 1):
        rho = float(dev[k:] @ dev[:-k] / n) / v0
        if rho <= 0:
            break
        s += (1.0 - k / n) * rho
    ratio = min(1.0, 1.0 / (1.0 + 2.0 * s))
    return {"n": int(n), "ess": int(max(1, round(n * ratio))),
            "ratio": float(ratio)}


def max_drawdown(net: np.ndarray) -> float:
    x = np.asarray(net, dtype=np.float64)
    x = np.where(np.isfinite(x), x, 0.0)
    eq = np.cumprod(1.0 + x)
    peak = np.maximum.accumulate(eq)
    return float((eq / peak - 1.0).min()) if x.size else float("nan")


def cvar(net: np.ndarray, q: float = 0.05) -> float:
    x = np.asarray(net, dtype=np.float64)
    x = x[np.isfinite(x)]
    if x.size < 20:
        return float("nan")
    cut = np.quantile(x, q)
    tail = x[x <= cut]
    return float(tail.mean()) if tail.size else float("nan")


def scorecard(gross: np.ndarray, cost: np.ndarray, control: np.ndarray, *,
              periods_per_year: float, overlap: int = 1,
              turnover_per_period: float = None) -> dict:
    """The economic judgement of ONE stream against ITS declared control.

    ``gross``: per-period gross return of the book; ``cost``: per-period cost
    (>= 0); ``control``: per-period return of the declared control on the
    SAME dates. ``overlap``: target overlap in periods (h for h-period
    targets marked every period; 1 for non-overlapping)."""
    g = np.asarray(gross, dtype=np.float64)
    k = np.asarray(cost, dtype=np.float64)
    b = np.asarray(control, dtype=np.float64)
    n = min(g.size, k.size, b.size)
    g, k, b = g[:n], k[:n], b[:n]
    ok = np.isfinite(g) & np.isfinite(k) & np.isfinite(b)
    g, k, b = g[ok], k[ok], b[ok]
    net = g - k
    d = net - b
    lags = max(4, int(overlap))
    base = hac_t(d, lags=lags)
    ppy = float(periods_per_year)
    # The stream is PER MARK (daily marks of a possibly overlapping book),
    # so annualisation uses marks per year; the overlap enters INFERENCE
    # (HAC lags, ESS), never the annualisation.
    ann = ppy
    half = len(d) // 2
    sd = float(np.nanstd(d, ddof=1)) if len(d) > 3 else float("nan")
    out = {
        "n_periods": int(len(d)),
        "overlap": int(overlap),
        "periods_per_year": ppy,
        "gross_ann": float(np.nanmean(g) * ann),
        "cost_ann": float(np.nanmean(k) * ann),
        "net_ann": float(np.nanmean(net) * ann),
        "excess_ann": float(np.nanmean(d) * ann),
        "vol_ann": float(sd * math.sqrt(ann)) if sd and sd > 0 else None,
        "excess_t_hac": base["t"],
        "hac_lags": base.get("hac_lags"),
        "sharpe": float(np.nanmean(d) / sd * math.sqrt(ann))
        if sd and sd > 0 else None,
        "sortino": _sortino(d, ann),
        "max_drawdown": max_drawdown(net),
        "cvar_5": cvar(d),
        "same_sign_halves": bool(len(d) >= 24
                                 and np.nanmean(d[:half]) * np.nanmean(
                                     d[half:]) > 0),
        "effective_sample": effective_sample(d),
        "cost_stress": {},
    }
    for mult in C.COST_STRESS_MULTIPLIERS:
        dm = (g - mult * k) - b
        r = hac_t(dm, lags=lags)
        out["cost_stress"]["x%g" % mult] = {
            "excess_ann": float(np.nanmean(dm) * ann), "t": r["t"]}
    if turnover_per_period is not None:
        out["turnover_per_period"] = float(turnover_per_period)
        out["turnover_ann"] = float(turnover_per_period * ppy)
    out["diff_stream"] = d
    return out


def _sortino(d: np.ndarray, ann: float) -> Optional[float]:
    x = d[np.isfinite(d)]
    down = x[x < 0]
    if x.size < 12 or down.size < 3:
        return None
    dd = float(down.std(ddof=1))
    return float(x.mean() / dd * math.sqrt(ann)) if dd > 0 else None


def factor_residual(diff: np.ndarray, factors: pd.DataFrame, *,
                    overlap: int = 1) -> dict:
    """OLS of the excess stream on declared factor streams; HAC t of alpha."""
    d = np.asarray(diff, dtype=np.float64)
    F = factors.to_numpy(dtype=np.float64)
    n = min(len(d), len(F))
    d, F = d[:n], F[:n]
    ok = np.isfinite(d) & np.all(np.isfinite(F), axis=1)
    d, F = d[ok], F[ok]
    if len(d) < 24 + F.shape[1]:
        return {"state": "INSUFFICIENT", "n": int(len(d))}
    X = np.column_stack([np.ones(len(d)), F])
    beta, *_ = np.linalg.lstsq(X, d, rcond=None)
    resid = d - X @ beta + beta[0]     # residual stream KEEPING the alpha
    r = hac_t(resid, lags=max(4, overlap))
    ss_res = float(((d - X @ beta) ** 2).sum())
    ss_tot = float(((d - d.mean()) ** 2).sum())
    return {"state": "OK", "n": int(len(d)),
            "alpha_per_period": float(beta[0]),
            "alpha_t_hac": r["t"],
            "betas": {c: float(b) for c, b in zip(factors.columns, beta[1:])},
            "r_squared": float(1.0 - ss_res / ss_tot) if ss_tot > 0 else None}


# --------------------------------------------------------------------------- #
# Gates
# --------------------------------------------------------------------------- #
def research_candidate_gate(card: dict, *, kill_no_flip: bool = None,
                            residual_t: float = None) -> dict:
    """Apply contract.RESEARCH_CANDIDATE_GATE to a Zone-B scorecard."""
    g = C.RESEARCH_CANDIDATE_GATE
    checks = {
        "t_min": (card.get("excess_t_hac") or 0)
        >= g["after_cost_excess_t_hac_min"],
        "same_sign_halves": bool(card.get("same_sign_halves")),
        "positive_at_2x_cost":
            (card.get("cost_stress", {}).get("x2", {}).get("excess_ann") or 0)
            > 0,
        "min_effective_decisions":
            (card.get("effective_sample", {}).get("ess") or 0)
            >= g["min_effective_decisions"],
    }
    if kill_no_flip is not None:
        checks["kill_tests_no_sign_flip"] = bool(kill_no_flip)
    if residual_t is not None:
        checks["factor_residual_t"] = residual_t >= g["factor_residual_t_min"]
    return {"checks": checks, "passes": all(checks.values())}


def family_bh(t_stats: dict, q: float = None) -> dict:
    """Benjamini-Hochberg within one family, via the R31 owner."""
    q = q or C.RESEARCH_CANDIDATE_GATE["family_bh_q"]
    named = [(k, v) for k, v in t_stats.items() if v is not None]
    ps = [MT.two_sided_p(t) for _, t in named]
    res = MT.benjamini_hochberg(ps, q=q)
    rejected = res.get("rejected") or res.get("n_rejected")
    flags = res.get("rejected_flags") or res.get("flags")
    out = {"q": q, "raw": {k: p for (k, _), p in zip(named, ps)}}
    if isinstance(flags, list) and len(flags) == len(named):
        out["survivors"] = [k for (k, _), f in zip(named, flags) if f]
    else:
        srt = sorted(zip(named, ps), key=lambda x: x[1])
        m = len(ps)
        keep, kmax = [], 0
        for i, ((k, _), p) in enumerate(srt, start=1):
            if p <= q * i / m:
                kmax = i
        out["survivors"] = [k for ((k, _), p) in srt[:kmax]]
    out["n_tests"] = len(named)
    out["n_survivors"] = len(out["survivors"])
    return out


def deflated_sharpe(net: np.ndarray, *, n_trials: int,
                    trial_sharpe_variance: float) -> dict:
    return R39B.deflated_sharpe(net, n_trials=n_trials,
                                trial_sharpe_variance=trial_sharpe_variance)


def summarise(card: dict) -> dict:
    """A JSON-safe copy of a scorecard (drops the raw stream)."""
    return {k: v for k, v in card.items() if k != "diff_stream"}
