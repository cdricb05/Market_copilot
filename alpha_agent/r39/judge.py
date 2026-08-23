"""alpha_agent.r39.judge - economic judgement, DELEGATING to the canonical
Release-34 economic owner.

Prediction metrics are secondary everywhere in this release; the primary
statistic is implementable after-cost excess versus the correct economic
control. This module owns NO annualisation, NO significance test and NO
control construction - those are imported from
:mod:`alpha_agent.r34.economics` (which itself reuses the Release-31
drawdown/CVaR owners) and :mod:`alpha_agent.r36.experiments` (minimum
detectable excess). What lives here is only the assembly of one candidate's
full economic report from a book path and its control series.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from ..r31.judge import cvar, max_drawdown
from ..r34 import economics as _econ
from ..r36.experiments import minimum_detectable_excess
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r39.judge"

#: Futures front-roll returns and self-financed long/short books are already
#: excess-of-cash; the cash leg is identically zero by construction.
CASH_LEG_IS_ZERO = True


def vol_matched(book_net: np.ndarray, control_net: np.ndarray) -> dict:
    """The Release-34 volatility-matched control with a zero cash leg."""
    cash = np.zeros(min(len(book_net), len(control_net)))
    return _econ.volatility_matched_control(
        np.asarray(book_net)[: cash.size],
        np.asarray(control_net)[: cash.size], cash)


def describe_book(net: np.ndarray, gross: np.ndarray, costs: np.ndarray,
                  traded: np.ndarray, gross_exposure: np.ndarray, *,
                  horizon: int) -> dict:
    net = np.asarray(net, dtype=np.float64)
    ppy = _econ.periods_per_year(horizon)
    return {
        "periods": int(net.size),
        "gross_return_annualised": _econ.annualised_return(
            np.asarray(gross), horizon=horizon),
        "net_return_annualised": _econ.annualised_return(net,
                                                         horizon=horizon),
        "volatility_annualised": _econ.annualised_volatility(
            net, horizon=horizon),
        "sharpe": _econ.sharpe(net, horizon=horizon),
        "sortino": _econ.sortino(net, horizon=horizon),
        "max_drawdown": max_drawdown(net),
        "cvar_5pct": cvar(net, 0.05),
        "hit_rate": float(np.mean(net > 0.0)) if net.size else None,
        "annualised_turnover": float(np.nanmean(traded) * ppy),
        "annualised_cost": float(np.nanmean(costs) * ppy),
        "mean_gross_exposure": float(np.nanmean(gross_exposure)),
    }


def judge_candidate(book: dict, control_net: np.ndarray, *, horizon: int,
                    control_name: str,
                    vol_match_control: bool = True) -> dict:
    """One candidate's economic report on one zone. ``control_net`` is the
    UNMATCHED control path; the vol-matched series is built here so every
    candidate faces the same matching rule."""
    net = np.asarray(book["net"], dtype=np.float64)
    ctrl = np.asarray(control_net, dtype=np.float64)
    n = min(net.size, ctrl.size)
    net, ctrl = net[:n], ctrl[:n]
    if vol_match_control:
        vm = vol_matched(net, ctrl)
        matched = vm["series"] if vm.get("state") == "OK" else ctrl
        match_state = vm.get("state")
        match_weight = vm.get("weight")
    else:
        matched, match_state, match_weight = ctrl, "UNMATCHED", None
    sig = _econ.excess_significance(net, np.asarray(matched)[:n],
                                    horizon=horizon)
    out = describe_book(net, book["gross"][:n], book["costs"][:n],
                        book["traded_notional"][:n],
                        book["gross_exposure"][:n], horizon=horizon)
    excess_ann = sig.get("annualised_excess")
    t = sig.get("t_stat")
    out.update({
        "control": control_name,
        "control_return_annualised": _econ.annualised_return(
            np.asarray(matched)[:n], horizon=horizon),
        "vol_match_state": match_state,
        "vol_match_weight": match_weight,
        "after_cost_excess_annualised": excess_ann,
        "after_cost_excess_t_stat": t,
        "minimum_detectable_excess": minimum_detectable_excess(
            excess_ann if excess_ann is not None else float("nan"),
            t if t is not None else float("nan")),
        "excess_diff_series": sig.get("diff"),
    })
    return out


def rank_ic(pred: pd.DataFrame, fwd: pd.DataFrame) -> dict:
    """Per-date Spearman IC between predictions and realised forwards."""
    ics = []
    for dt in pred.index:
        p = pred.loc[dt]
        f = fwd.loc[dt] if dt in fwd.index else None
        if f is None:
            continue
        m = p.notna() & f.notna()
        if m.sum() < 5:
            continue
        ics.append(float(p[m].rank().corr(f[m].rank())))
    if len(ics) < 8:
        return {"n_dates": len(ics), "mean_ic": None, "t_stat": None}
    a = np.asarray(ics)
    t = float(a.mean() / (a.std(ddof=1) / math.sqrt(a.size))) \
        if a.std(ddof=1) > 0 else float("nan")
    return {"n_dates": int(a.size), "mean_ic": float(a.mean()),
            "t_stat": t if math.isfinite(t) else None}


def sign_split_robustness(diff: np.ndarray) -> dict:
    """Same-sign in halves and thirds of the excess-difference series."""
    d = np.asarray(diff, dtype=np.float64)
    d = d[np.isfinite(d)]
    if d.size < 24:
        return {"state": "INSUFFICIENT", "halves_same_sign": None,
                "thirds_same_sign": None}
    overall = np.sign(d.mean())
    halves = [np.sign(x.mean()) == overall and x.mean() != 0
              for x in np.array_split(d, 2)]
    thirds = [np.sign(x.mean()) == overall and x.mean() != 0
              for x in np.array_split(d, 3)]
    return {"state": "OK", "halves_same_sign": bool(all(halves)),
            "thirds_same_sign": bool(all(thirds)),
            "n_periods": int(d.size)}


def regime_slices(diff: np.ndarray, dates: pd.DatetimeIndex,
                  vix: pd.Series) -> dict:
    """Calm/stress slices by the trailing VIX median - an OBSERVABLE split."""
    if vix is None or vix.empty:
        return {"state": "NO_VIX"}
    d = pd.Series(np.asarray(diff, dtype=np.float64)[: len(dates)],
                  index=dates[: len(diff)])
    v = vix.reindex(dates, method="ffill")
    med = v.expanding(min_periods=24).median()
    stress = v > med
    calm_mean = float(d[~stress.fillna(False)].mean())
    stress_mean = float(d[stress.fillna(False)].mean())
    return {"state": "OK",
            "calm_mean_excess": calm_mean,
            "stress_mean_excess": stress_mean,
            "same_sign_across_regimes": bool(
                np.sign(calm_mean) == np.sign(stress_mean)
                and calm_mean != 0)}
