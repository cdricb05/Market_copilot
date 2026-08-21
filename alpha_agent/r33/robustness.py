"""alpha_agent.r33.robustness - what an apparent finalist is actually made of.

A cross-market result can be a genuine broad effect, or it can be one market, one
decade, or one parameter setting wearing a costume. These diagnostics tell those
apart, and two of them are qualification conditions rather than commentary:

* **leave-one-market-out.** If removing a single market removes most of the
  result, the campaign found a trade in that market, not broad predictive edge.
  This is the whole premise of Lane A, so it is a gate.
* **subperiod stability.** If one subperiod carries the result, what was
  measured is an episode. 2008 and 2020 are large enough to fund an entire
  backtest on their own.

The others - leave-one-asset-class-out, parameter sensitivity, cost sensitivity,
volatility-target sensitivity, exposure and turnover concentration - are
reported for every finalist because they are how a reader judges what the number
means.

Nothing here re-selects anything. Robustness runs AFTER the finalist set is
frozen, on the same frozen evidence, and it can only remove a candidate.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as _contract
from . import economic as _economic

CALCULATION_OWNER = "alpha_agent.r33.robustness"


def market_contributions(weights: pd.DataFrame,
                         excess_returns: pd.DataFrame) -> pd.Series:
    """Total overlay PnL attributable to each market."""
    cols = [c for c in weights.columns if c in excess_returns.columns]
    idx = weights.index.intersection(excess_returns.index)
    W = weights.reindex(index=idx, columns=cols).fillna(0.0)
    R = excess_returns.reindex(index=idx, columns=cols).fillna(0.0)
    return (W * R).sum(axis=0)


def leave_one_market_out(weights: pd.DataFrame, excess_returns: pd.DataFrame,
                         cash: pd.Series, *, meta: dict, horizon: int,
                         control: np.ndarray) -> dict:
    """Re-judge the book with each market removed in turn."""
    base = _economic.evaluate_book(weights, excess_returns, cash, meta=meta,
                                   horizon=horizon)
    base_excess = _economic.excess_significance(base["net"], control,
                                                horizon=horizon)
    base_mean = base_excess.get("mean_excess")
    results, worst_symbol, worst_value = {}, None, None
    for sym in list(weights.columns):
        reduced = weights.drop(columns=[sym])
        if reduced.shape[1] == 0:
            continue
        path = _economic.evaluate_book(reduced, excess_returns, cash, meta=meta,
                                       horizon=horizon)
        ex = _economic.excess_significance(path["net"], control,
                                            horizon=horizon)
        v = ex.get("mean_excess")
        results[sym] = {"mean_excess": v, "t_stat": ex.get("t_stat")}
        if v is not None and (worst_value is None or v < worst_value):
            worst_value, worst_symbol = v, sym
    retained = None
    if base_mean not in (None, 0.0) and worst_value is not None:
        retained = float(worst_value / base_mean)
    return {
        "base_mean_excess": base_mean,
        "worst_market_when_removed": worst_symbol,
        "worst_mean_excess_when_removed": worst_value,
        "fraction_retained_worst_case": retained,
        "single_market_dependent": bool(
            retained is not None
            and retained < (1.0 - _contract.MAX_SINGLE_MARKET_CONTRIBUTION)),
        "per_market": results,
    }


def leave_one_asset_class_out(weights: pd.DataFrame,
                              excess_returns: pd.DataFrame, cash: pd.Series, *,
                              meta: dict, horizon: int,
                              control: np.ndarray) -> dict:
    classes = sorted({meta[s]["asset_class"] for s in weights.columns
                      if s in meta})
    out = {}
    for ac in classes:
        keep = [s for s in weights.columns
                if meta.get(s, {}).get("asset_class") != ac]
        if not keep:
            continue
        path = _economic.evaluate_book(weights[keep], excess_returns, cash,
                                       meta=meta, horizon=horizon)
        ex = _economic.excess_significance(path["net"], control,
                                            horizon=horizon)
        out[ac] = {"mean_excess": ex.get("mean_excess"),
                   "t_stat": ex.get("t_stat")}
    return out


def subperiod_stability(net: np.ndarray, control: np.ndarray,
                        dates: pd.DatetimeIndex, *, horizon: int,
                        n_periods: int = 4) -> dict:
    """Split the evaluation window into equal blocks and judge each."""
    n = min(len(net), len(control), len(dates))
    if n < n_periods * 8:
        # FAIL CLOSED. An unmeasurable stability check is not a passed one: the
        # first version returned no flag here, so a candidate with too few
        # observations to test for episode-dependence satisfied the
        # "not dependent on a single subperiod" condition by default.
        return {"state": "INSUFFICIENT_PERIODS", "subperiods": [],
                "single_subperiod_dependent": True,
                "unmeasurable": True,
                "periods_available": int(n),
                "periods_required": int(n_periods * 8)}
    edges = np.linspace(0, n, int(n_periods) + 1).astype(int)
    blocks, contributions = [], []
    for k in range(int(n_periods)):
        a, b = edges[k], edges[k + 1]
        d = np.asarray(net[a:b]) - np.asarray(control[a:b])
        d = d[np.isfinite(d)]
        if d.size < 4:
            continue
        total = float(d.sum())
        contributions.append(total)
        blocks.append({
            "from": str(dates[a].date()), "to": str(dates[b - 1].date()),
            "periods": int(d.size), "mean_excess": float(d.mean()),
            "total_excess": total,
            "annualised_excess": float(d.mean()
                                       * _economic.periods_per_year(horizon)),
            "positive": bool(d.mean() > 0)})
    total_abs = sum(abs(c) for c in contributions)
    top_share = (max(contributions) / total_abs) if total_abs > 0 and \
        contributions else None
    return {
        "state": "OK",
        "subperiods": blocks,
        "positive_subperiod_fraction":
            float(np.mean([b["positive"] for b in blocks])) if blocks else None,
        "largest_subperiod_share_of_total": top_share,
        "single_subperiod_dependent": bool(
            top_share is not None
            and top_share > _contract.MAX_SINGLE_SUBPERIOD_CONTRIBUTION),
    }


def cost_sensitivity(weights: pd.DataFrame, excess_returns: pd.DataFrame,
                     cash: pd.Series, *, meta: dict, horizon: int,
                     control: np.ndarray) -> dict:
    """Re-judge at every declared cost multiplier.

    A result that survives only its most optimistic cost assumption has not
    survived. The multipliers are frozen in the contract, not chosen here.
    """
    out = {}
    for mult in _contract.COST_SENSITIVITY_MULTIPLIERS:
        path = _economic.evaluate_book(weights, excess_returns, cash, meta=meta,
                                       horizon=horizon, cost_multiplier=mult)
        ex = _economic.excess_significance(path["net"], control,
                                           horizon=horizon)
        out[str(mult)] = {
            "mean_excess": ex.get("mean_excess"),
            "annualised_excess": ex.get("annualised_excess"),
            "t_stat": ex.get("t_stat"),
            "net_return_annualised": _economic.annualised_return(
                path["net"], horizon=horizon),
            "positive": bool((ex.get("mean_excess") or 0.0) > 0.0)}
    survives = all(v["positive"] for v in out.values())
    return {"by_multiplier": out, "positive_at_every_multiplier": survives,
            "acceptable": survives}


def volatility_target_sensitivity(forecast_rows: dict, *, build_positions_fn,
                                  excess_returns: pd.DataFrame,
                                  cash: pd.Series, meta: dict, horizon: int,
                                  control: np.ndarray) -> dict:
    """Re-judge at each declared volatility target."""
    out = {}
    for target in _contract.VOLATILITY_TARGET_SENSITIVITY:
        W = build_positions_fn(target_position_vol=target)
        if W is None or W.empty:
            continue
        path = _economic.evaluate_book(W, excess_returns, cash, meta=meta,
                                       horizon=horizon)
        ex = _economic.excess_significance(path["net"], control,
                                           horizon=horizon)
        out[str(target)] = {"mean_excess": ex.get("mean_excess"),
                            "t_stat": ex.get("t_stat"),
                            "sharpe": _economic.sharpe(path["net"],
                                                       horizon=horizon)}
    return out


def parameter_cliff(registry_rows: list, candidate: dict, *,
                    metric_key: str, segment: str = "validation") -> dict:
    """Do the candidate's NEIGHBOURS work, or is it alone on a spike?

    Neighbours are configurations from the same family and target that differ in
    at most one hyperparameter. A candidate whose neighbours all fail was found
    by the search, not by the data.
    """
    spec = candidate.get("spec", {})
    fam, target = spec.get("family"), spec.get("target")
    base = (candidate.get("result", {}).get(segment) or {}).get(metric_key)
    if base is None:
        return {"state": "NO_BASE_VALUE", "severe_cliff": False}
    neighbours = []
    for row in registry_rows:
        s = row.get("spec", {})
        if row.get("spec_hash") == candidate.get("spec_hash"):
            continue
        if s.get("family") != fam or s.get("target") != target:
            continue
        keys = set(spec) | set(s)
        diffs = sum(1 for k in keys if spec.get(k) != s.get(k))
        if diffs > 1:
            continue
        v = (row.get("result", {}).get(segment) or {}).get(metric_key)
        if v is not None:
            neighbours.append(float(v))
    if not neighbours:
        return {"state": "NO_NEIGHBOURS", "severe_cliff": False,
                "neighbour_count": 0}
    median = float(np.median(neighbours))
    retention = median / float(base) if float(base) != 0.0 else None
    severe = bool(retention is not None
                  and retention < _contract.MIN_NEIGHBOUR_RETENTION)
    return {"state": "OK", "neighbour_count": len(neighbours),
            "candidate_value": float(base), "neighbour_median": median,
            "retention": retention, "severe_cliff": severe,
            "min_neighbour_retention": _contract.MIN_NEIGHBOUR_RETENTION}


def exposure_decomposition(weights: pd.DataFrame, meta: dict) -> dict:
    by_class: dict = {}
    for sym in weights.columns:
        ac = meta.get(sym, {}).get("asset_class", "UNKNOWN")
        by_class.setdefault(ac, []).append(sym)
    gross = np.abs(weights.fillna(0.0)).sum(axis=1).replace(0.0, np.nan)
    out = {}
    for ac, syms in sorted(by_class.items()):
        share = (np.abs(weights[syms].fillna(0.0)).sum(axis=1) / gross).mean()
        out[ac] = {"mean_gross_share": float(share) if np.isfinite(share)
                   else None,
                   "mean_net_exposure": float(
                       weights[syms].fillna(0.0).sum(axis=1).mean())}
    return out


def turnover_concentration(weights: pd.DataFrame) -> dict:
    dw = weights.fillna(0.0).diff().abs()
    dw.iloc[0] = weights.fillna(0.0).iloc[0].abs()
    per_market = dw.sum(axis=0)
    total = float(per_market.sum())
    if total <= 0:
        return {"state": "NO_TURNOVER"}
    share = (per_market / total).sort_values(ascending=False)
    return {"state": "OK",
            "top_market": str(share.index[0]),
            "top_market_share": float(share.iloc[0]),
            "top_five_share": float(share.iloc[:5].sum())}
