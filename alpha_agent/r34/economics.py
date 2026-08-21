"""alpha_agent.r34.economics - the ONE economic judge for Release 34.

**The control decides everything.** Release 32 measured six sleeves against CASH
and all six beat it; not one beat a volatility-matched mix of the benchmark and
cash. Over a long window anything holding risk beats bills, so excess over cash
measures EXPOSURE rather than skill, and a campaign that ranks on it will
promote beta and call it alpha. This module reports excess over cash as a
diagnostic and refuses to rank on it.

**The primary statistic is after-cost excess UTILITY**, declared in the contract
before evaluation:

    dU = [mu_book - (g/2) var_book] - [mu_ctrl - (g/2) var_ctrl]

annualised, both legs on the same forecast dates. Utility rather than raw
excess, because a book can raise its mean purely by carrying more risk and the
volatility-matched control matches the book's REALISED volatility rather than
its risk at every moment - a book that is fully invested in calm periods and in
cash during crises can match on average while taking a quite different risk.

**Cost is charged on traded notional** - ``sum |dw|``, sells and buys - at the
MEASURED liquidity tier of each instrument. Release 31 charged one-way turnover
and every net return it published understated cost by roughly half.

**The book is a measurement device.** It is not a portfolio target, it is never
written anywhere operational, and no capital stands behind it.

``max_drawdown`` and ``cvar`` are imported from :mod:`alpha_agent.r31.judge`;
the annualising statistics are re-parameterised here because Release 34 runs
three decision cadences and Release 31's are bound to one.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from ..r31.judge import cvar, max_drawdown  # cadence-free, reused
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r34.economics"

SESSIONS_PER_YEAR = 252.0


def periods_per_year(horizon: int) -> float:
    return SESSIONS_PER_YEAR / float(horizon)


def annualised_return(r: np.ndarray, *, horizon: int) -> float:
    v = np.asarray(r, dtype=np.float64)
    v = v[np.isfinite(v)]
    if v.size == 0:
        return float("nan")
    growth = float(np.prod(1.0 + v))
    if growth <= 0:
        return float("nan")
    return float(growth ** (periods_per_year(horizon) / v.size) - 1.0)


def annualised_volatility(r: np.ndarray, *, horizon: int) -> float:
    v = np.asarray(r, dtype=np.float64)
    v = v[np.isfinite(v)]
    if v.size < 3:
        return float("nan")
    return float(v.std(ddof=1) * math.sqrt(periods_per_year(horizon)))


def sharpe(r: np.ndarray, *, horizon: int, cash: np.ndarray = None) -> float:
    """Sharpe of the EXCESS-over-cash return, which is what Sharpe means."""
    v = np.asarray(r, dtype=np.float64)
    if cash is not None:
        c = np.asarray(cash, dtype=np.float64)
        n = min(v.size, c.size)
        v = v[:n] - c[:n]
    v = v[np.isfinite(v)]
    if v.size < 3:
        return float("nan")
    sd = float(v.std(ddof=1))
    if sd <= 0:
        return float("nan")
    return float(v.mean() / sd * math.sqrt(periods_per_year(horizon)))


def sortino(r: np.ndarray, *, horizon: int, cash: np.ndarray = None) -> float:
    v = np.asarray(r, dtype=np.float64)
    if cash is not None:
        c = np.asarray(cash, dtype=np.float64)
        n = min(v.size, c.size)
        v = v[:n] - c[:n]
    v = v[np.isfinite(v)]
    down = v[v < 0.0]
    if v.size < 3 or down.size < 2:
        return float("nan")
    dd = float(down.std(ddof=1))
    if dd <= 0:
        return float("nan")
    return float(v.mean() / dd * math.sqrt(periods_per_year(horizon)))


def utility(net: np.ndarray, *, horizon: int,
            risk_aversion: float = _contract.RISK_AVERSION) -> float:
    """Mean-variance certainty equivalent, annualised."""
    v = np.asarray(net, dtype=np.float64)
    v = v[np.isfinite(v)]
    if v.size < 8:
        return float("nan")
    ppy = periods_per_year(horizon)
    mu = float(v.mean()) * ppy
    var = float(v.var(ddof=1)) * ppy
    return float(mu - 0.5 * float(risk_aversion) * var)


# --------------------------------------------------------------------------- #
# Book evaluation
# --------------------------------------------------------------------------- #
def cost_rates(symbols, meta: dict) -> np.ndarray:
    """One-way cost per side, as a fraction, from each instrument's MEASURED
    liquidity tier."""
    return np.asarray(
        [float(meta.get(s, {}).get("cost_bps_per_side", 6.0)) / 1e4
         for s in symbols], dtype=np.float64)


def evaluate_book(weights: pd.DataFrame, excess_returns: pd.DataFrame,
                  cash: pd.Series, *, meta: dict, horizon: int,
                  cost_multiplier: float = 1.0) -> dict:
    """Turn a weight path into an after-cost return path.

    Total return is ``cash + sum(w * excess) - cost``: the book is an overlay on
    cash, which is the correct formulation for a panel of excess returns and
    keeps the risk-free leg explicit. A weight in nothing earns the bill rate,
    which is exactly right - cash is a real asset choice.
    """
    if weights.empty:
        return {"state": "NO_POSITIONS", "net": np.zeros(0),
                "dates": pd.DatetimeIndex([])}
    cols = [c for c in weights.columns if c in excess_returns.columns]
    idx = weights.index.intersection(excess_returns.index)
    W = weights.reindex(index=idx, columns=cols).fillna(0.0)
    R = excess_returns.reindex(index=idx, columns=cols)
    rates = cost_rates(cols, meta) * float(cost_multiplier)

    Rf = R.to_numpy()
    Wf = W.to_numpy()
    # An instrument with no observed return contributes nothing rather than
    # being silently dropped, which would inflate whatever remains.
    contrib = np.where(np.isfinite(Rf), Wf * np.where(np.isfinite(Rf), Rf, 0.0),
                       0.0)
    gross_overlay = contrib.sum(axis=1)

    prev = np.zeros(Wf.shape[1])
    traded, costs = [], []
    for k in range(Wf.shape[0]):
        dw = np.abs(Wf[k] - prev)
        traded.append(float(dw.sum()))
        costs.append(float((dw * rates).sum()))
        prev = Wf[k]
    traded = np.asarray(traded)
    costs = np.asarray(costs)

    cash_leg = cash.reindex(idx).fillna(0.0).to_numpy()
    gross = cash_leg + gross_overlay
    net = gross - costs
    return {"state": "OK", "dates": idx, "gross": gross, "net": net,
            "costs": costs, "traded_notional": traded,
            "contribution": contrib, "columns": cols,
            "weights": Wf, "cash_leg": cash_leg,
            "cost_rates_used": rates,
            "gross_exposure": np.abs(Wf).sum(axis=1),
            "net_exposure": Wf.sum(axis=1),
            "cash_weight": 1.0 - np.clip(np.abs(Wf).sum(axis=1), 0.0, 1.0),
            "cost_multiplier": float(cost_multiplier)}


# --------------------------------------------------------------------------- #
# Controls
# --------------------------------------------------------------------------- #
def volatility_matched_control(book_net: np.ndarray, bench_excess: np.ndarray,
                               cash: np.ndarray) -> dict:
    """The passive benchmark/cash mix carrying the book's risk, none of its
    timing. Beating THIS is skill."""
    a = np.asarray(book_net, dtype=np.float64)
    b = np.asarray(bench_excess, dtype=np.float64)
    c = np.asarray(cash, dtype=np.float64)
    n = min(a.size, b.size, c.size)
    if n < 8:
        return {"state": "INSUFFICIENT_PERIODS", "weight": None, "series": None}
    a, b, c = a[:n], b[:n], c[:n]
    sa = float(np.nanstd(a - c, ddof=1))
    sb = float(np.nanstd(b, ddof=1))
    if not math.isfinite(sb) or sb <= 0:
        return {"state": "DEGENERATE_BENCHMARK", "weight": None, "series": None}
    # No leverage is available to this paper book, so the control cannot hold
    # more than 100 % of the benchmark either.
    w = float(min(sa / sb, 1.0))
    return {"state": "OK", "weight": w,
            "book_volatility": sa, "benchmark_volatility": sb,
            "series": c + w * b}


def equal_weight_control(excess_returns: pd.DataFrame,
                         tradable: pd.DataFrame, cash: pd.Series) -> np.ndarray:
    """Equal weight across everything TRADABLE - the naive multi-asset book.

    Restricted to tradable instruments on each date for the same reason the
    research book is: an equal-weight control that could hold instruments before
    they listed would be a control nobody could have run.
    """
    idx = excess_returns.index
    T = tradable.reindex(index=idx, columns=excess_returns.columns).fillna(
        False).to_numpy()
    R = excess_returns.reindex(index=idx).to_numpy()
    n = T.sum(axis=1)
    W = np.where(T, 1.0 / np.maximum(n, 1)[:, None], 0.0)
    contrib = np.where(np.isfinite(R), W * np.where(np.isfinite(R), R, 0.0), 0.0)
    return cash.reindex(idx).fillna(0.0).to_numpy() + contrib.sum(axis=1)


def sixty_forty_control(excess_returns: pd.DataFrame, cash: pd.Series, *,
                        equity: str, bond: str) -> np.ndarray:
    """The default multi-asset portfolio a reasonable person already holds.

    A cross-asset book that cannot beat 60/40 has not earned its complexity.
    """
    idx = excess_returns.index
    out = cash.reindex(idx).fillna(0.0).to_numpy().copy()
    for sym, w in ((equity, 0.60), (bond, 0.40)):
        if sym in excess_returns.columns:
            r = excess_returns[sym].reindex(idx).to_numpy()
            out = out + w * np.where(np.isfinite(r), r, 0.0)
    return out


def trend_control(prices: pd.DataFrame, excess_returns: pd.DataFrame,
                  tradable: pd.DataFrame, cash: pd.Series, *,
                  lookback: int = 252) -> np.ndarray:
    """Canonical transparent trend: hold equal-weight what is above its own
    trailing average, otherwise cash. The unfitted rule any learner must beat.
    """
    idx = excess_returns.index
    ma = prices.rolling(int(lookback), min_periods=int(lookback) // 2).mean()
    above = (prices > ma).reindex(idx).fillna(False)
    T = tradable.reindex(index=idx, columns=excess_returns.columns).fillna(False)
    hold = (above & T).to_numpy()
    n = hold.sum(axis=1)
    W = np.where(hold, 1.0 / np.maximum(n, 1)[:, None], 0.0)
    R = excess_returns.reindex(index=idx).to_numpy()
    contrib = np.where(np.isfinite(R), W * np.where(np.isfinite(R), R, 0.0), 0.0)
    return cash.reindex(idx).fillna(0.0).to_numpy() + contrib.sum(axis=1)


# --------------------------------------------------------------------------- #
# Inference
# --------------------------------------------------------------------------- #
def excess_significance(book_net: np.ndarray, control: np.ndarray, *,
                        horizon: int) -> dict:
    """Paired mean excess and its Newey-West t-statistic, on the same dates."""
    a = np.asarray(book_net, dtype=np.float64)
    b = np.asarray(control, dtype=np.float64)
    n = min(a.size, b.size)
    if n < 8:
        return {"n": int(n), "mean_excess": None, "t_stat": None}
    d = a[:n] - b[:n]
    d = d[np.isfinite(d)]
    if d.size < 8:
        return {"n": int(d.size), "mean_excess": None, "t_stat": None}
    mu = float(d.mean())
    dev = d - mu
    lags = 4
    var = float(dev @ dev / d.size)
    for L in range(1, min(lags, d.size - 1) + 1):
        var += 2.0 * (1.0 - L / (lags + 1.0)) * float(dev[L:] @ dev[:-L]
                                                      / d.size)
    t = mu / math.sqrt(var / d.size) if var > 0 else float("nan")
    return {"n": int(d.size), "mean_excess": mu,
            "annualised_excess": mu * periods_per_year(horizon),
            "t_stat": float(t) if math.isfinite(t) else None,
            "diff": d}


def describe(path: dict, *, horizon: int, control: np.ndarray = None) -> dict:
    """Every economic number the final report is required to carry."""
    net = np.asarray(path["net"], dtype=np.float64)
    gross = np.asarray(path["gross"], dtype=np.float64)
    cash_leg = np.asarray(path.get("cash_leg", np.zeros_like(net)),
                          dtype=np.float64)
    out = {
        "periods": int(net.size),
        "gross_return_annualised": annualised_return(gross, horizon=horizon),
        "net_return_annualised": annualised_return(net, horizon=horizon),
        "volatility_annualised": annualised_volatility(net, horizon=horizon),
        "sharpe": sharpe(net, horizon=horizon, cash=cash_leg),
        "sortino": sortino(net, horizon=horizon, cash=cash_leg),
        "max_drawdown": max_drawdown(net),
        "cvar_5pct": cvar(net, 0.05),
        "hit_rate": float(np.mean(net > 0.0)) if net.size else None,
        "utility_annualised": utility(net, horizon=horizon),
        "mean_traded_notional": float(np.mean(path["traded_notional"])),
        "annualised_turnover": float(np.mean(path["traded_notional"])
                                     * periods_per_year(horizon)),
        "annualised_cost": float(np.mean(path["costs"])
                                 * periods_per_year(horizon)),
        "mean_gross_exposure": float(np.mean(path["gross_exposure"])),
        "mean_net_exposure": float(np.mean(path["net_exposure"])),
        "mean_cash_weight": float(np.mean(path["cash_weight"])),
    }
    if control is not None:
        c = np.asarray(control, dtype=np.float64)
        n = min(net.size, c.size)
        out["control_return_annualised"] = annualised_return(c[:n],
                                                             horizon=horizon)
        out["control_volatility_annualised"] = annualised_volatility(
            c[:n], horizon=horizon)
        out["control_utility_annualised"] = utility(c[:n], horizon=horizon)
        sig = excess_significance(net[:n], c[:n], horizon=horizon)
        out["after_cost_excess_annualised"] = sig.get("annualised_excess")
        out["after_cost_excess_t_stat"] = sig.get("t_stat")
        u_book = out["utility_annualised"]
        u_ctrl = out["control_utility_annualised"]
        out["after_cost_excess_utility"] = (
            float(u_book - u_ctrl)
            if (u_book is not None and u_ctrl is not None
                and math.isfinite(u_book) and math.isfinite(u_ctrl))
            else None)
    return out


def judge_declaration() -> dict:
    """Everything that changes what an economic score MEANS."""
    return {
        "calculation_owner": CALCULATION_OWNER,
        "cost_base": _contract.COST_BASE,
        "cost_tier_bps": dict(_contract.COST_TIER_BPS),
        "cost_scenarios": dict(_contract.COST_SCENARIOS),
        "cost_scenario_primary": _contract.COST_SCENARIO_PRIMARY,
        "cash_is_scored": True,
        "cash_yield_symbol": _contract.CASH_YIELD_SYMBOL,
        "benchmark_symbol": _contract.BENCHMARK_SYMBOL,
        "primary_control": _contract.ECONOMIC_CONTROL,
        "controls": list(_contract.CONTROLS),
        "primary_decision_statistic": _contract.PRIMARY_DECISION_STATISTIC,
        "primary_decision_formula": _contract.PRIMARY_DECISION_FORMULA,
        "risk_aversion": _contract.RISK_AVERSION,
        "leverage_available": _contract.LEVERAGE_AVAILABLE,
        "max_gross_exposure": _contract.MAX_GROSS_EXPOSURE,
        "book_is_a_measurement_device": True,
        "excess_over_cash_may_rank": _contract.EXCESS_OVER_CASH_MAY_RANK,
        "excess_over_cash_rationale": (
            "over a long window anything holding risk beats bills, so excess "
            "over cash measures exposure rather than skill; it is reported and "
            "never ranked on"),
        "returns_are_total_returns": True,
        "total_return_rationale": (
            "dividends and coupons are part of what the holder earns; scoring "
            "a multi-asset book on capital-only prices would systematically "
            "penalise every income asset in it"),
    }


def behaviour_hash() -> str:
    from .. import r34
    return r34.sha(judge_declaration())
