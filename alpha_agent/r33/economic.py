"""alpha_agent.r33.economic - the ONE economic judge for Release 33.

Predictive improvement is necessary and is not sufficient. This module turns a
forecast into a bounded, implementable research book and asks whether it earns
more, after cost, than an appropriate risk-matched control.

**The control decides everything.** Release 32 measured six sleeves against
CASH, and all six beat it. Not one beat a volatility-matched mix of the
benchmark and cash. Over a long window anything holding risk beats bills, so
excess over cash measures EXPOSURE, not skill; a campaign that ranks on it will
promote beta and call it alpha. The volatility-matched control holds

    w = book_volatility / benchmark_volatility        (capped at 1, no leverage)

of the benchmark and the rest in cash, so it carries the book's risk with none
of its timing. Beating THAT is skill.

**Cost is charged on traded notional** - ``sum |dw|``, sells and buys - because
a rebalance pays on both legs. Release 31 charged one-way turnover and every net
return it published understated cost by roughly half.

**The book is a measurement device.** It is not a portfolio target, it is never
written anywhere operational, and no capital stands behind it. Positions are
built from EXCESS returns as an overlay on cash, which is the correct
formulation for this panel and keeps cash a real asset choice rather than a
free zero.

``max_drawdown`` and ``cvar`` are imported from :mod:`alpha_agent.r31.judge`.
The annualising statistics are re-parameterised here rather than imported
because Release 31's are bound to a single decision cadence and Release 33 runs
three horizons; the arithmetic is identical, the periods-per-year is not.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from ..r31.judge import cvar, max_drawdown  # cadence-free, reused
from . import contract as _contract
from . import universe as _universe

CALCULATION_OWNER = "alpha_agent.r33.economic"

SESSIONS_PER_YEAR = 252.0

CONSTRUCTION_CROSS_SECTIONAL = "CROSS_SECTIONAL_ZERO_MEAN_WITHIN_ASSET_CLASS"
CONSTRUCTION_DIRECTIONAL = "DIRECTIONAL_TIME_SERIES"
CONSTRUCTIONS = (CONSTRUCTION_CROSS_SECTIONAL, CONSTRUCTION_DIRECTIONAL)


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


def sharpe(r: np.ndarray, *, horizon: int) -> float:
    v = np.asarray(r, dtype=np.float64)
    v = v[np.isfinite(v)]
    if v.size < 3:
        return float("nan")
    sd = float(v.std(ddof=1))
    if sd <= 0:
        return float("nan")
    return float(v.mean() / sd * math.sqrt(periods_per_year(horizon)))


def sortino(r: np.ndarray, *, horizon: int) -> float:
    v = np.asarray(r, dtype=np.float64)
    v = v[np.isfinite(v)]
    down = v[v < 0.0]
    if v.size < 3 or down.size < 2:
        return float("nan")
    dd = float(down.std(ddof=1))
    if dd <= 0:
        return float("nan")
    return float(v.mean() / dd * math.sqrt(periods_per_year(horizon)))


# --------------------------------------------------------------------------- #
# Position construction
# --------------------------------------------------------------------------- #
def build_positions(forecast: np.ndarray, *, symbols: np.ndarray,
                    dates: pd.DatetimeIndex, asset_class: np.ndarray,
                    trailing_vol: np.ndarray,
                    construction: str = CONSTRUCTION_CROSS_SECTIONAL,
                    target_position_vol: float = 0.10,
                    max_gross: float = _contract.MAX_GROSS_EXPOSURE
                    ) -> pd.DataFrame:
    """Turn per-row forecasts into a weight matrix ``dates x symbols``.

    Cross-sectional construction demeans the signal WITHIN asset class. That is
    not cosmetic: this panel mixes equity price indices that exclude dividends
    with bond indices that include coupon, so a constant per-market drift
    difference exists, and a directional book would be paid for that difference
    rather than for predicting anything. Demeaning within class removes most of
    it.

    Each position is scaled to equal risk by its trailing volatility, so the
    book is not simply an expression of which market happens to be most
    volatile.
    """
    df = pd.DataFrame({
        "date": pd.DatetimeIndex(dates),
        "symbol": np.asarray(symbols),
        "f": np.asarray(forecast, dtype=np.float64),
        "ac": np.asarray(asset_class),
        "vol": np.asarray(trailing_vol, dtype=np.float64),
    })
    df = df[np.isfinite(df["f"])]
    if df.empty:
        return pd.DataFrame()

    def _weights(block: pd.DataFrame) -> pd.Series:
        s = block["f"].astype(float)
        if construction == CONSTRUCTION_CROSS_SECTIONAL:
            s = s.groupby(block["ac"]).transform(lambda v: v - v.mean())
        sd = float(s.std(ddof=1)) if len(s) > 1 else 0.0
        if not math.isfinite(sd) or sd <= 0:
            return pd.Series(0.0, index=block.index)
        z = (s / sd).clip(-3.0, 3.0)
        vol = block["vol"].astype(float).clip(lower=0.02)
        w = z * (float(target_position_vol) / vol)
        gross = float(np.abs(w).sum())
        if gross > float(max_gross) and gross > 0:
            w = w * (float(max_gross) / gross)
        return w

    df["w"] = np.concatenate([_weights(b).to_numpy()
                              for _d, b in df.groupby("date", sort=True)])
    return df.pivot_table(index="date", columns="symbol", values="w",
                          aggfunc="last").sort_index()


def cost_rates(symbols, meta: dict) -> np.ndarray:
    return np.asarray([
        _contract.COST_PER_SIDE_BPS.get(
            meta.get(s, {}).get("asset_class", "EQUITY_INDEX"), 5.0) / 1e4
        for s in symbols], dtype=np.float64)


def evaluate_book(weights: pd.DataFrame, excess_returns: pd.DataFrame,
                  cash: pd.Series, *, meta: dict, horizon: int,
                  cost_multiplier: float = 1.0) -> dict:
    """Turn a weight path into an after-cost return path.

    Total return is ``cash + sum(w * excess) - cost``: the book is an overlay on
    cash, which is the correct formulation for a panel of excess returns and
    keeps the risk-free leg explicit.
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
    # A market with no observed return contributes nothing rather than being
    # silently dropped, which would inflate whatever remains.
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
            "gross_exposure": np.abs(Wf).sum(axis=1),
            "net_exposure": Wf.sum(axis=1),
            "cost_multiplier": float(cost_multiplier)}


# --------------------------------------------------------------------------- #
# Controls
# --------------------------------------------------------------------------- #
def volatility_matched_control(book_net: np.ndarray, bench_excess: np.ndarray,
                               cash: np.ndarray) -> dict:
    """The passive benchmark/cash mix carrying the book's risk, none of its timing."""
    a = np.asarray(book_net, dtype=np.float64)
    b = np.asarray(bench_excess, dtype=np.float64)
    c = np.asarray(cash, dtype=np.float64)
    n = min(a.size, b.size, c.size)
    if n < 8:
        return {"state": "INSUFFICIENT_PERIODS", "weight": None, "series": None}
    a, b, c = a[:n], b[:n], c[:n]
    sa = float(np.nanstd(a, ddof=1))
    sb = float(np.nanstd(b, ddof=1))
    if not math.isfinite(sb) or sb <= 0:
        return {"state": "DEGENERATE_BENCHMARK", "weight": None, "series": None}
    # No leverage is available to this paper book, so the control cannot hold
    # more than 100 % of the benchmark either.
    w = float(min(sa / sb, 1.0))
    return {"state": "OK", "weight": w,
            "book_volatility": sa, "benchmark_volatility": sb,
            "series": c + w * b}


def equal_risk_control(excess_returns: pd.DataFrame, trailing_vol: pd.DataFrame,
                       cash: pd.Series, *, gross: float = 1.0) -> np.ndarray:
    """Long every market at equal risk - the naive cross-market alternative."""
    idx = excess_returns.index
    V = trailing_vol.reindex(index=idx, columns=excess_returns.columns)
    inv = (1.0 / V.clip(lower=0.02)).replace([np.inf, -np.inf], np.nan)
    W = inv.div(inv.abs().sum(axis=1), axis=0).fillna(0.0) * float(gross)
    contrib = (W * excess_returns.fillna(0.0)).sum(axis=1)
    return (cash.reindex(idx).fillna(0.0) + contrib).to_numpy()


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
        var += 2.0 * (1.0 - L / (lags + 1.0)) * float(dev[L:] @ dev[:-L] / d.size)
    t = mu / math.sqrt(var / d.size) if var > 0 else float("nan")
    return {"n": int(d.size), "mean_excess": mu,
            "annualised_excess": mu * periods_per_year(horizon),
            "t_stat": float(t) if math.isfinite(t) else None,
            "_diff": d}


def describe(path: dict, *, horizon: int) -> dict:
    """Every economic number the final report is required to carry."""
    net = np.asarray(path["net"], dtype=np.float64)
    gross = np.asarray(path["gross"], dtype=np.float64)
    return {
        "periods": int(net.size),
        "gross_return_annualised": annualised_return(gross, horizon=horizon),
        "net_return_annualised": annualised_return(net, horizon=horizon),
        "volatility_annualised": annualised_volatility(net, horizon=horizon),
        "sharpe": sharpe(net, horizon=horizon),
        "sortino": sortino(net, horizon=horizon),
        "max_drawdown": max_drawdown(net),
        "cvar_5pct": cvar(net, 0.05),
        "hit_rate": float(np.mean(net > 0.0)) if net.size else None,
        "mean_traded_notional": float(np.mean(path["traded_notional"])),
        "annualised_turnover": float(np.mean(path["traded_notional"])
                                     * periods_per_year(horizon)),
        "mean_gross_exposure": float(np.mean(path["gross_exposure"])),
        "mean_net_exposure": float(np.mean(path["net_exposure"])),
        "total_cost_annualised": float(np.mean(path["costs"])
                                       * periods_per_year(horizon)),
    }


def utility(net: np.ndarray, *, horizon: int, risk_aversion: float = 2.0
            ) -> float:
    """Mean-variance certainty equivalent, annualised.

    Reported because a book can raise its mean return purely by carrying more
    risk. Utility charges for that, and the qualification gate requires an
    improvement here as well as in raw excess.
    """
    v = np.asarray(net, dtype=np.float64)
    v = v[np.isfinite(v)]
    if v.size < 8:
        return float("nan")
    ppy = periods_per_year(horizon)
    mu = float(v.mean()) * ppy
    var = float(v.var(ddof=1)) * ppy
    return float(mu - 0.5 * float(risk_aversion) * var)


def trailing_vol_frame(panel: dict, dates: pd.DatetimeIndex,
                       window: int = 63) -> pd.DataFrame:
    """Trailing volatility observed at each decision date - never future vol."""
    lr = panel["log_returns"]
    vol = lr.rolling(window, min_periods=window // 2).std(ddof=1) \
        * math.sqrt(SESSIONS_PER_YEAR)
    return vol.reindex(dates).ffill()


def judge_declaration() -> dict:
    """Everything that changes what an economic score MEANS."""
    return {
        "calculation_owner": CALCULATION_OWNER,
        "cost_base": _contract.COST_BASE,
        "cost_per_side_bps": dict(_contract.COST_PER_SIDE_BPS),
        "cost_sensitivity_multipliers": list(
            _contract.COST_SENSITIVITY_MULTIPLIERS),
        "cash_is_scored": True,
        "cash_yield_symbol": _contract.CASH_YIELD_SYMBOL,
        "benchmark_symbol": _contract.BENCHMARK_SYMBOL,
        "primary_control": _contract.ECONOMIC_CONTROL,
        "controls": list(_contract.CONTROLS),
        "leverage_available": _contract.LEVERAGE_AVAILABLE,
        "max_gross_exposure": _contract.MAX_GROSS_EXPOSURE,
        "constructions": list(CONSTRUCTIONS),
        "book_is_a_measurement_device": True,
        "excess_over_cash_may_rank": False,
        "excess_over_cash_rationale": (
            "over a long window anything holding risk beats bills, so excess "
            "over cash measures exposure rather than skill; it is reported and "
            "never ranked on"),
    }


def behaviour_hash() -> str:
    from .. import r33
    return r33.sha(judge_declaration())
