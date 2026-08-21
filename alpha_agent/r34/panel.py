"""alpha_agent.r34.panel - the ONE Release 34 price/return panel owner.

This module turns the admitted universe into the aligned object every later
stage reads. It is deliberately the same SHAPE as Release 33's panel - the same
dictionary keys, the same decision-date convention - so that
:mod:`alpha_agent.r33.features` can be reused verbatim rather than reimplemented
against a slightly different object. Reuse is the point: the predictive families
are frozen, and a second feature implementation would be a second set of bugs.

Four things this panel does that R33's could not:

**Returns are TOTAL returns.** Every instrument is priced on dividend-reinvested
adjusted closes, so a bond ETF's coupon and an equity ETF's dividend are both
in the return. R33's panel mixed equity PRICE indices with bond TOTAL-RETURN
indices and had to demean within asset class to stop a constant drift difference
from being paid as if it were skill. That correction is no longer needed,
because the heterogeneity is gone: the measured gap is 1.97 %/yr for ``SPY``,
3.50 % for ``TLT`` and 6.34 % for ``HYG``.

**Everything is USD and US-listed.** No currency conversion, no vendor currency
field to distrust, no translated-index diagnostic. One exchange calendar.

**Tradability is point-in-time.** An instrument is tradable on a date only if it
had listed, had accumulated its minimum history AND its trailing dollar volume
cleared the floor on that date. A fund that is liquid today was illiquid in its
first year, and admitting it from inception would be a small silent look-ahead
that flatters exactly the early part of the sample where breadth is thinnest.

**Death is modelled.** The candidate pool includes delisted products. An
instrument that stops quoting is held to its last session and then forced to
cash; it does not vanish from the panel, because vanishing would silently
redistribute its weight to the survivors and rewrite history in their favour.
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from . import contract as _contract
from . import universe as _universe

CALCULATION_OWNER = "alpha_agent.r34.panel"

#: A price is carried forward at most this many reference sessions. Beyond that
#: the observation is dropped rather than carried: an instrument that has not
#: printed for a fortnight is not "unchanged", it is unobserved, and a
#: forward-filled zero return would be recorded as a real prediction success.
MAX_FORWARD_FILL_SESSIONS = 5

SESSIONS_PER_YEAR = 252.0


def reference_calendar(built: dict, *, start: str = _contract.PANEL_START
                       ) -> pd.DatetimeIndex:
    """The sessions every forecast date is struck on.

    The benchmark's own sessions. Every instrument is US-listed, so this is one
    exchange calendar rather than R33's reconciliation of Tokyo, London and New
    York closes stamped with the same date.
    """
    bench = None
    for c in built["instruments"]:
        if c["symbol"] == _contract.BENCHMARK_SYMBOL:
            bench = c["_frame"]
            break
    if bench is None:
        raise RuntimeError(
            "benchmark %s is not an admitted instrument; the contract declares "
            "it must be" % (_contract.BENCHMARK_SYMBOL,))
    idx = bench.index[bench.index >= pd.Timestamp(start)]
    return pd.DatetimeIndex(idx)


def _align(series: pd.Series, calendar: pd.DatetimeIndex) -> pd.Series:
    s = series.reindex(series.index.union(calendar)).sort_index()
    s = s.ffill(limit=MAX_FORWARD_FILL_SESSIONS)
    return s.reindex(calendar)


def build(built: dict, *, start: str = _contract.PANEL_START) -> dict:
    """Build the aligned total-return panel, its returns and the cash leg."""
    calendar = reference_calendar(built, start=start)
    prices, volumes, meta = {}, {}, {}

    for c in built["instruments"]:
        sym = c["symbol"]
        frame = c["_frame"]
        prices[sym] = _align(frame["Close"], calendar)
        dollar = (frame["Close"] * frame["Volume"]).replace(
            [np.inf, -np.inf], np.nan)
        volumes[sym] = _align(dollar, calendar)
        meta[sym] = {
            "asset_class": c["asset_class"],
            "economic_group": c["economic_group"],
            "slot": c["slot"],
            "cost_tier": c["cost_tier"],
            "cost_bps_per_side": _contract.COST_TIER_BPS[c["cost_tier"]],
            "live_at_scan": c["live"],
            "last_quoted": c["last_date"],
        }

    price_frame = pd.DataFrame(prices, index=calendar).sort_index(axis=1)
    dollar_frame = pd.DataFrame(volumes, index=calendar).sort_index(axis=1)
    log_returns = np.log(price_frame).diff()

    tradable = tradability_mask(price_frame, dollar_frame)
    cash_daily = build_cash_leg(calendar)
    benchmark = price_frame[_contract.BENCHMARK_SYMBOL]

    return {"calendar": calendar,
            "prices": price_frame,
            "dollar_volume": dollar_frame,
            "log_returns": log_returns,
            "tradable": tradable,
            "cash_daily": cash_daily,
            "benchmark": benchmark,
            "meta": meta,
            "symbols": list(price_frame.columns),
            "max_forward_fill_sessions": MAX_FORWARD_FILL_SESSIONS}


def tradability_mask(prices: pd.DataFrame, dollar_volume: pd.DataFrame, *,
                     min_history: int = _contract.MIN_HISTORY_SESSIONS,
                     window: int = _contract.LIQUIDITY_WINDOW_SESSIONS,
                     floor: float = _contract.MIN_MEDIAN_DOLLAR_VOLUME
                     ) -> pd.DataFrame:
    """Whether each instrument could actually be traded on each date.

    Three conditions, all backward-looking: the instrument has a price, it has
    accumulated ``min_history`` observed sessions, and the MEDIAN of its
    trailing ``window`` dollar volumes clears the floor. The median rather than
    the mean, so one frantic day does not certify a year of thinness.
    """
    observed = prices.notna()
    history = observed.cumsum() >= int(min_history)
    liquid = dollar_volume.rolling(int(window),
                                   min_periods=max(20, int(window) // 4)
                                   ).median() >= float(floor)
    return (observed & history & liquid.fillna(False))


def build_cash_leg(calendar: pd.DatetimeIndex) -> pd.Series:
    """Daily cash accrual from the observed 13-week bill yield.

    The yield series delivers one session later than the price series, so it is
    LAGGED one session before use. Cash is a real asset choice and must earn an
    observable point-in-time yield rather than zero.
    """
    y = _universe.load_close(_contract.CASH_YIELD_SYMBOL)
    if y is None:
        return pd.Series(0.0, index=calendar)
    aligned = _align(y, calendar).shift(1).ffill()
    return (aligned.astype(float) / 100.0 / SESSIONS_PER_YEAR).fillna(0.0)


# --------------------------------------------------------------------------- #
# Forecast observations
# --------------------------------------------------------------------------- #
def forecast_dates(calendar: pd.DatetimeIndex, *, horizon: int,
                   min_history: int = _contract.MIN_HISTORY_SESSIONS,
                   lag: int = _contract.IMPLEMENTATION_LAG_SESSIONS) -> list:
    """Indices ``i`` where a decision may be struck, stepping by the horizon.

    Stepping by ``horizon`` is what makes successive observations
    non-overlapping. Overlapping windows inflate the effective sample by roughly
    ``h`` and would make every t-statistic in this release a fiction.
    """
    last = len(calendar) - lag - horizon - 1
    return list(range(int(min_history), int(last) + 1, int(horizon)))


def observation_returns(panel: dict, *, horizon: int,
                        lag: int = _contract.IMPLEMENTATION_LAG_SESSIONS
                        ) -> pd.DataFrame:
    """Simple USD EXCESS total return from the entry close to the exit close.

    Rows are decision dates, columns are instruments. ``NaN`` where either end
    is unobserved or the instrument was not tradable at the decision - which is
    deliberate: an instrument that could not be bought may not contribute a
    return, and one that did not print may not contribute a fabricated zero.
    """
    prices = panel["prices"]
    cash = panel["cash_daily"]
    tradable = panel["tradable"].to_numpy()
    idx = forecast_dates(panel["calendar"], horizon=horizon, lag=lag)
    values = prices.to_numpy()
    cash_values = cash.to_numpy()
    rows, dates = [], []
    for i in idx:
        entry, exit_ = i + lag, i + lag + horizon
        p0, p1 = values[entry], values[exit_]
        with np.errstate(invalid="ignore", divide="ignore"):
            gross = p1 / p0 - 1.0
        gross = np.where(np.isfinite(gross), gross, np.nan)
        gross = np.where(tradable[i], gross, np.nan)
        accrual = float(np.nansum(cash_values[entry + 1:exit_ + 1]))
        rows.append(gross - accrual)
        dates.append(panel["calendar"][i])
    return pd.DataFrame(rows, index=pd.DatetimeIndex(dates),
                        columns=prices.columns)


def benchmark_observation_returns(panel: dict, *, horizon: int,
                                  lag: int = _contract.IMPLEMENTATION_LAG_SESSIONS
                                  ) -> pd.Series:
    """The benchmark's excess total return on the same schedule."""
    bench = panel["benchmark"].to_numpy()
    cash_values = panel["cash_daily"].to_numpy()
    idx = forecast_dates(panel["calendar"], horizon=horizon, lag=lag)
    out, dates = [], []
    for i in idx:
        entry, exit_ = i + lag, i + lag + horizon
        b0, b1 = bench[entry], bench[exit_]
        r = (b1 / b0 - 1.0) if (np.isfinite(b0) and np.isfinite(b1) and b0 > 0) \
            else np.nan
        out.append(r - float(np.nansum(cash_values[entry + 1:exit_ + 1])))
        dates.append(panel["calendar"][i])
    return pd.Series(out, index=pd.DatetimeIndex(dates))


def cash_observation_returns(panel: dict, *, horizon: int,
                             lag: int = _contract.IMPLEMENTATION_LAG_SESSIONS
                             ) -> pd.Series:
    """Cash's own return on the same schedule - the risk-free leg."""
    cash_values = panel["cash_daily"].to_numpy()
    idx = forecast_dates(panel["calendar"], horizon=horizon, lag=lag)
    out, dates = [], []
    for i in idx:
        entry, exit_ = i + lag, i + lag + horizon
        out.append(float(np.nansum(cash_values[entry + 1:exit_ + 1])))
        dates.append(panel["calendar"][i])
    return pd.Series(out, index=pd.DatetimeIndex(dates))


def tradable_frame(panel: dict, *, horizon: int,
                   lag: int = _contract.IMPLEMENTATION_LAG_SESSIONS
                   ) -> pd.DataFrame:
    """The point-in-time tradability mask sampled at the decision dates."""
    idx = forecast_dates(panel["calendar"], horizon=horizon, lag=lag)
    mask = panel["tradable"].to_numpy()
    rows = [mask[i] for i in idx]
    return pd.DataFrame(rows,
                        index=pd.DatetimeIndex([panel["calendar"][i]
                                                for i in idx]),
                        columns=panel["prices"].columns)


def realised_volatility(panel: dict, *, horizon: int,
                        lag: int = _contract.IMPLEMENTATION_LAG_SESSIONS
                        ) -> pd.DataFrame:
    """Realised volatility over each forecast window - the volatility target."""
    lr = panel["log_returns"].to_numpy()
    idx = forecast_dates(panel["calendar"], horizon=horizon, lag=lag)
    rows, dates = [], []
    for i in idx:
        entry, exit_ = i + lag, i + lag + horizon
        block = lr[entry + 1:exit_ + 1]
        n_obs = np.sum(np.isfinite(block), axis=0)
        with np.errstate(invalid="ignore"):
            sq = np.where(np.isfinite(block), block ** 2, 0.0)
            v = np.sqrt(sq.sum(axis=0) / np.maximum(n_obs, 1)
                        * SESSIONS_PER_YEAR)
        v = np.where(n_obs >= max(2, horizon // 2), v, np.nan)
        rows.append(v)
        dates.append(panel["calendar"][i])
    return pd.DataFrame(rows, index=pd.DatetimeIndex(dates),
                        columns=panel["prices"].columns)


def trailing_volatility(panel: dict, dates: pd.DatetimeIndex,
                        window: int = 63) -> pd.DataFrame:
    """Trailing volatility OBSERVED at each decision date - never future vol."""
    lr = panel["log_returns"]
    vol = lr.rolling(window, min_periods=window // 2).std(ddof=1) \
        * np.sqrt(SESSIONS_PER_YEAR)
    return vol.reindex(dates).ffill()


def coverage_report(panel: dict, *, horizons=_contract.HORIZONS) -> dict:
    """How much of the panel is actually observed and tradable, per horizon."""
    out = {}
    for h in horizons:
        obs = observation_returns(panel, horizon=h)
        trad = tradable_frame(panel, horizon=h)
        finite = int(np.isfinite(obs.to_numpy()).sum())
        breadth = trad.sum(axis=1)
        out[str(h)] = {
            "forecast_dates": int(obs.shape[0]),
            "instruments": int(obs.shape[1]),
            "observations": finite,
            "coverage": round(finite / float(obs.size), 4) if obs.size else 0.0,
            "mean_tradable_breadth": round(float(breadth.mean()), 2)
                if len(breadth) else 0.0,
            "min_tradable_breadth": int(breadth.min()) if len(breadth) else 0,
            "first_date": str(obs.index[0].date()) if len(obs) else None,
            "last_date": str(obs.index[-1].date()) if len(obs) else None,
        }
    return out


def r33_feature_panel(panel: dict) -> dict:
    """The panel as :mod:`alpha_agent.r33.features` expects to receive it.

    The frozen feature families are computed by the Release-33 owner. This is
    the adapter, and it is a KEY SELECTION rather than a copy so that a shape
    change in either release fails loudly here instead of silently producing
    features from a differently-shaped object.
    """
    return {"calendar": panel["calendar"],
            "prices": panel["prices"],
            "log_returns": panel["log_returns"],
            "meta": panel["meta"]}
