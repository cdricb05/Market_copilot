"""alpha_agent.r33.panel - the ONE Release 33 price/return panel owner.

This module turns the admitted universe into the aligned object every later
stage reads. Three decisions here determine whether the whole campaign measures
something real.

**Mixed calendars.** Tokyo, London and New York stamp different sessions with
the same date, and every market has its own holidays. Prices are aligned onto
ONE reference calendar - the sessions the benchmark trades - and carried forward
for at most ``MAX_FORWARD_FILL_SESSIONS``. Beyond that the observation is
dropped rather than carried: a market that has not printed for a fortnight is
not "unchanged", it is unobserved, and a forward-filled zero return would be
recorded as a real prediction success.

**Non-synchronous trading.** A signal built from "day t closes" across markets
can otherwise trade on moves that happened after some of those markets had
already closed. Nobody can buy the Nikkei at yesterday's Tokyo close. So the
decision uses information through the close of session ``i``, the position is
ENTERED at the close of session ``i+1``, and the return is measured from
``i+1`` to ``i+1+h``. The lost session is the cost of not fooling ourselves.

**Overlap.** Successive forecast dates step by the full horizon ``h``, so no two
observations of an ``h``-session return share a single day. Overlapping windows
inflate the effective sample by roughly ``h`` and would make every t-statistic
in this release a fiction.

Returns are USD-converted using the vendor's currency field and the owned Forex
Spot series, because the paper book is USD-denominated and an unhedged foreign
holding earns the currency move too. Excess returns subtract the observed
13-week bill accrual over the SAME window, so cash is a real asset choice
rather than a free zero.
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from . import contract as _contract
from . import universe as _universe

CALCULATION_OWNER = "alpha_agent.r33.panel"

#: A price is carried forward at most this many reference sessions.
MAX_FORWARD_FILL_SESSIONS = 5

#: Trading sessions per year, for annualisation.
SESSIONS_PER_YEAR = 252.0


def reference_calendar(*, start: str, benchmark: str = _contract.BENCHMARK_SYMBOL
                       ) -> pd.DatetimeIndex:
    """The sessions every forecast date is struck on."""
    bench = _universe.load_close(benchmark)
    if bench is None:
        raise RuntimeError(f"benchmark {benchmark} was not delivered")
    idx = bench.index[bench.index >= pd.Timestamp(start)]
    return pd.DatetimeIndex(idx)


def _align(series: pd.Series, calendar: pd.DatetimeIndex) -> pd.Series:
    """Reindex onto the calendar, carrying forward within the stale limit."""
    s = series.reindex(series.index.union(calendar)).sort_index()
    s = s.ffill(limit=MAX_FORWARD_FILL_SESSIONS)
    return s.reindex(calendar)


def build(built_universe: dict, *, start: str = _contract.PANEL_START) -> dict:
    """Build the aligned USD price panel, its returns and the cash leg."""
    calendar = reference_calendar(start=start)
    markets = built_universe["markets"]
    fx_series = built_universe["fx_series"]

    prices, meta, conversion = {}, {}, {}
    for m in markets:
        sym, cur = m["symbol"], m["currency"]
        px = _align(m["close"], calendar)
        if m["asset_class"] != _universe.AC_FX and cur != "USD":
            fx = fx_series.get(cur)
            if fx is None:
                conversion[sym] = "NO_FX_SERIES_LEFT_IN_LOCAL_CURRENCY"
            else:
                px = px * _align(fx, calendar)
                conversion[sym] = f"USD_CONVERTED_VIA_{cur}USD"
        else:
            conversion[sym] = "ALREADY_USD"
        prices[sym] = px
        meta[sym] = {"asset_class": m["asset_class"],
                     "economic_group": m["economic_group"],
                     "currency": cur,
                     "usd_conversion": conversion[sym]}

    price_frame = pd.DataFrame(prices, index=calendar).sort_index(axis=1)
    log_returns = np.log(price_frame).diff()

    cash_daily = build_cash_leg(calendar)
    benchmark = _align(_universe.load_close(_contract.BENCHMARK_SYMBOL),
                       calendar)

    return {"calendar": calendar,
            "prices": price_frame,
            "log_returns": log_returns,
            "cash_daily": cash_daily,
            "benchmark": benchmark,
            "meta": meta,
            "symbols": list(price_frame.columns),
            "max_forward_fill_sessions": MAX_FORWARD_FILL_SESSIONS}


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
    non-overlapping. The last usable index leaves room for the implementation
    lag AND the full holding period.
    """
    last = len(calendar) - lag - horizon - 1
    return list(range(int(min_history), int(last) + 1, int(horizon)))


def observation_returns(panel: dict, *, horizon: int,
                        lag: int = _contract.IMPLEMENTATION_LAG_SESSIONS
                        ) -> pd.DataFrame:
    """Simple USD excess return from the entry close to the exit close.

    Rows are decision indices, columns are markets. ``NaN`` where either the
    entry or the exit price is unobserved, which is deliberate: a market that
    did not print may not contribute a fabricated zero.
    """
    prices = panel["prices"]
    cash = panel["cash_daily"]
    idx = forecast_dates(panel["calendar"], horizon=horizon, lag=lag)
    rows, dates = [], []
    values = prices.to_numpy()
    cash_values = cash.to_numpy()
    for i in idx:
        entry, exit_ = i + lag, i + lag + horizon
        p0, p1 = values[entry], values[exit_]
        with np.errstate(invalid="ignore", divide="ignore"):
            gross = p1 / p0 - 1.0
        gross = np.where(np.isfinite(gross), gross, np.nan)
        cash_accrual = float(np.nansum(cash_values[entry + 1:exit_ + 1]))
        rows.append(gross - cash_accrual)
        dates.append(panel["calendar"][i])
    return pd.DataFrame(rows, index=pd.DatetimeIndex(dates),
                        columns=prices.columns)


def benchmark_observation_returns(panel: dict, *, horizon: int,
                                  lag: int = _contract.IMPLEMENTATION_LAG_SESSIONS
                                  ) -> pd.Series:
    """The benchmark's excess return on the same non-overlapping schedule."""
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


def coverage_report(panel: dict, *, horizons=_contract.HORIZONS) -> dict:
    """How much of the panel is actually observed, per horizon."""
    out = {}
    for h in horizons:
        obs = observation_returns(panel, horizon=h)
        finite = int(np.isfinite(obs.to_numpy()).sum())
        out[str(h)] = {
            "forecast_dates": int(obs.shape[0]),
            "markets": int(obs.shape[1]),
            "observations": finite,
            "coverage": round(finite / float(obs.size), 4) if obs.size else 0.0,
            "first_date": str(obs.index[0].date()) if len(obs) else None,
            "last_date": str(obs.index[-1].date()) if len(obs) else None,
        }
    return out
