"""alpha_agent.r46.marketdata - ONE read seam over what the estate owns LIVE.

The tournament needs a data path that is actually refreshing, because a
prospective tournament reading a static snapshot is a very expensive way to
produce nothing. Release 42 documented exactly that failure (a monthly archive
behind a daily shadow) and it was never acted on.

What refreshes here, verified in-run rather than assumed:

* **Norgate Data Updater**, served locally, nine databases - US equities
  (with delisted history and point-in-time index membership), continuous
  futures, dated futures, forex spot, US and world indices, cash commodities
  and economic series. Entitled since Release 37/38.
* **FRED**, free, owned key, for the risk-free rate that decides whether a
  collateralised book actually beat cash.

This module OPENS data. It acquires nothing, installs nothing, writes into no
provider store and spends nothing. Every loader is cached in-process and hands
back a naive daily ``DatetimeIndex``, because mixing tz-aware and naive
indices across five asset classes is how alignment bugs become "alpha".
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
import warnings
from functools import lru_cache
from typing import Optional

import numpy as np
import pandas as pd

CALCULATION_OWNER = "alpha_agent.r46.marketdata"

NORGATE_START = "2015-01-01"

#: Databases whose freshness the feasibility gate checks.
WATCHED_DATABASES = ("US Equities", "Continuous Futures", "Forex Spot",
                     "US Indices", "World Indices")

_UNAVAILABLE = {"state": "NOT_CONFIGURED"}


def _nd():
    """Import the vendor client without letting the CALLER'S warning filters
    decide whether owned data is readable.

    ``norgatedata`` calls the deprecated ``logging.warn`` at import time. Under
    a strict filter - pytest configures warnings-as-errors here - that
    DeprecationWarning is raised as an exception, the import fails, and every
    loader in this module returns ``None``. The failure is completely silent:
    the feasibility gate reports NO_DATA and a suite would conclude the estate
    owns nothing, which is exactly the kind of invisible degradation this
    release exists to stop. The warning belongs to a third-party package, we
    cannot fix it, and it must not be load-bearing.
    """
    logging.disable(logging.WARNING)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        import norgatedata as nd
    return nd


def available() -> bool:
    try:
        _nd()
        return True
    except Exception:
        return False


# --------------------------------------------------------------------------- #
# Provider state
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=1)
def provider_state() -> dict:
    """Observed provider state - never claimed, always probed."""
    try:
        nd = _nd()
    except Exception as exc:
        return {"state": "NOT_CONFIGURED", "error": repr(exc)[:200],
                "databases": [], "last_update": {}}
    out = {"state": "OK", "databases": [], "last_update": {}}
    try:
        out["databases"] = list(nd.databases())
    except Exception as exc:
        out["state"] = "DEGRADED"
        out["databases_error"] = repr(exc)[:200]
    for db in out["databases"]:
        try:
            out["last_update"][db] = str(nd.last_database_update_time(db))
        except Exception as exc:
            out["last_update"][db] = "ERR:" + type(exc).__name__
    try:
        out["status"] = bool(nd.status())
    except Exception:
        out["status"] = None
    return out


# --------------------------------------------------------------------------- #
# Bars
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=4096)
def bars(symbol: str, start: str = NORGATE_START):
    """Adjusted daily OHLCV for one symbol, naive daily index, or ``None``.

    Total-return adjustment is applied for equities (the survivorship-safe
    convention Release 34 established); futures and FX carry none.
    """
    try:
        nd = _nd()
    except Exception:
        return None
    kw = {"start_date": start, "format": "pandas-dataframe",
          "padding_setting": nd.PaddingType.NONE}
    if not symbol.startswith(("&", "$", "%", "#")):
        try:
            kw["stock_price_adjustment_setting"] = \
                nd.StockPriceAdjustmentType.TOTALRETURN
        except Exception:
            pass
    try:
        df = nd.price_timeseries(symbol, **kw)
    except Exception:
        return None
    if df is None or not len(df):
        return None
    df = df.copy()
    df.index = pd.DatetimeIndex(df.index).tz_localize(None).normalize()
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df


def closes(symbol: str, start: str = NORGATE_START):
    df = bars(symbol, start)
    if df is None or "Close" not in df.columns:
        return None
    s = pd.to_numeric(df["Close"], errors="coerce").dropna()
    return s if len(s) else None


def sessions(symbol: str, start: str = NORGATE_START) -> list:
    """The instrument's OWN realised bar dates - the eligible-session calendar."""
    s = closes(symbol, start)
    if s is None:
        return []
    return [d.date() for d in s.index]


def last_session(symbol: str) -> Optional[_dt.date]:
    ss = sessions(symbol)
    return ss[-1] if ss else None


# --------------------------------------------------------------------------- #
# Universes
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=8)
def _watchlist(name: str) -> tuple:
    try:
        nd = _nd()
        return tuple(nd.watchlist_symbols(name))
    except Exception:
        return ()


@lru_cache(maxsize=64)
def sp500_pit(as_of: str) -> tuple:
    """S&P 500 constituents AS OF ``as_of`` - survivorship-safe.

    Built from the ``Current & Past`` watchlist (1897 symbols including
    delisted and renamed history) filtered by Norgate's point-in-time index
    membership series. A name that had left the index by ``as_of`` is not in
    the result, and a name that had not yet joined is not either.
    """
    try:
        nd = _nd()
    except Exception:
        return ()
    universe = _watchlist("S&P 500 Current & Past")
    if not universe:
        return ()
    day = pd.Timestamp(as_of).normalize()
    out = []
    for sym in universe:
        try:
            ts = nd.index_constituent_timeseries(
                sym, "S&P 500", start_date=str(day.date()),
                end_date=str(day.date()), padding_setting=nd.PaddingType.NONE,
                format="pandas-dataframe")
        except Exception:
            continue
        if ts is None or not len(ts):
            continue
        col = ts.columns[-1]
        try:
            if float(ts[col].iloc[-1]) > 0:
                out.append(sym)
        except Exception:
            continue
    return tuple(sorted(out))


@lru_cache(maxsize=1)
def continuous_futures() -> tuple:
    """Distinct continuous-futures markets, excluding the ``_CCB`` variants."""
    try:
        nd = _nd()
        syms = [s for s in nd.database_symbols("Continuous Futures")
                if not s.endswith("_CCB")]
        return tuple(sorted(syms))
    except Exception:
        return ()


@lru_cache(maxsize=1)
def fx_spot_symbols() -> tuple:
    try:
        nd = _nd()
        return tuple(sorted(nd.database_symbols("Forex Spot")))
    except Exception:
        return ()


# --------------------------------------------------------------------------- #
# Risk-free rate - the control that decides whether cash was beaten
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=1)
def risk_free_annual() -> dict:
    """Latest 3-month T-bill yield, annual decimal, with provenance.

    Free FRED series through the owned key. NO silent constant fallback: if
    the series cannot be read, the miss is reported and the judge records
    ``rf_source = UNAVAILABLE`` on the affected rows rather than inventing a
    number that decides the control.
    """
    key = (os.environ.get("FRED_API_KEY")
           or os.environ.get("PAPER_TRADER_FRED_API_KEY"))
    if not key:
        return {"state": "NO_KEY", "annual": None, "as_of": None,
                "source": "FRED " + "DGS3MO"}
    url = ("https://api.stlouisfed.org/fred/series/observations"
           "?series_id=DGS3MO&api_key=" + key + "&file_type=json"
           "&sort_order=desc&limit=10")
    try:
        import json as _json
        import urllib.request
        with urllib.request.urlopen(url, timeout=20) as fh:
            payload = _json.loads(fh.read().decode("utf-8"))
        for obs in payload.get("observations", []):
            try:
                v = float(obs["value"])
            except (KeyError, TypeError, ValueError):
                continue
            return {"state": "OK", "annual": v / 100.0,
                    "as_of": obs.get("date"),
                    "source": "FRED DGS3MO (free, owned key)"}
        return {"state": "NO_OBSERVATION", "annual": None, "as_of": None,
                "source": "FRED DGS3MO"}
    except Exception as exc:
        return {"state": "FETCH_FAILED", "annual": None, "as_of": None,
                "error": type(exc).__name__, "source": "FRED DGS3MO"}


def risk_free_per_session(horizon_sessions: int) -> Optional[float]:
    """Risk-free accrual over ``horizon_sessions``, as a decimal return."""
    rf = risk_free_annual()
    if rf.get("annual") is None:
        return None
    return float(rf["annual"]) * (float(horizon_sessions) / 252.0)


# --------------------------------------------------------------------------- #
# Small analytics used by more than one challenger
# --------------------------------------------------------------------------- #
#: A percentage return is undefined across a non-positive price, and one
#: market in the declared futures set really does carry one: continuous WTI
#: prints -37.63 on 2020-04-20, the day the front contract settled negative.
#: That is a true historical fact, not a data defect, and it must be REFUSED
#: rather than quietly turned into a NaN that propagates into a rank.
NON_POSITIVE_PRICE = "NON_POSITIVE_PRICE_IN_WINDOW"


def has_non_positive(series: pd.Series, window: int = None) -> bool:
    if series is None or not len(series):
        return True
    w = series.iloc[-int(window):] if window else series
    return bool((w <= 0).any()) or bool((~np.isfinite(w)).any())


def _log_returns(series: pd.Series, window: int) -> Optional[pd.Series]:
    """Log returns over the last ``window`` steps, or ``None`` if undefined."""
    if series is None or len(series) < window + 1:
        return None
    w = series.iloc[-(window + 1):]
    if has_non_positive(w):
        return None
    return np.log(w).diff().dropna()


def total_return(series: pd.Series, lookback: int, skip: int = 0):
    """Return from ``t-lookback`` to ``t-skip``. ``None`` when undefined."""
    if series is None or len(series) < lookback + 1:
        return None
    end = series.iloc[-1 - skip] if skip else series.iloc[-1]
    start = series.iloc[-1 - lookback]
    if not np.isfinite(start) or not np.isfinite(end):
        return None
    if start <= 0 or end <= 0:
        return None
    return float(end / start - 1.0)


def realised_vol(series: pd.Series, window: int) -> Optional[float]:
    r = _log_returns(series, window)
    if r is None or len(r) < window // 2:
        return None
    v = float(r.std(ddof=1) * np.sqrt(252.0))
    return v if np.isfinite(v) and v > 0 else None


def beta_to(series: pd.Series, market: pd.Series,
            window: int) -> Optional[float]:
    if series is None or market is None:
        return None
    if has_non_positive(series, window + 1) or \
            has_non_positive(market, window + 1):
        return None
    a = np.log(series).diff().dropna()
    b = np.log(market).diff().dropna()
    j = a.align(b, join="inner")
    a, b = j[0].iloc[-window:], j[1].iloc[-window:]
    if len(a) < window // 2 or float(b.var(ddof=1)) <= 0:
        return None
    v = float(np.cov(a, b, ddof=1)[0, 1] / b.var(ddof=1))
    return v if np.isfinite(v) else None


def zscore_last(series: pd.Series, window: int) -> Optional[float]:
    if series is None or len(series) < window:
        return None
    w = series.iloc[-window:]
    sd = float(w.std(ddof=1))
    if not np.isfinite(sd) or sd <= 0:
        return None
    v = float((series.iloc[-1] - w.mean()) / sd)
    return v if np.isfinite(v) else None


def reset_cache() -> None:
    for fn in (provider_state, bars, sp500_pit, continuous_futures,
               fx_spot_symbols, risk_free_annual, _watchlist):
        try:
            fn.cache_clear()
        except AttributeError:
            pass
