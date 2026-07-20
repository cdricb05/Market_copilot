"""
api/price_panel.py — Phase 21 owned point-in-time trailing-price panel.

Builds a reusable, deterministic, point-in-time (PIT) trailing-price feature panel from the OWNED
local broad-universe daily price history (the Phase 7-I ``phase7i_broad_price_history_free.csv``
artifact: yfinance-sourced daily adjusted OHLCV + the SPY benchmark close + the daily return, for
~301 current S&P-scale names over 2016-06 .. 2026-06). It is the missing price ingredient that lets
the Phase 20 Alpha Factory activate its five trailing-price families (momentum / trend / volatility
/ relative-strength / mean-reversion) without fabricating data.

Design guarantees (why this is safe to backtest with):
    * OWNED DATA ONLY. Reads one local CSV. No network, no live provider, no prediction service.
    * POINT-IN-TIME. Every feature at an observation date T uses ONLY bars with date <= T; every
      forward return uses ONLY bars strictly after T. The two never overlap — there is no
      look-ahead by construction (see ``compute_features`` / ``forward_returns``).
    * DETERMINISTIC + STDLIB. Pure Python (bisect / math), no numpy / pandas / new packages, no
      randomness. Same inputs -> byte-identical outputs.
    * READ-ONLY. This module writes nothing anywhere.

Documented caveats (surfaced in the manifest so downstream research stays honest):
    * Survivorship: the owned file covers names that survived to the download date; delisted names
      are absent, so momentum / low-vol backtests are modestly optimistic.
    * Adjustment: yfinance ``adjusted_close`` is retro-adjusted for splits+dividends as-of download.
      Trailing-return RATIOS are PIT-valid; absolute adjusted levels are as-of-today. Benign for the
      ratio / rank features used here.
    * Universe overlap: only the intersection with the fundamental (10-L) universe is scored, so
      price factors cover ~55% of the fundamental universe (documented coverage, never hidden).
    * Window: history starts 2016-06, so the pre-2020 subperiod is ~2016-07..2019-12.

Public API:
    load_price_panel(path) -> dict | None      # per-ticker aligned arrays + SPY + manifest
    compute_features(series, j) -> dict         # PIT trailing features at bar index j
    forward_returns(series, j, horizons) -> dict# strictly-future returns at each horizon
    FEATURE_KEYS / HORIZONS                      # the stable feature / horizon vocabulary
"""
from __future__ import annotations

import bisect
import csv
import math
import os
from pathlib import Path
from typing import Any, Optional, Union

# --- env seam + default owned source -------------------------------------------------------
PRICE_ENV = "PAPER_TRADER_PRICE_PANEL"
DEFAULT_PRICE = Path(
    r"D:\Stock_Prediction_app_data\phase7i_broad_universe\prices"
    r"\phase7i_broad_price_history_free.csv"
)
SOURCE_NAME = "owned phase7i broad universe daily price history (yfinance, local)"
BENCHMARK_TICKER = "SPY"

# --- forward horizons (trading days) -------------------------------------------------------
HORIZONS = [5, 10, 21, 63]

# --- CSV columns ---------------------------------------------------------------------------
C_DATE = "date"
C_TICKER = "ticker"
C_ADJ = "adjusted_close"
C_BENCH = "benchmark_close"
C_VOL = "volume"

# --- stable feature vocabulary (every key this module can emit) ----------------------------
FEATURE_KEYS = [
    # momentum (trailing simple total returns, higher = stronger)
    "ret_1", "ret_5", "ret_10", "ret_21", "ret_63", "ret_126", "ret_252",
    "mom_12_1", "mom_accel", "mom_blend",
    # trend (price vs moving averages)
    "px_vs_ma10", "px_vs_ma20", "px_vs_ma63", "px_vs_ma126", "ma20_vs_ma63",
    "trend_persist_63", "trend_quality_126",
    # volatility / risk
    "rvol_63", "rvol_126", "dvol_126", "maxdd_252", "voladj_mom_63", "ddadj_trend_126",
    # relative strength vs SPY
    "bench_ret_63", "bench_ret_126", "bench_ret_252", "rs_63", "rs_126", "rs_252", "rs_blend",
    # mean reversion
    "resid_rev_21", "beta_63",
]

_TRADING_DAYS_YEAR = 252.0


# --------------------------------------------------------------------------- #
# Small numeric helpers (stdlib only)
# --------------------------------------------------------------------------- #
def _to_float(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _mean(xs: list[float]) -> Optional[float]:
    return (sum(xs) / len(xs)) if xs else None


def _std(xs: list[float], ddof: int = 1) -> Optional[float]:
    n = len(xs)
    if n - ddof <= 0:
        return None
    m = sum(xs) / n
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - ddof))


# --------------------------------------------------------------------------- #
# Panel load
# --------------------------------------------------------------------------- #
def _resolve(path: Optional[Union[str, Path]]) -> Path:
    if path is not None:
        return Path(path)
    env = os.environ.get(PRICE_ENV)
    return Path(env) if env else DEFAULT_PRICE


def load_price_panel(path: Optional[Union[str, Path]] = None) -> Optional[dict]:
    """Load the owned daily price CSV into per-ticker ascending-sorted arrays.

    Returns a dict ``{"series": {ticker: {dates, adj, bench, ret}}, "manifest": {...}}`` or None if
    the file is unreadable / empty. ``ret[t]`` is the 1-day simple return adj[t]/adj[t-1]-1 (ret[0]
    is None). ``bench[t]`` is the SPY close aligned to that ticker's bar t (from ``benchmark_close``).
    Rows with a non-positive / missing adjusted close are dropped. Dates are ISO strings that sort
    chronologically, so bisection on them is a valid PIT "as-of" lookup.
    """
    p = _resolve(path)
    try:
        with open(p, "r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            raw: dict[str, list[tuple[str, float, Optional[float]]]] = {}
            for r in reader:
                tk = (r.get(C_TICKER) or "").strip().upper()
                d = (r.get(C_DATE) or "").strip()
                adj = _to_float(r.get(C_ADJ))
                if not tk or len(d) < 10 or adj is None or adj <= 0:
                    continue
                bench = _to_float(r.get(C_BENCH))
                raw.setdefault(tk, []).append((d, adj, bench))
    except OSError:
        return None
    if not raw:
        return None

    series: dict[str, dict] = {}
    all_dates: set = set()
    min_d: Optional[str] = None
    max_d: Optional[str] = None
    for tk, rows in raw.items():
        rows.sort(key=lambda x: x[0])
        # de-duplicate dates (keep the last bar for a date), preserving order
        dates: list[str] = []
        adj: list[float] = []
        bench: list[Optional[float]] = []
        for d, a, b in rows:
            if dates and dates[-1] == d:
                adj[-1] = a
                bench[-1] = b
            else:
                dates.append(d)
                adj.append(a)
                bench.append(b)
        ret: list[Optional[float]] = [None]
        bret: list[Optional[float]] = [None]
        for t in range(1, len(adj)):
            prev = adj[t - 1]
            ret.append((adj[t] / prev - 1.0) if prev > 0 else None)
            pb, cb = bench[t - 1], bench[t]
            bret.append((cb / pb - 1.0) if (pb not in (None, 0) and cb is not None) else None)
        series[tk] = {"dates": dates, "adj": adj, "bench": bench, "ret": ret, "bret": bret}
        if dates:
            all_dates.update((dates[0], dates[-1]))
            if min_d is None or dates[0] < min_d:
                min_d = dates[0]
            if max_d is None or dates[-1] > max_d:
                max_d = dates[-1]

    n_bars = {tk: len(s["dates"]) for tk, s in series.items()}
    manifest = {
        "source": SOURCE_NAME,
        "path": str(p),
        "adjustment": "split+dividend adjusted close (retro-adjusted as-of download); ratios PIT-valid",
        "benchmark": BENCHMARK_TICKER,
        "n_tickers": len(series),
        "date_start": min_d,
        "date_end": max_d,
        "median_bars_per_ticker": sorted(n_bars.values())[len(n_bars) // 2] if n_bars else 0,
        "duplicate_dates_handling": "collapsed to the last bar for a repeated (ticker, date)",
        "missing_price_handling": "rows with non-positive / missing adjusted_close dropped",
        "survivorship_caveat": ("current-membership names only; delisted names absent -> momentum / "
                                "low-vol backtests modestly optimistic"),
        "pit_guarantee": ("features at date T use only bars with date<=T; forward returns use only "
                          "bars strictly after T"),
        "grid_support": "monthly (primary, aligned to the fundamental rebalance grid) and weekly",
        "horizons_trading_days": list(HORIZONS),
    }
    return {"series": series, "manifest": manifest}


# --------------------------------------------------------------------------- #
# As-of index resolution (PIT)
# --------------------------------------------------------------------------- #
def asof_index(dates: list[str], as_of: str) -> int:
    """Return the index of the latest bar with date <= ``as_of`` (or -1 if none). PIT-safe."""
    return bisect.bisect_right(dates, as_of) - 1


# --------------------------------------------------------------------------- #
# Trailing feature primitives (all read only adj[..j] / bench[..j] — never past j)
# --------------------------------------------------------------------------- #
def _tr(adj: list[float], j: int, k: int) -> Optional[float]:
    """Trailing simple return over k trading days ending at j: adj[j]/adj[j-k]-1."""
    if j - k < 0:
        return None
    base = adj[j - k]
    return (adj[j] / base - 1.0) if base > 0 else None


def _ma(adj: list[float], j: int, k: int) -> Optional[float]:
    if j - k + 1 < 0:
        return None
    seg = adj[j - k + 1: j + 1]
    return _mean(seg)


def _rvol(ret: list[Optional[float]], j: int, k: int) -> Optional[float]:
    """Realized volatility = sample std of the last k daily returns ending at j (annualized)."""
    if j - k + 1 < 1:
        return None
    seg = [r for r in ret[j - k + 1: j + 1] if r is not None]
    if len(seg) < k // 2:
        return None
    s = _std(seg, 1)
    return (s * math.sqrt(_TRADING_DAYS_YEAR)) if s is not None else None


def _dvol(ret: list[Optional[float]], j: int, k: int) -> Optional[float]:
    """Downside deviation about zero over the last k daily returns (annualized)."""
    if j - k + 1 < 1:
        return None
    seg = [r for r in ret[j - k + 1: j + 1] if r is not None]
    if len(seg) < k // 2:
        return None
    downs = [min(0.0, r) ** 2 for r in seg]
    return math.sqrt(sum(downs) / len(downs)) * math.sqrt(_TRADING_DAYS_YEAR)


def _maxdd(adj: list[float], j: int, k: int) -> Optional[float]:
    """Max drawdown (<=0) of the adjusted close over the last k+1 bars ending at j."""
    if j - k < 0:
        return None
    seg = adj[j - k: j + 1]
    peak = seg[0]
    mdd = 0.0
    for v in seg:
        if v > peak:
            peak = v
        if peak > 0:
            mdd = min(mdd, v / peak - 1.0)
    return mdd


def _persist(ret: list[Optional[float]], j: int, k: int) -> Optional[float]:
    """Trend persistence = fraction of up-days over the last k daily returns (0.5-centered)."""
    if j - k + 1 < 1:
        return None
    seg = [r for r in ret[j - k + 1: j + 1] if r is not None]
    if len(seg) < k // 2:
        return None
    up = sum(1 for r in seg if r > 0)
    return up / len(seg) - 0.5


def _beta(ret: list[Optional[float]], bret: list[Optional[float]], j: int, k: int) -> Optional[float]:
    """Trailing OLS beta of the stock vs SPY over the last k daily returns ending at j."""
    if j - k + 1 < 1:
        return None
    xs, ys = [], []
    for t in range(j - k + 1, j + 1):
        r, b = ret[t], bret[t]
        if r is not None and b is not None:
            xs.append(b)
            ys.append(r)
    if len(xs) < k // 2:
        return None
    mb = sum(xs) / len(xs)
    mr = sum(ys) / len(ys)
    var = sum((x - mb) ** 2 for x in xs)
    if var <= 0:
        return None
    cov = sum((xs[i] - mb) * (ys[i] - mr) for i in range(len(xs)))
    return cov / var


def _resid_rev(ret, bret, adj, bench, j, k, beta) -> Optional[float]:
    """Market-residual short reversal: negative of the cumulative beta-adjusted excess return over
    the last k days. Losers-vs-market are expected to rebound, so the sign is inverted."""
    stock_k = _tr(adj, j, k)
    if stock_k is None or beta is None:
        return None
    if j - k < 0 or bench[j] is None or bench[j - k] is None or bench[j - k] == 0:
        return None
    spy_k = bench[j] / bench[j - k] - 1.0
    return -(stock_k - beta * spy_k)


# --------------------------------------------------------------------------- #
# Feature bundle at one observation (bar index j) — all PIT
# --------------------------------------------------------------------------- #
def compute_features(series: dict, j: int) -> dict[str, Optional[float]]:
    """Compute the full trailing-feature bundle at bar index ``j`` using only bars with index <= j."""
    adj = series["adj"]
    bench = series["bench"]
    ret = series["ret"]
    f: dict[str, Optional[float]] = {}

    for k, key in ((1, "ret_1"), (5, "ret_5"), (10, "ret_10"), (21, "ret_21"),
                   (63, "ret_63"), (126, "ret_126"), (252, "ret_252")):
        f[key] = _tr(adj, j, k)

    # 12-1 momentum: cumulative return from t-252 to t-21 (skip the most recent month)
    if j - 252 >= 0 and adj[j - 252] > 0:
        f["mom_12_1"] = adj[j - 21] / adj[j - 252] - 1.0
    else:
        f["mom_12_1"] = None
    # momentum acceleration: recent 63d return minus the prior 63d return
    if j - 126 >= 0 and adj[j - 63] > 0 and adj[j - 126] > 0:
        f["mom_accel"] = (adj[j] / adj[j - 63] - 1.0) - (adj[j - 63] / adj[j - 126] - 1.0)
    else:
        f["mom_accel"] = None
    blend = [v for v in (f["ret_63"], f["ret_126"], f["ret_252"]) if v is not None]
    f["mom_blend"] = (sum(blend) / len(blend)) if len(blend) == 3 else None

    for k, key in ((10, "ma10"), (20, "ma20"), (63, "ma63"), (126, "ma126")):
        ma = _ma(adj, j, k)
        f["px_vs_" + key] = (adj[j] / ma - 1.0) if (ma and ma > 0) else None
    ma20 = _ma(adj, j, 20)
    ma63 = _ma(adj, j, 63)
    f["ma20_vs_ma63"] = (ma20 / ma63 - 1.0) if (ma20 and ma63 and ma63 > 0) else None
    f["trend_persist_63"] = _persist(ret, j, 63)

    f["rvol_63"] = _rvol(ret, j, 63)
    f["rvol_126"] = _rvol(ret, j, 126)
    f["dvol_126"] = _dvol(ret, j, 126)
    f["maxdd_252"] = _maxdd(adj, j, 252)
    f["trend_quality_126"] = ((f["ret_126"] / f["rvol_126"])
                              if (f["ret_126"] is not None and f["rvol_126"]) else None)
    f["voladj_mom_63"] = ((f["ret_63"] / f["rvol_63"])
                          if (f["ret_63"] is not None and f["rvol_63"]) else None)
    f["ddadj_trend_126"] = ((f["ret_126"] / abs(f["maxdd_252"]))
                            if (f["ret_126"] is not None and f["maxdd_252"] not in (None, 0)) else None)

    for k, key in ((63, "bench_ret_63"), (126, "bench_ret_126"), (252, "bench_ret_252")):
        if j - k >= 0 and bench[j] is not None and bench[j - k] not in (None, 0):
            f[key] = bench[j] / bench[j - k] - 1.0
        else:
            f[key] = None
    f["rs_63"] = ((f["ret_63"] - f["bench_ret_63"])
                  if (f["ret_63"] is not None and f["bench_ret_63"] is not None) else None)
    f["rs_126"] = ((f["ret_126"] - f["bench_ret_126"])
                   if (f["ret_126"] is not None and f["bench_ret_126"] is not None) else None)
    f["rs_252"] = ((f["ret_252"] - f["bench_ret_252"])
                   if (f["ret_252"] is not None and f["bench_ret_252"] is not None) else None)
    rsb = [v for v in (f["rs_63"], f["rs_126"], f["rs_252"]) if v is not None]
    f["rs_blend"] = (sum(rsb) / len(rsb)) if len(rsb) == 3 else None

    beta = _beta(ret, series["bret"], j, 63)
    f["beta_63"] = beta
    f["resid_rev_21"] = _resid_rev(ret, bench, adj, bench, j, 21, beta)
    return f


def forward_returns(series: dict, j: int, horizons: Optional[list[int]] = None) -> dict[int, Optional[float]]:
    """Strictly-future forward returns at each horizon h: adj[j+h]/adj[j]-1, using only bars > j.

    Returns None for a horizon that would run off the end of the series (the forward-return boundary).
    """
    adj = series["adj"]
    n = len(adj)
    out: dict[int, Optional[float]] = {}
    for h in (horizons or HORIZONS):
        k = j + h
        out[h] = (adj[k] / adj[j] - 1.0) if (k < n and adj[j] > 0) else None
    return out


__all__ = [
    "PRICE_ENV", "DEFAULT_PRICE", "SOURCE_NAME", "BENCHMARK_TICKER", "HORIZONS", "FEATURE_KEYS",
    "load_price_panel", "asof_index", "compute_features", "forward_returns",
]
