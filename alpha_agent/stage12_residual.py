"""Stage 12 Workstream E -- residual / relative price reaction features.

Price features that strip out broad exposures so what remains is stock-specific:
market-residual momentum, industry-residual and industry-relative momentum,
short-term reversal conditioned on an extreme abnormal move, idiosyncratic
volatility, and downside beta. Removing the market/industry component is exactly
what the Stage 11 raw price momentum lacked (best price momentum there had
rank_ic_t 1.99, below the 2.0 gate, and carried market-beta contamination).

All features use only bars on/before the formation date (no look-ahead) and are
sector-neutralised through the leakage-safe PIT SIC sector (``sector_as_of``),
never current GICS. Output is the same ``{as_of, names:[(key, adj_signal,
forward_ret)]}`` format the unchanged evaluator scores.
"""
from __future__ import annotations

import bisect
import math
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

try:
    from . import fundamental_evidence as _fev
    from . import signal_library as _sl
    from . import stage12_execution as _exec
except Exception:  # pragma: no cover
    import fundamental_evidence as _fev  # type: ignore
    import signal_library as _sl  # type: ignore
    import stage12_execution as _exec  # type: ignore


# --------------------------------------------------------------------------- #
# Price accessors (strictly on/before the formation date).
# --------------------------------------------------------------------------- #
def _pos_asof(dates: Sequence[str], as_of: str) -> int:
    """Index of the last date <= as_of (or -1)."""
    i = bisect.bisect_right(dates, as_of) - 1
    return i


def _return_between(dates, closes, as_of, start_off, end_off) -> Optional[float]:
    """Return from close ``start_off`` trading days before as_of to close
    ``end_off`` trading days before as_of (end_off < start_off; end_off may be 0).
    """
    i = _pos_asof(dates, as_of)
    if i < 0:
        return None
    a = i - int(start_off)
    b = i - int(end_off)
    if a < 0 or b < 0 or a >= len(closes) or b >= len(closes):
        return None
    if closes[a] <= 0:
        return None
    return closes[b] / closes[a] - 1.0


def _daily_returns_trailing(dates, closes, as_of, window) -> List[float]:
    i = _pos_asof(dates, as_of)
    if i < 1:
        return []
    lo = max(1, i - int(window) + 1)
    rets = []
    for k in range(lo, i + 1):
        if closes[k - 1] > 0:
            rets.append(closes[k] / closes[k - 1] - 1.0)
    return rets


def _std(xs: Sequence[float]) -> Optional[float]:
    n = len(xs)
    if n < 3:
        return None
    m = sum(xs) / n
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))


def _mean(xs: Sequence[float]) -> Optional[float]:
    return (sum(xs) / len(xs)) if xs else None


# --------------------------------------------------------------------------- #
# Market daily-return series (equal weight) built once per call.
# --------------------------------------------------------------------------- #
def _market_daily_map(close_index: Mapping[str, Tuple[list, list]]) -> Dict[str, float]:
    """Equal-weight cross-sectional mean daily return by date."""
    agg: Dict[str, List[float]] = {}
    for _key, idx in close_index.items():
        dates, closes = idx[0], idx[1]
        for k in range(1, len(dates)):
            if closes[k - 1] > 0:
                agg.setdefault(dates[k], []).append(closes[k] / closes[k - 1] - 1.0)
    return {d: (sum(v) / len(v)) for d, v in agg.items() if v}


def _beta_downside(name_rets: List[float], mkt_rets: List[float], *, downside: bool
                   ) -> Optional[float]:
    pairs = [(n, m) for n, m in zip(name_rets, mkt_rets)
             if (m < 0 if downside else True)]
    if len(pairs) < 6:
        return None
    ms = [m for _, m in pairs]
    ns = [n for n, _ in pairs]
    mm = sum(ms) / len(ms)
    varm = sum((m - mm) ** 2 for m in ms)
    if varm <= 0:
        return None
    nm = sum(ns) / len(ns)
    cov = sum((n - nm) * (m - mm) for n, m in pairs)
    return cov / varm


# --------------------------------------------------------------------------- #
# Feature builders. Each returns raw {assetid: value} for a formation date.
# --------------------------------------------------------------------------- #
_MOM_START, _MOM_END = 126, 21   # 6-1 momentum window (trading days)
_SHORT_WINDOW = 21               # 1-month
_VOL_WINDOW = 126


def _raw_momentum(ctx, as_of) -> Dict[str, float]:
    ci = ctx["close_index"]
    out = {}
    for key, idx in ci.items():
        r = _return_between(idx[0], idx[1], as_of, _MOM_START, _MOM_END)
        if r is not None:
            out[key] = r
    return out


def _assetid_to_cik(ctx) -> Dict[Any, Any]:
    return {v: k for k, v in (ctx.get("cik_to_assetid") or {}).items()}


def _sector_of(ctx, a2c, assetid, as_of) -> Optional[str]:
    ss = ctx.get("sector_series")
    if ss is None:
        return None
    cik = a2c.get(assetid)
    if cik is None:
        return None
    try:
        return ss.sector_as_of(cik, as_of)
    except Exception:
        return None


def _demean(raw: Dict[str, float]) -> Dict[str, float]:
    if not raw:
        return {}
    m = sum(raw.values()) / len(raw)
    return {k: v - m for k, v in raw.items()}


def _demean_by_group(raw: Dict[str, float], groups: Dict[str, Optional[str]]
                     ) -> Dict[str, float]:
    sums: Dict[Any, List[float]] = {}
    for k, v in raw.items():
        g = groups.get(k)
        if g is None:
            continue
        sums.setdefault(g, []).append(v)
    means = {g: (sum(v) / len(v)) for g, v in sums.items() if v}
    out = {}
    for k, v in raw.items():
        g = groups.get(k)
        if g is None or g not in means:
            continue
        out[k] = v - means[g]
    return out


def _b_market_momentum(ctx, as_of, **kw) -> Dict[str, float]:
    return _demean(_raw_momentum(ctx, as_of))


def _b_industry_momentum(ctx, as_of, *, a2c, **kw) -> Dict[str, float]:
    raw = _raw_momentum(ctx, as_of)
    groups = {k: _sector_of(ctx, a2c, k, as_of) for k in raw}
    return _demean_by_group(raw, groups)


def _b_industry_relative(ctx, as_of, *, a2c, **kw) -> Dict[str, float]:
    ci = ctx["close_index"]
    raw = {}
    for key, idx in ci.items():
        r = _return_between(idx[0], idx[1], as_of, _SHORT_WINDOW, 0)
        if r is not None:
            raw[key] = r
    groups = {k: _sector_of(ctx, a2c, k, as_of) for k in raw}
    return _demean_by_group(raw, groups)


def _b_short_term_reversal(ctx, as_of, **kw) -> Dict[str, float]:
    ci = ctx["close_index"]
    raw = {}
    for key, idx in ci.items():
        r = _return_between(idx[0], idx[1], as_of, _SHORT_WINDOW, 0)
        if r is not None:
            raw[key] = r
    resid = _demean(raw)  # abnormal (market-residual) 1m return
    if not resid:
        return {}
    mags = sorted(abs(v) for v in resid.values())
    if len(mags) < 10:
        return {}
    cutoff = mags[int(0.9 * (len(mags) - 1))]  # top-decile magnitude
    # reversal signal only on extreme names; others contribute 0 (neutral).
    return {k: (-v if abs(v) >= cutoff else 0.0) for k, v in resid.items()}


def _b_idiosyncratic_vol(ctx, as_of, *, mkt, **kw) -> Dict[str, float]:
    ci = ctx["close_index"]
    out = {}
    for key, idx in ci.items():
        dates, closes = idx[0], idx[1]
        i = _pos_asof(dates, as_of)
        if i < _VOL_WINDOW:
            continue
        rets = _daily_returns_trailing(dates, closes, as_of, _VOL_WINDOW)
        if len(rets) < 30:
            continue
        resid = []
        for k in range(i - len(rets) + 1, i + 1):
            m = mkt.get(dates[k])
            if m is None:
                continue
            resid.append((closes[k] / closes[k - 1] - 1.0) - m if closes[k - 1] > 0 else None)
        resid = [r for r in resid if r is not None]
        s = _std(resid)
        if s is not None:
            out[key] = -s  # low idiosyncratic vol favoured
    return out


def _b_downside_beta(ctx, as_of, *, mkt, **kw) -> Dict[str, float]:
    ci = ctx["close_index"]
    out = {}
    for key, idx in ci.items():
        dates, closes = idx[0], idx[1]
        i = _pos_asof(dates, as_of)
        if i < _VOL_WINDOW:
            continue
        name_r, mkt_r = [], []
        for k in range(max(1, i - _VOL_WINDOW + 1), i + 1):
            m = mkt.get(dates[k])
            if m is None or closes[k - 1] <= 0:
                continue
            name_r.append(closes[k] / closes[k - 1] - 1.0)
            mkt_r.append(m)
        b = _beta_downside(name_r, mkt_r, downside=True)
        if b is not None:
            out[key] = -b  # low downside beta favoured
    return out


RESIDUAL_BUILDERS: Dict[str, Callable] = {
    "residual_market_momentum": _b_market_momentum,
    "residual_industry_momentum": _b_industry_momentum,
    "residual_industry_relative": _b_industry_relative,
    "residual_short_term_reversal": _b_short_term_reversal,
    "risk_idiosyncratic_vol": _b_idiosyncratic_vol,
    "risk_downside_beta": _b_downside_beta,
}
_NEEDS_MARKET = {"risk_idiosyncratic_vol", "risk_downside_beta"}
_NEEDS_SECTOR = {"residual_industry_momentum", "residual_industry_relative"}


def build_residual_periods(builder_key: str, ctx: Mapping[str, Any],
                           rebalance_dates: Sequence[str], *, direction: int = 1,
                           horizon_days: int = 63, winsor: float = 0.02,
                           min_names_for_period: int = 3,
                           lag: int = _exec.DEFAULT_LAG) -> Dict[str, Any]:
    """Build PIT-safe residual price cross-sections for one builder.

    Forward returns enter at ``formation + lag`` (lag >= 1) via
    ``stage12_execution.forward_return_lagged`` -- the entry close is STRICTLY
    after the formation close, so a signal that reads the formation close (e.g.
    short-term reversal / industry-relative) can never be traded at that close
    (BLOCKER 3).
    """
    fn = RESIDUAL_BUILDERS.get(builder_key)
    pit = {"no_lookahead": True, "execution_lag": max(1, int(lag)),
           "entry": "formation_index + lag (strictly after formation close)",
           "sector_source": "leakage-safe PIT SIC (sector_as_of)" if builder_key in _NEEDS_SECTOR else None,
           "market_removed": builder_key in (_NEEDS_MARKET | {"residual_market_momentum", "residual_short_term_reversal"})}
    if fn is None:
        return {"periods": [], "coverage": {}, "pit": pit,
                "data_hold_reason": "UNKNOWN_RESIDUAL_BUILDER:%s" % builder_key}
    close_index = ctx.get("close_index") or {}
    if not close_index:
        return {"periods": [], "coverage": {}, "pit": pit,
                "data_hold_reason": "DATA_HOLD_NO_PRICE_PANEL"}
    if builder_key in _NEEDS_SECTOR and ctx.get("sector_series") is None:
        return {"periods": [], "coverage": {}, "pit": pit,
                "data_hold_reason": "DATA_HOLD_NO_PIT_SECTOR"}

    sign = int(direction) or 1
    a2c = _assetid_to_cik(ctx) if builder_key in _NEEDS_SECTOR else {}
    mkt = _market_daily_map(close_index) if builder_key in _NEEDS_MARKET else {}
    periods: List[Dict[str, Any]] = []
    names_counts: List[int] = []
    for d in rebalance_dates:
        raw = fn(ctx, d, a2c=a2c, mkt=mkt)
        raw = {k: v for k, v in raw.items() if v is not None}
        if len(raw) < min_names_for_period:
            continue
        raw = _sl.winsorize(raw, winsor)
        names = []
        for key, val in raw.items():
            idx = close_index.get(key)
            if idx is None:
                continue
            fwd = _exec.forward_return_lagged(idx[0], idx[1], d, horizon_days,
                                              lag=lag)
            if fwd is None:
                continue
            names.append((key, float(val) * sign, float(fwd)))
        if len(names) >= min_names_for_period:
            periods.append({"as_of": d, "names": names})
            names_counts.append(len(names))

    coverage = {
        "n_periods": len(periods),
        "median_names": (sorted(names_counts)[len(names_counts) // 2]
                         if names_counts else 0),
        "min_names": min(names_counts) if names_counts else 0,
        "max_names": max(names_counts) if names_counts else 0,
        "rebalance_dates_requested": len(rebalance_dates),
    }
    data_hold_reason = None
    if coverage["n_periods"] < 12:
        data_hold_reason = "DATA_HOLD_INSUFFICIENT_PERIODS(%d<12)" % coverage["n_periods"]
    elif coverage["median_names"] < 20:
        data_hold_reason = "DATA_HOLD_INSUFFICIENT_UNIVERSE(median=%d<20)" % coverage["median_names"]
    return {"periods": periods, "coverage": coverage, "pit": pit,
            "data_hold_reason": data_hold_reason}


def is_residual_builder(builder_key: str) -> bool:
    return builder_key in RESIDUAL_BUILDERS
