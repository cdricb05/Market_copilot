"""
alpha_agent.price_factors - Stage 9.4 Track A: deterministic, point-in-time-safe
price-derived factor calculators for the pre-registered catalogue.

Every factor value at formation index ``t`` is computed from OWNED, survivorship-
safe Norgate closes through ``t`` ONLY; the forward-return window is strictly
after ``t`` (no look-ahead). Factors that need data absent from the owned close
panel (volume/turnover, intraday/overnight opens, point-in-time GICS, calendar
event dates) are NOT computed here - they are pre-registered in
``price_factor_catalogue`` and resolve to an honest DATA_HOLD, never approximated.

These are genuinely NEW factor formulas (trend quality, breakout/compression,
market-residual & beta-neutral momentum, volatility dynamics, return-distribution
tails, market sensitivity) - none duplicates the six already-scored campaign
features in ``experiment_runner`` (residual_momentum, momentum_accel,
vol_scaled_momentum, short_term_reversal, low_volatility, combo_mom_lowvol).

Pure stdlib, byte-reproducible. Reuses the shared numeric primitives in
``experiment_contracts`` and the metric battery in ``experiment_runner`` rather
than reimplementing rank IC / decile spread / cost math.
"""
from __future__ import annotations

import math
from typing import Optional

from . import experiment_contracts as ec
from . import experiment_runner as er

_MAX_LOOKBACK = 252
_STEP_DAYS = {"daily": 1, "weekly": 5, "monthly": 21, "quarterly": 63}
# Pre-registered (fixed BEFORE any candidate is evaluated) cross-sectional
# regime-feature definitions: the trailing window over which each eligible name's
# formation-date return is measured, and the minimum cross-sectional name count
# below which breadth / dispersion are UNKNOWN rather than guessed.
_REGIME_TRAILING_LOOKBACK = 63
_REGIME_MIN_NAMES = 20

# The NEW computable price features (each formula is distinct from the existing
# campaign features). ``expected_sign`` documents the pre-registered direction.
COMPUTABLE_FEATURES: dict[str, dict] = {
    "trend_slope_t": {"family": "TREND_QUALITY", "lookback": 126, "sign": 1},
    "path_efficiency": {"family": "TREND_QUALITY", "lookback": 126, "sign": 1},
    "drawdown_adjusted_trend": {"family": "TREND_QUALITY", "lookback": 252,
                                "sign": 1},
    "distance_from_52w_high": {"family": "BREAKOUT_COMPRESSION",
                               "lookback": 252, "sign": 1},
    "channel_breakout": {"family": "BREAKOUT_COMPRESSION", "lookback": 63,
                         "sign": 1},
    "breakout_post_compression": {"family": "BREAKOUT_COMPRESSION",
                                  "lookback": 63, "sign": 1},
    "market_residual_momentum": {"family": "RESIDUAL_RELATIVE_MOM",
                                 "lookback": 252, "sign": 1},
    "beta_neutral_momentum": {"family": "RESIDUAL_RELATIVE_MOM",
                              "lookback": 252, "sign": 1},
    "idiosyncratic_volatility": {"family": "VOLATILITY_DYNAMICS",
                                 "lookback": 126, "sign": 1},
    "short_long_vol_ratio": {"family": "VOLATILITY_DYNAMICS", "lookback": 126,
                             "sign": 1},
    "vol_of_vol": {"family": "VOLATILITY_DYNAMICS", "lookback": 126, "sign": 1},
    "max_daily_return": {"family": "RETURN_DISTRIBUTION", "lookback": 21,
                         "sign": 1},
    "realized_skewness": {"family": "RETURN_DISTRIBUTION", "lookback": 126,
                          "sign": 1},
    "low_beta": {"family": "MARKET_SENSITIVITY", "lookback": 126, "sign": 1},
    "downside_beta": {"family": "MARKET_SENSITIVITY", "lookback": 126,
                      "sign": 1},
    "defensive_residual_return": {"family": "MARKET_SENSITIVITY",
                                  "lookback": 126, "sign": 1},
}


# --------------------------------------------------------------------------- #
# Small numeric helpers (leakage-safe: callers pass slices through t only).
# --------------------------------------------------------------------------- #
def _log_returns(closes: list[float]) -> list[float]:
    out = []
    for i in range(1, len(closes)):
        a, b = closes[i], closes[i - 1]
        if a and b and a > 0 and b > 0:
            out.append(math.log(a / b))
    return out


def _simple_returns(closes: list[float]) -> list[float]:
    out = []
    for i in range(1, len(closes)):
        a, b = closes[i], closes[i - 1]
        if a is not None and b and b > 0:
            out.append(a / b - 1.0)
    return out


def _ols_slope_t(y: list[float]) -> tuple[Optional[float], Optional[float]]:
    """Slope and slope t-stat of ``y`` on a 0..n-1 time index."""
    n = len(y)
    if n < 4:
        return None, None
    xs = list(range(n))
    mx = sum(xs) / n
    my = sum(y) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx <= 0:
        return None, None
    sxy = sum((xs[i] - mx) * (y[i] - my) for i in range(n))
    slope = sxy / sxx
    resid = [y[i] - (my + slope * (xs[i] - mx)) for i in range(n)]
    sse = sum(r * r for r in resid)
    if n <= 2:
        return slope, None
    se = math.sqrt((sse / (n - 2)) / sxx) if sse > 0 and sxx > 0 else None
    t = (slope / se) if se and se > 0 else None
    return slope, t


def _beta(name_r: list[float], mkt_r: list[float]) -> tuple[Optional[float],
                                                            Optional[float]]:
    """Market beta and intercept (alpha per period) via OLS of name on market."""
    pairs = [(n, m) for n, m in zip(name_r, mkt_r)
             if ec._is_num(n) and ec._is_num(m)]
    if len(pairs) < 4:
        return None, None
    nx = [p[1] for p in pairs]  # market
    ny = [p[0] for p in pairs]  # name
    mm = sum(nx) / len(nx)
    mn = sum(ny) / len(ny)
    vm = sum((x - mm) ** 2 for x in nx)
    if vm <= 0:
        return None, None
    cov = sum((nx[i] - mm) * (ny[i] - mn) for i in range(len(nx)))
    beta = cov / vm
    alpha = mn - beta * mm
    return beta, alpha


def _skew(vals: list[float]) -> Optional[float]:
    v = ec._finite(vals)
    n = len(v)
    if n < 3:
        return None
    m = sum(v) / n
    sd = ec.stdev(v, sample=True)
    if not sd:
        return None
    return (sum((x - m) ** 3 for x in v) / n) / (sd ** 3)


# --------------------------------------------------------------------------- #
# Per-name factor value (all inputs are through the formation date only).
# --------------------------------------------------------------------------- #
def factor_value(feature: str, *, name_dates: list, name_closes: list,
                 idx: int, mret_by_date: dict) -> Optional[float]:
    """One name's pre-registered factor at formation ``idx`` using only
    ``name_closes[:idx+1]`` and market returns dated on-or-before the formation
    date. Returns None when the required lookback is unavailable."""
    spec = COMPUTABLE_FEATURES.get(feature)
    if spec is None or idx < 1:
        return None
    L = int(spec["lookback"])
    lo = max(0, idx - L)
    win = name_closes[lo:idx + 1]
    if len(win) < max(20, L // 3):
        return None
    c_now = name_closes[idx]
    if not c_now or c_now <= 0:
        return None

    if feature == "trend_slope_t":
        ln = [math.log(c) for c in win if c and c > 0]
        _, t = _ols_slope_t(ln)
        return t
    if feature == "path_efficiency":
        net = win[-1] - win[0]
        gross = sum(abs(win[i] - win[i - 1]) for i in range(1, len(win)))
        return (net / gross) if gross > 0 else None
    if feature == "drawdown_adjusted_trend":
        rets = _simple_returns(win)
        total = ec.cumulative_return(rets)
        mdd = ec.max_drawdown(rets)
        if total is None or mdd is None:
            return None
        return total / (1.0 + abs(mdd))
    if feature == "distance_from_52w_high":
        hi = max(win)
        return (c_now / hi - 1.0) if hi > 0 else None
    if feature == "channel_breakout":
        prev = win[:-1]
        if not prev:
            return None
        hi = max(prev)
        return (c_now - hi) / c_now if c_now > 0 else None
    if feature == "breakout_post_compression":
        prev = win[:-1]
        if not prev:
            return None
        hi = max(prev)
        breakout = (c_now - hi) / c_now
        rets = _simple_returns(win)
        v_short = ec.stdev(rets[-21:]) if len(rets) >= 21 else None
        v_long = ec.stdev(rets) if len(rets) >= 20 else None
        if not v_short or not v_long or v_long <= 0:
            return None
        compression = v_long / v_short  # >1 when recently compressed
        return breakout * compression
    if feature in ("market_residual_momentum", "beta_neutral_momentum",
                   "defensive_residual_return", "low_beta", "downside_beta",
                   "idiosyncratic_volatility"):
        return _market_relative(feature, name_dates, name_closes, idx, L,
                                mret_by_date)
    if feature == "short_long_vol_ratio":
        rets = _simple_returns(win)
        v_short = ec.stdev(rets[-21:]) if len(rets) >= 21 else None
        v_long = ec.stdev(rets) if len(rets) >= 20 else None
        if not v_short or not v_long or v_short <= 0:
            return None
        return -(v_short / v_long)  # low recent-relative vol is favourable
    if feature == "vol_of_vol":
        rets = _simple_returns(win)
        rolling = [ec.stdev(rets[i:i + 21]) for i in range(0, len(rets) - 21, 5)]
        rolling = [r for r in rolling if r is not None]
        vv = ec.stdev(rolling) if len(rolling) >= 3 else None
        return None if vv is None else -vv
    if feature == "max_daily_return":
        rets = _simple_returns(win[-22:])
        return -max(rets) if rets else None
    if feature == "realized_skewness":
        rets = _simple_returns(win)
        sk = _skew(rets)
        return None if sk is None else -sk
    return None


def _market_relative(feature, name_dates, name_closes, idx, L, mret_by_date):
    """Market-relative factors needing the aligned market return series."""
    lo = max(1, idx - L)
    nr, mr = [], []
    for j in range(lo, idx + 1):
        c, cp = name_closes[j], name_closes[j - 1]
        if not c or not cp or cp <= 0:
            continue
        d = name_dates[j]
        m = mret_by_date.get(d)
        if m is None:
            continue
        nr.append(c / cp - 1.0)
        mr.append(m)
    if len(nr) < 20:
        return None
    beta, alpha = _beta(nr, mr)
    if beta is None:
        return None
    resid = [nr[i] - (alpha + beta * mr[i]) for i in range(len(nr))]
    if feature == "low_beta":
        return -beta
    if feature == "idiosyncratic_volatility":
        iv = ec.stdev(resid)
        return None if iv is None else -iv
    if feature == "downside_beta":
        dn = [(nr[i], mr[i]) for i in range(len(mr)) if mr[i] < 0]
        if len(dn) < 8:
            return None
        db, _ = _beta([p[0] for p in dn], [p[1] for p in dn])
        return None if db is None else -db
    if feature == "defensive_residual_return":
        return ec.cumulative_return(resid)
    if feature in ("market_residual_momentum", "beta_neutral_momentum"):
        # 12-1 style: exclude the most recent month of residual/name returns.
        if len(resid) <= 21:
            return None
        if feature == "market_residual_momentum":
            return ec.cumulative_return(resid[:-21])
        # beta-neutral momentum: name momentum minus beta * market momentum.
        name_mom = ec.cumulative_return(nr[:-21])
        mkt_mom = ec.cumulative_return(mr[:-21])
        if name_mom is None or mkt_mom is None:
            return None
        return name_mom - beta * mkt_mom
    return None


# --------------------------------------------------------------------------- #
# Cross-section builder (also emits per-period market features for regimes).
# --------------------------------------------------------------------------- #
def build_market_return_series(panel: dict) -> dict:
    """Equal-weight market daily return by date across the owned panel (used for
    residualization and for the regime market features). PIT-safe: each date's
    market return uses only that day's cross-section."""
    by_date: dict[str, list] = {}
    for _t, series in panel.items():
        s = sorted(set(series), key=lambda p: p[0])
        for i in range(1, len(s)):
            (d0, c0), (d1, c1) = s[i - 1], s[i]
            if c0 and c0 > 0 and c1 is not None:
                by_date.setdefault(d1, []).append(c1 / c0 - 1.0)
    return {d: (sum(v) / len(v)) for d, v in by_date.items() if v}


def build_factor_cross_sections(panel: dict, *, feature: str,
                                horizon_days: int, rebalance: str,
                                min_names_for_regime: int = _REGIME_MIN_NAMES,
                                value_fn=None) -> dict:
    """Deterministic per-rebalance ``(factor_vals, fwd_returns)`` cross-sections
    for a new price feature, plus the long-only top-decile book, turnover, the
    equal-weight benchmark, and per-period market features for regime
    conditioning. Leakage-safe: factor uses data through the formation date; the
    forward return is strictly after it. The regime market features (breadth /
    dispersion) are cross-sectional over the eligible-name universe at formation
    and are candidate-INDEPENDENT (identical for every ``feature``)."""
    step = _STEP_DAYS.get(rebalance, 21)
    mret = build_market_return_series(panel)
    all_dates = sorted({d for series in panel.values() for d, _ in series})
    mret_list = [mret.get(d) for d in all_dates]
    tk_close = {t: [c for _, c in sorted(set(s), key=lambda p: p[0])]
                for t, s in panel.items()}
    tk_dates = {t: [d for d, _ in sorted(set(s), key=lambda p: p[0])]
                for t, s in panel.items()}
    tk_index = {t: {d: i for i, d in enumerate(tk_dates[t])} for t in panel}

    cross_sections: list[tuple] = []
    portfolio_returns: list[float] = []
    turnovers: list[float] = []
    benchmark_returns: list[float] = []
    market_features: list[dict] = []
    cross_section_names: list[list] = []   # Stage 14 additive: aligned names
    formation_dates: list[str] = []        # Stage 14 additive: aligned dates
    prev_book: dict[str, float] = {}
    observations = 0

    for fi in range(_MAX_LOOKBACK, len(all_dates), step):
        fd = all_dates[fi]
        fac_vals, fwd_vals, names = [], [], []
        # Candidate-INDEPENDENT cross-sectional trailing returns of every eligible
        # name at the formation date (used only for the regime breadth/dispersion
        # features - NEVER the candidate's own factor values). Computed through
        # the formation date only (PIT-safe).
        name_trailing_returns: list[float] = []
        for t, closes in tk_close.items():
            idx = tk_index[t].get(fd)
            if idx is None:
                continue
            if idx >= _REGIME_TRAILING_LOOKBACK:
                b0 = closes[idx - _REGIME_TRAILING_LOOKBACK]
                b1 = closes[idx]
                if b0 and b0 > 0 and b1 is not None:
                    name_trailing_returns.append(b1 / b0 - 1.0)
            fidx = idx + horizon_days
            if fidx >= len(closes):
                continue
            base = closes[idx]
            if base is None or base <= 0:
                continue
            if value_fn is not None:
                # Stage 14 additive hook: an externally-frozen calculator (e.g.
                # an overnight factor needing owned opens) computes the value
                # under the SAME leakage contract (inputs through ``idx`` only);
                # ``name_key`` lets it join per-name auxiliary data. Default
                # behaviour (value_fn=None) is byte-identical to before.
                v = value_fn(feature, name_key=t, name_dates=tk_dates[t],
                             name_closes=closes, idx=idx, mret_by_date=mret)
            else:
                v = factor_value(feature, name_dates=tk_dates[t],
                                 name_closes=closes, idx=idx, mret_by_date=mret)
            if v is None or not ec._is_num(v):
                continue
            fac_vals.append(float(v))
            fwd_vals.append(closes[fidx] / base - 1.0)
            names.append(t)
        if len(fac_vals) < 4:
            continue
        cross_sections.append((fac_vals, fwd_vals))
        cross_section_names.append(list(names))
        formation_dates.append(fd)
        observations += len(fac_vals)
        benchmark_returns.append(sum(fwd_vals) / len(fwd_vals))
        order = sorted(range(len(fac_vals)), key=lambda i: fac_vals[i])
        size = max(1, len(order) // 10)
        top = order[-size:]
        book = {names[i]: 1.0 / size for i in top}
        portfolio_returns.append(sum(fwd_vals[i] for i in top) / size)
        turnovers.append(ec.turnover(prev_book, book))
        prev_book = book
        market_features.append(_market_features_at(
            fi, all_dates, mret_list, name_trailing_returns,
            min_names=min_names_for_regime))

    universe = (sorted(len(cs[0]) for cs in cross_sections)[
        len(cross_sections) // 2] if cross_sections else 0)
    return {"cross_sections": cross_sections,
            "portfolio_returns": portfolio_returns, "turnovers": turnovers,
            "benchmark_returns": benchmark_returns, "observations": observations,
            "periods": len(cross_sections), "universe": universe,
            "market_features": market_features,
            "cross_section_names": cross_section_names,
            "formation_dates": formation_dates}


def _market_features_at(fi: int, all_dates: list, mret_list: list,
                        name_trailing_returns: list, *,
                        min_names: int = _REGIME_MIN_NAMES) -> dict:
    """PIT-safe market + cross-sectional regime features at formation index ``fi``
    (all inputs observed through ``fi`` only).

    ``breadth`` is true cross-sectional participation - the fraction of eligible
    NAMES whose trailing formation-date return is positive - and ``dispersion``
    is the cross-sectional standard deviation of those same eligible-name trailing
    returns. Both are computed from the candidate-independent name cross-section
    (never the candidate's factor values) and are UNKNOWN (``None``) when fewer
    than ``min_names`` names are available. ``market_up_day_fraction`` is the
    RENAMED time-series positive-day count (kept as an honest diagnostic, no
    longer mislabelled as breadth)."""
    lo = max(0, fi - 63)
    hist = [mret_list[j] for j in range(lo, fi + 1) if ec._is_num(mret_list[j])]
    market_return = ec.cumulative_return(hist) if hist else None
    market_vol = ec.stdev(hist) if len(hist) >= 20 else None
    up_days = sum(1 for v in hist if v > 0)
    market_up_day_fraction = (up_days / len(hist)) if hist else None
    # Cross-sectional participation / dispersion at formation (candidate-free).
    ntr = [r for r in name_trailing_returns if ec._is_num(r)]
    if len(ntr) >= max(3, int(min_names)):
        breadth = sum(1 for r in ntr if r > 0) / len(ntr)
        dispersion = ec.stdev(ntr)
    else:
        breadth = None        # UNKNOWN - insufficient cross-sectional coverage
        dispersion = None
    return {"market_return": market_return, "market_vol": market_vol,
            "breadth": breadth, "dispersion": dispersion,
            "breadth_name_count": len(ntr),
            "market_up_day_fraction": market_up_day_fraction}


# --------------------------------------------------------------------------- #
# Bounded new-factor campaign -> Stage 5 metric rows (merge-compatible with the
# existing owned-price campaign result so the tournament tick scores them with
# no change to its evaluation logic).
# --------------------------------------------------------------------------- #
def run_new_price_factor_campaign(panel: dict, *, features=None,
                                  horizon_days: int = 21,
                                  rebalance: str = "monthly",
                                  cost_grid=None, cost_bps: float = 10.0,
                                  champion_returns=None,
                                  min_periods: int = 12,
                                  value_fn=None) -> dict:
    """Run the bounded, pre-registered NEW price factors deterministically and
    return per-feature Stage 5 metric rows (same shape as
    ``experiment_runner.run_price_factor_campaign`` results) plus honest
    NEED_MORE_DATA rows when history is short. Never forces a positive result."""
    feats = list(features or COMPUTABLE_FEATURES.keys())
    cost_grid = cost_grid or [5, 10, 25, 50]
    results: list[dict] = []
    for feature in feats:
        built = build_factor_cross_sections(
            panel, feature=feature, horizon_days=horizon_days,
            rebalance=rebalance, value_fn=value_fn)
        if built["periods"] < min_periods:
            results.append({"feature": feature, "label": feature,
                            "periods": built["periods"], "rank_ic_t": None,
                            "decision": ec.NEED_MORE_DATA})
            continue
        spec = {"template": "campaign_price_factor", "feature": feature,
                "study_kind": "cross_sectional_rank", "horizon_days": horizon_days,
                "rebalance": rebalance, "benchmark": "equal_weight_universe",
                "transaction_cost_bps": cost_bps}
        ppy = ec.periods_per_year_for(rebalance)
        metrics = er.score_experiment(spec, built, gates=ec.DEFAULT_GATES,
                                      cost_grid=cost_grid, periods_per_year=ppy,
                                      champion_returns=champion_returns,
                                      missing_data_rate=0.0)
        metrics["feature"] = feature
        metrics["label"] = feature
        metrics["market_features"] = built["market_features"]
        # Per-period aligned series (length == periods) for the Track C/D
        # regime-conditioned + bootstrap + purged-fold diagnostics. Transient:
        # never persisted into the candidate contract metrics.
        metrics["_periods_series"] = {
            "rank_ic": [ec.spearman(f, r) for f, r in built["cross_sections"]],
            "spread": [ec._decile_spread_one(f, r, 10)
                       for f, r in built["cross_sections"]],
            "portfolio_returns": built["portfolio_returns"],
            "turnovers": built["turnovers"],
            "market_features": built["market_features"]}
        metrics["decision"] = ec.evaluate_evidence(
            metrics, ec.DEFAULT_GATES)["decision"]
        results.append(metrics)
    return {"results": results, "features": feats,
            "horizon_days": horizon_days, "rebalance": rebalance}


__all__ = ["COMPUTABLE_FEATURES", "factor_value", "build_market_return_series",
           "build_factor_cross_sections", "run_new_price_factor_campaign"]
