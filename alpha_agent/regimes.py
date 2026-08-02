"""
alpha_agent.regimes - Stage 9.4 Track C: deterministic, point-in-time-safe
market-regime definitions and regime-conditioned evaluation.

The regime axes are FIXED here, before any candidate is evaluated, and are a pure
function of OWNED market features observed through the formation date only - never
of a candidate's forward performance. A regime label for period ``t`` uses only
data available at ``t`` (expanding-window medians over prior/current periods, or
static thresholds), so it cannot leak the forward return it will later condition.

No regime may be invented after examining a candidate: ``REGIME_AXES`` and
``regime_version_hash()`` pin the definition set. Regime-conditioned metrics carry
an explicit sample size for every bucket and refuse to assert a conditional-alpha
claim below the configured minimum observation count - the guard against calling
a tiny data-mined bucket "regime-specific alpha".

Pure stdlib, byte-reproducible, no clock, no randomness, no state writes.
"""
from __future__ import annotations

import hashlib
from typing import Optional, Sequence

from . import experiment_contracts as ec

# --------------------------------------------------------------------------- #
# FIXED regime definition set (pinned before evaluation).
# --------------------------------------------------------------------------- #
REGIME_VERSION = "stage9_4-regimes-1.0.0"

# Each axis: (axis_key, state_a, state_b, source_feature). A composite regime is
# the tuple of per-axis states. Axes are a closed set - never extended at runtime.
REGIME_AXES = (
    ("market_trend", "TREND_UP", "TREND_DOWN", "market_return"),
    ("market_volatility", "VOL_HIGH", "VOL_LOW", "market_vol"),
    ("participation", "BREADTH_BROAD", "BREADTH_NARROW", "breadth"),
    ("dispersion", "DISPERSION_HIGH", "DISPERSION_LOW", "dispersion"),
    ("risk_state", "RISK_ON", "RISK_OFF", "_composite"),
    ("rates", "RATES_RISING", "RATES_FALLING", "rate_change"),
)

REGIME_AXIS_KEYS = tuple(a[0] for a in REGIME_AXES)

# Stability verdicts (Track C).
BROADLY_ROBUST = "BROADLY_ROBUST"
REGIME_SPECIFIC = "LEGITIMATELY_REGIME_SPECIFIC"
UNSTABLE_DATA_MINED = "UNSTABLE_DATA_MINED"
INSUFFICIENT_EVIDENCE = "INSUFFICIENT_REGIME_EVIDENCE"

_UNKNOWN = "UNKNOWN"

_DEF = {
    "warmup_periods": 6,
    "min_periods_for_regime_claim": 12,
    "regime_specific_min_ic_t": 2.0,
    "robust_min_ic_mean": 0.0,
}


def regime_version_hash() -> str:
    """Deterministic 16-hex content hash of the pinned regime definition set."""
    payload = repr((REGIME_VERSION, REGIME_AXES))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _expanding_median_flags(values: Sequence[Optional[float]], *,
                            warmup: int) -> list[Optional[bool]]:
    """For each i, True when values[i] is strictly above the median of the
    finite values in [0..i] (expanding window - uses only data through i).
    Warmup periods and non-finite values yield None."""
    out: list[Optional[bool]] = []
    hist: list[float] = []
    for i, v in enumerate(values):
        num = float(v) if ec._is_num(v) else None
        med = _median(hist) if hist else None
        if num is None or i < warmup or med is None:
            out.append(None)
        else:
            out.append(num > med)
        if num is not None:
            hist.append(num)
    return out


def _median(vals: list[float]) -> Optional[float]:
    if not vals:
        return None
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def classify_regimes(market_features: Sequence[dict], *,
                     cfg: Optional[dict] = None) -> list[dict]:
    """Assign a fixed-axis regime label to every period from OWNED market
    features only (no forward returns, no candidate values). ``market_features[i]``
    should carry the period-``i`` statistics computed through formation ``i``:
    ``market_return``, ``market_vol`` and optionally ``breadth`` (true
    cross-sectional participation - the fraction of eligible NAMES with a positive
    trailing return at formation), ``dispersion`` (the cross-sectional standard
    deviation of those same eligible-name trailing returns) and ``rate_change``.
    ``breadth`` / ``dispersion`` are candidate-INDEPENDENT and are ``None``
    (UNKNOWN) when cross-sectional coverage is insufficient. Returns one label dict
    per period; unresolved axes are ``UNKNOWN`` (warmup / missing feature / below
    minimum coverage), never guessed."""
    c = dict(_DEF)
    for k, v in (cfg or {}).items():
        if k in c and isinstance(v, (int, float)):
            c[k] = float(v) if isinstance(v, float) else int(v)
    warmup = int(c["warmup_periods"])
    n = len(market_features)

    vol_hi = _expanding_median_flags(
        [f.get("market_vol") for f in market_features], warmup=warmup)
    disp_hi = _expanding_median_flags(
        [f.get("dispersion") for f in market_features], warmup=warmup)

    labels: list[dict] = []
    for i in range(n):
        f = market_features[i]
        # market_trend: sign of the trailing market return (natural zero split).
        mr = f.get("market_return")
        trend = _UNKNOWN if not ec._is_num(mr) else (
            "TREND_UP" if float(mr) > 0 else "TREND_DOWN")
        # market_volatility: expanding-median split (PIT-safe).
        vflag = vol_hi[i]
        vol = _UNKNOWN if vflag is None else ("VOL_HIGH" if vflag else "VOL_LOW")
        # participation / breadth: cross-sectional fraction of eligible names with
        # a positive trailing return at formation, natural 0.5 split (UNKNOWN when
        # coverage is insufficient - breadth is None).
        br = f.get("breadth")
        part = _UNKNOWN if not ec._is_num(br) else (
            "BREADTH_BROAD" if float(br) >= 0.5 else "BREADTH_NARROW")
        # dispersion: expanding-median split.
        dflag = disp_hi[i]
        disp = _UNKNOWN if dflag is None else (
            "DISPERSION_HIGH" if dflag else "DISPERSION_LOW")
        # risk_state: composite of fixed axes (up trend + low vol = risk-on).
        if trend == _UNKNOWN or vol == _UNKNOWN:
            risk = _UNKNOWN
        else:
            risk = ("RISK_ON" if (trend == "TREND_UP" and vol == "VOL_LOW")
                    else "RISK_OFF")
        # rates: only when an owned rate change is supplied.
        rc = f.get("rate_change")
        rates = _UNKNOWN if not ec._is_num(rc) else (
            "RATES_RISING" if float(rc) > 0 else "RATES_FALLING")
        labels.append({
            "period": i, "market_trend": trend, "market_volatility": vol,
            "participation": part, "dispersion": disp, "risk_state": risk,
            "rates": rates})
    return labels


def _drawdown(vals: list[float]) -> Optional[float]:
    dd = ec.max_drawdown(vals)
    return None if dd is None else dd * 100.0


def regime_conditioned_metrics(labels: Sequence[dict], *,
                               ic_series: Sequence[Optional[float]],
                               spread_series: Optional[Sequence] = None,
                               turnover_series: Optional[Sequence] = None,
                               portfolio_returns: Optional[Sequence] = None,
                               cfg: Optional[dict] = None) -> dict:
    """Rank IC, decile spread, turnover and drawdown conditioned on each regime
    state, with an explicit sample size per bucket. A bucket whose sample size is
    below ``min_periods_for_regime_claim`` is flagged ``sufficient=False`` so a
    conditional-alpha claim is never made on too few observations."""
    c = dict(_DEF)
    for k, v in (cfg or {}).items():
        if k in c and isinstance(v, (int, float)):
            c[k] = float(v) if isinstance(v, float) else int(v)
    min_claim = int(c["min_periods_for_regime_claim"])

    def _at(seq, i):
        return seq[i] if (seq is not None and i < len(seq)) else None

    out: dict[str, dict] = {}
    for axis, sa, sb, _src in REGIME_AXES:
        axis_out: dict[str, dict] = {}
        for state in (sa, sb):
            idx = [i for i, lb in enumerate(labels)
                   if lb.get(axis) == state and i < len(ic_series)]
            ics = [ic_series[i] for i in idx if ec._is_num(ic_series[i])]
            spr = [_at(spread_series, i) for i in idx]
            spr = [v for v in spr if ec._is_num(v)]
            trn = [_at(turnover_series, i) for i in idx]
            trn = [v for v in trn if ec._is_num(v)]
            pr = [_at(portfolio_returns, i) for i in idx]
            pr = [float(v) for v in pr if ec._is_num(v)]
            n = len(ics)
            axis_out[state] = {
                "n_periods": n,
                "rank_ic_mean": ec.mean(ics),
                "rank_ic_t": ec.tstat(ics),
                "decile_spread_mean": ec.mean(spr) if spr else None,
                "avg_turnover": ec.mean(trn) if trn else None,
                "max_drawdown_pct": _drawdown(pr) if pr else None,
                "sufficient": n >= min_claim,
            }
        out[axis] = axis_out
    return out


def classify_alpha_stability(conditioned: dict, *,
                             cfg: Optional[dict] = None) -> dict:
    """Distinguish broadly-robust alpha from legitimately regime-specific alpha
    from unstable data-mined behavior, using the conditioned IC means / t-stats
    and per-bucket sample sizes. Deterministic and conservative: a claim that a
    candidate 'works in a regime' requires sufficient observations in that
    regime, otherwise the verdict is UNSTABLE_DATA_MINED or INSUFFICIENT."""
    c = dict(_DEF)
    for k, v in (cfg or {}).items():
        if k in c and isinstance(v, (int, float)):
            c[k] = float(v) if isinstance(v, float) else int(v)
    ic_t_min = float(c["regime_specific_min_ic_t"])
    ic_mean_min = float(c["robust_min_ic_mean"])

    axis_verdicts: dict[str, str] = {}
    any_regime_specific = False
    any_robust_axis = False
    for axis, sa, sb, _src in REGIME_AXES:
        pair = conditioned.get(axis, {})
        a = pair.get(sa, {})
        b = pair.get(sb, {})
        if not (a.get("sufficient") and b.get("sufficient")):
            axis_verdicts[axis] = INSUFFICIENT_EVIDENCE
            continue
        at, bt = a.get("rank_ic_t"), b.get("rank_ic_t")
        am, bm = a.get("rank_ic_mean"), b.get("rank_ic_mean")
        a_strong = (at is not None and at >= ic_t_min
                    and am is not None and am > ic_mean_min)
        b_strong = (bt is not None and bt >= ic_t_min
                    and bm is not None and bm > ic_mean_min)
        a_pos = am is not None and am > ic_mean_min
        b_pos = bm is not None and bm > ic_mean_min
        if a_strong and b_strong:
            axis_verdicts[axis] = BROADLY_ROBUST
            any_robust_axis = True
        elif (a_strong and not b_pos) or (b_strong and not a_pos):
            axis_verdicts[axis] = REGIME_SPECIFIC
            any_regime_specific = True
        elif a_pos and b_pos:
            axis_verdicts[axis] = BROADLY_ROBUST
            any_robust_axis = True
        else:
            axis_verdicts[axis] = UNSTABLE_DATA_MINED
    # Overall verdict: robust on a sufficiently-sampled axis dominates; else a
    # legitimate regime-specific finding; else unstable / insufficient.
    resolved = [v for v in axis_verdicts.values()
                if v != INSUFFICIENT_EVIDENCE]
    if any_robust_axis:
        overall = BROADLY_ROBUST
    elif any_regime_specific:
        overall = REGIME_SPECIFIC
    elif resolved:
        overall = UNSTABLE_DATA_MINED
    else:
        overall = INSUFFICIENT_EVIDENCE
    return {"overall": overall, "by_axis": axis_verdicts,
            "regime_version_hash": regime_version_hash()}


__all__ = [
    "REGIME_VERSION", "REGIME_AXES", "REGIME_AXIS_KEYS", "regime_version_hash",
    "classify_regimes", "regime_conditioned_metrics", "classify_alpha_stability",
    "BROADLY_ROBUST", "REGIME_SPECIFIC", "UNSTABLE_DATA_MINED",
    "INSUFFICIENT_EVIDENCE",
]
