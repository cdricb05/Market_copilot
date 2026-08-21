"""Shared, pure helpers for the Release-32 sleeves.

Everything here is a function of PAST observations only. The single rule that
governs this file: a feature computed at decision index ``i`` may read
``series[:i+1]`` and never ``series[i+1:]``. Release 31 shipped a walk-forward
fallback that fitted on dates after the one being scored, so this is enforced by
construction - every helper takes ``i`` and slices with ``i + 1`` as the
exclusive upper bound.
"""
from __future__ import annotations

import numpy as np


def trailing(series: np.ndarray, i: int, window: int) -> np.ndarray:
    """The ``window`` observations ending AT and INCLUDING index ``i``."""
    lo = max(0, i - window + 1)
    return np.asarray(series[lo:i + 1], dtype=float)


def momentum(levels: np.ndarray, i: int, lookback: int,
             skip: int = 0) -> float:
    """Total return over ``lookback`` sessions ending ``skip`` sessions ago."""
    end = i - skip
    start = end - lookback
    if start < 0 or end < 0 or end >= len(levels):
        return float("nan")
    a, b = float(levels[start]), float(levels[end])
    if not np.isfinite(a) or not np.isfinite(b) or a <= 0.0:
        return float("nan")
    return (b / a) - 1.0


def realised_vol(levels: np.ndarray, i: int, window: int) -> float:
    """Annualised realised volatility of daily log changes up to ``i``."""
    w = trailing(levels, i, window + 1)
    w = w[np.isfinite(w)]
    if w.size < 5:
        return float("nan")
    r = np.diff(np.log(w))
    if r.size < 2:
        return float("nan")
    return float(np.std(r, ddof=1) * np.sqrt(252.0))


def moving_average(levels: np.ndarray, i: int, window: int) -> float:
    w = trailing(levels, i, window)
    w = w[np.isfinite(w)]
    return float(np.mean(w)) if w.size else float("nan")


def percentile_rank(series: np.ndarray, i: int, window: int) -> float:
    """Where today's value sits within its own trailing distribution, 0..1.

    An expanding or trailing rank is used rather than a fixed threshold because
    a threshold calibrated on the full sample is a parameter chosen with
    knowledge of the future.
    """
    w = trailing(series, i, window)
    w = w[np.isfinite(w)]
    if w.size < 10:
        return float("nan")
    x = float(series[i])
    if not np.isfinite(x):
        return float("nan")
    return float(np.mean(w <= x))


def zscore(series: np.ndarray, i: int, window: int) -> float:
    w = trailing(series, i, window)
    w = w[np.isfinite(w)]
    if w.size < 10:
        return float("nan")
    mu, sd = float(np.mean(w)), float(np.std(w, ddof=1))
    x = float(series[i])
    if not np.isfinite(x) or sd <= 0.0:
        return float("nan")
    return (x - mu) / sd


def inverse_vol_weights(vols: dict, *, gross: float = 1.0) -> dict:
    """Weights proportional to 1/vol. Risk parity's simplest honest form."""
    usable = {k: float(v) for k, v in vols.items()
              if v is not None and np.isfinite(v) and float(v) > 0.0}
    if not usable:
        return {}
    inv = {k: 1.0 / v for k, v in usable.items()}
    total = sum(inv.values())
    if total <= 0.0:
        return {}
    return {k: gross * (v / total) for k, v in inv.items()}


def clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    if not np.isfinite(x):
        return lo
    return float(min(hi, max(lo, x)))
