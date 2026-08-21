"""alpha_agent.r33.predictive - the ONE forecast-scoring owner.

A candidate cannot qualify on PnL. It has to improve an actual forecast score
against the baseline the contract named before validation existed. This module
computes those scores and nothing else - it has no opinion about portfolios.

The baseline is always fitted on TRAINING data and then applied unchanged to the
evaluation block. That detail decides whether a skill score means anything: an
"out-of-sample R-squared" computed against the evaluation period's OWN mean is
not out of sample, because the evaluation mean is not knowable in advance. The
same applies to the base rate for log loss and to trailing volatility for QLIKE.

Per-date aggregation is used everywhere inference is done. Rows within one
forecast date are strongly cross-sectionally correlated - sixty-six markets on
one day are nothing like sixty-six independent observations - so a t-statistic
computed across ROWS would be inflated by roughly the square root of the
cross-section. Statistics are therefore computed on the per-date series, whose
observations are non-overlapping by construction.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from ..r31.multiple_testing import two_sided_p
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r33.predictive"

METRIC_OOS_R2 = "OOS_R2"
METRIC_LOG_LOSS_SKILL = "LOG_LOSS_SKILL"
METRIC_QLIKE_SKILL = "QLIKE_SKILL"
METRIC_RANK_IC = "RANK_IC"


def _finite_pair(yhat, y):
    a = np.asarray(yhat, dtype=np.float64)
    b = np.asarray(y, dtype=np.float64)
    m = np.isfinite(a) & np.isfinite(b)
    return a[m], b[m], m


def newey_west_t(x: np.ndarray, lags: int = 4) -> float:
    """t-statistic of the mean with a Newey-West correction."""
    v = np.asarray(x, dtype=np.float64)
    v = v[np.isfinite(v)]
    n = v.size
    if n < 8:
        return float("nan")
    mu = float(v.mean())
    dev = v - mu
    gamma0 = float(dev @ dev / n)
    var = gamma0
    for L in range(1, min(int(lags), n - 1) + 1):
        cov = float(dev[L:] @ dev[:-L] / n)
        var += 2.0 * (1.0 - L / (lags + 1.0)) * cov
    if var <= 0:
        return float("nan")
    return mu / math.sqrt(var / n)


# --------------------------------------------------------------------------- #
# Return forecasts
# --------------------------------------------------------------------------- #
def oos_r2(yhat: np.ndarray, y: np.ndarray, *, baseline: float) -> dict:
    """Out-of-sample R-squared against a baseline fixed on TRAINING data."""
    a, b, _ = _finite_pair(yhat, y)
    if a.size < 8:
        return {"metric": METRIC_OOS_R2, "value": None, "n": int(a.size),
                "state": "INSUFFICIENT_OBSERVATIONS"}
    sse_model = float(np.sum((b - a) ** 2))
    sse_base = float(np.sum((b - float(baseline)) ** 2))
    if sse_base <= 0:
        return {"metric": METRIC_OOS_R2, "value": None, "n": int(a.size),
                "state": "DEGENERATE_BASELINE"}
    return {"metric": METRIC_OOS_R2,
            "value": float(1.0 - sse_model / sse_base),
            "mse_model": sse_model / a.size,
            "mse_baseline": sse_base / a.size,
            "mae_model": float(np.mean(np.abs(b - a))),
            "mae_baseline": float(np.mean(np.abs(b - float(baseline)))),
            "n": int(a.size), "baseline": float(baseline), "state": "OK"}


def per_date_error_gain(yhat, y, dates, *, baseline: float) -> pd.Series:
    """Per-date reduction in squared error versus the baseline.

    Positive means the model beat the baseline on that date. This is the series
    every inference statistic on a return forecast is computed from.
    """
    df = pd.DataFrame({"yhat": np.asarray(yhat, dtype=np.float64),
                       "y": np.asarray(y, dtype=np.float64),
                       "date": pd.DatetimeIndex(dates)}).dropna()
    if df.empty:
        return pd.Series(dtype=float)
    df["gain"] = (df["y"] - float(baseline)) ** 2 - (df["y"] - df["yhat"]) ** 2
    return df.groupby("date")["gain"].mean()


# --------------------------------------------------------------------------- #
# Probability forecasts
# --------------------------------------------------------------------------- #
def log_loss(p: np.ndarray, y: np.ndarray) -> float:
    a, b, _ = _finite_pair(p, y)
    if a.size == 0:
        return float("nan")
    a = np.clip(a, 1e-6, 1.0 - 1e-6)
    return float(-np.mean(b * np.log(a) + (1.0 - b) * np.log(1.0 - a)))


def brier(p: np.ndarray, y: np.ndarray) -> float:
    a, b, _ = _finite_pair(p, y)
    return float(np.mean((a - b) ** 2)) if a.size else float("nan")


def calibration(p: np.ndarray, y: np.ndarray, *, bins: int = 10) -> list:
    a, b, _ = _finite_pair(p, y)
    if a.size == 0:
        return []
    edges = np.linspace(0.0, 1.0, int(bins) + 1)
    out = []
    for k in range(int(bins)):
        m = (a >= edges[k]) & (a < edges[k + 1] if k < bins - 1 else a <= 1.0)
        if m.sum() < 5:
            continue
        out.append({"bin": [round(float(edges[k]), 3),
                            round(float(edges[k + 1]), 3)],
                    "n": int(m.sum()),
                    "mean_forecast": round(float(a[m].mean()), 5),
                    "observed_rate": round(float(b[m].mean()), 5)})
    return out


def log_loss_skill(p: np.ndarray, y: np.ndarray, *, base_rate: float) -> dict:
    """Skill relative to always forecasting the TRAINING base rate."""
    a, b, _ = _finite_pair(p, y)
    if a.size < 8:
        return {"metric": METRIC_LOG_LOSS_SKILL, "value": None,
                "n": int(a.size), "state": "INSUFFICIENT_OBSERVATIONS"}
    ll_model = log_loss(a, b)
    ll_base = log_loss(np.full(a.size, float(base_rate)), b)
    if not math.isfinite(ll_base) or ll_base <= 0:
        return {"metric": METRIC_LOG_LOSS_SKILL, "value": None,
                "n": int(a.size), "state": "DEGENERATE_BASELINE"}
    return {"metric": METRIC_LOG_LOSS_SKILL,
            "value": float(1.0 - ll_model / ll_base),
            "log_loss_model": ll_model, "log_loss_baseline": ll_base,
            "brier_model": brier(a, b),
            "brier_baseline": brier(np.full(a.size, float(base_rate)), b),
            "calibration": calibration(a, b),
            "base_rate": float(base_rate), "n": int(a.size), "state": "OK"}


def per_date_log_loss_gain(p, y, dates, *, base_rate: float) -> pd.Series:
    df = pd.DataFrame({"p": np.asarray(p, dtype=np.float64),
                       "y": np.asarray(y, dtype=np.float64),
                       "date": pd.DatetimeIndex(dates)}).dropna()
    if df.empty:
        return pd.Series(dtype=float)
    pc = np.clip(df["p"].to_numpy(), 1e-6, 1 - 1e-6)
    base = float(np.clip(base_rate, 1e-6, 1 - 1e-6))
    yv = df["y"].to_numpy()
    ll_m = -(yv * np.log(pc) + (1 - yv) * np.log(1 - pc))
    ll_b = -(yv * math.log(base) + (1 - yv) * math.log(1 - base))
    df["gain"] = ll_b - ll_m
    return df.groupby("date")["gain"].mean()


# --------------------------------------------------------------------------- #
# Volatility forecasts
# --------------------------------------------------------------------------- #
def qlike(forecast: np.ndarray, realised: np.ndarray) -> float:
    """QLIKE loss on VARIANCE. Robust to the noise in a realised proxy."""
    f, r, _ = _finite_pair(forecast, realised)
    m = (f > 1e-8) & (r >= 0.0)
    f, r = f[m], r[m]
    if f.size == 0:
        return float("nan")
    fv, rv = f ** 2, r ** 2
    return float(np.mean(rv / fv - np.log(np.clip(rv / fv, 1e-12, None)) - 1.0))


def qlike_skill(forecast: np.ndarray, realised: np.ndarray,
                *, baseline_forecast: np.ndarray) -> dict:
    q_model = qlike(forecast, realised)
    q_base = qlike(baseline_forecast, realised)
    n = int(np.isfinite(np.asarray(forecast, dtype=float)).sum())
    if not math.isfinite(q_model) or not math.isfinite(q_base) or q_base <= 0:
        return {"metric": METRIC_QLIKE_SKILL, "value": None, "n": n,
                "state": "DEGENERATE_BASELINE"}
    return {"metric": METRIC_QLIKE_SKILL,
            "value": float(1.0 - q_model / q_base),
            "qlike_model": q_model, "qlike_baseline": q_base,
            "n": n, "state": "OK"}


def per_date_qlike_gain(forecast, realised, dates, baseline_forecast
                        ) -> pd.Series:
    df = pd.DataFrame({"f": np.asarray(forecast, dtype=np.float64),
                       "r": np.asarray(realised, dtype=np.float64),
                       "b": np.asarray(baseline_forecast, dtype=np.float64),
                       "date": pd.DatetimeIndex(dates)}).dropna()
    df = df[(df["f"] > 1e-8) & (df["b"] > 1e-8) & (df["r"] >= 0.0)]
    if df.empty:
        return pd.Series(dtype=float)
    rv = df["r"].to_numpy() ** 2
    def _q(fv):
        ratio = np.clip(rv / fv, 1e-12, None)
        return ratio - np.log(ratio) - 1.0
    df["gain"] = _q(df["b"].to_numpy() ** 2) - _q(df["f"].to_numpy() ** 2)
    return df.groupby("date")["gain"].mean()


# --------------------------------------------------------------------------- #
# Cross-sectional forecasts
# --------------------------------------------------------------------------- #
def per_date_rank_ic(yhat, y, dates) -> pd.Series:
    """Spearman rank correlation within each forecast date."""
    df = pd.DataFrame({"yhat": np.asarray(yhat, dtype=np.float64),
                       "y": np.asarray(y, dtype=np.float64),
                       "date": pd.DatetimeIndex(dates)}).dropna()
    if df.empty:
        return pd.Series(dtype=float)
    out = {}
    for d, block in df.groupby("date"):
        if len(block) < 5:
            continue
        a = block["yhat"].rank().to_numpy()
        b = block["y"].rank().to_numpy()
        sa, sb = a.std(ddof=1), b.std(ddof=1)
        if sa <= 0 or sb <= 0:
            continue
        out[d] = float(np.corrcoef(a, b)[0, 1])
    return pd.Series(out).sort_index()


def rank_ic(yhat, y, dates) -> dict:
    ic = per_date_rank_ic(yhat, y, dates)
    if ic.size < 8:
        return {"metric": METRIC_RANK_IC, "value": None, "n": int(ic.size),
                "state": "INSUFFICIENT_DATES"}
    t = newey_west_t(ic.to_numpy())
    mean = float(ic.mean())
    sd = float(ic.std(ddof=1))
    return {"metric": METRIC_RANK_IC, "value": mean,
            "ic_std": sd,
            "ic_information_ratio": float(mean / sd) if sd > 0 else None,
            "ic_positive_fraction": float((ic > 0).mean()),
            "t_stat": t, "p_value": two_sided_p(t) if math.isfinite(t) else None,
            "n": int(ic.size), "state": "OK"}


# --------------------------------------------------------------------------- #
# One entry point
# --------------------------------------------------------------------------- #
def score(target: str, *, yhat, y, dates, baseline_value=None,
          baseline_forecast=None) -> dict:
    """Score one candidate on one target with that target's PRIMARY metric.

    ``primary`` is read from the contract, never chosen here after seeing the
    numbers. Everything else in the returned dict is a diagnostic.
    """
    primary = _contract.PRIMARY_METRIC[target]
    if target == _contract.TARGET_VOLATILITY:
        res = qlike_skill(yhat, y, baseline_forecast=baseline_forecast)
        gains = per_date_qlike_gain(yhat, y, dates, baseline_forecast)
    elif target == _contract.TARGET_SIGN:
        res = log_loss_skill(yhat, y, base_rate=float(baseline_value))
        gains = per_date_log_loss_gain(yhat, y, dates,
                                       base_rate=float(baseline_value))
    elif target == _contract.TARGET_CROSS_SECTION:
        res = rank_ic(yhat, y, dates)
        gains = per_date_rank_ic(yhat, y, dates)
    else:
        res = oos_r2(yhat, y, baseline=float(baseline_value))
        gains = per_date_error_gain(yhat, y, dates,
                                    baseline=float(baseline_value))
    out = dict(res)
    out["primary_metric"] = primary
    out["primary_value"] = res.get("value")
    out["scored_dates"] = int(gains.size)
    if gains.size >= 8:
        t = newey_west_t(gains.to_numpy())
        out["gain_t_stat"] = t
        out["gain_p_value"] = two_sided_p(t) if math.isfinite(t) else None
        out["gain_mean"] = float(gains.mean())
        out["gain_positive_fraction"] = float((gains > 0).mean())
    else:
        out["gain_t_stat"] = None
        out["gain_p_value"] = None
        out["gain_mean"] = None
        out["gain_positive_fraction"] = None
    out["_per_date_gain"] = gains
    return out
