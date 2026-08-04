"""Stage 12 Workstream C -- sample power and validation design.

The Stage 11 evaluator computes an IC t-statistic that treats every rebalance
period as i.i.d. It has NO effective-sample / overlap / autocorrelation-robust
adjustment (confirmed by the architecture survey). This module supplies that
missing machinery so Stage 12 can honestly answer: *was the Stage 11 tournament
underpowered, and how much power does the maximum valid owned-data expansion buy?*

Everything is pure numpy/stdlib (scipy is NOT available in this environment):
* Newey-West / Bartlett long-run variance -> overlap- and autocorrelation-aware
  effective sample size and a date-clustered t-statistic.
* Minimum detectable rank-IC and approximate power via an inverse-normal
  (Acklam) and ``math.erf`` normal CDF -- no scipy.
* The maximum honest confirmatory family size implied by a given best p-value
  (so the pre-registered family stays small enough to retain FDR power).
* Thin, deterministic wrappers over the existing purged walk-forward / embargo /
  moving-block-bootstrap primitives in ``selection_controls``.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Mapping, Optional, Sequence

FORWARD_HORIZONS_DAYS = (5, 20, 63)
TRADING_DAYS_PER_YEAR = 252


# ---------------------------------------------------------------------------
# Normal distribution helpers (no scipy).
# ---------------------------------------------------------------------------
def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def norm_ppf(p: float) -> float:
    """Inverse standard-normal CDF via Acklam's rational approximation."""
    if p <= 0.0:
        return -math.inf
    if p >= 1.0:
        return math.inf
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    plow, phigh = 0.02425, 1.0 - 0.02425
    if p < plow:
        q = math.sqrt(-2.0 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
               ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
    if p > phigh:
        q = math.sqrt(-2.0 * math.log(1.0 - p))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
               ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
    q = p - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / \
           (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)


def two_sided_pvalue(t: float) -> float:
    return 2.0 * (1.0 - norm_cdf(abs(float(t))))


# ---------------------------------------------------------------------------
# Newey-West effective sample size + date-clustered t-stat.
# ---------------------------------------------------------------------------
def _autocov(x: List[float], lag: int, mean: float) -> float:
    n = len(x)
    if lag >= n:
        return 0.0
    s = 0.0
    for i in range(lag, n):
        s += (x[i] - mean) * (x[i - lag] - mean)
    return s / n


def newey_west_effective_n(series: Sequence[float], *, max_lag: Optional[int] = None
                           ) -> Dict[str, Any]:
    """Overlap/autocorrelation-aware effective sample size for a series mean.

    Uses a Bartlett-kernel long-run variance. The effective N for the sample
    mean is ``N * gamma0 / LRV`` -- i.e. how many independent observations the
    autocorrelated series is worth. ``max_lag`` defaults to the automatic
    ``floor(4*(N/100)^(2/9))`` (Newey-West 1994), floored at 1.
    """
    x = [float(v) for v in series if v is not None]
    n = len(x)
    out: Dict[str, Any] = {"n_nominal": n, "effective_n": float(n),
                           "variance_inflation": 1.0, "max_lag": 0}
    if n < 3:
        return out
    mean = sum(x) / n
    g0 = _autocov(x, 0, mean)
    if g0 <= 0:
        return out
    if max_lag is None:
        max_lag = max(1, int(math.floor(4.0 * (n / 100.0) ** (2.0 / 9.0))))
    max_lag = min(max_lag, n - 1)
    lrv = g0
    for lag in range(1, max_lag + 1):
        w = 1.0 - lag / (max_lag + 1.0)  # Bartlett weight
        lrv += 2.0 * w * _autocov(x, lag, mean)
    lrv = max(lrv, 1e-18)
    vif = lrv / g0
    eff_n = n * g0 / lrv
    eff_n = max(1.0, min(float(n), eff_n))
    out.update({"effective_n": round(eff_n, 3),
                "variance_inflation": round(vif, 4),
                "max_lag": max_lag,
                "mean": mean})
    return out


def clustered_t_stat(series: Sequence[float], *, max_lag: Optional[int] = None
                     ) -> Dict[str, Any]:
    """Autocorrelation-robust (Newey-West) t-stat for the mean of a series."""
    x = [float(v) for v in series if v is not None]
    n = len(x)
    if n < 3:
        return {"t_naive": None, "t_clustered": None, "effective_n": float(n)}
    mean = sum(x) / n
    g0 = _autocov(x, 0, mean)
    sd = math.sqrt(g0) if g0 > 0 else 0.0
    t_naive = (mean / (sd / math.sqrt(n))) if sd > 0 else None
    nw = newey_west_effective_n(x, max_lag=max_lag)
    lrv = g0 * nw["variance_inflation"]
    se_nw = math.sqrt(lrv / n) if lrv > 0 else 0.0
    t_clustered = (mean / se_nw) if se_nw > 0 else None
    return {
        "t_naive": round(t_naive, 4) if t_naive is not None else None,
        "t_clustered": round(t_clustered, 4) if t_clustered is not None else None,
        "effective_n": nw["effective_n"],
        "variance_inflation": nw["variance_inflation"],
        "n_nominal": n,
    }


def overlap_effective_n(n_periods: int, horizon_days: int, step_days: int) -> Dict[str, Any]:
    """Analytic overlap adjustment when H-day returns are sampled every step days.

    Adjacent windows share ``max(0, H-step)`` days. The classic non-overlap bound
    is ``N_eff = N * step / H`` (each independent block spans H days). Reported
    alongside the overlap fraction so a design can be judged before evaluation.
    """
    if n_periods <= 0 or horizon_days <= 0 or step_days <= 0:
        return {"n_nominal": n_periods, "effective_n": float(max(0, n_periods)),
                "overlap_fraction": 0.0}
    overlap = max(0, horizon_days - step_days)
    overlap_fraction = overlap / horizon_days
    ratio = min(1.0, step_days / horizon_days)
    eff = max(1.0, n_periods * ratio) if n_periods else 0.0
    return {"n_nominal": n_periods, "horizon_days": horizon_days,
            "step_days": step_days, "overlap_days": overlap,
            "overlap_fraction": round(overlap_fraction, 4),
            "independence_ratio": round(ratio, 4),
            "effective_n": round(eff, 3)}


# ---------------------------------------------------------------------------
# Minimum detectable IC and power.
# ---------------------------------------------------------------------------
def minimum_detectable_ic(n_eff: float, ic_sd: float, *, alpha: float = 0.05,
                          power: float = 0.80) -> Optional[float]:
    """Smallest mean rank-IC detectable at the given size and power.

    For an IC t-test, ``t = mean / (ic_sd / sqrt(n_eff))``. Detection requires
    the non-centrality to exceed ``z_{alpha/2} + z_{power}``.
    """
    if n_eff <= 0 or ic_sd <= 0:
        return None
    z_a = norm_ppf(1.0 - alpha / 2.0)
    z_b = norm_ppf(power)
    return (z_a + z_b) * ic_sd / math.sqrt(n_eff)


def approximate_power(true_ic: float, n_eff: float, ic_sd: float, *,
                      alpha: float = 0.05) -> Optional[float]:
    """Two-sided power to detect a mean rank-IC of ``true_ic``."""
    if n_eff <= 0 or ic_sd <= 0:
        return None
    ncp = abs(true_ic) / (ic_sd / math.sqrt(n_eff))
    z_a = norm_ppf(1.0 - alpha / 2.0)
    return max(0.0, min(1.0, norm_cdf(ncp - z_a) + norm_cdf(-ncp - z_a)))


def max_confirmatory_family_size(best_pvalue: float, *, alpha: float = 0.05
                                 ) -> Optional[int]:
    """Largest BH family size at which the best candidate still survives.

    BH at rank 1 requires ``p_(1) * m / 1 <= alpha`` -> ``m <= alpha / p_(1)``.
    This is the hard ceiling on how many hypotheses the pre-registered family may
    contain before the strongest candidate is lost to the correction.
    """
    if best_pvalue is None or best_pvalue <= 0:
        return None
    return max(1, int(math.floor(alpha / float(best_pvalue))))


# ---------------------------------------------------------------------------
# Validation-design wrappers over the existing primitives.
# ---------------------------------------------------------------------------
def purged_walk_forward_design(n_periods: int, *, n_folds: int = 4, embargo: int = 1,
                               purge: int = 1, label_horizon: int = 1) -> Dict[str, Any]:
    """Purged, embargoed, forward-only folds + a leakage certificate."""
    try:
        from . import selection_controls as sc
    except Exception:  # pragma: no cover
        import selection_controls as sc  # type: ignore
    folds = sc.purged_walk_forward_folds(
        n_periods, n_folds=n_folds, embargo=embargo, purge=purge,
        label_horizon=label_horizon)
    try:
        clean = sc.verify_no_leakage(folds, embargo=embargo, purge=purge,
                                     label_horizon=label_horizon)
    except Exception:
        clean = None
    serial = []
    for f in folds:
        train = getattr(f, "train", None)
        test = getattr(f, "test", None)
        if train is None and isinstance(f, Mapping):
            train, test = f.get("train"), f.get("test")
        serial.append({"train_n": len(train) if train is not None else None,
                       "test_n": len(test) if test is not None else None})
    return {"n_periods": n_periods, "n_folds": n_folds, "embargo": embargo,
            "purge": purge, "label_horizon": label_horizon,
            "folds": serial, "no_leakage_verified": clean}


def moving_block_bootstrap_ci(series: Sequence[float], *, block_size: int = 4,
                              n_resamples: int = 1000, seed: int = 12345,
                              alpha: float = 0.05) -> Dict[str, Any]:
    """Deterministic circular moving-block bootstrap CI for the series mean."""
    try:
        from . import selection_controls as sc
    except Exception:  # pragma: no cover
        import selection_controls as sc  # type: ignore
    x = [float(v) for v in series if v is not None]
    if len(x) < max(4, block_size):
        return {"mean": (sum(x) / len(x) if x else None), "ci_low": None,
                "ci_high": None, "positive_fraction": None, "n": len(x)}
    ci = sc.block_bootstrap_ci(x, block_size=block_size, n_resamples=n_resamples,
                               seed=seed, alpha=alpha)
    if isinstance(ci, Mapping):
        return {"n": len(x), **{k: ci.get(k) for k in ci}}
    return {"n": len(x), "ci": ci}


# ---------------------------------------------------------------------------
# Design-level power diagnosis for a whole tournament.
# ---------------------------------------------------------------------------
def diagnose_design(*, n_periods: int, ic_series: Optional[Sequence[float]] = None,
                    ic_sd: Optional[float] = None, best_true_ic: Optional[float] = None,
                    best_pvalue: Optional[float] = None,
                    step_days: int = 63, horizons: Sequence[int] = FORWARD_HORIZONS_DAYS,
                    alpha: float = 0.05, power: float = 0.80) -> Dict[str, Any]:
    """One-call power diagnosis for a rebalance design.

    Returns nominal vs effective N (both the analytic overlap bound and, when an
    IC series is supplied, the data-driven Newey-West estimate), the minimum
    detectable IC, approximate power at the observed best effect, and the maximum
    honest confirmatory family size.
    """
    if ic_sd is None and ic_series is not None:
        xs = [float(v) for v in ic_series if v is not None]
        if len(xs) >= 2:
            m = sum(xs) / len(xs)
            ic_sd = math.sqrt(sum((v - m) ** 2 for v in xs) / (len(xs) - 1))
    per_horizon = []
    for h in horizons:
        ov = overlap_effective_n(n_periods, h, step_days)
        row = {"horizon_days": h, **ov}
        if ic_sd:
            row["min_detectable_ic"] = minimum_detectable_ic(
                ov["effective_n"], ic_sd, alpha=alpha, power=power)
            if best_true_ic is not None:
                row["power_at_best"] = approximate_power(
                    best_true_ic, ov["effective_n"], ic_sd, alpha=alpha)
        per_horizon.append(row)
    nw = newey_west_effective_n(ic_series) if ic_series is not None else None
    return {
        "n_periods_nominal": n_periods,
        "step_days": step_days,
        "ic_sd": round(ic_sd, 6) if ic_sd else None,
        "newey_west_effective_n": nw,
        "per_horizon": per_horizon,
        "max_confirmatory_family_size": max_confirmatory_family_size(best_pvalue, alpha=alpha)
        if best_pvalue else None,
        "alpha": alpha,
        "target_power": power,
        "note": ("Effective N is materially below nominal whenever forward "
                 "horizons overlap the rebalance step. A confirmatory family "
                 "larger than max_confirmatory_family_size cannot retain FDR "
                 "power for the strongest observed candidate."),
    }
