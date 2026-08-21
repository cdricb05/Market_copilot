"""alpha_agent.r34.calibration - the ONE expected-return calibration owner.

Release 33 established that ranking ability exists and that raw model scores did
not map reliably onto economic magnitude. That gap is the first place expected
value can disappear between a forecast and a portfolio, and it is the first
thing this release measures.

A calibration answers a narrow question: given a forecast score ``s`` produced
by a frozen model, what excess return should be EXPECTED, and how sure is that?
Everything downstream - sizing, risk budgeting, the turnover penalty - needs an
expected return in return units, not a score in score units. Feeding a raw score
into a mean-variance objective silently asserts that one unit of score equals
one unit of return, and it does not.

Every mapping here is fitted on TRAINING rows and applied unchanged to the
evaluation block. A calibration fitted on the block it is scored on is not a
calibration, it is a fit.

Each mapping returns three things, because sizing needs all three:

* ``expected_return`` - the calibrated conditional mean;
* ``uncertainty``     - the predictive dispersion around it, which is the
  residual standard deviation of the calibration on training data. It is
  conditional on the score bucket where the sample supports that, because
  forecast error is not homoscedastic: extreme scores are rarer and noisier;
* ``confidence``      - a bounded shrinkage factor in ``[0, 1]`` recording how
  much of the raw score the calibration actually retained.

The isotonic mapping is admitted only above
``MIN_ISOTONIC_TRAINING_ROWS``. A monotone step function on a small sample will
happily fit noise into steps and report a beautiful in-sample calibration curve.
"""
from __future__ import annotations

import numpy as np

from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r34.calibration"


# --------------------------------------------------------------------------- #
# Pool-adjacent-violators - isotonic regression, implemented once
# --------------------------------------------------------------------------- #
def pool_adjacent_violators(y: np.ndarray, w: np.ndarray = None) -> np.ndarray:
    """Least-squares monotone non-decreasing fit to ``y``.

    Implemented here rather than imported: the estate deliberately carries no
    scipy or scikit-learn dependency in the research packages, and PAV is
    twenty lines of exact arithmetic rather than an approximation.
    """
    v = np.asarray(y, dtype=np.float64).copy()
    n = v.size
    if n == 0:
        return v
    weight = np.ones(n) if w is None else np.asarray(w, dtype=np.float64).copy()
    level = v.copy()
    count = weight.copy()
    starts = np.arange(n)
    k = 0
    for i in range(n):
        level[k] = v[i]
        count[k] = weight[i]
        starts[k] = i
        while k > 0 and level[k - 1] > level[k]:
            total = count[k - 1] + count[k]
            level[k - 1] = (level[k - 1] * count[k - 1]
                            + level[k] * count[k]) / total
            count[k - 1] = total
            k -= 1
        k += 1
    out = np.empty(n, dtype=np.float64)
    end = n
    for b in range(k - 1, -1, -1):
        out[starts[b]:end] = level[b]
        end = starts[b]
    return out


# --------------------------------------------------------------------------- #
# Fitting
# --------------------------------------------------------------------------- #
def _residual_dispersion(resid: np.ndarray, score: np.ndarray,
                         buckets: int = 5) -> dict:
    """Residual standard deviation, conditional on the score bucket.

    Forecast error is not homoscedastic. Extreme scores are rarer and are
    produced by more extreme feature configurations, so their errors are wider,
    and a single pooled sigma would make the sizing layer over-confident exactly
    where the book takes its largest positions.
    """
    r = np.asarray(resid, dtype=np.float64)
    s = np.asarray(score, dtype=np.float64)
    ok = np.isfinite(r) & np.isfinite(s)
    r, s = r[ok], s[ok]
    pooled = float(np.std(r, ddof=1)) if r.size > 2 else float("nan")
    if r.size < buckets * 50:
        return {"pooled": pooled, "edges": None, "by_bucket": None}
    edges = np.quantile(s, np.linspace(0.0, 1.0, buckets + 1))
    edges[0], edges[-1] = -np.inf, np.inf
    by = []
    for k in range(buckets):
        m = (s >= edges[k]) & (s < edges[k + 1])
        by.append(float(np.std(r[m], ddof=1)) if m.sum() > 10 else pooled)
    return {"pooled": pooled, "edges": [float(e) for e in edges],
            "by_bucket": by}


def fit(method: str, score: np.ndarray, y: np.ndarray) -> dict:
    """Fit one calibration on TRAINING rows only."""
    s = np.asarray(score, dtype=np.float64)
    r = np.asarray(y, dtype=np.float64)
    ok = np.isfinite(s) & np.isfinite(r)
    s, r = s[ok], r[ok]
    n = int(s.size)
    base = {"method": method, "n": n,
            "training_mean": float(r.mean()) if n else 0.0}
    if n < 100 or float(np.std(s, ddof=1)) <= 0:
        return {**base, "state": "INSUFFICIENT_TRAINING_ROWS",
                "kind": "CONSTANT", "constant": base["training_mean"],
                "dispersion": {"pooled": float(np.std(r, ddof=1))
                               if n > 2 else float("nan"),
                               "edges": None, "by_bucket": None},
                "confidence": 0.0}

    sd_s = float(np.std(s, ddof=1))
    mu_s = float(np.mean(s))
    slope = float(np.cov(s, r, ddof=1)[0, 1] / np.var(s, ddof=1))
    intercept = float(np.mean(r) - slope * mu_s)

    if method == _contract.CAL_LINEAR:
        fitted = intercept + slope * s
        spec = {**base, "kind": "LINEAR", "intercept": intercept,
                "slope": slope, "confidence": 1.0}

    elif method == _contract.CAL_RIDGE_SHRUNK:
        lam = float(_contract.CALIBRATION_RIDGE_SHRINK)
        b = slope * (1.0 - lam)
        a = float(np.mean(r) - b * mu_s)
        fitted = a + b * s
        spec = {**base, "kind": "LINEAR", "intercept": a, "slope": b,
                "shrink": lam, "confidence": 1.0 - lam}

    elif method == _contract.CAL_ISOTONIC:
        if n < int(_contract.MIN_ISOTONIC_TRAINING_ROWS):
            return {**base, "state": "ISOTONIC_SAMPLE_TOO_SMALL",
                    "kind": "CONSTANT", "constant": base["training_mean"],
                    "dispersion": _residual_dispersion(
                        r - base["training_mean"], s),
                    "confidence": 0.0}
        order = np.argsort(s, kind="mergesort")
        iso = pool_adjacent_violators(r[order])
        knots_s = s[order]
        fitted = np.empty(n)
        fitted[order] = iso
        spec = {**base, "kind": "ISOTONIC",
                "knots_score": [float(v) for v in knots_s],
                "knots_value": [float(v) for v in iso],
                "confidence": 1.0}

    elif method == _contract.CAL_RANK_BUCKET:
        k = int(_contract.RANK_BUCKET_COUNT)
        edges = np.quantile(s, np.linspace(0.0, 1.0, k + 1))
        edges[0], edges[-1] = -np.inf, np.inf
        means = []
        fitted = np.full(n, base["training_mean"])
        for j in range(k):
            m = (s >= edges[j]) & (s < edges[j + 1])
            v = float(r[m].mean()) if m.sum() > 5 else base["training_mean"]
            means.append(v)
            fitted[m] = v
        spec = {**base, "kind": "RANK_BUCKET",
                "edges": [float(e) for e in edges],
                "bucket_mean": means, "confidence": 1.0}

    elif method == _contract.CAL_BAYES:
        # Posterior mean under a normal prior centred on zero: the shrinkage is
        # the share of the score's cross-sectional variance that the training
        # data says is signal rather than noise.
        resid_var = float(np.var(r - (intercept + slope * s), ddof=1))
        signal_var = max(float(np.var(slope * s, ddof=1)), 0.0)
        k0 = float(_contract.CALIBRATION_BAYES_PRIOR_STRENGTH)
        denom = signal_var + k0 * resid_var
        shrink = float(np.clip(signal_var / denom, 0.0, 1.0)) if denom > 0 \
            else 0.0
        b = slope * shrink
        a = float(np.mean(r) - b * mu_s)
        fitted = a + b * s
        spec = {**base, "kind": "LINEAR", "intercept": a, "slope": b,
                "posterior_shrink": shrink, "confidence": shrink}
    else:
        raise ValueError("unknown calibration %r" % (method,))

    spec["state"] = "OK"
    spec["dispersion"] = _residual_dispersion(r - fitted, s)
    spec["calibration_slope"] = slope
    spec["fitted_dispersion"] = float(np.std(fitted, ddof=1)) \
        if fitted.size > 2 else 0.0
    spec["realised_dispersion"] = float(np.std(r, ddof=1))
    return spec


def apply(spec: dict, score: np.ndarray) -> dict:
    """Map evaluation scores through a fitted calibration.

    Returns the three quantities the sizing layer needs. An unfitted or
    degenerate calibration returns the training mean with the training
    dispersion and zero confidence, which makes every downstream sizing rule
    treat it as no information rather than as a confident zero.
    """
    s = np.asarray(score, dtype=np.float64)
    kind = spec.get("kind")

    if kind == "CONSTANT":
        er = np.full(s.size, float(spec.get("constant", 0.0)))
    elif kind == "LINEAR":
        er = float(spec["intercept"]) + float(spec["slope"]) * s
    elif kind == "ISOTONIC":
        ks = np.asarray(spec["knots_score"], dtype=np.float64)
        kv = np.asarray(spec["knots_value"], dtype=np.float64)
        er = np.interp(s, ks, kv, left=kv[0], right=kv[-1])
    elif kind == "RANK_BUCKET":
        edges = np.asarray(spec["edges"], dtype=np.float64)
        means = np.asarray(spec["bucket_mean"], dtype=np.float64)
        pos = np.clip(np.searchsorted(edges, s, side="right") - 1, 0,
                      means.size - 1)
        er = means[pos]
    else:
        er = np.full(s.size, float(spec.get("training_mean", 0.0)))

    disp = spec.get("dispersion") or {}
    pooled = float(disp.get("pooled") or 0.0)
    if not np.isfinite(pooled) or pooled <= 0:
        pooled = 1e-4
    if disp.get("edges") and disp.get("by_bucket"):
        edges = np.asarray(disp["edges"], dtype=np.float64)
        by = np.asarray(disp["by_bucket"], dtype=np.float64)
        pos = np.clip(np.searchsorted(edges, s, side="right") - 1, 0,
                      by.size - 1)
        unc = np.where(np.isfinite(by[pos]) & (by[pos] > 0), by[pos], pooled)
    else:
        unc = np.full(s.size, pooled)

    er = np.where(np.isfinite(er), er, float(spec.get("training_mean", 0.0)))
    return {"expected_return": er,
            "uncertainty": np.clip(unc, 1e-5, None),
            "confidence": float(spec.get("confidence", 0.0))}


def rank_only(score: np.ndarray, tradable: np.ndarray = None) -> np.ndarray:
    """The MAGNITUDE-FREE comparison: the score reduced to its rank.

    This is the other half of the declared test of whether forecast magnitude
    carries information beyond rank. The same book is built from the calibrated
    expected return and from this, and the difference in after-cost excess
    utility is the answer. If it is not positive, only rank survived and this
    release says so.
    """
    s = np.asarray(score, dtype=np.float64)
    ok = np.isfinite(s)
    if tradable is not None:
        ok = ok & np.asarray(tradable, dtype=bool)
    out = np.zeros(s.size)
    if ok.sum() < 2:
        return out
    r = np.argsort(np.argsort(s[ok])).astype(np.float64)
    out[ok] = r / max(r.max(), 1.0) - 0.5
    return out


def reliability(expected: np.ndarray, realised: np.ndarray, *, bins: int = 10
                ) -> dict:
    """The calibration curve: predicted mean versus observed mean, by bin.

    A slope of one and an intercept of zero is a perfectly calibrated forecast.
    A slope well below one - the usual finding - says the model's magnitudes are
    too big and that sizing on them directly will over-trade.
    """
    e = np.asarray(expected, dtype=np.float64)
    r = np.asarray(realised, dtype=np.float64)
    ok = np.isfinite(e) & np.isfinite(r)
    e, r = e[ok], r[ok]
    if e.size < 50 or float(np.std(e, ddof=1)) <= 0:
        return {"state": "INSUFFICIENT_OBSERVATIONS", "n": int(e.size)}
    slope = float(np.cov(e, r, ddof=1)[0, 1] / np.var(e, ddof=1))
    intercept = float(np.mean(r) - slope * np.mean(e))
    edges = np.quantile(e, np.linspace(0.0, 1.0, bins + 1))
    edges[0], edges[-1] = -np.inf, np.inf
    curve = []
    for k in range(bins):
        m = (e >= edges[k]) & (e < edges[k + 1])
        if m.sum() < 10:
            continue
        curve.append({"bin": k, "n": int(m.sum()),
                      "mean_expected": float(e[m].mean()),
                      "mean_realised": float(r[m].mean())})
    return {"state": "OK", "n": int(e.size),
            "calibration_slope": slope, "calibration_intercept": intercept,
            "perfect_slope": 1.0, "curve": curve}
