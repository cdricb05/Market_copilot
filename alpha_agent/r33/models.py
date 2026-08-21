"""alpha_agent.r33.models - the Release 33 model families.

Ridge, elastic net, gradient-boosted trees and extremely randomised trees are
NOT re-implemented here. They are imported from :mod:`alpha_agent.r31.learners`
and :mod:`alpha_agent.release30_models`, which are the released owners, so there
is exactly one implementation of each in this repository. Release 33 adds only
what the prediction problems it poses actually need and the estate does not
already have:

* **partial pooling across markets**, the core Lane A hypothesis. Independent
  per-market estimation throws away the fact that markets share dynamics; full
  pooling pretends they are identical. Hierarchical shrinkage is the middle,
  and it is the thing being tested;
* **a logistic head**, because a probability-of-positive-return forecast scored
  by log loss and Brier needs a calibrated probability rather than a regression
  output squeezed through a sigmoid after the fact;
* **volatility models**, because a variance forecast scored by QLIKE is a
  different object from a return forecast;
* **a Gaussian hidden Markov model** whose state probabilities are FILTERED,
  never smoothed.

That last point is the one that quietly destroys regime studies. The smoothed
probability ``P(S_t | all data)`` conditions on the future, so a strategy driven
by it can appear to time regimes perfectly while being unimplementable. Only the
filtered ``P(S_t | data up to t)`` is admissible here, and
:func:`hmm_filter_states` is the only way this module exposes state beliefs.

Every learner is a pure function of ``(X, y, params, seed)`` returning a plain
dict SPEC, so a fitted model is JSON-serialisable evidence and a spec hash is a
real idempotency key.
"""
from __future__ import annotations

import math

import numpy as np

from ..r31 import learners as _l

CALCULATION_OWNER = "alpha_agent.r33.models"

# Released learners, re-exported. One implementation each.
fit_ridge = _l.fit_ridge
fit_elastic_net = _l.fit_elastic_net
fit_gbrt = _l.fit_gbrt
fit_extra_trees = _l.fit_extra_trees
predict_released = _l.predict

KIND_MEAN = "unconditional_mean"
KIND_ZERO = "zero_forecast"
KIND_RULE = "transparent_rule"
KIND_LINEAR_IC = "linear_with_intercept"
KIND_HIERARCHICAL = "hierarchical_shrinkage"
KIND_LOGISTIC = "ridge_logistic"
KIND_VOL = "volatility_model"
KIND_HMM = "gaussian_hmm"
KIND_BLEND = "blend"


# --------------------------------------------------------------------------- #
# Standardisation - training statistics ONLY
# --------------------------------------------------------------------------- #
def fit_scaler(X: np.ndarray) -> dict:
    """Column centre/scale computed on TRAINING rows only.

    Scaling a validation block by its own mean and standard deviation is a
    subtle leak: the model is then told something about the distribution of the
    period it is being tested on.
    """
    mu = np.nanmean(X, axis=0)
    sd = np.nanstd(X, axis=0, ddof=1)
    sd = np.where(np.isfinite(sd) & (sd > 1e-12), sd, 1.0)
    mu = np.where(np.isfinite(mu), mu, 0.0)
    return {"mu": [float(v) for v in mu], "sd": [float(v) for v in sd]}


def apply_scaler(scaler: dict, X: np.ndarray) -> np.ndarray:
    mu = np.asarray(scaler["mu"], dtype=np.float64)
    sd = np.asarray(scaler["sd"], dtype=np.float64)
    Z = (np.asarray(X, dtype=np.float64) - mu[None, :]) / sd[None, :]
    return np.clip(np.where(np.isfinite(Z), Z, 0.0), -8.0, 8.0)


def _with_intercept(X: np.ndarray) -> np.ndarray:
    return np.column_stack([np.ones(len(X)), X])


# --------------------------------------------------------------------------- #
# Baselines
# --------------------------------------------------------------------------- #
def fit_unconditional_mean(y: np.ndarray) -> dict:
    v = np.asarray(y, dtype=np.float64)
    v = v[np.isfinite(v)]
    return {"kind": KIND_MEAN, "mean": float(v.mean()) if v.size else 0.0}


def fit_zero() -> dict:
    return {"kind": KIND_ZERO}


def fit_transparent_rule(*, rule: str, feature_names, scale: float = 1.0) -> dict:
    """A named, unfitted rule. The honest baseline a challenger must beat."""
    return {"kind": KIND_RULE, "rule": str(rule), "scale": float(scale),
            "feature_names": list(feature_names)}


RULE_TSMOM = "TIME_SERIES_MOMENTUM_12_1"
RULE_VOL_SCALED_TREND = "VOLATILITY_SCALED_TREND"
RULE_XS_MOMENTUM = "CROSS_SECTIONAL_MOMENTUM"
RULES = (RULE_TSMOM, RULE_VOL_SCALED_TREND, RULE_XS_MOMENTUM)


def _rule_predict(spec: dict, X: np.ndarray) -> np.ndarray:
    names = list(spec["feature_names"])
    idx = {n: i for i, n in enumerate(names)}
    rule = spec["rule"]
    if rule == RULE_TSMOM:
        j = idx.get("tsmom_12_1")
        raw = X[:, j] if j is not None else np.zeros(len(X))
        return np.sign(raw) * float(spec["scale"])
    if rule == RULE_VOL_SCALED_TREND:
        j = idx.get("trend_norm_252")
        raw = X[:, j] if j is not None else np.zeros(len(X))
        return np.clip(raw, -3.0, 3.0) * float(spec["scale"])
    if rule == RULE_XS_MOMENTUM:
        j = idx.get("xs_rank_252")
        raw = X[:, j] if j is not None else np.zeros(len(X))
        return raw * float(spec["scale"])
    return np.zeros(len(X))


# --------------------------------------------------------------------------- #
# Linear with intercept
# --------------------------------------------------------------------------- #
def fit_linear(X: np.ndarray, y: np.ndarray, *, alpha: float) -> dict:
    """Ridge WITH an intercept.

    The released ``fit_ridge`` deliberately has no intercept because its target
    is cross-sectionally demeaned. Release 33 also predicts RAW excess returns,
    which have a non-zero unconditional mean, so the constant has to be
    estimated rather than assumed away.
    """
    Xi = _with_intercept(np.asarray(X, dtype=np.float64))
    yv = np.asarray(y, dtype=np.float64)
    ok = np.isfinite(yv) & np.isfinite(Xi).all(axis=1)
    Xi, yv = Xi[ok], yv[ok]
    if Xi.shape[0] <= Xi.shape[1]:
        return {"kind": KIND_LINEAR_IC, "coef": [0.0] * Xi.shape[1],
                "alpha": float(alpha), "n": int(Xi.shape[0])}
    pen = float(alpha) * np.eye(Xi.shape[1])
    pen[0, 0] = 0.0  # never penalise the intercept
    coef = np.linalg.solve(Xi.T @ Xi + pen * max(1.0, Xi.shape[0] / 1000.0),
                           Xi.T @ yv)
    return {"kind": KIND_LINEAR_IC, "coef": [float(c) for c in coef],
            "alpha": float(alpha), "n": int(Xi.shape[0])}


def _linear_predict(spec: dict, X: np.ndarray) -> np.ndarray:
    return _with_intercept(np.asarray(X, dtype=np.float64)) @ np.asarray(
        spec["coef"], dtype=np.float64)


# --------------------------------------------------------------------------- #
# Hierarchical shrinkage - the pooling hypothesis
# --------------------------------------------------------------------------- #
def fit_hierarchical(X: np.ndarray, y: np.ndarray, groups: np.ndarray, *,
                     alpha: float, shrink: float,
                     min_group_rows: int = 200) -> dict:
    """Partial pooling: each group's coefficients pulled toward the pooled fit.

    ``shrink = 1`` is full pooling (every market shares one model), ``shrink =
    0`` is independent estimation per group, and the interesting answers are in
    between. A group with too few rows is given the pooled fit outright rather
    than a noisy one of its own.
    """
    X = np.asarray(X, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    g = np.asarray(groups)
    pooled = fit_linear(X, y, alpha=alpha)
    b_pool = np.asarray(pooled["coef"], dtype=np.float64)
    lam = float(np.clip(shrink, 0.0, 1.0))
    per_group, counts = {}, {}
    for key in np.unique(g):
        m = g == key
        n = int(m.sum())
        counts[str(key)] = n
        if n < int(min_group_rows):
            per_group[str(key)] = [float(v) for v in b_pool]
            continue
        b_g = np.asarray(fit_linear(X[m], y[m], alpha=alpha)["coef"],
                         dtype=np.float64)
        per_group[str(key)] = [float(v)
                               for v in (lam * b_pool + (1.0 - lam) * b_g)]
    return {"kind": KIND_HIERARCHICAL, "pooled_coef": [float(v) for v in b_pool],
            "group_coef": per_group, "group_rows": counts,
            "alpha": float(alpha), "shrink": lam,
            "min_group_rows": int(min_group_rows)}


def _hierarchical_predict(spec: dict, X: np.ndarray,
                          groups: np.ndarray) -> np.ndarray:
    Xi = _with_intercept(np.asarray(X, dtype=np.float64))
    b_pool = np.asarray(spec["pooled_coef"], dtype=np.float64)
    out = np.empty(len(Xi), dtype=np.float64)
    g = np.asarray(groups)
    for key in np.unique(g):
        m = g == key
        b = np.asarray(spec["group_coef"].get(str(key), b_pool),
                       dtype=np.float64)
        out[m] = Xi[m] @ b
    return out


# --------------------------------------------------------------------------- #
# Logistic head
# --------------------------------------------------------------------------- #
def fit_logistic(X: np.ndarray, y: np.ndarray, *, alpha: float,
                 max_iter: int = 50, tol: float = 1e-8) -> dict:
    """Ridge-penalised logistic regression by IRLS, for calibrated probability."""
    Xi = _with_intercept(np.asarray(X, dtype=np.float64))
    yv = np.asarray(y, dtype=np.float64)
    ok = np.isfinite(yv) & np.isfinite(Xi).all(axis=1)
    Xi, yv = Xi[ok], yv[ok]
    n, k = Xi.shape
    if n <= k:
        base = float(yv.mean()) if yv.size else 0.5
        coef = np.zeros(k)
        coef[0] = math.log(max(base, 1e-6) / max(1.0 - base, 1e-6))
        return {"kind": KIND_LOGISTIC, "coef": [float(c) for c in coef],
                "alpha": float(alpha), "n": int(n)}
    pen = float(alpha) * np.eye(k)
    pen[0, 0] = 0.0
    beta = np.zeros(k)
    beta[0] = math.log(max(yv.mean(), 1e-6) / max(1.0 - yv.mean(), 1e-6))
    for _ in range(int(max_iter)):
        eta = np.clip(Xi @ beta, -30.0, 30.0)
        p = 1.0 / (1.0 + np.exp(-eta))
        w = np.clip(p * (1.0 - p), 1e-6, None)
        z = eta + (yv - p) / w
        WX = Xi * w[:, None]
        try:
            new = np.linalg.solve(Xi.T @ WX + pen, WX.T @ z)
        except np.linalg.LinAlgError:
            break
        if not np.isfinite(new).all():
            break
        if float(np.max(np.abs(new - beta))) < tol:
            beta = new
            break
        beta = new
    return {"kind": KIND_LOGISTIC, "coef": [float(c) for c in beta],
            "alpha": float(alpha), "n": int(n)}


def _logistic_predict(spec: dict, X: np.ndarray) -> np.ndarray:
    eta = np.clip(_with_intercept(np.asarray(X, dtype=np.float64))
                  @ np.asarray(spec["coef"], dtype=np.float64), -30.0, 30.0)
    return 1.0 / (1.0 + np.exp(-eta))


# --------------------------------------------------------------------------- #
# Volatility models
# --------------------------------------------------------------------------- #
VOL_TRAILING = "TRAILING_REALISED"
VOL_EWMA = "EWMA"
VOL_HAR = "HAR_LOG"
VOL_MODELS = (VOL_TRAILING, VOL_EWMA, VOL_HAR)


def fit_volatility(X: np.ndarray, y: np.ndarray, *, model: str,
                   feature_names, alpha: float = 1.0) -> dict:
    """Fit a variance forecast. HAR regresses log realised vol on log trailing
    vol at several horizons, which is the standard and is honest about the
    strong persistence that makes volatility the easiest of these targets."""
    names = list(feature_names)
    idx = {n: i for i, n in enumerate(names)}
    if model in (VOL_TRAILING, VOL_EWMA):
        return {"kind": KIND_VOL, "model": str(model),
                "feature_names": names,
                "col_21": idx.get("vol_21"), "col_63": idx.get("vol_63")}
    cols = [idx.get(n) for n in ("vol_21", "vol_63", "vol_ratio_21_63",
                                 "downside_vol_63", "g_vix_level")]
    cols = [c for c in cols if c is not None]
    Z = np.log(np.clip(np.asarray(X, dtype=np.float64)[:, cols], 1e-6, None))
    yv = np.log(np.clip(np.asarray(y, dtype=np.float64), 1e-6, None))
    ok = np.isfinite(yv) & np.isfinite(Z).all(axis=1)
    lin = fit_linear(Z[ok], yv[ok], alpha=alpha)
    return {"kind": KIND_VOL, "model": VOL_HAR, "feature_names": names,
            "cols": cols, "linear": lin}


def _volatility_predict(spec: dict, X: np.ndarray) -> np.ndarray:
    X = np.asarray(X, dtype=np.float64)
    if spec["model"] == VOL_TRAILING:
        j = spec.get("col_63")
        return X[:, j] if j is not None else np.full(len(X), np.nan)
    if spec["model"] == VOL_EWMA:
        a, b = spec.get("col_21"), spec.get("col_63")
        if a is None or b is None:
            return np.full(len(X), np.nan)
        return 0.6 * X[:, a] + 0.4 * X[:, b]
    Z = np.log(np.clip(X[:, spec["cols"]], 1e-6, None))
    return np.exp(np.clip(_linear_predict(spec["linear"], Z), -10.0, 3.0))


# --------------------------------------------------------------------------- #
# Gaussian hidden Markov model - FILTERED states only
# --------------------------------------------------------------------------- #
def fit_hmm(Z: np.ndarray, *, n_states: int, seed: int = 33,
            max_iter: int = 60, tol: float = 1e-6) -> dict:
    """Baum-Welch for a Gaussian HMM with diagonal covariance.

    Fitted on TRAINING observations only. The fitted parameters are then used to
    FILTER later data forward; the model is never refitted on the period it is
    evaluated on.
    """
    Z = np.asarray(Z, dtype=np.float64)
    Z = np.where(np.isfinite(Z), Z, 0.0)
    n, d = Z.shape
    k = int(n_states)
    rng = np.random.default_rng(int(seed))
    mu = Z[rng.choice(n, size=k, replace=False)] if n >= k else np.zeros((k, d))
    var = np.tile(np.var(Z, axis=0, ddof=1) + 1e-6, (k, 1))
    trans = np.full((k, k), 1.0 / k)
    start = np.full(k, 1.0 / k)
    prev_ll = -np.inf

    for _ in range(int(max_iter)):
        B = _gaussian_density(Z, mu, var)
        alpha, c = _forward(B, trans, start)
        beta = _backward(B, trans, c)
        gamma = alpha * beta
        gamma /= np.clip(gamma.sum(axis=1, keepdims=True), 1e-300, None)
        xi = np.zeros((k, k))
        for t in range(n - 1):
            num = (alpha[t][:, None] * trans * B[t + 1][None, :]
                   * beta[t + 1][None, :])
            xi += num / max(num.sum(), 1e-300)
        start = gamma[0] / max(gamma[0].sum(), 1e-300)
        trans = xi / np.clip(xi.sum(axis=1, keepdims=True), 1e-300, None)
        w = gamma / np.clip(gamma.sum(axis=0, keepdims=True), 1e-300, None)
        mu = w.T @ Z
        for j in range(k):
            diff = Z - mu[j][None, :]
            var[j] = np.clip(w[:, j] @ (diff ** 2), 1e-8, None)
        ll = float(-np.sum(np.log(np.clip(c, 1e-300, None))))
        if abs(ll - prev_ll) < tol:
            break
        prev_ll = ll

    return {"kind": KIND_HMM, "n_states": k,
            "mu": mu.tolist(), "var": var.tolist(),
            "trans": trans.tolist(), "start": start.tolist(),
            "log_likelihood": prev_ll, "seed": int(seed),
            "states_are_filtered_only": True}


def _gaussian_density(Z, mu, var):
    k, d = mu.shape
    out = np.empty((len(Z), k))
    for j in range(k):
        diff = Z - mu[j][None, :]
        logp = -0.5 * (np.sum(diff ** 2 / var[j][None, :], axis=1)
                       + np.sum(np.log(2.0 * np.pi * var[j])))
        out[:, j] = np.exp(np.clip(logp, -700.0, 700.0))
    return np.clip(out, 1e-300, None)


def _forward(B, trans, start):
    n, k = B.shape
    alpha = np.zeros((n, k))
    c = np.zeros(n)
    a = start * B[0]
    c[0] = max(a.sum(), 1e-300)
    alpha[0] = a / c[0]
    for t in range(1, n):
        a = (alpha[t - 1] @ trans) * B[t]
        c[t] = max(a.sum(), 1e-300)
        alpha[t] = a / c[t]
    return alpha, c


def _backward(B, trans, c):
    n, k = B.shape
    beta = np.zeros((n, k))
    beta[-1] = 1.0
    for t in range(n - 2, -1, -1):
        beta[t] = (trans @ (B[t + 1] * beta[t + 1])) / c[t + 1]
    return beta


def hmm_filter_states(spec: dict, Z: np.ndarray) -> np.ndarray:
    """FILTERED ``P(S_t | data up to t)``. The only admissible state belief.

    The smoothed probability conditions on the future. A regime strategy driven
    by smoothed states can look like flawless market timing while being
    impossible to trade, which is the single most common way a regime study
    fools itself.
    """
    Z = np.asarray(Z, dtype=np.float64)
    Z = np.where(np.isfinite(Z), Z, 0.0)
    mu = np.asarray(spec["mu"], dtype=np.float64)
    var = np.asarray(spec["var"], dtype=np.float64)
    trans = np.asarray(spec["trans"], dtype=np.float64)
    start = np.asarray(spec["start"], dtype=np.float64)
    B = _gaussian_density(Z, mu, var)
    alpha, _c = _forward(B, trans, start)
    return alpha


# --------------------------------------------------------------------------- #
# Blending
# --------------------------------------------------------------------------- #
def fit_blend(members: list, weights: list) -> dict:
    return {"kind": KIND_BLEND,
            "members": [{"spec": s, "weight": float(w)}
                        for s, w in zip(members, weights)]}


# --------------------------------------------------------------------------- #
# One dispatch
# --------------------------------------------------------------------------- #
def predict(spec: dict, X: np.ndarray, *, groups=None,
            feature_names=None) -> np.ndarray:
    kind = spec.get("kind")
    if kind == KIND_ZERO:
        out = np.zeros(len(X))
    elif kind == KIND_MEAN:
        out = np.full(len(X), float(spec["mean"]))
    elif kind == KIND_RULE:
        out = _rule_predict(spec, X)
    elif kind == KIND_LINEAR_IC:
        out = _linear_predict(spec, X)
    elif kind == KIND_HIERARCHICAL:
        out = _hierarchical_predict(spec, X, groups)
    elif kind == KIND_LOGISTIC:
        out = _logistic_predict(spec, X)
    elif kind == KIND_VOL:
        out = _volatility_predict(spec, X)
    elif kind == KIND_BLEND:
        out = np.zeros(len(X))
        for m in spec["members"]:
            out = out + float(m["weight"]) * predict(
                m["spec"], X, groups=groups, feature_names=feature_names)
    else:
        out = predict_released(spec, np.asarray(X, dtype=np.float64),
                               list(feature_names or ()))
    out = np.asarray(out, dtype=np.float64)
    return np.where(np.isfinite(out), out, 0.0)
