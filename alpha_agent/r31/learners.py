"""alpha_agent.r31.learners - the Release 31 mathematics library.

Every learner is implemented here in ``numpy`` rather than pulled from a
modelling framework, for three reasons that the campaign contract depends on:

* the project declares no modelling dependency, and the whole research lane
  (Stage 23 through Release 30) is numpy/pandas only - installing scipy and
  scikit-learn to reproduce a paper would change the environment the operational
  system runs beside;
* determinism. Every function below is a pure function of ``(X, y, params,
  seed)``. The same triple produces byte-identical coefficients, which is what
  makes a candidate spec hash a real idempotency key;
* auditability. A reviewer can read the objective being optimised. That matters
  more here than convenience, because the campaign's whole claim is that its
  measurements are defensible.

``fit_ridge``, ``fit_gbrt``, ``fit_extra_trees`` and the rank-blend spec are NOT
re-implemented - they are the released Release-30 owners in
``alpha_agent.release30_models``, re-exported so there is exactly one
implementation of each.

Every learner returns a plain dict SPEC. ``predict(spec, X)`` is the only way a
spec is consumed, so a fitted model is JSON-serialisable evidence rather than a
live object.
"""
from __future__ import annotations

import math

import numpy as np

from .. import release30_models as _rm

# Released Release-30 learners, re-exported. One implementation each.
fit_ridge = _rm.fit_ridge
fit_gbrt = _rm.fit_gbrt
fit_extra_trees = _rm.fit_extra_trees
rank_blend_spec = _rm.rank_blend_spec
standardise = _rm.standardise

KIND_LINEAR = _rm.KIND_LINEAR
KIND_TREE_ENSEMBLE = _rm.KIND_TREE_ENSEMBLE
KIND_RANK_BLEND = _rm.KIND_RANK_BLEND
KIND_ENSEMBLE = _rm.KIND_ENSEMBLE
KIND_NEURAL = "neural_net"
KIND_QUANTILE = "quantile_linear"
KIND_COMBINATION = "combination"
KIND_DIRECT_PORTFOLIO = "direct_portfolio"

MAX_ABS_PREDICTION = 1e6


# --------------------------------------------------------------------------- #
# Elastic net (Zou & Hastie 2005) - cyclic coordinate descent
# --------------------------------------------------------------------------- #
def fit_elastic_net(X: np.ndarray, y: np.ndarray, *, alpha: float,
                    l1_ratio: float, max_iter: int = 500,
                    tol: float = 1e-9) -> dict:
    """Coordinate descent on ``0.5/n * ||y - Xb||^2 + a*(r*||b||_1 + (1-r)/2*||b||^2)``.

    No intercept: the target is cross-sectionally demeaned per date by
    construction, so a fitted constant could only absorb sampling noise.
    """
    n, f = X.shape
    b = np.zeros(f, dtype=np.float64)
    col_sq = (X * X).sum(axis=0) / n
    resid = y.copy()
    a1 = float(alpha) * float(l1_ratio)
    a2 = float(alpha) * (1.0 - float(l1_ratio))
    for _ in range(int(max_iter)):
        delta = 0.0
        for j in range(f):
            if col_sq[j] <= 0.0:
                continue
            rho = float(X[:, j] @ resid) / n + col_sq[j] * b[j]
            new = (np.sign(rho) * max(abs(rho) - a1, 0.0)) / (col_sq[j] + a2)
            diff = new - b[j]
            if diff != 0.0:
                resid -= diff * X[:, j]
                b[j] = new
                delta = max(delta, abs(diff))
        if delta < tol:
            break
    return {"kind": KIND_LINEAR, "coef": [float(c) for c in b],
            "alpha": float(alpha), "l1_ratio": float(l1_ratio),
            "objective": "ELASTIC_NET"}


# --------------------------------------------------------------------------- #
# Huber robust linear regression - IRLS
# --------------------------------------------------------------------------- #
def fit_huber(X: np.ndarray, y: np.ndarray, *, delta: float, alpha: float,
              max_iter: int = 60, tol: float = 1e-10) -> dict:
    """Iteratively reweighted least squares on the Huber loss.

    Cross-sectional return tails are fat; a squared loss lets a handful of
    extreme realisations set the coefficients. Huber bounds each row's influence
    while staying convex.
    """
    n, f = X.shape
    b = np.zeros(f, dtype=np.float64)
    ridge = float(alpha) * np.eye(f) * max(1.0, n / 1000.0)
    for _ in range(int(max_iter)):
        r = y - X @ b
        s = float(np.median(np.abs(r - np.median(r)))) * 1.4826
        s = s if s > 1e-12 else 1.0
        z = np.abs(r) / (float(delta) * s)
        w = np.where(z <= 1.0, 1.0, 1.0 / np.maximum(z, 1e-12))
        xw = X * w[:, None]
        new = np.linalg.solve(X.T @ xw + ridge, xw.T @ y)
        if float(np.max(np.abs(new - b))) < tol:
            b = new
            break
        b = new
    return {"kind": KIND_LINEAR, "coef": [float(c) for c in b],
            "delta": float(delta), "alpha": float(alpha), "objective": "HUBER"}


# --------------------------------------------------------------------------- #
# Dimension reduction: principal component regression and PLS
# --------------------------------------------------------------------------- #
def fit_dimension_reduction(X: np.ndarray, y: np.ndarray, *, n_components: int,
                            variant: str) -> dict:
    """PCR (Stock & Watson) or PLS (Wold), collapsed back to a linear spec.

    Both reduce to a coefficient vector in the ORIGINAL feature space, so the
    frozen artifact is one dense vector and scoring never needs the training
    covariance.
    """
    n, f = X.shape
    k = int(max(1, min(int(n_components), f)))
    if variant == "PCR":
        cov = (X.T @ X) / max(n - 1, 1)
        vals, vecs = np.linalg.eigh(cov)
        order = np.argsort(vals)[::-1][:k]
        P = vecs[:, order]
        Z = X @ P
        g = np.linalg.solve(Z.T @ Z + 1e-8 * np.eye(k), Z.T @ y)
        coef = P @ g
    elif variant == "PLS":
        Xr, yr = X.copy(), y.copy()
        W = np.zeros((f, k))
        P = np.zeros((f, k))
        q = np.zeros(k)
        for a in range(k):
            w = Xr.T @ yr
            nw = float(np.linalg.norm(w))
            if nw <= 1e-12:
                W, P, q = W[:, :a], P[:, :a], q[:a]
                k = a
                break
            w = w / nw
            t = Xr @ w
            tt = float(t @ t)
            if tt <= 1e-12:
                W, P, q = W[:, :a], P[:, :a], q[:a]
                k = a
                break
            p = (Xr.T @ t) / tt
            qa = float(t @ yr) / tt
            W[:, a], P[:, a], q[a] = w, p, qa
            Xr = Xr - np.outer(t, p)
            yr = yr - qa * t
        if k == 0:
            coef = np.zeros(f)
        else:
            R = W @ np.linalg.pinv(P.T @ W)
            coef = R @ q
    else:
        raise ValueError("unknown dimension-reduction variant %r" % (variant,))
    return {"kind": KIND_LINEAR, "coef": [float(c) for c in coef],
            "n_components": int(k), "variant": str(variant),
            "objective": "DIMENSION_REDUCTION"}


# --------------------------------------------------------------------------- #
# Fama-MacBeth (1973) cross-sectional regression
# --------------------------------------------------------------------------- #
def fit_fama_macbeth(blocks, *, shrink: float = 0.0) -> dict:
    """Average of PERIOD-BY-PERIOD cross-sectional slopes.

    ``blocks`` is an iterable of ``(X_t, y_t)``. Estimating one regression per
    date and averaging the slopes - rather than pooling every row - is the whole
    point of the method: pooled OLS would weight a date with more names more
    heavily and would understate the standard error, because cross-sectional
    residuals are correlated within a date.
    """
    slopes = []
    for Xt, yt in blocks:
        if Xt.shape[0] < 10:
            continue
        try:
            g = np.linalg.solve(Xt.T @ Xt + 1e-8 * np.eye(Xt.shape[1]), Xt.T @ yt)
        except np.linalg.LinAlgError:
            continue
        if np.all(np.isfinite(g)):
            slopes.append(g)
    if not slopes:
        raise ValueError("Fama-MacBeth received no usable cross-section")
    S = np.vstack(slopes)
    mean = S.mean(axis=0)
    if shrink > 0.0:
        se = S.std(axis=0, ddof=1) / math.sqrt(S.shape[0])
        t = np.where(se > 0, mean / se, 0.0)
        keep = np.abs(t) >= float(shrink)
        mean = np.where(keep, mean, 0.0)
    return {"kind": KIND_LINEAR, "coef": [float(c) for c in mean],
            "n_periods": int(S.shape[0]), "shrink_t": float(shrink),
            "objective": "FAMA_MACBETH"}


# --------------------------------------------------------------------------- #
# Random forest (Breiman 2001) - bagged, optimised splits
# --------------------------------------------------------------------------- #
def fit_random_forest(X: np.ndarray, y: np.ndarray, *, n_trees: int,
                      max_depth: int, min_leaf: int = 300,
                      feature_fraction: float = 0.5, sample_fraction: float = 0.7,
                      n_bins: int = 32, seed: int = 31) -> dict:
    """Bootstrap-aggregated trees with OPTIMISED splits.

    This is materially distinct from the released extremely-randomised forest:
    there the split threshold is drawn at random and the randomness is the
    regulariser; here each split is chosen to maximise squared-error reduction
    and the BAGGING is the regulariser.
    """
    edges = _rm._quantile_edges(X, n_bins)
    B = _rm._bin_matrix(X, edges)
    rng = np.random.default_rng(int(seed))
    n, f = X.shape
    k = max(1, int(round(feature_fraction * f)))
    m = max(50, int(round(sample_fraction * n)))
    trees = []
    for _ in range(int(n_trees)):
        rows = rng.choice(n, size=m, replace=True)
        cols = np.sort(rng.choice(f, size=k, replace=False))
        trees.append(_rm._grow_tree(B[rows], y[rows], max_depth=int(max_depth),
                                    n_bins=n_bins + 1, min_leaf=int(min_leaf),
                                    l2=0.0, feat_idx=cols))
    return {"kind": KIND_TREE_ENSEMBLE, "edges": edges, "trees": trees,
            "shrinkage_applied": False, "n_trees": int(n_trees),
            "max_depth": int(max_depth), "objective": "RANDOM_FOREST"}


# --------------------------------------------------------------------------- #
# Shallow feed-forward network (Gu, Kelly & Xiu 2020, NN1-NN3 scale)
# --------------------------------------------------------------------------- #
def fit_neural_net(X: np.ndarray, y: np.ndarray, *, hidden, epochs: int = 60,
                   batch: int = 4096, lr: float = 1e-3, weight_decay: float = 1e-4,
                   seed: int = 31) -> dict:
    """One to three hidden ReLU layers trained with Adam and weight decay.

    Deliberately small. Cross-sectional return prediction is a low
    signal-to-noise problem with a few hundred thousand rows; a large network
    memorises noise, and the published asset-pricing result is that shallow
    networks dominate deep ones on exactly this data.
    """
    rng = np.random.default_rng(int(seed))
    dims = [X.shape[1]] + [int(h) for h in hidden] + [1]
    Ws, bs = [], []
    for i in range(len(dims) - 1):
        lim = math.sqrt(2.0 / dims[i])
        Ws.append(rng.normal(0.0, lim, size=(dims[i], dims[i + 1])))
        bs.append(np.zeros(dims[i + 1]))
    mW = [np.zeros_like(w) for w in Ws]
    vW = [np.zeros_like(w) for w in Ws]
    mb = [np.zeros_like(b) for b in bs]
    vb = [np.zeros_like(b) for b in bs]
    b1, b2, eps = 0.9, 0.999, 1e-8
    n = X.shape[0]
    step = 0
    ys = y.reshape(-1, 1)
    for _ in range(int(epochs)):
        perm = rng.permutation(n)
        for s in range(0, n, int(batch)):
            idx = perm[s:s + int(batch)]
            xb, yb = X[idx], ys[idx]
            acts = [xb]
            for i in range(len(Ws) - 1):
                acts.append(np.maximum(acts[-1] @ Ws[i] + bs[i], 0.0))
            out = acts[-1] @ Ws[-1] + bs[-1]
            g = 2.0 * (out - yb) / max(len(idx), 1)
            step += 1
            for i in range(len(Ws) - 1, -1, -1):
                gW = acts[i].T @ g + float(weight_decay) * Ws[i]
                gb = g.sum(axis=0)
                if i > 0:
                    g = (g @ Ws[i].T) * (acts[i] > 0.0)
                mW[i] = b1 * mW[i] + (1 - b1) * gW
                vW[i] = b2 * vW[i] + (1 - b2) * gW * gW
                mb[i] = b1 * mb[i] + (1 - b1) * gb
                vb[i] = b2 * vb[i] + (1 - b2) * gb * gb
                mh = mW[i] / (1 - b1 ** step)
                vh = vW[i] / (1 - b2 ** step)
                Ws[i] -= float(lr) * mh / (np.sqrt(vh) + eps)
                mh = mb[i] / (1 - b1 ** step)
                vh = vb[i] / (1 - b2 ** step)
                bs[i] -= float(lr) * mh / (np.sqrt(vh) + eps)
    return {"kind": KIND_NEURAL,
            "W": [w.tolist() for w in Ws], "b": [v.tolist() for v in bs],
            "hidden": [int(h) for h in hidden], "epochs": int(epochs),
            "lr": float(lr), "weight_decay": float(weight_decay),
            "objective": "SQUARED_ERROR_SHALLOW_MLP"}


def _predict_neural(spec: dict, X: np.ndarray) -> np.ndarray:
    a = X
    Ws = [np.asarray(w, dtype=np.float64) for w in spec["W"]]
    bs = [np.asarray(b, dtype=np.float64) for b in spec["b"]]
    for i in range(len(Ws) - 1):
        a = np.maximum(a @ Ws[i] + bs[i], 0.0)
    return (a @ Ws[-1] + bs[-1]).ravel()


# --------------------------------------------------------------------------- #
# Linear quantile regression (Koenker & Bassett 1978) - distributional
# --------------------------------------------------------------------------- #
def fit_quantile(X: np.ndarray, y: np.ndarray, *, tau, alpha: float = 1.0,
                 epochs: int = 300, lr: float = 0.05, seed: int = 31) -> dict:
    """Pinball-loss linear regression at one or more quantiles.

    This is the campaign's distributional family. A point mean forecast throws
    away the shape of the conditional distribution, and an allocator that cares
    about downside needs a lower quantile, not a mean. Optimised by smooth
    subgradient descent with a ridge penalty, which is deterministic given the
    seed and needs no linear-programming solver.
    """
    taus = [float(t) for t in (tau if isinstance(tau, (list, tuple)) else [tau])]
    n, f = X.shape
    rng = np.random.default_rng(int(seed))
    coefs = {}
    scale = float(np.std(y)) or 1.0
    for t in taus:
        b = np.zeros(f, dtype=np.float64)
        m = np.zeros(f)
        v = np.zeros(f)
        for e in range(int(epochs)):
            r = y - X @ b
            ind = np.where(r >= 0.0, t, t - 1.0)
            g = -(X.T @ ind) / n + float(alpha) * b / max(n, 1)
            m = 0.9 * m + 0.1 * g
            v = 0.999 * v + 0.001 * g * g
            b -= float(lr) * scale * (m / (1 - 0.9 ** (e + 1))) / (
                np.sqrt(v / (1 - 0.999 ** (e + 1))) + 1e-8)
        coefs[str(t)] = [float(c) for c in b]
    _ = rng
    return {"kind": KIND_QUANTILE, "coef_by_tau": coefs, "taus": taus,
            "alpha": float(alpha), "objective": "PINBALL"}


def _predict_quantile(spec: dict, X: np.ndarray, *, use_tau=None) -> np.ndarray:
    taus = spec["taus"]
    t = str(use_tau if use_tau is not None else taus[len(taus) // 2])
    if t not in spec["coef_by_tau"]:
        t = str(taus[len(taus) // 2])
    return X @ np.asarray(spec["coef_by_tau"][t], dtype=np.float64)


# --------------------------------------------------------------------------- #
# Direct portfolio decision learning (Track B)
# --------------------------------------------------------------------------- #
def _softmax_with_cash(s: np.ndarray, cash_logit: float) -> np.ndarray:
    """Softmax over ``[scores, cash_logit]``. The LAST entry is cash.

    Cash is a competing asset inside the same allocation, not a leftover. Without
    it the book is forced to be fully invested on every date, which is exactly the
    defect that superseded Campaign v2's judge - repeating it inside the learner
    would mean Track B could never propose holding nothing.
    """
    z = np.concatenate([np.asarray(s, dtype=np.float64), [float(cash_logit)]])
    z = z - z.max()
    e = np.exp(z)
    return e / e.sum()


def _align_previous(prev_w: dict, symbols: np.ndarray) -> np.ndarray:
    """Previous book weights aligned to THIS date's securities, by identity.

    A name absent from the previous book aligns to 0.0 - it is a genuine BUY.
    Names in the previous book that are absent today are exits: they contribute a
    real cost, but that cost does not depend on today's parameters, so it enters
    the objective's VALUE and not its gradient. Both facts are asserted by
    ``tests/test_release31_campaign_v3_corrections.py``.
    """
    return np.array([float(prev_w.get(str(sym), 0.0)) for sym in symbols],
                    dtype=np.float64)


def fit_direct_portfolio(blocks, *, n_features: int, gamma: float,
                         cost_rate: float, temperature: float,
                         epochs: int = 200, lr: float = 0.02,
                         l2: float = 1e-3, cash_logit: float = 0.0,
                         seed: int = 31) -> dict:
    """Learn a linear score whose induced PORTFOLIO maximises net utility.

    ``blocks`` is an ordered iterable of ``(X_t, r_t, symbols_t)``. Weights are a
    softmax over the score AND a cash unit, which is the differentiable stand-in
    for "hold the best names, or hold nothing":

        [w_t, cash_t] = softmax([temperature * X_t b, c])

    and the objective maximised is

        mean_t[ p_t ] - (gamma/2) * var_t(p_t) - cost_rate * mean_t[ turnover_t ]

    where ``p_t = w_t . r_t`` (cash earns zero, the canonical paper assumption)
    and turnover is measured over the UNION OF SECURITY IDENTITIES:

        turnover_t = sum_i | w_i,t - w_i,t-1 |      i ranges over symbols

    THE SYMBOL ALIGNMENT IS THE POINT. Campaign v2 compared consecutive weight
    vectors positionally whenever their lengths happened to match. Row order is an
    artifact of cross-section assembly, index membership changes month to month
    and names delist, so position ``i`` is simply not the same company on two
    dates. Positional comparison reports a book that sold everything and bought
    something else entirely as having done nothing - and a learner rewarded for
    low turnover then learns to exploit that fiction rather than to trade less.

    This is the campaign's Track-B family: trained against implementable
    portfolio economics INCLUDING the cost of the trade it implies, not against
    prediction error. A model can therefore be rewarded for a smaller, steadier
    edge that survives its own turnover - something an MSE objective cannot see.
    """
    rng = np.random.default_rng(int(seed))
    b = rng.normal(0.0, 0.01, size=int(n_features))
    c = float(cash_logit)
    m, v = np.zeros_like(b), np.zeros_like(b)
    mc = vc = 0.0

    data = []
    for blk in blocks:
        if len(blk) != 3:
            raise ValueError(
                "the direct-portfolio learner requires (X_t, r_t, symbols_t); a "
                "two-element block cannot carry security identity, and turnover "
                "without identity is the v2 defect this signature exists to "
                "prevent")
        X, r, syms = blk
        X = np.asarray(X, dtype=np.float64)
        r = np.asarray(r, dtype=np.float64)
        data.append((X, np.where(np.isfinite(r), r, 0.0),
                     np.asarray([str(s) for s in syms])))
    if not data:
        raise ValueError("direct portfolio learner received no block")
    T = float(temperature)

    for e in range(int(epochs)):
        # Pass 1: the portfolio return series under the current parameters. The
        # variance penalty is a property of the SERIES, so it cannot be evaluated
        # one period at a time.
        ports = np.empty(len(data), dtype=np.float64)
        books = []
        for i, (X, r, syms) in enumerate(data):
            full = _softmax_with_cash(T * (X @ b), c)
            w = full[:-1]
            ports[i] = float(w @ r)
            books.append((w, float(full[-1])))
        mbar = float(ports.mean())

        # Pass 2: gradients. d/dtheta [ mean - (gamma/2) var ] contributes the
        # factor (1 - gamma*(p_t - mean)) to each period, because the derivative
        # of the mean inside the variance term cancels against the deviations.
        grad = np.zeros_like(b)
        grad_c = 0.0
        prev_w: dict = {}
        for i, (X, r, syms) in enumerate(data):
            w, w_cash = books[i]
            port = ports[i]
            coef = 1.0 - float(gamma) * (port - mbar)

            dstock = w * (r - port)            # d port / d s_j
            g = T * (X.T @ dstock) * coef
            g_c = (w_cash * (0.0 - port)) * coef

            # Turnover, aligned BY SECURITY IDENTITY over the union.
            prev_aligned = _align_previous(prev_w, syms)
            sign = np.sign(w - prev_aligned)
            sw = float(sign @ w)
            g = g - float(cost_rate) * T * (X.T @ (w * (sign - sw)))
            g_c = g_c - float(cost_rate) * (w_cash * (0.0 - sw))

            grad += g
            grad_c += g_c
            prev_w = {str(s): float(x) for s, x in zip(syms, w) if x > 0.0}

        grad = grad / len(data) - float(l2) * b
        grad_c = grad_c / len(data)
        m = 0.9 * m + 0.1 * grad
        v = 0.999 * v + 0.001 * grad * grad
        b += float(lr) * (m / (1 - 0.9 ** (e + 1))) / (
            np.sqrt(v / (1 - 0.999 ** (e + 1))) + 1e-8)
        mc = 0.9 * mc + 0.1 * grad_c
        vc = 0.999 * vc + 0.001 * grad_c * grad_c
        c += float(lr) * (mc / (1 - 0.9 ** (e + 1))) / (
            math.sqrt(vc / (1 - 0.999 ** (e + 1))) + 1e-8)

    return {"kind": KIND_DIRECT_PORTFOLIO, "coef": [float(x) for x in b],
            "cash_logit": float(c), "gamma": float(gamma),
            "cost_rate": float(cost_rate), "temperature": float(temperature),
            "turnover_alignment": "BY_SECURITY_IDENTITY_NEVER_BY_ARRAY_POSITION",
            "cash_is_a_competing_asset": True,
            "objective": "DIRECT_NET_PORTFOLIO_UTILITY"}


def portfolio_weights(spec: dict, X: np.ndarray) -> np.ndarray:
    """Track-B output: proposed STOCK weights for one cross-section.

    Sums to ``1 - cash``, never forced to 1. The judge passes these through the
    same canonical feasibility seam Track A's allocation faces, so neither track
    is judged under looser rules than the other.
    """
    b = np.asarray(spec["coef"], dtype=np.float64)
    s = float(spec["temperature"]) * (np.asarray(X, dtype=np.float64) @ b)
    return _softmax_with_cash(s, float(spec.get("cash_logit", 0.0)))[:-1]


# --------------------------------------------------------------------------- #
# Forecast combination / stacking (Bates & Granger 1969; Rapach et al. 2010)
# --------------------------------------------------------------------------- #
def combination_spec(members: list, weights: list) -> dict:
    """A weighted blend of already-fitted member specs.

    Members are STANDARDISED before weighting, so no member gains influence
    merely by predicting on a larger scale.
    """
    return {"kind": KIND_COMBINATION,
            "members": [{"spec": s, "weight": float(w)}
                        for s, w in zip(members, weights)],
            "objective": "FORECAST_COMBINATION"}


# --------------------------------------------------------------------------- #
# Dispatch
# --------------------------------------------------------------------------- #
def predict(spec: dict, X: np.ndarray, feature_names=None) -> np.ndarray:
    kind = spec.get("kind")
    if kind == KIND_DIRECT_PORTFOLIO:
        # Track B emits WEIGHTS, not a score. Returning them through the same
        # entry point keeps one dispatch, and the judge routes them to the
        # feasibility seam rather than to a calibration.
        out = portfolio_weights(spec, X)
    elif kind == KIND_NEURAL:
        out = _predict_neural(spec, X)
    elif kind == KIND_QUANTILE:
        out = _predict_quantile(spec, X)
    elif kind == KIND_COMBINATION:
        out = np.zeros(X.shape[0], dtype=np.float64)
        for m in spec["members"]:
            w = float(m["weight"])
            if w != 0.0:
                out = out + w * standardise(predict(m["spec"], X, feature_names))
    else:
        out = _rm.predict(spec, X, feature_names or ())
    out = np.asarray(out, dtype=np.float64)
    bad = ~np.isfinite(out)
    if bad.any():
        out = np.where(bad, 0.0, out)
    return np.clip(out, -MAX_ABS_PREDICTION, MAX_ABS_PREDICTION)
