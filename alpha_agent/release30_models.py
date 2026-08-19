"""alpha_agent/release30_models.py - Release 30 forecasting learners.

RESEARCH ONLY. Training lives here, in the research lane, because it needs
``numpy``; the operational ``engine/return_forecast.py`` kernel is pure stdlib
and only ever CONSUMES the frozen artifacts these learners emit. That split is
the established Paper Trader pattern (Phase 29D.2): research owns the
mathematics, the operational owner owns validation, promotion and semantics.

Every learner here is deliberately modest. Asset returns are low-signal and
high-noise, so the tournament favours regularisation, stability, calibration and
reproducibility over training fit. Nothing here promotes a model, writes an
operational artifact, or touches a portfolio.

Determinism contract: every learner takes an explicit integer ``seed`` and uses
``numpy.random.default_rng``. The same (data, spec, seed) triple always produces
byte-identical artifacts, which is what
``tests/test_release30_return_forecast.py`` asserts.
"""
from __future__ import annotations

import math

import numpy as np

MODEL_CONTRACT = "release30_forecast_model/1"

#: Learner kinds a frozen artifact may declare. The stdlib kernel refuses any
#: other value rather than guessing.
KIND_LINEAR = "linear"
KIND_TREE_ENSEMBLE = "tree_ensemble"
KIND_RANK_BLEND = "rank_blend"
KIND_ENSEMBLE = "ensemble"
KINDS = (KIND_LINEAR, KIND_TREE_ENSEMBLE, KIND_RANK_BLEND, KIND_ENSEMBLE)


# --------------------------------------------------------------------------- #
# Regularised linear (ridge)
# --------------------------------------------------------------------------- #
def fit_ridge(X: np.ndarray, y: np.ndarray, *, alpha: float) -> dict:
    """Closed-form ridge on already cross-sectionally normalised features.

    No intercept: the target is cross-sectionally demeaned by construction, so a
    fitted constant would only absorb sampling noise.
    """
    n, f = X.shape
    xtx = X.T @ X
    xty = X.T @ y
    coef = np.linalg.solve(xtx + float(alpha) * np.eye(f) * max(1.0, n / 1000.0),
                           xty)
    return {"kind": KIND_LINEAR, "coef": [float(c) for c in coef],
            "alpha": float(alpha)}


def predict_linear(spec: dict, X: np.ndarray) -> np.ndarray:
    return X @ np.asarray(spec["coef"], dtype=np.float64)


# --------------------------------------------------------------------------- #
# Histogram gradient-boosted regression trees
# --------------------------------------------------------------------------- #
def _quantile_edges(X: np.ndarray, n_bins: int) -> list:
    """Bin edges from the TRAINING rows only. Stored in the artifact so scoring
    a later cross-section can never re-derive edges from its own distribution."""
    edges = []
    qs = np.linspace(0.0, 1.0, n_bins + 1)[1:-1]
    for j in range(X.shape[1]):
        col = X[:, j]
        e = np.unique(np.quantile(col, qs))
        edges.append([float(v) for v in e])
    return edges


def _bin_matrix(X: np.ndarray, edges: list) -> np.ndarray:
    out = np.empty(X.shape, dtype=np.int16)
    for j, e in enumerate(edges):
        out[:, j] = np.searchsorted(np.asarray(e, dtype=np.float64), X[:, j],
                                    side="left") if e else 0
    return out


def _grow_tree(B: np.ndarray, g: np.ndarray, *, max_depth: int, n_bins: int,
               min_leaf: int, l2: float, feat_idx: np.ndarray) -> dict:
    """One depth-limited regression tree over pre-binned features.

    Split gain is the standard squared-error reduction with an L2 leaf penalty.
    Ties resolve to the lowest feature index and lowest bin, so the tree is a
    pure function of its inputs.
    """
    feat: list = []
    thr: list = []
    left: list = []
    right: list = []
    value: list = []

    def _leaf(rows) -> int:
        node = len(feat)
        feat.append(-1)
        thr.append(0)
        left.append(-1)
        right.append(-1)
        s = float(g[rows].sum())
        value.append(s / (len(rows) + l2) if len(rows) else 0.0)
        return node

    def _build(rows, depth) -> int:
        if depth >= max_depth or len(rows) < 2 * min_leaf:
            return _leaf(rows)
        gg = g[rows]
        total = float(gg.sum())
        cnt = len(rows)
        best = (0.0, -1, -1)
        for j in feat_idx:
            hist_g = np.bincount(B[rows, j], weights=gg, minlength=n_bins)
            hist_n = np.bincount(B[rows, j], minlength=n_bins)
            cg = np.cumsum(hist_g)
            cn = np.cumsum(hist_n)
            ok = (cn >= min_leaf) & ((cnt - cn) >= min_leaf)
            ok[-1] = False
            if not ok.any():
                continue
            gain = np.where(
                ok, cg * cg / np.maximum(cn + l2, 1e-12)
                + (total - cg) ** 2 / np.maximum(cnt - cn + l2, 1e-12)
                - total * total / max(cnt + l2, 1e-12), -np.inf)
            b = int(np.argmax(gain))
            if gain[b] > best[0] + 1e-12:
                best = (float(gain[b]), int(j), b)
        if best[1] < 0:
            return _leaf(rows)
        _, j, b = best
        mask = B[rows, j] <= b
        lrows = rows[mask]
        rrows = rows[~mask]
        node = len(feat)
        feat.append(int(j))
        thr.append(int(b))
        left.append(-1)
        right.append(-1)
        value.append(0.0)
        left[node] = _build(lrows, depth + 1)
        right[node] = _build(rrows, depth + 1)
        return node

    _build(np.arange(B.shape[0]), 0)
    return {"feat": feat, "thr": thr, "left": left, "right": right,
            "value": value}


def _tree_predict(tree: dict, B: np.ndarray) -> np.ndarray:
    feat = np.asarray(tree["feat"])
    thr = np.asarray(tree["thr"])
    left = np.asarray(tree["left"])
    right = np.asarray(tree["right"])
    value = np.asarray(tree["value"], dtype=np.float64)
    node = np.zeros(B.shape[0], dtype=np.int64)
    active = feat[node] >= 0
    while active.any():
        idx = np.nonzero(active)[0]
        j = feat[node[idx]]
        go_left = B[idx, j] <= thr[node[idx]]
        node[idx] = np.where(go_left, left[node[idx]], right[node[idx]])
        active = feat[node] >= 0
    return value[node]


def fit_gbrt(X: np.ndarray, y: np.ndarray, *, n_trees: int, max_depth: int,
             learning_rate: float, n_bins: int = 32, min_leaf: int = 200,
             l2: float = 10.0, feature_fraction: float = 0.8,
             seed: int = 30) -> dict:
    edges = _quantile_edges(X, n_bins)
    B = _bin_matrix(X, edges)
    rng = np.random.default_rng(int(seed))
    pred = np.zeros(X.shape[0], dtype=np.float64)
    n_feat = X.shape[1]
    k = max(1, int(round(feature_fraction * n_feat)))
    trees = []
    for _ in range(int(n_trees)):
        resid = y - pred
        cols = np.sort(rng.choice(n_feat, size=k, replace=False))
        tree = _grow_tree(B, resid, max_depth=int(max_depth), n_bins=n_bins + 1,
                          min_leaf=int(min_leaf), l2=float(l2), feat_idx=cols)
        pred += float(learning_rate) * _tree_predict(tree, B)
        trees.append(tree)
    return {"kind": KIND_TREE_ENSEMBLE, "edges": edges, "trees": trees,
            "learning_rate": float(learning_rate), "shrinkage_applied": True,
            "n_trees": int(n_trees), "max_depth": int(max_depth)}


def predict_tree_ensemble(spec: dict, X: np.ndarray) -> np.ndarray:
    B = _bin_matrix(X, spec["edges"])
    out = np.zeros(X.shape[0], dtype=np.float64)
    lr = float(spec.get("learning_rate", 1.0))
    scale = lr if spec.get("shrinkage_applied") else 1.0 / max(1, len(spec["trees"]))
    for tree in spec["trees"]:
        out += scale * _tree_predict(tree, B)
    return out


# --------------------------------------------------------------------------- #
# Extremely randomised trees
# --------------------------------------------------------------------------- #
def fit_extra_trees(X: np.ndarray, y: np.ndarray, *, n_trees: int,
                    max_depth: int, min_leaf: int = 400,
                    feature_fraction: float = 0.7, n_bins: int = 32,
                    seed: int = 30) -> dict:
    """Extremely randomised trees: split thresholds are drawn, not optimised.

    The randomness is the regulariser. Each tree is fit to the RAW target and the
    forest averages, so the artifact declares no shrinkage.
    """
    edges = _quantile_edges(X, n_bins)
    B = _bin_matrix(X, edges)
    rng = np.random.default_rng(int(seed))
    n_feat = X.shape[1]
    k = max(1, int(round(feature_fraction * n_feat)))
    trees = []
    for _ in range(int(n_trees)):
        cols = np.sort(rng.choice(n_feat, size=k, replace=False))
        trees.append(_grow_random_tree(B, y, rng, max_depth=int(max_depth),
                                       n_bins=n_bins + 1, min_leaf=int(min_leaf),
                                       feat_idx=cols))
    return {"kind": KIND_TREE_ENSEMBLE, "edges": edges, "trees": trees,
            "shrinkage_applied": False, "n_trees": int(n_trees),
            "max_depth": int(max_depth)}


def _grow_random_tree(B, y, rng, *, max_depth, n_bins, min_leaf, feat_idx) -> dict:
    feat: list = []
    thr: list = []
    left: list = []
    right: list = []
    value: list = []

    def _leaf(rows) -> int:
        node = len(feat)
        feat.append(-1)
        thr.append(0)
        left.append(-1)
        right.append(-1)
        value.append(float(y[rows].mean()) if len(rows) else 0.0)
        return node

    def _build(rows, depth) -> int:
        if depth >= max_depth or len(rows) < 2 * min_leaf:
            return _leaf(rows)
        for _ in range(8):
            j = int(feat_idx[rng.integers(0, len(feat_idx))])
            col = B[rows, j]
            lo, hi = int(col.min()), int(col.max())
            if hi <= lo:
                continue
            b = int(rng.integers(lo, hi))
            mask = col <= b
            nl = int(mask.sum())
            if nl < min_leaf or (len(rows) - nl) < min_leaf:
                continue
            node = len(feat)
            feat.append(j)
            thr.append(b)
            left.append(-1)
            right.append(-1)
            value.append(0.0)
            left[node] = _build(rows[mask], depth + 1)
            right[node] = _build(rows[~mask], depth + 1)
            return node
        return _leaf(rows)

    _build(np.arange(B.shape[0]), 0)
    return {"feat": feat, "thr": thr, "left": left, "right": right,
            "value": value}


# --------------------------------------------------------------------------- #
# Rank blends (the frozen operational benchmark and its legs)
# --------------------------------------------------------------------------- #
def rank_blend_spec(weights: dict) -> dict:
    """A fixed weighted sum of already-normalised feature columns.

    This is the shape of the frozen operational champion
    ``fundamental_momentum_50_50_v1``: constant weights, no fitting, no data. It
    is included in the tournament as a BENCHMARK and is never re-estimated.
    """
    return {"kind": KIND_RANK_BLEND,
            "weights": {str(k): float(v) for k, v in weights.items()}}


def predict_rank_blend(spec: dict, X: np.ndarray, feature_names) -> np.ndarray:
    idx = {n: i for i, n in enumerate(feature_names)}
    out = np.zeros(X.shape[0], dtype=np.float64)
    for name, w in spec["weights"].items():
        j = idx.get(name)
        if j is not None:
            out += float(w) * X[:, j]
    return out


# --------------------------------------------------------------------------- #
# Dispatch
# --------------------------------------------------------------------------- #
def predict(spec: dict, X: np.ndarray, feature_names) -> np.ndarray:
    kind = spec.get("kind")
    if kind == KIND_LINEAR:
        return predict_linear(spec, X)
    if kind == KIND_TREE_ENSEMBLE:
        return predict_tree_ensemble(spec, X)
    if kind == KIND_RANK_BLEND:
        return predict_rank_blend(spec, X, feature_names)
    if kind == KIND_ENSEMBLE:
        # Members are STANDARDISED before they are weighted. Without this a
        # learner would gain influence merely by predicting on a larger scale,
        # and the walk-forward weights would no longer mean what they say.
        out = np.zeros(X.shape[0], dtype=np.float64)
        for member in spec["members"]:
            w = float(member["weight"])
            if w == 0.0:
                continue
            out += w * standardise(predict(member["spec"], X, feature_names))
        return out
    raise ValueError("unknown model kind: %r" % (kind,))


def member_predictions(spec: dict, X: np.ndarray, feature_names) -> list:
    """Per-member predictions of an ensemble, used for disagreement-based
    uncertainty. A non-ensemble spec yields a single-element list."""
    if spec.get("kind") != KIND_ENSEMBLE:
        return [standardise(predict(spec, X, feature_names))]
    return [standardise(predict(m["spec"], X, feature_names))
            for m in spec["members"] if float(m["weight"]) != 0.0]


def standardise(pred: np.ndarray) -> np.ndarray:
    """Cross-sectional standardisation of one date's raw model output.

    Every learner is free to work on its own scale; the forecast layer compares
    and blends them only after this per-date normalisation, so no learner can win
    influence merely by predicting on a larger scale.
    """
    m = float(np.nanmean(pred)) if pred.size else 0.0
    s = float(np.nanstd(pred, ddof=1)) if pred.size > 1 else 0.0
    if not math.isfinite(s) or s <= 0:
        return np.zeros_like(pred)
    return (pred - m) / s
