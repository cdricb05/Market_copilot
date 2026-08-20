"""alpha_agent.r31.novel - bounded original alpha research.

This layer asks whether the system can find mathematical structure the known
methods miss. It is NOT random formula mining, and the difference is enforced
rather than asserted:

* the GRAMMAR is frozen before a single candidate runs, and its hash is recorded
  in the campaign artifacts;
* candidate GENERATION never sees an evaluation result - each family is a
  deterministic enumeration from a recorded seed, so no candidate is proposed
  because an earlier one scored well on the layer that will judge it;
* candidate JUDGING is the same frozen judge every other candidate faces;
* every expression is complexity-penalised and bounded in depth, so the search
  cannot buy fit with structure;
* the budget is hard: two campaigns, 150 candidates each, refinement depth 3.
  A second null campaign is a terminal result, not an invitation to widen the
  grammar.

Six families, each answering a different question about what the incumbent and
the known methods might be missing.
"""
from __future__ import annotations

import math
from typing import Callable, Optional

import numpy as np

from .. import r31
from . import contract as _contract
from . import judge as _judge
from . import learners as _L
from . import methods as _methods
from . import registry as _registry

CALCULATION_OWNER = "alpha_agent.r31.novel"
GRAMMAR_SCHEMA = "r31_novel_discovery_contract/1"
RESULTS_SCHEMA = "r31_novel_discovery_results/1"
GRAMMAR_ARTIFACT = "novel_discovery_contract.json"
RESULTS_ARTIFACT = "novel_discovery_results.json"

FAM_RESIDUAL = "novel_residual_alpha"
FAM_REGIME = "novel_regime_conditional"
FAM_HORIZON = "novel_horizon_surface"
FAM_SYMBOLIC = "novel_symbolic_interaction"
FAM_RELATIONAL = "novel_cross_sectional_relational"
FAM_DECISION = "novel_direct_decision"
FAMILIES = (FAM_RESIDUAL, FAM_REGIME, FAM_HORIZON, FAM_SYMBOLIC,
            FAM_RELATIONAL, FAM_DECISION)

#: The frozen primitive set. Every symbolic expression is built from these and
#: nothing else.
PRIMITIVES = ("add", "sub", "mul", "safe_div", "rank", "threshold",
              "interaction", "neutralise_within_group", "decay_blend", "sign_gate")

#: Complexity bounds. Depth 2 over at most 3 base features keeps every
#: expression readable and keeps the search space finite and enumerable.
MAX_EXPRESSION_DEPTH = 2
MAX_BASE_FEATURES = 3

#: Per-family candidate allocation for campaign N1.
#:
#: The contract's ceiling is 150 per campaign and 300 in total; the EXECUTED
#: allocation is far below both, for the reason recorded in
#: ``contract.EXECUTED_GRID_POLICY``: the v3 judge allocates capital through the
#: canonical zero-base optimiser at every decision date, so a candidate costs
#: minutes rather than milliseconds to judge. All SIX families are retained -
#: narrowing the GRAMMAR would change what the search can express, which is a
#: different and much worse trade than sampling each family less densely - and
#: the per-family shares keep the same ordering the dense allocation had.
PER_CAMPAIGN = 30
N1_ALLOCATION = {FAM_RESIDUAL: 6, FAM_REGIME: 5, FAM_HORIZON: 4,
                 FAM_SYMBOLIC: 8, FAM_RELATIONAL: 4, FAM_DECISION: 3}
#: Campaign N2 re-allocates the same total toward whichever families showed the
#: least-bad DISCOVERY behaviour. The ALLOCATION may change between campaigns;
#: the grammar and the budget may not.
N2_ALLOCATION = {FAM_SYMBOLIC: 10, FAM_RESIDUAL: 7, FAM_RELATIONAL: 6,
                 FAM_REGIME: 4, FAM_DECISION: 3}


# --------------------------------------------------------------------------- #
# Point-in-time market state (for regime conditioning)
# --------------------------------------------------------------------------- #
def market_history(snap, sample: str, sections: list) -> np.ndarray:
    """Realised benchmark return between consecutive decision dates.

    Entry ``j`` is the equal-weight return of the eligible cross-section realised
    between section ``j`` and section ``j+1``. It is fully RESOLVED by the time
    section ``j+1`` is struck, so conditioning date ``i`` on entries ``0..i-1``
    reads only the past. Deriving the regime from the panel's own realised
    returns - rather than from an external index - also keeps the campaign inside
    the owned, survivorship-free data.
    """
    out = np.full(len(sections), np.nan)
    for j, k in enumerate(sections):
        _raw, bench = snap.holding_returns(sample, k, _judge.HOLD_SESSIONS)
        out[j] = bench
    return out


def regime_at(mkt: np.ndarray, i: int, *, kind: str, lookback: int = 12) -> int:
    """A PIT regime label at section index ``i``, from entries ``< i`` only."""
    lo = max(0, i - int(lookback))
    hist = mkt[lo:i]
    hist = hist[np.isfinite(hist)]
    if hist.size < 4:
        return 0
    if kind == "TREND":
        return 1 if float(hist.mean()) > 0.0 else 0
    if kind == "VOLATILITY":
        prior = mkt[:i]
        prior = prior[np.isfinite(prior)]
        if prior.size < 8:
            return 0
        return 1 if float(hist.std(ddof=1)) > float(np.median(
            np.abs(prior - prior.mean()))) * 1.4826 else 0
    if kind == "DRAWDOWN":
        eq = np.cumprod(1.0 + mkt[:i][np.isfinite(mkt[:i])])
        if eq.size < 8:
            return 0
        return 1 if float(eq[-1] / np.maximum.accumulate(eq)[-1] - 1.0) < -0.10 else 0
    return 0


# --------------------------------------------------------------------------- #
# Symbolic expressions
# --------------------------------------------------------------------------- #
def _safe_div(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    with np.errstate(invalid="ignore", divide="ignore"):
        out = a / np.where(np.abs(b) > 1e-6, b, np.nan)
    return np.where(np.isfinite(out), out, 0.0)


def apply_expression(expr: dict, X: np.ndarray, idx: dict) -> np.ndarray:
    """Evaluate one frozen-grammar expression against a rank-normalised block."""
    op = expr["op"]
    if op == "feature":
        return X[:, idx[expr["a"]]]
    a = apply_expression(expr["a"], X, idx) if isinstance(expr["a"], dict) else X[:, idx[expr["a"]]]
    if op == "rank":
        return _rank01(a)
    if op == "sign_gate":
        b = X[:, idx[expr["b"]]]
        return a * np.sign(b)
    if op == "threshold":
        return np.where(a > float(expr["c"]), a, 0.0)
    b = apply_expression(expr["b"], X, idx) if isinstance(expr["b"], dict) else X[:, idx[expr["b"]]]
    if op == "add":
        return a + b
    if op == "sub":
        return a - b
    if op == "mul":
        return a * b
    if op == "safe_div":
        return _safe_div(a, b)
    if op == "interaction":
        return a * b - float(np.mean(a * b))
    raise ValueError("unknown primitive %r" % (op,))


def _rank01(v: np.ndarray) -> np.ndarray:
    order = np.argsort(np.argsort(v, kind="stable"), kind="stable")
    n = max(len(v) - 1, 1)
    return order.astype(np.float64) / n - 0.5


def expression_complexity(expr: dict) -> int:
    if not isinstance(expr, dict) or expr.get("op") == "feature":
        return 1
    n = 1
    for k in ("a", "b"):
        v = expr.get(k)
        if isinstance(v, dict):
            n += expression_complexity(v)
        elif v is not None:
            n += 1
    return n


def enumerate_expressions(feats: tuple, limit: int, *, seed: int) -> list:
    """Deterministic enumeration of the frozen grammar, capped at ``limit``.

    Enumeration order is fixed by the feature order and the primitive order, and
    the RNG only shuffles a deterministic list, so the same seed always yields
    the same candidates in the same order.
    """
    out: list = []
    binaries = ("mul", "sub", "safe_div", "interaction")
    for i, fa in enumerate(feats):
        for j, fb in enumerate(feats):
            if j <= i:
                continue
            for op in binaries:
                out.append({"op": op, "a": fa, "b": fb})
            out.append({"op": "sign_gate", "a": fa, "b": fb})
    for fa in feats:
        for c in (-0.25, 0.0, 0.25):
            out.append({"op": "threshold", "a": fa, "b": None, "c": c})
    # depth-2: combine a binary with a third base feature
    depth1 = [e for e in out if e["op"] in binaries]
    for e in depth1[: max(1, limit // 2)]:
        for fc in feats:
            if fc in (e["a"], e["b"]):
                continue
            out.append({"op": "mul", "a": dict(e), "b": fc})
    out = [e for e in out if expression_complexity(e) <= 2 * MAX_BASE_FEATURES]
    rng = np.random.default_rng(int(seed))
    idx = rng.permutation(len(out))
    return [out[int(i)] for i in idx[:int(limit)]]


# --------------------------------------------------------------------------- #
# Candidate generators - each returns (params, predictor_factory)
# --------------------------------------------------------------------------- #
def _residual_candidates(n: int, feats: tuple, seed: int) -> list:
    base_learners = [("ridge", {"alpha": a}) for a in (10.0, 30.0, 100.0)]
    base_learners += [("gbrt", {"n_trees": 120, "max_depth": 2,
                                "learning_rate": 0.02})]
    lams = (0.25, 0.5, 1.0)
    incumbents = ({"mom_6_1": 1.0}, {"mom_12_1": 1.0},
                  {"mom_6_1": 0.5, "trend_200": 0.5})
    out = []
    for inc in incumbents:
        for lname, lp in base_learners:
            for lam in lams:
                out.append({"incumbent": inc, "learner": lname, "params": lp,
                            "lambda": lam})
    return out[:n]


def _regime_candidates(n: int, feats: tuple, seed: int) -> list:
    out = []
    for kind in ("TREND", "VOLATILITY", "DRAWDOWN"):
        for lb in (6, 12, 24):
            for lname, lp in (("ridge", {"alpha": 30.0}),
                              ("ridge", {"alpha": 100.0}),
                              ("elastic_net", {"alpha": 1e-4, "l1_ratio": 0.5})):
                out.append({"regime_kind": kind, "lookback": lb,
                            "learner": lname, "params": lp})
    return out[:n]


def _horizon_candidates(n: int, feats: tuple, seed: int) -> list:
    out = []
    for w in ((0.5, 0.3, 0.2), (0.2, 0.5, 0.3), (0.34, 0.33, 0.33),
              (0.6, 0.4, 0.0), (0.0, 0.5, 0.5)):
        for lname, lp in (("ridge", {"alpha": 30.0}),
                          ("ridge", {"alpha": 100.0}),
                          ("gbrt", {"n_trees": 120, "max_depth": 2,
                                    "learning_rate": 0.02}),
                          ("elastic_net", {"alpha": 1e-4, "l1_ratio": 0.5})):
            out.append({"horizon_weights": list(w), "learner": lname,
                        "params": lp})
    return out[:n]


def _symbolic_candidates(n: int, feats: tuple, seed: int) -> list:
    exprs = enumerate_expressions(feats, n, seed=seed)
    return [{"expression": e, "complexity": expression_complexity(e)}
            for e in exprs]


def _relational_candidates(n: int, feats: tuple, seed: int) -> list:
    out = []
    for group_by in ("log_adv_20", "beta_252", "vol_252"):
        if group_by not in feats:
            continue
        for n_groups in (3, 5):
            for lname, lp in (("ridge", {"alpha": 30.0}),
                              ("ridge", {"alpha": 100.0}),
                              ("gbrt", {"n_trees": 120, "max_depth": 2,
                                        "learning_rate": 0.02}),
                              ("elastic_net", {"alpha": 1e-4, "l1_ratio": 0.5})):
                out.append({"group_by": group_by, "n_groups": n_groups,
                            "learner": lname, "params": lp})
    return out[:n]


def _decision_candidates(n: int, feats: tuple, seed: int) -> list:
    out = []
    for hidden in ((6,), (12,)):
        for gamma in (0.5, 1.0, 3.0):
            for temp in (3.0, 8.0):
                out.append({"hidden": list(hidden), "gamma": gamma,
                            "temperature": temp, "epochs": 120})
    return out[:n]


GENERATORS = {
    FAM_RESIDUAL: _residual_candidates,
    FAM_REGIME: _regime_candidates,
    FAM_HORIZON: _horizon_candidates,
    FAM_SYMBOLIC: _symbolic_candidates,
    FAM_RELATIONAL: _relational_candidates,
    FAM_DECISION: _decision_candidates,
}


# --------------------------------------------------------------------------- #
# Predictor factories
# --------------------------------------------------------------------------- #
def predictor_factory(family: str, params: dict, *, snap, sample, horizon,
                      seed: int, mkt: np.ndarray) -> Callable:
    """Build the ``predictor_factory`` the campaign runner expects."""

    def _factory(*, sections, part, feats):
        idx = {n: i for i, n in enumerate(feats)}
        cap = part["validation"][-1] if part["validation"] else 0
        emb = part["embargo_dates"]
        warm = part["discovery"] + part["validation"]

        if family == FAM_SYMBOLIC:
            expr = params["expression"]

            def _p_sym(k, X, adv, syms):
                return apply_expression(expr, X, idx)
            return _p_sym

        if family == FAM_RESIDUAL:
            inc = _L.rank_blend_spec(params["incumbent"])
            base = _fitter(params["learner"], params["params"])
            cache: dict = {}

            def _p_res(k, X, adv, syms):
                li = _methods._index_of(sections, k)
                bucket = _bucket(li, emb, cap)
                if bucket not in cache:
                    tr = _train_indices(warm, bucket)
                    if tr is None:
                        return np.full(X.shape[0], np.nan, dtype=np.float64)
                    Xs, ys = [], []
                    for i in tr:
                        Xb, yb, _a, _s = snap.block(sample, sections[i], feats, horizon)
                        ok = np.isfinite(yb) & np.isfinite(Xb).all(axis=1)
                        if int(ok.sum()) < _contract.MIN_CROSS_SECTION:
                            continue
                        base_pred = _L.standardise(_L.predict(inc, Xb[ok], feats))
                        Xs.append(Xb[ok])
                        ys.append(yb[ok] - float(np.std(yb[ok])) * base_pred)
                    if not Xs:
                        return np.zeros(X.shape[0])
                    cache[bucket] = base(np.vstack(Xs), np.concatenate(ys), seed)
                resid = _L.standardise(_L.predict(cache[bucket], X, feats))
                return _L.standardise(_L.predict(inc, X, feats)) + \
                    float(params["lambda"]) * resid
            return _p_res

        if family == FAM_REGIME:
            base = _fitter(params["learner"], params["params"])
            cache: dict = {}

            def _p_reg(k, X, adv, syms):
                li = _methods._index_of(sections, k)
                bucket = _bucket(li, emb, cap)
                g = regime_at(mkt, li, kind=params["regime_kind"],
                              lookback=int(params["lookback"]))
                key = (bucket, g)
                if key not in cache:
                    tr = _train_indices(warm, bucket)
                    if tr is None:
                        return np.full(X.shape[0], np.nan, dtype=np.float64)
                    tr = [i for i in tr
                          if regime_at(mkt, i, kind=params["regime_kind"],
                                       lookback=int(params["lookback"])) == g]
                    Xs, ys = [], []
                    for i in tr:
                        Xb, yb, _a, _s = snap.block(sample, sections[i], feats, horizon)
                        ok = np.isfinite(yb) & np.isfinite(Xb).all(axis=1)
                        if int(ok.sum()) >= _contract.MIN_CROSS_SECTION:
                            Xs.append(Xb[ok])
                            ys.append(yb[ok])
                    if len(Xs) < 6:
                        cache[key] = None
                    else:
                        cache[key] = base(np.vstack(Xs), np.concatenate(ys), seed)
                spec = cache[key]
                return (np.zeros(X.shape[0]) if spec is None
                        else _L.predict(spec, X, feats))
            return _p_reg

        if family == FAM_HORIZON:
            base = _fitter(params["learner"], params["params"])
            cache: dict = {}
            hs = list(_contract.HORIZONS)
            ws = [float(w) for w in params["horizon_weights"]]

            def _p_hor(k, X, adv, syms):
                li = _methods._index_of(sections, k)
                bucket = _bucket(li, emb, cap)
                if bucket not in cache:
                    tr = _train_indices(warm, bucket)
                    if tr is None:
                        return np.full(X.shape[0], np.nan, dtype=np.float64)
                    specs = []
                    for hh in hs:
                        Xs, ys = [], []
                        for i in tr:
                            Xb, yb, _a, _s = snap.block(sample, sections[i], feats, hh)
                            ok = np.isfinite(yb) & np.isfinite(Xb).all(axis=1)
                            if int(ok.sum()) >= _contract.MIN_CROSS_SECTION:
                                Xs.append(Xb[ok])
                                ys.append(yb[ok])
                        specs.append(base(np.vstack(Xs), np.concatenate(ys), seed)
                                     if Xs else None)
                    cache[bucket] = specs
                out = np.zeros(X.shape[0])
                for w, spec in zip(ws, cache[bucket]):
                    if spec is not None and w != 0.0:
                        out += w * _L.standardise(_L.predict(spec, X, feats))
                return out
            return _p_hor

        if family == FAM_RELATIONAL:
            base = _fitter(params["learner"], params["params"])
            gcol = idx[params["group_by"]]
            ng = int(params["n_groups"])
            cache: dict = {}

            def _neutralise(Xb):
                g = np.clip((_rank01(Xb[:, gcol]) + 0.5) * ng, 0, ng - 1e-9).astype(int)
                out = Xb.copy()
                for gi in range(ng):
                    m = g == gi
                    if int(m.sum()) >= 10:
                        out[m] = Xb[m] - Xb[m].mean(axis=0)
                return out

            def _p_rel(k, X, adv, syms):
                li = _methods._index_of(sections, k)
                bucket = _bucket(li, emb, cap)
                if bucket not in cache:
                    tr = _train_indices(warm, bucket)
                    if tr is None:
                        return np.full(X.shape[0], np.nan, dtype=np.float64)
                    Xs, ys = [], []
                    for i in tr:
                        Xb, yb, _a, _s = snap.block(sample, sections[i], feats, horizon)
                        ok = np.isfinite(yb) & np.isfinite(Xb).all(axis=1)
                        if int(ok.sum()) >= _contract.MIN_CROSS_SECTION:
                            Xs.append(_neutralise(Xb[ok]))
                            ys.append(yb[ok])
                    if not Xs:
                        return np.zeros(X.shape[0])
                    cache[bucket] = base(np.vstack(Xs), np.concatenate(ys), seed)
                return _L.predict(cache[bucket], _neutralise(X), feats)
            return _p_rel

        if family == FAM_DECISION:
            cache: dict = {}

            def _p_dec(k, X, adv, syms):
                li = _methods._index_of(sections, k)
                bucket = _bucket(li, emb, cap)
                if bucket not in cache:
                    tr = _train_indices(warm, bucket)
                    if tr is None:
                        return np.full(X.shape[0], np.nan, dtype=np.float64)
                    blocks = []
                    for i in tr:
                        Xb, _y, _a, _s = snap.block(sample, sections[i], feats, horizon)
                        raw, bench = snap.holding_returns(sample, sections[i],
                                                          _judge.HOLD_SESSIONS)
                        ok = np.isfinite(raw) & np.isfinite(Xb).all(axis=1)
                        if int(ok.sum()) >= _contract.MIN_CROSS_SECTION:
                            names = np.array([str(snap.symbols[int(j)])
                                              for j in _s[ok]])
                            blocks.append((Xb[ok], raw[ok] - bench, names))
                    if not blocks:
                        return np.full(X.shape[0], np.nan, dtype=np.float64)
                    cache[bucket] = _fit_nonlinear_decision(
                        blocks, n_features=len(feats), hidden=params["hidden"],
                        gamma=float(params["gamma"]),
                        temperature=float(params["temperature"]),
                        cost_rate=float(_judge.policy()["cost_rate_per_side"]),
                        epochs=int(params["epochs"]), seed=seed)
                return _decision_predict(cache[bucket], X)
            return _p_dec

        raise ValueError("unknown novel family %r" % (family,))
    return _factory


def _bucket(li: int, emb: int, cap: int) -> int:
    upper = min(int(li) - int(emb), int(cap))
    return upper - (upper % _methods.REFIT_EVERY)


def _train_indices(warm: list, bucket: int):
    """The expanding training window, or ``None`` when none legitimately exists.

    Every novel family previously fell back to ``warm[:12]`` - the FIRST twelve
    dates of the layer - whenever the expanding window came up short. On the
    validation layer that branch is unreachable, so it never fired in Campaign v2.
    Campaign v3 runs these predictors across DISCOVERY to fit the Track-A
    calibration, where the earliest dates do reach it, and there those twelve
    dates lie AFTER the date being scored. Returning ``None`` makes the absence of
    a model explicit instead of filling it with the future.
    """
    tr = [i for i in warm if i <= bucket]
    return tr if len(tr) >= _methods.MIN_TRAIN_SECTIONS else None


def _fitter(name: str, params: dict) -> Callable:
    def _fit(X, y, seed):
        if name == "ridge":
            return _L.fit_ridge(X, y, alpha=float(params["alpha"]))
        if name == "elastic_net":
            return _L.fit_elastic_net(X, y, alpha=float(params["alpha"]),
                                      l1_ratio=float(params["l1_ratio"]))
        if name == "gbrt":
            return _L.fit_gbrt(X, y, seed=seed, **params)
        raise ValueError("unknown base learner %r" % (name,))
    return _fit


# --------------------------------------------------------------------------- #
# Nonlinear direct decision learner (materially distinct from the known family)
# --------------------------------------------------------------------------- #
def _fit_nonlinear_decision(blocks, *, n_features, hidden, gamma, temperature,
                            cost_rate, epochs, seed) -> dict:
    """A one-hidden-layer score whose induced PORTFOLIO maximises net utility.

    The known ``direct_portfolio`` family learns a LINEAR score under the same
    objective. This one learns a nonlinear score, which is a materially different
    specification rather than a re-parameterisation: it can express "this
    characteristic matters only when that one is extreme", which a linear score
    cannot, and which is precisely the interaction structure the tree families
    keep finding in prediction space.

    ``blocks`` is an ordered iterable of ``(X_t, r_t, symbols_t)``. Like its
    linear sibling it allocates across the names AND a cash unit, and it prices
    the turnover it implies over the UNION OF SECURITY IDENTITIES. Two Track-B
    families trained under different economics would not be comparable with each
    other, let alone with Track A - and an earlier draft of this function
    discarded ``cost_rate`` entirely, which would have let the nonlinear family
    win by trading for free.
    """
    rng = np.random.default_rng(int(seed))
    h = int(hidden[0])
    W1 = rng.normal(0.0, math.sqrt(2.0 / n_features), size=(n_features, h))
    b1 = np.zeros(h)
    w2 = rng.normal(0.0, math.sqrt(2.0 / h), size=h)
    params = [W1, b1, w2]
    m = [np.zeros_like(p) for p in params]
    v = [np.zeros_like(p) for p in params]
    cash_logit = 0.0
    mc = vc = 0.0
    T = float(temperature)
    data = []
    for blk in blocks:
        if len(blk) != 3:
            raise ValueError(
                "the nonlinear decision learner requires (X_t, r_t, symbols_t); "
                "turnover without security identity is the Campaign v2 defect")
        data.append(blk)

    for e in range(int(epochs)):
        ports = np.empty(len(data), dtype=np.float64)
        books = []
        for i, (X, r, _s) in enumerate(data):
            z = X @ params[0] + params[1]
            a = np.maximum(z, 0.0)
            full = _L._softmax_with_cash(T * (a @ params[2]), cash_logit)
            w = full[:-1]
            ports[i] = float(w @ r)
            books.append((z, a, w, float(full[-1])))
        mbar = float(ports.mean())

        grads = [np.zeros_like(p) for p in params]
        grad_c = 0.0
        prev_w: dict = {}
        for i, (X, r, syms) in enumerate(data):
            z, a, w, w_cash = books[i]
            port = ports[i]
            coef = 1.0 - float(gamma) * (port - mbar)

            prev_aligned = _L._align_previous(prev_w, syms)
            sign = np.sign(w - prev_aligned)
            sw = float(sign @ w)

            dw = w * (r - port) * coef - float(cost_rate) * (w * (sign - sw))
            ds = T * dw
            grads[2] += a.T @ ds
            da = np.outer(ds, params[2]) * (z > 0)
            grads[0] += X.T @ da
            grads[1] += da.sum(axis=0)
            grad_c += (w_cash * (0.0 - port)) * coef \
                - float(cost_rate) * (w_cash * (0.0 - sw))
            prev_w = {str(s): float(x) for s, x in zip(syms, w) if x > 0.0}

        for i in range(3):
            g = grads[i] / len(data) - 1e-3 * params[i]
            m[i] = 0.9 * m[i] + 0.1 * g
            v[i] = 0.999 * v[i] + 0.001 * g * g
            params[i] += 0.02 * (m[i] / (1 - 0.9 ** (e + 1))) / (
                np.sqrt(v[i] / (1 - 0.999 ** (e + 1))) + 1e-8)
        gc = grad_c / len(data)
        mc = 0.9 * mc + 0.1 * gc
        vc = 0.999 * vc + 0.001 * gc * gc
        cash_logit += 0.02 * (mc / (1 - 0.9 ** (e + 1))) / (
            math.sqrt(vc / (1 - 0.999 ** (e + 1))) + 1e-8)

    return {"kind": "nonlinear_decision", "W1": params[0].tolist(),
            "b1": params[1].tolist(), "w2": params[2].tolist(),
            "cash_logit": float(cash_logit), "gamma": float(gamma),
            "cost_rate": float(cost_rate), "temperature": T,
            "turnover_alignment": "BY_SECURITY_IDENTITY_NEVER_BY_ARRAY_POSITION",
            "cash_is_a_competing_asset": True,
            "objective": "DIRECT_NET_PORTFOLIO_UTILITY_NONLINEAR"}


def _decision_predict(spec: dict, X: np.ndarray) -> np.ndarray:
    """Track-B output: proposed STOCK weights, summing to ``1 - cash``."""
    a = np.maximum(X @ np.asarray(spec["W1"]) + np.asarray(spec["b1"]), 0.0)
    s = float(spec["temperature"]) * (a @ np.asarray(spec["w2"]))
    return _L._softmax_with_cash(s, float(spec.get("cash_logit", 0.0)))[:-1]


# --------------------------------------------------------------------------- #
# Frozen grammar artifact
# --------------------------------------------------------------------------- #
def grammar_contract(*, campaign_id: str = _contract.CAMPAIGN_ID) -> dict:
    body = {
        "contract": GRAMMAR_SCHEMA,
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "frozen_before_execution": True,
        "families": list(FAMILIES),
        "family_budget": _contract.MAX_NOVEL_FAMILIES,
        "primitives": list(PRIMITIVES),
        "max_expression_depth": MAX_EXPRESSION_DEPTH,
        "max_base_features": MAX_BASE_FEATURES,
        "complexity_penalised": True,
        "campaign_budget": _contract.MAX_NOVEL_CAMPAIGNS,
        "candidates_per_campaign": _contract.MAX_NOVEL_CANDIDATES_PER_CAMPAIGN,
        "candidates_total": _contract.MAX_NOVEL_CANDIDATES_TOTAL,
        "refinement_depth": _contract.MAX_NOVEL_REFINEMENT_DEPTH,
        "n1_allocation": dict(N1_ALLOCATION),
        "n2_allocation": dict(N2_ALLOCATION),
        "generation_never_reads_an_evaluation_result": True,
        "seed": _contract.SEEDS["novel_search"],
        "family_questions": {
            FAM_RESIDUAL: "what does the incumbent's ranking systematically miss?",
            FAM_REGIME: "does the relationship differ across point-in-time regimes?",
            FAM_HORIZON: "does predictive information change shape across horizons?",
            FAM_SYMBOLIC: "is there a bounded interaction the linear and tree "
                          "families do not express?",
            FAM_RELATIONAL: "is the signal relative to point-in-time peers rather "
                            "than absolute?",
            FAM_DECISION: "can a nonlinear score trained on net portfolio utility "
                          "beat one trained on prediction error?",
        },
        "no_hindsight_regime_labels": True,
        "regime_source": "the panel's OWN realised equal-weight return, resolved "
                         "strictly before the conditioning date",
        "peer_group_source": "point-in-time liquidity / beta / volatility "
                             "quantiles; SECTOR is deliberately excluded because "
                             "the canonical owner declares its historical "
                             "snapshot inadmissible",
    }
    body["novel_grammar_hash"] = r31.sha(body)
    body.update(r31.safety_block())
    return body


def generate(campaign_no: int, feats: tuple, *,
             seed: int = _contract.SEEDS["novel_search"]) -> list:
    """The frozen, deterministic candidate list for one novel campaign."""
    alloc = N1_ALLOCATION if int(campaign_no) == 1 else N2_ALLOCATION
    out = []
    for fam in FAMILIES:
        n = int(alloc.get(fam, 0))
        if n <= 0:
            continue
        gen = GENERATORS[fam]
        # A STABLE per-family seed offset. Python's builtin ``hash`` of a str is
        # salted per process, so using it here would make the "deterministic"
        # enumeration differ between runs and silently break the campaign's
        # idempotency key.
        offset = int(r31.sha(fam)[:8], 16) % 1000
        for i, params in enumerate(gen(n, feats, seed + offset)):
            out.append({"family": fam, "params": params,
                        "candidate_id": "nv%d:%s:%03d" % (campaign_no, fam, i)})
    return out[:_contract.MAX_NOVEL_CANDIDATES_PER_CAMPAIGN]
