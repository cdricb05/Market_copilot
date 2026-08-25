"""alpha_agent.r44.combine - ENGINE 2B, the predeclared combination rules.

Eight rules, all named in the frozen contract, all fitted on ZONE_A+ZONE_B
only, one of them - ``FAMILY_BALANCED_ERC`` - declared PRIMARY before the
lockbox is opened.

What is deliberately absent matters as much as what is here. There is no
unconstrained mean-variance optimiser and no rule that maximises historical
Sharpe, because both would let the data choose the weights and neither can
be defended once the holdout is opened. Six of the eight rules estimate no
expected return at all; the two that do (``MIN_CORRELATION_SHRINKAGE`` uses
none, ``BAYESIAN_SHRINKAGE`` uses a shrunken one) are constrained long-only
and capped, so a single flattering stream cannot take the book.

Every rule returns weights that satisfy the contract's constraints: long
only in stream space, at most 25% in one stream, 40% in one family and 50%
in one asset class.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..r43 import judge as J
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r44.combine"

TRADING_DAYS = J.TRADING_DAYS


# --------------------------------------------------------------------------- #
# Covariance
# --------------------------------------------------------------------------- #
def shrunk_covariance(X: pd.DataFrame) -> pd.DataFrame:
    """Ledoit-Wolf shrinkage towards a diagonal target.

    The target is the diagonal of the sample covariance, so the estimator
    shrinks CORRELATIONS towards zero while leaving each stream's own
    variance alone. With a dozen streams and a few thousand days this is
    conservative rather than clever, which is the intention.
    """
    d = X.dropna(how="all")
    cols = list(d.columns)
    A = d.to_numpy(dtype=float)
    mask = np.isfinite(A)
    A = np.where(mask, A, 0.0)
    n = np.maximum(mask.sum(axis=0), 1)
    mu = A.sum(axis=0) / n
    Z = np.where(mask, A - mu, 0.0)
    # Pairwise-complete covariance with an effective count per pair.
    cnt = mask.astype(float).T @ mask.astype(float)
    cnt = np.maximum(cnt - 1.0, 1.0)
    S = (Z.T @ Z) / cnt
    T = np.diag(np.diag(S))
    # Ledoit-Wolf intensity, computed on the pooled panel.
    m = int(mask.sum(axis=1).max() or 1)
    var_s = np.nanmean((Z ** 2).T @ (Z ** 2) / cnt - S ** 2) / max(m, 1)
    denom = float(np.sum((S - T) ** 2))
    lam = 0.0 if denom <= 0 else float(np.clip(var_s * S.shape[0] / denom,
                                               0.0, 1.0))
    out = (1.0 - lam) * S + lam * T
    out = pd.DataFrame(out, index=cols, columns=cols)
    out.attrs["shrinkage"] = lam
    return out


def _vols(cov: pd.DataFrame) -> np.ndarray:
    return np.sqrt(np.maximum(np.diag(cov.to_numpy(dtype=float)), 1e-24))


# --------------------------------------------------------------------------- #
# Constraint projection
# --------------------------------------------------------------------------- #
def _normalise(w: np.ndarray) -> np.ndarray:
    w = np.clip(np.nan_to_num(w, nan=0.0), 0.0, None)
    s = w.sum()
    return (w / s) if s > 0 else np.full_like(w, 1.0 / max(len(w), 1))


def apply_constraints(w: pd.Series, meta: dict) -> pd.Series:
    """Long-only, sum-to-one, and the contract's three caps, applied by
    iterative water-filling so no cap is silently violated."""
    ids = list(w.index)
    v = _normalise(w.reindex(ids).to_numpy(dtype=float))
    caps = C.PORTFOLIO_CONSTRAINTS
    groupings = [
        (None, float(caps["max_single_stream_weight"])),
        ("family", float(caps["max_family_weight"])),
        ("asset_class", float(caps["max_asset_class_weight"])),
    ]
    for _ in range(200):
        changed = False
        for key, cap in groupings:
            if key is None:
                labels = ids
            else:
                labels = [str((meta.get(i) or {}).get(key)) for i in ids]
            tot = {}
            for lab, x in zip(labels, v):
                tot[lab] = tot.get(lab, 0.0) + x
            over = {lab for lab, x in tot.items() if x > cap + 1e-12}
            if not over:
                continue
            changed = True
            free_mass = 0.0
            for lab in over:
                sel = np.array([lb == lab for lb in labels])
                cur = v[sel].sum()
                if cur <= 0:
                    continue
                free_mass += cur - cap
                v[sel] = v[sel] * (cap / cur)
            room = np.array([
                lb not in over for lb in labels], dtype=float)
            if room.sum() <= 0 or free_mass <= 0:
                v = _normalise(v)
                continue
            base = v * room
            if base.sum() <= 0:
                base = room
            v = v + free_mass * base / base.sum()
        v = _normalise(v)
        if not changed:
            break
    return pd.Series(v, index=ids, name="weight")


# --------------------------------------------------------------------------- #
# The eight rules
# --------------------------------------------------------------------------- #
def _equal_weight(cov, mu, meta):
    n = cov.shape[0]
    return pd.Series(np.full(n, 1.0 / n), index=cov.index)


def _inverse_vol(cov, mu, meta):
    inv = 1.0 / _vols(cov)
    return pd.Series(_normalise(inv), index=cov.index)


def _erc_weights(S: np.ndarray, *, iters: int = 5000,
                 budget: np.ndarray = None) -> np.ndarray:
    """Equal (or budgeted) risk contribution by cyclical coordinate descent.

    Solves for w > 0 with w_i (S w)_i proportional to ``budget``. The
    fixed-point iteration below is the standard Spinu/Griveau-Billion form
    and converges monotonically for a positive-definite S.
    """
    n = S.shape[0]
    b = np.full(n, 1.0 / n) if budget is None else _normalise(budget)
    d = np.sqrt(np.maximum(np.diag(S), 1e-24))
    w = (b / d)
    w = w / w.sum()
    for _ in range(iters):
        Sw = S @ w
        w_new = np.maximum(b / np.maximum(Sw, 1e-18), 1e-18)
        w_new = np.sqrt(w * w_new)
        w_new = w_new / w_new.sum()
        if np.max(np.abs(w_new - w)) < 1e-12:
            w = w_new
            break
        w = w_new
    return w


def _erc(cov, mu, meta):
    S = cov.to_numpy(dtype=float)
    return pd.Series(_erc_weights(S), index=cov.index)


def _capped_erc(cov, mu, meta):
    # The cap is applied by apply_constraints; this rule differs from plain
    # ERC only in that its cap is part of its identity rather than a
    # post-hoc adjustment.
    return _erc(cov, mu, meta)


def _family_balanced_erc(cov, mu, meta):
    """Equal RISK budget per information family, ERC within each family.

    This is the primary rule. It is invariant to how many streams a family
    happens to contain, which matters here because the equity panel supplies
    four streams and the volatility panel supplies one.
    """
    ids = list(cov.index)
    fams = [str((meta.get(i) or {}).get("family")) for i in ids]
    uniq = sorted(set(fams))
    per_family = 1.0 / len(uniq)
    budget = np.array([per_family / fams.count(f) for f in fams])
    S = cov.to_numpy(dtype=float)
    return pd.Series(_erc_weights(S, budget=budget), index=ids)


def _hrp(cov, mu, meta):
    """Hierarchical risk parity (Lopez de Prado) on the shrunk covariance."""
    from scipy.cluster.hierarchy import linkage, to_tree
    from scipy.spatial.distance import squareform

    S = cov.to_numpy(dtype=float)
    d = np.sqrt(np.maximum(np.diag(S), 1e-24))
    R = S / np.outer(d, d)
    R = np.clip(np.nan_to_num(R, nan=0.0), -1.0, 1.0)
    np.fill_diagonal(R, 1.0)
    dist = np.sqrt(np.maximum(0.5 * (1.0 - R), 0.0))
    np.fill_diagonal(dist, 0.0)
    link = linkage(squareform(dist, checks=False), method="single")

    def _order(node):
        if node.is_leaf():
            return [node.id]
        return _order(node.get_left()) + _order(node.get_right())

    order = _order(to_tree(link))

    def _ivp(idx):
        v = 1.0 / np.maximum(np.diag(S)[idx], 1e-24)
        return v / v.sum()

    def _cluster_var(idx):
        w = _ivp(idx)
        return float(w @ S[np.ix_(idx, idx)] @ w)

    w = np.ones(len(order))
    clusters = [order]
    while clusters:
        nxt = []
        for cl in clusters:
            if len(cl) <= 1:
                continue
            half = len(cl) // 2
            left, right = cl[:half], cl[half:]
            vl, vr = _cluster_var(left), _cluster_var(right)
            alpha = 1.0 - vl / (vl + vr) if (vl + vr) > 0 else 0.5
            for i in left:
                w[order.index(i)] *= alpha
            for i in right:
                w[order.index(i)] *= (1.0 - alpha)
            nxt += [left, right]
        clusters = nxt
    out = np.zeros(len(order))
    for pos, i in enumerate(order):
        out[i] = w[pos]
    return pd.Series(_normalise(out), index=cov.index)


def _min_correlation_shrinkage(cov, mu, meta):
    """Long-only minimum-variance on the shrunk covariance.

    Solved by projected gradient descent rather than a closed form, because
    the long-only and cap constraints are part of the contract and a closed
    form would violate both.
    """
    S = cov.to_numpy(dtype=float)
    n = S.shape[0]
    w = np.full(n, 1.0 / n)
    step = 1.0 / (2.0 * max(float(np.max(np.abs(S))) * n, 1e-12))
    cap = float(C.PORTFOLIO_CONSTRAINTS["max_single_stream_weight"])
    for _ in range(20000):
        g = 2.0 * (S @ w)
        w_new = np.clip(w - step * g, 0.0, cap)
        s = w_new.sum()
        w_new = w_new / s if s > 0 else np.full(n, 1.0 / n)
        if np.max(np.abs(w_new - w)) < 1e-13:
            w = w_new
            break
        w = w_new
    return pd.Series(w, index=cov.index)


def _bayesian_shrinkage(cov, mu, meta):
    """Inverse-vol weights tilted by a HEAVILY shrunken information ratio.

    The prior is that every stream's expected excess return is zero - which
    is, after 302 trials, this estate's honest prior. The posterior mean is
    ``mu * n / (n + k)`` with ``k`` fixed at 750 days, so a stream needs
    years of evidence to move its weight, and a negative posterior gets no
    weight at all rather than a short position.
    """
    k = 750.0
    vol = _vols(cov)
    ir = np.zeros(len(vol))
    for i, sid in enumerate(cov.index):
        m = (mu or {}).get(sid)
        n_obs = ((mu or {}).get("_n") or {}).get(sid, 0.0)
        if m is None or not np.isfinite(m):
            continue
        post = float(m) * (n_obs / (n_obs + k)) if n_obs else 0.0
        ir[i] = max(post / max(vol[i], 1e-12), 0.0)
    base = 1.0 / np.maximum(vol, 1e-12)
    if ir.sum() <= 0:
        return pd.Series(_normalise(base), index=cov.index)
    tilt = base * (1.0 + ir / max(ir.max(), 1e-12))
    return pd.Series(_normalise(tilt), index=cov.index)


_RULES = {
    "EQUAL_WEIGHT": _equal_weight,
    "INVERSE_VOL": _inverse_vol,
    "EQUAL_RISK_CONTRIBUTION": _erc,
    "CAPPED_EQUAL_RISK_CONTRIBUTION": _capped_erc,
    "FAMILY_BALANCED_ERC": _family_balanced_erc,
    "HIERARCHICAL_RISK_PARITY": _hrp,
    "MIN_CORRELATION_SHRINKAGE": _min_correlation_shrinkage,
    "BAYESIAN_SHRINKAGE": _bayesian_shrinkage,
}


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def fit_weights(frame: pd.DataFrame, fit_dates, meta: dict,
                rule: str = None) -> dict:
    """Fit ONE combination rule on the FIT zones only."""
    rule = rule or C.PRIMARY_COMBINATION_RULE
    if rule not in _RULES:
        raise ValueError("undeclared combination rule %r" % rule)
    fit = frame.reindex(pd.DatetimeIndex(fit_dates))
    fit = fit.loc[:, fit.notna().sum() >= 250]
    if fit.shape[1] < 2:
        return {"rule": rule, "state": "INSUFFICIENT_STREAMS",
                "n_streams": int(fit.shape[1])}
    cov = shrunk_covariance(fit)
    mu = {c: float(np.nanmean(fit[c].to_numpy(dtype=float)) * TRADING_DAYS)
          for c in fit.columns}
    mu["_n"] = {c: float(fit[c].notna().sum()) for c in fit.columns}
    raw = _RULES[rule](cov, mu, meta)
    w = apply_constraints(raw, meta)
    contrib = w.to_numpy() * (cov.to_numpy() @ w.to_numpy())
    total = float(contrib.sum())
    return {
        "rule": rule,
        "state": "FITTED",
        "weights": {k: float(v) for k, v in w.items()},
        "raw_weights": {k: float(v) for k, v in raw.items()},
        "n_streams": int(fit.shape[1]),
        "fit_days": int(fit.shape[0]),
        "shrinkage_intensity": float(cov.attrs.get("shrinkage", 0.0)),
        "risk_contribution": {k: float(c / total) if total else None
                              for k, c in zip(w.index, contrib)},
        "max_weight": float(w.max()),
        "effective_n_streams": float(1.0 / float((w ** 2).sum()))
        if float((w ** 2).sum()) else None,
        "constraints": dict(C.PORTFOLIO_CONSTRAINTS),
        "fitted_on": "FIT_ZONES_ONLY",
    }


def fit_all_rules(frame: pd.DataFrame, fit_dates, meta: dict) -> dict:
    return {r: fit_weights(frame, fit_dates, meta, r)
            for r in C.COMBINATION_RULES}


def portfolio_returns(frame: pd.DataFrame, weights: dict, dates=None, *,
                      overlay_cost: bool = True) -> pd.Series:
    """Apply FIXED weights with MONTHLY rebalancing back to target.

    Between rebalances the weights drift with the streams' own returns, and
    the reset is charged the contract's overlay cost. Holding the weights
    literally constant every day would understate turnover; never
    rebalancing would silently let a winner take the book.
    """
    ids = [k for k in weights if k in frame.columns]
    if not ids:
        return pd.Series(dtype=float)
    d = frame[ids] if dates is None else \
        frame[ids].reindex(pd.DatetimeIndex(dates))
    avail = d.notna().to_numpy()
    d = d.fillna(0.0)
    target = np.array([float(weights[i]) for i in ids])
    target = target / target.sum() if target.sum() else target
    idx = d.index
    R = d.to_numpy(dtype=float)
    w = target.copy()
    out = np.full(len(idx), np.nan)
    turn = np.zeros(len(idx))
    months = pd.Series(idx, index=idx).dt.to_period("M")
    prev_month = None
    for t in range(len(idx)):
        if prev_month is not None and months.iloc[t] != prev_month:
            turn[t] = float(np.abs(target - w).sum())
            w = target.copy()
        prev_month = months.iloc[t]
        # A stream that does not yet exist is not a zero-return holding; it
        # is not held at all. Weights are renormalised over what is live, so
        # a portfolio is never diluted by history it could not have had.
        m = avail[t]
        if not m.any():
            continue
        we = w * m
        s = we.sum()
        if s <= 0:
            we = target * m
            s = we.sum()
            if s <= 0:
                continue
        we = we / s
        out[t] = float(we @ R[t])
        grown = w * (1.0 + np.where(m, R[t], 0.0))
        g = grown.sum()
        w = grown / g if g else target.copy()
    s = pd.Series(out, index=idx, name="portfolio")
    if overlay_cost:
        bps = float(C.PORTFOLIO_CONSTRAINTS[
            "overlay_cost_bps_per_unit_turnover"])
        s = s - pd.Series(turn, index=idx) * (bps / 1e4)
    return s
