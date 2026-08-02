"""
alpha_agent.orthogonality - Stage 9.4 Track B: orthogonality & incremental value.

Pure, deterministic, stdlib-only helpers that measure whether a candidate signal
carries information that is INDEPENDENT of what the tournament already holds - the
operating champion, retained research candidates, market beta, sector exposure,
and volatility/size proxies. A candidate must never be described as valuable
merely because it duplicates the champion; these functions quantify exactly that.

Everything is byte-reproducible (no numpy/pandas, no clock, no randomness) and
builds on the shared Stage 5 numeric primitives in ``experiment_contracts``
(``spearman``/``pearson``/``ranks``) rather than reimplementing statistics.

Nothing here writes state, opens a network socket, or touches an operational
ledger. It is read-only measurement over already-computed return / factor series.
"""
from __future__ import annotations

from typing import Optional, Sequence

from . import experiment_contracts as ec

__all__ = [
    "factor_correlation", "rank_correlation", "residualize",
    "residualized_signal", "partial_rank_ic", "incremental_return_metrics",
    "diversification_contribution", "independent_information_score",
]


def _finite_pairs(xs: Sequence, ys: Sequence) -> tuple[list, list]:
    ax: list[float] = []
    ay: list[float] = []
    for a, b in zip(xs, ys):
        if ec._is_num(a) and ec._is_num(b):
            ax.append(float(a))
            ay.append(float(b))
    return ax, ay


def factor_correlation(a: Sequence, b: Sequence) -> Optional[float]:
    """Pearson correlation of two aligned series (factor/return correlation)."""
    ax, ay = _finite_pairs(a, b)
    return ec.pearson(ax, ay)


def rank_correlation(a: Sequence, b: Sequence) -> Optional[float]:
    """Spearman rank correlation of two aligned series."""
    ax, ay = _finite_pairs(a, b)
    return ec.spearman(ax, ay)


# --------------------------------------------------------------------------- #
# Ordinary-least-squares residualization (pure normal equations).
# --------------------------------------------------------------------------- #
def _ols_beta(y: list[float], cols: list[list[float]]) -> Optional[list[float]]:
    """Solve (X'X) b = X'y for design matrix X = [1, cols...]. Returns the
    coefficient vector (intercept first) or None if singular / degenerate."""
    n = len(y)
    if n < 2:
        return None
    design = [[1.0] + [c[i] for c in cols] for i in range(n)]
    k = len(design[0])
    if n < k + 1:
        return None
    # Normal equations: A = X'X (k x k), g = X'y (k).
    a = [[0.0] * k for _ in range(k)]
    g = [0.0] * k
    for i in range(n):
        row = design[i]
        yi = y[i]
        for p in range(k):
            g[p] += row[p] * yi
            for q in range(k):
                a[p][q] += row[p] * row[q]
    return _solve(a, g)


def _solve(a: list[list[float]], g: list[float]) -> Optional[list[float]]:
    """Gaussian elimination with partial pivoting. Deterministic."""
    k = len(g)
    m = [row[:] + [g[i]] for i, row in enumerate(a)]
    for col in range(k):
        piv = max(range(col, k), key=lambda r: abs(m[r][col]))
        if abs(m[piv][col]) < 1e-12:
            return None
        m[col], m[piv] = m[piv], m[col]
        pv = m[col][col]
        for j in range(col, k + 1):
            m[col][j] /= pv
        for r in range(k):
            if r == col:
                continue
            f = m[r][col]
            if f == 0.0:
                continue
            for j in range(col, k + 1):
                m[r][j] -= f * m[col][j]
    return [m[i][k] for i in range(k)]


def residualize(y: Sequence, controls: Sequence[Sequence]) -> Optional[list]:
    """Residuals of ``y`` after regressing (with intercept) on one or more
    control series. Removes the linear exposure captured by the controls - e.g.
    residualizing a candidate's returns on market returns removes market beta.
    Returns None when the system is degenerate."""
    yv = [float(v) if ec._is_num(v) else None for v in y]
    cols_raw = [list(c) for c in controls]
    # Keep only rows finite across y and every control.
    idx = [i for i in range(len(yv))
           if yv[i] is not None
           and all(i < len(c) and ec._is_num(c[i]) for c in cols_raw)]
    if len(idx) < 3:
        return None
    yf = [yv[i] for i in idx]
    colsf = [[float(c[i]) for i in idx] for c in cols_raw]
    beta = _ols_beta(yf, colsf)
    if beta is None:
        return None
    resid_full: list[Optional[float]] = [None] * len(yv)
    for pos, i in enumerate(idx):
        pred = beta[0] + sum(beta[1 + p] * colsf[p][pos]
                             for p in range(len(colsf)))
        resid_full[i] = yf[pos] - pred
    return resid_full


def residualized_signal(factor_vals: Sequence,
                        controls: Sequence[Sequence]) -> Optional[list]:
    """The candidate factor with its exposure to the controls removed (alias of
    :func:`residualize` in factor space)."""
    return residualize(factor_vals, controls)


def partial_rank_ic(factor_vals: Sequence, forward_returns: Sequence,
                    controls: Optional[Sequence[Sequence]] = None
                    ) -> Optional[float]:
    """Partial Spearman rank IC of ``factor_vals`` vs ``forward_returns`` after
    removing the rank-space linear influence of the controls from BOTH sides.

    With no controls this is the ordinary rank IC (Spearman). With controls it
    is the correlation between the residual ranks - the information the factor
    carries about forward returns that the controls do not already explain.
    """
    if not controls:
        return rank_correlation(factor_vals, forward_returns)
    # Align on rows finite across factor, forward and every control.
    ctl = [list(c) for c in controls]
    idx = [i for i in range(min(len(factor_vals), len(forward_returns)))
           if ec._is_num(factor_vals[i]) and ec._is_num(forward_returns[i])
           and all(i < len(c) and ec._is_num(c[i]) for c in ctl)]
    if len(idx) < 4:
        return None
    fr = ec.ranks([float(factor_vals[i]) for i in idx])
    yr = ec.ranks([float(forward_returns[i]) for i in idx])
    ctl_ranks = [ec.ranks([float(c[i]) for i in idx]) for c in ctl]
    f_res = residualize(fr, ctl_ranks)
    y_res = residualize(yr, ctl_ranks)
    if f_res is None or y_res is None:
        return None
    fx = [v for v in f_res if v is not None]
    yx = [v for v in y_res if v is not None]
    if len(fx) != len(yx):
        return None
    return ec.pearson(fx, yx)


def incremental_return_metrics(candidate: Sequence, champion: Sequence
                               ) -> dict:
    """Incremental performance of ``candidate`` versus ``champion`` on the
    overlapping period-return series. All values are None when there is not
    enough overlap. ``incremental_*`` are candidate-minus-champion; a positive
    value means the candidate adds beyond the champion."""
    ax, ay = _finite_pairs(candidate, champion)
    n = len(ax)
    if n < 3:
        return {"overlap_periods": n, "incremental_mean_return": None,
                "incremental_net_return": None, "incremental_max_drawdown": None,
                "champion_negative_capture": None, "correlation": None}
    diff = [ax[i] - ay[i] for i in range(n)]
    corr = ec.pearson(ax, ay)
    # Performance restricted to periods when the champion lost money.
    neg = [ax[i] for i in range(n) if ay[i] < 0]
    return {
        "overlap_periods": n,
        "incremental_mean_return": ec.mean(diff),
        "incremental_net_return": ec.cumulative_return(diff),
        "incremental_max_drawdown": ec.max_drawdown(diff),
        "champion_negative_capture": ec.mean(neg) if neg else None,
        "correlation": corr,
    }


def diversification_contribution(candidate: Sequence, champion: Sequence
                                 ) -> Optional[float]:
    """A [0,1] diversification score = 1 - |corr(candidate, champion)|. Higher
    means the candidate's return stream is more independent of the champion."""
    ax, ay = _finite_pairs(candidate, champion)
    corr = ec.pearson(ax, ay)
    if corr is None:
        return None
    c = abs(corr)
    return 0.0 if c > 1.0 else (1.0 - c)


def _clamp01(x: Optional[float]) -> float:
    if x is None or x != x:
        return 0.0
    return 0.0 if x < 0.0 else (1.0 if x > 1.0 else x)


# Defaults chosen so the score is dominated by genuine orthogonality; a highly
# champion-correlated candidate is driven toward zero regardless of raw strength.
_DEF = {
    "weight_orthogonality": 0.55,
    "weight_partial_ic": 0.30,
    "weight_incremental": 0.15,
    "partial_ic_full_credit": 0.03,
    "incremental_net_full_credit": 0.03,
    "redundant_abs_corr": 0.90,
    "unknown_correlation_credit": 0.50,
}


def independent_information_score(*, corr_champion: Optional[float] = None,
                                  corr_retained: Optional[float] = None,
                                  partial_rank_ic: Optional[float] = None,
                                  incremental_net_return: Optional[float] = None,
                                  cfg: Optional[dict] = None) -> dict:
    """The INDEPENDENT_INFORMATION sub-score in [0,1] and its decomposition.

    Rewards a candidate that (a) is uncorrelated with the champion and retained
    candidates, (b) retains rank-IC after controls are partialled out, and (c)
    adds incremental net return beyond the champion. A candidate that merely
    duplicates the champion (|corr| -> 1) is driven toward zero by both the
    orthogonality term AND an explicit redundancy penalty, so a redundant
    candidate can never score as independent. Never weakens any evidence gate -
    it is a decomposition/ranking signal only.
    """
    c = dict(_DEF)
    for k, v in (cfg or {}).items():
        if k in c and isinstance(v, (int, float)):
            c[k] = float(v)

    known = [abs(x) for x in (corr_champion, corr_retained) if x is not None]
    max_abs = max(known) if known else None
    if max_abs is None:
        orth = float(c["unknown_correlation_credit"])
    else:
        orth = _clamp01(1.0 - max_abs)

    if partial_rank_ic is None:
        partial = 0.5
    else:
        full = c["partial_ic_full_credit"] or 0.03
        partial = _clamp01(0.5 + 0.5 * (partial_rank_ic / full))

    if incremental_net_return is None:
        incr = 0.5
    else:
        full = c["incremental_net_full_credit"] or 0.03
        incr = _clamp01(0.5 + 0.5 * (incremental_net_return / full))

    base = (orth * c["weight_orthogonality"]
            + partial * c["weight_partial_ic"]
            + incr * c["weight_incremental"])
    base = _clamp01(base)

    # Explicit redundancy penalty: as |corr| rises above the redundant cap the
    # score is scaled toward zero (0 at |corr| == 1).
    penalty = 1.0
    cap = c["redundant_abs_corr"]
    if max_abs is not None and max_abs > cap and cap < 1.0:
        penalty = _clamp01((1.0 - max_abs) / (1.0 - cap))
    score = _clamp01(base * penalty)

    return {
        "independent_information_score": round(score, 6),
        "orthogonality_component": round(orth, 6),
        "partial_ic_component": round(partial, 6),
        "incremental_component": round(incr, 6),
        "max_abs_correlation": None if max_abs is None else round(max_abs, 6),
        "redundancy_penalty_applied": penalty < 1.0,
    }
