"""alpha_agent.r34.sizing - the ONE uncertainty-aware position-sizing owner.

Between a calibrated expected return and a portfolio weight there is a choice
that Release 33 made implicitly and never tested: how much conviction to put
behind a forecast the model is not sure about. R33 sized on a clipped z-score of
the raw forecast divided by trailing volatility, uniformly, and that single
untested choice sits between every one of its 46 statistically surviving
configurations and the zero that beat the control.

This module tests the choice. Each rule below turns per-row
``(expected_return, uncertainty, predicted_volatility, score)`` into a raw
conviction, and nothing else - the cross-sectional mapping, the caps and the
gross-exposure limit belong to :mod:`alpha_agent.r34.portfolio`. Keeping them
apart is what makes it possible to say which of the two destroyed the edge.

Two constraints are structural rather than parametric:

**No unconstrained optimiser.** An optimiser handed a noisy expected-return
vector and a sample covariance matrix will find the linear combination that
maximises estimation error, which is why the mean-variance mapping in the
portfolio layer carries an 80 % shrinkage that is declared in the contract
rather than tuned.

**No leverage.** Gross exposure is capped at 1.0 in the primary campaign, so a
sizing rule can only decide the SHAPE of the book and how much of it is cash.
Cash at 100 % is a legitimate answer and several of these rules will produce it
when forecasts are weak, which is the point.
"""
from __future__ import annotations

import numpy as np

from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r34.sizing"


def _safe(x, fill=0.0):
    v = np.asarray(x, dtype=np.float64)
    return np.where(np.isfinite(v), v, fill)


def conviction(rule: str, *, expected_return: np.ndarray,
               uncertainty: np.ndarray, predicted_vol: np.ndarray,
               score: np.ndarray, confidence: float = 1.0) -> np.ndarray:
    """Raw, unnormalised conviction per row under one sizing rule.

    The output is NOT a weight. It is a relative conviction that the portfolio
    layer maps onto weights under its own constraints, so that the sizing rule
    and the portfolio mapping can be attributed separately in the attrition
    waterfall.
    """
    er = _safe(expected_return)
    unc = np.clip(_safe(uncertainty, 1.0), 1e-5, None)
    vol = np.clip(_safe(predicted_vol, 0.15), 0.01, 2.0)
    s = _safe(score)

    if rule == _contract.SIZE_RANK_WEIGHT:
        # Magnitude discarded entirely: only the ordering is used.
        n = s.size
        if n < 2:
            return np.zeros(n)
        r = np.argsort(np.argsort(s)).astype(np.float64)
        return r / max(r.max(), 1.0) - 0.5

    if rule == _contract.SIZE_SIGNAL_OVER_VOL:
        sd = float(np.std(s, ddof=1)) if s.size > 1 else 0.0
        z = s / sd if sd > 0 else np.zeros_like(s)
        return np.clip(z, -3.0, 3.0) / vol

    if rule == _contract.SIZE_ER_OVER_VAR:
        # The textbook mean-variance tilt, per instrument.
        return er / (vol ** 2)

    if rule == _contract.SIZE_ER_OVER_UNCERTAINTY:
        # Conviction per unit of FORECAST error rather than per unit of market
        # risk. A confident small forecast outranks a wild large one.
        return er / unc

    if rule == _contract.SIZE_BAYES_POSTERIOR:
        # Shrink the expected return by the calibration's own confidence, then
        # size on variance. Two independent discounts: the calibration's
        # retained slope and the instrument's risk.
        return (float(np.clip(confidence, 0.0, 1.0)) * er) / (vol ** 2)

    if rule == _contract.SIZE_CLIPPED_Z:
        sd = float(np.std(er, ddof=1)) if er.size > 1 else 0.0
        z = er / sd if sd > 0 else np.zeros_like(er)
        return np.clip(z, -_contract.CLIPPED_Z_LIMIT,
                       _contract.CLIPPED_Z_LIMIT) / vol

    raise ValueError("unknown sizing rule %r" % (rule,))


def describe_rule(rule: str) -> dict:
    """What each rule asserts, in one line, for the results artifact."""
    return {
        _contract.SIZE_RANK_WEIGHT:
            "ordering only; forecast magnitude is discarded",
        _contract.SIZE_SIGNAL_OVER_VOL:
            "clipped score z-score scaled by predicted volatility",
        _contract.SIZE_ER_OVER_VAR:
            "calibrated expected return over predicted variance",
        _contract.SIZE_ER_OVER_UNCERTAINTY:
            "calibrated expected return over forecast uncertainty",
        _contract.SIZE_BAYES_POSTERIOR:
            "confidence-shrunk expected return over predicted variance",
        _contract.SIZE_CLIPPED_Z:
            "clipped z-score of the calibrated expected return over volatility",
    }.get(rule, rule)


def effective_instruments(weights: np.ndarray) -> float:
    """Inverse Herfindahl of the absolute weights.

    The number of instruments the book is REALLY holding. A book whose weights
    are 90 % in one name and 10 % spread over nine holds about 1.2 things, not
    ten, and the concentration gate reads this rather than the position count.
    """
    w = np.abs(_safe(weights))
    total = float(w.sum())
    if total <= 0:
        return 0.0
    p = w / total
    return float(1.0 / max(float((p ** 2).sum()), 1e-12))
