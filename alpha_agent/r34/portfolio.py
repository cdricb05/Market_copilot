"""alpha_agent.r34.portfolio - the ONE portfolio-construction owner.

A conviction vector is not a portfolio. This module maps conviction onto weights
under constraints that a real book would face, and every mapping here is
bounded and explainable: no unconstrained optimiser, no leverage, no hidden
short, and cash as a first-class holding rather than the residual nobody looked
at.

The constraint projection matters more than the mapping. Release 33 capped gross
exposure at 2.0 and let the cross-sectional construction demean within asset
class, which meant its "long/short overlay" could hold a 40 % position in one
market without anything objecting. Here the caps are frozen in the contract -
20 % per instrument, 40 % per asset class, 100 % gross - and they are applied by
a projection that RE-CHECKS after redistribution, because capping one name pushes
weight into the others and a single pass can leave a book that violates the cap
it just enforced.

Only instruments that were TRADABLE at the decision date can receive weight.
That is not a detail: an untradable instrument is one that had not listed, had
not accumulated its minimum history, or was below the liquidity floor on that
date, and letting it hold weight would be a look-ahead that flatters exactly the
early, thin part of the sample.

The primary campaign is LONG-ONLY. A long-short variant is computed as SECONDARY
research and the contract forbids it from qualifying the release, because
shortability, borrow cost and recall risk are not modelled from owned data and a
long-short result would therefore not be implementability-proven.
"""
from __future__ import annotations

import numpy as np

from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r34.portfolio"


def _normalise_long(raw: np.ndarray, *, gross: float) -> np.ndarray:
    w = np.where(np.isfinite(raw) & (raw > 0.0), raw, 0.0)
    total = float(w.sum())
    if total <= 0:
        return np.zeros_like(w)
    return w / total * float(gross)


def apply_constraints(w: np.ndarray, asset_class: np.ndarray, *,
                      max_weight: float = _contract.MAX_INSTRUMENT_WEIGHT,
                      max_class: float = _contract.MAX_ASSET_CLASS_WEIGHT,
                      max_gross: float = _contract.MAX_GROSS_EXPOSURE,
                      passes: int = 8) -> np.ndarray:
    """Project weights onto the frozen constraint set, long-only.

    Capping one instrument frees weight that has to go somewhere, and putting it
    into the others can push THEM over the cap. So the projection iterates:
    clip, redistribute proportionally over the names with headroom, re-check.
    Anything that will not fit stays in cash, which is a real holding here.
    """
    w = np.where(np.isfinite(w) & (w > 0.0), w, 0.0).astype(np.float64)
    ac = np.asarray(asset_class)
    if w.sum() <= 0:
        return w

    for _ in range(int(passes)):
        changed = False

        gross = float(w.sum())
        if gross > float(max_gross) + 1e-12:
            w *= float(max_gross) / gross
            changed = True

        over = w > float(max_weight) + 1e-12
        if over.any():
            excess = float((w[over] - float(max_weight)).sum())
            w[over] = float(max_weight)
            room = (~over) & (w > 0.0)
            if room.any() and excess > 0:
                head = np.maximum(float(max_weight) - w[room], 0.0)
                if head.sum() > 0:
                    w[room] += head / head.sum() * min(excess,
                                                       float(head.sum()))
            changed = True

        for key in np.unique(ac):
            m = ac == key
            total = float(w[m].sum())
            if total > float(max_class) + 1e-12:
                w[m] *= float(max_class) / total
                changed = True

        if not changed:
            break
    return w


def build_weights(mapping: str, *, conviction: np.ndarray,
                  expected_return: np.ndarray, predicted_vol: np.ndarray,
                  asset_class: np.ndarray, tradable: np.ndarray,
                  cov: np.ndarray = None,
                  gross: float = _contract.MAX_GROSS_EXPOSURE) -> np.ndarray:
    """One cross-section of weights under one bounded portfolio mapping."""
    n = int(np.asarray(conviction).size)
    trade = np.asarray(tradable, dtype=bool)
    if n == 0 or not trade.any():
        return np.zeros(n)

    c = np.where(np.isfinite(conviction), conviction, 0.0).astype(np.float64)
    c = np.where(trade, c, -np.inf)
    er = np.where(np.isfinite(expected_return), expected_return, 0.0)
    vol = np.clip(np.where(np.isfinite(predicted_vol), predicted_vol, 0.15),
                  0.01, 2.0)

    if mapping == _contract.PORT_LONG_CASH_RANKED:
        k = min(int(_contract.LONG_CASH_TOP_K), int(trade.sum()))
        raw = np.zeros(n)
        if k > 0:
            top = np.argsort(-c)[:k]
            # Only names whose conviction is actually positive are bought; if
            # nothing looks good the book holds cash, which is the answer.
            top = [j for j in top if np.isfinite(c[j]) and c[j] > 0.0]
            if top:
                raw[np.asarray(top)] = 1.0
        w = _normalise_long(raw, gross=gross)

    elif mapping == _contract.PORT_LONG_SCORE_WEIGHTED:
        raw = np.where(np.isfinite(c), np.maximum(c, 0.0), 0.0)
        w = _normalise_long(raw, gross=gross)

    elif mapping == _contract.PORT_VOL_SCALED_LONG_CASH:
        k = min(int(_contract.LONG_CASH_TOP_K), int(trade.sum()))
        raw = np.zeros(n)
        if k > 0:
            top = [j for j in np.argsort(-c)[:k]
                   if np.isfinite(c[j]) and c[j] > 0.0]
            if top:
                idx = np.asarray(top)
                raw[idx] = 1.0 / vol[idx]
        w = _normalise_long(raw, gross=gross)

    elif mapping == _contract.PORT_MEAN_VARIANCE:
        w = _mean_variance(er, vol, trade, cov, gross=gross)

    elif mapping == _contract.PORT_RISK_BUDGET_TILT:
        w = _risk_budget_tilt(c, vol, asset_class, trade, gross=gross)

    else:
        raise ValueError("unknown portfolio mapping %r" % (mapping,))

    return apply_constraints(w, asset_class, max_gross=gross)


def _mean_variance(er: np.ndarray, vol: np.ndarray, trade: np.ndarray,
                   cov: np.ndarray, *, gross: float) -> np.ndarray:
    """Shrunk mean-variance, long-only, no leverage.

    The shrinkage is not a tuning knob: it is fixed at
    ``MEAN_VARIANCE_SHRINKAGE`` in the contract, because an optimiser handed a
    noisy expected-return vector and a sample covariance will reliably find the
    linear combination that maximises estimation error. Shrinking the
    covariance toward its diagonal removes the near-singular directions that
    produce those positions.
    """
    n = er.size
    idx = np.flatnonzero(trade)
    if idx.size == 0:
        return np.zeros(n)
    mu = er[idx]
    if cov is None:
        sigma = np.diag(vol[idx] ** 2)
    else:
        sigma = np.asarray(cov, dtype=np.float64)[np.ix_(idx, idx)]
        sigma = np.where(np.isfinite(sigma), sigma, 0.0)
    lam = float(_contract.MEAN_VARIANCE_SHRINKAGE)
    sigma = (1.0 - lam) * sigma + lam * np.diag(np.diag(sigma))
    sigma = sigma + np.eye(idx.size) * 1e-8
    try:
        raw = np.linalg.solve(sigma, mu)
    except np.linalg.LinAlgError:
        raw = mu / np.maximum(np.diag(sigma), 1e-8)
    out = np.zeros(n)
    out[idx] = np.where(np.isfinite(raw), raw, 0.0)
    return _normalise_long(out, gross=gross)


def _risk_budget_tilt(conviction: np.ndarray, vol: np.ndarray,
                      asset_class: np.ndarray, trade: np.ndarray, *,
                      gross: float) -> np.ndarray:
    """Equal risk across asset classes, tilted by conviction.

    The neutral book is the one a manager with no view would hold: each asset
    class carries the same risk, each instrument inside a class carries the same
    risk. The forecast then TILTS around it, bounded, so the release measures
    what the forecast adds to a sensible default rather than what a forecast
    plus an arbitrary default earns together.
    """
    n = conviction.size
    ac = np.asarray(asset_class)
    neutral = np.zeros(n)
    classes = [k for k in np.unique(ac) if (trade & (ac == k)).any()]
    if not classes:
        return neutral
    per_class = 1.0 / len(classes)
    for key in classes:
        m = trade & (ac == key)
        inv = 1.0 / vol[m]
        neutral[m] = inv / inv.sum() * per_class

    c = np.where(np.isfinite(conviction) & trade, conviction, 0.0)
    sd = float(np.std(c[trade], ddof=1)) if trade.sum() > 1 else 0.0
    tilt = np.clip(c / sd, -1.0, 1.0) if sd > 0 else np.zeros(n)
    raw = neutral * (1.0 + 0.5 * tilt)
    return _normalise_long(raw, gross=gross)


def long_short_weights(conviction: np.ndarray, *, asset_class: np.ndarray,
                       tradable: np.ndarray,
                       gross: float = _contract.MAX_GROSS_EXPOSURE
                       ) -> np.ndarray:
    """SECONDARY research only. The contract forbids this from qualifying.

    Borrow cost, borrow availability and recall risk are not modelled from owned
    data, so a long-short result here is a research diagnostic and never an
    implementability-proven one.
    """
    trade = np.asarray(tradable, dtype=bool)
    c = np.where(np.isfinite(conviction) & trade, conviction, np.nan)
    if np.isfinite(c).sum() < 4:
        return np.zeros(c.size)
    centred = c - np.nanmean(c)
    centred = np.where(np.isfinite(centred), centred, 0.0)
    total = float(np.abs(centred).sum())
    if total <= 0:
        return np.zeros(c.size)
    return centred / total * float(gross)


def trailing_covariance(log_returns, dates, *, window: int = 252,
                        min_periods: int = 126):
    """Trailing sample covariance observed AT each decision date.

    Never a full-sample covariance. A covariance estimated over the whole panel
    knows which correlations were about to break, and a mean-variance book built
    on one is not a portfolio, it is a memory.
    """
    out = {}
    values = log_returns
    for d in dates:
        block = values.loc[:d].tail(int(window))
        if len(block) < int(min_periods):
            out[d] = None
            continue
        cov = block.cov().to_numpy() * 252.0
        out[d] = np.where(np.isfinite(cov), cov, 0.0)
    return out
