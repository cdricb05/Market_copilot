"""alpha_agent.r34.horizon - the ONE horizon-normalised evidence owner.

Every Release-33 finalist was a 60-session configuration, and that was very
nearly an arithmetic artifact rather than a finding. A rank IC measured over 60
sessions is mechanically larger than one measured over 5 - the signal has twelve
times as long to express itself in the realised return - while resting on a
twelfth as many non-overlapping observations. Ranking horizons on raw
primary-metric magnitude therefore selects the longest horizon almost regardless
of evidence, and then reports the selection as a result.

This module fixes that prospectively, with a formula frozen in the contract
before any evaluation:

    HNES(h) = IR_ann(h) * shrink(n_h) * stability(h)

    IR_ann(h)    = mean(g_h) / sd(g_h) * sqrt(252 / h)
    shrink(n_h)  = n_h / (n_h + HNES_SHRINK_N0)
    stability(h) = fraction of training sub-blocks with mean(g_h) > 0

where ``g_h`` is the per-forecast-date after-cost economic gain over the
risk-matched control at horizon ``h``.

Each term answers one of the ways the raw comparison lies. **Annualising**
removes the mechanical horizon scaling - a per-period edge of 1 % earned twelve
times a year and one earned once a year are put on the same footing.
**Shrinking** charges for the smaller observation count that a long horizon
necessarily has, so 12 observations with a flattering ratio cannot outrank 250
with a solid one. **Stability** refuses to reward an effect that lives entirely
in one sub-block of the training data.

Everything here is computed on the TRAINING partition only. A horizon chosen
using the block it is then scored on is not a choice, it is a peek.
"""
from __future__ import annotations

import numpy as np

from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r34.horizon"


def hnes(gain: np.ndarray, *, horizon: int,
         shrink_n0: float = _contract.HNES_SHRINK_N0,
         blocks: int = _contract.HNES_STABILITY_BLOCKS) -> dict:
    """The horizon-normalised evidence score for one horizon's gain series."""
    g = np.asarray(gain, dtype=np.float64)
    g = g[np.isfinite(g)]
    n = int(g.size)
    if n < 8:
        return {"horizon": int(horizon), "n": n, "hnes": None,
                "state": "INSUFFICIENT_OBSERVATIONS"}
    sd = float(g.std(ddof=1))
    if sd <= 0:
        return {"horizon": int(horizon), "n": n, "hnes": None,
                "state": "DEGENERATE_GAIN"}
    ir_ann = float(g.mean() / sd * np.sqrt(252.0 / float(horizon)))
    shrink = float(n / (n + float(shrink_n0)))
    parts = np.array_split(g, max(int(blocks), 1))
    stability = float(np.mean([1.0 if p.size and float(p.mean()) > 0 else 0.0
                               for p in parts]))
    return {"horizon": int(horizon), "n": n,
            "mean_gain": float(g.mean()), "sd_gain": sd,
            "ir_annualised": ir_ann, "shrink": shrink,
            "stability": stability,
            "hnes": float(ir_ann * shrink * stability),
            # The score is built to ORDER horizons that have an edge. When the
            # annualised IR is negative the multiplicative form inverts: the
            # least stable horizon multiplies its negative IR by a smaller
            # stability and scores HIGHER. That is not a defect to be patched
            # by changing the formula after seeing results - it is a range in
            # which the score carries no ordering information, and
            # ``combination_weights`` already refuses to act on it by clamping
            # at zero and degrading to equal weight. The flag says so out loud.
            "ordering_is_meaningful": bool(ir_ann > 0),
            "state": "OK"}


def rank_horizons(per_horizon_gain: dict) -> list:
    """Score every horizon and order them. NEVER by raw metric magnitude."""
    rows = [hnes(g, horizon=h) for h, g in sorted(per_horizon_gain.items())]
    scored = [r for r in rows if r.get("hnes") is not None]
    scored.sort(key=lambda r: -r["hnes"])
    return rows if not scored else scored + [r for r in rows
                                             if r.get("hnes") is None]


def combination_weights(method: str, per_horizon_gain: dict,
                        horizons: tuple) -> dict:
    """Weights for a horizon set, fixed a priori or trained IN TRAINING.

    ``EQUAL_WEIGHT_A_PRIORI`` needs no data at all, which is exactly its virtue:
    it cannot overfit, and any trained scheme has to beat it to earn its extra
    degrees of freedom.

    ``HNES_PROPORTIONAL`` uses only the training-partition gain series. Negative
    HNES contributes zero rather than a negative weight - a horizon that lost
    money in training is dropped, not shorted, because shorting a horizon is a
    different hypothesis that this campaign did not pre-register.
    """
    hs = tuple(int(h) for h in horizons)
    if method == _contract.COMBINE_EQUAL:
        w = {h: 1.0 / len(hs) for h in hs}
        return {"method": method, "weights": w, "trained": False}

    if method != _contract.COMBINE_HNES:
        raise ValueError("unknown combination method %r" % (method,))

    scores = {}
    for h in hs:
        g = per_horizon_gain.get(h)
        s = hnes(g, horizon=h) if g is not None else {"hnes": None}
        scores[h] = max(float(s.get("hnes") or 0.0), 0.0)
    total = float(sum(scores.values()))
    if total <= 0:
        w = {h: 1.0 / len(hs) for h in hs}
        return {"method": method, "weights": w, "trained": True,
                "degraded_to_equal": True,
                "reason": "no horizon in this set had positive training HNES"}
    return {"method": method, "weights": {h: scores[h] / total for h in hs},
            "trained": True, "hnes_scores": scores, "degraded_to_equal": False}


def combine_forecasts(per_horizon_conviction: dict, weights: dict
                      ) -> np.ndarray:
    """Blend per-horizon convictions, each standardised before blending.

    Standardising first is not cosmetic: a 60-session conviction has a naturally
    wider spread than a 5-session one, so blending the raw vectors would let the
    long horizon dominate the mix through its scale no matter what weight it was
    given - which is the same defect this module exists to remove, moved one
    step downstream.
    """
    stacked, used = None, 0.0
    for h, w in sorted(weights.items()):
        v = per_horizon_conviction.get(h)
        if v is None:
            continue
        a = np.asarray(v, dtype=np.float64)
        a = np.where(np.isfinite(a), a, 0.0)
        sd = float(a.std(ddof=1)) if a.size > 1 else 0.0
        z = a / sd if sd > 0 else np.zeros_like(a)
        stacked = z * float(w) if stacked is None else stacked + z * float(w)
        used += float(w)
    if stacked is None:
        return np.zeros(0)
    return stacked / used if used > 0 else stacked
