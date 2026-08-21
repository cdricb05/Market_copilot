"""alpha_agent.r34.turnover - the ONE cost-aware transition owner.

Prediction can be real and economically unusable. A forecast that changes every
month produces a book that trades every month, and at 3 basis points a side on
traded notional a book that turns over 400 % a year pays 24 basis points before
it earns anything. Release 33 charged cost correctly - on traded notional,
sells and buys - and never tested whether a transition rule could keep the edge
while spending less of it.

The objective here is NOT to minimise turnover. A book that never trades has
zero cost and zero edge, and a release that ranked on turnover would select it.
The objective declared in the contract is to maximise expected AFTER-COST
UTILITY, and each rule below is a different bounded answer to the same question:
given where the book is and where the forecast says it should be, how much of
that distance is worth paying for?

The parameter inside each rule (band width, adjustment speed, penalty) is chosen
INSIDE the training partition of each walk-forward fold and never on the block
the rule is scored on.
"""
from __future__ import annotations

import numpy as np

from . import contract as _contract
from . import portfolio as _portfolio

CALCULATION_OWNER = "alpha_agent.r34.turnover"


def transition(rule: str, *, previous: np.ndarray, target: np.ndarray,
               expected_return: np.ndarray = None,
               cost_rate: np.ndarray = None,
               asset_class: np.ndarray = None,
               param: float = 0.0,
               gross: float = _contract.MAX_GROSS_EXPOSURE) -> np.ndarray:
    """Where the book actually goes, given where it is and where it wants to be.

    Every rule returns a weight vector that is re-projected onto the frozen
    constraint set, because a transition that stops short of the target can
    otherwise leave the book holding a position the caps would have refused.
    """
    prev = np.where(np.isfinite(previous), previous, 0.0).astype(np.float64)
    tgt = np.where(np.isfinite(target), target, 0.0).astype(np.float64)
    ac = np.asarray(asset_class) if asset_class is not None \
        else np.zeros(prev.size)

    if rule == _contract.TURN_IMMEDIATE:
        out = tgt

    elif rule == _contract.TURN_NO_TRADE_BAND:
        # Only names that have drifted far enough to be worth the spread move;
        # the rest stay exactly where they are.
        delta = tgt - prev
        out = np.where(np.abs(delta) > float(param), tgt, prev)

    elif rule == _contract.TURN_FORECAST_CHANGE:
        # The whole book rebalances only when the target has changed
        # materially. Between rebalances it drifts, which is what a real book
        # does and what a per-period reoptimisation quietly ignores.
        change = float(np.abs(tgt - prev).sum())
        base = max(float(np.abs(prev).sum()), 1e-9)
        out = tgt if (change / base) > float(param) else prev

    elif rule == _contract.TURN_PARTIAL:
        out = prev + float(param) * (tgt - prev)

    elif rule == _contract.TURN_PENALISED:
        # Soft-threshold each leg by whether its expected gain covers its cost.
        # A trade whose expected return per unit traded is smaller than the
        # penalty-weighted cost of making it is simply not made, and a trade
        # worth twice its cost is made in full.
        er = np.abs(np.where(np.isfinite(expected_return), expected_return, 0.0)
                    ) if expected_return is not None else np.zeros(prev.size)
        rate = (np.asarray(cost_rate, dtype=np.float64)
                if cost_rate is not None else np.full(prev.size, 3e-4))
        hurdle = float(param) * rate
        keep = np.clip(1.0 - hurdle / np.maximum(er, 1e-9), 0.0, 1.0)
        out = prev + keep * (tgt - prev)

    else:
        raise ValueError("unknown turnover rule %r" % (rule,))

    return _portfolio.apply_constraints(out, ac, max_gross=gross)


def parameter_grid(rule: str) -> tuple:
    """The frozen grid searched INSIDE the training partition, per rule."""
    return {
        _contract.TURN_IMMEDIATE: (0.0,),
        _contract.TURN_NO_TRADE_BAND: _contract.NO_TRADE_BAND_GRID,
        _contract.TURN_FORECAST_CHANGE: _contract.FORECAST_CHANGE_GRID,
        _contract.TURN_PARTIAL: _contract.PARTIAL_ADJUSTMENT_GRID,
        _contract.TURN_PENALISED: _contract.TURNOVER_PENALTY_GRID,
    }[rule]


def describe_rule(rule: str) -> dict:
    return {
        _contract.TURN_IMMEDIATE:
            "move to the target every period, whatever it costs",
        _contract.TURN_NO_TRADE_BAND:
            "trade only the names that have drifted beyond the band",
        _contract.TURN_FORECAST_CHANGE:
            "rebalance the whole book only when the target changes materially",
        _contract.TURN_PARTIAL:
            "close a fixed fraction of the distance to the target each period",
        _contract.TURN_PENALISED:
            "trade each leg only insofar as its expected gain covers its cost",
    }.get(rule, rule)
