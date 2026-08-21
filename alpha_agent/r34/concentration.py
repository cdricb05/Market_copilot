"""alpha_agent.r34.concentration - the ONE single-dependency owner.

Release 33's five lockbox finalists all showed positive after-cost excess.
Leave-one-market-out attributed every bit of it to one currency: removing
``TRYUSD`` moved mean excess from ``+0.0041`` to ``-0.0069``. A broad
cross-market edge turned out to be a short-the-lira trade, and it was visible
only because leave-market-out was a REQUIRED diagnostic rather than an optional
extra.

This module makes the same failure impossible to hide, and it does so with
thresholds frozen in the contract before any economic result exists. That
ordering is the whole point: a concentration threshold invented after a
finalist's concentration is known is not a gate, it is a rationalisation.

Five measurements per finalist:

* **leave-one-instrument-out** - re-evaluate the book with each instrument's
  contribution removed and its weight sent to cash. A SIGN REVERSAL in
  after-cost excess disqualifies outright;
* **leave-one-asset-class-out** - the same, one whole asset class at a time,
  because a result can survive dropping any single fund and still be one bet;
* **contribution concentration** - the largest single instrument's share of
  gross PnL;
* **exposure concentration** - the largest mean weight, and the effective number
  of instruments by inverse Herfindahl;
* **PnL concentration over time** - the largest single sub-period's share, so an
  edge that lived entirely in one crisis is not sold as a persistent one.
"""
from __future__ import annotations

import numpy as np

from . import contract as _contract
from . import economics as _economics
from . import sizing as _sizing

CALCULATION_OWNER = "alpha_agent.r34.concentration"


def _rebuild_without(path: dict, drop_cols: list) -> np.ndarray:
    """The book's net path with some columns' contribution and cost removed.

    Weight is NOT redistributed to the survivors. Redistribution would answer a
    different question - "what if I had run a book that never held this?" -
    which requires refitting everything downstream. The question asked here is
    the attribution one: how much of what this book earned came from these
    instruments? So their contribution goes to cash and everything else stays
    exactly as it was.
    """
    cols = list(path["columns"])
    drop = [cols.index(c) for c in drop_cols if c in cols]
    if not drop:
        return np.asarray(path["net"], dtype=np.float64)

    contrib = np.asarray(path["contribution"], dtype=np.float64).copy()
    contrib[:, drop] = 0.0

    # The cost of the dropped legs goes too, recomputed on traded notional at
    # the SAME per-instrument rates the original path was charged.
    Wd = np.asarray(path["weights"], dtype=np.float64).copy()
    Wd[:, drop] = 0.0
    rates = np.asarray(path["cost_rates_used"], dtype=np.float64)
    prev = np.zeros(Wd.shape[1])
    costs = []
    for k in range(Wd.shape[0]):
        dw = np.abs(Wd[k] - prev)
        costs.append(float((dw * rates).sum()))
        prev = Wd[k]
    return (np.asarray(path["cash_leg"], dtype=np.float64)
            + contrib.sum(axis=1) - np.asarray(costs, dtype=np.float64))


def analyse(path: dict, control: np.ndarray, *, meta: dict, horizon: int
            ) -> dict:
    """Every concentration measurement the contract requires, plus its verdict."""
    cols = list(path["columns"])
    net = np.asarray(path["net"], dtype=np.float64)
    ctrl = np.asarray(control, dtype=np.float64)
    n = min(net.size, ctrl.size)
    base = _economics.excess_significance(net[:n], ctrl[:n], horizon=horizon)
    base_excess = base.get("annualised_excess")
    base_utility = (_economics.utility(net[:n], horizon=horizon)
                    - _economics.utility(ctrl[:n], horizon=horizon))

    contrib = np.asarray(path["contribution"], dtype=np.float64)
    total_gross = float(np.abs(contrib).sum())
    per_instrument_pnl = contrib.sum(axis=0)
    per_instrument_share = (np.abs(per_instrument_pnl) / total_gross
                            if total_gross > 0
                            else np.zeros(len(cols)))

    W = np.asarray(path["weights"], dtype=np.float64)
    mean_weight = np.abs(W).mean(axis=0)
    eff = _sizing.effective_instruments(mean_weight)

    # Leave one instrument out
    loo = []
    reversed_by = []
    for j, sym in enumerate(cols):
        alt = _rebuild_without(path, [sym])
        sig = _economics.excess_significance(alt[:n], ctrl[:n], horizon=horizon)
        e = sig.get("annualised_excess")
        u = (_economics.utility(alt[:n], horizon=horizon)
             - _economics.utility(ctrl[:n], horizon=horizon))
        flips = (base_excess is not None and e is not None
                 and np.isfinite(base_excess) and np.isfinite(e)
                 and np.sign(base_excess) != np.sign(e))
        if flips:
            reversed_by.append(sym)
        loo.append({"instrument": sym,
                    "asset_class": meta.get(sym, {}).get("asset_class"),
                    "excess_without": e, "utility_delta_without": u,
                    "pnl_share": float(per_instrument_share[j]),
                    "mean_weight": float(mean_weight[j]),
                    "sign_reversed": bool(flips)})

    # Leave one asset class out
    classes = sorted({meta.get(s, {}).get("asset_class") for s in cols}
                     - {None})
    lco, class_reversed = [], []
    class_pnl = {}
    for key in classes:
        members = [s for s in cols if meta.get(s, {}).get("asset_class") == key]
        class_pnl[key] = float(abs(sum(
            per_instrument_pnl[cols.index(s)] for s in members)))
        alt = _rebuild_without(path, members)
        sig = _economics.excess_significance(alt[:n], ctrl[:n], horizon=horizon)
        e = sig.get("annualised_excess")
        flips = (base_excess is not None and e is not None
                 and np.isfinite(base_excess) and np.isfinite(e)
                 and np.sign(base_excess) != np.sign(e))
        if flips:
            class_reversed.append(key)
        lco.append({"asset_class": key, "instruments": len(members),
                    "excess_without": e, "sign_reversed": bool(flips),
                    "pnl_share": (class_pnl[key] / total_gross
                                  if total_gross > 0 else 0.0)})

    # PnL concentration over time
    diff = np.asarray(base.get("diff", np.zeros(0)), dtype=np.float64)
    blocks = np.array_split(diff, 4) if diff.size >= 8 else []
    block_sums = [float(b.sum()) for b in blocks]
    total_abs = float(sum(abs(v) for v in block_sums))
    max_block_share = (max(abs(v) for v in block_sums) / total_abs
                       if total_abs > 0 else 0.0)

    max_instrument_share = float(per_instrument_share.max()) \
        if per_instrument_share.size else 0.0
    max_class_share = (max(class_pnl.values()) / total_gross
                       if (class_pnl and total_gross > 0) else 0.0)
    max_mean_weight = float(mean_weight.max()) if mean_weight.size else 0.0

    # Is the sign test capable of saying anything? A leave-one-out sign flip is
    # evidence of dependence only when the base result has a sign worth
    # flipping. When the base excess is statistically indistinguishable from
    # zero, EVERY removal flips it and the test reports total dependence on
    # every instrument at once - which is a statement about the base, not about
    # concentration. The gate is not loosened for this; the reading is labelled
    # so it cannot be misread as R33's TRYUSD finding.
    base_t = base.get("t_stat")
    informative = bool(base_t is not None and np.isfinite(float(base_t))
                       and abs(float(base_t)) >= 1.0)
    gates = {
        "no_sign_reversal_on_leave_one_instrument_out": not reversed_by,
        "no_sign_reversal_on_leave_one_asset_class_out": not class_reversed,
        "single_instrument_pnl_share_within_limit":
            max_instrument_share <= _contract.MAX_SINGLE_INSTRUMENT_PNL_SHARE,
        "single_asset_class_pnl_share_within_limit":
            max_class_share <= _contract.MAX_SINGLE_ASSET_CLASS_PNL_SHARE,
        "mean_single_instrument_exposure_within_limit":
            max_mean_weight <= _contract.MAX_MEAN_SINGLE_INSTRUMENT_EXPOSURE,
        "enough_effective_instruments":
            eff >= _contract.MIN_EFFECTIVE_INSTRUMENTS,
    }
    return {
        "calculation_owner": CALCULATION_OWNER,
        "base_after_cost_excess_annualised": base_excess,
        "base_after_cost_excess_utility": (
            float(base_utility) if np.isfinite(base_utility) else None),
        "leave_one_instrument_out": loo,
        "leave_one_asset_class_out": lco,
        "base_after_cost_excess_t_stat": base_t,
        "sign_reversal_test_is_informative": informative,
        "sign_reversal_reading": (
            "the base after-cost excess is significantly different from zero, "
            "so a leave-one-out sign reversal is evidence of dependence"
            if informative else
            "the base after-cost excess is statistically indistinguishable "
            "from zero, so almost any removal reverses its sign; the reversal "
            "lists below measure the base's proximity to zero and NOT "
            "concentration - read max_single_instrument_pnl_share and "
            "effective_instruments instead"),
        "instruments_that_reverse_the_sign": reversed_by,
        "asset_classes_that_reverse_the_sign": class_reversed,
        "max_single_instrument_pnl_share": max_instrument_share,
        "max_single_asset_class_pnl_share": max_class_share,
        "max_mean_single_instrument_exposure": max_mean_weight,
        "effective_instruments": eff,
        "max_subperiod_pnl_share": float(max_block_share),
        "thresholds": {
            "max_single_instrument_pnl_share":
                _contract.MAX_SINGLE_INSTRUMENT_PNL_SHARE,
            "max_single_asset_class_pnl_share":
                _contract.MAX_SINGLE_ASSET_CLASS_PNL_SHARE,
            "max_mean_single_instrument_exposure":
                _contract.MAX_MEAN_SINGLE_INSTRUMENT_EXPOSURE,
            "min_effective_instruments":
                _contract.MIN_EFFECTIVE_INSTRUMENTS,
            "frozen_before_evaluation":
                _contract.CONCENTRATION_GATE_FROZEN_BEFORE_EVALUATION,
        },
        "gates": gates,
        "passes": all(gates.values()),
    }
