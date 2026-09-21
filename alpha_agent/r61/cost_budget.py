r"""alpha_agent.r61.cost_budget - THE annualised cost-budget gate (Workstream A).

RESEARCH ONLY. This module computes and applies an ECONOMIC qualification
constraint. It creates no order, no fill, no position and no promotion.

What it replaces, and why the old gate was wrong
------------------------------------------------
R59 froze ``GATE_MAX_TURNOVER = 0.40``: a single one-way turnover scalar,
applied to every cell in every asset class at every horizon. R60 proved that
misspecified, and the director owned it as a pre-registration defect.

A turnover of 0.60 is not one economic fact. It is three:

    EQ_EXTENSION_H5_IMMEDIACY_REVERSAL   0.870 one-way, 25 bp/side, ~50.4
                                         rebalances/yr  ->  ~21.9%/yr drag
    COMMODITY_FUTURES OI growth          0.598 one-way,  5 bp/side, 12
                                         rebalances/yr  ->  ~0.72%/yr drag
    EQUITY_INDEX_FUTURES OI growth       0.612 one-way,  5 bp/side, 12
                                         rebalances/yr  ->  ~0.73%/yr drag

The same 0.40 scalar killed all three, before any return was scored. For the
equity cell that was CORRECT IN OUTCOME and wrong in form: no realistic
cross-sectional premium survives a 22%/yr drag. For the two futures cells it
was simply an equity cost imported into a world that does not pay it, and it
destroyed two of that campaign's four non-equity cells for nothing.

Turnover is not the burden. COST is the burden, and cost is turnover times a
rate times a frequency.

THE CANONICAL CALCULATION
-------------------------
::

    annualized_rebalance_cost_drag =
        one_way_turnover * 2 * cost_per_side * rebalances_per_year

    annualized_cost_drag =
        annualized_rebalance_cost_drag + additional_ann_cost_drag

UNITS, stated once and pinned by tests:

    one_way_turnover            dimensionless fraction of gross notional
                                traded on ONE side per rebalance. 0.870 means
                                87% of the book is replaced.
    the factor 2                a one-way turnover of x sells x and buys x, so
                                2x of notional crosses the spread.
    cost_per_side               a RATE on traded notional, not basis points.
                                25 bp/side is 0.0025. The R38 manifest states
                                basis points and its owner divides by 1e4
                                before the number reaches here.
    rebalances_per_year         252 / rebalance_interval_sessions. A 21-session
                                cadence is 12.0/yr; a 5-session hold is 50.4/yr.
    additional_ann_cost_drag    annualised drag the rebalance formula does not
                                model - for a dated-contract futures book, the
                                ROLL cost. Defaults to 0.0 and must be passed
                                explicitly when it is known, because a futures
                                book's roll cost is real money and omitting it
                                understates the burden.

    the result                  an annualised RETURN DRAG, same units as
                                ``ann_net_excess``: 0.219 is 21.9%/yr.

THE CEILING, AND WHY IT IS THIS NUMBER
--------------------------------------
``COST_BUDGET_CEILING = 2 x r59.GATE_MATERIALITY = 0.03`` - three percent of
annualised cost drag.

It is derived from a constant the estate froze long before this release and
for an unrelated reason: a candidate must deliver at least 1.5%/yr NET excess
to be material. Since ``net = gross - drag``, a cell sitting exactly at the
ceiling needs a GROSS premium of 4.5%/yr merely to reach the floor it must
clear anyway. The declared appetite is therefore: **a cell may spend on
trading at most twice what it must ultimately deliver.** Above that, the
required gross premium is larger than anything this estate's evaluation
conventions were built to find, so the cell cannot qualify however good its
signal is, and the burden spent measuring it is waste.

Three properties this ceiling has, all of them required:

* It is CAMPAIGN-WIDE and PREDECLARED - one number, frozen in this module,
  versioned by ``COST_BUDGET_VERSION`` and bound into the gate schema hash.
* It is INDEPENDENT OF ANY CANDIDATE'S REALIZED RETURN. It is a multiple of a
  frozen floor, and no measured return enters its derivation. It was not
  chosen by looking at what r60_05, r60_07 or r60_08 happened to produce.
* It REPRODUCES THE OLD GATE where the old gate was calibrated. Monthly equity
  at 25 bp/side sitting exactly on the retired 0.40 scalar costs
  0.40 x 2 x 0.0025 x 12 = 2.4%/yr, just inside the new ceiling. The two gates
  agree in the world the scalar came from and differ exactly where it was
  blind: a different cost, or a different frequency.

WHAT DID NOT CHANGE
-------------------
Raw one-way turnover is still measured and still reported, as a DIAGNOSTIC. It
is no longer the universal economic qualification gate, and nothing in the
governed path may refuse a cell on it alone.

The three R60 halts STAND. Nothing here re-runs, re-scores or rewrites them.
"""
from __future__ import annotations

from typing import Optional

from .. import r59

COST_BUDGET_OWNER = "alpha_agent.r61.cost_budget"
COST_BUDGET_VERSION = "R61_ANNUALISED_COST_BUDGET_V1"

#: Sessions in a trading year. The same constant the layer-statistics owners
#: annualise with (``PPY / cadence``), so a projected drag and a measured drag
#: are on one scale.
TRADING_SESSIONS_PER_YEAR = 252.0

#: The frozen, campaign-wide ceiling. See the module docstring for the
#: derivation. Expressed as a multiple so the derivation cannot drift away
#: from the constant it is derived from.
COST_BUDGET_CEILING_MULTIPLE = 2.0
COST_BUDGET_CEILING = COST_BUDGET_CEILING_MULTIPLE * r59.GATE_MATERIALITY

#: The retired scalar. Kept ONLY so a report can say what was replaced and so
#: the diagnostic can be compared against it. Never gate on this.
RETIRED_TURNOVER_CEILING = r59.GATE_MAX_TURNOVER
RETIRED_TURNOVER_CEILING_NOTE = (
    "RETIRED as the universal economic qualification gate by R61. Reported as "
    "a diagnostic only. It was horizon-blind and cost-blind: it priced a "
    "liquid futures book at a mid-cap equity cost.")

#: Outcomes of an evaluation.
BUDGET_PASS = "WITHIN_COST_BUDGET"
BUDGET_HALT = "COST_BUDGET_EXCEEDED"
BUDGET_NOT_EVALUABLE = "COST_BUDGET_NOT_EVALUABLE"


class CostBudgetRefusal(RuntimeError):
    """The cost budget could not be computed from what it was given."""


def _positive_float(value, name: str) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        raise CostBudgetRefusal("%s is not a number: %r" % (name, value)) from None
    if f != f:
        raise CostBudgetRefusal("%s is NaN" % name)
    if f < 0.0:
        raise CostBudgetRefusal("%s must not be negative: %r" % (name, f))
    return f


# --------------------------------------------------------------------------- #
# The arithmetic
# --------------------------------------------------------------------------- #
def rebalances_per_year(rebalance_interval_sessions) -> float:
    """252 / interval. A 21-session cadence is 12.0/yr; a 5-session hold 50.4."""
    interval = _positive_float(rebalance_interval_sessions,
                               "rebalance_interval_sessions")
    if interval <= 0.0:
        raise CostBudgetRefusal(
            "rebalance_interval_sessions must be > 0; a book that never "
            "rebalances has no rebalance cost and needs no budget")
    return TRADING_SESSIONS_PER_YEAR / interval


def annualized_rebalance_cost_drag(*, one_way_turnover: float,
                                   cost_per_side: float,
                                   rebalances_per_year: float) -> float:
    """THE canonical calculation. See the module docstring for every unit."""
    t = _positive_float(one_way_turnover, "one_way_turnover")
    c = _positive_float(cost_per_side, "cost_per_side")
    n = _positive_float(rebalances_per_year, "rebalances_per_year")
    return t * 2.0 * c * n


def annualized_cost_drag(*, one_way_turnover: float, cost_per_side: float,
                         rebalances_per_year: float,
                         additional_ann_cost_drag: float = 0.0) -> float:
    """Total projected annual drag: rebalance plus anything else known.

    ``additional_ann_cost_drag`` exists because a dated-contract futures book
    pays to ROLL as well as to rebalance, and that cost is a property of the
    instrument set and the horizon rather than of the signal. It is therefore
    projectable before any return is scored, and leaving it out would let a
    futures cell understate its own burden.
    """
    return (annualized_rebalance_cost_drag(
        one_way_turnover=one_way_turnover, cost_per_side=cost_per_side,
        rebalances_per_year=rebalances_per_year)
        + _positive_float(additional_ann_cost_drag,
                          "additional_ann_cost_drag"))


def required_gross_for_materiality(drag: float,
                                   materiality: float = r59.GATE_MATERIALITY
                                   ) -> float:
    """The gross premium a cell paying ``drag`` needs to reach materiality."""
    return float(drag) + float(materiality)


# --------------------------------------------------------------------------- #
# Resolving the inputs from a FROZEN pre-registration
# --------------------------------------------------------------------------- #
def cost_per_side_from_model(cost_model: Optional[dict]) -> Optional[float]:
    """The numeric per-side rate a frozen cost model states, or None.

    NONE IS NOT ZERO. ``FUTURES_PER_MARKET_R38_PLUS_ROLL_V1`` states its rate
    as the prose "per market, 2-15 bp" because the rate is a per-instrument
    vector, and a reader that coerced that to 0.0 would wave every futures
    cell through on a cost of nothing. The caller must supply the measured
    notional-weighted effective rate instead, and a caller that supplies
    neither gets ``COST_BUDGET_NOT_EVALUABLE`` - never a pass.
    """
    if not isinstance(cost_model, dict):
        return None
    for key in ("effective_rate_per_side", "rate_per_side"):
        v = cost_model.get(key)
        if isinstance(v, bool):
            continue
        if isinstance(v, (int, float)):
            f = float(v)
            if f == f and f >= 0.0:
                return f
    return None


def rebalance_interval_from_spec(spec: Optional[dict]) -> Optional[float]:
    """How often the frozen spec says the book re-forms, in sessions.

    ``hold_sessions`` wins over ``cadence_sessions`` because a daily-tranche
    book forms a tranche every session and replaces each one after ``hold``:
    the turnover it reports is per HOLD, so the frequency must be too. R60's
    5-session book is 252/5 = 50.4 rebalances a year, which is where its
    ~21.9%/yr drag comes from and why a monthly ceiling never fitted it.
    """
    if not isinstance(spec, dict):
        return None
    params = spec.get("parameters") or {}
    for src in (params, spec):
        for key in ("hold_sessions", "cadence_sessions"):
            v = src.get(key)
            if isinstance(v, (int, float)) and not isinstance(v, bool) \
                    and float(v) > 0:
                return float(v)
    v = spec.get("horizon_sessions")
    if isinstance(v, (int, float)) and not isinstance(v, bool) and float(v) > 0:
        return float(v)
    return None


# --------------------------------------------------------------------------- #
# The gate
# --------------------------------------------------------------------------- #
def evaluate_cost_budget(*, one_way_turnover, cost_per_side, rebalance_interval_sessions,
             additional_ann_cost_drag: float = 0.0,
             ceiling: float = COST_BUDGET_CEILING,
             label: str = "") -> dict:
    """Apply the frozen cost budget. Returns a RESULT, never raises on a fail.

    Named ``evaluate_cost_budget`` rather than ``evaluate``: the bare name is
    reserved by the research-agent kernel's sole-calculation-owner
    invariant, and a module that claimed it would read as a second owner
    of a concept it has nothing to do with.

    A missing or non-numeric input yields ``COST_BUDGET_NOT_EVALUABLE`` with
    the reason named. That state is a FAILURE for every governed caller: a
    budget that could not be computed has not been met.
    """
    reasons = []
    try:
        interval = _positive_float(rebalance_interval_sessions,
                                   "rebalance_interval_sessions")
        if interval <= 0:
            raise CostBudgetRefusal("rebalance_interval_sessions must be > 0")
        per_year = rebalances_per_year(interval)
    except CostBudgetRefusal as exc:
        interval, per_year = None, None
        reasons.append(str(exc))
    for name, value in (("one_way_turnover", one_way_turnover),
                        ("cost_per_side", cost_per_side),
                        ("additional_ann_cost_drag", additional_ann_cost_drag)):
        try:
            _positive_float(value, name)
        except CostBudgetRefusal as exc:
            reasons.append(str(exc))

    base = {
        "owner": COST_BUDGET_OWNER,
        "version": COST_BUDGET_VERSION,
        "label": str(label or ""),
        "metric": "annualized_cost_drag",
        "frozen_threshold": float(ceiling),
        "frozen_threshold_derivation":
            "%.1f x r59.GATE_MATERIALITY (%.4f)"
            % (COST_BUDGET_CEILING_MULTIPLE, r59.GATE_MATERIALITY),
        "inputs": {
            "one_way_turnover": one_way_turnover,
            "cost_per_side": cost_per_side,
            "rebalance_interval_sessions": rebalance_interval_sessions,
            "rebalances_per_year": per_year,
            "additional_ann_cost_drag": additional_ann_cost_drag,
            "trading_sessions_per_year": TRADING_SESSIONS_PER_YEAR,
        },
        # DIAGNOSTIC ONLY. Reported so a reader can see what the retired gate
        # would have said; nothing in the governed path may act on it.
        "diagnostic_retired_turnover_ceiling": RETIRED_TURNOVER_CEILING,
        "diagnostic_retired_gate_would_halt": (
            None if not isinstance(one_way_turnover, (int, float))
            or isinstance(one_way_turnover, bool)
            else bool(float(one_way_turnover) > RETIRED_TURNOVER_CEILING)),
        "diagnostic_note": RETIRED_TURNOVER_CEILING_NOTE,
    }
    if reasons:
        return {**base, "state": BUDGET_NOT_EVALUABLE, "passed": False,
                "measured": None, "reasons": reasons}

    rebal = annualized_rebalance_cost_drag(
        one_way_turnover=one_way_turnover, cost_per_side=cost_per_side,
        rebalances_per_year=per_year)
    total = rebal + float(additional_ann_cost_drag)
    passed = total <= float(ceiling)
    return {
        **base,
        "state": BUDGET_PASS if passed else BUDGET_HALT,
        "passed": bool(passed),
        "measured": float(total),
        "annualized_rebalance_cost_drag": float(rebal),
        "annualized_roll_or_other_cost_drag": float(additional_ann_cost_drag),
        "required_gross_for_materiality":
            required_gross_for_materiality(total),
        "materiality_floor": float(r59.GATE_MATERIALITY),
        "reasons": ([] if passed else [
            "COST_BUDGET_EXCEEDED: projected annual cost drag %.6f against "
            "the pre-registered ceiling %.6f; this construction would need a "
            "gross premium of %.6f/yr merely to reach the %.4f materiality "
            "floor" % (total, float(ceiling),
                       required_gross_for_materiality(total),
                       r59.GATE_MATERIALITY)]),
    }


def evaluate_frozen_spec(spec: Optional[dict], *, one_way_turnover,
                         cost_per_side: Optional[float] = None,
                         additional_ann_cost_drag: float = 0.0,
                         ceiling: float = COST_BUDGET_CEILING) -> dict:
    """Apply the budget using the rate and cadence a pre-registration froze.

    ``cost_per_side`` overrides the frozen model, and must be supplied when
    the frozen model states a per-instrument vector rather than a scalar. It
    is the caller's measured notional-weighted effective rate; there is no
    default, because defaulting a cost is how a cell passes on a cost nobody
    charged it.
    """
    spec = spec or {}
    rate = (cost_per_side if cost_per_side is not None
            else cost_per_side_from_model(spec.get("cost_model")))
    interval = rebalance_interval_from_spec(spec)
    out = evaluate_cost_budget(one_way_turnover=one_way_turnover,
                   cost_per_side=rate,
                   rebalance_interval_sessions=interval,
                   additional_ann_cost_drag=additional_ann_cost_drag,
                   ceiling=ceiling,
                   label=str((spec.get("parameters") or {}).get("label") or ""))
    out["cost_model_id"] = (spec.get("cost_model") or {}).get("cost_model_id")
    out["cost_per_side_source"] = ("CALLER_MEASURED_EFFECTIVE"
                                   if cost_per_side is not None
                                   else "FROZEN_COST_MODEL")
    if rate is None and out["state"] == BUDGET_NOT_EVALUABLE:
        out.setdefault("reasons", []).append(
            "the frozen cost model states no scalar rate_per_side; the caller "
            "must supply the measured notional-weighted effective rate")
    return out


def effective_cost_per_side(traded_notional_per_instrument,
                            cost_per_side_vector) -> Optional[float]:
    """The notional-weighted mean per-side rate a book actually pays.

    For a per-market futures book this is the only honest scalar: charging the
    cheapest market's rate flatters the cell and charging the dearest one
    kills it. Returns None when nothing was traded, because a rate is
    undefined on zero notional and 0.0 would be a lie.
    """
    import numpy as np

    w = np.asarray(traded_notional_per_instrument, dtype=np.float64)
    c = np.asarray(cost_per_side_vector, dtype=np.float64)
    if w.shape != c.shape:
        raise CostBudgetRefusal(
            "traded notional %s and cost vector %s do not align"
            % (w.shape, c.shape))
    ok = np.isfinite(w) & np.isfinite(c) & (w > 0)
    total = float(np.abs(w[ok]).sum())
    if total <= 0.0:
        return None
    return float((np.abs(w[ok]) * c[ok]).sum() / total)
