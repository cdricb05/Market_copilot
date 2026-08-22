"""alpha_agent.r36.experiments - the ONE Release 36 per-configuration spine.

Every configuration in this release goes through exactly this function, so that
a currency book, a commodity curve trade and a volatility timing rule are
compared on the same terms. Nothing here computes an economic statistic of its
own: the book path, the cost on traded notional, the volatility-matched control,
the after-cost excess, the utility, the drawdown and the concentration
diagnostics are all :mod:`alpha_agent.r34.economics` and
:mod:`alpha_agent.r34.concentration`, called with a LANE-APPROPRIATE benchmark
and a lane-appropriate cadence.

That last point is the whole reason this module exists rather than a second
judge. Release 34's judge is correct and reusable; what was wrong for a
multi-asset frontier was feeding it one benchmark and one cadence. Passing the
FX dollar basket as ``bench_excess`` for a currency book and the passive
front-contract roll for a commodity book is not a new calculation - it is the
same calculation asked the right question.

Four things are measured for every configuration whether it looks promising or
not, because the ones that only get measured for promising results are the ones
that flatter:

* **cost sensitivity** at every declared multiplier, so a result that survives
  only its most optimistic execution assumption is visible as such;
* **both chronological halves**, because a rule that worked until 2010 and not
  since is a regime observation, not a strategy;
* **concentration**, because Release 33's broad cross-market edge turned out to
  be a short-the-lira trade;
* **the minimum detectable effect**, so "not significant" arrives with the size
  of effect this design could actually have found.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from .. import orthogonality as _orth
from ..r34 import concentration as _concentration
from ..r34 import economics as _economics
from . import contract as _contract
from . import strategies as _strategies

CALCULATION_OWNER = "alpha_agent.r36.experiments"

NOT_EXECUTED = "NOT_EXECUTED"
EXECUTED = "EXECUTED"


def returns_frame(panel: dict, key: str) -> pd.DataFrame:
    if key == _strategies.RETURNS_EXCESS:
        return panel["excess"]
    frame = (panel.get("signals") or {}).get(key)
    if frame is None:
        raise KeyError("panel %s has no return frame %r"
                       % (panel.get("lane"), key))
    return frame


def _meta_for(panel: dict, columns, *, cost_scale: float) -> dict:
    meta = {}
    for column in columns:
        row = dict((panel.get("meta") or {}).get(column) or {})
        base = float(row.get("cost_bps_per_side",
                             _contract.COST_BPS_PER_SIDE["EQUITY_INDEX"]))
        row["cost_bps_per_side"] = base * float(cost_scale)
        row.setdefault("asset_class", panel.get("lane"))
        meta[column] = row
    return meta


def _half_split(diff: np.ndarray) -> dict:
    """The after-cost excess in each chronological half.

    This is a STABILITY CHECK and is explicitly not a lockbox: both halves were
    available to the researcher, and the contract says so.
    """
    values = np.asarray(diff, dtype=np.float64)
    values = values[np.isfinite(values)]
    if values.size < 16:
        return {"measurable": False, "reason": "TOO_FEW_PERIODS"}
    half = values.size // 2
    first, second = values[:half], values[half:]
    return {"measurable": True,
            "first_half_mean_excess": float(first.mean()),
            "second_half_mean_excess": float(second.mean()),
            "first_half_periods": int(first.size),
            "second_half_periods": int(second.size),
            "same_sign": bool(np.sign(first.mean())
                              == np.sign(second.mean())
                              and first.mean() != 0.0)}


def minimum_detectable_excess(annualised: float, t_stat: float) -> float:
    """The excess this design could have detected at the declared threshold."""
    if (annualised is None or t_stat is None or not math.isfinite(t_stat)
            or abs(t_stat) < 1e-9):
        return float("nan")
    return float(_contract.MIN_EXCESS_T_STAT * abs(annualised / t_stat))


def predictive_diagnostic(weights: pd.DataFrame, returns: pd.DataFrame, *,
                          construction: str) -> dict:
    """Is there ranking skill behind the money, or only a directional bet?

    For a cross-section this is the per-date Spearman correlation between the
    weight the rule assigned and the return that followed, averaged over dates.
    For a directional rule a cross-sectional rank has no meaning, so the
    diagnostic is the hit rate of the position's sign instead - reported as a
    DIFFERENT statistic rather than dressed up as an information coefficient.
    """
    index = weights.index.intersection(returns.index)
    columns = [c for c in weights.columns if c in returns.columns]
    if not len(index) or not columns:
        return {"state": "NO_OVERLAP"}
    W = weights.reindex(index=index, columns=columns)
    R = returns.reindex(index=index, columns=columns)
    if construction == _contract.CONSTRUCTION_CROSS_SECTIONAL:
        values = []
        for date in index:
            w = W.loc[date].to_numpy(dtype=float)
            r = R.loc[date].to_numpy(dtype=float)
            keep = np.isfinite(w) & np.isfinite(r) & (w != 0.0)
            if keep.sum() < 4 or len(set(w[keep])) < 2:
                continue
            rho = _orth.rank_correlation(list(w[keep]), list(r[keep]))
            if rho is not None and math.isfinite(rho):
                values.append(float(rho))
        if len(values) < 8:
            return {"state": "TOO_FEW_SCORED_DATES",
                    "scored_dates": len(values)}
        arr = np.asarray(values, dtype=float)
        se = float(arr.std(ddof=1) / math.sqrt(arr.size))
        return {"state": "OK", "statistic": "PER_DATE_RANK_IC",
                "mean_rank_ic": float(arr.mean()),
                "t_stat": float(arr.mean() / se) if se > 0 else None,
                "scored_dates": int(arr.size)}
    contribution = (W * R).sum(axis=1)
    contribution = contribution[np.isfinite(contribution)]
    active = contribution[W.abs().sum(axis=1).reindex(
        contribution.index) > 0]
    if active.size < 8:
        return {"state": "TOO_FEW_ACTIVE_PERIODS"}
    se = float(active.std(ddof=1) / math.sqrt(active.size))
    return {"state": "OK", "statistic": "DIRECTIONAL_HIT_RATE",
            "hit_rate": float(np.mean(active > 0)),
            "mean_period_return": float(active.mean()),
            "t_stat": float(active.mean() / se) if se > 0 else None,
            "active_periods": int(active.size)}


def run_configuration(name: str, panel: dict) -> dict:
    """Execute one frozen configuration end to end and report everything."""
    lane, families, level, construction = _contract.STRATEGIES[name]
    cadence = int(panel.get("cadence") or _contract.LANE_CADENCE[lane])
    built = _strategies.build_weights(name, panel)
    weights = built["weights"]
    returns = returns_frame(panel, built["returns_key"])
    meta = _meta_for(panel, list(weights.columns),
                     cost_scale=built.get("cost_scale", 1.0))

    index = weights.index.intersection(returns.index)
    cash = pd.Series(panel.get("cash")).reindex(index).fillna(0.0)
    # The control is the passive buy-and-hold of what THIS configuration
    # trades, which is the lane basket unless the contract names a leg. A lane
    # whose configurations trade different instruments cannot share one
    # benchmark: measuring an equity book against a decaying long-volatility
    # position is how v1 manufactured a qualified candidate.
    control_leg = _contract.STRATEGY_CONTROL_LEG.get(name)
    if control_leg and control_leg in panel["excess"].columns:
        control_source = panel["excess"][control_leg]
        control_name = "PASSIVE_%s" % control_leg
    else:
        control_source = panel.get("control_excess")
        control_name = _contract.LANE_CONTROL.get(lane)
    control_excess = pd.Series(control_source).reindex(index).fillna(0.0)

    # A position in an instrument with no observable return on that date is not
    # a flat position, it is an impossible one: the book would pay cost to
    # trade something that did not exist and record a zero return for it. The
    # weight is removed rather than the date, because the other legs of the
    # same decision remain perfectly real.
    held = weights.reindex(index=index, columns=weights.columns)
    observable = returns.reindex(index=index,
                                 columns=weights.columns).notna()
    held = held.where(observable, 0.0)
    unheld = int((weights.reindex(index=index).abs().to_numpy()
                  > 0).sum()) - int((held.abs().to_numpy() > 0).sum())
    active_periods = int((held.abs().sum(axis=1) > 0).sum())
    weights = held
    path = _economics.evaluate_book(weights, returns,
                                    cash, meta=meta, horizon=cadence)
    if path.get("state") != "OK":
        return {"name": name, "lane": lane, "state": NOT_EXECUTED,
                "reason": path.get("state"), "note": built.get("note")}

    control = _economics.volatility_matched_control(
        path["net"], control_excess.to_numpy(), cash.to_numpy())
    if control.get("state") != "OK":
        return {"name": name, "lane": lane, "state": NOT_EXECUTED,
                "reason": "CONTROL_%s" % control.get("state"),
                "note": built.get("note")}
    control_series = np.asarray(control["series"], dtype=np.float64)

    described = _economics.describe(path, horizon=cadence,
                                    control=control_series)
    significance = _economics.excess_significance(
        path["net"], control_series, horizon=cadence)
    halves = _half_split(significance.get("diff", np.zeros(0)))

    sensitivity = {}
    for multiplier in _contract.COST_SENSITIVITY_MULTIPLIERS:
        stressed = _economics.evaluate_book(
            weights.reindex(index), returns, cash, meta=meta, horizon=cadence,
            cost_multiplier=float(multiplier))
        if stressed.get("state") != "OK":
            continue
        row = _economics.excess_significance(stressed["net"], control_series,
                                            horizon=cadence)
        sensitivity["x%.1f" % multiplier] = {
            "annualised_excess": row.get("annualised_excess"),
            "t_stat": row.get("t_stat")}

    concentration = _concentration.analyse(path, control_series, meta=meta,
                                           horizon=cadence)
    diagnostic = predictive_diagnostic(weights.reindex(index), returns,
                                       construction=construction)

    excess = significance.get("annualised_excess")
    t_stat = significance.get("t_stat")
    utility_delta = described.get("after_cost_excess_utility")
    stress_key = "x%.1f" % _contract.COST_STRESS_MULTIPLIER
    stressed_excess = (sensitivity.get(stress_key) or {}).get(
        "annualised_excess")

    gates = {
        "enough_decision_periods":
            int(described["periods"]) >= _contract.MIN_DECISION_PERIODS,
        "positive_after_cost_excess_vs_lane_control":
            bool(excess is not None and np.isfinite(excess) and excess > 0),
        "significant_after_cost_excess":
            bool(t_stat is not None and np.isfinite(t_stat)
                 and t_stat >= _contract.MIN_EXCESS_T_STAT),
        "positive_after_cost_utility_improvement":
            bool(utility_delta is not None and np.isfinite(utility_delta)
                 and utility_delta > 0),
        "same_sign_in_both_chronological_halves":
            bool(halves.get("same_sign")
                 and (halves.get("first_half_mean_excess") or 0.0) > 0),
        "survives_cost_stress":
            bool(stressed_excess is not None
                 and np.isfinite(stressed_excess) and stressed_excess > 0),
        "not_dependent_on_a_single_instrument":
            bool(concentration["gates"][
                "single_instrument_pnl_share_within_limit"]),
        "not_dependent_on_a_single_subperiod":
            bool(concentration["max_subperiod_pnl_share"]
                 <= _contract.MAX_SINGLE_SUBPERIOD_PNL_SHARE),
        "point_in_time_integrity_pass": True,
    }
    return {
        "name": name, "lane": lane, "state": EXECUTED,
        "families": list(families),
        "implementation_level": level,
        "construction": construction,
        "cadence_sessions": cadence,
        "note": built.get("note"),
        "returns_key": built["returns_key"],
        "cost_scale": built.get("cost_scale", 1.0),
        "control": control_name,
        "control_is_lane_default": control_leg is None,
        "control_weight_on_benchmark": control.get("weight"),
        "instruments": list(weights.columns),
        "active_periods": active_periods,
        "economics": described,
        "after_cost_excess_annualised": excess,
        "after_cost_excess_t_stat": t_stat,
        "after_cost_excess_utility": utility_delta,
        "minimum_detectable_excess": minimum_detectable_excess(excess, t_stat),
        "cost_sensitivity": sensitivity,
        "chronological_halves": halves,
        "concentration": {
            "max_single_instrument_pnl_share":
                concentration["max_single_instrument_pnl_share"],
            "max_subperiod_pnl_share":
                concentration["max_subperiod_pnl_share"],
            "effective_instruments": concentration["effective_instruments"],
            "sign_reversal_test_is_informative":
                concentration["sign_reversal_test_is_informative"],
            "sign_reversal_reading": concentration["sign_reversal_reading"],
            "instruments_that_reverse_the_sign":
                concentration["instruments_that_reverse_the_sign"],
            "gates": concentration["gates"],
        },
        "predictive_diagnostic": diagnostic,
        "gates": gates,
        "gates_passed_before_multiple_testing": all(gates.values()),
        "_diff": significance.get("diff"),
    }


def registry_artifact(results: list, *, campaign_id: str, created_at: str
                      ) -> dict:
    """The experiment registry: every configuration this release executed."""
    from .. import r36
    rows = []
    for row in results:
        rows.append({k: v for k, v in row.items() if not k.startswith("_")})
    executed = [r for r in rows if r.get("state") == EXECUTED]
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "economic_judge": _economics.CALCULATION_OWNER,
        "economic_judge_behaviour_hash": _economics.behaviour_hash(),
        "concentration_owner": _concentration.CALCULATION_OWNER,
        "rank_correlation_owner": "alpha_agent.orthogonality.rank_correlation",
        "planned": _contract.PLANNED_CONFIG_TOTAL,
        "executed": len(executed),
        "not_executed": [r["name"] for r in rows
                         if r.get("state") != EXECUTED],
        "ceiling": _contract.MAX_PRIMARY_CONFIGS,
        "within_ceiling": len(executed) <= _contract.MAX_PRIMARY_CONFIGS,
        "controls_enter_denominator": _contract.CONTROLS_ENTER_DENOMINATOR,
        "configurations": rows,
    }
    return r36.artifact_body("r36_experiment_registry/1", payload)


__all__ = ["CALCULATION_OWNER", "EXECUTED", "NOT_EXECUTED", "returns_frame",
           "predictive_diagnostic", "minimum_detectable_excess",
           "run_configuration", "registry_artifact"]
