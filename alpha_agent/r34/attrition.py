"""alpha_agent.r34.attrition - the PREDICTION_TO_PNL_ATTRITION_WATERFALL owner.

This artifact is REQUIRED whether or not alpha qualifies, and it is the most
useful thing this release can leave behind if it does not. Release 33 ended with
a true statement - measurable prediction, no economics - and no account of WHERE
between the two the value went. "Prediction did not convert" is a fact; it is
not yet knowledge.

The waterfall converts the same book, one stage at a time, and records the
annualised value surviving each conversion. Each stage answers exactly one
question, and the DROP between two stages is the cost of that one conversion
step:

1. ``RAW_FORECAST_SKILL``      - is there ordering ability at all?
2. ``PERFECT_FORESIGHT_SIZED`` - what would this sizing rule earn if the
   forecast were replaced by the realised return? The ceiling the conversion
   layer is working against, and the thing that separates "the forecast is too
   weak" from "the machinery loses money even with a perfect forecast";
3. ``CALIBRATED_EXPECTED_RETURN`` - what the calibrated forecast earns before
   any portfolio constraint;
4. ``AFTER_SIZING``            - after the sizing rule shapes conviction;
5. ``AFTER_CONSTRAINTS``       - after caps and the gross-exposure limit;
6. ``AFTER_TURNOVER_CONTROL``  - after the transition rule;
7. ``AFTER_COST``              - after traded-notional cost at the base tier;
8. ``AFTER_RISK_MATCHED_CONTROL`` - after subtracting what a passive book of
   the same realised risk would have earned anyway;
9. ``AFTER_UTILITY_CHARGE``    - after charging for the variance carried.

Stage 2 is the one that makes the waterfall diagnostic rather than descriptive.
Without it a zero at the end is ambiguous between a weak forecast and a lossy
conversion layer, and those two findings point at completely different next
releases.
"""
from __future__ import annotations

import numpy as np

from . import economics as _economics

CALCULATION_OWNER = "alpha_agent.r34.attrition"

STAGES = (
    "RAW_FORECAST_SKILL",
    "PERFECT_FORESIGHT_SIZED",
    "CALIBRATED_EXPECTED_RETURN",
    "AFTER_SIZING",
    "AFTER_CONSTRAINTS",
    "AFTER_TURNOVER_CONTROL",
    "AFTER_COST",
    "AFTER_RISK_MATCHED_CONTROL",
    "AFTER_UTILITY_CHARGE",
)

#: The specific failure modes the release is required to quantify separately.
FAILURE_MODES = (
    "forecast_too_weak",
    "magnitude_poorly_calibrated",
    "sizing_destroys_rank_skill",
    "turnover_consumes_edge",
    "diversification_dilutes_edge",
    "risk_matched_benchmark_dominates",
    "exposure_neutrality_removes_apparent_alpha",
    "works_only_in_one_asset_class",
    "works_only_in_one_horizon",
    "works_only_under_unrealistic_cost",
    "covariance_or_risk_forecast_error",
)


def build(*, horizon: int, rank_ic: dict, stage_paths: dict,
          control: np.ndarray, cost_scenarios: dict,
          per_asset_class_excess: dict, per_horizon_excess: dict,
          concentration: dict) -> dict:
    """Assemble the waterfall from already-computed stage return paths.

    ``stage_paths`` maps a stage name to an annualised value already computed by
    the campaign; this module arranges them, computes the drops, and turns the
    result into the failure-mode attribution the contract requires.
    """
    rows, prev = [], None
    for stage in STAGES:
        value = stage_paths.get(stage)
        drop = (None if (value is None or prev is None)
                else float(prev - value))
        rows.append({"stage": stage, "annualised_value": value,
                     "drop_from_previous": drop,
                     "cumulative_from_first": (
                         None if (value is None
                                  or stage_paths.get(STAGES[1]) is None)
                         else float(stage_paths[STAGES[1]] - value))})
        if value is not None:
            prev = value

    def _v(stage):
        return stage_paths.get(stage)

    def _drop(a, b):
        va, vb = _v(a), _v(b)
        return None if (va is None or vb is None) else float(va - vb)

    ic = rank_ic.get("value")
    ic_t = rank_ic.get("t_stat")
    ceiling = _v("PERFECT_FORESIGHT_SIZED")
    realised = _v("AFTER_COST")

    modes = {
        "forecast_too_weak": {
            "measured": (ic is not None and ic_t is not None
                         and abs(float(ic_t)) < 2.0),
            "evidence": {"rank_ic": ic, "rank_ic_t_stat": ic_t,
                         "share_of_perfect_foresight_captured": (
                             None if (ceiling in (None, 0) or realised is None)
                             else float(realised / ceiling))}},
        "magnitude_poorly_calibrated": {
            "measured": None, "evidence": {
                "note": "see calibration_results.json: calibration slope and "
                        "the calibrated-minus-rank-only utility delta"}},
        "sizing_destroys_rank_skill": {
            "measured": (_drop("CALIBRATED_EXPECTED_RETURN", "AFTER_SIZING")
                         or 0.0) > 0,
            "evidence": {"drop": _drop("CALIBRATED_EXPECTED_RETURN",
                                       "AFTER_SIZING")}},
        "turnover_consumes_edge": {
            "measured": (_drop("AFTER_TURNOVER_CONTROL", "AFTER_COST")
                         or 0.0) > 0,
            "evidence": {"cost_drop": _drop("AFTER_TURNOVER_CONTROL",
                                            "AFTER_COST"),
                         "cost_scenarios": cost_scenarios}},
        "diversification_dilutes_edge": {
            "measured": (_drop("AFTER_SIZING", "AFTER_CONSTRAINTS")
                         or 0.0) > 0,
            "evidence": {"drop": _drop("AFTER_SIZING", "AFTER_CONSTRAINTS"),
                         "effective_instruments":
                             concentration.get("effective_instruments")}},
        "risk_matched_benchmark_dominates": {
            "measured": (_drop("AFTER_COST", "AFTER_RISK_MATCHED_CONTROL")
                         or 0.0) > 0,
            "evidence": {"drop": _drop("AFTER_COST",
                                       "AFTER_RISK_MATCHED_CONTROL"),
                         "control_return_annualised": (
                             _economics.annualised_return(control,
                                                          horizon=horizon))}},
        "exposure_neutrality_removes_apparent_alpha": {
            "measured": (_drop("AFTER_RISK_MATCHED_CONTROL",
                               "AFTER_UTILITY_CHARGE") or 0.0) > 0,
            "evidence": {"drop": _drop("AFTER_RISK_MATCHED_CONTROL",
                                       "AFTER_UTILITY_CHARGE")}},
        "works_only_in_one_asset_class": {
            "measured": bool(concentration.get(
                "asset_classes_that_reverse_the_sign")),
            "evidence": {"per_asset_class_excess": per_asset_class_excess,
                         "reversing_classes": concentration.get(
                             "asset_classes_that_reverse_the_sign"),
                         "max_class_pnl_share": concentration.get(
                             "max_single_asset_class_pnl_share")}},
        "works_only_in_one_horizon": {
            "measured": None,
            "evidence": {"per_horizon_excess": per_horizon_excess}},
        "works_only_under_unrealistic_cost": {
            "measured": _only_under_optimistic_cost(cost_scenarios),
            "evidence": {"cost_scenarios": cost_scenarios}},
        "covariance_or_risk_forecast_error": {
            "measured": None,
            "evidence": {"note": "see position_sizing_results.json: the "
                                 "variance-based sizing rules against the "
                                 "rank-weighted one"}},
    }

    return {
        "calculation_owner": CALCULATION_OWNER,
        "horizon": int(horizon),
        "stages": rows,
        "perfect_foresight_ceiling": ceiling,
        "realised_after_cost": realised,
        "share_of_ceiling_captured": (
            None if (ceiling in (None, 0) or realised is None)
            else float(realised / ceiling)),
        "failure_modes": modes,
        "declared_failure_modes": list(FAILURE_MODES),
    }


def _only_under_optimistic_cost(cost_scenarios: dict):
    """True when the result is positive only at the most optimistic cost."""
    try:
        opt = cost_scenarios.get("OPTIMISTIC")
        base = cost_scenarios.get("BASE")
        stressed = cost_scenarios.get("STRESSED")
    except AttributeError:
        return None
    if opt is None or base is None:
        return None
    if stressed is None:
        return bool(opt > 0 >= base)
    return bool(opt > 0 and (base <= 0 or stressed <= 0))
