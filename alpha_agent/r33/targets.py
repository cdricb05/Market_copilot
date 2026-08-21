"""alpha_agent.r33.targets - the ONE forecast-target owner.

Four targets, each with a PRIMARY METRIC that was declared in the campaign
contract before any validation number existed. Not every model family predicts
every target: forcing a volatility model to emit a return forecast, or a
cross-sectional ranker to emit a calibrated probability, produces a number that
scores badly for reasons that have nothing to do with predictive content.

    EXCESS_RETURN               the h-session USD excess return over cash
    POSITIVE_RETURN_PROBABILITY the probability that return is positive
    REALISED_VOLATILITY         realised volatility over the same window
    CROSS_SECTIONAL_RANK        the excess return demeaned across the panel

The cross-sectional target is the one that is robust to this panel's known
return-definition heterogeneity: equity indices exclude dividends while bond
indices include coupon, so a constant per-market drift difference exists and a
cross-sectionally demeaned target removes most of it.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as _contract
from . import panel as _panel

CALCULATION_OWNER = "alpha_agent.r33.targets"

T_RETURN = _contract.TARGET_RETURN
T_SIGN = _contract.TARGET_SIGN
T_VOL = _contract.TARGET_VOLATILITY
T_XS = _contract.TARGET_CROSS_SECTION


def build(panel: dict, *, horizon: int) -> dict:
    """All four target frames on the non-overlapping forecast schedule."""
    excess = _panel.observation_returns(panel, horizon=horizon)
    vol = _panel.realised_volatility(panel, horizon=horizon)
    xs = excess.sub(excess.mean(axis=1), axis=0)
    sign = (excess > 0.0).astype(float).where(excess.notna())
    return {T_RETURN: excess, T_SIGN: sign, T_VOL: vol, T_XS: xs}


def align_to_rows(target_frame: pd.DataFrame, design: dict) -> np.ndarray:
    """Pull the target for each design-matrix row, as a flat array."""
    values = target_frame.to_numpy()
    col = {s: j for j, s in enumerate(target_frame.columns)}
    row = {d: i for i, d in enumerate(target_frame.index)}
    out = np.full(len(design["symbol"]), np.nan)
    for k in range(len(out)):
        i = row.get(design["date"][k])
        j = col.get(design["symbol"][k])
        if i is not None and j is not None:
            out[k] = values[i, j]
    return out


def target_is_supported(target: str, family_supports: tuple) -> bool:
    return target in family_supports


def declaration() -> dict:
    return {
        "calculation_owner": CALCULATION_OWNER,
        "targets": list(_contract.TARGETS),
        "primary_metric": dict(_contract.PRIMARY_METRIC),
        "forecast_baseline": dict(_contract.FORECAST_BASELINE),
        "declared_before_validation": True,
        "metric_shopping_allowed": False,
        "cross_sectional_target_rationale": (
            "equity indices exclude dividends while bond indices include "
            "coupon, so a cross-sectionally demeaned target removes most of "
            "the constant per-market drift difference this panel carries"),
    }
