"""VOLATILITY_RISK_REGIME - does the volatility surface say when to own risk?

The instruments here are not volatility products. This project cannot buy VIX
futures, variance swaps or options, and inventing a tradable volatility return
series out of an index level would be fabricating an instrument. So the sleeve
uses the volatility surface strictly as OBSERVABLE STATE and expresses its
opinion in the one thing it can actually hold: equity beta, or cash.

That restraint is the point. ``$VIX`` is a number, not a position. A sleeve that
"owns volatility" because it can see a volatility index has confused a
measurement with a market, and its backtest would describe a portfolio nobody
could have held.

The term structure ``VIX / VIX3M`` is the sharpest observable here, and it
begins in 2002 - which caps this sleeve's history well short of the others and
is reported as such rather than papered over.
"""
from __future__ import annotations

import numpy as np

from .. import contract as _contract
from .. import panels as _panels
from ..sleeve import (
    DIRECTION_FLAT,
    DIRECTION_LONG,
    DIRECTION_REDUCE,
    SleeveSpec,
    StrategyOpportunity,
)
from . import _common as C

SLEEVE = _contract.SLEEVE_VOLATILITY_RISK_REGIME
PANEL = _panels.PANEL_VOLATILITY
INSTRUMENT = "EQUITY_US"

#: This sleeve may hold exactly one thing, plus cash. Declared so that no
#: configuration can quietly acquire a volatility instrument it cannot buy.
INVESTABLE_INSTRUMENTS = (INSTRUMENT,)
TRADABLE_VOLATILITY_PRODUCTS_OWNED = False


def _direction(x: float) -> str:
    if x <= 0.01:
        return DIRECTION_FLAT
    return DIRECTION_LONG if x >= 0.99 else DIRECTION_REDUCE


def _opportunity(date: str, exposure: float, rationale: str,
                 state: dict) -> StrategyOpportunity:
    exposure = C.clamp(exposure, 0.0, 1.0)
    return StrategyOpportunity(
        sleeve=SLEEVE, decision_date=date, direction=_direction(exposure),
        conviction=abs(exposure - 0.5) * 2.0,
        recommended_exposure=({INSTRUMENT: exposure} if exposure > 0 else {}),
        rationale=rationale, state_variables=state)


# --------------------------------------------------------------------------- #
# Families
# --------------------------------------------------------------------------- #
def gen_term_structure(panel: dict, idx: list, params: dict) -> list:
    """Own equity beta while the VIX curve is in contango.

    Backwardation - near-term implied volatility above three-month - is the
    market pricing stress now rather than later. Whether that is worth acting on
    is precisely what is being tested.
    """
    dates = panel["dates"]
    near = panel["columns"].get("VIX")
    far = panel["columns"].get("VIX3M")
    if near is None or far is None:
        return []
    cut = float(params["ratio_cut"])
    out = []
    for i in idx:
        a, b = float(near[i]), float(far[i])
        if not np.isfinite(a) or not np.isfinite(b) or b <= 0.0:
            continue
        ratio = a / b
        exposure = 1.0 if ratio < cut else 0.0
        out.append(_opportunity(dates[i], exposure,
                                f"VIX/VIX3M {ratio:.3f} vs cut {cut:.2f}",
                                {"vix": a, "vix3m": b, "ratio": ratio}))
    return out


def gen_level_percentile(panel: dict, idx: list, params: dict) -> list:
    """Reduce beta when an implied-volatility measure is in its own tail."""
    dates = panel["dates"]
    key = str(params["measure"])
    col = panel["columns"].get(key)
    if col is None:
        return []
    window = int(params["rank_window"])
    cut = float(params["cut"])
    out = []
    for i in idx:
        pr = C.percentile_rank(col, i, window)
        if not np.isfinite(pr):
            continue
        out.append(_opportunity(dates[i], 1.0 if pr <= cut else 0.0,
                                f"{key} trailing percentile {pr:.2f} vs "
                                f"{cut:.2f}", {key: float(col[i]),
                                               "percentile": pr}))
    return out


def gen_composite_stress(panel: dict, idx: list, params: dict) -> list:
    """Combine several surface measures into one standardised stress score.

    Each measure is z-scored against its OWN trailing window before being
    averaged, so a measure with a larger natural scale does not dominate, and no
    weight is fitted on the outcome.
    """
    dates = panel["dates"]
    keys = [k for k in str(params["measures"]).split("+")]
    window = int(params["rank_window"])
    cut = float(params["z_cut"])
    out = []
    for i in idx:
        zs, state = [], {}
        for k in keys:
            col = panel["columns"].get(k)
            if col is None:
                continue
            z = C.zscore(col, i, window)
            if np.isfinite(z):
                zs.append(z)
                state[k] = float(col[i])
        if not zs:
            continue
        stress = float(np.mean(zs))
        state["stress_z"] = stress
        out.append(_opportunity(dates[i], 1.0 if stress <= cut else 0.0,
                                f"composite stress z {stress:.2f} vs {cut:.2f} "
                                f"over {len(zs)} measures", state))
    return out


def gen_vol_of_vol(panel: dict, idx: list, params: dict) -> list:
    """Scale beta down as the volatility of volatility rises."""
    dates = panel["dates"]
    col = panel["columns"].get("VVIX")
    if col is None:
        return []
    window = int(params["rank_window"])
    out = []
    for i in idx:
        pr = C.percentile_rank(col, i, window)
        if not np.isfinite(pr):
            continue
        out.append(_opportunity(dates[i], C.clamp(1.0 - pr, 0.0, 1.0),
                                f"VVIX percentile {pr:.2f}; exposure scaled "
                                f"inversely", {"vvix": float(col[i]),
                                               "percentile": pr}))
    return out


FAMILIES = {
    "term_structure": gen_term_structure,
    "level_percentile": gen_level_percentile,
    "composite_stress": gen_composite_stress,
    "vol_of_vol": gen_vol_of_vol,
}


def screening_specs() -> list:
    return [
        SleeveSpec(sleeve=SLEEVE, family="term_structure",
                   params={"ratio_cut": 1.00}, generate=gen_term_structure),
        SleeveSpec(sleeve=SLEEVE, family="term_structure",
                   params={"ratio_cut": 0.95}, generate=gen_term_structure),
        SleeveSpec(sleeve=SLEEVE, family="level_percentile",
                   params={"measure": "VIX", "rank_window": 756, "cut": 0.80},
                   generate=gen_level_percentile),
        SleeveSpec(sleeve=SLEEVE, family="level_percentile",
                   params={"measure": "MOVE", "rank_window": 756, "cut": 0.80},
                   generate=gen_level_percentile),
        SleeveSpec(sleeve=SLEEVE, family="composite_stress",
                   params={"measures": "VIX+SKEW", "rank_window": 756,
                           "z_cut": 1.0}, generate=gen_composite_stress),
        SleeveSpec(sleeve=SLEEVE, family="composite_stress",
                   params={"measures": "VIX+MOVE+VVIX", "rank_window": 756,
                           "z_cut": 1.0}, generate=gen_composite_stress),
        SleeveSpec(sleeve=SLEEVE, family="vol_of_vol",
                   params={"rank_window": 756}, generate=gen_vol_of_vol),
        SleeveSpec(sleeve=SLEEVE, family="vol_of_vol",
                   params={"rank_window": 252}, generate=gen_vol_of_vol),
    ]


def qualification_specs(families: list) -> list:
    grids = {
        "term_structure": [{"ratio_cut": c}
                           for c in (0.90, 0.95, 1.00, 1.05, 1.10)],
        "level_percentile": [{"measure": m, "rank_window": 756, "cut": c}
                             for m in ("VIX", "MOVE", "SKEW")
                             for c in (0.70, 0.85)][:8],
        "composite_stress": [{"measures": m, "rank_window": w, "z_cut": z}
                             for m in ("VIX+SKEW", "VIX+MOVE+VVIX")
                             for w in (504, 756) for z in (0.5, 1.0)][:8],
        "vol_of_vol": [{"rank_window": w} for w in (252, 504, 756, 1260)],
    }
    out = []
    for fam in families:
        for p in grids.get(fam, [])[:_contract.QUALIFICATION_MAX_CONFIGS_PER_FAMILY]:
            out.append(SleeveSpec(sleeve=SLEEVE, family=fam, params=p,
                                  generate=FAMILIES[fam],
                                  stage=_contract.STAGE_QUALIFICATION))
    return out


def instrument_returns(panel: dict, idx: list) -> dict:
    lv = panel["columns"]["BENCHMARK"]
    dates = panel["dates"]
    hold = _contract.HOLD_SESSIONS
    return {dates[i]: {INSTRUMENT: _panels.hold_return(lv, i, hold)}
            for i in idx}
