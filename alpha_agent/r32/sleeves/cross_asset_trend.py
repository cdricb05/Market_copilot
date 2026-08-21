"""CROSS_ASSET_TREND - should capital sit outside equities at all?

This is the sleeve that makes Release 32 genuinely asset-agnostic. Its
instrument set is four owned legs: US equity total return, US investment-grade
bond total return, the Bloomberg commodity total return index, and the dollar
index. Cash is the fifth choice and it is always available.

The legs are honest about what they are. Three are total-return indices, so
their level already includes income. The dollar index is a PRICE index - holding
it is not a funded position with a carry - and it is declared as such rather
than quietly treated as a return stream.

The common overlap is set by the bond leg, which delivers data from 1994-12-30
even though its metadata advertises 1990. Every comparison in this sleeve is
made on the window all four legs actually cover.
"""
from __future__ import annotations

import numpy as np

from .. import contract as _contract
from .. import panels as _panels
from ..sleeve import (
    DIRECTION_FLAT,
    DIRECTION_ROTATE,
    SleeveSpec,
    StrategyOpportunity,
    normalise_exposure,
    top_k_long_only,
)
from . import _common as C

SLEEVE = _contract.SLEEVE_CROSS_ASSET_TREND
PANEL = _panels.PANEL_CROSS_ASSET

LEGS = tuple(l["leg"] for l in _panels.CROSS_ASSET_LEGS)

#: The dollar leg is a price index, not a funded total return. It may express a
#: trend opinion but its economics are not directly comparable to a bond or
#: equity total return, so this sleeve reports it separately rather than
#: pretending an index level is a portfolio holding.
PRICE_INDEX_LEGS = tuple(l["leg"] for l in _panels.CROSS_ASSET_LEGS
                         if l["kind"] == "PRICE_INDEX")
TOTAL_RETURN_LEGS = tuple(l["leg"] for l in _panels.CROSS_ASSET_LEGS
                          if l["kind"] == "TOTAL_RETURN")


def _opportunity(date: str, weights: dict, rationale: str,
                 state: dict) -> StrategyOpportunity:
    return StrategyOpportunity(
        sleeve=SLEEVE, decision_date=date,
        direction=DIRECTION_ROTATE if weights else DIRECTION_FLAT,
        conviction=float(sum(weights.values())),
        recommended_exposure=weights, rationale=rationale,
        state_variables=state)


def _investable(params: dict) -> tuple:
    return (TOTAL_RETURN_LEGS if params.get("total_return_only", True)
            else LEGS)


# --------------------------------------------------------------------------- #
# Families
# --------------------------------------------------------------------------- #
def gen_tsmom(panel: dict, idx: list, params: dict) -> list:
    """Time-series momentum: own each leg only while its own trend is positive.

    Cash absorbs whatever is not owned, which is the honest expression of "no
    asset currently qualifies" - the answer this campaign is required to allow.
    """
    dates = panel["dates"]
    lookback = int(params["lookback"])
    legs = _investable(params)
    out = []
    for i in idx:
        chosen = {}
        state = {}
        for leg in legs:
            col = panel["columns"].get(leg)
            if col is None:
                continue
            m = C.momentum(col, i, lookback)
            state[leg] = None if not np.isfinite(m) else float(m)
            if np.isfinite(m) and m > 0.0:
                chosen[leg] = 1.0
        if not state:
            continue
        if chosen:
            w = {k: 1.0 / len(chosen) for k in chosen}
        else:
            w = {}
        out.append(_opportunity(dates[i], w,
                                f"{len(chosen)} of {len(state)} legs trending "
                                f"positive over {lookback} sessions", state))
    return out


def gen_xsmom(panel: dict, idx: list, params: dict) -> list:
    """Cross-sectional momentum: own the strongest ``k`` legs."""
    dates = panel["dates"]
    lookback = int(params["lookback"])
    k = int(params["k"])
    legs = _investable(params)
    out = []
    for i in idx:
        scores = {}
        for leg in legs:
            col = panel["columns"].get(leg)
            if col is None:
                continue
            m = C.momentum(col, i, lookback)
            if np.isfinite(m):
                scores[leg] = m
        if len(scores) < k:
            continue
        out.append(_opportunity(dates[i], top_k_long_only(scores, k=k),
                                f"strongest {k} of {len(scores)} legs over "
                                f"{lookback} sessions", scores))
    return out


def gen_vol_scaled_tsmom(panel: dict, idx: list, params: dict) -> list:
    """Trend, sized by inverse volatility rather than equally.

    Equal weights across assets with very different volatilities is a bet
    dominated by the noisiest leg. Inverse-volatility sizing is the standard
    correction, and it is tested rather than assumed to help.
    """
    dates = panel["dates"]
    lookback = int(params["lookback"])
    window = int(params["vol_window"])
    legs = _investable(params)
    out = []
    for i in idx:
        vols, state = {}, {}
        for leg in legs:
            col = panel["columns"].get(leg)
            if col is None:
                continue
            m = C.momentum(col, i, lookback)
            v = C.realised_vol(col, i, window)
            state[leg] = None if not np.isfinite(m) else float(m)
            if np.isfinite(m) and m > 0.0 and np.isfinite(v) and v > 0.0:
                vols[leg] = v
        if not state:
            continue
        w = C.inverse_vol_weights(vols) if vols else {}
        out.append(_opportunity(dates[i], normalise_exposure(w),
                                f"{len(vols)} trending legs, inverse-volatility "
                                f"sized", state))
    return out


def gen_risk_parity(panel: dict, idx: list, params: dict) -> list:
    """Unconditional inverse-volatility across every leg - the null rotation.

    This family exists as an honest control. If a trend rule cannot beat simply
    spreading capital by risk, the trend is not the source of any value found.
    """
    dates = panel["dates"]
    window = int(params["vol_window"])
    legs = _investable(params)
    out = []
    for i in idx:
        vols = {}
        for leg in legs:
            col = panel["columns"].get(leg)
            if col is None:
                continue
            v = C.realised_vol(col, i, window)
            if np.isfinite(v) and v > 0.0:
                vols[leg] = v
        if len(vols) < 2:
            continue
        out.append(_opportunity(dates[i], C.inverse_vol_weights(vols),
                                f"inverse {window}-session volatility across "
                                f"{len(vols)} legs", {}))
    return out


FAMILIES = {
    "tsmom": gen_tsmom,
    "xsmom": gen_xsmom,
    "vol_scaled_tsmom": gen_vol_scaled_tsmom,
    "risk_parity": gen_risk_parity,
}


def screening_specs() -> list:
    return [
        SleeveSpec(sleeve=SLEEVE, family="tsmom", params={"lookback": 252},
                   generate=gen_tsmom),
        SleeveSpec(sleeve=SLEEVE, family="tsmom", params={"lookback": 126},
                   generate=gen_tsmom),
        SleeveSpec(sleeve=SLEEVE, family="xsmom",
                   params={"lookback": 252, "k": 2}, generate=gen_xsmom),
        SleeveSpec(sleeve=SLEEVE, family="xsmom",
                   params={"lookback": 126, "k": 1}, generate=gen_xsmom),
        SleeveSpec(sleeve=SLEEVE, family="vol_scaled_tsmom",
                   params={"lookback": 252, "vol_window": 63},
                   generate=gen_vol_scaled_tsmom),
        SleeveSpec(sleeve=SLEEVE, family="vol_scaled_tsmom",
                   params={"lookback": 126, "vol_window": 63},
                   generate=gen_vol_scaled_tsmom),
        SleeveSpec(sleeve=SLEEVE, family="risk_parity",
                   params={"vol_window": 63}, generate=gen_risk_parity),
        SleeveSpec(sleeve=SLEEVE, family="risk_parity",
                   params={"vol_window": 126}, generate=gen_risk_parity),
    ]


def qualification_specs(families: list) -> list:
    grids = {
        "tsmom": [{"lookback": lb} for lb in (21, 63, 126, 189, 252, 378)],
        "xsmom": [{"lookback": lb, "k": k}
                  for lb in (63, 126, 252) for k in (1, 2)],
        "vol_scaled_tsmom": [{"lookback": lb, "vol_window": w}
                             for lb in (126, 252) for w in (21, 63, 126)],
        "risk_parity": [{"vol_window": w} for w in (21, 63, 126, 252)],
    }
    out = []
    for fam in families:
        for p in grids.get(fam, [])[:_contract.QUALIFICATION_MAX_CONFIGS_PER_FAMILY]:
            out.append(SleeveSpec(sleeve=SLEEVE, family=fam, params=p,
                                  generate=FAMILIES[fam],
                                  stage=_contract.STAGE_QUALIFICATION))
    return out


def instrument_returns(panel: dict, idx: list) -> dict:
    dates = panel["dates"]
    hold = _contract.HOLD_SESSIONS
    out = {}
    for i in idx:
        row = {}
        for leg in LEGS:
            col = panel["columns"].get(leg)
            if col is None:
                continue
            row[leg] = _panels.hold_return(col, i, hold)
        out[dates[i]] = row
    return out
