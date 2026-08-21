"""EQUITY_BETA_TIMING - how much equity beta should be owned at all?

Release 31 asked which equities to own and found the constraint was information,
not method. This sleeve asks a different question with the same owned data: given
observable market state, should the portfolio carry full equity beta, less, or
none? The instrument is the investable index total return; the alternative is
cash, which earns the observed bill yield.

Every state variable used here is a market observable that changes on most
trading days - index level, VIX, bill and note yields, corporate yields, market
breadth, put/call. None of them is a statistical release: those are stamped at
the start of the period they measure and are unusable as history.
"""
from __future__ import annotations

import numpy as np

from .. import panels as _panels
from .. import contract as _contract
from ..sleeve import (
    DIRECTION_FLAT,
    DIRECTION_LONG,
    DIRECTION_REDUCE,
    SleeveSpec,
    StrategyOpportunity,
)
from . import _common as C

SLEEVE = _contract.SLEEVE_EQUITY_BETA_TIMING
PANEL = _panels.PANEL_BETA_TIMING
INSTRUMENT = "EQUITY_US"

#: A target volatility for the vol-targeting family. Declared, not fitted: a
#: target chosen to maximise the backtest is a parameter selected on the answer.
TARGET_VOL = 0.12


def _direction(exposure: float) -> str:
    if exposure <= 0.01:
        return DIRECTION_FLAT
    if exposure >= 0.99:
        return DIRECTION_LONG
    return DIRECTION_REDUCE


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
def gen_trend(panel: dict, idx: list, params: dict) -> list:
    """Own equity beta while the index is above its own moving average."""
    lv = panel["columns"]["BENCHMARK"]
    dates = panel["dates"]
    window = int(params["ma"])
    out = []
    for i in idx:
        ma = C.moving_average(lv, i, window)
        px = float(lv[i])
        if not np.isfinite(ma) or not np.isfinite(px):
            continue
        exposure = 1.0 if px > ma else 0.0
        out.append(_opportunity(dates[i], exposure,
                                f"index {'above' if exposure else 'below'} its "
                                f"{window}-session average",
                                {"ma": ma, "price": px}))
    return out


def gen_vol_target(panel: dict, idx: list, params: dict) -> list:
    """Scale equity beta so realised risk sits near a declared target."""
    lv = panel["columns"]["BENCHMARK"]
    dates = panel["dates"]
    window = int(params["vol_window"])
    cap = float(params["max_exposure"])
    out = []
    for i in idx:
        vol = C.realised_vol(lv, i, window)
        if not np.isfinite(vol) or vol <= 0.0:
            continue
        exposure = C.clamp(TARGET_VOL / vol, 0.0, cap)
        out.append(_opportunity(dates[i], exposure,
                                f"realised vol {vol:.3f} vs target "
                                f"{TARGET_VOL:.3f}", {"realised_vol": vol}))
    return out


def gen_state_screen(panel: dict, idx: list, params: dict) -> list:
    """Reduce beta when the observable risk state is in its own tail.

    The threshold is a trailing PERCENTILE, not a level. A VIX of 20 meant
    something different in 1995 and in 2020, and a fixed level chosen today is a
    parameter that knows the whole sample.
    """
    dates = panel["dates"]
    cols = panel["columns"]
    key = str(params["state"])
    window = int(params["rank_window"])
    cut = float(params["cut"])
    series = cols.get(key)
    if series is None:
        return []
    out = []
    for i in idx:
        pr = C.percentile_rank(series, i, window)
        if not np.isfinite(pr):
            continue
        exposure = 1.0 if pr <= cut else 0.0
        out.append(_opportunity(dates[i], exposure,
                                f"{key} trailing percentile {pr:.2f} vs cut "
                                f"{cut:.2f}", {key: float(series[i]),
                                               "percentile": pr}))
    return out


def gen_term_spread(panel: dict, idx: list, params: dict) -> list:
    """Own equity beta while the yield curve is not inverted."""
    dates = panel["dates"]
    cols = panel["columns"]
    long_leg = cols.get("YIELD_10Y")
    short_leg = cols.get(str(params["short_leg"]))
    if long_leg is None or short_leg is None:
        return []
    floor = float(params["floor"])
    out = []
    for i in idx:
        a, b = float(long_leg[i]), float(short_leg[i])
        if not np.isfinite(a) or not np.isfinite(b):
            continue
        spread = a - b
        exposure = 1.0 if spread > floor else 0.0
        out.append(_opportunity(dates[i], exposure,
                                f"term spread {spread:.2f} vs floor {floor:.2f}",
                                {"term_spread": spread}))
    return out


FAMILIES = {
    "trend": gen_trend,
    "vol_target": gen_vol_target,
    "state_screen": gen_state_screen,
    "term_spread": gen_term_spread,
}


def screening_specs() -> list:
    """At most eight cheap, pre-declared hypotheses."""
    return [
        SleeveSpec(sleeve=SLEEVE, family="trend", params={"ma": 200},
                   generate=gen_trend),
        SleeveSpec(sleeve=SLEEVE, family="trend", params={"ma": 100},
                   generate=gen_trend),
        SleeveSpec(sleeve=SLEEVE, family="vol_target",
                   params={"vol_window": 63, "max_exposure": 1.0},
                   generate=gen_vol_target),
        SleeveSpec(sleeve=SLEEVE, family="vol_target",
                   params={"vol_window": 21, "max_exposure": 1.0},
                   generate=gen_vol_target),
        SleeveSpec(sleeve=SLEEVE, family="state_screen",
                   params={"state": "VIX", "rank_window": 756, "cut": 0.80},
                   generate=gen_state_screen),
        SleeveSpec(sleeve=SLEEVE, family="state_screen",
                   params={"state": "BREADTH_SPX_MA200", "rank_window": 756,
                           "cut": 0.95},
                   generate=gen_state_screen),
        SleeveSpec(sleeve=SLEEVE, family="term_spread",
                   params={"short_leg": "YIELD_3M", "floor": 0.0},
                   generate=gen_term_spread),
        SleeveSpec(sleeve=SLEEVE, family="term_spread",
                   params={"short_leg": "YIELD_2Y", "floor": 0.0},
                   generate=gen_term_spread),
    ]


def qualification_specs(families: list) -> list:
    """Bounded grids for the families that survived screening."""
    grids = {
        "trend": [{"ma": m} for m in (50, 100, 150, 200, 250, 300)],
        "vol_target": [{"vol_window": w, "max_exposure": c}
                       for w in (21, 63, 126) for c in (1.0, 0.75)],
        "state_screen": [{"state": s, "rank_window": 756, "cut": c}
                         for s in ("VIX", "SKEW", "BREADTH_SPX_MA200")
                         for c in (0.70, 0.85)],
        "term_spread": [{"short_leg": s, "floor": f}
                        for s in ("YIELD_3M", "YIELD_2Y")
                        for f in (0.0, 0.25, 0.50)],
    }
    out = []
    for fam in families:
        for p in grids.get(fam, [])[:_contract.QUALIFICATION_MAX_CONFIGS_PER_FAMILY]:
            out.append(SleeveSpec(sleeve=SLEEVE, family=fam, params=p,
                                  generate=FAMILIES[fam],
                                  stage=_contract.STAGE_QUALIFICATION))
    return out


def instrument_returns(panel: dict, idx: list) -> dict:
    """Hold-window return of the one instrument this sleeve may own."""
    lv = panel["columns"]["BENCHMARK"]
    dates = panel["dates"]
    hold = _contract.HOLD_SESSIONS
    return {dates[i]: {INSTRUMENT: _panels.hold_return(lv, i, hold)}
            for i in idx}
