"""SECTOR_ROTATION - which parts of the equity market, if any, deserve capital?

Sector total-return indices are clean instruments for this question: they carry
no survivorship problem (an index level is what it was), they are investable in
principle, and eleven of them run back to 1989.

Two definitional facts constrain what may be concluded, and both are handled in
data rather than mentioned in passing:

* **Real Estate did not exist as a GICS sector until 2016-08-31.** Its
  constituents lived inside Financials. Admitting both before that date
  double-counts the same companies, so Real Estate is admitted only from its
  introduction.
* **Communication Services is a 2018 restructuring** that absorbed constituents
  from Information Technology and Consumer Discretionary. The vendor supplies
  the restated history back to 1989, which nobody could have traded. The sleeve
  therefore reports the conservative post-restatement window alongside maximum
  history, and a result that exists only in the restated era does not qualify.
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
    top_k_long_only,
)
from . import _common as C

SLEEVE = _contract.SLEEVE_SECTOR_ROTATION
PANEL = _panels.PANEL_SECTOR

#: Legs that are never instruments: support columns on the same panel.
NON_SECTOR = ("CASH_YIELD", "BENCHMARK")


def admissible_sectors(panel: dict, date: str) -> list:
    """Sectors whose GICS definition existed on ``date``."""
    out = []
    for leg in panel.get("legs", []):
        name = leg["leg"]
        if name in NON_SECTOR:
            continue
        if name not in panel["columns"] and name != "REAL_ESTATE":
            continue
        if str(date) < str(leg.get("definition_from", "0000-00-00")):
            continue
        out.append(name)
    return out


def _column(panel: dict, name: str):
    if name in panel["columns"]:
        return panel["columns"][name]
    if name == "REAL_ESTATE":
        return _real_estate_column(panel)
    return None


def _real_estate_column(panel: dict):
    """Real Estate placed on the panel index, NaN before it existed."""
    cached = panel.get("_real_estate_aligned")
    if cached is not None:
        return cached
    re = panel.get("real_estate") or {}
    col = np.full(len(panel["dates"]), np.nan, dtype=float)
    if re.get("available"):
        pos = {d: i for i, d in enumerate(re["dates"])}
        for i, d in enumerate(panel["dates"]):
            j = pos.get(d)
            if j is not None:
                col[i] = re["close"][j]
    panel["_real_estate_aligned"] = col
    return col


def _opportunity(date: str, weights: dict, rationale: str,
                 state: dict) -> StrategyOpportunity:
    direction = DIRECTION_ROTATE if weights else DIRECTION_FLAT
    return StrategyOpportunity(
        sleeve=SLEEVE, decision_date=date, direction=direction,
        conviction=float(sum(weights.values())),
        recommended_exposure=weights, rationale=rationale,
        state_variables=state)


# --------------------------------------------------------------------------- #
# Families
# --------------------------------------------------------------------------- #
def gen_momentum(panel: dict, idx: list, params: dict) -> list:
    """Own the strongest sectors by trailing total return."""
    dates = panel["dates"]
    lookback = int(params["lookback"])
    skip = int(params.get("skip", 0))
    k = int(params["k"])
    out = []
    for i in idx:
        d = dates[i]
        scores = {}
        for s in admissible_sectors(panel, d):
            col = _column(panel, s)
            if col is None:
                continue
            m = C.momentum(col, i, lookback, skip)
            if np.isfinite(m):
                scores[s] = m
        if len(scores) < k:
            continue
        w = top_k_long_only(scores, k=k)
        out.append(_opportunity(d, w, f"top {k} of {len(scores)} sectors by "
                                      f"{lookback}-session momentum",
                                {"n_admissible": len(scores)}))
    return out


def gen_mean_reversion(panel: dict, idx: list, params: dict) -> list:
    """Own the weakest sectors - the opposite hypothesis, tested explicitly."""
    dates = panel["dates"]
    lookback = int(params["lookback"])
    k = int(params["k"])
    out = []
    for i in idx:
        d = dates[i]
        scores = {}
        for s in admissible_sectors(panel, d):
            col = _column(panel, s)
            if col is None:
                continue
            m = C.momentum(col, i, lookback)
            if np.isfinite(m):
                scores[s] = -m
        if len(scores) < k:
            continue
        w = top_k_long_only({s: v + 1.0 for s, v in scores.items()}, k=k)
        out.append(_opportunity(d, w, f"weakest {k} sectors over {lookback} "
                                      f"sessions", {"n_admissible": len(scores)}))
    return out


def gen_risk_parity(panel: dict, idx: list, params: dict) -> list:
    """Spread capital by inverse realised volatility across all sectors."""
    dates = panel["dates"]
    window = int(params["vol_window"])
    out = []
    for i in idx:
        d = dates[i]
        vols = {}
        for s in admissible_sectors(panel, d):
            col = _column(panel, s)
            if col is None:
                continue
            v = C.realised_vol(col, i, window)
            if np.isfinite(v) and v > 0:
                vols[s] = v
        if len(vols) < 3:
            continue
        w = C.inverse_vol_weights(vols)
        out.append(_opportunity(d, w, f"inverse {window}-session volatility "
                                      f"across {len(vols)} sectors",
                                {"n_admissible": len(vols)}))
    return out


def gen_momentum_screened(panel: dict, idx: list, params: dict) -> list:
    """Momentum, but hold cash unless the market itself is trending.

    Cash is a real asset choice, so a rotation strategy is allowed to decide
    that no sector deserves capital rather than always owning the best of a bad
    set - which is what an unconditional top-k does.
    """
    dates = panel["dates"]
    bench = panel["columns"]["BENCHMARK"]
    ma = int(params["market_ma"])
    lookback = int(params["lookback"])
    k = int(params["k"])
    out = []
    for i in idx:
        d = dates[i]
        m_ma = C.moving_average(bench, i, ma)
        px = float(bench[i])
        if not np.isfinite(m_ma) or not np.isfinite(px):
            continue
        if px <= m_ma:
            out.append(_opportunity(d, {}, "market below its own average; "
                                           "no sector qualifies",
                                    {"market_ma": m_ma}))
            continue
        scores = {}
        for s in admissible_sectors(panel, d):
            col = _column(panel, s)
            if col is None:
                continue
            mm = C.momentum(col, i, lookback)
            if np.isfinite(mm):
                scores[s] = mm
        if len(scores) < k:
            continue
        out.append(_opportunity(d, top_k_long_only(scores, k=k),
                                f"market trending; top {k} sectors",
                                {"market_ma": m_ma, "n_admissible": len(scores)}))
    return out


FAMILIES = {
    "momentum": gen_momentum,
    "mean_reversion": gen_mean_reversion,
    "risk_parity": gen_risk_parity,
    "momentum_screened": gen_momentum_screened,
}


def screening_specs() -> list:
    return [
        SleeveSpec(sleeve=SLEEVE, family="momentum",
                   params={"lookback": 126, "k": 3}, generate=gen_momentum),
        SleeveSpec(sleeve=SLEEVE, family="momentum",
                   params={"lookback": 252, "skip": 21, "k": 3},
                   generate=gen_momentum),
        SleeveSpec(sleeve=SLEEVE, family="mean_reversion",
                   params={"lookback": 21, "k": 3}, generate=gen_mean_reversion),
        SleeveSpec(sleeve=SLEEVE, family="mean_reversion",
                   params={"lookback": 63, "k": 3}, generate=gen_mean_reversion),
        SleeveSpec(sleeve=SLEEVE, family="risk_parity",
                   params={"vol_window": 63}, generate=gen_risk_parity),
        SleeveSpec(sleeve=SLEEVE, family="risk_parity",
                   params={"vol_window": 126}, generate=gen_risk_parity),
        SleeveSpec(sleeve=SLEEVE, family="momentum_screened",
                   params={"lookback": 126, "k": 3, "market_ma": 200},
                   generate=gen_momentum_screened),
        SleeveSpec(sleeve=SLEEVE, family="momentum_screened",
                   params={"lookback": 252, "k": 2, "market_ma": 200},
                   generate=gen_momentum_screened),
    ]


def qualification_specs(families: list) -> list:
    grids = {
        "momentum": [{"lookback": lb, "k": k}
                     for lb in (63, 126, 252) for k in (2, 3, 4)][:8],
        "mean_reversion": [{"lookback": lb, "k": k}
                           for lb in (5, 21, 63) for k in (2, 3, 4)][:8],
        "risk_parity": [{"vol_window": w} for w in (21, 63, 126, 252)],
        "momentum_screened": [{"lookback": lb, "k": k, "market_ma": m}
                              for lb in (126, 252) for k in (2, 3)
                              for m in (100, 200)][:8],
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
        d = dates[i]
        row = {}
        for s in admissible_sectors(panel, d):
            col = _column(panel, s)
            if col is None:
                continue
            row[s] = _panels.hold_return(col, i, hold)
        out[d] = row
    return out
