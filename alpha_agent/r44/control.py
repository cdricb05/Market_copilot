"""alpha_agent.r44.control - ENGINE 2C, the structural-premium control.

The question Release 44 exists to answer is not "did the portfolio make
money". After 302 trials this estate knows perfectly well that a basket of
carry sleeves makes money on paper. The question is:

    could we have had the same outcome by harvesting KNOWN RISK PREMIA,
    with no informational Alpha anywhere in the book?

So the control is built the same way as the candidate - same combination
rule, same constraints, same capital treatment, same zones, same costs -
from the PREMIUM-role streams only. If the residual portfolio cannot beat
it, then diversification produced a smoother package of premia and the
honest label is STRUCTURAL_PREMIUM, not PORTFOLIO_ALPHA.

A second, harsher control is inherited unchanged from R43: a
volatility-matched, always-long equal-risk book over the same markets. It
answers the cruder question - could we have had this by being long?
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..r41 import evidence as EV
from ..r43 import carry as KARRY
from ..r43 import judge as J
from ..r43 import panels as P
from . import combine as CB
from . import contract as C
from . import streams as ST

CALCULATION_OWNER = "alpha_agent.r44.control"


# --------------------------------------------------------------------------- #
# Control 1 - the structural-premium portfolio
# --------------------------------------------------------------------------- #
def premium_portfolio(frame: pd.DataFrame, fit_dates, meta: dict, *,
                      rule: str = None, cost_multiplier: float = 1.0,
                      inv: dict = None) -> dict:
    """The same combination rule applied to the PREMIUM streams alone."""
    rule = rule or C.PRIMARY_COMBINATION_RULE
    ids = [s for s in C.PREMIUM_STREAM_IDS if s in frame.columns]
    sub = frame[ids]
    fit = CB.fit_weights(sub, fit_dates, meta, rule)
    if fit.get("state") != "FITTED":
        return {"state": fit.get("state"), "rule": rule, "stream_ids": ids}
    fit["stream_ids"] = list(fit["weights"])
    fit["role"] = "STRUCTURAL_PREMIUM_CONTROL"
    return fit


# --------------------------------------------------------------------------- #
# Control 2 - the volatility-matched passive long (R43's control)
# --------------------------------------------------------------------------- #
_PASSIVE_CACHE = {}


def passive_long_stream(asset_classes=("FX", "RATES", "COMMODITY",
                                       "US_EQUITY", "INTERNATIONAL_EQUITY")
                        ) -> pd.Series:
    """An always-long, equal-risk book over the estate's own futures markets.

    This is not a benchmark chosen to be beatable. It is the cheapest thing
    an investor could have done with the same instruments: hold them, sized
    by their own risk, and never trade on information at all.
    """
    key = tuple(asset_classes)
    if key in _PASSIVE_CACHE:
        return _PASSIVE_CACHE[key]
    markets = []
    for ac in asset_classes:
        markets += list(P.markets_by(asset_class=ac) or [])
    markets = sorted(set(markets))
    keep = []
    for m in markets:
        d = P.futures_daily(m)
        if d is not None and "ret1" in d.columns \
                and pd.to_numeric(d["ret1"], errors="coerce").notna().sum() \
                >= 500:
            keep.append(m)
    if not keep:
        _PASSIVE_CACHE[key] = pd.Series(dtype=float)
        return _PASSIVE_CACHE[key]
    ret = P.field_frame(keep, "ret1", min_obs=500)
    # Equal RISK, not equal notional: each market is scaled by its own
    # trailing volatility, causally, with no forward information.
    vol = ret.rolling(KARRY.VOL_WIN, min_periods=60).std().shift(1)
    w = (1.0 / vol).replace([np.inf, -np.inf], np.nan)
    w = w.div(w.sum(axis=1), axis=0)
    s = (w * ret).sum(axis=1)
    s.index = ST._naive_days(s.index)
    s = s[~s.index.duplicated(keep="last")].rename("passive_long")
    _PASSIVE_CACHE[key] = s
    return s


def volatility_matched_increment(candidate: pd.Series, control: pd.Series,
                                 dates=None, *, lags: int = 21) -> dict:
    """``candidate - control`` after matching the control's volatility.

    Matching is done on the SAME dates the increment is measured on, which
    is the only way the comparison is scale-free rather than flattering.
    """
    d = pd.DatetimeIndex(dates) if dates is not None else candidate.index
    cand = candidate.reindex(d).dropna()
    ctl = control.reindex(cand.index).fillna(0.0)
    sc = float(np.nanstd(cand.to_numpy(dtype=float), ddof=1))
    sp = float(np.nanstd(ctl.to_numpy(dtype=float), ddof=1))
    if not sp or not np.isfinite(sp):
        return {"state": "NOT_RUN", "reason": "control volatility is zero"}
    matched = ctl * (sc / sp)
    inc = cand - matched
    hac = EV.hac_t(inc.to_numpy(dtype=float), lags=lags)
    return {
        "state": "MEASURED",
        "n": int(len(inc)),
        "candidate_excess_ann": float(np.nanmean(cand) * J.TRADING_DAYS),
        "control_excess_ann_raw": float(np.nanmean(ctl) * J.TRADING_DAYS),
        "control_excess_ann_vol_matched": float(
            np.nanmean(matched) * J.TRADING_DAYS),
        "increment_ann": float(np.nanmean(inc) * J.TRADING_DAYS),
        "increment_t_hac": hac.get("t"),
        "volatility_matched": True,
        "scale_applied": float(sc / sp),
        "signal_is_decoration": bool((hac.get("t") or 0.0) < 2.0),
    }


def build_controls(frame: pd.DataFrame, fit_dates, meta: dict, *,
                   rule: str = None, cost_multiplier: float = 1.0) -> dict:
    """Both controls, as return streams ready to be differenced."""
    rule = rule or C.PRIMARY_COMBINATION_RULE
    prem = premium_portfolio(frame, fit_dates, meta, rule=rule,
                             cost_multiplier=cost_multiplier)
    prem_ret = None
    if prem.get("state") == "FITTED":
        prem_ret = CB.portfolio_returns(frame, prem["weights"])
    passive = passive_long_stream()
    return {
        "calculation_owner": CALCULATION_OWNER,
        "rule": rule,
        "structural_premium_control": prem,
        "structural_premium_returns": prem_ret,
        "passive_long_returns": passive,
        "controls_declared": dict(C.CONTROLS),
        "primary_control": C.PRIMARY_PORTFOLIO_CONTROL,
    }
