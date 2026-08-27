"""alpha_agent.r46.regime - EX-ANTE regime descriptors. Known at decision time.

Every descriptor here is computed from bars printed ON OR BEFORE the decision
session, from data the estate owns, with thresholds declared in this file
before any forward P&L existed. No descriptor is ever relabelled after the
fact: a regime row is appended once per decision session and the value it
carried on that day is the value the attribution uses forever.

Descriptors (each with its declared rule):

* ``equity_trend``       SPY close vs its 200-session mean (UP / DOWN)
* ``volatility_regime``  $VIX close: LOW < 15, MID 15-25, HIGH > 25
* ``rates_level``        10y CMT: LOW < 3, MID 3-4.5, HIGH > 4.5
* ``curve_regime``       10y-2y: INVERTED < 0, FLAT 0-0.5, STEEP > 0.5
* ``credit_stress``      CCC-and-lower HY OAS z vs 252-session window:
                         CALM < 0, ELEVATED 0-1.5, STRESSED > 1.5
* ``inflation_regime``   CPI 12-month change as last PUBLISHED: LOW < 2,
                         MODERATE 2-4, HIGH > 4 (the series is monthly and
                         carries publication lag; it is read as available)
* ``risk_appetite``      RISK_ON when equity trend UP and volatility not HIGH
                         and credit not STRESSED; RISK_OFF when two of the
                         three are adverse; MIXED otherwise
* ``liquidity``          SPY 20-session volume vs 252-session: THIN < 0.8,
                         NORMAL, HEAVY > 1.2
* ``cross_asset_corr``   60-session correlation of SPY and &ZN daily returns:
                         NEGATIVE < -0.2, NEUTRAL, POSITIVE > 0.2

Research only; a regime is a label for attribution, never a signal.
"""
from __future__ import annotations

import datetime as _dt
import math
from typing import Optional

import numpy as np

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, write_json
from . import clock as CK

CALCULATION_OWNER = "alpha_agent.r46.regime"

ARTIFACT = "R46_4_REGIME_STATE.json"

DESCRIPTORS = ("equity_trend", "volatility_regime", "rates_level",
               "curve_regime", "credit_stress", "inflation_regime",
               "risk_appetite", "liquidity", "cross_asset_corr")

THRESHOLDS = {
    "equity_trend_window": 200,
    "vix_low": 15.0, "vix_high": 25.0,
    "rates_low": 3.0, "rates_high": 4.5,
    "curve_flat_upper": 0.5,
    "credit_z_window": 252, "credit_elevated": 0.0, "credit_stressed": 1.5,
    "inflation_low": 2.0, "inflation_high": 4.0,
    "liquidity_short": 20, "liquidity_long": 252,
    "liquidity_thin": 0.8, "liquidity_heavy": 1.2,
    "corr_window": 60, "corr_negative": -0.2, "corr_positive": 0.2,
}

SERIES = {"equity": "SPY", "vix": "$VIX", "rates_10y": "%10YTCM",
          "curve": "#US10Y-2Y", "credit": "%CCCHYS", "cpi_yoy": "#CPISA3",
          "rates_future": "&ZN"}

UNKNOWN = "UNKNOWN"


def _upto(series, as_of: _dt.date):
    if series is None or not len(series):
        return None
    s = series[[ts.date() <= as_of for ts in series.index]]
    return s if len(s) else None


def _last(series, as_of: _dt.date) -> Optional[float]:
    s = _upto(series, as_of)
    if s is None:
        return None
    v = float(s.iloc[-1])
    return v if math.isfinite(v) else None


def describe(as_of: _dt.date, series_fn=None, volume_fn=None) -> dict:
    """Every descriptor for ONE session, from bars on or before it."""
    from . import marketdata as MD
    sf = series_fn or MD.closes
    vf = volume_fn or MD.volumes
    T = THRESHOLDS
    out = {"session": str(as_of)}

    spy = _upto(sf(SERIES["equity"]), as_of)
    if spy is not None and len(spy) >= T["equity_trend_window"]:
        ma = float(spy.iloc[-T["equity_trend_window"]:].mean())
        px = float(spy.iloc[-1])
        out["equity_trend"] = "UP" if px > ma else "DOWN"
        out["equity_trend_value"] = px / ma - 1.0
    else:
        out["equity_trend"] = UNKNOWN

    vix = _last(sf(SERIES["vix"]), as_of)
    out["volatility_value"] = vix
    out["volatility_regime"] = (UNKNOWN if vix is None else
                                "LOW" if vix < T["vix_low"] else
                                "HIGH" if vix > T["vix_high"] else "MID")

    r10 = _last(sf(SERIES["rates_10y"]), as_of)
    out["rates_value"] = r10
    out["rates_level"] = (UNKNOWN if r10 is None else
                          "LOW" if r10 < T["rates_low"] else
                          "HIGH" if r10 > T["rates_high"] else "MID")

    slope = _last(sf(SERIES["curve"]), as_of)
    out["curve_value"] = slope
    out["curve_regime"] = (UNKNOWN if slope is None else
                           "INVERTED" if slope < 0 else
                           "STEEP" if slope > T["curve_flat_upper"] else "FLAT")

    cr = _upto(sf(SERIES["credit"]), as_of)
    if cr is not None and len(cr) >= 60:
        w = cr.iloc[-T["credit_z_window"]:]
        sd = float(w.std(ddof=1))
        z = ((float(w.iloc[-1]) - float(w.mean())) / sd) if sd > 0 else None
        out["credit_value"] = float(w.iloc[-1])
        out["credit_z"] = z
        out["credit_stress"] = (UNKNOWN if z is None else
                                "CALM" if z < T["credit_elevated"] else
                                "STRESSED" if z > T["credit_stressed"]
                                else "ELEVATED")
    else:
        out["credit_stress"] = UNKNOWN

    cpi = _last(sf(SERIES["cpi_yoy"]), as_of)
    out["inflation_value_as_published"] = cpi
    out["inflation_regime"] = (UNKNOWN if cpi is None else
                               "LOW" if cpi < T["inflation_low"] else
                               "HIGH" if cpi > T["inflation_high"]
                               else "MODERATE")

    adverse = sum([out["equity_trend"] == "DOWN",
                   out["volatility_regime"] == "HIGH",
                   out["credit_stress"] == "STRESSED"])
    known = sum([out["equity_trend"] != UNKNOWN,
                 out["volatility_regime"] != UNKNOWN,
                 out["credit_stress"] != UNKNOWN])
    out["risk_appetite"] = (UNKNOWN if known < 2 else
                            "RISK_ON" if adverse == 0 else
                            "RISK_OFF" if adverse >= 2 else "MIXED")

    vol = _upto(vf(SERIES["equity"]), as_of) if vf else None
    if vol is not None and len(vol) >= T["liquidity_long"]:
        ratio = (float(vol.iloc[-T["liquidity_short"]:].mean())
                 / max(1e-9, float(vol.iloc[-T["liquidity_long"]:].mean())))
        out["liquidity_value"] = ratio
        out["liquidity"] = ("THIN" if ratio < T["liquidity_thin"] else
                            "HEAVY" if ratio > T["liquidity_heavy"]
                            else "NORMAL")
    else:
        out["liquidity"] = UNKNOWN

    zn = _upto(sf(SERIES["rates_future"]), as_of)
    if spy is not None and zn is not None:
        a = np.log(spy.where(spy > 0)).diff()
        b = np.log(zn.where(zn > 0)).diff()
        j = a.align(b, join="inner")
        a, b = j[0].dropna(), j[1].dropna()
        j = a.align(b, join="inner")
        a, b = j[0].iloc[-T["corr_window"]:], j[1].iloc[-T["corr_window"]:]
        if len(a) >= T["corr_window"] // 2 and float(a.std()) > 0 \
                and float(b.std()) > 0:
            c = float(np.corrcoef(a, b)[0, 1])
            out["cross_asset_corr_value"] = c
            out["cross_asset_corr"] = ("NEGATIVE" if c < T["corr_negative"]
                                       else "POSITIVE" if c > T["corr_positive"]
                                       else "NEUTRAL")
        else:
            out["cross_asset_corr"] = UNKNOWN
    else:
        out["cross_asset_corr"] = UNKNOWN

    out["ex_ante"] = True
    out["hindsight_labels"] = False
    out["calculation_owner"] = CALCULATION_OWNER
    return out


# --------------------------------------------------------------------------- #
def load(campaign_id: str = CAMPAIGN_ID) -> dict:
    return read_json(campaign_dir(campaign_id) / ARTIFACT, default=None) or {}


def record(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID,
           series_fn=None, volume_fn=None) -> dict:
    """Append the regime for ``as_of`` if not already recorded. Idempotent.

    A session already on the record keeps its original row: a regime label is
    an ex-ante fact and it is never recomputed with later bars.
    """
    prior = load(campaign_id)
    rows = list(prior.get("rows") or [])
    have = {str(r.get("session")) for r in rows}
    appended = False
    if str(as_of) not in have:
        rows.append(dict(describe(as_of, series_fn, volume_fn),
                         recorded_at_utc=CK.iso(CK.now_utc())))
        rows.sort(key=lambda r: r["session"])
        appended = True
    body = artifact_body(
        "r46_4_regime_state/1", CALCULATION_OWNER,
        descriptors=list(DESCRIPTORS),
        thresholds=dict(THRESHOLDS),
        series=dict(SERIES),
        n_sessions=len(rows),
        latest=rows[-1] if rows else None,
        appended_this_run=appended,
        ex_ante_only=True,
        never_relabelled=True,
        rows=rows,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


def regime_for(session: str, campaign_id: str = CAMPAIGN_ID) -> dict:
    """The regime recorded ON the session, or the latest one at or before it."""
    rows = load(campaign_id).get("rows") or []
    best = None
    for r in rows:
        if str(r.get("session")) <= str(session):
            best = r
        else:
            break
    return best or {}


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "DESCRIPTORS", "THRESHOLDS",
           "SERIES", "describe", "record", "load", "regime_for"]
