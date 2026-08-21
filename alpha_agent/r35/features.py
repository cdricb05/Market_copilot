"""alpha_agent.r35.features - the ONE Release 35 new-information feature owner.

Nineteen features across six acquired families, each with a declared economic
reading, each a backward-looking function of information that was PUBLIC at the
session it is stamped on. This module does not search transforms. Release 31
established that the binding constraint on this estate is information rather
than method, and Release 34 put a number on it; widening the transform grid here
would buy data-mining risk in exactly the place the release is trying to test
data.

Two conventions carried over from :mod:`alpha_agent.r33.features`, for the same
reasons they exist there:

* **Structural absence is a zero, not a median.** ``cot_spec_net_oi`` exists for
  the seventeen instruments with a mapped futures market and does not exist for
  ``AGG``. Filling the rest with a cross-sectional median would say "this bond
  fund has median speculative positioning", which is a claim; a neutral zero
  says "no such market", which is the truth. Every feature declares its fill in
  the contract and a companion ``_present`` mask records where the value is
  real.
* **Everything is lagged through one function.** No feature here reads a raw
  index; they all pass through :func:`alpha_agent.r35.information.as_of_align`,
  so the point-in-time rule has exactly one implementation.

The market-level features - implied volatility term structure, risk premia, the
market insider aggregate - are constant across instruments on a given date, and
that is not a defect. R33's ``g_vix_level`` is the same shape. A cross-sectional
model with per-group coefficients turns a common state variable into
cross-sectional variation, which is the mechanism by which "risk is expensive
today" becomes "own bonds rather than small caps".
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .. import pit_sector as _pit_sector
from ..r34 import contract as _r34_contract
from . import contract as _contract
from . import information as _info

CALCULATION_OWNER = "alpha_agent.r35.features"
REGISTRY_SCHEMA = "r35_new_feature_registry/1"
ARTIFACT_NAME = "new_feature_registry.json"

SESSIONS_PER_YEAR = 252.0
#: Trading sessions per calendar month, used to annualise a futures basis whose
#: legs are one contract month apart.
SESSIONS_PER_MONTH = 21.0


def _empty(calendar: pd.DatetimeIndex, symbols) -> pd.DataFrame:
    return pd.DataFrame(np.nan, index=calendar, columns=list(symbols),
                        dtype=float)


def _z(series: pd.Series, window: int) -> pd.Series:
    """Trailing z-score. The window ends at the current observation, so a value
    is scored against its own past and never against its future."""
    mean = series.rolling(window, min_periods=max(8, window // 4)).mean()
    std = series.rolling(window, min_periods=max(8, window // 4)).std(ddof=1)
    return (series - mean) / std.replace(0.0, np.nan)


# --------------------------------------------------------------------------- #
# FUTURES_POSITIONING
# --------------------------------------------------------------------------- #
def build_positioning(cot_frame: pd.DataFrame, calendar: pd.DatetimeIndex,
                      symbols, *,
                      lag_days: int = _contract.COT_PUBLICATION_LAG_DAYS
                      ) -> dict:
    """Speculator net positioning, its extremity, and its change."""
    out = {name: _empty(calendar, symbols)
           for name in _contract.features_of(_contract.FAM_POSITIONING)}
    present = pd.DataFrame(False, index=calendar, columns=list(symbols))
    mapped = {}
    for symbol, (codes, tier, market) in _contract.COT_MAPPING.items():
        if symbol not in set(symbols):
            continue
        weekly = _info.cot_instrument_series(cot_frame, codes,
                                             lag_days=lag_days)
        if weekly.empty:
            continue
        net = weekly["spec_net_oi"]
        block = pd.DataFrame({
            "cot_spec_net_oi": net,
            "cot_spec_net_z156": _z(net, _contract.COT_Z_WINDOW_WEEKS),
            "cot_spec_net_chg_13w": net - net.shift(_contract.COT_CHANGE_WEEKS),
            "cot_oi_chg_13w": np.log(
                weekly["open_interest"]
                / weekly["open_interest"].shift(_contract.COT_CHANGE_WEEKS)),
        })
        for name in out:
            aligned = _info.as_of_align(block[name], calendar)
            out[name][symbol] = aligned
            if name == "cot_spec_net_oi":
                present[symbol] = aligned.notna()
        mapped[symbol] = {"codes": list(codes), "tier": tier,
                          "market": market,
                          "weekly_observations": int(len(weekly)),
                          "first": str(weekly.index.min())[:10],
                          "last": str(weekly.index.max())[:10]}
    return {"features": out, "present": present, "mapped": mapped,
            "family": _contract.FAM_POSITIONING}


# --------------------------------------------------------------------------- #
# FX_INTEREST_CARRY
# --------------------------------------------------------------------------- #
def build_fx_carry(fred: dict, calendar: pd.DatetimeIndex, symbols) -> dict:
    """The interest differential Release 33 recorded as structurally absent."""
    out = {name: _empty(calendar, symbols)
           for name in _contract.features_of(_contract.FAM_FX_CARRY)}
    present = pd.DataFrame(False, index=calendar, columns=list(symbols))
    us = fred.get(_contract.FRED_US_SHORT_RATE)
    if us is None or us.empty:
        return {"features": out, "present": present, "mapped": {},
                "family": _contract.FAM_FX_CARRY,
                "reason": "US_SHORT_RATE_ABSENT"}
    us_daily = _info.as_of_align(us, calendar)

    def foreign(key: str):
        sid = _contract.FRED_FOREIGN_SHORT_RATES.get(key)
        line = fred.get(sid) if sid else None
        return None if line is None or line.empty else _info.as_of_align(
            line, calendar)

    basket = {}
    for key, weight in _contract.USDX_BASKET.items():
        line = foreign(key)
        if line is not None:
            basket[key] = (line, weight)

    mapped = {}
    for symbol, (currency, tier, reading) in _contract.FX_CARRY_MAPPING.items():
        if symbol not in set(symbols):
            continue
        if currency == "USDX_SHORT":
            if not basket:
                continue
            weights = sum(w for _, w in basket.values())
            blended = sum(line * (w / weights) for line, w in basket.values())
            differential = -(blended - us_daily)
            components = sorted(basket)
        else:
            line = foreign(currency)
            if line is None:
                continue
            differential = line - us_daily
            components = [currency]
        out["fx_carry_diff"][symbol] = differential
        out["fx_carry_chg_63"][symbol] = differential - differential.shift(
            _contract.CARRY_CHANGE_SESSIONS)
        present[symbol] = differential.notna()
        mapped[symbol] = {"currency": currency, "tier": tier,
                          "reading": reading, "components": components,
                          "observations": int(differential.notna().sum())}
    return {"features": out, "present": present, "mapped": mapped,
            "family": _contract.FAM_FX_CARRY}


# --------------------------------------------------------------------------- #
# COMMODITY_TERM_STRUCTURE
# --------------------------------------------------------------------------- #
def build_commodity_curve(curve: pd.DataFrame, calendar: pd.DatetimeIndex,
                          symbols) -> dict:
    """The shape of a real dated futures curve, never a spot transformation."""
    out = {name: _empty(calendar, symbols)
           for name in _contract.features_of(_contract.FAM_COMMODITY_CURVE)}
    present = pd.DataFrame(False, index=calendar, columns=list(symbols))
    if curve is None or curve.empty:
        return {"features": out, "present": present, "mapped": {},
                "family": _contract.FAM_COMMODITY_CURVE,
                "reason": "NO_CURVE"}
    c1, c2, c4 = (curve[_contract.EIA_WTI_CONTRACTS[0]],
                  curve[_contract.EIA_WTI_CONTRACTS[1]],
                  curve[_contract.EIA_WTI_CONTRACTS[3]])
    # One contract month apart, annualised; three months apart for the slope.
    front_basis = np.log(c1 / c2) * 12.0
    curve_slope = np.log(c1 / c4) * 4.0
    aligned_basis = _info.as_of_align(front_basis, calendar)
    aligned_slope = _info.as_of_align(curve_slope, calendar)
    basis_change = aligned_basis - aligned_basis.shift(
        _contract.COMMODITY_CHANGE_SESSIONS)

    mapped = {}
    for symbol, (market, tier, reading) in \
            _contract.COMMODITY_CURVE_MAPPING.items():
        if symbol not in set(symbols):
            continue
        out["cmdty_front_basis"][symbol] = aligned_basis
        out["cmdty_curve_slope"][symbol] = aligned_slope
        out["cmdty_basis_chg_63"][symbol] = basis_change
        present[symbol] = aligned_basis.notna()
        mapped[symbol] = {"market": market, "tier": tier, "reading": reading}
    return {"features": out, "present": present, "mapped": mapped,
            "family": _contract.FAM_COMMODITY_CURVE,
            "curve_first": str(curve.index.min())[:10],
            "curve_last": str(curve.index.max())[:10],
            "source_discontinued_at": str(curve.index.max())[:10]}


# --------------------------------------------------------------------------- #
# IMPLIED_VOLATILITY_TERM_STRUCTURE
# --------------------------------------------------------------------------- #
def build_iv_term(cboe: dict, panel: dict, calendar: pd.DatetimeIndex,
                  symbols) -> dict:
    """Slope of implied volatility, and implied minus realised variance.

    The BASE information set already carries the VIX level and its one-month
    change. Neither of those is the slope between 30-day and 93-day implied
    volatility, and neither is the variance risk premium; if the orthogonality
    measurement disagrees it will say so and these features will be labelled
    redundant.
    """
    out = {name: _empty(calendar, symbols)
           for name in _contract.features_of(_contract.FAM_IV_TERM)}
    present = pd.DataFrame(False, index=calendar, columns=list(symbols))
    vix, vix3m = cboe.get("VIX"), cboe.get("VIX3M")
    if vix is None or vix3m is None or vix.empty or vix3m.empty:
        return {"features": out, "present": present, "mapped": {},
                "family": _contract.FAM_IV_TERM, "reason": "CBOE_SERIES_ABSENT"}
    slope = np.log(vix3m / vix.reindex(vix3m.index).ffill())
    aligned_slope = _info.as_of_align(slope.dropna(), calendar)
    aligned_vix = _info.as_of_align(vix, calendar)

    benchmark = _r34_contract.BENCHMARK_SYMBOL
    benchmark_returns = (panel["log_returns"][benchmark]
                         if benchmark in panel["log_returns"].columns
                         else None)
    if benchmark_returns is None:
        realised = pd.Series(np.nan, index=calendar, dtype=float)
    else:
        realised = (benchmark_returns.rolling(
            _contract.IV_REALISED_WINDOW_SESSIONS,
            min_periods=_contract.IV_REALISED_WINDOW_SESSIONS // 2)
            .std(ddof=1) * np.sqrt(SESSIONS_PER_YEAR)) ** 2
        realised = realised.reindex(calendar)
    premium = (aligned_vix / 100.0) ** 2 - realised

    slope_change = aligned_slope - aligned_slope.shift(
        _contract.IV_CHANGE_SESSIONS)
    for symbol in symbols:
        out["iv_term_slope"][symbol] = aligned_slope
        out["iv_term_slope_chg_21"][symbol] = slope_change
        out["variance_risk_premium"][symbol] = premium
        present[symbol] = aligned_slope.notna()
    return {"features": out, "present": present,
            "mapped": {"scope": "MARKET_LEVEL",
                       "instruments": len(list(symbols)),
                       "vix3m_first": str(vix3m.index.min())[:10]},
            "family": _contract.FAM_IV_TERM}


# --------------------------------------------------------------------------- #
# MARKET_IMPLIED_RISK_PREMIA
# --------------------------------------------------------------------------- #
def build_risk_premia(fred: dict, calendar: pd.DatetimeIndex, symbols) -> dict:
    """Real yields, breakeven inflation, curve curvature, credit premium."""
    out = {name: _empty(calendar, symbols)
           for name in _contract.features_of(_contract.FAM_RISK_PREMIA)}
    present = pd.DataFrame(False, index=calendar, columns=list(symbols))

    def aligned(sid: str) -> pd.Series:
        line = fred.get(sid)
        if line is None or line.empty:
            return pd.Series(np.nan, index=calendar, dtype=float)
        return _info.as_of_align(line, calendar)

    real_yield = aligned("DFII10")
    breakeven = aligned("T10YIE")
    two, ten, thirty = aligned("DGS2"), aligned("DGS10"), aligned("DGS30")
    credit = aligned("BAA10Y")

    values = {
        "real_yield_10y": real_yield,
        "breakeven_10y_chg_63": breakeven - breakeven.shift(
            _contract.BREAKEVEN_CHANGE_SESSIONS),
        "curve_curvature": 2.0 * ten - two - thirty,
        "credit_premium_baa10y": credit,
    }
    for symbol in symbols:
        for name, line in values.items():
            out[name][symbol] = line
        present[symbol] = real_yield.notna() & credit.notna()
    return {"features": out, "present": present,
            "mapped": {"scope": "MARKET_LEVEL",
                       "series": ["DFII10", "T10YIE", "DGS2", "DGS10",
                                  "DGS30", "BAA10Y"]},
            "family": _contract.FAM_RISK_PREMIA}


# --------------------------------------------------------------------------- #
# INSIDER_TRANSACTION_INTENSITY
# --------------------------------------------------------------------------- #
def _net_buy_ratio(buys: pd.Series, sells: pd.Series, window: int,
                   floor: int) -> pd.Series:
    rolled_buy = buys.rolling(window, min_periods=1).sum()
    rolled_sell = sells.rolling(window, min_periods=1).sum()
    total = rolled_buy + rolled_sell
    ratio = (rolled_buy - rolled_sell) / total.replace(0.0, np.nan)
    return ratio.where(total >= floor)


def build_insider(sector_daily: pd.DataFrame, calendar: pd.DatetimeIndex,
                  symbols, *,
                  window: int = _contract.INSIDER_WINDOW_SESSIONS,
                  floor: int = _contract.MIN_INSIDER_FILINGS_IN_WINDOW) -> dict:
    """Sector and market insider buy-minus-sell share, by FILING count."""
    out = {name: _empty(calendar, symbols)
           for name in _contract.features_of(_contract.FAM_INSIDER)}
    present = pd.DataFrame(False, index=calendar, columns=list(symbols))
    if sector_daily is None or sector_daily.empty:
        return {"features": out, "present": present, "mapped": {},
                "family": _contract.FAM_INSIDER, "reason": "NO_INSIDER_TABLE"}

    table = sector_daily.copy()
    table["filed"] = pd.to_datetime(table["filed"])
    buys = table.pivot_table(index="filed", columns="sector",
                             values="buy_filings", aggfunc="sum").fillna(0.0)
    sells = table.pivot_table(index="filed", columns="sector",
                              values="sell_filings", aggfunc="sum").fillna(0.0)

    def on_calendar(frame: pd.DataFrame) -> pd.DataFrame:
        return frame.reindex(frame.index.union(calendar)).fillna(0.0) \
            .reindex(calendar).fillna(0.0)

    buys_cal, sells_cal = on_calendar(buys), on_calendar(sells)
    market_ratio = _net_buy_ratio(buys_cal.sum(axis=1), sells_cal.sum(axis=1),
                                  window, floor)
    market_ratio = _info.as_of_align(market_ratio.dropna(), calendar)

    mapped = {}
    for symbol, sector in _contract.INSIDER_SECTOR_MAPPING.items():
        if symbol not in set(symbols) or sector not in buys_cal.columns:
            continue
        ratio = _net_buy_ratio(buys_cal[sector], sells_cal[sector], window,
                               floor)
        anomaly = ratio - ratio.rolling(
            _contract.INSIDER_ANOMALY_WINDOW_SESSIONS,
            min_periods=_contract.INSIDER_ANOMALY_WINDOW_SESSIONS // 2).mean()
        out["insider_net_buy_63"][symbol] = _info.as_of_align(
            ratio.dropna(), calendar)
        out["insider_net_buy_anomaly"][symbol] = _info.as_of_align(
            anomaly.dropna(), calendar)
        present[symbol] = out["insider_net_buy_63"][symbol].notna()
        mapped[symbol] = {
            "sector": sector,
            "directional_filings": int(buys_cal[sector].sum()
                                       + sells_cal[sector].sum())}
    for symbol in symbols:
        out["insider_market_net_buy_63"][symbol] = market_ratio
        present[symbol] = present[symbol] | market_ratio.notna()
    unmapped = [s for s in _contract.INSIDER_UNMAPPED_SECTOR_ETFS
                if s in set(symbols)]
    return {"features": out, "present": present, "mapped": mapped,
            "family": _contract.FAM_INSIDER,
            "unmapped_sector_etfs": unmapped,
            "sector_map_caveat": _contract.INSIDER_SECTOR_MAP_CAVEAT,
            "unknown_sector_label": _pit_sector.UNKNOWN}


# --------------------------------------------------------------------------- #
# Assembly
# --------------------------------------------------------------------------- #
def build_all(*, panel: dict, cot_frame, fred: dict, cboe: dict,
              curve, insider_table,
              cot_lag_days: int = _contract.COT_PUBLICATION_LAG_DAYS) -> dict:
    """Every acquired family, on the panel's calendar and symbol set."""
    calendar, symbols = panel["calendar"], list(panel["symbols"])
    built = {}
    if cot_frame is not None:
        built[_contract.FAM_POSITIONING] = build_positioning(
            cot_frame, calendar, symbols, lag_days=cot_lag_days)
    if fred:
        built[_contract.FAM_FX_CARRY] = build_fx_carry(fred, calendar, symbols)
        built[_contract.FAM_RISK_PREMIA] = build_risk_premia(
            fred, calendar, symbols)
    if cboe:
        built[_contract.FAM_IV_TERM] = build_iv_term(cboe, panel, calendar,
                                                     symbols)
    if curve is not None:
        built[_contract.FAM_COMMODITY_CURVE] = build_commodity_curve(
            curve, calendar, symbols)
    if insider_table is not None:
        built[_contract.FAM_INSIDER] = build_insider(insider_table, calendar,
                                                     symbols)
    return built


def frames(built: dict) -> dict:
    """feature name -> (dates x instruments) frame, across every family."""
    out = {}
    for family in built.values():
        out.update(family.get("features") or {})
    return out


def presence(built: dict) -> dict:
    """family -> boolean (dates x instruments) mask of REAL, not filled, values."""
    return {name: family.get("present") for name, family in built.items()}


def coverage(built: dict, *, evaluation_dates=None) -> dict:
    """Row coverage, window and instrument breadth for every family.

    ``evaluation_dates`` restricts the count to the dates a forecast is actually
    struck on, which is the only coverage number that can affect an inference.
    """
    out = {}
    for name, family in built.items():
        mask = family.get("present")
        if mask is None or mask.empty:
            out[name] = {"ok": False, "reason": family.get("reason")
                         or "NO_PRESENCE_MASK", "row_coverage": 0.0}
            continue
        window = mask.loc[mask.index.isin(evaluation_dates)] \
            if evaluation_dates is not None else mask
        any_by_date = window.any(axis=1)
        first_date = (window.index[any_by_date.argmax()]
                      if bool(any_by_date.any()) else None)
        per_instrument = window.sum(axis=0)
        out[name] = {
            "ok": bool(any_by_date.any()),
            "features": sorted(_contract.features_of(name)),
            "row_coverage": float(window.to_numpy().mean()) if window.size
            else 0.0,
            "dates_with_any_value": int(any_by_date.sum()),
            "dates_total": int(len(window)),
            "instruments_with_any_value": int((per_instrument > 0).sum()),
            "instruments_total": int(window.shape[1]),
            "first_usable_date": str(first_date)[:10] if first_date is not None
            else None,
            "mapped": family.get("mapped"),
            "reason": family.get("reason"),
        }
        for key in ("source_discontinued_at", "curve_last",
                    "unmapped_sector_etfs", "sector_map_caveat"):
            if key in family:
                out[name][key] = family[key]
    return out


def registry_artifact(*, campaign_id: str, created_at: str, built: dict,
                      coverage_report: dict) -> dict:
    from .. import r35
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "base_feature_owner": _contract.BASE_FEATURE_OWNER,
        "base_feature_count": 28,
        "new_features": {n: {"family": f, "reading": r, "absent_fill": a}
                         for n, (f, r, a) in
                         sorted(_contract.NEW_FEATURES.items())},
        "new_feature_count": len(_contract.NEW_FEATURES),
        "families_built": sorted(built),
        "families_declared": list(_contract.ACQUIRED_FAMILIES),
        "families_absent": sorted(set(_contract.ACQUIRED_FAMILIES)
                                  - set(built)),
        "coverage": coverage_report,
        "structural_absence_fill": _contract.FILL_NEUTRAL_ZERO,
        "structural_absence_reason": (
            "a feature that does not exist for an instrument is filled with a "
            "neutral zero and flagged absent, so 'no such market' stays "
            "distinguishable from 'happened to be median'"),
    }
    return r35.artifact_body(REGISTRY_SCHEMA, payload)


def path_for(campaign_id: str = _contract.CAMPAIGN_ID):
    from .. import r35
    return r35.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    from .. import r35
    return r35.write_json(path_for(body.get("campaign_id",
                                            _contract.CAMPAIGN_ID)), body)


__all__ = ["CALCULATION_OWNER", "build_positioning", "build_fx_carry",
           "build_commodity_curve", "build_iv_term", "build_risk_premia",
           "build_insider", "build_all", "frames", "presence", "coverage",
           "registry_artifact", "freeze", "path_for"]
