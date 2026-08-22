"""alpha_agent.r36.native_markets - the ONE Release 36 instrument owner.

This module turns owned and free point-in-time data into the thing a strategy
can actually hold. It is where LEVEL 1 SIGNAL becomes LEVEL 3 NATIVE, and every
construction below is written to be defensible about exactly one question: could
somebody have earned this return, with this information, on this date?

The three constructions that matter, and why each is built the way it is:

**A currency position is a forward, and its return has two legs.** Release 33
held FX spot and recorded ``FX_SPOT_EXCLUDES_CARRY`` because it had no foreign
short rate. With one, the excess return of holding currency c against the dollar
is ``spot return + (i_c - i_usd) * days/365``, which is what a deliverable
one-month forward earns up to covered-interest-parity deviations. The interest
leg is the OECD three-month interbank rate, stamped forward by the publication
lag the estate already uses, never a spot-derived stand-in.

**A commodity futures return needs contract identity, and this one has it
without ever guessing.** EIA publishes settlement prices for the nearest four
DATED contracts. Over one month the second-nearest contract becomes the nearest,
so the return of buying contract 2 at month end and holding it is exactly
``C1(t+1) / C2(t) - 1``. No roll date is inferred, no contract is selected with
hindsight, and the curve slope ``ln(C1/C2)`` observed at t is a real basis
between two different contracts quoted on the same day - not a lagged
transformation of one price.

**A rates curve trade needs a duration, and the duration is measured.** The
tradable legs are ICE BofA Treasury total-return indices by maturity bucket. The
duration of each bucket is estimated by regressing its realised return on the
change in its matched constant-maturity yield over an EXPANDING TRAILING window,
so a duration-neutral weight on date t uses only what was observable before t.

Every alignment goes through :func:`alpha_agent.r35.information.as_of_align`,
which is the estate's one publication-lag rule. Every Norgate read goes through
:func:`alpha_agent.r33.universe.load_close` or
:func:`alpha_agent.r34.universe.load_total_return`, which are the estate's two
vendor readers.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .. import r36
from ..r33 import universe as _r33_universe
from ..r34 import universe as _r34_universe
from ..r35 import information as _r35_information
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r36.native_markets"
COVERAGE_SCHEMA = "r36_native_market_coverage/1"
ARTIFACT_NAME = "native_market_registry.json"

DAYS_PER_YEAR = 365.0
MIN_REGRESSION_OBSERVATIONS = 36


# --------------------------------------------------------------------------- #
# Vendor and payload readers - each delegating to the estate's owner
# --------------------------------------------------------------------------- #
def owned_close(symbol: str) -> Optional[pd.Series]:
    """An owned index, yield or spot series, unadjusted (R33's reader)."""
    return _r33_universe.load_close(symbol)


def owned_total_return(symbol: str) -> Optional[pd.Series]:
    """An owned listed product on dividend-reinvested closes (R34's reader)."""
    frame = _r34_universe.load_total_return(symbol)
    if frame is None or len(frame) == 0 or "Close" not in frame:
        return None
    series = frame["Close"].astype(float)
    series = series[np.isfinite(series.values) & (series.values > 0.0)]
    if series.empty:
        return None
    series.index = pd.to_datetime(series.index).tz_localize(None).normalize()
    return series[~series.index.duplicated(keep="last")].sort_index()


def read_fred(files: dict) -> dict:
    """FRED payloads with Release 36's monthly set and per-series lags.

    Delegates to the released Release-35 loader rather than reimplementing the
    parse. The monthly identifiers differ because the release does - Release 35
    read seven short rates, this one reads twenty short rates and twenty-one
    consumer price indices - and a monthly series treated as daily would be a
    two-month look-ahead repeated three hundred times.
    """
    monthly, lags = set(), {}
    for _code, spec in _contract.FX_UNIVERSE.items():
        monthly.add(spec[2])
        monthly.add(spec[3])
        lags[spec[2]] = _contract.OECD_RATE_PUBLICATION_LAG_MONTHS
        lags[spec[3]] = (
            _contract.QUARTERLY_CPI_PUBLICATION_LAG_MONTHS
            if spec[3] in _contract.FX_QUARTERLY_CPI
            else _contract.CPI_PUBLICATION_LAG_MONTHS)
    monthly.add(_contract.FX_BASE_SHORT_RATE)
    monthly.add(_contract.FX_BASE_CPI)
    lags[_contract.FX_BASE_SHORT_RATE] = \
        _contract.OECD_RATE_PUBLICATION_LAG_MONTHS
    lags[_contract.FX_BASE_CPI] = _contract.CPI_PUBLICATION_LAG_MONTHS
    return _r35_information.load_fred(files, monthly_ids=monthly,
                                      lag_months=lags)


def read_commodity_curves(petroleum_path, natural_gas_path) -> dict:
    """Every declared commodity curve, one dated-contract frame per market."""
    out = {}
    for market, (series_ids, group, terminated) in sorted(
            _contract.COMMODITY_CURVES.items()):
        path = natural_gas_path if series_ids[0].startswith("NG.") \
            else petroleum_path
        if path is None:
            out[market] = {"ok": False, "reason": "ARCHIVE_ABSENT"}
            continue
        loaded = _r35_information.load_eia_curve(
            path, series_ids=series_ids,
            cache_name="r36_curve_%s.csv" % market.lower())
        loaded["market"] = market
        loaded["economic_group"] = group
        loaded["terminated"] = bool(terminated)
        out[market] = loaded
    return out


# --------------------------------------------------------------------------- #
# Decision dates and period returns
# --------------------------------------------------------------------------- #
def decision_dates(calendar: pd.DatetimeIndex, *, cadence: int
                   ) -> pd.DatetimeIndex:
    """Non-overlapping decision dates struck every ``cadence`` sessions.

    Struck by session count rather than by calendar month so that successive
    observations of a ``cadence``-session return DO NOT OVERLAP. Overlapping
    observations inflate the effective sample by a factor of the cadence and
    make every t-statistic a fiction.
    """
    idx = pd.DatetimeIndex(calendar).sort_values()
    if len(idx) == 0:
        return idx
    lag = int(_contract.IMPLEMENTATION_LAG_SESSIONS)
    positions = list(range(0, len(idx) - lag, int(cadence)))
    return pd.DatetimeIndex([idx[p] for p in positions])


def period_returns(prices: pd.DataFrame, dates: pd.DatetimeIndex, *,
                   calendar: pd.DatetimeIndex) -> pd.DataFrame:
    """Simple returns earned between consecutive decisions, entered with lag.

    A position decided on the information of session ``t`` is entered at the
    close of session ``t + IMPLEMENTATION_LAG_SESSIONS`` and exited at the close
    of the next decision date plus the same lag. The return is stamped on ``t``,
    the date whose information produced it, so a weight matrix and a return
    matrix multiply element-wise with no further alignment.
    """
    lag = int(_contract.IMPLEMENTATION_LAG_SESSIONS)
    cal = pd.DatetimeIndex(calendar)
    aligned = prices.reindex(cal).ffill(limit=5)
    position = {d: i for i, d in enumerate(cal)}
    rows, index = [], []
    dates = pd.DatetimeIndex(dates)
    for k in range(len(dates) - 1):
        entry = position.get(dates[k])
        exit_ = position.get(dates[k + 1])
        if entry is None or exit_ is None:
            continue
        entry += lag
        exit_ += lag
        if exit_ >= len(cal) or entry >= len(cal) or exit_ <= entry:
            continue
        p0 = aligned.iloc[entry]
        p1 = aligned.iloc[exit_]
        rows.append((p1 / p0 - 1.0).where(p0 > 0))
        index.append(dates[k])
    if not rows:
        return pd.DataFrame(columns=prices.columns)
    return pd.DataFrame(rows, index=pd.DatetimeIndex(index))


def period_days(dates: pd.DatetimeIndex) -> pd.Series:
    """Calendar days each decision period spans, for accrual legs."""
    dates = pd.DatetimeIndex(dates)
    if len(dates) < 2:
        return pd.Series(dtype=float)
    spans = (dates[1:] - dates[:-1]).days.astype(float)
    return pd.Series(spans, index=dates[:-1])


def cash_leg(rate_percent: pd.Series, dates: pd.DatetimeIndex,
             calendar: pd.DatetimeIndex) -> pd.Series:
    """Risk-free return over each decision period from an observable bill rate."""
    observable = _r35_information.as_of_align(rate_percent, calendar)
    days = period_days(dates)
    level = observable.reindex(days.index).ffill()
    return (level / 100.0 * days / DAYS_PER_YEAR).fillna(0.0)


def trailing_slope(y: pd.Series, x: pd.Series, *,
                   minimum: int = MIN_REGRESSION_OBSERVATIONS) -> pd.Series:
    """Expanding-window OLS slope of ``y`` on ``x``, using only past rows.

    The value stamped on date ``t`` is estimated from observations STRICTLY
    BEFORE ``t``. That is what makes a measured duration or hedge ratio
    admissible: a full-sample regression would hand every date a coefficient
    fitted partly on its own future.
    """
    frame = pd.DataFrame({"y": y, "x": x}).dropna()
    out = pd.Series(np.nan, index=y.index, dtype=float)
    if frame.empty:
        return out
    xs = frame["x"].to_numpy()
    ys = frame["y"].to_numpy()
    idx = frame.index
    for k in range(len(idx)):
        if k < minimum:
            continue
        a = xs[:k]
        b = ys[:k]
        var = float(np.var(a, ddof=1))
        if not math.isfinite(var) or var <= 0:
            continue
        cov = float(np.cov(a, b, ddof=1)[0, 1])
        out.loc[idx[k]] = cov / var
    return out


# --------------------------------------------------------------------------- #
# Positioning (CFTC), shared by the FX and commodity lanes
# --------------------------------------------------------------------------- #
def trim_to_control(panel: dict) -> dict:
    """Drop decisions on which the lane's own control is not observable.

    A control that is not observable is not a control. The cross-asset lane
    makes the point: its 60/40 benchmark needs a Treasury total-return index
    that begins in 2005, and filling the missing leg with zero would silently
    turn fifteen years of "60/40" into sixty per cent equity - a benchmark
    nobody held, quietly easier or harder to beat than the real one depending
    on the decade.

    Trimming is done on the LANE, not on a strategy, so no configuration can
    acquire a window that flatters it. Inside the surviving window a rule that
    sits in cash is making a choice; outside it, the comparison did not exist.
    """
    excess = panel.get("excess")
    if excess is None or excess.empty:
        return panel
    control = pd.Series(panel.get("control_excess")).reindex(excess.index)
    usable = control.notna() & excess.notna().any(axis=1)
    if not bool(usable.any()):
        return dict(panel, ok=False, reason="CONTROL_NEVER_OBSERVABLE")
    first = usable.idxmax()
    last = usable[::-1].idxmax()
    keep = excess.index[(excess.index >= first) & (excess.index <= last)]
    if len(keep) == len(excess.index):
        return panel

    trimmed = dict(panel)
    trimmed["excess"] = excess.reindex(keep)
    trimmed["dates"] = keep
    trimmed["control_excess"] = control.reindex(keep)
    if panel.get("cash") is not None:
        trimmed["cash"] = pd.Series(panel["cash"]).reindex(keep).fillna(0.0)
    signals = {}
    for name, value in (panel.get("signals") or {}).items():
        signals[name] = (value.reindex(keep)
                         if isinstance(value, (pd.DataFrame, pd.Series))
                         else value)
    trimmed["signals"] = signals
    coverage = dict(panel.get("coverage") or {})
    coverage.update({"decisions": int(len(keep)),
                     "first": str(keep.min())[:10],
                     "last": str(keep.max())[:10],
                     "decisions_before_trim": int(len(excess.index)),
                     "trimmed_to_control_window": True})
    trimmed["coverage"] = coverage
    return trimmed


def positioning_frame(cot_frame: pd.DataFrame, dates: pd.DatetimeIndex,
                      calendar: pd.DatetimeIndex, *, keys) -> pd.DataFrame:
    """Speculative net position as a share of open interest, per instrument."""
    columns = {}
    for key in keys:
        codes = _contract.CFTC_CODES.get(key)
        if not codes:
            continue
        series = _r35_information.cot_instrument_series(
            cot_frame, codes, lag_days=_contract.COT_PUBLICATION_LAG_DAYS)
        if series.empty:
            continue
        columns[key] = _r35_information.as_of_align(
            series["spec_net_oi"], calendar).reindex(dates)
    if not columns:
        return pd.DataFrame(index=dates)
    return pd.DataFrame(columns, index=dates)


# --------------------------------------------------------------------------- #
# Lane: FX
# --------------------------------------------------------------------------- #
def build_fx(fred: dict, *, cot_frame=None) -> dict:
    """Currency excess returns, carry, trend and real-rate value.

    Admissibility is Release 33's measured rule, reused rather than re-invented:
    a currency whose spot repeats its previous close more than
    ``MAX_ZERO_RETURN_FRACTION`` of sessions is ADMINISTERED, and one below
    ``MIN_ANNUAL_VOLATILITY`` is PEGGED. Excluding a hard peg costs this lane
    its most flattering positions - a peg pays carry with no realised volatility
    right up until it does not - which is the reason to exclude it.
    """
    series = fred.get("series") or {}
    base_rate = series.get(_contract.FX_BASE_SHORT_RATE)
    if base_rate is None:
        return {"lane": _contract.LANE_FX, "ok": False,
                "reason": "US_SHORT_RATE_ABSENT"}
    base_cpi = series.get(_contract.FX_BASE_CPI)

    spots, rates, cpis, meta, admission = {}, {}, {}, {}, []
    for code, spec in sorted(_contract.FX_UNIVERSE.items()):
        group, tier, rate_id, cpi_id = spec
        resolved = _r33_universe.resolve_fx(code)
        spot = resolved.get("series")
        if spot is None or spot.size < 500:
            admission.append({"code": code, "state": "EXCLUDED_NOT_DELIVERED"})
            continue
        diagnostics = _r33_universe.series_diagnostics(spot)
        if diagnostics["zero_return_fraction"] > _contract.MAX_ZERO_RETURN_FRACTION:
            admission.append({"code": code,
                              "state": "EXCLUDED_ADMINISTERED",
                              "zero_return_fraction":
                                  diagnostics["zero_return_fraction"]})
            continue
        if diagnostics["annual_volatility"] < _contract.MIN_ANNUAL_VOLATILITY:
            admission.append({"code": code, "state": "EXCLUDED_PEGGED",
                              "annual_volatility":
                                  diagnostics["annual_volatility"]})
            continue
        rate = series.get(rate_id)
        if rate is None:
            admission.append({"code": code,
                              "state": "EXCLUDED_NO_SHORT_RATE_SERIES"})
            continue
        spots[code] = spot
        rates[code] = rate
        if cpi_id in series:
            cpis[code] = series[cpi_id]
        meta[code] = {"asset_class": "FX", "economic_group": group,
                      "cost_tier": tier,
                      "cost_bps_per_side": _contract.COST_BPS_PER_SIDE[tier],
                      "source_symbol": resolved.get("source_symbol"),
                      "short_rate_series": rate_id,
                      "cpi_series": cpi_id if cpi_id in series else None}
        admission.append({"code": code, "state": "ADMITTED",
                          "zero_return_fraction":
                              diagnostics["zero_return_fraction"],
                          "annual_volatility":
                              diagnostics["annual_volatility"]})
    if len(spots) < _contract.MIN_CROSS_SECTION:
        return {"lane": _contract.LANE_FX, "ok": False,
                "reason": "TOO_FEW_ADMITTED_CURRENCIES",
                "admission": admission}

    spot_frame = pd.DataFrame(spots).sort_index()
    calendar = spot_frame.dropna(how="all").index
    cadence = _contract.LANE_CADENCE[_contract.LANE_FX]
    dates = decision_dates(calendar, cadence=cadence)

    spot_returns = period_returns(spot_frame, dates, calendar=calendar)
    days = period_days(dates).reindex(spot_returns.index)

    base_observable = _r35_information.as_of_align(base_rate, calendar)
    carry = pd.DataFrame(index=spot_returns.index, columns=spot_frame.columns,
                         dtype=float)
    for code in spot_frame.columns:
        foreign = _r35_information.as_of_align(rates[code], calendar)
        differential = (foreign - base_observable).reindex(spot_returns.index)
        carry[code] = differential / 100.0

    accrual = carry.mul(days / DAYS_PER_YEAR, axis=0)
    excess = spot_returns + accrual
    # A currency is in the cross-section on a date only when BOTH legs of its
    # return were observable. A missing rate is a missing instrument, not a
    # zero: a spot-only return would be a different asset wearing the same name.
    excess = excess.where(carry.notna() & spot_returns.notna())

    # Real exchange rate: spot deflated by the price ratio. Currencies without
    # an admissible price index simply have no value score - a structural
    # absence, never a cross-sectional fill.
    real_rate = pd.DataFrame(index=spot_returns.index,
                             columns=spot_frame.columns, dtype=float)
    if base_cpi is not None:
        base_prices = _r35_information.as_of_align(base_cpi, calendar)
        for code in spot_frame.columns:
            if code not in cpis:
                continue
            foreign_prices = _r35_information.as_of_align(cpis[code], calendar)
            spot_level = spot_frame[code].reindex(calendar).ffill(limit=5)
            level = (np.log(spot_level) + np.log(foreign_prices)
                     - np.log(base_prices))
            real_rate[code] = level.reindex(spot_returns.index)

    control = excess.mean(axis=1, skipna=True)
    cash = cash_leg(series.get(_contract.CASH_YIELD_SERIES,
                               pd.Series(dtype=float)),
                    dates, calendar).reindex(excess.index).fillna(0.0)

    signals = {"carry": carry, "real_rate": real_rate,
               "spot_return": spot_returns}
    if cot_frame is not None:
        signals["positioning"] = positioning_frame(
            cot_frame, excess.index, calendar,
            keys=list(spot_frame.columns))
    return {
        "lane": _contract.LANE_FX, "ok": True,
        "dates": excess.index, "excess": excess, "signals": signals,
        "meta": meta, "control_excess": control, "cash": cash,
        "cadence": cadence,
        "instruments": list(spot_frame.columns),
        "admission": admission,
        "implementation_level": _contract.LEVEL_NATIVE,
        "implementation_note": (
            "a deliverable one-month forward; the excess return is spot plus "
            "the interest differential, which is what the forward earns under "
            "covered interest parity"),
        "coverage": {"instruments": len(spot_frame.columns),
                     "decisions": int(len(excess)),
                     "first": str(excess.index.min())[:10] if len(excess)
                     else None,
                     "last": str(excess.index.max())[:10] if len(excess)
                     else None},
    }


# --------------------------------------------------------------------------- #
# Lane: commodity curve
# --------------------------------------------------------------------------- #
def build_commodity(curves: dict, fred: Optional[dict] = None, *,
                    cot_frame=None) -> dict:
    """One-month held-contract returns and the curve slope that predicts them."""
    frames = {m: c["frame"] for m, c in curves.items()
              if c.get("ok") and c.get("frame") is not None}
    if len(frames) < 2:
        return {"lane": _contract.LANE_COMMODITY, "ok": False,
                "reason": "TOO_FEW_CURVES", "markets": sorted(frames)}

    union = None
    for frame in frames.values():
        union = frame.index if union is None else union.union(frame.index)
    calendar = pd.DatetimeIndex(sorted(union))
    cadence = _contract.LANE_CADENCE[_contract.LANE_COMMODITY]
    dates = decision_dates(calendar, cadence=cadence)
    lag = int(_contract.EIA_SETTLEMENT_LAG_SESSIONS)
    position = {d: i for i, d in enumerate(calendar)}

    excess_cols, carry_cols, curvature_cols, spread_cols = {}, {}, {}, {}
    meta = {}
    for market, frame in sorted(frames.items()):
        ids = _contract.COMMODITY_CURVES[market][0]
        group = _contract.COMMODITY_CURVES[market][1]
        aligned = frame.reindex(calendar).ffill(limit=5)
        c1, c2, c3 = aligned[ids[0]], aligned[ids[1]], aligned[ids[2]]
        held, spread, basis, curvature = [], [], [], []
        index = []
        for k in range(len(dates) - 1):
            entry = position.get(dates[k])
            exit_ = position.get(dates[k + 1])
            if entry is None or exit_ is None:
                continue
            entry += lag
            exit_ += lag
            if exit_ >= len(calendar) or entry >= len(calendar):
                continue
            p2 = c2.iloc[entry]
            p3 = c3.iloc[entry]
            q1 = c1.iloc[exit_]
            q2 = c2.iloc[exit_]
            # The second-nearest contract bought at entry IS the nearest
            # contract at exit: one month of holding, no roll to infer.
            held.append(q1 / p2 - 1.0 if (p2 and p2 > 0 and q1 > 0)
                        else np.nan)
            spread.append(((q1 / p2) - (q2 / p3)) if all(
                v and v > 0 for v in (p2, p3, q1, q2)) else np.nan)
            e1, e2 = c1.iloc[entry], c2.iloc[entry]
            e3 = c3.iloc[entry]
            basis.append(math.log(e1 / e2) if (e1 > 0 and e2 > 0) else np.nan)
            curvature.append(
                math.log(e1) - 2.0 * math.log(e2) + math.log(e3)
                if all(v > 0 for v in (e1, e2, e3)) else np.nan)
            index.append(dates[k])
        if not index:
            continue
        idx = pd.DatetimeIndex(index)
        excess_cols[market] = pd.Series(held, index=idx, dtype=float)
        spread_cols[market] = pd.Series(spread, index=idx, dtype=float)
        carry_cols[market] = pd.Series(basis, index=idx, dtype=float)
        curvature_cols[market] = pd.Series(curvature, index=idx, dtype=float)
        meta[market] = {
            "asset_class": "COMMODITY", "economic_group": group,
            "cost_tier": "ENERGY_FUTURE",
            "cost_bps_per_side": _contract.COST_BPS_PER_SIDE["ENERGY_FUTURE"],
            "contracts": list(ids),
            "terminated": bool(_contract.COMMODITY_CURVES[market][2])}
    if len(excess_cols) < 2:
        return {"lane": _contract.LANE_COMMODITY, "ok": False,
                "reason": "NO_USABLE_CURVE_RETURNS"}

    excess = pd.DataFrame(excess_cols).sort_index()
    carry = pd.DataFrame(carry_cols).reindex(excess.index)
    curvature = pd.DataFrame(curvature_cols).reindex(excess.index)
    spreads = pd.DataFrame(spread_cols).reindex(excess.index)

    signals = {"carry": carry, "curvature": curvature,
               "spread_return": spreads}
    if cot_frame is not None:
        signals["positioning"] = positioning_frame(
            cot_frame, excess.index, calendar, keys=list(excess.columns))
    control = excess.mean(axis=1, skipna=True)
    # A futures position is fully collateralised, so its total return is the
    # bill rate plus the contract's excess return. Carrying the real cash leg
    # keeps this lane's reported net return comparable with every other lane's.
    cash = cash_leg((fred or {}).get("series", {}).get(
        _contract.CASH_YIELD_SERIES, pd.Series(dtype=float)),
        dates, calendar).reindex(excess.index).fillna(0.0)
    return {
        "lane": _contract.LANE_COMMODITY, "ok": True,
        "dates": excess.index, "excess": excess, "signals": signals,
        "meta": meta, "control_excess": control, "cash": cash,
        "cadence": cadence, "instruments": list(excess.columns),
        "implementation_level": _contract.LEVEL_NATIVE,
        "implementation_note": (
            "a dated NYMEX contract held one month; the return is the "
            "second-nearest contract becoming the nearest, and the fully "
            "collateralised excess return over cash IS the futures return"),
        "terminated_markets": sorted(
            m for m, spec in meta.items() if spec.get("terminated")),
        "coverage": {"instruments": len(excess.columns),
                     "decisions": int(len(excess)),
                     "first": str(excess.index.min())[:10],
                     "last": str(excess.index.max())[:10],
                     "per_market": {m: int(excess[m].notna().sum())
                                    for m in excess.columns}},
    }


# --------------------------------------------------------------------------- #
# Lane: rates curve
# --------------------------------------------------------------------------- #
def build_rates(fred: dict) -> dict:
    """Duration-bucket Treasury returns, measured durations and curve signals."""
    prices, yields, meta = {}, {}, {}
    for bucket, (index_symbol, yield_symbol) in sorted(
            _contract.RATES_LEGS.items()):
        series = owned_close(index_symbol)
        if series is None or series.size < 500:
            continue
        prices[bucket] = series
        meta[bucket] = {
            "asset_class": "GOVERNMENT_BOND", "economic_group": bucket,
            "cost_tier": "TREASURY_INDEX",
            "cost_bps_per_side": _contract.COST_BPS_PER_SIDE["TREASURY_INDEX"],
            "index_symbol": index_symbol, "yield_symbol": yield_symbol}
    if len(prices) < 3:
        return {"lane": _contract.LANE_RATES, "ok": False,
                "reason": "TOO_FEW_TREASURY_LEGS"}
    for point in _contract.RATES_CURVE_POINTS:
        series = owned_close(point)
        if series is not None:
            yields[point] = series

    price_frame = pd.DataFrame(prices).sort_index()
    calendar = price_frame.dropna(how="all").index
    cadence = _contract.LANE_CADENCE[_contract.LANE_RATES]
    dates = decision_dates(calendar, cadence=cadence)
    total_returns = period_returns(price_frame, dates, calendar=calendar)

    curve = pd.DataFrame(
        {point: _r35_information.as_of_align(series, calendar)
         for point, series in yields.items()}).reindex(total_returns.index)

    cash = cash_leg((fred.get("series") or {}).get(
        _contract.CASH_YIELD_SERIES, pd.Series(dtype=float)),
        dates, calendar).reindex(total_returns.index).fillna(0.0)
    excess = total_returns.sub(cash, axis=0)

    # Duration is MEASURED, on trailing rows only, per bucket.
    durations = pd.DataFrame(index=excess.index, columns=excess.columns,
                             dtype=float)
    for bucket in excess.columns:
        point = _contract.RATES_LEGS[bucket][1]
        if point not in curve:
            continue
        change = curve[point].diff()
        slope = trailing_slope(excess[bucket], change)
        durations[bucket] = -slope * 100.0

    control_leg = None
    for bucket, spec in _contract.RATES_LEGS.items():
        if spec[0] == _contract.RATES_CONTROL_LEG and bucket in excess:
            control_leg = bucket
    control = excess[control_leg] if control_leg else excess.mean(axis=1)

    signals = {"curve": curve, "duration": durations,
               "slope_10y_2y": (curve.get("%10YTCM", pd.Series(dtype=float))
                                - curve.get("%2YTCM", pd.Series(dtype=float))),
               "butterfly_2_5_10": (
                   2.0 * curve.get("%5YTCM", pd.Series(dtype=float))
                   - curve.get("%2YTCM", pd.Series(dtype=float))
                   - curve.get("%10YTCM", pd.Series(dtype=float)))}

    breakeven = {}
    for symbol in _contract.RATES_BREAKEVEN_LEGS:
        series = owned_total_return(symbol)
        if series is not None:
            breakeven[symbol] = series
    if len(breakeven) == 2:
        be_frame = pd.DataFrame(breakeven).sort_index()
        be_returns = period_returns(be_frame, dates, calendar=calendar)
        signals["breakeven_returns"] = be_returns.reindex(excess.index)
        for sid in _contract.RATES_BREAKEVEN_SIGNAL:
            line = (fred.get("series") or {}).get(sid)
            if line is not None:
                signals["be_%s" % sid] = _r35_information.as_of_align(
                    line, calendar).reindex(excess.index)

    return {
        "lane": _contract.LANE_RATES, "ok": True,
        "dates": excess.index, "excess": excess, "signals": signals,
        "meta": meta, "control_excess": control, "cash": cash,
        "cadence": cadence, "instruments": list(excess.columns),
        "implementation_level": _contract.LEVEL_PROXY,
        "implementation_note": (
            "the tradable legs are duration-bucket total-return indices, which "
            "a listed fund tracks; the NATIVE implementation is a Treasury "
            "future and the owned futures entitlement does not serve one"),
        "coverage": {"instruments": len(excess.columns),
                     "decisions": int(len(excess)),
                     "first": str(excess.index.min())[:10] if len(excess)
                     else None,
                     "last": str(excess.index.max())[:10] if len(excess)
                     else None},
    }


# --------------------------------------------------------------------------- #
# Lane: credit
# --------------------------------------------------------------------------- #
def build_credit(fred: dict) -> dict:
    """Duration-hedged credit excess return and the spread signals for it."""
    corporate = owned_close(_contract.CREDIT_LEG)
    if corporate is None:
        return {"lane": _contract.LANE_CREDIT, "ok": False,
                "reason": "CREDIT_INDEX_ABSENT"}
    hedges = {sym: owned_close(sym) for sym in _contract.CREDIT_HEDGE_LEGS}
    hedges = {k: v for k, v in hedges.items() if v is not None}
    if not hedges:
        return {"lane": _contract.LANE_CREDIT, "ok": False,
                "reason": "TREASURY_HEDGE_LEG_ABSENT"}

    frame = pd.DataFrame({"CREDIT": corporate, **hedges}).sort_index()
    calendar = frame.dropna(how="all").index
    cadence = _contract.LANE_CADENCE[_contract.LANE_CREDIT]
    dates = decision_dates(calendar, cadence=cadence)
    returns = period_returns(frame, dates, calendar=calendar)
    cash = cash_leg((fred.get("series") or {}).get(
        _contract.CASH_YIELD_SERIES, pd.Series(dtype=float)),
        dates, calendar).reindex(returns.index).fillna(0.0)

    hedge_symbol = sorted(hedges)[-1]
    beta = trailing_slope(returns["CREDIT"] - cash,
                          returns[hedge_symbol] - cash)
    hedged = ((returns["CREDIT"] - cash)
              - beta * (returns[hedge_symbol] - cash))
    excess = pd.DataFrame({"CREDIT_DURATION_HEDGED": hedged}).dropna()
    if excess.empty:
        return {"lane": _contract.LANE_CREDIT, "ok": False,
                "reason": "NO_HEDGED_CREDIT_OBSERVATIONS"}

    spreads = {}
    for sid in _contract.CREDIT_SPREAD_SIGNALS:
        line = owned_close(sid) if sid.startswith("%") else \
            (fred.get("series") or {}).get(sid)
        if line is not None:
            spreads[sid] = _r35_information.as_of_align(
                line, calendar).reindex(excess.index)

    meta = {"CREDIT_DURATION_HEDGED": {
        "asset_class": "CREDIT_BOND", "economic_group": "CREDIT_IG",
        "cost_tier": "CREDIT_INDEX",
        "cost_bps_per_side": _contract.COST_BPS_PER_SIDE["CREDIT_INDEX"],
        "index_symbol": _contract.CREDIT_LEG,
        "hedge_symbol": hedge_symbol}}
    control = excess["CREDIT_DURATION_HEDGED"]
    return {
        "lane": _contract.LANE_CREDIT, "ok": True,
        "dates": excess.index, "excess": excess,
        "signals": {"spreads": pd.DataFrame(spreads).reindex(excess.index),
                    "hedge_ratio": beta.reindex(excess.index)},
        "meta": meta, "control_excess": control,
        "cash": cash.reindex(excess.index).fillna(0.0),
        "cadence": cadence, "instruments": list(excess.columns),
        "implementation_level": _contract.LEVEL_PROXY,
        "implementation_note": (
            "a broad investment-grade index hedged with a Treasury index; the "
            "NATIVE market is individual corporate bonds and credit default "
            "swaps, for which no free point-in-time source exists"),
        "coverage": {"instruments": 1, "decisions": int(len(excess)),
                     "first": str(excess.index.min())[:10],
                     "last": str(excess.index.max())[:10]},
    }


# --------------------------------------------------------------------------- #
# Lane: volatility term structure
# --------------------------------------------------------------------------- #
def build_volatility(cboe: dict, fred: dict) -> dict:
    """The term-structure signal, and the one direction that is testable.

    The native instrument - a VIX future - is not entitled from any free route.
    The short-volatility direction is not testable either, and for a reason that
    has nothing to do with entitlement: the products that terminated are absent
    from the owned delisted database, so a short-volatility book assembled from
    what survives would be backfilled with exactly the instruments that did not
    blow up. The LONG direction uses a product that never terminated.
    """
    series = cboe.get("series") or {}
    front = series.get(_contract.VOL_INDEX_LEGS[0])
    back = series.get(_contract.VOL_INDEX_LEGS[1])
    if front is None or back is None:
        return {"lane": _contract.LANE_VOL, "ok": False,
                "reason": "VOLATILITY_INDEX_TERM_STRUCTURE_ABSENT"}
    tradable = owned_total_return(_contract.VOL_TRADABLE_LEG)
    equity = owned_total_return(_contract.VOL_EQUITY_LEG)
    if tradable is None or equity is None:
        return {"lane": _contract.LANE_VOL, "ok": False,
                "reason": "VOLATILITY_TRADABLE_LEG_ABSENT"}

    frame = pd.DataFrame({_contract.VOL_TRADABLE_LEG: tradable,
                          _contract.VOL_EQUITY_LEG: equity}).dropna()
    calendar = frame.index
    cadence = _contract.LANE_CADENCE[_contract.LANE_VOL]
    dates = decision_dates(calendar, cadence=cadence)
    returns = period_returns(frame, dates, calendar=calendar)
    cash = cash_leg((fred.get("series") or {}).get(
        _contract.CASH_YIELD_SERIES, pd.Series(dtype=float)),
        dates, calendar).reindex(returns.index).fillna(0.0)
    excess = returns.sub(cash, axis=0)

    front_aligned = _r35_information.as_of_align(front, calendar)
    back_aligned = _r35_information.as_of_align(back, calendar)
    slope = (front_aligned / back_aligned - 1.0).reindex(excess.index)

    meta = {}
    for symbol, tier in ((_contract.VOL_TRADABLE_LEG, "VOLATILITY_ETP"),
                         (_contract.VOL_EQUITY_LEG, "EQUITY_INDEX")):
        meta[symbol] = {
            "asset_class": "VOLATILITY" if tier == "VOLATILITY_ETP"
            else "EQUITY_INDEX",
            "economic_group": "VOLATILITY_TERM_STRUCTURE",
            "cost_tier": tier,
            "cost_bps_per_side": _contract.COST_BPS_PER_SIDE[tier]}
    return {
        "lane": _contract.LANE_VOL, "ok": True,
        "dates": excess.index, "excess": excess,
        "signals": {"term_slope": slope, "front_level": front_aligned.reindex(
            excess.index)},
        "meta": meta,
        "control_excess": excess[_contract.VOL_TRADABLE_LEG],
        "cash": cash.reindex(excess.index).fillna(0.0),
        "cadence": cadence, "instruments": list(excess.columns),
        "implementation_level": _contract.LEVEL_PROXY,
        "implementation_note": (
            "an exchange-traded product that holds the front two VIX futures; "
            "the NATIVE curve is not entitled and the short direction is "
            "survivorship-blocked"),
        "short_direction_blocked": _contract.SHORT_VOLATILITY_BLOCK_REASON,
        "coverage": {"instruments": len(excess.columns),
                     "decisions": int(len(excess)),
                     "first": str(excess.index.min())[:10] if len(excess)
                     else None,
                     "last": str(excess.index.max())[:10] if len(excess)
                     else None},
    }


# --------------------------------------------------------------------------- #
# Lane: cross-asset relative value
# --------------------------------------------------------------------------- #
def snap_to_calendar(dates, calendar: pd.DatetimeIndex) -> pd.DatetimeIndex:
    """Map each date to the last session of ``calendar`` at or before it.

    Two lanes built on two different exchange calendars have decision dates
    that drift apart within a few weeks. Snapping one lane's grid onto the
    other's sessions is what lets a book built in one lane be an INPUT to the
    other without pairing a January decision with a February return.
    """
    cal = pd.DatetimeIndex(calendar).sort_values()
    out = []
    for date in pd.DatetimeIndex(dates):
        position = cal.searchsorted(date, side="right") - 1
        if position >= 0:
            out.append(cal[position])
    return pd.DatetimeIndex(sorted(set(out)))


def build_cross_asset(fred: dict, *, align_dates=None, extra_excess=None,
                      extra_meta=None) -> dict:
    """Economically interpretable cross-asset legs, and nothing else.

    Five relationships, each with a stated economic mechanism. A grid of every
    ratio of every pair would be a thousand configurations and no hypothesis.

    ``extra_excess`` admits a book built in ANOTHER lane as a column - the
    currency carry book, for the one relationship that allocates between a
    currency strategy and equity. ``align_dates`` snaps this lane's decision
    grid onto that lane's, so the two return series describe the same periods.
    """
    legs = {}
    for name, symbol in sorted(_contract.CROSS_ASSET_LEGS.items()):
        series = owned_total_return(symbol) if symbol == "SPY" \
            else owned_close(symbol)
        if series is not None:
            legs[name] = series
    if len(legs) < 4:
        return {"lane": _contract.LANE_CROSS_ASSET, "ok": False,
                "reason": "TOO_FEW_CROSS_ASSET_LEGS", "legs": sorted(legs)}

    frame = pd.DataFrame(legs).sort_index()
    calendar = frame.dropna(how="all").index
    cadence = _contract.LANE_CADENCE[_contract.LANE_CROSS_ASSET]
    dates = (snap_to_calendar(align_dates, calendar)
             if align_dates is not None
             else decision_dates(calendar, cadence=cadence))
    returns = period_returns(frame, dates, calendar=calendar)
    cash = cash_leg((fred.get("series") or {}).get(
        _contract.CASH_YIELD_SERIES, pd.Series(dtype=float)),
        dates, calendar).reindex(returns.index).fillna(0.0)
    excess = returns.sub(cash, axis=0)

    signals = {}
    for sid in ("DFII10", "T10YIE"):
        line = (fred.get("series") or {}).get(sid)
        if line is not None:
            signals[sid] = _r35_information.as_of_align(
                line, calendar).reindex(excess.index)
    for point in ("%10YTCM", "%2YTCM"):
        line = owned_close(point)
        if line is not None:
            signals[point] = _r35_information.as_of_align(
                line, calendar).reindex(excess.index)
    copper_gold = legs.get("COPPER_GOLD")
    if copper_gold is not None:
        signals["copper_gold_level"] = _r35_information.as_of_align(
            copper_gold, calendar).reindex(excess.index)

    tiers = {"EQUITY": "EQUITY_INDEX", "TREASURY": "TREASURY_INDEX",
             "GOLD": "PRECIOUS_METAL", "CREDIT": "CREDIT_INDEX",
             "COPPER_GOLD": "PRECIOUS_METAL"}
    meta = {name: {"asset_class": name, "economic_group": "CROSS_ASSET",
                   "cost_tier": tiers.get(name, "EQUITY_INDEX"),
                   "cost_bps_per_side": _contract.COST_BPS_PER_SIDE[
                       tiers.get(name, "EQUITY_INDEX")],
                   "symbol": _contract.CROSS_ASSET_LEGS[name]}
            for name in excess.columns}
    for name, series in (extra_excess or {}).items():
        excess[name] = pd.Series(series).reindex(excess.index)
        meta[name] = dict((extra_meta or {}).get(name) or {
            "asset_class": name, "economic_group": "CROSS_ASSET_BOOK",
            "cost_tier": "FX_G10",
            "cost_bps_per_side": _contract.COST_BPS_PER_SIDE["FX_G10"]})
    # NOT ``fillna(0.0)``: a 60/40 benchmark whose bond leg is missing is not a
    # 60/40 benchmark, and letting the missing leg contribute zero would invent
    # a control nobody could have held. The NaN propagates and
    # ``trim_to_control`` removes those decisions from the lane.
    control = pd.Series(np.nan, index=excess.index)
    if "EQUITY" in excess and "TREASURY" in excess:
        control = 0.6 * excess["EQUITY"] + 0.4 * excess["TREASURY"]
    return {
        "lane": _contract.LANE_CROSS_ASSET, "ok": True,
        "dates": excess.index, "excess": excess, "signals": signals,
        "meta": meta, "control_excess": control, "cash": cash,
        "cadence": cadence, "instruments": list(excess.columns),
        "implementation_level": _contract.LEVEL_PROXY,
        "implementation_note": (
            "listed funds and index series; the relationships are "
            "economically interpretable and each is stated as a mechanism"),
        "coverage": {"instruments": len(excess.columns),
                     "decisions": int(len(excess)),
                     "first": str(excess.index.min())[:10] if len(excess)
                     else None,
                     "last": str(excess.index.max())[:10] if len(excess)
                     else None},
    }


# --------------------------------------------------------------------------- #
# Lane: crypto
# --------------------------------------------------------------------------- #
def build_crypto(fred: dict) -> dict:
    """Two assets, and an explicit refusal to invent a third.

    Bitcoin and Ether were the two largest crypto assets by market
    capitalisation continuously across the sample, so admitting exactly them is
    not a survivor selection. Any BROADER cross-section would be: a current list
    of surviving tokens cannot be written back into history, and no free
    point-in-time listing record exists for the venues that would have traded
    them.
    """
    series = fred.get("series") or {}
    legs = {sid: series[sid] for sid in _contract.CRYPTO_LEGS
            if sid in series}
    if len(legs) < 2:
        return {"lane": _contract.LANE_CRYPTO, "ok": False,
                "reason": "CRYPTO_PRICE_SERIES_ABSENT", "legs": sorted(legs)}
    frame = pd.DataFrame(legs).sort_index()
    # Crypto trades every day, and the St. Louis Fed republishes every day. The
    # calendar is nevertheless restricted to BUSINESS days, because every
    # annualising statistic in the estate counts 252 sessions a year: a
    # five-CALENDAR-day cadence on a seven-day series is 73 periods a year
    # being annualised as 50, which would overstate every return by half.
    # Friday-to-Friday still contains the weekend move.
    frame = frame[frame.index.dayofweek < 5]
    calendar = frame.dropna(how="all").index
    cadence = _contract.LANE_CADENCE[_contract.LANE_CRYPTO]
    dates = decision_dates(calendar, cadence=cadence)
    returns = period_returns(frame, dates, calendar=calendar)
    cash = cash_leg(series.get(_contract.CASH_YIELD_SERIES,
                               pd.Series(dtype=float)),
                    dates, calendar).reindex(returns.index).fillna(0.0)
    excess = returns.sub(cash, axis=0)
    meta = {sid: {"asset_class": "CRYPTO", "economic_group": "CRYPTO_MAJOR",
                  "cost_tier": "CRYPTO",
                  "cost_bps_per_side": _contract.COST_BPS_PER_SIDE["CRYPTO"]}
            for sid in excess.columns}
    return {
        "lane": _contract.LANE_CRYPTO, "ok": True,
        "dates": excess.index, "excess": excess, "signals": {},
        "meta": meta, "control_excess": excess.mean(axis=1, skipna=True),
        "cash": cash, "cadence": cadence,
        "instruments": list(excess.columns),
        "implementation_level": _contract.LEVEL_PROXY,
        "implementation_note": (
            "spot prices republished by the St. Louis Fed from Coinbase; the "
            "NATIVE instruments - regulated futures, perpetual futures and "
            "their funding - have no free point-in-time history"),
        "broad_universe_refused":
            _contract.CRYPTO_BROAD_UNIVERSE_BLOCK_REASON,
        "coverage": {"instruments": len(excess.columns),
                     "decisions": int(len(excess)),
                     "first": str(excess.index.min())[:10] if len(excess)
                     else None,
                     "last": str(excess.index.max())[:10] if len(excess)
                     else None},
    }


# --------------------------------------------------------------------------- #
# Registry artifact
# --------------------------------------------------------------------------- #
def registry_artifact(panels: dict, *, campaign_id: str, created_at: str
                      ) -> dict:
    rows = {}
    for lane, panel in sorted(panels.items()):
        rows[lane] = {
            "ok": bool(panel.get("ok")),
            "reason": panel.get("reason"),
            "implementation_level": panel.get("implementation_level"),
            "implementation_note": panel.get("implementation_note"),
            "cadence_sessions": panel.get("cadence"),
            "cadence_reason": _contract.LANE_CADENCE_REASON.get(lane),
            "control": _contract.LANE_CONTROL.get(lane),
            "instruments": panel.get("instruments"),
            "coverage": panel.get("coverage"),
            "terminated_markets": panel.get("terminated_markets"),
            "admission": panel.get("admission"),
        }
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "lanes": rows,
        "lanes_built": sorted(l for l, r in rows.items() if r["ok"]),
        "lanes_unavailable": sorted(l for l, r in rows.items() if not r["ok"]),
        "vendor_readers": ["alpha_agent.r33.universe.load_close",
                           "alpha_agent.r34.universe.load_total_return"],
        "alignment_owner": "alpha_agent.r35.information.as_of_align",
        "prohibited_substitutions": list(_contract.PROHIBITED_SUBSTITUTIONS),
    }
    return r36.artifact_body(COVERAGE_SCHEMA, payload)


def path_for(campaign_id: str = _contract.CAMPAIGN_ID) -> Path:
    return r36.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> Path:
    return r36.write_json(path_for(body["campaign_id"]), body)


__all__ = ["CALCULATION_OWNER", "owned_close", "owned_total_return",
           "read_fred", "read_commodity_curves", "decision_dates",
           "period_returns", "period_days", "cash_leg", "trailing_slope",
           "snap_to_calendar", "trim_to_control",
           "positioning_frame", "build_fx", "build_commodity", "build_rates",
           "build_credit", "build_volatility", "build_cross_asset",
           "build_crypto", "registry_artifact", "freeze", "path_for"]
