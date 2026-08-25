"""alpha_agent.r44.intraday - ENGINE 1B, event time.

Release 43 measured a real reaction to scheduled macro releases and could not
trade it: on DAILY bars the transaction cost of the position exceeded the
measured effect. R43 recorded that honestly and named the missing ingredient
- native intraday history - as PAYMENT_REQUIRED for futures.

This module asks the question the daily result could not answer, using bytes
the estate already owns:

    the dislocation is measured in MINUTES. Does it survive its own cost
    when it is entered and exited in minutes rather than held for a day?

What is used, and what is deliberately not:

  * Dukascopy 1-minute bars for EURUSD, USDJPY and XAUUSD, 2012-2026, each
    row carrying the broker's OBSERVED bid/ask spread. These are real OTC
    instruments quoted by a real venue.
  * The Dukascopy index, Bund and WTI symbols are CFDs. The contract forbids
    a CFD standing in for a futures hypothesis, so they are excluded and the
    exclusion is reported rather than quietly skipped.
  * The FRED release CALENDAR is point-in-time: these are the dates the
    releases were SCHEDULED for, published in advance.
  * The release TIME is a declared constant (08:30 or 09:15 ET), not data.
    It is stated in the contract so a reader can check it, and the result is
    reported against a +-15 minute timing robustness sweep.

Nothing here fabricates a fill. Entry is at the CLOSE of a bar at least one
full minute after the release stamp - the estate has no fill at the print -
and both sides pay the observed half-spread.
"""
from __future__ import annotations

import gzip
import os
from pathlib import Path

import numpy as np
import pandas as pd

from ..r41 import evidence as EV
from ..r43 import acquisition as AQ
from ..r43 import judge as J
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r44.intraday"

DUKASCOPY_ROOT = Path(
    r"D:\Stock_Prediction_app_data\multi_horizon_alpha_r41\_data_dukascopy")
ET = "America/New_York"

#: The rules, both directions, declared before the first bar is read.
EVENT_RULES = ("REVERSAL", "CONTINUATION")

_BAR_CACHE: dict = {}


# --------------------------------------------------------------------------- #
# Bars
# --------------------------------------------------------------------------- #
def available_instruments() -> list:
    if not DUKASCOPY_ROOT.is_dir():
        return []
    return [s for s in C.INTRADAY_INSTRUMENTS
            if (DUKASCOPY_ROOT / s).is_dir()]


#: US macro prints land at 08:30 ET (13:30 UTC in winter, 12:30 in summer)
#: and 09:15 ET. Keeping 11:00-17:00 UTC covers every declared stamp under
#: both daylight regimes plus the longest declared hold, and it is the
#: difference between a 500MB frame and a 130MB one. It is a LOADING filter
#: only - no bar inside the window is dropped, resampled or interpolated.
KEEP_HOURS_UTC = (11, 17)


def load_bars(symbol: str, months: list = None,
              keep_hours=KEEP_HOURS_UTC) -> pd.DataFrame:
    """1-minute bars with the observed spread, indexed in UTC."""
    key = (symbol, tuple(months) if months else None, tuple(keep_hours or ()))
    if key in _BAR_CACHE:
        return _BAR_CACHE[key]
    d = DUKASCOPY_ROOT / symbol
    if not d.is_dir():
        return None
    files = sorted(os.listdir(d))
    if months:
        want = set(months)
        files = [f for f in files if f.split("_")[1] in want]
    frames = []
    for f in files:
        p = d / f
        try:
            with gzip.open(p, "rt") as fh:
                m = pd.read_csv(fh, usecols=["ts_utc", "close", "spread"])
        except Exception:                                 # pragma: no cover
            continue
        m["ts_utc"] = pd.to_datetime(m["ts_utc"], utc=True)
        if keep_hours:
            h = m["ts_utc"].dt.hour
            m = m[(h >= keep_hours[0]) & (h < keep_hours[1])]
        if len(m):
            frames.append(m)
    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True)
    df = df.dropna(subset=["close"]).sort_values("ts_utc")
    df = df.drop_duplicates(subset=["ts_utc"], keep="last")
    df = df.set_index("ts_utc")
    _BAR_CACHE[key] = df
    return df


def coverage(symbol: str) -> dict:
    df = load_bars(symbol)
    if df is None or df.empty:
        return {"symbol": symbol, "state": "HISTORICAL_DATA_UNAVAILABLE"}
    sp = pd.to_numeric(df["spread"], errors="coerce")
    px = pd.to_numeric(df["close"], errors="coerce")
    half_bps = (sp / 2.0) / px * 1e4
    return {
        "symbol": symbol, "state": "AVAILABLE",
        "n_bars": int(len(df)),
        "first": str(df.index[0]), "last": str(df.index[-1]),
        "median_half_spread_bps": float(np.nanmedian(half_bps)),
        "p90_half_spread_bps": float(np.nanpercentile(half_bps, 90)),
        "spread_is_observed": True,
    }


# --------------------------------------------------------------------------- #
# Event stamps
# --------------------------------------------------------------------------- #
def release_stamps(min_year: int = 2012) -> pd.DataFrame:
    """Scheduled release dates x the declared release time, in UTC."""
    cal = AQ.load_release_calendar()
    if cal is None or getattr(cal, "empty", True):
        return None
    cal = cal.copy()
    cal["date"] = pd.to_datetime(cal["date"])
    cal = cal[cal["date"].dt.year >= min_year]
    rows = []
    for _, r in cal.iterrows():
        t = C.MACRO_RELEASE_TIMES_ET.get(str(r["event"]))
        if t is None:
            continue
        hh, mm = (int(x) for x in t.split(":"))
        local = pd.Timestamp(r["date"]).tz_localize(ET) \
            + pd.Timedelta(hours=hh, minutes=mm)
        rows.append({"event": r["event"], "date": r["date"],
                     "stamp_utc": local.tz_convert("UTC"),
                     "declared_time_et": t})
    if not rows:
        return None
    return pd.DataFrame(rows).sort_values("stamp_utc").reset_index(drop=True)


# --------------------------------------------------------------------------- #
# The event study
# --------------------------------------------------------------------------- #
def _bar_at(df: pd.DataFrame, ts: pd.Timestamp, *, tol_min: int = 3):
    """The bar closing at or just after ``ts``, within tolerance."""
    i = df.index.searchsorted(ts, side="left")
    if i >= len(df.index):
        return None
    got = df.index[i]
    if (got - ts) > pd.Timedelta(minutes=tol_min):
        return None
    return got


def event_returns(symbol: str, stamps: pd.DataFrame, *,
                  entry_delay: int, hold: int,
                  shock_window: int = 1,
                  offset_minutes: int = 0) -> pd.DataFrame:
    """One row per event: the shock, the subsequent move, and the real cost.

    ``shock`` is measured from the last bar BEFORE the release to the entry
    bar; the position is taken at the entry bar's close and closed ``hold``
    minutes later. Both legs pay the observed half-spread of their own bar.
    """
    df = load_bars(symbol)
    if df is None or df.empty:
        return None
    px = pd.to_numeric(df["close"], errors="coerce")
    sp = pd.to_numeric(df["spread"], errors="coerce")
    half = (sp / 2.0) / px * 1e4                    # bps, per side

    rows = []
    for _, r in stamps.iterrows():
        t0 = r["stamp_utc"] + pd.Timedelta(minutes=offset_minutes)
        t_pre = _bar_at(df, t0 - pd.Timedelta(minutes=shock_window))
        t_in = _bar_at(df, t0 + pd.Timedelta(minutes=entry_delay))
        t_out = _bar_at(df, t0 + pd.Timedelta(minutes=entry_delay + hold))
        if t_pre is None or t_in is None or t_out is None:
            continue
        p_pre, p_in, p_out = px.get(t_pre), px.get(t_in), px.get(t_out)
        if not (np.isfinite(p_pre) and np.isfinite(p_in)
                and np.isfinite(p_out)) or p_pre <= 0 or p_in <= 0:
            continue
        shock = float(p_in / p_pre - 1.0)
        fwd = float(p_out / p_in - 1.0)
        cost = float((half.get(t_in, np.nan) + half.get(t_out, np.nan)) / 1e4)
        if not np.isfinite(cost):
            continue
        cost += 2.0 * float(C.INTRADAY_SLIPPAGE_BPS_PER_SIDE) / 1e4
        rows.append({"event": r["event"], "date": r["date"],
                     "stamp_utc": r["stamp_utc"], "shock": shock,
                     "forward": fwd, "cost": cost,
                     "half_spread_in_bps": float(half.get(t_in, np.nan)),
                     "half_spread_out_bps": float(half.get(t_out, np.nan))})
    if not rows:
        return None
    return pd.DataFrame(rows)


def score_rule(ev: pd.DataFrame, rule: str) -> dict:
    """Net per-event return of trading the shock, after the observed spread.

    The position is +-1 unit of notional; the cost is charged in full on
    every event, whether the rule made money on it or not.
    """
    if ev is None or ev.empty:
        return {"state": "NO_EVENTS"}
    sign = -np.sign(ev["shock"]) if rule == "REVERSAL" else np.sign(ev["shock"])
    gross = (sign * ev["forward"]).to_numpy(dtype=float)
    cost = ev["cost"].to_numpy(dtype=float)
    net = gross - cost
    n = len(net)
    hac = EV.hac_t(net, lags=5)
    hac_g = EV.hac_t(gross, lags=5)
    return {
        "state": "MEASURED", "rule": rule, "n_events": int(n),
        "gross_bps_per_event": float(np.nanmean(gross) * 1e4),
        "gross_t": hac_g.get("t"),
        "cost_bps_per_event": float(np.nanmean(cost) * 1e4),
        "net_bps_per_event": float(np.nanmean(net) * 1e4),
        "net_t": hac.get("t"),
        "hit_rate": float(np.mean(net > 0)),
        "cost_share_of_gross": (float(abs(np.nanmean(cost)
                                          / np.nanmean(gross)))
                                if np.nanmean(gross) else None),
        "shock_bps_mean_abs": float(np.nanmean(np.abs(ev["shock"])) * 1e4),
    }


def placebo(symbol: str, stamps: pd.DataFrame, *, entry_delay: int,
            hold: int, rule: str, shift_days: int = 7) -> dict:
    """The SAME rule at the SAME clock time on non-release days.

    Shifting the whole calendar by a week keeps the time of day, the day of
    week and the seasonal position, and removes only the release.
    """
    s = stamps.copy()
    s["stamp_utc"] = s["stamp_utc"] + pd.Timedelta(days=shift_days)
    s["date"] = pd.to_datetime(s["date"]) + pd.Timedelta(days=shift_days)
    real = set(pd.to_datetime(stamps["date"]).dt.date)
    s = s[~s["date"].dt.date.isin(real)]
    ev = event_returns(symbol, s, entry_delay=entry_delay, hold=hold)
    out = score_rule(ev, rule)
    out["placebo"] = True
    out["shift_days"] = shift_days
    return out


def zones_by_event(ev: pd.DataFrame) -> dict:
    """Chronological 50/30/20 over EVENTS, not calendar days."""
    idx = pd.DatetimeIndex(pd.to_datetime(ev["stamp_utc"]).dt.tz_localize(None))
    z = EV.zone_split(idx, embargo=0)
    return z


def to_daily_stream(ev: pd.DataFrame, rule: str) -> dict:
    """Turn the event book into a DAILY stream the portfolio can price.

    An intraday event book is flat except on release days, so its daily
    series is the per-event net return stamped on the release date and zero
    elsewhere. The capital is the FX margin the position immobilises while
    it is on.
    """
    if ev is None or ev.empty:
        return None
    sign = -np.sign(ev["shock"]) if rule == "REVERSAL" else np.sign(ev["shock"])
    gross = (sign * ev["forward"]).astype(float)
    idx = pd.DatetimeIndex(pd.to_datetime(ev["date"])).normalize()
    g = pd.Series(gross.to_numpy(), index=idx).groupby(level=0).sum()
    c = pd.Series(ev["cost"].to_numpy(), index=idx).groupby(level=0).sum()
    full = pd.date_range(g.index.min(), g.index.max(), freq="B")
    g = g.reindex(full).fillna(0.0).rename("gross")
    c = c.reindex(full).fillna(0.0).rename("cost")
    capt = J.futures_committed_capital(["FX_FUTURES"], [1.0])
    turn = (c > 0).astype(float) * 2.0
    return {"gross": g, "cost": c, "turnover": turn,
            "committed_capital": capt["committed_capital"],
            "index": full, "n_markets": 1}


def prosecute_zone_a(symbol: str, stamps: pd.DataFrame, *, entry_delay: int,
                     hold: int, rule: str) -> dict:
    """Everything that can be asked WITHOUT opening ZONE_B or ZONE_C.

    The screened cell did not clear the frozen advance bar, so no burden is
    charged and the judged zones stay shut. What remains answerable on the
    screening zone alone is whether the effect is event-specific, whether it
    survives being wrong about the release minute, and whether it lives in
    one release type or across them.
    """
    ev = event_returns(symbol, stamps, entry_delay=entry_delay, hold=hold)
    if ev is None or ev.empty:
        return {"state": "NO_EVENTS"}
    z = zones_by_event(ev)
    a_end = pd.Timestamp(z["a_range"][1]) if z["a_range"] else None
    ev_a = ev[pd.to_datetime(ev["date"]) <= a_end] if a_end is not None else ev

    base = score_rule(ev_a, rule)
    pb = placebo(symbol, stamps[pd.to_datetime(stamps["date"]) <= a_end]
                 if a_end is not None else stamps,
                 entry_delay=entry_delay, hold=hold, rule=rule)

    sweep = []
    for off in (-15, -5, -1, 0, 1, 5, 15):
        e = event_returns(symbol, stamps, entry_delay=entry_delay, hold=hold,
                          offset_minutes=off)
        if e is None:
            continue
        e = e[pd.to_datetime(e["date"]) <= a_end] if a_end is not None else e
        c = score_rule(e, rule)
        if c.get("state") == "MEASURED":
            sweep.append({"offset_min": off,
                          "net_bps_per_event": c["net_bps_per_event"],
                          "net_t": c["net_t"], "n_events": c["n_events"]})

    by_type = []
    for name, grp in ev_a.groupby("event"):
        if len(grp) < 30:
            continue
        c = score_rule(grp, rule)
        if c.get("state") == "MEASURED":
            by_type.append({"event": name, "n": c["n_events"],
                            "gross_bps": c["gross_bps_per_event"],
                            "net_bps": c["net_bps_per_event"],
                            "net_t": c["net_t"]})

    return {
        "state": "MEASURED",
        "cell": {"symbol": symbol, "entry_delay_min": entry_delay,
                 "hold_min": hold, "rule": rule},
        "zone_a": base,
        "placebo_non_release_days": pb,
        "event_specific": bool(
            base.get("net_t") is not None and pb.get("net_t") is not None
            and abs(base["net_t"]) > abs(pb["net_t"])),
        "timing_sweep": sweep,
        "timing_sweep_note": "the release minute is a DECLARED CONSTANT; if "
                             "the effect survives a +-15 minute error it was "
                             "never about the release minute",
        "by_release_type": sorted(by_type, key=lambda r: -(r["net_t"] or -9)),
        "zone_b_opened": False, "zone_c_opened": False,
        "burden_charged": False,
        "why_not_advanced": "did not reach the frozen advance bar",
    }


# --------------------------------------------------------------------------- #
# The lane
# --------------------------------------------------------------------------- #
def run(*, instruments=None, quick: bool = False) -> dict:
    """Screen every (instrument x rule x delay x hold) on ZONE_A only."""
    stamps = release_stamps()
    if stamps is None:
        return {"lane": "E1B_INTRADAY_EVENT",
                "state": "HISTORICAL_DATA_UNAVAILABLE",
                "reason": "no PIT release calendar"}
    instruments = list(instruments or available_instruments())
    if not instruments:
        return {"lane": "E1B_INTRADAY_EVENT",
                "state": "HISTORICAL_DATA_UNAVAILABLE",
                "reason": "no owned native minute bars"}

    cov = {s: coverage(s) for s in instruments}
    holds = C.INTRADAY_HOLD_MINUTES[:2] if quick else C.INTRADAY_HOLD_MINUTES
    delays = C.INTRADAY_ENTRY_DELAYS_MIN[:1] if quick \
        else C.INTRADAY_ENTRY_DELAYS_MIN

    screened, cache = [], {}
    for sym in instruments:
        for delay in delays:
            for hold in holds:
                ev = event_returns(sym, stamps, entry_delay=delay, hold=hold)
                if ev is None or len(ev) < 100:
                    continue
                z = zones_by_event(ev)
                a_end = pd.Timestamp(z["a_range"][1]) if z["a_range"] else None
                ev_a = ev[pd.to_datetime(ev["date"]) <= a_end] \
                    if a_end is not None else ev
                for rule in EVENT_RULES:
                    card = score_rule(ev_a, rule)
                    if card.get("state") != "MEASURED":
                        continue
                    card.update({"symbol": sym, "entry_delay_min": delay,
                                 "hold_min": hold, "zone": "A"})
                    screened.append(card)
                    cache[(sym, delay, hold, rule)] = ev
    return {
        "lane": "E1B_INTRADAY_EVENT",
        "state": "EXECUTED",
        "calculation_owner": CALCULATION_OWNER,
        "question": C.LANES["E1B_INTRADAY_EVENT"],
        "instruments": instruments,
        "excluded_as_cfd": list(C.INTRADAY_EXCLUDED_AS_CFD),
        "coverage": cov,
        "n_release_stamps": int(len(stamps)),
        "release_types": sorted(stamps["event"].unique().tolist()),
        "release_time_is_declared_constant": True,
        "cost_model": C.INTRADAY_COST_MODEL,
        "screened_zone_a": sorted(screened,
                                  key=lambda r: -(r["net_t"] or -9))[:40],
        "n_screened": len(screened),
        "_cache_keys": [list(k) for k in cache],
        "_cache": cache,
    }
