r"""alpha_agent.alpha_recovery.intraday_data - the ONE owner of the campaign's
native intraday cross-asset panel state.

WHY THIS EXISTS
    Every prior workstream in this campaign was DAILY. Contract rule 13 forbids
    reopening price-derived research for another transform, and permits it when
    COVERAGE materially improves. One-minute bars are not another daily
    transform: the overnight/pre-market split, the opening range, minute-level
    realised volatility and minute-level cross-market transmission are simply
    not reachable from a daily bar. This module establishes, MEASURED and
    before any strategy runs, exactly what intraday history the estate already
    owns - so the research that follows is bounded by the real data state
    rather than by an assumption about it.

WHAT IS OWNED (no purchase, no new infrastructure, no new provider)
    ``D:\Stock_Prediction_app_data\macro_event_alpha_r45\_data_intraday``
    acquired by Release 45 and left on disk. Two panels:

      ETF 1-minute      SPY QQQ TLT GLD IEF SHY UUP, 500 sessions,
                        2024-08-26 -> 2026-08-24.               USABLE
      futures 1-minute  ES NQ 6E 6J GC CL ZB ZN ZF ZT, 49 sessions.
                        DATA_INSUFFICIENT - cannot reach the frozen
                        MIN_EFFECTIVE_PERIODS floor under any honest scheme.

THE TWO TRAPS THIS MODULE EXISTS TO AVOID
    1. THE WINDOW IS FIXED IN UTC (11:00-16:59), NOT IN EXCHANGE TIME. The US
       regular session therefore starts at a DIFFERENT point inside the file
       depending on daylight saving: 13:30 UTC under EDT (331 sessions) and
       14:30 UTC under EST (169 sessions). A UTC-indexed minute grid silently
       mixes 09:30 ET with 08:30 ET and corrupts every opening-range and
       session-carry statistic. The grid here is built in EXCHANGE LOCAL TIME.
    2. THE PANEL STOPS EARLY. Coverage ends 16:59 UTC, i.e. 12:59 ET under EDT
       and 11:59 ET under EST. There is NO afternoon and NO closing auction, so
       nothing here can be marked to the close and the classic
       first-half-hour -> last-half-hour intraday-momentum result cannot be
       tested as published. Say so; do not approximate a close.

    The intersection that is complete on EVERY session is therefore

        REGULAR      09:30-11:59 ET   150 minutes, 500/500 sessions complete
        PRE-MARKET   07:00-09:29 ET   150 minutes, information only

RESEARCH ONLY. Reads two frozen CSV panels and writes one artifact under the
campaign research root. No provider call, no purchase, no operational write.
"""
from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd

from . import MIN_EFFECTIVE_PERIODS, research_root, write_artifact

CALCULATION_OWNER = "alpha_agent.alpha_recovery.intraday_data"
ARTIFACT_NAME = "intraday_data_state.json"

PANEL_ROOT = Path(r"D:\Stock_Prediction_app_data\macro_event_alpha_r45\_data_intraday")
OPTION_SURFACE = Path(r"D:\Stock_Prediction_app_data\macro_event_alpha_r45\_data_options"
                      r"\polygon_spy_option_surface_r45_extension.csv.gz")
TZ = "America/New_York"

#: Minutes-of-day, exchange local time. 09:30 -> 570, 11:59 -> 719.
REG_FIRST_MOD, REG_LAST_MOD = 9 * 60 + 30, 11 * 60 + 59
PRE_FIRST_MOD, PRE_LAST_MOD = 7 * 60, 9 * 60 + 29
REG_MODS = tuple(range(REG_FIRST_MOD, REG_LAST_MOD + 1))          # 150
PRE_MODS = tuple(range(PRE_FIRST_MOD, PRE_LAST_MOD + 1))          # 150
N_REG, N_PRE = len(REG_MODS), len(PRE_MODS)

#: Traded at minute resolution. Chosen by MEASURED coverage and liquidity, not
#: by preference: each prints ~100 % of the 150 regular minutes on ~every
#: session and turns over >= $1 bn a session.
TRADABLE = ("SPY", "QQQ", "TLT", "GLD")
#: Read by signals, never held: too few minutes print to cross a spread at this
#: resolution (UUP prints 117 of 210 minutes; SHY 197; IEF 208).
INFORMATION_ONLY = ("IEF", "SHY", "UUP")
INSTRUMENTS = TRADABLE + INFORMATION_ONLY

#: The economic market each leg represents - four distinct markets, which is
#: the whole point of a cross-asset intraday axis.
MARKET = {"SPY": "US_EQUITY_BETA", "QQQ": "US_TECH_BETA", "TLT": "US_LONG_DURATION",
          "GLD": "GOLD", "IEF": "US_INTERMEDIATE_DURATION", "SHY": "US_FRONT_END",
          "UUP": "US_DOLLAR"}

FUTURES = ("ES_F", "NQ_F", "6E_F", "6J_F", "GC_F", "CL_F", "ZB_F", "ZN_F", "ZF_F", "ZT_F")

# --------------------------------------------------------------------------- #
# Pre-registered cost ladder - FIXED BEFORE ANY STRATEGY RAN.
# Justified by measurement, not by preference: the median 1-minute high-low
# range is 4.04 bp on SPY, 5.90 on QQQ, 3.05 on TLT and 4.59 on GLD, and SPY's
# quoted half-spread on a ~$600 ETF is ~0.17 bp. PRIMARY is therefore ~12x the
# quoted half-spread and ~half of one median minute bar's entire range.
# --------------------------------------------------------------------------- #
COST_PRIMARY_BPS = 2.0
COST_STRESS_BPS = 5.0
COST_CANONICAL_BPS = 12.5           # the desk's single-name equity rate
COST_LADDER_BPS = (COST_PRIMARY_BPS, COST_STRESS_BPS, COST_CANONICAL_BPS)

#: Capital eligibility requires surviving this rate, not merely the headline.
COST_ELIGIBILITY_BPS = COST_STRESS_BPS

PPY = 252.0

_CACHE: dict = {}


def _cache_path() -> Path:
    return research_root() / "cells" / "intraday_panel_v1.npz"


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #
def _read_one(sym: str) -> pd.DataFrame:
    p = PANEL_ROOT / ("%s.csv.gz" % sym)
    df = pd.read_csv(p, usecols=lambda c: c in ("ts_utc", "open", "high", "low", "close", "volume"))
    t = pd.to_datetime(df["ts_utc"], utc=True).dt.tz_convert(TZ)
    df = df.drop(columns=["ts_utc"])
    df["date"] = t.dt.strftime("%Y-%m-%d").to_numpy()
    df["mod"] = (t.dt.hour * 60 + t.dt.minute).to_numpy()
    return df


def _grid(df: pd.DataFrame, mods: tuple, field: str, dates: list) -> np.ndarray:
    """(n_sessions x n_minutes) of ``field`` on the exchange-local minute grid.

    A minute with no print is forward-filled from the last print WITHIN the
    same session (the last trade is still the price); minutes before the
    session's first print stay NaN and are never back-filled.
    """
    sub = df[df["mod"].between(mods[0], mods[-1])]
    piv = sub.pivot_table(index="date", columns="mod", values=field, aggfunc="last")
    piv = piv.reindex(index=dates, columns=list(mods))
    arr = piv.to_numpy(dtype=float)
    if field != "volume":
        arr = pd.DataFrame(arr).ffill(axis=1).to_numpy()
    else:
        arr = np.nan_to_num(arr, nan=0.0)
    return arr


def panel(*, rebuild: bool = False) -> dict:
    """The aligned intraday panel. Cached to an npz under the research root."""
    if "panel" in _CACHE and not rebuild:
        return _CACHE["panel"]
    cp = _cache_path()
    if cp.exists() and not rebuild:
        z = np.load(cp, allow_pickle=False)
        out = {"dates": [str(d) for d in z["dates"]], "instruments": list(INSTRUMENTS),
               "reg_close": z["reg_close"], "pre_close": z["pre_close"],
               "reg_high": z["reg_high"], "reg_low": z["reg_low"], "reg_dollar": z["reg_dollar"]}
        _CACHE["panel"] = out
        return out

    raw = {s: _read_one(s) for s in INSTRUMENTS}
    # The session calendar is SPY's: it is the most complete leg and it is the
    # market whose session defines the others' tradability.
    dates = sorted(set(raw["SPY"]["date"].tolist()))
    n_d, n_i = len(dates), len(INSTRUMENTS)
    reg_c = np.full((n_d, N_REG, n_i), np.nan)
    pre_c = np.full((n_d, N_PRE, n_i), np.nan)
    reg_h = np.full((n_d, N_REG, n_i), np.nan)
    reg_l = np.full((n_d, N_REG, n_i), np.nan)
    reg_v = np.zeros((n_d, N_REG, n_i))
    for j, s in enumerate(INSTRUMENTS):
        df = raw[s]
        reg_c[:, :, j] = _grid(df, REG_MODS, "close", dates)
        pre_c[:, :, j] = _grid(df, PRE_MODS, "close", dates)
        reg_h[:, :, j] = _grid(df, REG_MODS, "high", dates)
        reg_l[:, :, j] = _grid(df, REG_MODS, "low", dates)
        reg_v[:, :, j] = _grid(df, REG_MODS, "volume", dates) * np.nan_to_num(reg_c[:, :, j])
    out = {"dates": dates, "instruments": list(INSTRUMENTS), "reg_close": reg_c,
           "pre_close": pre_c, "reg_high": reg_h, "reg_low": reg_l, "reg_dollar": reg_v}
    cp.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(cp, dates=np.array(dates), reg_close=reg_c, pre_close=pre_c,
                        reg_high=reg_h, reg_low=reg_l, reg_dollar=reg_v)
    _CACHE["panel"] = out
    return out


def ix(sym: str) -> int:
    return INSTRUMENTS.index(sym)


def minute_index(hh: int, mm: int) -> int:
    """Index into the regular grid for an exchange-local wall-clock time."""
    m = hh * 60 + mm
    if not (REG_FIRST_MOD <= m <= REG_LAST_MOD):
        raise ValueError("%02d:%02d ET is outside the covered regular window 09:30-11:59" % (hh, mm))
    return m - REG_FIRST_MOD


# --------------------------------------------------------------------------- #
# The measured state artifact
# --------------------------------------------------------------------------- #
def _coverage(sym: str) -> dict:
    df = _read_one(sym)
    reg = df[df["mod"].between(REG_FIRST_MOD, REG_LAST_MOD)]
    per = reg.groupby("date").size()
    rng = ((reg["high"] - reg["low"]) / reg["close"]).replace([np.inf, -np.inf], np.nan)
    dv = (reg["close"] * reg["volume"]).groupby(reg["date"]).sum()
    return {"instrument": sym, "market": MARKET.get(sym), "sessions": int(per.size),
            "median_bars_of_%d" % N_REG: int(per.median()) if per.size else 0,
            "pct_sessions_complete": round(float((per >= N_REG).mean()) * 100.0, 1) if per.size else 0.0,
            "median_1min_range_bp": round(float(rng.median()) * 1e4, 2),
            "median_session_dollar_volume_musd": round(float(dv.median()) / 1e6, 1),
            "first_session": str(per.index.min()) if per.size else None,
            "last_session": str(per.index.max()) if per.size else None,
            "role": "TRADABLE" if sym in TRADABLE else "INFORMATION_ONLY"}


def _futures_state() -> dict:
    rows = []
    for s in FUTURES:
        p = PANEL_ROOT / ("%s.csv.gz" % s)
        if not p.exists():
            continue
        df = _read_one(s)
        reg = df[df["mod"].between(REG_FIRST_MOD, REG_LAST_MOD)]
        per = reg.groupby("date").size()
        rows.append({"instrument": s, "sessions": int(per.size),
                     "median_bars_of_%d" % N_REG: int(per.median()) if per.size else 0,
                     "first_session": str(per.index.min()) if per.size else None,
                     "last_session": str(per.index.max()) if per.size else None})
    n = max([r["sessions"] for r in rows], default=0)
    return {
        "state": "DATA_INSUFFICIENT",
        "instruments": rows,
        "why": ("%d sessions of one-minute futures history cannot reach the frozen "
                "MIN_EFFECTIVE_PERIODS floor of %d under any honest block scheme, and the "
                "sample spans a single quarter, so no regime partition exists. The floor is "
                "NOT moved; the family is not opened." % (n, MIN_EFFECTIVE_PERIODS)),
        "exact_missing_requirement": ("~500 sessions of 1-minute history for ES, NQ, ZN, GC, CL and 6E "
                                      "covering the US regular session"),
        "acquisition_routes_measured_by_r45": [
            {"provider": "Databento", "state": "ACCOUNT_REQUIRED", "http": 401},
            {"provider": "CME DataMine", "state": "ACCOUNT_REQUIRED"},
            {"provider": "Norgate Data", "state": "OWNED_BUT_DAILY_ONLY",
             "detail": "price_timeseries returns the identical daily frame for interval 1min"},
            {"provider": "Kibot", "state": "ACCOUNT_REQUIRED"}],
        "blocks_alpha": False,
        "blocks_alpha_why": "the ETF panel supplies four economically distinct markets at "
                            "500 sessions; futures would widen the axis, not unblock it",
    }


def build(*, write: bool = True) -> dict:
    pn = panel()
    reg = pn["reg_close"]
    n_d = len(pn["dates"])
    complete = {}
    for s in INSTRUMENTS:
        col = reg[:, :, ix(s)]
        complete[s] = round(float(np.isfinite(col).all(axis=1).mean()) * 100.0, 1)
    body = {
        "schema": "alpha_recovery_intraday_data_state/1",
        "calculation_owner": CALCULATION_OWNER,
        "question": "what genuine intraday history does the estate ALREADY own, and what does it "
                    "permit us to ask?",
        "panel_root": str(PANEL_ROOT),
        "acquired_by": "Release 45 (alpha_agent.r45.acquisition); left on disk, re-used here",
        "purchased_anything": False,
        "timestamp_convention": {
            "stored": "ts_utc, genuine UTC with explicit +00:00 offset",
            "file_window_utc": "11:00-16:59",
            "trap": "the file window is fixed in UTC, so the US regular session starts at 13:30 UTC "
                    "under EDT and 14:30 UTC under EST; a UTC-indexed minute grid mixes 09:30 ET "
                    "with 08:30 ET",
            "resolution": "every grid in this campaign is built in exchange local time (%s)" % TZ,
            "edt_sessions": 331, "est_sessions": 169},
        "covered_window_exchange_local": {
            "regular": "09:30-11:59 ET (%d minutes, complete on every session)" % N_REG,
            "pre_market": "07:00-09:29 ET (%d minutes, information only)" % N_PRE,
            "what_is_missing": "the afternoon and the closing auction. Coverage ends 16:59 UTC = "
                               "12:59 ET (EDT) / 11:59 ET (EST).",
            "consequence": "no strategy here may be marked to the close, and the published "
                           "first-half-hour -> last-half-hour intraday momentum result cannot be "
                           "tested as specified. Nothing approximates a close."},
        "sessions": n_d, "first_session": pn["dates"][0], "last_session": pn["dates"][-1],
        "tradable": list(TRADABLE), "information_only": list(INFORMATION_ONLY),
        "markets_represented": sorted({MARKET[s] for s in TRADABLE}),
        "coverage": [_coverage(s) for s in INSTRUMENTS],
        "pct_sessions_with_complete_regular_grid": complete,
        "futures_panel": _futures_state(),
        "option_surface": {
            "path": str(OPTION_SURFACE), "exists": OPTION_SURFACE.exists(),
            "role": "information axis A (options / implied volatility), daily not intraday",
            "note": "held for the next axis if the intraday axis does not qualify a signal"},
        "live_forward_feed_state": {
            "source": "measured by R53.1 feed_capability on 2026-09-01; NOT re-probed here",
            "historical_research_lane": "frozen R38/R45 minute panels (this module)",
            "forward_emission_lane": "yahoo chart bars, ~94 s delay, "
                                     "engine.market_data.fetch_current_session_bars, ~30 days of "
                                     "minute history, via alpha_agent.r53.intraday_factory",
            "norgate": "DAILY_ONLY", "polygon": "NOT_ENTITLED_TODAY (403)",
            "consequence": "a qualified intraday challenger CAN be emitted forward; it cannot be "
                           "back-filled, and no forward row may be stamped from these panels"},
        "cost_ladder_bps_per_side": {
            "primary": COST_PRIMARY_BPS, "stress": COST_STRESS_BPS, "canonical": COST_CANONICAL_BPS,
            "eligibility_requires": COST_ELIGIBILITY_BPS,
            "fixed_before_any_result": True,
            "justification": "the median 1-minute high-low range is 4.04 bp (SPY), 5.90 (QQQ), "
                             "3.05 (TLT), 4.59 (GLD); SPY's quoted half-spread on a ~$600 ETF is "
                             "~0.17 bp. PRIMARY is ~12x the quoted half-spread and ~half of one "
                             "median minute bar's whole range.",
            "no_threshold_relaxed": "the frozen materiality, t, BH, Holm, halves and effective-period "
                                    "gates are unchanged"},
    }
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
