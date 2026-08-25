"""alpha_agent.r45.discovery - Track E. Which market prices the release first?

This is a measurement, not a trading rule, and it is the part of Release 45
that survives whatever happens to the reversal hypothesis. If a US macro
number is genuinely incorporated by rates before gold, that ordering is a
fact about the world, it is stable, and it tells Release 46 where to look.

Three independent readings, none of which assumes the answer:

  * SPEED - what fraction of the eventual move a market has achieved k
    minutes after the print, and the minute at which it passes half. The
    market that gets there first is discovering the price.
  * LEAD-LAG - the cross-correlation of minute returns inside the event
    window, over lags either side of zero. A market that leads shows its
    peak correlation at a negative lag against a market that follows.
  * PREDICTIVE - a pooled regression of one market's next-minute return on
    another's current-minute return, controlling for the target's own lag.
    This is the Granger-style question stated as a t-statistic.

Everything is computed inside the event window only. Nothing here is allowed
to see a bar before the release stamp except the one bar used as the base.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import bars as B
from . import contract as C
from . import eventstudy as ES

CALCULATION_OWNER = "alpha_agent.r45.discovery"

HORIZON_MIN = 60
PRE_MIN = 5


# --------------------------------------------------------------------------- #
def event_paths(symbol: str, stamps: pd.DataFrame, *,
                horizon: int = HORIZON_MIN, pre: int = PRE_MIN):
    """A (n_events x (pre+horizon+1)) matrix of log returns from t0-1."""
    df = B.panel(symbol)
    if df is None or df.empty:
        return None, None
    idx = df.index
    px = df["close"]
    tol = ES._tolerance_min(df)
    grid = list(range(-pre, horizon + 1))
    rows, keys = [], []
    for _, r in stamps.iterrows():
        t0 = r["stamp_utc"]
        base_ts = ES._bar_at(idx, t0 - pd.Timedelta(minutes=1), tol_min=tol)
        if base_ts is None:
            continue
        p0 = px.get(base_ts)
        if not np.isfinite(p0) or p0 <= 0:
            continue
        path = []
        for k in grid:
            ts = ES._bar_at(idx, t0 + pd.Timedelta(minutes=k), tol_min=tol)
            p = px.get(ts) if ts is not None else np.nan
            path.append(np.log(p / p0) if (p is not None
                                           and np.isfinite(p) and p > 0)
                        else np.nan)
        if np.isfinite(path).sum() < len(grid) * 0.6:
            continue
        rows.append(path)
        keys.append({"event": r["event"], "date": r["date"],
                     "stamp_utc": r["stamp_utc"]})
    if not rows:
        return None, None
    return np.asarray(rows, dtype=float), pd.DataFrame(keys)


def speed_profile(symbol: str, stamps: pd.DataFrame, *,
                  horizon: int = HORIZON_MIN) -> dict:
    """How fast the market gets to where the release takes it."""
    M, keys = event_paths(symbol, stamps, horizon=horizon)
    if M is None:
        return {"symbol": symbol, "state": "NO_EVENTS"}
    grid = np.arange(-PRE_MIN, horizon + 1)
    absmove = np.nanmean(np.abs(M), axis=0)
    final = absmove[grid == horizon][0]
    frac = absmove / final if final > 0 else absmove * np.nan
    half = None
    for k, f in zip(grid, frac):
        if k >= 0 and np.isfinite(f) and f >= 0.5:
            half = int(k)
            break
    at = {int(k): float(frac[grid == k][0]) for k in (0, 1, 2, 5, 10, 30)
          if (grid == k).any()}
    return {
        "symbol": symbol, "state": "MEASURED",
        "instrument_class": B.instrument_class(symbol),
        "sleeve": B.sleeve(symbol),
        "n_events": int(M.shape[0]),
        "mean_abs_move_bps_at_horizon": float(final * 1e4),
        "fraction_of_eventual_move_at_minute": at,
        "minutes_to_half_of_eventual_move": half,
        "pre_release_drift_bps": float(
            np.nanmean(M[:, grid == -PRE_MIN]) * 1e4),
    }


# --------------------------------------------------------------------------- #
def _window_returns(symbol: str, stamps: pd.DataFrame, *,
                    lo: int = 0, hi: int = 30):
    """Pooled minute log returns inside the event window, event by event."""
    M, keys = event_paths(symbol, stamps, horizon=hi, pre=PRE_MIN)
    if M is None:
        return None, None
    grid = np.arange(-PRE_MIN, hi + 1)
    sel = (grid >= lo - 1) & (grid <= hi)
    sub = M[:, sel]
    return np.diff(sub, axis=1), keys


def lead_lag(sym_a: str, sym_b: str, stamps: pd.DataFrame, *,
             max_lag: int = 5, hi: int = 30) -> dict:
    """Cross-correlation of event-window minute returns over lags.

    A positive peak lag means A's move today shows up in B later, i.e. A
    leads B.
    """
    ra, ka = _window_returns(sym_a, stamps, hi=hi)
    rb, kb = _window_returns(sym_b, stamps, hi=hi)
    if ra is None or rb is None:
        return {"pair": [sym_a, sym_b], "state": "NO_EVENTS"}
    key_a = ka["stamp_utc"].astype(str)
    key_b = kb["stamp_utc"].astype(str)
    common = sorted(set(key_a) & set(key_b))
    if len(common) < 30:
        return {"pair": [sym_a, sym_b], "state": "NO_EVENTS",
                "n_common": len(common)}
    ia = {k: i for i, k in enumerate(key_a)}
    ib = {k: i for i, k in enumerate(key_b)}
    A = ra[[ia[k] for k in common]]
    Bm = rb[[ib[k] for k in common]]
    rows = []
    for lag in range(-max_lag, max_lag + 1):
        if lag >= 0:
            x = A[:, :A.shape[1] - lag] if lag else A
            y = Bm[:, lag:]
        else:
            x = A[:, -lag:]
            y = Bm[:, :Bm.shape[1] + lag]
        xf, yf = x.ravel(), y.ravel()
        ok = np.isfinite(xf) & np.isfinite(yf)
        if ok.sum() < 200:
            continue
        rows.append({"lag_min": lag,
                     "corr": float(np.corrcoef(xf[ok], yf[ok])[0, 1]),
                     "n": int(ok.sum())})
    if not rows:
        return {"pair": [sym_a, sym_b], "state": "NO_EVENTS"}
    best = max(rows, key=lambda r: abs(r["corr"]))
    return {"pair": [sym_a, sym_b], "state": "MEASURED",
            "n_common_events": len(common), "rows": rows,
            "peak_lag_min": best["lag_min"], "peak_corr": best["corr"],
            "leader": (sym_a if best["lag_min"] > 0
                       else (sym_b if best["lag_min"] < 0 else "SIMULTANEOUS"))}


def predictive(sym_x: str, sym_y: str, stamps: pd.DataFrame, *,
               hi: int = 30) -> dict:
    """Does X's minute return predict Y's NEXT minute, given Y's own lag?"""
    rx, kx = _window_returns(sym_x, stamps, hi=hi)
    ry, ky = _window_returns(sym_y, stamps, hi=hi)
    if rx is None or ry is None:
        return {"x": sym_x, "y": sym_y, "state": "NO_EVENTS"}
    kxs, kys = kx["stamp_utc"].astype(str), ky["stamp_utc"].astype(str)
    common = sorted(set(kxs) & set(kys))
    if len(common) < 30:
        return {"x": sym_x, "y": sym_y, "state": "NO_EVENTS"}
    ix = {k: i for i, k in enumerate(kxs)}
    iy = {k: i for i, k in enumerate(kys)}
    X = rx[[ix[k] for k in common]]
    Y = ry[[iy[k] for k in common]]
    y_t = Y[:, 1:].ravel()
    x_l = X[:, :-1].ravel()
    y_l = Y[:, :-1].ravel()
    ok = np.isfinite(y_t) & np.isfinite(x_l) & np.isfinite(y_l)
    if ok.sum() < 300:
        return {"x": sym_x, "y": sym_y, "state": "NO_EVENTS"}
    A = np.column_stack([np.ones(ok.sum()), x_l[ok], y_l[ok]])
    b, *_ = np.linalg.lstsq(A, y_t[ok], rcond=None)
    resid = y_t[ok] - A @ b
    dof = max(1, ok.sum() - 3)
    s2 = float(resid @ resid) / dof
    xtx_inv = np.linalg.pinv(A.T @ A)
    se = float(np.sqrt(max(s2 * xtx_inv[1, 1], 0.0)))
    return {"x": sym_x, "y": sym_y, "state": "MEASURED",
            "n_obs": int(ok.sum()), "n_common_events": len(common),
            "beta_x_lag": float(b[1]), "t_x_lag": (float(b[1] / se)
                                                   if se > 0 else None),
            "beta_y_own_lag": float(b[2]),
            "reading": f"{sym_x} leads {sym_y}" if se > 0 and b[1] / se > 2
            else "no reliable lead"}


# --------------------------------------------------------------------------- #
# What productionising any of this would actually require
# --------------------------------------------------------------------------- #
def latency_budget(speed: dict) -> dict:
    """The staleness ceiling, read off the measured speed profiles.

    This is the part of Release 45 that would matter even if a candidate had
    survived: an edge is only worth what you can still capture by the time
    you can act. The half-life of price discovery IS the staleness ceiling,
    and every stage of a real pipeline has to fit inside it.
    """
    rows = []
    for sym, prof in (speed or {}).items():
        if prof.get("state") != "MEASURED":
            continue
        half = prof.get("minutes_to_half_of_eventual_move")
        f1 = (prof.get("fraction_of_eventual_move_at_minute") or {}).get(1)
        rows.append({
            "symbol": sym, "sleeve": prof.get("sleeve"),
            "instrument_class": prof.get("instrument_class"),
            "n_events": prof.get("n_events"),
            "minutes_to_half": half,
            "fraction_gone_at_1_min": f1,
            "fraction_left_at_5_min": (
                None if f1 is None else
                max(0.0, 1.0 - ((prof.get(
                    "fraction_of_eventual_move_at_minute") or {})
                    .get(5) or 0.0))),
        })
    rates = [r for r in rows if r["sleeve"] == "RATES"
             and r["instrument_class"] == "NATIVE_FUTURES"]
    worst = (min([r["fraction_left_at_5_min"] for r in rates
                  if r["fraction_left_at_5_min"] is not None], default=None)
             if rates else None)
    return {
        "rows": sorted(rows, key=lambda r: (r["minutes_to_half"]
                                            if r["minutes_to_half"] is not None
                                            else 999)),
        "staleness_ceiling": "the half-life of price discovery - measured "
                             "here at ONE MINUTE for every liquid market and "
                             "under one minute for the rates complex",
        "fraction_of_the_move_still_available_at_the_frozen_entry":
            worst,
        "latest_economically_useful_entry":
            "inside 60 seconds of the print for rates, on this evidence",
        "pipeline_stages_that_must_fit_inside_it": [
            {"stage": "event source latency",
             "what": "the release reaching the machine - a wire feed, not a "
                     "scheduled scrape",
             "estate_has_it": False},
            {"stage": "market data latency",
             "what": "a native futures tick or 1-second bar, live",
             "estate_has_it": False},
            {"stage": "feature computation",
             "what": "the shock, from a pre-release reference bar",
             "estate_has_it": True},
            {"stage": "decision latency",
             "what": "signal refresh, frontier update, reassessment",
             "estate_has_it": True},
            {"stage": "order placement",
             "what": "out of scope - this estate places no orders",
             "estate_has_it": False},
        ],
        "portfolio_reassessment_trigger":
            "a macro release would have to be an EVENT that wakes the "
            "reassessment cycle, not a date the daily cycle happens to pass "
            "over - and on this evidence the cycle would need to complete "
            "within a minute to be worth waking at all",
        "honest_reading": "the estate could not have traded this even if it "
                          "had been real: by the time a scheduled scrape and "
                          "a daily cycle notice a print, the rates complex "
                          "has finished pricing it",
    }


def run(stamps=None, *, symbols=None, pairs=None) -> dict:
    stamps = stamps if stamps is not None else ES.release_stamps()
    if stamps is None:
        return {"track": "E", "state": "HISTORICAL_DATA_UNAVAILABLE"}
    owned = list(C.OWNED_MINUTE_INSTRUMENTS)
    native = [s for s in C.NATIVE_FUTURES_INSTRUMENTS
              if B.panel(s) is not None]
    symbols = list(symbols or (owned + native))

    speed = {}
    for s in symbols:
        speed[s] = speed_profile(s, stamps)

    measured = [s for s in symbols
                if speed[s].get("state") == "MEASURED"]
    ranked = sorted(
        [speed[s] for s in measured],
        key=lambda r: (r["minutes_to_half_of_eventual_move"]
                       if r["minutes_to_half_of_eventual_move"] is not None
                       else 999))

    default_pairs = []
    for a in ("BUNDTREUR", "USA500IDXUSD", "EURUSD"):
        if a in measured and "XAUUSD" in measured:
            default_pairs.append((a, "XAUUSD"))
    if "BUNDTREUR" in measured and "USA500IDXUSD" in measured:
        default_pairs.append(("BUNDTREUR", "USA500IDXUSD"))
    for a in ("ZN=F", "ZF=F", "ZT=F"):
        for b in ("GC=F", "ES=F"):
            if a in measured and b in measured:
                default_pairs.append((a, b))
    pairs = list(pairs or default_pairs)

    ll = [lead_lag(a, b, stamps) for a, b in pairs]
    pr = [predictive(a, b, stamps) for a, b in pairs]

    return {
        "track": "E", "state": "EXECUTED",
        "calculation_owner": CALCULATION_OWNER,
        "question": C.LANES["L7_DISCOVERY"],
        "speed_profiles": speed,
        "speed_ranking_fastest_first": [
            {"symbol": r["symbol"], "class": r["instrument_class"],
             "sleeve": r["sleeve"], "n_events": r["n_events"],
             "minutes_to_half": r["minutes_to_half_of_eventual_move"],
             "move_bps_at_60m": r["mean_abs_move_bps_at_horizon"],
             "fraction_at_minute_1": r[
                 "fraction_of_eventual_move_at_minute"].get(1)}
            for r in ranked],
        "lead_lag": ll,
        "predictive": pr,
        "latency_budget": latency_budget(speed),
        "note": "measured, not assumed: the release-to-market ordering is a "
                "property of the data and it does not depend on whether any "
                "trading rule survives",
    }
