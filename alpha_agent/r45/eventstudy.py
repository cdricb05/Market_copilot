"""alpha_agent.r45.eventstudy - the frozen rule, and nothing but the frozen rule.

This module is deliberately small and deliberately rigid. It applies R44's
cell - fade the print, in at +5 minutes, out at +120 - to any panel, and it
has no parameters of its own that Release 45 is allowed to tune before the
replication verdict is in.

Three things it does that R44's version did not, all of them tightening
rather than loosening the test:

  * inference is CLUSTERED BY EVENT DATE, because CPI and PPI sometimes
    print into the same minute and their two event returns are the same
    trade seen twice;
  * the cost multiplier and the latency offset are first-class arguments, so
    the stress battery re-runs the identical code path rather than a
    re-implementation of it;
  * every event's record survives into the card, so the kill battery can
    remove a year, a family or the single largest winner without recomputing
    anything.

:func:`identity_check` re-derives R44's published zone-A numbers through this
code. If it does not match to 1e-6, Release 45 has no right to claim it is
testing the same rule, and the campaign refuses to run.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..r41 import evidence as EV
from ..r43 import acquisition as AQ
from . import bars as B
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r45.eventstudy"
ET = "America/New_York"


# --------------------------------------------------------------------------- #
# Event stamps
# --------------------------------------------------------------------------- #
def release_stamps(min_year: int = None, events: tuple = None) -> pd.DataFrame:
    """Scheduled release dates x the declared release time, in UTC."""
    cal = AQ.load_release_calendar()
    if cal is None or getattr(cal, "empty", True):
        return None
    cal = cal.copy()
    cal["date"] = pd.to_datetime(cal["date"])
    cal = cal[cal["date"].dt.year >= int(min_year or C.MIN_CALENDAR_YEAR)]
    rows = []
    for _, r in cal.iterrows():
        name = str(r["event"])
        if events is not None and name not in events:
            continue
        t = C.MACRO_RELEASE_TIMES_ET.get(name)
        if t is None:
            continue
        hh, mm = (int(x) for x in t.split(":"))
        local = pd.Timestamp(r["date"]).tz_localize(ET) \
            + pd.Timedelta(hours=hh, minutes=mm)
        rows.append({"event": name, "date": r["date"],
                     "stamp_utc": local.tz_convert("UTC"),
                     "declared_time_et": t})
    if not rows:
        return None
    return pd.DataFrame(rows).sort_values("stamp_utc").reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Bar lookup
# --------------------------------------------------------------------------- #
def _tolerance_min(df) -> int:
    """The frozen tolerance, widened to one bar for coarser panels.

    A 5-minute panel cannot honour a 3-minute tolerance without discarding
    almost every event; widening it to exactly one bar interval is the
    smallest change that lets the same rule be expressed at all, and every
    result computed on such a panel is labelled resolution-degraded.
    """
    base = int(C.FROZEN_RULE["bar_tolerance_min"])
    secs = int(df.attrs.get("bar_seconds", 60) or 60)
    return max(base, int(np.ceil(secs / 60.0)))


def _bar_at(idx: pd.DatetimeIndex, ts: pd.Timestamp, *, tol_min: int):
    i = idx.searchsorted(ts, side="left")
    if i >= len(idx):
        return None
    got = idx[i]
    if (got - ts) > pd.Timedelta(minutes=tol_min):
        return None
    return got


# --------------------------------------------------------------------------- #
# The event book
# --------------------------------------------------------------------------- #
def event_book(symbol: str, stamps: pd.DataFrame, *,
               entry_delay: int = None, hold: int = None,
               offset_minutes: int = 0, extra_latency: int = 0,
               panel=None) -> pd.DataFrame:
    """One row per event: the shock, the subsequent move, and the real cost.

    ``extra_latency`` delays ONLY the entry - the exit stays anchored to the
    release, which is what a late fill actually costs you.
    """
    df = panel if panel is not None else B.panel(symbol)
    if df is None or df.empty:
        return None
    entry_delay = int(C.FROZEN_RULE["entry_delay_min"]
                      if entry_delay is None else entry_delay)
    hold = int(C.FROZEN_RULE["hold_min"] if hold is None else hold)
    shock_w = int(C.FROZEN_RULE["shock_window_min"])
    tol = _tolerance_min(df)

    idx = df.index
    px = df["close"]
    half = df["half_bps"]
    dropped = 0
    rows = []
    for _, r in stamps.iterrows():
        t0 = r["stamp_utc"] + pd.Timedelta(minutes=offset_minutes)
        t_pre = _bar_at(idx, t0 - pd.Timedelta(minutes=shock_w), tol_min=tol)
        t_in = _bar_at(idx, t0 + pd.Timedelta(
            minutes=entry_delay + int(extra_latency)), tol_min=tol)
        t_out = _bar_at(idx, t0 + pd.Timedelta(minutes=entry_delay + hold),
                        tol_min=tol)
        if t_pre is None or t_in is None or t_out is None or t_out <= t_in:
            dropped += 1
            continue
        p_pre, p_in, p_out = px.get(t_pre), px.get(t_in), px.get(t_out)
        if not (np.isfinite(p_pre) and np.isfinite(p_in)
                and np.isfinite(p_out)) or p_pre <= 0 or p_in <= 0:
            dropped += 1
            continue
        h_in, h_out = float(half.get(t_in, np.nan)), float(half.get(t_out,
                                                                   np.nan))
        if not (np.isfinite(h_in) and np.isfinite(h_out)):
            dropped += 1
            continue
        rows.append({
            "event": r["event"], "date": r["date"],
            "stamp_utc": r["stamp_utc"],
            "shock": float(p_in / p_pre - 1.0),
            "forward": float(p_out / p_in - 1.0),
            "half_in_bps": h_in, "half_out_bps": h_out,
            "entry_ts": t_in, "exit_ts": t_out,
        })
    if not rows:
        return None
    out = pd.DataFrame(rows)
    out.attrs["symbol"] = symbol
    out.attrs["cost_source"] = df.attrs.get("cost_source")
    out.attrs["instrument_class"] = df.attrs.get(
        "instrument_class", B.instrument_class(symbol))
    out.attrs["dropped_events"] = int(dropped)
    out.attrs["fill_rate"] = float(len(out) / max(1, len(out) + dropped))
    # A panel that only spans two years cannot fill a fourteen-year calendar,
    # and scoring it against one would read as a data fault rather than a
    # date range. The number that means something is how many of the stamps
    # INSIDE the panel's own window it could actually trade.
    in_win = stamps[(stamps["stamp_utc"] >= idx[0])
                    & (stamps["stamp_utc"] <= idx[-1])]
    out.attrs["n_stamps_in_panel_window"] = int(len(in_win))
    out.attrs["fill_rate_in_window"] = float(
        len(out) / max(1, len(in_win)))
    out.attrs["panel_window"] = [str(idx[0]), str(idx[-1])]
    out.attrs["bar_tolerance_min"] = int(tol)
    out.attrs["resolution_degraded"] = bool(
        tol > int(C.FROZEN_RULE["bar_tolerance_min"]))
    return out


def net_series(ev: pd.DataFrame, *, rule: str = None,
               cost_mult: float = 1.0) -> tuple:
    rule = rule or C.FROZEN_RULE["rule"]
    sign = -np.sign(ev["shock"]) if rule == "REVERSAL" else np.sign(ev["shock"])
    gross = (sign * ev["forward"]).to_numpy(dtype=float)
    cost = ((ev["half_in_bps"].to_numpy(dtype=float)
             + ev["half_out_bps"].to_numpy(dtype=float)) / 1e4
            + 2.0 * float(C.SLIPPAGE_BPS_PER_SIDE) / 1e4) * float(cost_mult)
    return gross, cost, gross - cost


# --------------------------------------------------------------------------- #
# Inference
# --------------------------------------------------------------------------- #
def cluster_t(net: np.ndarray, dates) -> dict:
    """Mean and t with observations clustered by EVENT DATE.

    Two releases that print into the same minute are one trade, not two
    independent draws, and this is what stops that from inflating the t.
    """
    d = np.asarray(net, dtype=float)
    keys = pd.Index(pd.to_datetime(pd.Series(dates).values)).normalize()
    ok = np.isfinite(d)
    d, keys = d[ok], keys[ok]
    n = d.size
    if n < 12:
        return {"n": int(n), "mean": None, "t": None, "n_clusters": None}
    mu = float(d.mean())
    dev = d - mu
    g = pd.Series(dev).groupby(pd.Index(keys)).sum().to_numpy(dtype=float)
    nc = g.size
    if nc < 3:
        return {"n": int(n), "mean": mu, "t": None, "n_clusters": int(nc)}
    var = float((g ** 2).sum()) / (n ** 2)
    scale = nc / max(1.0, nc - 1.0)
    se = float(np.sqrt(max(var * scale, 0.0)))
    return {"n": int(n), "mean": mu, "n_clusters": int(nc),
            "t": (mu / se if se > 0 else None), "se": se}


def score(ev: pd.DataFrame, *, rule: str = None, cost_mult: float = 1.0,
          label: str = None) -> dict:
    """The card. Cost is charged on every event, winner or loser."""
    if ev is None or len(ev) == 0:
        return {"state": "NO_EVENTS"}
    rule = rule or C.FROZEN_RULE["rule"]
    gross, cost, net = net_series(ev, rule=rule, cost_mult=cost_mult)
    hac = EV.hac_t(net, lags=C.HAC_LAGS)
    hac_g = EV.hac_t(gross, lags=C.HAC_LAGS)
    cl = cluster_t(net, ev["date"])
    yr = pd.to_datetime(ev["date"]).dt.year
    pnl = pd.Series(net, index=yr.values)
    by_year = pnl.groupby(level=0).sum()
    tot = float(net.sum())
    n = int(len(net))
    return {
        "state": "MEASURED", "label": label, "rule": rule,
        "symbol": ev.attrs.get("symbol"),
        "instrument_class": ev.attrs.get("instrument_class"),
        "cost_source": ev.attrs.get("cost_source"),
        "cost_multiplier": float(cost_mult),
        "n_events": n,
        "gross_bps_per_event": float(np.nanmean(gross) * 1e4),
        "gross_t": hac_g.get("t"),
        "cost_bps_per_event": float(np.nanmean(cost) * 1e4),
        "net_bps_per_event": float(np.nanmean(net) * 1e4),
        "net_t": hac.get("t"),
        "net_t_cluster": cl.get("t"),
        "n_clusters": cl.get("n_clusters"),
        "hit_rate": float(np.mean(net > 0)),
        "cost_share_of_gross": (float(abs(np.nanmean(cost)
                                          / np.nanmean(gross)))
                                if np.nanmean(gross) else None),
        "shock_bps_mean_abs": float(np.nanmean(np.abs(ev["shock"])) * 1e4),
        "max_event_loss_bps": float(np.nanmin(net) * 1e4),
        "max_event_gain_bps": float(np.nanmax(net) * 1e4),
        "event_pnl_skew": (float(pd.Series(net).skew())
                           if n > 2 else None),
        "largest_event_share_of_pnl":
            (float(np.nanmax(net) / tot) if tot > 0 else None),
        "largest_year_share_of_pnl":
            (float(by_year.max() / tot) if tot > 0 else None),
        "year_range": [int(yr.min()), int(yr.max())],
        "fill_rate": ev.attrs.get("fill_rate"),
        "fill_rate_in_window": ev.attrs.get("fill_rate_in_window"),
        "n_stamps_in_panel_window": ev.attrs.get("n_stamps_in_panel_window"),
        "panel_window": ev.attrs.get("panel_window"),
        "dropped_events": ev.attrs.get("dropped_events"),
        "resolution_degraded": ev.attrs.get("resolution_degraded"),
        "bar_tolerance_min": ev.attrs.get("bar_tolerance_min"),
    }


# --------------------------------------------------------------------------- #
# Zones
# --------------------------------------------------------------------------- #
def zone_of(ev: pd.DataFrame) -> dict:
    """R44's chronological 50/30/20 split over EVENTS, recomputed."""
    idx = pd.DatetimeIndex(pd.to_datetime(ev["stamp_utc"]).dt.tz_localize(None))
    return EV.zone_split(idx, embargo=0)


def slice_zone(ev: pd.DataFrame, which: str) -> pd.DataFrame:
    z = zone_of(ev)
    d = pd.to_datetime(ev["date"])
    a_end = pd.Timestamp(z["a_range"][1]) if z["a_range"] else None
    b_end = pd.Timestamp(z["b_range"][1]) if z["b_range"] else None
    if which == "A":
        out = ev[d <= a_end]
    elif which == "B":
        out = ev[(d > a_end) & (d <= b_end)]
    elif which == "C":
        out = ev[d > b_end]
    elif which == "BC":
        out = ev[d > a_end]
    else:
        out = ev
    out = out.copy()
    out.attrs.update(ev.attrs)
    return out


# --------------------------------------------------------------------------- #
# Identity
# --------------------------------------------------------------------------- #
def identity_check() -> dict:
    """Re-derive R44's published zone-A card through Release 45's code."""
    ref = C.R44_ZONE_A_REFERENCE
    stamps = release_stamps()
    if stamps is None:
        return {"state": "HISTORICAL_DATA_UNAVAILABLE",
                "why": "no PIT release calendar"}
    ev = event_book(ref["symbol"], stamps)
    if ev is None:
        return {"state": "HISTORICAL_DATA_UNAVAILABLE",
                "why": "no owned bars for the instrument of origin"}
    a = slice_zone(ev, "A")
    got = score(a, label="R45_IDENTITY_ON_R44_ZONE_A")
    checked = {}
    worst = 0.0
    for k in ("n_events", "gross_bps_per_event", "gross_t",
              "cost_bps_per_event", "net_bps_per_event", "net_t", "hit_rate"):
        want, have = ref[k], got.get(k)
        if want is None or have is None:
            checked[k] = {"want": want, "got": have, "match": False}
            worst = max(worst, 1.0)
            continue
        diff = abs(float(have) - float(want))
        rel = diff / max(1e-12, abs(float(want)))
        checked[k] = {"want": want, "got": have, "abs_diff": diff}
        worst = max(worst, rel)
    ok = worst <= C.R44_REFERENCE_TOLERANCE
    return {
        "state": "IDENTICAL" if ok else "DIVERGED",
        "worst_relative_difference": worst,
        "tolerance": C.R44_REFERENCE_TOLERANCE,
        "checked": checked,
        "why_it_matters": "Release 45 may only claim to be testing R44's rule "
                          "if R44's own number falls out of Release 45's code",
    }
