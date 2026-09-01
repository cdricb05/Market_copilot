r"""alpha_agent.r53_1.intraday_signals - the FROZEN R53 spec implementations.

Eight signal owners, one per ``signal_owner`` declared in the frozen
:data:`alpha_agent.r53.intraday_factory.INTRADAY_SPECS`. The specifications
were frozen in Release 53 BEFORE any current intraday bar existed; this
module implements them verbatim - every threshold and window below is read
from the frozen spec parameters, nothing is retuned, and no parameter was
searched against intraday outcomes (none exist yet).

Declared conventions (implementation choices that the frozen specs left to
the standard convention, stated here once, before the first emission):

* the 30-minute volatility unit is the 20-session close-to-close daily
  volatility scaled by sqrt(30/390) - plain square-root-of-time;
* a "formation window anchored at the slot instant" ends at the last
  COMPLETED bar of the snapshot (the emission happens inside the slot's
  grace window, so that end is the slot instant to within one bar);
* the information timestamp of a signal is the freshest completed bar END
  of the instrument(s) it read - the as-of instant of its information set;
* reference windows ("same clock window, trailing N sessions") compare
  bars whose US/Eastern end-of-bar time falls in the same wall-clock window
  on each prior session;
* a signal whose preconditions cannot be computed (insufficient history,
  missing bars) emits NOTHING - absence, never a guess.

RESEARCH ONLY. SHADOW ONLY. No promotion, no operational write.
"""
from __future__ import annotations

import datetime as _dt
import math
from typing import Optional

from . import sha  # noqa: F401  (convention parity with sibling modules)
from .intraday_feed import session_date_of

CALCULATION_OWNER = "alpha_agent.r53_1.intraday_signals"

MINUTES_PER_SESSION = 390.0
MIN_DAILY_RETURNS = 15          # of the 20-session volatility window
MIN_REFERENCE_SESSIONS_FRACTION = 0.5


def _parse(iso: str) -> _dt.datetime:
    return _dt.datetime.fromisoformat(str(iso).replace("Z", "+00:00"))


def _et_hhmm(iso: str) -> str:
    from zoneinfo import ZoneInfo
    return _parse(iso).astimezone(ZoneInfo("America/New_York")).strftime(
        "%H:%M")


# --------------------------------------------------------------------------- #
# Shared feature helpers (pure, deterministic, snapshot-in signals-out)
# --------------------------------------------------------------------------- #
def daily_sigma(snapshot: dict, sym: str, *, window: int = 20
                ) -> Optional[float]:
    """20-session close-to-close volatility, excluding the current session."""
    rows = [r for r in (snapshot["daily_closes"].get(sym) or [])
            if r["date"] < snapshot["session_date_et"]]
    closes = [float(r["close"]) for r in rows][-(window + 1):]
    rets = [closes[i] / closes[i - 1] - 1.0 for i in range(1, len(closes))]
    if len(rets) < MIN_DAILY_RETURNS:
        return None
    mean = sum(rets) / len(rets)
    var = sum((x - mean) ** 2 for x in rets) / max(len(rets) - 1, 1)
    sig = math.sqrt(var)
    return sig if sig > 0 else None


def prior_close(snapshot: dict, sym: str) -> Optional[float]:
    rows = [r for r in (snapshot["daily_closes"].get(sym) or [])
            if r["date"] < snapshot["session_date_et"]]
    return float(rows[-1]["close"]) if rows else None


def today_open(snapshot: dict, sym: str) -> Optional[float]:
    rows = snapshot["bars_today"].get(sym) or []
    if not rows:
        return None
    o = rows[0].get("open")
    return float(o) if o is not None else None


def trailing_window(snapshot: dict, sym: str, minutes: int) -> list:
    """Today's completed bars inside the last ``minutes`` before the
    snapshot's freshest completed bar for the instrument."""
    rows = snapshot["bars_today"].get(sym) or []
    if not rows:
        return []
    end = _parse(rows[-1]["bar_end_utc"])
    start = end - _dt.timedelta(minutes=minutes)
    return [r for r in rows if _parse(r["bar_end_utc"]) > start]


def window_return(bars: list) -> Optional[float]:
    if len(bars) < 2:
        return None
    a = bars[0].get("open") or bars[0].get("close")
    b = bars[-1].get("close")
    if not a or not b:
        return None
    return float(b) / float(a) - 1.0


def sigma_scaled(sig_daily: float, minutes: int) -> float:
    return sig_daily * math.sqrt(minutes / MINUTES_PER_SESSION)


def same_clock_reference(snapshot: dict, sym: str, minutes: int,
                         sessions: int, value_fn) -> list:
    """``value_fn(bars_of_window)`` per prior session's same wall-clock
    window; sessions with no bars in the window are skipped."""
    rows = snapshot["bars"].get(sym) or []
    today_rows = snapshot["bars_today"].get(sym) or []
    if not today_rows:
        return []
    window_end_hhmm = _et_hhmm(today_rows[-1]["bar_end_utc"])
    window_start_hhmm = _et_hhmm(
        (_parse(today_rows[-1]["bar_end_utc"])
         - _dt.timedelta(minutes=minutes)).isoformat())
    by_session: dict = {}
    for r in rows:
        d = session_date_of(r["ts_utc"])
        if d >= snapshot["session_date_et"]:
            continue
        hh = _et_hhmm(r["bar_end_utc"])
        if window_start_hhmm < hh <= window_end_hhmm:
            by_session.setdefault(d, []).append(r)
    out = []
    for d in sorted(by_session)[-sessions:]:
        v = value_fn(by_session[d])
        if v is not None:
            out.append(v)
    return out


def realized_vol(bars: list) -> Optional[float]:
    closes = [float(r["close"]) for r in bars if r.get("close")]
    if len(closes) < 3:
        return None
    rets = [closes[i] / closes[i - 1] - 1.0 for i in range(1, len(closes))]
    return math.sqrt(sum(x * x for x in rets))


def window_volume(bars: list) -> Optional[float]:
    vols = [r.get("volume") for r in bars]
    vols = [float(v) for v in vols if v]
    return sum(vols) if vols else None


def _sig(sym: str, direction: int, score, snapshot: dict,
         extra_syms: tuple = ()) -> Optional[dict]:
    """One factory-contract signal row; information timestamp is the OLDEST
    of the freshest completed bar ends across every instrument read."""
    stamps = [snapshot["data_timestamp_utc"].get(s)
              for s in (sym,) + tuple(extra_syms)]
    stamps = [s for s in stamps if s]
    if direction == 0 or not stamps:
        return None
    return {"instrument": sym, "direction": int(direction),
            "score": (round(float(score), 6) if score is not None else None),
            "data_timestamp_utc": min(stamps)}


# --------------------------------------------------------------------------- #
# The eight frozen signal owners
# --------------------------------------------------------------------------- #
def gap_continuation(spec: dict, slot: dict, snapshot: dict) -> list:
    out = []
    thr = float(spec["parameters"]["min_abs_gap_sigma"])
    for sym in spec["instruments"]:
        pc, op = prior_close(snapshot, sym), today_open(snapshot, sym)
        sig = daily_sigma(snapshot, sym)
        if not pc or not op or not sig:
            continue
        gap_sigma = (op / pc - 1.0) / sig
        if abs(gap_sigma) >= thr:
            row = _sig(sym, 1 if gap_sigma > 0 else -1, abs(gap_sigma),
                       snapshot)
            if row:
                out.append(row)
    return out


def gap_reversal(spec: dict, slot: dict, snapshot: dict) -> list:
    out = []
    thr = float(spec["parameters"]["max_abs_gap_sigma"])
    for sym in spec["instruments"]:
        pc, op = prior_close(snapshot, sym), today_open(snapshot, sym)
        sig = daily_sigma(snapshot, sym)
        if not pc or not op or not sig:
            continue
        gap_sigma = (op / pc - 1.0) / sig
        if 0.0 < abs(gap_sigma) < thr:
            row = _sig(sym, -1 if gap_sigma > 0 else 1, abs(gap_sigma),
                       snapshot)
            if row:
                out.append(row)
    return out


def intraday_momentum(spec: dict, slot: dict, snapshot: dict) -> list:
    out = []
    formation = int(spec["parameters"]["formation_minutes"])
    for sym in spec["instruments"]:
        rows = snapshot["bars_today"].get(sym) or []
        first = [r for r in rows
                 if (_parse(r["bar_end_utc"]) - _parse(rows[0]["ts_utc"])
                     ).total_seconds() <= formation * 60.0]
        ret = window_return(first)
        if ret is None or ret == 0.0:
            continue
        row = _sig(sym, 1 if ret > 0 else -1, ret, snapshot)
        if row:
            out.append(row)
    return out


def intraday_reversal(spec: dict, slot: dict, snapshot: dict) -> list:
    out = []
    p = spec["parameters"]
    formation = int(p["formation_minutes"])
    for sym in spec["instruments"]:
        sig = daily_sigma(snapshot, sym)
        bars = trailing_window(snapshot, sym, formation)
        ret = window_return(bars)
        if sig is None or ret is None:
            continue
        move_sigma = ret / sigma_scaled(sig, formation)
        if abs(move_sigma) < float(p["min_abs_move_sigma"]):
            continue
        vol_now = window_volume(bars)
        ref = same_clock_reference(snapshot, sym, formation, 20, window_volume)
        if vol_now is None or len(ref) < 10:
            continue
        vol_ratio = vol_now / (sum(ref) / len(ref))
        if vol_ratio > float(p["volume_confirmation_max_ratio"]):
            continue                     # a volume shock is information, not
        row = _sig(sym, -1 if ret > 0 else 1, abs(move_sigma), snapshot)
        if row:
            out.append(row)
    return out


def vol_breakout(spec: dict, slot: dict, snapshot: dict) -> list:
    out = []
    p = spec["parameters"]
    window = int(p["rv_window_minutes"])
    for sym in spec["instruments"]:
        bars = trailing_window(snapshot, sym, window)
        rv_now = realized_vol(bars)
        ref = same_clock_reference(snapshot, sym, window,
                                   int(p["rv_reference_sessions"]),
                                   realized_vol)
        if rv_now is None or len(ref) < 3:
            continue
        rv_ref = sum(ref) / len(ref)
        if rv_ref <= 0:
            continue
        ratio = rv_now / rv_ref
        if ratio < float(p["min_expansion_ratio"]):
            continue
        ret = window_return(bars)
        if ret is None or ret == 0.0:
            continue
        row = _sig(sym, 1 if ret > 0 else -1, ratio, snapshot)
        if row:
            out.append(row)
    return out


def sector_relative_strength(spec: dict, slot: dict, snapshot: dict) -> list:
    p = spec["parameters"]
    formation = int(p["formation_minutes"])
    bench = str(p["benchmark"])
    bench_ret = window_return(trailing_window(snapshot, bench, formation))
    if bench_ret is None:
        return []
    rel = {}
    for sym in spec["instruments"]:
        r = window_return(trailing_window(snapshot, sym, formation))
        if r is not None:
            rel[sym] = r - bench_ret
    if len(rel) < 6:
        return []
    ranked = sorted(rel, key=lambda s: rel[s])
    third = max(len(ranked) // 3, 1)
    out = []
    for sym in ranked[-third:]:
        row = _sig(sym, 1, rel[sym], snapshot, extra_syms=(bench,))
        if row:
            out.append(row)
    for sym in ranked[:third]:
        row = _sig(sym, -1, rel[sym], snapshot, extra_syms=(bench,))
        if row:
            out.append(row)
    return out


def volume_confirmation(spec: dict, slot: dict, snapshot: dict) -> list:
    out = []
    p = spec["parameters"]
    window = int(p["volume_window_minutes"])
    for sym in spec["instruments"]:
        sig = daily_sigma(snapshot, sym)
        bars = trailing_window(snapshot, sym, window)
        ret = window_return(bars)
        vol_now = window_volume(bars)
        if sig is None or ret is None or vol_now is None:
            continue
        ref = same_clock_reference(snapshot, sym, window,
                                   int(p["reference_sessions"]),
                                   window_volume)
        if len(ref) < int(p["reference_sessions"]
                          * MIN_REFERENCE_SESSIONS_FRACTION):
            continue
        vol_ratio = vol_now / (sum(ref) / len(ref))
        move_sigma = ret / sigma_scaled(sig, window)
        if (vol_ratio >= float(p["min_volume_ratio"])
                and abs(move_sigma) >= float(p["min_abs_move_sigma"])):
            row = _sig(sym, 1 if ret > 0 else -1, vol_ratio, snapshot)
            if row:
                out.append(row)
    return out


def vix_lead(spec: dict, slot: dict, snapshot: dict) -> list:
    p = spec["parameters"]
    formation = int(p["formation_minutes"])
    vix_bars = trailing_window(snapshot, "^VIX", formation)
    vix_ret = window_return(vix_bars)
    if vix_ret is None:
        return []
    change_pct = vix_ret * 100.0
    if abs(change_pct) < float(p["min_abs_change_pct"]):
        return []
    out = []
    for sym in spec["instruments"]:
        row = _sig(sym, -1 if change_pct > 0 else 1, change_pct, snapshot,
                   extra_syms=("^VIX",))
        if row:
            out.append(row)
    return out


_OWNERS = {
    "gap_continuation": gap_continuation,
    "gap_reversal": gap_reversal,
    "intraday_momentum": intraday_momentum,
    "intraday_reversal": intraday_reversal,
    "vol_breakout": vol_breakout,
    "sector_relative_strength": sector_relative_strength,
    "volume_confirmation": volume_confirmation,
    "vix_lead": vix_lead,
}


def make_signal_fn(snapshot: dict):
    """The ``signal_fn(spec, slot, now)`` seam the frozen factory expects."""
    def signal_fn(spec: dict, slot: dict, now_utc) -> list:
        owner = _OWNERS.get(spec.get("signal_owner"))
        if owner is None:
            return []
        return owner(spec, slot, snapshot)
    return signal_fn
