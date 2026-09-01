r"""alpha_agent.r53_1.intraday_feed - the canonical intraday feed adapter.

ONE normalization seam between the canonical market-data owner
(``engine.market_data.fetch_recent_intraday_bars`` - the same provider family
already integrated as the ``yahoo_delayed_quote`` lane) and the FROZEN R53
intraday factory (:mod:`alpha_agent.r53.intraday_factory`). It creates no
second evidence system and no second market-data owner: it fetches through
the owner, normalizes, measures freshness, and hands completed bars to the
factory's injected seams (``signal_fn`` / ``mark_fn``).

Honesty rules enforced here:

* only COMPLETED bars (``bar_end_utc <= received_at_utc``) ever reach a
  signal - a forming bar is not an observation;
* the information timestamp of a signal snapshot is the freshest completed
  bar END for that instrument - never the wall clock;
* the measured feed delay is recorded on every snapshot, and the latency
  class comes from the ONE lane owner's taxonomy
  (:mod:`alpha_agent.r46.intraday`) - a delayed feed never masquerades as
  real-time;
* marks for outcome scoring are the first observable print AT OR AFTER the
  requested instant from the SAME feed (next completed bar's open; the
  session close instant is marked by the session's closing print).

RESEARCH ONLY. No operational write, no order, no promotion.
"""
from __future__ import annotations

import datetime as _dt
from typing import Iterable, Optional

from ..r46 import intraday as lane_owner

CALCULATION_OWNER = "alpha_agent.r53_1.intraday_feed"

#: The liquid-proxy instrument layer of the frozen R53 specs, plus the VIX
#: signal instrument. Exactly the union of ``INTRADAY_SPECS`` instruments.
SPEC_TRADE_INSTRUMENTS = ("SPY", "QQQ", "IWM",
                          "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP",
                          "XLU", "XLB")
VIX_SIGNAL_INSTRUMENT = "^VIX"
ALL_FEED_INSTRUMENTS = SPEC_TRADE_INSTRUMENTS + (VIX_SIGNAL_INSTRUMENT,)

INTERVAL_MINUTES = 5
#: Sessions of intraday history needed by the reference windows of the frozen
#: specs (20 trailing sessions for the volume clock + margin for holidays).
LOOKBACK_CALENDAR_DAYS = 32
DAILY_LOOKBACK_CALENDAR_DAYS = 60


def _md():
    from paper_trader.engine import market_data as md
    return md


def _et(ts: _dt.datetime) -> _dt.datetime:
    from zoneinfo import ZoneInfo
    return ts.astimezone(ZoneInfo("America/New_York"))


def _parse(iso: str) -> _dt.datetime:
    return _dt.datetime.fromisoformat(str(iso).replace("Z", "+00:00"))


def session_date_of(ts_utc_iso: str) -> str:
    """The US/Eastern session date a bar belongs to."""
    return _et(_parse(ts_utc_iso)).date().isoformat()


def session_close_utc_for(now_utc: _dt.datetime) -> str:
    """16:00 US/Eastern of the current Eastern session, as UTC ISO."""
    et = _et(now_utc)
    close_et = et.replace(hour=16, minute=0, second=0, microsecond=0)
    return close_et.astimezone(_dt.timezone.utc).isoformat().replace(
        "+00:00", "Z")


def build_snapshot(*, instruments: Iterable[str] = ALL_FEED_INSTRUMENTS,
                   now_utc: Optional[_dt.datetime] = None,
                   fetch_bars=None, fetch_daily=None) -> dict:
    """One bounded feed read, normalized for the frozen signal owners.

    Returns::

        {"received_at_utc", "provider", "interval_minutes",
         "session_date_et", "session_close_utc",
         "bars": {SYM: [completed bar rows, all sessions in window]},
         "bars_today": {SYM: [completed bar rows of the current ET session]},
         "daily_closes": {SYM: [{"date", "close"}, ... ascending]},
         "freshness_seconds": {SYM: seconds since last completed bar end},
         "data_timestamp_utc": {SYM: last completed bar END iso},
         "failures": [...]}

    ``fetch_bars`` / ``fetch_daily`` are injection seams for hermetic tests;
    the default is the canonical owner.
    """
    now = (now_utc or _dt.datetime.now(_dt.timezone.utc)).astimezone(
        _dt.timezone.utc)
    syms = list(dict.fromkeys(str(s).upper() for s in instruments))
    md = _md()
    bars_fn = fetch_bars or (lambda s: md.fetch_recent_intraday_bars(
        s, interval_minutes=INTERVAL_MINUTES,
        lookback_days=LOOKBACK_CALENDAR_DAYS))
    daily_fn = fetch_daily or _default_fetch_daily

    bars_raw, failures, meta = bars_fn(syms)
    received_iso = meta.get("received_at_utc") or now.isoformat().replace(
        "+00:00", "Z")
    session_date = _et(now).date().isoformat()

    bars: dict = {}
    bars_today: dict = {}
    freshness: dict = {}
    data_ts: dict = {}
    for sym, rows in (bars_raw or {}).items():
        completed = [r for r in rows if str(r.get("bar_end_utc")) <= received_iso]
        if not completed:
            continue
        bars[sym] = completed
        bars_today[sym] = [r for r in completed
                           if session_date_of(r["ts_utc"]) == session_date]
        last_end = completed[-1]["bar_end_utc"]
        data_ts[sym] = last_end
        freshness[sym] = round(
            (_parse(received_iso) - _parse(last_end)).total_seconds(), 1)

    daily = daily_fn(syms)

    return {
        "received_at_utc": received_iso,
        "provider": meta.get("provider"),
        "canonical_owner": "engine.market_data.fetch_recent_intraday_bars",
        "interval_minutes": INTERVAL_MINUTES,
        "session_date_et": session_date,
        "session_close_utc": session_close_utc_for(now),
        "bars": bars,
        "bars_today": bars_today,
        "daily_closes": daily,
        "freshness_seconds": freshness,
        "data_timestamp_utc": data_ts,
        "failures": list(failures or []),
    }


def _default_fetch_daily(syms: list) -> dict:
    """Trailing daily closes through the SAME canonical owner (one call)."""
    md = _md()
    end = _dt.date.today()
    start = end - _dt.timedelta(days=DAILY_LOOKBACK_CALENDAR_DAYS)
    ok, _failed = md.fetch_historical_prices(syms, start, end)
    out = {}
    for sym, rows in (ok or {}).items():
        out[sym] = [{"date": str(r["market_date"]), "close": float(r["price"])}
                    for r in rows]
    return out


def lane_is_available(lane: Optional[dict] = None) -> dict:
    """The ONE lane owner's verdict, via the factory's own reader."""
    from ..r53 import intraday_factory as fac
    return fac.lane_state(lane)


def bars_asof(snapshot: dict, sym: str, asof_utc_iso: str) -> list:
    """Completed bars of ``sym`` whose END is at or before the instant."""
    return [r for r in (snapshot["bars"].get(sym) or [])
            if str(r["bar_end_utc"]) <= str(asof_utc_iso)]


def make_mark_fn(snapshot: dict):
    """``mark_fn(instrument, at_utc_iso)`` for the factory's scorer.

    First observable print at or after the instant, from the SAME feed:
    the OPEN of the first completed bar whose START is >= the instant. An
    instant at or past the session close is marked with the session's closing
    print (the last completed bar's close of that Eastern session). Returns
    None when the feed cannot honestly mark the instant yet.
    """
    def mark(instrument: str, at_utc_iso: str):
        sym = str(instrument).upper()
        rows = snapshot["bars"].get(sym) or []
        if not rows:
            return None
        at_iso = str(at_utc_iso)
        session = session_date_of(at_iso)
        close_iso = _session_close_iso(at_iso)
        if at_iso >= close_iso:
            session_rows = [r for r in rows
                            if session_date_of(r["ts_utc"]) == session]
            if not session_rows:
                return None
            # the closing print: only observable once the session has ended
            if snapshot["received_at_utc"] <= close_iso:
                return None
            return session_rows[-1].get("close")
        for r in rows:
            if str(r["ts_utc"]) >= at_iso and str(r["bar_end_utc"]) <= str(
                    snapshot["received_at_utc"]):
                return r.get("open") if r.get("open") is not None else r.get(
                    "close")
        return None
    return mark


def _session_close_iso(at_utc_iso: str) -> str:
    et = _et(_parse(at_utc_iso))
    close_et = et.replace(hour=16, minute=0, second=0, microsecond=0)
    return close_et.astimezone(_dt.timezone.utc).isoformat().replace(
        "+00:00", "Z")


def snapshot_latency_class(snapshot: dict, sym: str = "SPY") -> str:
    """The honest label for what the snapshot's freshest data really is."""
    fresh = snapshot["freshness_seconds"].get(sym)
    if fresh is None:
        return lane_owner.LAT_NOT_ENTITLED
    return lane_owner._classify_delay(max(float(fresh), 0.0))
