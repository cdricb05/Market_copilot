"""alpha_agent.r46.intraday - can a PROSPECTIVE intraday lane run here today?

The question is narrow: does this estate, right now, hold a CURRENT, licensed,
PIT-safe intraday feed from which a challenger could emit a 30/60/120-minute
prediction whose signal timestamp, information timestamp and execution
timestamp can all be stated and defended? Historical intraday files do not
answer it - Release 38 acquired native futures minute panels and Release 45
mapped the free intraday routes, and every one of those is a HISTORY, usable
to nominate a challenger and useless to run one forward.

What is actually owned, probed rather than assumed:

* **Norgate (NDU)** serves DAILY bars. Release 45 measured that the vendor
  API ignores an ``interval`` argument entirely - the same call shape returns
  daily bars whatever is asked. There is no intraday capability to switch on.
* **Polygon (owned free key)** serves previous-session aggregates on this
  plan. A live probe for TODAY's intraday bars is made when the key is in the
  operator shell, and its result is recorded either way; the free tier's
  entitlement is end-of-day, which cannot time-stamp a 30-minute signal.

So the honest state is ``DATA_BLOCKED`` unless a probe demonstrates
otherwise, with the exact blocker named per source. A blocked lane stops
nothing else: the daily one-session cells already read the fastest clock the
owned data legitimately supports, and the session-close horizon IS the daily
horizon-1 cell.

No purchase, no trial, no account, no subscription change. Probing spends
nothing.
"""
from __future__ import annotations

import datetime as _dt
import os

from . import CAMPAIGN_ID, artifact_body, campaign_dir, write_json
from . import clock as CK

CALCULATION_OWNER = "alpha_agent.r46.intraday"

ARTIFACT = "R46_INTRADAY_LANE.json"

LANE_AVAILABLE = "AVAILABLE_NOW"
LANE_BLOCKED = "DATA_BLOCKED"
LANE_STATES = (LANE_AVAILABLE, LANE_BLOCKED)

REQUESTED_HORIZONS = ("30m", "60m", "120m", "session_close")

#: Release 53.1 honest latency taxonomy. A delayed feed may still produce
#: TRUE_FORWARD evidence (only already-observable information is used and the
#: outcome starts strictly after emission), but it must never masquerade as
#: real-time — every probed source carries one of these labels.
LAT_REAL_TIME = "REAL_TIME"
LAT_NEAR_REAL_TIME = "NEAR_REAL_TIME"
LAT_DELAYED_INTRADAY = "DELAYED_INTRADAY"
LAT_DAILY_ONLY = "DAILY_ONLY"
LAT_NOT_ENTITLED = "NOT_ENTITLED"
LATENCY_CLASSES = (LAT_REAL_TIME, LAT_NEAR_REAL_TIME, LAT_DELAYED_INTRADAY,
                   LAT_DAILY_ONLY, LAT_NOT_ENTITLED)

#: A source can stamp a 30-minute signal only if its freshest CURRENT-session
#: observation is younger than this at emission — the same ceiling the R53
#: intraday factory enforces row by row (MAX_INPUT_AGE_MINUTES).
STAMPABLE_MAX_AGE_SECONDS = 20 * 60.0


def _classify_delay(delay_seconds) -> str:
    if delay_seconds is None:
        return LAT_NOT_ENTITLED
    d = float(delay_seconds)
    if d <= 5.0:
        return LAT_REAL_TIME
    if d <= 120.0:
        return LAT_NEAR_REAL_TIME
    return LAT_DELAYED_INTRADAY


def _polygon_probe() -> dict:
    """One bounded read-only request: TODAY's 30-minute SPY aggregates.

    The free plan's entitlement is previous-session data; the probe records
    what the venue actually returns rather than what the plan page says. At
    most two HTTP calls, zero dollars, no state change anywhere.
    """
    key = (os.environ.get("POLYGON_API_KEY") or "").strip()
    if not key:
        return {"source": "polygon", "state": "KEY_NOT_IN_THIS_SHELL",
                "current_intraday": False,
                "detail": "the owned key lives in the operator shell; "
                          "entitlement on the owned plan is end-of-day "
                          "aggregates, which cannot stamp an intraday signal"}
    try:
        from ..r43 import acquisition as R43AQ
        today = _dt.date.today().isoformat()
        status, body, _ = R43AQ.http_get(
            R43AQ.POLY + "/v2/aggs/ticker/SPY/range/30/minute/%s/%s"
            "?adjusted=true&limit=50&apiKey=%s" % (today, today, key))
        import json as _json
        results = []
        if status == 200:
            try:
                results = _json.loads(body).get("results") or []
            except ValueError:
                results = []
        return {"source": "polygon", "probed_date": today,
                "http_status": status, "n_bars_today": len(results),
                "current_intraday": bool(results),
                "state": ("CURRENT_INTRADAY_SERVED" if results
                          else "NOT_ENTITLED_TODAY"),
                "detail": ("the venue returned %d intraday bar(s) for the "
                           "current session" % len(results)) if results else
                          "no intraday bars for the current session are "
                          "served on the owned plan"}
    except Exception as exc:                    # noqa: BLE001 - probe, not path
        return {"source": "polygon", "state": "PROBE_FAILED",
                "current_intraday": False,
                "detail": "%s: %s" % (type(exc).__name__, str(exc)[:160])}


def _yahoo_chart_probe() -> dict:
    """Current-session intraday bars via the canonical Yahoo market-data owner.

    Delegates to ``engine.market_data.fetch_current_session_bars`` — the SAME
    function any prospective emission would stamp against, so the probe
    measures exactly the capability the factory would use. Bar timestamps are
    exchange-stamped; delay is measured against the last COMPLETED bar's end.
    """
    try:
        from paper_trader.engine import market_data as md
        bars, failures, meta = md.fetch_current_session_bars(
            ["SPY"], interval_minutes=5)
        rows = bars.get("SPY") or []
        received = _dt.datetime.fromisoformat(
            meta["received_at_utc"].replace("Z", "+00:00"))
        completed = [r for r in rows
                     if r["bar_end_utc"] <= meta["received_at_utc"]]
        if not completed:
            return {"source": "yahoo_chart_bars", "state": "NO_CURRENT_BARS",
                    "latency_class": LAT_DAILY_ONLY, "current_intraday": False,
                    "detail": "no completed current-session bar was returned "
                              "(closed market or empty response); failures=%d"
                              % len(failures)}
        last = completed[-1]
        last_end = _dt.datetime.fromisoformat(
            last["bar_end_utc"].replace("Z", "+00:00"))
        delay = (received - last_end).total_seconds()
        today_utc = _dt.datetime.now(_dt.timezone.utc).date().isoformat()
        is_current = (last["ts_utc"][:10] == today_utc
                      and delay <= STAMPABLE_MAX_AGE_SECONDS)
        return {"source": "yahoo_chart_bars",
                "state": ("CURRENT_INTRADAY_SERVED" if is_current
                          else "STALE_OR_PRIOR_SESSION"),
                "latency_class": _classify_delay(max(delay, 0.0)) if is_current
                                 else LAT_DAILY_ONLY,
                "current_intraday": bool(is_current),
                "n_completed_bars_today": len(completed),
                "last_completed_bar_end_utc": last["bar_end_utc"],
                "measured_delay_seconds": round(delay, 1),
                "canonical_owner": "engine.market_data.fetch_current_session_bars",
                "detail": "exchange-stamped OHLCV bars for the current "
                          "session; free public entitlement class already "
                          "integrated as the yahoo_delayed_quote lane"}
    except Exception as exc:                    # noqa: BLE001 - probe, not path
        return {"source": "yahoo_chart_bars", "state": "PROBE_FAILED",
                "latency_class": LAT_NOT_ENTITLED, "current_intraday": False,
                "detail": "%s: %s" % (type(exc).__name__, str(exc)[:160])}


def _keyed_quote_probe(source: str, env_var: str, url_fn, parse_fn) -> dict:
    """One bounded keyed quote request; the key is read from the operator
    shell and never recorded. ``parse_fn(json_body)`` returns
    (quote_ts_utc: datetime|None, price: float|None)."""
    key = (os.environ.get(env_var) or "").strip()
    if not key:
        return {"source": source, "state": "KEY_NOT_IN_THIS_SHELL",
                "latency_class": LAT_NOT_ENTITLED, "current_intraday": False,
                "detail": "the owned key lives in the operator shell; "
                          "not present in this one"}
    try:
        import json as _json
        import urllib.request as _rq
        req = _rq.Request(url_fn(key),
                          headers={"User-Agent": "paper-trader-lane-probe"})
        with _rq.urlopen(req, timeout=20) as resp:
            body = _json.loads(resp.read().decode("utf-8", errors="replace"))
        ts, price = parse_fn(body)
        if ts is None or price is None:
            return {"source": source, "state": "NOT_ENTITLED_TODAY",
                    "latency_class": LAT_NOT_ENTITLED,
                    "current_intraday": False,
                    "detail": "the venue answered without a timestamped "
                              "current-session observation"}
        delay = (_dt.datetime.now(_dt.timezone.utc) - ts).total_seconds()
        is_current = (ts.date() == _dt.datetime.now(_dt.timezone.utc).date()
                      and delay <= STAMPABLE_MAX_AGE_SECONDS)
        return {"source": source,
                "state": ("CURRENT_INTRADAY_SERVED" if is_current
                          else "STALE_OR_PRIOR_SESSION"),
                "latency_class": (_classify_delay(max(delay, 0.0))
                                  if is_current else LAT_DAILY_ONLY),
                "current_intraday": bool(is_current),
                "quote_ts_utc": ts.isoformat(),
                "measured_delay_seconds": round(delay, 1),
                "detail": "timestamped last-observation quote; quote-only "
                          "(no bar history at this entitlement)"}
    except Exception as exc:                    # noqa: BLE001 - probe, not path
        return {"source": source, "state": "PROBE_FAILED",
                "latency_class": LAT_NOT_ENTITLED, "current_intraday": False,
                "detail": "%s: %s" % (type(exc).__name__, str(exc)[:160])}


def _tiingo_probe() -> dict:
    def parse(body):
        if isinstance(body, list) and body:
            q = body[0]
            ts = q.get("timestamp") or q.get("lastSaleTimestamp")
            price = q.get("last") or q.get("tngoLast")
            if isinstance(ts, str):
                return (_dt.datetime.fromisoformat(
                    ts.replace("Z", "+00:00")).astimezone(_dt.timezone.utc),
                    price)
        return (None, None)
    return _keyed_quote_probe(
        "tiingo_iex", "TIINGO_API_KEY",
        lambda k: "https://api.tiingo.com/iex/SPY?token=%s" % k, parse)


def _finnhub_probe() -> dict:
    def parse(body):
        ts = body.get("t") if isinstance(body, dict) else None
        if isinstance(ts, (int, float)) and ts > 0:
            return (_dt.datetime.fromtimestamp(ts, _dt.timezone.utc),
                    body.get("c"))
        return (None, None)
    return _keyed_quote_probe(
        "finnhub_quote", "FINNHUB_API_KEY",
        lambda k: "https://finnhub.io/api/v1/quote?symbol=SPY&token=%s" % k,
        parse)


def _eodhd_quote_probe() -> dict:
    def parse(body):
        ts = body.get("timestamp") if isinstance(body, dict) else None
        if isinstance(ts, (int, float)) and ts > 0:
            return (_dt.datetime.fromtimestamp(ts, _dt.timezone.utc),
                    body.get("close"))
        return (None, None)
    return _keyed_quote_probe(
        "eodhd_delayed_quote", "EODHD_API_KEY",
        lambda k: "https://eodhd.com/api/real-time/SPY.US?api_token=%s"
                  "&fmt=json" % k, parse)


def probe(campaign_id: str = CAMPAIGN_ID, live_probe: bool = True) -> dict:
    """Probe every candidate intraday source and persist the lane artifact."""
    sources = [{
        "source": "norgate_ndu",
        "state": "DAILY_ONLY",
        "latency_class": LAT_DAILY_ONLY,
        "current_intraday": False,
        "detail": "NDU serves daily bars; Release 45 measured that the "
                  "vendor API ignores an interval argument entirely, so "
                  "there is no intraday capability to enable",
    }, {
        "source": "historical_intraday_panels_r38_r45",
        "state": "HISTORICAL_ONLY",
        "latency_class": LAT_NOT_ENTITLED,
        "current_intraday": False,
        "detail": "acquired minute panels are frozen history: they can "
                  "nominate an intraday challenger and can never emit a "
                  "forward row, because a forward row needs a CURRENT bar",
    }]
    if live_probe:
        polygon = _polygon_probe()
        polygon.setdefault("latency_class",
                           LAT_NEAR_REAL_TIME if polygon.get("current_intraday")
                           else LAT_NOT_ENTITLED)
        sources.append(polygon)
        # Release 53.1: the owned sources the original lane never probed.
        sources.append(_yahoo_chart_probe())
        sources.append(_tiingo_probe())
        sources.append(_finnhub_probe())
        sources.append(_eodhd_quote_probe())
    else:
        for sid in ("polygon", "yahoo_chart_bars", "tiingo_iex",
                    "finnhub_quote", "eodhd_delayed_quote"):
            sources.append({"source": sid, "state": "NOT_PROBED",
                            "latency_class": LAT_NOT_ENTITLED,
                            "current_intraday": False,
                            "detail": "live probe not requested in this run"})

    available = any(s.get("current_intraday") for s in sources)
    state = LANE_AVAILABLE if available else LANE_BLOCKED

    body = artifact_body(
        "r46_intraday_lane/1", CALCULATION_OWNER,
        probed_at_utc=CK.iso(CK.now_utc()),
        question="does a CURRENT, licensed, PIT-safe intraday feed exist "
                 "here from which a prospective challenger could emit?",
        state=state,
        state_vocabulary=list(LANE_STATES),
        requested_horizons=list(REQUESTED_HORIZONS),
        horizon_verdicts={
            "30m": state, "60m": state, "120m": state,
            "session_close": "SERVED_BY_DAILY_H1",
        },
        session_close_note=(
            "the session-close horizon is already the daily one-session "
            "cell: signal at the previous close, entry at the next close, "
            "judged on the instrument's own realised calendar"),
        sources=sources,
        latency_taxonomy=list(LATENCY_CLASSES),
        stampable_max_age_seconds=STAMPABLE_MAX_AGE_SECONDS,
        exact_blocker=(None if available else
                       "no owned source serves a bar for the CURRENT session "
                       "intraday; Norgate is daily by construction and the "
                       "owned venue plan is end-of-day"),
        bar_capable_sources=[s["source"] for s in sources
                             if s.get("current_intraday")
                             and s["source"] == "yahoo_chart_bars"],
        quote_only_sources=[s["source"] for s in sources
                            if s.get("current_intraday")
                            and s["source"] != "yahoo_chart_bars"],
        a_blocked_lane_stops_nothing_else=True,
        historical_files_are_not_a_feed=True,
        money_spent_usd=0.0,
        purchases=0, trials_started=0, accounts_created=0,
        subscription_changes=0,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body
