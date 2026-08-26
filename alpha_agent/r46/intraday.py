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


def probe(campaign_id: str = CAMPAIGN_ID, live_probe: bool = True) -> dict:
    """Probe every candidate intraday source and persist the lane artifact."""
    sources = [{
        "source": "norgate_ndu",
        "state": "DAILY_ONLY",
        "current_intraday": False,
        "detail": "NDU serves daily bars; Release 45 measured that the "
                  "vendor API ignores an interval argument entirely, so "
                  "there is no intraday capability to enable",
    }, {
        "source": "historical_intraday_panels_r38_r45",
        "state": "HISTORICAL_ONLY",
        "current_intraday": False,
        "detail": "acquired minute panels are frozen history: they can "
                  "nominate an intraday challenger and can never emit a "
                  "forward row, because a forward row needs a CURRENT bar",
    }]
    if live_probe:
        sources.append(_polygon_probe())
    else:
        sources.append({"source": "polygon", "state": "NOT_PROBED",
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
        exact_blocker=(None if available else
                       "no owned source serves a bar for the CURRENT session "
                       "intraday; Norgate is daily by construction and the "
                       "owned venue plan is end-of-day"),
        a_blocked_lane_stops_nothing_else=True,
        historical_files_are_not_a_feed=True,
        money_spent_usd=0.0,
        purchases=0, trials_started=0, accounts_created=0,
        subscription_changes=0,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body
