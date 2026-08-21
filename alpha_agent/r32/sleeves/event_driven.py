"""EVENT_DRIVEN - what this project can honestly study, and what it cannot.

The obvious event sleeve would trade earnings surprises, analyst revisions and
corporate actions. This project cannot build it, and the reason is worth stating
plainly because the temptation to build it anyway is the exact failure mode the
Information Purchase Gate exists to prevent:

* the owned ``earnings`` store carries ``provider_id: synthetic_test`` and
  tickers named ``S000`` - it is a TEST FIXTURE, not a measurement;
* the owned ``analyst_revision`` store is ``PROXY_LOCAL``, derived, and equally
  unusable as history;
* the owned SEC filing timestamps are genuinely point-in-time - a filing
  acceptance time is exactly when the world learned - but cover 63 tickers.

Sixty-three names cannot support a cross-sectional conclusion about the S&P 500,
and a synthetic fixture cannot support any conclusion at all. Fabricating the
missing history would produce a confident, meaningless answer.

So this sleeve studies the ONE event family that is fully owned, fully
point-in-time and costs nothing: DETERMINISTIC CALENDAR STRUCTURE. Turn of
month, quarter boundaries and triple witching are computable from the date
itself. Nobody needs a data feed to know that the third Friday of March 2011 was
a witching day, and nothing about that knowledge is hindsight.

This is a genuine, testable event hypothesis. It is also a deliberately narrow
one, and the corporate-event gap it leaves is escalated to the purchase frontier
rather than filled with a proxy.
"""
from __future__ import annotations

import numpy as np

from .. import contract as _contract
from .. import panels as _panels
from ..sleeve import (
    DIRECTION_FLAT,
    DIRECTION_LONG,
    SleeveSpec,
    StrategyOpportunity,
)

SLEEVE = _contract.SLEEVE_EVENT_DRIVEN
PANEL = _panels.PANEL_CALENDAR

#: What this sleeve would need in order to study corporate events, and does not
#: own. Consumed by :mod:`alpha_agent.r32.purchase_gate`.
UNOWNED_EVENT_REQUIREMENTS = (
    {"requirement": "point-in-time earnings announcement dates and surprises",
     "owned_state": "SYNTHETIC_TEST_FIXTURE",
     "evidence": "provider_id=synthetic_test, tickers S000..S039, 960 rows"},
    {"requirement": "historical analyst estimate revisions with vintages",
     "owned_state": "LOCAL_PROXY_NOT_A_MEASUREMENT",
     "evidence": "provider_id=PROXY_LOCAL, derived from the same fixture"},
    {"requirement": "broad corporate filing timestamps",
     "owned_state": "COVERAGE_TOO_NARROW",
     "evidence": "SEC filing timestamps for 63 tickers"},
)

#: Every event family here is computed from the calendar alone.
CALENDAR_EVENTS = ("turn_of_month", "quarter_end", "triple_witching",
                   "santa_window")


def _opportunity(date: str, weights: dict, rationale: str,
                 state: dict) -> StrategyOpportunity:
    return StrategyOpportunity(
        sleeve=SLEEVE, decision_date=date,
        direction=DIRECTION_LONG if weights else DIRECTION_FLAT,
        conviction=float(sum(weights.values())),
        recommended_exposure=weights, rationale=rationale,
        state_variables=state)


def _event_instrument(event: str) -> str:
    return f"EVENT_{event.upper()}"


# --------------------------------------------------------------------------- #
# Families
# --------------------------------------------------------------------------- #
def gen_single_event(panel: dict, idx: list, params: dict) -> list:
    """Be invested only on one calendar event's sessions, cash otherwise."""
    dates = panel["dates"]
    event = str(params["event"])
    inst = _event_instrument(event)
    return [_opportunity(dates[i], {inst: 1.0},
                         f"invested only on {event} sessions", {"event": event})
            for i in idx]


def gen_event_pair(panel: dict, idx: list, params: dict) -> list:
    """Split capital across two calendar event windows."""
    dates = panel["dates"]
    a, b = str(params["event_a"]), str(params["event_b"])
    w = {_event_instrument(a): 0.5, _event_instrument(b): 0.5}
    return [_opportunity(dates[i], dict(w), f"split between {a} and {b}",
                         {"events": [a, b]}) for i in idx]


def gen_always_invested_control(panel: dict, idx: list, params: dict) -> list:
    """The control: hold the index every session.

    Without this, a calendar rule that is invested 40 % of the time and earns
    less than buy-and-hold could still look like a discovery. It is the same
    question the whole frontier asks - compared with what?
    """
    dates = panel["dates"]
    return [_opportunity(dates[i], {"EVENT_ALWAYS": 1.0},
                         "invested every session (control)", {})
            for i in idx]


FAMILIES = {
    "single_event": gen_single_event,
    "event_pair": gen_event_pair,
    "always_invested_control": gen_always_invested_control,
}


def screening_specs() -> list:
    specs = [SleeveSpec(sleeve=SLEEVE, family="single_event",
                        params={"event": e}, generate=gen_single_event)
             for e in CALENDAR_EVENTS]
    specs.append(SleeveSpec(sleeve=SLEEVE, family="event_pair",
                            params={"event_a": "turn_of_month",
                                    "event_b": "santa_window"},
                            generate=gen_event_pair))
    specs.append(SleeveSpec(sleeve=SLEEVE, family="event_pair",
                            params={"event_a": "turn_of_month",
                                    "event_b": "triple_witching"},
                            generate=gen_event_pair))
    specs.append(SleeveSpec(sleeve=SLEEVE, family="always_invested_control",
                            params={}, generate=gen_always_invested_control,
                            is_control=True))
    return specs


def qualification_specs(families: list) -> list:
    grids = {
        "single_event": [{"event": e} for e in CALENDAR_EVENTS],
        "event_pair": [{"event_a": a, "event_b": b}
                       for a in ("turn_of_month",)
                       for b in ("quarter_end", "triple_witching",
                                 "santa_window")],
        "always_invested_control": [{}],
    }
    out = []
    for fam in families:
        for p in grids.get(fam, [])[:_contract.QUALIFICATION_MAX_CONFIGS_PER_FAMILY]:
            out.append(SleeveSpec(sleeve=SLEEVE, family=fam, params=p,
                                  generate=FAMILIES[fam],
                                  stage=_contract.STAGE_QUALIFICATION,
                                  is_control=(fam == "always_invested_control")))
    return out


def instrument_returns(panel: dict, idx: list) -> dict:
    """Hold-window return of each event window, earning cash when not invested.

    This is what makes a within-month effect measurable by the SAME monthly
    judge every other sleeve faces, instead of needing a second judge that would
    make the comparison meaningless.
    """
    dates = panel["dates"]
    lv = panel["columns"]["BENCHMARK"]
    cash_y = panel["columns"]["CASH_YIELD"]
    cash_daily = _panels.cash_returns(cash_y, dates)
    cal = panel["calendar"]
    hold = _contract.HOLD_SESSIONS
    always = np.ones(len(dates), dtype=bool)
    out = {}
    for i in idx:
        row = {"EVENT_ALWAYS": _panels.masked_hold_return(
            lv, always, i, hold, cash_daily=cash_daily)}
        for e in CALENDAR_EVENTS:
            mask = cal[e].astype(bool)
            row[_event_instrument(e)] = _panels.masked_hold_return(
                lv, mask, i, hold, cash_daily=cash_daily)
        out[dates[i]] = row
    return out
