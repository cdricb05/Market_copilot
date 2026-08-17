"""
engine/market_hours.py — US/Eastern market-hours utilities.

All functions are pure (no I/O, no DB, no side effects) and depend only on
the standard library. No holiday calendar is included; only weekday + regular-
session window checks are supported.

Regular session: 09:30–16:00 US/Eastern, Monday–Friday.
Extended phases (Release 29): pre-market 04:00–09:30, post-market 16:00–20:00.

This module is the LOW-LEVEL market-clock primitive the canonical
``engine.market_session`` owner is built on. Release 29 added ``session_state``
so the continuous collection service can ask ONE owner "what phase is the market
in right now?" instead of doing its own weekday/time arithmetic — a market feed
must read NOT_DUE on a Sunday, never STALE.
"""
from __future__ import annotations

from datetime import date, datetime, time
from typing import Any, Optional
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")

_MARKET_OPEN  = time(9, 30)
_MARKET_CLOSE = time(16, 0)
_PREMARKET_OPEN  = time(4, 0)
_POSTMARKET_CLOSE = time(20, 0)

# Session phases (Release 29). CLOSED is a weekday outside every extended window;
# WEEKEND is Saturday/Sunday. There is deliberately no HOLIDAY phase: this repo
# has no authoritative exchange-holiday calendar, and inventing one would be a
# fabricated fact. A holiday therefore presents as a weekday on which the market
# feeds simply return nothing new, which the collection cadence records honestly.
PHASE_WEEKEND = "WEEKEND"
PHASE_PREMARKET = "PREMARKET"
PHASE_OPEN = "REGULAR_OPEN"
PHASE_POSTMARKET = "POSTMARKET"
PHASE_CLOSED = "CLOSED"
SESSION_PHASES = (PHASE_WEEKEND, PHASE_PREMARKET, PHASE_OPEN, PHASE_POSTMARKET,
                  PHASE_CLOSED)

CALENDAR_POLICY = "WEEKDAY_EXTENDED_HOURS_NO_HOLIDAY_CALENDAR"


def to_eastern(dt: datetime) -> datetime:
    """
    Return dt converted to US/Eastern.

    If dt is timezone-aware it is converted directly.
    If dt is naive it is assumed to already represent US/Eastern wall-clock
    time and is returned with the US/Eastern tzinfo attached (no offset shift).
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=_ET)
    return dt.astimezone(_ET)


def is_weekday(dt: datetime) -> bool:
    """
    Return True if dt (converted to US/Eastern) falls on Mon–Fri.
    Saturday = weekday() 5, Sunday = weekday() 6.
    """
    return to_eastern(dt).weekday() < 5


def is_market_open(dt: datetime) -> bool:
    """
    Return True if dt falls within regular market hours:
    09:30:00 <= t < 16:00:00 US/Eastern on a weekday.

    Does NOT account for NYSE holidays.
    """
    et = to_eastern(dt)
    if et.weekday() >= 5:
        return False
    t = et.time().replace(tzinfo=None)
    return _MARKET_OPEN <= t < _MARKET_CLOSE


def market_date_for(dt: datetime) -> date:
    """
    Return the US/Eastern calendar date for dt.

    This is the date used to bucket signals, orders, and trade decisions —
    i.e. the Eastern wall-clock date at the moment the event occurred,
    regardless of whether the market was actually open.
    """
    return to_eastern(dt).date()


def session_phase(dt: datetime) -> str:
    """Return the extended-hours session phase at ``dt`` (US/Eastern).

    WEEKEND / PREMARKET / REGULAR_OPEN / POSTMARKET / CLOSED. Does NOT account
    for NYSE holidays — this repo has no authoritative holiday calendar and does
    not fabricate one.
    """
    et = to_eastern(dt)
    if et.weekday() >= 5:
        return PHASE_WEEKEND
    t = et.time().replace(tzinfo=None)
    if t < _PREMARKET_OPEN:
        return PHASE_CLOSED
    if t < _MARKET_OPEN:
        return PHASE_PREMARKET
    if t < _MARKET_CLOSE:
        return PHASE_OPEN
    if t < _POSTMARKET_CLOSE:
        return PHASE_POSTMARKET
    return PHASE_CLOSED


def session_state(dt: datetime) -> dict[str, Any]:
    """THE canonical market-clock fact block for a moment in time.

    One owner answers every market-clock question the continuous collection
    service asks, so no caller performs its own weekday or cutoff arithmetic.
    Pure: no I/O, no ambient clock — the caller supplies ``dt``.
    """
    et = to_eastern(dt)
    phase = session_phase(dt)
    t = et.time().replace(tzinfo=None)
    return {
        "calculation_owner": "engine.market_hours",
        "calendar_policy": CALENDAR_POLICY,
        "evaluated_at_et": et.isoformat(),
        "et_date": et.date().isoformat(),
        "et_time": t.isoformat(timespec="minutes"),
        "et_minutes": et.hour * 60 + et.minute,
        "weekday": et.weekday(),
        "is_weekday": et.weekday() < 5,
        "phase": phase,
        "session_phases": list(SESSION_PHASES),
        "regular_session_open": phase == PHASE_OPEN,
        "extended_hours_open": phase in (PHASE_PREMARKET, PHASE_OPEN,
                                         PHASE_POSTMARKET),
        "regular_open_et": _MARKET_OPEN.isoformat(timespec="minutes"),
        "regular_close_et": _MARKET_CLOSE.isoformat(timespec="minutes"),
        "holiday_calendar_available": False,
        "note": ("Weekday + extended-hours windows only. No exchange-holiday "
                 "calendar is available, so a holiday presents as a weekday on "
                 "which the market feeds return nothing new rather than as a "
                 "fabricated HOLIDAY phase."),
    }


def parse_et(value: Any) -> Optional[datetime]:
    """Parse an ISO timestamp into an aware datetime, or None. Never raises."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
