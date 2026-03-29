"""
engine/market_hours.py — US/Eastern market-hours utilities.

All functions are pure (no I/O, no DB, no side effects) and depend only on
the standard library. No holiday calendar is included; only weekday + regular-
session window checks are supported.

Regular session: 09:30–16:00 US/Eastern, Monday–Friday.
"""
from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")

_MARKET_OPEN  = time(9, 30)
_MARKET_CLOSE = time(16, 0)


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
