"""alpha_agent.r46.clock - when a prediction may be made, and when it matures.

The whole release turns on two instants being unambiguously ordered:

    emitted_at_utc   <   outcome_window_start_utc

:func:`entry_session_date` implements the frozen
:data:`alpha_agent.r46.contract.ENTRY_RULE`: the first trading day strictly
after the emission's Eastern calendar date. It never consults a price, so it
cannot be argued into look-ahead by a fast market.

:func:`maturity_session` then counts horizon sessions on the instrument's OWN
realised bar calendar - the dates it actually printed - so mixed calendars,
holidays and market-specific closures are handled by observation rather than
by an assumed holiday table.
"""
from __future__ import annotations

import datetime as _dt
from typing import Iterable, Optional, Sequence

try:                                            # py>=3.9
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except Exception:                               # pragma: no cover
    _ET = None

CALCULATION_OWNER = "alpha_agent.r46.clock"

WEEKEND = (5, 6)


def now_utc() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def iso(dt: _dt.datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    return dt.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_eastern(dt: _dt.datetime) -> _dt.datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    if _ET is None:                             # pragma: no cover
        return dt - _dt.timedelta(hours=4)
    return dt.astimezone(_ET)


def eastern_date(dt: _dt.datetime) -> _dt.date:
    return to_eastern(dt).date()


def next_weekday(d: _dt.date) -> _dt.date:
    n = d + _dt.timedelta(days=1)
    while n.weekday() in WEEKEND:
        n += _dt.timedelta(days=1)
    return n


def entry_session_date(emitted_at: _dt.datetime) -> _dt.date:
    """The frozen entry rule: first weekday STRICTLY after the ET date of T.

    Weekends are skipped because no venue in this tournament prints a bar on
    one. Holidays are NOT assumed away here - if the resulting date turns out
    not to be an eligible session for a given instrument, that instrument's
    entry simply resolves forward to its next realised bar, and the
    resolution is recorded on the outcome row.
    """
    return next_weekday(eastern_date(emitted_at))


def outcome_window_start_utc(entry_date: _dt.date) -> _dt.datetime:
    """The instant the outcome window opens: midnight EASTERN on the entry day.

    It must be Eastern, not UTC. The entry rule is stated on the Eastern
    calendar, and midnight UTC is 8pm Eastern on the PREVIOUS day - so a
    UTC-midnight window would open BEFORE an emission made at, say, 20:30 ET,
    and the strict ordering the whole release rests on would fail for every
    evening emission. Anchoring the window to 00:00 ET of the entry date makes
    the ordering hold by construction: an emission whose Eastern date is
    strictly earlier than the entry date is necessarily earlier than the
    entry date's first instant.

    Used only for the ordering assertion. It never prices anything.
    """
    if _ET is None:                             # pragma: no cover
        return _dt.datetime(entry_date.year, entry_date.month, entry_date.day,
                            4, 0, tzinfo=_dt.timezone.utc)
    midnight_et = _dt.datetime(entry_date.year, entry_date.month,
                               entry_date.day, 0, 0, tzinfo=_ET)
    return midnight_et.astimezone(_dt.timezone.utc)


def is_true_forward(emitted_at: _dt.datetime, entry_date: _dt.date) -> bool:
    return emitted_at < outcome_window_start_utc(entry_date)


def resolve_entry(sessions: Sequence, entry_date: _dt.date) -> Optional[object]:
    """First eligible session on or after ``entry_date``.

    ``sessions`` is the instrument's own realised bar-date index, ascending.
    """
    for s in sessions:
        sd = _as_date(s)
        if sd is not None and sd >= entry_date:
            return s
    return None


def maturity_session(sessions: Sequence, entry_date: _dt.date,
                     horizon: int) -> Optional[object]:
    """The close ``horizon`` eligible sessions after the entry close."""
    idx = _index_of_first_on_or_after(sessions, entry_date)
    if idx is None:
        return None
    j = idx + int(horizon)
    if j >= len(sessions):
        return None
    return sessions[j]


def expected_maturity_date(entry_date: _dt.date, horizon: int) -> _dt.date:
    """A calendar ESTIMATE of maturity, for scheduling only.

    Counts ``horizon`` weekdays after ``entry_date``. Never used to score -
    the judge always counts realised sessions.
    """
    d = entry_date
    for _ in range(int(horizon)):
        d = next_weekday(d)
    return d


def sessions_remaining(sessions: Sequence, entry_date: _dt.date,
                       horizon: int) -> Optional[int]:
    """How many more eligible sessions until this prediction matures."""
    idx = _index_of_first_on_or_after(sessions, entry_date)
    if idx is None:
        return None
    have = len(sessions) - 1 - idx
    return max(0, int(horizon) - have)


# --------------------------------------------------------------------------- #
def _as_date(value) -> Optional[_dt.date]:
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    try:
        return _dt.date.fromisoformat(str(value)[:10])
    except Exception:
        return None


def _index_of_first_on_or_after(sessions: Sequence,
                                d: _dt.date) -> Optional[int]:
    for i, s in enumerate(sessions):
        sd = _as_date(s)
        if sd is not None and sd >= d:
            return i
    return None


def session_index(sessions: Iterable) -> list:
    return [_as_date(s) for s in sessions]


__all__ = [
    "now_utc", "iso", "to_eastern", "eastern_date", "next_weekday",
    "entry_session_date", "outcome_window_start_utc", "is_true_forward",
    "resolve_entry", "maturity_session", "expected_maturity_date",
    "sessions_remaining", "session_index", "CALCULATION_OWNER",
]
