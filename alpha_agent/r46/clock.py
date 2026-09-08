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

RELEASE 62.1 - THE SCHEDULING ESTIMATE STOPS COUNTING BARE WEEKDAYS
------------------------------------------------------------------
:func:`expected_maturity_date` is the CALENDAR ESTIMATE the boards schedule
from (``horizon_end_expected``). It counted weekdays, so on 2026-09-07 - Labor
Day, a full NYSE closure - the live board advertised a maturity on a session
that does not exist. It now accepts an AUTHORITATIVE non-session set and skips
those dates too. This module still owns no holiday table: the set is supplied by
:mod:`engine.exchange_calendar` through
:func:`api.forward_challenger_registry.observation_calendar_for`, which is the
ONE place that decides whether an asset class even keeps exchange sessions.
Called without a set the behaviour is byte-for-byte what it always was, so every
existing R46 row and every non-equity instrument - which legitimately trades on
days the NYSE is shut - is unchanged.
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
    """The FROZEN whole-second stamp. Never widened.

    Release 46's eleven prediction rows and the challenger registry that froze them
    are stamped with this format. Changing it would change their bytes and their
    chain hashes, so it stays exactly as it was; :func:`iso_precise` carries the
    extra resolution for rows written from Release 46.2 onward.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    return dt.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_precise(dt: _dt.datetime) -> str:
    """Microsecond UTC stamp, for FUTURE rows only (Release 46.2).

    Release 46.1 disclosed that a challenger's ``frozen_at`` and its first
    prediction's ``emitted_at_utc`` shared one whole second, so "the specification
    was frozen before the prediction was emitted" could be argued but not COMPUTED.
    Rows written from R46.2 onward carry this alongside the frozen whole-second
    field, which keeps every existing hash and every existing reader intact while
    making the ordering numerically decidable. A row without it is a legacy row and
    is read exactly as before - see :func:`ordering_evidence`.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    return dt.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_iso(value) -> Optional[_dt.datetime]:
    """Parse either stamp format (or any ISO instant). ``None`` when unparseable."""
    if isinstance(value, _dt.datetime):
        return (value if value.tzinfo else value.replace(tzinfo=_dt.timezone.utc))
    s = str(value or "").strip()
    if not s:
        return None
    try:
        return _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def ordering_evidence(earlier, later) -> dict:
    """Can ``earlier < later`` be decided NUMERICALLY, and does it hold?

    ``resolution`` is ``MICROSECOND`` when both stamps carry sub-second precision,
    ``WHOLE_SECOND`` otherwise. ``decidable`` is False only when both sides land in
    the same whole second with no sub-second digits to separate them - which is the
    exact R46 first-batch condition, reported honestly rather than asserted away.
    """
    a, b = parse_iso(earlier), parse_iso(later)
    if a is None or b is None:
        return {"decidable": False, "strictly_ordered": None,
                "resolution": "UNPARSEABLE", "delta_seconds": None,
                "earlier": earlier, "later": later}
    sub = ("." in str(earlier)) and ("." in str(later))
    delta = (b - a).total_seconds()
    same_second = (a.replace(microsecond=0) == b.replace(microsecond=0))
    return {
        "decidable": bool(sub or not same_second),
        "strictly_ordered": bool(delta > 0),
        "resolution": "MICROSECOND" if sub else "WHOLE_SECOND",
        "delta_seconds": delta,
        "earlier": earlier,
        "later": later,
    }


def to_eastern(dt: _dt.datetime) -> _dt.datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    if _ET is None:                             # pragma: no cover
        return dt - _dt.timedelta(hours=4)
    return dt.astimezone(_ET)


def eastern_date(dt: _dt.datetime) -> _dt.date:
    return to_eastern(dt).date()


def next_weekday(d: _dt.date, non_sessions=None) -> _dt.date:
    """The next weekday after ``d``, skipping any AUTHORITATIVE non-session.

    ``non_sessions`` is an iterable of ISO dates supplied by the canonical
    exchange-calendar owner. ``None`` keeps the frozen weekday-only rule
    exactly as it was - which is the correct answer for every market that is
    NOT on the NYSE session calendar, and the only answer available when the
    authoritative calendar cannot cover the range.
    """
    skip = {str(x)[:10] for x in (non_sessions or ())}
    n = d + _dt.timedelta(days=1)
    guard = 0
    while (n.weekday() in WEEKEND or n.isoformat() in skip) and guard < 40:
        n += _dt.timedelta(days=1)
        guard += 1
    return n


def exchange_non_sessions(asset_class, start: _dt.date, end: _dt.date):
    """The authoritative non-sessions for ``asset_class`` in ``[start, end]``.

    Delegation, not a calendar. It asks the ONE forward-evidence owner whether
    this asset class keeps exchange sessions at all and, if it does, asks the
    ONE supplier for that range's full-day closures. Any other asset class - and
    any range the supplier cannot cover - yields ``None``, which means "keep the
    weekday estimate", never "assume a holiday".
    """
    try:
        from paper_trader.api import forward_challenger_registry as FCR
        cal = FCR.observation_calendar_for(asset_class)
        if not cal.get("is_exchange_session_calendar"):
            return None
        from paper_trader.engine import exchange_calendar as EC
        if not EC.calendar_available_between(start, end):
            return None
        return EC.non_sessions_between(start, end)
    except Exception:                               # noqa: BLE001 - fail soft
        return None


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


def expected_maturity_date(entry_date: _dt.date, horizon: int,
                           non_sessions=None) -> _dt.date:
    """A calendar ESTIMATE of maturity, for scheduling only.

    Counts ``horizon`` sessions after ``entry_date``, skipping weekends and -
    when an AUTHORITATIVE ``non_sessions`` set is supplied - full-day exchange
    closures. Never used to score: the judge always counts the instrument's own
    realised sessions. Without a set the rule is the frozen weekday count, so
    non-equity instruments and legacy callers are unchanged.
    """
    d = entry_date
    for _ in range(int(horizon)):
        d = next_weekday(d, non_sessions)
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
    "now_utc", "iso", "iso_precise", "parse_iso", "ordering_evidence",
    "to_eastern", "eastern_date", "next_weekday", "exchange_non_sessions",
    "entry_session_date", "outcome_window_start_utc", "is_true_forward",
    "resolve_entry", "maturity_session", "expected_maturity_date",
    "sessions_remaining", "session_index", "CALCULATION_OWNER",
]
