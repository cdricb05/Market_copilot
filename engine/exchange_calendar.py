"""engine/exchange_calendar.py - Release 60.1 canonical exchange-session calendar.

THE authoritative answer to "did the exchange trade on date D?".

Until R60.1 the operational system had no exchange-holiday calendar at all.
``engine.market_session`` was written to accept one - it takes
``authoritative_non_sessions`` / ``exchange_calendar_available`` and has carried
a fully tested ``NON_SESSION`` branch since Phase 29D.1 - but **nothing ever
supplied it**, so in production those parameters were permanently ``None`` and
the holiday branch was unreachable. The weekday-only expectation therefore named
every exchange holiday a trading session, and the missed-session (catch-up)
projection turned that into a false obligation: on Labor Day 2026-09-07 the
operator surfaces reported "Sep 7, 2026 was not closed / CATCH UP WAITING FOR
OWNED DATA" for a session that never existed.

This module fills exactly that hole and nothing else. It is the SUPPLIER; the
market-session owner remains the only INTERPRETER of what a non-session means
for eligibility, recovery and readiness. Nothing here decides whether a session
is closeable, whether owned data has arrived, or what the operator should do.

PURE, like the owner it feeds: no network, no database, no file IO, no import
side effects, no wall-clock read, and no machine-local timezone. It is a
deterministic function of the Gregorian date alone, so the same date always
yields the same verdict on every machine.

Policy - the published NYSE rules, not an inference
---------------------------------------------------
This is a RULE-BASED calendar. It is not scraped, not vendored and not guessed
from the absence of market data (the absence of owned data is never a holiday -
that remains the market-session owner's Phase 29D.1 invariant). Two sources of
truth are encoded, both of them published facts:

  * the nine/ten annual NYSE holidays and their weekend-observance rule, and
  * an explicit table of the ad-hoc full-day closures the exchange actually
    declared (national days of mourning, 9/11, Hurricane Sandy). Those are not
    derivable from any rule, so they are listed as observed history.

Early closes (the 13:00 sessions before Independence Day, Thanksgiving and
Christmas) are deliberately NOT modelled: an early close IS a trading session,
it produces owned EOD data, and it is closeable. Only FULL-DAY closures matter
to session eligibility.

Supported range
---------------
``EARLIEST_SUPPORTED_YEAR`` (1998, the first year the current holiday set was in
force - MLK Day was added that year) through ``LATEST_SUPPORTED_YEAR``. Outside
that range the calendar reports itself UNAVAILABLE rather than answering, so the
market-session owner degrades to its documented weekday policy instead of acting
on a fabricated verdict. Saying "I do not know" is a supported answer here.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Optional

#: Identity of this calendar, surfaced in the market-session contract so an
#: operator can always see WHICH calendar produced a non-session verdict.
CALENDAR_ID = "NYSE_RULE_BASED_R60_1"
EXCHANGE = "NYSE"

#: The current NYSE holiday set has been in force since 1998 (MLK Day added).
#: Before that the rules differ and this module refuses to answer.
EARLIEST_SUPPORTED_YEAR = 1998
LATEST_SUPPORTED_YEAR = 2099

#: Juneteenth became an NYSE holiday in 2022. Before that it was a trading day.
JUNETEENTH_FIRST_YEAR = 2022

#: Ad-hoc FULL-DAY closures the exchange actually declared. These follow no rule
#: and are therefore recorded as observed history, each with the reason it was
#: closed. A date is listed here ONLY if the NYSE was shut for the whole day.
AD_HOC_CLOSURES: dict[str, str] = {
    "2001-09-11": "September 11 attacks",
    "2001-09-12": "September 11 attacks",
    "2001-09-13": "September 11 attacks",
    "2001-09-14": "September 11 attacks",
    "2004-06-11": "National day of mourning (President Reagan)",
    "2007-01-02": "National day of mourning (President Ford)",
    "2012-10-29": "Hurricane Sandy",
    "2012-10-30": "Hurricane Sandy",
    "2018-12-05": "National day of mourning (President G. H. W. Bush)",
    "2025-01-09": "National day of mourning (President Carter)",
}

_SATURDAY = 5
_SUNDAY = 6
_MONDAY = 0
_THURSDAY = 3


# --------------------------------------------------------------------------- #
# Date coercion (mirrors the market-session owner's tolerance).
# --------------------------------------------------------------------------- #
def _coerce_date(value: Any) -> Optional[date]:
    """date / datetime / ISO-string -> date, else None. Never raises."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# Pure calendar arithmetic.
# --------------------------------------------------------------------------- #
def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """The ``n``-th ``weekday`` (Mon=0) of ``month`` - e.g. 3rd Monday of January."""
    d = date(year, month, 1)
    offset = (weekday - d.weekday()) % 7
    return d + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """The LAST ``weekday`` (Mon=0) of ``month`` - e.g. last Monday of May.

    Anchored on the month's LAST DAY and walked backwards. Anchoring on day 28
    and stepping forward in weeks instead is the classic off-by-one-week trap:
    it lands on the last day in the 28..31 window, which can already be PAST the
    final occurrence of the target weekday (Memorial Day 2022 resolved to May 23
    rather than May 30 that way).
    """
    if month == 12:
        last = date(year, 12, 31)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def easter_sunday(year: int) -> date:
    """Gregorian Easter Sunday (anonymous / Meeus-Jones-Butcher algorithm).

    Needed only to locate Good Friday, the one NYSE holiday with no fixed date
    and no fixed weekday-of-month rule.
    """
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    ell = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ell) // 451
    month, day = divmod(h + ell - 7 * m + 114, 31)
    return date(year, month, day + 1)


def _observed(d: date) -> Optional[date]:
    """Apply the NYSE weekend-observance rule to a fixed-date holiday.

    Saturday -> the preceding Friday; Sunday -> the following Monday; a weekday
    is observed on the day itself.
    """
    if d.weekday() == _SATURDAY:
        return d - timedelta(days=1)
    if d.weekday() == _SUNDAY:
        return d + timedelta(days=1)
    return d


def _observed_new_year(d: date) -> Optional[date]:
    """New Year's Day observance - the ONE exception to ``_observed``.

    When January 1 falls on a Saturday the NYSE does **not** close on the
    preceding Friday (which is December 31 of the prior year); the holiday is
    simply not observed. Sunday still rolls forward to the Monday.
    """
    if d.weekday() == _SATURDAY:
        return None
    return _observed(d)


def holidays_for_year(year: int) -> dict[str, str]:
    """Every FULL-DAY NYSE closure in ``year`` as ``{iso_date: holiday name}``.

    Includes both the rule-based annual holidays (already weekend-adjusted) and
    any ad-hoc closure recorded for that year. Returns ``{}`` for a year outside
    the supported range rather than guessing.
    """
    if not (EARLIEST_SUPPORTED_YEAR <= int(year) <= LATEST_SUPPORTED_YEAR):
        return {}
    year = int(year)
    out: dict[str, str] = {}

    def put(d: Optional[date], name: str) -> None:
        # A None (the unobserved Saturday New Year's Day) is skipped explicitly.
        if d is not None and d.year == year:
            out[d.isoformat()] = name

    put(_observed_new_year(date(year, 1, 1)), "New Year's Day")
    put(_nth_weekday(year, 1, _MONDAY, 3), "Martin Luther King Jr. Day")
    put(_nth_weekday(year, 2, _MONDAY, 3), "Washington's Birthday")
    put(easter_sunday(year) - timedelta(days=2), "Good Friday")
    put(_last_weekday(year, 5, _MONDAY), "Memorial Day")
    if year >= JUNETEENTH_FIRST_YEAR:
        put(_observed(date(year, 6, 19)), "Juneteenth National Independence Day")
    put(_observed(date(year, 7, 4)), "Independence Day")
    put(_nth_weekday(year, 9, _MONDAY, 1), "Labor Day")
    put(_nth_weekday(year, 11, _THURSDAY, 4), "Thanksgiving Day")
    put(_observed(date(year, 12, 25)), "Christmas Day")

    # A New Year's Day that rolls forward from Sunday Jan 1 lands on Jan 2 of the
    # SAME year, so no cross-year fixup is needed.
    for iso, reason in AD_HOC_CLOSURES.items():
        if iso.startswith("%04d-" % year):
            out[iso] = reason
    return out


# --------------------------------------------------------------------------- #
# Public verdicts.
# --------------------------------------------------------------------------- #
def is_supported(value: Any) -> bool:
    """True when this calendar can answer authoritatively for ``value``."""
    d = _coerce_date(value)
    return bool(d is not None
                and EARLIEST_SUPPORTED_YEAR <= d.year <= LATEST_SUPPORTED_YEAR)


def is_weekend(value: Any) -> bool:
    d = _coerce_date(value)
    return bool(d is not None and d.weekday() >= _SATURDAY)


def holiday_name(value: Any) -> Optional[str]:
    """The name of the full-day closure on ``value``, else None.

    Weekends are not holidays and return None - ask ``is_non_session`` for the
    "did the exchange trade?" question.
    """
    d = _coerce_date(value)
    if d is None or not is_supported(d):
        return None
    return holidays_for_year(d.year).get(d.isoformat())


def is_non_session(value: Any) -> bool:
    """True when the exchange did NOT trade on ``value`` (weekend or holiday).

    An unsupported / unparseable date is NOT reported as a non-session: this
    module never converts "I cannot answer" into "the market was shut".
    """
    d = _coerce_date(value)
    if d is None or not is_supported(d):
        return False
    return bool(d.weekday() >= _SATURDAY or holiday_name(d) is not None)


def non_sessions_between(start: Any, end: Any, *,
                         include_weekends: bool = False) -> tuple[str, ...]:
    """The ordered ISO dates in ``[start, end]`` the exchange did not trade.

    By default ONLY holidays are returned. The market-session owner already
    handles weekends in its own weekday arithmetic, and handing it weekend dates
    as "authoritative non-sessions" would make every Saturday appear in the
    operator-facing contract as a declared exchange closure.

    Returns ``()`` for an unusable or inverted range, and covers only the part of
    the range this calendar supports.
    """
    a, b = _coerce_date(start), _coerce_date(end)
    if a is None or b is None or b < a:
        return ()
    out: list[str] = []
    for year in range(a.year, b.year + 1):
        for iso in sorted(holidays_for_year(year)):
            d = date.fromisoformat(iso)
            if a <= d <= b:
                out.append(iso)
    if include_weekends:
        cur = a
        while cur <= b:
            if cur.weekday() >= _SATURDAY and is_supported(cur):
                out.append(cur.isoformat())
            cur += timedelta(days=1)
        out = sorted(set(out))
    return tuple(out)


def calendar_available_between(start: Any, end: Any) -> bool:
    """True when the WHOLE range lies inside the supported years.

    This is what the market-session owner's ``exchange_calendar_available`` flag
    must be set from: a partially covered range is not an authoritative calendar.
    """
    a, b = _coerce_date(start), _coerce_date(end)
    if a is None or b is None or b < a:
        return False
    return bool(is_supported(a) and is_supported(b))


def describe() -> dict[str, Any]:
    """Machine-readable identity of this calendar for provenance surfaces."""
    return {
        "calendar_id": CALENDAR_ID,
        "exchange": EXCHANGE,
        "policy": "PUBLISHED_NYSE_HOLIDAY_RULES_PLUS_DECLARED_AD_HOC_CLOSURES",
        "earliest_supported_year": EARLIEST_SUPPORTED_YEAR,
        "latest_supported_year": LATEST_SUPPORTED_YEAR,
        "juneteenth_first_year": JUNETEENTH_FIRST_YEAR,
        "ad_hoc_closure_count": len(AD_HOC_CLOSURES),
        "models_early_closes": False,
        "early_close_note": ("An early close IS a trading session and produces "
                             "owned EOD data; only full-day closures are "
                             "non-sessions."),
        "infers_nothing_from_missing_market_data": True,
    }


__all__ = [
    "CALENDAR_ID",
    "EXCHANGE",
    "EARLIEST_SUPPORTED_YEAR",
    "LATEST_SUPPORTED_YEAR",
    "JUNETEENTH_FIRST_YEAR",
    "AD_HOC_CLOSURES",
    "easter_sunday",
    "holidays_for_year",
    "is_supported",
    "is_weekend",
    "holiday_name",
    "is_non_session",
    "non_sessions_between",
    "calendar_available_between",
    "describe",
]
