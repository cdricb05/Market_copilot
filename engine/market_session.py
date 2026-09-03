"""engine/market_session.py — Slice 1 canonical market-session domain.

PURE domain module (Consolidation Roadmap Slice 1; TARGET_ARCHITECTURE context
"Market Session and Data Freshness"). It owns ALL *current-session* calendar and
cutoff arithmetic for the operational system. It performs **no** network calls,
**no** database or file IO, has **no** application-startup side effects, accepts
its clock and data dates explicitly, and is deterministic for the same inputs.
It never reads the machine-local timezone: every instant is interpreted through
``zoneinfo.ZoneInfo("America/New_York")``.

It distinguishes three dates the rest of the system historically conflated:

  * ``expected_completed_market_date``   — the most recent session the clock /
    calendar POLICY expects could have completed.
  * ``latest_confirmed_owned_data_date`` — the most recent completed session
    OWNED market data (or persisted owned-data state) actually confirms.
  * ``eligible_market_date``             — the most recent session that is BOTH
    expected-complete AND confirmed by owned data.

Policy (see docs/CURRENT_ARCHITECTURE.md §Market Session):

  * There is no installed exchange-holiday calendar (only ``pytz`` / ``tzdata``).
    The weekday + close-cutoff calculation is therefore a calendar *expectation*
    only, explicitly labelled ``WEEKDAY_CUTOFF_NO_HOLIDAYS``.
  * Owned-provider-confirmed sessions are the holiday-safe authority. A weekday
    calculation NEVER overrides an owned-provider-confirmed date. A provider date
    *newer* than the calendar expectation is rejected as inconsistent, never
    silently accepted.

Two operational close policies coexist today and are preserved as an explicit,
configurable ``close_cutoff_et`` parameter rather than hard-coded in callers:

  * World A / legacy Daily Operating Run — 16:00 ET regular close.
  * World B / operational Daily Close    — 17:30 ET post-close safety cutoff.

This module deliberately does NOT own historical evidence calendars
(``forward_prediction_skill.eligible_calendar`` — the recorded PAST completed
sessions used for TRUE_FORWARD maturation). That is a semantically distinct
concept and is intentionally left separate.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional

from paper_trader.engine import market_hours as _mh

_ET = _mh._ET  # zoneinfo.ZoneInfo("America/New_York")

# Regular US equity session close and the operational post-close safety cutoff.
REGULAR_CLOSE_ET = time(16, 0)
DEFAULT_CLOSE_CUTOFF_ET = time(17, 30)

# Frozen calendar / confirmation policy labels (part of the tested contract).
CALENDAR_POLICY = "WEEKDAY_CUTOFF_NO_HOLIDAYS"
CONFIRMATION_SOURCE = "OWNED_EOD_PROVIDER_CONFIRMED_SESSIONS"

# Frozen session-status vocabulary (part of the tested contract).
BEFORE_SESSION_CLOSE = "BEFORE_SESSION_CLOSE"
EXPECTED_SESSION_COMPLETE = "EXPECTED_SESSION_COMPLETE"
WAITING_FOR_OWNED_DATA = "WAITING_FOR_OWNED_DATA"
SESSION_READY = "SESSION_READY"
NO_CONFIRMED_DATA = "NO_CONFIRMED_DATA"
# CALENDAR_POLICY_DEGRADED is now a POLICY FLAG (see ``calendar_policy_degraded``),
# NOT a ready-holiday state. It is retained in the vocabulary for compatibility but
# a weekday is NEVER classified a non-session merely because same-day owned data is
# absent (Phase 29D.1 live-acceptance correction). A true non-session requires an
# AUTHORITATIVE source and yields ``NON_SESSION`` (below).
CALENDAR_POLICY_DEGRADED = "CALENDAR_POLICY_DEGRADED"
# An AUTHORITATIVE non-session (exchange holiday / provider-confirmed non-session):
# the expected weekday did NOT trade. Only reachable via an authoritative calendar
# or a persisted provider-confirmed non-session contract — never inferred from the
# absence of same-day owned data.
NON_SESSION = "NON_SESSION"
INCONSISTENT_FUTURE_DATA = "INCONSISTENT_FUTURE_DATA"

SESSION_STATUSES = (
    BEFORE_SESSION_CLOSE,
    EXPECTED_SESSION_COMPLETE,
    WAITING_FOR_OWNED_DATA,
    SESSION_READY,
    NO_CONFIRMED_DATA,
    CALENDAR_POLICY_DEGRADED,
    NON_SESSION,
    INCONSISTENT_FUTURE_DATA,
)

# Short machine-readable weakest-gate tokens.
GATE_NONE = "NONE"
GATE_SESSION_NOT_CLOSED = "SESSION_NOT_CLOSED"
GATE_OWNED_DATA_LAG = "OWNED_DATA_LAG"
GATE_NO_OWNED_DATA = "NO_OWNED_DATA"
GATE_INCONSISTENT_OWNED_DATA = "INCONSISTENT_OWNED_DATA"


# --------------------------------------------------------------------------- #
# Pure calendar primitives (the ONLY home for this arithmetic).
# --------------------------------------------------------------------------- #
def to_eastern(dt: datetime) -> datetime:
    """Return ``dt`` in US/Eastern (delegates to engine.market_hours.to_eastern).

    A naive datetime is assumed to already be US/Eastern wall-clock time; an aware
    datetime is converted. Never consults the machine-local timezone.
    """
    return _mh.to_eastern(dt)


def format_operator_timestamp(value: Any, *, with_date: bool = True
                              ) -> Optional[str]:
    """Release 55 — an owner-stamped instant, spelled for a human operator.

    The operator reads the market in Eastern time; the stores record UTC. Before
    R55 every operator surface printed the raw UTC stamp, so reading "what
    happened since the decision?" meant a mental timezone conversion. The
    formatting lives HERE because this module already owns the Eastern clock
    (``to_eastern``) — no surface, and no browser, converts a timezone of its own.

    Returns e.g. ``"Sep 3, 12:26 PM ET"`` (or ``"12:26 PM ET"`` when
    ``with_date`` is False), a bare ISO date unchanged when ``value`` carries no
    time, and None when ``value`` is absent or unparseable. Never substitutes
    "now" for a missing stamp.
    """
    if value in (None, ""):
        return None
    raw = str(value).strip()
    if len(raw) == 10:  # a market DATE, not an instant — never given a clock time
        try:
            d = date.fromisoformat(raw)
        except ValueError:
            return None
        # Built by parts, not by strftime day/hour padding: "%-d" is not
        # portable to Windows and "%d" would print "Sep 03".
        return "%s %d, %d" % (d.strftime("%b"), d.day, d.year)
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    et = to_eastern(dt)
    clock = "%d:%02d %s ET" % (((et.hour % 12) or 12), et.minute,
                               "AM" if et.hour < 12 else "PM")
    if not with_date:
        return clock
    return "%s %d, %s" % (et.strftime("%b"), et.day, clock)


def walk_back_to_trading_day(d: date) -> date:
    """Return ``d`` moved back to the nearest weekday (Mon–Fri) at or before it.

    Weekday-only: there is no holiday calendar. Saturday/Sunday resolve to the
    prior Friday; a weekday is returned unchanged.
    """
    while d.weekday() >= 5:  # 5 = Sat, 6 = Sun
        d -= timedelta(days=1)
    return d


def previous_trading_day(d: date) -> date:
    """Return the most recent weekday strictly before ``d`` (weekday-only)."""
    return walk_back_to_trading_day(d - timedelta(days=1))


def next_trading_day(d: date) -> date:
    """Return the nearest weekday strictly after ``d`` (weekday-only)."""
    d += timedelta(days=1)
    while d.weekday() >= 5:  # 5 = Sat, 6 = Sun
        d += timedelta(days=1)
    return d


def _coerce_date(value: Any) -> Optional[date]:
    """Best-effort date coercion (date/datetime/ISO-string) → date, else None."""
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


def _coerce_cutoff(value: Any) -> time:
    if isinstance(value, time):
        return value
    if isinstance(value, str):
        parts = value.split(":")
        try:
            return time(int(parts[0]), int(parts[1]) if len(parts) > 1 else 0)
        except (TypeError, ValueError):
            return DEFAULT_CLOSE_CUTOFF_ET
    return DEFAULT_CLOSE_CUTOFF_ET


def _norm_non_sessions(*values: Any) -> frozenset[str]:
    """Normalize one or more authoritative non-session inputs to a set of ISO date
    strings. Accepts an iterable of date/datetime/ISO-string values (or None).

    These are the ONLY inputs that may classify a weekday as a non-session: an
    authoritative exchange calendar or a persisted provider-confirmed non-session
    contract. Absence of same-day owned data is NEVER a non-session (Phase 29D.1).
    """
    out: set[str] = set()
    for value in values:
        if value is None:
            continue
        if isinstance(value, (str, date, datetime)):
            items: Any = [value]
        else:
            try:
                items = list(value)
            except TypeError:
                items = [value]
        for item in items:
            d = _coerce_date(item)
            if d is not None:
                out.add(d.isoformat())
    return frozenset(out)


# --------------------------------------------------------------------------- #
# Expected-session resolution (clock → calendar expectation).
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ExpectedSession:
    """The clock's latest EXPECTED completed trading session + cutoff metadata."""
    market_date: date
    cutoff_passed: bool
    within_trading_day: bool
    now_et: datetime
    close_cutoff_et: time

    @property
    def market_date_iso(self) -> str:
        return self.market_date.isoformat()


def resolve_expected_session(
    now: datetime,
    *,
    close_cutoff_et: Any = DEFAULT_CLOSE_CUTOFF_ET,
) -> ExpectedSession:
    """Resolve the latest EXPECTED completed session from a datetime clock (pure).

    On a weekday at/after ``close_cutoff_et`` (US/Eastern) today's session is
    expected complete; otherwise the most recent prior weekday. Weekends resolve
    back to the prior Friday. No holiday calendar is consulted — a holiday only
    makes the expected date one session too *new*; owned-data confirmation
    (``evaluate_session``) then resolves it to the latest actual session.
    """
    cutoff = _coerce_cutoff(close_cutoff_et)
    et = to_eastern(now)
    is_weekday = et.weekday() < 5
    cutoff_passed = bool(is_weekday and et.timetz().replace(tzinfo=None) >= cutoff)
    candidate = et.date() if cutoff_passed else et.date() - timedelta(days=1)
    within_trading_day = bool(is_weekday and not cutoff_passed)
    return ExpectedSession(
        market_date=walk_back_to_trading_day(candidate),
        cutoff_passed=cutoff_passed,
        within_trading_day=within_trading_day,
        now_et=et,
        close_cutoff_et=cutoff,
    )


def expected_from_reference_date(reference_today: Any) -> ExpectedSession:
    """Offline / injected-date resolution: the latest completed session is the
    weekday BEFORE ``reference_today`` (the deterministic rule the offline harness
    and ``paper_trading_desk._required_mark_date`` already use). ``cutoff_passed``
    is treated as True and ``within_trading_day`` as False (no live clock)."""
    d = _coerce_date(reference_today)
    if d is None:
        raise ValueError("reference_today must be a date or ISO date string")
    expected = previous_trading_day(d)
    et_midnight = datetime(d.year, d.month, d.day, tzinfo=_ET)
    return ExpectedSession(
        market_date=expected,
        cutoff_passed=True,
        within_trading_day=False,
        now_et=et_midnight,
        close_cutoff_et=DEFAULT_CLOSE_CUTOFF_ET,
    )


# --------------------------------------------------------------------------- #
# Full market-session contract (expected + owned-data confirmation).
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MarketSession:
    """Immutable canonical market-session contract (Slice 1)."""
    evaluated_at_utc: Optional[str]
    evaluated_at_et: Optional[str]
    expected_completed_market_date: Optional[str]
    latest_confirmed_owned_data_date: Optional[str]
    eligible_market_date: Optional[str]
    latest_benchmark_date: Optional[str]
    calendar_policy: str
    close_cutoff_et: str
    confirmation_source: str
    cutoff_passed: bool
    within_trading_day: bool
    session_status: str
    ready_for_daily_signal_refresh: bool
    ready_for_operational_close: bool
    ready_for_true_forward_capture: bool
    reason: str
    weakest_gate: str
    operator_action: str
    warnings: tuple[str, ...]
    # Phase 29D.1: True when the weekday+cutoff calendar is degraded (no authoritative
    # exchange-holiday calendar available) AND owned data has not yet confirmed the
    # expected session — the date stays unresolved (WAITING_FOR_OWNED_DATA), NEVER
    # inferred as a holiday. False otherwise.
    calendar_policy_degraded: bool = False
    # Authoritative non-session dates (exchange holidays / provider-confirmed
    # non-sessions) that were used to resolve the expected session, if any.
    authoritative_non_sessions: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        d = {
            "evaluated_at_utc": self.evaluated_at_utc,
            "evaluated_at_et": self.evaluated_at_et,
            "expected_completed_market_date": self.expected_completed_market_date,
            "latest_confirmed_owned_data_date": self.latest_confirmed_owned_data_date,
            "eligible_market_date": self.eligible_market_date,
            "latest_benchmark_date": self.latest_benchmark_date,
            "calendar_policy": self.calendar_policy,
            "close_cutoff_et": self.close_cutoff_et,
            "confirmation_source": self.confirmation_source,
            "cutoff_passed": self.cutoff_passed,
            "within_trading_day": self.within_trading_day,
            "session_status": self.session_status,
            "ready_for_daily_signal_refresh": self.ready_for_daily_signal_refresh,
            "ready_for_operational_close": self.ready_for_operational_close,
            "ready_for_true_forward_capture": self.ready_for_true_forward_capture,
            "reason": self.reason,
            "weakest_gate": self.weakest_gate,
            "operator_action": self.operator_action,
            "calendar_policy_degraded": self.calendar_policy_degraded,
            "authoritative_non_sessions": list(self.authoritative_non_sessions),
            "warnings": list(self.warnings),
        }
        return d


_DEGRADED_WARNING = (
    "Calendar policy is weekday + close-cutoff without an exchange holiday "
    "calendar; owned provider-confirmed sessions are authoritative."
)


def evaluate_session(
    *,
    now: Optional[datetime] = None,
    reference_today: Any = None,
    latest_confirmed_owned_data_date: Any = None,
    latest_benchmark_date: Any = None,
    close_cutoff_et: Any = DEFAULT_CLOSE_CUTOFF_ET,
    require_confirmation: bool = True,
    calendar_policy: str = CALENDAR_POLICY,
    confirmation_source: str = CONFIRMATION_SOURCE,
    authoritative_non_sessions: Any = None,
    provider_confirmed_non_sessions: Any = None,
    exchange_calendar_available: Optional[bool] = None,
) -> MarketSession:
    """Evaluate the canonical market session (pure).

    Exactly one clock must be supplied: ``now`` (a datetime — live/explicit) or
    ``reference_today`` (a date/ISO-string — offline injected-date rule). Passing
    neither raises ``ValueError`` (the caller must be explicit about the clock;
    the module never reads the wall clock itself).

    ``require_confirmation=False`` produces a calendar-only view (World A / the
    legacy 16:00 resolver): the expected date is returned without owned-data
    confirmation. ``require_confirmation=True`` (default) classifies the expected
    date against ``latest_confirmed_owned_data_date`` using the frozen status
    vocabulary; owned data is authoritative over the weekday calendar.

    Non-session policy (Phase 29D.1 live-acceptance correction): a weekday is
    classified ``NON_SESSION`` ONLY when it is in an AUTHORITATIVE source —
    ``authoritative_non_sessions`` (an exchange calendar) or
    ``provider_confirmed_non_sessions`` (a persisted provider-confirmed contract).
    The absence of same-day owned data is NEVER a holiday. When no authoritative
    calendar is available (``exchange_calendar_available`` is falsey / no calendar
    set is supplied) and the expected session is not yet confirmed, the result is
    ``WAITING_FOR_OWNED_DATA`` with ``calendar_policy_degraded=True`` — the expected
    weekday stays unresolved, never advanced as a holiday. ``latest_benchmark_date``
    is retained for compatibility (surfaced in the contract) but no longer drives any
    holiday inference: the desk marks and the SPY benchmark share one owned provider
    and lag together on a normal publish delay.
    """
    if now is not None and reference_today is not None:
        raise ValueError("supply either now or reference_today, not both")
    if now is not None:
        exp = resolve_expected_session(now, close_cutoff_et=close_cutoff_et)
    elif reference_today is not None:
        exp = expected_from_reference_date(reference_today)
    else:
        raise ValueError("evaluate_session requires an explicit clock (now or reference_today)")

    expected = exp.market_date
    confirmed = _coerce_date(latest_confirmed_owned_data_date)
    benchmark = _coerce_date(latest_benchmark_date)
    warnings: list[str] = []

    # AUTHORITATIVE non-session dates (exchange holidays / provider-confirmed
    # non-sessions). These are the ONLY inputs allowed to classify a weekday as a
    # non-session. ``calendar_available`` is explicit when given, else inferred from
    # whether an authoritative-calendar set was supplied.
    non_sessions = _norm_non_sessions(authoritative_non_sessions,
                                      provider_confirmed_non_sessions)
    calendar_available = (bool(exchange_calendar_available)
                          if exchange_calendar_available is not None
                          else authoritative_non_sessions is not None)

    evaluated_utc = (exp.now_et.astimezone(timezone.utc).isoformat()
                     if exp.now_et.tzinfo else None)
    evaluated_et = exp.now_et.isoformat()

    def build(*, eligible: Optional[date], status: str, ready: bool,
              reason: str, gate: str, action: str,
              calendar_policy_degraded: bool = False,
              non_session_dates: tuple = (),
              expected_override: Optional[date] = None) -> MarketSession:
        return MarketSession(
            evaluated_at_utc=evaluated_utc,
            evaluated_at_et=evaluated_et,
            expected_completed_market_date=(expected_override or expected).isoformat(),
            latest_confirmed_owned_data_date=(confirmed.isoformat() if confirmed else None),
            eligible_market_date=(eligible.isoformat() if eligible else None),
            latest_benchmark_date=(benchmark.isoformat() if benchmark else None),
            calendar_policy=calendar_policy,
            close_cutoff_et=exp.close_cutoff_et.strftime("%H:%M"),
            confirmation_source=confirmation_source,
            cutoff_passed=exp.cutoff_passed,
            within_trading_day=exp.within_trading_day,
            session_status=status,
            ready_for_daily_signal_refresh=ready,
            ready_for_operational_close=ready,
            ready_for_true_forward_capture=ready,
            reason=reason,
            weakest_gate=gate,
            operator_action=action,
            warnings=tuple(warnings),
            calendar_policy_degraded=bool(calendar_policy_degraded),
            authoritative_non_sessions=tuple(non_session_dates),
        )

    # Calendar-only mode (no owned-data confirmation requested): World A / 16:00.
    if not require_confirmation:
        warnings.append(_DEGRADED_WARNING)
        if exp.within_trading_day:
            return build(
                eligible=expected, status=BEFORE_SESSION_CLOSE, ready=False,
                reason="Weekday before the %s ET close cutoff; today's session is still "
                       "forming. Latest completed (calendar) session is %s."
                       % (exp.close_cutoff_et.strftime("%H:%M"), expected.isoformat()),
                gate=GATE_SESSION_NOT_CLOSED,
                action="Wait for the session close cutoff before running the daily cycle.")
        return build(
            eligible=expected, status=EXPECTED_SESSION_COMPLETE, ready=True,
            reason="Calendar expects session %s complete (no owned-data confirmation "
                   "requested)." % expected.isoformat(),
            gate=GATE_NONE,
            action="None — calendar expectation only; confirm owned data before an "
                   "operational close.")

    # Confirmation-aware mode: owned data is authoritative.
    warnings.append(_DEGRADED_WARNING)

    if confirmed is not None and confirmed > expected:
        warnings.append(
            "Owned data date %s is NEWER than the calendar expectation %s; rejected "
            "as inconsistent (not accepted as eligible)."
            % (confirmed.isoformat(), expected.isoformat()))
        return build(
            eligible=expected, status=INCONSISTENT_FUTURE_DATA, ready=False,
            reason="Owned-data date %s is later than the expected completed session "
                   "%s — inconsistent; investigate the provider feed."
                   % (confirmed.isoformat(), expected.isoformat()),
            gate=GATE_INCONSISTENT_OWNED_DATA,
            action="Investigate the owned market-data feed: it reports a session "
                   "newer than the calendar expects.")

    if exp.within_trading_day:
        eligible = confirmed if (confirmed is not None and confirmed <= expected) else None
        return build(
            eligible=eligible, status=BEFORE_SESSION_CLOSE, ready=False,
            reason="Weekday before the %s ET close cutoff; today's session is still "
                   "forming." % exp.close_cutoff_et.strftime("%H:%M"),
            gate=GATE_SESSION_NOT_CLOSED,
            action="Wait for the session close cutoff before running the daily cycle.")

    if confirmed is None:
        return build(
            eligible=None, status=NO_CONFIRMED_DATA, ready=False,
            reason="No owned market-data session is confirmed; the weekday calendar "
                   "expects %s but owned data cannot confirm it." % expected.isoformat(),
            gate=GATE_NO_OWNED_DATA,
            action="Restore owned market-data availability; do not operate on the "
                   "unconfirmed calendar date alone.")

    if confirmed == expected:
        # The eligible session is confirmed only when BOTH the owned market marks AND
        # the benchmark (SPY) reach the expected session. If a benchmark is supplied
        # and still lags, wait for it (Phase 29D.1). When no benchmark is supplied the
        # single owned-data confirmation gates (backward compatible).
        if benchmark is not None and benchmark < expected:
            return build(
                eligible=benchmark, status=WAITING_FOR_OWNED_DATA, ready=False,
                reason="Owned market data confirms %s but the benchmark (SPY) is only "
                       "current through %s; both must reach the expected session %s."
                       % (confirmed.isoformat(), benchmark.isoformat(),
                          expected.isoformat()),
                gate=GATE_OWNED_DATA_LAG,
                action="Wait for the owned benchmark (SPY) to publish the %s session."
                       % expected.isoformat())
        return build(
            eligible=expected, status=SESSION_READY, ready=True,
            reason="Owned data (market + benchmark) confirms the expected completed "
                   "session %s." % expected.isoformat(),
            gate=GATE_NONE,
            action="None — the eligible market date is confirmed and ready.")

    # confirmed < expected. A weekday is a NON-SESSION only through an AUTHORITATIVE
    # source (exchange calendar / provider-confirmed contract) — NEVER inferred from
    # the absence of same-day owned data, and NEVER from a benchmark that stops one
    # session back (the desk marks and the SPY benchmark come from the SAME owned
    # provider and lag together on a normal post-cutoff publish delay). Phase 29D.1
    # live-acceptance correction of the false-holiday inference.
    #
    # Resolve the TRUE latest expected session by skipping authoritative non-sessions.
    true_expected = expected
    skipped: list[str] = []
    _guard = 0
    while true_expected.isoformat() in non_sessions and _guard < 15:
        skipped.append(true_expected.isoformat())
        true_expected = previous_trading_day(true_expected)
        _guard += 1

    if skipped:
        skipped_txt = ", ".join(skipped)
        if confirmed >= true_expected:
            warnings.append(
                "Authoritative calendar: %s is a non-session; the latest actual "
                "session is %s and owned data confirms %s."
                % (skipped_txt, true_expected.isoformat(), confirmed.isoformat()))
            return build(
                eligible=true_expected, status=NON_SESSION, ready=True,
                expected_override=true_expected, non_session_dates=tuple(skipped),
                reason="Authoritative non-session(s) %s; the latest completed session "
                       "is %s and owned data confirms it."
                       % (skipped_txt, true_expected.isoformat()),
                gate=GATE_NONE,
                action="None — %s is a confirmed non-session; %s is the eligible "
                       "session." % (skipped_txt, true_expected.isoformat()))
        # Authoritative non-session(s) skipped, but owned data still lags the true
        # latest session → wait for owned data (never holiday-inferred).
        return build(
            eligible=confirmed, status=WAITING_FOR_OWNED_DATA, ready=False,
            expected_override=true_expected, non_session_dates=tuple(skipped),
            reason="Owned data confirms only %s; the latest actual session %s (after "
                   "skipping authoritative non-session(s) %s) is not yet confirmed."
                   % (confirmed.isoformat(), true_expected.isoformat(), skipped_txt),
            gate=GATE_OWNED_DATA_LAG,
            action="Wait for owned market data to publish the %s session."
                   % true_expected.isoformat())

    # No authoritative non-session covers the expected weekday. The absence of
    # same-day owned data is a PUBLISH LAG, never an inferred holiday. When no
    # exchange calendar is available the calendar policy is DEGRADED but the expected
    # weekday session stays UNRESOLVED (WAITING_FOR_OWNED_DATA), never advanced as a
    # holiday. The latest valid prior session (confirmed) remains valid.
    degraded = not calendar_available
    if degraded:
        warnings.append(
            "No authoritative exchange-holiday calendar is available; the expected "
            "%s session is UNRESOLVED and treated as WAITING_FOR_OWNED_DATA, never "
            "inferred as a non-session from the absence of same-day owned data."
            % expected.isoformat())
    return build(
        eligible=confirmed, status=WAITING_FOR_OWNED_DATA, ready=False,
        calendar_policy_degraded=degraded,
        reason="Owned data confirms only %s; the expected completed session %s is "
               "not yet confirmed (absence of same-day owned data is never a "
               "holiday)." % (confirmed.isoformat(), expected.isoformat()),
        gate=GATE_OWNED_DATA_LAG,
        action="Wait for owned market data to publish the %s session (or refresh the "
               "owned EOD marks)." % expected.isoformat())


# --------------------------------------------------------------------------- #
# Release 54.2.1 — MISSED COMPLETED SESSIONS (pure calendar arithmetic).
#
# ``evaluate_session`` answers a CONFIRMATION question: which completed session do
# the owned marks confirm right now. That question can never express an OBLIGATION
# for a session whose data was never ingested, which is exactly how the 2026-09-01
# session disappeared from the operator surfaces when the wall clock rolled into
# 2026-09-02: the owned marks still confirmed 2026-08-31, so the eligible session
# stayed 2026-08-31 and every surface reported it "fully processed".
#
# The obligation question — WHICH completed sessions have not been closed — is a
# calendar question, and this is the calendar owner, so the arithmetic lives here.
# It is pure: no clock is read, no store is consulted, and the caller supplies both
# the last successfully closed session and the upper bound (the expected completed
# session, resolved by ``resolve_expected_session`` / ``evaluate_session``). The
# still-forming current session is therefore excluded by construction — a caller
# cannot accidentally ask for it, because the expected session is never today's
# open one. There is deliberately NO "today minus one" arithmetic anywhere here.
# --------------------------------------------------------------------------- #
#: Hard bound on how far back a catch-up enumeration may reach. A gap longer than
#: this is not a missed session — it is an outage, and it is reported as such
#: (``truncated``) rather than silently producing a hundred obligations.
MAX_MISSED_SESSIONS = 15


def completed_sessions_after(last_closed_session: Any, *, through: Any,
                             non_sessions: Any = None,
                             limit: int = MAX_MISSED_SESSIONS) -> dict[str, Any]:
    """The ordered completed trading sessions strictly AFTER ``last_closed_session``
    and at or before ``through`` (pure; weekday calendar + authoritative
    non-sessions).

    ``through`` is the caller's already-resolved EXPECTED completed session — never
    a wall-clock date — so today's still-forming session can never appear in the
    result. ``non_sessions`` carries the same authoritative exchange-holiday /
    provider-confirmed non-session inputs ``evaluate_session`` accepts; those dates
    are skipped, so weekends and holidays work without any caller arithmetic.

    Returns ``{"sessions": (...ascending ISO...), "oldest": iso|None,
    "newest": iso|None, "count": int, "truncated": bool, "skipped_non_sessions":
    (...)}``. An unusable / absent bound yields an empty, non-truncated result:
    absence of a bound is never an obligation.
    """
    start = _coerce_date(last_closed_session)
    end = _coerce_date(through)
    skipped_set = _norm_non_sessions(non_sessions)
    if start is None or end is None or end <= start:
        return {"sessions": (), "oldest": None, "newest": None, "count": 0,
                "truncated": False, "skipped_non_sessions": ()}
    out: list[str] = []
    skipped: list[str] = []
    cursor = next_trading_day(walk_back_to_trading_day(start))
    truncated = False
    while cursor <= end:
        iso = cursor.isoformat()
        if iso in skipped_set:
            skipped.append(iso)
        else:
            if len(out) >= max(0, int(limit)):
                truncated = True
                break
            out.append(iso)
        cursor = next_trading_day(cursor)
    return {"sessions": tuple(out),
            "oldest": (out[0] if out else None),
            "newest": (out[-1] if out else None),
            "count": len(out),
            "truncated": truncated,
            "skipped_non_sessions": tuple(skipped)}


__all__ = [
    "REGULAR_CLOSE_ET",
    "DEFAULT_CLOSE_CUTOFF_ET",
    "CALENDAR_POLICY",
    "CONFIRMATION_SOURCE",
    "BEFORE_SESSION_CLOSE",
    "EXPECTED_SESSION_COMPLETE",
    "WAITING_FOR_OWNED_DATA",
    "SESSION_READY",
    "NO_CONFIRMED_DATA",
    "CALENDAR_POLICY_DEGRADED",
    "NON_SESSION",
    "INCONSISTENT_FUTURE_DATA",
    "SESSION_STATUSES",
    "GATE_NONE",
    "GATE_SESSION_NOT_CLOSED",
    "GATE_OWNED_DATA_LAG",
    "GATE_NO_OWNED_DATA",
    "GATE_INCONSISTENT_OWNED_DATA",
    "ExpectedSession",
    "MarketSession",
    "MAX_MISSED_SESSIONS",
    "to_eastern",
    # Release 55 — the operator-facing Eastern spelling of an owner's stamp.
    "format_operator_timestamp",
    "walk_back_to_trading_day",
    "previous_trading_day",
    "next_trading_day",
    "completed_sessions_after",
    "resolve_expected_session",
    "expected_from_reference_date",
    "evaluate_session",
]
