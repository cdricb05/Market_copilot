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
CALENDAR_POLICY_DEGRADED = "CALENDAR_POLICY_DEGRADED"
INCONSISTENT_FUTURE_DATA = "INCONSISTENT_FUTURE_DATA"

SESSION_STATUSES = (
    BEFORE_SESSION_CLOSE,
    EXPECTED_SESSION_COMPLETE,
    WAITING_FOR_OWNED_DATA,
    SESSION_READY,
    NO_CONFIRMED_DATA,
    CALENDAR_POLICY_DEGRADED,
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

    evaluated_utc = (exp.now_et.astimezone(timezone.utc).isoformat()
                     if exp.now_et.tzinfo else None)
    evaluated_et = exp.now_et.isoformat()

    def build(*, eligible: Optional[date], status: str, ready: bool,
              reason: str, gate: str, action: str) -> MarketSession:
        return MarketSession(
            evaluated_at_utc=evaluated_utc,
            evaluated_at_et=evaluated_et,
            expected_completed_market_date=expected.isoformat(),
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
        return build(
            eligible=expected, status=SESSION_READY, ready=True,
            reason="Owned data confirms the expected completed session %s."
                   % expected.isoformat(),
            gate=GATE_NONE,
            action="None — the eligible market date is confirmed and ready.")

    # confirmed < expected. Distinguish a likely holiday (a SECOND independent
    # owned series — the benchmark — also stops exactly one trading session back)
    # from an ordinary owned-data publish lag.
    likely_holiday = (
        benchmark is not None
        and benchmark == confirmed
        and previous_trading_day(expected) == confirmed
    )
    if likely_holiday:
        warnings.append(
            "Weekday calendar expected %s but two independent owned series both stop "
            "at %s; %s is treated as a non-session (likely holiday). Owned data wins."
            % (expected.isoformat(), confirmed.isoformat(), expected.isoformat()))
        return build(
            eligible=confirmed, status=CALENDAR_POLICY_DEGRADED, ready=True,
            reason="Owned data confirms %s as the latest actual session; the weekday "
                   "calendar over-counted %s (no holiday calendar). Owned data is "
                   "authoritative." % (confirmed.isoformat(), expected.isoformat()),
            gate=GATE_NONE,
            action="None — owned data is authoritative; %s is the eligible session."
                   % confirmed.isoformat())

    return build(
        eligible=confirmed, status=WAITING_FOR_OWNED_DATA, ready=False,
        reason="Owned data confirms only %s; the expected completed session %s is "
               "not yet confirmed." % (confirmed.isoformat(), expected.isoformat()),
        gate=GATE_OWNED_DATA_LAG,
        action="Wait for owned market data to publish the %s session (or refresh the "
               "owned EOD marks)." % expected.isoformat())


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
    "INCONSISTENT_FUTURE_DATA",
    "SESSION_STATUSES",
    "GATE_NONE",
    "GATE_SESSION_NOT_CLOSED",
    "GATE_OWNED_DATA_LAG",
    "GATE_NO_OWNED_DATA",
    "GATE_INCONSISTENT_OWNED_DATA",
    "ExpectedSession",
    "MarketSession",
    "to_eastern",
    "walk_back_to_trading_day",
    "previous_trading_day",
    "resolve_expected_session",
    "expected_from_reference_date",
    "evaluate_session",
]
