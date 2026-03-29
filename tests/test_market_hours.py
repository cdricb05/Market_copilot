"""
tests/test_market_hours.py — Pure unit tests for engine/market_hours.py.

No database required. All tests are deterministic using fixed datetimes.
"""
from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from paper_trader.engine.market_hours import (
    is_market_open,
    is_weekday,
    market_date_for,
    to_eastern,
)

_ET  = ZoneInfo("America/New_York")
_UTC = timezone.utc

# ---------------------------------------------------------------------------
# Reference timestamps
#
# WED_ET_NAIVE   — Wednesday 2025-01-15 12:00 naive (treated as ET)
# WED_UTC_AWARE  — same wall-clock instant expressed as UTC-aware (17:00 UTC)
# SAT_ET_NAIVE   — Saturday 2025-01-18 12:00 naive ET
# SUN_ET_NAIVE   — Sunday  2025-01-19 12:00 naive ET
# EDT_AWARE      — Summer  2025-07-16 12:00 UTC-aware  (EDT = UTC-4)
# EST_AWARE      — Winter  2025-01-15 17:00 UTC-aware  (EST = UTC-5)
# ---------------------------------------------------------------------------


class TestToEastern:
    def test_naive_attaches_et_tzinfo(self) -> None:
        naive = datetime(2025, 1, 15, 12, 0, 0)
        result = to_eastern(naive)
        assert result.tzinfo is not None
        assert result.tzinfo.key == "America/New_York"

    def test_naive_does_not_shift_wall_clock(self) -> None:
        naive = datetime(2025, 1, 15, 12, 0, 0)
        result = to_eastern(naive)
        assert result.replace(tzinfo=None) == naive

    def test_aware_utc_converts_to_et_est(self) -> None:
        # 2025-01-15 17:00 UTC = 12:00 EST (UTC-5)
        aware = datetime(2025, 1, 15, 17, 0, 0, tzinfo=_UTC)
        result = to_eastern(aware)
        assert result.hour == 12
        assert result.minute == 0
        assert result.tzinfo.key == "America/New_York"

    def test_aware_utc_converts_to_et_edt(self) -> None:
        # 2025-07-16 16:00 UTC = 12:00 EDT (UTC-4)
        aware = datetime(2025, 7, 16, 16, 0, 0, tzinfo=_UTC)
        result = to_eastern(aware)
        assert result.hour == 12
        assert result.minute == 0
        assert result.tzinfo.key == "America/New_York"

    def test_already_eastern_aware_is_unchanged(self) -> None:
        et_aware = datetime(2025, 1, 15, 12, 0, 0, tzinfo=_ET)
        result = to_eastern(et_aware)
        assert result == et_aware


class TestIsWeekday:
    def test_monday_is_weekday(self) -> None:
        # 2025-01-13 is Monday
        assert is_weekday(datetime(2025, 1, 13, 10, 0, 0)) is True

    def test_friday_is_weekday(self) -> None:
        # 2025-01-17 is Friday
        assert is_weekday(datetime(2025, 1, 17, 10, 0, 0)) is True

    def test_saturday_is_not_weekday(self) -> None:
        # 2025-01-18 is Saturday
        assert is_weekday(datetime(2025, 1, 18, 10, 0, 0)) is False

    def test_sunday_is_not_weekday(self) -> None:
        # 2025-01-19 is Sunday
        assert is_weekday(datetime(2025, 1, 19, 10, 0, 0)) is False

    def test_aware_utc_weekday(self) -> None:
        # 2025-01-15 17:00 UTC = Wednesday 12:00 EST
        aware = datetime(2025, 1, 15, 17, 0, 0, tzinfo=_UTC)
        assert is_weekday(aware) is True

    def test_aware_utc_sunday_after_midnight_et(self) -> None:
        # 2025-01-20 02:00 UTC = Sunday 2025-01-19 21:00 EST
        aware = datetime(2025, 1, 20, 2, 0, 0, tzinfo=_UTC)
        assert is_weekday(aware) is False


class TestIsMarketOpen:
    # --- weekday, boundary times ---

    def test_exactly_at_open_is_open(self) -> None:
        dt = datetime(2025, 1, 15, 9, 30, 0)   # Wednesday 09:30 ET naive
        assert is_market_open(dt) is True

    def test_one_second_before_open_is_closed(self) -> None:
        dt = datetime(2025, 1, 15, 9, 29, 59)
        assert is_market_open(dt) is False

    def test_during_session_is_open(self) -> None:
        dt = datetime(2025, 1, 15, 13, 0, 0)   # midday
        assert is_market_open(dt) is True

    def test_one_second_before_close_is_open(self) -> None:
        dt = datetime(2025, 1, 15, 15, 59, 59)
        assert is_market_open(dt) is True

    def test_exactly_at_close_is_closed(self) -> None:
        dt = datetime(2025, 1, 15, 16, 0, 0)
        assert is_market_open(dt) is False

    def test_after_close_is_closed(self) -> None:
        dt = datetime(2025, 1, 15, 17, 0, 0)
        assert is_market_open(dt) is False

    def test_before_open_is_closed(self) -> None:
        dt = datetime(2025, 1, 15, 8, 0, 0)
        assert is_market_open(dt) is False

    # --- weekend ---

    def test_saturday_midday_is_closed(self) -> None:
        dt = datetime(2025, 1, 18, 12, 0, 0)   # Saturday
        assert is_market_open(dt) is False

    def test_sunday_midday_is_closed(self) -> None:
        dt = datetime(2025, 1, 19, 12, 0, 0)   # Sunday
        assert is_market_open(dt) is False

    # --- aware UTC input ---

    def test_aware_utc_during_session(self) -> None:
        # 2025-01-15 18:00 UTC = 13:00 EST — market open
        aware = datetime(2025, 1, 15, 18, 0, 0, tzinfo=_UTC)
        assert is_market_open(aware) is True

    def test_aware_utc_before_session(self) -> None:
        # 2025-01-15 13:00 UTC = 08:00 EST — pre-market
        aware = datetime(2025, 1, 15, 13, 0, 0, tzinfo=_UTC)
        assert is_market_open(aware) is False

    def test_aware_utc_after_session(self) -> None:
        # 2025-01-15 22:00 UTC = 17:00 EST — after close
        aware = datetime(2025, 1, 15, 22, 0, 0, tzinfo=_UTC)
        assert is_market_open(aware) is False


class TestMarketDateFor:
    def test_naive_returns_et_date(self) -> None:
        dt = datetime(2025, 1, 15, 12, 0, 0)
        from datetime import date
        assert market_date_for(dt) == date(2025, 1, 15)

    def test_aware_utc_returns_correct_et_date(self) -> None:
        # 2025-01-15 17:00 UTC = 12:00 EST same calendar day
        from datetime import date
        aware = datetime(2025, 1, 15, 17, 0, 0, tzinfo=_UTC)
        assert market_date_for(aware) == date(2025, 1, 15)

    def test_utc_midnight_crosses_to_prior_et_date(self) -> None:
        # 2025-01-16 00:05 UTC = 2025-01-15 19:05 EST
        # UTC date is Jan 16 but ET date is still Jan 15
        from datetime import date
        aware = datetime(2025, 1, 16, 0, 5, 0, tzinfo=_UTC)
        assert market_date_for(aware) == date(2025, 1, 15)

    def test_returns_date_type_not_datetime(self) -> None:
        from datetime import date
        dt = datetime(2025, 1, 15, 12, 0, 0)
        result = market_date_for(dt)
        assert type(result) is date
