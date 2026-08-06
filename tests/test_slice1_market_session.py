"""tests/test_slice1_market_session.py — Slice 1 (Phase 29B) canonical market session.

Deterministic, offline tests for the canonical market-session domain
(``engine.market_session``) and the delegation of the migrated compat wrappers.
No network, no database, no provider, no prediction — every clock and data date
is injected explicitly.

Covers the directive's MARKET-SESSION DOMAIN (1–13) and DELEGATION/COMPATIBILITY
(28–31, 34–35) requirements.
"""
from __future__ import annotations

import importlib.util
from datetime import date, datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from paper_trader.engine import market_session as ms
from paper_trader.api import daily_operating_run as dor
from paper_trader.api import daily_close as dc
from paper_trader.api import alpha_target as at

ET = ZoneInfo("America/New_York")

# A clock matrix spanning EST (Jan), EDT (Jul), weekends and a month boundary.
_MATRIX = [
    datetime(y, mo, d, h, mi, tzinfo=tz)
    for (y, mo, d) in [(2026, 1, 2), (2026, 1, 3), (2026, 1, 5),
                       (2026, 7, 3), (2026, 7, 4), (2026, 7, 6),
                       (2026, 7, 31), (2026, 8, 1), (2026, 8, 4)]
    for h in (0, 9, 15, 16, 17, 18, 23)
    for mi in (0, 29, 30)
    for tz in (ET, timezone.utc)
]


# --------------------------------------------------------------------------- #
# 1–13 Market-session domain
# --------------------------------------------------------------------------- #
def test_01_before_close_cutoff_is_before_session_close():
    now = datetime(2026, 8, 4, 20, 0, tzinfo=timezone.utc)   # Tue 16:00 EDT < 17:30
    s = ms.evaluate_session(now=now, latest_confirmed_owned_data_date="2026-08-03")
    assert s.within_trading_day is True
    assert s.session_status == ms.BEFORE_SESSION_CLOSE
    assert s.ready_for_operational_close is False


def test_02_after_close_cutoff_expected_is_today():
    now = datetime(2026, 8, 4, 21, 45, tzinfo=timezone.utc)  # Tue 17:45 EDT >= 17:30
    es = ms.resolve_expected_session(now, close_cutoff_et=ms.DEFAULT_CLOSE_CUTOFF_ET)
    assert es.cutoff_passed is True
    assert es.market_date == date(2026, 8, 4)
    s = ms.evaluate_session(now=now, latest_confirmed_owned_data_date="2026-08-04")
    assert s.session_status == ms.SESSION_READY


def test_03_weekend_resolves_back_to_friday():
    sat = datetime(2026, 8, 8, 20, 0, tzinfo=ET)
    sun = datetime(2026, 8, 9, 20, 0, tzinfo=ET)
    assert ms.resolve_expected_session(sat).market_date == date(2026, 8, 7)
    assert ms.resolve_expected_session(sun).market_date == date(2026, 8, 7)


def test_04_et_utc_conversion_consistent():
    # 2026-01-05 21:00 UTC == 16:00 EST (same wall instant).
    utc = datetime(2026, 1, 5, 21, 0, tzinfo=timezone.utc)
    es = ms.resolve_expected_session(utc, close_cutoff_et=time(16, 0))
    assert es.now_et.tzinfo is not None
    assert es.now_et.hour == 16 and es.now_et.date() == date(2026, 1, 5)
    assert es.cutoff_passed is True and es.market_date == date(2026, 1, 5)


def test_05_dst_standard_time_est():
    # January -> EST (UTC-5). 14:30 UTC = 09:30 EST.
    utc = datetime(2026, 1, 6, 14, 30, tzinfo=timezone.utc)
    es = ms.resolve_expected_session(utc, close_cutoff_et=time(16, 0))
    assert es.now_et.utcoffset().total_seconds() == -5 * 3600
    assert es.now_et.hour == 9


def test_06_dst_daylight_time_edt():
    # July -> EDT (UTC-4). 20:30 UTC = 16:30 EDT.
    utc = datetime(2026, 7, 6, 20, 30, tzinfo=timezone.utc)
    es = ms.resolve_expected_session(utc, close_cutoff_et=time(16, 0))
    assert es.now_et.utcoffset().total_seconds() == -4 * 3600
    assert es.now_et.hour == 16 and es.cutoff_passed is True


def test_07_expected_complete_but_no_provider_yet():
    now = datetime(2026, 8, 4, 21, 45, tzinfo=timezone.utc)
    s = ms.evaluate_session(now=now, latest_confirmed_owned_data_date="2026-07-31")
    assert s.session_status == ms.WAITING_FOR_OWNED_DATA
    assert s.eligible_market_date == "2026-07-31"
    assert s.ready_for_operational_close is False


def test_08_provider_earlier_than_calendar_waits():
    now = datetime(2026, 8, 4, 21, 45, tzinfo=timezone.utc)  # expected 2026-08-04
    s = ms.evaluate_session(now=now, latest_confirmed_owned_data_date="2026-07-29")
    assert s.session_status == ms.WAITING_FOR_OWNED_DATA
    assert s.weakest_gate == ms.GATE_OWNED_DATA_LAG


def test_09_provider_equals_calendar_ready():
    now = datetime(2026, 8, 4, 21, 45, tzinfo=timezone.utc)
    s = ms.evaluate_session(now=now, latest_confirmed_owned_data_date="2026-08-04")
    assert s.session_status == ms.SESSION_READY
    assert s.eligible_market_date == "2026-08-04"
    assert s.ready_for_operational_close is True


def test_10_provider_later_than_calendar_rejected():
    now = datetime(2026, 8, 4, 21, 45, tzinfo=timezone.utc)  # expected 2026-08-04
    s = ms.evaluate_session(now=now, latest_confirmed_owned_data_date="2026-08-05")
    assert s.session_status == ms.INCONSISTENT_FUTURE_DATA
    assert s.ready_for_operational_close is False
    assert s.eligible_market_date == "2026-08-04"  # never the future date


def test_11_no_confirmed_data():
    now = datetime(2026, 8, 4, 21, 45, tzinfo=timezone.utc)
    s = ms.evaluate_session(now=now, latest_confirmed_owned_data_date=None)
    assert s.session_status == ms.NO_CONFIRMED_DATA
    assert s.eligible_market_date is None
    assert s.ready_for_operational_close is False


def test_11b_absence_of_owned_data_is_never_a_holiday():
    # Phase 29D.1 live-acceptance correction: two owned series (desk marks + SPY)
    # both stop one session back — but they share ONE owned provider and lag together
    # on a normal post-cutoff publish delay. With NO authoritative exchange calendar
    # this is WAITING_FOR_OWNED_DATA (calendar policy DEGRADED), NEVER a holiday. The
    # prior session stays valid.
    now = datetime(2026, 8, 4, 21, 45, tzinfo=timezone.utc)  # expected 2026-08-04
    s = ms.evaluate_session(now=now, latest_confirmed_owned_data_date="2026-08-03",
                            latest_benchmark_date="2026-08-03")
    assert s.session_status == ms.WAITING_FOR_OWNED_DATA
    assert s.ready_for_operational_close is False
    assert s.calendar_policy_degraded is True
    assert s.eligible_market_date == "2026-08-03"  # the prior valid session stands


def test_11c_authoritative_calendar_holiday_permits_non_session():
    # An AUTHORITATIVE exchange calendar marks the expected date a non-session -> the
    # latest actual session is the prior day and owned data confirms it -> NON_SESSION.
    now = datetime(2026, 8, 4, 21, 45, tzinfo=timezone.utc)  # expected 2026-08-04
    s = ms.evaluate_session(now=now, latest_confirmed_owned_data_date="2026-08-03",
                            latest_benchmark_date="2026-08-03",
                            authoritative_non_sessions=["2026-08-04"])
    assert s.session_status == ms.NON_SESSION
    assert s.eligible_market_date == "2026-08-03"
    assert s.ready_for_operational_close is True
    assert "2026-08-04" in s.authoritative_non_sessions


def test_11d_provider_confirmed_non_session_permits_non_session():
    now = datetime(2026, 8, 4, 21, 45, tzinfo=timezone.utc)
    s = ms.evaluate_session(now=now, latest_confirmed_owned_data_date="2026-08-03",
                            provider_confirmed_non_sessions=("2026-08-04",))
    assert s.session_status == ms.NON_SESSION
    assert s.eligible_market_date == "2026-08-03"
    assert s.ready_for_operational_close is True


def test_11e_calendar_available_but_not_holiday_still_waits():
    # A calendar IS available and confirms the expected date is a trading day, but
    # owned data has not published yet -> WAITING_FOR_OWNED_DATA, NOT degraded.
    now = datetime(2026, 8, 4, 21, 45, tzinfo=timezone.utc)
    s = ms.evaluate_session(now=now, latest_confirmed_owned_data_date="2026-08-03",
                            latest_benchmark_date="2026-08-03",
                            authoritative_non_sessions=[])  # calendar present, no holiday
    assert s.session_status == ms.WAITING_FOR_OWNED_DATA
    assert s.ready_for_operational_close is False
    assert s.calendar_policy_degraded is False


def test_12_deterministic_output_same_inputs():
    now = datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc)
    a = ms.evaluate_session(now=now, latest_confirmed_owned_data_date="2026-07-06")
    b = ms.evaluate_session(now=now, latest_confirmed_owned_data_date="2026-07-06")
    assert a.as_dict() == b.as_dict()


def test_13_requires_explicit_clock_no_machine_local():
    with pytest.raises(ValueError):
        ms.evaluate_session()   # neither now nor reference_today
    with pytest.raises(ValueError):
        ms.evaluate_session(now=datetime(2026, 8, 4, tzinfo=timezone.utc),
                            reference_today="2026-08-04")
    # A naive datetime is treated as ET wall-clock (explicit), not machine-local.
    naive = datetime(2026, 8, 4, 17, 45)
    aware = datetime(2026, 8, 4, 17, 45, tzinfo=ET)
    assert (ms.resolve_expected_session(naive).market_date
            == ms.resolve_expected_session(aware).market_date == date(2026, 8, 4))


# --------------------------------------------------------------------------- #
# 28–31, 34–35 Delegation & compatibility
# --------------------------------------------------------------------------- #
def test_29_daily_operating_run_delegates_completed_session():
    for now in _MATRIX:
        assert dor.latest_completed_market_date(now) == \
            ms.resolve_expected_session(now, close_cutoff_et=ms.REGULAR_CLOSE_ET).market_date


def test_28_daily_close_delegates_session_eligibility():
    for now in _MATRIX:
        legacy = dc._resolve_clock(now=now)
        new = ms.resolve_expected_session(now, close_cutoff_et=dc.POST_CLOSE_CUTOFF_ET)
        assert legacy["expected_market_date"] == new.market_date.isoformat()
        assert bool(legacy["cutoff_passed"]) == new.cutoff_passed
        assert bool(legacy["within_trading_day"]) == new.within_trading_day
    # Injected-date (offline) rule also delegates.
    for s in ("2026-08-04", "2026-08-03", "2026-08-01", "2026-07-31"):
        assert dc._resolve_clock(today=s)["expected_market_date"] == \
            ms.expected_from_reference_date(s).market_date.isoformat()


def test_30_alpha_target_delegates_completed_session():
    # alpha_target.latest_completed() resolves through the canonical owner (via the
    # daily_operating_run compat wrapper). Prove it matches for an injected clock.
    at._now_override = datetime(2026, 8, 4, 21, 45, tzinfo=timezone.utc)
    try:
        got = at.latest_completed()
    finally:
        at._now_override = None
    expected = ms.resolve_expected_session(
        datetime(2026, 8, 4, 21, 45, tzinfo=timezone.utc),
        close_cutoff_et=ms.REGULAR_CLOSE_ET).market_date.isoformat()
    assert got == expected


def test_31_public_signatures_remain_compatible():
    # latest_completed_market_date(now) -> date
    out = dor.latest_completed_market_date(datetime(2026, 8, 4, 21, 45, tzinfo=timezone.utc))
    assert isinstance(out, date)
    # _resolve_clock keeps its documented keys.
    clk = dc._resolve_clock(now=datetime(2026, 8, 4, 21, 45, tzinfo=timezone.utc))
    for k in ("expected_market_date", "cutoff_passed", "within_trading_day",
              "post_close_cutoff_et", "timezone"):
        assert k in clk


def test_34_historical_evidence_calendar_kept_separate():
    from paper_trader.api import forward_prediction_skill as fps
    # The historical evidence calendar is still owned by forward_prediction_skill
    # and is NOT re-exported by the current-session owner.
    assert hasattr(fps, "eligible_calendar")
    assert not hasattr(ms, "eligible_calendar")


def test_35_migrated_wrappers_have_no_raw_session_arithmetic():
    audit = _load_audit()
    rep = audit.check_market_session_ownership(audit._iter_source_files())
    assert rep["owner_present"] is True
    assert rep["migrated_wrappers_clean"]["api/daily_operating_run.py"] is True
    assert rep["migrated_wrappers_clean"]["api/daily_close.py"] is True
    assert all(rep["delegating_wrappers"].values())
    assert rep["unexpected_session_resolvers"] == []


def _load_audit():
    p = Path(__file__).resolve().parent.parent / "scripts" / "audit_architecture.py"
    spec = importlib.util.spec_from_file_location("audit_architecture", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
