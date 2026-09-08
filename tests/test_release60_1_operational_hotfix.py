"""tests/test_release60_1_operational_hotfix.py — R60.1 operational hotfix.

Two live operational defects, both regression-locked here.

DEFECT 1 — FALSE SEP-7 SESSION RECOVERY. ``engine.market_session`` has accepted
an authoritative exchange calendar since Phase 29D.1, but NOTHING ever supplied
one, so in production ``authoritative_non_sessions`` was permanently ``None``,
the tested ``NON_SESSION`` branch was unreachable, and the weekday-only
expectation named every exchange holiday a trading session. The missed-session
(catch-up) projection then turned that into an OBLIGATION: on Labor Day
2026-09-07 the operator surfaces reported "Sep 7, 2026 was not closed / CATCH UP
WAITING FOR OWNED DATA" for a session that never existed.
``engine.exchange_calendar`` is the missing supplier; ``api.data_freshness`` is
the seam that wires it in. There is NO Labor Day special case anywhere.

DEFECT 2 — ALPHA & CAPITAL DID NOT ANSWER. The Alpha & Capital composition runs
the zero-base allocator, which legitimately costs tens of seconds on the live
book, and the view ALSO fetched ``/v1/operations/cash-deployment-frontier``, so a
single page load ran that same allocator twice, concurrently. The read exceeded
the browser's 45s abort budget, ``_mhzGet`` returned null, and the loader
returned early leaving ten panels reading "Loading..." forever.

Deterministic and offline: every clock and data date is injected, no network, no
provider, no prediction, no database. Nothing here runs a close, a cycle or a
research mutation.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from paper_trader.engine import exchange_calendar as xc
from paper_trader.engine import market_session as ms
from paper_trader.api import workflow_state as ws

ET = ZoneInfo("America/New_York")
REPO = Path(__file__).resolve().parents[1]
UI = REPO / "api" / "ui" / "index.html"

#: The 2026 calendar window the live defect occurred in.
CAL_2026 = xc.non_sessions_between("2026-01-01", "2026-12-31")


def _session(now, confirmed, *, calendar=CAL_2026, benchmark=None):
    """Evaluate the canonical session with the authoritative calendar supplied."""
    return ms.evaluate_session(
        now=now, latest_confirmed_owned_data_date=confirmed,
        latest_benchmark_date=(confirmed if benchmark is None else benchmark),
        authoritative_non_sessions=calendar,
        exchange_calendar_available=True)


def _recovery(sess, *, last_close):
    """The canonical catch-up projection over an already-evaluated session."""
    return ws.build_session_recovery(
        expected_completed_market_date=sess.expected_completed_market_date,
        eligible_market_date=sess.eligible_market_date,
        latest_completed_close_date=last_close,
        operational_close_valid=True,
        latest_confirmed_owned_data_date=sess.latest_confirmed_owned_data_date,
        session_status=sess.session_status,
        authoritative_non_sessions=sess.exchange_calendar_non_sessions)


# =========================================================================== #
# The exchange calendar itself — checked against the PUBLISHED NYSE closures.
# =========================================================================== #
class TestExchangeCalendar:
    #: Published NYSE full-day closure sets. A rule-based calendar is only worth
    #: having if it reproduces the real ones, so these are asserted verbatim
    #: rather than derived from the implementation.
    PUBLISHED = {
        2022: ["2022-01-17", "2022-02-21", "2022-04-15", "2022-05-30",
               "2022-06-20", "2022-07-04", "2022-09-05", "2022-11-24",
               "2022-12-26"],
        2024: ["2024-01-01", "2024-01-15", "2024-02-19", "2024-03-29",
               "2024-05-27", "2024-06-19", "2024-07-04", "2024-09-02",
               "2024-11-28", "2024-12-25"],
        2025: ["2025-01-01", "2025-01-09", "2025-01-20", "2025-02-17",
               "2025-04-18", "2025-05-26", "2025-06-19", "2025-07-04",
               "2025-09-01", "2025-11-27", "2025-12-25"],
        2026: ["2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
               "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
               "2026-11-26", "2026-12-25"],
        2027: ["2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26",
               "2027-05-31", "2027-06-18", "2027-07-05", "2027-09-06",
               "2027-11-25", "2027-12-24"],
    }

    @pytest.mark.parametrize("year", sorted(PUBLISHED))
    def test_matches_the_published_nyse_closures(self, year):
        assert sorted(xc.holidays_for_year(year)) == sorted(self.PUBLISHED[year])

    def test_labor_day_2026_is_the_defect_date(self):
        assert xc.holiday_name("2026-09-07") == "Labor Day"
        assert xc.is_non_session("2026-09-07") is True

    def test_the_sessions_around_labor_day_2026_still_trade(self):
        assert xc.is_non_session("2026-09-04") is False   # Friday
        assert xc.is_non_session("2026-09-08") is False   # Tuesday

    def test_memorial_day_is_the_last_monday_not_the_fourth(self):
        # The off-by-one-week trap: anchoring on day 28 and stepping forward in
        # weeks resolved Memorial Day 2022 to May 23 instead of May 30.
        assert "2022-05-30" in xc.holidays_for_year(2022)
        assert "2022-05-23" not in xc.holidays_for_year(2022)
        assert "2027-05-31" in xc.holidays_for_year(2027)

    def test_juneteenth_starts_in_2022_and_not_before(self):
        assert any(d.startswith("2022-06") for d in xc.holidays_for_year(2022))
        assert not any(d.startswith("2021-06") for d in xc.holidays_for_year(2021))

    def test_saturday_new_year_is_not_observed_on_the_friday(self):
        # 2022-01-01 was a Saturday: the NYSE did NOT close on Friday 2021-12-31.
        assert "2021-12-31" not in xc.holidays_for_year(2021)
        assert "2022-01-03" not in xc.holidays_for_year(2022)

    def test_sunday_holiday_rolls_forward_to_the_monday(self):
        # 2022-12-25 was a Sunday -> observed Monday 2022-12-26.
        assert "2022-12-26" in xc.holidays_for_year(2022)

    def test_saturday_holiday_rolls_back_to_the_friday(self):
        # 2026-07-04 is a Saturday -> observed Friday 2026-07-03.
        assert "2026-07-03" in xc.holidays_for_year(2026)

    def test_declared_ad_hoc_closures_are_present(self):
        assert xc.holiday_name("2012-10-29") == "Hurricane Sandy"
        assert "mourning" in (xc.holiday_name("2025-01-09") or "").lower()

    def test_weekends_are_not_returned_as_declared_closures(self):
        # Weekends are the session owner's own weekday arithmetic. Reporting them
        # as authoritative closures would make every Saturday look like a
        # declared exchange holiday in the operator-facing contract.
        for iso in xc.non_sessions_between("2026-01-01", "2026-12-31"):
            assert date.fromisoformat(iso).weekday() < 5, iso

    def test_it_never_answers_outside_its_supported_years(self):
        assert xc.is_supported("1997-07-04") is False
        assert xc.holidays_for_year(1997) == {}
        # "I cannot answer" is NEVER converted into "the market was shut".
        assert xc.is_non_session("1997-07-04") is False
        assert xc.calendar_available_between("1997-01-01", "2026-01-01") is False

    def test_it_is_pure_and_deterministic(self):
        assert xc.holidays_for_year(2026) == xc.holidays_for_year(2026)
        assert xc.describe()["infers_nothing_from_missing_market_data"] is True
        assert xc.describe()["models_early_closes"] is False


# =========================================================================== #
# DEFECT 1 — the required session regression cases.
# =========================================================================== #
class TestLaborDaySessionEligibility:
    """The canonical eligible session must stay 2026-09-04 until 2026-09-08."""

    @pytest.mark.parametrize("label,now", [
        ("Fri 2026-09-04 after close", datetime(2026, 9, 4, 18, 0, tzinfo=ET)),
        ("Sat 2026-09-05", datetime(2026, 9, 5, 12, 0, tzinfo=ET)),
        ("Sun 2026-09-06", datetime(2026, 9, 6, 12, 0, tzinfo=ET)),
        ("Mon 2026-09-07 Labor Day, morning",
         datetime(2026, 9, 7, 9, 0, tzinfo=ET)),
        ("Mon 2026-09-07 Labor Day, after the cutoff",
         datetime(2026, 9, 7, 20, 0, tzinfo=ET)),
        ("Tue 2026-09-08 before close", datetime(2026, 9, 8, 10, 0, tzinfo=ET)),
    ])
    def test_the_eligible_session_stays_2026_09_04(self, label, now):
        s = _session(now, "2026-09-04")
        assert s.eligible_market_date == "2026-09-04", label

    def test_tuesday_after_close_advances_to_2026_09_08(self):
        s = _session(datetime(2026, 9, 8, 18, 0, tzinfo=ET), "2026-09-08")
        assert s.eligible_market_date == "2026-09-08"
        assert s.session_status == ms.SESSION_READY
        assert s.ready_for_operational_close is True

    def test_labor_day_is_named_a_non_session_not_a_data_lag(self):
        s = _session(datetime(2026, 9, 7, 20, 0, tzinfo=ET), "2026-09-04")
        assert s.session_status == ms.NON_SESSION
        assert s.expected_completed_market_date == "2026-09-04"
        assert "2026-09-07" in s.authoritative_non_sessions
        # A confirmed non-session is READY: there is nothing left to wait for.
        assert s.ready_for_operational_close is True
        assert s.calendar_policy_degraded is False

    def test_the_holiday_morning_never_claims_a_session_is_forming(self):
        # Before R60.1 a holiday before the 17:30 cutoff produced
        # BEFORE_SESSION_CLOSE - "today's session is still forming" on a day the
        # market never opened, blocking the operator with a wait that could
        # never end.
        s = _session(datetime(2026, 9, 7, 9, 0, tzinfo=ET), "2026-09-04")
        assert s.session_status != ms.BEFORE_SESSION_CLOSE
        assert s.within_trading_day is False
        assert s.cutoff_passed is False

    @pytest.mark.parametrize("label,now,confirmed,expected_eligible", [
        ("Thanksgiving 2026-11-26", datetime(2026, 11, 26, 20, 0, tzinfo=ET),
         "2026-11-25", "2026-11-25"),
        ("Good Friday 2026-04-03", datetime(2026, 4, 3, 20, 0, tzinfo=ET),
         "2026-04-02", "2026-04-02"),
        ("Christmas 2026-12-25", datetime(2026, 12, 25, 20, 0, tzinfo=ET),
         "2026-12-24", "2026-12-24"),
        ("Independence Day observed 2026-07-03",
         datetime(2026, 7, 3, 20, 0, tzinfo=ET), "2026-07-02", "2026-07-02"),
    ])
    def test_other_exchange_holidays_resolve_the_same_way(
            self, label, now, confirmed, expected_eligible):
        s = _session(now, confirmed)
        assert s.eligible_market_date == expected_eligible, label
        assert s.session_status == ms.NON_SESSION, label
        assert s.ready_for_operational_close is True, label

    @pytest.mark.parametrize("now,expected", [
        (datetime(2026, 8, 8, 12, 0, tzinfo=ET), "2026-08-07"),   # Saturday
        (datetime(2026, 8, 9, 12, 0, tzinfo=ET), "2026-08-07"),   # Sunday
        (datetime(2026, 8, 10, 20, 0, tzinfo=ET), "2026-08-10"),  # Monday close
    ])
    def test_ordinary_weekends_are_unchanged(self, now, expected):
        s = _session(now, expected)
        assert s.eligible_market_date == expected
        assert s.session_status == ms.SESSION_READY

    def test_a_genuine_owned_data_lag_is_still_a_wait(self):
        # The fix must not turn every lag into "ready". After Tuesday's close
        # with data still on Friday, 09-08 IS owed.
        s = _session(datetime(2026, 9, 8, 18, 0, tzinfo=ET), "2026-09-04")
        assert s.session_status == ms.WAITING_FOR_OWNED_DATA
        assert s.expected_completed_market_date == "2026-09-08"
        assert s.ready_for_operational_close is False


# =========================================================================== #
# DEFECT 1 — the catch-up projection must not owe a session that never existed.
# =========================================================================== #
class TestSessionRecoveryNeverOwesANonSession:
    def test_labor_day_is_not_a_missed_session(self):
        s = _session(datetime(2026, 9, 7, 20, 0, tzinfo=ET), "2026-09-04")
        rec = _recovery(s, last_close="2026-09-04")
        assert rec["catch_up_required"] is False
        assert rec["recovery_state"] == ws.NO_CATCH_UP_REQUIRED
        assert rec["recovery_session"] is None
        assert list(rec["missed_completed_sessions"]) == []

    def test_a_real_gap_across_a_holiday_owes_only_the_real_sessions(self):
        # Last close Thursday 2026-09-03; clock Tuesday 2026-09-08 after cutoff.
        # 09-04 and 09-08 were owed. 09-05/09-06 are a weekend and 09-07 is Labor
        # Day, so neither may appear.
        s = _session(datetime(2026, 9, 8, 18, 0, tzinfo=ET), "2026-09-08")
        rec = _recovery(s, last_close="2026-09-03")
        assert list(rec["missed_completed_sessions"]) == ["2026-09-04",
                                                          "2026-09-08"]
        assert "2026-09-07" not in rec["missed_completed_sessions"]
        assert rec["recovery_session"] == "2026-09-04"   # oldest first

    def test_the_enumeration_uses_the_whole_calendar_window(self):
        # The session contract publishes TWO lists: the closures that moved the
        # current verdict, and the full calendar window. The enumeration must use
        # the window - a holiday that did not move today's date must still be
        # excluded from what was owed. Passing the applied-only list is exactly
        # what let Labor Day be enumerated as unclosed.
        s = _session(datetime(2026, 9, 8, 18, 0, tzinfo=ET), "2026-09-08")
        assert s.authoritative_non_sessions == ()          # none applied today
        assert "2026-09-07" in s.exchange_calendar_non_sessions

    def test_no_provider_data_is_ever_requested_for_a_non_trading_session(self):
        # The workflow asks the close owner to cover the OLDEST owed session. If
        # a non-session could be owed, the workflow would ask the owned provider
        # for data that can never exist. Across a full holiday year, no recovery
        # target may ever be a non-session.
        holidays = set(CAL_2026)
        for iso in sorted(holidays):
            d = date.fromisoformat(iso)
            s = _session(datetime(d.year, d.month, d.day, 20, 0, tzinfo=ET),
                         (d - timedelta(days=7)).isoformat())
            rec = _recovery(s, last_close=(d - timedelta(days=7)).isoformat())
            owed = list(rec["missed_completed_sessions"] or [])
            assert not (set(owed) & holidays), (iso, owed)
            for owed_iso in owed:
                assert date.fromisoformat(owed_iso).weekday() < 5, owed_iso


# =========================================================================== #
# DEFECT 1 — the composition seam, and the degraded policy that still stands.
# =========================================================================== #
class TestCalendarIsSuppliedByTheComposition:
    def test_data_freshness_supplies_the_calendar_by_default(self):
        from paper_trader.api import data_freshness as df
        fr = df.load_data_freshness(
            now=datetime(2026, 9, 7, 20, 0, tzinfo=ET),
            operational={}, inputs={}, daily_status={},
            desk_marks={}, daily_close_status={}, forward_status={},
            date_overrides={"desk_mark_date": "2026-09-04",
                            "benchmark_date": "2026-09-04"})
        sess = fr["market_session"]
        assert sess["eligible_market_date"] == "2026-09-04"
        assert sess["expected_completed_market_date"] == "2026-09-04"
        assert sess["session_status"] == ms.NON_SESSION
        assert "2026-09-07" in sess["authoritative_non_sessions"]
        assert sess["calendar_policy_degraded"] is False

    def test_an_injected_calendar_still_wins(self):
        # A caller who has already decided is never overridden by the default.
        from paper_trader.api import data_freshness as df
        fr = df.load_data_freshness(
            now=datetime(2026, 9, 7, 20, 0, tzinfo=ET),
            operational={}, inputs={}, daily_status={},
            desk_marks={}, daily_close_status={}, forward_status={},
            date_overrides={"desk_mark_date": "2026-09-04",
                            "benchmark_date": "2026-09-04"},
            authoritative_non_sessions=[], exchange_calendar_available=True)
        sess = fr["market_session"]
        assert sess["authoritative_non_sessions"] == []
        assert sess["session_status"] == ms.WAITING_FOR_OWNED_DATA

    def test_with_no_calendar_the_owner_is_byte_for_byte_the_old_policy(self):
        # engine.market_session must still never GUESS a holiday. With no
        # calendar supplied the weekday policy and the degraded flag stand.
        s = ms.evaluate_session(
            now=datetime(2026, 9, 7, 20, 0, tzinfo=ET),
            latest_confirmed_owned_data_date="2026-09-04",
            latest_benchmark_date="2026-09-04")
        assert s.session_status == ms.WAITING_FOR_OWNED_DATA
        assert s.calendar_policy_degraded is True
        assert s.expected_completed_market_date == "2026-09-07"

    def test_the_absence_of_owned_data_is_still_never_a_holiday(self):
        # The Phase 29D.1 invariant is untouched: a calendar that AFFIRMS the day
        # traded plus missing owned data is a publish lag, not a closure.
        s = _session(datetime(2026, 9, 8, 18, 0, tzinfo=ET), "2026-09-04")
        assert s.session_status == ms.WAITING_FOR_OWNED_DATA
        assert s.session_status != ms.NON_SESSION

    def test_there_is_no_labor_day_special_case_anywhere(self):
        # The fix is a calendar, not an exception. No module may name the defect
        # date or the holiday as a literal.
        for rel in ("engine/market_session.py", "api/data_freshness.py",
                    "api/workflow_state.py", "api/ui/index.html"):
            src = (REPO / rel).read_text(encoding="utf-8", errors="replace")
            body = "\n".join(ln for ln in src.splitlines()
                             if not ln.strip().startswith(("#", "*", "//")))
            assert "2026-09-07" not in body, rel


# =========================================================================== #
# DEFECT 2 — Alpha & Capital answers, and a partial answer still renders.
# =========================================================================== #
class TestAlphaCapitalReadModel:
    def test_the_owner_republishes_the_frontier_it_already_composed(self):
        from paper_trader.api import alpha_capital as ak
        payload = ak.load_alpha_capital(
            cash_frontier={"deployment_ladder": {"rungs": [{"label": "r1"}]},
                           "redeployment_ladder": {"rungs": []},
                           "provenance": {"allocation_hash": "abc"},
                           "eligible_market_date": "2026-09-04"},
            registry={}, shadow_portfolios={}, tournament={}, research_agent={},
            forward_evidence={}, capital_pool={}, desk_performance={})
        assert payload["deployment_ladder"] == {"rungs": [{"label": "r1"}]}
        assert payload["redeployment_ladder"] == {"rungs": []}
        assert payload["cash_deployment_frontier_provenance"] == {
            "allocation_hash": "abc"}

    def test_it_recomputes_nothing_and_stays_read_only(self):
        from paper_trader.api import alpha_capital as ak
        src = (REPO / "api" / "alpha_capital.py").read_text(encoding="utf-8")
        # The republished fields are read straight off the composed owner answer.
        assert 'get("deployment_ladder")' in src
        assert 'get("redeployment_ladder")' in src
        for forbidden in ("place_order(", "submit_order(", "create_order(",
                          "run_daily_close(", "run_fill_cycle(", "promote_"):
            assert forbidden not in src, forbidden
        assert ak.load_alpha_capital.__doc__


class TestAlphaCapitalView:
    @pytest.fixture(scope="class")
    def ui(self):
        return UI.read_text(encoding="utf-8", errors="replace")

    @pytest.fixture(scope="class")
    def loader(self, ui):
        return ui[ui.index("async function loadAlphaCapital("):
                  ui.index("window.loadAlphaCapital = loadAlphaCapital;")]

    def test_the_view_no_longer_runs_the_allocator_twice(self, loader):
        # One page load must issue ONE heavy read, not two of the same
        # composition concurrently.
        assert loader.count("_mhzGet(") == 3
        assert "_mhzGet('/v1/operations/cash-deployment-frontier'" not in loader
        assert "_mhzGet('/v1/operations/alpha-capital'" in loader

    def test_the_ladders_and_provenance_come_from_the_alpha_capital_payload(
            self, loader):
        assert "d.deployment_ladder" in loader
        assert "d.redeployment_ladder" in loader
        assert "d.cash_deployment_frontier_provenance" in loader

    def test_the_heavy_read_gets_a_budget_above_its_real_cost(self, ui, loader):
        assert "_R601_HEAVY_READ_TIMEOUT_MS" in ui
        m = re.search(r"var _R601_HEAVY_READ_TIMEOUT_MS = (\d+);", ui)
        assert m, "the heavy-read budget must be a declared constant"
        # The composition is measured in tens of seconds on the live book; the
        # shared 45s budget was below its real cost, which IS the defect.
        assert int(m.group(1)) > 45000
        assert "_R601_HEAVY_READ_TIMEOUT_MS" in loader

    def test_an_unanswered_read_model_leaves_no_panel_saying_loading(
            self, ui, loader):
        # The live symptom: the loader set the headline and returned, so ten
        # panels read "Loading..." forever - indistinguishable from a read still
        # in flight.
        assert "_akClearPanels(" in loader
        assert "function _akClearPanels(" in ui
        panels = re.search(r"var _AK_PANEL_IDS = \[(.*?)\];", ui, re.S)
        assert panels, "the panel list must be declared"
        for pid in ("ak-kpis", "ak-cash-body", "ak-limiter-body",
                    "ak-zerobase-body", "ak-scoreboard-body", "ak-registry-body",
                    "ak-answer-body", "ak-next-body", "ak-maturity-body",
                    "ak-audit-body"):
            assert pid in panels.group(1), pid

    def test_the_frontier_route_is_still_named_as_the_provenance_source(self, ui):
        # The route is unchanged and still served; the view simply stopped paying
        # for it twice. Naming it keeps the provenance honest and the endpoint
        # out of the orphan report.
        assert "'/v1/operations/cash-deployment-frontier'" in ui

    def test_the_view_stays_read_only_and_dialog_free(self, loader):
        for forbidden in ("method: 'POST'", "alert(", "confirm(", "prompt("):
            assert forbidden not in loader, forbidden

    def test_the_generic_read_helper_keeps_its_default_budget(self, ui):
        # The per-call budget is opt-in; every other read is unchanged.
        assert "function _mhzGet(path, timeoutMs)" in ui
        assert "var _R29_READ_TIMEOUT_MS = 45000;" in ui
