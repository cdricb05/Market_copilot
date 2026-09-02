"""Release 54.2.1 — MISSED ELIGIBLE SESSION RECOVERY.

THE LIVE DEFECT THIS SUITE LOCKS OUT
------------------------------------
On 2026-09-01 the operator could not run the Daily Close: the workflow correctly
failed closed on OWNED_DATA_NOT_CONFIRMED. On the morning of 2026-09-02 the same
payload reported:

    engine.market_session  session_status                        BEFORE_SESSION_CLOSE
                           expected_completed_market_date        2026-09-01
                           latest_eligible_completed_market_date 2026-08-31
    api.daily_close        latest_completed_close_date           2026-08-31
    api.workflow_state     overall_state                         WAITING_FOR_SESSION_CLOSE
                           next action    "Wait for the market session to close"
                           operator_command.next_text  "No action required right now."

…while the Daily Close owner, which probes the owned provider, reported
"SEPTEMBER 1 EOD DATA READY — RUN DAILY CLOSE". The 2026-09-01 obligation was not
resolved, forfeited or blocked: it silently ceased to exist when the wall clock
rolled forward, and no operator control could recover it.

THE CAUSE. ``eligible_session_closed`` is computed against the OWNED-DATA-CONFIRMED
session, and owned-data confirmation only advances when a close runs — so an
unclosed completed session can never confirm itself. The gate that claims "the
latest eligible completed session is already fully processed" was therefore asking
about a session that was NOT the one the operator owed.

THE RULE. The obligation is a CALENDAR question against the CLOSE JOURNAL:

    last_closed_session  = api.daily_close        (which session was processed)
    through              = engine.market_session  (expected completed session)
    recovery_session     = the OLDEST completed session in between

Every test below is deterministic and offline: no clock is read, no store is
written, no provider or prediction call is made, and no production artifact is
touched.
"""
from __future__ import annotations

import copy
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from paper_trader.api import active_manager_state as ams
from paper_trader.api import daily_close as dc
from paper_trader.api import operator_presentation as op
from paper_trader.api import portfolio_cycle as pc
from paper_trader.api import workflow_state as ws
from paper_trader.engine import market_session as ms

ET = ZoneInfo("America/New_York")
REPO = Path(__file__).resolve().parents[1]
UI_SRC = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8",
                                                        errors="replace")
APP_SRC = (REPO / "api" / "app.py").read_text(encoding="utf-8", errors="replace")
WS_SRC = (REPO / "api" / "workflow_state.py").read_text(encoding="utf-8",
                                                        errors="replace")
PC_SRC = (REPO / "api" / "portfolio_cycle.py").read_text(encoding="utf-8",
                                                         errors="replace")
AMS_SRC = (REPO / "api" / "active_manager_state.py").read_text(encoding="utf-8",
                                                               errors="replace")

# The live 2026-09-02 09:01 ET world, replayed through the repaired owners.
NOW_SEP2_MORNING = datetime(2026, 9, 2, 9, 1, tzinfo=ET)
AUG31, SEP1, SEP2 = "2026-08-31", "2026-09-01", "2026-09-02"

_OP = {"operational_book": {
    "book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1",
    "current_status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
    "nav_as_of_date": AUG31, "desk_mark_date": AUG31,
    "latest_desk_mark_date": AUG31, "nav": 99113.22, "cash": 4482.71,
    "holdings_count": 25, "pending_order_count": 0,
    "current_target": {"alpha_market_date": AUG31,
                       "latest_completed_market_date": AUG31}}}
_INPUTS = {"market_as_of_date": AUG31, "momentum_month": "2026-08",
           "fundamental_as_of_date": "2026-05-22"}
_DAILY = {"status": "DAILY_STATUS_READY", "latest_valid_mark_date": AUG31}
_DESK = {"series": {"SPY": [["2026-08-28", 760.0], [AUG31, 771.33]]},
         "latest_completed_date": AUG31}
_CLOSE = {"market_date": AUG31, "done": True,
          "final_close_status": "DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT",
          "status": "CLOSE_FINISHED"}
_FWD = {"latest_snapshot_date": AUG31, "snapshot_count": 6,
        "evidence_state": "FORWARD_EVIDENCE_COMPLETE", "interpretation": "complete",
        "active_book": {"model_id": "fundamental_momentum_50_50_v1", "book_id": "x"},
        "shadow_books": []}
_GATE = {"latest_completed_market_date": AUG31, "outcome": "NO_ACTION_TODAY",
         "headline": "No portfolio change required.", "target_state": "CURRENT_ALIGNED",
         "action_required": False, "next_scheduled_full_review": "2026-10-01",
         "scheduled_review_due": False, "opportunity_cost_available": True,
         "opportunity_cost_state": "AVAILABLE"}
_TR = {"dates": {"alpha_market_date": AUG31}}
_DRC = {"state": "COMPLETE", "governed_research_evidence_current": True,
        "blockers": []}
_REAS = {"reassessment_available": True, "reassessment_state": "HOLD_CURRENT_BOOK",
         "proposal_required": False, "blockers": []}


def _load(**kw) -> dict:
    """The live Sep-2 morning world; every seam bound (nothing reads a real store)."""
    args = dict(now=NOW_SEP2_MORNING, operational=copy.deepcopy(_OP),
                inputs=dict(_INPUTS), daily_status=dict(_DAILY),
                desk_marks=copy.deepcopy(_DESK), close_progress=dict(_CLOSE),
                forward_status=copy.deepcopy(_FWD), gate=dict(_GATE),
                target_readiness=copy.deepcopy(_TR), research_cycle=dict(_DRC),
                reassessment_summary=dict(_REAS), decision_record={})
    args.update(kw)
    return ws.load_workflow_state(**args)


def _recovery(**kw) -> dict:
    base = dict(expected_completed_market_date=SEP1, eligible_market_date=AUG31,
                latest_completed_close_date=AUG31, operational_close_valid=True,
                latest_confirmed_owned_data_date=AUG31,
                session_status=ms.BEFORE_SESSION_CLOSE)
    base.update(kw)
    return ws.build_session_recovery(**base)


_DECIDE_DEFAULTS = dict(
    inconsistent=False, session_status=ms.SESSION_READY, has_confirmed_eligible=True,
    eligible_session_closed=True, owned_data_lag=False, research_current=True,
    assessment_status=ws.ASSESS_CURRENT, manual_review_required=False,
    evidence_gap=False)


def _decide(**kw) -> str:
    args = dict(_DECIDE_DEFAULTS)
    args.update(kw)
    return ws._decide_overall(**args)   # noqa: SLF001


# =========================================================================== #
# 1-9  THE CANONICAL CATCH-UP RULE (session selection).
# =========================================================================== #
class TestSessionSelection:
    def test_01_no_missed_session_keeps_normal_current_session_behaviour(self):
        """The close journal has reached the expected session: nothing is owed."""
        r = _recovery(latest_completed_close_date=SEP1,
                      latest_confirmed_owned_data_date=SEP1,
                      eligible_market_date=SEP1)
        assert r["recovery_state"] == ws.NO_CATCH_UP_REQUIRED
        assert r["catch_up_required"] is False
        assert r["recovery_session"] is None
        assert r["missed_completed_sessions"] == []
        assert r["next_action"] == "NONE"
        # …and the workflow keeps saying "wait", exactly as before.
        assert _decide(session_status=ms.BEFORE_SESSION_CLOSE,
                       eligible_session_closed=True,
                       catch_up_required=False) == ws.WAITING_FOR_SESSION_CLOSE

    def test_02_one_missed_completed_session_is_CATCH_UP_REQUIRED(self):
        r = _recovery()
        assert r["recovery_state"] == ws.CATCH_UP_REQUIRED
        assert r["catch_up_required"] is True
        assert r["recovery_session"] == SEP1
        assert r["missed_completed_sessions"] == [SEP1]
        assert r["last_closed_session"] == AUG31
        assert r["next_action"] == "RUN_PORTFOLIO_CYCLE"

    def test_03_owned_data_unavailable_is_CATCH_UP_WAITING_FOR_OWNED_DATA(self):
        """The SESSION owner's own owned-data verdict is what makes this WAITING —
        never the absence of an ingested mark, which is a publish/ingest gap."""
        r = _recovery(session_status=ms.WAITING_FOR_OWNED_DATA, owned_data_lag=True)
        assert r["recovery_state"] == ws.CATCH_UP_WAITING_FOR_OWNED_DATA
        assert r["recovery_data_state"] == ws.RECOVERY_DATA_LAGGING
        assert r["recovery_data_ready"] is False
        assert r["next_action"] == "WAIT_FOR_OR_REFRESH_OWNED_DATA"
        assert r["recovery_session"] == SEP1      # the obligation is NOT erased

    def test_04_a_still_open_session_never_hides_an_older_missed_one(self):
        """This is the exact live payload: BEFORE_SESSION_CLOSE with the eligible
        date one session behind the expected one."""
        r = _load()
        assert r["current_session"]["session_status"] == ms.BEFORE_SESSION_CLOSE
        assert r["current_session"]["latest_eligible_completed_market_date"] == AUG31
        assert r["current_session"]["expected_completed_market_date"] == SEP1
        assert r["overall_state"] == ws.READY_FOR_DAILY_CLOSE
        assert r["overall_state"] != ws.WAITING_FOR_SESSION_CLOSE
        assert r["session_recovery"]["recovery_session"] == SEP1
        # The still-forming Sep-2 session is NEVER the recovery target.
        assert SEP2 not in r["session_recovery"]["missed_completed_sessions"]

    def test_05_two_missed_sessions_recover_the_OLDEST_first(self):
        """Machine offline Monday+Tuesday; Wednesday morning must require Monday."""
        r = ws.build_session_recovery(
            expected_completed_market_date="2026-09-08",   # Tue
            eligible_market_date="2026-09-04",
            latest_completed_close_date="2026-09-04",      # Fri
            operational_close_valid=True,
            latest_confirmed_owned_data_date="2026-09-04",
            session_status=ms.BEFORE_SESSION_CLOSE,
            authoritative_non_sessions=["2026-09-07"])     # Labor Day
        assert r["missed_completed_sessions"] == ["2026-09-08"]
        # …and without the holiday, Monday comes first and is never skipped.
        r2 = ws.build_session_recovery(
            expected_completed_market_date="2026-09-02",
            eligible_market_date="2026-08-31",
            latest_completed_close_date="2026-08-28", operational_close_valid=True,
            latest_confirmed_owned_data_date="2026-08-31",
            session_status=ms.SESSION_READY)
        assert r2["missed_completed_sessions"] == ["2026-08-31", "2026-09-01",
                                                   "2026-09-02"]
        assert r2["recovery_session"] == "2026-08-31"
        assert r2["oldest_first"] is True

    def test_06_weekends_work_without_any_caller_arithmetic(self):
        """Friday closed, Monday expected: Saturday and Sunday are not sessions."""
        cal = ms.completed_sessions_after("2026-08-28", through="2026-08-31")
        assert cal["sessions"] == ("2026-08-31",)
        r = ws.build_session_recovery(
            expected_completed_market_date="2026-08-31",
            eligible_market_date="2026-08-28",
            latest_completed_close_date="2026-08-28", operational_close_valid=True,
            latest_confirmed_owned_data_date="2026-08-28",
            session_status=ms.SESSION_READY)
        assert r["missed_completed_sessions"] == ["2026-08-31"]

    def test_07_an_authoritative_market_holiday_is_skipped(self):
        cal = ms.completed_sessions_after("2026-09-04", through="2026-09-08",
                                          non_sessions=["2026-09-07"])
        assert cal["sessions"] == ("2026-09-08",)
        assert cal["skipped_non_sessions"] == ("2026-09-07",)

    def test_08_no_wall_clock_today_minus_one_arithmetic_anywhere(self):
        """The obligation is bounded by the EXPECTED completed session the session
        owner resolved — never by a date derived from the wall clock."""
        assert "date.today()" not in WS_SRC
        assert "datetime.now(" not in _recovery_source()
        # An absent bound is never an obligation (no silent "yesterday" default).
        assert ws.build_session_recovery(
            expected_completed_market_date=None, eligible_market_date=None,
            latest_completed_close_date=None, operational_close_valid=False,
        )["recovery_state"] == ws.NO_CATCH_UP_REQUIRED

    def test_09_the_recovery_session_comes_from_the_market_session_owner(self):
        assert "msession.completed_sessions_after(" in WS_SRC
        assert _recovery()["calendar_owner"] == "engine.market_session"
        assert _recovery()["close_owner"] == "api.daily_close"
        # …and the enumeration itself is defined only there.
        assert hasattr(ms, "completed_sessions_after")
        assert "def completed_sessions_after(" not in WS_SRC


def _recovery_source() -> str:
    i = WS_SRC.find("def build_session_recovery(")
    j = WS_SRC.find("\ndef ", i + 10)
    return WS_SRC[i:j]


def _code_only(src: str) -> str:
    """Strip comments and string literals so a token search matches CODE, not the
    prose that explains why the code is safe (the audit's oldest false positive)."""
    out = re.sub(r"#.*", "", src)
    out = re.sub(r'"""(?:.|\n)*?"""', "", out)
    out = re.sub(r'"[^"\n]*"', '""', out)
    return re.sub(r"'[^'\n]*'", "''", out)


# =========================================================================== #
# 10-14  IDEMPOTENCY / IMMUTABILITY / STAGE-AWARE RESUME.
# =========================================================================== #
class TestIdempotencyAndResume:
    def test_10_a_completed_daily_close_is_never_offered_twice(self):
        """Once the close journal records the session, the obligation disappears —
        and the cycle stops before invoking any owner."""
        r = _load(close_progress={**_CLOSE, "market_date": SEP1},
                  operational={"operational_book": {
                      **_OP["operational_book"], "desk_mark_date": SEP1,
                      "latest_desk_mark_date": SEP1, "nav_as_of_date": SEP1}},
                  desk_marks={"series": {"SPY": [[AUG31, 771.3], [SEP1, 773.0]]},
                              "latest_completed_date": SEP1},
                  gate={**_GATE, "latest_completed_market_date": SEP1},
                  inputs={**_INPUTS, "market_as_of_date": SEP1},
                  daily_status={**_DAILY, "latest_valid_mark_date": SEP1},
                  forward_status={**_FWD, "latest_snapshot_date": SEP1},
                  target_readiness={"dates": {"alpha_market_date": SEP1}})
        assert r["session_recovery"]["recovery_state"] == ws.NO_CATCH_UP_REQUIRED
        assert r["overall_state"] != ws.READY_FOR_DAILY_CLOSE
        calls: list[str] = []
        out = pc.run_portfolio_cycle(
            confirm=pc.EXECUTE_CONFIRMATION, workflow_loader=lambda: r,
            close_runner=lambda **kw: calls.append("close") or {},
            drc_runner=lambda **kw: calls.append("drc") or {})
        assert calls == []
        assert out["performed_write"] is False

    def test_11_a_completed_close_with_a_missing_DRC_resumes_at_the_DRC(self):
        assert _decide(catch_up_required=False, eligible_session_closed=True,
                       research_current=False) == ws.RESEARCH_CYCLE_REQUIRED
        wf = {"overall_state": ws.RESEARCH_CYCLE_REQUIRED,
              "primary_action": ws._primary_action(                      # noqa: SLF001
                  ws.RESEARCH_CYCLE_REQUIRED, {"eligible_date": SEP1}),
              "session_recovery": _recovery(latest_completed_close_date=SEP1,
                                            eligible_market_date=SEP1,
                                            latest_confirmed_owned_data_date=SEP1)}
        assert pc.plan_next_step(wf)["step"] == pc.STEP_DAILY_RESEARCH_CYCLE

    def test_12_a_completed_DRC_with_no_decision_stops_at_the_decision_boundary(self):
        for state in (ws.MANUAL_REVIEW_REQUIRED, ws.DAILY_CYCLE_COMPLETE,
                      ws.DAILY_CYCLE_COMPLETE_EVIDENCE_GAP):
            plan = pc.plan_next_step({"overall_state": state, "primary_action": {}})
            assert plan["step"] is None
            assert plan["stop_reason"] == pc.STOP_DECISION_PRESENTED

    def test_13_a_fully_completed_session_is_idempotent_across_runs(self):
        wf = {"overall_state": ws.DAILY_CYCLE_COMPLETE, "primary_action": {},
              "session_recovery": _recovery(latest_completed_close_date=SEP1,
                                            eligible_market_date=SEP1,
                                            latest_confirmed_owned_data_date=SEP1)}
        calls: list[str] = []
        for _ in range(2):
            out = pc.run_portfolio_cycle(
                confirm=pc.EXECUTE_CONFIRMATION, workflow_loader=lambda: wf,
                close_runner=lambda **kw: calls.append("close") or {},
                drc_runner=lambda **kw: calls.append("drc") or {})
            assert out["stop_reason"] == pc.STOP_DECISION_PRESENTED
        assert calls == []

    def test_14_the_recovery_projection_rewrites_no_history(self):
        """It is a pure read: no store, no write, no owner mutation. (Only CODE is
        matched — the prose explains the close's fail-closed behaviour and must not
        be mistaken for a write path.)"""
        src = _code_only(_recovery_source())
        for forbidden in ("open(", ".write(", "_atomic_write", "unlink(", "rmtree",
                          "requests.", "urlopen", "json.dump"):
            assert forbidden not in src, forbidden
        assert pc.load_portfolio_cycle(workflow=_load())["safety"]["owns_no_store"]


# =========================================================================== #
# 15-16  POINT-IN-TIME SAFETY (never manufacture evidence for a past session).
# =========================================================================== #
class TestPointInTimeSafety:
    def test_15_the_bound_session_narrows_the_clock_and_never_advances_it(self):
        """A binding may only ever look BACKWARD: today's still-forming session can
        never be substituted for the session being recovered."""
        clock = dc._resolve_clock(today=SEP2, target_market_date=SEP1)  # noqa: SLF001
        assert clock["expected_market_date"] == SEP1
        assert clock["session_binding"] == SEP1
        assert clock["session_binding_owner"] == "api.workflow_state"
        assert clock["session_binding_rejected"] is None
        # A FORWARD binding is refused, not clamped.
        ahead = dc._resolve_clock(today=SEP2, target_market_date=SEP2)  # noqa: SLF001
        assert ahead["session_binding_rejected"] == dc.BINDING_REJECTED_FUTURE
        assert ahead["expected_market_date"] == SEP1
        assert ahead["session_binding"] is None
        # A genuinely older session narrows the clock (Friday recovered on Tuesday).
        older = dc._resolve_clock(today="2026-09-08",                   # noqa: SLF001
                                  target_market_date="2026-09-04")
        assert older["expected_market_date"] == "2026-09-04"
        assert older["clock_expected_market_date"] == "2026-09-07"

    def test_16_every_date_dependent_close_step_reads_the_bound_session(self):
        """The bound session is the ONE value the close threads into the provider
        probe, the desk-mark refresh window and the model-input refresh, so a
        recovered session is priced from ITS OWN owned bars."""
        src = (REPO / "api" / "daily_close.py").read_text(encoding="utf-8")
        assert 'latest_eligible = clock["expected_market_date"]' in src
        assert "completed_through=latest_eligible" in src
        assert "_run_alpha_refresh(completed_through=latest_eligible" in src
        assert "_run_probe(expected=latest_eligible" in src

    def test_16b_a_refused_binding_fails_closed_and_writes_nothing(self, tmp_path):
        out = dc.run_daily_close(
            confirm=dc.EXECUTE_CONFIRMATION, today=SEP2,
            target_market_date=SEP2,           # ahead of the expected session
            desk_dir=str(tmp_path / "desk"), ledger_dir=str(tmp_path / "ledger"),
            operational_loader=lambda *_a, **_k: {},
            gate_loader=lambda *_a, **_k: {})
        assert out["performed_write"] is False
        assert out["close_status"] == dc.AWAITING_MARKET_CLOSE
        assert dc.BINDING_REJECTED_FUTURE in out["operator_message"]
        assert out["clock"]["session_binding_rejected"] == dc.BINDING_REJECTED_FUTURE
        assert out["clock"]["session_binding"] is None
        # The clock's own expectation is left in place — never clamped to the target.
        assert out["clock"]["expected_market_date"] == SEP1

    def test_17_missing_point_in_time_evidence_fails_closed(self):
        """When the session owner reports owned data has not reached the session,
        the recovery state says WAITING and offers no safe action."""
        r = _recovery(session_status=ms.WAITING_FOR_OWNED_DATA, owned_data_lag=True)
        assert r["recovery_state"] == ws.CATCH_UP_WAITING_FOR_OWNED_DATA
        assert r["next_action"] != "RUN_PORTFOLIO_CYCLE"
        pres = op.build_operator_presentation(
            workflow={"status": "OK", "session_recovery": r},
            daily_close={"provider_readiness": {"provider_latest_date": AUG31,
                                                "ready": False}})
        assert pres["session_recovery"]["next_action_kind"] != "PORTFOLIO_CYCLE"
        assert pres["session_recovery"]["next_action_label"] == \
            "No action is currently safe"
        assert pres["session_recovery"]["provider_covers_recovery_session"] is False


# =========================================================================== #
# 18-21  ONE ORCHESTRATION PATH; the SERVER decides the date.
# =========================================================================== #
class TestOneOrchestrationPath:
    def test_18_recovery_uses_the_ONE_canonical_portfolio_cycle_action(self):
        r = _load()
        cmd = r["operator_command"]
        assert cmd["primary_action_available"] is True
        assert cmd["primary_action_code"] == "RUN_PORTFOLIO_CYCLE"
        assert cmd["primary_action_kind"] == ws.EXEC_PORTFOLIO_CYCLE
        assert cmd["confirmation_required"] == ws.PORTFOLIO_CYCLE_CONFIRMATION
        assert cmd["primary_action_execution_contract"]["path"] == pc.RUN_ROUTE
        assert r["primary_action"]["execution_kind"] == ws.EXEC_DAILY_CLOSE
        # The command names the session that will actually be processed.
        assert cmd["eligible_market_date"] == SEP1
        assert r["action_session_market_date"] == SEP1

    def test_19_there_is_no_recovery_specific_write_route(self):
        for path in ("/recover-close", "/backfill-close", "/session-recovery/run",
                     "/operations/catch-up", "/daily-close/backfill",
                     "/daily-close/recover", "/portfolio-cycle/recover"):
            assert path not in APP_SRC, path
        assert APP_SRC.count('"%s"' % pc.RUN_ROUTE) + \
            APP_SRC.count("'%s'" % pc.RUN_ROUTE) >= 1

    def test_20_the_operator_never_supplies_the_recovery_date(self):
        r = _load()["session_recovery"]
        assert r["operator_supplies_no_date"] is True
        assert r["recovery_specific_route"] is None
        assert r["orchestration_path"] == pc.RUN_ROUTE
        # The binding is not a request field on any route.
        assert "target_market_date" not in APP_SRC
        # The cycle READS it from the workflow owner; it derives no date.
        binding = pc.recovery_binding(_load())
        assert binding["bound_market_date"] == SEP1
        assert binding["binding_owner"] == "api.workflow_state"
        assert "date.today()" not in PC_SRC

    def test_21_the_cycle_binds_the_recovery_session_when_it_runs_the_close(self):
        seen: list[dict] = []

        def close_runner(*, requested_by, target_market_date=None):
            seen.append({"requested_by": requested_by,
                         "target_market_date": target_market_date})
            return {"status": dc.CLOSE_COMPLETE_HOLD, "performed_write": True,
                    "market_date": target_market_date}

        states = [_load(), {"overall_state": ws.DAILY_CYCLE_COMPLETE,
                            "primary_action": {}}]
        out = pc.run_portfolio_cycle(
            confirm=pc.EXECUTE_CONFIRMATION,
            workflow_loader=lambda: states.pop(0) if states else states,
            close_runner=close_runner,
            drc_runner=lambda **kw: pytest.fail("the DRC must not run here"))
        assert seen and seen[0]["target_market_date"] == SEP1
        assert seen[0]["requested_by"].startswith("portfolio_cycle:")
        assert out["steps"][0]["bound_market_date"] == SEP1
        assert out["session_recovery"]["binding_owner"] == "api.workflow_state"

    def test_21b_a_runner_seam_without_the_parameter_still_runs_unbound(self):
        """A legacy/fake runner must degrade, never raise."""
        calls: list[str] = []
        states = [_load(), {"overall_state": ws.DAILY_CYCLE_COMPLETE,
                            "primary_action": {}}]
        pc.run_portfolio_cycle(
            confirm=pc.EXECUTE_CONFIRMATION,
            workflow_loader=lambda: states.pop(0) if states else states,
            close_runner=lambda *, requested_by: calls.append(requested_by) or {},
            drc_runner=lambda **kw: {})
        assert len(calls) == 1


# =========================================================================== #
# 22-24  WORKFLOW PRIORITY + ACTIVE MANAGER STATE DELEGATION.
# =========================================================================== #
class TestPriorityAndDelegation:
    def test_22_a_missed_close_outranks_WAITING_FOR_SESSION_CLOSE(self):
        assert _decide(session_status=ms.BEFORE_SESSION_CLOSE,
                       eligible_session_closed=True,
                       catch_up_required=True) == ws.READY_FOR_DAILY_CLOSE
        assert _decide(session_status=ms.BEFORE_SESSION_CLOSE,
                       eligible_session_closed=True,
                       catch_up_required=False) == ws.WAITING_FOR_SESSION_CLOSE

    def test_23_genuine_safety_blockers_still_outrank_a_missed_close(self):
        """Ladder: safety blockers > missed-session recovery > current waiting."""
        assert _decide(inconsistent=True, catch_up_required=True) == \
            ws.INCONSISTENT_STATE
        assert _decide(session_status=ms.INCONSISTENT_FUTURE_DATA,
                       catch_up_required=True) == ws.INCONSISTENT_STATE
        assert _decide(has_confirmed_eligible=False,
                       catch_up_required=True) == ws.WAITING_FOR_OWNED_DATA
        assert _decide(cycle_running=True,
                       catch_up_required=True) == ws.RESEARCH_CYCLE_RUNNING
        assert _decide(cycle_blocked=True,
                       catch_up_required=True) == ws.RESEARCH_CYCLE_BLOCKED

    def test_24_active_manager_state_DELEGATES_the_recovery_state(self):
        wf = _load()
        d = ams.build_active_manager_state(workflow=wf)
        rec = d["session_recovery"]
        assert rec["available"] is True
        assert rec["delegated"] is True and rec["computed_here"] is False
        assert rec["owner"] == "api.workflow_state"
        assert rec["recovery_state"] == wf["session_recovery"]["recovery_state"]
        assert rec["recovery_session"] == SEP1
        assert rec["last_closed_session"] == AUG31
        assert rec["missed_completed_sessions"] == [SEP1]
        assert d["operator_guidance"]["session_recovery"] == rec
        assert d["time_state"]["operational"]["recovery_session"] == SEP1
        # It computes NO session date of its own.
        assert "completed_sessions_after" not in AMS_SRC
        assert "def build_session_recovery(" not in AMS_SRC
        # A missing workflow payload degrades honestly.
        assert ams.build_active_manager_state()["session_recovery"] == {
            "available": False, "recovery_state": "UNAVAILABLE",
            "owner": "api.workflow_state", "delegated": True,
            "computed_here": False,
            "detail": ("The workflow owner did not publish a session-recovery "
                       "contract; nothing is inferred in its place.")}


# =========================================================================== #
# 25-31  OPERATOR UX (Phases I and J) — every value from the backend.
# =========================================================================== #
def _pres(**kw) -> dict:
    wf = _load()
    args = dict(
        workflow=wf,
        daily_close={"provider_readiness": {"provider_latest_date": SEP1,
                                            "ready": True},
                     "pnl": {"valuation_date": AUG31, "daily_pnl": -270.0,
                             "daily_pnl_available": True,
                             "daily_return_pct": -0.27}},
        constrained={"best_feasible_target": {"allocations": []},
                     "switching_economics": {"clears_switching_hurdle": False},
                     "feasible_target_exists": True})
    args.update(kw)
    return op.build_operator_presentation(**args)


class TestOperatorUX:
    def test_25_today_visibly_names_the_missed_session(self):
        rec = _pres()["session_recovery"]
        assert rec["active"] is True
        assert rec["headline"] == "CATCH UP REQUIRED"
        assert rec["recovery_session"] == SEP1
        assert rec["recovery_session_display"] == "Sep 1, 2026"
        assert rec["detail"] == "Sep 1, 2026 was not closed."
        # The owned-provider answer travels beside the obligation.
        assert rec["owned_data_line"] == "READY"
        assert rec["provider_covers_recovery_session"] is True
        # The Today region exists, is backend-driven and is hidden by default.
        assert 'id="today-session-recovery"' in UI_SRC
        assert "function _opRenderSessionRecovery(" in UI_SRC
        assert "p.session_recovery" in UI_SRC

    def test_26_today_uses_the_normal_portfolio_cycle_control_only(self):
        rec = _pres()["session_recovery"]
        assert rec["next_action_kind"] == op.NA_PORTFOLIO_CYCLE
        assert rec["backfill_control_offered"] is False
        assert rec["force_close_control_offered"] is False
        assert rec["operator_supplies_no_date"] is True
        # Exactly ONE execution render site remains on the R49 surfaces, and the
        # recovery banner's control is explicitly navigation.
        assert UI_SRC.count('onclick="opresPrimaryAction(this)"') == 1
        assert "function opresFocusPrimaryAction(" in UI_SRC
        assert 'onclick="opresFocusPrimaryAction()"' in UI_SRC
        for forbidden in ("Backfill", "Force close", "Force Close"):
            assert forbidden not in _ui_recovery_renderer(), forbidden

    def test_26b_the_ui_performs_no_recovery_date_calculation(self):
        """Every date, label and verdict in the banner is rendered verbatim."""
        body = _ui_recovery_renderer()
        for forbidden in ("new Date(", "Date.now(", "getFullYear(", "getMonth(",
                          "setDate(", "86400", "toISOString(", "parseInt(",
                          "Math.", "> new ", "slice(0, 10)"):
            assert forbidden not in body, forbidden
        # It only reads backend fields.
        for field in ("recovery_session_display", "last_closed_session",
                      "owned_data_line", "next_action_kind", "headline"):
            assert "r." + field in body, field

    def test_27_the_pnl_label_is_never_TODAY_for_an_older_operational_mark(self):
        s = _pres()["portfolio_snapshot"]
        assert s["daily_pnl_session_date"] == AUG31
        assert s["daily_pnl_is_current_calendar_day"] is False
        assert s["daily_pnl_period_label"] == op.PNL_LABEL_LAST_CLOSED
        assert s["daily_pnl_period_label"] != op.PNL_LABEL_TODAY
        assert s["daily_pnl_session_display"] == "Aug 31, 2026"
        # …and it IS "TODAY" when the mark really is the current calendar day.
        s2 = op.build_operator_presentation(
            workflow={"status": "OK", "current_session": {"calendar_date": SEP1}},
            daily_close={"pnl": {"valuation_date": SEP1, "daily_pnl": 1.0,
                                 "daily_pnl_available": True}})["portfolio_snapshot"]
        assert s2["daily_pnl_period_label"] == op.PNL_LABEL_TODAY
        # The UI renders the backend label; it performs no date comparison.
        assert "s.daily_pnl_period_label" in UI_SRC
        assert "row('Today'," not in UI_SRC

    def test_28_a_rejected_feasible_target_is_labelled_non_recommended(self):
        ds = _pres()["decision_summary"]
        assert ds["target_class"] == "REJECTED_FEASIBLE_ALTERNATIVE"
        assert ds["target_class_label"] == "REJECTED FEASIBLE ALTERNATIVE"
        assert ds["is_recommended_portfolio"] is False
        assert ds["renders_approval_cta"] is False
        assert "NOT THE RECOMMENDED PORTFOLIO" in ds["not_recommended_banner"]
        # The analysis is NOT hidden — the counts are still published.
        assert "exits" in ds and "additions" in ds and "increases" in ds
        assert "REJECTED_FEASIBLE_ALTERNATIVE" in UI_SRC
        assert "ds.not_recommended_banner" in UI_SRC

    def test_29_raw_audit_state_cannot_masquerade_as_an_authoritative_change(self):
        raw = _pres()["raw_states"]
        assert raw["label"] == "RAW / NON-AUTHORITATIVE DIAGNOSTIC STATE"
        assert raw["authoritative"] is False
        assert raw["actionable"] is False
        assert raw["renders_cta"] is False
        assert "RAW / NON-AUTHORITATIVE DIAGNOSTIC STATE" in UI_SRC
        # The live 2026-09-02 contradiction: a raw proposal-ready artifact token
        # beside an authoritative HOLD. It is flagged, never rendered as an action.
        wf = {"status": "OK",
              "canonical_portfolio_decision": {"state": "HOLD_CURRENT_BOOK"},
              "reallocation_proposal_presentation": {
                  "state": "REALLOCATION_PROPOSAL_READY"},
              "portfolio_attention": {"review_required": False}}
        clash = op.build_operator_presentation(workflow=wf)["raw_states"]
        assert clash["disagrees_with_authoritative_decision"] is True
        assert clash["authoritative_decision_state"] == "HOLD_CURRENT_BOOK"
        assert clash["manual_review_required"] is False
        assert "no review CTA may be rendered" in clash["disagreement_note"]
        # …and when a review REALLY is required, it is not flagged as a clash.
        wf2 = dict(wf, portfolio_attention={"review_required": True})
        assert op.build_operator_presentation(workflow=wf2)[
            "raw_states"]["disagrees_with_authoritative_decision"] is False

    def test_30_a_HOLD_decision_produces_no_review_proposal_cta(self):
        p = _pres()
        assert p["portfolio_decision"]["state"] != "AWAITING_APPROVAL"
        assert p["raw_states"]["manual_review_required"] is False
        assert p["decision_summary"]["renders_approval_cta"] is False
        # The raw-state renderer never emits an action control.
        i = UI_SRC.find("function _opRenderRawStates(")
        j = UI_SRC.find("\nfunction ", i + 10)
        body = UI_SRC[i:j]
        assert "opresPrimaryAction" not in body
        assert "<button" not in body

    def test_31_freshness_wording_names_the_governed_session(self):
        items = {i["key"]: i for i in _pres()["system_readiness"]["items"]}
        elig = items["eligible_session"]
        assert elig["label"] == "Eligible session (governed)"
        assert elig["value"] == AUG31
        assert "Fresh for the governed session — %s" % AUG31 in elig["detail"]
        assert "Session recovery" in elig["detail"]
        # The outstanding session is its OWN row, not an absence to be noticed.
        assert items["session_recovery"]["label"] == "Session recovery"
        assert items["session_recovery"]["value"] == "Sep 1, 2026"
        # A missed session is work, not an incident: it never blocks the decision.
        assert items["session_recovery"]["blocks_portfolio_decision"] is False
        assert _pres()["system_readiness"]["portfolio_decision_remains_valid"] is True


def _ui_recovery_renderer() -> str:
    i = UI_SRC.find("function _opRenderSessionRecovery(")
    j = UI_SRC.find("\nfunction ", i + 10)
    return UI_SRC[i:j]


# =========================================================================== #
# 32  SAFETY — recovery adds no authority of any kind.
# =========================================================================== #
class TestSafety:
    def test_32_no_portfolio_mutation_beyond_the_canonical_close_semantics(self):
        src = _code_only(_recovery_source()).lower()
        for forbidden in ("approve", "confirm_order", "rebalance", "promote",
                          "recalibrat"):
            assert forbidden not in src, forbidden
        safety = pc.load_portfolio_cycle(workflow=_load())["safety"]
        assert safety["creates_orders"] is False
        assert safety["creates_fills"] is False
        assert safety["approves_proposals"] is False
        assert safety["executes_rebalance"] is False
        assert safety["automation"] == "OFF"
        assert safety["manual_review_required_for_portfolio_mutation"] is True

    def test_33_no_order_fill_or_broker_behaviour_is_added(self):
        """No CODE path to an order, a fill or a broker exists in the recovery lane.
        (The safety declarations that assert broker_enabled=False are data, not a
        path, so only code is matched.)"""
        for src in (_recovery_source(), PC_SRC, AMS_SRC):
            code = _code_only(src)
            for forbidden in ("broker_client", "create_order(", "submit_order(",
                              "create_fill(", "place_order("):
                assert forbidden not in code, forbidden

    def test_34_automation_remains_off_and_the_ui_shows_no_dialog(self):
        assert '"automation": "OFF"' in PC_SRC
        body = _ui_recovery_renderer()
        for forbidden in ("alert(", "confirm(", "prompt(", "setInterval("):
            assert forbidden not in body, forbidden
        # The recovery banner declares its own read-only nature.
        assert "No backfill, no force close, no date entry." in UI_SRC

    def test_35_the_whole_live_payload_stays_self_consistent(self):
        r = _load()
        assert r["consistency_status"] == ws.CONSISTENT
        assert r["consistency_violations"] == []
        assert r["safety"]["read_only"] is True


# =========================================================================== #
# 36  The frozen contract surface (vocabulary + schema).
# =========================================================================== #
class TestContract:
    def test_36_the_recovery_vocabulary_is_frozen(self):
        assert ws.SESSION_RECOVERY_STATES == (
            "NO_CATCH_UP_REQUIRED", "CATCH_UP_REQUIRED",
            "CATCH_UP_WAITING_FOR_OWNED_DATA", "CATCH_UP_BLOCKED")
        # Release 54.2.3.1 extends the data-state vocabulary with the provider-
        # confirmed-but-not-yet-closed state (the normal state of every owed close
        # between publish and close); the original three values are unchanged.
        assert ws.RECOVERY_DATA_STATES == (
            "CONFIRMED", "UNVERIFIED_UNTIL_CLOSE_REVALIDATES", "OWNED_DATA_LAGGING",
            "PROVIDER_CONFIRMED_AWAITING_CLOSE")

    def test_37_the_recovery_schema_exposes_every_required_field(self):
        r = _load()["session_recovery"]
        for field in ("recovery_session", "last_closed_session",
                      "current_open_or_next_session", "recovery_data_state",
                      "recovery_data_ready", "recovery_blockers", "next_action",
                      "recovery_state", "missed_completed_sessions", "summary"):
            assert field in r, field
        assert _load()["session_recovery_state"] == ws.CATCH_UP_REQUIRED
        assert _load()["catch_up_required"] is True

    def test_38_a_named_blocker_produces_CATCH_UP_BLOCKED(self):
        r = _recovery(inconsistent=True)
        assert r["recovery_state"] == ws.CATCH_UP_BLOCKED
        assert [b["code"] for b in r["recovery_blockers"]] == ["STATE_INCONSISTENT"]
        assert r["next_action"] == "RESOLVE_NAMED_BLOCKER"
        # A backlog longer than the calendar owner's bound is an OUTAGE, not a
        # hundred obligations.
        long_gap = _recovery(latest_completed_close_date="2026-01-02",
                             expected_completed_market_date=SEP1)
        assert long_gap["missed_session_backlog_truncated"] is True
        assert long_gap["recovery_state"] == ws.CATCH_UP_BLOCKED

    def test_38b_the_architecture_audit_enforces_the_single_recovery_owner(self):
        """Phase L — the build fails on a second recovery orchestrator, a second
        catch-up state owner, a recovery-specific write route or UI date math."""
        import sys
        sys.path.insert(0, str(REPO))
        from scripts import audit_architecture as audit   # noqa: PLC0415

        files = audit._iter_source_files()               # noqa: SLF001
        rep = audit.check_release54_2_1_missed_session_recovery(files)
        assert rep["owner_defs_missing"] == []
        assert rep["calendar_defs_missing"] == []
        assert rep["duplicate_state_owners"] == []
        assert rep["duplicate_calendar_owners"] == []
        assert rep["second_recovery_orchestrators"] == []
        assert rep["forbidden_routes_present"] == []
        assert rep["workflow_delegates_calendar"] is True
        assert rep["workflow_owns_no_calendar_walk"] is True
        assert rep["obligation_anchored_on_close"] is True
        assert rep["close_refuses_forward_binding"] is True
        assert rep["binding_is_not_a_request_field"] is True
        assert rep["ams_delegates_recovery"] is True
        assert rep["ui_recovery_derivation"] == []
        assert rep["ui_offers_no_date_entry"] is True
        assert rep["recovery_adds_automation"] is False
        assert rep["recovery_creates_orders"] is False
        # …and every one of those fields BLOCKS strict mode.
        keys = {f for k, f, _ in audit.BLOCKING_INVARIANTS
                if k == "release54_2_1_missed_session_recovery"}
        assert {"duplicate_state_owners", "second_recovery_orchestrators",
                "forbidden_routes_present", "ui_recovery_derivation"} <= keys

    def test_39_an_invalid_close_never_erases_the_obligation_it_failed(self):
        """A close the owner does not classify as complete is NOT a closed session."""
        r = _recovery(latest_completed_close_date=SEP1,
                      operational_close_valid=False)
        assert r["last_closed_session"] is None
        assert r["recovery_session"] == SEP1        # anchored on the eligible date
        assert r["catch_up_required"] is True
