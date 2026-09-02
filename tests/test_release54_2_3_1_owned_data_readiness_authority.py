"""Release 54.2.3.1 — OWNED-DATA READINESS AUTHORITY RECONCILIATION.

THE LIVE DEFECT THIS SUITE LOCKS OUT
------------------------------------
At ~2026-09-02 17:36 ET the system published contradictory authoritative states
for the owed Sep-2 close:

    api.daily_close        close_status DAILY_CLOSE_DUE, provider_readiness READY
                           (OWNED_EODHD_LIVE live-probed through 2026-09-02),
                           valuation 26/26 and decision 199/199 complete
    api.workflow_state     overall_state WAITING_FOR_OWNED_DATA,
                           blocker OWNED_DATA_NOT_CONFIRMED
                           ("Owned data confirms only 2026-09-01")
    …the same payload      daily_close_gate.execution_allowed = true,
                           operator_command.portfolio_cycle_actionable = true

Today therefore showed BLOCKED + "CATCH UP WAITING FOR OWNED DATA" + an "OWNED
DATA READY" badge + a green Run-the-portfolio-cycle CTA, simultaneously.

THE CAUSE. Two different business concepts were read through one date:

  * the PERSISTED owned-data confirmation (desk-mark date) answers "which
    session has already been PROCESSED?" — it advances only when a close runs,
    so for an owed session it is ALWAYS one session behind, by construction;
  * PROVIDER COVERAGE answers "does the owned provider currently hold the EOD
    data the owed close needs?" — the close owner's live probe already answers
    it, and the probe-free workflow owner never consumed that answer.

Requiring the persisted date to reach S before allowing the close that persists
S is circular. The reconciliation: api.daily_close owns the ONE coverage
calculation (``provider_covers_session`` over its probed ``provider_readiness``);
the COMPOSITION (decision snapshot for every GET, the portfolio-cycle
orchestrator at POST time) supplies that answer to ``load_workflow_state``,
which consumes it verbatim — and fails closed, in BOTH directions, when the
answer is negative or absent.

Every test is deterministic and offline: no clock, store, provider, prediction
or network read; every seam injected.
"""
from __future__ import annotations

import copy
import inspect
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
DS_SRC = (REPO / "api" / "decision_snapshot.py").read_text(encoding="utf-8",
                                                           errors="replace")
OP_SRC = (REPO / "api" / "operator_presentation.py").read_text(encoding="utf-8",
                                                               errors="replace")

# The live 2026-09-02 17:36 ET world: Sep-1 closed and persisted, Sep-2 owed.
NOW_SEP2_EVENING = datetime(2026, 9, 2, 17, 36, tzinfo=ET)
SEP1, SEP2 = "2026-09-01", "2026-09-02"

_OP = {"operational_book": {
    "book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1",
    "current_status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
    "nav_as_of_date": SEP1, "desk_mark_date": SEP1,
    "latest_desk_mark_date": SEP1, "nav": 97906.63, "cash": 4482.71,
    "holdings_count": 25, "pending_order_count": 0,
    "current_target": {"alpha_market_date": SEP1,
                       "latest_completed_market_date": SEP1}}}
_INPUTS = {"market_as_of_date": SEP1, "momentum_month": "2026-09",
           "fundamental_as_of_date": "2026-05-22"}
_DAILY = {"status": "DAILY_STATUS_READY", "latest_valid_mark_date": SEP1}
_DESK = {"series": {"SPY": [["2026-08-31", 760.0], [SEP1, 771.33]]},
         "latest_completed_date": SEP1}
_CLOSE = {"market_date": SEP1, "done": True,
          "final_close_status": "DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT",
          "status": "CLOSE_FINISHED"}
_FWD = {"latest_snapshot_date": SEP1, "snapshot_count": 7,
        "evidence_state": "FORWARD_EVIDENCE_COMPLETE", "interpretation": "complete",
        "active_book": {"model_id": "fundamental_momentum_50_50_v1", "book_id": "x"},
        "shadow_books": []}
_GATE = {"latest_completed_market_date": SEP1, "outcome": "NO_ACTION_TODAY",
         "headline": "No portfolio change required.", "target_state": "CURRENT_ALIGNED",
         "action_required": False, "next_scheduled_full_review": "2026-10-01",
         "scheduled_review_due": False, "opportunity_cost_available": True,
         "opportunity_cost_state": "AVAILABLE"}
_TR = {"dates": {"alpha_market_date": SEP1}}
_DRC = {"state": "COMPLETE", "governed_research_evidence_current": True,
        "eligible_market_date": SEP1, "blockers": []}
_REAS = {"reassessment_available": True, "reassessment_state": "HOLD_CURRENT_BOOK",
         "proposal_required": False, "blockers": []}

READY_SEP2 = {"provider_name": "OWNED_EODHD_LIVE", "provider_latest_date": SEP2,
              "expected_market_date": SEP2, "ready": True, "status": "READY",
              "queried_provider": True, "blocker_code": None,
              "blocker_message": None}
BEHIND_SEP1 = {"provider_name": "OWNED_EODHD_LIVE", "provider_latest_date": SEP1,
               "expected_market_date": SEP2, "ready": False, "status": "BEHIND",
               "queried_provider": True,
               "blocker_code": "PROVIDER_BEHIND_EXPECTED",
               "blocker_message": "not published"}
UNAVAILABLE = {"provider_name": None, "provider_latest_date": None,
               "expected_market_date": SEP2, "ready": False,
               "status": "PROVIDER_UNAVAILABLE", "queried_provider": False,
               "blocker_code": "PROVIDER_UNAVAILABLE",
               "blocker_message": "no completed date"}
SCOPE_OK = {"complete_for_valuation": True, "complete_for_decision": True}


def _load(**kw) -> dict:
    args = dict(now=NOW_SEP2_EVENING, operational=copy.deepcopy(_OP),
                inputs=dict(_INPUTS), daily_status=dict(_DAILY),
                desk_marks=copy.deepcopy(_DESK), close_progress=dict(_CLOSE),
                forward_status=copy.deepcopy(_FWD), gate=dict(_GATE),
                target_readiness=copy.deepcopy(_TR), research_cycle=dict(_DRC),
                reassessment_summary=dict(_REAS), decision_record={})
    args.update(kw)
    return ws.load_workflow_state(**args)


def st_provider_ready(**kw) -> dict:
    return _load(provider_readiness=dict(READY_SEP2),
                 market_data_scope=dict(SCOPE_OK), **kw)


def st_provider_behind(**kw) -> dict:
    return _load(provider_readiness=dict(BEHIND_SEP1), **kw)


def st_provider_unavailable(**kw) -> dict:
    return _load(provider_readiness=dict(UNAVAILABLE), **kw)


def st_unprobed(**kw) -> dict:
    return _load(**kw)


def _pres(wf: dict) -> dict:
    return op.build_operator_presentation(
        workflow=wf,
        daily_close={"provider_readiness":
                     (wf["session_recovery"].get("owned_provider_coverage")
                      or None)})


# =========================================================================== #
# 1-3  THE POSITIVE DIRECTION: owed Sep-2 + persisted Sep-1 + provider READY.
# =========================================================================== #
class TestProviderReadyMakesTheCycleExecutable:
    def test_01_owed_session_with_provider_data_is_executable(self):
        r = st_provider_ready()
        assert r["overall_state"] == ws.READY_FOR_DAILY_CLOSE
        assert r["primary_action"]["execution_available"] is True
        assert r["primary_action"]["execution_kind"] == ws.EXEC_DAILY_CLOSE
        cmd = r["operator_command"]
        assert cmd["portfolio_cycle_actionable"] is True
        assert cmd["portfolio_cycle_action_code"] == "RUN_PORTFOLIO_CYCLE"
        assert cmd["portfolio_cycle_blocking_reason"] is None
        assert not [b for b in r["blockers"]
                    if b["code"] == "OWNED_DATA_NOT_CONFIRMED"]
        plan = pc.plan_next_step(r)
        assert plan["step"] == pc.STEP_DAILY_CLOSE
        assert plan["stop_reason"] is None

    def test_02_persisted_mark_legitimately_remains_on_the_prior_session(self):
        """The desk mark on Sep-1 before the Sep-2 close is the EXPECTED state,
        stated as such — never as provider unavailability."""
        r = st_provider_ready()
        sr = r["session_recovery"]
        assert sr["owned_data_confirmation_date"] == SEP1
        assert sr["owned_data_confirmation_is_persisted_state"] is True
        assert sr["recovery_state"] == ws.CATCH_UP_REQUIRED
        assert sr["recovery_data_state"] == ws.RECOVERY_DATA_PROVIDER_READY
        assert sr["recovery_data_ready"] is True
        assert r["consistency_status"] == "CONSISTENT"

    def test_03_persisted_date_never_overrides_a_positive_provider_answer(self):
        """The two concepts carry different names and the LIVE answer decides."""
        r = st_provider_ready()
        sr = r["session_recovery"]
        # persisted says Sep-1; provider coverage says Sep-2 — coverage wins.
        assert sr["owned_data_confirmation_date"] == SEP1
        assert sr["provider_covers_recovery_session"] is True
        assert sr["provider_coverage_session"] == SEP2
        assert sr["provider_coverage_owner"] == "api.daily_close"
        assert sr["owned_provider_coverage"]["provider_latest_date"] == SEP2
        assert r["overall_state"] == ws.READY_FOR_DAILY_CLOSE
        # …and the recovery session is bound for the orchestrator, no date typed.
        binding = pc.recovery_binding(r)
        assert binding["bound_market_date"] == SEP2
        assert binding["operator_supplies_no_date"] is True


# =========================================================================== #
# 4-7  THE NEGATIVE DIRECTIONS: every genuine unavailability fails closed.
# =========================================================================== #
class TestGenuineUnavailabilityFailsClosed:
    @pytest.mark.parametrize("build,why", [
        (st_provider_behind, "provider published only Sep-1"),
        (st_provider_unavailable, "probe unavailable / failing"),
        (st_unprobed, "no provider answer observed at all"),
        (lambda: _load(provider_readiness=dict(READY_SEP2),
                       market_data_scope={"complete_for_valuation": False,
                                          "complete_for_decision": True}),
         "incomplete valuation coverage"),
        (lambda: _load(provider_readiness=dict(READY_SEP2),
                       market_data_scope={"complete_for_valuation": True,
                                          "complete_for_decision": False}),
         "incomplete decision-universe coverage"),
    ])
    def test_04_07_waiting_and_not_executable(self, build, why):
        r = build()
        assert r["overall_state"] == ws.WAITING_FOR_OWNED_DATA, why
        assert r["primary_action"]["execution_available"] is False, why
        assert r["primary_action"]["action_code"] == ws.ACTION_WAIT_FOR_OWNED_DATA
        cmd = r["operator_command"]
        assert cmd["portfolio_cycle_actionable"] is False, why
        assert cmd["portfolio_cycle_safe_to_execute"] is False, why
        assert cmd["portfolio_cycle_blocking_reason"], why
        assert r["daily_close_gate"]["execution_allowed"] is False, why
        assert [b["code"] for b in r["blockers"]] == ["OWNED_DATA_NOT_CONFIRMED"]
        assert r["session_recovery"]["recovery_state"] == \
            ws.CATCH_UP_WAITING_FOR_OWNED_DATA, why
        plan = pc.plan_next_step(r)
        assert plan["step"] is None, why
        assert plan["stop_reason"] == pc.STOP_WAITING_FOR_OWNED_DATA, why
        assert r["consistency_status"] == "CONSISTENT", why

    def test_04b_the_behind_blocker_names_the_provider_answer(self):
        b = st_provider_behind()["blockers"][0]
        assert b["provider_covers_owed_session"] is False
        assert b["provider_latest_date"] == SEP1
        assert b["provider_readiness_status"] == "BEHIND"
        assert "already been processed" in b["detail"]

    def test_05b_a_raising_probe_degrades_to_an_affirmative_negative(self, tmp_path):
        def _boom(**_kw):
            raise RuntimeError("transport down")
        readiness = dc.assess_owned_provider_readiness(
            today="2026-09-03", desk_dir=str(tmp_path), provider_probe=_boom)
        assert readiness["status"] == "PROVIDER_UNAVAILABLE"
        assert readiness["provider_latest_date"] is None
        assert readiness["owner"] == "api.daily_close"
        # …which the workflow then fails closed on.
        r = _load(provider_readiness=readiness)
        assert r["overall_state"] == ws.WAITING_FOR_OWNED_DATA
        assert r["operator_command"]["portfolio_cycle_actionable"] is False


# =========================================================================== #
# 8-10  CROSS-SURFACE CONSISTENCY: no payload may contradict itself again.
# =========================================================================== #
class TestNoContradictions:
    @pytest.mark.parametrize("build", [st_provider_ready, st_provider_behind,
                                       st_provider_unavailable, st_unprobed])
    def test_08_overall_state_and_close_gate_agree(self, build):
        r = build()
        gate = r["daily_close_gate"]
        cmd = r["operator_command"]
        if r["overall_state"] == ws.READY_FOR_DAILY_CLOSE:
            assert gate["execution_allowed"] is True
            assert cmd["portfolio_cycle_actionable"] is True
        else:
            assert r["overall_state"] == ws.WAITING_FOR_OWNED_DATA
            assert gate["execution_allowed"] is False
            assert cmd["portfolio_cycle_actionable"] is False
        # The R54.2.3 aliases stay a projection of the same verdict.
        assert cmd["portfolio_cycle_actionable"] == cmd["primary_action_available"]
        assert (cmd["portfolio_cycle_blocking_reason"] is None) == \
            cmd["portfolio_cycle_actionable"]

    @pytest.mark.parametrize("build", [st_provider_ready, st_provider_behind,
                                       st_provider_unavailable, st_unprobed])
    def test_09_owned_data_badge_and_blocker_agree(self, build):
        r = build()
        pres = _pres(r)
        panel = pres["session_recovery"]
        blocked = any(b["code"] == "OWNED_DATA_NOT_CONFIRMED"
                      for b in r["blockers"])
        if blocked:
            assert panel["owned_data_line"] != "READY"
            assert panel["next_action_label"] != "Run the Portfolio Cycle"
            assert panel["headline"] == "CATCH UP WAITING FOR OWNED DATA"
        else:
            assert panel["owned_data_line"] == "READY"
            assert panel["next_action_label"] == "Run the Portfolio Cycle"
            assert panel["headline"] == "CATCH UP REQUIRED"

    def test_09b_active_manager_delegates_the_same_recovery_state(self):
        for build in (st_provider_ready, st_provider_behind):
            r = build()
            block = ams._session_recovery_block(r)  # noqa: SLF001 — delegation seam
            assert block["delegated"] is True and block["computed_here"] is False
            assert block["recovery_state"] == \
                r["session_recovery"]["recovery_state"]

    def test_10_the_cta_reads_the_backend_verdict_verbatim(self):
        # The UI gates every workflow CTA on the backend's own flag…
        assert "primary_action_available === true" in UI_SRC or \
            "primary_action_available ? " in UI_SRC or \
            "c.primary_action_available" in UI_SRC
        # …and never re-derives actionability from state names or dates.
        assert "data-op-action-available" in UI_SRC

    def test_11_provider_readiness_is_never_recomputed_in_javascript(self):
        # Rendering the readiness fields is fine; COMPARING provider dates in the
        # client would be a second readiness calculation, and none may exist.
        assert not re.search(r"provider_latest_date\s*[<>]=?", UI_SRC)
        assert not re.search(r"provider_latest_date\s*!==?\s*", UI_SRC)
        assert "provider_covers_recovery_session" not in UI_SRC or \
            not re.search(r"provider_covers_recovery_session\s*=\s*[^=]", UI_SRC)


# =========================================================================== #
# 12-18  SAFETY: one path, no new authority, nothing automated.
# =========================================================================== #
class TestSafetyUnchanged:
    def test_12_no_manual_recovery_date(self):
        r = st_provider_ready()
        assert r["session_recovery"]["operator_supplies_no_date"] is True
        assert r["session_recovery"]["recovery_specific_route"] is None
        # The run route accepts confirmation + requested_by, nothing else.
        m = re.search(r"class PortfolioCycleRunRequest\(BaseModel\):"
                      r"(.*?)(?:^@app\.|^class )",
                      APP_SRC, re.S | re.M)
        assert m, "PortfolioCycleRunRequest not found"
        fields = re.findall(r"^\s{4}(\w+)\s*:", m.group(1), re.M)
        assert set(fields) <= {"confirmation", "requested_by"}

    def test_13_no_second_recovery_route(self):
        assert "portfolio-cycle/run" in APP_SRC
        for banned in ("force-close", "recover-session", "close-backfill",
                       "session-backfill"):
            assert banned not in APP_SRC

    def test_14_one_coverage_calculation_owned_by_the_close_owner(self):
        # ONE implementation of the comparison…
        impls = [fp for fp in (REPO / "api").glob("*.py")
                 if "def provider_covers_session(" in
                 fp.read_text(encoding="utf-8", errors="replace")]
        assert [fp.name for fp in impls] == ["daily_close.py"]
        # …the workflow consumes it (never re-derives)…
        assert "provider_covers_session(" in WS_SRC
        assert "_import_daily_close().provider_covers_session" in WS_SRC
        # …the snapshot supplies the answer to the workflow…
        assert "provider_readiness=(daily_close or {}).get(\"provider_readiness\")" \
            in DS_SRC
        # …and the presentation prefers the workflow's published verdict.
        assert "provider_covers_recovery_session" in OP_SRC

    def test_15_the_workflow_owner_remains_probe_free(self):
        assert "_PROVIDER_PROBE" not in WS_SRC
        assert "_default_provider_probe" not in WS_SRC
        assert "load_daily_close(" not in WS_SRC.replace(
            "load_daily_close would run", "")
        assert "load_close_progress()" in WS_SRC

    def test_16_the_bounded_assessment_writes_nothing(self, tmp_path):
        before = sorted(p.name for p in tmp_path.iterdir())
        dc.assess_owned_provider_readiness(
            today="2026-09-03", desk_dir=str(tmp_path),
            provider_probe=lambda **_kw: {"provider_latest_date": SEP2,
                                          "priced": ["SPY"], "source": "fake",
                                          "queried": True})
        assert sorted(p.name for p in tmp_path.iterdir()) == before

    def test_17_no_order_fill_or_broker_reach(self):
        src = inspect.getsource(dc.assess_owned_provider_readiness) + \
            inspect.getsource(dc.provider_covers_session)
        for banned in ("create_order", "fill", "broker", "submit_order",
                       "rebalance_execution"):
            assert banned not in src
        # No duplicate Daily Close either: one write path, one execute route.
        assert len(re.findall(r'"/v1/operations/daily-close/execute"',
                              APP_SRC)) == 1

    def test_18_automation_remains_off(self):
        # The cycle still requires the explicit operator token in every direction,
        # and the WAITING stop offers nothing to auto-run.
        assert pc.EXECUTE_CONFIRMATION == "RUN_PORTFOLIO_CYCLE"
        r = st_provider_behind()
        assert r["operator_command"]["confirmation_required"] is None
        ok = st_provider_ready()
        assert ok["operator_command"]["confirmation_required"] == \
            ws.PORTFOLIO_CYCLE_CONFIRMATION
        assert ok["primary_action"]["manual_confirmation_required"] is True


# =========================================================================== #
# CONTRACT: vocabulary + composition wiring.
# =========================================================================== #
class TestContract:
    def test_20_the_two_concepts_have_distinct_names_everywhere(self):
        r = st_provider_ready()
        cs = r["current_session"]
        assert cs["owned_data_confirmation_is_persisted_state"] is True
        assert cs["provider_covers_owed_session"] is True
        assert cs["provider_coverage_owner"] == "api.daily_close"
        sr = r["session_recovery"]
        assert sr["owned_data_confirmation_date"] != \
            sr["owned_provider_coverage"]["provider_latest_date"]

    def test_21_the_recovery_data_vocabulary_is_extended_not_rewritten(self):
        assert ws.RECOVERY_DATA_STATES == (
            "CONFIRMED", "UNVERIFIED_UNTIL_CLOSE_REVALIDATES",
            "OWNED_DATA_LAGGING", "PROVIDER_CONFIRMED_AWAITING_CLOSE")

    def test_22_coverage_comparison_semantics(self):
        assert dc.provider_covers_session(READY_SEP2, SEP2) is True
        assert dc.provider_covers_session(READY_SEP2, SEP1) is True
        assert dc.provider_covers_session(BEHIND_SEP1, SEP2) is False
        assert dc.provider_covers_session(UNAVAILABLE, SEP2) is False
        assert dc.provider_covers_session(None, SEP2) is None
        assert dc.provider_covers_session({}, SEP2) is None
        assert dc.provider_covers_session(READY_SEP2, None) is None
        assert dc.provider_covers_session(
            READY_SEP2, SEP2,
            market_data_scope={"complete_for_valuation": False,
                               "complete_for_decision": True}) is False

    def test_23_the_morning_unprobed_catch_up_behaviour_is_unchanged(self):
        """R54.2.1's shipped morning semantics survive: before the cutoff, with no
        provider answer composed, an owed prior session is CATCH_UP_REQUIRED with
        UNVERIFIED data (the close revalidates server-side)."""
        r = _load(now=datetime(2026, 9, 3, 9, 1, tzinfo=ET))
        sr = r["session_recovery"]
        assert sr["recovery_state"] == ws.CATCH_UP_REQUIRED
        assert sr["recovery_data_state"] == ws.RECOVERY_DATA_UNVERIFIED
        assert sr["recovery_session"] == SEP2
        assert r["overall_state"] == ws.READY_FOR_DAILY_CLOSE

    def test_24_a_morning_affirmative_negative_still_waits(self):
        """Direction symmetry: even before the cutoff, an AFFIRMATIVE provider
        answer that cannot cover the owed session fails the workflow closed."""
        r = _load(now=datetime(2026, 9, 3, 9, 1, tzinfo=ET),
                  provider_readiness=dict(BEHIND_SEP1))
        assert r["session_recovery"]["recovery_state"] == \
            ws.CATCH_UP_WAITING_FOR_OWNED_DATA
        assert r["overall_state"] == ws.WAITING_FOR_OWNED_DATA
        assert r["operator_command"]["portfolio_cycle_actionable"] is False

    def test_25_the_snapshot_composes_the_close_owner_before_the_workflow(self):
        dc_pos = DS_SRC.index('_timed("daily_close"')
        ws_pos = DS_SRC.index('_timed("workflow"')
        assert dc_pos < ws_pos

    def test_26_the_orchestrator_supplies_the_readiness_at_post_time(self):
        pc_src = (REPO / "api" / "portfolio_cycle.py").read_text(
            encoding="utf-8", errors="replace")
        assert "assess_owned_provider_readiness()" in pc_src
        assert "load_workflow_state(provider_readiness=readiness)" in pc_src

    def test_27_the_close_gate_echoes_the_verdict_it_obeyed(self):
        g = st_provider_behind()["daily_close_gate"]
        assert g["provider_confirms_owed_session"] is False
        assert g["provider_coverage_owner"] == "api.daily_close"
        g2 = st_provider_ready()["daily_close_gate"]
        assert g2["provider_confirms_owed_session"] is True
