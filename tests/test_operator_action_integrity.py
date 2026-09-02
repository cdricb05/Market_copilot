"""tests/test_operator_action_integrity.py — MAJOR RELIABILITY SLICE:
canonical operator action integrity.

Deterministic contract tests for the four live defects observed in the Aug-10
operating cycle:

  * DEFECT 1 — WAITING_FOR_OWNED_DATA dead CTA: the canonical primary action now
    EXECUTES the existing authoritative owned-data refresh owner (the manual
    Paper Desk refresh, POST /v1/paper-desk/refresh, token
    CONFIRM_PAPER_DESK_REFRESH) behind the same confirmation contract — never a
    dead button, never a second refresh implementation.
  * DEFECT 2 — Daily Research Cycle route-vs-execute confusion: exactly one
    executable primary DRC action (POST /v1/operations/daily-research-cycle/run
    through the existing handler, double-submit protected); a navigation link is
    labelled distinctly ("View Daily Workflow"), never like execution.
  * DEFECT 3 — invalid Daily Close affordance: only the canonical
    READY_FOR_DAILY_CLOSE state exposes an executable Daily Close control; every
    secondary close surface obeys the canonical daily_close_gate verbatim and
    shows a passive status otherwise. A stale control cannot execute a forbidden
    close (runDailyClose guards on the canonical gate).
  * DEFECT 4 — post-close stale "NEEDS ACTION": after DAILY_CYCLE_COMPLETE the
    canonical answer is "Monitor the portfolio" / "No action required right
    now"; the legacy stage surfaces re-frame as optional review context and the
    compatibility-only legacy membership comparison never claims NEEDS_ACTION.

Every clock/date is injected; no network, provider, prediction, DB or write.
The UI-behaviour tests execute the REAL index.html functions in Node through
scripts/operator_action_integrity_harness.js (no jsdom / npm install) and skip
cleanly when ``node`` is unavailable.
"""
from __future__ import annotations

import copy
import json
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from paper_trader.api import daily_close as dcm
from paper_trader.api import daily_research_cycle as drcm
from paper_trader.api import paper_trading_desk as ptd
from paper_trader.api import workflow_state as ws

ET = ZoneInfo("America/New_York")
REPO = Path(__file__).resolve().parent.parent
UI = REPO / "api" / "ui" / "index.html"
HARNESS = REPO / "scripts" / "operator_action_integrity_harness.js"
APP = REPO / "api" / "app.py"
NODE = shutil.which("node")


# --------------------------------------------------------------------------- #
# Injected canonical-state fixtures (no live dates in production code). The
# Wednesday 2026-08-05 evening (post 17:30 ET cutoff) is the deterministic
# reference session; every state is produced by the REAL priority policy in
# ws.load_workflow_state over injected owner payloads.
# --------------------------------------------------------------------------- #
def _op(desk_mark, nav_as_of):
    return {"operational_book": {
        "book_id": "alpha_paper_book_1", "current_status": "FORWARD_TRACKING_ACTIVE",
        "initialized": True, "nav_as_of_date": nav_as_of, "desk_mark_date": desk_mark,
        "latest_desk_mark_date": desk_mark, "nav": 102241.79, "cash": 1500.0,
        "holdings_count": 25, "pending_order_count": 0,
        "current_target": {"alpha_market_date": "2026-07-31",
                           "latest_completed_market_date": desk_mark}}}


# Owned-data session confirmation comes from the ACTIVE operational book's desk
# mark (api.data_freshness) — exactly the surface the live Aug-10 refresh
# advanced. The lagging book leaves the expected session unconfirmed.
_OP = _op("2026-08-04", "2026-08-04")
_OP_FRESH = _op("2026-08-05", "2026-08-04")
_OP_CLOSED = _op("2026-08-05", "2026-08-05")
_INPUTS_STALE = {"market_as_of_date": "2026-07-31", "momentum_month": "2026-07",
                 "fundamental_as_of_date": "2026-05-22"}
_INPUTS_FRESH = {"market_as_of_date": "2026-08-05", "momentum_month": "2026-08",
                 "fundamental_as_of_date": "2026-05-22"}
_DAILY = {"status": "NO_DAILY_REFRESH_YET", "latest_valid_mark_date": "2026-07-20"}
_DESK_LAG = {"series": {"SPY": [["2026-07-31", 747.03], ["2026-08-04", 771.33]]},
             "latest_completed_date": "2026-08-04"}
_DESK_FRESH = {"series": {"SPY": [["2026-07-31", 747.03], ["2026-08-04", 771.33],
                                  ["2026-08-05", 772.61]]},
               "latest_completed_date": "2026-08-05"}
_CLOSE_PRIOR = {"market_date": "2026-08-04", "done": True,
                "final_close_status": "DAILY_CLOSE_COMPLETE_HOLD",
                "status": "CLOSE_FINISHED"}
_CLOSE_TODAY = {"market_date": "2026-08-05", "done": True,
                "final_close_status": "DAILY_CLOSE_COMPLETE_HOLD",
                "status": "CLOSE_FINISHED"}
_FWD = {"latest_snapshot_date": "2026-08-04", "snapshot_count": 5,
        "evidence_state": "FORWARD_EVIDENCE_OK", "interpretation": "ok",
        "active_book": {"model_id": "m", "book_id": "x"}, "shadow_books": []}
_FWD_TODAY = dict(_FWD, latest_snapshot_date="2026-08-05")
_GATE = {"latest_completed_market_date": "2026-08-04", "outcome": "NO_ACTION_TODAY",
         "outcome_label": "NO ACTION TODAY",
         "headline": "NO PORTFOLIO CHANGE REQUIRED FROM THE LATEST COMPLETED CLOSE",
         "explanation": "The actual holdings match the model target.",
         "target_state": "CURRENT_ALIGNED", "target_state_label": "CURRENT ALIGNED",
         "action_required": False, "action_severity": "green",
         "next_scheduled_full_review": "2026-09-01", "scheduled_review_due": False,
         "review_cadence": "MONTHLY", "target_actual_match": True,
         "proposed_change_count": 0, "evaluation_date": "2026-08-05",
         "estimated_turnover": 0.0, "estimated_cost": 0.0,
         "primary_action_label": "Monitor Holdings and Performance"}
_TR = {"dates": {"alpha_market_date": "2026-07-31"}}
_EVENING = datetime(2026, 8, 5, 18, 30, tzinfo=ET)


def _load(**kw):
    args = dict(now=_EVENING, operational=copy.deepcopy(_OP),
                inputs=dict(_INPUTS_STALE), daily_status=dict(_DAILY),
                desk_marks=copy.deepcopy(_DESK_LAG),
                close_progress=dict(_CLOSE_PRIOR),
                forward_status=copy.deepcopy(_FWD), gate=copy.deepcopy(_GATE),
                target_readiness=copy.deepcopy(_TR),
                research_cycle={"state": "NOT_STARTED", "executable": False})
    args.update(kw)
    return ws.load_workflow_state(**args)


def st_waiting_owned():
    # Post-cutoff evening; owned desk data still ends at the prior session, and no
    # live provider answer was composed in — Release 54.2.3.1 fails this closed
    # (no mutation CTA until the provider answer affirms coverage).
    return _load()


def st_catch_up_provider_ready():
    # The SAME evening world, with api.daily_close's live provider answer composed
    # in and covering the owed session — the priority policy routes it to
    # READY_FOR_DAILY_CLOSE, where the canonical close is the promoted action.
    return _load(provider_readiness={
        "provider_name": "OWNED_EODHD_LIVE", "provider_latest_date": "2026-08-05",
        "expected_market_date": "2026-08-05", "ready": True, "status": "READY",
        "queried_provider": True, "blocker_code": None, "blocker_message": None})


def st_research_required():
    # STAGE 22 — the canonical POST-CLOSE research state: the eligible session (08-05)
    # is confirmed by owned data AND its Daily Close is complete, and the research
    # inputs are still stale. Before Stage 22 this fixture left the close undone, which
    # under the canonical normal cycle is the Daily Close's stage, not research's —
    # the close is what advances owned marks and settles NEXT_CLOSE paper orders, so
    # research run ahead of it describes a portfolio that is about to change.
    return _load(operational=copy.deepcopy(_OP_CLOSED),
                 desk_marks=copy.deepcopy(_DESK_FRESH),
                 close_progress=dict(_CLOSE_TODAY),
                 research_cycle={"state": "NOT_STARTED", "executable": True})


def st_ready_for_close():
    gate = copy.deepcopy(_GATE)
    gate["opportunity_cost_available"] = True
    gate["latest_completed_market_date"] = "2026-08-05"
    return _load(operational=copy.deepcopy(_OP_FRESH),
                 desk_marks=copy.deepcopy(_DESK_FRESH),
                 inputs=dict(_INPUTS_FRESH), gate=gate,
                 research_cycle={"state": "COMPLETE", "executable": False})


def st_cycle_complete():
    gate = copy.deepcopy(_GATE)
    gate["opportunity_cost_available"] = True
    gate["latest_completed_market_date"] = "2026-08-05"
    return _load(operational=copy.deepcopy(_OP_CLOSED),
                 desk_marks=copy.deepcopy(_DESK_FRESH),
                 inputs=dict(_INPUTS_FRESH), gate=gate,
                 close_progress=dict(_CLOSE_TODAY),
                 forward_status=copy.deepcopy(_FWD_TODAY),
                 research_cycle={"state": "COMPLETE", "executable": False})


def st_cycle_blocked():
    return _load(operational=copy.deepcopy(_OP_FRESH),
                 desk_marks=copy.deepcopy(_DESK_FRESH),
                 research_cycle={"state": "BLOCKED", "executable": False,
                                 "blockers": [{"source_id": "momentum_monthly",
                                               "code": "NO_SAFE_EMITTER"}]})


def st_inconsistent():
    return _load(operational=copy.deepcopy(_OP_FRESH),
                 desk_marks=copy.deepcopy(_DESK_FRESH),
                 research_cycle={"state": "INCONSISTENT", "executable": False})


def _ui():
    return UI.read_text(encoding="utf-8")


# =========================================================================== #
# BACKEND — canonical execution contract
# =========================================================================== #
class TestBackendExecutionContract:
    def test_fixture_states_are_the_canonical_states(self):
        assert st_waiting_owned()["overall_state"] == ws.WAITING_FOR_OWNED_DATA
        assert st_research_required()["overall_state"] == ws.RESEARCH_CYCLE_REQUIRED
        assert st_ready_for_close()["overall_state"] == ws.READY_FOR_DAILY_CLOSE
        assert st_cycle_complete()["overall_state"] == ws.DAILY_CYCLE_COMPLETE
        assert st_cycle_blocked()["overall_state"] == ws.RESEARCH_CYCLE_BLOCKED
        assert st_inconsistent()["overall_state"] == ws.INCONSISTENT_STATE

    def test_waiting_for_owned_data_primary_executes_the_canonical_daily_close(self):
        # STAGE 19.3 SUPERSEDED the original Defect-1 contract (the desk refresh is
        # never promoted; the Daily Close composes it). RELEASE 54.2.3.1 SUPERSEDES
        # the unconditional promotion: with the close owner's live provider answer
        # composed in and covering the owed session, the SAME world becomes
        # READY_FOR_DAILY_CLOSE and the canonical close is the promoted action —
        # while the unprobed world offers no mutation CTA at all.
        r = st_catch_up_provider_ready()
        assert r["overall_state"] == ws.READY_FOR_DAILY_CLOSE
        pa = r["primary_action"]
        assert pa["label"] == "Run the Daily Close"
        assert pa["execution_available"] is True
        assert pa["manual_confirmation_required"] is True
        assert pa["execution_kind"] == ws.EXEC_DAILY_CLOSE
        ec = pa["execution_contract"]
        assert ec["path"] == "/v1/operations/daily-close/execute"
        assert ec["confirmation_field"] == "confirmation"
        assert ec["confirmation_token"] == dcm.EXECUTE_CONFIRMATION
        assert pa["confirmation_required"] == dcm.EXECUTE_CONFIRMATION
        assert pa["destination"] == ws.DEST_DAILY_WORKFLOW
        # The maintenance executor must never reappear as the promoted action.
        assert pa["execution_kind"] not in ws.MAINTENANCE_EXECUTION_KINDS
        # The unprobed world stays honest: WAIT action, nothing executable.
        pw = st_waiting_owned()["primary_action"]
        assert pw["action_code"] == ws.ACTION_WAIT_FOR_OWNED_DATA
        assert pw["execution_available"] is False
        assert pw.get("execution_kind") is None

    def test_waiting_for_owned_data_allows_the_daily_close(self):
        # Release 54.2.3.1 — renamed in spirit: the close is allowed exactly when
        # the provider answer covers the owed session (the state is then
        # READY_FOR_DAILY_CLOSE); an unprobed/uncovered WAITING world exposes NO
        # executable close and no mutation CTA — never a BLOCKED banner beside a
        # green button again.
        r = st_catch_up_provider_ready()
        assert r["daily_close_gate"]["execution_allowed"] is True
        assert r["daily_close_gate"]["passive_status"] is None
        cmd = r["operator_command"]
        assert cmd["primary_action_available"] is True
        # Release 48: presented = the one portfolio cycle; underlying = the close.
        assert cmd["primary_action_kind"] == ws.EXEC_PORTFOLIO_CYCLE
        assert cmd["cycle_underlying_kind"] == ws.EXEC_DAILY_CLOSE
        assert "settle eligible NEXT_CLOSE paper orders" in cmd["supporting_text"]
        w = st_waiting_owned()
        assert w["daily_close_gate"]["execution_allowed"] is False
        assert w["daily_close_gate"]["passive_badge"] == "WAITING"
        assert w["operator_command"]["primary_action_available"] is False
        assert w["operator_command"]["portfolio_cycle_actionable"] is False
        assert w["operator_command"]["portfolio_cycle_blocking_reason"]

    def test_paper_desk_refresh_is_never_a_canonical_primary_action(self):
        # The endpoint and its execution contract survive as a MAINTENANCE capability…
        assert ws.EXEC_PAPER_DESK_REFRESH in ws.EXECUTION_KINDS
        assert ws.EXEC_PAPER_DESK_REFRESH in ws.MAINTENANCE_EXECUTION_KINDS
        assert ws.EXEC_PAPER_DESK_REFRESH not in ws.NORMAL_PATH_EXECUTION_KINDS
        # … but no canonical state may promote it, and the guard fails closed.
        for st in (st_waiting_owned(), st_research_required(), st_ready_for_close(),
                   st_cycle_complete(), st_cycle_blocked(), st_inconsistent()):
            assert st["primary_action"]["execution_kind"] != ws.EXEC_PAPER_DESK_REFRESH
        with pytest.raises(AssertionError):
            ws.assert_primary_action_contract(
                {"execution_kind": ws.EXEC_PAPER_DESK_REFRESH})

    def test_research_cycle_required_primary_executes_the_canonical_drc(self):
        pa = st_research_required()["primary_action"]
        assert pa["action_code"] == ws.ACTION_RUN_RESEARCH_CYCLE
        assert pa["execution_available"] is True
        assert pa["execution_kind"] == ws.EXEC_DAILY_RESEARCH_CYCLE
        ec = pa["execution_contract"]
        assert ec["path"] == "/v1/operations/daily-research-cycle/run"
        assert ec["confirmation_field"] == "confirmation"
        assert ec["confirmation_token"] == drcm.EXECUTE_CONFIRMATION
        assert pa["confirmation_required"] == drcm.EXECUTE_CONFIRMATION

    def test_research_cycle_required_forbids_daily_close(self):
        g = st_research_required()["daily_close_gate"]
        assert g["execution_allowed"] is False
        assert "Daily Research Cycle" in g["passive_status"]
        # STAGE 22: this state is only reachable once the eligible session's close is
        # COMPLETE (an unclosed session is READY_FOR_DAILY_CLOSE). The gate says so —
        # the previous "waiting for the Daily Research Cycle" inverted the dependency.
        assert g["passive_badge"] == "COMPLETE"
        assert "complete" in g["passive_status"].lower()

    def test_ready_for_daily_close_primary_executes_the_canonical_close(self):
        r = st_ready_for_close()
        pa = r["primary_action"]
        assert pa["action_code"] == ws.ACTION_RUN_DAILY_CLOSE
        assert pa["execution_available"] is True
        assert pa["execution_kind"] == ws.EXEC_DAILY_CLOSE
        ec = pa["execution_contract"]
        assert ec["path"] == "/v1/operations/daily-close/execute"
        assert ec["confirmation_token"] == dcm.EXECUTE_CONFIRMATION
        assert r["daily_close_gate"]["execution_allowed"] is True
        # DRC execution is no longer required/queued once research is current.
        assert all(q["action_code"] != ws.ACTION_RUN_RESEARCH_CYCLE
                   for q in r["queued_actions"])

    def test_daily_cycle_complete_requires_nothing(self):
        r = st_cycle_complete()
        pa = r["primary_action"]
        assert pa["action_code"] == ws.ACTION_MONITOR
        assert pa["execution_available"] is False
        assert pa["execution_kind"] is None
        assert pa["execution_contract"] is None
        assert r["current_task"] == "Monitor the portfolio."
        g = r["daily_close_gate"]
        assert g["execution_allowed"] is False
        assert g["passive_status"] == "Daily Close complete for 2026-08-05."
        assert g["passive_badge"] == "COMPLETE"

    def test_blocked_and_inconsistent_expose_no_executor(self):
        for r in (st_cycle_blocked(), st_inconsistent()):
            pa = r["primary_action"]
            assert pa["execution_kind"] is None
            assert pa["execution_contract"] is None
            g = r["daily_close_gate"]
            assert g["execution_allowed"] is False
            assert g["passive_badge"] == "BLOCKED"

    def test_execution_kind_present_iff_real_executor(self):
        for build in (st_waiting_owned, st_research_required, st_ready_for_close,
                      st_cycle_complete, st_cycle_blocked, st_inconsistent):
            pa = build()["primary_action"]
            if pa["execution_kind"]:
                assert pa["execution_available"] is True
                assert pa["execution_kind"] in ws.EXECUTION_KINDS
                assert pa["execution_contract"] is not None
            else:
                assert pa["execution_contract"] is None

    def test_execution_contracts_match_the_real_owners_and_routes(self):
        c = ws.EXECUTION_CONTRACTS
        assert c[ws.EXEC_PAPER_DESK_REFRESH]["confirmation_token"] == ptd.REFRESH_CONFIRM_TOKEN
        assert c[ws.EXEC_DAILY_RESEARCH_CYCLE]["confirmation_token"] == drcm.EXECUTE_CONFIRMATION
        assert c[ws.EXEC_DAILY_CLOSE]["confirmation_token"] == dcm.EXECUTE_CONFIRMATION
        app_src = APP.read_text(encoding="utf-8")
        assert '@app.post("/v1/paper-desk/refresh"' in app_src
        assert '"/v1/operations/daily-research-cycle/run"' in app_src
        assert '@app.post("/v1/operations/daily-close/execute"' in app_src

    def test_daily_close_gate_fails_closed_on_unknown_state(self):
        g = ws.build_daily_close_gate("SOME_FUTURE_STATE")
        assert g["execution_allowed"] is False
        assert g["passive_badge"] == "BLOCKED"

    def test_legacy_membership_comparison_never_claims_needs_action(self):
        stages = dcm._daily_cycle_stages(dcm.REBALANCE_PROPOSAL_READY)
        assert [s["status"] for s in stages] == [
            "COMPLETE", "COMPLETE", "COMPLETE", "PENDING", "ACTIVE"]
        # Genuinely pending paper orders remain a real ACTIVE stage (unchanged).
        submitted = dcm._daily_cycle_stages(dcm.PAPER_ORDERS_SUBMITTED)
        assert submitted[3]["status"] == "ACTIVE"


# =========================================================================== #
# UI SOURCE — one dispatcher, no navigation disguised as execution
# =========================================================================== #
class TestUiSourceContract:
    def test_every_canonical_cta_uses_the_one_dispatcher(self):
        ui = _ui()
        assert ui.count("function dispatchCanonicalPrimaryAction(") == 1
        # Today hero CTA, banner CTA and right-rail button all dispatch.
        hero = ui[ui.index("function _wsRenderTodayHero"):]
        hero = hero[:hero.index("window._wsRenderTodayHero")]
        assert "dispatchCanonicalPrimaryAction(this)" in hero
        banner = ui[ui.index("function _wsBannerHtml"):]
        banner = banner[:banner.index("function renderWorkflowState")]
        assert "dispatchCanonicalPrimaryAction(this)" in banner
        rail = ui[ui.index("function _wsApplyRightPanel"):]
        rail = rail[:rail.index("function _wsApplyAssessmentFraming")]
        assert "dispatchCanonicalPrimaryAction(btn)" in rail

    def test_primary_cta_never_navigates_directly(self):
        ui = _ui()
        hero = ui[ui.index("function _wsRenderTodayHero"):]
        hero = hero[:hero.index("window._wsRenderTodayHero")]
        # The ONLY navigateToRoute usages in the hero are the optional review
        # links and the distinctly-labelled view link — never the primary CTA.
        cta_start = hero.index("var ctaHtml")
        cta_block = hero[cta_start:hero.index("var secHtml")]
        assert 'class="th-cta ' in cta_block
        assert "dispatchCanonicalPrimaryAction(this)" in cta_block
        assert "Navigation only" in cta_block   # the view link is labelled as such

    def test_view_link_wording_is_distinct(self):
        ui = _ui()
        assert "'daily-workflow': 'View Daily Workflow'" in ui

    def test_owned_data_refresh_reuses_the_desk_spec(self):
        ui = _ui()
        ex = ui[ui.index("function _wsExecuteOwnedDataRefresh"):]
        ex = ex[:ex.index("function wsExecConfirmNo")]
        assert "_PD_ACTIONS.refresh" in ex
        # Exactly one refresh POST path in the whole UI — no duplicate writer.
        assert ui.count("'/v1/paper-desk/refresh'") == 1

    def test_daily_close_surfaces_obey_the_canonical_gate(self):
        ui = _ui()
        assert ui.count("function _wsDailyCloseGate(") == 1
        run = ui[ui.index("async function runDailyClose"):]
        run = run[:run.index("window.runDailyClose")]
        assert "_wsDailyCloseGate()" in run and "execution_allowed" in run
        for fn in ("function renderDailyClose(", "function renderDailyClosePm(",
                   "function renderDailyClosePerf("):
            seg = ui[ui.index(fn):]
            seg = seg[:seg.index("\nfunction ", 10)]
            assert "_wsDailyCloseGate" in seg

    def test_pd_band_focus_target_exists(self):
        ui = _ui()
        assert "'pd-band': 'pd-band'" in ui
        assert 'id="pd-band"' in ui

    def test_no_native_dialogs_in_the_new_paths(self):
        ui = _ui()
        for fn in ("function dispatchCanonicalPrimaryAction(",
                   "function _wsExecuteOwnedDataRefresh(",
                   "function wsExecConfirmYes("):
            seg = ui[ui.index(fn):]
            seg = seg[:seg.index("\nfunction ", 10)]
            assert "alert(" not in seg and "confirm(" not in seg


# =========================================================================== #
# UI BEHAVIOUR — the REAL code, executed in Node per canonical state
# =========================================================================== #
def _dc_close_due_payload():
    """A stale/eager secondary daily-close payload that CLAIMS the close is due
    (the exact Defect-3 shape observed live)."""
    return {"close_status": "CLOSE_DUE", "close_status_label": "READY FOR DAILY CLOSE",
            "severity": "amber",
            "headline": "AUG 05 EOD DATA READY — RUN DAILY CLOSE",
            "explanation": "EOD data is ready.",
            "latest_eligible_market_date": "2026-08-05",
            "last_processed_market_date": "2026-08-04",
            "primary_action": {"label": "Run Daily Close", "kind": "RUN_DAILY_CLOSE",
                               "enabled": True, "runs_daily_close": True,
                               "refreshes_status": False, "route": "#daily-workflow"},
            "daily_cycle_stages": [
                {"stage": 4, "code": "MANUAL_REVIEW_ORDERS",
                 "label": "Manual Review & Paper Orders",
                 "status": "NEEDS_ACTION", "detail": ""}],
            "pnl": {"nav": None}}


_RESPONSES = {
    "/v1/paper-desk/refresh": {
        "status": "PAPER_DESK_OK", "message": "refresh ok",
        "resulting_desk_mark_date": "2026-08-05",
        "settlement": {"n_filled": 0}, "performance": {"n_appended": 1}},
    "/v1/operations/daily-research-cycle/run": {"state": "COMPLETE"},
    "/v1/operations/daily-close/execute": {
        "close_status": "DAILY_CLOSE_COMPLETE_HOLD",
        "forward_evidence": {"severity": "green"}},
    "/v1/operations/daily-close/progress": {},
    # Release 48 — the ONE portfolio-cycle orchestration entrypoint.
    "/v1/operations/portfolio-cycle/run": {
        "status": "PORTFOLIO_CYCLE_COMPLETE",
        "steps_taken": ["DAILY_CLOSE"], "stop_reason": "DECISION_PRESENTED"},
}

_CC_DATA = {"alpha": {"available": True, "mark_stale": False,
                      "latest_mark_date": "2026-08-05"},
            "workflow": {"review_queue_count": 3, "approved_count": 2,
                         "order_eligible_count": 0, "blocked_count": 0},
            "portfolio": {"open_positions": 25, "review_for_exit_count": 0}}


@pytest.fixture(scope="module")
def harness_report():
    if not NODE:
        pytest.skip("node is unavailable")
    states = {
        # Release 54.2.3.1 — the executable evening world is the provider-covered
        # one (READY_FOR_DAILY_CLOSE via catch-up); the unprobed WAITING world is
        # exercised separately below and must render NO mutation CTA.
        "waiting_owned": {"ws": st_catch_up_provider_ready(),
                          "dc": _dc_close_due_payload(),
                          "cc": None, "responses": _RESPONSES},
        "waiting_owned_unprobed": {"ws": st_waiting_owned(),
                                   "dc": _dc_close_due_payload(),
                                   "cc": None, "responses": _RESPONSES},
        "research_required": {"ws": st_research_required(),
                              "dc": _dc_close_due_payload(),
                              "cc": None, "responses": _RESPONSES},
        "ready_for_close": {"ws": st_ready_for_close(),
                            "dc": _dc_close_due_payload(),
                            "cc": None, "responses": _RESPONSES},
        "cycle_complete": {"ws": st_cycle_complete(), "dc": _dc_close_due_payload(),
                           "cc": _CC_DATA, "responses": _RESPONSES},
        "cycle_blocked": {"ws": st_cycle_blocked(), "dc": _dc_close_due_payload(),
                          "cc": None, "responses": _RESPONSES},
        "inconsistent": {"ws": st_inconsistent(), "dc": None,
                         "cc": None, "responses": _RESPONSES},
    }
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "payload.json"
        p.write_text(json.dumps({"states": states}, default=str), encoding="utf-8")
        out = subprocess.run([NODE, str(HARNESS), str(UI), str(p)],
                             capture_output=True, text=True, encoding="utf-8",
                             errors="replace", timeout=180)
    assert out.returncode == 0, out.stderr[-3000:]
    return json.loads(out.stdout)["states"]


def _paths(posts):
    return [x["path"] for x in posts]


class TestUiBehaviourWaitingOwned:
    def test_one_primary_cta_executes_the_canonical_daily_close(self, harness_report):
        # STAGE 19.3: the promoted post-close action composes the Paper Desk owner
        # through the canonical Daily Close; the standalone desk refresh is
        # maintenance only. RELEASE 48: the operator sees the ONE portfolio-cycle
        # action, and the click POSTs the ONE orchestration entrypoint (which
        # sequences the same close owner server-side, with its own token).
        r = harness_report["waiting_owned"]
        assert r["opc"]["cta_count"] == 1
        assert "Run the portfolio cycle" in r["opc"]["html"]
        assert "Refresh owned market data" not in r["opc"]["html"]
        assert "dispatchCanonicalPrimaryAction(this)" in r["opc"]["html"]
        assert _paths(r["posts_after_click"]) == ["/v1/operations/portfolio-cycle/run"]
        assert r["posts_after_click"][0]["body"] == {
            "confirmation": ws.PORTFOLIO_CYCLE_CONFIRMATION}
        assert "/v1/paper-desk/refresh" not in _paths(r["posts_after_click"])
        assert "/v1/operations/daily-close/execute" not in _paths(r["posts_after_click"])
        assert r["navs_after_click"] == []          # execution, not navigation

    def test_not_a_dead_button_and_no_double_submit(self, harness_report):
        r = harness_report["waiting_owned"]
        assert len(r["posts_after_double"]) == 1

    def test_daily_close_is_the_available_action(self, harness_report):
        # The close is legitimately available here (it is the owned-mark owner), so a
        # close attempt genuinely posts the close — but only ONE surface offers it.
        r = harness_report["waiting_owned"]
        assert r["operator_command"]["attrs"]["data-op-action-available"] == "1"
        assert r["dc"]["cc_btn"]["display"] == "none"    # no duplicate panel control
        assert _paths(r["posts_after_close_attempt"]) == [
            "/v1/operations/daily-close/execute"]

    def test_unprobed_waiting_world_renders_no_mutation_cta(self, harness_report):
        # Release 54.2.3.1 — without a provider answer the workflow fails closed:
        # no green portfolio-cycle button may render beside the OWNED_DATA blocker.
        r = harness_report["waiting_owned_unprobed"]
        assert r["operator_command"]["attrs"]["data-op-action-available"] == "0"
        assert r["opc"]["cta_count"] == 0
        assert r["posts_after_click"] == []


class TestUiBehaviourResearchRequired:
    def test_exactly_one_executable_drc_action(self, harness_report):
        # STAGE 19.3: the ONE execution surface is the canonical Operator Command bar;
        # the hero no longer repeats the execute button (it keeps a navigation link).
        # The dispatcher still routes to exactly ONE owner.
        r = harness_report["research_required"]
        assert r["opc"]["cta_count"] == 1
        # Release 48: the presented action is the one portfolio cycle; the DRC is
        # the decided underlying step, sequenced server-side by the orchestrator.
        assert "Run the portfolio cycle" in r["opc"]["html"]
        assert "Run the portfolio cycle" not in r["hero"]["html"]
        assert _paths(r["posts_after_click"]) == ["/v1/operations/portfolio-cycle/run"]
        assert r["posts_after_click"][0]["body"] == {
            "confirmation": ws.PORTFOLIO_CYCLE_CONFIRMATION}
        assert "workflow-state" in r["loads_after_click"]

    def test_double_submit_protected(self, harness_report):
        r = harness_report["research_required"]
        assert len(r["posts_after_double"]) == 1

    def test_navigation_link_is_distinct_not_identical(self, harness_report):
        # Navigation is NEVER suppressed (routing is not a write): the hero and the
        # banner both keep the distinctly-worded view link beside the canonical action.
        r = harness_report["research_required"]
        assert "View Daily Workflow" in r["hero"]["html"]
        assert "View Daily Workflow" in r["banner_dw"]["html"]
        # …and neither disguises navigation as execution.
        assert "Run the Daily Research Cycle" not in r["banner_dw"]["html"]

    def test_no_daily_close_execution_available(self, harness_report):
        r = harness_report["research_required"]
        assert r["dc"]["cc_btn"]["display"] == "none"
        assert r["dc"]["dw_btn"]["display"] == "none"
        assert r["dc"]["pm_btn"]["display"] == "none"
        assert r["dc"]["perf_btn"]["display"] == "none"
        assert "Daily Research Cycle" in r["dc"]["dw_headline"]["text"]
        assert r["posts_after_close_attempt"] == []


class TestUiBehaviourReadyForClose:
    def test_exactly_one_primary_daily_close_action(self, harness_report):
        r = harness_report["ready_for_close"]
        assert r["opc"]["cta_count"] == 1
        # Release 48: one presented action (the portfolio cycle), one POST, one
        # orchestration entrypoint; the close is the decided underlying step.
        assert "Run the portfolio cycle" in r["opc"]["html"]
        assert _paths(r["posts_after_click"]) == ["/v1/operations/portfolio-cycle/run"]
        assert r["posts_after_click"][0]["body"] == {
            "confirmation": ws.PORTFOLIO_CYCLE_CONFIRMATION}

    def test_secondary_close_controls_recede(self, harness_report):
        """STAGE 19.3: the secondary close panels no longer duplicate the write action.
        The August-13 Today screen stacked FOUR "Run Daily Close" controls for ONE
        action; the panels now show status and the command bar owns execution."""
        r = harness_report["ready_for_close"]
        assert r["dc"]["cc_btn"]["display"] == "none"
        assert r["dc"]["dw_btn"]["display"] == "none"
        assert r["dc"]["pm_btn"]["display"] == "none"
        assert "Run the Daily Close" not in r["hero"]["html"]
        assert "Run the Daily Close" not in r["banner_dw"]["html"]
        # Before the canonical payload loaded the gate failed CLOSED.
        assert r["pre_ws"]["dcBtn"]["display"] == "none"

    def test_drc_no_longer_required(self, harness_report):
        r = harness_report["ready_for_close"]
        assert "Run the Daily Research Cycle" not in r["banner_dw"]["html"]

    def test_double_submit_protected(self, harness_report):
        r = harness_report["ready_for_close"]
        assert len(r["posts_after_double"]) == 1


class TestUiBehaviourCycleComplete:
    def test_no_required_cta(self, harness_report):
        r = harness_report["cycle_complete"]
        assert r["hero"]["attrs"].get("data-density") == "compact"
        assert "th-cta" not in r["hero"]["html"]
        # Stage 19.3: the rail MIRRORS the canonical operator-command text verbatim
        # (ws.NO_ACTION_TEXT) instead of composing its own sentence.
        assert r["right_next"]["text"] == ws.NO_ACTION_TEXT
        assert r["right_btn"]["display"] == "none"
        assert r["right_task"]["text"] == "Monitor the portfolio."
        assert r["posts_after_click"] == []

    def test_stale_close_panel_cannot_contradict(self, harness_report):
        r = harness_report["cycle_complete"]
        assert r["dc"]["cc_btn"]["display"] == "none"
        assert r["dc"]["cc_headline"]["text"] == "Daily Close complete for 2026-08-05."
        assert r["dc"]["cc_badge"]["text"] == "COMPLETE"
        assert r["posts_after_close_attempt"] == []
        assert any("complete" in t["msg"] for t in r["toasts_after_close_attempt"])

    def test_legacy_stage_row_is_optional_review_not_needs_action(self, harness_report):
        r = harness_report["cycle_complete"]
        assert r["stages"]["cc-stage-review"]["text"] == "OPTIONAL REVIEW"
        assert r["stages"]["cc-stage-decide"]["text"] == "OPTIONAL REVIEW"
        assert "NEEDS ACTION" not in json.dumps(r["stages"])

    def test_lifecycle_needs_action_framed_optional_after_completion(self, harness_report):
        # e.g. the manual model-target review (READY_TO_CONFIRM) is optional
        # context once the canonical answer is "no action required".
        done = harness_report["cycle_complete"]["ob_needs_action_framing"]
        assert done["label"] == "OPTIONAL REVIEW"
        # In a genuine action state the lifecycle urgency is preserved verbatim.
        live = harness_report["research_required"]["ob_needs_action_framing"]
        assert live["label"] == "NEEDS ACTION"

    def test_stale_close_cycle_stage_never_needs_action_after_completion(self, harness_report):
        # A stale close payload claiming "Manual Review & Paper Orders —
        # NEEDS_ACTION" renders as PENDING under the canonical no-op state
        # (the same verdict the repaired close owner emits server-side).
        rendered = harness_report["cycle_complete"]["rendered_cycle_stages"]
        assert rendered and all(s["status"] != "NEEDS_ACTION" for s in rendered)


class TestUiBehaviourBlockedInconsistent:
    def test_no_dangerous_execution_controls(self, harness_report):
        for name, dest in (("cycle_blocked", "daily-workflow"),
                           ("inconsistent", "research-audit")):
            r = harness_report[name]
            assert r["posts_after_click"] == []
            assert r["navs_after_click"] == [dest]
            assert r["posts_after_close_attempt"] == []
        assert harness_report["cycle_blocked"]["dc"]["cc_btn"]["display"] == "none"
