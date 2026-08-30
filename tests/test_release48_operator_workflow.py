"""Release 48 — the ONE canonical portfolio-cycle orchestration + operator
presentation contract.

What these tests prove:

  * ONE orchestration entrypoint (``api.portfolio_cycle``) sequences the EXISTING
    execution owners exactly as the ONE workflow owner decides between steps —
    never a second decision engine, never a second write path;
  * the operator confirms ONE token (``RUN_PORTFOLIO_CYCLE``); the composed
    owners' own tokens are supplied by the orchestrator and every step is
    attributed ``portfolio_cycle:<requested_by>``;
  * each owner runs AT MOST once per operator action; a blocked owner stops the
    run with the owner's own words; an unrecognised state stops fail-closed;
  * the run ALWAYS stops at the governed portfolio decision boundary — it can
    never review, approve, confirm, execute, promote or recalibrate anything;
  * the canonical operator command presents the one portfolio-cycle action
    whenever a normal-path mutation is due, with the decided underlying step
    beside it, and presents nothing new in any passive state;
  * the UI carries exactly one portfolio-cycle runner wired through the ONE
    canonical dispatcher, POSTing the one orchestration route.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from paper_trader.api import daily_close as dcm
from paper_trader.api import daily_research_cycle as drcm
from paper_trader.api import portfolio_cycle as pc
from paper_trader.api import workflow_state as ws

REPO = Path(__file__).resolve().parents[1]
APP_SRC = (REPO / "api" / "app.py").read_text(encoding="utf-8", errors="replace")
PC_SRC = (REPO / "api" / "portfolio_cycle.py").read_text(encoding="utf-8",
                                                         errors="replace")
UI_SRC = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8",
                                                        errors="replace")


def _primary(overall: str) -> dict:
    return ws._primary_action(overall, {"eligible_date": "2026-08-28",
                                        "session_operator_action": ""})


def _wf(overall: str, primary: dict | None = None) -> dict:
    return {"overall_state": overall,
            "primary_action": _primary(overall) if primary is None else primary,
            "operator_command": {"state": overall},
            "canonical_portfolio_decision": {"state": "NOT_RUN"}}


# --------------------------------------------------------------------------- #
# 1. Frozen contracts: one token, one route, owners' tokens matched verbatim.
# --------------------------------------------------------------------------- #
class TestContracts:
    def test_01_one_operator_token(self):
        assert pc.EXECUTE_CONFIRMATION == "RUN_PORTFOLIO_CYCLE"
        assert ws.PORTFOLIO_CYCLE_CONFIRMATION == pc.EXECUTE_CONFIRMATION
        assert ws.PORTFOLIO_CYCLE_OWNER == pc.ORCHESTRATION_OWNER == \
            "api.portfolio_cycle"

    def test_02_composed_owner_tokens_match_the_owners(self):
        assert pc._STEP_CONFIRMATIONS[pc.STEP_DAILY_CLOSE] == \
            dcm.EXECUTE_CONFIRMATION
        assert pc._STEP_CONFIRMATIONS[pc.STEP_DAILY_RESEARCH_CYCLE] == \
            drcm.EXECUTE_CONFIRMATION

    def test_03_presented_contract_names_the_run_route(self):
        assert ws.PORTFOLIO_CYCLE_EXECUTION_CONTRACT["path"] == pc.RUN_ROUTE
        assert ws.PORTFOLIO_CYCLE_EXECUTION_CONTRACT["confirmation_token"] == \
            pc.EXECUTE_CONFIRMATION
        assert ws.PORTFOLIO_CYCLE_EXECUTION_CONTRACT["method"] == "POST"

    def test_04_routes_declared_exactly_once(self):
        assert len(re.findall(
            r'@app\.post\(\s*\n?\s*"/v1/operations/portfolio-cycle/run"',
            APP_SRC)) == 1
        assert len(re.findall(
            r'@app\.get\(\s*\n?\s*"/v1/operations/portfolio-cycle"',
            APP_SRC)) == 1

    def test_05_step_vocabulary_is_the_two_existing_owners_only(self):
        assert pc.STEP_VOCABULARY == ("DAILY_CLOSE", "DAILY_RESEARCH_CYCLE")
        assert pc.STEP_OWNERS == {"DAILY_CLOSE": "api.daily_close",
                                  "DAILY_RESEARCH_CYCLE":
                                      "api.daily_research_cycle"}


# --------------------------------------------------------------------------- #
# 2. Pure planning: the workflow owner's decision, obeyed verbatim.
# --------------------------------------------------------------------------- #
class TestPlanNextStep:
    @pytest.mark.parametrize("overall,expected_step,expected_stop", [
        ("WAITING_FOR_SESSION_CLOSE", None, pc.STOP_WAITING_FOR_SESSION_CLOSE),
        ("WAITING_FOR_OWNED_DATA", "DAILY_CLOSE", None),
        ("READY_FOR_DAILY_CLOSE", "DAILY_CLOSE", None),
        ("RESEARCH_CYCLE_REQUIRED", "DAILY_RESEARCH_CYCLE", None),
        ("RESEARCH_CYCLE_RUNNING", None, pc.STOP_CYCLE_ALREADY_RUNNING),
        ("RESEARCH_CYCLE_BLOCKED", None, pc.STOP_RECOVERY_REQUIRED),
        ("PORTFOLIO_REASSESSMENT_REQUIRED", "DAILY_RESEARCH_CYCLE", None),
        ("MANUAL_REVIEW_REQUIRED", None, pc.STOP_DECISION_PRESENTED),
        ("DAILY_CYCLE_COMPLETE", None, pc.STOP_DECISION_PRESENTED),
        ("DAILY_CYCLE_COMPLETE_EVIDENCE_GAP", None, pc.STOP_DECISION_PRESENTED),
        ("INCONSISTENT_STATE", None, pc.STOP_RECOVERY_REQUIRED),
    ])
    def test_10_every_overall_state_maps_deterministically(
            self, overall, expected_step, expected_stop):
        plan = pc.plan_next_step(_wf(overall))
        assert plan["step"] == expected_step, (overall, plan)
        assert plan["stop_reason"] == expected_stop, (overall, plan)
        assert plan["reason"]

    def test_11_the_map_covers_every_declared_overall_state(self):
        for overall in ws.OVERALL_STATES:
            plan = pc.plan_next_step(_wf(overall))
            assert (plan["step"] in (None,) + pc.STEP_VOCABULARY)
            assert plan["step"] or plan["stop_reason"] in pc.STOP_VOCABULARY

    def test_12_unknown_state_fails_closed(self):
        plan = pc.plan_next_step({"overall_state": "SOME_FUTURE_STATE",
                                  "primary_action": {}})
        assert plan["step"] is None
        assert plan["stop_reason"] == pc.STOP_RECOVERY_REQUIRED
        # …and an empty payload never becomes runnable either.
        empty = pc.plan_next_step({})
        assert empty["step"] is None

    def test_13_a_mutation_step_requires_the_owners_own_availability(self):
        # The workflow owner said the close kind but NOT execution_available:
        # nothing may run off a withheld action.
        plan = pc.plan_next_step({
            "overall_state": "READY_FOR_DAILY_CLOSE",
            "primary_action": {"execution_kind": "DAILY_CLOSE",
                               "execution_available": False}})
        assert plan["step"] is None
        assert plan["stop_reason"] == pc.STOP_RECOVERY_REQUIRED


# --------------------------------------------------------------------------- #
# 3. The orchestrated run.
# --------------------------------------------------------------------------- #
def _runner_recorder(result, calls, key, expect_attr="portfolio_cycle:manual_ui"):
    def _run(*, requested_by):
        calls.append((key, requested_by))
        assert requested_by == expect_attr
        return result
    return _run


class TestRunPortfolioCycle:
    def test_20_wrong_token_refuses_and_runs_nothing(self):
        calls = []
        r = pc.run_portfolio_cycle(
            confirm="RUN_DAILY_RESEARCH_CYCLE",  # a REAL token — of another owner
            workflow_loader=lambda: (_ for _ in ()).throw(AssertionError),
            close_runner=_runner_recorder({}, calls, "close"),
            drc_runner=_runner_recorder({}, calls, "drc"))
        assert r["status"] == "PORTFOLIO_CYCLE_CONFIRM_REQUIRED"
        assert r["performed_write"] is False
        assert r["confirmation_required"] == pc.EXECUTE_CONFIRMATION
        assert calls == []

    def test_21_close_then_research_then_decision(self):
        seq = [_wf("READY_FOR_DAILY_CLOSE"), _wf("RESEARCH_CYCLE_REQUIRED"),
               _wf("DAILY_CYCLE_COMPLETE")]
        calls, i = [], {"n": 0}

        def loader():
            v = seq[min(i["n"], len(seq) - 1)]
            i["n"] += 1
            return v

        r = pc.run_portfolio_cycle(
            confirm=pc.EXECUTE_CONFIRMATION,
            workflow_loader=loader,
            close_runner=_runner_recorder(
                {"status": "DAILY_CLOSE_COMPLETE_HOLD",
                 "performed_write": True}, calls, "close"),
            drc_runner=_runner_recorder(
                {"state": "COMPLETE", "run_id": "r1",
                 "performed_write": True}, calls, "drc"))
        assert [c[0] for c in calls] == ["close", "drc"]
        assert r["steps_taken"] == ["DAILY_CLOSE", "DAILY_RESEARCH_CYCLE"]
        assert r["stop_reason"] == pc.STOP_DECISION_PRESENTED
        assert r["status"] == "PORTFOLIO_CYCLE_COMPLETE"
        assert r["stopped_at_decision_boundary"] is True

    def test_22_already_processed_close_is_a_clean_pass_through(self):
        # ALREADY_PROCESSED is the close owner's idempotent answer, not a
        # blocker: the run continues to the next decided step.
        seq = [_wf("READY_FOR_DAILY_CLOSE"), _wf("DAILY_CYCLE_COMPLETE")]
        i = {"n": 0}

        def loader():
            v = seq[min(i["n"], len(seq) - 1)]
            i["n"] += 1
            return v

        r = pc.run_portfolio_cycle(
            confirm=pc.EXECUTE_CONFIRMATION, workflow_loader=loader,
            close_runner=lambda *, requested_by: {
                "status": "ALREADY_PROCESSED", "performed_write": False},
            drc_runner=lambda *, requested_by: pytest.fail("no DRC required"))
        assert r["steps_taken"] == ["DAILY_CLOSE"]
        assert r["stop_reason"] == pc.STOP_DECISION_PRESENTED

    def test_23_each_owner_runs_at_most_once(self):
        # The workflow owner keeps demanding the close: the run refuses to
        # repeat an owner and stops with the named reason.
        calls = []
        r = pc.run_portfolio_cycle(
            confirm=pc.EXECUTE_CONFIRMATION,
            workflow_loader=lambda: _wf("READY_FOR_DAILY_CLOSE"),
            close_runner=_runner_recorder(
                {"status": "DAILY_CLOSE_COMPLETE_HOLD"}, calls, "close"),
            drc_runner=lambda *, requested_by: pytest.fail("never decided"))
        assert [c[0] for c in calls] == ["close"]
        assert r["stop_reason"] == pc.STOP_STATE_DID_NOT_ADVANCE
        assert r["status"] == "PORTFOLIO_CYCLE_STOPPED"

    def test_24_a_blocked_close_stops_the_run_with_the_owners_words(self):
        r = pc.run_portfolio_cycle(
            confirm=pc.EXECUTE_CONFIRMATION,
            workflow_loader=lambda: _wf("READY_FOR_DAILY_CLOSE"),
            close_runner=lambda *, requested_by: {
                "status": "DATA_BLOCKED",
                "message": "owned provider has not published the session"},
            drc_runner=lambda *, requested_by: pytest.fail(
                "a blocked close must stop the run"))
        assert r["stop_reason"] == pc.STOP_OWNER_REPORTED_BLOCKER
        assert "DATA_BLOCKED" in r["stop_detail"]
        assert "owned provider has not published" in r["stop_detail"]
        assert r["steps_taken"] == ["DAILY_CLOSE"]

    def test_25_a_blocked_research_cycle_stops_the_run(self):
        r = pc.run_portfolio_cycle(
            confirm=pc.EXECUTE_CONFIRMATION,
            workflow_loader=lambda: _wf("RESEARCH_CYCLE_REQUIRED"),
            close_runner=lambda *, requested_by: pytest.fail("no close decided"),
            drc_runner=lambda *, requested_by: {
                "state": "BLOCKED",
                "message": "monthly momentum input due; no safe emitter"})
        assert r["stop_reason"] == pc.STOP_OWNER_REPORTED_BLOCKER
        assert "BLOCKED" in r["stop_detail"]

    def test_26_decision_states_run_nothing(self):
        for overall in ("MANUAL_REVIEW_REQUIRED", "DAILY_CYCLE_COMPLETE",
                        "DAILY_CYCLE_COMPLETE_EVIDENCE_GAP",
                        "WAITING_FOR_SESSION_CLOSE", "RESEARCH_CYCLE_RUNNING",
                        "INCONSISTENT_STATE"):
            r = pc.run_portfolio_cycle(
                confirm=pc.EXECUTE_CONFIRMATION,
                workflow_loader=lambda o=overall: _wf(o),
                close_runner=lambda *, requested_by: pytest.fail("no step"),
                drc_runner=lambda *, requested_by: pytest.fail("no step"))
            assert r["steps_taken"] == [], overall
            assert r["performed_write"] is False, overall

    def test_27_result_carries_the_operator_projection_verbatim(self):
        final = _wf("DAILY_CYCLE_COMPLETE")
        final["canonical_portfolio_decision"] = {"state": "HOLD_CURRENT_BOOK",
                                                 "headline": "HOLD THE CURRENT BOOK"}
        r = pc.run_portfolio_cycle(
            confirm=pc.EXECUTE_CONFIRMATION, workflow_loader=lambda: final,
            close_runner=lambda *, requested_by: pytest.fail("no step"),
            drc_runner=lambda *, requested_by: pytest.fail("no step"))
        assert r["canonical_portfolio_decision"]["state"] == "HOLD_CURRENT_BOOK"
        assert r["overall_state"] == "DAILY_CYCLE_COMPLETE"

    def test_28_safety_block_is_explicit_and_false_everywhere_dangerous(self):
        r = pc.run_portfolio_cycle(
            confirm=pc.EXECUTE_CONFIRMATION,
            workflow_loader=lambda: _wf("DAILY_CYCLE_COMPLETE"))
        s = r["safety"]
        assert s["creates_orders"] is False
        assert s["creates_fills"] is False
        assert s["approves_proposals"] is False
        assert s["confirms_order_plans"] is False
        assert s["executes_rebalance"] is False
        assert s["promotes_models"] is False
        assert s["recalibrates_models"] is False
        assert s["touches_r46_research"] is False
        assert s["owns_no_store"] is True
        assert s["automation"] == "OFF"


# --------------------------------------------------------------------------- #
# 4. The read-only status.
# --------------------------------------------------------------------------- #
class TestLoadPortfolioCycle:
    def test_30_close_required_previews_close_then_conditional_research(self):
        d = pc.load_portfolio_cycle(workflow=_wf("READY_FOR_DAILY_CLOSE"))
        assert d["cycle_run_available"] is True
        assert [s["step"] for s in d["planned_steps"]] == [
            "DAILY_CLOSE", "DAILY_RESEARCH_CYCLE"]
        assert d["planned_steps"][1]["conditional"] is True
        assert d["execution_contract"]["path"] == pc.RUN_ROUTE

    def test_31_decision_presented_offers_no_run(self):
        d = pc.load_portfolio_cycle(workflow=_wf("MANUAL_REVIEW_REQUIRED"))
        assert d["cycle_run_available"] is False
        assert d["planned_steps"] == []
        assert d["stop_reason"] == pc.STOP_DECISION_PRESENTED

    def test_32_read_status_runs_no_owner(self):
        d = pc.load_portfolio_cycle(
            workflow=_wf("READY_FOR_DAILY_CLOSE"),
            workflow_loader=lambda: pytest.fail("injected workflow provided"))
        assert d["safety"]["performed_write"] is False


# --------------------------------------------------------------------------- #
# 5. The operator presentation (one concept, decided step beside it).
# --------------------------------------------------------------------------- #
class TestOperatorPresentation:
    def _cmd(self, overall):
        return ws.build_operator_command(
            overall=overall, primary=_primary(overall), pending_orders=0,
            eligible_date="2026-08-28", latest_close_date="2026-08-27")

    def test_40_every_mutation_state_presents_the_one_cycle_action(self):
        for overall in ws.OVERALL_STATES:
            c = self._cmd(overall)
            if c["primary_action_available"]:
                assert c["primary_action_kind"] == ws.EXEC_PORTFOLIO_CYCLE
                assert c["primary_action_label"] == ws.PORTFOLIO_CYCLE_LABEL
                assert c["confirmation_required"] == \
                    ws.PORTFOLIO_CYCLE_CONFIRMATION
                assert c["cycle_underlying_kind"] in \
                    ws.NORMAL_PATH_EXECUTION_KINDS
                assert c["primary_action_execution_contract"]["path"] == \
                    pc.RUN_ROUTE
            else:
                assert c["primary_action_kind"] is None
                assert c["cycle_underlying_kind"] is None

    def test_41_underlying_step_is_the_decided_owner(self):
        assert self._cmd("READY_FOR_DAILY_CLOSE")["cycle_underlying_kind"] == \
            ws.EXEC_DAILY_CLOSE
        assert self._cmd("WAITING_FOR_OWNED_DATA")["cycle_underlying_kind"] == \
            ws.EXEC_DAILY_CLOSE
        assert self._cmd("RESEARCH_CYCLE_REQUIRED")["cycle_underlying_kind"] == \
            ws.EXEC_DAILY_RESEARCH_CYCLE

    def test_42_supporting_text_promises_the_decision_boundary(self):
        for overall in ("READY_FOR_DAILY_CLOSE", "RESEARCH_CYCLE_REQUIRED"):
            sup = self._cmd(overall)["supporting_text"]
            assert "governed" in sup and "portfolio decision" in sup
            assert "nothing is approved or executed automatically" in sup.lower()

    def test_43_passive_states_present_nothing_new(self):
        for overall in ("MANUAL_REVIEW_REQUIRED", "WAITING_FOR_SESSION_CLOSE",
                        "DAILY_CYCLE_COMPLETE", "INCONSISTENT_STATE"):
            c = self._cmd(overall)
            assert c["primary_action_available"] is False
            assert c["mutation_controls_allowed"] is False


# --------------------------------------------------------------------------- #
# 6. Structural governance: no second engine, no reachable execution authority.
# --------------------------------------------------------------------------- #
class TestStructuralGovernance:
    def test_50_orchestrator_delegates_to_the_two_owners_only(self):
        assert "run_daily_close(" in PC_SRC
        assert "run_daily_research_cycle(" in PC_SRC

    def test_51_orchestrator_cannot_reach_execution_or_research_authority(self):
        # Scan CODE only (docstrings and comments legitimately NAME the
        # boundaries they promise) — parse, drop every docstring, unparse.
        import ast

        tree = ast.parse(PC_SRC)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.FunctionDef,
                                 ast.AsyncFunctionDef, ast.ClassDef)):
                body = node.body
                if body and isinstance(body[0], ast.Expr) and isinstance(
                        body[0].value, ast.Constant) and isinstance(
                        body[0].value.value, str):
                    node.body = body[1:] or [ast.Pass()]
        code_only = ast.unparse(tree)
        for forbidden in ("rebalance_execution", "record_decision",
                          "APPROVE_FOR_PAPER_REBALANCE",
                          "CONFIRM_PORTFOLIO_REBALANCE_DECISION",
                          "CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN",
                          "paper_trading_desk", "settle_due_orders",
                          "create_order", "alpha_agent",
                          "prospective_tournament", "promote(", "recalibrate("):
            assert forbidden not in code_only, forbidden

    def test_52_orchestrator_owns_no_persistence(self):
        for forbidden in ("open(", "write_text", "atomic_write", "json.dump(",
                          "Path.home", "PAPER_TRADER_", "mkdir"):
            assert forbidden not in PC_SRC, forbidden

    def test_53_no_second_portfolio_cycle_orchestrator(self):
        hits = []
        for fp in sorted((REPO / "api").glob("*.py")) + sorted(
                (REPO / "engine").glob("*.py")):
            if fp.name == "portfolio_cycle.py":
                continue
            src = fp.read_text(encoding="utf-8", errors="replace")
            if "def run_portfolio_cycle(" in src:
                hits.append(fp.name)
        assert hits == []

    def test_54_ui_has_exactly_one_cycle_runner_via_the_one_dispatcher(self):
        assert UI_SRC.count("function runPortfolioCycle(") == 1
        assert UI_SRC.count("'/v1/operations/portfolio-cycle/run'") == 1
        fn = UI_SRC.split("function dispatchCanonicalPrimaryAction(")[1]
        fn = fn.split("\nwindow.")[0]
        assert "PORTFOLIO_CYCLE" in fn and "runPortfolioCycle(btn)" in fn
        # The dispatcher still refuses to execute off Today.
        assert "_wsIsTodayRoute()" in fn

    def test_55_ui_cycle_runner_uses_no_forbidden_dialogs(self):
        fn = UI_SRC.split("function runPortfolioCycle(")[1].split("\nwindow.")[0]
        assert not re.search(r"(?<![\w.])alert\s*\(", fn)
        assert not re.search(r"(?<![\w.])confirm\s*\(", fn)

    def test_56_route_docstrings_promise_the_boundary(self):
        seg = APP_SRC.split('"/v1/operations/portfolio-cycle/run"')[1]
        seg = seg.split("class EventSignalRefreshRunRequest")[0]
        for promise in ("never", "stops at the governed portfolio decision"):
            assert promise in seg
