"""Release 49 — the ONE reconciled operator presentation + the Today / Portfolio
operator-experience rebuild.

What these tests prove:

  * ONE presentation owner (``api.operator_presentation``) exists, is pure
    (no I/O, no owner call inside the builder, no business recomputation) and
    reaches no execution / approval / desk / research authority;
  * the HISTORICAL (pre-R47) session is reconciled — a blocked decision whose
    blockers lie outside the owner's own true-blocker vocabulary, on a session
    the governed cycle completed without a target — as HISTORICAL DECISION with
    the next ELIGIBLE action, never a rerun instruction, never a fabricated
    proposal, never a rewritten record;
  * every Release-47 / Release-48 state maps deterministically onto the frozen
    presentation vocabulary; an unknown owner state fails CLOSED (BLOCKED);
  * the only executing next action is the Release-48 portfolio cycle, carried
    verbatim from the workflow owner's presented contract;
  * a collection-infrastructure problem is DEGRADED and explicitly non-blocking;
  * snapshot / alerts / economics are the owners' own numbers, verbatim;
  * Today carries exactly four primary sections, one loader, at most one primary
    CTA through the ONE canonical dispatcher, no badge wall and no material
    table; Portfolio carries the four task views, reads the presentation owner
    on Overview, and keeps the model target, the paper desk, corporate-action
    detail and the raw reassessment / HOC / proposal / rebalance machinery under
    Audit & Details; the performance charts live under Performance;
  * no grid of dashes renders for an absent target; the normal-mode renderer
    writes no raw implementation vocabulary;
  * R47 and R48 are preserved and the strict audit is green.
"""
from __future__ import annotations

import ast
import copy
import importlib
import json
import re
from pathlib import Path

import pytest

from paper_trader.api import operator_presentation as op
from paper_trader.api import portfolio_cycle as pc
from paper_trader.api import workflow_state as ws

REPO = Path(__file__).resolve().parents[1]
UI = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8", errors="replace")
APP = (REPO / "api" / "app.py").read_text(encoding="utf-8", errors="replace")
SRC = (REPO / "api" / "operator_presentation.py").read_text(encoding="utf-8",
                                                              errors="replace")

RAW_TOKENS = ("MANUAL_REVIEW_REQUIRED", "PORTFOLIO_DECISION_NO_PROPOSAL",
              "REBALANCE_NO_PROPOSAL", "STATE_NOT_RUN", "NOT_RUN",
              "RUN_DAILY_RESEARCH_CYCLE", "CONFIRM_ALPHA_DAILY_CLOSE",
              "CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN",
              "REALLOCATION_PROPOSAL_NOT_RUN", "SECTOR_WEIGHT_BREACH")


# --------------------------------------------------------------------------- #
# Fixtures — compact owner payloads with the exact shapes the owners publish.
# --------------------------------------------------------------------------- #
BREACHES = ["ABNB:SECTOR_WEIGHT_BREACH", "CVS:SECTOR_WEIGHT_BREACH",
            "MNST:RISK_CONTRIBUTION_BREACH"]
TRUE_BLOCKERS = ["CRITICAL_STALE_OR_MISSING_MARKET_DATA",
                 "POINT_IN_TIME_INTEGRITY_FAILURE", "NAV_ACCOUNTING_UNRECONCILED",
                 "IMPOSSIBLE_LIQUIDITY_OR_CAPACITY",
                 "NO_FEASIBLE_PORTFOLIO_UNDER_MANDATORY_CONSTRAINTS",
                 "REQUIRED_MANUAL_AUTHORIZATION_MISSING"]


def _wf_historical() -> dict:
    return {
        "status": "OK", "overall_state": "MANUAL_REVIEW_REQUIRED",
        "primary_action": {"action_code": "MANUAL_PORTFOLIO_REVIEW",
                           "label": "Review the portfolio constraint breach",
                           "explanation": "A holding breaches a hard portfolio constraint.",
                           "severity": "ATTENTION", "destination": "portfolio-manager",
                           "focus": "reassessment", "execution_available": False},
        "operator_command": {"state": "MANUAL_REVIEW_REQUIRED", "passive": True,
                             "primary_action_available": False,
                             "primary_action_kind": None, "next_text": "Review",
                             "eligible_market_date": "2026-08-28",
                             "latest_completed_close_date": "2026-08-28"},
        "blockers": [],
        "operational_state": {"active_book_id": "alpha_paper_book_1",
                              "active_book_name": "Alpha Paper Book #1",
                              "book_status": "FORWARD_TRACKING_ACTIVE",
                              "valuation_date": "2026-08-28", "nav": 99382.89,
                              "cash": 4482.71, "holdings_count": 25,
                              "pending_orders": 0,
                              "latest_completed_close_date": "2026-08-28",
                              "latest_close_status": "DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT",
                              "operational_close_valid": True,
                              "eligible_market_date": "2026-08-28"},
        "evidence_state": {"documented_gap": False, "gap_severity": "INFO",
                           "evidence_status": "PRELIMINARY_EVIDENCE"},
        "evidence_classification": {"is_operational_incident": False,
                                    "blocks_portfolio_action": False},
        "data_gap_taxonomy": {"has_blocking_gap": False, "affected_tickers": []},
        "model_review": {"model_review_state": "MODEL_HEALTHY",
                         "model_review_required": False},
        "portfolio_decision_state": {
            "portfolio_decision_state": "PORTFOLIO_DECISION_NO_PROPOSAL",
            "requires_manual_review": False, "approvable": False,
            "materiality": {"action_counts": {"RETAIN": 0, "INCREASE": 0, "REDUCE": 0,
                                              "EXIT": 0, "ADD": 0, "REPLACE_OUT": 0,
                                              "REPLACE_IN": 0}},
            "reallocation_outcome": None, "hold_current_book": False,
            "feasible_target_exists": False},
        "portfolio_reassessment": {"state": "MANUAL_REVIEW_REQUIRED",
                                   "blockers": list(BREACHES)},
        "portfolio_reassessment_execution_precedence": {
            "execution_active": False, "rebalance_state": None, "pending_orders": 0},
        "reallocation_proposal_presentation": {
            "state": "NOT_RUN", "governed_cycle_complete_for_session": True,
            "economic_gate_withheld_the_proposal": True,
            "running_the_cycle_again_would_change_nothing": True},
        "reallocation_operator_state": "REALLOCATION_PROPOSAL_NOT_RUN",
        "canonical_portfolio_decision": {
            "state": "BLOCKED", "headline": "PORTFOLIO DECISION BLOCKED",
            "eligible_market_date": "2026-08-28",
            "explanation": "A holding breaches a hard portfolio constraint (%s)."
                           % ", ".join(BREACHES),
            "withheld_reasons": list(BREACHES), "reallocation_outcome": None,
            "hold_current_book": False, "feasible_target_exists": False,
            "constraints_that_reshaped": [], "constraint_reoptimized": False,
            "expected_net_improvement": 0.0, "net_improvement_hurdle": 0.05,
            "expected_one_way_turnover": 0.0, "turnover_budget": 0.35,
            "expected_transaction_cost_usd": 0.0, "mandatory_exit_tickers": [],
            "operator_action_available": False},
        "safety": {"safety_badges": ["READ ONLY", "NO ORDERS", "MANUAL REVIEW"]},
    }


def _constrained_not_run() -> dict:
    return {
        "state": "NOT_RUN", "outcome": None, "feasible_target_exists": None,
        "eligible_market_date": "2026-08-28",
        "active_book": {"book_label": "Alpha Paper Book #1"},
        "current_paper_book": {"weights": {}, "position_count": None,
                               "cash_weight": None, "nav": None},
        "ideal_target": {"weights": {}}, "constraint_adjustments": [],
        "constraints_that_reshaped": [],
        "constraint_inventory": {"true_blocker_codes": list(TRUE_BLOCKERS)},
        "best_feasible_target": {"weights": {}, "position_count": None,
                                 "cash_weight": None, "allocations": [],
                                 "constraints": {}},
        "switching_economics": {}, "turnover": {}, "reallocation_outcome": {},
        "outcome_vocabulary": ["PROPOSAL_READY", "HOLD_CURRENT_BOOK", "TRUE_BLOCKER"],
        "headline": "NO PROPOSAL YET - RUN THE DAILY RESEARCH CYCLE",
        "approval": {"portfolio_decision_state": "PORTFOLIO_DECISION_NO_PROPOSAL",
                     "label": "No proposal yet", "approvable": False},
        "execution": {"rebalance_state": "REBALANCE_NO_PROPOSAL",
                      "message": "No reallocation proposal exists."},
        "safety": {"safety_badges": ["PAPER ONLY", "REVIEW ONLY"]},
    }


def _daily_close() -> dict:
    return {"pnl": {"valuation_date": "2026-08-28", "starting_capital": 100000.0,
                    "nav": 99382.89, "cash": 4482.71, "invested_value": 94900.18,
                    "daily_pnl": -419.17, "daily_return_pct": -0.42,
                    "daily_pnl_available": True, "cumulative_pnl": -617.11,
                    "cumulative_return_pct": -0.6171,
                    "spy_cumulative_return_pct": 2.9355, "excess_return_pct": -3.5526,
                    "drawdown_pct": -6.3861, "basis": "CURRENT_ECONOMIC_STATE"}}


def _material() -> dict:
    rows = [
        {"event_id": "e1", "ticker": "NVDA", "held": False, "what_changed": "Dividend 0.25",
         "signal_authority": "OPERATIONAL_RISK", "timestamp": "2026-09-10",
         "source_url_state": "NO_CANONICAL_SOURCE_URL"},
        {"event_id": "e2", "ticker": None, "held": False,
         "what_changed": "Regulatory Event: SEC proposes amendments",
         "signal_authority": "EVENT_TRIGGER_ONLY", "timestamp": "2026-08-28T13:33:00-04:00",
         "source_url": "https://www.sec.gov/x", "source_url_state": "CANONICAL_SOURCE_URL"},
        {"event_id": "e3", "ticker": "EXPE", "held": True, "hoc_affected": True,
         "hoc_recommendation": "REDUCE", "what_changed": "News: questionable fundamentals",
         "signal_authority": "EVENT_TRIGGER_ONLY", "timestamp": "2026-08-28T10:40:52+00:00",
         "source_url": "https://finance.yahoo.com/x", "source_url_state": "CANONICAL_SOURCE_URL"},
        {"event_id": "e4", "ticker": "AAPL", "held": False, "what_changed": "News: ETF",
         "signal_authority": "EVENT_TRIGGER_ONLY", "timestamp": "2026-08-28T10:20:01+00:00",
         "source_url_state": "NO_CANONICAL_SOURCE_URL"},
    ]
    return {"state": "READY", "composition_owner": "api.material_information",
            "authority_policy_owner": "engine.event_fabric", "rows": rows,
            "row_count": 4, "total_material_events": 19,
            "material_events_affecting_holdings": 5,
            "affected_holdings": ["CAT", "EXPE"]}


def _collection(state: str = "DEGRADED") -> dict:
    return {"service": {"service_state": state, "worker_activity": "DEAD",
                        "reason": "The worker process is gone."},
            "recovery": {"why": "The worker process is gone."},
            "headline": {"detail": "The worker process is gone."}}


def _outcomes(measured: int = 0) -> dict:
    return {"owner": "api.portfolio_decision_outcome", "decision_count": measured,
            "measured_count": measured, "pending_count": 0,
            "cumulative_incremental_pnl": 12.5 if measured else 0,
            "verdict_counts": {"DECISION_ADDED_VALUE": measured}}


def _build(wf=None, cn=None, **kw):
    return op.build_operator_presentation(
        workflow=_wf_historical() if wf is None else wf,
        constrained=_constrained_not_run() if cn is None else cn,
        daily_close=kw.get("daily_close", _daily_close()),
        material_information=kw.get("material_information", _material()),
        decision_outcomes=kw.get("decision_outcomes", _outcomes()),
        information_collection=kw.get("information_collection", _collection()))


def _wf_with(**over) -> dict:
    wf = _wf_historical()
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(wf.get(k), dict):
            wf[k] = {**wf[k], **v}
        else:
            wf[k] = v
    return wf


def _code_only(src: str) -> str:
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef,
                             ast.ClassDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(
                    body[0].value, ast.Constant) and isinstance(
                    body[0].value.value, str):
                node.body = body[1:] or [ast.Pass()]
    return ast.unparse(tree)


# --------------------------------------------------------------------------- #
# 1. Contracts: ONE owner, frozen vocabulary, one route, registered ownership.
# --------------------------------------------------------------------------- #
class TestContracts:
    def test_01_vocabularies_are_frozen(self):
        assert op.PORTFOLIO_DECISION_VOCABULARY == (
            "CYCLE_REQUIRED", "REALLOCATE", "HOLD", "BLOCKED", "AWAITING_APPROVAL",
            "AWAITING_CONFIRMATION", "AWAITING_NEXT_CLOSE", "OUTCOME_ACCRUING")
        assert op.SYSTEM_READINESS_VOCABULARY == ("READY", "DEGRADED", "BLOCKED")
        assert op.EXECUTING_NEXT_ACTION_KINDS == frozenset({"PORTFOLIO_CYCLE"})
        assert op.SAFETY_MODE_LINE == "PAPER · MANUAL APPROVAL · AUTOMATION OFF"

    def test_02_route_declared_exactly_once_and_get_only(self):
        assert len(re.findall(
            r'@app\.get\(\s*\n?\s*"/v1/operations/operator-presentation"', APP)) == 1
        assert not re.search(
            r'@app\.(post|put|delete|patch)\(\s*\n?\s*"/v1/operations/operator-presentation',
            APP)

    def test_03_route_ownership_and_module_registered(self):
        inv = json.loads((REPO / "docs" / "architecture" / "system_inventory.json")
                         .read_text(encoding="utf-8"))
        owners = [e for e in inv["route_ownership"]
                  if e.get("prefix") == "/v1/operations/operator-presentation"]
        assert len(owners) == 1 and owners[0]["owner"] == "api/operator_presentation.py"
        assert any(m.get("path") == "api/operator_presentation.py" for m in inv["modules"])

    def test_04_no_second_presentation_owner(self):
        hits = []
        for fp in sorted((REPO / "api").glob("*.py")) + sorted((REPO / "engine").glob("*.py")):
            if fp.name == "operator_presentation.py":
                continue
            if "def build_operator_presentation(" in fp.read_text(encoding="utf-8",
                                                                  errors="replace"):
                hits.append(fp.name)
        assert hits == []


# --------------------------------------------------------------------------- #
# 2. Purity: the builder recomputes nothing and reaches no authority.
# --------------------------------------------------------------------------- #
class TestPurity:
    def test_10_code_reaches_no_persistence_authority_or_research(self):
        code = _code_only(SRC)
        for forbidden in ("open(", "json.dump(", "write_text", "mkdir",
                          "run_daily_close(", "run_daily_research_cycle(",
                          "run_portfolio_cycle(", "record_decision",
                          "paper_trading_desk", "settle_due_orders", "create_order",
                          "APPROVE_FOR_PAPER_REBALANCE",
                          "CONFIRM_PORTFOLIO_REBALANCE_DECISION",
                          "CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN",
                          "alpha_agent", "prospective_tournament", "reoptimise(",
                          "reoptimize(", "build_proposal(", "build_assessment(",
                          "from paper_trader.engine", "import engine"):
            assert forbidden not in code, forbidden

    def test_11_builder_calls_no_owner_loader(self):
        tree = ast.parse(SRC)
        fn = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef)
                  and n.name == "build_operator_presentation")
        for node in ast.walk(fn):
            if isinstance(node, ast.Call):
                name = getattr(node.func, "id", None) or getattr(node.func, "attr", "")
                assert not str(name).startswith("load_"), name

    def test_12_declares_recomputes_nothing_and_degrades_without_owners(self):
        p = op.build_operator_presentation(workflow=None)
        assert p["recomputes_nothing"] is True and p["recomputed_concepts"] == []
        assert p["status"] == "DEGRADED"
        assert p["system_readiness"]["state"] == "BLOCKED"
        assert p["portfolio_decision"]["state"] in op.PORTFOLIO_DECISION_VOCABULARY
        assert p["read_only"] is True and p["wrote_to_ledger"] is False

    def test_13_snapshot_and_economics_are_verbatim(self):
        p = _build()
        s = p["portfolio_snapshot"]
        assert s["nav"] == 99382.89 and s["cash"] == 4482.71 and s["positions"] == 25
        assert s["daily_pnl"] == -419.17 and s["cumulative_pnl"] == -617.11
        assert s["excess_return_pct"] == -3.5526 and s["drawdown_pct"] == -6.3861
        d = p["decision_summary"]
        assert d["available"] is False and d["net_improvement"] == 0.0
        assert d["switching_hurdle"] == 0.05 and d["turnover_budget"] == 0.35


# --------------------------------------------------------------------------- #
# 3. Historical / pre-R47 reconciliation — never a rerun, never fabricated.
# --------------------------------------------------------------------------- #
class TestHistoricalReconciliation:
    def test_20_historical_session_is_reconciled_not_rerun(self):
        p = _build()
        d = p["portfolio_decision"]
        hc = p["historical_context"]
        assert hc["historical"] is True
        assert hc["recorded_under"] == "PRIOR_DECISION_WORKFLOW"
        assert hc["session_date"] == "2026-08-28"
        assert hc["breach_tickers"] == ["ABNB", "CVS", "MNST"]
        assert d["state"] == "BLOCKED"
        assert d["headline"] == "HISTORICAL DECISION — 2026-08-28"
        assert "prior decision workflow" in d["explanation"]
        assert "will not be rewritten" in d["explanation"]
        assert d["next_action"]["kind"] == "WAIT"
        assert d["next_action"]["available"] is False
        assert d["next_action"]["executes"] is False
        assert "after the next eligible market close" in d["next_action"]["label"]
        assert hc["history_rewritten"] is False
        assert hc["proposal_fabricated"] is False
        assert hc["rerun_of_historical_session_instructed"] is False

    def test_21_no_rerun_instruction_and_no_raw_vocabulary_in_normal_prose(self):
        p = _build()
        prose = " ".join(str(x) for x in (
            p["headline"], p["explanation"], p["next_action"]["label"],
            p["portfolio_decision"]["blocked_detail"]["cannot_trust"],
            p["portfolio_decision"]["blocked_detail"]["resolves"],
            p["system_readiness"]["summary"],
            *[i["relevance"] for i in p["alerts_summary"]["top_items"]]))
        assert "RUN THE DAILY RESEARCH CYCLE" not in prose.upper()
        assert "rerun" not in prose.lower()
        for tok in RAW_TOKENS:
            assert tok not in prose, tok

    def test_22_the_date_is_read_from_the_owner_not_hard_coded(self):
        wf = _wf_historical()
        wf["canonical_portfolio_decision"]["eligible_market_date"] = "2027-01-15"
        wf["operational_state"]["eligible_market_date"] = "2027-01-15"
        p = _build(wf=wf)
        assert p["portfolio_decision"]["headline"] == "HISTORICAL DECISION — 2027-01-15"
        assert "2026-08-28" not in SRC

    def test_23_a_true_blocker_is_not_historical(self):
        wf = _wf_historical()
        wf["canonical_portfolio_decision"]["withheld_reasons"] = [
            "NAV_ACCOUNTING_UNRECONCILED"]
        wf["canonical_portfolio_decision"]["explanation"] = "NAV does not reconcile."
        p = _build(wf=wf)
        assert p["historical_context"]["historical"] is False
        d = p["portfolio_decision"]
        assert d["state"] == "BLOCKED" and d["headline"] == "BLOCKED"
        assert d["next_action"]["kind"] == "REVIEW_BLOCKER"
        assert d["blocked_detail"]["why"] and d["blocked_detail"]["resolves"]

    def test_24_without_the_owners_true_blocker_vocabulary_nothing_is_called_historical(self):
        cn = _constrained_not_run()
        cn["constraint_inventory"] = {}
        p = _build(cn=cn)
        assert p["historical_context"]["historical"] is False
        assert p["portfolio_decision"]["state"] == "BLOCKED"


# --------------------------------------------------------------------------- #
# 4. Every owner state maps deterministically; unknown fails closed.
# --------------------------------------------------------------------------- #
class TestDecisionMapping:
    def test_30_cycle_required_carries_the_r48_contract_verbatim(self):
        wf = _wf_with(
            overall_state="READY_FOR_DAILY_CLOSE",
            operator_command={"primary_action_available": True,
                              "primary_action_kind": "PORTFOLIO_CYCLE",
                              "primary_action_label": ws.PORTFOLIO_CYCLE_LABEL,
                              "confirmation_required": ws.PORTFOLIO_CYCLE_CONFIRMATION,
                              "primary_action_owner": ws.PORTFOLIO_CYCLE_OWNER,
                              "primary_action_execution_contract":
                                  dict(ws.PORTFOLIO_CYCLE_EXECUTION_CONTRACT),
                              "supporting_text": "Runs the close then the research cycle."},
            canonical_portfolio_decision={"state": "NOT_RUN", "withheld_reasons": []})
        p = _build(wf=wf)
        d = p["portfolio_decision"]
        assert d["state"] == "CYCLE_REQUIRED"
        na = d["next_action"]
        assert na["kind"] == "PORTFOLIO_CYCLE" and na["executes"] is True
        assert na["execution_contract"]["path"] == pc.RUN_ROUTE
        assert na["confirmation_required"] == pc.EXECUTE_CONFIRMATION
        assert na["label"] == ws.PORTFOLIO_CYCLE_LABEL

    def test_31_only_the_cycle_ever_executes(self):
        for kind in ("REVIEW_REALLOCATION", "REVIEW_ORDER_PLAN", "REVIEW_BLOCKER",
                     "WAIT", "NONE"):
            na = op._next_action(kind, available=True, destination="x")
            assert na["executes"] is False
            assert na["execution_contract"] is None

    def test_32_reallocate(self):
        wf = _wf_with(
            overall_state="DAILY_CYCLE_COMPLETE",
            canonical_portfolio_decision={"state": "PROPOSAL_REVIEW_REQUIRED",
                                          "withheld_reasons": [],
                                          "feasible_target_exists": True},
            portfolio_decision_state={"portfolio_decision_state": "PROPOSAL_REVIEW_REQUIRED",
                                      "requires_manual_review": True, "approvable": True,
                                      "reallocation_outcome": "PROPOSAL_READY",
                                      "feasible_target_exists": True,
                                      "materiality": {"action_counts": {
                                          "RETAIN": 20, "EXIT": 2, "REDUCE": 1,
                                          "REPLACE_OUT": 2, "REPLACE_IN": 2, "ADD": 3,
                                          "INCREASE": 0}}})
        cn = _constrained_not_run()
        cn.update({"state": "READY", "outcome": "PROPOSAL_READY",
                   "feasible_target_exists": True,
                   "switching_economics": {"score_improvement_net_of_cost": 0.08,
                                           "switching_hurdle": 0.05,
                                           "clears_switching_hurdle": True,
                                           "one_way_turnover": 0.21,
                                           "estimated_transaction_cost": 52.5,
                                           "portfolio_volatility_before": 0.18,
                                           "portfolio_volatility_after": 0.17}})
        p = _build(wf=wf, cn=cn)
        d = p["portfolio_decision"]
        assert d["state"] == "REALLOCATE"
        assert d["headline"] == "REALLOCATE — 10 POSITIONS CHANGE"
        assert d["next_action"]["kind"] == "REVIEW_REALLOCATION"
        assert d["next_action"]["destination"] == "portfolio-manager/reallocation"
        assert d["governance"]["current_step"] == "REVIEW"
        s = p["decision_summary"]
        assert s["exits"] == 2 and s["reductions"] == 1 and s["replacements"] == 2
        assert s["additions"] == 3 and s["positions_changing"] == 10
        assert s["net_improvement"] == 0.08 and s["switching_hurdle"] == 0.05
        assert s["risk_before"] == 0.18 and s["risk_after"] == 0.17
        assert s["estimated_cost"] == 52.5

    def test_33_hold_is_a_legitimate_decision_with_no_cta(self):
        wf = _wf_with(overall_state="DAILY_CYCLE_COMPLETE",
                      canonical_portfolio_decision={
                          "state": "HOLD_CURRENT_BOOK", "withheld_reasons": [],
                          "no_proposal_reason": "a complete FEASIBLE alternative was priced; "
                                                "its improvement does not justify switching."})
        p = _build(wf=wf)
        d = p["portfolio_decision"]
        assert d["state"] == "HOLD" and d["headline"] == "HOLD CURRENT PORTFOLIO"
        assert d["tone"] == "ok"
        assert d["next_action"]["kind"] == "NONE" and d["next_action"]["available"] is False
        assert d["governance"]["current_step"] is None

    def test_34_no_change_and_withheld_present_as_hold(self):
        for st in ("NO_CHANGE", "CHANGE_CANDIDATE_WITHHELD"):
            wf = _wf_with(overall_state="DAILY_CYCLE_COMPLETE",
                          canonical_portfolio_decision={"state": st, "withheld_reasons": []})
            assert _build(wf=wf)["portfolio_decision"]["state"] == "HOLD", st

    @pytest.mark.parametrize("rb,expected,step", [
        ("PROPOSAL_APPROVED_ORDER_PLAN_REVIEW_REQUIRED", "AWAITING_CONFIRMATION", "CONFIRM"),
        ("ORDER_PLAN_CONFIRMED_PAPER_EXECUTION_PENDING", "AWAITING_NEXT_CLOSE", "AWAIT_NEXT_CLOSE"),
        ("PAPER_EXECUTED_RECONCILED", "OUTCOME_ACCRUING", "OUTCOME_ACCRUING"),
    ])
    def test_35_execution_lifecycle_outranks_the_decision_surface(self, rb, expected, step):
        cn = _constrained_not_run()
        cn["execution"] = {"rebalance_state": rb, "n_orders": 3}
        p = _build(cn=cn)
        d = p["portfolio_decision"]
        assert d["state"] == expected
        assert d["governance"]["current_step"] == step
        steps = {s["step"]: s["status"] for s in d["governance"]["steps"]}
        assert steps[step] == "CURRENT"
        if expected == "OUTCOME_ACCRUING":
            assert steps["EXECUTED"] == "DONE"
        if expected == "AWAITING_CONFIRMATION":
            assert d["next_action"]["kind"] == "REVIEW_ORDER_PLAN"

    def test_36_blocked_order_plan_is_blocked(self):
        cn = _constrained_not_run()
        cn["execution"] = {"rebalance_state": "ORDER_PLAN_BLOCKED_MISSING_OWNED_MARKS",
                           "blocked_reasons": ["ABC:NO_OWNED_MARK"],
                           "message": "Owned marks are missing for ABC."}
        d = _build(cn=cn)["portfolio_decision"]
        assert d["state"] == "BLOCKED" and d["reason_codes"] == ["ABC:NO_OWNED_MARK"]
        assert d["next_action"]["kind"] == "REVIEW_ORDER_PLAN"

    def test_37_held_or_stale_proposal_awaits_approval(self):
        for pdst in ("PROPOSAL_HELD", "STALE_PROPOSAL_REVIEW_REQUIRED"):
            wf = _wf_with(canonical_portfolio_decision={"state": "NOT_RUN",
                                                        "withheld_reasons": []},
                          portfolio_decision_state={"portfolio_decision_state": pdst})
            assert _build(wf=wf)["portfolio_decision"]["state"] == "AWAITING_APPROVAL"

    def test_38_unknown_owner_state_fails_closed(self):
        wf = _wf_with(overall_state="DAILY_CYCLE_COMPLETE",
                      canonical_portfolio_decision={"state": "SOME_FUTURE_STATE",
                                                    "withheld_reasons": []})
        d = _build(wf=wf)["portfolio_decision"]
        assert d["state"] == "BLOCKED" and "NOT RECOGNISED" in d["headline"]

    def test_39_waiting_for_session_close_is_cycle_required_without_a_cta(self):
        wf = _wf_with(overall_state="WAITING_FOR_SESSION_CLOSE",
                      canonical_portfolio_decision={"state": "NOT_RUN",
                                                    "withheld_reasons": []})
        d = _build(wf=wf)["portfolio_decision"]
        assert d["state"] == "CYCLE_REQUIRED"
        assert d["next_action"]["kind"] == "WAIT" and d["next_action"]["available"] is False

    def test_39b_every_vocabulary_state_is_reachable_and_labelled(self):
        assert set(op.PORTFOLIO_DECISION_LABELS) == set(op.PORTFOLIO_DECISION_VOCABULARY)
        for st in op.PORTFOLIO_DECISION_VOCABULARY:
            g = op._governance(st)
            assert [s["step"] for s in g["steps"]] == list(op.GOVERNANCE_SEQUENCE)


# --------------------------------------------------------------------------- #
# 5. System readiness, alerts, outcome.
# --------------------------------------------------------------------------- #
class TestReadinessAlertsOutcome:
    def test_40_collection_degraded_is_non_blocking(self):
        s = _build()["system_readiness"]
        assert s["state"] == "DEGRADED"
        assert s["portfolio_decision_remains_valid"] is True
        col = next(i for i in s["items"] if i["key"] == "collection")
        assert col["state"] == "degraded" and col["blocks_portfolio_decision"] is False
        assert "remains valid" in s["summary"]

    def test_41_ready_when_collection_runs(self):
        s = _build(information_collection=_collection("RUNNING"))["system_readiness"]
        assert s["state"] == "READY"

    def test_42_blocked_only_for_real_blockers(self):
        wf = _wf_with(overall_state="INCONSISTENT_STATE")
        assert _build(wf=wf)["system_readiness"]["state"] == "BLOCKED"
        wf2 = _wf_with(data_gap_taxonomy={"has_blocking_gap": True,
                                          "affected_tickers": ["X"]})
        s2 = _build(wf=wf2)["system_readiness"]
        assert s2["state"] == "BLOCKED" and "blocking data gap" in s2["summary"]

    def test_43_alerts_summary_is_verbatim_top_three_held_first(self):
        a = _build()["alerts_summary"]
        assert a["count"] == 19 and a["portfolio_relevant_count"] == 5
        assert a["affected_holdings"] == ["CAT", "EXPE"]
        assert len(a["top_items"]) == 3
        assert a["top_items"][0]["ticker"] == "EXPE"
        assert a["top_items"][0]["relevance"] == "Held · review signal REDUCE"
        assert a["top_items"][0]["source_url"] == "https://finance.yahoo.com/x"
        assert a["top_items"][1]["ticker"] == "NVDA"
        assert a["top_items"][1]["source_url"] is None
        # the owner's authority travels raw for audit; it is never re-classified
        assert a["top_items"][1]["signal_authority"] == "OPERATIONAL_RISK"
        assert a["detail_location"] == "system-audit/diagnostics"

    def test_44_decision_outcome_only_when_measured(self):
        assert _build()["decision_outcome"]["available"] is False
        o = _build(decision_outcomes=_outcomes(2))["decision_outcome"]
        assert o["available"] is True and o["cumulative_incremental_pnl"] == 12.5

    def test_45_raw_states_travel_for_audit_only(self):
        raw = _build()["raw_states"]
        assert raw["overall_state"] == "MANUAL_REVIEW_REQUIRED"
        assert raw["canonical_portfolio_decision_state"] == "BLOCKED"
        assert raw["rebalance_state"] == "REBALANCE_NO_PROPOSAL"
        assert raw["collection_service_state"] == "DEGRADED"


# --------------------------------------------------------------------------- #
# 6. The UI — Today, Portfolio, demotions, no dashes, no raw vocabulary.
# --------------------------------------------------------------------------- #
def _region(start: str, end: str) -> str:
    i = UI.find(start)
    assert i != -1, start
    j = UI.find(end, i)
    assert j != -1, end
    return UI[i:j]


TODAY = _region('<div id="tab-overview" class="tab-content active">', "<!-- end tab-overview -->")
TCC = _region('<div id="today-command-center"',
              "<!-- ===================== Phase 14-A COMMAND CENTER START")
R49 = _region("/* R49_REGION_START */", "/* R49_REGION_END */")
R49_CSS = _region('<style id="r49-styles">', "</style>")
R47_BODY = _region("function _r47Render(", "/* R47_REGION_END */")
SYSOPS = _region('<div class="card" id="sysops-panel"', "<!-- One page-level safety strip")


class TestTodayUI:
    def test_50_today_reads_the_presentation_owner_through_one_loader(self):
        assert 'data-presentation-owner="api.operator_presentation"' in TCC
        assert UI.count("function loadOperatorPresentation(") == 1
        assert UI.count("'/v1/operations/operator-presentation'") == 1
        assert "try { loadOperatorPresentation(); } catch (e) {}" in UI

    def test_51_today_has_exactly_four_primary_sections(self):
        # R54 Slice 1 deliberately adds ONE further region after the four R49
        # sections: the Active Manager operating-state strip, owned by a
        # DIFFERENT declared owner (api.active_manager_state). It is admitted
        # here only under that exact owner declaration — any other extra
        # section still fails (mirrors check_release54_active_manager_state).
        ids = re.findall(r'<div id="(today-[\w-]+)"', TCC)
        assert ids == ["today-command-center", "today-system-band", "today-decision",
                       "today-snapshot", "today-attention", "today-operating-state"]
        assert ('id="today-operating-state" data-owner='
                '"api.active_manager_state"') in TCC

    def test_52_no_badge_wall_and_legacy_cards_hidden_on_today(self):
        assert "cc-badge" not in TCC
        assert 'body[data-route="command-center"] #cc-root' in R49_CSS
        assert 'body[data-route="command-center"] #operator-command' in R49_CSS
        # the legacy cards survive as live write targets
        for keep in ('id="cc-dc-card"', 'id="cc-dag-card"', 'id="cc-ob-panel"',
                     'id="operator-command"'):
            assert keep in UI

    def test_53_material_table_moved_to_system_audit(self):
        assert 'id="cc-matinfo-card"' not in TODAY
        assert 'id="cc-matinfo-card"' in SYSOPS
        assert UI.count('id="cc-matinfo-card"') == 1
        assert "function openMaterialInformationDetail(" in R49
        assert "navigateToRoute('system-audit/diagnostics')" in R49

    def test_54_one_primary_cta_through_the_one_dispatcher(self):
        assert R49.count('onclick="opresPrimaryAction(this)"') == 1
        assert R49.count("dispatchCanonicalPrimaryAction(btn)") == 1
        # an executing action off Today is a routing notice, never a second execute
        assert "Open Today to act" in R49
        for forbidden in ("call('POST'", "fetch(", "/execute", "orders/confirm",
                          "rebalance/confirm", "portfolio-decision/record"):
            assert forbidden not in R49, forbidden
        assert not re.search(r"(?<![\w.])alert\s*\(", R49)
        assert not re.search(r"(?<![\w.])confirm\s*\(", R49)

    def test_55_normal_renderer_writes_no_raw_vocabulary(self):
        for tok in ("MANUAL_REVIEW_REQUIRED", "PORTFOLIO_DECISION_NO_PROPOSAL",
                    "REBALANCE_NO_PROPOSAL", "STATE_NOT_RUN", "RUN_DAILY_RESEARCH_CYCLE",
                    "CONFIRM_ALPHA_DAILY_CLOSE",
                    "CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN",
                    "RUN_PORTFOLIO_CYCLE"):
            assert tok not in R49, tok
            assert tok not in R47_BODY, tok

    def test_56_today_renders_the_reconciled_fields_not_raw_states(self):
        for field in ("system_readiness", "portfolio_decision", "portfolio_snapshot",
                      "alerts_summary", "historical_context", "next_action"):
            assert field in R49, field
        assert "_opRenderDecision(p, 'today-decision'" in R49


class TestPortfolioUI:
    def test_60_four_task_views(self):
        assert 'id="pm-views"' in UI
        for v in ("overview", "reallocation", "performance", "audit"):
            assert ('data-pm-view="%s"' % v) in UI
            assert ("navigateToRoute('portfolio-manager/%s')" % v) in UI
        assert 'id="tab-portfolio-manager" class="tab-content" data-pm-view="overview"' in UI
        assert "function pmSetView(" in R49 and "function pmApplyViewFromRoute(" in R49

    def test_61_overview_reads_the_presentation_owner(self):
        assert "_opRenderDecision(p, 'pm-overview-decision'" in R49
        assert 'id="pm-overview-decision"' in UI

    def test_62_model_target_paper_desk_and_corporate_actions_are_under_audit(self):
        ax = UI.find('id="pm-adv-exec"')
        ax_end = UI.find("end pm-adv-exec", ax)
        assert ax < UI.find('id="otr-band"') < ax_end          # model target snapshot
        assert ax < UI.find('id="pd-band"') < ax_end           # paper trading desk
        ad = UI.find('<details class="card" id="pm-advanced"')
        ad_end = UI.find('id="zb-card"', ad)
        for tok in ('id="stage19-ca-card"', 'id="stage19-rebalance-card"',
                    'id="realloc-card"', 'id="hoc-card"'):
            assert ad < UI.find(tok) < ad_end, tok
        for css in ('#tab-portfolio-manager:not([data-pm-view="audit"]) > #pm-adv-exec',
                    '#tab-portfolio-manager:not([data-pm-view="audit"]) > #pm-advanced',
                    '#tab-portfolio-manager:not([data-pm-view="audit"]) > #zb-card',
                    '#tab-portfolio-manager:not([data-pm-view="audit"]) > .card > #reassess-card',
                    '#tab-portfolio-manager:not([data-pm-view="audit"]) > .card > #pm-dag-card',
                    '#tab-portfolio-manager:not([data-pm-view="audit"]) > .card > #pa-decision'):
            assert css in R49_CSS, css

    def test_63_performance_charts_live_under_performance(self):
        assert ('#tab-portfolio-manager:not([data-pm-view="performance"]) > .card > #pdash-perf-charts'
                in R49_CSS)
        assert 'id="pdash-perf-charts"' in UI and "PTC.openDetail('performance')" in UI

    def test_64_model_target_and_best_feasible_are_distinct(self):
        assert "Current vs Best Feasible Target" in UI
        assert "MODEL TARGET SNAPSHOT REVIEW" in UI
        assert "not a portfolio reallocation proposal" in UI

    def test_65_no_dash_grid_for_an_absent_target(self):
        assert "NO CURRENT FEASIBLE TARGET" in R47_BODY
        assert "_r47Row('Positions', cur.position_count)" not in R47_BODY
        assert "No historical target will be fabricated" in R47_BODY

    def test_66_reallocation_view_uses_only_the_owners_replacement_relationship(self):
        assert "replacement_relationship" in R49 and "counterparty" in R49
        for group in ("'EXIT'", "'REDUCE'", "'REPLACE_OUT'", "'ADD'", "'RETAIN'"):
            assert group in R49
        assert "GOVERNANCE" not in R49.split("function _opRenderReallocationView")[0]
        assert "is-current" in R49

    def test_67_audit_reachability_kept_for_the_ux2_hidden_regions(self):
        for css in ('#tab-portfolio-manager[data-pm-view="audit"] > .card > #rout-card',
                    '#tab-portfolio-manager[data-pm-view="audit"] > .card > #reassess-audit',
                    '#tab-portfolio-manager[data-pm-view="audit"] > .card > #pm-sec-evidence'):
            assert css in R49_CSS, css
        assert 'id="pm-raw-states"' in UI

    def test_68_no_r49_dashboard_was_added(self):
        assert [m for m in re.findall(r'id="(r49-[\w-]+)"', UI) if m != "r49-styles"] == []


# --------------------------------------------------------------------------- #
# 7. Preservation + the strict audit.
# --------------------------------------------------------------------------- #
class TestPreservationAndAudit:
    def test_70_r48_cycle_and_dispatcher_preserved(self):
        assert pc.EXECUTE_CONFIRMATION == "RUN_PORTFOLIO_CYCLE"
        assert UI.count("function runPortfolioCycle(") == 1
        assert UI.count("'/v1/operations/portfolio-cycle/run'") == 1
        fn = UI.split("function dispatchCanonicalPrimaryAction(")[1].split("\nwindow.")[0]
        assert "_wsIsTodayRoute()" in fn and "runPortfolioCycle(btn)" in fn

    def test_71_r47_owner_and_gates_preserved(self):
        from paper_trader.engine import constrained_reallocation as cr
        from paper_trader.api import portfolio_decision as pdm
        from paper_trader.api import rebalance_execution as rex
        assert cr.INCUMBENCY_POLICY == "NO_INVESTMENT_PRIVILEGE_ONLY_PRICED_TRANSITION_COST"
        assert pdm.CONFIRM_TOKEN == "CONFIRM_PORTFOLIO_REBALANCE_DECISION"
        assert "CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN" in rex.CONFIRM_TOKEN
        assert UI.count("function _r47Load(") == 1

    def test_72_audit_guard_is_green(self):
        aud = importlib.import_module("scripts.audit_architecture")
        rep = aud.check_release49_operator_presentation(aud._iter_source_files())
        expected = {f: v for k, f, v in aud.BLOCKING_INVARIANTS
                    if k == "release49_operator_presentation"}
        assert len(expected) >= 40
        for field, must_be in expected.items():
            assert rep[field] == must_be, (field, rep[field])

    def test_73_r46_research_is_unreachable(self):
        code = _code_only(SRC)
        for tok in ("r46", "alpha_agent", "prospective_tournament", "research_trades"):
            assert tok not in code, tok
