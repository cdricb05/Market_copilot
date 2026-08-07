"""Phase 29G.2 — Slice 6 RESIDUAL hard cutover & FIRST-LIVE operator gates.

Deterministic and hermetic. No DB / provider / prediction / operational ledger / real
Daily Research Cycle / real Daily Close / real Holding Opportunity-Cost assessment is
touched. The backend contracts are exercised at the function level with injected inputs;
the UI and audit are asserted structurally; the two read-only operator gate scripts are
parsed and driven against an in-process MOCK HTTP server on an EPHEMERAL port (never the
live backend on 8001) through a real ``powershell.exe`` subprocess (skipped cleanly when
PowerShell is unavailable). No script issues a write request; no real artifact is created.

Covers the 39-case matrix: legacy PROPOSAL_READY / REBALANCE_PROPOSAL_READY normalized to
compatibility-only; no primary "Portfolio Changes Proposed" / "Proposal Ready" label; no
"Review Proposed Changes" / "Review Rebalance Proposal" action; the legacy comparison
collapsed and read-only; the HOC NOT_RUN + completed primary card on all three surfaces;
one HOC loader; no JS recommendation math; the workflow action sequence (before close ->
run DRC -> review HOC -> run Daily Close); no reassessment/rebalance/order route; the
pre/post-DRC operator scripts (parser, no-POST, success + blocked/mismatch fixtures); no
real artifact / operational mutation; the strict architecture guard; inventory drift zero;
and Slice 7 / the Persistent Alpha Research Agent (Slice 8) remaining future.
"""
from __future__ import annotations

import copy
import importlib.util
import json
import shutil
import subprocess
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from paper_trader.api import daily_action_gate as dag
from paper_trader.api import daily_close as dc
from paper_trader.api import workflow_state as ws

ET = ZoneInfo("America/New_York")
REPO = Path(__file__).resolve().parent.parent
UI = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8")
WS_SRC = (REPO / "api" / "workflow_state.py").read_text(encoding="utf-8")
GATE_SRC = (REPO / "api" / "daily_action_gate.py").read_text(encoding="utf-8")

SCRIPT_DIR = Path(r"D:\Temp\paper_trader_slice6_first_live_acceptance")
PRE_SCRIPT = SCRIPT_DIR / "pre_drc_readiness.ps1"
POST_SCRIPT = SCRIPT_DIR / "post_drc_acceptance.ps1"
POWERSHELL = shutil.which("powershell") or shutil.which("pwsh")


def _load_audit():
    p = REPO / "scripts" / "audit_architecture.py"
    spec = importlib.util.spec_from_file_location("audit_architecture_rc6", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# =========================================================================== #
# 1–2  Legacy PROPOSAL_READY / REBALANCE_PROPOSAL_READY normalized compat-only.
# =========================================================================== #
def test_01_legacy_proposal_ready_normalized_compatibility_only():
    # A stored gate PROPOSAL_READY outcome is normalized to a compatibility-only legacy
    # presentation in the read contract (never a primary decision).
    ap = ws.build_assessment_presentation(
        assessment_status=ws.ASSESS_DUE, assessment_date="2026-08-05",
        outcome="PROPOSAL_READY", recommendation=None, current_for_eligible=False,
        membership_add_count=2, membership_remove_count=1)
    assert ap["title"] == ws.LEGACY_COMPARISON_TITLE
    assert ap["compatibility_only"] is True
    assert ap["decision_authority"] == "NONE"
    assert ap["is_portfolio_proposal"] is False
    assert "Portfolio changes were proposed" not in ap["headline"]


def test_02_legacy_rebalance_proposal_ready_normalized_compatibility_only():
    # daily_close reclassifies the legacy REBALANCE_PROPOSAL_READY state, and the
    # workflow owner normalizes it to compatibility-only.
    pres = dc._PRESENTATION[dc.REBALANCE_PROPOSAL_READY]
    assert "LEGACY MEMBERSHIP-COMPARISON" in pres["label"]
    ap = ws.build_assessment_presentation(
        assessment_status=ws.ASSESS_STALE, assessment_date="2026-08-05",
        outcome="REBALANCE_PROPOSAL_READY", recommendation=None,
        current_for_eligible=False)
    assert ap["compatibility_only"] is True
    assert ap["is_portfolio_proposal"] is False
    assert ap["title"] == ws.LEGACY_COMPARISON_TITLE


# =========================================================================== #
# 3–6  No primary proposal label / action anywhere in the primary surfaces.
# =========================================================================== #
def test_03_no_primary_portfolio_changes_proposed_label():
    assert "PORTFOLIO CHANGES PROPOSED" not in UI
    assert "LATEST PORTFOLIO ASSESSMENT" not in WS_SRC


def test_04_no_primary_proposal_ready_label():
    assert "PROPOSAL READY" not in UI            # never rendered as a primary chip
    # The raw gate vocabulary is PRESERVED for historical consumers (compatibility).
    assert "PROPOSAL READY — MANUAL REVIEW REQUIRED" in GATE_SRC


def test_05_no_review_proposed_changes_action():
    assert "Review Proposed Changes" not in UI


def test_06_no_review_rebalance_proposal_action():
    assert "Review Rebalance Proposal" not in UI


# =========================================================================== #
# 7–8  Legacy comparison collapsed + read-only compatibility classification.
# =========================================================================== #
def test_07_legacy_comparison_collapsed_all_surfaces():
    for p in ("cc", "dw", "pm"):
        assert ('id="%s-dag-legacy"' % p) in UI
    assert "LEGACY MEMBERSHIP-COMPARISON SUMMARY" in UI
    assert "View Legacy Membership Comparison" in UI


def test_08_legacy_comparison_is_read_only_not_a_proposal():
    block = dag.build_legacy_membership_comparison_presentation(
        proposed_additions=[{"ticker": "AAA"}], proposed_removals=[{"ticker": "BBB"}],
        proposed_resizes=[], historical_date="2026-08-05", outcome="PROPOSAL_READY")
    assert block["read_only"] is True
    assert block["is_portfolio_proposal"] is False
    assert block["creates_orders"] is False
    assert block["decision_authority"] == "NONE"
    assert block["canonical_decision_owner"] == "api.holding_opportunity_cost"
    assert "Read-only compatibility view" in UI


# =========================================================================== #
# 9–10  HOC NOT_RUN + completed primary card.
# =========================================================================== #
def test_09_hoc_not_run_primary_card():
    p = ws.build_holding_opportunity_cost_presentation(
        state="NOT_RUN", available=False, eligible_date="2026-08-06")
    assert p["title"] == "HOLDING OPPORTUNITY-COST REVIEW"
    assert p["badge"] == "NOT_RUN"
    assert p["recommendations_label"] == "NONE YET"
    assert p["canonical_operator_state"] == ws.COS_NOT_RUN
    assert p["is_primary_decision"] is True
    assert p["recommendation_counts"] == {}     # no fabricated counts
    # UI renders the NOT_RUN state.
    assert "hasAssessment" in UI and "NONE YET" in UI


def test_10_hoc_completed_primary_card():
    p = ws.build_holding_opportunity_cost_presentation(
        state="READY", available=True, eligible_date="2026-08-06",
        recommendation_counts={"HOLD": 20, "REDUCE": 2, "EXIT": 1, "REPLACE": 1, "ADD": 1},
        assessment_hash="ABC123", data_gaps=["risk_short"])
    assert p["badge"] == "AVAILABLE"
    assert p["canonical_operator_state"] == ws.COS_AVAILABLE
    assert "20 HOLD / 2 REDUCE / 1 EXIT / 1 REPLACE / 1 ADD" in p["headline"]
    assert p["assessment_hash"] == "ABC123"
    assert p["data_gaps"] == ["risk_short"]


# =========================================================================== #
# 11–13  Every surface uses the HOC state (structural).
# =========================================================================== #
def test_11_command_center_uses_hoc_state():
    assert 'id="cc-dag-title"' in UI and 'id="cc-dag-badge"' in UI
    assert UI.count("PRIMARY PORTFOLIO DECISION") >= 3


def test_12_daily_workflow_uses_hoc_state():
    assert 'id="dw-dag-title"' in UI and 'id="dw-dag-badge"' in UI


def test_13_portfolio_manager_uses_hoc_state():
    assert 'id="pm-dag-title"' in UI and 'id="pm-dag-badge"' in UI
    assert 'id="hoc-card"' in UI                 # the detailed HOC table


# =========================================================================== #
# 14–15  One canonical HOC loader; no JS recommendation calculation.
# =========================================================================== #
def test_14_one_canonical_hoc_loader():
    assert UI.count("function loadHoldingOpportunityCost") == 1
    assert "window._hocInFlight" in UI


def test_15_no_js_recommendation_calculation():
    audit = _load_audit()
    rc = audit.check_slice6_residual_cutover_ownership(audit._iter_source_files())
    assert rc["ui_recommendation_or_cost_computation"] == []


# =========================================================================== #
# 16–19  Workflow action sequence: before close -> DRC -> review HOC -> close.
# =========================================================================== #
def _primary(overall):
    return ws._primary_action(overall, {"eligible_date": "2026-08-06",
                                         "session_operator_action": ""})


def test_16_workflow_before_close():
    a = _primary(ws.WAITING_FOR_SESSION_CLOSE)
    assert a["action_code"] == ws.ACTION_WAIT_FOR_SESSION_CLOSE
    assert a["label"] == "Wait for the market session to close"
    assert a["execution_available"] is False


def test_17_workflow_ready_for_daily_research_cycle():
    a = _primary(ws.RESEARCH_CYCLE_REQUIRED)
    assert a["action_code"] == ws.ACTION_RUN_RESEARCH_CYCLE
    assert a["label"] == "Run the Daily Research Cycle"
    assert a["confirmation_required"] == "RUN_DAILY_RESEARCH_CYCLE"


def test_18_workflow_after_completed_hoc_offers_review():
    q = ws._queued_actions(
        primary_code="OTHER", research_current=True, assessment_status=ws.ASSESS_CURRENT,
        eligible_session_closed=False, has_confirmed_eligible=True, evidence_gap=False,
        manual_review_required=False, pending_orders=0, hoc_available=True)
    codes = {x["action_code"]: x for x in q}
    assert ws.ACTION_REVIEW_HOC in codes
    rev = codes[ws.ACTION_REVIEW_HOC]
    assert "Holding Opportunity-Cost" in rev["label"]
    assert rev["execution_available"] is False   # review/routing only, never execution


def test_19_workflow_ready_for_daily_close():
    a = _primary(ws.READY_FOR_DAILY_CLOSE)
    assert a["action_code"] == ws.ACTION_RUN_DAILY_CLOSE
    assert a["label"] == "Run the Daily Close"


# =========================================================================== #
# 20–22  No reassessment / rebalance / order POST route introduced.
# =========================================================================== #
def test_20_21_22_no_reassessment_rebalance_or_order_route():
    audit = _load_audit()
    routes = audit.check_routes()["routes"]
    paths = {r["path"] for r in routes}
    for forbidden in ("/v1/operations/portfolio-reassessment", "/v1/operations/reassessment",
                      "/v1/operations/rebalance", "/v1/operations/rebalance-proposal",
                      "/v1/operations/holding-opportunity-cost/run",
                      "/v1/operations/confirm-target", "/v1/operations/target-confirmation"):
        assert forbidden not in paths
    hoc_methods = sorted({r["method"] for r in routes
                          if r["path"] == "/v1/operations/holding-opportunity-cost"})
    assert hoc_methods == ["GET"]


# =========================================================================== #
# 23–33  Operator gate scripts (parser, no-POST, fixtures via mock server).
# =========================================================================== #
def _require_script(path: Path):
    # The first-live operator scripts live under D:\Temp on the release host only; when
    # absent (e.g. a different machine / CI) these tests skip cleanly rather than fail.
    if not path.exists():
        pytest.skip("first-live operator script not present: %s" % path)


def _ps_parse_errors(path: Path):
    _require_script(path)
    if not POWERSHELL:
        pytest.skip("PowerShell unavailable")
    probe = (
        "$t=$null;$e=$null;"
        "[void][System.Management.Automation.Language.Parser]::ParseFile("
        "'%s',[ref]$t,[ref]$e);"
        "if($e -and $e.Count){$e|ForEach-Object{$_.Message}}else{'PARSE_OK'}"
        % str(path).replace("\\", "\\\\"))
    r = subprocess.run([POWERSHELL, "-NoProfile", "-NonInteractive", "-Command", probe],
                       capture_output=True, text=True, timeout=60)
    return (r.stdout or "").strip()


def _no_post_request(path: Path) -> bool:
    _require_script(path)
    txt = path.read_text(encoding="utf-8")
    low = txt.lower()
    # No HTTP write verbs and no .NET HTTP client construction.
    for bad in ("-method post", "-method 'post'", "-method \"post\"",
                "invoke-webrequest", ".post(", "httpclient", "httpclienthandler"):
        if bad in low:
            return False
    return "invoke-restmethod -method get" in low


def test_23_pre_drc_script_parses():
    assert _ps_parse_errors(PRE_SCRIPT) == "PARSE_OK"


def test_24_pre_drc_script_contains_no_post():
    assert _no_post_request(PRE_SCRIPT)


def test_27_post_drc_script_parses():
    assert _ps_parse_errors(POST_SCRIPT) == "PARSE_OK"


def test_28_post_drc_script_contains_no_post():
    assert _no_post_request(POST_SCRIPT)


# --- Mock HTTP server (ephemeral port; NEVER 8001) driving the real scripts. -------- #
def _base_scenario():
    """A fully READY scenario: service ready, session closed, DRC executable, HOC NOT_RUN,
    25 holdings, no pending orders."""
    return {
        "/v1/health": {"status": "ok", "service": "paper_trader", "version": "test"},
        "/v1/ready": {"status": "ok", "database": "ok", "ready": True,
                      "readiness_kind": "service", "reason": None},
        "/v1/operations/workflow-state": {
            "overall_state": "RESEARCH_CYCLE_REQUIRED",
            "research_cycle_state": {"executable": True, "eligible_market_date": "2026-08-06",
                                     "cycle_running": False},
            "operational_state": {"pending_orders": 0, "holdings_count": 25,
                                  "active_book_id": "alpha_paper_book_1"},
            "current_session": {"latest_eligible_completed_market_date": "2026-08-06"},
            "safety": {"created_orders": False, "created_fills": False, "read_only": True}},
        "/v1/operations/portfolio-state": {
            "active_book": {"book_id": "alpha_paper_book_1", "holdings_count": 25}},
        "/v1/operations/daily-research-cycle/status": {
            "executable": True, "eligible_market_date": "2026-08-06", "state": "PLANNING",
            "completed_steps": []},
        "/v1/operations/holding-opportunity-cost": {
            "state": "NOT_RUN", "artifact": None, "eligible_market_date": "2026-08-06",
            "active_book": {"book_id": "alpha_paper_book_1"}, "assessment_hash": None,
            "recommendation_counts": {}, "data_quality": {}},
    }


def _post_ready_scenario():
    """A fully READY post-DRC scenario: DRC complete, HOC artifact present, ready-for-close."""
    s = _base_scenario()
    s["/v1/operations/daily-research-cycle/status"] = {
        "state": "COMPLETE", "eligible_market_date": "2026-08-06",
        "completed_steps": ["SCORING_UNIVERSE", "ASSESS_HOLDING_OPPORTUNITY_COST"]}
    s["/v1/operations/holding-opportunity-cost"] = {
        "state": "READY", "artifact": {"artifact_id": "hoc_x"},
        "eligible_market_date": "2026-08-06",
        "active_book": {"book_id": "alpha_paper_book_1"}, "assessment_hash": "HASH123",
        "recommendation_counts": {"HOLD": 22, "REDUCE": 1, "EXIT": 1, "REPLACE": 1, "ADD": 1},
        "data_quality": {"holdings_evaluated": 25, "data_gaps": []}}
    s["/v1/operations/workflow-state"]["overall_state"] = "READY_FOR_DAILY_CLOSE"
    return s


class _MockHandler(BaseHTTPRequestHandler):
    scenario: dict = {}

    def log_message(self, *a):  # silence
        pass

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        body = type(self).scenario.get(path)
        if body is None:
            self.send_response(404)
            self.end_headers()
            return
        blob = json.dumps(body).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(blob)))
        self.end_headers()
        self.wfile.write(blob)


def _run_script_against(scenario: dict, script: Path):
    _require_script(script)
    if not POWERSHELL:
        pytest.skip("PowerShell unavailable")
    handler = type("H", (_MockHandler,), {"scenario": scenario})
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = srv.server_address[1]
    assert port != 8001                      # never the live backend
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    try:
        r = subprocess.run(
            [POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
             "-File", str(script), "-BaseUrl", "http://127.0.0.1:%d" % port,
             "-ApiKey", "test-key"],
            capture_output=True, text=True, timeout=90)
    finally:
        srv.shutdown()
        srv.server_close()
    return r.returncode, (r.stdout or ""), (r.stderr or "")


def test_25_pre_drc_success_fixture():
    code, out, err = _run_script_against(_base_scenario(), PRE_SCRIPT)
    assert out.strip().splitlines()[-1] == "READY_TO_RUN_DAILY_RESEARCH_CYCLE", (out, err)
    assert code == 0
    assert "2026-08-06" in out                # reports the eligible date


def test_26_pre_drc_blocked_when_session_open():
    s = _base_scenario()
    s["/v1/operations/workflow-state"]["overall_state"] = "WAITING_FOR_SESSION_CLOSE"
    code, out, err = _run_script_against(s, PRE_SCRIPT)
    last = out.strip().splitlines()[-1]
    assert last.startswith("DO_NOT_RUN_DAILY_RESEARCH_CYCLE -"), (out, err)
    assert "WAITING_FOR_SESSION_CLOSE" in last
    assert code == 1


def test_26b_pre_drc_blocked_when_pending_orders():
    s = _base_scenario()
    s["/v1/operations/workflow-state"]["operational_state"]["pending_orders"] = 3
    code, out, _ = _run_script_against(s, PRE_SCRIPT)
    last = out.strip().splitlines()[-1]
    assert last.startswith("DO_NOT_RUN_DAILY_RESEARCH_CYCLE -")
    assert "pending" in last.lower()


def test_29_post_drc_success_fixture():
    code, out, err = _run_script_against(_post_ready_scenario(), POST_SCRIPT)
    assert out.strip().splitlines()[-1] == "READY_TO_RUN_DAILY_CLOSE", (out, err)
    assert code == 0
    assert "HOLD 22" in out                   # concise recommendation summary
    assert "HASH123" in out                   # assessment hash


def test_30_post_drc_artifact_date_mismatch_rejected():
    s = _post_ready_scenario()
    s["/v1/operations/holding-opportunity-cost"]["eligible_market_date"] = "2026-08-05"
    code, out, _ = _run_script_against(s, POST_SCRIPT)
    last = out.strip().splitlines()[-1]
    assert last.startswith("DO_NOT_RUN_DAILY_CLOSE -")
    assert "eligible date" in last.lower()


def test_31_post_drc_active_book_mismatch_rejected():
    s = _post_ready_scenario()
    s["/v1/operations/holding-opportunity-cost"]["active_book"]["book_id"] = "other_book"
    code, out, _ = _run_script_against(s, POST_SCRIPT)
    last = out.strip().splitlines()[-1]
    assert last.startswith("DO_NOT_RUN_DAILY_CLOSE -")
    assert "active-book" in last.lower()


def test_32_post_drc_holdings_count_mismatch_rejected():
    s = _post_ready_scenario()
    s["/v1/operations/holding-opportunity-cost"]["data_quality"]["holdings_evaluated"] = 24
    code, out, _ = _run_script_against(s, POST_SCRIPT)
    last = out.strip().splitlines()[-1]
    assert last.startswith("DO_NOT_RUN_DAILY_CLOSE -")
    assert "holdings evaluated" in last.lower()


def test_33_post_drc_pending_order_rejected():
    s = _post_ready_scenario()
    s["/v1/operations/workflow-state"]["operational_state"]["pending_orders"] = 2
    code, out, _ = _run_script_against(s, POST_SCRIPT)
    last = out.strip().splitlines()[-1]
    assert last.startswith("DO_NOT_RUN_DAILY_CLOSE -")
    assert "pending" in last.lower()


# =========================================================================== #
# 34–35  No real artifact creation; no operational mutation.
# =========================================================================== #
def test_34_no_real_hoc_artifact_created(tmp_path):
    from paper_trader.api import holding_opportunity_cost as hoc
    ps = {"active_book": {"book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1",
                          "holdings_count": 25, "initialized": True},
          "dates": {"eligible_market_date": "2026-08-06"}, "capital": {}, "positions": []}
    hoc.load_holding_opportunity_cost(portfolio_state=ps, hoc_dir=str(tmp_path))
    assert list(tmp_path.iterdir()) == []     # a read never writes an artifact


def test_35_workflow_state_declares_no_mutation():
    now = datetime(2026, 8, 6, 12, 0, tzinfo=ET)
    r = ws.load_workflow_state(now=now, operational={}, inputs={}, daily_status={},
                               desk_marks={}, close_progress=None, forward_status={},
                               gate={}, target_readiness={})
    s = r["safety"]
    for flag in ("created_orders", "created_fills", "created_proposals",
                 "wrote_to_ledger", "wrote_to_database", "ran_daily_close",
                 "ran_portfolio_reassessment", "created_trade_decisions"):
        assert s[flag] is False, flag
    assert s["read_only"] is True


# =========================================================================== #
# 36–39  Strict audit; inventory drift zero; Slice 7 / Slice 8 future.
# =========================================================================== #
def test_36_strict_residual_cutover_audit_green():
    audit = _load_audit()
    rc = audit.check_slice6_residual_cutover_ownership(audit._iter_source_files())
    assert rc["forbidden_primary_ui"] == []
    assert rc["forbidden_primary_ws"] == []
    assert rc["gate_compatibility_classified"] is True
    assert rc["workflow_primary_is_hoc"] is True
    assert rc["missing_ws_primary_tokens"] == []
    assert rc["legacy_comparison_collapsed"] is True
    assert rc["hoc_is_sole_primary_card"] is True
    assert rc["surfaces_use_hoc_state"] is True
    assert rc["ui_hoc_loader_count"] == 1
    assert rc["ui_recommendation_or_cost_computation"] == []
    assert rc["drc_sole_execution_path"] is True
    assert rc["no_manual_hoc_execution_endpoint"] is True
    assert rc["forbidden_routes_present"] == []
    assert rc["cadence_enabled"] is False


def test_37_inventory_drift_zero():
    audit = _load_audit()
    d = audit.check_inventory_drift(audit._iter_source_files())
    assert d["status"] == "OK"
    assert d["on_disk_not_in_inventory"] == []
    assert d["in_inventory_not_on_disk"] == []


def test_38_slice7_landed():
    audit = _load_audit()
    rc = audit.check_slice6_residual_cutover_ownership(audit._iter_source_files())
    assert rc["slice7_missing"] == []


def test_39_persistent_alpha_research_agent_remains_planned():
    audit = _load_audit()
    rc = audit.check_slice6_residual_cutover_ownership(audit._iter_source_files())
    assert rc["slice8_present"] == []          # Persistent Alpha Research Agent (Slice 8)
