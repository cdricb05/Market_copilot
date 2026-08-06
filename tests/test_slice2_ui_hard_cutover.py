"""tests/test_slice2_ui_hard_cutover.py — Phase 29C.1 Slice 2 UI hard cutover.

Deterministic tests that the canonical ``api.workflow_state`` owns EVERY visible
primary operator interpretation after the UI hard cutover:

  * BACKEND PRESENTATION — an older no-change assessment is a DATED historical
    result with its canonical currency; "today" wording is forbidden unless the
    assessment is current; the still-open current session is separated from the
    latest completed close's documented (attention) forward-evidence gap.
  * DOM OWNERSHIP — the canonical primary nodes have ONE declared owner
    (renderWorkflowState); the specialized detail renderers cannot write them; the
    shared setters hard-guard them; detail metrics stay populated.
  * ASYNC ORDER — an in-process DOM harness runs the ACTUAL render functions in
    every completion order and proves the final visible state is identical (the
    canonical value wins by OWNERSHIP, not timing).
  * VISIBLE CONTRACT — the observed regression scenario shows WAITING FOR SESSION
    CLOSE / LATEST PORTFOLIO ASSESSMENT / OVERDUE / a dated historical statement /
    a required reassessment, and NEVER "DAILY ACTION GATE — TODAY" / "NO ACTION
    TODAY" as a current conclusion.
  * API / SAFETY / ARCHITECTURE — the endpoint stays authenticated, GET-only and
    read-only; the raw gate keeps its NO_ACTION_TODAY code; the audit guards hold.

Every clock/date is injected; no network, provider, prediction, DB or write. The
async-order tests execute the real UI functions in Node with a minimal DOM shim
(no jsdom / npm install) and skip cleanly when ``node`` is unavailable.
"""
from __future__ import annotations

import copy
import importlib.util
import json
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from paper_trader.api import workflow_state as ws

ET = ZoneInfo("America/New_York")
REPO = Path(__file__).resolve().parent.parent
UI = REPO / "api" / "ui" / "index.html"
HARNESS = REPO / "scripts" / "ui_render_harness.js"
APP = REPO / "api" / "app.py"
NODE = shutil.which("node")


def _load_audit():
    """Load scripts/audit_architecture.py by path (scripts is not a package)."""
    p = REPO / "scripts" / "audit_architecture.py"
    spec = importlib.util.spec_from_file_location("audit_architecture", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

# --------------------------------------------------------------------------- #
# The observed live regression fixture (Workstream I): current session open, the
# latest eligible completed session (prior day) fully processed, research stale,
# the latest assessment older (no change) and OVERDUE, a documented evidence gap.
# No live date is hardcoded in production code; the fixture injects everything.
# --------------------------------------------------------------------------- #
_OP = {"operational_book": {
    "book_id": "alpha_paper_book_1", "current_status": "FORWARD_TRACKING_ACTIVE",
    "initialized": True, "nav_as_of_date": "2026-08-04", "desk_mark_date": "2026-08-04",
    "latest_desk_mark_date": "2026-08-04", "nav": 102241.79, "cash": 1500.0,
    "holdings_count": 25, "pending_order_count": 0,
    "current_target": {"alpha_market_date": "2026-07-31",
                       "latest_completed_market_date": "2026-08-04"}}}
_INPUTS = {"market_as_of_date": "2026-07-31", "momentum_month": "2026-07",
           "fundamental_as_of_date": "2026-05-22"}
_DAILY = {"status": "NO_DAILY_REFRESH_YET", "latest_valid_mark_date": "2026-07-20"}
_DESK = {"series": {"SPY": [["2026-07-31", 747.03], ["2026-08-04", 771.33]]},
         "latest_completed_date": "2026-08-04"}
_CLOSE = {"market_date": "2026-08-04", "done": True,
          "final_close_status": "DAILY_CLOSE_COMPLETE_HOLD", "status": "CLOSE_FINISHED"}
_FWD = {"latest_snapshot_date": "2026-07-31", "snapshot_count": 5,
        "evidence_state": "FORWARD_EVIDENCE_PARTIAL", "interpretation": "partial",
        "active_book": {"model_id": "m", "book_id": "x"}, "shadow_books": []}
_GATE = {"latest_completed_market_date": "2026-07-31", "outcome": "NO_ACTION_TODAY",
         "outcome_label": "NO ACTION TODAY",
         "headline": "NO PORTFOLIO CHANGE REQUIRED FROM THE LATEST COMPLETED CLOSE",
         "explanation": "The actual holdings match the model target.",
         "target_state": "CURRENT_ALIGNED", "target_state_label": "CURRENT ALIGNED",
         "action_required": False, "action_severity": "green",
         "next_scheduled_full_review": "2026-08-01", "scheduled_review_due": True,
         "review_cadence": "MONTHLY", "target_actual_match": True,
         "proposed_change_count": 0, "evaluation_date": "2026-08-05",
         "estimated_turnover": 0.0, "estimated_cost": 0.0,
         "primary_action_label": "Monitor Holdings and Performance"}
_TR = {"dates": {"alpha_market_date": "2026-07-31"}}
_NOW = datetime(2026, 8, 5, 10, 0, tzinfo=ET)


def _regression(**kw):
    args = dict(now=_NOW, operational=copy.deepcopy(_OP), inputs=dict(_INPUTS),
                daily_status=dict(_DAILY), desk_marks=copy.deepcopy(_DESK),
                close_progress=dict(_CLOSE), forward_status=copy.deepcopy(_FWD),
                gate=copy.deepcopy(_GATE), target_readiness=copy.deepcopy(_TR))
    args.update(kw)
    return ws.load_workflow_state(**args)


def _ui():
    return UI.read_text(encoding="utf-8")


# =========================================================================== #
# BACKEND PRESENTATION (required 1–8)
# =========================================================================== #
def test_01_historical_no_change_result_preserved_and_dated():
    ap = _regression()["assessment_presentation"]
    assert ap["historical_result"] == "No portfolio change was recommended."
    assert ap["assessment_date"] == "2026-07-31"
    assert ap["title_with_date"] == "LATEST PORTFOLIO ASSESSMENT — 2026-07-31"
    assert "2026-07-31" in ap["headline"]
    assert ap["headline"] == "No portfolio change was recommended on 2026-07-31."


def test_02_overdue_assessment_gets_overdue_presentation():
    ap = _regression()["assessment_presentation"]
    assert ap["currency_status"] == ws.ASSESS_OVERDUE
    assert ap["currency_label"] == "Overdue"
    assert ap["badge"] == "OVERDUE"
    assert ap["severity"] == ws.SEV_ATTENTION
    assert ap["follow_up"] == "A new portfolio reassessment is required."


def test_03_stale_assessment_gets_stale_presentation():
    ap = ws.build_assessment_presentation(
        assessment_status=ws.ASSESS_STALE, assessment_date="2026-07-31",
        outcome="NO_ACTION_TODAY", recommendation=None, current_for_eligible=False)
    assert ap["currency_status"] == "STALE" and ap["currency_label"] == "Stale"
    assert ap["today_wording_allowed"] is False
    assert ap["explanation"] == "A new portfolio reassessment is required."


def test_04_current_assessment_may_be_presented_as_current():
    ap = ws.build_assessment_presentation(
        assessment_status=ws.ASSESS_CURRENT, assessment_date="2026-08-04",
        outcome="NO_ACTION_TODAY", recommendation=None, current_for_eligible=True)
    assert ap["may_be_called_current"] is True
    assert ap["today_wording_allowed"] is True
    assert ap["severity"] == ws.SEV_SUCCESS
    assert ap["follow_up"] is None


def test_05_today_wording_forbidden_when_not_current():
    for status in (ws.ASSESS_STALE, ws.ASSESS_DUE, ws.ASSESS_OVERDUE, ws.ASSESS_MISSING):
        ap = ws.build_assessment_presentation(
            assessment_status=status, assessment_date="2026-07-31",
            outcome="NO_ACTION_TODAY", recommendation=None, current_for_eligible=False)
        assert ap["today_wording_allowed"] is False, status
        assert "today" not in ap["headline"].lower(), status


def test_06_evidence_separates_current_session_from_completed_close():
    ep = _regression()["evidence_presentation"]
    assert ep["current_session"]["state"] == "NO_RESULT_YET"
    assert "still-open" in ep["current_session"]["explanation"]
    assert ep["latest_completed_close"]["state"] == "VALID_WITH_DOCUMENTED_GAP"
    assert "2026-08-04" in ep["latest_completed_close"]["explanation"]
    # The two are distinct concepts, never merged.
    assert ep["current_session"]["explanation"] != ep["latest_completed_close"]["explanation"]


def test_07_documented_evidence_gap_remains_attention():
    ep = _regression()["evidence_presentation"]
    assert ep["documented_gap"] is True
    assert ep["severity"] == ws.SEV_ATTENTION           # amber, never ERROR/red
    assert ep["severity"] != ws.SEV_ERROR


def test_08_valid_operational_close_remains_valid():
    r = _regression()
    assert r["operational_state"]["operational_close_valid"] is True
    assert r["completed_summary"]["latest_completed_close"]["valid"] is True
    # A documented gap never downgrades a valid completed close (D-7).
    assert "rerun" not in json.dumps(r).lower()


# =========================================================================== #
# DOM OWNERSHIP (required 9–15) — semantic source checks (audit-backed).
# =========================================================================== #
def _wf_report():
    return _load_audit().check_workflow_state_ownership([])


def test_09_canonical_nodes_have_one_declared_owner():
    html = _ui()
    assert "window.WS_CANONICAL_NODES" in html
    for helper in ("function _wsOwnSet", "function _wsOwnHtml",
                   "function _wsGuardedSet", "function _wsIsCanonicalNode"):
        assert helper in html, helper
    assert _wf_report()["ui_canonical_ownership_declared"] is True


def test_10_render_daily_action_gate_cannot_overwrite_canonical():
    rep = _wf_report()
    bad = [w for w in rep["ui_unauthorized_canonical_writers"]
           if w.startswith("renderDailyActionGate")]
    assert bad == [], bad


def test_11_render_daily_close_cannot_overwrite_canonical():
    rep = _wf_report()
    bad = [w for w in rep["ui_unauthorized_canonical_writers"]
           if w.startswith("renderDailyClose")]
    assert bad == [], bad


def test_12_render_operational_book_cannot_overwrite_canonical():
    rep = _wf_report()
    bad = [w for w in rep["ui_unauthorized_canonical_writers"]
           if w.startswith("renderOperationalBook")]
    assert bad == [], bad


def test_13_forward_evidence_and_legacy_panels_do_not_own_canonical():
    # The shared specialized setters hard-guard every canonical node, so a stray
    # _dcSet/_dagSet/_obSet from any legacy panel cannot overwrite the canonical value.
    rep = _wf_report()
    assert rep["ui_unguarded_setters"] == []
    assert set(rep["ui_shared_setters_guarded"]) == {"_dagSet", "_dcSet", "_obSet"}


def test_14_legacy_action_panel_functions_neutralised():
    html = _ui()
    # applyCanonicalToActionPanel is a deliberate no-op (owns nothing).
    start = html.find("function applyCanonicalToActionPanel()")
    end = html.find("}", start)
    body = html[start:end]
    assert "getElementById('right-current-task')" not in body
    assert "getElementById('right-primary-action-btn')" not in body


def test_15_detail_metrics_remain_populated():
    # The Daily Action Gate loader still renders its dated DETAIL nodes (the nodes
    # exist and the loader still writes them via the '<prefix> + "-dag-X"' calls).
    html = _ui()
    for node in ("cc-dag-market", "cc-dag-review", "pm-dag-eval",
                 "pm-dag-turnover", "pm-dag-cost"):
        assert ('id="%s"' % node) in html, node
    for suffix in ("-dag-market'", "-dag-review'", "-dag-eval'",
                   "-dag-turnover'", "-dag-cost'"):
        assert suffix in html, suffix   # still written by the DAG detail loader


# =========================================================================== #
# ASYNC ORDER (required 16–24) — the Node DOM harness runs the REAL functions.
# =========================================================================== #
def _run_harness():
    if not NODE:
        pytest.skip("node not available for the DOM render harness")
    if not HARNESS.exists():
        pytest.skip("ui_render_harness.js missing")
    r = _regression()
    with tempfile.TemporaryDirectory() as td:
        payload = Path(td) / "payload.json"
        payload.write_text(json.dumps({"ws": r, "gate": _GATE}), encoding="utf-8")
        proc = subprocess.run([NODE, str(HARNESS), str(UI), str(payload)],
                              capture_output=True, text=True, encoding="utf-8")
        assert proc.returncode == 0, (proc.stderr or "")[:3000]
        return json.loads(proc.stdout)


@pytest.fixture(scope="module")
def harness():
    return _run_harness()


def test_16_to_24_all_completion_orders_produce_the_same_canonical_state(harness):
    # Six orderings — workflow↔gate, guarded-fallback→workflow, and post-render
    # "attacks" from the specialized setters (standing in for Daily Close /
    # Operational Book / Forward Evidence completing after workflow-state) — all
    # converge to the identical canonical visible state. Ownership beats timing.
    assert harness["all_equal"] is True
    orders = harness["orders"]
    ref = orders["ws_then_gate"]["canonical"]
    for name, dump in orders.items():
        assert dump["canonical"] == ref, name
    # Named orders required by Workstream G are all present.
    for name in ("ws_then_gate", "gate_then_ws", "guardedpre_then_ws",
                 "ws_then_gate_then_attacks", "gate_then_ws_then_attacks"):
        assert name in orders, name


def test_24b_specialized_setters_cannot_overwrite_after_workflow(harness):
    # The *_attacks orders call _dcSet/_obSet/_dagSet on every canonical node AFTER
    # workflow-state rendered; the canonical text is unchanged.
    attacked = harness["orders"]["ws_then_gate_then_attacks"]["canonical"]
    clean = harness["orders"]["ws_then_gate"]["canonical"]
    assert attacked == clean
    assert "ATTACK_DC" not in json.dumps(attacked)
    assert "ATTACK_OB" not in json.dumps(attacked)
    assert "ATTACK_DAG" not in json.dumps(attacked)


# =========================================================================== #
# VISIBLE CONTRACT (required 25–38) — from the harness DOM dump + source.
# =========================================================================== #
def test_25_to_31_all_surfaces_use_canonical_assessment_presentation(harness):
    c = harness["orders"]["ws_then_gate"]["canonical"]
    for p in ("cc", "dw", "pm"):
        assert c["%s-dag-title" % p] == "LATEST PORTFOLIO ASSESSMENT — 2026-07-31"
        assert c["%s-dag-badge" % p] == "OVERDUE"
        assert c["%s-dag-headline" % p] == "No portfolio change was recommended on 2026-07-31."
        assert c["%s-dag-explanation" % p] == "A new portfolio reassessment is required."


def test_30_right_rail_uses_canonical_workflow_presentation(harness):
    c = harness["orders"]["ws_then_gate"]["canonical"]
    assert c["right-current-task"] == "Wait for the current market session to close."
    assert c["right-next-action"] == "Wait for the market session to close"
    assert c["right-dag-badge"] == "OVERDUE"          # never "NO ACTION TODAY"
    assert c["right-dc-badge"].startswith("VALID")     # factual completed close


def test_31_to_34_all_surfaces_agree(harness):
    c = harness["orders"]["ws_then_gate"]["canonical"]
    # overall assessment currency identical across CC/DW/PM and the right rail.
    badges = {c["cc-dag-badge"], c["dw-dag-badge"], c["pm-dag-badge"], c["right-dag-badge"]}
    assert badges == {"OVERDUE"}


def test_35_stale_assessment_not_shown_as_no_action_today(harness):
    dump = json.dumps(harness["orders"]["ws_then_gate"]["canonical"])
    assert "NO ACTION TODAY" not in dump
    assert "DAILY ACTION GATE — TODAY" not in dump
    # The offending phrase never appears as an undated/current canonical conclusion.
    for p in ("cc", "dw", "pm"):
        assert "TODAY" not in harness["orders"]["ws_then_gate"]["canonical"]["%s-dag-title" % p].upper() \
            or "PORTFOLIO ASSESSMENT" in harness["orders"]["ws_then_gate"]["canonical"]["%s-dag-title" % p]


def test_36_historical_result_remains_visible_and_dated(harness):
    c = harness["orders"]["ws_then_gate"]["canonical"]
    assert "2026-07-31" in c["cc-dag-title"]
    assert "2026-07-31" in c["cc-dag-headline"]


def test_37_current_session_and_completed_close_evidence_distinguished(harness):
    ev = harness["orders"]["ws_then_gate"]["evidence"]
    assert "Current session:" in ev
    assert "Latest completed close:" in ev
    assert "still-open" in ev


def test_38_no_rerun_close_recommendation_in_ui_evidence(harness):
    ev = harness["orders"]["ws_then_gate"]["evidence"].lower()
    assert "rerun" not in ev and "re-run" not in ev
    assert "failed" not in ev


# =========================================================================== #
# API / SAFETY (required 39–50)
# =========================================================================== #
def test_39_40_endpoint_authenticated_and_get_only():
    src = APP.read_text(encoding="utf-8")
    i = src.find('"/v1/operations/workflow-state"')
    assert i != -1
    decorator = src[src.rfind("@app.", 0, i):src.find(")", i) + 1]
    assert "@app.get(" in decorator
    assert "_verify_api_key" in src[i:i + 400]


def test_41_to_50_workflow_state_declares_no_side_effects():
    s = _regression()["safety"]
    for flag in ("called_provider", "called_prediction", "ran_daily_close",
                 "ran_research_refresh", "ran_portfolio_reassessment", "promoted_model",
                 "created_orders", "created_signals", "created_trade_decisions",
                 "created_fills", "created_proposals", "wrote_to_database",
                 "wrote_to_ledger", "automatic_promotion_allowed"):
        assert s[flag] is False, flag
    assert s["read_only"] is True and s["paper_only"] is True


# =========================================================================== #
# ARCHITECTURE (required 51–58)
# =========================================================================== #
def test_51_exactly_one_workflow_loader():
    assert _ui().count("function loadWorkflowState") == 1


def test_52_no_unauthorized_primary_node_writer():
    assert _wf_report()["ui_unauthorized_canonical_writers"] == []


def test_53_54_no_ui_currency_or_priority_derivation():
    assert _wf_report()["ui_workflow_priority_derivation"] == []


def test_55_inventory_drift_zero():
    audit = _load_audit()
    d = audit.check_inventory_drift(audit._iter_source_files())
    assert d["on_disk_not_in_inventory"] == []
    assert d["in_inventory_not_on_disk"] == []


def test_56_slice6_reassessment_routes_to_daily_research_cycle():
    # Phase 29G.1 hard cutover: Slice 3 (the Daily Research Cycle) and Slice 6 (the
    # Holding Opportunity-Cost engine) are IMPLEMENTED. The portfolio reassessment is
    # produced BY the Daily Research Cycle (the sole execution path); there is NO
    # separate reassessment execution control and NO "not yet implemented" placeholder.
    r = _regression()
    q_by_code = {q["action_code"]: q for q in r["queued_actions"]}
    assert ws.ACTION_RUN_RESEARCH_CYCLE in q_by_code
    assert ws.ACTION_RUN_PORTFOLIO_REASSESSMENT in q_by_code
    assert q_by_code[ws.ACTION_RUN_RESEARCH_CYCLE]["execution_available"] is True
    rea = q_by_code[ws.ACTION_RUN_PORTFOLIO_REASSESSMENT]
    # Descriptive/routing only — NOT a separate execution control.
    assert rea["execution_available"] is False
    assert rea["slice3_pending"] is False
    assert "not yet implemented" not in rea["label"]
    assert rea["destination"] == ws.DEST_DAILY_WORKFLOW


def test_57_roadmap_forbids_big_bang_rewrite():
    txt = (REPO / "docs" / "CONSOLIDATION_ROADMAP.md").read_text(encoding="utf-8")
    assert "No big-bang rewrite" in txt


def test_58_raw_gate_vocabulary_retained():
    assert _wf_report()["raw_gate_vocab_retained"] is True


# =========================================================================== #
# Regression overall-state (contract anchor)
# =========================================================================== #
def test_regression_overall_state_and_consistency():
    r = _regression()
    assert r["overall_state"] == ws.WAITING_FOR_SESSION_CLOSE
    assert r["primary_action"]["action_code"] == ws.ACTION_WAIT_FOR_SESSION_CLOSE
    assert r["portfolio_assessment_state"]["assessment_status"] == ws.ASSESS_OVERDUE
    assert r["consistency_status"] == ws.CONSISTENT
