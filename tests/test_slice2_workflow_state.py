"""tests/test_slice2_workflow_state.py — Slice 2 (Phase 29C) canonical workflow state.

Deterministic, offline tests for the canonical combined-operator-interpretation
owner (``api.workflow_state``): the frozen overall-state vocabulary, the
deterministic priority policy, the assessment-currency (decision-currency) rule,
the current-session-vs-completed-work separation, the single primary action +
queued follow-ups, the cross-surface consistency validator, the read-only
endpoint, the single UI ``loadWorkflowState`` loader, and the safety invariants.

Every clock and data date is injected; no network, database, provider, prediction
or write occurs. The live regression fixture (Workstream L) uses a live ``now``
inside the fixture only — no live date is hardcoded in production code.
"""
from __future__ import annotations

import copy
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from paper_trader.api import workflow_state as ws
from paper_trader.engine import market_session as msession

ET = ZoneInfo("America/New_York")
UI = Path(__file__).resolve().parent.parent / "api" / "ui" / "index.html"
SRC = (Path(__file__).resolve().parent.parent / "api" / "workflow_state.py").read_text(encoding="utf-8")

# --------------------------------------------------------------------------- #
# Live regression fixture (Workstream L): current session (2026-08-05) still open;
# latest eligible completed session = 2026-08-04, fully processed. Research stale
# (monthly momentum + price/score behind), assessment for 2026-07-31 is overdue,
# forward evidence lags at 2026-07-31. No live date appears in production code.
# --------------------------------------------------------------------------- #
_OP = {"operational_book": {
    "book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1",
    "current_status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
    "nav_as_of_date": "2026-08-04", "desk_mark_date": "2026-08-04",
    "latest_desk_mark_date": "2026-08-04", "nav": 102241.79, "cash": 1500.0,
    "holdings_count": 25, "pending_order_count": 0,
    "current_target": {"alpha_market_date": "2026-07-31",
                       "latest_completed_market_date": "2026-08-04"}}}
_INPUTS = {"market_as_of_date": "2026-07-31", "momentum_month": "2026-07",
           "fundamental_as_of_date": "2026-05-22"}
# Slice 3: a REFRESHABLE daily-only staleness (monthly momentum current for the
# eligible month) → the Daily Research Cycle is REQUIRED and executable (not blocked
# by the month boundary — that BLOCKED case is covered in the Slice-3 suite).
_STALE_DAILY = {"market_as_of_date": "2026-08-03", "momentum_month": "2026-08",
                "fundamental_as_of_date": "2026-05-22"}
_DAILY = {"status": "NO_DAILY_REFRESH_YET", "latest_valid_mark_date": "2026-07-20"}
_DESK = {"series": {"SPY": [["2026-07-31", 747.03], ["2026-08-03", 757.67],
                            ["2026-08-04", 771.33]]}, "latest_completed_date": "2026-08-04"}
_CLOSE = {"market_date": "2026-08-04", "done": True,
          "final_close_status": "DAILY_CLOSE_COMPLETE_HOLD", "status": "CLOSE_FINISHED"}
_FWD = {"latest_snapshot_date": "2026-07-31", "snapshot_count": 5,
        "evidence_state": "FORWARD_EVIDENCE_PARTIAL", "interpretation": "partial",
        "active_book": {"model_id": "fundamental_momentum_50_50_v1", "book_id": "x"},
        "shadow_books": []}
_GATE = {"latest_completed_market_date": "2026-07-31", "outcome": "NO_ACTION_TODAY",
         "headline": "No portfolio change required.", "target_state": "CURRENT_ALIGNED",
         "action_required": False, "next_scheduled_full_review": "2026-08-01",
         "scheduled_review_due": True, "review_cadence": "MONTHLY"}
_TR = {"dates": {"alpha_market_date": "2026-07-31"}}
_REGRESSION_NOW = datetime(2026, 8, 5, 10, 0, tzinfo=ET)


def _regression(**kw):
    args = dict(now=_REGRESSION_NOW, operational=copy.deepcopy(_OP), inputs=dict(_INPUTS),
                daily_status=dict(_DAILY), desk_marks=copy.deepcopy(_DESK),
                close_progress=dict(_CLOSE), forward_status=copy.deepcopy(_FWD),
                gate=dict(_GATE), target_readiness=copy.deepcopy(_TR))
    args.update(kw)
    return ws.load_workflow_state(**args)


# Session-ready fixtures for the isolated priority-policy integration cases.
def _ready(**kw):
    op = {"operational_book": {**_OP["operational_book"],
          "current_target": {"alpha_market_date": "2026-08-04",
                             "latest_completed_market_date": "2026-08-04"}}}
    fresh_inputs = {"market_as_of_date": "2026-08-04", "momentum_month": "2026-08",
                    "fundamental_as_of_date": "2026-05-22"}
    gate = {**_GATE, "latest_completed_market_date": "2026-08-04",
            "next_scheduled_full_review": "2026-09-01", "scheduled_review_due": False}
    args = dict(reference_today="2026-08-05", operational=op, inputs=fresh_inputs,
                daily_status={"status": "DAILY_STATUS_READY", "latest_valid_mark_date": "2026-08-04"},
                desk_marks=copy.deepcopy(_DESK), close_progress=dict(_CLOSE),
                forward_status={**_FWD, "latest_snapshot_date": "2026-08-04"},
                gate=gate, target_readiness={"dates": {"alpha_market_date": "2026-08-04"}})
    args.update(kw)
    return ws.load_workflow_state(**args)


_D_DEFAULTS = dict(inconsistent=False, session_status=msession.SESSION_READY,
                   has_confirmed_eligible=True, eligible_session_closed=True,
                   owned_data_lag=False, research_current=True,
                   assessment_status=ws.ASSESS_CURRENT, manual_review_required=False,
                   evidence_gap=False)


def _decide(**kw):
    base = dict(_D_DEFAULTS)
    base.update(kw)
    return ws._decide_overall(**base)


# =========================================================================== #
# WORKFLOW CONTRACT (required tests 1–10)
# =========================================================================== #
_REQUIRED_TOP_LEVEL = (
    "status", "phase", "evaluated_at", "overall_state", "current_task", "headline",
    "primary_action", "queued_actions", "blockers", "warnings", "current_session",
    "operational_state", "research_state", "portfolio_assessment_state",
    "evidence_state", "model_governance_state", "completed_summary",
    "consistency_status", "consistency_violations", "safety", "provenance")


def test_01_all_required_top_level_fields_present():
    r = _regression()
    for k in _REQUIRED_TOP_LEVEL:
        assert k in r, f"missing top-level field: {k}"
    for k in ("action_code", "label", "explanation", "severity", "destination",
              "safe_to_execute", "execution_available", "manual_confirmation_required"):
        assert k in r["primary_action"], f"missing primary_action field: {k}"


def test_02_deterministic_for_identical_inputs():
    a = _regression()
    b = _regression()
    a.pop("evaluated_at"); b.pop("evaluated_at")
    assert a == b


def test_03_04_05_no_write_no_provider_no_prediction_source_scan():
    # The composition must not reference any writing / provider / prediction call.
    forbidden = ("run_daily_close", "run_refresh(", "capture_for_daily_close",
                 "confirmation_gate", "place_order", "create_order", "submit_order",
                 "prediction_client", "yfinance", "httpx", "requests.get", "urllib.request")
    for tok in forbidden:
        assert tok not in SRC, f"workflow_state must not reference {tok!r}"


def test_06_does_not_run_daily_close_uses_probe_free_progress():
    # Uses the PROBE-FREE close-progress loader, never the probing load_daily_close.
    assert "load_close_progress" in SRC
    assert "load_daily_close(" not in SRC


def test_07_08_09_10_safety_flags_declare_no_side_effects():
    s = _regression()["safety"]
    for flag in ("wrote_to_database", "wrote_to_ledger", "called_provider",
                 "called_prediction", "ran_daily_close", "ran_research_refresh",
                 "ran_portfolio_reassessment", "created_orders", "created_signals",
                 "created_trade_decisions", "created_fills", "created_proposals",
                 "promoted_model", "automatic_promotion_allowed"):
        assert s[flag] is False, flag
    assert s["read_only"] is True and s["paper_only"] is True


# =========================================================================== #
# SESSION PRIORITY (required tests 11–19)
# =========================================================================== #
def test_11_before_session_close_waits():
    assert _decide(session_status=msession.BEFORE_SESSION_CLOSE,
                   eligible_session_closed=True) == ws.WAITING_FOR_SESSION_CLOSE
    assert _regression()["overall_state"] == ws.WAITING_FOR_SESSION_CLOSE


def test_12_expected_complete_owned_data_missing():
    assert _decide(session_status=msession.WAITING_FOR_OWNED_DATA,
                   owned_data_lag=True) == ws.WAITING_FOR_OWNED_DATA
    assert _decide(session_status=msession.NO_CONFIRMED_DATA,
                   has_confirmed_eligible=False) == ws.WAITING_FOR_OWNED_DATA


def test_13_research_stale_requires_cycle():
    assert _decide(research_current=False) == ws.RESEARCH_CYCLE_REQUIRED
    assert _ready(inputs=dict(_STALE_DAILY),
                  gate={**_GATE, "latest_completed_market_date": "2026-07-31"}
                  )["overall_state"] == ws.RESEARCH_CYCLE_REQUIRED


def test_14_research_current_assessment_due_requires_reassessment():
    assert _decide(assessment_status=ws.ASSESS_STALE) == ws.PORTFOLIO_REASSESSMENT_REQUIRED
    r = _ready(gate={**_GATE, "latest_completed_market_date": "2026-07-31",
                     "next_scheduled_full_review": "2026-09-01", "scheduled_review_due": False})
    assert r["overall_state"] == ws.PORTFOLIO_REASSESSMENT_REQUIRED
    assert r["portfolio_assessment_state"]["assessment_status"] == ws.ASSESS_STALE


def test_15_research_and_assessment_current_close_incomplete_ready_for_close():
    assert _decide(eligible_session_closed=False) == ws.READY_FOR_DAILY_CLOSE
    r = _ready(close_progress={"market_date": "2026-08-04", "done": False,
                               "status": "NO_CLOSE_PROGRESS"})
    assert r["overall_state"] == ws.READY_FOR_DAILY_CLOSE


def test_16_close_and_evidence_complete_cycle_complete():
    assert _decide() == ws.DAILY_CYCLE_COMPLETE
    assert _ready()["overall_state"] == ws.DAILY_CYCLE_COMPLETE


def test_17_close_complete_evidence_gap():
    assert _decide(evidence_gap=True) == ws.DAILY_CYCLE_COMPLETE_EVIDENCE_GAP
    r = _ready(forward_status={**_FWD, "latest_snapshot_date": "2026-07-31"})
    assert r["overall_state"] == ws.DAILY_CYCLE_COMPLETE_EVIDENCE_GAP
    assert r["evidence_state"]["documented_gap"] is True


def test_18_manual_review_takes_documented_precedence():
    # Manual review preempts the plain/evidence-gap complete terminals (P7>P8>P9)
    # but sits below the daily-cycle progression gates.
    assert _decide(manual_review_required=True) == ws.MANUAL_REVIEW_REQUIRED
    assert _decide(manual_review_required=True, evidence_gap=True) == ws.MANUAL_REVIEW_REQUIRED
    # ...yet an unmet upstream gate (research) still wins over manual review.
    assert _decide(manual_review_required=True, research_current=False) == ws.RESEARCH_CYCLE_REQUIRED
    r = _ready(gate={**_GATE, "latest_completed_market_date": "2026-08-04",
                     "target_state": "APPROVAL_REQUIRED", "action_required": True,
                     "next_scheduled_full_review": "2026-09-01", "scheduled_review_due": False})
    assert r["overall_state"] == ws.MANUAL_REVIEW_REQUIRED


def test_19_inconsistent_state_highest_priority():
    assert _decide(inconsistent=True, session_status=msession.BEFORE_SESSION_CLOSE) == ws.INCONSISTENT_STATE
    assert _decide(session_status=msession.INCONSISTENT_FUTURE_DATA) == ws.INCONSISTENT_STATE
    assert _decide(assessment_status=ws.ASSESS_INCONSISTENT) == ws.INCONSISTENT_STATE
    r = _regression(date_overrides={"valuation_date": "2026-07-20"})
    assert r["consistency_status"] == ws.INCONSISTENT
    assert r["overall_state"] == ws.INCONSISTENT_STATE


# =========================================================================== #
# DECISION CURRENCY (required tests 20–26)
# =========================================================================== #
def test_20_assessment_for_eligible_session_can_be_current():
    c = ws.classify_assessment(assessment_date="2026-08-04", eligible_date="2026-08-04",
                               current_reference_date="2026-08-04",
                               next_review_date="2026-09-01", review_due=False)
    assert c["status"] == ws.ASSESS_CURRENT
    assert c["current_for_eligible_session"] is True


def test_21_older_assessment_is_stale():
    c = ws.classify_assessment(assessment_date="2026-07-31", eligible_date="2026-08-04",
                               current_reference_date="2026-08-04",
                               next_review_date="2026-09-01", review_due=False)
    assert c["status"] == ws.ASSESS_STALE


def test_22_review_date_before_current_is_overdue():
    c = ws.classify_assessment(assessment_date="2026-07-31", eligible_date="2026-08-04",
                               current_reference_date="2026-08-05",
                               next_review_date="2026-08-01", review_due=True)
    assert c["status"] == ws.ASSESS_OVERDUE
    assert c["review_overdue"] is True


def test_23_24_historical_no_change_preserved_but_not_labelled_today():
    r = _regression()
    pa = r["completed_summary"]["latest_portfolio_assessment"]
    # 23 — the historical no-change result is preserved (dated).
    assert pa["market_date"] == "2026-07-31"
    assert pa["result"] == "NO_ACTION_TODAY"
    assert "2026-07-31" in (pa["preserved_statement"] or "")
    # 24 — it is NOT presented as a current "NO ACTION TODAY" conclusion.
    for field in (r["headline"], r["current_task"], r["primary_action"]["label"],
                  r["primary_action"]["explanation"], pa["preserved_statement"]):
        assert "NO ACTION TODAY" not in (field or "")
    assert r["portfolio_assessment_state"]["assessment_status"] == ws.ASSESS_OVERDUE


def test_25_missing_assessment_returns_missing():
    c = ws.classify_assessment(assessment_date=None, eligible_date="2026-08-04",
                               current_reference_date="2026-08-04",
                               next_review_date=None, review_due=False)
    assert c["status"] == ws.ASSESS_MISSING


def test_26_assessment_after_eligible_is_inconsistent():
    c = ws.classify_assessment(assessment_date="2026-08-05", eligible_date="2026-08-04",
                               current_reference_date="2026-08-05",
                               next_review_date=None, review_due=False)
    assert c["status"] == ws.ASSESS_INCONSISTENT
    r = _ready(gate={**_GATE, "latest_completed_market_date": "2026-08-06",
                     "next_scheduled_full_review": "2026-09-01", "scheduled_review_due": False})
    assert r["portfolio_assessment_state"]["assessment_status"] == ws.ASSESS_INCONSISTENT
    assert r["overall_state"] == ws.INCONSISTENT_STATE


# =========================================================================== #
# STATE SEPARATION (required tests 27–35)
# =========================================================================== #
def test_27_current_session_separate_from_latest_eligible():
    cs = _regression()["current_session"]
    assert cs["calendar_date"] == "2026-08-05"
    assert cs["latest_eligible_completed_market_date"] == "2026-08-04"
    assert cs["calendar_date"] != cs["latest_eligible_completed_market_date"]


def test_28_latest_close_separate_from_latest_assessment():
    r = _regression()
    assert r["operational_state"]["latest_completed_close_date"] == "2026-08-04"
    assert r["portfolio_assessment_state"]["latest_assessment_date"] == "2026-07-31"


def test_29_latest_research_run_separate_from_target_date():
    r = _regression()
    assert r["research_state"]["latest_price_score_refresh_date"] == "2026-07-31"
    assert r["research_state"]["target_calculation_date"] == "2026-07-31"
    # distinct concepts, resolved from distinct owners (both happen to be 07-31 here)
    assert "target_calculation_date" in r["research_state"]
    assert "latest_daily_research_run_date" in r["research_state"]


def test_30_target_date_separate_from_evidence_date():
    r = _regression()
    assert r["research_state"]["target_calculation_date"] == "2026-07-31"
    assert r["research_state"]["latest_true_forward_snapshot_date"] == "2026-07-31"
    assert r["evidence_state"]["latest_snapshot_date"] == "2026-07-31"


def test_31_operational_close_validity_separate_from_research_readiness():
    r = _regression()
    assert r["operational_state"]["operational_close_valid"] is True
    assert r["research_state"]["research_inputs_current"] is False


def test_32_evidence_gap_is_attention_not_error():
    r = _regression()
    assert r["evidence_state"]["documented_gap"] is True
    assert r["evidence_state"]["gap_severity"] == ws.SEV_ATTENTION
    assert r["evidence_state"]["gap_severity"] != ws.SEV_ERROR


def test_33_research_staleness_does_not_invalidate_completed_close():
    r = _regression()
    assert r["operational_state"]["operational_close_valid"] is True
    assert any("remains valid" in w for w in r["warnings"])


def test_34_pending_orders_surfaced():
    r = _regression(operational={"operational_book": {**_OP["operational_book"],
                                                      "pending_order_count": 3}})
    assert r["operational_state"]["pending_orders"] == 3
    assert any(q["action_code"] == ws.ACTION_REVIEW_PENDING_ORDERS
               for q in r["queued_actions"])


def test_35_automatic_model_promotion_remains_false():
    assert _regression()["model_governance_state"]["automatic_promotion_allowed"] is False
    assert _ready()["model_governance_state"]["automatic_promotion_allowed"] is False


# =========================================================================== #
# PRIMARY ACTION (required tests 36–41)
# =========================================================================== #
def test_36_exactly_one_primary_action():
    pa = _regression()["primary_action"]
    assert isinstance(pa, dict) and pa["action_code"] == ws.ACTION_WAIT_FOR_SESSION_CLOSE


def test_37_additional_required_actions_in_queued():
    q = {a["action_code"] for a in _regression()["queued_actions"]}
    assert ws.ACTION_RUN_RESEARCH_CYCLE in q          # research stale
    assert ws.ACTION_RUN_PORTFOLIO_REASSESSMENT in q  # assessment overdue


def test_38_current_task_and_primary_follow_priority_policy():
    for r in (_regression(), _ready()):
        assert ws._EXPECTED_ACTION_FOR[r["overall_state"]] == r["primary_action"]["action_code"]


def test_39_destination_is_valid_ui_route():
    for r in (_regression(), _ready(),
              _ready(inputs=dict(_INPUTS), gate={**_GATE, "latest_completed_market_date": "2026-07-31"}),
              _ready(close_progress={"market_date": "2026-08-04", "done": False, "status": "X"})):
        assert r["primary_action"]["destination"] in ws.VALID_DESTINATIONS
        for q in r["queued_actions"]:
            assert q["destination"] in ws.VALID_DESTINATIONS


def test_40_read_only_descriptive_actions_not_marked_executed():
    r = _regression()  # WAIT_FOR_SESSION_CLOSE
    assert r["primary_action"]["execution_available"] is False


def test_41_slice3_research_action_is_executable():
    # Slice 3: the research-cycle action is now implemented and executable (the
    # descriptive/routing-only Slice-2 placeholder is gone).
    r = _ready(inputs=dict(_STALE_DAILY),
               gate={**_GATE, "latest_completed_market_date": "2026-07-31"})
    pa = r["primary_action"]
    assert r["overall_state"] == ws.RESEARCH_CYCLE_REQUIRED
    assert pa["action_code"] == ws.ACTION_RUN_RESEARCH_CYCLE
    assert pa["slice3_pending"] is False
    assert pa["execution_available"] is True
    assert pa["confirmation_required"] == "RUN_DAILY_RESEARCH_CYCLE"


# =========================================================================== #
# CONSISTENCY (required tests 42–48)
# =========================================================================== #
def test_42_matching_inputs_are_consistent():
    r = _regression()
    assert r["consistency_status"] == ws.CONSISTENT
    assert r["consistency_violations"] == []


def test_43_active_book_mismatch_is_inconsistent():
    r = _regression(active_book_override=[_OP, {"operational_book": {
        **_OP["operational_book"], "book_id": "other_book"}}])
    assert r["consistency_status"] == ws.INCONSISTENT
    codes = {v["code"] for v in r["consistency_violations"]}
    assert "MULTIPLE_ACTIVE_BOOK_CANDIDATES" in codes


def test_44_close_date_mismatch_is_inconsistent():
    r = _regression(date_overrides={"daily_close_date": "2026-07-20"})
    assert r["consistency_status"] == ws.INCONSISTENT
    assert any(v["code"] == "DAILY_CLOSE_MISMATCH" for v in r["consistency_violations"])


def test_45_assessment_date_mismatch_is_inconsistent():
    # Freshness/gate assessment date disagrees with the gate's own value → the
    # consistency validator names the conflicting owners.
    r = _regression(gate={**_GATE, "latest_completed_market_date": "2026-08-06"})
    assert r["consistency_status"] == ws.INCONSISTENT
    codes = {v["code"] for v in r["consistency_violations"]}
    assert "ASSESSMENT_AHEAD_OF_ELIGIBLE_SESSION" in codes


def test_46_research_target_readiness_mismatch_is_inconsistent():
    r = _regression(target_readiness={"dates": {"alpha_market_date": "2026-06-30"}})
    assert r["consistency_status"] == ws.INCONSISTENT
    assert any(v["code"] == "TARGET_READINESS_MISMATCH" for v in r["consistency_violations"])


def test_47_evidence_date_mismatch_is_inconsistent():
    # The freshness evidence row disagrees with the Forward Prediction Skill owner
    # (data_freshness override vs the raw forward status): a genuine cross-owner
    # disagreement the validator must name (research staleness alone is NOT one).
    r = _regression(date_overrides={"true_forward_date": "2026-06-30"})
    assert r["consistency_status"] == ws.INCONSISTENT
    assert any(v["code"] == "EVIDENCE_DATE_MISMATCH" for v in r["consistency_violations"])


def test_48_every_violation_names_conflicting_owners():
    r = _regression(date_overrides={"valuation_date": "2026-07-20"})
    assert r["consistency_violations"]
    for v in r["consistency_violations"]:
        assert v.get("code")
        has_owner = ("authoritative_owner" in v or "authoritative_owners" in v)
        assert has_owner, v


# =========================================================================== #
# API (required tests 49–54)
# =========================================================================== #
def _client():
    from fastapi.testclient import TestClient
    from paper_trader.api.app import app
    return TestClient(app, raise_server_exceptions=False)


def _key():
    from paper_trader.config import get_settings
    return get_settings().service_api_key


def test_49_50_endpoint_authenticated_and_get_only():
    c = _client()
    assert c.get("/v1/operations/workflow-state").status_code in (401, 403)
    assert c.post("/v1/operations/workflow-state",
                  headers={"X-API-Key": _key()}).status_code == 405


def test_51_52_53_endpoint_read_only_degrades_and_returns_vocab(monkeypatch):
    canned = {
        "status": "OK", "phase": ws.PHASE, "overall_state": ws.WAITING_FOR_SESSION_CLOSE,
        "overall_state_vocabulary": list(ws.OVERALL_STATES), "current_task": "wait",
        "headline": "waiting", "primary_action": {
            "action_code": ws.ACTION_WAIT_FOR_SESSION_CLOSE, "label": "wait",
            "explanation": "", "severity": ws.SEV_INFO, "destination": ws.DEST_COMMAND_CENTER,
            "safe_to_execute": True, "execution_available": False,
            "manual_confirmation_required": False},
        "queued_actions": [], "blockers": [], "warnings": [],
        "current_session": {}, "operational_state": {}, "research_state": {},
        "portfolio_assessment_state": {}, "evidence_state": {},
        "model_governance_state": {"automatic_promotion_allowed": False},
        "completed_summary": {}, "consistency_status": ws.CONSISTENT,
        "consistency_violations": [], "safety": {"read_only": True,
        "wrote_to_database": False, "called_provider": False, "called_prediction": False,
        "ran_daily_close": False}}
    monkeypatch.setattr("paper_trader.api.app.load_workflow_state", lambda: canned)
    c = _client()
    resp = c.get("/v1/operations/workflow-state", headers={"X-API-Key": _key()})
    assert resp.status_code == 200
    body = resp.json()
    assert body["overall_state"] in ws.OVERALL_STATES
    assert set(body["overall_state_vocabulary"]) == set(ws.OVERALL_STATES)
    s = body["safety"]
    assert s["read_only"] is True and s["called_provider"] is False
    assert s["called_prediction"] is False and s["ran_daily_close"] is False


def test_52_degrades_when_a_source_is_unavailable(monkeypatch):
    def _boom():
        raise RuntimeError("operational book loader unavailable")
    monkeypatch.setattr("paper_trader.api.operational_book.load_operational_book", _boom)
    r = ws.load_workflow_state(reference_today="2026-08-05", inputs=dict(_INPUTS),
                               daily_status=dict(_DAILY), desk_marks=copy.deepcopy(_DESK),
                               close_progress=dict(_CLOSE), forward_status=copy.deepcopy(_FWD),
                               gate=dict(_GATE), target_readiness=copy.deepcopy(_TR))
    assert r["status"] == "OK"
    assert any("unavailable" in w for w in r["warnings"])


def test_54_no_post_endpoint_for_workflow_state_execution():
    from paper_trader.api.app import app
    posts = [r for r in app.routes
             if getattr(r, "path", None) == "/v1/operations/workflow-state"
             and "POST" in getattr(r, "methods", set())]
    assert posts == []


# =========================================================================== #
# UI (required tests 55–66) — substring / structural, like the Slice-1 tests.
# =========================================================================== #
def _ui():
    return UI.read_text(encoding="utf-8")


def test_55_ui_has_exactly_one_workflow_loader():
    html = _ui()
    assert html.count("function loadWorkflowState") == 1
    assert html.count("/v1/operations/workflow-state") >= 1


def test_56_57_58_59_60_61_62_surfaces_use_workflow_payload():
    html = _ui()
    # One canonical banner container per primary surface + the Action/Safety panel.
    for anchor in ('id="wf-cc"', 'id="wf-dw"', 'id="wf-pt"', 'id="wf-pm"',
                   'id="wf-ra"', 'id="wf-right"'):
        assert anchor in html, anchor
    # The renderer reads the canonical payload fields (server-provided).
    for field in ("d.overall_state", "d.primary_action", "d.current_task", "d.headline",
                  "d.queued_actions", "d.portfolio_assessment_state", "d.evidence_state"):
        assert field in html, field
    # renderWorkflowState fans the ONE payload out to all six containers.
    assert "['wf-cc', 'wf-dw', 'wf-pt', 'wf-pm', 'wf-ra', 'wf-right']" in html


def test_63_64_65_ui_performs_no_priority_or_currency_calculation():
    html = _ui()
    start = html.find("function loadWorkflowState")
    end = html.find("window.renderWorkflowState")
    assert start != -1 and end != -1 and end > start
    region = html[start:end]
    # 63/64 — no client-side date arithmetic (workflow priority / assessment currency).
    for bad in ("new Date(", "Date.now(", ".getTime("):
        assert bad not in region
    # 65 — the loader/render region never constructs a stale "NO ACTION TODAY" label.
    assert "NO ACTION TODAY" not in region


def test_66_existing_detailed_panels_and_loaders_intact():
    html = _ui()
    assert "function loadOperationalBook" in html
    assert "function renderDailyActionGate" in html and "function renderDailyClose" in html
    assert 'id="cc-dc-card"' in html and "Run Daily Close" in html
    assert html.count("function loadDataFreshness") == 1  # Slice-1 loader untouched


# =========================================================================== #
# Safety / vocabulary invariants
# =========================================================================== #
def test_frozen_vocabularies_are_stable():
    assert set(ws.OVERALL_STATES) == {
        "WAITING_FOR_SESSION_CLOSE", "WAITING_FOR_OWNED_DATA", "RESEARCH_CYCLE_REQUIRED",
        "RESEARCH_CYCLE_RUNNING", "RESEARCH_CYCLE_BLOCKED",
        "PORTFOLIO_REASSESSMENT_REQUIRED", "READY_FOR_DAILY_CLOSE", "DAILY_CYCLE_COMPLETE",
        "DAILY_CYCLE_COMPLETE_EVIDENCE_GAP", "MANUAL_REVIEW_REQUIRED", "INCONSISTENT_STATE"}
    assert set(ws.ASSESSMENT_STATUSES) == {
        "CURRENT", "STALE", "DUE", "OVERDUE", "MISSING", "INCONSISTENT"}
    assert set(ws.ACTION_SEVERITIES) == {"INFO", "SUCCESS", "ATTENTION", "BLOCKED", "ERROR"}


def test_every_overall_state_maps_to_an_expected_action():
    for s in ws.OVERALL_STATES:
        assert s in ws._EXPECTED_ACTION_FOR
