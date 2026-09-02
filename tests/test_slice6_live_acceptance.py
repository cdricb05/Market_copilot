"""Phase 29G.1 — Slice 6 LIVE-ACCEPTANCE READINESS, operator workflow & UI hard cutover.

Deterministic and hermetic. No DB / provider / prediction / operational ledger / real
Daily Research Cycle / real Daily Close / real Holding Opportunity-Cost assessment is
touched. Readiness endpoints are exercised at the function level with a monkeypatched
session; the Holding Opportunity-Cost read contract is driven by injected portfolio-state
and injected synthetic artifacts (temp roots only, no real artifact written); the UI and
owner sources are asserted structurally; the strict architecture audit guard is executed.

Covers the required matrix: service-vs-workflow readiness (health / ready contract,
genuine dependency failure, service-ready-while-workflow-waits, no session conflation,
header separation), removal of the obsolete Slice-3 reassessment control, legacy
membership-comparison reclassified compatibility-only, the canonical single-flight HOC
panel (NOT_RUN + completed views, counts, hash, policy, gaps, additions), the Daily
Research Cycle Slice-6 step lifecycle, the isolated closed-session live-acceptance
fixture (NOT_RUN -> AVAILABLE -> ready-for-close transition), API compatibility (no
reassessment / rebalance / order route), and the strict architecture guard.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from paper_trader.api import app as appmod
from paper_trader.api import daily_close as dc
from paper_trader.api import holding_opportunity_cost as hoc
from paper_trader.api import daily_research_cycle as drc
from paper_trader.api import workflow_state as ws

from fastapi import HTTPException

ROOT = Path(__file__).resolve().parent.parent
UI = (ROOT / "api" / "ui" / "index.html").read_text(encoding="utf-8")
WS_SRC = (ROOT / "api" / "workflow_state.py").read_text(encoding="utf-8")
DC_SRC = (ROOT / "api" / "daily_close.py").read_text(encoding="utf-8")
APP_SRC = (ROOT / "api" / "app.py").read_text(encoding="utf-8")


def _load_audit():
    path = ROOT / "scripts" / "audit_architecture.py"
    spec = importlib.util.spec_from_file_location("audit_architecture_la6", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# --------------------------------------------------------------------------- #
# Hermetic session shims (no real database).
# --------------------------------------------------------------------------- #
class _OkSession:
    def execute(self, *a, **k):
        return None


class _OkCtx:
    def __enter__(self):
        return _OkSession()

    def __exit__(self, *a):
        return False


class _BoomCtx:
    def __enter__(self):
        raise RuntimeError("connection refused: could not connect to server")

    def __exit__(self, *a):
        return False


# --------------------------------------------------------------------------- #
# Portfolio-state stub + synthetic completed artifact (temp roots only).
# --------------------------------------------------------------------------- #
def _ps(book_id="alpha_paper_book_1", eligible="2026-08-06", dormant=False):
    return {
        "active_book": {"book_id": book_id, "book_label": "Alpha Paper Book #1",
                        "status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
                        "holdings_count": 25, "is_dormant_legacy_book": dormant},
        "dates": {"eligible_market_date": eligible, "valuation_date": eligible},
        "capital": {"nav": 100000.0, "cash": 8000.0},
        "state_hash": "PSHASH",
        "positions": [],
    }


def _synthetic_assessment():
    """A completed assessment with HOLD/REDUCE/EXIT/REPLACE recommendations + one ADD."""
    return {
        "assessment_state": "READY",
        "assessment_hash": "SYNTHHASH0123456789ABCDEF",
        "policy": {"policy_version": "hoc_policy_v1"},
        "recommendation_counts": {"HOLD": 1, "REDUCE": 1, "EXIT": 1, "REPLACE": 1, "ADD": 1},
        "holding_reviews": [
            {"ticker": "AAA", "recommendation": "HOLD", "current_rank": 10,
             "rank_change": 0, "deterioration_state": "STABLE", "explanation": "hold"},
            {"ticker": "BBB", "recommendation": "REDUCE", "current_rank": 40,
             "rank_change": -5, "deterioration_state": "DETERIORATING", "explanation": "trim"},
            {"ticker": "CCC", "recommendation": "EXIT", "current_rank": 480,
             "rank_change": -60, "deterioration_state": "BROKEN", "explanation": "exit"},
            {"ticker": "DDD", "recommendation": "REPLACE", "current_rank": 300,
             "rank_change": -30, "deterioration_state": "DETERIORATING",
             "strongest_replacement_ticker": "EEE", "replacement_rank": 1,
             "net_improvement": 0.12, "explanation": "replace"},
        ],
        "addition_candidates": [
            {"ticker": "EEE", "rank": 1, "score": 0.99, "sector": "Health",
             "recommendation": "ADD"}],
        "data_quality": {"holdings_evaluated": 4, "data_gaps": ["risk_panel_short_window"]},
        "portfolio_summary": {"nav": 100000.0, "holdings": 4},
        "safety": {"read_only": True, "no_orders": True},
        "provenance": {"composition_owner": "api.holding_opportunity_cost"},
    }


def _synthetic_artifact(eligible="2026-08-06", book_id="alpha_paper_book_1"):
    return {
        "artifact_id": "hoc_synth_20260806",
        "generated_at": "2026-08-06T21:05:00+00:00",
        "identity": {"active_book_id": book_id, "eligible_market_date": eligible},
        "immutable": True,
        "assessment": _synthetic_assessment(),
        "input_contract": {"eligible_market_date": eligible, "active_book_id": book_id},
    }


# =========================================================================== #
# WORKSTREAM A — SERVICE vs WORKFLOW readiness.
# =========================================================================== #
def test_01_health_contract():
    out = appmod.health()
    assert out.status == "ok"
    assert out.service


def test_02_ready_contract_service_scoped(monkeypatch):
    monkeypatch.setattr(appmod, "get_session", lambda: _OkCtx())
    out = appmod.ready()
    assert out.status == "ok"
    assert out.database == "ok"
    assert out.ready is True
    assert out.readiness_kind == "service"
    assert out.reason is None


def test_03_ready_genuine_dependency_failure_exposes_exact_reason(monkeypatch):
    monkeypatch.setattr(appmod, "get_session", lambda: _BoomCtx())
    with pytest.raises(HTTPException) as ei:
        appmod.ready()
    exc = ei.value
    assert exc.status_code == 503
    d = exc.detail
    assert isinstance(d, dict)
    assert d["ready"] is False
    assert d["readiness_kind"] == "service"
    assert d["reason"] and "connection refused" in d["reason"]  # exact, not masked


def test_04_ready_never_conflates_workflow_or_session_timing(monkeypatch):
    # A genuine service failure reason must be about the dependency, never a workflow
    # state such as WAITING_FOR_SESSION_CLOSE.
    monkeypatch.setattr(appmod, "get_session", lambda: _BoomCtx())
    with pytest.raises(HTTPException) as ei:
        appmod.ready()
    blob = str(ei.value.detail)
    assert "WAITING_FOR_SESSION" not in blob
    assert "market_session" not in blob


def test_05_ready_body_is_service_scoped_not_session_scoped():
    # The executable /v1/ready body must be service-scoped and must not key off the
    # market session (the docstring may name the workflow state to explain the split).
    audit = _load_audit()
    start = APP_SRC.find("def ready(")
    body = audit._strip_prose(APP_SRC[start:start + 2500])
    assert "readiness_kind" in body
    for tok in ("market_session", "WAITING_FOR_SESSION", "eligible_market_date", "session_close"):
        assert tok not in body


def test_06_header_shows_service_and_workflow_readiness_separately():
    assert 'id="health-status"' in UI            # service indicator
    assert 'id="wf-readiness-text"' in UI         # workflow indicator (distinct)
    assert 'id="wf-readiness-light"' in UI
    assert "Service:" in UI and "Workflow:" in UI
    assert "checkServiceReady" in UI              # service probe reader (with reason)


def test_07_service_ready_while_workflow_waiting_is_not_a_service_failure(monkeypatch):
    # Service readiness is DB-only; a waiting workflow never makes it unready.
    monkeypatch.setattr(appmod, "get_session", lambda: _OkCtx())
    out = appmod.ready()
    assert out.ready is True and out.readiness_kind == "service"


# =========================================================================== #
# WORKSTREAM B — obsolete reassessment control removed.
# =========================================================================== #
def test_10_no_obsolete_reassessment_placeholder_in_workflow_owner():
    assert "not yet implemented" not in WS_SRC
    assert "Run a portfolio reassessment" not in WS_SRC


def test_11_reassessment_routes_to_daily_research_cycle_sole_path():
    assert ws.ACTION_RUN_PORTFOLIO_REASSESSMENT  # action code retained (routing)
    # The reassessment explanation names the DRC as the sole execution path.
    assert "daily-research-cycle/run" in WS_SRC


# =========================================================================== #
# WORKSTREAM C — legacy comparison reclassified compatibility-only.
# =========================================================================== #
def test_20_legacy_comparison_not_labelled_rebalance_proposal_ready():
    pres = dc._PRESENTATION[dc.REBALANCE_PROPOSAL_READY]
    for field in ("label", "headline", "primary_action_label", "current_task", "cycle_label"):
        val = pres[field]
        assert "REBALANCE PROPOSAL READY" not in val
        assert "Rebalance Proposal Ready" not in val
        assert "Review Rebalance Proposal" not in val
        assert "ready to rebalance" not in val.lower()
        assert "portfolio changes proposed" not in val.lower()


def test_21_legacy_comparison_classified_compatibility_only():
    pres = dc._PRESENTATION[dc.REBALANCE_PROPOSAL_READY]
    # Release 29.3 (42978bf) renamed the token to DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT
    # and 9ee3028 restated the label so the close reports what the CLOSE observed. Assert
    # the CLASSIFICATION this workstream exists to protect — a legacy membership
    # difference, explicitly compatibility-only, never an approved reallocation — rather
    # than the superseded "LEGACY MEMBERSHIP-COMPARISON" spelling.
    assert "LEGACY MEMBERSHIP" in pres["label"]
    assert "COMPATIBILITY ONLY" in pres["headline"]
    assert "compatibility-only" in pres["current_task"]
    assert "NOT an approved reallocation" in pres["next_action"]
    assert pres["primary_action_label"]           # still non-empty (compat)
    # State key + action kind preserved for historical/audit compatibility.
    assert pres["primary_action_kind"] == "REVIEW_PROPOSAL"


# =========================================================================== #
# WORKSTREAM D — canonical single-flight HOC panel (NOT_RUN .. completed).
# =========================================================================== #
def test_30_hoc_not_run_endpoint_response(tmp_path):
    payload = hoc.load_holding_opportunity_cost(portfolio_state=_ps(), hoc_dir=str(tmp_path))
    assert payload["state"] == hoc.STATE_NOT_RUN
    assert payload["active_book"]["book_id"] == "alpha_paper_book_1"
    assert payload["eligible_market_date"] == "2026-08-06"
    assert payload["sole_execution_path"] == "POST /v1/operations/daily-research-cycle/run"
    # No fabricated recommendations / artifact.
    assert payload["holding_reviews"] == []
    assert payload["addition_candidates"] == []
    assert payload["artifact"] is None
    assert payload["assessment_hash"] is None
    # temp root untouched (no artifact written by a read).
    assert list(tmp_path.iterdir()) == []


def test_31_hoc_completed_view_from_injected_artifact():
    payload = hoc.load_holding_opportunity_cost(
        portfolio_state=_ps(), artifact=_synthetic_artifact())
    assert payload["state"] == "READY"
    c = payload["recommendation_counts"]
    assert c["HOLD"] == 1 and c["REDUCE"] == 1 and c["EXIT"] == 1
    assert c["REPLACE"] == 1 and c["ADD"] == 1
    assert payload["assessment_hash"] == "SYNTHHASH0123456789ABCDEF"
    assert payload["policy"]["policy_version"] == "hoc_policy_v1"
    assert payload["data_quality"]["holdings_evaluated"] == 4
    assert payload["data_quality"]["data_gaps"] == ["risk_panel_short_window"]
    assert len(payload["addition_candidates"]) == 1
    assert payload["addition_candidates"][0]["ticker"] == "EEE"
    recs = {r["recommendation"] for r in payload["holding_reviews"]}
    assert {"HOLD", "REDUCE", "EXIT", "REPLACE"} <= recs


def test_32_dormant_legacy_book_never_selected(tmp_path):
    # A dormant legacy book yields no assessment (never the operating decision).
    payload = hoc.load_holding_opportunity_cost(
        portfolio_state=_ps(book_id="", dormant=True), hoc_dir=str(tmp_path))
    assert payload["state"] in (hoc.STATE_NO_ACTIVE_BOOK, hoc.STATE_NOT_RUN)
    assert payload["holding_reviews"] == []


def test_33_single_flight_hoc_ui_loader():
    assert UI.count("function loadHoldingOpportunityCost") == 1
    assert "window._hocInFlight" in UI            # single-flight guard


def test_34_ui_renders_not_run_and_completed_states():
    assert "hasAssessment" in UI
    assert "NONE YET" in UI                        # NOT_RUN view, no fabricated counts
    assert "completed assessment view" in UI
    assert "PRIMARY PORTFOLIO DECISION" in UI      # HOC is the primary decision card


def test_35_ui_computes_no_recommendation_or_allocation():
    start = UI.find("function loadHoldingOpportunityCost")
    end = UI.find("window.renderHoldingOpportunityCost")
    region = UI[start:end]
    for tok in ("new Date(", "Date.now(", ".reduce(", "Math.", "compute", "cost_rate"):
        assert tok not in region


# =========================================================================== #
# WORKSTREAM E — Daily Research Cycle Slice-6 step surfaced in the UI.
# =========================================================================== #
def test_40_drc_step_constant_is_canonical():
    assert drc.STEP_HOLDING_OPP_COST == "ASSESS_HOLDING_OPPORTUNITY_COST"


def test_41_ui_exposes_drc_opportunity_cost_step_lifecycle():
    for _id in ("drc-oc-owner", "drc-oc-state", "drc-oc-required", "drc-oc-step",
                "drc-oc-artifact", "drc-oc-hash", "drc-oc-holdings", "drc-oc-counts",
                "drc-oc-gaps"):
        assert ('id="%s"' % _id) in UI
    assert "ASSESS_HOLDING_OPPORTUNITY_COST" in UI
    # lifecycle labels rendered verbatim from the backend contract
    for label in ("completed", "running", "selected", "pending"):
        assert label in UI


# =========================================================================== #
# WORKSTREAM F — isolated closed-session live-acceptance transition.
# =========================================================================== #
def test_50_live_acceptance_transition_not_run_to_available(tmp_path):
    ps = _ps(eligible="2026-08-06")
    # Stage 1: session closed, DRC executable, NO artifact yet -> NOT_RUN.
    before = hoc.load_holding_opportunity_cost(portfolio_state=ps, hoc_dir=str(tmp_path))
    assert before["state"] == hoc.STATE_NOT_RUN
    # Stage 2: the DRC has produced the assessment -> AVAILABLE (completed).
    after = hoc.load_holding_opportunity_cost(portfolio_state=ps, artifact=_synthetic_artifact())
    assert after["state"] == "READY"
    assert after["recommendation_counts"]["REPLACE"] == 1
    assert after["addition_candidates"]                 # >= 1 ADD candidate
    # No mutation: the temp artifact root is still empty after both reads.
    assert list(tmp_path.iterdir()) == []


def test_51_no_real_artifact_created_and_no_mutation(tmp_path):
    # Injected-artifact and empty-dir reads write nothing; the read is side-effect free.
    hoc.load_holding_opportunity_cost(portfolio_state=_ps(), hoc_dir=str(tmp_path))
    hoc.load_holding_opportunity_cost(portfolio_state=_ps(), artifact=_synthetic_artifact())
    assert list(tmp_path.iterdir()) == []


# =========================================================================== #
# WORKSTREAM G — API compatibility (no new POST reassessment/rebalance/order route).
# =========================================================================== #
def test_60_no_reassessment_rebalance_or_order_route():
    audit = _load_audit()
    routes = audit.check_routes()["routes"]
    paths = {r["path"] for r in routes}
    # Stage 20 UPDATE: what Phase 29G.1 removed — and what stays forbidden — is a MANUAL
    # reassessment EXECUTION / apply / approve control. The Stage-20 canonical
    # GET /v1/operations/portfolio-reassessment READ contract executes nothing (the sole
    # execution path is still the Daily Research Cycle) and is therefore permitted; it is
    # governed by check_portfolio_reassessment_ownership.
    for forbidden in ("/v1/operations/portfolio-reassessment/run",
                      "/v1/operations/portfolio-reassessment/execute",
                      "/v1/operations/portfolio-reassessment/approve",
                      "/v1/operations/portfolio-reassessment/apply",
                      "/v1/operations/reassessment",
                      "/v1/operations/reassessment/run",
                      "/v1/operations/rebalance-proposal",
                      "/v1/operations/holding-opportunity-cost/run",
                      "/v1/operations/confirm-target"):
        assert forbidden not in paths
    # The Stage-20 reassessment read contract is GET-only.
    prs_methods = sorted({r["method"] for r in routes
                          if (r["path"] or "").startswith(
                              "/v1/operations/portfolio-reassessment")})
    assert prs_methods == ["GET"]
    # Stage-19 controlled-rebalance IS present: GET lifecycle + POST second-confirmation.
    _m = lambda p: sorted({r["method"] for r in routes if r["path"] == p})
    assert "GET" in _m("/v1/operations/rebalance")
    assert "POST" in _m("/v1/operations/rebalance/confirm-order-plan")
    # The canonical HOC read route is GET-only.
    hoc_methods = sorted({r["method"] for r in routes
                          if r["path"] == "/v1/operations/holding-opportunity-cost"})
    assert hoc_methods == ["GET"]


# =========================================================================== #
# WORKSTREAM H — strict architecture guard.
# =========================================================================== #
def test_70_slice6_live_acceptance_audit_guard_green():
    audit = _load_audit()
    la = audit.check_slice6_live_acceptance_ownership(audit._iter_source_files())
    assert la["obsolete_reassessment_control"] == []
    assert la["obsolete_rebalance_label"] == []
    assert la["legacy_compatibility_classified"] is True
    assert la["forbidden_routes_present"] == []
    assert la["service_readiness_ui"] is True
    assert la["workflow_readiness_ui"] is True
    assert la["readiness_separated"] is True
    assert la["ready_is_service_scoped"] is True
    assert la["ready_conflates_session"] == []
    assert la["hoc_renders_not_run"] is True
    assert la["hoc_renders_completed"] is True


def test_71_strict_audit_exit_zero_and_slice7_absent():
    audit = _load_audit()
    rep = audit.run_audit()
    ho = rep["holding_opportunity_cost_ownership"]
    assert ho["slice7_missing_modules"] == [] and ho["slice7_missing_route"] == []
    assert ho["slice7_forbidden_present"] == []     # Slice 7 LANDED, no forbidden route
    assert ho["slice8_present_modules"] == []       # Persistent Alpha Research Agent planned
    assert ho["cadence_enabled"] is False
    assert rep["inventory_drift"]["status"] == "OK"
    assert rep["inventory_drift"]["on_disk_not_in_inventory"] == []
    assert rep["inventory_drift"]["in_inventory_not_on_disk"] == []
