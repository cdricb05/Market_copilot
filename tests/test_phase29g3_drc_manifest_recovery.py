"""tests/test_phase29g3_drc_manifest_recovery.py — Phase 29G.3 first-live Slice 6 recovery.

Deterministic, offline coverage of the DRC terminal-manifest persistence/read-back
contract, the split-brain status reader (terminal manifest reflected / recovery
INCONSISTENT, never NOT_STARTED), the safe idempotent normal-path recovery (session-
stable identity; no duplicate evidence / HOC artifact; no operational mutation), the
pre-close portfolio-state consistency classification (PENDING_DAILY_CLOSE vs genuine
inconsistency), the preserved HOC DEGRADED gaps, the workflow readiness correction, the
read-only operator acceptance scripts, and the extended architecture guards.

Every read model / write boundary is injected; NO network, provider, prediction, real
cycle, Daily Close, operational-ledger write, order / fill, or model promotion occurs.
Manifests are written under a per-test ``tmp_path`` (never a production root).
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from paper_trader.api import daily_research_cycle as drc
from paper_trader.api import portfolio_state as ps
from paper_trader.api import workflow_state as ws

# Reuse the verified deterministic harnesses from the Slice-3 and Slice-5 suites
# (``tests`` is a package). No production data is touched.
from tests.test_slice3_daily_research_cycle import (  # noqa: E402
    Fakes, _inputs, _run, _status, _DESK, _DAILY, _FWD)
from tests.test_slice5_portfolio_state import (  # noqa: E402
    _operational as _ps_op, _performance as _ps_perf, _gate as _ps_gate,
    _forward as _ps_fwd, _fills as _ps_fills)

ROOT = Path(__file__).resolve().parent.parent
ACCEPT_DIR = Path(r"D:\Temp\paper_trader_slice6_first_live_acceptance")


# --------------------------------------------------------------------------- #
# Helpers.
# --------------------------------------------------------------------------- #
def _degraded_hoc_fn():
    """A holding-opportunity-cost seam that returns a DEGRADED assessment with a real
    artifact reference + documented data gaps (mirrors the first live August 6 run)."""
    def _fn(*, scoring=None, hoc_dir=None):
        return {"assessment": {
            "assessment_state": "DEGRADED", "assessment_hash": "deg_hash_29g3",
            "eligible_market_date": "2026-08-04", "holding_reviews": [{"ticker": "A"}],
            "recommendation_counts": {"HOLD": 12, "REDUCE": 1, "EXIT": 8, "REPLACE": 4,
                                      "ADD": 9},
            "data_quality": {"data_gaps": ["LIQUIDITY_UNAVAILABLE",
                                           "PRIOR_RANK_UNAVAILABLE"],
                             "holdings_evaluated": 25}},
            "persistence": {"status": "CREATED", "artifact_id": "hoc_deg_29g3",
                            "persisted": True}}
    return _fn


def _complete(tmp, **kw):
    return _run(tmp, inputs=_inputs(price="2026-08-03", month="2026-08"), **kw)


# =========================================================================== #
# WORKSTREAM B — terminal persistence / read-back contract (items 1–5)
# =========================================================================== #
def test_01_terminal_response_persists_manifest_before_returning(tmp_path):
    r, _ = _complete(tmp_path)
    assert r["state"] == drc.COMPLETE
    # The manifest file already exists the moment the run returns COMPLETE.
    assert (tmp_path / "runs" / ("%s.json" % r["run_id"])).exists()


def test_02_manifest_write_is_atomic_no_tmp_leftovers(tmp_path):
    r, _ = _complete(tmp_path)
    assert r["state"] == drc.COMPLETE
    assert not list((tmp_path / "runs").glob("*.tmp"))  # os.replace atomic move
    assert not list(tmp_path.glob("*.tmp"))


def test_03_run_index_updated_atomically(tmp_path):
    r, _ = _complete(tmp_path)
    idx = drc._load_index(str(tmp_path))
    entry = idx.get(r["eligible_market_date"])
    assert entry and entry["run_id"] == r["run_id"] and entry["state"] == drc.COMPLETE


def test_04_read_back_verification_of_terminal_manifest(tmp_path):
    r, _ = _complete(tmp_path)
    back = drc._load_run(r["run_id"], str(tmp_path))
    assert back is not None
    assert back["run_id"] == r["run_id"] and back["state"] == drc.COMPLETE
    assert back["opportunity_cost_assessment_hash"] == r["opportunity_cost_assessment_hash"]


def test_05_manifest_write_failure_never_returns_complete(tmp_path, monkeypatch):
    # Force the manifest persist to silently no-op → the read-back cannot confirm it.
    monkeypatch.setattr(drc, "_save_run", lambda rec, drc_dir=None: None)
    r, _ = _complete(tmp_path)
    assert r["state"] != drc.COMPLETE
    assert r["state"] == drc.INCONSISTENT
    assert any(b.get("code") == drc.MANIFEST_PERSISTENCE_UNVERIFIED
               for b in r.get("blockers", []))
    # The durable downstream references are PRESERVED for recovery (not discarded).
    assert r.get("scoring") is not None and r.get("holding_opportunity_cost") is not None


def test_05b_incomplete_manifest_contract_downgrades_from_complete(tmp_path, monkeypatch):
    # A COMPLETE record missing its required completed_at is not durable → never COMPLETE.
    real = drc._contract

    def _blank_completed_at(*a, **k):
        rec = real(*a, **k)
        if rec.get("state") in drc._COMPLETED:
            rec["completed_at"] = None
        return rec
    monkeypatch.setattr(drc, "_contract", _blank_completed_at)
    r, _ = _complete(tmp_path)
    assert r["state"] == drc.INCONSISTENT
    assert any(b.get("code") == drc.MANIFEST_CONTRACT_INCOMPLETE
               for b in r.get("blockers", []))


# =========================================================================== #
# WORKSTREAM C — status reader: same root/date/book/key; terminal preferred (6–14)
# =========================================================================== #
def test_06_post_and_get_use_the_same_artifact_root(tmp_path):
    r, _ = _complete(tmp_path)
    s = _status(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    # Both resolve the run under the SAME configured root (tmp_path).
    assert s["run_id"] == r["run_id"]
    assert (tmp_path / "runs" / ("%s.json" % s["run_id"])).exists()


def test_07_post_and_get_use_the_same_eligible_date(tmp_path):
    r, _ = _complete(tmp_path)
    s = _status(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert s["eligible_market_date"] == r["eligible_market_date"]


def test_08_post_and_get_use_the_same_active_book(tmp_path):
    r, _ = _complete(tmp_path)
    s = _status(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert s["active_book_id"] == r["active_book_id"] == "alpha_paper_book_1"


def test_09_post_and_get_use_the_same_idempotency_key(tmp_path):
    r, _ = _complete(tmp_path)
    s = _status(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert s["idempotency_key"] == r["idempotency_key"]


def test_10_status_prefers_the_persisted_terminal_manifest(tmp_path):
    r, _ = _complete(tmp_path)
    s = _status(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert s["state"] == drc.COMPLETE and s["reused_existing_run"] is True
    assert s["terminal"] is True and s["executable"] is False


def test_11_drifted_fast_input_cannot_reset_terminal_manifest_to_not_started(tmp_path):
    # The real August 6 defect: the cycle refreshed the fast inputs it hashes, so a later
    # status recomputed a DIFFERENT raw hash than the run persisted. Simulate that drift by
    # rewriting the stored raw hash to a value the current status will NOT recompute; the
    # persisted terminal manifest must STILL be reflected as COMPLETE (never NOT_STARTED).
    r, _ = _complete(tmp_path)
    rec = drc._load_run(r["run_id"], str(tmp_path))
    rec["input_contract_hash"] = "DRIFTED_HASH_000000000000"
    drc._save_run(rec, str(tmp_path))
    s = _status(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert s["state"] == drc.COMPLETE and s["state"] != drc.NOT_STARTED
    assert s["reused_existing_run"] is True
    # The reflection surfaces the manifest's STORED (now drifted) hash verbatim — proof it
    # reflected the persisted terminal manifest rather than recomputing / resetting.
    assert s["input_contract_hash"] == "DRIFTED_HASH_000000000000"


def test_12_restart_preserves_terminal_status(tmp_path):
    r, _ = _complete(tmp_path)
    # A fresh, stateless status call (simulating a backend restart) reads the file store.
    s = _status(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert s["state"] == drc.COMPLETE and s["run_id"] == r["run_id"]


def test_13_missing_manifest_with_terminal_hoc_artifact_is_inconsistent_not_not_started(tmp_path):
    probe = lambda **kw: {"present": True, "state": "DEGRADED",  # noqa: E731
                          "assessment_hash": "H",
                          "recommendation_counts": {"HOLD": 1},
                          "data_gaps": ["LIQUIDITY_UNAVAILABLE"]}
    s = _status(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"),
                downstream_artifacts_fn=probe)
    assert s["state"] == drc.INCONSISTENT and s["state"] != drc.NOT_STARTED
    # The HOC evidence is surfaced (never "assessment not run").
    assert s["opportunity_cost_selected"] is True
    assert s["opportunity_cost_assessment_hash"] == "H"


def test_14_exact_missing_manifest_reason_code(tmp_path):
    probe = lambda **kw: {"present": True, "assessment_hash": "H",  # noqa: E731
                          "recommendation_counts": {}, "data_gaps": []}
    s = _status(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"),
                downstream_artifacts_fn=probe)
    codes = [b.get("code") for b in s.get("blockers", [])]
    assert drc.TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST in codes
    ra = s.get("required_actions", [])
    assert any(a.get("confirmation_required") == drc.EXECUTE_CONFIRMATION for a in ra)


def test_14b_no_downstream_artifact_returns_not_started_not_inconsistent(tmp_path):
    # WITHOUT a downstream artifact a clean session is executable NOT_STARTED (not a false
    # recovery INCONSISTENT).
    s = _status(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"),
                downstream_artifacts_fn=lambda **kw: {"present": False})
    assert s["state"] == drc.NOT_STARTED and s["executable"] is True


# =========================================================================== #
# WORKSTREAM D — safe idempotent recovery (items 15–26)
# =========================================================================== #
def test_15_to_21_recovery_reuses_all_outputs_and_creates_no_duplicate(tmp_path):
    r1, _ = _complete(tmp_path)
    assert r1["state"] == drc.COMPLETE
    f2 = Fakes()
    r2, f2 = _run(tmp_path, f2, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r2["reused_existing_run"] is True and r2["run_id"] == r1["run_id"]
    # 15/16/17/18 reuse scoring / target / evidence / HOC artifact verbatim.
    assert r2["scoring"] == r1["scoring"]
    assert r2["target"] == r1["target"]
    assert r2["evidence"] == r1["evidence"]
    assert r2["holding_opportunity_cost"] == r1["holding_opportunity_cost"]
    # 19 assessment hash unchanged; 20/21 no duplicate HOC / evidence (seams not called).
    assert r2["opportunity_cost_assessment_hash"] == r1["opportunity_cost_assessment_hash"]
    assert f2.calls == {"refresh": 0, "monthly": 0, "score": 0, "target": 0,
                        "evidence": 0, "assess": 0}


def test_15b_recovery_after_fast_input_drift_still_reuses_by_session_identity(tmp_path):
    # The real recovery: owned daily inputs advanced since the run, so the raw contract
    # hash differs, but the session-stable identity matches → reuse (no re-run).
    r1, _ = _complete(tmp_path)
    f2 = Fakes()
    r2, f2 = _run(tmp_path, f2, inputs=_inputs(price="2026-08-04", month="2026-08"))
    assert r2["reused_existing_run"] is True and r2["run_id"] == r1["run_id"]
    assert f2.calls["score"] == 0 and f2.calls["evidence"] == 0


def test_22_recovery_writes_no_operational_ledger(tmp_path):
    r1, _ = _complete(tmp_path)
    r2, _ = _run(tmp_path, Fakes(), inputs=_inputs(price="2026-08-03", month="2026-08"))
    s = r2["safety"]
    assert s["wrote_to_operational_ledger"] is False and s["wrote_to_database"] is False
    assert s["changed_holdings"] is False and s["changed_cash_or_nav"] is False


def test_23_recovery_confirms_no_target(tmp_path):
    r, _ = _complete(tmp_path)
    assert r["target_operationally_approved"] is False


def test_24_recovery_creates_no_order_or_fill(tmp_path):
    r, _ = _complete(tmp_path)
    s = r["safety"]
    assert s["created_orders"] is False and s["created_fills"] is False
    assert s["created_signals"] is False and s["created_trade_decisions"] is False


def test_25_recovery_persists_exactly_one_terminal_manifest(tmp_path):
    _complete(tmp_path)
    _run(tmp_path, Fakes(), inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert len(list((tmp_path / "runs").glob("*.json"))) == 1


def test_26_get_status_is_terminal_after_recovery(tmp_path):
    _complete(tmp_path)
    _run(tmp_path, Fakes(), inputs=_inputs(price="2026-08-03", month="2026-08"))
    s = _status(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert s["terminal"] is True and s["state"] == drc.COMPLETE


def _wf(*, drc_state="COMPLETE", pending=0, assessment_date="2026-08-03",
        hoc_available=True, hoc_state="DEGRADED", drc_blockers=None, close=None):
    """Known-good CONSISTENT workflow scenario at eligible 2026-08-04 (owned data all
    2026-08-04, close not yet run for the eligible session), with a STALE legacy gate
    assessment so the reassessment currency is exercised."""
    op = {"operational_book": {"book_id": "alpha_paper_book_1", "book_label": "B1",
          "current_status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
          "nav_as_of_date": "2026-08-04", "desk_mark_date": "2026-08-04",
          "latest_desk_mark_date": "2026-08-04", "nav": 1.0, "cash": 1.0,
          "holdings_count": 25, "pending_order_count": pending,
          "current_target": {"alpha_market_date": "2026-08-04",
                             "latest_completed_market_date": "2026-08-04"}}}
    gate = {"latest_completed_market_date": assessment_date, "outcome": "NO_ACTION_TODAY",
            "target_state": "CURRENT_ALIGNED", "next_scheduled_full_review": "2026-09-01",
            "scheduled_review_due": False,
            "opportunity_cost_available": hoc_available, "opportunity_cost_state": hoc_state,
            "opportunity_cost_assessment_hash": "H",
            "opportunity_cost_recommendation_counts": {"HOLD": 12},
            "opportunity_cost_data_gaps": ["LIQUIDITY_UNAVAILABLE"]}
    rc = {"state": drc_state, "eligible_market_date": "2026-08-04"}
    if drc_blockers is not None:
        rc["blockers"] = drc_blockers
    return ws.load_workflow_state(
        reference_today="2026-08-05", operational=op,
        inputs=_inputs(price="2026-08-04", month="2026-08"),
        desk_marks=dict(_DESK), daily_status=dict(_DAILY), forward_status=dict(_FWD),
        close_progress=(close or {"market_date": "2026-08-04", "done": False,
                                  "status": "X"}),
        gate=gate, target_readiness={"dates": {"alpha_market_date": "2026-08-04"}},
        research_cycle=rc)


def test_27_workflow_ready_for_daily_close_after_completed_cycle_with_stale_gate(tmp_path):
    # Item 27 + Workstream G: a COMPLETE cycle + a current HOC assessment satisfies the
    # reassessment even when the legacy gate date lags (STALE) — the operator reviews the
    # HOC assessment and proceeds to the Daily Close (no separate reassessment control).
    r = _wf(drc_state="COMPLETE")
    assert r["portfolio_assessment_state"]["assessment_status"] == ws.ASSESS_STALE
    assert r["overall_state"] == ws.READY_FOR_DAILY_CLOSE
    assert r["canonical_operator_state"] == ws.COS_DEGRADED
    assert r["primary_action"]["action_code"] == ws.ACTION_RUN_DAILY_CLOSE
    queued = [q["action_code"] for q in r["queued_actions"]]
    assert ws.ACTION_RUN_PORTFOLIO_REASSESSMENT not in queued
    assert ws.ACTION_REVIEW_HOC in queued


def test_27a_stale_gate_without_completed_cycle_still_requires_reassessment(tmp_path):
    # Guard: the reassessment is satisfied ONLY by a completed cycle + current HOC.
    #
    # STAGE 22 (close precedence): this world also has an UNCLOSED eligible session
    # (close_progress done=False), and the canonical normal cycle runs the Daily Close
    # BEFORE the research cycle for a session — the close is what advances owned marks,
    # settles NEXT_CLOSE paper orders and records NAV, so research produced ahead of it
    # describes a portfolio that is about to change. The overall state is therefore the
    # close; the reassessment requirement is still recorded and still unsatisfied, and
    # it becomes the operator's action as soon as the close completes.
    r = _wf(drc_state="NOT_STARTED", hoc_available=False)
    assert r["overall_state"] == ws.READY_FOR_DAILY_CLOSE
    assert r["portfolio_assessment_state"]["assessment_status"] == ws.ASSESS_STALE
    assert r["normal_cycle"]["current_stage"] == "DAILY_CLOSE"
    assert r["normal_cycle"]["next_stage"] == "DAILY_RESEARCH_CYCLE"
    # ...and with the close COMPLETE for that session, the missing assessment is what
    # the operator is asked for — exactly one action, never two.
    r2 = _wf(drc_state="NOT_STARTED", hoc_available=False,
             close={"market_date": "2026-08-04", "done": True,
                    "final_close_status": "DAILY_CLOSE_COMPLETE_HOLD"})
    assert r2["overall_state"] == ws.RESEARCH_CYCLE_REQUIRED
    assert r2["normal_cycle"]["current_stage"] == "DAILY_RESEARCH_CYCLE"


def test_27b_missing_manifest_recovery_surfaces_inconsistent_not_not_run(tmp_path):
    # Workstream G: a DRC INCONSISTENT (recovery required) is a genuine inconsistency,
    # never "the assessment has not run" / NOT_STARTED.
    r = _wf(drc_state="INCONSISTENT",
            drc_blockers=[{"code": drc.TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST}])
    assert r["overall_state"] == ws.INCONSISTENT_STATE
    codes = [b.get("code") for b in r["blockers"]]
    assert drc.TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST in codes


# =========================================================================== #
# WORKSTREAM E — pre-close portfolio consistency (items 28–33)
# =========================================================================== #
def _pf(*, valuation, close, eligible, benchmark=None, consistency="CONSISTENT",
        nav=100000.0, book_id="alpha_paper_book_1", fresh_book_id=None):
    benchmark = benchmark or valuation
    op = _ps_op(nav=nav)
    cs = op["canonical_state"]
    ob = op["operational_book"]
    cs["valuation_date"] = valuation
    cs["desk_mark_date"] = valuation
    ob["nav_as_of_date"] = valuation
    ob["desk_mark_date"] = valuation
    ob["current_target"]["alpha_market_date"] = valuation
    fresh = {
        "eligible_market_date": eligible,
        "active_book": {"active_book_id": (fresh_book_id or book_id),
                        "active_book_name": "Alpha Paper Book #1",
                        "active_book_authoritative_owner": "api.operational_book",
                        "ambiguous": False},
        "consistency_status": consistency, "consistency_violations": [],
        "source_freshness": [
            {"source_id": "operational_valuation", "as_of_date": valuation},
            {"source_id": "desk_marks", "as_of_date": valuation},
            {"source_id": "benchmark", "as_of_date": benchmark},
            {"source_id": "latest_daily_close", "as_of_date": close},
            {"source_id": "target_calculation", "as_of_date": valuation},
        ],
    }
    perf = _ps_perf()
    perf["rows"][-1]["date"] = benchmark
    return ps.load_portfolio_state(operational=op, freshness=fresh, performance=perf,
                                   gate=_ps_gate(), forward_status=_ps_fwd(),
                                   fills=_ps_fills())


def test_28_one_session_gap_is_pending_close_not_inconsistent(tmp_path):
    r = _pf(valuation="2026-08-06", close="2026-08-05", eligible="2026-08-06")
    chk = {c["code"]: c for c in r["consistency"]["checks"]}
    vc = chk["VALUATION_VS_DAILY_CLOSE"]
    assert vc["result"] == "PASS" and vc["classification"] == ps.EXPECTED_PRE_CLOSE_GAP
    assert r["consistency"]["pending_daily_close"] is True
    assert r["state"] == ps.STATE_READY_WITH_PENDING_CLOSE
    assert r["consistency"]["status"] != ps.INCONSISTENT


def test_29_two_session_gap_remains_inconsistent(tmp_path):
    r = _pf(valuation="2026-08-06", close="2026-08-04", eligible="2026-08-06")
    chk = {c["code"]: c for c in r["consistency"]["checks"]}
    assert chk["VALUATION_VS_DAILY_CLOSE"]["result"] == "FAIL"
    assert r["state"] == ps.STATE_INCONSISTENT


def test_30_future_dated_valuation_remains_inconsistent(tmp_path):
    r = _pf(valuation="2026-08-07", close="2026-08-06", eligible="2026-08-06")
    chk = {c["code"]: c for c in r["consistency"]["checks"]}
    assert chk["VALUATION_VS_DAILY_CLOSE"]["result"] == "FAIL"
    assert chk["VALUATION_VS_DAILY_CLOSE"].get("future_dated") is True
    assert r["state"] == ps.STATE_INCONSISTENT


def test_31_nav_mismatch_remains_inconsistent(tmp_path):
    r = _pf(valuation="2026-08-06", close="2026-08-05", eligible="2026-08-06", nav=99000.0)
    chk = {c["code"]: c for c in r["consistency"]["checks"]}
    assert chk["NAV_RECONCILIATION"]["result"] == "FAIL"
    assert r["state"] == ps.STATE_INCONSISTENT


def test_32_active_book_mismatch_remains_inconsistent(tmp_path):
    r = _pf(valuation="2026-08-06", close="2026-08-05", eligible="2026-08-06",
            fresh_book_id="other_book")
    chk = {c["code"]: c for c in r["consistency"]["checks"]}
    assert chk["ACTIVE_BOOK_IDENTITY"]["result"] == "FAIL"
    assert r["state"] == ps.STATE_INCONSISTENT


def test_33_post_close_alignment_is_consistent_ready(tmp_path):
    r = _pf(valuation="2026-08-06", close="2026-08-06", eligible="2026-08-06")
    chk = {c["code"]: c for c in r["consistency"]["checks"]}
    assert chk["VALUATION_VS_DAILY_CLOSE"]["result"] == "PASS"
    assert r["consistency"]["pending_daily_close"] is False
    assert r["consistency"]["status"] == ps.CONSISTENT and r["state"] == ps.STATE_READY


# =========================================================================== #
# WORKSTREAM F — HOC DEGRADED + gaps preserved (items 34–35)
# =========================================================================== #
def test_34_hoc_degraded_remains_degraded():
    p = ws.build_holding_opportunity_cost_presentation(
        state="DEGRADED", available=True, eligible_date="2026-08-06",
        recommendation_counts={"HOLD": 12, "EXIT": 8}, assessment_hash="H",
        data_gaps=["LIQUIDITY_UNAVAILABLE", "PRIOR_RANK_UNAVAILABLE"])
    assert p["state"] == "DEGRADED"
    assert p["canonical_operator_state"] == ws.COS_DEGRADED
    assert p["is_primary_decision"] is True and p["has_assessment"] is True


def test_35_optional_gaps_remain_visible():
    gaps = ["LIQUIDITY_UNAVAILABLE", "PRIOR_RANK_UNAVAILABLE"]
    p = ws.build_holding_opportunity_cost_presentation(
        state="DEGRADED", available=True, eligible_date="2026-08-06",
        recommendation_counts={"HOLD": 12}, assessment_hash="H", data_gaps=gaps)
    assert p["data_gaps"] == gaps
    assert "data gap" in p["headline"].lower()


# =========================================================================== #
# WORKSTREAM F/G — post-DRC readiness gate (items 36–39)
# =========================================================================== #
def test_36_gate_accepts_documented_optional_hoc_gaps():
    r = _wf(drc_state="COMPLETE", hoc_state="DEGRADED")
    assert r["overall_state"] == ws.READY_FOR_DAILY_CLOSE  # DEGRADED gaps do not block


def test_37_gate_rejects_nonterminal_drc():
    r = _wf(drc_state="SCORING_UNIVERSE")
    assert r["overall_state"] != ws.READY_FOR_DAILY_CLOSE
    assert r["overall_state"] == ws.RESEARCH_CYCLE_RUNNING


def test_38_gate_surfaces_pending_orders_for_rejection():
    r = _wf(drc_state="COMPLETE", pending=2)
    assert r["overall_state"] == ws.READY_FOR_DAILY_CLOSE
    assert r["operational_state"]["pending_orders"] == 2
    assert any(q["action_code"] == ws.ACTION_REVIEW_PENDING_ORDERS
               for q in r["queued_actions"])


def test_39_gate_exposes_artifact_and_eligible_for_mismatch_check():
    r = _wf(drc_state="COMPLETE")
    # The eligible session + the HOC assessment hash are both exposed so the acceptance
    # gate can reject an artifact/date mismatch.
    assert r["research_cycle_state"]["eligible_market_date"] is not None
    assert r["holding_opportunity_cost_presentation"]["assessment_hash"] == "H"


# =========================================================================== #
# WORKSTREAM H — operator acceptance scripts (items 40–41)
# =========================================================================== #
@pytest.mark.skipif(not ACCEPT_DIR.exists(), reason="acceptance-script dir not present")
@pytest.mark.parametrize("name", ["post_drc_acceptance.ps1", "pre_resume_drc.ps1"])
def test_40_acceptance_scripts_parse_and_reference_required_fields(name):
    txt = (ACCEPT_DIR / name).read_text(encoding="utf-8")
    assert txt.strip()
    # Uses the exact API response field names.
    assert "daily-research-cycle/status" in txt
    assert "holding-opportunity-cost" in txt


@pytest.mark.skipif(not ACCEPT_DIR.exists(), reason="acceptance-script dir not present")
@pytest.mark.parametrize("name", ["post_drc_acceptance.ps1", "pre_resume_drc.ps1"])
def test_41_acceptance_scripts_contain_no_write_request(name):
    low = (ACCEPT_DIR / name).read_text(encoding="utf-8").lower()
    for bad in ("-method post", "invoke-webrequest", ".post(", "httpclient"):
        assert bad not in low
    assert "invoke-restmethod -method get" in low


@pytest.mark.skipif(not ACCEPT_DIR.exists(), reason="acceptance-script dir not present")
def test_41b_post_gate_guards_present():
    txt = (ACCEPT_DIR / "post_drc_acceptance.ps1").read_text(encoding="utf-8")
    assert "not terminal-complete" in txt                      # rejects nonterminal DRC
    assert "pending paper orders present" in txt               # rejects pending orders
    assert "does not match" in txt                             # rejects artifact/date mismatch
    assert "READY_WITH_PENDING_CLOSE" in txt or "PENDING_CLOSE" in txt  # accepts pre-close


# =========================================================================== #
# ARCHITECTURE GUARDS (items 42–46)
# =========================================================================== #
def _load_audit():
    spec = importlib.util.spec_from_file_location(
        "audit_architecture_29g3", ROOT / "scripts" / "audit_architecture.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_42_architecture_audit_drc_manifest_recovery_clean():
    rep = _load_audit().run_audit()
    mr = rep["drc_manifest_recovery"]
    assert mr["sole_orchestrator"] is True and mr["competing_orchestrators"] == []
    assert mr["missing_terminal_persistence_tokens"] == []
    assert mr["terminal_read_back_present"] is True
    assert mr["mark_complete_defs"] == [] and mr["forbidden_recovery_routes"] == []
    assert mr["single_artifact_root"] is True
    assert mr["missing_status_reflect_tokens"] == []
    assert mr["separate_recovery_entry_defs"] == []
    assert mr["forbidden_execution_calls"] == []
    assert mr["missing_preclose_tokens"] == []
    assert mr["missing_genuine_inconsistency_tokens"] == []
    assert mr["hoc_data_gaps_explicit"] is True


def test_43_inventory_drift_zero():
    d = _load_audit().run_audit()["inventory_drift"]
    assert d["status"] == "OK"
    assert d["on_disk_not_in_inventory"] == [] and d["in_inventory_not_on_disk"] == []


def test_44_slice7_landed():
    mr = _load_audit().run_audit()["drc_manifest_recovery"]
    assert mr["slice7_missing_modules"] == [] and mr["slice7_missing_route"] == []
    assert mr["slice7_forbidden_present"] == []


def test_45_persistent_alpha_research_agent_remains_planned():
    mr = _load_audit().run_audit()["drc_manifest_recovery"]
    assert mr["slice8_present_modules"] == []


def test_46_cadence_remains_disabled():
    mr = _load_audit().run_audit()["drc_manifest_recovery"]
    assert mr["cadence_enabled"] is False
