"""tests/test_slice3_1_live_acceptance.py — Phase 29D.1 Slice 3 LIVE ACCEPTANCE completion.

Deterministic, offline tests for the corrective completion of the first live
acceptance:

  * post-close session confirmation (no false holiday inference; NON_SESSION only via
    an AUTHORITATIVE source; WAITING_FOR_OWNED_DATA with calendar_policy_degraded);
  * execution-plan precedence (WAITING_FOR_OWNED_DATA outranks research blockers);
  * the canonical monthly momentum input adapter (api.monthly_momentum_input);
  * the canonical target-calculation owner (no NO_REFRESH_OWNER);
  * the live-acceptance fixture (D-1 waiting → D executable);
  * the UI / API contract, the architecture guard, and the safety invariants.

Every read model and every provider / write seam is injected. NO network, provider,
prediction, Daily Close, operational-ledger write, order / signal / decision / fill,
or REAL research-artifact mutation occurs — the monthly adapter writes ONLY under a
per-test ``tmp_path`` inputs dir, never the real inputs store.
"""
from __future__ import annotations

import copy
import csv
import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from paper_trader.engine import market_session as ms
from paper_trader.api import data_freshness as df
from paper_trader.api import daily_research_cycle as drc
from paper_trader.api import workflow_state as ws
from paper_trader.api import monthly_momentum_input as mmi

ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent.parent
UI = ROOT / "api" / "ui" / "index.html"

# D = 2026-08-05 (Wed, weekday); D-1 = 2026-08-04 (Tue). After-cutoff clock on D
# (expected session = D). NOW_AFTER_CUTOFF_D1 is an after-cutoff clock on D-1
# (expected session = D-1) used for the "session ready at D-1" scenarios.
NOW_AFTER_CUTOFF_D = datetime(2026, 8, 5, 21, 45, tzinfo=timezone.utc)   # 17:45 EDT
NOW_BEFORE_CUTOFF_D = datetime(2026, 8, 5, 19, 0, tzinfo=timezone.utc)   # 15:00 EDT
NOW_AFTER_CUTOFF_D1 = datetime(2026, 8, 4, 21, 45, tzinfo=timezone.utc)  # 17:45 EDT on D-1
D = "2026-08-05"
D1 = "2026-08-04"


# --------------------------------------------------------------------------- #
# Injected read-model fixtures.
# --------------------------------------------------------------------------- #
def _op(desk=D1, nav=D1, target=D1, book="alpha_paper_book_1", **kw):
    ob = {"book_id": book, "book_label": "Alpha Paper Book #1",
          "current_status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
          "nav_as_of_date": nav, "desk_mark_date": desk, "latest_desk_mark_date": desk,
          "nav": 102000.0, "cash": 1500.0, "holdings_count": 25, "pending_order_count": 0,
          "current_target": {"alpha_market_date": target, "latest_completed_market_date": desk}}
    ob.update(kw)
    return {"operational_book": ob}


def _desk(spy=(("2026-07-31", 747.0), (D1, 771.3))):
    return {"series": {"SPY": [list(x) for x in spy]}, "latest_completed_date": spy[-1][0]}


def _inputs(price=D1, month="2026-08", fundamental="2026-05-22"):
    return {"market_as_of_date": price, "momentum_month": month,
            "fundamental_as_of_date": fundamental}


_CLOSE = {"market_date": D1, "done": True,
          "final_close_status": "DAILY_CLOSE_COMPLETE_HOLD", "status": "CLOSE_FINISHED"}
_FWD = {"latest_snapshot_date": D1, "snapshot_count": 6, "evidence_state": "X"}
_DAILY = {"status": "DAILY_STATUS_READY", "latest_valid_mark_date": D1}


def _fresh(*, now=NOW_AFTER_CUTOFF_D, op=None, inputs=None, desk=None, **kw):
    return df.load_data_freshness(
        now=now, operational=(op if op is not None else _op()),
        inputs=(inputs if inputs is not None else _inputs()),
        daily_status=dict(_DAILY), desk_marks=(desk if desk is not None else _desk()),
        daily_close_status=dict(_CLOSE), forward_status=copy.deepcopy(_FWD), **kw)


def _drc_status(tmp, *, now=NOW_AFTER_CUTOFF_D, op=None, inputs=None, desk=None, **kw):
    return drc.load_daily_research_cycle_status(
        drc_dir=str(tmp), now=now, operational=(op if op is not None else _op()),
        inputs=(inputs if inputs is not None else _inputs()),
        daily_status=dict(_DAILY), desk_marks=(desk if desk is not None else _desk()),
        close_progress=dict(_CLOSE), forward_status=copy.deepcopy(_FWD), **kw)


def _wf(*, now=NOW_AFTER_CUTOFF_D, op=None, inputs=None, desk=None,
        close_progress=None, **kw):
    return ws.load_workflow_state(
        now=now, operational=(op if op is not None else _op()),
        inputs=(inputs if inputs is not None else _inputs()),
        daily_status=dict(_DAILY), desk_marks=(desk if desk is not None else _desk()),
        close_progress=(close_progress if close_progress is not None else dict(_CLOSE)),
        forward_status=copy.deepcopy(_FWD),
        gate={"latest_completed_market_date": D1, "outcome": "NO_ACTION_TODAY",
              "target_state": "CURRENT_ALIGNED", "next_scheduled_full_review": "2026-09-01",
              "scheduled_review_due": False},
        target_readiness={"dates": {"alpha_market_date": D1}}, **kw)


# =========================================================================== #
# SESSION (1–8)
# =========================================================================== #
def test_s1_before_cutoff_waits_for_session_close(tmp_path):
    s = _drc_status(tmp_path, now=NOW_BEFORE_CUTOFF_D)
    assert s["state"] == drc.WAITING_FOR_SESSION_CLOSE and s["executable"] is False


def test_s2_after_cutoff_with_prior_day_data_waits_for_owned_data(tmp_path):
    fr = _fresh()
    assert fr["market_session"]["session_status"] == ms.WAITING_FOR_OWNED_DATA
    assert fr["market_session"]["calendar_policy_degraded"] is True
    s = _drc_status(tmp_path)
    assert s["state"] == drc.WAITING_FOR_OWNED_DATA and s["executable"] is False


def test_s3_absence_of_d_data_is_not_a_holiday():
    fr = _fresh()
    sess = fr["market_session"]
    assert sess["session_status"] == ms.WAITING_FOR_OWNED_DATA
    assert sess["session_status"] != ms.NON_SESSION
    assert sess["authoritative_non_sessions"] == []


def test_s4_authoritative_calendar_holiday_permits_non_session():
    fr = _fresh(authoritative_non_sessions=[D])
    assert fr["market_session"]["session_status"] == ms.NON_SESSION
    assert fr["eligible_market_date"] == D1


def test_s5_provider_confirmed_non_session_permits_non_session():
    fr = _fresh(provider_confirmed_non_sessions=[D])
    assert fr["market_session"]["session_status"] == ms.NON_SESSION
    assert fr["eligible_market_date"] == D1


def test_s6_market_d_but_benchmark_d1_remains_waiting():
    # Owned holdings marks reach D, but the benchmark (SPY) only reaches D-1.
    op = _op(desk=D, nav=D, target=D)
    desk = _desk(spy=(("2026-07-31", 747.0), (D1, 771.3)))   # SPY latest is D-1
    fr = _fresh(op=op, desk=desk)
    assert fr["market_session"]["session_status"] == ms.WAITING_FOR_OWNED_DATA


def test_s7_market_and_benchmark_d_confirm_the_session():
    op = _op(desk=D, nav=D, target=D)
    desk = _desk(spy=((D1, 771.3), (D, 773.0)))              # SPY reaches D
    fr = _fresh(op=op, desk=desk, inputs=_inputs(price=D, month="2026-08"))
    assert fr["market_session"]["session_status"] == ms.SESSION_READY
    assert fr["eligible_market_date"] == D


def test_s8_prior_close_remains_valid_while_waiting():
    fr = _fresh()
    close_row = next(r for r in fr["source_freshness"] if r["source_id"] == "latest_daily_close")
    assert close_row["as_of_date"] == D1   # the prior completed close is unchanged


# =========================================================================== #
# PRIORITY (9–12)
# =========================================================================== #
def test_p9_waiting_for_owned_data_outranks_research_blockers():
    # Session unresolved AND a research cycle status that (stale) claims BLOCKED must
    # still resolve to WAITING_FOR_OWNED_DATA — the research blocker cannot outrank it.
    r = _wf(research_cycle={"state": "BLOCKED",
                            "blockers": [{"code": "UNSUPPORTED_AUTOMATIC_REFRESH"}]})
    assert r["overall_state"] == ws.WAITING_FOR_OWNED_DATA


def test_p10_prior_session_inputs_not_executed_while_d_unresolved(tmp_path):
    # The cycle does NOT plan/execute the D-1 research inputs while D is unresolved.
    s = _drc_status(tmp_path)
    assert s["state"] == drc.WAITING_FOR_OWNED_DATA
    assert s["executable"] is False
    assert not (s.get("completed_steps") or [])


def test_p11_missing_owner_blocks_only_after_session_confirmation(tmp_path):
    # While waiting, no NO_REFRESH_OWNER / blocker is surfaced as the state.
    s = _drc_status(tmp_path)
    assert s["state"] == drc.WAITING_FOR_OWNED_DATA
    assert not any(b.get("code") == "NO_REFRESH_OWNER" for b in (s.get("blockers") or []))


def test_p12_inconsistent_state_retains_highest_priority():
    r = _wf(active_book_override=[_op(), _op(book="other")])
    assert r["overall_state"] == ws.INCONSISTENT_STATE


# =========================================================================== #
# MONTHLY OWNER (13–22)
# =========================================================================== #
def _write_mom(tmp, month, asof):
    p = Path(tmp) / "current_momentum_scores.csv"
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(mmi.REQUIRED_COLUMNS))
        w.writeheader()
        for tk in ("AAA", "BBB", "CCC"):
            w.writerow({"ticker": tk, "mom_6_1": "0.1", "is_member": "1", "sector": "Tech",
                        "market_as_of_date": asof, "month_label": month})
    return p


def _emitter(*, rows_val="0.2", **overrides):
    def fn(*, month, eligible, inputs_dir, flags=None):
        rows = [{"ticker": tk, "mom_6_1": rows_val, "is_member": "1", "sector": "Tech",
                 "market_as_of_date": eligible, "month_label": month}
                for tk in ("AAA", "BBB", "CCC")]
        art = {"month_label": month, "market_as_of_date": eligible, "rows": rows,
               "source": "test.fake_emitter"}
        art.update(overrides)
        return art
    return fn


def test_m13_monthly_owner_discovered(tmp_path):
    _write_mom(tmp_path, "2026-07", "2026-07-31")
    st = mmi.monthly_status(eligible=D, inputs_dir=str(tmp_path))
    assert st["authoritative_owner"] == "api.monthly_momentum_input"
    assert st["current_month"] == "2026-07"


def test_m14_monthly_adapter_selected_when_due(tmp_path):
    _write_mom(tmp_path, "2026-07", "2026-07-31")
    r = mmi.emit_if_due(eligible=D, inputs_dir=str(tmp_path), emitter_fn=_emitter())
    assert r["status"] == mmi.S_EMITTED and r["performed_write"] is True


def test_m15_monthly_adapter_skipped_when_current(tmp_path):
    _write_mom(tmp_path, "2026-08", "2026-08-01")
    r = mmi.emit_if_due(eligible=D, inputs_dir=str(tmp_path), emitter_fn=_emitter())
    assert r["status"] == mmi.S_CURRENT and r["performed_write"] is False


def test_m16_identical_artifact_reused(tmp_path):
    _write_mom(tmp_path, "2026-07", "2026-07-31")
    r1 = mmi.emit_if_due(eligible=D, inputs_dir=str(tmp_path), emitter_fn=_emitter())
    assert r1["status"] == mmi.S_EMITTED
    r2 = mmi.emit_if_due(eligible=D, inputs_dir=str(tmp_path), emitter_fn=_emitter())
    assert r2["status"] == mmi.S_CURRENT and r2["performed_write"] is False


def test_m17_conflicting_artifact_rejected(tmp_path):
    # A persisted month AHEAD of the eligible month is a conflict (never overwritten).
    _write_mom(tmp_path, "2026-09", "2026-09-01")
    r = mmi.emit_if_due(eligible=D, inputs_dir=str(tmp_path), emitter_fn=_emitter())
    assert r["status"] == mmi.S_CONFLICT and r["performed_write"] is False


def test_m18_intramonth_approximation_forbidden(tmp_path):
    _write_mom(tmp_path, "2026-07", "2026-07-31")
    r = mmi.emit_if_due(eligible=D, inputs_dir=str(tmp_path),
                        emitter_fn=_emitter(approximated=True))
    assert r["status"] == mmi.S_INVALID
    assert any("INTRAMONTH" in e for e in r["errors"])


def test_m19_backdating_forbidden(tmp_path):
    # Emitter returns a month older than the due month (a backdate / period mismatch).
    _write_mom(tmp_path, "2026-07", "2026-07-31")

    def bad(*, month, eligible, inputs_dir):
        return _emitter()(month="2026-06", eligible=eligible, inputs_dir=inputs_dir)
    r = mmi.emit_if_due(eligible=D, inputs_dir=str(tmp_path), emitter_fn=bad)
    assert r["status"] == mmi.S_INVALID


def test_m20_output_period_and_provenance_validated(tmp_path):
    _write_mom(tmp_path, "2026-07", "2026-07-31")

    def future_prov(*, month, eligible, inputs_dir):
        return _emitter()(month=month, eligible="2026-08-20", inputs_dir=inputs_dir)
    r = mmi.emit_if_due(eligible=D, inputs_dir=str(tmp_path), emitter_fn=future_prov)
    assert r["status"] == mmi.S_INVALID and any("PROVENANCE" in e for e in r["errors"])


def test_m21_failure_blocks_scoring(tmp_path):
    # A DRC run whose monthly step blocks never invokes scoring.
    calls = {"score": 0}

    def score():
        calls["score"] += 1
        return {"status": "MHZ_READY"}
    # month boundary (input July, eligible D in August) with NO emitter → BLOCKED.
    r = drc.run_daily_research_cycle(
        confirm=drc.EXECUTE_CONFIRMATION, drc_dir=str(tmp_path), now=NOW_AFTER_CUTOFF_D,
        operational=_op(desk=D, nav=D, target=D),
        inputs=_inputs(price=D, month="2026-07"),
        daily_status=dict(_DAILY), desk_marks=_desk(spy=((D1, 1.0), (D, 1.1))),
        close_progress=dict(_CLOSE), forward_status=copy.deepcopy(_FWD),
        scoring_fn=score, monthly_emitter_fn=None)
    assert r["state"] == drc.BLOCKED and calls["score"] == 0


def test_m22_no_separate_operator_prerequisite_remains():
    # The DRC declares the canonical adapter owner (no 'external ... emitter' owner)
    # and the UI has no separate monthly-input primary action.
    src = (ROOT / "api" / "daily_research_cycle.py").read_text(encoding="utf-8")
    assert "api.monthly_momentum_input" in src
    assert "external research monthly momentum emitter" not in src
    html = UI.read_text(encoding="utf-8")
    assert "runMonthlyInputEmitter" not in html and "month-boundary-btn" not in html


# =========================================================================== #
# TARGET OWNER (23–30)
# =========================================================================== #
def _stale_target_freshness():
    # eligible D-1 (session ready), target_calc behind eligible → target_calculation STALE.
    op = _op(desk=D1, nav=D1, target="2026-07-31")
    return _fresh(op=op)


def test_t23_target_owner_discovered_in_registry():
    assert "target_calculation" in drc._REFRESH_OWNERS
    spec = drc._REFRESH_OWNERS["target_calculation"]
    assert spec["owner"] == "api.alpha_target.load_readiness"
    assert spec["prepared_downstream_by"] == drc.STEP_PREPARE_TARGET


def test_t24_no_no_refresh_owner_when_target_owner_exists():
    fr = _stale_target_freshness()
    plan = drc.build_execution_plan(fr)
    codes = [b.get("code") for b in plan["blockers"]]
    assert "NO_REFRESH_OWNER" not in codes
    tgt = next(r for r in fr["source_freshness"] if r["source_id"] == "target_calculation")
    assert tgt["status"] == df.STALE   # it IS stale, but it is not a NO_REFRESH_OWNER blocker


def test_t25_target_is_prepared_downstream_not_a_pre_scoring_step():
    fr = _stale_target_freshness()
    plan = drc.build_execution_plan(fr)
    prepared = [p["source_id"] for p in plan["prepared_downstream_inputs"]]
    assert "target_calculation" in prepared
    assert "target_calculation" not in [s["source_id"] for s in plan["refresh_steps"]]
    assert plan["plan_blocked"] is False


def test_t26_stale_target_does_not_block_the_plan():
    fr = _stale_target_freshness()
    plan = drc.build_execution_plan(fr)
    assert plan["plan_blocked"] is False


def test_t27_target_operationally_approved_not_silently_replaced(tmp_path):
    # A full cycle prepares the target but NEVER marks it operationally approved.
    r = _full_cycle(tmp_path)
    assert r["state"] in (drc.COMPLETE, drc.COMPLETE_WITH_EVIDENCE_GAP)
    assert r["target_operationally_approved"] is False


def _full_cycle(tmp_path):
    def score():
        return {"status": "MHZ_READY", "market_as_of_date": D1, "momentum_month": "2026-08",
                "fundamental_as_of_date": "2026-05-22",
                "scores": {"composite_sn": {t: {"eligible": True} for t in ("A", "B")},
                           "counts": {}},
                "combined": {"n_common": 2},
                "books": {"primary_book_id": "fundamental_momentum_50_50_top25", "books": {
                    "fundamental_momentum_50_50_top25": {"constituents": [
                        {"ticker": "A", "rank": 1, "weight": 0.5, "sector": "Tech"},
                        {"ticker": "B", "rank": 2, "weight": 0.5, "sector": "Fin"}]},
                    "fundamental_momentum_50_50_top50": {"constituents": []}}}}

    def target():
        return {"state": "READY_TO_CONFIRM", "dates": {"alpha_market_date": D1},
                "required_next_action": "PREVIEW_CONFIRM"}

    def capture(*, market_date, current, ops, downloader):
        return {"snapshots_expected": 1, "snapshots_created": 1, "snapshots_already_present": 0,
                "mandatory_active_snapshot_persisted": True,
                "evidence_status": "FORWARD_EVIDENCE_COMPLETE",
                "artifact_bundle_id": "fca_%s" % market_date, "artifact_hash": "h",
                "performed_write": True}

    def refresh(*, confirm, downloader, completed_through):
        return {"status": "ALPHA_TARGET_ALREADY_FRESH", "performed_write": False}

    def assess(*, today):
        return {"latest_completed_market_date": today, "outcome": "NO_ACTION_TODAY",
                "target_state": "CURRENT_ALIGNED", "headline": "No change."}

    def holding_opp(*, scoring=None, hoc_dir=None):   # Slice 6: hermetic stub (no I/O)
        return {"assessment": {"assessment_state": "READY", "assessment_hash": "hoc_stub",
                               "eligible_market_date": D1, "holding_reviews": [],
                               "recommendation_counts": {"HOLD": 0, "REDUCE": 0, "EXIT": 0,
                                                         "REPLACE": 0, "ADD": 0},
                               "data_quality": {"data_gaps": []}},
                "persistence": {"status": "CREATED", "artifact_id": "hoc_stub",
                                "persisted": True}}
    # Session ready at D-1 (both desk and SPY at D-1); a stale daily price/score input
    # is refreshed to D-1 (so the refresh step runs), momentum current for the month.
    return drc.run_daily_research_cycle(
        confirm=drc.EXECUTE_CONFIRMATION, drc_dir=str(tmp_path),
        now=NOW_AFTER_CUTOFF_D1,
        operational=_op(desk=D1, nav=D1, target=D1),
        inputs=_inputs(price="2026-08-03", month="2026-08"), daily_status=dict(_DAILY),
        desk_marks=_desk(spy=(("2026-07-31", 1.0), (D1, 1.1))),
        close_progress={"market_date": "2026-07-31", "done": False, "status": "X"},
        forward_status=copy.deepcopy(_FWD),
        daily_refresh_fn=refresh, scoring_fn=score, target_loader=target,
        evidence_capture_fn=capture, evidence_registry=[("m", "b", 25, "ACTIVE")],
        assessment_loader=assess, holding_opp_cost_fn=holding_opp,
        refresh_confirm_token="CONFIRM_ALPHA_TARGET_REFRESH",
        monthly_emitter_fn=None)


def test_t28_manual_review_policy_preserved(tmp_path):
    r = _full_cycle(tmp_path)
    assert r["safety"]["manual_review"] is True
    assert r["safety"]["automatic_promotion_allowed"] is False


def test_t29_30_target_failure_prevents_evidence_and_assessment(tmp_path):
    calls = {"evidence": 0, "assess": 0}

    def bad_target():
        raise RuntimeError("target boom")

    def cap(*, market_date, current, ops, downloader):
        calls["evidence"] += 1
        return {}

    def assess(*, today):
        calls["assess"] += 1
        return {}

    def score():
        return {"status": "MHZ_READY", "market_as_of_date": D1, "momentum_month": "2026-08",
                "fundamental_as_of_date": "2026-05-22",
                "scores": {"composite_sn": {"A": {"eligible": True}}, "counts": {}},
                "combined": {"n_common": 1},
                "books": {"primary_book_id": "fundamental_momentum_50_50_top25", "books": {
                    "fundamental_momentum_50_50_top25": {"constituents": [
                        {"ticker": "A", "rank": 1, "weight": 1.0, "sector": "Tech"}]},
                    "fundamental_momentum_50_50_top50": {"constituents": []}}}}

    def refresh(*, confirm, downloader, completed_through):
        return {"status": "ALPHA_TARGET_ALREADY_FRESH", "performed_write": False}
    r = drc.run_daily_research_cycle(
        confirm=drc.EXECUTE_CONFIRMATION, drc_dir=str(tmp_path),
        now=NOW_AFTER_CUTOFF_D1,
        operational=_op(desk=D1, nav=D1, target=D1),
        inputs=_inputs(price=D1, month="2026-08"), daily_status=dict(_DAILY),
        desk_marks=_desk(spy=(("2026-07-31", 1.0), (D1, 1.1))),
        close_progress={"market_date": "2026-07-31", "done": False, "status": "X"},
        forward_status=copy.deepcopy(_FWD),
        daily_refresh_fn=refresh, scoring_fn=score, target_loader=bad_target,
        evidence_capture_fn=cap, evidence_registry=[("m", "b", 25, "ACTIVE")],
        assessment_loader=assess, monthly_emitter_fn=None)
    assert r["state"] == drc.FAILED and r["failed_step"] == drc.STEP_PREPARE_TARGET
    assert calls["evidence"] == 0 and calls["assess"] == 0


# =========================================================================== #
# CYCLE (31–40)
# =========================================================================== #
def test_c31_confirmed_session_plus_safe_owners_becomes_executable(tmp_path):
    # Session ready at D-1, all inputs current → the cycle is executable.
    s = drc.load_daily_research_cycle_status(
        drc_dir=str(tmp_path), now=NOW_AFTER_CUTOFF_D1,
        operational=_op(desk=D1, nav=D1, target=D1),
        inputs=_inputs(price=D1, month="2026-08"), daily_status=dict(_DAILY),
        desk_marks=_desk(spy=(("2026-07-31", 1.0), (D1, 1.1))),
        close_progress={"market_date": "2026-07-31", "done": False, "status": "X"},
        forward_status=copy.deepcopy(_FWD))
    assert s["eligible_market_date"] == D1
    assert s["state"] == drc.NOT_STARTED and s["executable"] is True


def test_c32_one_click_runs_planned_steps_in_order(tmp_path):
    r = _full_cycle(tmp_path)
    assert r["completed_steps"] == list(drc.STEP_SEQUENCE)


def test_c33_idempotent_rerun_reuses_artifacts(tmp_path):
    r1 = _full_cycle(tmp_path)
    r2 = _full_cycle(tmp_path)
    assert r2["reused_existing_run"] is True and r2["run_id"] == r1["run_id"]


def test_c34_completion_transitions_to_ready_for_daily_close(tmp_path):
    _full_cycle(tmp_path)
    r = _wf(op=_op(desk=D1, nav=D1, target=D1), inputs=_inputs(price=D1, month="2026-08"),
            now=NOW_AFTER_CUTOFF_D1,
            desk=_desk(spy=(("2026-07-31", 1.0), (D1, 1.1))),
            close_progress={"market_date": "2026-07-31", "done": False, "status": "X"},
            research_cycle={"state": "COMPLETE"})
    assert r["overall_state"] == ws.READY_FOR_DAILY_CLOSE


def test_c40_no_daily_close_no_ledger_mutation(tmp_path):
    r = _full_cycle(tmp_path)
    s = r["safety"]
    assert s["ran_daily_close"] is False and s["wrote_to_operational_ledger"] is False
    assert s["changed_holdings"] is False and s["changed_cash_or_nav"] is False


# =========================================================================== #
# UI / API (41–48)
# =========================================================================== #
def test_u41_status_exposes_expected_and_confirmed_dates(tmp_path):
    s = _drc_status(tmp_path)
    assert "eligible_market_date" in s and "expected_completed_market_date" in s
    assert s["expected_completed_market_date"] == D


def test_u42_button_disabled_while_owned_data_unresolved(tmp_path):
    s = _drc_status(tmp_path)
    assert s["executable"] is False


def test_u43_button_enabled_only_from_backend_executable():
    html = UI.read_text(encoding="utf-8")
    render_start = html.find("function renderDailyResearchCycle")
    end = html.find("function runDailyResearchCycle")
    region = html[render_start:end]
    assert "btn.disabled = !d.executable" in region


def test_u44_no_ui_holiday_or_session_inference():
    html = UI.read_text(encoding="utf-8")
    start = html.find("function loadDailyResearchCycle")
    end = html.find("window.runDailyResearchCycle")
    region = html[start:end]
    for bad in ("new Date(", "Date.now(", "NON_SESSION", "likely_holiday",
                "authoritative_non_sessions", "calendar_policy_degraded"):
        assert bad not in region


def test_u46_47_one_status_loader_one_execution_function():
    html = UI.read_text(encoding="utf-8")
    assert html.count("function loadDailyResearchCycle") == 1
    assert html.count("function runDailyResearchCycle") == 1


def test_u48_badge_is_accurate_no_misleading_signal_claim():
    html = UI.read_text(encoding="utf-8")
    drc_start = html.find('id="drc-panel"')
    drc_end = html.find('id="drc-run-btn"')
    panel = html[drc_start:drc_end]
    assert "CREATES SIGNALS ONLY" not in panel
    assert "CREATES RESEARCH EVIDENCE ONLY" in panel
    # The module badges match.
    assert "CREATES RESEARCH EVIDENCE ONLY" in drc.SAFETY_BADGES
    assert "CREATES SIGNALS ONLY" not in drc.SAFETY_BADGES


# =========================================================================== #
# ARCHITECTURE / SAFETY (49–56)
# =========================================================================== #
def _audit():
    spec = importlib.util.spec_from_file_location(
        "audit_arch", ROOT / "scripts" / "audit_architecture.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_a49_canonical_owner_map_passes_audit():
    mod = _audit()
    la = mod.check_slice3_live_acceptance_ownership(mod._iter_source_files())
    assert la["non_session_requires_authoritative_source"] is True
    assert la["monthly_input_adapter_present"] is True
    assert la["monthly_input_owner_declared"] is True
    assert la["target_calculation_owner_declared"] is True
    assert la["monthly_adapter_forbidden_calls"] == []
    assert la["waiting_outranks_research_blockers"] is True


def test_a50_inventory_drift_zero():
    mod = _audit()
    d = mod.check_inventory_drift(mod._iter_source_files())
    assert d["on_disk_not_in_inventory"] == [] and d["in_inventory_not_on_disk"] == []


def test_a51_52_no_live_provider_or_prediction_in_monthly_adapter():
    src = (ROOT / "api" / "monthly_momentum_input.py").read_text(encoding="utf-8")
    for tok in ("requests.get(", "requests.post(", "httpx.", ":9000", "yfinance",
                "predict(", "prediction_client", "urllib.request"):
        assert tok not in src, tok


def test_a53_54_monthly_adapter_no_ledger_or_execution_calls():
    src = (ROOT / "api" / "monthly_momentum_input.py").read_text(encoding="utf-8")
    for tok in ("run_daily_close(", "run_fill_cycle(", "place_order(", "submit_order(",
                "Signal(", "TradeDecision(", "create_order("):
        assert tok not in src, tok


def test_a55_no_scheduler_change():
    src = (ROOT / "api" / "monthly_momentum_input.py").read_text(encoding="utf-8")
    assert "ScheduledTask" not in src and "schtasks" not in src


def test_a56_slice6_landed_slice7_not_implemented():
    # Slice 5 (Phase 29F) and Slice 6 (Holding Opportunity-Cost engine, Phase 29G) have
    # LANDED. The boundary has advanced: the NEXT slice — Slice 7 (Reallocation Proposal
    # engine) — is NOT started, and no Slice-7 owner module exists.
    roadmap = (ROOT / "docs" / "CONSOLIDATION_ROADMAP.md").read_text(encoding="utf-8")
    s5 = roadmap.index("## Slice 5")
    s6 = roadmap.index("## Slice 6")
    s7 = roadmap.index("## Slice 7")
    s8 = roadmap.index("## Slice 8")
    assert "LANDED (Phase 29F)" in roadmap[s5:s6]
    assert "LANDED (Phase 29G)" in roadmap[s6:s7]
    assert "LANDED" not in roadmap[s7:s8]
    # The Slice 6 owners exist; the Slice 7 owner does not.
    assert (ROOT / "engine" / "holding_opportunity_cost.py").exists()
    assert (ROOT / "api" / "holding_opportunity_cost.py").exists()
    assert not (ROOT / "api" / "portfolio_proposal.py").exists()


def test_frozen_monthly_status_vocabulary_stable():
    assert set(mmi.STATUS_VOCAB) == {
        "MONTHLY_INPUT_CURRENT", "MONTHLY_INPUT_EMITTED", "MONTHLY_INPUT_REUSED",
        "MONTHLY_EMITTER_UNAVAILABLE", "MONTHLY_INPUT_CONFLICT", "MONTHLY_INPUT_INVALID",
        "MONTHLY_INPUT_SOURCE_UNAVAILABLE"}
    assert mmi.MONTHLY_EMITTER_ACTION == drc.MONTHLY_EMITTER_ACTION


def test_non_session_added_to_session_vocabulary():
    assert "NON_SESSION" in ms.SESSION_STATUSES
    assert ms.NON_SESSION == "NON_SESSION"
