"""
tests/test_command_center.py — Phase 14-A command-center view-model unit tests.

Fully offline and DB-free: exercises the PURE aggregation logic of
``api/command_center.py`` — next-action selection, capacity explanation
(including MAX_POSITIONS_REACHED), the safety block, the alpha slice (fed by a
real read-only decision-gate over a synthetic Phase 13-I artifact), and the
workflow slice — for every branch the endpoint can return. The DB-backed
endpoint wiring is covered separately in test_command_center_endpoint.py.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from paper_trader.api import command_center as cc
from paper_trader.api.current_alpha_decision_gate import load_current_alpha_decision_gate
from paper_trader.api.current_alpha_performance import BACKFILL_DIR_ENV

from tests.test_current_alpha_decision_gate import _write_backfill, _SIGNAL

_FRESH_TODAY = "2026-06-26"  # 1 day after the default artifact end -> not stale


@pytest.fixture
def bdir(tmp_path, monkeypatch) -> Path:
    d = tmp_path / "backfill"
    monkeypatch.setenv(BACKFILL_DIR_ENV, str(d))
    return d


# --------------------------------------------------------------------------- #
# Capacity — including the MAX_POSITIONS_REACHED explanation
# --------------------------------------------------------------------------- #

def test_capacity_no_positions():
    state, msg = cc._capacity_state(0, 5)
    assert state == cc.CAP_EMPTY
    assert "up to 5" in msg


def test_capacity_available():
    state, msg = cc._capacity_state(3, 5)
    assert state == cc.CAP_AVAILABLE
    assert "2 of 5" in msg
    assert "manual review" in msg.lower()


def test_capacity_max_positions_reached_explanation():
    state, msg = cc._capacity_state(5, 5)
    assert state == cc.CAP_FULL
    # Must explain the risk-engine guard, never prompt a live order.
    assert "risk engine is preventing additional exposure" in msg
    assert "Review current positions" in msg
    assert "live" not in msg.lower()


def test_capacity_over_full():
    state, _ = cc._capacity_state(7, 5)
    assert state == cc.CAP_FULL


# --------------------------------------------------------------------------- #
# Safety block
# --------------------------------------------------------------------------- #

def test_safety_block_is_paper_only_no_live():
    s = cc._safety_block()
    assert s["manual_review"] is True
    assert s["paper_only"] is True
    assert s["no_broker_execution"] is True
    assert s["automation_off"] is True
    assert s["no_live_orders"] is True
    assert s["creates_orders"] is False
    assert s["creates_signals"] is False
    assert s["creates_trade_decisions"] is False
    assert s["is_live_trading_approval"] is False
    for badge in ("MANUAL REVIEW", "PAPER ONLY", "NO BROKER EXECUTION",
                  "AUTOMATION OFF", "NO LIVE ORDERS"):
        assert badge in s["safety_badges"]


# --------------------------------------------------------------------------- #
# Next-action selection — one branch per scenario
# --------------------------------------------------------------------------- #

def _sys(ready=True):
    return {"backend_ready": ready}


def _alpha(available=True, stale=False):
    return {"available": available, "mark_stale": stale, "mark_age_calendar_days": 9}


def _wf(review=0, approved=0, order_eligible=0):
    return {
        "review_queue_count": review,
        "approved_count": approved,
        "order_eligible_count": order_eligible,
    }


def _pf(state=cc.CAP_AVAILABLE, open_positions=0):
    return {
        "capacity_state": state,
        "open_positions": open_positions,
        "capacity_explanation": "cap note",
    }


def _pick(system, alpha, workflow, portfolio):
    na = cc._select_next_action(
        system=system, alpha=alpha, workflow=workflow, portfolio=portfolio
    )
    # Every action must carry label + ui target + safety context (never a live order).
    assert na["action_label"]
    assert na["ui_target"]
    assert na["safety_context"]
    return na


def test_next_action_backend_not_ready_disconnected():
    na = _pick(_sys(ready=False), _alpha(), _wf(), _pf())
    assert na["action"] == cc.NA_REFRESH_APP


def test_next_action_capacity_block_when_pending_work():
    na = _pick(_sys(), _alpha(), _wf(approved=3), _pf(state=cc.CAP_FULL))
    assert na["action"] == cc.NA_RESOLVE_CAPACITY
    assert "capacity is full" in na["explanation"].lower()


def test_next_action_review_candidates():
    na = _pick(_sys(), _alpha(), _wf(review=4), _pf())
    assert na["action"] == cc.NA_REVIEW_CANDIDATES
    assert na["ui_target"] == "daily-workflow"


def test_next_action_create_signals():
    na = _pick(_sys(), _alpha(), _wf(approved=2), _pf())
    assert na["action"] == cc.NA_CREATE_SIGNALS


def test_next_action_review_decisions():
    na = _pick(_sys(), _alpha(), _wf(order_eligible=1), _pf())
    assert na["action"] == cc.NA_REVIEW_DECISIONS


def test_next_action_run_refresh_on_stale_mark():
    # Healthy workflow (nothing pending), alpha available but mark stale.
    na = _pick(_sys(), _alpha(available=True, stale=True), _wf(), _pf())
    assert na["action"] == cc.NA_RUN_REFRESH
    assert "stale" in na["explanation"].lower()


def test_next_action_load_alpha_when_unavailable():
    na = _pick(_sys(), _alpha(available=False), _wf(), _pf())
    assert na["action"] == cc.NA_LOAD_ALPHA


def test_next_action_monitor_portfolio():
    na = _pick(_sys(), _alpha(available=True, stale=False), _wf(), _pf(open_positions=3))
    assert na["action"] == cc.NA_MONITOR
    assert na["ui_target"] == "portfolio"


def test_next_action_no_action_required_empty_workflow():
    na = _pick(_sys(), _alpha(available=True, stale=False), _wf(), _pf(open_positions=0))
    assert na["action"] == cc.NA_NONE
    assert na["requires_user_action"] is False


def test_next_action_handles_degraded_none_counts():
    # Partial service failure: workflow counts are None, alpha unavailable.
    degraded_wf = {"review_queue_count": None, "approved_count": None,
                   "order_eligible_count": None}
    na = _pick(_sys(), _alpha(available=False), degraded_wf, _pf())
    assert na["action"] == cc.NA_LOAD_ALPHA  # does not crash on None


# --------------------------------------------------------------------------- #
# Alpha slice — fed by the real read-only decision gate
# --------------------------------------------------------------------------- #

def test_alpha_section_healthy_top50_primary(bdir):
    _write_backfill(bdir)
    gate = load_current_alpha_decision_gate(today=_FRESH_TODAY)
    alpha = cc._alpha_section(gate)
    assert alpha["available"] is True
    assert alpha["mark_stale"] is False
    assert alpha["primary_paper_book"]["book"] == "TOP50"
    assert alpha["challenger_paper_book"]["label"] == "TOP25"
    assert alpha["alpha_name"] == "composite_sn"
    assert alpha["top50"]["current_return_pct"] is not None
    assert alpha["top50"]["spy_cumulative_return_pct"] is not None
    assert alpha["remaining_trading_days"] is not None


def test_alpha_section_stale_mark_flagged(bdir):
    _write_backfill(bdir)
    stale_today = (date.fromisoformat(_SIGNAL) + timedelta(days=400)).isoformat()
    gate = load_current_alpha_decision_gate(today=stale_today)
    alpha = cc._alpha_section(gate)
    assert alpha["mark_stale"] is True


def test_alpha_section_unavailable_when_rejected(bdir):
    _write_backfill(bdir, decision="BACKFILL_REJECTED_INTEGRITY_FAILURE")
    gate = load_current_alpha_decision_gate(today=_FRESH_TODAY)
    alpha = cc._alpha_section(gate)
    assert alpha["available"] is False
    assert alpha["primary_paper_book"]["book"] is None


def test_degraded_alpha_is_controlled():
    alpha = cc._degraded_alpha("boom")
    assert alpha["available"] is False
    assert alpha["unavailable_reason"] == "boom"


# --------------------------------------------------------------------------- #
# Workflow slice — from raw counts
# --------------------------------------------------------------------------- #

def _counts(**over):
    base = {
        "total_candidates": 0, "today_total": 0, "today_pending": 0,
        "today_approved": 0, "older_count": 0, "decision_count": 0,
        "signal_count": 0, "order_eligible": 0, "pending_orders": 0,
        "filled_orders": 0, "open_positions": 0,
    }
    base.update(over)
    return base


def test_workflow_section_review_stage():
    wf = cc._workflow_section(_counts(total_candidates=6, today_total=6, today_pending=6), False)
    assert wf["stage"] == "REVIEW_CANDIDATES"
    assert wf["review_queue_count"] == 6
    assert wf["actionable_count"] == 6
    assert wf["blocked_count"] == 0
    assert wf["current_blocker"] is None


def test_workflow_section_blocked_when_capacity_full():
    wf = cc._workflow_section(
        _counts(today_total=3, today_approved=2, order_eligible=1), True
    )
    assert wf["blocked_count"] == 3
    assert wf["current_blocker"] is not None
    assert "MAX_POSITIONS_REACHED" in wf["current_blocker"]


def test_workflow_section_empty_is_needs_review():
    wf = cc._workflow_section(_counts(), False)
    assert wf["stage"] == "NEEDS_DAILY_REVIEW"
    assert wf["actionable_count"] == 0


def test_degraded_workflow_is_controlled():
    wf = cc._degraded_workflow("boom")
    assert wf["stage"] == "UNAVAILABLE"
    assert wf["review_queue_count"] is None
