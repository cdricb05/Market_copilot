"""
tests/test_daily_workflow_dashboard.py — Phase 14-B daily-workflow unit tests.

Fully offline and DB-free: exercises the PURE aggregation logic of
``api/daily_workflow_dashboard.py`` — active-stage derivation (exactly one active
stage), per-stage status incl. the capacity BLOCKED state, the stage-scoped next
action (incl. the MAX_POSITIONS capacity-block explanation that never implies a
live order), the review-queue grouping by stable identity, provenance, and the
degraded fallback. The DB-backed endpoint wiring is covered separately in
test_daily_workflow_dashboard_endpoint.py.
"""
from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from paper_trader.api import daily_workflow_dashboard as dw


def _counts(**over) -> dict[str, int]:
    base = {
        "total_candidates": 0, "today_total": 0, "today_pending": 0,
        "today_approved": 0, "older_count": 0, "decision_count": 0,
        "signal_count": 0, "order_eligible": 0, "pending_orders": 0,
        "filled_orders": 0, "open_positions": 0,
    }
    base.update(over)
    return base


# --------------------------------------------------------------------------- #
# Active-stage derivation — exactly one active stage, from live counts
# --------------------------------------------------------------------------- #

def test_active_stage_review_when_pending():
    assert dw._derive_active_stage(_counts(today_total=3, today_pending=2), capacity_full=False) == dw.ST_REVIEW


def test_active_stage_signals_when_approved():
    assert dw._derive_active_stage(_counts(today_approved=1), capacity_full=False) == dw.ST_SIGNALS


def test_active_stage_decisions_when_order_eligible():
    assert dw._derive_active_stage(_counts(order_eligible=1), capacity_full=True) == dw.ST_DECISIONS


def test_active_stage_decisions_when_pending_orders():
    assert dw._derive_active_stage(_counts(pending_orders=1), capacity_full=False) == dw.ST_DECISIONS


def test_active_stage_portfolio_when_positions_only():
    assert dw._derive_active_stage(_counts(open_positions=2), capacity_full=False) == dw.ST_PORTFOLIO


def test_active_stage_candidates_when_scanned_but_idle():
    assert dw._derive_active_stage(_counts(today_total=4), capacity_full=False) == dw.ST_CANDIDATES


def test_active_stage_data_when_empty():
    assert dw._derive_active_stage(_counts(), capacity_full=False) == dw.ST_DATA


# --------------------------------------------------------------------------- #
# Stages — exactly one active; capacity BLOCKED state
# --------------------------------------------------------------------------- #

def test_build_stages_exactly_one_active():
    counts = _counts(today_total=3, today_pending=2, open_positions=1)
    active = dw._derive_active_stage(counts, capacity_full=False)
    stages = dw._build_stages(counts, active_stage=active, capacity_full=False, last_signal_date=None)
    assert len(stages) == 6
    actives = [s for s in stages if s["is_active"]]
    assert len(actives) == 1
    assert actives[0]["stage"] == dw.ST_REVIEW
    # Every stage carries the required contract fields.
    for s in stages:
        for key in ("stage", "label", "status", "count", "last_completed_at",
                    "blocker_code", "blocker_explanation", "action_label",
                    "action_target", "enabled", "disabled_reason", "is_active"):
            assert key in s
        assert s["status"] in (dw.S_COMPLETE, dw.S_READY, dw.S_NEEDS_ACTION, dw.S_BLOCKED, dw.S_NOT_AVAILABLE)


def test_decisions_blocked_when_capacity_full():
    counts = _counts(today_approved=1, order_eligible=1)
    stages = dw._build_stages(counts, active_stage=dw.ST_DECISIONS, capacity_full=True, last_signal_date=None)
    dec = next(s for s in stages if s["stage"] == dw.ST_DECISIONS)
    assert dec["status"] == dw.S_BLOCKED
    assert dec["blocker_code"] == "MAX_POSITIONS_REACHED"
    assert "risk engine" in dec["blocker_explanation"].lower()
    assert "live" not in dec["blocker_explanation"].lower()


def test_review_stage_needs_action_and_complete():
    s_needs = dw._stage_status(dw.ST_REVIEW, _counts(today_total=3, today_pending=2),
                               active_stage=dw.ST_REVIEW, capacity_full=False)
    assert s_needs[0] == dw.S_NEEDS_ACTION
    s_done = dw._stage_status(dw.ST_REVIEW, _counts(today_total=3, today_pending=0),
                              active_stage=dw.ST_PORTFOLIO, capacity_full=False)
    assert s_done[0] == dw.S_COMPLETE


def test_data_stage_complete_when_data_present():
    s = dw._stage_status(dw.ST_DATA, _counts(open_positions=1), active_stage=dw.ST_PORTFOLIO, capacity_full=False)
    assert s[0] == dw.S_COMPLETE


def test_stage_disabled_reason_when_empty():
    counts = _counts()
    stages = dw._build_stages(counts, active_stage=dw.ST_DATA, capacity_full=False, last_signal_date=None)
    review = next(s for s in stages if s["stage"] == dw.ST_REVIEW)
    assert review["enabled"] is False
    assert review["disabled_reason"]


# --------------------------------------------------------------------------- #
# Next action — stage-scoped, never implies a live order
# --------------------------------------------------------------------------- #

def _assert_na_shape(na):
    assert na["action_label"]
    assert na["action_target"]
    assert na["safety_context"]
    assert "live" not in na["explanation"].lower() or "no live" in na["explanation"].lower()


def test_next_action_review():
    na = dw._next_action(dw.ST_REVIEW, _counts(today_pending=3), capacity_full=False)
    _assert_na_shape(na)
    assert na["action"] == "REVIEW_CANDIDATES"
    assert na["action_target"] == "daily-workflow/review"


def test_next_action_signals():
    na = dw._next_action(dw.ST_SIGNALS, _counts(today_approved=2), capacity_full=False)
    _assert_na_shape(na)
    assert na["action"] == "CREATE_SIGNALS_PREVIEW"
    assert na["action_target"] == "daily-workflow/signals"


def test_next_action_decisions():
    na = dw._next_action(dw.ST_DECISIONS, _counts(order_eligible=1), capacity_full=False)
    _assert_na_shape(na)
    assert na["action"] == "REVIEW_DECISIONS"
    assert na["action_target"] == "daily-workflow/decisions"


def test_next_action_capacity_block():
    na = dw._next_action(dw.ST_DECISIONS, _counts(order_eligible=1), capacity_full=True)
    assert na["action"] == "RESOLVE_CAPACITY_BLOCK"
    assert "capacity is full" in na["explanation"].lower()
    assert "live" not in na["explanation"].lower()


def test_next_action_portfolio():
    na = dw._next_action(dw.ST_PORTFOLIO, _counts(open_positions=2), capacity_full=False)
    assert na["action"] == "MONITOR_PORTFOLIO"


def test_next_action_data():
    na = dw._next_action(dw.ST_DATA, _counts(), capacity_full=False)
    assert na["action"] == "RUN_DAILY_ALPHA_REFRESH"


# --------------------------------------------------------------------------- #
# Review grouping by stable identity (dedup, history count)
# --------------------------------------------------------------------------- #

def _cr(ticker, status="NEW", n=0):
    return SimpleNamespace(
        id=f"id-{ticker}-{n}", ticker=ticker, review_status=status,
        preview_decision="CONSIDER", preview_score="80.0",
        prediction_recommendation="BUY", prediction_confidence="0.90",
        expected_return_pct="1.5", scan_score="12.0",
        created_at=datetime(2026, 7, 15, 10, n % 60),
    )


def test_group_by_ticker_dedups_and_counts_history():
    rows = [_cr("AAA", n=0), _cr("AAA", n=1), _cr("BBB", n=2)]
    grouped = dw._group_by_ticker(rows)
    by = {g["ticker"]: g for g in grouped}
    assert set(by) == {"AAA", "BBB"}
    assert by["AAA"]["history_count"] == 1
    assert by["BBB"]["history_count"] == 0


def test_source_tail_strips_prefix():
    tail = dw._source_tail(dw._REVIEW_SOURCE_PREFIX + "abc123")
    assert tail == "abc123"
    assert dw._source_tail(None) is None


# --------------------------------------------------------------------------- #
# Provenance + degraded fallback
# --------------------------------------------------------------------------- #

def test_provenance_is_read_only():
    p = dw._provenance()
    assert p["read_only"] is True
    for k in ("wrote_to_database", "created_orders", "created_signals",
              "created_trade_decisions", "invoked_daily_refresh",
              "called_prediction_service", "called_external_provider",
              "made_loopback_http_calls"):
        assert p[k] is False


def test_degraded_is_controlled():
    body = dw._degraded("boom")
    assert body["status"] == "DEGRADED"
    assert body["summary"]["active_stage"] == dw.ST_DATA
    assert body["next_action"]["action"]
    assert len(body["stages"]) == 6
    assert body["safety"]["paper_only"] is True
