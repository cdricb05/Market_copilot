"""scripts/stage19_3_ui_fixtures.py — Stage 19.3 hermetic UI-acceptance fixtures.

Generates the EXACT canonical backend payloads the browser acceptance run serves, by
calling the REAL contract builders (``api.workflow_state``, ``api.daily_close``,
``api.operational_book``, ``api.rebalance_execution``) with synthetic inputs.

Strictly offline and read-only: it opens no live store, calls no provider, contacts no
prediction service and writes only the fixture file it is given. It exists so the
Playwright acceptance run never touches live operational state.

Usage:
    python scripts/stage19_3_ui_fixtures.py --out <path.json>
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from paper_trader.api import daily_close as dc
from paper_trader.api import operational_book as ob
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import rebalance_execution as rb
from paper_trader.api import workflow_state as ws

BOOK = "alpha_paper_book_1"
PLAN_CUR = "rbop_2026-08-12_alpha_paper_book_1_1a198f560cca"
PLAN_OLD = "rbop_2026-08-12_alpha_paper_book_1_5bf9c6c20f8a"
PLAN_HASH = "1a198f560cca5c7457e58f151b0e409b772b5ab85368a4f8bdf5eacc4d9315b9"
PROP_ID = "reap_2026-08-12_alpha_paper_book_1_f64fe4998d9d"
PROP_HASH = "f64fe4998d9d5cb5fe6e1fc74636e2557e9c406c7ac18867f190e9deb68812c7"


def _orders():
    """The August-13 live cohort SHAPE (synthetic rows, no live store is read)."""
    out = [{"order_id": "ord_h%02d" % i, "book_id": BOOK, "ticker": "H%02d" % i,
            "side": desk.SIDE_BUY, "quantity": 10, "status": desk.ST_FILLED,
            "approval_date": "2026-07-18"} for i in range(25)]
    out += [{"order_id": "ord_d%02d" % i, "book_id": BOOK, "ticker": "D%02d" % i,
             "side": desk.SIDE_BUY, "quantity": 10, "status": desk.ST_CANCELLED,
             "approval_date": "2026-08-12",
             "rebalance_lineage": {"order_plan_id": PLAN_OLD,
                                   "order_plan_hash": "5bf9c6c20f8a" + "0" * 52,
                                   "proposal_id": PROP_ID, "proposal_hash": PROP_HASH}}
            for i in range(22)]
    out += [{"order_id": "ord_c%02d" % i, "book_id": BOOK, "ticker": "C%02d" % i,
             "side": (desk.SIDE_BUY if i < 15 else desk.SIDE_SELL), "quantity": 10,
             "status": desk.ST_SUBMITTED, "approval_date": "2026-08-13",
             "rebalance_lineage": {"order_plan_id": PLAN_CUR,
                                   "order_plan_hash": PLAN_HASH,
                                   "proposal_id": PROP_ID, "proposal_hash": PROP_HASH}}
            for i in range(29)]
    return out


def _fold(orders):
    by_status: dict[str, int] = {}
    for o in orders:
        by_status[o["status"]] = by_status.get(o["status"], 0) + 1
    return {"by_status": by_status,
            "current_rebalance": ob.current_rebalance_lineage(orders),
            "latest_submission_date": "2026-08-13"}


def _canonical_state(*, pending, fills, lifecycle_orders):
    """The canonical_state block the UI consumes, built by the REAL lifecycle owner."""
    lc = ob.derive_lifecycle_view(
        initialized=True, orders=_fold(lifecycle_orders), fills_count=fills,
        plan_exists=False, submitted_date="2026-08-13", execution_model="NEXT_CLOSE")
    return {
        "operational_book_id": BOOK, "operational_book_name": "Alpha Paper Book #1",
        "operational_book_status": "ORDERS_CONFIRMED",
        "workflow_state": "ORDERS_CONFIRMED",
        "workflow_state_label": "ORDERS CONFIRMED",
        "next_action_code": "MONITOR", "next_action_label": lc["primary_action_label"],
        "next_action_route_or_anchor": "#portfolio-manager/pd-band",
        "next_action_enabled": True,
        "lifecycle_stage": lc["lifecycle_stage"],
        "lifecycle_stage_label": lc["lifecycle_stage_label"],
        "primary_headline": lc["primary_headline"],
        "primary_explanation": lc["primary_explanation"],
        "next_action_explanation": lc["primary_explanation"],
        "current_task_label": lc["current_task_label"],
        "secondary_action_label": lc["secondary_action_label"],
        "proposed_count": lc["proposed_count"], "submitted_count": lc["submitted_count"],
        "filled_count": lc["filled_count"], "cancelled_count": lc["cancelled_count"],
        "expired_count": lc["expired_count"], "open_order_count": lc["open_order_count"],
        "current_rebalance": lc["current_rebalance"],
        "requires_separate_desk_refresh": lc["requires_separate_desk_refresh"],
        "settlement_owner": lc["settlement_owner"],
        "submitted_date": lc["submitted_date"],
        "next_eligible_fill_explanation": lc["next_eligible_fill_explanation"],
        "no_further_confirmation_required": lc["no_further_confirmation_required"],
        "execution_model": "NEXT_CLOSE", "pending_order_count": pending,
        "fill_count": fills, "holdings_count": 25, "nav": 99880.94, "cash": 1880.94,
        "desk_valuation_date": "2026-08-12", "valuation_date": "2026-08-12",
        "desk_mark_date": "2026-08-12", "next_review_date": "2026-09-01",
        "review_due": False, "review_cadence": "MONTHLY", "holdings_detail": [],
    }


def _workflow(overall, *, pending, eligible, latest_close):
    primary = ws.assert_primary_action_contract(
        ws._primary_action(overall, {"eligible_date": eligible,
                                     "session_operator_action": ""}))
    if primary.get("execution_kind") == ws.EXEC_DAILY_CLOSE and pending > 0:
        primary = dict(primary, explanation=(
            "%s %d NEXT_CLOSE paper order(s) are working; this Daily Close settles the "
            "eligible ones through the Paper Desk before reassessing the portfolio."
            % (primary.get("explanation") or "", pending)).strip())
    return {
        "status": "WORKFLOW_STATE_OK", "phase": ws.PHASE,
        "overall_state": overall, "overall_state_vocabulary": list(ws.OVERALL_STATES),
        "current_task": primary["current_task"], "headline": primary["headline"],
        "primary_action": {
            "action_code": primary["action_code"], "label": primary["label"],
            "explanation": primary["explanation"], "severity": primary["severity"],
            "destination": primary["destination"], "focus": primary.get("focus"),
            "safe_to_execute": primary["safe_to_execute"],
            "execution_available": primary["execution_available"],
            "manual_confirmation_required": primary["manual_confirmation_required"],
            "slice3_pending": False,
            "confirmation_required": primary.get("confirmation_required"),
            "execution_kind": primary.get("execution_kind"),
            "execution_contract": (
                dict(ws.EXECUTION_CONTRACTS[primary["execution_kind"]])
                if primary.get("execution_kind") in ws.EXECUTION_CONTRACTS else None)},
        "operator_command": ws.build_operator_command(
            overall=overall, primary=primary, pending_orders=pending,
            eligible_date=eligible, latest_close_date=latest_close),
        "daily_close_gate": ws.build_daily_close_gate(
            overall, eligible_date=eligible, latest_close_date=latest_close),
        "queued_actions": [], "blockers": [], "warnings": [],
        "current_session": {"calendar_date": "2026-08-13",
                            "expected_completed_market_date": eligible,
                            "latest_eligible_completed_market_date": eligible,
                            "session_status": "BEFORE_SESSION_CLOSE"},
        "operational_state": {"active_book_id": BOOK,
                              "active_book_name": "Alpha Paper Book #1",
                              "nav": 99880.94, "cash": 1880.94, "holdings_count": 25,
                              "pending_orders": pending,
                              "latest_completed_close_date": latest_close,
                              "latest_close_status": "PAPER_ORDERS_SUBMITTED",
                              "operational_close_valid": True,
                              "operational_consistency_status": "CONSISTENT"},
        "assessment_presentation": {"currency_status": "CURRENT", "severity": "SUCCESS"},
        "evidence_presentation": {"gap_severity": "green", "documented_gap": False},
        "research_state": {}, "research_cycle_state": {}, "consistency": {},
    }


def _daily_close(close_status, *, pending, eligible, last_processed):
    book = {"book_id": BOOK, "book_label": "Alpha Paper Book #1", "initialized": True,
            "pending_orders": pending, "forward_tracking": True, "fills_count": 25,
            "lifecycle_stage": "SUBMITTED", "book_active": pending == 0,
            "starting_capital": 100000.0, "nav": 99880.94, "cash": 1880.94,
            "holdings_count": 25, "valuation_date": "2026-08-12",
            "desk_mark_date": "2026-08-12", "next_scheduled_full_review": "2026-09-01",
            "scheduled_review_due": False, "review_cadence": "MONTHLY"}
    ctx = {"clock": {"expected_market_date": eligible}, "provider_readiness": None,
           "market_data_scope": None, "baseline": None,
           "paper_order_settlement": dc.build_settlement_context(
               pending_before=pending, market_date=eligible)}
    return dc._assemble(
        close_status=close_status, book=book, gate={}, pnl=None, history=[],
        processed_row=None, last_processed_date=last_processed,
        latest_eligible=eligible, decision_history=[], warnings=[],
        performed_write=False, evaluation_date="2026-08-13", context=ctx)


def _rebalance(state):
    summary = {
        "lifecycle_stage": state,
        "lifecycle_stage_label": rb._EXECUTION_STAGE_LABELS.get(state),
        "lifecycle_stages": [
            {"stage": i + 1, "code": c, "label": rb._EXECUTION_STAGE_LABELS[c],
             "current": c == state} for i, c in enumerate(rb.EXECUTION_STAGES)],
        "order_plan_id": PLAN_CUR, "order_plan_id_short": "1a198f560cca",
        "order_plan_hash": PLAN_HASH, "proposal_id": PROP_ID,
        "approval_date": "2026-08-13", "execution_model": "NEXT_CLOSE",
        "order_count": 29, "submitted_count": 29, "filled_count": 0,
        "cancelled_count": 0, "buy_count": 15, "sell_count": 14,
        "further_confirmation_required": False,
        "expected_next_execution_event": (
            "Fills at the first eligible completed owned close on or after 2026-08-13, "
            "settled by that session's Daily Close."),
        "superseded_plan_order_count": 22, "superseded_plan_ids": [PLAN_OLD],
        "historical_implementation_fill_count": 25,
        "counts_are_lineage_scoped": True,
        "current_rebalance_label": "Current rebalance: 29 submitted / 0 filled",
        "historical_label": ("Existing operational holdings from the initial "
                             "implementation: 25 filled order(s)"),
    }
    return {"phase": rb.PHASE, "owner": rb.OWNER, "status": "OK",
            "rebalance_state": state, "state_vocabulary": list(rb.STATE_VOCAB),
            "label": "Paper execution pending (NEXT_CLOSE)",
            "bound": {"proposal_id": PROP_ID, "proposal_hash": PROP_HASH,
                      "active_book_id": BOOK, "eligible_market_date": "2026-08-12"},
            "active_book_id": BOOK, "primary_action": {"label": "Awaiting the next owned close (paper execution pending)"},
            "order_plan": None, "executed_order_ids": ["ord_c%02d" % i for i in range(29)],
            "execution_summary": summary, "order_plan_buildable": False,
            "confirmation_available": False, "blocked_tickers": [], "blocked_count": 0,
            "blocked_reasons": [], "missing_marks": [],
            "provider_called": False, "performed_write": False, "created_orders": False}


def build() -> dict:
    orders = _orders()
    cs_pending = _canonical_state(pending=29, fills=25, lifecycle_orders=orders)
    settled = [dict(o, status=desk.ST_FILLED) if o.get("rebalance_lineage", {})
               .get("order_plan_id") == PLAN_CUR else o for o in orders]
    cs_done = _canonical_state(pending=0, fills=54, lifecycle_orders=settled)

    def _ob(cs):
        return {"status": "OPERATIONAL_BOOK_OK", "canonical_state": cs,
                "operational_book": {"book_id": BOOK, "current_status": "ORDERS_CONFIRMED",
                                     "canonical_state": cs, "nav": cs["nav"],
                                     "cash": cs["cash"], "holdings_count": 25,
                                     "pending_order_count": cs["pending_order_count"],
                                     "fill_count": cs["fill_count"],
                                     "target_market_date": "2026-08-12",
                                     "desk_mark_date": "2026-08-12", "target_count": 25,
                                     "holdings_detail": []}}

    return {
        "scenario_1_waiting_session_close": {
            "title": "WAITING_FOR_SESSION_CLOSE + 29 submitted orders",
            # The operator must read the CURRENT rebalance as 29 submitted / 0 filled,
            # with the book's 25 historical implementation fills labelled separately.
            "expect_current_rebalance": {
                "submitted": "29", "filled": "0", "cancelled": "0",
                "buys": "15", "sells": "14", "plan": "1a198f560cca",
                "approved": "2026-08-13", "historicalFills": "25"},
            "/v1/operations/workflow-state": _workflow(
                ws.WAITING_FOR_SESSION_CLOSE, pending=29, eligible="2026-08-12",
                latest_close="2026-08-12"),
            "/v1/operations/daily-close": _daily_close(
                dc.PAPER_ORDERS_SUBMITTED, pending=29, eligible="2026-08-12",
                last_processed="2026-08-12"),
            "/v1/operational-book": _ob(cs_pending),
            "/v1/operations/rebalance": _rebalance(rb.RB_PLAN_CONFIRMED),
        },
        "scenario_2_new_eligible_close": {
            "title": "New eligible completed close + 29 pending orders",
            "expect_current_rebalance": {
                "submitted": "29", "filled": "0", "cancelled": "0",
                "historicalFills": "25"},
            "/v1/operations/workflow-state": _workflow(
                ws.READY_FOR_DAILY_CLOSE, pending=29, eligible="2026-08-13",
                latest_close="2026-08-12"),
            "/v1/operations/daily-close": _daily_close(
                dc.CLOSE_DUE, pending=29, eligible="2026-08-13",
                last_processed="2026-08-12"),
            "/v1/operational-book": _ob(cs_pending),
            "/v1/operations/rebalance": _rebalance(rb.RB_PLAN_CONFIRMED),
        },
        "scenario_3_proposal_review": {
            "title": "Rebalance proposal ready — one review action",
            "/v1/operations/workflow-state": _workflow(
                ws.MANUAL_REVIEW_REQUIRED, pending=0, eligible="2026-08-13",
                latest_close="2026-08-13"),
            "/v1/operations/daily-close": _daily_close(
                dc.REBALANCE_PROPOSAL_READY, pending=0, eligible="2026-08-13",
                last_processed="2026-08-13"),
            "/v1/operational-book": _ob(cs_done),
            "/v1/operations/rebalance": _rebalance(rb.RB_PROPOSAL_REVIEW_REQUIRED),
        },
        "scenario_4_order_plan_review": {
            "title": "Order-plan review — one second-confirmation action",
            "/v1/operations/workflow-state": _workflow(
                ws.MANUAL_REVIEW_REQUIRED, pending=0, eligible="2026-08-13",
                latest_close="2026-08-13"),
            "/v1/operations/daily-close": _daily_close(
                dc.REBALANCE_PROPOSAL_READY, pending=0, eligible="2026-08-13",
                last_processed="2026-08-13"),
            "/v1/operational-book": _ob(cs_done),
            "/v1/operations/rebalance": _rebalance(rb.RB_PLAN_REVIEW_REQUIRED),
        },
        "scenario_5_paper_execution_reconciled": {
            "title": "Paper execution reconciled — no stale execution action",
            "/v1/operations/workflow-state": _workflow(
                ws.DAILY_CYCLE_COMPLETE, pending=0, eligible="2026-08-13",
                latest_close="2026-08-13"),
            "/v1/operations/daily-close": _daily_close(
                dc.CLOSE_COMPLETE_HOLD, pending=0, eligible="2026-08-13",
                last_processed="2026-08-13"),
            "/v1/operational-book": _ob(cs_done),
            "/v1/operations/rebalance": _rebalance(rb.RB_EXECUTED),
        },
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)
    Path(args.out).write_text(json.dumps(build(), indent=1, default=str) + "\n",
                              encoding="utf-8")
    print("Stage 19.3 UI fixtures written to: %s" % args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
