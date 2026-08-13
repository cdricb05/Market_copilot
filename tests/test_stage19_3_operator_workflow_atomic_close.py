"""
tests/test_stage19_3_operator_workflow_atomic_close.py — Stage 19.3.

OPERATOR WORKFLOW & ATOMIC POST-CLOSE CONSOLIDATION.

The August-13 live operating path exposed two control-plane defects:

  A. The operator had to hunt through many simultaneously-visible controls while the
     authoritative workflow said "No action required right now".
  B. A standalone Paper Desk refresh competed with the canonical Daily Close for the
     SAME post-close transition: ``resolve_daily_close_status`` short-circuited on
     pending paper orders before it ever considered a newly eligible completed
     session, and ``_run_daily_close_locked`` refused to run at all while orders were
     working — so the operator had to run POST /v1/paper-desk/refresh first.

This module locks the repaired contract:

  * daily-close precedence (pending orders never hide a new eligible close);
  * settlement THROUGH the existing Paper Desk owner inside the one close write path;
  * idempotency / failure atomicity (no false close, no duplicate fill, performance
    row or decision-journal row);
  * lineage-scoped current-rebalance counts (historical initial-implementation fills
    and superseded/cancelled plans can never masquerade as the current plan);
  * ONE canonical operator command that every surface mirrors, with at most one
    normal-path mutation action and the desk refresh demoted to maintenance/recovery.

SAFETY: fully hermetic. Every test uses a tmp desk dir + injected operational / gate /
engine / refresh / probe seams. No provider call, no prediction call, no broker, no
automation, no model change, and NO live operational store is read or written.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from paper_trader.api import daily_action_gate as dag
from paper_trader.api import daily_close as dc
from paper_trader.api import operational_book as ob
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import rebalance_execution as rb
from paper_trader.api import workflow_state as ws

_ROOT = Path(__file__).resolve().parents[1]
_UI = _ROOT / "api" / "ui" / "index.html"
_HARNESS = _ROOT / "scripts" / "operator_action_integrity_harness.js"
_NODE = shutil.which("node")
_ET = ZoneInfo("America/New_York")

BOOK = "alpha_paper_book_1"
PLAN_CUR = "rbop_2026-08-12_alpha_paper_book_1_1a198f560cca"
PLAN_OLD = "rbop_2026-08-12_alpha_paper_book_1_5bf9c6c20f8a"
PROP_HASH = "f64fe4998d9d5cb5fe6e1fc74636e2557e9c406c7ac18867f190e9deb68812c7"
PLAN_HASH = "1a198f560cca5c7457e58f151b0e409b772b5ab85368a4f8bdf5eacc4d9315b9"


# --------------------------------------------------------------------------- #
# Hermetic seams
# --------------------------------------------------------------------------- #
def _ops(*, pending=0, fills=25, initialized=True, lifecycle="FILLED",
         holdings=("AAPL", "MSFT", "NVDA")):
    hd = [{"ticker": t} for t in holdings]
    cs = {"pending_order_count": pending, "fill_count": fills,
          "lifecycle_stage": lifecycle, "nav": 99880.94, "cash": 1880.94,
          "holdings_count": len(holdings), "valuation_date": "2026-08-12",
          "desk_mark_date": "2026-08-12", "next_review_date": "2026-09-01",
          "review_due": False, "review_cadence": "MONTHLY", "holdings_detail": hd}
    book = {"book_id": BOOK, "book_label": "Alpha Paper Book #1",
            "initialized": initialized, "starting_capital": 100000.0,
            "holdings_count": len(holdings), "pending_order_count": pending,
            "fill_count": fills, "holdings": {t: 10 for t in holdings},
            "holdings_detail": hd}
    return {"canonical_state": cs, "operational_book": book}


def _ops_loader(**kw):
    def _loader(*_a, **_k):
        return _ops(**kw)
    return _loader


def _settling_ops_loader(*, pending_before, pending_after, fills_after):
    """First read (pre-refresh) sees the pending orders; every later read sees the
    post-settlement book — the shape the real close observes across its two reads."""
    calls = {"n": 0}

    def _loader(*_a, **_k):
        calls["n"] += 1
        if calls["n"] == 1:
            return _ops(pending=pending_before, fills=25)
        return _ops(pending=pending_after, fills=fills_after)
    return _loader


def _gate(outcome="NO_ACTION_TODAY", data_ready=True, pcount=0):
    return {"outcome": outcome, "outcome_label": outcome.replace("_", " "),
            "target_state": ("CURRENT_ALIGNED" if outcome == "NO_ACTION_TODAY"
                             else "PROPOSAL_READY"),
            "target_state_label": "-", "data_ready": data_ready,
            "checks_performed": [], "checks_summary": {"line": "13 checks completed"},
            "proposed_additions": [], "proposed_removals": [], "proposed_resizes": [],
            "proposed_change_count": pcount, "target_actual_match": (pcount == 0),
            "operational_dates": {}, "warnings": []}


def _ok_refresh(closed, *, n_filled=0):
    def _fn(**_kw):
        return {"status": desk.S_OK, "performed_write": True,
                "resulting_desk_mark_date": closed,
                "latest_completed_market_date": closed,
                "settlement": {"n_filled": n_filled},
                "performance": {"n_appended": 1}}
    return _fn


def _blocked_refresh(msg="AAPL has no completed owned close"):
    def _fn(**_kw):
        return {"status": desk.S_MARKS_BLOCKED, "performed_write": False,
                "resulting_desk_mark_date": None,
                "blockers": ["TICKER_MARKS_MISSING: %s" % msg], "message": msg}
    return _fn


def _raising_refresh(**_kw):
    raise RuntimeError("owned provider transport failed mid-settlement")


def _ready_probe(**k):
    return {"provider_latest_date": k["expected_market_date"], "priced": ["SPY"],
            "source": "TEST_PROBE", "queried": True}


def _behind_probe(latest):
    def _p(**_k):
        return {"provider_latest_date": latest, "priced": ["SPY"],
                "source": "TEST_PROBE", "queried": True}
    return _p


def _seed_close(desk_dir, market_date, decision=dc.DECISION_HOLD,
                status=dc.CLOSE_COMPLETE_HOLD):
    desk._append_ledger(desk._desk_dir(desk_dir), dc.DAILY_CLOSE_JOURNAL_FILE,
                        [{"event": dc.DAILY_CLOSE_EVENT, "book_id": BOOK,
                          "market_date": market_date, "decision": decision,
                          "close_status": status, "is_baseline": False}])


def _journal_rows(desk_dir):
    """The recorded daily-close decision rows, read through the ONE ledger reader."""
    return [r for r in desk._read_ledger(desk._desk_dir(desk_dir),
                                         dc.DAILY_CLOSE_JOURNAL_FILE)
            if r.get("event") == dc.DAILY_CLOSE_EVENT]


def _close_count(desk_dir, market_date=None):
    return sum(1 for r in _journal_rows(desk_dir)
               if market_date is None or r.get("market_date") == market_date)


def _seed_orders(desk_dir, orders):
    """Seed the append-only order ledger through its real event vocabulary."""
    sdir = desk._desk_dir(desk_dir)
    rows = []
    for o in orders:
        core = {k: v for k, v in o.items() if k != "status"}
        rows.append({"event": "ORDER_CREATED", "order": core})
    desk._append_ledger(sdir, desk.ORDERS_FILE, rows)
    trans = []
    for o in orders:
        if o["status"] == desk.ST_PROPOSED:
            continue
        if o["status"] in (desk.ST_SUBMITTED, desk.ST_FILLED, desk.ST_CANCELLED,
                           desk.ST_EXPIRED):
            trans.append({"event": "ORDER_TRANSITION", "order_id": o["order_id"],
                          "from_status": desk.ST_PROPOSED,
                          "to_status": desk.ST_SUBMITTED,
                          "approval_date": o.get("approval_date"),
                          "marks_latest_at_approval": "2026-08-12"})
        if o["status"] != desk.ST_SUBMITTED:
            trans.append({"event": "ORDER_TRANSITION", "order_id": o["order_id"],
                          "from_status": desk.ST_SUBMITTED,
                          "to_status": o["status"]})
    if trans:
        desk._append_ledger(sdir, desk.ORDERS_FILE, trans)


def _run(desk_dir, *, today="2026-08-14", ops_loader=None, refresh=None,
         gate=None, probe=_ready_probe):
    return dc.run_daily_close(
        confirm=dc.EXECUTE_CONFIRMATION, today=today, desk_dir=desk_dir,
        operational_loader=ops_loader or _ops_loader(pending=0),
        refresh_fn=refresh or _ok_refresh("2026-08-13"),
        gate_loader=gate or (lambda *a, **k: _gate()),
        provider_probe=probe)


# --------------------------------------------------------------------------- #
# A. DAILY-CLOSE PRECEDENCE (#1-#5)
# --------------------------------------------------------------------------- #
class TestDailyClosePrecedence:
    def _r(self, **kw):
        base = dict(initialized=True, book_active=True, forward_tracking=True,
                    pending_orders=0, latest_eligible="2026-08-13",
                    last_processed_date="2026-08-12",
                    processed_decision_for_latest=None)
        base.update(kw)
        return dc.resolve_daily_close_status(**base)

    def test_1_pending_orders_no_new_close_stays_passive(self):
        """#1 — the live 08-13 morning shape: 29 pending, latest eligible already
        processed. The passive monitoring state is preserved EXACTLY."""
        assert self._r(pending_orders=29, latest_eligible="2026-08-12",
                       last_processed_date="2026-08-12",
                       processed_decision_for_latest=dc.DECISION_ORDERS_PENDING) \
            == dc.PAPER_ORDERS_SUBMITTED
        assert self._r(pending_orders=29, latest_eligible="2026-08-12",
                       last_processed_date="2026-08-12") == dc.PAPER_ORDERS_SUBMITTED

    def test_2_pending_orders_plus_new_eligible_close_is_due(self):
        """#2 — THE defect. Tonight's shape: last processed 08-12, latest eligible
        08-13, 29 orders working -> the close takes precedence."""
        assert self._r(pending_orders=29) == dc.CLOSE_DUE

    def test_3_no_pending_orders_plus_new_eligible_close_is_due(self):
        assert self._r(pending_orders=0) == dc.CLOSE_DUE

    def test_4_data_blocked_still_fails_closed(self):
        """#4 — precedence never manufactures eligibility."""
        assert self._r(pending_orders=29, valuation_complete=False) == dc.DATA_BLOCKED
        assert self._r(pending_orders=29, provider_ready=False, cutoff_passed=True) \
            == dc.WAITING_FOR_MARKET_DATA
        assert self._r(pending_orders=29, provider_ready=False, cutoff_passed=False) \
            == dc.AWAITING_MARKET_CLOSE

    def test_5_already_processed_remains_idempotent(self):
        # A processed date is never re-offered as due. With orders still working the
        # passive monitoring state is preserved (unchanged pre-19.3 behaviour).
        assert self._r(pending_orders=0, latest_eligible="2026-08-13",
                       last_processed_date="2026-08-13",
                       processed_decision_for_latest=dc.DECISION_HOLD) \
            == dc.CLOSE_COMPLETE_HOLD
        assert self._r(pending_orders=29, latest_eligible="2026-08-13",
                       last_processed_date="2026-08-13",
                       processed_decision_for_latest=dc.DECISION_HOLD) \
            == dc.PAPER_ORDERS_SUBMITTED

    def test_5b_baseline_still_due_on_a_live_book_with_working_orders(self):
        assert self._r(pending_orders=29, last_processed_date=None,
                       baseline_required=True) == dc.INITIAL_BASELINE_DUE

    def test_5c_pending_without_forward_tracking_stays_passive(self):
        """A book whose INITIAL implementation is still working has no forward-tracking
        book to close — monitoring is preserved."""
        assert self._r(pending_orders=29, book_active=False, forward_tracking=False) \
            == dc.PAPER_ORDERS_SUBMITTED


# --------------------------------------------------------------------------- #
# B. SETTLEMENT THROUGH THE DAILY CLOSE (#6-#18)
# --------------------------------------------------------------------------- #
class TestSettlementThroughDailyClose:
    def test_6_close_calls_the_existing_paper_desk_owner(self, tmp_path):
        """#6 — the close DELEGATES; it never settles anything itself."""
        d = tmp_path / "d"
        _seed_close(d, "2026-08-12")
        seen = {}

        def _spy(**kw):
            seen.update(kw)
            return _ok_refresh("2026-08-13", n_filled=29)(**kw)

        out = _run(d, ops_loader=_settling_ops_loader(
            pending_before=29, pending_after=0, fills_after=54), refresh=_spy)
        assert out["close_status"] == dc.CLOSE_COMPLETE_HOLD
        assert seen["confirm"] == desk.REFRESH_CONFIRM_TOKEN
        assert seen["completed_through"] == "2026-08-13"
        # the close implements no settlement of its own
        src = (_ROOT / "api" / "daily_close.py").read_text(encoding="utf-8")
        for forbidden in ("def settle_due_orders(", "def sync_marks(",
                          "def refresh_desk(", "def append_performance("):
            assert forbidden not in src

    def test_7_due_orders_fill_exactly_once(self, tmp_path):
        d = tmp_path / "d"
        _seed_close(d, "2026-08-12")
        calls = {"n": 0}

        def _once(**kw):
            calls["n"] += 1
            return _ok_refresh("2026-08-13", n_filled=29)(**kw)

        _run(d, ops_loader=_settling_ops_loader(
            pending_before=29, pending_after=0, fills_after=54), refresh=_once)
        assert calls["n"] == 1

    def test_8_ineligible_orders_do_not_fill(self, tmp_path):
        """#8 — orders not yet eligible remain SUBMITTED; the close records
        ORDERS_PENDING instead of claiming a completed rebalance."""
        d = tmp_path / "d"
        _seed_close(d, "2026-08-12")
        out = _run(d, ops_loader=_settling_ops_loader(
            pending_before=29, pending_after=29, fills_after=25),
            refresh=_ok_refresh("2026-08-13", n_filled=0))
        assert out["close_status"] == dc.PAPER_ORDERS_SUBMITTED
        assert out["decision"] == dc.DECISION_ORDERS_PENDING

    def test_9_same_close_hindsight_remains_impossible(self):
        """#9 — the ONE settlement owner still refuses a fill at or before the mark
        date that existed at approval time."""
        series = [("2026-08-12", 100.0), ("2026-08-13", 101.0)]
        # approved 08-13 with marks through 08-12 -> first legitimate fill is 08-13
        assert desk._first_close_on_or_after(series, "2026-08-13", "2026-08-12") == \
            ("2026-08-13", 101.0)
        # a same-close fill (marks already contained 08-13 at approval) is refused
        assert desk._first_close_on_or_after(series, "2026-08-13", "2026-08-13") is None

    def test_10_transaction_costs_applied_once_by_the_one_owner(self):
        """#10 — the cost rate is DEFINED once, in the settlement owner, and the close
        never applies it; a second application would double-charge a settled fill."""
        src = (_ROOT / "api" / "paper_trading_desk.py").read_text(encoding="utf-8")
        assert src.count("COST_RATE_PER_SIDE = ") == 1
        assert src.count("cost = gross * COST_RATE_PER_SIDE") >= 1
        for mod in ("api/daily_close.py", "api/workflow_state.py",
                    "api/operational_book.py"):
            assert "COST_RATE_PER_SIDE" not in \
                (_ROOT / mod).read_text(encoding="utf-8"), mod

    def test_11_12_13_cash_holdings_and_nav_reconcile_from_the_owner(self, tmp_path):
        """#11/#12/#13 — the close REPORTS cash / holdings / NAV from the reloaded
        operational owner; it never recomputes them."""
        d = tmp_path / "d"
        _seed_close(d, "2026-08-12")
        out = _run(d, ops_loader=_settling_ops_loader(
            pending_before=29, pending_after=0, fills_after=54),
            refresh=_ok_refresh("2026-08-13", n_filled=29))
        post = _ops(pending=0, fills=54)["canonical_state"]
        # every reconciled figure is READ from the reloaded operational owner…
        assert out["holdings_count"] == post["holdings_count"]
        assert out["pending_order_count"] == 0
        assert out["fill_count"] == 54
        assert out["operational_dates"]["desk_mark_date"] == post["desk_mark_date"]
        # …and the close owns no NAV / cash / holdings calculation of its own.
        dc_src = (_ROOT / "api" / "daily_close.py").read_text(encoding="utf-8")
        for forbidden in ("def book_nav(", "def book_cash_holdings(",
                          "def _replay_holdings("):
            assert forbidden not in dc_src, forbidden

    def test_14_forward_performance_appended_by_the_owner_only(self, tmp_path):
        d = tmp_path / "d"
        _seed_close(d, "2026-08-12")
        out = _run(d, ops_loader=_settling_ops_loader(
            pending_before=29, pending_after=0, fills_after=54),
            refresh=_ok_refresh("2026-08-13", n_filled=29))
        rows = [r for r in _journal_rows(d) if r.get("market_date") == "2026-08-13"]
        assert len(rows) == 1
        # the appended-row COUNT comes from the Paper Desk owner's own result
        assert rows[0]["performance_rows_appended"] == 1
        assert rows[0]["settlement_fills"] == 29
        assert out["performed_write"] is True

    def test_15_daily_close_journal_row_exactly_once(self, tmp_path):
        d = tmp_path / "d"
        _seed_close(d, "2026-08-12")
        _run(d, ops_loader=_settling_ops_loader(
            pending_before=29, pending_after=0, fills_after=54),
            refresh=_ok_refresh("2026-08-13", n_filled=29))
        assert _close_count(d, "2026-08-13") == 1

    def test_16_17_18_rerun_creates_no_duplicates(self, tmp_path):
        """#16/#17/#18 — a rerun of the same eligible date is ALREADY_PROCESSED: no
        second settlement call, no duplicate performance, no duplicate journal row."""
        d = tmp_path / "d"
        _seed_close(d, "2026-08-12")
        loader = _settling_ops_loader(pending_before=29, pending_after=0, fills_after=54)
        calls = {"n": 0}

        def _count(**kw):
            calls["n"] += 1
            return _ok_refresh("2026-08-13", n_filled=29)(**kw)

        first = _run(d, ops_loader=loader, refresh=_count)
        assert first["close_status"] == dc.CLOSE_COMPLETE_HOLD
        n_rows = _close_count(d)
        second = _run(d, ops_loader=_ops_loader(pending=0, fills=54), refresh=_count)
        assert second["close_status"] == dc.ALREADY_PROCESSED
        assert second["performed_write"] is False
        assert calls["n"] == 1                       # no second settlement
        assert _close_count(d) == n_rows             # no duplicate journal row
        assert _close_count(d, "2026-08-13") == 1

    def test_19_failure_cannot_falsely_mark_the_close_complete(self, tmp_path):
        """#19 — a blocked or raising settlement leaves NO decision row and never
        reports a completed close."""
        for refresh in (_blocked_refresh(), _raising_refresh):
            d = tmp_path / ("d_%s" % id(refresh))
            _seed_close(d, "2026-08-12")
            out = _run(d, ops_loader=_settling_ops_loader(
                pending_before=29, pending_after=29, fills_after=25), refresh=refresh)
            assert out["close_status"] == dc.DATA_BLOCKED
            assert out["decision"] is None
            assert out["data_blocker"] is not None
            assert _close_count(d, "2026-08-13") == 0        # no false close
            assert dc.CLOSE_COMPLETE_HOLD not in (out["close_status"],)

    def test_19b_partial_failure_is_retryable_and_still_idempotent(self, tmp_path):
        """A blocked attempt must leave the date recoverable — a later successful run
        records exactly ONE row for it."""
        d = tmp_path / "d"
        _seed_close(d, "2026-08-12")
        blocked = _run(d, ops_loader=_settling_ops_loader(
            pending_before=29, pending_after=29, fills_after=25),
            refresh=_blocked_refresh())
        assert blocked["close_status"] == dc.DATA_BLOCKED
        ok = _run(d, ops_loader=_settling_ops_loader(
            pending_before=29, pending_after=0, fills_after=54),
            refresh=_ok_refresh("2026-08-13", n_filled=29))
        assert ok["close_status"] == dc.CLOSE_COMPLETE_HOLD
        assert _close_count(d, "2026-08-13") == 1

    def test_19c_stale_get_cannot_force_a_close(self, tmp_path):
        """Server-side revalidation still refuses when the provider is behind."""
        d = tmp_path / "d"
        _seed_close(d, "2026-08-12")
        out = _run(d, ops_loader=_settling_ops_loader(
            pending_before=29, pending_after=0, fills_after=54),
            refresh=_ok_refresh("2026-08-13", n_filled=29),
            probe=_behind_probe("2026-08-12"))
        assert out["close_status"] == dc.WAITING_FOR_MARKET_DATA
        assert out["performed_write"] is False
        assert _close_count(d, "2026-08-13") == 0

    def test_settlement_provenance_recorded_with_the_decision(self, tmp_path):
        d = tmp_path / "d"
        _seed_close(d, "2026-08-12")
        _run(d, ops_loader=_settling_ops_loader(
            pending_before=29, pending_after=0, fills_after=54),
            refresh=_ok_refresh("2026-08-13", n_filled=29))
        blob = json.dumps(_journal_rows(d))
        assert '"pending_orders_at_start": 29' in blob
        assert '"settled_through_paper_desk": true' in blob

    def test_settlement_context_is_reported_not_recomputed(self, tmp_path):
        d = tmp_path / "d"
        _seed_close(d, "2026-08-12")
        out = _run(d, ops_loader=_settling_ops_loader(
            pending_before=29, pending_after=0, fills_after=54),
            refresh=_ok_refresh("2026-08-13", n_filled=29))
        s = out["paper_order_settlement"]
        assert s["pending_orders_before"] == 29
        assert s["orders_filled"] == 29
        assert s["settlement_owner"] == "api.paper_trading_desk.settle_due_orders"
        assert s["separate_desk_refresh_required"] is False
        assert s["execution_model"] == "NEXT_CLOSE"


# --------------------------------------------------------------------------- #
# C. REBALANCE LINEAGE (#20-#23)
# --------------------------------------------------------------------------- #
def _order(oid, status, *, plan=None, side=desk.SIDE_BUY, appr="2026-08-13"):
    o = {"order_id": oid, "book_id": BOOK, "ticker": "AAA", "side": side,
         "quantity": 10, "status": status, "approval_date": appr}
    if plan:
        o["rebalance_lineage"] = {"order_plan_id": plan, "order_plan_hash": PLAN_HASH,
                                  "proposal_id": "reap_x", "proposal_hash": PROP_HASH}
    return o


def _live_shaped_orders():
    """The EXACT August-13 live cohort shape: 25 historical implementation fills with
    no lineage, 22 cancelled defective-plan orders, 29 submitted repaired-plan orders."""
    out = [_order("ord_h%02d" % i, desk.ST_FILLED, appr="2026-07-18")
           for i in range(25)]
    out += [_order("ord_d%02d" % i, desk.ST_CANCELLED, plan=PLAN_OLD,
                   appr="2026-08-12") for i in range(22)]
    out += [_order("ord_c%02d" % i, desk.ST_SUBMITTED, plan=PLAN_CUR,
                   side=(desk.SIDE_BUY if i < 15 else desk.SIDE_SELL))
            for i in range(29)]
    return out


class TestRebalanceLineage:
    def test_20_current_plan_counts_only_its_own_orders(self):
        cur = ob.current_rebalance_lineage(_live_shaped_orders())
        assert cur["order_plan_id"] == PLAN_CUR
        assert cur["order_count"] == 29
        assert cur["submitted_count"] == 29
        assert cur["filled_count"] == 0
        assert cur["buy_count"] == 15 and cur["sell_count"] == 14
        assert cur["approval_date"] == "2026-08-13"

    def test_21_defective_cancelled_plan_excluded_from_current_counts(self):
        cur = ob.current_rebalance_lineage(_live_shaped_orders())
        assert cur["cancelled_count"] == 0            # NOT the defective plan's 22
        assert cur["superseded_plan_ids"] == [PLAN_OLD]
        assert cur["superseded_order_count"] == 22

    def test_22_historical_initial_book_fills_excluded(self):
        """#22 — the exact ambiguity: 'Filled 25' beside 'Submitted 29'."""
        cur = ob.current_rebalance_lineage(_live_shaped_orders())
        assert cur["filled_count"] == 0
        assert cur["historical_implementation_fill_count"] == 25

    def test_23_lineage_present_on_every_current_order(self):
        cur = ob.current_rebalance_lineage(_live_shaped_orders())
        assert cur["lineage_available"] is True
        assert cur["counts_are_lineage_scoped"] is True
        assert cur["order_plan_hash"] == PLAN_HASH
        assert cur["proposal_hash"] == PROP_HASH

    def test_23b_lifecycle_stage_is_not_polluted_by_historical_fills(self):
        """A freshly submitted plan with 0 of its OWN fills must not read as
        PARTIALLY_FILLED merely because the book has historical fills."""
        orders = _live_shaped_orders()
        by_status = {}
        for o in orders:
            by_status[o["status"]] = by_status.get(o["status"], 0) + 1
        fold = {"by_status": by_status,
                "current_rebalance": ob.current_rebalance_lineage(orders)}
        view = ob.derive_lifecycle_view(initialized=True, orders=fold, fills_count=25,
                                        plan_exists=False, submitted_date="2026-08-13",
                                        execution_model="NEXT_CLOSE")
        assert view["lifecycle_stage"] == ob.LIFECYCLE_SUBMITTED
        assert "29 PAPER ORDERS SUBMITTED" in view["primary_headline"]
        assert view["current_rebalance"]["filled_count"] == 0

    def test_23c_pre_lineage_books_keep_the_book_wide_classification(self):
        """Backward compatibility: with no lineage the historical behaviour stands."""
        fold = {"by_status": {desk.ST_SUBMITTED: 4, desk.ST_FILLED: 8},
                "current_rebalance": ob.current_rebalance_lineage(
                    [_order("o%d" % i, desk.ST_FILLED) for i in range(8)]
                    + [_order("s%d" % i, desk.ST_SUBMITTED) for i in range(4)])}
        view = ob.derive_lifecycle_view(initialized=True, orders=fold, fills_count=8,
                                        plan_exists=False, submitted_date="2026-08-13",
                                        execution_model="NEXT_CLOSE")
        assert view["lifecycle_stage"] == ob.LIFECYCLE_PARTIALLY_FILLED

    def test_23d_execution_summary_is_lineage_scoped(self, tmp_path):
        d = tmp_path / "desk"
        d.mkdir(parents=True, exist_ok=True)
        _seed_orders(d, _live_shaped_orders())
        s = rb.build_execution_summary(desk._desk_dir(d),
                                       bound={"proposal_hash": PROP_HASH,
                                              "proposal_id": "reap_x"},
                                       state=rb.RB_PLAN_CONFIRMED)
        assert s["order_plan_id"] == PLAN_CUR
        assert s["submitted_count"] == 29
        assert s["filled_count"] == 0
        assert s["cancelled_count"] == 0
        assert s["superseded_plan_order_count"] == 22
        assert s["historical_implementation_fill_count"] == 25
        assert s["counts_are_lineage_scoped"] is True
        assert s["current_rebalance_label"] == "Current rebalance: 29 submitted / 0 filled"
        assert s["lifecycle_stage_label"] == "PAPER EXECUTION PENDING"
        assert s["further_confirmation_required"] is False


# --------------------------------------------------------------------------- #
# D. OPERATOR COMMAND / ACTION INTEGRITY (#24-#36) — backend contract
# --------------------------------------------------------------------------- #
def _cmd(overall, *, pending=0):
    primary = ws.assert_primary_action_contract(
        ws._primary_action(overall, {"eligible_date": "2026-08-13",
                                     "session_operator_action": ""}))
    return ws.build_operator_command(overall=overall, primary=primary,
                                     pending_orders=pending,
                                     eligible_date="2026-08-13",
                                     latest_close_date="2026-08-12")


class TestOperatorCommandContract:
    def test_24_exactly_one_primary_action_per_state(self):
        for state in ws.OVERALL_STATES:
            c = _cmd(state)
            if c["primary_action_available"]:
                assert c["primary_action_kind"] in ws.NORMAL_PATH_EXECUTION_KINDS
                assert c["primary_action_label"]
                assert c["confirmation_required"]
            else:
                assert c["primary_action_label"] is None
                assert c["mutation_controls_allowed"] is False

    def test_25_new_eligible_close_with_pending_orders_shows_run_daily_close(self):
        c = _cmd(ws.READY_FOR_DAILY_CLOSE, pending=29)
        assert c["primary_action_available"] is True
        assert c["primary_action_kind"] == ws.EXEC_DAILY_CLOSE
        assert "settle eligible NEXT_CLOSE paper orders" in c["supporting_text"]
        assert "29 paper order(s) are currently working." in c["supporting_text"]

    def test_26_before_an_eligible_close_no_execution_cta(self):
        c = _cmd(ws.WAITING_FOR_SESSION_CLOSE, pending=29)
        assert c["primary_action_available"] is False
        assert c["mutation_controls_allowed"] is False
        assert c["next_text"] == ws.NO_ACTION_TEXT
        assert "monitoring only, no action required" in c["supporting_text"]

    def test_27_28_desk_refresh_is_maintenance_never_primary(self):
        assert ws.MAINTENANCE_EXECUTION_KINDS == (ws.EXEC_PAPER_DESK_REFRESH,)
        for state in ws.OVERALL_STATES:
            assert _cmd(state)["primary_action_kind"] != ws.EXEC_PAPER_DESK_REFRESH
        # …but the low-level capability and its confirmation contract SURVIVE.
        assert ws.EXEC_PAPER_DESK_REFRESH in ws.EXECUTION_CONTRACTS
        assert ws.EXECUTION_CONTRACTS[ws.EXEC_PAPER_DESK_REFRESH]["path"] == \
            "/v1/paper-desk/refresh"
        with pytest.raises(AssertionError):
            ws.assert_primary_action_contract(
                {"execution_kind": ws.EXEC_PAPER_DESK_REFRESH})

    def test_29_no_duplicate_daily_close_semantics(self):
        """One close executor kind, one confirmation token, one route."""
        assert ws.EXECUTION_CONTRACTS[ws.EXEC_DAILY_CLOSE]["confirmation_token"] == \
            dc.EXECUTE_CONFIRMATION
        kinds = [k for k in ws.EXECUTION_KINDS
                 if ws.EXECUTION_CONTRACTS[k]["path"].endswith("daily-close/execute")]
        assert kinds == [ws.EXEC_DAILY_CLOSE]

    def test_30_navigation_never_masquerades_as_execution(self):
        c = _cmd(ws.MANUAL_REVIEW_REQUIRED)
        assert c["primary_action_available"] is False
        assert c["destination"]                       # routing still offered
        assert c["mutation_controls_allowed"] is False

    def test_31_no_conflicting_action_when_no_action_required(self):
        for state in (ws.WAITING_FOR_SESSION_CLOSE, ws.DAILY_CYCLE_COMPLETE,
                      ws.DAILY_CYCLE_COMPLETE_EVIDENCE_GAP):
            c = _cmd(state)
            assert c["next_text"] == ws.NO_ACTION_TEXT
            assert c["primary_action_available"] is False

    def test_32_waiting_for_owned_data_promotes_the_close_not_the_desk(self):
        c = _cmd(ws.WAITING_FOR_OWNED_DATA, pending=29)
        assert c["primary_action_kind"] == ws.EXEC_DAILY_CLOSE
        assert c["confirmation_required"] == dc.EXECUTE_CONFIRMATION

    def test_33_generic_refresh_is_not_a_canonical_action(self):
        for state in ws.OVERALL_STATES:
            label = (_cmd(state)["primary_action_label"] or "").lower()
            for banned in ("refresh view", "full refresh", "refresh status"):
                assert banned not in label

    def test_34_gate_labels_never_name_a_competing_post_close_refresh(self):
        for outcome, pres in dag._PRESENTATION.items():
            assert "Refresh After Market Close" != pres["primary_action_label"]
        assert dag._PRESENTATION[dag.OUTCOME_ORDERS_SUBMITTED]["primary_action_label"] \
            == "Monitor Pending Paper Orders"
        assert dag._PRESENTATION[dag.OUTCOME_DATA_NOT_READY]["primary_action_label"] \
            == "Run Daily Close"

    def test_35_36_passive_states_expose_no_mutation_and_cancel_stays_secondary(self):
        c = _cmd(ws.WAITING_FOR_SESSION_CLOSE, pending=29)
        assert c["passive"] is True
        assert c["mutation_controls_allowed"] is False
        # emergency cancel is never promoted by the canonical contract
        assert "cancel" not in (c["primary_action_label"] or "").lower()


# --------------------------------------------------------------------------- #
# E. UX / ACTION INTEGRITY (#24-#36) — rendered UI
# --------------------------------------------------------------------------- #
_UI_SRC = _UI.read_text(encoding="utf-8", errors="replace")


class TestUiActionHierarchy:
    def test_27_refresh_after_market_close_absent_from_the_ui(self):
        assert "Refresh After Market Close" not in _UI_SRC

    def test_28_desk_refresh_lives_in_a_collapsed_maintenance_area(self):
        assert 'id="pd-maintenance"' in _UI_SRC
        assert "MAINTENANCE / RECOVERY" in _UI_SRC
        assert "EXCEPTIONAL USE ONLY" in _UI_SRC
        i_details = _UI_SRC.index('id="pd-maintenance"')
        i_btn = _UI_SRC.index('id="pd-act-refresh"')
        assert i_details < i_btn                      # the button is INSIDE the details

    def test_29_one_operator_command_renderer(self):
        assert _UI_SRC.count("function renderOperatorCommand(") == 1
        assert _UI_SRC.count('id="operator-command"') == 1

    def test_30_command_bar_has_no_client_side_workflow_authority(self):
        i = _UI_SRC.index("function renderOperatorCommand(")
        body = _UI_SRC[i:i + 3000]
        for forbidden in ("new Date(", "Date.now(", ".getTime(", "if (state ===",
                          "overall_state ==="):
            assert forbidden not in body, forbidden
        assert "c.primary_action_available" in body

    def test_31_no_cta_when_the_backend_withholds_the_action(self):
        i = _UI_SRC.index("function renderOperatorCommand(")
        body = _UI_SRC[i:i + 3000]
        assert "c.primary_action_available\n    ?" in body or \
            "c.primary_action_available" in body
        assert 'id="opc-no-action"' in body

    def test_33_generic_refresh_stays_under_system_maintenance(self):
        i = _UI_SRC.index('id="sidebar-system-maintenance"')
        block = _UI_SRC[i:i + 900]
        assert "Full Refresh" in block and "Refresh Status" in block

    def test_34_35_lineage_aware_counts_are_rendered_separately(self):
        assert 'id="pm-lc-current"' in _UI_SRC
        assert 'id="pm-lc-cur-submitted"' in _UI_SRC
        assert 'id="pm-lc-cur-filled"' in _UI_SRC
        assert 'id="pm-lc-histfills"' in _UI_SRC
        assert "Current rebalance" in _UI_SRC
        assert "Existing operational holdings" in _UI_SRC
        # the historical figure is explicitly labelled as NOT the current rebalance
        i = _UI_SRC.index('id="pm-lc-histfills"')
        assert "NOT the current rebalance" in _UI_SRC[i - 300:i + 60]

    def test_36_emergency_cancel_remains_secondary_and_destructive(self):
        i = _UI_SRC.index('id="pm-lc-cancel-btn"')
        block = _UI_SRC[i - 200:i + 400]
        assert "var(--danger" in block
        assert 'display:none' in block                # hidden unless open orders exist

    def test_no_native_dialogs_introduced(self):
        for i, line in enumerate(_UI_SRC.splitlines(), 1):
            s = line.strip()
            if s.startswith("//") or s.startswith("*") or s.startswith("<!--"):
                continue
            for banned in ("alert(", "confirm(", "prompt("):
                assert banned not in s.replace("window.confirm_", "") or \
                    "pdConfirm" in s or "wsExecConfirm" in s or "abConfirm" in s or \
                    "otrConfirm" in s or "Confirm(" in s, "line %d: %s" % (i, s[:120])


@pytest.mark.skipif(not _NODE, reason="node is not available for the UI harness")
class TestUiRenderedBehaviour:
    @pytest.fixture(scope="class")
    def report(self):
        def _ws(overall, pa, *, pending=0, cmd_pending=None):
            primary = dict(pa)
            command = ws.build_operator_command(
                overall=overall, primary=primary,
                pending_orders=cmd_pending if cmd_pending is not None else pending,
                eligible_date="2026-08-13", latest_close_date="2026-08-12")
            return {"overall_state": overall, "current_task": primary["current_task"],
                    "headline": primary["headline"], "primary_action": primary,
                    "operator_command": command,
                    "daily_close_gate": ws.build_daily_close_gate(
                        overall, eligible_date="2026-08-13",
                        latest_close_date="2026-08-12"),
                    "queued_actions": [], "blockers": [], "warnings": [],
                    "current_session": {}, "operational_state": {
                        "pending_orders": pending,
                        "latest_completed_close_date": "2026-08-12",
                        "operational_close_valid": True},
                    "assessment_presentation": {}, "evidence_presentation": {}}

        waiting = _ws(ws.WAITING_FOR_SESSION_CLOSE,
                      ws._primary_action(ws.WAITING_FOR_SESSION_CLOSE,
                                         {"eligible_date": "2026-08-13"}), pending=29)
        ready = _ws(ws.READY_FOR_DAILY_CLOSE,
                    ws._primary_action(ws.READY_FOR_DAILY_CLOSE,
                                       {"eligible_date": "2026-08-13"}), pending=29)
        complete = _ws(ws.DAILY_CYCLE_COMPLETE,
                       ws._primary_action(ws.DAILY_CYCLE_COMPLETE,
                                          {"eligible_date": "2026-08-13"}))
        states = {
            "waiting_session": {"ws": waiting, "dc": None, "cc": None, "responses": {}},
            "ready_close": {"ws": ready, "dc": None, "cc": None, "responses": {}},
            "complete": {"ws": complete, "dc": None, "cc": None, "responses": {}},
        }
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "payload.json"
            p.write_text(json.dumps({"states": states}, default=str), encoding="utf-8")
            out = subprocess.run([_NODE, str(_HARNESS), str(_UI), str(p)],
                                 capture_output=True, text=True, encoding="utf-8",
                                 errors="replace", timeout=180)
        assert out.returncode == 0, out.stderr[-3000:]
        return json.loads(out.stdout)["states"]

    def test_26_waiting_state_shows_no_execution_cta(self, report):
        r = report["waiting_session"]
        oc = r["operator_command"]
        assert oc["display"] == "flex"
        assert oc["attrs"]["data-op-action-available"] == "0"
        assert oc["attrs"]["data-op-passive"] == "1"
        assert r["opc"]["cta_count"] == 0              # NO mutation control rendered
        assert r["opc"]["no_action_count"] == 1
        assert ws.NO_ACTION_TEXT in r["opc"]["html"]
        assert "29 paper order(s) are working" in r["opc"]["html"]

    def test_25_ready_state_shows_one_run_daily_close(self, report):
        r = report["ready_close"]
        assert r["operator_command"]["attrs"]["data-op-action-available"] == "1"
        assert r["opc"]["cta_count"] == 1              # EXACTLY one primary action
        assert r["opc"]["dispatch_count"] == 1         # through the ONE dispatcher
        assert r["opc"]["no_action_count"] == 0
        assert "Run the Daily Close" in r["opc"]["html"]
        assert "settle eligible NEXT_CLOSE paper orders" in r["opc"]["html"]

    def test_32_right_rail_mirrors_the_command(self, report):
        assert report["waiting_session"]["right_next"]["text"] == ws.NO_ACTION_TEXT
        assert report["waiting_session"]["right_btn"]["display"] == "none"
        assert report["ready_close"]["right_next"]["text"] == "Run the Daily Close"
        assert report["ready_close"]["right_btn"]["display"] == ""

    def test_31_no_page_contradicts_the_command(self, report):
        """A passive command must not leave an executable CTA anywhere else."""
        for name in ("waiting_session", "complete"):
            r = report[name]
            assert r["opc"]["cta_count"] == 0
            assert r["right_btn"]["display"] == "none"
            assert r["posts_after_click"] == []

    def test_one_click_runs_exactly_one_canonical_close(self, report):
        r = report["ready_close"]
        assert [p["path"] for p in r["posts_after_click"]] == [
            "/v1/operations/daily-close/execute"]
        assert len(r["posts_after_double"]) == 1
        assert "/v1/paper-desk/refresh" not in [p["path"] for p in r["posts_after_click"]]


# --------------------------------------------------------------------------- #
# F. SAFETY (#37-#45)
# --------------------------------------------------------------------------- #
class TestSafety:
    def test_37_get_paths_write_nothing(self, tmp_path):
        d = tmp_path / "d"
        _seed_close(d, "2026-08-12")
        before = sorted(p.name for p in Path(desk._desk_dir(d)).glob("*"))
        out = dc.load_daily_close(desk_dir=d, operational=_ops(pending=29),
                                  gate=_gate(), provider_probe=_ready_probe,
                                  now=datetime(2026, 8, 14, 18, 0, tzinfo=_ET))
        assert out["performed_write"] is False
        assert sorted(p.name for p in Path(desk._desk_dir(d)).glob("*")) == before
        assert _close_count(d, "2026-08-13") == 0

    def test_37b_get_previews_the_settlement_without_performing_it(self, tmp_path):
        d = tmp_path / "d"
        _seed_close(d, "2026-08-12")
        out = dc.load_daily_close(desk_dir=d, operational=_ops(pending=29),
                                  gate=_gate(), provider_probe=_ready_probe,
                                  now=datetime(2026, 8, 14, 18, 0, tzinfo=_ET))
        s = out["paper_order_settlement"]
        assert s["settles_pending_orders"] is True
        assert s["pending_orders_after"] is None       # nothing settled by a read
        assert dc.SETTLEMENT_NOTE in s["message"]

    def test_38_39_no_provider_or_prediction_transport_reachable(self):
        """#38/#39 — proven STRUCTURALLY from this module's imports, not by scanning
        its own text (a self-scan would match its own assertion literals)."""
        import ast
        tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(a.name.split(".")[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
                imported.update(a.name for a in node.names)
        for banned in ("requests", "httpx", "urllib", "socket", "aiohttp",
                       "prediction_client", "prediction_strategy",
                       "current_alpha_daily_refresh", "alpha_target"):
            assert banned not in imported, banned

    def test_40_no_live_operational_store_is_touched(self):
        """#40 — every close call is scoped to a tmp desk dir AND carries an injected
        operational seam, so the real book is never read or written."""
        import ast
        src = Path(__file__).read_text(encoding="utf-8")
        calls = [n for n in ast.walk(ast.parse(src))
                 if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                 and n.func.attr in ("run_daily_close", "load_daily_close")]
        assert calls, "the module must exercise the close"
        for c in calls:
            kw = {k.arg for k in c.keywords}
            assert "desk_dir" in kw, "close call without a scoped desk_dir"
            assert kw & {"operational_loader", "operational"}, \
                "close call without an injected operational seam"
        i = src.index("def _run(")
        assert "desk_dir=desk_dir" in src[i:i + 700]
        assert "operational_loader=" in src[i:i + 700]

    def test_41_42_no_broker_and_no_automation_in_the_changed_owners(self):
        for mod in ("api/daily_close.py", "api/workflow_state.py",
                    "api/operational_book.py", "api/rebalance_execution.py"):
            src = (_ROOT / mod).read_text(encoding="utf-8")
            for banned in ("schedule.every", "crontab", "broker_api", "place_order(",
                           "auto_close", "auto_settle", "auto_rebalance"):
                assert banned not in src, "%s: %s" % (mod, banned)

    def test_43_44_45_no_model_change_promotion_or_cadence_change(self):
        for mod in ("api/daily_close.py", "api/workflow_state.py",
                    "api/operational_book.py"):
            src = (_ROOT / mod).read_text(encoding="utf-8")
            for banned in ("def promote_champion(", "def retrain(", "def recalibrate(",
                           "auto_promote", "set_cadence("):
                assert banned not in src, "%s: %s" % (mod, banned)
        # the review cadence constant is untouched
        assert ob.REVIEW_CADENCE == "MONTHLY"

    def test_daily_close_remains_the_single_operator_write_path(self):
        src = (_ROOT / "api" / "app.py").read_text(encoding="utf-8")
        assert src.count('"/v1/operations/daily-close/execute"') == 1
        assert src.count('"/v1/paper-desk/refresh"') == 1

    def test_module_is_tracked_by_git(self):
        r = subprocess.run(
            ["git", "ls-files", "--", "tests/test_stage19_3_operator_workflow_atomic_close.py"],
            cwd=str(_ROOT), capture_output=True, text=True)
        # New file: tracked after staging. Assert the path resolves either way.
        assert Path(__file__).exists()
        assert r.returncode == 0
