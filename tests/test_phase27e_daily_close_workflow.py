"""
tests/test_phase27e_daily_close_workflow.py — Phase 27E EXPLICIT DAILY CLOSE.

Fully offline. Two flavors:

  * fast contract tests inject fake operational-book / gate / refresh seams and a
    tmp desk dir, so the canonical status resolution, decision recording,
    idempotency, token gate, proposal shape and safety are deterministic without
    the engine or the network;
  * real-desk P&L / idempotency tests reuse the Phase 27A/27B.1/27B.5 offline
    harness (``_filled_world`` builds a FILLED Alpha Paper Book #1 from tmp desk
    ledgers + an injectable marks downloader) so NAV / daily+cumulative P&L /
    "one performance row" / "one decision-journal row" / ALREADY_PROCESSED are
    proven against the REAL append-only ledgers — never the user's real book, no
    live broker, no automation.

Every write happens ONLY inside a tmp desk dir of this harness. The daily close
never creates a paper order, never touches a broker, and changes no model /
champion / weight / sleeve.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from paper_trader.api import daily_close as dc
from paper_trader.api import paper_trading_desk as desk

from tests.test_phase27a_paper_operations import (  # reuse the offline harness
    _AUTH, _D0, _TICKS, _dl, _marks_table, client, env,  # noqa: F401
)
from tests.test_phase27b1_operational_surface_cutover import env27b1  # noqa: F401
from tests.test_phase27b5_operator_flow import _filled_world

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"


# --------------------------------------------------------------------------- #
# Fake seams for the fast contract tests
# --------------------------------------------------------------------------- #
def _fake_ops(*, pending=0, fills=8, initialized=True, nav=99881.0, cash=1881.0,
              holdings_count=8, valuation_date="2026-07-20", starting=100000.0,
              lifecycle="FILLED"):
    cs = {
        "pending_order_count": pending, "fill_count": fills, "lifecycle_stage": lifecycle,
        "nav": nav, "cash": cash, "holdings_count": holdings_count,
        "valuation_date": valuation_date, "desk_mark_date": valuation_date,
        "next_review_date": "2026-08-01", "review_due": False, "review_cadence": "MONTHLY",
    }
    ob_book = {
        "book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1",
        "initialized": initialized, "starting_capital": starting, "initial_capital": starting,
        "holdings_count": holdings_count, "pending_order_count": pending, "fill_count": fills,
    }
    return {"canonical_state": cs, "operational_book": ob_book}


def _checks(triggered=0, unavailable=0, total=13):
    line = "%d checks completed · %d triggered · %d unavailable" % (total, triggered, unavailable)
    return {"total": total, "triggered": triggered, "not_available": unavailable, "line": line}


def _fake_gate(outcome="NO_ACTION_TODAY", pcount=0, adds=None, rems=None, resizes=None,
               triggered=0):
    return {
        "outcome": outcome, "outcome_label": outcome.replace("_", " "),
        "target_state": ("CURRENT_ALIGNED" if outcome == "NO_ACTION_TODAY" else "PROPOSAL_READY"),
        "target_state_label": ("CURRENT — ALIGNED WITH HOLDINGS" if outcome == "NO_ACTION_TODAY"
                               else "PROPOSAL READY — MANUAL REVIEW REQUIRED"),
        "checks_performed": [{"code": "DATA_FRESHNESS", "status": "PASS", "label": "Data freshness"}],
        "checks_summary": _checks(triggered=triggered),
        "proposed_additions": adds or [], "proposed_removals": rems or [],
        "proposed_resizes": resizes or [], "blocked_changes": [],
        "proposed_change_count": pcount, "estimated_turnover": 0.04, "estimated_cost": 0.001,
        "trigger_categories": (["MATERIAL_TARGET_MEMBERSHIP_CHANGE"] if pcount else []),
        "trigger_reasons": (["NVDA entered the combined target."] if pcount else []),
        "target_actual_match": (pcount == 0), "operational_dates": {}, "warnings": [],
    }


def _ok_refresh(closed="2026-07-21", appended=1, filled=0):
    def _fn(**kw):
        return {"status": desk.S_OK, "performed_write": True,
                "resulting_desk_mark_date": closed, "latest_completed_market_date": closed,
                "settlement": {"n_filled": filled}, "performance": {"n_appended": appended}}
    return _fn


def _blocked_refresh():
    def _fn(**kw):
        return {"status": desk.S_MARKS_BLOCKED, "performed_write": False,
                "resulting_desk_mark_date": None, "blockers": ["NO_COMPLETED_MARKS_RECORDED"],
                "message": "no completed close available"}
    return _fn


def _hold_loader(*a, **k):
    return _fake_gate("NO_ACTION_TODAY", 0)


def _prop_loader(*a, **k):
    return _fake_gate("PROPOSAL_READY", 2,
                      adds=[{"ticker": "NVDA", "target_weight": 0.04}],
                      rems=[{"ticker": "XYZ", "current_weight": 0.05}], triggered=2)


def _run(desk_dir, *, today="2026-07-22", ops=None, gate=_hold_loader, refresh=None,
         confirm=dc.EXECUTE_CONFIRMATION):
    return dc.run_daily_close(
        confirm=confirm, today=today, desk_dir=desk_dir,
        operational_loader=(lambda t: ops if ops is not None else _fake_ops()),
        gate_loader=gate, refresh_fn=refresh or _ok_refresh())


def _load(desk_dir, *, today="2026-07-22", ops=None, gate=_hold_loader):
    return dc.load_daily_close(
        desk_dir=desk_dir, today=today,
        operational=(ops if ops is not None else _fake_ops()),
        gate=(gate() if callable(gate) else gate))


def _journal_count(desk_dir, book_id="alpha_paper_book_1"):
    return len(dc._journal_rows(desk._desk_dir(desk_dir)))


# --------------------------------------------------------------------------- #
# 1. Pure status resolver — the seven canonical statuses
# --------------------------------------------------------------------------- #
class TestPureResolver:
    def _r(self, **kw):
        base = dict(initialized=True, book_active=True, pending_orders=0,
                    latest_eligible="2026-07-22", last_processed_date=None,
                    processed_decision_for_latest=None)
        base.update(kw)
        return dc.resolve_daily_close_status(**base)

    def test_due_when_active_unprocessed(self):
        assert self._r() == dc.CLOSE_DUE

    def test_hold_when_processed_hold(self):
        assert self._r(last_processed_date="2026-07-22",
                       processed_decision_for_latest=dc.DECISION_HOLD) == dc.CLOSE_COMPLETE_HOLD

    def test_proposal_when_processed_rebalance(self):
        assert self._r(last_processed_date="2026-07-22",
                       processed_decision_for_latest=dc.DECISION_REBALANCE) == dc.REBALANCE_PROPOSAL_READY

    def test_orders_when_pending(self):
        assert self._r(pending_orders=3) == dc.PAPER_ORDERS_SUBMITTED

    def test_awaiting_when_not_active(self):
        assert self._r(book_active=False) == dc.AWAITING_ELIGIBLE_CLOSE
        assert self._r(initialized=False) == dc.AWAITING_ELIGIBLE_CLOSE

    def test_awaiting_when_latest_already_processed_no_record(self):
        assert self._r(last_processed_date="2026-07-22") == dc.AWAITING_ELIGIBLE_CLOSE

    def test_data_blocked_recorded_surfaces(self):
        assert self._r(last_processed_date="2026-07-22",
                       processed_decision_for_latest=dc.DECISION_DATA_BLOCKED) == dc.DATA_BLOCKED

    def test_every_status_has_presentation(self):
        for s in dc.ALL_CLOSE_STATUSES:
            assert s in dc._PRESENTATION
            assert dc._PRESENTATION[s]["primary_action_label"]


# --------------------------------------------------------------------------- #
# 4. Latest eligible completed market date
# --------------------------------------------------------------------------- #
class TestLatestEligible:
    def test_weekday_before_reference(self):
        # 2026-07-22 is a Wednesday -> latest completed weekday = Tuesday 2026-07-21
        assert dc._latest_eligible_market_date(today="2026-07-22") == "2026-07-21"

    def test_walks_back_over_weekend(self):
        # 2026-07-20 is a Monday -> latest completed weekday = Friday 2026-07-17
        assert dc._latest_eligible_market_date(today="2026-07-20") == "2026-07-17"


# --------------------------------------------------------------------------- #
# 3. + token gate + no-orders + safety (fast contract)
# --------------------------------------------------------------------------- #
class TestExecuteContract:
    def test_requires_confirmation_token(self, tmp_path):
        out = dc.run_daily_close(confirm="WRONG", today="2026-07-22", desk_dir=tmp_path / "d")
        assert out["status"] == "DAILY_CLOSE_CONFIRM_REQUIRED"
        assert out["performed_write"] is False
        assert _journal_count(tmp_path / "d") == 0

    def test_new_date_records_one_hold_row(self, tmp_path):
        d = tmp_path / "d"
        out = _run(d)
        assert out["close_status"] == dc.CLOSE_COMPLETE_HOLD
        assert out["decision"] == dc.DECISION_HOLD
        assert out["performed_write"] is True
        assert out["last_processed_market_date"] == "2026-07-21"
        assert _journal_count(d) == 1

    def test_rerun_same_date_already_processed_no_duplicate(self, tmp_path):
        d = tmp_path / "d"
        _run(d)
        assert _journal_count(d) == 1
        again = _run(d)
        assert again["close_status"] == dc.ALREADY_PROCESSED
        assert again["performed_write"] is False
        assert _journal_count(d) == 1                      # no duplicate row

    def test_proposal_when_trigger_fires(self, tmp_path):
        d = tmp_path / "d"
        out = _run(d, gate=_prop_loader)
        assert out["close_status"] == dc.REBALANCE_PROPOSAL_READY
        assert out["decision"] == dc.DECISION_REBALANCE
        assert out["proposal"] is not None
        assert out["proposal"]["proposed_change_count"] == 2
        assert out["proposal"]["creates_orders"] is False

    def test_proposal_contains_affected_positions_only(self, tmp_path):
        d = tmp_path / "d"
        out = _run(d, gate=_prop_loader)
        adds = [a["ticker"] for a in out["proposal"]["proposed_additions"]]
        rems = [r["ticker"] for r in out["proposal"]["proposed_removals"]]
        assert adds == ["NVDA"] and rems == ["XYZ"]        # only affected names, not all 8

    def test_orders_pending_defers(self, tmp_path):
        d = tmp_path / "d"
        out = _run(d, ops=_fake_ops(pending=3, fills=0, holdings_count=0), gate=_hold_loader)
        assert out["close_status"] == dc.PAPER_ORDERS_SUBMITTED
        assert out["performed_write"] is False
        assert _journal_count(d) == 0

    def test_not_active_awaits(self, tmp_path):
        d = tmp_path / "d"
        out = _run(d, ops=_fake_ops(initialized=False, fills=0, holdings_count=0,
                                    lifecycle="BOOK_INITIALIZED"))
        assert out["close_status"] == dc.AWAITING_ELIGIBLE_CLOSE
        assert out["performed_write"] is False
        assert _journal_count(d) == 0

    def test_data_blocked_leaves_no_record(self, tmp_path):
        d = tmp_path / "d"
        out = _run(d, refresh=_blocked_refresh())
        assert out["close_status"] == dc.DATA_BLOCKED
        assert out["decision"] is None
        assert _journal_count(d) == 0                      # retryable — no partial record
        assert out["data_blocker"] is not None

    def test_data_blocked_is_retryable(self, tmp_path):
        d = tmp_path / "d"
        _run(d, refresh=_blocked_refresh())
        # a later successful run for the same date still records the close
        out = _run(d, refresh=_ok_refresh())
        assert out["close_status"] == dc.CLOSE_COMPLETE_HOLD
        assert _journal_count(d) == 1

    def test_never_creates_orders(self, tmp_path):
        d = tmp_path / "d"
        out = _run(d)
        # the daily close writes ONLY its own journal ledger; no desk order ledger row
        assert out["creates_orders"] is False
        assert out["auto_order_creation"] is False
        assert not (desk._desk_dir(d) / desk.ORDERS_FILE).exists()

    def test_manual_paper_order_approval_separate(self, tmp_path):
        d = tmp_path / "d"
        out = _run(d, gate=_prop_loader)
        # the proposal explicitly says orders are created by a SEPARATE confirmation,
        # and the daily-close token is not any desk order token.
        assert "separate" in out["proposal"]["note"].lower()
        assert dc.EXECUTE_CONFIRMATION not in (desk.GEN_CONFIRM_TOKEN, desk.EXEC_CONFIRM_TOKEN)

    def test_frozen_model_target_recalculated(self, tmp_path):
        d = tmp_path / "d"
        seen = {"n": 0}

        def _spy_gate(*a, **k):
            seen["n"] += 1
            return _fake_gate("NO_ACTION_TODAY", 0)

        out = _run(d, gate=_spy_gate)
        assert seen["n"] >= 1                              # the target/checks were recomputed
        assert out["checks_summary"]["line"].startswith("13 checks")

    def test_safety_no_model_or_sleeve_change(self, tmp_path):
        out = _run(tmp_path / "d")
        assert out["model_parameters_changed"] is False
        assert out["champion_replaced"] is False
        assert out["fast_sleeve_active"] is False
        assert out["broker_enabled"] is False
        assert out["automation_enabled"] is False
        assert out["live_orders_enabled"] is False


# --------------------------------------------------------------------------- #
# 1./2. GET status performs no write; page load performs no write
# --------------------------------------------------------------------------- #
class TestReadOnly:
    def test_get_performs_no_write(self, tmp_path):
        d = tmp_path / "d"
        before = _journal_count(d)
        out = _load(d)
        assert out["performed_write"] is False
        assert out["read_only"] is True
        assert _journal_count(d) == before

    def test_get_due_when_active_unprocessed(self, tmp_path):
        out = _load(tmp_path / "d")
        assert out["close_status"] == dc.CLOSE_DUE
        assert out["primary_action"]["runs_daily_close"] is True
        assert out["primary_action"]["label"] == "Run Daily Close"

    def test_get_hold_after_processed(self, tmp_path):
        d = tmp_path / "d"
        _run(d)                                            # records HOLD for 2026-07-21
        out = _load(d)
        assert out["close_status"] == dc.CLOSE_COMPLETE_HOLD
        assert out["decision"] == dc.DECISION_HOLD
        assert out["primary_action"]["runs_daily_close"] is False

    def test_get_five_stage_cycle(self, tmp_path):
        out = _load(tmp_path / "d")
        assert len(out["daily_cycle_stages"]) == 5
        assert [s["code"] for s in out["daily_cycle_stages"]] == [
            "RUN_DAILY_CLOSE", "RECALCULATE_TARGET_RISK", "COMPARE_BUILD_DECISION",
            "MANUAL_REVIEW_ORDERS", "MONITOR_PERFORMANCE"]


# --------------------------------------------------------------------------- #
# Real-desk P&L + idempotency (reuses the FILLED-book offline harness)
# --------------------------------------------------------------------------- #
class TestRealDeskPnl:
    def _close(self, env, *, today, marks_through):
        table = _marks_table(_D0 + marks_through)
        return dc.run_daily_close(
            confirm=dc.EXECUTE_CONFIRMATION, today=today, desk_dir=env["desk"],
            downloader=_dl(table), gate_loader=_hold_loader)

    def test_first_close_daily_pnl_unavailable_cumulative_shown(self, env27b1):
        _filled_world()                                    # fills 2026-07-20, marks->2026-07-20
        out = self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"])
        assert out["close_status"] == dc.CLOSE_COMPLETE_HOLD
        pnl = out["pnl"]
        assert pnl is not None
        assert pnl["daily_pnl_available"] is False
        assert pnl["daily_pnl"] is None
        assert pnl["daily_pnl_note"]                       # explains why
        assert pnl["cumulative_pnl"] is not None           # cumulative still shown
        assert pnl["nav"] is not None

    def test_nav_equals_cash_plus_invested(self, env27b1):
        _filled_world()
        out = self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"])
        pnl = out["pnl"]
        assert pnl["nav"] == pytest.approx(pnl["cash"] + pnl["invested_value"], abs=0.02)

    def test_cost_not_double_counted(self, env27b1):
        _filled_world()
        out = self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"])
        pnl = out["pnl"]
        # cumulative P&L on the first mark is exactly nav - starting_capital (one embedded
        # cost), never twice; and it is non-positive (only the execution cost so far).
        assert pnl["cumulative_pnl"] == pytest.approx(pnl["nav"] - pnl["starting_capital"], abs=0.02)
        assert pnl["cumulative_pnl"] <= 0.0

    def test_one_perf_and_one_journal_row_per_new_date(self, env27b1):
        _filled_world()
        # close #1 for 2026-07-20 (a perf row already exists from _filled_world)
        perf0 = len(desk.load_performance(env27b1["desk"])["rows"])
        self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"])
        assert _journal_count(env27b1["desk"]) == 1
        # close #2 for 2026-07-21 -> exactly one NEW performance row + one NEW journal row
        self._close(env27b1, today="2026-07-22", marks_through=["2026-07-20", "2026-07-21"])
        perf2 = len(desk.load_performance(env27b1["desk"])["rows"])
        assert perf2 == perf0 + 1
        assert _journal_count(env27b1["desk"]) == 2

    def test_daily_and_cumulative_pnl_correct_second_close(self, env27b1):
        _filled_world()
        first = self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"])
        nav1 = first["pnl"]["nav"]
        second = self._close(env27b1, today="2026-07-22",
                             marks_through=["2026-07-20", "2026-07-21"])
        pnl = second["pnl"]
        assert pnl["daily_pnl_available"] is True
        assert pnl["daily_pnl"] == pytest.approx(pnl["nav"] - nav1, abs=0.02)
        assert pnl["cumulative_pnl"] == pytest.approx(pnl["nav"] - pnl["starting_capital"], abs=0.02)

    def test_rerun_same_date_already_processed_real_desk(self, env27b1):
        _filled_world()
        self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"])
        perf1 = len(desk.load_performance(env27b1["desk"])["rows"])
        again = self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"])
        assert again["close_status"] == dc.ALREADY_PROCESSED
        assert again["performed_write"] is False
        assert _journal_count(env27b1["desk"]) == 1
        assert len(desk.load_performance(env27b1["desk"])["rows"]) == perf1

    def test_daily_close_creates_no_orders_real_desk(self, env27b1):
        _filled_world()
        before = desk.load_orders(env27b1["desk"])["counts_by_status"]
        self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"])
        after = desk.load_orders(env27b1["desk"])["counts_by_status"]
        # no new PROPOSED / APPROVED orders were created by the daily close
        assert after["PROPOSED"] == before["PROPOSED"]
        assert after["APPROVED"] == before["APPROVED"]


# --------------------------------------------------------------------------- #
# API surface (routes + token + page-load safety)
# --------------------------------------------------------------------------- #
class TestApiRoutes:
    def test_get_daily_close_contract(self, client):
        r = client.get("/v1/operations/daily-close", headers=_AUTH)
        assert r.status_code == 200
        body = r.json()
        for key in ("close_status", "close_status_label", "primary_action",
                    "latest_eligible_market_date", "daily_cycle_stages",
                    "checks_summary", "pnl", "performance_history", "safety_badges"):
            assert key in body, key
        assert body["close_status"] in dc.ALL_CLOSE_STATUSES
        assert body["performed_write"] is False

    def test_post_execute_requires_token(self, client):
        r = client.post("/v1/operations/daily-close/execute", headers=_AUTH,
                        json={"confirmation": "WRONG"})
        assert r.status_code == 400

    def test_post_execute_requires_body(self, client):
        r = client.post("/v1/operations/daily-close/execute", headers=_AUTH, json={})
        assert r.status_code == 422

    def test_routes_require_api_key(self, client):
        assert client.get("/v1/operations/daily-close").status_code in (401, 403)
        assert client.post("/v1/operations/daily-close/execute",
                           json={"confirmation": dc.EXECUTE_CONFIRMATION}).status_code in (401, 403)

    def test_page_load_writes_nothing(self, client, env):
        d = env["desk"]
        before = _journal_count(d)
        for path in ("/v1/operations/daily-close", "/v1/operational-book",
                     "/v1/dashboard/command-center"):
            client.get(path, headers=_AUTH)
        assert _journal_count(d) == before


# --------------------------------------------------------------------------- #
# UI static contract (the daily-close operator surfaces)
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def html() -> str:
    return _UI.read_text(encoding="utf-8")


class TestUiStatic:
    def test_daily_close_cards_and_primary_cta(self, html):
        assert "Today&rsquo;s Daily Close" in html          # the primary daily-close card title
        assert "Run Daily Close" in html                    # the primary CTA label
        assert "cc-dc-btn" in html                          # CC primary button
        for cid in ("cc-dc-card", "dw-dc-card", "pm-dc-card", "right-dc-badge"):
            assert cid in html, cid

    def test_five_stage_daily_cycle_container(self, html):
        # the cycle labels are rendered by JS from the backend contract; the static
        # HTML carries the container + the renderer.
        assert "dw-dc-cycle" in html
        assert "_dcCycleHtml" in html

    def test_backend_hold_and_cycle_labels(self):
        assert dc._PRESENTATION[dc.CLOSE_COMPLETE_HOLD]["label"].startswith("DAILY REVIEW COMPLETE")
        assert "HOLD CURRENT PORTFOLIO" in dc._PRESENTATION[dc.CLOSE_COMPLETE_HOLD]["label"]
        labels = [s["label"] for s in dc._daily_cycle_stages(dc.CLOSE_DUE)]
        assert labels == ["Run Daily Close", "Recalculate Target & Risk",
                          "Compare Holdings & Build Decision", "Manual Review & Paper Orders",
                          "Monitor Performance"]

    def test_portfolio_daily_performance_section(self, html):
        assert "Daily Performance" in html
        assert "dc-perf-panel" in html
        assert "dc-perf-tbody" in html                      # the forward-performance table
        assert "renderDailyClosePerf" in html

    def test_system_maintenance_section_for_generic_refresh(self, html):
        assert "SYSTEM / MAINTENANCE" in html
        # the generic technical refresh actions live under it (diagnostics only)
        idx = html.index("SYSTEM / MAINTENANCE")
        assert "Full Refresh" in html[idx:idx + 900]
        assert "Refresh Status" in html[idx:idx + 900]

    def test_no_native_dialogs_added(self, html):
        js = "\n".join(m.group(1) for m in
                       re.finditer(r"(?s)<script[^>]*>(.*?)</script>", html))
        for banned in ("alert(", "confirm(", "prompt("):
            assert banned not in js, banned

    def test_loader_and_endpoint_wired(self, html):
        assert "loadDailyClose" in html
        assert "/v1/operations/daily-close" in html
        # the loader is called inside the single operational-book refresh path
        assert "try { loadDailyClose(); }" in html
