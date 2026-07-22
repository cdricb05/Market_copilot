"""
tests/test_phase27b5_operator_flow.py - Phase 27B.5 final operator flow
simplification + canonical paper-order lifecycle.

Fully offline (Phase 27A/27B.1 harness: owned-style CSV fixtures, tmp desk /
ledger dirs, injectable marks downloader, deterministic clock seams). Order
creation, submission and fills happen ONLY against the isolated tmp desk stores
of this harness - never against the user's real development book; no live
broker activity, no signals, no automation.

Proves the ONE canonical, state-driven operator workflow:

1. lifecycle resolution for all six stages (PLAN_NOT_CREATED, PLAN_READY,
   PROPOSED, SUBMITTED, PARTIALLY_FILLED, FILLED) from the existing truth;
2. the SUBMITTED state reads exactly as the operator card requires: N
   submitted / 0 filled / 0 holdings, awaiting the next eligible close, no
   further confirmation required, primary action "Refresh After Market Close";
3. every operational surface consumes the SAME canonical values (backend
   cross-endpoint agreement + the UI single-source view model);
4. confirm/preview buttons recede after submission; the submitted order table
   is visible without selecting a hidden tab;
5. legacy CDW/HUM holdings never count as Alpha holdings and stay collapsed;
6. research warnings can never replace the operational next action;
7. paper-only / no-broker / automation-off safety invariants;
8. loading pages performs no writes.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from paper_trader.api import alpha_book as ab
from paper_trader.api import operational_book as ob_mod
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import portfolio_manager as pm

from tests.test_phase27a_paper_operations import (  # reuse the offline harness
    _AUTH, _D0, _TICKS, _bars, _confirm_snapshot, _dl, _marks_table, _refresh,
    client, env,  # noqa: F401  (pytest fixtures resolved by name)
)
from tests.test_phase27b1_operational_surface_cutover import (  # 27B.1 seams
    env27b1, _init_book,  # noqa: F401
)

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"

_N = len(_TICKS)


# --------------------------------------------------------------------------- #
# World builders (tmp stores only - the real development book is never touched)
# --------------------------------------------------------------------------- #
def _plan_ready_world():
    """Confirmed target + initialized book + valid desk marks -> plan exists."""
    _confirm_snapshot()
    _init_book("2026-07-18")
    _refresh("2026-07-18")


def _proposed_world():
    """...then confirm the executable order plan -> PROPOSED alpha orders."""
    _plan_ready_world()
    out = ab.confirm_order_plan(confirm=ab.PLAN_CONFIRM_TOKEN, today="2026-07-18")
    assert out["status"] == ab.A_OK, out
    return out


def _submitted_world():
    """...then submit the proposed orders -> SUBMITTED, awaiting a later close."""
    _proposed_world()
    c = desk.confirm_orders(confirm=desk.EXEC_CONFIRM_TOKEN, today="2026-07-18")
    assert c["status"] == desk.S_OK, c
    assert c["settlement"]["n_filled"] == 0     # never a same-session fill
    return c


def _partially_filled_world():
    """...then a later refresh fills all names except a stale one."""
    _submitted_world()
    table = _marks_table(_D0 + ["2026-07-20"], drop=(_TICKS[0],))
    table[_TICKS[0]] = _bars(_D0, 100.0)        # stale: nothing after approval
    r = _refresh("2026-07-21", table)
    assert r["status"] == desk.S_OK, r
    assert r["settlement"]["n_filled"] == _N - 1
    return r


def _filled_world():
    """...then a later refresh fills every submitted order."""
    _submitted_world()
    r = _refresh("2026-07-21", _marks_table(_D0 + ["2026-07-20"]))
    assert r["status"] == desk.S_OK, r
    assert r["settlement"]["n_filled"] == _N
    return r


def _load(today="2026-07-18"):
    return ob_mod.load_operational_book(today=today)


def _cs(today="2026-07-18"):
    return _load(today)["canonical_state"]


@pytest.fixture(scope="module")
def html() -> str:
    return _UI.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def js(html) -> str:
    return "\n".join(m.group(1) for m in
                     re.finditer(r"(?s)<script[^>]*>(.*?)</script>", html))


# --------------------------------------------------------------------------- #
# 1. Canonical lifecycle resolution - all six stages
# --------------------------------------------------------------------------- #
class TestLifecycleResolution:
    def test_plan_not_created(self, env27b1):
        _confirm_snapshot()
        _init_book("2026-07-18")                 # no desk marks yet
        cs = _cs()
        assert cs["lifecycle_stage"] == "PLAN_NOT_CREATED"
        assert cs["primary_headline"] == "ORDER PLAN NOT CREATED"
        assert cs["next_action_label"] == "Refresh Desk Marks"
        assert cs["submitted_count"] == 0 and cs["proposed_count"] == 0

    def test_plan_ready(self, env27b1):
        _plan_ready_world()
        cs = _cs()
        assert cs["lifecycle_stage"] == "PLAN_READY"
        assert cs["primary_headline"] == "ORDER PLAN READY FOR REVIEW"
        assert cs["primary_explanation"] == (
            "The executable paper-order plan is ready. Review the proposed "
            "orders and confirm them manually.")
        assert cs["next_action_label"] == "Review Order Plan"
        assert cs["open_order_count"] == 0

    def test_proposed(self, env27b1):
        _proposed_world()
        cs = _cs()
        assert cs["lifecycle_stage"] == "PROPOSED"
        assert cs["primary_headline"] == "PAPER ORDERS AWAITING CONFIRMATION"
        assert cs["proposed_count"] == _N
        assert cs["next_action_label"] == (
            "Confirm and Submit %d Paper Orders" % _N)
        assert cs["next_action_route_or_anchor"] == "#portfolio-manager/pd-band"
        assert cs["no_further_confirmation_required"] is False

    def test_submitted(self, env27b1):
        _submitted_world()
        cs = _cs()
        assert cs["lifecycle_stage"] == "SUBMITTED"
        assert cs["primary_headline"] == (
            "%d PAPER ORDERS SUBMITTED — AWAITING NEXT ELIGIBLE CLOSE" % _N)
        assert cs["submitted_count"] == _N
        assert cs["next_action_label"] == "Refresh After Market Close"

    def test_submitted_on_a_later_calendar_day(self, env27b1):
        """WAITING_FOR_ELIGIBLE_CLOSE still presents as the SUBMITTED stage."""
        _submitted_world()
        cs = _cs(today="2026-07-20")
        assert cs["workflow_state"] == "WAITING_FOR_ELIGIBLE_CLOSE"
        assert cs["lifecycle_stage"] == "SUBMITTED"
        assert cs["next_action_label"] == "Refresh After Market Close"

    def test_partially_filled(self, env27b1):
        _partially_filled_world()
        cs = _cs(today="2026-07-21")
        assert cs["lifecycle_stage"] == "PARTIALLY_FILLED"
        assert cs["primary_headline"] == "PAPER EXECUTION IN PROGRESS"
        assert cs["filled_count"] == _N - 1
        assert cs["submitted_count"] == 1
        assert cs["next_action_label"] == "Refresh After Market Close"
        assert cs["no_further_confirmation_required"] is True

    def test_fully_filled(self, env27b1):
        _filled_world()
        cs = _cs(today="2026-07-21")
        assert cs["lifecycle_stage"] == "FILLED"
        assert cs["primary_headline"] == "ALPHA PAPER BOOK ACTIVE"
        assert cs["filled_count"] == _N
        assert cs["open_order_count"] == 0
        assert cs["holdings_count"] == _N
        assert cs["next_action_label"] == "Monitor Holdings and Performance"
        assert cs["secondary_action_label"] is None

    def test_pure_derivation_monthly_rebalance_reemergence(self):
        """Terminal fills + a NEW plan cycle present as PLAN_READY, not FILLED."""
        view = ob_mod.derive_lifecycle_view(
            initialized=True,
            orders={"by_status": {"FILLED": 25}, "latest_submission_date": None},
            fills_count=25, plan_exists=True)
        assert view["lifecycle_stage"] == "PLAN_READY"

    def test_pure_derivation_counts_never_hardcoded(self):
        view = ob_mod.derive_lifecycle_view(
            initialized=True,
            orders={"by_status": {"SUBMITTED": 3, "CANCELLED": 2},
                    "latest_submission_date": "2026-07-22"},
            fills_count=0, plan_exists=False, submitted_date="2026-07-22",
            execution_model="NEXT_CLOSE")
        assert view["lifecycle_stage"] == "SUBMITTED"
        assert view["primary_headline"] == \
            "3 PAPER ORDERS SUBMITTED — AWAITING NEXT ELIGIBLE CLOSE"
        assert view["cancelled_count"] == 2
        assert "2026-07-22" in view["next_eligible_fill_explanation"]
        assert view["execution_model"] == "NEXT_CLOSE"


# --------------------------------------------------------------------------- #
# 2. The current SUBMITTED state - exact operator presentation
# --------------------------------------------------------------------------- #
class TestSubmittedStatePresentation:
    def test_counts_and_dates(self, env27b1):
        _submitted_world()
        cs = _cs()
        assert cs["submitted_count"] == _N
        assert cs["filled_count"] == 0
        assert cs["cancelled_count"] == 0
        assert cs["holdings_count"] == 0
        assert cs["fill_count"] == 0
        assert cs["nav"] == pytest.approx(100000.0)
        assert cs["cash"] == pytest.approx(100000.0)
        assert cs["submitted_date"] == "2026-07-18"
        assert cs["desk_mark_date"] == "2026-07-17"
        assert cs["execution_model"] == "NEXT_CLOSE"

    def test_no_further_confirmation_required(self, env27b1):
        _submitted_world()
        cs = _cs()
        assert cs["no_further_confirmation_required"] is True
        assert "No further confirmation is required" in cs["primary_explanation"]

    def test_awaiting_next_eligible_close_explanation(self, env27b1):
        _submitted_world()
        cs = _cs()
        e = cs["next_eligible_fill_explanation"]
        assert "2026-07-18" in e                      # dynamic submission date
        assert "expected, not a failure" in e
        assert "LATER manual desk refresh" in e

    def test_never_a_confirmation_action(self, env27b1):
        """No surface may recommend confirming anything once orders exist."""
        _submitted_world()
        cs = _cs()
        assert cs["next_action_code"] == "REFRESH_DESK"
        assert cs["next_action_code"] not in (
            "CONFIRM_PAPER_ORDERS", "CONFIRM_ORDER_PLAN",
            "REVIEW_AND_CONFIRM_ORDER_PLAN", "CONFIRM_TARGET_SNAPSHOT")
        assert "Confirm" not in cs["next_action_label"]
        assert cs["secondary_action_label"] == "Cancel Submitted Orders"

    def test_same_day_refresh_records_zero_fills_and_stays_submitted(self, env27b1):
        _submitted_world()
        r = _refresh("2026-07-18")                    # no newer completed close
        assert r["status"] == desk.S_OK
        assert r["settlement"]["n_filled"] == 0
        cs = _cs()
        assert cs["lifecycle_stage"] == "SUBMITTED"
        assert cs["submitted_count"] == _N

    def test_current_task_is_await_next_eligible_close(self, env27b1):
        _submitted_world()
        cs = _cs()
        assert cs["current_task_label"] == "Await Next Eligible Close"
        assert cs["workflow_state_label"] == "ORDERS CONFIRMED"
        assert cs["lifecycle_stage_label"] == \
            "Paper Orders Submitted — Awaiting Next Eligible Close"


# --------------------------------------------------------------------------- #
# 3. Cross-surface agreement (backend endpoints + UI single source)
# --------------------------------------------------------------------------- #
class TestCrossSurfaceAgreement:
    def test_backend_surfaces_agree_on_the_submitted_state(self, env27b1, client):
        _submitted_world()
        ob_d = client.get("/v1/operational-book", headers=_AUTH).json()
        ab_d = client.get("/v1/alpha-book/status", headers=_AUTH).json()
        pd_d = client.get("/v1/paper-desk/status", headers=_AUTH).json()
        cs = ob_d["canonical_state"]
        assert cs["lifecycle_stage"] == "SUBMITTED"
        assert cs == ob_d["operational_book"]["canonical_state"]
        assert ab_d["current_state"] == cs["workflow_state"]
        assert ab_d["orders_awaiting_fill"] == cs["submitted_count"] == _N
        assert ab_d["n_alpha_fills"] == cs["fill_count"] == 0
        assert pd_d["order_counts"].get("SUBMITTED", 0) == _N
        s = pm.load_summary()
        ops = s["operational_book"]
        assert ops["current_status"] == cs["workflow_state"]
        assert ops["pending_order_count"] == cs["pending_order_count"] == _N
        assert ops["holdings_count"] == cs["holdings_count"] == 0
        assert ops["canonical_state"]["lifecycle_stage"] == "SUBMITTED"
        assert ops["canonical_state"]["next_action_label"] == \
            "Refresh After Market Close"

    def test_ui_consumes_one_view_model(self, js):
        """Pages render the shared derivation - never their own state logic."""
        assert "function deriveOperationalBookUiState" in js
        assert "window._obView = view" in js
        # the ONE label source of 27B.2 is retained (lifecycle-aware backend label)
        assert "var primaryLabel = (cs && cs.next_action_label)" in js
        # every surface consumes the view model
        for probe in ("view.ordersExist && view.headline",
                      "renderPmLifecycle(view",
                      "view.currentTask",
                      "right-ob-statement",
                      "cc-ob-lifeline",
                      "ptob-orders-note"):
            assert probe in js, probe

    def test_ui_waiting_language_is_calm_and_specific(self, js):
        assert "Wait for an eligible completed close. Then run " in js
        assert "expected, not a failure" in js
        assert "Refresh After Market Close" in js


# --------------------------------------------------------------------------- #
# 4. Confirm buttons recede after submission
# --------------------------------------------------------------------------- #
class TestCompletedActionsRecede:
    def test_desk_confirm_and_preview_hide_when_tracking(self, js):
        assert "['pd-act-confirm', 'pd-act-preview'].forEach(function (bid)" in js
        assert "b.style.display = lcTracking ? 'none' : ''" in js

    def test_plan_band_recedes_once_orders_exist(self, js):
        assert "if (abBand) abBand.style.display = 'none';" in js

    def test_refresh_button_becomes_refresh_after_market_close(self, js):
        assert "lcTracking ? 'Refresh After Market Close' : " in js

    def test_completed_workflow_stage_buttons_hide(self, js):
        assert "if (row.status === 'COMPLETE') {" in js
        assert "sbtn.style.display = 'none';" in js

    def test_submission_success_is_a_prominent_in_page_result(self, html, js):
        assert "PAPER ORDERS SUBMITTED SUCCESSFULLY" in js
        assert "No further confirmation is required." in js
        assert ">View Submitted Orders<" in js
        assert ">Cancel Submitted Orders<" in js
        # never a native browser dialog anywhere in the UI
        blocks = "\n".join(m.group(1) for m in
                           re.finditer(r"(?s)<script[^>]*>(.*?)</script>", html))
        assert not re.search(r"(?<![A-Za-z0-9_.])alert\(", blocks)
        assert not re.search(r"(?<![A-Za-z0-9_.])confirm\(", blocks)


# --------------------------------------------------------------------------- #
# 5. Submitted orders visible without a hidden tab
# --------------------------------------------------------------------------- #
class TestOrdersVisibleDirectly:
    def test_lifecycle_card_renders_the_order_table_directly(self, html, js):
        assert 'id="pm-lc-strip"' in html
        assert 'id="pm-lc-orders"' in html
        assert "function renderPmLifecycleOrders" in js
        assert "SUBMITTED PAPER ORDERS (" in js

    def test_desk_default_view_is_the_open_orders_not_todays_tab(self, js):
        assert "(nOpen ? 'pending' : 'today-orders')" in js

    def test_lifecycle_counts_are_visually_prominent(self, html):
        for eid in ("pm-lc-submitted", "pm-lc-filled", "pm-lc-cancelled",
                    "pm-lc-holdings", "pm-lc-cash", "pm-lc-nav", "pm-lc-mark",
                    "pm-lc-exec", "pm-lc-subdate", "pm-lc-eligible",
                    "pm-lc-cancel-btn", "cc-ob-lifeline",
                    "right-ob-statement", "ptob-orders-note",
                    "ptob-view-orders-btn"):
            assert ('id="%s"' % eid) in html, eid

    def test_portfolio_links_straight_to_the_submitted_orders(self, js):
        assert "function obOpenSubmittedOrders" in js
        assert "submitted paper orders await the next eligible completed close" in js


# --------------------------------------------------------------------------- #
# 6/7. Legacy separation + collapsed archives
# --------------------------------------------------------------------------- #
class TestLegacySeparation:
    def test_legacy_holdings_never_count_as_alpha_holdings(self, env27b1):
        _submitted_world()
        cs = _cs()
        assert cs["holdings_count"] == 0                       # alpha book empty
        legacy = cs["legacy_archive_summary"]
        assert legacy["positions_count"] == 2                  # CDW + HUM archive
        assert set(legacy["tickers"]) == {"CDW", "HUM"}
        # and the alpha submitted counts never include the legacy names
        assert cs["submitted_count"] == _N

    def test_alpha_portfolio_offers_one_operational_link(self, html):
        assert ">Open Operational Book</button>" in html
        assert 'id="mhz-open-pm"' in html


# --------------------------------------------------------------------------- #
# 8. Research warnings can never replace the operational next action
# --------------------------------------------------------------------------- #
class TestResearchNeverOverrides:
    def test_legacy_valuation_failure_never_changes_the_lifecycle(self, env27b1,
                                                                  monkeypatch):
        _submitted_world()
        def _boom():
            raise RuntimeError("legacy/research store unavailable")
        monkeypatch.setattr(ob_mod, "_VALUATION_LOADER", _boom)
        cs = _cs()
        assert cs["lifecycle_stage"] == "SUBMITTED"
        assert cs["next_action_label"] == "Refresh After Market Close"
        assert cs["primary_headline"].endswith(
            "PAPER ORDERS SUBMITTED — AWAITING NEXT ELIGIBLE CLOSE")

    def test_research_summary_stays_research_only(self, env27b1):
        _submitted_world()
        rs = _cs()["research_summary"]
        assert rs["research_champion"] == "composite_sn"
        assert "RESEARCH ONLY" in rs["note"]

    def test_no_operational_blockers_in_the_submitted_state(self, env27b1):
        _submitted_world()
        cs = _cs()
        assert cs["blockers"] == []
        assert any("TARGET_ALREADY_CONFIRMED" in n
                   for n in cs["informational_notices"])


# --------------------------------------------------------------------------- #
# 9/10. Safety invariants + read-only page loads
# --------------------------------------------------------------------------- #
class TestSafetyAndReadOnly:
    def test_safety_invariants_hold_with_submitted_orders(self, env27b1, client):
        _submitted_world()
        d = client.get("/v1/operational-book", headers=_AUTH).json()
        assert d["paper_only"] is True
        assert d["read_only"] is True
        assert d["broker_enabled"] is False
        assert d["automation_enabled"] is False
        assert d["live_orders_enabled"] is False
        sm = d["canonical_state"]["safety_mode"]
        assert sm["manual_review"] and sm["paper_orders_only"]
        assert not sm["broker_execution"] and not sm["automation"]

    def test_loading_every_operational_surface_writes_nothing(self, env27b1, client):
        _submitted_world()
        before = desk.load_orders()["counts_by_status"]
        n_fills_before = desk.load_fills()["n_fills"]
        for path in ("/v1/operational-book", "/v1/alpha-book/status",
                     "/v1/paper-desk/status", "/v1/paper-desk/orders",
                     "/v1/paper-desk/fills", "/v1/portfolio-manager/summary"):
            r = client.get(path, headers=_AUTH)
            assert r.status_code == 200, path
            body = r.json()
            if "performed_write" in body:
                assert body["performed_write"] is False, path
        assert desk.load_orders()["counts_by_status"] == before
        assert desk.load_fills()["n_fills"] == n_fills_before
