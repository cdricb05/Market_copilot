"""
tests/test_phase27c_daily_event_operator_cutover.py — Phase 27C.

DAILY EVENT-DRIVEN ACTION GATE + operator UX hard cutover.

Fully offline. The pure gate (``evaluate_daily_action_gate``) is exercised with
hand-built inputs — no DB, no engine, no prediction tunnel. The composition
loader is exercised with a synthetic operational payload + a fake engine current
(``compute_recommendations`` monkeypatched), and the page-load-safety / route
tests reuse the Phase 27A/27B offline harness (tmp desk + ledger stores). No
order, fill, snapshot or trade is ever created; nothing here writes anywhere.

Covers the required Part C scenarios:
  1. active book, no material event         -> NO_ACTION_TODAY
  2. hard eligibility before scheduled review -> proposal (review date never suppresses)
  3. hard risk before scheduled review        -> proposal (no auto fill / broker)
  4. immaterial rank / weight change           -> NO_ACTION (no churn)
  5. material target change                    -> proposal + changes + cost exposed
  6. scheduled full review                     -> full comparison, may still be NO_ACTION
  7. daily recalculation, NOT retraining       -> frozen model identity, fast sleeve inactive
  8. page-load safety                          -> GET / load creates nothing
  9. cross-surface consistency                 -> one canonical outcome everywhere
 10. portfolio visibility                      -> the 25 operational holdings, not archive
 11. legacy UI absence                         -> forbidden legacy controls gone
 12. safety                                    -> manual review, no broker/automation/live orders
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from paper_trader.api import daily_action_gate as dag
from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import portfolio_manager as pm

from tests.test_phase27a_paper_operations import (  # offline harness fixtures
    _AUTH, client, env,  # noqa: F401
)
from tests.test_phase27b1_operational_surface_cutover import env27b1  # noqa: F401
from tests.test_phase27b5_operator_flow import _filled_world  # canonical world builder

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"


@pytest.fixture(scope="module")
def html() -> str:
    return _UI.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def js(html) -> str:
    return "\n".join(m.group(1) for m in
                     re.finditer(r"(?s)<script[^>]*>(.*?)</script>", html))


# --------------------------------------------------------------------------- #
# Builders
# --------------------------------------------------------------------------- #
def _holdings(weights: dict) -> dict:
    return {tk: {"weight": w, "quantity": 10} for tk, w in weights.items()}


def _target(tickers, weight=0.04) -> dict:
    return {tk: {"weight": weight, "rank": i + 1, "sector": "Tech"}
            for i, tk in enumerate(tickers)}


# =========================================================================== #
# Scenario 1 / 4 / 6 — the pure gate: NO_ACTION when nothing material fires
# =========================================================================== #
class TestPureGateNoAction:
    def test_active_book_no_event_no_action(self):
        # 1. target == holdings, review not due -> NO_ACTION_TODAY
        h = _holdings({t: 0.038 for t in "ABCD"})
        tg = _target(list("ABCD"))
        r = dag.evaluate_daily_action_gate(
            holdings=h, target=tg, target_count=4,
            next_scheduled_full_review="2026-08-01", scheduled_review_due=False,
            data_ready=True)
        assert r["outcome"] == dag.OUTCOME_NO_ACTION_TODAY
        assert r["action_required"] is False
        assert r["action_severity"] == "green"
        assert r["proposed_change_count"] == 0
        assert r["target_actual_match"] is True
        assert r["headline"] == "NO PORTFOLIO CHANGE REQUIRED TODAY"

    def test_immaterial_drift_no_action(self):
        # 4. every name drifts 0.01 (< 0.02 band) -> NO_ACTION, no churn
        h = _holdings({t: 0.05 for t in "ABCD"})     # target 0.04 -> drift 0.01
        tg = _target(list("ABCD"))
        r = dag.evaluate_daily_action_gate(holdings=h, target=tg, target_count=4,
                                           scheduled_review_due=False, data_ready=True)
        assert r["outcome"] == dag.OUTCOME_NO_ACTION_TODAY
        assert r["proposed_resizes"] == []
        assert r["trigger_categories"] == []

    def test_scheduled_review_due_but_no_change(self):
        # 6. review due, but target == holdings -> still NO_ACTION (comparison ran)
        h = _holdings({t: 0.038 for t in "ABCD"})
        tg = _target(list("ABCD"))
        r = dag.evaluate_daily_action_gate(
            holdings=h, target=tg, target_count=4,
            next_scheduled_full_review="2026-07-22", scheduled_review_due=True,
            data_ready=True)
        assert r["outcome"] == dag.OUTCOME_NO_ACTION_TODAY
        assert dag.TRIGGER_SCHEDULED_REVIEW in r["trigger_categories"]
        assert "scheduled full portfolio review ran" in r["explanation"].lower()


# =========================================================================== #
# Scenario 2 — HARD ELIGIBILITY event before the scheduled review
# =========================================================================== #
class TestPureGateHardEligibility:
    def test_hard_eligibility_proposes_before_scheduled_review(self):
        h = _holdings({t: 0.038 for t in "ABCD"})
        tg = _target(list("ABC"))                       # D dropped from target
        r = dag.evaluate_daily_action_gate(
            holdings=h, target=tg, target_count=3,
            eligibility={"D": {"eligible": False, "reason": "NOT_CURRENT_MEMBER"}},
            ranked_current={"D": None},
            next_scheduled_full_review="2026-08-01", scheduled_review_due=False,
            data_ready=True)
        # a hard event fires even though the scheduled review is not due
        assert r["outcome"] in (dag.OUTCOME_PROPOSAL_READY, dag.OUTCOME_APPROVAL_REQUIRED)
        assert r["scheduled_review_due"] is False
        assert dag.TRIGGER_HARD_ELIGIBILITY in r["trigger_categories"]
        rem = [x["ticker"] for x in r["proposed_removals"]]
        assert "D" in rem
        d_row = next(x for x in r["proposed_removals"] if x["ticker"] == "D")
        assert d_row["reason"] == "NOT_CURRENT_MEMBER"
        assert d_row["hard_event"] is True
        # the gate never creates an order
        assert r["performed_write"] is False
        assert r["auto_order_creation"] is False


# =========================================================================== #
# Scenario 3 — HARD RISK event before the scheduled review
# =========================================================================== #
class TestPureGateHardRisk:
    def test_hard_risk_proposes_no_auto_fill(self):
        h = _holdings({t: 0.038 for t in "ABCD"})
        tg = _target(list("ABCD"))
        r = dag.evaluate_daily_action_gate(
            holdings=h, target=tg, target_count=4,
            risk_events=[{"ticker": "C", "reason": "DATA_QUALITY_BLOCK",
                          "detail": "corporate-action artifact"}],
            next_scheduled_full_review="2026-08-01", scheduled_review_due=False,
            data_ready=True)
        assert r["outcome"] == dag.OUTCOME_PROPOSAL_READY
        assert dag.TRIGGER_HARD_RISK in r["trigger_categories"]
        assert "C" in [x["ticker"] for x in r["proposed_removals"]]
        assert r["live_orders_enabled"] is False
        assert r["broker_enabled"] is False


# =========================================================================== #
# Scenario 5 — MATERIAL TARGET membership change (with cost exposed)
# =========================================================================== #
class TestPureGateMembership:
    def test_material_membership_change_proposes(self):
        h = _holdings({t: 0.038 for t in "ABCD"})
        tg = _target(["A", "B", "C", "E"])              # E enters, D leaves
        r = dag.evaluate_daily_action_gate(
            holdings=h, target=tg, target_count=4,
            ranked_current={"D": 99},                   # D far beyond the exit buffer
            scheduled_review_due=False, data_ready=True)
        assert r["outcome"] == dag.OUTCOME_PROPOSAL_READY
        assert dag.TRIGGER_MEMBERSHIP_CHANGE in r["trigger_categories"]
        assert "E" in [x["ticker"] for x in r["proposed_additions"]]
        assert "D" in [x["ticker"] for x in r["proposed_removals"]]
        assert r["estimated_turnover"] is not None and r["estimated_turnover"] > 0
        assert r["estimated_cost"] is not None and r["estimated_cost"] > 0

    def test_within_exit_buffer_holds_no_churn(self):
        # D left the target but sits inside the exit buffer (rank <= ceil(4*1.2)=5)
        h = _holdings({t: 0.038 for t in "ABCD"})
        tg = _target(list("ABC"))
        r = dag.evaluate_daily_action_gate(
            holdings=h, target=tg, target_count=4, ranked_current={"D": 5},
            scheduled_review_due=False, data_ready=True)
        assert r["outcome"] == dag.OUTCOME_NO_ACTION_TODAY
        assert r["exit_buffer_rank"] == 5
        assert r["proposed_removals"] == []


# =========================================================================== #
# Economic action gate — a resize-only proposal below the floor is suppressed
# =========================================================================== #
class TestEconomicGate:
    def test_small_resize_only_suppressed(self):
        # single 0.03 drift -> one-way turnover 0.015 < 0.03 floor -> suppressed
        h = _holdings({"A": 0.07, "B": 0.04, "C": 0.04, "D": 0.04})
        tg = _target(list("ABCD"))
        r = dag.evaluate_daily_action_gate(holdings=h, target=tg, target_count=4,
                                           scheduled_review_due=False, data_ready=True)
        assert r["outcome"] == dag.OUTCOME_NO_ACTION_TODAY
        assert dag.TRIGGER_ECONOMIC_GATE in r["trigger_categories"]
        assert len(r["blocked_changes"]) == 1
        assert r["blocked_changes"][0]["blocked_reason"] == "ECONOMIC_THRESHOLD_NOT_MET"

    def test_hard_event_bypasses_economic_floor(self):
        # a tiny drift on A + a hard eligibility on D -> proposal is NOT suppressed
        h = _holdings({"A": 0.07, "B": 0.038, "C": 0.038, "D": 0.038})
        tg = _target(list("ABC"))
        r = dag.evaluate_daily_action_gate(
            holdings=h, target=tg, target_count=3,
            eligibility={"D": {"eligible": False, "reason": "LIQUIDITY_FILTER_FAILED"}},
            ranked_current={"D": None}, scheduled_review_due=False, data_ready=True)
        assert r["outcome"] == dag.OUTCOME_PROPOSAL_READY
        assert dag.TRIGGER_HARD_ELIGIBILITY in r["trigger_categories"]


# =========================================================================== #
# Scenario 7 — daily recalculation is NOT retraining
# =========================================================================== #
class TestDailyRecalcNotRetraining:
    def test_result_carries_frozen_identity_and_no_retrain(self):
        r = dag.evaluate_daily_action_gate(
            holdings=_holdings({t: 0.04 for t in "ABCD"}), target=_target(list("ABCD")),
            target_count=4, data_ready=True)
        assert r["strategy_name"] == "fundamental_momentum_50_50_v1"
        assert r["target_book_name"] == "fundamental_momentum_50_50_top25"
        assert r["model_parameters_changed"] is False
        assert r["champion_replaced"] is False
        assert r["fast_sleeve_active"] is False
        assert "does not retrain" in r["daily_recalculation_note"]

    def test_policy_constants_documented_from_existing_assumptions(self):
        p = dag._policy()
        assert p["execution_cost_bps_per_side"] == 12.5           # desk 12.5 bps/side
        assert p["membership_exit_buffer_fraction"] == eng.EXIT_BUFFER_FRACTION
        assert p["material_weight_drift"] == 0.02
        assert p["min_action_turnover"] == 0.03


# =========================================================================== #
# Scenario 12 — safety
# =========================================================================== #
class TestGateSafety:
    def test_data_not_ready_requires_refresh(self):
        r = dag.evaluate_daily_action_gate(holdings={}, target={}, target_count=0,
                                           data_ready=False)
        assert r["outcome"] == dag.OUTCOME_DATA_NOT_READY
        assert r["action_required"] is True
        assert r["primary_action_label"] == "Refresh After Market Close"

    def test_pending_orders_defer(self):
        r = dag.evaluate_daily_action_gate(
            holdings=_holdings({t: 0.04 for t in "ABCD"}), target=_target(list("ABCD")),
            target_count=4, orders_pending=25, data_ready=True)
        assert r["outcome"] == dag.OUTCOME_ORDERS_SUBMITTED

    def test_loader_safety_block(self, env27b1):
        r = dag.load_daily_action_gate(today="2026-07-22")
        for flag in ("paper_only", "read_only"):
            assert r[flag] is True
        for flag in ("broker_enabled", "automation_enabled", "live_orders_enabled",
                     "performed_write", "auto_order_creation", "auto_target_confirmation",
                     "model_parameters_changed", "champion_replaced", "fast_sleeve_active"):
            assert r[flag] is False


# =========================================================================== #
# Loader composition — synthetic operational payload + fake engine current
# =========================================================================== #
def _fake_current(target_tickers, ranks=None):
    ranks = ranks or {}
    cons = [{"ticker": tk, "weight": 0.04, "rank": ranks.get(tk, i + 1), "sector": "Tech"}
            for i, tk in enumerate(target_tickers)]
    return {"status": eng.STATUS_READY, "market_as_of_date": "2026-07-22",
            "inputs": {"risk": {tk: {"realized_vol_63d": 0.2} for tk in target_tickers}},
            "books": {"primary_book_id": "fundamental_momentum_50_50_top25",
                      "books": {"fundamental_momentum_50_50_top25":
                                {"equal_weight": 0.04, "size_actual": len(cons),
                                 "constituents": cons}}}}


def _synth_ops(held_weights, *, next_review="2026-08-01", review_due=False,
               orders_pending=0, fills=4, desk_ready=True):
    cs = {
        "holdings_detail": [{"ticker": tk, "current_weight": w, "quantity": 10}
                            for tk, w in held_weights.items()],
        "pending_order_count": orders_pending, "fill_count": fills,
        "lifecycle_stage": "FILLED", "next_review_date": next_review,
        "review_due": review_due, "desk_valuation_date": "2026-07-22",
        "valuation_date": "2026-07-22", "target_count": None,
        "desk_mark_status": "DESK_MARK_READY" if desk_ready else "STALE",
        "holdings_count": len(held_weights), "nav": 99000.0, "cash": 4000.0,
        "review_cadence": "MONTHLY",
    }
    ob = {"book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1",
          "holdings": {tk: 10 for tk in held_weights}, "target_count": None,
          "pending_order_count": orders_pending, "fill_count": fills,
          "holdings_count": len(held_weights)}
    return {"canonical_state": cs, "operational_book": ob}


def _fake_recs(rows):
    def _cr(current, prior, sleeve, size=25, review_due=None):
        return {"recommendations": rows}
    return _cr


class TestLoaderComposition:
    def test_loader_no_action_when_target_matches_holdings(self, monkeypatch):
        held = {t: 0.038 for t in "ABCD"}
        rows = [{"ticker": t, "recommendation": eng.REC_HOLD, "reason_codes": [],
                 "model_ranks": {"current": i + 1}} for i, t in enumerate("ABCD")]
        monkeypatch.setattr(dag.eng, "compute_recommendations", _fake_recs(rows))
        r = dag.load_daily_action_gate(today="2026-07-22", current=_fake_current(list("ABCD")),
                                       operational=_synth_ops(held))
        assert r["status"] == "DAILY_ACTION_GATE_OK"
        assert r["data_ready"] is True
        assert r["outcome"] == dag.OUTCOME_NO_ACTION_TODAY
        assert r["actual_holding_count"] == 4
        assert r["current_target_count"] == 4

    def test_loader_hard_eligibility_from_engine_exit(self, monkeypatch):
        held = {t: 0.038 for t in "ABCD"}
        rows = ([{"ticker": t, "recommendation": eng.REC_HOLD, "reason_codes": [],
                  "model_ranks": {"current": i + 1}} for i, t in enumerate("ABC")]
                + [{"ticker": "D", "recommendation": eng.REC_EXIT,
                    "reason_codes": ["NOT_CURRENT_MEMBER"], "model_ranks": {"current": None}}])
        monkeypatch.setattr(dag.eng, "compute_recommendations", _fake_recs(rows))
        r = dag.load_daily_action_gate(today="2026-07-22", current=_fake_current(list("ABC")),
                                       operational=_synth_ops(held))
        assert r["outcome"] in (dag.OUTCOME_PROPOSAL_READY, dag.OUTCOME_APPROVAL_REQUIRED)
        assert dag.TRIGGER_HARD_ELIGIBILITY in r["trigger_categories"]
        assert "D" in [x["ticker"] for x in r["proposed_removals"]]

    def test_loader_data_not_ready_when_engine_unavailable(self, monkeypatch):
        held = {t: 0.04 for t in "ABCD"}
        r = dag.load_daily_action_gate(today="2026-07-22",
                                       current={"status": eng.STATUS_INPUTS_UNAVAILABLE},
                                       operational=_synth_ops(held))
        assert r["outcome"] == dag.OUTCOME_DATA_NOT_READY
        assert r["data_ready"] is False


# =========================================================================== #
# Scenario 8 — page-load safety (loading the gate writes nothing)
# =========================================================================== #
class TestPageLoadSafety:
    def test_gate_load_creates_nothing(self, env27b1):
        from paper_trader.api import paper_trading_desk as desk
        _filled_world()
        before = len(desk._fills(desk._desk_dir(None)))
        r = dag.load_daily_action_gate(today="2026-07-22")
        after = len(desk._fills(desk._desk_dir(None)))
        assert r["performed_write"] is False
        assert after == before                 # no fill created by loading the gate
        assert r["outcome"] in dag.ALL_OUTCOMES


# =========================================================================== #
# Route — the read-only endpoint is registered and returns a canonical result
# =========================================================================== #
class TestRoute:
    def test_daily_action_gate_route(self, client, env27b1):
        _filled_world()
        resp = client.get("/v1/operations/daily-action-gate", headers=_AUTH)
        assert resp.status_code == 200
        g = resp.json()
        assert g["outcome"] in dag.ALL_OUTCOMES
        assert g["performed_write"] is False
        assert g["live_orders_enabled"] is False
        assert g["strategy_name"] == "fundamental_momentum_50_50_v1"


# =========================================================================== #
# Scenario 9 — cross-surface consistency (Portfolio Manager passthrough)
# =========================================================================== #
class TestPmPassthrough:
    def test_pm_daily_gate_block_matches_gate(self, monkeypatch):
        held = {t: 0.038 for t in "ABCD"}
        rows = [{"ticker": t, "recommendation": eng.REC_HOLD, "reason_codes": [],
                 "model_ranks": {"current": i + 1}} for i, t in enumerate("ABCD")]
        monkeypatch.setattr(dag.eng, "compute_recommendations", _fake_recs(rows))
        cur = _fake_current(list("ABCD"))
        # PM's gate loader passes its own engine current; operational comes from the seam.
        monkeypatch.setattr(pm, "_DAILY_ACTION_GATE_LOADER",
                            lambda current=None: dag.load_daily_action_gate(
                                current=cur, operational=_synth_ops(held)))
        block = pm._daily_gate_block(current=cur)
        assert block["available"] is True
        assert block["outcome"] == dag.OUTCOME_NO_ACTION_TODAY
        assert block["current_task"]
        assert block["primary_action_label"]
        assert block["target_actual_match"] is True


# =========================================================================== #
# Scenario 9 (UI) — every operator surface renders the ONE canonical gate result
# =========================================================================== #
class TestUiStaticGateSurfaces:
    def test_one_gate_fetch(self, js):
        assert js.count("_mhzGet('/v1/operations/daily-action-gate')") == 1

    def test_gate_elements_on_every_operator_surface(self, html):
        for el in ('id="cc-dag-card"', 'id="dw-dag-card"', 'id="pm-dag-card"',
                   'id="right-dag-badge"'):
            assert el in html, el

    def test_one_renderer_feeds_the_surfaces(self, js):
        assert "function renderDailyActionGate" in js
        start = js.index("function renderDailyActionGate")
        body = js[start:js.index("window.renderDailyActionGate")]
        for tok in ("cc-dag", "dw-dag", "pm-dag", "right-dag-badge"):
            assert tok in body, tok

    def test_gate_load_wired_into_operational_book_loader(self, js):
        assert "try { loadDailyActionGate(); } catch (e) {}" in js

    def test_pm_hides_confirm_when_no_action(self, js):
        # brief B5: no confirmation controls when no action is required
        assert "if (g.action_required)" in js
        assert "brief B5: no confirmation controls when no action" in js

    def test_no_native_dialogs(self, html):
        for pat in ("alert(", "confirm(", "prompt("):
            assert len(re.findall(r"(?<![\w.])" + re.escape(pat), html)) == 0, pat


# =========================================================================== #
# Scenario 10 — Portfolio page shows the operational holdings, not the archive
# =========================================================================== #
class TestUiStaticPortfolioVisibility:
    def test_portfolio_dashboard_present(self, html):
        for el in ('id="pdash-table"', 'id="pdash-tbody"', 'id="pdash-kpi-holdings"'):
            assert el in html, el

    def test_no_archived_two_position_portfolio_on_portfolio_route(self, html):
        i0 = html.index('id="tab-portfolio"')
        i1 = html.index('id="tab-portfolio-manager"')
        route = html[i0:i1]
        # the operational holdings dashboard renders from the canonical payload;
        # the archived legacy portfolio is never the current Portfolio page.
        assert "pdash-tbody" in route
        assert "Legacy Portfolio Terminal" not in route


# =========================================================================== #
# Scenario 11 — legacy UI absence; Research & Audit remains
# =========================================================================== #
class TestUiStaticLegacyAbsence:
    def test_orphaned_legacy_tabs_removed(self, html):
        assert 'id="tab-review-queue"' not in html
        assert 'id="tab-signals-decisions"' not in html

    def test_right_panel_order_controls_removed(self, html):
        assert "Advanced order controls (legacy paper workflow)" not in html
        assert 'id="right-create-orders-btn"' not in html
        assert 'id="right-fill-orders-btn"' not in html
        assert 'id="right-cancel-orders-btn"' not in html

    def test_prediction_path_badge_removed(self, html):
        header = html[html.index("<header>"):html.index("</header>")]
        assert 'id="pred-health-badge"' not in header
        assert "LEGACY PREDICTION PATH" not in html

    def test_research_audit_still_available(self, html):
        assert 'data-route="research-audit"' in html
        assert 'id="tab-audit-advanced"' in html


# =========================================================================== #
# Nav cutover — OPERATE / RESEARCH groups + Model Target rename
# =========================================================================== #
class TestUiStaticNavCutover:
    def test_operate_and_research_groups(self, html):
        assert '<div class="sidebar-label">Operate</div>' in html
        assert '<div class="sidebar-label">Research</div>' in html

    def test_model_target_rename(self, html):
        i_research = html.index('<div class="sidebar-label">Research</div>')
        i_actions = html.index('<div class="sidebar-label">Actions</div>')
        research_block = html[i_research:i_actions]
        assert ">Model Target</a>" in research_block
        assert 'data-route="multi-horizon"' in research_block
        # the route id and alias are unchanged (no route change)
        assert "'alpha-portfolio': 'multi-horizon'" in html

    def test_operate_group_has_the_four_operator_routes(self, html):
        i_op = html.index('<div class="sidebar-label">Operate</div>')
        i_res = html.index('<div class="sidebar-label">Research</div>')
        operate_block = html[i_op:i_res]
        for route in ("command-center", "portfolio", "daily-workflow", "portfolio-manager"):
            assert 'data-route="%s"' % route in operate_block, route
