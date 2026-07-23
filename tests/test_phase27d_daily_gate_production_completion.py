"""
tests/test_phase27d_daily_gate_production_completion.py — Phase 27D.

DAILY GATE PRODUCTION COMPLETION + OPERATOR CONSISTENCY.

Fully offline. Exercises:
  * the canonical target-state mapping (CURRENT_ALIGNED / PROPOSAL_READY / ...);
  * the canonical checks_performed contract (13 daily risk / control checks),
    including the honest MONITOR_ONLY volatility/drawdown monitors and the
    NOT_AVAILABLE degrade when source data is absent;
  * position-weight-limit and sector-concentration breaches -> proposal, and the
    data/integrity blocker when the model target itself violates its cap;
  * the renamed MATERIALITY / COST-CONTROL check (no fabricated economic benefit);
  * the operator date-warning fix (canonical operational dates, never the archived
    legacy portfolio date);
  * page-load safety, the safety block, and operator-UI consistency (no legacy
    controls, no native dialogs, Model Target clearly non-executed, the 25 holdings
    on Portfolio, the collapsed Order-and-Fill-History audit).

Nothing here writes anywhere; no order / fill / snapshot / trade is created.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from paper_trader.api import daily_action_gate as dag
from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import portfolio_manager as pm

# Reuse the Phase 27C offline harness + builders.
from tests.test_phase27a_paper_operations import _AUTH, client, env  # noqa: F401
from tests.test_phase27b1_operational_surface_cutover import env27b1  # noqa: F401
from tests.test_phase27b5_operator_flow import _filled_world
from tests.test_phase27c_daily_event_operator_cutover import (  # noqa: F401
    _fake_current, _fake_recs, _holdings, _synth_ops, _target,
)

_ROOT = Path(__file__).resolve().parents[1]
_UI = _ROOT / "api" / "ui" / "index.html"


@pytest.fixture(scope="module")
def html() -> str:
    return _UI.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def js(html) -> str:
    return "\n".join(m.group(1) for m in
                     re.finditer(r"(?s)<script[^>]*>(.*?)</script>", html))


# --------------------------------------------------------------------------- #
# Builders — a rich, fully-enriched aligned book (weights, sectors, ADV, risk)
# --------------------------------------------------------------------------- #
def _rich_aligned(n=6, weight=0.04, sectors=None, adv=2.0e7, vol=0.20, dd=-0.30):
    ticks = [chr(65 + i) for i in range(n)]
    holdings, target = {}, {}
    for i, tk in enumerate(ticks):
        sec = sectors[i] if sectors else "S%d" % (i % 4)
        holdings[tk] = {"weight": weight, "quantity": 10, "sector": sec,
                        "adv_dollar": adv, "realized_vol": vol, "max_drawdown": dd}
        target[tk] = {"weight": weight, "rank": i + 1, "sector": sec, "adv_dollar": adv}
    return holdings, target


def _evaluate(holdings, target, **kw):
    kw.setdefault("target_count", len(target))
    kw.setdefault("data_ready", True)
    return dag.evaluate_daily_action_gate(holdings=holdings, target=target, **kw)


# =========================================================================== #
# A — canonical target-state mapping
# =========================================================================== #
class TestTargetStateMapping:
    def test_no_action_maps_to_current_aligned(self):
        h, tg = _rich_aligned()
        r = _evaluate(h, tg)
        assert r["outcome"] == dag.OUTCOME_NO_ACTION_TODAY
        assert r["target_state"] == dag.TARGET_STATE_CURRENT_ALIGNED
        assert r["target_state_label"] == "CURRENT — ALIGNED WITH HOLDINGS"
        assert "READY TO CONFIRM" not in r["target_state_label"].upper()

    def test_proposal_maps_to_manual_review(self):
        h, tg = _rich_aligned()
        tg["Z"] = {"weight": 0.04, "rank": 99, "sector": "S1"}   # a genuine entrant
        r = _evaluate(h, tg, target_count=len(tg))
        assert r["outcome"] == dag.OUTCOME_PROPOSAL_READY
        assert r["target_state"] == dag.TARGET_STATE_PROPOSAL_READY
        assert "MANUAL REVIEW" in r["target_state_label"].upper()
        assert r["action_required"] is True

    def test_data_not_ready_maps_to_refresh(self):
        r = dag.evaluate_daily_action_gate(holdings={}, target={}, target_count=0,
                                           data_ready=False)
        assert r["target_state"] == dag.TARGET_STATE_DATA_NOT_READY
        assert r["target_state_label"] == "DATA REFRESH REQUIRED"

    def test_orders_submitted_maps(self):
        h, tg = _rich_aligned()
        r = _evaluate(h, tg, orders_pending=25)
        assert r["target_state"] == dag.TARGET_STATE_ORDERS_SUBMITTED
        assert r["target_state_label"] == "PAPER ORDERS SUBMITTED"

    def test_every_outcome_has_a_target_state(self):
        # the mapping is total over the canonical outcome vocabulary
        for outcome in dag.ALL_OUTCOMES:
            ts, label = dag._target_state_for(outcome)
            assert ts in dag.ALL_TARGET_STATES
            assert label and "READY TO CONFIRM" not in label.upper()


# =========================================================================== #
# B — operator date warning uses canonical OPERATIONAL dates only
# =========================================================================== #
def _fake_ctx():
    return {"cur": {"market_as_of_date": "2026-07-22", "fundamental_as_of_date": "2026-05-22",
                    "fundamental_month": "2026-05"},
            "state": {"sleeves": []},
            "valuation": {"as_of_market_date": "2026-07-20"},  # archived legacy date
            "ready": True, "prior_combined": {}}


class TestOperatorDateWarning:
    def test_aligned_operational_dates_no_warning(self):
        # model market date == desk mark == book valuation -> no operator warning even
        # though the ARCHIVED legacy valuation date (2026-07-20) differs.
        dates, warns = pm._dates_block(_fake_ctx(), operational_dates={
            "latest_completed_market_date": "2026-07-22",
            "desk_mark_date": "2026-07-22", "book_valuation_date": "2026-07-22"})
        assert not any("misalignment" in w.lower() for w in warns)
        assert dates["portfolio_valuation_date"] == "2026-07-20"  # still labeled, archived

    def test_no_false_july20_warning(self):
        # the exact production defect: model 2026-07-22 vs legacy 2026-07-20 must NOT warn.
        _dates, warns = pm._dates_block(_fake_ctx(), operational_dates={
            "latest_completed_market_date": "2026-07-22",
            "desk_mark_date": "2026-07-22", "book_valuation_date": "2026-07-22"})
        assert not any("2026-07-20" in w for w in warns)

    def test_genuine_operational_misalignment_warns_with_sources(self):
        _dates, warns = pm._dates_block(_fake_ctx(), operational_dates={
            "latest_completed_market_date": "2026-07-22",
            "desk_mark_date": "2026-07-20", "book_valuation_date": "2026-07-20"})
        w = "\n".join(warns)
        assert "Operational date misalignment" in w
        assert "2026-07-22" in w and "2026-07-20" in w      # each date
        assert "market date" in w.lower() and "desk mark" in w.lower()  # each field name
        assert "manual after-market desk refresh" in w.lower()          # required action

    def test_archived_label_is_explicit(self):
        assert "Archived legacy" in pm.DATE_LABELS["portfolio_valuation_date"]
        assert "operational" in pm.DATE_LABELS["portfolio_valuation_date"].lower()


# =========================================================================== #
# D — checks_performed contract (13 canonical checks)
# =========================================================================== #
class TestChecksPerformed:
    def test_contains_all_required_checks(self):
        h, tg = _rich_aligned()
        r = _evaluate(h, tg)
        codes = {c["code"] for c in r["checks_performed"]}
        assert codes == set(dag.ALL_CHECK_CODES)
        assert len(r["checks_performed"]) == 13
        for c in r["checks_performed"]:
            assert c["status"] in (dag.CHK_PASS, dag.CHK_WARN, dag.CHK_TRIGGERED,
                                   dag.CHK_NOT_AVAILABLE, dag.CHK_MONITOR_ONLY)
            assert set(c) >= {"code", "label", "status", "as_of_date", "source",
                              "summary", "affected_tickers"}

    def test_live_aligned_fixture_zero_triggered(self):
        h, tg = _rich_aligned()
        r = _evaluate(h, tg)
        assert r["checks_summary"]["triggered"] == 0
        assert r["checks_summary"]["not_available"] == 0
        assert "0 triggered" in r["checks_summary"]["line"]

    def test_volatility_monitor_only_no_invented_threshold(self):
        h, tg = _rich_aligned(vol=0.9)   # a very high vol still never TRIGGERS
        r = _evaluate(h, tg)
        vol = next(c for c in r["checks_performed"] if c["code"] == dag.CHECK_VOLATILITY_MONITOR)
        assert vol["status"] == dag.CHK_MONITOR_ONLY
        assert "no validated enforcement threshold" in vol["summary"].lower()
        assert dag.CHK_TRIGGERED not in [vol["status"]]

    def test_drawdown_monitor_only(self):
        h, tg = _rich_aligned(dd=-0.85)
        r = _evaluate(h, tg)
        dd = next(c for c in r["checks_performed"] if c["code"] == dag.CHECK_DRAWDOWN_MONITOR)
        assert dd["status"] == dag.CHK_MONITOR_ONLY

    def test_missing_source_data_not_available_no_exception(self):
        # holdings without weight / sector / adv / risk -> those checks degrade to
        # NOT_AVAILABLE; the evaluator never raises.
        h = _holdings({t: None for t in "ABCD"})   # weight None
        tg = _target(list("ABCD"))
        r = _evaluate(h, tg)
        by = {c["code"]: c["status"] for c in r["checks_performed"]}
        assert by[dag.CHECK_POSITION_WEIGHT_LIMIT] == dag.CHK_NOT_AVAILABLE
        assert by[dag.CHECK_VOLATILITY_MONITOR] == dag.CHK_NOT_AVAILABLE
        assert by[dag.CHECK_DRAWDOWN_MONITOR] == dag.CHK_NOT_AVAILABLE

    def test_risk_data_availability_reflects_flag(self):
        h, tg = _rich_aligned()
        r = _evaluate(h, tg, risk_ready=False)
        rd = next(c for c in r["checks_performed"] if c["code"] == dag.CHECK_RISK_DATA_AVAILABILITY)
        assert rd["status"] == dag.CHK_NOT_AVAILABLE


# =========================================================================== #
# D — concentration breaches -> proposal / integrity blocker
# =========================================================================== #
class TestConcentrationBreaches:
    def test_hard_eligibility_before_scheduled_review_proposes(self):
        h, tg = _rich_aligned(4)
        del tg["D"]
        r = _evaluate(h, tg, target_count=3,
                      eligibility={"D": {"eligible": False, "reason": "NOT_CURRENT_MEMBER"}},
                      next_scheduled_full_review="2026-08-01", scheduled_review_due=False)
        assert r["outcome"] in (dag.OUTCOME_PROPOSAL_READY, dag.OUTCOME_APPROVAL_REQUIRED)
        assert dag.TRIGGER_HARD_ELIGIBILITY in r["trigger_categories"]
        assert "D" in [x["ticker"] for x in r["proposed_removals"]]

    def test_position_weight_limit_breach_proposes(self):
        h, tg = _rich_aligned(6)
        h["A"]["weight"] = 0.15                       # > 0.10 individual cap
        r = _evaluate(h, tg)
        assert r["outcome"] == dag.OUTCOME_PROPOSAL_READY
        assert dag.TRIGGER_POSITION_LIMIT in r["trigger_categories"]
        chk = next(c for c in r["checks_performed"] if c["code"] == dag.CHECK_POSITION_WEIGHT_LIMIT)
        assert chk["status"] == dag.CHK_TRIGGERED and "A" in chk["affected_tickers"]
        assert any(x["ticker"] == "A" for x in r["proposed_resizes"])

    def test_sector_breach_compliant_target_proposes(self):
        # actual Tech 0.36 > 0.25, target Tech 0.24 <= 0.25 -> propose resize toward target.
        h, tg = _rich_aligned(6, sectors=["Tech"] * 6, weight=0.04)
        h["A"]["weight"] = 0.10
        h["B"]["weight"] = 0.10                        # Tech actual = 0.10+0.10+0.04*4 = 0.36
        r = _evaluate(h, tg)
        assert r["outcome"] == dag.OUTCOME_PROPOSAL_READY
        assert dag.TRIGGER_SECTOR_CONCENTRATION in r["trigger_categories"]
        chk = next(c for c in r["checks_performed"] if c["code"] == dag.CHECK_SECTOR_CONCENTRATION)
        assert chk["status"] == dag.CHK_TRIGGERED

    def test_sector_breach_noncompliant_target_is_integrity_blocker(self):
        # target Tech itself = 0.32 > 0.25 -> data/integrity blocker, no invented trade.
        h, tg = _rich_aligned(8, sectors=["Tech"] * 8, weight=0.04)   # both actual & target 0.32
        r = _evaluate(h, tg)
        assert any(b.get("blocked_reason") == "TARGET_SECTOR_CAP_VIOLATION"
                   for b in r["blocked_changes"])
        assert dag.TRIGGER_SECTOR_CONCENTRATION in r["trigger_categories"]
        assert r["proposed_resizes"] == []             # nothing invented


# =========================================================================== #
# E — materiality / cost-control (renamed economic gate; accurate wording)
# =========================================================================== #
class TestMaterialityCostControl:
    def test_resize_only_below_floor_suppressed(self):
        h = _holdings({"A": 0.07, "B": 0.04, "C": 0.04, "D": 0.04})
        for tk in h:
            h[tk]["sector"] = "S0"
        tg = _target(list("ABCD"))
        r = _evaluate(h, tg)
        assert r["outcome"] == dag.OUTCOME_NO_ACTION_TODAY
        assert dag.TRIGGER_MATERIALITY_COST_CONTROL in r["trigger_categories"]
        assert r["blocked_changes"][0]["blocked_reason"] == "MATERIALITY_FLOOR_NOT_MET"
        # accurate wording: a materiality / cost-control floor, NOT a proven alpha benefit
        note = r["policy"]["note"].lower()
        assert "execution cost" in note
        det = r["blocked_changes"][0]["detail"].lower()
        assert "materiality" in det or "minimum-action floor" in det
        assert "expected alpha" not in det and "proven" not in det

    def test_hard_event_bypasses_the_floor(self):
        h = _holdings({"A": 0.07, "B": 0.038, "C": 0.038, "D": 0.038})
        for tk in h:
            h[tk]["sector"] = "S0"
        tg = _target(list("ABC"))
        r = _evaluate(h, tg, target_count=3,
                      eligibility={"D": {"eligible": False, "reason": "LIQUIDITY_FILTER_FAILED"}})
        assert r["outcome"] == dag.OUTCOME_PROPOSAL_READY
        assert dag.TRIGGER_HARD_ELIGIBILITY in r["trigger_categories"]

    def test_check_labels_use_materiality_wording(self):
        h, tg = _rich_aligned()
        r = _evaluate(h, tg)
        mc = next(c for c in r["checks_performed"] if c["code"] == dag.CHECK_MATERIALITY_COST_CONTROL)
        assert "materiality" in mc["label"].lower() or "cost control" in mc["label"].lower()


# =========================================================================== #
# Safety — page load writes nothing; no broker / automation / live / auto-confirm
# =========================================================================== #
class TestSafety:
    def test_page_load_performs_no_write(self, env27b1):
        from paper_trader.api import paper_trading_desk as desk
        _filled_world()
        before = len(desk._fills(desk._desk_dir(None)))
        r = dag.load_daily_action_gate(today="2026-07-22")
        after = len(desk._fills(desk._desk_dir(None)))
        assert r["performed_write"] is False
        assert after == before

    def test_no_broker_automation_live_or_auto_confirm(self):
        h, tg = _rich_aligned()
        r = _evaluate(h, tg)
        for flag in ("broker_enabled", "automation_enabled", "live_orders_enabled",
                     "auto_order_creation", "auto_target_confirmation",
                     "model_parameters_changed", "champion_replaced", "fast_sleeve_active",
                     "performed_write"):
            assert r[flag] is False

    def test_route_returns_canonical_contract(self, client, env27b1):
        _filled_world()
        resp = client.get("/v1/operations/daily-action-gate", headers=_AUTH)
        assert resp.status_code == 200
        g = resp.json()
        assert g["outcome"] in dag.ALL_OUTCOMES
        assert g["target_state"] in dag.ALL_TARGET_STATES
        assert isinstance(g.get("checks_performed"), list)
        assert g["checks_summary"]["total"] == len(g["checks_performed"])
        assert g["performed_write"] is False


# =========================================================================== #
# G / cross-surface — Portfolio Manager passthrough carries the new contract
# =========================================================================== #
class TestPmPassthrough:
    def test_pm_gate_block_carries_target_state_and_checks(self, monkeypatch):
        held = {t: 0.038 for t in "ABCD"}
        rows = [{"ticker": t, "recommendation": eng.REC_HOLD, "reason_codes": [],
                 "model_ranks": {"current": i + 1}} for i, t in enumerate("ABCD")]
        monkeypatch.setattr(dag.eng, "compute_recommendations", _fake_recs(rows))
        cur = _fake_current(list("ABCD"))
        monkeypatch.setattr(pm, "_DAILY_ACTION_GATE_LOADER",
                            lambda current=None: dag.load_daily_action_gate(
                                current=cur, operational=_synth_ops(held)))
        block = pm._daily_gate_block(current=cur)
        assert block["available"] is True
        assert block["target_state"] == dag.TARGET_STATE_CURRENT_ALIGNED
        assert block["target_state_label"] == "CURRENT — ALIGNED WITH HOLDINGS"
        assert len(block["checks_performed"]) == 13
        assert block["checks_summary"]["triggered"] == 0
        assert block["operational_dates"]["desk_mark_date"] == "2026-07-22"

    def test_research_state_cannot_override_operational_gate(self, js):
        # the OTR (model-target snapshot) band defers to the canonical gate in the
        # aligned state — it never shows a raw operational "READY TO CONFIRM".
        assert "NEXT-CYCLE MODEL TARGET — REVIEW NOT DUE" in js
        assert "window._dagData" in js
        assert "gate.outcome === 'NO_ACTION_TODAY'" in js


# =========================================================================== #
# C — Portfolio Manager decision workspace (collapsed order/fill history)
# =========================================================================== #
class TestPmDecisionWorkspace:
    def test_order_fill_history_is_collapsed_audit(self, html):
        assert 'id="pm-order-fill-history"' in html
        assert "Order and Fill History &mdash; Audit" in html
        # the 25-row order table lives INSIDE the collapsed audit <details>
        seg = html[html.index('id="pm-order-fill-history"'):]
        seg = seg[:seg.index("</details>")]
        assert 'id="pm-lc-orders"' in seg

    def test_history_details_not_open_by_default(self, html):
        i = html.index('id="pm-order-fill-history"')
        tag = html[html.rindex("<details", 0, i):i + 40]
        assert "open" not in tag.split(">")[0]     # collapsed by default

    def test_checks_card_present_on_pm_and_dw(self, html):
        for el in ('id="pm-checks-card"', 'id="pm-checks-body"', 'id="pm-checks-detail"',
                   'id="dw-checks-card"', 'id="dw-checks-body"', 'id="cc-dag-checks"'):
            assert el in html, el

    def test_one_checks_renderer(self, js):
        assert "function renderChecksPerformed" in js
        body = js[js.index("function renderChecksPerformed"):
                  js.index("window.renderChecksPerformed")]
        # one renderer feeds PM + Daily Workflow (via the ['pm','dw'] loop) and the
        # Command Center compact line.
        for tok in ("'pm', 'dw'", "-checks-card", "-checks-body", "cc-dag-checks"):
            assert tok in body, tok


# =========================================================================== #
# H / consistency — operator surfaces render the canonical target state
# =========================================================================== #
class TestOperatorConsistency:
    def test_target_state_chip_on_every_gate_card(self, html):
        for el in ('id="cc-dag-tstate"', 'id="dw-dag-tstate"', 'id="pm-dag-tstate"'):
            assert el in html, el

    def test_renderer_sets_target_state(self, js):
        body = js[js.index("function renderDailyActionGate"):
                  js.index("window.renderDailyActionGate")]
        assert "_dagTstate" in body
        assert "renderChecksPerformed(g)" in body

    def test_no_native_dialogs(self, html):
        for pat in ("alert(", "confirm(", "prompt("):
            assert len(re.findall(r"(?<![\w.])" + re.escape(pat), html)) == 0, pat

    def test_legacy_order_controls_absent(self, html):
        assert 'id="right-create-orders-btn"' not in html
        assert 'id="right-fill-orders-btn"' not in html
        assert 'id="right-cancel-orders-btn"' not in html
        assert 'id="tab-review-queue"' not in html
        assert 'id="tab-signals-decisions"' not in html

    def test_model_target_is_non_executed(self, html):
        assert "MODEL TARGET &mdash; NOT EXECUTED HOLDINGS" in html
        assert "NOT EXECUTED HOLDINGS" in html

    def test_portfolio_holdings_dashboard_present(self, html):
        for el in ('id="pdash-table"', 'id="pdash-tbody"', 'id="pdash-kpi-holdings"'):
            assert el in html, el


# =========================================================================== #
# Accidental validation artifact removed + ignored
# =========================================================================== #
class TestArtifactCleanup:
    def test_artifact_dir_absent_and_ignored(self):
        assert not (_ROOT / ".pytest-phase25-validation").exists()
        gi = (_ROOT / ".gitignore").read_text(encoding="utf-8")
        assert ".pytest-phase25-validation/" in gi
        assert "pytest-phase25-validation/" in gi
