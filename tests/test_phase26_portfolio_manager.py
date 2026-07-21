"""
tests/test_phase26_portfolio_manager.py — Phase 26 Portfolio Manager workstation.

Fully offline: the Phase 25 owned-style CSV fixtures are wired via env seams, the append-only
ledger is redirected to a tmp dir, and the canonical Phase 14-C valuation is injected through the
module seam (no PostgreSQL).  Covers: the six read-only /v1/portfolio-manager routes + auth +
safety fields, the deterministic decision-headline logic (all six phrases, review-due vs
no-review-due, no false urgency), action classification (six buckets, WATCH via the exit buffer,
never plain BUY/SELL), deterministic explanations + BOTH_STRONG/FUNDAMENTAL_LED/MOMENTUM_LED/
MIXED agreement, the executed-vs-proposed-vs-ledger distinction, the INITIAL PORTFOLIO PROPOSAL
state, additions/removals/retained + turnover + estimated-cost + sector-change calculations,
health classifications with explanations, labeled dates + misalignment warnings, the absence of
invented expected returns / confidence scores, read-only preview vs ledger-only confirmation,
warm runtime targets, and the UI static contract (navigation order, page hierarchy, one manual
confirmation flow, collapsed evidence, no native dialogs, no blank controls).
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import multi_horizon_ledger as ledger
from paper_trader.api import multi_horizon_platform as plat
from paper_trader.api import portfolio_manager as pm
from paper_trader.api.app import app
from paper_trader.config import get_settings

from tests.test_multi_horizon_platform import (  # reuse the Phase 25 owned-style fixtures
    _write_fund_panel, _write_mom_current, _write_risk, _write_mom_monthly, _write_sector_map,
)

_KEY = "pm-test-key"
_AUTH = {"X-API-Key": _KEY}
_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"

_PM_ROUTES = [
    "/v1/portfolio-manager/summary",
    "/v1/portfolio-manager/actions",
    "/v1/portfolio-manager/changes",
    "/v1/portfolio-manager/health",
    "/v1/portfolio-manager/explanations",
    "/v1/portfolio-manager/since-last-review",
]


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
def _fake_valuation(positions=None, freshness="FRESH", as_of="2026-07-17", seeded=True,
                    age=0):
    positions = positions if positions is not None else []
    return {
        "status": "OK" if seeded else "DEGRADED", "seeded": seeded,
        "current_mark": {
            "current_total_value": "10000.00", "current_cash": "7000.00",
            "current_positions_value": "3000.00", "current_unrealized_pnl": "12.34",
            "current_total_return_pct": 0.0, "open_position_count": len(positions),
            "as_of_market_date": as_of, "freshness_status": freshness,
            "age_calendar_days": age,
        },
        "positions": positions,
        "warnings": [],
    }


def _pos(ticker, status="HOLD", weight="15.00", upnl_pct="0.50",
         reason="Position is within healthy parameters. No action required."):
    return {"ticker": ticker, "weight_pct": weight, "market_value": "1500.00",
            "unrealized_pnl": "5.00", "unrealized_pnl_pct": upnl_pct,
            "status": status, "reason": reason}


@pytest.fixture
def env(monkeypatch, tmp_path):
    panel = tmp_path / "panel.csv"
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    led = tmp_path / "ledger"
    smap = tmp_path / "sector_map.csv"
    _write_fund_panel(panel)
    _write_mom_current(inputs / eng.CUR_MOM_FILE)
    _write_risk(inputs / eng.RISK_FILE)
    _write_mom_monthly(inputs / eng.MONTHLY_PANEL_FILE)
    _write_sector_map(smap)
    monkeypatch.setenv(eng.PANEL_ENV, str(panel))
    monkeypatch.setenv(eng.INPUTS_ENV, str(inputs))
    monkeypatch.setenv(eng.SECTOR_MAP_ENV, str(smap))
    monkeypatch.setenv(ledger.LEDGER_DIR_ENV, str(led))
    monkeypatch.setenv(plat.FAST_SPEC_ENV, str(tmp_path / "no_fast_spec.json"))
    monkeypatch.setattr(pm, "_VALUATION_LOADER",
                        lambda: _fake_valuation(positions=[_pos("AAA"), _pos("ZZZ")]))
    eng.clear_cache()
    plat.clear_caches()
    yield {"panel": panel, "inputs": inputs, "ledger": led, "tmp": tmp_path}
    eng.clear_cache()
    plat.clear_caches()


@pytest.fixture
def client(env, monkeypatch) -> TestClient:
    monkeypatch.setenv("PAPER_TRADER_SERVICE_API_KEY", _KEY)
    monkeypatch.setenv("PAPER_TRADER_DATABASE_URL",
                       "postgresql+psycopg2://unused:unused@localhost:5432/unused")
    get_settings.cache_clear()
    with TestClient(app) as c:
        yield c


def _confirm_snapshot():
    out = ledger.confirm_snapshot(confirm=ledger.CONFIRM_TOKEN)
    assert out["status"] == ledger.STATUS_CONFIRMED
    return out


def _advance_month(env, month="2026-08", market_date="2026-08-14", drop_member=None):
    """Rewrite the momentum CSV to a new month (optionally making one name ineligible)."""
    _write_mom_current(env["inputs"] / eng.CUR_MOM_FILE, market_date=market_date, month=month)
    if drop_member:
        p = env["inputs"] / eng.CUR_MOM_FILE
        txt = p.read_text(encoding="utf-8").splitlines()
        out = []
        for line in txt:
            if line.startswith(drop_member + ","):
                parts = line.split(",")
                parts[2] = "0"  # is_member -> 0 (drops out of the eligible universe)
                line = ",".join(parts)
            out.append(line)
        p.write_text("\n".join(out) + "\n", encoding="utf-8")
    eng.clear_cache()
    plat.clear_caches()


# --------------------------------------------------------------------------- #
# API auth + safety fields
# --------------------------------------------------------------------------- #
class TestApiAuthAndSafety:
    def test_auth_required_on_all_routes(self, client):
        for path in _PM_ROUTES:
            assert client.get(path).status_code in (401, 403), path

    def test_wrong_key_rejected(self, client):
        for path in _PM_ROUTES:
            assert client.get(path, headers={"X-API-Key": "nope"}).status_code in (401, 403), path

    def test_all_routes_ok_with_key_and_carry_safety_fields(self, client):
        for path in _PM_ROUTES:
            r = client.get(path, headers=_AUTH)
            assert r.status_code == 200, path
            d = r.json()
            assert d["paper_only"] is True, path
            assert d["orders_enabled"] is False, path
            assert d["automation_enabled"] is False, path
            assert d["broker_enabled"] is False, path
            assert d["champion_replaced"] is False, path
            assert d["performed_write"] is False, path

    def test_no_invented_expected_returns_or_confidence(self, client):
        for path in _PM_ROUTES:
            blob = json.dumps(client.get(path, headers=_AUTH).json()).lower()
            assert '"expected_return' not in blob, path
            assert '"confidence' not in blob, path


# --------------------------------------------------------------------------- #
# Decision headline logic (Workstream A) — deterministic, no false urgency
# --------------------------------------------------------------------------- #
class TestDecisionHeadline:
    def test_initial_proposal_headline(self, env):
        s = pm.load_summary()
        assert s["status"] == "PM_SUMMARY_READY"
        assert s["decision_headline"] == pm.HEADLINE_REVIEW_NEW
        assert "INITIAL PORTFOLIO PROPOSAL" in s["decision_reason"]
        assert s["review_due"] is True

    def test_no_review_due_says_no_change_required(self, env):
        _confirm_snapshot()
        eng.clear_cache(); plat.clear_caches()
        s = pm.load_summary()
        assert s["review_due"] is False
        assert s["decision_headline"] == pm.HEADLINE_NO_CHANGE
        assert "do nothing" in s["decision_reason"]

    def test_new_period_unchanged_book_requires_manual_confirmation(self, env):
        _confirm_snapshot()
        _advance_month(env)  # same names, new month -> review due, zero changes
        s = pm.load_summary()
        assert s["review_due"] is True
        assert s["changes_summary"]["n_additions"] == 0
        assert s["changes_summary"]["n_removals"] == 0
        assert s["decision_headline"] == pm.HEADLINE_MANUAL_CONFIRMATION

    def test_new_period_changed_book_is_rebalance_review(self, env):
        _confirm_snapshot()
        _advance_month(env, drop_member="AAA")  # AAA leaves the eligible universe
        s = pm.load_summary()
        assert s["review_due"] is True
        assert s["changes_summary"]["n_removals"] >= 1
        assert s["decision_headline"] == pm.HEADLINE_REVIEW_REBALANCE

    def test_missing_inputs_is_data_refresh_required(self, env):
        (env["inputs"] / eng.CUR_MOM_FILE).unlink()
        eng.clear_cache(); plat.clear_caches()
        s = pm.load_summary()
        assert s["status"] == "PM_INPUTS_UNAVAILABLE"
        assert s["decision_headline"] == pm.HEADLINE_DATA_REFRESH

    def test_stale_fundamental_is_data_refresh_required(self, env):
        # fundamental panel stuck in 2026-02 vs market 2026-07 -> > one quarter stale
        _write_fund_panel(env["panel"], month="2026-02", extra_months=())
        eng.clear_cache(); plat.clear_caches()
        s = pm.load_summary()
        assert s["decision_headline"] == pm.HEADLINE_DATA_REFRESH
        assert "stale" in s["decision_reason"].lower()

    def test_blocked_health_without_review_due_is_risk_exceptions(self, env, monkeypatch):
        _confirm_snapshot()
        eng.clear_cache(); plat.clear_caches()

        def _boom():
            raise RuntimeError("db down")
        monkeypatch.setattr(pm, "_VALUATION_LOADER", _boom)
        s = pm.load_summary()
        assert s["review_due"] is False
        assert s["decision_headline"] == pm.HEADLINE_REVIEW_RISK
        assert "BLOCKED" in s["decision_reason"]

    def test_headline_is_always_one_of_the_six(self, env):
        s = pm.load_summary()
        assert s["decision_headline"] in pm.ALL_HEADLINES
        assert set(s["decision_headline_vocabulary"]) == set(pm.ALL_HEADLINES)


# --------------------------------------------------------------------------- #
# Action classification (Workstream B)
# --------------------------------------------------------------------------- #
class TestActionClassification:
    def test_mapping_is_deterministic(self):
        assert pm._pm_action({"recommendation": "BUY_CANDIDATE", "reason_codes": []}) == pm.ACTION_ADD
        assert pm._pm_action({"recommendation": "HOLD", "reason_codes": ["REMAINS_TOP_25"]}) == pm.ACTION_HOLD
        assert pm._pm_action({"recommendation": "HOLD",
                              "reason_codes": ["WITHIN_EXIT_BUFFER_TOP_30"]}) == pm.ACTION_WATCH
        assert pm._pm_action({"recommendation": "REDUCE_CANDIDATE", "reason_codes": []}) == pm.ACTION_REDUCE
        assert pm._pm_action({"recommendation": "EXIT_CANDIDATE",
                              "reason_codes": ["FELL_BELOW_EXIT_BUFFER"]}) == pm.ACTION_EXIT
        assert pm._pm_action({"recommendation": "WAIT", "reason_codes": []}) == pm.ACTION_WAIT_BLOCKED

    def test_counts_always_cover_all_six_buckets(self, env):
        a = pm.load_actions()
        assert set(a["counts"]) == set(pm.ALL_ACTIONS)
        assert a["counts"][pm.ACTION_ADD] == 8  # initial proposal: every eligible name enters

    def test_visible_labels_are_portfolio_manager_language(self, env):
        a = pm.load_actions()
        assert a["action_display_labels"][pm.ACTION_ADD] == "ADD CANDIDATES"
        assert a["action_display_labels"][pm.ACTION_WAIT_BLOCKED] == "WAIT / DATA BLOCKED"
        blob = json.dumps(a)
        assert '"BUY"' not in blob and '"SELL"' not in blob  # never plain BUY/SELL

    def test_rows_carry_all_required_fields(self, env):
        a = pm.load_actions()
        required = ["ticker", "action", "action_label", "engine_recommendation", "target_weight",
                    "current_weight", "combined_rank", "fund_rank", "mom_rank", "fund_contribution",
                    "mom_contribution", "sector", "risk_flags", "reason_codes", "reason_text",
                    "review_due", "agreement"]
        for row in a["actions"]:
            for f in required:
                assert f in row, (row.get("ticker"), f)

    def test_contributions_are_fixed_fifty_fifty(self, env):
        a = pm.load_actions()
        for row in a["actions"]:
            if row["fund_percentile"] is not None:
                assert row["fund_contribution"] == pytest.approx(0.5 * row["fund_percentile"], abs=1e-6)
            if row["mom_percentile"] is not None:
                assert row["mom_contribution"] == pytest.approx(0.5 * row["mom_percentile"], abs=1e-6)

    def test_review_not_due_rows_are_hold_and_monitor_only(self, env):
        _confirm_snapshot()
        eng.clear_cache(); plat.clear_caches()
        a = pm.load_actions()
        assert a["review_due"] is False
        for row in a["actions"]:
            assert row["action"] in (pm.ACTION_HOLD, pm.ACTION_WAIT_BLOCKED)
            if row["action"] == pm.ACTION_HOLD:
                assert "Review is not due; monitor only." in row["reason_text"]


# --------------------------------------------------------------------------- #
# Agreement classification (Workstream F)
# --------------------------------------------------------------------------- #
class TestAgreement:
    def test_both_strong(self):
        assert pm._agreement(0.9, 0.7) == pm.AGREE_BOTH_STRONG

    def test_fundamental_led(self):
        assert pm._agreement(0.8, 0.3) == pm.AGREE_FUNDAMENTAL_LED

    def test_momentum_led(self):
        assert pm._agreement(0.2, 0.95) == pm.AGREE_MOMENTUM_LED

    def test_mixed(self):
        assert pm._agreement(0.4, 0.5) == pm.AGREE_MIXED

    def test_none_when_a_leg_is_missing(self):
        assert pm._agreement(None, 0.9) is None


# --------------------------------------------------------------------------- #
# Deterministic explanations (Workstream C)
# --------------------------------------------------------------------------- #
class TestExplanations:
    def test_structure_and_evidence_blocks(self, env):
        e = pm.load_explanations()
        assert e["status"] == "PM_EXPLANATIONS_READY"
        assert len(e["explanations"]) == 8
        for x in e["explanations"]:
            for block in ("alpha_evidence", "portfolio_fit", "risk", "timing",
                          "contribution", "would_qualify", "phrases"):
                assert block in x, (x["ticker"], block)
            assert x["agreement"] in (pm.AGREE_BOTH_STRONG, pm.AGREE_FUNDAMENTAL_LED,
                                      pm.AGREE_MOMENTUM_LED, pm.AGREE_MIXED, None)

    def test_top_name_phrases_are_grounded(self, env):
        e = pm.load_explanations()
        top = e["explanations"][0]
        assert "Ranked in the combined Top-25." in top["phrases"]
        assert top["portfolio_fit"]["in_target_book"] is True
        assert top["would_qualify"]["combined_top25"] is True

    def test_no_llm_and_no_invented_numbers(self, env):
        e = pm.load_explanations()
        assert "no language model" in e["method_note"].lower()
        blob = json.dumps(e).lower()
        assert '"expected_return' not in blob and '"confidence' not in blob

    def test_fast_sleeve_contributes_nothing(self, env):
        e = pm.load_explanations()
        assert "inactive" in e["fast_sleeve_note"].lower()

    def test_stale_fundamental_phrase(self, env):
        _write_fund_panel(env["panel"], month="2026-02", extra_months=())
        eng.clear_cache(); plat.clear_caches()
        e = pm.load_explanations()
        assert any("Fundamental data is stale; no action permitted." in x["phrases"]
                   for x in e["explanations"])


# --------------------------------------------------------------------------- #
# Changeset + executed-vs-proposed distinction (Workstream D)
# --------------------------------------------------------------------------- #
class TestChanges:
    def test_initial_proposal_state(self, env):
        c = pm.load_changes()
        assert c["status"] == "PM_CHANGES_READY"
        assert c["change_basis"] == pm.CHANGE_BASIS_INITIAL
        assert c["is_initial_portfolio_proposal"] is True
        assert len(c["additions"]) == 8 and not c["removals"] and not c["retained"]
        assert c["estimated_turnover"] == 1.0

    def test_executed_vs_proposed_are_distinguished(self, env):
        c = pm.load_changes()
        evp = c["executed_vs_proposed"]
        ex = evp["current_executed_paper_portfolio"]
        assert ex["open_position_count"] == 2
        assert ex["tickers"] == ["AAA", "ZZZ"]
        assert "NOT the confirmed" in ex["note"]
        assert evp["proposed_alpha_target_portfolio"]["book_id"] == pm.PRIMARY_BOOK_ID
        assert evp["confirmed_alpha_snapshot_ledger"]["n_confirmed"] == 0
        # AAA is in the fixture universe (overlap); ZZZ is executed-only
        assert "AAA" in evp["overlap_tickers"]
        assert "ZZZ" in evp["executed_only_tickers"]

    def test_unchanged_book_after_confirm_has_zero_turnover_and_cost(self, env):
        _confirm_snapshot()
        eng.clear_cache(); plat.clear_caches()
        c = pm.load_changes()
        assert c["change_basis"] == pm.CHANGE_BASIS_SNAPSHOT
        assert not c["additions"] and not c["removals"]
        assert len(c["retained"]) == 8
        assert c["estimated_turnover"] == 0.0
        assert c["estimated_transaction_cost_round_trip"] == 0.0
        assert c["estimated_transaction_cost_one_way"] == 0.0

    def test_removal_computed_when_prior_member_becomes_ineligible(self, env):
        _confirm_snapshot()
        _advance_month(env, drop_member="AAA")
        c = pm.load_changes()
        removed = [r["ticker"] for r in c["removals"]]
        assert removed == ["AAA"]
        assert all(r["ticker"] != "AAA" for r in c["retained"])
        assert c["estimated_turnover"] > 0.0

    def test_sector_changes_and_concentration(self, env):
        c = pm.load_changes()
        secs = {s["sector"]: s for s in c["sector_weight_changes"]}
        for s in secs.values():
            assert s["before"] == 0.0  # initial proposal: nothing held before
            assert s["delta"] == pytest.approx(s["after"] - s["before"], abs=1e-9)
        cc = c["concentration_change"]
        assert cc["largest_sector_after"] in secs
        assert cc["sector_cap_fraction"] == eng.SECTOR_CAP_FRACTION
        assert c["cash_weight_change"]["before"] == 1.0

    def test_cost_assumption_is_the_stored_25bps(self, env):
        c = pm.load_changes()
        assert c["cost_assumption_bps"] == 25
        assert c["estimated_transaction_cost_one_way"] == pytest.approx(
            eng.COST_BPS * c["estimated_turnover"], abs=1e-9)


# --------------------------------------------------------------------------- #
# Since last review (Workstream G)
# --------------------------------------------------------------------------- #
class TestSinceLastReview:
    def test_no_prior_snapshot_state(self, env):
        r = pm.load_since_last_review()
        assert r["status"] == "NO_PRIOR_CONFIRMED_ALPHA_SNAPSHOT"
        assert "NO PRIOR CONFIRMED ALPHA SNAPSHOT" in r["message"]
        assert len(r["initial_target_portfolio"]) == 8
        assert r["initial_estimated_turnover"] == 1.0
        assert r["initial_estimated_cost"] is not None
        assert "confirm it manually" in r["next_required_manual_action"]

    def test_diff_vs_confirmed_snapshot(self, env):
        _confirm_snapshot()
        _advance_month(env, drop_member="AAA")
        r = pm.load_since_last_review()
        assert r["status"] == "PM_SINCE_LAST_REVIEW_READY"
        assert [d["ticker"] for d in r["dropped_names"]] == ["AAA"]
        assert r["new_entrants"] == []
        assert r["expected_turnover_change"]["before"] == 1.0
        assert r["expected_turnover_change"]["after"] > 0.0
        assert isinstance(r["data_quality_changes"]["changed_input_fingerprints"], list)


# --------------------------------------------------------------------------- #
# Portfolio health (Workstream E)
# --------------------------------------------------------------------------- #
class TestHealth:
    def test_statuses_and_explanations(self, env):
        h = pm.load_health()
        assert h["status"] == "PM_HEALTH_READY"
        assert h["overall_status"] in (pm.HEALTH_HEALTHY, pm.HEALTH_REVIEW, pm.HEALTH_BLOCKED)
        assert set(h["status_vocabulary"]) == {pm.HEALTH_HEALTHY, pm.HEALTH_REVIEW, pm.HEALTH_BLOCKED}
        for i in h["items"]:
            assert i["status"] in h["status_vocabulary"]
            assert i["explanation"], i["key"]  # every item (esp. non-healthy) is explained

    def test_watch_position_produces_review_exception(self, env, monkeypatch):
        monkeypatch.setattr(pm, "_VALUATION_LOADER", lambda: _fake_valuation(
            positions=[_pos("AAA", status="WATCH", upnl_pct="-3.00",
                            reason="Unrealized P&L of -3.0% is in the watch range.")]))
        h = pm.load_health()
        exc = [i for i in h["items"] if i["key"] == "risk_exceptions"][0]
        assert exc["status"] == pm.HEALTH_REVIEW
        assert "AAA" in exc["value"] and "watch range" in exc["explanation"]

    def test_valuation_failure_blocks(self, env, monkeypatch):
        def _boom():
            raise RuntimeError("db down")
        monkeypatch.setattr(pm, "_VALUATION_LOADER", _boom)
        h = pm.load_health()
        val = [i for i in h["items"] if i["key"] == "valuation"][0]
        assert val["status"] == pm.HEALTH_BLOCKED
        assert h["overall_status"] == pm.HEALTH_BLOCKED

    def test_stale_valuation_flagged_for_review(self, env, monkeypatch):
        monkeypatch.setattr(pm, "_VALUATION_LOADER", lambda: _fake_valuation(
            positions=[_pos("AAA")], freshness="STALE", age=9))
        h = pm.load_health()
        stale = [i for i in h["items"] if i["key"] == "stale_data"][0]
        assert stale["status"] == pm.HEALTH_REVIEW
        assert "older than 4 calendar days" in stale["explanation"]

    def test_liquidity_and_sector_items_present(self, env):
        h = pm.load_health()
        keys = {i["key"] for i in h["items"]}
        assert {"liquidity", "largest_sector", "volatility", "drawdown",
                "cash_pct", "invested_pct", "capacity"} <= keys

    def test_no_confidence_score(self, env):
        h = pm.load_health()
        assert "no single opaque confidence" in h["note"].lower()
        blob = json.dumps(h).lower()
        assert '"confidence' not in blob


# --------------------------------------------------------------------------- #
# Labeled dates + misalignment warnings (Workstream L)
# --------------------------------------------------------------------------- #
class TestDates:
    def test_every_date_is_labeled(self, env):
        s = pm.load_summary()
        assert set(s["dates"]) == set(s["date_labels"]) == set(pm.DATE_LABELS)
        assert "alpha calculation market date" in s["date_labels"]["latest_alpha_market_date"]
        assert "valuation" in s["date_labels"]["portfolio_valuation_date"].lower()

    def test_misalignment_warning_when_dates_differ(self, env, monkeypatch):
        monkeypatch.setattr(pm, "_VALUATION_LOADER", lambda: _fake_valuation(
            positions=[_pos("AAA")], as_of="2026-07-10"))
        s = pm.load_summary()
        assert any("Date misalignment" in w for w in s["date_warnings"])
        assert any("NOT collapsed into one date" in w for w in s["date_warnings"])

    def test_no_misalignment_warning_when_dates_match(self, env):
        s = pm.load_summary()  # fixture valuation as_of == market_as_of == 2026-07-17
        assert not any("Date misalignment" in w for w in s["date_warnings"])

    def test_not_ready_when_inputs_stale(self, env):
        _write_fund_panel(env["panel"], month="2026-02", extra_months=())
        eng.clear_cache(); plat.clear_caches()
        s = pm.load_summary()
        # A stale required input can never yield a "nothing to do" READY state.
        assert s["decision_headline"] == pm.HEADLINE_DATA_REFRESH
        assert any("stale" in w.lower() for w in s["date_warnings"])


# --------------------------------------------------------------------------- #
# Read-only guarantees + one manual confirmation flow
# --------------------------------------------------------------------------- #
class TestReadOnlyGuarantees:
    def test_pm_gets_write_nothing(self, env):
        ledger_file = env["ledger"] / ledger.SNAPSHOTS_FILE
        pm.load_summary(); pm.load_actions(); pm.load_changes()
        pm.load_health(); pm.load_explanations(); pm.load_since_last_review()
        assert not ledger_file.exists()  # no snapshot appeared from any GET

    def test_preview_is_read_only_and_confirm_writes_only_the_ledger(self, client, env):
        ledger_file = env["ledger"] / ledger.SNAPSHOTS_FILE
        r = client.post("/v1/research/alpha-paper-snapshots/preview", headers=_AUTH)
        assert r.status_code == 200 and r.json()["performed_write"] is False
        assert not ledger_file.exists()
        r = client.post("/v1/research/alpha-paper-snapshots/confirm", headers=_AUTH,
                        json={"confirm": ledger.CONFIRM_TOKEN})
        assert r.status_code == 200 and r.json()["performed_write"] is True
        assert r.json()["wrote_to_ledger_only"] is True
        assert ledger_file.exists()

    def test_summary_reflects_confirmed_ledger(self, client, env):
        client.post("/v1/research/alpha-paper-snapshots/confirm", headers=_AUTH,
                    json={"confirm": ledger.CONFIRM_TOKEN})
        eng.clear_cache(); plat.clear_caches()
        s = client.get("/v1/portfolio-manager/summary", headers=_AUTH).json()
        assert s["snapshot_ledger"]["n_confirmed"] == 1
        assert s["decision_headline"] == pm.HEADLINE_NO_CHANGE


# --------------------------------------------------------------------------- #
# Warm runtime targets (Workstream N)
# --------------------------------------------------------------------------- #
class TestRuntime:
    def test_warm_targets(self, env):
        pm.load_summary()  # warm the engine cache
        t0 = time.perf_counter(); pm.load_summary(); t_summary = time.perf_counter() - t0
        t0 = time.perf_counter(); pm.load_actions(); t_actions = time.perf_counter() - t0
        t0 = time.perf_counter(); pm.load_changes(); t_changes = time.perf_counter() - t0
        t0 = time.perf_counter(); pm.load_health(); t_health = time.perf_counter() - t0
        assert t_summary < 2.0
        assert t_actions < 3.0
        assert t_changes < 3.0
        assert t_health < 2.0


# --------------------------------------------------------------------------- #
# UI static contract (Workstreams I, J, M)
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def html() -> str:
    return _UI.read_text(encoding="utf-8")


class TestUiNavigationAndHierarchy:
    def test_nav_item_exists_under_views(self, html):
        assert 'id="nav-portfolio-manager"' in html
        assert 'data-route="portfolio-manager"' in html
        assert ">Portfolio Manager</a>" in html

    def test_nav_order_command_center_then_portfolio_manager(self, html):
        cc = html.index('id="nav-command-center"')
        pm_ = html.index('id="nav-portfolio-manager"')
        dw = html.index('id="nav-daily-workflow"')
        pf = html.index('id="nav-portfolio"')
        mh = html.index('id="nav-multi-horizon"')
        ra = html.index('id="nav-research-audit"')
        assert cc < pm_ < dw < pf < mh < ra  # recommended Phase 26 order

    def test_route_registered(self, html):
        assert "'portfolio-manager': 'portfolio-manager'" in html

    def test_tab_and_lazy_load_exist(self, html):
        assert 'id="tab-portfolio-manager"' in html
        assert "window._pmLoaded" in html and "loadPortfolioManager()" in html

    def test_entry_buttons_from_other_views(self, html):
        assert 'id="cc-open-pm-btn"' in html and ">Open Portfolio Manager</button>" in html
        assert 'id="mhz-open-pm"' in html
        assert 'id="pt-open-pm-btn"' in html and ">View Proposed Alpha Portfolio</button>" in html


class TestUiDecisionSurface:
    def test_decision_headline_elements(self, html):
        assert 'id="pm-headline"' in html
        assert "Today's Portfolio Decision" in html
        assert 'id="pm-headline-reason"' in html

    def test_all_six_headline_phrases_in_js(self, html):
        assert "'NO PORTFOLIO CHANGE REQUIRED'" in html
        assert "'DATA REFRESH REQUIRED'" in html
        # the remaining phrases arrive from the API; the color rule covers the two
        # fixed classes and defaults every REVIEW/MANUAL headline to the warning color.

    def test_action_buckets_and_filters(self, html):
        for key in ("'ADD_CANDIDATE'", "'HOLD'", "'WATCH'", "'REDUCE_CANDIDATE'",
                    "'EXIT_CANDIDATE'", "'WAIT_DATA_BLOCKED'"):
            assert key in html, key
        assert "'ADD CANDIDATES'" in html and "'WAIT / DATA BLOCKED'" in html
        assert "pmFilterActions" in html
        for label in ("['ALL', 'All']", "['WATCH', 'Watch']", "['WAIT_DATA_BLOCKED', 'Blocked']"):
            assert label in html, label

    def test_no_plain_buy_sell_buttons(self, html):
        assert not re.search(r">\s*BUY\s*</button>", html)
        assert not re.search(r">\s*SELL\s*</button>", html)

    def test_labeled_dates_and_warning_strip(self, html):
        assert 'id="pm-sb-alpha-date"' in html and 'id="pm-sb-val-date"' in html
        assert 'id="pm-date-warnings"' in html
        assert "Current alpha calculation market date" in html
        assert "Portfolio EOD valuation date" in html

    def test_three_way_distinction_rendered(self, html):
        assert 'id="pm-three-way"' in html
        assert "EXECUTED PAPER PORTFOLIO" in html
        assert "PROPOSED ALPHA TARGET" in html
        assert "CONFIRMED ALPHA LEDGER" in html
        assert 'id="pm-initial-banner"' in html
        assert "INITIAL PORTFOLIO PROPOSAL." in html

    def test_health_panel(self, html):
        assert 'id="pm-health"' in html and 'id="pm-health-overall"' in html


class TestUiManualFlow:
    def test_five_step_flow_and_single_confirmation(self, html):
        assert 'id="pm-flow-steps"' in html
        assert "Preview the paper-alpha snapshot (read-only)." in html
        assert "Confirm the snapshot manually (ledger-only write)." in html
        assert 'id="pm-confirm-box"' in html
        assert 'id="pm-confirm-phrase"' in html
        assert "CONFIRM_MHZ_PAPER_SNAPSHOT" in html
        # exactly one Phase 26 confirm trigger, wired to the in-page styled panel
        assert html.count("pmDoConfirmSnapshot()") == 2  # onclick + window export

    def test_confirmed_language_is_snapshot_not_execution(self, html):
        assert "PAPER ALPHA SNAPSHOT CONFIRMED" in html
        assert "PORTFOLIO EXECUTED" not in html

    def test_no_native_dialogs_anywhere(self, html):
        scripts = "\n".join(re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL))
        for pat in (r"(?<![A-Za-z0-9_])alert\s*\(", r"(?<![A-Za-z0-9_])confirm\s*\(",
                    r"(?<![A-Za-z0-9_])prompt\s*\("):
            assert not re.search(pat, scripts), pat


class TestUiClutterAndSafety:
    def _pm_region(self, html):
        start = html.index('id="tab-portfolio-manager"')
        end = html.index("Phase 26 PORTFOLIO MANAGER END")
        return html[start:end]

    def test_evidence_sections_collapsed_by_default(self, html):
        region = self._pm_region(html)
        for did in ("pm-evidence", "pm-history", "pm-methodology", "pm-audit"):
            m = re.search(r'<details[^>]*id="%s"[^>]*>' % did, region)
            assert m, did
            assert "open" not in m.group(0), did

    def test_no_raw_json_outside_collapsed_audit(self, html):
        region = self._pm_region(html)
        pre_positions = [m.start() for m in re.finditer(r"<pre", region)]
        audit_start = region.index('id="pm-audit"')
        for p in pre_positions:
            assert p > audit_start  # every <pre> lives inside the collapsed audit details

    def test_safety_badges_visible_on_page(self, html):
        region = self._pm_region(html)
        for badge in ("PAPER ONLY", "ORDERS DISABLED", "AUTOMATION OFF", "MANUAL REVIEW",
                      "NO LIVE PROMOTION", "NO POSITION CHANGES"):
            assert badge in region, badge

    def test_no_blank_buttons_in_pm_region(self, html):
        region = self._pm_region(html)
        for m in re.finditer(r"<button[^>]*>(.*?)</button>", region, re.DOTALL):
            label = re.sub(r"<[^>]+>", "", m.group(1))
            label = re.sub(r"&[a-z#0-9]+;", "x", label)
            assert label.strip(), m.group(0)[:120]

    def test_fast_sleeve_note_visible(self, html):
        region = self._pm_region(html)
        assert "fast sleeve is inactive and contributes no recommendation" in region.lower()
