"""Release 30 - read models, operator surfaces and continuous-manager wiring.

These are the tests that keep the two new read models HONEST: they own no
calculation, they read every conclusion from the owner that made it, and neither
of them - nor any UI region they feed - can act.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from paper_trader.api import alpha_leaderboard as lb
from paper_trader.api import material_information as mi
from paper_trader.engine import event_fabric as ef

REPO = Path(__file__).resolve().parents[1]
UI = REPO / "api" / "ui" / "index.html"
APP = REPO / "api" / "app.py"


def _events():
    return {
        "eligible_market_date": "2026-08-18",
        "cycle_id": "cyc_1",
        "last_run": {"run_id": "evt_1", "state": "REASSESSED_NO_CHANGE",
                     "generated_at": "2026-08-19T00:00:00+00:00"},
        "holdings": [{"ticker": "DDOG"}, {"ticker": "MSFT"}],
        "material_events": [
            {"event_id": "e1", "primary_ticker": "DDOG",
             "published_at": "2026-08-18", "source_id": "eodhd",
             "event_type": "DELAYED_QUOTE", "family": "market",
             "decision_authority": ef.AUTH_OPERATIONAL_RISK,
             "materiality_inputs": {"change_pct": -0.068},
             "point_in_time_status": "POINT_IN_TIME_OK"},
            {"event_id": "e2", "primary_ticker": "MSFT",
             "published_at": "2026-08-17", "source_id": "sec",
             "event_type": "FILING_8K", "family": "filing",
             "decision_authority": ef.AUTH_EVENT_TRIGGER_ONLY,
             "materiality_inputs": {}},
            {"event_id": "e3", "primary_ticker": "ZZZZ",
             "published_at": "2026-08-16", "source_id": "news",
             "event_type": "NEWS", "family": "news",
             "decision_authority": ef.AUTH_RESEARCH_ALPHA,
             "materiality_inputs": {}},
        ],
        "affected_holdings": ["DDOG"],
    }


def _hoc():
    return {"holding_reviews": [
        {"ticker": "DDOG", "recommendation": "REDUCE",
         "deterioration_state": "DETERIORATING"},
        {"ticker": "MSFT", "recommendation": "HOLD",
         "deterioration_state": "STABLE"}]}


def _decision():
    return {"portfolio_decision_state": "CHANGE_CANDIDATE_WITHHELD",
            "label": "Change withheld", "approvable": False,
            "requires_manual_review": True}


# =========================================================================== #
# MATERIAL INFORMATION
# =========================================================================== #
def test_01_authority_reach_is_read_from_the_event_fabric():
    """A private copy of the authority table is the duplicated-vocabulary defect
    this project has already paid for several times."""
    src = (REPO / "api" / "material_information.py").read_text(encoding="utf-8")
    assert "ALPHA_BEARING_AUTHORITIES" in src
    assert "RISK_BEARING_AUTHORITIES" in src
    assert "TRIGGER_BEARING_AUTHORITIES" in src
    assert not re.search(r"^_AUTHORITY_REACH\s*=\s*\{", src, re.M)
    policy = mi.authority_reach_policy()
    assert set(policy) == set(ef.SIGNAL_AUTHORITIES)


def test_02_only_an_alpha_bearing_authority_can_reach_the_forecast():
    for a in ef.SIGNAL_AUTHORITIES:
        reach = mi.authority_reach(a)
        assert reach["forecast"] == (a in ef.ALPHA_BEARING_AUTHORITIES)


def test_03_an_event_trigger_only_event_never_becomes_expected_return():
    out = mi.build(event_refresh=_events(), hoc=_hoc(), decision=_decision())
    row = next(r for r in out["rows"] if r["event_id"] == "e2")
    assert row["signal_authority"] == ef.AUTH_EVENT_TRIGGER_ONLY
    assert row["forecast_affected"] is False
    assert row["hoc_affected"] is True, "it may still trigger a review"


def test_04_a_research_alpha_event_reaches_no_operational_calculation():
    out = mi.build(event_refresh=_events(), hoc=_hoc(), decision=_decision())
    row = next(r for r in out["rows"] if r["event_id"] == "e3")
    assert row["forecast_affected"] is False
    assert row["risk_affected"] is False
    assert row["hoc_affected"] is False


def test_05_the_feed_reports_what_changed_and_what_it_touched():
    out = mi.build(event_refresh=_events(), hoc=_hoc(), decision=_decision())
    row = next(r for r in out["rows"] if r["event_id"] == "e1")
    assert row["ticker"] == "DDOG" and row["held"] is True
    assert row["risk_affected"] is True
    assert row["hoc_recommendation"] == "REDUCE"
    assert row["portfolio_reassessed"] is True
    assert row["result"] == "CHANGE_CANDIDATE_WITHHELD"
    assert "-6.80%" in row["what_changed"] or "6.8" in row["what_changed"]


def test_06_the_conclusion_comes_from_the_owner_not_from_the_feed():
    """Change only the HOC payload; the feed's conclusion must follow it."""
    hoc = {"holding_reviews": [{"ticker": "DDOG", "recommendation": "EXIT"}]}
    out = mi.build(event_refresh=_events(), hoc=hoc, decision=_decision())
    row = next(r for r in out["rows"] if r["ticker"] == "DDOG")
    assert row["hoc_recommendation"] == "EXIT"


def test_07_reassessment_is_the_event_lanes_answer():
    ev = _events()
    ev["last_run"]["state"] = "NO_MATERIAL_INFORMATION"
    out = mi.build(event_refresh=ev, hoc=_hoc(), decision=_decision())
    assert out["portfolio_reassessed"] is False
    assert all(r["portfolio_reassessed"] is False for r in out["rows"])


def test_08_the_feed_is_row_capped():
    ev = _events()
    ev["material_events"] = ev["material_events"] * 20
    out = mi.build(event_refresh=ev, hoc=_hoc(), decision=_decision(), limit=5)
    assert out["row_count"] == 5
    assert out["total_material_events"] == 60


def test_09_no_material_information_is_its_own_state():
    ev = _events()
    ev["material_events"] = []
    out = mi.build(event_refresh=ev, hoc=_hoc(), decision=_decision())
    assert out["state"] == mi.STATE_EMPTY
    assert out["rows"] == []


def test_10_the_feed_owns_no_calculation_and_cannot_act():
    out = mi.build(event_refresh=_events(), hoc=_hoc(), decision=_decision())
    assert out["owns_no_calculation"] is True
    assert out["safety"]["creates_orders"] is False
    assert out["safety"]["creates_decisions"] is False
    assert out["safety"]["mutates_holdings"] is False


# =========================================================================== #
# ALPHA LEADERBOARD
# =========================================================================== #
def _tournament():
    return {
        "n_decision_dates": 304, "dates": ["2001-01-02", "2026-04-23"],
        "horizons": [20], "feature_names": ["mom_6_1"],
        "by_horizon": {"20": {
            "folds": 8,
            "models": {
                "adaptive_ensemble": {
                    "model_id": "adaptive_ensemble", "role": "ADAPTIVE_CANDIDATE",
                    "test": {"rank_ic_mean": 0.003, "rank_ic_t": 0.24,
                             "n_dates": 192, "n_rows": 100},
                    "book": {"annualised_net_return": 0.09,
                             "information_ratio": 0.41, "max_drawdown": -0.41,
                             "mean_one_way_turnover": 0.74}},
                "s25_operating_profitability": {
                    "model_id": "s25_operating_profitability",
                    "role": "COMPONENT_ALPHA",
                    "test": {"rank_ic_mean": 0.0009, "rank_ic_t": 0.16,
                             "n_dates": 72, "n_rows": 50},
                    "book": {"annualised_net_return": 0.13,
                             "information_ratio": 0.57, "max_drawdown": -0.30,
                             "mean_one_way_turnover": 0.02}},
                "baseline_momentum_leg": {
                    "model_id": "baseline_momentum_leg", "role": "BENCHMARK",
                    "test": {"rank_ic_mean": 0.0006, "rank_ic_t": 0.05,
                             "n_dates": 192, "n_rows": 100},
                    "book": {"annualised_net_return": 0.185,
                             "information_ratio": 0.68, "max_drawdown": -0.41,
                             "mean_one_way_turnover": 0.51}},
            },
            "ensemble": {"weights": {"ridge": 0.74}, "method": "SHRUNK",
                         "components": {}},
        }},
    }


def _verdict():
    return {"benchmark_model_id": "baseline_momentum_leg",
            "candidate_model_id": "adaptive_ensemble",
            "forecast_model_verdict": "R30_ADAPTIVE_MODEL_NO_GO",
            "criteria": [{"key": "net_return", "meaning": "..."}],
            "by_horizon": {"20": {"state": "FAIL",
                                  "checks": {"net_return": False},
                                  "failed": ["net_return", "rank_ic_t"],
                                  "net_return_difference_annualised": -0.0915,
                                  "paired_net_return_t": -0.67}}}


def test_11_the_leaderboard_reports_the_verdict_it_was_given():
    out = lb.build(tournaments={"price_only": _tournament()},
                   verdicts={"price_only": _verdict()},
                   artifacts={"price_only": {"model_spec_hash": "abc"}},
                   scoring={"primary_model_id": "fundamental_momentum_50_50_v1"},
                   horizon=20)
    u = out["universes"][0]
    assert u["forecast_model_verdict"] == "R30_ADAPTIVE_MODEL_NO_GO"
    assert u["criteria_failed"] == ["net_return", "rank_ic_t"]
    assert u["paired_net_return_t"] == -0.67


def test_12_s25_operating_profitability_is_surfaced_not_buried():
    out = lb.build(tournaments={"price_only": _tournament()},
                   verdicts={"price_only": _verdict()}, artifacts={},
                   scoring={}, horizon=20)
    ids = [m["model_id"] for m in out["universes"][0]["models"]]
    assert "s25_operating_profitability" in ids
    row = next(m for m in out["universes"][0]["models"]
               if m["model_id"] == "s25_operating_profitability")
    assert row["lifecycle"] == lb.LC_COMPONENT
    assert row["oos_rank_ic"] is not None and row["oos_rank_ic_t"] is not None


def test_13_lifecycle_distinguishes_champion_candidate_and_benchmark():
    out = lb.build(tournaments={"price_only": _tournament()},
                   verdicts={"price_only": _verdict()}, artifacts={},
                   scoring={}, horizon=20)
    by = {m["model_id"]: m["lifecycle"] for m in out["universes"][0]["models"]}
    assert by["adaptive_ensemble"] == lb.LC_CANDIDATE
    assert by["baseline_momentum_leg"] == lb.LC_BENCHMARK
    assert by["s25_operating_profitability"] == lb.LC_COMPONENT


def test_14_the_leaderboard_promotes_nothing():
    out = lb.build(tournaments={}, verdicts={}, artifacts={}, scoring={})
    assert out["automatic_promotion_allowed"] is False
    assert out["safety"]["promotes_models"] is False
    assert out["owns_no_calculation"] is True
    assert "requires a human" in out["promotion_doc"]


def test_15_walk_forward_and_forward_evidence_are_never_merged():
    out = lb.build(tournaments={}, verdicts={}, artifacts={}, scoring={},
                   forward_evidence={"observations": 6})
    note = out["forward_evidence"]["note"]
    assert "never merged" in note


def test_16_no_research_evidence_is_its_own_state():
    out = lb.build(tournaments={"price_only": None}, verdicts={}, artifacts={},
                   scoring={})
    assert out["state"] == lb.STATE_NO_RESEARCH
    assert out["universes"] == []


# =========================================================================== #
# ROUTES / OPERATOR SURFACE
# =========================================================================== #
def test_17_release30_declares_read_only_routes():
    app = APP.read_text(encoding="utf-8")
    for route in ("/v1/operations/zero-base-target",
                  "/v1/research/return-forecast",
                  "/v1/operations/material-information",
                  "/v1/research/alpha-leaderboard"):
        assert '"%s"' % route in app, route
    for m in re.finditer(r'@app\.(post|put|delete|patch)\(\s*"([^"]+)"', app):
        assert not any(k in m.group(2) for k in
                       ("zero-base", "return-forecast", "material-information",
                        "alpha-leaderboard")), m.group(2)


def test_18_routes_are_declared_exactly_once():
    app = APP.read_text(encoding="utf-8")
    for route in ("/v1/operations/zero-base-target",
                  "/v1/research/return-forecast",
                  "/v1/operations/material-information",
                  "/v1/research/alpha-leaderboard"):
        assert len(re.findall(r'@app\.get\(\s*"%s"' % re.escape(route), app)) == 1


# =========================================================================== #
# UI
# =========================================================================== #
def test_19_the_three_regions_exist_with_one_loader_each():
    ui = UI.read_text(encoding="utf-8")
    for region in ('id="cc-matinfo-card"', 'id="zb-card"', 'id="albd-panel"'):
        assert region in ui, region
    for fn in ("loadMaterialInformation", "loadZeroBaseTarget",
               "loadAlphaLeaderboard"):
        assert ui.count("function %s(" % fn) == 1, fn


def test_20_no_browser_dialog_in_any_release30_renderer():
    ui = UI.read_text(encoding="utf-8")
    for fn in ("renderMaterialInformation", "renderZeroBaseTarget",
               "renderAlphaLeaderboard"):
        m = re.search(r"function %s\([\s\S]{0,9000}?\n}\n" % fn, ui)
        assert m, fn
        assert "alert(" not in m.group(0), fn
        assert "confirm(" not in m.group(0), fn


def test_21_no_execute_control_inside_a_release30_region():
    ui = UI.read_text(encoding="utf-8")
    for region in ('id="cc-matinfo-card"', 'id="zb-card"', 'id="albd-panel"'):
        i = ui.find(region)
        block = ui[i:i + 6000]
        for token in ("dispatchCanonicalPrimaryAction", "CONFIRM_", "/execute",
                      "orders/confirm", "rebalance/confirm", "onclick=\"run"):
            assert token not in block, "%s in %s" % (token, region)


def test_22_every_region_carries_visible_safety_badges():
    ui = UI.read_text(encoding="utf-8")
    for region, badges in (('id="cc-matinfo-card"', ["READ ONLY"]),
                           ('id="zb-card"', ["PREVIEW ONLY", "NOT A PROPOSAL",
                                             "NO ORDERS", "MANUAL REVIEW"]),
                           ('id="albd-panel"', ["READ ONLY",
                                                "NO LIVE PROMOTION"])):
        i = ui.find(region)
        block = ui[i:i + 3000]
        for b in badges:
            assert b in block, "%s missing %s" % (region, b)


def test_23_the_zero_base_region_states_it_is_not_a_proposal():
    ui = UI.read_text(encoding="utf-8")
    i = ui.find('id="zb-card"')
    assert "NOT A PROPOSAL" in ui[i:i + 3000]
    assert "not a proposal and not a decision" in ui


def test_24_the_ui_computes_no_verdict_and_no_authority():
    """Presentation only: the browser may format, never decide."""
    ui = UI.read_text(encoding="utf-8")
    m = re.search(r"function renderAlphaLeaderboard\([\s\S]{0,9000}?\n}\n", ui)
    body = m.group(0)
    for token in ("R30_ADAPTIVE_MODEL", "NO_GO", "> 2", ">= 2"):
        assert token not in body, "the UI must not derive a verdict (%s)" % token
    m2 = re.search(r"function renderMaterialInformation\([\s\S]{0,9000}?\n}\n", ui)
    for token in ("OPERATIONAL_ALPHA", "EVENT_TRIGGER_ONLY", "ALPHA_BEARING"):
        assert token not in m2.group(0), (
            "the UI must not classify authority (%s)" % token)


def test_25_today_keeps_one_material_information_region_only():
    ui = UI.read_text(encoding="utf-8")
    assert ui.count('id="cc-matinfo-card"') == 1
    assert ui.count('id="matinfo-table"') == 1


def test_26_the_leaderboard_lives_under_the_existing_research_section():
    """No new navigation item: the operator asks 'is the research trustworthy?'
    once."""
    ui = UI.read_text(encoding="utf-8")
    m = re.search(r"'research-agent':\s*\{\s*panels:\s*\[([^\]]*)\]", ui)
    assert m and "albd-panel" in m.group(1)
    assert "'loadAlphaLeaderboard'" in ui


def test_27_the_new_regions_are_wired_into_existing_load_paths():
    ui = UI.read_text(encoding="utf-8")
    assert "loadZeroBaseTarget(); } catch" in ui
    assert "loadMaterialInformation(); } catch" in ui


# =========================================================================== #
# CONTINUOUS MANAGER / SAFETY
# =========================================================================== #
def test_28_the_forecast_refresh_does_not_execute_anything():
    src = (REPO / "api" / "zero_base_target.py").read_text(encoding="utf-8")
    called = set()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Call):
            called.add(getattr(node.func, "attr", getattr(node.func, "id", "")))
    for forbidden in ("run_daily_close", "execute", "generate_orders",
                      "confirm_order_plan", "record_decision"):
        assert forbidden not in called, forbidden


def test_29_release30_adds_no_scheduler_or_automation():
    for rel in ("api/zero_base_target.py", "api/return_forecast.py",
                "api/material_information.py", "api/alpha_leaderboard.py",
                "engine/zero_base_allocator.py", "engine/return_forecast.py"):
        src = (REPO / rel).read_text(encoding="utf-8")
        for token in ("schedule", "cron", "Timer(", "Thread(", "asyncio.create_task"):
            assert token not in src, "%s in %s" % (token, rel)


def test_30_the_canonical_proposal_and_decision_owners_are_untouched_by_r30():
    """Release 30 must not have quietly become a second proposal path."""
    realloc = (REPO / "engine" / "reallocation_proposal.py").read_text(
        encoding="utf-8")
    decision = (REPO / "api" / "portfolio_decision.py").read_text(encoding="utf-8")
    for src in (realloc, decision):
        assert "zero_base_allocator" not in src
        assert "return_forecast" not in src
