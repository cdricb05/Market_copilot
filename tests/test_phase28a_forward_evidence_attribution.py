"""
tests/test_phase28a_forward_evidence_attribution.py

Phase 28A — FORWARD EVIDENCE, ATTRIBUTION AND SHADOW-BOOK COMPARISON.

The forward-evidence module is a read-only, deterministic diagnostic built on the
existing append-only daily-close / mark / performance records. These tests prove:

  * CANONICAL ATTRIBUTION (Part A) reconciles per-position and per-sector to the NAV
    move, computes SPY daily/cumulative return, daily/cumulative excess, drawdown and
    rolling-peak NAV, and exposes explicit availability/coverage when marks are missing;
  * the deterministic "WHY P&L MOVED" narrative (Part B) is traceable and stable;
  * ROLLING EVIDENCE (Part D) is explicitly unavailable before 5 / 20 observations and
    never annualises a short sample without a warning;
  * ACTIVE vs SHADOW (Part C) keeps the active book's FORWARD OPERATIONAL evidence
    strictly separate from the shadow books' HISTORICAL RECONSTRUCTION, never mixed,
    and promotes nothing;
  * empty states are controlled (no null-reference), and nothing is ever mutated
    (holdings / cash / orders / model), proven against the REAL append-only ledgers.

Most tests inject fully offline perf / mark / ops seams with hand-computed expected
values; two integration tests reuse the Phase 27A/27B.1/27B.5 FILLED-book harness so
the reconciliation and no-mutation guarantees hold against the real desk store.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from paper_trader.api import forward_evidence as fe
from paper_trader.api import operational_book as ob
from paper_trader.api import paper_trading_desk as desk

from tests.test_phase27a_paper_operations import (  # reuse the offline harness
    _AUTH, _D0, _TICKS, _marks_table, _refresh, client, env,  # noqa: F401
)
from tests.test_phase27b1_operational_surface_cutover import env27b1  # noqa: F401
from tests.test_phase27b5_operator_flow import _filled_world

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"


# --------------------------------------------------------------------------- #
# Offline fixtures — a hand-computed 3-mark world (deterministic).
#
#   AAA: 50 -> 52 -> 51   (qty 10, avg 49, sector Tech)
#   BBB: 20 -> 19 -> 20.5 (qty 5,  avg 21, sector Energy)
#   nav = 10*AAA + 5*BBB  ->  600 -> 615 -> 612.5   (starting_capital 600)
#   SPY cumulative return %: 0.0 -> 1.0 -> 0.5
# --------------------------------------------------------------------------- #
_SERIES = {
    "AAA": [("2026-07-20", 50.0), ("2026-07-21", 52.0), ("2026-07-22", 51.0)],
    "BBB": [("2026-07-20", 20.0), ("2026-07-21", 19.0), ("2026-07-22", 20.5)],
}
_PERF_ROWS = [
    {"date": "2026-07-20", "nav": 600.0, "cumulative_return_pct": 0.0,
     "benchmark_cumulative_return_pct": 0.0, "drawdown_pct": 0.0, "transaction_cost": 0.0},
    {"date": "2026-07-21", "nav": 615.0, "cumulative_return_pct": 2.5,
     "benchmark_cumulative_return_pct": 1.0, "drawdown_pct": 0.0, "transaction_cost": 0.0},
    {"date": "2026-07-22", "nav": 612.5, "cumulative_return_pct": 2.0833,
     "benchmark_cumulative_return_pct": 0.5, "drawdown_pct": -0.4065, "transaction_cost": 0.0},
]


def _perf(rows, cost=1.0):
    return lambda desk_dir=None: {"rows": list(rows),
                                  "summary": {"total_transaction_cost": cost}}


def _marks(series):
    return lambda desk_dir=None: {"series": {k: list(v) for k, v in series.items()}}


def _ops(*, holdings, starting=600.0, book_id="alpha_paper_book_1"):
    return {"operational_book": {"book_id": book_id, "starting_capital": starting,
                                 "holdings_detail": holdings},
            "canonical_state": {"holdings_detail": holdings}}


def _holds_2():
    return [
        {"ticker": "AAA", "quantity": 10, "sector": "Tech", "average_cost": 49.0,
         "current_weight": 0.83, "cost_basis": 490.0},
        {"ticker": "BBB", "quantity": 5, "sector": "Energy", "average_cost": 21.0,
         "current_weight": 0.17, "cost_basis": 105.0},
    ]


def _attr(**kw):
    return fe.build_daily_attribution(
        perf_loader=_perf(kw.pop("rows", _PERF_ROWS), kw.pop("cost", 1.0)),
        marks_loader=_marks(kw.pop("series", _SERIES)),
        ops=kw.pop("ops", _ops(holdings=_holds_2())), **kw)


def _lin_world(n, base=1000.0, step=1.0, spy_step=0.1):
    """n perf rows + a single-ticker mark series matching the nav path (qty 1)."""
    rows, series = [], {"AAA": []}
    for i in range(n):
        nav = base + step * i
        d = "2026-06-%02d" % (i + 1)
        rows.append({"date": d, "nav": nav, "cumulative_return_pct": (nav / base - 1) * 100.0,
                     "benchmark_cumulative_return_pct": spy_step * i, "drawdown_pct": 0.0,
                     "transaction_cost": 0.0})
        series["AAA"].append((d, nav))
    ops = _ops(holdings=[{"ticker": "AAA", "quantity": 1, "sector": "Tech",
                          "average_cost": base, "current_weight": 1.0}], starting=base)
    return rows, series, ops


# =========================================================================== #
# A. CANONICAL ATTRIBUTION
# =========================================================================== #
class TestAttribution:
    def test_baseline_first_mark_has_no_prior(self):
        a = _attr(rows=_PERF_ROWS[:1])
        assert a["available"] is False
        assert a["status"] == fe.ATTRIB_NO_PRIOR

    def test_second_close_has_valid_daily_pnl(self):
        a = _attr()
        assert a["available"] is True and a["status"] == fe.ATTRIB_READY
        assert a["market_date"] == "2026-07-22" and a["prior_market_date"] == "2026-07-21"
        assert a["portfolio"]["daily_pnl"] == pytest.approx(-2.5, abs=1e-9)

    def test_holding_contributions_reconcile_to_daily_pnl(self):
        a = _attr()
        assert a["reconciliation"]["position_contribution_sum"] == pytest.approx(-2.5, abs=1e-6)
        assert a["reconciliation"]["market_movement"] == pytest.approx(-2.5, abs=1e-6)
        assert a["reconciliation"]["residual"] == pytest.approx(0.0, abs=1e-6)
        assert a["reconciliation"]["reconciles"] is True
        byt = {h["ticker"]: h for h in a["holdings"]}
        assert byt["AAA"]["pnl_contribution"] == pytest.approx(-10.0, abs=1e-6)
        assert byt["BBB"]["pnl_contribution"] == pytest.approx(7.5, abs=1e-6)
        assert byt["AAA"]["prior_market_value"] == pytest.approx(520.0, abs=1e-6)  # 10*52
        assert byt["AAA"]["cumulative_unrealized_pnl"] == pytest.approx(20.0, abs=1e-6)  # 10*(51-49)

    def test_sector_contributions_sum_to_position_contributions(self):
        a = _attr()
        sec = sum(r["pnl_contribution"] for r in a["sectors"]
                  if r["pnl_contribution"] is not None)
        assert sec == pytest.approx(a["reconciliation"]["position_contribution_sum"], abs=1e-6)
        names = {r["sector"] for r in a["sectors"]}
        assert names == {"Tech", "Energy"}

    def test_spy_daily_and_cumulative_return(self):
        a = _attr()["portfolio"]
        # SPY daily from cumulative 1.0% -> 0.5%
        assert a["spy_daily_return_pct"] == pytest.approx(-0.4950, abs=1e-3)
        assert a["spy_cumulative_return_pct"] == pytest.approx(0.5, abs=1e-6)

    def test_daily_and_cumulative_excess_return(self):
        a = _attr()["portfolio"]
        # daily excess = daily_ret(-0.4065) - spy_daily(-0.4950) ~ +0.088
        assert a["daily_excess_return_pct"] == pytest.approx(0.088, abs=1e-2)
        # cumulative excess = 2.0833 - 0.5
        assert a["cumulative_excess_return_pct"] == pytest.approx(1.5833, abs=1e-3)

    def test_drawdown_and_rolling_peak(self):
        a = _attr()["portfolio"]
        assert a["drawdown_pct"] == pytest.approx(-0.4065, abs=1e-6)
        assert a["rolling_peak_nav"] == pytest.approx(615.0, abs=1e-6)  # peak across 600/615/612.5

    def test_date_alignment_selects_requested_close(self):
        a = _attr(market_date="2026-07-21")
        assert a["market_date"] == "2026-07-21" and a["prior_market_date"] == "2026-07-20"
        assert a["portfolio"]["daily_pnl"] == pytest.approx(15.0, abs=1e-6)   # 600->615
        assert a["reconciliation"]["reconciles"] is True

    def test_unknown_market_date(self):
        a = _attr(market_date="2020-01-01")
        assert a["available"] is False and a["status"] == fe.ATTRIB_DATE_NOT_FOUND

    def test_incomplete_ticker_coverage(self):
        holds = _holds_2() + [{"ticker": "CCC", "quantity": 3, "sector": "Health",
                               "average_cost": 10.0, "current_weight": 0.0}]
        a = _attr(ops=_ops(holdings=holds))          # CCC has no marks
        assert a["available"] is True
        assert a["status"] == fe.ATTRIB_COVERAGE_INCOMPLETE
        assert a["coverage"] == {"priced": 2, "total": 3, "missing_tickers": ["CCC"],
                                 "complete": False}
        # reconciliation still holds on the priced names (CCC contributes nothing)
        assert a["reconciliation"]["reconciles"] is True

    def test_missing_benchmark_coverage(self):
        rows = [dict(r, benchmark_cumulative_return_pct=None) for r in _PERF_ROWS]
        a = _attr(rows=rows)["portfolio"]
        assert a["spy_daily_return_pct"] is None
        assert a["daily_excess_return_pct"] is None
        # portfolio P&L is still computed from NAV
        assert a["daily_pnl"] == pytest.approx(-2.5, abs=1e-9)

    def test_attribution_history_reconciles_each_row(self):
        h = fe.build_attribution_history(perf_loader=_perf(_PERF_ROWS),
                                         marks_loader=_marks(_SERIES),
                                         ops=_ops(holdings=_holds_2()))
        assert h["count"] == 2
        assert all(r["reconciles"] for r in h["rows"])
        # newest first
        assert h["rows"][0]["market_date"] == "2026-07-22"


# =========================================================================== #
# B. DETERMINISTIC "WHY P&L MOVED"
# =========================================================================== #
class TestWhyPnlMoved:
    def test_narrative_is_traceable_and_deterministic(self):
        a = _attr()
        w1 = fe.build_why_pnl_moved(a, perf_summary={"total_transaction_cost": 1.0})
        w2 = fe.build_why_pnl_moved(a, perf_summary={"total_transaction_cost": 1.0})
        assert w1["narrative"] == w2["narrative"]           # deterministic
        assert w1["generation"] == "DETERMINISTIC_NO_LLM"
        assert w1["outperformed_spy"] is True
        assert "OUTPERFORMED" in w1["narrative"]
        assert "BBB" in w1["narrative"] and "AAA" in w1["narrative"]
        assert "Energy" in w1["narrative"] and "Tech" in w1["narrative"]

    def test_trigger_flag_from_decision_row(self):
        a = _attr()
        w = fe.build_why_pnl_moved(a, decision_row={"decision": "REBALANCE_PROPOSAL_READY",
                                                    "proposed_change_count": 2})
        assert w["daily_action_trigger_fired"] is True
        w2 = fe.build_why_pnl_moved(a, decision_row={"decision": "HOLD_CURRENT_PORTFOLIO",
                                                     "proposed_change_count": 0})
        assert w2["daily_action_trigger_fired"] is False

    def test_execution_cost_materiality(self):
        a = _attr()   # cumulative_pnl = 12.5
        big = fe.build_why_pnl_moved(a, perf_summary={"total_transaction_cost": 100.0})
        assert big["execution_cost_material"] is True
        small = fe.build_why_pnl_moved(a, perf_summary={"total_transaction_cost": 1.0})
        assert small["execution_cost_material"] is False

    def test_unavailable_when_no_attribution(self):
        w = fe.build_why_pnl_moved(_attr(rows=_PERF_ROWS[:1]))
        assert w["available"] is False and w["statements"] == []


# =========================================================================== #
# D. ROLLING EVIDENCE
# =========================================================================== #
class TestRolling:
    def _roll(self, n):
        rows, series, ops = _lin_world(n)
        return fe.build_rolling_evidence(perf_loader=_perf(rows), marks_loader=_marks(series),
                                         ops=ops)

    def test_rolling_5_unavailable_before_five_observations(self):
        r = self._roll(5)                     # 4 daily returns
        w5 = next(w for w in r["windows"] if w["window_returns"] == 5)
        assert w5["available"] is False
        assert w5["observations_available"] == 4

    def test_rolling_5_available_at_five_observations(self):
        r = self._roll(6)                     # 5 daily returns
        w5 = next(w for w in r["windows"] if w["window_returns"] == 5)
        assert w5["available"] is True and w5["n_daily_returns"] == 5

    def test_rolling_20_unavailable_before_twenty_observations(self):
        r = self._roll(6)
        w20 = next(w for w in r["windows"] if w["window_returns"] == 20)
        assert w20["available"] is False

    def test_short_sample_vol_carries_warning(self):
        r = self._roll(6)
        w5 = next(w for w in r["windows"] if w["window_returns"] == 5)
        assert w5["annualized_volatility_warning"] is not None  # 5 < 20 obs
        assert r["sample_status"] == fe.INSUFFICIENT_FORWARD_SAMPLE

    def test_since_inception_available_with_two_marks(self):
        rows, series, ops = _lin_world(3)
        r = fe.build_rolling_evidence(perf_loader=_perf(rows), marks_loader=_marks(series), ops=ops)
        assert r["since_inception"]["available"] is True
        assert r["since_inception"]["n_daily_returns"] == 2

    def test_no_forward_marks_status(self):
        r = fe.build_rolling_evidence(perf_loader=_perf([]), marks_loader=_marks({}),
                                      ops=_ops(holdings=[]))
        assert r["status"] == "NO_FORWARD_MARKS"


# =========================================================================== #
# C. ACTIVE vs SHADOW — evidence-class separation
# =========================================================================== #
def _fake_tournament():
    def _bv(name, cum, exc):
        return {"signal": "composite_sn", "n_marks": 40, "start_date": "2026-05-22",
                "end_date": "2026-07-22", "cumulative_return_pct": cum,
                "excess_return_vs_spy_pct_points": exc, "max_drawdown_pct": -3.0,
                "daily_volatility_pct_points": 0.8, "positive_day_rate_pct": 55.0,
                "days_outperforming_spy_pct": 52.0, "coverage_pct": 100.0,
                "contributor_concentration_top5_pct": 30.0}
    return lambda: {"status": "MONITORING", "book_summaries": {
        "champion_top25": _bv("champion_top25", 2.0, 0.5),
        "champion_top50": _bv("champion_top50", 1.8, 0.4),
        "challenger_top25": _bv("challenger_top25", 2.2, 0.7),
        "challenger_top50": _bv("challenger_top50", 1.9, 0.5)}}


def _fake_multi_history():
    def _bk(cadence):
        return {"cadence": cadence, "metrics": {
            "n_periods": 30, "net_cumulative_return": 0.35, "annualized_net_return": 0.12,
            "annualized_vol": 0.18, "sharpe": 0.9, "max_drawdown": -0.22, "hit_rate": 0.6,
            "mean_turnover": 0.4, "first_month": "2019-01", "last_month": "2026-06",
            "sufficient_history": True}}
    return lambda: {"status": "MHZ_HISTORY_READY", "books": {
        "composite_sn_top25": _bk("quarterly"), "composite_sn_top50": _bk("quarterly"),
        "mom_6_1_top25": _bk("monthly"), "mom_6_1_top50": _bk("monthly"),
        "fundamental_momentum_50_50_top25": _bk("monthly"),
        "fundamental_momentum_50_50_top50": _bk("monthly")}}


class TestActiveVsShadow:
    def _shadow(self, **kw):
        return fe.build_active_vs_shadow(
            perf_loader=_perf(_PERF_ROWS), marks_loader=_marks(_SERIES),
            ops=_ops(holdings=_holds_2()),
            tournament_loader=kw.get("t", _fake_tournament()),
            multi_history_loader=kw.get("m", _fake_multi_history()))

    def test_active_is_forward_operational(self):
        s = self._shadow()
        assert s["active_book"]["evidence_type"] == fe.FORWARD_OPERATIONAL
        assert s["active_book"]["book_id"] == ob.OPERATIONAL_BOOK_ID

    def test_all_shadow_are_historical_reconstruction(self):
        s = self._shadow()
        assert len(s["shadow_books"]) == 10   # 4 tournament + 6 multi-horizon
        assert all(b["evidence_type"] == fe.HISTORICAL_RECONSTRUCTION for b in s["shadow_books"])
        names = {b["book"] for b in s["shadow_books"]}
        assert "composite_sn_top25" in names and "mom_6_1_top25" in names
        assert "fundamental_momentum_50_50_top25" in names
        assert "champion_top25" in names

    def test_forward_and_historical_are_separated_not_mixed(self):
        s = self._shadow()
        assert s["forward_operational_overlap"]["status"] == fe.INSUFFICIENT_FORWARD_SAMPLE
        assert s["forward_operational_overlap"]["common_forward_dates"] == 0
        assert s["research_only"] is True
        assert s["active_book_unchanged"] is True
        assert s["operational_recommendation"] == "NO_OPERATIONAL_CHANGE"
        # none of the shadow rows claim date-comparability to forward marks
        assert all(b.get("date_comparable_to_forward") is False for b in s["shadow_books"])

    def test_degrades_when_shadow_sources_raise(self):
        def _boom():
            raise RuntimeError("artifact missing")
        s = self._shadow(t=_boom, m=_boom)
        assert s["status"] == "SHADOW_COMPARISON_READY"      # never crashes
        assert s["shadow_books"] == [] and s["warnings"]
        assert s["active_book"]["evidence_type"] == fe.FORWARD_OPERATIONAL


# =========================================================================== #
# Aggregator + empty states + safety
# =========================================================================== #
class TestAggregatorAndSafety:
    def _load(self, tmp_path, **kw):
        return fe.load_forward_evidence(
            desk_dir=str(tmp_path),
            perf_loader=_perf(kw.pop("rows", _PERF_ROWS)),
            marks_loader=_marks(kw.pop("series", _SERIES)),
            ops=kw.pop("ops", _ops(holdings=_holds_2())),
            tournament_loader=_fake_tournament(), multi_history_loader=_fake_multi_history())

    def test_aggregator_todays_review(self, tmp_path):
        out = self._load(tmp_path)
        tr = out["todays_review"]
        assert tr["available"] is True
        assert tr["daily_pnl"] == pytest.approx(-2.5, abs=1e-9)
        assert tr["outperformed_spy"] is True
        assert tr["top_positive"]["ticker"] == "BBB"
        assert tr["top_negative"]["ticker"] == "AAA"

    def test_safety_block_no_mutation_flags(self, tmp_path):
        out = self._load(tmp_path)
        assert out["read_only"] is True and out["performed_write"] is False
        assert out["creates_orders"] is False and out["mutates_holdings"] is False
        assert out["mutates_cash"] is False and out["model_weights_changed"] is False
        assert out["automation_enabled"] is False and out["broker_enabled"] is False

    def test_empty_state_is_controlled(self, tmp_path):
        out = self._load(tmp_path, rows=[], series={}, ops=_ops(holdings=[]))
        assert out["status"] == "FORWARD_EVIDENCE_READY"
        assert out["todays_review"]["available"] is False
        assert out["attribution"]["available"] is False
        assert out["rolling_evidence"]["status"] == "NO_FORWARD_MARKS"
        # active book still present, shadow still separated
        assert out["active_vs_shadow"]["active_book"]["evidence_type"] == fe.FORWARD_OPERATIONAL


# =========================================================================== #
# Integration — REAL append-only desk ledgers (reconciliation + no mutation)
# =========================================================================== #
class TestRealDeskIntegration:
    def _build(self):
        _filled_world()   # fills at the first close after approval; performance rows begin
        _refresh("2026-07-22", _marks_table(_D0 + ["2026-07-20", "2026-07-21", "2026-07-22"]))

    def test_real_attribution_reconciles(self, env27b1):
        self._build()
        ops = ob.load_operational_book(today="2026-07-22")
        a = fe.build_daily_attribution(desk_dir=desk._desk_dir(None), ops=ops)
        if not a["available"]:
            pytest.skip("fewer than two real operational marks in this harness build")
        assert a["reconciliation"]["reconciles"] is True
        # sector contributions sum to position contributions
        sec = sum(r["pnl_contribution"] for r in a["sectors"]
                  if r["pnl_contribution"] is not None)
        assert sec == pytest.approx(a["reconciliation"]["position_contribution_sum"], abs=0.05)

    def test_forward_evidence_mutates_nothing(self, env27b1):
        self._build()
        before_perf = desk.load_performance()["n_rows"]
        before_orders = desk.load_orders()["counts_by_status"]
        before_fills = desk.load_fills()["n_fills"]
        out = fe.load_forward_evidence(
            today="2026-07-22", tournament_loader=_fake_tournament(),
            multi_history_loader=_fake_multi_history())
        assert out["performed_write"] is False
        assert desk.load_performance()["n_rows"] == before_perf
        assert desk.load_orders()["counts_by_status"] == before_orders
        assert desk.load_fills()["n_fills"] == before_fills


# =========================================================================== #
# API routes (read-only) + operator UI static assertions
# =========================================================================== #
class TestApiRoutes:
    def test_forward_route_shape_and_safety(self, client, env):
        r = client.get("/v1/evidence/forward", headers=_AUTH)
        assert r.status_code == 200
        d = r.json()
        assert d["status"] == "FORWARD_EVIDENCE_READY"
        assert d["read_only"] is True and d["performed_write"] is False
        assert d["active_vs_shadow"]["active_book"]["evidence_type"] == fe.FORWARD_OPERATIONAL
        for b in d["active_vs_shadow"]["shadow_books"]:
            assert b["evidence_type"] == fe.HISTORICAL_RECONSTRUCTION

    def test_granular_routes_200(self, client, env):
        for path in ("/v1/evidence/daily-attribution", "/v1/evidence/attribution-history",
                     "/v1/evidence/holding-contributions", "/v1/evidence/sector-contributions",
                     "/v1/evidence/rolling", "/v1/evidence/shadow-comparison"):
            resp = client.get(path, headers=_AUTH)
            assert resp.status_code == 200, path
            assert resp.json().get("read_only") is True, path


class TestUiStatic:
    def test_ui_has_forward_evidence_sections(self):
        html = _UI.read_text(encoding="utf-8")
        assert "fe-today" in html                 # Portfolio Today's Review
        assert "fe-why" in html                   # collapsible Why P&L Moved
        assert "fe-research" in html              # Research & Audit Forward Evidence
        assert "Why P&amp;L Moved" in html or "Why P&L Moved" in html
        assert "HISTORICAL RECONSTRUCTION" in html
        assert "/v1/evidence/forward" in html

    def test_ui_has_no_dialogs(self):
        html = _UI.read_text(encoding="utf-8")
        # forward-evidence code must not introduce native dialogs (whole file guard)
        assert "alert(" not in html
        assert "confirm(" not in html
        assert "prompt(" not in html
