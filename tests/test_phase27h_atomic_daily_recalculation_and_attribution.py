"""
tests/test_phase27h_atomic_daily_recalculation_and_attribution.py

Phase 27H — ONE ATOMIC daily close: the close now advances BOTH owned pipelines
(the desk marks/valuation AND the model-input pipeline the gate/model target read)
to the same completed session, then attributes daily P&L and monitors forward
performance. Fully offline:

  * fast contract tests inject fake operational-book / gate / desk-refresh / engine /
    alpha-refresh seams so the atomic date reconciliation, the model-recalculation
    provenance, the labeled date set, the honest month-boundary / provider-blocked
    handling, the forward-monitor sample floor and the safety block are deterministic;
  * a REAL owned-input integration test drives the actual ``alpha_target.run_refresh``
    against fixture inputs + an offline downloader and proves ``build_current`` advances
    its ``market_as_of_date`` (momentum scores / formulas / weights unchanged);
  * real-desk P&L / attribution / idempotency tests reuse the Phase 27A/27B.1/27B.5
    FILLED-book offline harness so the beginning->ending NAV reconciliation, the
    position/sector attribution reconciliation, one-row idempotency and "no orders"
    are proven against the REAL append-only ledgers.

Every write happens ONLY inside a tmp desk dir / fixture inputs of this harness. The
close never creates a paper order, never touches a broker, and changes no model /
champion / weight / sleeve.
"""
from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path

import pytest

from paper_trader.api import alpha_target as at
from paper_trader.api import daily_close as dc
from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import portfolio_manager as pm

from tests.test_phase27a_paper_operations import (  # reuse the offline harness
    _AUTH, _D0, _TICKS, _bars, _dl, _marks_table, client, env,  # noqa: F401
)
from tests.test_phase27b1_operational_surface_cutover import env27b1  # noqa: F401
from tests.test_phase27b5_operator_flow import _filled_world

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"
_BOOK = "alpha_paper_book_1"


# --------------------------------------------------------------------------- #
# Fast-contract fake seams
# --------------------------------------------------------------------------- #
def _fake_ops(*, pending=0, fills=25, initialized=True, nav=99570.46, cash=1500.0,
              holdings_count=25, valuation_date="2026-07-23", starting=100000.0):
    hd = [{"ticker": t, "quantity": 100 + i, "sector": ("Tech" if i % 2 else "Health"),
           "current_weight": 0.04} for i, t in enumerate(_TICKS)]
    cs = {
        "pending_order_count": pending, "fill_count": fills, "lifecycle_stage": "FILLED",
        "nav": nav, "cash": cash, "holdings_count": holdings_count,
        "valuation_date": valuation_date, "desk_mark_date": valuation_date,
        "next_review_date": "2026-08-01", "review_due": False, "review_cadence": "MONTHLY",
        "holdings_detail": hd,
    }
    ob_book = {
        "book_id": _BOOK, "book_label": "Alpha Paper Book #1",
        "initialized": initialized, "starting_capital": starting, "initial_capital": starting,
        "holdings_count": holdings_count, "pending_order_count": pending, "fill_count": fills,
    }
    return {"canonical_state": cs, "operational_book": ob_book}


def _fake_cur(market_date="2026-07-23", fund="2026-03-31", n=200):
    """A minimal engine current cross-section: only the fields the close reads."""
    return {"status": eng.STATUS_READY, "market_as_of_date": market_date,
            "fundamental_as_of_date": fund, "fundamental_month": fund[:7],
            "combined": {"common_universe": ["U%03d" % i for i in range(n)],
                         "combined": {}},
            "books": {"books": {}}}


def _gate_at(market_date="2026-07-23", outcome="NO_ACTION_TODAY", pcount=0, adds=None,
             rems=None, triggered=0):
    return {
        "outcome": outcome, "outcome_label": outcome.replace("_", " "),
        "target_state": ("CURRENT_ALIGNED" if outcome == "NO_ACTION_TODAY" else "PROPOSAL_READY"),
        "target_state_label": ("CURRENT — ALIGNED WITH HOLDINGS" if outcome == "NO_ACTION_TODAY"
                               else "PROPOSAL READY — MANUAL REVIEW REQUIRED"),
        "checks_performed": [
            {"code": "DATA_FRESHNESS", "status": "PASS", "label": "Data freshness",
             "summary": "Owned market data and model scores are current (%s)." % market_date},
            {"code": "TARGET_ALIGNMENT", "status": "PASS", "label": "Target vs actual alignment",
             "summary": "Actual holdings match the current model target."},
        ],
        "checks_summary": {"total": 13, "triggered": triggered, "not_available": 0,
                           "line": "13 checks completed · %d triggered · 0 unavailable" % triggered},
        "proposed_additions": adds or [], "proposed_removals": rems or [],
        "proposed_resizes": [], "blocked_changes": [], "proposed_change_count": pcount,
        "estimated_turnover": 0.0, "estimated_cost": 0.0,
        "trigger_categories": ([] if not pcount else ["MATERIAL_TARGET_MEMBERSHIP_CHANGE"]),
        "trigger_reasons": ([] if not pcount else ["NVDA entered the combined target."]),
        "target_actual_match": (pcount == 0),
        "latest_completed_market_date": market_date,
        "operational_dates": {"latest_completed_market_date": market_date,
                              "desk_mark_date": market_date},
        "data_ready": True, "warnings": [],
    }


def _ok_refresh(closed="2026-07-23", appended=1, filled=0):
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


def _alpha_ok(resulting="2026-07-23"):
    def _fn(**kw):
        return {"status": at.R_REFRESHED, "performed_write": True,
                "resulting_alpha_market_date": kw.get("completed_through") or resulting,
                "momentum_scores_changed": False, "model_formulas_changed": False,
                "model_weights_changed": False}
    return _fn


def _alpha_month_boundary():
    def _fn(**kw):
        return {"status": at.R_MONTH_BOUNDARY, "performed_write": False,
                "momentum_scores_changed": False, "model_formulas_changed": False,
                "model_weights_changed": False}
    return _fn


def _alpha_blocked():
    def _fn(**kw):
        return {"status": at.R_PROVIDER_BLOCKED, "performed_write": False,
                "momentum_scores_changed": False, "model_formulas_changed": False,
                "model_weights_changed": False}
    return _fn


def _seed_prior(desk_dir, *, market_date="2026-07-22", decision=None):
    """Seed one prior daily-close journal row so the next run is a HOLD, not baseline."""
    desk._append_ledger(desk._desk_dir(desk_dir), dc.DAILY_CLOSE_JOURNAL_FILE, [{
        "event": dc.DAILY_CLOSE_EVENT, "book_id": _BOOK, "market_date": market_date,
        "decision": decision or dc.DECISION_HOLD,
        "close_status": dc.CLOSE_COMPLETE_HOLD, "is_baseline": False}])


def _next_day(closed: str) -> str:
    """The injected-clock convention resolves the expected close as ``today - 1``, so
    the caller's ``today`` is the day after the completed session it is closing."""
    return (date.fromisoformat(closed) + timedelta(days=1)).isoformat()


def _run(desk_dir, *, today=None, closed="2026-07-23", model_date="2026-07-23",
         fund="2026-03-31", gate=None, alpha=None, refresh=None, ops=None, seed_prior=True):
    if seed_prior:
        _seed_prior(desk_dir, market_date=(date.fromisoformat(closed)
                                           - timedelta(days=1)).isoformat())
    return dc.run_daily_close(
        confirm=dc.EXECUTE_CONFIRMATION, today=today or _next_day(closed), desk_dir=desk_dir,
        operational_loader=(lambda t: ops if ops is not None else _fake_ops()),
        gate_loader=(gate if gate is not None else (lambda *a, **k: _gate_at(closed))),
        engine_loader=(lambda: _fake_cur(model_date, fund)),
        refresh_fn=(refresh or _ok_refresh(closed)),
        alpha_refresh_fn=(alpha or _alpha_ok(closed)))


def _load(desk_dir, *, today="2026-07-23", gate=None, model_date="2026-07-23", fund="2026-03-31"):
    return dc.load_daily_close(
        today=today, desk_dir=desk_dir, operational_loader=lambda t: _fake_ops(),
        gate_loader=(gate if gate is not None else (lambda *a, **k: _gate_at("2026-07-23"))),
        engine_loader=(lambda: _fake_cur(model_date, fund)),
        provider_probe=(lambda **kw: {"provider_latest_date": "2026-07-23",
                                      "priced": ["SPY"], "source": "T", "queried": True}))


# =========================================================================== #
# A. ATOMIC DATE CONSISTENCY (the core defect fix)
# =========================================================================== #
class TestAtomicDates:
    def test_successful_close_valuation_date_is_closed_date(self, tmp_path):
        out = _run(tmp_path / "d", closed="2026-07-23")
        assert out["close_status"] == dc.CLOSE_COMPLETE_HOLD
        assert out["close_dates"]["operational_valuation_date"] == "2026-07-23"

    def test_desk_mark_date_becomes_closed_date(self, tmp_path):
        out = _run(tmp_path / "d", closed="2026-07-23")
        assert out["close_dates"]["desk_mark_date"] == "2026-07-23"
        assert out["close_dates"]["price_data_through"] == "2026-07-23"

    def test_model_target_calculation_date_becomes_closed_date(self, tmp_path):
        out = _run(tmp_path / "d", closed="2026-07-23", model_date="2026-07-23")
        assert out["close_dates"]["target_calculation_date"] == "2026-07-23"
        assert out["model_recalculation"]["model_calc_date"] == "2026-07-23"

    def test_price_and_target_aligned_after_atomic_close(self, tmp_path):
        out = _run(tmp_path / "d", closed="2026-07-23", model_date="2026-07-23")
        assert out["close_dates"]["price_and_target_aligned"] is True
        assert out["model_recalculation"]["recalculation_complete"] is True
        assert out["model_recalculation_complete"] is True

    def test_fundamental_retains_its_own_separate_older_date(self, tmp_path):
        out = _run(tmp_path / "d", closed="2026-07-23", model_date="2026-07-23", fund="2026-03-31")
        cd = out["close_dates"]
        assert cd["fundamental_data_as_of"] == "2026-03-31"          # separate + older
        assert cd["fundamental_data_as_of"] != cd["price_data_through"]
        assert "quarterly" in cd["fundamental_note"].lower()

    def test_alpha_refresh_targets_the_exact_closed_session(self, tmp_path):
        seen = {}

        def _spy(**kw):
            seen["completed_through"] = kw.get("completed_through")
            return _alpha_ok()(** kw)

        _run(tmp_path / "d", closed="2026-07-23", alpha=_spy)
        assert seen["completed_through"] == "2026-07-23"

    def test_no_post_close_separate_refresh_instruction(self, tmp_path):
        out = _run(tmp_path / "d", closed="2026-07-23")
        # the primary action after a HOLD is to VIEW the review, not run another refresh
        assert out["primary_action"]["kind"] == "VIEW_REVIEW"
        assert out["primary_action"]["refreshes_status"] is False
        assert "another" not in (out["explanation"] or "").lower()
        assert "separate" not in (out["explanation"] or "").lower()


# =========================================================================== #
# B. DAILY MODEL RECALCULATION provenance + honest degraded paths
# =========================================================================== #
class TestModelRecalculation:
    def test_recalc_block_reports_no_model_parameter_change(self, tmp_path):
        out = _run(tmp_path / "d")
        mr = out["model_recalculation"]
        assert mr["momentum_scores_changed"] is False
        assert mr["model_formulas_changed"] is False
        assert mr["model_weights_changed"] is False
        assert mr["model_input_refresh_ran"] is True

    def test_month_boundary_is_honest_not_a_silent_stale_hold(self, tmp_path):
        # model inputs cannot advance across a month boundary -> the close still marks
        # the book but the model date stays behind and recalculation_complete is False.
        out = _run(tmp_path / "d", closed="2026-08-03", model_date="2026-07-31",
                   gate=(lambda *a, **k: _gate_at("2026-07-31")), alpha=_alpha_month_boundary())
        mr = out["model_recalculation"]
        assert mr["recalculation_complete"] is False
        assert mr["model_input_refresh_status"] == at.R_MONTH_BOUNDARY
        assert out["close_dates"]["price_and_target_aligned"] is False
        assert "pending" in (out["explanation"] or "").lower()

    def test_provider_blocked_model_refresh_is_reported_not_hidden(self, tmp_path):
        out = _run(tmp_path / "d", closed="2026-07-23", model_date="2026-07-22",
                   gate=(lambda *a, **k: _gate_at("2026-07-22")), alpha=_alpha_blocked())
        assert out["model_recalculation"]["recalculation_complete"] is False
        assert any("did not fully advance" in w.lower() for w in out["warnings"])


# =========================================================================== #
# C. ACTION-GATE DECISION (hold reasons / proposal routing)
# =========================================================================== #
class TestActionGate:
    def test_hold_carries_exact_evaluated_reasons(self, tmp_path):
        out = _run(tmp_path / "d")
        assert out["close_status"] == dc.CLOSE_COMPLETE_HOLD
        assert out["decision"] == dc.DECISION_HOLD
        assert out["checks_summary"]["total"] == 13
        assert any(c["code"] == "TARGET_ALIGNMENT" for c in out["checks_performed"])

    def test_material_trigger_routes_to_manual_review_proposal(self, tmp_path):
        gate = lambda *a, **k: _gate_at("2026-07-23", outcome="PROPOSAL_READY", pcount=2,
                                        adds=[{"ticker": "NVDA", "target_weight": 0.04}],
                                        rems=[{"ticker": "XYZ", "current_weight": 0.05}],
                                        triggered=2)
        out = _run(tmp_path / "d", gate=gate)
        assert out["close_status"] == dc.REBALANCE_PROPOSAL_READY
        assert out["proposal"]["manual_review_required"] is True
        assert out["proposal"]["creates_orders"] is False
        assert out["proposal"]["proposed_change_count"] == 2


# =========================================================================== #
# D. P&L ATTRIBUTION (deterministic, from stored marks) — real desk
# =========================================================================== #
class TestAttributionRealDesk:
    def _close(self, env, *, today, marks_through, model_date="2026-07-21"):
        table = _marks_table(_D0 + marks_through)
        return dc.run_daily_close(
            confirm=dc.EXECUTE_CONFIRMATION, today=today, desk_dir=env["desk"],
            downloader=_dl(table),
            gate_loader=(lambda *a, **k: _gate_at(marks_through[-1])),
            engine_loader=(lambda: _fake_cur(model_date)),
            alpha_refresh_fn=_alpha_ok(marks_through[-1]))

    def test_attribution_unavailable_on_baseline_then_available_second(self, env27b1):
        _filled_world()
        first = self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"],
                            model_date="2026-07-20")
        assert first["attribution"]["available"] is False       # baseline: no prior day
        second = self._close(env27b1, today="2026-07-22",
                             marks_through=["2026-07-20", "2026-07-21"], model_date="2026-07-21")
        assert second["attribution"]["available"] is True

    def test_position_attribution_reconciles_to_market_movement(self, env27b1):
        _filled_world()
        self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"],
                    model_date="2026-07-20")
        out = self._close(env27b1, today="2026-07-22",
                          marks_through=["2026-07-20", "2026-07-21"], model_date="2026-07-21")
        a = out["attribution"]
        assert a["available"] is True
        # ending - beginning == market movement == today's daily P&L
        assert a["ending_nav"] - a["beginning_nav"] == pytest.approx(a["market_movement_pnl"], abs=0.02)
        # sum of per-position contributions reconciles to the NAV move
        assert a["position_contribution_sum"] == pytest.approx(a["market_movement_pnl"], abs=0.05)
        assert a["reconciles"] is True

    def test_sector_attribution_sums_to_position_attribution(self, env27b1):
        _filled_world()
        self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"],
                    model_date="2026-07-20")
        out = self._close(env27b1, today="2026-07-22",
                          marks_through=["2026-07-20", "2026-07-21"], model_date="2026-07-21")
        a = out["attribution"]
        sec_sum = sum(r["pnl_contribution"] for r in a["sector_contributions"]
                      if r["pnl_contribution"] is not None)
        assert sec_sum == pytest.approx(a["position_contribution_sum"], abs=0.05)

    def test_initial_execution_cost_not_charged_again_on_daily_mark(self, env27b1):
        _filled_world()
        out = self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"],
                          model_date="2026-07-20")
        assert out["attribution"]["available"] is False   # first mark = baseline
        # cumulative P&L on the baseline is exactly nav - starting_capital (one embedded cost)
        pnl = out["pnl"]
        assert pnl["cumulative_pnl"] == pytest.approx(pnl["nav"] - pnl["starting_capital"], abs=0.02)

    def test_second_day_attribution_cost_field_is_zero(self, env27b1):
        _filled_world()
        self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"],
                    model_date="2026-07-20")
        out = self._close(env27b1, today="2026-07-22",
                          marks_through=["2026-07-20", "2026-07-21"], model_date="2026-07-21")
        assert out["attribution"]["execution_cost_charged_today"] == 0.0

    def test_spy_and_excess_return_present(self, env27b1):
        _filled_world()
        self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"],
                    model_date="2026-07-20")
        out = self._close(env27b1, today="2026-07-22",
                          marks_through=["2026-07-20", "2026-07-21"], model_date="2026-07-21")
        a = out["attribution"]
        assert "spy_return_pct" in a and "excess_return_pct" in a


# =========================================================================== #
# E. FORWARD PERFORMANCE MONITOR (sample floor)
# =========================================================================== #
class TestForwardMonitor:
    def test_insufficient_sample_withholds_ratios(self, tmp_path):
        out = _run(tmp_path / "d")
        fm = out["forward_performance"]
        assert fm["status"] == "INSUFFICIENT_FORWARD_SAMPLE"
        assert fm["sufficient_sample"] is False
        assert fm["sharpe_ratio"] is None and fm["beta_vs_spy"] is None
        assert fm["information_ratio"] is None
        assert fm["insufficient_message"] == "INSUFFICIENT FORWARD SAMPLE — NO MODEL CONCLUSION"

    def test_forward_block_present_on_get(self, tmp_path):
        out = _load(tmp_path / "d")
        assert "forward_performance" in out
        assert out["forward_performance"]["min_ratio_observations"] == dc._FORWARD_MIN_RATIO_OBS


# =========================================================================== #
# F. cumulative P&L / SPY reconciliation (real desk)
# =========================================================================== #
class TestPnlReconciliation:
    def _close(self, env, *, today, marks_through, model_date):
        table = _marks_table(_D0 + marks_through)
        return dc.run_daily_close(
            confirm=dc.EXECUTE_CONFIRMATION, today=today, desk_dir=env["desk"],
            downloader=_dl(table), gate_loader=(lambda *a, **k: _gate_at(marks_through[-1])),
            engine_loader=(lambda: _fake_cur(model_date)),
            alpha_refresh_fn=_alpha_ok(marks_through[-1]))

    def test_cumulative_pnl_reconciles_to_initial_capital(self, env27b1):
        _filled_world()
        out = self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"],
                          model_date="2026-07-20")
        pnl = out["pnl"]
        assert pnl["cumulative_pnl"] == pytest.approx(pnl["nav"] - pnl["starting_capital"], abs=0.02)

    def test_daily_pnl_reconciles_beginning_to_ending_nav(self, env27b1):
        _filled_world()
        first = self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"],
                            model_date="2026-07-20")
        nav1 = first["pnl"]["nav"]
        second = self._close(env27b1, today="2026-07-22",
                             marks_through=["2026-07-20", "2026-07-21"], model_date="2026-07-21")
        assert second["pnl"]["daily_pnl"] == pytest.approx(second["pnl"]["nav"] - nav1, abs=0.02)


# =========================================================================== #
# G. IDEMPOTENCY / PARTIAL-FAILURE (Part H)
# =========================================================================== #
class TestIdempotencyAndFailure:
    def _journal_count(self, desk_dir):
        return len([r for r in desk._read_ledger(desk._desk_dir(desk_dir), dc.DAILY_CLOSE_JOURNAL_FILE)
                    if r.get("event") == dc.DAILY_CLOSE_EVENT and r.get("book_id") == _BOOK])

    def test_duplicate_close_is_idempotent_no_duplicate_rows(self, env27b1):
        _filled_world()
        table = _marks_table(_D0 + ["2026-07-20"])
        kw = dict(confirm=dc.EXECUTE_CONFIRMATION, today="2026-07-21", desk_dir=env27b1["desk"],
                  downloader=_dl(table), gate_loader=(lambda *a, **k: _gate_at("2026-07-20")),
                  engine_loader=(lambda: _fake_cur("2026-07-20")),
                  alpha_refresh_fn=_alpha_ok("2026-07-20"))
        dc.run_daily_close(**kw)
        j1 = self._journal_count(env27b1["desk"])
        perf1 = len(desk.load_performance(env27b1["desk"])["rows"])
        again = dc.run_daily_close(**kw)
        assert again["close_status"] == dc.ALREADY_PROCESSED
        assert self._journal_count(env27b1["desk"]) == j1           # no duplicate decision row
        assert len(desk.load_performance(env27b1["desk"])["rows"]) == perf1  # no duplicate perf row

    def test_already_processed_reports_model_recalculation_block(self, env27b1):
        _filled_world()
        table = _marks_table(_D0 + ["2026-07-20"])
        kw = dict(confirm=dc.EXECUTE_CONFIRMATION, today="2026-07-21", desk_dir=env27b1["desk"],
                  downloader=_dl(table), gate_loader=(lambda *a, **k: _gate_at("2026-07-20")),
                  engine_loader=(lambda: _fake_cur("2026-07-20")),
                  alpha_refresh_fn=_alpha_ok("2026-07-20"))
        dc.run_daily_close(**kw)
        again = dc.run_daily_close(**kw)
        assert again["model_recalculation"] is not None
        assert again["close_dates"] is not None

    def test_partial_failure_blocked_refresh_writes_no_decision_row(self, tmp_path):
        d = tmp_path / "d"
        _seed_prior(d, market_date="2026-07-22")
        out = dc.run_daily_close(
            confirm=dc.EXECUTE_CONFIRMATION, today="2026-07-24", desk_dir=d,
            operational_loader=lambda t: _fake_ops(),
            gate_loader=lambda *a, **k: _gate_at("2026-07-23"),
            engine_loader=lambda: _fake_cur("2026-07-23"),
            refresh_fn=_blocked_refresh(), alpha_refresh_fn=_alpha_ok("2026-07-23"))
        assert out["close_status"] == dc.DATA_BLOCKED
        # only the seeded prior row exists; no new decision row for 2026-07-23
        rows = [r for r in desk._read_ledger(desk._desk_dir(d), dc.DAILY_CLOSE_JOURNAL_FILE)
                if r.get("event") == dc.DAILY_CLOSE_EVENT]
        assert all(r["market_date"] != "2026-07-23" for r in rows)


# =========================================================================== #
# H. REAL owned-input integration — the actual alpha_target refresh advances build_current
# =========================================================================== #
class TestRealModelInputAdvance:
    def test_real_alpha_refresh_advances_build_current_market_date(self, env):
        eng.clear_cache()
        before = eng.build_current()
        assert before["status"] == eng.STATUS_READY
        assert before["market_as_of_date"] == "2026-07-17"
        before_book = before["books"]["books"].get("fundamental_momentum_50_50_top25")
        before_tickers = [c["ticker"] for c in (before_book or {}).get("constituents", [])]

        # >=120 completed daily bars ending 2026-07-20 so the intramonth risk refresh
        # keeps eligible_history=1 (the frozen monthly ranks must survive unchanged).
        end = date(2026, 7, 20)
        dates = [(end - timedelta(days=k)).isoformat() for k in range(160)][::-1]
        dl = lambda sym, start: _bars(dates, 100.0)                     # noqa: E731
        r = at.run_refresh(confirm=at.REFRESH_CONFIRM_TOKEN, downloader=dl,
                           completed_through="2026-07-20")
        assert r["status"] == at.R_REFRESHED, r
        assert r["resulting_alpha_market_date"] == "2026-07-20"
        assert r["model_formulas_changed"] is False
        assert r["model_weights_changed"] is False

        after = eng.build_current()
        assert after["market_as_of_date"] == "2026-07-20"              # advanced atomically
        # fundamental as-of unchanged (its own cadence)
        assert after["fundamental_as_of_date"] == before["fundamental_as_of_date"]
        after_book = after["books"]["books"].get("fundamental_momentum_50_50_top25")
        after_tickers = [c["ticker"] for c in (after_book or {}).get("constituents", [])]
        assert after_tickers == before_tickers                        # frozen monthly ranks


# =========================================================================== #
# I. SAFETY (no orders / no broker / no automation / no model change)
# =========================================================================== #
class TestSafety:
    def test_no_orders_no_broker_no_automation(self, tmp_path):
        out = _run(tmp_path / "d")
        assert out["creates_orders"] is False
        assert out["auto_order_creation"] is False
        assert out["broker_enabled"] is False
        assert out["live_orders_enabled"] is False
        assert out["automation_enabled"] is False

    def test_no_model_parameter_or_champion_or_sleeve_change(self, tmp_path):
        out = _run(tmp_path / "d")
        assert out["model_parameters_changed"] is False
        assert out["champion_replaced"] is False
        assert out["fast_sleeve_active"] is False

    def test_daily_close_creates_no_orders_real_desk(self, env27b1):
        _filled_world()
        before = desk.load_orders(env27b1["desk"])["counts_by_status"]
        table = _marks_table(_D0 + ["2026-07-20"])
        dc.run_daily_close(
            confirm=dc.EXECUTE_CONFIRMATION, today="2026-07-21", desk_dir=env27b1["desk"],
            downloader=_dl(table), gate_loader=(lambda *a, **k: _gate_at("2026-07-20")),
            engine_loader=(lambda: _fake_cur("2026-07-20")), alpha_refresh_fn=_alpha_ok("2026-07-20"))
        after = desk.load_orders(env27b1["desk"])["counts_by_status"]
        assert after["PROPOSED"] == before["PROPOSED"]
        assert after["APPROVED"] == before["APPROVED"]

    def test_get_writes_nothing(self, tmp_path):
        out = _load(tmp_path / "d")
        assert out["performed_write"] is False
        assert out["read_only"] is True


# =========================================================================== #
# J. CROSS-SURFACE CONSISTENCY + API + UI static
# =========================================================================== #
class TestConsistencyApiUi:
    def test_pm_passes_through_27h_blocks(self, monkeypatch):
        monkeypatch.setattr(pm, "_DAILY_CLOSE_LOADER",
                            lambda: dc.load_daily_close(
                                today="2026-07-23",
                                operational_loader=lambda t: _fake_ops(),
                                gate_loader=lambda *a, **k: _gate_at("2026-07-23"),
                                engine_loader=lambda: _fake_cur("2026-07-23"),
                                provider_probe=lambda **kw: {"provider_latest_date": "2026-07-23",
                                                             "priced": ["SPY"], "queried": True,
                                                             "source": "T"}))
        blk = pm._daily_close_block()
        assert blk["available"] is True
        for k in ("close_dates", "model_recalculation", "attribution", "forward_performance"):
            assert k in blk

    def test_pm_no_misalignment_warning_when_dates_align(self):
        _dates, warns = pm._dates_block(_pm_fake_ctx(), operational_dates={
            "latest_completed_market_date": "2026-07-23",
            "desk_mark_date": "2026-07-23", "book_valuation_date": "2026-07-23"})
        assert not any("misalignment" in w.lower() for w in warns)

    def test_pm_misalignment_message_points_to_atomic_daily_close(self):
        _dates, warns = pm._dates_block(_pm_fake_ctx(), operational_dates={
            "latest_completed_market_date": "2026-07-23",
            "desk_mark_date": "2026-07-22", "book_valuation_date": "2026-07-22"})
        w = "\n".join(warns)
        assert "daily close" in w.lower()
        assert "no separate after-market desk refresh is required" in w.lower()

    def test_api_get_carries_27h_blocks(self, client):
        r = client.get("/v1/operations/daily-close", headers=_AUTH)
        assert r.status_code == 200
        body = r.json()
        for k in ("close_dates", "model_recalculation", "attribution", "forward_performance",
                  "model_recalculation_complete"):
            assert k in body, k
        assert body["performed_write"] is False

    def test_ui_static_has_27h_surfaces(self):
        html = _UI.read_text(encoding="utf-8")
        assert "Price data through" in html
        assert "Fundamental data as of" in html
        assert "Target calculation date" in html
        assert "renderDailyClosePerf" in html
        # no separate "run another desk refresh after a successful close" narrative
        assert "confirm(" not in html.replace("confirmation", "")
        assert re.search(r"alert\s*\(", html) is None


# =========================================================================== #
# K. RESEARCH-ONLY calibration study (Part F) — no operational promotion
# =========================================================================== #
class TestCalibrationStudy:
    def test_study_is_research_only_and_makes_no_operational_change(self, env):
        from paper_trader.api import calibration_study as cal
        eng.clear_cache()
        out = cal.load_calibration_study()
        assert out["research_only"] is True
        assert out["operational_promotion"] is False
        assert out["modifies_operational_model"] is False
        assert out["performed_write"] is False
        assert out["model_parameters_changed"] is False
        assert out["forward_evidence_status"] == "INSUFFICIENT_FORWARD_SAMPLE"
        assert out["operational_recommendation"] == "NO_OPERATIONAL_CHANGE"

    def test_study_reports_blend_membership_sensitivity(self, env):
        from paper_trader.api import calibration_study as cal
        eng.clear_cache()
        out = cal.load_calibration_study()
        assert out["status"] == "CALIBRATION_STUDY_READY"
        names = {b["blend"] for b in out["blend_membership_sensitivity"]}
        assert "fund50_mom50" in names and "fund30_mom70" in names
        op = next(b for b in out["blend_membership_sensitivity"] if b["is_operational"])
        # the operational blend overlaps itself completely (identity)
        assert op["overlap_with_operational"] == op["top_count"]
        assert op["one_way_turnover_vs_operational"] == 0.0

    def test_api_route_is_read_only(self, client):
        r = client.get("/v1/research/calibration-study", headers=_AUTH)
        assert r.status_code == 200
        b = r.json()
        assert b["performed_write"] is False
        assert b["operational_recommendation"] == "NO_OPERATIONAL_CHANGE"


def _pm_fake_ctx():
    """Minimal ctx for pm._dates_block (mirrors the 27D operator-date test)."""
    return {
        "cur": {"market_as_of_date": "2026-07-23", "fundamental_as_of_date": "2026-03-31",
                "fundamental_month": "2026-03"},
        "state": {"sleeves": []},
        "valuation": {"as_of_market_date": "2026-07-20"},
        "prior_combined": {"market_as_of_date": "2026-07-23"},
        "ready": True,
    }
