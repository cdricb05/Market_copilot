"""
tests/test_current_alpha_decision_gate.py — Phase 13-J paper book decision gate.

Fully offline: a synthetic Phase 13-I *historical daily mark backfill* artifact is
written to a tmp dir and ``PAPER_TRADER_CURRENT_ALPHA_BACKFILL_DIR`` points the loader
at it. No research runner, no network, no EODHD key, no database. Verifies the
provisional-primary rule, the no-primary fallback, the deterioration / risk-threshold
decisions, the quarterly rebalance-readiness calculations, stale-mark handling, that
Top-25 and Top-50 stay isolated, that the loader is read-only (writes nothing), and
that the payload carries the "not live approval" safety block (no orders / no broker /
no automation / promotes no book to live).
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from paper_trader.api.current_alpha_decision_gate import (
    DEC_CONTINUE,
    DEC_DETERIORATING,
    DEC_INSUFFICIENT,
    DEC_NO_CLEAR,
    DEC_RISK,
    DEC_TOP25,
    DEC_TOP50,
    MIN_FORWARD_OBS,
    READY_APPROACHING,
    READY_DUE,
    READY_EARLY,
    READY_MID,
    READY_OVERDUE,
    ROLE_NONE,
    ROLE_TOP50,
    _dominance,
    _rebalance_readiness,
    load_current_alpha_decision_gate,
)
from paper_trader.api.current_alpha_performance import BACKFILL_DIR_ENV

_SIGNAL = "2026-05-22"


# --------------------------------------------------------------------------- #
# Deterministic Phase 13-I artifact builder
# --------------------------------------------------------------------------- #
def _seq_dates(n: int, start: str = _SIGNAL) -> list[str]:
    d0 = date.fromisoformat(start)
    return [(d0 + timedelta(days=i)).isoformat() for i in range(n)]


def _lin(n: int, lo: float, hi: float) -> list[float]:
    if n <= 1:
        return [round(hi, 4)]
    return [round(lo + (hi - lo) * i / (n - 1), 4) for i in range(n)]


def _analytics(book_id: str, size: int, *, cur_return: float, excess: float,
               max_dd: float, vol: float, conc: float, spy: float = 1.5,
               insufficient: int = 0, cov_warn: int = 0, n_obs: int = 25) -> dict:
    return {
        "book_id": book_id, "book_size": size, "n_observations": n_obs,
        "start_date": _SIGNAL, "end_date": "2026-06-25",
        "current_cumulative_return_pct": cur_return,
        "spy_cumulative_return_pct": spy,
        "current_excess_return_pct_points": excess,
        "max_drawdown_pct": max_dd, "max_drawdown_duration_obs": 3,
        "max_drawdown_recovered": False,
        "daily_change_volatility_pct_points": vol,
        "pct_positive_daily_changes": 57.0, "pct_days_outperforming_spy": 55.0,
        "average_daily_excess_change_pct_points": 0.03,
        "tracking_error_pct_points": 0.9, "information_ratio": None,
        "information_ratio_valid": False,
        "contributor_concentration_top5_pct": conc,
        "n_coverage_warning_dates": cov_warn,
        "n_insufficient_coverage_dates": insufficient,
        "n_daily_change_observations": n_obs - 1,
    }


def _history(book_id: str, size: int, cum: list[float], exc: list[float],
             cov: list[float]) -> dict:
    dates = _seq_dates(len(cum))
    rows = [{
        "mark_date": dates[i], "book_id": book_id, "book_size": size,
        "covered_count": size, "missing_count": 0, "total_count": size,
        "coverage_pct": cov[i], "coverage_status": "FULL_COVERAGE",
        "average_return_pct": cum[i], "cumulative_return_pct": cum[i],
        "excess_return_vs_spy_pct_points": exc[i], "order_action_all": "NO_ORDER",
    } for i in range(len(cum))]
    return {"book_size": size, "n_observations": len(rows), "rows": rows}


def _write_backfill(
    bdir: Path, *,
    decision: str = "BACKFILL_RECONCILED",
    n_obs: int = 25,
    top25_cum=None, top25_exc=None, top25_cov=None,
    top50_cum=None, top50_exc=None, top50_cov=None,
    a25=None, a50=None,
    start: str = _SIGNAL, end: str = "2026-06-25",
) -> None:
    """Write a Phase 13-I artifact. Defaults describe the real-world case where Top50
    dominates on all five evidence criteria and neither book is at risk."""
    bdir.mkdir(parents=True, exist_ok=True)
    b25 = "composite_sn__%s__top25" % _SIGNAL
    b50 = "composite_sn__%s__top50" % _SIGNAL

    if top25_cum is None:
        top25_cum = _lin(n_obs, 0.0, 2.6)
    if top25_exc is None:
        top25_exc = _lin(n_obs, 0.0, 1.1)
    if top25_cov is None:
        top25_cov = [100.0] * n_obs
    if top50_cum is None:
        top50_cum = _lin(n_obs, 0.0, 3.7)
    if top50_exc is None:
        top50_exc = _lin(n_obs, 0.0, 2.2)
    if top50_cov is None:
        top50_cov = [100.0] * n_obs

    if a25 is None:
        a25 = _analytics(b25, 25, cur_return=top25_cum[-1], excess=top25_exc[-1],
                         max_dd=-7.8, vol=1.17, conc=39.0, n_obs=n_obs)
    if a50 is None:
        a50 = _analytics(b50, 50, cur_return=top50_cum[-1], excess=top50_exc[-1],
                         max_dd=-4.9, vol=0.93, conc=23.5, n_obs=n_obs)

    recon = {"status": decision, "reference_available": True,
             "reference_mark_date": end, "reason": "within tight tolerance."}
    manifest = {
        "phase": "13-I", "decision": decision, "blocked": decision.startswith("BLOCKED_"),
        "analytics_published": decision not in ("BACKFILL_REJECTED_INTEGRITY_FAILURE",)
        and not decision.startswith("BLOCKED_"),
        "alpha_name": "composite_sn", "signal_date": _SIGNAL,
        "backfill_start_date": start, "backfill_end_date": end, "n_observations": n_obs,
        "reference_today": end,
        "benchmark": {"ticker": "SPY", "reference_date": _SIGNAL,
                      "latest_return_since_signal_pct": 1.5},
        "reconciliation": recon, "price_source": "EODHD_LIVE_EOD(adjusted_close)",
        "run_at": "2026-06-26T00:00:00+00:00",
    }
    (bdir / "backfill_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    if decision == "BACKFILL_REJECTED_INTEGRITY_FAILURE" or decision.startswith("BLOCKED_"):
        return

    summary = {
        "top25": a25, "top50": a50,
        "stability_comparison": {"assessment": "TOP50_MORE_STABLE",
                                 "promotes_to_live": False,
                                 "note": "Operational paper-book comparison only."},
        "not_alpha_validation": "short forward window — not alpha validation.",
    }
    (bdir / "paper_performance_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (bdir / "top25_daily_history.json").write_text(
        json.dumps(_history(b25, 25, top25_cum, top25_exc, top25_cov)), encoding="utf-8")
    (bdir / "top50_daily_history.json").write_text(
        json.dumps(_history(b50, 50, top50_cum, top50_exc, top50_cov)), encoding="utf-8")
    spy = _lin(n_obs, 0.0, 1.5)
    (bdir / "spy_daily_history.json").write_text(json.dumps({"ticker": "SPY", "rows": [
        {"mark_date": d, "return_since_signal_pct": spy[i]}
        for i, d in enumerate(_seq_dates(n_obs))]}), encoding="utf-8")


@pytest.fixture
def bdir(tmp_path, monkeypatch) -> Path:
    d = tmp_path / "backfill"
    monkeypatch.setenv(BACKFILL_DIR_ENV, str(d))
    return d


_FRESH_TODAY = "2026-06-26"  # one day after the default end date -> not stale


# --------------------------------------------------------------------------- #
# Provisional-primary rule + decision
# --------------------------------------------------------------------------- #
def test_top50_provisional_primary(bdir):
    _write_backfill(bdir)
    out = load_current_alpha_decision_gate(today=_FRESH_TODAY)
    assert out["status"] == "DECISION_READY"
    assert out["decision"] == DEC_TOP50
    assert out["book_role_status"] == ROLE_TOP50
    assert out["primary_paper_book"]["book"] == "TOP50"
    assert out["primary_paper_book"]["book_id"].endswith("top50")
    assert out["challenger_paper_book"]["label"] == "TOP25"
    assert out["challenger_paper_book"]["book_id"].endswith("top25")
    # never production / approved / live
    assert out["promotes_to_live"] is False
    assert "not production" in out["primary_paper_book"]["qualifier"]


def test_dominance_rule_all_five_criteria(bdir):
    _write_backfill(bdir)
    out = load_current_alpha_decision_gate(today=_FRESH_TODAY)
    t25, t50 = out["top25"], out["top50"]
    passes, checks = _dominance(t50, t25)
    assert passes is True
    names = {c["criterion"]: c["passed"] for c in checks}
    assert names["no_insufficient_coverage_dates"] is True
    assert names["current_excess_positive"] is True
    assert names["shallower_max_drawdown_than_other"] is True
    assert names["lower_daily_volatility_than_other"] is True
    assert names["lower_contributor_concentration_than_other"] is True


def test_no_primary_fallback(bdir):
    # top50 has HIGHER volatility than top25 (fails), top25 has HIGHER concentration
    # than top50 (fails) -> neither dominates, but both excess > 0 -> NO_CLEAR_PRIMARY.
    b25 = "composite_sn__%s__top25" % _SIGNAL
    b50 = "composite_sn__%s__top50" % _SIGNAL
    a25 = _analytics(b25, 25, cur_return=2.6, excess=1.5, max_dd=-6.0, vol=1.0, conc=40.0)
    a50 = _analytics(b50, 50, cur_return=3.7, excess=2.0, max_dd=-5.0, vol=1.5, conc=20.0)
    _write_backfill(bdir, a25=a25, a50=a50)
    out = load_current_alpha_decision_gate(today=_FRESH_TODAY)
    assert out["book_role_status"] == ROLE_NONE
    assert out["decision"] == DEC_NO_CLEAR
    assert out["primary_paper_book"]["book"] is None
    assert out["challenger_paper_book"] is None


def test_continue_monitoring_when_a_book_not_yet_positive(bdir):
    # Neither dominates AND top25 excess is not positive -> CONTINUE_PAPER_MONITORING.
    b25 = "composite_sn__%s__top25" % _SIGNAL
    b50 = "composite_sn__%s__top50" % _SIGNAL
    a25 = _analytics(b25, 25, cur_return=0.5, excess=-0.4, max_dd=-6.0, vol=1.0, conc=40.0)
    a50 = _analytics(b50, 50, cur_return=1.0, excess=0.6, max_dd=-5.0, vol=1.5, conc=20.0)
    _write_backfill(bdir, a25=a25, a50=a50,
                    top25_exc=_lin(25, -0.6, -0.4), top50_exc=_lin(25, 0.0, 0.6))
    out = load_current_alpha_decision_gate(today=_FRESH_TODAY)
    assert out["decision"] == DEC_CONTINUE


def test_deterioration_decision(bdir):
    # top50 cumulative rises to 6% by mid-window then falls to 1% -> rolling-10 return
    # change <= -3% -> PERFORMANCE_DETERIORATING_REVIEW (no risk breach).
    cum = _lin(15, 0.0, 6.0) + _lin(10, 5.5, 1.0)
    exc = _lin(15, 0.0, 2.0) + _lin(10, 1.8, 0.2)
    _write_backfill(bdir, n_obs=25, top50_cum=cum, top50_exc=exc, top50_cov=[100.0] * 25)
    out = load_current_alpha_decision_gate(today=_FRESH_TODAY)
    assert out["decision"] == DEC_DETERIORATING
    assert out["deterioration_review"]["any"] is True
    assert out["top50"]["rolling_10_return_change_pct_points"] <= -3.0


def test_risk_threshold_decision_excess_breach(bdir):
    # top25 current excess <= -5pp -> RISK_THRESHOLD_BREACH_REVIEW (risk beats role).
    b25 = "composite_sn__%s__top25" % _SIGNAL
    a25 = _analytics(b25, 25, cur_return=-4.0, excess=-6.0, max_dd=-8.0, vol=1.4, conc=39.0)
    _write_backfill(bdir, a25=a25, top25_exc=_lin(25, 0.0, -6.0))
    out = load_current_alpha_decision_gate(today=_FRESH_TODAY)
    assert out["decision"] == DEC_RISK
    assert out["risk_review"]["any_breach"] is True
    triggers = [t["trigger"] for t in out["risk_review"]["top25_triggers"]]
    assert "CURRENT_EXCESS_BREACH" in triggers


def test_risk_threshold_decision_coverage_breach(bdir):
    # latest coverage below 90% -> risk breach.
    cov = [100.0] * 24 + [80.0]
    _write_backfill(bdir, top50_cov=cov)
    out = load_current_alpha_decision_gate(today=_FRESH_TODAY)
    assert out["decision"] == DEC_RISK
    triggers = [t["trigger"] for t in out["risk_review"]["top50_triggers"]]
    assert "LATEST_COVERAGE_BELOW_MIN" in triggers


def test_stale_mark_handling(bdir):
    # A fresh view is not stale; a view 30 days after the last mark is a risk breach.
    _write_backfill(bdir, end="2026-06-25")
    fresh = load_current_alpha_decision_gate(today="2026-06-26")
    assert fresh["mark_freshness"]["mark_freshness_status"] == "FRESH_MARK"
    assert fresh["decision"] == DEC_TOP50

    stale = load_current_alpha_decision_gate(today="2026-07-25")
    assert stale["mark_freshness"]["mark_age_calendar_days"] == 30
    assert stale["mark_freshness"]["mark_freshness_status"] in ("STALE_MARK_WARNING", "STALE_MARK_REJECT")
    assert stale["decision"] == DEC_RISK
    both = (stale["risk_review"]["top25_triggers"] + stale["risk_review"]["top50_triggers"])
    assert any(t["trigger"] == "STALE_LATEST_MARK" for t in both)


def test_insufficient_forward_history(bdir):
    _write_backfill(bdir, n_obs=MIN_FORWARD_OBS - 5)
    out = load_current_alpha_decision_gate(today=_FRESH_TODAY)
    assert out["decision"] == DEC_INSUFFICIENT


# --------------------------------------------------------------------------- #
# Quarterly rebalance readiness (pure function)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("completed,expected,elapsed,remaining", [
    (10, READY_EARLY, 9, 54),
    (36, READY_MID, 35, 28),
    (58, READY_APPROACHING, 57, 6),
    (64, READY_DUE, 63, 0),
    (70, READY_OVERDUE, 69, 0),
])
def test_rebalance_readiness_bands(completed, expected, elapsed, remaining):
    r = _rebalance_readiness(completed, _SIGNAL, "2026-06-25")
    assert r["readiness_status"] == expected
    assert r["estimated_trading_days_elapsed"] == elapsed
    assert r["remaining_trading_days"] == remaining
    assert r["target_holding_period_trading_days"] == 63


def test_readiness_surfaced_in_payload(bdir):
    _write_backfill(bdir, n_obs=36)
    out = load_current_alpha_decision_gate(today=_FRESH_TODAY)
    rb = out["quarterly_rebalance_readiness"]
    assert rb["readiness_status"] == READY_MID
    assert rb["completed_financial_marks"] == 36
    assert rb["signal_date"] == _SIGNAL


# --------------------------------------------------------------------------- #
# Rolling metrics + isolation
# --------------------------------------------------------------------------- #
def test_rolling_windows_present_and_isolated(bdir):
    _write_backfill(bdir)
    out = load_current_alpha_decision_gate(today=_FRESH_TODAY)
    t25, t50 = out["top25"], out["top50"]
    for w in (5, 10, 20):
        assert ("rolling_%d_return_change_pct_points" % w) in t25
        assert ("rolling_%d_excess_change_pct_points" % w) in t50
    # Top-25 and Top-50 are never merged: distinct ids + distinct concentration.
    assert t25["book_id"] != t50["book_id"]
    assert t25["book_id"].endswith("top25") and t50["book_id"].endswith("top50")
    assert t25["contributor_concentration_top5_pct"] != t50["contributor_concentration_top5_pct"]


def test_rolling_change_none_when_too_short(bdir):
    _write_backfill(bdir, n_obs=8)  # < 20 obs; rolling-20 undefined
    out = load_current_alpha_decision_gate(today=_FRESH_TODAY)
    assert out["top25"]["rolling_20_return_change_pct_points"] is None
    assert out["top25"]["rolling_5_return_change_pct_points"] is not None


# --------------------------------------------------------------------------- #
# Read-only + no-backfill / not-published
# --------------------------------------------------------------------------- #
def test_no_backfill_yet(bdir):
    out = load_current_alpha_decision_gate(today=_FRESH_TODAY)
    assert out["status"] == "NO_BACKFILL_YET"
    assert out["decision"] == DEC_INSUFFICIENT
    assert "top25" not in out or out.get("top25") is None
    assert out["order_action_all"] == "NO_ORDER"


def test_rejected_backfill_not_published(bdir):
    _write_backfill(bdir, decision="BACKFILL_REJECTED_INTEGRITY_FAILURE")
    out = load_current_alpha_decision_gate(today=_FRESH_TODAY)
    assert out["status"] == "BACKFILL_NOT_PUBLISHED"
    assert out["decision"] == DEC_INSUFFICIENT
    assert out["primary_paper_book"]["status"] == ROLE_NONE


def test_loader_is_read_only(bdir):
    _write_backfill(bdir)
    before = sorted(p.name for p in bdir.iterdir())
    load_current_alpha_decision_gate(today=_FRESH_TODAY)
    after = sorted(p.name for p in bdir.iterdir())
    assert before == after  # writes nothing


# --------------------------------------------------------------------------- #
# Safety
# --------------------------------------------------------------------------- #
def test_safety_block_present(bdir):
    _write_backfill(bdir)
    out = load_current_alpha_decision_gate(today=_FRESH_TODAY)
    for badge in ("PROVISIONAL PAPER BOOK ONLY", "NOT LIVE-TRADING APPROVAL",
                  "NO ORDERS", "NO BROKER", "NO AUTOMATION", "MANUAL REVIEW REQUIRED"):
        assert badge in out["safety_badges"], f"missing badge: {badge}"
    assert out["no_orders"] is True
    assert out["no_broker"] is True
    assert out["no_automation"] is True
    assert out["creates_signals"] is False
    assert out["creates_trade_decisions"] is False
    assert out["wrote_to_paper_trader"] is False
    assert out["live_trading"] is False
    assert out["promotes_to_live"] is False
    assert out["is_live_trading_approval"] is False
    assert out["order_action_all"] == "NO_ORDER"
    assert out["top25"]["order_action"] == "NO_ORDER"
    assert out["top50"]["order_action"] == "NO_ORDER"
