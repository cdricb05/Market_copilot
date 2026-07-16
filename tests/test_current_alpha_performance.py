"""
tests/test_current_alpha_performance.py — Phase 13-I paper performance loader.

Fully offline: a synthetic Phase 13-I backfill artifact is written to a tmp dir and
``PAPER_TRADER_CURRENT_ALPHA_BACKFILL_DIR`` points the loader at it. No research
runner, no network, no EODHD key, no database. Verifies: the controlled statuses
(NO_BACKFILL_YET / PERFORMANCE_READY / BACKFILL_REJECTED / BACKFILL_BLOCKED), that the
Top-25 and Top-50 analytics + curves stay separate (never merged), the drawdown-curve
math, that a rejected/blocked backfill publishes no analytics, and that the payload
carries the frozen-holdings safety block (no orders / no DB / promotes no book to live).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from paper_trader.api.current_alpha_performance import (
    BACKFILL_DIR_ENV,
    load_current_alpha_performance,
)


# --------------------------------------------------------------------------- #
# Fixture builder: a small, deterministic 3-date reconstruction.
# --------------------------------------------------------------------------- #
def _analytics(size: int) -> dict:
    return {
        "book_id": "composite_sn__2026-05-22__top%d" % size,
        "book_size": size,
        "n_observations": 3,
        "start_date": "2026-05-22",
        "end_date": "2026-05-27",
        "current_cumulative_return_pct": 5.0,
        "spy_cumulative_return_pct": 2.0,
        "current_excess_return_pct_points": 3.0,
        "max_drawdown_pct": -4.5455,
        "max_drawdown_duration_obs": 1,
        "max_drawdown_recovered": False,
        "best_daily_change_pct_points": 10.0,
        "worst_daily_change_pct_points": -5.0,
        "daily_change_volatility_pct_points": 7.5,
        "pct_positive_daily_changes": 50.0,
        "pct_days_outperforming_spy": 50.0,
        "average_daily_excess_change_pct_points": -3.0,
        "tracking_error_pct_points": 7.5,
        "information_ratio": None,
        "information_ratio_valid": False,
        "contributor_concentration_top5_pct": (20.0 if size == 25 else 10.0),
        "n_coverage_warning_dates": 0,
        "n_insufficient_coverage_dates": 0,
        "n_daily_change_observations": 2,
    }


def _book_rows(size: int) -> dict:
    rows = [
        {"mark_date": "2026-05-22", "average_return_pct": 0.0, "excess_return_vs_spy_pct_points": 0.0},
        {"mark_date": "2026-05-26", "average_return_pct": 10.0, "excess_return_vs_spy_pct_points": 9.0},
        {"mark_date": "2026-05-27", "average_return_pct": 5.0, "excess_return_vs_spy_pct_points": 3.0},
    ]
    return {"book_size": size, "n_observations": 3, "rows": rows}


def _write_backfill(bdir: Path, *, decision: str = "BACKFILL_RECONCILED",
                    blocked_message=None) -> None:
    bdir.mkdir(parents=True, exist_ok=True)
    recon = {"status": decision, "reference_mark_date": "2026-05-27",
             "reference_available": True, "reason": "within tight tolerance."}
    manifest = {
        "phase": "13-I", "decision": decision, "blocked": decision.startswith("BLOCKED_"),
        "analytics_published": decision not in ("BACKFILL_REJECTED_INTEGRITY_FAILURE",)
        and not decision.startswith("BLOCKED_"),
        "alpha_name": "composite_sn", "signal_date": "2026-05-22",
        "backfill_start_date": "2026-05-22", "backfill_end_date": "2026-05-27",
        "n_observations": 3,
        "benchmark": {"ticker": "SPY", "reference_date": "2026-05-22",
                      "latest_return_since_signal_pct": 2.0},
        "reconciliation": recon, "price_source": "EODHD_LIVE_EOD(adjusted_close)",
        "blocked_message": blocked_message, "run_at": "2026-05-28T00:00:00+00:00",
    }
    (bdir / "backfill_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    # Rejected / blocked reconstructions publish ONLY the manifest.
    if decision == "BACKFILL_REJECTED_INTEGRITY_FAILURE" or decision.startswith("BLOCKED_"):
        return

    summary = {
        "top25": _analytics(25), "top50": _analytics(50),
        "stability_comparison": {"assessment": "NO_CLEAR_STABILITY_WINNER",
                                 "promotes_to_live": False,
                                 "note": "Operational paper-book comparison only."},
        "not_alpha_validation": "short forward window — not alpha validation.",
    }
    (bdir / "paper_performance_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (bdir / "top25_daily_history.json").write_text(json.dumps(_book_rows(25)), encoding="utf-8")
    (bdir / "top50_daily_history.json").write_text(json.dumps(_book_rows(50)), encoding="utf-8")
    (bdir / "spy_daily_history.json").write_text(json.dumps({"ticker": "SPY", "rows": [
        {"mark_date": "2026-05-22", "return_since_signal_pct": 0.0},
        {"mark_date": "2026-05-26", "return_since_signal_pct": 1.0},
        {"mark_date": "2026-05-27", "return_since_signal_pct": 2.0},
    ]}), encoding="utf-8")


@pytest.fixture
def bdir(tmp_path, monkeypatch) -> Path:
    d = tmp_path / "backfill"
    monkeypatch.setenv(BACKFILL_DIR_ENV, str(d))
    return d


# --------------------------------------------------------------------------- #
# Statuses
# --------------------------------------------------------------------------- #
def test_no_backfill_yet(bdir):
    out = load_current_alpha_performance()
    assert out["status"] == "NO_BACKFILL_YET"
    assert out["backfill_decision"] is None
    assert "guidance" in out
    # safety block still present
    assert out["no_orders"] is True and out["order_action_all"] == "NO_ORDER"


def test_performance_ready_reconciled(bdir):
    _write_backfill(bdir)
    out = load_current_alpha_performance()
    assert out["status"] == "PERFORMANCE_READY"
    assert out["backfill_decision"] == "BACKFILL_RECONCILED"
    assert out["reconciliation_status"] == "BACKFILL_RECONCILED"
    assert out["latest_mark_date"] == "2026-05-27"
    assert out["backfill_start_date"] == "2026-05-22"
    assert out["observation_count"] == 3


def test_top25_top50_isolated(bdir):
    _write_backfill(bdir)
    out = load_current_alpha_performance()
    a25, a50 = out["top25_analytics"], out["top50_analytics"]
    assert a25["book_id"].endswith("top25") and a50["book_id"].endswith("top50")
    assert a25["book_id"] != a50["book_id"]
    # concentration differs (5/25 vs 5/50) -> not merged
    assert a25["contributor_concentration_top5_pct"] == 20.0
    assert a50["contributor_concentration_top5_pct"] == 10.0


def test_curves_present_and_separate(bdir):
    _write_backfill(bdir)
    out = load_current_alpha_performance()
    c25, c50 = out["top25_curves"], out["top50_curves"]
    assert c25["n_marks"] == 3 and c50["n_marks"] == 3
    for c in (c25, c50):
        assert {r["mark_date"] for r in c["cumulative_curve"]} == {"2026-05-22", "2026-05-26", "2026-05-27"}
        assert "excess_curve" in c and "drawdown_curve" in c
    assert [r["mark_date"] for r in out["spy_curve"]] == ["2026-05-22", "2026-05-26", "2026-05-27"]


def test_drawdown_curve_math(bdir):
    _write_backfill(bdir)
    out = load_current_alpha_performance()
    dd = out["top25_curves"]["drawdown_curve"]
    by_date = {r["mark_date"]: r["drawdown_pct"] for r in dd}
    assert by_date["2026-05-22"] == pytest.approx(0.0, abs=1e-6)
    assert by_date["2026-05-26"] == pytest.approx(0.0, abs=1e-6)     # new peak
    assert by_date["2026-05-27"] == pytest.approx(-4.5455, abs=1e-3)  # 1.05/1.10 - 1


def test_stability_comparison_surfaced(bdir):
    _write_backfill(bdir)
    out = load_current_alpha_performance()
    st = out["stability_comparison"]
    assert st["assessment"] == "NO_CLEAR_STABILITY_WINNER"
    assert st["promotes_to_live"] is False


def test_warning_still_publishes(bdir):
    _write_backfill(bdir, decision="BACKFILL_RECONCILIATION_WARNING")
    out = load_current_alpha_performance()
    assert out["status"] == "PERFORMANCE_READY"
    assert out["backfill_decision"] == "BACKFILL_RECONCILIATION_WARNING"
    assert out["top25_analytics"] is not None
    assert any("WARNING" in w for w in out["warnings"])


def test_rejected_blocks_analytics(bdir):
    _write_backfill(bdir, decision="BACKFILL_REJECTED_INTEGRITY_FAILURE")
    out = load_current_alpha_performance()
    assert out["status"] == "BACKFILL_REJECTED"
    assert out["backfill_decision"] == "BACKFILL_REJECTED_INTEGRITY_FAILURE"
    assert "top25_analytics" not in out          # no analytics published
    assert "reconciliation" in out
    assert any("integrity" in w.lower() for w in out["warnings"])


def test_blocked_backfill(bdir):
    _write_backfill(bdir, decision="BLOCKED_EODHD_ENTITLEMENT",
                    blocked_message="entitlement probe failed")
    out = load_current_alpha_performance()
    assert out["status"] == "BACKFILL_BLOCKED"
    assert out["backfill_decision"] == "BLOCKED_EODHD_ENTITLEMENT"
    assert "top25_analytics" not in out


# --------------------------------------------------------------------------- #
# Safety
# --------------------------------------------------------------------------- #
def test_safety_block_present(bdir):
    _write_backfill(bdir)
    out = load_current_alpha_performance()
    for badge in ("HISTORICAL PAPER MARK RECONSTRUCTION", "FROZEN HOLDINGS",
                  "NO DAILY REBALANCING", "PAPER TEST ONLY", "NO ORDERS", "NO BROKER",
                  "NO AUTOMATION", "DOES NOT CREATE SIGNALS", "DOES NOT CREATE TRADE DECISIONS",
                  "DOES NOT EXECUTE TRADES"):
        assert badge in out["safety_badges"], f"missing badge: {badge}"
    assert out["no_orders"] is True
    assert out["no_broker"] is True
    assert out["no_automation"] is True
    assert out["wrote_to_paper_trader"] is False
    assert out["live_trading"] is False
    assert out["frozen_holdings"] is True
    assert out["daily_rebalancing"] is False
    assert out["reranking"] is False
    assert out["promotes_to_live"] is False
    assert out["order_action_all"] == "NO_ORDER"


def test_provenance_reconstruction_kind(bdir):
    _write_backfill(bdir)
    out = load_current_alpha_performance()
    prov = out["provenance"]
    assert prov["alpha_name"] == "composite_sn"
    assert prov["reconstruction_kind"] == "HISTORICAL_MARK_TO_MARKET_FROZEN_HOLDINGS"
