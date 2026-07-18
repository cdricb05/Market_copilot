"""
tests/test_current_alpha_integrity_gate.py — Phase 16-A integrity-gate loader (unit).

Fully offline: a synthetic Phase 16-A artifact dir is written to tmp and the decision-gate /
daily-status / operating-state dependencies are INJECTED, so no research runner, no network, no
EODHD key, and no database are needed. Verifies the paper-test status ladder (continue / checkpoint /
research-revalidation / data-integrity-blocked), that INITIAL vs CURRENT coverage are kept separate,
that no status approves live trading, and that the loader never raises and never writes.
"""
from __future__ import annotations

import json
from pathlib import Path

from paper_trader.api.current_alpha_integrity_gate import (
    load_current_alpha_integrity_gate,
    ST_CONTINUE, ST_CHECKPOINT, ST_REVALIDATE, ST_BLOCKED, ALLOWED_STATUSES,
)


def _write_artifacts(d: Path, *, shadow_decision="RESEARCH_REVALIDATION_REQUIRED",
                     reproduces=True, full_spearman=0.8915):
    d.mkdir(parents=True, exist_ok=True)
    shadow = {
        "phase": "16-A", "decision": shadow_decision,
        "decision_reasons": ["Repaired sectors materially change ranks: full-panel Spearman "
                             "%.4f < 0.90." % full_spearman],
        "reproduction": {"reproduces_frozen_composite": reproduces, "max_abs_error": 0.0,
                         "rank_spearman": 0.9999996},
        "latest_cross_section": {"month": "2026-05", "n_names": 234,
                                 "rank_spearman_champion_vs_shadow": 0.775, "top25_overlap": 0.88,
                                 "top50_overlap": 0.84, "bottom25_overlap": 0.64,
                                 "top25_turnover": 0.12, "top50_turnover": 0.16,
                                 "shadow_top25_largest_sector_share_pct": 32.0},
        "full_panel": {"rank_spearman_champion_vs_shadow": full_spearman,
                       "champion": {"ic_t_stat": 3.2645, "net25_spread": 0.011174},
                       "shadow": {"ic_t_stat": 2.9285, "net25_spread": 0.010211}},
        "sector_coverage_summary": {"all234_before_pct": 16.67, "all234_after_pct": 100.0,
                                    "top25_before_pct": 0.0, "top25_after_pct": 100.0,
                                    "top50_before_pct": 12.0, "top50_after_pct": 100.0,
                                    "n_resolved": 234, "n_unresolved": 0},
        "phase13a_context": {"phase13a_signal_date": "2026-05-22",
                             "phase13a_price_coverage": {"top25": 14, "top50": 24, "bottom25": 17}},
    }
    (d / "phase16a_shadow_revalidation_report.json").write_text(json.dumps(shadow), encoding="utf-8")
    (d / "phase16a_sector_integrity_report.json").write_text(json.dumps({"phase": "16-A"}), encoding="utf-8")
    (d / "sector_metadata_coverage.json").write_text(json.dumps({"n_ranked": 234}), encoding="utf-8")
    return d


def _gate(*, elapsed=35, remaining=28, readiness="MONITORING_MID_CYCLE"):
    return {
        "status": "PAPER_BOOK_DECISION_READY", "decision": "PROVISIONAL_TOP50_PRIMARY",
        "signal_date": "2026-05-22", "latest_mark_date": "2026-07-15",
        "quarterly_rebalance_readiness": {"estimated_trading_days_elapsed": elapsed,
                                          "remaining_trading_days": remaining,
                                          "readiness_status": readiness,
                                          "target_holding_period_trading_days": 63},
        "risk_review": {"any_breach": False}, "mark_freshness": {"mark_freshness_status": "FRESH"},
    }


def _daily():
    return {"status": "DAILY_STATUS_READY", "latest_valid_mark_date": "2026-07-16",
            "latest_valid_mark_available": True,
            "top25": {"covered_count": 25, "total_count": 25, "average_return_pct": 3.3593,
                      "excess_return_vs_spy_pct_points": 2.4187},
            "top50": {"covered_count": 50, "total_count": 50, "average_return_pct": 4.6841,
                      "excess_return_vs_spy_pct_points": 3.7435},
            "spy_benchmark": {"return_since_signal_pct": 0.9406}}


def _operating():
    return {"current_mark_date": "2026-07-16", "history_end_date": "2026-07-15"}


# --------------------------------------------------------------------------- #
def test_status_research_revalidation_required(tmp_path):
    d = _write_artifacts(tmp_path / "a")
    p = load_current_alpha_integrity_gate(integrity_dir=d, gate=_gate(), daily=_daily(),
                                          operating=_operating())
    assert p["status"] == ST_REVALIDATE
    assert p["sector_shadow_decision"] == "RESEARCH_REVALIDATION_REQUIRED"
    assert any("materially change ranks" in b for b in p["blockers"])
    # rank/overlap metrics surfaced from the shadow report
    m = p["rank_correlation_and_overlap_metrics"]
    assert m["full_panel_rank_spearman"] == 0.8915 or round(m["full_panel_rank_spearman"], 4) == 0.8915
    assert m["top25_overlap"] == 0.88 and m["top50_overlap"] == 0.84


def test_status_continue_when_shadow_keeps(tmp_path):
    d = _write_artifacts(tmp_path / "a", shadow_decision="KEEP_CURRENT_CHAMPION", full_spearman=0.99)
    p = load_current_alpha_integrity_gate(integrity_dir=d, gate=_gate(remaining=28), daily=_daily(),
                                          operating=_operating())
    assert p["status"] == ST_CONTINUE


def test_status_checkpoint_when_horizon_due(tmp_path):
    d = _write_artifacts(tmp_path / "a", shadow_decision="KEEP_CURRENT_CHAMPION")
    p = load_current_alpha_integrity_gate(integrity_dir=d,
                                          gate=_gate(elapsed=63, remaining=0, readiness="READY_DUE"),
                                          daily=_daily(), operating=_operating())
    assert p["status"] == ST_CHECKPOINT


def test_status_blocked_when_artifacts_missing(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    p = load_current_alpha_integrity_gate(integrity_dir=empty, gate=_gate(), daily=_daily(),
                                          operating=_operating())
    assert p["status"] == ST_BLOCKED
    assert p["data_integrity_blocked"] is True
    assert any("missing or fails its reproduction" in b for b in p["blockers"])


def test_status_blocked_when_reproduction_fails(tmp_path):
    d = _write_artifacts(tmp_path / "a", reproduces=False)
    p = load_current_alpha_integrity_gate(integrity_dir=d, gate=_gate(), daily=_daily(),
                                          operating=_operating())
    assert p["status"] == ST_BLOCKED


def test_initial_and_current_coverage_are_separate(tmp_path):
    d = _write_artifacts(tmp_path / "a")
    p = load_current_alpha_integrity_gate(integrity_dir=d, gate=_gate(), daily=_daily(),
                                          operating=_operating())
    cur = p["current_daily_mark_coverage"]
    init = p["initial_entry_price_coverage"]
    assert cur["top25_covered"] == 25 and cur["top25_total"] == 25          # current (fresh marks)
    assert cur["top50_covered"] == 50 and cur["top50_total"] == 50
    assert init["top25_covered"] == 14 and init["top50_covered"] == 24      # frozen 13-A initialization
    assert cur["label"] == "CURRENT DAILY-MARK COVERAGE"
    assert init["label"] == "INITIAL ENTRY-PRICE COVERAGE"
    assert cur is not init and cur["top25_covered"] != init["top25_covered"]


def test_no_status_approves_live_trading(tmp_path):
    for dec in ("RESEARCH_REVALIDATION_REQUIRED", "KEEP_CURRENT_CHAMPION"):
        d = _write_artifacts(tmp_path / dec, shadow_decision=dec, full_spearman=0.99)
        p = load_current_alpha_integrity_gate(integrity_dir=d, gate=_gate(), daily=_daily(),
                                              operating=_operating())
        assert p["status"] in ALLOWED_STATUSES
        assert p["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"
        assert p["status_approves_live_trading"] is False
        assert p["no_status_approves_live_trading"] is True
        assert p["promotes_to_live"] is False
        assert p["no_live_trading"] is True


def test_read_only_safety_block(tmp_path):
    d = _write_artifacts(tmp_path / "a")
    p = load_current_alpha_integrity_gate(integrity_dir=d, gate=_gate(), daily=_daily(),
                                          operating=_operating())
    for k in ("read_only", "no_orders", "no_broker", "no_automation", "preview_only"):
        assert p[k] is True
    for k in ("creates_orders", "creates_signals", "creates_trade_decisions", "wrote_to_database",
              "mutates_champion"):
        assert p[k] is False
    assert set(["PAPER ONLY", "MANUAL REVIEW", "NO BROKER EXECUTION", "AUTOMATION OFF",
                "NO LIVE ORDERS"]).issubset(set(p["safety_badges"]))


def test_horizon_and_next_checkpoint(tmp_path):
    d = _write_artifacts(tmp_path / "a")
    p = load_current_alpha_integrity_gate(integrity_dir=d, gate=_gate(elapsed=35, remaining=28),
                                          daily=_daily(), operating=_operating())
    hp = p["horizon_progress"]
    assert hp["trading_days_elapsed"] == 35 and hp["trading_days_remaining"] == 28
    assert hp["target_holding_period_trading_days"] == 63
    # signal 2026-05-22 + ~92 calendar days -> 2026-08-22
    assert hp["next_formal_checkpoint_date"] == "2026-08-22"


def test_never_raises_on_empty_inputs(tmp_path):
    p = load_current_alpha_integrity_gate(integrity_dir=tmp_path / "nope", gate={}, daily={},
                                          operating={})
    assert p["status"] == ST_BLOCKED
    assert isinstance(p["warnings"], list)
