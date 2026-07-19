"""
tests/test_current_alpha_tournament.py — Phase 18 tournament loader + manual refresh (unit).

Fully offline: a synthetic Phase 18-A forward-test report is written to a tmp forward dir and
a tmp dedicated tournament store is used, so no research runner, no network, no EODHD key and
no database are needed. Verifies the four-book side-by-side surfacing, same-date comparison,
the manual + idempotent refresh (preview / confirm / no-new), that the refresh writes ONLY the
dedicated local store (never a database / positions / orders), and that NO status approves live
trading and the champion is never replaced.
"""
from __future__ import annotations

import json
from pathlib import Path

from paper_trader.api.current_alpha_tournament import (
    load_current_alpha_tournament,
    run_current_alpha_tournament_refresh,
    REFRESH_CONFIRM_TOKEN,
    CHAMPION_SIGNAL,
    CHALLENGER_SIGNAL,
    _STATE_FILE,
    _SNAPSHOTS_FILE,
)


def _book(key, size, cum, excess, cov, total, dd=-6.0, conc=45.0):
    return {"book_key": key, "signal": (CHAMPION_SIGNAL if "champion" in key else CHALLENGER_SIGNAL),
            "book_size": size, "n_members": total, "n_marks": 24,
            "start_date": "2026-05-22", "end_date": "2026-06-26",
            "cumulative_return_pct": cum, "excess_return_vs_spy_pct_points": excess,
            "max_drawdown_pct": dd, "daily_volatility_pct_points": 1.2,
            "positive_day_rate_pct": 52.17, "days_outperforming_spy_pct": 43.0,
            "coverage_pct": round(100.0 * cov / total, 2), "covered_count": cov,
            "total_count": total, "contributor_concentration_top5_pct": conc,
            "best_contributor": {"ticker": "AAA", "sector": "Tech", "paper_return_pct": 5.0},
            "worst_contributor": {"ticker": "ZZZ", "sector": "Energy", "paper_return_pct": -5.0}}


def _h2h(size, cp_ex, ch_ex):
    return {"book_size": size,
            "champion": {"excess_return_vs_spy_pct_points": cp_ex, "cumulative_return_pct": -2.0,
                         "max_drawdown_pct": -6.0, "coverage_pct": 56.0},
            "challenger": {"excess_return_vs_spy_pct_points": ch_ex, "cumulative_return_pct": -0.9,
                           "max_drawdown_pct": -8.0, "coverage_pct": 44.0},
            "challenger_minus_champion": {"excess_return_vs_spy_pct_points": round(ch_ex - cp_ex, 4)},
            "same_date_comparison": True, "n_marks": 24}


def _write_forward_report(fdir: Path, *, decision="MONITORING_MID_CYCLE", latest="2026-06-26",
                          elapsed=24, checkpoint=False):
    fdir.mkdir(parents=True, exist_ok=True)
    report = {
        "phase": "18-A", "decision": decision,
        "decision_reasons": ["Forward window is incomplete (24 of 63 trading marks)."],
        "current_paper_champion": {"signal": CHAMPION_SIGNAL},
        "sector_repaired_paper_challenger": {"signal": CHALLENGER_SIGNAL},
        "frozen_books": True, "reranked": False, "rebalanced": False,
        "signal_date": "2026-05-22",
        "horizon_progress": {"signal_date": "2026-05-22", "horizon_trading_days": 63,
                             "elapsed_marks": elapsed, "remaining_marks": 63 - elapsed,
                             "checkpoint_reached": checkpoint,
                             "latest_common_owned_eod_date": latest,
                             "review_target_date": "2026-08-22"},
        "calendar": {"n_marks": elapsed, "start_date": "2026-05-22", "end_date": latest},
        "spy": {"available": True, "ticker": "SPY", "cumulative_return_pct": -1.9812,
                "reference_date": "2026-05-22", "price_source": "EODHD_OWNED_SPY"},
        "book_summaries": {
            "champion_top25": _book("champion_top25", 25, -2.2328, -0.2516, 14, 25),
            "challenger_top25": _book("challenger_top25", 25, -0.8657, 1.1155, 11, 25),
            "champion_top50": _book("champion_top50", 50, -0.1204, 1.8608, 24, 50),
            "challenger_top50": _book("challenger_top50", 50, -0.7558, 1.2254, 21, 50),
        },
        "daily_curves": {"champion_top25": {"cumulative_curve": [], "excess_curve": [],
                                            "drawdown_curve": []}, "spy": []},
        "sector_exposure": {
            "champion_top25": [{"book_key": "champion_top25", "sector": "Unknown",
                                "n_names": 25, "weight_pct": 100.0}],
            "challenger_top25": [{"book_key": "challenger_top25", "sector": "Information Technology",
                                  "n_names": 7, "weight_pct": 28.0}],
        },
        "top25_head_to_head": _h2h(25, -0.2516, 1.1155),
        "top50_head_to_head": _h2h(50, 1.8608, 1.2254),
        "book_isolation": {"all_isolated": True},
        "reproduction": {"reproduces_stored_entries": True, "max_abs_error": 0.0},
        "coverage_warnings": ["champion_top25: 14 of 25 names have owned local prices."],
        "risk_flags": [{"book_key": "champion_top25", "flag": "CONCENTRATION_REVIEW",
                        "value": 69.36, "limit": 60.0}],
        "entering_leaving_inherited_from_phase17": [
            {"book": "TOP25", "direction": "ENTERING", "ticker": "APD"},
            {"book": "TOP25", "direction": "LEAVING", "ticker": "HAS"}],
        "next_review_target": "2026-08-22",
        "next_action": "Continue the parallel paper tournament.",
        "price_source": "EODHD_LOCAL_EOD(adjusted_close, owned)", "run_at": "2026-07-18T00:00:00Z",
    }
    (fdir / "phase18a_parallel_challenger_forward_test_report.json").write_text(
        json.dumps(report), encoding="utf-8")
    return fdir


def _dirs(tmp_path):
    return tmp_path / "forward", tmp_path / "store"


# --------------------------------------------------------------------------- #
def test_loader_surfaces_four_books_and_decision(tmp_path):
    fdir, tdir = _dirs(tmp_path)
    _write_forward_report(fdir)
    p = load_current_alpha_tournament(forward_dir=fdir, tournament_dir=tdir)
    assert p["decision"] == "MONITORING_MID_CYCLE"
    assert p["current_paper_champion"]["signal"] == CHAMPION_SIGNAL
    assert p["sector_repaired_paper_challenger"]["signal"] == CHALLENGER_SIGNAL
    bs = p["book_summaries"]
    assert set(bs) == {"champion_top25", "challenger_top25", "champion_top50", "challenger_top50"}
    assert bs["challenger_top25"]["excess_return_vs_spy_pct_points"] == 1.1155
    assert p["latest_common_financial_mark"] == "2026-06-26"
    assert p["spy"]["available"] is True


def test_head_to_head_never_reverses_champion_challenger(tmp_path):
    fdir, tdir = _dirs(tmp_path)
    _write_forward_report(fdir)
    p = load_current_alpha_tournament(forward_dir=fdir, tournament_dir=tdir)
    h25 = p["top25_head_to_head"]
    assert h25["champion"]["excess_return_vs_spy_pct_points"] == -0.2516
    assert h25["challenger"]["excess_return_vs_spy_pct_points"] == 1.1155
    assert p["aligned_comparison"]["same_date_top25"] is True
    assert p["horizon_progress"]["elapsed_marks"] == 24


def test_missing_forward_artifact_degrades_not_raises(tmp_path):
    p = load_current_alpha_tournament(forward_dir=tmp_path / "nope", tournament_dir=tmp_path / "no2")
    assert p["decision"] == "TOURNAMENT_UNAVAILABLE"
    assert p["warnings"]
    assert p["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"


def test_no_status_approves_live_trading(tmp_path):
    for dec in ("MONITORING_MID_CYCLE", "CHALLENGER_PAPER_PROMOTION_ELIGIBLE",
                "KEEP_CURRENT_PAPER_CHAMPION", "REJECT_PAPER_CHALLENGER"):
        fdir, tdir = tmp_path / dec / "f", tmp_path / dec / "s"
        _write_forward_report(fdir, decision=dec)
        p = load_current_alpha_tournament(forward_dir=fdir, tournament_dir=tdir)
        assert p["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"
        assert p["no_decision_approves_live_trading"] is True
        assert p["champion_replaced"] is False and p["promotes_to_live"] is False


def test_read_only_get_writes_nothing(tmp_path):
    fdir, tdir = _dirs(tmp_path)
    _write_forward_report(fdir)
    load_current_alpha_tournament(forward_dir=fdir, tournament_dir=tdir)
    # a read-only GET must not create the store
    assert not (tdir / _STATE_FILE).exists()
    assert not (tdir / _SNAPSHOTS_FILE).exists()


# --------------------------------------------------------------------------- #
# Manual refresh: preview / confirm / idempotency / store-only / no-DB.
# --------------------------------------------------------------------------- #
def test_refresh_preview_writes_nothing(tmp_path):
    fdir, tdir = _dirs(tmp_path)
    _write_forward_report(fdir)
    p = run_current_alpha_tournament_refresh(commit=False, forward_dir=fdir, tournament_dir=tdir)
    assert p["status"] == "TOURNAMENT_REFRESH_PREVIEW"
    assert p["wrote_store"] is False
    assert not (tdir / _STATE_FILE).exists()


def test_refresh_commit_requires_confirmation(tmp_path):
    fdir, tdir = _dirs(tmp_path)
    _write_forward_report(fdir)
    p = run_current_alpha_tournament_refresh(commit=True, confirm="WRONG",
                                             forward_dir=fdir, tournament_dir=tdir)
    assert p["status"] == "REFRESH_CONFIRMATION_REQUIRED"
    assert p["wrote_store"] is False
    assert not (tdir / _STATE_FILE).exists()


def test_refresh_commit_writes_only_local_store(tmp_path):
    fdir, tdir = _dirs(tmp_path)
    _write_forward_report(fdir)
    p = run_current_alpha_tournament_refresh(commit=True, confirm=REFRESH_CONFIRM_TOKEN,
                                             forward_dir=fdir, tournament_dir=tdir)
    assert p["status"] == "TOURNAMENT_REFRESH_COMPLETE"
    assert p["action"] == "SNAPSHOT_WRITTEN"
    assert p["wrote_store"] is True
    assert p["last_recorded_financial_date"] == "2026-06-26"
    assert p["wrote_to_database"] is False
    # the ONLY artifacts are the two local store files
    assert (tdir / _STATE_FILE).exists() and (tdir / _SNAPSHOTS_FILE).exists()
    state = json.loads((tdir / _STATE_FILE).read_text(encoding="utf-8"))
    assert state["last_recorded_financial_date"] == "2026-06-26"
    assert state["order_action_all"] == "NO_ORDER"


def test_refresh_is_idempotent_by_financial_date(tmp_path):
    fdir, tdir = _dirs(tmp_path)
    _write_forward_report(fdir)
    run_current_alpha_tournament_refresh(commit=True, confirm=REFRESH_CONFIRM_TOKEN,
                                         forward_dir=fdir, tournament_dir=tdir)
    p2 = run_current_alpha_tournament_refresh(commit=True, confirm=REFRESH_CONFIRM_TOKEN,
                                              forward_dir=fdir, tournament_dir=tdir)
    assert p2["status"] == "NO_NEW_COMPLETED_EOD_DATE"
    assert p2["wrote_store"] is False
    snaps = json.loads((tdir / _SNAPSHOTS_FILE).read_text(encoding="utf-8"))
    assert snaps["n_snapshots"] == 1  # no duplicate snapshot


def test_refresh_advances_on_newer_date(tmp_path):
    fdir, tdir = _dirs(tmp_path)
    _write_forward_report(fdir, latest="2026-06-26", elapsed=24)
    run_current_alpha_tournament_refresh(commit=True, confirm=REFRESH_CONFIRM_TOKEN,
                                         forward_dir=fdir, tournament_dir=tdir)
    # a newer completed date arrives -> a new snapshot is recorded
    _write_forward_report(fdir, latest="2026-06-29", elapsed=25)
    p = run_current_alpha_tournament_refresh(commit=True, confirm=REFRESH_CONFIRM_TOKEN,
                                             forward_dir=fdir, tournament_dir=tdir)
    assert p["status"] == "TOURNAMENT_REFRESH_COMPLETE"
    snaps = json.loads((tdir / _SNAPSHOTS_FILE).read_text(encoding="utf-8"))
    assert snaps["n_snapshots"] == 2
    dates = [s["financial_mark_date"] for s in snaps["snapshots"]]
    assert dates == ["2026-06-26", "2026-06-29"]


def test_refresh_safety_block(tmp_path):
    fdir, tdir = _dirs(tmp_path)
    _write_forward_report(fdir)
    p = run_current_alpha_tournament_refresh(commit=True, confirm=REFRESH_CONFIRM_TOKEN,
                                             forward_dir=fdir, tournament_dir=tdir)
    for k in ("creates_orders", "creates_signals", "creates_trade_decisions", "creates_fills",
              "wrote_to_database", "champion_replaced", "promotes_to_live", "is_automation",
              "calls_prediction_service"):
        assert p[k] is False
    assert p["order_action_all"] == "NO_ORDER"
    assert p["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"
