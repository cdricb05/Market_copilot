"""
tests/test_current_alpha_daily_refresh.py — Phase 13-G/H manual daily-refresh orchestrator.

Fully offline: every test injects a FAKE launcher (no subprocess, no network, no key)
that writes a synthetic Phase 13-G mark artifact, then drives the orchestrator through
its snapshot flow. It verifies: the subprocess command is shell-free and never carries
the API key; a new price date snapshots TOP 25 and TOP 50 independently; a same price
date adds no duplicate; NO_NEW / blocked results add no snapshot; the daily artifact is
preferred over the Phase 13-A package (labelled PHASE13A_STALE_FALLBACK when it is not);
the paper-only / no-DB / no-order contract; and the read-only daily-status aggregator.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from paper_trader.api.current_alpha_daily_refresh import (
    DAILY_REFRESH_SAFETY_BADGES,
    _build_refresh_command,
    load_current_alpha_daily_status,
    run_current_alpha_daily_refresh,
)
from paper_trader.api.current_alpha_book import snapshot_current_alpha_book

from tests.test_current_alpha_operations import _write_ops_fixture

_REPO_ROOT = Path(__file__).resolve().parents[1]
_MODULE_PATH = _REPO_ROOT / "api" / "current_alpha_daily_refresh.py"

_TICKERS = ["AAA", "BBB", "CCC", "DDD"]


def _write_daily_artifact(mark_dir: Path, *, mark_date: str = "2026-07-14", pct: float = 5.0,
                          spy_ret: float = 1.0, refresh_result: str = "REFRESH_OK_NEW_MARK_DATE",
                          prev: str | None = None) -> None:
    """Write a synthetic Phase 13-G latest/ mark artifact matching the ops fixture book
    (composite_sn, signal 2026-05-22, tickers AAA..DDD)."""
    latest = mark_dir / "latest"
    latest.mkdir(parents=True, exist_ok=True)
    marks = [{
        "ticker": t, "alpha_name": "composite_sn", "signal_date": "2026-05-22",
        "source_rank": i + 1, "in_top25": True, "in_top50": True,
        "entry_reference_date": "2026-05-22", "entry_price": 100.0,
        "latest_completed_eod_date": mark_date, "latest_adjusted_close": 100.0 * (1 + pct / 100),
        "paper_return_pct": pct, "price_source": "EODHD_LIVE_EOD(adjusted_close)",
        "price_status": "MARKED", "acquisition_status": "OK",
    } for i, t in enumerate(_TICKERS)]
    benchmark = {
        "ticker": "SPY", "available": True, "reference_date": "2026-05-22",
        "reference_price": 500.0, "latest_completed_eod_date": mark_date,
        "latest_adjusted_close": 500.0 * (1 + spy_ret / 100), "return_since_signal_pct": spy_ret,
    }

    def _book(size: int) -> dict:
        return {
            "book_id": "composite_sn__2026-05-22__top%d" % size, "book_size": size,
            "mark_date": mark_date, "covered_count": 4, "missing_count": 0, "total_count": 4,
            "coverage_pct": 100.0, "coverage_status": "FULL_COVERAGE", "pnl_claim_valid": True,
            "average_return_pct": pct, "median_return_pct": pct, "hit_rate_pct": 100.0,
            "best_5": [], "worst_5": [], "previous_mark_date": prev,
            "previous_average_return_pct": None, "change_since_previous_mark_pct_points": None,
            "benchmark_return_pct": spy_ret, "excess_return_vs_spy_pct_points": round(pct - spy_ret, 4),
            "order_action_all": "NO_ORDER",
        }

    (latest / "daily_alpha_marks.json").write_text(json.dumps(
        {"phase": "13-G", "alpha_name": "composite_sn", "signal_date": "2026-05-22",
         "mark_date": mark_date, "marks": marks, "benchmark": benchmark}), encoding="utf-8")
    (latest / "book_summaries.json").write_text(json.dumps(
        {"top25": _book(25), "top50": _book(50), "benchmark": benchmark, "mark_date": mark_date}),
        encoding="utf-8")
    (latest / "refresh_manifest.json").write_text(json.dumps({
        "phase": "13-G", "refresh_result": refresh_result, "new_mark_date": True,
        "mark_date": mark_date, "previous_mark_date": prev, "alpha_name": "composite_sn",
        "signal_date": "2026-05-22", "n_marks": len(marks),
        "price_source": "EODHD_LIVE_EOD(adjusted_close)",
        "universe": {"benchmark": "SPY", "n_top50": 4},
        "benchmark_summary_preview": benchmark,
        "book_summaries_preview": [_book(25), _book(50)],
        "last_refresh_run_at": "2026-07-15T00:00:00+00:00",
    }), encoding="utf-8")


def _launcher(**kwargs):
    """A fake launcher that writes the synthetic artifact and returns a launch record."""
    def launcher(py, runner, marks_root, audit, repo, timeout):
        _write_daily_artifact(Path(marks_root), **kwargs)
        return {"launched": True, "returncode": 0, "stderr_tail": ""}
    return launcher


def _no_artifact_launcher(py, runner, marks_root, audit, repo, timeout):
    # writes nothing (simulates a refresh that produced no manifest)
    return {"launched": True, "returncode": 1, "error": "no manifest"}


@pytest.fixture
def env(tmp_path: Path):
    pkg = _write_ops_fixture(tmp_path / "pkg")
    return {"pkg": pkg, "store": tmp_path / "store", "marks": tmp_path / "marks",
            "repo": tmp_path / "repo"}


# --------------------------------------------------------------------------- #
# Subprocess command safety
# --------------------------------------------------------------------------- #
def test_build_command_shell_free_and_no_key(monkeypatch):
    monkeypatch.setenv("EODHD_API_KEY", "SECRET_KEY_XYZ_9999")
    cmd = _build_refresh_command("python", Path("runner.py"), Path("D:/marks"), Path("D:/audit"))
    assert isinstance(cmd, list)               # an argv list -> used with shell=False
    assert "SECRET_KEY_XYZ_9999" not in cmd
    assert not any("SECRET_KEY_XYZ_9999" in str(part) for part in cmd)
    assert "--mark-dir" in cmd and "--audit-dir" in cmd


def test_module_uses_shell_false_only():
    src = _MODULE_PATH.read_text(encoding="utf-8")
    assert "shell=False" in src
    assert "shell=True" not in src
    low = src.lower()
    for needle in (".commit(", ".add(", "insert into", "update ", "delete from",
                   "create_order", "place_order", "submit_order", "/v1/orders",
                   "/v1/signals", "/v1/decisions", "get_session", "session.add",
                   "engine.connect", "prediction_client"):
        assert needle not in low, f"forbidden token in orchestrator: {needle!r}"


# --------------------------------------------------------------------------- #
# New mark date -> both books snapshotted independently
# --------------------------------------------------------------------------- #
def test_new_mark_snapshots_both_books_independently(env):
    r = run_current_alpha_daily_refresh(
        commit=True, book_dir=env["store"], mark_dir=env["marks"],
        package_dir=env["pkg"], launcher=_launcher())
    assert r["status"] == "DAILY_REFRESH_COMPLETE"
    t25, t50 = r["snapshots"]["top25"], r["snapshots"]["top50"]
    assert t25["action"] == "SNAPSHOT_WRITTEN" and t50["action"] == "SNAPSHOT_WRITTEN"
    assert t25["book_id"].endswith("top25") and t50["book_id"].endswith("top50")
    assert t25["book_id"] != t50["book_id"]                    # marked independently
    assert t25["mark_source"] == "PHASE13G_DAILY_REFRESH"
    assert t25["as_of_price_date"] == "2026-07-14"
    # daily marks cover all 4 names -> avg 5.0 (fallback would exclude CCC -> 4.0)
    assert t25["average_return_pct"] == pytest.approx(5.0)
    assert t25["covered_count"] == 4
    assert t25["benchmark_return_pct"] == pytest.approx(1.0)
    assert t25["excess_return_vs_spy_pct_points"] == pytest.approx(4.0)
    assert t25["wrote_to_local_paper_store"] is True


def test_same_price_date_no_duplicate(env):
    run_current_alpha_daily_refresh(commit=True, book_dir=env["store"], mark_dir=env["marks"],
                                    package_dir=env["pkg"], launcher=_launcher())
    again = run_current_alpha_daily_refresh(commit=True, book_dir=env["store"], mark_dir=env["marks"],
                                            package_dir=env["pkg"], launcher=_launcher())
    t25 = again["snapshots"]["top25"]
    assert t25["action"] == "SNAPSHOT_SKIPPED_NO_NEW_PRICE_DATE"
    assert t25["n_snapshots_after"] == 1


def test_new_price_date_advances_both_books(env):
    run_current_alpha_daily_refresh(commit=True, book_dir=env["store"], mark_dir=env["marks"],
                                    package_dir=env["pkg"], launcher=_launcher(mark_date="2026-07-14"))
    second = run_current_alpha_daily_refresh(
        commit=True, book_dir=env["store"], mark_dir=env["marks"], package_dir=env["pkg"],
        launcher=_launcher(mark_date="2026-07-16", prev="2026-07-14"))
    assert second["snapshots"]["top25"]["action"] == "SNAPSHOT_WRITTEN"
    assert second["snapshots"]["top25"]["n_snapshots_after"] == 2
    assert second["snapshots"]["top50"]["n_snapshots_after"] == 2


def test_no_new_mark_date_adds_no_snapshot(env):
    r = run_current_alpha_daily_refresh(
        commit=True, book_dir=env["store"], mark_dir=env["marks"], package_dir=env["pkg"],
        launcher=_launcher(refresh_result="NO_NEW_MARK_DATE"))
    assert r["action"] == "NO_SNAPSHOT"
    assert r["refresh_result"] == "NO_NEW_MARK_DATE"
    assert r["snapshots"] == {}


def test_blocked_refresh_adds_no_snapshot(env):
    r = run_current_alpha_daily_refresh(
        commit=True, book_dir=env["store"], mark_dir=env["marks"], package_dir=env["pkg"],
        launcher=_launcher(refresh_result="BLOCKED_EODHD_KEY"))
    assert r["action"] == "NO_SNAPSHOT"
    assert r["refresh_result"] == "BLOCKED_EODHD_KEY"
    assert r["snapshots"] == {}


def test_no_manifest_is_controlled(env):
    r = run_current_alpha_daily_refresh(
        commit=True, book_dir=env["store"], mark_dir=env["marks"], package_dir=env["pkg"],
        launcher=_no_artifact_launcher)
    assert r["status"] == "REFRESH_UNAVAILABLE"
    assert r["snapshots"] == {}


# --------------------------------------------------------------------------- #
# Daily marks preferred over the Phase 13-A fallback
# --------------------------------------------------------------------------- #
def test_daily_marks_preferred_then_fallback_labeled(env):
    # 1) with a fresh daily artifact -> daily marks (avg 5.0, PHASE13G_DAILY_REFRESH)
    run_current_alpha_daily_refresh(commit=True, book_dir=env["store"], mark_dir=env["marks"],
                                    package_dir=env["pkg"], launcher=_launcher())
    daily = snapshot_current_alpha_book(env["pkg"], commit=False, book_dir=env["store"],
                                        book_size=25, daily_mark_dir=env["marks"])
    assert daily["mark_source"] == "PHASE13G_DAILY_REFRESH"
    assert daily["snapshot"]["average_return_pct"] == pytest.approx(5.0)

    # 2) point at an empty daily dir -> Phase 13-A package fallback (avg 4.0, labelled)
    fallback = snapshot_current_alpha_book(env["pkg"], commit=False, book_dir=env["store"],
                                           book_size=25, daily_mark_dir=env["marks"] / "empty")
    assert fallback["mark_source"] == "PHASE13A_STALE_FALLBACK"
    assert fallback["snapshot"]["average_return_pct"] == pytest.approx(4.0)
    assert any("PHASE13A_STALE_FALLBACK" in w for w in fallback["warnings"])


# --------------------------------------------------------------------------- #
# Safety contract
# --------------------------------------------------------------------------- #
def test_refresh_safety_flags(env):
    r = run_current_alpha_daily_refresh(commit=True, book_dir=env["store"], mark_dir=env["marks"],
                                        package_dir=env["pkg"], launcher=_launcher())
    assert r["no_orders"] is True
    assert r["wrote_to_paper_trader"] is False
    assert r["creates_signals"] is False
    assert r["creates_trade_decisions"] is False
    assert r["order_action_all"] == "NO_ORDER"
    assert r["is_automation"] is False
    assert r["manual_user_triggered"] is True
    assert "DOES NOT EXECUTE TRADES" in r["safety_badges"]
    assert r["refresh"]["shell"] is False
    assert r["refresh"]["api_key_in_command_line"] is False


def test_preview_mode_writes_nothing(env):
    r = run_current_alpha_daily_refresh(commit=False, book_dir=env["store"], mark_dir=env["marks"],
                                        package_dir=env["pkg"], launcher=_launcher())
    assert r["committed"] is False
    assert r["action"] == "SNAPSHOTS_PREVIEWED"
    # no snapshots file written to the paper store
    assert not (env["store"] / "pnl_snapshots.json").is_file()


# --------------------------------------------------------------------------- #
# Daily status (read-only aggregator)
# --------------------------------------------------------------------------- #
def _write_audit(repo: Path) -> None:
    out = repo / "research" / "output" / "phase13g_current_alpha_universe_integrity_audit"
    out.mkdir(parents=True, exist_ok=True)
    (out / "phase13g_current_alpha_universe_integrity_audit.json").write_text(json.dumps({
        "validated_alpha_universe_name": "phase8v_combined_eodhd_price_fundamentals_universe",
        "universe_definition": "S&P-500-seeded but broader combined EODHD universe",
        "is_strict_sp500_universe": False,
        "decision": "CURRENT_UNIVERSE_BROADER_KEEP_CHAMPION",
        "latest_ranked_count": 234,
        "latest_cross_section_membership": {"confirmed_sp500": 194},
        "sp500_shadow": {"net_25bps": 0.00013, "ic_t_63d": 2.593,
                         "average_quarterly_return": 0.00328},
        "sp500_shadow_decision": "SP500_SHADOW_REJECTED_WEAKER",
    }), encoding="utf-8")


def test_daily_status_reports_benchmark_universe_and_histories(env):
    run_current_alpha_daily_refresh(commit=True, book_dir=env["store"], mark_dir=env["marks"],
                                    package_dir=env["pkg"], launcher=_launcher())
    _write_audit(env["repo"])
    status = load_current_alpha_daily_status(
        book_dir=env["store"], mark_dir=env["marks"], research_repo_dir=env["repo"])
    assert status["status"] == "DAILY_STATUS_READY"
    assert status["data_source"] == "PHASE13G_DAILY_REFRESH"
    assert status["latest_completed_eod_date"] == "2026-07-14"
    # universe identity: NOT strict S&P 500; shadow shown separately
    ui = status["universe_identity"]
    assert ui["is_strict_sp500"] is False
    assert "phase8v" in ui["current_champion_universe"]
    assert ui["sp500_shadow_decision"] == "SP500_SHADOW_REJECTED_WEAKER"
    # benchmark + coverage
    assert status["top25"]["coverage_status"] == "FULL_COVERAGE"
    assert status["spy_benchmark"]["return_since_signal_pct"] == pytest.approx(1.0)
    # histories isolated per book
    assert status["top25_history"]["selected_book_id"].endswith("top25")
    assert status["top50_history"]["selected_book_id"].endswith("top50")
    assert status["top25_history"]["n_snapshots"] == 1
    assert status["top50_history"]["n_snapshots"] == 1


def test_daily_status_no_refresh_yet(env):
    status = load_current_alpha_daily_status(
        book_dir=env["store"], mark_dir=env["marks"] / "empty", research_repo_dir=env["repo"])
    assert status["status"] == "NO_DAILY_REFRESH_YET"
    assert status["data_source"] == "PHASE13A_STALE_FALLBACK"
    assert status["no_orders"] is True
