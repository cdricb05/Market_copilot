"""
tests/test_current_alpha_revalidation.py — Phase 17 revalidation loader (unit).

Fully offline: synthetic Phase 17-A report + Phase 17-B challenger artifacts are written to tmp and
the daily-status dependency is INJECTED, so no research runner, no network, no EODHD key, and no
database are needed. Verifies the terminal-decision surfacing, the side-by-side original-vs-repaired
battery, the challenger-package availability (created only on the eligible decision), the entering/
leaving parsing, that NO decision approves live trading and the champion is never replaced, and that the
loader never raises and never writes.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

from paper_trader.api.current_alpha_revalidation import (
    load_current_alpha_revalidation,
    DEC_ELIGIBLE, DEC_KEEP, DEC_FAILED, DEC_UNAVAILABLE, ALLOWED_DECISIONS,
    CHAMPION_SIGNAL, CANDIDATE_SIGNAL,
)


def _eval(ic_t, net25, net50, *, mean_ic=0.03):
    return {"ic_t_stat": ic_t, "mean_ic": mean_ic, "mean_gross_spread": 0.0130, "net25_spread": net25,
            "net50_spread": net50, "mean_turnover": 0.99, "cumulative_spread": 1.5, "max_drawdown": -0.30,
            "positive_ic_month_rate": 0.83, "positive_spread_month_rate": 0.80,
            "subperiod": {"pre2020": {"mean_ic": 0.04, "ic_t": 2.4, "mean_spread": 0.008,
                                      "positive_month_rate": 0.60},
                          "post2020": {"mean_ic": 0.03, "ic_t": 2.0, "mean_spread": 0.015,
                                       "positive_month_rate": 0.62}},
            "rolling_stability": {"ic_12m": {"supported": True, "mean_of_rolling_means": 0.038,
                                             "min_rolling_mean": -0.07, "positive_window_rate": 0.90},
                                  "ic_24m": {"supported": True, "mean_of_rolling_means": 0.042,
                                             "min_rolling_mean": -0.02, "positive_window_rate": 0.96}}}


def _write_reval_artifacts(reval_dir: Path, challenger_dir: Path, *, decision=DEC_ELIGIBLE,
                           created=True):
    reval_dir.mkdir(parents=True, exist_ok=True)
    challenger_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "phase": "17-A", "decision": decision,
        "decision_reasons": ["Repaired-sector candidate is a valid standalone alpha and materially "
                             "differs from the champion."],
        "decision_logic": ["GUARD ...", "FAIL ...", "KEEP ...", "ELIGIBLE ..."],
        "champion_signal": CHAMPION_SIGNAL, "candidate_signal": CANDIDATE_SIGNAL,
        "reproduction": {"reproduces_frozen_composite": True, "max_abs_error": 0.0,
                         "rank_spearman": 0.9999996, "rows_checked": 38404},
        "coverage": {"all234": {"n": 234, "before_pct": 16.67, "after_pct": 100.0},
                     "resolved_fraction": 1.0, "n_universe_resolved": 234},
        "latest_cross_section": {"month": "2026-05", "signal_date": "2026-05-22", "n_names": 234,
                                 "rank_spearman_champion_vs_repaired": 0.775, "top25_overlap": 0.88,
                                 "top50_overlap": 0.84, "bottom25_overlap": 0.64,
                                 "top25_turnover": 0.12, "top50_turnover": 0.16,
                                 "repaired_top25_largest_sector_share_pct": 28.0,
                                 "repaired_top50_largest_sector_share_pct": 26.0},
        "full_panel": {"rank_spearman_champion_vs_repaired": 0.891517,
                       "champion": _eval(3.2645, 0.011174, 0.008687),
                       "repaired_candidate": _eval(2.9285, 0.010211, 0.007724)},
        "challenger_package_created": created,
        "phase13a_context": {"phase13a_signal_date": "2026-05-22"},
    }
    (reval_dir / "phase17a_sector_repaired_champion_revalidation_report.json").write_text(
        json.dumps(report), encoding="utf-8")
    with open(reval_dir / "phase17a_names_entering_leaving.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["book", "direction", "ticker", "repaired_sector", "composite_sn"])
        w.writerow(["TOP25", "ENTERING", "APD", "Materials", "2.70"])
        w.writerow(["TOP25", "LEAVING", "IT", "Information Technology", "1.58"])
    for name, sec in (("phase17a_top25_sector_exposure.csv", "Information Technology"),
                      ("phase17a_top50_sector_exposure.csv", "Financials")):
        with open(reval_dir / name, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["sector", "n_names", "weight_pct", "flag"])
            w.writerow([sec, 7, 28.0, "OK"])
    if created:
        pkg = {"phase": "17-B", "package_type": "SECTOR_REPAIRED_PAPER_CHALLENGER",
               "champion_relationship": "PARALLEL_PAPER_CHALLENGER_DOES_NOT_REPLACE_CHAMPION",
               "candidate_signal": CANDIDATE_SIGNAL, "immutable": True,
               "book_sizes": {"top25": 25, "top50": 50, "bottom25_avoid": 25},
               "price_coverage": {"top25": 11, "top50": 21, "bottom25": 17},
               "go_no_go": "PAPER_CHALLENGER_ELIGIBLE_PAPER_ONLY_NOT_LIVE", "order_action_all": "NO_ORDER"}
        (challenger_dir / "phase17b_sector_repaired_challenger_package.json").write_text(
            json.dumps(pkg), encoding="utf-8")
    else:
        (challenger_dir / "phase17b_challenger_not_created_manifest.json").write_text(
            json.dumps({"phase": "17-B", "challenger_package_created": False,
                        "reasons": ["not eligible under the decision ladder"]}), encoding="utf-8")
    return reval_dir, challenger_dir


def _dirs(tmp_path):
    return tmp_path / "reval", tmp_path / "challenger"


def _daily():
    return {"status": "DAILY_STATUS_READY", "latest_valid_mark_date": "2026-07-16",
            "top25": {"average_return_pct": 3.36}, "top50": {"average_return_pct": 4.68},
            "spy_benchmark": {"return_since_signal_pct": 0.94}}


# --------------------------------------------------------------------------- #
def test_eligible_decision_surfaces(tmp_path):
    r, c = _write_reval_artifacts(*_dirs(tmp_path), decision=DEC_ELIGIBLE, created=True)
    p = load_current_alpha_revalidation(revalidation_dir=r, challenger_dir=c, daily=_daily())
    assert p["decision"] == DEC_ELIGIBLE
    assert p["current_paper_champion"]["signal"] == CHAMPION_SIGNAL
    assert p["sector_repaired_candidate"]["signal"] == CANDIDATE_SIGNAL
    m = p["original_vs_repaired_metrics"]
    assert m["ic_t_stat"]["champion"] == 3.2645 and m["ic_t_stat"]["repaired_candidate"] == 2.9285
    assert m["net25_spread"]["repaired_candidate"] == 0.010211
    assert p["reproduction"]["reproduces_frozen_composite"] is True
    assert p["sector_coverage_before_after"]["all_after_pct"] == 100.0
    assert p["top_book_overlap"]["top25_overlap"] == 0.88


def test_side_by_side_and_stability_present(tmp_path):
    r, c = _write_reval_artifacts(*_dirs(tmp_path))
    p = load_current_alpha_revalidation(revalidation_dir=r, challenger_dir=c, daily=_daily())
    st = p["stability"]
    assert st["rolling_ic_12m"]["champion"]["supported"] is True
    assert st["subperiod_post2020"]["repaired_candidate"]["ic_t"] == 2.0
    el = p["entering_leaving"]
    assert any(x["ticker"] == "APD" for x in el["top25"]["entering"])
    assert any(x["ticker"] == "IT" for x in el["top25"]["leaving"])
    assert p["sector_exposure"]["top25"][0]["sector"] == "Information Technology"


def test_challenger_created_flag(tmp_path):
    r, c = _write_reval_artifacts(*_dirs(tmp_path), decision=DEC_ELIGIBLE, created=True)
    p = load_current_alpha_revalidation(revalidation_dir=r, challenger_dir=c, daily=_daily())
    assert p["challenger_package"]["created"] is True
    assert p["challenger_package"]["order_action_all"] == "NO_ORDER"
    assert p["challenger_package"]["go_no_go"].endswith("NOT_LIVE")


def test_not_eligible_keep_no_challenger(tmp_path):
    r, c = _write_reval_artifacts(*_dirs(tmp_path), decision=DEC_KEEP, created=False)
    p = load_current_alpha_revalidation(revalidation_dir=r, challenger_dir=c, daily=_daily())
    assert p["decision"] == DEC_KEEP
    assert p["challenger_package"]["created"] is False
    assert p["challenger_package"]["not_created_reason"]


def test_failed_decision_no_challenger(tmp_path):
    r, c = _write_reval_artifacts(*_dirs(tmp_path), decision=DEC_FAILED, created=False)
    p = load_current_alpha_revalidation(revalidation_dir=r, challenger_dir=c, daily=_daily())
    assert p["decision"] == DEC_FAILED
    assert p["challenger_package"]["created"] is False


def test_missing_artifacts_degrades_not_raises(tmp_path):
    p = load_current_alpha_revalidation(revalidation_dir=tmp_path / "nope",
                                        challenger_dir=tmp_path / "nope2", daily={})
    assert p["decision"] in (DEC_UNAVAILABLE,) + ALLOWED_DECISIONS
    assert p["warnings"]
    assert p["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"


def test_no_decision_approves_live_trading(tmp_path):
    for dec, created in ((DEC_ELIGIBLE, True), (DEC_KEEP, False), (DEC_FAILED, False)):
        r, c = _write_reval_artifacts(tmp_path / dec / "r", tmp_path / dec / "c", decision=dec, created=created)
        p = load_current_alpha_revalidation(revalidation_dir=r, challenger_dir=c, daily=_daily())
        assert p["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"
        assert p["no_decision_approves_live_trading"] is True
        assert p["promotes_to_live"] is False and p["champion_replaced"] is False


def test_read_only_safety_block(tmp_path):
    r, c = _write_reval_artifacts(*_dirs(tmp_path))
    p = load_current_alpha_revalidation(revalidation_dir=r, challenger_dir=c, daily=_daily())
    assert p["read_only"] is True and p["wrote_to_database"] is False
    assert p["creates_orders"] is False and p["creates_signals"] is False
    assert p["no_automation"] is True and p["no_broker"] is True
    assert p["mutates_champion"] is False and p["replaces_champion"] is False


def test_current_daily_marks_composed(tmp_path):
    r, c = _write_reval_artifacts(*_dirs(tmp_path))
    p = load_current_alpha_revalidation(revalidation_dir=r, challenger_dir=c, daily=_daily())
    cm = p["current_daily_marks"]
    assert cm["champion_top50_return_pct"] == 4.68 and cm["spy_return_pct"] == 0.94
