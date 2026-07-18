"""
api/current_alpha_revalidation.py — Phase 17 read-only sector-repaired revalidation service.

This module surfaces, read-only, the result of the FORMAL Phase 17-A sector-repaired
champion revalidation: whether the sector-repaired candidate (``composite_sn_repaired``)
is eligible to run as a PARALLEL PAPER CHALLENGER alongside the current paper champion
(``composite_sn``), how the two compare across the full validation battery, and whether
the immutable Phase 17-B challenger package was created. It reads the committed Phase 17
research artifacts and (where useful) composes the read-only daily status for current
marks.

Read-only by design:
    - Reads the committed Phase 17-A report + Phase 17-B package artifacts.
    - Writes no files, touches no database, creates no signals / decisions / orders /
      trades / fills.
    - Never calls the prediction service or any external market-data provider.
    - Never changes the current champion and never approves live trading; every payload
      carries an explicit no-live-trading block.

Public API:
    load_current_alpha_revalidation(...) -> dict   (never raises; degrades to warnings)

Default Phase 17 artifact directories (overridable via env):
    PAPER_TRADER_CURRENT_ALPHA_REVALIDATION_DIR ->
        C:\\Users\\binis\\Stock_Prediction_app_push\\research\\output\\
            phase17a_sector_repaired_champion_revalidation
    PAPER_TRADER_CURRENT_ALPHA_CHALLENGER_DIR ->
        C:\\Users\\binis\\Stock_Prediction_app_push\\research\\output\\
            phase17b_sector_repaired_challenger_package
"""
from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

# ---------------------------------------------------------------------------
# Location + constants
# ---------------------------------------------------------------------------

PHASE = "17"
CHAMPION_SIGNAL = "composite_sn"
CANDIDATE_SIGNAL = "composite_sn_repaired"

REVAL_DIR_ENV_VAR = "PAPER_TRADER_CURRENT_ALPHA_REVALIDATION_DIR"
CHALLENGER_DIR_ENV_VAR = "PAPER_TRADER_CURRENT_ALPHA_CHALLENGER_DIR"
DEFAULT_REVAL_DIR = Path(
    r"C:\Users\binis\Stock_Prediction_app_push\research\output"
    r"\phase17a_sector_repaired_champion_revalidation"
)
DEFAULT_CHALLENGER_DIR = Path(
    r"C:\Users\binis\Stock_Prediction_app_push\research\output"
    r"\phase17b_sector_repaired_challenger_package"
)

REPORT_JSON = "phase17a_sector_repaired_champion_revalidation_report.json"
ENTERING_LEAVING_CSV = "phase17a_names_entering_leaving.csv"
TOP25_EXP_CSV = "phase17a_top25_sector_exposure.csv"
TOP50_EXP_CSV = "phase17a_top50_sector_exposure.csv"
CHALLENGER_JSON = "phase17b_sector_repaired_challenger_package.json"
CHALLENGER_NOT_CREATED = "phase17b_challenger_not_created_manifest.json"

# Terminal decisions (mirror the runner). NONE approves live trading.
DEC_ELIGIBLE = "PAPER_CHALLENGER_ELIGIBLE"
DEC_KEEP = "KEEP_CURRENT_PAPER_CHAMPION"
DEC_FAILED = "RESEARCH_REVALIDATION_FAILED"
DEC_BLOCKED_DATA = "BLOCKED_DATA_MISSING"
DEC_BLOCKED_ERROR = "BLOCKED_RUNNER_ERROR"
DEC_UNAVAILABLE = "REVALIDATION_ARTIFACTS_UNAVAILABLE"
ALLOWED_DECISIONS = (DEC_ELIGIBLE, DEC_KEEP, DEC_FAILED, DEC_BLOCKED_DATA, DEC_BLOCKED_ERROR)

SAFETY_BADGES = ["PAPER ONLY", "MANUAL REVIEW", "NO BROKER EXECUTION", "AUTOMATION OFF", "NO LIVE ORDERS"]

_NEXT_ACTION = {
    DEC_ELIGIBLE: (
        "Adopt the sector-repaired candidate as a PARALLEL PAPER CHALLENGER (Phase 17-B package) and "
        "track it side-by-side with the current paper champion over the 63-trading-day paper horizon. Do "
        "NOT replace the champion and do NOT promote anything to live trading — paper-only, manual review."),
    DEC_KEEP: (
        "Keep running the single current paper champion: repaired sectors did not materially change the "
        "book, so a separate challenger is not warranted. Paper-only, no live trading."),
    DEC_FAILED: (
        "Hold the current paper champion; the sector-repaired revalidation did not clear the a-priori "
        "eligibility ladder. Open a deeper research pass before any book change. No live trading."),
    DEC_BLOCKED_DATA: (
        "Restore the missing owned artifact and re-run Phase 17-A, then re-read this revalidation. No "
        "decision is made while blocked."),
    DEC_BLOCKED_ERROR: (
        "The Phase 17-A runner errored; inspect its report and re-run. No decision is made while blocked."),
    DEC_UNAVAILABLE: (
        "Run research/run_phase17a_sector_repaired_champion_revalidation.py to produce the Phase 17 "
        "artifacts, then re-read this revalidation. No decision is available yet."),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _num(x: Any) -> Optional[float]:
    if isinstance(x, bool) or x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _resolve_dir(explicit: Optional[Union[str, Path]], env_var: str, default: Path) -> Path:
    if explicit is not None:
        return Path(explicit)
    env = os.environ.get(env_var)
    if env:
        return Path(env)
    return default


def _read_json(path: Path) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            obj = json.load(fh)
        return obj if isinstance(obj, dict) else None
    except (OSError, ValueError):
        return None


def _read_csv_rows(path: Path) -> list[dict]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return list(csv.DictReader(fh))
    except OSError:
        return []


def _safety_block() -> dict[str, Any]:
    return {
        "safety_badges": list(SAFETY_BADGES),
        "preview_only": True,
        "read_only": True,
        "manual_review_only": True,
        "no_orders": True,
        "no_broker": True,
        "no_automation": True,
        "no_prediction_call": True,
        "no_live_trading": True,
        "promotes_to_live": False,
        "decision_approves_live_trading": False,
        "creates_signals": False,
        "creates_trade_decisions": False,
        "creates_orders": False,
        "creates_fills": False,
        "wrote_to_database": False,
        "mutates_champion": False,
        "replaces_champion": False,
    }


def _side_metrics(champ: dict, cand: dict) -> dict[str, Any]:
    """Side-by-side original vs repaired battery metrics."""
    def pair(key):
        return {"champion": _num(champ.get(key)), "repaired_candidate": _num(cand.get(key))}
    return {
        "ic_t_stat": pair("ic_t_stat"),
        "mean_ic": pair("mean_ic"),
        "mean_gross_spread": pair("mean_gross_spread"),
        "net25_spread": pair("net25_spread"),
        "net50_spread": pair("net50_spread"),
        "mean_turnover": pair("mean_turnover"),
        "cumulative_spread": pair("cumulative_spread"),
        "max_drawdown": pair("max_drawdown"),
        "positive_ic_month_rate": pair("positive_ic_month_rate"),
        "positive_spread_month_rate": pair("positive_spread_month_rate"),
    }


def _stability(champ: dict, cand: dict) -> dict[str, Any]:
    def roll(ev, key):
        st = (ev.get("rolling_stability") or {}).get(key) or {}
        return {"supported": st.get("supported"),
                "mean_of_rolling_means": _num(st.get("mean_of_rolling_means")),
                "min_rolling_mean": _num(st.get("min_rolling_mean")),
                "positive_window_rate": _num(st.get("positive_window_rate"))}

    def sub(ev, pk):
        s = (ev.get("subperiod") or {}).get(pk) or {}
        return {"mean_ic": _num(s.get("mean_ic")), "ic_t": _num(s.get("ic_t")),
                "mean_spread": _num(s.get("mean_spread")),
                "positive_month_rate": _num(s.get("positive_month_rate"))}

    return {
        "rolling_ic_12m": {"champion": roll(champ, "ic_12m"), "repaired_candidate": roll(cand, "ic_12m")},
        "rolling_ic_24m": {"champion": roll(champ, "ic_24m"), "repaired_candidate": roll(cand, "ic_24m")},
        "subperiod_pre2020": {"champion": sub(champ, "pre2020"), "repaired_candidate": sub(cand, "pre2020")},
        "subperiod_post2020": {"champion": sub(champ, "post2020"), "repaired_candidate": sub(cand, "post2020")},
    }


def _exposure_rows(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        out.append({"sector": r.get("sector"), "n_names": r.get("n_names"),
                    "weight_pct": _num(r.get("weight_pct")), "flag": r.get("flag")})
    return out


# ---------------------------------------------------------------------------
# Public service
# ---------------------------------------------------------------------------

def load_current_alpha_revalidation(
    *,
    revalidation_dir: Optional[Union[str, Path]] = None,
    challenger_dir: Optional[Union[str, Path]] = None,
    daily: Optional[dict] = None,
    today: Optional[str] = None,
) -> dict[str, Any]:
    """Read-only Phase 17 sector-repaired revalidation payload.

    Reads the committed Phase 17-A report + Phase 17-B challenger artifacts and (where useful) composes
    the read-only daily status for current marks. ``daily`` is injectable for deterministic tests. The
    function never raises, never writes, never changes the champion, and never approves live trading.
    """
    warnings: list[str] = []
    rdir = _resolve_dir(revalidation_dir, REVAL_DIR_ENV_VAR, DEFAULT_REVAL_DIR)
    cdir = _resolve_dir(challenger_dir, CHALLENGER_DIR_ENV_VAR, DEFAULT_CHALLENGER_DIR)

    report = _read_json(rdir / REPORT_JSON)
    if report is None:
        warnings.append("Phase 17-A revalidation report not found at %s — run the Phase 17-A runner."
                        % (rdir / REPORT_JSON))
        report = {}

    decision = report.get("decision") or (DEC_UNAVAILABLE if not report else DEC_BLOCKED_DATA)
    decision_reasons = report.get("decision_reasons") or []

    # --- reproduction + coverage ---------------------------------------------------------
    repro = report.get("reproduction") or {}
    coverage = report.get("coverage") or {}
    cov_all = coverage.get("all234") or {}

    # --- full-panel battery (champion vs repaired candidate) -----------------------------
    fp = report.get("full_panel") or {}
    champ_fp = fp.get("champion") or {}
    cand_fp = fp.get("repaired_candidate") or {}
    side_by_side_metrics = _side_metrics(champ_fp, cand_fp)
    stability = _stability(champ_fp, cand_fp)

    # --- latest cross-section overlap + concentration ------------------------------------
    lcs = report.get("latest_cross_section") or {}
    overlap = {
        "full_panel_rank_spearman": _num(fp.get("rank_spearman_champion_vs_repaired")),
        "latest_month_rank_spearman": _num(lcs.get("rank_spearman_champion_vs_repaired")),
        "top25_overlap": _num(lcs.get("top25_overlap")),
        "top50_overlap": _num(lcs.get("top50_overlap")),
        "bottom25_overlap": _num(lcs.get("bottom25_overlap")),
        "top25_turnover": _num(lcs.get("top25_turnover")),
        "top50_turnover": _num(lcs.get("top50_turnover")),
    }
    repaired_top25_conc = _num(lcs.get("repaired_top25_largest_sector_share_pct"))
    repaired_top50_conc = _num(lcs.get("repaired_top50_largest_sector_share_pct"))
    concentration_warnings: list[str] = []
    if repaired_top25_conc is not None and repaired_top25_conc > 30:
        concentration_warnings.append("Repaired candidate Top25 book single-sector concentration is "
                                      "%.0f%% (>30%%)." % repaired_top25_conc)

    sector_exposure = {
        "top25": _exposure_rows(_read_csv_rows(rdir / TOP25_EXP_CSV)),
        "top50": _exposure_rows(_read_csv_rows(rdir / TOP50_EXP_CSV)),
        "repaired_top25_largest_sector_share_pct": repaired_top25_conc,
        "repaired_top50_largest_sector_share_pct": repaired_top50_conc,
    }

    # --- entering / leaving names --------------------------------------------------------
    entering_leaving = {"top25": {"entering": [], "leaving": []},
                        "top50": {"entering": [], "leaving": []}}
    for r in _read_csv_rows(rdir / ENTERING_LEAVING_CSV):
        book = (r.get("book") or "").lower()
        direction = (r.get("direction") or "").lower()
        if book in entering_leaving and direction in ("entering", "leaving"):
            entering_leaving[book][direction].append(
                {"ticker": r.get("ticker"), "repaired_sector": r.get("repaired_sector"),
                 "composite_sn": _num(r.get("composite_sn"))})

    # --- challenger package availability -------------------------------------------------
    challenger_pkg = _read_json(cdir / CHALLENGER_JSON)
    challenger_not_created = _read_json(cdir / CHALLENGER_NOT_CREATED)
    challenger_created = bool(report.get("challenger_package_created")) and challenger_pkg is not None
    challenger = {
        "created": challenger_created,
        "package_type": (challenger_pkg or {}).get("package_type"),
        "champion_relationship": (challenger_pkg or {}).get("champion_relationship"),
        "candidate_signal": (challenger_pkg or {}).get("candidate_signal") or CANDIDATE_SIGNAL,
        "book_sizes": (challenger_pkg or {}).get("book_sizes"),
        "price_coverage": (challenger_pkg or {}).get("price_coverage"),
        "go_no_go": (challenger_pkg or {}).get("go_no_go"),
        "order_action_all": (challenger_pkg or {}).get("order_action_all"),
        "immutable": (challenger_pkg or {}).get("immutable"),
        "dir": str(cdir),
        "not_created_reason": (challenger_not_created or {}).get("reasons") if not challenger_created else None,
    }

    # --- current daily marks (champion book; context only) -------------------------------
    if daily is None:
        try:
            from paper_trader.api.current_alpha_daily_refresh import load_current_alpha_daily_status
            daily = load_current_alpha_daily_status()
        except Exception as exc:  # noqa: BLE001 - degrade, never fail the endpoint
            daily = {}
            warnings.append("daily status unavailable: %s" % exc)
    daily = daily or {}
    ds25 = daily.get("top25") or {}
    ds50 = daily.get("top50") or {}
    spy = daily.get("spy_benchmark") or {}
    current_daily_marks = {
        "as_of_mark_date": daily.get("latest_valid_mark_date"),
        "champion_top25_return_pct": _num(ds25.get("average_return_pct")),
        "champion_top50_return_pct": _num(ds50.get("average_return_pct")),
        "spy_return_pct": _num(spy.get("return_since_signal_pct")),
        "basis": "current champion daily operating mark (context; the challenger is not yet marked)",
    }

    # --- status class for the UI ---------------------------------------------------------
    status_class = {
        DEC_ELIGIBLE: "safe", DEC_KEEP: "safe", DEC_FAILED: "warn",
        DEC_BLOCKED_DATA: "danger", DEC_BLOCKED_ERROR: "danger", DEC_UNAVAILABLE: "warn",
    }.get(decision, "warn")

    payload = {
        "phase": PHASE,
        "decision": decision,
        "decision_label": str(decision).replace("_", " ").title(),
        "status_class": status_class,
        "decision_reasons": decision_reasons,
        "decision_logic": report.get("decision_logic") or [],
        "allowed_decisions": list(ALLOWED_DECISIONS),
        "current_paper_champion": {"signal": CHAMPION_SIGNAL, "role": "CURRENT PAPER CHAMPION"},
        "sector_repaired_candidate": {"signal": CANDIDATE_SIGNAL, "role": "SECTOR-REPAIRED PAPER CHALLENGER"},
        "signal_date": lcs.get("signal_date") or report.get("phase13a_context", {}).get("phase13a_signal_date"),
        "cross_section_month": lcs.get("month"),
        # reproduction + coverage
        "reproduction": {
            "reproduces_frozen_composite": bool(repro.get("reproduces_frozen_composite")),
            "max_abs_error": _num(repro.get("max_abs_error")),
            "rank_spearman": _num(repro.get("rank_spearman")),
            "rows_checked": repro.get("rows_checked"),
        },
        "sector_coverage_before_after": {
            "all_before_pct": _num(cov_all.get("before_pct")),
            "all_after_pct": _num(cov_all.get("after_pct")),
            "resolved_fraction": _num(coverage.get("resolved_fraction")),
            "n_universe_resolved": coverage.get("n_universe_resolved"),
            "n_names": cov_all.get("n"),
            "source": "owned EODHD fundamentals GicSector (committed Phase 16-A resolved map)",
            "point_in_time": False,
        },
        # side-by-side battery
        "original_vs_repaired_metrics": side_by_side_metrics,
        "stability": stability,
        "top_book_overlap": overlap,
        "entering_leaving": entering_leaving,
        "sector_exposure": sector_exposure,
        "concentration_warnings": concentration_warnings,
        # challenger + marks + action
        "challenger_package": challenger,
        "current_daily_marks": current_daily_marks,
        "next_recommended_action": _NEXT_ACTION.get(decision, _NEXT_ACTION[DEC_UNAVAILABLE]),
        # explicit no-live-trading status
        "live_trading_status": "NOT_APPROVED_FOR_LIVE_TRADING",
        "no_decision_approves_live_trading": True,
        "champion_replaced": False,
        "provenance": {
            "revalidation_dir": str(rdir),
            "challenger_dir": str(cdir),
            "report_found": bool(report),
            "challenger_report_found": challenger_pkg is not None,
            "sources": [REPORT_JSON, ENTERING_LEAVING_CSV, TOP25_EXP_CSV, TOP50_EXP_CSV, CHALLENGER_JSON,
                        "load_current_alpha_daily_status"],
            "champion_signal": CHAMPION_SIGNAL,
            "candidate_signal": CANDIDATE_SIGNAL,
        },
        "loaded_at": _iso_now(),
        "warnings": warnings,
    }
    payload.update(_safety_block())
    return payload


__all__ = [
    "load_current_alpha_revalidation",
    "SAFETY_BADGES",
    "ALLOWED_DECISIONS",
    "REVAL_DIR_ENV_VAR",
    "CHALLENGER_DIR_ENV_VAR",
    "CHAMPION_SIGNAL",
    "CANDIDATE_SIGNAL",
]
