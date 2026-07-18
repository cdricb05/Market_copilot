"""
api/current_alpha_integrity_gate.py — Phase 16-A read-only paper-test integrity gate.

This module answers, read-only, whether the current paper champion (``composite_sn``)
should continue its paper test, hit a formal checkpoint, or go back for research
revalidation, and whether anything is data-integrity blocked. It COMPOSES existing
read-only loaders (the decision gate, the daily status, the canonical operating state)
with the committed Phase 16-A research artifacts (the sector-metadata integrity audit
and the shadow sector-neutral revalidation).

Read-only by design:
    - It reads existing service outputs and the committed Phase 16-A JSON artifacts.
    - It writes no files, touches no database, and creates no signals / decisions /
      orders / trades / fills.
    - It never calls the prediction service or any external market-data provider.
    - It never approves live trading. No status it can return implies live trading;
      every payload carries an explicit no-live-trading block.

Public API:
    load_current_alpha_integrity_gate(...) -> dict   (never raises; degrades to warnings)

Default Phase 16-A artifact directory (overridable via
PAPER_TRADER_CURRENT_ALPHA_INTEGRITY_DIR):
    C:\\Users\\binis\\Stock_Prediction_app_push\\research\\output\\
        phase16a_sector_metadata_integrity_and_shadow_revalidation
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

from paper_trader.api.current_alpha_decision_gate import load_current_alpha_decision_gate
from paper_trader.api.current_alpha_daily_refresh import load_current_alpha_daily_status

# ---------------------------------------------------------------------------
# Location + constants
# ---------------------------------------------------------------------------

PHASE = "16-A"
CHAMPION_SIGNAL = "composite_sn"

INTEGRITY_DIR_ENV_VAR = "PAPER_TRADER_CURRENT_ALPHA_INTEGRITY_DIR"
DEFAULT_INTEGRITY_DIR = Path(
    r"C:\Users\binis\Stock_Prediction_app_push\research\output"
    r"\phase16a_sector_metadata_integrity_and_shadow_revalidation"
)

SHADOW_JSON = "phase16a_shadow_revalidation_report.json"
INTEGRITY_JSON = "phase16a_sector_integrity_report.json"
COVERAGE_JSON = "sector_metadata_coverage.json"

REBALANCE_TARGET_TRADING_DAYS = 63
# 63 trading days ~ 92 calendar days (matches the Phase 13-A next-rebalance approximation).
REBALANCE_CAL_DAYS_APPROX = 92

# Statuses. NONE of these approve live trading.
ST_CONTINUE = "PAPER_TEST_CONTINUE"
ST_CHECKPOINT = "PAPER_TEST_CHECKPOINT_DUE"
ST_REVALIDATE = "RESEARCH_REVALIDATION_REQUIRED"
ST_BLOCKED = "DATA_INTEGRITY_BLOCKED"
ALLOWED_STATUSES = (ST_CONTINUE, ST_CHECKPOINT, ST_REVALIDATE, ST_BLOCKED)

SHADOW_REVALIDATE = "RESEARCH_REVALIDATION_REQUIRED"

SAFETY_BADGES = ["PAPER ONLY", "MANUAL REVIEW", "NO BROKER EXECUTION", "AUTOMATION OFF", "NO LIVE ORDERS"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _num(x: Any) -> Optional[float]:
    if isinstance(x, bool) or x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v


def _resolve_integrity_dir(integrity_dir: Optional[Union[str, Path]]) -> Path:
    if integrity_dir is not None:
        return Path(integrity_dir)
    env = os.environ.get(INTEGRITY_DIR_ENV_VAR)
    if env:
        return Path(env)
    return DEFAULT_INTEGRITY_DIR


def _read_json(path: Path) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            obj = json.load(fh)
        return obj if isinstance(obj, dict) else None
    except (OSError, ValueError):
        return None


def _add_cal_days(iso: Optional[str], days: int) -> Optional[str]:
    if not iso:
        return None
    try:
        return date.fromordinal(date.fromisoformat(str(iso)[:10]).toordinal() + days).isoformat()
    except (ValueError, TypeError):
        return None


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
        "status_approves_live_trading": False,
        "creates_signals": False,
        "creates_trade_decisions": False,
        "creates_orders": False,
        "creates_fills": False,
        "wrote_to_database": False,
        "mutates_champion": False,
    }


_NEXT_ACTION = {
    ST_REVALIDATE: (
        "Run a FORMAL sector-repaired revalidation of composite_sn before the next quarterly "
        "rebalance: rebuild the sector-neutral composite over the repaired GICS sectors and re-run the "
        "full validation battery (IC t-stat, cost-adjusted net-25/50bps spread, cohort / subperiod / "
        "regime stability) against the frozen champion. Do NOT replace or promote the champion until it "
        "passes. Paper-only — no live trading."),
    ST_BLOCKED: (
        "Resolve the data-integrity block first: (re)run the Phase 16-A sector integrity + shadow "
        "revalidation so the reproduction guard passes and the artifacts are present, then re-read this "
        "gate. No decision is made while blocked."),
    ST_CHECKPOINT: (
        "The 63-trading-day paper horizon checkpoint is due: manually review the paper book's realised "
        "vs benchmark performance and the sector-integrity status, then decide on a rebalance REVIEW "
        "(no orders are placed here)."),
    ST_CONTINUE: (
        "Continue the paper test: keep refreshing the daily mark and monitoring the champion; the next "
        "formal review is the quarterly checkpoint. No trading action is required."),
}


# ---------------------------------------------------------------------------
# Public service
# ---------------------------------------------------------------------------

def load_current_alpha_integrity_gate(
    *,
    integrity_dir: Optional[Union[str, Path]] = None,
    gate: Optional[dict] = None,
    daily: Optional[dict] = None,
    operating: Optional[dict] = None,
    today: Optional[str] = None,
) -> dict[str, Any]:
    """Read-only Phase 16-A paper-test integrity gate.

    Composes the decision gate, daily status and canonical operating state with the committed Phase
    16-A sector-integrity + shadow-revalidation artifacts. Every dependency is wrapped so a single
    failure degrades to a ``warnings[]`` entry rather than failing the endpoint; the function never
    raises and never approves live trading. ``gate`` / ``daily`` / ``operating`` are injectable for
    deterministic tests.
    """
    warnings: list[str] = []
    idir = _resolve_integrity_dir(integrity_dir)

    # --- committed Phase 16-A research artifacts (the NEW integrity evidence) ------------
    shadow = _read_json(idir / SHADOW_JSON)
    integrity = _read_json(idir / INTEGRITY_JSON)
    coverage = _read_json(idir / COVERAGE_JSON)
    if shadow is None:
        warnings.append("Phase 16-A shadow revalidation report not found at %s — sector-integrity "
                        "evidence unavailable." % (idir / SHADOW_JSON))

    # --- existing read-only loaders ------------------------------------------------------
    if gate is None:
        try:
            gate = load_current_alpha_decision_gate(today=today)
        except Exception as exc:  # noqa: BLE001 - degrade, never fail the endpoint
            gate = {}
            warnings.append("decision gate unavailable: %s" % exc)
    if daily is None:
        try:
            daily = load_current_alpha_daily_status()
        except Exception as exc:  # noqa: BLE001
            daily = {}
            warnings.append("daily status unavailable: %s" % exc)
    if operating is None:
        try:
            from paper_trader.api.current_operating_state import load_current_operating_state
            operating = load_current_operating_state(gate=gate, daily_status=daily)
        except Exception as exc:  # noqa: BLE001
            operating = {}
            warnings.append("operating state unavailable: %s" % exc)

    gate = gate or {}
    daily = daily or {}
    operating = operating or {}

    # --- sector-shadow decision + rank/overlap metrics (from the 16-A artifacts) ----------
    shadow = shadow or {}
    sector_shadow_decision = shadow.get("decision")
    repro = shadow.get("reproduction") or {}
    reproduces_frozen = bool(repro.get("reproduces_frozen_composite"))
    lcs = shadow.get("latest_cross_section") or {}
    fp = shadow.get("full_panel") or {}
    champ_fp = fp.get("champion") or {}
    shad_fp = fp.get("shadow") or {}
    rank_overlap_metrics = {
        "full_panel_rank_spearman": _num(fp.get("rank_spearman_champion_vs_shadow")),
        "latest_month_rank_spearman": _num(lcs.get("rank_spearman_champion_vs_shadow")),
        "top25_overlap": _num(lcs.get("top25_overlap")),
        "top50_overlap": _num(lcs.get("top50_overlap")),
        "bottom25_overlap": _num(lcs.get("bottom25_overlap")),
        "top25_turnover": _num(lcs.get("top25_turnover")),
        "top50_turnover": _num(lcs.get("top50_turnover")),
        "champion_ic_t_stat": _num(champ_fp.get("ic_t_stat")),
        "shadow_ic_t_stat": _num(shad_fp.get("ic_t_stat")),
        "champion_net25_spread": _num(champ_fp.get("net25_spread")),
        "shadow_net25_spread": _num(shad_fp.get("net25_spread")),
        "shadow_top25_largest_sector_share_pct": _num(lcs.get("shadow_top25_largest_sector_share_pct")),
    }

    # --- sector metadata coverage (before vs after repair) --------------------------------
    scov = shadow.get("sector_coverage_summary") or {}
    sector_metadata_coverage = {
        "all234_before_pct": _num(scov.get("all234_before_pct")),
        "all234_after_pct": _num(scov.get("all234_after_pct")),
        "top25_before_pct": _num(scov.get("top25_before_pct")),
        "top25_after_pct": _num(scov.get("top25_after_pct")),
        "top50_before_pct": _num(scov.get("top50_before_pct")),
        "top50_after_pct": _num(scov.get("top50_after_pct")),
        "n_resolved": scov.get("n_resolved"),
        "n_unresolved": scov.get("n_unresolved"),
        "source": "owned EODHD fundamentals GicSector / Morningstar->GICS crosswalk (Phase 16-A)",
        "point_in_time": False,
    }

    # --- horizon progress (63 trading-day quarterly) --------------------------------------
    readiness = gate.get("quarterly_rebalance_readiness") or {}
    signal_date = gate.get("signal_date") or (shadow.get("phase13a_context") or {}).get("phase13a_signal_date")
    trading_days_elapsed = readiness.get("estimated_trading_days_elapsed")
    trading_days_remaining = readiness.get("remaining_trading_days")
    readiness_status = readiness.get("readiness_status")
    next_checkpoint_date = _add_cal_days(signal_date, REBALANCE_CAL_DAYS_APPROX)
    horizon_progress = {
        "target_holding_period_trading_days": REBALANCE_TARGET_TRADING_DAYS,
        "trading_days_elapsed": trading_days_elapsed,
        "trading_days_remaining": trading_days_remaining,
        "readiness_status": readiness_status,
        "next_formal_checkpoint_date": next_checkpoint_date,
        "next_formal_checkpoint_basis": "signal_date + ~92 calendar days (~63 trading days)",
        "cadence": "QUARTERLY_63_TRADING_DAYS",
    }

    # --- current daily-mark coverage (counts) vs initial entry-price coverage -------------
    ds25 = daily.get("top25") or {}
    ds50 = daily.get("top50") or {}
    current_daily_mark_coverage = {
        "top25_covered": ds25.get("covered_count"),
        "top25_total": ds25.get("total_count"),
        "top50_covered": ds50.get("covered_count"),
        "top50_total": ds50.get("total_count"),
        "as_of_mark_date": daily.get("latest_valid_mark_date"),
        "label": "CURRENT DAILY-MARK COVERAGE",
        "note": "Latest daily refresh coverage — how many held names have a fresh completed EOD mark.",
    }
    init_cov = (shadow.get("phase13a_context") or {}).get("phase13a_price_coverage") or {}
    initial_entry_price_coverage = {
        "top25_covered": init_cov.get("top25"),
        "top25_total": 25,
        "top50_covered": init_cov.get("top50"),
        "top50_total": 50,
        "label": "INITIAL ENTRY-PRICE COVERAGE",
        "as_of": "Phase 13-A package initialization (frozen)",
        "note": "Frozen Phase 13-A entry-price coverage when the book was first initialized — NOT a "
                "current missing-price warning.",
    }

    # --- current Top25/Top50/SPY performance (current operating mark) ----------------------
    spy = daily.get("spy_benchmark") or {}
    performance = {
        "as_of_mark_date": daily.get("latest_valid_mark_date"),
        "top25_return_pct": _num(ds25.get("average_return_pct")),
        "top25_excess_return_pct_points": _num(ds25.get("excess_return_vs_spy_pct_points")),
        "top50_return_pct": _num(ds50.get("average_return_pct")),
        "top50_excess_return_pct_points": _num(ds50.get("excess_return_vs_spy_pct_points")),
        "spy_return_pct": _num(spy.get("return_since_signal_pct")),
        "basis": "since-signal daily operating mark (current, NOT the 13-I reconstruction window)",
    }

    # --- current operating mark + historical-evidence end date ----------------------------
    current_operating_mark_date = (operating.get("current_mark_date")
                                   or daily.get("latest_valid_mark_date") or gate.get("latest_mark_date"))
    historical_evidence_end_date = (operating.get("history_end_date")
                                    or (repro.get("note") and None) or gate.get("latest_mark_date"))

    # --- risk flags -----------------------------------------------------------------------
    risk_review = gate.get("risk_review") or {}
    risk_flags: list[str] = []
    if risk_review.get("any_breach"):
        risk_flags.append("A paper-review risk threshold is breached — see the decision gate risk triggers.")
    mf = (gate.get("mark_freshness") or {}).get("mark_freshness_status")
    if isinstance(mf, str) and mf.startswith("STALE"):
        risk_flags.append("Latest financial mark is stale (%s) — refresh the daily mark." % mf)
    if lcs.get("shadow_top25_largest_sector_share_pct") is not None and \
            _num(lcs.get("shadow_top25_largest_sector_share_pct")) > 30:
        risk_flags.append("Under repaired sectors the Top25 book shows a single-sector concentration "
                          "of %.0f%% (>30%%)." % _num(lcs.get("shadow_top25_largest_sector_share_pct")))

    # --- status resolution (NONE approves live trading) -----------------------------------
    data_blocked = (not shadow) or (not reproduces_frozen and shadow.get("reproduction") is not None)
    if not shadow:
        status = ST_BLOCKED
    elif not reproduces_frozen:
        status = ST_BLOCKED
    elif sector_shadow_decision == SHADOW_REVALIDATE:
        status = ST_REVALIDATE
    elif isinstance(readiness_status, str) and readiness_status in ("READY_DUE", "READY_OVERDUE"):
        status = ST_CHECKPOINT
    elif isinstance(trading_days_remaining, int) and trading_days_remaining <= 0:
        status = ST_CHECKPOINT
    else:
        status = ST_CONTINUE

    # --- blockers -------------------------------------------------------------------------
    blockers: list[str] = []
    if status == ST_BLOCKED:
        blockers.append("Phase 16-A sector-integrity evidence is missing or fails its reproduction "
                        "guard; no paper-test decision can be made until it is rebuilt.")
    if sector_shadow_decision == SHADOW_REVALIDATE:
        for r in (shadow.get("decision_reasons") or []):
            blockers.append("Sector shadow: %s" % r)
    blockers.extend(risk_flags)

    payload = {
        "phase": PHASE,
        "status": status,
        "status_label": status.replace("_", " ").title(),
        "champion": CHAMPION_SIGNAL,
        "champion_role": "CURRENT PAPER CHAMPION",
        "current_operating_mark_date": current_operating_mark_date,
        "historical_evidence_end_date": historical_evidence_end_date,
        "signal_date": signal_date,
        # coverage — the two concepts kept explicitly separate (Part D)
        "current_daily_mark_coverage": current_daily_mark_coverage,
        "initial_entry_price_coverage": initial_entry_price_coverage,
        "sector_metadata_coverage": sector_metadata_coverage,
        # sector shadow revalidation (Part F)
        "sector_shadow_decision": sector_shadow_decision,
        "sector_shadow_reasons": shadow.get("decision_reasons") or [],
        "sector_shadow_reproduces_frozen_composite": reproduces_frozen,
        "rank_correlation_and_overlap_metrics": rank_overlap_metrics,
        # horizon + performance + risk
        "horizon_progress": horizon_progress,
        "current_performance": performance,
        "risk_flags": risk_flags,
        "blockers": blockers,
        "next_recommended_research_action": _NEXT_ACTION.get(status, _NEXT_ACTION[ST_CONTINUE]),
        # explicit no-live-trading status (Part G)
        "live_trading_status": "NOT_APPROVED_FOR_LIVE_TRADING",
        "no_status_approves_live_trading": True,
        "decision_gate_status": gate.get("status"),
        "decision_gate_decision": gate.get("decision"),
        "provenance": {
            "phase16a_artifacts_dir": str(idir),
            "shadow_report_found": bool(shadow),
            "integrity_report_found": bool(integrity),
            "coverage_report_found": bool(coverage),
            "decision_gate_status": gate.get("status"),
            "daily_status": daily.get("status"),
            "sector_labels_point_in_time": False,
            "sources": [SHADOW_JSON, INTEGRITY_JSON, COVERAGE_JSON,
                        "load_current_alpha_decision_gate", "load_current_alpha_daily_status",
                        "load_current_operating_state"],
        },
        "loaded_at": _iso_now(),
        "warnings": warnings,
        "data_integrity_blocked": bool(data_blocked),
    }
    payload.update(_safety_block())
    return payload


__all__ = [
    "load_current_alpha_integrity_gate",
    "SAFETY_BADGES",
    "ALLOWED_STATUSES",
    "INTEGRITY_DIR_ENV_VAR",
    "REBALANCE_TARGET_TRADING_DAYS",
]
