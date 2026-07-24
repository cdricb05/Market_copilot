"""api/calibration_study.py — Phase 27H Part F: RESEARCH-ONLY calibration diagnostics.

A strictly read-only, research-only diagnostic that compares the operational fixed
fundamental/momentum blend against alternative fixed configurations. It NEVER changes,
promotes, reweights or replaces the operational model, champion or sleeve; it never
touches the operational book, the desk, the daily close or any order path; and it
performs NO writes.

What it CAN compute deterministically from the owned current cross-section (the frozen
Phase 25 engine ``build_current`` — point-in-time by construction): for each candidate
fixed blend it forms the Top-25 by ``w_f * fund_percentile + w_m * mom_percentile`` and
reports the membership overlap and one-way turnover versus the operational 50/50 primary.
This is a current-cross-section *structure* diagnostic — it shows how sensitive the target
membership is to the blend weight — NOT a forward, cost-adjusted performance conclusion.

What it explicitly DOES NOT do here: a proper walk-forward, point-in-time, survivorship-
safe, transaction-cost-adjusted, regime-analysed backtest. That requires the research-side
historical panel + backtest harness and, above all, SUFFICIENT FORWARD EVIDENCE. With the
live operational book only days old, any forward performance conclusion would be
statistically meaningless, so the study emits ``forward_evidence_status =
INSUFFICIENT_FORWARD_SAMPLE`` and a hard ``operational_recommendation = NO_OPERATIONAL_CHANGE``.
No parameter is ever selected from a single day's result.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from paper_trader.api import multi_horizon_engine as eng

PHASE = "27H"

TARGET_SIZE = 25

# Candidate FIXED blends studied (weights are fund/mom). The operational blend is 50/50.
CANDIDATE_BLENDS = [
    ("fund30_mom70", 0.30, 0.70),
    ("fund40_mom60", 0.40, 0.60),
    ("fund50_mom50", 0.50, 0.50),   # the operational blend
    ("fund60_mom40", 0.60, 0.40),
    ("fund70_mom30", 0.70, 0.30),
]
OPERATIONAL_BLEND = "fund50_mom50"

# Research dimensions enumerated but NOT concluded here (need the research walk-forward
# harness + sufficient forward evidence). Each is described, never scored on live data.
_RESEARCH_DIMENSIONS = [
    {"dimension": "fixed_blend", "operational": "50/50 fundamental/momentum",
     "candidates": ["30/70", "40/60", "60/40", "70/30"],
     "diagnostic_available": True,
     "note": "Current-cross-section membership sensitivity is computed below; a forward "
             "cost-adjusted conclusion requires the research walk-forward harness."},
    {"dimension": "position_sizing", "operational": "equal weight",
     "candidates": ["volatility-aware (inverse-vol) sizing"],
     "diagnostic_available": False,
     "note": "Requires a walk-forward risk-adjusted comparison; not concluded on live data."},
    {"dimension": "sector_construction", "operational": "25% sector cap",
     "candidates": ["hard sector caps", "sector-neutral ranking", "sector risk budgets"],
     "diagnostic_available": False,
     "note": "Requires a walk-forward comparison with point-in-time sectors."},
    {"dimension": "materiality_threshold", "operational": "3% one-way turnover floor",
     "candidates": ["1.5% floor", "5% floor"],
     "diagnostic_available": False,
     "note": "Turnover/churn vs realized benefit needs forward evidence."},
    {"dimension": "defensive_overlay", "operational": "monitor-only vol/drawdown",
     "candidates": ["beta-sensitive overlay", "drawdown-sensitive overlay"],
     "diagnostic_available": False,
     "note": "Requires a regime-aware walk-forward study."},
]


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _safety() -> dict:
    return {
        "research_only": True,
        "operational_promotion": False,
        "modifies_operational_model": False,
        "model_parameters_changed": False,
        "champion_replaced": False,
        "fast_sleeve_active": False,
        "performed_write": False,
        "read_only": True,
        "creates_orders": False,
        "broker_enabled": False,
        "automation_enabled": False,
        "prediction_service_used": False,
        "safety_badges": ["RESEARCH ONLY", "NO OPERATIONAL CHANGE", "NO PROMOTION",
                          "PREVIEW ONLY", "NO ORDERS", "AUTOMATION OFF"],
    }


def _blend_top(combined: dict, w_f: float, w_m: float, size: int) -> list[str]:
    """Deterministic Top-N by a fixed percentile blend of the owned current cross-section.
    Point-in-time by construction (uses only the current cross-section percentiles)."""
    scored = []
    for tk, row in (combined or {}).items():
        fp = row.get("fund_percentile")
        mp = row.get("mom_percentile")
        if fp is None or mp is None:
            continue
        scored.append((tk, w_f * float(fp) + w_m * float(mp)))
    scored.sort(key=lambda t: (-t[1], t[0]))
    return [tk for tk, _ in scored[:size]]


def load_calibration_study(*, panel_path=None, inputs_dir=None, size: int = TARGET_SIZE) -> dict:
    """Read-only research-only calibration diagnostic. Never writes, never promotes."""
    warnings: list[str] = []
    try:
        cur = eng.build_current(panel_path=panel_path, inputs_dir=inputs_dir)
    except Exception as exc:  # noqa: BLE001
        cur = {"status": "MHZ_ERROR"}
        warnings.append("Engine current unavailable: %s" % str(exc)[:160])

    ready = cur.get("status") == eng.STATUS_READY
    if not ready:
        return {
            "status": "CALIBRATION_STUDY_UNAVAILABLE",
            "phase": PHASE,
            "reason": "The owned model cross-section is not available (%s)." % cur.get("status"),
            "research_dimensions": _RESEARCH_DIMENSIONS,
            "forward_evidence_status": "INSUFFICIENT_FORWARD_SAMPLE",
            "operational_recommendation": "NO_OPERATIONAL_CHANGE",
            "warnings": warnings, "generated_at": _now_iso(), **_safety(),
        }

    combined = (cur.get("combined") or {}).get("combined") or {}
    market_date = cur.get("market_as_of_date")
    operational_top = set(_blend_top(combined, 0.5, 0.5, size))

    blends = []
    for name, w_f, w_m in CANDIDATE_BLENDS:
        top = _blend_top(combined, w_f, w_m, size)
        top_set = set(top)
        overlap = len(top_set & operational_top)
        # one-way turnover vs the operational blend = |symmetric difference| / 2 / size
        turnover = (len(top_set ^ operational_top) / 2.0 / size) if size else None
        blends.append({
            "blend": name,
            "fund_weight": w_f,
            "momentum_weight": w_m,
            "is_operational": name == OPERATIONAL_BLEND,
            "top_count": len(top),
            "overlap_with_operational": overlap,
            "overlap_fraction": (round(overlap / size, 4) if size else None),
            "one_way_turnover_vs_operational": (round(turnover, 4) if turnover is not None else None),
            "entrants_vs_operational": sorted(top_set - operational_top)[:25],
            "leavers_vs_operational": sorted(operational_top - top_set)[:25],
        })

    return {
        "status": "CALIBRATION_STUDY_READY",
        "phase": PHASE,
        "market_as_of_date": market_date,
        "fundamental_as_of_date": cur.get("fundamental_as_of_date"),
        "target_size": size,
        "operational_blend": OPERATIONAL_BLEND,
        "blend_membership_sensitivity": blends,
        "research_dimensions": _RESEARCH_DIMENSIONS,
        # The crucial honesty guards.
        "forward_evidence_status": "INSUFFICIENT_FORWARD_SAMPLE",
        "forward_evidence_note": (
            "This diagnostic reports current-cross-section membership sensitivity only. A "
            "cost-adjusted, walk-forward, point-in-time, survivorship-safe performance "
            "comparison across these configurations requires the research backtest harness "
            "AND enough forward observations to be statistically meaningful. Neither exists "
            "yet, so no configuration is preferred and nothing is promoted."),
        "operational_recommendation": "NO_OPERATIONAL_CHANGE",
        "safeguards": {
            "no_lookahead": True,
            "point_in_time_cross_section": True,
            "survivorship_safe_required_for_conclusion": True,
            "transaction_costs_required_for_conclusion": True,
            "walk_forward_required_for_conclusion": True,
            "holdout_required_for_conclusion": True,
            "no_parameter_selected_from_single_day": True,
        },
        "warnings": warnings,
        "generated_at": _now_iso(),
        **_safety(),
    }


__all__ = ["PHASE", "CANDIDATE_BLENDS", "OPERATIONAL_BLEND", "TARGET_SIZE",
           "load_calibration_study"]
