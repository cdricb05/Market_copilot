"""api/multi_horizon_registry.py - Phase 25 versioned model + sleeve contract (Track A A1/A5).

The multi-horizon platform must never pretend the slow/medium models are daily-trading models.
This module is the durable, deterministic CONTRACT layer that makes each model's cadence explicit:
every registered model carries its exact formula, required inputs, universe, PIT rules, and - crucially -
its FIVE separate frequencies (observation / signal-horizon / holding / evaluation / rebalance), its
next-review rule, its transaction-cost assumption, its validation evidence, its correlation cluster,
its deployment status and its daily actionability.

It is pure stdlib (no numpy/pandas, no IO, no network) so it is trivially testable and cannot leak a
credential or write a file.  The dynamic per-sleeve STATE (last calculation date, next review date,
review-due flags) is computed by ``multi_horizon_engine`` on top of these static contracts.

Deployment statuses (exactly the Phase 25 vocabulary):
    PAPER_CHAMPION | PAPER_CHALLENGER | RESEARCH_REFERENCE | INFORMATION_ONLY_NOT_TRADABLE
    | REJECTED | ARCHIVED

Safety: this platform is paper-only, preview-only, manual-review-only.  Nothing here approves live
trading, creates orders/fills/signals/trade-decisions, connects a broker, runs automation, replaces
the champion, or calls the prediction service.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Optional

PHASE = "25"

# --------------------------------------------------------------------------- #
# Deployment status vocabulary (Phase 25)
# --------------------------------------------------------------------------- #
STATUS_PAPER_CHAMPION = "PAPER_CHAMPION"
STATUS_PAPER_CHALLENGER = "PAPER_CHALLENGER"
STATUS_RESEARCH_REFERENCE = "RESEARCH_REFERENCE"
STATUS_INFO_ONLY = "INFORMATION_ONLY_NOT_TRADABLE"
STATUS_REJECTED = "REJECTED"
STATUS_ARCHIVED = "ARCHIVED"

ALL_STATUSES = [
    STATUS_PAPER_CHAMPION, STATUS_PAPER_CHALLENGER, STATUS_RESEARCH_REFERENCE,
    STATUS_INFO_ONLY, STATUS_REJECTED, STATUS_ARCHIVED,
]
STATUS_CLASS = {
    STATUS_PAPER_CHAMPION: "safe",
    STATUS_PAPER_CHALLENGER: "manual",
    STATUS_RESEARCH_REFERENCE: "muted",
    STATUS_INFO_ONLY: "danger",
    STATUS_REJECTED: "danger",
    STATUS_ARCHIVED: "muted",
}

# --------------------------------------------------------------------------- #
# Cadence vocabulary + review-date arithmetic (stdlib date math, no imports of
# the calendar beyond the standard library's date object used by the engine).
# --------------------------------------------------------------------------- #
CADENCE_DAILY = "daily"
CADENCE_WEEKLY = "weekly"
CADENCE_MONTHLY = "monthly"
CADENCE_QUARTERLY = "quarterly"
CADENCE_MANUAL = "manual"

# Actionability labels (daily): what a review of this sleeve would normally yield today.
ACT_HOLD = "HOLD"
ACT_WAIT = "WAIT"
ACT_REVIEW_DUE = "REVIEW_DUE"
ACT_MONITOR_ONLY = "MONITOR_ONLY"
ACT_INACTIVE = "INACTIVE"

# Safety classification per model.
SAFE_TRADABLE_PAPER = "TRADABLE_PAPER_ONLY"          # eligible to generate paper recommendations
SAFE_DIAGNOSTIC_ONLY = "DIAGNOSTIC_ONLY"             # risk cluster: cannot create BUY/EXIT
SAFE_BLOCKED = "BLOCKED_FROM_RECOMMENDATIONS"        # e.g. reversal - information only

SAFETY_BADGES = ["PAPER ONLY", "ORDERS DISABLED", "AUTOMATION OFF", "MANUAL REVIEW", "NO LIVE PROMOTION"]

# Fast-sleeve sentinel (until Track B validates a fast alpha).
NO_VALIDATED_FAST_ALPHA = "NO_VALIDATED_FAST_ALPHA"


def _fp(obj: Any) -> str:
    """Deterministic short reproducibility fingerprint of a JSON-able contract fragment."""
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()[:16]


# --------------------------------------------------------------------------- #
# The versioned model contracts (A1). One record per model, fixed schema.
# --------------------------------------------------------------------------- #
def _model(
    *, model_id, model_version, display_name, family, formula, required_inputs, universe,
    pit_rules, observation_frequency, signal_horizon, expected_holding_period, rebalance_frequency,
    evaluation_frequency, next_manual_review, transaction_cost_assumption, eligibility_filters,
    validation_evidence, source_phase, source_commit, correlation_cluster, deployment_status,
    actionability, safety_classification, blocked_from_recommendations=False, notes=None,
) -> dict:
    contract = {
        "model_id": model_id,
        "model_version": model_version,
        "display_name": display_name,
        "family": family,
        "formula": formula,
        "required_inputs": list(required_inputs),
        "universe": universe,
        "pit_rules": pit_rules,
        "observation_frequency": observation_frequency,
        "signal_horizon": signal_horizon,
        "expected_holding_period": expected_holding_period,
        "rebalance_frequency": rebalance_frequency,
        "evaluation_frequency": evaluation_frequency,
        "next_manual_review": next_manual_review,
        "transaction_cost_assumption": transaction_cost_assumption,
        "eligibility_filters": list(eligibility_filters),
        "validation_evidence": validation_evidence,
        "source_phase": source_phase,
        "source_commit": source_commit,
        "correlation_cluster": correlation_cluster,
        "deployment_status": deployment_status,
        "status_class": STATUS_CLASS.get(deployment_status, "warn"),
        "actionability": actionability,
        "safety_classification": safety_classification,
        "blocked_from_recommendations": bool(blocked_from_recommendations),
        "notes": notes,
    }
    # reproducibility fingerprint over the deterministic definition (not the evidence text)
    contract["reproducibility_fingerprint"] = _fp({
        "model_id": model_id, "model_version": model_version, "formula": formula,
        "required_inputs": sorted(required_inputs), "universe": universe,
        "rebalance_frequency": rebalance_frequency, "signal_horizon": signal_horizon,
        "eligibility_filters": sorted(eligibility_filters)})
    return contract


def model_registry() -> list[dict]:
    """The five (+1 combined) durable model contracts. Deterministic and side-effect free."""
    return [
        _model(
            model_id="composite_sn", model_version="v1",
            display_name="Quality / cash-flow composite (sector-neutral)",
            family="QUALITY",
            formula="Equal-weight sector-neutral z of FCF/assets + operating accruals: "
                    "composite_sn = fcf_to_assets_sector_neutral_z + operating_accruals_sector_neutral_z.",
            required_inputs=["frozen Phase 10-L sector-neutral scored panel (composite_sn, sector, "
                             "liquidity_proxy, forward_63d_return)"],
            universe="S&P 500 multifactor (owned frozen Phase 10-L scored panel)",
            pit_rules="Fundamentals as-of the panel rebalance date; forward return measured after; one "
                      "representative row per (month,ticker); no future fundamentals.",
            observation_frequency="when fundamentals refresh (quarterly cadence)",
            signal_horizon="~63 trading days (slow)",
            expected_holding_period="~1 quarter",
            rebalance_frequency=CADENCE_QUARTERLY,
            evaluation_frequency=CADENCE_QUARTERLY,
            next_manual_review="next quarter boundary or when the owned fundamental panel refreshes",
            transaction_cost_assumption="25 bps round-trip",
            eligibility_filters=["valid composite_sn", "known sector", "liquidity_proxy present",
                                 "minimum names per cross-section"],
            validation_evidence={"newey_west_t": 2.93, "net25": 0.0102, "horizon_days": 63,
                                 "source": "Phase 17 revalidation (committed)"},
            source_phase="10-D / 17", source_commit="0266d4b",
            correlation_cluster="fundamental_slow",
            deployment_status=STATUS_PAPER_CHAMPION,
            actionability="Normally HOLD or WAIT (quarterly cadence).",
            safety_classification=SAFE_TRADABLE_PAPER,
            notes="Current paper champion. Never replaced automatically."),
        _model(
            model_id="mom_6_1", model_version="v1",
            display_name="6-1 price momentum (skip most recent month)",
            family="MOMENTUM",
            formula="mom_6_1 = close[m-1]/close[m-7]-1 on month-end TOTAL-RETURN closes "
                    "(~6 months momentum, skipping the most recent month).",
            required_inputs=["owned survivorship-free daily total-return panel (Phase 24 NPZ, Norgate "
                             "Russell 1000 Current & Past), month-end resampled"],
            universe="Russell 1000 point-in-time members (survivorship-free)",
            pit_rules="mom_6_1 for month m uses only closes through m-1; membership as-of m; forward "
                      "return m->m+1; delisted names retained in history.",
            observation_frequency="daily price refresh allowed (signal recomputed monthly)",
            signal_horizon="~63 trading days (medium)",
            expected_holding_period="~1-3 months",
            rebalance_frequency=CADENCE_MONTHLY,
            evaluation_frequency=CADENCE_MONTHLY,
            next_manual_review="next month boundary",
            transaction_cost_assumption="25 bps round-trip",
            eligibility_filters=["current PIT membership", "eligible trailing history (>=120/126 days)",
                                 "not an extreme corporate-action artifact (|mom|<=3)",
                                 "minimum dollar-liquidity"],
            validation_evidence={"mean_rank_ic": 0.0384, "ic_t": 6.65, "newey_west_t": 4.96,
                                 "positive_ic_months": 0.66, "net25": 0.0296, "holdout_net25": 0.0415,
                                 "corr_vs_composite_sn": 0.12,
                                 "source": "Phase 22 survivorship-free validation (committed)"},
            source_phase="22", source_commit="4b3089b",
            correlation_cluster="momentum_medium",
            deployment_status=STATUS_PAPER_CHALLENGER,
            actionability="Usually HOLD or WAIT (monthly cadence).",
            safety_classification=SAFE_TRADABLE_PAPER,
            notes="Paper challenger. Diversifies the champion (corr ~0.12)."),
        _model(
            model_id="fundamental_momentum_50_50_v1", model_version="v1",
            display_name="Fundamental + Momentum 50/50 combined",
            family="COMPOSITE",
            formula="Equal (50/50) blend of the within-universe cross-sectional PERCENTILE ranks of "
                    "composite_sn and mom_6_1 over the common eligible universe.",
            required_inputs=["composite_sn (frozen panel)", "mom_6_1 (owned daily panel)"],
            universe="Common eligible universe = fundamental panel names that are current PIT momentum members",
            pit_rules="Both legs point-in-time as above; ranks computed only within the common eligible "
                      "cross-section for the current period.",
            observation_frequency="daily price refresh allowed; fundamental leg refreshes quarterly",
            signal_horizon="~63 trading days (medium)",
            expected_holding_period="~1-3 months",
            rebalance_frequency=CADENCE_MONTHLY,
            evaluation_frequency=CADENCE_MONTHLY,
            next_manual_review="next month boundary",
            transaction_cost_assumption="25 bps round-trip",
            eligibility_filters=["in both legs' eligible universes", "known sector", "minimum dollar-liquidity"],
            validation_evidence={"construction": "fixed 50/50 rank blend (NOT weight-optimized)",
                                 "components": {"composite_sn": 0.5, "mom_6_1": 0.5},
                                 "sensitivity_views": ["30/70 fundamental/momentum", "70/30 fundamental/momentum"],
                                 "source": "Phase 25 (this phase) - historical reconstruction below"},
            source_phase="25", source_commit=None,
            correlation_cluster="fundamental_slow + momentum_medium",
            deployment_status=STATUS_PAPER_CHALLENGER,
            actionability="Primary paper portfolio. Usually HOLD or WAIT (monthly cadence).",
            safety_classification=SAFE_TRADABLE_PAPER,
            notes="PRIMARY combined model. Fixed weights; sensitivity views never silently replace it."),
        _model(
            model_id="lowvol_12m_sn", model_version="v1",
            display_name="12-month low realized volatility (sector-neutral)",
            family="VOLATILITY",
            formula="Sector-neutral z of negative trailing 12-month realized volatility (lower vol = higher score).",
            required_inputs=["owned daily total-return panel (realized volatility)"],
            universe="Russell 1000 point-in-time members",
            pit_rules="Trailing realized vol through t; membership as-of t.",
            observation_frequency="daily observation allowed",
            signal_horizon="risk diagnostic (not a return horizon)",
            expected_holding_period="n/a (diagnostic)",
            rebalance_frequency=CADENCE_DAILY,
            evaluation_frequency=CADENCE_MONTHLY,
            next_manual_review="risk refresh only",
            transaction_cost_assumption="n/a (diagnostic)",
            eligibility_filters=["current PIT membership", "sufficient vol history"],
            validation_evidence={"role": "risk diagnostic / concentration control / tie-breaker",
                                 "note": "regime-fragile as a standalone return alpha (Phase 23)",
                                 "source": "Phase 23"},
            source_phase="23", source_commit="041c256",
            correlation_cluster="lowvol_defensive",
            deployment_status=STATUS_RESEARCH_REFERENCE,
            actionability="MONITOR_ONLY (diagnostic).",
            safety_classification=SAFE_DIAGNOSTIC_ONLY,
            notes="Defensive risk cluster - NOT an equal-confidence return alpha. Cannot create BUY/EXIT."),
        _model(
            model_id="ivol_spy_sn", model_version="v1",
            display_name="Idiosyncratic volatility vs SPY (sector-neutral)",
            family="VOLATILITY",
            formula="Sector-neutral z of negative residual (idiosyncratic) volatility from a market "
                    "(SPY/universe) regression.",
            required_inputs=["owned daily total-return panel", "market proxy returns"],
            universe="Russell 1000 point-in-time members",
            pit_rules="Trailing residual vol through t; membership as-of t.",
            observation_frequency="daily observation allowed",
            signal_horizon="risk diagnostic (not a return horizon)",
            expected_holding_period="n/a (diagnostic)",
            rebalance_frequency=CADENCE_DAILY,
            evaluation_frequency=CADENCE_MONTHLY,
            next_manual_review="risk refresh only",
            transaction_cost_assumption="n/a (diagnostic)",
            eligibility_filters=["current PIT membership", "sufficient history for the regression"],
            validation_evidence={"role": "risk diagnostic / defensive exposure / sensitivity",
                                 "source": "Phase 23"},
            source_phase="23", source_commit="041c256",
            correlation_cluster="lowvol_defensive",
            deployment_status=STATUS_RESEARCH_REFERENCE,
            actionability="MONITOR_ONLY (diagnostic).",
            safety_classification=SAFE_DIAGNOSTIC_ONLY,
            notes="Defensive risk cluster - NOT an equal-confidence return alpha. Cannot create BUY/EXIT."),
        _model(
            model_id="short_reversal_close_to_close", model_version="v1",
            display_name="Short-horizon close-to-close reversal",
            family="MEAN_REVERSION",
            formula="Negative prior 1-5 day total return (cross-sectional reversal).",
            required_inputs=["owned daily total-return panel"],
            universe="Russell 1000 point-in-time members (survivorship-free)",
            pit_rules="Reversal known at t; forward from t+1.",
            observation_frequency="daily",
            signal_horizon="1-5 trading days (fast)",
            expected_holding_period="1-10 trading days",
            rebalance_frequency=CADENCE_DAILY,
            evaluation_frequency=CADENCE_DAILY,
            next_manual_review="n/a - blocked from recommendations",
            transaction_cost_assumption="25 bps round-trip (fails it)",
            eligibility_filters=["n/a - blocked"],
            validation_evidence={"daily_ic_t": 8.4, "turnover": 0.85, "breakeven_bps": 2.3,
                                 "net25": "negative", "holdout_net25": "negative",
                                 "verdict": "INFORMATION_ONLY_NOT_TRADABLE",
                                 "source": "Phase 24 (committed ed28bb5)"},
            source_phase="24", source_commit="ed28bb5",
            correlation_cluster="fast_reversal_information_only",
            deployment_status=STATUS_INFO_ONLY,
            actionability="INACTIVE (blocked from recommendations).",
            safety_classification=SAFE_BLOCKED,
            blocked_from_recommendations=True,
            notes="Cost-killed at 25 bps. Explicitly BLOCKED from recommendation generation. Do not tune again."),
    ]


def model_by_id(model_id: str) -> Optional[dict]:
    for m in model_registry():
        if m["model_id"] == model_id:
            return m
    return None


def recommendation_eligible_model_ids() -> list[str]:
    """Model ids allowed to generate paper recommendations (excludes diagnostics + blocked reversal)."""
    return [m["model_id"] for m in model_registry()
            if m["safety_classification"] == SAFE_TRADABLE_PAPER
            and not m["blocked_from_recommendations"]]


# --------------------------------------------------------------------------- #
# Sleeve contracts (A5). Dynamic state (last calc / next review) is attached by
# the engine; here we define the static sleeve identities.
# --------------------------------------------------------------------------- #
SLEEVE_FUNDAMENTAL = "fundamental"
SLEEVE_MOMENTUM = "momentum"
SLEEVE_COMBINED = "combined"
SLEEVE_DEFENSIVE = "defensive_risk_overlay"
SLEEVE_FAST = "fast"


def sleeve_registry() -> list[dict]:
    """The five sleeve identities. The combined sleeve is the primary portfolio."""
    return [
        {"sleeve_id": SLEEVE_FUNDAMENTAL, "display_name": "Fundamental sleeve",
         "active_model": "composite_sn", "cadence": CADENCE_QUARTERLY,
         "observation_frequency": "quarterly (when fundamentals refresh)",
         "horizon": "slow (~63d)", "action_generation_enabled": True, "is_primary": False,
         "role": "Slow quality/cash-flow sleeve.", "safety_classification": SAFE_TRADABLE_PAPER},
        {"sleeve_id": SLEEVE_MOMENTUM, "display_name": "Momentum sleeve",
         "active_model": "mom_6_1", "cadence": CADENCE_MONTHLY,
         "observation_frequency": "daily price refresh allowed",
         "horizon": "medium (~63d)", "action_generation_enabled": True, "is_primary": False,
         "role": "Medium momentum sleeve.", "safety_classification": SAFE_TRADABLE_PAPER},
        {"sleeve_id": SLEEVE_COMBINED, "display_name": "Combined sleeve (PRIMARY)",
         "active_model": "fundamental_momentum_50_50_v1", "cadence": CADENCE_MONTHLY,
         "observation_frequency": "daily price refresh allowed; fundamentals quarterly",
         "horizon": "medium (~63d)", "action_generation_enabled": True, "is_primary": True,
         "role": "Primary paper portfolio: fixed 50/50 fundamental+momentum.",
         "safety_classification": SAFE_TRADABLE_PAPER},
        {"sleeve_id": SLEEVE_DEFENSIVE, "display_name": "Defensive risk overlay",
         "active_model": "lowvol_12m_sn", "cadence": CADENCE_DAILY,
         "observation_frequency": "daily observation allowed",
         "horizon": "risk diagnostic", "action_generation_enabled": False, "is_primary": False,
         "role": "Diagnostic only. Cannot independently create BUY or EXIT recommendations.",
         "safety_classification": SAFE_DIAGNOSTIC_ONLY},
        {"sleeve_id": SLEEVE_FAST, "display_name": "Fast sleeve",
         "active_model": None, "cadence": CADENCE_DAILY,
         "observation_frequency": "daily",
         "horizon": "fast (1-10d)", "action_generation_enabled": False, "is_primary": False,
         "role": "Inactive unless Track B validates a net-tradable fast model.",
         "safety_classification": SAFE_BLOCKED, "fast_status": NO_VALIDATED_FAST_ALPHA},
    ]


def sleeve_by_id(sleeve_id: str) -> Optional[dict]:
    for s in sleeve_registry():
        if s["sleeve_id"] == sleeve_id:
            return s
    return None


# --------------------------------------------------------------------------- #
# Safety block (A10 API fields + UI badges)
# --------------------------------------------------------------------------- #
def safety_block(validated_fast_alpha_available: bool = False) -> dict:
    """The read-only, paper-only safety block attached to every Phase 25 payload."""
    return {
        "paper_only": True,
        "orders_enabled": False,
        "automation_enabled": False,
        "champion_replaced": False,
        "validated_fast_alpha_available": bool(validated_fast_alpha_available),
        "safety_badges": list(SAFETY_BADGES),
        "preview_only": True,
        "read_only": True,
        "manual_review_only": True,
        "no_orders": True,
        "no_broker": True,
        "no_fills": True,
        "no_automation": True,
        "no_scheduled_jobs": True,
        "no_auto_rebalance": True,
        "no_live_promotion": True,
        "no_champion_replacement": True,
        "creates_signals": False,
        "creates_trade_decisions": False,
        "creates_orders": False,
        "creates_fills": False,
        "calls_prediction_service": False,
        "wrote_to_database": False,
        "wrote_to_trading_workflow": False,
        "live_trading_status": "NOT_APPROVED_FOR_LIVE_TRADING",
    }


def registry_counts(models: list[dict]) -> dict:
    counts = {s: 0 for s in ALL_STATUSES}
    for m in models:
        s = m.get("deployment_status")
        if s in counts:
            counts[s] += 1
    counts["total"] = len(models)
    return counts


__all__ = [
    "PHASE",
    "STATUS_PAPER_CHAMPION", "STATUS_PAPER_CHALLENGER", "STATUS_RESEARCH_REFERENCE",
    "STATUS_INFO_ONLY", "STATUS_REJECTED", "STATUS_ARCHIVED", "ALL_STATUSES", "STATUS_CLASS",
    "CADENCE_DAILY", "CADENCE_WEEKLY", "CADENCE_MONTHLY", "CADENCE_QUARTERLY", "CADENCE_MANUAL",
    "ACT_HOLD", "ACT_WAIT", "ACT_REVIEW_DUE", "ACT_MONITOR_ONLY", "ACT_INACTIVE",
    "SAFE_TRADABLE_PAPER", "SAFE_DIAGNOSTIC_ONLY", "SAFE_BLOCKED", "SAFETY_BADGES",
    "NO_VALIDATED_FAST_ALPHA",
    "SLEEVE_FUNDAMENTAL", "SLEEVE_MOMENTUM", "SLEEVE_COMBINED", "SLEEVE_DEFENSIVE", "SLEEVE_FAST",
    "model_registry", "model_by_id", "recommendation_eligible_model_ids",
    "sleeve_registry", "sleeve_by_id", "safety_block", "registry_counts",
]
