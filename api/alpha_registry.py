"""
api/alpha_registry.py — Phase 20 Alpha Registry (schema, statuses, families, records).

The registry is the catalogue that turns Paper Trader from a single-alpha platform into a
multi-alpha research platform: it holds one metadata record per candidate / champion /
challenger alpha, keyed by name, with a fixed schema and a controlled lifecycle status.

This module is deliberately pure and dependency-free (stdlib only):
    - It defines the alpha FAMILIES (the taxonomy the generation framework supports).
    - It defines the alpha STATUS lifecycle (RESEARCH / ACTIVE / CHALLENGER / CHAMPION /
      REJECTED / ARCHIVED).
    - It defines the reject-reason vocabulary the evaluation pipeline uses.
    - It builds normalized registry RECORDS (``make_alpha_record``) and aggregates them
      (counts by status / family, leaderboard ordering).
    - It carries the shared read-only safety block (no orders / no broker / no automation /
      no champion replacement / no live promotion).

Nothing here reads the network, the database, or the prediction service, and nothing here
creates signals / trade decisions / orders / fills or replaces the champion.
"""
from __future__ import annotations

from typing import Any, Optional

PHASE = "20"

# --------------------------------------------------------------------------- #
# Alpha families (the taxonomy the generation framework supports). Ordered to
# mirror the Phase 20 objective list exactly.
# --------------------------------------------------------------------------- #
FAM_MOMENTUM = "MOMENTUM"
FAM_TREND = "TREND"
FAM_VOLATILITY = "VOLATILITY"
FAM_RELATIVE_STRENGTH = "RELATIVE_STRENGTH"
FAM_MEAN_REVERSION = "MEAN_REVERSION"
FAM_QUALITY = "QUALITY"
FAM_FUNDAMENTAL = "FUNDAMENTAL"
FAM_HYBRID = "HYBRID"
FAM_COMPOSITE = "COMPOSITE"
FAM_SECTOR_NEUTRAL = "SECTOR_NEUTRAL"

ALL_FAMILIES = [
    FAM_MOMENTUM, FAM_TREND, FAM_VOLATILITY, FAM_RELATIVE_STRENGTH, FAM_MEAN_REVERSION,
    FAM_QUALITY, FAM_FUNDAMENTAL, FAM_HYBRID, FAM_COMPOSITE, FAM_SECTOR_NEUTRAL,
]

# Families that require a trailing PRICE panel (per-name daily price history aligned to the
# monthly rebalance grid). The owned frozen Phase 10-L panel is a FUNDAMENTAL panel and does not
# carry trailing-price factors, so these families are supported by the framework but data-gated
# in V1 — they generate zero candidates and say so, rather than fabricating price data.
PRICE_GATED_FAMILIES = [
    FAM_MOMENTUM, FAM_TREND, FAM_VOLATILITY, FAM_RELATIVE_STRENGTH, FAM_MEAN_REVERSION,
]
DATA_READY_FAMILIES = [
    FAM_FUNDAMENTAL, FAM_QUALITY, FAM_SECTOR_NEUTRAL, FAM_COMPOSITE, FAM_HYBRID,
]

FAMILY_DESCRIPTIONS = {
    FAM_MOMENTUM: "Trailing price momentum (e.g. 6/12-month return). Requires a trailing price panel.",
    FAM_TREND: "Price trend / moving-average slope. Requires a trailing price panel.",
    FAM_VOLATILITY: "Low-volatility / idiosyncratic-vol factor. Requires a trailing price panel.",
    FAM_RELATIVE_STRENGTH: "Cross-sectional price relative strength. Requires a trailing price panel.",
    FAM_MEAN_REVERSION: "Short-horizon price reversal. Requires a trailing price panel.",
    FAM_QUALITY: "Profitability / earnings-quality level factors (free-cash-flow, low accruals).",
    FAM_FUNDAMENTAL: "Single-factor owned fundamental normalized legs (FCF/assets, operating accruals).",
    FAM_HYBRID: "Mixed sector-neutral + market-relative legs, cohort- or liquidity-conditioned blends.",
    FAM_COMPOSITE: "Market-relative multi-leg composites (reweighted normalized fundamental legs).",
    FAM_SECTOR_NEUTRAL: "Within-month, within-sector de-meaned z-legs and their reweighted composites.",
}

# --------------------------------------------------------------------------- #
# Lifecycle status. NONE of these approve live trading.
# --------------------------------------------------------------------------- #
STATUS_RESEARCH = "RESEARCH"       # generated + evaluated, survives gates but provisional
STATUS_ACTIVE = "ACTIVE"           # surviving candidate strong enough to actively track (paper)
STATUS_CHALLENGER = "CHALLENGER"   # the sector-repaired paper challenger (Phase 17-B)
STATUS_CHAMPION = "CHAMPION"       # the current paper champion (composite_sn)
STATUS_REJECTED = "REJECTED"       # failed an automatic gate
STATUS_ARCHIVED = "ARCHIVED"       # kept for provenance (champion reproduction / diagnostic)

ALL_STATUSES = [
    STATUS_RESEARCH, STATUS_ACTIVE, STATUS_CHALLENGER, STATUS_CHAMPION,
    STATUS_REJECTED, STATUS_ARCHIVED,
]

STATUS_CLASS = {
    STATUS_CHAMPION: "safe",
    STATUS_CHALLENGER: "manual",
    STATUS_ACTIVE: "safe",
    STATUS_RESEARCH: "warn",
    STATUS_REJECTED: "danger",
    STATUS_ARCHIVED: "muted",
}

# --------------------------------------------------------------------------- #
# Automatic reject-reason vocabulary.
# --------------------------------------------------------------------------- #
REJECT_LOW_COVERAGE = "LOW_COVERAGE"
REJECT_NEGATIVE_IC = "NEGATIVE_IC"
REJECT_STATISTICALLY_WEAK = "STATISTICALLY_WEAK_IC_T"
REJECT_COST_KILLED = "NOT_COST_ROBUST_NET25_NONPOSITIVE"
REJECT_UNSTABLE = "UNSTABLE_SUBPERIOD_OR_LOW_HIT_RATE"
REJECT_REDUNDANT = "REDUNDANT_HIGH_CORRELATION"
REJECT_NO_DATA = "REQUIRES_TRAILING_PRICE_PANEL_NOT_WIRED_IN_V1"
# Phase 21 additional price-alpha overfit-defense gates
REJECT_INSUFFICIENT_PERIODS = "INSUFFICIENT_INDEPENDENT_PERIODS"
REJECT_EXCESSIVE_TURNOVER = "EXCESSIVE_TURNOVER_THIN_NET"
REJECT_SEVERE_DRAWDOWN = "SEVERE_DRAWDOWN_VS_CUMULATIVE"
REJECT_PARAM_UNSTABLE = "PARAMETER_NEIGHBOR_INSTABILITY"
REJECT_CONCENTRATED = "PERFORMANCE_CONCENTRATED_IN_SHORT_PERIOD"

REJECT_REASON_TEXT = {
    REJECT_LOW_COVERAGE: "Signal coverage of scoreable name-months is below the minimum.",
    REJECT_NEGATIVE_IC: "Mean information coefficient is negative (wrong-signed).",
    REJECT_STATISTICALLY_WEAK: "IC t-statistic is below the significance floor.",
    REJECT_COST_KILLED: "Gross spread does not survive 25bps round-trip costs (net25 <= 0).",
    REJECT_UNSTABLE: "Unstable: subperiod IC sign reversal or positive-IC-month rate below 50%.",
    REJECT_REDUNDANT: "Rank correlation vs the champion or a stronger survivor exceeds the cap.",
    REJECT_NO_DATA: "Family requires a trailing price panel not wired in Alpha Factory V1.",
    REJECT_INSUFFICIENT_PERIODS: "Too few independent scored rebalance months to trust the estimate.",
    REJECT_EXCESSIVE_TURNOVER: "Turnover is extreme and the net-of-cost spread is too thin to justify it.",
    REJECT_SEVERE_DRAWDOWN: "Max drawdown is large relative to the cumulative spread (fragile equity curve).",
    REJECT_PARAM_UNSTABLE: "IC sign disagrees with its own parameter-lookback neighbours (overfit-prone).",
    REJECT_CONCENTRATED: "Cost-adjusted edge is concentrated in a single subperiod (pre- or post-2020).",
}

# --------------------------------------------------------------------------- #
# Registry record schema. This is the fixed metadata contract for every alpha.
# --------------------------------------------------------------------------- #
REGISTRY_METADATA_FIELDS = [
    "name", "family", "horizon", "universe", "signal_date",
    "turnover", "ic", "rank_ic", "spread", "net25", "net50",
    "drawdown", "sharpe", "ic_t", "coverage_pct", "missing_data",
    "corr_vs_champion", "regime_notes", "status", "reject_reason",
]

SAFETY_BADGES = [
    "RESEARCH ONLY", "PAPER ONLY", "NO ORDERS", "NO BROKER",
    "AUTOMATION OFF", "NO LIVE PROMOTION", "NO CHAMPION REPLACEMENT",
]


def safety_block() -> dict[str, Any]:
    """The shared read-only, no-live-trading safety block attached to every payload."""
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
        "calls_prediction_service": False,
        "wrote_to_database": False,
        "mutates_champion": False,
        "replaces_champion": False,
        "champion_replaced": False,
        "live_trading_status": "NOT_APPROVED_FOR_LIVE_TRADING",
        "no_decision_approves_live_trading": True,
    }


def _r(x: Optional[float], nd: int) -> Optional[float]:
    if x is None:
        return None
    try:
        return round(float(x), nd)
    except (TypeError, ValueError):
        return None


def make_alpha_record(
    *,
    name: str,
    family: str,
    status: str,
    horizon: int,
    universe: str,
    signal_date: Optional[str],
    metrics: Optional[dict] = None,
    corr_vs_champion: Optional[float] = None,
    regime_notes: Optional[str] = None,
    reject_reason: Optional[str] = None,
    description: Optional[str] = None,
    spec: Optional[dict] = None,
    role: Optional[str] = None,
    extra: Optional[dict] = None,
) -> dict[str, Any]:
    """Build one normalized registry record with the fixed metadata schema.

    ``metrics`` is the raw battery output (``evaluate_signal`` shape). Any missing metric maps to
    None so the record shape is always stable and JSON-serializable.
    """
    m = metrics or {}
    rec: dict[str, Any] = {
        "name": name,
        "family": family,
        "status": status,
        "status_class": STATUS_CLASS.get(status, "warn"),
        "role": role,
        "description": description,
        "horizon": horizon,
        "horizon_label": "%d trading days" % horizon,
        "universe": universe,
        "signal_date": signal_date,
        # core battery metrics
        "ic": _r(m.get("mean_ic"), 6),
        "rank_ic": _r(m.get("mean_ic"), 6),  # IC here is a monthly rank-IC (Spearman)
        "ic_t": _r(m.get("ic_t_stat"), 4),
        "spread": _r(m.get("mean_gross_spread"), 6),
        "net25": _r(m.get("net25_spread"), 6),
        "net50": _r(m.get("net50_spread"), 6),
        "turnover": _r(m.get("mean_turnover"), 4),
        "drawdown": _r(m.get("max_drawdown"), 6),
        "sharpe": _r(m.get("sharpe"), 4),
        "cumulative_spread": _r(m.get("cumulative_spread"), 6),
        "positive_ic_month_rate": _r(m.get("positive_ic_month_rate"), 4),
        "positive_spread_month_rate": _r(m.get("positive_spread_month_rate"), 4),
        "coverage_pct": _r(m.get("coverage_pct"), 2),
        "missing_data": m.get("missing_name_months"),
        "n_months_scored": m.get("n_months_scored"),
        "n_ic_months": m.get("n_ic_months"),
        # relationships + provenance
        "corr_vs_champion": _r(corr_vs_champion, 4),
        "regime_notes": regime_notes,
        "reject_reason": reject_reason,
        "reject_reason_text": REJECT_REASON_TEXT.get(reject_reason) if reject_reason else None,
        "spec": spec,
        "subperiod": m.get("subperiod"),
    }
    if extra:
        rec.update(extra)
    return rec


def registry_counts(records: list[dict]) -> dict[str, int]:
    counts = {s: 0 for s in ALL_STATUSES}
    for r in records:
        s = r.get("status")
        if s in counts:
            counts[s] += 1
    counts["total"] = len(records)
    return counts


def family_counts(records: list[dict]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {f: {"total": 0, STATUS_ACTIVE: 0, STATUS_RESEARCH: 0,
                                          STATUS_REJECTED: 0, STATUS_ARCHIVED: 0}
                                      for f in ALL_FAMILIES}
    for r in records:
        f = r.get("family")
        if f not in out:
            continue
        out[f]["total"] += 1
        s = r.get("status")
        if s in out[f]:
            out[f][s] += 1
    return out


def leaderboard_sort_key(rec: dict) -> tuple:
    """Rank survivors by net25 (cost-robust spread) desc, then IC t desc, then IC desc.

    None metrics sort last. Returns a tuple usable with ``sorted(..., reverse=True)``.
    """
    def v(x):
        return x if isinstance(x, (int, float)) else float("-inf")
    return (v(rec.get("net25")), v(rec.get("ic_t")), v(rec.get("ic")))


__all__ = [
    "PHASE",
    "ALL_FAMILIES", "PRICE_GATED_FAMILIES", "DATA_READY_FAMILIES", "FAMILY_DESCRIPTIONS",
    "FAM_MOMENTUM", "FAM_TREND", "FAM_VOLATILITY", "FAM_RELATIVE_STRENGTH", "FAM_MEAN_REVERSION",
    "FAM_QUALITY", "FAM_FUNDAMENTAL", "FAM_HYBRID", "FAM_COMPOSITE", "FAM_SECTOR_NEUTRAL",
    "ALL_STATUSES", "STATUS_CLASS",
    "STATUS_RESEARCH", "STATUS_ACTIVE", "STATUS_CHALLENGER", "STATUS_CHAMPION",
    "STATUS_REJECTED", "STATUS_ARCHIVED",
    "REJECT_LOW_COVERAGE", "REJECT_NEGATIVE_IC", "REJECT_STATISTICALLY_WEAK",
    "REJECT_COST_KILLED", "REJECT_UNSTABLE", "REJECT_REDUNDANT", "REJECT_NO_DATA",
    "REJECT_INSUFFICIENT_PERIODS", "REJECT_EXCESSIVE_TURNOVER", "REJECT_SEVERE_DRAWDOWN",
    "REJECT_PARAM_UNSTABLE", "REJECT_CONCENTRATED",
    "REJECT_REASON_TEXT",
    "REGISTRY_METADATA_FIELDS", "SAFETY_BADGES",
    "safety_block", "make_alpha_record", "registry_counts", "family_counts",
    "leaderboard_sort_key",
]
