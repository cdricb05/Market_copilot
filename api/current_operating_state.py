"""
api/current_operating_state.py — Phase 15-B canonical current operating state.

Phase 15-A aligned the portfolio mark, the official portfolio snapshot and the
current-alpha Top25/Top50/SPY marks to one completed US market date. But the UI
still mixed FOUR different state families as if they were interchangeable:

    * the CURRENT operating mark        (latest completed EOD, e.g. 2026-07-16)
    * the reconstructed 13-I history    (evidence window, ends e.g. 2026-07-15)
    * the reconciler cache              (last fill cycle — a stale total)
    * assorted archived research values (Ridge candidate, raw package values)

This module is the ONE read-only aggregation that returns those families as
three explicitly separated, never-interchangeable categories:

    1. current_operating_mark      — what is true RIGHT NOW (the completed date)
    2. historical_evidence_window  — the reconstructed 13-I performance evidence
    3. legacy_archived_state       — cached / archived values, clearly non-current

It composes EXISTING read-only loaders only (portfolio valuation, the daily
operating run status, the current-alpha daily mark, the decision gate and the
performance reconstruction). It performs NO database writes, makes NO loopback
HTTP calls, invokes NO daily refresh, calls NO prediction provider, and creates
NO orders / signals / decisions. Every dependency is isolated so a single
failure degrades to a ``warnings[]`` entry instead of failing the aggregation.

Consumers: Command Center, Daily Workflow, Portfolio, Research & Audit and the
global market-data badge all read the SAME canonical current operating mark, so
"today" means one date everywhere.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from paper_trader.api.portfolio_valuation import load_portfolio_valuation
from paper_trader.api.daily_operating_run import load_daily_operating_run_status
from paper_trader.api.current_alpha_daily_refresh import load_current_alpha_daily_status
from paper_trader.api.current_alpha_decision_gate import load_current_alpha_decision_gate
from paper_trader.api.current_alpha_performance import load_current_alpha_performance

PHASE = "15-B"

# The three canonical categories — a state value belongs to exactly one.
CAT_CURRENT = "CURRENT_OPERATING_MARK"
CAT_HISTORICAL = "HISTORICAL_EVIDENCE_WINDOW"
CAT_LEGACY = "LEGACY_ARCHIVED_STATE"

LEGACY_CACHE_LABEL = "LEGACY RECONCILER CACHE — NOT CURRENT EOD VALUE"

SAFETY_BADGES = [
    "PAPER ONLY",
    "MANUAL REVIEW",
    "NO BROKER EXECUTION",
    "AUTOMATION OFF",
    "NO LIVE ORDERS",
]


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #

def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _num(x: Any) -> Optional[float]:
    if isinstance(x, bool) or x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(str(x))
    except (TypeError, ValueError):
        return None


def _dec(x: Any) -> Optional[Decimal]:
    if x is None:
        return None
    try:
        return Decimal(str(x))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _safety() -> dict[str, Any]:
    return {
        "paper_only": True,
        "read_only": True,
        "manual_review": True,
        "no_broker_execution": True,
        "automation_off": True,
        "no_live_orders": True,
        "creates_orders": False,
        "creates_signals": False,
        "creates_trade_decisions": False,
        "creates_fills": False,
        "promotes_to_live": False,
        "safety_badges": list(SAFETY_BADGES),
    }


def _provenance() -> dict[str, Any]:
    return {
        "phase": PHASE,
        "generated_at": _now_iso(),
        "read_only": True,
        "wrote_to_database": False,
        "created_orders": False,
        "created_signals": False,
        "created_trade_decisions": False,
        "created_fills": False,
        "invoked_daily_refresh": False,
        "called_prediction_service": False,
        "called_external_provider": False,
        "made_loopback_http_calls": False,
        "categories_are_interchangeable": False,
        "sources": [
            "loader:load_daily_operating_run_status (alignment + completed date)",
            "loader:load_portfolio_valuation (canonical current portfolio mark)",
            "loader:load_current_alpha_daily_status (current Top25/Top50/SPY mark)",
            "loader:load_current_alpha_decision_gate (historical decision evidence)",
            "loader:load_current_alpha_performance (13-I reconstruction window)",
        ],
    }


# --------------------------------------------------------------------------- #
# 1. CURRENT OPERATING MARK — the latest completed date, everywhere
# --------------------------------------------------------------------------- #

def _current_book(ds_book: dict[str, Any]) -> dict[str, Any]:
    """Normalize a daily-status book slice to the current operating mark."""
    ds_book = ds_book or {}
    return {
        "book_id": ds_book.get("book_id"),
        "book_size": ds_book.get("book_size"),
        "mark_date": ds_book.get("mark_date"),
        "return_pct": _num(ds_book.get("average_return_pct")),
        "median_return_pct": _num(ds_book.get("median_return_pct")),
        "hit_rate_pct": _num(ds_book.get("hit_rate_pct")),
        "spy_return_pct": _num(ds_book.get("benchmark_return_pct")),
        "excess_pct_points": _num(ds_book.get("excess_return_vs_spy_pct_points")),
        "coverage_pct": _num(ds_book.get("coverage_pct")),
        "coverage_status": ds_book.get("coverage_status"),
        "previous_mark_date": ds_book.get("previous_mark_date"),
        "previous_return_pct": _num(ds_book.get("previous_average_return_pct")),
        "change_since_previous_pct_points": _num(
            ds_book.get("change_since_previous_mark_pct_points")
        ),
    }


def _current_operating_mark(
    *, run_status: dict[str, Any], valuation: dict[str, Any],
    daily_status: dict[str, Any], gate: dict[str, Any],
) -> dict[str, Any]:
    """Assemble the single source of truth for RIGHT NOW.

    Portfolio numbers come from the canonical valuation; alpha numbers come from
    the DAILY operating mark (never the reconstructed history); alignment comes
    from the daily operating run status. Every alpha excess is recomputed from
    the current Top25/Top50 return and the current SPY return — never carried
    over from a previous mark.
    """
    alignment = (run_status or {}).get("alignment") or {}
    cm = (valuation or {}).get("current_mark") or {}
    ds = daily_status or {}
    t25 = _current_book(ds.get("top25") or {})
    t50 = _current_book(ds.get("top50") or {})
    spy_b = ds.get("spy_benchmark") or {}
    gate = gate or {}

    # Current drawdown is only known from the reconstructed curve; expose it under
    # the current mark as an explicitly-labelled overlay (nearest available), but
    # the *return / excess* are strictly the current daily mark.
    g25 = gate.get("top25") or {}
    g50 = gate.get("top50") or {}

    return {
        "category": CAT_CURRENT,
        "latest_completed_market_date": (run_status or {}).get("required_market_date"),
        "alignment_status": (run_status or {}).get("status"),
        "aligned": bool(alignment.get("aligned")),
        "freshness_status": (run_status or {}).get("freshness_status")
        or cm.get("freshness_status"),
        "coverage": {
            "covered_position_count": cm.get("covered_position_count"),
            "total_position_count": cm.get("total_position_count"),
            "complete": cm.get("valuation_complete"),
        },
        "signal_date": spy_b.get("reference_date"),
        "portfolio": {
            "current_total_value": cm.get("current_total_value"),
            "current_positions_value": cm.get("current_positions_value"),
            "current_cash": cm.get("current_cash"),
            "current_total_return_pct": _num(cm.get("current_total_return_pct")),
            "current_unrealized_pnl": cm.get("current_unrealized_pnl"),
            "initial_capital": cm.get("initial_capital"),
            "as_of_market_date": cm.get("as_of_market_date"),
            "price_source": cm.get("price_source"),
        },
        "top25": {**t25, "current_drawdown_pct": _num(g25.get("current_drawdown_pct"))},
        "top50": {**t50, "current_drawdown_pct": _num(g50.get("current_drawdown_pct"))},
        "spy": {
            "mark_date": spy_b.get("latest_completed_eod_date"),
            "return_since_signal_pct": _num(spy_b.get("return_since_signal_pct")),
            "reference_date": spy_b.get("reference_date"),
            "reference_price": _num(spy_b.get("reference_price")),
            "latest_adjusted_close": _num(spy_b.get("latest_adjusted_close")),
        },
        "primary_book": (gate.get("primary_paper_book") or {}).get("book"),
        "challenger_book": (gate.get("challenger_paper_book") or {}).get("label"),
        "current_decision": gate.get("decision"),
        "current_decision_label": gate.get("decision_label"),
        "note": (
            "The current operating mark is the latest completed US market date. "
            "Alpha return and excess are the current daily mark; excess is "
            "recomputed from the current Top25/Top50 and SPY returns."
        ),
    }


# --------------------------------------------------------------------------- #
# 2. HISTORICAL EVIDENCE WINDOW — the reconstructed 13-I performance
# --------------------------------------------------------------------------- #

def _hist_book(analytics: dict[str, Any], gate_book: dict[str, Any]) -> dict[str, Any]:
    analytics = analytics or {}
    gate_book = gate_book or {}
    return {
        "cumulative_return_pct": _num(
            analytics.get("cumulative_return_pct")
            if analytics.get("cumulative_return_pct") is not None
            else gate_book.get("current_return_pct")
        ),
        "excess_return_pct_points": _num(
            analytics.get("excess_return_pct_points")
            if analytics.get("excess_return_pct_points") is not None
            else gate_book.get("current_excess_return_pct_points")
        ),
        "max_drawdown_pct": _num(
            analytics.get("max_drawdown_pct")
            if analytics.get("max_drawdown_pct") is not None
            else gate_book.get("max_drawdown_pct")
        ),
        "current_drawdown_pct": _num(gate_book.get("current_drawdown_pct")),
        "spy_cumulative_return_pct": _num(gate_book.get("spy_cumulative_return_pct")),
    }


def _historical_evidence_window(
    *, performance: dict[str, Any], gate: dict[str, Any],
) -> dict[str, Any]:
    """Assemble the reconstructed evidence window (NEVER the current mark)."""
    pf = performance or {}
    gate = gate or {}
    benchmark = pf.get("benchmark") or {}
    return {
        "category": CAT_HISTORICAL,
        "available": pf.get("status") == "PERFORMANCE_READY",
        "status": pf.get("status"),
        "reconstruction_start_date": pf.get("backfill_start_date"),
        "reconstruction_end_date": pf.get("latest_mark_date"),
        "observation_count": pf.get("observation_count"),
        "spy_cumulative_return_pct": _num(
            benchmark.get("latest_return_since_signal_pct")
        ),
        "top25": _hist_book(pf.get("top25_analytics") or {}, gate.get("top25") or {}),
        "top50": _hist_book(pf.get("top50_analytics") or {}, gate.get("top50") or {}),
        "stability_comparison": pf.get("stability_comparison")
        or gate.get("stability_comparison"),
        "decision": gate.get("decision"),
        "decision_label": gate.get("decision_label"),
        "decision_reasons": gate.get("decision_reasons"),
        "book_role_status": gate.get("book_role_status"),
        "quarterly_rebalance_readiness": gate.get("quarterly_rebalance_readiness"),
        "risk_review": gate.get("risk_review"),
        "deterioration_review": gate.get("deterioration_review"),
        "backfill_decision": pf.get("backfill_decision"),
        "backfill_reconciliation": pf.get("reconciliation")
        or {"status": pf.get("reconciliation_status")},
        "note": (
            "Reconstructed Phase 13-I daily-mark evidence used for the decision. It "
            "ends at the reconstruction date and is NOT the current operating mark."
        ),
    }


# --------------------------------------------------------------------------- #
# 3. LEGACY / ARCHIVED STATE — cached + archived, clearly non-current
# --------------------------------------------------------------------------- #

def _legacy_archived_state(*, valuation: dict[str, Any]) -> dict[str, Any]:
    """Assemble the cached / archived values, always labelled non-current."""
    recon = (valuation or {}).get("reconciliation") or {}
    cm = (valuation or {}).get("current_mark") or {}
    vs_cache = recon.get("vs_cached_total_value") or {}

    cached_total = _dec(vs_cache.get("cached_total_value"))
    current_cash = _dec(cm.get("current_cash"))
    # The cached invested value is the cached total minus the (still-current) cash.
    cached_invested = None
    if cached_total is not None and current_cash is not None:
        cached_invested = str((cached_total - current_cash).quantize(Decimal("0.01")))

    return {
        "category": CAT_LEGACY,
        "label": LEGACY_CACHE_LABEL,
        "cached_reconciler": {
            "cached_total_value": vs_cache.get("cached_total_value"),
            "cached_positions_value": cached_invested,
            "delta_vs_current_total": vs_cache.get("delta"),
            "label": LEGACY_CACHE_LABEL,
        },
        "note": (
            "Archived / non-current artifacts (reconciler cache, previous research "
            "candidates such as the Ridge model, raw package values). These are shown "
            "for audit only and are never interchangeable with the current operating "
            "mark. Diagnostics / Archive surfaces render the full detail."
        ),
    }


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #

def load_current_operating_state(
    *,
    valuation: Optional[dict[str, Any]] = None,
    run_status: Optional[dict[str, Any]] = None,
    daily_status: Optional[dict[str, Any]] = None,
    gate: Optional[dict[str, Any]] = None,
    performance: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Return the canonical current operating state (Part A).

    Three explicitly separated, never-interchangeable categories plus safety and
    provenance. Read-only: no writes, no prediction call, no loopback HTTP. Each
    dependency is isolated so a single failure degrades to a warning.
    """
    warnings: list[str] = []

    if valuation is None:
        try:
            valuation = load_portfolio_valuation()
        except Exception as exc:  # noqa: BLE001
            valuation = {"current_mark": {}, "latest_snapshot": None, "reconciliation": {}}
            warnings.append(f"Canonical valuation unavailable: {str(exc)[:160]}")
    if run_status is None:
        try:
            run_status = load_daily_operating_run_status(valuation=valuation)
        except Exception as exc:  # noqa: BLE001
            run_status = {}
            warnings.append(f"Daily operating run status unavailable: {str(exc)[:160]}")
    if daily_status is None:
        try:
            daily_status = load_current_alpha_daily_status()
        except Exception as exc:  # noqa: BLE001
            daily_status = {}
            warnings.append(f"Current-alpha daily status unavailable: {str(exc)[:160]}")
    if gate is None:
        try:
            gate = load_current_alpha_decision_gate()
        except Exception as exc:  # noqa: BLE001
            gate = {}
            warnings.append(f"Decision gate unavailable: {str(exc)[:160]}")
    if performance is None:
        try:
            performance = load_current_alpha_performance()
        except Exception as exc:  # noqa: BLE001
            performance = {}
            warnings.append(f"Performance reconstruction unavailable: {str(exc)[:160]}")

    current = _current_operating_mark(
        run_status=run_status, valuation=valuation,
        daily_status=daily_status, gate=gate,
    )
    historical = _historical_evidence_window(performance=performance, gate=gate)
    legacy = _legacy_archived_state(valuation=valuation)

    # A guard the UI can assert on: the current alpha mark and the reconstructed
    # history are DIFFERENT dates whenever a newer daily mark exists.
    current_mark_date = current.get("latest_completed_market_date")
    history_end_date = historical.get("reconstruction_end_date")
    current_is_newer_than_history = bool(
        current_mark_date and history_end_date and current_mark_date > history_end_date
    )

    for w in (valuation or {}).get("warnings", []) or []:
        if w not in warnings:
            warnings.append(w)

    return {
        "status": "OK",
        "phase": PHASE,
        "loaded_at": _now_iso(),
        "current_operating_mark": current,
        "historical_evidence_window": historical,
        "legacy_archived_state": legacy,
        "current_mark_date": current_mark_date,
        "history_end_date": history_end_date,
        "current_is_newer_than_history": current_is_newer_than_history,
        "categories_are_interchangeable": False,
        "warnings": warnings,
        "safety": _safety(),
        "provenance": _provenance(),
    }


__all__ = [
    "load_current_operating_state",
    "CAT_CURRENT",
    "CAT_HISTORICAL",
    "CAT_LEGACY",
    "LEGACY_CACHE_LABEL",
    "SAFETY_BADGES",
]
