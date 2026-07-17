"""
api/portfolio_terminal.py — Phase 14-B read-only Portfolio Terminal view model.

Aggregates everything the professional portfolio terminal needs into ONE
read-only view model: the capital summary, per-position exposure & P&L with a
read-only status recommendation, the paper-order book separated into
pending / filled / history, a compact performance time-series, the risk &
capacity roll-up, the current-alpha context, and position-level alerts.

Position status and the risk roll-up reuse the EXACT same rules as
/v1/portfolio/analytics and /v1/review/position-monitor-preview:

    REVIEW_FOR_EXIT   if unrealized P&L %% <= -5.0
    WATCH             if unrealized P&L %% <= -2.0  (or portfolio weight > 25%%)
    HOLD              otherwise
    PRICE_UNAVAILABLE when the latest owned price is missing

It composes EXISTING internal service state only. It makes NO loopback HTTP
calls, performs NO database writes, calls NO prediction provider, and creates
NO orders / signals / decisions. Every section is isolated so a single failing
dependency degrades to a ``warnings[]`` entry instead of failing the endpoint.

Sections returned:
    summary, positions, paper_orders, performance, risk, capacity,
    alpha_context, alerts, warnings, safety, provenance
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from paper_trader.config import get_settings
from paper_trader.db.models import (
    Order,
    Portfolio,
    PortfolioSnapshot,
    Position,
    TradeDecision,
)
from paper_trader.db.session import get_session
from paper_trader.workflows.decision import _latest_price
from paper_trader.api.current_alpha_decision_gate import load_current_alpha_decision_gate
from paper_trader.api.portfolio_valuation import load_portfolio_valuation

from paper_trader.api import command_center as cc

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

PHASE = "14-B"

# Position status enum (mirrors portfolio_analytics recommendations).
POS_HOLD = "HOLD"
POS_WATCH = "WATCH"
POS_REVIEW_FOR_EXIT = "REVIEW_FOR_EXIT"
POS_PRICE_UNAVAILABLE = "PRICE_UNAVAILABLE"

# Paper-order buckets.
_PENDING_STATUS = "PENDING"
_FILLED_STATUS = "FILLED"
_HISTORY_STATUSES = ("CANCELLED", "EXPIRED", "FAILED")

_MAX_ORDER_ROWS = 50
_MAX_PERF_POINTS = 400  # compact series cap

_TWO = Decimal("0.01")


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #

def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _iso(ts: Any) -> Optional[str]:
    if ts is None:
        return None
    try:
        return ts.isoformat()
    except Exception:  # noqa: BLE001
        return str(ts)


def _dstr(d: Decimal) -> str:
    return str(d.quantize(_TWO))


# --------------------------------------------------------------------------- #
# Positions (read-only exposure + status; mirrors portfolio_analytics rules)
# --------------------------------------------------------------------------- #

def _position_status(upnl_pct: Optional[float], weight_pct: Optional[float]) -> tuple[str, str]:
    """Return (status, reason) using the shared monitor rules."""
    if upnl_pct is None:
        return (POS_PRICE_UNAVAILABLE,
                "Latest price unavailable; position cannot be fully evaluated.")
    if upnl_pct <= -5.0:
        return (POS_REVIEW_FOR_EXIT,
                f"Unrealized P&L of {upnl_pct:.1f}% is below the stop-loss review "
                f"threshold (-5.0%). Manual review recommended.")
    if upnl_pct <= -2.0:
        return (POS_WATCH,
                f"Unrealized P&L of {upnl_pct:.1f}% is in the watch range "
                f"(-2.0% to -5.0%). Monitor closely.")
    if weight_pct is not None and weight_pct > 25.0:
        return (POS_WATCH,
                f"Portfolio weight of {weight_pct:.1f}% exceeds the concentration "
                f"threshold (25.0%). Consider position sizing.")
    return (POS_HOLD, "Position is within healthy parameters. No action required.")


def _latest_order_for_ticker(session, ticker: str) -> Optional[Order]:
    return (
        session.query(Order)
        .filter(Order.ticker == ticker)
        .order_by(Order.requested_at.desc())
        .first()
    )


def _enrich_positions(session, valuation: dict[str, Any]) -> dict[str, Any]:
    """Build the terminal position rows from the CANONICAL valuation positions.

    The value formulas (market value, unrealized P&L, weight, status) live only
    in ``portfolio_valuation``; here we add terminal-only presentation fields
    (avg_entry alias, latest order refs) and roll up the status counts. No price
    is re-marked in this module.
    """
    cm = valuation.get("current_mark") or {}
    canonical = valuation.get("positions") or []
    rows: list[dict[str, Any]] = []
    hold = watch = review_for_exit = missing = 0
    largest_ticker: Optional[str] = None
    largest_weight: Optional[Decimal] = None

    for r in canonical:
        status = r.get("status")
        if status == POS_HOLD:
            hold += 1
        elif status == POS_REVIEW_FOR_EXIT:
            review_for_exit += 1
        elif status == POS_WATCH:
            watch += 1
        elif status == POS_PRICE_UNAVAILABLE:
            missing += 1

        weight = r.get("weight_pct")
        if weight is not None:
            wd = Decimal(weight)
            if largest_weight is None or wd > largest_weight:
                largest_weight = wd
                largest_ticker = r.get("ticker")

        latest_order = _latest_order_for_ticker(session, r.get("ticker"))
        decision_ref = str(latest_order.trade_decision_id) if latest_order is not None else None
        signal_ref = str(latest_order.id) if latest_order is not None else None

        rows.append({
            "ticker": r.get("ticker"),
            "qty": r.get("qty"),
            "avg_entry": r.get("avg_cost"),
            "cost_basis": r.get("cost_basis"),
            "latest_price": r.get("latest_price"),
            "market_value": r.get("market_value"),
            "unrealized_pnl": r.get("unrealized_pnl"),
            "unrealized_pnl_pct": r.get("unrealized_pnl_pct"),
            "weight_pct": r.get("weight_pct"),
            "status": status,
            "reason": r.get("reason"),
            "signal_ref": signal_ref,
            "decision_ref": decision_ref,
            "price_as_of_market_date": r.get("price_as_of_market_date"),
            "price_source": r.get("price_source"),
            "opened_at": r.get("opened_at"),
            "last_updated": r.get("last_updated"),
            "paper_only": True,
        })

    return {
        "rows": rows,
        "open_positions": cm.get("total_position_count", len(rows)),
        "positions_value": cm.get("current_positions_value"),
        "unrealized_pnl": cm.get("current_unrealized_pnl"),
        "hold_count": hold,
        "watch_count": watch,
        "review_for_exit_count": review_for_exit,
        "missing_price_count": missing,
        "largest_position_ticker": largest_ticker,
        "largest_position_weight_pct": (
            str(largest_weight.quantize(_TWO)) if largest_weight is not None else None
        ),
        "concentration_warning": bool(largest_weight is not None and largest_weight > Decimal("25.0")),
    }


def _collect_positions(session, *, total_value: Decimal) -> dict[str, Any]:
    positions = (
        session.query(Position).order_by(Position.opened_at).all()
    )
    rows: list[dict[str, Any]] = []
    total_positions_value = Decimal("0")
    total_unrealized_pnl = Decimal("0")
    hold = watch = review_for_exit = missing = 0
    largest_ticker: Optional[str] = None
    largest_weight: Optional[Decimal] = None

    for pos in positions:
        qty = Decimal(str(pos.qty))
        cost_basis = Decimal(str(pos.cost_basis))
        latest = _latest_price(session, pos.ticker)

        mv = upnl = upnl_pct = weight = None
        upnl_pct_f: Optional[float] = None
        weight_f: Optional[float] = None

        if latest is None:
            missing += 1
        else:
            market_value = qty * latest
            unrealized = market_value - cost_basis
            unrealized_pct = (
                unrealized / cost_basis * Decimal("100")
                if cost_basis != Decimal("0") else Decimal("0")
            )
            total_positions_value += market_value
            total_unrealized_pnl += unrealized
            weight_pct = (
                market_value / total_value * Decimal("100")
                if total_value > Decimal("0") else None
            )
            mv = _dstr(market_value)
            upnl = _dstr(unrealized)
            upnl_pct = str(unrealized_pct.quantize(_TWO))
            weight = str(weight_pct.quantize(_TWO)) if weight_pct is not None else None
            upnl_pct_f = float(unrealized_pct)
            weight_f = float(weight_pct) if weight_pct is not None else None
            if weight_pct is not None and (largest_weight is None or weight_pct > largest_weight):
                largest_weight = weight_pct
                largest_ticker = pos.ticker

        status, reason = _position_status(upnl_pct_f, weight_f)
        if status == POS_HOLD:
            hold += 1
        elif status == POS_REVIEW_FOR_EXIT:
            review_for_exit += 1
        elif status == POS_WATCH and latest is not None:
            watch += 1

        latest_order = _latest_order_for_ticker(session, pos.ticker)
        decision_ref = None
        signal_ref = None
        if latest_order is not None:
            decision_ref = str(latest_order.trade_decision_id)
            signal_ref = str(latest_order.id)

        rows.append({
            "ticker": pos.ticker,
            "qty": str(qty),
            "avg_entry": str(pos.avg_cost),
            "cost_basis": _dstr(cost_basis),
            "latest_price": str(latest) if latest is not None else None,
            "market_value": mv,
            "unrealized_pnl": upnl,
            "unrealized_pnl_pct": upnl_pct,
            "weight_pct": weight,
            "status": status,
            "reason": reason,
            "signal_ref": signal_ref,
            "decision_ref": decision_ref,
            "opened_at": _iso(pos.opened_at),
            "last_updated": _iso(pos.last_updated),
            "paper_only": True,
        })

    rows.sort(key=lambda r: (Decimal(r["weight_pct"]) if r["weight_pct"] else Decimal("0")),
              reverse=True)

    return {
        "rows": rows,
        "open_positions": len(positions),
        "positions_value": _dstr(total_positions_value),
        "unrealized_pnl": _dstr(total_unrealized_pnl),
        "hold_count": hold,
        "watch_count": watch,
        "review_for_exit_count": review_for_exit,
        "missing_price_count": missing,
        "largest_position_ticker": largest_ticker,
        "largest_position_weight_pct": (
            str(largest_weight.quantize(_TWO)) if largest_weight is not None else None
        ),
        "concentration_warning": bool(largest_weight is not None and largest_weight > Decimal("25.0")),
    }


# --------------------------------------------------------------------------- #
# Paper orders (pending / filled / history — never mislabelled)
# --------------------------------------------------------------------------- #

def _order_view(o: Order) -> dict[str, Any]:
    return {
        "order_id": str(o.id),
        "ticker": o.ticker,
        "side": o.side,
        "status": o.status,
        "order_type": o.order_type,
        "requested_qty": str(o.requested_qty),
        "filled_qty": str(o.filled_qty) if o.filled_qty is not None else None,
        "fill_price": str(o.fill_price) if o.fill_price is not None else None,
        "market_date": _iso(o.market_date),
        "requested_at": _iso(o.requested_at),
        "filled_at": _iso(o.filled_at),
        "paper_only": True,
    }


def _collect_orders(session) -> dict[str, Any]:
    pending = (
        session.query(Order).filter(Order.status == _PENDING_STATUS)
        .order_by(Order.requested_at.desc()).limit(_MAX_ORDER_ROWS).all()
    )
    filled = (
        session.query(Order).filter(Order.status == _FILLED_STATUS)
        .order_by(Order.filled_at.desc().nullslast()).limit(_MAX_ORDER_ROWS).all()
    )
    history = (
        session.query(Order).filter(Order.status.in_(_HISTORY_STATUSES))
        .order_by(Order.requested_at.desc()).limit(_MAX_ORDER_ROWS).all()
    )
    return {
        "pending": [_order_view(o) for o in pending],
        "filled": [_order_view(o) for o in filled],
        "history": [_order_view(o) for o in history],
        "pending_count": session.query(Order).filter(Order.status == _PENDING_STATUS).count(),
        "filled_count": session.query(Order).filter(Order.status == _FILLED_STATUS).count(),
        "history_count": session.query(Order).filter(Order.status.in_(_HISTORY_STATUSES)).count(),
    }


# --------------------------------------------------------------------------- #
# Performance time-series (from PortfolioSnapshot; compact)
# --------------------------------------------------------------------------- #

def _collect_performance(session, *, initial_capital: Optional[Decimal],
                         alpha: dict[str, Any]) -> dict[str, Any]:
    snaps = (
        session.query(PortfolioSnapshot)
        .order_by(PortfolioSnapshot.market_date.asc())
        .limit(_MAX_PERF_POINTS)
        .all()
    )
    value_series: list[dict[str, Any]] = []
    cumret_series: list[dict[str, Any]] = []
    cash_series: list[dict[str, Any]] = []
    invested_series: list[dict[str, Any]] = []
    realized_pnl = None
    unrealized_pnl = None

    base = initial_capital if (initial_capital and initial_capital != Decimal("0")) else None
    for s in snaps:
        d = _iso(s.market_date)
        tv = Decimal(str(s.total_value))
        value_series.append({"date": d, "value": _dstr(tv)})
        cash_series.append({"date": d, "value": _dstr(Decimal(str(s.cash)))})
        invested_series.append({"date": d, "value": _dstr(Decimal(str(s.positions_value)))})
        if base is not None:
            cumret_series.append({
                "date": d,
                "pct": float(((tv - base) / base * Decimal("100")).quantize(Decimal("0.0001"))),
            })

    if snaps:
        last = snaps[-1]
        realized_pnl = _dstr(Decimal(str(last.realized_pnl_cumulative)))
        unrealized_pnl = _dstr(Decimal(str(last.unrealized_pnl)))

    top25 = alpha.get("top25") or {}
    top50 = alpha.get("top50") or {}
    primary = (alpha.get("primary_paper_book") or {}).get("book")
    primary_book = top25 if primary == "TOP25" else top50
    alpha_vs_spy = {
        "available": bool(alpha.get("available")),
        "primary_book": primary,
        "primary_return_pct": primary_book.get("current_return_pct"),
        "excess_pct": primary_book.get("current_excess_return_pct_points"),
        "spy_return_pct": primary_book.get("spy_cumulative_return_pct"),
        "top25_return_pct": top25.get("current_return_pct"),
        "top50_return_pct": top50.get("current_return_pct"),
    }

    return {
        "observation_count": len(snaps),
        "portfolio_value_series": value_series,
        "cumulative_return_series": cumret_series,
        "cash_series": cash_series,
        "invested_series": invested_series,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "alpha_vs_spy": alpha_vs_spy,
    }


# --------------------------------------------------------------------------- #
# Alerts
# --------------------------------------------------------------------------- #

def _build_alerts(pos: dict[str, Any], *, capacity_state: str) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    for r in pos.get("rows", []):
        if r["status"] == POS_REVIEW_FOR_EXIT:
            alerts.append({"level": "warning", "code": "REVIEW_FOR_EXIT",
                           "ticker": r["ticker"], "message": r["reason"]})
        elif r["status"] == POS_PRICE_UNAVAILABLE:
            alerts.append({"level": "info", "code": "PRICE_UNAVAILABLE",
                           "ticker": r["ticker"], "message": r["reason"]})
    if pos.get("concentration_warning"):
        alerts.append({"level": "warning", "code": "HIGH_CONCENTRATION",
                       "ticker": pos.get("largest_position_ticker"),
                       "message": (f"Largest position weight "
                                   f"{pos.get('largest_position_weight_pct')}% exceeds the 25% "
                                   f"concentration threshold.")})
    if capacity_state == cc.CAP_FULL:
        alerts.append({"level": "warning", "code": cc.CAP_FULL, "ticker": None,
                       "message": cc._capacity_state(1, 0)[1]})
    return alerts


# --------------------------------------------------------------------------- #
# Provenance + degraded
# --------------------------------------------------------------------------- #

def _provenance() -> dict[str, Any]:
    return {
        "phase": PHASE,
        "generated_at": _now_iso(),
        "read_only": True,
        "wrote_to_database": False,
        "created_orders": False,
        "created_signals": False,
        "created_trade_decisions": False,
        "invoked_daily_refresh": False,
        "called_prediction_service": False,
        "called_external_provider": False,
        "made_loopback_http_calls": False,
        "sources": [
            "db:portfolio",
            "db:positions",
            "db:orders",
            "db:portfolio_snapshots",
            "loader:load_current_alpha_decision_gate",
            "helper:workflows.decision._latest_price",
        ],
    }


def _empty_summary(*, seeded: bool, max_positions: int) -> dict[str, Any]:
    return {
        "seeded": seeded, "total_value": None, "cash": None, "invested_value": None,
        "open_positions": 0, "pending_paper_orders": 0, "available_slots": 0,
        "max_positions": max_positions, "total_return_pct": None, "daily_change_pct": None,
        "unrealized_pnl": None,
    }


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #

def load_portfolio_terminal() -> dict[str, Any]:
    """Aggregate the read-only Portfolio Terminal view model.

    Never raises: each dependency is isolated so a single failure becomes a
    warning and a degraded section rather than an HTTP 500. Performs no writes,
    no prediction call, and no external provider call.
    """
    warnings: list[str] = []
    settings = get_settings()

    # --- Alpha context (read-only decision gate) --------------------------- #
    try:
        gate = load_current_alpha_decision_gate()
        alpha = cc._alpha_section(gate)
    except Exception as exc:  # noqa: BLE001
        alpha = cc._degraded_alpha(f"decision-gate unavailable: {str(exc)[:160]}")
        warnings.append(f"Current alpha unavailable: {str(exc)[:160]}")

    # --- Canonical current valuation (single source of truth) -------------- #
    try:
        valuation = load_portfolio_valuation()
        for w in valuation.get("warnings", []):
            warnings.append(w)
    except Exception as exc:  # noqa: BLE001
        valuation = {"current_mark": {}, "positions": [], "latest_snapshot": None,
                     "reconciliation": {}, "seeded": False}
        warnings.append(f"Canonical valuation unavailable: {str(exc)[:160]}")
    current_mark = valuation.get("current_mark") or {}
    latest_snapshot = valuation.get("latest_snapshot")
    reconciliation = valuation.get("reconciliation") or {}

    summary = _empty_summary(seeded=False, max_positions=settings.max_positions)
    positions: dict[str, Any] = {"rows": [], "open_positions": 0}
    orders: dict[str, Any] = {"pending": [], "filled": [], "history": [],
                              "pending_count": 0, "filled_count": 0, "history_count": 0}
    performance: dict[str, Any] = {"observation_count": 0, "portfolio_value_series": [],
                                   "cumulative_return_series": [], "cash_series": [],
                                   "invested_series": [], "realized_pnl": None,
                                   "unrealized_pnl": None, "alpha_vs_spy": {}}
    capacity_state = cc.CAP_EMPTY
    capacity_expl = "Portfolio not seeded."
    max_positions = settings.max_positions
    open_count = 0

    try:
        with get_session() as session:
            portfolio = session.query(Portfolio).first()
            if portfolio is None:
                warnings.append("Portfolio not seeded.")
            else:
                initial = Decimal(str(portfolio.initial_capital))
                max_positions = int(
                    (portfolio.config or {}).get("max_positions", settings.max_positions)
                )

                try:
                    positions = _enrich_positions(session, valuation)
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"Positions unavailable: {str(exc)[:160]}")
                    positions = {"rows": [], "open_positions": 0, "positions_value": "0.00",
                                 "unrealized_pnl": "0.00", "hold_count": 0, "watch_count": 0,
                                 "review_for_exit_count": 0, "missing_price_count": 0,
                                 "largest_position_ticker": None,
                                 "largest_position_weight_pct": None,
                                 "concentration_warning": False}
                try:
                    orders = _collect_orders(session)
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"Paper orders unavailable: {str(exc)[:160]}")
                try:
                    performance = _collect_performance(session, initial_capital=initial, alpha=alpha)
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"Performance history unavailable: {str(exc)[:160]}")

                open_count = positions.get("open_positions", 0)
                capacity_state, capacity_expl = cc._capacity_state(open_count, max_positions)
                available_slots = max(0, max_positions - open_count)

                # All CURRENT totals come from the canonical valuation — never
                # from Portfolio.cached_total_value while positions are re-marked.
                summary = {
                    "seeded": True,
                    "total_value": current_mark.get("current_total_value"),
                    "cash": current_mark.get("current_cash"),
                    "invested_value": current_mark.get("current_positions_value"),
                    "open_positions": open_count,
                    "pending_paper_orders": orders.get("pending_count", 0),
                    "available_slots": available_slots,
                    "max_positions": max_positions,
                    "total_return_pct": current_mark.get("current_total_return_pct"),
                    "daily_change_pct": None,  # intraday change not tracked read-only
                    "unrealized_pnl": current_mark.get("current_unrealized_pnl"),
                    # canonical valuation metadata (CURRENT_MARKED_EOD)
                    "valuation_type": current_mark.get("valuation_type"),
                    "valuation_complete": current_mark.get("valuation_complete"),
                    "as_of_market_date": current_mark.get("as_of_market_date"),
                    "price_source": current_mark.get("price_source"),
                    "freshness_status": current_mark.get("freshness_status"),
                    "covered_position_count": current_mark.get("covered_position_count"),
                    "total_position_count": current_mark.get("total_position_count"),
                }
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Backend/database unavailable: {str(exc)[:160]}")

    available_slots = max(0, max_positions - open_count)
    capacity = {
        "open_positions": open_count,
        "max_positions": max_positions,
        "available_slots": available_slots,
        "capacity_state": capacity_state,
        "capacity_explanation": capacity_expl,
        "pending_paper_orders": orders.get("pending_count", 0),
    }

    review_for_exit = positions.get("review_for_exit_count", 0)
    watch = positions.get("watch_count", 0)
    missing = positions.get("missing_price_count", 0)
    if review_for_exit > 0:
        risk_message = f"Exit review required for {review_for_exit} position(s)."
    elif watch > 0:
        risk_message = f"Watch required for {watch} position(s)."
    elif open_count > 0:
        risk_message = "Portfolio healthy. Monitor only."
    else:
        risk_message = "No open positions."

    blockers: list[str] = []
    if capacity_state == cc.CAP_FULL:
        blockers.append(capacity_expl)

    risk = {
        "capacity_state": capacity_state,
        "concentration_warning": positions.get("concentration_warning", False),
        "largest_position_ticker": positions.get("largest_position_ticker"),
        "largest_position_weight_pct": positions.get("largest_position_weight_pct"),
        "hold_count": positions.get("hold_count", 0),
        "watch_count": watch,
        "review_for_exit_count": review_for_exit,
        "missing_price_count": missing,
        "risk_message": risk_message,
        "risk_engine_explanation": (
            "The risk engine caps the portfolio at its maximum position count and "
            "flags positions for review — it never places or blocks a live order "
            "(paper only, automation off)."
        ),
        "blockers": blockers,
    }

    alpha_context = {
        "available": bool(alpha.get("available")),
        "primary_book": (alpha.get("primary_paper_book") or {}).get("book"),
        "primary_book_id": (alpha.get("primary_paper_book") or {}).get("book_id"),
        "challenger_book": (alpha.get("challenger_paper_book") or {}).get("label")
            if alpha.get("challenger_paper_book") else None,
        "primary_return_pct": performance.get("alpha_vs_spy", {}).get("primary_return_pct"),
        "excess_pct": performance.get("alpha_vs_spy", {}).get("excess_pct"),
        "spy_return_pct": performance.get("alpha_vs_spy", {}).get("spy_return_pct"),
        "mark_freshness_status": alpha.get("mark_freshness_status"),
        "decision_label": alpha.get("decision_label") or alpha.get("decision"),
    }

    alerts = _build_alerts(positions, capacity_state=capacity_state)

    status = "OK" if summary.get("seeded") else "DEGRADED"
    return {
        "status": status,
        "summary": summary,
        "current_mark": current_mark,
        "latest_snapshot": latest_snapshot,
        "reconciliation": reconciliation,
        "positions": positions.get("rows", []),
        "position_summary": {
            "open_positions": open_count,
            "positions_value": positions.get("positions_value"),
            "unrealized_pnl": positions.get("unrealized_pnl"),
            "hold_count": positions.get("hold_count", 0),
            "watch_count": watch,
            "review_for_exit_count": review_for_exit,
            "missing_price_count": missing,
        },
        "paper_orders": orders,
        "performance": performance,
        "risk": risk,
        "capacity": capacity,
        "alpha_context": alpha_context,
        "alerts": alerts,
        "warnings": warnings,
        "safety": cc._safety_block(),
        "provenance": _provenance(),
    }


__all__ = [
    "load_portfolio_terminal",
    "_position_status",
    "_collect_positions",
    "_collect_orders",
    "POS_HOLD", "POS_WATCH", "POS_REVIEW_FOR_EXIT", "POS_PRICE_UNAVAILABLE",
]
