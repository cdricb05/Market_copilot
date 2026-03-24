"""
engine/risk.py — Signal evaluation and position-sizing logic.

evaluate_signal() is the single entry point. It accepts a live Session plus
pre-fetched signal fields and returns a RiskDecision that the caller uses to
create a trade_decision row or skip. It never commits.

Risk parameters are read from portfolio.config (JSONB overrides) with
Settings as the fallback default. The JSONB key "ticker_cooldown_hours"
maps to Settings.cooldown_hours.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from paper_trader.config import get_settings
from paper_trader.constants import (
    DecisionType,
    OrderSide,
    OrderStatus,
    RejectionReason,
    SignalDirection,
)
from paper_trader.db.models import Order, Portfolio, Position, Trade, TradeDecision
from paper_trader.engine.portfolio import (
    compute_cash,
    get_open_positions,
    get_position,
)

_QTY     = Decimal("0.00000001")
_PRICE   = Decimal("0.000001")
_DOLLARS = Decimal("0.01")


# ---------------------------------------------------------------------------
# RiskDecision
# ---------------------------------------------------------------------------

@dataclass
class RiskDecision:
    decision: str                          # DecisionType value
    reason_code: str | None                # RejectionReason value, or None on approval
    requested_notional: Decimal            # confidence-weighted target (0 if not computed)
    approved_notional: Decimal             # after resource clamping (0 if rejected)
    requested_qty: Decimal                 # shares implied by requested_notional (0 if not computed)
    approved_qty: Decimal                  # whole shares approved (0 if rejected)
    risk_snapshot: dict[str, Any] = field(default_factory=dict)
    sizing_adjustments: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _effective_settings(portfolio: Portfolio) -> dict[str, Any]:
    """
    Merge portfolio.config JSONB overrides on top of Settings defaults.
    Returns a plain dict with all risk parameters needed by evaluate_signal().
    """
    s = get_settings()
    cfg: dict[str, Any] = portfolio.config or {}
    return {
        "strategy_enabled":           portfolio.strategy_enabled,
        "trading_enabled":            portfolio.trading_enabled,
        "allow_new_positions":        portfolio.allow_new_positions,
        "max_positions":              int(cfg.get("max_positions", s.max_positions)),
        "max_concentration_pct":      Decimal(str(cfg.get("max_concentration_pct", s.max_concentration_pct))),
        "min_cash_pct":               Decimal(str(cfg.get("min_cash_pct", s.min_cash_pct))),
        "max_daily_new_exposure_pct": Decimal(str(cfg.get("max_daily_new_exposure_pct", s.max_daily_new_exposure_pct))),
        "confidence_threshold":       Decimal(str(cfg.get("confidence_threshold", s.confidence_threshold))),
        "min_order_notional":         Decimal(str(cfg.get("min_order_notional", s.min_order_notional))),
        "cooldown_hours":             int(cfg.get("ticker_cooldown_hours", s.cooldown_hours)),
        "allow_averaging_down":       bool(cfg.get("allow_averaging_down", s.allow_averaging_down)),
    }


def _daily_buy_exposure(session: Session, market_date: date) -> Decimal:
    """
    Return the total approved_notional of BUY trade_decisions that have a
    PENDING or FILLED order on market_date. Single-portfolio v1: no
    portfolio_id filter needed.
    """
    result = session.execute(
        select(func.coalesce(func.sum(TradeDecision.approved_notional), Decimal("0")))
        .join(Order, Order.trade_decision_id == TradeDecision.id)
        .where(
            TradeDecision.decision == DecisionType.BUY,
            TradeDecision.market_date == market_date,
            Order.status.in_([OrderStatus.PENDING, OrderStatus.FILLED]),
        )
    ).scalar()
    return Decimal(str(result)).quantize(_DOLLARS)


def _last_sell_ts(session: Session, ticker: str) -> datetime | None:
    """
    Return the most recent trade_ts of a SELL trade for ticker, or None if no
    SELL has ever been executed. Used for wall-clock cooldown enforcement.
    """
    return session.execute(
        select(func.max(Trade.trade_ts))
        .where(
            Trade.ticker == ticker,
            Trade.side == OrderSide.SELL,
        )
    ).scalar()


def _has_pending_order(
    session: Session,
    ticker: str,
    side: str,
    market_date: date,
) -> bool:
    """
    Return True if a PENDING order already exists for ticker/side on
    market_date. Prevents duplicate orders within the same trading session.
    """
    count = session.execute(
        select(func.count())
        .select_from(Order)
        .join(TradeDecision, Order.trade_decision_id == TradeDecision.id)
        .where(
            TradeDecision.ticker == ticker,
            TradeDecision.market_date == market_date,
            Order.side == side,
            Order.status == OrderStatus.PENDING,
        )
    ).scalar()
    return (count or 0) > 0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def evaluate_signal(
    session: Session,
    *,
    portfolio: Portfolio,
    direction: str,
    ticker: str,
    confidence: Decimal,
    snapshot_price: Decimal | None,
    market_date: date,
    now: datetime,
) -> RiskDecision:
    """
    Evaluate a signal against current portfolio state and risk parameters.

    Returns a RiskDecision. If decision == DecisionType.REJECTED, reason_code
    explains why. Does not commit or mutate any ORM object.
    """
    cfg = _effective_settings(portfolio)

    # --- Portfolio state snapshot (computed once for auditability) ---
    cash           = compute_cash(session)
    total_value    = portfolio.cached_total_value or cash
    open_positions = get_open_positions(session)
    daily_exposure = _daily_buy_exposure(session, market_date)
    daily_limit    = (cfg["max_daily_new_exposure_pct"] * total_value).quantize(_DOLLARS)

    risk_snapshot: dict[str, Any] = {
        "cash":                str(cash),
        "total_value":         str(total_value),
        "open_position_count": len(open_positions),
        "daily_exposure_used": str(daily_exposure),
        "daily_exposure_limit":str(daily_limit),
    }

    def _reject(reason: str) -> RiskDecision:
        return RiskDecision(
            decision=DecisionType.REJECTED,
            reason_code=reason,
            requested_notional=Decimal("0"),
            approved_notional=Decimal("0"),
            requested_qty=Decimal("0"),
            approved_qty=Decimal("0"),
            risk_snapshot=risk_snapshot,
        )

    # 1. Strategy / trading kill-switches
    if not cfg["strategy_enabled"]:
        return _reject(RejectionReason.STRATEGY_DISABLED)
    if not cfg["trading_enabled"]:
        return _reject(RejectionReason.TRADING_DISABLED)

    # 2. HOLD signals are recorded but never acted on
    if direction == SignalDirection.HOLD:
        return RiskDecision(
            decision=DecisionType.HOLD,
            reason_code=RejectionReason.HOLD_SIGNAL,
            requested_notional=Decimal("0"),
            approved_notional=Decimal("0"),
            requested_qty=Decimal("0"),
            approved_qty=Decimal("0"),
            risk_snapshot=risk_snapshot,
        )

    # 3. Confidence threshold
    if confidence < cfg["confidence_threshold"]:
        return _reject(RejectionReason.CONFIDENCE_BELOW_THRESHOLD)

    # -------------------------------------------------------------------
    # BUY path
    # -------------------------------------------------------------------
    if direction == SignalDirection.BUY:
        position: Position | None = get_position(session, ticker)

        # 4. New-position gates
        if position is None:
            if not cfg["allow_new_positions"]:
                return _reject(RejectionReason.NEW_POSITIONS_DISABLED)
            if len(open_positions) >= cfg["max_positions"]:
                return _reject(RejectionReason.MAX_POSITIONS_REACHED)

        # 5. Averaging-down check (unconditional when position exists and
        #    allow_averaging_down is False — no price comparison)
        if position is not None and not cfg["allow_averaging_down"]:
            return _reject(RejectionReason.AVERAGING_DOWN_BLOCKED)

        # 6. Duplicate pending BUY check (scoped to market_date)
        if _has_pending_order(session, ticker, OrderSide.BUY, market_date):
            return _reject(RejectionReason.DUPLICATE_SIGNAL)

        # 7. Cooldown check (wall-clock)
        last_sell = _last_sell_ts(session, ticker)
        if last_sell is not None:
            if (now - last_sell) < timedelta(hours=cfg["cooldown_hours"]):
                return _reject(RejectionReason.TICKER_IN_COOLDOWN)

        # 8. Snapshot price required and must be positive
        if snapshot_price is None or snapshot_price <= Decimal("0"):
            return _reject(RejectionReason.NO_PRICE_SNAPSHOT)

        # 9. Cash headroom
        cash_reserve    = (cfg["min_cash_pct"] * total_value).quantize(_DOLLARS)
        available_cash  = (cash - cash_reserve).quantize(_DOLLARS)
        daily_remaining = (daily_limit - daily_exposure).quantize(_DOLLARS)

        # 10. Early exits — limits already exhausted before this trade
        if available_cash <= Decimal("0"):
            return _reject(RejectionReason.CASH_RESERVE_BREACH)
        if daily_remaining <= Decimal("0"):
            return _reject(RejectionReason.DAILY_EXPOSURE_LIMIT)

        # 11. Concentration sizing
        existing_value = (
            (position.qty * snapshot_price).quantize(_DOLLARS)
            if position is not None
            else Decimal("0")
        )
        concentration_cap  = (cfg["max_concentration_pct"] * total_value).quantize(_DOLLARS)
        concentration_room = (concentration_cap - existing_value).quantize(_DOLLARS)

        # Confidence-weighted target; capped by concentration room
        requested_notional = min(
            (confidence * cfg["max_concentration_pct"] * total_value).quantize(_DOLLARS),
            concentration_room,
        )

        if concentration_room <= Decimal("0") or requested_notional <= Decimal("0"):
            return _reject(RejectionReason.CONCENTRATION_LIMIT)

        # 12. Clamp approved notional to available resources
        approved_notional = requested_notional
        sizing_adjustments: list[str] = []

        if approved_notional > daily_remaining:
            approved_notional = daily_remaining
            sizing_adjustments.append("clamped_to_daily_room")
        if approved_notional > available_cash:
            approved_notional = available_cash
            sizing_adjustments.append("clamped_to_available_cash")
        approved_notional = approved_notional.quantize(_DOLLARS)

        # 13. Floor to whole shares
        requested_qty = (requested_notional / snapshot_price).to_integral_value(
            rounding=ROUND_DOWN
        )
        approved_qty = Decimal(int(approved_notional / snapshot_price))

        # Approved allocation too small to buy even one share at current price
        if approved_qty <= Decimal("0"):
            return _reject(RejectionReason.CONCENTRATION_LIMIT)

        # 14. Minimum order notional
        final_notional = (approved_qty * snapshot_price).quantize(_DOLLARS)
        if final_notional < cfg["min_order_notional"]:
            return _reject(RejectionReason.MIN_ORDER_TOO_SMALL)

        buy_snapshot = {
            **risk_snapshot,
            "cash_reserve":       str(cash_reserve),
            "available_cash":     str(available_cash),
            "daily_remaining":    str(daily_remaining),
            "concentration_cap":  str(concentration_cap),
            "concentration_room": str(concentration_room),
            "snapshot_price":     str(snapshot_price),
        }

        return RiskDecision(
            decision=DecisionType.BUY,
            reason_code=None,
            requested_notional=requested_notional,
            approved_notional=final_notional,
            requested_qty=requested_qty,
            approved_qty=approved_qty,
            risk_snapshot=buy_snapshot,
            sizing_adjustments=sizing_adjustments,
        )

    # -------------------------------------------------------------------
    # SELL path
    # -------------------------------------------------------------------
    if direction == SignalDirection.SELL:
        position = get_position(session, ticker)
        if position is None:
            return _reject(RejectionReason.NO_POSITION_TO_SELL)

        # Duplicate pending SELL check (scoped to market_date)
        if _has_pending_order(session, ticker, OrderSide.SELL, market_date):
            return _reject(RejectionReason.DUPLICATE_SIGNAL)

        # Snapshot price required and must be positive
        if snapshot_price is None or snapshot_price <= Decimal("0"):
            return _reject(RejectionReason.NO_PRICE_SNAPSHOT)

        sell_notional = (position.qty * snapshot_price).quantize(_DOLLARS)

        return RiskDecision(
            decision=DecisionType.SELL,
            reason_code=None,
            requested_notional=sell_notional,
            approved_notional=sell_notional,
            requested_qty=position.qty,
            approved_qty=position.qty,
            risk_snapshot=risk_snapshot,
        )

    # Unreachable with valid SignalDirection values
    return _reject(RejectionReason.STRATEGY_DISABLED)
