"""
engine/reconciler.py — Fill-cycle execution and portfolio accounting.

run_fill_cycle() is the single entry point. It processes all PENDING orders
for a given market_date in requested_at order, expires stale ones, attempts
to fill valid ones, and refreshes the portfolio cache at the end.

Responsibilities:
    - TTL expiry of orders whose requested_at + order_ttl_hours <= now
    - Latest-price lookup from price_snapshots (no external API calls)
    - Slippage-adjusted fill price derivation
    - Authoritative cash check for BUY fills
    - Position existence and qty check for SELL fills
    - Atomic per-fill DB writes: Order update, Trade row, Position update,
      CashLedger entries
    - Portfolio cache refresh after the fill loop

Out of scope:
    - Pre-trade risk evaluation (engine/risk.py)
    - portfolio_snapshots creation (post-market workflow)
    - External price fetching
    - Partial fills (v1 fills entire requested_qty or not at all)
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from paper_trader.constants import CashEntryType, OrderSide, OrderStatus
from paper_trader.db.models import Order, Portfolio, PriceSnapshot, Trade
from paper_trader.engine.portfolio import (
    append_cash_entry,
    compute_cash,
    get_open_positions,
    get_position,
    open_position,
    reduce_position,
    refresh_portfolio_cache,
    update_position_wac,
)

_QTY     = Decimal("0.00000001")
_PRICE   = Decimal("0.000001")
_DOLLARS = Decimal("0.01")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _reconciler_cfg(portfolio: Portfolio) -> dict[str, Any]:
    """
    Read fill-cycle parameters from portfolio.config JSONB.
    Falls back to hard-coded v1 defaults; Settings has no execution-layer fields.
    """
    cfg: dict[str, Any] = portfolio.config or {}
    return {
        "slippage_bps":    int(cfg.get("slippage_bps", 10)),
        "commission_flat": Decimal(str(cfg.get("commission_flat", "1.00"))),
        "order_ttl_hours": int(cfg.get("order_ttl_hours", 24)),
    }


def _latest_price(session: Session, ticker: str) -> Decimal | None:
    """
    Return the most recent snapshot price for ticker, or None.
    Uses global snapshot_ts ordering — no market_date filter — so a PENDING
    order from an earlier session can still fill against a current price.
    """
    result = session.execute(
        select(PriceSnapshot.price)
        .where(PriceSnapshot.ticker == ticker)
        .order_by(PriceSnapshot.snapshot_ts.desc())
        .limit(1)
    ).scalar()
    return Decimal(str(result)).quantize(_PRICE) if result is not None else None


def _do_fill(
    session: Session,
    order: Order,
    *,
    portfolio: Portfolio,
    snapshot_price: Decimal,
    fill_price: Decimal,
    commission: Decimal,
    now: datetime,
    market_date: date,
    job_run_id: uuid.UUID,
) -> None:
    """
    Apply all DB mutations for a single fill. Does not commit.

    Raises ValueError if a fill-time precondition fails (insufficient cash
    for BUY; missing or undersized position for SELL). The caller catches
    ValueError, rolls back, and marks the order FAILED.

    trade_id is pre-generated so cash ledger rows can reference it without
    requiring a flush. Order fields are written last so any ValueError above
    aborts before the order status changes.
    """
    qty = order.requested_qty

    if order.side == OrderSide.BUY:
        fill_cost  = (qty * fill_price).quantize(_DOLLARS)
        total_cost = (fill_cost + commission).quantize(_DOLLARS)

        # Authoritative cash check — never use cached_cash here
        cash = compute_cash(session)
        if cash < total_cost:
            raise ValueError(
                f"Insufficient cash for BUY {order.ticker!r}: "
                f"need {total_cost}, have {cash}."
            )

        # Open new position or WAC-average into existing
        position = get_position(session, order.ticker)
        if position is None:
            position = open_position(
                session,
                ticker=order.ticker,
                qty=qty,
                fill_price=fill_price,
                now=now,
            )
        else:
            update_position_wac(position, fill_qty=qty, fill_price=fill_price, now=now)

        # post-WAC avg_cost is now the cost basis for this fill
        cost_basis_per_share = position.avg_cost
        gross_value = (qty * fill_price).quantize(_DOLLARS)
        net_value   = (gross_value - commission).quantize(_DOLLARS)

        # Pre-generate trade_id so ledger rows can reference it before flush
        trade_id = uuid.uuid4()
        session.add(Trade(
            id=trade_id,
            order_id=order.id,
            job_run_id=job_run_id,
            ticker=order.ticker,
            side=order.side,
            qty=qty,
            snapshot_price=snapshot_price,
            fill_price=fill_price,
            gross_value=gross_value,
            commission=commission,
            net_value=net_value,
            cost_basis_per_share=cost_basis_per_share,
            realized_pnl=None,
            trade_ts=now,
            market_date=market_date,
        ))
        # Force Trade insert before appending CashLedger rows that reference it
        session.flush()

        append_cash_entry(
            session,
            portfolio_id=portfolio.id,
            entry_type=CashEntryType.BUY_DEBIT,
            amount=-fill_cost,
            trade_id=trade_id,
            order_id=order.id,
            job_run_id=job_run_id,
            description=f"BUY {qty} {order.ticker} @ {fill_price}",
        )
        if commission > Decimal("0"):
            append_cash_entry(
                session,
                portfolio_id=portfolio.id,
                entry_type=CashEntryType.COMMISSION_DEBIT,
                amount=-commission,
                trade_id=trade_id,
                order_id=order.id,
                job_run_id=job_run_id,
                description=f"Commission BUY {order.ticker}",
            )

    else:  # SELL
        position = get_position(session, order.ticker)
        if position is None:
            raise ValueError(
                f"No position found for {order.ticker!r} at fill time."
            )
        if position.qty < qty:
            raise ValueError(
                f"Position qty insufficient for SELL {order.ticker!r}: "
                f"need {qty}, have {position.qty}."
            )

        # Capture avg_cost before reduce_position may delete the row
        cost_basis_per_share = position.avg_cost
        gross_value  = (qty * fill_price).quantize(_DOLLARS)
        net_value    = (gross_value - commission).quantize(_DOLLARS)
        realized_pnl = ((fill_price - cost_basis_per_share) * qty).quantize(_DOLLARS)

        reduce_position(session, position, fill_qty=qty, now=now)

        # Pre-generate trade_id so ledger rows can reference it before flush
        trade_id = uuid.uuid4()
        session.add(Trade(
            id=trade_id,
            order_id=order.id,
            job_run_id=job_run_id,
            ticker=order.ticker,
            side=order.side,
            qty=qty,
            snapshot_price=snapshot_price,
            fill_price=fill_price,
            gross_value=gross_value,
            commission=commission,
            net_value=net_value,
            cost_basis_per_share=cost_basis_per_share,
            realized_pnl=realized_pnl,
            trade_ts=now,
            market_date=market_date,
        ))
        # Force Trade insert before appending CashLedger rows that reference it
        session.flush()

        append_cash_entry(
            session,
            portfolio_id=portfolio.id,
            entry_type=CashEntryType.SELL_CREDIT,
            amount=gross_value,
            trade_id=trade_id,
            order_id=order.id,
            job_run_id=job_run_id,
            description=f"SELL {qty} {order.ticker} @ {fill_price}",
        )
        if commission > Decimal("0"):
            append_cash_entry(
                session,
                portfolio_id=portfolio.id,
                entry_type=CashEntryType.COMMISSION_DEBIT,
                amount=-commission,
                trade_id=trade_id,
                order_id=order.id,
                job_run_id=job_run_id,
                description=f"Commission SELL {order.ticker}",
            )

    # Update order fields last — ensures any ValueError above aborts cleanly
    order.status          = OrderStatus.FILLED
    order.filled_qty      = qty
    order.fill_price      = fill_price
    order.filled_at       = now
    order.fill_job_run_id = job_run_id
    order.commission      = commission
    order.slippage_cost   = ((fill_price - snapshot_price) * qty).quantize(_DOLLARS)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_fill_cycle(
    session: Session,
    *,
    portfolio: Portfolio,
    job_run_id: uuid.UUID,
    now: datetime,
    market_date: date,
) -> dict[str, int]:
    """
    Process all PENDING orders for market_date and update portfolio state.

    Each state transition (expire / fill / fail) is committed as its own
    transaction. An unexpected non-ValueError exception from _do_fill rolls
    back the current fill and re-raises; the order remains PENDING.

    Portfolio cache refresh runs after the fill loop. Raises ValueError if
    any open position is missing a latest price snapshot.

    Returns a summary dict: {filled, expired, failed, skipped}.
    The caller must hold the portfolio advisory lock for the full duration.
    """
    cfg        = _reconciler_cfg(portfolio)
    commission = cfg["commission_flat"]
    ttl_hours  = cfg["order_ttl_hours"]
    slippage   = Decimal(str(cfg["slippage_bps"])) / Decimal("10000")

    orders = list(
        session.execute(
            select(Order)
            .where(
                Order.status == OrderStatus.PENDING,
                Order.market_date == market_date,
            )
            .order_by(Order.requested_at)
        ).scalars().all()
    )

    counts: dict[str, int] = {"filled": 0, "expired": 0, "failed": 0, "skipped": 0}

    for order in orders:

        # --- TTL expiry ---
        ttl_deadline = order.requested_at + timedelta(hours=ttl_hours)
        if now >= ttl_deadline:
            order.status = OrderStatus.EXPIRED
            order.notes  = f"TTL exceeded: expired at {ttl_deadline.isoformat()}."
            session.commit()
            counts["expired"] += 1
            continue

        # --- Price lookup — leave PENDING if no snapshot exists ---
        snapshot_price = _latest_price(session, order.ticker)
        if snapshot_price is None:
            counts["skipped"] += 1
            continue

        # --- Slippage-adjusted fill price ---
        if order.side == OrderSide.BUY:
            fill_price = (snapshot_price * (Decimal("1") + slippage)).quantize(_PRICE)
        else:
            fill_price = (snapshot_price * (Decimal("1") - slippage)).quantize(_PRICE)

        # --- Attempt fill ---
        try:
            _do_fill(
                session,
                order,
                portfolio=portfolio,
                snapshot_price=snapshot_price,
                fill_price=fill_price,
                commission=commission,
                now=now,
                market_date=market_date,
                job_run_id=job_run_id,
            )
            session.commit()
            counts["filled"] += 1

        except ValueError as exc:
            order_id = order.id  # Capture ID before rollback
            session.rollback()
            # Re-fetch the order from the database to avoid stale object
            fresh_order = session.execute(
                select(Order).where(Order.id == order_id)
            ).scalar_one()
            fresh_order.status = OrderStatus.FAILED
            fresh_order.notes  = str(exc)
            session.commit()
            counts["failed"] += 1

        except Exception:
            session.rollback()
            raise

    # --- Portfolio cache refresh ---
    positions = get_open_positions(session)
    if not positions:
        refresh_portfolio_cache(session, portfolio, price_map={}, now=now)
        session.commit()
    else:
        price_map: dict[str, Decimal] = {}
        missing: list[str] = []
        for pos in positions:
            price = _latest_price(session, pos.ticker)
            if price is None:
                missing.append(pos.ticker)
            else:
                price_map[pos.ticker] = price

        if missing:
            raise ValueError(
                f"Cannot refresh portfolio cache: no price snapshot found for "
                f"open tickers: {missing}"
            )

        refresh_portfolio_cache(session, portfolio, price_map=price_map, now=now)
        session.commit()

    return counts
