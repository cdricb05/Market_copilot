"""
engine/portfolio.py — Low-level portfolio accounting operations.

All public functions accept a live SQLAlchemy Session and operate directly
on ORM objects. None of them commit; that responsibility belongs to the caller.

Authoritative cash balance is always SUM(cash_ledger.amount).
portfolio.cached_cash is a read-optimised cache — never use it as truth.

All monetary arithmetic uses Python Decimal. Rounding conventions:
    qty        → quantize to Decimal("0.00000001")  (Numeric 18,8)
    avg_cost   → quantize to Decimal("0.000001")    (Numeric 18,6)
    cost_basis → quantize to Decimal("0.01")        (Numeric 18,2)
    cash       → quantize to Decimal("0.01")        (Numeric 18,2)
"""
from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from paper_trader.db.models import CashLedger, Portfolio, Position

_QTY     = Decimal("0.00000001")
_PRICE   = Decimal("0.000001")
_DOLLARS = Decimal("0.01")


# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------

def get_portfolio(session: Session) -> Portfolio:
    """Return the single portfolio row. Raises RuntimeError if not seeded."""
    portfolio = session.execute(select(Portfolio)).scalar_one_or_none()
    if portfolio is None:
        raise RuntimeError(
            "Portfolio row not found. Run scripts/seed.py to initialise."
        )
    return portfolio


def compute_cash(session: Session) -> Decimal:
    """
    Return the authoritative cash balance: SUM(cash_ledger.amount).
    Returns Decimal("0.00") when the ledger is empty (pre-seed state).
    Never use portfolio.cached_cash as the source of truth.
    """
    result = session.execute(select(func.sum(CashLedger.amount))).scalar()
    return Decimal(result).quantize(_DOLLARS) if result is not None else Decimal("0.00")


def get_open_positions(session: Session) -> list[Position]:
    """Return all open position rows. Empty list when no positions are held."""
    return list(session.execute(select(Position)).scalars().all())


def get_position(session: Session, ticker: str) -> Position | None:
    """Return the position for ticker, or None if not currently held."""
    return session.execute(
        select(Position).where(Position.ticker == ticker)
    ).scalar_one_or_none()


# ---------------------------------------------------------------------------
# Cash ledger
# ---------------------------------------------------------------------------

def append_cash_entry(
    session: Session,
    *,
    portfolio_id: int,
    entry_type: str,
    amount: Decimal,
    trade_id: uuid.UUID | None = None,
    order_id: uuid.UUID | None = None,
    job_run_id: uuid.UUID | None = None,
    description: str | None = None,
) -> CashLedger:
    """
    Append an immutable cash ledger entry and add it to the session.

    amount must be non-zero (enforced by the DB constraint):
        positive = cash in  (INITIAL_CAPITAL, SELL_CREDIT, DIVIDEND_CREDIT)
        negative = cash out (BUY_DEBIT, COMMISSION_DEBIT)

    Does not commit. Caller is responsible for session.commit().
    """
    if amount == Decimal("0"):
        raise ValueError("Cash ledger amount must be non-zero.")
    entry = CashLedger(
        portfolio_id=portfolio_id,
        entry_type=entry_type,
        amount=amount.quantize(_DOLLARS),
        trade_id=trade_id,
        order_id=order_id,
        job_run_id=job_run_id,
        description=description,
    )
    session.add(entry)
    return entry


# ---------------------------------------------------------------------------
# Position management
# ---------------------------------------------------------------------------

def open_position(
    session: Session,
    *,
    ticker: str,
    qty: Decimal,
    fill_price: Decimal,
    now: datetime,
) -> Position:
    """
    Create a new Position row for a ticker not currently held.

    avg_cost = fill_price (first fill; WAC collapses to fill price).
    cost_basis = avg_cost * qty, rounded to cents.

    Raises ValueError if qty or fill_price are not positive, or if a
    position for ticker already exists.
    Does not commit.
    """
    if qty <= Decimal("0"):
        raise ValueError(f"open_position: qty must be > 0, got {qty!r}.")
    if fill_price <= Decimal("0"):
        raise ValueError(f"open_position: fill_price must be > 0, got {fill_price!r}.")
    if get_position(session, ticker) is not None:
        raise ValueError(
            f"Position for {ticker!r} already exists. "
            "Use update_position_wac() to add to an existing position."
        )
    qty = qty.quantize(_QTY)
    avg_cost = fill_price.quantize(_PRICE)
    cost_basis = (avg_cost * qty).quantize(_DOLLARS)
    position = Position(
        ticker=ticker,
        qty=qty,
        avg_cost=avg_cost,
        cost_basis=cost_basis,
        opened_at=now,
        last_updated=now,
    )
    session.add(position)
    return position


def update_position_wac(
    position: Position,
    *,
    fill_qty: Decimal,
    fill_price: Decimal,
    now: datetime,
) -> None:
    """
    Update an existing position on a BUY fill using the WAC formula.

    new_avg_cost  = (existing_qty * existing_avg_cost + fill_qty * fill_price)
                    / (existing_qty + fill_qty)
    new_cost_basis = new_avg_cost * new_qty

    Raises ValueError if fill_qty or fill_price are not positive.
    Mutates the Position object in place. Does not commit.
    No Session parameter needed — the object is already tracked.
    """
    if fill_qty <= Decimal("0"):
        raise ValueError(f"update_position_wac: fill_qty must be > 0, got {fill_qty!r}.")
    if fill_price <= Decimal("0"):
        raise ValueError(f"update_position_wac: fill_price must be > 0, got {fill_price!r}.")
    fill_qty = fill_qty.quantize(_QTY)
    new_qty = position.qty + fill_qty
    new_avg_cost = (
        (position.qty * position.avg_cost + fill_qty * fill_price) / new_qty
    ).quantize(_PRICE)
    position.qty = new_qty
    position.avg_cost = new_avg_cost
    position.cost_basis = (new_avg_cost * new_qty).quantize(_DOLLARS)
    position.last_updated = now


def reduce_position(
    session: Session,
    position: Position,
    *,
    fill_qty: Decimal,
    now: datetime,
) -> bool:
    """
    Reduce a position on a SELL fill.

    If fill_qty equals position.qty the row is deleted (full close) and True
    is returned. Otherwise qty and cost_basis are reduced, avg_cost is
    unchanged (WAC does not change on a SELL), and False is returned.

    Raises ValueError if fill_qty is not positive or exceeds the held quantity.
    Does not commit.
    """
    if fill_qty <= Decimal("0"):
        raise ValueError(f"reduce_position: fill_qty must be > 0, got {fill_qty!r}.")
    fill_qty = fill_qty.quantize(_QTY)
    if fill_qty > position.qty:
        raise ValueError(
            f"Cannot sell {fill_qty} of {position.ticker!r}: only {position.qty} held."
        )
    if fill_qty == position.qty:
        session.delete(position)
        return True
    new_qty = position.qty - fill_qty
    position.qty = new_qty
    position.cost_basis = (position.avg_cost * new_qty).quantize(_DOLLARS)
    position.last_updated = now
    return False


# ---------------------------------------------------------------------------
# Cache refresh
# ---------------------------------------------------------------------------

def refresh_portfolio_cache(
    session: Session,
    portfolio: Portfolio,
    *,
    price_map: dict[str, Decimal],
    now: datetime,
) -> None:
    """
    Recompute and store portfolio.cached_cash and portfolio.cached_total_value.

    price_map must contain a positive price for every open ticker. Raises
    ValueError listing all tickers missing from price_map, then raises
    ValueError listing all ticker/price pairs whose price is <= 0, before
    any computation is performed.

    Mutates the Portfolio object in place. Does not commit.
    """
    positions = get_open_positions(session)
    missing = [pos.ticker for pos in positions if pos.ticker not in price_map]
    if missing:
        raise ValueError(
            f"price_map is missing prices for open tickers: {missing}"
        )
    invalid = [
        (pos.ticker, price_map[pos.ticker])
        for pos in positions
        if price_map[pos.ticker] <= Decimal("0")
    ]
    if invalid:
        raise ValueError(
            f"price_map contains non-positive prices for open tickers: {invalid}"
        )
    cash = compute_cash(session)
    positions_value = sum(
        (pos.qty * price_map[pos.ticker] for pos in positions),
        Decimal("0"),
    ).quantize(_DOLLARS)
    portfolio.cached_cash = cash
    portfolio.cached_total_value = (cash + positions_value).quantize(_DOLLARS)
    portfolio.cached_as_of_ts = now
