"""
workflows/snapshot.py — Post-market portfolio snapshot workflow.

run_snapshot_workflow() is the single entry point. It:
    1. Enforces idempotency via JobRun.idempotency_key.
    2. Acquires the portfolio advisory lock for the duration of the run.
    3. Computes portfolio state from authoritative sources:
           cash                  — SUM(cash_ledger.amount)
           positions + prices    — positions table + price_snapshots
           realized P&L          — SUM(trades.realized_pnl) for all SELLs
           daily exposure        — SUM(trade_decisions.approved_notional)
                                   for BUY decisions with PENDING or FILLED orders
    4. Optionally populates benchmark fields when benchmark price data is available.
    5. Writes exactly one PortfolioSnapshot row per market_date.
    6. Marks the JobRun COMPLETED with a result summary, or FAILED on error.

Idempotency contract:
    COMPLETED → return cached result_summary immediately (no work).
    RUNNING   → raise RuntimeError (concurrent execution guard).
    FAILED    → raise RuntimeError; caller must supply a new idempotency_key
                or manually clean up the failed run before retrying.

Raises ValueError if any open position has no available price snapshot.
Benchmark fields are all left NULL when benchmark price data is absent — this
is not an error condition.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from paper_trader.constants import (
    PORTFOLIO_ADVISORY_LOCK_KEY,
    DecisionType,
    JobRunStatus,
    OrderStatus,
    WorkflowType,
)
from paper_trader.db.models import (
    BenchmarkPrice,
    JobRun,
    Order,
    PortfolioSnapshot,
    PriceSnapshot,
    Trade,
    TradeDecision,
)
from paper_trader.db.session import get_dedicated_session
from paper_trader.engine.portfolio import (
    compute_cash,
    get_open_positions,
    get_portfolio,
)

_PRICE   = Decimal("0.000001")
_DOLLARS = Decimal("0.01")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _latest_price(session: Session, ticker: str) -> Decimal | None:
    """Return the most recent price_snapshots price for ticker, or None."""
    result = session.execute(
        select(PriceSnapshot.price)
        .where(PriceSnapshot.ticker == ticker)
        .order_by(PriceSnapshot.snapshot_ts.desc())
        .limit(1)
    ).scalar()
    return Decimal(str(result)).quantize(_PRICE) if result is not None else None


def _latest_benchmark_price(session: Session, ticker: str) -> Decimal | None:
    """Return the most recent benchmark_prices price for ticker, or None."""
    result = session.execute(
        select(BenchmarkPrice.price)
        .where(BenchmarkPrice.ticker == ticker)
        .order_by(BenchmarkPrice.snapshot_ts.desc())
        .limit(1)
    ).scalar()
    return Decimal(str(result)).quantize(_PRICE) if result is not None else None


def _benchmark_inception_price(
    session: Session,
    ticker: str,
    inception_date: date,
) -> Decimal | None:
    """
    Return the most recent benchmark price on or before inception_date, or None.

    Used as the cost-basis reference for computing how a benchmark investment
    made on portfolio inception day would have grown.
    """
    result = session.execute(
        select(BenchmarkPrice.price)
        .where(
            BenchmarkPrice.ticker == ticker,
            BenchmarkPrice.market_date <= inception_date,
        )
        .order_by(BenchmarkPrice.market_date.desc())
        .limit(1)
    ).scalar()
    return Decimal(str(result)).quantize(_PRICE) if result is not None else None


def _realized_pnl_cumulative(session: Session) -> Decimal:
    """
    Return the running sum of all realised P&L from SELL trades.

    Uses SUM(trades.realized_pnl) where realized_pnl IS NOT NULL.
    Returns Decimal("0.00") when no SELL trades have been executed.
    """
    result = session.execute(
        select(func.sum(Trade.realized_pnl))
        .where(Trade.realized_pnl.is_not(None))
    ).scalar()
    return Decimal(str(result)).quantize(_DOLLARS) if result is not None else Decimal("0.00")


def _daily_new_exposure(session: Session, market_date: date) -> Decimal:
    """
    Return total approved BUY notional for market_date.

    Joins trade_decisions to orders and restricts to Order.status IN (PENDING,
    FILLED) so that expired, cancelled, and failed orders are excluded.
    Consistent with the exposure cap logic used by engine/risk.py.
    Returns Decimal("0.00") when no qualifying BUY decisions exist for the date.
    """
    result = session.execute(
        select(func.sum(TradeDecision.approved_notional))
        .join(Order, Order.trade_decision_id == TradeDecision.id)
        .where(
            TradeDecision.decision == DecisionType.BUY,
            TradeDecision.market_date == market_date,
            TradeDecision.approved_notional.is_not(None),
            Order.status.in_([OrderStatus.PENDING, OrderStatus.FILLED]),
        )
    ).scalar()
    return Decimal(str(result)).quantize(_DOLLARS) if result is not None else Decimal("0.00")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_snapshot_workflow(
    *,
    idempotency_key: str,
    market_date: date,
    now: datetime,
) -> dict:
    """
    Create a post-market portfolio snapshot for market_date.

    Returns the result_summary dict on success. Raises RuntimeError on
    idempotency conflicts or lock contention. All other exceptions mark the
    JobRun FAILED before re-raising.
    """
    with get_dedicated_session() as session:

        # Initialise before try so the outer except can reference it safely.
        job_run: JobRun | None = None

        try:
            # ------------------------------------------------------------------
            # Idempotency check
            # ------------------------------------------------------------------
            existing = session.execute(
                select(JobRun).where(JobRun.idempotency_key == idempotency_key)
            ).scalar_one_or_none()

            if existing is not None:
                if existing.status == JobRunStatus.COMPLETED:
                    return existing.result_summary or {}

                if existing.status == JobRunStatus.RUNNING:
                    raise RuntimeError(
                        f"Snapshot job run {idempotency_key!r} is currently RUNNING. "
                        "Another worker may be processing this workflow. "
                        "If the run is stale, update its status manually."
                    )

                if existing.status == JobRunStatus.FAILED:
                    raise RuntimeError(
                        f"Snapshot job run {idempotency_key!r} previously FAILED "
                        f"(id={existing.id}). Inspect job_runs.error_detail, "
                        "fix the underlying issue, then retry with a new "
                        "idempotency_key or manually delete the failed row."
                    )

            # ------------------------------------------------------------------
            # Create the JobRun and commit before acquiring the lock so that
            # a concurrent caller sees RUNNING and backs off immediately.
            # ------------------------------------------------------------------
            job_run = JobRun(
                idempotency_key=idempotency_key,
                workflow_type=WorkflowType.POST_MARKET,
                market_date=market_date,
                status=JobRunStatus.RUNNING,
                started_at=now,
            )
            session.add(job_run)
            session.commit()

            # ------------------------------------------------------------------
            # Acquire portfolio advisory lock (connection-scoped).
            # ------------------------------------------------------------------
            acquired = session.execute(
                text("SELECT pg_try_advisory_lock(:key)"),
                {"key": PORTFOLIO_ADVISORY_LOCK_KEY},
            ).scalar()
            if not acquired:
                raise RuntimeError(
                    "Could not acquire portfolio advisory lock. "
                    "Another workflow is currently running."
                )

            try:
                portfolio = get_portfolio(session)
                positions = get_open_positions(session)

                # --------------------------------------------------------------
                # Build price map for all open positions.
                # Fail fast if any position is missing a price — a partial
                # snapshot would produce incorrect total_value and unrealized_pnl.
                # --------------------------------------------------------------
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
                        f"Cannot create snapshot: no price snapshot found for "
                        f"open tickers: {missing}"
                    )

                # --------------------------------------------------------------
                # Core portfolio values — all from authoritative sources.
                # --------------------------------------------------------------
                cash = compute_cash(session)

                positions_value = sum(
                    (pos.qty * price_map[pos.ticker] for pos in positions),
                    Decimal("0"),
                ).quantize(_DOLLARS)

                total_value = (cash + positions_value).quantize(_DOLLARS)

                unrealized_pnl = sum(
                    (
                        (price_map[pos.ticker] - pos.avg_cost) * pos.qty
                        for pos in positions
                    ),
                    Decimal("0"),
                ).quantize(_DOLLARS)

                realized_pnl_cum = _realized_pnl_cumulative(session)
                daily_exposure   = _daily_new_exposure(session, market_date)

                # --------------------------------------------------------------
                # Point-in-time position detail for historical charts.
                # NULL when no positions are open (not an empty list).
                # --------------------------------------------------------------
                positions_detail: list[dict] | None = None
                if positions:
                    positions_detail = [
                        {
                            "ticker":         pos.ticker,
                            "qty":            str(pos.qty),
                            "avg_cost":       str(pos.avg_cost),
                            "current_price":  str(price_map[pos.ticker]),
                            "market_value":   str(
                                (pos.qty * price_map[pos.ticker]).quantize(_DOLLARS)
                            ),
                            "unrealized_pnl": str(
                                (
                                    (price_map[pos.ticker] - pos.avg_cost) * pos.qty
                                ).quantize(_DOLLARS)
                            ),
                        }
                        for pos in positions
                    ]

                # --------------------------------------------------------------
                # Optional benchmark fields.
                # Any absence of data leaves all four benchmark columns NULL.
                # No error is raised for missing benchmark data.
                # --------------------------------------------------------------
                cfg = portfolio.config or {}
                benchmark_ticker    = cfg.get("benchmark_ticker") or None
                benchmark_price_val = None
                benchmark_inc_price = None
                benchmark_value     = None
                portfolio_vs_bench  = None

                if benchmark_ticker:
                    benchmark_price_val = _latest_benchmark_price(session, benchmark_ticker)

                    if benchmark_price_val is not None:
                        # Inception price: prefer config override, then table lookup.
                        cfg_inc = cfg.get("benchmark_inception_price")
                        if cfg_inc is not None:
                            benchmark_inc_price = Decimal(str(cfg_inc)).quantize(_PRICE)
                        else:
                            benchmark_inc_price = _benchmark_inception_price(
                                session, benchmark_ticker, portfolio.inception_date
                            )

                        if benchmark_inc_price is not None and benchmark_inc_price > Decimal("0"):
                            benchmark_value = (
                                portfolio.initial_capital
                                / benchmark_inc_price
                                * benchmark_price_val
                            ).quantize(_DOLLARS)
                            portfolio_vs_bench = (
                                total_value - benchmark_value
                            ).quantize(_DOLLARS)

                # --------------------------------------------------------------
                # Write the PortfolioSnapshot row.
                # --------------------------------------------------------------
                session.add(PortfolioSnapshot(
                    job_run_id=job_run.id,
                    snapshot_ts=now,
                    market_date=market_date,
                    cash=cash,
                    positions_value=positions_value,
                    total_value=total_value,
                    unrealized_pnl=unrealized_pnl,
                    realized_pnl_cumulative=realized_pnl_cum,
                    open_position_count=len(positions),
                    daily_new_exposure=daily_exposure,
                    benchmark_ticker=benchmark_ticker,
                    benchmark_price=benchmark_price_val,
                    benchmark_inception_price=benchmark_inc_price,
                    benchmark_value=benchmark_value,
                    portfolio_vs_benchmark=portfolio_vs_bench,
                    positions_detail=positions_detail,
                ))

                # --------------------------------------------------------------
                # Finalise JobRun with a result summary for idempotent replay.
                # --------------------------------------------------------------
                result_summary = {
                    "total_value":             str(total_value),
                    "cash":                    str(cash),
                    "positions_value":         str(positions_value),
                    "unrealized_pnl":          str(unrealized_pnl),
                    "realized_pnl_cumulative": str(realized_pnl_cum),
                    "open_position_count":     len(positions),
                    "benchmark_ticker":        benchmark_ticker,
                    "portfolio_vs_benchmark":  (
                        str(portfolio_vs_bench)
                        if portfolio_vs_bench is not None
                        else None
                    ),
                }

                job_run.status         = JobRunStatus.COMPLETED
                job_run.completed_at   = now
                job_run.result_summary = result_summary
                session.commit()

                return result_summary

            finally:
                # Release the advisory lock regardless of success or failure.
                #
                # session.rollback() is called first to discard any uncommitted
                # ORM state (e.g. a PortfolioSnapshot added but not committed on
                # the failure path). After a successful session.commit() above,
                # rollback() is a no-op. This prevents the lock-release commit
                # from accidentally persisting business data on the error path.
                try:
                    session.rollback()
                    session.execute(
                        text("SELECT pg_advisory_unlock(:key)"),
                        {"key": PORTFOLIO_ADVISORY_LOCK_KEY},
                    )
                    session.commit()
                except Exception:
                    session.rollback()

        except Exception as exc:
            # Only attempt FAILED transition when we own the JobRun row.
            if job_run is not None:
                try:
                    job_run.status       = JobRunStatus.FAILED
                    job_run.error_detail = str(exc)[:2000]
                    job_run.completed_at = now
                    session.commit()
                except Exception:
                    session.rollback()
            raise
