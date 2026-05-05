"""
api/app.py — FastAPI application for paper_trader.

Endpoints:
    GET  /v1/health                — lightweight health check (no auth required)
    GET  /v1/ready                 — readiness probe with database connectivity check (no auth required)
    POST /v1/signals               — ingest a signal batch and run the decision workflow
    POST /v1/fill                  — run the fill cycle for a given market_date
    POST /v1/snapshot              — run the post-market portfolio snapshot workflow
    POST /v1/prices                — bulk-insert price snapshots (manual ingestion)
    POST /v1/benchmark-prices      — bulk-insert benchmark price observations (manual ingestion)
    GET  /v1/positions             — list all open positions
    GET  /v1/orders                — list orders, optionally filtered by status/market_date
    GET  /v1/snapshots             — list all portfolio snapshots, most recent first
    GET  /v1/snapshots/{market_date} — return the portfolio snapshot for a specific date
    GET  /v1/portfolio             — return current portfolio state
    GET  /v1/performance           — inception-to-date performance summary
    GET  /v1/performance/history   — time-series performance history for charting
    GET  /v1/performance/history.csv — same history exported as a CSV file

Authentication: every endpoint except /v1/health and /v1/ready requires the
X-API-Key header to match PAPER_TRADER_SERVICE_API_KEY.

Clock convention: market_date is always derived server-side from the current
UTC timestamp converted to US/Eastern, never trusted from the caller.
"""
from __future__ import annotations

import csv
import io
import pathlib
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, HTTPException, Query, Response, Security, status
from fastapi.responses import RedirectResponse
from fastapi.security import APIKeyHeader
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import func, select, text

from paper_trader.config import get_settings
from paper_trader.constants import (
    PORTFOLIO_ADVISORY_LOCK_KEY,
    JobRunStatus,
    PriceType,
    SessionType,
    WorkflowType,
)
from paper_trader.db.models import (
    BenchmarkPrice,
    JobRun,
    Order,
    Portfolio,
    PortfolioSnapshot,
    Position,
    PriceSnapshot,
)
from paper_trader.db.session import get_dedicated_session, get_session
from paper_trader.engine.market_hours import is_weekday
from paper_trader.engine.portfolio import get_portfolio
from paper_trader.engine.reconciler import run_fill_cycle
from paper_trader.workflows.decision import run_decision_workflow
from paper_trader.workflows.snapshot import run_snapshot_workflow

_EASTERN = ZoneInfo("America/New_York")
_API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=True)

_SERVICE_NAME = "paper_trader"
_SERVICE_VERSION = "1.0.0"

app = FastAPI(title=_SERVICE_NAME, version=_SERVICE_VERSION)


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def _verify_api_key(api_key: str = Security(_API_KEY_HEADER)) -> None:
    if api_key != get_settings().service_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key.",
        )


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class HealthOut(BaseModel):
    status: str
    service: str
    version: str


class ReadyOut(BaseModel):
    status: str
    service: str
    version: str
    database: str


class SignalIn(BaseModel):
    ticker: str
    direction: str
    confidence: Decimal
    signal_ts: datetime
    source_run: str
    raw_payload: dict[str, Any] | None = None


class DecisionRequest(BaseModel):
    idempotency_key: str
    workflow_type: str = WorkflowType.PRE_MARKET
    signals: list[SignalIn]


class DecisionResponse(BaseModel):
    signals_ingested: int
    decisions_made: int
    orders_created: int
    errors: int


class FillRequest(BaseModel):
    idempotency_key: str
    market_date: date | None = Field(
        default=None,
        description="US Eastern market date. Defaults to today's Eastern date.",
    )


class FillResponse(BaseModel):
    filled: int
    expired: int
    failed: int
    skipped: int


class SnapshotRequest(BaseModel):
    idempotency_key: str
    market_date: date | None = Field(
        default=None,
        description="US Eastern market date. Defaults to today's Eastern date.",
    )


class SnapshotWorkflowResponse(BaseModel):
    total_value: str
    cash: str
    positions_value: str
    unrealized_pnl: str
    realized_pnl_cumulative: str
    open_position_count: int
    benchmark_ticker: str | None
    portfolio_vs_benchmark: str | None


class PriceSnapshotIn(BaseModel):
    ticker: str
    price: Decimal
    session_type: str = SessionType.MANUAL
    price_type: str = PriceType.LAST
    exchange: str | None = None
    data_source: str | None = None
    snapshot_ts: datetime | None = Field(
        default=None,
        description="Defaults to server UTC clock if omitted.",
    )
    market_date: date | None = Field(
        default=None,
        description="Defaults to US-Eastern date of snapshot_ts if omitted.",
    )


class PricesRequest(BaseModel):
    snapshots: list[PriceSnapshotIn]


class PricesResponse(BaseModel):
    inserted: int


class BenchmarkPriceIn(BaseModel):
    ticker: str
    price: Decimal
    session_type: str = SessionType.MANUAL
    snapshot_ts: datetime | None = Field(
        default=None,
        description="Defaults to server UTC clock if omitted.",
    )
    market_date: date | None = Field(
        default=None,
        description="Defaults to US-Eastern date of snapshot_ts if omitted.",
    )


class BenchmarkPricesRequest(BaseModel):
    prices: list[BenchmarkPriceIn]


class BenchmarkPricesResponse(BaseModel):
    inserted: int


class PositionOut(BaseModel):
    id: str
    ticker: str
    qty: str
    avg_cost: str
    cost_basis: str
    opened_at: datetime
    last_updated: datetime


class OrderOut(BaseModel):
    id: str
    ticker: str
    side: str
    status: str
    market_date: date
    requested_qty: str
    filled_qty: str | None
    requested_at: datetime
    filled_at: datetime | None
    fill_price: str | None
    commission: str | None
    notes: str | None


class SnapshotOut(BaseModel):
    id: str
    market_date: date
    snapshot_ts: datetime
    cash: str
    positions_value: str
    total_value: str
    unrealized_pnl: str
    realized_pnl_cumulative: str
    open_position_count: int
    daily_new_exposure: str | None
    benchmark_ticker: str | None
    benchmark_price: str | None
    benchmark_inception_price: str | None
    benchmark_value: str | None
    portfolio_vs_benchmark: str | None
    positions_detail: list[dict] | None


class PortfolioOut(BaseModel):
    id: int
    inception_date: date
    initial_capital: str
    cached_cash: str
    cached_total_value: str
    cached_as_of_ts: datetime | None
    strategy_enabled: bool
    trading_enabled: bool
    allow_new_positions: bool


class PerformanceOut(BaseModel):
    first_snapshot_date: date
    latest_snapshot_date: date
    initial_capital: str
    latest_total_value: str
    absolute_return: str
    return_pct: str | None
    benchmark_ticker: str | None
    benchmark_return_pct: str | None
    excess_return_pct: str | None


class PerformanceHistoryItem(BaseModel):
    market_date: date
    total_value: str
    cash: str
    positions_value: str
    unrealized_pnl: str
    realized_pnl_cumulative: str
    benchmark_ticker: str | None
    benchmark_value: str | None
    portfolio_vs_benchmark: str | None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_FILL_RESULT_KEYS = frozenset({"filled", "expired", "failed", "skipped"})

_HISTORY_CSV_COLUMNS = (
    "market_date",
    "total_value",
    "cash",
    "positions_value",
    "unrealized_pnl",
    "realized_pnl_cumulative",
    "benchmark_ticker",
    "benchmark_value",
    "portfolio_vs_benchmark",
)


def _now_and_date() -> tuple[datetime, date]:
    """Return (UTC now, US-Eastern market_date) from a single clock read."""
    now = datetime.now(tz=timezone.utc)
    return now, now.astimezone(_EASTERN).date()


def _to_snapshot_out(snap: PortfolioSnapshot) -> SnapshotOut:
    """Convert a PortfolioSnapshot ORM row to a SnapshotOut response model."""
    return SnapshotOut(
        id=str(snap.id),
        market_date=snap.market_date,
        snapshot_ts=snap.snapshot_ts,
        cash=str(snap.cash),
        positions_value=str(snap.positions_value),
        total_value=str(snap.total_value),
        unrealized_pnl=str(snap.unrealized_pnl),
        realized_pnl_cumulative=str(snap.realized_pnl_cumulative),
        open_position_count=snap.open_position_count,
        daily_new_exposure=(
            str(snap.daily_new_exposure)
            if snap.daily_new_exposure is not None else None
        ),
        benchmark_ticker=snap.benchmark_ticker,
        benchmark_price=(
            str(snap.benchmark_price)
            if snap.benchmark_price is not None else None
        ),
        benchmark_inception_price=(
            str(snap.benchmark_inception_price)
            if snap.benchmark_inception_price is not None else None
        ),
        benchmark_value=(
            str(snap.benchmark_value)
            if snap.benchmark_value is not None else None
        ),
        portfolio_vs_benchmark=(
            str(snap.portfolio_vs_benchmark)
            if snap.portfolio_vs_benchmark is not None else None
        ),
        positions_detail=snap.positions_detail,
    )


def _to_performance_history_item(snap: PortfolioSnapshot) -> PerformanceHistoryItem:
    """Convert a PortfolioSnapshot ORM row to a PerformanceHistoryItem response model."""
    return PerformanceHistoryItem(
        market_date=snap.market_date,
        total_value=str(snap.total_value),
        cash=str(snap.cash),
        positions_value=str(snap.positions_value),
        unrealized_pnl=str(snap.unrealized_pnl),
        realized_pnl_cumulative=str(snap.realized_pnl_cumulative),
        benchmark_ticker=snap.benchmark_ticker,
        benchmark_value=(
            str(snap.benchmark_value)
            if snap.benchmark_value is not None else None
        ),
        portfolio_vs_benchmark=(
            str(snap.portfolio_vs_benchmark)
            if snap.portfolio_vs_benchmark is not None else None
        ),
    )


def _query_history_snaps(
    session,
    start_date: date | None,
    end_date: date | None,
) -> list[PortfolioSnapshot]:
    """
    Build, filter, and execute the performance history query.

    Raises HTTPException 404 when no rows match.
    Caller is responsible for the 503 portfolio check before calling this.
    """
    stmt = select(PortfolioSnapshot).order_by(PortfolioSnapshot.market_date.asc())
    if start_date is not None:
        stmt = stmt.where(PortfolioSnapshot.market_date >= start_date)
    if end_date is not None:
        stmt = stmt.where(PortfolioSnapshot.market_date <= end_date)
    snaps = session.execute(stmt).scalars().all()
    if not snaps:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No portfolio snapshots recorded yet.",
        )
    return list(snaps)


def _run_fill_workflow(
    *,
    idempotency_key: str,
    market_date: date,
    now: datetime,
) -> dict[str, int]:
    """
    Wrap run_fill_cycle with JobRun idempotency and the portfolio advisory lock.

    Idempotency contract:
        COMPLETED → return cached result_summary if it contains all four fill
                    keys; raise RuntimeError if missing or malformed.
        RUNNING   → raise RuntimeError.
        FAILED    → raise RuntimeError; use new idempotency_key or delete row.
    """
    with get_dedicated_session() as session:
        job_run: JobRun | None = None
        try:
            existing = session.execute(
                select(JobRun).where(JobRun.idempotency_key == idempotency_key)
            ).scalar_one_or_none()

            if existing is not None:
                if existing.status == JobRunStatus.COMPLETED:
                    summary = existing.result_summary or {}
                    if not _FILL_RESULT_KEYS.issubset(summary.keys()):
                        raise RuntimeError(
                            f"Fill run {idempotency_key!r} is COMPLETED but its "
                            f"result_summary is missing required keys "
                            f"{sorted(_FILL_RESULT_KEYS - summary.keys())}. "
                            "Inspect the job_runs row manually."
                        )
                    return {k: summary[k] for k in _FILL_RESULT_KEYS}
                if existing.status == JobRunStatus.RUNNING:
                    raise RuntimeError(
                        f"Fill run {idempotency_key!r} is currently RUNNING."
                    )
                if existing.status == JobRunStatus.FAILED:
                    raise RuntimeError(
                        f"Fill run {idempotency_key!r} previously FAILED "
                        f"(id={existing.id}). Use a new idempotency_key or "
                        "delete the failed row before retrying."
                    )

            job_run = JobRun(
                idempotency_key=idempotency_key,
                workflow_type=WorkflowType.POST_MARKET,
                market_date=market_date,
                status=JobRunStatus.RUNNING,
                started_at=now,
            )
            session.add(job_run)
            session.commit()

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
                counts = run_fill_cycle(
                    session,
                    portfolio=portfolio,
                    job_run_id=job_run.id,
                    now=now,
                    market_date=market_date,
                )

                job_run.status         = JobRunStatus.COMPLETED
                job_run.completed_at   = now
                job_run.result_summary = dict(counts)
                session.commit()

                return dict(counts)

            finally:
                try:
                    session.execute(
                        text("SELECT pg_advisory_unlock(:key)"),
                        {"key": PORTFOLIO_ADVISORY_LOCK_KEY},
                    )
                    session.commit()
                except Exception:
                    session.rollback()

        except Exception as exc:
            if job_run is not None:
                try:
                    job_run.status       = JobRunStatus.FAILED
                    job_run.error_detail = str(exc)[:2000]
                    job_run.completed_at = now
                    session.commit()
                except Exception:
                    session.rollback()
            raise


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get(
    "/v1/health",
    response_model=HealthOut,
    status_code=status.HTTP_200_OK,
)
def health() -> HealthOut:
    """
    Lightweight health check endpoint.

    No authentication required. Returns immediately with status and version.
    Used by load balancers and deployment systems to verify the service is alive.
    """
    return HealthOut(
        status="ok",
        service=_SERVICE_NAME,
        version=_SERVICE_VERSION,
    )


@app.get(
    "/v1/ready",
    response_model=ReadyOut,
    status_code=status.HTTP_200_OK,
)
def ready() -> ReadyOut:
    """
    Readiness probe endpoint.

    No authentication required. Performs a lightweight database connectivity
    check (SELECT 1) to verify the service is ready to serve traffic.

    Returns 200 when the database is reachable, 503 when it is not.
    Used by Kubernetes readiness probes and load balancers.
    """
    try:
        with get_session() as session:
            session.execute(text("SELECT 1"))
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unreachable.",
        )
    return ReadyOut(
        status="ok",
        service=_SERVICE_NAME,
        version=_SERVICE_VERSION,
        database="ok",
    )


@app.post(
    "/v1/signals",
    response_model=DecisionResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def ingest_signals(body: DecisionRequest) -> DecisionResponse:
    """
    Ingest a signal batch and run the decision workflow.

    market_date is derived server-side from the current US-Eastern clock.
    Rejected with 422 when called on a weekend (no valid trading day).
    """
    now, market_date = _now_and_date()
    if not is_weekday(now):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Signal ingestion is only permitted on weekdays. "
                f"Current US/Eastern date {market_date} is a weekend."
            ),
        )
    try:
        result = run_decision_workflow(
            idempotency_key=body.idempotency_key,
            workflow_type=body.workflow_type,
            market_date=market_date,
            signals=[s.model_dump() for s in body.signals],
            now=now,
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        )
    return DecisionResponse(**result)


@app.post(
    "/v1/fill",
    response_model=FillResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def trigger_fill(body: FillRequest) -> FillResponse:
    """
    Run the fill cycle.

    market_date defaults to today's US-Eastern date if not supplied.
    """
    now, today = _now_and_date()
    market_date = body.market_date or today
    try:
        result = _run_fill_workflow(
            idempotency_key=body.idempotency_key,
            market_date=market_date,
            now=now,
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        )
    return FillResponse(**result)


@app.post(
    "/v1/snapshot",
    response_model=SnapshotWorkflowResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def trigger_snapshot(body: SnapshotRequest) -> SnapshotWorkflowResponse:
    """
    Run the post-market portfolio snapshot workflow.

    market_date defaults to today's US-Eastern date if not supplied.
    Returns the result summary on success. Raises 409 on idempotency
    conflicts (RUNNING or FAILED job run for the key). Raises 422 when
    an open position has no available price snapshot — ingest prices
    first, then retry with a new idempotency_key.
    """
    now, today = _now_and_date()
    market_date = body.market_date or today
    try:
        result = run_snapshot_workflow(
            idempotency_key=body.idempotency_key,
            market_date=market_date,
            now=now,
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        )
    return SnapshotWorkflowResponse(**result)


@app.post(
    "/v1/prices",
    response_model=PricesResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def ingest_prices(body: PricesRequest) -> PricesResponse:
    """
    Bulk-insert price snapshots.

    job_run_id is null for all rows (manual ingestion outside a workflow run).
    snapshot_ts defaults to the server UTC clock. market_date defaults to the
    US-Eastern date of snapshot_ts.
    """
    now, _ = _now_and_date()
    rows = []
    for snap in body.snapshots:
        ts = snap.snapshot_ts or now
        md = snap.market_date or ts.astimezone(_EASTERN).date()
        rows.append(PriceSnapshot(
            ticker=snap.ticker,
            price=snap.price,
            session_type=snap.session_type,
            price_type=snap.price_type,
            exchange=snap.exchange,
            data_source=snap.data_source,
            snapshot_ts=ts,
            market_date=md,
            job_run_id=None,
        ))
    with get_session() as session:
        session.add_all(rows)
    return PricesResponse(inserted=len(rows))


@app.post(
    "/v1/benchmark-prices",
    response_model=BenchmarkPricesResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def ingest_benchmark_prices(body: BenchmarkPricesRequest) -> BenchmarkPricesResponse:
    """
    Bulk-insert benchmark price observations.

    job_run_id is null for all rows (manual ingestion outside a workflow run).
    snapshot_ts defaults to the server UTC clock. market_date defaults to the
    US-Eastern date of snapshot_ts.
    """
    now, _ = _now_and_date()
    rows = []
    for bp in body.prices:
        ts = bp.snapshot_ts or now
        md = bp.market_date or ts.astimezone(_EASTERN).date()
        rows.append(BenchmarkPrice(
            ticker=bp.ticker,
            price=bp.price,
            session_type=bp.session_type,
            snapshot_ts=ts,
            market_date=md,
            job_run_id=None,
        ))
    with get_session() as session:
        session.add_all(rows)
    return BenchmarkPricesResponse(inserted=len(rows))


@app.get(
    "/v1/positions",
    response_model=list[PositionOut],
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def list_positions() -> list[PositionOut]:
    """List all currently open positions."""
    with get_session() as session:
        positions = session.execute(
            select(Position).order_by(Position.opened_at)
        ).scalars().all()
        return [
            PositionOut(
                id=str(pos.id),
                ticker=pos.ticker,
                qty=str(pos.qty),
                avg_cost=str(pos.avg_cost),
                cost_basis=str(pos.cost_basis),
                opened_at=pos.opened_at,
                last_updated=pos.last_updated,
            )
            for pos in positions
        ]


@app.get(
    "/v1/orders",
    response_model=list[OrderOut],
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def list_orders(
    order_status: str | None = Query(default=None, alias="status"),
    market_date: date | None = Query(default=None),
) -> list[OrderOut]:
    """
    List orders, most recent first.

    Optional filters:
        status      — e.g. PENDING, FILLED, EXPIRED, FAILED
        market_date — US Eastern trading date
    """
    with get_session() as session:
        stmt = select(Order).order_by(Order.requested_at.desc())
        if order_status is not None:
            stmt = stmt.where(Order.status == order_status)
        if market_date is not None:
            stmt = stmt.where(Order.market_date == market_date)
        orders = session.execute(stmt).scalars().all()
        return [
            OrderOut(
                id=str(o.id),
                ticker=o.ticker,
                side=o.side,
                status=o.status,
                market_date=o.market_date,
                requested_qty=str(o.requested_qty),
                filled_qty=str(o.filled_qty) if o.filled_qty is not None else None,
                requested_at=o.requested_at,
                filled_at=o.filled_at,
                fill_price=str(o.fill_price) if o.fill_price is not None else None,
                commission=str(o.commission) if o.commission is not None else None,
                notes=o.notes,
            )
            for o in orders
        ]


@app.get(
    "/v1/snapshots",
    response_model=list[SnapshotOut],
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def list_snapshots() -> list[SnapshotOut]:
    """List all portfolio snapshots, most recent first."""
    with get_session() as session:
        snaps = session.execute(
            select(PortfolioSnapshot).order_by(PortfolioSnapshot.market_date.desc())
        ).scalars().all()
        return [_to_snapshot_out(s) for s in snaps]


@app.get(
    "/v1/snapshots/{market_date}",
    response_model=SnapshotOut,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def get_snapshot(market_date: date) -> SnapshotOut:
    """
    Return the portfolio snapshot for a specific market date.

    Returns 404 if no snapshot has been recorded for that date.
    """
    with get_session() as session:
        snap = session.execute(
            select(PortfolioSnapshot).where(
                PortfolioSnapshot.market_date == market_date
            )
        ).scalar_one_or_none()
        if snap is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No snapshot found for market_date={market_date}.",
            )
        return _to_snapshot_out(snap)


@app.get(
    "/v1/portfolio",
    response_model=PortfolioOut,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def get_portfolio_state() -> PortfolioOut:
    """Return current portfolio state from the reconciler cache."""
    with get_session() as session:
        portfolio = session.execute(select(Portfolio)).scalar_one_or_none()
        if portfolio is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Portfolio not seeded. Run scripts/seed.py first.",
            )
        return PortfolioOut(
            id=portfolio.id,
            inception_date=portfolio.inception_date,
            initial_capital=str(portfolio.initial_capital),
            cached_cash=str(portfolio.cached_cash),
            cached_total_value=str(portfolio.cached_total_value),
            cached_as_of_ts=portfolio.cached_as_of_ts,
            strategy_enabled=portfolio.strategy_enabled,
            trading_enabled=portfolio.trading_enabled,
            allow_new_positions=portfolio.allow_new_positions,
        )


@app.get(
    "/v1/performance",
    response_model=PerformanceOut,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def get_performance() -> PerformanceOut:
    """
    Return inception-to-date portfolio performance summary.

    Derived entirely from portfolio_snapshots and the portfolio row.
    Returns 404 when no snapshots have been recorded yet.
    Returns 503 when the portfolio row is missing.
    """
    with get_session() as session:
        portfolio = session.execute(select(Portfolio)).scalar_one_or_none()
        if portfolio is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Portfolio not seeded. Run scripts/seed.py first.",
            )

        # Oldest and newest snapshot in one pass using aggregation.
        row = session.execute(
            select(
                func.min(PortfolioSnapshot.market_date),
                func.max(PortfolioSnapshot.market_date),
            )
        ).one()
        first_date, latest_date = row

        if first_date is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No portfolio snapshots recorded yet.",
            )

        # Fetch the latest snapshot row for value fields.
        latest = session.execute(
            select(PortfolioSnapshot)
            .where(PortfolioSnapshot.market_date == latest_date)
        ).scalar_one()

        initial_capital = Decimal(str(portfolio.initial_capital))
        latest_total    = Decimal(str(latest.total_value))
        absolute_return = (latest_total - initial_capital).quantize(Decimal("0.01"))

        return_pct: Decimal | None = None
        if initial_capital != Decimal("0"):
            return_pct = (
                (latest_total - initial_capital) / initial_capital * Decimal("100")
            ).quantize(Decimal("0.0001"))

        benchmark_return_pct: Decimal | None = None
        excess_return_pct:    Decimal | None = None
        if (
            latest.benchmark_value is not None
            and initial_capital != Decimal("0")
        ):
            bv = Decimal(str(latest.benchmark_value))
            benchmark_return_pct = (
                (bv - initial_capital) / initial_capital * Decimal("100")
            ).quantize(Decimal("0.0001"))
            if return_pct is not None:
                excess_return_pct = (
                    return_pct - benchmark_return_pct
                ).quantize(Decimal("0.0001"))

        return PerformanceOut(
            first_snapshot_date=first_date,
            latest_snapshot_date=latest_date,
            initial_capital=str(initial_capital),
            latest_total_value=str(latest_total),
            absolute_return=str(absolute_return),
            return_pct=str(return_pct) if return_pct is not None else None,
            benchmark_ticker=latest.benchmark_ticker,
            benchmark_return_pct=(
                str(benchmark_return_pct) if benchmark_return_pct is not None else None
            ),
            excess_return_pct=(
                str(excess_return_pct) if excess_return_pct is not None else None
            ),
        )


@app.get(
    "/v1/performance/history",
    response_model=list[PerformanceHistoryItem],
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def get_performance_history(
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
) -> list[PerformanceHistoryItem]:
    """
    Return time-series performance history for charting and trend analysis.

    Returns portfolio snapshots ordered chronologically (ascending by
    market_date). Each item includes portfolio values, PnL, and benchmark
    comparison data. Benchmark fields degrade gracefully to null when
    benchmark data is unavailable.

    Optional filters:
        start_date — include only rows with market_date >= start_date
        end_date   — include only rows with market_date <= end_date

    Returns 404 when no snapshots match (no data recorded, or the date
    window contains no rows).
    Returns 503 when the portfolio row is missing.
    """
    with get_session() as session:
        portfolio = session.execute(select(Portfolio)).scalar_one_or_none()
        if portfolio is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Portfolio not seeded. Run scripts/seed.py first.",
            )
        snaps = _query_history_snaps(session, start_date, end_date)
        return [_to_performance_history_item(s) for s in snaps]


@app.get(
    "/v1/performance/history.csv",
    response_class=Response,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def get_performance_history_csv(
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
) -> Response:
    """
    Return performance history as a downloadable CSV file.

    Same data, filtering semantics, and error codes as
    GET /v1/performance/history — see that endpoint for full details.

    Columns (in order):
        market_date, total_value, cash, positions_value, unrealized_pnl,
        realized_pnl_cumulative, benchmark_ticker, benchmark_value,
        portfolio_vs_benchmark

    Optional fields (benchmark_ticker, benchmark_value, portfolio_vs_benchmark)
    are written as empty strings when null.
    """
    with get_session() as session:
        portfolio = session.execute(select(Portfolio)).scalar_one_or_none()
        if portfolio is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Portfolio not seeded. Run scripts/seed.py first.",
            )
        snaps = _query_history_snaps(session, start_date, end_date)

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(_HISTORY_CSV_COLUMNS)
    for snap in snaps:
        item = _to_performance_history_item(snap)
        writer.writerow([
            str(item.market_date),
            item.total_value,
            item.cash,
            item.positions_value,
            item.unrealized_pnl,
            item.realized_pnl_cumulative,
            item.benchmark_ticker if item.benchmark_ticker is not None else "",
            item.benchmark_value if item.benchmark_value is not None else "",
            item.portfolio_vs_benchmark if item.portfolio_vs_benchmark is not None else "",
        ])

    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=performance_history.csv"},
    )


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

_UI_DIR = pathlib.Path(__file__).parent / "ui"
app.mount("/ui", StaticFiles(directory=str(_UI_DIR), html=True), name="ui")


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse("/ui/")
