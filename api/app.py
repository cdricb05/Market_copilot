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
from sqlalchemy import distinct, func, select, text

from paper_trader.config import get_settings
from paper_trader.constants import (
    PORTFOLIO_ADVISORY_LOCK_KEY,
    DecisionType,
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
    TradeDecision,
)
from paper_trader.db.session import get_dedicated_session, get_session
from paper_trader.engine.market_data import fetch_latest_prices
from paper_trader.engine.market_hours import is_weekday
from paper_trader.engine.portfolio import get_portfolio
from paper_trader.engine.prediction_client import (
    fetch_predictions_for_tickers,
    normalize_prediction_response,
    normalize_prediction_response_with_error,
)
from paper_trader.engine.prediction_strategy import generate_prediction_signals
from paper_trader.engine.reconciler import run_fill_cycle
from paper_trader.engine.strategy import generate_signals
from paper_trader.workflows.decision import run_decision_workflow
from paper_trader.workflows.snapshot import MissingPricesError, run_snapshot_workflow

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


class FetchPricesRequest(BaseModel):
    tickers: list[str]
    price_type: str = PriceType.LAST
    session_type: str = SessionType.REGULAR


class FetchPriceDetail(BaseModel):
    ticker: str
    price: str
    market_date: date
    price_type: str
    session_type: str
    data_source: str


class FetchPriceFailure(BaseModel):
    ticker: str
    reason: str


class FetchPricesResponse(BaseModel):
    inserted: int
    prices: list[FetchPriceDetail]
    failures: list[FetchPriceFailure]


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


class StrategyRunRequest(BaseModel):
    idempotency_key: str
    market_date: date | None = Field(
        default=None,
        description="US Eastern market date. Defaults to today's Eastern date.",
    )
    short_window: int = Field(
        default=3,
        ge=1,
        description="Number of periods for short-term SMA. Must be < long_window.",
    )
    long_window: int = Field(
        default=5,
        ge=1,
        description="Number of periods for long-term SMA. Must be > short_window.",
    )
    tickers: list[str] | None = Field(
        default=None,
        description="Optional list of tickers to process. If None, process all available.",
    )


class StrategyRunResponse(BaseModel):
    signals_generated: int
    signals_submitted: int
    skipped_tickers: dict[str, str]
    decisions_made: int
    orders_created: int
    errors: int
    generated_signals: list[dict] | None = None
    decisions_breakdown: dict[str, int] = Field(default_factory=lambda: {"approved": 0, "rejected": 0, "hold": 0})
    rejection_reasons: dict[str, int] = Field(default_factory=dict)


class PredictionRunRequest(BaseModel):
    idempotency_key: str
    predictions: list[dict[str, Any]]


class FetchAndRunPredictionRequest(BaseModel):
    idempotency_key: str
    tickers: list[str]


class FetchFailure(BaseModel):
    ticker: str
    reason: str


class NormalizedPrediction(BaseModel):
    ticker: str
    recommendation: str
    confidence: str
    current_price: str
    forecast_price_5d: str
    expected_return_pct: str
    market_context: str
    model_consensus: dict[str, str]
    reason: str


class FetchAndRunPredictionResponse(BaseModel):
    fetched_count: int
    failed_count: int
    fetch_failures: list[FetchFailure]
    normalized_predictions: list[NormalizedPrediction]
    signals_generated: int
    signals_submitted: int
    skipped_tickers: dict[str, str]
    decisions_made: int
    orders_created: int
    errors: int
    decisions_breakdown: dict[str, int] = Field(default_factory=lambda: {"approved": 0, "rejected": 0, "hold": 0})
    rejection_reasons: dict[str, int] = Field(default_factory=dict)


class MarketScanRequest(BaseModel):
    universe: str = Field(
        default="SP500",
        description="Universe name ('SP500'). Ignored if tickers provided.",
    )
    tickers: list[str] | None = Field(
        default=None,
        description="Explicit list of tickers to scan. Takes precedence over universe.",
    )
    benchmark_ticker: str = Field(
        default="SPY",
        description="Benchmark ticker for relative strength calculation.",
    )
    lookback_days: int = Field(
        default=20,
        ge=1,
        description="Number of days of history to consider.",
    )
    top_n: int = Field(
        default=25,
        ge=1,
        le=100,
        description="Return top N candidates (capped at 100).",
    )
    min_price_points: int = Field(
        default=5,
        ge=1,
        description="Minimum number of price points required per ticker.",
    )


class CandidateOut(BaseModel):
    rank: int
    ticker: str
    score: str
    latest_price: str
    latest_market_date: str
    price_count: int
    momentum_5d_pct: str | None
    momentum_20d_pct: str | None
    volatility_20d_pct: str | None
    relative_strength_vs_spy_20d: str | None
    reason_codes: list[str]


class SkippedTickerOut(BaseModel):
    ticker: str
    reason: str
    price_count: int


class MarketScanResponse(BaseModel):
    universe: str
    scan_date: str | None
    benchmark_ticker: str
    total_universe_count: int
    evaluated_count: int
    skipped_count: int
    top_n: int
    candidates: list[CandidateOut]
    skipped_tickers: list[SkippedTickerOut]


class TickerReadinessOut(BaseModel):
    ticker: str
    price_count: int
    latest_market_date: date | None
    has_sufficient_history: bool
    missing_count: int


class StrategyReadinessOut(BaseModel):
    market_date: date
    long_window: int
    overall_status: str
    tickers_status: list[TickerReadinessOut]


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
    conflicts (RUNNING or FAILED job run for the key). Raises 400 when
    an open position has no available price snapshot — ingest prices
    first, then retry.
    """
    now, today = _now_and_date()
    market_date = body.market_date or today
    try:
        result = run_snapshot_workflow(
            idempotency_key=body.idempotency_key,
            market_date=market_date,
            now=now,
        )
    except MissingPricesError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
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


@app.post(
    "/v1/prices/fetch",
    response_model=FetchPricesResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def fetch_prices(body: FetchPricesRequest) -> FetchPricesResponse:
    """
    Fetch latest prices from market data source and insert into price_snapshots.

    Tickers are normalized to uppercase. Empty tickers list returns inserted=0.
    Individual ticker failures do not fail the whole request; they are reported
    in the failures array.

    Inserted prices have:
        data_source = "yahoo_finance"
        price_type = request.price_type (default LAST)
        session_type = request.session_type (default REGULAR)
        snapshot_ts = server UTC now
        market_date = US-Eastern date of snapshot_ts
        job_run_id = null (outside workflow context)

    Request validation:
        - tickers: list of symbols (required, can be empty)
        - price_type: defaults to LAST
        - session_type: defaults to REGULAR

    Invalid prices (zero, negative) or network failures are reported as failures.
    """
    now, _ = _now_and_date()
    market_date = now.astimezone(_EASTERN).date()

    if not body.tickers:
        return FetchPricesResponse(inserted=0, prices=[], failures=[])

    successful, failures = fetch_latest_prices(body.tickers)

    rows = []
    prices_detail = []
    failures_dict = {f["ticker"]: f for f in failures}

    for price_dict in successful:
        # Validate required keys
        if "ticker" not in price_dict or "price" not in price_dict:
            failures_dict.setdefault(price_dict.get("ticker", "unknown"), {
                "ticker": price_dict.get("ticker", "unknown"),
                "reason": "Missing ticker or price in result"
            })
            continue

        ticker = price_dict["ticker"]

        # Validate and convert price
        try:
            price = Decimal(price_dict["price"])
            if price <= 0:
                failures_dict[ticker] = {"ticker": ticker, "reason": "Price is zero or negative"}
                continue
        except (ValueError, TypeError):
            failures_dict[ticker] = {"ticker": ticker, "reason": "Invalid price format"}
            continue

        snapshot = PriceSnapshot(
            ticker=ticker,
            price=price,
            session_type=body.session_type,
            price_type=body.price_type,
            exchange=None,
            data_source="yahoo_finance",
            snapshot_ts=now,
            market_date=market_date,
            job_run_id=None,
        )
        rows.append(snapshot)

        prices_detail.append(FetchPriceDetail(
            ticker=ticker,
            price=str(price),
            market_date=market_date,
            price_type=body.price_type,
            session_type=body.session_type,
            data_source="yahoo_finance",
        ))

    with get_session() as session:
        session.add_all(rows)

    failures_detail = [FetchPriceFailure(**f) for f in failures_dict.values()]

    return FetchPricesResponse(
        inserted=len(rows),
        prices=prices_detail,
        failures=failures_detail,
    )


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


def _override_signals_source_run(
    signals: list[dict],
    idempotency_key: str,
) -> list[dict]:
    """
    Prepare strategy signals for submission by overriding source_run to idempotency_key.

    For each signal, preserves the original source_run (usually "strategy_v1") in
    raw_payload.strategy_name for traceability, then sets source_run to the
    idempotency_key. This ensures each strategy run has a unique source_run,
    preventing unique constraint collisions on (source_run, ticker, direction).

    Args:
        signals: List of signal dicts from generate_signals().
        idempotency_key: Unique request identifier for this strategy run.

    Returns:
        List of modified signals ready for run_decision_workflow().
    """
    submitted_signals = []
    for signal in signals:
        submitted_signal = signal.copy()
        if "raw_payload" not in submitted_signal:
            submitted_signal["raw_payload"] = {}
        # Preserve original source label for traceability
        submitted_signal["raw_payload"]["strategy_name"] = submitted_signal.get(
            "source_run", "strategy_v1"
        )
        # Override source_run to make each strategy run unique
        submitted_signal["source_run"] = idempotency_key
        submitted_signals.append(submitted_signal)
    return submitted_signals


@app.post(
    "/v1/strategy/run",
    response_model=StrategyRunResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def run_strategy(body: StrategyRunRequest) -> StrategyRunResponse:
    """
    Generate strategy signals from price snapshots and submit to decision workflow.

    Generates trading signals using configurable moving average rules, then submits
    non-skipped signals through the existing decision workflow. Returns a summary of
    generated signals, submissions, skipped tickers with reasons, and resulting
    decisions/orders.

    Window validation:
        - short_window and long_window must be > 0
        - short_window must be < long_window
        - Returns 400 if validation fails

    Behavior:
        - Returns 200 even if all tickers are skipped (insufficient history)
        - Tickers with fewer than long_window prices are skipped with a reason
        - No price snapshots → zero signals, empty skipped dict
        - Generated signals flow through the existing decision workflow unchanged
        - Orders created only from decision engine, not directly by strategy

    market_date defaults to current US-Eastern date if not supplied.
    """
    now, market_date = _now_and_date()
    if body.market_date is not None:
        market_date = body.market_date

    # Validate window parameters
    if body.short_window >= body.long_window:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"short_window ({body.short_window}) must be < "
                f"long_window ({body.long_window})"
            ),
        )
    if body.short_window <= 0 or body.long_window <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="short_window and long_window must be > 0",
        )

    # Generate signals from price snapshots
    try:
        with get_dedicated_session() as session:
            signals, skipped_reasons = generate_signals(
                session,
                market_date=market_date,
                now=now,
                short_window=body.short_window,
                long_window=body.long_window,
                tickers=body.tickers,
            )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate strategy signals: {str(exc)}",
        )

    signals_generated = len(signals)

    # If no signals generated, return early
    if not signals:
        return StrategyRunResponse(
            signals_generated=signals_generated,
            signals_submitted=0,
            skipped_tickers=skipped_reasons,
            decisions_made=0,
            orders_created=0,
            errors=0,
            generated_signals=None,
        )

    # Submit generated signals through the decision workflow
    if not is_weekday(now):
        # Return on weekend with skipped message
        return StrategyRunResponse(
            signals_generated=signals_generated,
            signals_submitted=0,
            skipped_tickers={
                **skipped_reasons,
                "_all": f"Weekend trading disabled ({market_date})",
            },
            decisions_made=0,
            orders_created=0,
            errors=0,
            generated_signals=signals,
        )

    submitted_signals = _override_signals_source_run(signals, body.idempotency_key)

    try:
        result = run_decision_workflow(
            idempotency_key=body.idempotency_key,
            workflow_type=WorkflowType.PRE_MARKET,
            market_date=market_date,
            signals=submitted_signals,
            now=now,
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        )

    # Query TradeDecision rows to build decisions_breakdown and rejection_reasons
    decisions_breakdown = {"approved": 0, "rejected": 0, "hold": 0}
    rejection_reasons: dict[str, int] = {}
    with get_dedicated_session() as session:
        job_run = session.execute(
            select(JobRun).where(JobRun.idempotency_key == body.idempotency_key)
        ).scalar_one_or_none()

        if job_run is not None:
            decisions = session.execute(
                select(TradeDecision).where(TradeDecision.job_run_id == job_run.id)
            ).scalars().all()

            for decision in decisions:
                if decision.decision == DecisionType.BUY or decision.decision == DecisionType.SELL:
                    decisions_breakdown["approved"] += 1
                elif decision.decision == DecisionType.REJECTED:
                    decisions_breakdown["rejected"] += 1
                    if decision.reason_code:
                        rejection_reasons[decision.reason_code] = rejection_reasons.get(decision.reason_code, 0) + 1
                elif decision.decision == DecisionType.HOLD:
                    decisions_breakdown["hold"] += 1

    return StrategyRunResponse(
        signals_generated=signals_generated,
        signals_submitted=result.get("signals_ingested", 0),
        skipped_tickers=skipped_reasons,
        decisions_made=result.get("decisions_made", 0),
        orders_created=result.get("orders_created", 0),
        errors=result.get("errors", 0),
        generated_signals=signals,
        decisions_breakdown=decisions_breakdown,
        rejection_reasons=rejection_reasons,
    )


@app.post(
    "/v1/strategy/prediction/run",
    response_model=StrategyRunResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def run_prediction_strategy(body: PredictionRunRequest) -> StrategyRunResponse:
    """
    Convert predictions to signals and submit to decision workflow.

    Accepts ML predictions with confidence scores and market context, converts them
    to standard signals, and routes through the existing decision/risk pipeline.
    Does not bypass risk controls or directly create orders.

    Prediction input contract:
        ticker: str (required)
        current_price: str (required)
        forecast_price_5d: str (required)
        expected_return_pct: str (required)
        confidence: str|float (required, 0-1)
        recommendation: str (required: BUY|SELL|HOLD)
        reason: str (optional)
        model_consensus: dict (optional)
        market_context: str (optional)

    Behavior:
        - Empty predictions list returns zero counts.
        - Invalid predictions are skipped with reasons in skipped_tickers.
        - Valid predictions flow through decision/risk engine.
        - Weekday enforcement: returns zero submissions on weekend.
        - Returns 200 even if all predictions are invalid.
    """
    now, market_date = _now_and_date()

    # Convert predictions to signals
    try:
        signals, skipped_reasons = generate_prediction_signals(
            predictions=body.predictions,
            source_run=body.idempotency_key,
            now=now,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to convert predictions to signals: {str(exc)}",
        )

    signals_generated = len(signals)

    # If no valid signals, return early
    if not signals:
        return StrategyRunResponse(
            signals_generated=signals_generated,
            signals_submitted=0,
            skipped_tickers=skipped_reasons,
            decisions_made=0,
            orders_created=0,
            errors=0,
            generated_signals=None,
        )

    # Check weekday before submission
    if not is_weekday(now):
        return StrategyRunResponse(
            signals_generated=signals_generated,
            signals_submitted=0,
            skipped_tickers={
                **skipped_reasons,
                "_all": f"Weekend trading disabled ({market_date})",
            },
            decisions_made=0,
            orders_created=0,
            errors=0,
            generated_signals=signals,
        )

    # Submit signals through the decision workflow
    try:
        result = run_decision_workflow(
            idempotency_key=body.idempotency_key,
            workflow_type=WorkflowType.PRE_MARKET,
            market_date=market_date,
            signals=signals,
            now=now,
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        )

    # Query TradeDecision rows to build decisions_breakdown and rejection_reasons
    decisions_breakdown = {"approved": 0, "rejected": 0, "hold": 0}
    rejection_reasons: dict[str, int] = {}
    with get_dedicated_session() as session:
        job_run = session.execute(
            select(JobRun).where(JobRun.idempotency_key == body.idempotency_key)
        ).scalar_one_or_none()

        if job_run is not None:
            decisions = session.execute(
                select(TradeDecision).where(TradeDecision.job_run_id == job_run.id)
            ).scalars().all()

            for decision in decisions:
                if decision.decision == DecisionType.BUY or decision.decision == DecisionType.SELL:
                    decisions_breakdown["approved"] += 1
                elif decision.decision == DecisionType.REJECTED:
                    decisions_breakdown["rejected"] += 1
                    if decision.reason_code:
                        rejection_reasons[decision.reason_code] = rejection_reasons.get(decision.reason_code, 0) + 1
                elif decision.decision == DecisionType.HOLD:
                    decisions_breakdown["hold"] += 1

    return StrategyRunResponse(
        signals_generated=signals_generated,
        signals_submitted=result.get("signals_ingested", 0),
        skipped_tickers=skipped_reasons,
        decisions_made=result.get("decisions_made", 0),
        orders_created=result.get("orders_created", 0),
        errors=result.get("errors", 0),
        generated_signals=signals,
        decisions_breakdown=decisions_breakdown,
        rejection_reasons=rejection_reasons,
    )


@app.post(
    "/v1/strategy/prediction/fetch-and-run",
    response_model=FetchAndRunPredictionResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def fetch_and_run_prediction_strategy(
    body: FetchAndRunPredictionRequest,
) -> FetchAndRunPredictionResponse:
    """
    Fetch predictions from external API and run prediction strategy.

    Fetches predictions for the requested tickers from the configured stock
    prediction service, normalizes them to the internal prediction contract,
    and submits them through the existing decision/risk pipeline.

    Behavior:
        - Fetches predictions concurrently for all tickers.
        - Per-ticker fetch failures do not block others.
        - Normalizes raw API responses to Paper Trader prediction contract.
        - Invalid predictions after normalization are skipped with reasons.
        - Passes normalized predictions through decision/risk engine.
        - Returns 200 if at least one fetch succeeds (partial failures included).
        - Returns 503 if all fetches fail due to service unavailability.
        - Returns 422 if the request is invalid.
    """
    now, market_date = _now_and_date()
    settings = get_settings()

    # Fetch predictions from external API
    try:
        fetched_responses, fetch_failures = await fetch_predictions_for_tickers(
            tickers=body.tickers,
            api_url=settings.stock_prediction_api_url,
            timeout_seconds=settings.stock_prediction_api_timeout_seconds,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch predictions: {str(exc)}",
        )

    fetched_count = len(fetched_responses)
    failed_count = len(fetch_failures)

    # If all tickers failed to fetch, return 503
    if fetched_count == 0 and failed_count > 0:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Prediction service unavailable for all requested tickers",
        )

    # Normalize fetched responses to prediction contract
    normalized_predictions = []
    normalization_failures: dict[str, str] = {}

    for raw_response in fetched_responses:
        normalized, error_reason = normalize_prediction_response_with_error(raw_response)
        if normalized:
            normalized_predictions.append(NormalizedPrediction(**normalized))
        else:
            ticker = raw_response.get("ticker", "unknown")
            normalization_failures[ticker] = error_reason or "Failed to normalize API response"

    # If we have normalized predictions, submit them through the decision workflow
    if normalized_predictions:
        try:
            signals, skipped_reasons = generate_prediction_signals(
                predictions=normalized_predictions,
                source_run=body.idempotency_key,
                now=now,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to convert predictions to signals: {str(exc)}",
            )

        signals_generated = len(signals)

        # If no valid signals, return early
        if not signals:
            return FetchAndRunPredictionResponse(
                fetched_count=fetched_count,
                failed_count=failed_count,
                fetch_failures=[FetchFailure(**f) for f in fetch_failures],
                normalized_predictions=normalized_predictions,
                signals_generated=signals_generated,
                signals_submitted=0,
                skipped_tickers={**skipped_reasons, **normalization_failures},
                decisions_made=0,
                orders_created=0,
                errors=len(fetch_failures),
            )

        # Check weekday before submission
        if not is_weekday(now):
            return FetchAndRunPredictionResponse(
                fetched_count=fetched_count,
                failed_count=failed_count,
                fetch_failures=[FetchFailure(**f) for f in fetch_failures],
                normalized_predictions=normalized_predictions,
                signals_generated=signals_generated,
                signals_submitted=0,
                skipped_tickers={
                    **skipped_reasons,
                    **normalization_failures,
                    "_all": f"Weekend trading disabled ({market_date})",
                },
                decisions_made=0,
                orders_created=0,
                errors=len(fetch_failures),
            )

        # Submit signals through the decision workflow
        try:
            result = run_decision_workflow(
                idempotency_key=body.idempotency_key,
                workflow_type=WorkflowType.PRE_MARKET,
                market_date=market_date,
                signals=signals,
                now=now,
            )
        except RuntimeError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=str(exc),
            )

        # Query TradeDecision rows to build decisions_breakdown and rejection_reasons
        decisions_breakdown = {"approved": 0, "rejected": 0, "hold": 0}
        rejection_reasons: dict[str, int] = {}
        with get_dedicated_session() as session:
            job_run = session.execute(
                select(JobRun).where(JobRun.idempotency_key == body.idempotency_key)
            ).scalar_one_or_none()

            if job_run is not None:
                decisions = session.execute(
                    select(TradeDecision).where(TradeDecision.job_run_id == job_run.id)
                ).scalars().all()

                for decision in decisions:
                    if decision.decision == DecisionType.BUY or decision.decision == DecisionType.SELL:
                        decisions_breakdown["approved"] += 1
                    elif decision.decision == DecisionType.REJECTED:
                        decisions_breakdown["rejected"] += 1
                        if decision.reason_code:
                            rejection_reasons[decision.reason_code] = rejection_reasons.get(
                                decision.reason_code, 0
                            ) + 1
                    elif decision.decision == DecisionType.HOLD:
                        decisions_breakdown["hold"] += 1

        return FetchAndRunPredictionResponse(
            fetched_count=fetched_count,
            failed_count=failed_count,
            fetch_failures=[FetchFailure(**f) for f in fetch_failures],
            normalized_predictions=normalized_predictions,
            signals_generated=signals_generated,
            signals_submitted=result.get("signals_ingested", 0),
            skipped_tickers={**skipped_reasons, **normalization_failures},
            decisions_made=result.get("decisions_made", 0),
            orders_created=result.get("orders_created", 0),
            errors=result.get("errors", 0) + len(fetch_failures),
            decisions_breakdown=decisions_breakdown,
            rejection_reasons=rejection_reasons,
        )
    else:
        # No normalized predictions, return early
        return FetchAndRunPredictionResponse(
            fetched_count=fetched_count,
            failed_count=failed_count,
            fetch_failures=[FetchFailure(**f) for f in fetch_failures],
            normalized_predictions=normalized_predictions,
            signals_generated=0,
            signals_submitted=0,
            skipped_tickers={**normalization_failures},
            decisions_made=0,
            orders_created=0,
            errors=failed_count,
        )


@app.get(
    "/v1/strategy/readiness",
    response_model=StrategyReadinessOut,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def check_strategy_readiness(
    long_window: int = Query(..., ge=1),
    market_date: date | None = Query(None),
    tickers: str | None = Query(None),
) -> StrategyReadinessOut:
    """
    Check whether selected tickers have sufficient price history for strategy.

    Evaluates price snapshot counts for each ticker up to market_date (not including
    future prices), using the same date logic as /v1/strategy/run.

    Query params:
        long_window: Number of periods required for strategy (required, >= 1).
        market_date: US-Eastern trading date (optional, defaults to today's Eastern date).
        tickers: Optional comma-separated list or single ticker. If omitted, checks
                 all distinct tickers in price_snapshots up to market_date.

    Response:
        market_date: The effective market_date used for the readiness check.
        long_window: The window size checked.
        overall_status: "Ready" if all requested tickers have >= long_window prices,
                       "Insufficient History" otherwise.
        tickers_status: List of per-ticker readiness with counts and missing_count.
    """
    now, effective_date = _now_and_date()
    if market_date is not None:
        effective_date = market_date

    # Parse tickers from comma-separated string
    ticker_list = None
    if tickers:
        ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]

    with get_dedicated_session() as session:
        # If tickers not specified, fetch all distinct tickers
        if not ticker_list:
            query_tickers = session.execute(
                select(distinct(PriceSnapshot.ticker))
                .where(PriceSnapshot.market_date <= effective_date)
            ).scalars().all()
            ticker_list = sorted(query_tickers) if query_tickers else []

        tickers_status = []
        all_ready = True

        for ticker in ticker_list:
            # Count prices up to effective_date (not future)
            prices_result = session.execute(
                select(PriceSnapshot.price, PriceSnapshot.market_date)
                .where(PriceSnapshot.ticker == ticker)
                .where(PriceSnapshot.market_date <= effective_date)
                .order_by(PriceSnapshot.snapshot_ts.desc())
                .limit(long_window)
            ).all()

            price_count = len(prices_result)
            has_sufficient = price_count >= long_window
            latest_date = prices_result[0][1] if prices_result else None
            missing = max(0, long_window - price_count)

            tickers_status.append(
                TickerReadinessOut(
                    ticker=ticker,
                    price_count=price_count,
                    latest_market_date=latest_date,
                    has_sufficient_history=has_sufficient,
                    missing_count=missing,
                )
            )

            if not has_sufficient:
                all_ready = False

    return StrategyReadinessOut(
        market_date=effective_date,
        long_window=long_window,
        overall_status="Ready" if all_ready else "Insufficient History",
        tickers_status=tickers_status,
    )


@app.post(
    "/v1/market/scan",
    response_model=MarketScanResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def scan_market(body: MarketScanRequest) -> MarketScanResponse:
    """
    Scan market candidates using price snapshot data.

    Returns a ranked list of candidates based on momentum, volatility, and
    relative strength versus a benchmark. Read-only; does not create orders.

    Uses only historical price data from the database. Does not call external
    APIs (GCP, yfinance, etc.).

    Request:
        universe: Universe name ("SP500"). Ignored if tickers provided.
        tickers: Explicit ticker list. Takes precedence over universe.
        benchmark_ticker: Benchmark for relative strength (default "SPY").
        lookback_days: History window in days (default 20).
        top_n: Limit results to top N candidates, capped at 100 (default 25).
        min_price_points: Min price points per ticker (default 5).

    Response:
        Returns 200 OK even if no candidates found (skipped_tickers populated).
        Contains candidates sorted by score descending.
    """
    from paper_trader.engine.market_screener import scan_market as scan_market_fn

    with get_dedicated_session() as session:
        candidates, skipped, scan_date = scan_market_fn(
            session=session,
            tickers=body.tickers,
            universe=body.universe,
            benchmark_ticker=body.benchmark_ticker,
            lookback_days=body.lookback_days,
            top_n=body.top_n,
            min_price_points=body.min_price_points,
        )

    # Determine universe size (for response metadata)
    from paper_trader.engine.universe import get_sp500_universe

    universe_tickers = get_sp500_universe() if body.universe == "SP500" else []
    if body.tickers:
        universe_tickers = body.tickers

    return MarketScanResponse(
        universe=body.universe,
        scan_date=str(scan_date) if scan_date else None,
        benchmark_ticker=body.benchmark_ticker,
        total_universe_count=len(universe_tickers),
        evaluated_count=len(candidates) + len(skipped),
        skipped_count=len(skipped),
        top_n=min(body.top_n, 100),
        candidates=[CandidateOut(**c.to_dict()) for c in candidates],
        skipped_tickers=[SkippedTickerOut(**s.to_dict()) for s in skipped],
    )


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

_UI_DIR = pathlib.Path(__file__).parent / "ui"
app.mount("/ui", StaticFiles(directory=str(_UI_DIR), html=True), name="ui")


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse("/ui/")
