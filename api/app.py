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
    POST /v1/market/scan           — scan market candidates (read-only)
    POST /v1/strategy/prediction/fetch-and-run — fetch predictions and run workflow (creates decisions/orders)
    POST /v1/strategy/market-scan/prediction-candidates — market scan + prediction preview (V1 PREVIEW ONLY)
    POST /v1/review/rotation-preview — preview portfolio rotations when at max positions (read-only)
    POST /v1/review/daily-plan-preview — consolidated daily plan: BUY/SELL/HOLD/ROTATION/BLOCKED (read-only)

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
    CandidateReview,
    JobRun,
    Order,
    Portfolio,
    PortfolioSnapshot,
    Position,
    PriceSnapshot,
    Signal,
    TradeDecision,
)
from paper_trader.db.session import get_dedicated_session, get_session
from paper_trader.engine.market_data import fetch_latest_prices, fetch_historical_prices
from paper_trader.engine.market_hours import is_weekday
from paper_trader.engine.portfolio import get_portfolio
from paper_trader.engine.prediction_client import (
    fetch_predictions_for_tickers,
    normalize_prediction_response,
    normalize_prediction_response_with_error,
)
from paper_trader.engine.prediction_strategy import generate_prediction_signals
from paper_trader.engine.reconciler import run_fill_cycle
from paper_trader.engine.risk import evaluate_signal
from paper_trader.engine.strategy import generate_signals
from paper_trader.workflows.decision import run_decision_workflow, _latest_price
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


class AuthCheckOut(BaseModel):
    authenticated: bool
    service: str


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
    market_date: date | None = None


class FetchAndRunPredictionRequest(BaseModel):
    idempotency_key: str
    tickers: list[str]
    market_date: date | None = None


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


class CandidatePreview(BaseModel):
    """Enriched candidate preview combining scan and prediction context."""
    ticker: str
    scan_rank: int | None
    scan_score: str | None
    latest_price: str | None
    momentum_5d_pct: str | None
    momentum_20d_pct: str | None
    relative_strength_vs_spy_20d: str | None
    scan_reason_codes: list[str]

    prediction_recommendation: str | None
    prediction_confidence: str | None
    forecast_price_5d: str | None
    expected_return_pct: str | None
    market_context: str | None

    preview_decision: str
    preview_score: str
    preview_reasons: list[str]
    status: str


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


class MarketScanPredictionCandidatesRequest(BaseModel):
    """Request to scan market and fetch predictions for top candidates (PREVIEW ONLY in V1)."""
    idempotency_key: str
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
        description="Number of days of history to consider for scanning.",
    )
    top_n: int = Field(
        default=25,
        ge=1,
        le=100,
        description="Return top N candidates from scan (capped at 100).",
    )
    min_price_points: int = Field(
        default=5,
        ge=1,
        description="Minimum number of price points required per ticker.",
    )
    prediction_top_n: int = Field(
        default=5,
        ge=1,
        le=100,
        description="Select top N candidates for prediction fetching.",
    )
    dry_run: bool = Field(
        default=True,
        description="V1: must be true. PREVIEW mode only; no database writes.",
    )
    submit_signals: bool = Field(
        default=False,
        description="V1: must be false. No Signal rows created.",
    )
    run_risk: bool = Field(
        default=False,
        description="V1: must be false. No risk evaluation or decisions.",
    )
    create_orders: bool = Field(
        default=False,
        description="V1: must be false. No orders created.",
    )


class ScanSummaryOut(BaseModel):
    """Market scan summary."""
    universe: str
    scan_date: str | None
    total_universe_count: int
    evaluated_count: int
    skipped_count: int
    candidate_count: int


class PredictionFailureDetail(BaseModel):
    ticker: str
    reason: str


class MarketScanPredictionCandidatesResponse(BaseModel):
    """Response from market scan + prediction candidate preview endpoint."""
    idempotency_key: str
    dry_run: bool
    execution_mode: str
    scan: ScanSummaryOut
    selected_tickers: list[str]
    predictions_fetched: int
    prediction_failures: list[PredictionFailureDetail]
    normalized_predictions: list[NormalizedPrediction]
    candidate_previews: list[CandidatePreview]
    signals_submitted: int
    decisions_made: int
    orders_created: int


# ---------------------------------------------------------------------------
# Candidate Review Queue schemas
# ---------------------------------------------------------------------------

class CandidateReviewCreate(BaseModel):
    """One candidate to save to review queue."""
    ticker: str
    scan_rank: int | None = None
    scan_score: str | None = None
    latest_price: str | None = None
    momentum_5d_pct: str | None = None
    momentum_20d_pct: str | None = None
    relative_strength_vs_spy_20d: str | None = None
    scan_reason_codes: list[str] | None = None
    prediction_recommendation: str | None = None
    prediction_confidence: str | None = None
    forecast_price_5d: str | None = None
    expected_return_pct: str | None = None
    market_context: str | None = None
    preview_decision: str
    preview_score: str
    preview_reasons: list[str] | None = None
    status: str = "OK"


class CandidateReviewSaveRequest(BaseModel):
    """Save one or more candidates to review queue."""
    idempotency_key: str
    candidates: list[CandidateReviewCreate] = Field(
        ...,
        min_length=1,
        description="At least one candidate required.",
    )


class CandidateReviewOut(BaseModel):
    """Candidate review queue row."""
    id: str
    idempotency_key: str
    ticker: str
    scan_rank: str | None
    scan_score: str | None
    latest_price: str | None
    momentum_5d_pct: str | None
    momentum_20d_pct: str | None
    relative_strength_vs_spy_20d: str | None
    scan_reason_codes: list[str] | None
    prediction_recommendation: str | None
    prediction_confidence: str | None
    forecast_price_5d: str | None
    expected_return_pct: str | None
    market_context: str | None
    preview_decision: str
    preview_score: str
    preview_reasons: list[str] | None
    status: str
    review_status: str
    created_at: datetime
    updated_at: datetime


class CandidateReviewSaveResponse(BaseModel):
    """Response from save candidates endpoint."""
    inserted_count: int
    skipped_existing_count: int
    candidates_saved: list[CandidateReviewOut]


class CandidateReviewStatusUpdate(BaseModel):
    """Update review_status for a candidate."""
    review_status: str = Field(
        ...,
        description="NEW | WATCHING | REJECTED | APPROVED_FOR_SIGNAL",
    )


class ReviewSignalPreviewRequest(BaseModel):
    """Request to generate signal previews from approved candidates."""
    idempotency_key: str = Field(
        ...,
        description="Unique key for this preview batch.",
    )
    review_status: str = Field(
        default="APPROVED_FOR_SIGNAL",
        description="Filter by review_status. Defaults to APPROVED_FOR_SIGNAL.",
    )
    candidate_ids: list[str] | None = Field(
        default=None,
        description="Optional list of candidate UUIDs to preview. If provided, queries those IDs without review_status SQL filter; review_status is evaluated in Python.",
    )
    limit: int = Field(
        default=50,
        ge=1,
        le=100,
        description="Maximum number of candidates to preview (default 50, max 100).",
    )


class SignalPreviewItem(BaseModel):
    """A signal preview (not yet created in database)."""
    candidate_review_id: str
    ticker: str
    side: str
    confidence: str
    source: str
    preview_decision: str
    preview_score: str
    expected_return_pct: str | None
    reason: str
    raw_payload: dict


class SkippedCandidateDetail(BaseModel):
    """Detail on a skipped candidate."""
    candidate_review_id: str
    ticker: str
    reason: str


class ReviewSignalPreviewResponse(BaseModel):
    """Response from signal preview endpoint (PREVIEW ONLY, no database writes)."""
    idempotency_key: str
    execution_mode: str
    candidates_evaluated: int
    signal_previews_generated: int
    skipped_count: int
    signal_previews: list[SignalPreviewItem]
    skipped: list[SkippedCandidateDetail]
    signals_created: int
    decisions_created: int
    orders_created: int


class ReviewCreateSignalsRequest(BaseModel):
    """Request to create Signal rows from approved candidates in the review queue."""
    idempotency_key: str = Field(
        ...,
        description="Unique key for this signal creation batch.",
    )
    candidate_ids: list[str] | None = Field(
        default=None,
        description="Optional list of candidate IDs to process. If None, uses review_status filter.",
    )
    review_status: str = Field(
        default="APPROVED_FOR_SIGNAL",
        description="Review status to filter by (ignored if candidate_ids provided).",
    )
    limit: int = Field(
        default=50,
        ge=1,
        le=100,
        description="Maximum number of candidates to process (default 50, max 100).",
    )
    confirm_create_signals: bool = Field(
        default=False,
        description="Must be true to actually create Signal rows, else returns 422.",
    )


class CreatedSignalItem(BaseModel):
    """A signal that was created in the database."""
    candidate_review_id: str
    signal_id: str
    ticker: str
    side: str
    confidence: str
    source_run: str


class ReviewCreateSignalsResponse(BaseModel):
    """Response from create signals endpoint (creates actual Signal rows)."""
    execution_mode: str
    candidates_evaluated: int
    signals_created: int
    skipped_count: int
    skipped_existing_count: int
    created_signals: list[CreatedSignalItem]
    skipped: list[SkippedCandidateDetail]
    trade_decisions_created: int
    orders_created: int


class ReviewDecisionPreviewRequest(BaseModel):
    """Request to preview trade decisions for review-created Signal rows."""
    idempotency_key: str = Field(
        ...,
        description="Unique key for this decision preview batch.",
    )
    signal_ids: list[str] | None = Field(
        default=None,
        description="Optional list of signal IDs to preview. If provided, queries those exact IDs without source_run SQL filter; source_run and status are evaluated in Python.",
    )
    source_run_prefix: str = Field(
        default="review_queue_create_signals_v1:",
        description="Source run prefix to filter review-created signals (ignored if signal_ids provided).",
    )
    limit: int = Field(
        default=50,
        ge=1,
        le=100,
        description="Maximum number of signals to preview (default 50, max 100).",
    )
    received_only: bool = Field(
        default=True,
        description="If true, only preview Signals with status='RECEIVED' (ignored when signal_ids provided with received_only=false).",
    )


class DecisionPreviewItem(BaseModel):
    """A decision preview for a Signal (not yet converted to TradeDecision)."""
    signal_id: str
    ticker: str
    side: str
    confidence: str
    source_run: str
    signal_status: str
    preview_decision: str
    reason_code: str | None
    requested_notional: str
    approved_notional: str
    requested_qty: str
    approved_qty: str
    risk_snapshot: dict[str, Any]
    sizing_adjustments: list[str]
    reason: str


class SkippedSignalDetail(BaseModel):
    """Detail on a skipped signal."""
    signal_id: str
    ticker: str
    reason: str


class ReviewDecisionPreviewResponse(BaseModel):
    """Response from decision preview endpoint (PREVIEW ONLY, no database writes)."""
    execution_mode: str
    signals_evaluated: int
    decision_previews_generated: int
    skipped_count: int
    decision_previews: list[DecisionPreviewItem]
    skipped: list[SkippedSignalDetail]
    trade_decisions_created: int
    orders_created: int


class ReviewCreateDecisionsRequest(BaseModel):
    """Request to create TradeDecision rows from review-created Signal rows."""
    idempotency_key: str = Field(
        ...,
        description="Unique key for this decision creation batch.",
    )
    signal_ids: list[str] | None = Field(
        default=None,
        description="Optional list of signal IDs to process. If provided, queries those exact IDs and validates in Python; source_run and status are evaluated per request parameters.",
    )
    source_run_prefix: str = Field(
        default="review_queue_create_signals_v1:",
        description="Source run prefix to filter review-created signals (ignored if signal_ids provided).",
    )
    limit: int = Field(
        default=50,
        ge=1,
        le=100,
        description="Maximum number of signals to process (default 50, max 100).",
    )
    received_only: bool = Field(
        default=True,
        description="If true, only process Signals with status='RECEIVED'.",
    )
    confirm_create_decisions: bool = Field(
        default=False,
        description="Must be true to actually create TradeDecision rows, else returns 422.",
    )


class CreatedDecisionDetail(BaseModel):
    """A TradeDecision that was created in the database."""
    signal_id: str
    trade_decision_id: str
    ticker: str
    side: str
    decision: str
    reason_code: str | None
    requested_notional: str
    approved_notional: str
    requested_qty: str
    approved_qty: str
    job_run_id: str


class ReviewCreateDecisionsResponse(BaseModel):
    """Response from create decisions endpoint (creates actual TradeDecision rows)."""
    execution_mode: str
    signals_evaluated: int
    trade_decisions_created: int
    skipped_count: int
    skipped_existing_count: int
    created_decisions: list[CreatedDecisionDetail]
    skipped: list[SkippedSignalDetail]
    orders_created: int


class OrderPreviewItem(BaseModel):
    """A preview of an Order that would be created from a TradeDecision."""
    trade_decision_id: str
    signal_id: str
    ticker: str
    side: str
    order_type: str
    status: str
    qty: str
    notional: str
    decision: str
    reason_code: str | None
    source_run: str
    reason: str


class SkippedTradeDecisionDetail(BaseModel):
    """Detail on a skipped trade decision."""
    trade_decision_id: str
    ticker: str
    reason: str


class ReviewOrderPreviewRequest(BaseModel):
    """Request to preview Orders that would be created from TradeDecisions."""
    idempotency_key: str = Field(
        ...,
        description="Unique key for this order preview batch (not persisted).",
    )
    trade_decision_ids: list[str] | None = Field(
        default=None,
        description="Optional list of trade decision IDs to preview. If provided, validates in Python; source_run and approved status are evaluated per request parameters.",
    )
    source_run_prefix: str = Field(
        default="review_queue_create_signals_v1:",
        description="Source run prefix to filter review-created trade decisions (ignored if trade_decision_ids provided).",
    )
    limit: int = Field(
        default=50,
        ge=1,
        le=100,
        description="Maximum number of trade decisions to preview (default 50, max 100).",
    )
    approved_only: bool = Field(
        default=True,
        description="If true, only preview TradeDecisions with approved_qty > 0.",
    )


class ReviewOrderPreviewResponse(BaseModel):
    """Response from order preview endpoint (PREVIEW ONLY, no database writes)."""
    execution_mode: str
    trade_decisions_evaluated: int
    order_previews_generated: int
    skipped_count: int
    skipped_existing_count: int
    order_previews: list[OrderPreviewItem]
    skipped: list[SkippedTradeDecisionDetail]
    orders_created: int
    job_runs_created: int


class ReviewCandidatesCounts(BaseModel):
    """Count of review candidates by status."""
    total: int
    new: int
    watching: int
    approved_for_signal: int
    rejected: int


class ReviewCreatedSignalsCounts(BaseModel):
    """Count of review-created signals by status."""
    total: int
    received: int
    decision_made: int
    error: int


class ReviewCreatedDecisionsCounts(BaseModel):
    """Count of review-created trade decisions by decision type."""
    total: int
    buy: int
    sell: int
    rejected: int
    order_eligible: int
    already_has_order: int


class OrdersCounts(BaseModel):
    """Count of orders."""
    total: int
    review_created: int


class WorkflowStepStatus(BaseModel):
    """Status of a single workflow step."""
    step: str
    status: str
    reason: str


class WorkflowStatusResponse(BaseModel):
    """Complete workflow status with counts and step evaluation."""
    review_candidates: ReviewCandidatesCounts
    review_created_signals: ReviewCreatedSignalsCounts
    review_created_trade_decisions: ReviewCreatedDecisionsCounts
    orders: OrdersCounts
    workflow_steps: list[WorkflowStepStatus]
    safety: dict[str, bool]


class WeakestPositionDetail(BaseModel):
    """A position with unrealized P&L data for rotation analysis."""
    ticker: str
    qty: str
    avg_cost: str
    cost_basis: str
    latest_price: str | None
    current_value: str | None
    unrealized_pnl: str | None
    unrealized_pnl_pct: str | None
    sellable_for_rotation: bool
    blocked_reason: str | None


class CandidateStrengthDetail(BaseModel):
    """A candidate from the review queue with opportunity scoring."""
    candidate_review_id: str
    ticker: str
    decision: str
    recommendation: str | None
    preview_score: str
    prediction_confidence: str | None
    expected_return_pct: str | None
    candidate_score: str


class RotationPairDetail(BaseModel):
    """A proposed or rejected sell-then-buy rotation pair."""
    sell_ticker: str
    buy_ticker: str
    sell_unrealized_pnl_pct: str
    sell_unrealized_pnl: str
    buy_candidate_score: str
    improvement_score: str
    meets_threshold: bool
    reason: str
    safety_note: str


class RotationSafetyCounts(BaseModel):
    """Zero-write safety counters confirming no DB rows were created."""
    signals_created: int = 0
    decisions_created: int = 0
    orders_created: int = 0
    db_rows_created: int = 0


class RotationPreviewRequest(BaseModel):
    """Request to preview possible portfolio rotations (PREVIEW ONLY, no DB writes)."""
    candidate_review_ids: list[str] | None = Field(
        default=None,
        description="Optional list of CandidateReview UUIDs to consider. If None, uses approved_only filter.",
    )
    limit_pairs: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Maximum number of rotation pairs to propose.",
    )
    min_improvement_score: float = Field(
        default=5.0,
        description="Minimum improvement score (candidate_score - holding_unrealized_pnl_pct) required to propose a pair.",
    )
    min_exit_pnl_pct: float = Field(
        default=0.0,
        description="Minimum unrealized P&L % required to sell a position. Positions below this are blocked when block_loss_realization=True.",
    )
    block_loss_realization: bool = Field(
        default=True,
        description="If True, positions with unrealized_pnl_pct < min_exit_pnl_pct are blocked from rotation.",
    )
    max_price_age_days: int = Field(
        default=5,
        ge=1,
        le=30,
        description="Maximum age in calendar days for a price snapshot to be considered fresh.",
    )
    approved_only: bool = Field(
        default=True,
        description="If True and candidate_review_ids is None, only load APPROVED_FOR_SIGNAL candidates.",
    )


class RotationPreviewResponse(BaseModel):
    """Response from rotation preview endpoint (PREVIEW ONLY, no database writes)."""
    current_position_count: int
    max_positions: int
    capacity_available: bool
    rotation_required: bool
    block_loss_realization: bool
    min_exit_pnl_pct: str
    min_improvement_score: str
    candidates_considered: int
    positions_considered: int
    weakest_positions: list[WeakestPositionDetail]
    blocked_positions: list[WeakestPositionDetail]
    strongest_candidates: list[CandidateStrengthDetail]
    rotation_pairs: list[RotationPairDetail]
    rejected_pairs: list[RotationPairDetail]
    explanation: str
    safety_counts: RotationSafetyCounts


# ---------------------------------------------------------------------------
# Daily Plan Preview schemas
# ---------------------------------------------------------------------------

class DailyPlanSafetyCounts(BaseModel):
    """Zero-write safety counters confirming no DB rows were created."""
    signals_created: int = 0
    trade_decisions_created: int = 0
    orders_created: int = 0
    job_runs_created: int = 0
    db_rows_created: int = 0


class PortfolioCapacitySummary(BaseModel):
    """Portfolio capacity and value summary for daily plan."""
    total_value: str
    cash: str
    open_positions: int
    max_positions: int
    available_slots: int
    capacity_status: str


class BuyRecommendationItem(BaseModel):
    """A BUY candidate approved by the risk engine."""
    ticker: str
    candidate_review_id: str
    prediction_confidence: str | None
    expected_return_pct: str | None
    forecast_price_5d: str | None
    latest_price: str | None
    candidate_score: str
    approved_qty: str
    approved_notional: str
    reason: str


class SellRecommendationItem(BaseModel):
    """A position recommended for sale (SELL signal from review queue, PnL >= 0)."""
    ticker: str
    qty: str
    avg_cost: str
    latest_price: str | None
    unrealized_pnl: str
    unrealized_pnl_pct: str
    sell_qty: str
    sell_notional: str
    reason: str


class HoldPositionItem(BaseModel):
    """A position to hold (no sell signal, or sell blocked)."""
    ticker: str
    qty: str
    avg_cost: str
    latest_price: str | None
    unrealized_pnl: str | None
    unrealized_pnl_pct: str | None
    reason: str


class WatchCandidateItem(BaseModel):
    """A candidate to watch (HOLD/non-BUY recommendation, or ticker already held)."""
    candidate_review_id: str
    ticker: str
    prediction_recommendation: str | None
    prediction_confidence: str | None
    expected_return_pct: str | None
    preview_decision: str
    reason: str


class DailyPlanRotationItem(BaseModel):
    """A proposed rotation pair (sell a profitable holding, buy a candidate)."""
    sell_ticker: str
    buy_ticker: str
    sell_unrealized_pnl_pct: str
    sell_unrealized_pnl: str
    buy_candidate_score: str
    improvement_score: str
    meets_threshold: bool
    reason: str


class DailyPlanBlockedItem(BaseModel):
    """An action blocked by risk rules, with reason and plain-English explanation."""
    ticker: str
    action: str
    blocked_reason: str
    explanation: str


class DailyPlanPreviewRequest(BaseModel):
    """Request to generate a read-only consolidated daily trading plan."""
    approved_only: bool = Field(
        default=True,
        description="If True, only consider APPROVED_FOR_SIGNAL candidates.",
    )
    min_confidence: float = Field(
        default=0.65,
        ge=0.0,
        le=1.0,
        description="Minimum prediction confidence to include a BUY candidate.",
    )
    max_price_age_days: int = Field(
        default=5,
        ge=1,
        le=30,
        description="Maximum age in days for a price snapshot to be considered fresh.",
    )
    block_loss_realization: bool = Field(
        default=True,
        description="If True, positions with unrealized PnL < 0 are blocked from SELL.",
    )
    include_rotation: bool = Field(
        default=True,
        description="If True, include rotation plan when portfolio is at max positions.",
    )
    limit_candidates: int = Field(
        default=25,
        ge=1,
        le=100,
        description="Maximum number of candidates to evaluate.",
    )
    min_rotation_improvement_pct: float = Field(
        default=5.0,
        description="Minimum improvement score to propose a rotation pair.",
    )
    candidate_ids: list[str] | None = Field(
        default=None,
        description=(
            "If provided, evaluate only these specific CandidateReview UUIDs. "
            "An empty list evaluates zero candidates. "
            "Bypasses limit_candidates; approved_only still applies."
        ),
    )
    position_tickers: list[str] | None = Field(
        default=None,
        description=(
            "If provided, evaluate only open positions for these tickers (case-insensitive). "
            "Bypasses full portfolio scan; useful for deterministic preview scope."
        ),
    )


class DailyPlanPreviewResponse(BaseModel):
    """Response from daily plan preview endpoint (PREVIEW ONLY, no database writes)."""
    as_of: datetime
    portfolio_summary: PortfolioCapacitySummary
    buy_recommendations: list[BuyRecommendationItem]
    sell_recommendations: list[SellRecommendationItem]
    hold_positions: list[HoldPositionItem]
    watch_candidates: list[WatchCandidateItem]
    rotation_plan: list[DailyPlanRotationItem]
    blocked_actions: list[DailyPlanBlockedItem]
    recommended_next_action: str
    explanation: str
    safety_counts: DailyPlanSafetyCounts


# ---------------------------------------------------------------------------

class BackfillRequest(BaseModel):
    universe: str = Field(
        default="SP500",
        description="Universe name ('SP500'). Ignored if tickers provided.",
    )
    tickers: list[str] | None = Field(
        default=None,
        description="Explicit list of tickers to backfill. Takes precedence over universe.",
    )
    start_date: date = Field(
        description="Start date (inclusive) for historical prices.",
    )
    end_date: date = Field(
        description="End date (inclusive) for historical prices.",
    )
    price_type: str = Field(
        default=PriceType.CLOSE,
        description="Price type ('CLOSE'). Currently only CLOSE is supported.",
    )
    session_type: str = Field(
        default=SessionType.REGULAR,
        description="Session type ('REGULAR' for daily close).",
    )
    max_tickers: int = Field(
        description="Maximum number of tickers to process (required, capped at 50).",
    )
    dry_run: bool = Field(
        default=False,
        description="If true, fetch data but don't insert rows.",
    )


class BackfillResultDetail(BaseModel):
    ticker: str
    rows_found: int
    inserted: int
    updated: int
    skipped_existing: int
    status: str
    error: str | None = None


class BackfillFailure(BaseModel):
    ticker: str
    error: str


class BackfillResponse(BaseModel):
    universe: str
    requested_count: int
    processed_count: int
    inserted_count: int
    updated_count: int
    skipped_existing_count: int
    failed_count: int
    dry_run: bool
    start_date: str
    end_date: str
    results: list[BackfillResultDetail]
    failures: list[BackfillFailure]


class BenchmarkBackfillRequest(BaseModel):
    benchmark_tickers: list[str] = Field(
        description="List of benchmark tickers to backfill (e.g., ['SPY']).",
    )
    start_date: date = Field(
        description="Start date (inclusive) for historical prices.",
    )
    end_date: date = Field(
        description="End date (inclusive) for historical prices.",
    )
    price_type: str = Field(
        default=PriceType.CLOSE,
        description="Price type ('CLOSE' only for v1).",
    )
    session_type: str = Field(
        default=SessionType.REGULAR,
        description="Session type ('REGULAR' for daily close).",
    )
    max_benchmarks: int = Field(
        description="Maximum number of benchmarks to process (required, capped at 10).",
    )
    dry_run: bool = Field(
        default=False,
        description="If true, fetch data but don't insert rows.",
    )


class BenchmarkResultDetail(BaseModel):
    benchmark_ticker: str
    rows_found: int
    inserted: int
    updated: int
    skipped_existing: int
    status: str
    error: str | None = None


class BenchmarkBackfillResponse(BaseModel):
    requested_count: int
    processed_count: int
    inserted_count: int
    updated_count: int
    skipped_existing_count: int
    failed_count: int
    dry_run: bool
    start_date: str
    end_date: str
    results: list[BenchmarkResultDetail]
    failures: list[BackfillFailure]


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


@app.get(
    "/v1/auth/check",
    response_model=AuthCheckOut,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def auth_check() -> AuthCheckOut:
    """
    Lightweight authentication check endpoint.

    Requires valid X-API-Key header. Returns 401 if key is missing or invalid.
    No database operations; useful for UI-side connection validation.
    """
    return AuthCheckOut(
        authenticated=True,
        service=_SERVICE_NAME,
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
    # Allow processing of past market dates; only reject if trading NOW on a weekend
    if not is_weekday(now) and market_date >= now.date():
        # Return on weekend with skipped message (current-day or future trading)
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

    market_date defaults to current US-Eastern date if not supplied.
    """
    now, market_date = _now_and_date()
    if body.market_date is not None:
        market_date = body.market_date

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
    # Allow processing of past market dates; only reject if trading NOW on a weekend
    if not is_weekday(now) and market_date >= now.date():
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

    market_date defaults to current US-Eastern date if not supplied.
    """
    now, market_date = _now_and_date()
    if body.market_date is not None:
        market_date = body.market_date
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
        # Allow processing of past market dates; only reject if trading NOW on a weekend
        if not is_weekday(now) and market_date >= now.date():
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
    "/v1/market/backfill-prices",
    response_model=BackfillResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def backfill_prices(body: BackfillRequest) -> BackfillResponse:
    """
    Backfill historical daily CLOSE prices for a date range.

    Validates request, fetches prices from yfinance, and idempotently inserts
    PriceSnapshot rows. Does not overwrite existing rows (skips them instead).

    Request:
        universe: "SP500" (ignored if tickers provided).
        tickers: Explicit ticker list (takes precedence over universe).
        start_date: Start date (inclusive).
        end_date: End date (inclusive).
        price_type: "CLOSE" (hardcoded for v1).
        session_type: "REGULAR" (hardcoded for v1).
        max_tickers: Required. Max 50 for v1.
        dry_run: If true, fetches but doesn't insert rows.

    Validation:
        - max_tickers is required and must be <= 50.
        - start_date <= end_date.
        - date range <= 180 days.
        - If tickers null and universe != SP500, use SP500.

    Response:
        Returns 200 OK always (even with failures/empty results).
        Includes per-ticker results and failures array.
    """
    # Validation: max_tickers
    if body.max_tickers > 50:
        raise HTTPException(
            status_code=422,
            detail=f"max_tickers must be <= 50, got {body.max_tickers}",
        )

    # Validation: date range
    if body.start_date > body.end_date:
        raise HTTPException(
            status_code=422,
            detail=f"start_date must be <= end_date",
        )

    from datetime import timedelta
    date_range_days = (body.end_date - body.start_date).days
    if date_range_days > 180:
        raise HTTPException(
            status_code=422,
            detail=f"date range must be <= 180 days, got {date_range_days}",
        )

    # Resolve tickers: explicit tickers take precedence
    from paper_trader.engine.universe import get_sp500_universe
    if body.tickers:
        tickers_to_backfill = body.tickers
    else:
        if body.universe == "SP500":
            tickers_to_backfill = get_sp500_universe()
        else:
            tickers_to_backfill = get_sp500_universe()

    # Cap to max_tickers
    requested_count = len(tickers_to_backfill)
    tickers_to_backfill = tickers_to_backfill[:body.max_tickers]

    # Fetch historical prices from yfinance
    successful_prices, fetch_failures = fetch_historical_prices(
        tickers=tickers_to_backfill,
        start_date=body.start_date,
        end_date=body.end_date,
    )

    # Process results
    results = []
    inserted_count = 0
    updated_count = 0
    skipped_existing_count = 0
    failed_count = len(fetch_failures)

    # Counters for tickers in failures
    failures_list = [
        BackfillFailure(ticker=t, error=r)
        for t, r in fetch_failures.items()
    ]

    with get_dedicated_session() as session:
        for ticker in tickers_to_backfill:
            if ticker in fetch_failures:
                # Already recorded in failures_list
                continue

            ticker_data = successful_prices.get(ticker, [])
            if not ticker_data:
                # This shouldn't happen if fetch succeeded, but safety check
                failed_count += 1
                failures_list.append(
                    BackfillFailure(ticker=ticker, error="No price data")
                )
                results.append(
                    BackfillResultDetail(
                        ticker=ticker,
                        rows_found=0,
                        inserted=0,
                        updated=0,
                        skipped_existing=0,
                        status="FAILED",
                        error="No price data",
                    )
                )
                continue

            rows_found = len(ticker_data)
            ticker_inserted = 0
            ticker_skipped = 0

            for row in ticker_data:
                market_date = row["market_date"]
                price = row["price"]

                # Check if row already exists
                existing = session.execute(
                    select(PriceSnapshot).where(
                        PriceSnapshot.ticker == ticker,
                        PriceSnapshot.market_date == market_date,
                        PriceSnapshot.price_type == body.price_type,
                        PriceSnapshot.session_type == body.session_type,
                    )
                ).scalar_one_or_none()

                if existing:
                    ticker_skipped += 1
                    skipped_existing_count += 1
                else:
                    if not body.dry_run:
                        # Create snapshot_ts as datetime for that market_date at market close (16:00 UTC is typical)
                        from datetime import datetime
                        snapshot_ts = datetime.combine(market_date, datetime.min.time())
                        snapshot_ts = snapshot_ts.replace(hour=16, minute=0, second=0, tzinfo=timezone.utc)

                        ps = PriceSnapshot(
                            ticker=ticker,
                            price=price,
                            market_date=market_date,
                            price_type=body.price_type,
                            session_type=body.session_type,
                            snapshot_ts=snapshot_ts,
                            job_run_id=None,
                        )
                        session.add(ps)

                    ticker_inserted += 1
                    inserted_count += 1

            if not body.dry_run:
                session.commit()

            results.append(
                BackfillResultDetail(
                    ticker=ticker,
                    rows_found=rows_found,
                    inserted=ticker_inserted,
                    updated=0,
                    skipped_existing=ticker_skipped,
                    status="OK",
                    error=None,
                )
            )

    return BackfillResponse(
        universe=body.universe,
        requested_count=requested_count,
        processed_count=len(tickers_to_backfill),
        inserted_count=inserted_count,
        updated_count=updated_count,
        skipped_existing_count=skipped_existing_count,
        failed_count=failed_count,
        dry_run=body.dry_run,
        start_date=str(body.start_date),
        end_date=str(body.end_date),
        results=results,
        failures=failures_list,
    )


@app.post(
    "/v1/market/backfill-benchmark-prices",
    response_model=BenchmarkBackfillResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def backfill_benchmark_prices(body: BenchmarkBackfillRequest) -> BenchmarkBackfillResponse:
    """
    Backfill historical daily CLOSE prices for benchmark tickers (e.g., SPY).

    Validates request, fetches prices from yfinance, and idempotently inserts
    BenchmarkPrice rows. Does not overwrite existing rows (skips them instead).

    Request:
        benchmark_tickers: List of benchmark tickers (required, non-empty).
        start_date: Start date (inclusive).
        end_date: End date (inclusive).
        price_type: "CLOSE" only for v1 (default).
        session_type: "REGULAR" (default).
        max_benchmarks: Required. Max 10 for v1.
        dry_run: If true, fetches but doesn't insert rows.

    Validation:
        - benchmark_tickers is required and non-empty.
        - max_benchmarks is required and must be <= 10.
        - start_date <= end_date.
        - date range <= 180 days.
        - price_type must be "CLOSE" (only value supported in v1).

    Response:
        Returns 200 OK always (even with failures/empty results).
        Includes per-ticker results and failures array.
        Note: updated_count is always 0 in v1 (we skip existing, don't overwrite).
    """
    # Normalize and deduplicate benchmark tickers (order-preserving, deterministic)
    normalized_tickers = []
    seen = set()
    for raw in body.benchmark_tickers:
        ticker = raw.strip().upper()
        if not ticker:
            continue
        if ticker not in seen:
            normalized_tickers.append(ticker)
            seen.add(ticker)

    # Validation: benchmark_tickers must not be empty after normalization
    if not normalized_tickers:
        raise HTTPException(
            status_code=422,
            detail="benchmark_tickers is required and must not be empty",
        )

    # Validation: max_benchmarks
    if body.max_benchmarks > 10:
        raise HTTPException(
            status_code=422,
            detail=f"max_benchmarks must be <= 10, got {body.max_benchmarks}",
        )

    # Validation: price_type (only CLOSE supported in v1)
    if body.price_type != PriceType.CLOSE:
        raise HTTPException(
            status_code=422,
            detail=f"price_type must be '{PriceType.CLOSE}' in v1, got '{body.price_type}'",
        )

    # Validation: date range
    if body.start_date > body.end_date:
        raise HTTPException(
            status_code=422,
            detail="start_date must be <= end_date",
        )

    from datetime import timedelta
    date_range_days = (body.end_date - body.start_date).days
    if date_range_days > 180:
        raise HTTPException(
            status_code=422,
            detail=f"date range must be <= 180 days, got {date_range_days}",
        )

    # Cap to max_benchmarks
    requested_count = len(normalized_tickers)
    tickers_to_backfill = normalized_tickers[:body.max_benchmarks]

    # Fetch historical prices from yfinance
    successful_prices, fetch_failures = fetch_historical_prices(
        tickers=tickers_to_backfill,
        start_date=body.start_date,
        end_date=body.end_date,
    )

    # Process results
    results = []
    inserted_count = 0
    updated_count = 0
    skipped_existing_count = 0
    failed_count = len(fetch_failures)

    # Counters for tickers in failures
    failures_list = [
        BackfillFailure(ticker=t, error=r)
        for t, r in fetch_failures.items()
    ]

    with get_dedicated_session() as session:
        for ticker in tickers_to_backfill:
            if ticker in fetch_failures:
                # Already recorded in failures_list
                continue

            ticker_data = successful_prices.get(ticker, [])
            if not ticker_data:
                # This shouldn't happen if fetch succeeded, but safety check
                failed_count += 1
                failures_list.append(
                    BackfillFailure(ticker=ticker, error="No price data")
                )
                results.append(
                    BenchmarkResultDetail(
                        benchmark_ticker=ticker,
                        rows_found=0,
                        inserted=0,
                        updated=0,
                        skipped_existing=0,
                        status="FAILED",
                        error="No price data",
                    )
                )
                continue

            rows_found = len(ticker_data)
            ticker_inserted = 0
            ticker_skipped = 0

            for row in ticker_data:
                market_date = row["market_date"]
                price = row["price"]

                # Check if row already exists
                # BenchmarkPrice idempotency key: ticker + market_date + session_type
                existing = session.execute(
                    select(BenchmarkPrice).where(
                        BenchmarkPrice.ticker == ticker,
                        BenchmarkPrice.market_date == market_date,
                        BenchmarkPrice.session_type == body.session_type,
                    )
                ).scalar_one_or_none()

                if existing:
                    ticker_skipped += 1
                    skipped_existing_count += 1
                else:
                    if not body.dry_run:
                        # Create snapshot_ts as datetime for that market_date at market close (16:00 UTC)
                        from datetime import datetime
                        snapshot_ts = datetime.combine(market_date, datetime.min.time())
                        snapshot_ts = snapshot_ts.replace(hour=16, minute=0, second=0, tzinfo=timezone.utc)

                        bp = BenchmarkPrice(
                            ticker=ticker,
                            price=price,
                            market_date=market_date,
                            session_type=body.session_type,
                            snapshot_ts=snapshot_ts,
                            job_run_id=None,
                        )
                        session.add(bp)

                    ticker_inserted += 1
                    inserted_count += 1

            if not body.dry_run:
                session.commit()

            results.append(
                BenchmarkResultDetail(
                    benchmark_ticker=ticker,
                    rows_found=rows_found,
                    inserted=ticker_inserted,
                    updated=0,
                    skipped_existing=ticker_skipped,
                    status="OK",
                    error=None,
                )
            )

    return BenchmarkBackfillResponse(
        requested_count=requested_count,
        processed_count=len(tickers_to_backfill),
        inserted_count=inserted_count,
        updated_count=updated_count,
        skipped_existing_count=skipped_existing_count,
        failed_count=failed_count,
        dry_run=body.dry_run,
        start_date=str(body.start_date),
        end_date=str(body.end_date),
        results=results,
        failures=failures_list,
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

    # Accounting: evaluated = total - skipped; returned = top_n from evaluated
    return MarketScanResponse(
        universe=body.universe,
        scan_date=str(scan_date) if scan_date else None,
        benchmark_ticker=body.benchmark_ticker,
        total_universe_count=len(universe_tickers),
        evaluated_count=len(universe_tickers) - len(skipped),
        skipped_count=len(skipped),
        top_n=min(body.top_n, 100),
        candidates=[CandidateOut(**c.to_dict()) for c in candidates],
        skipped_tickers=[SkippedTickerOut(**s.to_dict()) for s in skipped],
    )


def _calculate_preview_score(
    normalized_prediction: dict | None,
    candidate_score: str | None,
    relative_strength_vs_spy_20d: str | None,
    momentum_20d_pct: str | None,
    status: str,
) -> str:
    """
    Calculate preview score (0-100 bounded, deterministic).

    Scoring formula:
    - BUY = +35
    - HOLD = +10
    - SELL = -25
    - confidence contribution = confidence * 30 (0-1 range, so 0-30)
    - positive expected_return_pct = min(expected_return_pct * 4, 20)
    - relative_strength_vs_spy_20d > 0 = +10
    - momentum_20d_pct > 0 = +5
    - status not OK = score 0
    - cap between 0 and 100
    """
    from decimal import Decimal

    if status != "OK":
        return "0"

    if not normalized_prediction:
        return "0"

    score = Decimal("0")

    # Recommendation bonus
    recommendation = normalized_prediction.get("recommendation")
    if recommendation == "BUY":
        score += Decimal("35")
    elif recommendation == "HOLD":
        score += Decimal("10")
    elif recommendation == "SELL":
        score -= Decimal("25")

    # Confidence contribution
    try:
        confidence = Decimal(str(normalized_prediction.get("confidence", "0")))
        score += confidence * Decimal("30")
    except Exception:
        pass

    # Positive expected return
    try:
        expected_return = Decimal(str(normalized_prediction.get("expected_return_pct", "0")))
        if expected_return > 0:
            score += min(expected_return * Decimal("4"), Decimal("20"))
    except Exception:
        pass

    # Relative strength bonus
    if relative_strength_vs_spy_20d:
        try:
            rs = Decimal(str(relative_strength_vs_spy_20d))
            if rs > 0:
                score += Decimal("10")
        except Exception:
            pass

    # Momentum bonus
    if momentum_20d_pct:
        try:
            mom = Decimal(str(momentum_20d_pct))
            if mom > 0:
                score += Decimal("5")
        except Exception:
            pass

    # Cap at 0-100
    score = max(Decimal("0"), min(score, Decimal("100")))
    return str(score.quantize(Decimal("0.01")))


def _determine_preview_decision(
    normalized_prediction: dict | None,
    status: str,
    expected_return_pct: str | None,
) -> tuple[str, list[str]]:
    """
    Determine preview decision (CONSIDER/WATCH/REJECT) and reasons.

    Decision logic:
    - CONSIDER if: status=OK, recommendation=BUY, confidence >= 0.70, expected_return_pct > 0
    - WATCH if: status=OK, (recommendation=HOLD OR confidence 0.50-0.69 OR expected_return_pct -0.5 to 0.5)
    - REJECT if: status not OK OR recommendation=SELL OR expected_return_pct < -0.5
    """
    from decimal import Decimal

    reasons = []

    # Failure status → REJECT
    if status != "OK":
        if status == "FAILED_FETCH":
            reasons.append("Prediction unavailable: API fetch failed")
        elif status == "FAILED_NORMALIZATION":
            reasons.append("Prediction unavailable: API response format invalid")
        elif status == "MISSING_PREDICTION":
            reasons.append("Prediction unavailable: No response from API")
        return "REJECT", reasons

    if not normalized_prediction:
        return "REJECT", ["Prediction unavailable: No data"]

    recommendation = normalized_prediction.get("recommendation")
    confidence = Decimal(str(normalized_prediction.get("confidence", "0")))
    expected_return = Decimal(str(expected_return_pct or "0"))

    # REJECT if SELL or negative return
    if recommendation == "SELL":
        reasons.append("Prediction SELL recommendation")
        return "REJECT", reasons

    if expected_return < Decimal("-0.5"):
        reasons.append(f"Expected return {expected_return}% below threshold")
        return "REJECT", reasons

    # CONSIDER if BUY + high confidence + positive return
    if (
        recommendation == "BUY"
        and confidence >= Decimal("0.70")
        and expected_return > Decimal("0")
    ):
        reasons.append("Prediction BUY with high confidence")
        reasons.append(f"Positive expected 5D return: {expected_return}%")
        return "CONSIDER", reasons

    # WATCH if HOLD or medium confidence or neutral return
    if recommendation == "HOLD":
        reasons.append("Prediction HOLD recommendation")
        return "WATCH", reasons

    if Decimal("0.50") <= confidence < Decimal("0.70"):
        reasons.append(f"Moderate confidence: {confidence * 100}%")
        return "WATCH", reasons

    if Decimal("-0.5") <= expected_return <= Decimal("0.5"):
        reasons.append(f"Expected return {expected_return}% near breakeven")
        return "WATCH", reasons

    # Default to WATCH for other cases
    return "WATCH", reasons


@app.post(
    "/v1/strategy/market-scan/prediction-candidates",
    response_model=MarketScanPredictionCandidatesResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def market_scan_prediction_candidates(
    body: MarketScanPredictionCandidatesRequest,
) -> MarketScanPredictionCandidatesResponse:
    """
    Preview endpoint: Scan market and fetch predictions for top candidates (V1 PREVIEW ONLY).

    This endpoint is PREVIEW-ONLY in V1. It runs market scan, selects top candidates,
    fetches predictions, and normalizes them WITHOUT creating any database artifacts
    (no Signal, TradeDecision, Order rows). No trading workflows are executed.

    V1 Rules (enforced):
        - dry_run must be true
        - submit_signals must be false
        - run_risk must be false
        - create_orders must be false
        - Returns 422 if any rule is violated

    Behavior:
        - Runs market scan to generate candidates
        - Excludes skipped tickers and DATA_QUALITY_OUTLIER tickers
        - Selects top prediction_top_n clean candidates
        - Fetches predictions from configured GCP API
        - Normalizes predictions to Paper Trader contract
        - Per-ticker fetch/normalization failures do not block others
        - Returns 200 with preview results even if all predictions fail (valid preview outcome)
    """
    # V1 Safety rules: enforce PREVIEW-ONLY mode
    if not body.dry_run or body.submit_signals or body.run_risk or body.create_orders:
        raise HTTPException(
            status_code=422,
            detail=(
                "V1 endpoint is PREVIEW-ONLY: dry_run must be true, "
                "submit_signals must be false, run_risk must be false, create_orders must be false"
            ),
        )

    now, market_date = _now_and_date()
    settings = get_settings()

    # Run market scan
    from paper_trader.engine.market_screener import scan_market as scan_market_fn
    from paper_trader.engine.universe import get_sp500_universe

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

    # Determine universe size
    universe_tickers = get_sp500_universe() if body.universe == "SP500" else []
    if body.tickers:
        universe_tickers = body.tickers

    # Filter candidates to exclude DATA_QUALITY_OUTLIER
    clean_candidates = [
        c for c in candidates
        if "DATA_QUALITY_OUTLIER" not in c.reason_codes
    ]

    # Select top prediction_top_n tickers from clean candidates
    selected_for_prediction = clean_candidates[:body.prediction_top_n]
    selected_tickers = [c.ticker for c in selected_for_prediction]

    # Fetch predictions for selected tickers
    fetched_responses = []
    fetch_failures = []

    if selected_tickers:
        try:
            fetched_responses, fetch_failures = await fetch_predictions_for_tickers(
                tickers=selected_tickers,
                api_url=settings.stock_prediction_api_url,
                timeout_seconds=settings.stock_prediction_api_timeout_seconds,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch predictions: {str(exc)}",
            )

    # Normalize fetched responses
    normalized_predictions = []
    all_prediction_failures: list[dict[str, str]] = []

    # Add fetch failures (API didn't return a response for these tickers)
    for failure in fetch_failures:
        all_prediction_failures.append(failure)

    # Build set of tickers that failed fetch
    failed_fetch_tickers = {f["ticker"] for f in fetch_failures}

    # Process fetched responses: normalize and track failures
    normalized_by_ticker = {}
    for raw_response in fetched_responses:
        ticker = raw_response.get("ticker", "unknown")
        normalized, error_reason = normalize_prediction_response_with_error(raw_response)
        if normalized:
            normalized_predictions.append(NormalizedPrediction(**normalized))
            normalized_by_ticker[ticker] = normalized
        else:
            # Normalization failed for this ticker
            all_prediction_failures.append({
                "ticker": ticker,
                "reason": f"Normalization failed: {error_reason or 'Invalid API response format'}"
            })

    # Check for selected_tickers missing from both fetched_responses and fetch_failures
    fetched_tickers = {r.get("ticker", "unknown") for r in fetched_responses}
    for selected_ticker in selected_tickers:
        if selected_ticker not in fetched_tickers and selected_ticker not in failed_fetch_tickers:
            all_prediction_failures.append({
                "ticker": selected_ticker,
                "reason": "No prediction response received from API"
            })

    # Build candidate_previews: one row per selected_ticker
    # This combines scan data with prediction data and decision logic
    candidate_previews = []

    # Create a map of selected tickers to their scan candidates for quick lookup
    selected_candidates_map = {c.ticker: c for c in selected_for_prediction}

    # Track which tickers failed and why
    failed_fetch_map = {f["ticker"]: "FAILED_FETCH" for f in fetch_failures}
    failed_norm_map = {}
    missing_pred_map = {}

    # Track normalization failures
    for failure in all_prediction_failures:
        ticker = failure["ticker"]
        if ticker in failed_fetch_tickers:
            failed_fetch_map[ticker] = "FAILED_FETCH"
        elif "Normalization failed" in failure.get("reason", ""):
            failed_norm_map[ticker] = "FAILED_NORMALIZATION"
        elif "No prediction response" in failure.get("reason", ""):
            missing_pred_map[ticker] = "MISSING_PREDICTION"

    # Build preview for each selected ticker
    for selected_ticker in selected_tickers:
        candidate = selected_candidates_map.get(selected_ticker)
        normalized = normalized_by_ticker.get(selected_ticker)

        # Determine status
        if selected_ticker in failed_fetch_map:
            status = "FAILED_FETCH"
        elif selected_ticker in failed_norm_map:
            status = "FAILED_NORMALIZATION"
        elif selected_ticker in missing_pred_map:
            status = "MISSING_PREDICTION"
        elif normalized:
            status = "OK"
        else:
            status = "MISSING_PREDICTION"

        # Extract scan data
        scan_rank = candidate.rank if candidate else None
        scan_score = candidate.score if candidate else None
        latest_price = candidate.latest_price if candidate else None
        momentum_5d_pct = candidate.momentum_5d_pct if candidate else None
        momentum_20d_pct = candidate.momentum_20d_pct if candidate else None
        relative_strength_vs_spy_20d = candidate.relative_strength_vs_spy_20d if candidate else None
        scan_reason_codes = candidate.reason_codes if candidate else []

        # Extract prediction data
        prediction_recommendation = normalized.get("recommendation") if normalized else None
        prediction_confidence = normalized.get("confidence") if normalized else None
        forecast_price_5d = normalized.get("forecast_price_5d") if normalized else None
        expected_return_pct = normalized.get("expected_return_pct") if normalized else None
        market_context = normalized.get("market_context") if normalized else None

        # Determine preview decision and reasons
        preview_decision, preview_reasons = _determine_preview_decision(
            normalized, status, expected_return_pct
        )

        # Add scan context to reasons if available
        if status == "OK" and preview_decision != "REJECT":
            if relative_strength_vs_spy_20d:
                try:
                    from decimal import Decimal
                    rs = Decimal(str(relative_strength_vs_spy_20d))
                    if rs > 0:
                        preview_reasons.append(f"Outperforming SPY by {relative_strength_vs_spy_20d}%")
                except Exception:
                    pass

            if momentum_20d_pct:
                try:
                    from decimal import Decimal
                    mom = Decimal(str(momentum_20d_pct))
                    if mom > 0:
                        preview_reasons.append(f"Positive 20D momentum: {momentum_20d_pct}%")
                except Exception:
                    pass

        # Calculate preview score
        preview_score = _calculate_preview_score(
            normalized,
            scan_score,
            relative_strength_vs_spy_20d,
            momentum_20d_pct,
            status,
        )

        # Build preview row
        preview = CandidatePreview(
            ticker=selected_ticker,
            scan_rank=scan_rank,
            scan_score=scan_score,
            latest_price=latest_price,
            momentum_5d_pct=momentum_5d_pct,
            momentum_20d_pct=momentum_20d_pct,
            relative_strength_vs_spy_20d=relative_strength_vs_spy_20d,
            scan_reason_codes=scan_reason_codes,
            prediction_recommendation=prediction_recommendation,
            prediction_confidence=prediction_confidence,
            forecast_price_5d=forecast_price_5d,
            expected_return_pct=expected_return_pct,
            market_context=market_context,
            preview_decision=preview_decision,
            preview_score=preview_score,
            preview_reasons=preview_reasons,
            status=status,
        )
        candidate_previews.append(preview)

    # Prepare response (no database writes, no workflow execution)
    return MarketScanPredictionCandidatesResponse(
        idempotency_key=body.idempotency_key,
        dry_run=True,
        execution_mode="PREVIEW_ONLY",
        scan=ScanSummaryOut(
            universe=body.universe,
            scan_date=str(scan_date) if scan_date else None,
            total_universe_count=len(universe_tickers),
            evaluated_count=len(universe_tickers) - len(skipped),
            skipped_count=len(skipped),
            candidate_count=len(clean_candidates),
        ),
        selected_tickers=selected_tickers,
        predictions_fetched=len(fetched_responses),
        prediction_failures=[
            PredictionFailureDetail(**f) for f in all_prediction_failures
        ],
        normalized_predictions=normalized_predictions,
        candidate_previews=candidate_previews,
        signals_submitted=0,
        decisions_made=0,
        orders_created=0,
    )


# ---------------------------------------------------------------------------
# Candidate Review Queue endpoints
# ---------------------------------------------------------------------------

@app.post(
    "/v1/review/candidates",
    response_model=CandidateReviewSaveResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def save_review_candidates(
    body: CandidateReviewSaveRequest,
) -> CandidateReviewSaveResponse:
    """
    Save candidate previews to the review queue.

    Candidates are stored with review_status=NEW for later manual approval.
    This endpoint is idempotent on (idempotency_key, ticker) — reposting the
    same pair is a no-op and counted in skipped_existing_count.

    IMPORTANT: This endpoint does NOT create Signal, TradeDecision, or Order rows.
    Saving to the review queue does not trigger any trading workflows.
    """
    inserted = 0
    skipped = 0
    saved_rows = []

    with get_session() as session:
        for candidate in body.candidates:
            # Check if (idempotency_key, ticker) already exists
            existing = session.query(CandidateReview).filter(
                CandidateReview.idempotency_key == body.idempotency_key,
                CandidateReview.ticker == candidate.ticker,
            ).first()

            if existing:
                skipped += 1
                saved_rows.append(CandidateReviewOut(
                    id=str(existing.id),
                    idempotency_key=existing.idempotency_key,
                    ticker=existing.ticker,
                    scan_rank=existing.scan_rank,
                    scan_score=existing.scan_score,
                    latest_price=existing.latest_price,
                    momentum_5d_pct=existing.momentum_5d_pct,
                    momentum_20d_pct=existing.momentum_20d_pct,
                    relative_strength_vs_spy_20d=existing.relative_strength_vs_spy_20d,
                    scan_reason_codes=existing.scan_reason_codes,
                    prediction_recommendation=existing.prediction_recommendation,
                    prediction_confidence=existing.prediction_confidence,
                    forecast_price_5d=existing.forecast_price_5d,
                    expected_return_pct=existing.expected_return_pct,
                    market_context=existing.market_context,
                    preview_decision=existing.preview_decision,
                    preview_score=existing.preview_score,
                    preview_reasons=existing.preview_reasons,
                    status=existing.status,
                    review_status=existing.review_status,
                    created_at=existing.created_at,
                    updated_at=existing.updated_at,
                ))
                continue

            # Insert new row
            new_review = CandidateReview(
                idempotency_key=body.idempotency_key,
                ticker=candidate.ticker,
                scan_rank=str(candidate.scan_rank) if candidate.scan_rank is not None else None,
                scan_score=candidate.scan_score,
                latest_price=candidate.latest_price,
                momentum_5d_pct=candidate.momentum_5d_pct,
                momentum_20d_pct=candidate.momentum_20d_pct,
                relative_strength_vs_spy_20d=candidate.relative_strength_vs_spy_20d,
                scan_reason_codes=candidate.scan_reason_codes or [],
                prediction_recommendation=candidate.prediction_recommendation,
                prediction_confidence=candidate.prediction_confidence,
                forecast_price_5d=candidate.forecast_price_5d,
                expected_return_pct=candidate.expected_return_pct,
                market_context=candidate.market_context,
                preview_decision=candidate.preview_decision,
                preview_score=candidate.preview_score,
                preview_reasons=candidate.preview_reasons or [],
                status=candidate.status,
                review_status="NEW",
            )
            session.add(new_review)
            session.flush()

            inserted += 1
            saved_rows.append(CandidateReviewOut(
                id=str(new_review.id),
                idempotency_key=new_review.idempotency_key,
                ticker=new_review.ticker,
                scan_rank=new_review.scan_rank,
                scan_score=new_review.scan_score,
                latest_price=new_review.latest_price,
                momentum_5d_pct=new_review.momentum_5d_pct,
                momentum_20d_pct=new_review.momentum_20d_pct,
                relative_strength_vs_spy_20d=new_review.relative_strength_vs_spy_20d,
                scan_reason_codes=new_review.scan_reason_codes,
                prediction_recommendation=new_review.prediction_recommendation,
                prediction_confidence=new_review.prediction_confidence,
                forecast_price_5d=new_review.forecast_price_5d,
                expected_return_pct=new_review.expected_return_pct,
                market_context=new_review.market_context,
                preview_decision=new_review.preview_decision,
                preview_score=new_review.preview_score,
                preview_reasons=new_review.preview_reasons,
                status=new_review.status,
                review_status=new_review.review_status,
                created_at=new_review.created_at,
                updated_at=new_review.updated_at,
            ))

    return CandidateReviewSaveResponse(
        inserted_count=inserted,
        skipped_existing_count=skipped,
        candidates_saved=saved_rows,
    )


@app.get(
    "/v1/review/candidates",
    response_model=list[CandidateReviewOut],
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def list_review_candidates(
    status: str | None = Query(None, description="Filter by review_status (NEW, WATCHING, REJECTED, APPROVED_FOR_SIGNAL)"),
    ticker: str | None = Query(None, description="Filter by ticker"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of rows to return"),
) -> list[CandidateReviewOut]:
    """
    List candidates in the review queue.

    Returns rows ordered by created_at descending (most recent first).
    Optionally filters by review_status and/or ticker.
    """
    with get_session() as session:
        query = session.query(CandidateReview)

        if status:
            query = query.filter(CandidateReview.review_status == status)
        if ticker:
            query = query.filter(CandidateReview.ticker == ticker)

        rows = query.order_by(CandidateReview.created_at.desc()).limit(limit).all()

        return [
            CandidateReviewOut(
                id=str(row.id),
                idempotency_key=row.idempotency_key,
                ticker=row.ticker,
                scan_rank=row.scan_rank,
                scan_score=row.scan_score,
                latest_price=row.latest_price,
                momentum_5d_pct=row.momentum_5d_pct,
                momentum_20d_pct=row.momentum_20d_pct,
                relative_strength_vs_spy_20d=row.relative_strength_vs_spy_20d,
                scan_reason_codes=row.scan_reason_codes,
                prediction_recommendation=row.prediction_recommendation,
                prediction_confidence=row.prediction_confidence,
                forecast_price_5d=row.forecast_price_5d,
                expected_return_pct=row.expected_return_pct,
                market_context=row.market_context,
                preview_decision=row.preview_decision,
                preview_score=row.preview_score,
                preview_reasons=row.preview_reasons,
                status=row.status,
                review_status=row.review_status,
                created_at=row.created_at,
                updated_at=row.updated_at,
            )
            for row in rows
        ]


@app.patch(
    "/v1/review/candidates/{candidate_id}",
    response_model=CandidateReviewOut,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def update_review_candidate_status(
    candidate_id: str,
    body: CandidateReviewStatusUpdate,
) -> CandidateReviewOut:
    """
    Update the review_status of a candidate in the review queue.

    Allowed values: NEW, WATCHING, REJECTED, APPROVED_FOR_SIGNAL

    IMPORTANT: APPROVED_FOR_SIGNAL is a label only. It does NOT create a Signal,
    TradeDecision, or Order row. Approving a candidate for signal does not
    trigger any trading workflows.

    Returns 404 if the candidate_id is not found.
    Returns 422 if the review_status value is invalid.
    """
    # Validate review_status
    allowed_statuses = {"NEW", "WATCHING", "REJECTED", "APPROVED_FOR_SIGNAL"}
    if body.review_status not in allowed_statuses:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid review_status '{body.review_status}'. Allowed: {allowed_statuses}",
        )

    with get_session() as session:
        # Attempt to parse candidate_id as UUID
        try:
            import uuid as uuid_module
            candidate_uuid = uuid_module.UUID(candidate_id)
        except (ValueError, AttributeError):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Candidate '{candidate_id}' not found.",
            )

        # Query by id
        row = session.query(CandidateReview).filter(
            CandidateReview.id == candidate_uuid
        ).first()

        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Candidate '{candidate_id}' not found.",
            )

        # Update review_status
        row.review_status = body.review_status
        session.add(row)
        session.flush()

        return CandidateReviewOut(
            id=str(row.id),
            idempotency_key=row.idempotency_key,
            ticker=row.ticker,
            scan_rank=row.scan_rank,
            scan_score=row.scan_score,
            latest_price=row.latest_price,
            momentum_5d_pct=row.momentum_5d_pct,
            momentum_20d_pct=row.momentum_20d_pct,
            relative_strength_vs_spy_20d=row.relative_strength_vs_spy_20d,
            scan_reason_codes=row.scan_reason_codes,
            prediction_recommendation=row.prediction_recommendation,
            prediction_confidence=row.prediction_confidence,
            forecast_price_5d=row.forecast_price_5d,
            expected_return_pct=row.expected_return_pct,
            market_context=row.market_context,
            preview_decision=row.preview_decision,
            preview_score=row.preview_score,
            preview_reasons=row.preview_reasons,
            status=row.status,
            review_status=row.review_status,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )


@app.post(
    "/v1/review/signal-preview",
    response_model=ReviewSignalPreviewResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def preview_signal_from_candidates(
    body: ReviewSignalPreviewRequest,
) -> ReviewSignalPreviewResponse:
    """
    Generate signal previews from approved candidates in the review queue.

    This endpoint is PREVIEW ONLY. No Signal, TradeDecision, Order, or JobRun
    rows are created. It shows what signals WOULD be generated without actually
    creating them.

    Filtering rules:
    - If candidate_ids is NOT provided: query by review_status in SQL
    - If candidate_ids IS provided: query all requested IDs, evaluate review_status in Python
    - Only candidates with status='OK' produce signal previews
    - HOLD recommendations are skipped (not actionable)
    - Missing/invalid prediction_recommendation, prediction_confidence are skipped
    - Review status mismatch (when candidate_ids provided) adds to skipped list

    Returns 0 for signals_created, decisions_created, orders_created.
    """
    import uuid as uuid_module

    evaluated = 0
    generated = 0
    skipped_list = []
    signal_previews = []

    with get_session() as session:
        query = session.query(CandidateReview)

        # Build query based on whether candidate_ids is provided
        if body.candidate_ids:
            # Query all requested IDs without review_status filter
            try:
                candidate_uuids = [uuid_module.UUID(cid) for cid in body.candidate_ids]
                query = query.filter(CandidateReview.id.in_(candidate_uuids))
            except (ValueError, AttributeError):
                # If any UUID is invalid, return empty result
                return ReviewSignalPreviewResponse(
                    idempotency_key=body.idempotency_key,
                    execution_mode="PREVIEW_ONLY",
                    candidates_evaluated=0,
                    signal_previews_generated=0,
                    skipped_count=0,
                    signal_previews=[],
                    skipped=[],
                    signals_created=0,
                    decisions_created=0,
                    orders_created=0,
                )
        else:
            # Filter by review_status in SQL
            query = query.filter(CandidateReview.review_status == body.review_status)

        # Apply limit
        rows = query.limit(body.limit).all()

        # Process each row
        for row in rows:
            evaluated += 1
            candidate_id_str = str(row.id)

            # If candidate_ids was provided, check review_status in Python
            if body.candidate_ids and row.review_status != body.review_status:
                skipped_list.append(SkippedCandidateDetail(
                    candidate_review_id=candidate_id_str,
                    ticker=row.ticker,
                    reason=f"Review status is {row.review_status}, not {body.review_status}",
                ))
                continue

            # Check status is OK
            if row.status != "OK":
                skipped_list.append(SkippedCandidateDetail(
                    candidate_review_id=candidate_id_str,
                    ticker=row.ticker,
                    reason=f"Status is {row.status}, not OK",
                ))
                continue

            # Check recommendation exists
            if not row.prediction_recommendation:
                skipped_list.append(SkippedCandidateDetail(
                    candidate_review_id=candidate_id_str,
                    ticker=row.ticker,
                    reason="Missing prediction_recommendation",
                ))
                continue

            # Check HOLD is skipped
            if row.prediction_recommendation.upper() == "HOLD":
                skipped_list.append(SkippedCandidateDetail(
                    candidate_review_id=candidate_id_str,
                    ticker=row.ticker,
                    reason="HOLD recommendations do not create actionable signal previews",
                ))
                continue

            # Check confidence is present and valid
            if not row.prediction_confidence:
                skipped_list.append(SkippedCandidateDetail(
                    candidate_review_id=candidate_id_str,
                    ticker=row.ticker,
                    reason="Missing prediction_confidence",
                ))
                continue

            try:
                confidence_decimal = Decimal(row.prediction_confidence)
                if not (Decimal("0") <= confidence_decimal <= Decimal("1")):
                    skipped_list.append(SkippedCandidateDetail(
                        candidate_review_id=candidate_id_str,
                        ticker=row.ticker,
                        reason=f"Confidence {confidence_decimal} out of range [0, 1]",
                    ))
                    continue
            except (ValueError, Exception):
                skipped_list.append(SkippedCandidateDetail(
                    candidate_review_id=candidate_id_str,
                    ticker=row.ticker,
                    reason=f"Invalid confidence: {row.prediction_confidence}",
                ))
                continue

            # Map recommendation to side (BUY/SELL only)
            side = None
            rec_upper = row.prediction_recommendation.upper()
            if rec_upper == "BUY":
                side = "BUY"
            elif rec_upper == "SELL":
                side = "SELL"
            else:
                skipped_list.append(SkippedCandidateDetail(
                    candidate_review_id=candidate_id_str,
                    ticker=row.ticker,
                    reason=f"Invalid recommendation: {row.prediction_recommendation}",
                ))
                continue

            # Build raw_payload with traceability fields
            raw_payload = {
                "candidate_review_id": candidate_id_str,
                "prediction_recommendation": row.prediction_recommendation,
                "prediction_confidence": row.prediction_confidence,
                "forecast_price_5d": row.forecast_price_5d,
                "market_context": row.market_context,
                "preview_reasons": row.preview_reasons or [],
            }

            # Create signal preview item with all required fields
            signal_preview = SignalPreviewItem(
                candidate_review_id=candidate_id_str,
                ticker=row.ticker,
                side=side,
                confidence=row.prediction_confidence,
                source="review_queue_preview_v1",
                preview_decision=row.preview_decision,
                preview_score=row.preview_score,
                expected_return_pct=row.expected_return_pct,
                reason=f"Preview only: would create {side} signal from approved review candidate.",
                raw_payload=raw_payload,
            )
            signal_previews.append(signal_preview)
            generated += 1

    return ReviewSignalPreviewResponse(
        idempotency_key=body.idempotency_key,
        execution_mode="PREVIEW_ONLY",
        candidates_evaluated=evaluated,
        signal_previews_generated=generated,
        skipped_count=len(skipped_list),
        signal_previews=signal_previews,
        skipped=skipped_list,
        signals_created=0,
        decisions_created=0,
        orders_created=0,
    )


@app.post(
    "/v1/review/create-signals",
    response_model=ReviewCreateSignalsResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def create_signals_from_candidates(
    body: ReviewCreateSignalsRequest,
) -> ReviewCreateSignalsResponse:
    """
    Create Signal rows from approved candidates in the review queue.

    This endpoint creates actual Signal rows (not previews). It does NOT create
    TradeDecision, Order, or trigger any downstream workflows.

    Validation rules:
    - Only candidates with status='OK' create signals
    - Only BUY/SELL recommendations create signals (HOLD is skipped)
    - Confidence must be numeric in range [0, 1]
    - Duplicate protection: per-candidate source_run prevents duplicate Signals
    - confirm_create_signals must be true, else returns 422

    Filtering rules:
    - If candidate_ids is NOT provided: query by review_status in SQL
    - If candidate_ids IS provided: query those IDs, evaluate review_status in Python

    Response distinguishes:
    - signals_created: new Signals just created
    - skipped_existing_count: candidates that already had Signals (duplicate protection)
    - skipped_count: candidates skipped due to validation errors
    """
    import uuid as uuid_module

    # Validation: confirm_create_signals must be true
    if not body.confirm_create_signals:
        raise HTTPException(
            status_code=422,
            detail="confirm_create_signals must be true to create Signal rows.",
        )

    evaluated = 0
    created = 0
    skipped_list = []
    skipped_existing = 0
    created_signals_list = []

    with get_session() as session:
        # Determine market_date (US Eastern)
        eastern_now = datetime.now(_EASTERN)
        market_date = eastern_now.date()

        # Build query for candidates
        query = session.query(CandidateReview)

        if body.candidate_ids:
            # Query explicit candidate IDs
            try:
                candidate_uuids = [uuid_module.UUID(cid) for cid in body.candidate_ids]
                query = query.filter(CandidateReview.id.in_(candidate_uuids))
            except (ValueError, AttributeError):
                # If any UUID is invalid, return empty result with 0 created
                return ReviewCreateSignalsResponse(
                    execution_mode="SIGNAL_CREATION_ONLY",
                    candidates_evaluated=0,
                    signals_created=0,
                    skipped_count=0,
                    skipped_existing_count=0,
                    created_signals=[],
                    skipped=[],
                    trade_decisions_created=0,
                    orders_created=0,
                )
        else:
            # Filter by review_status in SQL
            query = query.filter(CandidateReview.review_status == body.review_status)

        # Apply limit
        rows = query.limit(body.limit).all()

        # First pass: collect signals to create and validation skips
        signals_to_create = []  # List of (row, direction, confidence_decimal, source_run)

        # Process each row
        for row in rows:
            evaluated += 1
            candidate_id_str = str(row.id)

            # If candidate_ids was provided, check review_status in Python
            if body.candidate_ids and row.review_status != body.review_status:
                skipped_list.append(SkippedCandidateDetail(
                    candidate_review_id=candidate_id_str,
                    ticker=row.ticker,
                    reason=f"Review status is {row.review_status}, not {body.review_status}",
                ))
                continue

            # Check status is OK
            if row.status != "OK":
                skipped_list.append(SkippedCandidateDetail(
                    candidate_review_id=candidate_id_str,
                    ticker=row.ticker,
                    reason=f"Status is {row.status}, not OK",
                ))
                continue

            # Check recommendation exists
            if not row.prediction_recommendation:
                skipped_list.append(SkippedCandidateDetail(
                    candidate_review_id=candidate_id_str,
                    ticker=row.ticker,
                    reason="Missing prediction_recommendation",
                ))
                continue

            # Check HOLD is skipped
            if row.prediction_recommendation.upper() == "HOLD":
                skipped_list.append(SkippedCandidateDetail(
                    candidate_review_id=candidate_id_str,
                    ticker=row.ticker,
                    reason="HOLD recommendations do not create actionable signals",
                ))
                continue

            # Check confidence is present and valid
            if not row.prediction_confidence:
                skipped_list.append(SkippedCandidateDetail(
                    candidate_review_id=candidate_id_str,
                    ticker=row.ticker,
                    reason="Missing prediction_confidence",
                ))
                continue

            try:
                confidence_decimal = Decimal(row.prediction_confidence)
                if not (Decimal("0") <= confidence_decimal <= Decimal("1")):
                    skipped_list.append(SkippedCandidateDetail(
                        candidate_review_id=candidate_id_str,
                        ticker=row.ticker,
                        reason=f"Confidence {confidence_decimal} out of range [0, 1]",
                    ))
                    continue
            except (ValueError, Exception):
                skipped_list.append(SkippedCandidateDetail(
                    candidate_review_id=candidate_id_str,
                    ticker=row.ticker,
                    reason=f"Invalid confidence: {row.prediction_confidence}",
                ))
                continue

            # Map recommendation to direction (BUY/SELL only)
            direction = None
            rec_upper = row.prediction_recommendation.upper()
            if rec_upper == "BUY":
                direction = "BUY"
            elif rec_upper == "SELL":
                direction = "SELL"
            else:
                skipped_list.append(SkippedCandidateDetail(
                    candidate_review_id=candidate_id_str,
                    ticker=row.ticker,
                    reason=f"Invalid recommendation: {row.prediction_recommendation}",
                ))
                continue

            # Deterministic source_run based on candidate_review_id
            source_run = f"review_queue_create_signals_v1:{candidate_id_str}"

            # Check if Signal already exists with this source_run+ticker+direction
            existing_signal = session.query(Signal).filter(
                Signal.source_run == source_run,
                Signal.ticker == row.ticker,
                Signal.direction == direction,
            ).first()

            if existing_signal:
                skipped_existing += 1
                skipped_list.append(SkippedCandidateDetail(
                    candidate_review_id=candidate_id_str,
                    ticker=row.ticker,
                    reason="Signal already exists for candidate review",
                ))
                continue

            # This candidate will create a new signal; collect it
            signals_to_create.append((row, direction, confidence_decimal, source_run, candidate_id_str))

        # Only create JobRun if we have new signals to create
        job_run = None
        if signals_to_create:
            job_run = JobRun(
                idempotency_key=body.idempotency_key,
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=market_date,
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
                result_summary={},
            )
            session.add(job_run)
            session.flush()

            # Second pass: create signals now that we have a JobRun
            for row, direction, confidence_decimal, source_run, candidate_id_str in signals_to_create:
                # Build raw_payload with traceability fields
                raw_payload = {
                    "source": "review_queue_create_signals_v1",
                    "candidate_review_id": candidate_id_str,
                    "request_idempotency_key": body.idempotency_key,
                    "review_status": body.review_status,
                    "preview_decision": row.preview_decision,
                    "preview_score": row.preview_score,
                    "expected_return_pct": row.expected_return_pct,
                    "forecast_price_5d": row.forecast_price_5d,
                    "market_context": row.market_context,
                    "preview_reasons": row.preview_reasons or [],
                }

                # Create Signal row
                signal = Signal(
                    job_run_id=job_run.id,
                    ticker=row.ticker,
                    direction=direction,
                    confidence=confidence_decimal,
                    signal_ts=datetime.now(timezone.utc),
                    market_date=market_date,
                    source_run=source_run,
                    status="RECEIVED",
                    raw_payload=raw_payload,
                )
                session.add(signal)
                session.flush()

                created_signals_list.append(CreatedSignalItem(
                    candidate_review_id=candidate_id_str,
                    signal_id=str(signal.id),
                    ticker=row.ticker,
                    side=direction,
                    confidence=str(confidence_decimal),
                    source_run=source_run,
                ))
                created += 1

            # Update JobRun result_summary with counts
            job_run.result_summary = {
                "signals_created": created,
                "skipped_count": len(skipped_list),
                "skipped_existing_count": skipped_existing,
            }
            session.add(job_run)
            session.flush()

    # Count existing TradeDecision and Order rows to prove none were created
    with get_session() as session:
        trade_decision_count = session.query(TradeDecision).count()
        order_count = session.query(Order).count()

    return ReviewCreateSignalsResponse(
        execution_mode="SIGNAL_CREATION_ONLY",
        candidates_evaluated=evaluated,
        signals_created=created,
        skipped_count=len(skipped_list),
        skipped_existing_count=skipped_existing,
        created_signals=created_signals_list,
        skipped=skipped_list,
        trade_decisions_created=0,
        orders_created=0,
    )


@app.post(
    "/v1/review/decision-preview",
    response_model=ReviewDecisionPreviewResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def preview_decisions_from_signals(
    body: ReviewDecisionPreviewRequest,
) -> ReviewDecisionPreviewResponse:
    """
    Preview trade decisions that would be made for review-created Signal rows.

    This endpoint is PREVIEW ONLY. No TradeDecision, Order, or JobRun rows are
    created. It shows what decision would be made without actually creating them.
    Signal.status is NOT updated.

    Filtering rules:
    - If signal_ids is NOT provided: query by source_run_prefix in SQL, optionally filter by status
    - If signal_ids IS provided: query those exact IDs, validate source_run and status in Python

    For each Signal:
    - Fetch the latest PriceSnapshot for the ticker
    - Call evaluate_signal() with the Signal data
    - Return the resulting RiskDecision as a preview
    - If price is missing, evaluate_signal() will return NO_PRICE_SNAPSHOT reason

    Returns 0 for trade_decisions_created and orders_created.
    """
    import uuid as uuid_module

    evaluated = 0
    generated = 0
    skipped_list = []
    decision_previews = []

    with get_session() as session:
        # Determine market_date (US Eastern) for evaluate_signal call
        eastern_now = datetime.now(_EASTERN)
        market_date = eastern_now.date()
        now = datetime.now(timezone.utc)

        # Get portfolio for risk evaluation
        portfolio = get_portfolio(session)

        query = session.query(Signal)

        # Build query based on whether signal_ids is provided
        if body.signal_ids:
            # Query exact signal IDs without source_run/status filters
            try:
                signal_uuids = [uuid_module.UUID(sid) for sid in body.signal_ids]
                query = query.filter(Signal.id.in_(signal_uuids))
            except (ValueError, AttributeError):
                # If any UUID is invalid, return empty result
                return ReviewDecisionPreviewResponse(
                    execution_mode="DECISION_PREVIEW_ONLY",
                    signals_evaluated=0,
                    decision_previews_generated=0,
                    skipped_count=0,
                    decision_previews=[],
                    skipped=[],
                    trade_decisions_created=0,
                    orders_created=0,
                )
        else:
            # Filter by source_run_prefix in SQL
            query = query.filter(Signal.source_run.startswith(body.source_run_prefix))
            # Optionally filter by status in SQL
            if body.received_only:
                query = query.filter(Signal.status == "RECEIVED")

        # Apply limit
        rows = query.limit(body.limit).all()

        # Process each signal
        for signal in rows:
            evaluated += 1
            signal_id_str = str(signal.id)

            # If signal_ids was provided, validate source_run and status in Python
            if body.signal_ids:
                # Check source_run starts with allowed prefix
                if not signal.source_run.startswith(body.source_run_prefix):
                    skipped_list.append(SkippedSignalDetail(
                        signal_id=signal_id_str,
                        ticker=signal.ticker,
                        reason="Signal source_run is not review-created",
                    ))
                    continue

                # Check status if received_only=true
                if body.received_only and signal.status != "RECEIVED":
                    skipped_list.append(SkippedSignalDetail(
                        signal_id=signal_id_str,
                        ticker=signal.ticker,
                        reason=f"Signal status is {signal.status}, not RECEIVED",
                    ))
                    continue

            # Fetch latest price for the ticker (same as real decision workflow)
            snapshot_price = _latest_price(session, signal.ticker)

            # Call evaluate_signal with signal data (pure function, no DB writes)
            rd = evaluate_signal(
                session,
                portfolio=portfolio,
                direction=signal.direction,
                ticker=signal.ticker,
                confidence=signal.confidence,
                snapshot_price=snapshot_price,
                market_date=market_date,
                now=now,
            )

            # Create decision preview item with full traceability
            decision_preview = DecisionPreviewItem(
                signal_id=signal_id_str,
                ticker=signal.ticker,
                side=signal.direction,
                confidence=str(signal.confidence),
                source_run=signal.source_run,
                signal_status=signal.status,
                preview_decision=rd.decision,
                reason_code=rd.reason_code,
                requested_notional=str(rd.requested_notional),
                approved_notional=str(rd.approved_notional),
                requested_qty=str(rd.requested_qty),
                approved_qty=str(rd.approved_qty),
                risk_snapshot=rd.risk_snapshot,
                sizing_adjustments=rd.sizing_adjustments,
                reason=f"Decision preview only: would create {rd.decision} decision from Signal.",
            )
            decision_previews.append(decision_preview)
            generated += 1

    return ReviewDecisionPreviewResponse(
        execution_mode="DECISION_PREVIEW_ONLY",
        signals_evaluated=evaluated,
        decision_previews_generated=generated,
        skipped_count=len(skipped_list),
        decision_previews=decision_previews,
        skipped=skipped_list,
        trade_decisions_created=0,
        orders_created=0,
    )


@app.post(
    "/v1/review/create-decisions",
    response_model=ReviewCreateDecisionsResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def create_decisions_from_signals(
    body: ReviewCreateDecisionsRequest,
) -> ReviewCreateDecisionsResponse:
    """
    Create TradeDecision rows from review-created Signal rows.

    This endpoint creates real TradeDecision rows but does NOT create Orders.
    It is a pure decision-creation workflow: Signal → TradeDecision, no Order.

    Signal.status is updated to DECISION_MADE for successfully processed signals.
    Skipped signals (duplicates, validation failures) have their status unchanged.

    Filtering rules:
    - If signal_ids is NOT provided: query by source_run_prefix in SQL, optionally filter by status
    - If signal_ids IS provided: query those exact IDs, validate source_run and status in Python

    Duplicate protection:
    - Before creating a TradeDecision, check if one already exists for that Signal.id
    - If it exists: skip, count in skipped_existing_count, do not mutate Signal.status
    - If it doesn't exist: create the TradeDecision and update Signal.status to DECISION_MADE

    JobRun creation:
    - Created only if at least one new TradeDecision will be inserted
    - workflow_type = REVIEW_QUEUE_CREATE_DECISIONS
    - status = COMPLETED
    - result_summary includes counts

    confirm_create_decisions validation:
    - If false or missing, return HTTP 422
    """
    import uuid as uuid_module

    # Validate confirm_create_decisions
    if not body.confirm_create_decisions:
        raise HTTPException(
            status_code=422,
            detail="confirm_create_decisions must be true to create TradeDecisions",
        )

    evaluated = 0
    created = 0
    skipped_existing = 0
    skipped_list = []
    created_decisions_list = []
    decisions_to_create = []

    with get_session() as session:
        # Determine market_date (US Eastern) for evaluate_signal call
        eastern_now = datetime.now(_EASTERN)
        market_date = eastern_now.date()
        now = datetime.now(timezone.utc)

        # Get portfolio for risk evaluation
        portfolio = get_portfolio(session)

        query = session.query(Signal)

        # Build query based on whether signal_ids is provided
        if body.signal_ids:
            # Query exact signal IDs without source_run/status filters
            try:
                signal_uuids = [uuid_module.UUID(sid) for sid in body.signal_ids]
                query = query.filter(Signal.id.in_(signal_uuids))
            except (ValueError, AttributeError):
                # If any UUID is invalid, return empty result
                return ReviewCreateDecisionsResponse(
                    execution_mode="DECISION_CREATION_ONLY",
                    signals_evaluated=0,
                    trade_decisions_created=0,
                    skipped_count=0,
                    skipped_existing_count=0,
                    created_decisions=[],
                    skipped=[],
                    orders_created=0,
                )
        else:
            # Filter by source_run_prefix in SQL
            query = query.filter(Signal.source_run.startswith(body.source_run_prefix))
            # Optionally filter by status in SQL
            if body.received_only:
                query = query.filter(Signal.status == "RECEIVED")

        # Apply limit
        rows = query.limit(body.limit).all()

        # First pass: validate and collect signals to process
        for signal in rows:
            evaluated += 1
            signal_id_str = str(signal.id)

            # If signal_ids was provided, validate source_run and status in Python
            if body.signal_ids:
                # Check source_run starts with allowed prefix
                if not signal.source_run.startswith(body.source_run_prefix):
                    skipped_list.append(SkippedSignalDetail(
                        signal_id=signal_id_str,
                        ticker=signal.ticker,
                        reason="Signal source_run is not review-created",
                    ))
                    continue

                # Check status if received_only=true
                if body.received_only and signal.status != "RECEIVED":
                    skipped_list.append(SkippedSignalDetail(
                        signal_id=signal_id_str,
                        ticker=signal.ticker,
                        reason=f"Signal status is {signal.status}, not RECEIVED",
                    ))
                    continue

            # Check if TradeDecision already exists for this signal
            existing_decision = session.query(TradeDecision).filter(
                TradeDecision.signal_id == signal.id
            ).first()

            if existing_decision:
                skipped_existing += 1
                skipped_list.append(SkippedSignalDetail(
                    signal_id=signal_id_str,
                    ticker=signal.ticker,
                    reason="TradeDecision already exists for signal",
                ))
                continue

            # This signal will create a new TradeDecision; collect it
            decisions_to_create.append(signal)

        # Only create JobRun if we have new decisions to create
        job_run = None
        if decisions_to_create:
            job_run = JobRun(
                idempotency_key=body.idempotency_key,
                workflow_type="REVIEW_QUEUE_CREATE_DECISIONS",
                market_date=market_date,
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
                result_summary={},
            )
            session.add(job_run)
            session.flush()

            # Second pass: evaluate and create TradeDecisions
            for signal in decisions_to_create:
                # Fetch latest price for the ticker (same as real decision workflow)
                snapshot_price = _latest_price(session, signal.ticker)

                # Call evaluate_signal with signal data (pure function, no DB writes)
                rd = evaluate_signal(
                    session,
                    portfolio=portfolio,
                    direction=signal.direction,
                    ticker=signal.ticker,
                    confidence=signal.confidence,
                    snapshot_price=snapshot_price,
                    market_date=market_date,
                    now=now,
                )

                # Create TradeDecision row
                trade_decision = TradeDecision(
                    signal_id=signal.id,
                    job_run_id=job_run.id,
                    ticker=signal.ticker,
                    signal_direction=signal.direction,
                    decision=rd.decision,
                    reason_code=rd.reason_code,
                    requested_notional=rd.requested_notional if rd.requested_notional > Decimal("0") else None,
                    approved_notional=rd.approved_notional if rd.approved_notional > Decimal("0") else None,
                    requested_qty=rd.requested_qty if rd.requested_qty > Decimal("0") else None,
                    approved_qty=rd.approved_qty if rd.approved_qty > Decimal("0") else None,
                    risk_snapshot=rd.risk_snapshot,
                    sizing_adjustments=rd.sizing_adjustments,
                    decided_at=datetime.now(timezone.utc),
                    market_date=market_date,
                )
                session.add(trade_decision)
                session.flush()

                # Update Signal.status to DECISION_MADE after successful creation
                signal.status = "DECISION_MADE"
                session.add(signal)
                session.flush()

                # Create response detail
                created_decisions_list.append(CreatedDecisionDetail(
                    signal_id=str(signal.id),
                    trade_decision_id=str(trade_decision.id),
                    ticker=signal.ticker,
                    side=signal.direction,
                    decision=rd.decision,
                    reason_code=rd.reason_code,
                    requested_notional=str(rd.requested_notional),
                    approved_notional=str(rd.approved_notional),
                    requested_qty=str(rd.requested_qty),
                    approved_qty=str(rd.approved_qty),
                    job_run_id=str(job_run.id),
                ))
                created += 1

            # Update JobRun result_summary with counts
            job_run.result_summary = {
                "signals_evaluated": evaluated,
                "trade_decisions_created": created,
                "skipped_count": len(skipped_list),
                "skipped_existing_count": skipped_existing,
                "orders_created": 0,
            }
            session.add(job_run)
            session.flush()

    # Count existing Order rows to prove none were created
    with get_session() as session:
        order_count = session.query(Order).count()

    return ReviewCreateDecisionsResponse(
        execution_mode="DECISION_CREATION_ONLY",
        signals_evaluated=evaluated,
        trade_decisions_created=created,
        skipped_count=len(skipped_list),
        skipped_existing_count=skipped_existing,
        created_decisions=created_decisions_list,
        skipped=skipped_list,
        orders_created=0,
    )


@app.post(
    "/v1/review/order-preview",
    response_model=ReviewOrderPreviewResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def preview_orders_from_decisions(
    body: ReviewOrderPreviewRequest,
) -> ReviewOrderPreviewResponse:
    """
    Preview which TradeDecision rows would become Orders, without creating any rows.

    This endpoint is PREVIEW ONLY: no Order rows created, no JobRun rows created,
    no TradeDecision or Signal rows modified. Safe to call repeatedly.

    Filtering rules:
    - If trade_decision_ids is NOT provided: query by source_run_prefix in SQL
    - If trade_decision_ids IS provided: query those exact IDs, validate source_run in Python

    Duplicate protection:
    - Before previewing a TradeDecision, check if an Order already exists for that
      TradeDecision.trade_decision_id. If it exists: skip with reason "Order already exists".
    - If it doesn't exist: preview it with reason "Preview only: this TradeDecision would create an order."

    approved_only parameter:
    - If true: only preview TradeDecisions with decision=BUY or SELL AND approved_qty > 0
    - If false: evaluate all decisions but skip non-approved ones with clear reasons
    """
    import uuid as uuid_module

    evaluated = 0
    previewed = 0
    skipped_existing = 0
    skipped_list = []
    preview_list = []

    with get_session() as session:
        query = session.query(TradeDecision).join(Signal, Signal.id == TradeDecision.signal_id)

        # Build query based on whether trade_decision_ids is provided
        if body.trade_decision_ids:
            # Query exact trade decision IDs without source_run filter
            try:
                decision_uuids = [uuid_module.UUID(did) for did in body.trade_decision_ids]
                query = session.query(TradeDecision).filter(TradeDecision.id.in_(decision_uuids))
            except (ValueError, AttributeError):
                # If any UUID is invalid, return empty result
                return ReviewOrderPreviewResponse(
                    execution_mode="ORDER_PREVIEW_ONLY",
                    trade_decisions_evaluated=0,
                    order_previews_generated=0,
                    skipped_count=0,
                    skipped_existing_count=0,
                    order_previews=[],
                    skipped=[],
                    orders_created=0,
                    job_runs_created=0,
                )
        else:
            # Filter by source_run_prefix in SQL
            query = query.filter(Signal.source_run.startswith(body.source_run_prefix))

        # Apply limit
        rows = query.limit(body.limit).all()

        # Evaluate each trade decision
        for td in rows:
            evaluated += 1
            decision_id_str = str(td.id)
            signal_id_str = str(td.signal_id)

            # Fetch the signal for source_run validation (required when trade_decision_ids provided)
            signal = session.query(Signal).filter(Signal.id == td.signal_id).first()
            if not signal:
                skipped_list.append(SkippedTradeDecisionDetail(
                    trade_decision_id=decision_id_str,
                    ticker=td.ticker,
                    reason="Signal not found",
                ))
                continue

            # If trade_decision_ids was provided, validate source_run in Python
            if body.trade_decision_ids:
                if not signal.source_run.startswith(body.source_run_prefix):
                    skipped_list.append(SkippedTradeDecisionDetail(
                        trade_decision_id=decision_id_str,
                        ticker=td.ticker,
                        reason="TradeDecision source signal is not review-created",
                    ))
                    continue

            # Check if an Order already exists for this TradeDecision
            existing_order = session.query(Order).filter(
                Order.trade_decision_id == td.id
            ).first()

            if existing_order:
                skipped_existing += 1
                skipped_list.append(SkippedTradeDecisionDetail(
                    trade_decision_id=decision_id_str,
                    ticker=td.ticker,
                    reason="Order already exists for TradeDecision",
                ))
                continue

            # Check if this TradeDecision would create an order
            # Only BUY and SELL decisions with approved_qty > 0 can create orders
            if td.decision not in ("BUY", "SELL"):
                if body.approved_only:
                    # Skip silently when approved_only=true and decision is not BUY/SELL
                    skipped_list.append(SkippedTradeDecisionDetail(
                        trade_decision_id=decision_id_str,
                        ticker=td.ticker,
                        reason="TradeDecision is not approved for order creation",
                    ))
                    continue
                else:
                    # Evaluate but skip with reason when approved_only=false
                    skipped_list.append(SkippedTradeDecisionDetail(
                        trade_decision_id=decision_id_str,
                        ticker=td.ticker,
                        reason=f"TradeDecision decision is {td.decision}, not BUY/SELL",
                    ))
                    continue

            # Check if approved_qty is > 0
            if not td.approved_qty or td.approved_qty <= Decimal("0"):
                reason = "TradeDecision is not approved for order creation" if body.approved_only else "TradeDecision has zero approved_qty"
                skipped_list.append(SkippedTradeDecisionDetail(
                    trade_decision_id=decision_id_str,
                    ticker=td.ticker,
                    reason=reason,
                ))
                continue

            # This TradeDecision would create an order preview
            preview = OrderPreviewItem(
                trade_decision_id=decision_id_str,
                signal_id=signal_id_str,
                ticker=td.ticker,
                side=td.signal_direction,
                order_type="MARKET",
                status="PREVIEW_ONLY",
                qty=str(td.approved_qty),
                notional=str(td.approved_notional) if td.approved_notional else "0.00",
                decision=td.decision,
                reason_code=td.reason_code,
                source_run=signal.source_run,
                reason="Preview only: this TradeDecision would create an order.",
            )
            preview_list.append(preview)
            previewed += 1

    # Count existing Order rows to prove none were created
    with get_session() as session:
        order_count = session.query(Order).count()
        job_run_count = session.query(JobRun).count()

    return ReviewOrderPreviewResponse(
        execution_mode="ORDER_PREVIEW_ONLY",
        trade_decisions_evaluated=evaluated,
        order_previews_generated=previewed,
        skipped_count=len(skipped_list),
        skipped_existing_count=skipped_existing,
        order_previews=preview_list,
        skipped=skipped_list,
        orders_created=0,
        job_runs_created=0,
    )


@app.get(
    "/v1/review/workflow-status",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def get_workflow_status() -> WorkflowStatusResponse:
    """
    Get live workflow status with counts and step evaluations.

    This endpoint is READ-ONLY: no database writes, no JobRun creation.
    Returns counts of:
    - Review candidates by status (NEW, WATCHING, REJECTED, APPROVED_FOR_SIGNAL)
    - Review-created signals by status (RECEIVED, DECISION_MADE, ERROR)
    - Review-created trade decisions by decision type and order eligibility
    - Orders (total and review-created)
    - Workflow step statuses with reasons for blocked steps
    """
    import uuid as uuid_module

    REVIEW_SOURCE_PREFIX = "review_queue_create_signals_v1:"

    with get_session() as session:
        # Count review candidates by status
        candidate_total = session.query(CandidateReview).count()
        candidate_new = session.query(CandidateReview).filter(
            CandidateReview.review_status == "NEW"
        ).count()
        candidate_watching = session.query(CandidateReview).filter(
            CandidateReview.review_status == "WATCHING"
        ).count()
        candidate_approved = session.query(CandidateReview).filter(
            CandidateReview.review_status == "APPROVED_FOR_SIGNAL"
        ).count()
        candidate_rejected = session.query(CandidateReview).filter(
            CandidateReview.review_status == "REJECTED"
        ).count()

        candidates_counts = ReviewCandidatesCounts(
            total=candidate_total,
            new=candidate_new,
            watching=candidate_watching,
            approved_for_signal=candidate_approved,
            rejected=candidate_rejected,
        )

        # Count review-created signals by status
        signal_total = session.query(Signal).filter(
            Signal.source_run.startswith(REVIEW_SOURCE_PREFIX)
        ).count()
        signal_received = session.query(Signal).filter(
            Signal.source_run.startswith(REVIEW_SOURCE_PREFIX),
            Signal.status == "RECEIVED",
        ).count()
        signal_decision_made = session.query(Signal).filter(
            Signal.source_run.startswith(REVIEW_SOURCE_PREFIX),
            Signal.status == "DECISION_MADE",
        ).count()
        signal_error = session.query(Signal).filter(
            Signal.source_run.startswith(REVIEW_SOURCE_PREFIX),
            Signal.status == "ERROR",
        ).count()

        signals_counts = ReviewCreatedSignalsCounts(
            total=signal_total,
            received=signal_received,
            decision_made=signal_decision_made,
            error=signal_error,
        )

        # Count review-created trade decisions
        # Join Signal → TradeDecision, filter by source_run prefix
        decision_query = session.query(TradeDecision).join(
            Signal, Signal.id == TradeDecision.signal_id
        ).filter(Signal.source_run.startswith(REVIEW_SOURCE_PREFIX))

        decision_total = decision_query.count()
        decision_buy = decision_query.filter(
            TradeDecision.decision == "BUY"
        ).count()
        decision_sell = decision_query.filter(
            TradeDecision.decision == "SELL"
        ).count()
        decision_rejected = decision_query.filter(
            TradeDecision.decision == "REJECTED"
        ).count()

        # Count order-eligible decisions (BUY/SELL with approved_qty > 0 and no existing order)
        order_eligible = 0
        already_has_order = 0
        eligible_decisions = decision_query.filter(
            TradeDecision.decision.in_(["BUY", "SELL"]),
            TradeDecision.approved_qty > Decimal("0"),
        ).all()
        for td in eligible_decisions:
            existing_order = session.query(Order).filter(
                Order.trade_decision_id == td.id
            ).first()
            if existing_order:
                already_has_order += 1
            else:
                order_eligible += 1

        decisions_counts = ReviewCreatedDecisionsCounts(
            total=decision_total,
            buy=decision_buy,
            sell=decision_sell,
            rejected=decision_rejected,
            order_eligible=order_eligible,
            already_has_order=already_has_order,
        )

        # Count all orders and review-created orders
        order_total = session.query(Order).count()
        review_created_orders = session.query(Order).join(
            TradeDecision, TradeDecision.id == Order.trade_decision_id
        ).join(Signal, Signal.id == TradeDecision.signal_id).filter(
            Signal.source_run.startswith(REVIEW_SOURCE_PREFIX)
        ).count()

        orders_counts = OrdersCounts(
            total=order_total,
            review_created=review_created_orders,
        )

        # Evaluate workflow steps
        steps = []

        # Step 1: Prediction Preview (always ready)
        steps.append(WorkflowStepStatus(
            step="Prediction Preview",
            status="READY",
            reason="Run prediction preview to fetch model recommendations.",
        ))

        # Step 2: Save Candidates (blocked if no CONSIDER candidates)
        if candidate_approved == 0:
            # Count CONSIDER candidates from candidate_reviews
            consider_count = session.query(CandidateReview).filter(
                CandidateReview.preview_decision == "CONSIDER"
            ).count()
            if consider_count == 0:
                steps.append(WorkflowStepStatus(
                    step="Save Candidates",
                    status="BLOCKED",
                    reason="No CONSIDER candidates available from prediction preview.",
                ))
            else:
                steps.append(WorkflowStepStatus(
                    step="Save Candidates",
                    status="READY",
                    reason=f"{consider_count} CONSIDER candidate(s) available to save.",
                ))
        else:
            steps.append(WorkflowStepStatus(
                step="Save Candidates",
                status="COMPLETE",
                reason=f"{candidate_approved} candidate(s) approved for signal creation.",
            ))

        # Step 3: Review Queue (depends on approved candidates)
        if candidate_approved == 0:
            steps.append(WorkflowStepStatus(
                step="Review Queue",
                status="BLOCKED",
                reason="No candidates approved for signal creation.",
            ))
        else:
            steps.append(WorkflowStepStatus(
                step="Review Queue",
                status="READY",
                reason=f"Review {candidate_approved} approved candidate(s).",
            ))

        # Step 4: Signal Preview (blocked if no signals to preview)
        if signal_total == 0:
            steps.append(WorkflowStepStatus(
                step="Signal Preview",
                status="BLOCKED",
                reason="No review-created signals yet.",
            ))
        else:
            steps.append(WorkflowStepStatus(
                step="Signal Preview",
                status="READY",
                reason=f"{signal_received} RECEIVED signal(s) available to preview.",
            ))

        # Step 5: Create Signals (blocked if no RECEIVED signals)
        if signal_received == 0:
            steps.append(WorkflowStepStatus(
                step="Create Signals",
                status="BLOCKED",
                reason="No RECEIVED signals ready for decision creation.",
            ))
        else:
            steps.append(WorkflowStepStatus(
                step="Create Signals",
                status="READY",
                reason=f"Create decisions for {signal_received} RECEIVED signal(s).",
            ))

        # Step 6: Decision Preview (blocked if no decisions to preview)
        if decision_total == 0:
            steps.append(WorkflowStepStatus(
                step="Decision Preview",
                status="BLOCKED",
                reason="No review-created trade decisions yet.",
            ))
        else:
            steps.append(WorkflowStepStatus(
                step="Decision Preview",
                status="READY",
                reason=f"{decision_total} trade decision(s) ready to preview.",
            ))

        # Step 7: Create Decisions (skipped if no BUY/SELL decisions available)
        if decision_buy + decision_sell == 0:
            steps.append(WorkflowStepStatus(
                step="Create Decisions",
                status="BLOCKED",
                reason="No approved trade decisions (BUY/SELL) available.",
            ))
        else:
            steps.append(WorkflowStepStatus(
                step="Create Decisions",
                status="COMPLETE",
                reason=f"{decision_buy + decision_sell} approved decision(s) available.",
            ))

        # Step 8: Order Preview (blocked if no order-eligible decisions)
        if order_eligible == 0:
            steps.append(WorkflowStepStatus(
                step="Order Preview",
                status="BLOCKED",
                reason="No review-created trade decisions eligible for orders.",
            ))
        else:
            steps.append(WorkflowStepStatus(
                step="Order Preview",
                status="READY",
                reason=f"{order_eligible} trade decision(s) eligible for order preview.",
            ))

        # Step 9: Create Orders (disabled)
        steps.append(WorkflowStepStatus(
            step="Create Orders",
            status="DISABLED",
            reason="Not yet implemented.",
        ))

    return WorkflowStatusResponse(
        review_candidates=candidates_counts,
        review_created_signals=signals_counts,
        review_created_trade_decisions=decisions_counts,
        orders=orders_counts,
        workflow_steps=steps,
        safety={"create_orders_enabled": False, "automation_enabled": False},
    )


@app.post(
    "/v1/review/rotation-preview",
    response_model=RotationPreviewResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def preview_portfolio_rotation(
    body: RotationPreviewRequest,
) -> RotationPreviewResponse:
    """
    Preview possible portfolio rotations when max positions are reached.

    This endpoint is PREVIEW ONLY. No Signal, TradeDecision, Order, Position,
    JobRun, or any other database rows are created or mutated.

    Rotation logic:
    1. Read open positions and latest price snapshots.
    2. Compute unrealized P&L for each position.
    3. Block positions from rotation if: price is missing/stale, or
       block_loss_realization=True and unrealized_pnl_pct < min_exit_pnl_pct.
    4. If portfolio has capacity, returns CAPACITY_AVAILABLE_NO_ROTATION_REQUIRED.
    5. If at max positions, ranks sellable positions by weakness (ascending pnl_pct)
       and approved BUY candidates by opportunity (prediction_confidence * expected_return_pct).
    6. Proposes rotation pairs where improvement_score >= min_improvement_score.

    improvement_score = candidate_score - holding_unrealized_pnl_pct
    (where candidate_score = prediction_confidence * expected_return_pct)
    """
    import uuid as uuid_module
    from datetime import timedelta

    _SAFETY = "Preview only. No signals, decisions, or orders created."
    _DOLLARS = Decimal("0.01")

    with get_session() as session:
        portfolio = get_portfolio(session)
        cfg_max = int((portfolio.config or {}).get("max_positions", get_settings().max_positions))

        open_positions = list(session.execute(select(Position)).scalars().all())
        held_tickers = {p.ticker for p in open_positions}
        current_count = len(open_positions)
        capacity_available = current_count < cfg_max

        now = datetime.now(timezone.utc)
        stale_cutoff = now - timedelta(days=body.max_price_age_days)

        positions_with_pnl: list[tuple[WeakestPositionDetail, float]] = []
        blocked_positions: list[WeakestPositionDetail] = []

        for pos in open_positions:
            row = session.execute(
                select(PriceSnapshot.price, PriceSnapshot.snapshot_ts)
                .where(PriceSnapshot.ticker == pos.ticker)
                .order_by(PriceSnapshot.snapshot_ts.desc())
                .limit(1)
            ).first()

            if row is None:
                blocked_positions.append(WeakestPositionDetail(
                    ticker=pos.ticker,
                    qty=str(pos.qty),
                    avg_cost=str(pos.avg_cost),
                    cost_basis=str(pos.cost_basis),
                    latest_price=None,
                    current_value=None,
                    unrealized_pnl=None,
                    unrealized_pnl_pct=None,
                    sellable_for_rotation=False,
                    blocked_reason="MISSING_PRICE",
                ))
                continue

            snap_price, snap_ts = row
            snap_price = Decimal(str(snap_price))
            if snap_ts.tzinfo is None:
                snap_ts = snap_ts.replace(tzinfo=timezone.utc)

            if snap_ts < stale_cutoff:
                blocked_positions.append(WeakestPositionDetail(
                    ticker=pos.ticker,
                    qty=str(pos.qty),
                    avg_cost=str(pos.avg_cost),
                    cost_basis=str(pos.cost_basis),
                    latest_price=str(snap_price),
                    current_value=None,
                    unrealized_pnl=None,
                    unrealized_pnl_pct=None,
                    sellable_for_rotation=False,
                    blocked_reason="STALE_PRICE",
                ))
                continue

            current_value = (pos.qty * snap_price).quantize(_DOLLARS)
            unrealized_pnl = (current_value - pos.cost_basis).quantize(_DOLLARS)
            cost_basis_f = float(pos.cost_basis)
            pnl_pct = float(unrealized_pnl) / cost_basis_f * 100.0 if cost_basis_f != 0.0 else 0.0

            sellable = True
            blocked_reason = None
            if body.block_loss_realization and pnl_pct < body.min_exit_pnl_pct:
                sellable = False
                blocked_reason = "LOSS_REALIZATION_BLOCKED"

            detail = WeakestPositionDetail(
                ticker=pos.ticker,
                qty=str(pos.qty),
                avg_cost=str(pos.avg_cost),
                cost_basis=str(pos.cost_basis),
                latest_price=str(snap_price),
                current_value=str(current_value),
                unrealized_pnl=str(unrealized_pnl),
                unrealized_pnl_pct=f"{pnl_pct:.4f}",
                sellable_for_rotation=sellable,
                blocked_reason=blocked_reason,
            )

            if sellable:
                positions_with_pnl.append((detail, pnl_pct))
            else:
                blocked_positions.append(detail)

        # Sort sellable positions: weakest (lowest pnl_pct) first
        positions_with_pnl.sort(key=lambda x: x[1])
        weakest_positions = [x[0] for x in positions_with_pnl]

        # Load candidates
        if body.candidate_review_ids:
            try:
                uuids = [uuid_module.UUID(cid) for cid in body.candidate_review_ids]
                raw_candidates = list(
                    session.query(CandidateReview).filter(CandidateReview.id.in_(uuids)).all()
                )
            except (ValueError, AttributeError):
                raw_candidates = []
        else:
            q = session.query(CandidateReview)
            if body.approved_only:
                q = q.filter(CandidateReview.review_status == "APPROVED_FOR_SIGNAL")
            raw_candidates = list(q.all())

        candidates_considered = len(raw_candidates)

        # Score BUY candidates not already held
        scored_candidates: list[tuple[CandidateStrengthDetail, float]] = []
        for cand in raw_candidates:
            if not cand.prediction_recommendation:
                continue
            if cand.prediction_recommendation.upper() != "BUY":
                continue
            if cand.ticker in held_tickers:
                continue
            try:
                conf = float(cand.prediction_confidence or "0")
                exp_ret = float(cand.expected_return_pct or "0")
            except (ValueError, TypeError):
                continue
            cand_score = conf * exp_ret
            scored_candidates.append((
                CandidateStrengthDetail(
                    candidate_review_id=str(cand.id),
                    ticker=cand.ticker,
                    decision=cand.preview_decision,
                    recommendation=cand.prediction_recommendation,
                    preview_score=cand.preview_score,
                    prediction_confidence=cand.prediction_confidence,
                    expected_return_pct=cand.expected_return_pct,
                    candidate_score=f"{cand_score:.4f}",
                ),
                cand_score,
            ))

        # Sort candidates: strongest (highest score) first
        scored_candidates.sort(key=lambda x: x[1], reverse=True)
        strongest_candidates = [x[0] for x in scored_candidates]

        rotation_pairs: list[RotationPairDetail] = []
        rejected_pairs: list[RotationPairDetail] = []

        if capacity_available:
            explanation = (
                f"Portfolio has capacity ({current_count}/{cfg_max} positions used). "
                "Rotation is not required; new positions can be opened without selling existing ones."
            )
        elif not weakest_positions:
            n_blocked = len(blocked_positions)
            explanation = (
                f"At max positions ({current_count}/{cfg_max}). "
                f"All {n_blocked} position(s) are blocked from rotation "
                "(loss realization protection or missing/stale price)."
            ) if n_blocked else (
                f"At max positions ({current_count}/{cfg_max}). No positions available for rotation."
            )
        elif not scored_candidates:
            explanation = (
                f"At max positions ({current_count}/{cfg_max}). "
                "No approved BUY candidates available for rotation."
            )
        else:
            used_cand_tickers: set[str] = set()
            pairs_limit = min(body.limit_pairs, len(positions_with_pnl), len(scored_candidates))

            for pos_detail, pos_pnl_pct in positions_with_pnl[:pairs_limit]:
                best: tuple[CandidateStrengthDetail, float] | None = None
                for cd, cs in scored_candidates:
                    if cd.ticker not in used_cand_tickers:
                        best = (cd, cs)
                        break
                if best is None:
                    break

                best_detail, best_score = best
                improvement = best_score - pos_pnl_pct
                meets = improvement >= body.min_improvement_score

                pair = RotationPairDetail(
                    sell_ticker=pos_detail.ticker,
                    buy_ticker=best_detail.ticker,
                    sell_unrealized_pnl_pct=pos_detail.unrealized_pnl_pct or "0.0000",
                    sell_unrealized_pnl=pos_detail.unrealized_pnl or "0.00",
                    buy_candidate_score=best_detail.candidate_score,
                    improvement_score=f"{improvement:.4f}",
                    meets_threshold=meets,
                    reason=(
                        f"Candidate {best_detail.ticker} (score {best_score:.4f}) vs "
                        f"holding {pos_detail.ticker} (unrealized_pnl_pct {pos_pnl_pct:.4f}%); "
                        f"improvement {improvement:.4f} "
                        f"{'meets' if meets else 'below'} threshold {body.min_improvement_score}."
                    ),
                    safety_note=_SAFETY,
                )

                if meets:
                    rotation_pairs.append(pair)
                    used_cand_tickers.add(best_detail.ticker)
                else:
                    rejected_pairs.append(pair)

            if rotation_pairs:
                explanation = (
                    f"At max positions ({current_count}/{cfg_max}). "
                    f"{len(rotation_pairs)} rotation pair(s) proposed. "
                    f"{len(rejected_pairs)} pair(s) below improvement threshold."
                )
            else:
                explanation = (
                    f"At max positions ({current_count}/{cfg_max}). "
                    f"No rotation pairs meet the minimum improvement threshold of {body.min_improvement_score}."
                )

    return RotationPreviewResponse(
        current_position_count=current_count,
        max_positions=cfg_max,
        capacity_available=capacity_available,
        rotation_required=not capacity_available,
        block_loss_realization=body.block_loss_realization,
        min_exit_pnl_pct=str(body.min_exit_pnl_pct),
        min_improvement_score=str(body.min_improvement_score),
        candidates_considered=candidates_considered,
        positions_considered=current_count,
        weakest_positions=weakest_positions,
        blocked_positions=blocked_positions,
        strongest_candidates=strongest_candidates,
        rotation_pairs=rotation_pairs,
        rejected_pairs=rejected_pairs,
        explanation=explanation,
        safety_counts=RotationSafetyCounts(
            signals_created=0,
            decisions_created=0,
            orders_created=0,
            db_rows_created=0,
        ),
    )


@app.post(
    "/v1/review/daily-plan-preview",
    response_model=DailyPlanPreviewResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def daily_plan_preview(
    body: DailyPlanPreviewRequest,
) -> DailyPlanPreviewResponse:
    """
    Generate a read-only consolidated daily trading plan.

    PREVIEW ONLY. No Signal, TradeDecision, Order, Position, JobRun, or any
    other database rows are created or mutated.

    Logic:
    1. Read portfolio state and open positions.
    2. Compute unrealized P&L per holding using latest price snapshots.
    3. Positions with a SELL signal in the review queue and PnL >= 0 → sell_recommendations.
       Positions with PnL < 0 and block_loss_realization=True → blocked from SELL.
       All others → hold_positions.
    4. BUY candidates from review queue are evaluated via evaluate_signal().
       Approved → buy_recommendations; rejected → blocked_actions.
    5. If at max positions and BUYs are blocked by MAX_POSITIONS_REACHED,
       build rotation_plan pairing profitable holdings with strong BUY candidates.
    6. Derive recommended_next_action from results.
    """
    from datetime import timedelta

    _DOLLARS = Decimal("0.01")

    with get_session() as session:
        # 1. Portfolio & position state
        portfolio = get_portfolio(session)
        cfg_max = int((portfolio.config or {}).get("max_positions", get_settings().max_positions))
        open_positions = list(session.execute(select(Position)).scalars().all())
        if body.position_tickers is not None:
            _pos_filter = {t.strip().upper() for t in body.position_tickers}
            open_positions = [p for p in open_positions if p.ticker.upper() in _pos_filter]
        held_tickers = {p.ticker for p in open_positions}
        current_count = len(open_positions)
        available_slots = max(0, cfg_max - current_count)
        capacity_status = "HAS_CAPACITY" if current_count < cfg_max else "AT_CAPACITY"

        now = datetime.now(timezone.utc)
        eastern_date = now.astimezone(_EASTERN).date()
        stale_cutoff = now - timedelta(days=body.max_price_age_days)

        # 2. Load candidates
        import uuid as _uuid_mod
        cand_q = session.query(CandidateReview)
        if body.approved_only:
            cand_q = cand_q.filter(CandidateReview.review_status == "APPROVED_FOR_SIGNAL")
        if body.candidate_ids is not None:
            if not body.candidate_ids:
                raw_candidates = []
            else:
                _id_list = [_uuid_mod.UUID(cid) for cid in body.candidate_ids]
                cand_q = cand_q.filter(CandidateReview.id.in_(_id_list))
                raw_candidates = list(cand_q.all())
        else:
            raw_candidates = list(cand_q.limit(body.limit_candidates).all())
        no_candidates = len(raw_candidates) == 0

        sell_signal_tickers: set[str] = {
            c.ticker for c in raw_candidates
            if c.prediction_recommendation
            and c.prediction_recommendation.upper() == "SELL"
            and c.ticker in held_tickers
        }

        # 3. Analyze current holdings
        blocked_actions: list[DailyPlanBlockedItem] = []
        hold_positions: list[HoldPositionItem] = []
        sell_recommendations: list[SellRecommendationItem] = []
        profitable_positions: list[tuple] = []  # (ticker, pnl_pct, snap_price, pos)

        for pos in open_positions:
            price_row = session.execute(
                select(PriceSnapshot.price, PriceSnapshot.snapshot_ts)
                .where(PriceSnapshot.ticker == pos.ticker)
                .order_by(PriceSnapshot.snapshot_ts.desc())
                .limit(1)
            ).first()

            if price_row is None:
                if pos.ticker in sell_signal_tickers:
                    blocked_actions.append(DailyPlanBlockedItem(
                        ticker=pos.ticker, action="SELL",
                        blocked_reason="MISSING_PRICE",
                        explanation="No price snapshot found. Cannot evaluate sell.",
                    ))
                hold_positions.append(HoldPositionItem(
                    ticker=pos.ticker, qty=str(pos.qty), avg_cost=str(pos.avg_cost),
                    latest_price=None, unrealized_pnl=None, unrealized_pnl_pct=None,
                    reason="No price data. Hold until price is refreshed.",
                ))
                continue

            snap_price, snap_ts = price_row
            snap_price = Decimal(str(snap_price))
            if snap_ts.tzinfo is None:
                snap_ts = snap_ts.replace(tzinfo=timezone.utc)
            stale = snap_ts < stale_cutoff

            current_value = (pos.qty * snap_price).quantize(_DOLLARS)
            unrealized_pnl = (current_value - pos.cost_basis).quantize(_DOLLARS)
            cost_basis_f = float(pos.cost_basis)
            pnl_pct = float(unrealized_pnl) / cost_basis_f * 100.0 if cost_basis_f != 0.0 else 0.0

            if stale:
                if pos.ticker in sell_signal_tickers:
                    blocked_actions.append(DailyPlanBlockedItem(
                        ticker=pos.ticker, action="SELL",
                        blocked_reason="STALE_PRICE",
                        explanation=f"Price snapshot older than {body.max_price_age_days} days.",
                    ))
                hold_positions.append(HoldPositionItem(
                    ticker=pos.ticker, qty=str(pos.qty), avg_cost=str(pos.avg_cost),
                    latest_price=str(snap_price),
                    unrealized_pnl=str(unrealized_pnl),
                    unrealized_pnl_pct=f"{pnl_pct:.4f}",
                    reason=f"Price stale (>{body.max_price_age_days}d). Hold until refreshed.",
                ))
                continue

            is_sell_ticker = pos.ticker in sell_signal_tickers

            if is_sell_ticker:
                if body.block_loss_realization and pnl_pct < 0:
                    blocked_actions.append(DailyPlanBlockedItem(
                        ticker=pos.ticker, action="SELL",
                        blocked_reason="NEGATIVE_PNL_BLOCKED",
                        explanation=f"Position is {pnl_pct:.2f}% below cost. Selling would realize a loss.",
                    ))
                    hold_positions.append(HoldPositionItem(
                        ticker=pos.ticker, qty=str(pos.qty), avg_cost=str(pos.avg_cost),
                        latest_price=str(snap_price),
                        unrealized_pnl=str(unrealized_pnl),
                        unrealized_pnl_pct=f"{pnl_pct:.4f}",
                        reason=f"Sell signal blocked: {pnl_pct:.2f}% below cost. Selling would realize a loss.",
                    ))
                else:
                    sell_cand = next(
                        (c for c in raw_candidates
                         if c.ticker == pos.ticker
                         and c.prediction_recommendation
                         and c.prediction_recommendation.upper() == "SELL"),
                        None,
                    )
                    try:
                        sell_conf = Decimal(str(sell_cand.prediction_confidence or "0.80")) if sell_cand else Decimal("0.80")
                    except (ValueError, TypeError):
                        sell_conf = Decimal("0.80")

                    rd = None
                    try:
                        rd = evaluate_signal(
                            session,
                            portfolio=portfolio,
                            direction="SELL",
                            ticker=pos.ticker,
                            confidence=sell_conf,
                            snapshot_price=snap_price,
                            market_date=eastern_date,
                            now=now,
                        )
                    except Exception as exc:
                        blocked_actions.append(DailyPlanBlockedItem(
                            ticker=pos.ticker, action="SELL",
                            blocked_reason="EVALUATION_ERROR",
                            explanation=f"Risk evaluation error: {exc}",
                        ))
                        hold_positions.append(HoldPositionItem(
                            ticker=pos.ticker, qty=str(pos.qty), avg_cost=str(pos.avg_cost),
                            latest_price=str(snap_price),
                            unrealized_pnl=str(unrealized_pnl),
                            unrealized_pnl_pct=f"{pnl_pct:.4f}",
                            reason="Sell evaluation failed. Hold.",
                        ))

                    if rd is not None and rd.decision == DecisionType.SELL:
                        sell_notional = (rd.approved_qty * snap_price).quantize(_DOLLARS)
                        sell_recommendations.append(SellRecommendationItem(
                            ticker=pos.ticker,
                            qty=str(pos.qty),
                            avg_cost=str(pos.avg_cost),
                            latest_price=str(snap_price),
                            unrealized_pnl=str(unrealized_pnl),
                            unrealized_pnl_pct=f"{pnl_pct:.4f}",
                            sell_qty=str(rd.approved_qty),
                            sell_notional=str(sell_notional),
                            reason=f"Sell {rd.approved_qty} share(s) at ~${snap_price}. Gain: {pnl_pct:.2f}%.",
                        ))
                    elif rd is not None:
                        reason_code = rd.reason_code or "REJECTED"
                        blocked_actions.append(DailyPlanBlockedItem(
                            ticker=pos.ticker, action="SELL",
                            blocked_reason=reason_code,
                            explanation=f"Sell rejected: {reason_code}.",
                        ))
                        hold_positions.append(HoldPositionItem(
                            ticker=pos.ticker, qty=str(pos.qty), avg_cost=str(pos.avg_cost),
                            latest_price=str(snap_price),
                            unrealized_pnl=str(unrealized_pnl),
                            unrealized_pnl_pct=f"{pnl_pct:.4f}",
                            reason=f"Sell signal rejected: {reason_code}.",
                        ))
                        if pnl_pct >= 0:
                            profitable_positions.append((pos.ticker, pnl_pct, snap_price, pos))
            else:
                pnl_sign = "+" if pnl_pct >= 0 else ""
                hold_positions.append(HoldPositionItem(
                    ticker=pos.ticker, qty=str(pos.qty), avg_cost=str(pos.avg_cost),
                    latest_price=str(snap_price),
                    unrealized_pnl=str(unrealized_pnl),
                    unrealized_pnl_pct=f"{pnl_pct:.4f}",
                    reason=f"HOLD ({pnl_sign}{pnl_pct:.2f}%). No sell signal in review queue.",
                ))
                if pnl_pct >= 0:
                    profitable_positions.append((pos.ticker, pnl_pct, snap_price, pos))

        # 4. Evaluate BUY candidates
        buy_recommendations: list[BuyRecommendationItem] = []
        watch_candidates: list[WatchCandidateItem] = []
        _buy_reason_map = {
            "MAX_POSITIONS_REACHED": f"Portfolio is full ({current_count}/{cfg_max}). Free capacity or use rotation.",
            "CASH_RESERVE_BREACH": "Not enough available cash after maintaining the minimum cash reserve.",
            "DAILY_EXPOSURE_LIMIT": "Daily new-exposure limit already reached for today.",
            "CONCENTRATION_LIMIT": "Would exceed max single-ticker concentration limit.",
            "MIN_ORDER_TOO_SMALL": "Order size too small (below minimum notional threshold).",
            "DUPLICATE_SIGNAL": "A pending order for this ticker already exists today.",
            "TICKER_IN_COOLDOWN": "Ticker recently sold — cooldown period has not expired.",
            "NEW_POSITIONS_DISABLED": "New position creation is disabled in portfolio settings.",
            "AVERAGING_DOWN_BLOCKED": "Averaging down is blocked by portfolio settings.",
            "STRATEGY_DISABLED": "Strategy is currently disabled in portfolio settings.",
            "TRADING_DISABLED": "Trading is currently disabled in portfolio settings.",
        }

        for cand in raw_candidates:
            rec = (cand.prediction_recommendation or "").upper()

            if rec == "SELL":
                continue  # handled in positions section

            if rec != "BUY":
                watch_candidates.append(WatchCandidateItem(
                    candidate_review_id=str(cand.id),
                    ticker=cand.ticker,
                    prediction_recommendation=cand.prediction_recommendation,
                    prediction_confidence=cand.prediction_confidence,
                    expected_return_pct=cand.expected_return_pct,
                    preview_decision=cand.preview_decision,
                    reason=f"Recommendation is '{cand.prediction_recommendation or 'unknown'}' — watching.",
                ))
                continue

            if cand.ticker in held_tickers:
                watch_candidates.append(WatchCandidateItem(
                    candidate_review_id=str(cand.id),
                    ticker=cand.ticker,
                    prediction_recommendation=cand.prediction_recommendation,
                    prediction_confidence=cand.prediction_confidence,
                    expected_return_pct=cand.expected_return_pct,
                    preview_decision=cand.preview_decision,
                    reason="Ticker already held. Not a new buy candidate.",
                ))
                continue

            try:
                conf = float(cand.prediction_confidence or "0")
            except (ValueError, TypeError):
                conf = 0.0

            if conf < body.min_confidence:
                blocked_actions.append(DailyPlanBlockedItem(
                    ticker=cand.ticker, action="BUY",
                    blocked_reason="CONFIDENCE_BELOW_THRESHOLD",
                    explanation=f"Confidence {conf:.2f} is below minimum {body.min_confidence:.2f}.",
                ))
                continue

            price_row = session.execute(
                select(PriceSnapshot.price, PriceSnapshot.snapshot_ts)
                .where(PriceSnapshot.ticker == cand.ticker)
                .order_by(PriceSnapshot.snapshot_ts.desc())
                .limit(1)
            ).first()

            snapshot_price: Decimal | None = None
            if price_row:
                sp, sts = price_row
                if sts.tzinfo is None:
                    sts = sts.replace(tzinfo=timezone.utc)
                if sts >= stale_cutoff:
                    snapshot_price = Decimal(str(sp))

            if snapshot_price is None:
                blocked_actions.append(DailyPlanBlockedItem(
                    ticker=cand.ticker, action="BUY",
                    blocked_reason="NO_PRICE_SNAPSHOT",
                    explanation="No current price snapshot. Cannot size the order.",
                ))
                continue

            try:
                rd = evaluate_signal(
                    session,
                    portfolio=portfolio,
                    direction="BUY",
                    ticker=cand.ticker,
                    confidence=Decimal(str(conf)),
                    snapshot_price=snapshot_price,
                    market_date=eastern_date,
                    now=now,
                )
            except Exception as exc:
                blocked_actions.append(DailyPlanBlockedItem(
                    ticker=cand.ticker, action="BUY",
                    blocked_reason="EVALUATION_ERROR",
                    explanation=f"Risk evaluation failed: {exc}",
                ))
                continue

            if rd.decision == DecisionType.BUY:
                try:
                    exp_ret = float(cand.expected_return_pct or "0")
                except (ValueError, TypeError):
                    exp_ret = 0.0
                cand_score = conf * exp_ret
                buy_recommendations.append(BuyRecommendationItem(
                    ticker=cand.ticker,
                    candidate_review_id=str(cand.id),
                    prediction_confidence=cand.prediction_confidence,
                    expected_return_pct=cand.expected_return_pct,
                    forecast_price_5d=cand.forecast_price_5d,
                    latest_price=str(snapshot_price),
                    candidate_score=f"{cand_score:.4f}",
                    approved_qty=str(rd.approved_qty),
                    approved_notional=str(rd.approved_notional),
                    reason=f"Buy {rd.approved_qty} share(s) at ~${snapshot_price} = ${rd.approved_notional}.",
                ))
            else:
                reason_code = rd.reason_code or "REJECTED"
                blocked_actions.append(DailyPlanBlockedItem(
                    ticker=cand.ticker, action="BUY",
                    blocked_reason=reason_code,
                    explanation=_buy_reason_map.get(reason_code, f"Rejected: {reason_code}."),
                ))

        # 5. Rotation plan (at max positions, BUYs blocked by capacity)
        rotation_plan: list[DailyPlanRotationItem] = []

        if body.include_rotation and current_count >= cfg_max:
            capacity_blocked_tickers = {
                a.ticker for a in blocked_actions
                if a.action == "BUY" and a.blocked_reason == "MAX_POSITIONS_REACHED"
            }

            if capacity_blocked_tickers and profitable_positions:
                profitable_positions.sort(key=lambda x: x[1])  # weakest first

                scored_buy_cands: list[tuple] = []
                for cand in raw_candidates:
                    if (cand.prediction_recommendation or "").upper() == "BUY" and cand.ticker in capacity_blocked_tickers:
                        try:
                            conf = float(cand.prediction_confidence or "0")
                            exp_ret = float(cand.expected_return_pct or "0")
                            score = conf * exp_ret
                        except (ValueError, TypeError):
                            score = 0.0
                        scored_buy_cands.append((cand, score))
                scored_buy_cands.sort(key=lambda x: x[1], reverse=True)

                pairs_limit = min(3, len(profitable_positions), len(scored_buy_cands))
                used_buy_tickers: set[str] = set()

                for sell_ticker, sell_pnl_pct, sell_price, sell_pos in profitable_positions[:pairs_limit]:
                    best: tuple | None = None
                    for cand, score in scored_buy_cands:
                        if cand.ticker not in used_buy_tickers:
                            best = (cand, score)
                            break
                    if best is None:
                        break

                    buy_cand, buy_score = best
                    improvement = buy_score - sell_pnl_pct
                    meets = improvement >= body.min_rotation_improvement_pct
                    sell_value = (sell_pos.qty * sell_price).quantize(_DOLLARS)
                    sell_pnl_d = (sell_value - sell_pos.cost_basis).quantize(_DOLLARS)

                    rotation_plan.append(DailyPlanRotationItem(
                        sell_ticker=sell_ticker,
                        buy_ticker=buy_cand.ticker,
                        sell_unrealized_pnl_pct=f"{sell_pnl_pct:.4f}",
                        sell_unrealized_pnl=str(sell_pnl_d),
                        buy_candidate_score=f"{buy_score:.4f}",
                        improvement_score=f"{improvement:.4f}",
                        meets_threshold=meets,
                        reason=(
                            f"Sell {sell_ticker} (+{sell_pnl_pct:.2f}%) to buy {buy_cand.ticker} "
                            f"(score {buy_score:.4f}). "
                            f"Improvement {improvement:.4f} "
                            f"({'meets' if meets else 'below'} threshold {body.min_rotation_improvement_pct})."
                        ),
                    ))
                    if meets:
                        used_buy_tickers.add(buy_cand.ticker)

        # 6. Recommended next action
        good_rotations = [r for r in rotation_plan if r.meets_threshold]
        cap_blocked_count = sum(1 for a in blocked_actions if a.action == "BUY" and a.blocked_reason == "MAX_POSITIONS_REACHED")

        if no_candidates:
            recommended_next_action = "Open Review Queue and approve candidates first. No approved candidates found."
            explanation = (
                "No candidates are approved for trading. "
                "Go to the Review Queue tab and approve candidates before running the daily plan."
            )
        elif buy_recommendations:
            slots_note = f" Portfolio has {available_slots} open slot(s)." if available_slots > 0 else ""
            recommended_next_action = (
                f"{len(buy_recommendations)} BUY candidate(s) are risk-approved in this preview.{slots_note}"
                " Review the plan below, then proceed to Signal Creation only if you agree."
            )
            explanation = (
                f"{len(buy_recommendations)} BUY candidate(s) passed the risk engine check. "
                "This preview does not create any signals or decisions. "
                "Proceed to the Signals & Decisions tab only after reviewing the plan."
            )
        elif good_rotations:
            recommended_next_action = (
                f"Execute {len(good_rotations)} rotation pair(s): sell weakest profitable holding(s) "
                "to make room for stronger candidates."
            )
            explanation = (
                f"Portfolio is at max positions ({current_count}/{cfg_max}). "
                f"{len(good_rotations)} rotation pair(s) proposed. "
                "Selling profitable holdings creates capacity for higher-scoring candidates."
            )
        elif sell_recommendations:
            recommended_next_action = f"Review {len(sell_recommendations)} SELL recommendation(s) from prediction model."
            explanation = (
                f"{len(sell_recommendations)} position(s) have SELL signals from the prediction model "
                "with no loss-realization risk. Review and approve in the Signals & Decisions tab."
            )
        elif cap_blocked_count:
            recommended_next_action = (
                f"Portfolio is full. {cap_blocked_count} BUY candidate(s) blocked by max positions. "
                "No profitable rotation meets the threshold."
            )
            explanation = (
                f"Portfolio is at max positions ({current_count}/{cfg_max}). "
                "No rotation pairs meet the improvement threshold. "
                "Wait for existing positions to appreciate, or lower the rotation threshold."
            )
        elif watch_candidates:
            recommended_next_action = (
                f"Review {len(watch_candidates)} WATCH candidate(s) below. "
                "No buy/sell action is recommended yet."
            )
            explanation = (
                "These candidates are in watch status. "
                "No immediate action is required. "
                "Approve or reject them in the Review Queue when ready."
            )
        else:
            recommended_next_action = "No action required. All positions are in good standing."
            explanation = (
                "No approved BUY candidates and no sell signals. "
                "Consider running a new prediction scan to find opportunities."
            )

        total_value = portfolio.cached_total_value or portfolio.cached_cash or Decimal("0")
        cash = portfolio.cached_cash or Decimal("0")
        portfolio_summary = PortfolioCapacitySummary(
            total_value=str(total_value),
            cash=str(cash),
            open_positions=current_count,
            max_positions=cfg_max,
            available_slots=available_slots,
            capacity_status=capacity_status,
        )

    return DailyPlanPreviewResponse(
        as_of=now,
        portfolio_summary=portfolio_summary,
        buy_recommendations=buy_recommendations,
        sell_recommendations=sell_recommendations,
        hold_positions=hold_positions,
        watch_candidates=watch_candidates,
        rotation_plan=rotation_plan,
        blocked_actions=blocked_actions,
        recommended_next_action=recommended_next_action,
        explanation=explanation,
        safety_counts=DailyPlanSafetyCounts(
            signals_created=0,
            trade_decisions_created=0,
            orders_created=0,
            job_runs_created=0,
            db_rows_created=0,
        ),
    )


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

_UI_DIR = pathlib.Path(__file__).parent / "ui"
app.mount("/ui", StaticFiles(directory=str(_UI_DIR), html=True), name="ui")


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse("/ui/")
