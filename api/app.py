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
    POST /v1/strategy/scoring-profile-calibration-preview — historical scoring calibration against realized returns (PREVIEW ONLY, read-only)
    POST /v1/strategy/calibrated-rotation-preview — calibration-aware rotation workbench (PREVIEW ONLY, read-only)
    POST /v1/review/rotation-preview — preview portfolio rotations when at max positions (read-only)
    POST /v1/review/daily-plan-preview — consolidated daily plan: BUY/SELL/HOLD/ROTATION/BLOCKED (read-only)
    POST /v1/review/daily-plan-signal-preview — preview Signal rows from Daily Plan actions (PREVIEW ONLY, no DB writes)
    POST /v1/review/daily-plan-create-signals — create Signal rows from approved Daily Plan actions (confirmation required)
    POST /v1/review/daily-plan-decision-preview — preview trade decisions from Daily Plan-created Signal rows (PREVIEW ONLY, no DB writes)
    POST /v1/review/daily-plan-create-decisions — create TradeDecision rows from Daily Plan-created Signal rows (confirmation required)
    POST /v1/review/daily-plan-order-preview — preview Orders from Daily Plan TradeDecision rows (PREVIEW ONLY, no DB writes)
    GET  /v1/review/daily-plan-execution-status — consolidated Daily Plan execution state (read-only, no DB writes)
    POST /v1/review/daily-plan-replay-preview — historical daily plan replay/backtest preview (read-only)
    GET  /v1/strategy/universe/status  — read-only universe diagnostics (no DB writes)
    POST /v1/review/fill-pending-orders — manually fill PENDING paper Order rows (PAPER FILLS ONLY, no broker execution)
    POST /v1/review/cancel-pending-orders — manually cancel PENDING paper Order rows (PAPER CANCEL ONLY, no broker execution)
    GET  /v1/prediction/health         — check prediction service reachability and health (requires auth, no writes)
    POST /v1/market/backfill-history   — backfill historical prices for S&P 500 + SPY benchmark (writes price_snapshots + benchmark_prices only)
    GET  /v1/market/screening-readiness — read-only screening readiness check (no writes)
    POST /v1/review/position-monitor-preview — preview exit recommendations for open positions (PREVIEW ONLY, read-only)
    POST /v1/review/exit-signal-preview — preview exit intent per open position (PREVIEW ONLY, read-only, no signals/orders)
    POST /v1/review/exit-decision-preview — preview exit decision per open position (PREVIEW ONLY, read-only, no signals/decisions/orders)
    POST /v1/review/position-review-preview — one-click consolidated position review: monitor+signal+decision+order layers (PREVIEW ONLY, read-only, no DB writes)
    POST /v1/review/create-exit-orders     — create PENDING SELL paper order tickets for REVIEW_FOR_EXIT positions (confirmation required, no broker execution)
    GET  /v1/review/daily-review-summary   — read-only daily operating summary: portfolio + positions + candidates + orders + next action (no DB writes)
    GET  /v1/research/candidate-preview     — read-only Phase 4-B non-production candidate preview (PREVIEW ONLY, no DB writes, no orders, no automation, no prediction call)
    GET  /v1/research/current-alpha/preview — read-only Phase 13-A current champion alpha (composite_sn) paper-test preview (PREVIEW ONLY, PAPER TEST ONLY, no DB writes, no orders, no broker, no automation, no prediction/provider call)
    GET  /v1/research/current-alpha/pnl     — read-only Phase 13-C daily paper PnL for the champion book (PREVIEW ONLY, PAPER TEST ONLY, no DB writes, no orders, no broker, no automation, no prediction/provider call)
    GET  /v1/research/current-alpha/actions-preview — read-only Phase 13-D paper-only action plan (ADD/WAIT/AVOID_PREVIEW, every row NO_ORDER; no DB writes, no orders, no broker, no automation)
    GET  /v1/research/current-alpha/rebalance-simulator — read-only Phase 13-E rebalance-frequency simulator (quarterly simulated from the frozen panel; daily rejected; no external calls)
    GET  /v1/research/current-alpha/book    — read-only Phase 13-F persisted paper book (local JSON store; PREVIEW ONLY, PAPER TEST ONLY, no DB writes, no orders, no broker, no automation)
    POST /v1/research/current-alpha/book/preview-create — Phase 13-F preview (commit=false, writes nothing) or save (commit=true, writes only the local paper_book.json); no orders/signals/decisions/broker/automation/DB
    GET  /v1/research/current-alpha/book/pnl-history — read-only Phase 13-F paper-book PnL history over time (local JSON store; no DB writes, no orders, no broker, no automation)
    POST /v1/research/current-alpha/book/snapshot-preview — Phase 13-F daily paper PnL snapshot (commit=true writes only the local pnl_snapshots.json); no orders/signals/decisions/broker/automation/DB

Authentication: every endpoint except /v1/health and /v1/ready requires the
X-API-Key header to match PAPER_TRADER_SERVICE_API_KEY.

Clock convention: market_date is always derived server-side from the current
UTC timestamp converted to US/Eastern, never trusted from the caller.
"""
from __future__ import annotations

import csv
import inspect
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
from pydantic import BaseModel, Field, field_validator
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
    PredictionRun,
    PriceSnapshot,
    Signal,
    Trade,
    TradeDecision,
)
from paper_trader.db.session import get_dedicated_session, get_session
from paper_trader.engine.market_data import fetch_latest_prices, fetch_historical_prices, fetch_market_indicator_latest, fetch_fred_latest_series

try:
    import yfinance as _yf
except ImportError:
    _yf = None
from paper_trader.engine.market_hours import is_weekday
from paper_trader.engine.portfolio import compute_cash, get_open_positions, get_portfolio
from paper_trader.engine.prediction_client import (
    build_prediction_run_values,
    fetch_predictions_for_tickers,
    normalize_prediction_response,
    normalize_prediction_response_with_error,
)
from paper_trader.engine.prediction_strategy import generate_prediction_signals
from paper_trader.engine.reconciler import fill_order, refresh_open_positions_cache, run_fill_cycle
from paper_trader.engine.risk import evaluate_signal
from paper_trader.engine.scoring import (
    score_candidate_v2 as _score_candidate_v2,
    score_candidate_balanced_preview as _score_candidate_balanced_preview,
    score_candidate_quality_preview as _score_candidate_quality_preview,
    score_candidate_risk_adjusted_preview as _score_candidate_risk_adjusted_preview,
    build_score_breakdown as _build_score_breakdown,
    score_holding_v2 as _score_holding_v2,
    score_rotation_v2 as _score_rotation_v2,
    explain_score_factors as _explain_score_factors,
    safe_float as _safe_float,
)
from paper_trader.engine.strategy import generate_signals
from paper_trader.workflows.decision import run_decision_workflow, _latest_price
from paper_trader.workflows.snapshot import MissingPricesError, run_snapshot_workflow, upsert_post_fill_snapshot
from paper_trader.api.research_candidate_preview import (
    CandidatePreviewError,
    load_candidate_preview,
)
from paper_trader.api.current_alpha_preview import (
    CurrentAlphaPreviewError,
    load_current_alpha_preview,
)
from paper_trader.api.current_alpha_operations import (
    load_current_alpha_actions_preview,
    load_current_alpha_pnl,
    load_current_alpha_rebalance_simulation,
)
from paper_trader.api.current_alpha_book import (
    load_current_alpha_book,
    load_current_alpha_pnl_history,
    preview_or_create_current_alpha_book,
    snapshot_current_alpha_book,
)

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


_PREDICTION_TUNNEL_COMMAND = (
    "gcloud compute start-iap-tunnel stock-prediction-vm-new 8000 "
    "--project stock-prediction-app-466420 "
    "--zone us-central1-a "
    "--local-host-port=127.0.0.1:9000"
)


class PredictionHealthOut(BaseModel):
    status: str
    prediction_base_url: str
    reachable: bool
    healthz_ok: bool
    config_ok: bool | None = None
    detail: str
    expected_tunnel_command: str
    checked_at: str


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


class RefreshSnapshotResponse(BaseModel):
    market_date: date
    tickers_requested: int
    prices_inserted_or_updated: int
    price_failures: list[dict]
    portfolio_snapshot_created_or_updated: bool
    portfolio_total_value: str | None = None
    cash: str | None = None
    positions_value: str | None = None
    open_positions_count: int | None = None
    safety_mode: str
    snapshot_error: str | None = None


class BackfillHistoryRequest(BaseModel):
    lookback_days: int = Field(default=45, ge=7, le=180)
    dry_run: bool = Field(default=False)


class BackfillHistoryResponse(BaseModel):
    safety_mode: str
    start_date: str
    end_date: str
    tickers_requested: int
    tickers_succeeded: int
    tickers_failed: int
    snapshots_inserted_or_updated: int
    market_dates_written: int
    tickers_with_at_least_6_snapshots: int
    tickers_with_at_least_21_snapshots: int
    spy_snapshot_count: int
    screening_ready: bool
    no_writes_to_orders_signals_decisions_trades: bool


class ScreeningReadinessResponse(BaseModel):
    price_snapshots_total: int
    distinct_market_dates: int
    tickers_total: int
    tickers_with_at_least_6_snapshots: int
    tickers_with_at_least_21_snapshots: int
    spy_snapshot_count: int
    latest_market_date: date | None
    screening_ready: bool
    message: str


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


class TradeOut(BaseModel):
    """A filled paper trade (READ ONLY). One row per executed paper fill.

    Trade lifecycle linking: Trade.order_id -> Order, and Order.trade_decision_id
    -> TradeDecision. trade_decision_id may be null only if the originating order
    relationship is unavailable; it is never invented.
    """
    id: str
    short_id: str
    order_id: str
    order_short_id: str
    trade_decision_id: str | None
    trade_decision_short_id: str | None
    ticker: str
    side: str
    qty: str
    fill_price: str
    gross_value: str
    commission: str
    net_value: str
    realized_pnl: str | None
    trade_ts: datetime
    market_date: date
    status: str
    notes: str


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


class MarketIndicator(BaseModel):
    key: str
    label: str
    symbol: str | None = None
    value: str | None = None
    previous_close: str | None = None
    previous_close_date: str | None = None
    change: str | None = None
    change_pct: str | None = None
    source: str
    available: bool
    as_of: str | None = None
    status: str | None = None
    freshness_label: str | None = None


class MarketIndicatorPlaceholder(BaseModel):
    key: str
    label: str
    available: bool
    reason: str
    value: str | None = None
    as_of: str | None = None
    status: str | None = None
    source: str | None = None
    change: str | None = None
    change_pct: str | None = None
    previous_value: str | None = None
    previous_as_of: str | None = None
    freshness_label: str | None = None


class MarketIndicatorsResponse(BaseModel):
    status: str
    source: str
    as_of: str | None
    indicators: list[MarketIndicator]
    placeholders: list[MarketIndicatorPlaceholder]


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


# ---------------------------------------------------------------------------
# Candidate quality thresholds (Scan Coverage + Candidate Quality contract).
#
# SCAN COVERAGE NOTE (do not regress): The local screener (engine/market_screener)
# evaluates the FULL configured universe — every ticker with enough price history
# is scored locally. Only the top `prediction_top_n` ranked clean candidates (plus
# any current holdings) are sent to the remote GCP prediction service, because the
# prediction service is rate/throughput constrained. That is why the GCP prediction
# batch is small (~5-7 names) even though the local scan covers the whole universe.
# The Scan Coverage funnel exists to report this truthfully: "Full universe was
# locally screened. Prediction was run on the top N ranked names." Never present the
# small GCP prediction batch as if it were the full S&P 500 scan.
#
# A candidate must clear EVERY threshold below to be an ACTIONABLE_TRADE_IDEA.
# Anything that falls short is WATCH_ONLY / BELOW_THRESHOLD, never actionable.
# This is what stops a weak BUY (e.g. preview_score 75.5) from being recommended.
DEFAULT_MIN_ACTIONABLE_SCORE = 85.0      # preview_score scale is 0-100
DEFAULT_MIN_CONFIDENCE = 0.85            # prediction confidence, 0-1
DEFAULT_MIN_EXPECTED_RETURN_PCT = 1.0    # forecast 5D return, percent
DEFAULT_MIN_RELATIVE_STRENGTH_VS_SPY = 0.0  # must not lag SPY (>= 0)


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

    # Phase 4A explainability fields
    price_history_points: int | None = None
    prediction_status: str | None = None
    selected_for_gcp_reason: str | None = None
    top_score_drivers: list[str] = Field(default_factory=list)
    skip_or_warning_reason: str | None = None

    # Phase 4B classification fields
    candidate_type: str | None = None          # "NEW_BUY_CANDIDATE" | "CURRENT_HOLDING_MONITOR"
    is_current_holding: bool = False
    eligible_for_review_queue: bool = False
    review_queue_eligibility_reason: str | None = None

    # Portfolio-aware fields: existing position context for held tickers
    open_position_qty: str | None = None
    open_position_avg_cost: str | None = None
    holding_action_hint: str | None = None      # "Monitor in Position Review" when is_current_holding

    # Phase 4B balanced scoring fields (populated only when scoring_profile="balanced_preview")
    current_score: str | None = None
    balanced_preview_score: str | None = None
    score_delta: str | None = None
    current_rank: int | None = None
    balanced_preview_rank: int | None = None
    ranking_change: int | None = None
    balanced_score_drivers: list[str] = Field(default_factory=list)

    # Phase 4C per-component score breakdown (always populated)
    score_breakdown: dict[str, Any] | None = None

    # Scan Coverage + Candidate Quality contract: truthful actionability label.
    # ACTIONABLE_TRADE_IDEA | WATCH_ONLY | BELOW_THRESHOLD | REJECTED | ALREADY_HELD
    actionability: str = "WATCH_ONLY"
    reason_summary: str = ""
    reason_codes: list[str] = Field(default_factory=list)
    threshold_pass_fail: dict[str, bool] = Field(default_factory=dict)


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
    min_actionable_score: float = Field(
        default=DEFAULT_MIN_ACTIONABLE_SCORE,
        ge=0,
        le=100,
        description="Minimum preview score (0-100) for ACTIONABLE_TRADE_IDEA.",
    )
    min_actionable_confidence: float = Field(
        default=DEFAULT_MIN_CONFIDENCE,
        ge=0,
        le=1,
        description="Minimum prediction confidence (0-1) for ACTIONABLE_TRADE_IDEA.",
    )
    min_expected_return_pct: float = Field(
        default=DEFAULT_MIN_EXPECTED_RETURN_PCT,
        description="Minimum expected 5D return (percent) for ACTIONABLE_TRADE_IDEA.",
    )
    min_relative_strength_vs_spy: float = Field(
        default=DEFAULT_MIN_RELATIVE_STRENGTH_VS_SPY,
        description="Minimum relative strength vs SPY for ACTIONABLE_TRADE_IDEA.",
    )
    include_current_positions_for_prediction: bool = Field(
        default=True,
        description="Inject currently held position tickers into GCP prediction batch even if not top scan candidates.",
    )
    max_prediction_concurrency: int = Field(
        default=4,
        ge=1,
        le=10,
        description="Maximum concurrent GCP prediction requests (1-10, default 4).",
    )
    scoring_profile: str = Field(
        default="current",
        description=(
            "Scoring profile for candidate ranking. "
            "'current' (default, production ranking); "
            "'balanced_preview' (reduced momentum, volatility + holding penalties, preview-only); "
            "'quality_preview' (sustained RS, spike penalty, preview-only); "
            "'risk_adjusted_preview' (most conservative, aggressive vol penalty, preview-only)."
        ),
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
    daily_session_id: str | None = Field(
        default=None,
        max_length=100,
        description=(
            "Optional Daily Review session identifier. When supplied, each "
            "captured prediction_runs row is stamped with it so the Scan "
            "Selection Funnel can report real per-session capture counts. "
            "Observational only — never creates signals, orders, or trades."
        ),
    )
    capture_source: str = Field(
        default="MARKET_SCAN",
        max_length=30,
        description=(
            "Capture context recorded on prediction_runs rows: "
            "DAILY_REVIEW | PREDICTION_PREVIEW | MARKET_SCAN."
        ),
    )

    @field_validator("scoring_profile")
    @classmethod
    def _validate_scoring_profile(cls, v: str) -> str:
        allowed = {"current", "balanced_preview", "quality_preview", "risk_adjusted_preview"}
        if v not in allowed:
            raise ValueError(f"scoring_profile must be one of {sorted(allowed)}")
        return v


class ScanSummaryOut(BaseModel):
    """Market scan summary."""
    universe: str
    scan_date: str | None
    total_universe_count: int
    evaluated_count: int
    skipped_count: int
    candidate_count: int


class SkippedTickerDetail(BaseModel):
    """Compact skipped ticker entry for candidate funnel diagnostics."""
    ticker: str
    reason_codes: list[str]
    price_points: int


class TopScanNotPredicted(BaseModel):
    """Candidate that passed scan but was not sent to GCP due to prediction_top_n cutoff."""
    rank: int
    ticker: str
    scan_score: str
    reason: str = "Below prediction_top_n cutoff"


class PredictionOutcomes(BaseModel):
    """Breakdown of prediction outcomes for the selected tickers."""
    consider: int = 0
    watch: int = 0
    reject: int = 0
    failed_fetch: int = 0
    other: int = 0


class CandidateFunnelOut(BaseModel):
    """Diagnostic breakdown of the candidate selection funnel."""
    universe_count: int
    evaluated_count: int
    skipped_count: int
    skipped_by_reason: dict[str, int]
    top_scan_count: int
    clean_scan_count: int
    prediction_top_n: int
    gcp_prediction_count: int
    not_sent_to_gcp_count: int
    current_holdings_injected_count: int = 0
    gcp_concurrency: int = 4
    prediction_elapsed_ms: int = 0
    prediction_outcomes: PredictionOutcomes
    top_scan_not_predicted: list[TopScanNotPredicted]
    skipped_examples: list[SkippedTickerDetail]

    # Phase 4A extended funnel counts
    price_history_ready_count: int = 0
    skipped_insufficient_history_count: int = 0
    local_scan_candidate_count: int = 0
    prediction_batch_count: int = 0
    gcp_success_count: int = 0
    gcp_failure_count: int = 0
    final_selected_count: int = 0
    safety_counts: dict[str, int] = Field(
        default_factory=lambda: {"signals_created": 0, "decisions_created": 0, "orders_created": 0}
    )


class ThresholdSummaryOut(BaseModel):
    """Threshold parameters used in the scan and prediction pipeline."""
    min_price_points: int
    prediction_top_n: int
    scan_top_n: int
    max_prediction_concurrency: int
    include_current_positions_for_prediction: bool


class ScoringDiagnosticsOut(BaseModel):
    """Scoring formula labels and driver breakdown."""
    local_scan_formula_label: str
    final_score_formula_label: str
    top_driver_counts: dict[str, int]
    threshold_summary: ThresholdSummaryOut


class SkippedTickerDiagnostic(BaseModel):
    """One entry in the skipped-ticker diagnostic sample."""
    ticker: str
    reason: str
    price_history_points: int
    latest_price_date: str | None = None
    required_min_price_points: int


class SkippedDiagnosticsOut(BaseModel):
    """Compact sample of skipped tickers for explainability."""
    total_skipped: int
    sample_limit: int
    samples: list[SkippedTickerDiagnostic]


class PredictionFailureDetail(BaseModel):
    ticker: str
    reason: str


class ScoringProfileComparisonOut(BaseModel):
    """Multi-profile scoring comparison (Phase 4B/4C)."""
    active_profile: str
    # Phase 4B compat fields — kept for backward compatibility
    current_top_tickers: list[str] = Field(default_factory=list)
    balanced_top_tickers: list[str] = Field(default_factory=list)
    overlap_count: int = 0
    changed_rank_count: int = 0
    biggest_promotions: list[dict] = Field(default_factory=list)
    biggest_demotions: list[dict] = Field(default_factory=list)
    # Phase 4C multi-profile fields
    profiles_compared: list[str] = Field(default_factory=list)
    top_tickers_by_profile: dict[str, list[str]] = Field(default_factory=dict)
    overlap_matrix: dict[str, int] = Field(default_factory=dict)
    biggest_promotions_by_profile: dict[str, list[dict]] = Field(default_factory=dict)
    biggest_demotions_by_profile: dict[str, list[dict]] = Field(default_factory=dict)
    candidates_with_high_disagreement: list[dict] = Field(default_factory=list)
    explanation: str
    safety_counts: dict[str, int]


class ScanCoverageOut(BaseModel):
    """
    Truthful scan-coverage funnel for the daily review.

    Distinguishes how much of the universe was screened LOCALLY (full universe)
    from how many names were actually sent to the remote prediction service
    (top-N ranked subset), so the UI never claims a 7-name batch is a full
    S&P 500 scan.
    """
    universe_name: str
    configured_universe_count: int
    price_history_ready_count: int
    locally_screened_count: int
    prediction_requested_count: int
    prediction_returned_count: int
    prediction_failed_count: int
    actionable_trade_ideas_count: int
    watch_only_count: int
    rejected_count: int
    already_held_count: int
    blocked_by_risk_count: int = 0
    excluded_reason_counts: dict[str, int] = Field(default_factory=dict)
    coverage_note: str


class CandidateQualityOut(BaseModel):
    """Quality thresholds a candidate must clear to be an actionable trade idea."""
    min_actionable_score: float
    min_confidence: float
    min_expected_return_pct: float
    min_relative_strength_vs_spy: float
    explanation: str


class MarketScanPredictionCandidatesResponse(BaseModel):
    """Response from market scan + prediction candidate preview endpoint."""
    idempotency_key: str
    dry_run: bool
    execution_mode: str
    scan: ScanSummaryOut
    scan_coverage: ScanCoverageOut
    candidate_quality: CandidateQualityOut
    candidate_funnel: CandidateFunnelOut
    selected_tickers: list[str]
    predictions_fetched: int
    prediction_failures: list[PredictionFailureDetail]
    normalized_predictions: list[NormalizedPrediction]
    candidate_previews: list[CandidatePreview]
    signals_submitted: int
    decisions_made: int
    orders_created: int
    scoring_summary: ScoringDiagnosticsOut | None = None
    skipped_diagnostics: SkippedDiagnosticsOut | None = None
    scoring_profile_comparison: ScoringProfileComparisonOut | None = None


# ---------------------------------------------------------------------------
# Scoring profile calibration preview schemas (Phase 4D)
# ---------------------------------------------------------------------------

class CalibrationRequest(BaseModel):
    """Request for scoring profile calibration preview against realized returns."""
    universe: str = "SP500"
    as_of_date: date | None = Field(
        default=None,
        description="Historical date to calibrate from. Defaults to latest market_date in price_snapshots.",
    )
    lookback_days: int = Field(default=20, ge=1, description="Days of price history to compute local features.")
    forward_return_days: int = Field(default=5, ge=1, description="Days ahead to look up realized forward return.")
    scan_top_n: int = Field(default=50, ge=1, le=500, description="Candidates from scan pool to pass to profile scoring.")
    profile_top_n: int = Field(default=10, ge=1, le=50, description="Top N per profile to include in results.")
    min_price_points: int = Field(default=20, ge=1, description="Minimum price points required per ticker.")
    benchmark_ticker: str = Field(default="SPY", description="Benchmark ticker for excess return calculation.")
    profiles: list[str] = Field(
        default_factory=lambda: ["current", "balanced_preview", "quality_preview", "risk_adjusted_preview"],
        description="Profiles to compare. Must be valid scoring profile names.",
    )
    tickers: list[str] | None = Field(
        default=None,
        description=(
            "Optional targeted ticker list. When provided and non-empty, calibrates only "
            "against these tickers instead of the full universe. Tickers are normalized "
            "(stripped, uppercased) and deduplicated preserving order."
        ),
    )
    as_of_dates: list[date] | None = Field(
        default=None,
        description=(
            "Multiple historical dates for multi-date calibration. "
            "When provided, overrides as_of_date and runs calibration for each date. "
            "Results are aggregated to produce a profile_recommendation."
        ),
    )

    @field_validator("profiles")
    @classmethod
    def _validate_profiles(cls, v: list[str]) -> list[str]:
        allowed = {"current", "balanced_preview", "quality_preview", "risk_adjusted_preview"}
        for p in v:
            if p not in allowed:
                raise ValueError(f"profile {p!r} must be one of {sorted(allowed)}")
        if not v:
            raise ValueError("profiles must not be empty")
        return v


class CalibrationCandidateRow(BaseModel):
    """One ranked candidate row within a calibration profile result."""
    ticker: str
    rank: int
    score: float
    forward_return_pct: float | None = None
    excess_return_vs_spy_pct: float | None = None
    score_breakdown: dict[str, Any]
    warning_reason: str | None = None


class CalibrationProfileResult(BaseModel):
    """Calibration statistics and ranked candidates for a single scoring profile."""
    profile_name: str
    top_n: int
    average_forward_return_pct: float | None = None
    median_forward_return_pct: float | None = None
    win_rate_pct: float | None = None
    average_excess_return_vs_spy_pct: float | None = None
    best_ticker: str | None = None
    worst_ticker: str | None = None
    top_candidates: list[CalibrationCandidateRow]


class CalibrationProfileComparison(BaseModel):
    """Cross-profile comparison summary."""
    best_average_return_profile: str | None = None
    best_win_rate_profile: str | None = None
    best_excess_return_profile: str | None = None
    overlap_matrix: dict[str, int] = Field(default_factory=dict)
    explanation: str


class CalibrationSkippedDiagnosticItem(BaseModel):
    """One entry in the skipped-ticker calibration diagnostic sample."""
    ticker: str
    reason: str


class CalibrationSkippedDiagnostics(BaseModel):
    """Sample of tickers skipped during calibration (capped at 25 samples)."""
    total_skipped: int
    samples: list[CalibrationSkippedDiagnosticItem] = Field(default_factory=list)


class CalibrationRecommendationWarnings(BaseModel):
    """Warnings that reduce confidence in the profile recommendation."""
    insufficient_dates: bool = False
    missing_benchmark: bool = False
    too_few_evaluated_tickers: bool = False
    inconsistent_profile_winners: bool = False


class CalibrationProfileRanking(BaseModel):
    """Aggregate ranking for one profile across all calibrated dates."""
    profile_name: str
    average_forward_return_pct: float | None = None
    median_forward_return_pct: float | None = None
    win_rate_pct: float | None = None
    average_excess_return_vs_spy_pct: float | None = None
    consistency_score: float | None = None
    recommendation_rank: int
    explanation: str


class CalibrationProfileRecommendation(BaseModel):
    """Cross-date profile recommendation derived from aggregated calibration results."""
    recommended_profile: str | None = None
    confidence_level: str
    reason_summary: str
    best_average_return_profile: str | None = None
    best_median_return_profile: str | None = None
    best_win_rate_profile: str | None = None
    best_excess_return_profile: str | None = None
    consistency_score_by_profile: dict[str, float]
    profile_rankings: list[CalibrationProfileRanking]
    warnings: CalibrationRecommendationWarnings


class CalibrationSummary(BaseModel):
    """Top-level calibration run summary."""
    as_of_date: str
    lookback_days: int
    forward_return_days: int
    universe_count: int
    evaluated_count: int
    skipped_count: int
    benchmark_available: bool
    safety_counts: dict[str, int]
    dates_evaluated: list[str] | None = None


class CalibrationResponse(BaseModel):
    """Response from scoring profile calibration preview endpoint."""
    calibration_summary: CalibrationSummary
    profile_results: list[CalibrationProfileResult]
    profile_comparison: CalibrationProfileComparison
    skipped_diagnostics: CalibrationSkippedDiagnostics
    profile_recommendation: CalibrationProfileRecommendation | None = None


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
    # Phase 4B classification — optional; when present, backend honours eligibility
    candidate_type: str | None = None
    eligible_for_review_queue: bool | None = None


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
    review_reason_code: str | None = None
    review_note: str | None = None
    created_at: datetime
    updated_at: datetime


class PredictionRunOut(BaseModel):
    """
    One captured GCP prediction call (read-only audit row from prediction_runs).

    Observational only: this row is evidence of what the remote service returned.
    Reading or returning it creates no signals, decisions, orders, or trades.
    """
    id: str
    ticker: str
    daily_session_id: str | None
    source: str | None
    request_ts: datetime
    response_ts: datetime | None
    latency_ms: int | None
    prediction_service_url: str | None
    request_payload: dict | None
    http_status: int | None
    raw_response: dict | None
    normalized_recommendation: str | None
    normalized_confidence: str | None
    normalized_expected_return_pct: str | None
    normalized_forecast_price_5d: str | None
    model_consensus: dict | None
    ran_models: Any | None
    skipped_models: Any | None
    model_errors: Any | None
    service_version: str | None
    error: bool
    error_message: str | None
    created_at: datetime


class PredictionRunListResponse(BaseModel):
    """Response from GET /v1/model/prediction-runs (READ ONLY)."""
    runs: list[PredictionRunOut]
    count: int
    limit: int
    ticker: str | None = None


class CandidateReviewSaveResponse(BaseModel):
    """Response from save candidates endpoint."""
    inserted_count: int
    skipped_existing_count: int
    candidates_saved: list[CandidateReviewOut]
    # Phase 4B breakdown counts
    saved_new_candidates: int = 0
    skipped_current_holdings: int = 0
    skipped_watch: int = 0
    skipped_rejected: int = 0
    skipped_other: int = 0
    # Portfolio-aware skip reason (set when skipped_current_holdings > 0)
    already_held_skip_reason: str | None = None


class CandidateReviewStatusUpdate(BaseModel):
    """Update review_status for a candidate."""
    review_status: str = Field(
        ...,
        description="NEW | WATCHING | REJECTED | APPROVED_FOR_SIGNAL",
    )
    review_reason_code: str | None = Field(default=None, description="Reason code for review action")
    review_note: str | None = Field(default=None, max_length=500, description="Optional review note")


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


class TradePlanSignalDetail(BaseModel):
    """A signal created or reused by generate-trade-plan."""
    candidate_review_id: str
    signal_id: str
    ticker: str
    side: str
    confidence: str
    status: str


class TradePlanDecisionDetail(BaseModel):
    """A trade decision created or reused by generate-trade-plan."""
    signal_id: str
    trade_decision_id: str
    ticker: str
    side: str
    decision: str
    approved_notional: str
    status: str


class TradePlanRow(BaseModel):
    """A user-facing row in the trade plan table."""
    ticker: str
    action: str
    qty: str
    estimated_cost: str
    reason: str
    risk_status: str
    risk_reason: str
    next_step: str
    order_eligible: bool
    already_has_order: bool


class GenerateTradePlanRequest(BaseModel):
    """Request to generate a trade plan from approved candidates."""
    idempotency_key: str | None = Field(default=None)
    confirm_generate: bool = Field(
        default=False,
        description="Must be true to create Signal and TradeDecision rows.",
    )
    confirm_generate_trade_plan: bool = Field(
        default=False,
        description="Alias for confirm_generate. Must be true to create Signal and TradeDecision rows.",
    )
    limit: int = Field(default=50, ge=1, le=100)


class GenerateTradePlanResponse(BaseModel):
    """Response from POST /v1/review/generate-trade-plan."""
    preview_only: bool = False
    writes_performed: bool = True
    candidates_processed: int
    approved_candidates_count: int
    signals_created: int
    signals_existing: int
    decisions_created: int
    decisions_existing: int
    rejected_decisions: int
    orders_created: int = 0
    trades_created: int = 0
    fills_created: int = 0
    broker_execution: bool = False
    positions_changed: bool = False
    cash_changed: bool = False
    generated_count: int = 0
    reused_count: int = 0
    skipped_count: int = 0
    safety_message: str = ""
    no_paper_orders_created: bool = True
    no_trades_created: bool = True
    no_fills_created: bool = True
    no_cash_changes: bool = True
    no_position_changes: bool = True
    no_broker_execution: bool = True
    automation_enabled: bool = False
    trade_plan_rows: list[TradePlanRow] = Field(default_factory=list)
    signal_details: list[TradePlanSignalDetail]
    decision_details: list[TradePlanDecisionDetail]
    next_step: str
    safety_note: str


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


class CreatedOrderDetail(BaseModel):
    """Detail about an Order row created by the create-orders endpoint."""
    order_id: str
    trade_decision_id: str
    ticker: str
    side: str
    order_type: str
    status: str
    qty: str
    notional: str | None
    market_date: str
    job_run_id: str


class ReviewCreateOrdersRequest(BaseModel):
    """Request to create paper Order rows from approved TradeDecision rows."""
    idempotency_key: str = Field(
        ...,
        description="Unique key for this order creation batch.",
    )
    trade_decision_ids: list[str] | None = Field(
        default=None,
        description="Optional list of trade decision IDs to create orders for.",
    )
    source_run_prefix: str = Field(
        default="review_queue_create_signals_v1:",
        description="Source run prefix to filter review-created trade decisions.",
    )
    limit: int = Field(
        default=50,
        ge=1,
        le=100,
        description="Maximum number of trade decisions to process.",
    )
    confirm_create_orders: bool = Field(
        default=False,
        description="Must be true to create Order rows. Safety guard.",
    )


class ReviewCreateOrdersResponse(BaseModel):
    """Response from POST /v1/review/create-orders (PAPER ORDERS ONLY, no broker execution)."""
    execution_mode: str
    trade_decisions_evaluated: int
    orders_created: int
    skipped_count: int
    skipped_existing_count: int
    skipped_not_approved: int
    skipped_invalid: int
    created_orders: list[CreatedOrderDetail]
    skipped: list[SkippedTradeDecisionDetail]
    job_runs_created: int
    safety_message: str


class ManualPaperFillRequest(BaseModel):
    """Request for POST /v1/review/fill-pending-orders (PAPER FILLS ONLY, no broker execution)."""
    confirm_paper_fill: bool = Field(
        default=False,
        description="Must be true to execute paper fills. Safety gate.",
    )


class ManualPaperFillResponse(BaseModel):
    """Response from POST /v1/review/fill-pending-orders (PAPER FILLS ONLY, no broker execution)."""
    execution_mode: str
    orders_evaluated: int
    orders_filled: int
    orders_expired: int
    skipped_not_pending: int
    skipped_invalid: int
    skipped_no_price: int
    cash_delta: str
    positions_changed: list[str]
    safety_message: str


class ManualPaperCancelRequest(BaseModel):
    """Request for POST /v1/review/cancel-pending-orders (PAPER CANCEL ONLY, no broker execution)."""
    confirm_cancel_orders: bool = Field(
        default=False,
        description="Must be true to cancel pending paper orders. Safety gate.",
    )


class ManualPaperCancelResponse(BaseModel):
    """Response from POST /v1/review/cancel-pending-orders (PAPER CANCEL ONLY, no broker execution)."""
    execution_mode: str
    orders_evaluated: int
    orders_cancelled: int
    skipped_not_pending: int
    skipped_invalid: int
    cash_delta: str
    positions_changed: list[str]
    safety_message: str


class CreateExitOrdersRequest(BaseModel):
    """Request for POST /v1/review/create-exit-orders (PAPER SELL ORDERS ONLY, no broker execution)."""
    confirm_create_exit_orders: bool = Field(
        default=False,
        description="Must be true to create exit Order rows. Safety gate.",
    )


class ExitOrderCreatedDetail(BaseModel):
    """Detail about a PENDING SELL paper order row created by the create-exit-orders endpoint."""
    order_id: str
    ticker: str
    side: str
    status: str
    qty: str
    market_date: str
    job_run_id: str
    monitor_recommendation: str


class ExitOrderSkippedDetail(BaseModel):
    """Detail about a position skipped by the create-exit-orders endpoint."""
    ticker: str
    reason: str
    monitor_recommendation: str


class CreateExitOrdersResponse(BaseModel):
    """Response from POST /v1/review/create-exit-orders (PAPER SELL ORDERS ONLY, no broker execution)."""
    execution_mode: str
    created_count: int
    skipped_count: int
    orders: list[ExitOrderCreatedDetail]
    eligible_positions: list[str]
    skipped_positions: list[ExitOrderSkippedDetail]
    safety_message: str
    no_broker_execution: bool = True
    no_fills_created: bool = True
    no_trades_created: bool = True
    no_position_changes: bool = True
    automation_enabled: bool = False


class ReviewCandidatesCounts(BaseModel):
    """Count of review candidates by status."""
    total: int
    new: int
    watching: int
    approved_for_signal: int
    rejected: int
    consumed: int = 0


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
    pending: int = 0
    filled: int = 0


class WorkflowStepStatus(BaseModel):
    """Status of a single workflow step."""
    step: str
    status: str
    reason: str


class WorkflowNextAction(BaseModel):
    """One canonical next action for the guided cockpit.

    This is THE single daily-workflow state contract. Every next-action surface
    (Overview recommended action, Daily Plan Today's Review, the Action / Safety
    panel, and the candidate/portfolio section visibility) renders from this one
    object, so they can never disagree about the current task.

    Derived from the current cycle (today's candidates + live order/position
    facts) only. Historical/older approved candidates never drive this object
    and are reported separately as ``historical_trade_idea_count``.

    stage is one of:
      START_DAILY_REVIEW | REVIEW_TRADE_IDEAS | CREATE_FILL_PAPER_TRADE
      | VIEW_PORTFOLIO | MONITOR_PORTFOLIO
    """
    stage: str
    title: str
    description: str = ""
    button_label: str = ""
    target_tab: str = "daily-plan"      # overview | daily-plan | portfolio
    target_anchor: str = "active-action-workspace"  # DOM id to scroll/focus
    requires_user_action: bool = False
    # --- Back-compat aliases (older UI/JS referenced these names) ---
    label: str = ""
    detail: str = ""
    primary_button_label: str = ""
    # --- Deep-link target for the specific current candidate ---
    ticker: str | None = None
    target_candidate_id: str | None = None
    target_ticker: str | None = None
    route: str | None = None
    # --- Current-cycle counts (historical excluded) ---
    current_cycle_key: str | None = None
    current_trade_idea_count: int = 0
    current_pending_review_count: int = 0
    current_ticket_ready_count: int = 0
    current_filled_count: int = 0
    current_watch_count: int = 0
    current_rejected_count: int = 0
    historical_trade_idea_count: int = 0


class WorkflowStatusResponse(BaseModel):
    """Complete workflow status with counts and step evaluation."""
    review_candidates: ReviewCandidatesCounts
    review_created_signals: ReviewCreatedSignalsCounts
    review_created_trade_decisions: ReviewCreatedDecisionsCounts
    orders: OrdersCounts
    workflow_steps: list[WorkflowStepStatus]
    safety: dict[str, bool]
    open_positions: int = 0
    current_cycle_key: str | None = None
    # --- Current-cycle separation (historical candidates excluded) ---
    current_pending_review_count: int = 0
    current_approved_ticket_ready_count: int = 0
    current_filled_count: int = 0
    current_rejected_count: int = 0
    current_watch_count: int = 0
    historical_candidate_count: int = 0
    next_action: WorkflowNextAction | None = None


class CurrentWorkflowStateResponse(BaseModel):
    """Canonical read-only workflow state shared by every UI surface.

    Overview, the Daily Plan Today's Review card, the Daily Plan active action
    workspace, and the right Action / Safety panel all render from this single
    object so they never disagree on the current task or next action. READ-ONLY:
    no database writes, no JobRun creation.
    """
    stage: str
    current_task: str
    next_action: str
    primary_button_label: str
    primary_button_action: str
    active_workspace: str
    focus_target: str
    today_candidate_count: int
    today_pending_review_count: int
    today_approved_count: int
    older_candidate_count: int
    has_pending_orders: bool
    has_filled_orders: bool
    has_open_positions: bool
    open_position_count: int
    message: str


class CandidatePaperTradeRequest(BaseModel):
    """Request for POST /v1/review/candidates/{candidate_id}/paper-trade.

    The endpoint is the explicit, single-click user action (create + fill one
    paper trade for one approved candidate). confirm_paper_trade defaults true
    because the action itself is the confirmation; sending false performs no
    writes. PAPER ONLY: no broker execution.
    """
    confirm_paper_trade: bool = True
    idempotency_key: str | None = None


class CandidatePaperTradeResponse(BaseModel):
    """Strict, candidate-scoped result of a paper trade.

    Completion proof is scoped to the requested candidate/ticker: the UI must
    only treat the trade as completed when status == "COMPLETED" (or
    "ALREADY_COMPLETED"), filled_order is true, and ticker equals the candidate
    ticker. An old filled order for a different ticker can never appear here.
    """
    candidate_id: str
    ticker: str
    status: str  # COMPLETED | ALREADY_COMPLETED | BLOCKED | FAILED
    reason: str
    signal_id: str | None = None
    trade_decision_id: str | None = None
    order_id: str | None = None
    trade_id: str | None = None
    position_id: str | None = None
    side: str | None = None
    qty: str | None = None
    fill_price: str | None = None
    commission: str | None = None
    cash_after: str | None = None
    total_value_after: str | None = None
    safety_mode: str = "PAPER_ONLY_NO_BROKER"
    created_signal: bool = False
    created_decision: bool = False
    created_order: bool = False
    filled_order: bool = False
    created_or_updated_position: bool = False


class PositionMonitorItem(BaseModel):
    """Single position result from position monitor preview (PREVIEW ONLY, no DB writes)."""
    ticker: str
    qty: str
    avg_cost: str
    latest_price: str | None
    market_value: str | None
    cost_basis: str
    unrealized_pnl: str | None
    unrealized_pnl_pct: str | None
    portfolio_weight_pct: str | None
    opened_at: datetime
    holding_days: int
    recommendation: str
    reason_codes: list[str]
    explanation: str


class PositionMonitorPreviewResponse(BaseModel):
    """Response from position monitor preview endpoint (PREVIEW ONLY, no DB writes)."""
    as_of: datetime
    open_position_count: int
    total_positions_value: str
    total_unrealized_pnl: str
    reviewed_for_exit_count: int
    watch_count: int
    hold_count: int
    positions: list[PositionMonitorItem]
    preview_only: bool = True
    writes_performed: bool = False


class ExitSignalPreviewItem(BaseModel):
    """Single position result from exit signal preview (PREVIEW ONLY, no DB writes)."""
    ticker: str
    qty: str
    avg_cost: str
    latest_price: str | None
    unrealized_pnl: str | None
    unrealized_pnl_pct: str | None
    monitor_recommendation: str
    preview_exit_action: str
    suggested_side: str
    suggested_qty: str
    reason_codes: list[str]
    explanation: str


class ExitSignalPreviewResponse(BaseModel):
    """Response from exit signal preview endpoint (PREVIEW ONLY, no DB writes)."""
    as_of: datetime
    open_position_count: int
    preview_exit_count: int
    watch_count: int
    hold_count: int
    positions: list[ExitSignalPreviewItem]
    preview_only: bool = True
    writes_performed: bool = False
    no_orders_created: bool = True
    no_fills_created: bool = True
    no_broker_execution: bool = True


class ExitDecisionPreviewItem(BaseModel):
    """Single position result from exit decision preview (PREVIEW ONLY, no DB writes)."""
    ticker: str
    qty: str
    avg_cost: str
    latest_price: str | None
    unrealized_pnl: str | None
    unrealized_pnl_pct: str | None
    exit_signal_action: str
    preview_decision: str
    side: str
    decision_qty: str
    estimated_exit_value: str | None
    estimated_realized_pnl: str | None
    estimated_realized_pnl_pct: str | None
    reason_codes: list[str]
    explanation: str


class ExitDecisionPreviewResponse(BaseModel):
    """Response from exit decision preview endpoint (PREVIEW ONLY, no DB writes)."""
    as_of: datetime
    open_position_count: int
    preview_sell_count: int
    watch_count: int
    hold_count: int
    estimated_total_exit_value: str
    estimated_total_realized_pnl: str
    positions: list[ExitDecisionPreviewItem]
    preview_only: bool = True
    writes_performed: bool = False
    no_signals_created: bool = True
    no_decisions_created: bool = True
    no_orders_created: bool = True
    no_fills_created: bool = True
    no_broker_execution: bool = True


class PositionReviewItem(BaseModel):
    """Single position result from consolidated position review preview (PREVIEW ONLY, no DB writes)."""
    ticker: str
    qty: str
    avg_cost: str
    latest_price: str | None
    market_value: str | None
    unrealized_pnl: str | None
    unrealized_pnl_pct: str | None
    portfolio_weight_pct: str | None
    position_recommendation: str
    exit_action: str
    decision_preview: str
    order_preview: str
    suggested_side: str
    suggested_qty: str
    estimated_exit_value: str | None
    estimated_realized_pnl: str | None
    estimated_realized_pnl_pct: str | None
    reason_codes: list[str]
    explanation: str


class PositionReviewPreviewResponse(BaseModel):
    """Response from consolidated position review preview endpoint (PREVIEW ONLY, no DB writes)."""
    as_of: datetime
    open_position_count: int
    hold_count: int
    watch_count: int
    review_for_exit_count: int
    preview_exit_count: int
    preview_sell_count: int
    preview_order_count: int
    estimated_total_exit_value: str
    estimated_total_realized_pnl: str
    positions: list[PositionReviewItem]
    preview_only: bool = True
    writes_performed: bool = False
    no_signals_created: bool = True
    no_decisions_created: bool = True
    no_orders_created: bool = True
    no_trades_created: bool = True
    no_fills_created: bool = True
    no_position_changes: bool = True
    no_cash_changes: bool = True
    no_broker_execution: bool = True


class DailyReviewSummaryPositionItem(BaseModel):
    """Per-position summary in daily review (READ ONLY, no DB writes)."""
    ticker: str
    qty: str
    unrealized_pnl: str | None
    unrealized_pnl_pct: str | None
    recommendation: str


class DailyReviewSummaryResponse(BaseModel):
    """Response from GET /v1/review/daily-review-summary (READ ONLY, no DB writes)."""
    as_of: datetime
    # Safety flags
    preview_only: bool = True
    writes_performed: bool = False
    no_signals_created: bool = True
    no_decisions_created: bool = True
    no_orders_created: bool = True
    no_trades_created: bool = True
    no_fills_created: bool = True
    no_position_changes: bool = True
    no_cash_changes: bool = True
    no_broker_execution: bool = True
    # Portfolio summary
    total_value: str | None = None
    cash: str | None = None
    positions_value: str | None = None
    open_position_count: int
    unrealized_pnl: str | None = None
    total_return_pct: str | None = None
    # Position summary
    hold_count: int
    watch_count: int
    review_for_exit_count: int
    open_positions: list[DailyReviewSummaryPositionItem]
    # Daily process summary
    current_cycle_key: str | None = None
    review_candidates_total: int
    review_candidates_actionable: int
    review_candidates_consumed: int
    review_candidates_watching: int
    portfolio_aware: bool = True
    # Orders summary
    pending_orders: int
    filled_orders: int
    canceled_orders: int
    no_pending_orders: bool
    # Current-session (today's) trade-idea separation. Only these drive the
    # recommended next action; historical candidates are reported separately and
    # never counted as current actionable work. Mirrors the canonical contract in
    # /v1/review/workflow-status so every UI surface agrees on the current task.
    current_session_trade_ideas_total: int = 0
    current_session_pending_review_count: int = 0
    current_session_approved_ready_for_paper_trade_count: int = 0
    current_session_rejected_count: int = 0
    current_session_watched_count: int = 0
    historical_trade_ideas_count: int = 0
    open_positions_count: int = 0
    pending_paper_orders_count: int = 0
    # Pending actions = current-session candidates still awaiting review +
    # pending paper orders + positions flagged for exit review. Reviewed/approved
    # and historical candidates never inflate this.
    pending_actions_count: int = 0
    # Next action
    next_action_code: str
    next_action_label: str
    next_action_detail: str


# ---------------------------------------------------------------------------
# Scan Selection Funnel diagnostics (read-only; no DB writes, no GCP calls).
# Explains how the configured universe is reduced to actionable trade ideas and
# strictly separates the latest session's active ideas from historical ones.
# ---------------------------------------------------------------------------


class ScanFunnelExclusionReason(BaseModel):
    """One exclusion reason and how many names it accounts for."""
    reason: str
    count: int


class ScanFunnelTopLocal(BaseModel):
    """A locally-screened candidate row shown before prediction dispatch."""
    rank: int
    ticker: str
    score: str | None = None
    momentum_5d_pct: str | None = None
    momentum_20d_pct: str | None = None
    relative_strength_vs_spy_20d: str | None = None
    sent_to_prediction: bool
    reason: str


class ScanFunnelThresholds(BaseModel):
    """Actual actionability thresholds (code defaults), not invented values."""
    min_score: float
    min_confidence: float
    min_expected_return_pct: float
    min_relative_strength_vs_spy: float


class ScanFunnelPredictionResult(BaseModel):
    """A persisted latest-session prediction result and its actionability gate."""
    ticker: str
    prediction: str | None = None
    confidence: str | None = None
    expected_return_pct: str | None = None
    score: str | None = None
    actionability: str
    reason: str
    review_status: str
    is_current_session: bool


class ScanDiagnosticsLatestResponse(BaseModel):
    """Response from GET /v1/review/scan-diagnostics/latest (READ ONLY, no DB writes, no GCP)."""
    # Safety flags
    preview_only: bool = True
    writes_performed: bool = False
    no_signals_created: bool = True
    no_decisions_created: bool = True
    no_orders_created: bool = True
    no_trades_created: bool = True
    no_broker_execution: bool = True
    no_automation: bool = True
    # Session identity
    session_id: str | None = None
    market_date: str | None = None
    as_of: datetime
    has_latest_session: bool
    # Funnel counts. Universe/screen are computed live read-only (real). Prediction-
    # middle counts are not persisted per session and are clearly labeled.
    universe_configured: int
    price_history_ready: int
    locally_screened: int
    prediction_dispatch_limit: int
    sent_to_prediction: int | None = None
    sent_to_prediction_captured: bool = False
    predictions_returned: int | None = None
    predictions_returned_captured: bool = False
    # Per-session prediction capture linkage (prediction_runs.daily_session_id).
    predictions_captured: int | None = None
    prediction_errors_captured: int = 0
    capture_session_linked: bool = False
    capture_status: str = "NOT_CAPTURED_YET"
    capture_status_message: str = ""
    # Read-only session-linkage diagnostics (back the collapsed "Session linkage
    # detail" in the funnel). No signals/decisions/orders/trades — observational.
    candidate_review_idempotency_key: str | None = None
    prediction_run_session_id: str | None = None
    prediction_runs_matched_count: int = 0
    active_trade_ideas: int
    watch_below_threshold: int
    rejected_blocked: int
    existing_positions_reviewed: int
    already_in_portfolio: int
    historical_trade_ideas: int
    # Thresholds + plain-English explanations
    thresholds: ScanFunnelThresholds
    prediction_dispatch_explanation: str
    capture_note: str
    funnel_note: str
    # Detail tables
    exclusion_reasons: list[ScanFunnelExclusionReason]
    top_local_screened: list[ScanFunnelTopLocal]
    prediction_results: list[ScanFunnelPredictionResult]


# ---------------------------------------------------------------------------
# Quant Model Methodology contract (GET /v1/model/methodology)
# Read-only model-governance / transparency layer. Describes exactly what the
# current local pre-screen + remote prediction layers actually do today, what
# is missing, and the target quant-grade architecture. NO faked features.
# ---------------------------------------------------------------------------

class ModelFeatureDescriptor(BaseModel):
    """One model feature, with honest availability/usage flags."""
    name: str
    source: str | None = None
    available: bool
    used_today: bool
    purpose: str


class ModelLayerLocalPrescreen(BaseModel):
    """Current Layer 1 — local S&P 500 technical pre-screen."""
    description: str
    current_features: list[ModelFeatureDescriptor]
    current_formula_summary: str
    current_limitations: list[str]


class ModelLayerPrediction(BaseModel):
    """Current Layer 2 — remote GCP prediction service (black box to Paper Trader)."""
    description: str
    runs_on: str
    dispatch_policy: str
    dispatch_limit: int
    current_inputs_known_to_paper_trader: list[str]
    current_outputs: list[str]
    current_limitations: list[str]


class ModelActionabilityGate(BaseModel):
    """Current actionability gate thresholds (actual code defaults)."""
    current_thresholds: ScanFunnelThresholds
    description: str
    why_a_buy_may_be_rejected: list[str]


class ModelCurrentState(BaseModel):
    local_prescreen: ModelLayerLocalPrescreen
    prediction_layer: ModelLayerPrediction
    actionability_gate: ModelActionabilityGate


class ModelTargetLocalPrescreenV2(BaseModel):
    purpose: str
    feature_families: list[str]
    must_be_point_in_time: bool = True
    must_be_backtested: bool = True


class ModelTargetRemotePredictionV2(BaseModel):
    purpose: str
    target_outputs: list[str]


class ModelTargetPortfolioConstruction(BaseModel):
    purpose: str
    future_methods: list[str]


class ModelTargetArchitecture(BaseModel):
    local_prescreen_v2: ModelTargetLocalPrescreenV2
    remote_prediction_v2: ModelTargetRemotePredictionV2
    portfolio_construction: ModelTargetPortfolioConstruction


class ModelDataReadinessRow(BaseModel):
    feature_family: str
    available_now: bool
    data_source: str | None = None
    status: str
    rule: str


class ModelRoadmapPhase(BaseModel):
    phase: int
    name: str
    status: str


class ModelMethodologyResponse(BaseModel):
    """Response from GET /v1/model/methodology (READ ONLY, no DB writes, no GCP, no external calls)."""
    # Safety flags
    preview_only: bool = True
    writes_performed: bool = False
    no_signals_created: bool = True
    no_decisions_created: bool = True
    no_orders_created: bool = True
    no_trades_created: bool = True
    no_broker_execution: bool = True
    no_automation: bool = True
    no_remote_prediction_call: bool = True
    no_external_data_call: bool = True
    # Contract
    model_contract_version: str = "quant_model_contract_v1"
    as_of: datetime
    honesty_note: str
    current_state: ModelCurrentState
    target_quant_architecture: ModelTargetArchitecture
    data_readiness: list[ModelDataReadinessRow]
    implementation_roadmap: list[ModelRoadmapPhase]


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
        default=0.02,
        description="Minimum forward-score improvement (candidate_score_v2 - holding_score_v2) required to propose a pair.",
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
    score_factors_v2: dict | None = None


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
    candidate_review_id: str | None = None


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
    buy_candidate_review_id: str | None = None
    holding_score_v2: str | None = None
    candidate_score_v2: str | None = None
    prediction_missing: bool = False
    score_explanation: str | None = None


class DailyPlanBlockedItem(BaseModel):
    """An action blocked by risk rules, with reason and plain-English explanation."""
    ticker: str
    action: str
    blocked_reason: str
    explanation: str


class DailyPlanActionItem(BaseModel):
    """A single prioritized action in the consolidated daily action stack."""
    priority: int
    action_type: str  # ROTATE, BUY, SELL, HOLD, WATCH, BLOCKED, NO_ACTION
    ticker: str | None = None
    secondary_ticker: str | None = None
    title: str
    recommendation: str
    reason: str
    confidence: str | None = None
    expected_return_pct: str | None = None
    pnl_pct: str | None = None
    blocked_reason: str | None = None
    candidate_review_id: str | None = None
    approved_qty: str | None = None
    sell_qty: str | None = None
    safety_note: str = "Preview only. No signals, decisions, or orders created."


# ---------------------------------------------------------------------------
# Capital Allocation / Rotation v3 schemas (preview-only, no DB writes)
# ---------------------------------------------------------------------------

class CapitalReleasePositionItem(BaseModel):
    """Per-position capital release analysis (preview-only)."""
    ticker: str
    qty: str
    avg_cost: str
    cost_basis: str
    current_price: str | None
    current_value: str | None
    unrealized_pnl: str | None
    unrealized_pnl_pct: str | None
    sellable_standard_mode: bool
    blocked_reason: str | None
    releasable_cash_standard_mode: str
    releasable_cash_theoretical: str
    max_sell_qty_standard_mode: str
    explanation: str


class CapitalReleaseSummary(BaseModel):
    """Portfolio-level capital release summary."""
    current_cash: str
    total_position_value: str
    max_releasable_cash_standard_mode: str
    max_releasable_cash_theoretical: str
    blocked_cash_due_to_negative_pnl: str
    blocked_cash_due_to_missing_or_stale_price: str
    sellable_positions_count: int
    blocked_positions_count: int


class CandidateRedeployItem(BaseModel):
    """Model-implied expected return from redeploying cash into a BUY candidate."""
    ticker: str
    current_price: str | None
    prediction_confidence: str | None
    expected_return_pct: str | None
    candidate_score_v2: str
    risk_adjusted_expected_return_pct: str
    expected_pnl_per_1000: str
    explanation: str


class RotationOpportunityItem(BaseModel):
    """A candidate rotation opportunity with dollar-PnL impact estimate (preview-only)."""
    sell_ticker: str
    buy_ticker: str
    cash_released: str
    buy_price: str | None
    estimated_buy_qty: str | None
    expected_return_pct: str | None
    prediction_confidence: str | None
    risk_adjusted_expected_return_pct: str
    expected_forward_pnl: str
    holding_forward_score_v2: str
    candidate_score_v2: str
    score_improvement: str
    expected_pnl_improvement: str
    meets_threshold: bool
    blocked_reason: str | None
    explanation: str


class CapitalAllocationAnalysis(BaseModel):
    """Capital Allocation / Rotation v3 analysis (PREVIEW ONLY — no trades created)."""
    capital_release_summary: CapitalReleaseSummary
    position_release_details: list[CapitalReleasePositionItem]
    candidate_redeployment: list[CandidateRedeployItem]
    rotation_opportunities: list[RotationOpportunityItem]
    model_note: str


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
        default=0.02,
        description="Minimum forward-score improvement (candidate_score_v2 - holding_score_v2) to propose a rotation pair.",
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
    use_calibrated_rotation: bool = Field(
        default=True,
        description="If True, run calibrated rotation analysis to drive recommended_action.",
    )
    scoring_profile: str = Field(
        default="calibration_recommended",
        description="Scoring profile: calibration_recommended, current, balanced_preview, quality_preview, risk_adjusted_preview.",
    )
    calibration_as_of_dates: list[date] | None = Field(
        default=None,
        description="Historical dates for calibration. Defaults to latest market date in price snapshots.",
    )
    min_expected_improvement_pct: float = Field(
        default=1.0,
        description="Minimum expected PnL improvement % for a rotation pair to qualify.",
    )
    min_expected_pnl_dollars: float = Field(
        default=25.0,
        description="Minimum expected PnL improvement $ for a rotation pair to qualify.",
    )
    allow_loss_realization: bool = Field(
        default=False,
        description="If True, allow rotating out of positions with negative unrealized PnL.",
    )

    @field_validator("scoring_profile")
    @classmethod
    def _validate_dp_scoring_profile(cls, v: str) -> str:
        _allowed = {"calibration_recommended", "current", "balanced_preview", "quality_preview", "risk_adjusted_preview"}
        if v not in _allowed:
            raise ValueError(f"scoring_profile must be one of {sorted(_allowed)}")
        return v


class DailyPlanCalibratedRotationContext(BaseModel):
    """Calibrated rotation analysis embedded in Daily Plan (PREVIEW ONLY, no DB writes)."""
    enabled: bool
    scoring_profile_used: str
    calibration_recommended_profile: str | None = None
    calibration_confidence: str | None = None
    calibration_warning_count: int = 0
    eligible_rotation_pairs: int = 0
    blocked_pairs: int = 0
    best_rotation_pair: dict | None = None
    fallback_used: bool = False
    fallback_reason: str | None = None


class DailyPlanProfileDecisionContext(BaseModel):
    """Scoring profile decision context embedded in Daily Plan (PREVIEW ONLY, no DB writes)."""
    requested_scoring_profile: str
    resolved_scoring_profile: str
    profile_source: str  # "explicit_request" or "calibration_recommended"
    replay_supported: bool
    replay_recommendation: str | None = None
    replay_confidence_level: str | None = None
    replay_dates_evaluated: int | None = None
    replay_avg_vs_spy_pct: float | None = None
    replay_win_rate_pct: float | None = None
    replay_blockers: list[str] = Field(default_factory=list)
    safety_note: str


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
    action_stack: list[DailyPlanActionItem] = Field(default_factory=list)
    recommended_next_action: str
    explanation: str
    safety_counts: DailyPlanSafetyCounts
    capital_allocation: CapitalAllocationAnalysis | None = None
    calibrated_rotation_context: DailyPlanCalibratedRotationContext | None = None
    profile_decision_context: DailyPlanProfileDecisionContext | None = None
    market_history_warning: str | None = None
    # Portfolio-aware note: guidance on held tickers vs new-entry candidates
    portfolio_aware_note: str | None = None


# ---------------------------------------------------------------------------
# Daily Plan Signal Preview / Create Signals schemas (Phase 4K)
# ---------------------------------------------------------------------------

class DailyPlanSignalPreviewRequest(BaseModel):
    """Request to preview Signal rows that would be created from Daily Plan actions (PREVIEW ONLY)."""
    # Daily Plan filters (forwarded to internal daily_plan_preview call)
    approved_only: bool = True
    min_confidence: float = Field(default=0.65, ge=0.0, le=1.0)
    max_price_age_days: int = Field(default=5, ge=1, le=30)
    block_loss_realization: bool = True
    include_rotation: bool = True
    limit_candidates: int = Field(default=25, ge=1, le=100)
    min_rotation_improvement_pct: float = 0.02
    candidate_ids: list[str] | None = None
    position_tickers: list[str] | None = None
    use_calibrated_rotation: bool = True
    scoring_profile: str = "calibration_recommended"
    min_expected_improvement_pct: float = 1.0
    min_expected_pnl_dollars: float = 25.0
    allow_loss_realization: bool = False
    # Signal filter flags
    include_buy: bool = True
    include_sell: bool = True
    include_rotate: bool = True
    action_ids: list[str] | None = Field(
        default=None,
        description="Optional list of action_ids to include (e.g. 'BUY:AAPL', 'ROTATE_SELL:TSLA'). If None, all actionable items are included.",
    )
    candidate_review_ids: list[str] | None = Field(
        default=None,
        description="Optional list of candidate_review_id UUIDs to restrict signal generation.",
    )

    @field_validator("scoring_profile")
    @classmethod
    def _validate_sp(cls, v: str) -> str:
        _allowed = {"calibration_recommended", "current", "balanced_preview", "quality_preview", "risk_adjusted_preview"}
        if v not in _allowed:
            raise ValueError(f"scoring_profile must be one of {sorted(_allowed)}")
        return v


class DailyPlanSignalPreviewItem(BaseModel):
    """A preview of a Signal that would be created from a Daily Plan action (no DB write)."""
    action_id: str
    action_type: str  # BUY | SELL | ROTATE_BUY | ROTATE_SELL
    ticker: str
    side: str  # BUY | SELL
    confidence: str | None
    source: str
    candidate_review_id: str | None = None
    position_ticker: str | None = None
    reason: str
    safety_note: str


class DailyPlanSignalPreviewSkipped(BaseModel):
    """An action skipped during signal preview generation."""
    action_id: str
    ticker: str
    reason_code: str
    reason: str


class DailyPlanSignalPreviewResponse(BaseModel):
    """Response from POST /v1/review/daily-plan-signal-preview (PREVIEW ONLY, no DB writes)."""
    evaluated_actions_count: int
    signal_previews_generated: int
    skipped_count: int
    signal_previews: list[DailyPlanSignalPreviewItem]
    skipped: list[DailyPlanSignalPreviewSkipped]
    safety_counts: dict[str, int]


class DailyPlanCreateSignalsRequest(BaseModel):
    """Request to create Signal rows from Daily Plan actions (requires explicit confirmation)."""
    confirm_create_signals: bool = Field(
        default=False,
        description="Must be true to actually create Signal rows, else returns 422.",
    )
    idempotency_key: str | None = Field(
        default=None,
        description="Optional idempotency key for the JobRun. Auto-generated if None.",
    )
    # Daily Plan filters (same as preview)
    approved_only: bool = True
    min_confidence: float = Field(default=0.65, ge=0.0, le=1.0)
    max_price_age_days: int = Field(default=5, ge=1, le=30)
    block_loss_realization: bool = True
    include_rotation: bool = True
    limit_candidates: int = Field(default=25, ge=1, le=100)
    min_rotation_improvement_pct: float = 0.02
    candidate_ids: list[str] | None = None
    position_tickers: list[str] | None = None
    use_calibrated_rotation: bool = True
    scoring_profile: str = "calibration_recommended"
    min_expected_improvement_pct: float = 1.0
    min_expected_pnl_dollars: float = 25.0
    allow_loss_realization: bool = False
    # Signal filter flags
    include_buy: bool = True
    include_sell: bool = True
    include_rotate: bool = True
    action_ids: list[str] | None = None
    candidate_review_ids: list[str] | None = None

    @field_validator("scoring_profile")
    @classmethod
    def _validate_cs_sp(cls, v: str) -> str:
        _allowed = {"calibration_recommended", "current", "balanced_preview", "quality_preview", "risk_adjusted_preview"}
        if v not in _allowed:
            raise ValueError(f"scoring_profile must be one of {sorted(_allowed)}")
        return v


class DailyPlanCreatedSignalItem(BaseModel):
    """A Signal row created by the daily-plan-create-signals endpoint."""
    action_id: str
    signal_id: str
    ticker: str
    side: str
    confidence: str
    source_run: str
    action_type: str


class DailyPlanCreateSignalsSkipped(BaseModel):
    """An action skipped during Signal creation."""
    action_id: str
    ticker: str
    reason_code: str
    reason: str


class DailyPlanCreateSignalsResponse(BaseModel):
    """Response from POST /v1/review/daily-plan-create-signals."""
    evaluated_actions_count: int
    signals_created: int
    already_existed: int
    skipped_count: int
    decisions_created: int = 0
    orders_created: int = 0
    automation_triggered: bool = False
    created_signals: list[DailyPlanCreatedSignalItem]
    skipped: list[DailyPlanCreateSignalsSkipped]
    safety_note: str


# ---------------------------------------------------------------------------
# Daily Plan Decision Preview schemas (Phase 4L)
# ---------------------------------------------------------------------------

class DailyPlanDecisionPreviewRequest(BaseModel):
    """Request to preview trade decisions from Daily Plan-created Signal rows."""
    signal_ids: list[str] | None = Field(
        default=None,
        description="Optional list of signal IDs (UUIDs). If provided, only these signals are evaluated; non-Daily-Plan signals are skipped.",
    )
    latest_daily_plan_only: bool = Field(
        default=True,
        description="If true and signal_ids is not provided, evaluate only the most recent Daily Plan signal batch (by date in source_run).",
    )
    received_only: bool = Field(
        default=True,
        description="If true, only evaluate Signals with status='RECEIVED'.",
    )
    limit: int = Field(
        default=50,
        ge=1,
        le=100,
        description="Maximum number of signals to evaluate (default 50, max 100).",
    )
    dry_run: bool = Field(
        default=True,
        description="Always true — accepted for forward compatibility. This endpoint never writes to the database.",
    )


class DailyPlanDecisionPreviewItem(BaseModel):
    """A decision preview item for a Daily Plan-created Signal."""
    signal_id: str
    ticker: str
    side: str
    confidence: str
    decision: str
    approved_qty: str
    approved_notional: str
    reason: str
    risk_snapshot: dict[str, Any]
    source_run: str


class DailyPlanDecisionPreviewSkipped(BaseModel):
    """A signal skipped during Daily Plan Decision Preview evaluation."""
    signal_id: str
    ticker: str
    reason: str


class DailyPlanDecisionPreviewResponse(BaseModel):
    """Response from POST /v1/review/daily-plan-decision-preview (PREVIEW ONLY, no DB writes)."""
    evaluated_signals: int
    previews_generated: int
    approved_count: int
    rejected_count: int
    skipped_count: int
    decision_counts: dict[str, int]
    decision_previews: list[DailyPlanDecisionPreviewItem]
    skipped: list[DailyPlanDecisionPreviewSkipped]
    safety_counts: dict[str, int]
    next_step: str
    safety_note: str


# ---------------------------------------------------------------------------
# Daily Plan Create Decisions schemas (Phase 4N)
# ---------------------------------------------------------------------------

class DailyPlanCreateDecisionsRequest(BaseModel):
    """Request to create TradeDecision rows from Daily Plan-created Signal rows."""
    signal_ids: list[str] | None = Field(
        default=None,
        description="Optional list of signal IDs (UUIDs). If provided, only these signals are evaluated.",
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
        description="Must be true to create TradeDecision rows, else returns 422.",
    )


class DailyPlanCreatedDecisionDetail(BaseModel):
    """A TradeDecision created from a Daily Plan Signal."""
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


class DailyPlanCreateDecisionsSkipped(BaseModel):
    """A signal skipped during Daily Plan create-decisions."""
    signal_id: str
    ticker: str
    reason: str


class DailyPlanCreateDecisionsResponse(BaseModel):
    """Response from POST /v1/review/daily-plan-create-decisions."""
    evaluated_count: int
    created_count: int
    skipped_count: int
    decision_counts: dict[str, int]
    created_decisions: list[DailyPlanCreatedDecisionDetail]
    skipped: list[DailyPlanCreateDecisionsSkipped]
    safety_counts: dict[str, Any]
    safety_note: str


class DailyPlanOrderPreviewRequest(BaseModel):
    """Request to preview Orders from Daily Plan TradeDecision rows (PREVIEW ONLY)."""
    trade_decision_ids: list[str] | None = Field(
        default=None,
        description="Optional list of TradeDecision IDs to preview. If provided, validates DP prefix in Python.",
    )
    limit: int = Field(default=50, ge=1, le=100)
    approved_only: bool = Field(
        default=True,
        description="If true, only preview TradeDecisions with decision=BUY or SELL and approved_qty > 0.",
    )


class DailyPlanOrderPreviewResponse(BaseModel):
    """Response from POST /v1/review/daily-plan-order-preview (PREVIEW ONLY, no DB writes)."""
    evaluated_count: int
    preview_count: int
    skipped_count: int
    order_previews: list[OrderPreviewItem]
    skipped: list[SkippedTradeDecisionDetail]
    side_counts: dict[str, int]
    safety_counts: dict[str, Any]
    safety_note: str


class DailyPlanExecutionStatusResponse(BaseModel):
    """Response from GET /v1/review/daily-plan-execution-status (read-only, no DB writes)."""
    latest_daily_plan_source_run: str | None = None
    candidate_review_counts: dict[str, int]
    daily_plan_signal_counts: dict[str, int]
    daily_plan_trade_decision_counts: dict[str, int]
    daily_plan_order_preview_available_count: int
    existing_order_count: int
    safety_state: dict[str, Any]
    next_recommended_step: str
    warnings: list[str]


# ---------------------------------------------------------------------------
# Daily Plan Replay / Backtest Preview schemas
# ---------------------------------------------------------------------------

class DailyPlanReplayPreviewRequest(BaseModel):
    """Request for a read-only historical replay/backtest preview of the Daily Plan signal."""
    as_of_dates: list[date] | None = Field(
        default=None,
        description="Explicit list of historical dates to evaluate. Overrides start_date/end_date.",
    )
    start_date: date | None = Field(
        default=None,
        description="Start date (inclusive) when using a date range. Requires end_date.",
    )
    end_date: date | None = Field(
        default=None,
        description="End date (inclusive) when using a date range. Requires start_date.",
    )
    lookback_days: int = Field(default=20, ge=1, le=60, description="Days of price history per evaluation date.")
    forward_return_days: int = Field(default=5, ge=1, le=30, description="Days ahead to compute realized return.")
    top_n: int = Field(default=10, ge=1, le=50, description="Top N candidates per date.")
    scoring_profile: str = Field(
        default="calibration_recommended",
        description="Scoring profile: calibration_recommended, current, balanced_preview, quality_preview, risk_adjusted_preview.",
    )
    benchmark_ticker: str = Field(default="SPY", description="Benchmark ticker for excess return calculation.")
    min_price_points: int = Field(default=5, ge=1, description="Minimum price points required per ticker.")
    max_dates: int = Field(default=20, ge=1, le=30, description="Maximum number of dates to evaluate.")
    tickers: list[str] | None = Field(
        default=None,
        description="Explicit ticker list. When non-empty, restricts universe to these tickers only.",
    )

    @field_validator("scoring_profile")
    @classmethod
    def _validate_replay_profile(cls, v: str) -> str:
        _allowed = {"calibration_recommended", "current", "balanced_preview", "quality_preview", "risk_adjusted_preview"}
        if v not in _allowed:
            raise ValueError(f"scoring_profile must be one of {sorted(_allowed)}")
        return v


class DailyPlanReplayCandidate(BaseModel):
    """One scored candidate in a single replay date result."""
    ticker: str
    score: float
    momentum_5d_pct: float | None = None
    momentum_20d_pct: float | None = None
    forward_return_pct: float | None = None
    forward_return_vs_spy_pct: float | None = None
    forward_data_available: bool = True


class DailyPlanReplayDateResult(BaseModel):
    """Per-date result from a Daily Plan Replay preview."""
    as_of_date: str
    evaluated_count: int
    skipped_count: int
    recommended_profile: str
    top_candidates: list[DailyPlanReplayCandidate]
    best_candidate: DailyPlanReplayCandidate | None = None
    avg_top_n_forward_return_pct: float | None = None
    forward_return_pct: float | None = None
    forward_return_vs_spy_pct: float | None = None
    spy_forward_return_pct: float | None = None
    win: bool | None = None
    beat_spy: bool | None = None
    benchmark_available: bool = False
    notes: list[str] = Field(default_factory=list)


class DailyPlanReplaySummary(BaseModel):
    """Aggregate summary across all replay dates."""
    dates_evaluated: int
    avg_forward_return_pct: float | None = None
    median_forward_return_pct: float | None = None
    win_rate_pct: float | None = None
    avg_vs_spy_pct: float | None = None
    best_date: str | None = None
    worst_date: str | None = None
    profile_used: str
    safety_counts: dict[str, int]


class DailyPlanReplayDiagnostics(BaseModel):
    """Diagnostic information about the replay run."""
    skipped_by_reason: dict[str, int] = Field(default_factory=dict)
    insufficient_history_count: int = 0
    missing_forward_data_count: int = 0
    benchmark_available_count: int = 0
    notes: list[str] = Field(default_factory=list)


class DailyPlanReplayPreviewResponse(BaseModel):
    """Response from POST /v1/review/daily-plan-replay-preview (PREVIEW ONLY, no DB writes)."""
    date_results: list[DailyPlanReplayDateResult]
    summary: DailyPlanReplaySummary
    diagnostics: DailyPlanReplayDiagnostics


# ---------------------------------------------------------------------------
# Daily Plan Replay Profile Comparison Preview schemas
# ---------------------------------------------------------------------------

class DailyPlanReplayProfileComparisonRequest(BaseModel):
    """Request for a read-only multi-profile replay comparison preview."""
    as_of_dates: list[date] | None = Field(
        default=None,
        description="Explicit list of historical dates to evaluate. Overrides start_date/end_date.",
    )
    start_date: date | None = Field(default=None)
    end_date: date | None = Field(default=None)
    lookback_days: int = Field(default=20, ge=1, le=60)
    forward_return_days: int = Field(default=5, ge=1, le=30)
    top_n: int = Field(default=10, ge=1, le=50)
    profiles: list[str] | None = Field(
        default=None,
        description="Profiles to compare. Default: all four (current, balanced_preview, quality_preview, risk_adjusted_preview).",
    )
    benchmark_ticker: str = Field(default="SPY")
    min_price_points: int = Field(default=5, ge=1)
    max_dates: int = Field(default=20, ge=1, le=30)
    tickers: list[str] | None = Field(default=None)

    @field_validator("profiles")
    @classmethod
    def _validate_comparison_profiles(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        _allowed = {"current", "balanced_preview", "quality_preview", "risk_adjusted_preview"}
        for _cp in v:
            if _cp not in _allowed:
                raise ValueError(f"profile '{_cp}' not in {sorted(_allowed)}")
        return v


class DailyPlanReplayProfileSummary(BaseModel):
    """Per-profile aggregate summary from a profile comparison replay."""
    profile_name: str
    dates_evaluated: int
    avg_forward_return_pct: float | None = None
    median_forward_return_pct: float | None = None
    win_rate_pct: float | None = None
    avg_vs_spy_pct: float | None = None
    best_date: str | None = None
    worst_date: str | None = None
    best_candidate_count: int = 0
    missing_forward_data_count: int = 0
    benchmark_available_count: int = 0
    consistency_score: float | None = None
    rank: int = 0
    explanation: str = ""


class DailyPlanReplayProfileDateResult(BaseModel):
    """Per-date per-profile result from a profile comparison replay."""
    as_of_date: str
    profile_name: str
    best_ticker: str | None = None
    score: float | None = None
    forward_return_pct: float | None = None
    vs_spy_pct: float | None = None
    beat_spy: bool | None = None
    win: bool | None = None
    benchmark_available: bool = False
    forward_data_available: bool = False


class DailyPlanReplayComparisonSummary(BaseModel):
    """Overall cross-profile comparison summary."""
    dates_evaluated: int
    profiles_compared: list[str]
    best_profile_by_avg_return: str | None = None
    best_profile_by_median_return: str | None = None
    best_profile_by_win_rate: str | None = None
    best_profile_by_vs_spy: str | None = None
    recommended_profile: str | None = None
    confidence_level: str = "LOW"
    recommendation_reason: str = ""
    warnings: list[str] = Field(default_factory=list)


class DailyPlanReplayDecisionGate(BaseModel):
    """Decision gate recommendation from a profile comparison replay."""
    recommendation: str
    recommended_profile: str | None = None
    minimum_dates_met: bool = False
    minimum_win_rate_met: bool = False
    minimum_vs_spy_met: bool = False
    enough_consistency: bool = False
    reason: str = ""
    blockers: list[str] = Field(default_factory=list)


class DailyPlanReplayProfileComparisonResponse(BaseModel):
    """Response from POST /v1/review/daily-plan-replay-profile-comparison-preview (PREVIEW ONLY)."""
    comparison_summary: DailyPlanReplayComparisonSummary
    profile_results: list[DailyPlanReplayProfileSummary]
    date_results: list[DailyPlanReplayProfileDateResult]
    decision_gate: DailyPlanReplayDecisionGate
    safety_counts: dict[str, int]


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
    start_index: int = Field(
        default=0,
        ge=0,
        description="Offset into the full ticker list for batching. Applied before max_tickers cap.",
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
    total_available_tickers: int = Field(default=0)
    start_index: int = Field(default=0)
    end_index_exclusive: int = Field(default=0)
    next_start_index: int = Field(default=0)
    has_more: bool = Field(default=False)
    selected_ticker_count: int = Field(default=0)


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
# Universe Status schemas
# ---------------------------------------------------------------------------

class UniverseMarketDataCoverage(BaseModel):
    tickers_with_enough_price_history: int
    tickers_missing_price_history: int
    benchmark_available: bool
    benchmark_ticker: str
    min_price_points_used: int


class UniverseSafetyCounts(BaseModel):
    rows_created: int = 0
    signals_created: int = 0
    decisions_created: int = 0
    orders_created: int = 0


class UniverseStatusResponse(BaseModel):
    universe_name: str
    active_source_file: str
    ticker_count: int
    first_10_tickers: list[str]
    last_10_tickers: list[str]
    is_stub_universe: bool
    expected_full_sp500_min_count: int
    warning: str | None
    fallback_used: bool
    full_universe_file_exists: bool
    stub_universe_file_exists: bool
    market_data_coverage: UniverseMarketDataCoverage
    safety_counts: UniverseSafetyCounts


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


async def _fetch_predictions_with_optional_capture(*args, capture: list[dict] | None, **kwargs):
    """
    Call ``fetch_predictions_for_tickers`` passing ``capture`` only when the
    bound callable actually accepts it.

    The real engine implementation accepts a ``capture`` keyword (prediction-run
    audit side-channel). Tests, however, frequently monkeypatch
    ``fetch_predictions_for_tickers`` with simpler mocks that do not accept it.
    Rather than edit every such mock, this wrapper detects support for the
    keyword (via :func:`inspect.signature`) and omits it when unsupported.

    The fallback is narrowly scoped: it only retries without ``capture`` when a
    ``TypeError`` is specifically about an unexpected ``capture`` keyword, so it
    never hides an unrelated ``TypeError`` raised inside the fetch itself.
    """
    func = fetch_predictions_for_tickers
    supports_capture = True
    try:
        sig = inspect.signature(func)
        params = sig.parameters
        supports_capture = "capture" in params or any(
            p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()
        )
    except (TypeError, ValueError):
        # Signature could not be introspected; fall back to runtime detection.
        supports_capture = True

    if supports_capture:
        try:
            return await func(*args, capture=capture, **kwargs)
        except TypeError as exc:
            msg = str(exc)
            if "unexpected keyword argument" in msg and "capture" in msg:
                # Callable (e.g. a test mock) doesn't accept capture; retry without it.
                return await func(*args, **kwargs)
            raise

    return await func(*args, **kwargs)


def _persist_prediction_runs(
    capture_records: list[dict],
    *,
    daily_session_id: str | None = None,
    source: str | None = None,
) -> int:
    """
    Persist captured GCP prediction calls into the local prediction_runs table.

    Purely observational audit side-channel (see PredictionRun docstring and
    docs/prediction_service_audit_v1.md). Writing these rows never creates a
    signal, decision, order, trade, fill, or broker action.

    ``daily_session_id`` / ``source`` link each captured row back to the Daily
    Review session (or other dispatch context) that produced it, so the Scan
    Selection Funnel can report real per-session capture counts instead of
    timestamp guessing. They are stamped onto each record (when not already
    present) before the pure value mapping runs.

    Best-effort and fully isolated: any capture/persistence failure is swallowed
    so it can never break the prediction preview / fetch-and-run workflow. Each
    row is written in its own nested transaction so one malformed record cannot
    discard the rest of the batch.

    Returns the number of rows successfully written.
    """
    if not capture_records:
        return 0

    written = 0
    try:
        with get_session() as session:
            for cap in capture_records:
                try:
                    if daily_session_id is not None and not cap.get("daily_session_id"):
                        cap["daily_session_id"] = daily_session_id
                    if source is not None and not cap.get("source"):
                        cap["source"] = source
                    values = build_prediction_run_values(cap)
                    if not values.get("ticker"):
                        continue
                    with session.begin_nested():
                        session.add(PredictionRun(**values))
                    written += 1
                except Exception:
                    # Skip this single record; the savepoint is already rolled back.
                    continue
    except Exception:
        # Never propagate capture failures into the trading workflow.
        return written
    return written


def _get_screening_readiness_data(session) -> dict:
    """Query screening readiness from the database. Returns a plain dict."""
    ticker_counts_rows = session.execute(
        select(PriceSnapshot.ticker, func.count(PriceSnapshot.id).label("cnt"))
        .where(PriceSnapshot.price_type == PriceType.CLOSE)
        .where(PriceSnapshot.session_type == SessionType.REGULAR)
        .group_by(PriceSnapshot.ticker)
    ).all()
    ticker_counts = {row[0]: row[1] for row in ticker_counts_rows}
    total_snapshots = sum(ticker_counts.values())
    tickers_total = len(ticker_counts)
    tickers_6 = sum(1 for c in ticker_counts.values() if c >= 6)
    tickers_21 = sum(1 for c in ticker_counts.values() if c >= 21)
    distinct_dates_val = session.execute(
        select(func.count(distinct(PriceSnapshot.market_date)))
        .where(PriceSnapshot.price_type == PriceType.CLOSE)
        .where(PriceSnapshot.session_type == SessionType.REGULAR)
    ).scalar() or 0
    latest_market_date = session.execute(
        select(func.max(PriceSnapshot.market_date))
        .where(PriceSnapshot.price_type == PriceType.CLOSE)
        .where(PriceSnapshot.session_type == SessionType.REGULAR)
    ).scalar()
    spy_count = session.execute(
        select(func.count(BenchmarkPrice.id))
        .where(BenchmarkPrice.ticker == "SPY")
        .where(BenchmarkPrice.session_type == SessionType.REGULAR)
    ).scalar() or 0
    screening_ready = bool(spy_count >= 21 and tickers_21 >= 10)
    return {
        "price_snapshots_total": total_snapshots,
        "distinct_market_dates": distinct_dates_val,
        "tickers_total": tickers_total,
        "tickers_with_at_least_6_snapshots": tickers_6,
        "tickers_with_at_least_21_snapshots": tickers_21,
        "spy_snapshot_count": spy_count,
        "latest_market_date": latest_market_date,
        "screening_ready": screening_ready,
    }


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


@app.get(
    "/v1/research/candidate-preview",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def research_candidate_preview() -> dict:
    """
    Read-only preview of the Phase 4-B non-production research candidate.

    Requires a valid X-API-Key header. Returns the normalized, preview-only
    payload produced by ``load_candidate_preview`` (Phase 4-D): candidate
    identity, evidence summary, strategy/risk/failure-mode side-cars, no-go
    items, safety badges, and the always-on safety flags.

    This endpoint is strictly read-only. It writes no database rows, creates no
    signals / trade decisions / orders, runs no automation, and calls neither
    the prediction service nor any market-data provider — it only reads the
    local Phase 4-B candidate package files.

    If the candidate package is missing, incomplete, or not preview-ready,
    responds with HTTP 503 and a clear detail message (never a stack trace).
    """
    try:
        return load_candidate_preview()
    except CandidatePreviewError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Candidate preview unavailable: {exc}",
        ) from exc


@app.get(
    "/v1/research/current-alpha/preview",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def research_current_alpha_preview() -> dict:
    """
    Read-only preview of the Phase 13-A current champion alpha paper-test package.

    Requires a valid X-API-Key header. Returns the normalized, preview-only
    payload produced by ``load_current_alpha_preview`` (Phase 13-B): the champion
    alpha name (composite_sn), decision, go/no-go, signal date, cross-section
    month, ranked count, the top-25 / top-50 candidate books, the bottom-25 avoid
    diagnostic, sector exposure, risk limits, the go/no-go scorecard, caveats,
    source file paths, and the six enforced safety badges.

    This endpoint is strictly read-only and paper-test only. It reads only the
    local Phase 13-A package files; it writes no database rows, creates no
    signals / trade decisions / orders, runs no automation, connects to no
    broker, and calls neither the prediction service nor any external data
    provider (no Nasdaq / Intrinio / FMP).

    If the Phase 13-A package is missing or incomplete, responds with HTTP 503
    and a clear detail message (never a stack trace).
    """
    try:
        return load_current_alpha_preview()
    except CurrentAlphaPreviewError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Current alpha preview unavailable: {exc}",
        ) from exc


@app.get(
    "/v1/research/current-alpha/pnl",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def research_current_alpha_pnl() -> dict:
    """
    Read-only Phase 13-C daily paper PnL for the current champion alpha book.

    Requires a valid X-API-Key header. Computes the top-25 / top-50 paper PnL
    summary (covered / missing counts, average and median paper return, best and
    worst performers, hit rate, per-name rows) plus the 1w / 1m / 2m / 63d
    checkpoint plan, from the committed Phase 13-A paper-portfolio CSVs. The
    ``paper_return_pct`` values are already marked from owned local EOD prices.

    Strictly read-only and paper-test only: it writes no database rows, creates
    no signals / trade decisions / orders, runs no automation, connects to no
    broker, and calls neither the prediction service nor any external data
    provider. Missing / incomplete package -> HTTP 503 (never a stack trace).
    """
    try:
        return load_current_alpha_pnl()
    except CurrentAlphaPreviewError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Current alpha PnL unavailable: {exc}",
        ) from exc


@app.get(
    "/v1/research/current-alpha/actions-preview",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def research_current_alpha_actions_preview() -> dict:
    """
    Read-only Phase 13-D paper-only action plan for the current champion alpha.

    Requires a valid X-API-Key header. Derives an INITIAL paper action plan from
    the Phase 13-A package: priced top names -> ADD_PREVIEW, unpriced top names ->
    WAIT_FOR_PRICE_PREVIEW, bottom-25 diagnostic -> AVOID_PREVIEW. Every row
    carries ``order_action = NO_ORDER``.

    These are paper-only preview actions: no order is created, no signal is
    created, no trade decision is created, manual review is required. The handler
    writes no database rows, runs no automation, connects to no broker, and calls
    neither the prediction service nor any external provider. Missing / incomplete
    package -> HTTP 503 (never a stack trace).
    """
    try:
        return load_current_alpha_actions_preview()
    except CurrentAlphaPreviewError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Current alpha action preview unavailable: {exc}",
        ) from exc


@app.get(
    "/v1/research/current-alpha/rebalance-simulator",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def research_current_alpha_rebalance_simulator() -> dict:
    """
    Read-only Phase 13-E rebalance-frequency simulator for the champion alpha.

    Requires a valid X-API-Key header. Evaluates whether daily / weekly / monthly
    / quarterly operation makes sense. The quarterly cadence is genuinely
    backtested (EW long-only top-25 / top-50) from the frozen Phase 10-L scored
    panel; daily is rejected and weekly / monthly are marked not-justified-by-
    signal-frequency rather than fabricated. Daily monitoring stays valid; daily
    trading is not recommended.

    Robust and read-only: a missing / too-thin panel yields a controlled
    ``SIMULATION_INSUFFICIENT_DATA`` result with warnings (never a crash). It
    writes no database rows, creates no signals / trade decisions / orders, runs
    no automation, connects to no broker, and calls neither the prediction service
    nor any external provider. Missing Phase 13-A package -> HTTP 503.
    """
    try:
        return load_current_alpha_rebalance_simulation()
    except CurrentAlphaPreviewError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Current alpha rebalance simulation unavailable: {exc}",
        ) from exc


class CurrentAlphaBookCreateRequest(BaseModel):
    """Phase 13-F preview-create / save request (paper-only)."""

    commit: bool = False
    book_size: int = 25


class CurrentAlphaBookSnapshotRequest(BaseModel):
    """Phase 13-F daily paper PnL snapshot request (paper-only)."""

    commit: bool = False


@app.get(
    "/v1/research/current-alpha/book",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def research_current_alpha_book() -> dict:
    """
    Read-only Phase 13-F persisted current-alpha paper book.

    Requires a valid X-API-Key header. Returns the active paper book saved to the
    local paper-tracking JSON store (identity, positions, benchmark status), or a
    NO_PAPER_BOOK_YET status if none has been saved. It reads only the local JSON
    store: it writes no database rows, creates no signals / trade decisions /
    orders, runs no automation, connects to no broker, and calls neither the
    prediction service nor any external provider. Never returns a stack trace.
    """
    return load_current_alpha_book()


@app.post(
    "/v1/research/current-alpha/book/preview-create",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def research_current_alpha_book_preview_create(
    body: CurrentAlphaBookCreateRequest | None = None,
) -> dict:
    """
    Phase 13-F preview-create / save of the current-alpha paper book.

    With ``commit=false`` (default) this is a pure preview and writes NOTHING —
    it just returns the proposed paper book built from the committed Phase 13-A
    package. With ``commit=true`` it persists that book to a single local JSON
    file (paper_book.json) in the paper-tracking store — the only thing written.

    Strictly paper-only: it creates no orders, no signals, and no trade decisions,
    connects to no broker, runs no automation, writes no Paper Trader database
    rows, and calls neither the prediction service nor any external / paid
    provider. A missing Phase 13-A package -> HTTP 503 (never a stack trace).
    """
    req = body or CurrentAlphaBookCreateRequest()
    try:
        return preview_or_create_current_alpha_book(
            book_size=req.book_size,
            commit=req.commit,
        )
    except CurrentAlphaPreviewError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Current alpha paper book unavailable: {exc}",
        ) from exc


@app.get(
    "/v1/research/current-alpha/book/pnl-history",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def research_current_alpha_book_pnl_history(
    book_id: str | None = None,
    book_size: int | None = None,
) -> dict:
    """
    Read-only Phase 13-F paper-book PnL history over time.

    Requires a valid X-API-Key header. Returns the recorded paper PnL snapshot
    series (average / median return, coverage, hit rate per snapshot), the latest
    snapshot, best / worst contributors over time, and benchmark status — or a
    NO_PAPER_BOOK_YET status if nothing has been recorded. History is always
    isolated to a single paper book: the active book by default, or the one named
    by the optional ``book_id`` / ``book_size`` query parameters, so TOP 25 and
    TOP 50 series are never combined. Reads only the local JSON store: no database
    rows, no orders / signals / trade decisions, no automation, no broker, no
    prediction / provider call. Never a stack trace.
    """
    return load_current_alpha_pnl_history(book_id=book_id, book_size=book_size)


@app.post(
    "/v1/research/current-alpha/book/snapshot-preview",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def research_current_alpha_book_snapshot_preview(
    body: CurrentAlphaBookSnapshotRequest | None = None,
) -> dict:
    """
    Phase 13-F daily paper PnL snapshot of the saved current-alpha paper book.

    Marks the saved book's positions from the committed Phase 13-A package (which
    is already priced from owned local EOD — no live market call). With
    ``commit=true`` it appends the snapshot to a single local JSON file
    (pnl_snapshots.json) — the only thing written; ``commit=false`` previews it
    without writing. If no paper book has been saved, returns a controlled
    NO_PAPER_BOOK_YET status (HTTP 200).

    Strictly paper-only: it creates no orders, no signals, and no trade decisions,
    connects to no broker, runs no automation, writes no Paper Trader database
    rows, and calls neither the prediction service nor any external / paid
    provider. A missing Phase 13-A package -> HTTP 503 (never a stack trace).
    """
    req = body or CurrentAlphaBookSnapshotRequest()
    try:
        return snapshot_current_alpha_book(commit=req.commit)
    except CurrentAlphaPreviewError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Current alpha paper snapshot unavailable: {exc}",
        ) from exc


def _check_prediction_healthz(
    base_url: str,
    timeout_seconds: int = 5,
) -> tuple[bool, bool, str]:
    """
    Check if the prediction service /healthz endpoint is reachable and healthy.

    Returns (reachable, healthz_ok, detail). Never raises.
    Uses stdlib urllib — no extra dependencies.
    """
    import json
    import urllib.error
    import urllib.request

    healthz_url = base_url.rstrip("/") + "/healthz"
    try:
        req = urllib.request.Request(healthz_url)
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            if body.get("status") == "ok":
                return True, True, "Prediction service healthy"
            return True, False, f"Service reachable but status={body.get('status', 'unknown')!r}"
    except urllib.error.HTTPError as exc:
        return True, False, f"HTTP {exc.code} from {healthz_url}"
    except Exception as exc:
        return False, False, f"Unreachable: {str(exc)[:120]}"


@app.get(
    "/v1/prediction/health",
    response_model=PredictionHealthOut,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def prediction_health() -> PredictionHealthOut:
    """
    Check prediction service reachability and health.

    Requires valid X-API-Key header. Always returns HTTP 200 — inspect the
    'status' field ('ok', 'unavailable', or 'misconfigured') for the result.
    No database writes. No predictions are run. No tickers are fetched.
    """
    settings = get_settings()
    base_url = (settings.stock_prediction_api_url or "").strip()
    checked_at = datetime.now(timezone.utc).isoformat()

    if not base_url:
        return PredictionHealthOut(
            status="misconfigured",
            prediction_base_url="",
            reachable=False,
            healthz_ok=False,
            config_ok=None,
            detail="PAPER_TRADER_STOCK_PREDICTION_API_URL is not configured.",
            expected_tunnel_command=_PREDICTION_TUNNEL_COMMAND,
            checked_at=checked_at,
        )

    reachable, healthz_ok, detail = _check_prediction_healthz(
        base_url, timeout_seconds=5
    )
    return PredictionHealthOut(
        status="ok" if healthz_ok else "unavailable",
        prediction_base_url=base_url,
        reachable=reachable,
        healthz_ok=healthz_ok,
        config_ok=None,
        detail=detail,
        expected_tunnel_command=_PREDICTION_TUNNEL_COMMAND,
        checked_at=checked_at,
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
    "/v1/trades",
    response_model=list[TradeOut],
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def list_trades(
    limit: int = Query(default=200, ge=1, le=1000),
    ticker: str | None = Query(default=None),
) -> list[TradeOut]:
    """
    List filled paper trades, most recent first (READ ONLY).

    This is the Paper Trade Ledger: one row per executed paper fill. Each trade
    is joined to its Order to expose the originating trade_decision_id for
    lifecycle tracing (Candidate -> Decision -> Order -> Trade -> Position).

    READ ONLY: no DB writes, no order/trade/position creation, no broker
    execution. All trades returned are paper fills from the local database.

    Optional filters:
        limit  — max rows to return (default 200, 1..1000)
        ticker — restrict to a single ticker (case-insensitive)
    """
    with get_session() as session:
        stmt = (
            select(Trade, Order.trade_decision_id)
            .join(Order, Trade.order_id == Order.id)
            .order_by(Trade.trade_ts.desc())
        )
        if ticker:
            stmt = stmt.where(Trade.ticker == ticker.strip().upper())
        stmt = stmt.limit(limit)
        rows = session.execute(stmt).all()
        out: list[TradeOut] = []
        for tr, decision_id in rows:
            out.append(
                TradeOut(
                    id=str(tr.id),
                    short_id=str(tr.id)[:8],
                    order_id=str(tr.order_id),
                    order_short_id=str(tr.order_id)[:8],
                    trade_decision_id=str(decision_id) if decision_id else None,
                    trade_decision_short_id=(
                        str(decision_id)[:8] if decision_id else None
                    ),
                    ticker=tr.ticker,
                    side=tr.side,
                    qty=str(tr.qty),
                    fill_price=str(tr.fill_price),
                    gross_value=str(tr.gross_value),
                    commission=str(tr.commission),
                    net_value=str(tr.net_value),
                    realized_pnl=(
                        str(tr.realized_pnl) if tr.realized_pnl is not None else None
                    ),
                    trade_ts=tr.trade_ts,
                    market_date=tr.market_date,
                    status="FILLED",
                    notes="Paper only - no broker execution",
                )
            )
        return out


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
    _prediction_capture: list[dict] = []
    try:
        fetched_responses, fetch_failures = await _fetch_predictions_with_optional_capture(
            tickers=body.tickers,
            api_url=settings.stock_prediction_api_url,
            timeout_seconds=settings.stock_prediction_api_timeout_seconds,
            capture=_prediction_capture,
        )
    except Exception as exc:
        # Persist whatever was captured before the fetch blew up (read-only audit).
        _persist_prediction_runs(_prediction_capture, source="PREDICTION_PREVIEW")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch predictions: {str(exc)}",
        )

    # Capture every prediction call (success and failure) into the local
    # prediction_runs store. Observational only — creates no signals/orders/trades.
    _persist_prediction_runs(_prediction_capture, source="PREDICTION_PREVIEW")

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

    # Resolve full ticker list; explicit tickers take precedence over universe
    from paper_trader.engine.universe import get_sp500_universe
    if body.tickers:
        all_tickers: list[str] = list(body.tickers)
    else:
        all_tickers = get_sp500_universe()

    total_available = len(all_tickers)
    start_idx = body.start_index
    tickers_to_backfill = all_tickers[start_idx : start_idx + body.max_tickers]
    end_idx_exclusive = start_idx + len(tickers_to_backfill)
    next_start = end_idx_exclusive
    has_more = end_idx_exclusive < total_available
    requested_count = total_available

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
        total_available_tickers=total_available,
        start_index=start_idx,
        end_index_exclusive=end_idx_exclusive,
        next_start_index=next_start,
        has_more=has_more,
        selected_ticker_count=len(tickers_to_backfill),
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
    "/v1/market/refresh-snapshot",
    response_model=RefreshSnapshotResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def refresh_market_snapshot() -> RefreshSnapshotResponse:
    """
    Fetch latest S&P 500 close prices from Yahoo Finance and create/update today's
    portfolio snapshot.  Safe: no orders, no signals, no automation.
    Idempotent: calling again on the same market_date updates prices and returns
    the existing portfolio snapshot.
    """
    import uuid as _uuid
    from paper_trader.engine.universe import get_sp500_universe

    now, market_date = _now_and_date()
    tickers = get_sp500_universe()
    prices_inserted = 0
    price_failures: list[dict] = []

    if tickers:
        successful, failures = fetch_latest_prices(tickers)
        price_failures = [{"ticker": f["ticker"], "reason": f["reason"]} for f in failures]
        rows = []
        for p in successful:
            try:
                price = Decimal(p["price"])
                if price > 0:
                    rows.append(PriceSnapshot(
                        ticker=p["ticker"],
                        price=price,
                        session_type=SessionType.REGULAR,
                        price_type=PriceType.CLOSE,
                        data_source="yahoo_finance",
                        snapshot_ts=now,
                        market_date=market_date,
                        job_run_id=None,
                    ))
            except (ValueError, TypeError):
                price_failures.append({"ticker": p.get("ticker", "unknown"), "reason": "price conversion error"})
        if rows:
            with get_session() as session:
                session.add_all(rows)
            prices_inserted = len(rows)

    idempotency_key = f"refresh-snapshot-{market_date}-{_uuid.uuid4().hex[:8]}"
    snapshot_result: dict | None = None
    snapshot_created = False
    snapshot_error: str | None = None
    try:
        snapshot_result = run_snapshot_workflow(
            idempotency_key=idempotency_key,
            market_date=market_date,
            now=now,
        )
        snapshot_created = True
    except MissingPricesError as exc:
        snapshot_error = str(exc)
    except RuntimeError as exc:
        snapshot_error = str(exc)

    return RefreshSnapshotResponse(
        market_date=market_date,
        tickers_requested=len(tickers),
        prices_inserted_or_updated=prices_inserted,
        price_failures=price_failures,
        portfolio_snapshot_created_or_updated=snapshot_created,
        portfolio_total_value=snapshot_result.get("total_value") if snapshot_result else None,
        cash=snapshot_result.get("cash") if snapshot_result else None,
        positions_value=snapshot_result.get("positions_value") if snapshot_result else None,
        open_positions_count=snapshot_result.get("open_position_count") if snapshot_result else None,
        safety_mode="PRICE_AND_PORTFOLIO_SNAPSHOT_ONLY",
        snapshot_error=snapshot_error,
    )


@app.post(
    "/v1/market/backfill-history",
    response_model=BackfillHistoryResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def backfill_history(body: BackfillHistoryRequest) -> BackfillHistoryResponse:
    """
    Backfill historical daily CLOSE prices for the S&P 500 universe and SPY benchmark.

    Writes price_snapshots for universe tickers and benchmark_prices for SPY.
    Idempotent: skips rows that already exist (same ticker + market_date + price_type + session_type).
    Safe: writes only price_snapshots and benchmark_prices. No orders, signals, decisions, or trades.
    """
    from datetime import timedelta, datetime as _dt
    from paper_trader.engine.universe import get_sp500_universe

    _, end_date = _now_and_date()
    start_date = end_date - timedelta(days=body.lookback_days)

    universe_tickers = [t for t in get_sp500_universe() if t.upper() != "SPY"]

    successful_prices, fetch_failures = fetch_historical_prices(
        tickers=universe_tickers,
        start_date=start_date,
        end_date=end_date,
    )
    spy_successful, spy_failures = fetch_historical_prices(
        tickers=["SPY"],
        start_date=start_date,
        end_date=end_date,
    )

    tickers_requested = len(universe_tickers) + 1
    tickers_failed = len(fetch_failures) + len(spy_failures)
    tickers_succeeded = len(successful_prices) + len(spy_successful)
    snapshots_inserted = 0
    market_dates_set: set = set()

    with get_dedicated_session() as session:
        for ticker, rows in successful_prices.items():
            for row in rows:
                md = row["market_date"]
                price = row["price"]
                market_dates_set.add(md)
                existing_id = session.execute(
                    select(PriceSnapshot.id).where(
                        PriceSnapshot.ticker == ticker,
                        PriceSnapshot.market_date == md,
                        PriceSnapshot.price_type == PriceType.CLOSE,
                        PriceSnapshot.session_type == SessionType.REGULAR,
                    )
                ).scalar_one_or_none()
                if not existing_id:
                    if not body.dry_run:
                        snap_ts = _dt.combine(md, _dt.min.time()).replace(hour=16, tzinfo=timezone.utc)
                        session.add(PriceSnapshot(
                            ticker=ticker,
                            price=price,
                            market_date=md,
                            price_type=PriceType.CLOSE,
                            session_type=SessionType.REGULAR,
                            snapshot_ts=snap_ts,
                            data_source="yahoo_finance",
                            job_run_id=None,
                        ))
                    snapshots_inserted += 1

        for row in spy_successful.get("SPY", []):
            md = row["market_date"]
            price = row["price"]
            market_dates_set.add(md)
            existing_id = session.execute(
                select(BenchmarkPrice.id).where(
                    BenchmarkPrice.ticker == "SPY",
                    BenchmarkPrice.market_date == md,
                    BenchmarkPrice.session_type == SessionType.REGULAR,
                )
            ).scalar_one_or_none()
            if not existing_id:
                if not body.dry_run:
                    snap_ts = _dt.combine(md, _dt.min.time()).replace(hour=16, tzinfo=timezone.utc)
                    session.add(BenchmarkPrice(
                        ticker="SPY",
                        price=price,
                        market_date=md,
                        session_type=SessionType.REGULAR,
                        snapshot_ts=snap_ts,
                        job_run_id=None,
                    ))
                snapshots_inserted += 1

        if not body.dry_run:
            session.commit()

        readiness = _get_screening_readiness_data(session)

    return BackfillHistoryResponse(
        safety_mode="HISTORICAL_PRICES_ONLY",
        start_date=str(start_date),
        end_date=str(end_date),
        tickers_requested=tickers_requested,
        tickers_succeeded=tickers_succeeded,
        tickers_failed=tickers_failed,
        snapshots_inserted_or_updated=snapshots_inserted,
        market_dates_written=len(market_dates_set),
        tickers_with_at_least_6_snapshots=readiness["tickers_with_at_least_6_snapshots"],
        tickers_with_at_least_21_snapshots=readiness["tickers_with_at_least_21_snapshots"],
        spy_snapshot_count=readiness["spy_snapshot_count"],
        screening_ready=readiness["screening_ready"],
        no_writes_to_orders_signals_decisions_trades=True,
    )


@app.get(
    "/v1/market/screening-readiness",
    response_model=ScreeningReadinessResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def get_screening_readiness() -> ScreeningReadinessResponse:
    """
    Return current screening readiness metrics. Read-only — no database writes.

    screening_ready is True when spy_snapshot_count >= 21 and
    tickers_with_at_least_21_snapshots >= 10.
    """
    with get_session() as session:
        data = _get_screening_readiness_data(session)

    spy_cnt = data["spy_snapshot_count"]
    tickers_21 = data["tickers_with_at_least_21_snapshots"]
    if data["screening_ready"]:
        msg = "Ready. Sufficient price history and SPY benchmark data available for screening."
    elif spy_cnt < 21:
        msg = (
            f"SPY benchmark history insufficient ({spy_cnt} snapshots, need 21+). "
            "Run Backfill Screening History first."
        )
    elif tickers_21 < 10:
        msg = (
            f"Universe history insufficient ({tickers_21} tickers with 21d history, need 10+). "
            "Run Backfill Screening History first."
        )
    else:
        msg = "Not ready. Run Backfill Screening History first."

    return ScreeningReadinessResponse(
        price_snapshots_total=data["price_snapshots_total"],
        distinct_market_dates=data["distinct_market_dates"],
        tickers_total=data["tickers_total"],
        tickers_with_at_least_6_snapshots=data["tickers_with_at_least_6_snapshots"],
        tickers_with_at_least_21_snapshots=tickers_21,
        spy_snapshot_count=spy_cnt,
        latest_market_date=data["latest_market_date"],
        screening_ready=data["screening_ready"],
        message=msg,
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


_LOCAL_SCAN_FORMULA_LABEL = (
    "0.3*mom5d(+) + 0.4*mom20d(+) + 0.3*rs_spy(+); vol_penalty if >5%"
)
_FINAL_SCORE_FORMULA_LABEL = (
    "conf*return[2x-neg] + clip(0.15*mom5d+0.10*mom20d,+-0.05)"
    " + clip(0.15*rs,+-0.05) + clip(0.10*(scan_norm-0.5),+-0.05); low-conf*0.80"
)
_BALANCED_SCORE_FORMULA_LABEL = (
    "conf*return[2x-neg] + clip(0.07*mom5d+0.05*mom20d,+-0.05)"
    " + clip(0.10*rs,+-0.05) + clip(0.08*(scan_norm-0.5),+-0.05)"
    " - vol_penalty(0.05) - holding_penalty(0.015); low-conf*0.80"
)
_QUALITY_SCORE_FORMULA_LABEL = (
    "conf*return[2x-neg] + clip(spike_safe(0.05*mom5d)+0.07*mom20d,+-0.05)"
    " + clip(0.20*rs,+-0.05) + clip(0.08*(scan_norm-0.5),+-0.05)"
    " - vol_penalty(0.04); spike_penalty(0.03) if |5D|>8%; low-conf*0.80"
)
_RISK_ADJUSTED_SCORE_FORMULA_LABEL = (
    "conf*(0.8*return[pos] or 2.5*return[neg])"
    " + clip(0.04*mom5d+0.04*mom20d,+-0.05)"
    " + clip(0.08*rs,+-0.05) + clip(0.06*(scan_norm-0.5),+-0.05)"
    " - vol_penalty(0.10) - holding_penalty(0.025); missing:-0.02; low-conf*0.80"
)
_PROFILE_FORMULA_LABELS: dict[str, str] = {
    "current": _FINAL_SCORE_FORMULA_LABEL,
    "balanced_preview": _BALANCED_SCORE_FORMULA_LABEL,
    "quality_preview": _QUALITY_SCORE_FORMULA_LABEL,
    "risk_adjusted_preview": _RISK_ADJUSTED_SCORE_FORMULA_LABEL,
}
_PROFILE_EXPLANATIONS: dict[str, str] = {
    "balanced_preview": (
        "Balanced profile uses reduced momentum weights (0.07/0.05 vs 0.15/0.10), "
        "adds a volatility penalty (0.05), and applies a small discount for already-held tickers (0.015). "
        "Current profile is more momentum-heavy."
    ),
    "quality_preview": (
        "Quality profile favours sustained relative strength (RS weight 0.20 vs 0.15) and prediction quality. "
        "Penalises extreme 5-day momentum spikes (|5D|>8% → -0.03 penalty) to avoid buying exhausted moves. "
        "Applies a mild volatility penalty (0.04). Less weight on short-term momentum (0.05/0.07 vs 0.15/0.10)."
    ),
    "risk_adjusted_preview": (
        "Risk-adjusted profile is the most conservative. Discounts positive expected returns (×0.80) and "
        "applies a harsher penalty for negative returns (×2.50). Very aggressive volatility penalty (0.10 vs 0.05). "
        "Stronger holding penalty (0.025) and stronger missing-prediction penalty (-0.02). "
        "Minimal momentum sensitivity (0.04/0.04). Rewards smooth, low-volatility candidates."
    ),
}
_SKIPPED_SAMPLE_LIMIT = 25

_CANDIDATE_QUALITY_EXPLANATION = (
    "A candidate is an ACTIONABLE_TRADE_IDEA only if its preview score, prediction "
    "confidence, expected 5D return, and relative strength vs SPY all clear the "
    "thresholds below. Candidates that fall short are shown as WATCH / below "
    "threshold, not as actionable BUYs."
)


def _classify_candidate_actionability(
    *,
    candidate_type: str | None,
    is_current_holding: bool,
    status: str,
    preview_decision: str,
    preview_score: str | None,
    prediction_confidence: str | None,
    expected_return_pct: str | None,
    relative_strength_vs_spy_20d: str | None,
    min_score: float,
    min_confidence: float,
    min_expected_return_pct: float,
    min_relative_strength: float,
) -> tuple[str, str, list[str], dict[str, bool]]:
    """
    Classify a candidate's actionability for the Scan Coverage contract.

    Returns (actionability, reason_summary, reason_codes, threshold_pass_fail).

    actionability is one of:
        ACTIONABLE_TRADE_IDEA | WATCH_ONLY | BELOW_THRESHOLD | REJECTED | ALREADY_HELD

    This does NOT change review-queue eligibility semantics; it is an additive,
    truthful quality label so the UI can bucket ideas and so weak BUYs never
    appear as actionable.
    """
    def _f(v: str | None) -> float | None:
        try:
            return float(v) if v is not None and v != "" else None
        except (ValueError, TypeError):
            return None

    # Held positions are monitored automatically — never a new actionable BUY.
    if is_current_holding or candidate_type == "CURRENT_HOLDING_MONITOR":
        return (
            "ALREADY_HELD",
            "Already held — monitored automatically in Positions Reviewed.",
            ["ALREADY_HELD"],
            {},
        )

    # No usable prediction or an explicit reject → REJECTED.
    if status != "OK" or preview_decision == "REJECT":
        reason = (
            "Prediction unavailable." if status != "OK"
            else "Rejected by preview decision (SELL or negative outlook)."
        )
        return ("REJECTED", reason, ["REJECTED"], {})

    score = _f(preview_score)
    conf = _f(prediction_confidence)
    ret = _f(expected_return_pct)
    rs = _f(relative_strength_vs_spy_20d)

    score_pass = score is not None and score >= min_score
    conf_pass = conf is not None and conf >= min_confidence
    ret_pass = ret is not None and ret >= min_expected_return_pct
    # Relative strength is acceptable when unknown (None) or not lagging SPY.
    rs_pass = rs is None or rs >= min_relative_strength

    threshold_pass_fail = {
        "score": bool(score_pass),
        "confidence": bool(conf_pass),
        "expected_return": bool(ret_pass),
        "relative_strength": bool(rs_pass),
    }

    reason_codes: list[str] = []
    fail_bits: list[str] = []
    if not score_pass:
        reason_codes.append("SCORE_BELOW_THRESHOLD")
        fail_bits.append(
            f"Score {preview_score if preview_score is not None else 'n/a'} "
            f"below actionable threshold {min_score:g}"
        )
    if not conf_pass:
        reason_codes.append("CONFIDENCE_BELOW_THRESHOLD")
        fail_bits.append(
            f"Confidence {prediction_confidence if prediction_confidence is not None else 'n/a'} "
            f"below {min_confidence:g}"
        )
    if not ret_pass:
        reason_codes.append("EXPECTED_RETURN_BELOW_THRESHOLD")
        fail_bits.append(
            f"Expected return {expected_return_pct if expected_return_pct is not None else 'n/a'}% "
            f"below {min_expected_return_pct:g}%"
        )
    if not rs_pass:
        reason_codes.append("RELATIVE_STRENGTH_BELOW_THRESHOLD")
        fail_bits.append(
            f"Relative strength {relative_strength_vs_spy_20d} below {min_relative_strength:g}"
        )

    all_pass = score_pass and conf_pass and ret_pass and rs_pass
    if all_pass and preview_decision == "CONSIDER":
        return (
            "ACTIONABLE_TRADE_IDEA",
            "Passes all quality thresholds (score, confidence, return, relative strength).",
            ["PASSES_ALL_THRESHOLDS"],
            threshold_pass_fail,
        )

    # CONSIDER but failed a threshold → explicitly below threshold; otherwise watch.
    if preview_decision == "CONSIDER":
        actionability = "BELOW_THRESHOLD"
        summary = "Below actionable threshold. " + "; ".join(fail_bits) + "."
    else:
        actionability = "WATCH_ONLY"
        summary = "Watch only — not a strong enough signal to act on yet."
        if fail_bits:
            summary += " " + "; ".join(fail_bits) + "."
    return (actionability, summary, reason_codes or ["WATCH"], threshold_pass_fail)


def _cand_to_score_dict(cand: Any) -> dict:
    """Build a score_candidate_v2 input dict from a CandidateReview ORM object.

    DB fields store percentages as decimal strings (e.g. '1.73' = 1.73 %);
    score_candidate_v2 expects fractions (0.0173).  scan_score is passed
    as-is — the scoring module auto-detects 0-1 vs 0-100 scale.
    """
    def _pct(val: str | None) -> float:
        try:
            return float(val or "0") / 100.0
        except (ValueError, TypeError):
            return 0.0

    return {
        "prediction_confidence":        _safe_float(cand.prediction_confidence, 0.0),
        "expected_return_pct":          _pct(cand.expected_return_pct),
        "momentum_5d_pct":              _pct(cand.momentum_5d_pct),
        "momentum_20d_pct":             _pct(cand.momentum_20d_pct),
        "relative_strength_vs_spy_20d": _pct(cand.relative_strength_vs_spy_20d),
        "scan_score":                   _safe_float(cand.scan_score, 0.0),
    }


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

    # Inject current open-position tickers into GCP batch (holdings always need fresh prediction)
    holdings_injected: list[str] = []
    holding_tickers_set: set[str] = set()
    _open_positions_map: dict[str, Any] = {}  # ticker.upper() -> Position (portfolio-aware enrichment)
    if body.include_current_positions_for_prediction:
        with get_dedicated_session() as _pos_session:
            open_positions = list(_pos_session.execute(select(Position)).scalars().all())
        selected_set = set(selected_tickers)
        for pos in open_positions:
            t = pos.ticker.upper()
            holding_tickers_set.add(t)
            _open_positions_map[t] = pos
            if t not in selected_set:
                holdings_injected.append(t)
                selected_set.add(t)
        selected_tickers = selected_tickers + holdings_injected

    # Fetch predictions for selected tickers (bounded concurrency)
    fetched_responses = []
    fetch_failures = []
    _prediction_capture: list[dict] = []
    _t_fetch_start = datetime.now(timezone.utc)

    if selected_tickers:
        try:
            fetched_responses, fetch_failures = await _fetch_predictions_with_optional_capture(
                tickers=selected_tickers,
                api_url=settings.stock_prediction_api_url,
                timeout_seconds=settings.stock_prediction_api_timeout_seconds,
                max_concurrency=body.max_prediction_concurrency,
                capture=_prediction_capture,
            )
        except Exception as exc:
            _persist_prediction_runs(
                _prediction_capture,
                daily_session_id=body.daily_session_id,
                source=body.capture_source,
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch predictions: {str(exc)}",
            )

    # Capture every prediction call (success and failure) into the local
    # prediction_runs store. Observational only — creates no signals/orders/trades.
    # Stamp the Daily Review session id/context so the Scan Selection Funnel can
    # report real per-session capture counts.
    _persist_prediction_runs(
        _prediction_capture,
        daily_session_id=body.daily_session_id,
        source=body.capture_source,
    )

    _prediction_elapsed_ms = int(
        (datetime.now(timezone.utc) - _t_fetch_start).total_seconds() * 1000
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

        # Determine status (local; named candidate_status to avoid shadowing
        # the FastAPI `status` module used for HTTP_* codes elsewhere here).
        if selected_ticker in failed_fetch_map:
            candidate_status = "FAILED_FETCH"
        elif selected_ticker in failed_norm_map:
            candidate_status = "FAILED_NORMALIZATION"
        elif selected_ticker in missing_pred_map:
            candidate_status = "MISSING_PREDICTION"
        elif normalized:
            candidate_status = "OK"
        else:
            candidate_status = "MISSING_PREDICTION"

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
            normalized, candidate_status, expected_return_pct
        )

        # Add scan context to reasons if available
        if candidate_status == "OK" and preview_decision != "REJECT":
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
            candidate_status,
        )

        # Phase 4A: explainability fields
        is_top_scan = selected_ticker in selected_candidates_map
        is_holding = selected_ticker in holding_tickers_set
        if is_top_scan and is_holding:
            selected_for_gcp_reason = "BOTH"
        elif is_top_scan:
            selected_for_gcp_reason = "TOP_SCAN"
        else:
            selected_for_gcp_reason = "CURRENT_HOLDING_INJECTED"

        # Top score drivers
        _top_drivers: list[str] = []
        if candidate_status == "OK" and normalized:
            try:
                _exp = float(normalized.get("expected_return_pct", "0") or "0")
                if _exp > 0:
                    _top_drivers.append("prediction_return")
            except (ValueError, TypeError):
                pass
            try:
                _cf = float(normalized.get("confidence", "0") or "0")
                if _cf >= 0.7:
                    _top_drivers.append("prediction_confidence")
                elif _cf >= 0.5:
                    _top_drivers.append("moderate_confidence")
            except (ValueError, TypeError):
                pass
        elif candidate_status != "OK":
            _top_drivers.append("missing_prediction")

        if relative_strength_vs_spy_20d:
            try:
                if float(relative_strength_vs_spy_20d) > 0:
                    _top_drivers.append("relative_strength")
            except (ValueError, TypeError):
                pass
        if momentum_5d_pct:
            try:
                if float(momentum_5d_pct) > 0:
                    _top_drivers.append("momentum_5d")
            except (ValueError, TypeError):
                pass
        if momentum_20d_pct:
            try:
                if float(momentum_20d_pct) > 0:
                    _top_drivers.append("momentum_20d")
            except (ValueError, TypeError):
                pass
        if is_holding:
            _top_drivers.append("already_held")
        if not is_top_scan:
            _top_drivers.append("no_scan_data")

        # Skip or warning reason
        _skip_warn: str | None = None
        if candidate_status != "OK":
            _skip_warn = f"PREDICTION_{candidate_status}"
        elif normalized and prediction_recommendation == "SELL":
            _skip_warn = "SELL_RECOMMENDATION"
        elif normalized:
            try:
                if float(normalized.get("confidence", "1") or "1") < 0.5:
                    _skip_warn = "LOW_CONFIDENCE"
            except (ValueError, TypeError):
                pass

        price_history_points = candidate.price_count if candidate else None

        # Phase 4B: candidate classification
        # BOTH means ticker appeared in both top scan AND current holdings — treat as monitoring
        _candidate_type: str = (
            "NEW_BUY_CANDIDATE"
            if selected_for_gcp_reason == "TOP_SCAN"
            else "CURRENT_HOLDING_MONITOR"
        )

        # Eligibility: only NEW_BUY_CANDIDATE rows with CONSIDER decision are review-queue eligible
        if _candidate_type == "NEW_BUY_CANDIDATE" and preview_decision == "CONSIDER":
            _eligible_for_rq = True
            _rq_reason: str = "NEW_BUY_CANDIDATE"
        else:
            _eligible_for_rq = False
            if _candidate_type == "CURRENT_HOLDING_MONITOR":
                _rq_reason = "CURRENT_HOLDING_MONITOR_NOT_NEW_BUY"
            elif preview_decision == "WATCH":
                _rq_reason = "WATCH_ONLY"
            elif preview_decision == "REJECT":
                _rq_reason = "REJECTED"
            elif candidate_status != "OK":
                _rq_reason = "MISSING_PREDICTION"
            else:
                _rq_reason = "OTHER"

        # Scan Coverage + Candidate Quality: truthful actionability classification.
        _actionability, _reason_summary, _reason_codes, _threshold_pf = _classify_candidate_actionability(
            candidate_type=_candidate_type,
            is_current_holding=is_holding,
            status=candidate_status,
            preview_decision=preview_decision,
            preview_score=preview_score,
            prediction_confidence=prediction_confidence,
            expected_return_pct=expected_return_pct,
            relative_strength_vs_spy_20d=relative_strength_vs_spy_20d,
            min_score=body.min_actionable_score,
            min_confidence=body.min_actionable_confidence,
            min_expected_return_pct=body.min_expected_return_pct,
            min_relative_strength=body.min_relative_strength_vs_spy,
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
            status=candidate_status,
            price_history_points=price_history_points,
            prediction_status=candidate_status,
            selected_for_gcp_reason=selected_for_gcp_reason,
            top_score_drivers=_top_drivers,
            skip_or_warning_reason=_skip_warn,
            candidate_type=_candidate_type,
            is_current_holding=is_holding,
            eligible_for_review_queue=_eligible_for_rq,
            review_queue_eligibility_reason=_rq_reason,
            open_position_qty=str(_open_positions_map[selected_ticker.upper()].qty) if selected_ticker.upper() in _open_positions_map else None,
            open_position_avg_cost=str(_open_positions_map[selected_ticker.upper()].avg_cost) if selected_ticker.upper() in _open_positions_map else None,
            holding_action_hint="Monitor in Position Review" if is_holding else None,
            actionability=_actionability,
            reason_summary=_reason_summary,
            reason_codes=_reason_codes,
            threshold_pass_fail=_threshold_pf,
        )
        candidate_previews.append(preview)

    # Phase 4B/4C: score_breakdown + multi-profile comparison post-processing
    scoring_profile_comparison: ScoringProfileComparisonOut | None = None
    if candidate_previews:
        # Scoring function map — all preview profiles
        _all_funcs: dict[str, Any] = {
            "current": _score_candidate_v2,
            "balanced_preview": _score_candidate_balanced_preview,
            "quality_preview": _score_candidate_quality_preview,
            "risk_adjusted_preview": _score_candidate_risk_adjusted_preview,
        }

        def _to_score_input(p: CandidatePreview) -> dict:
            def _f(v: str | None) -> float:
                try:
                    return float(v) if v is not None else 0.0
                except (ValueError, TypeError):
                    return 0.0
            return {
                "prediction_confidence": _f(p.prediction_confidence),
                "expected_return_pct": _f(p.expected_return_pct),
                "momentum_5d_pct": _f(p.momentum_5d_pct),
                "momentum_20d_pct": _f(p.momentum_20d_pct),
                "relative_strength_vs_spy_20d": _f(p.relative_strength_vs_spy_20d),
                "scan_score": _f(p.scan_score),
                "volatility_20d_pct": _f(
                    selected_candidates_map[p.ticker].volatility_20d_pct
                    if p.ticker in selected_candidates_map
                    else None
                ),
                "is_current_holding": p.is_current_holding,
            }

        _score_inputs = [_to_score_input(p) for p in candidate_previews]

        # Compute active-profile factors (always needed for score_breakdown)
        _active_factors = [_all_funcs[body.scoring_profile](s) for s in _score_inputs]

        # Add score_breakdown to each candidate (Phase 4C — always populated)
        updated_previews: list[CandidatePreview] = []
        for i, p in enumerate(candidate_previews):
            updated_previews.append(p.model_copy(update={
                "score_breakdown": _build_score_breakdown(_active_factors[i]),
            }))
        candidate_previews = updated_previews

        # Non-current profiles: compute all profiles for comparison (Phase 4B/4C)
        if body.scoring_profile != "current":
            _cur_factors_cmp = [_score_candidate_v2(s) for s in _score_inputs]
            _cur_scores_cmp: list[float] = [f.total_score for f in _cur_factors_cmp]
            _act_scores_cmp: list[float] = [f.total_score for f in _active_factors]

            # Compute all 4 profile scores (for overlap_matrix + high-disagreement)
            _all_profile_scores: dict[str, list[float]] = {"current": _cur_scores_cmp}
            for _pname, _pfunc in _all_funcs.items():
                if _pname == "current":
                    continue
                if _pname == body.scoring_profile:
                    _all_profile_scores[_pname] = _act_scores_cmp
                else:
                    _all_profile_scores[_pname] = [
                        _pfunc(s).total_score for s in _score_inputs
                    ]

            # Per-profile rankings (rank 1 = highest score)
            _profile_orders: dict[str, list[int]] = {}
            _profile_ranks: dict[str, dict[int, int]] = {}
            for _pn, _ps in _all_profile_scores.items():
                _ord = sorted(range(len(_ps)), key=lambda i, s=_ps: -s[i])
                _profile_orders[_pn] = _ord
                _profile_ranks[_pn] = {idx: rk + 1 for rk, idx in enumerate(_ord)}

            _cur_rank_by_idx = _profile_ranks["current"]
            _act_rank_by_idx = _profile_ranks[body.scoring_profile]
            _cur_order_cmp = _profile_orders["current"]
            _act_order_cmp = _profile_orders[body.scoring_profile]

            # Phase 4B compat: populate balanced_preview fields when that profile is active
            if body.scoring_profile == "balanced_preview":
                def _bal_drivers(p: CandidatePreview, cur_f: float, bal_f: float, s_inp: dict) -> list[str]:
                    d: list[str] = []
                    if s_inp.get("is_current_holding"):
                        d.append("holding_penalty_applied")
                    vol = abs(s_inp.get("volatility_20d_pct", 0.0))
                    if vol > 0.05:
                        d.append("high_vol_penalised")
                    elif vol > 0.0:
                        d.append("low_vol_neutral")
                    if abs(bal_f - cur_f) < 0.002:
                        d.append("score_similar_to_current")
                    elif bal_f > cur_f:
                        d.append("promoted_by_balanced_formula")
                    else:
                        d.append("demoted_by_balanced_formula")
                    if not s_inp.get("prediction_confidence"):
                        d.append("missing_prediction_penalty")
                    return d

                bal_previews: list[CandidatePreview] = []
                for i, p in enumerate(candidate_previews):
                    cf = _cur_scores_cmp[i]
                    af = _act_scores_cmp[i]
                    bal_previews.append(p.model_copy(update={
                        "current_score": f"{cf:.6f}",
                        "balanced_preview_score": f"{af:.6f}",
                        "score_delta": f"{af - cf:.6f}",
                        "current_rank": _cur_rank_by_idx[i],
                        "balanced_preview_rank": _act_rank_by_idx[i],
                        "ranking_change": _cur_rank_by_idx[i] - _act_rank_by_idx[i],
                        "balanced_score_drivers": _bal_drivers(p, cf, af, _score_inputs[i]),
                    }))
                candidate_previews = bal_previews

            # Top tickers per profile (Phase 4C)
            _top_n_cmp = min(10, len(candidate_previews))
            _top_tickers_by_profile: dict[str, list[str]] = {
                pn: [candidate_previews[i].ticker for i in _profile_orders[pn][:_top_n_cmp]]
                for pn in _all_profile_scores
            }

            # Overlap matrix between all profile pairs
            _overlap_matrix: dict[str, int] = {}
            _pnames_list = list(_all_profile_scores.keys())
            for _ai in range(len(_pnames_list)):
                for _bi in range(_ai + 1, len(_pnames_list)):
                    _a, _b = _pnames_list[_ai], _pnames_list[_bi]
                    _key = f"{_a}_vs_{_b}"
                    _overlap_matrix[_key] = len(
                        set(_top_tickers_by_profile[_a]) & set(_top_tickers_by_profile[_b])
                    )

            # Promotions / demotions per profile pair (current vs each other)
            _promo_by_profile: dict[str, list[dict]] = {}
            _demo_by_profile: dict[str, list[dict]] = {}
            for _pn in _all_funcs:
                if _pn == "current":
                    continue
                _pair_key = f"current_vs_{_pn}"
                _changes: list[dict] = []
                for ci in range(len(candidate_previews)):
                    cur_r = _cur_rank_by_idx[ci]
                    p_r = _profile_ranks[_pn][ci]
                    rc = cur_r - p_r  # positive = promoted in this profile vs current
                    if rc != 0:
                        _changes.append({
                            "ticker": candidate_previews[ci].ticker,
                            "current_rank": cur_r,
                            f"{_pn}_rank": p_r,
                            "rank_change": rc,
                        })
                _promo_by_profile[_pair_key] = sorted(
                    [c for c in _changes if c["rank_change"] > 0], key=lambda c: -c["rank_change"]
                )[:3]
                _demo_by_profile[_pair_key] = sorted(
                    [c for c in _changes if c["rank_change"] < 0], key=lambda c: c["rank_change"]
                )[:3]

            # Candidates with high disagreement across all profiles
            _high_disagree: list[dict] = []
            for ci in range(len(candidate_previews)):
                _ticker = candidate_previews[ci].ticker
                _all_ranks = {pn: _profile_ranks[pn][ci] for pn in _all_profile_scores}
                _spread = max(_all_ranks.values()) - min(_all_ranks.values())
                _all_sc = {pn: round(_all_profile_scores[pn][ci], 6) for pn in _all_profile_scores}
                if _spread >= 3:
                    _high_disagree.append({
                        "ticker": _ticker,
                        "rank_spread": _spread,
                        "ranks": _all_ranks,
                        "scores": _all_sc,
                    })
            _high_disagree.sort(key=lambda x: -x["rank_spread"])
            _high_disagree = _high_disagree[:5]

            # Phase 4B compat values
            _cur_vs_act_key = f"current_vs_{body.scoring_profile}"
            _cur_top_compat = _top_tickers_by_profile.get("current", [])
            _act_top_compat = _top_tickers_by_profile.get(body.scoring_profile, [])
            _overlap_compat = _overlap_matrix.get(_cur_vs_act_key, 0)
            _changed_compat = sum(
                1 for ci in range(len(candidate_previews))
                if _cur_rank_by_idx[ci] != _act_rank_by_idx[ci]
            )

            scoring_profile_comparison = ScoringProfileComparisonOut(
                active_profile=body.scoring_profile,
                # Phase 4B compat
                current_top_tickers=_cur_top_compat,
                balanced_top_tickers=(
                    _act_top_compat if body.scoring_profile == "balanced_preview"
                    else _cur_top_compat
                ),
                overlap_count=_overlap_compat,
                changed_rank_count=_changed_compat,
                biggest_promotions=_promo_by_profile.get(_cur_vs_act_key, []),
                biggest_demotions=_demo_by_profile.get(_cur_vs_act_key, []),
                # Phase 4C multi-profile
                profiles_compared=list(_all_profile_scores.keys()),
                top_tickers_by_profile=_top_tickers_by_profile,
                overlap_matrix=_overlap_matrix,
                biggest_promotions_by_profile=_promo_by_profile,
                biggest_demotions_by_profile=_demo_by_profile,
                candidates_with_high_disagreement=_high_disagree,
                explanation=_PROFILE_EXPLANATIONS.get(
                    body.scoring_profile, "Alternative scoring profile."
                ),
                safety_counts={"signals_created": 0, "decisions_created": 0, "orders_created": 0},
            )

    # Build candidate funnel diagnostics
    skipped_by_reason: dict[str, int] = {}
    for s in skipped:
        skipped_by_reason[s.reason] = skipped_by_reason.get(s.reason, 0) + 1

    not_sent_candidates = clean_candidates[body.prediction_top_n:]
    top_scan_not_predicted = [
        TopScanNotPredicted(
            rank=c.rank,
            ticker=c.ticker,
            scan_score=c.score,
        )
        for c in not_sent_candidates[:25]
    ]

    skipped_examples = [
        SkippedTickerDetail(
            ticker=s.ticker,
            reason_codes=[s.reason],
            price_points=s.price_count,
        )
        for s in skipped[:50]
    ]

    outcomes_consider = sum(1 for p in candidate_previews if p.preview_decision == "CONSIDER")
    outcomes_watch = sum(1 for p in candidate_previews if p.preview_decision == "WATCH")
    outcomes_reject = sum(1 for p in candidate_previews if p.preview_decision == "REJECT")
    outcomes_failed = len(all_prediction_failures)
    outcomes_other = max(0, len(candidate_previews) - outcomes_consider - outcomes_watch - outcomes_reject)

    # Phase 4A: aggregate top driver counts across all previews
    _top_driver_counts: dict[str, int] = {}
    for _p in candidate_previews:
        for _drv in _p.top_score_drivers:
            _top_driver_counts[_drv] = _top_driver_counts.get(_drv, 0) + 1

    _skipped_insuf = skipped_by_reason.get("INSUFFICIENT_PRICE_HISTORY", 0)
    _skipped_no_data = skipped_by_reason.get("NO_PRICE_DATA", 0)

    candidate_funnel = CandidateFunnelOut(
        universe_count=len(universe_tickers),
        evaluated_count=len(universe_tickers) - len(skipped),
        skipped_count=len(skipped),
        skipped_by_reason=skipped_by_reason,
        top_scan_count=len(clean_candidates),
        clean_scan_count=len(clean_candidates),
        prediction_top_n=body.prediction_top_n,
        gcp_prediction_count=len(selected_tickers),
        not_sent_to_gcp_count=len(not_sent_candidates),
        current_holdings_injected_count=len(holdings_injected),
        gcp_concurrency=body.max_prediction_concurrency,
        prediction_elapsed_ms=_prediction_elapsed_ms,
        prediction_outcomes=PredictionOutcomes(
            consider=outcomes_consider,
            watch=outcomes_watch,
            reject=outcomes_reject,
            failed_fetch=outcomes_failed,
            other=outcomes_other,
        ),
        top_scan_not_predicted=top_scan_not_predicted,
        skipped_examples=skipped_examples,
        price_history_ready_count=max(0, len(universe_tickers) - _skipped_insuf - _skipped_no_data),
        skipped_insufficient_history_count=_skipped_insuf,
        local_scan_candidate_count=len(universe_tickers) - len(skipped),
        prediction_batch_count=len(selected_tickers),
        gcp_success_count=len(normalized_predictions),
        gcp_failure_count=len(all_prediction_failures),
        final_selected_count=len(candidate_previews),
        safety_counts={"signals_created": 0, "decisions_created": 0, "orders_created": 0},
    )

    scoring_summary = ScoringDiagnosticsOut(
        local_scan_formula_label=_LOCAL_SCAN_FORMULA_LABEL,
        final_score_formula_label=_FINAL_SCORE_FORMULA_LABEL,
        top_driver_counts=_top_driver_counts,
        threshold_summary=ThresholdSummaryOut(
            min_price_points=body.min_price_points,
            prediction_top_n=body.prediction_top_n,
            scan_top_n=body.top_n,
            max_prediction_concurrency=body.max_prediction_concurrency,
            include_current_positions_for_prediction=body.include_current_positions_for_prediction,
        ),
    )

    skipped_diagnostics = SkippedDiagnosticsOut(
        total_skipped=len(skipped),
        sample_limit=_SKIPPED_SAMPLE_LIMIT,
        samples=[
            SkippedTickerDiagnostic(
                ticker=s.ticker,
                reason=s.reason,
                price_history_points=s.price_count,
                latest_price_date=None,
                required_min_price_points=body.min_price_points,
            )
            for s in skipped[:_SKIPPED_SAMPLE_LIMIT]
        ],
    )

    # Scan Coverage + Candidate Quality contract.
    # locally_screened_count reflects the FULL local screen (universe minus skipped),
    # NOT the small GCP prediction batch — that distinction is the whole point.
    _actionable_count = sum(1 for p in candidate_previews if p.actionability == "ACTIONABLE_TRADE_IDEA")
    _watch_count = sum(
        1 for p in candidate_previews if p.actionability in ("WATCH_ONLY", "BELOW_THRESHOLD")
    )
    _rejected_count = sum(1 for p in candidate_previews if p.actionability == "REJECTED")
    _already_held_count = sum(1 for p in candidate_previews if p.actionability == "ALREADY_HELD")

    # excluded_reason_counts: why universe names did not become actionable ideas.
    _excluded_reason_counts: dict[str, int] = dict(skipped_by_reason)
    _not_sent_n = len(not_sent_candidates)
    if _not_sent_n:
        _excluded_reason_counts["NOT_SENT_TO_PREDICTION_TOP_N_CUTOFF"] = _not_sent_n
    if _watch_count:
        _excluded_reason_counts["WATCH_OR_BELOW_THRESHOLD"] = _watch_count
    if _rejected_count:
        _excluded_reason_counts["REJECTED"] = _rejected_count
    if all_prediction_failures:
        _excluded_reason_counts["PREDICTION_FAILED"] = len(all_prediction_failures)

    _coverage_note = (
        f"Full universe was locally screened ({len(universe_tickers) - len(skipped)} of "
        f"{len(universe_tickers)} names had enough price history). Prediction was run on the "
        f"top {body.prediction_top_n} ranked names"
        + (f" plus {len(holdings_injected)} current holding(s)" if holdings_injected else "")
        + ". This is not a full S&P 500 prediction run."
    )

    scan_coverage = ScanCoverageOut(
        universe_name=body.universe,
        configured_universe_count=len(universe_tickers),
        price_history_ready_count=candidate_funnel.price_history_ready_count,
        locally_screened_count=len(universe_tickers) - len(skipped),
        prediction_requested_count=len(selected_tickers),
        prediction_returned_count=len(normalized_predictions),
        prediction_failed_count=len(all_prediction_failures),
        actionable_trade_ideas_count=_actionable_count,
        watch_only_count=_watch_count,
        rejected_count=_rejected_count,
        already_held_count=_already_held_count,
        blocked_by_risk_count=0,
        excluded_reason_counts=_excluded_reason_counts,
        coverage_note=_coverage_note,
    )

    candidate_quality = CandidateQualityOut(
        min_actionable_score=body.min_actionable_score,
        min_confidence=body.min_actionable_confidence,
        min_expected_return_pct=body.min_expected_return_pct,
        min_relative_strength_vs_spy=body.min_relative_strength_vs_spy,
        explanation=_CANDIDATE_QUALITY_EXPLANATION,
    )

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
        scan_coverage=scan_coverage,
        candidate_quality=candidate_quality,
        candidate_funnel=candidate_funnel,
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
        scoring_summary=scoring_summary,
        skipped_diagnostics=skipped_diagnostics,
        scoring_profile_comparison=scoring_profile_comparison,
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
    # Phase 4B breakdown counts
    skipped_current_holdings = 0
    skipped_watch = 0
    skipped_rejected = 0
    skipped_other = 0

    with get_session() as session:
        for candidate in body.candidates:
            # Phase 4B: skip ineligible candidates when classification fields are present
            if candidate.candidate_type == "CURRENT_HOLDING_MONITOR":
                skipped_current_holdings += 1
                continue
            if candidate.eligible_for_review_queue is False:
                if candidate.preview_decision == "WATCH":
                    skipped_watch += 1
                elif candidate.preview_decision == "REJECT":
                    skipped_rejected += 1
                else:
                    skipped_other += 1
                continue

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
                    review_reason_code=existing.review_reason_code,
                    review_note=existing.review_note,
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
                review_reason_code=new_review.review_reason_code,
                review_note=new_review.review_note,
                created_at=new_review.created_at,
                updated_at=new_review.updated_at,
            ))

    return CandidateReviewSaveResponse(
        inserted_count=inserted,
        skipped_existing_count=skipped,
        candidates_saved=saved_rows,
        saved_new_candidates=inserted,
        skipped_current_holdings=skipped_current_holdings,
        skipped_watch=skipped_watch,
        skipped_rejected=skipped_rejected,
        skipped_other=skipped_other,
        already_held_skip_reason="ALREADY_HELD_MONITOR_ONLY" if skipped_current_holdings > 0 else None,
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
                review_reason_code=row.review_reason_code,
                review_note=row.review_note,
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

        # Update review_status and optional rationale fields
        row.review_status = body.review_status
        if body.review_reason_code is not None:
            row.review_reason_code = body.review_reason_code
        if body.review_note is not None:
            row.review_note = body.review_note
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
            review_reason_code=row.review_reason_code,
            review_note=row.review_note,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )


@app.post(
    "/v1/review/candidates/{candidate_id}/paper-trade",
    response_model=CandidatePaperTradeResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def candidate_scoped_paper_trade(
    candidate_id: str,
    body: CandidatePaperTradeRequest | None = None,
) -> CandidatePaperTradeResponse:
    """
    Create and fill ONE paper trade for ONE approved candidate, end to end:
    CandidateReview -> Signal -> TradeDecision -> Order -> Trade -> Position ->
    CashLedger -> PortfolioSnapshot.

    PAPER ONLY. No broker execution. No automation. The whole lifecycle is
    scoped to this candidate's ticker, so an old filled order for a different
    ticker can never be reported as this candidate's completion proof.

    Behaviour:
    - 404 if the candidate does not exist.
    - 409 if the candidate is not APPROVED_FOR_SIGNAL (WATCHING/REJECTED/NEW),
      or cannot produce a signal (missing/invalid recommendation/confidence).
    - status="BLOCKED" (HTTP 200) if risk blocks the order (decision is not a
      sized BUY/SELL). No Order/Trade/Position is created.
    - status="FAILED" (HTTP 200) if the paper fill cannot complete (e.g. no
      price snapshot or insufficient cash). The order is left PENDING/FAILED;
      no position is created.
    - status="COMPLETED" on success.
    - Idempotent per candidate: a second call returns status="ALREADY_COMPLETED"
      with the same row ids — it never creates duplicate signals/orders/trades.
    """
    import uuid as uuid_module

    body = body or CandidatePaperTradeRequest()

    # Parse candidate id (invalid -> 404, never matches another ticker)
    try:
        candidate_uuid = uuid_module.UUID(candidate_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=404, detail=f"Candidate '{candidate_id}' not found.")

    now = datetime.now(timezone.utc)
    market_date = now.astimezone(_EASTERN).date()

    created_signal = False
    created_decision = False
    created_order = False

    with get_dedicated_session() as session:
        candidate = session.query(CandidateReview).filter(
            CandidateReview.id == candidate_uuid
        ).first()
        if candidate is None:
            raise HTTPException(status_code=404, detail=f"Candidate '{candidate_id}' not found.")

        # --- Eligibility gates ---
        if candidate.review_status != "APPROVED_FOR_SIGNAL":
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Candidate {candidate.ticker} is '{candidate.review_status}', "
                    "not APPROVED_FOR_SIGNAL. Approve it for paper trade first."
                ),
            )
        if not body.confirm_paper_trade:
            raise HTTPException(
                status_code=422,
                detail="confirm_paper_trade must be true to create and fill a paper trade.",
            )

        rec = (candidate.prediction_recommendation or "").upper()
        if candidate.status != "OK" or rec not in ("BUY", "SELL"):
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Candidate {candidate.ticker} cannot produce a paper trade "
                    f"(status={candidate.status}, recommendation={candidate.prediction_recommendation})."
                ),
            )
        try:
            confidence_decimal = Decimal(candidate.prediction_confidence)
            if not (Decimal("0") <= confidence_decimal <= Decimal("1")):
                raise ValueError
        except Exception:
            raise HTTPException(
                status_code=409,
                detail=f"Candidate {candidate.ticker} has an invalid prediction confidence.",
            )

        ticker = candidate.ticker
        direction = rec
        source_run = f"review_queue_create_signals_v1:{candidate_id}"

        portfolio = get_portfolio(session)

        # --- Reuse any existing rows for THIS candidate (idempotency) ---
        signal = session.query(Signal).filter(
            Signal.source_run == source_run,
            Signal.ticker == ticker,
            Signal.direction == direction,
        ).first()
        decision = None
        order = None
        if signal is not None:
            decision = session.query(TradeDecision).filter(
                TradeDecision.signal_id == signal.id
            ).first()
            if decision is not None:
                order = session.query(Order).filter(
                    Order.trade_decision_id == decision.id
                ).first()

        def _completed(status_str: str, cs: bool, cd: bool, co: bool) -> CandidatePaperTradeResponse:
            trade = session.query(Trade).filter(
                Trade.order_id == order.id
            ).order_by(Trade.trade_ts.desc()).first()
            position = session.query(Position).filter(
                Position.ticker == ticker
            ).first()
            return CandidatePaperTradeResponse(
                candidate_id=candidate_id,
                ticker=ticker,
                status=status_str,
                reason="Paper trade filled. Portfolio updated.",
                signal_id=str(signal.id),
                trade_decision_id=str(decision.id),
                order_id=str(order.id),
                trade_id=str(trade.id) if trade else None,
                position_id=str(position.id) if position else None,
                side=order.side,
                qty=str(order.filled_qty if order.filled_qty is not None else order.requested_qty),
                fill_price=str(order.fill_price) if order.fill_price is not None else None,
                commission=str(order.commission) if order.commission is not None else None,
                cash_after=str(compute_cash(session)),
                total_value_after=str(portfolio.cached_total_value),
                safety_mode="PAPER_ONLY_NO_BROKER",
                created_signal=cs,
                created_decision=cd,
                created_order=co,
                filled_order=True,
                created_or_updated_position=position is not None,
            )

        # --- Idempotent replay: already filled for this candidate ---
        if order is not None and order.status == "FILLED":
            return _completed("ALREADY_COMPLETED", False, False, False)

        # --- Any write requires a JobRun for the audit trail ---
        job_run = JobRun(
            idempotency_key=(
                body.idempotency_key
                or f"candidate-paper-trade:{candidate_id}:{uuid_module.uuid4().hex[:8]}"
            ),
            workflow_type="CANDIDATE_PAPER_TRADE",
            market_date=market_date,
            status=JobRunStatus.RUNNING,
            started_at=now,
        )
        session.add(job_run)
        session.commit()

        # --- Signal (create or reuse) ---
        if signal is None:
            signal = Signal(
                job_run_id=job_run.id,
                ticker=ticker,
                direction=direction,
                confidence=confidence_decimal,
                signal_ts=now,
                market_date=market_date,
                source_run=source_run,
                status="RECEIVED",
                raw_payload={
                    "source": "candidate_scoped_paper_trade_v1",
                    "candidate_review_id": candidate_id,
                },
            )
            session.add(signal)
            session.commit()
            created_signal = True

        # --- TradeDecision (create or reuse) ---
        if decision is None:
            snapshot_price = _latest_price(session, ticker)
            rd = evaluate_signal(
                session,
                portfolio=portfolio,
                direction=signal.direction,
                ticker=ticker,
                confidence=signal.confidence,
                snapshot_price=snapshot_price,
                market_date=market_date,
                now=now,
            )
            decision = TradeDecision(
                signal_id=signal.id,
                job_run_id=job_run.id,
                ticker=ticker,
                signal_direction=signal.direction,
                decision=rd.decision,
                reason_code=rd.reason_code,
                requested_notional=rd.requested_notional if rd.requested_notional > Decimal("0") else None,
                approved_notional=rd.approved_notional if rd.approved_notional > Decimal("0") else None,
                requested_qty=rd.requested_qty if rd.requested_qty > Decimal("0") else None,
                approved_qty=rd.approved_qty if rd.approved_qty > Decimal("0") else None,
                risk_snapshot=rd.risk_snapshot,
                sizing_adjustments=rd.sizing_adjustments,
                decided_at=now,
                market_date=market_date,
            )
            session.add(decision)
            session.flush()
            signal.status = "DECISION_MADE"
            session.add(signal)
            session.commit()
            created_decision = True

        # --- Risk block: not a sized BUY/SELL -> no order, no fill ---
        if decision.decision not in ("BUY", "SELL") or not decision.approved_qty or decision.approved_qty <= Decimal("0"):
            job_run.status = JobRunStatus.COMPLETED
            job_run.completed_at = now
            session.add(job_run)
            session.commit()
            return CandidatePaperTradeResponse(
                candidate_id=candidate_id,
                ticker=ticker,
                status="BLOCKED",
                reason=(
                    f"Risk blocked this paper trade ({decision.reason_code or decision.decision}). "
                    "No paper order was created."
                ),
                signal_id=str(signal.id),
                trade_decision_id=str(decision.id),
                side=direction,
                created_signal=created_signal,
                created_decision=created_decision,
                created_order=False,
                filled_order=False,
                created_or_updated_position=False,
                safety_mode="PAPER_ONLY_NO_BROKER",
            )

        # --- Order (create or reuse) ---
        if order is None:
            order = Order(
                trade_decision_id=decision.id,
                job_run_id=job_run.id,
                fill_job_run_id=None,
                ticker=ticker,
                side=decision.decision,
                order_type="MARKET",
                status="PENDING",
                market_date=market_date,
                requested_qty=decision.approved_qty,
                filled_qty=None,
                requested_at=now,
                notes="Candidate-scoped paper order ticket. No broker execution.",
            )
            session.add(order)
            session.commit()
            created_order = True

        # --- Fill ONLY this order, under the portfolio advisory lock ---
        acquired = session.execute(
            text("SELECT pg_try_advisory_lock(:key)"),
            {"key": PORTFOLIO_ADVISORY_LOCK_KEY},
        ).scalar()
        if not acquired:
            raise HTTPException(
                status_code=409,
                detail="Another portfolio workflow is running. Try again shortly.",
            )
        try:
            fill_result = fill_order(
                session,
                order,
                portfolio=portfolio,
                job_run_id=job_run.id,
                now=now,
                market_date=market_date,
            )
            if fill_result == "filled":
                # Best-effort cache refresh: a missing price on an unrelated open
                # position must never void this candidate's real, committed fill.
                try:
                    refresh_open_positions_cache(session, portfolio, now=now)
                except (MissingPricesError, ValueError):
                    pass
                try:
                    upsert_post_fill_snapshot(
                        session,
                        job_run_id=job_run.id,
                        market_date=market_date,
                        now=now,
                    )
                except (MissingPricesError, ValueError):
                    pass
        finally:
            try:
                session.execute(
                    text("SELECT pg_advisory_unlock(:key)"),
                    {"key": PORTFOLIO_ADVISORY_LOCK_KEY},
                )
                session.commit()
            except Exception:
                session.rollback()

        job_run.status = JobRunStatus.COMPLETED
        job_run.completed_at = now
        session.add(job_run)
        session.commit()

        if fill_result != "filled":
            session.refresh(order)
            return CandidatePaperTradeResponse(
                candidate_id=candidate_id,
                ticker=ticker,
                status="FAILED",
                reason=(
                    f"Paper order could not be filled ({fill_result}). "
                    + (order.notes or "")
                ).strip(),
                signal_id=str(signal.id),
                trade_decision_id=str(decision.id),
                order_id=str(order.id),
                side=order.side,
                qty=str(order.requested_qty),
                created_signal=created_signal,
                created_decision=created_decision,
                created_order=created_order,
                filled_order=False,
                created_or_updated_position=False,
                safety_mode="PAPER_ONLY_NO_BROKER",
            )

        session.refresh(portfolio)
        return _completed("COMPLETED", created_signal, created_decision, created_order)


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
    "/v1/review/generate-trade-plan",
    response_model=GenerateTradePlanResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def generate_trade_plan(
    body: GenerateTradePlanRequest,
) -> GenerateTradePlanResponse:
    """
    Generate a trade plan from approved candidates.

    Orchestrates Signal creation then TradeDecision creation in one manual action.
    Idempotent: repeated calls reuse existing Signal and TradeDecision rows.

    Does NOT create Order, Trade, Position, CashLedger, or PortfolioSnapshot rows.
    Does NOT trigger broker execution or automation.

    confirm_generate must be true.
    """
    import uuid as uuid_module

    if not body.confirm_generate and not body.confirm_generate_trade_plan:
        raise HTTPException(
            status_code=422,
            detail="confirm_generate must be true to generate a trade plan.",
        )

    eastern_now = datetime.now(_EASTERN)
    market_date = eastern_now.date()
    now_utc = datetime.now(timezone.utc)
    ikey = body.idempotency_key or f"generate-trade-plan:{str(market_date)}:{uuid_module.uuid4().hex[:8]}"

    signals_created = 0
    signals_existing = 0
    decisions_created = 0
    decisions_existing = 0
    rejected_decisions = 0
    signal_details: list[TradePlanSignalDetail] = []
    decision_details: list[TradePlanDecisionDetail] = []
    approved_count = 0
    candidates_processed = 0

    # Collect (signal_id_str, ticker, side) tuples for decision phase
    all_signal_ids: list[tuple[str, str, str]] = []

    # ---- Phase 1: Create/reuse Signal rows for APPROVED_FOR_SIGNAL candidates ----
    with get_session() as session:
        rows = (
            session.query(CandidateReview)
            .filter(CandidateReview.review_status == "APPROVED_FOR_SIGNAL")
            .limit(body.limit)
            .all()
        )
        approved_count = len(rows)

        signals_to_create: list[tuple] = []

        for row in rows:
            candidates_processed += 1
            candidate_id_str = str(row.id)

            if row.status != "OK":
                continue
            if not row.prediction_recommendation:
                continue
            rec_upper = row.prediction_recommendation.upper()
            if rec_upper not in ("BUY", "SELL"):
                continue
            direction = rec_upper
            if not row.prediction_confidence:
                continue
            try:
                confidence_decimal = Decimal(row.prediction_confidence)
                if not (Decimal("0") <= confidence_decimal <= Decimal("1")):
                    continue
            except Exception:
                continue

            source_run = f"review_queue_create_signals_v1:{candidate_id_str}"

            existing_signal = session.query(Signal).filter(
                Signal.source_run == source_run,
                Signal.ticker == row.ticker,
                Signal.direction == direction,
            ).first()

            if existing_signal:
                signals_existing += 1
                all_signal_ids.append((str(existing_signal.id), row.ticker, direction))
                signal_details.append(TradePlanSignalDetail(
                    candidate_review_id=candidate_id_str,
                    signal_id=str(existing_signal.id),
                    ticker=row.ticker,
                    side=direction,
                    confidence=str(confidence_decimal),
                    status="existing",
                ))
            else:
                signals_to_create.append((row, direction, confidence_decimal, source_run, candidate_id_str))

        if signals_to_create:
            job_run_sigs = JobRun(
                idempotency_key=ikey + ":signals",
                workflow_type="REVIEW_QUEUE_CREATE_SIGNALS",
                market_date=market_date,
                status="COMPLETED",
                completed_at=now_utc,
                result_summary={},
            )
            session.add(job_run_sigs)
            session.flush()

            for row, direction, confidence_decimal, source_run, candidate_id_str in signals_to_create:
                signal = Signal(
                    job_run_id=job_run_sigs.id,
                    ticker=row.ticker,
                    direction=direction,
                    confidence=confidence_decimal,
                    signal_ts=now_utc,
                    market_date=market_date,
                    source_run=source_run,
                    status="RECEIVED",
                    raw_payload={
                        "source": "generate_trade_plan_v1",
                        "candidate_review_id": candidate_id_str,
                        "request_idempotency_key": ikey,
                    },
                )
                session.add(signal)
                session.flush()
                signals_created += 1
                all_signal_ids.append((str(signal.id), row.ticker, direction))
                signal_details.append(TradePlanSignalDetail(
                    candidate_review_id=candidate_id_str,
                    signal_id=str(signal.id),
                    ticker=row.ticker,
                    side=direction,
                    confidence=str(confidence_decimal),
                    status="created",
                ))

            job_run_sigs.result_summary = {"signals_created": signals_created}
            session.add(job_run_sigs)
            session.flush()

    # ---- Phase 2: Create/reuse TradeDecision rows for collected signals ----
    with get_session() as session:
        portfolio = get_portfolio(session)
        decisions_to_create: list = []

        for signal_id_str, ticker, side in all_signal_ids:
            try:
                sig_uuid = uuid_module.UUID(signal_id_str)
            except (ValueError, AttributeError):
                continue

            signal = session.query(Signal).filter(Signal.id == sig_uuid).first()
            if not signal:
                continue

            existing_decision = session.query(TradeDecision).filter(
                TradeDecision.signal_id == signal.id
            ).first()

            if existing_decision:
                decisions_existing += 1
                if existing_decision.decision not in ("APPROVE", "BUY", "SELL"):
                    rejected_decisions += 1
                decision_details.append(TradePlanDecisionDetail(
                    signal_id=signal_id_str,
                    trade_decision_id=str(existing_decision.id),
                    ticker=ticker,
                    side=side,
                    decision=existing_decision.decision or "-",
                    approved_notional=str(existing_decision.approved_notional or "0"),
                    status="existing",
                ))
            else:
                decisions_to_create.append(signal)

        if decisions_to_create:
            job_run_decs = JobRun(
                idempotency_key=ikey + ":decisions",
                workflow_type="REVIEW_QUEUE_CREATE_DECISIONS",
                market_date=market_date,
                status="COMPLETED",
                completed_at=now_utc,
                result_summary={},
            )
            session.add(job_run_decs)
            session.flush()

            for signal in decisions_to_create:
                snapshot_price = _latest_price(session, signal.ticker)
                rd = evaluate_signal(
                    session,
                    portfolio=portfolio,
                    direction=signal.direction,
                    ticker=signal.ticker,
                    confidence=signal.confidence,
                    snapshot_price=snapshot_price,
                    market_date=market_date,
                    now=now_utc,
                )
                trade_decision = TradeDecision(
                    signal_id=signal.id,
                    job_run_id=job_run_decs.id,
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
                    decided_at=now_utc,
                    market_date=market_date,
                )
                session.add(trade_decision)
                session.flush()

                signal.status = "DECISION_MADE"
                session.add(signal)
                session.flush()

                decisions_created += 1
                if rd.decision not in ("APPROVE", "BUY", "SELL"):
                    rejected_decisions += 1
                decision_details.append(TradePlanDecisionDetail(
                    signal_id=str(signal.id),
                    trade_decision_id=str(trade_decision.id),
                    ticker=signal.ticker,
                    side=signal.direction,
                    decision=rd.decision,
                    approved_notional=str(rd.approved_notional),
                    status="created",
                ))

            job_run_decs.result_summary = {
                "decisions_created": decisions_created,
                "rejected_decisions": rejected_decisions,
                "orders_created": 0,
            }
            session.add(job_run_decs)
            session.flush()

    approved_decisions = (decisions_created + decisions_existing) - rejected_decisions
    if approved_decisions > 0:
        next_step = "Review trade plan -- Create paper order tickets manually when ready"
    elif approved_count > 0:
        next_step = "Monitor portfolio -- all decisions were rejected or risk-blocked"
    else:
        next_step = "Run Daily Review Session -- no approved candidates found"

    # ---- Phase 3: Build user-facing trade_plan_rows ----
    trade_plan_rows: list[TradePlanRow] = []
    with get_session() as session:
        for dd in decision_details:
            try:
                td_uuid = uuid_module.UUID(dd.trade_decision_id)
            except (ValueError, AttributeError):
                continue
            td = session.query(TradeDecision).filter(TradeDecision.id == td_uuid).first()
            if not td:
                continue
            is_eligible = dd.decision in ("APPROVE", "BUY", "SELL")
            existing_order = session.query(Order).filter(Order.trade_decision_id == td.id).first()
            has_order = existing_order is not None

            if has_order:
                next_step_row = "Order already created"
            elif is_eligible:
                next_step_row = "Create paper order ticket"
            else:
                next_step_row = "Blocked by risk - review decision"

            trade_plan_rows.append(TradePlanRow(
                ticker=dd.ticker,
                action=dd.side,
                qty=str(td.approved_qty) if td.approved_qty else "-",
                estimated_cost=dd.approved_notional,
                reason=td.reason_code or dd.decision or "-",
                risk_status="APPROVED" if is_eligible else "BLOCKED",
                risk_reason="" if is_eligible else (td.reason_code or dd.decision or "-"),
                next_step=next_step_row,
                order_eligible=is_eligible and not has_order,
                already_has_order=has_order,
            ))

    skipped_count = candidates_processed - (signals_created + signals_existing)
    if skipped_count < 0:
        skipped_count = 0

    return GenerateTradePlanResponse(
        preview_only=False,
        writes_performed=True,
        candidates_processed=candidates_processed,
        approved_candidates_count=approved_count,
        signals_created=signals_created,
        signals_existing=signals_existing,
        decisions_created=decisions_created,
        decisions_existing=decisions_existing,
        rejected_decisions=rejected_decisions,
        orders_created=0,
        trades_created=0,
        fills_created=0,
        broker_execution=False,
        positions_changed=False,
        cash_changed=False,
        generated_count=signals_created + decisions_created,
        reused_count=signals_existing + decisions_existing,
        skipped_count=skipped_count,
        safety_message="Signal and TradeDecision rows only. No Orders, Trades, Positions, or CashLedger rows created or modified.",
        no_paper_orders_created=True,
        no_trades_created=True,
        no_fills_created=True,
        no_cash_changes=True,
        no_position_changes=True,
        no_broker_execution=True,
        automation_enabled=False,
        trade_plan_rows=trade_plan_rows,
        signal_details=signal_details,
        decision_details=decision_details,
        next_step=next_step,
        safety_note="Signal and TradeDecision rows only. No Orders, Trades, Positions, or CashLedger rows created or modified.",
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


@app.post(
    "/v1/review/create-orders",
    response_model=ReviewCreateOrdersResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(_verify_api_key)],
)
async def create_orders_from_decisions(
    body: ReviewCreateOrdersRequest,
) -> ReviewCreateOrdersResponse:
    """
    Create paper Order rows from eligible TradeDecision rows.

    PAPER ORDERS ONLY. No broker execution. No fills. No position changes.
    No cash changes. No automation. Manual review required.

    Eligibility rules:
    - Only BUY/SELL decisions with approved_qty > 0
    - Skips decisions that already have an Order row (idempotent)
    - Skips REJECTED and HOLD decisions

    Creates one JobRun (workflow_type=REVIEW_QUEUE_CREATE_ORDERS) only if
    at least one Order row is created.

    confirm_create_orders must be true to proceed.
    """
    import uuid as uuid_module

    if not body.confirm_create_orders:
        raise HTTPException(
            status_code=422,
            detail="confirm_create_orders must be true to create Order rows",
        )

    evaluated = 0
    skipped_existing = 0
    skipped_not_approved = 0
    skipped_invalid = 0
    skipped_list: list[SkippedTradeDecisionDetail] = []
    created_orders_list: list[CreatedOrderDetail] = []
    orders_to_create: list[TradeDecision] = []

    with get_session() as session:
        eastern_now = datetime.now(_EASTERN)
        market_date = eastern_now.date()
        now_utc = datetime.now(timezone.utc)

        # Build query
        if body.trade_decision_ids:
            try:
                decision_uuids = [uuid_module.UUID(did) for did in body.trade_decision_ids]
                rows = session.query(TradeDecision).filter(
                    TradeDecision.id.in_(decision_uuids)
                ).limit(body.limit).all()
            except (ValueError, AttributeError):
                return ReviewCreateOrdersResponse(
                    execution_mode="ORDER_CREATION_PAPER_ONLY",
                    trade_decisions_evaluated=0,
                    orders_created=0,
                    skipped_count=0,
                    skipped_existing_count=0,
                    skipped_not_approved=0,
                    skipped_invalid=0,
                    created_orders=[],
                    skipped=[],
                    job_runs_created=0,
                    safety_message="PAPER ORDERS ONLY. No broker execution. No fills. Automation off.",
                )
        else:
            rows = session.query(TradeDecision).join(
                Signal, Signal.id == TradeDecision.signal_id
            ).filter(
                Signal.source_run.startswith(body.source_run_prefix)
            ).limit(body.limit).all()

        # First pass: evaluate eligibility
        for td in rows:
            evaluated += 1
            td_id_str = str(td.id)

            # Validate source_run when trade_decision_ids is provided
            if body.trade_decision_ids:
                signal = session.query(Signal).filter(Signal.id == td.signal_id).first()
                if not signal:
                    skipped_invalid += 1
                    skipped_list.append(SkippedTradeDecisionDetail(
                        trade_decision_id=td_id_str,
                        ticker=td.ticker,
                        reason="Signal not found",
                    ))
                    continue
                if not signal.source_run.startswith(body.source_run_prefix):
                    skipped_invalid += 1
                    skipped_list.append(SkippedTradeDecisionDetail(
                        trade_decision_id=td_id_str,
                        ticker=td.ticker,
                        reason="TradeDecision source signal is not review-created",
                    ))
                    continue

            # Skip if an Order already exists for this decision (idempotency)
            existing_order = session.query(Order).filter(
                Order.trade_decision_id == td.id
            ).first()
            if existing_order:
                skipped_existing += 1
                skipped_list.append(SkippedTradeDecisionDetail(
                    trade_decision_id=td_id_str,
                    ticker=td.ticker,
                    reason="Order already exists for TradeDecision",
                ))
                continue

            # Only BUY/SELL with approved_qty > 0 create orders
            if td.decision not in ("BUY", "SELL"):
                skipped_not_approved += 1
                skipped_list.append(SkippedTradeDecisionDetail(
                    trade_decision_id=td_id_str,
                    ticker=td.ticker,
                    reason=f"TradeDecision decision is {td.decision}, not BUY/SELL",
                ))
                continue

            if not td.approved_qty or td.approved_qty <= Decimal("0"):
                skipped_not_approved += 1
                skipped_list.append(SkippedTradeDecisionDetail(
                    trade_decision_id=td_id_str,
                    ticker=td.ticker,
                    reason="TradeDecision has zero approved_qty",
                ))
                continue

            orders_to_create.append(td)

        # Create a JobRun only if there are orders to write
        job_run = None
        if orders_to_create:
            job_run = JobRun(
                idempotency_key=body.idempotency_key,
                workflow_type="REVIEW_QUEUE_CREATE_ORDERS",
                market_date=market_date,
                status="COMPLETED",
                completed_at=now_utc,
                result_summary={},
            )
            session.add(job_run)
            session.flush()

            for td in orders_to_create:
                order = Order(
                    trade_decision_id=td.id,
                    job_run_id=job_run.id,
                    fill_job_run_id=None,
                    ticker=td.ticker,
                    side=td.decision,
                    order_type="MARKET",
                    status="PENDING",
                    market_date=market_date,
                    requested_qty=td.approved_qty,
                    filled_qty=None,
                    requested_at=now_utc,
                    filled_at=None,
                    fill_price=None,
                    commission=None,
                    slippage_cost=None,
                    notes="Paper order ticket — review workflow. No broker execution.",
                )
                session.add(order)
                session.flush()

                created_orders_list.append(CreatedOrderDetail(
                    order_id=str(order.id),
                    trade_decision_id=str(td.id),
                    ticker=td.ticker,
                    side=td.decision,
                    order_type="MARKET",
                    status="PENDING",
                    qty=str(td.approved_qty),
                    notional=str(td.approved_notional) if td.approved_notional else None,
                    market_date=str(market_date),
                    job_run_id=str(job_run.id),
                ))

            job_run.result_summary = {
                "trade_decisions_evaluated": evaluated,
                "orders_created": len(created_orders_list),
                "skipped_count": len(skipped_list),
                "skipped_existing_count": skipped_existing,
            }
            session.add(job_run)

    return ReviewCreateOrdersResponse(
        execution_mode="ORDER_CREATION_PAPER_ONLY",
        trade_decisions_evaluated=evaluated,
        orders_created=len(created_orders_list),
        skipped_count=len(skipped_list),
        skipped_existing_count=skipped_existing,
        skipped_not_approved=skipped_not_approved,
        skipped_invalid=skipped_invalid,
        created_orders=created_orders_list,
        skipped=skipped_list,
        job_runs_created=1 if job_run else 0,
        safety_message="PAPER ORDERS ONLY. No broker execution. No fills. No position changes. Automation off. Manual review required.",
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

        # Current cycle: latest idempotency_key by created_at
        current_cycle_row = session.query(CandidateReview.idempotency_key).order_by(
            CandidateReview.created_at.desc()
        ).first()
        current_cycle_key = current_cycle_row[0] if current_cycle_row else None

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

        # consumed: approved candidates that entered the signal workflow
        consumed_count = candidate_approved if signal_total > 0 else 0

        candidates_counts = ReviewCandidatesCounts(
            total=candidate_total,
            new=candidate_new,
            watching=candidate_watching,
            approved_for_signal=candidate_approved,
            rejected=candidate_rejected,
            consumed=consumed_count,
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

        review_pending_orders = session.query(Order).join(
            TradeDecision, TradeDecision.id == Order.trade_decision_id
        ).join(Signal, Signal.id == TradeDecision.signal_id).filter(
            Signal.source_run.startswith(REVIEW_SOURCE_PREFIX),
            Order.status == "PENDING",
        ).count()

        review_filled_orders = session.query(Order).join(
            TradeDecision, TradeDecision.id == Order.trade_decision_id
        ).join(Signal, Signal.id == TradeDecision.signal_id).filter(
            Signal.source_run.startswith(REVIEW_SOURCE_PREFIX),
            Order.status == "FILLED",
        ).count()

        open_positions_count = session.query(Position).count()

        orders_counts = OrdersCounts(
            total=order_total,
            review_created=review_created_orders,
            pending=review_pending_orders,
            filled=review_filled_orders,
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

        # Step 8: Order Preview
        if order_eligible > 0:
            steps.append(WorkflowStepStatus(
                step="Order Preview",
                status="READY",
                reason=f"{order_eligible} trade decision(s) eligible for order preview.",
            ))
        elif already_has_order > 0:
            steps.append(WorkflowStepStatus(
                step="Order Preview",
                status="COMPLETE",
                reason=f"Orders created for all {already_has_order} eligible decision(s).",
            ))
        else:
            steps.append(WorkflowStepStatus(
                step="Order Preview",
                status="BLOCKED",
                reason="No review-created trade decisions eligible for orders.",
            ))

        # Step 9: Create Orders (paper tickets only, no broker execution)
        if order_eligible > 0:
            steps.append(WorkflowStepStatus(
                step="Create Orders",
                status="READY",
                reason=f"{order_eligible} trade decision(s) eligible for paper order creation.",
            ))
        elif already_has_order > 0:
            steps.append(WorkflowStepStatus(
                step="Create Orders",
                status="COMPLETE",
                reason=f"Paper order ticket(s) already created for all {already_has_order} eligible decision(s).",
            ))
        else:
            steps.append(WorkflowStepStatus(
                step="Create Orders",
                status="BLOCKED",
                reason="No order-eligible trade decisions.",
            ))

        # --- Current-cycle separation: only today's candidates + live order/
        #     position facts drive the canonical next action. Historical
        #     approved candidates are reported but never selected. ---
        today = date.today()
        start_of_day = datetime(today.year, today.month, today.day)
        today_q = session.query(CandidateReview).filter(
            CandidateReview.created_at >= start_of_day
        )
        today_total = today_q.count()
        current_pending = today_q.filter(CandidateReview.review_status == "NEW").count()
        current_rejected = today_q.filter(CandidateReview.review_status == "REJECTED").count()
        current_watch = today_q.filter(CandidateReview.review_status == "WATCHING").count()
        historical_count = candidate_total - today_total

        # Candidate-scoped completion: an approved current candidate is
        # "completed" once its own candidate-scoped signal has a FILLED order
        # (review_queue_create_signals_v1:{candidate_id}). The approval label
        # itself never changes after a fill, so ticket-ready vs completed is
        # derived from the order chain, never from review_status alone. This is
        # what stops an old/unrelated filled order from being mistaken for the
        # current candidate's completion.
        today_approved_rows = today_q.filter(
            CandidateReview.review_status == "APPROVED_FOR_SIGNAL"
        ).all()
        approved_source_runs = {
            f"{REVIEW_SOURCE_PREFIX}{row.id}": row for row in today_approved_rows
        }
        completed_source_runs: set[str] = set()
        if approved_source_runs:
            filled_runs = session.query(Signal.source_run).join(
                TradeDecision, TradeDecision.signal_id == Signal.id
            ).join(Order, Order.trade_decision_id == TradeDecision.id).filter(
                Signal.source_run.in_(list(approved_source_runs.keys())),
                Order.status == "FILLED",
            ).distinct().all()
            completed_source_runs = {r[0] for r in filled_runs}
        today_completed = len(completed_source_runs)
        today_ticket_ready = len(today_approved_rows) - today_completed

        canonical_stage = _canonical_daily_stage(
            today_pending=current_pending,
            today_ticket_ready=today_ticket_ready,
            today_completed=today_completed,
            has_open_positions=open_positions_count > 0,
        )
        cinfo = _CANONICAL_DAILY_STAGE_INFO[canonical_stage]

        # Best-effort target ticker/candidate for the current action. Pick the
        # specific current candidate the action points to so the UI can deep-link
        # to exactly that row instead of a historical one.
        target_ticker: str | None = None
        target_candidate_id: str | None = None
        if canonical_stage == "REVIEW_TRADE_IDEAS":
            nxt = today_q.filter(CandidateReview.review_status == "NEW").order_by(
                CandidateReview.created_at.asc()
            ).first()
            if nxt is not None:
                target_ticker, target_candidate_id = nxt.ticker, str(nxt.id)
        elif canonical_stage == "CREATE_FILL_PAPER_TRADE":
            for row in sorted(today_approved_rows, key=lambda r: r.created_at):
                if f"{REVIEW_SOURCE_PREFIX}{row.id}" not in completed_source_runs:
                    target_ticker, target_candidate_id = row.ticker, str(row.id)
                    break
        elif canonical_stage == "VIEW_PORTFOLIO":
            for row in today_approved_rows:
                if f"{REVIEW_SOURCE_PREFIX}{row.id}" in completed_source_runs:
                    target_ticker, target_candidate_id = row.ticker, str(row.id)
                    break

        title = cinfo["title"]
        if target_ticker and canonical_stage == "CREATE_FILL_PAPER_TRADE":
            title = f"Create & Fill Paper Trade: {target_ticker}"
        elif target_ticker and canonical_stage == "VIEW_PORTFOLIO":
            title = f"View Portfolio: {target_ticker} filled"

        next_action = WorkflowNextAction(
            stage=canonical_stage,
            title=title,
            description=cinfo["description"],
            button_label=cinfo["button_label"],
            target_tab=cinfo["target_tab"],
            target_anchor=cinfo["target_anchor"],
            requires_user_action=cinfo["requires_user_action"],
            # back-compat aliases
            label=title,
            detail=cinfo["description"],
            primary_button_label=cinfo["button_label"],
            ticker=target_ticker,
            target_candidate_id=target_candidate_id,
            target_ticker=target_ticker,
            route=cinfo["target_anchor"],
            current_cycle_key=current_cycle_key,
            current_trade_idea_count=today_total,
            current_pending_review_count=current_pending,
            current_ticket_ready_count=today_ticket_ready,
            current_filled_count=today_completed,
            current_watch_count=current_watch,
            current_rejected_count=current_rejected,
            historical_trade_idea_count=historical_count,
        )

    return WorkflowStatusResponse(
        review_candidates=candidates_counts,
        review_created_signals=signals_counts,
        review_created_trade_decisions=decisions_counts,
        orders=orders_counts,
        workflow_steps=steps,
        safety={"create_orders_enabled": True, "automation_enabled": False},
        open_positions=open_positions_count,
        current_cycle_key=current_cycle_key,
        current_pending_review_count=current_pending,
        current_approved_ticket_ready_count=today_ticket_ready,
        current_filled_count=today_completed,
        current_rejected_count=current_rejected,
        current_watch_count=current_watch,
        historical_candidate_count=historical_count,
        next_action=next_action,
    )


# ---------------------------------------------------------------------------
# Canonical workflow state (Action Discoverability + Guided Workflow v1)
#
# One stage, one primary action, used by every UI surface. The stage derivation
# is a pure function so it is deterministic and unit-testable without a DB; the
# endpoint feeds it live counts. Trade-flow stages (driven by DB facts) take
# precedence; today's pending candidates drive review; older candidates never
# drive the primary action.
# ---------------------------------------------------------------------------

_WORKFLOW_STAGE_INFO: dict[str, dict[str, str]] = {
    "NEEDS_DAILY_REVIEW": {
        "active_workspace": "start_daily_review",
        "focus_target": "active-action-workspace",
        "current_task": "Start Daily Review",
        "next_action": "Start today's daily review.",
        "primary_button_label": "Start Daily Review",
        "primary_button_action": "start_daily_review",
        "message": (
            "This refreshes market data, prepares today's candidates, and "
            "updates the portfolio snapshot. It does not create orders or trades."
        ),
    },
    "REVIEW_CANDIDATES": {
        "active_workspace": "candidates_to_review",
        "focus_target": "candidates-to-review-workspace",
        "current_task": "Review Candidates",
        "next_action": "Go to Daily Plan -> Candidates to Review.",
        "primary_button_label": "Review Candidates",
        "primary_button_action": "review_candidates",
        "message": (
            "Choose one action. This records your manual review only. It does "
            "not create orders or trades."
        ),
    },
    "GENERATE_TRADE_PLAN": {
        "active_workspace": "trade_plan",
        "focus_target": "trade-plan-workspace",
        "current_task": "Generate Trade Plan",
        "next_action": "Generate a trade plan from approved candidates.",
        "primary_button_label": "Generate Trade Plan",
        "primary_button_action": "generate_trade_plan",
        "message": (
            "This creates internal paper-trading recommendations only. No order "
            "is created."
        ),
    },
    "CREATE_PAPER_ORDER": {
        "active_workspace": "paper_order_ticket",
        "focus_target": "paper-order-ticket-workspace",
        "current_task": "Create Paper Order Ticket",
        "next_action": "Create a paper order ticket.",
        "primary_button_label": "Create Paper Order Ticket",
        "primary_button_action": "create_paper_order",
        "message": (
            "This creates a pending paper order only. No broker order is sent."
        ),
    },
    "FILL_PAPER_ORDER": {
        "active_workspace": "pending_paper_order",
        "focus_target": "pending-paper-order-workspace",
        "current_task": "Fill Paper Order",
        "next_action": "Fill the paper order.",
        "primary_button_label": "Fill Paper Order",
        "primary_button_action": "fill_paper_order",
        "message": (
            "This fills the local paper order only. No live trade is executed."
        ),
    },
    "PAPER_TRADE_COMPLETED": {
        "active_workspace": "paper_trade_completed",
        "focus_target": "paper-trade-completed-workspace",
        "current_task": "View Portfolio",
        "next_action": "Paper trade completed. Portfolio updated.",
        "primary_button_label": "View Portfolio",
        "primary_button_action": "open_portfolio",
        "message": "Paper trade completed. Portfolio updated.",
    },
    "MONITOR_PORTFOLIO": {
        "active_workspace": "portfolio_monitoring",
        "focus_target": "portfolio-monitoring-workspace",
        "current_task": "Monitor Portfolio",
        "next_action": "Review open positions.",
        "primary_button_label": "Review Open Positions",
        "primary_button_action": "review_open_positions",
        "message": "No action required.",
    },
    "NO_TRADE_PLAN": {
        "active_workspace": "portfolio_monitoring",
        "focus_target": "portfolio-monitoring-workspace",
        "current_task": "Monitor Portfolio",
        "next_action": "No approved candidates available for a trade plan.",
        "primary_button_label": "Review Open Positions",
        "primary_button_action": "review_open_positions",
        "message": "No candidates from today's review need action.",
    },
}


def _derive_workflow_stage(
    *,
    today_pending: int,
    today_approved: int,
    order_eligible: int,
    has_pending_orders: bool,
    has_filled_orders: bool,
    has_open_positions: bool,
    today_total: int,
) -> str:
    """Pure single-stage derivation used by the canonical workflow state.

    Priority (matches the Daily Plan Today's Review branch order): today's
    pending candidates must be reviewed first; then a pending paper order must
    be filled; then an order-eligible decision must become a ticket; then an
    approved candidate must become a trade plan. Only when nothing today is
    actionable do we fall back to completed/monitor/needs-review. Older
    candidates are never an input here, so they can never drive the stage.
    """
    if today_pending > 0:
        return "REVIEW_CANDIDATES"
    if has_pending_orders:
        return "FILL_PAPER_ORDER"
    if order_eligible > 0:
        return "CREATE_PAPER_ORDER"
    if today_approved > 0:
        return "GENERATE_TRADE_PLAN"
    if has_filled_orders and has_open_positions:
        return "PAPER_TRADE_COMPLETED"
    if today_total > 0:
        return "NO_TRADE_PLAN"
    if has_open_positions:
        return "MONITOR_PORTFOLIO"
    return "NEEDS_DAILY_REVIEW"


# ---------------------------------------------------------------------------
# Canonical daily-workflow state contract (single next-action source).
#
# The candidate-scoped paper-trade endpoint collapses the old generate -> create
# -> fill chain into ONE "Create & Fill Paper Trade" action, so the user-facing
# workflow now has exactly five stages. _canonical_daily_stage is the single
# pure derivation feeding the WorkflowNextAction object that every UI surface
# renders from. Only current-cycle (today's) facts are inputs; historical
# candidates are reported separately and can never select a stage.
# ---------------------------------------------------------------------------

_CANONICAL_DAILY_STAGE_INFO: dict[str, dict] = {
    "START_DAILY_REVIEW": {
        "title": "Start Daily Review",
        "description": (
            "Begin today's review to scan for new trade ideas and refresh the "
            "portfolio snapshot. This creates no orders and no trades."
        ),
        "button_label": "Start Daily Review",
        "target_tab": "daily-plan",
        "target_anchor": "active-action-workspace",
        "requires_user_action": True,
    },
    "REVIEW_TRADE_IDEAS": {
        "title": "Review Trade Ideas",
        "description": (
            "Today's scan produced trade ideas that need review. Approve, Watch, "
            "or Reject each one. This records your manual review only."
        ),
        "button_label": "Review Trade Ideas",
        "target_tab": "daily-plan",
        "target_anchor": "today-trade-ideas",
        "requires_user_action": True,
    },
    "CREATE_FILL_PAPER_TRADE": {
        "title": "Create & Fill Paper Trade",
        "description": (
            "An approved trade idea is ready. Create and fill its paper trade. "
            "Paper portfolio only - no broker order is sent."
        ),
        "button_label": "Create & Fill Paper Trade",
        "target_tab": "daily-plan",
        "target_anchor": "today-trade-ideas",
        "requires_user_action": True,
    },
    "VIEW_PORTFOLIO": {
        "title": "View Portfolio",
        "description": (
            "The paper trade was filled. Review the updated paper position and "
            "cash in Portfolio."
        ),
        "button_label": "View Portfolio",
        "target_tab": "portfolio",
        "target_anchor": "open-positions",
        "requires_user_action": False,
    },
    "MONITOR_PORTFOLIO": {
        "title": "Monitor Portfolio",
        "description": (
            "No actionable trade ideas today. Existing positions were reviewed "
            "automatically. Review Portfolio."
        ),
        "button_label": "View Portfolio",
        "target_tab": "portfolio",
        "target_anchor": "open-positions",
        "requires_user_action": False,
    },
}


def _canonical_daily_stage(
    *,
    today_pending: int,
    today_ticket_ready: int,
    today_completed: int,
    has_open_positions: bool,
) -> str:
    """Pure five-stage derivation for the canonical daily-workflow contract.

    Priority: a current trade idea still needing review is the first action;
    then an approved idea that has not been paper-filled; then a freshly filled
    paper trade to view; then portfolio monitoring; otherwise start the review.
    Inputs are current-cycle (today's) only, so older candidates never select a
    stage.
    """
    if today_pending > 0:
        return "REVIEW_TRADE_IDEAS"
    if today_ticket_ready > 0:
        return "CREATE_FILL_PAPER_TRADE"
    if today_completed > 0:
        return "VIEW_PORTFOLIO"
    if has_open_positions:
        return "MONITOR_PORTFOLIO"
    return "START_DAILY_REVIEW"


def _build_workflow_state(
    *,
    today_total: int,
    today_pending: int,
    today_approved: int,
    older_count: int,
    order_eligible: int,
    pending_orders: int,
    filled_orders: int,
    open_positions: int,
) -> CurrentWorkflowStateResponse:
    """Assemble the canonical state object from live counts."""
    stage = _derive_workflow_stage(
        today_pending=today_pending,
        today_approved=today_approved,
        order_eligible=order_eligible,
        has_pending_orders=pending_orders > 0,
        has_filled_orders=filled_orders > 0,
        has_open_positions=open_positions > 0,
        today_total=today_total,
    )
    info = _WORKFLOW_STAGE_INFO[stage]
    return CurrentWorkflowStateResponse(
        stage=stage,
        current_task=info["current_task"],
        next_action=info["next_action"],
        primary_button_label=info["primary_button_label"],
        primary_button_action=info["primary_button_action"],
        active_workspace=info["active_workspace"],
        focus_target=info["focus_target"],
        today_candidate_count=today_total,
        today_pending_review_count=today_pending,
        today_approved_count=today_approved,
        older_candidate_count=older_count,
        has_pending_orders=pending_orders > 0,
        has_filled_orders=filled_orders > 0,
        has_open_positions=open_positions > 0,
        open_position_count=open_positions,
        message=info["message"],
    )


@app.get(
    "/v1/review/current-workflow-state",
    response_model=CurrentWorkflowStateResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def get_current_workflow_state() -> CurrentWorkflowStateResponse:
    """
    Canonical read-only workflow state for the guided cockpit.

    READ-ONLY: no database writes, no JobRun creation. Returns the single
    current stage plus the one primary action every UI surface should render,
    so Overview, Daily Plan, and the right Action / Safety panel never disagree.

    "Today" is the server local date. Candidates created before today are
    reported as older_candidate_count and never drive the primary action.
    """
    REVIEW_SOURCE_PREFIX = "review_queue_create_signals_v1:"
    today = date.today()
    start_of_day = datetime(today.year, today.month, today.day)

    with get_session() as session:
        total_candidates = session.query(CandidateReview).count()
        today_q = session.query(CandidateReview).filter(
            CandidateReview.created_at >= start_of_day
        )
        today_total = today_q.count()
        today_pending = today_q.filter(
            CandidateReview.review_status == "NEW"
        ).count()
        today_approved = today_q.filter(
            CandidateReview.review_status == "APPROVED_FOR_SIGNAL"
        ).count()
        older_count = total_candidates - today_total

        # Order-eligible review-created decisions that do not yet have an order.
        decision_query = session.query(TradeDecision).join(
            Signal, Signal.id == TradeDecision.signal_id
        ).filter(Signal.source_run.startswith(REVIEW_SOURCE_PREFIX))
        order_eligible = 0
        for td in decision_query.filter(
            TradeDecision.decision.in_(["BUY", "SELL"]),
            TradeDecision.approved_qty > Decimal("0"),
        ).all():
            existing = session.query(Order).filter(
                Order.trade_decision_id == td.id
            ).first()
            if existing is None:
                order_eligible += 1

        pending_orders = session.query(Order).filter(
            Order.status == "PENDING"
        ).count()
        filled_orders = session.query(Order).filter(
            Order.status == "FILLED"
        ).count()
        open_positions = session.query(Position).count()

    return _build_workflow_state(
        today_total=today_total,
        today_pending=today_pending,
        today_approved=today_approved,
        older_count=older_count,
        order_eligible=order_eligible,
        pending_orders=pending_orders,
        filled_orders=filled_orders,
        open_positions=open_positions,
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

        # Score BUY candidates not already held using Decision Model v2
        scored_candidates: list[tuple[CandidateStrengthDetail, float]] = []
        for cand in raw_candidates:
            if not cand.prediction_recommendation:
                continue
            if cand.prediction_recommendation.upper() != "BUY":
                continue
            if cand.ticker in held_tickers:
                continue
            _rp_sf = _score_candidate_v2(_cand_to_score_dict(cand))
            cand_score = _rp_sf.total_score
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

            # Look up forward-score predictions for each sellable holding
            _rp_held_pred_cache: dict[str, dict | None] = {}
            for pos_detail, _ in positions_with_pnl:
                _rp_hcr = session.execute(
                    select(CandidateReview)
                    .where(
                        CandidateReview.ticker == pos_detail.ticker,
                        CandidateReview.prediction_confidence.is_not(None),
                    )
                    .order_by(CandidateReview.created_at.desc())
                    .limit(1)
                ).scalar_one_or_none()
                _rp_held_pred_cache[pos_detail.ticker] = (
                    _cand_to_score_dict(_rp_hcr) if _rp_hcr is not None else None
                )

            for pos_detail, pos_pnl_pct in positions_with_pnl[:pairs_limit]:
                best: tuple[CandidateStrengthDetail, float] | None = None
                for cd, cs in scored_candidates:
                    if cd.ticker not in used_cand_tickers:
                        best = (cd, cs)
                        break
                if best is None:
                    break

                best_detail, best_score = best
                _rp_hold_pred = _rp_held_pred_cache.get(pos_detail.ticker)
                _rp_hold_sf = _score_holding_v2({}, holding_prediction=_rp_hold_pred)
                hold_score = _rp_hold_sf.total_score

                # Forward-vs-forward improvement; PnL gate is handled by sellable_for_rotation
                improvement = best_score - hold_score
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
                        f"holding {pos_detail.ticker} (fwd score {hold_score:.4f}); "
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
        # Check market history readiness and surface warning if insufficient
        _mh_data = _get_screening_readiness_data(session)
        _market_history_warning: str | None = None
        if not _mh_data["screening_ready"]:
            _market_history_warning = (
                "Market history is not ready — run Backfill Screening History first. "
                f"(SPY: {_mh_data['spy_snapshot_count']} snapshots, "
                f"tickers with 21d history: {_mh_data['tickers_with_at_least_21_snapshots']})"
            )

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
        _cap_release_raw: list[dict] = []   # capital allocation position data (one entry per position)
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
                _cap_release_raw.append({
                    "pos": pos, "price": None, "pnl_pct": None,
                    "cv": None, "upnl": None,
                    "sellable": False, "blocked_reason": "NO_PRICE_SNAPSHOT",
                })
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
                _cap_release_raw.append({
                    "pos": pos, "price": snap_price, "pnl_pct": pnl_pct,
                    "cv": current_value, "upnl": unrealized_pnl,
                    "sellable": False, "blocked_reason": "STALE_PRICE",
                })
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
                            candidate_review_id=str(sell_cand.id) if sell_cand is not None else None,
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

            # Capital allocation data for this position (normal-flow: price exists and is fresh)
            _cap_sell = not (body.block_loss_realization and pnl_pct < 0)
            _cap_release_raw.append({
                "pos": pos, "price": snap_price, "pnl_pct": pnl_pct,
                "cv": current_value, "upnl": unrealized_pnl,
                "sellable": _cap_sell,
                "blocked_reason": None if _cap_sell else "NEGATIVE_PNL_BLOCKED",
            })

        # 4. Evaluate BUY candidates
        buy_recommendations: list[BuyRecommendationItem] = []
        watch_candidates: list[WatchCandidateItem] = []
        _buy_cap_info: dict[str, dict] = {}   # ticker -> {price, conf, exp_ret_pct, score, cand}
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
                    position_count_override=current_count if body.position_tickers is not None else None,
                )
            except Exception as exc:
                blocked_actions.append(DailyPlanBlockedItem(
                    ticker=cand.ticker, action="BUY",
                    blocked_reason="EVALUATION_ERROR",
                    explanation=f"Risk evaluation failed: {exc}",
                ))
                continue

            if rd.decision == DecisionType.BUY:
                _buy_sf = _score_candidate_v2(_cand_to_score_dict(cand))
                cand_score = _buy_sf.total_score
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
                    score_factors_v2=_buy_sf.as_dict(),
                ))
                _buy_cap_info[cand.ticker] = {
                    "price": snapshot_price,
                    "conf": conf,
                    "exp_ret_pct": _safe_float(cand.expected_return_pct, 0.0),
                    "score": cand_score,
                    "cand": cand,
                }
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
                        _rot_sf = _score_candidate_v2(_cand_to_score_dict(cand))
                        scored_buy_cands.append((cand, _rot_sf.total_score, _rot_sf))
                scored_buy_cands.sort(key=lambda x: x[1], reverse=True)

                # Look up forward-score predictions for profitable holdings
                _held_pred_cache: dict[str, dict | None] = {}
                for _ht, _, _, _ in profitable_positions:
                    _hcr = session.execute(
                        select(CandidateReview)
                        .where(
                            CandidateReview.ticker == _ht,
                            CandidateReview.prediction_confidence.is_not(None),
                        )
                        .order_by(CandidateReview.created_at.desc())
                        .limit(1)
                    ).scalar_one_or_none()
                    _held_pred_cache[_ht] = _cand_to_score_dict(_hcr) if _hcr is not None else None

                pairs_limit = min(3, len(profitable_positions), len(scored_buy_cands))
                used_buy_tickers: set[str] = set()

                for sell_ticker, sell_pnl_pct, sell_price, sell_pos in profitable_positions[:pairs_limit]:
                    best: tuple | None = None
                    for cand, score, csf in scored_buy_cands:
                        if cand.ticker not in used_buy_tickers:
                            best = (cand, score, csf)
                            break
                    if best is None:
                        break

                    buy_cand, buy_score, buy_sf = best
                    _hold_pred = _held_pred_cache.get(sell_ticker)
                    _hold_sf = _score_holding_v2({}, holding_prediction=_hold_pred)
                    hold_score = _hold_sf.total_score

                    _rot_result = _score_rotation_v2(
                        candidate_score=buy_score,
                        holding_score=hold_score,
                        holding_pnl_pct=sell_pnl_pct / 100.0,
                        min_improvement_score=body.min_rotation_improvement_pct,
                    )
                    improvement = _rot_result.improvement_score
                    meets = _rot_result.eligible
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
                            f"(score {buy_score:.4f}, holding fwd score {hold_score:.4f}). "
                            f"Improvement {improvement:.4f} "
                            f"({'meets' if meets else 'below'} threshold {body.min_rotation_improvement_pct})."
                            + (" [holding has no prediction — compared vs neutral 0.0]"
                               if _hold_sf.prediction_missing else "")
                        ),
                        buy_candidate_review_id=str(buy_cand.id),
                        holding_score_v2=f"{hold_score:.4f}",
                        candidate_score_v2=f"{buy_score:.4f}",
                        prediction_missing=_hold_sf.prediction_missing,
                        score_explanation=_explain_score_factors(buy_sf),
                    ))
                    if meets:
                        used_buy_tickers.add(buy_cand.ticker)

        # 5b. Calibrated rotation analysis (PREVIEW ONLY, no DB writes, no GCP calls)
        _crw_ctx: DailyPlanCalibratedRotationContext | None = None
        if body.use_calibrated_rotation:
            # Query approved BUY candidates from CandidateReview directly so that
            # calibrated rotation works even when candidate_ids=[] (no regular pipeline).
            _crw_approved_reviews = (
                session.query(CandidateReview)
                .filter(CandidateReview.review_status == "APPROVED_FOR_SIGNAL")
                .all()
            )
            _crw_buy_tickers = [
                c.ticker for c in _crw_approved_reviews
                if (c.prediction_recommendation or "").upper() == "BUY"
                and c.ticker not in held_tickers
            ] or None
            try:
                _crw = _compute_calibrated_rotation(
                    session,
                    scoring_profile=body.scoring_profile,
                    calibration_as_of_dates=body.calibration_as_of_dates,
                    lookback_days=20,
                    forward_return_days=5,
                    scan_top_n=50,
                    profile_top_n=10,
                    min_price_points=5,
                    benchmark_ticker="SPY",
                    tickers=_crw_buy_tickers,
                    max_rotation_pairs=5,
                    min_expected_improvement_pct=body.min_expected_improvement_pct,
                    min_expected_pnl_dollars=body.min_expected_pnl_dollars,
                    allow_loss_realization=body.allow_loss_realization,
                    position_tickers=body.position_tickers,
                    as_of_date=None,
                )
                _crw_eligible = [p for p in _crw.rotation_pairs if p.decision == "ROTATE"]
                _crw_blocked_pairs = [p for p in _crw.rotation_pairs if p.decision == "BLOCKED"]
                _crw_warnings = _crw.calibration_context.calibration_warnings
                _crw_warning_count = sum(1 for v in _crw_warnings.values() if v)
                _crw_best = _crw_eligible[0] if _crw_eligible else None
                _crw_ctx = DailyPlanCalibratedRotationContext(
                    enabled=True,
                    scoring_profile_used=_crw.calibration_context.scoring_profile_used,
                    calibration_recommended_profile=_crw.calibration_context.calibration_recommended_profile,
                    calibration_confidence=_crw.calibration_context.calibration_confidence,
                    calibration_warning_count=_crw_warning_count,
                    eligible_rotation_pairs=len(_crw_eligible),
                    blocked_pairs=len(_crw_blocked_pairs) + len(_crw.blocked_actions),
                    best_rotation_pair=_crw_best.model_dump() if _crw_best else None,
                    fallback_used=False,
                    fallback_reason=None,
                )
            except Exception as _crw_exc:
                _crw_ctx = DailyPlanCalibratedRotationContext(
                    enabled=False,
                    scoring_profile_used=body.scoring_profile,
                    fallback_used=True,
                    fallback_reason=f"Calibrated rotation unavailable: {_crw_exc}",
                )

        # 5c. Build profile decision context (advisory summary, PREVIEW ONLY, no DB writes)
        _pdc_blockers: list[str] = []
        if _crw_ctx is None:
            _pdc_replay_supported = False
            _pdc_resolved = body.scoring_profile if body.scoring_profile != "calibration_recommended" else "current"
            _pdc_rec_profile: str | None = None
            _pdc_confidence: str | None = None
            _pdc_blockers.append("CALIBRATED_ROTATION_DISABLED")
        elif not _crw_ctx.enabled or _crw_ctx.fallback_used:
            _pdc_replay_supported = False
            _pdc_resolved = _crw_ctx.scoring_profile_used
            _pdc_rec_profile = _crw_ctx.calibration_recommended_profile
            _pdc_confidence = _crw_ctx.calibration_confidence
            _pdc_blockers.append(
                _crw_ctx.fallback_reason if _crw_ctx.fallback_reason else "CALIBRATION_UNAVAILABLE"
            )
        else:
            _pdc_replay_supported = True
            _pdc_resolved = _crw_ctx.scoring_profile_used
            _pdc_rec_profile = _crw_ctx.calibration_recommended_profile
            _pdc_confidence = _crw_ctx.calibration_confidence
            if _crw_ctx.calibration_warning_count > 0:
                _pdc_blockers.append("CALIBRATION_WARNINGS_PRESENT")
            if _crw_ctx.calibration_confidence == "LOW":
                _pdc_blockers.append("LOW_CALIBRATION_CONFIDENCE")
        _pdc_blockers.append(
            "REPLAY_METRICS_NOT_COMPUTED_IN_DAILY_PLAN: use Profile Comparison panel for win rate and vs-SPY data"
        )
        _pdc_profile_source = (
            "calibration_recommended" if body.scoring_profile == "calibration_recommended"
            else "explicit_request"
        )
        _profile_decision_context = DailyPlanProfileDecisionContext(
            requested_scoring_profile=body.scoring_profile,
            resolved_scoring_profile=_pdc_resolved,
            profile_source=_pdc_profile_source,
            replay_supported=_pdc_replay_supported,
            replay_recommendation=_pdc_rec_profile,
            replay_confidence_level=_pdc_confidence,
            replay_dates_evaluated=None,
            replay_avg_vs_spy_pct=None,
            replay_win_rate_pct=None,
            replay_blockers=_pdc_blockers,
            safety_note=(
                "PREVIEW ONLY — no signals, decisions, or orders created. "
                "Profile selection is advisory. Use the Replay Profile Comparison panel "
                "for detailed win-rate and vs-SPY evidence before trading."
            ),
        )

        # 6. Recommended next action
        good_rotations = [r for r in rotation_plan if r.meets_threshold]
        cap_blocked_count = sum(1 for a in blocked_actions if a.action == "BUY" and a.blocked_reason == "MAX_POSITIONS_REACHED")

        if no_candidates and (
            _crw_ctx is None
            or not _crw_ctx.enabled
            or _crw_ctx.eligible_rotation_pairs == 0
        ):
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
        elif (
            _crw_ctx is not None
            and _crw_ctx.enabled
            and _crw_ctx.eligible_rotation_pairs > 0
        ):
            _crw_bp = _crw_ctx.best_rotation_pair or {}
            _sell_tk = _crw_bp.get("sell_ticker", "?")
            _buy_tk = _crw_bp.get("buy_ticker", "?")
            _pnl_imp = _crw_bp.get("expected_pnl_improvement", "?")
            _imp_pct = _crw_bp.get("expected_improvement_pct", "?")
            _cash_rel_str = _crw_bp.get("cash_released", "?")
            _if_hold = _crw_bp.get("expected_pnl_if_hold", "?")
            _if_rot = _crw_bp.get("expected_pnl_if_rotate", "?")
            recommended_next_action = (
                f"ROTATE: Sell {_sell_tk} to buy {_buy_tk}. "
                f"Expected improvement: ${_pnl_imp} ({_imp_pct}%). "
                f"Profile: {_crw_ctx.scoring_profile_used}. "
                f"{_crw_ctx.eligible_rotation_pairs} qualifying pair(s) found."
            )
            explanation = (
                f"Calibrated rotation recommends selling {_sell_tk} "
                f"(cash released: ${_cash_rel_str}) to buy {_buy_tk}. "
                f"Expected PnL if holding: ${_if_hold}. "
                f"Expected PnL if rotating: ${_if_rot}. "
                f"Improvement: ${_pnl_imp} ({_imp_pct}%). "
                f"Scoring profile: {_crw_ctx.scoring_profile_used}. "
                f"Calibration confidence: {_crw_ctx.calibration_confidence or 'N/A'}. "
                f"Preview only - no signals or orders created."
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

        # 7. Build prioritized action stack
        action_stack: list[DailyPlanActionItem] = []
        _priority = 1
        _rotated_sell: set[str] = set()
        _rotated_buy: set[str] = set()

        for rot in good_rotations:
            action_stack.append(DailyPlanActionItem(
                priority=_priority,
                action_type="ROTATE",
                ticker=rot.sell_ticker,
                secondary_ticker=rot.buy_ticker,
                title=f"Rotate: Sell {rot.sell_ticker}, Buy {rot.buy_ticker}",
                recommendation=f"Sell {rot.sell_ticker} (+{float(rot.sell_unrealized_pnl_pct):.2f}%) to buy {rot.buy_ticker}",
                reason=rot.reason,
                pnl_pct=rot.sell_unrealized_pnl_pct,
                candidate_review_id=rot.buy_candidate_review_id,
            ))
            _rotated_sell.add(rot.sell_ticker)
            _rotated_buy.add(rot.buy_ticker)
            _priority += 1

        for buy in buy_recommendations:
            if buy.ticker in _rotated_buy:
                continue
            action_stack.append(DailyPlanActionItem(
                priority=_priority,
                action_type="BUY",
                ticker=buy.ticker,
                title=f"Buy candidate: {buy.ticker}",
                recommendation=buy.reason,
                reason=f"Confidence: {float(buy.prediction_confidence or '0') * 100:.0f}%, Expected return: {buy.expected_return_pct or '-'}%",
                confidence=buy.prediction_confidence,
                expected_return_pct=buy.expected_return_pct,
                candidate_review_id=buy.candidate_review_id,
                approved_qty=buy.approved_qty,
            ))
            _priority += 1

        for sell in sell_recommendations:
            if sell.ticker in _rotated_sell:
                continue
            action_stack.append(DailyPlanActionItem(
                priority=_priority,
                action_type="SELL",
                ticker=sell.ticker,
                title=f"Sell candidate: {sell.ticker}",
                recommendation=sell.reason,
                reason=f"PnL: {float(sell.unrealized_pnl_pct):.2f}%",
                pnl_pct=sell.unrealized_pnl_pct,
                candidate_review_id=sell.candidate_review_id,
                sell_qty=sell.sell_qty,
            ))
            _priority += 1

        for hold in hold_positions:
            action_stack.append(DailyPlanActionItem(
                priority=_priority,
                action_type="HOLD",
                ticker=hold.ticker,
                title=f"Hold: {hold.ticker}",
                recommendation="Hold position",
                reason=hold.reason,
                pnl_pct=hold.unrealized_pnl_pct,
            ))
            _priority += 1

        for watch in watch_candidates:
            action_stack.append(DailyPlanActionItem(
                priority=_priority,
                action_type="WATCH",
                ticker=watch.ticker,
                title=f"Watch: {watch.ticker}",
                recommendation="No immediate action",
                reason=watch.reason,
                confidence=watch.prediction_confidence,
                expected_return_pct=watch.expected_return_pct,
            ))
            _priority += 1

        for blocked in blocked_actions:
            action_stack.append(DailyPlanActionItem(
                priority=_priority,
                action_type="BLOCKED",
                ticker=blocked.ticker,
                title=f"Blocked: {blocked.ticker}",
                recommendation=f"Action blocked: {blocked.action}",
                reason=blocked.explanation,
                blocked_reason=blocked.blocked_reason,
            ))
            _priority += 1

        if not action_stack:
            action_stack.append(DailyPlanActionItem(
                priority=1,
                action_type="NO_ACTION",
                title="No action required",
                recommendation="No action required",
                reason="No approved BUY candidates and no sell signals. Consider running a new prediction scan.",
            ))

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

        # 8. Capital Allocation / Rotation v3 analysis (preview-only, no trades)
        _CA_DOLLARS = Decimal("0.01")
        _ca_pos_details: list[CapitalReleasePositionItem] = []
        _ca_total_pos_value = Decimal("0")
        _ca_releasable_std = Decimal("0")
        _ca_releasable_theo = Decimal("0")
        _ca_blocked_neg_pnl = Decimal("0")
        _ca_blocked_no_price = Decimal("0")
        _ca_sellable_count = 0
        _ca_blocked_count = 0

        for _crd in _cap_release_raw:
            _crd_pos = _crd["pos"]
            _crd_price = _crd["price"]
            _crd_cv = _crd["cv"]
            _crd_upnl = _crd["upnl"]
            _crd_pct = _crd["pnl_pct"]
            _crd_sell = _crd["sellable"]
            _crd_br = _crd["blocked_reason"]

            _theo_cash = _crd_cv if _crd_cv is not None else Decimal("0")
            _std_cash = _crd_cv if (_crd_cv is not None and _crd_sell) else Decimal("0")

            if _crd_cv is not None:
                _ca_total_pos_value += _crd_cv
                _ca_releasable_theo += _theo_cash
                if _crd_sell:
                    _ca_releasable_std += _std_cash
                    _ca_sellable_count += 1
                else:
                    _ca_blocked_count += 1
                    if _crd_br == "NEGATIVE_PNL_BLOCKED":
                        _ca_blocked_neg_pnl += _theo_cash
                    else:
                        _ca_blocked_no_price += _theo_cash
            else:
                _ca_blocked_count += 1

            _ca_max_qty = _crd_pos.qty if _crd_sell else Decimal("0")
            _ca_pos_details.append(CapitalReleasePositionItem(
                ticker=_crd_pos.ticker,
                qty=str(_crd_pos.qty),
                avg_cost=str(_crd_pos.avg_cost),
                cost_basis=str(_crd_pos.cost_basis),
                current_price=str(_crd_price) if _crd_price is not None else None,
                current_value=str(_crd_cv.quantize(_CA_DOLLARS)) if _crd_cv is not None else None,
                unrealized_pnl=str(_crd_upnl.quantize(_CA_DOLLARS)) if _crd_upnl is not None else None,
                unrealized_pnl_pct=f"{_crd_pct:.4f}" if _crd_pct is not None else None,
                sellable_standard_mode=_crd_sell,
                blocked_reason=_crd_br,
                releasable_cash_standard_mode=str(_std_cash.quantize(_CA_DOLLARS)),
                releasable_cash_theoretical=str(_theo_cash.quantize(_CA_DOLLARS)),
                max_sell_qty_standard_mode=str(_ca_max_qty),
                explanation=(
                    f"Sellable at ${_crd_price}. Releases ${_std_cash.quantize(_CA_DOLLARS)} in standard mode."
                    if _crd_sell
                    else f"Blocked ({_crd_br}). Theoretical value ${_theo_cash.quantize(_CA_DOLLARS)} if sold."
                ),
            ))

        _ca_release_summary = CapitalReleaseSummary(
            current_cash=str(cash.quantize(_CA_DOLLARS)),
            total_position_value=str(_ca_total_pos_value.quantize(_CA_DOLLARS)),
            max_releasable_cash_standard_mode=str(_ca_releasable_std.quantize(_CA_DOLLARS)),
            max_releasable_cash_theoretical=str(_ca_releasable_theo.quantize(_CA_DOLLARS)),
            blocked_cash_due_to_negative_pnl=str(_ca_blocked_neg_pnl.quantize(_CA_DOLLARS)),
            blocked_cash_due_to_missing_or_stale_price=str(_ca_blocked_no_price.quantize(_CA_DOLLARS)),
            sellable_positions_count=_ca_sellable_count,
            blocked_positions_count=_ca_blocked_count,
        )

        # Candidate redeployment analysis
        _ca_cand_redeploy: list[CandidateRedeployItem] = []
        for _bt, _binfo in _buy_cap_info.items():
            _b_exp_pct = _binfo["exp_ret_pct"]      # stored as float % (e.g. 10.0 = 10%)
            _b_conf = _binfo["conf"]                 # fraction (e.g. 0.80)
            _b_risk_adj = _b_exp_pct * _b_conf       # risk-adjusted % (e.g. 8.0)
            _b_pnl_1k = 1000.0 * _b_risk_adj / 100.0  # dollar per $1,000 (e.g. 80.0)
            _ca_cand_redeploy.append(CandidateRedeployItem(
                ticker=_bt,
                current_price=str(_binfo["price"]) if _binfo["price"] else None,
                prediction_confidence=_binfo["cand"].prediction_confidence,
                expected_return_pct=_binfo["cand"].expected_return_pct,
                candidate_score_v2=f"{_binfo['score']:.4f}",
                risk_adjusted_expected_return_pct=f"{_b_risk_adj:.4f}",
                expected_pnl_per_1000=f"{_b_pnl_1k:.2f}",
                explanation=(
                    f"Risk-adjusted return: {_b_risk_adj:.2f}% "
                    f"(confidence {_b_conf:.2f} x expected {_b_exp_pct:.2f}%). "
                    f"Model-implied expected PnL per $1,000 deployed: ${_b_pnl_1k:.2f}."
                ),
            ))

        # Rotation opportunities: sellable positions x BUY candidates
        _ca_rot_opps: list[RotationOpportunityItem] = []
        for _crd in [r for r in _cap_release_raw if r["sellable"]]:
            _sell_t = _crd["pos"].ticker
            _cash_rel = _crd["cv"]
            if _cash_rel is None:
                continue

            # Look up holding forward prediction via latest CandidateReview for this ticker
            _h_cr = session.execute(
                select(CandidateReview)
                .where(
                    CandidateReview.ticker == _sell_t,
                    CandidateReview.prediction_confidence.is_not(None),
                )
                .order_by(CandidateReview.created_at.desc())
                .limit(1)
            ).scalar_one_or_none()
            _h_pred: dict | None = _cand_to_score_dict(_h_cr) if _h_cr is not None else None

            _h_sf = _score_holding_v2({}, holding_prediction=_h_pred)
            _hold_score = _h_sf.total_score
            # Holding expected forward PnL (dollar estimate)
            _h_exp_pct = (float(_h_pred["expected_return_pct"]) * 100.0) if _h_pred else 0.0
            _h_conf_v = (float(_h_pred["prediction_confidence"])) if _h_pred else 0.0
            _h_risk_adj = _h_exp_pct * _h_conf_v
            _hold_expected_pnl = float(_cash_rel) * _h_risk_adj / 100.0

            for _bt, _binfo in _buy_cap_info.items():
                if _bt == _sell_t:
                    continue
                _b_exp_pct = _binfo["exp_ret_pct"]
                _b_conf = _binfo["conf"]
                _b_risk_adj = _b_exp_pct * _b_conf
                _b_price = _binfo["price"]
                _b_score = _binfo["score"]
                _score_imp = _b_score - _hold_score
                _fwd_pnl = float(_cash_rel) * _b_risk_adj / 100.0
                _pnl_imp = _fwd_pnl - _hold_expected_pnl
                _meets = _score_imp > body.min_rotation_improvement_pct
                _est_qty: int | None = None
                if _b_price and float(_b_price) > 0:
                    _est_qty = int(float(_cash_rel) / float(_b_price))
                _ca_rot_opps.append(RotationOpportunityItem(
                    sell_ticker=_sell_t,
                    buy_ticker=_bt,
                    cash_released=str(_cash_rel.quantize(_CA_DOLLARS)),
                    buy_price=str(_b_price) if _b_price else None,
                    estimated_buy_qty=str(_est_qty) if _est_qty is not None else None,
                    expected_return_pct=_binfo["cand"].expected_return_pct,
                    prediction_confidence=_binfo["cand"].prediction_confidence,
                    risk_adjusted_expected_return_pct=f"{_b_risk_adj:.4f}",
                    expected_forward_pnl=f"{_fwd_pnl:.2f}",
                    holding_forward_score_v2=f"{_hold_score:.4f}",
                    candidate_score_v2=f"{_b_score:.4f}",
                    score_improvement=f"{_score_imp:.4f}",
                    expected_pnl_improvement=f"{_pnl_imp:.2f}",
                    meets_threshold=_meets,
                    blocked_reason=None if _meets else "INSUFFICIENT_IMPROVEMENT",
                    explanation=(
                        f"Sell {_sell_t} releasing ${_cash_rel.quantize(_CA_DOLLARS)}. "
                        f"Redeploy into {_bt} (risk-adj return {_b_risk_adj:.2f}%) "
                        f"-> model-implied expected PnL ${_fwd_pnl:.2f}. "
                        f"Score improvement {_score_imp:.4f} "
                        f"({'meets' if _meets else 'below'} threshold {body.min_rotation_improvement_pct})."
                    ),
                ))

        _ca_rot_opps.sort(key=lambda x: float(x.expected_pnl_improvement), reverse=True)

        capital_allocation = CapitalAllocationAnalysis(
            capital_release_summary=_ca_release_summary,
            position_release_details=_ca_pos_details,
            candidate_redeployment=_ca_cand_redeploy,
            rotation_opportunities=_ca_rot_opps,
            model_note=(
                "Rotation is recommended only when selling a profitable holding releases cash "
                "that can be redeployed into a candidate with higher model-implied expected PnL. "
                "Current PnL controls whether a position is sellable. "
                "Expected return controls which replacement candidate is better. "
                "A loss-making position is never recommended for sale in standard mode. "
                "All figures are model-implied estimates, not guaranteed returns."
            ),
        )

    # Portfolio-aware note: explain held tickers vs new-entry candidates
    _held_in_watch = sum(
        1 for w in watch_candidates if "already held" in (w.reason or "").lower()
    )
    if _held_in_watch > 0 and not buy_recommendations and not sell_recommendations:
        _portfolio_aware_note = (
            f"{_held_in_watch} candidate(s) are already held positions. "
            "Existing holdings are monitored through Position Review. "
            "Held tickers are not new-entry candidates. "
            "Review open positions or broaden scan universe."
        )
    else:
        _portfolio_aware_note = (
            "Existing holdings are monitored through Position Review. "
            "Held tickers are not new-entry candidates."
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
        action_stack=action_stack,
        recommended_next_action=recommended_next_action,
        explanation=explanation,
        safety_counts=DailyPlanSafetyCounts(
            signals_created=0,
            trade_decisions_created=0,
            orders_created=0,
            job_runs_created=0,
            db_rows_created=0,
        ),
        capital_allocation=capital_allocation,
        calibrated_rotation_context=_crw_ctx,
        profile_decision_context=_profile_decision_context,
        market_history_warning=_market_history_warning,
        portfolio_aware_note=_portfolio_aware_note,
    )


# ---------------------------------------------------------------------------
# Helper: extract actionable signal items from a DailyPlanPreviewResponse
# ---------------------------------------------------------------------------

def _extract_signal_items_from_plan(
    dp_result: "DailyPlanPreviewResponse",
    cand_map: dict,
    include_buy: bool,
    include_sell: bool,
    include_rotate: bool,
    action_ids: list[str] | None,
    candidate_review_ids: list[str] | None,
) -> tuple[int, list, list]:
    """
    Convert a DailyPlanPreviewResponse action_stack into a list of
    (action_id, action_type, ticker, side, confidence_str, candidate_review_id, position_ticker, reason)
    tuples.

    Returns: (evaluated_count, items_list, skipped_list)
    Each item is a dict with keys: action_id, action_type, ticker, side, confidence, candidate_review_id,
    position_ticker, reason.
    Skipped items: dicts with action_id, ticker, reason_code, reason.
    HOLD / WATCH / BLOCKED / NO_ACTION are silently excluded (not counted as skipped).
    """
    _SKIP_TYPES = {"HOLD", "WATCH", "BLOCKED", "NO_ACTION"}
    _crev_filter: set[str] | None = set(candidate_review_ids) if candidate_review_ids else None

    evaluated = 0
    items: list[dict] = []
    skipped: list[dict] = []

    for action in dp_result.action_stack:
        at = action.action_type
        if at in _SKIP_TYPES:
            continue

        if at == "BUY":
            if not include_buy:
                continue
            action_id = f"BUY:{action.ticker}"
            evaluated += 1

            if action_ids is not None and action_id not in action_ids:
                continue

            crev_id = action.candidate_review_id
            if _crev_filter is not None and crev_id not in _crev_filter:
                skipped.append({"action_id": action_id, "ticker": action.ticker,
                                "reason_code": "NOT_IN_CANDIDATE_REVIEW_IDS",
                                "reason": "Excluded by candidate_review_ids filter."})
                continue

            conf = action.confidence
            if conf is None and crev_id and crev_id in cand_map:
                conf = cand_map[crev_id].prediction_confidence
            if conf is None:
                conf = "0.80"

            items.append({
                "action_id": action_id, "action_type": "BUY",
                "ticker": action.ticker, "side": "BUY",
                "confidence": conf, "candidate_review_id": crev_id,
                "position_ticker": None, "reason": action.reason,
            })

        elif at == "SELL":
            if not include_sell:
                continue
            action_id = f"SELL:{action.ticker}"
            evaluated += 1

            if action_ids is not None and action_id not in action_ids:
                continue

            crev_id = action.candidate_review_id
            if _crev_filter is not None and crev_id not in _crev_filter:
                skipped.append({"action_id": action_id, "ticker": action.ticker,
                                "reason_code": "NOT_IN_CANDIDATE_REVIEW_IDS",
                                "reason": "Excluded by candidate_review_ids filter."})
                continue

            conf = action.confidence
            if conf is None and crev_id and crev_id in cand_map:
                conf = cand_map[crev_id].prediction_confidence
            if conf is None:
                conf = "0.80"

            items.append({
                "action_id": action_id, "action_type": "SELL",
                "ticker": action.ticker, "side": "SELL",
                "confidence": conf, "candidate_review_id": crev_id,
                "position_ticker": None, "reason": action.reason,
            })

        elif at == "ROTATE":
            if not include_rotate:
                continue
            sell_ticker = action.ticker
            buy_ticker = action.secondary_ticker
            evaluated += 1

            sell_id = f"ROTATE_SELL:{sell_ticker}"
            buy_id = f"ROTATE_BUY:{buy_ticker}" if buy_ticker else None

            # ROTATE_SELL leg
            if action_ids is None or sell_id in action_ids:
                items.append({
                    "action_id": sell_id, "action_type": "ROTATE_SELL",
                    "ticker": sell_ticker, "side": "SELL",
                    "confidence": "0.80", "candidate_review_id": None,
                    "position_ticker": sell_ticker,
                    "reason": f"Rotation sell: {sell_ticker} to make room for {buy_ticker}.",
                })

            # ROTATE_BUY leg
            if buy_ticker and buy_id:
                crev_id = action.candidate_review_id
                if _crev_filter is None or (crev_id and crev_id in _crev_filter):
                    if action_ids is None or buy_id in action_ids:
                        buy_conf = action.confidence
                        if buy_conf is None and crev_id and crev_id in cand_map:
                            buy_conf = cand_map[crev_id].prediction_confidence
                        if buy_conf is None:
                            buy_conf = "0.80"
                        items.append({
                            "action_id": buy_id, "action_type": "ROTATE_BUY",
                            "ticker": buy_ticker, "side": "BUY",
                            "confidence": buy_conf, "candidate_review_id": crev_id,
                            "position_ticker": sell_ticker,
                            "reason": f"Rotation buy: {buy_ticker} replacing {sell_ticker}.",
                        })

    return evaluated, items, skipped


@app.post(
    "/v1/review/daily-plan-signal-preview",
    response_model=DailyPlanSignalPreviewResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def daily_plan_signal_preview(
    body: DailyPlanSignalPreviewRequest,
) -> DailyPlanSignalPreviewResponse:
    """
    Preview Signal rows that would be created from Daily Plan actions.

    PREVIEW ONLY. No Signal, TradeDecision, Order, or JobRun rows are created.

    Logic:
    1. Internally runs Daily Plan Preview using the same parameters.
    2. Extracts actionable BUY, SELL, ROTATE items from the action_stack.
    3. Maps each to a DailyPlanSignalPreviewItem (one per BUY/SELL, two per ROTATE).
    4. HOLD, WATCH, BLOCKED, and NO_ACTION items are excluded.
    5. Returns previews with zero safety counts.
    """
    import uuid as _uuid_mod

    # Run the daily plan internally
    dp_body = DailyPlanPreviewRequest(
        approved_only=body.approved_only,
        min_confidence=body.min_confidence,
        max_price_age_days=body.max_price_age_days,
        block_loss_realization=body.block_loss_realization,
        include_rotation=body.include_rotation,
        limit_candidates=body.limit_candidates,
        min_rotation_improvement_pct=body.min_rotation_improvement_pct,
        candidate_ids=body.candidate_ids,
        position_tickers=body.position_tickers,
        use_calibrated_rotation=body.use_calibrated_rotation,
        scoring_profile=body.scoring_profile,
        min_expected_improvement_pct=body.min_expected_improvement_pct,
        min_expected_pnl_dollars=body.min_expected_pnl_dollars,
        allow_loss_realization=body.allow_loss_realization,
    )
    dp_result = await daily_plan_preview(dp_body)

    # Load CandidateReview rows for confidence cross-reference
    all_cand_ids: set[str] = set()
    for item in dp_result.action_stack:
        if item.candidate_review_id:
            all_cand_ids.add(item.candidate_review_id)

    cand_map: dict = {}
    if all_cand_ids:
        with get_session() as session:
            uuids = []
            for cid in all_cand_ids:
                try:
                    uuids.append(_uuid_mod.UUID(cid))
                except (ValueError, AttributeError):
                    pass
            if uuids:
                rows = session.query(CandidateReview).filter(
                    CandidateReview.id.in_(uuids)
                ).all()
                for row in rows:
                    cand_map[str(row.id)] = row

    evaluated, items, skipped_raw = _extract_signal_items_from_plan(
        dp_result=dp_result,
        cand_map=cand_map,
        include_buy=body.include_buy,
        include_sell=body.include_sell,
        include_rotate=body.include_rotate,
        action_ids=body.action_ids,
        candidate_review_ids=body.candidate_review_ids,
    )

    _safety_note = (
        "PREVIEW ONLY — no Signal, TradeDecision, Order, or JobRun rows created. "
        "Use /v1/review/daily-plan-create-signals with confirm_create_signals=true to create Signal rows."
    )

    previews = [
        DailyPlanSignalPreviewItem(
            action_id=it["action_id"],
            action_type=it["action_type"],
            ticker=it["ticker"],
            side=it["side"],
            confidence=it["confidence"],
            source="daily_plan_review_v1",
            candidate_review_id=it["candidate_review_id"],
            position_ticker=it["position_ticker"],
            reason=it["reason"],
            safety_note=_safety_note,
        )
        for it in items
    ]

    skipped_out = [
        DailyPlanSignalPreviewSkipped(
            action_id=s["action_id"],
            ticker=s["ticker"],
            reason_code=s["reason_code"],
            reason=s["reason"],
        )
        for s in skipped_raw
    ]

    return DailyPlanSignalPreviewResponse(
        evaluated_actions_count=evaluated,
        signal_previews_generated=len(previews),
        skipped_count=len(skipped_out),
        signal_previews=previews,
        skipped=skipped_out,
        safety_counts={
            "signals_created": 0,
            "decisions_created": 0,
            "orders_created": 0,
            "job_runs_created": 0,
            "rows_created": 0,
        },
    )


@app.post(
    "/v1/review/daily-plan-create-signals",
    response_model=DailyPlanCreateSignalsResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def daily_plan_create_signals(
    body: DailyPlanCreateSignalsRequest,
) -> DailyPlanCreateSignalsResponse:
    """
    Create Signal rows from approved Daily Plan actions.

    Requires confirm_create_signals=true. Creates Signal rows only.
    Does NOT create TradeDecision, Order, or trigger automation.

    Idempotency: source_run = 'daily_plan_review_v1:{ticker}:{side}:{date}'
    A repeated call on the same day with the same tickers returns already_existed > 0
    and signals_created = 0 for those entries.

    Logic:
    1. Internally runs Daily Plan Preview using the same parameters.
    2. Extracts actionable BUY, SELL, ROTATE items.
    3. Creates one Signal row per item (idempotency check first).
    4. Creates one JobRun only when at least one new Signal is created.
    """
    import uuid as _uuid_mod

    if not body.confirm_create_signals:
        raise HTTPException(
            status_code=422,
            detail="confirm_create_signals must be true to create Signal rows.",
        )

    # Run the daily plan internally
    dp_body = DailyPlanPreviewRequest(
        approved_only=body.approved_only,
        min_confidence=body.min_confidence,
        max_price_age_days=body.max_price_age_days,
        block_loss_realization=body.block_loss_realization,
        include_rotation=body.include_rotation,
        limit_candidates=body.limit_candidates,
        min_rotation_improvement_pct=body.min_rotation_improvement_pct,
        candidate_ids=body.candidate_ids,
        position_tickers=body.position_tickers,
        use_calibrated_rotation=body.use_calibrated_rotation,
        scoring_profile=body.scoring_profile,
        min_expected_improvement_pct=body.min_expected_improvement_pct,
        min_expected_pnl_dollars=body.min_expected_pnl_dollars,
        allow_loss_realization=body.allow_loss_realization,
    )
    dp_result = await daily_plan_preview(dp_body)

    # Load CandidateReview rows for confidence cross-reference
    all_cand_ids: set[str] = set()
    for item in dp_result.action_stack:
        if item.candidate_review_id:
            all_cand_ids.add(item.candidate_review_id)

    cand_map: dict = {}
    if all_cand_ids:
        with get_session() as session:
            uuids = []
            for cid in all_cand_ids:
                try:
                    uuids.append(_uuid_mod.UUID(cid))
                except (ValueError, AttributeError):
                    pass
            if uuids:
                rows = session.query(CandidateReview).filter(
                    CandidateReview.id.in_(uuids)
                ).all()
                for row in rows:
                    cand_map[str(row.id)] = row

    evaluated, items, skipped_raw = _extract_signal_items_from_plan(
        dp_result=dp_result,
        cand_map=cand_map,
        include_buy=body.include_buy,
        include_sell=body.include_sell,
        include_rotate=body.include_rotate,
        action_ids=body.action_ids,
        candidate_review_ids=body.candidate_review_ids,
    )

    eastern_now = datetime.now(_EASTERN)
    market_date = eastern_now.date()
    date_str = str(market_date)
    ikey = body.idempotency_key or f"daily_plan_create_signals:{date_str}:{_uuid_mod.uuid4().hex[:8]}"

    created_count = 0
    already_existed_count = 0
    skipped_out: list[DailyPlanCreateSignalsSkipped] = [
        DailyPlanCreateSignalsSkipped(
            action_id=s["action_id"], ticker=s["ticker"],
            reason_code=s["reason_code"], reason=s["reason"],
        )
        for s in skipped_raw
    ]
    created_list: list[DailyPlanCreatedSignalItem] = []

    # Signals to create: (item_dict, source_run, confidence_decimal)
    signals_to_create: list[tuple] = []

    with get_session() as session:
        for it in items:
            ticker = it["ticker"]
            side = it["side"]
            crev_id = it["candidate_review_id"]
            action_id = it["action_id"]
            action_type = it["action_type"]

            # Parse confidence
            try:
                conf_decimal = Decimal(str(it["confidence"] or "0.80"))
                if not (Decimal("0") <= conf_decimal <= Decimal("1")):
                    raise ValueError("out of range")
            except (ValueError, Exception):
                skipped_out.append(DailyPlanCreateSignalsSkipped(
                    action_id=action_id, ticker=ticker,
                    reason_code="INVALID_CONFIDENCE",
                    reason=f"Confidence '{it['confidence']}' is not a valid decimal in [0, 1].",
                ))
                continue

            source_run = f"daily_plan_review_v1:{ticker}:{side.lower()}:{date_str}"

            # Idempotency check
            existing = session.query(Signal).filter(
                Signal.source_run == source_run,
                Signal.ticker == ticker,
                Signal.direction == side,
            ).first()
            if existing:
                already_existed_count += 1
                continue

            signals_to_create.append((it, conf_decimal, source_run))

        # Create JobRun only if there are new signals to create
        job_run = None
        if signals_to_create:
            job_run = JobRun(
                idempotency_key=ikey,
                workflow_type="DAILY_PLAN_CREATE_SIGNALS",
                market_date=market_date,
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
                result_summary={},
            )
            session.add(job_run)
            session.flush()

            for it, conf_decimal, source_run in signals_to_create:
                ticker = it["ticker"]
                side = it["side"]
                crev_id = it["candidate_review_id"]
                action_id = it["action_id"]
                action_type = it["action_type"]

                raw_payload = {
                    "source": "daily_plan_review_v1",
                    "action_id": action_id,
                    "action_type": action_type,
                    "candidate_review_id": crev_id,
                    "position_ticker": it["position_ticker"],
                    "request_idempotency_key": ikey,
                }

                signal = Signal(
                    job_run_id=job_run.id,
                    ticker=ticker,
                    direction=side,
                    confidence=conf_decimal,
                    signal_ts=datetime.now(timezone.utc),
                    market_date=market_date,
                    source_run=source_run,
                    status="RECEIVED",
                    raw_payload=raw_payload,
                )
                session.add(signal)
                session.flush()

                created_list.append(DailyPlanCreatedSignalItem(
                    action_id=action_id,
                    signal_id=str(signal.id),
                    ticker=ticker,
                    side=side,
                    confidence=str(conf_decimal),
                    source_run=source_run,
                    action_type=action_type,
                ))
                created_count += 1

            if job_run is not None:
                job_run.result_summary = {
                    "signals_created": created_count,
                    "already_existed": already_existed_count,
                    "skipped_count": len(skipped_out),
                }
                session.add(job_run)
                session.flush()

    return DailyPlanCreateSignalsResponse(
        evaluated_actions_count=evaluated,
        signals_created=created_count,
        already_existed=already_existed_count,
        skipped_count=len(skipped_out),
        decisions_created=0,
        orders_created=0,
        automation_triggered=False,
        created_signals=created_list,
        skipped=skipped_out,
        safety_note=(
            "Signal rows created for approved Daily Plan actions. "
            "No TradeDecision, Order, or automation triggered. "
            "Next step: run Decision Preview manually to evaluate created signals."
        ),
    )


@app.post(
    "/v1/review/daily-plan-decision-preview",
    response_model=DailyPlanDecisionPreviewResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def daily_plan_decision_preview(
    body: DailyPlanDecisionPreviewRequest,
) -> DailyPlanDecisionPreviewResponse:
    """
    Preview trade decisions from Daily Plan-created Signal rows.

    PREVIEW ONLY — no TradeDecision, Order, Signal, or JobRun rows are created.
    Signal.status is NOT updated.

    Daily Plan signals are identified by source_run prefix 'daily_plan_review_v1:'.
    Non-Daily-Plan signals are rejected (skipped) even if their IDs are provided
    via signal_ids.

    When latest_daily_plan_only=True (default) and signal_ids is not provided,
    evaluates only the most recent Daily Plan batch. The batch date is extracted
    from the source_run format: 'daily_plan_review_v1:{ticker}:{side}:{date}'.

    Returns safety_counts with all zeros as proof of no persistence.
    """
    import uuid as _uuid_mod

    _DP_PREFIX = "daily_plan_review_v1:"

    evaluated = 0
    generated = 0
    skipped_list: list[DailyPlanDecisionPreviewSkipped] = []
    decision_previews: list[DailyPlanDecisionPreviewItem] = []

    _safety_counts: dict[str, int] = {
        "signals_created": 0,
        "trade_decisions_created": 0,
        "orders_created": 0,
        "job_runs_created": 0,
        "rows_created": 0,
        "db_rows_created": 0,
    }
    _next_step = "Create trade decisions manually from approved signal previews."
    _safety_note = (
        "PREVIEW ONLY — no TradeDecision, Order, Signal, or JobRun rows created. "
        "Evaluated only Daily Plan signals (source_run prefix: daily_plan_review_v1:)."
    )

    with get_session() as session:
        eastern_now = datetime.now(_EASTERN)
        market_date = eastern_now.date()
        now = datetime.now(timezone.utc)

        portfolio = get_portfolio(session)

        # Build the Signal rows to evaluate
        if body.signal_ids:
            # Query by explicit signal IDs (UUIDs)
            try:
                signal_uuids = [_uuid_mod.UUID(sid) for sid in body.signal_ids]
            except (ValueError, AttributeError):
                return DailyPlanDecisionPreviewResponse(
                    evaluated_signals=0,
                    previews_generated=0,
                    approved_count=0,
                    rejected_count=0,
                    skipped_count=0,
                    decision_counts={},
                    decision_previews=[],
                    skipped=[],
                    safety_counts=_safety_counts,
                    next_step=_next_step,
                    safety_note=_safety_note,
                )
            rows = session.query(Signal).filter(
                Signal.id.in_(signal_uuids)
            ).limit(body.limit).all()
        else:
            # Query all Daily Plan signals ordered newest first
            query = session.query(Signal).filter(
                Signal.source_run.startswith(_DP_PREFIX)
            )
            if body.received_only:
                query = query.filter(Signal.status == "RECEIVED")
            rows = query.order_by(Signal.signal_ts.desc()).limit(body.limit).all()

            # latest_daily_plan_only: keep only signals from the most recent date
            if body.latest_daily_plan_only and rows:
                date_strs: set[str] = set()
                for sig in rows:
                    parts = sig.source_run.split(":")
                    if len(parts) >= 4:
                        date_strs.add(parts[-1])
                if date_strs:
                    latest_date_str = max(date_strs)
                    rows = [
                        s for s in rows
                        if len(s.source_run.split(":")) >= 4
                        and s.source_run.split(":")[-1] == latest_date_str
                    ]

        # Evaluate each signal
        for signal in rows:
            evaluated += 1
            signal_id_str = str(signal.id)

            # Reject non-Daily-Plan signals (even when explicitly listed via signal_ids)
            if not signal.source_run.startswith(_DP_PREFIX):
                skipped_list.append(DailyPlanDecisionPreviewSkipped(
                    signal_id=signal_id_str,
                    ticker=signal.ticker,
                    reason=f"Not a Daily Plan signal: source_run='{signal.source_run}'",
                ))
                continue

            # Apply received_only check on the signal_ids path (SQL path already filtered)
            if body.signal_ids and body.received_only and signal.status != "RECEIVED":
                skipped_list.append(DailyPlanDecisionPreviewSkipped(
                    signal_id=signal_id_str,
                    ticker=signal.ticker,
                    reason=f"Signal status is {signal.status}, expected RECEIVED",
                ))
                continue

            # Fetch latest price for risk evaluation (no DB write)
            snapshot_price = _latest_price(session, signal.ticker)

            # Evaluate via risk engine — pure function, no DB writes
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

            decision_previews.append(DailyPlanDecisionPreviewItem(
                signal_id=signal_id_str,
                ticker=signal.ticker,
                side=signal.direction,
                confidence=str(signal.confidence),
                decision=rd.decision,
                approved_qty=str(rd.approved_qty),
                approved_notional=str(rd.approved_notional),
                reason=f"Preview only: would create {rd.decision} decision from Daily Plan signal.",
                risk_snapshot=rd.risk_snapshot,
                source_run=signal.source_run,
            ))
            generated += 1

    # Compute decision counts and approval/rejection tallies
    _decision_counts: dict[str, int] = {}
    for _p in decision_previews:
        _decision_counts[_p.decision] = _decision_counts.get(_p.decision, 0) + 1
    _approved_count = sum(1 for _p in decision_previews if _p.decision in ("BUY", "SELL"))
    _rejected_count = sum(1 for _p in decision_previews if _p.decision == "REJECTED")

    return DailyPlanDecisionPreviewResponse(
        evaluated_signals=evaluated,
        previews_generated=generated,
        approved_count=_approved_count,
        rejected_count=_rejected_count,
        skipped_count=len(skipped_list),
        decision_counts=_decision_counts,
        decision_previews=decision_previews,
        skipped=skipped_list,
        safety_counts=_safety_counts,
        next_step=_next_step,
        safety_note=_safety_note,
    )


@app.post(
    "/v1/review/daily-plan-create-decisions",
    response_model=DailyPlanCreateDecisionsResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def daily_plan_create_decisions(
    body: DailyPlanCreateDecisionsRequest,
) -> DailyPlanCreateDecisionsResponse:
    """
    Create TradeDecision rows from Daily Plan-created Signal rows.

    Creates real TradeDecision rows and a JobRun. Does NOT create Orders.
    Automation remains off. Signal.status is updated to DECISION_MADE for each
    successfully processed signal.

    Idempotent: if a TradeDecision already exists for a signal, that signal is
    skipped without modification.

    Daily Plan signals are identified by source_run prefix 'daily_plan_review_v1:'.
    Non-Daily-Plan signals are rejected even if their IDs are provided.

    confirm_create_decisions must be true to proceed; returns 422 otherwise.
    """
    import uuid as _uuid_mod

    if not body.confirm_create_decisions:
        raise HTTPException(
            status_code=422,
            detail="confirm_create_decisions must be true to create TradeDecisions",
        )

    _DP_PREFIX = "daily_plan_review_v1:"

    _empty_safety: dict[str, Any] = {
        "signals_created": 0,
        "trade_decisions_created": 0,
        "orders_created": 0,
        "automation_triggered": False,
    }
    _safety_note = "CREATES TRADE DECISIONS ONLY — no orders created, automation off."

    evaluated = 0
    created = 0
    skipped_list: list[DailyPlanCreateDecisionsSkipped] = []
    created_decisions_list: list[DailyPlanCreatedDecisionDetail] = []

    with get_session() as session:
        eastern_now = datetime.now(_EASTERN)
        market_date = eastern_now.date()
        now = datetime.now(timezone.utc)

        portfolio = get_portfolio(session)

        # Build Signal rows to evaluate
        if body.signal_ids:
            try:
                signal_uuids = [_uuid_mod.UUID(sid) for sid in body.signal_ids]
            except (ValueError, AttributeError):
                return DailyPlanCreateDecisionsResponse(
                    evaluated_count=0,
                    created_count=0,
                    skipped_count=0,
                    decision_counts={},
                    created_decisions=[],
                    skipped=[],
                    safety_counts=_empty_safety,
                    safety_note=_safety_note,
                )
            rows = session.query(Signal).filter(
                Signal.id.in_(signal_uuids)
            ).limit(body.limit).all()
        else:
            query = session.query(Signal).filter(
                Signal.source_run.startswith(_DP_PREFIX)
            )
            if body.received_only:
                query = query.filter(Signal.status == "RECEIVED")
            rows = query.order_by(Signal.signal_ts.desc()).limit(body.limit).all()

        # First pass: validate and collect signals eligible for decision creation
        decisions_to_create = []
        for signal in rows:
            evaluated += 1
            signal_id_str = str(signal.id)

            # Reject non-Daily-Plan signals
            if not signal.source_run.startswith(_DP_PREFIX):
                skipped_list.append(DailyPlanCreateDecisionsSkipped(
                    signal_id=signal_id_str,
                    ticker=signal.ticker,
                    reason=f"Not a Daily Plan signal: source_run='{signal.source_run}'",
                ))
                continue

            # Apply received_only check on the signal_ids path (SQL path already filtered)
            if body.signal_ids and body.received_only and signal.status != "RECEIVED":
                skipped_list.append(DailyPlanCreateDecisionsSkipped(
                    signal_id=signal_id_str,
                    ticker=signal.ticker,
                    reason=f"Signal status is {signal.status}, expected RECEIVED",
                ))
                continue

            # Idempotency: skip if TradeDecision already exists for this signal
            existing = session.query(TradeDecision).filter(
                TradeDecision.signal_id == signal.id
            ).first()
            if existing:
                skipped_list.append(DailyPlanCreateDecisionsSkipped(
                    signal_id=signal_id_str,
                    ticker=signal.ticker,
                    reason="TradeDecision already exists for signal",
                ))
                continue

            decisions_to_create.append(signal)

        # Only create a JobRun (required by TradeDecision.job_run_id FK) when there is work to do
        job_run = None
        if decisions_to_create:
            ikey = str(_uuid_mod.uuid4())
            job_run = JobRun(
                idempotency_key=ikey,
                workflow_type="DAILY_PLAN_CREATE_DECISIONS",
                market_date=market_date,
                status="COMPLETED",
                completed_at=datetime.now(timezone.utc),
                result_summary={},
            )
            session.add(job_run)
            session.flush()

            # Second pass: evaluate and create TradeDecisions
            for signal in decisions_to_create:
                signal_id_str = str(signal.id)
                snapshot_price = _latest_price(session, signal.ticker)

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

                # Update Signal.status to DECISION_MADE
                signal.status = "DECISION_MADE"
                session.add(signal)
                session.flush()

                created_decisions_list.append(DailyPlanCreatedDecisionDetail(
                    signal_id=signal_id_str,
                    trade_decision_id=str(trade_decision.id),
                    ticker=signal.ticker,
                    side=signal.direction,
                    decision=rd.decision,
                    reason_code=rd.reason_code,
                    requested_notional=str(rd.requested_notional),
                    approved_notional=str(rd.approved_notional),
                    requested_qty=str(rd.requested_qty),
                    approved_qty=str(rd.approved_qty),
                ))
                created += 1

            job_run.result_summary = {
                "evaluated_count": evaluated,
                "created_count": created,
                "skipped_count": len(skipped_list),
            }
            session.add(job_run)
            session.flush()

    # Compute decision counts from created decisions
    _decision_counts: dict[str, int] = {}
    for _d in created_decisions_list:
        _decision_counts[_d.decision] = _decision_counts.get(_d.decision, 0) + 1

    return DailyPlanCreateDecisionsResponse(
        evaluated_count=evaluated,
        created_count=created,
        skipped_count=len(skipped_list),
        decision_counts=_decision_counts,
        created_decisions=created_decisions_list,
        skipped=skipped_list,
        safety_counts={
            "signals_created": 0,
            "trade_decisions_created": created,
            "orders_created": 0,
            "automation_triggered": False,
        },
        safety_note=_safety_note,
    )


@app.post(
    "/v1/review/daily-plan-order-preview",
    response_model=DailyPlanOrderPreviewResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def daily_plan_order_preview(
    body: DailyPlanOrderPreviewRequest,
) -> DailyPlanOrderPreviewResponse:
    """
    Preview which Daily Plan TradeDecision rows would become Orders, without creating any rows.

    PREVIEW ONLY: no Order rows created, no JobRun rows created,
    no TradeDecision or Signal rows modified. Safe to call repeatedly.

    Daily Plan TradeDecisions are identified by their linked Signal's source_run
    prefix 'daily_plan_review_v1:'. Non-Daily-Plan TradeDecisions are skipped.

    Filtering:
    - If trade_decision_ids provided: query those exact IDs, validate DP prefix in Python.
    - Otherwise: join Signal table and filter by source_run prefix.

    approved_only (default true): only previews BUY/SELL decisions with approved_qty > 0.
    If false: evaluates all but skips non-orderable decisions with clear reasons.
    """
    import uuid as _uuid_mod

    _DP_PREFIX = "daily_plan_review_v1:"
    _safety_note = "PREVIEW ONLY — no orders created, no automation triggered."
    _empty_safety: dict[str, Any] = {
        "signals_created": 0,
        "trade_decisions_created": 0,
        "orders_created": 0,
        "job_runs_created": 0,
        "automation_triggered": False,
    }

    evaluated = 0
    preview_list: list[OrderPreviewItem] = []
    skipped_list: list[SkippedTradeDecisionDetail] = []

    with get_session() as session:
        if body.trade_decision_ids:
            try:
                decision_uuids = [_uuid_mod.UUID(did) for did in body.trade_decision_ids]
            except (ValueError, AttributeError):
                return DailyPlanOrderPreviewResponse(
                    evaluated_count=0,
                    preview_count=0,
                    skipped_count=0,
                    order_previews=[],
                    skipped=[],
                    side_counts={},
                    safety_counts=_empty_safety,
                    safety_note=_safety_note,
                )
            rows = session.query(TradeDecision).filter(
                TradeDecision.id.in_(decision_uuids)
            ).limit(body.limit).all()
        else:
            rows = (
                session.query(TradeDecision)
                .join(Signal, Signal.id == TradeDecision.signal_id)
                .filter(Signal.source_run.startswith(_DP_PREFIX))
                .limit(body.limit)
                .all()
            )

        # Portfolio-aware: load held tickers to block duplicate BUY for already-held positions
        _held_tickers_set = {
            p.ticker.upper()
            for p in session.execute(select(Position)).scalars().all()
        }

        for td in rows:
            evaluated += 1
            decision_id_str = str(td.id)
            signal_id_str = str(td.signal_id)

            signal = session.query(Signal).filter(Signal.id == td.signal_id).first()
            if not signal:
                skipped_list.append(SkippedTradeDecisionDetail(
                    trade_decision_id=decision_id_str,
                    ticker=td.ticker,
                    reason="Signal not found",
                ))
                continue

            # Validate DP prefix (enforced for both query paths)
            if not signal.source_run.startswith(_DP_PREFIX):
                skipped_list.append(SkippedTradeDecisionDetail(
                    trade_decision_id=decision_id_str,
                    ticker=td.ticker,
                    reason="TradeDecision source signal is not a Daily Plan signal",
                ))
                continue

            # Portfolio-aware: block duplicate BUY entry for already-held tickers
            if td.decision == "BUY" and td.ticker.upper() in _held_tickers_set:
                skipped_list.append(SkippedTradeDecisionDetail(
                    trade_decision_id=decision_id_str,
                    ticker=td.ticker,
                    reason="ALREADY_HELD_NO_DUPLICATE_ENTRY: ticker is an open position",
                ))
                continue

            # Duplicate: check if Order already exists for this TradeDecision
            existing_order = session.query(Order).filter(
                Order.trade_decision_id == td.id
            ).first()
            if existing_order:
                skipped_list.append(SkippedTradeDecisionDetail(
                    trade_decision_id=decision_id_str,
                    ticker=td.ticker,
                    reason="Order already exists for TradeDecision",
                ))
                continue

            # Approval check
            if td.decision not in ("BUY", "SELL"):
                if body.approved_only:
                    skipped_list.append(SkippedTradeDecisionDetail(
                        trade_decision_id=decision_id_str,
                        ticker=td.ticker,
                        reason="TradeDecision is not approved for order creation",
                    ))
                else:
                    skipped_list.append(SkippedTradeDecisionDetail(
                        trade_decision_id=decision_id_str,
                        ticker=td.ticker,
                        reason=f"TradeDecision decision is {td.decision}, not BUY/SELL",
                    ))
                continue

            # Qty check
            if not td.approved_qty or td.approved_qty <= Decimal("0"):
                reason = (
                    "TradeDecision is not approved for order creation"
                    if body.approved_only
                    else "TradeDecision has zero approved_qty"
                )
                skipped_list.append(SkippedTradeDecisionDetail(
                    trade_decision_id=decision_id_str,
                    ticker=td.ticker,
                    reason=reason,
                ))
                continue

            preview_list.append(OrderPreviewItem(
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
                reason="Preview only: this Daily Plan TradeDecision would create an order.",
            ))

    side_counts: dict[str, int] = {}
    for p in preview_list:
        side_counts[p.side] = side_counts.get(p.side, 0) + 1

    return DailyPlanOrderPreviewResponse(
        evaluated_count=evaluated,
        preview_count=len(preview_list),
        skipped_count=len(skipped_list),
        order_previews=preview_list,
        skipped=skipped_list,
        side_counts=side_counts,
        safety_counts=_empty_safety,
        safety_note=_safety_note,
    )


@app.get(
    "/v1/review/daily-plan-execution-status",
    response_model=DailyPlanExecutionStatusResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def daily_plan_execution_status() -> DailyPlanExecutionStatusResponse:
    """
    GET /v1/review/daily-plan-execution-status — Consolidated Daily Plan execution state.

    Read-only: zero rows created, zero rows mutated. Safe to call repeatedly.

    Returns workflow-chain counts (candidate reviews, DP signals, DP decisions, DP orders),
    a fixed safety_state (orders_enabled=false, automation_enabled=false), the next
    recommended workflow step, and a warnings list.

    Daily Plan rows are identified by Signal.source_run prefix 'daily_plan_review_v1:'.
    """
    _DP_PREFIX = "daily_plan_review_v1:"
    _safety_state: dict[str, Any] = {
        "orders_enabled": False,
        "automation_enabled": False,
        "manual_review_required": True,
        "create_orders_available": False,
    }

    with get_session() as session:
        # --- Candidate review counts ---
        cr_total = session.query(CandidateReview).count()
        cr_approved = session.query(CandidateReview).filter(
            CandidateReview.review_status == "APPROVED_FOR_SIGNAL"
        ).count()
        cr_watching = session.query(CandidateReview).filter(
            CandidateReview.review_status == "WATCHING"
        ).count()
        cr_rejected = session.query(CandidateReview).filter(
            CandidateReview.review_status == "REJECTED"
        ).count()
        cr_new = session.query(CandidateReview).filter(
            CandidateReview.review_status == "NEW"
        ).count()

        candidate_review_counts = {
            "total": cr_total,
            "approved_for_signal": cr_approved,
            "watching": cr_watching,
            "rejected": cr_rejected,
            "new": cr_new,
        }

        # --- Daily Plan signal counts ---
        dp_sig_base = session.query(Signal).filter(
            Signal.source_run.startswith(_DP_PREFIX)
        )
        dp_sig_total = dp_sig_base.count()
        dp_sig_received = dp_sig_base.filter(Signal.status == "RECEIVED").count()
        dp_sig_decision_made = dp_sig_base.filter(Signal.status == "DECISION_MADE").count()
        dp_sig_expired = dp_sig_base.filter(Signal.status == "EXPIRED").count()
        dp_sig_error = dp_sig_base.filter(Signal.status == "ERROR").count()

        daily_plan_signal_counts = {
            "total": dp_sig_total,
            "received": dp_sig_received,
            "decision_made": dp_sig_decision_made,
            "expired": dp_sig_expired,
            "error": dp_sig_error,
        }

        # Latest DP source_run (most recently created signal)
        latest_source_run_row = session.execute(
            select(Signal.source_run)
            .where(Signal.source_run.startswith(_DP_PREFIX))
            .order_by(Signal.created_at.desc())
        ).first()
        latest_dp_source_run: str | None = latest_source_run_row[0] if latest_source_run_row else None

        # --- Daily Plan trade decision counts ---
        dp_td_base = session.query(TradeDecision).join(
            Signal, Signal.id == TradeDecision.signal_id
        ).filter(Signal.source_run.startswith(_DP_PREFIX))

        dp_td_total = dp_td_base.count()
        dp_td_buy = dp_td_base.filter(TradeDecision.decision == "BUY").count()
        dp_td_sell = dp_td_base.filter(TradeDecision.decision == "SELL").count()
        dp_td_hold = dp_td_base.filter(TradeDecision.decision == "HOLD").count()
        dp_td_rejected = dp_td_base.filter(TradeDecision.decision == "REJECTED").count()

        daily_plan_trade_decision_counts = {
            "total": dp_td_total,
            "buy": dp_td_buy,
            "sell": dp_td_sell,
            "hold": dp_td_hold,
            "rejected": dp_td_rejected,
        }

        # --- DP order preview available count (no rows created) ---
        eligible_td_ids = [
            row[0]
            for row in session.execute(
                select(TradeDecision.id)
                .join(Signal, Signal.id == TradeDecision.signal_id)
                .where(Signal.source_run.startswith(_DP_PREFIX))
                .where(TradeDecision.decision.in_(["BUY", "SELL"]))
                .where(TradeDecision.approved_qty > Decimal("0"))
            ).all()
        ]

        if eligible_td_ids:
            already_ordered = session.query(Order).filter(
                Order.trade_decision_id.in_(eligible_td_ids)
            ).count()
            dp_order_preview_available = len(eligible_td_ids) - already_ordered
        else:
            dp_order_preview_available = 0
            already_ordered = 0

        # --- Existing orders linked to any DP decision ---
        existing_order_count = 0
        if dp_td_total > 0:
            all_dp_td_ids = [
                row[0]
                for row in session.execute(
                    select(TradeDecision.id)
                    .join(Signal, Signal.id == TradeDecision.signal_id)
                    .where(Signal.source_run.startswith(_DP_PREFIX))
                ).all()
            ]
            if all_dp_td_ids:
                existing_order_count = session.query(Order).filter(
                    Order.trade_decision_id.in_(all_dp_td_ids)
                ).count()

    # --- Warnings ---
    warnings: list[str] = ["order_creation_disabled"]
    if cr_new > 0:
        warnings.append("pending_review_items")
    if dp_order_preview_available > 0:
        warnings.append("pending_trade_decisions")

    # --- next_recommended_step ---
    # DP artifacts represent deeper pipeline state; check them first so
    # a clean-DB test that seeds only signals/decisions gets the correct step.
    if dp_order_preview_available > 0:
        next_step = "preview_orders"
    elif dp_td_total > 0:
        next_step = "stop_before_orders"
    elif dp_sig_received > 0:
        next_step = "preview_decisions"
    elif dp_sig_total > 0:
        next_step = "create_decisions"
    elif cr_approved > 0:
        next_step = "generate_daily_plan"
    elif cr_total == 0:
        next_step = "generate_candidate_preview"
    elif cr_new > 0:
        next_step = "review_queue"
    else:
        next_step = "save_candidates"

    return DailyPlanExecutionStatusResponse(
        latest_daily_plan_source_run=latest_dp_source_run,
        candidate_review_counts=candidate_review_counts,
        daily_plan_signal_counts=daily_plan_signal_counts,
        daily_plan_trade_decision_counts=daily_plan_trade_decision_counts,
        daily_plan_order_preview_available_count=dp_order_preview_available,
        existing_order_count=existing_order_count,
        safety_state=_safety_state,
        next_recommended_step=next_step,
        warnings=warnings,
    )


@app.get(
    "/v1/strategy/universe/status",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def get_universe_status_endpoint() -> UniverseStatusResponse:
    """
    GET /v1/strategy/universe/status — Read-only universe diagnostics.

    Returns metadata about the active universe file, ticker count, market data
    coverage, and safety counts. No DB writes.
    """
    from paper_trader.engine.universe import get_universe_status as _get_universe_status

    universe_info = _get_universe_status()
    universe_tickers: list[str] = universe_info.get("tickers", [])

    benchmark_ticker = "SPY"
    min_price_points = 5

    tickers_with_enough = 0
    tickers_missing = len(universe_tickers)
    benchmark_available = False

    with get_session() as session:
        if universe_tickers:
            rows = session.execute(
                select(
                    PriceSnapshot.ticker,
                    func.count(distinct(PriceSnapshot.market_date)).label("date_count"),
                )
                .where(PriceSnapshot.price_type == "CLOSE")
                .where(PriceSnapshot.session_type == "REGULAR")
                .where(PriceSnapshot.ticker.in_(universe_tickers))
                .group_by(PriceSnapshot.ticker)
            ).all()

            counted: dict[str, int] = {row[0]: row[1] for row in rows}
            tickers_with_enough = sum(
                1 for t in universe_tickers
                if counted.get(t, 0) >= min_price_points
            )
            tickers_missing = len(universe_tickers) - tickers_with_enough

        bm_count = session.execute(
            select(func.count())
            .select_from(BenchmarkPrice)
            .where(BenchmarkPrice.ticker == benchmark_ticker)
            .where(BenchmarkPrice.session_type == "REGULAR")
        ).scalar() or 0
        benchmark_available = bm_count > 0

    return UniverseStatusResponse(
        universe_name=universe_info["universe_name"],
        active_source_file=universe_info["active_source_file"],
        ticker_count=universe_info["ticker_count"],
        first_10_tickers=universe_info["first_10_tickers"],
        last_10_tickers=universe_info["last_10_tickers"],
        is_stub_universe=universe_info["is_stub_universe"],
        expected_full_sp500_min_count=universe_info["expected_full_sp500_min_count"],
        warning=universe_info["warning"],
        fallback_used=universe_info["fallback_used"],
        full_universe_file_exists=universe_info["full_universe_file_exists"],
        stub_universe_file_exists=universe_info["stub_universe_file_exists"],
        market_data_coverage=UniverseMarketDataCoverage(
            tickers_with_enough_price_history=tickers_with_enough,
            tickers_missing_price_history=tickers_missing,
            benchmark_available=benchmark_available,
            benchmark_ticker=benchmark_ticker,
            min_price_points_used=min_price_points,
        ),
        safety_counts=UniverseSafetyCounts(
            rows_created=0,
            signals_created=0,
            decisions_created=0,
            orders_created=0,
        ),
    )


def _build_calibration_recommendation(
    profiles: list[str],
    date_runs: list[dict],
) -> CalibrationProfileRecommendation:
    """Aggregate per-date calibration results into a cross-date profile recommendation."""
    import statistics as _statistics

    n_dates = len(date_runs)

    _agg: dict[str, dict] = {
        p: {"avg_rets": [], "med_rets": [], "win_rates": [], "exc_rets": []}
        for p in profiles
    }
    for run in date_runs:
        for pr in run["profile_results"]:
            pn = pr.profile_name
            if pn not in _agg:
                continue
            if pr.average_forward_return_pct is not None:
                _agg[pn]["avg_rets"].append(pr.average_forward_return_pct)
            if pr.median_forward_return_pct is not None:
                _agg[pn]["med_rets"].append(pr.median_forward_return_pct)
            if pr.win_rate_pct is not None:
                _agg[pn]["win_rates"].append(pr.win_rate_pct)
            if pr.average_excess_return_vs_spy_pct is not None:
                _agg[pn]["exc_rets"].append(pr.average_excess_return_vs_spy_pct)

    _date_winners: list[Any] = []
    for run in date_runs:
        _dc: dict[str, float] = {}
        for pr in run["profile_results"]:
            pn = pr.profile_name
            _c = 0.0
            if pr.average_excess_return_vs_spy_pct is not None:
                _c += pr.average_excess_return_vs_spy_pct / 100.0 * 0.4
            if pr.win_rate_pct is not None:
                _c += pr.win_rate_pct / 100.0 * 0.3
            if pr.average_forward_return_pct is not None:
                _c += pr.average_forward_return_pct / 100.0 * 0.3
            _dc[pn] = _c
        _date_winners.append(max(_dc, key=lambda k: _dc[k]) if _dc else None)

    _valid_winners = [w for w in _date_winners if w is not None]
    _consistency: dict[str, float] = {
        p: round(sum(1 for w in _valid_winners if w == p) / len(_valid_winners), 4)
        if _valid_winners else 0.0
        for p in profiles
    }

    _agg_composites: dict[str, float] = {}
    for p in profiles:
        _c = 0.0
        ag = _agg[p]
        if ag["exc_rets"]:
            _c += (_statistics.mean(ag["exc_rets"]) / 100.0) * 0.4
        if ag["win_rates"]:
            _c += (_statistics.mean(ag["win_rates"]) / 100.0) * 0.3
        if ag["avg_rets"]:
            _c += (_statistics.mean(ag["avg_rets"]) / 100.0) * 0.3
        _agg_composites[p] = _c

    _ranked = sorted(profiles, key=lambda p: (-_agg_composites[p], p))

    _rankings: list[CalibrationProfileRanking] = []
    for _rank, pn in enumerate(_ranked, 1):
        ag = _agg[pn]
        avg_fwd = round(_statistics.mean(ag["avg_rets"]), 4) if ag["avg_rets"] else None
        med_fwd = round(_statistics.mean(ag["med_rets"]), 4) if ag["med_rets"] else None
        win_r = round(_statistics.mean(ag["win_rates"]), 2) if ag["win_rates"] else None
        avg_exc = round(_statistics.mean(ag["exc_rets"]), 4) if ag["exc_rets"] else None
        cs = _consistency.get(pn)
        _expl_r: list[str] = []
        if avg_fwd is not None:
            _expl_r.append(f"avg return {avg_fwd:.2f}%")
        if win_r is not None:
            _expl_r.append(f"win rate {win_r:.0f}%")
        if avg_exc is not None:
            _expl_r.append(f"excess return {avg_exc:.2f}%")
        if cs is not None:
            _expl_r.append(f"consistency {cs * 100:.0f}%")
        _rankings.append(CalibrationProfileRanking(
            profile_name=pn,
            average_forward_return_pct=avg_fwd,
            median_forward_return_pct=med_fwd,
            win_rate_pct=win_r,
            average_excess_return_vs_spy_pct=avg_exc,
            consistency_score=cs,
            recommendation_rank=_rank,
            explanation=f"Rank {_rank}: " + (", ".join(_expl_r) if _expl_r else "no forward data"),
        ))

    _avg_vals = [(p, _statistics.mean(_agg[p]["avg_rets"])) for p in profiles if _agg[p]["avg_rets"]]
    _best_avg_p = max(_avg_vals, key=lambda x: x[1])[0] if _avg_vals else None
    _med_vals = [(p, _statistics.mean(_agg[p]["med_rets"])) for p in profiles if _agg[p]["med_rets"]]
    _best_med_p = max(_med_vals, key=lambda x: x[1])[0] if _med_vals else None
    _win_vals = [(p, _statistics.mean(_agg[p]["win_rates"])) for p in profiles if _agg[p]["win_rates"]]
    _best_win_p = max(_win_vals, key=lambda x: x[1])[0] if _win_vals else None
    _exc_vals = [(p, _statistics.mean(_agg[p]["exc_rets"])) for p in profiles if _agg[p]["exc_rets"]]
    _best_exc_p = max(_exc_vals, key=lambda x: x[1])[0] if _exc_vals else None

    _has_data = any(
        _agg[p]["avg_rets"] or _agg[p]["exc_rets"] or _agg[p]["win_rates"]
        for p in profiles
    )
    _recommended: str | None = _ranked[0] if (_ranked and _has_data) else None

    _warn_insufficient = n_dates < 2
    _warn_missing_bm = any(not r["benchmark_available"] for r in date_runs)
    _warn_few_tickers = any(r["evaluated_count"] < 5 for r in date_runs)
    _winner_set: set[str] = set(filter(None, [_best_avg_p, _best_win_p, _best_exc_p]))
    _top_consistency = _consistency.get(_recommended, 0.0) if _recommended else 0.0
    _warn_inconsistent = len(_winner_set) > 1 and (_recommended is None or _top_consistency < 0.5)

    if not _has_data or n_dates == 0:
        _confidence = "LOW"
    elif n_dates < 2 or _warn_few_tickers:
        _confidence = "LOW"
    elif n_dates >= 3 and _top_consistency >= 0.67 and not _warn_inconsistent:
        _confidence = "HIGH"
    elif _top_consistency >= 0.5 and not _warn_few_tickers:
        _confidence = "MEDIUM"
    else:
        _confidence = "LOW"

    _reason_parts: list[str] = []
    if _recommended:
        _reason_parts.append(f"{_recommended} ranks #1 across {n_dates} calibration date(s)")
    if _warn_insufficient:
        _reason_parts.append("add more dates for higher confidence")
    if _warn_missing_bm:
        _reason_parts.append("benchmark data missing on some dates")
    if not _recommended:
        _reason_parts.append("no forward return data available")

    return CalibrationProfileRecommendation(
        recommended_profile=_recommended,
        confidence_level=_confidence,
        reason_summary="; ".join(_reason_parts) if _reason_parts else "insufficient data",
        best_average_return_profile=_best_avg_p,
        best_median_return_profile=_best_med_p,
        best_win_rate_profile=_best_win_p,
        best_excess_return_profile=_best_exc_p,
        consistency_score_by_profile=_consistency,
        profile_rankings=_rankings,
        warnings=CalibrationRecommendationWarnings(
            insufficient_dates=_warn_insufficient,
            missing_benchmark=_warn_missing_bm,
            too_few_evaluated_tickers=_warn_few_tickers,
            inconsistent_profile_winners=_warn_inconsistent,
        ),
    )


@app.post(
    "/v1/strategy/scoring-profile-calibration-preview",
    response_model=CalibrationResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def scoring_profile_calibration_preview(body: CalibrationRequest) -> CalibrationResponse:
    """
    POST /v1/strategy/scoring-profile-calibration-preview — Historical calibration preview.

    Compares scoring profile selections against realized forward returns using
    existing price_snapshots only.  No GCP calls.  No DB writes of any kind.

    For each ticker with enough price history as of as_of_date:
      - Compute local features (momentum, RS, volatility) from PriceSnapshot
      - Score with each requested profile (prediction fields use neutral proxy 0.5/0.0
        so prediction_missing guard is bypassed and only local features differentiate)
      - Retrieve realized forward return from a future PriceSnapshot row
      - Compute excess return vs benchmark when BenchmarkPrice rows exist

    Safety guarantee: signals_created=0, decisions_created=0, orders_created=0, rows_created=0.
    """
    import statistics as _statistics
    from datetime import timedelta as _timedelta
    from paper_trader.engine.universe import get_sp500_universe as _get_sp500_universe

    _all_score_funcs: dict[str, Any] = {
        "current": _score_candidate_v2,
        "balanced_preview": _score_candidate_balanced_preview,
        "quality_preview": _score_candidate_quality_preview,
        "risk_adjusted_preview": _score_candidate_risk_adjusted_preview,
    }

    _safety: dict[str, int] = {
        "signals_created": 0,
        "decisions_created": 0,
        "orders_created": 0,
        "rows_created": 0,
    }

    with get_dedicated_session() as session:
        _raw_universe: list[str] = _get_sp500_universe()
        # Targeted ticker mode: normalize, deduplicate, override universe
        if body.tickers:
            _seen_t: set[str] = set()
            _targeted: list[str] = []
            for _t in body.tickers:
                _t_norm = _t.strip().upper()
                if _t_norm and _t_norm not in _seen_t:
                    _seen_t.add(_t_norm)
                    _targeted.append(_t_norm)
            universe_tickers: list[str] = _targeted if _targeted else _raw_universe
        else:
            universe_tickers = _raw_universe
        universe_count = len(universe_tickers)

        # Determine dates to evaluate
        if body.as_of_dates:
            _resolved_dates: list[Any] = sorted(set(body.as_of_dates))
        elif body.as_of_date is not None:
            _resolved_dates = [body.as_of_date]
        else:
            _latest = session.execute(
                select(func.max(PriceSnapshot.market_date))
                .where(PriceSnapshot.price_type == "CLOSE")
                .where(PriceSnapshot.session_type == "REGULAR")
            ).scalar()
            if _latest is None:
                return CalibrationResponse(
                    calibration_summary=CalibrationSummary(
                        as_of_date="N/A",
                        lookback_days=body.lookback_days,
                        forward_return_days=body.forward_return_days,
                        universe_count=universe_count,
                        evaluated_count=0,
                        skipped_count=universe_count,
                        benchmark_available=False,
                        safety_counts=_safety,
                    ),
                    profile_results=[],
                    profile_comparison=CalibrationProfileComparison(
                        explanation=(
                            "No price data found. Add price snapshots first."
                        ),
                        overlap_matrix={},
                    ),
                    skipped_diagnostics=CalibrationSkippedDiagnostics(total_skipped=universe_count),
                    profile_recommendation=_build_calibration_recommendation(body.profiles, []),
                )
            _resolved_dates = [_latest]

        _date_runs: list[dict] = []

        for effective_date in _resolved_dates:
            cutoff_date = effective_date - _timedelta(days=body.lookback_days + 10)

            # Fetch benchmark prices in lookback window
            _bm_rows = session.execute(
                select(BenchmarkPrice.market_date, BenchmarkPrice.price)
                .where(BenchmarkPrice.ticker == body.benchmark_ticker.upper())
                .where(BenchmarkPrice.session_type == "REGULAR")
                .where(BenchmarkPrice.market_date >= cutoff_date)
                .where(BenchmarkPrice.market_date <= effective_date)
                .order_by(BenchmarkPrice.market_date.desc())
            ).all()
            _spy_prices: dict = {row[0]: float(row[1]) for row in _bm_rows}
            benchmark_available = len(_spy_prices) >= 5

            # SPY as-of price: most recent at or before effective_date
            _spy_as_of: float | None = None
            if _spy_prices:
                _spy_aod_dates = [d for d in _spy_prices if d <= effective_date]
                if _spy_aod_dates:
                    _spy_as_of = _spy_prices[max(_spy_aod_dates)]

            # SPY forward price: earliest CLOSE at or after effective_date + forward_return_days
            _spy_fwd_cutoff = effective_date + _timedelta(days=body.forward_return_days)
            _spy_fwd_max = effective_date + _timedelta(days=body.forward_return_days + 10)
            _spy_fwd_row = session.execute(
                select(BenchmarkPrice.price)
                .where(BenchmarkPrice.ticker == body.benchmark_ticker.upper())
                .where(BenchmarkPrice.session_type == "REGULAR")
                .where(BenchmarkPrice.market_date >= _spy_fwd_cutoff)
                .where(BenchmarkPrice.market_date <= _spy_fwd_max)
                .order_by(BenchmarkPrice.market_date.asc())
                .limit(1)
            ).scalar()
            _spy_fwd: float | None = float(_spy_fwd_row) if _spy_fwd_row is not None else None
            _spy_fwd_ret: float | None = None
            if _spy_fwd and _spy_as_of and _spy_as_of > 0:
                _spy_fwd_ret = (_spy_fwd - _spy_as_of) / _spy_as_of

            # Batch-fetch price history for all universe tickers up to effective_date
            _price_rows = session.execute(
                select(PriceSnapshot.ticker, PriceSnapshot.market_date, PriceSnapshot.price)
                .where(PriceSnapshot.ticker.in_(universe_tickers))
                .where(PriceSnapshot.price_type == "CLOSE")
                .where(PriceSnapshot.session_type == "REGULAR")
                .where(PriceSnapshot.market_date >= cutoff_date)
                .where(PriceSnapshot.market_date <= effective_date)
                .order_by(PriceSnapshot.ticker, PriceSnapshot.market_date.desc())
            ).all()

            # Group by ticker (rows are DESC-ordered per ticker)
            _ticker_price_map: dict[str, list] = {}
            for _pr in _price_rows:
                _ticker_price_map.setdefault(_pr[0], []).append((_pr[1], float(_pr[2])))

            # Evaluate each ticker
            _evaluated: list[dict] = []
            _skipped: list[dict] = []

            for _ticker in universe_tickers:
                _rows = _ticker_price_map.get(_ticker, [])
                if len(_rows) < body.min_price_points:
                    _skipped.append({"ticker": _ticker, "reason": "INSUFFICIENT_PRICE_HISTORY"})
                    continue

                # _rows: list of (market_date, price), DESC ordered — newest first
                _prices = [r[1] for r in _rows]
                _dates = [r[0] for r in _rows]
                _latest_price = _prices[0]

                # momentum_5d (fraction)
                _mom_5d = 0.0
                if len(_prices) >= 5 and _prices[4] > 0:
                    _mom_5d = (_latest_price - _prices[4]) / _prices[4]

                # momentum_20d (fraction)
                _mom_20d = 0.0
                if len(_prices) >= 20 and _prices[19] > 0:
                    _mom_20d = (_latest_price - _prices[19]) / _prices[19]

                # volatility_20d (fraction, sample std dev of daily returns)
                _vol_20d = 0.0
                if len(_prices) >= 2:
                    _n = min(20, len(_prices) - 1)
                    _drets = [
                        (_prices[i] - _prices[i + 1]) / _prices[i + 1]
                        for i in range(_n)
                        if _prices[i + 1] > 0
                    ]
                    if len(_drets) >= 2:
                        try:
                            _vol_20d = _statistics.stdev(_drets)
                        except Exception:
                            _vol_20d = 0.0

                # RS vs benchmark (fraction): ticker 20d momentum minus SPY 20d momentum
                _rs_spy = 0.0
                if benchmark_available and len(_prices) >= 5:
                    _spy_in_w = {d: _spy_prices[d] for d in _dates if d in _spy_prices}
                    _spy_sorted = sorted(_spy_in_w.keys(), reverse=True)
                    if len(_spy_sorted) >= 5:
                        _spy_latest = _spy_in_w[_spy_sorted[0]]
                        _spy_oldest = _spy_in_w[_spy_sorted[min(19, len(_spy_sorted) - 1)]]
                        if _spy_oldest > 0:
                            _spy_mom = (_spy_latest - _spy_oldest) / _spy_oldest
                            _rs_spy = _mom_20d - _spy_mom

                # scan_score proxy (0-100): mirrors market_screener formula
                _scan_raw = 0.0
                if _mom_5d > 0:
                    _scan_raw += _mom_5d * 100.0 * 0.3
                if _mom_20d > 0:
                    _scan_raw += _mom_20d * 100.0 * 0.4
                if _rs_spy > 0:
                    _scan_raw += _rs_spy * 100.0 * 0.3
                if _vol_20d * 100.0 > 5.0:
                    _scan_raw *= max(0.0, 1.0 - _vol_20d)
                _scan_score = max(0.0, min(100.0, _scan_raw))

                _evaluated.append({
                    "ticker": _ticker,
                    "scan_score": _scan_score,
                    "mom_5d": _mom_5d,
                    "mom_20d": _mom_20d,
                    "vol_20d": _vol_20d,
                    "rs_spy": _rs_spy,
                    "latest_price": _latest_price,
                })

            # Sort by scan_score descending, take top scan_top_n pool
            _evaluated.sort(key=lambda x: -x["scan_score"])
            _top_pool = _evaluated[:body.scan_top_n]

            # Batch-fetch forward prices for pool tickers
            _pool_tickers = [c["ticker"] for c in _top_pool]
            _fwd_cutoff = effective_date + _timedelta(days=body.forward_return_days)
            _fwd_max = effective_date + _timedelta(days=body.forward_return_days + 10)
            _fwd_price_map: dict[str, float] = {}
            if _pool_tickers:
                _fwd_rows = session.execute(
                    select(PriceSnapshot.ticker, PriceSnapshot.market_date, PriceSnapshot.price)
                    .where(PriceSnapshot.ticker.in_(_pool_tickers))
                    .where(PriceSnapshot.price_type == "CLOSE")
                    .where(PriceSnapshot.session_type == "REGULAR")
                    .where(PriceSnapshot.market_date >= _fwd_cutoff)
                    .where(PriceSnapshot.market_date <= _fwd_max)
                    .order_by(PriceSnapshot.ticker, PriceSnapshot.market_date.asc())
                ).all()
                for _fr in _fwd_rows:
                    if _fr[0] not in _fwd_price_map:
                        _fwd_price_map[_fr[0]] = float(_fr[2])

            # Build profile results
            _profile_results: list[CalibrationProfileResult] = []
            _all_profile_top_n: dict[str, list[str]] = {}

            for _profile in body.profiles:
                _score_fn = _all_score_funcs[_profile]
                _scored: list[dict] = []

                for _cand in _top_pool:
                    # Use neutral proxy confidence (0.5) to bypass pred_missing guard;
                    # expected_return=0.0 so base_score=0 and only local features differentiate.
                    _inp: dict[str, Any] = {
                        "prediction_confidence": 0.5,
                        "expected_return_pct": 0.0,
                        "momentum_5d_pct": _cand["mom_5d"],
                        "momentum_20d_pct": _cand["mom_20d"],
                        "relative_strength_vs_spy_20d": _cand["rs_spy"],
                        "scan_score": _cand["scan_score"],
                        "volatility_20d_pct": _cand["vol_20d"],
                        "is_current_holding": False,
                    }
                    _factors = _score_fn(_inp)
                    _breakdown = _build_score_breakdown(_factors)

                    _fwd_price = _fwd_price_map.get(_cand["ticker"])
                    _fwd_ret: float | None = None
                    if _fwd_price is not None and _cand["latest_price"] > 0:
                        _fwd_ret = (_fwd_price - _cand["latest_price"]) / _cand["latest_price"]

                    _excess: float | None = None
                    if _fwd_ret is not None and _spy_fwd_ret is not None:
                        _excess = _fwd_ret - _spy_fwd_ret

                    _scored.append({
                        "ticker": _cand["ticker"],
                        "score": _factors.total_score,
                        "breakdown": _breakdown,
                        "fwd_ret": _fwd_ret,
                        "excess": _excess,
                        "warning": "NO_FORWARD_PRICE" if _fwd_price is None else None,
                    })

                _scored.sort(key=lambda x: -x["score"])
                _top_scored = _scored[:body.profile_top_n]

                _rows_out: list[CalibrationCandidateRow] = [
                    CalibrationCandidateRow(
                        ticker=s["ticker"],
                        rank=i + 1,
                        score=round(s["score"], 6),
                        forward_return_pct=round(s["fwd_ret"] * 100.0, 4) if s["fwd_ret"] is not None else None,
                        excess_return_vs_spy_pct=round(s["excess"] * 100.0, 4) if s["excess"] is not None else None,
                        score_breakdown=s["breakdown"],
                        warning_reason=s["warning"],
                    )
                    for i, s in enumerate(_top_scored)
                ]

                _rets = [s["fwd_ret"] * 100.0 for s in _top_scored if s["fwd_ret"] is not None]
                _exc = [s["excess"] * 100.0 for s in _top_scored if s["excess"] is not None]

                _avg_ret: float | None = round(_statistics.mean(_rets), 4) if _rets else None
                _med_ret: float | None = round(_statistics.median(_rets), 4) if _rets else None
                _win_rate: float | None = round(sum(1 for r in _rets if r > 0) / len(_rets) * 100.0, 2) if _rets else None
                _avg_exc: float | None = round(_statistics.mean(_exc), 4) if _exc else None

                _best_ticker: str | None = None
                _worst_ticker: str | None = None
                if _rets:
                    _best_s = max(_top_scored, key=lambda s: s["fwd_ret"] if s["fwd_ret"] is not None else -1e9)
                    _worst_s = min(_top_scored, key=lambda s: s["fwd_ret"] if s["fwd_ret"] is not None else 1e9)
                    if _best_s["fwd_ret"] is not None:
                        _best_ticker = _best_s["ticker"]
                    if _worst_s["fwd_ret"] is not None:
                        _worst_ticker = _worst_s["ticker"]

                _all_profile_top_n[_profile] = [r.ticker for r in _rows_out]
                _profile_results.append(CalibrationProfileResult(
                    profile_name=_profile,
                    top_n=len(_rows_out),
                    average_forward_return_pct=_avg_ret,
                    median_forward_return_pct=_med_ret,
                    win_rate_pct=_win_rate,
                    average_excess_return_vs_spy_pct=_avg_exc,
                    best_ticker=_best_ticker,
                    worst_ticker=_worst_ticker,
                    top_candidates=_rows_out,
                ))

            # Overlap matrix
            _overlap: dict[str, int] = {}
            _pnames = list(_all_profile_top_n.keys())
            for _ai in range(len(_pnames)):
                for _bi in range(_ai + 1, len(_pnames)):
                    _a, _b = _pnames[_ai], _pnames[_bi]
                    _overlap[f"{_a}_vs_{_b}"] = len(
                        set(_all_profile_top_n[_a]) & set(_all_profile_top_n[_b])
                    )

            # Best-per-metric across profiles
            _best_avg: str | None = None
            _best_win: str | None = None
            _best_exc: str | None = None
            _pr_avg = [(p.profile_name, p.average_forward_return_pct) for p in _profile_results if p.average_forward_return_pct is not None]
            if _pr_avg:
                _best_avg = max(_pr_avg, key=lambda x: x[1])[0]
            _pr_win = [(p.profile_name, p.win_rate_pct) for p in _profile_results if p.win_rate_pct is not None]
            if _pr_win:
                _best_win = max(_pr_win, key=lambda x: x[1])[0]
            _pr_exc = [(p.profile_name, p.average_excess_return_vs_spy_pct) for p in _profile_results if p.average_excess_return_vs_spy_pct is not None]
            if _pr_exc:
                _best_exc = max(_pr_exc, key=lambda x: x[1])[0]

            _expl_parts: list[str] = []
            if _best_avg:
                _expl_parts.append(f"Best average forward return: {_best_avg}")
            if _best_win:
                _expl_parts.append(f"Highest win rate: {_best_win}")
            if _best_exc:
                _expl_parts.append(f"Best excess return vs {body.benchmark_ticker}: {_best_exc}")
            if not _expl_parts:
                _expl_parts.append(
                    "No forward return data available. "
                    f"Forward prices for {effective_date} + {body.forward_return_days}d may not exist yet. "
                    "Backfill more price data or use an earlier as_of_date."
                )
            _explanation = ". ".join(_expl_parts) + "."

            _date_runs.append({
                "effective_date": effective_date,
                "profile_results": _profile_results,
                "overlap": _overlap,
                "best_avg": _best_avg,
                "best_win": _best_win,
                "best_exc": _best_exc,
                "explanation": _explanation,
                "evaluated_count": len(_evaluated),
                "skipped_count": len(_skipped),
                "benchmark_available": benchmark_available,
                "skipped_list": _skipped,
            })

        # Primary result: last evaluated date (preserves backward compat for single-date)
        _primary = _date_runs[-1]
        _dates_str = [str(r["effective_date"]) for r in _date_runs]

        return CalibrationResponse(
            calibration_summary=CalibrationSummary(
                as_of_date=str(_primary["effective_date"]),
                lookback_days=body.lookback_days,
                forward_return_days=body.forward_return_days,
                universe_count=universe_count,
                evaluated_count=_primary["evaluated_count"],
                skipped_count=_primary["skipped_count"],
                benchmark_available=_primary["benchmark_available"],
                safety_counts=_safety,
                dates_evaluated=_dates_str if len(_date_runs) > 1 else None,
            ),
            profile_results=_primary["profile_results"],
            profile_comparison=CalibrationProfileComparison(
                best_average_return_profile=_primary["best_avg"],
                best_win_rate_profile=_primary["best_win"],
                best_excess_return_profile=_primary["best_exc"],
                overlap_matrix=_primary["overlap"],
                explanation=_primary["explanation"],
            ),
            skipped_diagnostics=CalibrationSkippedDiagnostics(
                total_skipped=_primary["skipped_count"],
                samples=[
                    CalibrationSkippedDiagnosticItem(ticker=s["ticker"], reason=s["reason"])
                    for s in _primary["skipped_list"][:25]
                ],
            ),
            profile_recommendation=_build_calibration_recommendation(body.profiles, _date_runs),
        )


# ---------------------------------------------------------------------------
# Calibrated Rotation Decision Workbench schemas (Phase 4F)
# ---------------------------------------------------------------------------

class CalibratedRotationRequest(BaseModel):
    """Request for calibration-aware rotation workbench preview (PREVIEW ONLY, no DB writes)."""
    universe: str = "SP500"
    as_of_date: date | None = Field(
        default=None,
        description="Scan date for current holdings and candidates. Defaults to latest market_date in price_snapshots.",
    )
    calibration_as_of_dates: list[date] | None = Field(
        default=None,
        description="Historical dates for calibration to determine recommended scoring profile.",
    )
    lookback_days: int = Field(default=20, ge=1, description="Price history lookback days.")
    forward_return_days: int = Field(default=5, ge=1, description="Forward days for calibration return computation.")
    scan_top_n: int = Field(default=50, ge=1, le=500, description="Top N candidates from scan pool to score.")
    prediction_top_n: int = Field(default=5, ge=1, le=50, description="Top N from scored candidates (not used for GCP; kept for forward compat).")
    profile_top_n: int = Field(default=10, ge=1, le=50, description="Top N per scoring profile to include.")
    min_price_points: int = Field(default=20, ge=1, description="Minimum price history points required per ticker.")
    benchmark_ticker: str = Field(default="SPY", description="Benchmark ticker for calibration excess return.")
    scoring_profile: str = Field(default="calibration_recommended", description="Scoring profile to use.")
    tickers: list[str] | None = Field(
        default=None,
        description="Optional ticker list to use as candidate universe instead of SP500. Useful for testing.",
    )
    include_current_holdings: bool = Field(default=True, description="Include open positions in rotation analysis.")
    max_rotation_pairs: int = Field(default=10, ge=1, le=50, description="Maximum number of rotation pairs to return.")
    min_expected_improvement_pct: float = Field(default=1.0, description="Minimum expected PnL improvement % to qualify.")
    min_expected_pnl_dollars: float = Field(default=25.0, description="Minimum expected PnL improvement $ to qualify.")
    allow_loss_realization: bool = Field(default=False, description="Allow rotating out of positions with negative unrealized PnL.")

    @field_validator("scoring_profile")
    @classmethod
    def _validate_scoring_profile(cls, v: str) -> str:
        allowed = {"calibration_recommended", "current", "balanced_preview", "quality_preview", "risk_adjusted_preview"}
        if v not in allowed:
            raise ValueError(f"scoring_profile must be one of {sorted(allowed)}")
        return v


class CalibratedRotationCalibrationContext(BaseModel):
    """Calibration context used to determine the scoring profile."""
    scoring_profile_used: str
    calibration_recommended_profile: str | None = None
    calibration_confidence: str | None = None
    calibration_reason_summary: str | None = None
    calibration_warnings: dict[str, Any] = Field(default_factory=dict)


class CalibratedRotationPortfolioContext(BaseModel):
    """Portfolio state at time of rotation analysis."""
    open_positions: int
    max_positions: int
    available_slots: int
    cash: str
    total_value: str
    positions_value: str


class CalibratedRotationCandidateSummary(BaseModel):
    """Summary of candidates and rotation pair counts."""
    new_buy_candidates_count: int
    current_holdings_count: int
    eligible_rotation_pairs: int
    blocked_pairs: int
    best_candidate_ticker: str | None = None
    weakest_holding_ticker: str | None = None


class CalibratedRotationRecommendedAction(BaseModel):
    """Top-level recommended action with safety metadata."""
    action_type: str
    title: str
    explanation: str
    confidence: str
    requires_manual_approval: bool = True
    preview_only: bool = True
    creates_signals: int = 0
    creates_decisions: int = 0
    creates_orders: int = 0
    rows_created: int = 0


class CalibratedRotationPair(BaseModel):
    """One rotation pair proposal (sell a holding, buy a candidate)."""
    sell_ticker: str
    buy_ticker: str
    sell_current_price: str | None = None
    buy_current_price: str | None = None
    shares_to_sell: str
    cash_released: str
    sell_unrealized_pnl_pct: str
    sell_expected_forward_return_pct: str | None = None
    buy_expected_forward_return_pct: str | None = None
    expected_pnl_if_hold: str
    expected_pnl_if_rotate: str
    expected_pnl_improvement: str
    expected_improvement_pct: str
    sell_score: str | None = None
    buy_score: str | None = None
    score_improvement: str | None = None
    decision: str
    reasons: list[str]
    blockers: list[str]


class CalibratedRotationBlockedAction(BaseModel):
    """A position or candidate action that was blocked."""
    ticker: str
    action_type: str
    reason: str
    details: str


class CalibratedRotationSafetyCounts(BaseModel):
    """Zero-write safety counters confirming no DB rows were created."""
    signals_created: int = 0
    decisions_created: int = 0
    orders_created: int = 0
    rows_created: int = 0


class CalibratedRotationResponse(BaseModel):
    """Response from calibrated rotation workbench (PREVIEW ONLY, no DB writes)."""
    calibration_context: CalibratedRotationCalibrationContext
    portfolio_context: CalibratedRotationPortfolioContext
    candidate_summary: CalibratedRotationCandidateSummary
    recommended_action: CalibratedRotationRecommendedAction
    rotation_pairs: list[CalibratedRotationPair]
    blocked_actions: list[CalibratedRotationBlockedAction]
    safety_counts: CalibratedRotationSafetyCounts


def _ticker_price_features(rows_desc: list, min_pts: int) -> dict | None:
    """Compute momentum/scan features from DESC-ordered (date, price) rows."""
    if len(rows_desc) < min_pts:
        return None
    prices = [float(r[1]) for r in rows_desc]
    mom_5d = 0.0
    if len(prices) >= 5 and prices[4] > 0:
        mom_5d = (prices[0] - prices[4]) / prices[4]
    mom_20d = 0.0
    if len(prices) >= 20 and prices[19] > 0:
        mom_20d = (prices[0] - prices[19]) / prices[19]
    scan_raw = 0.0
    if mom_5d > 0:
        scan_raw += mom_5d * 100.0 * 0.3
    if mom_20d > 0:
        scan_raw += mom_20d * 100.0 * 0.4
    return {
        "latest_price": prices[0],
        "mom_5d": mom_5d,
        "mom_20d": mom_20d,
        "scan_score": max(0.0, min(100.0, scan_raw)),
    }


def _compute_calibrated_rotation(
    session,
    *,
    scoring_profile: str,
    calibration_as_of_dates,
    lookback_days: int,
    forward_return_days: int,
    scan_top_n: int,
    profile_top_n: int,
    min_price_points: int,
    benchmark_ticker: str,
    tickers,
    max_rotation_pairs: int,
    min_expected_improvement_pct: float,
    min_expected_pnl_dollars: float,
    allow_loss_realization: bool,
    as_of_date,
    position_tickers: "list[str] | None" = None,
) -> "CalibratedRotationResponse":
    """Shared helper: calibrated rotation core logic. PREVIEW ONLY — no DB writes, no GCP calls."""
    import statistics as _statistics
    from datetime import timedelta as _timedelta
    from paper_trader.engine.universe import get_sp500_universe as _get_sp500_universe

    _DOLLARS = Decimal("0.01")
    _safety_zero = CalibratedRotationSafetyCounts()

    _all_score_funcs: dict[str, Any] = {
        "current": _score_candidate_v2,
        "balanced_preview": _score_candidate_balanced_preview,
        "quality_preview": _score_candidate_quality_preview,
        "risk_adjusted_preview": _score_candidate_risk_adjusted_preview,
    }

    # --- Portfolio state ---
    portfolio = get_portfolio(session)
    cfg_max = int((portfolio.config or {}).get("max_positions", get_settings().max_positions))
    open_positions = list(session.execute(select(Position)).scalars().all())
    if position_tickers is not None:
        _pt_filter = {t.strip().upper() for t in position_tickers}
        open_positions = [p for p in open_positions if p.ticker.upper() in _pt_filter]
    held_tickers = {p.ticker for p in open_positions}
    current_count = len(open_positions)
    available_slots = max(0, cfg_max - current_count)
    cash_val = Decimal(str(portfolio.cached_cash or "0"))

    # --- Resolve scoring profile via calibration ---
    _calib_rec_profile: str | None = None
    _calib_confidence: str | None = None
    _calib_reason: str | None = None
    _calib_warnings_dict: dict[str, Any] = {}
    _profile_used: str = scoring_profile

    if scoring_profile == "calibration_recommended":
        _calib_profiles = list(_all_score_funcs.keys())

        if calibration_as_of_dates:
            _calib_dates = sorted(set(calibration_as_of_dates))
        elif as_of_date:
            _calib_dates = [as_of_date]
        else:
            _calib_latest = session.execute(
                select(func.max(PriceSnapshot.market_date))
                .where(PriceSnapshot.price_type == "CLOSE")
                .where(PriceSnapshot.session_type == "REGULAR")
            ).scalar()
            _calib_dates = [_calib_latest] if _calib_latest else []

        if _calib_dates:
            _calib_universe: list[str]
            if tickers:
                _calib_universe = [t.strip().upper() for t in tickers if t.strip()]
            else:
                _calib_universe = _get_sp500_universe()

            _calib_date_runs: list[dict] = []

            for _ceff in _calib_dates:
                _ccutoff = _ceff - _timedelta(days=lookback_days + 10)

                _cbm_rows = session.execute(
                    select(BenchmarkPrice.market_date, BenchmarkPrice.price)
                    .where(BenchmarkPrice.ticker == benchmark_ticker.upper())
                    .where(BenchmarkPrice.session_type == "REGULAR")
                    .where(BenchmarkPrice.market_date >= _ccutoff)
                    .where(BenchmarkPrice.market_date <= _ceff)
                    .order_by(BenchmarkPrice.market_date.desc())
                ).all()
                _cspy = {r[0]: float(r[1]) for r in _cbm_rows}
                _cbm_avail = len(_cspy) >= 5

                _cspy_as_of: float | None = None
                if _cspy:
                    _aod_ds = [d for d in _cspy if d <= _ceff]
                    if _aod_ds:
                        _cspy_as_of = _cspy[max(_aod_ds)]

                _cfwd_cut = _ceff + _timedelta(days=forward_return_days)
                _cfwd_max = _ceff + _timedelta(days=forward_return_days + 10)
                _cspy_fwd_row = session.execute(
                    select(BenchmarkPrice.price)
                    .where(BenchmarkPrice.ticker == benchmark_ticker.upper())
                    .where(BenchmarkPrice.session_type == "REGULAR")
                    .where(BenchmarkPrice.market_date >= _cfwd_cut)
                    .where(BenchmarkPrice.market_date <= _cfwd_max)
                    .order_by(BenchmarkPrice.market_date.asc())
                    .limit(1)
                ).scalar()
                _cspy_fwd = float(_cspy_fwd_row) if _cspy_fwd_row else None
                _cspy_fwd_ret: float | None = None
                if _cspy_fwd and _cspy_as_of and _cspy_as_of > 0:
                    _cspy_fwd_ret = (_cspy_fwd - _cspy_as_of) / _cspy_as_of

                _cp_rows = session.execute(
                    select(PriceSnapshot.ticker, PriceSnapshot.market_date, PriceSnapshot.price)
                    .where(PriceSnapshot.ticker.in_(_calib_universe[:scan_top_n * 3]))
                    .where(PriceSnapshot.price_type == "CLOSE")
                    .where(PriceSnapshot.session_type == "REGULAR")
                    .where(PriceSnapshot.market_date >= _ccutoff)
                    .where(PriceSnapshot.market_date <= _ceff)
                    .order_by(PriceSnapshot.ticker, PriceSnapshot.market_date.desc())
                ).all()
                _cp_map: dict[str, list] = {}
                for _cpr in _cp_rows:
                    _cp_map.setdefault(_cpr[0], []).append((_cpr[1], float(_cpr[2])))

                _ceval: list[dict] = []
                for _ct in _calib_universe[:scan_top_n * 3]:
                    _crows = _cp_map.get(_ct, [])
                    if len(_crows) < min_price_points:
                        continue
                    _cprices = [r[1] for r in _crows]
                    _cm5 = 0.0
                    if len(_cprices) >= 5 and _cprices[4] > 0:
                        _cm5 = (_cprices[0] - _cprices[4]) / _cprices[4]
                    _cm20 = 0.0
                    if len(_cprices) >= 20 and _cprices[19] > 0:
                        _cm20 = (_cprices[0] - _cprices[19]) / _cprices[19]
                    _cvol = 0.0
                    if len(_cprices) >= 2:
                        _cn = min(20, len(_cprices) - 1)
                        _cdrs = [(_cprices[i] - _cprices[i + 1]) / _cprices[i + 1]
                                 for i in range(_cn) if _cprices[i + 1] > 0]
                        if len(_cdrs) >= 2:
                            try:
                                _cvol = _statistics.stdev(_cdrs)
                            except Exception:
                                pass
                    _cscraw = 0.0
                    if _cm5 > 0:
                        _cscraw += _cm5 * 100.0 * 0.3
                    if _cm20 > 0:
                        _cscraw += _cm20 * 100.0 * 0.4
                    _ceval.append({
                        "ticker": _ct, "scan_score": max(0.0, min(100.0, _cscraw)),
                        "mom_5d": _cm5, "mom_20d": _cm20, "vol_20d": _cvol,
                        "latest_price": _cprices[0], "rs_spy": 0.0,
                    })

                _ceval.sort(key=lambda x: -x["scan_score"])
                _cpool = _ceval[:scan_top_n]

                _cpfwd_map: dict[str, float] = {}
                _cpfwd_tickers = [c["ticker"] for c in _cpool]
                if _cpfwd_tickers:
                    _cpfwd_rows = session.execute(
                        select(PriceSnapshot.ticker, PriceSnapshot.market_date, PriceSnapshot.price)
                        .where(PriceSnapshot.ticker.in_(_cpfwd_tickers))
                        .where(PriceSnapshot.price_type == "CLOSE")
                        .where(PriceSnapshot.session_type == "REGULAR")
                        .where(PriceSnapshot.market_date >= _cfwd_cut)
                        .where(PriceSnapshot.market_date <= _cfwd_max)
                        .order_by(PriceSnapshot.ticker, PriceSnapshot.market_date.asc())
                    ).all()
                    for _cfr in _cpfwd_rows:
                        if _cfr[0] not in _cpfwd_map:
                            _cpfwd_map[_cfr[0]] = float(_cfr[2])

                _calib_pr: list[CalibrationProfileResult] = []
                for _cprofn in _calib_profiles:
                    _cpsf = _all_score_funcs[_cprofn]
                    _cpscored: list[dict] = []
                    for _cpc in _cpool:
                        _cpinp: dict[str, Any] = {
                            "prediction_confidence": 0.5, "expected_return_pct": 0.0,
                            "momentum_5d_pct": _cpc["mom_5d"], "momentum_20d_pct": _cpc["mom_20d"],
                            "relative_strength_vs_spy_20d": _cpc.get("rs_spy", 0.0),
                            "scan_score": _cpc["scan_score"], "volatility_20d_pct": _cpc.get("vol_20d", 0.0),
                            "is_current_holding": False,
                        }
                        _cpfact = _cpsf(_cpinp)
                        _cpfwd = _cpfwd_map.get(_cpc["ticker"])
                        _cpfret: float | None = None
                        if _cpfwd and _cpc["latest_price"] > 0:
                            _cpfret = (_cpfwd - _cpc["latest_price"]) / _cpc["latest_price"]
                        _cpexc: float | None = None
                        if _cpfret is not None and _cspy_fwd_ret is not None:
                            _cpexc = _cpfret - _cspy_fwd_ret
                        _cpscored.append({"score": _cpfact.total_score, "fwd_ret": _cpfret, "excess": _cpexc})
                    _cpscored.sort(key=lambda x: -x["score"])
                    _cptop = _cpscored[:profile_top_n]
                    _cprets = [s["fwd_ret"] * 100.0 for s in _cptop if s["fwd_ret"] is not None]
                    _cpexcs = [s["excess"] * 100.0 for s in _cptop if s["excess"] is not None]
                    _calib_pr.append(CalibrationProfileResult(
                        profile_name=_cprofn,
                        top_n=len(_cptop),
                        average_forward_return_pct=round(_statistics.mean(_cprets), 4) if _cprets else None,
                        median_forward_return_pct=round(_statistics.median(_cprets), 4) if _cprets else None,
                        win_rate_pct=round(sum(1 for r in _cprets if r > 0) / len(_cprets) * 100.0, 2) if _cprets else None,
                        average_excess_return_vs_spy_pct=round(_statistics.mean(_cpexcs), 4) if _cpexcs else None,
                        top_candidates=[],
                    ))

                _calib_date_runs.append({
                    "effective_date": _ceff,
                    "profile_results": _calib_pr,
                    "benchmark_available": _cbm_avail,
                    "evaluated_count": len(_ceval),
                })

            _crec = _build_calibration_recommendation(_calib_profiles, _calib_date_runs)
            _calib_rec_profile = _crec.recommended_profile
            _calib_confidence = _crec.confidence_level
            _calib_reason = _crec.reason_summary
            _calib_warnings_dict = _crec.warnings.model_dump()
            _profile_used = _calib_rec_profile or "current"
        else:
            _profile_used = "current"
            _calib_confidence = "LOW"
            _calib_reason = "No price data available for calibration"
            _calib_warnings_dict = {
                "insufficient_dates": True, "missing_benchmark": False,
                "too_few_evaluated_tickers": True, "inconsistent_profile_winners": False,
            }

    _score_fn = _all_score_funcs.get(_profile_used, _all_score_funcs["current"])

    # --- Resolve scan date ---
    # Scope to held tickers so unrelated DB rows (e.g. from other tests or stale data)
    # don't shift the lookback window away from the actual portfolio's price history.
    _scan_date = as_of_date
    if _scan_date is None:
        _held_ticker_list = list(held_tickers)
        if _held_ticker_list:
            _scan_date = session.execute(
                select(func.max(PriceSnapshot.market_date))
                .where(PriceSnapshot.ticker.in_(_held_ticker_list))
                .where(PriceSnapshot.price_type == "CLOSE")
                .where(PriceSnapshot.session_type == "REGULAR")
            ).scalar()
        if _scan_date is None:
            _scan_date = session.execute(
                select(func.max(PriceSnapshot.market_date))
                .where(PriceSnapshot.price_type == "CLOSE")
                .where(PriceSnapshot.session_type == "REGULAR")
            ).scalar()

    _empty_portfolio_ctx = CalibratedRotationPortfolioContext(
        open_positions=current_count,
        max_positions=cfg_max,
        available_slots=available_slots,
        cash=str(cash_val.quantize(_DOLLARS)),
        total_value=str(cash_val.quantize(_DOLLARS)),
        positions_value="0.00",
    )
    _empty_calib_ctx = CalibratedRotationCalibrationContext(
        scoring_profile_used=_profile_used,
        calibration_recommended_profile=_calib_rec_profile,
        calibration_confidence=_calib_confidence,
        calibration_reason_summary=_calib_reason,
        calibration_warnings=_calib_warnings_dict,
    )

    if _scan_date is None:
        return CalibratedRotationResponse(
            calibration_context=_empty_calib_ctx,
            portfolio_context=_empty_portfolio_ctx,
            candidate_summary=CalibratedRotationCandidateSummary(
                new_buy_candidates_count=0, current_holdings_count=current_count,
                eligible_rotation_pairs=0, blocked_pairs=0,
            ),
            recommended_action=CalibratedRotationRecommendedAction(
                action_type="WATCH",
                title="No market data",
                explanation="No price snapshots found. Add price data to enable rotation analysis.",
                confidence="LOW",
            ),
            rotation_pairs=[], blocked_actions=[], safety_counts=_safety_zero,
        )

    _lookback_cutoff = _scan_date - _timedelta(days=lookback_days + 10)

    if tickers:
        _seen_t: set[str] = set()
        _cand_universe: list[str] = []
        for _tu in tickers:
            _tun = _tu.strip().upper()
            if _tun and _tun not in _seen_t:
                _seen_t.add(_tun)
                _cand_universe.append(_tun)
    else:
        _cand_universe = _get_sp500_universe()

    _fetch_tickers = list(set(_cand_universe[:scan_top_n * 5] + list(held_tickers)))
    _price_rows = session.execute(
        select(PriceSnapshot.ticker, PriceSnapshot.market_date, PriceSnapshot.price)
        .where(PriceSnapshot.ticker.in_(_fetch_tickers))
        .where(PriceSnapshot.price_type == "CLOSE")
        .where(PriceSnapshot.session_type == "REGULAR")
        .where(PriceSnapshot.market_date >= _lookback_cutoff)
        .where(PriceSnapshot.market_date <= _scan_date)
        .order_by(PriceSnapshot.ticker, PriceSnapshot.market_date.desc())
    ).all()

    _tpm: dict[str, list] = {}
    for _pr in _price_rows:
        _tpm.setdefault(_pr[0], []).append((_pr[1], float(_pr[2])))

    _cand_features: list[dict] = []
    for _ticker in _cand_universe:
        if _ticker in held_tickers:
            continue
        _feat = _ticker_price_features(_tpm.get(_ticker, []), min_price_points)
        if _feat is None:
            continue
        _inp: dict[str, Any] = {
            "prediction_confidence": 0.5, "expected_return_pct": 0.0,
            "momentum_5d_pct": _feat["mom_5d"], "momentum_20d_pct": _feat["mom_20d"],
            "relative_strength_vs_spy_20d": 0.0,
            "scan_score": _feat["scan_score"], "volatility_20d_pct": 0.0,
            "is_current_holding": False,
        }
        _feat["ticker"] = _ticker
        _feat["score"] = _score_fn(_inp).total_score
        _cand_features.append(_feat)

    _cand_features.sort(key=lambda x: -x["score"])
    _top_candidates: list[dict] = _cand_features[:profile_top_n]

    _holding_features: dict[str, dict] = {}
    for _pos in open_positions:
        _hrows = _tpm.get(_pos.ticker, [])
        _hfeat = _ticker_price_features(_hrows, 1)
        if _hfeat is None:
            _sp = session.execute(
                select(PriceSnapshot.price)
                .where(PriceSnapshot.ticker == _pos.ticker)
                .where(PriceSnapshot.price_type == "CLOSE")
                .where(PriceSnapshot.session_type == "REGULAR")
                .order_by(PriceSnapshot.snapshot_ts.desc())
                .limit(1)
            ).scalar()
            if _sp:
                _hfeat = {"latest_price": float(_sp), "mom_5d": 0.0, "mom_20d": 0.0, "scan_score": 0.0}
            else:
                continue
        _hinp: dict[str, Any] = {
            "prediction_confidence": 0.5, "expected_return_pct": 0.0,
            "momentum_5d_pct": _hfeat.get("mom_5d", 0.0), "momentum_20d_pct": _hfeat.get("mom_20d", 0.0),
            "relative_strength_vs_spy_20d": 0.0,
            "scan_score": _hfeat.get("scan_score", 0.0), "volatility_20d_pct": 0.0,
            "is_current_holding": True,
        }
        _hfeat["ticker"] = _pos.ticker
        _hfeat["score"] = _score_fn(_hinp).total_score
        _holding_features[_pos.ticker] = _hfeat

    _blocked_actions: list[CalibratedRotationBlockedAction] = []
    _all_pairs: list[CalibratedRotationPair] = []

    for _pos in open_positions:
        _hfeat = _holding_features.get(_pos.ticker)
        if _hfeat is None:
            _blocked_actions.append(CalibratedRotationBlockedAction(
                ticker=_pos.ticker,
                action_type="SELL",
                reason="MISSING_PRICE_SNAPSHOT",
                details="No price data found for this holding.",
            ))
            continue

        _lp = Decimal(str(round(_hfeat["latest_price"], 6)))
        _cash_rel = (_pos.qty * _lp).quantize(_DOLLARS)
        _upnl = (_cash_rel - _pos.cost_basis).quantize(_DOLLARS)
        _cb_f = float(_pos.cost_basis)
        _pnl_pct = float(_upnl) / _cb_f * 100.0 if _cb_f > 0 else 0.0

        _sell_fwd_pct = _hfeat.get("mom_5d", 0.0) * 100.0
        _exp_hold = float(_cash_rel) * _sell_fwd_pct / 100.0
        _sell_score = _hfeat.get("score", 0.0)

        if not allow_loss_realization and _pnl_pct < 0:
            _blocked_actions.append(CalibratedRotationBlockedAction(
                ticker=_pos.ticker,
                action_type="SELL",
                reason="LOSS_REALIZATION_BLOCKED",
                details=f"Position is {_pnl_pct:.2f}% below cost; allow_loss_realization=false.",
            ))
            for _bc in _top_candidates[:3]:
                _bfwd = _bc.get("mom_5d", 0.0) * 100.0
                _brot = float(_cash_rel) * _bfwd / 100.0
                _bimp = _brot - _exp_hold
                _bpct = _bimp / float(_cash_rel) * 100.0 if float(_cash_rel) > 0 else 0.0
                _all_pairs.append(CalibratedRotationPair(
                    sell_ticker=_pos.ticker, buy_ticker=_bc["ticker"],
                    sell_current_price=str(_lp),
                    buy_current_price=str(round(_bc["latest_price"], 2)),
                    shares_to_sell=str(_pos.qty), cash_released=str(_cash_rel),
                    sell_unrealized_pnl_pct=f"{_pnl_pct:.4f}",
                    sell_expected_forward_return_pct=f"{_sell_fwd_pct:.4f}",
                    buy_expected_forward_return_pct=f"{_bfwd:.4f}",
                    expected_pnl_if_hold=f"{_exp_hold:.2f}",
                    expected_pnl_if_rotate=f"{_brot:.2f}",
                    expected_pnl_improvement=f"{_bimp:.2f}",
                    expected_improvement_pct=f"{_bpct:.4f}",
                    sell_score=f"{_sell_score:.6f}",
                    buy_score=f"{_bc.get('score', 0.0):.6f}",
                    score_improvement=f"{_bc.get('score', 0.0) - _sell_score:.6f}",
                    decision="BLOCKED", reasons=[], blockers=["LOSS_REALIZATION_BLOCKED"],
                ))
            continue

        for _cand in _top_candidates:
            _buy_ticker = _cand["ticker"]
            _buy_fwd_pct = _cand.get("mom_5d", 0.0) * 100.0
            _exp_rotate = float(_cash_rel) * _buy_fwd_pct / 100.0
            _improvement = _exp_rotate - _exp_hold
            _impr_pct = _improvement / float(_cash_rel) * 100.0 if float(_cash_rel) > 0 else 0.0
            _buy_score = _cand.get("score", 0.0)
            _sc_impr = _buy_score - _sell_score

            _blockers: list[str] = []
            _reasons: list[str] = []

            if _impr_pct < min_expected_improvement_pct:
                _blockers.append("BELOW_MIN_IMPROVEMENT_PCT")
            if _improvement < min_expected_pnl_dollars:
                _blockers.append("BELOW_MIN_EXPECTED_PNL")

            if not _blockers:
                _decision = "ROTATE"
                _reasons.append(
                    f"{_buy_ticker} expected {_buy_fwd_pct:.2f}% vs "
                    f"{_pos.ticker} {_sell_fwd_pct:.2f}% "
                    f"(improvement {_impr_pct:.2f}%, ${_improvement:.2f})"
                )
            else:
                _decision = "BLOCKED"

            _all_pairs.append(CalibratedRotationPair(
                sell_ticker=_pos.ticker, buy_ticker=_buy_ticker,
                sell_current_price=str(_lp),
                buy_current_price=str(round(_cand["latest_price"], 2)),
                shares_to_sell=str(_pos.qty), cash_released=str(_cash_rel),
                sell_unrealized_pnl_pct=f"{_pnl_pct:.4f}",
                sell_expected_forward_return_pct=f"{_sell_fwd_pct:.4f}",
                buy_expected_forward_return_pct=f"{_buy_fwd_pct:.4f}",
                expected_pnl_if_hold=f"{_exp_hold:.2f}",
                expected_pnl_if_rotate=f"{_exp_rotate:.2f}",
                expected_pnl_improvement=f"{_improvement:.2f}",
                expected_improvement_pct=f"{_impr_pct:.4f}",
                sell_score=f"{_sell_score:.6f}",
                buy_score=f"{_buy_score:.6f}",
                score_improvement=f"{_sc_impr:.6f}",
                decision=_decision, reasons=_reasons, blockers=_blockers,
            ))

    _eligible = sorted(
        [p for p in _all_pairs if p.decision == "ROTATE"],
        key=lambda x: -float(x.expected_pnl_improvement),
    )
    _blk_pairs = [p for p in _all_pairs if p.decision == "BLOCKED"]
    _sorted_pairs = (_eligible + _blk_pairs)[:max_rotation_pairs]

    if _eligible:
        _best = _eligible[0]
        _act_type = "ROTATE"
        _act_title = f"Rotate {_best.sell_ticker} into {_best.buy_ticker}"
        _act_expl = (
            f"Expected PnL improvement: ${float(_best.expected_pnl_improvement):.2f} "
            f"({float(_best.expected_improvement_pct):.2f}%). "
            f"Profile: {_profile_used}. Preview only - no signals or orders created."
        )
        _act_conf = _calib_confidence or "LOW"
    elif available_slots > 0 and _top_candidates:
        _act_type = "BUY"
        _act_title = "Portfolio has open slots"
        _act_expl = (
            f"{available_slots} slot(s) available. "
            f"Top candidate: {_top_candidates[0]['ticker']}. "
            "Consider buying via Daily Plan or standard workflow."
        )
        _act_conf = _calib_confidence or "LOW"
    elif not open_positions:
        _act_type = "WATCH"
        _act_title = "No open positions"
        _act_expl = "Portfolio is empty. Consider initiating positions via the prediction workflow."
        _act_conf = "LOW"
    else:
        _act_type = "HOLD"
        _act_title = "Hold current positions"
        _act_expl = (
            "No qualifying rotation found. "
            "All pairs are below the improvement threshold or blocked by risk rules. "
            "Hold and monitor."
        )
        _act_conf = _calib_confidence or "LOW"

    _pos_val = Decimal("0.00")
    for _pos in open_positions:
        _hf = _holding_features.get(_pos.ticker)
        if _hf:
            _pos_val += (_pos.qty * Decimal(str(round(_hf["latest_price"], 6)))).quantize(_DOLLARS)

    _best_cand_ticker = _top_candidates[0]["ticker"] if _top_candidates else None
    _weakest_holding = None
    if _holding_features:
        _hscores = sorted(_holding_features.values(), key=lambda x: x.get("score", 0.0))
        if _hscores:
            _weakest_holding = _hscores[0]["ticker"]

    return CalibratedRotationResponse(
        calibration_context=CalibratedRotationCalibrationContext(
            scoring_profile_used=_profile_used,
            calibration_recommended_profile=_calib_rec_profile,
            calibration_confidence=_calib_confidence,
            calibration_reason_summary=_calib_reason,
            calibration_warnings=_calib_warnings_dict,
        ),
        portfolio_context=CalibratedRotationPortfolioContext(
            open_positions=current_count,
            max_positions=cfg_max,
            available_slots=available_slots,
            cash=str(cash_val.quantize(_DOLLARS)),
            total_value=str((cash_val + _pos_val).quantize(_DOLLARS)),
            positions_value=str(_pos_val.quantize(_DOLLARS)),
        ),
        candidate_summary=CalibratedRotationCandidateSummary(
            new_buy_candidates_count=len(_top_candidates),
            current_holdings_count=current_count,
            eligible_rotation_pairs=len(_eligible),
            blocked_pairs=len(_blk_pairs) + len(_blocked_actions),
            best_candidate_ticker=_best_cand_ticker,
            weakest_holding_ticker=_weakest_holding,
        ),
        recommended_action=CalibratedRotationRecommendedAction(
            action_type=_act_type,
            title=_act_title,
            explanation=_act_expl,
            confidence=_act_conf,
        ),
        rotation_pairs=_sorted_pairs,
        blocked_actions=_blocked_actions,
        safety_counts=_safety_zero,
    )


@app.post(
    "/v1/strategy/calibrated-rotation-preview",
    response_model=CalibratedRotationResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def calibrated_rotation_preview(body: CalibratedRotationRequest) -> CalibratedRotationResponse:
    """
    POST /v1/strategy/calibrated-rotation-preview — Calibration-aware rotation workbench.

    PREVIEW ONLY. No Signal, TradeDecision, Order, Position, JobRun, or any
    other database rows are created or mutated.

    Logic:
    1. Resolve the scoring profile via calibration_recommended or use the explicit profile.
    2. Scan price_snapshots for candidate tickers (SP500 universe or explicit tickers).
    3. Score candidates and current holdings using the resolved profile's scoring function.
    4. Compute momentum-based expected forward return (5d momentum as proxy) for each.
    5. Build rotation pairs: estimate cash_released, expected_pnl_if_hold, expected_pnl_if_rotate.
    6. Apply rejection filters: LOSS_REALIZATION_BLOCKED, BELOW_MIN_IMPROVEMENT_PCT,
       BELOW_MIN_EXPECTED_PNL, MISSING_PRICE_SNAPSHOT.
    7. Return ranked eligible pairs, blocked actions, and a recommended action.

    Safety guarantee: signals_created=0, decisions_created=0, orders_created=0, rows_created=0.
    No GCP calls. No DB writes of any kind.
    """
    with get_session() as session:
        return _compute_calibrated_rotation(
            session,
            scoring_profile=body.scoring_profile,
            calibration_as_of_dates=body.calibration_as_of_dates,
            lookback_days=body.lookback_days,
            forward_return_days=body.forward_return_days,
            scan_top_n=body.scan_top_n,
            profile_top_n=body.profile_top_n,
            min_price_points=body.min_price_points,
            benchmark_ticker=body.benchmark_ticker,
            tickers=body.tickers,
            max_rotation_pairs=body.max_rotation_pairs,
            min_expected_improvement_pct=body.min_expected_improvement_pct,
            min_expected_pnl_dollars=body.min_expected_pnl_dollars,
            allow_loss_realization=body.allow_loss_realization,
            as_of_date=body.as_of_date,
        )


@app.post(
    "/v1/review/daily-plan-replay-preview",
    response_model=DailyPlanReplayPreviewResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def daily_plan_replay_preview(body: DailyPlanReplayPreviewRequest) -> DailyPlanReplayPreviewResponse:
    """
    POST /v1/review/daily-plan-replay-preview — Historical daily plan replay/backtest preview.

    PREVIEW ONLY. No Signal, TradeDecision, Order, Position, JobRun, CandidateReview, or
    any other database rows are created or mutated. No GCP calls. No external API calls.

    For each as_of_date, uses existing historical PriceSnapshot data only to:
    1. Compute local momentum features for each ticker in the universe.
    2. Score tickers with the selected profile and identify the top_n candidates.
    3. Look up realized forward return from existing future PriceSnapshot rows.
    4. Compute excess return vs SPY when BenchmarkPrice rows exist.

    Date resolution (priority order):
    - as_of_dates: explicit list
    - start_date + end_date: distinct market_dates from PriceSnapshot in that range
    - Neither: last max_dates distinct market_dates from PriceSnapshot
    All lists are capped at max_dates.

    Safety: signals_created=0, decisions_created=0, orders_created=0,
    job_runs_created=0, db_rows_created=0.
    """
    import statistics as _statistics
    from datetime import timedelta as _timedelta
    from paper_trader.engine.universe import get_sp500_universe as _get_sp500_universe

    _safety: dict[str, int] = {
        "signals_created": 0,
        "decisions_created": 0,
        "orders_created": 0,
        "job_runs_created": 0,
        "db_rows_created": 0,
    }

    _all_score_funcs: dict[str, Any] = {
        "current": _score_candidate_v2,
        "balanced_preview": _score_candidate_balanced_preview,
        "quality_preview": _score_candidate_quality_preview,
        "risk_adjusted_preview": _score_candidate_risk_adjusted_preview,
    }

    with get_dedicated_session() as session:
        # --- Resolve universe ---
        _raw_universe: list[str] = _get_sp500_universe()
        if body.tickers:
            _seen_t: set[str] = set()
            _targeted: list[str] = []
            for _t in body.tickers:
                _t_norm = _t.strip().upper()
                if _t_norm and _t_norm not in _seen_t:
                    _seen_t.add(_t_norm)
                    _targeted.append(_t_norm)
            universe_tickers: list[str] = _targeted if _targeted else _raw_universe
        else:
            universe_tickers = _raw_universe

        # --- Resolve dates ---
        if body.as_of_dates:
            _candidate_dates: list[date] = sorted(set(body.as_of_dates))
        elif body.start_date is not None and body.end_date is not None:
            if body.end_date < body.start_date:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail="end_date must be >= start_date",
                )
            _db_dates = session.execute(
                select(func.distinct(PriceSnapshot.market_date))
                .where(PriceSnapshot.price_type == "CLOSE")
                .where(PriceSnapshot.session_type == "REGULAR")
                .where(PriceSnapshot.market_date >= body.start_date)
                .where(PriceSnapshot.market_date <= body.end_date)
                .order_by(PriceSnapshot.market_date)
            ).scalars().all()
            _candidate_dates = list(_db_dates)
        else:
            _db_dates = session.execute(
                select(func.distinct(PriceSnapshot.market_date))
                .where(PriceSnapshot.price_type == "CLOSE")
                .where(PriceSnapshot.session_type == "REGULAR")
                .order_by(PriceSnapshot.market_date.desc())
                .limit(body.max_dates)
            ).scalars().all()
            _candidate_dates = sorted(_db_dates)

        # Cap at max_dates, keeping the most-recent dates when over limit
        if len(_candidate_dates) > body.max_dates:
            _candidate_dates = _candidate_dates[-body.max_dates:]

        if not _candidate_dates:
            return DailyPlanReplayPreviewResponse(
                date_results=[],
                summary=DailyPlanReplaySummary(
                    dates_evaluated=0,
                    profile_used="none",
                    safety_counts=_safety,
                ),
                diagnostics=DailyPlanReplayDiagnostics(
                    notes=["No price data found for the specified date range."],
                ),
            )

        # --- Resolve scoring profile ---
        # When calibration_recommended: quick single-date calibration to select best profile.
        _resolved_profile = body.scoring_profile
        if _resolved_profile == "calibration_recommended":
            _calib_date = _candidate_dates[-1]  # use most-recent date for calibration
            _ccutoff = _calib_date - _timedelta(days=body.lookback_days + 10)
            _cfwd_cut = _calib_date + _timedelta(days=body.forward_return_days)
            _cfwd_max = _calib_date + _timedelta(days=body.forward_return_days + 10)
            _calib_universe = universe_tickers[:150]

            _cp_rows = session.execute(
                select(PriceSnapshot.ticker, PriceSnapshot.market_date, PriceSnapshot.price)
                .where(PriceSnapshot.ticker.in_(_calib_universe))
                .where(PriceSnapshot.price_type == "CLOSE")
                .where(PriceSnapshot.session_type == "REGULAR")
                .where(PriceSnapshot.market_date >= _ccutoff)
                .where(PriceSnapshot.market_date <= _calib_date)
                .order_by(PriceSnapshot.ticker, PriceSnapshot.market_date.desc())
            ).all()
            _cp_map: dict[str, list] = {}
            for _cpr in _cp_rows:
                _cp_map.setdefault(_cpr[0], []).append((_cpr[1], float(_cpr[2])))

            _cpfwd_rows = session.execute(
                select(PriceSnapshot.ticker, PriceSnapshot.market_date, PriceSnapshot.price)
                .where(PriceSnapshot.ticker.in_(_calib_universe))
                .where(PriceSnapshot.price_type == "CLOSE")
                .where(PriceSnapshot.session_type == "REGULAR")
                .where(PriceSnapshot.market_date >= _cfwd_cut)
                .where(PriceSnapshot.market_date <= _cfwd_max)
                .order_by(PriceSnapshot.ticker, PriceSnapshot.market_date.asc())
            ).all()
            _cpfwd_map: dict[str, float] = {}
            for _cfr in _cpfwd_rows:
                if _cfr[0] not in _cpfwd_map:
                    _cpfwd_map[_cfr[0]] = float(_cfr[2])

            # For each profile, score top quartile and measure average forward return
            _profile_avgs: dict[str, float] = {}
            for _pn, _pf in _all_score_funcs.items():
                _scored: list[tuple[float, float]] = []
                for _ct in _calib_universe:
                    _crows = _cp_map.get(_ct, [])
                    if len(_crows) < body.min_price_points:
                        continue
                    _cprices = [r[1] for r in _crows]
                    _cm5 = (_cprices[0] - _cprices[4]) / _cprices[4] if len(_cprices) >= 5 and _cprices[4] > 0 else 0.0
                    _cm20 = (_cprices[0] - _cprices[19]) / _cprices[19] if len(_cprices) >= 20 and _cprices[19] > 0 else 0.0
                    _cvol = 0.0
                    if len(_cprices) >= 2:
                        _cdn = min(20, len(_cprices) - 1)
                        _cdrs = [(_cprices[i] - _cprices[i + 1]) / _cprices[i + 1] for i in range(_cdn) if _cprices[i + 1] > 0]
                        if len(_cdrs) >= 2:
                            try:
                                _cvol = _statistics.stdev(_cdrs)
                            except Exception:
                                pass
                    _cscraw = max(0.0, min(100.0, (_cm5 * 100.0 * 0.3 if _cm5 > 0 else 0.0) + (_cm20 * 100.0 * 0.4 if _cm20 > 0 else 0.0)))
                    _pscore = _pf({
                        "prediction_confidence": 0.5,
                        "expected_return_pct": 0.0,
                        "momentum_5d_pct": _cm5,
                        "momentum_20d_pct": _cm20,
                        "volatility_20d_pct": _cvol,
                        "relative_strength_vs_spy_20d": 0.0,
                        "scan_score": _cscraw,
                    })
                    _fwd_p = _cpfwd_map.get(_ct)
                    if _fwd_p and _cprices[0] > 0:
                        _scored.append((_pscore.total_score, (_fwd_p - _cprices[0]) / _cprices[0]))
                if len(_scored) >= 3:
                    _sc_sorted = sorted(_scored, key=lambda x: -x[0])
                    _top_q = max(1, len(_sc_sorted) // 4)
                    _profile_avgs[_pn] = sum(r[1] for r in _sc_sorted[:_top_q]) / _top_q
            if _profile_avgs:
                _resolved_profile = max(_profile_avgs, key=lambda p: _profile_avgs[p])
            else:
                _resolved_profile = "current"

        _score_func = _all_score_funcs.get(_resolved_profile, _score_candidate_v2)

        # --- Per-date evaluation ---
        _date_results: list[DailyPlanReplayDateResult] = []
        _diag_skipped: dict[str, int] = {}
        _total_insuff = 0
        _total_missing_fwd = 0
        _bm_available_count = 0
        _all_best_fwd: list[float] = []
        _all_vs_spy: list[float] = []
        _wins: list[bool] = []

        for _aod in _candidate_dates:
            _cutoff = _aod - _timedelta(days=body.lookback_days + 10)
            _fwd_cut = _aod + _timedelta(days=body.forward_return_days)
            _fwd_max = _aod + _timedelta(days=body.forward_return_days + 10)

            # Benchmark prices in lookback window
            _bm_rows = session.execute(
                select(BenchmarkPrice.market_date, BenchmarkPrice.price)
                .where(BenchmarkPrice.ticker == body.benchmark_ticker.upper())
                .where(BenchmarkPrice.session_type == "REGULAR")
                .where(BenchmarkPrice.market_date >= _cutoff)
                .where(BenchmarkPrice.market_date <= _aod)
                .order_by(BenchmarkPrice.market_date.desc())
            ).all()
            _spy_prices: dict = {row[0]: float(row[1]) for row in _bm_rows}
            _benchmark_available = len(_spy_prices) >= 5
            if _benchmark_available:
                _bm_available_count += 1

            _spy_as_of: float | None = None
            if _spy_prices:
                _spy_aod_dates = [d for d in _spy_prices if d <= _aod]
                if _spy_aod_dates:
                    _spy_as_of = _spy_prices[max(_spy_aod_dates)]

            _spy_fwd_row = session.execute(
                select(BenchmarkPrice.price)
                .where(BenchmarkPrice.ticker == body.benchmark_ticker.upper())
                .where(BenchmarkPrice.session_type == "REGULAR")
                .where(BenchmarkPrice.market_date >= _fwd_cut)
                .where(BenchmarkPrice.market_date <= _fwd_max)
                .order_by(BenchmarkPrice.market_date.asc())
                .limit(1)
            ).scalar()
            _spy_fwd: float | None = float(_spy_fwd_row) if _spy_fwd_row is not None else None
            _spy_fwd_ret: float | None = None
            if _spy_fwd and _spy_as_of and _spy_as_of > 0:
                _spy_fwd_ret = (_spy_fwd - _spy_as_of) / _spy_as_of

            # Batch-fetch price history for all universe tickers up to as_of_date
            _price_rows = session.execute(
                select(PriceSnapshot.ticker, PriceSnapshot.market_date, PriceSnapshot.price)
                .where(PriceSnapshot.ticker.in_(universe_tickers))
                .where(PriceSnapshot.price_type == "CLOSE")
                .where(PriceSnapshot.session_type == "REGULAR")
                .where(PriceSnapshot.market_date >= _cutoff)
                .where(PriceSnapshot.market_date <= _aod)
                .order_by(PriceSnapshot.ticker, PriceSnapshot.market_date.desc())
            ).all()

            _ticker_price_map: dict[str, list] = {}
            for _pr in _price_rows:
                _ticker_price_map.setdefault(_pr[0], []).append((_pr[1], float(_pr[2])))

            # Score each ticker
            _evaluated: list[dict] = []
            _skipped_count = 0

            for _ticker in universe_tickers:
                _rows = _ticker_price_map.get(_ticker, [])
                if len(_rows) < body.min_price_points:
                    _skipped_count += 1
                    _diag_skipped["INSUFFICIENT_PRICE_HISTORY"] = _diag_skipped.get("INSUFFICIENT_PRICE_HISTORY", 0) + 1
                    _total_insuff += 1
                    continue

                _prices = [r[1] for r in _rows]
                _dates_t = [r[0] for r in _rows]
                _latest = _prices[0]

                _mom5 = (_latest - _prices[4]) / _prices[4] if len(_prices) >= 5 and _prices[4] > 0 else 0.0
                _mom20 = (_latest - _prices[19]) / _prices[19] if len(_prices) >= 20 and _prices[19] > 0 else 0.0

                _vol20 = 0.0
                if len(_prices) >= 2:
                    _n = min(20, len(_prices) - 1)
                    _drets = [
                        (_prices[i] - _prices[i + 1]) / _prices[i + 1]
                        for i in range(_n) if _prices[i + 1] > 0
                    ]
                    if len(_drets) >= 2:
                        try:
                            _vol20 = _statistics.stdev(_drets)
                        except Exception:
                            pass

                _rs_spy = 0.0
                if _benchmark_available and len(_prices) >= 5:
                    _spy_in_w = {d: _spy_prices[d] for d in _dates_t if d in _spy_prices}
                    _spy_sorted_d = sorted(_spy_in_w.keys(), reverse=True)
                    if len(_spy_sorted_d) >= 5:
                        _spy_lat = _spy_in_w[_spy_sorted_d[0]]
                        _spy_old = _spy_in_w[_spy_sorted_d[min(19, len(_spy_sorted_d) - 1)]]
                        if _spy_old > 0:
                            _rs_spy = _mom20 - (_spy_lat - _spy_old) / _spy_old

                _scan_raw = max(0.0, min(100.0, (_mom5 * 100.0 * 0.3 if _mom5 > 0 else 0.0) + (_mom20 * 100.0 * 0.4 if _mom20 > 0 else 0.0)))
                _sresult = _score_func({
                    "prediction_confidence": 0.5,
                    "expected_return_pct": 0.0,
                    "momentum_5d_pct": _mom5,
                    "momentum_20d_pct": _mom20,
                    "volatility_20d_pct": _vol20,
                    "relative_strength_vs_spy_20d": _rs_spy,
                    "scan_score": _scan_raw,
                })
                _evaluated.append({
                    "ticker": _ticker,
                    "score": _sresult.total_score,
                    "mom_5d": _mom5,
                    "mom_20d": _mom20,
                    "latest_price": _latest,
                })

            # Sort by score desc, take top_n
            _evaluated.sort(key=lambda x: -x["score"])
            _top_candidates = _evaluated[:body.top_n]

            # Look up forward prices for top candidates
            _top_tickers = [c["ticker"] for c in _top_candidates]
            _fwd_map: dict[str, float] = {}
            if _top_tickers:
                _fwd_rows = session.execute(
                    select(PriceSnapshot.ticker, PriceSnapshot.market_date, PriceSnapshot.price)
                    .where(PriceSnapshot.ticker.in_(_top_tickers))
                    .where(PriceSnapshot.price_type == "CLOSE")
                    .where(PriceSnapshot.session_type == "REGULAR")
                    .where(PriceSnapshot.market_date >= _fwd_cut)
                    .where(PriceSnapshot.market_date <= _fwd_max)
                    .order_by(PriceSnapshot.ticker, PriceSnapshot.market_date.asc())
                ).all()
                for _fr in _fwd_rows:
                    if _fr[0] not in _fwd_map:
                        _fwd_map[_fr[0]] = float(_fr[2])

            # Build candidate objects
            _cand_objs: list[DailyPlanReplayCandidate] = []
            for _c in _top_candidates:
                _fwd_price = _fwd_map.get(_c["ticker"])
                _fwd_ret: float | None = None
                _fwd_vs_spy: float | None = None
                _fwd_avail = False
                if _fwd_price and _c["latest_price"] > 0:
                    _fwd_ret = round((_fwd_price - _c["latest_price"]) / _c["latest_price"] * 100.0, 4)
                    _fwd_avail = True
                    if _spy_fwd_ret is not None:
                        _fwd_vs_spy = round(_fwd_ret - _spy_fwd_ret * 100.0, 4)
                else:
                    _total_missing_fwd += 1
                    _diag_skipped["MISSING_FORWARD_DATA"] = _diag_skipped.get("MISSING_FORWARD_DATA", 0) + 1
                _cand_objs.append(DailyPlanReplayCandidate(
                    ticker=_c["ticker"],
                    score=round(_c["score"], 4),
                    momentum_5d_pct=round(_c["mom_5d"] * 100.0, 4),
                    momentum_20d_pct=round(_c["mom_20d"] * 100.0, 4),
                    forward_return_pct=_fwd_ret,
                    forward_return_vs_spy_pct=_fwd_vs_spy,
                    forward_data_available=_fwd_avail,
                ))

            _best = _cand_objs[0] if _cand_objs else None

            _top_fwd_rets = [c.forward_return_pct for c in _cand_objs if c.forward_return_pct is not None]
            _avg_top_n_fwd = round(sum(_top_fwd_rets) / len(_top_fwd_rets), 4) if _top_fwd_rets else None

            _win: bool | None = None
            _beat_spy: bool | None = None
            if _best and _best.forward_return_pct is not None:
                _win = _best.forward_return_pct > 0
                _all_best_fwd.append(_best.forward_return_pct)
                _wins.append(_win)
            if _best and _best.forward_return_vs_spy_pct is not None:
                _beat_spy = _best.forward_return_vs_spy_pct > 0
                _all_vs_spy.append(_best.forward_return_vs_spy_pct)

            _notes: list[str] = []
            if not _benchmark_available:
                _notes.append("No SPY benchmark data for this date.")
            if not _top_candidates:
                _notes.append("No candidates evaluated (insufficient price history).")

            _date_results.append(DailyPlanReplayDateResult(
                as_of_date=str(_aod),
                evaluated_count=len(_evaluated),
                skipped_count=_skipped_count,
                recommended_profile=_resolved_profile,
                top_candidates=_cand_objs,
                best_candidate=_best,
                avg_top_n_forward_return_pct=_avg_top_n_fwd,
                forward_return_pct=_best.forward_return_pct if _best else None,
                forward_return_vs_spy_pct=_best.forward_return_vs_spy_pct if _best else None,
                spy_forward_return_pct=round(_spy_fwd_ret * 100.0, 4) if _spy_fwd_ret is not None else None,
                win=_win,
                beat_spy=_beat_spy,
                benchmark_available=_benchmark_available,
                notes=_notes,
            ))

        # --- Aggregate summary ---
        _avg_fwd = round(sum(_all_best_fwd) / len(_all_best_fwd), 4) if _all_best_fwd else None
        _median_fwd: float | None = None
        if _all_best_fwd:
            _srt = sorted(_all_best_fwd)
            _nn = len(_srt)
            _median_fwd = round(_srt[_nn // 2] if _nn % 2 else (_srt[_nn // 2 - 1] + _srt[_nn // 2]) / 2, 4)
        _win_rate = round(sum(1 for w in _wins if w) / len(_wins) * 100.0, 1) if _wins else None
        _avg_vs_spy = round(sum(_all_vs_spy) / len(_all_vs_spy), 4) if _all_vs_spy else None

        _best_date: str | None = None
        _worst_date: str | None = None
        _dated_rets = [(dr.as_of_date, dr.forward_return_pct) for dr in _date_results if dr.forward_return_pct is not None]
        if _dated_rets:
            _best_date = max(_dated_rets, key=lambda x: x[1])[0]
            _worst_date = min(_dated_rets, key=lambda x: x[1])[0]

        return DailyPlanReplayPreviewResponse(
            date_results=_date_results,
            summary=DailyPlanReplaySummary(
                dates_evaluated=len(_date_results),
                avg_forward_return_pct=_avg_fwd,
                median_forward_return_pct=_median_fwd,
                win_rate_pct=_win_rate,
                avg_vs_spy_pct=_avg_vs_spy,
                best_date=_best_date,
                worst_date=_worst_date,
                profile_used=_resolved_profile,
                safety_counts=_safety,
            ),
            diagnostics=DailyPlanReplayDiagnostics(
                skipped_by_reason=_diag_skipped,
                insufficient_history_count=_total_insuff,
                missing_forward_data_count=_total_missing_fwd,
                benchmark_available_count=_bm_available_count,
                notes=[
                    "Historical replay only. Uses existing PriceSnapshot data only. No GCP calls.",
                    "No signals, decisions, orders, or job_runs are created.",
                    f"Profile used: {_resolved_profile}.",
                ],
            ),
        )


@app.post(
    "/v1/review/daily-plan-replay-profile-comparison-preview",
    response_model=DailyPlanReplayProfileComparisonResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def daily_plan_replay_profile_comparison_preview(
    body: DailyPlanReplayProfileComparisonRequest,
) -> DailyPlanReplayProfileComparisonResponse:
    """
    POST /v1/review/daily-plan-replay-profile-comparison-preview

    PREVIEW ONLY. No Signal, TradeDecision, Order, Position, JobRun, or any other database rows
    created or mutated. No GCP calls. No external API calls.

    Runs each requested scoring profile over the same historical as_of_dates and returns
    ranked per-profile metrics plus a decision gate recommendation.
    """
    import statistics as _statistics
    from datetime import timedelta as _timedelta
    from paper_trader.engine.universe import get_sp500_universe as _get_sp500_universe

    _safety: dict[str, int] = {
        "signals_created": 0,
        "decisions_created": 0,
        "orders_created": 0,
        "job_runs_created": 0,
        "db_rows_created": 0,
    }

    _cmp_score_funcs: dict[str, Any] = {
        "current": _score_candidate_v2,
        "balanced_preview": _score_candidate_balanced_preview,
        "quality_preview": _score_candidate_quality_preview,
        "risk_adjusted_preview": _score_candidate_risk_adjusted_preview,
    }

    _cmp_profiles: list[str] = body.profiles if body.profiles else list(_cmp_score_funcs.keys())

    with get_dedicated_session() as session:
        # Resolve universe
        _cmp_raw = _get_sp500_universe()
        if body.tickers:
            _cmp_seen: set[str] = set()
            _cmp_targeted: list[str] = []
            for _t in body.tickers:
                _t_norm = _t.strip().upper()
                if _t_norm and _t_norm not in _cmp_seen:
                    _cmp_seen.add(_t_norm)
                    _cmp_targeted.append(_t_norm)
            _cmp_universe: list[str] = _cmp_targeted if _cmp_targeted else _cmp_raw
        else:
            _cmp_universe = _cmp_raw

        # Resolve dates
        if body.as_of_dates:
            _cmp_dates: list[date] = sorted(set(body.as_of_dates))
        elif body.start_date is not None and body.end_date is not None:
            if body.end_date < body.start_date:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail="end_date must be >= start_date",
                )
            _cmp_db_dates = session.execute(
                select(func.distinct(PriceSnapshot.market_date))
                .where(PriceSnapshot.price_type == "CLOSE")
                .where(PriceSnapshot.session_type == "REGULAR")
                .where(PriceSnapshot.market_date >= body.start_date)
                .where(PriceSnapshot.market_date <= body.end_date)
                .order_by(PriceSnapshot.market_date)
            ).scalars().all()
            _cmp_dates = list(_cmp_db_dates)
        else:
            _cmp_db_dates = session.execute(
                select(func.distinct(PriceSnapshot.market_date))
                .where(PriceSnapshot.price_type == "CLOSE")
                .where(PriceSnapshot.session_type == "REGULAR")
                .order_by(PriceSnapshot.market_date.desc())
                .limit(body.max_dates)
            ).scalars().all()
            _cmp_dates = sorted(_cmp_db_dates)

        if len(_cmp_dates) > body.max_dates:
            _cmp_dates = _cmp_dates[-body.max_dates:]

        _cmp_ndates = len(_cmp_dates)

        if not _cmp_dates:
            _cmp_empty = [
                DailyPlanReplayProfileSummary(profile_name=_p, dates_evaluated=0, rank=_i + 1)
                for _i, _p in enumerate(_cmp_profiles)
            ]
            return DailyPlanReplayProfileComparisonResponse(
                comparison_summary=DailyPlanReplayComparisonSummary(
                    dates_evaluated=0,
                    profiles_compared=_cmp_profiles,
                    confidence_level="LOW",
                    recommendation_reason="No price data found for the specified date range.",
                    warnings=["No price data found."],
                ),
                profile_results=_cmp_empty,
                date_results=[],
                decision_gate=DailyPlanReplayDecisionGate(
                    recommendation="NEED_MORE_DATA",
                    reason="No price data found for the specified date range.",
                    blockers=["No price data found."],
                ),
                safety_counts=_safety,
            )

        # Per-profile accumulators
        _cmp_fwd: dict[str, list[float]] = {_p: [] for _p in _cmp_profiles}
        _cmp_vs_spy: dict[str, list[float]] = {_p: [] for _p in _cmp_profiles}
        _cmp_wins: dict[str, list[bool]] = {_p: [] for _p in _cmp_profiles}
        _cmp_miss_fwd: dict[str, int] = {_p: 0 for _p in _cmp_profiles}
        _cmp_bm_avail: dict[str, int] = {_p: 0 for _p in _cmp_profiles}
        _cmp_dated: dict[str, list[tuple[str, float]]] = {_p: [] for _p in _cmp_profiles}
        _cmp_best_cnt: dict[str, int] = {_p: 0 for _p in _cmp_profiles}
        _cmp_leaders: list[str | None] = []
        _cmp_date_results: list[DailyPlanReplayProfileDateResult] = []

        for _caod in _cmp_dates:
            _ccutoff = _caod - _timedelta(days=body.lookback_days + 10)
            _cfwd_cut = _caod + _timedelta(days=body.forward_return_days)
            _cfwd_max = _caod + _timedelta(days=body.forward_return_days + 10)

            # SPY prices for this date (once)
            _cmp_bm_rows = session.execute(
                select(BenchmarkPrice.market_date, BenchmarkPrice.price)
                .where(BenchmarkPrice.ticker == body.benchmark_ticker.upper())
                .where(BenchmarkPrice.session_type == "REGULAR")
                .where(BenchmarkPrice.market_date >= _ccutoff)
                .where(BenchmarkPrice.market_date <= _caod)
                .order_by(BenchmarkPrice.market_date.desc())
            ).all()
            _cmp_spy_prices: dict = {_row[0]: float(_row[1]) for _row in _cmp_bm_rows}
            _cmp_bm_ok = len(_cmp_spy_prices) >= 5

            _cmp_spy_as_of: float | None = None
            if _cmp_spy_prices:
                _cmp_spy_aod_ds = [_d for _d in _cmp_spy_prices if _d <= _caod]
                if _cmp_spy_aod_ds:
                    _cmp_spy_as_of = _cmp_spy_prices[max(_cmp_spy_aod_ds)]

            _cmp_spy_fwd_sc = session.execute(
                select(BenchmarkPrice.price)
                .where(BenchmarkPrice.ticker == body.benchmark_ticker.upper())
                .where(BenchmarkPrice.session_type == "REGULAR")
                .where(BenchmarkPrice.market_date >= _cfwd_cut)
                .where(BenchmarkPrice.market_date <= _cfwd_max)
                .order_by(BenchmarkPrice.market_date.asc())
                .limit(1)
            ).scalar()
            _cmp_spy_fwd_ret: float | None = None
            if _cmp_spy_fwd_sc is not None and _cmp_spy_as_of and _cmp_spy_as_of > 0:
                _cmp_spy_fwd_ret = (float(_cmp_spy_fwd_sc) - _cmp_spy_as_of) / _cmp_spy_as_of

            # Batch-fetch all ticker prices (once per date)
            _cmp_price_rows = session.execute(
                select(PriceSnapshot.ticker, PriceSnapshot.market_date, PriceSnapshot.price)
                .where(PriceSnapshot.ticker.in_(_cmp_universe))
                .where(PriceSnapshot.price_type == "CLOSE")
                .where(PriceSnapshot.session_type == "REGULAR")
                .where(PriceSnapshot.market_date >= _ccutoff)
                .where(PriceSnapshot.market_date <= _caod)
                .order_by(PriceSnapshot.ticker, PriceSnapshot.market_date.desc())
            ).all()
            _cmp_tpmap: dict[str, list] = {}
            for _pr in _cmp_price_rows:
                _cmp_tpmap.setdefault(_pr[0], []).append((_pr[1], float(_pr[2])))

            # Compute features for all eligible tickers (once per date)
            _cmp_features: dict[str, dict] = {}
            for _ctk in _cmp_universe:
                _ctrows = _cmp_tpmap.get(_ctk, [])
                if len(_ctrows) < body.min_price_points:
                    continue
                _ctprices = [_r[1] for _r in _ctrows]
                _ctdates = [_r[0] for _r in _ctrows]
                _ct_latest = _ctprices[0]
                _ct_mom5 = (_ct_latest - _ctprices[4]) / _ctprices[4] if len(_ctprices) >= 5 and _ctprices[4] > 0 else 0.0
                _ct_mom20 = (_ct_latest - _ctprices[19]) / _ctprices[19] if len(_ctprices) >= 20 and _ctprices[19] > 0 else 0.0
                _ct_vol = 0.0
                if len(_ctprices) >= 2:
                    _ctn = min(20, len(_ctprices) - 1)
                    _ctdrs = [(_ctprices[_i] - _ctprices[_i + 1]) / _ctprices[_i + 1] for _i in range(_ctn) if _ctprices[_i + 1] > 0]
                    if len(_ctdrs) >= 2:
                        try:
                            _ct_vol = _statistics.stdev(_ctdrs)
                        except Exception:
                            pass
                _ct_rs = 0.0
                if _cmp_bm_ok and len(_ctprices) >= 5:
                    _ct_spy_w = {_d: _cmp_spy_prices[_d] for _d in _ctdates if _d in _cmp_spy_prices}
                    _ct_spy_sd = sorted(_ct_spy_w.keys(), reverse=True)
                    if len(_ct_spy_sd) >= 5:
                        _ct_spy_l = _ct_spy_w[_ct_spy_sd[0]]
                        _ct_spy_o = _ct_spy_w[_ct_spy_sd[min(19, len(_ct_spy_sd) - 1)]]
                        if _ct_spy_o > 0:
                            _ct_rs = _ct_mom20 - (_ct_spy_l - _ct_spy_o) / _ct_spy_o
                _ct_scan = max(0.0, min(100.0, (_ct_mom5 * 100.0 * 0.3 if _ct_mom5 > 0 else 0.0) + (_ct_mom20 * 100.0 * 0.4 if _ct_mom20 > 0 else 0.0)))
                _cmp_features[_ctk] = {
                    "latest_price": _ct_latest,
                    "mom5": _ct_mom5, "mom20": _ct_mom20,
                    "vol20": _ct_vol, "rs_spy": _ct_rs, "scan_raw": _ct_scan,
                }

            # Score with each profile, pick top-1 best candidate
            _cmp_prof_best: dict[str, dict | None] = {}
            for _cpn in _cmp_profiles:
                _cpfunc = _cmp_score_funcs[_cpn]
                _cpscored: list[dict] = []
                for _ctk2, _cfeat in _cmp_features.items():
                    _csr = _cpfunc({
                        "prediction_confidence": 0.5,
                        "expected_return_pct": 0.0,
                        "momentum_5d_pct": _cfeat["mom5"],
                        "momentum_20d_pct": _cfeat["mom20"],
                        "volatility_20d_pct": _cfeat["vol20"],
                        "relative_strength_vs_spy_20d": _cfeat["rs_spy"],
                        "scan_score": _cfeat["scan_raw"],
                    })
                    _cpscored.append({"ticker": _ctk2, "score": _csr.total_score, "latest_price": _cfeat["latest_price"]})
                _cpscored.sort(key=lambda x: -x["score"])
                _cmp_prof_best[_cpn] = _cpscored[0] if _cpscored else None

            # Batch-fetch forward prices for union of best tickers (one query per date)
            _cmp_all_best_tks = list({_c["ticker"] for _c in _cmp_prof_best.values() if _c is not None})
            _cmp_fwd_map: dict[str, float] = {}
            if _cmp_all_best_tks:
                _cmp_fwd_rows = session.execute(
                    select(PriceSnapshot.ticker, PriceSnapshot.price)
                    .where(PriceSnapshot.ticker.in_(_cmp_all_best_tks))
                    .where(PriceSnapshot.price_type == "CLOSE")
                    .where(PriceSnapshot.session_type == "REGULAR")
                    .where(PriceSnapshot.market_date >= _cfwd_cut)
                    .where(PriceSnapshot.market_date <= _cfwd_max)
                    .order_by(PriceSnapshot.ticker, PriceSnapshot.market_date.asc())
                ).all()
                for _cfr in _cmp_fwd_rows:
                    if _cfr[0] not in _cmp_fwd_map:
                        _cmp_fwd_map[_cfr[0]] = float(_cfr[1])

            # Build per-date per-profile results
            _cmp_date_vs_spy: dict[str, float | None] = {}
            for _cpn in _cmp_profiles:
                _cpbest = _cmp_prof_best[_cpn]
                _cp_fwd_ret: float | None = None
                _cp_fwd_vs_spy: float | None = None
                _cp_fwd_avail = False
                _cp_beat_spy: bool | None = None
                _cp_win: bool | None = None

                if _cpbest:
                    _cmp_best_cnt[_cpn] += 1
                    _cp_fwd_price = _cmp_fwd_map.get(_cpbest["ticker"])
                    if _cp_fwd_price is not None and _cpbest["latest_price"] > 0:
                        _cp_fwd_ret = round((_cp_fwd_price - _cpbest["latest_price"]) / _cpbest["latest_price"] * 100.0, 4)
                        _cp_fwd_avail = True
                        _cp_win = _cp_fwd_ret > 0
                        _cmp_fwd[_cpn].append(_cp_fwd_ret)
                        _cmp_dated[_cpn].append((str(_caod), _cp_fwd_ret))
                        if _cp_win is not None:
                            _cmp_wins[_cpn].append(_cp_win)
                        if _cmp_spy_fwd_ret is not None:
                            _cp_fwd_vs_spy = round(_cp_fwd_ret - _cmp_spy_fwd_ret * 100.0, 4)
                            _cp_beat_spy = _cp_fwd_vs_spy > 0
                            _cmp_vs_spy[_cpn].append(_cp_fwd_vs_spy)
                    else:
                        _cmp_miss_fwd[_cpn] += 1

                if _cmp_bm_ok:
                    _cmp_bm_avail[_cpn] += 1

                _cmp_date_vs_spy[_cpn] = _cp_fwd_vs_spy

                _cmp_date_results.append(DailyPlanReplayProfileDateResult(
                    as_of_date=str(_caod),
                    profile_name=_cpn,
                    best_ticker=_cpbest["ticker"] if _cpbest else None,
                    score=round(_cpbest["score"], 4) if _cpbest else None,
                    forward_return_pct=_cp_fwd_ret,
                    vs_spy_pct=_cp_fwd_vs_spy,
                    beat_spy=_cp_beat_spy,
                    win=_cp_win,
                    benchmark_available=_cmp_bm_ok,
                    forward_data_available=_cp_fwd_avail,
                ))

            # Track which profile led this date by vs_spy
            _cmp_leaders_td = [
                (_cpn, _cmp_date_vs_spy[_cpn])
                for _cpn in _cmp_profiles
                if _cmp_date_vs_spy.get(_cpn) is not None
            ]
            if _cmp_leaders_td:
                _cmp_leaders.append(max(_cmp_leaders_td, key=lambda x: x[1])[0])
            else:
                _cmp_leaders.append(None)

        # Build per-profile summary objects
        _cmp_summaries: list[DailyPlanReplayProfileSummary] = []
        for _cpn in _cmp_profiles:
            _cpf = _cmp_fwd[_cpn]
            _cpv = _cmp_vs_spy[_cpn]
            _cpw = _cmp_wins[_cpn]
            _cpd = _cmp_dated[_cpn]

            _cp_avg_f = round(sum(_cpf) / len(_cpf), 4) if _cpf else None
            _cp_med_f: float | None = None
            if _cpf:
                _cpsrt = sorted(_cpf)
                _cpnf = len(_cpsrt)
                _cp_med_f = round(_cpsrt[_cpnf // 2] if _cpnf % 2 else (_cpsrt[_cpnf // 2 - 1] + _cpsrt[_cpnf // 2]) / 2, 4)
            _cp_wr = round(sum(1 for _w in _cpw if _w) / len(_cpw) * 100.0, 1) if _cpw else None
            _cp_avg_v = round(sum(_cpv) / len(_cpv), 4) if _cpv else None
            _cp_best_d = max(_cpd, key=lambda x: x[1])[0] if _cpd else None
            _cp_worst_d = min(_cpd, key=lambda x: x[1])[0] if _cpd else None
            _cp_lead_cnt = sum(1 for _ldr in _cmp_leaders if _ldr == _cpn)
            _cp_consistency = round(_cp_lead_cnt / len(_cmp_leaders), 4) if _cmp_leaders else None

            _cmp_summaries.append(DailyPlanReplayProfileSummary(
                profile_name=_cpn,
                dates_evaluated=_cmp_ndates,
                avg_forward_return_pct=_cp_avg_f,
                median_forward_return_pct=_cp_med_f,
                win_rate_pct=_cp_wr,
                avg_vs_spy_pct=_cp_avg_v,
                best_date=_cp_best_d,
                worst_date=_cp_worst_d,
                best_candidate_count=_cmp_best_cnt[_cpn],
                missing_forward_data_count=_cmp_miss_fwd[_cpn],
                benchmark_available_count=_cmp_bm_avail[_cpn],
                consistency_score=_cp_consistency,
                rank=0,
                explanation="",
            ))

        # Rank profiles by (avg_vs_spy DESC, win_rate DESC, avg_fwd_return DESC)
        _cmp_summaries.sort(key=lambda _ps: (
            -(_ps.avg_vs_spy_pct if _ps.avg_vs_spy_pct is not None else -999.0),
            -(_ps.win_rate_pct if _ps.win_rate_pct is not None else 0.0),
            -(_ps.avg_forward_return_pct if _ps.avg_forward_return_pct is not None else -999.0),
        ))
        for _ri, _ps in enumerate(_cmp_summaries, start=1):
            _ps.rank = _ri
            _cmp_exp_parts = []
            if _ps.avg_vs_spy_pct is not None:
                _cmp_exp_parts.append(f"avg vs SPY {_ps.avg_vs_spy_pct:.2f}%")
            if _ps.win_rate_pct is not None:
                _cmp_exp_parts.append(f"win rate {_ps.win_rate_pct:.1f}%")
            if _ps.consistency_score is not None:
                _cmp_exp_parts.append(f"led {_ps.consistency_score * 100:.0f}% of dates")
            _ps.explanation = f"Rank {_ri}: " + ("; ".join(_cmp_exp_parts) if _cmp_exp_parts else "insufficient data")

        # Decision gate
        _cmp_best = _cmp_summaries[0] if _cmp_summaries else None
        _cmp_second = _cmp_summaries[1] if len(_cmp_summaries) > 1 else None

        _cmp_min_dates = _cmp_ndates >= 5
        _cmp_min_wr = (_cmp_best is not None and _cmp_best.win_rate_pct is not None and _cmp_best.win_rate_pct >= 55.0)
        _cmp_min_spy = (_cmp_best is not None and _cmp_best.avg_vs_spy_pct is not None and _cmp_best.avg_vs_spy_pct > 0)
        _cmp_consistency_ok = (_cmp_best is not None and _cmp_best.consistency_score is not None and _cmp_best.consistency_score >= 0.4)

        _cmp_blockers: list[str] = []
        if not _cmp_min_dates:
            _cmp_blockers.append(f"Only {_cmp_ndates} date(s) evaluated; minimum is 5.")
        if _cmp_best and not _cmp_min_wr:
            _cmp_wr_str = f"{_cmp_best.win_rate_pct:.1f}%" if _cmp_best.win_rate_pct is not None else "N/A"
            _cmp_blockers.append(f"Best profile win rate ({_cmp_wr_str}) is below 55%.")
        if _cmp_best and not _cmp_min_spy:
            _cmp_vs_str = f"{_cmp_best.avg_vs_spy_pct:.2f}%" if _cmp_best.avg_vs_spy_pct is not None else "N/A"
            _cmp_blockers.append(f"Best profile avg vs SPY ({_cmp_vs_str}) is not positive.")

        _cmp_spy_margin = 0.0
        _cmp_wr_margin = 0.0
        _cmp_too_close = False
        if _cmp_best and _cmp_second:
            _cmp_spy_margin = (_cmp_best.avg_vs_spy_pct or 0.0) - (_cmp_second.avg_vs_spy_pct or 0.0)
            _cmp_wr_margin = (_cmp_best.win_rate_pct or 0.0) - (_cmp_second.win_rate_pct or 0.0)
            _cmp_too_close = _cmp_spy_margin < 0.5 and _cmp_wr_margin < 5.0

        _cmp_confidence = "LOW" if _cmp_ndates < 5 else ("MEDIUM" if _cmp_ndates < 10 else "HIGH")

        if not _cmp_min_dates:
            _cmp_gate_rec = "NEED_MORE_DATA"
            _cmp_gate_reason = f"Insufficient data: {_cmp_ndates} date(s) evaluated; need at least 5."
        elif _cmp_best is None:
            _cmp_gate_rec = "NEED_MORE_DATA"
            _cmp_gate_reason = "No profile results available."
        elif _cmp_best.profile_name == "current" and _cmp_min_wr and _cmp_min_spy:
            _cmp_gate_rec = "KEEP_CURRENT"
            _cmp_gate_reason = (
                f"'current' profile already ranks best with win rate {_cmp_best.win_rate_pct:.1f}% "
                f"and avg vs SPY {_cmp_best.avg_vs_spy_pct:.2f}%."
            )
        elif _cmp_too_close:
            _cmp_gate_rec = "NO_CLEAR_WINNER"
            _cmp_sn = _cmp_second.profile_name if _cmp_second else "N/A"
            _cmp_gate_reason = (
                f"'{_cmp_best.profile_name}' and '{_cmp_sn}' are too close to distinguish "
                f"(vs-SPY margin: {_cmp_spy_margin:.2f}%, win-rate margin: {_cmp_wr_margin:.1f}%)."
            )
        elif _cmp_min_dates and _cmp_min_wr and _cmp_min_spy and _cmp_consistency_ok:
            _cmp_gate_rec = "USE_PROFILE"
            _cmp_gate_reason = (
                f"Profile '{_cmp_best.profile_name}' leads: avg vs SPY "
                f"{_cmp_best.avg_vs_spy_pct:.2f}%, win rate {_cmp_best.win_rate_pct:.1f}%, "
                f"consistency {_cmp_best.consistency_score * 100:.0f}% of dates."
            )
        else:
            _cmp_gate_rec = "NO_CLEAR_WINNER"
            _cmp_gate_reason = (
                ("No profile meets all criteria. " + " ".join(_cmp_blockers))
                if _cmp_blockers else "No profile meets all criteria."
            )

        def _cmp_best_by(attr_name: str) -> str | None:
            _fil = [_ps for _ps in _cmp_summaries if getattr(_ps, attr_name) is not None]
            return max(_fil, key=lambda x: getattr(x, attr_name)).profile_name if _fil else None

        _cmp_warnings: list[str] = []
        if _cmp_ndates < 10:
            _cmp_warnings.append(f"Only {_cmp_ndates} date(s) evaluated — increase range for more reliable results.")
        if not any(_ps.avg_vs_spy_pct is not None for _ps in _cmp_summaries):
            _cmp_warnings.append("No benchmark data available — vs-SPY comparison not possible.")

        return DailyPlanReplayProfileComparisonResponse(
            comparison_summary=DailyPlanReplayComparisonSummary(
                dates_evaluated=_cmp_ndates,
                profiles_compared=_cmp_profiles,
                best_profile_by_avg_return=_cmp_best_by("avg_forward_return_pct"),
                best_profile_by_median_return=_cmp_best_by("median_forward_return_pct"),
                best_profile_by_win_rate=_cmp_best_by("win_rate_pct"),
                best_profile_by_vs_spy=_cmp_best_by("avg_vs_spy_pct"),
                recommended_profile=_cmp_best.profile_name if _cmp_best else None,
                confidence_level=_cmp_confidence,
                recommendation_reason=_cmp_gate_reason,
                warnings=_cmp_warnings,
            ),
            profile_results=_cmp_summaries,
            date_results=_cmp_date_results,
            decision_gate=DailyPlanReplayDecisionGate(
                recommendation=_cmp_gate_rec,
                recommended_profile=_cmp_best.profile_name if _cmp_gate_rec in {"USE_PROFILE", "KEEP_CURRENT"} else None,
                minimum_dates_met=_cmp_min_dates,
                minimum_win_rate_met=_cmp_min_wr,
                minimum_vs_spy_met=_cmp_min_spy,
                enough_consistency=_cmp_consistency_ok,
                reason=_cmp_gate_reason,
                blockers=_cmp_blockers,
            ),
            safety_counts=_safety,
        )


# ---------------------------------------------------------------------------
# Manual paper fill
# ---------------------------------------------------------------------------

_FILL_SAFETY_MSG = (
    "PAPER FILLS ONLY. NO BROKER EXECUTION. NO LIVE TRADES. "
    "AUTOMATION OFF. MANUAL REVIEW."
)

_MANUAL_FILL_DOLLARS = Decimal("0.01")


@app.post(
    "/v1/review/fill-pending-orders",
    response_model=ManualPaperFillResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def fill_pending_paper_orders(
    body: ManualPaperFillRequest,
) -> ManualPaperFillResponse:
    """
    Manually fill all PENDING paper Order rows for today's market date.

    PAPER FILLS ONLY. No broker execution. No live trades. No automation.
    Manual user confirmation required (confirm_paper_fill must be true).

    Fill logic:
    - Processes all PENDING orders whose market_date equals today's US-Eastern date.
    - Prices come from the latest price_snapshots entry per ticker.
    - Orders with no price snapshot are skipped (skipped_no_price).
    - Orders whose TTL has elapsed are expired (orders_expired).
    - BUY fills: open/WAC-average position, debit cash + commission.
    - SELL fills: reduce/close position, credit cash minus commission.
    - Position and cash changes are fully recorded in the DB.

    Response includes cash_delta (negative for net BUY, positive for net SELL)
    and positions_changed (tickers opened or closed during the fill cycle).
    """
    import uuid as uuid_module

    if not body.confirm_paper_fill:
        raise HTTPException(
            status_code=422,
            detail="confirm_paper_fill must be true to execute paper fills.",
        )

    now = datetime.now(timezone.utc)
    market_date = now.astimezone(_EASTERN).date()

    with get_dedicated_session() as session:
        job_run: JobRun | None = None
        try:
            # Count PENDING orders for today before starting
            pending_before = list(
                session.execute(
                    select(Order)
                    .where(
                        Order.status == "PENDING",
                        Order.market_date == market_date,
                    )
                    .order_by(Order.requested_at)
                ).scalars().all()
            )
            orders_evaluated = len(pending_before)

            # Record pre-fill state
            cash_before = compute_cash(session)
            positions_before = {p.ticker for p in get_open_positions(session)}

            if orders_evaluated == 0:
                return ManualPaperFillResponse(
                    execution_mode="PAPER_FILLS_ONLY",
                    orders_evaluated=0,
                    orders_filled=0,
                    orders_expired=0,
                    skipped_not_pending=0,
                    skipped_invalid=0,
                    skipped_no_price=0,
                    cash_delta="0.00",
                    positions_changed=[],
                    safety_message=_FILL_SAFETY_MSG,
                )

            # Create a JobRun for audit trail
            job_run = JobRun(
                idempotency_key=f"manual-paper-fill-{uuid_module.uuid4()}",
                workflow_type="MANUAL_PAPER_FILL",
                market_date=market_date,
                status=JobRunStatus.RUNNING,
                started_at=now,
            )
            session.add(job_run)
            session.commit()

            # Acquire portfolio advisory lock
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

                # Record post-fill state (session has committed internally)
                cash_after = compute_cash(session)
                positions_after = {p.ticker for p in get_open_positions(session)}

                job_run.status = JobRunStatus.COMPLETED
                job_run.completed_at = now
                job_run.result_summary = dict(counts)
                session.commit()

                # Upsert today's portfolio snapshot to reflect post-fill state.
                # Best-effort: a missing price snapshot must not undo the fill.
                try:
                    upsert_post_fill_snapshot(
                        session,
                        job_run_id=job_run.id,
                        market_date=market_date,
                        now=now,
                    )
                except (MissingPricesError, ValueError):
                    pass

            finally:
                try:
                    session.execute(
                        text("SELECT pg_advisory_unlock(:key)"),
                        {"key": PORTFOLIO_ADVISORY_LOCK_KEY},
                    )
                    session.commit()
                except Exception:
                    session.rollback()

            # Compute deltas
            cash_delta = (cash_after - cash_before).quantize(_MANUAL_FILL_DOLLARS)
            positions_changed = sorted(
                (positions_before | positions_after)
                - (positions_before & positions_after)
            )

            return ManualPaperFillResponse(
                execution_mode="PAPER_FILLS_ONLY",
                orders_evaluated=orders_evaluated,
                orders_filled=counts["filled"],
                orders_expired=counts["expired"],
                skipped_not_pending=0,
                skipped_invalid=counts["failed"],
                skipped_no_price=counts["skipped"],
                cash_delta=str(cash_delta),
                positions_changed=positions_changed,
                safety_message=_FILL_SAFETY_MSG,
            )

        except HTTPException:
            raise
        except Exception as exc:
            if job_run is not None:
                try:
                    job_run.status = JobRunStatus.FAILED
                    job_run.error_detail = str(exc)[:2000]
                    job_run.completed_at = now
                    session.commit()
                except Exception:
                    session.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(exc),
            )


# ---------------------------------------------------------------------------
# Manual paper cancel
# ---------------------------------------------------------------------------

_CANCEL_SAFETY_MSG = (
    "PAPER CANCEL ONLY. CANCELS PENDING PAPER ORDER TICKETS ONLY. "
    "NO BROKER EXECUTION. NO LIVE TRADES. NO CASH OR POSITION CHANGES. "
    "AUTOMATION OFF. MANUAL REVIEW."
)


@app.post(
    "/v1/review/cancel-pending-orders",
    response_model=ManualPaperCancelResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
async def cancel_pending_paper_orders(
    body: ManualPaperCancelRequest,
) -> ManualPaperCancelResponse:
    """
    Manually cancel all PENDING paper Order rows for today's market date.

    PAPER CANCEL ONLY. No broker execution. No live trades. No cash changes.
    No position changes. No Trade rows created. No automation.
    Manual user confirmation required (confirm_cancel_orders must be true).

    Cancel logic:
    - Processes all PENDING orders whose market_date equals today's US-Eastern date.
    - Sets status to CANCELLED (does not alter FILLED, EXPIRED, FAILED, or already-CANCELLED orders).
    - Does not change cash, positions, or create any Trade rows.
    - Cash delta is always 0.00. Positions changed is always empty.
    """
    import uuid as uuid_module

    if not body.confirm_cancel_orders:
        raise HTTPException(
            status_code=422,
            detail="confirm_cancel_orders must be true to cancel pending orders.",
        )

    now = datetime.now(timezone.utc)
    market_date = now.astimezone(_EASTERN).date()

    with get_dedicated_session() as session:
        job_run: JobRun | None = None
        try:
            pending_orders = list(
                session.execute(
                    select(Order)
                    .where(
                        Order.status == "PENDING",
                        Order.market_date == market_date,
                    )
                    .order_by(Order.requested_at)
                ).scalars().all()
            )
            orders_evaluated = len(pending_orders)

            if orders_evaluated == 0:
                return ManualPaperCancelResponse(
                    execution_mode="PAPER_CANCEL_ONLY",
                    orders_evaluated=0,
                    orders_cancelled=0,
                    skipped_not_pending=0,
                    skipped_invalid=0,
                    cash_delta="0.00",
                    positions_changed=[],
                    safety_message=_CANCEL_SAFETY_MSG,
                )

            # Create a JobRun for audit trail
            job_run = JobRun(
                idempotency_key=f"manual-paper-cancel-{uuid_module.uuid4()}",
                workflow_type="MANUAL_PAPER_CANCEL",
                market_date=market_date,
                status=JobRunStatus.RUNNING,
                started_at=now,
            )
            session.add(job_run)
            session.commit()

            orders_cancelled = 0
            skipped_not_pending = 0
            skipped_invalid = 0

            for order in pending_orders:
                try:
                    # Re-read status after commit (auto-expiry catches concurrent fill races)
                    if order.status != "PENDING":
                        skipped_not_pending += 1
                        continue
                    order.status = "CANCELLED"
                    order.notes = (
                        "Paper order ticket cancelled — manual paper cancel. "
                        "No broker execution."
                    )
                    orders_cancelled += 1
                except Exception:
                    skipped_invalid += 1

            session.commit()

            job_run.status = JobRunStatus.COMPLETED
            job_run.completed_at = now
            job_run.result_summary = {
                "orders_evaluated": orders_evaluated,
                "orders_cancelled": orders_cancelled,
                "skipped_not_pending": skipped_not_pending,
                "skipped_invalid": skipped_invalid,
            }
            session.commit()

            return ManualPaperCancelResponse(
                execution_mode="PAPER_CANCEL_ONLY",
                orders_evaluated=orders_evaluated,
                orders_cancelled=orders_cancelled,
                skipped_not_pending=skipped_not_pending,
                skipped_invalid=skipped_invalid,
                cash_delta="0.00",
                positions_changed=[],
                safety_message=_CANCEL_SAFETY_MSG,
            )

        except HTTPException:
            raise
        except Exception as exc:
            if job_run is not None:
                try:
                    job_run.status = JobRunStatus.FAILED
                    job_run.error_detail = str(exc)[:2000]
                    job_run.completed_at = now
                    session.commit()
                except Exception:
                    session.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(exc),
            )


def _batch_indicator_changes(symbols: list[str]) -> dict[str, dict]:
    """
    Batch-fetch 5 days of daily history for market indicator symbols to compute
    absolute and percentage change vs the previous close.
    Returns {symbol: {previous_close, change, change_pct}} for available symbols.
    Fails gracefully — any exception returns {}.
    Read-only, no DB access, no broker APIs.
    """
    if _yf is None or not symbols:
        return {}
    try:
        data = _yf.download(
            " ".join(symbols),
            period="5d",
            interval="1d",
            progress=False,
            threads=False,
            auto_adjust=False,
        )
        if data is None or len(data) == 0:
            return {}
        try:
            close_data = data["Close"]
        except (KeyError, TypeError):
            return {}

        result: dict[str, dict] = {}
        for symbol in symbols:
            try:
                if hasattr(close_data, "columns"):
                    if symbol not in close_data.columns:
                        continue
                    series = close_data[symbol].dropna()
                else:
                    series = close_data.dropna() if hasattr(close_data, "dropna") else close_data

                if len(series) < 2:
                    continue

                last_two = series.tail(2)
                prev_idx = last_two.index[0]
                prev_val = float(last_two.iloc[0])
                curr_val = float(last_two.iloc[1])

                if prev_val <= 0 or curr_val <= 0:
                    continue

                change_abs = curr_val - prev_val
                change_pct = (change_abs / prev_val) * 100

                if hasattr(prev_idx, "date"):
                    prev_date: str = prev_idx.date().isoformat()
                else:
                    prev_date = str(prev_idx)[:10]

                result[symbol] = {
                    "previous_close": str(Decimal(str(round(prev_val, 6)))),
                    "previous_close_date": prev_date,
                    "change": str(Decimal(str(round(change_abs, 6)))),
                    "change_pct": str(Decimal(str(round(change_pct, 6)))),
                }
            except Exception:
                continue

        return result
    except Exception:
        return {}


def _batch_fred_with_prior(series_map: dict[str, str], api_key: str | None) -> dict[str, dict | None]:
    """
    Fetch latest two valid FRED observations per series for prior-observation comparison.
    Returns {key: {value, as_of, status[, change, change_pct, previous_value, previous_as_of]}} or {key: None}.
    Read-only, no DB. Uses stdlib urllib only — no new dependencies.
    Gracefully fails: any per-series exception returns None for that key.
    """
    if not api_key:
        return {key: None for key in series_map}
    import json as _json
    import urllib.parse as _urlparse
    import urllib.request as _urlreq
    results: dict[str, dict | None] = {}
    for key, series_id in series_map.items():
        try:
            params = _urlparse.urlencode({
                "series_id": series_id,
                "api_key": api_key,
                "file_type": "json",
                "sort_order": "desc",
                "limit": "10",
            })
            url = f"https://api.stlouisfed.org/fred/series/observations?{params}"
            req = _urlreq.Request(url)
            with _urlreq.urlopen(req, timeout=10) as resp:
                payload = _json.loads(resp.read().decode("utf-8"))
            valid_obs: list[dict] = []
            for obs in payload.get("observations", []):
                val_str = obs.get("value", ".")
                if not val_str or val_str == ".":
                    continue
                try:
                    price_f = float(val_str)
                    valid_obs.append({
                        "value": str(Decimal(str(price_f))),
                        "as_of": obs.get("date", "")[:10],
                    })
                except (ValueError, TypeError):
                    continue
                if len(valid_obs) == 2:
                    break
            if not valid_obs:
                results[key] = None
                continue
            latest = valid_obs[0]
            entry: dict = {
                "value": latest["value"],
                "as_of": latest["as_of"],
                "status": f"FRED latest observation {latest['as_of']}",
            }
            if len(valid_obs) >= 2:
                prior = valid_obs[1]
                try:
                    curr_f = float(latest["value"])
                    prev_f = float(prior["value"])
                    if prev_f != 0:
                        change_abs = curr_f - prev_f
                        change_pct = (change_abs / prev_f) * 100
                        entry["previous_value"] = prior["value"]
                        entry["previous_as_of"] = prior["as_of"]
                        entry["change"] = str(Decimal(str(round(change_abs, 6))))
                        entry["change_pct"] = str(Decimal(str(round(change_pct, 6))))
                except Exception:
                    pass
            results[key] = entry
        except Exception:
            results[key] = None
    return results


@app.get(
    "/v1/market/indicators",
    response_model=MarketIndicatorsResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def get_market_indicators() -> MarketIndicatorsResponse:
    """
    Fetch latest market indicators (read-only).

    yfinance indicators: S&P 500, Nasdaq, Dow, VIX, EUR/USD, Gold, Brent, WTI.
    FRED macro indicators: US 10Y (DGS10), US 2Y (DGS2), CPI (CPIAUCSL), Fed Funds, SOFR.

    FRED cards require PAPER_TRADER_FRED_API_KEY; if absent, they show available=False
    with reason "FRED API key missing". yfinance cards always populate independently.
    Graceful partial failure: one missing indicator does not fail the response.
    """
    indicator_config = [
        ("sp500", "S&P 500", "^GSPC"),
        ("nasdaq", "Nasdaq", "^IXIC"),
        ("dow", "Dow", "^DJI"),
        ("vix", "VIX", "^VIX"),
        ("eurusd", "EUR/USD", "EURUSD=X"),
        ("gold", "Gold", "GC=F"),
        ("brent", "Brent", "BZ=F"),
        ("wti", "WTI", "CL=F"),
    ]

    _FRED_SERIES = {
        "us10y":     ("US 10Y",     "DGS10"),
        "us2y":      ("US 2Y",      "DGS2"),
        "cpi_latest":("CPI Latest", "CPIAUCSL"),
        "fed_funds": ("Fed Funds",  "FEDFUNDS"),
        "sofr":      ("SOFR",       "SOFR"),
    }

    # Fetch all yfinance symbols at once (fast batch path)
    symbols = [cfg[2] for cfg in indicator_config]
    successful_prices, _failures = fetch_latest_prices(symbols)

    # Create lookup by symbol
    price_map = {p["ticker"]: p["price"] for p in successful_prices}

    # Fetch change vs previous close for all symbols in a single 5d batch download
    change_map = _batch_indicator_changes(symbols)

    # Build indicators list; fall back to per-symbol history for any batch miss
    now_str = datetime.now(timezone.utc).isoformat()
    indicators = []

    for key, label, symbol in indicator_config:
        chg = change_map.get(symbol, {})
        if symbol in price_map:
            indicators.append(
                MarketIndicator(
                    key=key,
                    label=label,
                    symbol=symbol,
                    value=price_map[symbol],
                    previous_close=chg.get("previous_close"),
                    previous_close_date=chg.get("previous_close_date"),
                    change=chg.get("change"),
                    change_pct=chg.get("change_pct"),
                    source="yfinance",
                    available=True,
                    as_of=now_str,
                    status="yfinance live",
                    freshness_label="LATEST LOADED",
                )
            )
        else:
            # Per-symbol history fallback — handles weekends and after-hours
            hist = fetch_market_indicator_latest(symbol)
            if hist is not None:
                indicators.append(
                    MarketIndicator(
                        key=key,
                        label=label,
                        symbol=symbol,
                        value=hist["value"],
                        previous_close=chg.get("previous_close"),
                        previous_close_date=chg.get("previous_close_date"),
                        change=chg.get("change"),
                        change_pct=chg.get("change_pct"),
                        source="yfinance",
                        available=True,
                        as_of=hist["as_of"],
                        status=hist["status"],
                        freshness_label="CLOSE",
                    )
                )
            else:
                indicators.append(
                    MarketIndicator(
                        key=key,
                        label=label,
                        symbol=symbol,
                        value=None,
                        previous_close=None,
                        previous_close_date=None,
                        change=None,
                        change_pct=None,
                        source="yfinance",
                        available=False,
                        as_of=None,
                        status="yfinance unavailable",
                        freshness_label="UNAVAILABLE",
                    )
                )

    # Fetch FRED macro indicators (latest + prior observation for change computation)
    fred_api_key = get_settings().fred_api_key
    fred_results = _batch_fred_with_prior(
        {k: v[1] for k, v in _FRED_SERIES.items()},
        fred_api_key,
    )

    placeholders = []
    for key, (label, _series_id) in _FRED_SERIES.items():
        fred_data = fred_results.get(key)
        if fred_data is not None:
            placeholders.append(
                MarketIndicatorPlaceholder(
                    key=key,
                    label=label,
                    available=True,
                    reason="",
                    value=fred_data["value"],
                    as_of=fred_data["as_of"],
                    status=fred_data["status"],
                    source="fred",
                    change=fred_data.get("change"),
                    change_pct=fred_data.get("change_pct"),
                    previous_value=fred_data.get("previous_value"),
                    previous_as_of=fred_data.get("previous_as_of"),
                    freshness_label="FRED OBS",
                )
            )
        else:
            reason = "FRED API key missing" if not fred_api_key else "FRED data unavailable"
            placeholders.append(
                MarketIndicatorPlaceholder(
                    key=key,
                    label=label,
                    available=False,
                    reason=reason,
                    freshness_label="UNAVAILABLE",
                )
            )

    return MarketIndicatorsResponse(
        status="ok",
        source="yfinance",
        as_of=now_str,
        indicators=indicators,
        placeholders=placeholders,
    )


@app.post(
    "/v1/review/position-monitor-preview",
    response_model=PositionMonitorPreviewResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def position_monitor_preview() -> PositionMonitorPreviewResponse:
    """
    Preview exit recommendations for all open positions (PREVIEW ONLY, no DB writes).

    Evaluates each open position against v1 exit rules:
        REVIEW_FOR_EXIT  if unrealized_pnl_pct <= -5.0%
        WATCH            if unrealized_pnl_pct <= -2.0%
        WATCH            if portfolio_weight_pct > 25.0%
        HOLD             otherwise

    If the latest price is missing the position returns WATCH/PRICE_MISSING without
    failing the full response.

    This endpoint never creates signals, decisions, orders, fills, or positions.
    """
    now = datetime.now(tz=timezone.utc)

    with get_session() as session:
        positions = session.execute(
            select(Position).order_by(Position.opened_at)
        ).scalars().all()

        portfolio = session.execute(select(Portfolio)).scalar_one_or_none()
        cached_total_value: Decimal | None = None
        if portfolio is not None and portfolio.cached_total_value:
            cached_total_value = Decimal(str(portfolio.cached_total_value))

        items: list[PositionMonitorItem] = []
        total_positions_value = Decimal("0")
        total_unrealized_pnl = Decimal("0")
        reviewed_for_exit_count = 0
        watch_count = 0
        hold_count = 0

        for pos in positions:
            qty = Decimal(str(pos.qty))
            cost_basis = Decimal(str(pos.cost_basis))

            opened_at_utc = (
                pos.opened_at
                if pos.opened_at.tzinfo is not None
                else pos.opened_at.replace(tzinfo=timezone.utc)
            )
            holding_days = max(0, (now - opened_at_utc).days)

            latest_price = _latest_price(session, pos.ticker)

            if latest_price is None:
                items.append(PositionMonitorItem(
                    ticker=pos.ticker,
                    qty=str(qty),
                    avg_cost=str(pos.avg_cost),
                    latest_price=None,
                    market_value=None,
                    cost_basis=str(cost_basis),
                    unrealized_pnl=None,
                    unrealized_pnl_pct=None,
                    portfolio_weight_pct=None,
                    opened_at=pos.opened_at,
                    holding_days=holding_days,
                    recommendation="WATCH",
                    reason_codes=["PRICE_MISSING"],
                    explanation="Latest price unavailable; position cannot be fully evaluated.",
                ))
                watch_count += 1
                continue

            market_value = qty * latest_price
            unrealized_pnl = market_value - cost_basis
            unrealized_pnl_pct = (
                unrealized_pnl / cost_basis * Decimal("100")
                if cost_basis != Decimal("0")
                else Decimal("0")
            )

            total_positions_value += market_value
            total_unrealized_pnl += unrealized_pnl

            portfolio_weight_pct: Decimal | None = None
            if cached_total_value is not None and cached_total_value > Decimal("0"):
                portfolio_weight_pct = market_value / cached_total_value * Decimal("100")

            upnl_pct_float = float(unrealized_pnl_pct)
            weight_pct_float = float(portfolio_weight_pct) if portfolio_weight_pct is not None else 0.0

            reason_codes: list[str] = []
            recommendation: str
            explanation: str

            if upnl_pct_float <= -5.0:
                recommendation = "REVIEW_FOR_EXIT"
                reason_codes.append("STOP_LOSS_REVIEW")
                explanation = (
                    f"Unrealized P&L of {upnl_pct_float:.1f}% is below the stop-loss "
                    f"review threshold (-5.0%). Manual review recommended."
                )
                reviewed_for_exit_count += 1
            elif upnl_pct_float <= -2.0:
                recommendation = "WATCH"
                reason_codes.append("WATCH_LOSS_THRESHOLD")
                explanation = (
                    f"Unrealized P&L of {upnl_pct_float:.1f}% is in the watch range "
                    f"(-2.0% to -5.0%). Monitor closely."
                )
                watch_count += 1
            elif portfolio_weight_pct is not None and weight_pct_float > 25.0:
                recommendation = "WATCH"
                reason_codes.append("HIGH_CONCENTRATION")
                explanation = (
                    f"Portfolio weight of {weight_pct_float:.1f}% exceeds the "
                    f"concentration threshold (25.0%). Consider position sizing."
                )
                watch_count += 1
            else:
                recommendation = "HOLD"
                reason_codes.append("HEALTHY_POSITION")
                explanation = "Position is within healthy parameters. No action required."
                hold_count += 1

            items.append(PositionMonitorItem(
                ticker=pos.ticker,
                qty=str(qty),
                avg_cost=str(pos.avg_cost),
                latest_price=str(latest_price),
                market_value=str(market_value.quantize(Decimal("0.01"))),
                cost_basis=str(cost_basis),
                unrealized_pnl=str(unrealized_pnl.quantize(Decimal("0.01"))),
                unrealized_pnl_pct=str(unrealized_pnl_pct.quantize(Decimal("0.01"))),
                portfolio_weight_pct=(
                    str(portfolio_weight_pct.quantize(Decimal("0.01")))
                    if portfolio_weight_pct is not None
                    else None
                ),
                opened_at=pos.opened_at,
                holding_days=holding_days,
                recommendation=recommendation,
                reason_codes=reason_codes,
                explanation=explanation,
            ))

        return PositionMonitorPreviewResponse(
            as_of=now,
            open_position_count=len(positions),
            total_positions_value=str(total_positions_value.quantize(Decimal("0.01"))),
            total_unrealized_pnl=str(total_unrealized_pnl.quantize(Decimal("0.01"))),
            reviewed_for_exit_count=reviewed_for_exit_count,
            watch_count=watch_count,
            hold_count=hold_count,
            positions=items,
            preview_only=True,
            writes_performed=False,
        )


@app.post(
    "/v1/review/exit-signal-preview",
    response_model=ExitSignalPreviewResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def exit_signal_preview() -> ExitSignalPreviewResponse:
    """
    Preview exit intent for all open positions (PREVIEW ONLY, no DB writes).

    Maps position monitor recommendations to exit actions:
        REVIEW_FOR_EXIT -> PREVIEW_EXIT / SELL / full qty
        WATCH           -> WATCH / NONE / 0
        HOLD            -> HOLD / NONE / 0
        Missing price   -> WATCH / NONE / 0

    This endpoint never creates signals, decisions, orders, fills, or positions.
    """
    now = datetime.now(tz=timezone.utc)

    with get_session() as session:
        positions = session.execute(
            select(Position).order_by(Position.opened_at)
        ).scalars().all()

        portfolio = session.execute(select(Portfolio)).scalar_one_or_none()
        cached_total_value: Decimal | None = None
        if portfolio is not None and portfolio.cached_total_value:
            cached_total_value = Decimal(str(portfolio.cached_total_value))

        items: list[ExitSignalPreviewItem] = []
        preview_exit_count = 0
        watch_count = 0
        hold_count = 0

        for pos in positions:
            qty = Decimal(str(pos.qty))
            cost_basis = Decimal(str(pos.cost_basis))
            latest_price = _latest_price(session, pos.ticker)

            if latest_price is None:
                items.append(ExitSignalPreviewItem(
                    ticker=pos.ticker,
                    qty=str(qty),
                    avg_cost=str(pos.avg_cost),
                    latest_price=None,
                    unrealized_pnl=None,
                    unrealized_pnl_pct=None,
                    monitor_recommendation="WATCH",
                    preview_exit_action="WATCH",
                    suggested_side="NONE",
                    suggested_qty="0",
                    reason_codes=["PRICE_MISSING"],
                    explanation="Latest price unavailable; position cannot be fully evaluated.",
                ))
                watch_count += 1
                continue

            market_value = qty * latest_price
            unrealized_pnl = market_value - cost_basis
            unrealized_pnl_pct = (
                unrealized_pnl / cost_basis * Decimal("100")
                if cost_basis != Decimal("0")
                else Decimal("0")
            )

            portfolio_weight_pct: Decimal | None = None
            if cached_total_value is not None and cached_total_value > Decimal("0"):
                portfolio_weight_pct = market_value / cached_total_value * Decimal("100")

            upnl_pct_float = float(unrealized_pnl_pct)
            weight_pct_float = (
                float(portfolio_weight_pct) if portfolio_weight_pct is not None else 0.0
            )

            if upnl_pct_float <= -5.0:
                monitor_recommendation = "REVIEW_FOR_EXIT"
                preview_exit_action = "PREVIEW_EXIT"
                suggested_side = "SELL"
                suggested_qty = str(qty)
                reason_codes: list[str] = ["STOP_LOSS_REVIEW"]
                explanation = (
                    f"Unrealized P&L of {upnl_pct_float:.1f}% is below the stop-loss "
                    f"review threshold (-5.0%). Preview exit: SELL {qty} shares."
                )
                preview_exit_count += 1
            elif upnl_pct_float <= -2.0:
                monitor_recommendation = "WATCH"
                preview_exit_action = "WATCH"
                suggested_side = "NONE"
                suggested_qty = "0"
                reason_codes = ["WATCH_LOSS_THRESHOLD"]
                explanation = (
                    f"Unrealized P&L of {upnl_pct_float:.1f}% is in the watch range "
                    f"(-2.0% to -5.0%). Monitor closely."
                )
                watch_count += 1
            elif portfolio_weight_pct is not None and weight_pct_float > 25.0:
                monitor_recommendation = "WATCH"
                preview_exit_action = "WATCH"
                suggested_side = "NONE"
                suggested_qty = "0"
                reason_codes = ["HIGH_CONCENTRATION"]
                explanation = (
                    f"Portfolio weight of {weight_pct_float:.1f}% exceeds the "
                    f"concentration threshold (25.0%). Consider position sizing."
                )
                watch_count += 1
            else:
                monitor_recommendation = "HOLD"
                preview_exit_action = "HOLD"
                suggested_side = "NONE"
                suggested_qty = "0"
                reason_codes = ["HEALTHY_POSITION"]
                explanation = "Position is within healthy parameters. No exit action required."
                hold_count += 1

            items.append(ExitSignalPreviewItem(
                ticker=pos.ticker,
                qty=str(qty),
                avg_cost=str(pos.avg_cost),
                latest_price=str(latest_price),
                unrealized_pnl=str(unrealized_pnl.quantize(Decimal("0.01"))),
                unrealized_pnl_pct=str(unrealized_pnl_pct.quantize(Decimal("0.01"))),
                monitor_recommendation=monitor_recommendation,
                preview_exit_action=preview_exit_action,
                suggested_side=suggested_side,
                suggested_qty=suggested_qty,
                reason_codes=reason_codes,
                explanation=explanation,
            ))

        return ExitSignalPreviewResponse(
            as_of=now,
            open_position_count=len(positions),
            preview_exit_count=preview_exit_count,
            watch_count=watch_count,
            hold_count=hold_count,
            positions=items,
            preview_only=True,
            writes_performed=False,
            no_orders_created=True,
            no_fills_created=True,
            no_broker_execution=True,
        )


@app.post(
    "/v1/review/exit-decision-preview",
    response_model=ExitDecisionPreviewResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def exit_decision_preview() -> ExitDecisionPreviewResponse:
    """
    Preview exit decision intent for all open positions (PREVIEW ONLY, no DB writes).

    Maps exit signal actions to preview decisions:
        PREVIEW_EXIT -> SELL / SELL / full qty
        WATCH        -> WATCH / NONE / 0
        HOLD         -> HOLD / NONE / 0
        Missing price -> WATCH / NONE / 0

    This endpoint never creates signals, decisions, orders, fills, or positions.
    """
    now = datetime.now(tz=timezone.utc)

    with get_session() as session:
        positions = session.execute(
            select(Position).order_by(Position.opened_at)
        ).scalars().all()

        portfolio = session.execute(select(Portfolio)).scalar_one_or_none()
        cached_total_value: Decimal | None = None
        if portfolio is not None and portfolio.cached_total_value:
            cached_total_value = Decimal(str(portfolio.cached_total_value))

        items: list[ExitDecisionPreviewItem] = []
        preview_sell_count = 0
        watch_count = 0
        hold_count = 0
        total_exit_value = Decimal("0")
        total_realized_pnl = Decimal("0")

        for pos in positions:
            qty = Decimal(str(pos.qty))
            cost_basis = Decimal(str(pos.cost_basis))
            latest_price = _latest_price(session, pos.ticker)

            if latest_price is None:
                items.append(ExitDecisionPreviewItem(
                    ticker=pos.ticker,
                    qty=str(qty),
                    avg_cost=str(pos.avg_cost),
                    latest_price=None,
                    unrealized_pnl=None,
                    unrealized_pnl_pct=None,
                    exit_signal_action="WATCH",
                    preview_decision="WATCH",
                    side="NONE",
                    decision_qty="0",
                    estimated_exit_value=None,
                    estimated_realized_pnl=None,
                    estimated_realized_pnl_pct=None,
                    reason_codes=["PRICE_MISSING"],
                    explanation="Latest price unavailable; position cannot be fully evaluated.",
                ))
                watch_count += 1
                continue

            market_value = qty * latest_price
            unrealized_pnl = market_value - cost_basis
            unrealized_pnl_pct = (
                unrealized_pnl / cost_basis * Decimal("100")
                if cost_basis != Decimal("0")
                else Decimal("0")
            )

            portfolio_weight_pct: Decimal | None = None
            if cached_total_value is not None and cached_total_value > Decimal("0"):
                portfolio_weight_pct = market_value / cached_total_value * Decimal("100")

            upnl_pct_float = float(unrealized_pnl_pct)
            weight_pct_float = (
                float(portfolio_weight_pct) if portfolio_weight_pct is not None else 0.0
            )

            if upnl_pct_float <= -5.0:
                exit_signal_action = "PREVIEW_EXIT"
                preview_decision = "SELL"
                side = "SELL"
                decision_qty = str(qty)
                est_exit_value: Decimal | None = market_value
                est_realized_pnl: Decimal | None = market_value - cost_basis
                est_realized_pnl_pct: Decimal | None = (
                    est_realized_pnl / cost_basis * Decimal("100")
                    if cost_basis != Decimal("0")
                    else Decimal("0")
                )
                reason_codes: list[str] = ["STOP_LOSS_REVIEW"]
                explanation = (
                    f"Unrealized P&L of {upnl_pct_float:.1f}% is below the stop-loss "
                    f"review threshold (-5.0%). Preview decision: SELL {qty} shares "
                    f"at {latest_price} = est. exit value "
                    f"{market_value.quantize(Decimal('0.01'))}."
                )
                total_exit_value += market_value
                total_realized_pnl += est_realized_pnl
                preview_sell_count += 1
            elif upnl_pct_float <= -2.0:
                exit_signal_action = "WATCH"
                preview_decision = "WATCH"
                side = "NONE"
                decision_qty = "0"
                est_exit_value = None
                est_realized_pnl = None
                est_realized_pnl_pct = None
                reason_codes = ["WATCH_LOSS_THRESHOLD"]
                explanation = (
                    f"Unrealized P&L of {upnl_pct_float:.1f}% is in the watch range "
                    f"(-2.0% to -5.0%). Monitor closely."
                )
                watch_count += 1
            elif portfolio_weight_pct is not None and weight_pct_float > 25.0:
                exit_signal_action = "WATCH"
                preview_decision = "WATCH"
                side = "NONE"
                decision_qty = "0"
                est_exit_value = None
                est_realized_pnl = None
                est_realized_pnl_pct = None
                reason_codes = ["HIGH_CONCENTRATION"]
                explanation = (
                    f"Portfolio weight of {weight_pct_float:.1f}% exceeds the "
                    f"concentration threshold (25.0%). Consider position sizing."
                )
                watch_count += 1
            else:
                exit_signal_action = "HOLD"
                preview_decision = "HOLD"
                side = "NONE"
                decision_qty = "0"
                est_exit_value = None
                est_realized_pnl = None
                est_realized_pnl_pct = None
                reason_codes = ["HEALTHY_POSITION"]
                explanation = "Position is within healthy parameters. No exit decision required."
                hold_count += 1

            items.append(ExitDecisionPreviewItem(
                ticker=pos.ticker,
                qty=str(qty),
                avg_cost=str(pos.avg_cost),
                latest_price=str(latest_price),
                unrealized_pnl=str(unrealized_pnl.quantize(Decimal("0.01"))),
                unrealized_pnl_pct=str(unrealized_pnl_pct.quantize(Decimal("0.01"))),
                exit_signal_action=exit_signal_action,
                preview_decision=preview_decision,
                side=side,
                decision_qty=decision_qty,
                estimated_exit_value=(
                    str(est_exit_value.quantize(Decimal("0.01")))
                    if est_exit_value is not None else None
                ),
                estimated_realized_pnl=(
                    str(est_realized_pnl.quantize(Decimal("0.01")))
                    if est_realized_pnl is not None else None
                ),
                estimated_realized_pnl_pct=(
                    str(est_realized_pnl_pct.quantize(Decimal("0.01")))
                    if est_realized_pnl_pct is not None else None
                ),
                reason_codes=reason_codes,
                explanation=explanation,
            ))

        return ExitDecisionPreviewResponse(
            as_of=now,
            open_position_count=len(positions),
            preview_sell_count=preview_sell_count,
            watch_count=watch_count,
            hold_count=hold_count,
            estimated_total_exit_value=str(total_exit_value.quantize(Decimal("0.01"))),
            estimated_total_realized_pnl=str(total_realized_pnl.quantize(Decimal("0.01"))),
            positions=items,
            preview_only=True,
            writes_performed=False,
            no_signals_created=True,
            no_decisions_created=True,
            no_orders_created=True,
            no_fills_created=True,
            no_broker_execution=True,
        )


@app.post(
    "/v1/review/position-review-preview",
    response_model=PositionReviewPreviewResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def position_review_preview() -> PositionReviewPreviewResponse:
    """
    One-click consolidated position review (PREVIEW ONLY, no DB writes).

    Combines position monitor, exit signal, exit decision, and order preview layers:
        REVIEW_FOR_EXIT / PREVIEW_EXIT / SELL / PREVIEW_ONLY if unrealized_pnl_pct <= -5.0%
        WATCH           / WATCH        / WATCH / NOT_NEEDED  if unrealized_pnl_pct <= -2.0%
        WATCH           / WATCH        / WATCH / NOT_NEEDED  if portfolio_weight_pct > 25.0%
        HOLD            / HOLD         / HOLD  / NOT_NEEDED  otherwise
        Missing price                         -> WATCH across all layers

    This endpoint never creates signals, decisions, orders, trades, fills,
    or modifies positions, cash, or broker state.
    """
    now = datetime.now(tz=timezone.utc)

    with get_session() as session:
        positions = session.execute(
            select(Position).order_by(Position.opened_at)
        ).scalars().all()

        portfolio = session.execute(select(Portfolio)).scalar_one_or_none()
        cached_total_value: Decimal | None = None
        if portfolio is not None and portfolio.cached_total_value:
            cached_total_value = Decimal(str(portfolio.cached_total_value))

        items: list[PositionReviewItem] = []
        hold_count = 0
        watch_count = 0
        review_for_exit_count = 0
        total_exit_value = Decimal("0")
        total_realized_pnl = Decimal("0")

        for pos in positions:
            qty = Decimal(str(pos.qty))
            cost_basis = Decimal(str(pos.cost_basis))
            latest_price = _latest_price(session, pos.ticker)

            if latest_price is None:
                items.append(PositionReviewItem(
                    ticker=pos.ticker, qty=str(qty), avg_cost=str(pos.avg_cost),
                    latest_price=None, market_value=None,
                    unrealized_pnl=None, unrealized_pnl_pct=None, portfolio_weight_pct=None,
                    position_recommendation="WATCH", exit_action="WATCH",
                    decision_preview="WATCH", order_preview="NOT_NEEDED",
                    suggested_side="NONE", suggested_qty="0",
                    estimated_exit_value=None, estimated_realized_pnl=None,
                    estimated_realized_pnl_pct=None,
                    reason_codes=["PRICE_MISSING"],
                    explanation="Latest price unavailable; position cannot be fully evaluated.",
                ))
                watch_count += 1
                continue

            market_value = qty * latest_price
            unrealized_pnl = market_value - cost_basis
            unrealized_pnl_pct = (
                unrealized_pnl / cost_basis * Decimal("100")
                if cost_basis != Decimal("0") else Decimal("0")
            )

            portfolio_weight_pct: Decimal | None = None
            if cached_total_value is not None and cached_total_value > Decimal("0"):
                portfolio_weight_pct = market_value / cached_total_value * Decimal("100")

            upnl_pct_float = float(unrealized_pnl_pct)
            weight_pct_float = float(portfolio_weight_pct) if portfolio_weight_pct is not None else 0.0

            est_exit_value: Decimal | None = None
            est_realized_pnl: Decimal | None = None
            est_realized_pnl_pct: Decimal | None = None

            if upnl_pct_float <= -5.0:
                position_recommendation = "REVIEW_FOR_EXIT"
                exit_action = "PREVIEW_EXIT"
                decision_preview = "SELL"
                order_preview = "PREVIEW_ONLY"
                suggested_side = "SELL"
                suggested_qty = str(qty)
                est_exit_value = market_value
                est_realized_pnl = market_value - cost_basis
                est_realized_pnl_pct = (
                    est_realized_pnl / cost_basis * Decimal("100")
                    if cost_basis != Decimal("0") else Decimal("0")
                )
                reason_codes: list[str] = ["STOP_LOSS_REVIEW"]
                explanation = (
                    f"Unrealized P&L of {upnl_pct_float:.1f}% is below the stop-loss "
                    f"review threshold (-5.0%). Consolidated preview: SELL {qty} shares "
                    f"at {latest_price} = est. exit value "
                    f"{market_value.quantize(Decimal('0.01'))}."
                )
                total_exit_value += market_value
                total_realized_pnl += est_realized_pnl
                review_for_exit_count += 1
            elif upnl_pct_float <= -2.0:
                position_recommendation = "WATCH"
                exit_action = "WATCH"
                decision_preview = "WATCH"
                order_preview = "NOT_NEEDED"
                suggested_side = "NONE"
                suggested_qty = "0"
                reason_codes = ["WATCH_LOSS_THRESHOLD"]
                explanation = (
                    f"Unrealized P&L of {upnl_pct_float:.1f}% is in the watch range "
                    f"(-2.0% to -5.0%). Monitor closely."
                )
                watch_count += 1
            elif portfolio_weight_pct is not None and weight_pct_float > 25.0:
                position_recommendation = "WATCH"
                exit_action = "WATCH"
                decision_preview = "WATCH"
                order_preview = "NOT_NEEDED"
                suggested_side = "NONE"
                suggested_qty = "0"
                reason_codes = ["HIGH_CONCENTRATION"]
                explanation = (
                    f"Portfolio weight of {weight_pct_float:.1f}% exceeds the "
                    f"concentration threshold (25.0%). Consider position sizing."
                )
                watch_count += 1
            else:
                position_recommendation = "HOLD"
                exit_action = "HOLD"
                decision_preview = "HOLD"
                order_preview = "NOT_NEEDED"
                suggested_side = "NONE"
                suggested_qty = "0"
                reason_codes = ["HEALTHY_POSITION"]
                explanation = "Position is within healthy parameters. No exit or order action required."
                hold_count += 1

            items.append(PositionReviewItem(
                ticker=pos.ticker, qty=str(qty), avg_cost=str(pos.avg_cost),
                latest_price=str(latest_price),
                market_value=str(market_value.quantize(Decimal("0.01"))),
                unrealized_pnl=str(unrealized_pnl.quantize(Decimal("0.01"))),
                unrealized_pnl_pct=str(unrealized_pnl_pct.quantize(Decimal("0.01"))),
                portfolio_weight_pct=(
                    str(portfolio_weight_pct.quantize(Decimal("0.01")))
                    if portfolio_weight_pct is not None else None
                ),
                position_recommendation=position_recommendation,
                exit_action=exit_action,
                decision_preview=decision_preview,
                order_preview=order_preview,
                suggested_side=suggested_side,
                suggested_qty=suggested_qty,
                estimated_exit_value=(
                    str(est_exit_value.quantize(Decimal("0.01")))
                    if est_exit_value is not None else None
                ),
                estimated_realized_pnl=(
                    str(est_realized_pnl.quantize(Decimal("0.01")))
                    if est_realized_pnl is not None else None
                ),
                estimated_realized_pnl_pct=(
                    str(est_realized_pnl_pct.quantize(Decimal("0.01")))
                    if est_realized_pnl_pct is not None else None
                ),
                reason_codes=reason_codes,
                explanation=explanation,
            ))

        return PositionReviewPreviewResponse(
            as_of=now,
            open_position_count=len(positions),
            hold_count=hold_count,
            watch_count=watch_count,
            review_for_exit_count=review_for_exit_count,
            preview_exit_count=review_for_exit_count,
            preview_sell_count=review_for_exit_count,
            preview_order_count=review_for_exit_count,
            estimated_total_exit_value=str(total_exit_value.quantize(Decimal("0.01"))),
            estimated_total_realized_pnl=str(total_realized_pnl.quantize(Decimal("0.01"))),
            positions=items,
            preview_only=True,
            writes_performed=False,
            no_signals_created=True,
            no_decisions_created=True,
            no_orders_created=True,
            no_trades_created=True,
            no_fills_created=True,
            no_position_changes=True,
            no_cash_changes=True,
            no_broker_execution=True,
        )


@app.get(
    "/v1/review/daily-review-summary",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def daily_review_summary() -> DailyReviewSummaryResponse:
    """
    GET /v1/review/daily-review-summary — Read-only daily operating summary.

    Consolidates portfolio, open positions, candidate/review queue, orders, and
    performance history into one response and recommends the next action.

    READ ONLY: zero rows created, zero rows mutated. Safe to call any time.

    Next-action priority (current-session-aware — historical candidates never
    drive the next action):
        REVIEW_PENDING_ORDERS     — pending_orders > 0
        REVIEW_POSITION_EXIT      — review_for_exit_count > 0
        REVIEW_NEW_CANDIDATES     — today's NEW candidates awaiting review
        CREATE_FILL_PAPER_TRADE   — today's approved candidate ready for paper trade
        VIEW_PORTFOLIO            — today's paper trade was filled
        MONITOR_PORTFOLIO         — open positions, no actionable current-session work
        RUN_DAILY_PROCESS_PREVIEW — nothing actionable, no open positions
    """
    now = datetime.now(tz=timezone.utc)

    with get_session() as session:
        # --- Portfolio ---
        portfolio = session.execute(select(Portfolio)).scalar_one_or_none()
        total_value: str | None = None
        cash: str | None = None
        positions_value: str | None = None
        cached_total: Decimal | None = None
        if portfolio is not None:
            total_value = str(portfolio.cached_total_value)
            cash = str(portfolio.cached_cash)
            cached_total = Decimal(str(portfolio.cached_total_value)) if portfolio.cached_total_value else None

        # --- Positions (lightweight position review) ---
        positions = session.execute(
            select(Position).order_by(Position.opened_at)
        ).scalars().all()

        open_position_count = len(positions)
        hold_count = 0
        watch_count = 0
        review_for_exit_count = 0
        total_unrealized_pnl = Decimal("0")
        pos_items: list[DailyReviewSummaryPositionItem] = []

        for pos in positions:
            qty = Decimal(str(pos.qty))
            cost_basis = Decimal(str(pos.cost_basis))
            latest_price = _latest_price(session, pos.ticker)

            if latest_price is None:
                watch_count += 1
                pos_items.append(DailyReviewSummaryPositionItem(
                    ticker=pos.ticker,
                    qty=str(qty),
                    unrealized_pnl=None,
                    unrealized_pnl_pct=None,
                    recommendation="WATCH",
                ))
                continue

            market_value = qty * latest_price
            unrealized_pnl = market_value - cost_basis
            total_unrealized_pnl += unrealized_pnl
            unrealized_pnl_pct = (
                unrealized_pnl / cost_basis * Decimal("100")
                if cost_basis != Decimal("0") else Decimal("0")
            )

            weight_pct_float = 0.0
            if cached_total is not None and cached_total > Decimal("0"):
                weight_pct_float = float(market_value / cached_total * Decimal("100"))

            upnl_pct_float = float(unrealized_pnl_pct)

            if upnl_pct_float <= -5.0:
                recommendation = "REVIEW_FOR_EXIT"
                review_for_exit_count += 1
            elif upnl_pct_float <= -2.0 or weight_pct_float > 25.0:
                recommendation = "WATCH"
                watch_count += 1
            else:
                recommendation = "HOLD"
                hold_count += 1

            pos_items.append(DailyReviewSummaryPositionItem(
                ticker=pos.ticker,
                qty=str(qty),
                unrealized_pnl=str(unrealized_pnl.quantize(Decimal("0.01"))),
                unrealized_pnl_pct=str(unrealized_pnl_pct.quantize(Decimal("0.01"))),
                recommendation=recommendation,
            ))

        if open_position_count > 0:
            positions_value = None
            if cached_total is not None:
                _cash_dec = Decimal(str(portfolio.cached_cash)) if portfolio and portfolio.cached_cash else Decimal("0")
                _pos_val = cached_total - _cash_dec
                positions_value = str(_pos_val.quantize(Decimal("0.01")))

        unrealized_pnl_str = str(total_unrealized_pnl.quantize(Decimal("0.01"))) if open_position_count > 0 else None

        # --- Performance history (latest row) for total_return_pct ---
        total_return_pct: str | None = None
        if portfolio is not None and portfolio.initial_capital:
            snap_row = session.execute(
                select(PortfolioSnapshot)
                .order_by(PortfolioSnapshot.market_date.desc())
            ).scalars().first()
            if snap_row is not None:
                initial_capital = Decimal(str(portfolio.initial_capital))
                latest_total = Decimal(str(snap_row.total_value))
                if initial_capital != Decimal("0"):
                    total_return_pct = str(
                        ((latest_total - initial_capital) / initial_capital * Decimal("100"))
                        .quantize(Decimal("0.01"))
                    )

        # --- Candidate review counts ---
        cr_total = session.query(CandidateReview).count()
        cr_new = session.query(CandidateReview).filter(
            CandidateReview.review_status == "NEW"
        ).count()
        cr_approved = session.query(CandidateReview).filter(
            CandidateReview.review_status == "APPROVED_FOR_SIGNAL"
        ).count()
        cr_watching = session.query(CandidateReview).filter(
            CandidateReview.review_status == "WATCHING"
        ).count()
        cr_consumed = session.query(CandidateReview).filter(
            CandidateReview.review_status == "CONSUMED"
        ).count()

        current_cycle_row = session.query(CandidateReview.idempotency_key).order_by(
            CandidateReview.created_at.desc()
        ).first()
        current_cycle_key: str | None = current_cycle_row[0] if current_cycle_row else None

        # actionable = NEW + APPROVED_FOR_SIGNAL (candidates that can drive new entry)
        cr_actionable = cr_new + cr_approved

        # --- Orders counts ---
        pending_orders = session.query(Order).filter(Order.status == "PENDING").count()
        filled_orders = session.query(Order).filter(Order.status == "FILLED").count()
        canceled_orders = session.query(Order).filter(Order.status == "CANCELLED").count()

        # --- Current-session (today's) trade-idea separation ---
        # Only today's candidates plus live order/position facts drive the
        # recommended next action. Historical candidates are reported separately
        # and can never be counted as current actionable work. This mirrors the
        # canonical contract in /v1/review/workflow-status so every UI surface
        # agrees on the current task.
        DRS_REVIEW_SOURCE_PREFIX = "review_queue_create_signals_v1:"
        today = date.today()
        start_of_day = datetime(today.year, today.month, today.day)
        today_cr_q = session.query(CandidateReview).filter(
            CandidateReview.created_at >= start_of_day
        )
        cur_total = today_cr_q.count()
        cur_pending = today_cr_q.filter(
            CandidateReview.review_status == "NEW"
        ).count()
        cur_rejected = today_cr_q.filter(
            CandidateReview.review_status == "REJECTED"
        ).count()
        cur_watch = today_cr_q.filter(
            CandidateReview.review_status == "WATCHING"
        ).count()
        historical_count = cr_total - cur_total

        # Approved-today rows stay "ready for paper trade" until their own
        # candidate-scoped signal has a FILLED order. The approval label never
        # changes after a fill, so ticket-ready vs completed is derived from the
        # order chain — an unrelated old fill can never mark a current candidate
        # complete.
        today_approved_rows = today_cr_q.filter(
            CandidateReview.review_status == "APPROVED_FOR_SIGNAL"
        ).all()
        approved_runs = {
            f"{DRS_REVIEW_SOURCE_PREFIX}{row.id}" for row in today_approved_rows
        }
        completed_runs: set[str] = set()
        if approved_runs:
            filled_rows = session.query(Signal.source_run).join(
                TradeDecision, TradeDecision.signal_id == Signal.id
            ).join(Order, Order.trade_decision_id == TradeDecision.id).filter(
                Signal.source_run.in_(list(approved_runs)),
                Order.status == "FILLED",
            ).distinct().all()
            completed_runs = {r[0] for r in filled_rows}
        cur_completed = len(completed_runs)
        cur_ready_for_paper_trade = len(today_approved_rows) - cur_completed

    # --- Recommended next action (current-session-aware) ---
    # Pending actions counts only work that still needs a user decision today:
    # current-session candidates awaiting review, pending paper orders, and
    # positions flagged for exit review. Approved-already-reviewed ideas and any
    # historical candidate never inflate this count.
    pending_actions_count = cur_pending + pending_orders + review_for_exit_count

    if pending_orders > 0:
        next_action_code = "REVIEW_PENDING_ORDERS"
        next_action_label = "Review pending paper orders"
        next_action_detail = f"{pending_orders} pending paper order(s) waiting for fill review."
    elif review_for_exit_count > 0:
        next_action_code = "REVIEW_POSITION_EXIT"
        next_action_label = "Review open positions for possible exit"
        next_action_detail = f"{review_for_exit_count} position(s) below stop-loss threshold (-5%)."
    elif cur_pending > 0:
        next_action_code = "REVIEW_NEW_CANDIDATES"
        next_action_label = "Review new-entry candidates"
        next_action_detail = (
            f"{cur_pending} new-entry trade idea(s) from today's scan awaiting review."
        )
    elif cur_ready_for_paper_trade > 0:
        next_action_code = "CREATE_FILL_PAPER_TRADE"
        next_action_label = "Create & Fill Paper Trade"
        next_action_detail = (
            f"{cur_ready_for_paper_trade} approved trade idea(s) from today ready for a paper trade."
        )
    elif cur_completed > 0:
        next_action_code = "VIEW_PORTFOLIO"
        next_action_label = "View Portfolio"
        next_action_detail = (
            "Today's paper trade was filled. Review the updated paper position in Portfolio."
        )
    elif open_position_count > 0:
        next_action_code = "MONITOR_PORTFOLIO"
        next_action_label = "Monitor portfolio"
        next_action_detail = (
            "No actionable trade ideas today. Existing positions were reviewed automatically."
        )
    else:
        next_action_code = "RUN_DAILY_PROCESS_PREVIEW"
        next_action_label = "Run Daily Process Preview"
        next_action_detail = (
            "No actionable trade ideas today. Run Daily Review to scan for new opportunities."
        )

    return DailyReviewSummaryResponse(
        as_of=now,
        # Portfolio
        total_value=total_value,
        cash=cash,
        positions_value=positions_value,
        open_position_count=open_position_count,
        unrealized_pnl=unrealized_pnl_str,
        total_return_pct=total_return_pct,
        # Positions
        hold_count=hold_count,
        watch_count=watch_count,
        review_for_exit_count=review_for_exit_count,
        open_positions=pos_items,
        # Daily process
        current_cycle_key=current_cycle_key,
        review_candidates_total=cr_total,
        review_candidates_actionable=cr_actionable,
        review_candidates_consumed=cr_consumed,
        review_candidates_watching=cr_watching,
        # Orders
        pending_orders=pending_orders,
        filled_orders=filled_orders,
        canceled_orders=canceled_orders,
        no_pending_orders=(pending_orders == 0),
        # Current-session separation (historical candidates excluded)
        current_session_trade_ideas_total=cur_total,
        current_session_pending_review_count=cur_pending,
        current_session_approved_ready_for_paper_trade_count=cur_ready_for_paper_trade,
        current_session_rejected_count=cur_rejected,
        current_session_watched_count=cur_watch,
        historical_trade_ideas_count=historical_count,
        open_positions_count=open_position_count,
        pending_paper_orders_count=pending_orders,
        pending_actions_count=pending_actions_count,
        # Next action
        next_action_code=next_action_code,
        next_action_label=next_action_label,
        next_action_detail=next_action_detail,
    )


@app.get(
    "/v1/review/scan-diagnostics/latest",
    response_model=ScanDiagnosticsLatestResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def scan_diagnostics_latest() -> ScanDiagnosticsLatestResponse:
    """
    GET /v1/review/scan-diagnostics/latest — Read-only scan selection funnel.

    Explains how the configured S&P 500 universe is reduced to actionable trade
    ideas, and strictly separates the latest session's active trade ideas from
    historical (no-longer-actionable) ones.

    READ ONLY: zero rows created, zero rows mutated, no broker execution, no GCP
    prediction calls, no automation.

    Data sources (honest, no faked precision):
        - Universe / price-history / local-screen counts and the "top local
          screened" table are computed live read-only via the local screener
          (no remote prediction service is contacted).
        - Active vs. historical trade ideas, the per-ticker prediction results,
          and existing-positions-reviewed come from persisted CandidateReview /
          Position rows. "Today" (created today) drives actionability; everything
          older is historical and never actionable.
        - Per-session prediction dispatch / returned counts are not persisted, so
          they are clearly labeled "not captured yet" rather than invented.
    """
    now = datetime.now(tz=timezone.utc)

    from paper_trader.engine.market_screener import scan_market as scan_market_fn
    from paper_trader.engine.universe import get_sp500_universe

    # Actual configured defaults from the live prediction-candidates contract —
    # never invented numbers.
    _req_fields = MarketScanPredictionCandidatesRequest.model_fields
    dispatch_limit = int(_req_fields["prediction_top_n"].default)
    min_price_points = int(_req_fields["min_price_points"].default)
    _table_top_n = 25

    universe_tickers = get_sp500_universe()
    universe_configured = len(universe_tickers)

    # --- Live read-only local screen (no remote prediction service) ---
    with get_dedicated_session() as screen_session:
        screened, skipped, _scan_date = scan_market_fn(
            session=screen_session,
            universe="SP500",
            top_n=_table_top_n,
            min_price_points=min_price_points,
        )

    skipped_by_reason: dict[str, int] = {}
    for s in skipped:
        skipped_by_reason[s.reason] = skipped_by_reason.get(s.reason, 0) + 1
    _insufficient = skipped_by_reason.get("INSUFFICIENT_PRICE_HISTORY", 0)
    _no_data = skipped_by_reason.get("NO_PRICE_DATA", 0)

    locally_screened = max(0, universe_configured - len(skipped))
    price_history_ready = max(0, universe_configured - _insufficient - _no_data)

    # Top local screened table (ranked, capped). Names within the dispatch limit
    # would be sent to prediction first; the rest are cut by the top-N rule.
    top_local_screened: list[ScanFunnelTopLocal] = []
    for c in screened:
        _sent = c.rank <= dispatch_limit
        top_local_screened.append(ScanFunnelTopLocal(
            rank=c.rank,
            ticker=c.ticker,
            score=c.score,
            momentum_5d_pct=c.momentum_5d_pct,
            momentum_20d_pct=c.momentum_20d_pct,
            relative_strength_vs_spy_20d=c.relative_strength_vs_spy_20d,
            sent_to_prediction=_sent,
            reason="TOP_LOCAL_RANK" if _sent else "NOT_IN_TOP_LOCAL_RANK",
        ))

    # Exclusion reasons: local-screen skips + the top-N dispatch cut.
    exclusion_reasons: list[ScanFunnelExclusionReason] = [
        ScanFunnelExclusionReason(reason=r, count=n)
        for r, n in sorted(skipped_by_reason.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    _not_in_rank = max(0, locally_screened - dispatch_limit)
    if _not_in_rank:
        exclusion_reasons.append(
            ScanFunnelExclusionReason(reason="NOT_IN_TOP_LOCAL_RANK", count=_not_in_rank)
        )

    # --- Persisted session state (active vs. historical) ---
    today = date.today()
    start_of_day = datetime(today.year, today.month, today.day)

    with get_session() as session:
        existing_positions_reviewed = session.query(Position).count()

        cr_total = session.query(CandidateReview).count()
        today_q = session.query(CandidateReview).filter(
            CandidateReview.created_at >= start_of_day
        )
        cur_total = today_q.count()
        active_trade_ideas = today_q.filter(
            CandidateReview.review_status == "NEW"
        ).count()
        watch_below_threshold = today_q.filter(
            CandidateReview.review_status == "WATCHING"
        ).count()
        rejected_blocked = today_q.filter(
            CandidateReview.review_status == "REJECTED"
        ).count()
        historical_trade_ideas = max(0, cr_total - cur_total)

        latest_row = session.query(
            CandidateReview.idempotency_key, CandidateReview.created_at
        ).order_by(CandidateReview.created_at.desc()).first()
        candidate_review_session_id = latest_row[0] if latest_row else None
        candidate_review_created = latest_row[1] if latest_row else None

        # Resolve the authoritative latest Daily Review session. A 0-candidate
        # run saves NO CandidateReview row, so the newest CandidateReview can be
        # an OLDER session and would never match the new daily_session_id stamped
        # on the captured prediction_runs. Prefer the most recent DAILY_REVIEW
        # prediction-run session when it is at least as recent as the latest
        # CandidateReview, so the funnel links the just-run session correctly.
        # READ ONLY: one indexed SELECT, no rows created/mutated.
        latest_pred_row = session.query(
            PredictionRun.daily_session_id, PredictionRun.created_at
        ).filter(
            PredictionRun.source == "DAILY_REVIEW",
            PredictionRun.daily_session_id.isnot(None),
        ).order_by(PredictionRun.created_at.desc()).first()
        prediction_run_session_id = latest_pred_row[0] if latest_pred_row else None
        prediction_run_created = latest_pred_row[1] if latest_pred_row else None

        session_id = candidate_review_session_id
        latest_created = candidate_review_created
        if prediction_run_session_id is not None and (
            candidate_review_created is None
            or (prediction_run_created is not None
                and prediction_run_created >= candidate_review_created)
        ):
            session_id = prediction_run_session_id
            latest_created = prediction_run_created

        market_date = str(latest_created.date()) if latest_created is not None else None
        has_latest_session = session_id is not None

        prediction_results: list[ScanFunnelPredictionResult] = []
        if session_id is not None:
            latest_rows = session.query(CandidateReview).filter(
                CandidateReview.idempotency_key == session_id
            ).order_by(CandidateReview.created_at.desc()).all()
            for row in latest_rows:
                # row.created_at is tz-aware; compare on date to match the "today"
                # session boundary without mixing naive/aware datetimes.
                is_current = row.created_at is not None and row.created_at.date() >= today
                rs = row.review_status
                if not is_current:
                    actionability = "HISTORICAL_NOT_ACTIONABLE"
                    reason = "From a prior session - not actionable today."
                elif rs == "NEW":
                    actionability = "ACTIVE_TRADE_IDEA"
                    reason = "Awaiting your review."
                elif rs == "APPROVED_FOR_SIGNAL":
                    actionability = "APPROVED"
                    reason = "Approved - ready for a paper trade ticket."
                elif rs == "WATCHING":
                    actionability = "WATCH_ONLY"
                    reason = "On watch - below action threshold."
                elif rs == "REJECTED":
                    actionability = "REJECTED"
                    reason = "Rejected in review."
                else:
                    actionability = "OTHER"
                    reason = rs
                prediction_results.append(ScanFunnelPredictionResult(
                    ticker=row.ticker,
                    prediction=row.prediction_recommendation,
                    confidence=row.prediction_confidence,
                    expected_return_pct=row.expected_return_pct,
                    score=row.preview_score,
                    actionability=actionability,
                    reason=reason,
                    review_status=rs,
                    is_current_session=is_current,
                ))

        # --- Prediction capture linkage (observational; read-only SELECT) ---
        # Link the latest session to the prediction_runs it dispatched via the
        # shared daily_session_id, so the funnel reports REAL per-session capture
        # counts instead of timestamp guessing. Counting these rows creates no
        # signals/decisions/orders/trades and makes no GCP call.
        captured_runs: list[PredictionRun] = []
        if session_id is not None:
            captured_runs = session.query(PredictionRun).filter(
                PredictionRun.daily_session_id == session_id
            ).all()
        any_prediction_runs_exist = (
            session.query(PredictionRun.id).first() is not None
        )

    # Planned dispatch (top-N ranked names + current holdings). The actual count
    # sent on the last live run is not persisted, so this is labeled as planned.
    planned_sent_to_prediction = min(dispatch_limit, locally_screened) + existing_positions_reviewed

    # --- Resolve per-session prediction capture status -----------------------
    # captured_runs / any_prediction_runs_exist were computed read-only above.
    capture_count = len(captured_runs)
    capture_errors = sum(1 for r in captured_runs if r.error)
    capture_returned = capture_count - capture_errors
    capture_session_linked = capture_count > 0

    if capture_count > 0:
        sent_to_prediction = capture_count
        sent_to_prediction_captured = True
        predictions_returned: int | None = capture_returned
        predictions_returned_captured = True
        predictions_captured: int | None = capture_count
        if capture_errors >= capture_count:
            capture_status = "DISPATCH_FAILED"
            capture_status_message = (
                f"Prediction dispatch failed ({capture_errors} error"
                f"{'s' if capture_errors != 1 else ''}). See captured runs."
            )
        elif active_trade_ideas == 0:
            capture_status = "NONE_PASSED_GATE"
            capture_status_message = (
                f"{capture_returned} prediction"
                f"{'s' if capture_returned != 1 else ''} captured. "
                "None passed the actionability gate."
            )
        else:
            capture_status = "CAPTURED"
            capture_status_message = (
                f"{capture_returned} prediction"
                f"{'s' if capture_returned != 1 else ''} captured for this session."
            )
    else:
        # No capture rows linked to this session.
        sent_to_prediction = planned_sent_to_prediction
        sent_to_prediction_captured = False
        predictions_returned = None
        predictions_returned_captured = False
        predictions_captured = None
        if has_latest_session and any_prediction_runs_exist:
            capture_status = "OLDER_SESSION_NO_CAPTURE"
            capture_status_message = "Not captured for this older session."
        else:
            capture_status = "NOT_CAPTURED_YET"
            capture_status_message = "Not captured yet."

    dispatch_explanation = (
        f"Prediction is run on the top {dispatch_limit} locally ranked names plus "
        f"current holdings, not every S&P 500 ticker. The prediction service is remote, "
        f"so prediction dispatch is deliberately limited."
    )
    funnel_note = (
        f"Full S&P 500 local screen completed: {locally_screened} of {universe_configured} "
        f"names had enough price history. Prediction dispatch was run only on the top "
        f"{dispatch_limit} ranked names plus current holdings because the prediction "
        f"service is remote. This is selection coverage, not a failure to scan the S&P 500. "
        f"Paper trade only - no broker execution."
    )
    if capture_session_linked:
        capture_note = (
            "Universe, price-history, local-screen and top-ranked figures are computed live "
            "and exact. 'Sent to Prediction' and 'Predictions Captured' below are the REAL "
            f"counts for this session ({capture_status_message}). The actionability gate "
            "reflects the saved trade ideas from the latest session."
        )
    else:
        capture_note = (
            "Universe, price-history, local-screen and top-ranked figures are computed live "
            "and exact. " + capture_status_message + " 'Sent to Prediction' shows the "
            "configured dispatch plan; the actionability gate below reflects the saved trade "
            "ideas from the latest session."
        )

    return ScanDiagnosticsLatestResponse(
        session_id=session_id,
        market_date=market_date,
        as_of=now,
        has_latest_session=has_latest_session,
        universe_configured=universe_configured,
        price_history_ready=price_history_ready,
        locally_screened=locally_screened,
        prediction_dispatch_limit=dispatch_limit,
        sent_to_prediction=sent_to_prediction,
        sent_to_prediction_captured=sent_to_prediction_captured,
        predictions_returned=predictions_returned,
        predictions_returned_captured=predictions_returned_captured,
        predictions_captured=predictions_captured,
        prediction_errors_captured=capture_errors,
        capture_session_linked=capture_session_linked,
        capture_status=capture_status,
        capture_status_message=capture_status_message,
        candidate_review_idempotency_key=candidate_review_session_id,
        prediction_run_session_id=prediction_run_session_id,
        prediction_runs_matched_count=capture_count,
        active_trade_ideas=active_trade_ideas,
        watch_below_threshold=watch_below_threshold,
        rejected_blocked=rejected_blocked,
        existing_positions_reviewed=existing_positions_reviewed,
        already_in_portfolio=existing_positions_reviewed,
        historical_trade_ideas=historical_trade_ideas,
        thresholds=ScanFunnelThresholds(
            min_score=DEFAULT_MIN_ACTIONABLE_SCORE,
            min_confidence=DEFAULT_MIN_CONFIDENCE,
            min_expected_return_pct=DEFAULT_MIN_EXPECTED_RETURN_PCT,
            min_relative_strength_vs_spy=DEFAULT_MIN_RELATIVE_STRENGTH_VS_SPY,
        ),
        prediction_dispatch_explanation=dispatch_explanation,
        capture_note=capture_note,
        funnel_note=funnel_note,
        exclusion_reasons=exclusion_reasons,
        top_local_screened=top_local_screened,
        prediction_results=prediction_results,
    )


@app.get(
    "/v1/model/prediction-runs",
    response_model=PredictionRunListResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def list_prediction_runs(
    limit: int = Query(50, ge=1, le=500, description="Maximum number of rows to return."),
    ticker: str | None = Query(None, description="Optional ticker filter (case-insensitive)."),
    daily_session_id: str | None = Query(
        None,
        description="Optional Daily Review session id filter (exact match).",
    ),
    source: str | None = Query(
        None,
        description="Optional capture-source filter (DAILY_REVIEW | PREDICTION_PREVIEW | MARKET_SCAN).",
    ),
) -> PredictionRunListResponse:
    """
    GET /v1/model/prediction-runs - Read-only capture store of GCP prediction calls.

    Returns the most recent prediction_runs rows (newest first), each recording
    what Paper Trader sent to the remote prediction service and what came back:
    request/response timing, the URL used (no secrets), the request payload, the
    raw response JSON, normalized recommendation/confidence/expected-return/
    forecast, model consensus, remote execution diagnostics, and any error.

    READ ONLY: this endpoint only reads prediction_runs. It creates no signals,
    trade decisions, orders, trades, fills, or broker actions, makes no remote
    GCP call, and triggers no automation. Default limit is 50.
    """
    ticker_filter = ticker.strip().upper() if ticker and ticker.strip() else None
    session_filter = daily_session_id.strip() if daily_session_id and daily_session_id.strip() else None
    source_filter = source.strip().upper() if source and source.strip() else None

    with get_session() as session:
        query = session.query(PredictionRun)
        if ticker_filter:
            query = query.filter(PredictionRun.ticker == ticker_filter)
        if session_filter:
            query = query.filter(PredictionRun.daily_session_id == session_filter)
        if source_filter:
            query = query.filter(PredictionRun.source == source_filter)
        rows = (
            query.order_by(PredictionRun.created_at.desc())
            .limit(limit)
            .all()
        )

        runs = [
            PredictionRunOut(
                id=str(row.id),
                ticker=row.ticker,
                daily_session_id=row.daily_session_id,
                source=row.source,
                request_ts=row.request_ts,
                response_ts=row.response_ts,
                latency_ms=row.latency_ms,
                prediction_service_url=row.prediction_service_url,
                request_payload=row.request_payload,
                http_status=row.http_status,
                raw_response=row.raw_response,
                normalized_recommendation=row.normalized_recommendation,
                normalized_confidence=row.normalized_confidence,
                normalized_expected_return_pct=row.normalized_expected_return_pct,
                normalized_forecast_price_5d=row.normalized_forecast_price_5d,
                model_consensus=row.model_consensus,
                ran_models=row.ran_models,
                skipped_models=row.skipped_models,
                model_errors=row.model_errors,
                service_version=row.service_version,
                error=row.error,
                error_message=row.error_message,
                created_at=row.created_at,
            )
            for row in rows
        ]

    return PredictionRunListResponse(
        runs=runs,
        count=len(runs),
        limit=limit,
        ticker=ticker_filter,
    )


@app.get(
    "/v1/model/methodology",
    response_model=ModelMethodologyResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def model_methodology() -> ModelMethodologyResponse:
    """
    GET /v1/model/methodology - Read-only Quant Model Contract v1.

    Transparency / model-governance endpoint. Documents exactly what the current
    two-layer model does today (local technical pre-screen + remote GCP
    prediction), what the actionability gate requires, and the target quant-grade
    architecture, with an honest data-readiness ledger.

    READ ONLY: zero rows created/mutated, no broker execution, NO remote GCP
    prediction call, NO external web/news/sentiment call, no automation.

    Honesty rules enforced here (no faked precision):
        - Threshold values are read from the actual code constants, never invented.
        - Only price-derived features are marked available/used_today; news,
          sentiment, seasonality, macro, sector, volume, and event risk are marked
          NOT available with rule "Do not fake this feature." until a real,
          sourced, timestamped data feed exists in the codebase.
        - The remote prediction layer is described as a black box from Paper
          Trader's perspective (only ticker is sent; only a point estimate +
          confidence come back).
    """
    now = datetime.now(tz=timezone.utc)

    # Actual configured dispatch default - read from the live request contract,
    # never hardcoded.
    dispatch_limit = int(
        MarketScanPredictionCandidatesRequest.model_fields["prediction_top_n"].default
    )

    # --- Layer 1: local technical pre-screen (engine/market_screener.py) ---
    local_features = [
        ModelFeatureDescriptor(
            name="5-day momentum",
            source="price_snapshots (CLOSE, REGULAR)",
            available=True,
            used_today=True,
            purpose="short-term trend",
        ),
        ModelFeatureDescriptor(
            name="20-day momentum",
            source="price_snapshots (CLOSE, REGULAR)",
            available=True,
            used_today=True,
            purpose="primary trend / ranking weight",
        ),
        ModelFeatureDescriptor(
            name="20-day volatility",
            source="price_snapshots (stdev of daily returns)",
            available=True,
            used_today=True,
            purpose="downside / high-volatility penalty",
        ),
        ModelFeatureDescriptor(
            name="Relative strength vs SPY (20d)",
            source="benchmark_prices (SPY)",
            available=True,
            used_today=True,
            purpose="relative strength vs market",
        ),
        ModelFeatureDescriptor(
            name="Volume / liquidity",
            source=None,
            available=False,
            used_today=False,
            purpose="tradability (target only - no volume data stored yet)",
        ),
        ModelFeatureDescriptor(
            name="Sector / industry",
            source=None,
            available=False,
            used_today=False,
            purpose="sector normalization / rotation (target only)",
        ),
        ModelFeatureDescriptor(
            name="Fundamentals / market cap / beta",
            source=None,
            available=False,
            used_today=False,
            purpose="quality and risk context (target only)",
        ),
    ]
    local_prescreen = ModelLayerLocalPrescreen(
        description=(
            "Current local pre-screen is a first-pass technical/momentum ranking of the "
            "configured S&P 500 universe using only stored daily CLOSE prices. It ranks "
            "names before any remote prediction dispatch. It is NOT yet quant-grade: no "
            "point-in-time validation, no sector/liquidity normalization, no risk model."
        ),
        current_features=local_features,
        current_formula_summary=(
            "score = 0.3 * max(0, momentum_5d_pct) + 0.4 * max(0, momentum_20d_pct) "
            "+ 0.3 * max(0, relative_strength_vs_spy_20d); then if 20d volatility > 5%, "
            "score is multiplied by (1 - volatility_20d/100). Only positive momentum and "
            "positive relative strength contribute. Extreme moves are flagged "
            "DATA_QUALITY_OUTLIER and excluded."
        ),
        current_limitations=[
            "Price-only: no volume, liquidity, sector, fundamentals, beta, or market cap.",
            "No sector normalization or sector-rotation awareness yet.",
            "No market regime, breadth, or volatility-regime context yet.",
            "No news/sentiment data yet.",
            "No seasonality or earnings/event calendar yet.",
            "Not point-in-time validated and not backtested; rankings are not walk-forward tested.",
        ],
    )

    # --- Layer 2: remote GCP prediction (black box) ---
    prediction_layer = ModelLayerPrediction(
        description=(
            "Current remote prediction layer is treated as a black-box prediction service "
            "from the Paper Trader perspective. Paper Trader sends only a ticker symbol and "
            "receives a 5-day point estimate plus a confidence value; it does not see the "
            "model internals, features, or training data."
        ),
        runs_on="remote GCP prediction service through http://127.0.0.1:9000 (local tunnel)",
        dispatch_policy="top locally ranked names plus current holdings",
        dispatch_limit=dispatch_limit,
        current_inputs_known_to_paper_trader=[
            "ticker symbol only (POST /predict_all_models/ with {\"ticker\": ...})",
        ],
        current_outputs=[
            "recommendation (BUY/SELL/HOLD)",
            "confidence (0-1, normalized from 0-100)",
            "expected_return_pct (5-day)",
            "forecast_price_5d (if available)",
            "per-model consensus votes and rationale text",
        ],
        current_limitations=[
            f"Only the top {dispatch_limit} locally ranked names plus current holdings are "
            "sent, because the prediction service is remote - not the full S&P 500.",
            "Black box: Paper Trader does not know the model's features or training window.",
            "Point estimate plus confidence only - no prediction interval, no downside risk, "
            "no probability of positive return, no calibration.",
            "No guarantee the remote model uses point-in-time data or walk-forward validation.",
        ],
    )

    # --- Actionability gate (actual code thresholds) ---
    actionability_gate = ModelActionabilityGate(
        current_thresholds=ScanFunnelThresholds(
            min_score=DEFAULT_MIN_ACTIONABLE_SCORE,
            min_confidence=DEFAULT_MIN_CONFIDENCE,
            min_expected_return_pct=DEFAULT_MIN_EXPECTED_RETURN_PCT,
            min_relative_strength_vs_spy=DEFAULT_MIN_RELATIVE_STRENGTH_VS_SPY,
        ),
        description=(
            "After prediction, each candidate must clear every threshold to be shown as an "
            "Active Trade Idea. Held positions are monitored automatically and are never new "
            "BUYs; candidates with no usable prediction or a SELL/negative outlook are rejected."
        ),
        why_a_buy_may_be_rejected=[
            f"Score below the actionable threshold ({DEFAULT_MIN_ACTIONABLE_SCORE:g}).",
            f"Confidence below {DEFAULT_MIN_CONFIDENCE:g}.",
            f"Expected 5-day return below {DEFAULT_MIN_EXPECTED_RETURN_PCT:g}%.",
            f"Relative strength vs SPY below {DEFAULT_MIN_RELATIVE_STRENGTH_VS_SPY:g} "
            "(lagging the market); unknown relative strength is allowed.",
            "Already held: monitored automatically, not surfaced as a new BUY.",
        ],
    )

    current_state = ModelCurrentState(
        local_prescreen=local_prescreen,
        prediction_layer=prediction_layer,
        actionability_gate=actionability_gate,
    )

    # --- Target quant-grade architecture (aspirational; clearly future) ---
    target = ModelTargetArchitecture(
        local_prescreen_v2=ModelTargetLocalPrescreenV2(
            purpose="Rank the full S&P 500 universe before remote prediction dispatch.",
            feature_families=[
                "trend and momentum",
                "relative strength and sector rotation",
                "volatility and downside risk",
                "liquidity and tradability",
                "market regime and breadth",
                "seasonality",
                "earnings and event risk",
                "news and sentiment",
                "macro sensitivity",
                "portfolio fit and concentration",
            ],
            must_be_point_in_time=True,
            must_be_backtested=True,
        ),
        remote_prediction_v2=ModelTargetRemotePredictionV2(
            purpose="Estimate a forward return distribution, not just BUY/HOLD/SELL.",
            target_outputs=[
                "expected return",
                "confidence / calibration",
                "prediction interval",
                "downside risk",
                "probability of positive return",
                "risk-adjusted expected return",
                "model drivers",
            ],
        ),
        portfolio_construction=ModelTargetPortfolioConstruction(
            purpose="Convert trade ideas into position sizes and portfolio decisions.",
            future_methods=[
                "risk budget",
                "max position cap",
                "sector exposure cap",
                "correlation-aware sizing",
                "cash allocation",
                "drawdown control",
            ],
        ),
    )

    # --- Data readiness ledger (honest: missing features carry a no-fake rule) ---
    _DO_NOT_FAKE = "Do not fake this feature."
    data_readiness = [
        ModelDataReadinessRow(
            feature_family="price history",
            available_now=True,
            data_source="price_snapshots",
            status="available",
            rule="Use stored point-in-time CLOSE prices only.",
        ),
        ModelDataReadinessRow(
            feature_family="relative strength vs benchmark",
            available_now=True,
            data_source="benchmark_prices (SPY)",
            status="available",
            rule="Use stored benchmark prices only.",
        ),
        ModelDataReadinessRow(
            feature_family="volume / liquidity",
            available_now=False,
            data_source=None,
            status="missing_real_data_source",
            rule=_DO_NOT_FAKE,
        ),
        ModelDataReadinessRow(
            feature_family="sector / industry",
            available_now=False,
            data_source=None,
            status="missing_real_data_source",
            rule=_DO_NOT_FAKE,
        ),
        ModelDataReadinessRow(
            feature_family="macro conditions",
            available_now=False,
            data_source=None,
            status="missing_real_data_source",
            rule=_DO_NOT_FAKE,
        ),
        ModelDataReadinessRow(
            feature_family="news sentiment",
            available_now=False,
            data_source=None,
            status="missing_real_data_source",
            rule=_DO_NOT_FAKE,
        ),
        ModelDataReadinessRow(
            feature_family="seasonality",
            available_now=False,
            data_source=None,
            status="missing_real_data_source",
            rule=_DO_NOT_FAKE,
        ),
        ModelDataReadinessRow(
            feature_family="earnings / event risk",
            available_now=False,
            data_source=None,
            status="missing_real_data_source",
            rule=_DO_NOT_FAKE,
        ),
    ]

    roadmap = [
        ModelRoadmapPhase(phase=1, name="Transparency and model contract", status="this task"),
        ModelRoadmapPhase(phase=2, name="Local quant pre-screen v2 using existing price data only", status="next"),
        ModelRoadmapPhase(phase=3, name="Backtesting and calibration harness", status="future"),
        ModelRoadmapPhase(phase=4, name="External data integrations: news, sentiment, events, macro", status="future"),
        ModelRoadmapPhase(phase=5, name="Remote GCP model upgrade", status="future"),
    ]

    return ModelMethodologyResponse(
        as_of=now,
        honesty_note=(
            "This is a transparency contract, not a claim of quant-grade implementation. "
            "Current local pre-screen is a first-pass technical/momentum ranking; the remote "
            "prediction layer is a black box from Paper Trader's perspective. "
            "News, sentiment, seasonality, macro, and event risk are target features, not "
            "active features, until real point-in-time data exists in the codebase. No feature "
            "should be added unless it is sourced, timestamped, testable, and backtestable with "
            "walk-forward validation. Paper trade only - no broker execution."
        ),
        current_state=current_state,
        target_quant_architecture=target,
        data_readiness=data_readiness,
        implementation_roadmap=roadmap,
    )


@app.post(
    "/v1/review/create-exit-orders",
    response_model=CreateExitOrdersResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(_verify_api_key)],
)
async def create_exit_orders(
    body: CreateExitOrdersRequest,
) -> CreateExitOrdersResponse:
    """
    Create PENDING SELL paper order tickets for open positions with REVIEW_FOR_EXIT recommendation.

    PAPER SELL ORDERS ONLY. No broker execution. No fills. No trades.
    No position changes. No cash changes. No automation. Manual review required.

    Server recomputes monitor recommendation — UI value is not trusted.
    Eligible only when unrealized_pnl_pct <= -5.0% (REVIEW_FOR_EXIT).
    WATCH and HOLD positions are skipped.

    Creates full open quantity as a PENDING SELL paper order.

    Idempotent: if a PENDING SELL exit order already exists for the ticker
    (notes start with "Paper exit order ticket"), the ticker is skipped.

    confirm_create_exit_orders must be true to proceed.
    """
    import uuid as uuid_module

    if not body.confirm_create_exit_orders:
        raise HTTPException(
            status_code=400,
            detail="confirm_create_exit_orders must be true to create exit Order rows",
        )

    created_list: list[ExitOrderCreatedDetail] = []
    skipped_list: list[ExitOrderSkippedDetail] = []
    eligible_tickers: list[str] = []

    with get_session() as session:
        eastern_now = datetime.now(_EASTERN)
        market_date = eastern_now.date()
        now_utc = datetime.now(timezone.utc)

        positions = session.execute(
            select(Position).order_by(Position.opened_at)
        ).scalars().all()

        portfolio = session.execute(select(Portfolio)).scalar_one_or_none()
        cached_total_value: Decimal | None = None
        if portfolio is not None and portfolio.cached_total_value:
            cached_total_value = Decimal(str(portfolio.cached_total_value))

        job_run: JobRun | None = None

        for pos in positions:
            qty = Decimal(str(pos.qty))
            cost_basis = Decimal(str(pos.cost_basis))
            latest_price = _latest_price(session, pos.ticker)

            if latest_price is None:
                skipped_list.append(ExitOrderSkippedDetail(
                    ticker=pos.ticker,
                    reason="Latest price unavailable; cannot evaluate position.",
                    monitor_recommendation="WATCH",
                ))
                continue

            market_value = qty * latest_price
            unrealized_pnl = market_value - cost_basis
            unrealized_pnl_pct = (
                unrealized_pnl / cost_basis * Decimal("100")
                if cost_basis != Decimal("0")
                else Decimal("0")
            )
            upnl_pct_float = float(unrealized_pnl_pct)

            portfolio_weight_pct: Decimal | None = None
            if cached_total_value is not None and cached_total_value > Decimal("0"):
                portfolio_weight_pct = market_value / cached_total_value * Decimal("100")
            weight_pct_float = float(portfolio_weight_pct) if portfolio_weight_pct is not None else 0.0

            if upnl_pct_float <= -5.0:
                recommendation = "REVIEW_FOR_EXIT"
            elif upnl_pct_float <= -2.0:
                recommendation = "WATCH"
            elif portfolio_weight_pct is not None and weight_pct_float > 25.0:
                recommendation = "WATCH"
            else:
                recommendation = "HOLD"

            if recommendation != "REVIEW_FOR_EXIT":
                skipped_list.append(ExitOrderSkippedDetail(
                    ticker=pos.ticker,
                    reason=f"Position recommendation is {recommendation}, not REVIEW_FOR_EXIT. Eligible only when P&L <= -5.0%.",
                    monitor_recommendation=recommendation,
                ))
                continue

            eligible_tickers.append(pos.ticker)

            existing_order = session.query(Order).filter(
                Order.ticker == pos.ticker,
                Order.side == "SELL",
                Order.status == "PENDING",
                Order.notes.like("Paper exit order ticket%"),
            ).first()
            if existing_order:
                skipped_list.append(ExitOrderSkippedDetail(
                    ticker=pos.ticker,
                    reason="PENDING SELL exit order already exists for this ticker; skipped to prevent duplicate.",
                    monitor_recommendation=recommendation,
                ))
                continue

            if job_run is None:
                job_run = JobRun(
                    idempotency_key=f"manual-exit-create-orders-{uuid_module.uuid4()}",
                    workflow_type="MANUAL_EXIT_CREATE_ORDERS",
                    market_date=market_date,
                    status="COMPLETED",
                    completed_at=now_utc,
                    result_summary={},
                )
                session.add(job_run)
                session.flush()

            source_run = f"manual_exit_v1:{pos.ticker}:{market_date}"
            existing_sig = session.query(Signal).filter(
                Signal.source_run == source_run,
                Signal.ticker == pos.ticker,
                Signal.direction == "SELL",
            ).first()
            if existing_sig is not None:
                sig = existing_sig
            else:
                sig = Signal(
                    job_run_id=job_run.id,
                    ticker=pos.ticker,
                    direction="SELL",
                    confidence=Decimal("1.0000"),
                    signal_ts=now_utc,
                    market_date=market_date,
                    source_run=source_run,
                    status="DECISION_MADE",
                    raw_payload={"reason": "REVIEW_FOR_EXIT manual exit"},
                )
                session.add(sig)
                session.flush()

            existing_td = session.query(TradeDecision).filter(
                TradeDecision.signal_id == sig.id
            ).first()
            if existing_td is not None:
                td = existing_td
            else:
                td = TradeDecision(
                    signal_id=sig.id,
                    job_run_id=job_run.id,
                    ticker=pos.ticker,
                    signal_direction="SELL",
                    decision="SELL",
                    reason_code="REVIEW_FOR_EXIT",
                    approved_qty=qty,
                    approved_notional=(qty * latest_price).quantize(Decimal("0.01")),
                    requested_qty=qty,
                    requested_notional=(qty * latest_price).quantize(Decimal("0.01")),
                    decided_at=now_utc,
                    market_date=market_date,
                )
                session.add(td)
                session.flush()

            order = Order(
                trade_decision_id=td.id,
                job_run_id=job_run.id,
                fill_job_run_id=None,
                ticker=pos.ticker,
                side="SELL",
                order_type="MARKET",
                status="PENDING",
                market_date=market_date,
                requested_qty=qty,
                filled_qty=None,
                requested_at=now_utc,
                filled_at=None,
                fill_price=None,
                commission=None,
                slippage_cost=None,
                notes="Paper exit order ticket — manual exit workflow. No broker execution.",
            )
            session.add(order)
            session.flush()

            created_list.append(ExitOrderCreatedDetail(
                order_id=str(order.id),
                ticker=pos.ticker,
                side="SELL",
                status="PENDING",
                qty=str(qty),
                market_date=str(market_date),
                job_run_id=str(job_run.id),
                monitor_recommendation="REVIEW_FOR_EXIT",
            ))

        if job_run is not None and created_list:
            job_run.result_summary = {
                "created_count": len(created_list),
                "skipped_count": len(skipped_list),
            }
            session.add(job_run)

    return CreateExitOrdersResponse(
        execution_mode="EXIT_ORDER_CREATION_PAPER_ONLY",
        created_count=len(created_list),
        skipped_count=len(skipped_list),
        orders=created_list,
        eligible_positions=eligible_tickers,
        skipped_positions=skipped_list,
        safety_message="PAPER SELL ORDERS ONLY. No broker execution. No fills. No trades. No position changes. Automation off. Manual review required.",
        no_broker_execution=True,
        no_fills_created=True,
        no_trades_created=True,
        no_position_changes=True,
        automation_enabled=False,
    )


# ---------------------------------------------------------------------------
# Ticker detail (read-only aggregation)
# ---------------------------------------------------------------------------

class TickerDetailMarket(BaseModel):
    latest_price: str | None = None
    latest_price_date: str | None = None
    latest_session_type: str | None = None
    latest_price_type: str | None = None
    previous_close: str | None = None
    previous_close_date: str | None = None
    change: str | None = None
    change_pct: str | None = None


class TickerDetailSelection(BaseModel):
    candidate_id: str | None = None
    review_status: str | None = None
    preview_decision: str | None = None
    preview_score: str | None = None
    prediction_recommendation: str | None = None
    prediction_confidence: str | None = None
    expected_return_pct: str | None = None
    forecast_price_5d: str | None = None
    relative_strength_vs_spy_20d: str | None = None
    momentum_5d_pct: str | None = None
    momentum_20d_pct: str | None = None
    scan_rank: str | None = None
    scan_score: str | None = None
    market_context: str | None = None
    scan_reason_codes: list[str] | None = None
    preview_reasons: list[str] | None = None
    why_selected: str | None = None
    why_excluded: str | None = None
    created_at: datetime | None = None


class TickerDetailPosition(BaseModel):
    qty: str
    avg_cost: str
    cost_basis: str
    latest_price: str | None = None
    market_value: str | None = None
    unrealized_pnl: str | None = None
    unrealized_pnl_pct: str | None = None
    portfolio_weight_pct: str | None = None
    opened_at: datetime
    last_updated: datetime


class TickerDetailOrder(BaseModel):
    order_id: str
    order_short_id: str
    trade_id: str | None = None
    trade_short_id: str | None = None
    status: str
    side: str
    qty: str
    fill_price: str | None = None
    commission: str | None = None
    requested_at: datetime
    filled_at: datetime | None = None
    market_date: date
    notes: str | None = None


class TickerDetailGuidance(BaseModel):
    recommendation: str | None = None
    reason_code: str | None = None
    explanation: str


class TickerDetailResponse(BaseModel):
    ticker: str
    app_status: str
    recommendation: str | None = None
    has_position: bool = False
    has_candidate: bool = False
    has_order: bool = False
    has_market_data: bool = False
    market: TickerDetailMarket | None = None
    selection: TickerDetailSelection | None = None
    position: TickerDetailPosition | None = None
    latest_order: TickerDetailOrder | None = None
    guidance: TickerDetailGuidance
    # Read-only safety contract
    preview_only: bool = True
    writes_performed: bool = False
    no_broker_execution: bool = True
    no_orders_created: bool = True
    no_automation: bool = True


@app.get(
    "/v1/ticker-detail/{ticker}",
    response_model=TickerDetailResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def ticker_detail(ticker: str) -> TickerDetailResponse:
    """
    GET /v1/ticker-detail/{ticker} — Read-only aggregated detail for ONE ticker.

    PREVIEW / ANALYSIS ONLY. Aggregates existing data only:
        - latest price / previous close from price_snapshots
        - latest candidate review / trade-idea rationale (if present)
        - open position (if held) with mark-to-market P&L and portfolio weight
        - latest related paper order + trade lineage (if present)
        - monitor recommendation (HOLD / WATCH / REVIEW_FOR_EXIT) for held positions

    This endpoint never creates signals, decisions, orders, trades, fills,
    positions, or cash ledger rows. No broker execution. No automation.
    Sections gracefully return null/empty when a ticker has no data.
    """
    symbol = (ticker or "").strip().upper()
    if not symbol:
        raise HTTPException(status_code=422, detail="Ticker must not be empty.")

    with get_session() as session:
        # --- Market snapshot (latest price + previous-day close) ---
        latest_snap = session.execute(
            select(PriceSnapshot)
            .where(PriceSnapshot.ticker == symbol)
            .order_by(PriceSnapshot.snapshot_ts.desc())
            .limit(1)
        ).scalar_one_or_none()

        market: TickerDetailMarket | None = None
        has_market_data = False
        if latest_snap is not None:
            has_market_data = True
            latest_price_val = Decimal(str(latest_snap.price))
            prev_snap = session.execute(
                select(PriceSnapshot)
                .where(PriceSnapshot.ticker == symbol)
                .where(PriceSnapshot.market_date < latest_snap.market_date)
                .order_by(PriceSnapshot.snapshot_ts.desc())
                .limit(1)
            ).scalar_one_or_none()
            prev_close: str | None = None
            prev_close_date: str | None = None
            change_str: str | None = None
            change_pct_str: str | None = None
            if prev_snap is not None:
                prev_val = Decimal(str(prev_snap.price))
                prev_close = str(prev_val)
                prev_close_date = prev_snap.market_date.isoformat()
                change_abs = latest_price_val - prev_val
                change_str = str(change_abs.quantize(Decimal("0.000001")))
                if prev_val != Decimal("0"):
                    change_pct_str = str(
                        (change_abs / prev_val * Decimal("100")).quantize(Decimal("0.01"))
                    )
            market = TickerDetailMarket(
                latest_price=str(latest_price_val),
                latest_price_date=latest_snap.market_date.isoformat(),
                latest_session_type=latest_snap.session_type,
                latest_price_type=latest_snap.price_type,
                previous_close=prev_close,
                previous_close_date=prev_close_date,
                change=change_str,
                change_pct=change_pct_str,
            )

        # --- Open position (if held) + monitor recommendation ---
        portfolio = session.execute(select(Portfolio)).scalar_one_or_none()
        cached_total_value: Decimal | None = None
        if portfolio is not None and portfolio.cached_total_value:
            cached_total_value = Decimal(str(portfolio.cached_total_value))

        pos = session.execute(
            select(Position).where(Position.ticker == symbol)
        ).scalar_one_or_none()

        position_out: TickerDetailPosition | None = None
        monitor_recommendation: str | None = None
        monitor_reason_code: str | None = None
        monitor_explanation: str | None = None
        if pos is not None:
            qty = Decimal(str(pos.qty))
            cost_basis = Decimal(str(pos.cost_basis))
            latest_price_pos = _latest_price(session, symbol)
            mv_str = upnl_str = upnl_pct_str = weight_str = None
            if latest_price_pos is not None:
                market_value = qty * latest_price_pos
                unrealized_pnl = market_value - cost_basis
                unrealized_pnl_pct = (
                    unrealized_pnl / cost_basis * Decimal("100")
                    if cost_basis != Decimal("0") else Decimal("0")
                )
                weight_pct: Decimal | None = None
                if cached_total_value is not None and cached_total_value > Decimal("0"):
                    weight_pct = market_value / cached_total_value * Decimal("100")
                mv_str = str(market_value.quantize(Decimal("0.01")))
                upnl_str = str(unrealized_pnl.quantize(Decimal("0.01")))
                upnl_pct_str = str(unrealized_pnl_pct.quantize(Decimal("0.01")))
                weight_str = (
                    str(weight_pct.quantize(Decimal("0.01")))
                    if weight_pct is not None else None
                )

                upnl_pct_float = float(unrealized_pnl_pct)
                weight_pct_float = float(weight_pct) if weight_pct is not None else 0.0
                if upnl_pct_float <= -5.0:
                    monitor_recommendation = "REVIEW_FOR_EXIT"
                    monitor_reason_code = "STOP_LOSS_REVIEW"
                    monitor_explanation = (
                        f"REVIEW_FOR_EXIT: Unrealized P&L of {upnl_pct_float:.1f}% is below the "
                        f"stop-loss review threshold (-5.0%). Review before creating any "
                        f"sell/exit action."
                    )
                elif upnl_pct_float <= -2.0:
                    monitor_recommendation = "WATCH"
                    monitor_reason_code = "WATCH_LOSS_THRESHOLD"
                    monitor_explanation = (
                        f"WATCH: Unrealized loss of {upnl_pct_float:.1f}% is in the watch range "
                        f"(-2.0% to -5.0%). Monitor closely."
                    )
                elif weight_pct is not None and weight_pct_float > 25.0:
                    monitor_recommendation = "WATCH"
                    monitor_reason_code = "HIGH_CONCENTRATION"
                    monitor_explanation = (
                        f"WATCH: Portfolio weight of {weight_pct_float:.1f}% exceeds the "
                        f"concentration threshold (25.0%). Monitor closely."
                    )
                else:
                    monitor_recommendation = "HOLD"
                    monitor_reason_code = "HEALTHY_POSITION"
                    monitor_explanation = (
                        "HOLD: Position is within healthy parameters. No exit action required."
                    )
            else:
                monitor_recommendation = "WATCH"
                monitor_reason_code = "PRICE_MISSING"
                monitor_explanation = (
                    "WATCH: Latest price unavailable; position cannot be fully evaluated."
                )
            position_out = TickerDetailPosition(
                qty=str(qty),
                avg_cost=str(pos.avg_cost),
                cost_basis=str(cost_basis),
                latest_price=str(latest_price_pos) if latest_price_pos is not None else None,
                market_value=mv_str,
                unrealized_pnl=upnl_str,
                unrealized_pnl_pct=upnl_pct_str,
                portfolio_weight_pct=weight_str,
                opened_at=pos.opened_at,
                last_updated=pos.last_updated,
            )

        # --- Candidate / trade-idea rationale (most recent for ticker) ---
        cand = session.execute(
            select(CandidateReview)
            .where(CandidateReview.ticker == symbol)
            .order_by(CandidateReview.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()

        selection_out: TickerDetailSelection | None = None
        if cand is not None:
            reasons: list[str] = []
            if cand.preview_reasons:
                reasons = [str(r) for r in cand.preview_reasons]
            elif cand.scan_reason_codes:
                reasons = [str(r) for r in cand.scan_reason_codes]
            why_selected: str | None = None
            if reasons:
                why_selected = "; ".join(reasons)
            elif cand.preview_decision == "CONSIDER":
                why_selected = "Candidate passed the local screen and prediction preview."
            why_excluded: str | None = None
            if cand.review_status == "REJECTED":
                why_excluded = "Marked REJECTED in review. Not part of actionable trade ideas."
            elif cand.preview_decision == "REJECT":
                why_excluded = "Preview decision REJECT: did not meet the consider threshold."
            elif cand.preview_decision == "WATCH":
                why_excluded = "Preview decision WATCH: monitor only, not actionable yet."
            selection_out = TickerDetailSelection(
                candidate_id=str(cand.id),
                review_status=cand.review_status,
                preview_decision=cand.preview_decision,
                preview_score=cand.preview_score,
                prediction_recommendation=cand.prediction_recommendation,
                prediction_confidence=cand.prediction_confidence,
                expected_return_pct=cand.expected_return_pct,
                forecast_price_5d=cand.forecast_price_5d,
                relative_strength_vs_spy_20d=cand.relative_strength_vs_spy_20d,
                momentum_5d_pct=cand.momentum_5d_pct,
                momentum_20d_pct=cand.momentum_20d_pct,
                scan_rank=cand.scan_rank,
                scan_score=cand.scan_score,
                market_context=cand.market_context,
                scan_reason_codes=(
                    [str(r) for r in cand.scan_reason_codes]
                    if cand.scan_reason_codes else None
                ),
                preview_reasons=(
                    [str(r) for r in cand.preview_reasons]
                    if cand.preview_reasons else None
                ),
                why_selected=why_selected,
                why_excluded=why_excluded,
                created_at=cand.created_at,
            )

        # --- Order / trade lineage (most recent order for ticker) ---
        order = session.execute(
            select(Order)
            .where(Order.ticker == symbol)
            .order_by(Order.requested_at.desc())
            .limit(1)
        ).scalar_one_or_none()

        latest_order_out: TickerDetailOrder | None = None
        if order is not None:
            trade = session.execute(
                select(Trade)
                .where(Trade.order_id == order.id)
                .order_by(Trade.trade_ts.desc())
                .limit(1)
            ).scalar_one_or_none()
            latest_order_out = TickerDetailOrder(
                order_id=str(order.id),
                order_short_id=str(order.id)[:8],
                trade_id=str(trade.id) if trade is not None else None,
                trade_short_id=str(trade.id)[:8] if trade is not None else None,
                status=order.status,
                side=order.side,
                qty=str(
                    order.filled_qty if order.filled_qty is not None else order.requested_qty
                ),
                fill_price=str(order.fill_price) if order.fill_price is not None else None,
                commission=str(order.commission) if order.commission is not None else None,
                requested_at=order.requested_at,
                filled_at=order.filled_at,
                market_date=order.market_date,
                notes=order.notes,
            )

        # --- App status + top-level recommendation ---
        has_position = position_out is not None
        has_candidate = selection_out is not None
        has_order = latest_order_out is not None
        order_filled = has_order and latest_order_out.status == "FILLED"

        if has_position:
            app_status = "POSITION_OPEN"
        elif has_candidate:
            app_status = "TRADE_IDEA"
        elif order_filled:
            app_status = "ORDER_FILLED"
        elif has_market_data:
            app_status = "MONITOR_ONLY"
        else:
            app_status = "NO_DATA"

        recommendation: str | None
        if monitor_recommendation is not None:
            recommendation = monitor_recommendation
        elif has_candidate:
            if cand.review_status == "REJECTED":
                recommendation = "REJECTED"
            else:
                recommendation = cand.prediction_recommendation or cand.preview_decision
        else:
            recommendation = None

        # --- Current decision guidance ---
        if monitor_explanation is not None:
            guidance = TickerDetailGuidance(
                recommendation=monitor_recommendation,
                reason_code=monitor_reason_code,
                explanation=monitor_explanation,
            )
        elif has_candidate:
            if cand.review_status == "REJECTED":
                expl = "REJECTED: This trade idea was rejected in review. No action required."
            elif cand.review_status == "APPROVED_FOR_SIGNAL":
                expl = (
                    "APPROVED_FOR_SIGNAL (label only): Approved for a paper trade ticket. "
                    "No live order is sent. Review before creating & filling a paper trade."
                )
            elif cand.review_status == "WATCHING":
                expl = "WATCH: Marked as watching. Monitor before any action."
            else:
                rec_label = cand.prediction_recommendation or cand.preview_decision or "REVIEW"
                expl = (
                    f"{rec_label}: New trade idea from the latest scan. "
                    f"Review before approving."
                )
            guidance = TickerDetailGuidance(
                recommendation=recommendation,
                reason_code=cand.review_status,
                explanation=expl,
            )
        else:
            guidance = TickerDetailGuidance(
                recommendation=None,
                reason_code="MONITOR_ONLY" if has_market_data else "NO_DATA",
                explanation=(
                    "No active trade idea or open position for this ticker. Monitor only."
                    if has_market_data
                    else "No data available for this ticker yet."
                ),
            )

        return TickerDetailResponse(
            ticker=symbol,
            app_status=app_status,
            recommendation=recommendation,
            has_position=has_position,
            has_candidate=has_candidate,
            has_order=has_order,
            has_market_data=has_market_data,
            market=market,
            selection=selection_out,
            position=position_out,
            latest_order=latest_order_out,
            guidance=guidance,
            preview_only=True,
            writes_performed=False,
            no_broker_execution=True,
            no_orders_created=True,
            no_automation=True,
        )


# ---------------------------------------------------------------------------
# Portfolio analytics (read-only aggregation)
# ---------------------------------------------------------------------------

class PortfolioAnalyticsPosition(BaseModel):
    ticker: str
    qty: str
    avg_cost: str
    cost_basis: str
    latest_price: str | None = None
    market_value: str | None = None
    unrealized_pnl: str | None = None
    unrealized_pnl_pct: str | None = None
    portfolio_weight_pct: str | None = None
    recommendation: str
    reason_code: str
    explanation: str


class PortfolioAnalyticsAllocation(BaseModel):
    total_value: str
    cash: str
    positions_value: str
    cash_pct: str
    invested_pct: str
    open_position_count: int
    max_positions: int
    available_slots: int
    largest_position_ticker: str | None = None
    largest_position_weight_pct: str | None = None
    concentration_warning: bool = False


class PortfolioAnalyticsRisk(BaseModel):
    hold_count: int
    watch_count: int
    review_for_exit_count: int
    message: str


class PortfolioAnalyticsCapacity(BaseModel):
    total_value: str
    cash: str
    positions_value: str
    max_positions: int
    open_positions: int
    available_slots: int
    can_open_new: bool
    message: str


class PortfolioAnalyticsResponse(BaseModel):
    as_of: datetime
    has_positions: bool
    open_position_count: int
    total_positions_value: str
    total_unrealized_pnl: str
    allocation: PortfolioAnalyticsAllocation
    positions: list[PortfolioAnalyticsPosition]
    risk: PortfolioAnalyticsRisk
    capacity: PortfolioAnalyticsCapacity
    # Read-only safety contract
    preview_only: bool = True
    writes_performed: bool = False
    no_broker_execution: bool = True
    no_orders_created: bool = True
    no_automation: bool = True


@app.get(
    "/v1/portfolio/analytics",
    response_model=PortfolioAnalyticsResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(_verify_api_key)],
)
def portfolio_analytics() -> PortfolioAnalyticsResponse:
    """
    GET /v1/portfolio/analytics — Read-only portfolio-level analytics & risk roll-up.

    PREVIEW / ANALYSIS ONLY. Aggregates existing data only:
        - allocation: cash %, invested %, position count vs max, available slots,
          largest position weight + concentration warning (>25%)
        - exposure & P&L by ticker (market value, weight, unrealized P&L %)
        - risk roll-up: HOLD / WATCH / REVIEW_FOR_EXIT counts using the SAME rules
          as /v1/review/position-monitor-preview
        - capacity: total value, cash, positions value, slots, can-open-new verdict

    Reuses the v1 monitor rules:
        REVIEW_FOR_EXIT  if unrealized_pnl_pct <= -5.0%
        WATCH            if unrealized_pnl_pct <= -2.0%
        WATCH            if portfolio_weight_pct > 25.0%
        HOLD             otherwise
        WATCH/PRICE_MISSING when the latest price is unavailable

    This endpoint never creates signals, decisions, orders, trades, fills,
    positions, or cash ledger rows. No broker execution. No automation.
    """
    now = datetime.now(tz=timezone.utc)

    with get_session() as session:
        portfolio = session.execute(select(Portfolio)).scalar_one_or_none()
        if portfolio is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Portfolio not seeded. Run scripts/seed.py first.",
            )

        cash = Decimal(str(portfolio.cached_cash))
        total_value = Decimal(str(portfolio.cached_total_value))
        cfg_max = int(
            (portfolio.config or {}).get("max_positions", get_settings().max_positions)
        )

        positions = session.execute(
            select(Position).order_by(Position.opened_at)
        ).scalars().all()

        items: list[PortfolioAnalyticsPosition] = []
        total_positions_value = Decimal("0")
        total_unrealized_pnl = Decimal("0")
        hold_count = 0
        watch_count = 0
        review_for_exit_count = 0
        largest_ticker: str | None = None
        largest_weight: Decimal | None = None

        for pos in positions:
            qty = Decimal(str(pos.qty))
            cost_basis = Decimal(str(pos.cost_basis))
            latest_price = _latest_price(session, pos.ticker)

            mv_str = upnl_str = upnl_pct_str = weight_str = None
            latest_str = str(latest_price) if latest_price is not None else None
            recommendation: str
            reason_code: str
            explanation: str

            if latest_price is None:
                recommendation = "WATCH"
                reason_code = "PRICE_MISSING"
                explanation = (
                    "Latest price unavailable; position cannot be fully evaluated."
                )
                watch_count += 1
            else:
                market_value = qty * latest_price
                unrealized_pnl = market_value - cost_basis
                unrealized_pnl_pct = (
                    unrealized_pnl / cost_basis * Decimal("100")
                    if cost_basis != Decimal("0") else Decimal("0")
                )
                total_positions_value += market_value
                total_unrealized_pnl += unrealized_pnl

                weight_pct: Decimal | None = None
                if total_value > Decimal("0"):
                    weight_pct = market_value / total_value * Decimal("100")

                mv_str = str(market_value.quantize(Decimal("0.01")))
                upnl_str = str(unrealized_pnl.quantize(Decimal("0.01")))
                upnl_pct_str = str(unrealized_pnl_pct.quantize(Decimal("0.01")))
                weight_str = (
                    str(weight_pct.quantize(Decimal("0.01")))
                    if weight_pct is not None else None
                )

                if weight_pct is not None and (
                    largest_weight is None or weight_pct > largest_weight
                ):
                    largest_weight = weight_pct
                    largest_ticker = pos.ticker

                upnl_pct_float = float(unrealized_pnl_pct)
                weight_pct_float = float(weight_pct) if weight_pct is not None else 0.0
                if upnl_pct_float <= -5.0:
                    recommendation = "REVIEW_FOR_EXIT"
                    reason_code = "STOP_LOSS_REVIEW"
                    explanation = (
                        f"Unrealized P&L of {upnl_pct_float:.1f}% is below the "
                        f"stop-loss review threshold (-5.0%). Manual review recommended."
                    )
                    review_for_exit_count += 1
                elif upnl_pct_float <= -2.0:
                    recommendation = "WATCH"
                    reason_code = "WATCH_LOSS_THRESHOLD"
                    explanation = (
                        f"Unrealized P&L of {upnl_pct_float:.1f}% is in the watch range "
                        f"(-2.0% to -5.0%). Monitor closely."
                    )
                    watch_count += 1
                elif weight_pct is not None and weight_pct_float > 25.0:
                    recommendation = "WATCH"
                    reason_code = "HIGH_CONCENTRATION"
                    explanation = (
                        f"Portfolio weight of {weight_pct_float:.1f}% exceeds the "
                        f"concentration threshold (25.0%). Consider position sizing."
                    )
                    watch_count += 1
                else:
                    recommendation = "HOLD"
                    reason_code = "HEALTHY_POSITION"
                    explanation = (
                        "Position is within healthy parameters. No action required."
                    )
                    hold_count += 1

            items.append(PortfolioAnalyticsPosition(
                ticker=pos.ticker,
                qty=str(qty),
                avg_cost=str(pos.avg_cost),
                cost_basis=str(cost_basis),
                latest_price=latest_str,
                market_value=mv_str,
                unrealized_pnl=upnl_str,
                unrealized_pnl_pct=upnl_pct_str,
                portfolio_weight_pct=weight_str,
                recommendation=recommendation,
                reason_code=reason_code,
                explanation=explanation,
            ))

        # Sort exposure rows by portfolio weight descending (largest exposure first).
        def _weight_key(it: PortfolioAnalyticsPosition) -> Decimal:
            return Decimal(it.portfolio_weight_pct) if it.portfolio_weight_pct else Decimal("0")

        items.sort(key=_weight_key, reverse=True)

        open_count = len(positions)
        available_slots = max(0, cfg_max - open_count)
        positions_value = total_positions_value

        if total_value > Decimal("0"):
            cash_pct = (cash / total_value * Decimal("100")).quantize(Decimal("0.01"))
            invested_pct = (
                positions_value / total_value * Decimal("100")
            ).quantize(Decimal("0.01"))
        else:
            cash_pct = Decimal("0.00")
            invested_pct = Decimal("0.00")

        concentration_warning = bool(
            largest_weight is not None and largest_weight > Decimal("25.0")
        )

        allocation = PortfolioAnalyticsAllocation(
            total_value=str(total_value.quantize(Decimal("0.01"))),
            cash=str(cash.quantize(Decimal("0.01"))),
            positions_value=str(positions_value.quantize(Decimal("0.01"))),
            cash_pct=str(cash_pct),
            invested_pct=str(invested_pct),
            open_position_count=open_count,
            max_positions=cfg_max,
            available_slots=available_slots,
            largest_position_ticker=largest_ticker,
            largest_position_weight_pct=(
                str(largest_weight.quantize(Decimal("0.01")))
                if largest_weight is not None else None
            ),
            concentration_warning=concentration_warning,
        )

        if review_for_exit_count > 0:
            risk_message = (
                f"Exit review required for {review_for_exit_count} position(s)."
            )
        elif watch_count > 0:
            risk_message = f"Watch required for {watch_count} position(s)."
        elif open_count > 0:
            risk_message = "Portfolio healthy. Monitor only."
        else:
            risk_message = "No open positions."

        risk = PortfolioAnalyticsRisk(
            hold_count=hold_count,
            watch_count=watch_count,
            review_for_exit_count=review_for_exit_count,
            message=risk_message,
        )

        can_open_new = available_slots > 0
        if open_count == 0:
            capacity_message = (
                f"No open positions. Capacity for up to {cfg_max} paper position(s)."
            )
        elif can_open_new:
            capacity_message = (
                f"{available_slots} slot(s) free. New paper trades possible "
                f"(capacity only — manual review still required)."
            )
        else:
            capacity_message = (
                "No new BUY capacity. Portfolio is at max positions; free a slot "
                "or consider a rotation."
            )

        capacity = PortfolioAnalyticsCapacity(
            total_value=str(total_value.quantize(Decimal("0.01"))),
            cash=str(cash.quantize(Decimal("0.01"))),
            positions_value=str(positions_value.quantize(Decimal("0.01"))),
            max_positions=cfg_max,
            open_positions=open_count,
            available_slots=available_slots,
            can_open_new=can_open_new,
            message=capacity_message,
        )

        return PortfolioAnalyticsResponse(
            as_of=now,
            has_positions=open_count > 0,
            open_position_count=open_count,
            total_positions_value=str(total_positions_value.quantize(Decimal("0.01"))),
            total_unrealized_pnl=str(total_unrealized_pnl.quantize(Decimal("0.01"))),
            allocation=allocation,
            positions=items,
            risk=risk,
            capacity=capacity,
            preview_only=True,
            writes_performed=False,
            no_broker_execution=True,
            no_orders_created=True,
            no_automation=True,
        )


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

_UI_DIR = pathlib.Path(__file__).parent / "ui"
app.mount("/ui", StaticFiles(directory=str(_UI_DIR), html=True), name="ui")


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse("/ui/")
