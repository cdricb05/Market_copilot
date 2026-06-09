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
    PriceSnapshot,
    Signal,
    TradeDecision,
)
from paper_trader.db.session import get_dedicated_session, get_session
from paper_trader.engine.market_data import fetch_latest_prices, fetch_historical_prices, fetch_market_indicator_latest, fetch_fred_latest_series
from paper_trader.engine.market_hours import is_weekday
from paper_trader.engine.portfolio import compute_cash, get_open_positions, get_portfolio
from paper_trader.engine.prediction_client import (
    fetch_predictions_for_tickers,
    normalize_prediction_response,
    normalize_prediction_response_with_error,
)
from paper_trader.engine.prediction_strategy import generate_prediction_signals
from paper_trader.engine.reconciler import run_fill_cycle
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


class MarketIndicator(BaseModel):
    key: str
    label: str
    symbol: str | None = None
    value: str | None = None
    change: str | None = None
    change_pct: str | None = None
    source: str
    available: bool
    as_of: str | None = None
    status: str | None = None


class MarketIndicatorPlaceholder(BaseModel):
    key: str
    label: str
    available: bool
    reason: str
    value: str | None = None
    as_of: str | None = None
    status: str | None = None
    source: str | None = None


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


class MarketScanPredictionCandidatesResponse(BaseModel):
    """Response from market scan + prediction candidate preview endpoint."""
    idempotency_key: str
    dry_run: bool
    execution_mode: str
    scan: ScanSummaryOut
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
    created_at: datetime
    updated_at: datetime


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
    if body.include_current_positions_for_prediction:
        with get_dedicated_session() as _pos_session:
            open_positions = list(_pos_session.execute(select(Position)).scalars().all())
        selected_set = set(selected_tickers)
        for pos in open_positions:
            t = pos.ticker.upper()
            holding_tickers_set.add(t)
            if t not in selected_set:
                holdings_injected.append(t)
                selected_set.add(t)
        selected_tickers = selected_tickers + holdings_injected

    # Fetch predictions for selected tickers (bounded concurrency)
    fetched_responses = []
    fetch_failures = []
    _t_fetch_start = datetime.now(timezone.utc)

    if selected_tickers:
        try:
            fetched_responses, fetch_failures = await fetch_predictions_for_tickers(
                tickers=selected_tickers,
                api_url=settings.stock_prediction_api_url,
                timeout_seconds=settings.stock_prediction_api_timeout_seconds,
                max_concurrency=body.max_prediction_concurrency,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch predictions: {str(exc)}",
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
        if status == "OK" and normalized:
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
        elif status != "OK":
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
        if status != "OK":
            _skip_warn = f"PREDICTION_{status}"
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
            elif status != "OK":
                _rq_reason = "MISSING_PREDICTION"
            else:
                _rq_reason = "OTHER"

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
            price_history_points=price_history_points,
            prediction_status=status,
            selected_for_gcp_reason=selected_for_gcp_reason,
            top_score_drivers=_top_drivers,
            skip_or_warning_reason=_skip_warn,
            candidate_type=_candidate_type,
            is_current_holding=is_holding,
            eligible_for_review_queue=_eligible_for_rq,
            review_queue_eligibility_reason=_rq_reason,
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
        saved_new_candidates=inserted,
        skipped_current_holdings=skipped_current_holdings,
        skipped_watch=skipped_watch,
        skipped_rejected=skipped_rejected,
        skipped_other=skipped_other,
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

        # Step 9: Create Orders (paper tickets only, no broker execution)
        if order_eligible == 0:
            steps.append(WorkflowStepStatus(
                step="Create Orders",
                status="BLOCKED",
                reason="No order-eligible trade decisions. Complete Order Preview first.",
            ))
        else:
            steps.append(WorkflowStepStatus(
                step="Create Orders",
                status="READY",
                reason=f"{order_eligible} trade decision(s) eligible for paper order creation.",
            ))

    return WorkflowStatusResponse(
        review_candidates=candidates_counts,
        review_created_signals=signals_counts,
        review_created_trade_decisions=decisions_counts,
        orders=orders_counts,
        workflow_steps=steps,
        safety={"create_orders_enabled": True, "automation_enabled": False},
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

    # Build indicators list; fall back to per-symbol history for any batch miss
    now_str = datetime.now(timezone.utc).isoformat()
    indicators = []

    for key, label, symbol in indicator_config:
        if symbol in price_map:
            indicators.append(
                MarketIndicator(
                    key=key,
                    label=label,
                    symbol=symbol,
                    value=price_map[symbol],
                    change=None,
                    change_pct=None,
                    source="yfinance",
                    available=True,
                    as_of=now_str,
                    status="yfinance live",
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
                        change=None,
                        change_pct=None,
                        source="yfinance",
                        available=True,
                        as_of=hist["as_of"],
                        status=hist["status"],
                    )
                )
            else:
                indicators.append(
                    MarketIndicator(
                        key=key,
                        label=label,
                        symbol=symbol,
                        value=None,
                        change=None,
                        change_pct=None,
                        source="yfinance",
                        available=False,
                        as_of=None,
                        status="yfinance unavailable",
                    )
                )

    # Fetch FRED macro indicators
    fred_api_key = get_settings().fred_api_key
    fred_results = fetch_fred_latest_series(
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
                )
            )

    return MarketIndicatorsResponse(
        status="ok",
        source="yfinance",
        as_of=now_str,
        indicators=indicators,
        placeholders=placeholders,
    )


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

_UI_DIR = pathlib.Path(__file__).parent / "ui"
app.mount("/ui", StaticFiles(directory=str(_UI_DIR), html=True), name="ui")


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse("/ui/")
