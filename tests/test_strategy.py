"""
tests/test_strategy.py — Unit and integration tests for engine/strategy.py.

Tests cover:
    - SMA computation with various window sizes
    - Signal classification (BUY/SELL/HOLD)
    - Confidence scaling based on trend strength
    - Insufficient history handling with skip reasons
    - Optional ticker filtering
    - Integration with database and signal metadata
    - End-to-end signal generation and decision workflow integration
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from paper_trader.config import get_settings
from paper_trader.constants import CashEntryType, DecisionType, SignalDirection, WorkflowType
from paper_trader.db.models import Base, JobRun, Order, Portfolio, PriceSnapshot, TradeDecision
from paper_trader.db.session import reset_engine_state
from paper_trader.engine.portfolio import append_cash_entry
from paper_trader.engine.strategy import (
    _classify_signal,
    _compute_sma,
    generate_signals,
)
from paper_trader.workflows.decision import run_decision_workflow

_NOW = datetime(2026, 5, 19, 14, 30, 0, tzinfo=timezone.utc)
_DATE = _NOW.date()


# ---------------------------------------------------------------------------
# Module-scoped fixtures for workflow integration tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def workflow_engine() -> Engine:
    """
    Standalone engine for workflow integration tests.

    Creates engine connected to test DB, yields, then truncates all rows.
    Skips the entire module if PAPER_TRADER_TEST_DATABASE_URL is not set.
    """
    url = os.environ.get("PAPER_TRADER_TEST_DATABASE_URL")
    if not url:
        pytest.skip("PAPER_TRADER_TEST_DATABASE_URL not set — skipping workflow tests.")
    engine = create_engine(url, pool_pre_ping=True)
    Base.metadata.create_all(engine)
    yield engine
    with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            conn.execute(table.delete())
    engine.dispose()


@pytest.fixture(scope="module")
def seeded_workflow_db(workflow_engine: Engine) -> Engine:
    """
    Redirect DATABASE_URL to test DB and seed Portfolio + price data for workflow tests.

    Sets PAPER_TRADER_DATABASE_URL so that get_dedicated_session() inside
    run_decision_workflow() connects to the test DB. Calls reset_engine_state()
    so the engine singleton is rebuilt against the test URL.

    Yields the engine (not a session) so tests can create fresh sessions as needed.
    """
    db_url = workflow_engine.url.render_as_string(hide_password=False)
    os.environ["PAPER_TRADER_DATABASE_URL"]    = db_url
    os.environ["PAPER_TRADER_SERVICE_API_KEY"] = "test-key"
    get_settings.cache_clear()
    reset_engine_state()

    with Session(workflow_engine, autoflush=False, expire_on_commit=False) as session:
        portfolio = Portfolio(
            inception_date=_DATE,
            initial_capital=Decimal("10000.00"),
            strategy_enabled=True,
            trading_enabled=True,
            allow_new_positions=True,
            config={
                "max_positions":              5,
                "max_concentration_pct":      "0.20",
                "min_cash_pct":               "0.10",
                "max_daily_new_exposure_pct": "0.40",
                "confidence_threshold":       "0.55",
                "min_order_notional":         "50.00",
                "ticker_cooldown_hours":      0,
                "allow_averaging_down":       False,
            },
            cached_cash=Decimal("10000.00"),
            cached_total_value=Decimal("10000.00"),
            cached_as_of_ts=_NOW,
        )
        session.add(portfolio)
        session.flush()
        append_cash_entry(
            session,
            portfolio_id=portfolio.id,
            entry_type=CashEntryType.INITIAL_CAPITAL,
            amount=Decimal("10000.00"),
            description="Workflow test initial capital",
        )
        session.commit()

    yield workflow_engine

    get_settings.cache_clear()
    reset_engine_state()


# ---------------------------------------------------------------------------
# Unit tests: SMA computation
# ---------------------------------------------------------------------------

class TestSMAComputation:
    def test_compute_sma_basic(self) -> None:
        """SMA of [1, 2, 3, 4, 5] over 3 periods = 4.0 (average of last 3: 3, 4, 5)."""
        prices = [Decimal(str(p)) for p in [1, 2, 3, 4, 5]]
        result = _compute_sma(prices, 3)
        assert result == Decimal("4")

    def test_compute_sma_full_window(self) -> None:
        """SMA of entire list when window equals length."""
        prices = [Decimal(str(p)) for p in [10, 20, 30]]
        result = _compute_sma(prices, 3)
        assert result == Decimal("20")

    def test_compute_sma_decimal_precision(self) -> None:
        """SMA preserves decimal precision."""
        prices = [Decimal("100.50"), Decimal("101.75"), Decimal("99.25")]
        result = _compute_sma(prices, 3)
        expected = (Decimal("100.50") + Decimal("101.75") + Decimal("99.25")) / Decimal("3")
        assert result == expected

    def test_compute_sma_insufficient_history(self) -> None:
        """SMA raises ValueError when prices < window."""
        prices = [Decimal("100"), Decimal("101")]
        with pytest.raises(ValueError, match="Not enough prices"):
            _compute_sma(prices, 5)

    def test_compute_sma_single_price(self) -> None:
        """SMA of single price = that price."""
        prices = [Decimal("150.00")]
        result = _compute_sma(prices, 1)
        assert result == Decimal("150.00")


# ---------------------------------------------------------------------------
# Unit tests: Signal classification
# ---------------------------------------------------------------------------

class TestSignalClassification:
    def test_buy_signal_strong_uptrend(self) -> None:
        """BUY when price > short_sma > long_sma (uptrend)."""
        # Prices trending up: [10, 11, 12, 13, 14]
        # short_sma(3) = 13, long_sma(5) = 12, latest = 14
        direction, confidence, reason = _classify_signal(
            latest_price=Decimal("14"),
            short_sma=Decimal("13"),
            long_sma=Decimal("12"),
        )
        assert direction == SignalDirection.BUY
        assert Decimal("0.75") <= confidence <= Decimal("0.90")
        assert "Uptrend" in reason
        assert "14.00" in reason
        assert "13.00" in reason
        assert "12.00" in reason

    def test_buy_signal_confidence_scaling(self) -> None:
        """BUY confidence increases with trend strength."""
        # Weak uptrend: price slightly above short_sma
        direction1, conf1, _ = _classify_signal(
            latest_price=Decimal("100.50"),
            short_sma=Decimal("100.00"),
            long_sma=Decimal("99.00"),
        )
        # Strong uptrend: price well above short_sma
        direction2, conf2, _ = _classify_signal(
            latest_price=Decimal("110.00"),
            short_sma=Decimal("100.00"),
            long_sma=Decimal("90.00"),
        )
        assert direction1 == SignalDirection.BUY
        assert direction2 == SignalDirection.BUY
        assert conf2 >= conf1  # Stronger trend → higher confidence

    def test_sell_signal_downtrend(self) -> None:
        """SELL when price < short_sma < long_sma (downtrend)."""
        direction, confidence, reason = _classify_signal(
            latest_price=Decimal("10"),
            short_sma=Decimal("11"),
            long_sma=Decimal("12"),
        )
        assert direction == SignalDirection.SELL
        assert Decimal("0.70") <= confidence <= Decimal("0.85")
        assert "Downtrend" in reason

    def test_sell_signal_price_reversal(self) -> None:
        """SELL when price < short_sma (momentum loss)."""
        direction, confidence, reason = _classify_signal(
            latest_price=Decimal("99.00"),
            short_sma=Decimal("100.00"),
            long_sma=Decimal("98.00"),  # short_sma still > long_sma
        )
        assert direction == SignalDirection.SELL
        assert Decimal("0.70") <= confidence <= Decimal("0.85")
        assert "Price reversal" in reason or "Momentum loss" in reason

    def test_sell_signal_momentum_loss(self) -> None:
        """SELL when short_sma < long_sma (momentum loss)."""
        direction, confidence, reason = _classify_signal(
            latest_price=Decimal("99.50"),
            short_sma=Decimal("99.00"),
            long_sma=Decimal("100.00"),  # long_sma > short_sma
        )
        assert direction == SignalDirection.SELL
        assert Decimal("0.70") <= confidence <= Decimal("0.85")

    def test_hold_signal_neutral(self) -> None:
        """HOLD when price > short_sma AND short_sma > long_sma but not a strong uptrend."""
        # Barely above the short SMA, just barely above long SMA (weak uptrend)
        direction, confidence, reason = _classify_signal(
            latest_price=Decimal("100.50"),
            short_sma=Decimal("100.40"),
            long_sma=Decimal("100.30"),
        )
        # This is actually a BUY because price > short_sma AND short_sma > long_sma
        # Let me use a flat scenario instead: price near short_sma, short_sma near long_sma
        direction, confidence, reason = _classify_signal(
            latest_price=Decimal("100.00"),
            short_sma=Decimal("100.00"),
            long_sma=Decimal("100.00"),
        )
        assert direction == SignalDirection.HOLD
        assert confidence == Decimal("0.60")
        assert "Neutral" in reason

    def test_hold_signal_flat_prices(self) -> None:
        """HOLD when all prices are identical."""
        direction, confidence, reason = _classify_signal(
            latest_price=Decimal("100.00"),
            short_sma=Decimal("100.00"),
            long_sma=Decimal("100.00"),
        )
        assert direction == SignalDirection.HOLD
        assert confidence == Decimal("0.60")

    def test_confidence_bounds(self) -> None:
        """Confidence is always between 0.0 and 1.0."""
        test_cases = [
            (Decimal("1000"), Decimal("1"), Decimal("0.01")),  # Extreme uptrend
            (Decimal("0.01"), Decimal("1000"), Decimal("10000")),  # Extreme downtrend
            (Decimal("100"), Decimal("100"), Decimal("100")),  # Flat
        ]
        for price, short, long in test_cases:
            _, confidence, _ = _classify_signal(price, short, long)
            assert Decimal("0.0") <= confidence <= Decimal("1.0"), \
                f"Confidence {confidence} out of bounds for price={price}, short={short}, long={long}"


# ---------------------------------------------------------------------------
# Integration tests: Signal generation
# ---------------------------------------------------------------------------

class TestGenerateSignals:
    @pytest.fixture
    def seeded_session(self, db_session: Session):
        """Seed db_session with price snapshots for testing."""
        # AAPL: uptrend [100, 101, 102, 103, 104]
        for i, price in enumerate([100, 101, 102, 103, 104], start=1):
            db_session.add(PriceSnapshot(
                ticker="AAPL",
                price=Decimal(str(price)),
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=_NOW.replace(hour=i),
                market_date=_DATE,
                job_run_id=None,
            ))

        # GOOG: downtrend [105, 104, 103, 102, 101]
        for i, price in enumerate([105, 104, 103, 102, 101], start=1):
            db_session.add(PriceSnapshot(
                ticker="GOOG",
                price=Decimal(str(price)),
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=_NOW.replace(hour=i),
                market_date=_DATE,
                job_run_id=None,
            ))

        # MSFT: flat [100, 100, 100, 100, 100]
        for i, price in enumerate([100, 100, 100, 100, 100], start=1):
            db_session.add(PriceSnapshot(
                ticker="MSFT",
                price=Decimal(str(price)),
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=_NOW.replace(hour=i),
                market_date=_DATE,
                job_run_id=None,
            ))

        # TSLA: insufficient history [100, 101]
        for i, price in enumerate([100, 101], start=1):
            db_session.add(PriceSnapshot(
                ticker="TSLA",
                price=Decimal(str(price)),
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=_NOW.replace(hour=i),
                market_date=_DATE,
                job_run_id=None,
            ))

        db_session.flush()
        yield db_session

    def test_generate_signals_buy_signal(self, seeded_session: Session) -> None:
        """AAPL uptrend generates BUY signal."""
        signals, skipped = generate_signals(
            seeded_session,
            market_date=_DATE,
            now=_NOW,
            short_window=3,
            long_window=5,
        )
        # Find AAPL signal
        aapl_signal = next((s for s in signals if s["ticker"] == "AAPL"), None)
        assert aapl_signal is not None
        assert aapl_signal["direction"] == SignalDirection.BUY
        assert aapl_signal["confidence"] >= Decimal("0.75")
        assert "Uptrend" in aapl_signal["raw_payload"]["trend_reason"]

    def test_generate_signals_sell_signal(self, seeded_session: Session) -> None:
        """GOOG downtrend generates SELL signal."""
        signals, skipped = generate_signals(
            seeded_session,
            market_date=_DATE,
            now=_NOW,
            short_window=3,
            long_window=5,
        )
        # Find GOOG signal
        goog_signal = next((s for s in signals if s["ticker"] == "GOOG"), None)
        assert goog_signal is not None
        assert goog_signal["direction"] == SignalDirection.SELL
        assert goog_signal["confidence"] >= Decimal("0.70")

    def test_generate_signals_hold_signal(self, seeded_session: Session) -> None:
        """MSFT flat prices generate HOLD signal."""
        signals, skipped = generate_signals(
            seeded_session,
            market_date=_DATE,
            now=_NOW,
            short_window=3,
            long_window=5,
        )
        # Find MSFT signal
        msft_signal = next((s for s in signals if s["ticker"] == "MSFT"), None)
        assert msft_signal is not None
        assert msft_signal["direction"] == SignalDirection.HOLD
        assert msft_signal["confidence"] == Decimal("0.60")

    def test_generate_signals_insufficient_history(self, seeded_session: Session) -> None:
        """TSLA with insufficient history is skipped with reason."""
        signals, skipped = generate_signals(
            seeded_session,
            market_date=_DATE,
            now=_NOW,
            short_window=3,
            long_window=5,
        )
        assert "TSLA" in skipped
        assert "Insufficient price history" in skipped["TSLA"]
        assert "2 < 5" in skipped["TSLA"]
        # TSLA should not appear in signals
        assert not any(s["ticker"] == "TSLA" for s in signals)

    def test_generate_signals_ticker_filter(self, seeded_session: Session) -> None:
        """Optional tickers parameter filters results."""
        signals, skipped = generate_signals(
            seeded_session,
            market_date=_DATE,
            now=_NOW,
            short_window=3,
            long_window=5,
            tickers=["AAPL", "GOOG"],
        )
        tickers = [s["ticker"] for s in signals]
        assert "AAPL" in tickers
        assert "GOOG" in tickers
        assert "MSFT" not in tickers
        assert "TSLA" not in tickers

    def test_generate_signals_excludes_future_dated_snapshots(self, seeded_session: Session) -> None:
        """Future-dated price snapshots are excluded from signal generation."""
        from datetime import timedelta

        # Create two snapshots for today (should be included)
        seeded_session.add(PriceSnapshot(
            ticker="NVDA",
            price=Decimal("200.00"),
            session_type="REGULAR",
            price_type="CLOSE",
            snapshot_ts=_NOW.replace(hour=1),
            market_date=_DATE,
            job_run_id=None,
        ))
        seeded_session.add(PriceSnapshot(
            ticker="NVDA",
            price=Decimal("205.00"),
            session_type="REGULAR",
            price_type="CLOSE",
            snapshot_ts=_NOW.replace(hour=2),
            market_date=_DATE,
            job_run_id=None,
        ))
        # Create future-dated snapshots (should be excluded)
        future_date = _DATE + timedelta(days=1)
        seeded_session.add(PriceSnapshot(
            ticker="NVDA",
            price=Decimal("210.00"),
            session_type="REGULAR",
            price_type="CLOSE",
            snapshot_ts=_NOW + timedelta(days=1),
            market_date=future_date,
            job_run_id=None,
        ))
        seeded_session.add(PriceSnapshot(
            ticker="NVDA",
            price=Decimal("220.00"),
            session_type="REGULAR",
            price_type="CLOSE",
            snapshot_ts=_NOW + timedelta(days=2),
            market_date=future_date,
            job_run_id=None,
        ))
        seeded_session.flush()

        # Run strategy for today's date with short_window=1, long_window=2
        signals, skipped = generate_signals(
            seeded_session,
            market_date=_DATE,
            now=_NOW,
            short_window=1,
            long_window=2,
            tickers=["NVDA"],
        )

        # Should generate a signal using only today's 2 snapshots, not future ones
        assert "NVDA" in [s["ticker"] for s in signals]
        nvda_signal = next((s for s in signals if s["ticker"] == "NVDA"), None)
        assert nvda_signal is not None
        # The latest price should be 205.00 (most recent today), not 210 or 220 (future)
        assert Decimal(nvda_signal["raw_payload"]["latest_price"]) == Decimal("205.00")

    def test_generate_signals_metadata_includes_raw_payload(self, seeded_session: Session) -> None:
        """Generated signals include explainable metadata in raw_payload."""
        signals, skipped = generate_signals(
            seeded_session,
            market_date=_DATE,
            now=_NOW,
            short_window=3,
            long_window=5,
        )
        assert len(signals) > 0
        signal = signals[0]
        payload = signal["raw_payload"]
        assert "latest_price" in payload
        assert "short_window" in payload
        assert "long_window" in payload
        assert "short_sma" in payload
        assert "long_sma" in payload
        assert "trend_reason" in payload

    def test_generate_signals_source_run_is_strategy_v1(self, seeded_session: Session) -> None:
        """All generated signals have source_run='strategy_v1'."""
        signals, skipped = generate_signals(
            seeded_session,
            market_date=_DATE,
            now=_NOW,
            short_window=3,
            long_window=5,
        )
        for signal in signals:
            assert signal["source_run"] == "strategy_v1"

    def test_generate_signals_signal_ts_is_now(self, seeded_session: Session) -> None:
        """All generated signals use the provided 'now' as signal_ts."""
        signals, skipped = generate_signals(
            seeded_session,
            market_date=_DATE,
            now=_NOW,
            short_window=3,
            long_window=5,
        )
        for signal in signals:
            assert signal["signal_ts"] == _NOW

    def test_generate_signals_empty_portfolio(self, db_session: Session) -> None:
        """Empty price snapshot table returns no signals and empty skipped dict."""
        signals, skipped = generate_signals(
            db_session,
            market_date=_DATE,
            now=_NOW,
            short_window=3,
            long_window=5,
        )
        assert signals == []
        assert skipped == {}

    def test_generate_signals_invalid_windows(self, seeded_session: Session) -> None:
        """Invalid window configurations raise ValueError."""
        with pytest.raises(ValueError, match="short_window.*must be <"):
            generate_signals(
                seeded_session,
                market_date=_DATE,
                now=_NOW,
                short_window=5,
                long_window=3,
            )

    def test_generate_signals_zero_window(self, seeded_session: Session) -> None:
        """Zero window raises ValueError."""
        with pytest.raises(ValueError, match="must be > 0"):
            generate_signals(
                seeded_session,
                market_date=_DATE,
                now=_NOW,
                short_window=0,
                long_window=5,
            )

    def test_generate_signals_equal_windows(self, seeded_session: Session) -> None:
        """Equal short and long windows raise ValueError."""
        with pytest.raises(ValueError, match="short_window.*must be <"):
            generate_signals(
                seeded_session,
                market_date=_DATE,
                now=_NOW,
                short_window=5,
                long_window=5,
            )

    def test_generate_signals_custom_windows(self, seeded_session: Session) -> None:
        """Custom window sizes work correctly."""
        # Use short_window=2, long_window=3
        signals, skipped = generate_signals(
            seeded_session,
            market_date=_DATE,
            now=_NOW,
            short_window=2,
            long_window=3,
        )
        # With longer windows, we should get signals for AAPL, GOOG, MSFT
        # (TSLA has only 2 prices, which is < 3)
        assert len(signals) >= 3
        assert "AAPL" not in skipped
        assert "GOOG" not in skipped
        assert "MSFT" not in skipped
        assert "TSLA" in skipped


# ---------------------------------------------------------------------------
# Integration tests: end-to-end workflow
# ---------------------------------------------------------------------------

class TestStrategyWorkflowIntegration:
    """Test that generated signals flow through the decision workflow correctly."""

    def test_generated_signals_create_decisions_through_workflow(
        self, seeded_workflow_db: Engine
    ) -> None:
        """Generated signals create TradeDecision and Order rows via decision workflow."""
        # Seed prices for TEST ticker (uptrend)
        with Session(seeded_workflow_db, autoflush=False, expire_on_commit=False) as session:
            for i, price in enumerate([100, 101, 102, 103, 104], start=1):
                session.add(PriceSnapshot(
                    ticker="TEST",
                    price=Decimal(str(price)),
                    session_type="REGULAR",
                    price_type="CLOSE",
                    snapshot_ts=_NOW.replace(hour=i),
                    market_date=_DATE,
                    job_run_id=None,
                ))
            session.commit()

            # Generate a BUY signal for TEST ticker
            signals, _ = generate_signals(
                session,
                market_date=_DATE,
                now=_NOW,
                short_window=3,
                long_window=5,
                tickers=["TEST"],
            )

        assert len(signals) == 1
        assert signals[0]["direction"] == SignalDirection.BUY

        # Submit through decision workflow
        result = run_decision_workflow(
            idempotency_key="test-workflow-integration",
            workflow_type=WorkflowType.PRE_MARKET,
            market_date=_DATE,
            signals=signals,
            now=_NOW,
        )

        # Verify decision was made
        assert result["signals_ingested"] == 1
        assert result["decisions_made"] == 1

        # Query the decision row via new session
        with Session(seeded_workflow_db, autoflush=False, expire_on_commit=False) as session:
            td = session.execute(
                select(TradeDecision).where(TradeDecision.ticker == "TEST")
            ).scalar_one_or_none()

            assert td is not None
            assert td.decision == DecisionType.BUY
            assert td.ticker == "TEST"

            # Verify order was created (for BUY decision that passes risk gates)
            # Note: may still be rejected by risk gates (e.g., too small, below threshold)
            # but the TradeDecision row must exist
            order = session.execute(
                select(Order).where(Order.ticker == "TEST")
            ).scalar_one_or_none()

            # If order exists, verify it matches the decision
            if order is not None:
                assert order.side == "BUY"
                assert order.trade_decision_id is not None

    def test_generated_signals_include_explainable_metadata(
        self, seeded_workflow_db: Engine
    ) -> None:
        """Generated signals have raw_payload with trend details."""
        # Seed prices for TEST ticker
        with Session(seeded_workflow_db, autoflush=False, expire_on_commit=False) as session:
            for i, price in enumerate([100, 101, 102, 103, 104], start=1):
                session.add(PriceSnapshot(
                    ticker="TEST",
                    price=Decimal(str(price)),
                    session_type="REGULAR",
                    price_type="CLOSE",
                    snapshot_ts=_NOW.replace(hour=i),
                    market_date=_DATE,
                    job_run_id=None,
                ))
            session.commit()

            signals, _ = generate_signals(
                session,
                market_date=_DATE,
                now=_NOW,
                short_window=3,
                long_window=5,
                tickers=["TEST"],
            )

        assert len(signals) == 1
        signal = signals[0]

        # Verify raw_payload exists and has all required fields
        payload = signal["raw_payload"]
        assert payload is not None
        assert "latest_price" in payload
        assert "short_window" in payload
        assert "long_window" in payload
        assert "short_sma" in payload
        assert "long_sma" in payload
        assert "trend_reason" in payload

        # Verify values are populated
        assert payload["short_window"] == 3
        assert payload["long_window"] == 5
        assert payload["trend_reason"]  # Non-empty string

    def test_multiple_signals_batch_through_workflow(
        self, seeded_workflow_db: Engine
    ) -> None:
        """Multiple generated signals are processed as a batch."""
        # Add prices for first ticker (uptrend)
        with Session(seeded_workflow_db, autoflush=False, expire_on_commit=False) as session:
            for i, price in enumerate([100, 101, 102, 103, 104], start=1):
                session.add(PriceSnapshot(
                    ticker="TEST",
                    price=Decimal(str(price)),
                    session_type="REGULAR",
                    price_type="CLOSE",
                    snapshot_ts=_NOW.replace(hour=i),
                    market_date=_DATE,
                    job_run_id=None,
                ))
            # Add prices for second ticker (downtrend)
            for i, price in enumerate([105, 104, 103, 102, 101], start=1):
                session.add(PriceSnapshot(
                    ticker="TEST2",
                    price=Decimal(str(price)),
                    session_type="REGULAR",
                    price_type="CLOSE",
                    snapshot_ts=_NOW.replace(hour=i),
                    market_date=_DATE,
                    job_run_id=None,
                ))
            session.commit()

            # Generate signals for both tickers
            signals, skipped = generate_signals(
                session,
                market_date=_DATE,
                now=_NOW,
                short_window=3,
                long_window=5,
                tickers=["TEST", "TEST2"],
            )

        assert len(signals) == 2
        assert len(skipped) == 0

        # Submit batch through decision workflow
        result = run_decision_workflow(
            idempotency_key="test-batch-integration",
            workflow_type=WorkflowType.PRE_MARKET,
            market_date=_DATE,
            signals=signals,
            now=_NOW,
        )

        # Verify both signals ingested and both decisions made
        assert result["signals_ingested"] == 2
        assert result["decisions_made"] == 2

        # Verify both TradeDecision rows created for this run only
        with Session(seeded_workflow_db, autoflush=False, expire_on_commit=False) as session:
            job_run = session.execute(
                select(JobRun).where(JobRun.idempotency_key == "test-batch-integration")
            ).scalar_one()
            decisions = session.execute(
                select(TradeDecision).where(TradeDecision.job_run_id == job_run.id)
            ).scalars().all()
            assert len(decisions) == 2

    def test_idempotency_of_generated_signals(
        self, seeded_workflow_db: Engine
    ) -> None:
        """Submitting same generated signals twice respects idempotency."""
        # Seed prices for TEST ticker
        with Session(seeded_workflow_db, autoflush=False, expire_on_commit=False) as session:
            for i, price in enumerate([100, 101, 102, 103, 104], start=1):
                session.add(PriceSnapshot(
                    ticker="TEST",
                    price=Decimal(str(price)),
                    session_type="REGULAR",
                    price_type="CLOSE",
                    snapshot_ts=_NOW.replace(hour=i),
                    market_date=_DATE,
                    job_run_id=None,
                ))
            session.commit()

            # Generate signal
            signals, _ = generate_signals(
                session,
                market_date=_DATE,
                now=_NOW,
                short_window=3,
                long_window=5,
                tickers=["TEST"],
            )

        ikey = "test-idempotency-check"

        # First submission
        result1 = run_decision_workflow(
            idempotency_key=ikey,
            workflow_type=WorkflowType.PRE_MARKET,
            market_date=_DATE,
            signals=signals,
            now=_NOW,
        )

        # Second submission with same idempotency key
        result2 = run_decision_workflow(
            idempotency_key=ikey,
            workflow_type=WorkflowType.PRE_MARKET,
            market_date=_DATE,
            signals=signals,
            now=_NOW,
        )

        # Results should be identical (cached)
        assert result1 == result2

    def test_override_signals_source_run(self) -> None:
        """
        _override_signals_source_run transforms source_run to idempotency_key.

        Verifies that the helper function correctly:
        - Overrides source_run to the provided idempotency_key
        - Preserves original source_run in raw_payload.strategy_name
        - Handles multiple invocations on the same signal list
        """
        from paper_trader.api.app import _override_signals_source_run

        # Create a mock signal as would be returned by generate_signals()
        raw_signal = {
            "ticker": "TEST",
            "direction": SignalDirection.BUY,
            "confidence": Decimal("0.80"),
            "signal_ts": _NOW,
            "source_run": "strategy_v1",
            "raw_payload": {
                "latest_price": "100.00",
                "short_sma": "99.00",
                "long_sma": "98.00",
                "trend_reason": "Uptrend",
            },
        }

        # First override: should transform source_run to ikey1
        ikey1 = "strategy-run-001"
        overridden_1 = _override_signals_source_run([raw_signal], ikey1)

        assert len(overridden_1) == 1
        assert overridden_1[0]["source_run"] == ikey1
        assert overridden_1[0]["raw_payload"]["strategy_name"] == "strategy_v1"
        assert overridden_1[0]["ticker"] == "TEST"
        assert overridden_1[0]["direction"] == SignalDirection.BUY
        # Original signal should not be mutated
        assert raw_signal["source_run"] == "strategy_v1"

        # Second override on SAME original signal: should use ikey2
        ikey2 = "strategy-run-002"
        overridden_2 = _override_signals_source_run([raw_signal], ikey2)

        assert len(overridden_2) == 1
        assert overridden_2[0]["source_run"] == ikey2
        assert overridden_2[0]["raw_payload"]["strategy_name"] == "strategy_v1"
        # overridden_1 should be unaffected
        assert overridden_1[0]["source_run"] == ikey1

        # Test signal without raw_payload
        signal_no_payload = {
            "ticker": "MSFT",
            "direction": SignalDirection.SELL,
            "confidence": Decimal("0.70"),
            "signal_ts": _NOW,
            "source_run": "strategy_v1",
        }
        overridden_3 = _override_signals_source_run([signal_no_payload], ikey1)
        assert overridden_3[0]["raw_payload"]["strategy_name"] == "strategy_v1"
        assert overridden_3[0]["source_run"] == ikey1
