"""
tests/test_market_screener.py — Unit tests for market_screener module.

Tests focus on:
    - Ticker list normalization
    - Universe loading from CSV
    - Candidate scoring (momentum, volatility, relative strength)
    - Insufficient price history handling
    - Deterministic sorting and ranking
    - Reason code population
    - Benchmark relative strength calculation when benchmark data exists
    - Graceful handling when benchmark data is missing
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from paper_trader.db.models import Base, BenchmarkPrice, PriceSnapshot
from paper_trader.engine.market_screener import scan_market
from paper_trader.engine.universe import get_sp500_universe, normalize_ticker_list


class TestNormalizeTickerList:
    """normalize_ticker_list() normalizes and deduplicates tickers."""

    def test_normalize_list_of_strings(self):
        """List of ticker strings normalizes to uppercase."""
        result = normalize_ticker_list(["aapl", "msft", "googl"])
        assert result == ["AAPL", "MSFT", "GOOGL"]

    def test_normalize_single_string(self):
        """Single ticker string becomes list."""
        result = normalize_ticker_list("tsla")
        assert result == ["TSLA"]

    def test_normalize_removes_whitespace(self):
        """Whitespace is stripped."""
        result = normalize_ticker_list(["  aapl  ", "msft "])
        assert result == ["AAPL", "MSFT"]

    def test_normalize_deduplicates(self):
        """Duplicates are removed."""
        result = normalize_ticker_list(["aapl", "AAPL", "aapl"])
        assert result == ["AAPL"]

    def test_normalize_none_returns_empty(self):
        """None input returns empty list."""
        result = normalize_ticker_list(None)
        assert result == []

    def test_normalize_empty_list(self):
        """Empty list returns empty list."""
        result = normalize_ticker_list([])
        assert result == []

    def test_normalize_filters_non_strings(self):
        """Non-string items are skipped."""
        result = normalize_ticker_list(["aapl", 123, "msft", None])
        assert result == ["AAPL", "MSFT"]

    def test_normalize_filters_empty_strings(self):
        """Empty strings and whitespace-only strings are skipped."""
        result = normalize_ticker_list(["aapl", "", "msft", "   "])
        assert result == ["AAPL", "MSFT"]


class TestGetSP500Universe:
    """get_sp500_universe() loads S&P 500 tickers from CSV."""

    def test_load_universe_returns_list(self):
        """Universe returns list of tickers."""
        universe = get_sp500_universe()
        assert isinstance(universe, list)
        assert len(universe) > 0
        assert all(isinstance(t, str) for t in universe)

    def test_all_tickers_uppercase(self):
        """All tickers are uppercase."""
        universe = get_sp500_universe()
        assert all(t == t.upper() for t in universe)

    def test_universe_is_deterministic(self):
        """Multiple calls return the same list."""
        u1 = get_sp500_universe()
        u2 = get_sp500_universe()
        assert u1 == u2


@pytest.fixture
def scanner_session():
    """Create an in-memory SQLite test database with only PriceSnapshot and BenchmarkPrice tables."""
    engine = create_engine("sqlite:///:memory:")
    try:
        # Create only the tables needed for screener tests (avoid JSONB in Portfolio)
        PriceSnapshot.__table__.create(engine, checkfirst=True)
        BenchmarkPrice.__table__.create(engine, checkfirst=True)
        session = Session(bind=engine)
        yield session
    finally:
        session.close()
        engine.dispose()


class TestScanMarket:
    """scan_market() generates ranked candidates from price snapshots."""

    def test_scan_with_no_prices_returns_empty(self, scanner_session):
        """Empty database returns no candidates, skipped tickers for requested tickers."""
        candidates, skipped, scan_date = scan_market(
            session=scanner_session,
            tickers=["AAPL", "MSFT"],
            lookback_days=20,
            min_price_points=5,
        )
        assert len(candidates) == 0
        # When tickers are explicitly requested but have no data, they show up in skipped
        assert len(skipped) == 2
        assert scan_date is None

    def test_scan_with_insufficient_prices_skips_ticker(self, scanner_session):
        """Ticker with fewer than min_price_points is skipped."""
        base_date = date(2026, 5, 26)
        for i in range(3):  # Only 3 prices, min is 5
            ps = PriceSnapshot(
                ticker="AAPL",
                price=Decimal("150.00"),
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=datetime(2026, 5, 26 - i, 16, 0, 0, tzinfo=timezone.utc),
                market_date=base_date - timedelta(days=i),
            )
            scanner_session.add(ps)
        scanner_session.commit()

        candidates, skipped, scan_date = scan_market(
            session=scanner_session,
            tickers=["AAPL"],
            min_price_points=5,
        )
        assert len(candidates) == 0
        assert len(skipped) == 1
        assert skipped[0].ticker == "AAPL"
        assert skipped[0].reason == "INSUFFICIENT_PRICE_HISTORY"
        assert skipped[0].price_count == 3

    def test_scan_ranks_higher_momentum_first(self, scanner_session):
        """Candidate with higher score ranks higher."""
        base_date = date(2026, 5, 26)

        # AAPL: 20-day uptrend (oldest=150, newest=160)
        # Generate in chronological order (oldest to newest)
        for i in range(20):
            price = Decimal("150.00") + Decimal(i) * Decimal("0.50")
            ps = PriceSnapshot(
                ticker="AAPL",
                price=price,
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=datetime(2026, 5, 7 + i, 16, 0, 0, tzinfo=timezone.utc),
                market_date=base_date - timedelta(days=19 - i),
            )
            scanner_session.add(ps)

        # MSFT: flat prices (160 throughout)
        for i in range(20):
            ps = PriceSnapshot(
                ticker="MSFT",
                price=Decimal("160.00"),
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=datetime(2026, 5, 7 + i, 16, 0, 0, tzinfo=timezone.utc),
                market_date=base_date - timedelta(days=19 - i),
            )
            scanner_session.add(ps)

        scanner_session.commit()

        candidates, skipped, scan_date = scan_market(
            session=scanner_session,
            tickers=["AAPL", "MSFT"],
            lookback_days=20,
            min_price_points=5,
            top_n=25,
        )

        assert len(candidates) == 2
        assert candidates[0].ticker == "AAPL"  # Higher momentum
        assert candidates[0].rank == 1
        assert candidates[1].ticker == "MSFT"
        assert candidates[1].rank == 2

    def test_scan_deterministic_tie_breaking(self, scanner_session):
        """Candidates with same score are sorted by ticker ascending."""
        base_date = date(2026, 5, 26)

        # Create two tickers with identical prices (same score)
        for ticker in ["ZZZZ", "AAAA"]:
            for i in range(20):
                ps = PriceSnapshot(
                    ticker=ticker,
                    price=Decimal("100.00"),
                    session_type="REGULAR",
                    price_type="CLOSE",
                    snapshot_ts=datetime(2026, 5, 7 + i, 16, 0, 0, tzinfo=timezone.utc),
                    market_date=base_date - timedelta(days=19 - i),
                )
                scanner_session.add(ps)
        scanner_session.commit()

        candidates, skipped, scan_date = scan_market(
            session=scanner_session,
            tickers=["ZZZZ", "AAAA"],
            lookback_days=20,
            min_price_points=5,
            top_n=25,
        )

        # AAAA should come first (alphabetical ascending)
        if len(candidates) >= 2:
            assert candidates[0].ticker == "AAAA"
            assert candidates[1].ticker == "ZZZZ"

    def test_scan_populates_reason_codes(self, scanner_session):
        """Reason codes are populated for positive and negative indicators."""
        base_date = date(2026, 5, 26)

        # Create ticker with uptrend: oldest price=100, newest price=120 (20% gain)
        for i in range(20):
            price = Decimal("100.00") + Decimal(i) * Decimal("1.00")  # 100 -> 120
            # Insert in chronological order: oldest first (May 7) to newest (May 26)
            ps = PriceSnapshot(
                ticker="AAPL",
                price=price,
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=datetime(2026, 5, 7 + i, 16, 0, 0, tzinfo=timezone.utc),
                market_date=base_date - timedelta(days=19 - i),
            )
            scanner_session.add(ps)
        scanner_session.commit()

        candidates, skipped, scan_date = scan_market(
            session=scanner_session,
            tickers=["AAPL"],
            lookback_days=20,
            min_price_points=5,
        )

        assert len(candidates) == 1
        assert "POSITIVE_20D_MOMENTUM" in candidates[0].reason_codes

    def test_scan_with_benchmark_calculates_relative_strength(self, scanner_session):
        """Relative strength is calculated when benchmark data exists."""
        base_date = date(2026, 5, 26)

        # AAPL: 20% uptrend (oldest=100, newest=120)
        for i in range(20):
            price = Decimal("100.00") + Decimal(i) * Decimal("1.00")
            ps = PriceSnapshot(
                ticker="AAPL",
                price=price,
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=datetime(2026, 5, 7 + i, 16, 0, 0, tzinfo=timezone.utc),
                market_date=base_date - timedelta(days=19 - i),
            )
            scanner_session.add(ps)

        # SPY: 5% uptrend (oldest=300, newest=315) - less than AAPL
        for i in range(20):
            price = Decimal("300.00") + Decimal(i) * Decimal("0.75")
            bp = BenchmarkPrice(
                ticker="SPY",
                price=price,
                session_type="REGULAR",
                snapshot_ts=datetime(2026, 5, 7 + i, 16, 0, 0, tzinfo=timezone.utc),
                market_date=base_date - timedelta(days=19 - i),
            )
            scanner_session.add(bp)

        scanner_session.commit()

        candidates, skipped, scan_date = scan_market(
            session=scanner_session,
            tickers=["AAPL"],
            benchmark_ticker="SPY",
            lookback_days=20,
            min_price_points=5,
        )

        assert len(candidates) == 1
        assert candidates[0].relative_strength_vs_spy_20d is not None
        assert Decimal(candidates[0].relative_strength_vs_spy_20d) > Decimal("0")
        assert "OUTPERFORMING_SPY" in candidates[0].reason_codes

    def test_scan_without_benchmark_marks_missing(self, scanner_session):
        """Missing benchmark data is noted in reason codes."""
        base_date = date(2026, 5, 26)

        # Create ticker but no SPY benchmark
        for i in range(20):
            ps = PriceSnapshot(
                ticker="AAPL",
                price=Decimal("150.00"),
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=datetime(2026, 5, 26 - i, 16, 0, 0, tzinfo=timezone.utc),
                market_date=base_date - timedelta(days=i),
            )
            scanner_session.add(ps)
        scanner_session.commit()

        candidates, skipped, scan_date = scan_market(
            session=scanner_session,
            tickers=["AAPL"],
            benchmark_ticker="SPY",
            lookback_days=20,
            min_price_points=5,
        )

        assert len(candidates) == 1
        assert candidates[0].relative_strength_vs_spy_20d is None
        assert "BENCHMARK_MISSING" in candidates[0].reason_codes

    def test_scan_top_n_limit(self, scanner_session):
        """Top N limit is respected."""
        base_date = date(2026, 5, 26)

        # Create 5 tickers with different uptrends
        for ticker_idx in range(5):
            ticker = chr(65 + ticker_idx)  # A, B, C, D, E
            for i in range(20):
                price = Decimal("100.00") + Decimal(ticker_idx * 10) + Decimal(i) * Decimal("0.50")
                ps = PriceSnapshot(
                    ticker=ticker,
                    price=price,
                    session_type="REGULAR",
                    price_type="CLOSE",
                    snapshot_ts=datetime(2026, 5, 7 + i, 16, 0, 0, tzinfo=timezone.utc),
                    market_date=base_date - timedelta(days=19 - i),
                )
                scanner_session.add(ps)
        scanner_session.commit()

        candidates, skipped, scan_date = scan_market(
            session=scanner_session,
            tickers=["A", "B", "C", "D", "E"],
            lookback_days=20,
            min_price_points=5,
            top_n=3,
        )

        assert len(candidates) == 3
        assert all(c.rank <= 3 for c in candidates)

    def test_scan_date_from_latest_market_date(self, scanner_session):
        """Scan date is derived from latest market_date in data."""
        base_date = date(2026, 5, 26)

        for i in range(20):
            ps = PriceSnapshot(
                ticker="AAPL",
                price=Decimal("150.00"),
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=datetime(2026, 5, 7 + i, 16, 0, 0, tzinfo=timezone.utc),
                market_date=base_date - timedelta(days=19 - i),
            )
            scanner_session.add(ps)
        scanner_session.commit()

        candidates, skipped, scan_date = scan_market(
            session=scanner_session,
            tickers=["AAPL"],
            lookback_days=20,
            min_price_points=5,
        )

        assert scan_date == base_date
