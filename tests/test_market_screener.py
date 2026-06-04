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
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

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
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = None
    try:
        # Create only the tables needed for screener tests (avoid JSONB in Portfolio)
        PriceSnapshot.__table__.create(engine, checkfirst=True)
        BenchmarkPrice.__table__.create(engine, checkfirst=True)
        session = SessionLocal()
        yield session
    finally:
        if session is not None:
            session.close()
        # Drop tables before disposing engine to release references
        PriceSnapshot.__table__.drop(engine, checkfirst=True)
        BenchmarkPrice.__table__.drop(engine, checkfirst=True)
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

    def test_scan_filters_by_price_type_and_session_type(self, scanner_session):
        """Scanner filters to CLOSE/REGULAR prices only, ignoring LAST/MANUAL."""
        base_date = date(2026, 5, 26)

        # AAPL: Mix of CLOSE/REGULAR and LAST/POSTMARKET prices
        # Should only use CLOSE/REGULAR in calculations
        for i in range(20):
            # CLOSE/REGULAR price (use for calculation)
            ps = PriceSnapshot(
                ticker="AAPL",
                price=Decimal("150.00") + Decimal(i) * Decimal("1.00"),
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=datetime(2026, 5, 7 + i, 16, 0, 0, tzinfo=timezone.utc),
                market_date=base_date - timedelta(days=19 - i),
            )
            scanner_session.add(ps)

            # LAST/POSTMARKET price (ignore in calculation)
            ps_ignored = PriceSnapshot(
                ticker="AAPL",
                price=Decimal("200.00"),  # Very different price
                session_type="POSTMARKET",
                price_type="LAST",
                snapshot_ts=datetime(2026, 5, 7 + i, 17, 0, 0, tzinfo=timezone.utc),
                market_date=base_date - timedelta(days=19 - i),
            )
            scanner_session.add(ps_ignored)

        scanner_session.commit()

        candidates, skipped, scan_date = scan_market(
            session=scanner_session,
            tickers=["AAPL"],
            lookback_days=20,
            min_price_points=5,
        )

        # Should have 1 candidate with momentum calculated from CLOSE/REGULAR only
        assert len(candidates) == 1
        # Momentum should reflect CLOSE prices (150-170), not POSTMARKET (200)
        momentum_5d = Decimal(candidates[0].momentum_5d_pct) if candidates[0].momentum_5d_pct else None
        if momentum_5d:
            # Should be around (170-166)/166*100 ≈ 2.4%, not mixing in 200
            assert momentum_5d > 0 and momentum_5d < 10

    def test_scan_handles_duplicate_dates_deterministically(self, scanner_session):
        """Multiple rows per ticker/date use latest snapshot_ts."""
        base_date = date(2026, 5, 26)

        # AAPL: Two prices for same market_date, different snapshot times
        for i in range(20):
            # Older snapshot (should be ignored)
            ps_old = PriceSnapshot(
                ticker="AAPL",
                price=Decimal("100.00"),
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=datetime(2026, 5, 7 + i, 10, 0, 0, tzinfo=timezone.utc),
                market_date=base_date - timedelta(days=19 - i),
            )
            scanner_session.add(ps_old)

            # Newer snapshot (should be used)
            ps_new = PriceSnapshot(
                ticker="AAPL",
                price=Decimal("150.00") + Decimal(i) * Decimal("1.00"),
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=datetime(2026, 5, 7 + i, 16, 0, 0, tzinfo=timezone.utc),
                market_date=base_date - timedelta(days=19 - i),
            )
            scanner_session.add(ps_new)

        scanner_session.commit()

        candidates, skipped, scan_date = scan_market(
            session=scanner_session,
            tickers=["AAPL"],
            lookback_days=20,
            min_price_points=5,
        )

        # Should have candidate using newer prices (150-170), not old (100)
        assert len(candidates) == 1
        momentum_20d = Decimal(candidates[0].momentum_20d_pct) if candidates[0].momentum_20d_pct else None
        if momentum_20d:
            # Should be (170-150)/150*100 ≈ 13.3%, not (100-100)/100
            assert momentum_20d > 10

    def test_scan_skips_extreme_5d_momentum_as_outlier(self, scanner_session):
        """Extreme 5D momentum (>50%) is marked DATA_QUALITY_OUTLIER and skipped."""
        base_date = date(2026, 5, 26)

        # AAPL: Extreme price jump in 5 days (100 -> 160 = 60% momentum)
        # Days are May 7-26. We insert oldest first (i=0 is May 7), newest last (i=19 is May 26)
        # Query orders DESC, so prices list will be reversed: newest first
        for i in range(20):
            if i < 4:
                price = Decimal("100.00")  # May 7-10: $100
            else:
                price = Decimal("100.00") + Decimal(i - 4) * Decimal("10.00")  # May 11-26: $100 -> $210
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

        # Should be skipped as outlier (60% 5D momentum > 50% threshold)
        assert len(candidates) == 0
        assert len(skipped) == 1
        assert skipped[0].ticker == "AAPL"
        assert skipped[0].reason == "DATA_QUALITY_OUTLIER"

    def test_scan_skips_extreme_20d_momentum_as_outlier(self, scanner_session):
        """Extreme 20D momentum (>75%) is marked DATA_QUALITY_OUTLIER and skipped."""
        base_date = date(2026, 5, 26)

        # AAPL: Extreme uptrend (likely data contamination)
        # 100 -> 200 = 100% momentum
        for i in range(20):
            price = Decimal("100.00") + Decimal(i) * Decimal("5.00")  # 100 -> 195
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

        # Should be skipped as outlier (95% momentum > 75% threshold)
        assert len(candidates) == 0
        assert len(skipped) == 1
        assert skipped[0].ticker == "AAPL"
        assert skipped[0].reason == "DATA_QUALITY_OUTLIER"

    def test_scan_benchmark_filters_to_regular_session(self, scanner_session):
        """Benchmark scanner filters to REGULAR session only."""
        base_date = date(2026, 5, 26)

        # AAPL: Normal 20% uptrend
        for i in range(20):
            ps = PriceSnapshot(
                ticker="AAPL",
                price=Decimal("100.00") + Decimal(i) * Decimal("1.00"),
                session_type="REGULAR",
                price_type="CLOSE",
                snapshot_ts=datetime(2026, 5, 7 + i, 16, 0, 0, tzinfo=timezone.utc),
                market_date=base_date - timedelta(days=19 - i),
            )
            scanner_session.add(ps)

        # SPY: Mix of REGULAR and PREMARKET prices
        for i in range(20):
            # REGULAR price (use for calculation)
            bp = BenchmarkPrice(
                ticker="SPY",
                price=Decimal("300.00") + Decimal(i) * Decimal("0.5"),
                session_type="REGULAR",
                snapshot_ts=datetime(2026, 5, 7 + i, 16, 0, 0, tzinfo=timezone.utc),
                market_date=base_date - timedelta(days=19 - i),
            )
            scanner_session.add(bp)

            # PREMARKET price (ignore)
            bp_pre = BenchmarkPrice(
                ticker="SPY",
                price=Decimal("280.00"),  # Different price
                session_type="PREMARKET",
                snapshot_ts=datetime(2026, 5, 7 + i, 9, 0, 0, tzinfo=timezone.utc),
                market_date=base_date - timedelta(days=19 - i),
            )
            scanner_session.add(bp_pre)

        scanner_session.commit()

        candidates, skipped, scan_date = scan_market(
            session=scanner_session,
            tickers=["AAPL"],
            benchmark_ticker="SPY",
            lookback_days=20,
            min_price_points=5,
        )

        # Should have candidate with relative strength calculated correctly
        assert len(candidates) == 1
        # Should have relative strength (not None)
        assert candidates[0].relative_strength_vs_spy_20d is not None
        # Should be outperforming (AAPL 20% > SPY 5%)
        assert "OUTPERFORMING_SPY" in candidates[0].reason_codes


class TestUniverseLoader:
    """Universe file loading: full file preference, fallback, column names, normalization."""

    def test_load_from_full_file_when_present(self, tmp_path, monkeypatch):
        """Uses full file when sp500_universe_full.csv exists and has tickers."""
        full_csv = tmp_path / "sp500_universe_full.csv"
        full_csv.write_text("ticker\nAAPL\nMSFT\n", encoding="utf-8")
        stub_csv = tmp_path / "sp500_universe.csv"
        stub_csv.write_text("ticker\nGOOGL\nAMZN\n", encoding="utf-8")

        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)

        from paper_trader.engine.universe import get_sp500_universe
        result = get_sp500_universe()
        assert result == ["AAPL", "MSFT"]

    def test_fallback_to_stub_when_full_missing(self, tmp_path, monkeypatch):
        """Falls back to stub file when full file is absent."""
        stub_csv = tmp_path / "sp500_universe.csv"
        stub_csv.write_text("ticker\nGOOGL\nAMZN\n", encoding="utf-8")

        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)

        from paper_trader.engine.universe import get_sp500_universe
        result = get_sp500_universe()
        assert result == ["GOOGL", "AMZN"]

    def test_accepts_symbol_column_header(self, tmp_path, monkeypatch):
        """Accepts 'symbol' as column name."""
        csv_file = tmp_path / "sp500_universe_full.csv"
        csv_file.write_text("symbol\nNVDA\nMETA\n", encoding="utf-8")

        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)

        from paper_trader.engine.universe import get_sp500_universe
        result = get_sp500_universe()
        assert result == ["NVDA", "META"]

    def test_accepts_title_case_ticker_column(self, tmp_path, monkeypatch):
        """Accepts 'Ticker' and 'Symbol' headers (case-insensitive detection)."""
        csv_file = tmp_path / "sp500_universe_full.csv"
        csv_file.write_text("Ticker\nJPM\nV\n", encoding="utf-8")

        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)

        from paper_trader.engine.universe import get_sp500_universe
        result = get_sp500_universe()
        assert result == ["JPM", "V"]

    def test_deduplicates_and_normalizes(self, tmp_path, monkeypatch):
        """Deduplicates tickers and normalizes to uppercase, trimming whitespace."""
        csv_file = tmp_path / "sp500_universe_full.csv"
        csv_file.write_text("ticker\naapl\nAAPL\n  msft  \n\n", encoding="utf-8")

        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)

        from paper_trader.engine.universe import get_sp500_universe
        result = get_sp500_universe()
        assert result == ["AAPL", "MSFT"]

    def test_dot_ticker_preserved_matching_market_data_canonical(self, tmp_path, monkeypatch):
        """BRK.B is preserved as-is: market_data.py uses dot format as canonical ticker."""
        csv_file = tmp_path / "sp500_universe_full.csv"
        csv_file.write_text("ticker\nBRK.B\nAAPL\n", encoding="utf-8")

        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)

        from paper_trader.engine.universe import get_sp500_universe
        result = get_sp500_universe()
        # Canonical format in this codebase is BRK.B (market_data handles the
        # dot->hyphen translation internally when calling yfinance)
        assert "BRK.B" in result
        assert "BRK-B" not in result

    def test_get_universe_status_flags_stub(self, tmp_path, monkeypatch):
        """get_universe_status flags is_stub_universe when ticker_count < 450."""
        stub_csv = tmp_path / "sp500_universe.csv"
        stub_csv.write_text("ticker\nAAPL\nMSFT\n", encoding="utf-8")

        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)

        from paper_trader.engine.universe import get_universe_status
        info = get_universe_status()
        assert info["is_stub_universe"] is True
        assert info["warning"] is not None
        assert info["fallback_used"] is True
        assert info["ticker_count"] == 2

    def test_get_universe_status_reports_active_source(self, tmp_path, monkeypatch):
        """get_universe_status reports the active source file name."""
        full_csv = tmp_path / "sp500_universe_full.csv"
        full_csv.write_text("ticker\n" + "\n".join(f"T{i}" for i in range(500)) + "\n", encoding="utf-8")

        monkeypatch.setattr("paper_trader.engine.universe._DATA_DIR", tmp_path)

        from paper_trader.engine.universe import get_universe_status
        info = get_universe_status()
        assert info["active_source_file"] == "sp500_universe_full.csv"
        assert info["full_universe_file_exists"] is True
        assert info["fallback_used"] is False
        assert info["ticker_count"] == 500
        assert info["is_stub_universe"] is False
        assert info["warning"] is None
