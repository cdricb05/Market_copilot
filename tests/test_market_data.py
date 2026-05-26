"""
tests/test_market_data.py — Unit tests for engine/market_data.py.

Tests fetch_latest_prices() and _extract_latest_price() with mocked yfinance
output. No real network calls or pandas imports in test file.

Mock strategy: Create simple objects that mimic yfinance DataFrame structure
to test _extract_latest_price() logic without depending on pandas.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from paper_trader.engine.market_data import fetch_latest_prices, _extract_latest_price


class MockSeries:
    """Simulates a pandas Series."""

    def __init__(self, values: list):
        self.values = values
        self.iloc = MockIndexer(values)

    def __len__(self):
        return len(self.values)

    def __getitem__(self, key):
        if isinstance(key, str):
            return None
        return self.values[key]


class MockDataFrame:
    """Simulates a pandas DataFrame with dict-like access."""

    def __init__(self, data: dict):
        self.data = data
        self.columns = list(data.keys())

    def __len__(self):
        values = list(self.data.values())
        return len(values[0]) if values else 0

    def __getitem__(self, key):
        if isinstance(key, str):
            if key in self.data:
                return MockSeries(self.data[key])
        elif isinstance(key, tuple):
            # MultiIndex access like data[("AAPL", "Close")]
            return None
        return None

    def __contains__(self, key):
        """Support 'in' operator for checking column existence."""
        return key in self.data


class MockMultiIndexDataFrame:
    """Simulates a DataFrame with MultiIndex columns like (Ticker, OHLCV)."""

    def __init__(self, data: dict):
        # data: {("AAPL", "Close"): [values], ("MSFT", "Close"): [values], ...}
        self.data = data
        self.columns = list(data.keys())

    def __len__(self):
        values = list(self.data.values())
        return len(values[0]) if values else 0

    def __getitem__(self, key):
        if isinstance(key, str) and key == "Close":
            # Return a DataFrame-like object with ticker columns
            close_data = {}
            for key_tuple, values in self.data.items():
                if isinstance(key_tuple, tuple) and len(key_tuple) == 2:
                    ticker, ohlcv = key_tuple
                    if ohlcv == "Close":
                        close_data[ticker] = values
            return MockDataFrame(close_data)
        return None


class MockIndexer:
    """Simulates Series.iloc for integer-based indexing."""

    def __init__(self, values: list):
        self.values = values

    def __getitem__(self, key):
        if isinstance(key, int):
            try:
                return self.values[key]
            except IndexError:
                pass
        return None


class TestExtractLatestPrice:
    """Unit tests for _extract_latest_price() with mocked DataFrame shapes."""

    def test_single_ticker_series_with_close(self) -> None:
        """Single ticker: Series of close prices, latest value extracted."""
        data = MockDataFrame({
            "Close": [101.0, 102.0, 103.5],
        })

        price = _extract_latest_price("AAPL", data)
        assert price == 103.5

    def test_multi_ticker_multiindex_close_column(self) -> None:
        """Multiple tickers: MultiIndex with (Ticker, Close) access."""
        data = MockMultiIndexDataFrame({
            ("AAPL", "Close"): [101.5, 102.5],
            ("MSFT", "Close"): [401.5, 402.5],
        })

        aapl_price = _extract_latest_price("AAPL", data)
        msft_price = _extract_latest_price("MSFT", data)

        assert aapl_price == 102.5
        assert msft_price == 402.5

    def test_missing_close_column(self) -> None:
        """DataFrame without Close column returns None."""
        data = MockDataFrame({
            "Open": [100.0, 101.0],
            "High": [102.0, 103.0],
        })

        price = _extract_latest_price("AAPL", data)
        assert price is None

    def test_missing_ticker_in_close_data(self) -> None:
        """Multi-ticker Close data without requested ticker returns None."""
        data = MockMultiIndexDataFrame({
            ("AAPL", "Close"): [101.5, 102.5],
            ("MSFT", "Close"): [401.5, 402.5],
        })

        price = _extract_latest_price("NOTFOUND", data)
        assert price is None

    def test_zero_price_returns_none(self) -> None:
        """Latest close of 0 is treated as invalid."""
        data = MockDataFrame({
            "Close": [100.0, 0.0],
        })

        price = _extract_latest_price("AAPL", data)
        assert price is None

    def test_negative_price_returns_none(self) -> None:
        """Negative close price is treated as invalid."""
        data = MockDataFrame({
            "Close": [100.0, -5.0],
        })

        price = _extract_latest_price("AAPL", data)
        assert price is None

    def test_nan_price_returns_none(self) -> None:
        """NaN close price is treated as invalid."""
        data = MockDataFrame({
            "Close": [100.0, float("nan")],
        })

        price = _extract_latest_price("AAPL", data)
        assert price is None

    def test_empty_series_returns_none(self) -> None:
        """Empty Close series returns None."""
        data = MockDataFrame({
            "Close": [],
        })

        price = _extract_latest_price("AAPL", data)
        assert price is None

    def test_none_input_returns_none(self) -> None:
        """None input returns None."""
        price = _extract_latest_price("AAPL", None)
        assert price is None


class MockYFinance:
    """Mock yfinance module with download method."""

    def __init__(self):
        self.download = None


class TestFetchLatestPrices:
    """Unit tests for fetch_latest_prices() with mocked yfinance."""

    def test_empty_ticker_list(self) -> None:
        """Empty input returns ([], [])."""
        successful, failures = fetch_latest_prices([])
        assert successful == []
        assert failures == []

    def test_yfinance_not_installed(self, monkeypatch) -> None:
        """When yfinance is None, all tickers are reported as failures."""
        import paper_trader.engine.market_data as market_data_module
        monkeypatch.setattr(market_data_module, "yfinance", None)

        successful, failures = fetch_latest_prices(["AAPL", "MSFT"])

        assert successful == []
        assert len(failures) == 2
        assert failures[0]["reason"] == "yfinance not installed"
        assert failures[1]["reason"] == "yfinance not installed"

    def test_single_ticker_success(self, monkeypatch) -> None:
        """Single ticker with valid price returns success."""
        import paper_trader.engine.market_data as market_data_module

        mock_data = MockDataFrame({
            "Close": [105.50],
        })

        def mock_download(*args, **kwargs):
            return mock_data

        mock_yf = MockYFinance()
        mock_yf.download = mock_download
        monkeypatch.setattr(market_data_module, "yfinance", mock_yf)

        successful, failures = fetch_latest_prices(["AAPL"])

        assert len(successful) == 1
        assert successful[0]["ticker"] == "AAPL"
        assert Decimal(successful[0]["price"]) == Decimal("105.5")
        assert failures == []

    def test_multiple_tickers_all_success(self, monkeypatch) -> None:
        """Multiple tickers, all with valid prices."""
        import paper_trader.engine.market_data as market_data_module

        mock_data = MockMultiIndexDataFrame({
            ("AAPL", "Close"): [105.5],
            ("MSFT", "Close"): [410.75],
        })

        def mock_download(*args, **kwargs):
            return mock_data

        mock_yf = MockYFinance()
        mock_yf.download = mock_download
        monkeypatch.setattr(market_data_module, "yfinance", mock_yf)

        successful, failures = fetch_latest_prices(["AAPL", "MSFT"])

        assert len(successful) == 2
        assert len(failures) == 0
        tickers = [p["ticker"] for p in successful]
        assert "AAPL" in tickers
        assert "MSFT" in tickers

    def test_mixed_success_and_failure(self, monkeypatch) -> None:
        """Some tickers succeed, others fail; both reported."""
        import paper_trader.engine.market_data as market_data_module

        # AAPL has valid price, MSFT has NaN
        mock_data = MockMultiIndexDataFrame({
            ("AAPL", "Close"): [105.5],
            ("MSFT", "Close"): [float("nan")],
        })

        def mock_download(*args, **kwargs):
            return mock_data

        mock_yf = MockYFinance()
        mock_yf.download = mock_download
        monkeypatch.setattr(market_data_module, "yfinance", mock_yf)

        successful, failures = fetch_latest_prices(["AAPL", "MSFT"])

        assert len(successful) == 1
        assert successful[0]["ticker"] == "AAPL"
        assert len(failures) == 1
        assert failures[0]["ticker"] == "MSFT"

    def test_yfinance_exception_returns_failures(self, monkeypatch) -> None:
        """yfinance.download exception marks all tickers as failures."""
        import paper_trader.engine.market_data as market_data_module

        def mock_download(*args, **kwargs):
            raise Exception("Network error")

        mock_yf = MockYFinance()
        mock_yf.download = mock_download
        monkeypatch.setattr(market_data_module, "yfinance", mock_yf)

        successful, failures = fetch_latest_prices(["AAPL", "MSFT"])

        assert successful == []
        assert len(failures) == 2
        assert all("Network error" in f["reason"] for f in failures)

    def test_case_insensitive_normalization(self, monkeypatch) -> None:
        """Tickers are normalized to uppercase."""
        import paper_trader.engine.market_data as market_data_module

        mock_data = MockMultiIndexDataFrame({
            ("AAPL", "Close"): [105.5],
        })

        def mock_download(*args, **kwargs):
            return mock_data

        mock_yf = MockYFinance()
        mock_yf.download = mock_download
        monkeypatch.setattr(market_data_module, "yfinance", mock_yf)

        successful, failures = fetch_latest_prices(["aapl", "AaPl"])

        assert len(successful) == 1
        assert successful[0]["ticker"] == "AAPL"

    def test_deduplication(self, monkeypatch) -> None:
        """Duplicate tickers in input are deduplicated before fetch."""
        import paper_trader.engine.market_data as market_data_module

        mock_data = MockMultiIndexDataFrame({
            ("AAPL", "Close"): [105.5],
        })

        fetch_calls = []

        def mock_download(symbols, *args, **kwargs):
            fetch_calls.append(symbols)
            return mock_data

        mock_yf = MockYFinance()
        mock_yf.download = mock_download
        monkeypatch.setattr(market_data_module, "yfinance", mock_yf)

        successful, failures = fetch_latest_prices(["AAPL", "AAPL", "AAPL"])

        # Verify only one AAPL was fetched
        assert len(fetch_calls) == 1
        assert "AAPL" in fetch_calls[0]
        # Count occurrences of AAPL in the fetch call (space-separated)
        aapl_count = fetch_calls[0].count("AAPL")
        assert aapl_count == 1

    def test_price_returned_as_decimal_string(self, monkeypatch) -> None:
        """Prices are returned as decimal strings."""
        import paper_trader.engine.market_data as market_data_module

        mock_data = MockDataFrame({
            "Close": [123.456789],
        })

        def mock_download(*args, **kwargs):
            return mock_data

        mock_yf = MockYFinance()
        mock_yf.download = mock_download
        monkeypatch.setattr(market_data_module, "yfinance", mock_yf)

        successful, failures = fetch_latest_prices(["AAPL"])

        assert len(successful) == 1
        # Verify it's a string (not Decimal)
        assert isinstance(successful[0]["price"], str)
        # Verify it's a valid number
        price_decimal = Decimal(successful[0]["price"])
        assert price_decimal > 0

    def test_zero_price_marked_as_failure(self, monkeypatch) -> None:
        """Zero price is marked as failure."""
        import paper_trader.engine.market_data as market_data_module

        mock_data = MockMultiIndexDataFrame({
            ("AAPL", "Close"): [0.0],
        })

        def mock_download(*args, **kwargs):
            return mock_data

        mock_yf = MockYFinance()
        mock_yf.download = mock_download
        monkeypatch.setattr(market_data_module, "yfinance", mock_yf)

        successful, failures = fetch_latest_prices(["AAPL"])

        assert successful == []
        assert len(failures) == 1
        assert "AAPL" in failures[0]["ticker"]
