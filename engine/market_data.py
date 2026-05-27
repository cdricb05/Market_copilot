"""
engine/market_data.py — Market data fetching from external sources.

Functions:
    fetch_latest_prices() — Fetch latest prices from Yahoo Finance.
    fetch_historical_prices() — Fetch daily CLOSE prices for date range from Yahoo Finance.

Design principles:
    - No database writes. Returns data only.
    - Easy to mock for tests.
    - Graceful failure per-ticker (one bad ticker doesn't break the batch).
    - Normalizes tickers to uppercase.
    - Extracts close prices from yfinance.download() history.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

try:
    import yfinance
except ImportError:
    yfinance = None


# Provider-specific ticker mappings (canonical -> yfinance symbol)
_TICKER_SYMBOL_MAPPING = {
    "BRK.B": "BRK-B",  # Berkshire Hathaway Class B: dot vs hyphen
}


def _get_yfinance_symbol(ticker: str) -> str:
    """Map canonical ticker to yfinance symbol."""
    return _TICKER_SYMBOL_MAPPING.get(ticker, ticker)


def _get_canonical_ticker(yfinance_symbol: str) -> str:
    """Map yfinance symbol back to canonical ticker."""
    for canonical, yf_symbol in _TICKER_SYMBOL_MAPPING.items():
        if yf_symbol == yfinance_symbol:
            return canonical
    return yfinance_symbol


def fetch_latest_prices(tickers: list[str]) -> tuple[list[dict], list[dict]]:
    """
    Fetch latest available prices from Yahoo Finance.

    Args:
        tickers: List of stock tickers (case-insensitive). Empty list returns ([], []).

    Returns:
        (successful_prices, failures)
        successful_prices: list of dicts {ticker, price} normalized to uppercase tickers
        failures: list of dicts {ticker, reason} for tickers that couldn't be fetched

    Behavior:
        - Normalizes tickers to uppercase.
        - Extracts latest close price from yfinance.download() history.
        - Returns Decimal-formatted price strings.
        - Skips tickers with None/zero/negative prices (treated as failures).
        - Network errors or missing symbols are recorded as failures, not exceptions.
    """
    if not tickers:
        return [], []

    if yfinance is None:
        reasons = {t.upper(): "yfinance not installed" for t in tickers}
        return [], [{"ticker": t, "reason": r} for t, r in reasons.items()]

    successful = []
    failed = {}

    # Normalize tickers to uppercase and deduplicate
    normalized_tickers = list(set(t.upper() for t in tickers))

    # Map to yfinance symbols for fetching
    ticker_to_yf_symbol = {t: _get_yfinance_symbol(t) for t in normalized_tickers}
    yf_symbols = list(set(ticker_to_yf_symbol.values()))

    try:
        # Fetch data for all tickers at once
        data = yfinance.download(
            " ".join(yf_symbols),
            period="1d",
            progress=False,
            threads=False,
        )
    except Exception as exc:
        # Network error or other yfinance exception
        for ticker in normalized_tickers:
            failed[ticker] = f"Failed to fetch: {str(exc)[:100]}"
        return [], [{"ticker": t, "reason": r} for t, r in failed.items()]

    # Process each ticker
    for ticker in normalized_tickers:
        yf_symbol = ticker_to_yf_symbol[ticker]
        price = _extract_latest_price(yf_symbol, data)

        if price is None:
            failed[ticker] = "No valid price returned"
        else:
            try:
                price_decimal = Decimal(str(price))
                if price_decimal > 0:
                    successful.append({
                        "ticker": ticker,
                        "price": str(price_decimal),
                    })
                else:
                    failed[ticker] = "Price is zero or negative"
            except Exception:
                failed[ticker] = "Price conversion error"

    failures = [{"ticker": t, "reason": r} for t, r in failed.items()]
    return successful, failures


def _extract_latest_price(ticker: str, data: Any) -> float | None:
    """
    Extract the latest close price from yfinance download data.

    Handles two yfinance.download() output shapes:
        - Single ticker: DataFrame with columns [Open, High, Low, Close, ...]
        - Multiple tickers: DataFrame with MultiIndex columns like (Ticker, OHLCV)

    Args:
        ticker: Normalized uppercase ticker.
        data: DataFrame from yfinance.download().

    Returns:
        Latest close price (float) or None if unavailable.
    """
    if data is None or len(data) == 0:
        return None

    try:
        # Try to get Close data (works for both single and multi-ticker)
        close_data = None
        if hasattr(data, "__getitem__"):
            try:
                close_data = data["Close"]
            except (KeyError, TypeError):
                pass

        if close_data is None:
            return None

        # Case 1: Single ticker - close_data is a Series with dates as index
        if hasattr(close_data, "iloc") and not hasattr(close_data, "columns"):
            latest_close = close_data.iloc[-1]
            if latest_close is not None and not (hasattr(latest_close, "__nan__")):
                try:
                    val = float(latest_close)
                    if val > 0:
                        return val
                except (ValueError, TypeError):
                    pass
            return None

        # Case 2: Multiple tickers - close_data is a DataFrame with ticker columns
        if hasattr(close_data, "columns"):
            if ticker in close_data.columns:
                ticker_close = close_data[ticker]
                if hasattr(ticker_close, "iloc") and len(ticker_close) > 0:
                    latest = ticker_close.iloc[-1]
                    if latest is not None and not (hasattr(latest, "__nan__")):
                        try:
                            val = float(latest)
                            if val > 0:
                                return val
                        except (ValueError, TypeError):
                            pass

        return None
    except Exception:
        return None


def fetch_historical_prices(
    tickers: list[str],
    start_date: date,
    end_date: date,
) -> tuple[dict[str, list[dict]], dict[str, str]]:
    """
    Fetch daily CLOSE prices for a date range from Yahoo Finance.

    Args:
        tickers: List of stock tickers (case-insensitive). Empty list returns ({}, {}).
        start_date: Start date (inclusive).
        end_date: End date (inclusive). Note: yfinance uses exclusive end, so we add 1 day.

    Returns:
        (successful_prices, failures)
        successful_prices: dict of {ticker: [{"market_date": date, "price": Decimal}, ...]}
        failures: dict of {ticker: reason_string}

    Behavior:
        - Normalizes tickers to uppercase.
        - Extracts daily close prices from yfinance.download() history.
        - Returns Decimal-formatted price strings.
        - Skips tickers with no data, missing Close, or only NaN prices (recorded as failures).
        - Network errors or exceptions are recorded as failures, not raised.
    """
    if not tickers:
        return {}, {}

    if yfinance is None:
        reasons = {t.upper(): "yfinance not installed" for t in tickers}
        return {}, reasons

    successful = {}
    failed = {}

    # Normalize tickers to uppercase and deduplicate
    normalized_tickers = list(set(t.upper() for t in tickers))

    # Map to yfinance symbols for fetching
    ticker_to_yf_symbol = {t: _get_yfinance_symbol(t) for t in normalized_tickers}
    yf_symbols = list(set(ticker_to_yf_symbol.values()))

    try:
        # Note: yfinance end date is exclusive, so add 1 day
        from datetime import timedelta
        yf_end_date = end_date + timedelta(days=1)

        # Fetch data for all tickers at once
        data = yfinance.download(
            " ".join(yf_symbols),
            start=start_date,
            end=yf_end_date,
            progress=False,
            threads=False,
        )
    except Exception as exc:
        # Network error or other yfinance exception
        for ticker in normalized_tickers:
            failed[ticker] = f"Failed to fetch: {str(exc)[:100]}"
        return {}, failed

    # Process each ticker
    for ticker in normalized_tickers:
        yf_symbol = ticker_to_yf_symbol[ticker]
        prices = _extract_historical_prices(yf_symbol, data)

        if not prices:  # Empty list or None
            failed[ticker] = "No valid prices returned"
        else:
            successful[ticker] = prices

    return successful, failed


def _extract_historical_prices(ticker: str, data: Any) -> list[dict] | None:
    """
    Extract daily close prices from yfinance download data for a single ticker.

    Returns list of {"market_date": date, "price": Decimal} dicts, ordered chronologically.
    Returns None if ticker has no data or all prices are invalid.

    Handles two yfinance.download() output shapes:
        - Single ticker: DataFrame with columns [Open, High, Low, Close, ...]
        - Multiple tickers: DataFrame with MultiIndex columns like (Ticker, OHLCV)
    """
    if data is None or len(data) == 0:
        return None

    try:
        # Try to get Close data (works for both single and multi-ticker)
        close_data = None
        if hasattr(data, "__getitem__"):
            try:
                close_data = data["Close"]
            except (KeyError, TypeError):
                pass

        if close_data is None:
            return None

        result = []

        # Case 1: Single ticker - close_data is a Series with dates as index
        if hasattr(close_data, "iloc") and not hasattr(close_data, "columns"):
            if hasattr(close_data, "items"):
                # Use items() method if available (works on Series/MockSeries)
                for market_date, close_val in close_data.items():
                    if close_val is None or (hasattr(close_val, "__nan__")):
                        continue
                    try:
                        price_float = float(close_val)
                        if price_float > 0:
                            # Convert market_date to date object if needed
                            if hasattr(market_date, "date"):
                                md = market_date.date()
                            else:
                                md = market_date
                            result.append({
                                "market_date": md,
                                "price": Decimal(str(price_float)),
                            })
                    except (ValueError, TypeError, AttributeError):
                        continue
            return result if result else None

        # Case 2: Multiple tickers - close_data is a DataFrame with ticker columns
        if hasattr(close_data, "columns"):
            if ticker in close_data.columns:
                ticker_close = close_data[ticker]
                if hasattr(ticker_close, "items"):
                    for market_date, close_val in ticker_close.items():
                        if close_val is None or (hasattr(close_val, "__nan__")):
                            continue
                        try:
                            price_float = float(close_val)
                            if price_float > 0:
                                # Convert market_date to date object if needed
                                if hasattr(market_date, "date"):
                                    md = market_date.date()
                                else:
                                    md = market_date
                                result.append({
                                    "market_date": md,
                                    "price": Decimal(str(price_float)),
                                })
                        except (ValueError, TypeError, AttributeError):
                            continue
                    return result if result else None

        return None
    except Exception:
        return None
