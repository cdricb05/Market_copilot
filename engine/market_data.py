"""
engine/market_data.py — Market data fetching from external sources.

Functions:
    fetch_latest_prices() — Fetch latest prices from Yahoo Finance.

Design principles:
    - No database writes. Returns data only.
    - Easy to mock for tests.
    - Graceful failure per-ticker (one bad ticker doesn't break the batch).
    - Normalizes tickers to uppercase.
    - Extracts latest close price from yfinance.download() history.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

try:
    import yfinance
except ImportError:
    yfinance = None


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

    try:
        # Fetch data for all tickers at once
        data = yfinance.download(
            " ".join(normalized_tickers),
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
        price = _extract_latest_price(ticker, data)

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
