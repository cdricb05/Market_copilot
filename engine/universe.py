"""
engine/universe.py — Stock universe management.

Supports loading ticker lists from CSV for market scanning and candidate selection.
Currently designed to support the S&P 500 universe via data/sp500_universe.csv.
"""
from __future__ import annotations

import pathlib
from typing import Any


def normalize_ticker_list(raw: list[str] | str | None) -> list[str]:
    """
    Normalize and validate a list of tickers.

    Args:
        raw: List of ticker strings, a single ticker string, or None.

    Returns:
        List of uppercase, deduplicated tickers (empty list if input is None/empty).
    """
    if raw is None:
        return []

    if isinstance(raw, str):
        raw = [raw]

    if not isinstance(raw, list):
        return []

    # Normalize: uppercase, strip whitespace, deduplicate
    normalized = []
    seen = set()
    for ticker in raw:
        if not isinstance(ticker, str):
            continue
        ticker = ticker.strip().upper()
        if ticker and ticker not in seen:
            normalized.append(ticker)
            seen.add(ticker)

    return normalized


def get_sp500_universe() -> list[str]:
    """
    Load S&P 500 ticker universe from CSV file.

    Reads data/sp500_universe.csv, expecting one column: ticker

    Returns:
        List of S&P 500 tickers (uppercase). Empty list if file not found.

    Note:
        Deterministic for testing. Does not fetch from internet.
        Currently contains a representative validated subset of S&P 500 constituents.
        Replace with a full maintained S&P 500 constituent file before production use.
    """
    csv_path = pathlib.Path(__file__).parent.parent / "data" / "sp500_universe.csv"

    if not csv_path.exists():
        return []

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        # Skip header line
        if lines and lines[0].strip().lower() == "ticker":
            lines = lines[1:]

        tickers = [line.strip().upper() for line in lines if line.strip()]
        return tickers
    except Exception:
        return []
