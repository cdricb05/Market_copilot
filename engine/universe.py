"""
engine/universe.py — Stock universe management.

Supports loading ticker lists from CSV for market scanning and candidate selection.
Prefers data/sp500_universe_full.csv (full universe); falls back to
data/sp500_universe.csv (stub). Accepts ticker/symbol/Ticker/Symbol column names.
"""
from __future__ import annotations

import csv as _csv
import pathlib
from typing import Any


_DATA_DIR = pathlib.Path(__file__).parent.parent / "data"
_FULL_UNIVERSE_FILE = "sp500_universe_full.csv"
_STUB_UNIVERSE_FILE = "sp500_universe.csv"
_STUB_WARNING_THRESHOLD = 450


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


def _load_tickers_from_csv(csv_path: pathlib.Path) -> list[str]:
    """
    Load and normalize tickers from a CSV file.

    Accepted column headers: ticker, symbol (case-insensitive; Ticker, Symbol also work).
    Falls back to the first column if no recognized header is found.
    Normalizes: uppercase, strip whitespace, remove blanks, deduplicate preserving
    first-seen order.

    Args:
        csv_path: Path to the CSV file.

    Returns:
        List of normalized tickers. Empty list on any read error.
    """
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            reader = _csv.reader(f)
            rows = list(reader)
    except Exception:
        return []

    if not rows:
        return []

    # Detect ticker column index from header row
    header = [c.strip().lower() for c in rows[0]]
    ticker_col_idx: int | None = None
    for accepted in ("ticker", "symbol"):
        if accepted in header:
            ticker_col_idx = header.index(accepted)
            break

    # Skip header row only if we recognized the column name
    data_rows = rows[1:] if ticker_col_idx is not None else rows
    if ticker_col_idx is None:
        ticker_col_idx = 0

    tickers: list[str] = []
    seen: set[str] = set()
    for row in data_rows:
        if not row or ticker_col_idx >= len(row):
            continue
        raw_ticker = row[ticker_col_idx].strip()
        if not raw_ticker:
            continue
        ticker = raw_ticker.upper()
        if ticker not in seen:
            tickers.append(ticker)
            seen.add(ticker)

    return tickers


def _resolve_universe_source() -> tuple[list[str], pathlib.Path, bool]:
    """
    Resolve the active universe source file.

    Prefers sp500_universe_full.csv; falls back to sp500_universe.csv.

    Returns:
        (tickers, source_path, fallback_used)
    """
    full_path = _DATA_DIR / _FULL_UNIVERSE_FILE
    stub_path = _DATA_DIR / _STUB_UNIVERSE_FILE

    if full_path.exists():
        tickers = _load_tickers_from_csv(full_path)
        if tickers:
            return tickers, full_path, False

    tickers = _load_tickers_from_csv(stub_path) if stub_path.exists() else []
    return tickers, stub_path, True


def get_sp500_universe() -> list[str]:
    """
    Load S&P 500 ticker universe from CSV file.

    Prefers data/sp500_universe_full.csv; falls back to data/sp500_universe.csv.
    Accepted column names: ticker, symbol, Ticker, Symbol (case-insensitive).
    Normalizes to uppercase, removes blanks, deduplicates preserving order.

    Returns:
        List of tickers (uppercase, deduplicated). Empty list if no file found.

    Note:
        Deterministic for testing. Does not fetch from internet.
        Provide data/sp500_universe_full.csv for full S&P 500 coverage.
    """
    tickers, _, _ = _resolve_universe_source()
    return tickers


def get_universe_status() -> dict[str, Any]:
    """
    Return metadata about the current universe.

    No DB access, no writes. The returned dict includes a 'tickers' key with the
    full ticker list for use by the status endpoint's market data coverage query.

    Returns:
        Dict with universe metadata fields.
    """
    full_path = _DATA_DIR / _FULL_UNIVERSE_FILE
    stub_path = _DATA_DIR / _STUB_UNIVERSE_FILE

    full_exists = full_path.exists()
    stub_exists = stub_path.exists()

    tickers, source_path, fallback_used = _resolve_universe_source()
    ticker_count = len(tickers)
    is_stub = ticker_count < _STUB_WARNING_THRESHOLD

    warning: str | None = None
    if is_stub:
        warning = (
            f"Universe contains only {ticker_count} tickers. "
            f"Expected {_STUB_WARNING_THRESHOLD}+ for full S&P 500 coverage. "
            "Provide data/sp500_universe_full.csv to use a full universe."
        )

    return {
        "universe_name": "SP500",
        "active_source_file": source_path.name,
        "ticker_count": ticker_count,
        "first_10_tickers": tickers[:10],
        "last_10_tickers": tickers[-10:] if len(tickers) > 10 else list(tickers),
        "is_stub_universe": is_stub,
        "expected_full_sp500_min_count": _STUB_WARNING_THRESHOLD,
        "warning": warning,
        "fallback_used": fallback_used,
        "full_universe_file_exists": full_exists,
        "stub_universe_file_exists": stub_exists,
        "tickers": tickers,
    }
