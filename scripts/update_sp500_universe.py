"""
scripts/update_sp500_universe.py -- Maintenance utility for populating
data/sp500_universe_full.csv from a supplied CSV source.

This is a ONE-TIME / on-demand maintenance script.
The app does NOT call it automatically at startup. Run it manually whenever
you want to update the universe from a new external source.

IMPORTANT: This script never fetches from the internet unless you explicitly
supply --source-url. Normal app startup is always offline.

Usage examples:
  python scripts/update_sp500_universe.py --source-file ~/Downloads/sp500.csv
  python scripts/update_sp500_universe.py --source-url https://example.com/sp500.csv
  python scripts/update_sp500_universe.py --source-file ~/sp500.csv --dry-run
  python scripts/update_sp500_universe.py --source-file ~/sp500.csv --min-count 100
  python scripts/update_sp500_universe.py --source-file ~/sp500.csv --output data/custom.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import pathlib
import sys
import urllib.request
from datetime import datetime, timezone
from io import StringIO

_REPO_ROOT = pathlib.Path(__file__).parent.parent
_DEFAULT_OUTPUT = _REPO_ROOT / "data" / "sp500_universe_full.csv"
_DEFAULT_MIN_COUNT = 450
_ACCEPTED_COLUMNS = ("ticker", "symbol")


# ---------------------------------------------------------------------------
# CSV parsing (stdlib only, no pandas)
# ---------------------------------------------------------------------------

def _parse_tickers_from_csv_text(text: str) -> list[str]:
    """
    Parse a CSV string and return a normalized, deduplicated ticker list.

    Accepted column headers: ticker, symbol (case-insensitive).
    Falls back to the first column when no recognized header is found.
    Normalizes: trim whitespace, uppercase, remove blanks, deduplicate
    preserving first-seen order. BRK.B is preserved as dot-format (canonical
    in this codebase; market_data.py handles the yfinance translation).
    """
    reader = csv.reader(StringIO(text))
    rows = list(reader)

    if not rows:
        return []

    header = [c.strip().lower() for c in rows[0]]
    ticker_col_idx: int | None = None
    for accepted in _ACCEPTED_COLUMNS:
        if accepted in header:
            ticker_col_idx = header.index(accepted)
            break

    data_rows = rows[1:] if ticker_col_idx is not None else rows
    if ticker_col_idx is None:
        ticker_col_idx = 0

    tickers: list[str] = []
    seen: set[str] = set()
    for row in data_rows:
        if not row or ticker_col_idx >= len(row):
            continue
        raw = row[ticker_col_idx].strip()
        if not raw:
            continue
        ticker = raw.upper()
        if ticker not in seen:
            tickers.append(ticker)
            seen.add(ticker)

    return tickers


# ---------------------------------------------------------------------------
# Network fetch (only called when --source-url is used)
# ---------------------------------------------------------------------------

def _fetch_csv_from_url(url: str) -> str:
    """Fetch CSV text from an HTTPS URL. Only called via --source-url."""
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "paper-trader-universe-updater/1.0"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        charset = "utf-8"
        ct = resp.headers.get("Content-Type", "")
        if "charset=" in ct:
            charset = ct.split("charset=")[-1].strip()
        return resp.read().decode(charset, errors="replace")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_tickers(
    tickers: list[str],
    min_count: int,
) -> list[str]:
    """Return a list of validation error strings (empty = all OK)."""
    errors: list[str] = []

    if len(tickers) < min_count:
        errors.append(
            f"Ticker count {len(tickers)} is below the minimum of {min_count}. "
            f"Use --min-count to override if intentional (e.g. for testing)."
        )

    blanks = [t for t in tickers if not t.strip()]
    if blanks:
        errors.append(
            f"Found {len(blanks)} blank ticker(s) after normalization. "
            "The parser should have stripped these — check your source CSV."
        )

    return errors


# ---------------------------------------------------------------------------
# File writing
# ---------------------------------------------------------------------------

def _write_universe_csv(output_path: pathlib.Path, tickers: list[str]) -> None:
    """Write the normalized ticker list as a single-column CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ticker"])
        for ticker in tickers:
            writer.writerow([ticker])


def _write_metadata_json(
    meta_path: pathlib.Path,
    source_type: str,
    source_value: str,
    tickers: list[str],
) -> None:
    """Write a companion metadata JSON file alongside the CSV."""
    meta = {
        "source_type": source_type,
        "source_value": source_value,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "ticker_count": len(tickers),
        "first_10_tickers": tickers[:10],
        "last_10_tickers": tickers[-10:] if len(tickers) > 10 else list(tickers),
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="update_sp500_universe",
        description=(
            "Populate data/sp500_universe_full.csv from a supplied CSV source.\n\n"
            "This script is a MANUAL maintenance tool, not app runtime.\n"
            "The app never runs it automatically at startup."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Accepted CSV column headers: ticker, symbol, Ticker, Symbol\n"
            "  (case-insensitive; falls back to first column if none match)\n\n"
            "BRK.B is preserved in dot format -- this codebase treats BRK.B as\n"
            "canonical and handles the yfinance translation internally.\n\n"
            "Examples:\n"
            "  python scripts/update_sp500_universe.py --source-file ~/Downloads/sp500.csv\n"
            "  python scripts/update_sp500_universe.py --source-url https://example.com/tickers.csv\n"
            "  python scripts/update_sp500_universe.py --source-file ~/sp500.csv --dry-run\n"
            "  python scripts/update_sp500_universe.py --source-file ~/sp500.csv --min-count 100\n"
        ),
    )

    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--source-file",
        metavar="PATH",
        help="Path to a local CSV file containing ticker symbols.",
    )
    source_group.add_argument(
        "--source-url",
        metavar="URL",
        help=(
            "HTTPS URL to a remote CSV file. "
            "WARNING: this is the ONLY code path that accesses the internet. "
            "Normal app startup never calls this."
        ),
    )
    parser.add_argument(
        "--output",
        metavar="PATH",
        default=str(_DEFAULT_OUTPUT),
        help=f"Output CSV path (default: {_DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=_DEFAULT_MIN_COUNT,
        metavar="N",
        help=(
            f"Minimum required ticker count. Aborts if source has fewer "
            f"(default: {_DEFAULT_MIN_COUNT})."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print summary but do NOT write any files.",
    )

    args = parser.parse_args(argv)
    output_path = pathlib.Path(args.output).expanduser().resolve()
    meta_path = output_path.with_name(output_path.stem + "_meta.json")

    # --- Load source CSV ---
    if args.source_file:
        source_type = "file"
        source_value = str(pathlib.Path(args.source_file).expanduser().resolve())
        src = pathlib.Path(args.source_file).expanduser()
        if not src.exists():
            print(f"ERROR: Source file not found: {src}", file=sys.stderr)
            return 1
        try:
            csv_text = src.read_text(encoding="utf-8")
        except Exception as exc:
            print(f"ERROR: Could not read source file: {exc}", file=sys.stderr)
            return 1
    else:
        source_type = "url"
        source_value = args.source_url
        print(f"Fetching from URL: {args.source_url}", file=sys.stderr)
        try:
            csv_text = _fetch_csv_from_url(args.source_url)
        except Exception as exc:
            print(f"ERROR: Failed to fetch URL: {exc}", file=sys.stderr)
            return 1

    # --- Parse ---
    tickers = _parse_tickers_from_csv_text(csv_text)

    # --- Validate ---
    errors = _validate_tickers(tickers, args.min_count)
    if errors:
        for err in errors:
            print(f"ERROR: {err}", file=sys.stderr)
        return 1

    # --- Print summary ---
    print(f"Source type   : {source_type}")
    print(f"Source value  : {source_value}")
    print(f"Ticker count  : {len(tickers)}")
    print(f"First 10      : {tickers[:10]}")
    print(f"Last 10       : {tickers[-10:]}")
    print(f"Output CSV    : {output_path}")
    print(f"Output meta   : {meta_path}")

    if args.dry_run:
        print("\nDRY RUN -- no files written.")
        return 0

    # --- Write ---
    try:
        _write_universe_csv(output_path, tickers)
        _write_metadata_json(meta_path, source_type, source_value, tickers)
    except Exception as exc:
        print(f"ERROR: Failed to write output: {exc}", file=sys.stderr)
        return 1

    print(f"\nWrote {len(tickers)} tickers to {output_path}")
    print(f"Wrote metadata to {meta_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
