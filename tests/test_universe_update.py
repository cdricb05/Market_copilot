"""
tests/test_universe_update.py -- Unit tests for scripts/update_sp500_universe.py.

All tests are fully offline: the --source-url path is exercised via a
monkeypatched _fetch_csv_from_url, so no real network calls are made.
"""
from __future__ import annotations

import csv
import json
import pathlib
import sys

import pytest

# Make the scripts/ directory importable regardless of PYTHONPATH
_SCRIPTS_DIR = pathlib.Path(__file__).parent.parent / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import update_sp500_universe as usu


# ---------------------------------------------------------------------------
# _parse_tickers_from_csv_text
# ---------------------------------------------------------------------------

class TestParseTickers:
    """_parse_tickers_from_csv_text: column detection, normalization, deduplication."""

    def test_ticker_column_header(self):
        """Reads tickers from a 'ticker' column."""
        text = "ticker\nAAPL\nMSFT\nGOOGL\n"
        result = usu._parse_tickers_from_csv_text(text)
        assert result == ["AAPL", "MSFT", "GOOGL"]

    def test_symbol_column_header(self):
        """Reads tickers from a 'symbol' column."""
        text = "symbol\nNVDA\nMETA\nAMZN\n"
        result = usu._parse_tickers_from_csv_text(text)
        assert result == ["NVDA", "META", "AMZN"]

    def test_title_case_ticker_header(self):
        """Accepts 'Ticker' header (case-insensitive detection)."""
        text = "Ticker\nJPM\nV\n"
        result = usu._parse_tickers_from_csv_text(text)
        assert result == ["JPM", "V"]

    def test_title_case_symbol_header(self):
        """Accepts 'Symbol' header (case-insensitive detection)."""
        text = "Symbol\nBAC\nWFC\n"
        result = usu._parse_tickers_from_csv_text(text)
        assert result == ["BAC", "WFC"]

    def test_normalizes_to_uppercase(self):
        """Lowercased tickers are uppercased."""
        text = "ticker\naapl\nmsft\n"
        result = usu._parse_tickers_from_csv_text(text)
        assert result == ["AAPL", "MSFT"]

    def test_trims_whitespace(self):
        """Leading/trailing whitespace is stripped."""
        text = "ticker\n  AAPL  \n MSFT\n"
        result = usu._parse_tickers_from_csv_text(text)
        assert result == ["AAPL", "MSFT"]

    def test_deduplicates_preserving_first_seen_order(self):
        """Duplicates are removed; first occurrence is kept."""
        text = "ticker\nAAPL\nMSFT\nAAPL\naapL\n"
        result = usu._parse_tickers_from_csv_text(text)
        assert result == ["AAPL", "MSFT"]

    def test_removes_blank_rows(self):
        """Blank rows are skipped."""
        text = "ticker\nAAPL\n\nMSFT\n\n"
        result = usu._parse_tickers_from_csv_text(text)
        assert result == ["AAPL", "MSFT"]

    def test_dot_ticker_preserved(self):
        """BRK.B is preserved in dot format (canonical in this codebase)."""
        text = "ticker\nBRK.B\nAAPL\n"
        result = usu._parse_tickers_from_csv_text(text)
        assert "BRK.B" in result
        assert "BRK-B" not in result

    def test_empty_csv_returns_empty(self):
        """Empty CSV text returns an empty list."""
        assert usu._parse_tickers_from_csv_text("") == []

    def test_falls_back_to_first_column_when_no_recognized_header(self):
        """Falls back to first column when header is unrecognized."""
        text = "company,exchange\nAAPL,NASDAQ\nMSFT,NASDAQ\n"
        result = usu._parse_tickers_from_csv_text(text)
        # First column values: "company", "AAPL", "MSFT" -- no header skip
        assert "AAPL" in result
        assert "MSFT" in result


# ---------------------------------------------------------------------------
# _validate_tickers
# ---------------------------------------------------------------------------

class TestValidateTickers:
    """_validate_tickers: min-count enforcement and blank detection."""

    def test_accepts_sufficient_count(self):
        """No errors when ticker count meets min_count."""
        tickers = [f"T{i}" for i in range(450)]
        errors = usu._validate_tickers(tickers, min_count=450)
        assert errors == []

    def test_rejects_insufficient_count(self):
        """Returns error when ticker count is below min_count."""
        tickers = [f"T{i}" for i in range(10)]
        errors = usu._validate_tickers(tickers, min_count=450)
        assert len(errors) == 1
        assert "10" in errors[0]
        assert "450" in errors[0]

    def test_custom_min_count_respected(self):
        """Lowered --min-count allows smaller lists through."""
        tickers = [f"T{i}" for i in range(5)]
        errors = usu._validate_tickers(tickers, min_count=3)
        assert errors == []

    def test_blank_tickers_flagged(self):
        """Blank strings in the ticker list are flagged (parser should prevent this)."""
        tickers = ["AAPL", "", "MSFT"]
        errors = usu._validate_tickers(tickers, min_count=1)
        assert any("blank" in e.lower() for e in errors)


# ---------------------------------------------------------------------------
# main() -- dry-run
# ---------------------------------------------------------------------------

class TestDryRun:
    """--dry-run validates but writes nothing."""

    def test_dry_run_creates_no_files(self, tmp_path):
        """--dry-run exits 0 and writes no CSV or JSON."""
        src = tmp_path / "source.csv"
        src.write_text("ticker\n" + "\n".join(f"T{i}" for i in range(500)) + "\n", encoding="utf-8")
        out = tmp_path / "out" / "sp500_universe_full.csv"

        rc = usu.main([
            "--source-file", str(src),
            "--output", str(out),
            "--min-count", "1",
            "--dry-run",
        ])
        assert rc == 0
        assert not out.exists(), "CSV must not be written in dry-run mode"
        meta = out.with_name(out.stem + "_meta.json")
        assert not meta.exists(), "Metadata must not be written in dry-run mode"

    def test_dry_run_returns_zero_on_valid_source(self, tmp_path):
        """--dry-run returns exit code 0 when validation passes."""
        src = tmp_path / "source.csv"
        src.write_text("ticker\n" + "\n".join(f"T{i}" for i in range(500)) + "\n", encoding="utf-8")
        out = tmp_path / "sp500_universe_full.csv"

        rc = usu.main(["--source-file", str(src), "--output", str(out), "--min-count", "1", "--dry-run"])
        assert rc == 0


# ---------------------------------------------------------------------------
# main() -- file writing
# ---------------------------------------------------------------------------

class TestFileWriting:
    """main() with --source-file writes CSV and metadata correctly."""

    def _make_source(self, tmp_path: pathlib.Path, n: int = 500, col: str = "ticker") -> pathlib.Path:
        src = tmp_path / "source.csv"
        src.write_text(col + "\n" + "\n".join(f"T{i:04d}" for i in range(n)) + "\n", encoding="utf-8")
        return src

    def test_writes_universe_csv(self, tmp_path):
        """Writes sp500_universe_full.csv with a 'ticker' header and one ticker per row."""
        src = self._make_source(tmp_path)
        out = tmp_path / "sp500_universe_full.csv"

        rc = usu.main(["--source-file", str(src), "--output", str(out), "--min-count", "1"])
        assert rc == 0
        assert out.exists()

        with open(out, newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))
        assert rows[0] == ["ticker"]
        tickers = [r[0] for r in rows[1:]]
        assert len(tickers) == 500

    def test_csv_tickers_are_normalized(self, tmp_path):
        """Written CSV contains uppercase, trimmed tickers without duplicates."""
        src = tmp_path / "source.csv"
        src.write_text("ticker\naapl\nAAPL\n  msft  \n", encoding="utf-8")
        out = tmp_path / "sp500_universe_full.csv"

        rc = usu.main(["--source-file", str(src), "--output", str(out), "--min-count", "1"])
        assert rc == 0

        with open(out, newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))
        tickers = [r[0] for r in rows[1:]]
        assert tickers == ["AAPL", "MSFT"]

    def test_writes_metadata_json(self, tmp_path):
        """Writes a companion _meta.json with required keys."""
        src = self._make_source(tmp_path)
        out = tmp_path / "sp500_universe_full.csv"
        meta_path = tmp_path / "sp500_universe_full_meta.json"

        rc = usu.main(["--source-file", str(src), "--output", str(out), "--min-count", "1"])
        assert rc == 0
        assert meta_path.exists()

        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        assert meta["source_type"] == "file"
        assert meta["ticker_count"] == 500
        assert isinstance(meta["first_10_tickers"], list)
        assert len(meta["first_10_tickers"]) == 10
        assert isinstance(meta["last_10_tickers"], list)
        assert "generated_at_utc" in meta

    def test_symbol_column_source_file(self, tmp_path):
        """Works with a source CSV using a 'Symbol' column header."""
        src = tmp_path / "source.csv"
        src.write_text("Symbol\nAAPL\nMSFT\nGOOGL\n", encoding="utf-8")
        out = tmp_path / "sp500_universe_full.csv"

        rc = usu.main(["--source-file", str(src), "--output", str(out), "--min-count", "1"])
        assert rc == 0

        with open(out, newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))
        tickers = [r[0] for r in rows[1:]]
        assert tickers == ["AAPL", "MSFT", "GOOGL"]

    def test_output_directory_created_if_missing(self, tmp_path):
        """Creates intermediate output directories as needed."""
        src = tmp_path / "source.csv"
        src.write_text("ticker\nAAPL\nMSFT\n", encoding="utf-8")
        out = tmp_path / "new_subdir" / "sp500_universe_full.csv"

        rc = usu.main(["--source-file", str(src), "--output", str(out), "--min-count", "1"])
        assert rc == 0
        assert out.exists()


# ---------------------------------------------------------------------------
# main() -- min-count validation
# ---------------------------------------------------------------------------

class TestMinCountValidation:
    """--min-count rejection when source is too small."""

    def test_rejects_source_below_default_min(self, tmp_path):
        """Returns non-zero exit code when source has fewer than 450 tickers."""
        src = tmp_path / "source.csv"
        src.write_text("ticker\nAAPL\nMSFT\n", encoding="utf-8")
        out = tmp_path / "sp500_universe_full.csv"

        rc = usu.main(["--source-file", str(src), "--output", str(out)])
        assert rc != 0
        assert not out.exists(), "Output file must not be written on validation failure"

    def test_passes_with_lowered_min_count(self, tmp_path):
        """Succeeds when --min-count is lowered to accommodate the source."""
        src = tmp_path / "source.csv"
        src.write_text("ticker\nAAPL\nMSFT\n", encoding="utf-8")
        out = tmp_path / "sp500_universe_full.csv"

        rc = usu.main(["--source-file", str(src), "--output", str(out), "--min-count", "2"])
        assert rc == 0
        assert out.exists()


# ---------------------------------------------------------------------------
# main() -- missing source file
# ---------------------------------------------------------------------------

class TestMissingSourceFile:
    """Error handling when --source-file path does not exist."""

    def test_nonexistent_source_file_returns_error(self, tmp_path):
        """Returns non-zero exit code when source file is not found."""
        out = tmp_path / "sp500_universe_full.csv"
        rc = usu.main([
            "--source-file", str(tmp_path / "does_not_exist.csv"),
            "--output", str(out),
        ])
        assert rc != 0


# ---------------------------------------------------------------------------
# main() -- --source-url (monkeypatched, no real network)
# ---------------------------------------------------------------------------

class TestSourceUrl:
    """--source-url path exercised via monkeypatched _fetch_csv_from_url."""

    def test_url_source_writes_csv(self, tmp_path, monkeypatch):
        """Fetched CSV text is parsed and written correctly when URL is supplied."""
        fake_csv = "ticker\n" + "\n".join(f"U{i:04d}" for i in range(500)) + "\n"
        monkeypatch.setattr(usu, "_fetch_csv_from_url", lambda url: fake_csv)

        out = tmp_path / "sp500_universe_full.csv"
        rc = usu.main([
            "--source-url", "https://example.invalid/sp500.csv",
            "--output", str(out),
            "--min-count", "1",
        ])
        assert rc == 0
        assert out.exists()

        with open(out, newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))
        assert rows[0] == ["ticker"]
        assert len(rows) == 501  # header + 500 tickers

    def test_url_metadata_records_source_type_url(self, tmp_path, monkeypatch):
        """Metadata JSON records source_type='url' for URL-sourced universes."""
        fake_csv = "ticker\n" + "\n".join(f"U{i}" for i in range(500)) + "\n"
        monkeypatch.setattr(usu, "_fetch_csv_from_url", lambda url: fake_csv)

        out = tmp_path / "sp500_universe_full.csv"
        rc = usu.main([
            "--source-url", "https://example.invalid/sp500.csv",
            "--output", str(out),
            "--min-count", "1",
        ])
        assert rc == 0
        meta_path = tmp_path / "sp500_universe_full_meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        assert meta["source_type"] == "url"
        assert meta["source_value"] == "https://example.invalid/sp500.csv"

    def test_url_fetch_failure_returns_error(self, tmp_path, monkeypatch):
        """Returns non-zero exit code when URL fetch raises an exception."""
        def _fail(url: str) -> str:
            raise OSError("Network unreachable")

        monkeypatch.setattr(usu, "_fetch_csv_from_url", _fail)
        out = tmp_path / "sp500_universe_full.csv"
        rc = usu.main([
            "--source-url", "https://example.invalid/sp500.csv",
            "--output", str(out),
        ])
        assert rc != 0
        assert not out.exists()

    def test_url_dry_run_no_files_written(self, tmp_path, monkeypatch):
        """--dry-run with --source-url writes nothing even on successful fetch."""
        fake_csv = "ticker\n" + "\n".join(f"U{i}" for i in range(500)) + "\n"
        monkeypatch.setattr(usu, "_fetch_csv_from_url", lambda url: fake_csv)

        out = tmp_path / "sp500_universe_full.csv"
        rc = usu.main([
            "--source-url", "https://example.invalid/sp500.csv",
            "--output", str(out),
            "--min-count", "1",
            "--dry-run",
        ])
        assert rc == 0
        assert not out.exists()
