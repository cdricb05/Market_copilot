"""
api/current_alpha_preview.py — Phase 13-B read-only current-alpha preview loader.

This module loads the committed *Phase 13-A current champion alpha paper-test
package* from the research repo and normalizes it into a safe, preview-only
payload that the Paper Trader endpoint (``GET /v1/research/current-alpha/preview``)
and the "Current Alpha Paper Test" UI panel render.

Phase 13-B scope (read-only, by design):
    - It ONLY reads the Phase 13-A package files (one JSON + eleven CSVs).
    - It returns a plain dict. It writes no files and touches no database.
    - It creates no signals, trade decisions, orders, or automation.
    - It never calls the prediction service or any external market-data provider,
      and requires no Nasdaq / Intrinio / FMP data.
    - Every value is copied verbatim from the package — nothing is faked. Only the
      six safety badges and the safety flags are enforced constants defined here.

Public API:
    load_current_alpha_preview(package_dir=None) -> dict
    CurrentAlphaPreviewError                       (raised on any contract violation)

Default package location (overridable via PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR):
    C:\\Users\\binis\\Stock_Prediction_app_push\\research\\output\\
        phase13a_current_champion_alpha_paper_test_package
"""
from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

# ---------------------------------------------------------------------------
# Package location
# ---------------------------------------------------------------------------

#: Environment variable that overrides the default package directory.
PACKAGE_DIR_ENV_VAR = "PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR"

#: Default Phase 13-A package directory (research repo, read-only).
DEFAULT_PACKAGE_DIR = Path(
    r"C:\Users\binis\Stock_Prediction_app_push\research\output"
    r"\phase13a_current_champion_alpha_paper_test_package"
)

#: The Phase 13-A package JSON file name.
PACKAGE_JSON_NAME = "phase13a_current_champion_alpha_paper_test_package.json"

#: All required package CSV side-cars. Loading fails if any is missing.
REQUIRED_CSV_FILES = (
    "current_alpha_full_ranked_universe.csv",
    "current_alpha_top25_candidates.csv",
    "current_alpha_top50_candidates.csv",
    "current_alpha_bottom25_avoid_list.csv",
    "current_alpha_sector_exposure.csv",
    "current_alpha_missing_data_report.csv",
    "current_alpha_paper_portfolio_top25.csv",
    "current_alpha_paper_portfolio_top50.csv",
    "current_alpha_tracking_template.csv",
    "current_alpha_risk_limits.csv",
    "current_alpha_go_no_go_scorecard.csv",
)

#: Every required package file (JSON + CSVs).
REQUIRED_PACKAGE_FILES = (PACKAGE_JSON_NAME,) + REQUIRED_CSV_FILES

# ---------------------------------------------------------------------------
# Contract constants
# ---------------------------------------------------------------------------

#: The package must be a Phase 13-A package or loading fails.
REQUIRED_PHASE = "13-A"

#: The champion alpha this package is built around (surfaced verbatim, asserted).
CHAMPION_SIGNAL = "composite_sn"

#: Always-on safety flags. Each value is a fixed guarantee, never derived from
#: package input. Positive guarantees are True; "does not do X" guarantees are
#: False. The endpoint and UI both surface these so the read-only, preview-only,
#: paper-test-only contract is machine-checkable.
SAFETY_FLAGS: dict[str, bool] = {
    "preview_only": True,
    "paper_test_only": True,
    "manual_review_only": True,
    "read_only": True,
    "no_orders": True,
    "no_broker": True,
    "no_automation": True,
    "creates_signals": False,
    "creates_trade_decisions": False,
    "wrote_to_paper_trader": False,
    "calls_prediction_service": False,
    "calls_external_providers": False,
    "uses_paid_data": False,
    "live_trading": False,
}

#: Safety badges the preview surfaces (Phase 13-B contract, exactly these six).
SAFETY_BADGES: tuple[str, ...] = (
    "PREVIEW ONLY",
    "NO ORDERS",
    "NO BROKER",
    "NO AUTOMATION",
    "MANUAL REVIEW ONLY",
    "PAPER TEST ONLY",
)


class CurrentAlphaPreviewError(RuntimeError):
    """Raised when the Phase 13-A package is missing, incomplete, or invalid."""


# ---------------------------------------------------------------------------
# Internal helpers (all read-only)
# ---------------------------------------------------------------------------

def _resolve_package_dir(package_dir: Optional[Union[str, Path]]) -> Path:
    """Resolve the package directory: explicit arg > env var > default."""
    if package_dir is not None:
        return Path(package_dir)
    env_value = os.environ.get(PACKAGE_DIR_ENV_VAR)
    if env_value:
        return Path(env_value)
    return DEFAULT_PACKAGE_DIR


def _resolve_json_path(base: Path) -> Optional[Path]:
    """Locate the package JSON (inside the package dir, or one level up)."""
    inside = base / PACKAGE_JSON_NAME
    if inside.is_file():
        return inside
    sibling = base.parent / PACKAGE_JSON_NAME
    if sibling.is_file():
        return sibling
    return None


def _require_files(base: Path) -> Path:
    """Validate the package and return the resolved JSON path.

    Raise CurrentAlphaPreviewError if the directory, the JSON, or any required
    CSV is absent — with a message the UI can surface directly.
    """
    if not base.is_dir():
        raise CurrentAlphaPreviewError(
            "Phase 13-A package not found. Run Phase 13-A in the research repo "
            f"first. (looked in: {base})"
        )
    missing = [name for name in REQUIRED_CSV_FILES if not (base / name).is_file()]
    json_path = _resolve_json_path(base)
    if json_path is None:
        missing.append(PACKAGE_JSON_NAME)
    if missing:
        raise CurrentAlphaPreviewError(
            "Phase 13-A package is incomplete; missing required file(s): "
            + ", ".join(sorted(missing))
            + ". Run Phase 13-A in the research repo first."
        )
    return json_path


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    """Read a CSV into a list of row dicts (read-only, stdlib csv)."""
    with open(path, newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _load_json(path: Path) -> dict[str, Any]:
    """Load the package JSON (read-only)."""
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _tickers(rows: list[dict[str, str]], limit: Optional[int] = None) -> list[str]:
    """Extract the ticker column from candidate rows (verbatim, in order)."""
    out = [str(r.get("ticker", "")).strip() for r in rows if r.get("ticker")]
    return out[:limit] if limit is not None else out


def _build_caveats(data: dict[str, Any]) -> list[str]:
    """Derive plain-English caveats from real package fields (never faked)."""
    caveats: list[str] = []

    thresholds = data.get("stale_thresholds") or {}
    days = data.get("days_since_signal")
    if data.get("stale_warning") and days is not None:
        caveats.append(
            f"Signal staleness: {days} days since signal date "
            f"{data.get('signal_date')} (warn > {thresholds.get('warn_days')}d, "
            f"reject > {thresholds.get('reject_days')}d) — a warning, not a rejection."
        )

    coverage = data.get("price_coverage") or {}
    if coverage:
        caveats.append(
            "Partial local price coverage: top25 "
            f"{coverage.get('top25')}/25, top50 {coverage.get('top50')}/50 — "
            "entry prices are only initialized for covered names."
        )

    sectors = data.get("sector_coverage") or {}
    unknown = sectors.get("Unknown")
    n_ranked = data.get("n_ranked")
    if unknown is not None and n_ranked:
        caveats.append(
            f"Weak sector metadata: {unknown} of {n_ranked} names are "
            "Unknown-sector; sector-neutrality and sector caps are approximate."
        )

    if data.get("spy_benchmark_available_locally") is False:
        caveats.append(
            "SPY benchmark not available locally; an equal-weight-universe "
            "reference is used instead."
        )

    benchmark_caveat = data.get("expected_benchmark_caveat")
    if isinstance(benchmark_caveat, str) and benchmark_caveat.strip():
        caveats.append(benchmark_caveat.strip())

    return caveats


# ---------------------------------------------------------------------------
# Public loader
# ---------------------------------------------------------------------------

def load_current_alpha_preview(
    package_dir: Optional[Union[str, Path]] = None,
) -> dict[str, Any]:
    """
    Load and normalize the Phase 13-A current-alpha paper-test package.

    Args:
        package_dir: Optional package directory. If omitted, falls back to the
            ``PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR`` environment variable and
            then to :data:`DEFAULT_PACKAGE_DIR`.

    Returns:
        A normalized, preview-only dict. Package values are copied verbatim;
        the six safety badges and the safety flags are enforced here.

    Raises:
        CurrentAlphaPreviewError: if the package directory or any required file
            is missing, or if the JSON is not a Phase 13-A package.
    """
    base = _resolve_package_dir(package_dir)
    json_path = _require_files(base)

    data = _load_json(json_path)

    # --- integrity gate: must be a real Phase 13-A package -------------------
    phase = data.get("phase")
    if phase != REQUIRED_PHASE:
        raise CurrentAlphaPreviewError(
            f"Package JSON is not a Phase {REQUIRED_PHASE} package "
            f"(phase={phase!r}). Run Phase 13-A in the research repo first."
        )
    decision = data.get("decision")
    if not isinstance(decision, str) or not decision.strip():
        raise CurrentAlphaPreviewError(
            "Phase 13-A package JSON has no decision string."
        )

    # --- parse the CSV side-cars (read-only) ---------------------------------
    top25 = _read_csv_rows(base / "current_alpha_top25_candidates.csv")
    top50 = _read_csv_rows(base / "current_alpha_top50_candidates.csv")
    bottom25 = _read_csv_rows(base / "current_alpha_bottom25_avoid_list.csv")
    sector_exposure = _read_csv_rows(base / "current_alpha_sector_exposure.csv")
    risk_limits = _read_csv_rows(base / "current_alpha_risk_limits.csv")
    scorecard = _read_csv_rows(base / "current_alpha_go_no_go_scorecard.csv")
    missing_data = _read_csv_rows(base / "current_alpha_missing_data_report.csv")

    source_file_paths = [str(json_path)] + [
        str(base / name) for name in REQUIRED_CSV_FILES
    ]

    payload: dict[str, Any] = {
        # --- identity / decision (verbatim from the package) -----------------
        "phase": phase,
        "alpha_name": data.get("champion_signal", CHAMPION_SIGNAL),
        "champion_definition": data.get("champion_definition"),
        "decision": decision,
        "decision_rationale": data.get("decision_rationale"),
        "go_no_go": data.get("go_no_go"),
        "go_no_go_note": data.get("go_no_go_note"),
        "signal_date": data.get("signal_date"),
        "cross_section_month": data.get("cross_section_month"),
        "cross_section_unit": data.get("cross_section_unit"),
        "n_ranked": data.get("n_ranked"),
        "holding_horizon_trading_days": data.get("holding_horizon_trading_days"),
        "rebalance_cadence": data.get("rebalance_cadence"),
        "next_rebalance_target": data.get("next_rebalance_target"),
        "weighting": data.get("weighting"),
        # --- freshness / coverage context ------------------------------------
        "package_date": data.get("package_date"),
        "days_since_signal": data.get("days_since_signal"),
        "stale_warning": data.get("stale_warning"),
        "stale_thresholds": data.get("stale_thresholds") or {},
        "price_coverage": data.get("price_coverage") or {},
        "sector_coverage": data.get("sector_coverage") or {},
        "spy_benchmark_available_locally": data.get(
            "spy_benchmark_available_locally"
        ),
        "expected_benchmark": data.get("expected_benchmark") or {},
        # --- candidate books (verbatim CSV rows) -----------------------------
        "top10_tickers": _tickers(top25, 10),
        "top25_candidates": top25,
        "top50_candidates": top50,
        "bottom25_avoid": bottom25,
        "sector_exposure": sector_exposure,
        "risk_limits": risk_limits,
        "go_no_go_scorecard": scorecard,
        "missing_data_report": missing_data,
        "caveats": _build_caveats(data),
        # --- enforced safety surface -----------------------------------------
        "safety_badges": list(SAFETY_BADGES),
        "safety": dict(SAFETY_FLAGS),
        # Mirror each safety flag at the top level so simple consumers do not
        # have to reach into the nested ``safety`` block.
        **dict(SAFETY_FLAGS),
        # --- provenance ------------------------------------------------------
        "source_file_paths": source_file_paths,
        "loaded_at": datetime.now(timezone.utc).isoformat(),
    }
    return payload


__all__ = [
    "CurrentAlphaPreviewError",
    "load_current_alpha_preview",
    "SAFETY_BADGES",
    "SAFETY_FLAGS",
    "REQUIRED_PACKAGE_FILES",
    "REQUIRED_CSV_FILES",
    "REQUIRED_PHASE",
    "CHAMPION_SIGNAL",
    "DEFAULT_PACKAGE_DIR",
    "PACKAGE_DIR_ENV_VAR",
]
