"""
api/research_candidate_preview.py — Phase 4-D read-only candidate preview service.

This module loads the Phase 4-B *non-production research candidate package* from
the research repo and normalizes it into a safe, preview-only payload that a
future Paper Trader endpoint (Phase 4-E) and UI panel (Phase 4-F) can render.

Phase 4-D scope (read-only, by design):
    - It ONLY reads the Phase 4-B package files (one JSON + nine CSVs).
    - It returns a plain dict. It writes no files and touches no database.
    - It adds NO FastAPI route and changes NO UI. Those come later (4-E / 4-F).

Hard guarantees enforced here (and asserted by the tests):
    - No order / broker / trade / automation logic.
    - No database session, no network call, no prediction-service call.
    - The candidate must carry the Phase 4-B readiness recommendation
      ``NONPROD_CANDIDATE_READY_FOR_PREVIEW_INTEGRATION`` or loading fails.
    - Every candidate value is copied verbatim from the package — never faked.

Public API:
    load_candidate_preview(package_dir=None) -> dict
    CandidatePreviewError                      (raised on any contract violation)

Default package location (overridable via PAPER_TRADER_CANDIDATE_PACKAGE_DIR):
    C:\\Users\\binis\\Stock_Prediction_app_push\\research\\output\\
        phase4b_nonproduction_candidate_package
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
PACKAGE_DIR_ENV_VAR = "PAPER_TRADER_CANDIDATE_PACKAGE_DIR"

#: Default Phase 4-B package directory (research repo, read-only).
DEFAULT_PACKAGE_DIR = Path(
    r"C:\Users\binis\Stock_Prediction_app_push\research\output"
    r"\phase4b_nonproduction_candidate_package"
)

#: The candidate package JSON file name.
PACKAGE_JSON_NAME = "phase4b_nonproduction_candidate_package.json"

#: All required package files. Loading fails if any is missing.
REQUIRED_PACKAGE_FILES = (
    PACKAGE_JSON_NAME,
    "candidate_summary_card.csv",
    "model_candidate_spec.csv",
    "selected_strategy_spec.csv",
    "evidence_scorecard.csv",
    "risk_guardrails.csv",
    "known_failure_modes.csv",
    "preview_integration_contract.csv",
    "no_go_items.csv",
    "readiness_decision_table.csv",
)

# ---------------------------------------------------------------------------
# Contract constants
# ---------------------------------------------------------------------------

#: The only Phase 4-B recommendation that may be surfaced as a preview.
REQUIRED_RECOMMENDATION = "NONPROD_CANDIDATE_READY_FOR_PREVIEW_INTEGRATION"

#: Fallback status if the candidate summary card omits it (it should not).
DEFAULT_STATUS = "RESEARCH_NONPRODUCTION_CANDIDATE"

#: Always-on safety flags. Every value is True and is never derived from input.
SAFETY_FLAGS: dict[str, bool] = {
    "preview_only": True,
    "nonproduction_candidate": True,
    "research_only": True,
    "no_orders": True,
    "no_broker_execution": True,
    "no_automation": True,
    "no_live_portfolio_weights": True,
    "manual_review_required": True,
}

#: Safety badges that the preview UI must display (Phase 4-C contract, 11 badges).
SAFETY_BADGES: tuple[str, ...] = (
    "PREVIEW ONLY",
    "NON-PRODUCTION CANDIDATE",
    "RESEARCH ONLY",
    "NO ORDERS",
    "NO BROKER EXECUTION",
    "NO AUTOMATION",
    "NO LIVE PORTFOLIO WEIGHTS",
    "MANUAL REVIEW REQUIRED",
    "OVERLAPPING LABEL WARNING",
    "SURVIVORSHIP BIAS WARNING",
    "2024 DRAWDOWN WARNING",
)


class CandidatePreviewError(RuntimeError):
    """Raised when the candidate package is missing, incomplete, or unsafe."""


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
    """Locate the package JSON.

    The Phase 4-B runner writes the side-car CSVs *inside*
    ``phase4b_nonproduction_candidate_package/`` but writes the package JSON one
    level up, as a sibling ``phase4b_nonproduction_candidate_package.json`` in
    ``research/output/``. Accept either location so the loader works against the
    real research layout as well as a self-contained fixture (JSON inside).
    """
    inside = base / PACKAGE_JSON_NAME
    if inside.is_file():
        return inside
    sibling = base.parent / PACKAGE_JSON_NAME
    if sibling.is_file():
        return sibling
    return None


def _require_files(base: Path) -> Path:
    """Validate the package and return the resolved JSON path.

    Raise CandidatePreviewError if the directory, the JSON, or any required CSV
    is absent.
    """
    if not base.is_dir():
        raise CandidatePreviewError(
            f"Candidate package directory not found: {base}"
        )
    missing = [
        name
        for name in REQUIRED_PACKAGE_FILES
        if name != PACKAGE_JSON_NAME and not (base / name).is_file()
    ]
    json_path = _resolve_json_path(base)
    if json_path is None:
        missing.append(PACKAGE_JSON_NAME)
    if missing:
        raise CandidatePreviewError(
            "Candidate package is incomplete; missing required file(s): "
            + ", ".join(sorted(missing))
        )
    return json_path


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    """Read a CSV into a list of row dicts (read-only, stdlib csv)."""
    with open(path, newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_kv_csv(path: Path, key_col: str, value_col: str) -> dict[str, str]:
    """Read a two-column 'field,value' style CSV into an ordered dict."""
    result: dict[str, str] = {}
    for row in _read_csv_rows(path):
        if key_col in row:
            result[row[key_col]] = row.get(value_col, "")
    return result


def _load_json(path: Path) -> dict[str, Any]:
    """Load the candidate package JSON (read-only)."""
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _extract_recommendation(data: dict[str, Any]) -> Optional[str]:
    """Pull the recommendation string from the (possibly nested) JSON block."""
    block = data.get("recommendation")
    if isinstance(block, dict):
        return block.get("recommendation")
    if isinstance(block, str):
        return block
    return None


# ---------------------------------------------------------------------------
# Public loader
# ---------------------------------------------------------------------------

def load_candidate_preview(
    package_dir: Optional[Union[str, Path]] = None,
) -> dict[str, Any]:
    """
    Load and normalize the Phase 4-B non-production candidate package.

    Args:
        package_dir: Optional package directory. If omitted, falls back to the
            ``PAPER_TRADER_CANDIDATE_PACKAGE_DIR`` environment variable and then
            to :data:`DEFAULT_PACKAGE_DIR`.

    Returns:
        A normalized, preview-only dict. Candidate values are copied verbatim
        from the package; safety badges and flags are enforced here.

    Raises:
        CandidatePreviewError: if the package directory or any required file is
            missing, if no no-go items are present, or if the candidate's
            recommendation is not ``NONPROD_CANDIDATE_READY_FOR_PREVIEW_INTEGRATION``.
    """
    base = _resolve_package_dir(package_dir)
    json_path = _require_files(base)

    data = _load_json(json_path)

    # --- safety gate: recommendation must be the preview-ready value ---------
    recommendation = _extract_recommendation(data)
    if recommendation != REQUIRED_RECOMMENDATION:
        raise CandidatePreviewError(
            "Candidate package recommendation is not preview-ready: "
            f"expected {REQUIRED_RECOMMENDATION!r}, got {recommendation!r}."
        )

    # --- parse the CSV side-cars (read-only) ---------------------------------
    summary_card = _read_kv_csv(base / "candidate_summary_card.csv", "field", "value")
    risk_guardrails = _read_csv_rows(base / "risk_guardrails.csv")
    known_failure_modes = _read_csv_rows(base / "known_failure_modes.csv")
    no_go_items = _read_csv_rows(base / "no_go_items.csv")

    # --- safety gate: no-go items must be present ----------------------------
    if not no_go_items:
        raise CandidatePreviewError(
            "Candidate package no_go_items.csv contains no enforced no-go items."
        )

    selected = data.get("selected_candidate") or {}

    payload: dict[str, Any] = {
        "candidate_id": data.get("candidate_id"),
        "candidate_name": data.get("candidate_name"),
        "model_name": selected.get("model_name"),
        "horizon": selected.get("horizon"),
        "strategy_name": selected.get("strategy_name"),
        "status": summary_card.get("status", DEFAULT_STATUS),
        "recommendation": recommendation,
        "evidence_summary": data.get("evidence_summary") or {},
        "selected_strategy_summary": selected,
        "risk_guardrails": risk_guardrails,
        "known_failure_modes": known_failure_modes,
        "no_go_items": no_go_items,
        "safety_badges": list(SAFETY_BADGES),
        # Nested safety block (kept for structured consumers).
        "safety": dict(SAFETY_FLAGS),
        # Top-level safety flags (each always True) so simple consumers and the
        # smoke load do not have to reach into the nested ``safety`` block.
        **{flag: True for flag in SAFETY_FLAGS},
        "source_files": [str(json_path)]
        + [
            str(base / name)
            for name in REQUIRED_PACKAGE_FILES
            if name != PACKAGE_JSON_NAME
        ],
        "loaded_at": datetime.now(timezone.utc).isoformat(),
    }
    return payload


__all__ = [
    "CandidatePreviewError",
    "load_candidate_preview",
    "SAFETY_BADGES",
    "SAFETY_FLAGS",
    "REQUIRED_PACKAGE_FILES",
    "REQUIRED_RECOMMENDATION",
    "DEFAULT_PACKAGE_DIR",
    "PACKAGE_DIR_ENV_VAR",
]
