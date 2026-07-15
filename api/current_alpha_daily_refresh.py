"""
api/current_alpha_daily_refresh.py — Phase 13-G/H manual daily-refresh orchestrator.

This backs one EXPLICIT, user-triggered manual action ("RUN DAILY ALPHA REFRESH")
and one read-only status view for the champion ``composite_sn`` paper books:

    run_current_alpha_daily_refresh(...)  -> POST /v1/research/current-alpha/daily-refresh
    load_current_alpha_daily_status(...)  -> GET  /v1/research/current-alpha/daily-status

The manual action synchronously launches the Phase 13-G research runner (a live
READ-ONLY EODHD end-of-day mark refresh) as a subprocess, then marks the TOP 25 and
TOP 50 paper books against the resulting fresh mark artifact. It is NOT automation and
NOT scheduling: it runs once, only when the user clicks it.

Strict safety contract (enforced):
    - It launches the research runner with ``subprocess.run(argument_list,
      shell=False, ...)``. The EODHD API key is NEVER passed on the command line
      (the runner reads it from the inherited environment) and is never returned in
      the API response or logged here.
    - It creates NO orders, NO broker instructions, NO signals, NO trade decisions,
      and NO automation, and it writes NO Paper Trader database rows. The only writes
      are the local paper-book JSON store (on ``commit=True``) and the dynamic mark
      artifact under the D: data root (written by the research runner, outside git).
    - Every snapshot row carries ``order_action = NO_ORDER``.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional, Union

from paper_trader.api.current_alpha_preview import SAFETY_FLAGS
from paper_trader.api.current_alpha_book import (
    DAILY_MARK_DIR_ENV,
    DEFAULT_DAILY_MARK_DIR,
    MARK_SOURCE_DAILY,
    MARK_SOURCE_FALLBACK,
    CurrentAlphaPreviewError,
    _iso_now,
    _mark_freshness,
    _read_json_file,
    _resolve_book_dir,
    _resolve_daily_mark_dir,
    _today_iso,
    load_current_alpha_pnl_history,
    preview_or_create_current_alpha_book,
    snapshot_current_alpha_book,
)

# ---------------------------------------------------------------------------
# Configuration (env-overridable; never a secret)
# ---------------------------------------------------------------------------

RESEARCH_REPO_DIR_ENV = "PAPER_TRADER_RESEARCH_REPO_DIR"
DEFAULT_RESEARCH_REPO_DIR = Path(r"C:\Users\binis\Stock_Prediction_app_push")

RESEARCH_PYTHON_ENV = "PAPER_TRADER_RESEARCH_PYTHON"

#: Research-repo-relative paths (the runner + the Part A universe audit output).
DAILY_REFRESH_RUNNER_REL = Path("research") / "run_phase13g_daily_alpha_mark_refresh.py"
UNIVERSE_AUDIT_DIR_REL = (Path("research") / "output"
                          / "phase13g_current_alpha_universe_integrity_audit")
UNIVERSE_AUDIT_JSON = "phase13g_current_alpha_universe_integrity_audit.json"

#: Test seam: set to "0" / "off" to skip the subprocess launch (consume an existing
#: mark artifact instead). Never used in production.
LAUNCH_ENABLED_ENV = "PAPER_TRADER_DAILY_REFRESH_LAUNCH"

DEFAULT_TIMEOUT_SECONDS = 300

#: Books this manual action ensures + marks.
_BOOK_SIZES = (25, 50)

DAILY_REFRESH_SAFETY_BADGES = (
    "MANUAL DAILY REFRESH",
    "PAPER TEST ONLY",
    "NO ORDERS",
    "NO BROKER",
    "NO AUTOMATION",
    "DOES NOT CREATE SIGNALS",
    "DOES NOT CREATE TRADE DECISIONS",
    "DOES NOT EXECUTE TRADES",
)

#: Refresh-result enums that mean "do not add a paper snapshot".
_NO_SNAPSHOT_RESULTS = {
    "NO_NEW_MARK_DATE", "BLOCKED_EODHD_KEY", "BLOCKED_EODHD_ENTITLEMENT",
    "BLOCKED_EODHD_RATE_LIMIT", "BLOCKED_PROVIDER_ERROR", "BLOCKED_SCHEMA_ERROR",
}


# ---------------------------------------------------------------------------
# Configuration resolution
# ---------------------------------------------------------------------------

def _resolve_research_repo_dir(research_repo_dir: Optional[Union[str, Path]]) -> Path:
    if research_repo_dir is not None:
        return Path(research_repo_dir)
    env_value = os.environ.get(RESEARCH_REPO_DIR_ENV)
    return Path(env_value) if env_value else DEFAULT_RESEARCH_REPO_DIR


def _resolve_research_python(research_python: Optional[str]) -> str:
    """Resolve a local Python interpreter for the research runner. We never install
    packages; the runner only needs stdlib + the reused EODHD client."""
    if research_python:
        return str(research_python)
    env_value = os.environ.get(RESEARCH_PYTHON_ENV)
    if env_value:
        return env_value
    return sys.executable or "python"


def _audit_dir(research_repo_dir: Path) -> Path:
    return research_repo_dir / UNIVERSE_AUDIT_DIR_REL


def _safety_block() -> dict[str, Any]:
    return {
        "safety_badges": list(DAILY_REFRESH_SAFETY_BADGES),
        "safety": dict(SAFETY_FLAGS),
        **dict(SAFETY_FLAGS),
        "order_action_all": "NO_ORDER",
        "is_automation": False,
        "is_scheduled": False,
        "manual_user_triggered": True,
    }


# ---------------------------------------------------------------------------
# Subprocess launch (shell=False; key stays in the environment, never in argv)
# ---------------------------------------------------------------------------

def _build_refresh_command(research_python: str, runner_path: Path, mark_dir: Path,
                           audit_dir: Path) -> list[str]:
    """Build the argument list for the research runner. The EODHD API key is NEVER an
    argument — the runner reads it from the inherited environment."""
    return [
        research_python,
        str(runner_path),
        "--mark-dir", str(mark_dir),
        "--audit-dir", str(audit_dir),
        "--quiet",
    ]


def _launch_refresh(research_python: str, runner_path: Path, mark_dir: Path,
                    audit_dir: Path, cwd: Path, timeout: int) -> dict[str, Any]:
    """Run the research daily-mark refresh runner as a subprocess (shell=False).

    Returns a sanitized launch record (return code + a short stderr tail with any
    accidental key occurrence redacted). Raw stdout/stderr are NOT surfaced to the API.
    """
    cmd = _build_refresh_command(research_python, runner_path, mark_dir, audit_dir)
    if not runner_path.is_file():
        return {"launched": False, "returncode": None,
                "error": "research runner not found: %s" % runner_path}
    try:
        proc = subprocess.run(  # noqa: S603 - fixed argv, shell=False, no user input
            cmd, cwd=str(cwd), shell=False, capture_output=True, text=True,
            timeout=timeout, env=dict(os.environ),
        )
    except subprocess.TimeoutExpired:
        return {"launched": True, "returncode": None,
                "error": "research runner timed out after %ss" % timeout}
    except (OSError, ValueError) as exc:
        return {"launched": False, "returncode": None,
                "error": "could not launch research runner: %s" % type(exc).__name__}
    tail = _redact_secret((proc.stderr or "")[-400:])
    return {"launched": True, "returncode": proc.returncode, "stderr_tail": tail}


def _redact_secret(text: str) -> str:
    key = os.environ.get("EODHD_API_KEY") or ""
    return text.replace(key, "***REDACTED***") if key else text


# ---------------------------------------------------------------------------
# Read the fresh mark artifact
# ---------------------------------------------------------------------------

def _read_latest_manifest(mark_dir: Path) -> Optional[dict[str, Any]]:
    data, _err = _read_json_file(mark_dir / "latest" / "refresh_manifest.json")
    return data if isinstance(data, dict) else None


def _read_latest_book_summaries(mark_dir: Path) -> Optional[dict[str, Any]]:
    data, _err = _read_json_file(mark_dir / "latest" / "book_summaries.json")
    return data if isinstance(data, dict) else None


def _sanitized_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    """Surface only the safe, non-secret manifest fields to the API."""
    keys = ("phase", "refresh_result", "new_mark_date", "mark_date", "previous_mark_date",
            "reference_today", "alpha_name", "signal_date", "n_marks", "price_source",
            "universe", "benchmark_summary_preview", "book_summaries_preview",
            "last_refresh_run_at", "blocked", "blocked_message")
    return {k: manifest.get(k) for k in keys if k in manifest}


# ---------------------------------------------------------------------------
# Public service 1 — POST daily-refresh (manual, user-triggered)
# ---------------------------------------------------------------------------

def run_current_alpha_daily_refresh(
    *,
    commit: bool = False,
    book_dir: Optional[Union[str, Path]] = None,
    mark_dir: Optional[Union[str, Path]] = None,
    research_repo_dir: Optional[Union[str, Path]] = None,
    research_python: Optional[str] = None,
    package_dir: Optional[Union[str, Path]] = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    launcher: Optional[Callable[..., dict[str, Any]]] = None,
) -> dict[str, Any]:
    """Run the manual daily refresh: fetch fresh marks, then mark TOP 25 + TOP 50.

    ``commit=False`` (default) previews: it still runs the read-only market-data
    refresh (which writes the dynamic D: mark artifact — not a paper decision) but
    writes NO paper book or snapshot. ``commit=True`` also saves/ensures both books
    and appends one snapshot per book for the new price date.
    """
    run_at = _iso_now()
    repo = _resolve_research_repo_dir(research_repo_dir)
    py = _resolve_research_python(research_python)
    marks_root = _resolve_daily_mark_dir(mark_dir)
    audit = _audit_dir(repo)
    runner = repo / DAILY_REFRESH_RUNNER_REL
    warnings: list[str] = []

    # --- 1. run the refresh (unless the test seam disables the launch) --------
    launch: dict[str, Any]
    if launcher is not None:
        launch = launcher(py, runner, marks_root, audit, repo, timeout)
    elif os.environ.get(LAUNCH_ENABLED_ENV, "1").lower() in ("0", "false", "off"):
        launch = {"launched": False, "returncode": None,
                  "error": "launch disabled; consuming existing mark artifact"}
    else:
        launch = _launch_refresh(py, runner, marks_root, audit, repo, timeout)

    manifest = _read_latest_manifest(marks_root)
    if manifest is None:
        payload = {
            "status": "REFRESH_UNAVAILABLE",
            "action": "NO_SNAPSHOT",
            "refresh_result": None,
            "refresh": _sanitized_launch(launch),
            "snapshots": {},
            "guidance": (launch.get("error")
                         or "the daily mark refresh produced no manifest; check the "
                            "research runner and the EODHD entitlement."),
            "warnings": warnings,
            "loaded_at": run_at,
        }
        payload.update(_safety_block())
        return payload

    refresh_result = manifest.get("refresh_result")
    sanitized = _sanitized_manifest(manifest)

    # --- 2. decide whether to snapshot --------------------------------------
    if refresh_result in _NO_SNAPSHOT_RESULTS:
        reason = ("no new completed EOD price date (paper PnL was not advanced)"
                  if refresh_result == "NO_NEW_MARK_DATE"
                  else "the market-data refresh was blocked (%s)" % refresh_result)
        payload = {
            "status": refresh_result,
            "action": "NO_SNAPSHOT",
            "refresh_result": refresh_result,
            "refresh": _sanitized_launch(launch),
            "manifest": sanitized,
            "snapshots": {},
            "guidance": "No snapshot was added: %s." % reason,
            "warnings": warnings,
            "loaded_at": run_at,
        }
        payload.update(_safety_block())
        return payload

    # --- 3. mark TOP 25 + TOP 50 independently (by explicit book_id) ---------
    snapshots: dict[str, Any] = {}
    for size in _BOOK_SIZES:
        key = "top%d" % size
        try:
            created = preview_or_create_current_alpha_book(
                package_dir, book_size=size, commit=commit, book_dir=book_dir)
        except CurrentAlphaPreviewError as exc:
            snapshots[key] = {"status": "PACKAGE_UNAVAILABLE", "detail": str(exc)}
            warnings.append("TOP %d: %s" % (size, exc))
            continue
        book = created.get("book") or {}
        target_book_id = book.get("book_id")
        try:
            snap = snapshot_current_alpha_book(
                package_dir, commit=commit, book_dir=book_dir,
                book_id=target_book_id, book_size=size, daily_mark_dir=marks_root)
        except CurrentAlphaPreviewError as exc:
            snapshots[key] = {"status": "PACKAGE_UNAVAILABLE", "detail": str(exc)}
            warnings.append("TOP %d snapshot: %s" % (size, exc))
            continue
        snapshots[key] = _slim_snapshot(snap, target_book_id)

    payload = {
        "status": "DAILY_REFRESH_COMPLETE",
        "action": "SNAPSHOTS_WRITTEN" if commit else "SNAPSHOTS_PREVIEWED",
        "committed": bool(commit),
        "refresh_result": refresh_result,
        "mark_date": manifest.get("mark_date"),
        "previous_mark_date": manifest.get("previous_mark_date"),
        "refresh": _sanitized_launch(launch),
        "manifest": sanitized,
        "snapshots": snapshots,
        "warnings": warnings,
        "loaded_at": run_at,
    }
    payload.update(_safety_block())
    return payload


def _sanitized_launch(launch: dict[str, Any]) -> dict[str, Any]:
    return {
        "launched": launch.get("launched"),
        "returncode": launch.get("returncode"),
        "error": launch.get("error"),
        "stderr_tail": launch.get("stderr_tail"),
        "api_key_in_command_line": False,
        "shell": False,
    }


def _slim_snapshot(snap: dict[str, Any], book_id: Optional[str]) -> dict[str, Any]:
    inner = snap.get("snapshot") or {}
    cov = inner.get("coverage") or {}
    return {
        "book_id": snap.get("book_id") or book_id,
        "action": snap.get("action"),
        "status": snap.get("status"),
        "mark_source": snap.get("mark_source"),
        "as_of_price_date": snap.get("as_of_price_date"),
        "observation_date": snap.get("observation_date"),
        "mark_freshness_status": snap.get("mark_freshness_status"),
        "wrote_to_local_paper_store": snap.get("wrote_to_local_paper_store"),
        "average_return_pct": inner.get("average_return_pct"),
        "covered_count": cov.get("covered_count"),
        "total_count": cov.get("total_count"),
        "benchmark_return_pct": snap.get("benchmark_return_pct"),
        "excess_return_vs_spy_pct_points": snap.get("excess_return_vs_spy_pct_points"),
        "n_snapshots_after": snap.get("n_snapshots_after"),
        "order_action_all": "NO_ORDER",
    }


# ---------------------------------------------------------------------------
# Public service 2 — GET daily-status (read-only aggregator)
# ---------------------------------------------------------------------------

def _universe_identity(research_repo_dir: Path) -> dict[str, Any]:
    """Read the Phase 13-G Part A universe audit (validated universe + shadow)."""
    audit, _err = _read_json_file(_audit_dir(research_repo_dir) / UNIVERSE_AUDIT_JSON)
    if not isinstance(audit, dict):
        return {
            "available": False,
            "current_champion_universe": "unknown (run the Phase 13-G universe audit)",
            "is_strict_sp500": None,
            "sp500_shadow_available": False,
        }
    shadow = audit.get("sp500_shadow") or {}
    return {
        "available": True,
        "current_champion_universe": audit.get("validated_alpha_universe_name"),
        "universe_definition": audit.get("universe_definition"),
        "is_strict_sp500": audit.get("is_strict_sp500_universe"),
        "universe_decision": audit.get("decision"),
        "latest_ranked_count": audit.get("latest_ranked_count"),
        "confirmed_sp500": (audit.get("latest_cross_section_membership") or {}).get(
            "confirmed_sp500"),
        "sp500_shadow_available": bool(shadow),
        "sp500_shadow_decision": audit.get("sp500_shadow_decision"),
        "sp500_shadow": {
            "net_25bps": shadow.get("net_25bps"),
            "ic_t_63d": shadow.get("ic_t_63d"),
            "average_quarterly_return": shadow.get("average_quarterly_return"),
        } if shadow else None,
    }


def _history_status(book_dir: Optional[Union[str, Path]], size: int) -> dict[str, Any]:
    h = load_current_alpha_pnl_history(book_dir=book_dir, book_size=size)
    return {
        "book_size": size,
        "status": h.get("status"),
        "selected_book_id": h.get("selected_book_id"),
        "n_snapshots": h.get("n_snapshots"),
        "latest_price_mark_date": (h.get("latest_snapshot") or {}).get("as_of_price_date"),
    }


def load_current_alpha_daily_status(
    *,
    book_dir: Optional[Union[str, Path]] = None,
    mark_dir: Optional[Union[str, Path]] = None,
    research_repo_dir: Optional[Union[str, Path]] = None,
) -> dict[str, Any]:
    """Read-only daily operating status: latest mark, freshness, universe identity,
    TOP 25 / TOP 50 book summaries + SPY benchmark, and each book's history status."""
    repo = _resolve_research_repo_dir(research_repo_dir)
    marks_root = _resolve_daily_mark_dir(mark_dir)
    manifest = _read_latest_manifest(marks_root)
    summaries = _read_latest_book_summaries(marks_root)

    universe = _universe_identity(repo)
    top25_hist = _history_status(book_dir, 25)
    top50_hist = _history_status(book_dir, 50)

    if manifest is None:
        payload = {
            "status": "NO_DAILY_REFRESH_YET",
            "data_source": MARK_SOURCE_FALLBACK,
            "last_refresh_status": None,
            "last_refresh_run_at": None,
            "latest_completed_eod_date": None,
            "mark_freshness_status": "UNKNOWN_MARK_AGE",
            "universe_identity": universe,
            "top25": None,
            "top50": None,
            "spy_benchmark": None,
            "top25_history": top25_hist,
            "top50_history": top50_hist,
            "warnings": [
                "No Phase 13-G daily mark artifact yet. Click RUN DAILY ALPHA REFRESH "
                "to fetch fresh completed EOD prices and mark the paper books."
            ],
            "guidance": "Run the manual daily refresh to populate current marks.",
            "loaded_at": _iso_now(),
        }
        payload.update(_safety_block())
        return payload

    mark_date = manifest.get("mark_date")
    observation_date = _today_iso()
    mark_age, freshness = _mark_freshness(mark_date, observation_date)
    benchmark = (summaries or {}).get("benchmark") or manifest.get("benchmark_summary_preview")

    warnings: list[str] = []
    for size, book in (("top25", (summaries or {}).get("top25")),
                       ("top50", (summaries or {}).get("top50"))):
        if isinstance(book, dict) and book.get("coverage_status") == "INSUFFICIENT_COVERAGE_REJECT":
            warnings.append(
                "%s coverage is below 90%%: full-book PnL is not claimed for this mark."
                % size.upper())
    if freshness in ("STALE_MARK_WARNING", "STALE_MARK_REJECT"):
        warnings.append(
            "The latest completed EOD mark is %s calendar days old; run the daily "
            "refresh for a current mark." % mark_age)

    payload = {
        "status": "DAILY_STATUS_READY",
        "data_source": MARK_SOURCE_DAILY,
        "last_refresh_status": manifest.get("refresh_result"),
        "last_refresh_run_at": manifest.get("last_refresh_run_at"),
        "latest_completed_eod_date": mark_date,
        "mark_age_calendar_days": mark_age,
        "mark_freshness_status": freshness,
        "price_source": manifest.get("price_source"),
        "universe_identity": universe,
        "top25": (summaries or {}).get("top25"),
        "top50": (summaries or {}).get("top50"),
        "spy_benchmark": benchmark,
        "top25_history": top25_hist,
        "top50_history": top50_hist,
        "warnings": warnings,
        "loaded_at": _iso_now(),
    }
    payload.update(_safety_block())
    return payload


__all__ = [
    "run_current_alpha_daily_refresh",
    "load_current_alpha_daily_status",
    "DAILY_REFRESH_SAFETY_BADGES",
    "RESEARCH_REPO_DIR_ENV",
    "RESEARCH_PYTHON_ENV",
    "LAUNCH_ENABLED_ENV",
    "_build_refresh_command",
    "_resolve_research_python",
    "_resolve_research_repo_dir",
]
