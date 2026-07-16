"""
api/current_alpha_performance.py — Phase 13-I paper performance history loader.

Read-only loader for the Phase 13-I *historical daily mark backfill* artifacts
(``GET /v1/research/current-alpha/performance``). It reads ONLY the dynamic
reconstruction artifact written by the research runner under
``<daily-mark-dir>/backfill`` and normalizes it into a safe, preview-only payload the
"PAPER PERFORMANCE HISTORY" UI section renders.

What it shows (never combining Top-25 and Top-50):
    - the backfill decision + reconciliation status,
    - the observation date range + number of financial marks,
    - Top-25 / Top-50 / SPY cumulative curves, excess-return curves, drawdown curves,
    - per-book analytics (current return, current excess, max drawdown, positive-day %,
      outperform-SPY %, volatility, contributor concentration, information ratio),
    - the preliminary operating stability comparison (never a live-promotion).

Strict scope (read-only, paper-only — enforced):
    - It ONLY reads local JSON files written by the Phase 13-I runner. It writes no
      files, launches no subprocess, and touches no database.
    - It creates no orders, no signals, no trade decisions; connects to no broker; runs
      no automation; enables no live trading; and calls neither the prediction service
      nor any external / paid market-data provider.
    - The reconstruction is of FROZEN holdings — no reranking, no rebalancing. A short
      forward window is NOT alpha validation and promotes no book to live trading.
    - A missing / rejected backfill yields a controlled status (HTTP 200), never a crash.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional, Union

from paper_trader.api.current_alpha_preview import SAFETY_FLAGS
from paper_trader.api.current_alpha_book import (
    _iso_now,
    _read_json_file,
    _resolve_daily_mark_dir,
)

# ---------------------------------------------------------------------------
# Artifact location (env-overridable; never a secret)
# ---------------------------------------------------------------------------

#: Optional explicit override for the Phase 13-I backfill directory. When unset, the
#: backfill dir is ``<daily-mark-dir>/backfill`` (the research runner's default).
BACKFILL_DIR_ENV = "PAPER_TRADER_CURRENT_ALPHA_BACKFILL_DIR"
BACKFILL_SUBDIR = "backfill"

_MANIFEST = "backfill_manifest.json"
_SUMMARY = "paper_performance_summary.json"
_TOP25_HISTORY = "top25_daily_history.json"
_TOP50_HISTORY = "top50_daily_history.json"
_SPY_HISTORY = "spy_daily_history.json"

#: Reconstruction is a historical mark of frozen holdings — the required safety language.
PERFORMANCE_SAFETY_BADGES = (
    "HISTORICAL PAPER MARK RECONSTRUCTION",
    "FROZEN HOLDINGS",
    "NO DAILY REBALANCING",
    "PAPER TEST ONLY",
    "NO ORDERS",
    "NO BROKER",
    "NO AUTOMATION",
    "DOES NOT CREATE SIGNALS",
    "DOES NOT CREATE TRADE DECISIONS",
    "DOES NOT EXECUTE TRADES",
)

_REJECTED_DECISION = "BACKFILL_REJECTED_INTEGRITY_FAILURE"
_BLOCKED_PREFIX = "BLOCKED_"


def _resolve_backfill_dir(backfill_dir: Optional[Union[str, Path]],
                          mark_dir: Optional[Union[str, Path]]) -> Path:
    if backfill_dir is not None:
        return Path(backfill_dir)
    env_value = os.environ.get(BACKFILL_DIR_ENV)
    if env_value:
        return Path(env_value)
    return _resolve_daily_mark_dir(mark_dir) / BACKFILL_SUBDIR


def _safety_block() -> dict[str, Any]:
    return {
        "safety_badges": list(PERFORMANCE_SAFETY_BADGES),
        "safety": dict(SAFETY_FLAGS),
        **dict(SAFETY_FLAGS),
        "frozen_holdings": True,
        "daily_rebalancing": False,
        "reranking": False,
        "promotes_to_live": False,
        "order_action_all": "NO_ORDER",
    }


def _round(x: Any, nd: int = 4) -> Optional[float]:
    return round(x, nd) if isinstance(x, (int, float)) else None


def _drawdown_curve(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build the running-drawdown curve from a book's per-date cumulative returns
    (equity = 1 + cum/100; drawdown = equity/peak - 1)."""
    peak = None
    out: list[dict[str, Any]] = []
    for r in rows:
        cum = r.get("average_return_pct")
        if not isinstance(cum, (int, float)):
            out.append({"mark_date": r.get("mark_date"), "drawdown_pct": None})
            continue
        eq = 1.0 + cum / 100.0
        if peak is None or eq > peak:
            peak = eq
        dd = (eq / peak - 1.0) * 100.0 if peak else 0.0
        out.append({"mark_date": r.get("mark_date"), "drawdown_pct": _round(dd, 4)})
    return out


def _book_curves(history: Optional[dict[str, Any]]) -> dict[str, Any]:
    """Cumulative + excess + drawdown curves for one book (x-axis = mark date)."""
    rows = (history or {}).get("rows") or []
    cumulative = [{"mark_date": r.get("mark_date"),
                   "cumulative_return_pct": r.get("average_return_pct")} for r in rows]
    excess = [{"mark_date": r.get("mark_date"),
               "excess_return_vs_spy_pct_points": r.get("excess_return_vs_spy_pct_points")}
              for r in rows]
    return {
        "n_marks": len(rows),
        "cumulative_curve": cumulative,
        "excess_curve": excess,
        "drawdown_curve": _drawdown_curve(rows),
    }


def _spy_curve(history: Optional[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = (history or {}).get("rows") or []
    return [{"mark_date": r.get("mark_date"),
             "return_since_signal_pct": r.get("return_since_signal_pct")} for r in rows]


def _provenance(manifest: dict[str, Any], backfill_dir: Path) -> dict[str, Any]:
    return {
        "phase": manifest.get("phase"),
        "alpha_name": manifest.get("alpha_name"),
        "signal_date": manifest.get("signal_date"),
        "price_source": manifest.get("price_source"),
        "reference_today": manifest.get("reference_today"),
        "run_at": manifest.get("run_at"),
        "artifact_dir": str(backfill_dir),
        "reconstruction_kind": "HISTORICAL_MARK_TO_MARKET_FROZEN_HOLDINGS",
    }


def load_current_alpha_performance(
    *,
    backfill_dir: Optional[Union[str, Path]] = None,
    mark_dir: Optional[Union[str, Path]] = None,
) -> dict[str, Any]:
    """Read-only Phase 13-I paper performance history.

    Returns a controlled status in every case (never raises): NO_BACKFILL_YET when no
    artifact exists, BACKFILL_REJECTED when the reconstruction failed reconciliation (no
    analytics published), or PERFORMANCE_READY with the full analytics + curves.
    """
    loaded_at = _iso_now()
    bdir = _resolve_backfill_dir(backfill_dir, mark_dir)
    manifest, _err = _read_json_file(bdir / _MANIFEST)

    if not isinstance(manifest, dict):
        payload = {
            "status": "NO_BACKFILL_YET",
            "guidance": ("No Phase 13-I historical backfill artifact yet. Run "
                         "research/run_phase13i_historical_daily_mark_backfill.py to "
                         "reconstruct the frozen-book daily mark history (read-only EODHD)."),
            "backfill_decision": None,
            "reconciliation_status": None,
            "warnings": [],
            "loaded_at": loaded_at,
        }
        payload.update(_safety_block())
        return payload

    decision = manifest.get("decision")
    recon = manifest.get("reconciliation") or {}
    warnings: list[str] = []

    # --- rejected / blocked -> no analytics were published --------------------
    if decision == _REJECTED_DECISION or (isinstance(decision, str)
                                          and decision.startswith(_BLOCKED_PREFIX)):
        if decision == _REJECTED_DECISION:
            warnings.append("Backfill reconciliation FAILED integrity — analytics are not "
                            "published for this reconstruction.")
        else:
            warnings.append("Backfill was blocked (%s); no analytics were produced." % decision)
        payload = {
            "status": "BACKFILL_REJECTED" if decision == _REJECTED_DECISION else "BACKFILL_BLOCKED",
            "backfill_decision": decision,
            "reconciliation_status": recon.get("status") or decision,
            "reconciliation": recon,
            "blocked_message": manifest.get("blocked_message"),
            "provenance": _provenance(manifest, bdir),
            "warnings": warnings,
            "loaded_at": loaded_at,
        }
        payload.update(_safety_block())
        return payload

    # --- reconciled / warning -> analytics published --------------------------
    summary, _e1 = _read_json_file(bdir / _SUMMARY)
    top25_hist, _e2 = _read_json_file(bdir / _TOP25_HISTORY)
    top50_hist, _e3 = _read_json_file(bdir / _TOP50_HISTORY)
    spy_hist, _e4 = _read_json_file(bdir / _SPY_HISTORY)
    summary = summary if isinstance(summary, dict) else {}

    if decision == "BACKFILL_RECONCILIATION_WARNING":
        warnings.append("Backfill published with a reconciliation WARNING: %s"
                        % (recon.get("reason") or "see reconciliation detail."))

    stability = summary.get("stability_comparison") or {}
    payload = {
        "status": "PERFORMANCE_READY",
        "backfill_decision": decision,
        "reconciliation_status": recon.get("status") or decision,
        "reconciliation": recon,
        "latest_mark_date": manifest.get("backfill_end_date"),
        "backfill_start_date": manifest.get("backfill_start_date"),
        "observation_count": manifest.get("n_observations"),
        "benchmark": manifest.get("benchmark"),
        "top25_analytics": summary.get("top25"),
        "top50_analytics": summary.get("top50"),
        "spy_analytics": {
            "ticker": "SPY",
            "cumulative_return_pct": (manifest.get("benchmark") or {}).get(
                "latest_return_since_signal_pct"),
            "reference_date": (manifest.get("benchmark") or {}).get("reference_date"),
        },
        "stability_comparison": stability,
        "top25_curves": _book_curves(top25_hist),
        "top50_curves": _book_curves(top50_hist),
        "spy_curve": _spy_curve(spy_hist),
        "not_alpha_validation": summary.get("not_alpha_validation")
            or ("Historical mark of frozen holdings over a short forward window — not alpha "
                "validation; promotes no book to live trading."),
        "provenance": _provenance(manifest, bdir),
        "warnings": warnings,
        "loaded_at": loaded_at,
    }
    payload.update(_safety_block())
    return payload


__all__ = [
    "load_current_alpha_performance",
    "PERFORMANCE_SAFETY_BADGES",
    "BACKFILL_DIR_ENV",
]
