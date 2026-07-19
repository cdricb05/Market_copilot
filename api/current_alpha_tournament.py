"""
api/current_alpha_tournament.py — Phase 18 parallel champion-vs-challenger paper tournament.

Backs one read-only view and one EXPLICIT, user-triggered manual refresh for the parallel
paper tournament between the CURRENT PAPER CHAMPION (``composite_sn``) and the SECTOR-REPAIRED
PAPER CHALLENGER (``composite_sn_repaired``):

    load_current_alpha_tournament(...)     -> GET  /v1/research/current-alpha/tournament
    run_current_alpha_tournament_refresh() -> POST /v1/research/current-alpha/tournament/refresh

The GET is strictly read-only. It reads ONLY the owned Phase 18-A forward-test artifact
(produced offline by the research runner from owned EODHD data) plus the dedicated local
tournament tracking store, and normalizes them into a preview-only, four-book payload.

The POST is a MANUAL, idempotent advance of the dedicated local tournament store to the
latest completed COMMON financial mark date in the owned forward test. It writes ONLY to the
dedicated local tournament store (never PostgreSQL, never the champion's paper-book files),
advances only the union of the four frozen paper books plus SPY, and returns
``NO_NEW_COMPLETED_EOD_DATE`` when rerun without a newer mark.

Strict safety contract (enforced):
    - No orders, no signals, no trade decisions, no fills; no broker; no automation; no
      scheduling; no live trading; no champion replacement.
    - No Paper Trader database writes; no Paper Trader position/order mutation.
    - No prediction-service call and no external / paid provider call.
    - Every book row and snapshot carries ``order_action = NO_ORDER``. No status this module
      returns approves live trading; the strongest tournament outcome is eligibility for a
      later, explicit, MANUAL paper-champion decision.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional, Union

from paper_trader.api.current_alpha_preview import SAFETY_FLAGS
from paper_trader.api.current_alpha_book import (
    DEFAULT_DAILY_MARK_DIR,
    _atomic_write_json,
    _iso_now,
    _read_json_file,
    _resolve_daily_mark_dir,
)
from paper_trader.api.current_alpha_daily_refresh import _resolve_research_repo_dir

# ---------------------------------------------------------------------------
# Artifact + store locations (env-overridable; never a secret)
# ---------------------------------------------------------------------------

#: The owned Phase 18-A forward-test artifact dir (research/output; read-only here).
FORWARD_DIR_ENV = "PAPER_TRADER_CURRENT_ALPHA_TOURNAMENT_FORWARD_DIR"
FORWARD_REL = (Path("research") / "output" / "phase18a_parallel_challenger_forward_test")
FORWARD_REPORT = "phase18a_parallel_challenger_forward_test_report.json"

#: The DEDICATED local tournament tracking store (outside PostgreSQL, outside git; does NOT
#: overwrite the champion's paper-book files). Default: ``<daily-mark-dir>/tournament``.
TOURNAMENT_DIR_ENV = "PAPER_TRADER_CURRENT_ALPHA_TOURNAMENT_DIR"
TOURNAMENT_SUBDIR = "tournament"
_STATE_FILE = "tournament_state.json"
_SNAPSHOTS_FILE = "tournament_snapshots.json"

CHAMPION_SIGNAL = "composite_sn"
CHALLENGER_SIGNAL = "composite_sn_repaired"

#: Refresh confirmation token (the POST commit path requires an explicit manual request).
REFRESH_CONFIRM_TOKEN = "RUN_MANUAL_TOURNAMENT_REFRESH"

TOURNAMENT_SAFETY_BADGES = (
    "PARALLEL PAPER TOURNAMENT",
    "FROZEN BOOKS",
    "PAPER ONLY",
    "MANUAL REVIEW",
    "MANUAL REFRESH",
    "NO ORDERS",
    "NO BROKER",
    "NO AUTOMATION",
    "DOES NOT CREATE SIGNALS",
    "DOES NOT CREATE TRADE DECISIONS",
    "DOES NOT EXECUTE TRADES",
    "DOES NOT REPLACE THE CHAMPION",
    "NOT APPROVED FOR LIVE TRADING",
)

#: Decision enums surfaced from the forward test (Phase 18-A Part C).
ALLOWED_DECISIONS = (
    "MONITORING_MID_CYCLE", "CHECKPOINT_READY_FOR_REVIEW", "EXTEND_PARALLEL_PAPER_TEST",
    "KEEP_CURRENT_PAPER_CHAMPION", "CHALLENGER_PAPER_PROMOTION_ELIGIBLE",
    "REJECT_PAPER_CHALLENGER", "BLOCKED_INSUFFICIENT_COVERAGE", "BLOCKED_DATA_MISMATCH",
)
DEC_UNAVAILABLE = "TOURNAMENT_UNAVAILABLE"


# ---------------------------------------------------------------------------
# Location resolution
# ---------------------------------------------------------------------------

def _resolve_forward_dir(forward_dir: Optional[Union[str, Path]],
                         research_repo_dir: Optional[Union[str, Path]] = None) -> Path:
    if forward_dir is not None:
        return Path(forward_dir)
    env_value = os.environ.get(FORWARD_DIR_ENV)
    if env_value:
        return Path(env_value)
    return _resolve_research_repo_dir(research_repo_dir) / FORWARD_REL


def _resolve_tournament_dir(tournament_dir: Optional[Union[str, Path]],
                            mark_dir: Optional[Union[str, Path]] = None) -> Path:
    if tournament_dir is not None:
        return Path(tournament_dir)
    env_value = os.environ.get(TOURNAMENT_DIR_ENV)
    if env_value:
        return Path(env_value)
    base = _resolve_daily_mark_dir(mark_dir) if mark_dir is not None else DEFAULT_DAILY_MARK_DIR
    return Path(base) / TOURNAMENT_SUBDIR


def _safety_block(*, wrote_store: bool = False) -> dict[str, Any]:
    return {
        "safety_badges": list(TOURNAMENT_SAFETY_BADGES),
        "safety": dict(SAFETY_FLAGS),
        **dict(SAFETY_FLAGS),
        "order_action_all": "NO_ORDER",
        "champion_replaced": False,
        "replaces_champion": False,
        "mutates_champion": False,
        "promotes_to_live": False,
        "no_decision_approves_live_trading": True,
        "live_trading_status": "NOT_APPROVED_FOR_LIVE_TRADING",
        "creates_orders": False,
        "creates_signals": False,
        "creates_trade_decisions": False,
        "creates_fills": False,
        "wrote_to_database": False,
        "no_broker": True,
        "no_automation": True,
        "is_automation": False,
        "is_scheduled": False,
        "calls_prediction_service": False,
        # Honest, separate reporting of the local JSON store write (never the DB).
        "wrote_to_local_tournament_store": bool(wrote_store),
    }


# ---------------------------------------------------------------------------
# Forward-report normalization (read-only)
# ---------------------------------------------------------------------------

def _book_view(summary: Optional[dict[str, Any]]) -> dict[str, Any]:
    s = summary or {}
    return {
        "book_key": s.get("book_key"),
        "signal": s.get("signal"),
        "book_size": s.get("book_size"),
        "n_members": s.get("n_members"),
        "n_marks": s.get("n_marks"),
        "start_date": s.get("start_date"),
        "end_date": s.get("end_date"),
        "cumulative_return_pct": s.get("cumulative_return_pct"),
        "excess_return_vs_spy_pct_points": s.get("excess_return_vs_spy_pct_points"),
        "max_drawdown_pct": s.get("max_drawdown_pct"),
        "daily_volatility_pct_points": s.get("daily_volatility_pct_points"),
        "positive_day_rate_pct": s.get("positive_day_rate_pct"),
        "days_outperforming_spy_pct": s.get("days_outperforming_spy_pct"),
        "coverage_pct": s.get("coverage_pct"),
        "covered_count": s.get("covered_count"),
        "total_count": s.get("total_count"),
        "contributor_concentration_top5_pct": s.get("contributor_concentration_top5_pct"),
        "best_contributor": s.get("best_contributor"),
        "worst_contributor": s.get("worst_contributor"),
        "order_action_all": "NO_ORDER",
    }


def _load_store_state(tournament_dir: Path) -> dict[str, Any]:
    data, _err = _read_json_file(tournament_dir / _STATE_FILE)
    return data if isinstance(data, dict) else {}


def _attach_alignment(payload: dict[str, Any], static_report: Optional[dict[str, Any]],
                      tournament_dir: Optional[Union[str, Path]] = None) -> dict[str, Any]:
    """Best-effort: attach the Phase 19 alignment block + synced overlay to a GET payload.

    Imported lazily (the sync module imports this one) and wrapped so a read-only GET never
    breaks if the alignment cannot be computed — it simply omits the block. ``tournament_dir``
    is threaded so the synced overlay is scoped to the SAME store the caller resolved (keeps
    tests hermetic instead of reading the default D: store)."""
    try:
        from paper_trader.api.current_alpha_tournament_sync import attach_alignment
        return attach_alignment(payload, static_report=static_report, tournament_dir=tournament_dir)
    except Exception:  # noqa: BLE001 — alignment is additive; never break the read-only GET
        payload.setdefault("alignment", None)
        payload.setdefault("synced_tournament", {"available": False})
        return payload


def load_current_alpha_tournament(
    *,
    forward_dir: Optional[Union[str, Path]] = None,
    tournament_dir: Optional[Union[str, Path]] = None,
    research_repo_dir: Optional[Union[str, Path]] = None,
) -> dict[str, Any]:
    """Read-only Phase 18 tournament view.

    Reads the owned Phase 18-A forward-test report and the local tournament store and returns
    a controlled status in every case (never raises): TOURNAMENT_UNAVAILABLE when no forward
    artifact exists yet, otherwise the four-book side-by-side view with the forward-test
    decision, horizon progress, coverage warnings, risk flags and the exact next action.
    """
    loaded_at = _iso_now()
    fdir = _resolve_forward_dir(forward_dir, research_repo_dir)
    tdir = _resolve_tournament_dir(tournament_dir)
    report, _err = _read_json_file(fdir / FORWARD_REPORT)
    warnings: list[str] = []

    if not isinstance(report, dict):
        payload = {
            "phase": "18",
            "status": DEC_UNAVAILABLE,
            "decision": DEC_UNAVAILABLE,
            "guidance": ("No Phase 18-A forward-test artifact yet. Run "
                         "research/run_phase18a_parallel_challenger_forward_test.py to "
                         "reconstruct the four frozen paper books from owned data."),
            "current_paper_champion": {"signal": CHAMPION_SIGNAL},
            "sector_repaired_paper_challenger": {"signal": CHALLENGER_SIGNAL},
            "book_summaries": {}, "warnings": ["forward-test artifact not found"],
            "artifact_dir": str(fdir),
            "loaded_at": loaded_at,
        }
        payload.update(_safety_block())
        return _attach_alignment(payload, None, tournament_dir=tdir)

    decision = report.get("decision")
    if decision not in ALLOWED_DECISIONS:
        warnings.append("Forward-test decision %r is not a recognized tournament state."
                        % decision)

    summaries = report.get("book_summaries") or {}
    horizon = report.get("horizon_progress") or {}
    h25 = report.get("top25_head_to_head") or {}
    h50 = report.get("top50_head_to_head") or {}
    calendar = report.get("calendar") or {}
    spy = report.get("spy") or {}
    store = _load_store_state(tdir)

    payload = {
        "phase": "18",
        "status": decision,
        "decision": decision,
        "decision_reasons": report.get("decision_reasons") or [],
        "allowed_decisions": list(ALLOWED_DECISIONS),
        "current_paper_champion": {"signal": CHAMPION_SIGNAL,
                                   "label": "CURRENT PAPER CHAMPION"},
        "sector_repaired_paper_challenger": {"signal": CHALLENGER_SIGNAL,
                                             "label": "SECTOR-REPAIRED PAPER CHALLENGER"},
        # four book summaries
        "book_summaries": {
            "champion_top25": _book_view(summaries.get("champion_top25")),
            "challenger_top25": _book_view(summaries.get("challenger_top25")),
            "champion_top50": _book_view(summaries.get("champion_top50")),
            "challenger_top50": _book_view(summaries.get("challenger_top50")),
        },
        # aligned comparison dates + latest common mark
        "aligned_comparison": {
            "start_date": calendar.get("start_date"),
            "end_date": calendar.get("end_date"),
            "n_marks": calendar.get("n_marks"),
            "latest_common_financial_mark": horizon.get("latest_common_owned_eod_date"),
            "same_date_top25": h25.get("same_date_comparison"),
            "same_date_top50": h50.get("same_date_comparison"),
        },
        "latest_common_financial_mark": horizon.get("latest_common_owned_eod_date"),
        # SPY result
        "spy": {
            "available": spy.get("available"),
            "ticker": spy.get("ticker"),
            "cumulative_return_pct": spy.get("cumulative_return_pct"),
            "reference_date": spy.get("reference_date"),
            "price_source": spy.get("price_source"),
        },
        # Top25 / Top50 champion vs challenger comparison
        "top25_head_to_head": h25,
        "top50_head_to_head": h50,
        # compact aligned-date curves (cumulative / excess / drawdown per book + SPY)
        "daily_curves": report.get("daily_curves") or {},
        # per-book sector exposure (for the sector-exposure comparison)
        "sector_exposure": report.get("sector_exposure") or {},
        # horizon progress + review target + status
        "horizon_progress": horizon,
        "next_review_target": report.get("next_review_target"),
        "checkpoint_reached": horizon.get("checkpoint_reached"),
        "no_winner_before_checkpoint": not bool(horizon.get("checkpoint_reached")),
        # coverage / risk
        "coverage_warnings": report.get("coverage_warnings") or [],
        "risk_flags": report.get("risk_flags") or [],
        "reproduction": report.get("reproduction") or {},
        "book_isolation": report.get("book_isolation") or {},
        "entering_leaving_inherited_from_phase17":
            report.get("entering_leaving_inherited_from_phase17") or [],
        # next action + provenance
        "next_action": report.get("next_action"),
        "source_provenance": {
            "forward_artifact_dir": str(fdir),
            "forward_report": FORWARD_REPORT,
            "price_source": report.get("price_source"),
            "run_at": report.get("run_at"),
            "signal_date": report.get("signal_date"),
        },
        # local store state (last recorded refresh)
        "tournament_store": {
            "store_dir": str(tdir),
            "last_recorded_financial_date": store.get("last_recorded_financial_date"),
            "n_refreshes": store.get("n_refreshes", 0),
            "updated_at": store.get("updated_at"),
        },
        "warnings": warnings,
        "loaded_at": loaded_at,
    }
    payload.update(_safety_block())
    return _attach_alignment(payload, report, tournament_dir=tdir)


# ---------------------------------------------------------------------------
# Manual, idempotent refresh (writes ONLY the dedicated local store)
# ---------------------------------------------------------------------------

def _snapshot_from_report(report: dict[str, Any], latest_date: str) -> dict[str, Any]:
    summaries = report.get("book_summaries") or {}
    spy = report.get("spy") or {}
    return {
        "financial_mark_date": latest_date,
        "decision": report.get("decision"),
        "recorded_at": _iso_now(),
        "spy_cumulative_return_pct": spy.get("cumulative_return_pct"),
        "books": {k: _book_view(summaries.get(k)) for k in
                  ("champion_top25", "challenger_top25", "champion_top50", "challenger_top50")},
        "order_action_all": "NO_ORDER",
    }


def run_current_alpha_tournament_refresh(
    *,
    commit: bool = False,
    confirm: Optional[str] = None,
    forward_dir: Optional[Union[str, Path]] = None,
    tournament_dir: Optional[Union[str, Path]] = None,
    research_repo_dir: Optional[Union[str, Path]] = None,
) -> dict[str, Any]:
    """Manually advance the dedicated local tournament store to the latest completed COMMON
    financial mark date in the owned Phase 18-A forward test.

    ``commit=False`` (default) previews the advance and writes nothing. ``commit=True``
    requires ``confirm == REFRESH_CONFIRM_TOKEN`` and writes a single snapshot to the local
    store (append-only, keyed by financial date). Idempotent: a rerun with no newer completed
    mark returns ``NO_NEW_COMPLETED_EOD_DATE`` and writes nothing. Never writes PostgreSQL,
    never mutates portfolio positions/orders, never creates signals/decisions/orders/fills,
    and never invokes the prediction service.
    """
    run_at = _iso_now()
    fdir = _resolve_forward_dir(forward_dir, research_repo_dir)
    tdir = _resolve_tournament_dir(tournament_dir)
    report, _err = _read_json_file(fdir / FORWARD_REPORT)

    if not isinstance(report, dict):
        payload = {
            "status": "NO_FORWARD_TEST_YET", "action": "NO_SNAPSHOT",
            "guidance": ("No Phase 18-A forward-test artifact to refresh from. Run the "
                         "research runner first."),
            "committed": False, "wrote_store": False, "loaded_at": run_at,
        }
        payload.update(_safety_block())
        return payload

    horizon = report.get("horizon_progress") or {}
    latest_date = horizon.get("latest_common_owned_eod_date")
    store = _load_store_state(tdir)
    last_recorded = store.get("last_recorded_financial_date")

    # --- no completed EOD date at all ----------------------------------------
    if not latest_date:
        payload = {
            "status": "NO_COMPLETED_EOD_DATE", "action": "NO_SNAPSHOT",
            "guidance": "The forward test has no completed common EOD date to record.",
            "committed": False, "wrote_store": False,
            "last_recorded_financial_date": last_recorded, "loaded_at": run_at,
        }
        payload.update(_safety_block())
        return payload

    # --- idempotency: no newer completed financial mark ----------------------
    if last_recorded is not None and str(latest_date) <= str(last_recorded):
        payload = {
            "status": "NO_NEW_COMPLETED_EOD_DATE", "action": "NO_SNAPSHOT",
            "guidance": ("The tournament store is already at the latest completed common EOD "
                         "date (%s); no new snapshot was written." % last_recorded),
            "committed": False, "wrote_store": False,
            "latest_common_financial_mark": latest_date,
            "last_recorded_financial_date": last_recorded, "loaded_at": run_at,
        }
        payload.update(_safety_block())
        return payload

    # --- a genuinely newer mark ----------------------------------------------
    snapshot = _snapshot_from_report(report, str(latest_date))

    if not commit:
        payload = {
            "status": "TOURNAMENT_REFRESH_PREVIEW", "action": "SNAPSHOT_PREVIEWED",
            "guidance": ("A newer completed common EOD date (%s) is available. Preview only "
                         "— no store write. Confirm the manual refresh to record it."
                         % latest_date),
            "committed": False, "wrote_store": False,
            "latest_common_financial_mark": latest_date,
            "last_recorded_financial_date": last_recorded,
            "preview_snapshot": snapshot, "loaded_at": run_at,
        }
        payload.update(_safety_block())
        return payload

    # commit requires an explicit manual confirmation token
    if confirm != REFRESH_CONFIRM_TOKEN:
        payload = {
            "status": "REFRESH_CONFIRMATION_REQUIRED", "action": "NO_SNAPSHOT",
            "guidance": ("A store write requires an explicit manual confirmation "
                         "(confirm=%s)." % REFRESH_CONFIRM_TOKEN),
            "committed": False, "wrote_store": False,
            "latest_common_financial_mark": latest_date, "loaded_at": run_at,
        }
        payload.update(_safety_block())
        return payload

    # --- write ONLY the dedicated local tournament store ---------------------
    snapshots_data, _e2 = _read_json_file(tdir / _SNAPSHOTS_FILE)
    snapshots = (snapshots_data or {}).get("snapshots") if isinstance(snapshots_data, dict) else None
    snapshots = snapshots if isinstance(snapshots, list) else []
    # de-dup by financial date (idempotent even if the guard above is bypassed)
    snapshots = [s for s in snapshots if s.get("financial_mark_date") != str(latest_date)]
    snapshots.append(snapshot)
    snapshots.sort(key=lambda s: str(s.get("financial_mark_date") or ""))
    _atomic_write_json(tdir / _SNAPSHOTS_FILE,
                       {"alpha_tournament": "phase18a", "n_snapshots": len(snapshots),
                        "snapshots": snapshots, "order_action_all": "NO_ORDER"})
    new_state = {
        "last_recorded_financial_date": str(latest_date),
        "decision": report.get("decision"),
        "n_refreshes": int(store.get("n_refreshes", 0)) + 1,
        "n_snapshots": len(snapshots),
        "latest_snapshot": snapshot,
        "updated_at": run_at,
        "order_action_all": "NO_ORDER",
    }
    _atomic_write_json(tdir / _STATE_FILE, new_state)

    payload = {
        "status": "TOURNAMENT_REFRESH_COMPLETE", "action": "SNAPSHOT_WRITTEN",
        "guidance": ("Recorded the tournament snapshot for the latest completed common EOD "
                     "date (%s) in the dedicated local store." % latest_date),
        "committed": True, "wrote_store": True,
        "latest_common_financial_mark": latest_date,
        "last_recorded_financial_date": str(latest_date),
        "n_snapshots": len(snapshots), "store_dir": str(tdir),
        "snapshot": snapshot, "loaded_at": run_at,
    }
    payload.update(_safety_block(wrote_store=True))
    return payload


__all__ = [
    "load_current_alpha_tournament",
    "run_current_alpha_tournament_refresh",
    "TOURNAMENT_SAFETY_BADGES",
    "ALLOWED_DECISIONS",
    "REFRESH_CONFIRM_TOKEN",
    "FORWARD_DIR_ENV",
    "TOURNAMENT_DIR_ENV",
    "CHAMPION_SIGNAL",
    "CHALLENGER_SIGNAL",
]
