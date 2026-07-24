"""api/alpha_target.py - Phase 27A.2 fresh alpha-target gate + owned-data target refresh.

One authoritative DATE CONTRACT for the operational alpha workflow, a HARD backend
confirmation gate for the Phase 25 paper-alpha snapshot, and the single manual
REFRESH ALPHA TARGET action that advances the owned Phase 25 alpha inputs to the
latest completed US market date.

Date contract (six independent dates - deliberately never collapsed into one):
  * latest_completed_market_date - clock-resolved latest COMPLETED US session
    (same rule as the Phase 15-A daily operating run).
  * alpha_market_date            - market date of the current owned alpha inputs
    (``current_momentum_scores.csv`` in the Phase 25 platform inputs store).
  * portfolio_valuation_date     - canonical Phase 14-C executed-portfolio mark date.
  * fundamental_as_of_date       - frozen fundamental panel as-of date (its OWN
    quarterly cadence; intentionally allowed to lag the market date).
  * snapshot_preview_date        - the market date the NEXT confirmed paper-alpha
    snapshot would be stamped with.
  * desk_mark_date               - latest completed close in the local paper-desk
    mark store.

Snapshot confirmation requires alpha_market_date == latest_completed_market_date
plus a complete 25-name primary target with reconciling weights, intact snapshot
ledger, and no duplicate of the latest confirmed snapshot. The gate is enforced
in the BACKEND confirm endpoint, never only by disabled UI buttons.

The refresh is strictly WITHIN the frozen-model contract: mom_6_1 uses only
month-end closes through the prior month, so an intramonth refresh NEVER changes
a momentum score, a model formula or a model weight - it advances the market
date and re-observes the liquidity / realized-vol / history diagnostics from the
owned EOD provider (the same existing owned-EODHD transport the desk and the
Phase 19 tournament sync already use). A refresh that would cross a month
boundary requires the research-side monthly input emitter (owned survivorship-
free pipeline) and is explicitly refused here rather than approximated.

This module NEVER calls the GCP prediction service, never uses the prediction
tunnel, never creates a trading instruction / fill / live signal / trade
decision, never touches PostgreSQL, never initializes the alpha book, and never
confirms a snapshot. Its only writes are the two owned Phase 25 current-state
input CSVs (+ manifest / refresh log) after the explicit refresh token.
"""
from __future__ import annotations

import csv
import json
import math
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import multi_horizon_ledger as ledger
from paper_trader.api import multi_horizon_registry as mreg
from paper_trader.api import portfolio_manager as _pm_mod
from paper_trader.api import portfolio_valuation
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api.current_alpha_tournament_sync import (
    TournamentSyncBlocked, _FATAL_BLOCKS, _classify_provider_error, _clean_symbol,
)
from paper_trader.api.daily_operating_run import latest_completed_market_date

PHASE = "27A.2"

PRIMARY_MODEL_ID = "fundamental_momentum_50_50_v1"
PRIMARY_BOOK_ID = "fundamental_momentum_50_50_top25"
REQUIRED_TARGET_COUNT = 25

REFRESH_CONFIRM_TOKEN = "CONFIRM_ALPHA_TARGET_REFRESH"

# Deterministic clock seams: module override for tests (mirrors the desk's
# _today_override convention) and an env override for fixture app instances.
NOW_ENV = "PAPER_TRADER_ALPHA_TARGET_NOW"
_now_override: Optional[datetime] = None

# Offline refresh fixture (JSON: symbol -> EOD payload). Mirrors the desk's
# MARKS_FIXTURE_ENV pattern so browser fixtures never touch the network.
REFRESH_FIXTURE_ENV = "PAPER_TRADER_ALPHA_TARGET_FIXTURE"

# --- confirmation blocker codes (exact, machine-readable) -------------------- #
B_ALPHA_STALE = "ALPHA_MARKET_DATE_BEHIND_LATEST_COMPLETED_MARKET_DATE"
B_PLATFORM_NOT_READY = "ALPHA_PLATFORM_NOT_READY"
B_INPUTS_MISSING = "MANDATORY_MODEL_INPUTS_MISSING"
B_TARGET_COUNT = "TARGET_COUNT_NOT_25"
B_WEIGHTS_INVALID = "TARGET_WEIGHTS_INVALID"
B_DUPLICATE = "DUPLICATE_OF_LATEST_CONFIRMED_SNAPSHOT"
B_LEDGER_INTEGRITY = "SNAPSHOT_LEDGER_INTEGRITY_FAILED"

ALL_BLOCKERS = [B_ALPHA_STALE, B_PLATFORM_NOT_READY, B_INPUTS_MISSING, B_TARGET_COUNT,
                B_WEIGHTS_INVALID, B_DUPLICATE, B_LEDGER_INTEGRITY]

# --- operator states ---------------------------------------------------------- #
STATE_STALE = "STALE_TARGET"
STATE_READY = "READY_TO_CONFIRM"
STATE_CONFIRMED = "CONFIRMED"
STATE_BLOCKED = "BLOCKED"

# --- fundamental freshness (its OWN cadence - never required to equal market) - #
FUND_FRESH = "FRESH_FOR_CADENCE"
FUND_STALE = "STALE_BEYOND_CADENCE"
FUND_UNKNOWN = "UNKNOWN"

# --- next manual actions ------------------------------------------------------ #
ACT_REFRESH = "REFRESH_ALPHA_TARGET"
ACT_PREVIEW_CONFIRM = "PREVIEW_THEN_CONFIRM_TARGET_SNAPSHOT"
ACT_PROCEED_BOOK = "PROCEED_TO_ALPHA_BOOK_PLAN"
ACT_RESOLVE = "RESOLVE_CONFIRMATION_BLOCKERS"

# --- refresh statuses --------------------------------------------------------- #
R_CONFIRM_REQUIRED = "ALPHA_TARGET_REFRESH_CONFIRM_REQUIRED"
R_ALREADY_FRESH = "ALPHA_TARGET_ALREADY_FRESH"
R_REFRESHED = "ALPHA_TARGET_REFRESHED"
R_MONTH_BOUNDARY = "ALPHA_TARGET_REFRESH_NEEDS_MONTHLY_INPUT_REBUILD"
R_PROVIDER_BLOCKED = "ALPHA_TARGET_REFRESH_PROVIDER_BLOCKED"
R_INPUTS_UNAVAILABLE = "ALPHA_TARGET_INPUTS_UNAVAILABLE"
R_INSUFFICIENT = "ALPHA_TARGET_REFRESH_INSUFFICIENT_COVERAGE"

# Refresh data-quality guards (all pure diagnostics of the owned fetch).
_MIN_COVERAGE_FRACTION = 0.60   # abort (write nothing) below this fresh-bar coverage
_VOL_WINDOW = 63
_ADV_WINDOW = 20
_HISTORY_WINDOW = 126
_MIN_HISTORY_OBS = 120
_BETA_WINDOW = 252
_DD_WINDOW = 252
_FETCH_CALENDAR_DAYS = 550      # >= ~380 trading days: covers beta/drawdown windows

_MOM_FIELDS = ["ticker", "mom_6_1", "is_member", "adv_dollar", "realized_vol_63d",
               "trailing_obs_126", "eligible_history", "extreme_flag", "sector",
               "market_as_of_date", "month_label"]

REFRESH_LOG_FILE = "alpha_target_refresh_log.json"

SAFETY_BADGES = ["LOCAL ALPHA DATA", "NO PREDICTION TUNNEL REQUIRED", "PREVIEW ONLY",
                 "NO ORDERS", "ORDERS DISABLED", "AUTOMATION OFF", "MANUAL REVIEW"]

Downloader = Callable[[str, str], Any]

# Injection seam for tests (the canonical valuation loader reads the DB).
_VALUATION_LOADER = portfolio_valuation.load_portfolio_valuation


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now() -> datetime:
    """Deterministic reference clock: module seam > env seam > real UTC now."""
    if _now_override is not None:
        return _now_override
    raw = os.environ.get(NOW_ENV)
    if raw:
        try:
            parsed = datetime.fromisoformat(raw)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def latest_completed() -> str:
    return latest_completed_market_date(_now()).isoformat()


def _safety(performed_write: bool = False) -> dict:
    return {
        "paper_only": True,
        "broker_enabled": False,
        "automation_enabled": False,
        "live_orders_enabled": False,
        "orders_enabled": False,
        "performed_write": bool(performed_write),
        "read_only": not performed_write,
        "manual_confirmation_required": True,
        "prediction_service_used": False,
        "prediction_tunnel_required": False,
        "local_alpha_data": True,
        "safety_badges": list(SAFETY_BADGES),
    }


def _valuation_date() -> tuple[Optional[str], list[str]]:
    """Canonical executed-portfolio mark date. Degrades to None, never raises."""
    try:
        v = _VALUATION_LOADER()
    except Exception as exc:  # noqa: BLE001
        return None, ["Portfolio valuation unavailable: %s" % str(exc)[:160]]
    cm = (v or {}).get("current_mark") or {}
    d = cm.get("as_of_market_date")
    return (str(d)[:10] if d else None), []


def _desk_mark_date() -> Optional[str]:
    try:
        return desk.marks_latest_date(desk.read_marks())
    except Exception:  # noqa: BLE001
        return None


# --------------------------------------------------------------------------- #
# Snapshot-ledger integrity (parse + append-only invariants; absent = intact)
# --------------------------------------------------------------------------- #
_VALID_SNAPSHOT_STATUSES = {ledger.STATUS_PREVIEW, ledger.STATUS_CONFIRMED,
                            ledger.STATUS_SKIPPED_DUPLICATE}


def ledger_integrity(ledger_dir=None) -> dict:
    sdir = ledger._ledger_dir(ledger_dir)
    path = sdir / ledger.SNAPSHOTS_FILE
    if not path.exists():
        return {"intact": True, "n_snapshots": 0, "issues": [],
                "note": "No snapshot ledger yet - an empty ledger is intact."}
    issues: list[str] = []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            obj = json.load(fh)
    except (OSError, ValueError) as exc:
        return {"intact": False, "n_snapshots": 0,
                "issues": ["Snapshot ledger unreadable: %s" % str(exc)[:120]]}
    snaps = obj.get("snapshots") if isinstance(obj, dict) else obj
    if not isinstance(snaps, list):
        return {"intact": False, "n_snapshots": 0,
                "issues": ["Snapshot ledger has no snapshots list."]}
    seen: set[str] = set()
    for i, s in enumerate(snaps):
        if not isinstance(s, dict):
            issues.append("Snapshot #%d is not an object." % i)
            continue
        sid = s.get("snapshot_id")
        if not sid:
            issues.append("Snapshot #%d has no snapshot_id." % i)
        elif sid in seen:
            issues.append("Duplicate snapshot_id %s (append-only violation)." % sid)
        else:
            seen.add(sid)
        if s.get("confirmation_status") not in _VALID_SNAPSHOT_STATUSES:
            issues.append("Snapshot %s has unknown confirmation_status %r."
                          % (sid, s.get("confirmation_status")))
        if s.get("confirmation_status") == ledger.STATUS_CONFIRMED and not s.get("market_as_of_date"):
            issues.append("Confirmed snapshot %s has no market_as_of_date." % sid)
    return {"intact": not issues, "n_snapshots": len(snaps), "issues": issues}


def _weights_valid(book: dict) -> tuple[bool, str]:
    """Equal-weight book reconciliation: positive weight, individual cap respected,
    weights + unallocated cash sum to 1, every constituent at the same weight."""
    w = book.get("equal_weight")
    n = book.get("size_actual") or 0
    unalloc = book.get("unallocated_weight") or 0.0
    cap = book.get("max_individual_weight_cap") or 1.0
    if w is None or w <= 0:
        return False, "Equal weight is missing or non-positive."
    if w > cap + 1e-9:
        return False, "Equal weight %.4f exceeds the %.2f individual cap." % (w, cap)
    if abs(w * n + unalloc - 1.0) > 1e-6:
        return False, ("Weights do not reconcile: %d x %.6f + %.6f cash != 1.0"
                       % (n, w, unalloc))
    for c in book.get("constituents") or []:
        if c.get("weight") != w:
            return False, "Constituent %s weight differs from the equal weight." % c.get("ticker")
    return True, "%d x %.4f + %.4f residual cash = 100%%" % (n, w, unalloc)


# --------------------------------------------------------------------------- #
# Workstream A - the authoritative readiness / date contract
# --------------------------------------------------------------------------- #
def compute_readiness(*, panel_path=None, inputs_dir=None, ledger_dir=None) -> dict:
    cur = eng.build_current(panel_path=panel_path, inputs_dir=inputs_dir)
    ready = cur.get("status") == eng.STATUS_READY
    lcd = latest_completed()
    amd = (cur.get("market_as_of_date") or None) if ready else None
    if amd:
        amd = str(amd)[:10]
    vdate, val_warnings = _valuation_date()
    warnings = list(cur.get("warnings", [])) + val_warnings

    dates = {
        "latest_completed_market_date": lcd,
        "alpha_market_date": amd,
        "portfolio_valuation_date": vdate,
        "fundamental_as_of_date": cur.get("fundamental_as_of_date") if ready else None,
        "snapshot_preview_date": amd,
        "desk_mark_date": _desk_mark_date(),
    }
    alpha_aligned = bool(amd) and amd == lcd
    portfolio_aligned = bool(vdate) and vdate == lcd

    blockers: list[str] = []
    details: list[dict] = []

    def _block(code: str, message: str) -> None:
        blockers.append(code)
        details.append({"code": code, "message": message})

    integrity = ledger_integrity(ledger_dir)
    duplicate = False
    target_count = None
    weights_ok = None
    weights_note = None

    if not ready:
        _block(B_PLATFORM_NOT_READY,
               "The Phase 25 alpha platform is not ready (%s)." % cur.get("status"))
        v = cur.get("inputs", {}).get("validations", {})
        missing = [name for name, key in (("fundamental panel", "fundamental_available"),
                                          ("momentum scores", "momentum_available"))
                   if not v.get(key)]
        if missing:
            _block(B_INPUTS_MISSING,
                   "Mandatory owned model inputs missing: %s." % ", ".join(missing))
    else:
        if amd is None:
            _block(B_INPUTS_MISSING,
                   "The owned momentum input carries no market_as_of_date.")
        elif amd < lcd:
            _block(B_ALPHA_STALE,
                   "The alpha target market date (%s) is behind the latest completed "
                   "market date (%s). Refresh the alpha target from owned local data "
                   "before confirming." % (amd, lcd))
        book = cur["books"]["books"].get(PRIMARY_BOOK_ID)
        if book is None:
            _block(B_PLATFORM_NOT_READY, "The primary target book %s is missing."
                   % PRIMARY_BOOK_ID)
        else:
            target_count = book.get("size_actual")
            if target_count != REQUIRED_TARGET_COUNT:
                _block(B_TARGET_COUNT,
                       "The primary target holds %s names; the complete validated "
                       "target is exactly %d." % (target_count, REQUIRED_TARGET_COUNT))
            weights_ok, weights_note = _weights_valid(book)
            if not weights_ok:
                _block(B_WEIGHTS_INVALID, "Target weights invalid: %s" % weights_note)
        if not integrity["intact"]:
            _block(B_LEDGER_INTEGRITY,
                   "Snapshot ledger integrity failed: %s"
                   % "; ".join(integrity["issues"][:3]))
        prior = ledger.latest_confirmed_by_sleeve(ledger_dir).get(mreg.SLEEVE_COMBINED) or {}
        if prior and book is not None:
            fp_prior = ledger._book_fingerprint(prior.get("constituents_top25") or [])
            fp_cur = ledger._book_fingerprint([c["ticker"] for c in book["constituents"]])
            duplicate = (prior.get("market_as_of_date") == amd and fp_prior == fp_cur)
            if duplicate:
                _block(B_DUPLICATE,
                       "The latest confirmed snapshot already covers market date %s "
                       "with this identical primary book - confirming again would "
                       "duplicate it." % amd)

    fund_month = cur.get("fundamental_month") if ready else None
    if not ready:
        fund_fresh = FUND_UNKNOWN
    else:
        fund_fresh = FUND_STALE if eng._is_fundamental_stale(fund_month, amd) else FUND_FRESH

    if duplicate and all(b == B_DUPLICATE for b in blockers):
        state, next_action = STATE_CONFIRMED, ACT_PROCEED_BOOK
    elif B_ALPHA_STALE in blockers:
        state, next_action = STATE_STALE, ACT_REFRESH
    elif blockers:
        state, next_action = STATE_BLOCKED, ACT_RESOLVE
    else:
        state, next_action = STATE_READY, ACT_PREVIEW_CONFIRM

    return {
        "status": "ALPHA_TARGET_READINESS_READY",
        "phase": PHASE,
        "state": state,
        "required_next_action": next_action,
        "dates": dates,
        "alpha_market_aligned": alpha_aligned,
        "portfolio_mark_aligned": portfolio_aligned,
        "fundamental_freshness_status": fund_fresh,
        "fundamental_cadence_note": (
            "Fundamental data follows its OWN quarterly cadence; it is allowed to be "
            "older than the market date and is only flagged when it lags by more "
            "than one quarter."),
        "snapshot_confirmation_allowed": not blockers,
        "confirmation_blockers": blockers,
        "blocker_details": details,
        "primary_model_id": PRIMARY_MODEL_ID,
        "primary_book_id": PRIMARY_BOOK_ID,
        "required_target_count": REQUIRED_TARGET_COUNT,
        "target_count": target_count,
        "weights_reconcile": weights_ok,
        "weights_note": weights_note,
        "ledger_integrity": integrity,
        "already_confirmed_for_current_target": duplicate,
        "refresh_required_token": REFRESH_CONFIRM_TOKEN,
        "confirm_required_token": ledger.CONFIRM_TOKEN,
        "warnings": warnings,
        "loaded_at": _iso_now(),
    }


def load_readiness(*, panel_path=None, inputs_dir=None, ledger_dir=None) -> dict:
    out = compute_readiness(panel_path=panel_path, inputs_dir=inputs_dir,
                            ledger_dir=ledger_dir)
    out.update(_safety(False))
    return out


# --------------------------------------------------------------------------- #
# Workstream D - the single operational snapshot-review payload
# --------------------------------------------------------------------------- #
def _risk_status(vol: Optional[float]) -> str:
    if vol is None:
        return "NO_RISK_DATA"
    return "REVIEW" if vol > _pm_mod._VOL_REVIEW_THRESHOLD else "OK"


def _row_reason(agreement: Optional[str], rank: int) -> str:
    base = "Combined rank %d of the fixed 50/50 blend." % rank
    phrase = _pm_mod._AGREEMENT_PHRASE.get(agreement)
    return base + (" " + phrase if phrase else "")


def load_review(*, panel_path=None, inputs_dir=None, ledger_dir=None) -> dict:
    readiness = compute_readiness(panel_path=panel_path, inputs_dir=inputs_dir,
                                  ledger_dir=ledger_dir)
    cur = eng.build_current(panel_path=panel_path, inputs_dir=inputs_dir)
    if cur.get("status") != eng.STATUS_READY:
        return {"status": "ALPHA_TARGET_REVIEW_UNAVAILABLE", "phase": PHASE,
                "readiness": readiness, "warnings": cur.get("warnings", []),
                "loaded_at": _iso_now(), **_safety(False)}

    book = cur["books"]["books"].get(PRIMARY_BOOK_ID) or {}
    combined = cur["combined"]["combined"]
    risk = cur.get("inputs", {}).get("risk", {})
    prior = ledger.latest_confirmed_by_sleeve(ledger_dir)
    rec = eng.compute_recommendations(cur, prior, mreg.SLEEVE_COMBINED,
                                      size=REQUIRED_TARGET_COUNT)

    # -- target table -------------------------------------------------------- #
    rows = []
    vols, dds = [], []
    n_liq_fail = n_liq_unknown = 0
    for c in book.get("constituents") or []:
        tk = c["ticker"]
        cb = combined.get(tk) or {}
        rk = risk.get(tk) or {}
        vol = rk.get("realized_vol_63d")
        dd = rk.get("max_drawdown_252d")
        if vol is not None:
            vols.append(vol)
        if dd is not None:
            dds.append(dd)
        adv = c.get("adv_dollar")
        if adv is None:
            n_liq_unknown += 1
        elif adv < eng.MIN_ADV_DOLLAR:
            n_liq_fail += 1
        agreement = _pm_mod._agreement(cb.get("fund_percentile"), cb.get("mom_percentile"))
        rows.append({
            "rank": c.get("rank"),
            "ticker": tk,
            "target_weight": c.get("weight"),
            "fund_rank": cb.get("fund_rank"),
            "mom_rank": cb.get("mom_rank"),
            "agreement": agreement,
            "sector": c.get("sector"),
            "risk_status": _risk_status(vol),
            "reason": _row_reason(agreement, c.get("rank") or 0),
        })

    # -- target summary ------------------------------------------------------ #
    exposure = book.get("sector_exposure") or {}
    largest_sector = max(exposure.items(), key=lambda kv: kv[1])[0] if exposure else None
    n_vol_review = sum(1 for v in vols if v > _pm_mod._VOL_REVIEW_THRESHOLD)
    summary = {
        "target_count": book.get("size_actual"),
        "required_target_count": REQUIRED_TARGET_COUNT,
        "target_weight_per_name": book.get("equal_weight"),
        "unallocated_weight": book.get("unallocated_weight"),
        "estimated_turnover": rec.get("estimated_turnover"),
        "estimated_transaction_cost": rec.get("estimated_transaction_cost"),
        "largest_sector": largest_sector,
        "largest_sector_weight": exposure.get(largest_sector) if largest_sector else None,
        "max_sector_cap_fraction": book.get("sector_cap_fraction"),
        "max_per_sector": book.get("max_per_sector"),
        "sector_exposure": exposure,
        "liquidity": {
            "min_adv_dollar": eng.MIN_ADV_DOLLAR,
            "all_pass": n_liq_fail == 0,
            "n_below_minimum": n_liq_fail,
            "n_unknown": n_liq_unknown,
        },
        "volatility_review": {
            "threshold": _pm_mod._VOL_REVIEW_THRESHOLD,
            "n_above_threshold": n_vol_review,
            "n_with_data": len(vols),
            "status": "REVIEW" if n_vol_review else ("OK" if vols else "NO_RISK_DATA"),
        },
        "drawdown": {
            "mean_max_drawdown_252d": (round(sum(dds) / len(dds), 6) if dds else None),
            "worst_max_drawdown_252d": (min(dds) if dds else None),
            "n_with_data": len(dds),
        },
        "construction_exclusions": {
            "sector_capped_out": book.get("sector_capped_out") or [],
            "explanation": (
                "Higher-ranked names skipped by the validated %d%% sector concentration "
                "cap during deterministic book construction; the engine refills the "
                "book from the next-ranked eligible names."
                % int(round(100 * (book.get("sector_cap_fraction") or 0)))),
        },
    }

    # -- approval checklist -------------------------------------------------- #
    r = readiness

    def _item(key: str, label: str, ok: bool, detail: str) -> dict:
        return {"key": key, "item": label, "status": "PASS" if ok else "FAIL",
                "detail": detail}

    checklist = [
        _item("market_data", "Latest market data acquired",
              r["alpha_market_aligned"],
              "alpha %s vs latest completed %s" % (r["dates"]["alpha_market_date"],
                                                   r["dates"]["latest_completed_market_date"])),
        _item("recalculated", "Alpha target recalculated", True,
              "Deterministic Phase 25 build at %s (frozen formulas, fixed 50/50)."
              % r["dates"]["alpha_market_date"]),
        _item("count", "Target count = %d" % REQUIRED_TARGET_COUNT,
              summary["target_count"] == REQUIRED_TARGET_COUNT,
              "%s of %d names" % (summary["target_count"], REQUIRED_TARGET_COUNT)),
        _item("weights", "Weights reconcile", bool(r["weights_reconcile"]),
              r["weights_note"] or "-"),
        _item("sector_cap", "Sector cap passed",
              all(v <= (book.get("sector_cap_fraction") or 1.0) + 1e-9
                  for k, v in exposure.items() if k != "Unknown"),
              "largest sector %s at %s" % (largest_sector,
                                           exposure.get(largest_sector))),
        _item("liquidity", "Liquidity passed", summary["liquidity"]["all_pass"],
              "$%dM minimum ADV; %d below, %d unknown"
              % (int(eng.MIN_ADV_DOLLAR / 1e6), n_liq_fail, n_liq_unknown)),
        _item("ledger", "Ledger integrity passed", r["ledger_integrity"]["intact"],
              "%d snapshots on file" % r["ledger_integrity"]["n_snapshots"]),
        _item("no_orders", "No orders created", True,
              "This review reads owned data only; nothing was created."),
        _item("no_broker", "No broker", True, "No broker is connected."),
        _item("automation_off", "Automation off", True,
              "Every step is an explicit manual action."),
    ]

    return {
        "status": "ALPHA_TARGET_REVIEW_READY",
        "phase": PHASE,
        "state": r["state"],
        "required_next_action": r["required_next_action"],
        "readiness": r,
        "target_summary": summary,
        "approval_checklist": checklist,
        "checklist_all_pass": all(c["status"] == "PASS" for c in checklist),
        "target_table": rows,
        "warnings": r["warnings"],
        "loaded_at": _iso_now(),
        **_safety(False),
    }


# --------------------------------------------------------------------------- #
# Workstream C - the hard confirmation gate (called by the confirm ENDPOINT)
# --------------------------------------------------------------------------- #
def confirmation_gate(*, panel_path=None, inputs_dir=None, ledger_dir=None) -> Optional[dict]:
    """Return the structured SNAPSHOT_CONFIRMATION_BLOCKED payload, or None when
    confirmation may proceed to the existing manual token gate."""
    r = compute_readiness(panel_path=panel_path, inputs_dir=inputs_dir,
                          ledger_dir=ledger_dir)
    if r["snapshot_confirmation_allowed"]:
        return None
    return {
        "status": "SNAPSHOT_CONFIRMATION_BLOCKED",
        "phase": PHASE,
        "performed_write": False,
        "confirmation_blockers": r["confirmation_blockers"],
        "blocker_details": r["blocker_details"],
        "required_next_action": (ACT_REFRESH if B_ALPHA_STALE in r["confirmation_blockers"]
                                 else r["required_next_action"]),
        "state": r["state"],
        "dates": r["dates"],
        "alpha_market_aligned": r["alpha_market_aligned"],
        "loaded_at": _iso_now(),
        **_safety(False),
    }


# --------------------------------------------------------------------------- #
# Workstream B - REFRESH ALPHA TARGET (owned data only; explicit token)
# --------------------------------------------------------------------------- #
def _fixture_downloader(fixture_path: Path) -> Downloader:
    try:
        with open(fixture_path, "r", encoding="utf-8") as fh:
            table = json.load(fh)
    except (OSError, ValueError):
        table = {}
    if not isinstance(table, dict):
        table = {}

    def _get(symbol: str, _start: str) -> Any:
        return table.get(symbol, table.get(_clean_symbol(symbol), []))

    return _get


def _resolve_downloader(downloader: Optional[Downloader]) -> tuple[Downloader, str]:
    if downloader is not None:
        return downloader, "INJECTED"
    fixture = os.environ.get(REFRESH_FIXTURE_ENV)
    if fixture:
        return _fixture_downloader(Path(fixture)), "FIXTURE"
    return desk._live_downloader(), "OWNED_EODHD_LIVE"


def _normalize_ohlcv(payload: Any, through: str) -> list[tuple[str, float, Optional[float],
                                                               Optional[float]]]:
    """Flatten an owned-EODHD EOD payload into sorted (date, adj_close, close, volume),
    keeping only completed bars on/before ``through``. Malformed rows are dropped."""
    if not isinstance(payload, list):
        return []
    out = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        d = str(row.get("date") or "")[:10]
        if len(d) != 10 or d > through:
            continue
        adj = row.get("adjusted_close", row.get("close"))
        try:
            adj = float(adj)
        except (TypeError, ValueError):
            continue
        if math.isnan(adj) or math.isinf(adj) or adj <= 0:
            continue
        close = row.get("close", adj)
        vol = row.get("volume")
        try:
            close = float(close)
        except (TypeError, ValueError):
            close = None
        try:
            vol = float(vol)
        except (TypeError, ValueError):
            vol = None
        out.append((d, adj, close, vol))
    out.sort(key=lambda b: b[0])
    dedup = {}
    for b in out:
        dedup[b[0]] = b
    return [dedup[d] for d in sorted(dedup)]


def _read_csv_rows(path: Path) -> tuple[Optional[list[dict]], Optional[list[str]]]:
    try:
        with open(path, "r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            return list(reader), list(reader.fieldnames or [])
    except OSError:
        return None, None


def _atomic_write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def _std(vals: list[float]) -> Optional[float]:
    n = len(vals)
    if n < 2:
        return None
    m = sum(vals) / n
    return math.sqrt(sum((v - m) ** 2 for v in vals) / (n - 1))


def _returns(bars: list[tuple]) -> list[tuple[str, float]]:
    out = []
    for prev, cur in zip(bars, bars[1:]):
        if prev[1] and cur[1]:
            out.append((cur[0], cur[1] / prev[1] - 1.0))
    return out


def run_refresh(*, confirm: Optional[str] = None, downloader: Optional[Downloader] = None,
                panel_path=None, inputs_dir=None, ledger_dir=None,
                completed_through: Optional[str] = None) -> dict:
    """The ONE manual REFRESH ALPHA TARGET action. Owned local/EODHD data only -
    never the prediction service, never the tunnel, never a trading instruction.

    ``completed_through`` (Phase 27H) lets an atomic caller (the daily close) target
    the EXACT clock-resolved completed session it also marked the desk against, so
    the model-input market date and the desk mark date advance together. When absent
    the standalone refresh resolves the latest completed session from its own clock."""
    base = {
        "phase": PHASE,
        "snapshot_confirmed": False,
        "orders_created": False,
        "signals_created": False,
        "trade_decisions_created": False,
        "fills_created": False,
        "alpha_book_initialized": False,
        "model_formulas_changed": False,
        "model_weights_changed": False,
        "prediction_service_called": False,
    }
    if confirm != REFRESH_CONFIRM_TOKEN:
        return {"status": R_CONFIRM_REQUIRED, **base,
                "message": "Refreshing the alpha target requires confirm='%s'."
                           % REFRESH_CONFIRM_TOKEN,
                "refresh_required_token": REFRESH_CONFIRM_TOKEN,
                "loaded_at": _iso_now(), **_safety(False)}

    idir = eng._resolve(inputs_dir, eng.INPUTS_ENV, eng.DEFAULT_INPUTS)
    mom_path = idir / eng.CUR_MOM_FILE
    risk_path = idir / eng.RISK_FILE
    mom_rows, mom_fields = _read_csv_rows(mom_path)
    if not mom_rows:
        return {"status": R_INPUTS_UNAVAILABLE, **base,
                "message": "Owned momentum input missing/empty at %s." % mom_path,
                "loaded_at": _iso_now(), **_safety(False)}

    prev_amd = next((str(r.get("market_as_of_date") or "")[:10]
                     for r in mom_rows if r.get("market_as_of_date")), None)
    month_label = next((r.get("month_label") for r in mom_rows if r.get("month_label")), None)
    lcd = str(completed_through)[:10] if completed_through else latest_completed()

    if prev_amd and prev_amd >= lcd:
        return {"status": R_ALREADY_FRESH, **base,
                "previous_alpha_market_date": prev_amd,
                "resulting_alpha_market_date": prev_amd,
                "latest_completed_market_date": lcd,
                "message": "The alpha target already reflects the latest completed "
                           "market date; nothing to refresh.",
                "readiness_after": compute_readiness(panel_path=panel_path,
                                                     inputs_dir=inputs_dir,
                                                     ledger_dir=ledger_dir),
                "loaded_at": _iso_now(), **_safety(False)}

    if month_label and lcd[:7] != month_label:
        return {"status": R_MONTH_BOUNDARY, **base,
                "previous_alpha_market_date": prev_amd,
                "resulting_alpha_market_date": prev_amd,
                "latest_completed_market_date": lcd,
                "message": (
                    "The latest completed market date (%s) is in a new month vs the "
                    "current input month (%s). New month-end momentum scores must come "
                    "from the research-side monthly input emitter over the owned "
                    "survivorship-free daily panel - the frozen mom_6_1 contract is "
                    "never approximated here." % (lcd, month_label)),
                "required_next_action": "RUN_RESEARCH_MONTHLY_INPUT_EMITTER",
                "loaded_at": _iso_now(), **_safety(False)}

    dl, source = _resolve_downloader(downloader)
    lcd_date = datetime.strptime(lcd, "%Y-%m-%d").date()
    start = (lcd_date - timedelta(days=_FETCH_CALENDAR_DAYS)).isoformat()
    tickers = [str(r.get("ticker") or "").strip().upper() for r in mom_rows]
    tickers = [t for t in tickers if t]

    series: dict[str, list[tuple]] = {}
    failed: list[str] = []
    try:
        for tk in tickers:
            try:
                payload = dl(_clean_symbol(tk), start)
            except Exception as exc:  # noqa: BLE001 - sanitized taxonomy below
                enum = _classify_provider_error(exc)
                if enum in _FATAL_BLOCKS:
                    raise TournamentSyncBlocked(enum, "provider stop: %s"
                                                % getattr(exc, "error_type", "error"))
                failed.append(tk)
                continue
            bars = _normalize_ohlcv(payload, lcd)
            if bars:
                series[tk] = bars
            else:
                failed.append(tk)
    except TournamentSyncBlocked as blocked:
        return {"status": R_PROVIDER_BLOCKED, **base,
                "blocked_reason": blocked.result_enum,
                "previous_alpha_market_date": prev_amd,
                "resulting_alpha_market_date": prev_amd,
                "latest_completed_market_date": lcd,
                "message": "The owned provider refused the sync (%s). Nothing was "
                           "written." % blocked.result_enum,
                "loaded_at": _iso_now(), **_safety(False)}

    resulting = max((bars[-1][0] for bars in series.values()), default=None)
    fresh = [tk for tk, bars in series.items()
             if prev_amd is None or bars[-1][0] > prev_amd]
    coverage = (len(fresh) / len(tickers)) if tickers else 0.0
    if resulting is None or (prev_amd and resulting <= prev_amd) \
            or coverage < _MIN_COVERAGE_FRACTION:
        return {"status": R_INSUFFICIENT, **base,
                "previous_alpha_market_date": prev_amd,
                "resulting_alpha_market_date": prev_amd,
                "latest_completed_market_date": lcd,
                "coverage_fraction": round(coverage, 4),
                "n_failed": len(failed),
                "message": ("The owned fetch produced no sufficient new completed data "
                            "(coverage %.0f%% of %d names; newest observed %s). Nothing "
                            "was written." % (100 * coverage, len(tickers), resulting)),
                "loaded_at": _iso_now(), **_safety(False)}

    # Union trading calendar of the owned observations (completed bars only).
    union = sorted({d for bars in series.values() for d, *_ in bars})
    tail126 = set(union[-_HISTORY_WINDOW:])
    beta_dates = union[-(_BETA_WINDOW + 1):]
    dd_dates = set(union[-_DD_WINDOW:])

    rets_by_tk = {tk: dict(_returns(bars)) for tk, bars in series.items()}
    # Equal-weight universe return per date (owned proxy - diagnostic only).
    uni_ret: dict[str, float] = {}
    for d in beta_dates:
        vals = [r[d] for r in rets_by_tk.values() if d in r]
        if len(vals) >= 5:
            uni_ret[d] = sum(vals) / len(vals)
    uni_vals = [uni_ret[d] for d in beta_dates if d in uni_ret]
    uni_mean = (sum(uni_vals) / len(uni_vals)) if uni_vals else None
    uni_var = None
    if uni_vals and len(uni_vals) >= 2:
        uni_var = sum((v - uni_mean) ** 2 for v in uni_vals) / (len(uni_vals) - 1)

    def _ticker_stats(tk: str) -> dict:
        bars = series.get(tk) or []
        rets = [r for _d, r in _returns(bars)][-_VOL_WINDOW:]
        vol = None
        if len(rets) >= 20:
            sd = _std(rets)
            vol = round(sd * math.sqrt(252), 6) if sd is not None else None
        adv = None
        dollar = [(b[2] or b[1]) * b[3] for b in bars[-_ADV_WINDOW:]
                  if b[3] is not None and (b[2] or b[1])]
        if len(dollar) >= 10:
            adv = round(sum(dollar) / len(dollar), 2)
        obs = sum(1 for b in bars if b[0] in tail126)
        beta = None
        if uni_var and uni_var > 0:
            pairs = [(rets_by_tk[tk][d], uni_ret[d]) for d in beta_dates
                     if d in rets_by_tk.get(tk, {}) and d in uni_ret]
            if len(pairs) >= 60:
                mx = sum(p[0] for p in pairs) / len(pairs)
                my = sum(p[1] for p in pairs) / len(pairs)
                cov = sum((x - mx) * (y - my) for x, y in pairs) / (len(pairs) - 1)
                beta = round(cov / uni_var, 4)
        dd = None
        window = [b for b in bars if b[0] in dd_dates]
        if len(window) >= 20:
            peak, worst = None, 0.0
            for b in window:
                peak = b[1] if peak is None or b[1] > peak else peak
                worst = min(worst, b[1] / peak - 1.0)
            dd = round(worst, 6)
        return {"vol": vol, "adv": adv, "obs": obs, "beta": beta, "dd": dd,
                "last_date": bars[-1][0] if bars else None}

    stats = {tk: _ticker_stats(tk) for tk in series}

    # -- rewrite current_momentum_scores.csv (mom_6_1 / membership / month UNCHANGED) --
    n_updated = 0
    for row in mom_rows:
        tk = str(row.get("ticker") or "").strip().upper()
        st = stats.get(tk)
        row["market_as_of_date"] = resulting
        if not st:
            continue
        if st["adv"] is not None:
            row["adv_dollar"] = st["adv"]
        if st["vol"] is not None:
            row["realized_vol_63d"] = st["vol"]
        row["trailing_obs_126"] = st["obs"]
        row["eligible_history"] = 1 if st["obs"] >= _MIN_HISTORY_OBS else 0
        n_updated += 1
    _atomic_write_csv(mom_path, mom_fields or _MOM_FIELDS, mom_rows)

    # -- refresh current_risk_stats.csv for fetched names (others untouched) --------
    risk_rows, risk_fields = _read_csv_rows(risk_path)
    n_risk_updated = 0
    if risk_rows:
        for row in risk_rows:
            tk = str(row.get("ticker") or "").strip().upper()
            st = stats.get(tk)
            if not st:
                continue
            if st["vol"] is not None:
                row["realized_vol_63d"] = st["vol"]
            if st["beta"] is not None:
                row["beta_universe"] = st["beta"]
            if st["adv"] is not None:
                row["adv_dollar_20d"] = st["adv"]
            if st["dd"] is not None:
                row["max_drawdown_252d"] = st["dd"]
            if st["last_date"]:
                row["last_price_date"] = st["last_date"]
            n_risk_updated += 1
        _atomic_write_csv(risk_path, risk_fields or [], risk_rows)

    # -- manifest market date + append-only refresh log ----------------------------
    manifest_path = idir / "inputs_manifest.json"
    try:
        with open(manifest_path, "r", encoding="utf-8") as fh:
            manifest = json.load(fh)
    except (OSError, ValueError):
        manifest = None
    refresh_record = {
        "refreshed_at": _iso_now(),
        "phase": PHASE,
        "source": source,
        "previous_alpha_market_date": prev_amd,
        "resulting_alpha_market_date": resulting,
        "latest_completed_market_date": lcd,
        "tickers_total": len(tickers),
        "tickers_refreshed": len(series),
        "tickers_failed": len(failed),
        "momentum_scores_changed": False,
        "note": "Intramonth owned-data refresh: mom_6_1 / membership / sectors / "
                "month label unchanged per the frozen monthly contract.",
    }
    if isinstance(manifest, dict):
        manifest["market_as_of_date"] = resulting
        manifest.setdefault("paper_refreshes", []).append(refresh_record)
        desk._atomic_write_json(manifest_path, manifest)
    log_path = idir / REFRESH_LOG_FILE
    log = desk._read_json(log_path)
    entries = log.get("entries") if isinstance(log, dict) else None
    entries = entries if isinstance(entries, list) else []
    entries.append(refresh_record)
    desk._atomic_write_json(log_path, {"phase": PHASE, "kind": "alpha_target_refresh_log",
                                       "entries": entries})

    eng.clear_cache()
    try:  # keep the platform history cache coherent with the new inputs
        from paper_trader.api import multi_horizon_platform as plat
        plat.clear_caches()
    except Exception:  # noqa: BLE001
        pass

    readiness_after = compute_readiness(panel_path=panel_path, inputs_dir=inputs_dir,
                                        ledger_dir=ledger_dir)
    return {
        "status": R_REFRESHED,
        **base,
        "previous_alpha_market_date": prev_amd,
        "resulting_alpha_market_date": resulting,
        "latest_completed_market_date": lcd,
        "source": source,
        "counts": {"tickers_total": len(tickers), "tickers_refreshed": len(series),
                   "tickers_failed": len(failed), "momentum_rows_updated": n_updated,
                   "risk_rows_updated": n_risk_updated},
        "failed_tickers": failed[:50],
        "coverage_fraction": round(coverage, 4),
        "artifacts_written": [str(mom_path)] + ([str(risk_path)] if risk_rows else [])
                             + [str(log_path)],
        "historical_evidence_modified": False,
        "readiness_after": readiness_after,
        "message": ("Refreshed the owned alpha inputs %s -> %s (latest completed %s). "
                    "Momentum scores, model formulas and weights are unchanged; only "
                    "the market date and owned risk/liquidity observations moved."
                    % (prev_amd, resulting, lcd)),
        "loaded_at": _iso_now(),
        **_safety(True),
    }


__all__ = [
    "PHASE", "PRIMARY_MODEL_ID", "PRIMARY_BOOK_ID", "REQUIRED_TARGET_COUNT",
    "REFRESH_CONFIRM_TOKEN", "NOW_ENV", "REFRESH_FIXTURE_ENV",
    "B_ALPHA_STALE", "B_PLATFORM_NOT_READY", "B_INPUTS_MISSING", "B_TARGET_COUNT",
    "B_WEIGHTS_INVALID", "B_DUPLICATE", "B_LEDGER_INTEGRITY", "ALL_BLOCKERS",
    "STATE_STALE", "STATE_READY", "STATE_CONFIRMED", "STATE_BLOCKED",
    "FUND_FRESH", "FUND_STALE", "FUND_UNKNOWN",
    "ACT_REFRESH", "ACT_PREVIEW_CONFIRM", "ACT_PROCEED_BOOK", "ACT_RESOLVE",
    "R_CONFIRM_REQUIRED", "R_ALREADY_FRESH", "R_REFRESHED", "R_MONTH_BOUNDARY",
    "R_PROVIDER_BLOCKED", "R_INPUTS_UNAVAILABLE", "R_INSUFFICIENT",
    "latest_completed", "ledger_integrity", "compute_readiness", "load_readiness",
    "load_review", "confirmation_gate", "run_refresh",
]
