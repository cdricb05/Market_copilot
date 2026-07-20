"""api/multi_horizon_ledger.py - Phase 25 append-only forward paper-alpha snapshot ledger (A9).

A dedicated, additive, paper-ONLY tracking store.  It records immutable snapshots of the multi-horizon
paper books at manual review time so the platform can (a) know each sleeve's last confirmed constituents
(the ``prior`` the operating-state + recommendation engine compares against) and (b) build a forward
paper track record over time.

Design mirrors the existing Phase 13-F current-alpha book store:
    * The ONLY thing this module ever writes is a local JSON file, and only on an explicit confirmed
      commit.  Storage lives OUTSIDE the git tree (env-overridable) so the repo stays clean.
    * Preview is strictly read-only (computes the snapshot, writes nothing).
    * Confirmation requires an explicit confirm token AND commit=True, is idempotent (a duplicate
      confirm for the same market date + identical primary book is skipped), and is append-only:
      past snapshots are never rewritten and past scores are never mutated.
    * It NEVER writes PostgreSQL, an order, a fill, a trade decision, a live signal, or any existing
      execution / broker / trading workflow.  It calls neither the prediction service nor any provider.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import multi_horizon_registry as mreg

# --------------------------------------------------------------------------- #
# Store location (env-overridable; outside the git tree by default)
# --------------------------------------------------------------------------- #
LEDGER_DIR_ENV = "PAPER_TRADER_MHZ_LEDGER_DIR"
DEFAULT_LEDGER_DIR = Path.home() / ".paper_trader" / "multi_horizon_alpha_ledger"
SNAPSHOTS_FILE = "mhz_snapshots.json"

CONFIRM_TOKEN = "CONFIRM_MHZ_PAPER_SNAPSHOT"

# Statuses
STATUS_PREVIEW = "MHZ_SNAPSHOT_PREVIEW"
STATUS_CONFIRMED = "MHZ_SNAPSHOT_CONFIRMED"
STATUS_SKIPPED_DUPLICATE = "MHZ_SNAPSHOT_SKIPPED_DUPLICATE"
STATUS_CONFIRM_REQUIRED = "MHZ_SNAPSHOT_CONFIRM_REQUIRED"
STATUS_INPUTS_UNAVAILABLE = "MHZ_INPUTS_UNAVAILABLE"

_TRADABLE_SLEEVES = (mreg.SLEEVE_FUNDAMENTAL, mreg.SLEEVE_MOMENTUM, mreg.SLEEVE_COMBINED)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ledger_dir(ledger_dir=None) -> Path:
    if ledger_dir is not None:
        return Path(ledger_dir)
    env = os.environ.get(LEDGER_DIR_ENV)
    return Path(env) if env else DEFAULT_LEDGER_DIR


def _read_snapshots(sdir: Path) -> list[dict]:
    try:
        with open(sdir / SNAPSHOTS_FILE, "r", encoding="utf-8") as fh:
            obj = json.load(fh)
        if isinstance(obj, dict) and isinstance(obj.get("snapshots"), list):
            return obj["snapshots"]
        if isinstance(obj, list):
            return obj
    except (OSError, ValueError):
        pass
    return []


def _atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, indent=2, sort_keys=False)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def _book_fingerprint(constituents: list[str]) -> str:
    blob = "|".join(sorted(constituents))
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()[:16]


def _snapshot_id(market_as_of: Optional[str], fp: str) -> str:
    return "mhz_%s_%s" % ((market_as_of or "unknown").replace("-", ""), fp[:10])


# --------------------------------------------------------------------------- #
# Build the snapshot payload (pure; no writes)
# --------------------------------------------------------------------------- #
def build_snapshot_payload(current: dict, prior: Optional[dict], *,
                           confirmation_status: str = STATUS_PREVIEW) -> Optional[dict]:
    """Assemble the immutable snapshot record from a current build + prior confirmed state. No IO."""
    if current.get("status") != eng.STATUS_READY:
        return None
    market_as_of = current.get("market_as_of_date")
    books = current["books"]["books"]
    sleeves_block: dict[str, dict] = {}
    total_turnover = []
    for sid in _TRADABLE_SLEEVES:
        model_id = eng._SLEEVE_MODEL[sid]
        rec25 = eng.compute_recommendations(current, prior, sid, size=25)
        rec50 = eng.compute_recommendations(current, prior, sid, size=50)
        b25 = books[rec25["book_id"]]
        b50 = books[rec50["book_id"]]
        const25 = [c["ticker"] for c in b25["constituents"]]
        const50 = [c["ticker"] for c in b50["constituents"]]
        cadence = mreg.sleeve_by_id(sid)["cadence"]
        period = eng._sleeve_current_period(sid, current)
        total_turnover.append(rec25["estimated_turnover"])
        sleeves_block[sid] = {
            "sleeve_id": sid, "model_id": model_id,
            "model_version": mreg.model_by_id(model_id)["model_version"] if mreg.model_by_id(model_id) else "v1",
            "cadence": cadence, "period": period, "review_due": rec25["review_due"],
            "book_id_top25": rec25["book_id"], "book_id_top50": rec50["book_id"],
            "constituents_top25": const25, "constituents_top50": const50,
            "target_weights": {"top25": b25["equal_weight"], "top50": b50["equal_weight"]},
            "component_scores": {c["ticker"]: {"score": c.get("score")} for c in b25["constituents"]},
            "sector_exposure_top25": b25["sector_exposure"],
            "recommendations_top25_counts": rec25["counts"],
            "estimated_turnover": rec25["estimated_turnover"],
            "estimated_transaction_cost": rec25["estimated_transaction_cost"],
            "book_fingerprint_top25": _book_fingerprint(const25),
        }
    primary_fp = sleeves_block[mreg.SLEEVE_COMBINED]["book_fingerprint_top25"]
    snap = {
        "snapshot_id": _snapshot_id(market_as_of, primary_fp),
        "calculation_timestamp": _iso_now(),
        "market_as_of_date": market_as_of,
        "fundamental_as_of_date": current.get("fundamental_as_of_date"),
        "fundamental_month": current.get("fundamental_month"),
        "momentum_month": current.get("momentum_month"),
        "model_versions": {m["model_id"]: m["model_version"] for m in mreg.model_registry()},
        "input_fingerprints": current.get("inputs", {}).get("fingerprints"),
        "sleeves": sleeves_block,
        "primary_book_id": "fundamental_momentum_50_50_top25",
        "primary_book_fingerprint": primary_fp,
        "estimated_turnover_primary": sleeves_block[mreg.SLEEVE_COMBINED]["estimated_turnover"],
        "estimated_transaction_cost_primary": sleeves_block[mreg.SLEEVE_COMBINED]["estimated_transaction_cost"],
        "risks": _snapshot_risks(current),
        "confirmation_status": confirmation_status,
        "later_realized_returns": None,
        "immutable": True,
        "creation_record": {"created_by": "manual_confirmation", "phase": "25",
                            "confirm_token_required": CONFIRM_TOKEN},
        "safety": {"paper_only": True, "no_orders": True, "no_broker": True, "no_automation": True,
                   "wrote_to_trading_workflow": False, "wrote_to_database": False},
    }
    return snap


def _snapshot_risks(current: dict) -> dict:
    risk = current.get("inputs", {}).get("risk", {})
    combined = current["books"]["books"]["fundamental_momentum_50_50_top25"]["constituents"]
    vols, betas, dds = [], [], []
    for c in combined:
        r = risk.get(c["ticker"])
        if not r:
            continue
        if r.get("realized_vol_63d") is not None:
            vols.append(r["realized_vol_63d"])
        if r.get("beta_universe") is not None:
            betas.append(r["beta_universe"])
        if r.get("max_drawdown_252d") is not None:
            dds.append(r["max_drawdown_252d"])
    def _avg(xs):
        return round(sum(xs) / len(xs), 6) if xs else None
    return {"primary_book_mean_realized_vol_63d": _avg(vols),
            "primary_book_mean_beta_universe": _avg(betas),
            "primary_book_mean_max_drawdown_252d": _avg(dds),
            "primary_book_names_with_risk_data": len(vols)}


# --------------------------------------------------------------------------- #
# Latest-confirmed-by-sleeve (the ``prior`` the engine compares against)
# --------------------------------------------------------------------------- #
def latest_confirmed_by_sleeve(ledger_dir=None) -> dict:
    """Return {sleeve_id: {period, constituents_top25/50, confirmed_at, snapshot_id, market_as_of_date}}."""
    sdir = _ledger_dir(ledger_dir)
    snaps = [s for s in _read_snapshots(sdir) if s.get("confirmation_status") == STATUS_CONFIRMED]
    out: dict[str, dict] = {}
    for snap in snaps:  # snapshots are appended in order; the last wins per sleeve
        for sid, blk in (snap.get("sleeves") or {}).items():
            out[sid] = {
                "period": blk.get("period"),
                "constituents_top25": blk.get("constituents_top25"),
                "constituents_top50": blk.get("constituents_top50"),
                "confirmed_at": snap.get("calculation_timestamp"),
                "snapshot_id": snap.get("snapshot_id"),
                "market_as_of_date": snap.get("market_as_of_date"),
            }
    return out


# --------------------------------------------------------------------------- #
# Public API: history, preview, confirm
# --------------------------------------------------------------------------- #
def list_snapshots(ledger_dir=None) -> dict:
    sdir = _ledger_dir(ledger_dir)
    snaps = _read_snapshots(sdir)
    summary = [{"snapshot_id": s.get("snapshot_id"), "market_as_of_date": s.get("market_as_of_date"),
                "calculation_timestamp": s.get("calculation_timestamp"),
                "confirmation_status": s.get("confirmation_status"),
                "primary_book_id": s.get("primary_book_id"),
                "primary_book_fingerprint": s.get("primary_book_fingerprint"),
                "estimated_turnover_primary": s.get("estimated_turnover_primary")}
               for s in snaps]
    return {"store_dir": str(sdir), "n_snapshots": len(snaps), "snapshots": summary,
            "n_confirmed": sum(1 for s in snaps if s.get("confirmation_status") == STATUS_CONFIRMED)}


def get_snapshot(snapshot_id: str, ledger_dir=None) -> Optional[dict]:
    for s in _read_snapshots(_ledger_dir(ledger_dir)):
        if s.get("snapshot_id") == snapshot_id:
            return s
    return None


def preview_snapshot(*, ledger_dir=None, panel_path=None, inputs_dir=None) -> dict:
    """Read-only preview of the snapshot that WOULD be appended. Writes nothing."""
    current = eng.build_current(panel_path=panel_path, inputs_dir=inputs_dir)
    if current.get("status") != eng.STATUS_READY:
        return {"status": STATUS_INPUTS_UNAVAILABLE, "performed_write": False,
                "warnings": current.get("warnings", []), **mreg.safety_block()}
    prior = latest_confirmed_by_sleeve(ledger_dir)
    snap = build_snapshot_payload(current, prior, confirmation_status=STATUS_PREVIEW)
    # would this be a duplicate?
    sdir = _ledger_dir(ledger_dir)
    existing = latest_confirmed_by_sleeve(ledger_dir).get(mreg.SLEEVE_COMBINED) or {}
    dup = (existing.get("market_as_of_date") == snap["market_as_of_date"]
           and _book_fingerprint(existing.get("constituents_top25") or []) == snap["primary_book_fingerprint"])
    return {"status": STATUS_PREVIEW, "performed_write": False, "would_be_duplicate": bool(dup),
            "confirm_required_token": CONFIRM_TOKEN, "store_dir": str(sdir),
            "snapshot": snap, **mreg.safety_block()}


def confirm_snapshot(*, confirm: Optional[str] = None, ledger_dir=None,
                     panel_path=None, inputs_dir=None) -> dict:
    """Append a CONFIRMED snapshot to the local ledger (idempotent). Requires the confirm token."""
    if confirm != CONFIRM_TOKEN:
        return {"status": STATUS_CONFIRM_REQUIRED, "performed_write": False,
                "message": "A confirmed paper snapshot requires confirm='%s'." % CONFIRM_TOKEN,
                **mreg.safety_block()}
    current = eng.build_current(panel_path=panel_path, inputs_dir=inputs_dir)
    if current.get("status") != eng.STATUS_READY:
        return {"status": STATUS_INPUTS_UNAVAILABLE, "performed_write": False,
                "warnings": current.get("warnings", []), **mreg.safety_block()}
    sdir = _ledger_dir(ledger_dir)
    prior = latest_confirmed_by_sleeve(ledger_dir)
    snap = build_snapshot_payload(current, prior, confirmation_status=STATUS_CONFIRMED)

    # idempotency: same market date + identical primary Top-25 as the latest confirmed -> skip
    existing = prior.get(mreg.SLEEVE_COMBINED) or {}
    if (existing.get("market_as_of_date") == snap["market_as_of_date"]
            and _book_fingerprint(existing.get("constituents_top25") or []) == snap["primary_book_fingerprint"]):
        return {"status": STATUS_SKIPPED_DUPLICATE, "performed_write": False,
                "snapshot_id": existing.get("snapshot_id"), "store_dir": str(sdir),
                "message": "A confirmed snapshot with the same market date and primary book already exists.",
                **mreg.safety_block()}

    snaps = _read_snapshots(sdir)
    snaps.append(snap)  # APPEND-ONLY: never rewrite an existing snapshot
    _atomic_write_json(sdir / SNAPSHOTS_FILE, {"phase": "25", "updated_at": _iso_now(),
                                               "snapshots": snaps})
    return {"status": STATUS_CONFIRMED, "performed_write": True, "wrote_to_ledger_only": True,
            "snapshot_id": snap["snapshot_id"], "n_snapshots": len(snaps), "store_dir": str(sdir),
            "snapshot": snap, **mreg.safety_block()}


__all__ = [
    "LEDGER_DIR_ENV", "DEFAULT_LEDGER_DIR", "SNAPSHOTS_FILE", "CONFIRM_TOKEN",
    "STATUS_PREVIEW", "STATUS_CONFIRMED", "STATUS_SKIPPED_DUPLICATE", "STATUS_CONFIRM_REQUIRED",
    "STATUS_INPUTS_UNAVAILABLE",
    "build_snapshot_payload", "latest_confirmed_by_sleeve", "list_snapshots", "get_snapshot",
    "preview_snapshot", "confirm_snapshot",
]
