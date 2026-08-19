"""api/return_forecast.py - composition, persistence and read owner for the
Release 30 forward-return forecast.

Layering, unchanged from every prior slice:

* ``engine.return_forecast``  - the KERNEL: what a forecast is, how a frozen
  model is applied, how uncertainty is formed. Pure stdlib, pure function.
* ``alpha_agent.release30_forecast_emitter`` - the numpy RESEARCH bridge that
  emits the feature cross-section (it has no authority).
* this module - COMPOSITION and READ: it locates the frozen artifact and the
  emitted cross-section, validates both through the kernel, reports the result,
  and owns the immutable forward-evidence capture.

Two boundaries this module enforces and never blurs:

**Activation is manual.** A frozen artifact existing on disk does NOT make it
operational. ``activation_state`` is ``NOT_ACTIVATED`` unless a human has
written an explicit activation record, and no code path here can write one. The
read surface is honest about this: an unactivated forecast is research evidence,
displayed as such, and it is never fed to the canonical portfolio decision.

**Reading never writes.** ``load_return_forecast`` is pure read. Forward evidence
is captured only by ``capture_forecast_snapshot``, which is called by an
operator-initiated path, never by a GET.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from paper_trader.engine import return_forecast as kernel

SCHEMA_VERSION = kernel.SCHEMA_VERSION
COMPOSITION_OWNER = "api.return_forecast"
PHASE = "R30"

STATE_READY = kernel.STATE_READY
STATE_DEGRADED = kernel.STATE_DEGRADED
STATE_BLOCKED = kernel.STATE_BLOCKED
STATE_NOT_ACTIVATED = kernel.STATE_NOT_ACTIVATED
STATE_UNAVAILABLE = "UNAVAILABLE"
READ_STATE_VOCAB = (STATE_READY, STATE_DEGRADED, STATE_BLOCKED,
                    STATE_NOT_ACTIVATED, STATE_UNAVAILABLE)

#: Activation vocabulary. There is no third value that means "sort of".
ACTIVATION_NOT_ACTIVATED = "NOT_ACTIVATED"
ACTIVATION_ACTIVE = "ACTIVE_FOR_PAPER"
ACTIVATION_VOCAB = (ACTIVATION_NOT_ACTIVATED, ACTIVATION_ACTIVE)
ACTIVATION_FILE = "forecast_model_activation.json"
ACTIVATION_TOKEN = "CONFIRM_ACTIVATE_RETURN_FORECAST_MODEL"

AUTOMATIC_PROMOTION_ALLOWED = False

R30_ROOT_ENV = "PAPER_TRADER_R30_ROOT"
_DEFAULT_R30_ROOT = Path(
    r"D:\Stock_Prediction_app_data\release30_zero_base_adaptive_allocator")
EVIDENCE_DIR_ENV = "PAPER_TRADER_RETURN_FORECAST_DIR"
_DEFAULT_EVIDENCE_DIR = Path(r"D:\Stock_Prediction_app_data\return_forecast")

#: Which frozen artifact the operational lane would use if activated. The
#: universe tag is part of the file name so a fundamental-augmented artifact can
#: never be loaded under the impression that it is the price-only one.
DEFAULT_ARTIFACT_TAG = "price_only"
_ARTIFACT_FILE = "model_artifact_%s.json"
_INPUT_FILE = "forecast_input_%s.json"

SAFETY_BADGES = ["PREVIEW ONLY", "READ ONLY", "NO ORDERS", "NO LIVE PROMOTION",
                 "MANUAL REVIEW", "AUTOMATION OFF"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def r30_root(root=None) -> Path:
    return Path(root or os.environ.get(R30_ROOT_ENV) or _DEFAULT_R30_ROOT)


def evidence_dir(evidence=None) -> Path:
    return Path(evidence or os.environ.get(EVIDENCE_DIR_ENV)
                or _DEFAULT_EVIDENCE_DIR)


def _load_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=1, default=str), encoding="utf-8")
    tmp.replace(path)


# --------------------------------------------------------------------------- #
# Artifact / input loading
# --------------------------------------------------------------------------- #
def load_model_artifact(*, tag: str = DEFAULT_ARTIFACT_TAG,
                        root=None) -> Optional[dict]:
    return _load_json(r30_root(root) / (_ARTIFACT_FILE % tag))


def load_forecast_input(*, tag: str = DEFAULT_ARTIFACT_TAG,
                        root=None) -> Optional[dict]:
    return _load_json(r30_root(root) / (_INPUT_FILE % tag))


def activation_state(*, evidence=None) -> dict:
    """Whether a human has activated the forecasting model for paper use.

    Fails CLOSED: anything other than a well-formed activation record carrying
    the explicit token reads as NOT_ACTIVATED.
    """
    rec = _load_json(evidence_dir(evidence) / ACTIVATION_FILE)
    ok = bool(rec and rec.get("confirm_token") == ACTIVATION_TOKEN
              and rec.get("state") == ACTIVATION_ACTIVE
              and rec.get("model_spec_hash"))
    return {
        "state": ACTIVATION_ACTIVE if ok else ACTIVATION_NOT_ACTIVATED,
        "vocabulary": list(ACTIVATION_VOCAB),
        "activated_model_spec_hash": (rec or {}).get("model_spec_hash") if ok else None,
        "activated_at": (rec or {}).get("activated_at") if ok else None,
        "activated_by": (rec or {}).get("activated_by") if ok else None,
        "automatic_promotion_allowed": AUTOMATIC_PROMOTION_ALLOWED,
        "requires": ("a human-written activation record carrying the token %s; "
                     "no code path in this module can create one"
                     % ACTIVATION_TOKEN),
    }


# --------------------------------------------------------------------------- #
# Read surface
# --------------------------------------------------------------------------- #
def build(*, tag: str = DEFAULT_ARTIFACT_TAG, root=None,
          evidence=None, artifact: Optional[dict] = None,
          cross_section: Optional[dict] = None) -> dict:
    """Compose the forecast payload. PURE READ - writes nothing."""
    art = artifact if artifact is not None else load_model_artifact(tag=tag, root=root)
    ic = (cross_section if cross_section is not None
          else load_forecast_input(tag=tag, root=root))
    act = activation_state(evidence=evidence)

    if art is None or ic is None:
        missing = []
        if art is None:
            missing.append({"code": "MODEL_ARTIFACT_ABSENT", "tag": tag})
        if ic is None:
            missing.append({"code": "FORECAST_INPUT_ABSENT", "tag": tag})
        return {
            "schema_version": SCHEMA_VERSION,
            "composition_owner": COMPOSITION_OWNER, "phase": PHASE,
            "state": STATE_UNAVAILABLE,
            "state_vocabulary": list(READ_STATE_VOCAB),
            "generated_at": _now_iso(), "artifact_tag": tag,
            "activation": act, "by_horizon": {}, "horizons": [],
            "blockers": missing, "warnings": [],
            "safety": {"badges": list(SAFETY_BADGES), "creates_orders": False,
                       "creates_decisions": False, "mutates_holdings": False,
                       "promotes_models": False},
        }

    forecast = kernel.build_forecast(cross_section=ic, artifact=art)
    forecast["generated_at"] = _now_iso()
    forecast["composition_owner"] = COMPOSITION_OWNER
    forecast["artifact_tag"] = tag
    forecast["activation"] = act
    # An artifact that has never been activated is RESEARCH EVIDENCE. Saying so
    # in the state - rather than only in a footnote - is what stops a forecast
    # being read as an operational instruction.
    forecast["operational_use"] = (
        "ACTIVE" if act["state"] == ACTIVATION_ACTIVE else "RESEARCH_EVIDENCE_ONLY")
    if act["state"] != ACTIVATION_ACTIVE and forecast["state"] == STATE_READY:
        forecast["state"] = STATE_NOT_ACTIVATED
    forecast["input_staleness"] = {
        "feature_as_of_date": ic.get("as_of_date"),
        "requested_eligible_market_date": ic.get("requested_eligible_market_date"),
        "behind_eligible_session": bool(
            ic.get("feature_panel_behind_eligible_session")),
        "gap_calendar_days": ic.get("feature_panel_gap_calendar_days"),
        "doc": ("The owned daily feature panel is a periodic build. When its last "
                "session sits behind the eligible market date the forecast is "
                "stamped with the session it ACTUALLY used, and the gap is "
                "reported rather than papered over."),
    }
    return forecast


def load_return_forecast(**kwargs) -> dict:
    return build(**kwargs)


def summary(payload: Optional[dict] = None, **kwargs) -> dict:
    """Compact block for composition into other read models."""
    p = payload if payload is not None else build(**kwargs)
    h = str(p.get("horizons")[0]) if p.get("horizons") else None
    blk = (p.get("by_horizon") or {}).get(h) or {}
    return {
        "state": p.get("state"),
        "operational_use": p.get("operational_use"),
        "activation_state": (p.get("activation") or {}).get("state"),
        "eligible_market_date": p.get("eligible_market_date"),
        "universe_size": p.get("universe_size"),
        "horizons": p.get("horizons") or [],
        "model_spec_hash": p.get("model_spec_hash"),
        "feature_snapshot_hash": p.get("feature_snapshot_hash"),
        "target_quantity": p.get("target_quantity"),
        "weights": blk.get("weights") or {},
        "input_staleness": p.get("input_staleness") or {},
    }


# --------------------------------------------------------------------------- #
# Forward evidence
# --------------------------------------------------------------------------- #
def capture_forecast_snapshot(*, forecast: dict, evidence=None,
                              now: Optional[str] = None) -> dict:
    """Freeze every operational prediction BEFORE its outcome is known.

    Append-only and first-write-wins per (market date, model hash): a snapshot
    that already exists is returned untouched rather than rewritten, because a
    forecast that can be edited after the fact is not forward evidence.

    Deliberately NOT called by any GET. Outcomes are appended later by the
    existing forward-evidence owners when a horizon matures; nothing here
    backdates, reconstructs, or infers an outcome from a current snapshot.
    """
    d = evidence_dir(evidence)
    market_date = forecast.get("eligible_market_date")
    model_hash = forecast.get("model_spec_hash")
    if not market_date or not model_hash:
        return {"state": "REJECTED",
                "reason": "SNAPSHOT_REQUIRES_MARKET_DATE_AND_MODEL_HASH"}
    path = d / "snapshots" / ("%s_%s.json" % (market_date, str(model_hash)[:16]))
    if path.exists():
        return {"state": "ALREADY_CAPTURED", "path": str(path),
                "immutable": True}
    rows = []
    for h, blk in sorted((forecast.get("by_horizon") or {}).items()):
        for r in blk.get("forecasts") or []:
            rows.append({
                "ticker": r["ticker"], "horizon_sessions": int(h),
                "expected_return": r.get("expected_return"),
                "expected_excess_return": r.get("expected_excess_return"),
                "forecast_uncertainty": r.get("forecast_uncertainty"),
                "downside_return_q05": r.get("downside_return_q05"),
                "rank": r.get("rank"),
            })
    payload = {
        "schema_version": "return_forecast.snapshot.v1",
        "immutable": True, "append_only": True,
        "captured_at": now or _now_iso(),
        "market_date": market_date,
        "model_spec_hash": model_hash,
        "feature_snapshot_hash": forecast.get("feature_snapshot_hash"),
        "target_quantity": forecast.get("target_quantity"),
        "horizons": forecast.get("horizons") or [],
        "rows": rows,
        "outcomes_appended": False,
        "outcome_policy": ("realised return, excess return, prediction error, "
                           "rank error and direction are appended by the "
                           "canonical forward-evidence owners when a horizon "
                           "matures; never backdated, never reconstructed"),
    }
    _atomic_write_json(path, payload)
    return {"state": "CAPTURED", "path": str(path), "rows": len(rows),
            "immutable": True}
