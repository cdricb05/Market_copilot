"""Stage 21 — reassessment-outcome COMPOSITION and PERSISTENCE owner.

Sources the immutable inputs, hands them to the ONE pure calculation owner
(``engine.reassessment_outcomes``), persists immutable outcome observations and serves
the read-only contract.

OWNERSHIP BOUNDARIES (deliberate, and enforced by scripts/audit_architecture.py)
--------------------------------------------------------------------------------
* Forward prices / eligible calendar / horizons -> ``api.forward_prediction_skill``.
  Stage 21 introduces NO second price-history owner and NO second horizon taxonomy.
* Execution lineage -> ``api.execution_lineage`` (Workstream 0A). Execution is never
  inferred from a current target.
* Recommendation history -> ``api.portfolio_reassessment`` (Stage 20, append-only).
* NAV / cash / holdings -> ``api.portfolio_state``. Stage 21 values nothing.
* Transaction costs -> the desk's cost policy, read through the recorded
  ``expected_net_improvement``; Stage 21 re-derives no cost model.

MATURATION TRIGGER (Workstream N)
---------------------------------
Observations mature in exactly ONE place: the canonical forward-evidence capture that
already runs inside the Daily Close (``MATURE_OUTCOMES``). That is the moment new owned
forward closes become knowable. There is deliberately NO "Refresh Outcome Evidence"
button and no second operator action — Stage 21 adds no step to the operator's day.

SAFETY
------
Every route is GET. Capture is append-only and idempotent. Nothing here approves,
proposes, orders, fills, promotes or recalibrates.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from paper_trader.engine import reassessment_outcomes as kernel

PHASE = "STAGE21"
OWNER = "api.reassessment_outcomes"
COMPOSITION_OWNER = OWNER
CALCULATION_OWNER = kernel.CALCULATION_OWNER
SCHEMA_VERSION = kernel.SCHEMA_VERSION
OUTCOME_POLICY_VERSION = kernel.OUTCOME_POLICY_VERSION

#: Its OWN evidence root (never an operational ledger root).
OUTCOME_DIR_ENV = "PAPER_TRADER_REASSESSMENT_OUTCOME_DIR"
_DEFAULT_OUTCOME_DIR = Path(r"D:\Stock_Prediction_app_data\reassessment_outcomes")
_OBSERVATIONS_FILE = "outcome_observations.json"


# --------------------------------------------------------------------------- #
# io helpers (atomic; append-only)
# --------------------------------------------------------------------------- #
def _now_iso(now: Optional[datetime] = None) -> str:
    return (now or datetime.now(timezone.utc)).isoformat()


def _outcome_dir(outcome_dir=None) -> Path:
    if outcome_dir is not None:
        return Path(outcome_dir)
    env = os.environ.get(OUTCOME_DIR_ENV)
    return Path(env) if env else _DEFAULT_OUTCOME_DIR


def _observations_path(outcome_dir=None) -> Path:
    return _outcome_dir(outcome_dir) / _OBSERVATIONS_FILE


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(blob)
        os.replace(tmp, str(path))
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _safety() -> dict:
    return {
        "read_only": True, "performed_write": False, "provider_called": False,
        "prediction_called": False, "created_orders": False, "created_fills": False,
        "changed_holdings": False, "changed_cash": False, "changed_nav": False,
        "broker_enabled": False, "live_orders_enabled": False,
        "automation_enabled": False, "changed_cadence": False,
        "approved_proposal": False, "created_proposal": False,
        "promoted_model": False, "recalibrated_model": False,
        "changed_policy": False, "changed_thresholds": False,
        "safety_badges": ["READ ONLY", "EVIDENCE ONLY", "REVIEW ONLY", "NO ORDERS",
                          "NO BROKER", "AUTOMATION OFF", "MANUAL REVIEW",
                          "NO MODEL PROMOTION"],
    }


# --------------------------------------------------------------------------- #
# Default source loaders (all injectable seams for hermetic tests)
# --------------------------------------------------------------------------- #
def _default_history_loader(*, active_book_id: Optional[str] = None) -> list:
    """The Stage-20 history, reduced to ONE authoritative row per session.

    Release 54.2 — a session can now hold several immutable assessments, and only
    the last one is what the system concluded about it. Observing every version
    would count the same recommendation once per intraday reassessment and inflate
    the forward-outcome evidence this owner exists to keep honest. Superseded
    versions remain on disk; they are simply not observed twice.
    """
    from paper_trader.api import portfolio_reassessment as prs
    return prs.authoritative_history_rows(
        prs.load_history(active_book_id=active_book_id))


def _default_evidence_loader(desk_dir=None) -> dict:
    """Prices + eligible calendar + horizons from the ONE forward-evidence owner."""
    from paper_trader.api import forward_prediction_skill as fps
    store = fps.read_price_store(desk_dir)
    series = store.get("series") or {}
    return {"series": series,
            "calendar": fps.eligible_calendar(desk_dir, price_store=store),
            "horizons": list(fps.HORIZONS),
            "price_source": fps.PRICE_STORE_FILE,
            "evidence_owner": "api.forward_prediction_skill",
            "evidence_fingerprint": kernel.stable_hash(
                {"updated_at": store.get("updated_at"),
                 "tickers": len(series),
                 "calendar_last": (store.get("series") or {}) and None})}


def _default_lineage_loader(desk_dir=None) -> dict:
    from paper_trader.api import execution_lineage as el
    return el.load_execution_lineage(desk_dir=desk_dir)


def _filled_tickers(lineage: dict) -> dict:
    """Which names a COMPLETED, immutably recorded rebalance actually filled.

    Governance uses this and nothing else to claim EXECUTED. A plan whose orders were
    all cancelled contributes nothing, however recent it is.
    """
    latest = (lineage or {}).get("latest_completed_rebalance") or {}
    tickers = set()
    for oid in latest.get("order_ids") or []:
        # Order ids end in the ticker: ord_<book>_<seq>_<TICKER>.
        part = str(oid).rsplit("_", 1)[-1]
        if part:
            tickers.add(part.upper())
    return {"filled_tickers": sorted(tickers), "approved": bool(latest),
            "order_plan_id": latest.get("order_plan_id"),
            "proposal_id": latest.get("proposal_id"),
            "decision_id": latest.get("decision_id"),
            "settlement_market_date": latest.get("settlement_market_date")}


# --------------------------------------------------------------------------- #
# Observation construction (composition over the pure kernel)
# --------------------------------------------------------------------------- #
def build_observations(*, history: Optional[list] = None,
                       evidence: Optional[dict] = None,
                       lineage: Optional[dict] = None,
                       active_book_id: Optional[str] = None,
                       horizons: Optional[list] = None,
                       policy: Optional[dict] = None,
                       desk_dir=None,
                       history_loader: Optional[Callable] = None,
                       evidence_loader: Optional[Callable] = None,
                       lineage_loader: Optional[Callable] = None) -> dict:
    """Every outcome observation derivable from the immutable Stage-20 history.

    PURE COMPOSITION: sources are read, then the kernel decides everything. A row is
    produced for every (recommendation, horizon) pair, including the ones that are not
    yet mature — an honest pending count is evidence too.
    """
    rows = history if history is not None else (
        history_loader or _default_history_loader)(active_book_id=active_book_id)
    ev = evidence if evidence is not None else (
        (evidence_loader or _default_evidence_loader)(desk_dir))
    lin = lineage if lineage is not None else (
        (lineage_loader or _default_lineage_loader)(desk_dir))

    series = ev.get("series") or {}
    calendar = list(ev.get("calendar") or [])
    hz = list(horizons or ev.get("horizons") or [])
    exec_lineage = _filled_tickers(lin)
    proposal = {"action_tickers": exec_lineage["filled_tickers"]}

    observations: list[dict] = []
    for row in rows:
        if active_book_id and row.get("active_book_id") != active_book_id:
            continue
        for rec in (row.get("recommendations") or []):
            for h in hz:
                observations.append(kernel.build_observation(
                    row=row, rec=rec, horizon=h, calendar=calendar, series=series,
                    lineage=exec_lineage, proposal=proposal, policy=policy))
    return {"observations": observations, "evidence": ev, "lineage": exec_lineage,
            "history_rows": len(rows), "horizons": hz}


# --------------------------------------------------------------------------- #
# Immutable persistence (Workstream I) — the ONLY writer, driven by the Daily Close
# --------------------------------------------------------------------------- #
def load_observations(*, outcome_dir=None,
                      active_book_id: Optional[str] = None) -> list[dict]:
    rows = _load_json(_observations_path(outcome_dir))
    if not isinstance(rows, list):
        return []
    out = [r for r in rows if isinstance(r, dict)
           and (active_book_id is None or r.get("active_book_id") == active_book_id)]
    out.sort(key=lambda r: (r.get("eligible_market_date") or "",
                            r.get("ticker") or "",
                            r.get("horizon_eligible_closes") or 0))
    return out


def capture_matured_outcomes(*, outcome_dir=None, active_book_id: Optional[str] = None,
                             history: Optional[list] = None,
                             evidence: Optional[dict] = None,
                             lineage: Optional[dict] = None,
                             policy: Optional[dict] = None, desk_dir=None,
                             now: Optional[datetime] = None,
                             history_loader: Optional[Callable] = None,
                             evidence_loader: Optional[Callable] = None,
                             lineage_loader: Optional[Callable] = None) -> dict:
    """Append every NEWLY MATURED observation. Append-only and idempotent.

    * An observation is written ONLY when its horizon has genuinely matured against the
      owned eligible-session calendar. A pending horizon is never persisted, so no row
      can ever be "filled in later" with hindsight.
    * Identity binds the reassessment, book, recommendation, horizon, evidence
      fingerprint and every policy version. Re-running writes nothing.
    * A previously recorded observation is NEVER rewritten. If the same identity ever
      resolves to different metrics, the existing row wins and the conflict is reported.
    """
    built = build_observations(
        history=history, evidence=evidence, lineage=lineage,
        active_book_id=active_book_id, policy=policy, desk_dir=desk_dir,
        history_loader=history_loader, evidence_loader=evidence_loader,
        lineage_loader=lineage_loader)
    ev_fp = (built["evidence"] or {}).get("evidence_fingerprint")
    existing = load_observations(outcome_dir=outcome_dir)
    by_id = {r.get("observation_id"): r for r in existing}

    appended, conflicts = [], []
    for obs in built["observations"]:
        if obs.get("maturity") != kernel.MAT_MATURE:
            continue
        identity = kernel.observation_identity(obs, evidence_fingerprint=ev_fp)
        oid = kernel.observation_id(identity)
        if oid in by_id:
            prior = by_id[oid]
            if prior.get("realized_spread") != obs.get("realized_spread"):
                conflicts.append({
                    "observation_id": oid, "kept": "EXISTING",
                    "reason": "IMMUTABLE_OBSERVATION_NEVER_REWRITTEN"})
            continue
        appended.append({**obs, "observation_id": oid, "identity": identity,
                         "recorded_at": _now_iso(now), "immutable": True,
                         "backfilled": False})

    if appended:
        _atomic_write_json(_observations_path(outcome_dir), existing + appended)
    return {
        "status": "OK", "phase": PHASE, "owner": OWNER,
        "observations_newly_matured": len(appended),
        "observations_total": len(existing) + len(appended),
        "pending_observation_count": sum(
            1 for o in built["observations"]
            if o.get("maturity") == kernel.MAT_NOT_YET_MATURE),
        "blocked_observation_count": sum(
            1 for o in built["observations"]
            if o.get("maturity") == kernel.MAT_DATA_BLOCKED),
        "conflicts": conflicts,
        "performed_write": bool(appended),
        "append_only": True, "rewrote_existing_evidence": False,
        "evidence_fingerprint": ev_fp,
    }


def capture_for_daily_close(*, desk_dir=None, outcome_dir=None,
                            active_book_id: Optional[str] = None,
                            now: Optional[datetime] = None) -> dict:
    """The ONE Daily Close integration point (Workstream N).

    Called from the canonical forward-evidence capture the close already performs, at
    the exact moment new owned forward closes become knowable. Degrade-safe by contract
    of the caller: a Stage-21 failure is evidence-only and can never invalidate a close.
    """
    try:
        return capture_matured_outcomes(desk_dir=desk_dir, outcome_dir=outcome_dir,
                                        active_book_id=active_book_id, now=now)
    except Exception as exc:  # noqa: BLE001
        return {"status": "OUTCOME_CAPTURE_FAILED", "phase": PHASE, "owner": OWNER,
                "observations_newly_matured": 0, "performed_write": False,
                "error": str(exc)[:200]}


# --------------------------------------------------------------------------- #
# Read contracts (Workstream M) — GET only
# --------------------------------------------------------------------------- #
_HISTORICAL_GAP_NOTE = (
    "Outcome evidence begins when Stage 20 first produced immutable reassessment "
    "artifacts. Earlier eligible sessions have NO recommendation to measure and are "
    "NOT reconstructed: a hindsight backfill would be fabricated evidence. The gap is "
    "a documented limitation, not a data error.")


def load_reassessment_outcomes(*, outcome_dir=None, desk_dir=None,
                               active_book_id: Optional[str] = None,
                               observations: Optional[list] = None,
                               policy: Optional[dict] = None,
                               now: Optional[datetime] = None,
                               **kwargs) -> dict:
    """The Stage-21 summary read: scorecard + policy intelligence + evidence state."""
    generated_at = _now_iso(now)
    try:
        persisted = observations if observations is not None else load_observations(
            outcome_dir=outcome_dir, active_book_id=active_book_id)
        # Pending / blocked rows are derived live (they are not persisted, by design)
        # so the operator can see honestly what is still ripening.
        live = build_observations(active_book_id=active_book_id, policy=policy,
                                  desk_dir=desk_dir, **kwargs)
        pending = [o for o in live["observations"]
                   if o.get("maturity") != kernel.MAT_MATURE]
        allobs = list(persisted) + pending
        scorecard = kernel.build_scorecard(allobs, policy=policy)
        intelligence = kernel.build_policy_intelligence(allobs, policy=policy)
    except Exception as exc:  # noqa: BLE001 - a read must never crash the caller
        return {"phase": PHASE, "owner": OWNER, "status": "UNAVAILABLE",
                "generated_at": generated_at, "active_book_id": active_book_id,
                "message": "Outcome evidence unavailable: %s" % str(exc)[:160],
                "scorecard": None, "policy_intelligence": None, **_safety()}

    ev_state = scorecard["evidence"]["state"]
    insufficient = ev_state in (kernel.EV_NO_OBSERVATIONS, kernel.EV_INSUFFICIENT)
    return {
        "phase": PHASE, "owner": OWNER, "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": CALCULATION_OWNER, "schema_version": SCHEMA_VERSION,
        "status": "OK", "generated_at": generated_at,
        "active_book_id": active_book_id,
        "outcome_policy_version": OUTCOME_POLICY_VERSION,
        "scorecard": scorecard,
        "policy_intelligence": intelligence,
        "evidence_state": ev_state,
        "evidence_sufficient": not insufficient,
        "maturity_vocabulary": list(kernel.MATURITY_VOCAB),
        "governance_vocabulary": list(kernel.GOVERNANCE_VOCAB),
        "measurement_basis_vocabulary": list(kernel.BASIS_VOCAB),
        "policy_state_vocabulary": list(kernel.POLICY_VOCAB),
        "horizon_owner": "api.forward_prediction_skill",
        "price_owner": "api.forward_prediction_skill",
        "execution_lineage_owner": "api.execution_lineage",
        "maturation_trigger": "api.daily_close (forward-evidence capture)",
        "manual_refresh_endpoint": None,
        "backfilled": False,
        "historical_gap_note": _HISTORICAL_GAP_NOTE,
        "message": (
            "Insufficient outcome evidence: %d matured observation(s) at the %d-session "
            "horizon. No decision-quality or policy conclusion may be read from this yet."
            % (scorecard["evidence"]["matured_observations"], scorecard["primary_horizon"])
            if insufficient else
            "Decision outcome evidence over %d matured observation(s) at the %d-session "
            "horizon. Evidence only — it changes no policy, model or portfolio."
            % (scorecard["evidence"]["matured_observations"], scorecard["primary_horizon"])),
        **_safety(),
    }


def load_outcome_history(*, outcome_dir=None, active_book_id: Optional[str] = None,
                         limit: int = 500, now: Optional[datetime] = None) -> dict:
    """The full, append-only outcome observation history (audit surface)."""
    rows = load_observations(outcome_dir=outcome_dir, active_book_id=active_book_id)
    return {
        "phase": PHASE, "owner": OWNER, "schema_version": SCHEMA_VERSION,
        "status": "OK", "generated_at": _now_iso(now),
        "active_book_id": active_book_id,
        "rows": rows[-limit:] if limit else rows,
        "row_count": len(rows),
        "append_only": True, "backfilled": False,
        "historical_gap_note": _HISTORICAL_GAP_NOTE,
        **_safety(),
    }


def load_outcome_observation(observation_id: str, *, outcome_dir=None,
                             now: Optional[datetime] = None) -> dict:
    """ONE immutable observation by id, with its full point-in-time binding."""
    for r in load_observations(outcome_dir=outcome_dir):
        if r.get("observation_id") == observation_id:
            return {"phase": PHASE, "owner": OWNER, "status": "OK",
                    "generated_at": _now_iso(now), "observation": r, **_safety()}
    return {"phase": PHASE, "owner": OWNER, "status": "NOT_FOUND",
            "generated_at": _now_iso(now), "observation": None,
            "message": "No outcome observation with id %r." % observation_id,
            **_safety()}


__all__ = [
    "PHASE", "OWNER", "COMPOSITION_OWNER", "CALCULATION_OWNER", "SCHEMA_VERSION",
    "OUTCOME_POLICY_VERSION", "OUTCOME_DIR_ENV",
    "build_observations", "capture_matured_outcomes", "capture_for_daily_close",
    "load_observations", "load_reassessment_outcomes", "load_outcome_history",
    "load_outcome_observation",
]
