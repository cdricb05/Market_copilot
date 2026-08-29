r"""Release 47 - PORTFOLIO DECISION FORWARD EVIDENCE (composition + persistence owner).

This is the ONE owner of the durable, append-only, IDEMPOTENT ledger of EXECUTED
portfolio-reallocation decisions and of their two frozen forward paths. It composes
the authoritative owners - it computes nothing itself:

  * the immutable proposal and its economics      ``api.reallocation_proposal``
  * the manual approval that authorised the change ``api.portfolio_decision``
  * the executed paper orders and the desk marks   ``api.paper_trading_desk``
  * every measurement                              ``engine.portfolio_decision_outcome``

What it owns
------------
  1. FREEZING. At the moment a governed paper rebalance creates its orders - and
     only then - one immutable record is written carrying the previous portfolio,
     the proposed target, the executed target, the reasons, the expected
     improvement, the costs, the risk, the constraints, the market date, the model
     state, and BOTH forward paths (executed and counterfactual hold) with their
     decision-session reference prices. Nothing is ever written retrospectively.
  2. MEASURING. A read-only forward measurement of those frozen paths against the
     desk's own settled marks, at sessions strictly AFTER the decision session.

What it never does
------------------
  * It creates no order, no fill and no target; it changes no holding, cash or NAV.
  * It never RECONSTRUCTS a counterfactual: a record that was not frozen at decision
    time simply does not exist, and no later process may invent one.
  * It promotes no model, recalibrates nothing and changes no policy. A verdict here
    is evidence; acting on it is a separate, manual, governed decision.

The ledger lives under its OWN evidence root (``PAPER_TRADER_PORTFOLIO_DECISION_
OUTCOME_DIR``) - never the operational paper-desk ledger root, and never the Stage-18
decision root.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from paper_trader.api import paper_trading_desk as desk
from paper_trader.engine import portfolio_decision_outcome as kernel

PHASE = "R47"
OWNER = "api.portfolio_decision_outcome"
CALCULATION_OWNER = kernel.CALCULATION_OWNER

PATH_EXECUTED = kernel.PATH_EXECUTED
PATH_HOLD = kernel.PATH_HOLD

OUTCOME_DIR_ENV = "PAPER_TRADER_PORTFOLIO_DECISION_OUTCOME_DIR"
_DEFAULT_OUTCOME_DIR = Path(r"D:\Stock_Prediction_app_data\portfolio_decision_outcomes")
_RECORDS_FILE = "decision_records.json"
_INDEX_FILE = "index.json"

# --- Freeze statuses --------------------------------------------------------- #
F_CREATED = "DECISION_EVIDENCE_FROZEN"
F_REUSED = "REUSED_EXISTING_NO_DUPLICATE"
F_REFUSED_NOT_EXECUTED = "REFUSED_NO_EXECUTION_TO_RECORD"
F_REFUSED_INCOMPLETE = "REFUSED_INCOMPLETE_DECISION_EVIDENCE"
FREEZE_STATUS_VOCAB = (F_CREATED, F_REUSED, F_REFUSED_NOT_EXECUTED,
                       F_REFUSED_INCOMPLETE)


# --------------------------------------------------------------------------- #
# io helpers (the same atomic pattern as the neighbouring evidence owners)
# --------------------------------------------------------------------------- #
def _now_iso(now: Optional[datetime]) -> str:
    return (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()


def _outcome_dir(outcome_dir=None) -> Path:
    if outcome_dir is not None:
        return Path(outcome_dir)
    env = os.environ.get(OUTCOME_DIR_ENV)
    return Path(env) if env else _DEFAULT_OUTCOME_DIR


def _records_path(outcome_dir=None) -> Path:
    return _outcome_dir(outcome_dir) / _RECORDS_FILE


def _index_path(outcome_dir=None) -> Path:
    return _outcome_dir(outcome_dir) / _INDEX_FILE


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


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def load_records(*, outcome_dir=None) -> list[dict]:
    """Every frozen decision record, oldest first. PURE reader; never raises."""
    recs = _load_json(_records_path(outcome_dir))
    return [r for r in recs if isinstance(r, dict)] if isinstance(recs, list) else []


def load_record(*, decision_id: str, outcome_dir=None) -> Optional[dict]:
    for rec in load_records(outcome_dir=outcome_dir):
        if rec.get("decision_id") == decision_id:
            return rec
    return None


# --------------------------------------------------------------------------- #
# Freezing - the ONE write, at the execution boundary
# --------------------------------------------------------------------------- #
def freeze_executed_decision(*, proposal_hash: Optional[str],
                             order_plan_id: Optional[str],
                             eligible_market_date: Optional[str],
                             active_book_id: Optional[str],
                             previous_weights: dict,
                             proposed_weights: dict,
                             executed_weights: dict,
                             reference_prices: dict,
                             nav: Optional[float],
                             transaction_cost: Optional[float],
                             orders_created: int = 0,
                             decision_reasons: Optional[dict] = None,
                             expected_improvement: Optional[dict] = None,
                             risk_at_decision: Optional[dict] = None,
                             constraints_at_decision: Optional[dict] = None,
                             model_state: Optional[dict] = None,
                             provenance: Optional[dict] = None,
                             outcome_dir=None,
                             now: Optional[datetime] = None) -> dict:
    """Freeze ONE immutable decision record. Idempotent by decision identity.

    Called at the execution boundary, after the governed paper orders exist and
    before any forward price does. Replaying an approval or an execution reuses the
    existing record and writes nothing - the same idempotency the order plan itself
    guarantees, so a replay can never manufacture a second, differently-timed
    counterfactual.

    Refuses (and writes nothing) when there is no execution to record, or when the
    evidence needed to freeze a HONEST counterfactual is incomplete. A record that
    cannot be frozen truthfully is not written at all; it is never patched later.
    """
    base = {"owner": OWNER, "phase": PHASE, "frozen": False,
            "created_orders": False, "changed_holdings": False,
            "changed_cash": False, "changed_nav": False, "paper_only": True,
            "hindsight_reconstruction": False,
            "freeze_status_vocabulary": list(FREEZE_STATUS_VOCAB)}

    if int(orders_created or 0) <= 0:
        return {**base, "status": F_REFUSED_NOT_EXECUTED,
                "message": ("No paper order was created, so there is no executed "
                            "decision to record.")}
    missing = [k for k, v in (("proposal_hash", proposal_hash),
                              ("eligible_market_date", eligible_market_date),
                              ("active_book_id", active_book_id),
                              ("nav", nav)) if not v]
    if missing:
        return {**base, "status": F_REFUSED_INCOMPLETE, "missing": missing,
                "message": ("A decision record must bind an exact proposal, book, "
                            "session and NAV, or its counterfactual is not "
                            "trustworthy.")}

    decision_id = kernel.decision_id_for(
        proposal_hash=proposal_hash, order_plan_id=order_plan_id,
        eligible_market_date=eligible_market_date, active_book_id=active_book_id)
    existing = load_record(decision_id=decision_id, outcome_dir=outcome_dir)
    if existing:
        return {**base, "status": F_REUSED, "frozen": True, "reused": True,
                "decision_id": decision_id, "record": existing}

    record = kernel.freeze_decision_record(
        decision_id=decision_id, frozen_at=_now_iso(now),
        eligible_market_date=eligible_market_date, active_book_id=active_book_id,
        previous_portfolio=previous_weights or {},
        proposed_target=proposed_weights or {},
        executed_target=executed_weights or {},
        reference_prices=reference_prices or {}, nav_at_decision=nav,
        transaction_cost=transaction_cost,
        decision_reasons=decision_reasons, expected_improvement=expected_improvement,
        risk_at_decision=risk_at_decision,
        constraints_at_decision=constraints_at_decision,
        model_state=model_state,
        provenance={**(provenance or {}),
                    "proposal_hash": proposal_hash,
                    "order_plan_id": order_plan_id,
                    "composition_owner": OWNER,
                    "orders_created": int(orders_created or 0)})

    records = load_records(outcome_dir=outcome_dir)
    records.append(record)
    _atomic_write_json(_records_path(outcome_dir), records)
    index = _load_json(_index_path(outcome_dir)) or {}
    if not isinstance(index, dict):
        index = {}
    index[decision_id] = {
        "decision_id": decision_id, "frozen_at": record["frozen_at"],
        "eligible_market_date": eligible_market_date,
        "active_book_id": active_book_id, "proposal_hash": proposal_hash,
        "order_plan_id": order_plan_id, "record_hash": record["record_hash"]}
    _atomic_write_json(_index_path(outcome_dir), index)
    return {**base, "status": F_CREATED, "frozen": True, "reused": False,
            "decision_id": decision_id, "record": record}


# --------------------------------------------------------------------------- #
# Measuring - read only
# --------------------------------------------------------------------------- #
def _price_history(marks: dict, tickers: set) -> dict:
    """``{ticker: {date: close}}`` from the desk's OWN settled mark series.

    The desk is the single mark owner; this reads its published series and calls no
    provider. A ticker with no series simply has no prices, and the kernel's
    coverage floor then withholds the measurement instead of inventing one.
    """
    series = (marks or {}).get("series") or {}
    out: dict[str, dict] = {}
    for tk in sorted(tickers):
        rows = series.get(tk) or []
        hist: dict[str, float] = {}
        for row in rows:
            try:
                d, v = row[0], row[1]
            except (IndexError, TypeError, KeyError):
                continue
            if d is None or v is None:
                continue
            try:
                hist[str(d)] = float(v)
            except (TypeError, ValueError):
                continue
        if hist:
            out[tk] = hist
    return out


def measure_record(*, record: dict, marks: Optional[dict] = None,
                   desk_dir=None, policy: Optional[dict] = None) -> dict:
    """Measure ONE frozen record forward against the desk's settled marks."""
    if marks is None:
        marks = desk.read_marks(desk_dir)
    paths = (record or {}).get("paths") or {}
    tickers: set = set()
    for p in paths.values():
        tickers |= set((p or {}).get("basket") or {})
    history = _price_history(marks, tickers)
    dates = sorted({d for series in history.values() for d in series})
    pit = kernel.point_in_time_check(
        record=record,
        evidence_dates=[d for d in dates
                        if str(d) > str(record.get("eligible_market_date") or "")])
    measured = kernel.measure_paths(record=record, price_history=history,
                                    measurement_dates=dates, policy=policy)
    measured["point_in_time"] = pit
    measured["mark_owner"] = "api.paper_trading_desk"
    measured["marks_latest_date"] = desk.marks_latest_date(marks)
    return measured


def _safety() -> dict:
    blk = dict(kernel.safety_block())
    blk.update({"preview_only": True, "wrote_to_ledger": False,
                "wrote_to_database": False, "called_provider": False,
                "called_prediction": False, "automatic_approval_allowed": False,
                "automatic_rebalance_allowed": False})
    return blk


def load_portfolio_decision_outcomes(*, outcome_dir=None, desk_dir=None,
                                     marks: Optional[dict] = None,
                                     policy: Optional[dict] = None,
                                     now: Optional[datetime] = None) -> dict:
    """The GET read contract: every frozen decision and its forward evidence.

    READ-ONLY and degrade-safe. It writes nothing, and a record it cannot measure is
    reported as unmeasured rather than quietly omitted.
    """
    generated_at = _now_iso(now)
    try:
        records = load_records(outcome_dir=outcome_dir)
    except Exception as exc:  # noqa: BLE001 - a read must never crash the caller
        return {"phase": PHASE, "owner": OWNER, "status": "UNAVAILABLE",
                "generated_at": generated_at,
                "message": "Decision-outcome ledger unavailable: %s" % str(exc)[:160],
                "decisions": [], "decision_count": 0, **_safety()}

    if marks is None:
        try:
            marks = desk.read_marks(desk_dir)
        except Exception:  # noqa: BLE001
            marks = {"series": {}, "latest_completed_date": None}

    rows: list[dict] = []
    for rec in records:
        try:
            m = measure_record(record=rec, marks=marks, policy=policy)
        except Exception as exc:  # noqa: BLE001
            m = {"state": kernel.M_UNMEASURABLE, "verdict": kernel.V_PENDING,
                 "detail": str(exc)[:160]}
        rows.append({
            "decision_id": rec.get("decision_id"),
            "frozen_at": rec.get("frozen_at"),
            "eligible_market_date": rec.get("eligible_market_date"),
            "active_book_id": rec.get("active_book_id"),
            "record_hash": rec.get("record_hash"),
            "nav_at_decision": rec.get("nav_at_decision"),
            "transaction_cost": rec.get("transaction_cost"),
            "previous_position_count": len(rec.get("previous_portfolio") or {}),
            "executed_position_count": len(rec.get("executed_target") or {}),
            "executed_matches_proposed": rec.get("executed_matches_proposed"),
            "decision_reasons": rec.get("decision_reasons") or {},
            "expected_improvement": rec.get("expected_improvement") or {},
            "constraints_at_decision": rec.get("constraints_at_decision") or {},
            "model_state": rec.get("model_state") or {},
            "paths": {k: {"path_kind": v.get("path_kind"),
                          "position_count": v.get("position_count"),
                          "invested_weight": v.get("invested_weight"),
                          "cash_weight": v.get("cash_weight"),
                          "transaction_cost_charged":
                              v.get("transaction_cost_charged"),
                          "basket": v.get("basket")}
                      for k, v in (rec.get("paths") or {}).items()},
            "forward_evidence": m,
        })

    measured = [r for r in rows
                if (r["forward_evidence"] or {}).get("state") == kernel.M_MEASURED]
    verdicts = {v: sum(1 for r in measured
                       if (r["forward_evidence"] or {}).get("verdict") == v)
                for v in kernel.VERDICT_VOCAB}
    total_incr = sum((r["forward_evidence"] or {}).get("incremental_pnl") or 0.0
                     for r in measured)
    total_cost = sum(r.get("transaction_cost") or 0.0 for r in rows)
    return {
        "phase": PHASE, "owner": OWNER, "status": "OK",
        "generated_at": generated_at,
        "calculation_owner": CALCULATION_OWNER,
        "ledger_root_env": OUTCOME_DIR_ENV,
        "decisions": rows,
        "decision_count": len(rows),
        "measured_count": len(measured),
        "pending_count": len(rows) - len(measured),
        "verdict_counts": verdicts,
        "verdict_vocabulary": list(kernel.VERDICT_VOCAB),
        "state_vocabulary": list(kernel.MEASUREMENT_STATE_VOCAB),
        "path_vocabulary": list(kernel.PATH_VOCAB),
        "cumulative_incremental_pnl": round(total_incr, 2),
        "cumulative_transaction_cost": round(total_cost, 2),
        "improvement_basis": "EXECUTED_MINUS_FROZEN_COUNTERFACTUAL_HOLD_NET_OF_COST",
        "counterfactual_frozen_prospectively": True,
        "counterfactual_doc": (
            "Both paths are frozen when the reallocation executes, before any "
            "forward price exists. Nothing here is reconstructed with hindsight."),
        "separate_from_research_alpha": True,
        "separation_doc": (
            "PORTFOLIO_DECISION_ALPHA measures executed capital decisions on the "
            "operational paper book. Release-46 challenger alpha measures research "
            "signals in shadow. The two ledgers are separate and are never summed."),
        "marks_latest_date": desk.marks_latest_date(marks or {}),
        **_safety(),
    }


__all__ = [
    "PHASE", "OWNER", "CALCULATION_OWNER", "OUTCOME_DIR_ENV",
    "PATH_EXECUTED", "PATH_HOLD", "FREEZE_STATUS_VOCAB",
    "F_CREATED", "F_REUSED", "F_REFUSED_NOT_EXECUTED", "F_REFUSED_INCOMPLETE",
    "freeze_executed_decision", "load_records", "load_record", "measure_record",
    "load_portfolio_decision_outcomes",
]
