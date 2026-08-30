r"""api/decision_snapshot.py - Release 50: the ONE read-consistent decision snapshot
and the read fan-out behind every normal operator surface.

The defect
----------
Release 49 measured ~4.6 s for one operator presentation and ~106 heavyweight
owner calls behind one cold Portfolio load: every route composed the same
authoritative owners independently (portfolio state, NAV replay, the Release-47
read contract, the rebalance state, the workflow state, the Daily Close), so a
page that issued nine GETs paid for nine full compositions of one truth.

The replacement
---------------
ONE snapshot IDENTITY, read from the stores that can change a decision, and ONE
composition per identity. The identity is a fingerprint over:

    market date (the eligible session, from the calendar owner)
    holdings / cash / NAV / marks (the desk ledgers and the mark store)
    the corporate-action registry
    signal identity (the model-input / target ledger roots)
    HOC + reassessment + DRC artifacts (the research-cycle roots)
    proposal / decision / order-plan / decision-outcome ledgers
    the store-root environment (a hermetic process never shares a memo)

A snapshot is served while its identity still matches the stores; the moment any
of them changes (a Daily Close, a portfolio cycle, an approval, a confirmation, a
settled fill, a registered corporate action, a new session) the identity differs
and the snapshot is REGENERATED from the canonical owners. A short absolute age
bound is a second safety valve, never the invalidation rule. This is not a stale
cache: nothing is served whose inputs have moved.

It is READ-ONLY and it is NOT a business-calculation owner: it calls the owners
that already own NAV, target, decision, constraint, proposal, execution state and
presentation, exactly once each per identity, and hands their payloads out with
the identity stamped on. It computes no number of its own.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

PHASE = "R50"
OWNER = "api.decision_snapshot"
SCHEMA_VERSION = "decision_snapshot.v1"
ROUTE = "/v1/operations/decision-snapshot"

#: Absolute age bound (seconds) - a safety valve, NEVER the invalidation rule.
MAX_AGE_SECONDS = 180.0

SECTIONS = ("operational", "portfolio_state", "workflow", "daily_close",
            "decision_lane", "rebalance", "constrained", "capital_pool",
            "material_information", "decision_outcomes", "information_collection",
            "presentation")

SECTION_OWNERS = {
    "operational": "api.operational_book.load_operational_book",
    "portfolio_state": "api.portfolio_state.load_portfolio_state",
    "workflow": "api.workflow_state.load_workflow_state",
    "daily_close": "api.daily_close.load_daily_close",
    "decision_lane": "api.portfolio_decision.load_portfolio_decision",
    "rebalance": "api.rebalance_execution.load_rebalance_state",
    "constrained": "api.reallocation_proposal.load_constrained_reallocation",
    "capital_pool": "api.capital_pool.load_capital_pool",
    "material_information": "api.material_information.build",
    "decision_outcomes": "api.portfolio_decision_outcome.load_portfolio_decision_outcomes",
    "information_collection": "api.information_collection.resolve_service_lifecycle",
    "presentation": "api.operator_presentation.build_operator_presentation",
}

#: Store-root environment variables whose values are part of the identity.
_STORE_ENV = (
    "PAPER_TRADER_DESK_DIR", "PAPER_TRADER_LEDGER_DIR", "PAPER_TRADER_HOC_DIR",
    "PAPER_TRADER_REALLOC_DIR", "PAPER_TRADER_REASSESSMENT_DIR",
    "PAPER_TRADER_REASSESSMENT_OUTCOME_DIR", "PAPER_TRADER_DRC_DIR",
    "PAPER_TRADER_PORTFOLIO_DECISION_DIR", "PAPER_TRADER_REBALANCE_PLAN_DIR",
    "PAPER_TRADER_CORPORATE_ACTIONS_DIR", "PAPER_TRADER_PORTFOLIO_DECISION_OUTCOME_DIR",
    "PAPER_TRADER_MHZ_INPUTS_DIR", "PAPER_TRADER_MHZ_LEDGER_DIR",
    "PAPER_TRADER_ACCEPTANCE_MODE", "PAPER_TRADER_DESK_MARKS_FIXTURE",
    "PAPER_TRADER_REFERENCE_DATA_FIXTURE",
)

_LOCK = threading.Lock()
_MEMO: dict[str, Any] = {"identity_hash": None, "built_at": 0.0, "payload": None,
                         "hits": 0, "builds": 0}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fingerprint_dir(path: Optional[Path], *, pattern: str = "*.json", limit: int = 400) -> list:
    """``[(name, size, mtime_ns)]`` of the files that can change a decision under
    one store root. Cheap (stat only); a missing root is an empty list."""
    out = []
    try:
        if path is None or not Path(path).exists():
            return out
        files = sorted(Path(path).glob(pattern))[:limit]
        for f in files:
            try:
                st = f.stat()
                out.append((f.name, st.st_size, st.st_mtime_ns))
            except OSError:
                continue
    except Exception:  # noqa: BLE001
        return out
    return out


def _roots() -> dict:
    """Every store root that can change a decision, resolved through the OWNER
    that owns it (never a second path definition). Degrade-safe."""
    roots: dict[str, Optional[Path]] = {}

    def _try(name, fn):
        try:
            roots[name] = Path(fn())
        except Exception:  # noqa: BLE001
            roots[name] = None

    from paper_trader.api import paper_trading_desk as desk
    _try("desk", lambda: desk._desk_dir(None))
    from paper_trader.api import reallocation_proposal as rp
    _try("reallocation", lambda: rp._realloc_dir(None))
    from paper_trader.api import holding_opportunity_cost as hoc
    _try("hoc", lambda: hoc._hoc_dir(None))
    from paper_trader.api import portfolio_reassessment as prs
    _try("reassessment", lambda: prs._reassessment_dir(None))
    from paper_trader.api import portfolio_decision as pdec
    _try("decision", lambda: pdec._decision_dir(None))
    from paper_trader.api import rebalance_execution as rex
    _try("order_plan", lambda: rex._plan_dir(None))
    from paper_trader.api import portfolio_decision_outcome as pdo
    _try("decision_outcome", lambda: pdo._outcome_dir(None))
    from paper_trader.api import corporate_actions as ca
    _try("corporate_actions", lambda: ca._ca_dir(None))
    from paper_trader.api import daily_research_cycle as drc
    _try("drc_runs", lambda: drc._runs_dir(None))
    from paper_trader.api import multi_horizon_ledger as mhz
    _try("mhz_ledger", lambda: mhz._ledger_dir(None))
    return roots


def snapshot_identity() -> dict:
    """The identity every served snapshot is bound to. Stat-only; no owner runs."""
    roots = _roots()
    fp = {name: _fingerprint_dir(p) for name, p in roots.items()}
    # research-cycle runs are one file per run; the newest one is what matters
    if roots.get("drc_runs") and roots["drc_runs"].exists():
        fp["drc_runs"] = _fingerprint_dir(roots["drc_runs"])[-5:]
    try:
        from paper_trader.api import alpha_target as at
        eligible = at.latest_completed()
    except Exception:  # noqa: BLE001
        eligible = None
    ident = {
        "schema_version": SCHEMA_VERSION,
        "eligible_market_date": eligible,
        "store_fingerprints": {k: v for k, v in sorted(fp.items())},
        "store_env": {k: os.environ.get(k) for k in _STORE_ENV if os.environ.get(k)},
        "roots": {k: (str(v) if v is not None else None) for k, v in sorted(roots.items())},
    }
    ident["identity_hash"] = hashlib.sha256(
        json.dumps({k: v for k, v in ident.items() if k != "roots"}, sort_keys=True,
                   default=str).encode("utf-8")).hexdigest()
    return ident


def _compose(identity: dict) -> dict:
    """ONE composition of every owner behind the normal surfaces. Each heavy
    owner runs exactly once; the ONE portfolio state / operational book is
    passed to every consumer that accepts it."""
    from paper_trader.api import operational_book as ob
    from paper_trader.api import portfolio_state as pst
    from paper_trader.api import workflow_state as ws
    from paper_trader.api import daily_close as dc
    from paper_trader.api import portfolio_decision as pdm
    from paper_trader.api import rebalance_execution as rex
    from paper_trader.api import reallocation_proposal as rp
    from paper_trader.api import capital_pool as cp
    from paper_trader.api import operator_presentation as op

    timings: dict[str, float] = {}
    warnings: list[str] = []

    def _timed(name: str, fn: Callable):
        t0 = time.perf_counter()
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - one owner failing never hides the rest
            warnings.append("%s unavailable: %s" % (name, str(exc)[:160]))
            return None
        finally:
            timings[name] = round(time.perf_counter() - t0, 3)

    operational = _timed("operational", ob.load_operational_book)
    ps = _timed("portfolio_state", lambda: pst.load_portfolio_state(operational=operational))
    workflow = _timed("workflow", lambda: ws.load_workflow_state(operational=operational))
    daily_close = _timed("daily_close", lambda: dc.load_daily_close(operational=operational))
    lane = _timed("decision_lane", lambda: pdm.load_portfolio_decision(portfolio_state=ps))
    rebalance = _timed("rebalance", lambda: rex.load_rebalance_state(portfolio_state=ps))
    constrained = _timed("constrained", lambda: rp.load_constrained_reallocation(
        portfolio_state=ps, decision_lane=lane, rebalance=rebalance))
    capital_pool = _timed("capital_pool", lambda: cp.load_capital_pool(portfolio_state=ps))
    lds = op.owner_loaders(portfolio_state=ps)
    material = _timed("material_information", lds["material_information"])
    outcomes = _timed("decision_outcomes", lds["decision_outcomes"])
    collection = _timed("information_collection", lds["information_collection"])
    presentation = _timed("presentation", lambda: op.build_operator_presentation(
        workflow=workflow, constrained=constrained, daily_close=daily_close,
        material_information=material, decision_outcomes=outcomes,
        information_collection=collection, capital_pool=capital_pool,
        warnings=list(warnings)))
    stamp = {
        "identity_hash": identity["identity_hash"],
        "eligible_market_date": identity.get("eligible_market_date"),
        "computed_at": _now_iso(),
        "owner": OWNER,
    }
    payload = {
        "schema_version": SCHEMA_VERSION, "phase": PHASE, "owner": OWNER, "route": ROUTE,
        "identity": identity,
        "computed_at": stamp["computed_at"],
        "sections": {
            "operational": operational, "portfolio_state": ps, "workflow": workflow,
            "daily_close": daily_close, "decision_lane": lane, "rebalance": rebalance,
            "constrained": constrained, "capital_pool": capital_pool,
            "material_information": material, "decision_outcomes": outcomes,
            "information_collection": collection, "presentation": presentation,
        },
        "section_owners": dict(SECTION_OWNERS),
        "timings_seconds": timings,
        "total_seconds": round(sum(timings.values()), 3),
        "warnings": warnings,
        "references": {
            "market_date": (ps or {}).get("dates", {}).get("eligible_market_date") if ps else None,
            "portfolio_state_hash": (ps or {}).get("state_hash") if ps else None,
            "economic_state_hash": (ps or {}).get("economic_state_hash") if ps else None,
            "holdings_fingerprint": hashlib.sha256(json.dumps(
                sorted((p.get("ticker"), p.get("quantity")) for p in ((ps or {}).get("positions") or [])),
                default=str).encode("utf-8")).hexdigest()[:16] if ps else None,
            "nav": ((ps or {}).get("capital") or {}).get("nav") if ps else None,
            "marks_identity": ((operational or {}).get("operational_book") or {}).get("desk_mark_date")
                              if operational else None,
            "signal_identity": ((workflow or {}).get("operational_state") or {}).get("target_calculation_date")
                               if workflow else None,
            "reassessment_identity": ((workflow or {}).get("portfolio_reassessment") or {}).get("assessment_hash")
                                     if workflow else None,
            "hoc_identity": ((workflow or {}).get("portfolio_reassessment") or {}).get("hoc_assessment_hash")
                            if workflow else None,
            "frontier_identity": (((constrained or {}).get("multi_asset") or {}).get("frontier_hash")
                                  if constrained else None),
            "zero_base_target_identity": None,
            "feasible_target_identity": (lane or {}).get("proposal_hash") if lane else None,
            "switching_economics_identity": ((constrained or {}).get("switching_economics") or {}).get(
                "switching_policy_version") if constrained else None,
            "proposal_identity": (lane or {}).get("proposal_id") if lane else None,
            "execution_state": (rebalance or {}).get("rebalance_state") if rebalance else None,
            "workflow_state": (workflow or {}).get("overall_state") if workflow else None,
            "presentation_decision": ((presentation or {}).get("portfolio_decision") or {}).get("state")
                                     if presentation else None,
        },
        "read_only": True, "business_calculation_owner": False, "recomputes_nothing": True,
        "invalidation": "IDENTITY_CHANGE (any decision-relevant store or the market date)",
        "max_age_seconds": MAX_AGE_SECONDS,
    }
    return payload


def _stamp(payload: dict, identity: dict) -> dict:
    """Bind every served section to the identity it was composed under."""
    stamp = {"identity_hash": identity["identity_hash"],
             "eligible_market_date": identity.get("eligible_market_date"),
             "computed_at": payload.get("computed_at") or _now_iso(), "owner": OWNER}
    for name, sec in (payload.get("sections") or {}).items():
        if isinstance(sec, dict):
            sec["decision_snapshot"] = dict(stamp, section=name)
    return payload


def load_decision_snapshot(*, force: bool = False) -> dict:
    """The ONE snapshot. Served while its identity matches; regenerated otherwise."""
    ident = snapshot_identity()
    with _LOCK:
        memo = _MEMO
        fresh = (memo["payload"] is not None and memo["identity_hash"] == ident["identity_hash"]
                 and (time.monotonic() - memo["built_at"]) <= MAX_AGE_SECONDS)
        if fresh and not force:
            memo["hits"] += 1
            out = dict(memo["payload"])
            out["served_at"] = _now_iso()
            out["served_from"] = "SNAPSHOT_IDENTITY_MATCH"
            out["snapshot_hits"] = memo["hits"]
            out["snapshot_builds"] = memo["builds"]
            return out
    payload = _stamp(_compose(ident), ident)
    with _LOCK:
        _MEMO.update({"identity_hash": ident["identity_hash"], "built_at": time.monotonic(),
                      "payload": payload, "builds": _MEMO["builds"] + 1})
        payload["served_at"] = _now_iso()
        payload["served_from"] = "REGENERATED_FROM_CANONICAL_OWNERS"
        payload["snapshot_hits"] = _MEMO["hits"]
        payload["snapshot_builds"] = _MEMO["builds"]
    return payload


def section(name: str, *, force: bool = False) -> dict:
    """One owner's payload, served through the snapshot (the route fan-out)."""
    if name not in SECTIONS:
        raise KeyError(name)
    snap = load_decision_snapshot(force=force)
    sec = (snap.get("sections") or {}).get(name)
    if not isinstance(sec, dict):
        return {"status": "UNAVAILABLE", "owner": SECTION_OWNERS.get(name),
                "decision_snapshot": {"identity_hash": snap["identity"]["identity_hash"],
                                      "section": name, "owner": OWNER},
                "warnings": snap.get("warnings")}
    return sec


def summary() -> dict:
    """The identity + references without the heavy sections (the GET route)."""
    snap = load_decision_snapshot()
    return {k: v for k, v in snap.items() if k != "sections"} | {
        "section_status": {k: ("OK" if isinstance(v, dict) else "UNAVAILABLE")
                           for k, v in (snap.get("sections") or {}).items()}}


def reset() -> None:
    with _LOCK:
        _MEMO.update({"identity_hash": None, "built_at": 0.0, "payload": None,
                      "hits": 0, "builds": 0})


__all__ = ["PHASE", "OWNER", "SCHEMA_VERSION", "ROUTE", "SECTIONS", "SECTION_OWNERS",
           "MAX_AGE_SECONDS", "snapshot_identity", "load_decision_snapshot", "section",
           "summary", "reset"]
