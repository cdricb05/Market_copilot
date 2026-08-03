"""
alpha_agent.market_bar_jobs — Stage 10.3 CANONICAL MARKET_BAR REPAIR JOBS + PLANNER.

The reusable capability the AlphaAgent needs to OPERATE the survivorship-safe
historical price-panel repair that closes the LAST blocker on the historical
fundamental-alpha path — Stage 10.2 unlocked deep owned PIT fundamentals but the
experiment still reached only 11 scored periods (< 12) because the evaluator read
the price panel through the truncated ``NormalizedStore(max_files=6000,
max_symbols=300)`` reader (earliest-6000-files slice ends 2018-12-20) even though
the owned normalized MARKET_BAR tree already spans 2015-01-02 .. 2026-07-31 with
500+ survivorship-safe securities. Claude builds the reusable capability
(diagnosis, canonical assetid-anchored panel reader, per-rebalance price readiness,
store-derived gate); the AlphaAgent prioritizes, creates, claims and executes the
jobs through the SAME durable ``ResearchQueue`` + one-job-per-cycle fair scheduler,
re-evaluates readiness and (when the combined measured gate opens) the tournament
tick generates + runs the fundamental experiment on the REPAIRED panel.

A distinct Stage 10.3 campaign (origin ``stage10.3-marketbar``, allowlisted
``market_bar.`` lane prefix) that reuses EXACTLY the existing infrastructure: the
owned normalized MARKET_BAR store, the assetid-anchored identity store, the owned
companyfacts PIT store and the measured readiness contract. Every job is BOUNDED,
IDEMPOTENT, RESTART-SAFE, FAILURE-ISOLATED, DEDUPLICATED and CONFIG-CONTROLLED;
completion flags are stamped by the HANDLER on success (never at plan time). No
second worker/queue/scheduler; at most one live market_bar job at a time.

Lanes (all origin ``stage10.3-marketbar``, prefix ``market_bar.``):
  * market_bar.coverage_diagnose  (DATA_VALIDATION) — diagnose the owned MARKET_BAR
    coverage + the exact per-rebalance price/identity overlap that limited the
    truncated reader to 11 periods.
  * market_bar.panel_validate     (DATA_VALIDATION) — build + validate the canonical
    assetid-anchored survivorship-safe panel over the full owned history.
  * market_bar.readiness_recheck  (DATA_VALIDATION) — measure per-rebalance PRICE
    readiness (REAL evaluator scored periods) per signal on the repaired panel.
  * market_bar.experiment_recheck (DATA_VALIDATION) — record the COMBINED measured
    gate (fundamental PIT readiness AND price readiness). When open, the tournament
    tick — never this handler — generates the fundamental experiment.

Pure stdlib; deterministic; the only side effects are identity-store meta flags,
immutable artifacts and enqueues into the shared research queue. Never writes an
operational ledger / order / fill / signal / trade decision / model / price row.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from . import historical_identity as _hi
from .identity_jobs import write_artifact

ORIGIN_103 = "stage10.3-marketbar"
MARKET_BAR_LANE_PREFIX = "market_bar."

LANE_MB_COVERAGE_DIAGNOSE = "market_bar.coverage_diagnose"
LANE_MB_PANEL_VALIDATE = "market_bar.panel_validate"
LANE_MB_READINESS_RECHECK = "market_bar.readiness_recheck"
LANE_MB_EXPERIMENT_RECHECK = "market_bar.experiment_recheck"

_VALIDATION_LANES = frozenset({
    LANE_MB_COVERAGE_DIAGNOSE, LANE_MB_PANEL_VALIDATE,
    LANE_MB_READINESS_RECHECK, LANE_MB_EXPERIMENT_RECHECK})

# Below the identity backlog (6), companyfacts bootstrap (5) and the Stage 10.2
# fundamentals campaign (4): the upstream identity + fundamental work keeps
# precedence; this finite panel-repair backlog can never permanently starve the
# tournament.
DEFAULT_PRIORITY = 3
DEFAULT_SIGNALS = ("gross_profitability", "asset_growth", "balance_sheet_quality")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# --------------------------------------------------------------------------- #
# Market-bar job context (all heavy dependencies injected for testability).
# --------------------------------------------------------------------------- #
@dataclass
class MarketBarJobContext:
    store: _hi.IdentityStore                      # the Stage 10 identity store
    ingestion_root: str                           # owned normalized MARKET_BAR root
    artifact_root: str
    rebalance_dates: "list[str]" = field(default_factory=list)
    cfg9: dict = field(default_factory=dict)
    stage103: dict = field(default_factory=dict)  # the stage10_3 config block
    index_name: str = "S&P 500"
    signals: "tuple" = DEFAULT_SIGNALS
    sources: "tuple" = ("norgate_local",)
    horizon_days: int = 63
    pit_store: Any = None                          # injectable (tests); else built
    clock: Optional[Callable[[], str]] = None

    def now(self) -> str:
        return (self.clock or _utc_now_iso)()


def _outcomes():
    from . import autonomous_research as _ar
    return (_ar.OUTCOME_COMPLETED, _ar.OUTCOME_BLOCKED_SPECIFIC,
            _ar.OUTCOME_RETRYABLE)


def _cfg(ctx: MarketBarJobContext) -> dict:
    return ctx.stage103 or {}


def _price_thresholds(ctx: MarketBarJobContext) -> dict:
    from . import historical_price_panel as _hpp
    thr = dict(_hpp.DEFAULT_PRICE_READINESS)
    over = (_cfg(ctx).get("price_readiness") or {})
    for k in thr:
        if over.get(k) is not None:
            thr[k] = over[k]
    return thr


def _mapping_version(ctx: MarketBarJobContext) -> str:
    return (ctx.store.get_meta("stage101_mapping_version_hash")
            or ctx.store.get_meta("stage102_mapped_epoch") or "nomvh")


def _epoch(ctx: MarketBarJobContext) -> str:
    """(owned-price coverage signature, identity mapping-version). Changes only
    when the owned MARKET_BAR history grows OR the resolved mapping changes, so each
    measurement runs once per epoch and re-runs honestly when evidence changes."""
    from . import historical_price_panel as _hpp
    pe = _hpp.panel_coverage_epoch(ctx.ingestion_root, sources=ctx.sources)
    return "%s:%s" % (pe, str(_mapping_version(ctx))[:12])


def _pit_store(ctx: MarketBarJobContext):
    if ctx.pit_store is not None:
        return ctx.pit_store
    from . import fundamental_readiness as _fr
    return _fr.open_companyfacts_pit_store(ctx.cfg9, store=ctx.store)


# --------------------------------------------------------------------------- #
# Per-lane handlers. Each is bounded, idempotent, restart-safe and returns
# (outcome, detail) with an immutable artifact path or an exact blocker.
# --------------------------------------------------------------------------- #
def _mb_coverage_diagnose(ctx: MarketBarJobContext, job) -> tuple:
    """Diagnose the owned MARKET_BAR coverage + the exact per-rebalance survivorship
    universe vs owned-price (assetid) overlap that limited the truncated reader to
    11 scored periods. Records a durable diagnosis snapshot; stamps the epoch."""
    OK, BLK, RETRY = _outcomes()
    from . import historical_price_panel as _hpp
    sig = _hpp.panel_coverage_signature(ctx.ingestion_root, sources=ctx.sources)
    panel = _hpp.build_assetid_price_panel(ctx.ingestion_root, sources=ctx.sources)
    if not panel:
        return BLK, {"real_work": LANE_MB_COVERAGE_DIAGNOSE,
                     "blocker": "NO_OWNED_MARKET_BAR",
                     "reason": ("no owned assetid-bearing MARKET_BAR records under "
                                "%s for sources %s" % (ctx.ingestion_root,
                                                       list(ctx.sources))),
                     "disposition": "DATA_HOLD"}
    panel_assetids = set(panel.keys())
    pdates = sorted({d for bars in panel.values() for d, _ in bars})
    per_date = []
    for d in ctx.rebalance_dates:
        univ = ctx.store.historical_universe_on(d, index_name=ctx.index_name)
        mapped = [u for u in univ if u.get("cik")
                  and u.get("mapping_status") == _hi.STATUS_RESOLVED]
        aids = {str(u.get("norgate_assetid")) for u in mapped
                if u.get("norgate_assetid")}
        per_date.append({"as_of": d, "universe": len(univ),
                         "mapped_resolved": len(mapped),
                         "price_assetid_overlap": len(aids & panel_assetids)})
    diag = {"coverage_signature": sig,
            "panel_assetids": len(panel),
            "panel_date_start": pdates[0] if pdates else None,
            "panel_date_end": pdates[-1] if pdates else None,
            "panel_trading_days": len(pdates),
            "truncated_reader_note": (
                "the legacy NormalizedStore(max_files=6000, max_symbols=300) reader "
                "truncated the owned history to its earliest 6000 files (ending "
                "~2018-12-20) and 300 symbols, yielding only 11 quarterly scored "
                "periods; the canonical assetid panel reads the full owned history"),
            "per_date": per_date}
    ctx.store.set_meta("stage103_diagnosis", json.dumps(diag)[:60000])
    ctx.store.set_meta("stage103_diagnose_complete", _epoch(ctx))
    detail = {"real_work": LANE_MB_COVERAGE_DIAGNOSE, **{
        k: diag[k] for k in ("panel_assetids", "panel_date_start",
                             "panel_date_end", "panel_trading_days")},
        "rebalance_dates": len(ctx.rebalance_dates),
        "disposition": "DATA_HOLD", "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _mb_panel_validate(ctx: MarketBarJobContext, job) -> tuple:
    """Build + validate the canonical assetid-anchored survivorship-safe panel over
    the FULL owned MARKET_BAR history and the resolved CIK->assetid join. Validates
    the identity join is assetid-keyed (never ticker-only) and records the panel
    summary. Stamps the epoch."""
    OK, BLK, RETRY = _outcomes()
    from . import historical_price_panel as _hpp
    panel, c2a = _hpp.build_repaired_panel(ctx.ingestion_root, ctx.store,
                                           sources=ctx.sources)
    if not panel:
        return BLK, {"real_work": LANE_MB_PANEL_VALIDATE,
                     "blocker": "NO_OWNED_MARKET_BAR",
                     "reason": "canonical panel is empty",
                     "disposition": "DATA_HOLD"}
    joined = sum(1 for a in c2a.values() if a in panel)
    pdates = sorted({d for bars in panel.values() for d, _ in bars})
    summary = {"panel_assetids": len(panel),
               "cik_to_assetid": len(c2a),
               "cik_with_owned_price_series": joined,
               "date_start": pdates[0] if pdates else None,
               "date_end": pdates[-1] if pdates else None,
               "trading_days": len(pdates),
               "identity_key": "norgate_assetid (stable; ticker text never the "
                               "sole key)"}
    ctx.store.set_meta("stage103_panel_summary", json.dumps(summary))
    ctx.store.set_meta("stage103_panel_complete", _epoch(ctx))
    detail = {"real_work": LANE_MB_PANEL_VALIDATE, **summary,
              "disposition": "DATA_HOLD", "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _mb_readiness_recheck(ctx: MarketBarJobContext, job) -> tuple:
    """Measure per-rebalance PRICE readiness (REAL evaluator scored periods) for
    every candidate on the repaired assetid panel + owned PIT store. Total price
    rows / current-company coverage can never open it — only real scored periods
    reaching min_scored_periods. Persists a durable snapshot; stamps the epoch."""
    OK, BLK, RETRY = _outcomes()
    from . import historical_price_panel as _hpp
    pit = _pit_store(ctx)
    if pit is None:
        return BLK, {"real_work": LANE_MB_READINESS_RECHECK,
                     "blocker": "NO_OWNED_PIT_STORE",
                     "reason": ("owned companyfacts PIT store unavailable; run the "
                                "Stage 10.2 fundamentals campaign first"),
                     "disposition": "DATA_HOLD"}
    panel, c2a = _hpp.build_repaired_panel(ctx.ingestion_root, ctx.store,
                                           sources=ctx.sources)
    thr = _price_thresholds(ctx)
    snapshot = {}
    any_open = False
    for sig in ctx.signals:
        rr = _hpp.per_rebalance_price_readiness(
            pit, panel, c2a, sig, horizon_days=ctx.horizon_days,
            thresholds=thr)
        any_open = any_open or bool(rr["sufficient"])
        snapshot[sig] = {k: rr[k] for k in (
            "sufficient", "blocker", "valid_scored_periods", "min_scored_periods",
            "formation_dates", "panel_assetids", "min_names_in_scored_period",
            "median_names_in_period", "earliest_scored_date", "latest_scored_date")}
    ctx.store.set_meta("stage103_price_readiness_snapshot", json.dumps(snapshot))
    ctx.store.set_meta("stage103_price_readiness_open", "1" if any_open else "0")
    # The owned-price coverage epoch the tournament folds into the fundamental
    # experiment spec, so a repaired/extended panel regenerates the experiment.
    ctx.store.set_meta("stage103_panel_epoch",
                       _hpp.panel_coverage_epoch(ctx.ingestion_root,
                                                 sources=ctx.sources))
    ctx.store.set_meta("stage103_readiness_complete", _epoch(ctx))
    detail = {"real_work": LANE_MB_READINESS_RECHECK,
              "price_readiness_open_any_signal": any_open,
              "readiness": snapshot, "disposition": "DATA_HOLD",
              "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _mb_experiment_recheck(ctx: MarketBarJobContext, job) -> tuple:
    """Record the FINAL COMBINED measured gate: a fundamental experiment is
    eligible for a signal only when BOTH the Stage 10.2 fundamental PIT readiness
    AND the Stage 10.3 price readiness (>=12 real scored periods) are measured
    sufficient. When open, the tournament tick — NOT this handler — generates the
    canonical fundamental experiment on the repaired panel. Stamps the epoch; never
    enqueues an experiment; never flips a switch; never moves a lifecycle."""
    OK, BLK, RETRY = _outcomes()
    fund = {}
    price = {}
    try:
        fund = json.loads(ctx.store.get_meta("stage102_readiness_snapshot") or "{}")
    except (ValueError, TypeError):
        fund = {}
    try:
        price = json.loads(
            ctx.store.get_meta("stage103_price_readiness_snapshot") or "{}")
    except (ValueError, TypeError):
        price = {}
    combined = {}
    any_open = False
    for sig in ctx.signals:
        f_ok = bool((fund.get(sig) or {}).get("sufficient"))
        p_ok = bool((price.get(sig) or {}).get("sufficient"))
        open_ = f_ok and p_ok
        any_open = any_open or open_
        combined[sig] = {"fundamental_readiness": f_ok, "price_readiness": p_ok,
                         "combined_open": open_}
    ctx.store.set_meta("stage103_combined_gate_open", "1" if any_open else "0")
    ctx.store.set_meta("stage103_experiment_complete", _epoch(ctx))
    detail = {"real_work": LANE_MB_EXPERIMENT_RECHECK,
              "combined_gate_open_any_signal": any_open, "gate": combined,
              "note": ("when open, the tournament tick generates the fundamental "
                       "experiment on the repaired panel; never enqueued here"),
              "safety_switch_flipped": False, "disposition": "DATA_HOLD",
              "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


_LANE_HANDLERS = {
    LANE_MB_COVERAGE_DIAGNOSE: _mb_coverage_diagnose,
    LANE_MB_PANEL_VALIDATE: _mb_panel_validate,
    LANE_MB_READINESS_RECHECK: _mb_readiness_recheck,
    LANE_MB_EXPERIMENT_RECHECK: _mb_experiment_recheck,
}


def dispatch_market_bar_job(job, ctx: MarketBarJobContext) -> tuple:
    """Dispatch ONE claimed Stage 10.3 job to its lane handler. Bounded, idempotent
    (a job already recorded in the identity store's processed_jobs is a no-op
    COMPLETED), restart-safe and failure-isolated (a handler exception becomes a
    bounded RETRYABLE — never crashes the drain). Returns (outcome, detail)."""
    from . import autonomous_research as _ar
    lane = str(getattr(job, "lane", ""))
    handler = _LANE_HANDLERS.get(lane)
    if handler is None:
        return _ar.OUTCOME_BLOCKED_SPECIFIC, {
            "reason": "no market_bar handler for lane %s" % lane}
    job_key = "%s:%s" % (lane, _hi.content_hash(job.payload or {})[:16])
    if ctx.store.already_processed(job_key):
        return _ar.OUTCOME_COMPLETED, {"real_work": lane,
                                       "idempotent_skip": True,
                                       "disposition": "DATA_HOLD"}
    try:
        outcome, detail = handler(ctx, job)
    except Exception as exc:  # noqa: BLE001 - never crash the drain
        return _ar.OUTCOME_RETRYABLE, {
            "reason": "market_bar handler raised: %s" % type(exc).__name__,
            "lane": lane}
    if outcome == _ar.OUTCOME_COMPLETED:
        ctx.store.mark_processed(job_key, job_id=getattr(job, "job_id", None),
                                 kind=lane)
    detail.setdefault("lane", lane)
    return outcome, detail


# --------------------------------------------------------------------------- #
# Autonomous dependency planner (single highest-value next step, <=1 live job).
# --------------------------------------------------------------------------- #
def _has_live_market_bar_job(queue) -> bool:
    from . import autonomous_research as _ar
    for st in (_ar.STATE_QUEUED, _ar.STATE_RUNNING, _ar.STATE_RETRYABLE):
        for j in queue.list_jobs(state=st, limit=1000):
            if str(j.lane).startswith(MARKET_BAR_LANE_PREFIX) and \
                    j.origin == ORIGIN_103:
                return True
    return False


def _enqueue103(queue, ctx: MarketBarJobContext, lane: str, reason: str,
                payload: dict, *, max_attempts: Optional[int] = None) -> dict:
    from . import autonomous_research as _ar
    p = dict(payload or {})
    p["prioritization_reason"] = reason
    p["planned_at"] = ctx.now()
    p["stage"] = "10.3"
    pr = int((ctx.stage103 or {}).get("priority", DEFAULT_PRIORITY))
    job_id = queue.enqueue(_ar.CAT_DATA_VALIDATION, lane=lane, payload=p,
                           priority=pr, origin=ORIGIN_103,
                           max_attempts=max_attempts)
    ctx.store.set_meta("last_stage103_reason", "%s | %s" % (lane, reason))
    return {"job_id": job_id, "lane": lane, "category": _ar.CAT_DATA_VALIDATION,
            "reason": reason, "origin": ORIGIN_103, "payload": p}


def _plan_stage103(queue, ctx: MarketBarJobContext) -> Optional[dict]:
    """Drive the Stage 10.3 repair in strict dependency order: diagnose the owned
    coverage -> validate the canonical panel -> measure per-rebalance PRICE
    readiness -> record the combined measured gate. Each step runs once per epoch
    (the HANDLER stamps the epoch flag on success). Returns the SINGLE highest-value
    next step, or None when the whole path is measured for the current epoch (so the
    tournament / identity / fundamentals reclaim the slot). The AlphaAgent — not
    Claude — generates each next action, and (when the combined gate opens) the
    tournament tick generates the fundamental experiment on the repaired panel."""
    epoch = _epoch(ctx)
    if ctx.store.get_meta("stage103_diagnose_complete") != epoch:
        return _enqueue103(queue, ctx, LANE_MB_COVERAGE_DIAGNOSE,
                           "diagnose owned MARKET_BAR coverage + per-rebalance "
                           "price/identity overlap (the 11-period blocker)",
                           {"epoch": epoch})
    if ctx.store.get_meta("stage103_panel_complete") != epoch:
        return _enqueue103(queue, ctx, LANE_MB_PANEL_VALIDATE,
                           "build + validate the canonical assetid-anchored "
                           "survivorship-safe panel over the full owned history",
                           {"epoch": epoch})
    if ctx.store.get_meta("stage103_readiness_complete") != epoch:
        return _enqueue103(queue, ctx, LANE_MB_READINESS_RECHECK,
                           "measure per-rebalance PRICE readiness (real evaluator "
                           "scored periods) on the repaired panel", {"epoch": epoch})
    if ctx.store.get_meta("stage103_experiment_complete") != epoch:
        return _enqueue103(queue, ctx, LANE_MB_EXPERIMENT_RECHECK,
                           "record the combined measured gate (fundamental PIT + "
                           "price readiness); tournament generates the experiment",
                           {"epoch": epoch})
    return None


def plan_next_market_bar_job(queue, ctx: MarketBarJobContext, *,
                             cfg: Optional[dict] = None) -> Optional[dict]:
    """Determine and enqueue the SINGLE highest-value next Stage 10.3 action, or
    return None when the repair path is measured for the current epoch. At most ONE
    live market_bar job at a time (idempotent). Driven only when Stage 10.3 is
    enabled AND an owned identity store exists."""
    cfg = cfg or ctx.stage103 or {}
    if not cfg.get("enabled") or not cfg.get("planner_enabled", True):
        return None
    if ctx.store is None:
        return None
    if _has_live_market_bar_job(queue):
        return None
    return _plan_stage103(queue, ctx)


__all__ = [
    "ORIGIN_103", "MARKET_BAR_LANE_PREFIX",
    "LANE_MB_COVERAGE_DIAGNOSE", "LANE_MB_PANEL_VALIDATE",
    "LANE_MB_READINESS_RECHECK", "LANE_MB_EXPERIMENT_RECHECK",
    "MarketBarJobContext", "dispatch_market_bar_job",
    "plan_next_market_bar_job", "DEFAULT_PRIORITY",
]
