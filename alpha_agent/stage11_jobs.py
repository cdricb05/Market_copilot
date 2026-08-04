"""
alpha_agent.stage11_jobs - Stage 11 AUTONOMOUS MULTI-FACTOR CAMPAIGN lanes +
planner (Workstreams A + H, driving B/C/D/E/F).

Claude builds the reusable capability (signal factory, generic evaluator, two-
stage funnel, multiple-testing controls, ensemble discovery, shadow-only
portfolio, breadth inventory/materialization glue); the AlphaAgent OPERATES it
through the SAME durable ``ResearchQueue`` + one-job-per-cycle fair scheduler on a
dedicated origin ``stage11-multifactor`` and lane prefix ``stage11.``. Every job is
BOUNDED, IDEMPOTENT, RESTART-SAFE, FAILURE-ISOLATED, DEDUPLICATED and
CONFIG-CONTROLLED; completion flags are stamped by the HANDLER on success (never at
plan time); at most one live Stage 11 job at a time. No second worker/queue/
scheduler is created.

The chunked lanes (signal coverage, broad screen, deep eval) advance a durable
cursor a BOUNDED batch at a time and stamp their epoch-complete flag only when the
cursor is exhausted, so the campaign drains safely across many collect cycles
within the 120s per-cycle drain budget and resumes exactly where it left off.

Research writes go ONLY to dedicated Stage 11 research artifacts + a SHADOW-ONLY
portfolio store under the Stage 11 root on D:. Never an operational ledger, order,
fill, position, cash, holding, signal, trade decision, model promotion, or (except
the reused append-only Norgate backfill writer) a price row.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from . import historical_identity as _hi
from .identity_jobs import write_artifact

ORIGIN_11 = "stage11-multifactor"
STAGE11_LANE_PREFIX = "stage11."

LANE_INVENTORY = "stage11.data_inventory"
LANE_BREADTH_PLAN = "stage11.price_breadth_plan"
LANE_BREADTH_MATERIALIZE = "stage11.price_breadth_materialize"
LANE_BREADTH_VALIDATE = "stage11.price_breadth_validate"
LANE_PANEL_BUILD = "stage11.panel_build"
LANE_CATALOG_BUILD = "stage11.signal_catalog_build"
LANE_COVERAGE_MEASURE = "stage11.signal_coverage_measure"
LANE_BROAD_SCREEN = "stage11.broad_screen"
LANE_MULTIPLE_TESTING = "stage11.multiple_testing"
LANE_DEEP_EVAL = "stage11.deep_eval"
LANE_ENSEMBLE = "stage11.ensemble"
LANE_SHADOW_DECIDE = "stage11.shadow_decide"
LANE_FINALIZE = "stage11.finalize"

# Dependency order (the planner walks this list and enqueues the first lane whose
# epoch-complete flag is stale).
LANE_ORDER = [
    LANE_INVENTORY, LANE_BREADTH_PLAN, LANE_BREADTH_MATERIALIZE,
    LANE_BREADTH_VALIDATE, LANE_PANEL_BUILD, LANE_CATALOG_BUILD,
    LANE_COVERAGE_MEASURE, LANE_BROAD_SCREEN, LANE_MULTIPLE_TESTING,
    LANE_DEEP_EVAL, LANE_ENSEMBLE, LANE_SHADOW_DECIDE, LANE_FINALIZE,
]

# Per-lane epoch-complete meta flag on the identity store.
_FLAG = {ln: "stage11_%s_complete" % ln.split(".")[-1] for ln in LANE_ORDER}

# Lanes that advance a durable cursor a bounded batch at a time (no idempotency
# de-dup guard - resumability is provided by the cursor, not the processed ledger).
_CHUNKED_LANES = frozenset({LANE_COVERAGE_MEASURE, LANE_BROAD_SCREEN,
                            LANE_DEEP_EVAL})

DEFAULT_PRIORITY = 2                 # below the Stage 10.x campaigns (>=3)
DEFAULT_SCREEN_BATCH = 12
DEFAULT_COVERAGE_BATCH = 16
DEFAULT_DEEP_BATCH = 2
DEFAULT_MAX_SURVIVORS = 24
DEFAULT_HORIZONS = (63,)
DEFAULT_WINSORS = (0.02, 0.05)
DEFAULT_SCREEN_HORIZONS = (21, 63, 126)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# --------------------------------------------------------------------------- #
# Context (all heavy dependencies injectable for tests).
# --------------------------------------------------------------------------- #
@dataclass
class Stage11JobContext:
    store: _hi.IdentityStore
    ingestion_root: str
    artifact_root: str
    stage11_root: str                             # dedicated Stage 11 research root
    shadow_root: str
    cache_dir: str
    rebalance_dates: "list[str]" = field(default_factory=list)
    cfg9: dict = field(default_factory=dict)
    stage11: dict = field(default_factory=dict)   # the stage11 config block
    backfill_config_path: Optional[str] = None
    index_name: str = "S&P 500"
    sources: "tuple" = ("norgate_local",)
    horizon_days: int = 63
    pit_store: Any = None                          # injectable
    sector_series: Any = None                      # injectable
    clock: Optional[Callable[[], str]] = None

    def now(self) -> str:
        return (self.clock or _utc_now_iso)()


def _outcomes():
    from . import autonomous_research as _ar
    return (_ar.OUTCOME_COMPLETED, _ar.OUTCOME_BLOCKED_SPECIFIC,
            _ar.OUTCOME_RETRYABLE)


def _cfg(ctx: Stage11JobContext) -> dict:
    return ctx.stage11 or {}


def _mapping_version(ctx: Stage11JobContext) -> str:
    return (ctx.store.get_meta("stage101_mapping_version_hash")
            or ctx.store.get_meta("stage102_mapped_epoch") or "nomvh")


def _stable_panel_epoch(ctx: Stage11JobContext) -> str:
    """A STABLE owned-price epoch: a hash of the owned MARKET_BAR date RANGE
    (month-bucketed) + sources, NOT the partition-file count. Each collect cycle
    re-materializes run files for existing dates, which churns a file-count
    signature every cycle and would reset the multi-cycle campaign to lane 1
    forever; bucketing the date span to month granularity keeps the epoch stable
    across cycles so the campaign completes, and advances only when the owned
    history genuinely grows into a new month (or breadth is expanded)."""
    import hashlib
    from pathlib import Path as _P
    base = _P(ctx.ingestion_root) / "normalized" / "MARKET_BAR"
    dmin = dmax = None
    if base.exists():
        for f in base.rglob("*.jsonl"):
            parts = f.parts
            if len(parts) >= 4 and parts[-4].isdigit() and parts[-3].isdigit() \
                    and parts[-2].isdigit():
                d = "%s-%s" % (parts[-4], parts[-3])   # YYYY-MM (month bucket)
                if dmin is None or d < dmin:
                    dmin = d
                if dmax is None or d > dmax:
                    dmax = d
    sig = {"date_min_month": dmin, "date_max_month": dmax,
           "sources": sorted(set(ctx.sources))}
    return hashlib.sha256(
        json.dumps(sig, sort_keys=True).encode("utf-8")).hexdigest()[:16]


def _epoch(ctx: Stage11JobContext) -> str:
    return "%s:%s" % (_stable_panel_epoch(ctx), str(_mapping_version(ctx))[:12])


def _panel_epoch(ctx: Stage11JobContext) -> str:
    return _stable_panel_epoch(ctx)


def _pit_store(ctx: Stage11JobContext):
    if ctx.pit_store is not None:
        return ctx.pit_store
    from . import fundamental_readiness as _fr
    try:
        return _fr.open_companyfacts_pit_store(ctx.cfg9, store=ctx.store)
    except Exception:  # noqa: BLE001
        return None


def _state_dir(ctx: Stage11JobContext) -> Path:
    d = Path(ctx.stage11_root) / "state"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _read_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return default
    return default


def _write_json(path: Path, doc) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(doc, sort_keys=True, indent=1), encoding="utf-8")
    tmp.replace(path)


# --------------------------------------------------------------------------- #
# Shared panel-context loader (cached OHLCV panel + PIT + identity + sector).
# --------------------------------------------------------------------------- #
def _load_context(ctx: Stage11JobContext, *,
                  panel_epoch: Optional[str] = None) -> dict:
    from . import fundamental_evidence as _fev
    from . import historical_price_panel as _hpp
    from . import signal_library as _sl
    from . import stage11_research as _sr
    # ``panel_epoch`` overrides the month-bucketed Stage 11 epoch ONLY for callers
    # (Stage 12) that need a breadth-sensitive OHLCV cache key; passing None keeps
    # the Stage 11 behaviour (and cache file / artifacts) byte-for-byte identical.
    epoch = panel_epoch or _panel_epoch(ctx)
    ohlcv = _sl.load_or_build_ohlcv_panel(ctx.ingestion_root, ctx.cache_dir,
                                          epoch, sources=ctx.sources)
    c2a = _hpp.build_cik_to_assetid(ctx.store)
    pit = _pit_store(ctx)
    panel_ctx = _sr.build_panel_context(ohlcv, store=pit, cik_to_assetid=c2a,
                                        sector_series=ctx.sector_series)
    cpanel = {a: list(zip(s["d"], s["c"])) for a, s in ohlcv.items()}
    rebs = list(ctx.rebalance_dates) or _fev._default_rebalance_dates(
        cpanel, horizon=int(ctx.horizon_days))
    return {"panel_ctx": panel_ctx, "rebalance_dates": rebs,
            "cik_to_assetid": c2a, "pit": pit, "n_assetids": len(ohlcv)}


def _catalogue(ctx: Stage11JobContext, *, screen: bool = False) -> list:
    from . import signal_library as _sl
    cfg = _cfg(ctx)
    horizons = tuple(cfg.get("screen_horizons") or DEFAULT_SCREEN_HORIZONS) \
        if screen else tuple(cfg.get("horizons") or DEFAULT_HORIZONS)
    winsors = tuple(cfg.get("winsors") or DEFAULT_WINSORS)
    return _sl.build_catalogue(panel_epoch=_panel_epoch(ctx),
                               fundamental_epoch=str(_mapping_version(ctx))[:12],
                               identity_epoch=str(_mapping_version(ctx))[:12],
                               horizons=horizons, winsors=winsors)


def _supported_ids(ctx: Stage11JobContext, specs, pit) -> tuple:
    from . import signal_library as _sl
    owned = _sl.concept_availability(pit)
    sector_available = (ctx.sector_series is not None
                        and bool(getattr(ctx.sector_series, "covered_keys",
                                         lambda: set())()))
    supported = set()
    data_hold = []
    by_defn = {d["name"]: d for d in _sl.PRIMITIVE_DEFS}
    for s in specs:
        defn = by_defn.get(s.primitive) or {"requires": s.requires}
        ok, reason = _sl.spec_supported(
            {"requires": s.requires or defn.get("requires", ())},
            owned_concepts=owned, sector_available=sector_available,
            volume_available=True)
        if ok:
            supported.add(s.spec_id)
        else:
            data_hold.append({"spec_id": s.spec_id, "name": s.name,
                              "family": s.family, "reason": reason})
    return supported, data_hold


# --------------------------------------------------------------------------- #
# Lane handlers.
# --------------------------------------------------------------------------- #
def _h_inventory(ctx: Stage11JobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    from . import historical_price_panel as _hpp
    from . import production_universe as _pu
    sig = _hpp.panel_coverage_signature(ctx.ingestion_root, sources=ctx.sources)
    panel = _hpp.build_assetid_price_panel(ctx.ingestion_root,
                                           sources=ctx.sources)
    c2a = _hpp.build_cik_to_assetid(ctx.store)
    materialized = set(panel.keys())
    joined = sum(1 for a in c2a.values() if a in materialized)
    # owned Norgate current+past universe count (measured, never hard-coded).
    universe_count = None
    try:
        res = _pu.resolve_production_universe(
            {"universe": {"scope": "survivorship"}}, scope="survivorship")
        universe_count = getattr(res, "target_count", None) or \
            len(getattr(res, "symbols", []) or [])
    except Exception:  # noqa: BLE001
        universe_count = None
    per_date = []
    for d in ctx.rebalance_dates[:80]:
        univ = ctx.store.historical_universe_on(d, index_name=ctx.index_name)
        mapped = [u for u in univ if u.get("cik")
                  and u.get("mapping_status") == _hi.STATUS_RESOLVED]
        aids = {str(u.get("norgate_assetid")) for u in mapped
                if u.get("norgate_assetid")}
        per_date.append({"as_of": d, "universe": len(univ),
                         "mapped_resolved": len(mapped),
                         "price_assetid_overlap": len(aids & materialized)})
    inv = {"coverage_signature": sig,
           "owned_norgate_universe_count": universe_count,
           "materialized_assetids": len(materialized),
           "cik_to_assetid": len(c2a), "cik_with_owned_price_series": joined,
           "panel_date_start": (sorted({d for b in panel.values()
                                        for d, _ in b})[:1] or [None])[0],
           "rebalance_dates": len(ctx.rebalance_dates),
           "per_rebalance_coverage": per_date}
    _write_json(_state_dir(ctx) / "inventory.json", inv)
    # persist the materialized assetid set once so downstream breadth lanes never
    # re-scan the full owned tree (a full close-panel read is expensive).
    _write_json(_state_dir(ctx) / "assetids.json",
                {"assetids": sorted(materialized)})
    ctx.store.set_meta("stage11_inventory", json.dumps(
        {k: inv[k] for k in ("owned_norgate_universe_count",
                             "materialized_assetids",
                             "cik_with_owned_price_series")})[:8000])
    ctx.store.set_meta(_FLAG[LANE_INVENTORY], _epoch(ctx))
    detail = {"real_work": LANE_INVENTORY, "disposition": "DATA_HOLD",
              "owned_norgate_universe_count": universe_count,
              "materialized_assetids": len(materialized),
              "cik_with_owned_price_series": joined,
              "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _h_breadth_plan(ctx: Stage11JobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    inv = _read_json(_state_dir(ctx) / "inventory.json", {})
    materialized = inv.get("materialized_assetids") or 0
    universe = inv.get("owned_norgate_universe_count")
    # identity-side reasons (assetid missing / unresolved / resolved-but-no-price).
    unresolved = 0
    no_assetid = 0
    resolved_no_price = 0
    # reuse the materialized assetid set persisted by the inventory lane (never
    # re-scan the full owned tree here).
    have = set((_read_json(_state_dir(ctx) / "assetids.json", {}) or {}
                ).get("assetids") or [])
    for s in ctx.store.list_securities(limit=1000000):
        mp = ctx.store.active_mapping(s["security_id"])
        aid = s.get("norgate_assetid")
        if not aid:
            no_assetid += 1
            continue
        if not (mp and mp.get("status") == _hi.STATUS_RESOLVED):
            unresolved += 1
            continue
        if str(aid) not in have:
            resolved_no_price += 1
    plan = {"owned_norgate_universe_count": universe,
            "materialized_assetids": materialized,
            "target_gap": (None if universe is None
                           else max(0, int(universe) - int(materialized))),
            "unavailable_reason_counts": {
                "resolved_identity_no_owned_price_series": resolved_no_price,
                "identity_unresolved": unresolved,
                "assetid_missing": no_assetid},
            "materialize_increment": int(_cfg(ctx).get("breadth_increment", 0)),
            "materialize_enabled": bool(_cfg(ctx).get(
                "breadth_materialize_enabled", False))}
    _write_json(_state_dir(ctx) / "breadth_plan.json", plan)
    ctx.store.set_meta(_FLAG[LANE_BREADTH_PLAN], _epoch(ctx))
    detail = {"real_work": LANE_BREADTH_PLAN, "disposition": "DATA_HOLD",
              **{k: plan[k] for k in ("materialized_assetids", "target_gap",
                                      "unavailable_reason_counts")},
              "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _h_breadth_materialize(ctx: Stage11JobContext, job) -> tuple:
    """Bounded, idempotent, append-only Norgate breadth expansion via the EXISTING
    Stage 6 backfill writer (never a new price writer). Config-gated: when
    disabled or the increment is 0 this is an explicit measure-only no-op, so the
    campaign never spends its budget on a materialization the operator did not
    enable. Re-running never duplicates canonical bars (deterministic record_id +
    resumable ticker_batches + read-time collapse)."""
    OK, BLK, RETRY = _outcomes()
    cfg = _cfg(ctx)
    increment = int(cfg.get("breadth_increment", 0))
    if not cfg.get("breadth_materialize_enabled", False) or increment <= 0 \
            or not ctx.backfill_config_path:
        result = {"materialized_this_run": 0, "mode": "measure_only",
                  "reason": "breadth materialization disabled or increment 0"}
        _write_json(_state_dir(ctx) / "materialize.json", result)
        ctx.store.set_meta(_FLAG[LANE_BREADTH_MATERIALIZE], _epoch(ctx))
        detail = {"real_work": LANE_BREADTH_MATERIALIZE, "disposition": "DATA_HOLD",
                  **result, "no_automatic_promotion": True}
        detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                            clock=ctx.clock)
        return OK, detail
    try:
        from . import historical_backfill as _hb
        p = Path(ctx.backfill_config_path)
        if not p.is_absolute():
            p = Path(__file__).resolve().parents[1] / ctx.backfill_config_path
        bcfg = json.loads(p.read_text(encoding="utf-8-sig"))
        base_cap = int((bcfg.get("batch") or {}).get("max_universe_symbols", 800))
        bcfg.setdefault("batch", {})["max_universe_symbols"] = base_cap + increment
        res = _hb.run_backfill(bcfg, mode="backfill", as_of="latest")
        summary = {"mode": "materialized", "new_cap": base_cap + increment,
                   "terminal": res.get("terminal") if isinstance(res, dict)
                   else None,
                   "records_written": (res.get("records_written")
                                       if isinstance(res, dict) else None)}
        _write_json(_state_dir(ctx) / "materialize.json", summary)
        ctx.store.set_meta(_FLAG[LANE_BREADTH_MATERIALIZE], _epoch(ctx))
        detail = {"real_work": LANE_BREADTH_MATERIALIZE, "disposition": "DATA_HOLD",
                  **summary, "no_automatic_promotion": True}
        detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                            clock=ctx.clock)
        return OK, detail
    except Exception as exc:  # noqa: BLE001 - Norgate/NDU unavailable etc.
        return BLK, {"real_work": LANE_BREADTH_MATERIALIZE,
                     "blocker": "BREADTH_MATERIALIZE_UNAVAILABLE",
                     "reason": "%s: %s" % (type(exc).__name__, str(exc)[:160]),
                     "disposition": "DATA_HOLD"}


def _h_breadth_validate(ctx: Stage11JobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    from . import historical_price_panel as _hpp
    # cheap path-based signature (no line reads); reuse the inventory assetid set
    # unless a materialization run changed the owned-price epoch.
    sig = _hpp.panel_coverage_signature(ctx.ingestion_root, sources=ctx.sources)
    mat = _read_json(_state_dir(ctx) / "materialize.json", {})
    have = set((_read_json(_state_dir(ctx) / "assetids.json", {}) or {}
                ).get("assetids") or [])
    if (mat.get("mode") == "materialized"):
        # the owned tree grew; re-measure the assetid set once.
        panel = _hpp.build_assetid_price_panel(ctx.ingestion_root,
                                               sources=ctx.sources)
        have = set(panel.keys())
        _write_json(_state_dir(ctx) / "assetids.json",
                    {"assetids": sorted(have)})
    validate = {"coverage_signature": sig,
                "materialized_assetids": len(have),
                "panel_epoch": _panel_epoch(ctx)}
    _write_json(_state_dir(ctx) / "breadth_validate.json", validate)
    ctx.store.set_meta(_FLAG[LANE_BREADTH_VALIDATE], _epoch(ctx))
    detail = {"real_work": LANE_BREADTH_VALIDATE, "disposition": "DATA_HOLD",
              **validate, "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _h_panel_build(ctx: Stage11JobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    from . import signal_library as _sl
    epoch = _panel_epoch(ctx)
    panel = _sl.load_or_build_ohlcv_panel(ctx.ingestion_root, ctx.cache_dir,
                                          epoch, sources=ctx.sources)
    if not panel:
        return BLK, {"real_work": LANE_PANEL_BUILD, "blocker": "NO_OWNED_OHLCV",
                     "reason": "no owned assetid MARKET_BAR OHLCV",
                     "disposition": "DATA_HOLD"}
    summary = {"panel_assetids": len(panel), "panel_epoch": epoch,
               "cache_dir": ctx.cache_dir}
    _write_json(_state_dir(ctx) / "panel_build.json", summary)
    ctx.store.set_meta("stage11_panel_epoch", epoch)
    ctx.store.set_meta(_FLAG[LANE_PANEL_BUILD], _epoch(ctx))
    detail = {"real_work": LANE_PANEL_BUILD, "disposition": "DATA_HOLD",
              **summary, "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _h_catalog_build(ctx: Stage11JobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    from . import signal_library as _sl
    pit = _pit_store(ctx)
    specs = _catalogue(ctx, screen=True)
    supported, data_hold = _supported_ids(ctx, specs, pit)
    cat = {"primitive_count": _sl.PRIMITIVE_COUNT,
           "families": _sl.primitive_families(),
           "generated_specs": len(specs),
           "supported_specs": len(supported),
           "data_hold_specs": data_hold,
           "specs": [s.to_dict() for s in specs]}
    _write_json(_state_dir(ctx) / "catalogue.json", cat)
    ctx.store.set_meta("stage11_catalogue", json.dumps(
        {"generated": len(specs), "supported": len(supported),
         "data_hold": len(data_hold), "primitives": _sl.PRIMITIVE_COUNT})[:4000])
    ctx.store.set_meta(_FLAG[LANE_CATALOG_BUILD], _epoch(ctx))
    detail = {"real_work": LANE_CATALOG_BUILD, "disposition": "DATA_HOLD",
              "generated_specs": len(specs), "supported_specs": len(supported),
              "data_hold_specs": len(data_hold),
              "primitive_count": _sl.PRIMITIVE_COUNT,
              "families": len(_sl.primitive_families()),
              "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _spec_by_id(specs, sid):
    for s in specs:
        if s.spec_id == sid:
            return s
    return None


def _h_coverage_measure(ctx: Stage11JobContext, job) -> tuple:
    """Chunked: measure per-signal scored-period coverage on the owned panel a
    bounded batch at a time; stamp complete when the cursor is exhausted."""
    OK, BLK, RETRY = _outcomes()
    from . import historical_price_panel as _hpp
    cat = _read_json(_state_dir(ctx) / "catalogue.json", {})
    supported = [s for s in cat.get("specs", []) if s["spec_id"] in
                 {x["spec_id"] for x in cat.get("specs", [])}]
    supported = [s for s in cat.get("specs", [])
                 if s["spec_id"] not in {d["spec_id"]
                                         for d in cat.get("data_hold_specs", [])}]
    ctx_load = _load_context(ctx)
    pit = ctx_load["pit"]
    c2a = ctx_load["cik_to_assetid"]
    cov_path = _state_dir(ctx) / "coverage.json"
    cov = _read_json(cov_path, {"done": [], "measured": {}})
    done = set(cov.get("done", []))
    # only fundamental signals need a scored-period coverage probe; price signals
    # are covered by the panel breadth measured in inventory.
    fund_specs = [s for s in supported
                  if s["kind"] == "fundamental" and s["primitive"] in
                  ("gross_profitability", "asset_growth", "balance_sheet_quality")]
    batch = int(_cfg(ctx).get("coverage_batch", DEFAULT_COVERAGE_BATCH))
    pending = [s for s in fund_specs if s["spec_id"] not in done][:batch]
    from . import signal_library as _sl
    panel = ctx_load["panel_ctx"]["ohlcv"]
    cpanel = {a: list(zip(s["d"], s["c"])) for a, s in panel.items()}
    for s in pending:
        rr = _hpp.per_rebalance_price_readiness(
            pit, cpanel, c2a, s["primitive"],
            horizon_days=int(ctx.horizon_days))
        cov["measured"][s["spec_id"]] = {
            "name": s["name"], "valid_scored_periods": rr["valid_scored_periods"],
            "sufficient": rr["sufficient"]}
        done.add(s["spec_id"])
    cov["done"] = sorted(done)
    _write_json(cov_path, cov)
    remaining = [s for s in fund_specs if s["spec_id"] not in done]
    detail = {"real_work": LANE_COVERAGE_MEASURE, "disposition": "DATA_HOLD",
              "measured": len(done), "remaining": len(remaining),
              "no_automatic_promotion": True}
    if not remaining:
        ctx.store.set_meta(_FLAG[LANE_COVERAGE_MEASURE], _epoch(ctx))
        detail["chunk_complete"] = True
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _h_broad_screen(ctx: Stage11JobContext, job) -> tuple:
    """Chunked broad screen: evaluate a bounded batch of supported specs, append
    to the durable screen file, stamp complete when every spec is screened."""
    OK, BLK, RETRY = _outcomes()
    from . import signal_library as _sl
    from . import stage11_research as _sr
    cat = _read_json(_state_dir(ctx) / "catalogue.json", {})
    data_hold_ids = {d["spec_id"] for d in cat.get("data_hold_specs", [])}
    all_specs = _catalogue(ctx, screen=True)
    supported_specs = [s for s in all_specs if s.spec_id not in data_hold_ids]
    screen_path = _state_dir(ctx) / "screen.json"
    st = _read_json(screen_path, {"records": {}, "series": {}, "rows": {},
                                  "done": []})
    done = set(st.get("done", []))
    pending = [s for s in supported_specs if s.spec_id not in done]
    batch = int(_cfg(ctx).get("screen_batch", DEFAULT_SCREEN_BATCH))
    chunk = pending[:batch]
    if chunk:
        ctx_load = _load_context(ctx)
        res = _sr.screen_specs(chunk, ctx_load["panel_ctx"],
                               ctx_load["rebalance_dates"],
                               supported_ids={s.spec_id for s in chunk},
                               min_scored_periods=int(
                                   (ctx.cfg9.get("evidence_completeness") or {}
                                    ).get("min_scored_periods", 12)),
                               cfg=ctx.cfg9)
        for rec in res["screened"]:
            st["records"][rec["spec_id"]] = rec
            done.add(rec["spec_id"])
        for sid, ser in res["series"].items():
            st["series"][sid] = ser
        for sid, row in res["rows"].items():
            st["rows"][sid] = row
    st["done"] = sorted(done)
    _write_json(screen_path, st)
    remaining = [s for s in supported_specs if s.spec_id not in done]
    survivors = [r for r in st["records"].values() if not r.get("screen_reason")]
    detail = {"real_work": LANE_BROAD_SCREEN, "disposition": "DATA_HOLD",
              "screened": len(done), "remaining": len(remaining),
              "survivors": len(survivors), "no_automatic_promotion": True}
    if not remaining:
        st["family_size"] = len([s for s in supported_specs])
        _write_json(screen_path, st)
        ctx.store.set_meta(_FLAG[LANE_BROAD_SCREEN], _epoch(ctx))
        detail["chunk_complete"] = True
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _h_multiple_testing(ctx: Stage11JobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    from . import stage11_research as _sr
    st = _read_json(_state_dir(ctx) / "screen.json", {})
    records = list((st.get("records") or {}).values())
    family = int(st.get("family_size") or len(records))
    mt = _sr.multiple_testing(records, family_size=family)
    _write_json(_state_dir(ctx) / "multiple_testing.json", mt)
    ctx.store.set_meta("stage11_multiple_testing", json.dumps(
        {"family_size": mt["family_size"], "tested": mt["tested"],
         "fdr_survivors": len(mt["fdr_survivors"]),
         "method": mt["method"]})[:2000])
    ctx.store.set_meta(_FLAG[LANE_MULTIPLE_TESTING], _epoch(ctx))
    detail = {"real_work": LANE_MULTIPLE_TESTING, "disposition": "DATA_HOLD",
              "family_size": mt["family_size"], "tested": mt["tested"],
              "fdr_survivors": len(mt["fdr_survivors"]),
              "method": mt["method"], "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _h_deep_eval(ctx: Stage11JobContext, job) -> tuple:
    """Chunked deep evaluation of the strongest, non-redundant screen survivors."""
    OK, BLK, RETRY = _outcomes()
    from . import stage11_research as _sr
    st = _read_json(_state_dir(ctx) / "screen.json", {})
    mt = _read_json(_state_dir(ctx) / "multiple_testing.json", {})
    fdr = set(mt.get("fdr_survivors") or [])
    survivors = [r for r in (st.get("records") or {}).values()
                 if not r.get("screen_reason")]
    survivors.sort(key=lambda r: abs(r.get("rank_ic_t") or 0.0), reverse=True)
    max_surv = int(_cfg(ctx).get("max_survivors", DEFAULT_MAX_SURVIVORS))
    survivors = survivors[:max_surv]
    deep_path = _state_dir(ctx) / "deep_eval.json"
    de = _read_json(deep_path, {"done": [], "results": {}})
    done = set(de.get("done", []))
    pending = [r for r in survivors if r["spec_id"] not in done]
    batch = int(_cfg(ctx).get("deep_batch", DEFAULT_DEEP_BATCH))
    chunk = pending[:batch]
    if chunk:
        all_specs = _catalogue(ctx, screen=True)
        ctx_load = _load_context(ctx)
        from . import selection_controls as _sc
        holdout = _sc.HoldoutLedger(str(_state_dir(ctx) / "holdout_ledger.json"))
        family = int(mt.get("family_size") or len(survivors))
        for r in chunk:
            spec = _spec_by_id(all_specs, r["spec_id"])
            if spec is None:
                done.add(r["spec_id"])
                continue
            res = _sr.deep_evaluate(
                spec, ctx_load["panel_ctx"], ctx_load["rebalance_dates"],
                cfg9=ctx.cfg9, family_size=family,
                multiple_testing_survived=(r["spec_id"] in fdr),
                holdout_ledger=holdout, cfg=ctx.cfg9)
            # keep the series for the ensemble stage; trim the row for storage.
            de["results"][r["spec_id"]] = {
                "spec_id": r["spec_id"], "name": res["name"],
                "family": res["family"],
                "gate_target_state": res["gate_verdict"]["target_state"],
                "gate_blocker": res["gate_verdict"]["blocker"],
                "rank_ic_t": res["row"]["rank_ic_t"],
                "spread_t": res["row"]["spread_t"],
                "net25": res["row"]["net_annualized_return"],
                "holdout_confirms": res["holdout"]["confirms_sign"],
                "sign_stable": res["parameter_neighborhood"]["sign_stable"],
                "multiple_testing_survived": res["multiple_testing_survived"],
                "capacity": res["capacity"], "qualifies": res["qualifies"]}
            done.add(r["spec_id"])
    de["done"] = sorted(done)
    _write_json(deep_path, de)
    remaining = [r for r in survivors if r["spec_id"] not in done]
    qualified = [v for v in de["results"].values() if v.get("qualifies")]
    detail = {"real_work": LANE_DEEP_EVAL, "disposition": "DATA_HOLD",
              "deep_evaluated": len(done), "remaining": len(remaining),
              "qualified_singles": len(qualified),
              "no_automatic_promotion": True}
    if not remaining:
        ctx.store.set_meta(_FLAG[LANE_DEEP_EVAL], _epoch(ctx))
        detail["chunk_complete"] = True
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _h_ensemble(ctx: Stage11JobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    from . import stage11_research as _sr
    st = _read_json(_state_dir(ctx) / "screen.json", {})
    series = st.get("series") or {}
    survivors = [r for r in (st.get("records") or {}).values()
                 if not r.get("screen_reason")]
    all_specs = _catalogue(ctx, screen=True)
    sel = _sr.select_orthogonal(survivors, series,
                                max_factors=int(_cfg(ctx).get("max_factors", 5)))
    ensembles = {"ensembles": [], "qualified": []}
    if len(sel["selected"]) >= 2:
        ctx_load = _load_context(ctx)
        sel_specs = [_spec_by_id(all_specs, sid) for sid in sel["selected"]]
        sel_specs = [s for s in sel_specs if s is not None]
        ensembles = _sr.discover_ensembles(
            sel_specs, st.get("rows") or {}, ctx_load["panel_ctx"],
            ctx_load["rebalance_dates"], cfg9=ctx.cfg9,
            horizon=int(ctx.horizon_days), cfg=ctx.cfg9)
    out = {"orthogonal_selection": sel,
           "ensembles": [{"ensemble": e["ensemble"],
                          "target_state": e["gate_verdict"]["target_state"],
                          "rank_ic_t": e["row"]["rank_ic_t"],
                          "spread_t": e["row"]["spread_t"],
                          "net25": e["row"]["net_annualized_return"],
                          "holdout_confirms": e["holdout_confirms"],
                          "qualifies": e["qualifies"],
                          "components": e["components"]}
                         for e in ensembles.get("ensembles", [])],
           "qualified_ensembles": len(ensembles.get("qualified", []))}
    _write_json(_state_dir(ctx) / "ensemble.json", out)
    ctx.store.set_meta("stage11_ensemble", json.dumps(
        {"selected": len(sel["selected"]),
         "ensembles": len(out["ensembles"]),
         "qualified": out["qualified_ensembles"]})[:2000])
    ctx.store.set_meta(_FLAG[LANE_ENSEMBLE], _epoch(ctx))
    detail = {"real_work": LANE_ENSEMBLE, "disposition": "DATA_HOLD",
              "orthogonal_selected": len(sel["selected"]),
              "ensembles": len(out["ensembles"]),
              "qualified_ensembles": out["qualified_ensembles"],
              "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _h_shadow_decide(ctx: Stage11JobContext, job) -> tuple:
    """Decide shadow activation on the UNCHANGED gates + the tournament shadow
    floor, or record the explicit NO_DEFENSIBLE_ALPHA status. Claude never chooses
    a winner: activation happens only for a candidate/ensemble the measured gates
    already qualified."""
    OK, BLK, RETRY = _outcomes()
    from . import shadow_portfolio as _shp
    de = _read_json(_state_dir(ctx) / "deep_eval.json", {})
    ens = _read_json(_state_dir(ctx) / "ensemble.json", {})
    q_singles = [v for v in (de.get("results") or {}).values()
                 if v.get("qualifies")]
    q_ens = [e for e in (ens.get("ensembles") or []) if e.get("qualifies")]
    store = _shp.ShadowPortfolioStore(ctx.shadow_root)
    evaluated = len(de.get("results") or {}) + len(ens.get("ensembles") or [])
    shadow_floor = float((ctx.cfg9.get("shadow_books") or {}).get(
        "min_combined_score_for_shadow", 0.55))
    decided = None
    if q_singles or q_ens:
        # a qualifying candidate/ensemble EXISTS; build its target book. Combined
        # score is scored via the unchanged tournament scorer; only activate above
        # the shadow floor.
        from . import tournament as _tt
        all_specs = _catalogue(ctx, screen=True)
        ctx_load = _load_context(ctx)
        ticker_map = {}
        try:
            from . import historical_price_panel as _hpp
            ticker_map = _hpp.assetid_to_effective_ticker(
                ctx.store, ctx_load["rebalance_dates"][-1])
        except Exception:  # noqa: BLE001
            ticker_map = {}
        best = None
        if q_singles:
            q_singles.sort(key=lambda v: abs(v.get("rank_ic_t") or 0), reverse=True)
            top = q_singles[0]
            spec = _spec_by_id(all_specs, top["spec_id"])
            if spec is not None:
                from . import stage11_research as _sr
                full = _sr.evaluate_in_context(spec, ctx_load["panel_ctx"],
                                               ctx_load["rebalance_dates"],
                                               cfg=ctx.cfg9)
                metrics = _tt.row_to_contract_metrics(full["row"])
                verdict = _tt.classify_evidence(metrics, ctx.cfg9)
                scores = _tt.score_candidate(metrics, ctx.cfg9) \
                    if hasattr(_tt, "score_candidate") else {}
                combined = (scores or {}).get("combined_score")
                if combined is not None and combined >= shadow_floor:
                    best = ("single", [(spec, 1.0)], full["row"], verdict, combined)
        if best is None:
            decided = store.record_no_alpha(
                as_of=ctx_load["rebalance_dates"][-1]
                if ctx_load["rebalance_dates"] else "n/a",
                evaluated=evaluated,
                reason=("candidate(s) cleared the strength gates but none reached "
                        "the shadow combined-score floor %.2f" % shadow_floor),
                detail={"qualified_singles": len(q_singles),
                        "qualified_ensembles": len(q_ens)})
        else:
            kind, comps, row, verdict, combined = best
            book = _shp.build_target_book(
                strategy_name="stage11_%s_%s" % (kind, comps[0][0].name),
                components=comps, ctx=ctx_load["panel_ctx"],
                as_of=ctx_load["rebalance_dates"][-1], evidence_row=row,
                gate_verdict=verdict, combined_score=combined,
                ticker_map=ticker_map, sector_series=ctx.sector_series,
                holding_period_days=int(ctx.horizon_days),
                provenance=("Stage 11: passed unchanged classify_evidence KEEP + "
                            "FDR + parameter-stable + holdout-confirmed + shadow "
                            "floor"))
            decided = store.activate(book)
    else:
        decided = store.record_no_alpha(
            as_of=(ctx.rebalance_dates[-1] if ctx.rebalance_dates else "n/a"),
            evaluated=evaluated,
            detail={"qualified_singles": 0, "qualified_ensembles": 0})
    ctx.store.set_meta("stage11_shadow_status", str(decided.get("status")))
    ctx.store.set_meta(_FLAG[LANE_SHADOW_DECIDE], _epoch(ctx))
    detail = {"real_work": LANE_SHADOW_DECIDE, "disposition": "DATA_HOLD",
              "shadow_status": decided.get("status"),
              "qualified_singles": len(q_singles),
              "qualified_ensembles": len(q_ens),
              "safety": _shp.SAFETY_LABELS, "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def persist_terminal_command_center(ctx: Stage11JobContext) -> dict:
    """Establish the Stage 11 finalize completion state, THEN build and persist the
    terminal command-center snapshot, so the persisted state file agrees with a live
    recomputation (status ``COMPLETE`` and next_action = measured-for-epoch).

    ``build_command_center`` keys ``status`` off the finalize flag, so the flag MUST
    be stamped before the snapshot is assembled; the previous ordering persisted the
    snapshot first and froze it at ``IN_PROGRESS`` forever.

    The single reusable code path used by both the finalize lane handler and any
    one-shot repair. IDEMPOTENT and side-effect-bounded: it re-stamps the same epoch
    flag, rewrites the same deterministic snapshot, and mirrors the compact status
    into store meta - it enqueues no job, evaluates no candidate, builds no ensemble,
    activates no shadow book, writes no research artifact, and never touches an
    operational ledger, order, fill, position, cash, holding, or trade decision. The
    shadow decision is only READ (never re-decided), so a NO_DEFENSIBLE_ALPHA outcome
    stays NO_DEFENSIBLE_ALPHA."""
    # 1) establish the terminal completion state FIRST.
    ctx.store.set_meta(_FLAG[LANE_FINALIZE], _epoch(ctx))
    # 2) assemble the snapshot from the durable state files (read-only assembly);
    #    with the flag set it now reports COMPLETE + measured-for-epoch.
    snap = build_command_center(ctx)
    # 3) persist atomically + mirror the compact status into store meta.
    _write_json(_state_dir(ctx) / "command_center.json", snap)
    ctx.store.set_meta("stage11_command_center", json.dumps(
        {"status": snap.get("status"),
         "shadow_status": (snap.get("shadow") or {}).get("status")})[:2000])
    return snap


def _h_finalize(ctx: Stage11JobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    snap = persist_terminal_command_center(ctx)
    detail = {"real_work": LANE_FINALIZE, "disposition": "DATA_HOLD",
              "status": snap.get("status"),
              "shadow_status": (snap.get("shadow") or {}).get("status"),
              "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


_LANE_HANDLERS = {
    LANE_INVENTORY: _h_inventory, LANE_BREADTH_PLAN: _h_breadth_plan,
    LANE_BREADTH_MATERIALIZE: _h_breadth_materialize,
    LANE_BREADTH_VALIDATE: _h_breadth_validate, LANE_PANEL_BUILD: _h_panel_build,
    LANE_CATALOG_BUILD: _h_catalog_build,
    LANE_COVERAGE_MEASURE: _h_coverage_measure, LANE_BROAD_SCREEN: _h_broad_screen,
    LANE_MULTIPLE_TESTING: _h_multiple_testing, LANE_DEEP_EVAL: _h_deep_eval,
    LANE_ENSEMBLE: _h_ensemble, LANE_SHADOW_DECIDE: _h_shadow_decide,
    LANE_FINALIZE: _h_finalize,
}


# --------------------------------------------------------------------------- #
# Read-only command-center snapshot (also served by the research API).
# --------------------------------------------------------------------------- #
SAFETY_BADGES = ["SHADOW ONLY", "NO ORDERS", "ORDERS DISABLED", "AUTOMATION OFF",
                 "MANUAL REVIEW", "RESEARCH USE ONLY"]


def build_command_center(ctx: Stage11JobContext) -> dict:
    from . import shadow_portfolio as _shp
    sd = _state_dir(ctx)
    inv = _read_json(sd / "inventory.json", {})
    plan = _read_json(sd / "breadth_plan.json", {})
    cat = _read_json(sd / "catalogue.json", {})
    screen = _read_json(sd / "screen.json", {})
    mt = _read_json(sd / "multiple_testing.json", {})
    deep = _read_json(sd / "deep_eval.json", {})
    ens = _read_json(sd / "ensemble.json", {})
    records = list((screen.get("records") or {}).values())
    survivors = [r for r in records if not r.get("screen_reason")]
    deep_results = list((deep.get("results") or {}).values())
    qualified = [v for v in deep_results if v.get("qualifies")]
    shadow = _shp.summarize(_shp.ShadowPortfolioStore(ctx.shadow_root))
    # best current evidence (strongest survivor by |rank_ic_t|).
    best = None
    if survivors:
        b = max(survivors, key=lambda r: abs(r.get("rank_ic_t") or 0))
        best = {"name": b["name"], "family": b["family"],
                "periods": b["periods"], "rank_ic_t": b["rank_ic_t"],
                "spread_t": b["spread_t"], "net25": b["net25"],
                "turnover": b["turnover"]}
    epoch = _epoch(ctx)
    status = "COMPLETE" if ctx.store.get_meta(_FLAG[LANE_FINALIZE]) == epoch \
        else "IN_PROGRESS"
    return {
        "stage": "11", "status": status, "epoch": epoch,
        "safety_badges": SAFETY_BADGES,
        "data_breadth": {
            "owned_norgate_universe": inv.get("owned_norgate_universe_count"),
            "materialized_assetids": inv.get("materialized_assetids"),
            "cik_with_owned_price_series": inv.get("cik_with_owned_price_series"),
            "unavailable_reason_counts": plan.get("unavailable_reason_counts"),
            "target_gap": plan.get("target_gap")},
        "candidate_funnel": {
            "primitive_count": cat.get("primitive_count"),
            "families": len(cat.get("families") or {}),
            "generated": cat.get("generated_specs"),
            "supported": cat.get("supported_specs"),
            "data_hold": len(cat.get("data_hold_specs") or []),
            "screened": len(records), "survivors": len(survivors),
            "rejected": len(records) - len(survivors),
            "deep_evaluated": len(deep_results),
            "qualified_singles": len(qualified)},
        "multiple_testing": {"method": mt.get("method"),
                             "family_size": mt.get("family_size"),
                             "fdr_survivors": len(mt.get("fdr_survivors") or [])},
        "best_current_evidence": best,
        "ensembles": {"selected": len((ens.get("orthogonal_selection") or {}
                                       ).get("selected") or []),
                      "evaluated": len(ens.get("ensembles") or []),
                      "qualified": ens.get("qualified_ensembles", 0)},
        "shadow": shadow,
        "next_action": _next_lane_label(ctx, epoch),
        "no_automatic_promotion": True,
    }


def _next_lane_label(ctx: Stage11JobContext, epoch: str) -> str:
    for lane in LANE_ORDER:
        if ctx.store.get_meta(_FLAG[lane]) != epoch:
            return "AlphaAgent next: %s" % lane
    return "Stage 11 campaign measured for the current owned-data epoch"


# --------------------------------------------------------------------------- #
# Read-only snapshot loaders for the research API (no panel / PIT build).
# --------------------------------------------------------------------------- #
def _lite_context(config_path: str):
    """A minimal read-only context (identity store + resolved Stage 11 roots, no
    panel / PIT / sector build) for the read-only API snapshot."""
    from . import runtime as _rt
    cfg = json.loads(Path(config_path).read_text(encoding="utf-8-sig"))
    s11 = _rt._resolve_stage11_runtime(cfg)
    if not s11:
        return None
    store = _rt.build_identity_store(cfg)
    return Stage11JobContext(
        store=store, ingestion_root=s11.get("ingestion_root"),
        artifact_root=s11.get("artifact_root"),
        stage11_root=s11.get("stage11_root"), shadow_root=s11.get("shadow_root"),
        cache_dir=s11.get("cache_dir"), cfg9={}, stage11=s11,
        sources=tuple(s11.get("sources") or ("norgate_local",)),
        horizon_days=int(s11.get("horizon_days", 63)))


def load_stage11_snapshot(config_path: str) -> dict:
    """Read-only Stage 11 command-center snapshot for the research API. Prefers the
    persisted snapshot written by the finalize lane (fast); otherwise assembles a
    live one from the durable state files + identity-store meta. Never builds the
    price panel and never mutates any store."""
    ctx = _lite_context(config_path)
    if ctx is None:
        return {"stage": "11", "status": "DISABLED",
                "safety_badges": SAFETY_BADGES,
                "reason": "Stage 11 is not configured/enabled"}
    persisted = _read_json(Path(ctx.stage11_root) / "state" /
                           "command_center.json", None)
    if persisted:
        persisted["safety_badges"] = SAFETY_BADGES
        persisted.setdefault("no_automatic_promotion", True)
        return persisted
    try:
        return build_command_center(ctx)
    except Exception as exc:  # noqa: BLE001
        return {"stage": "11", "status": "IN_PROGRESS",
                "safety_badges": SAFETY_BADGES, "reason": str(exc)[:180]}


def load_stage11_shadow(config_path: str) -> dict:
    """Read-only SHADOW-ONLY portfolio summary for the research API."""
    from . import shadow_portfolio as _shp
    ctx = _lite_context(config_path)
    if ctx is None:
        return {"status": "DISABLED", "safety_labels": _shp.SAFETY_LABELS}
    return _shp.summarize(_shp.ShadowPortfolioStore(ctx.shadow_root))


# --------------------------------------------------------------------------- #
# Dispatch + planner.
# --------------------------------------------------------------------------- #
def dispatch_stage11_job(job, ctx: Stage11JobContext) -> tuple:
    from . import autonomous_research as _ar
    lane = str(getattr(job, "lane", ""))
    handler = _LANE_HANDLERS.get(lane)
    if handler is None:
        return _ar.OUTCOME_BLOCKED_SPECIFIC, {
            "reason": "no stage11 handler for lane %s" % lane}
    # chunked lanes are resumable via their cursor - never idempotency-skipped.
    if lane not in _CHUNKED_LANES:
        job_key = "%s:%s" % (lane, _hi.content_hash(job.payload or {})[:16])
        if ctx.store.already_processed(job_key):
            return _ar.OUTCOME_COMPLETED, {"real_work": lane,
                                           "idempotent_skip": True,
                                           "disposition": "DATA_HOLD"}
    else:
        job_key = None
    try:
        outcome, detail = handler(ctx, job)
    except Exception as exc:  # noqa: BLE001 - never crash the drain
        return _ar.OUTCOME_RETRYABLE, {
            "reason": "stage11 handler raised: %s" % type(exc).__name__,
            "lane": lane}
    if outcome == _ar.OUTCOME_COMPLETED and job_key is not None:
        ctx.store.mark_processed(job_key, job_id=getattr(job, "job_id", None),
                                 kind=lane)
    detail.setdefault("lane", lane)
    return outcome, detail


def _has_live_stage11_job(queue) -> bool:
    from . import autonomous_research as _ar
    for st in (_ar.STATE_QUEUED, _ar.STATE_RUNNING, _ar.STATE_RETRYABLE):
        for j in queue.list_jobs(state=st, limit=1000):
            if str(j.lane).startswith(STAGE11_LANE_PREFIX) and \
                    j.origin == ORIGIN_11:
                return True
    return False


def _enqueue11(queue, ctx: Stage11JobContext, lane: str, reason: str) -> dict:
    from . import autonomous_research as _ar
    p = {"prioritization_reason": reason, "planned_at": ctx.now(),
         "stage": "11", "epoch": _epoch(ctx),
         "cursor_hint": _cursor_hint(ctx, lane)}
    pr = int((ctx.stage11 or {}).get("priority", DEFAULT_PRIORITY))
    job_id = queue.enqueue(_ar.CAT_DATA_VALIDATION, lane=lane, payload=p,
                           priority=pr, origin=ORIGIN_11)
    ctx.store.set_meta("last_stage11_reason", "%s | %s" % (lane, reason))
    return {"job_id": job_id, "lane": lane, "category": _ar.CAT_DATA_VALIDATION,
            "reason": reason, "origin": ORIGIN_11}


def _cursor_hint(ctx: Stage11JobContext, lane: str) -> int:
    """A monotone cursor stamped into the payload so successive chunk jobs for the
    same lane have DISTINCT payloads (and distinct job identities)."""
    if lane not in _CHUNKED_LANES:
        return 0
    sd = _state_dir(ctx)
    if lane == LANE_BROAD_SCREEN:
        return len((_read_json(sd / "screen.json", {}) or {}).get("done") or [])
    if lane == LANE_DEEP_EVAL:
        return len((_read_json(sd / "deep_eval.json", {}) or {}).get("done") or [])
    if lane == LANE_COVERAGE_MEASURE:
        return len((_read_json(sd / "coverage.json", {}) or {}).get("done") or [])
    return 0


def _plan_stage11(queue, ctx: Stage11JobContext) -> Optional[dict]:
    epoch = _epoch(ctx)
    for lane in LANE_ORDER:
        if ctx.store.get_meta(_FLAG[lane]) != epoch:
            return _enqueue11(queue, ctx, lane,
                              "Stage 11 multi-factor: advance %s" % lane)
    return None


def plan_next_stage11_job(queue, ctx: Stage11JobContext, *,
                          cfg: Optional[dict] = None) -> Optional[dict]:
    """Enqueue the SINGLE highest-value next Stage 11 action (dependency-ordered),
    or None when the whole pipeline is measured for the current owned-data epoch.
    At most one live Stage 11 job at a time. Driven only when Stage 11 is enabled
    AND an owned identity store exists."""
    cfg = cfg or ctx.stage11 or {}
    if not cfg.get("enabled") or not cfg.get("planner_enabled", True):
        return None
    if ctx.store is None:
        return None
    if _has_live_stage11_job(queue):
        return None
    return _plan_stage11(queue, ctx)


__all__ = [
    "ORIGIN_11", "STAGE11_LANE_PREFIX", "LANE_ORDER", "Stage11JobContext",
    "dispatch_stage11_job", "plan_next_stage11_job", "build_command_center",
    "persist_terminal_command_center",
    "load_stage11_snapshot", "load_stage11_shadow", "SAFETY_BADGES",
    "DEFAULT_PRIORITY",
]
