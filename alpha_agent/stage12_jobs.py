"""Stage 12 Workstreams J/K -- campaign lanes, bounded driver, snapshot loaders.

Reuses the Stage 11 machinery wholesale: ``runtime.build_stage11_context`` +
``stage11_jobs._load_context`` supply the identical owned panel / PIT store /
leakage-safe sector / rebalance calendar, ``identity_jobs.write_artifact`` writes
immutable content-addressed artifacts, and the epoch is the stable owned-data
identity + registry version (never a mutable file count). Stage 12 co-locates its
durable state under ``<identity>/stage12`` and NEVER reads or writes an
operational ledger.

The campaign is thirteen lanes in dependency order. Light lanes (autopsy,
owned-data, sample-power, event-coverage, registry) read only artifacts and
metadata. Heavy lanes (event/residual build, screen, multiple-testing, deep-eval,
combinations, shadow-decide) build the owned panel once and run the focused
tournament. The driver is bounded (wall-clock budget), resumable (per-lane epoch
flags), and idempotent (a lane at the current epoch is a no-op).
"""
from __future__ import annotations

import hashlib
import io
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

try:
    from . import stage12_autopsy as _autopsy
    from . import stage12_inventory as _inv
    from . import stage12_owned_data as _owned
    from . import stage12_power as _power
    from . import stage12_registry as _registry
    from . import stage12_tournament as _tourn
    from .identity_jobs import write_artifact as _write_artifact
except Exception:  # pragma: no cover
    import stage12_autopsy as _autopsy  # type: ignore
    import stage12_inventory as _inv  # type: ignore
    import stage12_owned_data as _owned  # type: ignore
    import stage12_power as _power  # type: ignore
    import stage12_registry as _registry  # type: ignore
    import stage12_tournament as _tourn  # type: ignore
    from identity_jobs import write_artifact as _write_artifact  # type: ignore

ORIGIN_12 = "stage12-autopsy"
STAGE12_LANE_PREFIX = "stage12."

LANE_AUTOPSY = "stage12.stage11_autopsy"
LANE_OWNED_DATA = "stage12.owned_data_inventory"
LANE_FULL_INVENTORY = "stage12.full_universe_inventory"
LANE_MATERIALIZE = "stage12.norgate_materialize"
LANE_SAMPLE_POWER = "stage12.sample_power"
LANE_EVENT_COVERAGE = "stage12.event_coverage"
LANE_EVENT_BUILD = "stage12.event_feature_build"
LANE_RESIDUAL_BUILD = "stage12.residual_price_build"
LANE_HYP_REGISTRY = "stage12.hypothesis_registry"
LANE_SCREEN = "stage12.screen"
LANE_MULTIPLE_TESTING = "stage12.multiple_testing"
LANE_DEEP_EVAL = "stage12.deep_eval"
LANE_COMBINATIONS = "stage12.combinations"
LANE_SHADOW_DECIDE = "stage12.shadow_decide"
LANE_FINALIZE = "stage12.finalize"

LANE_ORDER: List[str] = [
    LANE_AUTOPSY, LANE_OWNED_DATA, LANE_FULL_INVENTORY, LANE_MATERIALIZE,
    LANE_SAMPLE_POWER, LANE_EVENT_COVERAGE, LANE_HYP_REGISTRY, LANE_EVENT_BUILD,
    LANE_RESIDUAL_BUILD, LANE_SCREEN, LANE_MULTIPLE_TESTING, LANE_DEEP_EVAL,
    LANE_COMBINATIONS, LANE_SHADOW_DECIDE, LANE_FINALIZE,
]
# Lanes that touch the heavy owned panel / Norgate (cursor-chunked / bounded).
_HEAVY_LANES = {LANE_FULL_INVENTORY, LANE_MATERIALIZE, LANE_EVENT_BUILD,
                LANE_RESIDUAL_BUILD, LANE_SCREEN, LANE_MULTIPLE_TESTING,
                LANE_DEEP_EVAL, LANE_COMBINATIONS, LANE_SHADOW_DECIDE}

SAFETY_BADGES = ["RESEARCH ONLY", "SHADOW ONLY", "NO LIVE BROKER ORDERS",
                 "AUTOMATION OFF", "MANUAL REVIEW", "NO AUTO-PROMOTION"]

_STATE_FILE = {
    LANE_AUTOPSY: "autopsy.json",
    LANE_OWNED_DATA: "owned_data.json",
    LANE_FULL_INVENTORY: "full_inventory.json",
    LANE_MATERIALIZE: "materialize.json",
    LANE_SAMPLE_POWER: "sample_power.json",
    LANE_EVENT_COVERAGE: "event_coverage.json",
    LANE_HYP_REGISTRY: "registry_manifest.json",
    LANE_EVENT_BUILD: "event_build.json",
    LANE_RESIDUAL_BUILD: "residual_build.json",
    LANE_SCREEN: "screen.json",
    LANE_MULTIPLE_TESTING: "multiple_testing.json",
    LANE_DEEP_EVAL: "deep_eval.json",
    LANE_COMBINATIONS: "combinations.json",
    LANE_SHADOW_DECIDE: "shadow_state.json",
    LANE_FINALIZE: "command_center.json",
}


# --------------------------------------------------------------------------- #
# Config + paths.
# --------------------------------------------------------------------------- #
def _load_cfg(config_path: str) -> dict:
    return json.load(io.open(config_path, encoding="utf-8-sig"))


def _stage12_root_from_stage11(stage11_root: str | Path) -> Path:
    return Path(stage11_root).parent / "stage12"


def _state_dir(stage12_root: str | Path) -> Path:
    d = Path(stage12_root) / "state"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _read_json(path: Path, default=None):
    if Path(path).exists():
        try:
            return json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return default
    return default


def _write_json(path: Path, doc) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(path).with_suffix(Path(path).suffix + ".tmp")
    tmp.write_text(json.dumps(doc, sort_keys=True, indent=1), encoding="utf-8")
    tmp.replace(path)


# --------------------------------------------------------------------------- #
# Stage 12 context (lazy heavy panel build).
# --------------------------------------------------------------------------- #
class Stage12Context:
    def __init__(self, config_path: Optional[str] = None, *,
                 cfg: Optional[dict] = None):
        self.config_path = config_path
        self.cfg = dict(cfg) if cfg is not None else _load_cfg(config_path)
        self._base = None
        self._loaded = None
        self._tournament = None
        self._cov_sig = None
        self._cf_index = None
        self._registry = _registry.build_registry()
        self._resolve_roots()

    def _resolve_roots(self) -> None:
        try:
            from . import runtime as _rt
        except Exception:  # pragma: no cover
            import runtime as _rt  # type: ignore
        self._runtime = _rt
        s11 = _rt._resolve_stage11_runtime(self.cfg)
        self.stage11_root = s11.get("stage11_root")
        self.stage11_state_dir = Path(self.stage11_root) / "state"
        self.stage12_root = _stage12_root_from_stage11(self.stage11_root)
        self.state_dir = _state_dir(self.stage12_root)
        self.artifact_root = s11.get("artifact_root")

    # -- lazy base ctx (identity store, cfg9, sector, rebalance dates) --------
    @property
    def base(self):
        if self._base is None:
            self._base = self._runtime.build_stage11_context(self.cfg)
        return self._base

    @property
    def cfg9(self) -> dict:
        return self.base.cfg9 or {}

    @property
    def registry(self) -> dict:
        return self._registry

    def _panel_epoch(self) -> str:
        try:
            from . import stage11_jobs as _s11
        except Exception:  # pragma: no cover
            import stage11_jobs as _s11  # type: ignore
        try:
            return _s11._panel_epoch(self.base)
        except Exception:
            cc = _read_json(self.stage11_state_dir / "command_center.json", {}) or {}
            return str(cc.get("epoch", "unknown")).split(":")[0]

    def coverage_signature(self) -> dict:
        """The owned-data coverage that scopes the Stage 12 epoch: materialized
        assetid count, identity mapping version, event-source (companyfacts) fact
        count. Memoised (cheap sqlite counts) so ``epoch`` stays inexpensive."""
        if self._cov_sig is not None:
            return self._cov_sig
        materialized = None
        try:
            aids = _read_json(self.stage11_state_dir / "assetids.json", {}) or {}
            materialized = len(aids.get("assetids") or [])
        except Exception:
            materialized = None
        mapping_version = cf_facts = None
        try:
            st = self.base.store
            mapping_version = (st.get_meta("stage101_mapping_version_hash")
                               or st.get_meta("stage102_mapped_epoch"))
        except Exception:
            mapping_version = None
        try:
            idx = self._open_cf_index()
            cf_facts = int(idx.counts().get("facts", 0)) if idx else 0
        except Exception:
            cf_facts = None
        self._cov_sig = {"materialized_assetids": materialized,
                         "identity_mapping_version": mapping_version,
                         "event_source_cf_facts": cf_facts}
        return self._cov_sig

    def epoch(self) -> str:
        # BLOCKER 4: the epoch scopes lane-completion idempotency and MUST advance
        # when any owned-data state that changes a measurement changes -- owned-data
        # date span + materialized assetid coverage + identity mapping version +
        # event-source coverage + hypothesis-registry version.
        panel = self._panel_epoch()
        reg = self._registry["registry_version"][:8]
        cov = self.coverage_signature()
        h = hashlib.sha256(json.dumps(
            {"panel": panel, "reg": reg, "cov": cov},
            sort_keys=True, default=str).encode()).hexdigest()[:8]
        return "%s:%s:%s" % (panel, reg, h)

    # -- heavy panel context -------------------------------------------------
    def loaded(self) -> dict:
        if self._loaded is None:
            try:
                from . import stage11_jobs as _s11
            except Exception:  # pragma: no cover
                import stage11_jobs as _s11  # type: ignore
            self._loaded = _s11._load_context(self.base)
        return self._loaded

    @property
    def panel_ctx(self) -> dict:
        return self.loaded()["panel_ctx"]

    @property
    def rebalance_dates(self) -> List[str]:
        rebs = self.loaded().get("rebalance_dates") or []
        if not rebs:  # fall back to the authoritative Stage 11 rebalance calendar
            rebs = _stage11_rebalance_dates(self.stage11_state_dir)
        return rebs

    def _open_cf_index(self):
        """Open the durable companyfacts fact index (retains accession/form) for
        the event-time evaluator. Read-only; memoised; degrades to None."""
        if self._cf_index is not None:
            return self._cf_index or None
        try:
            from . import fundamental_readiness as _fr
            from . import sec_companyfacts_index as _cfi
        except Exception:  # pragma: no cover
            import fundamental_readiness as _fr  # type: ignore
            import sec_companyfacts_index as _cfi  # type: ignore
        idx = None
        try:
            db = _fr._companyfacts_index_db(self.cfg9)
            if db and Path(db).exists():
                cand = _cfi.SecCompanyFactsIndex(db)
                if int(cand.counts().get("facts", 0)) > 0:
                    idx = cand
        except Exception:  # noqa: BLE001
            idx = None
        self._cf_index = idx if idx is not None else False
        return idx

    def _event_gates(self) -> dict:
        au = (self.cfg.get("autonomy") or {}) if isinstance(self.cfg, dict) else {}
        return {"min_issuers": int(au.get("event_min_issuers", 30)),
                "min_events": int(au.get("event_min_events", 30)),
                "min_cohorts": 12}

    def tournament(self) -> dict:
        if self._tournament is None:
            s11 = _stage11_screen_artifacts(self.stage11_state_dir)
            ctx = dict(self.panel_ctx)
            ctx["cf_index"] = self._open_cf_index()  # event-time filing calendar
            self._tournament = _tourn.run_tournament(
                self.registry, ctx, self.rebalance_dates, self.cfg9,
                stage11_records=s11.get("records"), stage11_rows=s11.get("rows"),
                effective_sample=_effective_sample_from_autopsy(self.state_dir),
                event_gates=self._event_gates())
        return self._tournament


def _stage11_rebalance_dates(state_dir: Path) -> List[str]:
    sc = _read_json(Path(state_dir) / "screen.json", {}) or {}
    for s in (sc.get("series") or {}).values():
        ds = s.get("dates")
        if ds:
            return [str(x) for x in ds]
    return []


def _stage11_screen_artifacts(state_dir: Path) -> dict:
    sc = _read_json(Path(state_dir) / "screen.json", {}) or {}
    return {"records": sc.get("records") or {}, "rows": sc.get("rows") or {}}


def _effective_sample_from_autopsy(state_dir: Path) -> Optional[float]:
    au = _read_json(Path(state_dir) / "autopsy.json", {}) or {}
    return (au.get("effective_hypothesis_count") or {}).get("effective_family_size")


# --------------------------------------------------------------------------- #
# Lane handlers.  Each returns the lane payload dict (persisted by the driver).
# --------------------------------------------------------------------------- #
def _h_autopsy(sctx: Stage12Context) -> dict:
    return _autopsy.run_autopsy(sctx.stage11_state_dir)


def _h_owned_data(sctx: Stage12Context) -> dict:
    inv = _read_json(sctx.stage11_state_dir / "inventory.json", {}) or {}
    cat = _read_json(sctx.stage11_state_dir / "catalogue.json", {}) or {}
    id_store = None
    try:
        id_store = sctx.base.store
    except Exception:
        id_store = None
    return _owned.run_owned_data_inventory(inv, cat, id_store=id_store)


def _stage12_cfg(cfg: Mapping[str, Any]) -> dict:
    return (cfg.get("stage12") or {}) if isinstance(cfg, Mapping) else {}


def _backfill_config_path(sctx: Stage12Context) -> Optional[str]:
    ph = (sctx.cfg.get("production_handlers") or {}) if isinstance(sctx.cfg, dict) else {}
    return ph.get("stage6_backfill_config") or "configs/alpha_agent/stage6_historical_backfill.json"


def _current_materialized_assetids(sctx: Stage12Context) -> set:
    """The materialized assetid set from the owned MARKET_BAR tree (authoritative),
    refreshing the cached assetids.json so downstream coverage stays consistent
    after a fresh Norgate materialization. Falls back to the cache on any error."""
    try:
        from . import historical_price_panel as _hpp
    except Exception:  # pragma: no cover
        import historical_price_panel as _hpp  # type: ignore
    try:
        ing = sctx.base.ingestion_root
        srcs = list(getattr(sctx.base, "sources", None) or ["norgate_local"])
        panel = _hpp.build_assetid_price_panel(ing, sources=srcs)
        aids = {str(a) for a in panel.keys()}
        try:
            _write_json(sctx.stage11_state_dir / "assetids.json",
                        {"assetids": sorted(aids)})
        except Exception:
            pass
        return aids
    except Exception:
        return set((_read_json(sctx.stage11_state_dir / "assetids.json", {})
                    or {}).get("assetids") or [])


def _backfill_window_start(sctx: Stage12Context) -> str:
    p = _backfill_config_path(sctx)
    try:
        pp = Path(p)
        if not pp.is_absolute():
            pp = Path(__file__).resolve().parents[1] / p
        b = json.loads(pp.read_text(encoding="utf-8-sig"))
        return (b.get("date_range") or {}).get("start") or "2015-01-01"
    except Exception:
        return "2015-01-01"


def _h_full_inventory(sctx: Stage12Context) -> dict:
    """BLOCKER 1: classify EVERY name in the ~1895 survivorship-safe Norgate
    universe by its stable assetid (cheap owned metadata, no bar fetch). Cursor-
    chunked (``stage12.inventory_batch``) so a large universe is a bounded sweep."""
    s12 = _stage12_cfg(sctx.cfg)
    batch = int(s12.get("inventory_batch", 2000))
    prev = _read_json(sctx.state_dir / _STATE_FILE[LANE_FULL_INVENTORY], {}) or {}
    epoch = sctx.epoch()
    resume = (prev.get("epoch") == epoch and not prev.get("complete"))
    cursor = int(prev.get("cursor_next") or 0) if resume else 0
    agg = prev.get("aggregate") if resume else None
    avail_win = int(prev.get("available_in_window") or 0) if resume else 0
    avail_pre = int(prev.get("available_pre_window_only") or 0) if resume else 0
    symbols = _inv.resolve_universe_symbols(sctx.cfg)
    # Authoritative materialized set from the owned MARKET_BAR tree (not the cached
    # assetids.json, which can lag a fresh materialization). Refresh the cache so
    # the classification's MATERIALIZED bucket reflects the current owned tree.
    materialized = _current_materialized_assetids(sctx)
    nd = _inv.open_norgate()
    id_map = {}
    try:
        id_map = _inv.build_identity_map(sctx.base.store)
    except Exception:
        id_map = {}
    window = _backfill_window_start(sctx)
    chunk = _inv.classify_universe(symbols, materialized_assetids=materialized,
                                   id_map=id_map, nd=nd, offset=cursor, limit=batch,
                                   window_start=window)
    agg = _inv.merge_chunk(agg, chunk)
    avail_win += int(chunk.get("available_in_window") or 0)
    avail_pre += int(chunk.get("available_pre_window_only") or 0)
    return {
        "stage": "12", "workstream": "full_universe_inventory", "epoch": epoch,
        "cursor_next": chunk["cursor_next"], "complete": bool(chunk["complete"]),
        "universe_total": chunk["universe_total"],
        "counts_by_classification": agg["counts_by_classification"],
        "counts_by_price_status": agg["counts_by_price_status"],
        "available_not_materialized": agg["available_not_materialized"],
        "available_in_window": avail_win,
        "available_pre_window_only": avail_pre,
        "materialized": agg["materialized"],
        "window_start": window, "norgate_available": nd is not None,
        "aggregate": agg, "no_automatic_promotion": True,
        "note": ("AVAILABLE_NOT_MATERIALIZED > 0 means owned licensed history "
                 "remains unmaterialised -> owned evidence is NOT exhausted."),
    }


def _h_materialize(sctx: Stage12Context) -> dict:
    """BLOCKER 1: bounded, idempotent, resumable Norgate materialisation via the
    EXISTING Stage 6 backfill cursor. Config-gated (``stage12.materialize_enabled``
    + ``materialize_cap_increment``); a measure-only no-op by default so the
    scheduled collect never runs a heavy unattended fetch. Never a new writer,
    never a norgatedata upgrade; writes ONLY under the Stage 6 ingestion tree."""
    s12 = _stage12_cfg(sctx.cfg)
    inv = _read_json(sctx.state_dir / _STATE_FILE[LANE_FULL_INVENTORY], {}) or {}
    avail_window = int(inv.get("available_in_window") or 0)
    avail_total = inv.get("available_not_materialized")
    enabled = bool(s12.get("materialize_enabled", False))
    increment = int(s12.get("materialize_cap_increment", 0))
    bcfg = _backfill_config_path(sctx)
    if not enabled or increment <= 0 or avail_window <= 0 or not bcfg:
        reason = ("materialize_enabled is false" if not enabled else
                  "materialize_cap_increment is 0" if increment <= 0 else
                  "no available_in_window names to materialise" if avail_window <= 0
                  else "no backfill config")
        return {"stage": "12", "workstream": "norgate_materialize",
                "mode": "measure_only", "reason": reason,
                "available_in_window": avail_window,
                "available_not_materialized": avail_total,
                "materialize_remaining": bool((avail_total or 0) > 0),
                "no_automatic_promotion": True}
    summary = _inv.materialize_bounded(bcfg, cap_increment=increment)
    return {"stage": "12", "workstream": "norgate_materialize", "mode": "materialized",
            "available_in_window_before": avail_window,
            "available_not_materialized": avail_total,
            "materialize_remaining": bool((avail_total or 0) > 0),
            "no_automatic_promotion": True, **summary}


def _h_sample_power(sctx: Stage12Context) -> dict:
    sc = _read_json(sctx.stage11_state_dir / "screen.json", {}) or {}
    cat = _read_json(sctx.stage11_state_dir / "catalogue.json", {}) or {}
    name_by = {s.get("spec_id"): s.get("name") for s in (cat.get("specs") or [])}
    best_id = None
    best_t = -9.0
    for sid, s in (sc.get("series") or {}).items():
        rec = (sc.get("records") or {}).get(sid) or {}
        t = rec.get("rank_ic_t")
        if t is not None and abs(t) > best_t:
            best_t = abs(t); best_id = sid
    ic_series = ((sc.get("series") or {}).get(best_id) or {}).get("ic") if best_id else None
    mt = _read_json(sctx.stage11_state_dir / "multiple_testing.json", {}) or {}
    best_p = None
    for r in mt.get("rows") or []:
        p = r.get("raw_pvalue")
        if p is not None and (best_p is None or p < best_p):
            best_p = p
    inv = _read_json(sctx.stage11_state_dir / "inventory.json", {}) or {}
    n_periods = int(inv.get("rebalance_dates") or (len(ic_series) if ic_series else 0))
    diag = _power.diagnose_design(
        n_periods=n_periods, ic_series=ic_series,
        best_true_ic=(sum(ic_series) / len(ic_series) if ic_series else None),
        best_pvalue=best_p, step_days=63)
    diag["best_candidate_name"] = name_by.get(best_id)
    diag["best_raw_pvalue"] = best_p
    return diag


def _h_event_coverage(sctx: Stage12Context) -> dict:
    owned = _read_json(sctx.state_dir / _STATE_FILE[LANE_OWNED_DATA], {}) or {}
    return {
        "stage": "12", "workstream": "event_coverage",
        "event_coverage": owned.get("event_coverage"),
        "event_families_supported": owned.get("event_families_supported"),
        "event_families_total": owned.get("event_families_total"),
        "owned_concepts": owned.get("owned_concepts"),
        "note": ("Concept availability != adequate cross-sectional coverage per "
                 "formation date; the event_feature_build lane measures the latter "
                 "and DATA_HOLDs any family below the 12-period / 20-name gates."),
    }


def _h_hypothesis_registry(sctx: Stage12Context) -> dict:
    reg = sctx.registry
    problems = _registry.validate_registry(reg)
    frozen_path = _registry.default_registry_path()
    frozen_exists = Path(frozen_path).exists()
    return {
        "stage": "12", "workstream": "hypothesis_registry",
        "registry_version": reg["registry_version"],
        "n_hypotheses": reg["n_hypotheses"],
        "n_economic_families": reg["n_economic_families"],
        "economic_families": reg["economic_families"],
        "n_confirmatory": reg["n_confirmatory"],
        "n_exploratory": reg["n_exploratory"],
        "frozen_artifact": str(frozen_path),
        "frozen_artifact_exists": frozen_exists,
        "validation_problems": problems,
        "immutable": True,
    }


def _coverage_lane(sctx: Stage12Context, prefix: str) -> dict:
    """Shared event/residual build coverage summary from the full tournament.

    For event-time families the row also carries the event-study coverage (total
    events, distinct issuers, events-by-year, cohort span) and the overlap-aware /
    autocorrelation-robust effective-N + clustered t.
    """
    t = sctx.tournament()
    rows = []
    events_by_year: Dict[str, int] = {}
    for m in t["measurements"]:
        bk = str(m.get("builder_key") or "")
        if not bk.startswith(prefix):
            continue
        cd = m.get("coverage_detail") or {}
        overlap = cd.get("overlap") or {}
        row = {"hypothesis_id": m["hypothesis_id"], "builder_key": bk,
               "periods": m["periods"],
               "median_names": cd.get("median_cohort_names") or cd.get("median_names"),
               "target_state": m["target_state"], "rank_ic_t": m["rank_ic_t"],
               "spread_t": m.get("spread_t"),
               "data_hold_reason": m["data_hold_reason"]}
        if cd.get("design") == "event_time_monthly_cohort":
            row.update({
                "design": "event_time_monthly_cohort",
                "n_cohorts": cd.get("n_cohorts"),
                "total_events": cd.get("total_events"),
                "distinct_issuers": cd.get("distinct_issuers"),
                "cohort_span": cd.get("cohort_span"),
                "events_by_year": cd.get("events_by_year"),
                "overlap_effective_n": overlap.get("newey_west_effective_n"),
                "overlap_clustered_t": overlap.get("t_clustered"),
                "overlap_fraction": overlap.get("overlap_fraction"),
                "overlap_robust": m.get("overlap_robust")})
            for y, c in (cd.get("events_by_year") or {}).items():
                events_by_year[y] = events_by_year.get(y, 0) + int(c)
        rows.append(row)
    built = sum(1 for r in rows if (r["periods"] or 0) >= 12)
    out = {"stage": "12", "prefix": prefix, "n_hypotheses": len(rows),
           "n_built_with_coverage": built, "rows": rows}
    if events_by_year:
        out["event_counts_by_year_all_families"] = dict(sorted(events_by_year.items()))
    return out


def _h_event_build(sctx: Stage12Context) -> dict:
    return _coverage_lane(sctx, "event_")


def _h_residual_build(sctx: Stage12Context) -> dict:
    out = _coverage_lane(sctx, "residual_")
    out2 = _coverage_lane(sctx, "risk_")
    out["rows"].extend(out2["rows"])
    out["n_hypotheses"] += out2["n_hypotheses"]
    out["n_built_with_coverage"] += out2["n_built_with_coverage"]
    return out


def _h_screen(sctx: Stage12Context) -> dict:
    t = sctx.tournament()
    return {"stage": "12", "family_size": t["family_size"], "funnel": t["funnel"],
            "measurements": t["measurements"], "registry_version": t["registry_version"]}


def _h_multiple_testing(sctx: Stage12Context) -> dict:
    return sctx.tournament()["multiple_testing"]


def _h_deep_eval(sctx: Stage12Context) -> dict:
    t = sctx.tournament()
    return {"stage": "12", "qualified_singles": t["qualified_singles"],
            "qualified_new_alpha": t["qualified_new_alpha"],
            "same_sample_confirmations": t["same_sample_confirmations"],
            "ml_eligibility": t["ml_eligibility"]}


def _h_combinations(sctx: Stage12Context) -> dict:
    t = sctx.tournament()
    return {"stage": "12", "combination_measurements": t["combination_measurements"]}


def _h_shadow_decide(sctx: Stage12Context) -> dict:
    t = sctx.tournament()
    return t["shadow_decision"]


def _h_finalize(sctx: Stage12Context) -> dict:
    return build_command_center(sctx)


_LANE_HANDLERS = {
    LANE_AUTOPSY: _h_autopsy, LANE_OWNED_DATA: _h_owned_data,
    LANE_FULL_INVENTORY: _h_full_inventory, LANE_MATERIALIZE: _h_materialize,
    LANE_SAMPLE_POWER: _h_sample_power, LANE_EVENT_COVERAGE: _h_event_coverage,
    LANE_HYP_REGISTRY: _h_hypothesis_registry, LANE_EVENT_BUILD: _h_event_build,
    LANE_RESIDUAL_BUILD: _h_residual_build, LANE_SCREEN: _h_screen,
    LANE_MULTIPLE_TESTING: _h_multiple_testing, LANE_DEEP_EVAL: _h_deep_eval,
    LANE_COMBINATIONS: _h_combinations, LANE_SHADOW_DECIDE: _h_shadow_decide,
    LANE_FINALIZE: _h_finalize,
}


# --------------------------------------------------------------------------- #
# Command center (aggregated read-only snapshot).
# --------------------------------------------------------------------------- #
def build_command_center(sctx: Stage12Context) -> dict:
    sd = sctx.state_dir
    autopsy = _read_json(sd / _STATE_FILE[LANE_AUTOPSY], {}) or {}
    owned = _read_json(sd / _STATE_FILE[LANE_OWNED_DATA], {}) or {}
    power = _read_json(sd / _STATE_FILE[LANE_SAMPLE_POWER], {}) or {}
    registry = _read_json(sd / _STATE_FILE[LANE_HYP_REGISTRY], {}) or {}
    screen = _read_json(sd / _STATE_FILE[LANE_SCREEN], {}) or {}
    mt = _read_json(sd / _STATE_FILE[LANE_MULTIPLE_TESTING], {}) or {}
    deep = _read_json(sd / _STATE_FILE[LANE_DEEP_EVAL], {}) or {}
    shadow = _read_json(sd / _STATE_FILE[LANE_SHADOW_DECIDE], {}) or {}
    combos = _read_json(sd / _STATE_FILE[LANE_COMBINATIONS], {}) or {}
    full_inv = _read_json(sd / _STATE_FILE[LANE_FULL_INVENTORY], {}) or {}
    materialize = _read_json(sd / _STATE_FILE[LANE_MATERIALIZE], {}) or {}
    event_build = _read_json(sd / _STATE_FILE[LANE_EVENT_BUILD], {}) or {}
    flags = _read_json(sd / "lane_flags.json", {}) or {}
    epoch = sctx.epoch()
    complete = flags.get(LANE_FINALIZE) == epoch or all(
        flags.get(ln) == epoch for ln in LANE_ORDER if ln != LANE_FINALIZE)

    funnel = screen.get("funnel") or {}
    best_new = None
    for q in deep.get("qualified_new_alpha") or []:
        best_new = {"name": q.get("name"), "family": q.get("family"),
                    "rank_ic_t": q.get("rank_ic_t"), "net25": q.get("net25")}
        break
    terminal = _terminal_token(autopsy, deep, shadow, owned, full_inv)
    return {
        "stage": "12",
        "status": "COMPLETE" if complete else "IN_PROGRESS",
        "epoch": epoch,
        "safety_badges": SAFETY_BADGES,
        "no_automatic_promotion": True,
        "norgate_universe_materialization": {
            "universe_total": full_inv.get("universe_total"),
            "counts_by_classification": full_inv.get("counts_by_classification"),
            "counts_by_price_status": full_inv.get("counts_by_price_status"),
            "available_not_materialized": full_inv.get("available_not_materialized"),
            "available_in_window": full_inv.get("available_in_window"),
            "available_pre_window_only": full_inv.get("available_pre_window_only"),
            "materialized": full_inv.get("materialized"),
            "materialize_state": {"mode": materialize.get("mode"),
                                  "records_written": materialize.get("records_written"),
                                  "new_cap": materialize.get("new_cap"),
                                  "reason": materialize.get("reason")},
            "owned_evidence_exhausted": bool((full_inv.get("available_not_materialized") or 0) == 0),
        },
        "event_time_study": {
            "design": "monthly_event_cohorts",
            "families_built_with_coverage": event_build.get("n_built_with_coverage"),
            "event_counts_by_year": event_build.get("event_counts_by_year_all_families"),
        },
        "stage11_failure_taxonomy": {
            "verdict": (autopsy.get("design_recommendation") or {}).get("verdict"),
            "weakest_gate_distribution": autopsy.get("weakest_gate_distribution_all"),
            "weakest_gate_distribution_authoritative":
                autopsy.get("weakest_gate_distribution_deep_authoritative"),
            "effective_hypothesis_count": autopsy.get("effective_hypothesis_count"),
            "multiple_testing_burden": autopsy.get("multiple_testing_burden_diagnostic"),
        },
        "owned_data_breadth": {
            "price_span": owned.get("price_span"),
            "identity_resolution": owned.get("identity_resolution"),
            "cross_sectional_breadth": owned.get("cross_sectional_breadth"),
            "event_families_supported": owned.get("event_families_supported"),
            "event_families_total": owned.get("event_families_total"),
        },
        "sample_power": {
            "n_periods_nominal": power.get("n_periods_nominal"),
            "newey_west_effective_n": power.get("newey_west_effective_n"),
            "max_confirmatory_family_size": power.get("max_confirmatory_family_size"),
            "best_candidate_name": power.get("best_candidate_name"),
        },
        "pre_registered_hypotheses": {
            "registry_version": registry.get("registry_version"),
            "n_hypotheses": registry.get("n_hypotheses"),
            "n_economic_families": registry.get("n_economic_families"),
            "n_confirmatory": registry.get("n_confirmatory"),
            "n_exploratory": registry.get("n_exploratory"),
        },
        "tournament_funnel": funnel,
        "multiple_testing": {"family_size": mt.get("family_size"),
                             "fdr_survivors": mt.get("fdr_survivors"),
                             "method": mt.get("method")},
        "best_new_alpha_candidate": best_new,
        "same_sample_confirmations": deep.get("same_sample_confirmations"),
        "combination_keep_for_research": funnel.get("combination_keep_for_research"),
        "holdout_status": "reserved final 30% (sign-confirm only, once-usable)",
        "ml_eligibility": (deep.get("ml_eligibility") or {}).get("status"),
        "shadow_decision": {"status": shadow.get("status"),
                            "message": shadow.get("message")},
        "remaining_evidence_gaps": _evidence_gaps(owned, deep),
        "terminal_recommendation": terminal,
    }


def _evidence_gaps(owned: Mapping[str, Any], deep: Mapping[str, Any]) -> List[str]:
    gaps = []
    if (deep.get("ml_eligibility") or {}).get("status") == "ML_NOT_JUSTIFIED":
        gaps.append("ML not justified: PIT event features and/or effective sample "
                    "insufficient on owned data.")
    ir = owned.get("identity_resolution") or {}
    if (ir.get("coverage_pct_of_universe") or 100) < 60:
        gaps.append("Cross-sectional coverage %.0f%% of the Norgate universe is "
                    "survivorship-biased; genuinely-new orthogonal event alpha "
                    "cannot be adequately powered on owned data alone."
                    % (ir.get("coverage_pct_of_universe") or 0))
    if not (deep.get("qualified_new_alpha")):
        gaps.append("No genuinely-new, holdout-confirmed alpha on owned data; "
                    "further improvement needs fresh forward evidence or one "
                    "external orthogonal data family.")
    return gaps


def _terminal_token(autopsy, deep, shadow, owned, inventory=None) -> str:
    """Corrected terminal logic (BLOCKER 1). Owned evidence can ONLY be called
    exhausted once no AVAILABLE licensed history remains unmaterialised. While
    ``available_not_materialized > 0`` the honest terminal is RESUMABLE -- a low
    cross-sectional coverage percentage is NOT proof of exhaustion (that was the
    premature terminal this pass corrects)."""
    if deep.get("qualified_new_alpha"):
        return "STAGE12_SHADOW_STRATEGY_QUALIFIED"
    inv = inventory or {}
    avail = inv.get("available_not_materialized")
    if avail is not None and int(avail) > 0:
        # available licensed Norgate history remains unmaterialised
        return "STAGE12_CAMPAIGN_RESUMABLE"
    # The corrected event-time + residual study is complete and well-powered and
    # no defensible new alpha was found on the fully-materialised owned data.
    return "STAGE12_NO_DEFENSIBLE_ALPHA_FOUND"


# --------------------------------------------------------------------------- #
# Bounded, resumable campaign driver.
# --------------------------------------------------------------------------- #
def run_campaign(config_path: str, *, budget_seconds: float = 3600.0,
                 max_lanes: Optional[int] = None, resume: bool = True,
                 clock=time.monotonic) -> dict:
    """Run Stage 12 lanes in dependency order, bounded + resumable.

    A lane whose recorded epoch flag equals the current epoch is skipped (already
    complete). Stops cleanly when the wall-clock budget is exhausted, returning a
    RESUMABLE report. Writes only under the Stage 12 root + immutable artifacts.
    """
    sctx = Stage12Context(config_path)
    epoch = sctx.epoch()
    flags_path = sctx.state_dir / "lane_flags.json"
    flags = _read_json(flags_path, {}) or {}
    start = clock()
    done: List[str] = []
    skipped: List[str] = []
    ran = 0
    for lane in LANE_ORDER:
        if resume and flags.get(lane) == epoch:
            skipped.append(lane)
            continue
        if max_lanes is not None and ran >= max_lanes:
            break
        if clock() - start > budget_seconds:
            break
        _payload, lane_complete = _execute_lane(sctx, lane, epoch)
        if lane_complete:
            _stamp_lane(sctx, lane, epoch)
            flags[lane] = epoch
        done.append(lane)
        ran += 1
        if not lane_complete:
            break  # cursor-chunked lane not exhausted; resume the SAME lane next run
    remaining = [ln for ln in LANE_ORDER if flags.get(ln) != epoch]
    return {
        "stage": "12", "epoch": epoch, "lanes_completed_this_run": done,
        "lanes_skipped_already_complete": skipped, "lanes_remaining": remaining,
        "campaign_complete": not remaining,
        "terminal": ("STAGE12_CAMPAIGN_COMPLETE" if not remaining
                     else "STAGE12_CAMPAIGN_RESUMABLE"),
        "elapsed_seconds": round(clock() - start, 1),
    }


def _lane_summary(lane: str, payload: Mapping[str, Any]) -> dict:
    keys = ("status", "family_size", "n_hypotheses", "n_built_with_coverage",
            "fdr_survivors", "registry_version", "terminal_recommendation",
            "available_not_materialized", "available_in_window", "materialized",
            "mode", "records_written", "complete")
    return {k: payload.get(k) for k in keys if k in payload}


def _execute_lane(sctx: "Stage12Context", lane: str, epoch: str):
    """Run one lane handler, persist its state file + immutable artifact. Returns
    ``(payload, lane_complete)``. A cursor-chunked lane that has not exhausted its
    cursor sets ``payload['complete'] is False`` so its epoch flag is NOT stamped
    and the planner re-enqueues the SAME lane next cycle (with an advanced cursor).
    """
    payload = _LANE_HANDLERS[lane](sctx)
    _write_json(sctx.state_dir / _STATE_FILE[lane], payload)
    try:
        _write_artifact(sctx.artifact_root, lane, {"epoch": epoch, "lane": lane,
                        "summary": _lane_summary(lane, payload)})
    except Exception:
        pass
    return payload, (payload.get("complete") is not False)


def _stamp_lane(sctx: "Stage12Context", lane: str, epoch: str) -> None:
    fp = sctx.state_dir / "lane_flags.json"
    flags = _read_json(fp, {}) or {}
    flags[lane] = epoch
    _write_json(fp, flags)


# --------------------------------------------------------------------------- #
# Read-only snapshot loaders (for the API).
# --------------------------------------------------------------------------- #
def load_stage12_snapshot(config_path: str) -> dict:
    try:
        sctx = Stage12Context(config_path)
    except Exception as exc:  # pragma: no cover
        return {"stage": "12", "status": "UNAVAILABLE", "reason": str(exc)[:200],
                "safety_badges": SAFETY_BADGES}
    cc = _read_json(sctx.state_dir / _STATE_FILE[LANE_FINALIZE], None)
    if cc:
        return cc
    return {"stage": "12", "status": "IN_PROGRESS", "epoch": _safe_epoch(sctx),
            "safety_badges": SAFETY_BADGES, "no_automatic_promotion": True,
            "note": "Stage 12 campaign has not yet finalized a command center."}


def load_stage12_shadow(config_path: str) -> dict:
    try:
        sctx = Stage12Context(config_path)
        sh = _read_json(sctx.state_dir / _STATE_FILE[LANE_SHADOW_DECIDE], None)
        if sh:
            return sh
        return {"stage": "12", "status": "UNINITIALIZED", "safety_labels": SAFETY_BADGES}
    except Exception as exc:  # pragma: no cover
        return {"stage": "12", "status": "UNAVAILABLE", "reason": str(exc)[:200],
                "safety_labels": SAFETY_BADGES}


def _safe_epoch(sctx: Stage12Context) -> str:
    try:
        return sctx.epoch()
    except Exception:
        return "unknown"


# --------------------------------------------------------------------------- #
# Canonical ResearchQueue integration (BLOCKER 4).
#
# Stage 12 rides the SAME autonomous architecture as Stage 11: one live job at a
# time (origin=stage12-autopsy, category=DATA_VALIDATION, lane GLOB stage12.*),
# claim/attempt/retry/stale-recovery via ResearchQueue, epoch-scoped idempotent
# lane flags, and cursor-chunked heavy lanes. ``runtime`` wires the planner into
# the collect drain and the dispatcher into the DATA_VALIDATION handler chain.
# --------------------------------------------------------------------------- #
def build_stage12_context(cfg: Mapping[str, Any], *, queue=None, clock=None
                          ) -> "Stage12Context":
    """Build a Stage 12 context from an in-memory Stage 8 config dict (the shape
    ``runtime`` holds), so the collect cycle can plan/dispatch without a file path."""
    return Stage12Context(cfg=dict(cfg))


def next_incomplete_lane(flags: Mapping[str, Any], epoch: str) -> Optional[str]:
    """The first lane whose epoch flag is not the current epoch (pure helper)."""
    for lane in LANE_ORDER:
        if flags.get(lane) != epoch:
            return lane
    return None


def _has_live_stage12_job(queue) -> bool:
    """True if a Stage 12 job is already QUEUED/RUNNING/RETRYABLE -> at most one
    live Stage 12 job at a time."""
    try:
        from . import autonomous_research as _ar
    except Exception:  # pragma: no cover
        import autonomous_research as _ar  # type: ignore
    for st in (_ar.STATE_QUEUED, _ar.STATE_RUNNING, _ar.STATE_RETRYABLE):
        for j in queue.list_jobs(state=st, limit=1000):
            if str(getattr(j, "lane", "")).startswith(STAGE12_LANE_PREFIX) \
                    and getattr(j, "origin", None) == ORIGIN_12:
                return True
    return False


def _cursor_hint12(sctx: "Stage12Context", lane: str) -> int:
    """A monotone cursor stamped into the payload so successive chunk jobs for the
    same cursor-chunked lane carry DISTINCT payloads (distinct job identities)."""
    if lane == LANE_FULL_INVENTORY:
        st = _read_json(sctx.state_dir / _STATE_FILE[LANE_FULL_INVENTORY], {}) or {}
        return int(st.get("cursor_next") or 0)
    if lane == LANE_MATERIALIZE:
        st = _read_json(sctx.state_dir / _STATE_FILE[LANE_MATERIALIZE], {}) or {}
        return int(st.get("new_cap") or 0)
    return 0


def _enqueue12(queue, sctx: "Stage12Context", lane: str, reason: str) -> dict:
    try:
        from . import autonomous_research as _ar
    except Exception:  # pragma: no cover
        import autonomous_research as _ar  # type: ignore
    epoch = sctx.epoch()
    payload = {"stage": "12", "epoch": epoch, "lane": lane, "reason": reason,
               "cursor_hint": _cursor_hint12(sctx, lane)}
    pr = int(_stage12_cfg(sctx.cfg).get("priority", 1))
    job_id = queue.enqueue(_ar.CAT_DATA_VALIDATION, lane=lane, payload=payload,
                           priority=pr, origin=ORIGIN_12)
    return {"job_id": job_id, "lane": lane, "category": _ar.CAT_DATA_VALIDATION,
            "origin": ORIGIN_12, "reason": reason, "epoch": epoch}


def plan_next_stage12_job(queue, sctx: "Stage12Context", *,
                          cfg: Optional[dict] = None) -> Optional[dict]:
    """Enqueue the SINGLE next Stage 12 lane (dependency-ordered) into the shared
    ResearchQueue, or None when every lane is complete for the current epoch. At
    most one live Stage 12 job at a time. Gated on ``enabled`` + ``planner_enabled``."""
    cfg = cfg or _stage12_cfg(sctx.cfg)
    if not cfg.get("enabled") or not cfg.get("planner_enabled", False):
        return None
    if _has_live_stage12_job(queue):
        return None
    epoch = sctx.epoch()
    flags = _read_json(sctx.state_dir / "lane_flags.json", {}) or {}
    lane = next_incomplete_lane(flags, epoch)
    if lane is None:
        return None
    return _enqueue12(queue, sctx, lane, "Stage 12 evidence completion: advance %s" % lane)


def dispatch_stage12_job(job, sctx: "Stage12Context"):
    """Execute one claimed Stage 12 job -> ``(outcome, detail)`` for the drain.

    Runs the lane handler, persists its state + immutable artifact, and stamps the
    epoch-scoped lane flag ONLY when the (possibly cursor-chunked) lane is complete.
    An unknown lane is BLOCKED_SPECIFIC; a handler exception propagates so the
    drain settles it RETRYABLE (bounded by max_attempts)."""
    try:
        from . import autonomous_research as _ar
    except Exception:  # pragma: no cover
        import autonomous_research as _ar  # type: ignore
    lane = getattr(job, "lane", None)
    if lane is None and isinstance(job, Mapping):
        lane = job.get("lane")
    if lane not in _LANE_HANDLERS:
        return _ar.OUTCOME_BLOCKED_SPECIFIC, {"reason": "unknown stage12 lane %s" % lane,
                                              "lane": lane, "no_automatic_promotion": True}
    epoch = sctx.epoch()
    payload, lane_complete = _execute_lane(sctx, lane, epoch)
    if lane_complete:
        _stamp_lane(sctx, lane, epoch)
    detail = {"real_work": lane, "lane": lane, "disposition": "RESEARCH_ONLY",
              "lane_complete": bool(lane_complete), "epoch": epoch,
              "no_automatic_promotion": True, "summary": _lane_summary(lane, payload)}
    return _ar.OUTCOME_COMPLETED, detail
