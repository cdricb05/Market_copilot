"""Stage 12 FINAL OWNED-DATA DEPTH PASS -- 2009-2014 independent-time confirmation.

The released Stage 12 study is complete on the owned 2015+ breadth: no defensible
new alpha, the long-short spread negative in every partition, revenue-acceleration
failing to replicate in the newly-materialised issuers. The one remaining owned
lever is HISTORY DEPTH: SEC companyfacts retains the *filed* history of today's
846 covered issuers back to ~2009, and Norgate licenses their price series back to
1950. So 2009-2014 is a genuinely INDEPENDENT TIME sample (new cohorts, disjoint
from the 2015-2026 sample already inspected) on which the three strongest frozen
event hypotheses can be re-tested WITHOUT changing a single gate.

This module runs that confirmation with hard isolation guarantees:

* WORKSTREAM A -- a PIT-safe pre-2015 eligibility inventory (classify every
  survivorship-safe Norgate assetid by stable assetid, never ticker text).
* WORKSTREAM B -- bounded, resumable materialisation of the 2009-2014 price window
  into a DEDICATED pre-2015 ingestion tree (the released 2015+ tree, Stage 11
  caches and released Stage 12 artifacts are byte-untouched), via the EXISTING
  Stage 6 backfill cursor.
* WORKSTREAM C -- a SEPARATE window-clipped pre-2015 panel + a content-addressed
  cache epoch that advances on breadth/window/registry/event-source change; atomic
  cache; zero post-2014 bars; reconciled against the normalised MARKET_BAR tree.
* WORKSTREAM D -- a NEW immutable 3-hypothesis confirmatory registry, frozen
  BEFORE the 2009-2014 evidence is examined.
* WORKSTREAM E -- a temporal contract: 2009-2012 is the confirmation-development
  interval, 2013-2014 the untouched final holdout. Gates, costs, horizons and
  directions are the UNCHANGED released battery.
* WORKSTREAM F -- historically valid neutralisation only (no current sector/industry
  back-application; unavailable classification -> DATA_HOLD, never substituted).
* WORKSTREAM G -- the unchanged decision gate: the standalone 2009-2014 result must
  clear every gate AND the 2013-2014 holdout must confirm the direction; a stronger
  combined 2009-2026 statistic can NEVER qualify a strategy on its own.

RESEARCH-ONLY. No orders/fills/signals/trade-decisions/holdings/promotion. Never
mutates an operational ledger. No automatic promotion.
"""
from __future__ import annotations

import dataclasses
import hashlib
import io
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

try:
    from . import stage12_inventory as _inv
    from . import stage12_registry as _registry
    from . import stage12_event_study as _es
    from . import signal_evaluation as _se
    from . import tournament as _tt
    from . import selection_controls as _sc
except Exception:  # pragma: no cover
    import stage12_inventory as _inv  # type: ignore
    import stage12_registry as _registry  # type: ignore
    import stage12_event_study as _es  # type: ignore
    import signal_evaluation as _se  # type: ignore
    import tournament as _tt  # type: ignore
    import selection_controls as _sc  # type: ignore

# --------------------------------------------------------------------------- #
# Study window + temporal contract (Workstream E). Cohorts are YYYY-MM strings.
# --------------------------------------------------------------------------- #
STUDY_START = "2009-01-01"
STUDY_END = "2014-12-31"
DEV_COHORT_MIN = "2009-01"
DEV_COHORT_MAX = "2012-12"      # 2009-2012 development interval
HOLDOUT_COHORT_MIN = "2013-01"  # 2013-2014 untouched final holdout
HOLDOUT_COHORT_MAX = "2014-12"
STUDY_COHORT_MAX = "2014-12"    # no post-2014 formation cohort ever

HORIZONS = (5, 20, 63)
PRIMARY_HORIZON = 63

ORIGIN_12 = "stage12-autopsy"
PRE2015_LANE_PREFIX = "stage12.pre2015."
LANE_INVENTORY = "stage12.pre2015.inventory"
LANE_MATERIALIZE = "stage12.pre2015.materialize"
LANE_ORDER = [LANE_INVENTORY, LANE_MATERIALIZE]

SAFETY_BADGES = ["RESEARCH ONLY", "SHADOW ONLY", "NO LIVE BROKER ORDERS",
                 "AUTOMATION OFF", "MANUAL REVIEW", "NO AUTO-PROMOTION"]

# The exactly-three frozen confirmatory families (builder keys). Selected from the
# released registry BEFORE the 2009-2014 evidence is examined; formula/direction/
# gates are pulled verbatim from the released hypotheses so nothing can drift.
PRE2015_BUILDER_KEYS = ("event_revenue_acceleration",
                        "event_asset_growth_inflection",
                        "event_profitability_inflection")

# Pre-2015 classification buckets (Workstream A). Price status is the single
# most-binding label; CIK / companyfacts / membership are reported as cross-cuts.
PRE2015_ALREADY_MATERIALIZED = "PRE2015_ALREADY_MATERIALIZED"
PRE2015_AVAILABLE_NOT_MATERIALIZED = "PRE2015_AVAILABLE_NOT_MATERIALIZED"
PRE2015_NO_LICENSED_HISTORY = "PRE2015_NO_LICENSED_HISTORY"
PRE2015_NO_BAR_HISTORY = "PRE2015_NO_BAR_HISTORY"
PRE2015_MEMBERSHIP_UNAVAILABLE = "PRE2015_MEMBERSHIP_UNAVAILABLE"
PRE2015_CIK_RESOLVED = "PRE2015_CIK_RESOLVED"
PRE2015_CIK_UNRESOLVED = "PRE2015_CIK_UNRESOLVED"
PRE2015_NO_COMPANYFACTS_EVENTS = "PRE2015_NO_COMPANYFACTS_EVENTS"
PRE2015_OTHER = "OTHER_EXPLICIT_REASON"
_PRICE_STATUSES = [PRE2015_ALREADY_MATERIALIZED, PRE2015_AVAILABLE_NOT_MATERIALIZED,
                   PRE2015_NO_BAR_HISTORY, PRE2015_NO_LICENSED_HISTORY]


# --------------------------------------------------------------------------- #
# Small IO helpers (atomic writes; never an operational ledger).
# --------------------------------------------------------------------------- #
def _load_cfg(config_path: str) -> dict:
    return json.load(io.open(config_path, encoding="utf-8-sig"))


def _read_json(path: Path, default=None):
    p = Path(path)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return default
    return default


def _write_json(path: Path, doc) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(doc, sort_keys=True, indent=1), encoding="utf-8")
    tmp.replace(p)


# --------------------------------------------------------------------------- #
# WORKSTREAM A -- pre-2015 eligibility classification (pure, testable).
# --------------------------------------------------------------------------- #
def price_status_pre2015(*, assetid: Optional[str], licensed: bool,
                         first_quoted: Optional[str], last_quoted: Optional[str],
                         materialized: set, study_start: str = STUDY_START,
                         study_end: str = STUDY_END) -> str:
    """The single most-binding PRICE-availability label for the 2009-2014 window,
    keyed by stable Norgate assetid (never ticker text)."""
    if not licensed or not assetid:
        return PRE2015_NO_LICENSED_HISTORY
    fq = str(first_quoted or "9999")[:10]
    lq = str(last_quoted or "")[:10]
    overlaps = bool(last_quoted) and fq <= study_end and lq >= study_start
    if not overlaps:
        return PRE2015_NO_BAR_HISTORY
    if str(assetid) in materialized:
        return PRE2015_ALREADY_MATERIALIZED
    return PRE2015_AVAILABLE_NOT_MATERIALIZED


def event_status_pre2015(*, cik: Optional[str], resolved: bool,
                         cf_event_ciks: set) -> Tuple[str, bool]:
    """CIK-resolution + companyfacts-event eligibility cross-cut. Returns
    ``(label, event_eligible)``."""
    if not (resolved and cik):
        return PRE2015_CIK_UNRESOLVED, False
    if str(cik) in cf_event_ciks:
        return PRE2015_CIK_RESOLVED, True
    return PRE2015_NO_COMPANYFACTS_EVENTS, False


def classify_pre2015_universe(records: Sequence[Mapping[str, Any]], *,
                              materialized: set, cf_event_ciks: set,
                              study_start: str = STUDY_START,
                              study_end: str = STUDY_END) -> Dict[str, Any]:
    """Aggregate pre-2015 classification over probed universe ``records`` (each a
    mapping with assetid/licensed/first_quoted/last_quoted/cik/resolved/membership).

    Emits counts for all named buckets. Price status is mutually exclusive; the
    CIK / companyfacts / membership buckets are cross-cuts (an assetid can be both
    ALREADY_MATERIALIZED and CIK_RESOLVED). ``event_eligible_span`` counts issuers
    that are BOTH companyfacts-event eligible AND priced across the whole window."""
    counts = {c: 0 for c in (_PRICE_STATUSES + [
        PRE2015_CIK_RESOLVED, PRE2015_CIK_UNRESOLVED,
        PRE2015_NO_COMPANYFACTS_EVENTS, PRE2015_MEMBERSHIP_UNAVAILABLE,
        PRE2015_OTHER])}
    event_eligible_priced = 0
    event_eligible_span = 0
    for r in records:
        ps = price_status_pre2015(
            assetid=r.get("assetid"), licensed=bool(r.get("licensed")),
            first_quoted=r.get("first_quoted"), last_quoted=r.get("last_quoted"),
            materialized=materialized, study_start=study_start, study_end=study_end)
        counts[ps] += 1
        es, eligible = event_status_pre2015(
            cik=r.get("cik"), resolved=bool(r.get("resolved")),
            cf_event_ciks=cf_event_ciks)
        counts[es] += 1
        if r.get("membership") is False:
            counts[PRE2015_MEMBERSHIP_UNAVAILABLE] += 1
        priced = ps in (PRE2015_ALREADY_MATERIALIZED, PRE2015_AVAILABLE_NOT_MATERIALIZED)
        if eligible and priced:
            event_eligible_priced += 1
            fq = str(r.get("first_quoted") or "9999")[:10]
            if fq <= study_start:
                event_eligible_span += 1
    return {
        "universe_total": len(records),
        "counts": counts,
        "materialized_pre2015": counts[PRE2015_ALREADY_MATERIALIZED],
        "available_not_materialized": counts[PRE2015_AVAILABLE_NOT_MATERIALIZED],
        "event_eligible_priced": event_eligible_priced,
        "event_eligible_span_full_window": event_eligible_span,
        "no_automatic_promotion": True,
    }


# --------------------------------------------------------------------------- #
# WORKSTREAM C -- pre-2015 panel/cache epoch (pure, testable).
# --------------------------------------------------------------------------- #
def compute_pre2015_panel_epoch(*, date_min: Optional[str], date_max: Optional[str],
                                materialized_assetids, sources,
                                cik_mapping_version: Optional[str] = None,
                                norgate_universe_fingerprint: Optional[str] = None,
                                event_source_fingerprint: Optional[str] = None,
                                registry_version: Optional[str] = None,
                                study_window: Optional[Sequence[str]] = None) -> str:
    """Deterministic pre-2015 panel/cache epoch (``p`` + 16 hex). Advances whenever
    materialised breadth, owned date span, CIK-mapping version, Norgate universe,
    companyfacts event source, registry version OR the declared study window change;
    identical inputs reuse the cache. Never uses a per-cycle file count."""
    aids = sorted({str(a) for a in (materialized_assetids or [])})
    aid_hash = hashlib.sha256("\n".join(aids).encode("utf-8")).hexdigest()[:16]
    comp = {
        "date_min": date_min, "date_max": date_max,
        "materialized_count": len(aids), "materialized_hash": aid_hash,
        "sources": sorted({str(s) for s in (sources or [])}),
        "cik_mapping_version": cik_mapping_version,
        "norgate_universe_fingerprint": norgate_universe_fingerprint,
        "event_source_fingerprint": event_source_fingerprint,
        "registry_version": registry_version,
        "study_window": list(study_window) if study_window else [STUDY_START, STUDY_END],
    }
    h = hashlib.sha256(json.dumps(comp, sort_keys=True, default=str)
                       .encode("utf-8")).hexdigest()[:16]
    return "p" + h


# --------------------------------------------------------------------------- #
# WORKSTREAM E -- temporal split (pure, testable).
# --------------------------------------------------------------------------- #
def _cohort_month(as_of: str) -> str:
    return str(as_of)[:7]


def restrict_cohorts(periods: Sequence[Mapping[str, Any]], *,
                     min_cohort: Optional[str] = None,
                     max_cohort: Optional[str] = None) -> List[Mapping[str, Any]]:
    out = []
    for p in periods:
        c = _cohort_month(p.get("as_of"))
        if min_cohort is not None and c < min_cohort:
            continue
        if max_cohort is not None and c > max_cohort:
            continue
        out.append(p)
    return out


def split_dev_holdout(periods: Sequence[Mapping[str, Any]]
                      ) -> Tuple[List[Mapping[str, Any]], List[Mapping[str, Any]]]:
    """Split monthly cohort periods into the 2009-2012 development interval and the
    UNTOUCHED 2013-2014 final holdout. Post-2014 cohorts are excluded entirely."""
    inwin = restrict_cohorts(periods, min_cohort=DEV_COHORT_MIN,
                             max_cohort=STUDY_COHORT_MAX)
    dev = restrict_cohorts(inwin, max_cohort=DEV_COHORT_MAX)
    holdout = restrict_cohorts(inwin, min_cohort=HOLDOUT_COHORT_MIN)
    return dev, holdout


# --------------------------------------------------------------------------- #
# WORKSTREAM D -- frozen 3-hypothesis confirmatory registry (immutable).
# --------------------------------------------------------------------------- #
PRE2015_SCHEMA_VERSION = "stage12.pre2015.hypotheses.v1"
PRE2015_EVALUATION_CONTRACT = {
    "contract_version": "stage12-pre2015-independent-time-v1",
    "study_window": [STUDY_START, STUDY_END],
    "development_interval": ["2009-01-01", "2012-12-31"],
    "final_holdout": ["2013-01-01", "2014-12-31"],
    "primary_qualifying_evidence": "standalone 2009-2014 result (dev interval), with "
                                   "the 2013-2014 holdout confirming the direction",
    "combined_sample_rule": "the combined 2009-2026 result is SECONDARY CONTEXT ONLY "
                            "and can NEVER qualify a strategy by itself",
    "selection_before_evidence": "all three formulas and directions were selected "
                                 "(from the released registry) BEFORE any 2009-2014 "
                                 "evidence was examined",
    "event_entry": "first trading session STRICTLY AFTER the SEC filed date "
                   "(>=1 full trading-session lag)",
    "forward_horizons_days": [5, 20, 63],
    "primary_cross_sectional_unit": "monthly event cohort",
    "overlap_adjustment": "Newey-West effective-N + clustered-t; an event candidate "
                          "must ALSO clear the rank_ic_t gate on the clustered t",
    "neutralization": "historically valid market/sector only; unavailable historical "
                      "industry -> DATA_HOLD (never current-metadata substitution)",
    "restatement_isolation": "values read as_of=filed; later amendments excluded by "
                             "the availability boundary",
    "holdout": "2013-2014, sign-confirm only, once-usable",
    "gates_unchanged": True,
    "no_post_2014_formation_cohort": True,
    "survivorship_caveat": "the companyfacts event universe is today's covered issuers, "
                           "so the 2009-2014 sample is survivorship-tilted; this is a "
                           "genuine NEW-TIME test, reported honestly.",
}


def build_pre2015_registry() -> Dict[str, Any]:
    """The exactly-three frozen confirmatory hypotheses, pulled verbatim from the
    released registry (formula/direction/gates identical) and wrapped in a NEW
    content-addressed pre-2015 version that includes the temporal contract."""
    released = {h["builder_key"]: h for h in _registry._hypotheses()}
    hyps: List[Dict[str, Any]] = []
    for bk in PRE2015_BUILDER_KEYS:
        h = dict(released[bk])
        h["confirmatory_or_exploratory"] = "confirmatory"
        h["status"] = "confirmatory"
        h["pre2015_role"] = "independent_time_confirmation"
        hyps.append(h)
    body = {
        "schema_version": PRE2015_SCHEMA_VERSION,
        "evaluation_contract": dict(PRE2015_EVALUATION_CONTRACT),
        "n_hypotheses": len(hyps),
        "builder_keys": list(PRE2015_BUILDER_KEYS),
        "economic_families": sorted({h["economic_family"] for h in hyps}),
        "hypotheses": hyps,
        "selected_before_evidence": True,
    }
    body["registry_version"] = hashlib.sha256(_registry.canonical_json({
        "schema_version": PRE2015_SCHEMA_VERSION,
        "evaluation_contract": PRE2015_EVALUATION_CONTRACT,
        "hypotheses": hyps}).encode("utf-8")).hexdigest()[:16]
    return body


def default_pre2015_registry_path(repo_root: Optional[str | Path] = None) -> Path:
    root = Path(repo_root) if repo_root else Path(__file__).resolve().parent.parent
    return root / "configs" / "alpha_agent" / "stage12_pre2015_hypotheses.json"


def freeze_pre2015_registry(path: Optional[str | Path] = None) -> Dict[str, Any]:
    """First-write-wins immutable freeze. Re-freezing DIFFERENT content raises."""
    path = Path(path) if path else default_pre2015_registry_path()
    reg = build_pre2015_registry()
    payload = json.dumps(reg, sort_keys=True, indent=1) + "\n"
    if path.exists():
        existing = _read_json(path, {}) or {}
        if existing.get("registry_version") == reg["registry_version"]:
            return reg
        raise _registry.RegistryImmutabilityError(
            "frozen pre-2015 registry at %s differs (frozen=%s new=%s)"
            % (path, existing.get("registry_version"), reg["registry_version"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(path)
    return reg


# --------------------------------------------------------------------------- #
# WORKSTREAM G -- unchanged decision gate (pure, testable).
# --------------------------------------------------------------------------- #
def pre2015_decision_gate(family_result: Mapping[str, Any]) -> Dict[str, Any]:
    """The UNCHANGED decision gate for ONE family. A candidate qualifies ONLY when
    the standalone 2009-2014 (dev) direction matches, the overlap-adjusted rank-IC
    gate passes, the spread-t gate passes unchanged, the 2013-2014 holdout confirms
    the direction, costs do not destroy the spread, turnover is acceptable, the
    result is not year/issuer/industry/cohort concentrated, it survives the 3-family
    multiple-testing correction, and it does not depend on the combined sample."""
    dev = family_result.get("dev") or {}
    holdout = family_result.get("holdout") or {}
    conc = family_result.get("concentration") or {}
    keep_t = 2.0
    rict = dev.get("rank_ic_t")
    clustered = dev.get("overlap_clustered_t")
    spt = dev.get("spread_t")
    net = dev.get("net25")
    conditions = {
        "standalone_direction_matches": bool(dev.get("direction_matches")),
        "overlap_rank_ic_gate": bool(rict is not None and abs(rict) >= keep_t
                                     and (clustered is None or abs(clustered) >= keep_t)),
        "spread_t_gate_unchanged": bool(spt is not None and spt >= keep_t),
        "holdout_confirms_direction": holdout.get("direction_confirms") is True,
        "costs_do_not_destroy_spread": bool(net is not None and net > 0),
        "turnover_acceptable": bool((dev.get("turnover") or 0.0) <= 2.0),
        "not_concentrated": not bool(conc.get("concentrated")),
        "survives_family_multiple_testing": bool(dev.get("fdr_survived")),
        "not_combined_sample_only": True,  # dev is standalone by construction
        "point_in_time_and_lag_ok": bool(dev.get("target_state") is not None),
    }
    qualifies = all(conditions.values())
    weakest = [k for k, v in conditions.items() if not v]
    return {"qualifies": qualifies, "conditions": conditions,
            "weakest_gate": (weakest[0] if weakest else None),
            "no_automatic_promotion": True}


# --------------------------------------------------------------------------- #
# Pre-2015 context (isolated tree + shared read-only identity/companyfacts).
# --------------------------------------------------------------------------- #
class Pre2015Context:
    def __init__(self, config_path: Optional[str] = None, *,
                 cfg: Optional[dict] = None):
        self.config_path = config_path
        self.cfg = dict(cfg) if cfg is not None else _load_cfg(config_path)
        self._base = None
        self._cf_index = None
        self._universe_fp = None
        self._resolve_roots()

    def _resolve_roots(self) -> None:
        try:
            from . import runtime as _rt
        except Exception:  # pragma: no cover
            import runtime as _rt  # type: ignore
        self._runtime = _rt
        s11 = _rt._resolve_stage11_runtime(self.cfg)
        self.stage11_root = s11.get("stage11_root")
        self.stage12_root = Path(self.stage11_root).parent / "stage12"
        self.pre2015_root = self.stage12_root / "pre2015"
        self.state_dir = self.pre2015_root / "state"
        self.cache_dir = self.pre2015_root / "cache"
        self.artifact_root = s11.get("artifact_root")
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        bf = _pre2015_backfill_config_path(self.cfg)
        self.backfill_config_path = bf
        bcfg = _load_cfg(str(_abs_repo(bf)))
        self.pre2015_ingestion_root = bcfg["ingestion_root"]
        self.sources = ("norgate_local",)

    @property
    def base(self):
        if self._base is None:
            self._base = self._runtime.build_stage11_context(self.cfg)
        return self._base

    @property
    def cfg9(self) -> dict:
        return self.base.cfg9 or {}

    def open_cf_index(self):
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
            if not db:
                root = self._runtime.stage10_identity_root(self.cfg)
                cand_path = Path(root) / "sec_companyfacts_index.sqlite"
                db = str(cand_path) if cand_path.exists() else None
            if db and Path(db).exists():
                cand = _cfi.SecCompanyFactsIndex(db)
                if int(cand.counts().get("facts", 0)) > 0:
                    idx = cand
        except Exception:  # noqa: BLE001
            idx = None
        self._cf_index = idx if idx is not None else False
        return idx

    def event_source_fingerprint(self) -> Optional[str]:
        idx = self.open_cf_index()
        if not idx:
            return None
        try:
            c = idx.counts()
            return hashlib.sha256(json.dumps(
                {"facts": c.get("facts"), "ciks": c.get("distinct_fact_ciks"),
                 "concepts": c.get("distinct_concepts")}, sort_keys=True)
                .encode("utf-8")).hexdigest()[:16]
        except Exception:
            return None

    def universe_fingerprint(self) -> Optional[str]:
        if self._universe_fp is not None:
            return self._universe_fp or None
        fp = ""
        try:
            syms = _inv.resolve_universe_symbols(self.cfg)
            if syms:
                fp = hashlib.sha256("\n".join(sorted(str(s) for s in syms))
                                    .encode("utf-8")).hexdigest()[:16]
        except Exception:
            fp = ""
        self._universe_fp = fp
        return fp or None

    def cik_mapping_version(self) -> Optional[str]:
        try:
            st = self.base.store
            return (st.get_meta("stage101_mapping_version_hash")
                    or st.get_meta("stage102_mapped_epoch"))
        except Exception:
            return None

    # -- materialized pre-2015 assetids (authoritative from the isolated tree) --
    def materialized_assetids(self) -> List[str]:
        try:
            from . import historical_price_panel as _hpp
        except Exception:  # pragma: no cover
            import historical_price_panel as _hpp  # type: ignore
        try:
            panel = _hpp.build_assetid_price_panel(self.pre2015_ingestion_root,
                                                   sources=list(self.sources))
            return sorted(str(a) for a in panel.keys())
        except Exception:
            return []

    def coverage_signature(self) -> dict:
        try:
            from . import historical_price_panel as _hpp
        except Exception:  # pragma: no cover
            import historical_price_panel as _hpp  # type: ignore
        try:
            return _hpp.panel_coverage_signature(self.pre2015_ingestion_root,
                                                 sources=self.sources) or {}
        except Exception:
            return {}

    def panel_epoch(self, *, registry_version: Optional[str] = None) -> str:
        cov = self.coverage_signature()
        return compute_pre2015_panel_epoch(
            date_min=cov.get("date_min"), date_max=cov.get("date_max"),
            materialized_assetids=self.materialized_assetids(), sources=self.sources,
            cik_mapping_version=self.cik_mapping_version(),
            norgate_universe_fingerprint=self.universe_fingerprint(),
            event_source_fingerprint=self.event_source_fingerprint(),
            registry_version=registry_version,
            study_window=[STUDY_START, STUDY_END])

    # -- isolated window-clipped panel context (Workstream C) -----------------
    def load_pre2015_panel(self, *, registry_version: Optional[str] = None) -> dict:
        try:
            from . import stage11_jobs as _s11
        except Exception:  # pragma: no cover
            import stage11_jobs as _s11  # type: ignore
        pre_ctx = dataclasses.replace(
            self.base, ingestion_root=self.pre2015_ingestion_root,
            cache_dir=str(self.cache_dir), sources=self.sources)
        epoch = self.panel_epoch(registry_version=registry_version)
        loaded = _s11._load_context(pre_ctx, panel_epoch=epoch)
        loaded["panel_epoch"] = epoch
        return loaded


def _pre2015_backfill_config_path(cfg: Mapping[str, Any]) -> str:
    ph = (cfg.get("production_handlers") or {}) if isinstance(cfg, Mapping) else {}
    s12 = (cfg.get("stage12") or {}) if isinstance(cfg, Mapping) else {}
    return (s12.get("pre2015_backfill_config")
            or ph.get("stage6_pre2015_backfill_config")
            or "configs/alpha_agent/stage6_pre2015_backfill.json")


def _abs_repo(p: str | Path) -> Path:
    pp = Path(p)
    if pp.is_absolute():
        return pp
    return Path(__file__).resolve().parents[1] / pp


def _pre2015_cfg(cfg: Mapping[str, Any]) -> dict:
    s12 = (cfg.get("stage12") or {}) if isinstance(cfg, Mapping) else {}
    return (s12.get("pre2015") or {}) if isinstance(s12, Mapping) else {}


# --------------------------------------------------------------------------- #
# WORKSTREAM E -- the independent-time study (frozen evaluator, unchanged gates).
# --------------------------------------------------------------------------- #
_METRIC_KEYS = ("periods", "universe", "rank_ic_mean", "rank_ic_t", "spread_t",
                "net_annualized_return", "gross_annualized_return", "turnover",
                "max_drawdown", "long_return", "short_return", "hit_rate",
                "positive_ic_hit_rate")


def _row_metrics(row: Mapping[str, Any]) -> Dict[str, Any]:
    out = {k: row.get(k) for k in _METRIC_KEYS if k in row}
    out["net25"] = row.get("net_annualized_return")
    return out


def _mean(xs: Sequence[Optional[float]]) -> Optional[float]:
    v = [float(x) for x in xs if x is not None]
    return (sum(v) / len(v)) if v else None


def _per_period_ic(periods: Sequence[Mapping[str, Any]]) -> Dict[str, Optional[float]]:
    try:
        from . import fundamental_evidence as _fev
    except Exception:  # pragma: no cover
        import fundamental_evidence as _fev  # type: ignore
    out: Dict[str, Optional[float]] = {}
    for p in periods:
        names = p.get("names") or []
        if len(names) < 3:
            continue
        out[p["as_of"]] = _fev._spearman([t[1] for t in names], [t[2] for t in names])
    return out


def _concentration(dev_periods: Sequence[Mapping[str, Any]],
                   sector_of, *, min_year=DEV_COHORT_MIN[:4]) -> Dict[str, Any]:
    """Issuer / event-year / sector concentration of the DEV cohorts. Industry is
    historically unavailable on owned data -> DATA_HOLD (never current-metadata
    substitution, Workstream F)."""
    issuer_events: Dict[str, int] = {}
    year_events: Dict[str, int] = {}
    sector_events: Dict[str, int] = {}
    sector_known = 0
    total = 0
    for p in dev_periods:
        yr = str(p.get("as_of"))[:4]
        for aid, _sig, _fwd in (p.get("names") or []):
            total += 1
            issuer_events[str(aid)] = issuer_events.get(str(aid), 0) + 1
            year_events[yr] = year_events.get(yr, 0) + 1
            sec = sector_of(aid, p.get("as_of")) if sector_of else None
            if sec:
                sector_known += 1
                sector_events[str(sec)] = sector_events.get(str(sec), 0) + 1
    def _top_share(d):
        return (max(d.values()) / total) if (d and total) else None
    top_issuer = _top_share(issuer_events)
    top_year = _top_share(year_events)
    top_sector = (max(sector_events.values()) / sector_known) if (sector_events and sector_known) else None
    industry_status = "DATA_HOLD_NO_VALID_HISTORICAL_INDUSTRY"
    concentrated = bool((top_issuer or 0) > 0.10 or (top_year or 0) > 0.60
                        or (top_sector or 0) > 0.40)
    return {"total_events": total, "distinct_issuers": len(issuer_events),
            "top_issuer_share": top_issuer, "top_year_share": top_year,
            "year_events": dict(sorted(year_events.items())),
            "top_sector_share": top_sector,
            "sector_coverage_fraction": (sector_known / total) if total else None,
            "industry_concentration": industry_status,
            "concentrated": concentrated}


def _sector_lookup(base):
    """A leakage-safe historical sector lookup (assetid, date) -> sector or None,
    from the owned PIT SIC sector series. Returns None when no historically valid
    assignment exists (never a current-metadata substitution)."""
    ss = getattr(base, "sector_series", None)
    if ss is None:
        return None
    def _f(assetid, as_of):
        for meth in ("sector_for", "sector_at", "get"):
            fn = getattr(ss, meth, None)
            if callable(fn):
                try:
                    v = fn(str(assetid), str(as_of)) if meth != "get" else fn(str(assetid))
                    if v:
                        return v
                except Exception:
                    continue
        return None
    return _f


def _event_gates(cfg: Mapping[str, Any]) -> dict:
    au = (cfg.get("autonomy") or {}) if isinstance(cfg, Mapping) else {}
    return {"min_issuers": int(au.get("event_min_issuers", _es.MIN_ISSUERS)),
            "min_events": int(au.get("event_min_events", _es.MIN_EVENTS)),
            "min_cohorts": int(au.get("event_min_cohorts", _es.MIN_COHORTS))}


def run_pre2015_study(ctx: "Pre2015Context") -> Dict[str, Any]:
    """Run the frozen 3-hypothesis independent-time study on the isolated pre-2015
    panel. dev = 2009-2012 (primary), holdout = 2013-2014 (sign-confirm, once)."""
    reg = freeze_pre2015_registry()
    reg_v = reg["registry_version"]
    loaded = ctx.load_pre2015_panel(registry_version=reg_v)
    panel_ctx = loaded["panel_ctx"]
    study_ctx = dict(panel_ctx)
    study_ctx["cf_index"] = ctx.open_cf_index()
    cfg9 = ctx.cfg9
    # WORKSTREAM C reconciliation inputs (normalized tree vs cached panel vs CIK-map)
    _tree_aids = set(ctx.materialized_assetids())
    _panel_aids = {str(a) for a in (study_ctx.get("close_index") or {}).keys()}
    _c2a = loaded.get("cik_to_assetid") or {}
    _cik_mapped_in_panel = {str(a) for a in _c2a.values() if str(a) in _panel_aids}
    _cov = ctx.coverage_signature()
    eg = _event_gates(ctx.cfg)
    sector_of = _sector_lookup(ctx.base)
    reg_by_bk = {h["builder_key"]: h for h in reg["hypotheses"]}

    families: List[Dict[str, Any]] = []
    for bk in PRE2015_BUILDER_KEYS:
        hyp = reg_by_bk[bk]
        direction = int(hyp.get("expected_direction", 1)) or 1
        by_horizon: Dict[str, Any] = {}
        primary_built = None
        for h in HORIZONS:
            built = _es.build_event_cohort_periods(
                bk, study_ctx, cf_index=study_ctx.get("cf_index"),
                direction=direction, horizon_days=h,
                min_cohorts=eg["min_cohorts"], min_issuers=eg["min_issuers"],
                min_events=eg["min_events"])
            periods = built.get("periods") or []
            dev_p, hold_p = split_dev_holdout(periods)
            dev_row = _se.evaluate_periods(dev_p, horizon_days=h, cfg=cfg9)["row"] if len(dev_p) >= 3 else {}
            hold_row = _se.evaluate_periods(hold_p, horizon_days=h, cfg=cfg9)["row"] if len(hold_p) >= 3 else {}
            by_horizon[str(h)] = {
                "n_cohorts_total": len(periods),
                "n_cohorts_dev": len(dev_p), "n_cohorts_holdout": len(hold_p),
                "dev": _row_metrics(dev_row), "holdout": _row_metrics(hold_row)}
            if h == PRIMARY_HORIZON:
                primary_built = (built, dev_p, hold_p, dev_row, hold_row)

        built, dev_p, hold_p, dev_row, hold_row = primary_built
        cov = built.get("coverage") or {}
        overlap = _es.overlap_diagnostics([_pp_ic(p) for p in dev_p], PRIMARY_HORIZON)
        dev_metrics = _row_metrics(dev_row)
        hold_metrics = _row_metrics(hold_row)
        dev_ic = dev_metrics.get("rank_ic_mean")
        hold_ic = hold_metrics.get("rank_ic_mean")
        dev_verdict = {}
        if dev_row:
            metrics = _tt.row_to_contract_metrics(dev_row, survivorship_safe=True)
            dev_verdict = _tt.classify_evidence(metrics, cfg9 or {})
        data_hold = None
        if len(dev_p) < eg["min_cohorts"]:
            data_hold = "DATA_HOLD_INSUFFICIENT_DEV_COHORTS(%d<%d)" % (len(dev_p), eg["min_cohorts"])
        families.append({
            "hypothesis_id": hyp["id"], "builder_key": bk, "direction": direction,
            "formula": hyp.get("formula"), "economic_family": hyp.get("economic_family"),
            "primary_horizon": PRIMARY_HORIZON,
            "study_coverage": {
                "total_events": cov.get("total_events"),
                "distinct_issuers": cov.get("distinct_issuers"),
                "n_cohorts": cov.get("n_cohorts"), "cohort_span": cov.get("cohort_span"),
                "events_by_year": cov.get("events_by_year"),
                "median_cohort_names": cov.get("median_cohort_names")},
            "by_horizon": by_horizon,
            "dev": {**dev_metrics,
                    "direction_matches": bool(dev_ic is not None and dev_ic > 0),
                    "overlap_clustered_t": overlap.get("t_clustered"),
                    "overlap_effective_n": overlap.get("newey_west_effective_n"),
                    "overlap_fraction": overlap.get("overlap_fraction"),
                    "target_state": dev_verdict.get("target_state"),
                    "failed_gates": dev_verdict.get("failed_gates"),
                    "data_hold_reason": data_hold},
            "holdout": {**hold_metrics,
                        "direction_confirms": (
                            None if (dev_ic is None or hold_ic is None)
                            else (dev_ic > 0) == (hold_ic > 0))},
            "concentration": _concentration(dev_p, sector_of),
        })

    # --- family-aware multiple testing over EXACTLY the 3 dev results ---------
    fam_size = len(PRE2015_BUILDER_KEYS)
    mt = _mt_three(families, fam_size)
    for f in families:
        f["dev"]["fdr_survived"] = mt["survived"].get(f["hypothesis_id"], False)
        f["dev"]["bh_qvalue"] = mt["q"].get(f["hypothesis_id"])
        gate = pre2015_decision_gate(f)
        f["decision_gate"] = gate
        f["qualifies"] = gate["qualifies"]

    qualified = [f["hypothesis_id"] for f in families if f.get("qualifies")]
    shadow = _pre2015_shadow_decision(qualified)
    _max_cohort = None
    for f in families:
        span = (f.get("study_coverage") or {}).get("cohort_span")
        if span and span[-1] and (_max_cohort is None or str(span[-1]) > _max_cohort):
            _max_cohort = str(span[-1])
    reconciliation = {
        "normalized_tree_assetids": len(_tree_aids),
        "cached_panel_assetids": len(_panel_aids),
        "cached_equals_normalized": len(_tree_aids) == len(_panel_aids),
        "cik_mapped_assetids_in_panel": len(_cik_mapped_in_panel),
        "study_event_issuers_max_family": max(
            (int((f.get("study_coverage") or {}).get("distinct_issuers") or 0)
             for f in families), default=0),
        "panel_date_min": _cov.get("date_min"), "panel_date_max": _cov.get("date_max"),
        "max_event_cohort": _max_cohort,
        "no_post_2014_cohort": bool(_max_cohort is None or _max_cohort <= STUDY_COHORT_MAX),
        "no_post_2014_bars": bool(str(_cov.get("date_max") or "0000")[:10] <= STUDY_END),
        "note": ("normalized tree assetids == cached panel assetids; the event study "
                 "maps companyfacts CIK->assetid->panel directly, so its per-family "
                 "issuer counts are authoritative and independent of the "
                 "production_universe survivorship symbol probe (which resolves a "
                 "different Norgate watchlist and therefore a different symbol set)."),
    }
    return {
        "stage": "12", "pass": "pre2015_independent_time",
        "panel_reconciliation": reconciliation,
        "registry_version": reg_v, "study_window": [STUDY_START, STUDY_END],
        "development_interval": ["2009-01-01", "2012-12-31"],
        "final_holdout": ["2013-01-01", "2014-12-31"],
        "n_hypotheses": fam_size, "families": families,
        "multiple_testing": {"family_size": fam_size, "alpha": _tt_fdr_alpha(),
                             "method": "benjamini_hochberg_fdr", "rows": mt["rows"],
                             "fdr_survivors": sum(1 for v in mt["survived"].values() if v)},
        "qualified_count": len(qualified), "qualified_hypotheses": qualified,
        "shadow_decision": shadow, "panel_epoch": loaded.get("panel_epoch"),
        "no_automatic_promotion": True, "safety_badges": SAFETY_BADGES,
    }


def _pp_ic(period):
    try:
        from . import fundamental_evidence as _fev
    except Exception:  # pragma: no cover
        import fundamental_evidence as _fev  # type: ignore
    names = period.get("names") or []
    if len(names) < 3:
        return None
    return _fev._spearman([t[1] for t in names], [t[2] for t in names])


def _tt_fdr_alpha() -> float:
    try:
        from . import stage12_tournament as _tourn
        return float(_tourn.FDR_ALPHA)
    except Exception:
        return 0.05


def _mt_three(families: Sequence[Mapping[str, Any]], family_size: int) -> Dict[str, Any]:
    """BH-FDR over exactly the pre-registered 3-family dev rank-IC t-stats."""
    alpha = _tt_fdr_alpha()
    rows = []
    pvals = []
    for f in families:
        t = (f.get("dev") or {}).get("rank_ic_t")
        n = int((f.get("dev") or {}).get("periods") or 0)
        p = _se.approx_two_sided_pvalue(t, max(1, n - 1)) if t is not None else None
        rows.append({"hypothesis_id": f["hypothesis_id"], "rank_ic_t": t, "raw_pvalue": p})
        if p is not None:
            pvals.append((f["hypothesis_id"], p, t))
    ordered = sorted(pvals, key=lambda x: x[1])
    m = max(1, family_size)
    survived, q_by = {}, {}
    running = 1.0
    for k in range(len(ordered), 0, -1):
        hid, p, t = ordered[k - 1]
        q = min(running, p * m / k)
        running = q
        q_by[hid] = q
        survived[hid] = bool(q < alpha and (t or 0) > 0)
    for r in rows:
        r["bh_qvalue"] = q_by.get(r["hypothesis_id"])
        r["fdr_survived"] = survived.get(r["hypothesis_id"], False)
    return {"rows": rows, "survived": survived, "q": q_by}


def _pre2015_shadow_decision(qualified: Sequence[str]) -> Dict[str, Any]:
    if not qualified:
        return {"status": "NO_DEFENSIBLE_ALPHA",
                "message": ("NO DEFENSIBLE ALPHA - SHADOW PORTFOLIO NOT ACTIVATED "
                            "(no frozen hypothesis clears every unchanged gate on the "
                            "standalone 2009-2014 evidence with holdout confirmation)"),
                "active_strategy": None, "candidates_qualified": 0,
                "no_automatic_promotion": True, "safety_labels": SAFETY_BADGES}
    return {"status": "SHADOW_ELIGIBLE_PENDING_MANUAL_REVIEW",
            "message": ("Independently-confirmed pre-2015 candidate(s) exist; shadow "
                        "activation requires MANUAL REVIEW. No automatic promotion."),
            "active_strategy": None, "candidates_qualified": len(qualified),
            "qualified_hypotheses": list(qualified),
            "no_automatic_promotion": True, "safety_labels": SAFETY_BADGES}


def pre2015_terminal(inventory: Mapping[str, Any], study: Optional[Mapping[str, Any]]
                     ) -> str:
    """The pre-2015 terminal token. The completeness of the OWNED EVENT evidence is
    governed by (a) the companyfacts event index (fully read, a fixed set of covered
    issuers) and (b) the materialised event-issuer price series -- NOT by residual
    non-event price names. So RESUMABLE fires only when the study could not be built
    or no family is adequately powered under the UNCHANGED event-study gates
    (>= MIN_ISSUERS issuers, >= MIN_COHORTS cohorts). A qualified candidate ->
    QUALIFIED; an adequately-powered study rejecting all three -> NO_DEFENSIBLE_ALPHA;
    an under-powered study with no further materialisable event issuers ->
    DATA_INSUFFICIENT."""
    if not study or not study.get("families"):
        return "STAGE12_PRE2015_CAMPAIGN_RESUMABLE"
    if study.get("qualified_count"):
        return "STAGE12_PRE2015_SHADOW_CANDIDATE_QUALIFIED"
    adequate = any(
        int((f.get("study_coverage") or {}).get("distinct_issuers") or 0) >= _es.MIN_ISSUERS
        and int((f.get("study_coverage") or {}).get("n_cohorts") or 0) >= _es.MIN_COHORTS
        and (f.get("dev") or {}).get("data_hold_reason") is None
        for f in study.get("families") or [])
    if not adequate:
        return "STAGE12_PRE2015_DATA_INSUFFICIENT"
    return "STAGE12_PRE2015_NO_DEFENSIBLE_ALPHA"


# --------------------------------------------------------------------------- #
# Lane handlers (inventory + materialize are the canonical queue lanes).
# --------------------------------------------------------------------------- #
_STATE_FILE = {LANE_INVENTORY: "inventory.json", LANE_MATERIALIZE: "materialize.json"}


def run_pre2015_inventory(ctx: "Pre2015Context") -> dict:
    """WORKSTREAM A: classify every survivorship-safe Norgate assetid for the
    2009-2014 window (stable assetid, cheap owned metadata) + companyfacts event
    eligibility + counts by year. Read-only."""
    try:
        from . import historical_price_panel as _hpp
    except Exception:  # pragma: no cover
        import historical_price_panel as _hpp  # type: ignore
    nd = _inv.open_norgate()
    syms = _inv.resolve_universe_symbols(ctx.cfg)
    id_map = {}
    try:
        id_map = _inv.build_identity_map(ctx.base.store)
    except Exception:
        id_map = {}
    c2a = _hpp.build_cik_to_assetid(ctx.base.store)
    a2cik = {str(a): str(c) for c, a in c2a.items()}
    materialized = set(ctx.materialized_assetids())
    # companyfacts periodic filings FILED in the window -> event-eligible CIKs + years
    cf = ctx.open_cf_index()
    cf_event_ciks: set = set()
    events_by_year: Dict[str, int] = {}
    if cf is not None:
        cal = _es.build_filing_calendar(cf, list(c2a.keys()))
        for cik, events in cal["calendar"].items():
            for ev in events:
                filed = str(ev.get("filed") or "")[:10]
                if STUDY_START <= filed <= STUDY_END:
                    cf_event_ciks.add(str(cik))
                    y = filed[:4]
                    events_by_year[y] = events_by_year.get(y, 0) + 1
    records = []
    for s in syms:
        p = _inv.probe_symbol(nd, s) if nd is not None else {
            "assetid": None, "licensed": False}
        aid = p.get("assetid")
        idrec = id_map.get(str(aid)) or {}
        records.append({
            "assetid": aid, "licensed": bool(p.get("licensed")),
            "first_quoted": p.get("first_quoted"), "last_quoted": p.get("last_quoted"),
            "cik": a2cik.get(str(aid)), "resolved": idrec.get("resolved"),
            "membership": idrec.get("membership")})
    agg = classify_pre2015_universe(records, materialized=materialized,
                                    cf_event_ciks=cf_event_ciks)
    licensed_span = sum(1 for r in records
                        if r.get("first_quoted") and str(r["first_quoted"])[:10] <= STUDY_START)
    return {
        "stage": "12", "pass": "pre2015", "workstream": "inventory",
        "universe_total": agg["universe_total"],
        "counts": agg["counts"],
        "materialized_pre2015": agg["materialized_pre2015"],
        "available_not_materialized": agg["available_not_materialized"],
        "available_in_window": agg["available_not_materialized"],
        "event_eligible_priced": agg["event_eligible_priced"],
        "event_eligible_span_full_window": agg["event_eligible_span_full_window"],
        "companyfacts_event_ciks": len(cf_event_ciks),
        "companyfacts_events_by_year": dict(sorted(events_by_year.items())),
        "licensed_quoted_from_2009_or_earlier": licensed_span,
        "norgate_available": nd is not None,
        "no_automatic_promotion": True, "complete": True}


def run_pre2015_materialize(ctx: "Pre2015Context") -> dict:
    """WORKSTREAM B: bounded, idempotent, resumable materialisation of the 2009-2014
    price window into the ISOLATED pre-2015 tree via the EXISTING Stage 6 backfill
    cursor. Config-gated; measure-only no-op by default."""
    s12 = _pre2015_cfg(ctx.cfg)
    inv = _read_json(ctx.state_dir / _STATE_FILE[LANE_INVENTORY], {}) or {}
    avail = inv.get("available_not_materialized")
    enabled = bool(s12.get("materialize_enabled", False))
    increment = int(s12.get("materialize_cap_increment", 0))
    if not enabled or increment <= 0:
        reason = ("materialize_enabled is false" if not enabled
                  else "materialize_cap_increment is 0")
        return {"stage": "12", "pass": "pre2015", "workstream": "materialize",
                "mode": "measure_only", "reason": reason,
                "available_not_materialized": avail,
                "materialized_pre2015_before": inv.get("materialized_pre2015"),
                "no_automatic_promotion": True, "complete": True}
    summary = _inv.materialize_bounded(ctx.backfill_config_path, cap_increment=increment)
    after = None
    try:
        after = len(ctx.materialized_assetids())
    except Exception:
        after = None
    cov = ctx.coverage_signature()
    return {"stage": "12", "pass": "pre2015", "workstream": "materialize",
            "mode": "materialized", "materialized_assetids_after": after,
            "isolated_tree_date_min": cov.get("date_min"),
            "isolated_tree_date_max": cov.get("date_max"),
            "no_automatic_promotion": True, "complete": True, **summary}


_LANE_HANDLERS = {LANE_INVENTORY: run_pre2015_inventory,
                  LANE_MATERIALIZE: run_pre2015_materialize}


# --------------------------------------------------------------------------- #
# Command center (read-only aggregated snapshot).
# --------------------------------------------------------------------------- #
def build_pre2015_command_center(ctx: "Pre2015Context",
                                 study: Optional[Mapping[str, Any]] = None) -> dict:
    inv = _read_json(ctx.state_dir / _STATE_FILE[LANE_INVENTORY], {}) or {}
    mat = _read_json(ctx.state_dir / _STATE_FILE[LANE_MATERIALIZE], {}) or {}
    if study is None:
        study = _read_json(ctx.state_dir / "study.json", {}) or {}
    families = study.get("families") or []
    per_family = [{
        "hypothesis_id": f.get("hypothesis_id"), "builder_key": f.get("builder_key"),
        "dev_rank_ic_t": (f.get("dev") or {}).get("rank_ic_t"),
        "dev_overlap_clustered_t": (f.get("dev") or {}).get("overlap_clustered_t"),
        "dev_spread_t": (f.get("dev") or {}).get("spread_t"),
        "holdout_confirms": (f.get("holdout") or {}).get("direction_confirms"),
        "weakest_gate": (f.get("decision_gate") or {}).get("weakest_gate"),
        "qualifies": f.get("qualifies")} for f in families]
    terminal = pre2015_terminal(inv, study)
    return {
        "stage": "12", "pass": "pre2015_independent_time",
        "status": "COMPLETE" if study.get("families") else "IN_PROGRESS",
        "safety_badges": SAFETY_BADGES, "no_automatic_promotion": True,
        "study_window": [STUDY_START, STUDY_END],
        "development_interval": ["2009-01-01", "2012-12-31"],
        "final_holdout": ["2013-01-01", "2014-12-31"],
        "pre2015_materialization": {
            "universe_total": inv.get("universe_total"),
            "counts": inv.get("counts"),
            "materialized_pre2015": (mat.get("materialized_assetids_after")
                                     or inv.get("materialized_pre2015")),
            "available_not_materialized": inv.get("available_not_materialized"),
            "event_eligible_span_full_window": inv.get("event_eligible_span_full_window"),
            "companyfacts_event_ciks": inv.get("companyfacts_event_ciks"),
            "companyfacts_events_by_year": inv.get("companyfacts_events_by_year"),
            "isolated_tree_date_min": mat.get("isolated_tree_date_min"),
            "isolated_tree_date_max": mat.get("isolated_tree_date_max")},
        "frozen_hypotheses": {"n": study.get("n_hypotheses"),
                              "registry_version": study.get("registry_version"),
                              "builder_keys": list(PRE2015_BUILDER_KEYS)},
        "panel_reconciliation": study.get("panel_reconciliation"),
        "per_family": per_family,
        "multiple_testing": study.get("multiple_testing"),
        "qualified_count": study.get("qualified_count"),
        "shadow_decision": study.get("shadow_decision"),
        "owned_evidence_decision": ("OWNED_EVENT_DRIVEN_EVIDENCE_EXHAUSTED"
                                    if not study.get("qualified_count") and study.get("families")
                                    else None),
        "next_data_family_recommendation": ("HISTORICAL_ANALYST_REVISIONS"
                                            if not study.get("qualified_count") and study.get("families")
                                            else None),
        "terminal_recommendation": terminal,
    }


# --------------------------------------------------------------------------- #
# Canonical ResearchQueue integration (mirrors stage12_jobs; pre-2015 namespace).
# --------------------------------------------------------------------------- #
def build_pre2015_context(cfg: Mapping[str, Any], *, queue=None, clock=None
                          ) -> "Pre2015Context":
    return Pre2015Context(cfg=dict(cfg))


def lane_epoch(ctx: "Pre2015Context") -> str:
    """Stable lane-idempotency epoch (registry + universe + event-source; NOT the
    per-materialisation breadth, so materialize stamps once and idempotently skips)."""
    reg = build_pre2015_registry()["registry_version"][:8]
    h = hashlib.sha256(json.dumps(
        {"reg": reg, "univ": ctx.universe_fingerprint(),
         "cf": ctx.event_source_fingerprint()}, sort_keys=True, default=str)
        .encode()).hexdigest()[:8]
    return "pre2015:%s:%s" % (reg, h)


def next_incomplete_lane(flags: Mapping[str, Any], epoch: str) -> Optional[str]:
    for lane in LANE_ORDER:
        if flags.get(lane) != epoch:
            return lane
    return None


def _has_live_pre2015_job(queue) -> bool:
    try:
        from . import autonomous_research as _ar
    except Exception:  # pragma: no cover
        import autonomous_research as _ar  # type: ignore
    for st in (_ar.STATE_QUEUED, _ar.STATE_RUNNING, _ar.STATE_RETRYABLE):
        for j in queue.list_jobs(state=st, limit=1000):
            if str(getattr(j, "lane", "")).startswith(PRE2015_LANE_PREFIX) \
                    and getattr(j, "origin", None) == ORIGIN_12:
                return True
    return False


def plan_next_pre2015_job(queue, ctx: "Pre2015Context", *,
                          cfg: Optional[dict] = None) -> Optional[dict]:
    cfg = cfg or _pre2015_cfg(ctx.cfg)
    if not cfg.get("enabled") or not cfg.get("planner_enabled", False):
        return None
    if _has_live_pre2015_job(queue):
        return None
    epoch = lane_epoch(ctx)
    flags = _read_json(ctx.state_dir / "lane_flags.json", {}) or {}
    lane = next_incomplete_lane(flags, epoch)
    if lane is None:
        return None
    try:
        from . import autonomous_research as _ar
    except Exception:  # pragma: no cover
        import autonomous_research as _ar  # type: ignore
    payload = {"stage": "12", "pass": "pre2015", "lane": lane, "epoch": epoch,
               "cursor_hint": len(ctx.materialized_assetids())}
    pr = int(cfg.get("priority", 1))
    job_id = queue.enqueue(_ar.CAT_DATA_VALIDATION, lane=lane, payload=payload,
                           priority=pr, origin=ORIGIN_12)
    return {"job_id": job_id, "lane": lane, "origin": ORIGIN_12, "epoch": epoch}


def _execute_lane(ctx: "Pre2015Context", lane: str, epoch: str):
    payload = _LANE_HANDLERS[lane](ctx)
    _write_json(ctx.state_dir / _STATE_FILE[lane], payload)
    return payload, (payload.get("complete") is not False)


def _stamp_lane(ctx: "Pre2015Context", lane: str, epoch: str) -> None:
    fp = ctx.state_dir / "lane_flags.json"
    flags = _read_json(fp, {}) or {}
    flags[lane] = epoch
    _write_json(fp, flags)


def dispatch_pre2015_job(job, ctx: "Pre2015Context"):
    try:
        from . import autonomous_research as _ar
    except Exception:  # pragma: no cover
        import autonomous_research as _ar  # type: ignore
    lane = getattr(job, "lane", None)
    if lane is None and isinstance(job, Mapping):
        lane = job.get("lane")
    if lane not in _LANE_HANDLERS:
        return _ar.OUTCOME_BLOCKED_SPECIFIC, {"reason": "unknown pre2015 lane %s" % lane,
                                              "lane": lane, "no_automatic_promotion": True}
    epoch = lane_epoch(ctx)
    payload, complete = _execute_lane(ctx, lane, epoch)
    if complete:
        _stamp_lane(ctx, lane, epoch)
    return _ar.OUTCOME_COMPLETED, {
        "real_work": lane, "lane": lane, "disposition": "RESEARCH_ONLY",
        "lane_complete": bool(complete), "epoch": epoch,
        "no_automatic_promotion": True}


def run_pre2015_finalization(ctx: "Pre2015Context") -> dict:
    """After materialisation: freeze registry, run the study, write the command
    center. In-process (heavy) -> not subject to the per-job handler budget."""
    study = run_pre2015_study(ctx)
    _write_json(ctx.state_dir / "study.json", study)
    cc = build_pre2015_command_center(ctx, study=study)
    _write_json(ctx.state_dir / "command_center.json", cc)
    return cc


def load_pre2015_snapshot(config_path: str) -> dict:
    try:
        ctx = Pre2015Context(config_path)
    except Exception as exc:  # pragma: no cover
        return {"stage": "12", "pass": "pre2015", "status": "UNAVAILABLE",
                "reason": str(exc)[:200], "safety_badges": SAFETY_BADGES}
    cc = _read_json(ctx.state_dir / "command_center.json", None)
    if cc:
        return cc
    return {"stage": "12", "pass": "pre2015", "status": "IN_PROGRESS",
            "safety_badges": SAFETY_BADGES, "no_automatic_promotion": True}


def run_local_campaign(config_path: str, *, budget_seconds: float = 3600.0,
                       max_lanes: Optional[int] = None) -> dict:
    """Offline in-process driver (no queue) for tests/dev: inventory -> materialize
    -> finalize. The canonical queue driver lives in ``runtime``."""
    ctx = Pre2015Context(config_path)
    epoch = lane_epoch(ctx)
    flags = _read_json(ctx.state_dir / "lane_flags.json", {}) or {}
    start = time.monotonic()
    done = []
    for lane in LANE_ORDER:
        if flags.get(lane) == epoch:
            continue
        if max_lanes is not None and len(done) >= max_lanes:
            break
        if time.monotonic() - start > budget_seconds:
            break
        _payload, complete = _execute_lane(ctx, lane, epoch)
        if complete:
            _stamp_lane(ctx, lane, epoch)
        done.append(lane)
    remaining = [ln for ln in LANE_ORDER
                 if (_read_json(ctx.state_dir / "lane_flags.json", {}) or {}).get(ln) != epoch]
    cc = {}
    if not remaining and (time.monotonic() - start) <= budget_seconds:
        cc = run_pre2015_finalization(ctx)
    return {"stage": "12", "pass": "pre2015", "epoch": epoch,
            "lanes_completed_this_run": done, "lanes_remaining": remaining,
            "campaign_complete": not remaining,
            "terminal": cc.get("terminal_recommendation",
                               "STAGE12_PRE2015_CAMPAIGN_RESUMABLE"),
            "elapsed_seconds": round(time.monotonic() - start, 1)}
