r"""Controlled Autonomous Research Execution Bridge — composition / dispatch / persistence /
read owner.

This is the ONE composition, idempotent-dispatch, immutable-artifact and read-contract
owner for the bridge that lets the Persistent Alpha Research Agent (the research GOVERNOR)
commission BOUNDED research from the existing AlphaAgent research lab (``alpha_agent`` —
ResearchQueue, tournament, gates). It performs NO bridge calculation of its own — the
single canonical calculation lives in ``engine.research_bridge``. This module only:

  1. READS the latest immutable Research Agent assessment (``api.research_agent``) and
     selects the top eligible, DISPATCHABLE research opportunity (the opportunity concept
     is NOT forked — it is normalized into a provenance-complete contract by the kernel).
  2. BUILDS a bounded Controlled Research Mandate (kernel) and persists it immutably under
     a dedicated research root (idempotent by mandate hash; a different opportunity /
     evidence hash SUPERSEDES — never silently reuses).
  3. DISPATCHES the mandate IDEMPOTENTLY into the EXISTING ``alpha_agent`` durable
     ``ResearchQueue`` (``enqueue`` deduplicated by the deterministic dispatch identity).
     It does NOT create a second queue / campaign / tournament / registry; it does NOT
     manufacture candidate results. Recovery reuses the queue's own claim / lease /
     ``requeue_stale`` machinery.
  4. INGESTS the returned canonical evidence into an immutable research-result envelope,
     runs the ONE canonical Alpha Strength Gate (delegating to
     ``alpha_agent.tournament.classify_evidence`` + Benjamini-Hochberg FDR
     (``alpha_agent.selection_controls``) + champion orthogonality), and produces the
     Research Agent re-assessment (kernel). Terminal outcomes are the EXISTING constants;
     ``NO_DEFENSIBLE_ALPHA`` / ``DATA_HOLD`` are valid successful conclusions.
  5. Exposes ONE read contract for ``GET /v1/research/research-bridge`` (read-only;
     ``NOT_RUN`` before a mandate exists) — no GET recomputes or dispatches anything.

It writes ONLY to its own research root (``PAPER_TRADER_RESEARCH_BRIDGE_DIR``, default
``D:\Stock_Prediction_app_data\research_bridge``) and enqueues into the existing alpha_agent
research DBs on ``D:`` — NEVER an operational ledger root under the user home, PostgreSQL, a
champion pointer, an operational / alpha target, an order, a fill, a holding, cash or NAV.
It promotes / recalibrates / retrains no model and enables no cadence. Research governance
only; manual model review remains mandatory.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from paper_trader.engine import research_bridge as kernel

SCHEMA_VERSION = kernel.SCHEMA_VERSION
COMPOSITION_OWNER = "api.research_bridge"
PHASE = "ResearchExecutionBridge"

# Read-layer states.
STATE_NOT_RUN = "NOT_RUN"
STATE_NO_OPPORTUNITY = "NO_DISPATCHABLE_OPPORTUNITY"
STATE_UNAVAILABLE = "UNAVAILABLE"
STATE_DRAFTED = kernel.MANDATE_DRAFTED
STATE_DISPATCHED = kernel.MANDATE_DISPATCHED
STATE_IN_PROGRESS = kernel.MANDATE_IN_PROGRESS
STATE_COMPLETED = kernel.MANDATE_COMPLETED
STATE_DATA_HOLD = kernel.MANDATE_DATA_HOLD
READ_STATE_VOCAB = tuple([STATE_NOT_RUN, STATE_NO_OPPORTUNITY, STATE_UNAVAILABLE]
                         + list(kernel.MANDATE_LIFECYCLE_VOCAB))

# --- immutable research root (NEVER an operational ledger) -------------------- #
BRIDGE_DIR_ENV = "PAPER_TRADER_RESEARCH_BRIDGE_DIR"
_DEFAULT_BRIDGE_DIR = Path(r"D:\Stock_Prediction_app_data\research_bridge")
_MANDATES_SUBDIR = "mandates"
_ENVELOPES_SUBDIR = "envelopes"
_INDEX_FILE = "index.json"

# Canonical tournament gate config (thresholds are read, never re-invented here).
_REPO_ROOT = Path(__file__).resolve().parent.parent
_STAGE9_CFG = _REPO_ROOT / "configs" / "alpha_agent" / "stage9_tournament.json"


# --------------------------------------------------------------------------- #
# time / io helpers
# --------------------------------------------------------------------------- #
def _now(now: Optional[datetime]) -> datetime:
    return now or datetime.now(timezone.utc)


def _now_iso(now: Optional[datetime]) -> str:
    return _now(now).astimezone(timezone.utc).isoformat()


def _bridge_dir(bridge_dir=None) -> Path:
    if bridge_dir is not None:
        return Path(bridge_dir)
    env = os.environ.get(BRIDGE_DIR_ENV)
    return Path(env) if env else _DEFAULT_BRIDGE_DIR


def _mandates_dir(bridge_dir=None) -> Path:
    return _bridge_dir(bridge_dir) / _MANDATES_SUBDIR


def _envelopes_dir(bridge_dir=None) -> Path:
    return _bridge_dir(bridge_dir) / _ENVELOPES_SUBDIR


def _index_path(bridge_dir=None) -> Path:
    return _bridge_dir(bridge_dir) / _INDEX_FILE


def _load_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _atomic_write_json(path: Path, obj: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, sort_keys=True, separators=(",", ":"))
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def load_gate_cfg(path: Optional[Path] = None) -> dict:
    """Load the canonical Stage-9 tournament gate config (thresholds source of truth)."""
    cfg = _load_json(path or _STAGE9_CFG)
    return cfg or {}


# --------------------------------------------------------------------------- #
# 1) select + normalize the top eligible dispatchable opportunity
# --------------------------------------------------------------------------- #
def _champion_from_assessment(assessment: dict) -> dict:
    ch = dict((assessment or {}).get("champion") or {})
    return {
        "model_id": ch.get("model_id"),
        "primary_model_id": ch.get("primary_model_id"),
        "strategy_version": ch.get("strategy_version"),
        "model_registry_version": ch.get("model_registry_version"),
        "automatic_promotion_allowed": bool(ch.get("automatic_promotion_allowed", False)),
    }


def select_opportunity(*, assessment: dict) -> Optional[dict]:
    """Normalize the top eligible DISPATCHABLE opportunity from a Research Agent assessment.

    Opportunities are already ranked (priority desc) by the Research Agent kernel. The
    bridge selects the highest-priority opportunity that maps to at least one economically
    coherent owned-data hypothesis family; observational (EVIDENCE/DATA) opportunities are
    normalized too but returned only if nothing dispatchable exists.
    """
    a = assessment or {}
    champion = _champion_from_assessment(a)
    eligible_date = a.get("eligible_market_date")
    evidence_hash = ((a.get("provenance") or {}).get("forward_evidence_hash")
                     or a.get("assessment_hash"))
    degradation = (a.get("degradation") or {}).get("categories") or []
    opps = list(a.get("research_opportunities") or [])
    normalized = [kernel.normalize_opportunity(
        opportunity=o, champion=champion, eligible_market_date=eligible_date,
        evidence_snapshot_hash=evidence_hash, degradation_categories=degradation)
        for o in opps]
    dispatchable = [n for n in normalized if n.get("dispatchable")]
    pool = dispatchable or normalized
    if not pool:
        return None
    # opportunities already ordered by priority desc; keep that order deterministically.
    return pool[0]


def read_latest_assessment(*, active_book_id: str, eligible_market_date: str,
                           research_agent_dir=None) -> Optional[dict]:
    """Read the current immutable Research Agent assessment (never runs the engine)."""
    from paper_trader.api import research_agent as ra
    art = ra.load_latest_artifact(active_book_id=active_book_id,
                                  eligible_market_date=eligible_market_date,
                                  research_agent_dir=research_agent_dir)
    if not art:
        return None
    return art.get("assessment") or art


# --------------------------------------------------------------------------- #
# 2) build + persist the bounded mandate (immutable, idempotent, supersede-safe)
# --------------------------------------------------------------------------- #
def _index_key(active_book_id: Optional[str], eligible_market_date: Optional[str]) -> str:
    return "%s|%s" % (active_book_id or "?", eligible_market_date or "?")


def build_and_persist_mandate(*, normalized_opportunity: dict, active_book_id: str,
                              mandate_version: int = 1, policy: Optional[dict] = None,
                              bridge_dir=None, now: Optional[datetime] = None) -> dict:
    mandate = kernel.finalize_mandate_hash(
        kernel.build_mandate(normalized_opportunity=normalized_opportunity,
                             mandate_version=mandate_version, policy=policy))
    key = _index_key(active_book_id, mandate.get("eligible_market_date"))
    index = _load_json(_index_path(bridge_dir)) or {}
    existing = (index.get(key) or {})
    mandate_id = mandate["mandate_id"]

    if existing.get("mandate_hash") == mandate.get("mandate_hash"):
        return {"status": "REUSED_EXISTING", "mandate_id": existing.get("mandate_id"),
                "mandate": mandate, "persisted": True, "reused": True, "superseded": False}

    superseded = existing.get("mandate_id")
    payload = {
        "record_type": "mandate",
        "mandate_id": mandate_id,
        "composition_owner": COMPOSITION_OWNER,
        "generated_at": _now_iso(now),
        "active_book_id": active_book_id,
        "supersedes_mandate_id": superseded,
        "mandate": mandate,
        "normalized_opportunity": normalized_opportunity,
        "dispatch": None,
        "envelope_id": None,
        "reassessment": None,
        "lifecycle_state": kernel.MANDATE_DRAFTED,
    }
    path = _mandates_dir(bridge_dir) / ("%s.json" % mandate_id)
    _atomic_write_json(path, payload)
    index[key] = {"mandate_id": mandate_id, "path": str(path),
                  "mandate_hash": mandate.get("mandate_hash"),
                  "opportunity_id": mandate.get("opportunity_id"),
                  "eligible_market_date": mandate.get("eligible_market_date"),
                  "active_book_id": active_book_id,
                  "lifecycle_state": kernel.MANDATE_DRAFTED,
                  "generated_at": payload["generated_at"]}
    _atomic_write_json(_index_path(bridge_dir), index)
    return {"status": ("SUPERSEDED_PRIOR" if superseded else "CREATED"),
            "mandate_id": mandate_id, "path": str(path), "mandate": mandate,
            "persisted": True, "reused": False, "superseded": bool(superseded)}


def load_mandate_record(*, mandate_id: str, bridge_dir=None) -> Optional[dict]:
    return _load_json(_mandates_dir(bridge_dir) / ("%s.json" % mandate_id))


def load_latest_mandate_record(*, active_book_id: str, eligible_market_date: str,
                               bridge_dir=None) -> Optional[dict]:
    index = _load_json(_index_path(bridge_dir)) or {}
    entry = index.get(_index_key(active_book_id, eligible_market_date))
    if not entry:
        return None
    return _load_json(Path(entry["path"])) if entry.get("path") else \
        load_mandate_record(mandate_id=entry.get("mandate_id"), bridge_dir=bridge_dir)


def _update_mandate_record(mandate_id: str, updates: dict, bridge_dir=None,
                           active_book_id=None) -> Optional[dict]:
    rec = load_mandate_record(mandate_id=mandate_id, bridge_dir=bridge_dir)
    if rec is None:
        return None
    rec.update(updates)
    path = _mandates_dir(bridge_dir) / ("%s.json" % mandate_id)
    _atomic_write_json(path, rec)
    # advance index lifecycle pointer
    index = _load_json(_index_path(bridge_dir)) or {}
    key = _index_key(active_book_id or rec.get("active_book_id"),
                     rec.get("mandate", {}).get("eligible_market_date"))
    if key in index and "lifecycle_state" in updates:
        index[key]["lifecycle_state"] = updates["lifecycle_state"]
        _atomic_write_json(_index_path(bridge_dir), index)
    return rec


# --------------------------------------------------------------------------- #
# 3) idempotent dispatch into the EXISTING alpha_agent ResearchQueue
# --------------------------------------------------------------------------- #
def _build_real_queue(clock: Optional[Callable[[], str]] = None):
    """Open the EXISTING durable ResearchQueue (never a second queue).

    Uses the canonical opener ``runtime.build_autonomy_queue`` with the canonical
    ``configs/alpha_agent/stage8_autonomy.json`` so the bridge dispatches into the exact
    same ``autonomy.sqlite`` the AlphaAgent uses.
    """
    from paper_trader.alpha_agent import autonomous_research as ar
    from paper_trader.alpha_agent import runtime as rt

    def _default_clock() -> str:
        return datetime.now(timezone.utc).isoformat()

    clk = clock or _default_clock
    cfg = _load_json(_REPO_ROOT / "configs" / "alpha_agent" / "stage8_autonomy.json")
    if cfg and hasattr(rt, "build_autonomy_queue"):
        return rt.build_autonomy_queue(cfg, clock=clk)
    db = os.environ.get("PAPER_TRADER_ALPHA_AUTONOMY_DB") or \
        r"D:\Stock_Prediction_app_data\alpha_agent\stage8\autonomy.sqlite"
    return ar.ResearchQueue(db, clock=clk)


def dispatch_mandate(*, mandate: dict, active_book_id: str, queue=None, bridge_dir=None,
                     now: Optional[datetime] = None) -> dict:
    """Idempotently dispatch the mandate into the EXISTING durable ResearchQueue.

    Repeat dispatch of the same mandate does NOT create a duplicate job — the queue's
    partial-unique live-dedupe index keyed on the deterministic dispatch identity returns
    the existing live job. Persists the dispatch record onto the mandate artifact.
    """
    if not mandate.get("dispatchable"):
        return {"status": "NOT_DISPATCHABLE",
                "reason": mandate.get("non_dispatchable_reason"),
                "mandate_id": mandate.get("mandate_id"), "dispatched": False}

    desc = kernel.dispatch_descriptor(mandate)
    q = queue if queue is not None else _build_real_queue()
    # Reuse the queue's own idempotent enqueue (dedupe_key = deterministic dispatch id).
    job_id = q.enqueue(desc["category"], lane=desc["lane"], payload=desc["payload"],
                       priority=desc["priority"], dedupe_key=desc["dedupe_seed"],
                       origin=desc["origin"], not_before=desc.get("not_before"))
    dispatch = {
        "schema_version": kernel.DISPATCH_SCHEMA_VERSION,
        "dispatch_identity": mandate.get("dispatch_identity"),
        "job_id": job_id,
        "category": desc["category"],
        "origin": desc["origin"],
        "lane": desc["lane"],
        "queue_owner": "alpha_agent.autonomous_research.ResearchQueue",
        "dispatched_at": _now_iso(now),
        "idempotent": True,
    }
    _update_mandate_record(mandate.get("mandate_id"),
                           {"dispatch": dispatch, "lifecycle_state": kernel.MANDATE_DISPATCHED},
                           bridge_dir=bridge_dir, active_book_id=active_book_id)
    return {"status": "DISPATCHED", "mandate_id": mandate.get("mandate_id"),
            "job_id": job_id, "dispatch": dispatch, "dispatched": True}


def recover_stale_dispatch(*, queue=None, stale_seconds: Optional[int] = None) -> dict:
    """Reuse the queue's own stale-claim recovery (no bridge-specific recovery invented)."""
    q = queue if queue is not None else _build_real_queue()
    reclaimed = 0
    if hasattr(q, "requeue_stale"):
        try:
            reclaimed = q.requeue_stale() if stale_seconds is None \
                else q.requeue_stale(stale_seconds=stale_seconds)
        except TypeError:
            reclaimed = q.requeue_stale()
    return {"status": "RECOVERED", "reclaimed": reclaimed,
            "recovery_owner": "alpha_agent.autonomous_research.ResearchQueue.requeue_stale"}


# --------------------------------------------------------------------------- #
# 3b) EXECUTE the mandate THROUGH the shared AlphaAgent machinery (operator-run).
#     This closes the true end-to-end loop WITHOUT a second queue / registry /
#     handler / experiment runner / tournament:
#       generate -> claim -> execute -> settle -> ingest.
#     The mandate seeds REAL, pre-registered owned-price revalidation experiments
#     (the SAME canonical shape the tournament's own generate_stage9_4_followups
#     enqueues) into the ONE durable ResearchQueue, tagged with the bridge
#     origin/lane; an EXPLICIT bounded operator drain (never the scheduled cadence,
#     which does not admit origin=research-bridge) claims ONLY those jobs and runs
#     them through the PRODUCTION CAT_EXPERIMENT handler on the owned Norgate panel;
#     the canonical tournament ingester records the resulting Stage-5 evidence. No
#     candidate result is ever manufactured.
# --------------------------------------------------------------------------- #
BRIDGE_ORIGIN = "research-bridge"                 # == kernel.dispatch_descriptor origin
BRIDGE_EXPERIMENT_STRATEGY = "research_bridge_revalidation"
# MORE SPECIFIC than the kernel mandate-governance lane ("research_bridge.experiment")
# so the operator drain admits ONLY executable experiments and never the inert
# governance-marker job the kernel dispatch enqueues on the shorter lane.
BRIDGE_EXPERIMENT_LANE = "research_bridge.experiment.revalidation"

# Canonical configs (never re-declared here): the Stage-5 config carries the three
# protected operational-ledger roots (and is the production_handlers stage-5 config)
# so the ONE canonical fingerprint owner reads them; the Stage-8 config opens the
# ONE durable queue + the real production handler map.
_STAGE5_CFG = _REPO_ROOT / "configs" / "alpha_agent" / "stage5_experiment_factory.json"
_STAGE8_CFG = _REPO_ROOT / "configs" / "alpha_agent" / "stage8_autonomy.json"

# Bridge hypothesis families that map to the OWNED computable-price catalogue
# (the executable owned-data family for the tournament price-factor engine).
_PRICE_HYPOTHESIS_FAMILIES = frozenset({
    kernel.FAM_PRICE_MOMENTUM, kernel.FAM_REVERSAL_PATH, kernel.FAM_RISK_VOLATILITY,
    kernel.FAM_LIQUIDITY_VOLUME, kernel.FAM_CROSS_FAMILY, kernel.FAM_RESIDUAL_PRICE,
})


def _as_int(value: Any, default: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def fingerprint_operational_ledgers(cfg_path: Optional[Path] = None) -> dict:
    """The ONE canonical operational-ledger integrity fingerprint.

    Reuses ``alpha_agent.runtime.fingerprint_ledgers`` over the roots DECLARED in the
    canonical Stage-5 config (never a bridge-local re-implementation and never a
    hard-coded operational-ledger path). Research READS this baseline to prove it
    changed nothing; it never writes an operational ledger.
    """
    from paper_trader.alpha_agent import runtime as rt
    cfg = _load_json(cfg_path or _STAGE5_CFG) or {}
    return rt.fingerprint_ledgers(cfg)


def _open_production_stack(*, stage8_cfg: Optional[dict] = None, clock=None) -> dict:
    """Open the EXISTING durable queue + REAL production handler map + tournament
    registry + Stage-9 gate config — every one a canonical ``alpha_agent`` owner. No
    second queue / registry / handler / tournament is created."""
    from paper_trader.alpha_agent import runtime as rt
    from paper_trader.alpha_agent import tournament as trn
    cfg8 = stage8_cfg if stage8_cfg is not None else (_load_json(_STAGE8_CFG) or {})
    queue = rt.build_autonomy_queue(cfg8, clock=clock)
    handlers = rt.build_production_autonomy_handlers(
        cfg8, contact_email=(cfg8.get("production_handlers") or {}).get("contact_email"),
        queue=queue)
    p9 = rt.resolve_stage9_config_path(cfg8)
    cfg9 = trn.load_config(p9) if p9 else {}
    registry = (rt.build_tournament_registry(cfg9, clock=clock)
                if isinstance(cfg9, dict) and cfg9.get("tournament_db") else None)
    return {"queue": queue, "handlers": handlers, "registry": registry,
            "cfg9": cfg9, "cfg8": cfg8}


def _mandate_permits_owned_price(mandate: dict) -> bool:
    fams = set((mandate or {}).get("allowed_hypothesis_families") or [])
    return bool(fams & _PRICE_HYPOTHESIS_FAMILIES)


def generate_mandate_experiments(*, mandate: dict, registry, queue,
                                 max_experiments: Optional[int] = None) -> list:
    """Seed REAL, pre-registered owned-price revalidation experiments BOUNDED by the
    mandate into the shared queue (origin=research-bridge, lane=
    research_bridge.experiment.revalidation, category=EXPERIMENT).

    Reuses the tournament registry's OWN ``try_register_generated`` spec-dedupe and
    the production CAT_EXPERIMENT handler; it MANUFACTURES NO candidate result. Each
    experiment reproduces an existing owned-price catalogue candidate on its own
    registered horizon (the canonical Stage-9.4 revalidation shape), stamped with the
    mandate id for attribution. Idempotent: a repeat call for the same mandate
    re-registers nothing (dedupe) and enqueues nothing new.
    """
    from paper_trader.alpha_agent import autonomous_research as ar
    from paper_trader.alpha_agent import price_factors as pf
    mid = (mandate or {}).get("mandate_id")
    budget = _as_int(max_experiments if max_experiments is not None
                     else (mandate.get("bounds") or {}).get("max_experiments"), 6)
    generated: list = []
    if registry is None or budget <= 0 or not _mandate_permits_owned_price(mandate):
        return generated
    computable = pf.COMPUTABLE_FEATURES
    # DETERMINISTIC, BOUNDED selection: the first ``budget`` distinct (feature, horizon)
    # owned-price studies in a stable order. The SAME set is selected on every run for a
    # given mandate + budget, so a repeat run adds nothing new (it does not advance to the
    # next slice of the catalogue) — the campaign is idempotent, not an open-ended search.
    seen_study: set = set()
    selected: list = []
    for cand in sorted((registry.list() or []),
                       key=lambda c: ((c.get("spec") or {}).get("feature") or "",
                                      _as_int((c.get("spec") or {}).get("horizon_days"), 21),
                                      c.get("candidate_id") or "")):
        spec0 = cand.get("spec") or {}
        feat = spec0.get("feature")
        if not feat or feat not in computable:
            continue
        hz = _as_int(spec0.get("horizon_days"), 21)
        study = (feat, hz)
        if study in seen_study:
            continue
        seen_study.add(study)
        selected.append((cand, feat, hz, spec0))
        if len(selected) >= budget:
            break
    for cand, feat, hz, spec0 in selected:
        vspec = {
            "feature": feat,
            "horizon_days": hz,
            "rebalance": spec0.get("rebalance", "monthly"),
            "template": "campaign_price_factor",
            "study_kind": BRIDGE_EXPERIMENT_STRATEGY,
            "revalidation_of": cand.get("candidate_id"),
            "research_bridge_mandate_id": mid,
        }
        sh = registry.try_register_generated(
            strategy=BRIDGE_EXPERIMENT_STRATEGY, spec=vspec,
            candidate_id=cand.get("candidate_id"))
        if sh is None:
            continue   # already generated for this mandate in a prior run (idempotent)
        job_id = queue.enqueue(
            ar.CAT_EXPERIMENT, lane=BRIDGE_EXPERIMENT_LANE,
            payload={"tournament": True, "candidate_id": cand.get("candidate_id"),
                     "strategy": BRIDGE_EXPERIMENT_STRATEGY, "feature": feat,
                     "spec": vspec, "research_bridge_mandate_id": mid},
            priority=3, origin=BRIDGE_ORIGIN,
            dedupe_key="rbx_%s_%s_%s" % (mid, feat, hz))
        generated.append({"candidate_id": cand.get("candidate_id"), "feature": feat,
                          "horizon_days": hz, "spec_hash": sh, "job_id": job_id})
    return generated


def drain_mandate_experiments(*, queue, handlers, mandate: dict,
                              max_jobs: Optional[int] = None,
                              budget_seconds: Optional[float] = None) -> dict:
    """Run an EXPLICIT bounded operator drain that admits ONLY this bridge's
    executable experiments (origin=research-bridge AND category=EXPERIMENT AND lane
    GLOB research_bridge.experiment.revalidation*). Reuses the queue's OWN atomic
    claim/lease + the production handler map; nothing else in the shared queue is
    claimed, and the scheduled cadence never admits this origin."""
    from paper_trader.alpha_agent import autonomous_research as ar
    bounds = (mandate or {}).get("bounds") or {}
    mx = _as_int(max_jobs if max_jobs is not None else bounds.get("max_experiments"), 6)
    bs = budget_seconds if budget_seconds is not None \
        else bounds.get("compute_budget_seconds")
    return ar.drain_jobs(
        queue, handlers, max_jobs=mx, origins=[BRIDGE_ORIGIN],
        categories=[ar.CAT_EXPERIMENT], lane_prefixes=[BRIDGE_EXPERIMENT_LANE],
        budget_seconds=(float(bs) if bs else None))


def _completed_mandate_jobs(*, queue, mandate_id: str, limit: int = 500) -> list:
    """All COMPLETED bridge experiment jobs stamped with this mandate id, in the
    shape ``tournament.ingest_completed_experiments`` consumes (feature + Stage-5
    row). Read-only; a queue error degrades to an empty list."""
    from paper_trader.alpha_agent import autonomous_research as ar
    out: list = []
    try:
        for job in queue.list_jobs(state=ar.STATE_COMPLETED, limit=limit):
            payload = getattr(job, "payload", None) or {}
            if payload.get("research_bridge_mandate_id") != mandate_id:
                continue
            spec = payload.get("spec") or {}
            result = getattr(job, "result", None) or {}
            row = result.get("row") or result.get("metrics") or result
            out.append({"job_id": getattr(job, "job_id", None), "spec": spec,
                        "feature": spec.get("feature"), "result": row})
    except Exception:  # noqa: BLE001 - collection is best-effort, never fatal
        return []
    return out


def _mandate_job_census(*, queue, mandate_id: str, limit: int = 1000) -> dict:
    """Honest attempt/state census over ALL bridge experiment jobs for this mandate
    (any lifecycle). ``claimed`` counts jobs whose ``attempts>=1`` — the auditable
    proof that the queue genuinely claimed and executed the work."""
    from paper_trader.alpha_agent import autonomous_research as ar
    census = {"generated": 0, "claimed": 0, "completed": 0, "data_hold": 0,
              "rejected": 0, "retryable": 0, "failed": 0, "queued": 0, "running": 0}
    state_key = {ar.STATE_COMPLETED: "completed", ar.STATE_BLOCKED_SPECIFIC: "data_hold",
                 ar.STATE_REJECTED: "rejected", ar.STATE_RETRYABLE: "retryable",
                 ar.STATE_FAILED_PERMANENT: "failed", ar.STATE_QUEUED: "queued",
                 ar.STATE_RUNNING: "running"}
    for st, key in state_key.items():
        try:
            jobs = queue.list_jobs(state=st, limit=limit)
        except Exception:  # noqa: BLE001
            continue
        for job in jobs:
            payload = getattr(job, "payload", None) or {}
            if payload.get("research_bridge_mandate_id") != mandate_id:
                continue
            census["generated"] += 1
            census[key] += 1
            if _as_int(getattr(job, "attempts", 0), 0) >= 1:
                census["claimed"] += 1
    return census


def ingest_mandate_experiments(*, registry, cfg9: dict, completed: list,
                               evidence_date: Optional[str] = None) -> dict:
    """Idempotently record the canonical Stage-5 evidence from the executed bridge
    experiments into the tournament CandidateRegistry via the CANONICAL ingester
    (``alpha_agent.tournament.ingest_completed_experiments``). No bridge-local
    ingestion path is invented."""
    from paper_trader.alpha_agent import tournament as trn
    if registry is None or not completed:
        return {"imported": 0, "skipped": 0, "malformed": 0}
    return trn.ingest_completed_experiments(
        registry, cfg9 or {}, completed=completed,
        source="research_bridge_campaign", evidence_date=evidence_date)


def _fresh_candidate_rows(*, registry, candidate_ids: list) -> list:
    """Bridge gate rows for EXACTLY the candidates this mandate revalidated, read
    from the tournament registry's fresh ``latest_evaluation`` (the canonical
    per-candidate evidence just ingested)."""
    rows: list = []
    if registry is None:
        return rows
    by_id = {c.get("candidate_id"): c for c in (registry.list() or [])}
    for cid in candidate_ids or []:
        cand = by_id.get(cid) or {}
        ev = registry.latest_evaluation(cid) or {}
        metrics = ev.get("metrics") or {}
        exp_ids = cand.get("experiment_ids") or []
        rows.append({
            "candidate_id": cid,
            "family": cand.get("family"),
            "horizon": metrics.get("horizon_days") or metrics.get("horizon"),
            "metrics": metrics,
            "canonical_target_state": cand.get("lifecycle_state"),
            "experiment_id": (exp_ids[-1] if exp_ids else None),
            "datasets": cand.get("data_dependencies"),
            "sample_window": [metrics.get("data_boundaries")]
            if metrics.get("data_boundaries") else None,
            "rank_ic": metrics.get("rank_ic"),
            "rank_ic_t": metrics.get("rank_ic_t"),
            "n_periods": metrics.get("scored_periods"),
            "net25_spread": metrics.get("net25_spread"),
            "turnover": metrics.get("turnover_per_rebalance"),
            "max_drawdown_pct": metrics.get("max_drawdown_pct"),
            "subperiod_consistency": metrics.get("subperiod_consistency"),
            "regime_consistency": metrics.get("regime_consistency"),
            "champion_correlation": metrics.get("corr_vs_champion"),
            "incremental_net25_spread": metrics.get("incremental_net_return"),
            "forward_observation_count": metrics.get("forward_observation_count") or 0,
            "result_hash": ev.get("evaluated_at"),
        })
    return rows


def run_mandate_campaign(*, mandate: dict, active_book_id: str,
                         eligible_market_date: Optional[str] = None, stack=None,
                         max_experiments: Optional[int] = None) -> dict:
    """Operator-run bounded end-to-end campaign for a dispatchable mandate:
    generate -> claim -> execute -> settle -> ingest -> fresh candidate rows.

    EVERY step reuses a canonical ``alpha_agent`` owner. Returns TRUE per-mandate
    counts (never ``len(all pre-existing registry rows)``). Read-only w.r.t.
    operational ledgers (the caller fingerprints before/after and asserts
    unchanged). The returned ``candidate_rows`` feed the ONE canonical Alpha
    Strength Gate via :func:`ingest_result`.
    """
    st = stack or _open_production_stack()
    queue, handlers = st["queue"], st["handlers"]
    registry, cfg9 = st["registry"], st["cfg9"]
    mid = (mandate or {}).get("mandate_id")
    evd = eligible_market_date or (mandate or {}).get("eligible_market_date")

    prior_candidates = len(registry.list() or []) if registry is not None else 0
    generated = generate_mandate_experiments(
        mandate=mandate, registry=registry, queue=queue,
        max_experiments=max_experiments)
    drain = drain_mandate_experiments(queue=queue, handlers=handlers, mandate=mandate)
    completed = _completed_mandate_jobs(queue=queue, mandate_id=mid)
    ingest = ingest_mandate_experiments(
        registry=registry, cfg9=cfg9, completed=completed, evidence_date=evd)
    census = _mandate_job_census(queue=queue, mandate_id=mid)

    cand_ids = sorted({(c.get("spec") or {}).get("revalidation_of")
                       for c in completed
                       if (c.get("spec") or {}).get("revalidation_of")})
    rows = _fresh_candidate_rows(registry=registry, candidate_ids=cand_ids)

    campaign = {
        "entrypoint": "api.research_bridge.run_mandate_campaign -> alpha_agent "
                      "(generate_mandate_experiments seed + ResearchQueue atomic "
                      "claim + build_production_autonomy_handlers CAT_EXPERIMENT on "
                      "owned Norgate panel + tournament.ingest_completed_experiments)"
                      " [owned data only; scheduled cadence never admits this origin]",
        "campaign_id": "rbc_%s_%s" % (active_book_id, evd),
        "operator_run_bounded_drain": True,
        "scheduled_cadence_admits_bridge_origin": False,
        "prior_candidates_considered": prior_candidates,
        "experiments_generated_for_mandate": census["generated"],
        "experiments_generated_this_run": len(generated),
        "jobs_claimed": census["claimed"],
        "experiments_executed": census["claimed"],
        "experiments_completed": census["completed"],
        "experiments_data_hold": census["data_hold"],
        "experiments_rejected": census["rejected"],
        "canonical_results_ingested": _as_int(ingest.get("imported"), 0)
        + _as_int(ingest.get("skipped"), 0),
        "generations": 1,
        "budget_seconds": (mandate.get("bounds") or {}).get("compute_budget_seconds"),
        "drain_report": drain,
        "ingest_report": ingest,
        "job_census": census,
    }
    return {"stack": st, "generated": generated, "drain": drain, "completed": completed,
            "ingest": ingest, "census": census, "candidate_ids": list(cand_ids),
            "candidate_rows": rows, "campaign": campaign}


# --------------------------------------------------------------------------- #
# 4) ingest evidence -> alpha strength gate -> envelope -> reassessment
# --------------------------------------------------------------------------- #
def _default_classify_fn():
    from paper_trader.alpha_agent import tournament as trn
    return trn.classify_evidence


def _fdr_qvalues(candidate_rows: list) -> dict:
    """Benjamini-Hochberg q-values keyed by candidate_id (reuse selection_controls)."""
    from paper_trader.alpha_agent import selection_controls as sc
    from paper_trader.alpha_agent import signal_evaluation as se
    ids, pvals = [], []
    for c in candidate_rows or []:
        cid = c.get("candidate_id")
        p = c.get("pvalue")
        if p is None:
            t = c.get("rank_ic_t")
            n = c.get("n_periods") or c.get("effective_sample")
            if t is not None and n:
                try:
                    p = se.approx_two_sided_pvalue(float(t), max(1, int(n) - 1))
                except Exception:
                    p = None
        if p is not None:
            ids.append(cid)
            pvals.append(float(p))
    if not pvals:
        return {}
    try:
        q = sc.benjamini_hochberg(pvals)
    except Exception:
        return {}
    return {cid: qv for cid, qv in zip(ids, q)}


def ingest_result(*, mandate: dict, candidate_rows: list, campaign: dict,
                  active_book_id: str, classify_fn: Optional[Callable] = None,
                  gate_cfg: Optional[dict] = None, ledger_fingerprint: Optional[dict] = None,
                  data_gaps: Optional[list] = None, bridge_dir=None,
                  now: Optional[datetime] = None) -> dict:
    """Ingest returned canonical evidence idempotently: run the ONE Alpha Strength Gate,
    build the immutable envelope, produce the re-assessment, persist all.

    ``candidate_rows`` reference the canonical tournament/experiment evidence (each with a
    ``candidate_id``, tournament contract ``metrics``, optional ``champion_correlation`` /
    ``incremental_net25_spread`` / ``rank_ic_t`` / ``n_periods``). Idempotent: a second
    ingest with identical evidence yields the same envelope hash and does not duplicate.
    """
    classify = classify_fn or _default_classify_fn()
    cfg = gate_cfg if gate_cfg is not None else load_gate_cfg()
    fdr = _fdr_qvalues(candidate_rows)

    alpha = kernel.evaluate_alpha_strength(candidates=candidate_rows, classify_fn=classify,
                                           gate_cfg=cfg, fdr_qvalues=fdr)
    ref_rows = [{
        "candidate_id": c.get("candidate_id"),
        "experiment_id": c.get("experiment_id"),
        "family": c.get("family"),
        "horizon": c.get("horizon"),
        "datasets": c.get("datasets"),
        "sample_window": c.get("sample_window"),
        "rank_ic": c.get("rank_ic"),
        "rank_ic_t": c.get("rank_ic_t"),
        "net25_spread": c.get("net25_spread"),
        "turnover": c.get("turnover"),
        "max_drawdown_pct": c.get("max_drawdown_pct"),
        "subperiod_consistency": c.get("subperiod_consistency"),
        "regime_consistency": c.get("regime_consistency"),
        "champion_correlation": c.get("champion_correlation"),
        "result_hash": c.get("result_hash"),
    } for c in (candidate_rows or [])]

    envelope = kernel.build_result_envelope(
        mandate=mandate, alpha_strength=alpha, campaign=campaign,
        candidate_evidence=ref_rows, ledger_fingerprint=ledger_fingerprint,
        data_gaps=data_gaps)
    normalized_opp = (load_mandate_record(mandate_id=mandate.get("mandate_id"),
                                          bridge_dir=bridge_dir) or {}).get(
        "normalized_opportunity") or {}
    reassessment = kernel.reassess(normalized_opportunity=normalized_opp, envelope=envelope)

    envelope["envelope_hash"] = kernel.stable_hash(envelope)
    reassessment["reassessment_hash"] = kernel.stable_hash(reassessment)
    envelope_id = "rbe_%s_%s" % (mandate.get("mandate_id", "m"),
                                 (envelope["envelope_hash"] or "")[:12])

    payload = {
        "record_type": "envelope",
        "envelope_id": envelope_id,
        "composition_owner": COMPOSITION_OWNER,
        "generated_at": _now_iso(now),
        "active_book_id": active_book_id,
        "envelope": envelope,
        "reassessment": reassessment,
    }
    _atomic_write_json(_envelopes_dir(bridge_dir) / ("%s.json" % envelope_id), payload)
    _update_mandate_record(
        mandate.get("mandate_id"),
        {"envelope_id": envelope_id, "reassessment": reassessment,
         "lifecycle_state": envelope.get("mandate_lifecycle_state")},
        bridge_dir=bridge_dir, active_book_id=active_book_id)
    return {"status": "INGESTED", "envelope_id": envelope_id, "envelope": envelope,
            "reassessment": reassessment, "outcome": alpha.get("outcome"),
            "verdict": reassessment.get("verdict")}


def load_envelope(*, envelope_id: str, bridge_dir=None) -> Optional[dict]:
    return _load_json(_envelopes_dir(bridge_dir) / ("%s.json" % envelope_id))


# --------------------------------------------------------------------------- #
# 5) read contract for GET /v1/research/research-bridge (read-only)
# --------------------------------------------------------------------------- #
def _safety() -> dict:
    return kernel._safety()


def load_research_bridge(*, active_book_id: Optional[str] = None,
                         eligible_market_date: Optional[str] = None,
                         bridge_dir=None, now: Optional[datetime] = None) -> dict:
    """Read-only read model for the Research UI. Never runs the engine, never dispatches.

    Resolves the latest persisted mandate for (book, eligible date); if not given, uses the
    most recently generated mandate in the index. Returns ``NOT_RUN`` before any mandate
    exists.
    """
    base = {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": kernel.CALCULATION_OWNER,
        "generated_at": _now_iso(now),
        "state_vocabulary": list(READ_STATE_VOCAB),
        "research_only": True,
        "safety": _safety(),
        "governance": {
            "champion_changed": False,
            "automatic_model_promotion_allowed": False,
            "automatic_recalibration_allowed": False,
            "manual_model_review_required": True,
            "gate_owner": "alpha_agent.tournament.classify_evidence",
            "queue_owner": "alpha_agent.autonomous_research.ResearchQueue",
            "opportunity_owner": "engine.research_agent",
        },
    }
    index = _load_json(_index_path(bridge_dir)) or {}
    if not index:
        base.update({"state": STATE_NOT_RUN,
                     "message": "No research mandate has been dispatched yet."})
        return base

    entry = None
    if active_book_id and eligible_market_date:
        entry = index.get(_index_key(active_book_id, eligible_market_date))
    if entry is None:
        # most recently generated mandate across the index
        entries = [e for e in index.values() if isinstance(e, dict)]
        entry = max(entries, key=lambda e: e.get("generated_at") or "", default=None)
    if entry is None:
        base.update({"state": STATE_NOT_RUN, "message": "No mandate index entry found."})
        return base

    rec = _load_json(Path(entry["path"])) if entry.get("path") else \
        load_mandate_record(mandate_id=entry.get("mandate_id"), bridge_dir=bridge_dir)
    if not rec:
        base.update({"state": STATE_UNAVAILABLE,
                     "message": "Mandate index entry has no readable artifact."})
        return base

    mandate = rec.get("mandate") or {}
    dispatch = rec.get("dispatch")
    envelope_id = rec.get("envelope_id")
    envelope_rec = load_envelope(envelope_id=envelope_id, bridge_dir=bridge_dir) if envelope_id else None
    envelope = (envelope_rec or {}).get("envelope") if envelope_rec else None
    reassessment = rec.get("reassessment")
    alpha = (envelope or {}).get("alpha_strength") or {}

    opp = rec.get("normalized_opportunity") or {}
    base.update({
        "state": rec.get("lifecycle_state") or kernel.MANDATE_DRAFTED,
        "eligible_market_date": mandate.get("eligible_market_date"),
        "active_book_id": rec.get("active_book_id"),
        "champion": mandate.get("champion") or {},
        "research_opportunity": {
            "opportunity_id": opp.get("opportunity_id"),
            "category": opp.get("category"),
            "degradation_category": opp.get("degradation_category"),
            "affected_family": opp.get("affected_family"),
            "reason": opp.get("reason"),
            "hypothesis": opp.get("hypothesis"),
            "priority": opp.get("priority"),
            "supporting_evidence": opp.get("supporting_evidence"),
            "evidence_gaps": opp.get("evidence_gaps"),
        },
        "mandate": {
            "mandate_id": mandate.get("mandate_id"),
            "mandate_version": mandate.get("mandate_version"),
            "lifecycle_state": rec.get("lifecycle_state"),
            "allowed_hypothesis_families": mandate.get("allowed_hypothesis_families"),
            "allowed_data_families": mandate.get("allowed_data_families"),
            "horizons": mandate.get("horizons"),
            "bounds": mandate.get("bounds"),
            "requirements": mandate.get("requirements"),
            "dispatch_identity": mandate.get("dispatch_identity"),
            "gate_reference": mandate.get("gate_reference"),
        },
        "dispatch": dispatch,
        "alpha_discovery": {
            "outcome": alpha.get("outcome"),
            "outcome_vocabulary": alpha.get("outcome_vocabulary"),
            "counts": alpha.get("counts"),
            "fdr_alpha": alpha.get("fdr_alpha"),
            "best_challenger": alpha.get("best_challenger"),
            "no_defensible_alpha": alpha.get("no_defensible_alpha"),
            "data_hold": alpha.get("data_hold"),
            "requires_manual_model_review": alpha.get("requires_manual_model_review"),
        } if envelope else None,
        "evidence": {
            "campaign": (envelope or {}).get("campaign"),
            "candidate_evidence": (envelope or {}).get("candidate_evidence"),
            "data_gaps": (envelope or {}).get("data_gaps"),
            "operational_ledger_fingerprint": (envelope or {}).get(
                "operational_ledger_fingerprint"),
        } if envelope else None,
        "reassessment": reassessment,
        "envelope_id": envelope_id,
    })
    return base


def compact_summary(*, active_book_id: Optional[str] = None,
                    eligible_market_date: Optional[str] = None, bridge_dir=None) -> dict:
    """Compact summary for the DRC manifest / workflow state (read-only)."""
    payload = load_research_bridge(active_book_id=active_book_id,
                                   eligible_market_date=eligible_market_date,
                                   bridge_dir=bridge_dir)
    return {
        "research_bridge_state": payload.get("state"),
        "research_bridge_mandate_id": (payload.get("mandate") or {}).get("mandate_id"),
        "research_bridge_outcome": (payload.get("alpha_discovery") or {}).get("outcome")
        if payload.get("alpha_discovery") else None,
        "research_bridge_verdict": (payload.get("reassessment") or {}).get("verdict")
        if payload.get("reassessment") else None,
        "champion_changed": False,
        "manual_model_review_required": True,
    }


__all__ = [
    "SCHEMA_VERSION", "COMPOSITION_OWNER", "PHASE", "READ_STATE_VOCAB", "BRIDGE_DIR_ENV",
    "load_gate_cfg", "select_opportunity", "read_latest_assessment",
    "build_and_persist_mandate", "load_mandate_record", "load_latest_mandate_record",
    "dispatch_mandate", "recover_stale_dispatch", "ingest_result", "load_envelope",
    "load_research_bridge", "compact_summary",
    "BRIDGE_ORIGIN", "BRIDGE_EXPERIMENT_STRATEGY", "BRIDGE_EXPERIMENT_LANE",
    "fingerprint_operational_ledgers", "generate_mandate_experiments",
    "drain_mandate_experiments", "ingest_mandate_experiments", "run_mandate_campaign",
]
