r"""Phase 29J Slice 9 — Data Expansion / Purchase-Gate composition, persistence & read owner.

This is the ONE canonical orchestration / catalog / immutable-artifact / read-contract owner
for the Data Expansion purchase/integration gate (Consolidation Roadmap Slice 9 / Charter
Milestone 5). It performs NO dataset-evaluation of its own — the single canonical calculation
lives in ``engine.data_expansion_gate``. This module only:

  1. LOADS the canonical dataset catalog (external-dataset candidates + provider metadata +
     licensing/cost metadata) and the MEASURED research evidence that the existing
     experiment/evidence owners produced. It reuses — never forks — the existing data /
     provider owners: ``alpha_agent.source_contracts`` (normalized-record provenance),
     ``api.data_freshness`` (freshness), ``alpha_agent.experiment_contracts`` (evidence
     gates), the Stage 13A analyst-revisions purchase-gate framework
     (``alpha_agent.analyst_revisions``) for the analyst-revisions candidate, and the Slice 8
     ``engine.research_agent`` DATA research opportunities.
  2. COMPOSES the immutable dataset-evaluation input contract and RUNS the pure kernel.
  3. PERSISTS a completed evaluation as an immutable artifact under a dedicated research root
     (atomic write, index/manifest, idempotent identical rerun; an evaluation from DIFFERENT
     dataset metadata / evidence / policy SUPERSEDES — never silently reuses — the stale
     evaluation, keeping every artifact immutable on disk). It NEVER purchases a dataset,
     subscribes to / activates a provider, calls a paid provider, uses a paid API quota,
     alters credentials, integrates a dataset, writes an operational ledger, PostgreSQL, a
     champion pointer, an order, a fill, a holding, cash or NAV, and enables no cadence.
  4. Exposes ONE read contract for ``GET /v1/research/data-expansion`` (catalog + latest
     evaluations) and ``GET /v1/research/data-expansion/{dataset_id}`` (detail), plus a
     compact summary the Daily Research Cycle / workflow state may READ.

Cadence is DISABLED: a full purchase-gate evaluation is NOT a daily job — it runs only when a
candidate is added, candidate metadata changes, enough new research evidence exists, a formal
review checkpoint is due, or an operator explicitly requests a re-evaluation. There is
deliberately NO create / purchase / subscribe / activate / integrate endpoint. This module is
research-only, preview-first, paper-only, manual-review: it recommends dataset acquisition /
integration but takes no such action, and enables no automation.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from paper_trader.engine import data_expansion_gate as kernel

# Re-export the frozen vocabularies / versions so callers use one source.
SCHEMA_VERSION = kernel.SCHEMA_VERSION
INPUT_SCHEMA_VERSION = kernel.INPUT_SCHEMA_VERSION
POLICY_VERSION = kernel.POLICY_VERSION
RECOMMENDATION_VOCAB = kernel.RECOMMENDATION_VOCAB
COMPOSITION_OWNER = "api.data_expansion"
PHASE = "29J-Slice9"

# --- decision context — re-exported so no caller invents its own spelling ----------- #
# Stage A ("is it worth paying to learn?") and Stage B ("did it earn continued purchase?")
# are both answered by the ONE canonical kernel. The default is Stage B, which is the
# historical behaviour of every existing caller.
CONTEXT_RESEARCH_ACQUISITION = kernel.CONTEXT_RESEARCH_ACQUISITION
CONTEXT_POST_ACQUISITION_VALUE = kernel.CONTEXT_POST_ACQUISITION_VALUE
DECISION_CONTEXT_VOCAB = kernel.DECISION_CONTEXT_VOCAB
DEFAULT_DECISION_CONTEXT = kernel.DEFAULT_DECISION_CONTEXT
ACQUISITION_RECOMMENDATION_VOCAB = kernel.ACQUISITION_RECOMMENDATION_VOCAB
DECISION_STATE_VOCAB = kernel.DECISION_STATE_VOCAB

# Read-layer states extend the kernel's recommendation vocabularies (both contexts).
STATE_NOT_RUN = "NOT_RUN"
STATE_OK = "OK"
STATE_EMPTY_CATALOG = "EMPTY_CATALOG"
STATE_UNAVAILABLE = "UNAVAILABLE"
STATE_NOT_FOUND = "NOT_FOUND"
READ_STATE_VOCAB = tuple(list(DECISION_STATE_VOCAB) + [STATE_NOT_RUN, STATE_OK,
                                                       STATE_EMPTY_CATALOG, STATE_UNAVAILABLE,
                                                       STATE_NOT_FOUND])

# Cadence is DISABLED for Slice 9 — a full purchase-gate evaluation is never a daily job.
CADENCE_ENABLED = False

# --- immutable artifact root (configurable; a research / decision-evidence root, ------ #
# NEVER the operational ledger root). ----------------------------------------------- #
DATA_EXPANSION_DIR_ENV = "PAPER_TRADER_DATA_EXPANSION_DIR"
_DEFAULT_DATA_EXPANSION_DIR = Path(r"D:\Stock_Prediction_app_data\data_expansion")
_ARTIFACTS_SUBDIR = "artifacts"
_INDEX_FILE = "index.json"

# --- dataset catalog (configurable; an embedded research-only default seed) ---------- #
DATA_EXPANSION_CATALOG_ENV = "PAPER_TRADER_DATA_EXPANSION_CATALOG"


# --------------------------------------------------------------------------- #
# Time / io helpers
# --------------------------------------------------------------------------- #
def _now(now: Optional[datetime]) -> datetime:
    return now or datetime.now(timezone.utc)


def _now_iso(now: Optional[datetime]) -> str:
    return _now(now).astimezone(timezone.utc).isoformat()


def _root_dir(data_expansion_dir=None) -> Path:
    if data_expansion_dir is not None:
        return Path(data_expansion_dir)
    env = os.environ.get(DATA_EXPANSION_DIR_ENV)
    return Path(env) if env else _DEFAULT_DATA_EXPANSION_DIR


def _artifacts_dir(data_expansion_dir=None) -> Path:
    return _root_dir(data_expansion_dir) / _ARTIFACTS_SUBDIR


def _index_path(data_expansion_dir=None) -> Path:
    return _root_dir(data_expansion_dir) / _INDEX_FILE


def _atomic_write_json(path: Path, payload: dict) -> None:
    """Atomic write: a temp file in the same dir then ``os.replace`` (an interrupted write
    leaves the prior file intact and only an orphan .tmp, never a partial)."""
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


def _load_json(path: Path) -> Optional[Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


# --------------------------------------------------------------------------- #
# Policy (reuse authoritative evidence gates where an owner already exists)
# --------------------------------------------------------------------------- #
def resolve_policy(policy_overrides: Optional[dict] = None) -> dict:
    """The single explicit gate policy. The genuinely-new Slice-9 lift/redundancy/cost floors
    live in ``engine.data_expansion_gate.default_policy``; where an authoritative
    research-evidence gate already has an owner it MAY be injected here (never forked)."""
    pol = dict(kernel.default_policy())
    if policy_overrides:
        pol.update(policy_overrides)
    return pol


# --------------------------------------------------------------------------- #
# Canonical dataset catalog contract + embedded research-only default seed.
# --------------------------------------------------------------------------- #
# The catalog is METADATA ONLY (candidate descriptions). It contains NO secrets, NO provider
# credentials, and NO invented costs — an unknown cost is left explicitly unknown so the gate
# raises an UNKNOWN_COST gap rather than a fabricated number. Every seed candidate carries
# ``evidence.available = false`` (no paid trial has run), so the honest default gate result is
# INSUFFICIENT_EVIDENCE — never a fabricated purchase recommendation.
CATALOG_FIELDS = (
    "dataset_id", "provider", "dataset_name", "data_category", "feature_families",
    "dataset", "research_requirements", "evidence", "existing_ownership",
    "provenance", "notes",
)

DEFAULT_CATALOG: list[dict] = [
    {
        "dataset_id": "analyst_estimate_revisions_history",
        "provider": "external_analyst_revisions_vendor",
        "dataset_name": "Historical analyst estimate revisions (point-in-time)",
        "data_category": "analyst_revisions",
        "feature_families": ["eps_revision_magnitude", "revenue_revision_magnitude",
                             "up_minus_down_breadth", "revision_acceleration",
                             "estimate_dispersion_change"],
        "dataset": {
            "pit_guarantee": None,
            "publication_timestamps": None,
            "history_years": None,
            "inactive_delisted_support": None,
            "survivorship_bias_risk": None,
            "revision_history_support": True,
            "estimates_are_current_consensus": None,
            "restatement_backfill_behavior": None,
            "universe_size": None,
            "universe_coverage_ratio": None,
            "identifier_quality": "MODERATE",
            "identifier_mapping_available": True,
            "update_frequency": "DAILY",
            "operational_reliability": None,
            "implementation_complexity": "MEDIUM",
            "licensing": {"research_use_allowed": None, "prohibited": None,
                          "redistribution_allowed": None, "internal_commercial_use_allowed": None,
                          "derived_data_allowed": None, "model_training_allowed": None,
                          "notes": "Trial not started; licensing to be quoted (Stage 13A)."},
            "cost": {"annual_usd": None, "one_time_usd": None, "cost_known": False,
                     "notes": "Trial not started; price quote pending."},
            "existing_entitlement_state": "NONE",
            "acquisition_state": "NOT_ACQUIRED",
            "technical_integration_state": "NOT_INTEGRATED",
        },
        "research_requirements": {
            "requires_point_in_time": True,
            "requires_historical_revisions": True,
            "requires_full_universe_integrity": True,
            "min_history_years": 15,
            "min_universe_size": 800,
            "min_universe_coverage": 0.60,
            "min_effective_sample": 24,
            "research_intent": "Test whether historical analyst-revision breadth/magnitude "
                               "adds economically distinct, cost-adjusted alpha beyond owned "
                               "price + fundamental factors.",
        },
        "evidence": {"available": False,
                     "study_reference": "alpha_agent.analyst_revisions (Stage 13A) — "
                                        "TRIAL_NOT_STARTED",
                     "evidence_owner": "alpha_agent.experiment_contracts"},
        "existing_ownership": {"entitlement_state": "NONE"},
        "provenance": "Candidate framework owned by alpha_agent.analyst_revisions (Stage 13A); "
                      "Slice 9 evaluates the acquisition/integration decision without purchasing.",
        "notes": "Milestone-5 analyst-revisions candidate. No paid trial has run; the gate "
                 "returns INSUFFICIENT_EVIDENCE until a local trial extract supplies measured "
                 "out-of-sample evidence.",
    },
    {
        "dataset_id": "historical_pit_fundamentals_vendor",
        "provider": "external_fundamentals_vendor",
        "dataset_name": "Point-in-time historical fundamentals with delisted coverage",
        "data_category": "fundamentals",
        "feature_families": ["quality", "accruals", "profitability", "growth"],
        "dataset": {
            "pit_guarantee": "EFFECTIVE_DATED",
            "publication_timestamps": True,
            "history_years": None,
            "inactive_delisted_support": True,
            "survivorship_bias_risk": "LOW",
            "revision_history_support": True,
            "restatement_backfill_behavior": "PIT_PRESERVED",
            "universe_size": None,
            "universe_coverage_ratio": None,
            "identifier_quality": "STRONG",
            "identifier_mapping_available": True,
            "update_frequency": "DAILY",
            "operational_reliability": "HIGH",
            "implementation_complexity": "MEDIUM",
            "licensing": {"research_use_allowed": None, "prohibited": None,
                          "notes": "To be quoted."},
            "cost": {"annual_usd": None, "cost_known": False, "notes": "To be quoted."},
            "existing_entitlement_state": "NONE",
            "acquisition_state": "NOT_ACQUIRED",
            "technical_integration_state": "NOT_INTEGRATED",
        },
        "research_requirements": {
            "requires_point_in_time": True,
            "requires_full_universe_integrity": True,
            "min_history_years": 15,
            "min_universe_size": 800,
            "min_universe_coverage": 0.60,
            "min_effective_sample": 24,
            "research_intent": "Replace survivor-biased owned EODHD fundamentals snapshot with "
                               "PIT vendor fundamentals; must beat owned SEC companyfacts.",
        },
        "evidence": {"available": False,
                     "study_reference": "No measured trial; owned SEC companyfacts + "
                                        "pit_fundamentals remain the baseline.",
                     "evidence_owner": "alpha_agent.experiment_contracts"},
        "existing_ownership": {"entitlement_state": "NONE",
                               "owned_baseline": "alpha_agent.pit_fundamentals / sec_companyfacts"},
        "provenance": "Owned fundamentals owners: alpha_agent.sec_companyfacts + "
                      "alpha_agent.pit_fundamentals (PIT) and collectors/eodhd (snapshot).",
        "notes": "Candidate to close the PIT / delisted-coverage gap in fundamentals.",
    },
    {
        "dataset_id": "historical_market_cap_history",
        "provider": "external_reference_vendor",
        "dataset_name": "Point-in-time market-capitalisation history",
        "data_category": "market_cap",
        "feature_families": ["size", "float_adjusted_market_cap"],
        "dataset": {
            "pit_guarantee": "EFFECTIVE_DATED",
            "publication_timestamps": True,
            "history_years": None,
            "inactive_delisted_support": True,
            "survivorship_bias_risk": "LOW",
            "revision_history_support": False,
            "restatement_backfill_behavior": "PIT_PRESERVED",
            "universe_size": None,
            "universe_coverage_ratio": None,
            "identifier_quality": "STRONG",
            "identifier_mapping_available": True,
            "update_frequency": "DAILY",
            "operational_reliability": "HIGH",
            "implementation_complexity": "LOW",
            "licensing": {"research_use_allowed": None, "prohibited": None,
                          "notes": "To be quoted."},
            "cost": {"annual_usd": None, "cost_known": False, "notes": "To be quoted."},
            "existing_entitlement_state": "NONE",
            "acquisition_state": "NOT_ACQUIRED",
            "technical_integration_state": "NOT_INTEGRATED",
        },
        "research_requirements": {
            "requires_point_in_time": True,
            "requires_full_universe_integrity": True,
            "min_history_years": 15,
            "min_universe_size": 800,
            "min_universe_coverage": 0.60,
            "min_effective_sample": 24,
            "research_intent": "Provide a dedicated PIT market-cap history (no owned owner "
                               "exists today — a genuine gap, not a duplication).",
        },
        "evidence": {"available": False,
                     "study_reference": "No owned PIT market-cap-history owner exists.",
                     "evidence_owner": "alpha_agent.experiment_contracts"},
        "existing_ownership": {"entitlement_state": "NONE",
                               "owned_baseline": "NONE — no dedicated PIT market-cap-history owner"},
        "provenance": "Genuine ownership gap identified by the Slice 9 ownership map.",
        "notes": "Low-complexity reference dataset; still requires measured incremental value.",
    },
]


def load_catalog(*, catalog: Optional[list] = None, catalog_path=None) -> list[dict]:
    """Load the canonical dataset catalog (metadata only).

    Precedence: an explicitly-injected ``catalog`` (tests) → an explicit ``catalog_path`` /
    the ``PAPER_TRADER_DATA_EXPANSION_CATALOG`` env JSON file → the embedded
    ``DEFAULT_CATALOG`` seed. Always degrade-safe (a malformed / missing file falls back to
    the embedded seed). Never reads a secret or a credential.
    """
    if catalog is not None:
        return [dict(c) for c in catalog]
    path = catalog_path or os.environ.get(DATA_EXPANSION_CATALOG_ENV)
    if path:
        loaded = _load_json(Path(path))
        entries = None
        if isinstance(loaded, list):
            entries = loaded
        elif isinstance(loaded, dict) and isinstance(loaded.get("datasets"), list):
            entries = loaded["datasets"]
        if entries is not None:
            return [dict(c) for c in entries]
    return [dict(c) for c in DEFAULT_CATALOG]


def _catalog_entry(dataset_id: str, *, catalog: Optional[list] = None,
                   catalog_path=None) -> Optional[dict]:
    for c in load_catalog(catalog=catalog, catalog_path=catalog_path):
        if c.get("dataset_id") == dataset_id:
            return c
    return None


# --------------------------------------------------------------------------- #
# Input-contract composition
# --------------------------------------------------------------------------- #
def build_input_contract(*, catalog_entry: dict,
                         evidence_override: Optional[dict] = None,
                         dataset_override: Optional[dict] = None,
                         requirements_override: Optional[dict] = None,
                         acquisition_case: Optional[dict] = None,
                         decision_context: Optional[str] = None) -> dict:
    """Assemble the immutable dataset-evaluation input contract from a catalog entry.

    Overrides let an operator / the test suite inject freshly-MEASURED research evidence (from
    the existing experiment/evidence owners) or corrected metadata without editing the catalog
    on disk. Nothing is re-derived here; measured evidence always comes from its owner.

    ``acquisition_case`` carries the Stage-A declarations the kernel refuses to invent — what
    capability the acquisition unlocks, how distinct it is expected to be from owned data,
    whether an owned substitute was tried and whether a bounded evaluation exists. Omitting it
    in the research-acquisition context does not produce a recommendation; it produces an
    explained INSUFFICIENT_EVIDENCE.
    """
    ce = catalog_entry or {}
    ctx = kernel.resolve_decision_context(input_contract=ce,
                                          decision_context=decision_context)
    acq = dict(ce.get("acquisition_case") or {})
    if acquisition_case is not None:
        acq = dict(acquisition_case)
    dataset = dict(ce.get("dataset") or {})
    if dataset_override:
        dataset.update(dataset_override)
    requirements = dict(ce.get("research_requirements") or {})
    if requirements_override:
        requirements.update(requirements_override)
    evidence = dict(ce.get("evidence") or {})
    if evidence_override is not None:
        evidence = dict(evidence_override)
    return {
        "schema_version": INPUT_SCHEMA_VERSION,
        "dataset_id": ce.get("dataset_id"),
        "provider": ce.get("provider"),
        "dataset_name": ce.get("dataset_name"),
        "data_category": ce.get("data_category"),
        "feature_families": list(ce.get("feature_families") or []),
        "dataset": dataset,
        "research_requirements": requirements,
        "evidence": evidence,
        "acquisition_case": acq,
        "decision_context": ctx,
        "existing_ownership": dict(ce.get("existing_ownership") or {}),
        "catalog_provenance": ce.get("provenance"),
    }


# --------------------------------------------------------------------------- #
# Run (build contract + kernel). Does NOT persist.
# --------------------------------------------------------------------------- #
def run_evaluation(*, dataset_id: Optional[str] = None,
                   catalog_entry: Optional[dict] = None,
                   input_contract: Optional[dict] = None,
                   evidence_override: Optional[dict] = None,
                   dataset_override: Optional[dict] = None,
                   requirements_override: Optional[dict] = None,
                   catalog: Optional[list] = None, catalog_path=None,
                   policy: Optional[dict] = None,
                   acquisition_case: Optional[dict] = None,
                   decision_context: Optional[str] = None) -> dict:
    """Build the dataset-evaluation contract (unless supplied) and run the pure kernel.

    Returns ``{"input_contract": ..., "evaluation": <kernel result>}``. Read-only: persists
    nothing, purchases nothing, acquires nothing, activates no provider, calls no paid API.
    ``decision_context`` selects Stage A or Stage B; omitting it keeps the historical
    post-acquisition semantics for every caller that has not asked for anything else.
    """
    pol = policy or resolve_policy()
    if input_contract is None:
        ce = catalog_entry
        if ce is None and dataset_id is not None:
            ce = _catalog_entry(dataset_id, catalog=catalog, catalog_path=catalog_path)
        if ce is None:
            raise ValueError("Unknown dataset_id and no catalog_entry / input_contract supplied.")
        input_contract = build_input_contract(
            catalog_entry=ce, evidence_override=evidence_override,
            dataset_override=dataset_override, requirements_override=requirements_override,
            acquisition_case=acquisition_case, decision_context=decision_context)
    evaluation = kernel.evaluate_dataset(input_contract=input_contract, policy=pol,
                                         decision_context=decision_context)
    return {"input_contract": input_contract, "evaluation": evaluation}


# --------------------------------------------------------------------------- #
# Persist (immutable artifact)
# --------------------------------------------------------------------------- #
def evaluation_identity(*, input_contract: dict, result: dict) -> dict:
    return {
        "dataset_id": input_contract.get("dataset_id"),
        "provider": input_contract.get("provider"),
        "data_category": input_contract.get("data_category"),
        "decision_context": result.get("decision_context") or DEFAULT_DECISION_CONTEXT,
        "policy_version": POLICY_VERSION,
        "input_schema_version": INPUT_SCHEMA_VERSION,
        "evaluation_hash": result.get("evaluation_hash"),
    }


def evaluation_id_for(identity: dict) -> str:
    ds = identity.get("dataset_id") or "dataset"
    h = (identity.get("evaluation_hash") or "")[:12]
    # The legacy (post-acquisition) id shape is unchanged so existing artifacts keep resolving;
    # a Stage-A evaluation is tagged so the two are never mistaken for one another on disk.
    if identity.get("decision_context") == CONTEXT_RESEARCH_ACQUISITION:
        return "dxev_acq_%s_%s" % (ds, h)
    return "dxev_%s_%s" % (ds, h)


def _index_key(dataset_id: Optional[str],
               decision_context: Optional[str] = None) -> str:
    """The index pointer for one dataset in one decision context.

    The default context keeps the bare ``dataset_id`` key it has always had, so no existing
    index entry is orphaned. A Stage-A evaluation gets its own key: the two stages answer
    different questions, and letting one supersede the other would silently destroy the
    answer to the question nobody re-ran.
    """
    key = str(dataset_id or "?")
    ctx = decision_context or DEFAULT_DECISION_CONTEXT
    if ctx == DEFAULT_DECISION_CONTEXT:
        return key
    return "%s::%s" % (key, ctx)


def _compact_input_contract(ic: dict) -> dict:
    return {
        "schema_version": INPUT_SCHEMA_VERSION,
        "dataset_id": ic.get("dataset_id"),
        "provider": ic.get("provider"),
        "dataset_name": ic.get("dataset_name"),
        "data_category": ic.get("data_category"),
        "feature_families": ic.get("feature_families"),
        "dataset": ic.get("dataset"),
        "research_requirements": ic.get("research_requirements"),
        "evidence": ic.get("evidence"),
        "acquisition_case": ic.get("acquisition_case"),
        "decision_context": ic.get("decision_context") or DEFAULT_DECISION_CONTEXT,
        "existing_ownership": ic.get("existing_ownership"),
        "catalog_provenance": ic.get("catalog_provenance"),
        "policy_version": POLICY_VERSION,
    }


def persist_evaluation(*, result: dict, input_contract: dict, data_expansion_dir=None,
                       now: Optional[datetime] = None) -> dict:
    """Persist a completed evaluation as an immutable artifact.

    Idempotency contract:
      * identical inputs (same ``evaluation_hash``) reuse the existing artifact;
      * DIFFERENT dataset metadata / evidence / policy for the same dataset SUPERSEDES — the
        fresh evaluation is written to its own immutable artifact and the index pointer
        advanced; the stale evaluation is NEVER silently reused. Every artifact on disk is
        immutable (the artifact id embeds the evaluation hash).

    Every recommendation state is persistable (INSUFFICIENT_EVIDENCE is a meaningful, durable
    result). A missing dataset_id is not persistable (nothing durable to key on).
    """
    dataset_id = input_contract.get("dataset_id")
    if not dataset_id or not result.get("evaluation_hash"):
        return {"status": "NOT_PERSISTED", "reason": "MISSING_DATASET_IDENTITY",
                "evaluation_id": None, "persisted": False, "reused": False,
                "superseded": False}

    identity = evaluation_identity(input_contract=input_contract, result=result)
    eid = evaluation_id_for(identity)
    key = _index_key(dataset_id, identity.get("decision_context"))
    index = _load_json(_index_path(data_expansion_dir)) or {}
    if not isinstance(index, dict):
        index = {}
    existing = index.get(key)

    if existing and existing.get("evaluation_hash") == identity["evaluation_hash"]:
        return {"status": "REUSED_EXISTING", "evaluation_id": existing.get("evaluation_id"),
                "path": existing.get("path"), "persisted": True, "reused": True,
                "superseded": False, "identity": identity}

    superseded_id = existing.get("evaluation_id") if existing else None

    payload = {
        "evaluation_id": eid,
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "generated_at": _now_iso(now),
        "identity": identity,
        "supersedes_evaluation_id": superseded_id,
        "input_contract": _compact_input_contract(input_contract),
        "evaluation": result,
    }
    path = _artifacts_dir(data_expansion_dir) / ("%s.json" % eid)
    _atomic_write_json(path, payload)
    index[key] = {"evaluation_id": eid, "path": str(path),
                  "evaluation_hash": identity["evaluation_hash"],
                  "recommendation_state": result.get("recommendation_state"),
                  "decision_context": identity["decision_context"],
                  "dataset_id": dataset_id, "provider": identity["provider"],
                  "data_category": identity["data_category"],
                  "generated_at": payload["generated_at"]}
    _atomic_write_json(_index_path(data_expansion_dir), index)
    return {"status": ("SUPERSEDED_PRIOR" if superseded_id else "CREATED"),
            "evaluation_id": eid, "path": str(path), "persisted": True, "reused": False,
            "superseded": bool(superseded_id), "superseded_evaluation_id": superseded_id,
            "identity": identity}


def load_latest_evaluation(*, dataset_id: Optional[str], data_expansion_dir=None,
                           decision_context: Optional[str] = None) -> Optional[dict]:
    """Load the current persisted evaluation artifact for a dataset (or None).

    Defaults to the post-acquisition context, which is the key shape every existing artifact
    was written under."""
    index = _load_json(_index_path(data_expansion_dir)) or {}
    if not isinstance(index, dict):
        return None
    entry = index.get(_index_key(dataset_id, decision_context))
    if not entry:
        return None
    art = _load_json(Path(entry.get("path"))) if entry.get("path") else None
    if art is None and entry.get("evaluation_id"):
        art = _load_json(_artifacts_dir(data_expansion_dir)
                         / ("%s.json" % entry.get("evaluation_id")))
    return art


def run_and_persist(*, dataset_id: Optional[str] = None,
                    catalog_entry: Optional[dict] = None,
                    input_contract: Optional[dict] = None,
                    evidence_override: Optional[dict] = None,
                    dataset_override: Optional[dict] = None,
                    requirements_override: Optional[dict] = None,
                    catalog: Optional[list] = None, catalog_path=None,
                    policy: Optional[dict] = None,
                    acquisition_case: Optional[dict] = None,
                    decision_context: Optional[str] = None,
                    data_expansion_dir=None, now: Optional[datetime] = None) -> dict:
    """Operator / checkpoint entry: build -> kernel -> persist (idempotent, supersede-safe).

    This is NOT a daily job (cadence disabled). It is invoked only when a candidate is added,
    candidate metadata changes, enough new research evidence exists, a formal review checkpoint
    is due, or an operator explicitly requests a re-evaluation.
    """
    run = run_evaluation(
        dataset_id=dataset_id, catalog_entry=catalog_entry, input_contract=input_contract,
        evidence_override=evidence_override, dataset_override=dataset_override,
        requirements_override=requirements_override, catalog=catalog, catalog_path=catalog_path,
        policy=policy, acquisition_case=acquisition_case,
        decision_context=decision_context)
    persist = persist_evaluation(result=run["evaluation"], input_contract=run["input_contract"],
                                 data_expansion_dir=data_expansion_dir, now=now)
    return {"input_contract": run["input_contract"], "evaluation": run["evaluation"],
            "persistence": persist}


# --------------------------------------------------------------------------- #
# Read contract — GET /v1/research/data-expansion
# --------------------------------------------------------------------------- #
def _metadata_summary(ce: dict) -> dict:
    ds = dict(ce.get("dataset") or {})
    lic = dict(ds.get("licensing") or {})
    cost = dict(ds.get("cost") or {})
    return {
        "pit_guarantee": ds.get("pit_guarantee"),
        "history_years": ds.get("history_years"),
        "inactive_delisted_support": ds.get("inactive_delisted_support"),
        "survivorship_bias_risk": ds.get("survivorship_bias_risk"),
        "revision_history_support": ds.get("revision_history_support"),
        "universe_size": ds.get("universe_size"),
        "universe_coverage_ratio": ds.get("universe_coverage_ratio"),
        "update_frequency": ds.get("update_frequency"),
        "identifier_quality": ds.get("identifier_quality"),
        "licensing_research_use_allowed": lic.get("research_use_allowed"),
        "licensing_prohibited": lic.get("prohibited"),
        "cost_annual_usd": cost.get("annual_usd"),
        "cost_known": cost.get("cost_known"),
        "existing_entitlement_state": ds.get("existing_entitlement_state"),
        "technical_integration_state": ds.get("technical_integration_state"),
    }


def _evaluation_view(art: Optional[dict]) -> dict:
    """A read-only projection of a persisted evaluation artifact (or a NOT_RUN stub)."""
    if not art:
        return {"state": STATE_NOT_RUN, "recommendation_state": STATE_NOT_RUN,
                "recommendation": None, "evaluation_id": None, "evaluation_hash": None,
                "decision_context": None, "blockers": [], "gaps": [], "generated_at": None}
    ev = art.get("evaluation") or {}
    rec = ev.get("recommendation") or {}
    return {
        "state": ev.get("recommendation_state"),
        "recommendation_state": ev.get("recommendation_state"),
        "decision_context": ev.get("decision_context") or DEFAULT_DECISION_CONTEXT,
        "decision_context_question": ev.get("decision_context_question"),
        "measured_lift_required": ev.get("measured_lift_required"),
        "acquisition_case": ev.get("acquisition_case"),
        "recommendation": {"state": rec.get("state"), "headline": rec.get("headline"),
                           "manual_approval_required": rec.get("manual_approval_required"),
                           "is_research_acquisition_recommendation":
                               rec.get("is_research_acquisition_recommendation"),
                           "is_purchase_recommendation":
                               rec.get("is_purchase_recommendation"),
                           "reason_codes": rec.get("reason_codes") or []},
        "dimension_summary": ev.get("dimension_summary") or {},
        "failed_dimensions": ev.get("failed_dimensions") or [],
        "watch_dimensions": ev.get("watch_dimensions") or [],
        "unknown_dimensions": ev.get("unknown_dimensions") or [],
        "disqualifying_blockers": ev.get("disqualifying_blockers") or [],
        "evidence_blockers": ev.get("evidence_blockers") or [],
        "purchase_blockers": ev.get("purchase_blockers") or [],
        "blockers": ev.get("blockers") or [],
        "gaps": ev.get("gaps") or [],
        "incremental_value": ev.get("incremental_value") or {},
        "measured_lift": ev.get("measured_lift") or {},
        "cost": ev.get("cost") or {},
        "licensing": ev.get("licensing") or {},
        "point_in_time": ev.get("point_in_time") or {},
        "state_reason_codes": ev.get("state_reason_codes") or [],
        "evaluation_id": art.get("evaluation_id"),
        "evaluation_hash": ev.get("evaluation_hash"),
        "supersedes_evaluation_id": art.get("supersedes_evaluation_id"),
        "generated_at": art.get("generated_at"),
        "immutable": True,
        "root_env": DATA_EXPANSION_DIR_ENV,
    }


def _decision_contexts_block() -> dict:
    """The two questions the ONE canonical gate answers, and what each result is NOT.

    Declared in the read contract so a reader never has to infer which question produced a
    state, and never reads a pre-research acquisition recommendation as post-research proof.
    """
    return {
        "vocabulary": list(DECISION_CONTEXT_VOCAB),
        "default": DEFAULT_DECISION_CONTEXT,
        "default_is_legacy_behaviour": True,
        "questions": dict(kernel.DECISION_CONTEXT_QUESTION),
        "research_acquisition_states": list(ACQUISITION_RECOMMENDATION_VOCAB),
        "post_acquisition_states": list(RECOMMENDATION_VOCAB),
        "acquisition_recommendation_is_alpha_evidence":
            kernel.ACQUISITION_RECOMMENDATION_IS_ALPHA_EVIDENCE,
        "acquisition_recommendation_is_integration_approval":
            kernel.ACQUISITION_RECOMMENDATION_IS_INTEGRATION_APPROVAL,
        "acquisition_recommendation_requires_manual_approval":
            kernel.ACQUISITION_RECOMMENDATION_REQUIRES_MANUAL_APPROVAL,
        "detail": ("Stage A asks whether the dataset is worth paying to LEARN, before any "
                   "lift can be measured. Stage B asks whether the measured evidence earned "
                   "continued purchase or integration. One canonical calculation owner "
                   "answers both; neither result may stand in for the other, and neither "
                   "grants purchasing authority."),
    }


def _cadence_block() -> dict:
    return {
        "enabled": CADENCE_ENABLED,
        "is_daily_job": False,
        "detail": "A full purchase-gate evaluation runs only on candidate add / metadata "
                  "change / sufficient new evidence / a review checkpoint / an explicit "
                  "operator request. The Daily Research Cycle may READ the latest status.",
        "reevaluation_triggers": ["CANDIDATE_ADDED", "CANDIDATE_METADATA_CHANGED",
                                  "SUFFICIENT_NEW_RESEARCH_EVIDENCE", "REVIEW_CHECKPOINT_DUE",
                                  "EXPLICIT_OPERATOR_REQUEST"],
    }


def load_data_expansion(*, catalog: Optional[list] = None, catalog_path=None,
                        data_expansion_dir=None, now: Optional[datetime] = None) -> dict:
    """The read contract for ``GET /v1/research/data-expansion``.

    STRICTLY READ-ONLY: it loads the dataset catalog (metadata) and the current persisted
    evaluation for each candidate. It NEVER runs the gate / recomputes a research study
    (candidates without a persisted evaluation show NOT_RUN). Always degrade-safe.
    """
    generated_at = _now_iso(now)
    try:
        entries = load_catalog(catalog=catalog, catalog_path=catalog_path)
    except Exception as exc:  # noqa: BLE001
        return _empty_read(generated_at, STATE_UNAVAILABLE,
                           "Dataset catalog is unavailable: %s" % str(exc)[:160])

    if not entries:
        return _empty_read(generated_at, STATE_EMPTY_CATALOG,
                           "The dataset catalog is empty.")

    datasets = []
    evaluated = 0
    acquisition_evaluated = 0
    for ce in entries:
        did = ce.get("dataset_id")
        art = None
        acq_art = None
        try:
            art = load_latest_evaluation(dataset_id=did, data_expansion_dir=data_expansion_dir)
        except Exception:  # noqa: BLE001 — a pure artifact read must never crash the read
            art = None
        try:
            acq_art = load_latest_evaluation(
                dataset_id=did, data_expansion_dir=data_expansion_dir,
                decision_context=CONTEXT_RESEARCH_ACQUISITION)
        except Exception:  # noqa: BLE001
            acq_art = None
        view = _evaluation_view(art)
        if art:
            evaluated += 1
        if acq_art:
            acquisition_evaluated += 1
        datasets.append({
            "dataset_id": did,
            "provider": ce.get("provider"),
            "dataset_name": ce.get("dataset_name"),
            "data_category": ce.get("data_category"),
            "feature_families": ce.get("feature_families") or [],
            "metadata_summary": _metadata_summary(ce),
            "research_intent": (ce.get("research_requirements") or {}).get("research_intent"),
            "notes": ce.get("notes"),
            "evaluation": view,
            # Stage A, reported BESIDE Stage B and never merged into it: one says "worth
            # paying to learn", the other "the measured evidence earned continued purchase".
            "acquisition_evaluation": _evaluation_view(acq_art),
        })

    datasets.sort(key=lambda d: d.get("dataset_id") or "")
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": kernel.CALCULATION_OWNER,
        "generated_at": generated_at,
        "state": STATE_OK,
        "message": "Data Expansion purchase-gate catalog and latest immutable evaluations. "
                   "Research governance only — manual purchase approval required; no dataset "
                   "purchased, no provider activated, no paid API called, automation off.",
        "catalog_size": len(entries),
        "evaluated_count": evaluated,
        "not_run_count": len(entries) - evaluated,
        "acquisition_evaluated_count": acquisition_evaluated,
        "recommendation_vocabulary": list(RECOMMENDATION_VOCAB),
        "acquisition_recommendation_vocabulary": list(ACQUISITION_RECOMMENDATION_VOCAB),
        "read_state_vocabulary": list(READ_STATE_VOCAB),
        "decision_contexts": _decision_contexts_block(),
        "datasets": datasets,
        "policy": resolve_policy(),
        "policy_version": POLICY_VERSION,
        "cadence": _cadence_block(),
        "provenance": _read_provenance(),
        "safety": kernel._safety(),
        "research_only": True,
        "sole_run_path": "api.data_expansion.run_and_persist (operator / checkpoint only)",
    }


def load_data_expansion_detail(dataset_id: str, *, catalog: Optional[list] = None,
                               catalog_path=None, data_expansion_dir=None,
                               now: Optional[datetime] = None) -> dict:
    """The read contract for ``GET /v1/research/data-expansion/{dataset_id}``.

    Returns the full catalog entry + the full latest immutable evaluation (or NOT_RUN).
    NOT_FOUND (HTTP-200 body) when the dataset id is not in the catalog. Read-only."""
    generated_at = _now_iso(now)
    try:
        ce = _catalog_entry(dataset_id, catalog=catalog, catalog_path=catalog_path)
    except Exception as exc:  # noqa: BLE001
        return {"schema_version": SCHEMA_VERSION, "phase": PHASE, "generated_at": generated_at,
                "state": STATE_UNAVAILABLE, "dataset_id": dataset_id,
                "message": "Catalog unavailable: %s" % str(exc)[:160],
                "safety": kernel._safety()}
    if ce is None:
        return {"schema_version": SCHEMA_VERSION, "phase": PHASE, "generated_at": generated_at,
                "state": STATE_NOT_FOUND, "dataset_id": dataset_id,
                "message": "No such dataset candidate in the catalog.",
                "recommendation_vocabulary": list(RECOMMENDATION_VOCAB),
                "cadence": _cadence_block(), "safety": kernel._safety()}
    art = None
    acq_art = None
    try:
        art = load_latest_evaluation(dataset_id=dataset_id, data_expansion_dir=data_expansion_dir)
    except Exception:  # noqa: BLE001
        art = None
    try:
        acq_art = load_latest_evaluation(
            dataset_id=dataset_id, data_expansion_dir=data_expansion_dir,
            decision_context=CONTEXT_RESEARCH_ACQUISITION)
    except Exception:  # noqa: BLE001
        acq_art = None
    full_eval = (art or {}).get("evaluation")
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": kernel.CALCULATION_OWNER,
        "generated_at": generated_at,
        "state": (full_eval or {}).get("recommendation_state") if full_eval else STATE_NOT_RUN,
        "dataset_id": dataset_id,
        "provider": ce.get("provider"),
        "dataset_name": ce.get("dataset_name"),
        "data_category": ce.get("data_category"),
        "feature_families": ce.get("feature_families") or [],
        "catalog_entry": ce,
        "metadata_summary": _metadata_summary(ce),
        "evaluation_view": _evaluation_view(art),
        "evaluation": full_eval,
        "acquisition_evaluation_view": _evaluation_view(acq_art),
        "acquisition_evaluation": (acq_art or {}).get("evaluation"),
        "recommendation_vocabulary": list(RECOMMENDATION_VOCAB),
        "acquisition_recommendation_vocabulary": list(ACQUISITION_RECOMMENDATION_VOCAB),
        "read_state_vocabulary": list(READ_STATE_VOCAB),
        "decision_contexts": _decision_contexts_block(),
        "cadence": _cadence_block(),
        "policy": resolve_policy(),
        "policy_version": POLICY_VERSION,
        "provenance": _read_provenance(),
        "safety": kernel._safety(),
        "message": "Data Expansion candidate detail — research governance only; manual "
                   "purchase approval required; no purchase, no provider activation, "
                   "automation off.",
    }


def load_data_expansion_summary(*, catalog: Optional[list] = None, catalog_path=None,
                                data_expansion_dir=None) -> dict:
    """A compact, read-only summary the Daily Research Cycle / workflow state may READ.

    PURE ARTIFACT/CATALOG READER — it never runs the gate and never calls a provider. It lets
    the DRC surface the latest data-expansion status without turning the purchase gate into a
    daily job (cadence disabled)."""
    empty = {
        "data_expansion_available": False,
        "data_expansion_catalog_size": 0,
        "data_expansion_evaluated_count": 0,
        "data_expansion_purchase_recommended_count": 0,
        "data_expansion_integration_recommended_count": 0,
        "data_expansion_research_acquisition_recommended_count": 0,
        "data_expansion_states": {},
        "data_expansion_acquisition_states": {},
        "data_expansion_cadence_enabled": CADENCE_ENABLED,
    }
    try:
        entries = load_catalog(catalog=catalog, catalog_path=catalog_path)
    except Exception:  # noqa: BLE001
        return dict(empty)
    if not entries:
        return dict(empty)
    states: dict[str, str] = {}
    acq_states: dict[str, str] = {}
    evaluated = 0
    for ce in entries:
        did = ce.get("dataset_id")
        art = None
        acq_art = None
        try:
            art = load_latest_evaluation(dataset_id=did, data_expansion_dir=data_expansion_dir)
        except Exception:  # noqa: BLE001
            art = None
        try:
            acq_art = load_latest_evaluation(
                dataset_id=did, data_expansion_dir=data_expansion_dir,
                decision_context=CONTEXT_RESEARCH_ACQUISITION)
        except Exception:  # noqa: BLE001
            acq_art = None
        st = ((art or {}).get("evaluation") or {}).get("recommendation_state") or STATE_NOT_RUN
        states[did] = st
        acq_states[did] = (((acq_art or {}).get("evaluation") or {})
                           .get("recommendation_state") or STATE_NOT_RUN)
        if art:
            evaluated += 1
    return {
        "data_expansion_available": True,
        "data_expansion_catalog_size": len(entries),
        "data_expansion_evaluated_count": evaluated,
        "data_expansion_purchase_recommended_count":
            sum(1 for s in states.values() if s == kernel.REC_PURCHASE),
        "data_expansion_integration_recommended_count":
            sum(1 for s in states.values() if s == kernel.REC_INTEGRATION),
        "data_expansion_research_acquisition_recommended_count":
            sum(1 for s in acq_states.values() if s == kernel.REC_RESEARCH_ACQUISITION),
        "data_expansion_states": states,
        "data_expansion_acquisition_states": acq_states,
        "data_expansion_cadence_enabled": CADENCE_ENABLED,
    }


def _read_provenance() -> dict:
    return {
        "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": kernel.CALCULATION_OWNER,
        "decision_contexts": list(DECISION_CONTEXT_VOCAB),
        "default_decision_context": DEFAULT_DECISION_CONTEXT,
        "catalog_source": "api.data_expansion.DEFAULT_CATALOG / %s" % DATA_EXPANSION_CATALOG_ENV,
        "artifact_root_env": DATA_EXPANSION_DIR_ENV,
        "reused_owners": {
            "provider_provenance": "alpha_agent.source_contracts",
            "freshness": "api.data_freshness",
            "evidence_gates": "alpha_agent.experiment_contracts.DEFAULT_GATES",
            "analyst_revisions_framework": "alpha_agent.analyst_revisions (Stage 13A)",
            "research_opportunities": "engine.research_agent (Slice 8 DATA opportunities)",
        },
        "policy_version": POLICY_VERSION,
    }


def _empty_read(generated_at: str, state: str, message: str) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": kernel.CALCULATION_OWNER,
        "generated_at": generated_at,
        "state": state,
        "message": message,
        "catalog_size": 0,
        "evaluated_count": 0,
        "not_run_count": 0,
        "acquisition_evaluated_count": 0,
        "recommendation_vocabulary": list(RECOMMENDATION_VOCAB),
        "acquisition_recommendation_vocabulary": list(ACQUISITION_RECOMMENDATION_VOCAB),
        "read_state_vocabulary": list(READ_STATE_VOCAB),
        "decision_contexts": _decision_contexts_block(),
        "datasets": [],
        "policy": resolve_policy(),
        "policy_version": POLICY_VERSION,
        "cadence": _cadence_block(),
        "provenance": _read_provenance(),
        "safety": kernel._safety(),
        "research_only": True,
    }
