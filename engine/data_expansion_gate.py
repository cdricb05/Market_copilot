r"""Phase 29J Slice 9 — Data Expansion / Purchase-Gate (pure evaluation kernel).

This module is the ONE canonical dataset purchase/integration-gate calculation (Consolidation
Roadmap Slice 9 / Charter Milestone 5). It is a **pure, deterministic kernel**: it performs
NO I/O — no file, database, network, provider, prediction or credential access — and never
mutates its inputs. Everything it needs arrives as one immutable dataset-evaluation input
contract (assembled by ``api.data_expansion`` from the authoritative dataset catalog,
provider-metadata and research-evidence owners) and it returns one immutable evaluation
result.

Given an external-dataset candidate's metadata, the intended research requirements and the
MEASURED research evidence produced by the existing experiment/evidence owners, it decides —
across sixteen explicit dimensions — whether the dataset is worth acquiring / integrating.
The dimensions are:

  1. point-in-time integrity;              9.  survivorship-bias risk;
  2. historical depth;                     10. restatement / backfill behaviour;
  3. inactive / delisted coverage;         11. licensing / redistribution restrictions;
  4. universe breadth / coverage;          12. acquisition cost;
  5. effective usable sample;              13. incremental research information (redundancy);
  6. revision-history support;             14. measured challenger / research lift;
  7. update frequency / freshness;         15. implementation complexity;
  8. identifier quality;                   16. operational reliability.

Every dimension yields an explained sub-assessment (state + reason codes + value), never one
opaque score, and the missing data of any dimension is surfaced as a documented gap — the
kernel NEVER fabricates a score when required data is absent. Hard blockers (no defensible
point-in-time history; survivorship-only history when the research requires full historical
universe integrity; insufficient usable history; insufficient universe breadth; unclear or
prohibited licensing; future leakage; no reliable identifier mapping; a research sample too
small; a dataset that materially duplicates already-owned information without measurable
lift) are separated from soft, visible gaps.

The gate answers TWO business questions, and which one it is answering is an explicit input
(``decision_context``) rather than an assumption:

  * ``POST_ACQUISITION_VALUE`` — *"now that the dataset has been acquired and tested, did it
    produce enough MEASURED incremental value to justify continued purchase / integration?"*
    This is the historical behaviour of this kernel and remains the DEFAULT for every caller
    that does not ask for anything else. Its evidence standards are unchanged: no purchase
    recommendation without out-of-sample, adequately-sampled, cost-adjusted measured lift.
  * ``RESEARCH_ACQUISITION`` — *"is this dataset credible and economically valuable enough to
    justify a MANUALLY APPROVED research acquisition, so that we can learn?"* This stage
    happens BEFORE measured lift can exist, and requiring lift here is circular: the lift of
    data that has not been acquired cannot be measured. Stage A therefore does not require
    measured lift — and is strict about everything else, including two requirements Stage B
    has no reason to make: a DECLARED capability the acquisition unlocks, and a DECLARED
    expectation of economic distinctness from already-owned data, each of which must be
    supplied by the caller and neither of which the kernel will invent.

The result is ONE explicit recommendation. The post-acquisition vocabulary is frozen:

    REJECT · INSUFFICIENT_EVIDENCE · RESEARCH_ONLY · CANDIDATE ·
    PURCHASE_RECOMMENDED · INTEGRATION_RECOMMENDED

and the research-acquisition vocabulary reuses the three states that mean the same thing in
both stages and adds the two that only exist before acquisition:

    REJECT · INSUFFICIENT_EVIDENCE · CANDIDATE ·
    RESEARCH_ACQUISITION_RECOMMENDED · NO_ACQUISITION_REQUIRED_ALREADY_ENTITLED

``RESEARCH_ACQUISITION_RECOMMENDED`` is deliberately NOT ``PURCHASE_RECOMMENDED``: one says
"worth paying to learn", the other says "the measured evidence earned continued purchase",
and collapsing them would let a pre-research judgement be read as post-research proof.

Everything the gate produces is RESEARCH GOVERNANCE ONLY. The result NEVER purchases a
dataset, subscribes to or activates a provider, calls a paid provider, uses a paid API quota,
alters credentials, integrates a dataset, mutates the portfolio, promotes a model, creates an
order/fill, changes holdings / cash / NAV, or enables cadence. ``PURCHASE_RECOMMENDED`` /
``INTEGRATION_RECOMMENDED`` are always emitted as *MANUAL APPROVAL REQUIRED* and never
constitute purchasing authority. No purchase recommendation is ever made on in-sample-only
evidence, and current / live P&L is never used as proof that a paid dataset is necessary.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Optional

SCHEMA_VERSION = "data_expansion_gate.v1"
INPUT_SCHEMA_VERSION = "data_expansion_gate.input.v1"
POLICY_VERSION = "data_expansion_gate_policy.v1"

CALCULATION_OWNER = "engine.data_expansion_gate"

# --- Decision context — WHICH question the gate is being asked ---------------- #
# Stage A: before acquisition, "is it worth paying to learn?". Stage B: after acquisition and
# research, "did it earn continued purchase / integration?". The DEFAULT is Stage B, which is
# this kernel's historical behaviour, so every existing caller keeps its exact semantics
# without being edited.
CONTEXT_RESEARCH_ACQUISITION = "RESEARCH_ACQUISITION"
CONTEXT_POST_ACQUISITION_VALUE = "POST_ACQUISITION_VALUE"
DECISION_CONTEXT_VOCAB = (CONTEXT_RESEARCH_ACQUISITION, CONTEXT_POST_ACQUISITION_VALUE)
DEFAULT_DECISION_CONTEXT = CONTEXT_POST_ACQUISITION_VALUE
LEGACY_DECISION_CONTEXT = CONTEXT_POST_ACQUISITION_VALUE

DECISION_CONTEXT_QUESTION = {
    CONTEXT_RESEARCH_ACQUISITION:
        "Is this dataset credible and economically valuable enough to justify a manually "
        "approved research acquisition, so that we can learn? Measured research lift cannot "
        "exist yet and is therefore NOT required — everything else is.",
    CONTEXT_POST_ACQUISITION_VALUE:
        "Now that the dataset has been acquired and tested, did it produce enough measured "
        "incremental value to justify continued purchase / integration?",
}

# --- Frozen post-acquisition recommendation vocabulary ------------------------ #
REC_REJECT = "REJECT"
REC_INSUFFICIENT = "INSUFFICIENT_EVIDENCE"
REC_RESEARCH_ONLY = "RESEARCH_ONLY"
REC_CANDIDATE = "CANDIDATE"
REC_PURCHASE = "PURCHASE_RECOMMENDED"
REC_INTEGRATION = "INTEGRATION_RECOMMENDED"
RECOMMENDATION_VOCAB = (REC_REJECT, REC_INSUFFICIENT, REC_RESEARCH_ONLY, REC_CANDIDATE,
                        REC_PURCHASE, REC_INTEGRATION)
#: The historical name is the POST-ACQUISITION vocabulary; the alias says so out loud.
POST_ACQUISITION_RECOMMENDATION_VOCAB = RECOMMENDATION_VOCAB

# --- Research-acquisition (Stage A) vocabulary -------------------------------- #
# REJECT / INSUFFICIENT_EVIDENCE / CANDIDATE mean exactly what they mean in Stage B and are
# reused rather than duplicated. The two states below exist only before acquisition.
REC_RESEARCH_ACQUISITION = "RESEARCH_ACQUISITION_RECOMMENDED"
REC_ALREADY_ENTITLED = "NO_ACQUISITION_REQUIRED_ALREADY_ENTITLED"
ACQUISITION_RECOMMENDATION_VOCAB = (REC_REJECT, REC_INSUFFICIENT, REC_CANDIDATE,
                                    REC_RESEARCH_ACQUISITION, REC_ALREADY_ENTITLED)
#: Every state either stage can emit, in a stable order, for read layers.
DECISION_STATE_VOCAB = tuple(dict.fromkeys(RECOMMENDATION_VOCAB
                                           + ACQUISITION_RECOMMENDATION_VOCAB))

VOCAB_FOR_CONTEXT = {
    CONTEXT_RESEARCH_ACQUISITION: ACQUISITION_RECOMMENDATION_VOCAB,
    CONTEXT_POST_ACQUISITION_VALUE: RECOMMENDATION_VOCAB,
}

#: A Stage-A recommendation is NOT evidence of alpha, NOT integration approval and NOT
#: purchasing authority. Declared as a constant so no reader has to infer it.
ACQUISITION_RECOMMENDATION_IS_ALPHA_EVIDENCE = False
ACQUISITION_RECOMMENDATION_IS_INTEGRATION_APPROVAL = False
ACQUISITION_RECOMMENDATION_REQUIRES_MANUAL_APPROVAL = True

# --- Per-dimension state vocabulary ------------------------------------------ #
D_PASS = "PASS"
D_WATCH = "WATCH"
D_FAIL = "FAIL"
D_UNKNOWN = "UNKNOWN"
DIMENSION_STATE_VOCAB = (D_PASS, D_WATCH, D_FAIL, D_UNKNOWN)

# The sixteen canonical dimensions (stable order).
DIMENSIONS = (
    "point_in_time_integrity", "historical_depth", "inactive_delisted_coverage",
    "universe_breadth", "effective_sample", "revision_history",
    "update_frequency_freshness", "identifier_quality", "survivorship_bias_risk",
    "restatement_backfill_behavior", "licensing", "acquisition_cost",
    "incremental_information", "measured_research_lift", "implementation_complexity",
    "operational_reliability",
)

# Two dimensions that only exist BEFORE acquisition. Stage B has no reason to ask them (by
# then the answer is measured, not expected), so they are appended only in Stage A and the
# canonical sixteen above are left exactly as they were.
ACQ_CAPABILITY = "capability_unlocked"
ACQ_DISTINCTNESS = "expected_incremental_distinctness"
ACQUISITION_ONLY_DIMENSIONS = (ACQ_CAPABILITY, ACQ_DISTINCTNESS)
ACQUISITION_DIMENSIONS = DIMENSIONS + ACQUISITION_ONLY_DIMENSIONS

DIMENSIONS_FOR_CONTEXT = {
    CONTEXT_RESEARCH_ACQUISITION: ACQUISITION_DIMENSIONS,
    CONTEXT_POST_ACQUISITION_VALUE: DIMENSIONS,
}

#: Declared expectation of economic distinctness from already-owned data. Stage A cannot
#: measure a correlation it has no data for, so the CALLER must declare an expectation and
#: name what it rests on; the kernel refuses to guess and treats an absent declaration as
#: unknown rather than as good news.
DISTINCTNESS_VOCAB = ("HIGH", "MEDIUM", "LOW", "NONE", "UNKNOWN")

# --- Blocker tiers ----------------------------------------------------------- #
TIER_DISQUALIFYING = "DISQUALIFYING"   # -> REJECT
TIER_EVIDENCE = "EVIDENCE"             # -> INSUFFICIENT_EVIDENCE
TIER_PURCHASE = "PURCHASE"             # -> caps below PURCHASE_RECOMMENDED (CANDIDATE)
BLOCKER_TIER_VOCAB = (TIER_DISQUALIFYING, TIER_EVIDENCE, TIER_PURCHASE)

# The gate NEVER acquires / integrates / activates anything; every recommended action needs
# explicit manual governance approval. Embedded in every recommendation.
FORBIDDEN_ACTIONS = (
    "AUTO_PURCHASE_DATASET", "AUTO_SUBSCRIBE_PROVIDER", "AUTO_ACTIVATE_PROVIDER",
    "AUTO_INTEGRATE_DATASET", "AUTO_ENABLE_PAID_DATA", "CALL_PAID_PROVIDER",
    "USE_PAID_API_QUOTA", "ALTER_PROVIDER_CREDENTIALS", "MODIFY_EXTERNAL_ACCOUNT",
    "MUTATE_PORTFOLIO", "PROMOTE_MODEL", "CREATE_ORDERS", "ENABLE_CADENCE",
)

MANUAL_APPROVAL_PENDING = "PENDING_MANUAL_APPROVAL"


# --------------------------------------------------------------------------- #
# Policy — one explicit, versioned set of gate thresholds.
# --------------------------------------------------------------------------- #
def default_policy() -> dict[str, Any]:
    """The single explicit, versioned data-expansion gate policy.

    Per-research requirements (``min_history_years`` / ``min_universe_size`` /
    ``min_universe_coverage`` / ``min_effective_sample``) OVERRIDE the ``default_*`` floors
    here when the intended research supplies them; the ``default_*`` values are only the
    fallback when a research requirement is silent. The lift / redundancy / cost floors are
    genuinely-new Slice-9 policy, declared once, returned in the payload and folded into the
    evaluation hash, and exercised at their boundaries by the test suite. Where an
    authoritative research-evidence gate already exists (``alpha_agent.experiment_contracts``)
    the API owner may inject it so no evidence threshold is silently forked.
    """
    return {
        "policy_version": POLICY_VERSION,
        # --- requirement fallbacks (overridden per research intent) ----------- #
        "default_min_history_years": 10,
        "default_min_universe_size": 500,
        "default_min_universe_coverage": 0.60,
        "default_min_effective_sample": 24,
        "requires_point_in_time_default": True,
        # --- incremental value / redundancy ---------------------------------- #
        "redundancy_correlation_ceiling": 0.85,   # |corr| at/above -> redundant overlap
        # --- measured out-of-sample lift floors ------------------------------ #
        "min_rank_ic_lift": 0.005,                # OOS rank-IC improvement
        "min_decile_spread_lift_pp": 0.25,        # OOS decile-spread improvement (pct points)
        "min_challenger_lift_pp": 1.0,            # challenger excess improvement (pct points)
        "min_cost_adjusted_lift_pp": 0.0,         # cost-adjusted lift must be >= this
        "max_turnover_deterioration": 0.25,       # one-way turnover increase tolerated
        # --- cost / freshness / implementation ------------------------------- #
        "annual_cost_budget_usd": 12000.0,        # over-budget caps below PURCHASE
        "max_acceptable_staleness_days": 45,      # last-update lag beyond this -> WATCH
        # --- RESEARCH_ACQUISITION (Stage A) floors --------------------------- #
        # Used ONLY when the caller asks for the research-acquisition context; they change
        # no Stage-B behaviour. Each encodes a rule this estate already holds elsewhere:
        # capability must be named (a purchase that unlocks nothing teaches nothing), owned
        # and free substitutes must have been tried first (USE_OWNED_DATA_FIRST), and a
        # bounded evaluation that CAN return DO_NOT_BUY must exist before money is asked for.
        "acquisition_min_capability_unlocked": 1,
        "acquisition_min_distinctness": ("HIGH", "MEDIUM"),
        "acquisition_requires_owned_substitute_tried": True,
        "acquisition_requires_bounded_evaluation": True,
    }


# --------------------------------------------------------------------------- #
# Small numeric / normalisation helpers (stdlib only, deterministic)
# --------------------------------------------------------------------------- #
def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _i(x: Any) -> Optional[int]:
    v = _f(x)
    return int(v) if v is not None else None


def _r(x: Optional[float], nd: int) -> Optional[float]:
    return None if x is None else round(float(x), nd)


def _u(x: Any) -> Optional[str]:
    """Upper-cased trimmed string, or None."""
    if x is None:
        return None
    s = str(x).strip()
    return s.upper() if s else None


def _b(x: Any) -> Optional[bool]:
    """Tri-state boolean: True / False / None (unknown)."""
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    if s in ("true", "yes", "y", "1"):
        return True
    if s in ("false", "no", "n", "0"):
        return False
    return None


# --------------------------------------------------------------------------- #
# Stable hashing (evaluation_hash excludes generated_at / volatile keys)
# --------------------------------------------------------------------------- #
_VOLATILE_KEYS = frozenset({"generated_at", "evaluated_at", "loaded_at", "built_at",
                            "evaluation_hash", "artifact", "last_evaluated_at"})


def _strip_volatile(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _strip_volatile(v) for k, v in obj.items() if k not in _VOLATILE_KEYS}
    if isinstance(obj, (list, tuple)):
        return [_strip_volatile(v) for v in obj]
    return obj


def stable_hash(obj: Any) -> str:
    payload = json.dumps(_strip_volatile(obj), sort_keys=True, default=str,
                         separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Record constructors
# --------------------------------------------------------------------------- #
def _dim(name: str, state: str, *, value=None, required=None, reason_codes=None,
         detail=None) -> dict:
    return {
        "dimension": name,
        "state": state,
        "value": value,
        "required": required,
        "reason_codes": sorted(set(reason_codes or [])),
        "detail": detail,
    }


def _blocker(code: str, tier: str, dimension: str, detail: str) -> dict:
    return {"code": code, "tier": tier, "dimension": dimension, "detail": detail}


def _gap(code: str, dimension: str, detail: str, *, by_design: bool = False) -> dict:
    return {"code": code, "dimension": dimension, "by_design": bool(by_design),
            "detail": detail}


# --------------------------------------------------------------------------- #
# Evidence-derived facts (computed once; drive incremental / lift / redundancy)
# --------------------------------------------------------------------------- #
def _evidence_facts(ev: dict, req: dict, pol: dict) -> dict:
    available = bool(ev.get("available"))
    oos = _b(ev.get("out_of_sample"))
    in_sample_only = (_b(ev.get("in_sample_only")) is True) or (available and oos is False)
    eff = _i(ev.get("effective_sample"))
    min_eff = _i(req.get("min_effective_sample")) or pol["default_min_effective_sample"]
    sample_ok = eff is not None and eff >= min_eff

    ric = _f(ev.get("rank_ic_lift"))
    dsl = _f(ev.get("decile_spread_lift_pp"))
    chl = _f(ev.get("challenger_excess_lift_pp"))
    cal = _f(ev.get("cost_adjusted_lift_pp"))
    tdelta = _f(ev.get("turnover_delta"))
    corr = _f(ev.get("max_abs_correlation_with_existing"))
    regime_robust = _b(ev.get("regime_robust"))
    sector_robust = _b(ev.get("sector_robust"))
    distinct = _b(ev.get("distinct_information"))

    material = bool(oos) and (
        (ric is not None and ric >= pol["min_rank_ic_lift"])
        or (dsl is not None and dsl >= pol["min_decile_spread_lift_pp"])
        or (chl is not None and chl >= pol["min_challenger_lift_pp"]))
    redundant = corr is not None and corr >= pol["redundancy_correlation_ceiling"]
    distinct_info = (distinct is True) or (corr is not None
                                           and corr < pol["redundancy_correlation_ceiling"])
    turnover_ok = tdelta is None or tdelta <= pol["max_turnover_deterioration"]
    cost_adj_ok = cal is not None and cal >= pol["min_cost_adjusted_lift_pp"]
    robust = bool(material and regime_robust is True and sector_robust is True
                  and turnover_ok and cost_adj_ok)

    return {
        "available": available, "out_of_sample": oos, "in_sample_only": in_sample_only,
        "effective_sample": eff, "min_effective_sample": min_eff, "sample_ok": sample_ok,
        "rank_ic_lift": ric, "decile_spread_lift_pp": dsl, "challenger_excess_lift_pp": chl,
        "cost_adjusted_lift_pp": cal, "turnover_delta": tdelta,
        "max_abs_correlation_with_existing": corr,
        "regime_robust": regime_robust, "sector_robust": sector_robust,
        "distinct_information": distinct_info,
        "material_lift": material, "redundant": redundant, "turnover_ok": turnover_ok,
        "cost_adjusted_ok": cost_adj_ok, "robust": robust,
        "study_reference": ev.get("study_reference"),
        "evidence_owner": ev.get("evidence_owner") or "alpha_agent.experiment_contracts",
    }


# --------------------------------------------------------------------------- #
# Per-dimension evaluators — each returns (dim, [blockers], [gaps]).
# --------------------------------------------------------------------------- #
def _eval_pit(ds: dict, req: dict, pol: dict):
    requires_pit = req.get("requires_point_in_time")
    if requires_pit is None:
        requires_pit = pol["requires_point_in_time_default"]
    requires_pit = bool(requires_pit)
    leak = _b(ds.get("contains_future_leakage"))
    pit = _u(ds.get("pit_guarantee"))
    ts = _b(ds.get("publication_timestamps"))
    blk, gap = [], []
    if leak is True:
        blk.append(_blocker("FUTURE_LEAKAGE", TIER_DISQUALIFYING, "point_in_time_integrity",
                            "The dataset exhibits future leakage / look-ahead."))
        return _dim("point_in_time_integrity", D_FAIL, value=pit,
                    reason_codes=["FUTURE_LEAKAGE"],
                    detail="Future leakage invalidates any point-in-time use."), blk, gap
    if pit in ("GUARANTEED_PIT", "EFFECTIVE_DATED"):
        return _dim("point_in_time_integrity", D_PASS, value=pit,
                    detail="Effective-dated / guaranteed point-in-time history."), blk, gap
    if pit == "SNAPSHOT_ONLY":
        if ts is True:
            return _dim("point_in_time_integrity", D_PASS, value=pit,
                        reason_codes=["SNAPSHOT_WITH_PUBLICATION_TIMESTAMPS"],
                        detail="Snapshot vintages carry publication timestamps (defensible PIT)."), blk, gap
        gap.append(_gap("SNAPSHOT_ONLY_WITHOUT_PUBLICATION_TIMESTAMPS", "point_in_time_integrity",
                        "Snapshot-only history without explicit publication timestamps."))
        return _dim("point_in_time_integrity", D_WATCH, value=pit,
                    reason_codes=["SNAPSHOT_ONLY_WITHOUT_PUBLICATION_TIMESTAMPS"],
                    detail="Snapshot-only history; PIT is only partially defensible."), blk, gap
    if pit == "CURRENT_ONLY":
        if requires_pit:
            blk.append(_blocker("NO_DEFENSIBLE_PIT_HISTORY", TIER_DISQUALIFYING,
                                "point_in_time_integrity",
                                "Current-only data cannot serve point-in-time research."))
            return _dim("point_in_time_integrity", D_FAIL, value=pit, required="PIT",
                        reason_codes=["NO_DEFENSIBLE_PIT_HISTORY"],
                        detail="Current-only history; no defensible point-in-time record."), blk, gap
        gap.append(_gap("CURRENT_ONLY_HISTORY", "point_in_time_integrity",
                        "Current-only history (research does not require PIT)."))
        return _dim("point_in_time_integrity", D_WATCH, value=pit,
                    detail="Current-only history; acceptable only for non-PIT research."), blk, gap
    # Missing / unknown PIT metadata: do not fabricate — surface as an evidence gap.
    if requires_pit:
        blk.append(_blocker("PIT_METADATA_MISSING", TIER_EVIDENCE, "point_in_time_integrity",
                            "Point-in-time metadata is absent; PIT integrity cannot be confirmed."))
    gap.append(_gap("PIT_METADATA_MISSING", "point_in_time_integrity",
                    "No point-in-time guarantee metadata was supplied."))
    return _dim("point_in_time_integrity", D_UNKNOWN, value=pit,
                reason_codes=["PIT_METADATA_MISSING"],
                detail="No PIT metadata; integrity is unproven."), blk, gap


def _eval_history(ds: dict, req: dict, pol: dict):
    req_years = _f(req.get("min_history_years"))
    if req_years is None:
        req_years = float(pol["default_min_history_years"])
    years = _f(ds.get("history_years"))
    blk, gap = [], []
    if years is None:
        blk.append(_blocker("HISTORY_DEPTH_UNKNOWN", TIER_EVIDENCE, "historical_depth",
                            "Usable historical depth is not specified."))
        gap.append(_gap("HISTORY_DEPTH_UNKNOWN", "historical_depth",
                        "No usable historical-depth metadata was supplied."))
        return _dim("historical_depth", D_UNKNOWN, value=years, required=req_years,
                    reason_codes=["HISTORY_DEPTH_UNKNOWN"],
                    detail="Historical depth unknown."), blk, gap
    if years < req_years:
        blk.append(_blocker("INSUFFICIENT_HISTORY", TIER_DISQUALIFYING, "historical_depth",
                            "Usable history %.1fy is below the required %.1fy." % (years, req_years)))
        return _dim("historical_depth", D_FAIL, value=years, required=req_years,
                    reason_codes=["INSUFFICIENT_HISTORY"],
                    detail="Insufficient usable history for the intended research."), blk, gap
    return _dim("historical_depth", D_PASS, value=years, required=req_years,
                detail="Sufficient usable history."), blk, gap


def _eval_inactive(ds: dict, req: dict, pol: dict):
    support = _b(ds.get("inactive_delisted_support"))
    integ = bool(req.get("requires_full_universe_integrity"))
    blk, gap = [], []
    if support is True:
        return _dim("inactive_delisted_coverage", D_PASS, value=True,
                    detail="Inactive / delisted / security-history is covered."), blk, gap
    if support is False:
        if integ:
            # The disqualifying blocker is raised by the survivorship dimension; here we FAIL.
            return _dim("inactive_delisted_coverage", D_FAIL, value=False, required=True,
                        reason_codes=["INACTIVE_DELISTED_NOT_SUPPORTED"],
                        detail="No inactive/delisted coverage while integrity is required."), blk, gap
        gap.append(_gap("INACTIVE_DELISTED_NOT_SUPPORTED", "inactive_delisted_coverage",
                        "No inactive/delisted coverage (research does not require integrity)."))
        return _dim("inactive_delisted_coverage", D_WATCH, value=False,
                    reason_codes=["INACTIVE_DELISTED_NOT_SUPPORTED"],
                    detail="No inactive/delisted coverage."), blk, gap
    gap.append(_gap("INACTIVE_DELISTED_SUPPORT_UNKNOWN", "inactive_delisted_coverage",
                    "Inactive/delisted support is unspecified."))
    return _dim("inactive_delisted_coverage", D_UNKNOWN, value=None,
                reason_codes=["INACTIVE_DELISTED_SUPPORT_UNKNOWN"],
                detail="Inactive/delisted support unknown."), blk, gap


def _eval_universe(ds: dict, req: dict, pol: dict):
    size = _i(ds.get("universe_size"))
    cov = _f(ds.get("universe_coverage_ratio"))
    req_size = _i(req.get("min_universe_size")) or pol["default_min_universe_size"]
    req_cov = _f(req.get("min_universe_coverage"))
    if req_cov is None:
        req_cov = pol["default_min_universe_coverage"]
    blk, gap = [], []
    deteriorating = _b(ds.get("coverage_deterioration")) is True \
        or _u(ds.get("coverage_trend")) == "DETERIORATING"
    if deteriorating:
        gap.append(_gap("COVERAGE_DETERIORATION", "universe_breadth",
                        "Universe coverage is deteriorating over time."))
    if size is None and cov is None:
        gap.append(_gap("UNIVERSE_BREADTH_UNKNOWN", "universe_breadth",
                        "Neither universe size nor coverage ratio supplied."))
        return _dim("universe_breadth", D_UNKNOWN, value=None,
                    required={"size": req_size, "coverage": req_cov},
                    reason_codes=["UNIVERSE_BREADTH_UNKNOWN"],
                    detail="Universe breadth unknown."), blk, gap
    below = (cov is not None and cov < req_cov) or (size is not None and size < req_size)
    if below:
        blk.append(_blocker("INSUFFICIENT_UNIVERSE_BREADTH", TIER_DISQUALIFYING,
                            "universe_breadth",
                            "Universe breadth (size=%s, coverage=%s) is below requirement "
                            "(size>=%s, coverage>=%s)." % (size, cov, req_size, req_cov)))
        return _dim("universe_breadth", D_FAIL, value={"size": size, "coverage": cov},
                    required={"size": req_size, "coverage": req_cov},
                    reason_codes=["INSUFFICIENT_UNIVERSE_BREADTH"],
                    detail="Insufficient universe breadth."), blk, gap
    state = D_WATCH if deteriorating else D_PASS
    return _dim("universe_breadth", state, value={"size": size, "coverage": cov},
                required={"size": req_size, "coverage": req_cov},
                reason_codes=(["COVERAGE_DETERIORATION"] if deteriorating else []),
                detail="Universe breadth meets the requirement."), blk, gap


def _eval_effective_sample(facts: dict):
    eff = facts["effective_sample"]
    req = facts["min_effective_sample"]
    blk, gap = [], []
    if eff is None:
        gap.append(_gap("EFFECTIVE_SAMPLE_UNKNOWN", "effective_sample",
                        "Effective usable sample size is not specified."))
        return _dim("effective_sample", D_UNKNOWN, value=None, required=req,
                    reason_codes=["EFFECTIVE_SAMPLE_UNKNOWN"],
                    detail="Effective sample size unknown."), blk, gap
    if eff < req:
        return _dim("effective_sample", D_FAIL, value=eff, required=req,
                    reason_codes=["RESEARCH_SAMPLE_TOO_SMALL"],
                    detail="Effective sample below the research adequacy floor."), blk, gap
    return _dim("effective_sample", D_PASS, value=eff, required=req,
                detail="Effective sample meets the adequacy floor."), blk, gap


def _eval_revision(ds: dict, req: dict, pol: dict):
    requires_rev = bool(req.get("requires_historical_revisions"))
    rev = _b(ds.get("revision_history_support"))
    current_consensus = _b(ds.get("estimates_are_current_consensus"))
    pit = _u(ds.get("pit_guarantee"))
    blk, gap = [], []
    if requires_rev:
        historical_ok = (rev is True and current_consensus is not True
                         and pit != "CURRENT_ONLY")
        if historical_ok:
            return _dim("revision_history", D_PASS, value=True, required=True,
                        detail="Actual historical revision snapshots (not reconstructed "
                               "current consensus)."), blk, gap
        blk.append(_blocker("CURRENT_ONLY_ESTIMATES_FOR_REVISION_RESEARCH", TIER_DISQUALIFYING,
                            "revision_history",
                            "Current-consensus estimates cannot serve historical-revision research."))
        return _dim("revision_history", D_FAIL, value=rev, required=True,
                    reason_codes=["CURRENT_ONLY_ESTIMATES_FOR_REVISION_RESEARCH"],
                    detail="No true historical revision snapshots for revision research."), blk, gap
    if rev is True:
        return _dim("revision_history", D_PASS, value=True,
                    detail="Revision history available (not required)."), blk, gap
    if rev is False:
        return _dim("revision_history", D_PASS, value=False,
                    reason_codes=["REVISION_HISTORY_NOT_APPLICABLE"],
                    detail="No revision history (not required by the research)."), blk, gap
    gap.append(_gap("REVISION_HISTORY_UNKNOWN", "revision_history",
                    "Revision-history support unspecified (not required).", by_design=True))
    return _dim("revision_history", D_UNKNOWN, value=None,
                detail="Revision-history support unknown (not required)."), blk, gap


def _eval_freshness(ds: dict, req: dict, pol: dict):
    freq = _u(ds.get("update_frequency"))
    lag = _i(ds.get("last_update_lag_days"))
    blk, gap = [], []
    fresh_map = {"REALTIME": D_PASS, "INTRADAY": D_PASS, "DAILY": D_PASS,
                 "WEEKLY": D_PASS, "MONTHLY": D_WATCH, "QUARTERLY": D_WATCH,
                 "IRREGULAR": D_WATCH}
    state = fresh_map.get(freq, D_UNKNOWN if freq is None else D_WATCH)
    rc = []
    if freq is None:
        gap.append(_gap("UPDATE_FREQUENCY_UNKNOWN", "update_frequency_freshness",
                        "Update frequency unspecified."))
        rc.append("UPDATE_FREQUENCY_UNKNOWN")
    if lag is not None and lag > pol["max_acceptable_staleness_days"]:
        state = D_WATCH
        rc.append("FRESHNESS_STALE")
        gap.append(_gap("FRESHNESS_STALE", "update_frequency_freshness",
                        "Last-update lag %sd exceeds %sd." % (lag, pol["max_acceptable_staleness_days"])))
    return _dim("update_frequency_freshness", state, value={"update_frequency": freq,
                "last_update_lag_days": lag}, reason_codes=rc,
                detail="Update frequency / freshness (soft — never a hard blocker)."), blk, gap


def _eval_identifier(ds: dict, req: dict, pol: dict):
    idq = _u(ds.get("identifier_quality"))
    mapping = _b(ds.get("identifier_mapping_available"))
    dup = _b(ds.get("duplicate_identifiers"))
    blk, gap = [], []
    if dup is True:
        gap.append(_gap("DUPLICATE_IDENTIFIERS", "identifier_quality",
                        "Duplicate identifiers observed in the dataset."))
    if idq == "NONE" or mapping is False:
        blk.append(_blocker("NO_RELIABLE_IDENTIFIER_MAPPING", TIER_DISQUALIFYING,
                            "identifier_quality",
                            "No reliable identifier mapping to the research universe."))
        return _dim("identifier_quality", D_FAIL, value=idq or ("mapping=%s" % mapping),
                    reason_codes=["NO_RELIABLE_IDENTIFIER_MAPPING"]
                    + (["DUPLICATE_IDENTIFIERS"] if dup is True else []),
                    detail="No reliable identifier mapping."), blk, gap
    if idq in ("STRONG", "MODERATE"):
        st = D_WATCH if dup is True else D_PASS
        return _dim("identifier_quality", st, value=idq,
                    reason_codes=(["DUPLICATE_IDENTIFIERS"] if dup is True else []),
                    detail="Identifier mapping is defensible."), blk, gap
    if idq == "WEAK":
        gap.append(_gap("IDENTIFIER_QUALITY_WEAK", "identifier_quality",
                        "Identifier quality is weak."))
        return _dim("identifier_quality", D_WATCH, value=idq,
                    reason_codes=["IDENTIFIER_QUALITY_WEAK"]
                    + (["DUPLICATE_IDENTIFIERS"] if dup is True else []),
                    detail="Weak identifier quality."), blk, gap
    blk.append(_blocker("IDENTIFIER_QUALITY_UNKNOWN", TIER_PURCHASE, "identifier_quality",
                        "Identifier quality is unspecified; mapping cannot be confirmed."))
    gap.append(_gap("IDENTIFIER_QUALITY_UNKNOWN", "identifier_quality",
                    "Identifier quality unspecified."))
    return _dim("identifier_quality", D_UNKNOWN, value=idq,
                reason_codes=["IDENTIFIER_QUALITY_UNKNOWN"],
                detail="Identifier quality unknown."), blk, gap


def _eval_survivorship(ds: dict, req: dict, pol: dict):
    risk = _u(ds.get("survivorship_bias_risk"))
    support = _b(ds.get("inactive_delisted_support"))
    integ = bool(req.get("requires_full_universe_integrity"))
    blk, gap = [], []
    survivorship_only = risk == "SURVIVORSHIP_ONLY" or support is False
    if survivorship_only:
        if integ:
            blk.append(_blocker("SURVIVORSHIP_ONLY_HISTORY", TIER_DISQUALIFYING,
                                "survivorship_bias_risk",
                                "Survivorship-only history while the research requires full "
                                "historical universe integrity."))
            return _dim("survivorship_bias_risk", D_FAIL, value=risk or "SURVIVORSHIP_ONLY",
                        required="FULL_UNIVERSE_INTEGRITY",
                        reason_codes=["SURVIVORSHIP_ONLY_HISTORY"],
                        detail="Survivorship-only history disqualifies integrity research."), blk, gap
        gap.append(_gap("SURVIVORSHIP_BIAS_RISK", "survivorship_bias_risk",
                        "Survivorship-only history (integrity not required)."))
        return _dim("survivorship_bias_risk", D_WATCH, value=risk or "SURVIVORSHIP_ONLY",
                    reason_codes=["SURVIVORSHIP_BIAS_RISK"],
                    detail="Survivorship-only history; acceptable only without integrity need."), blk, gap
    if risk == "HIGH":
        gap.append(_gap("SURVIVORSHIP_BIAS_RISK", "survivorship_bias_risk",
                        "High survivorship-bias risk."))
        return _dim("survivorship_bias_risk", D_WATCH, value=risk,
                    reason_codes=["SURVIVORSHIP_BIAS_RISK"],
                    detail="Elevated survivorship-bias risk."), blk, gap
    if risk in ("NONE", "LOW"):
        return _dim("survivorship_bias_risk", D_PASS, value=risk,
                    detail="Low survivorship-bias risk."), blk, gap
    gap.append(_gap("SURVIVORSHIP_RISK_UNKNOWN", "survivorship_bias_risk",
                    "Survivorship-bias risk unspecified."))
    return _dim("survivorship_bias_risk", D_UNKNOWN, value=risk,
                detail="Survivorship-bias risk unknown."), blk, gap


def _eval_restatement(ds: dict, req: dict, pol: dict):
    beh = _u(ds.get("restatement_backfill_behavior"))
    blk, gap = [], []
    if beh == "PIT_PRESERVED":
        return _dim("restatement_backfill_behavior", D_PASS, value=beh,
                    detail="Restatements preserve the point-in-time record."), blk, gap
    if beh in ("SILENT_BACKFILL", "RESTATED", "BACKFILLED"):
        gap.append(_gap("RESTATEMENT_SILENT_BACKFILL_PIT_RISK", "restatement_backfill_behavior",
                        "Restatements silently backfill history (point-in-time risk)."))
        return _dim("restatement_backfill_behavior", D_WATCH, value=beh,
                    reason_codes=["RESTATEMENT_SILENT_BACKFILL_PIT_RISK"],
                    detail="Silent restatement/backfill introduces point-in-time risk."), blk, gap
    gap.append(_gap("RESTATEMENT_BEHAVIOR_UNKNOWN", "restatement_backfill_behavior",
                    "Restatement/backfill behaviour unspecified.", by_design=True))
    return _dim("restatement_backfill_behavior", D_UNKNOWN, value=beh,
                detail="Restatement/backfill behaviour unknown."), blk, gap


def _eval_licensing(ds: dict, req: dict, pol: dict):
    lic = dict(ds.get("licensing") or {})
    prohibited = _b(lic.get("prohibited"))
    research_use = _b(lic.get("research_use_allowed"))
    blk, gap = [], []
    # Surface every notable restriction as a visible constraint (never silently dropped).
    restriction_map = {
        "internal_commercial_use_allowed": "COMMERCIAL_USE_RESTRICTED",
        "redistribution_allowed": "REDISTRIBUTION_RESTRICTED",
        "derived_data_allowed": "DERIVED_DATA_RESTRICTED",
        "model_training_allowed": "MODEL_TRAINING_RESTRICTED",
    }
    restriction_codes = []
    for field, code in restriction_map.items():
        if _b(lic.get(field)) is False:
            restriction_codes.append(code)
            gap.append(_gap(code, "licensing",
                            "Licence restricts %s (acceptable for paper research use, recorded)."
                            % field.replace("_allowed", "").replace("_", " ")))
    if prohibited is True:
        blk.append(_blocker("PROHIBITED_LICENSE", TIER_DISQUALIFYING, "licensing",
                            "Licensing prohibits use of this dataset."))
        return _dim("licensing", D_FAIL, value="PROHIBITED",
                    reason_codes=["PROHIBITED_LICENSE"] + restriction_codes,
                    detail="Prohibited licensing."), blk, gap
    if research_use is False:
        blk.append(_blocker("LICENSE_PROHIBITS_RESEARCH_USE", TIER_DISQUALIFYING, "licensing",
                            "Licence does not permit research use."))
        return _dim("licensing", D_FAIL, value="NO_RESEARCH_USE",
                    reason_codes=["LICENSE_PROHIBITS_RESEARCH_USE"] + restriction_codes,
                    detail="Licence forbids research use."), blk, gap
    if research_use is None:
        blk.append(_blocker("UNKNOWN_LICENSE", TIER_PURCHASE, "licensing",
                            "Licensing terms are unclear; a purchase cannot be recommended."))
        gap.append(_gap("UNKNOWN_LICENSE", "licensing",
                        "Licensing / redistribution terms are unclear."))
        return _dim("licensing", D_UNKNOWN, value=None,
                    reason_codes=["UNKNOWN_LICENSE"] + restriction_codes,
                    detail="Licensing unclear — blocks any purchase recommendation."), blk, gap
    st = D_WATCH if restriction_codes else D_PASS
    return _dim("licensing", st, value="RESEARCH_USE_ALLOWED",
                reason_codes=restriction_codes,
                detail="Research use permitted; restrictions recorded."), blk, gap


def _eval_cost(ds: dict, req: dict, pol: dict):
    cost = dict(ds.get("cost") or {})
    annual = _f(cost.get("annual_usd"))
    if annual is None:
        monthly = _f(cost.get("monthly_usd"))
        if monthly is not None:
            annual = monthly * 12.0
    one_time = _f(cost.get("one_time_usd"))
    cost_known = _b(cost.get("cost_known"))
    budget = pol["annual_cost_budget_usd"]
    blk, gap = [], []
    known = annual is not None or (cost_known is True) or (one_time is not None and annual is not None)
    if annual is None and cost_known is not True:
        blk.append(_blocker("UNKNOWN_COST", TIER_PURCHASE, "acquisition_cost",
                            "Acquisition cost is unknown; a purchase cannot be recommended."))
        gap.append(_gap("UNKNOWN_COST", "acquisition_cost",
                        "No acquisition cost supplied (explicit UNKNOWN_COST)."))
        return _dim("acquisition_cost", D_UNKNOWN, value={"annual_usd": annual,
                    "one_time_usd": one_time}, reason_codes=["UNKNOWN_COST"],
                    detail="Acquisition cost unknown."), blk, gap
    if annual is not None and annual > budget:
        blk.append(_blocker("COST_ABOVE_BUDGET", TIER_PURCHASE, "acquisition_cost",
                            "Estimated annual cost $%.0f exceeds the $%.0f budget." % (annual, budget)))
        gap.append(_gap("COST_ABOVE_BUDGET", "acquisition_cost",
                        "Estimated annual cost above the policy budget."))
        return _dim("acquisition_cost", D_WATCH, value={"annual_usd": annual,
                    "one_time_usd": one_time}, required={"annual_budget_usd": budget},
                    reason_codes=["COST_ABOVE_BUDGET"],
                    detail="Cost above budget."), blk, gap
    return _dim("acquisition_cost", D_PASS, value={"annual_usd": annual,
                "one_time_usd": one_time}, required={"annual_budget_usd": budget},
                detail="Acquisition cost known and within budget."), blk, gap


def _eval_incremental(facts: dict, pol: dict):
    corr = facts["max_abs_correlation_with_existing"]
    blk, gap = [], []
    if corr is None:
        gap.append(_gap("INCREMENTAL_INFORMATION_UNKNOWN", "incremental_information",
                        "No correlation/redundancy evidence versus existing owned features."))
        return _dim("incremental_information", D_UNKNOWN, value=None,
                    reason_codes=["INCREMENTAL_INFORMATION_UNKNOWN"],
                    detail="Incremental information versus owned data unknown."), blk, gap
    if facts["redundant"]:
        if facts["material_lift"]:
            blk.append(_blocker("REDUNDANCY_HIGH_DESPITE_LIFT", TIER_PURCHASE,
                                "incremental_information",
                                "Highly correlated with owned data despite measured lift."))
            gap.append(_gap("REDUNDANCY_HIGH_DESPITE_LIFT", "incremental_information",
                            "High redundancy with owned data despite lift."))
            return _dim("incremental_information", D_WATCH,
                        value=_r(corr, 3), required=pol["redundancy_correlation_ceiling"],
                        reason_codes=["REDUNDANCY_HIGH_DESPITE_LIFT"],
                        detail="High redundancy with owned data (lift present)."), blk, gap
        blk.append(_blocker("REDUNDANT_NO_LIFT", TIER_DISQUALIFYING, "incremental_information",
                            "Materially duplicates already-owned information without measurable lift."))
        return _dim("incremental_information", D_FAIL, value=_r(corr, 3),
                    required=pol["redundancy_correlation_ceiling"],
                    reason_codes=["REDUNDANT_NO_LIFT"],
                    detail="Redundant with owned data and no measurable lift."), blk, gap
    return _dim("incremental_information", D_PASS, value=_r(corr, 3),
                required=pol["redundancy_correlation_ceiling"],
                detail="Economically distinct from already-owned information."), blk, gap


def _eval_lift(facts: dict, pol: dict):
    blk, gap = [], []
    if not facts["available"]:
        gap.append(_gap("MEASURED_LIFT_UNAVAILABLE", "measured_research_lift",
                        "No measured research evidence supplied."))
        return _dim("measured_research_lift", D_UNKNOWN, value=None,
                    reason_codes=["MEASURED_LIFT_UNAVAILABLE"],
                    detail="No measured research lift available."), blk, gap
    val = {"rank_ic_lift": facts["rank_ic_lift"],
           "decile_spread_lift_pp": facts["decile_spread_lift_pp"],
           "challenger_excess_lift_pp": facts["challenger_excess_lift_pp"],
           "cost_adjusted_lift_pp": facts["cost_adjusted_lift_pp"],
           "effective_sample": facts["effective_sample"]}
    if facts["in_sample_only"]:
        gap.append(_gap("IN_SAMPLE_ONLY_EVIDENCE", "measured_research_lift",
                        "Evidence is in-sample only; no purchase on in-sample evidence."))
        return _dim("measured_research_lift", D_WATCH, value=val,
                    reason_codes=["IN_SAMPLE_ONLY_EVIDENCE"],
                    detail="In-sample-only lift; not decision-grade."), blk, gap
    if not facts["material_lift"]:
        return _dim("measured_research_lift", D_FAIL, value=val,
                    reason_codes=["NO_MATERIAL_LIFT"],
                    detail="No material out-of-sample lift versus the champion."), blk, gap
    rc = []
    if facts["regime_robust"] is not True:
        rc.append("REGIME_ROBUSTNESS_NOT_ESTABLISHED")
    if facts["sector_robust"] is not True:
        rc.append("SECTOR_ROBUSTNESS_NOT_ESTABLISHED")
    if not facts["turnover_ok"]:
        rc.append("TURNOVER_DETERIORATION")
    if not facts["cost_adjusted_ok"]:
        rc.append("COST_ADJUSTED_LIFT_NOT_ESTABLISHED")
    if facts["robust"]:
        return _dim("measured_research_lift", D_PASS, value=val,
                    detail="Robust, out-of-sample, cost-adjusted lift across regime & sector."), blk, gap
    for code in rc:
        gap.append(_gap(code, "measured_research_lift", "Robustness gap: %s." % code))
    return _dim("measured_research_lift", D_WATCH, value=val, reason_codes=rc,
                detail="Material lift present but not fully robust."), blk, gap


def _eval_implementation(ds: dict, req: dict, pol: dict):
    cx = _u(ds.get("implementation_complexity")) or _u((ds.get("cost") or {}).get("implementation_effort"))
    blk, gap = [], []
    if cx in ("LOW",):
        return _dim("implementation_complexity", D_PASS, value=cx,
                    detail="Low integration complexity."), blk, gap
    if cx in ("MEDIUM",):
        return _dim("implementation_complexity", D_PASS, value=cx,
                    reason_codes=["IMPLEMENTATION_COMPLEXITY_MEDIUM"],
                    detail="Moderate integration complexity."), blk, gap
    if cx in ("HIGH", "VERY_HIGH"):
        gap.append(_gap("IMPLEMENTATION_COMPLEXITY_HIGH", "implementation_complexity",
                        "High integration complexity / effort."))
        return _dim("implementation_complexity", D_WATCH, value=cx,
                    reason_codes=["IMPLEMENTATION_COMPLEXITY_HIGH"],
                    detail="High integration complexity."), blk, gap
    gap.append(_gap("IMPLEMENTATION_COMPLEXITY_UNKNOWN", "implementation_complexity",
                    "Integration complexity unspecified.", by_design=True))
    return _dim("implementation_complexity", D_UNKNOWN, value=cx,
                detail="Integration complexity unknown."), blk, gap


def _eval_operational(ds: dict, req: dict, pol: dict):
    rel = _u(ds.get("operational_reliability"))
    blk, gap = [], []
    if rel == "HIGH":
        return _dim("operational_reliability", D_PASS, value=rel,
                    detail="High operational reliability."), blk, gap
    if rel == "MEDIUM":
        return _dim("operational_reliability", D_PASS, value=rel,
                    reason_codes=["OPERATIONAL_RELIABILITY_MEDIUM"],
                    detail="Moderate operational reliability."), blk, gap
    if rel == "LOW":
        gap.append(_gap("OPERATIONAL_RELIABILITY_LOW", "operational_reliability",
                        "Low operational reliability / SLA risk."))
        return _dim("operational_reliability", D_WATCH, value=rel,
                    reason_codes=["OPERATIONAL_RELIABILITY_LOW"],
                    detail="Low operational reliability."), blk, gap
    gap.append(_gap("OPERATIONAL_RELIABILITY_UNKNOWN", "operational_reliability",
                    "Operational reliability unspecified.", by_design=True))
    return _dim("operational_reliability", D_UNKNOWN, value=rel,
                detail="Operational reliability unknown."), blk, gap


# --------------------------------------------------------------------------- #
# RESEARCH_ACQUISITION (Stage A) evaluators — evaluated ONLY in that context.
# --------------------------------------------------------------------------- #
def _eval_capability(acq: dict, pol: dict):
    """What research capability does acquiring this dataset actually unlock?

    Stage A's central question, and the one Stage B never has to ask. A dataset that unlocks
    no named capability cannot be worth paying to learn from, however clean it is; a dataset
    whose capability was never declared is unproven rather than good.
    """
    count = _i(acq.get("capability_unlocked_count"))
    ceiling = _i(acq.get("capability_unlocked_ceiling_count"))
    detail = acq.get("capability_unlocked_detail")
    units = acq.get("capability_unlocked_units")
    floor = _i(pol.get("acquisition_min_capability_unlocked")) or 1
    blk, gap = [], []
    if count is None:
        blk.append(_blocker("CAPABILITY_UNLOCKED_NOT_DECLARED", TIER_EVIDENCE, ACQ_CAPABILITY,
                            "No declared capability this acquisition would unlock."))
        gap.append(_gap("CAPABILITY_UNLOCKED_NOT_DECLARED", ACQ_CAPABILITY,
                        "The acquisition case did not declare what the dataset unlocks."))
        return _dim(ACQ_CAPABILITY, D_UNKNOWN, value=None, required=floor,
                    reason_codes=["CAPABILITY_UNLOCKED_NOT_DECLARED"],
                    detail="Unlocked capability undeclared."), blk, gap
    if count < floor:
        # A dataset that unlocks the capability only at a weaker implementation level is
        # LIMITED, not worthless: that is a reported constraint which caps the result below a
        # recommendation, and only a dataset that unlocks nothing at any level is disqualified.
        if ceiling is not None and ceiling >= floor:
            blk.append(_blocker("CAPABILITY_ONLY_AT_CEILING", TIER_PURCHASE, ACQ_CAPABILITY,
                                "Unlocks %s capabilities only at the weaker ceiling level and "
                                "%s fully." % (ceiling, count)))
            gap.append(_gap("CAPABILITY_ONLY_AT_CEILING", ACQ_CAPABILITY,
                            "The declared capability is reachable only at a weaker "
                            "implementation level than the research requires."))
            return _dim(ACQ_CAPABILITY, D_WATCH,
                        value={"count": count, "ceiling": ceiling, "units": units,
                               "detail": detail},
                        required=floor, reason_codes=["CAPABILITY_ONLY_AT_CEILING"],
                        detail="Capability unlocked only at the ceiling level."), blk, gap
        blk.append(_blocker("NO_CAPABILITY_UNLOCKED", TIER_DISQUALIFYING, ACQ_CAPABILITY,
                            "The acquisition unlocks %s declared capabilities, below the "
                            "floor of %s, at any implementation level." % (count, floor)))
        return _dim(ACQ_CAPABILITY, D_FAIL, value={"count": count, "ceiling": ceiling},
                    required=floor, reason_codes=["NO_CAPABILITY_UNLOCKED"],
                    detail="Acquiring this dataset would unlock nothing that is blocked."), blk, gap
    if not detail:
        gap.append(_gap("CAPABILITY_DETAIL_MISSING", ACQ_CAPABILITY,
                        "A capability count was declared without naming what it consists of."))
        return _dim(ACQ_CAPABILITY, D_WATCH, value={"count": count, "ceiling": ceiling,
                    "units": units}, required=floor,
                    reason_codes=["CAPABILITY_DETAIL_MISSING"],
                    detail="Capability counted but not named."), blk, gap
    return _dim(ACQ_CAPABILITY, D_PASS, value={"count": count, "ceiling": ceiling,
                "units": units, "detail": detail}, required=floor,
                detail="The acquisition unlocks a named, counted research capability."), blk, gap


def _eval_expected_distinctness(acq: dict, pol: dict):
    """Is the dataset EXPECTED to be economically distinct from what is already owned?

    Stage B measures this as a correlation. Stage A has no data to correlate, so the caller
    declares an expectation and says what it rests on — and an undeclared expectation caps
    the result below a recommendation rather than passing quietly.
    """
    declared = _u(acq.get("expected_incremental_distinctness")) or "UNKNOWN"
    basis = acq.get("expected_distinctness_basis")
    tried = _b(acq.get("owned_substitute_tried"))
    bounded = _b(acq.get("bounded_evaluation_declared"))
    floor = tuple(pol.get("acquisition_min_distinctness") or ("HIGH", "MEDIUM"))
    blk, gap = [], []
    rc = []

    if declared not in DISTINCTNESS_VOCAB:
        declared = "UNKNOWN"
    if declared in ("LOW", "NONE"):
        blk.append(_blocker("NOT_ECONOMICALLY_DISTINCT_FROM_OWNED_DATA", TIER_DISQUALIFYING,
                            ACQ_DISTINCTNESS,
                            "Declared distinctness %s duplicates already-owned information."
                            % declared))
        return _dim(ACQ_DISTINCTNESS, D_FAIL, value=declared, required=list(floor),
                    reason_codes=["NOT_ECONOMICALLY_DISTINCT_FROM_OWNED_DATA"],
                    detail="Expected to duplicate owned data."), blk, gap
    if declared not in floor:
        blk.append(_blocker("EXPECTED_DISTINCTNESS_NOT_DECLARED", TIER_PURCHASE,
                            ACQ_DISTINCTNESS,
                            "Expected incremental distinctness was not declared."))
        gap.append(_gap("EXPECTED_DISTINCTNESS_NOT_DECLARED", ACQ_DISTINCTNESS,
                        "No declared expectation of distinctness from owned data."))
        rc.append("EXPECTED_DISTINCTNESS_NOT_DECLARED")

    if pol.get("acquisition_requires_owned_substitute_tried") and tried is not True:
        blk.append(_blocker("OWNED_SUBSTITUTE_NOT_TRIED", TIER_PURCHASE, ACQ_DISTINCTNESS,
                            "No owned or free substitute has been tried and measured as "
                            "insufficient; owned data is used first."))
        gap.append(_gap("OWNED_SUBSTITUTE_NOT_TRIED", ACQ_DISTINCTNESS,
                        "The owned/free substitute was not tried before asking to buy."))
        rc.append("OWNED_SUBSTITUTE_NOT_TRIED")

    if pol.get("acquisition_requires_bounded_evaluation") and bounded is not True:
        blk.append(_blocker("NO_BOUNDED_EVALUATION_DECLARED", TIER_PURCHASE, ACQ_DISTINCTNESS,
                            "No bounded evaluation that could return DO_NOT_BUY was declared."))
        gap.append(_gap("NO_BOUNDED_EVALUATION_DECLARED", ACQ_DISTINCTNESS,
                        "An acquisition with no evaluation that can fail is open-ended."))
        rc.append("NO_BOUNDED_EVALUATION_DECLARED")

    if not basis and declared in floor:
        gap.append(_gap("EXPECTED_DISTINCTNESS_BASIS_MISSING", ACQ_DISTINCTNESS,
                        "A distinctness expectation was declared without naming its basis."))
        rc.append("EXPECTED_DISTINCTNESS_BASIS_MISSING")

    state = D_PASS if not rc else (D_UNKNOWN if declared not in floor else D_WATCH)
    return _dim(ACQ_DISTINCTNESS, state, value={"declared": declared, "basis": basis,
                "owned_substitute_tried": tried, "bounded_evaluation_declared": bounded},
                required=list(floor), reason_codes=rc,
                detail="Expected economic distinctness from already-owned data."), blk, gap


# --------------------------------------------------------------------------- #
# Entitlement / integration context
# --------------------------------------------------------------------------- #
def _ownership(ds: dict, own: dict) -> dict:
    ent = _u(ds.get("existing_entitlement_state")) or _u(own.get("entitlement_state"))
    acq = _u(ds.get("acquisition_state"))
    integ = _u(ds.get("technical_integration_state"))
    owned = ent in ("OWNED", "ENTITLED") or acq in ("PURCHASED",)
    integrated = integ in ("INTEGRATED",)
    return {"entitlement_state": ent, "acquisition_state": acq,
            "technical_integration_state": integ, "already_owned": bool(owned),
            "already_integrated": bool(integrated)}


# --------------------------------------------------------------------------- #
# Classification (deterministic decision tree)
# --------------------------------------------------------------------------- #
def _classify(*, blockers: list, facts: dict, ownership: dict) -> tuple[str, list[str]]:
    fatal = [b for b in blockers if b["tier"] == TIER_DISQUALIFYING]
    evidence_b = [b for b in blockers if b["tier"] == TIER_EVIDENCE]
    purchase_b = [b for b in blockers if b["tier"] == TIER_PURCHASE]
    reasons: list[str] = []

    if fatal:
        reasons.append("DISQUALIFYING_BLOCKER_PRESENT")
        return REC_REJECT, reasons + [b["code"] for b in fatal]

    # Evidence must be sufficient (available, out-of-sample, adequate sample, PIT/history
    # known) before any value judgement is possible.
    if not facts["available"]:
        return REC_INSUFFICIENT, ["EVIDENCE_UNAVAILABLE"]
    if facts["out_of_sample"] is not True:
        return REC_INSUFFICIENT, ["OUT_OF_SAMPLE_EVIDENCE_REQUIRED"]
    if not facts["sample_ok"]:
        return REC_INSUFFICIENT, ["RESEARCH_SAMPLE_TOO_SMALL"]
    if evidence_b:
        return REC_INSUFFICIENT, [b["code"] for b in evidence_b]

    # Hard gates pass and evidence is decision-grade.
    if not facts["material_lift"]:
        reasons.append("NO_MATERIAL_LIFT_BUT_DISTINCT_INFORMATION")
        return REC_RESEARCH_ONLY, reasons

    if purchase_b or not facts["robust"]:
        reasons.append("MATERIAL_LIFT_WITH_OUTSTANDING_GAPS")
        return REC_CANDIDATE, reasons + [b["code"] for b in purchase_b]

    if ownership["already_owned"] and not ownership["already_integrated"]:
        reasons.append("ROBUST_LIFT_ALREADY_ENTITLED_INTEGRATION_WARRANTED")
        return REC_INTEGRATION, reasons
    if ownership["already_owned"] and ownership["already_integrated"]:
        reasons.append("ROBUST_LIFT_ENTITLED_AND_INTEGRATED_CONTINUE")
        return REC_INTEGRATION, reasons
    reasons.append("ROBUST_OUT_OF_SAMPLE_LIFT_ALL_HARD_GATES_PASS")
    return REC_PURCHASE, reasons


def _classify_acquisition(*, blockers: list, ownership: dict) -> tuple[str, list[str]]:
    """Stage A — "is it worth paying to learn?" (deterministic decision tree).

    The ONE thing this classifier does differently from ``_classify`` is that it does not
    require measured research lift, because at this stage measured lift cannot exist. Every
    other gate binds exactly as hard: a disqualifying data-integrity blocker still REJECTs, an
    unproven dataset is still INSUFFICIENT_EVIDENCE, and an unknown cost, unclear licence,
    undeclared distinctness, untried owned substitute or absent bounded evaluation still caps
    the result at CANDIDATE. It is not a softer gate — it is a gate asked a different question.
    """
    fatal = [b for b in blockers if b["tier"] == TIER_DISQUALIFYING]
    evidence_b = [b for b in blockers if b["tier"] == TIER_EVIDENCE]
    purchase_b = [b for b in blockers if b["tier"] == TIER_PURCHASE]
    reasons: list[str] = []

    # Ownership is settled BEFORE any other test, and only in this context. Stage A asks
    # whether to ACQUIRE, and there is no acquisition decision to take about something the
    # estate already holds — whatever else is true of it, that is an integration question and
    # the post-acquisition context is where it belongs. Every blocker found on the way is
    # still carried in the reason codes, so nothing is hidden by the short circuit.
    if ownership["already_owned"]:
        reasons.append("ALREADY_ENTITLED_NO_ACQUISITION_DECISION_TO_TAKE")
        return REC_ALREADY_ENTITLED, reasons + sorted({b["code"] for b in blockers})

    if fatal:
        reasons.append("DISQUALIFYING_BLOCKER_PRESENT")
        return REC_REJECT, reasons + [b["code"] for b in fatal]

    # Dataset PROPERTIES must be proven even though its lift cannot be. Missing point-in-time
    # metadata, unknown historical depth or an undeclared capability are all "we do not know",
    # and "we do not know" never buys anything.
    if evidence_b:
        return REC_INSUFFICIENT, sorted({b["code"] for b in evidence_b})

    if purchase_b:
        reasons.append("ACQUISITION_CASE_HAS_OUTSTANDING_GAPS")
        return REC_CANDIDATE, reasons + sorted({b["code"] for b in purchase_b})

    reasons.append("ALL_ACQUISITION_GATES_PASS_MEASURED_LIFT_NOT_REQUIRED_YET")
    return REC_RESEARCH_ACQUISITION, reasons


def _recommendation_block(state: str, reasons: list, blockers: list, facts: dict,
                          ownership: dict, context: str = DEFAULT_DECISION_CONTEXT) -> dict:
    manual = state in (REC_PURCHASE, REC_INTEGRATION, REC_RESEARCH_ACQUISITION)
    headline = {
        REC_REJECT: "REJECT — dataset not worth acquiring / integrating as evaluated.",
        REC_INSUFFICIENT: "INSUFFICIENT_EVIDENCE — cannot judge incremental value yet.",
        REC_RESEARCH_ONLY: "RESEARCH_ONLY — usable & distinct but no proven purchase-grade lift.",
        REC_CANDIDATE: "CANDIDATE — promising; outstanding gaps before a purchase recommendation.",
        REC_PURCHASE: "PURCHASE_RECOMMENDED — MANUAL APPROVAL REQUIRED.",
        REC_INTEGRATION: "INTEGRATION_RECOMMENDED — MANUAL APPROVAL REQUIRED.",
        REC_RESEARCH_ACQUISITION:
            "RESEARCH_ACQUISITION_RECOMMENDED — worth paying to LEARN; MANUAL APPROVAL "
            "REQUIRED. This is not alpha evidence and not integration approval.",
        REC_ALREADY_ENTITLED:
            "NO_ACQUISITION_REQUIRED_ALREADY_ENTITLED — the estate already holds this; the "
            "open question is integration, not acquisition.",
    }[state]
    is_acquisition = state == REC_RESEARCH_ACQUISITION
    return {
        "state": state,
        "decision_context": context,
        "vocabulary": list(VOCAB_FOR_CONTEXT.get(context, RECOMMENDATION_VOCAB)),
        "headline": headline,
        "reason_codes": sorted(set(reasons)),
        "manual_approval_required": True,
        "manual_approval_state": MANUAL_APPROVAL_PENDING,
        "auto_purchase_allowed": False,
        "auto_integration_allowed": False,
        "auto_provider_activation_allowed": False,
        "auto_acquisition_allowed": False,
        "is_purchase_recommendation": state == REC_PURCHASE,
        "is_integration_recommendation": state == REC_INTEGRATION,
        "is_research_acquisition_recommendation": is_acquisition,
        "is_alpha_evidence": False,
        "is_integration_approval": False,
        "requires_manual_purchase_approval": manual,
        "detail": headline + " No dataset is ever purchased, no provider ever activated and "
                  "no paid API ever called automatically — this is research governance only."
                  + (" A research-acquisition recommendation says the dataset is worth paying "
                     "to learn from; it says nothing about whether it will produce alpha, and "
                     "it grants no purchasing authority."
                     if is_acquisition else ""),
    }


# --------------------------------------------------------------------------- #
# Core entry point
# --------------------------------------------------------------------------- #
def resolve_decision_context(*, input_contract: Optional[dict] = None,
                             decision_context: Optional[str] = None) -> str:
    """Which question is being asked. Precedence: explicit argument → contract → LEGACY.

    An unrecognised value falls back to the legacy default rather than raising, because this
    kernel degrades to an explained result instead of crashing a caller — but the fallback is
    always the STRICTER, historical context, never the newer one.
    """
    raw = decision_context
    if raw is None:
        raw = (input_contract or {}).get("decision_context")
    ctx = _u(raw)
    return ctx if ctx in DECISION_CONTEXT_VOCAB else DEFAULT_DECISION_CONTEXT


def evaluate_dataset(*, input_contract: dict, policy: Optional[dict] = None,
                     decision_context: Optional[str] = None) -> dict:
    """Evaluate one dataset-expansion candidate (pure, deterministic).

    ``input_contract`` is the immutable dataset-evaluation contract (dataset metadata,
    research requirements and — for the post-acquisition context — measured research
    evidence). ``decision_context`` selects WHICH question is answered; omitting it preserves
    this kernel's historical ``POST_ACQUISITION_VALUE`` behaviour exactly, so no existing
    caller changes meaning. Returns the frozen evaluation result with one recommendation drawn
    from that context's vocabulary. Never raises on incomplete data — it degrades to BLOCKED /
    INSUFFICIENT_EVIDENCE with explicit reason codes, blockers and gaps, and NEVER fabricates a
    score when required data is absent. Purchases nothing, activates no provider, calls no
    paid API, in either context.
    """
    pol = dict(default_policy())
    if policy:
        pol.update(policy)

    ic = input_contract or {}
    context = resolve_decision_context(input_contract=ic, decision_context=decision_context)
    acquiring = context == CONTEXT_RESEARCH_ACQUISITION
    dataset_id = ic.get("dataset_id")
    provider = ic.get("provider")
    category = ic.get("data_category")
    ds = dict(ic.get("dataset") or {})
    req = dict(ic.get("research_requirements") or {})
    ev = dict(ic.get("evidence") or {})
    own = dict(ic.get("existing_ownership") or {})

    # --- core-input validation -> BLOCKED ------------------------------------ #
    core_blockers: list[dict] = []
    if not dataset_id:
        core_blockers.append(_blocker("MISSING_DATASET_ID", TIER_DISQUALIFYING, "catalog",
                                      "No dataset_id supplied."))
    if not provider:
        core_blockers.append(_blocker("MISSING_PROVIDER", TIER_DISQUALIFYING, "catalog",
                                      "No provider supplied."))
    if not category:
        core_blockers.append(_blocker("MISSING_DATA_CATEGORY", TIER_DISQUALIFYING, "catalog",
                                      "No data_category supplied."))
    if core_blockers:
        return _blocked_result(pol, ic, core_blockers, context=context)

    facts = _evidence_facts(ev, req, pol)
    ownership = _ownership(ds, own)
    acq = dict(ic.get("acquisition_case") or {})

    dims: list[dict] = []
    blockers: list[dict] = []
    gaps: list[dict] = []

    evaluators = [
        _eval_pit(ds, req, pol), _eval_history(ds, req, pol), _eval_inactive(ds, req, pol),
        _eval_universe(ds, req, pol), _eval_effective_sample(facts),
        _eval_revision(ds, req, pol), _eval_freshness(ds, req, pol),
        _eval_identifier(ds, req, pol), _eval_survivorship(ds, req, pol),
        _eval_restatement(ds, req, pol), _eval_licensing(ds, req, pol),
        _eval_cost(ds, req, pol), _eval_incremental(facts, pol), _eval_lift(facts, pol),
        _eval_implementation(ds, req, pol), _eval_operational(ds, req, pol),
    ]
    if acquiring:
        evaluators.extend([_eval_capability(acq, pol),
                           _eval_expected_distinctness(acq, pol)])
    for dim, blk, gap in evaluators:
        dims.append(dim)
        blockers.extend(blk)
        gaps.extend(gap)

    if acquiring:
        # Measured lift is UNAVAILABLE at this stage by definition, not missing by accident.
        # Recording it as a by-design gap keeps the artifact honest without letting the
        # absence be read either as a failure or as a pass.
        gaps.append(_gap("MEASURED_LIFT_NOT_REQUIRED_BEFORE_ACQUISITION",
                         "measured_research_lift",
                         "The research-acquisition context does not require measured lift: "
                         "the lift of data that has not been acquired cannot be measured.",
                         by_design=True))
        state, reasons = _classify_acquisition(blockers=blockers, ownership=ownership)
    else:
        state, reasons = _classify(blockers=blockers, facts=facts, ownership=ownership)
    recommendation = _recommendation_block(state, reasons, blockers, facts, ownership,
                                           context=context)

    dim_by_name = {d["dimension"]: d for d in dims}
    result = {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "policy_version": POLICY_VERSION,
        "dataset_id": dataset_id,
        "provider": provider,
        "data_category": category,
        "dataset_name": ic.get("dataset_name") or ds.get("dataset_name"),
        "recommendation": recommendation,
        "recommendation_state": state,
        "recommendation_vocabulary": list(VOCAB_FOR_CONTEXT.get(context,
                                                                RECOMMENDATION_VOCAB)),
        "decision_context": context,
        "decision_context_question": DECISION_CONTEXT_QUESTION[context],
        "decision_context_vocabulary": list(DECISION_CONTEXT_VOCAB),
        "decision_context_is_default": context == DEFAULT_DECISION_CONTEXT,
        "measured_lift_required": not acquiring,
        "evaluated_dimensions": list(DIMENSIONS_FOR_CONTEXT[context]),
        "acquisition_case": (acq if acquiring else None),
        "dimensions": dims,
        "dimension_summary": {d["dimension"]: d["state"] for d in dims},
        "failed_dimensions": sorted(d["dimension"] for d in dims if d["state"] == D_FAIL),
        "watch_dimensions": sorted(d["dimension"] for d in dims if d["state"] == D_WATCH),
        "unknown_dimensions": sorted(d["dimension"] for d in dims if d["state"] == D_UNKNOWN),
        "blockers": sorted(blockers, key=lambda b: (b["tier"], b["code"])),
        "disqualifying_blockers": sorted(b["code"] for b in blockers
                                         if b["tier"] == TIER_DISQUALIFYING),
        "evidence_blockers": sorted(b["code"] for b in blockers if b["tier"] == TIER_EVIDENCE),
        "purchase_blockers": sorted(b["code"] for b in blockers if b["tier"] == TIER_PURCHASE),
        "gaps": sorted(gaps, key=lambda g: (g["dimension"], g["code"])),
        "incremental_value": _incremental_value_block(facts, dim_by_name),
        "measured_lift": _measured_lift_block(facts, dim_by_name),
        "cost": _cost_block(dim_by_name.get("acquisition_cost")),
        "licensing": _licensing_block(dim_by_name.get("licensing")),
        "point_in_time": _pit_block(dim_by_name.get("point_in_time_integrity")),
        "ownership": ownership,
        "evidence_facts": _facts_summary(facts),
        "state_reason_codes": recommendation["reason_codes"],
        "manual_review": _manual_review(state),
        "policy": pol,
        "provenance": _provenance(ic, context=context),
        "safety": _safety(),
    }
    result["evaluation_hash"] = stable_hash(result)
    return result


# --------------------------------------------------------------------------- #
# Derived summary blocks
# --------------------------------------------------------------------------- #
def _incremental_value_block(facts: dict, dim_by_name: dict) -> dict:
    d = dim_by_name.get("incremental_information") or {}
    return {
        "state": d.get("state"),
        "max_abs_correlation_with_existing": facts["max_abs_correlation_with_existing"],
        "redundant": facts["redundant"],
        "distinct_information": facts["distinct_information"],
        "material_lift": facts["material_lift"],
        "detail": "Whether the dataset adds economically distinct information beyond owned data.",
    }


def _measured_lift_block(facts: dict, dim_by_name: dict) -> dict:
    d = dim_by_name.get("measured_research_lift") or {}
    return {
        "state": d.get("state"),
        "out_of_sample": facts["out_of_sample"],
        "in_sample_only": facts["in_sample_only"],
        "effective_sample": facts["effective_sample"],
        "rank_ic_lift": facts["rank_ic_lift"],
        "decile_spread_lift_pp": facts["decile_spread_lift_pp"],
        "challenger_excess_lift_pp": facts["challenger_excess_lift_pp"],
        "cost_adjusted_lift_pp": facts["cost_adjusted_lift_pp"],
        "turnover_delta": facts["turnover_delta"],
        "regime_robust": facts["regime_robust"],
        "sector_robust": facts["sector_robust"],
        "material_lift": facts["material_lift"],
        "robust": facts["robust"],
        "evidence_owner": facts["evidence_owner"],
        "study_reference": facts["study_reference"],
    }


def _cost_block(dim: Optional[dict]) -> dict:
    d = dim or {}
    val = d.get("value") or {}
    return {"state": d.get("state"), "annual_usd": (val or {}).get("annual_usd"),
            "one_time_usd": (val or {}).get("one_time_usd"),
            "reason_codes": d.get("reason_codes") or []}


def _licensing_block(dim: Optional[dict]) -> dict:
    d = dim or {}
    return {"state": d.get("state"), "value": d.get("value"),
            "reason_codes": d.get("reason_codes") or []}


def _pit_block(dim: Optional[dict]) -> dict:
    d = dim or {}
    return {"state": d.get("state"), "value": d.get("value"),
            "reason_codes": d.get("reason_codes") or []}


def _facts_summary(facts: dict) -> dict:
    return {k: facts[k] for k in (
        "available", "out_of_sample", "in_sample_only", "effective_sample",
        "min_effective_sample", "sample_ok", "material_lift", "redundant", "robust",
        "distinct_information", "turnover_ok", "cost_adjusted_ok")}


# --------------------------------------------------------------------------- #
# Manual review / safety / provenance / blocked result
# --------------------------------------------------------------------------- #
def _manual_review(state: str) -> dict:
    return {
        "manual_review_required": True,
        "manual_purchase_approval_required": state in (REC_PURCHASE, REC_INTEGRATION,
                                                       REC_RESEARCH_ACQUISITION),
        "manual_acquisition_approval_required": state == REC_RESEARCH_ACQUISITION,
        "automatic_purchase_allowed": False,
        "automatic_subscription_allowed": False,
        "automatic_provider_activation_allowed": False,
        "automatic_integration_allowed": False,
        "automatic_acquisition_allowed": False,
        "detail": ("Every data-expansion recommendation — in either decision context — is a "
                   "manual-approval governance recommendation; no dataset is acquired, "
                   "purchased or integrated automatically."),
    }


def _safety() -> dict:
    return {
        "read_only": True,
        "research_only": True,
        "paper_only": True,
        "manual_review": True,
        "purchased_dataset": False,
        "subscribed_provider": False,
        "activated_provider": False,
        "integrated_dataset": False,
        "called_paid_provider": False,
        "used_paid_api_quota": False,
        "altered_credentials": False,
        "modified_external_account": False,
        "mutated_portfolio": False,
        "promoted_model": False,
        "recalibrated_model": False,
        "created_order_plan": False,
        "created_orders": False,
        "created_fills": False,
        "changed_holdings": False,
        "changed_cash": False,
        "changed_nav": False,
        "wrote_to_database": False,
        "wrote_to_ledger": False,
        "enabled_cadence": False,
        "automation_off": True,
        "auto_purchase_allowed": False,
        "auto_integration_allowed": False,
        "auto_provider_activation_allowed": False,
        "auto_acquisition_allowed": False,
        "acquired_dataset": False,
        "safety_badges": ["RESEARCH ONLY", "MANUAL PURCHASE APPROVAL", "NO AUTO-PURCHASE",
                          "NO AUTO-ACQUISITION", "NO PROVIDER ACTIVATION",
                          "NO PORTFOLIO MUTATION"],
    }


def _provenance(ic: dict, *, context: str = DEFAULT_DECISION_CONTEXT) -> dict:
    return {
        "calculation_owner": CALCULATION_OWNER,
        "decision_context": context,
        "decision_context_question": DECISION_CONTEXT_QUESTION.get(context),
        "catalog_source": "api.data_expansion (dataset catalog + provider metadata)",
        "provenance_contract_source": "alpha_agent.source_contracts (normalized-record provenance)",
        "freshness_source": "api.data_freshness",
        "evidence_gate_source": "alpha_agent.experiment_contracts.DEFAULT_GATES",
        "analyst_revisions_candidate_source": "alpha_agent.analyst_revisions (Stage 13A)",
        "research_opportunity_source": "engine.research_agent (Slice 8 DATA opportunities)",
        "dataset_id": ic.get("dataset_id"),
        "provider": ic.get("provider"),
        "data_category": ic.get("data_category"),
        "input_schema_version": INPUT_SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
    }


def _blocked_result(pol: dict, ic: dict, blockers: list,
                    *, context: str = DEFAULT_DECISION_CONTEXT) -> dict:
    """A readable BLOCKED result for missing core inputs (never raises; degrade-safe)."""
    result = {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "policy_version": POLICY_VERSION,
        "dataset_id": ic.get("dataset_id"),
        "provider": ic.get("provider"),
        "data_category": ic.get("data_category"),
        "dataset_name": ic.get("dataset_name"),
        "recommendation": {
            "state": REC_INSUFFICIENT,
            "decision_context": context,
            "vocabulary": list(VOCAB_FOR_CONTEXT.get(context, RECOMMENDATION_VOCAB)),
            "headline": "BLOCKED — core dataset identity is incomplete.",
            "reason_codes": sorted({b["code"] for b in blockers}),
            "manual_approval_required": True,
            "manual_approval_state": MANUAL_APPROVAL_PENDING,
            "auto_purchase_allowed": False,
            "auto_integration_allowed": False,
            "auto_acquisition_allowed": False,
            "is_purchase_recommendation": False,
            "is_integration_recommendation": False,
            "is_research_acquisition_recommendation": False,
        },
        "recommendation_state": REC_INSUFFICIENT,
        "recommendation_vocabulary": list(VOCAB_FOR_CONTEXT.get(context,
                                                                RECOMMENDATION_VOCAB)),
        "decision_context": context,
        "decision_context_question": DECISION_CONTEXT_QUESTION.get(context),
        "decision_context_vocabulary": list(DECISION_CONTEXT_VOCAB),
        "decision_context_is_default": context == DEFAULT_DECISION_CONTEXT,
        "blocked": True,
        "dimensions": [],
        "dimension_summary": {},
        "failed_dimensions": [], "watch_dimensions": [], "unknown_dimensions": [],
        "blockers": blockers,
        "disqualifying_blockers": sorted({b["code"] for b in blockers}),
        "evidence_blockers": [], "purchase_blockers": [],
        "gaps": [_gap(b["code"], "catalog", b.get("detail")) for b in blockers],
        "incremental_value": {}, "measured_lift": {}, "cost": {}, "licensing": {},
        "point_in_time": {}, "ownership": {},
        "evidence_facts": {},
        "state_reason_codes": sorted({b["code"] for b in blockers}),
        "manual_review": _manual_review(REC_INSUFFICIENT),
        "policy": pol,
        "provenance": _provenance(ic, context=context),
        "safety": _safety(),
    }
    result["evaluation_hash"] = stable_hash(result)
    return result
