r"""Controlled Autonomous Research Execution Bridge — pure deterministic kernel.

This module is the ONE calculation owner for the bridge that connects the Persistent
Alpha Research Agent (the research GOVERNOR, ``engine.research_agent`` /
``api.research_agent``) to the existing AlphaAgent quantitative research lab
(``alpha_agent`` — ResearchQueue, tournament, gates, shadow books). It is a **pure,
deterministic kernel**: it performs NO I/O — no file, database, network, provider,
prediction, clock or RNG access — and never mutates its inputs. Everything it needs
arrives as immutable input dicts; everything it produces is an immutable result dict
with a stable ``*_hash``.

It owns five bridge calculations and NOTHING else:

  1. **Research Opportunity normalization** (:func:`normalize_opportunity`) — the
     Research Opportunity concept already EXISTS on ``engine.research_agent``
     (``_research_opportunities``). The bridge does NOT fork it; it wraps one existing
     opportunity in a provenance-complete envelope (champion identity, eligible market
     date, evidence-snapshot hash, degradation category, affected family, candidate
     research families, coherent horizons, data gaps, priority, lifecycle) so the same
     opportunity can be reproduced and audited.

  2. **Controlled Research Mandate** (:func:`build_mandate`) — converts an eligible,
     normalized opportunity into a BOUNDED statement of *what AlphaAgent is allowed to
     investigate* (hypothesis families, owned data families, economically-coherent
     horizons, candidate / experiment / generation / compute budgets, minimum-sample,
     PIT, cost, FDR, robustness, regime and orthogonality REQUIREMENTS). The mandate
     never specifies the answer AlphaAgent is expected to find.

  3. **Multi-horizon coherence** (:func:`coherent_horizons`, :func:`horizon_eligible`)
     — deterministic {1,5,20,63}-session eligibility per hypothesis family. A
     medium-term fundamental/quality/momentum factor is NOT automatically a 1-day
     hypothesis; an event/residual signal may justify 1/5-day testing. The AlphaAgent
     cross-sectional forward-label vocabulary is {5,20,63}; ``1`` is the execution /
     label-maturity lag, not a fourth research horizon; ``1/5/20/63`` is only the
     forward-prediction maturation vocabulary. This function keeps all three explicit.

  4. **Immutable research-result envelope** (:func:`build_result_envelope`) — one
     immutable envelope that REFERENCES the canonical underlying evidence (candidate /
     experiment ids, horizons, datasets, sample, metrics, FDR / robustness /
     orthogonality outcomes, data gaps, reproducibility hashes, terminal state) without
     duplicating it, plus the canonical **Alpha Strength Gate**
     (:func:`evaluate_alpha_strength`).

  5. **Persistent-Research-Agent re-assessment** (:func:`reassess`) — re-evaluates the
     originating opportunity using ONLY the returned canonical evidence. It may conclude
     the opportunity is unresolved, that more research is justified, DATA_HOLD, no
     defensible alpha, that a challenger warrants continued shadow research, or that a
     challenger meets the evidence gate for MANUAL model review. It may NOT promote,
     recalibrate, retrain, replace the champion, change holdings / targets / orders, or
     enable cadence.

The Alpha Strength Gate does NOT invent thresholds: it DELEGATES to the injected
canonical ``classify_fn`` (``alpha_agent.tournament.classify_evidence``) with the
injected ``gate_cfg`` (``configs/alpha_agent/stage9_tournament.json``), plus the
injected Benjamini-Hochberg FDR outcome and champion-correlation orthogonality cutoff.
Terminal outcomes are the EXISTING constants (``DATA_HOLD``, ``KEEP_FOR_RESEARCH``,
``READY_FOR_MANUAL_REVIEW``, ``REJECTED``, ``NO_DEFENSIBLE_ALPHA``) — never new strings.
``NO_DEFENSIBLE_ALPHA`` is a SUCCESSFUL scientific outcome; ``DATA_HOLD`` is a
SUCCESSFUL safety outcome. The gate never forces a challenger.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Callable, Optional

SCHEMA_VERSION = "research_bridge.v1"
MANDATE_SCHEMA_VERSION = "research_mandate.v1"
DISPATCH_SCHEMA_VERSION = "research_dispatch.v1"
ENVELOPE_SCHEMA_VERSION = "research_result_envelope.v1"
REASSESSMENT_SCHEMA_VERSION = "research_reassessment.v1"
POLICY_VERSION = "research_bridge_policy.v1"

CALCULATION_OWNER = "engine.research_bridge"

# --- Horizon vocabulary (three DISTINCT concepts kept explicit) -------------- #
# AlphaAgent cross-sectional forward-LABEL research vocabulary (stage12_*/analyst).
FORWARD_LABEL_HORIZONS = (5, 20, 63)
# Forward-PREDICTION maturation vocabulary (api.forward_prediction_skill, Phase 28B).
FORWARD_MATURITY_HORIZONS = (1, 5, 20, 63)
# Execution / label-maturity lag: entry is strictly >= this many sessions after
# formation/filing (alpha_agent.stage12_execution.entry_index). NOT a research horizon.
EXECUTION_LAG_SESSIONS = 1
ALL_HORIZONS = (1, 5, 20, 63)

# --- Mandate lifecycle vocabulary (mandate altitude; distinct from queue states) #
MANDATE_DRAFTED = "DRAFTED"
MANDATE_DISPATCHED = "DISPATCHED"
MANDATE_IN_PROGRESS = "IN_PROGRESS"
MANDATE_COMPLETED = "COMPLETED"
MANDATE_DATA_HOLD = "DATA_HOLD"
MANDATE_FAILED = "FAILED"
MANDATE_SUPERSEDED = "SUPERSEDED"
MANDATE_LIFECYCLE_VOCAB = (MANDATE_DRAFTED, MANDATE_DISPATCHED, MANDATE_IN_PROGRESS,
                           MANDATE_COMPLETED, MANDATE_DATA_HOLD, MANDATE_FAILED,
                           MANDATE_SUPERSEDED)

# --- Alpha-strength terminal outcomes (REUSE existing tournament constants) --- #
OUTCOME_MANUAL_REVIEW = "READY_FOR_MANUAL_REVIEW"   # tournament strongest state
OUTCOME_KEEP_FOR_RESEARCH = "KEEP_FOR_RESEARCH"
OUTCOME_DATA_HOLD = "DATA_HOLD"
OUTCOME_REJECTED = "REJECTED"
OUTCOME_NO_DEFENSIBLE_ALPHA = "NO_DEFENSIBLE_ALPHA"
OUTCOME_NOT_EVALUATED = "NOT_EVALUATED"
ALPHA_STRENGTH_OUTCOME_VOCAB = (OUTCOME_MANUAL_REVIEW, OUTCOME_KEEP_FOR_RESEARCH,
                                OUTCOME_DATA_HOLD, OUTCOME_REJECTED,
                                OUTCOME_NO_DEFENSIBLE_ALPHA, OUTCOME_NOT_EVALUATED)

# --- Re-assessment verdict vocabulary ---------------------------------------- #
REASSESS_UNRESOLVED = "OPPORTUNITY_UNRESOLVED"
REASSESS_MORE_RESEARCH = "MORE_RESEARCH_JUSTIFIED"
REASSESS_DATA_HOLD = "DATA_HOLD"
REASSESS_NO_DEFENSIBLE_ALPHA = "NO_DEFENSIBLE_ALPHA"
REASSESS_CONTINUE_SHADOW = "CHALLENGER_CONTINUE_SHADOW_RESEARCH"
REASSESS_MANUAL_REVIEW = "CHALLENGER_MEETS_GATE_FOR_MANUAL_MODEL_REVIEW"
REASSESS_VOCAB = (REASSESS_UNRESOLVED, REASSESS_MORE_RESEARCH, REASSESS_DATA_HOLD,
                  REASSESS_NO_DEFENSIBLE_ALPHA, REASSESS_CONTINUE_SHADOW,
                  REASSESS_MANUAL_REVIEW)

# --- Hypothesis family taxonomy (REUSE alpha_agent.signal_library.FAMILIES + the
#     stage12_registry event/residual economic families; never a parallel library) #
FAM_PRICE_MOMENTUM = "price_momentum"
FAM_REVERSAL_PATH = "reversal_path"
FAM_RISK_VOLATILITY = "risk_volatility"
FAM_LIQUIDITY_VOLUME = "liquidity_volume"
FAM_PROFITABILITY_QUALITY = "profitability_quality"
FAM_GROWTH_INVESTMENT = "growth_investment"
FAM_VALUATION = "valuation"
FAM_CROSS_FAMILY = "cross_family"
FAM_FUNDAMENTAL_CHANGE_EVENT = "fundamental_change_event"
FAM_POST_FILING_PRICE_REACTION = "post_filing_price_reaction"
FAM_RESIDUAL_PRICE = "residual_price"
HYPOTHESIS_FAMILIES = (
    FAM_PRICE_MOMENTUM, FAM_REVERSAL_PATH, FAM_RISK_VOLATILITY, FAM_LIQUIDITY_VOLUME,
    FAM_PROFITABILITY_QUALITY, FAM_GROWTH_INVESTMENT, FAM_VALUATION, FAM_CROSS_FAMILY,
    FAM_FUNDAMENTAL_CHANGE_EVENT, FAM_POST_FILING_PRICE_REACTION, FAM_RESIDUAL_PRICE,
)

# Owned data families the mandate may permit. This build permits OWNED data only;
# paid/expansion datasets (e.g. Intrinio) are never permitted here.
OWNED_DATA_FAMILIES = (
    "OWNED_PRICE_PANEL", "OWNED_UNIVERSE_SCORING", "OWNED_FORWARD_EVIDENCE",
    "OWNED_HOC_HISTORY", "OWNED_REALLOCATION_HISTORY", "OWNED_PIT_FUNDAMENTALS",
    "OWNED_PIT_SECTOR",
)

# The bridge NEVER promotes / retrains / mutates; every recommended action needs
# explicit manual governance approval (mirrors engine.research_agent.FORBIDDEN_ACTIONS).
FORBIDDEN_ACTIONS = (
    "AUTO_PROMOTE_MODEL", "AUTO_RECALIBRATE_CHAMPION", "AUTO_REPLACE_CHAMPION",
    "AUTO_RETRAIN_MODEL", "MUTATE_OPERATIONAL_PORTFOLIO", "CHANGE_TARGET_WEIGHTS",
    "CREATE_ORDERS", "CREATE_FILLS", "MUTATE_HOLDINGS_CASH_NAV", "ENABLE_CADENCE",
    "BROKER_EXECUTION", "ACTIVATE_PAID_DATA_PROVIDER", "PURCHASE_DATA",
)
MANUAL_APPROVAL_PENDING = "PENDING_MANUAL_APPROVAL"

# Which existing research-opportunity categories are DISPATCHABLE as bounded
# cross-sectional/portfolio studies vs OBSERVATIONAL (evidence accrual / inspection).
DISPATCHABLE_CATEGORIES = ("SIGNAL", "CHALLENGER", "PORTFOLIO")
OBSERVATIONAL_CATEGORIES = ("EVIDENCE", "DATA")


# --------------------------------------------------------------------------- #
# Policy — one explicit, versioned set of mandate bounds + gate REQUIREMENTS.
# Requirements marked ``authoritative`` MIRROR the Stage 9 tournament gate config
# (``configs/alpha_agent/stage9_tournament.json``) and are OVERRIDDEN by the api owner
# with the live value so no gate threshold is silently forked. Bounds marked ``new``
# are genuinely-new bridge policy (budget ceilings), declared once and folded into the
# mandate hash.
# --------------------------------------------------------------------------- #
def default_policy() -> dict[str, Any]:
    return {
        "policy_version": POLICY_VERSION,
        # --- bounded research budgets (new) ---------------------------------- #
        "max_candidate_families": 5,          # new: mandate breadth ceiling
        "max_candidates": 24,                 # new: mirrors stage11 max_survivors
        "max_experiments": 6,                 # new: mirrors stage9 max_new_experiments_per_cycle
        "max_campaign_generations": 2,        # new: bounded generations
        "compute_budget_seconds": 1800,       # new: cooperative wall-clock ceiling
        # --- minimum-sample / PIT requirements (authoritative: stage9 evidence_completeness)
        "min_universe_names_per_period": 20,  # authoritative
        "min_scored_periods": 12,             # authoritative
        "min_coverage_pct": 60.0,             # authoritative
        "require_point_in_time_valid": True,  # authoritative
        "require_survivorship_safe": True,    # authoritative
        "execution_lag_sessions": EXECUTION_LAG_SESSIONS,
        # --- statistical / robustness requirements (authoritative: stage9 gates) #
        "fdr_alpha": 0.05,                    # authoritative: selection_controls FDR alpha
        "keep_min_rank_ic_t": 2.0,            # authoritative: gates.keep_min_rank_ic_t
        "keep_min_net25_spread": 0.0,         # authoritative: gates.keep_min_net25_spread
        "keep_min_subperiod_consistency": 0.60,  # authoritative
        "keep_min_regime_consistency": 0.50,  # authoritative
        "max_abs_corr_vs_champion": 0.90,     # authoritative: manual_review_gate
        "min_incremental_net25_spread": 0.0,  # authoritative: manual_review_gate
        "min_forward_observations": 20,       # authoritative: manual_review_gate
        # --- cost assumption (authoritative: stage9 shadow_books.cost_bps_round_trip) #
        "cost_bps_round_trip": 50,            # authoritative
    }


# --------------------------------------------------------------------------- #
# Small deterministic helpers (stdlib only)
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


def _i(x: Any) -> int:
    v = _f(x)
    return int(v) if v is not None else 0


def _r(x: Optional[float], nd: int) -> Optional[float]:
    return None if x is None else round(float(x), nd)


_VOLATILE_KEYS = frozenset({"generated_at", "evaluated_at", "loaded_at", "built_at",
                            "dispatched_at", "ingested_at", "assessment_hash",
                            "mandate_hash", "dispatch_hash", "envelope_hash",
                            "reassessment_hash", "artifact"})


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


def _short(*parts: str) -> str:
    """Deterministic short id fragment (stdlib sha256; no RNG)."""
    return hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()


# Map the tournament's 8 authoritative lifecycle states onto the bridge's 4 gate buckets.
_LIFECYCLE_BUCKET = {
    "READY_FOR_MANUAL_REVIEW": OUTCOME_MANUAL_REVIEW,
    "KEEP_FOR_RESEARCH": OUTCOME_KEEP_FOR_RESEARCH,
    "SHADOW_BOOK_ACTIVE": OUTCOME_KEEP_FOR_RESEARCH,
    "REJECTED": OUTCOME_REJECTED,
    "ARCHIVED": OUTCOME_REJECTED,
    "DATA_HOLD": OUTCOME_DATA_HOLD,
    "PROPOSED": OUTCOME_DATA_HOLD,
    "TESTING": OUTCOME_DATA_HOLD,
}


def _bucket_lifecycle(state: Optional[str]) -> str:
    return _LIFECYCLE_BUCKET.get(state or "", OUTCOME_DATA_HOLD)


# --------------------------------------------------------------------------- #
# Multi-horizon economic coherence
# --------------------------------------------------------------------------- #
# Per-family economically-coherent research horizons (sessions). A quality/growth/
# valuation/fundamental factor is a MEDIUM/LONG hypothesis (20/63); price momentum is
# medium/long; reversal/liquidity are short/medium; event & residual signals justify
# short (1/5) testing. This is deliberately NOT "every family at every horizon".
_FAMILY_HORIZONS: dict[str, tuple] = {
    FAM_PRICE_MOMENTUM: (20, 63),
    FAM_REVERSAL_PATH: (5, 20),
    FAM_RISK_VOLATILITY: (20, 63),
    FAM_LIQUIDITY_VOLUME: (5, 20),
    FAM_PROFITABILITY_QUALITY: (20, 63),
    FAM_GROWTH_INVESTMENT: (20, 63),
    FAM_VALUATION: (20, 63),
    FAM_CROSS_FAMILY: (20, 63),
    FAM_FUNDAMENTAL_CHANGE_EVENT: (1, 5, 20),
    FAM_POST_FILING_PRICE_REACTION: (1, 5),
    FAM_RESIDUAL_PRICE: (5, 20),
}


def horizon_eligible(family: str, horizon: int) -> tuple[bool, str]:
    """Deterministic (eligible, reason) for one (family, horizon-session) pair."""
    allowed = _FAMILY_HORIZONS.get(family)
    if allowed is None:
        return False, "UNKNOWN_FAMILY"
    if horizon not in ALL_HORIZONS:
        return False, "HORIZON_NOT_IN_VOCABULARY"
    if horizon == 1 and family not in (FAM_FUNDAMENTAL_CHANGE_EVENT,
                                       FAM_POST_FILING_PRICE_REACTION):
        return False, "ONE_SESSION_ONLY_FOR_EVENT_OR_FILING_REACTION_FAMILIES"
    if horizon in allowed:
        return True, "ECONOMICALLY_COHERENT_FOR_FAMILY"
    return False, "HORIZON_NOT_ECONOMICALLY_COHERENT_FOR_FAMILY"


def coherent_horizons(*, category: str, families: list[str]) -> dict:
    """Resolve the economically-coherent horizon set for a mandate's families.

    Returns the union of coherent research horizons across ``families``, split into the
    AlphaAgent cross-sectional forward-LABEL subset ({5,20,63}) actually evaluable by
    the tournament and the short (1) event/filing subset, plus the execution lag and an
    explicit list of rejected (horizon, family, reason) pairs.
    """
    eligible: set[int] = set()
    rejected: list[dict] = []
    for fam in families:
        for h in ALL_HORIZONS:
            ok, reason = horizon_eligible(fam, h)
            if ok:
                eligible.add(h)
            elif reason not in ("HORIZON_NOT_IN_VOCABULARY",):
                rejected.append({"family": fam, "horizon": h, "reason": reason})
    ordered = sorted(eligible)
    label = [h for h in ordered if h in FORWARD_LABEL_HORIZONS]
    short_event = [h for h in ordered if h == 1]
    return {
        "horizons": ordered,
        "forward_label_horizons": label,            # tournament-evaluable {5,20,63} subset
        "short_event_horizons": short_event,        # 1-session (event/filing only)
        "forward_maturity_vocabulary": list(FORWARD_MATURITY_HORIZONS),
        "execution_lag_sessions": EXECUTION_LAG_SESSIONS,
        "rejected": sorted(rejected, key=lambda d: (d["family"], d["horizon"])),
        "detail": ("Cross-sectional research is evaluated on forward-label horizons "
                   "{5,20,63}; horizon 1 is the execution/label-maturity lag and is only "
                   "a research horizon for event / post-filing-reaction families; the "
                   "1/5/20/63 set is the forward-prediction maturation vocabulary."),
    }


# --------------------------------------------------------------------------- #
# Candidate research families for an existing opportunity
# --------------------------------------------------------------------------- #
def _candidate_families(opportunity: dict, champion: dict) -> list[str]:
    """Deterministically map an existing research opportunity to hypothesis families.

    Uses the opportunity category + proposed experiment_type + the champion's known
    composition. The champion (``composite_sn`` = fundamental-quality + momentum) implies
    profitability/quality + price-momentum + their cross-family interaction as the base
    research families; specific experiment types add residual/regime-relevant families.
    """
    cat = (opportunity or {}).get("category")
    exp = ((opportunity or {}).get("proposed_experiment") or {}).get("experiment_type", "")
    fams: list[str] = []
    if cat not in DISPATCHABLE_CATEGORIES:
        return []   # OBSERVATIONAL opportunities dispatch no cross-sectional hypotheses
    # Champion base families (composite_sn = quality + momentum composite).
    base = [FAM_PROFITABILITY_QUALITY, FAM_PRICE_MOMENTUM, FAM_CROSS_FAMILY]
    if cat == "CHALLENGER":
        fams = base + [FAM_GROWTH_INVESTMENT]
    elif cat == "SIGNAL":
        fams = list(base)
        if "CROWDING" in exp or "ORTHOGONALITY" in exp:
            fams += [FAM_RESIDUAL_PRICE, FAM_RISK_VOLATILITY]
        if "REGIME" in exp:
            fams += [FAM_RISK_VOLATILITY]
    elif cat == "PORTFOLIO":
        # Construction studies (turnover / buffer / sector) study the champion families.
        fams = list(base)
    # Deterministic de-dup preserving canonical family order.
    return [f for f in HYPOTHESIS_FAMILIES if f in set(fams)]


# --------------------------------------------------------------------------- #
# Phase 1 — Research Opportunity normalization (wrap existing owner's opportunity)
# --------------------------------------------------------------------------- #
def normalize_opportunity(*, opportunity: dict, champion: dict, eligible_market_date: str,
                          evidence_snapshot_hash: Optional[str],
                          degradation_categories: Optional[list] = None) -> dict:
    """Wrap ONE existing ``engine.research_agent`` opportunity in a provenance-complete
    contract. Does not fork the concept; adds the fields needed to reproduce *why* the
    opportunity exists and whether it is dispatchable.
    """
    opp = dict(opportunity or {})
    families = _candidate_families(opp, champion)
    dispatchable = opp.get("category") in DISPATCHABLE_CATEGORIES and bool(families)
    horizons = coherent_horizons(category=opp.get("category", ""), families=families)
    permitted = [d for d in (opp.get("permitted_data") or []) if d in OWNED_DATA_FAMILIES]
    champ_id = "|".join([str(champion.get("model_id")), str(champion.get("strategy_version")),
                         str(champion.get("model_registry_version"))])
    opportunity_id = opp.get("opportunity_id") or "unknown_opportunity"
    return {
        "schema_version": SCHEMA_VERSION,
        "opportunity_id": opportunity_id,
        "eligible_market_date": eligible_market_date,
        "champion": {
            "model_id": champion.get("model_id"),
            "primary_model_id": champion.get("primary_model_id"),
            "strategy_version": champion.get("strategy_version"),
            "model_registry_version": champion.get("model_registry_version"),
            "automatic_promotion_allowed": bool(
                champion.get("automatic_promotion_allowed", False)),
        },
        "champion_identity": champ_id,
        "evidence_snapshot_hash": evidence_snapshot_hash,
        "category": opp.get("category"),
        "degradation_category": (degradation_categories or []),
        "affected_family": opp.get("affected_family") or champion.get("model_id"),
        "hypothesis": opp.get("hypothesis"),
        "reason": opp.get("reason"),
        "priority": _i(opp.get("priority")),
        "expected_research_value": opp.get("expected_research_value"),
        "supporting_evidence": opp.get("supporting_evidence") or [],
        "evidence_gaps": sorted(set(opp.get("evidence_gaps") or [])),
        "candidate_research_families": families,
        "coherent_horizons": horizons,
        "permitted_data": sorted(set(permitted)),
        "proposed_experiment": opp.get("proposed_experiment") or {},
        "dispatchable": bool(dispatchable),
        "non_dispatchable_reason": (
            None if dispatchable else
            ("OBSERVATIONAL_OPPORTUNITY_NO_CROSS_SECTIONAL_HYPOTHESIS"
             if opp.get("category") in OBSERVATIONAL_CATEGORIES
             else "NO_ECONOMICALLY_COHERENT_HYPOTHESIS_FAMILY")),
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "required_manual_approval": True,
        "manual_approval_state": MANUAL_APPROVAL_PENDING,
    }


# --------------------------------------------------------------------------- #
# Phase 2 — Controlled Research Mandate
# --------------------------------------------------------------------------- #
def build_mandate(*, normalized_opportunity: dict, mandate_version: int = 1,
                  policy: Optional[dict] = None) -> dict:
    """Convert an eligible, normalized opportunity into a BOUNDED research mandate.

    The mandate specifies what AlphaAgent MAY investigate (families / data / horizons /
    budgets / requirements) — never the answer it should find. It is deterministic and
    carries a stable ``mandate_id`` + ``dispatch_identity`` for idempotency.
    """
    pol = dict(default_policy())
    if policy:
        pol.update(policy)
    no = dict(normalized_opportunity or {})
    families = list(no.get("candidate_research_families") or [])[: pol["max_candidate_families"]]
    horizons = no.get("coherent_horizons") or coherent_horizons(
        category=no.get("category", ""), families=families)
    champ = dict(no.get("champion") or {})

    dispatchable = bool(no.get("dispatchable"))
    # Deterministic mandate identity — stable across repeat construction (idempotency).
    identity_seed = _short(
        no.get("opportunity_id", ""), no.get("champion_identity", ""),
        no.get("eligible_market_date", ""), str(no.get("evidence_snapshot_hash")),
        "v%d" % int(mandate_version),
        ",".join(sorted(families)),
        ",".join(str(h) for h in (horizons.get("horizons") or [])),
    )
    mandate_id = "rbm_" + identity_seed[:16]
    dispatch_identity = "rbd_" + identity_seed[:20]

    return {
        "schema_version": MANDATE_SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "mandate_id": mandate_id,
        "mandate_version": int(mandate_version),
        "dispatch_identity": dispatch_identity,
        "opportunity_id": no.get("opportunity_id"),
        "eligible_market_date": no.get("eligible_market_date"),
        "champion": champ,
        "champion_identity": no.get("champion_identity"),
        "evidence_snapshot_hash": no.get("evidence_snapshot_hash"),
        "category": no.get("category"),
        "degradation_category": no.get("degradation_category") or [],
        "lifecycle_state": MANDATE_DRAFTED,
        "lifecycle_vocabulary": list(MANDATE_LIFECYCLE_VOCAB),
        "dispatchable": dispatchable,
        "non_dispatchable_reason": no.get("non_dispatchable_reason"),
        # --- what may be investigated (never the answer) --------------------- #
        "allowed_hypothesis_families": families,
        "allowed_data_families": no.get("permitted_data") or [],
        "horizons": horizons,
        "bounds": {
            "max_candidate_families": pol["max_candidate_families"],
            "max_candidates": pol["max_candidates"],
            "max_experiments": pol["max_experiments"],
            "max_campaign_generations": pol["max_campaign_generations"],
            "compute_budget_seconds": pol["compute_budget_seconds"],
        },
        "requirements": {
            "min_universe_names_per_period": pol["min_universe_names_per_period"],
            "min_scored_periods": pol["min_scored_periods"],
            "min_coverage_pct": pol["min_coverage_pct"],
            "require_point_in_time_valid": pol["require_point_in_time_valid"],
            "require_survivorship_safe": pol["require_survivorship_safe"],
            "execution_lag_sessions": pol["execution_lag_sessions"],
            "fdr_alpha": pol["fdr_alpha"],
            "keep_min_rank_ic_t": pol["keep_min_rank_ic_t"],
            "keep_min_net25_spread": pol["keep_min_net25_spread"],
            "keep_min_subperiod_consistency": pol["keep_min_subperiod_consistency"],
            "keep_min_regime_consistency": pol["keep_min_regime_consistency"],
            "max_abs_corr_vs_champion": pol["max_abs_corr_vs_champion"],
            "min_incremental_net25_spread": pol["min_incremental_net25_spread"],
            "min_forward_observations": pol["min_forward_observations"],
            "cost_bps_round_trip": pol["cost_bps_round_trip"],
            "owned_data_only": True,
            "paid_data_permitted": False,
        },
        "gate_reference": "alpha_agent.tournament.classify_evidence + "
                          "configs/alpha_agent/stage9_tournament.json",
        "policy": pol,
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "required_manual_approval": True,
        "manual_approval_state": MANUAL_APPROVAL_PENDING,
        "safety": _safety(),
        "provenance": {
            "opportunity_owner": "engine.research_agent",
            "champion_identity_source": "api.universe_scoring",
            "research_lab": "alpha_agent (ResearchQueue + tournament)",
            "gate_owner": "alpha_agent.tournament.classify_evidence",
        },
    }


def finalize_mandate_hash(mandate: dict) -> dict:
    m = dict(mandate)
    m["mandate_hash"] = stable_hash(m)
    return m


# --------------------------------------------------------------------------- #
# Phase 3 — deterministic dispatch descriptor (pure; the api owner enqueues)
# --------------------------------------------------------------------------- #
def dispatch_descriptor(mandate: dict) -> dict:
    """Deterministic queue-job descriptor for a mandate.

    Returns the exact fields the api owner passes to ``ResearchQueue.enqueue`` (category /
    lane / origin / priority / payload) plus the stable ``dedupe_seed`` used to derive the
    idempotency key. The kernel NEVER touches the queue — this is a pure descriptor so the
    dispatch identity is deterministic and reproducible in the pure layer.
    """
    m = dict(mandate or {})
    lane = "research_bridge.%s" % (str(m.get("category") or "unknown").lower())
    payload = {
        "mandate_id": m.get("mandate_id"),
        "mandate_version": m.get("mandate_version"),
        "opportunity_id": m.get("opportunity_id"),
        "eligible_market_date": m.get("eligible_market_date"),
        "champion_identity": m.get("champion_identity"),
        "evidence_snapshot_hash": m.get("evidence_snapshot_hash"),
        "allowed_hypothesis_families": m.get("allowed_hypothesis_families") or [],
        "horizons": (m.get("horizons") or {}).get("horizons") or [],
        "bounds": m.get("bounds") or {},
        "dispatch_identity": m.get("dispatch_identity"),
    }
    return {
        "schema_version": DISPATCH_SCHEMA_VERSION,
        "category": "EXPERIMENT",                 # alpha_agent.autonomous_research JOB_CATEGORY
        "origin": "research-bridge",
        "lane": lane,
        "priority": max(1, min(9, m.get("priority_hint", 2) or 2)),
        "payload": payload,
        "dedupe_seed": str(m.get("dispatch_identity")),
        "not_before": None,
    }


# --------------------------------------------------------------------------- #
# Phase 6 — canonical Alpha Strength Gate (delegates to injected tournament gate)
# --------------------------------------------------------------------------- #
def evaluate_alpha_strength(*, candidates: list, classify_fn: Callable[[dict, dict], dict],
                            gate_cfg: dict, fdr_qvalues: Optional[dict] = None,
                            policy: Optional[dict] = None) -> dict:
    """The ONE canonical Alpha Strength Gate between research result and manual review.

    ``candidates`` is a list of dicts each with ``candidate_id``, a ``metrics`` dict (the
    tournament contract metrics from ``row_to_contract_metrics``) and optional
    ``champion_correlation`` / ``incremental_net25_spread``. The KEEP/REJECT/DATA_HOLD
    decision DELEGATES to the injected ``classify_fn`` (``tournament.classify_evidence``)
    with the injected ``gate_cfg``; the bridge adds the FDR-survival and champion
    orthogonality checks required for the strongest state. No threshold is invented here.

    A candidate is NEVER a challenger merely for a positive backtest. Honest terminal
    outcomes include ``NO_DEFENSIBLE_ALPHA`` (successful scientific null) and ``DATA_HOLD``
    (successful safety hold). The gate never forces a challenger.
    """
    pol = dict(default_policy())
    if policy:
        pol.update(policy)
    fdr_qvalues = fdr_qvalues or {}

    per: list[dict] = []
    for c in (candidates or []):
        cid = c.get("candidate_id")
        metrics = dict(c.get("metrics") or {})
        decision = classify_fn(metrics, gate_cfg) or {}
        reproduced_target = decision.get("target_state")
        # If the caller supplies the research-lab's AUTHORITATIVE per-candidate lifecycle
        # state (e.g. the tournament CandidateRegistry lifecycle, which already folds in the
        # full evaluation history + multiple-testing + no-auto-reopen policy), it is the
        # authority for aggregation; classify_fn still runs to REPRODUCE / verify the gate on
        # the current metrics. Otherwise classify_fn's fresh decision is authoritative.
        canonical = c.get("canonical_target_state")
        if canonical:
            target = _bucket_lifecycle(canonical)
            gate_reproduced = (target == reproduced_target)
        else:
            target = reproduced_target
            gate_reproduced = True
        complete = bool(decision.get("complete"))
        q = _f(fdr_qvalues.get(cid))
        fdr_survives = (q is not None and q < pol["fdr_alpha"])
        corr = _f(c.get("champion_correlation"))
        incr = _f(c.get("incremental_net25_spread"))
        # Manual-review eligibility layered ON TOP of a KEEP: FDR survival + orthogonality
        # + incremental value + forward observations (the tournament manual_review_gate).
        fwd_obs = _i(c.get("forward_observation_count"))
        manual_review_eligible = (
            target == OUTCOME_KEEP_FOR_RESEARCH
            and fdr_survives
            and corr is not None and abs(corr) <= pol["max_abs_corr_vs_champion"]
            and incr is not None and incr >= pol["min_incremental_net25_spread"]
            and fwd_obs >= pol["min_forward_observations"]
        )
        per.append({
            "candidate_id": cid,
            "family": c.get("family"),
            "horizon": c.get("horizon"),
            "target_state": target,
            "reproduced_target_state": reproduced_target,
            "canonical_target_state": canonical,
            "gate_reproduced": bool(gate_reproduced),
            "evidence_status": decision.get("evidence_status"),
            "blocker": decision.get("blocker"),
            "failed_gates": decision.get("failed_gates") or [],
            "complete_evidence": complete,
            "fdr_qvalue": _r(q, 6),
            "fdr_survives": bool(fdr_survives),
            "champion_correlation": _r(corr, 6),
            "incremental_net25_spread": _r(incr, 6),
            "forward_observation_count": fwd_obs,
            "manual_review_eligible": bool(manual_review_eligible),
        })

    n = len(per)
    n_keep = sum(1 for p in per if p["target_state"] == OUTCOME_KEEP_FOR_RESEARCH)
    n_reject = sum(1 for p in per if p["target_state"] == OUTCOME_REJECTED)
    n_hold = sum(1 for p in per if p["target_state"] == OUTCOME_DATA_HOLD)
    n_fdr = sum(1 for p in per if p["fdr_survives"])
    n_manual = sum(1 for p in per if p["manual_review_eligible"])

    # Aggregate terminal outcome (existing constants only; honest null/hold allowed).
    # A challenger is never forced. With >=1 complete-and-weak rejection and ZERO keeps,
    # the evaluable evidence yields no defensible alpha (any remaining candidates are merely
    # DATA_HOLD — insufficient, not defensible); that is the honest scientific null.
    if n == 0:
        outcome = OUTCOME_NOT_EVALUATED
    elif n_manual > 0:
        outcome = OUTCOME_MANUAL_REVIEW
    elif n_keep > 0:
        outcome = OUTCOME_KEEP_FOR_RESEARCH
    elif n_reject > 0:
        outcome = OUTCOME_NO_DEFENSIBLE_ALPHA
    elif n_hold > 0:
        outcome = OUTCOME_DATA_HOLD
    else:
        outcome = OUTCOME_DATA_HOLD

    best = None
    keeps = [p for p in per if p["target_state"] == OUTCOME_KEEP_FOR_RESEARCH]
    if keeps:
        best = sorted(keeps, key=lambda p: (
            -(1 if p["manual_review_eligible"] else 0),
            -(1 if p["fdr_survives"] else 0),
            (p["fdr_qvalue"] if p["fdr_qvalue"] is not None else 1.0),
        ))[0]

    return {
        "schema_version": ENVELOPE_SCHEMA_VERSION,
        "gate_owner": "alpha_agent.tournament.classify_evidence",
        "outcome": outcome,
        "outcome_vocabulary": list(ALPHA_STRENGTH_OUTCOME_VOCAB),
        "counts": {
            "candidates_evaluated": n,
            "keep_for_research": n_keep,
            "rejected": n_reject,
            "data_hold": n_hold,
            "fdr_survivors": n_fdr,
            "manual_review_eligible": n_manual,
        },
        "fdr_alpha": pol["fdr_alpha"],
        "best_challenger": best,
        "per_candidate": per,
        "gate_reproduced_all": all(p["gate_reproduced"] for p in per) if per else True,
        "no_defensible_alpha": outcome == OUTCOME_NO_DEFENSIBLE_ALPHA,
        "data_hold": outcome == OUTCOME_DATA_HOLD,
        "forces_challenger": False,
        "requires_manual_model_review": outcome == OUTCOME_MANUAL_REVIEW,
        "automatic_promotion_allowed": False,
    }


# --------------------------------------------------------------------------- #
# Phase 5 — immutable research-result envelope (references evidence, no duplication)
# --------------------------------------------------------------------------- #
def build_result_envelope(*, mandate: dict, alpha_strength: dict,
                          campaign: dict, candidate_evidence: list,
                          ledger_fingerprint: Optional[dict] = None,
                          data_gaps: Optional[list] = None) -> dict:
    """One immutable envelope referencing the canonical underlying evidence.

    ``candidate_evidence`` is a list of REFERENCE rows (candidate_id, experiment_id,
    family, horizon, datasets, sample window, headline metrics, robustness / orthogonality
    outcomes, terminal state, reproducibility hashes) — pointers into the canonical
    tournament / experiment stores, not a second copy of the evidence.
    """
    m = dict(mandate or {})
    outcome = (alpha_strength or {}).get("outcome", OUTCOME_NOT_EVALUATED)
    if outcome == OUTCOME_DATA_HOLD or outcome == OUTCOME_NOT_EVALUATED:
        lifecycle = MANDATE_DATA_HOLD
    else:
        lifecycle = MANDATE_COMPLETED
    env = {
        "schema_version": ENVELOPE_SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "mandate_id": m.get("mandate_id"),
        "mandate_version": m.get("mandate_version"),
        "dispatch_identity": m.get("dispatch_identity"),
        "opportunity_id": m.get("opportunity_id"),
        "eligible_market_date": m.get("eligible_market_date"),
        "champion": m.get("champion") or {},
        "champion_identity": m.get("champion_identity"),
        "evidence_snapshot_hash": m.get("evidence_snapshot_hash"),
        "mandate_lifecycle_state": lifecycle,
        "campaign": {
            "entrypoint": (campaign or {}).get("entrypoint"),
            "campaign_id": (campaign or {}).get("campaign_id"),
            "terminal_token": (campaign or {}).get("terminal_token"),
            "experiments_executed": _i((campaign or {}).get("experiments_executed")),
            "generations": _i((campaign or {}).get("generations")),
            "budget_seconds": _f((campaign or {}).get("budget_seconds")),
            "owned_data_only": True,
        },
        "alpha_strength": alpha_strength or {},
        "candidate_evidence": list(candidate_evidence or []),
        "data_gaps": sorted(set(data_gaps or [])),
        "operational_ledger_fingerprint": ledger_fingerprint or {
            "checked": False,
            "note": "ledger fingerprint attached by api owner / campaign driver",
        },
        "immutable": True,
        "references_canonical_evidence_only": True,
        "safety": _safety(),
        "provenance": {
            "gate_owner": "alpha_agent.tournament.classify_evidence",
            "queue_owner": "alpha_agent.autonomous_research.ResearchQueue",
            "tournament_owner": "alpha_agent.tournament.CandidateRegistry",
            "opportunity_owner": "engine.research_agent",
        },
    }
    return env


# --------------------------------------------------------------------------- #
# Phase 7 — Persistent-Research-Agent re-assessment (no promotion)
# --------------------------------------------------------------------------- #
def reassess(*, normalized_opportunity: dict, envelope: dict) -> dict:
    """Re-evaluate the originating opportunity using ONLY the returned canonical evidence.

    Returns one of the :data:`REASSESS_VOCAB` verdicts. Never promotes / recalibrates /
    retrains / replaces the champion / changes holdings / creates orders / enables cadence.
    """
    alpha = dict((envelope or {}).get("alpha_strength") or {})
    outcome = alpha.get("outcome", OUTCOME_NOT_EVALUATED)
    counts = dict(alpha.get("counts") or {})
    best = alpha.get("best_challenger")

    reasons: list[str] = []
    if outcome == OUTCOME_MANUAL_REVIEW:
        verdict = REASSESS_MANUAL_REVIEW
        reasons.append("CHALLENGER_MEETS_EVIDENCE_GATE_MANUAL_MODEL_REVIEW_REQUIRED")
    elif outcome == OUTCOME_KEEP_FOR_RESEARCH:
        verdict = REASSESS_CONTINUE_SHADOW
        reasons.append("KEEP_FOR_RESEARCH_CANDIDATE_WARRANTS_CONTINUED_SHADOW_RESEARCH")
    elif outcome == OUTCOME_NO_DEFENSIBLE_ALPHA:
        verdict = REASSESS_NO_DEFENSIBLE_ALPHA
        reasons.append("ALL_CANDIDATES_COMPLETE_AND_WEAK_HONEST_SCIENTIFIC_NULL")
    elif outcome == OUTCOME_DATA_HOLD:
        verdict = REASSESS_DATA_HOLD
        reasons.append("EVIDENCE_INCOMPLETE_HONEST_SAFETY_HOLD")
    elif outcome == OUTCOME_NOT_EVALUATED:
        verdict = REASSESS_UNRESOLVED
        reasons.append("NO_CANDIDATE_EVIDENCE_RETURNED_OPPORTUNITY_UNRESOLVED")
    else:
        verdict = REASSESS_MORE_RESEARCH
        reasons.append("MIXED_EVIDENCE_MORE_RESEARCH_JUSTIFIED")

    return {
        "schema_version": REASSESSMENT_SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "opportunity_id": (normalized_opportunity or {}).get("opportunity_id"),
        "mandate_id": (envelope or {}).get("mandate_id"),
        "eligible_market_date": (envelope or {}).get("eligible_market_date"),
        "verdict": verdict,
        "verdict_vocabulary": list(REASSESS_VOCAB),
        "reason_codes": sorted(set(reasons)),
        "alpha_strength_outcome": outcome,
        "counts": counts,
        "best_challenger": best,
        "opportunity_resolved": verdict in (REASSESS_NO_DEFENSIBLE_ALPHA,
                                            REASSESS_MANUAL_REVIEW),
        "more_research_justified": verdict in (REASSESS_MORE_RESEARCH,
                                               REASSESS_CONTINUE_SHADOW,
                                               REASSESS_UNRESOLVED, REASSESS_DATA_HOLD),
        "manual_model_review_required": verdict == REASSESS_MANUAL_REVIEW,
        "automatic_model_promotion_allowed": False,
        "automatic_recalibration_allowed": False,
        "champion_changed": False,
        "manual_review_required": True,
        "safety": _safety(),
    }


# --------------------------------------------------------------------------- #
# Safety block (mirrors engine.research_agent._safety)
# --------------------------------------------------------------------------- #
def _safety() -> dict:
    return {
        "read_only": True,
        "research_only": True,
        "paper_only": True,
        "manual_review": True,
        "promoted_model": False,
        "recalibrated_model": False,
        "replaced_champion": False,
        "retrained_model": False,
        "wrote_champion_pointer": False,
        "changed_target_weights": False,
        "created_operational_target": False,
        "confirmed_alpha_target": False,
        "created_order_plan": False,
        "created_orders": False,
        "created_fills": False,
        "changed_holdings": False,
        "changed_cash": False,
        "changed_nav": False,
        "performed_broker_execution": False,
        "wrote_to_operational_ledger": False,
        "called_provider": False,
        "called_prediction": False,
        "purchased_data": False,
        "activated_paid_provider": False,
        "enabled_cadence": False,
        "automatic_promotion_allowed": False,
        "automatic_recalibration_allowed": False,
        "automatic_experiment_execution_allowed": False,
        "automation_off": True,
        "safety_badges": ["RESEARCH ONLY", "MANUAL REVIEW", "NO MODEL PROMOTION",
                          "NO PORTFOLIO MUTATION", "NO ORDERS", "AUTOMATION OFF",
                          "OWNED DATA ONLY", "NO PAID DATA"],
    }


__all__ = [
    "SCHEMA_VERSION", "MANDATE_SCHEMA_VERSION", "DISPATCH_SCHEMA_VERSION",
    "ENVELOPE_SCHEMA_VERSION", "REASSESSMENT_SCHEMA_VERSION", "POLICY_VERSION",
    "CALCULATION_OWNER", "FORWARD_LABEL_HORIZONS", "FORWARD_MATURITY_HORIZONS",
    "EXECUTION_LAG_SESSIONS", "ALL_HORIZONS", "MANDATE_LIFECYCLE_VOCAB",
    "ALPHA_STRENGTH_OUTCOME_VOCAB", "REASSESS_VOCAB", "HYPOTHESIS_FAMILIES",
    "OWNED_DATA_FAMILIES", "FORBIDDEN_ACTIONS", "DISPATCHABLE_CATEGORIES",
    "OBSERVATIONAL_CATEGORIES", "default_policy", "horizon_eligible", "coherent_horizons",
    "normalize_opportunity", "build_mandate", "finalize_mandate_hash",
    "dispatch_descriptor", "evaluate_alpha_strength", "build_result_envelope",
    "reassess", "stable_hash",
]
