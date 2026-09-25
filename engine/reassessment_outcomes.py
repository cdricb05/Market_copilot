"""Stage 21 — REASSESSMENT OUTCOME EVIDENCE & POLICY INTELLIGENCE (pure calculation).

WHAT THIS ANSWERS
-----------------
Stage 20 decides whether the current holdings remain the best risk-adjusted use of
capital. Stage 21 asks the only question that can ever validate that machinery: WERE
THOSE DECISIONS ANY GOOD? For each past reassessment recommendation with matured,
truthful forward evidence it links what the system recommended to what actually
happened — to the incumbent, to the preferred replacement, to the portfolio — and to
whether the recommendation was proposed, approved, executed, or never acted on.

WHAT THIS IS NOT
----------------
It is EVIDENCE, not policy optimisation. Nothing here promotes a model, recalibrates a
threshold, creates a proposal, approves anything, or changes a holding. Crossing an
evidence threshold may RECOMMEND a manual policy review; it never performs one.

PURITY
------
No network, no provider, no prediction, no file I/O, no database, no clock ownership.
Every input is supplied by the composition owner (``api.reassessment_outcomes``).

POINT-IN-TIME INTEGRITY
-----------------------
Every observation binds to the ORIGINAL reassessment: its id, hash, market date,
incumbent, preferred replacement, rank, policy versions, model identity and portfolio
weight AS RECORDED at the time. Today's rank, today's replacement and today's portfolio
can never rewrite a past recommendation. Stage-20 history begins when Stage 20 first
ran; earlier sessions are a documented gap and are NEVER reconstructed.

HORIZONS
--------
No new horizon taxonomy is invented. The horizons and the eligible-session calendar are
the project's authoritative forward-evidence ones (``api.forward_prediction_skill``),
measured in ELIGIBLE COMPLETED SESSIONS, never calendar days.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import date as _date
from typing import Any, Optional

CALCULATION_OWNER = "engine.reassessment_outcomes"
SCHEMA_VERSION = "reassessment_outcomes.v1"
PHASE = "STAGE21"

#: Versioned outcome policy. Every threshold here is documented, configurable and
#: sensitivity-testable, and NONE of them can change operational policy — the most a
#: crossed threshold can do is recommend a manual review.
#: Release 70 bumped this. What the numbers MEAN changed: buckets key on the
#: PROPOSED action rather than the permitted one, controls are judged at their
#: declared horizon, consequences are sized by the proposed quantity, and a
#: bucket must carry distinct candidates and effective independent observations
#: before any verdict is published. A v1 scorecard is not comparable to a v2.
OUTCOME_POLICY_VERSION = "reassessment_outcome_policy.v2"

# --------------------------------------------------------------------------- #
# Maturity — whether an observation may be read at all.
# --------------------------------------------------------------------------- #
MAT_NOT_YET_MATURE = "NOT_YET_MATURE"
MAT_MATURE = "MATURE"
MAT_DATA_BLOCKED = "DATA_BLOCKED"
MAT_POINT_IN_TIME_GAP = "POINT_IN_TIME_GAP"
MAT_UNMEASURABLE = "UNMEASURABLE"
MATURITY_VOCAB = (MAT_NOT_YET_MATURE, MAT_MATURE, MAT_DATA_BLOCKED,
                  MAT_POINT_IN_TIME_GAP, MAT_UNMEASURABLE)

# --------------------------------------------------------------------------- #
# Governance — what actually happened to the recommendation.
# --------------------------------------------------------------------------- #
GOV_RECOMMENDED_NOT_PROPOSED = "RECOMMENDED_NOT_PROPOSED"
GOV_PROPOSED_NOT_APPROVED = "PROPOSED_NOT_APPROVED"
GOV_APPROVED_NOT_EXECUTED = "APPROVED_NOT_EXECUTED"
GOV_EXECUTED = "EXECUTED"
GOV_NO_CHANGE = "NO_CHANGE"
GOV_BLOCKED = "BLOCKED"
GOVERNANCE_VOCAB = (GOV_RECOMMENDED_NOT_PROPOSED, GOV_PROPOSED_NOT_APPROVED,
                    GOV_APPROVED_NOT_EXECUTED, GOV_EXECUTED, GOV_NO_CHANGE, GOV_BLOCKED)

# --------------------------------------------------------------------------- #
# Measurement basis — OBSERVED vs COUNTERFACTUAL_ESTIMATE. NEVER mixed.
#
# A ticker's forward return between two owned completed closes is a MARKET FACT and is
# always OBSERVED. What is counterfactual is the PORTFOLIO consequence of a decision
# that was not taken: "what the book would have earned had this replacement executed".
# Those are different claims and they are labelled separately on every metric.
# --------------------------------------------------------------------------- #
BASIS_OBSERVED = "OBSERVED"
BASIS_COUNTERFACTUAL = "COUNTERFACTUAL_ESTIMATE"
BASIS_VOCAB = (BASIS_OBSERVED, BASIS_COUNTERFACTUAL)

# --------------------------------------------------------------------------- #
# Evidence sufficiency. Deliberately the SAME gate boundaries the project already uses
# for forward-model evidence (api.forward_prediction_skill.EVIDENCE_GATES) so Stage 21
# introduces no new hidden sample thresholds.
# --------------------------------------------------------------------------- #
EV_NO_OBSERVATIONS = "NO_OUTCOME_OBSERVATIONS"
EV_INSUFFICIENT = "INSUFFICIENT_SAMPLE"
EV_PRELIMINARY = "PRELIMINARY_EVIDENCE"
EV_HORIZON_ALIGNED = "HORIZON_ALIGNED_EVIDENCE"
EVIDENCE_VOCAB = (EV_NO_OBSERVATIONS, EV_INSUFFICIENT, EV_PRELIMINARY,
                  EV_HORIZON_ALIGNED)

EVIDENCE_GATES = (
    {"min_observations": 0, "max_observations": 4, "state": EV_INSUFFICIENT,
     "interpretation": "Pipeline verification only — no policy conclusion."},
    {"min_observations": 5, "max_observations": 19, "state": EV_PRELIMINARY,
     "interpretation": "Preliminary diagnostics only — no policy conclusion."},
    {"min_observations": 20, "max_observations": 62, "state": EV_PRELIMINARY,
     "interpretation": "Preliminary decision-quality read — still not horizon-aligned."},
    {"min_observations": 63, "max_observations": None, "state": EV_HORIZON_ALIGNED,
     "interpretation": "First horizon-aligned decision-quality window."},
)

# --------------------------------------------------------------------------- #
# Policy intelligence states. NONE of them changes operational policy.
# --------------------------------------------------------------------------- #
POLICY_INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
POLICY_STABLE = "POLICY_STABLE"
POLICY_REVIEW_CANDIDATE = "POLICY_REVIEW_CANDIDATE"
POLICY_RESEARCH_REQUIRED = "RESEARCH_REQUIRED"
POLICY_VOCAB = (POLICY_INSUFFICIENT_EVIDENCE, POLICY_STABLE,
                POLICY_REVIEW_CANDIDATE, POLICY_RESEARCH_REQUIRED)

#: Recommendation vocabulary (mirrors engine.holding_opportunity_cost; mirrored rather
#: than imported to keep this kernel free of sibling-kernel coupling).
REC_HOLD = "HOLD"
REC_REDUCE = "REDUCE"
REC_EXIT = "EXIT"
REC_REPLACE = "REPLACE"
REC_ADD = "ADD"
MEASURED_RECOMMENDATIONS = (REC_HOLD, REC_REDUCE, REC_EXIT, REC_REPLACE, REC_ADD)


def default_policy() -> dict:
    """The versioned Stage-21 outcome policy.

    Each threshold is economically justified below and none of them can promote a model,
    change a gate or alter a portfolio. They bound only what may be READ into the
    numbers and when a MANUAL review is worth recommending.
    """
    return {
        "outcome_policy_version": OUTCOME_POLICY_VERSION,
        # The horizon the headline decision-quality read uses. 20 eligible sessions is
        # the project's established medium horizon and is long enough for a replacement
        # thesis to express itself while short enough to accumulate a usable sample.
        "primary_horizon": 20,
        # Minimum matured observations before a REASON CODE (a specific gate: churn
        # cooldown, turnover budget, switching cost...) may be characterised at all.
        # Below this the per-gate read is reported as insufficient, never as a verdict.
        "min_observations_per_reason_code": 12,
        # A recommendation "won" if the realized spread beats this. Zero, deliberately:
        # the spread is already net of nothing, and the switching cost the decision was
        # measured against is carried separately as `expected_net_improvement`.
        "win_threshold_spread": 0.0,
        # Fraction of matured, above-hurdle replacements that must have gone the WRONG
        # way before a manual policy review is worth recommending. 0.60 is a clear
        # majority against, not a coin-flip.
        "adverse_fraction_for_review": 0.60,
        # A single outlier must not trigger a review, so the adverse-fraction test also
        # requires this many matured observations in the bucket.
        "min_observations_for_review": 20,
        # --- Release 70: sample INDEPENDENCE, not row count ------------------- #
        # Matured rows are not independent observations. Before R70 a bucket of 8,825
        # rows carrying ONE distinct replacement ticker, repeated across overlapping
        # sessions, was published with a hit rate and an adverse verdict as though it
        # held 8,825 independent trials. It held roughly one.
        #
        # Minimum DISTINCT candidate names before a bucket may carry a verdict. Two is
        # the smallest number that is not "one stock's price path".
        "min_distinct_candidates_for_verdict": 2,
        # Minimum effective independent observations. The effective count discounts a
        # bucket for repeating the same (ticker, candidate) pair across sessions that
        # overlap inside the measurement horizon.
        "min_effective_observations_for_verdict": 8,
        # Sessions closer together than the measured horizon observe overlapping
        # returns, so they are clustered and counted once.
        "cluster_sessions_by_horizon": True,
        # --- Release 70: judge a control at the horizon it DECLARES ----------- #
        # A control that acts over N sessions must be judged over N sessions. The churn
        # cooldown declares 5 eligible sessions (engine.portfolio_reassessment
        # ``churn_cooldown_trading_days``); judging it at the 20-session headline
        # horizon asks it about returns it never claimed to influence, and measurably
        # inverts its verdict. Any code absent here falls back to the primary horizon,
        # and the horizon actually used is published on every control row.
        "control_declared_horizon": {
            "CHURN_COOLDOWN_ACTIVE": 5,
            "CHURN_COOLDOWN": 5,
            "RECENTLY_CHANGED": 5,
        },
    }


# --------------------------------------------------------------------------- #
# Small pure helpers
# --------------------------------------------------------------------------- #
def stable_hash(obj: Any) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _r6(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), 6)


def maturity_date(*, calendar: list, from_date: Optional[str],
                  horizon: int) -> Optional[str]:
    """The eligible completed session exactly ``horizon`` sessions after ``from_date``.

    ELIGIBLE SESSIONS, never calendar days — weekends and holidays are structurally
    absent from the calendar, so they can never be counted as elapsed horizon.
    """
    if not from_date or horizon <= 0:
        return None
    after = [d for d in calendar if d > from_date]
    return after[horizon - 1] if len(after) >= horizon else None


def _price_exact(series: dict, ticker: Optional[str], d: Optional[str]) -> Optional[float]:
    """The recorded completed close for exactly this (ticker, date), or None.

    EXACT only. Nothing is interpolated, carried forward or approximated: a missing
    close is a data gap and is reported as one.
    """
    if not ticker or not d:
        return None
    for row in (series.get(ticker) or []):
        try:
            if row[0] == d:
                return _f(row[1])
        except (IndexError, TypeError):
            continue
    return None


def forward_return(series: dict, ticker: Optional[str], *, from_date: Optional[str],
                   to_date: Optional[str]) -> Optional[float]:
    """Total return between two OWNED completed closes, or None. Never extrapolated."""
    a = _price_exact(series, ticker, from_date)
    b = _price_exact(series, ticker, to_date)
    if a in (None, 0) or b is None:
        return None
    try:
        return round(float(b) / float(a) - 1.0, 6)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


# --------------------------------------------------------------------------- #
# Governance resolution (Workstream E)
# --------------------------------------------------------------------------- #
def resolve_governance(*, recommendation: str, decision_state: Optional[str],
                       action_withheld: bool, blockers: list,
                       proposal: Optional[dict], lineage: Optional[dict],
                       ticker: Optional[str]) -> dict:
    """What ACTUALLY happened to this recommendation.

    Execution is established ONLY through immutable lineage — the desk orders that
    carry the proposal/plan identity and actually filled. It is never inferred from a
    current target, a current holdings list or a state name.
    """
    lin = lineage or {}
    prop = proposal or {}
    executed_tickers = {str(t).upper() for t in (lin.get("filled_tickers") or [])}
    proposed_tickers = {str(t).upper() for t in (prop.get("action_tickers") or [])}
    tk = str(ticker or "").upper()

    if blockers:
        state, why = GOV_BLOCKED, "The reassessment was blocked; nothing was proposed."
    elif recommendation == REC_HOLD:
        state, why = GOV_NO_CHANGE, "HOLD recommends no change; none was made."
    elif tk and tk in executed_tickers:
        state, why = GOV_EXECUTED, ("An order carrying this rebalance lineage filled "
                                    "for this name.")
    elif tk and tk in proposed_tickers and lin.get("approved"):
        state, why = GOV_APPROVED_NOT_EXECUTED, (
            "The proposal was approved but no filled order carries this name.")
    elif tk and tk in proposed_tickers:
        state, why = GOV_PROPOSED_NOT_APPROVED, (
            "A proposal included this name but it was never approved.")
    elif action_withheld:
        state, why = GOV_RECOMMENDED_NOT_PROPOSED, (
            "A deterministic control withheld the action, so it never became a proposal.")
    elif decision_state in ("CURRENT_NO_CHANGE", "NO_CHANGE"):
        state, why = GOV_NO_CHANGE, (
            "The portfolio-level gate concluded no change was justified.")
    else:
        state, why = GOV_RECOMMENDED_NOT_PROPOSED, (
            "The recommendation was surfaced for review but never became a proposal.")
    return {"governance_state": state, "governance_reason": why,
            "executed": state == GOV_EXECUTED}


# --------------------------------------------------------------------------- #
# One observation (Workstream B)
# --------------------------------------------------------------------------- #
def build_observation(*, row: dict, rec: dict, horizon: int, calendar: list,
                      series: dict, lineage: Optional[dict] = None,
                      proposal: Optional[dict] = None,
                      policy: Optional[dict] = None) -> dict:
    """ONE deterministic outcome observation for (reassessment, recommendation, horizon).

    Bound entirely to the ORIGINAL recommendation as recorded. Nothing is re-ranked,
    re-scored or re-derived from current state.
    """
    pol = {**default_policy(), **(policy or {})}
    d0 = row.get("eligible_market_date")
    action = rec.get("recommendation")
    incumbent = rec.get("ticker")
    replacement = rec.get("strongest_replacement_ticker")
    weight = _f(rec.get("current_weight"))

    gov = resolve_governance(
        recommendation=action, decision_state=row.get("decision"),
        action_withheld=bool(rec.get("action_withheld")),
        blockers=list(row.get("blockers") or []), proposal=proposal,
        lineage=lineage, ticker=incumbent)

    mdate = maturity_date(calendar=calendar, from_date=d0, horizon=horizon)
    obs = {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        # --- POINT-IN-TIME binding (Workstream D) — the ORIGINAL decision -------- #
        "reassessment_id": row.get("reassessment_id"),
        "reassessment_hash": row.get("reassessment_hash"),
        "active_book_id": row.get("active_book_id"),
        "eligible_market_date": d0,
        "portfolio_decision": row.get("decision"),
        "ticker": incumbent,
        "recommendation": action,
        "source_recommendation": rec.get("source_recommendation"),
        "replacement_ticker": replacement,
        "replacement_rank_at_decision": rec.get("replacement_rank"),
        "current_rank_at_decision": rec.get("current_rank"),
        "portfolio_weight_at_decision": weight,
        "expected_net_improvement_at_decision": _f(rec.get("expected_net_improvement")),
        # --- Release 70: IMMUTABLE forward evidence, frozen at the decision ------ #
        # Everything a later reader needs to re-judge this decision on its own terms,
        # recorded once and never re-derived from current state: which candidates were
        # actually considered, what they scored, what the entry mark was, what the
        # switching cost was, how much position the action proposed to move, and the
        # horizon at which it matures.
        "replacement_score_at_decision": _f(rec.get("replacement_score")),
        "replacement_sector_at_decision": rec.get("replacement_sector"),
        "incumbent_score_at_decision": _f(rec.get("signal_score")),
        "incumbent_sector_at_decision": rec.get("sector"),
        "replacement_shortlist_at_decision": list(rec.get("replacement_shortlist") or []),
        "shortlist_size_at_decision": len(rec.get("replacement_shortlist") or []),
        "entry_mark_at_decision": _f(rec.get("entry_mark")),
        "incumbent_mark_at_decision": _f(rec.get("market_value")),
        "switching_cost_bps_at_decision": _f(rec.get("switching_cost_bps")),
        "switching_cost_usd_at_decision": _f(rec.get("switching_cost_usd")),
        "proposed_exposure_reduction_at_decision": _f(rec.get("proposed_exposure_reduction")),
        "proposed_exposure_reduction_basis": rec.get("proposed_exposure_reduction_basis"),
        "decision_date": d0,
        "evidence_immutability": (
            "Frozen at the decision. Never re-ranked, re-scored or re-derived from "
            "current state; a later correction appends a new observation."),
        "action_withheld": bool(rec.get("action_withheld")),
        "withheld_reason_codes": list(rec.get("withheld_reason_codes") or []),
        "decision_reason_codes": list(row.get("reason_codes") or []),
        "reassessment_policy_version": row.get("policy_version"),
        "churn_policy_version": row.get("churn_policy_version"),
        "outcome_policy_version": pol["outcome_policy_version"],
        "horizon_eligible_closes": horizon,
        "maturity_market_date": mdate,
        **gov,
    }

    if action not in MEASURED_RECOMMENDATIONS:
        return {**obs, "maturity": MAT_UNMEASURABLE,
                "maturity_detail": "UNSUPPORTED_RECOMMENDATION",
                **_empty_metrics()}
    if not d0:
        return {**obs, "maturity": MAT_UNMEASURABLE,
                "maturity_detail": "NO_ELIGIBLE_MARKET_DATE", **_empty_metrics()}
    if mdate is None:
        elapsed = len([d for d in calendar if d > d0])
        return {**obs, "maturity": MAT_NOT_YET_MATURE,
                "maturity_detail": "NOT_ENOUGH_ELIGIBLE_CLOSES",
                "eligible_closes_elapsed": elapsed,
                "eligible_closes_required": horizon, **_empty_metrics()}

    inc_ret = forward_return(series, incumbent, from_date=d0, to_date=mdate)
    rep_ret = forward_return(series, replacement, from_date=d0, to_date=mdate)

    if inc_ret is None and (action != REC_ADD):
        return {**obs, "maturity": MAT_DATA_BLOCKED,
                "maturity_detail": "NO_OWNED_CLOSE_FOR_INCUMBENT",
                **_empty_metrics()}
    if action == REC_ADD and rep_ret is None and inc_ret is None:
        return {**obs, "maturity": MAT_DATA_BLOCKED,
                "maturity_detail": "NO_OWNED_CLOSE_FOR_CANDIDATE",
                **_empty_metrics()}

    metrics = _metrics_for(action=action, inc_ret=inc_ret, rep_ret=rep_ret,
                           weight=weight, executed=gov["executed"], policy=pol,
                           proposed_reduction=_f(rec.get("proposed_exposure_reduction")))
    return {**obs, "maturity": MAT_MATURE, "maturity_detail": None, **metrics}


def _empty_metrics() -> dict:
    return {
        "incumbent_forward_return": None,
        "incumbent_forward_return_basis": None,
        "replacement_forward_return": None,
        "replacement_forward_return_basis": None,
        "realized_spread": None,
        "realized_spread_basis": None,
        "portfolio_impact": None,
        "portfolio_impact_basis": None,
        "outcome_direction": None,
        "unmeasurable_components": [],
        # Release 70 — present in every metric set so the contract never varies.
        "proposed_exposure_reduction": None,
        "portfolio_impact_sizing_weight": None,
        "portfolio_impact_sizing_basis": None,
    }


def _metrics_for(*, action: str, inc_ret: Optional[float], rep_ret: Optional[float],
                 weight: Optional[float], executed: bool, policy: dict,
                 proposed_reduction: Optional[float] = None) -> dict:
    """The per-recommendation metric set, each labelled OBSERVED or COUNTERFACTUAL.

    A ticker's forward return is a market fact -> OBSERVED. A portfolio consequence is
    OBSERVED only when the recommendation was actually EXECUTED; otherwise it is an
    explicit COUNTERFACTUAL_ESTIMATE of what would have happened. The two are never
    added together and never share a field.
    """
    unmeasurable: list[str] = []
    spread = None
    if inc_ret is not None and rep_ret is not None:
        spread = round(rep_ret - inc_ret, 6)
    elif action in (REC_REPLACE, REC_HOLD):
        unmeasurable.append("REPLACEMENT_FORWARD_RETURN_UNAVAILABLE")

    # Release 70 — size the consequence by the quantity the action actually PROPOSED,
    # not by the whole position. A REDUCE moves `reduce_fraction` of the holding;
    # scoring it at full weight overstates its portfolio consequence by
    # 1/reduce_fraction. `proposed_reduction` falls back to the full position weight only
    # when the assessment predates the field.
    sizing_weight = proposed_reduction if proposed_reduction is not None else weight
    impact = None
    if spread is not None and sizing_weight is not None:
        impact = round(sizing_weight * spread, 6)
    elif action in (REC_REPLACE,):
        unmeasurable.append("PORTFOLIO_IMPACT_REQUIRES_SPREAD_AND_WEIGHT")

    # Direction is stated from the perspective of the recommendation that was MADE.
    direction = None
    thr = policy["win_threshold_spread"]
    if action == REC_REPLACE and spread is not None:
        direction = "REPLACEMENT_OUTPERFORMED" if spread > thr else \
            "INCUMBENT_OUTPERFORMED" if spread < -thr else "FLAT"
    elif action == REC_HOLD and spread is not None:
        # HOLD wins when the incumbent beat the best-known alternative.
        direction = "HOLD_ADVANTAGE" if spread < -thr else \
            "HOLD_REGRET" if spread > thr else "FLAT"
    elif action in (REC_EXIT, REC_REDUCE) and inc_ret is not None:
        direction = "EXIT_AVOIDED_LOSS" if inc_ret < -thr else \
            "EXIT_MISSED_UPSIDE" if inc_ret > thr else "FLAT"
    elif action == REC_ADD and rep_ret is not None:
        direction = "CANDIDATE_ROSE" if rep_ret > thr else \
            "CANDIDATE_FELL" if rep_ret < -thr else "FLAT"

    if action == REC_REDUCE and sizing_weight is None:
        unmeasurable.append("EXPOSURE_REDUCTION_NOT_DETERMINISTICALLY_MEASURABLE")

    basis = BASIS_OBSERVED if executed else BASIS_COUNTERFACTUAL
    return {
        "proposed_exposure_reduction": _r6(proposed_reduction),
        "portfolio_impact_sizing_weight": _r6(sizing_weight),
        "portfolio_impact_sizing_basis": (
            "PROPOSED_QUANTITY" if proposed_reduction is not None
            else "FULL_POSITION_WEIGHT_FALLBACK"),
        # Market facts — always OBSERVED.
        "incumbent_forward_return": _r6(inc_ret),
        "incumbent_forward_return_basis": BASIS_OBSERVED if inc_ret is not None else None,
        "replacement_forward_return": _r6(rep_ret),
        "replacement_forward_return_basis": BASIS_OBSERVED if rep_ret is not None else None,
        "realized_spread": spread,
        "realized_spread_basis": BASIS_OBSERVED if spread is not None else None,
        # Portfolio consequence — OBSERVED only if the recommendation was executed.
        "portfolio_impact": impact,
        "portfolio_impact_basis": basis if impact is not None else None,
        "outcome_direction": direction,
        "unmeasurable_components": sorted(set(unmeasurable)),
    }


# --------------------------------------------------------------------------- #
# Observation identity (Workstream I)
# --------------------------------------------------------------------------- #
def observation_identity(obs: dict, *, evidence_fingerprint: Optional[str] = None,
                         model_identity: Optional[dict] = None,
                         corporate_actions_hash: Optional[str] = None) -> dict:
    """The deterministic identity of ONE observation.

    Repeated capture of the same matured horizon is idempotent; a NEW matured horizon
    appends a new row; previously recorded evidence is never silently rewritten.
    """
    return {
        "reassessment_id": obs.get("reassessment_id"),
        "reassessment_hash": obs.get("reassessment_hash"),
        "active_book_id": obs.get("active_book_id"),
        "eligible_market_date": obs.get("eligible_market_date"),
        "ticker": obs.get("ticker"),
        "recommendation": obs.get("recommendation"),
        "horizon_eligible_closes": obs.get("horizon_eligible_closes"),
        "evidence_fingerprint": evidence_fingerprint,
        "model_identity": model_identity or {},
        "corporate_actions_hash": corporate_actions_hash,
        "reassessment_policy_version": obs.get("reassessment_policy_version"),
        "churn_policy_version": obs.get("churn_policy_version"),
        "outcome_policy_version": obs.get("outcome_policy_version"),
    }


def observation_id(identity: dict) -> str:
    return "rout_%s_%s_%s_h%s_%s" % (
        identity.get("eligible_market_date") or "nodate",
        identity.get("active_book_id") or "book",
        identity.get("ticker") or "na",
        identity.get("horizon_eligible_closes"),
        stable_hash(identity)[:12])


# --------------------------------------------------------------------------- #
# ECONOMIC identity (MULTI_ASSET_CAPITAL_ACTIVATION_R55_V1)
#
# ``observation_identity`` binds an observation to the EVIDENCE it was measured
# on, which is right for immutability: a later, different price history for the
# same recommendation is a conflict to report, not a row to overwrite. But it is
# the wrong DEDUPLICATION axis. The live store held 8,550 rows for 950 distinct
# recommendation-horizon pairs because the evidence fingerprint carried the price
# store's mutable ``updated_at``: every capture run minted a new fingerprint, a
# new observation_id, and one more copy of every economically identical row -
# multiplicity 1..19, linear in the number of runs. Governance then read 125
# matured observations where 50 existed, flipped an evidence gate, and summed an
# opportunity cost over the duplicates.
#
# The ECONOMIC identity is the axis that actually varies between two distinct
# observations: WHICH reassessment (id + hash, because a same-session reversion
# is different evidence - R54.2), WHICH holding, WHICH recommendation, WHICH
# horizon, WHICH book. Two rows sharing it are ONE observation observed twice.
# --------------------------------------------------------------------------- #
ECONOMIC_IDENTITY_FIELDS = (
    "active_book_id", "reassessment_id", "reassessment_hash",
    "eligible_market_date", "ticker", "recommendation", "horizon_eligible_closes",
)
DEDUPLICATION_AXIS = "ECONOMIC_IDENTITY (%s)" % ", ".join(ECONOMIC_IDENTITY_FIELDS)


def economic_identity(obs: dict) -> dict:
    """The fields that make two matured observations DIFFERENT evidence."""
    return {k: (obs or {}).get(k) for k in ECONOMIC_IDENTITY_FIELDS}


def economic_observation_key(obs: dict) -> str:
    """ONE stable string per economic identity (the deduplication key)."""
    return stable_hash(economic_identity(obs))


def deduplicate_observations(rows: list) -> dict:
    """Collapse persisted rows to ONE authoritative row per economic identity.

    The FIRST-RECORDED row wins (``recorded_at`` ascending, then observation_id
    for a deterministic tie), because it is the one written when the horizon
    first matured; later copies added no information. Nothing is deleted or
    rewritten here - this is a READ projection over an append-only store, and
    every duplicate is returned beside the row it duplicates so the audit trail
    stays complete. A duplicate whose economic fields agree but whose realised
    spread differs is reported as a CONFLICT, never silently dropped.
    """
    ordered = sorted((r for r in (rows or []) if isinstance(r, dict)),
                     key=lambda r: (str(r.get("recorded_at") or ""),
                                    str(r.get("observation_id") or "")))
    first: dict[str, dict] = {}
    duplicates: list[dict] = []
    conflicts: list[dict] = []
    multiplicity: dict[str, int] = {}
    for r in ordered:
        key = economic_observation_key(r)
        multiplicity[key] = multiplicity.get(key, 0) + 1
        if key not in first:
            first[key] = r
            continue
        keeper = first[key]
        duplicates.append({"observation_id": r.get("observation_id"),
                           "duplicate_of": keeper.get("observation_id"),
                           "economic_key": key,
                           "recorded_at": r.get("recorded_at")})
        if r.get("realized_spread") != keeper.get("realized_spread"):
            conflicts.append({"observation_id": r.get("observation_id"),
                              "kept_observation_id": keeper.get("observation_id"),
                              "economic_key": key,
                              "kept_realized_spread": keeper.get("realized_spread"),
                              "duplicate_realized_spread": r.get("realized_spread"),
                              "reason": "SAME_ECONOMIC_IDENTITY_DIFFERENT_METRICS"})
    authoritative = list(first.values())
    authoritative.sort(key=lambda r: (r.get("eligible_market_date") or "",
                                      r.get("ticker") or "",
                                      r.get("horizon_eligible_closes") or 0))
    return {
        "rows": authoritative,
        "persisted_rows": len(ordered),
        "distinct_economic_observations": len(first),
        "duplicate_rows": len(duplicates),
        "duplicates": duplicates,
        "conflicts": conflicts,
        "max_multiplicity": (max(multiplicity.values()) if multiplicity else 0),
        "dedup_axis": DEDUPLICATION_AXIS,
        "first_recorded_wins": True,
        "history_preserved": True,
    }


# --------------------------------------------------------------------------- #
# Evidence sufficiency + policy intelligence (Workstreams G + H)
# --------------------------------------------------------------------------- #
def classify_evidence(matured_count: int) -> dict:
    if matured_count <= 0:
        return {"state": EV_NO_OBSERVATIONS, "matured_observations": 0,
                "interpretation": "No matured outcome observation exists yet."}
    for gate in EVIDENCE_GATES:
        lo, hi = gate["min_observations"], gate["max_observations"]
        if matured_count >= lo and (hi is None or matured_count <= hi):
            return {"state": gate["state"], "matured_observations": matured_count,
                    "interpretation": gate["interpretation"]}
    return {"state": EV_INSUFFICIENT, "matured_observations": matured_count,
            "interpretation": "Pipeline verification only — no policy conclusion."}


def _iso_date(value: Any) -> Optional[_date]:
    """Parse an ISO market date, or None. Never raises."""
    try:
        return _date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _proposed_action(obs: dict) -> Optional[str]:
    """The action the assessment actually PROPOSED, before governance had its say.

    Release 70. ``recommendation`` on an observation is the EFFECTIVE action — what
    governance permitted — so a REPLACE that the churn cooldown withheld is stored as
    ``HOLD``. ``source_recommendation`` is the original, has been recorded on every
    observation since Stage 20, and was never read by any consumer. A blocked REPLACE
    and a genuine HOLD are different decisions and must never share a bucket.
    """
    return obs.get("source_recommendation") or obs.get("recommendation")


def _effective_observations(rows: list, policy: dict) -> dict:
    """How many INDEPENDENT observations a bucket really holds (Release 70).

    Three quantities, published separately because they answer different questions:

    * ``distinct_candidates`` — how many different alternative names the bucket
      compared against. One means the bucket is a single stock's price path and
      carries no cross-sectional information at all, however many rows it has.
    * ``session_clusters`` — decision sessions grouped so that sessions closer
      together than the measurement horizon (whose forward windows overlap, and which
      therefore observe substantially the same returns) count once.
    * ``effective_observations`` — distinct ``(ticker, candidate, cluster)`` triples.
      This is the number a significance claim may be made against.
    """
    horizon = max(1, int(policy["primary_horizon"]))
    cands = {r.get("replacement_ticker") for r in rows if r.get("replacement_ticker")}
    incumbents = {r.get("ticker") for r in rows if r.get("ticker")}
    dates = sorted({r.get("eligible_market_date") for r in rows
                    if r.get("eligible_market_date")})

    # Cluster sessions whose forward windows overlap, by ACTUAL elapsed time. Counting
    # positions in this bucket's own date list instead would make two decisions a year
    # apart "overlap" merely because no decision was recorded between them. A horizon
    # of N eligible sessions spans about N*7/5 calendar days.
    span_days = int(math.ceil(horizon * 7.0 / 5.0))
    cluster_of: dict[str, int] = {}
    if policy.get("cluster_sessions_by_horizon", True) and dates:
        cid = 0
        anchor = None
        unparsed = -1
        for d in dates:
            dt = _iso_date(d)
            if dt is None:
                # Conservative: everything undateable shares one cluster rather than
                # being credited as independent.
                cluster_of[d] = unparsed
                continue
            if anchor is None:
                anchor = dt
            elif (dt - anchor).days >= span_days:
                cid += 1
                anchor = dt
            cluster_of[d] = cid
        n_clusters = len(set(cluster_of.values()))
    else:
        for i, d in enumerate(dates):
            cluster_of[d] = i
        n_clusters = len(dates)

    triples = {(r.get("ticker"), r.get("replacement_ticker"),
                cluster_of.get(r.get("eligible_market_date")))
               for r in rows if r.get("realized_spread") is not None}
    return {
        "distinct_candidates": len(cands),
        "distinct_incumbents": len(incumbents),
        "distinct_sessions": len(dates),
        "session_clusters": n_clusters,
        "effective_observations": len(triples),
        "independence_note": (
            "effective_observations counts distinct (incumbent, candidate, session "
            "cluster) triples. Rows repeating one pair across overlapping sessions are "
            "NOT independent trials and are not counted as such."),
    }


def _bucket(rows: list, policy: dict) -> dict:
    """Win/loss summary for a set of matured observations.

    Release 70: the summary now carries its own INDEPENDENCE evidence, and states
    whether it is entitled to a verdict at all. A bucket built from one distinct
    candidate, or from too few effective observations, reports
    ``verdict_permitted: False`` with the reason; a consumer that renders a hit rate
    without reading it is rendering one stock's price path as a policy finding.
    """
    thr = policy["win_threshold_spread"]
    spreads = [r["realized_spread"] for r in rows if r.get("realized_spread") is not None]
    wins = sum(1 for s in spreads if s > thr)
    losses = sum(1 for s in spreads if s < -thr)
    mean = round(sum(spreads) / len(spreads), 6) if spreads else None
    srt = sorted(spreads)
    med = None
    if srt:
        mid = len(srt) // 2
        med = srt[mid] if len(srt) % 2 else round((srt[mid - 1] + srt[mid]) / 2.0, 6)

    ind = _effective_observations(rows, policy)
    blockers: list[str] = []
    if ind["distinct_candidates"] < policy["min_distinct_candidates_for_verdict"]:
        blockers.append("SINGLE_CANDIDATE_NOT_EVIDENCE"
                        if ind["distinct_candidates"] <= 1
                        else "TOO_FEW_DISTINCT_CANDIDATES")
    if ind["effective_observations"] < policy["min_effective_observations_for_verdict"]:
        blockers.append("TOO_FEW_EFFECTIVE_OBSERVATIONS")
    if not spreads:
        blockers.append("NO_MEASURED_SPREADS")

    return {"observations": len(rows), "measured_spreads": len(spreads),
            "wins": wins, "losses": losses, "flat": len(spreads) - wins - losses,
            "hit_rate": (round(wins / len(spreads), 6) if spreads else None),
            "mean_spread": mean, "median_spread": med,
            **ind,
            "verdict_permitted": not blockers,
            "verdict_blocked_reason_codes": sorted(set(blockers))}


def build_policy_intelligence(observations: list, *, policy: Optional[dict] = None) -> dict:
    """Evaluate Stage-20 POLICY BEHAVIOUR by decision reason code.

    Read-only diagnosis. Nothing here tunes a threshold, promotes a model or changes a
    gate: the strongest possible output is "a human should review this".
    """
    pol = {**default_policy(), **(policy or {})}
    mature = [o for o in observations if o.get("maturity") == MAT_MATURE
              and o.get("horizon_eligible_closes") == pol["primary_horizon"]]
    ev = classify_evidence(len(mature))

    # Release 70 — bucket on the ORIGINAL proposed action, not the one governance
    # permitted. ``recommendation`` carries the EFFECTIVE action, so a REPLACE that
    # the churn cooldown withheld is persisted as HOLD; filtering on it made every
    # withheld REPLACE structurally invisible to this bucket. ``source_recommendation``
    # is what the assessment actually proposed and is the honest key. It has been
    # recorded on every observation since Stage 20 and was never read.
    above_hurdle = [o for o in mature
                    if _proposed_action(o) == REC_REPLACE
                    and not o.get("action_withheld")]
    withheld = [o for o in mature if o.get("action_withheld")]
    executed = [o for o in mature if o.get("governance_state") == GOV_EXECUTED]
    no_change = [o for o in mature if o.get("governance_state") == GOV_NO_CHANGE]

    # Per reason code: did the control help or hurt? A control that WITHHELD an action
    # helped when the replacement it blocked went on to underperform the incumbent.
    # Release 70 — each control is evaluated at the horizon IT declares, drawn from the
    # full observation set rather than the 20-session slice.
    all_mature = [o for o in observations if o.get("maturity") == MAT_MATURE]
    by_code: dict[str, list] = {}
    for o in all_mature:
        if not o.get("action_withheld"):
            continue
        for code in (o.get("withheld_reason_codes") or []):
            by_code.setdefault(code, []).append(o)

    controls = []
    declared = dict(pol.get("control_declared_horizon") or {})
    for code, all_rows in sorted(by_code.items()):
        h = int(declared.get(code, pol["primary_horizon"]))
        rows = [o for o in all_rows if o.get("horizon_eligible_closes") == h]
        # A control must never VANISH because nothing was captured at the horizon it
        # declares. When that happens the control is still published, with the shortfall
        # named, so the gap is visible rather than silent.
        horizon_state = "EVALUATED_AT_DECLARED_HORIZON"
        if not rows and all_rows:
            horizon_state = "NO_OBSERVATIONS_AT_DECLARED_HORIZON"
        cpol = {**pol, "primary_horizon": h}
        b = _bucket(rows, cpol)
        enough = (b["measured_spreads"] >= pol["min_observations_per_reason_code"]
                  and b["verdict_permitted"])
        # `wins` here means the withheld replacement WOULD have outperformed -> the
        # control cost the book something (regret). `losses` -> the control helped.
        verdict = "INSUFFICIENT_EVIDENCE"
        if enough and b["hit_rate"] is not None:
            verdict = ("CONTROL_REGRET" if b["hit_rate"] > 0.5 else
                       "CONTROL_BENEFIT" if b["hit_rate"] < 0.5 else "NEUTRAL")
        controls.append({
            "reason_code": code, **b,
            "control_helped_count": b["losses"], "control_cost_count": b["wins"],
            "declared_horizon_eligible_closes": h,
            "horizon_source": ("CONTROL_DECLARED" if code in declared
                               else "PRIMARY_HORIZON_FALLBACK"),
            "horizon_state": horizon_state,
            "observations_at_all_horizons": len(all_rows),
            "horizons_observed": sorted(
                {o.get("horizon_eligible_closes") for o in all_rows
                 if o.get("horizon_eligible_closes") is not None}),
            "evidence_sufficient": enough, "verdict": verdict,
            "note": ("A withheld action 'helped' when the replacement it blocked went on "
                     "to underperform the incumbent. This is a COUNTERFACTUAL_ESTIMATE: "
                     "the action was not taken, so no portfolio effect was observed. The "
                     "control is judged at the horizon it declares (%d eligible closes), "
                     "not at the headline horizon." % h),
        })

    hurdle = _bucket(above_hurdle, pol)
    state = POLICY_INSUFFICIENT_EVIDENCE
    findings: list[str] = []

    # Release 70 — the HEADLINE read (were the replacements that cleared the hurdle any
    # good?) is gated on evidence at the primary horizon. A CONTROL is not: it is judged
    # at the horizon IT declares, and a control with sufficient evidence there is
    # exactly the case a human should review, whether or not the headline horizon has
    # accumulated anything. Gating both on the same counter made a control that acts
    # over 5 sessions unreportable until 20-session evidence existed.
    headline_ready = ev["state"] in (EV_PRELIMINARY, EV_HORIZON_ALIGNED)
    regretful = [c for c in controls
                 if c["verdict"] == "CONTROL_REGRET" and c["evidence_sufficient"]]
    # A bucket that is not entitled to a verdict cannot produce an adverse finding. The
    # pre-R70 "0 wins / 6 losses, adverse" read was six rows of ONE candidate across
    # overlapping sessions.
    adverse = (headline_ready
               and hurdle["verdict_permitted"]
               and hurdle["hit_rate"] is not None
               and hurdle["measured_spreads"] >= pol["min_observations_for_review"]
               and (1.0 - hurdle["hit_rate"]) >= pol["adverse_fraction_for_review"])

    if not hurdle["verdict_permitted"] and hurdle["observations"]:
        findings.append(
            "The replacement bucket holds %d matured row(s) but only %d distinct "
            "candidate(s) and %d effective independent observation(s) (%s); no "
            "alpha verdict is published from it."
            % (hurdle["observations"], hurdle["distinct_candidates"],
               hurdle["effective_observations"],
               ", ".join(hurdle["verdict_blocked_reason_codes"])))
    if adverse:
        state = POLICY_REVIEW_CANDIDATE
        findings.append(
            "Replacements that cleared the net-improvement hurdle underperformed "
            "their incumbents in %d of %d matured comparisons."
            % (hurdle["losses"], hurdle["measured_spreads"]))
    elif regretful:
        state = POLICY_REVIEW_CANDIDATE
        findings.append(
            "Control(s) %s withheld actions that would more often than not have "
            "improved the portfolio, judged at each control's declared horizon."
            % ", ".join(c["reason_code"] for c in regretful))
    elif headline_ready:
        state = POLICY_STABLE
    return {
        "policy_state": state,
        "policy_state_vocabulary": list(POLICY_VOCAB),
        "evidence": ev,
        "primary_horizon": pol["primary_horizon"],
        "replacements_above_hurdle": hurdle,
        "controls": controls,
        "executed_observations": len(executed),
        "no_change_observations": len(no_change),
        "findings": findings,
        "changes_policy": False, "changes_thresholds": False,
        "changes_model": False, "changes_champion": False, "changes_portfolio": False,
        "recommends_manual_review_only": True,
        "note": ("Policy intelligence is EVIDENCE for a later, human-gated review. It "
                 "never tunes a threshold, promotes a model, creates a proposal or "
                 "changes a holding. INSUFFICIENT_EVIDENCE never changes anything."),
    }


# --------------------------------------------------------------------------- #
# Scorecard (Workstream J)
# --------------------------------------------------------------------------- #
def build_scorecard(observations: list, *, policy: Optional[dict] = None) -> dict:
    """The read-only decision outcome scorecard. Deliberately NOT collapsed into one
    opaque score — each dimension is reported so a human can see what drove it."""
    pol = {**default_policy(), **(policy or {})}
    by_maturity = {m: 0 for m in MATURITY_VOCAB}
    by_governance = {g: 0 for g in GOVERNANCE_VOCAB}
    by_recommendation: dict[str, int] = {}
    by_proposed_action: dict[str, int] = {}
    withheld_by_proposed: dict[str, int] = {}
    for o in observations:
        by_maturity[o.get("maturity")] = by_maturity.get(o.get("maturity"), 0) + 1
        gs = o.get("governance_state")
        by_governance[gs] = by_governance.get(gs, 0) + 1
        rc = o.get("recommendation")
        by_recommendation[rc] = by_recommendation.get(rc, 0) + 1
        pa = _proposed_action(o)
        by_proposed_action[pa] = by_proposed_action.get(pa, 0) + 1
        if o.get("action_withheld"):
            withheld_by_proposed[pa] = withheld_by_proposed.get(pa, 0) + 1

    mature = [o for o in observations if o.get("maturity") == MAT_MATURE
              and o.get("horizon_eligible_closes") == pol["primary_horizon"]]
    # Release 70 — bucket on the action the assessment PROPOSED. Bucketing on the
    # effective action filed every governance-withheld REPLACE under HOLD, so the
    # replacement bucket could only ever contain sessions where no control bound, and
    # the hold bucket was contaminated with decisions that were never holds.
    replacements = [o for o in mature if _proposed_action(o) == REC_REPLACE]
    holds = [o for o in mature if _proposed_action(o) == REC_HOLD]
    exits = [o for o in mature if _proposed_action(o) == REC_EXIT]
    reduces = [o for o in mature if _proposed_action(o) == REC_REDUCE]
    withheld_replacements = [o for o in replacements if o.get("action_withheld")]

    # Observed vs counterfactual portfolio value, kept STRICTLY apart.
    observed_impact = [o["portfolio_impact"] for o in mature
                       if o.get("portfolio_impact") is not None
                       and o.get("portfolio_impact_basis") == BASIS_OBSERVED]
    counterfactual_impact = [o["portfolio_impact"] for o in mature
                             if o.get("portfolio_impact") is not None
                             and o.get("portfolio_impact_basis") == BASIS_COUNTERFACTUAL]
    return {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "outcome_policy_version": pol["outcome_policy_version"],
        "primary_horizon": pol["primary_horizon"],
        "reassessments_evaluated": len({o.get("reassessment_id") for o in observations
                                        if o.get("reassessment_id")}),
        "observations_total": len(observations),
        "observations_matured": by_maturity.get(MAT_MATURE, 0),
        "observations_pending": by_maturity.get(MAT_NOT_YET_MATURE, 0),
        "observations_blocked": by_maturity.get(MAT_DATA_BLOCKED, 0),
        "observations_unmeasurable": by_maturity.get(MAT_UNMEASURABLE, 0),
        "by_maturity": by_maturity,
        "by_governance": by_governance,
        "by_recommendation": by_recommendation,
        # Release 70 — the action counts by what was PROPOSED, published beside the
        # counts by what governance permitted. A blocked REPLACE appears as a REPLACE
        # here and as a HOLD in `by_recommendation`; the difference is the governance
        # effect, and collapsing the two made that effect unobservable.
        "by_proposed_action": by_proposed_action,
        "withheld_by_proposed_action": withheld_by_proposed,
        "proposed_vs_permitted_note": (
            "`by_recommendation` counts the action governance PERMITTED; "
            "`by_proposed_action` counts the action the assessment PROPOSED. A "
            "withheld REPLACE is a REPLACE that was blocked, never a HOLD."),
        "replacement_outcomes": _bucket(replacements, pol),
        "replacement_outcomes_scope": "PROPOSED_REPLACE_INCLUDING_WITHHELD",
        "withheld_replacement_outcomes": _bucket(withheld_replacements, pol),
        "hold_outcomes": _bucket(holds, pol),
        "hold_outcomes_scope": "PROPOSED_HOLD_ONLY",
        "reduce_outcomes": {
            "observations": len(reduces),
            "sized_by_proposed_quantity": sum(
                1 for o in reduces
                if o.get("portfolio_impact_sizing_basis") == "PROPOSED_QUANTITY"),
            "note": ("A REDUCE moves only part of the position; its portfolio "
                     "consequence is sized by the proposed quantity, not the whole "
                     "holding."),
        },
        "exit_outcomes": {
            "observations": len(exits),
            "avoided_loss_count": sum(1 for o in exits
                                      if o.get("outcome_direction") == "EXIT_AVOIDED_LOSS"),
            "missed_upside_count": sum(1 for o in exits
                                       if o.get("outcome_direction") == "EXIT_MISSED_UPSIDE"),
            "basis": BASIS_OBSERVED,
            "note": ("An EXIT's forward return is a market fact. Whether the released "
                     "capital did better elsewhere is a separate, counterfactual "
                     "question and is not asserted here."),
        },
        "observed_portfolio_impact": {
            "basis": BASIS_OBSERVED, "observations": len(observed_impact),
            "total": round(sum(observed_impact), 6) if observed_impact else None,
            "note": "Executed recommendations only — an actual portfolio effect."},
        "counterfactual_opportunity_cost": {
            "basis": BASIS_COUNTERFACTUAL, "observations": len(counterfactual_impact),
            "total": (round(sum(counterfactual_impact), 6)
                      if counterfactual_impact else None),
            "note": ("Recommendations that were NOT executed. An estimate of what the "
                     "portfolio would have earned, never a realized result, and never "
                     "added to the observed total.")},
        "evidence": classify_evidence(len(mature)),
        "collapsed_to_single_score": False,
        "read_only": True,
    }


__all__ = [
    "CALCULATION_OWNER", "SCHEMA_VERSION", "PHASE", "OUTCOME_POLICY_VERSION",
    "MATURITY_VOCAB", "MAT_NOT_YET_MATURE", "MAT_MATURE", "MAT_DATA_BLOCKED",
    "MAT_POINT_IN_TIME_GAP", "MAT_UNMEASURABLE",
    "GOVERNANCE_VOCAB", "GOV_RECOMMENDED_NOT_PROPOSED", "GOV_PROPOSED_NOT_APPROVED",
    "GOV_APPROVED_NOT_EXECUTED", "GOV_EXECUTED", "GOV_NO_CHANGE", "GOV_BLOCKED",
    "BASIS_OBSERVED", "BASIS_COUNTERFACTUAL", "BASIS_VOCAB",
    "EVIDENCE_VOCAB", "EVIDENCE_GATES", "POLICY_VOCAB",
    "POLICY_INSUFFICIENT_EVIDENCE", "POLICY_STABLE", "POLICY_REVIEW_CANDIDATE",
    "POLICY_RESEARCH_REQUIRED",
    "REC_HOLD", "REC_REDUCE", "REC_EXIT", "REC_REPLACE", "REC_ADD",
    "ECONOMIC_IDENTITY_FIELDS", "DEDUPLICATION_AXIS", "economic_identity",
    "economic_observation_key", "deduplicate_observations",
    "MEASURED_RECOMMENDATIONS", "default_policy", "stable_hash", "maturity_date",
    "forward_return", "resolve_governance", "build_observation",
    "observation_identity", "observation_id", "classify_evidence",
    "build_policy_intelligence", "build_scorecard",
]
