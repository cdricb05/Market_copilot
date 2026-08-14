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
from typing import Any, Optional

CALCULATION_OWNER = "engine.reassessment_outcomes"
SCHEMA_VERSION = "reassessment_outcomes.v1"
PHASE = "STAGE21"

#: Versioned outcome policy. Every threshold here is documented, configurable and
#: sensitivity-testable, and NONE of them can change operational policy — the most a
#: crossed threshold can do is recommend a manual review.
OUTCOME_POLICY_VERSION = "reassessment_outcome_policy.v1"

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
                           weight=weight, executed=gov["executed"], policy=pol)
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
    }


def _metrics_for(*, action: str, inc_ret: Optional[float], rep_ret: Optional[float],
                 weight: Optional[float], executed: bool, policy: dict) -> dict:
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

    impact = None
    if spread is not None and weight is not None:
        impact = round(weight * spread, 6)
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

    if action == REC_REDUCE and weight is None:
        unmeasurable.append("EXPOSURE_REDUCTION_NOT_DETERMINISTICALLY_MEASURABLE")

    basis = BASIS_OBSERVED if executed else BASIS_COUNTERFACTUAL
    return {
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


def _bucket(rows: list, policy: dict) -> dict:
    """Win/loss summary for a set of matured observations."""
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
    return {"observations": len(rows), "measured_spreads": len(spreads),
            "wins": wins, "losses": losses, "flat": len(spreads) - wins - losses,
            "hit_rate": (round(wins / len(spreads), 6) if spreads else None),
            "mean_spread": mean, "median_spread": med}


def build_policy_intelligence(observations: list, *, policy: Optional[dict] = None) -> dict:
    """Evaluate Stage-20 POLICY BEHAVIOUR by decision reason code.

    Read-only diagnosis. Nothing here tunes a threshold, promotes a model or changes a
    gate: the strongest possible output is "a human should review this".
    """
    pol = {**default_policy(), **(policy or {})}
    mature = [o for o in observations if o.get("maturity") == MAT_MATURE
              and o.get("horizon_eligible_closes") == pol["primary_horizon"]]
    ev = classify_evidence(len(mature))

    above_hurdle = [o for o in mature if o.get("recommendation") == REC_REPLACE
                    and not o.get("action_withheld")]
    withheld = [o for o in mature if o.get("action_withheld")]
    executed = [o for o in mature if o.get("governance_state") == GOV_EXECUTED]
    no_change = [o for o in mature if o.get("governance_state") == GOV_NO_CHANGE]

    # Per reason code: did the control help or hurt? A control that WITHHELD an action
    # helped when the replacement it blocked went on to underperform the incumbent.
    by_code: dict[str, list] = {}
    for o in withheld:
        for code in (o.get("withheld_reason_codes") or []):
            by_code.setdefault(code, []).append(o)

    controls = []
    for code, rows in sorted(by_code.items()):
        b = _bucket(rows, pol)
        enough = b["measured_spreads"] >= pol["min_observations_per_reason_code"]
        # `wins` here means the withheld replacement WOULD have outperformed -> the
        # control cost the book something (regret). `losses` -> the control helped.
        verdict = "INSUFFICIENT_EVIDENCE"
        if enough and b["hit_rate"] is not None:
            verdict = ("CONTROL_REGRET" if b["hit_rate"] > 0.5 else
                       "CONTROL_BENEFIT" if b["hit_rate"] < 0.5 else "NEUTRAL")
        controls.append({
            "reason_code": code, **b,
            "control_helped_count": b["losses"], "control_cost_count": b["wins"],
            "evidence_sufficient": enough, "verdict": verdict,
            "note": ("A withheld action 'helped' when the replacement it blocked went on "
                     "to underperform the incumbent. This is a COUNTERFACTUAL_ESTIMATE: "
                     "the action was not taken, so no portfolio effect was observed."),
        })

    hurdle = _bucket(above_hurdle, pol)
    state = POLICY_INSUFFICIENT_EVIDENCE
    findings: list[str] = []
    if ev["state"] in (EV_PRELIMINARY, EV_HORIZON_ALIGNED):
        adverse = (hurdle["hit_rate"] is not None
                   and hurdle["measured_spreads"] >= pol["min_observations_for_review"]
                   and (1.0 - hurdle["hit_rate"]) >= pol["adverse_fraction_for_review"])
        regretful = [c for c in controls
                     if c["verdict"] == "CONTROL_REGRET" and c["evidence_sufficient"]]
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
                "improved the portfolio." % ", ".join(c["reason_code"] for c in regretful))
        else:
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
    for o in observations:
        by_maturity[o.get("maturity")] = by_maturity.get(o.get("maturity"), 0) + 1
        gs = o.get("governance_state")
        by_governance[gs] = by_governance.get(gs, 0) + 1
        rc = o.get("recommendation")
        by_recommendation[rc] = by_recommendation.get(rc, 0) + 1

    mature = [o for o in observations if o.get("maturity") == MAT_MATURE
              and o.get("horizon_eligible_closes") == pol["primary_horizon"]]
    replacements = [o for o in mature if o.get("recommendation") == REC_REPLACE]
    holds = [o for o in mature if o.get("recommendation") == REC_HOLD]
    exits = [o for o in mature if o.get("recommendation") == REC_EXIT]

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
        "replacement_outcomes": _bucket(replacements, pol),
        "hold_outcomes": _bucket(holds, pol),
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
    "MEASURED_RECOMMENDATIONS", "default_policy", "stable_hash", "maturity_date",
    "forward_return", "resolve_governance", "build_observation",
    "observation_identity", "observation_id", "classify_evidence",
    "build_policy_intelligence", "build_scorecard",
]
