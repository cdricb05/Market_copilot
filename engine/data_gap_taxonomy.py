r"""Stage 22 — the canonical DATA-GAP TAXONOMY (PURE kernel).

WHY THIS EXISTS
---------------
The Holding Opportunity-Cost surface reported::

    1 data gap(s) documented

and that was the whole story. The operator could not tell WHICH input was missing,
for WHICH holding, as of WHICH date, who owns it, whether the recommendation was
still safe to act on, or whether anything was silently substituted. A downstream
consumer had to infer severity from a string code — which is how "documented" quietly
becomes "ignored".

This kernel turns every raw gap code into a MACHINE-READABLE record carrying:

    ticker · metric · expected_as_of_date · available_as_of_date · source_owner ·
    reason · blocking · effect_on_recommendation · safe_fallback

and classifies it as exactly one of:

    BLOCKING      the portfolio-level conclusion cannot safely be produced;
    NON_BLOCKING  a recommendation may be produced, but coverage/uncertainty is
                  DISCLOSED rather than hidden.

HARD RULE
---------
Missing data is NEVER converted into zero or into current data. A gap with no
genuine point-in-time substitute reports ``safe_fallback = None`` and says so. The
only fallbacks named here are ones the calculation owner already implements and that
do not fabricate a point-in-time value.

PURITY
------
A pure function of its arguments: no IO, no clock, no store, no provider, no
prediction call, no write, and no ``api.*`` import. It reads an already-computed
immutable assessment and describes its gaps; it never recomputes an assessment and
never changes a recommendation.
"""
from __future__ import annotations

from typing import Any, Optional

CALCULATION_OWNER = "engine.data_gap_taxonomy"
TAXONOMY_VERSION = "data_gap_taxonomy.v1"
PHASE = "STAGE22"

# --------------------------------------------------------------------------- #
# Frozen severity vocabulary.
# --------------------------------------------------------------------------- #
BLOCKING = "BLOCKING"
NON_BLOCKING = "NON_BLOCKING"
SEVERITY_VOCABULARY = (BLOCKING, NON_BLOCKING)

#: Frozen scope vocabulary — a gap is either about ONE holding or about the whole book.
SCOPE_HOLDING = "HOLDING"
SCOPE_PORTFOLIO = "PORTFOLIO"
SCOPE_VOCABULARY = (SCOPE_HOLDING, SCOPE_PORTFOLIO)

#: Every gap code this taxonomy knows, with its canonical classification. A code that
#: is NOT in this table is classified BLOCKING (fail closed): an unclassified gap must
#: never be silently downgraded to "documented and harmless".
GAP_CONTRACT: dict[str, dict] = {
    # ---- Holding Opportunity-Cost kernel gaps ------------------------------- #
    "PRIOR_RANK_UNAVAILABLE": {
        "scope": SCOPE_PORTFOLIO,
        "metric": "previous_rank",
        "source_owner": "api.holding_opportunity_cost (previous eligible-session artifact)",
        "severity": NON_BLOCKING,
        "reason": ("No persisted opportunity-cost artifact exists for the previous "
                   "eligible session, so point-in-time previous rank is unavailable."),
        "effect_on_recommendation": (
            "Rank-change deterioration cannot be evaluated for any holding. "
            "Deterioration is decided from the remaining point-in-time signals; a "
            "holding is never marked deteriorating on an assumed rank move."),
        "safe_fallback": None,
        "fallback_note": ("None. Previous rank is reported UNAVAILABLE — it is never "
                          "back-filled from the current snapshot, which would fabricate "
                          "a zero rank change."),
    },
    "RISK_CONTRIBUTION_UNAVAILABLE": {
        "scope": SCOPE_PORTFOLIO,
        "metric": "risk_contribution_pct",
        "source_owner": "api.price_panel (owned trailing closes)",
        "severity": NON_BLOCKING,
        "reason": ("The aligned trailing-return panel is too short or too sparse to "
                   "estimate a covariance, so per-name risk contribution is unavailable."),
        "effect_on_recommendation": (
            "Risk-adjusted improvement falls back to the GROSS score improvement, and "
            "the risk-contribution constraint breach cannot be evaluated. Replacement "
            "still has to clear the gross and net-of-cost hurdles."),
        "safe_fallback": "gross_score_improvement",
        "fallback_note": ("The calculation owner already uses the gross score "
                          "improvement when no risk adjustment can be computed. No "
                          "covariance is invented and no risk number is imputed."),
    },
    "REQUIRED_HOLDING_INPUT_INCOMPLETE": {
        "scope": SCOPE_HOLDING,
        "metric": "required_holding_inputs",
        "source_owner": "api.universe_scoring / api.price_panel",
        "severity": NON_BLOCKING,
        "reason": ("A required point-in-time input (rank, score, 20-day return or "
                   "60-day volatility) is missing for this holding."),
        "effect_on_recommendation": (
            "This holding's recommendation is issued with LOW confidence and it is "
            "never promoted to REPLACE on incomplete data. The portfolio-level "
            "conclusion still stands for the remaining names."),
        "safe_fallback": None,
        "fallback_note": ("None. A missing rank, score, return or volatility is never "
                          "imputed as zero or carried forward from an earlier date."),
    },
    "LIQUIDITY_UNAVAILABLE": {
        "scope": SCOPE_HOLDING,
        "metric": "median_dollar_volume_20d",
        "source_owner": "api.price_panel (owned trailing dollar volume)",
        "severity": NON_BLOCKING,
        "reason": ("No owned trailing dollar-volume history reaches the eligible "
                   "session for this holding, so days-to-liquidate is unavailable."),
        "effect_on_recommendation": (
            "The liquidity state is reported UNAVAILABLE for the affected holding and "
            "the illiquidity screen cannot run for it. Its recommendation is issued "
            "with reduced confidence rather than assumed liquid."),
        "safe_fallback": None,
        "fallback_note": ("None. Missing volume is never treated as adequate volume; "
                          "the holding is reported as liquidity-unknown."),
    },
    # ---- Holding Opportunity-Cost core-input blockers ----------------------- #
    "MISSING_ELIGIBLE_MARKET_DATE": {
        "scope": SCOPE_PORTFOLIO, "metric": "eligible_market_date",
        "source_owner": "engine.market_session", "severity": BLOCKING,
        "reason": "No eligible completed market session could be resolved.",
        "effect_on_recommendation": (
            "No portfolio-level conclusion is produced. Nothing is recommended."),
        "safe_fallback": None,
        "fallback_note": "None. An unresolved session is never advanced by calendar guess.",
    },
    "MISSING_PORTFOLIO_STATE_HASH": {
        "scope": SCOPE_PORTFOLIO, "metric": "portfolio_state_hash",
        "source_owner": "api.portfolio_state", "severity": BLOCKING,
        "reason": "The portfolio state carried no identity fingerprint to bind to.",
        "effect_on_recommendation": (
            "No portfolio-level conclusion is produced: an assessment that cannot be "
            "bound to the portfolio it describes is not evidence."),
        "safe_fallback": None, "fallback_note": "None.",
    },
    "MISSING_UNIVERSE_SCORING_HASH": {
        "scope": SCOPE_PORTFOLIO, "metric": "universe_scoring_hash",
        "source_owner": "api.universe_scoring", "severity": BLOCKING,
        "reason": "The universe scoring carried no output fingerprint to bind to.",
        "effect_on_recommendation": "No portfolio-level conclusion is produced.",
        "safe_fallback": None, "fallback_note": "None.",
    },
    "MISSING_UNIVERSE_ROWS": {
        "scope": SCOPE_PORTFOLIO, "metric": "universe_rows",
        "source_owner": "api.universe_scoring", "severity": BLOCKING,
        "reason": "The eligible scoring universe is empty for the eligible session.",
        "effect_on_recommendation": (
            "No opportunity cost can be computed without an alternative set. Nothing "
            "is recommended."),
        "safe_fallback": None,
        "fallback_note": "None. A stale prior universe is never substituted.",
    },
    "NO_HOLDINGS": {
        "scope": SCOPE_PORTFOLIO, "metric": "positions",
        "source_owner": "api.portfolio_state", "severity": BLOCKING,
        "reason": "The active book reports no holdings to assess.",
        "effect_on_recommendation": "No portfolio-level conclusion is produced.",
        "safe_fallback": None, "fallback_note": "None.",
    },
    # ---- Reassessment evidence-binding blockers ----------------------------- #
    "STALE_CORPORATE_ACTION_EVIDENCE": {
        "scope": SCOPE_PORTFOLIO, "metric": "corporate_actions_hash",
        "source_owner": "api.corporate_actions", "severity": BLOCKING,
        "reason": ("A corporate action was registered after this evidence was "
                   "produced, so it describes holdings that no longer exist "
                   "economically."),
        "effect_on_recommendation": (
            "No portfolio-level conclusion is produced from this evidence. The "
            "artifact is preserved as immutable history."),
        "safe_fallback": None,
        "fallback_note": ("None. Projecting an assessment through a later corporate "
                          "action would be a hindsight rewrite of point-in-time "
                          "evidence."),
    },
    "PORTFOLIO_STATE_CHANGED_SINCE_ASSESSMENT": {
        "scope": SCOPE_PORTFOLIO, "metric": "economic_state_hash",
        "source_owner": "api.portfolio_state", "severity": BLOCKING,
        "reason": ("The economic portfolio (holdings / cash / NAV / corporate "
                   "actions) changed after this evidence was produced."),
        "effect_on_recommendation": (
            "No portfolio-level conclusion is produced from this evidence; a fresh "
            "Daily Research Cycle reassesses against the current portfolio."),
        "safe_fallback": None, "fallback_note": "None.",
    },
    "ASSESSMENT_ELIGIBLE_DATE_MISMATCH": {
        "scope": SCOPE_PORTFOLIO, "metric": "eligible_market_date",
        "source_owner": "api.holding_opportunity_cost", "severity": BLOCKING,
        "reason": ("The bound opportunity-cost assessment covers a different eligible "
                   "session than the reassessment."),
        "effect_on_recommendation": "No portfolio-level conclusion is produced.",
        "safe_fallback": None, "fallback_note": "None.",
    },
    "HOLDING_OPPORTUNITY_COST_NOT_RUN": {
        "scope": SCOPE_PORTFOLIO, "metric": "hoc_assessment_hash",
        "source_owner": "api.holding_opportunity_cost", "severity": BLOCKING,
        "reason": ("No Holding Opportunity-Cost assessment exists for this eligible "
                   "session yet."),
        "effect_on_recommendation": (
            "No portfolio-level conclusion is produced until the Daily Research Cycle "
            "produces the assessment."),
        "safe_fallback": None, "fallback_note": "None.",
    },
    "HOLDING_OPPORTUNITY_COST_BLOCKED": {
        "scope": SCOPE_PORTFOLIO, "metric": "hoc_assessment_state",
        "source_owner": "api.holding_opportunity_cost", "severity": BLOCKING,
        "reason": "The bound opportunity-cost assessment is itself BLOCKED.",
        "effect_on_recommendation": "No portfolio-level conclusion is produced.",
        "safe_fallback": None, "fallback_note": "None.",
    },
    "NO_HOLDINGS_EVALUATED": {
        "scope": SCOPE_PORTFOLIO, "metric": "holding_reviews",
        "source_owner": "api.holding_opportunity_cost", "severity": BLOCKING,
        "reason": "The bound assessment evaluated no holdings.",
        "effect_on_recommendation": "No portfolio-level conclusion is produced.",
        "safe_fallback": None, "fallback_note": "None.",
    },
    "NO_ACTIVE_BOOK": {
        "scope": SCOPE_PORTFOLIO, "metric": "active_book_id",
        "source_owner": "api.operational_book", "severity": BLOCKING,
        "reason": "No active operational book exists to assess.",
        "effect_on_recommendation": "No portfolio-level conclusion is produced.",
        "safe_fallback": None, "fallback_note": "None.",
    },
}

#: The classification used for a code this taxonomy does not know. FAIL CLOSED.
UNKNOWN_GAP = {
    "scope": SCOPE_PORTFOLIO,
    "metric": None,
    "source_owner": "UNKNOWN",
    "severity": BLOCKING,
    "reason": ("This gap code is not in the canonical data-gap taxonomy, so its "
               "effect on the portfolio conclusion cannot be established."),
    "effect_on_recommendation": (
        "Treated as BLOCKING: an unclassified gap is never assumed harmless."),
    "safe_fallback": None,
    "fallback_note": "None. Classify the code in engine.data_gap_taxonomy first.",
}


def _s(x: Any) -> Optional[str]:
    return None if x is None else str(x)


def classify_code(code: Any, *, ticker: Optional[str] = None,
                  expected_as_of_date: Optional[str] = None,
                  available_as_of_date: Optional[str] = None,
                  detail: Optional[str] = None) -> dict[str, Any]:
    """Turn ONE raw gap code into the canonical machine-readable record."""
    c = str(code) if code is not None else "UNKNOWN"
    spec = GAP_CONTRACT.get(c)
    known = spec is not None
    spec = spec or UNKNOWN_GAP
    severity = spec["severity"]
    return {
        "code": c,
        "known_code": known,
        "scope": (SCOPE_HOLDING if ticker else spec["scope"]),
        "ticker": _s(ticker),
        "metric": spec["metric"],
        "expected_as_of_date": _s(expected_as_of_date),
        "available_as_of_date": _s(available_as_of_date),
        "source_owner": spec["source_owner"],
        "reason": (detail or spec["reason"]),
        "severity": severity,
        "blocking": severity == BLOCKING,
        "effect_on_recommendation": spec["effect_on_recommendation"],
        "safe_fallback": spec["safe_fallback"],
        "fallback_note": spec["fallback_note"],
        "silently_substituted": False,
        "taxonomy_version": TAXONOMY_VERSION,
    }


def _holding_gaps(reviews: Any, *, eligible: Optional[str]) -> list[dict]:
    """Per-ticker gap records derived from the assessment's own holding reviews.

    Only genuine per-holding gaps are emitted; a complete holding produces nothing.
    """
    out: list[dict] = []
    for r in (reviews or []):
        if not isinstance(r, dict):
            continue
        tk = r.get("ticker")
        if r.get("liquidity_state") == "UNAVAILABLE":
            out.append(classify_code(
                "LIQUIDITY_UNAVAILABLE", ticker=tk, expected_as_of_date=eligible,
                available_as_of_date=None))
        if r.get("required_data_complete") is False:
            missing = [name for name, key in (
                ("current rank", "current_rank"), ("current score", "current_score"),
                ("20-day return", "return_20d"), ("60-day volatility", "volatility_60d"))
                if r.get(key) is None]
            # A per-holding completeness gap does NOT block the portfolio conclusion:
            # the calculation owner already issues that holding's recommendation at
            # LOW confidence and never treats the missing value as zero.
            out.append(classify_code(
                "REQUIRED_HOLDING_INPUT_INCOMPLETE", ticker=tk,
                expected_as_of_date=eligible, available_as_of_date=None,
                detail=("Required point-in-time input(s) missing for %s: %s."
                        % (tk, ", ".join(missing) if missing else "unspecified"))))
    return out


def classify_assessment_gaps(*, assessment: Any,
                             eligible_market_date: Optional[str] = None,
                             previous_eligible_market_date: Optional[str] = None
                             ) -> dict[str, Any]:
    """Classify EVERY data gap in one immutable Holding Opportunity-Cost assessment.

    Reads only what the assessment already recorded (``data_quality.data_gaps``, the
    per-holding ``holding_reviews`` and any ``blockers``). It recomputes nothing,
    changes no recommendation and never mutates the artifact.
    """
    a = assessment if isinstance(assessment, dict) else {}
    dq = a.get("data_quality") or {}
    eligible = _s(eligible_market_date or a.get("eligible_market_date"))
    prev = _s(previous_eligible_market_date)

    records: list[dict] = []
    for code in (dq.get("data_gaps") or []):
        expected = prev if str(code) == "PRIOR_RANK_UNAVAILABLE" else eligible
        # LIQUIDITY_UNAVAILABLE is emitted per-holding below with its ticker; the
        # portfolio-level duplicate would be an unattributed restatement.
        if str(code) == "LIQUIDITY_UNAVAILABLE":
            continue
        records.append(classify_code(code, expected_as_of_date=expected,
                                     available_as_of_date=None))
    for b in (a.get("blockers") or (a.get("diagnostics") or {}).get("blockers") or []):
        code = b.get("code") if isinstance(b, dict) else b
        detail = b.get("detail") if isinstance(b, dict) else None
        records.append(classify_code(code, expected_as_of_date=eligible,
                                     available_as_of_date=None, detail=detail))
    records.extend(_holding_gaps(a.get("holding_reviews"), eligible=eligible))

    return summarize(records, eligible_market_date=eligible)


def summarize(records: Any, *, eligible_market_date: Optional[str] = None
              ) -> dict[str, Any]:
    """Fold classified gap records into the canonical machine-readable summary."""
    rows = [r for r in (records or []) if isinstance(r, dict)]
    blocking = [r for r in rows if r.get("blocking")]
    non_blocking = [r for r in rows if not r.get("blocking")]
    unknown = [r for r in rows if not r.get("known_code")]
    tickers = sorted({r["ticker"] for r in rows if r.get("ticker")})
    if blocking:
        conclusion = "PORTFOLIO_CONCLUSION_NOT_SAFE"
        headline = ("%d blocking data gap(s): no portfolio-level conclusion can "
                    "safely be produced." % len(blocking))
    elif non_blocking:
        conclusion = "PORTFOLIO_CONCLUSION_WITH_DISCLOSED_UNCERTAINTY"
        headline = ("%d non-blocking data gap(s) documented; the recommendation "
                    "stands with disclosed coverage limits." % len(non_blocking))
    else:
        conclusion = "PORTFOLIO_CONCLUSION_COMPLETE"
        headline = "No data gaps: every required point-in-time input was available."
    return {
        "taxonomy_version": TAXONOMY_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "severity_vocabulary": list(SEVERITY_VOCABULARY),
        "scope_vocabulary": list(SCOPE_VOCABULARY),
        "eligible_market_date": _s(eligible_market_date),
        "gaps": rows,
        "gap_count": len(rows),
        "blocking_gaps": blocking,
        "blocking_gap_count": len(blocking),
        "non_blocking_gap_count": len(non_blocking),
        "unclassified_gap_count": len(unknown),
        "affected_tickers": tickers,
        "has_blocking_gap": bool(blocking),
        "conclusion": conclusion,
        "headline": headline,
        "missing_data_converted_to_zero": False,
        "missing_data_converted_to_current": False,
        "note": ("Every gap is classified from the canonical taxonomy. An unknown "
                 "code is BLOCKING by construction, and no missing value is ever "
                 "replaced by zero or by a current-date substitute."),
    }


__all__ = [
    "PHASE", "CALCULATION_OWNER", "TAXONOMY_VERSION",
    "BLOCKING", "NON_BLOCKING", "SEVERITY_VOCABULARY",
    "SCOPE_HOLDING", "SCOPE_PORTFOLIO", "SCOPE_VOCABULARY",
    "GAP_CONTRACT", "UNKNOWN_GAP",
    "classify_code", "classify_assessment_gaps", "summarize",
]
