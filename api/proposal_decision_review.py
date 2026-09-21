r"""R62 - PORTFOLIO PROPOSAL DECISION REVIEW: the read / composition owner.

The ONE place a Paper Trader operator - or any surface - can ask what to REVIEW
about a standing reallocation proposal, and get a complete, deterministic answer
without combining eight read models by hand and without asking a language model.

This module performs NO business calculation. It sources, from the owners that
already own them and strictly READ-ONLY:

  * the standing immutable proposal and its read state - ``api.reallocation_proposal``
    (which never runs the engine; the sole execution path stays the Daily Research
    Cycle);
  * the persisted opportunity-cost assessment BOUND to that proposal by its own
    ``hoc_assessment_hash`` - ``api.holding_opportunity_cost``. Bound, not latest:
    a review of a proposal must read the evidence that proposal was built on;
  * the matured decision-outcome evidence - ``api.reassessment_outcomes``;
  * the owned point-in-time return panel behind the ONE covariance kernel -
    ``api.price_panel``, for the eligible session the proposal was built for.

It then calls the pure kernel ``engine.proposal_decision_review`` and publishes the
result. It writes NOTHING: no artifact, no index, no ledger, no database, no order,
no fill, no target. It never approves, rejects, supersedes, regenerates or otherwise
touches the proposal it reviews - the proposal artifact is opened read-only and the
review is derived from it every time it is asked for.

NO RUNTIME LLM. Nothing in this path makes an API call, executes a prompt or depends
on a language model. The operator explanation is assembled by the kernel from
structured reason codes and authoritative backend facts.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from paper_trader.engine import proposal_decision_review as kernel

PHASE = "R62"
OWNER = "api.proposal_decision_review"
COMPOSITION_OWNER = OWNER
CALCULATION_OWNER = kernel.CALCULATION_OWNER
SCHEMA_VERSION = kernel.SCHEMA_VERSION
REVIEW_POLICY_VERSION = kernel.REVIEW_POLICY_VERSION
REPAIR_SCOPE_VERSION = kernel.REPAIR_SCOPE_VERSION
ROUTE = "/v1/operations/proposal-decision-review"

STATUS_OK = "OK"
STATUS_NO_PROPOSAL = "NO_PROPOSAL"
STATUS_UNAVAILABLE = "UNAVAILABLE"
STATUS_VOCAB = (STATUS_OK, STATUS_NO_PROPOSAL, STATUS_UNAVAILABLE)

#: The return panel is the one genuinely expensive input (it is the operational
#: price panel the proposal itself was priced from). It is memoised by the exact
#: proposal identity, so a repeated review of the SAME immutable proposal never
#: re-reads it, and a different proposal can never be served another's returns.
_LOCK = threading.Lock()
_RETURNS_MEMO: dict[str, Any] = {"key": None, "returns": None}


def _now_iso(now: Optional[datetime] = None) -> str:
    return (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# Default loaders - every one of them a READ of an existing owner
# --------------------------------------------------------------------------- #
def _default_proposal_loader(**kwargs) -> dict:
    from paper_trader.api import reallocation_proposal as rp
    return rp.load_reallocation_proposal(**kwargs)


def _default_artifact_loader(*, active_book_id, eligible_market_date,
                             reallocation_dir=None) -> Optional[dict]:
    """The FULL immutable proposal artifact.

    The proposal READ contract flattens the artifact for its surfaces and does not
    republish every block (the risk-contribution policy block, for one). A review
    that read the flattened view would silently see an EMPTY after-target breach
    list where the artifact holds a measured one - and would then report a target
    as clean on evidence it never had. So the read contract is used for the read
    STATE, and the artifact is used for the facts.
    """
    from paper_trader.api import reallocation_proposal as rp
    return rp.load_latest_artifact(active_book_id=active_book_id,
                                   eligible_market_date=eligible_market_date,
                                   reallocation_dir=reallocation_dir)


def _default_hoc_loader(*, active_book_id, eligible_market_date,
                        assessment_hash=None, hoc_dir=None) -> Optional[dict]:
    """The assessment the proposal was BUILT ON, resolved by its own hash.

    The proposal binds ``hoc_assessment_hash``; a session can hold several
    immutable assessment versions (R54.3), so taking "the latest" would review one
    proposal against another's evidence. The hash is matched first and the bound
    version is used; only when no version carries that hash does this fall back to
    the session's newest, and it says so.
    """
    from paper_trader.api import holding_opportunity_cost as hoc
    art = hoc.load_latest_artifact(active_book_id=active_book_id,
                                   eligible_market_date=eligible_market_date,
                                   hoc_dir=hoc_dir)
    latest = (art or {}).get("assessment") or None
    if not assessment_hash or (latest or {}).get("assessment_hash") == assessment_hash:
        return latest
    # Walk the append-only version chain for the version the proposal bound.
    try:
        for row in hoc.load_artifact_versions(
                active_book_id=active_book_id,
                eligible_market_date=eligible_market_date, hoc_dir=hoc_dir):
            if row.get("assessment_hash") != assessment_hash or not row.get("artifact_id"):
                continue
            bound = hoc.load_artifact_by_id(
                artifact_id=row["artifact_id"], active_book_id=active_book_id,
                eligible_market_date=eligible_market_date, hoc_dir=hoc_dir)
            if (bound or {}).get("assessment"):
                return bound["assessment"]
    except Exception:  # noqa: BLE001 - a read never crashes on a store miss
        pass
    return latest


def _default_evidence_loader(*, active_book_id=None, outcome_dir=None) -> Optional[dict]:
    from paper_trader.api import reassessment_outcomes as ro
    return ro.load_reassessment_outcomes(active_book_id=active_book_id,
                                         outcome_dir=outcome_dir)


def _default_returns_loader(*, tickers: list, as_of: str, lookback: int) -> Optional[dict]:
    from paper_trader.api import price_panel as pp
    return pp.aligned_returns(price_panel=pp.load_operational_price_panel(),
                              tickers=tickers, as_of=as_of, lookback=lookback)


def _memoised_returns(*, key: Optional[str], tickers: list, as_of: str, lookback: int,
                      loader: Callable) -> Optional[dict]:
    if not key:
        return loader(tickers=tickers, as_of=as_of, lookback=lookback)
    with _LOCK:
        if _RETURNS_MEMO["key"] == key and _RETURNS_MEMO["returns"] is not None:
            return _RETURNS_MEMO["returns"]
    got = loader(tickers=tickers, as_of=as_of, lookback=lookback)
    with _LOCK:
        _RETURNS_MEMO["key"], _RETURNS_MEMO["returns"] = key, got
    return got


def reset_cache() -> None:
    """Drop the memoised return panel (tests / a deliberate refresh)."""
    with _LOCK:
        _RETURNS_MEMO["key"], _RETURNS_MEMO["returns"] = None, None


# --------------------------------------------------------------------------- #
# The read contract
# --------------------------------------------------------------------------- #
def _envelope(*, status: str, generated_at: str, message: str,
              proposal_payload: Optional[dict] = None, review: Optional[dict] = None,
              inputs: Optional[dict] = None) -> dict:
    p = proposal_payload or {}
    art = p.get("artifact") or {}
    return {
        "phase": PHASE,
        "owner": OWNER,
        "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": CALCULATION_OWNER,
        "schema_version": SCHEMA_VERSION,
        "route": ROUTE,
        "status": status,
        "status_vocabulary": list(STATUS_VOCAB),
        "generated_at": generated_at,
        "message": message,
        "active_book": p.get("active_book") or {},
        "eligible_market_date": p.get("eligible_market_date"),
        "proposal_read_state": p.get("state"),
        "proposal_id": art.get("proposal_id"),
        "proposal_hash": (art.get("identity") or {}).get("proposal_hash")
                         or p.get("proposal_hash"),
        "proposal_state": p.get("proposal_state"),
        "proposal_generated_at": art.get("generated_at"),
        "proposal_owner": "api.reallocation_proposal",
        "proposal_approvable": p.get("approvable"),
        "manual_approval_required": True,
        "review": review,
        "review_policy_version": REVIEW_POLICY_VERSION,
        "repair_scope_version": REPAIR_SCOPE_VERSION,
        "inputs": inputs or {},
        "runtime_llm_dependency": "NONE",
        "read_only": True,
        "writes_nothing": True,
        "mutates_proposal": False,
        "business_calculation_owner": False,
        "safety": kernel._safety(),
    }


def load_proposal_decision_review(
        *, portfolio_state: Optional[dict] = None,
        proposal_payload: Optional[dict] = None,
        proposal: Optional[dict] = None,
        hoc_assessment: Optional[dict] = None,
        outcome_evidence: Optional[dict] = None,
        aligned_returns: Optional[dict] = None,
        reallocation_dir=None, hoc_dir=None, outcome_dir=None,
        reassessment_dir=None, drc_dir=None, decision_dir=None,
        now: Optional[datetime] = None,
        proposal_loader: Optional[Callable] = None,
        artifact_loader: Optional[Callable] = None,
        hoc_loader: Optional[Callable] = None,
        evidence_loader: Optional[Callable] = None,
        returns_loader: Optional[Callable] = None,
        portfolio_state_loader: Optional[Callable] = None) -> dict:
    """THE read: adjudicate the standing proposal. Read-only and degrade-safe.

    Every heavy input may be injected, so a caller that has already composed the
    proposal payload (the decision snapshot) pays for it once, and a test can build
    a hermetic world without touching a store.
    """
    generated_at = _now_iso(now)
    try:
        payload = proposal_payload if proposal_payload is not None else (
            proposal_loader or _default_proposal_loader)(
                portfolio_state=portfolio_state, reallocation_dir=reallocation_dir,
                reassessment_dir=reassessment_dir, drc_dir=drc_dir,
                decision_dir=decision_dir, now=now,
                portfolio_state_loader=portfolio_state_loader)
    except Exception as exc:  # noqa: BLE001 - a read never crashes its caller
        return _envelope(status=STATUS_UNAVAILABLE, generated_at=generated_at,
                         message="The reallocation proposal could not be read: %s"
                                 % str(exc)[:160])

    read_state = (payload or {}).get("state")
    art_meta = (payload or {}).get("artifact") or {}
    book_id = ((payload or {}).get("active_book") or {}).get("book_id")
    eligible = (payload or {}).get("eligible_market_date")
    identity = dict(art_meta.get("identity") or {})

    artifact = None
    if proposal is None and art_meta.get("proposal_id"):
        try:
            artifact = (artifact_loader or _default_artifact_loader)(
                active_book_id=book_id, eligible_market_date=eligible,
                reallocation_dir=reallocation_dir)
        except Exception:  # noqa: BLE001
            artifact = None
        # The read contract and the artifact must be the SAME proposal, or the
        # review would adjudicate one and label it the other.
        if artifact and artifact.get("proposal_id") == art_meta.get("proposal_id"):
            proposal = artifact.get("proposal")
            identity = dict(artifact.get("identity") or identity)
        else:
            artifact = None

    if not proposal:
        return _envelope(
            status=STATUS_NO_PROPOSAL, generated_at=generated_at,
            proposal_payload=payload,
            message=("There is no reallocation proposal to review for the active book "
                     "and eligible session (read state %s). Nothing is fabricated."
                     % read_state))

    # --- the opportunity-cost assessment this proposal was built on ------------ #
    used_hoc_hash = None
    if hoc_assessment is None:
        try:
            hoc_assessment = (hoc_loader or _default_hoc_loader)(
                active_book_id=book_id, eligible_market_date=eligible,
                assessment_hash=identity.get("hoc_assessment_hash"), hoc_dir=hoc_dir)
        except Exception:  # noqa: BLE001
            hoc_assessment = None
    used_hoc_hash = (hoc_assessment or {}).get("assessment_hash")

    # --- the matured decision evidence ----------------------------------------- #
    if outcome_evidence is None:
        try:
            outcome_evidence = (evidence_loader or _default_evidence_loader)(
                active_book_id=book_id, outcome_dir=outcome_dir)
        except Exception:  # noqa: BLE001
            outcome_evidence = None

    # --- the owned returns behind the ONE covariance kernel --------------------- #
    policy = kernel.review_policy(proposal)
    tickers = sorted(set(kernel.current_weights(proposal))
                     | set(kernel.target_weights(proposal)))
    returns_state = "INJECTED" if aligned_returns is not None else "UNAVAILABLE"
    if aligned_returns is None and tickers and eligible:
        try:
            aligned_returns = _memoised_returns(
                key=identity.get("proposal_hash"), tickers=tickers, as_of=eligible,
                lookback=int(policy.get("covariance_lookback") or 60),
                loader=(returns_loader or _default_returns_loader))
            returns_state = "LOADED" if aligned_returns else "UNAVAILABLE"
        except Exception:  # noqa: BLE001 - the review degrades, it never crashes
            aligned_returns, returns_state = None, "UNAVAILABLE"

    review = kernel.build_review(
        proposal=proposal, hoc_assessment=hoc_assessment,
        outcome_evidence=outcome_evidence, aligned_returns=aligned_returns,
        read_state=read_state, identity=identity)

    verdict = (review.get("review_verdict") or {}).get("verdict")
    return _envelope(
        status=STATUS_OK, generated_at=generated_at, proposal_payload=payload,
        review=review,
        inputs={
            "proposal_hash": identity.get("proposal_hash"),
            "proposal_read_state": read_state,
            "hoc_assessment_hash_bound_by_proposal": identity.get("hoc_assessment_hash"),
            "hoc_assessment_hash_used": used_hoc_hash,
            "hoc_assessment_bound": bool(
                used_hoc_hash and used_hoc_hash == identity.get("hoc_assessment_hash")),
            "hoc_available": bool(hoc_assessment),
            "outcome_evidence_status": (outcome_evidence or {}).get("status"),
            "outcome_evidence_state": (outcome_evidence or {}).get("evidence_state"),
            "return_panel_state": returns_state,
            "return_panel_owner": "api.price_panel",
            "covariance_lookback": policy.get("covariance_lookback"),
            "tickers_priced": len(tickers),
        },
        message=("Proposal decision review for %s (%s). Recommended review path: %s. "
                 "Manual review only - it approves nothing, creates no order plan and "
                 "executes nothing."
                 % (art_meta.get("proposal_id") or "the standing proposal",
                    eligible or "?", verdict)))


def load_review_summary(**kwargs) -> dict:
    """A compact summary for a surface that only needs the headline (the workflow
    state / a status strip). Same owner, same calculation - never a second one."""
    full = load_proposal_decision_review(**kwargs)
    review = full.get("review") or {}
    verdict = review.get("review_verdict") or {}
    states = review.get("states") or {}
    cl = review.get("change_classification") or {}
    incr = ((review.get("marginal_economics") or {}).get(
        "full_target_vs_minimum_repair") or {})
    return {
        "phase": PHASE, "owner": OWNER, "route": ROUTE,
        "status": full.get("status"),
        "generated_at": full.get("generated_at"),
        "eligible_market_date": full.get("eligible_market_date"),
        "proposal_id": full.get("proposal_id"),
        "proposal_read_state": full.get("proposal_read_state"),
        "review_available": bool(review),
        "verdict": verdict.get("verdict"),
        "verdict_label": verdict.get("label"),
        "verdict_vocabulary": list(kernel.VERDICT_VOCAB),
        "reason_codes": list(verdict.get("reason_codes") or []),
        "headline": (review.get("explanation") or {}).get("headline"),
        "repair_obligations": len(review.get("repair_obligations") or []),
        "mandatory_changes": cl.get("mandatory_change_count"),
        "discretionary_changes": cl.get("discretionary_change_count"),
        "minimum_repair_turnover": (states.get(kernel.STATE_MINIMUM_REPAIR) or {}
                                    ).get("one_way_turnover"),
        "full_target_turnover": (states.get(kernel.STATE_FULL_TARGET) or {}
                                 ).get("one_way_turnover"),
        "incremental_net_improvement": incr.get(
            "incremental_score_improvement_net_of_cost"),
        "clears_incremental_hurdle": incr.get("clears_incremental_hurdle"),
        "manual_approval_required": True,
        "runtime_llm_dependency": "NONE",
        "read_only": True,
        "is_an_approval": False,
    }


__all__ = [
    "PHASE", "OWNER", "COMPOSITION_OWNER", "CALCULATION_OWNER", "SCHEMA_VERSION",
    "REVIEW_POLICY_VERSION", "REPAIR_SCOPE_VERSION", "ROUTE",
    "STATUS_OK", "STATUS_NO_PROPOSAL", "STATUS_UNAVAILABLE", "STATUS_VOCAB",
    "load_proposal_decision_review", "load_review_summary", "reset_cache",
]
