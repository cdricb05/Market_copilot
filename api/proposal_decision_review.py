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
result. R69.2 adds ONE composition step on top: ``engine.selected_target`` turns each
of the three reviewed targets into a complete, implementable representation - every
weight, every allocation row, every economic and the before/after risk-contribution
comparison - published on the envelope as ``selected_targets``. It computes nothing:
every number inside those blocks was produced by the kernel above and is read
verbatim.

It writes NOTHING: no artifact, no index, no ledger, no database, no order,
no fill, no target. It never approves, rejects, supersedes, regenerates or otherwise
touches the proposal it reviews - the proposal artifact is opened read-only and the
review is derived from it every time it is asked for.

NO RUNTIME LLM. Nothing in this path makes an API call, executes a prompt or depends
on a language model. The operator explanation is assembled by the kernel from
structured reason codes and authoritative backend facts.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from paper_trader.engine import constrained_reallocation as _cr
from paper_trader.engine import proposal_decision_review as kernel
from paper_trader.engine import selected_target as _selected_target

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
#: R69 - a proposal that EXISTS but whose artifact does not carry the identity the
#: read contract names is not an absent proposal, and saying "there is nothing to
#: review" about a standing proposal is the worst answer this route can give. It
#: is its own terminal status so an operator, and the acceptance gate, can tell the
#: two apart.
STATUS_IDENTITY_MISMATCH = "PROPOSAL_IDENTITY_MISMATCH"
STATUS_VOCAB = (STATUS_OK, STATUS_NO_PROPOSAL, STATUS_UNAVAILABLE,
                STATUS_IDENTITY_MISMATCH)

#: The five states an operator surface must be able to tell apart, published as ONE
#: field so no caller has to re-derive them from a status plus a freshness block.
REVIEW_STATE_CURRENT = "COMPLETE_CURRENT"
REVIEW_STATE_HISTORICAL = "COMPLETE_HISTORICAL"
REVIEW_STATE_NO_PROPOSAL = "PROPOSAL_ABSENT"
REVIEW_STATE_UNAVAILABLE = "PROPOSAL_PRESENT_REVIEW_UNAVAILABLE"
REVIEW_STATE_MISMATCH = "PROPOSAL_REVIEW_IDENTITY_MISMATCH"
REVIEW_STATE_VOCAB = (REVIEW_STATE_CURRENT, REVIEW_STATE_HISTORICAL,
                      REVIEW_STATE_NO_PROPOSAL, REVIEW_STATE_UNAVAILABLE,
                      REVIEW_STATE_MISMATCH)


def _review_state(*, status: str, review: Optional[dict],
                  governance: Optional[dict]) -> str:
    """Collapse (status, review present, session freshness) into ONE named state."""
    if status == STATUS_IDENTITY_MISMATCH:
        return REVIEW_STATE_MISMATCH
    if status == STATUS_NO_PROPOSAL:
        return REVIEW_STATE_NO_PROPOSAL
    if status != STATUS_OK or not review:
        return REVIEW_STATE_UNAVAILABLE
    return (REVIEW_STATE_CURRENT if bool((governance or {}).get("actionable"))
            else REVIEW_STATE_HISTORICAL)

#: The return panel is the one genuinely expensive input (it is the operational
#: price panel the proposal itself was priced from). It is memoised by the exact
#: proposal identity, so a repeated review of the SAME immutable proposal never
#: re-reads it, and a different proposal can never be served another's returns.
_LOCK = threading.Lock()
_RETURNS_MEMO: dict[str, Any] = {"key": None, "returns": None}

#: Release 69 - the two OTHER expensive inputs of this read. Measured cold on the
#: live book, one composition costs ~13.8s: the proposal payload 4.2s, the matured
#: evidence 2.4s, the return panel 5.7s - and the kernel that does the actual
#: reasoning costs 0.005s. The panel was already memoised; these two were re-read
#: in full on every refresh, and re-read AGAIN by the standalone proposal and
#: outcome routes the same screen calls. Memoising them here removes the redundant
#: composition without introducing a second owner: each memo still holds exactly
#: what its canonical owner returned.
#:
#: A memo is used ONLY on the live default read path. The instant a caller injects
#: a payload, a loader, a store directory or a clock, the memo is bypassed - so a
#: fixture stays hermetic and two tests can never see each other's state.
#:
#: Staleness is bounded TWICE. The payload memo is validated against the immutable
#: artifact index, so a NEW proposal invalidates it immediately; the TTL is the
#: backstop for a session rollover, which ``_governance`` independently catches by
#: comparing the bound session with the live latest session - a review bound to a
#: session the workflow has moved past is returned NOT actionable. A memoised
#: review can therefore never be served as actionable when it is stale.
_PAYLOAD_MEMO: dict[str, Any] = {"key": None, "payload": None, "at": 0.0}
_EVIDENCE_MEMO: dict[str, Any] = {"key": None, "evidence": None, "at": 0.0}
MEMO_TTL_SECONDS = 90.0


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


def _current_proposal_id(*, active_book_id, eligible_market_date,
                         reallocation_dir=None) -> Optional[str]:
    """The identity of the standing artifact, read from the index alone (~2ms).

    This is the cheap probe that makes the payload memo exact rather than merely
    time-bounded: if the Daily Research Cycle has persisted a different proposal,
    the id changes and the memo is dropped on the very next read.
    """
    try:
        from paper_trader.api import reallocation_proposal as rp
        art = rp.load_latest_artifact(active_book_id=active_book_id,
                                      eligible_market_date=eligible_market_date,
                                      reallocation_dir=reallocation_dir)
        return (art or {}).get("proposal_id")
    except Exception:  # noqa: BLE001 - a probe never breaks the read it guards
        return None


def _memoised_payload(*, live: bool, loader: Callable, reallocation_dir,
                      **kw) -> tuple:
    """(payload, memo_state). ``memo_state`` is published, never hidden."""
    if not live:
        return loader(reallocation_dir=reallocation_dir, **kw), "BYPASS"
    with _LOCK:
        key, payload, at = (_PAYLOAD_MEMO["key"], _PAYLOAD_MEMO["payload"],
                            _PAYLOAD_MEMO["at"])
    if key and payload is not None and (time.monotonic() - at) < MEMO_TTL_SECONDS:
        book_id, eligible, proposal_id = key
        if _current_proposal_id(active_book_id=book_id, eligible_market_date=eligible,
                                reallocation_dir=reallocation_dir) == proposal_id:
            return payload, "HIT"
    payload = loader(reallocation_dir=reallocation_dir, **kw)
    art = (payload or {}).get("artifact") or {}
    book_id = ((payload or {}).get("active_book") or {}).get("book_id")
    eligible = (payload or {}).get("eligible_market_date")
    proposal_id = art.get("proposal_id")
    if proposal_id:
        with _LOCK:
            _PAYLOAD_MEMO["key"] = (book_id, eligible, proposal_id)
            _PAYLOAD_MEMO["payload"] = payload
            _PAYLOAD_MEMO["at"] = time.monotonic()
    return payload, "MISS"


def _memoised_evidence(*, live: bool, loader: Callable, active_book_id,
                       outcome_dir) -> tuple:
    """(evidence, memo_state). TTL-bounded: matured evidence changes by the day."""
    if not live:
        return loader(active_book_id=active_book_id, outcome_dir=outcome_dir), "BYPASS"
    with _LOCK:
        key, evidence, at = (_EVIDENCE_MEMO["key"], _EVIDENCE_MEMO["evidence"],
                             _EVIDENCE_MEMO["at"])
    if key == active_book_id and evidence is not None             and (time.monotonic() - at) < MEMO_TTL_SECONDS:
        return evidence, "HIT"
    evidence = loader(active_book_id=active_book_id, outcome_dir=outcome_dir)
    with _LOCK:
        _EVIDENCE_MEMO["key"] = active_book_id
        _EVIDENCE_MEMO["evidence"] = evidence
        _EVIDENCE_MEMO["at"] = time.monotonic()
    return evidence, "MISS"


def reset_cache() -> None:
    """Drop every memoised input (tests / a deliberate refresh)."""
    with _LOCK:
        _RETURNS_MEMO["key"], _RETURNS_MEMO["returns"] = None, None
        _PAYLOAD_MEMO["key"], _PAYLOAD_MEMO["payload"], _PAYLOAD_MEMO["at"] = None, None, 0.0
        _EVIDENCE_MEMO["key"], _EVIDENCE_MEMO["evidence"], _EVIDENCE_MEMO["at"] = None, None, 0.0


# --------------------------------------------------------------------------- #
# The read contract
# --------------------------------------------------------------------------- #
def _governance(*, review: Optional[dict], bound_session: Optional[str],
                active_book_id: Optional[str],
                latest_session: Optional[str] = None,
                workflow_state: Optional[dict] = None,
                decision_dir=None) -> dict:
    """R63 — the governance layer over a review: is this decision still actionable,
    and which target (if any) has the operator already selected?

    Freshness can only ever REMOVE selectability. The review kernel holds no clock
    and rules on obligations and economics; a decision bound to a session the
    workflow has moved past is historical evidence, however sound its economics.
    """
    from paper_trader.api import portfolio_decision as _pd  # lazy: import cycle
    fresh = _pd.decision_freshness(
        bound_session=bound_session,
        latest_session=(latest_session if latest_session is not None
                        else _pd.latest_eligible_session(workflow_state=workflow_state)))
    selection = _pd.load_target_selection(active_book_id=active_book_id,
                                          eligible_market_date=bound_session,
                                          decision_dir=decision_dir)
    sel_block = dict((review or {}).get("target_selection") or {})
    if sel_block and not fresh["target_selection_allowed"]:
        blocker = {"code": _pd.PDS_SESSION_STALE, "detail": fresh["detail"]}
        opts = []
        for o in sel_block.get("options") or []:
            o = dict(o)
            o["selectable"] = False
            o["blockers"] = list(o.get("blockers") or []) + [blocker]
            o["blocker_codes"] = sorted(set(o.get("blocker_codes") or [])
                                        | {_pd.PDS_SESSION_STALE})
            opts.append(o)
        sel_block["options"] = opts
        sel_block["selectable_targets"] = []
        sel_block["blocked_by_session_freshness"] = True
    return {
        "freshness": fresh,
        "actionable": bool(fresh["actionable"]),
        "target_selection": sel_block,
        "selection": selection,
        "selected_target": (selection or {}).get("selected_target"),
        "selection_id": (selection or {}).get("selection_id"),
        "governance_owner": _pd.OWNER,
        "selection_confirm_token": _pd.SELECTION_CONFIRM_TOKEN,
        "approval_confirm_token": _pd.CONFIRM_TOKEN,
    }


def _envelope(*, status: str, generated_at: str, message: str,
              proposal_payload: Optional[dict] = None, review: Optional[dict] = None,
              inputs: Optional[dict] = None,
              governance: Optional[dict] = None,
              selected_targets: Optional[dict] = None,
              review_hash_value: Optional[str] = None) -> dict:
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
        "review_state": _review_state(status=status, review=review,
                                      governance=governance),
        "review_state_vocabulary": list(REVIEW_STATE_VOCAB),
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
        # R63 - the review's IDENTITY. The review is a pure projection and is never
        # persisted, so it carries no id of its own; this hash is computed over the
        # payload it just returned. That is sound precisely BECAUSE the projection
        # is byte-stable for the same inputs: a governed target selection binds it,
        # and a selection made against a review that no longer reproduces fails
        # closed rather than approving something nobody reviewed.
        "review_hash": (review_hash_value if review_hash_value is not None
                        else review_hash(review)),
        "review_identity_owner": OWNER,
        # R69.2 - the COMPLETE, implementable representation of each reviewed
        # target: every weight, every allocation row, every economic and the
        # before/after risk-contribution comparison.
        #
        # It is published HERE, on the envelope, and deliberately NOT inside
        # ``review``. ``review_hash`` is computed over ``review``; a governed
        # selection binds that hash, and folding a new block into it would change
        # the identity of every review ever made without one input moving. The
        # block is bound all the same: it is a pure function of the proposal, the
        # review and the target, so ``proposal_hash`` + ``review_hash`` + target
        # determine it exactly, and it carries its own
        # ``selected_target_implementation_hash`` on top.
        #
        # Nothing here is an approval, an order plan or an order.
        "selected_targets": selected_targets or {},
        "selected_target_owner": _selected_target.CALCULATION_OWNER,
        "selected_target_schema_version": _selected_target.SCHEMA_VERSION,
        "selected_target_order": list(_selected_target.TARGET_ORDER),
        "implementable_targets": sorted(
            t for t, b in (selected_targets or {}).items()
            if (b or {}).get("implementable")),
        # R63 — the governance layer: session freshness, the selectability the
        # BACKEND decided, and any selection the operator has already made.
        "governance": governance or {},
        "freshness": (governance or {}).get("freshness") or {},
        "actionable": bool((governance or {}).get("actionable")),
        "target_selection": ((governance or {}).get("target_selection")
                             or (review or {}).get("target_selection") or {}),
        # R69.1 - say OUT LOUD which copy of the option list is authoritative.
        #
        # This envelope carries the same three options twice: the kernel's copy
        # under ``review.target_selection``, which holds no clock and therefore
        # always reports ``selectable: True``, and the freshness-reconciled copy
        # here and under ``governance``. A reader that picked the kernel copy got
        # a proposal bound to a session the workflow had moved past, presented as
        # selectable. One already had - the write gate in api.portfolio_decision.
        #
        # The kernel copy is NOT reconciled in place on purpose: ``review_hash``
        # is computed over ``review``, a governed selection binds that hash, and
        # folding a clock into it would make the review's identity change with the
        # calendar rather than with its inputs.
        "selectability_authority": "target_selection",
        "selectability_authority_doc": (
            "Read selectability from the top-level 'target_selection' (identical to "
            "governance.target_selection). 'review.target_selection' is the kernel's "
            "pre-freshness copy, kept byte-stable because review_hash is computed over "
            "it; it reports selectability BEFORE session freshness is applied and must "
            "not be used to decide whether a target may be selected."),
        "selection": (governance or {}).get("selection"),
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


def _selected_targets(*, proposal: dict, review: dict, identity: dict,
                      proposal_id: Optional[str], review_hash_value: Optional[str],
                      active_book_id: Optional[str],
                      eligible_market_date: Optional[str]) -> dict:
    """The three target representations, from the ONE kernel that owns them.

    Composition only: every weight and every economic inside these blocks was
    computed by ``engine.proposal_decision_review`` and is read verbatim. A failure
    here degrades to an EMPTY map rather than a partial one - a half-built target is
    exactly the thing this release exists to prevent, and a surface that finds no
    block simply offers no implementation rather than guessing at one.
    """
    try:
        bound = dict(identity or {})
        bound.update({"proposal_id": proposal_id, "review_hash": review_hash_value,
                      "active_book_id": (bound.get("active_book_id")
                                         or active_book_id),
                      "eligible_market_date": (bound.get("eligible_market_date")
                                               or eligible_market_date)})
        return _selected_target.build_all_selected_targets(
            proposal=proposal, review=review, identity=bound)
    except Exception:  # noqa: BLE001 - a pure read never crashes its caller
        return {}


def review_hash(review: Optional[dict]) -> Optional[str]:
    """A stable identity for ONE review payload, or None when there is no review.

    Reuses the constraint kernel's canonical stable hash (SHA-256 over the payload
    with volatile keys stripped), so no second hashing convention is introduced.
    """
    if not review:
        return None
    try:
        return _cr.stable_hash(review)
    except Exception:  # noqa: BLE001 - an identity must never break a pure read
        return None


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
        portfolio_state_loader: Optional[Callable] = None,
        latest_session: Optional[str] = None,
        workflow_state: Optional[dict] = None) -> dict:
    """THE read: adjudicate the standing proposal. Read-only and degrade-safe.

    Every heavy input may be injected, so a caller that has already composed the
    proposal payload (the decision snapshot) pays for it once, and a test can build
    a hermetic world without touching a store.
    """
    generated_at = _now_iso(now)
    _t0 = time.monotonic()
    # R69 - the memo serves the LIVE default read only. Any injected payload,
    # loader, store directory or clock bypasses it (see the memo declarations).
    _payload_live = (proposal_payload is None and proposal_loader is None
                     and portfolio_state is None and portfolio_state_loader is None
                     and reallocation_dir is None and reassessment_dir is None
                     and drc_dir is None and decision_dir is None and now is None)
    payload_memo = "INJECTED"
    try:
        if proposal_payload is not None:
            payload = proposal_payload
        else:
            payload, payload_memo = _memoised_payload(
                live=_payload_live,
                loader=(proposal_loader or _default_proposal_loader),
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
    identity_mismatch = False
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
            identity_mismatch = bool(artifact and artifact.get("proposal_id")
                                     and artifact.get("proposal_id")
                                     != art_meta.get("proposal_id"))
            artifact = None

    if not proposal:
        if identity_mismatch:
            return _envelope(
                status=STATUS_IDENTITY_MISMATCH, generated_at=generated_at,
                proposal_payload=payload,
                message=("A reallocation proposal is present for the active book, but "
                         "the persisted artifact does not carry the proposal identity "
                         "the read contract names (%s). The review is withheld rather "
                         "than adjudicating one proposal and labelling it another. "
                         "Nothing is fabricated."
                         % (art_meta.get("proposal_id") or "unknown")))
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
    evidence_memo = "INJECTED"
    if outcome_evidence is None:
        try:
            outcome_evidence, evidence_memo = _memoised_evidence(
                live=(evidence_loader is None and outcome_dir is None),
                loader=(evidence_loader or _default_evidence_loader),
                active_book_id=book_id, outcome_dir=outcome_dir)
        except Exception:  # noqa: BLE001
            outcome_evidence, evidence_memo = None, "UNAVAILABLE"

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
    governance = _governance(
        review=review, bound_session=(identity.get("eligible_market_date") or eligible),
        active_book_id=book_id, latest_session=latest_session,
        workflow_state=workflow_state, decision_dir=decision_dir)
    # R69.2 - the three implementable target representations. The review hash is
    # computed ONCE and travels into both the blocks (which bind it) and the
    # envelope, so the identity a selection freezes is provably the identity the
    # operator was served.
    rhash = review_hash(review)
    selected_targets = _selected_targets(
        proposal=proposal, review=review, identity=identity,
        proposal_id=art_meta.get("proposal_id"), review_hash_value=rhash,
        active_book_id=book_id, eligible_market_date=eligible)
    return _envelope(
        status=STATUS_OK, generated_at=generated_at, proposal_payload=payload,
        review=review, governance=governance,
        selected_targets=selected_targets, review_hash_value=rhash,
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
            # R69 - the composition is OBSERVABLE. An operator (and an acceptance
            # test) can see which inputs were recomputed and which were reused,
            # and how long the whole read took. A memo that cannot be seen is a
            # memo that hides a regression.
            "composition": {
                "proposal_payload": payload_memo,
                "outcome_evidence": evidence_memo,
                "return_panel": returns_state,
                "memo_ttl_seconds": MEMO_TTL_SECONDS,
                "compose_ms": int(round((time.monotonic() - _t0) * 1000)),
            },
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
    "STATUS_IDENTITY_MISMATCH", "REVIEW_STATE_VOCAB", "MEMO_TTL_SECONDS",
    "review_hash",
]
