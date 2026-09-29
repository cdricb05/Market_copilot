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
                proposal_hash: Optional[str] = None,
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
    # R82 — the governed risk-policy RULING this frozen book carries.
    #
    # Published as a SIBLING of the selection and never folded into
    # ``selected_target_implementation``: that block is the frozen book, its hash is
    # bound by the selection record and by every order plan built from it, and a
    # ruling recorded afterwards must not move one byte of it. The ruling is a layer
    # over the frozen book, so the screen can state what the operator decided while
    # the book itself stays exactly as it was selected.
    #
    # ``review_hash`` is computed over ``review`` and is untouched here for the same
    # reason the freshness reconciliation is kept out of it.
    ruling = (_pd.risk_policy_ruling_state(selection=selection,
                                           decision_dir=decision_dir)
              if selection else None)
    # R82.1 — the operator DECISION contract: which rulings exist, which are
    # available, what each one does, what the submission must bind, and the tokens
    # that make it governed. Published so the screen can offer the decision the
    # panel has been demanding since R69.5 without holding one rule of its own.
    decision = (_pd.risk_policy_decision(selection=selection, ruling_state=ruling,
                                         decision_dir=decision_dir)
                if selection else None)
    if selection and ruling:
        selection = dict(selection)
        selection["risk_policy_ruling"] = ruling
        selection["risk_policy_decision"] = decision
    # R82.2 — THE APPROVAL GATE. The decision owner's own ordered answer to "is
    # approval the operator's current act on this frozen selection?", published here
    # so every surface on this screen reads ONE verdict.
    #
    # Before this release nothing published it. The status bar derived its own next
    # action from `actionable && a selection exists`, and the Step 2 approve control
    # from `actionable && implementable`, so on 2026-09-28 the panel offered an armed
    # APPROVE MINIMUM REPAIR and reported NEXT REQUIRED ACTION = APPROVE SELECTED
    # TARGET in the same paint as APPROVAL WITHHELD and Step 4's demand for a ruling.
    #
    # It is a SIBLING of the selection, the ruling and the decision, outside
    # ``review`` for the reason stated above ``selected_targets``: ``review_hash`` is
    # computed over ``review`` and every governed selection ever made binds it.
    approval_gate = _pd.selected_target_approval_gate(
        selection=selection, freshness=fresh, ruling_state=ruling,
        current_proposal_hash=proposal_hash, decision_dir=decision_dir)
    return {
        "freshness": fresh,
        "actionable": bool(fresh["actionable"]),
        "target_selection": sel_block,
        "selection": selection,
        "risk_policy_ruling": ruling,
        "risk_policy_decision": decision,
        "approval_gate": approval_gate,
        "approval_gate_owner": _pd.OWNER,
        "risk_policy_ruling_owner": _pd.OWNER,
        "risk_policy_ruling_confirm_token": _pd.RULING_CONFIRM_TOKEN,
        "risk_policy_ruling_route": _pd.RULING_ROUTE,
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
              review_hash_value: Optional[str] = None,
              policy_compliant_successor: Optional[dict] = None) -> dict:
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
        # R82 — the ruling rides the same envelope as the selection it is about, so
        # a reader never has to ask a second route whether a frozen book was ruled
        # on. It is identical to ``governance.risk_policy_ruling``.
        "risk_policy_ruling": (governance or {}).get("risk_policy_ruling"),
        "risk_policy_ruling_confirm_token": (governance or {}).get(
            "risk_policy_ruling_confirm_token"),
        # R82.1 — the operator's OWN step: the decision contract a write surface
        # renders, and the successor target an authoritative ruling produced. Both
        # ride the same envelope as the selection they are about, and both sit
        # OUTSIDE ``review`` for the reason stated above ``selected_targets``.
        "risk_policy_decision": (governance or {}).get("risk_policy_decision"),
        "risk_policy_ruling_route": (governance or {}).get(
            "risk_policy_ruling_route"),
        # R82.2 — the ONE authoritative answer to "is approval the operator's current
        # act?", identical to ``governance.approval_gate``. Every approval affordance
        # and every next-required-action label on this screen reads THIS field; a
        # surface that derives either from freshness, selectability or
        # implementability alone is reading three of the gate's inputs and calling
        # the result a verdict.
        "approval_gate": (governance or {}).get("approval_gate"),
        "approval_gate_owner": (governance or {}).get("approval_gate_owner"),
        "approval_available": bool(
            ((governance or {}).get("approval_gate") or {}).get("available")),
        # R83 — THE TERMINAL GOVERNED OUTCOME, when the reason there is nothing to
        # review is that the proposal owner RULED rather than that it never ran.
        #
        # ``status`` stays NO_PROPOSAL and ``review_state`` stays PROPOSAL_ABSENT on
        # purpose: for a withheld session those ARE the correct answers, and the
        # governed cycle's own coherence check (proposal_review_verification, R74)
        # asserts exactly that pair. What was missing was never the status — it was
        # the REASON and the next act. A withheld verdict is settled, so "refresh the
        # review" is not an action, it is a loop.
        "governed_withheld_outcome": (proposal_payload or {}).get(
            "governed_withheld_outcome"),
        "next_required_action": (
            ((governance or {}).get("approval_gate") or {}).get("next_required_action")
            or ((proposal_payload or {}).get("governed_withheld_outcome")
                or {}).get("next_required_action")),
        "policy_compliant_successor": policy_compliant_successor,
        "policy_compliant_successor_state_vocabulary": list(SUCCESSOR_STATE_VOCAB),
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


# --------------------------------------------------------------------------- #
# R82.1 — THE POLICY-COMPLIANT SUCCESSOR TARGET
#
# R82 reopened AMD and DDOG against the 12% cap an operator ruled binding, refused
# the approval, and told the operator to "select a compliant target". No compliant
# target existed: the only ones on offer were the three the review always publishes,
# and the ruling had just declared one of them non-compliant. That is a requirement,
# not a workflow.
#
# This composition closes it. It hands the reopened obligations back to the SAME
# canonical repair owner with the ruled cap held constant, so the successor is
# solved, not sketched:
#
#   * the obligations are ``engine.proposal_decision_review.repair_obligations``'
#     own, unchanged - the current book's real breaches at the cap the ruling names;
#   * the weights are solved by ``solve_minimum_repair``, the same first-order
#     reduction applied round after round, and the portfolio's risk is RE-MEASURED
#     between rounds by ``engine.reallocation_proposal.portfolio_volatility``,
#     because reducing one name raises every other name's share;
#   * the first-order ``indicative_weight_at_reference`` figures R69.5 publishes are
#     NEVER used as solved weights. They are a sight line for one name in isolation;
#     the successor is a measured book;
#   * the resulting book is verified independently by
#     ``engine.constrained_reallocation.verify_feasibility`` and every mandatory
#     obligation is re-judged against it;
#   * it is turned into a governed target representation by the one owner of those,
#     ``engine.selected_target``, so it carries its own implementation hash and can
#     be selected and approved through the existing gates with no new writer.
#
# There is no second optimiser and no second risk engine here: this module chooses
# WHICH cap to hand the existing owner, and reads what that owner returns.
#
# It is published BESIDE the review, never inside it. ``review_hash`` is computed
# over ``review`` and every governed selection ever made binds it, so the ordinary
# review is returned byte-identical whether or not a ruling exists.
#
# If the repair cannot reach a compliant book it FAILS CLOSED with the open breach
# named. Nothing here approves anything or creates an order plan, order or fill.
# --------------------------------------------------------------------------- #
SUCCESSOR_SOLVED = "SOLVED_AND_COMPLIANT_AT_THE_RULED_CAP"
SUCCESSOR_INFEASIBLE = "NO_FEASIBLE_COMPLIANT_TARGET"
SUCCESSOR_RISK_UNMEASURED = "RISK_NOT_MEASURED_SUCCESSOR_WITHHELD"
SUCCESSOR_NO_BINDING_LIMIT = "THE_RULING_ESTABLISHED_NO_BINDING_LIMIT"
SUCCESSOR_STATE_VOCAB = (SUCCESSOR_SOLVED, SUCCESSOR_INFEASIBLE,
                         SUCCESSOR_RISK_UNMEASURED, SUCCESSOR_NO_BINDING_LIMIT)


def _successor_comparison(*, selected: Optional[dict], successor: dict,
                          instruments: list) -> list:
    """Per-name before/after for the names the ruling reopened. Read, never derived."""
    sel_w = ((selected or {}).get("weights") or {})
    sel_rc = (((selected or {}).get("risk_contribution") or {}).get("after") or {}
              ).get("contributions") or {}
    new_w = successor.get("weights") or {}
    after = (successor.get("risk_contribution") or {}).get("after") or {}
    new_rc = after.get("contributions") or {}
    # The cap the successor was judged against, read off the comparison block the
    # representation owner published. NOT from ``economics``, which carries no
    # risk_contribution_limit key - reading it there silently yielded None and every
    # compliance cell rendered as unknown.
    binding = {"limit": after.get("limit")}
    rows = []
    for tk in sorted(set(instruments) | (set(sel_w) ^ set(new_w))
                     | {t for t in new_w if sel_w.get(t) != new_w.get(t)}):
        if tk not in sel_w and tk not in new_w:
            continue
        rows.append({
            "ticker": tk,
            "reopened_by_the_ruling": tk in set(instruments),
            "weight_selected": sel_w.get(tk),
            "weight_solved": new_w.get(tk),
            "risk_contribution_selected": sel_rc.get(tk),
            "risk_contribution_solved": new_rc.get(tk),
            "binding_limit": binding.get("limit"),
            "complies_at_the_binding_limit": (
                None if new_rc.get(tk) is None or binding.get("limit") is None
                else bool(float(new_rc[tk]) <= float(binding["limit"]) + 1.0e-12)),
            "obligation": ("CLOSED_BY_REDUCTION" if tk in set(instruments)
                           else "NOT_REOPENED_BY_THE_RULING"),
        })
    return rows


def _policy_compliant_successor(*, ruling: Optional[dict], selection: Optional[dict],
                                proposal: dict, hoc_assessment: Optional[dict],
                                outcome_evidence: Optional[dict],
                                aligned_returns: Optional[dict],
                                read_state: Optional[str], identity: dict,
                                proposal_id: Optional[str],
                                review_hash_value: Optional[str],
                                active_book_id: Optional[str],
                                eligible_market_date: Optional[str]) -> Optional[dict]:
    """The successor target an AUTHORITATIVE ruling requires, or None.

    ``None`` whenever no successor is called for, which is the ordinary case and
    includes ``ACCEPT_AS_IS``: a ruling that reopens nothing must not cause a target
    to be built for the sake of building one.
    """
    if not ruling or not ruling.get("authoritative"):
        return None
    if not ruling.get("reopened_obligation_count"):
        return None
    binding_limit = ruling.get("binding_limit")
    instruments = list(ruling.get("instruments_in_breach_of_the_binding_limit")
                       or ruling.get("instruments") or [])
    base = {
        "target": _pd_target(),
        "label": "Policy-compliant repair (solved at the ruled cap)",
        "owner": OWNER,
        "solved_by": kernel.CALCULATION_OWNER,
        "representation_owner": _selected_target.CALCULATION_OWNER,
        "risk_measured_by": "engine.reallocation_proposal.portfolio_volatility",
        "verified_by": "engine.constrained_reallocation.verify_feasibility",
        "state_vocabulary": list(SUCCESSOR_STATE_VOCAB),
        "derived_under": {
            "ruling_id": ruling.get("ruling_id"),
            "ruling": ruling.get("ruling"),
            "binding_limit": binding_limit,
            "binding_limit_source": ruling.get("binding_limit_source"),
            "governed_limit": ruling.get("governed_limit"),
            "reopened_instruments": instruments,
            "superseded_selection_id": (selection or {}).get("selection_id"),
            "superseded_target": (selection or {}).get("selected_target"),
            "superseded_selected_target_implementation_hash": (
                (selection or {}).get("selected_target_implementation_hash")),
        },
        "uses_first_order_indicative_weights": False,
        "weights_are_solved_and_remeasured": True,
        "is_a_new_proposal": False,
        "second_optimiser": False,
        "second_risk_engine": False,
        "is_an_approval": False,
        "creates_order_plan": False,
        "creates_orders": False,
        "approves_nothing": True,
        "manual_approval_still_required": True,
    }
    if binding_limit is None:
        return {**base, "state": SUCCESSOR_NO_BINDING_LIMIT, "available": False,
                "implementation": None,
                "detail": ("The ruling on record established no binding per-name "
                           "cap for this book, so there is no cap to re-solve "
                           "against. No successor target was built and none is "
                           "guessed at.")}
    try:
        ruled_review = kernel.build_review(
            proposal=proposal, hoc_assessment=hoc_assessment,
            outcome_evidence=outcome_evidence, aligned_returns=aligned_returns,
            read_state=read_state, identity=identity,
            binding_risk_contribution_limit=float(binding_limit),
            max_risk_repair_rounds=kernel.RULED_REPAIR_MAX_ROUNDS)
        impl = _selected_target.build_selected_target(
            proposal=proposal, review=ruled_review,
            target=_selected_target.TARGET_MINIMUM_REPAIR,
            identity={**dict(identity or {}), "proposal_id": proposal_id,
                      "review_hash": review_hash_value,
                      "active_book_id": ((identity or {}).get("active_book_id")
                                         or active_book_id),
                      "eligible_market_date": (
                          (identity or {}).get("eligible_market_date")
                          or eligible_market_date)})
    except Exception as exc:  # noqa: BLE001 - a pure read never crashes its caller
        return {**base, "state": SUCCESSOR_INFEASIBLE, "available": False,
                "implementation": None,
                "detail": ("The policy-compliant repair could not be solved: %s. "
                           "No target is published rather than a partial one."
                           % str(exc)[:200])}

    repair = ruled_review.get("repair") or {}
    state_block = (ruled_review.get("states") or {}).get(
        _selected_target.TARGET_MINIMUM_REPAIR) or {}
    cs = state_block.get("constraint_status") or {}
    open_obligations = list(cs.get("obligations_remaining") or [])
    rc_open = list(state_block.get("risk_contribution_breaches") or [])
    measured = state_block.get("risk_measurement_state") == kernel.RISK_MEASURED
    converged = bool(repair.get("risk_repair_converged"))
    # The SAME target name this successor is published under, relabelled on the
    # block so no consumer has to infer which of the four it is looking at.
    impl = dict(impl)
    impl["target"] = _pd_target()
    impl["label"] = base["label"]
    impl["derived_under"] = dict(base["derived_under"])
    impl["solved_target_of"] = _selected_target.TARGET_MINIMUM_REPAIR
    impl["selected_target_implementation_hash"] = _selected_target.selected_target_hash(
        impl)
    solved = bool(measured and converged and not open_obligations and not rc_open
                  and impl.get("implementable"))
    if not measured:
        state = SUCCESSOR_RISK_UNMEASURED
    elif solved:
        state = SUCCESSOR_SOLVED
    else:
        state = SUCCESSOR_INFEASIBLE
    econ = impl.get("economics") or {}
    detail = {
        SUCCESSOR_SOLVED: (
            "The repair was re-solved against the %s cap this ruling makes binding "
            "and reaches a book that breaches it nowhere: %d positions, %s of "
            "one-way turnover, %s cash, and every mandatory obligation closed. The "
            "reduction was applied and the portfolio's risk re-measured over %s "
            "rounds by the canonical covariance owner - these are solved weights, "
            "not the first-order indicative figures. It is a preview: selecting it "
            "is a separate governed act and approving it another."
            % (binding_limit, impl.get("position_count") or 0,
               econ.get("one_way_turnover"), econ.get("cash_weight"),
               repair.get("risk_repair_rounds_measured"))),
        SUCCESSOR_RISK_UNMEASURED: (
            "The repaired book's risk could not be measured, so no compliant "
            "successor is asserted. The target is withheld rather than published "
            "unverified."),
        SUCCESSOR_INFEASIBLE: (
            "No book reachable by the governed repair complies with the %s cap this "
            "ruling makes binding within the declared round budget of %s. The open "
            "breach is named rather than a compliant target fabricated: %s. Reject "
            "or hold this proposal, or revise the ruling."
            % (binding_limit, repair.get("risk_repair_round_budget"),
               ", ".join("%s at %s" % (b.get("ticker"),
                                       b.get("risk_contribution_pct"))
                         for b in rc_open) or
               ", ".join("%s %s" % (o.get("ticker"), o.get("constraint_code"))
                         for o in open_obligations) or "unspecified")),
    }[state]
    return {
        **base,
        "state": state,
        "available": state == SUCCESSOR_SOLVED,
        "selectable_target": (_pd_target() if state == SUCCESSOR_SOLVED else None),
        "implementation": (impl if state == SUCCESSOR_SOLVED else None),
        "implementation_hash": (impl.get("selected_target_implementation_hash")
                               if state == SUCCESSOR_SOLVED else None),
        # Step 9's list, published verbatim from the state the repair owner built.
        "revised_statistics": {
            "positions": state_block.get("positions"),
            "weights": dict(state_block.get("weights") or {}),
            "changes": state_block.get("changes"),
            "one_way_turnover": state_block.get("one_way_turnover"),
            "two_way_turnover": state_block.get("two_way_turnover"),
            "estimated_cost": state_block.get("estimated_cost"),
            "cost_basis": state_block.get("cost_basis"),
            "cash_weight": state_block.get("cash_weight"),
            "invested_weight": state_block.get("invested_weight"),
            "portfolio_volatility": state_block.get("portfolio_volatility"),
            "volatility_basis": state_block.get("volatility_basis"),
            "portfolio_volatility_capital_basis": state_block.get(
                "portfolio_volatility_capital_basis"),
            "volatility_capital_basis": state_block.get("volatility_capital_basis"),
            "concentration": state_block.get("concentration"),
            "largest_position": state_block.get("largest_position"),
            "sector_concentration": state_block.get("sector_concentration"),
            "allocation_by_asset_class": dict(
                state_block.get("allocation_by_asset_class") or {}),
            "risk_contributions": dict(state_block.get("risk_contributions") or {}),
            "risk_contribution_limit": dict(
                state_block.get("risk_contribution_limit") or {}),
            "risk_contribution_breaches": rc_open,
            "binding_cap_breaches": len(rc_open),
            "mandatory_obligations_remaining": len(open_obligations),
            "obligations_remaining": open_obligations,
            "constraint_status_valid": cs.get("valid"),
            "risk_measurement_state": state_block.get("risk_measurement_state"),
            "risk_repair_rounds_measured": repair.get("risk_repair_rounds_measured"),
            "risk_repair_round_budget": repair.get("risk_repair_round_budget"),
            "risk_repair_convergence": repair.get("risk_repair_convergence"),
            "turnover_budget": state_block.get("turnover_budget"),
            "turnover_budget_exceeded": state_block.get("turnover_budget_exceeded"),
        },
        "repair_adjustments": list(state_block.get("repair_adjustments") or []),
        "comparison": _successor_comparison(
            selected=((selection or {}).get("selected_target_implementation") or {}),
            successor=impl, instruments=instruments),
        "ruled_review_hash": review_hash(ruled_review),
        "detail": detail,
    }


def _pd_target() -> str:
    """The successor's governed target name, from the selection writer that owns it."""
    from paper_trader.api import portfolio_decision as _pd  # lazy: import cycle
    return _pd.TARGET_POLICY_COMPLIANT_REPAIR


def _offer_successor(*, governance: dict, successor: Optional[dict],
                     actionable: bool) -> dict:
    """Add the successor to the option list the write gate reads, or leave it alone.

    The option is offered ONLY for a solved, compliant successor on an actionable
    decision. Selectability stays the BACKEND's: a browser renders this answer and
    never derives it.
    """
    if not successor or successor.get("state") != SUCCESSOR_SOLVED:
        return governance
    impl = successor.get("implementation") or {}
    econ = impl.get("economics") or {}
    stats = successor.get("revised_statistics") or {}
    gov = dict(governance)
    block = dict(gov.get("target_selection") or {})
    blockers = ([] if actionable else
                [{"code": kernel.SELECT_BLOCK_NOT_REVIEWABLE,
                  "detail": "This decision is no longer actionable."}])
    option = {
        "target": successor["target"],
        "label": successor["label"],
        "recommended": False,
        "selectable": not blockers,
        "blockers": blockers,
        "blocker_codes": sorted({b["code"] for b in blockers}),
        "positions": impl.get("position_count"),
        "changes": econ.get("changes"),
        "one_way_turnover": econ.get("one_way_turnover"),
        "estimated_cost": econ.get("estimated_cost"),
        "score": econ.get("score"),
        "score_improvement_net_of_cost": econ.get("score_improvement_net_of_cost"),
        "portfolio_volatility": econ.get("portfolio_volatility"),
        "portfolio_volatility_capital_basis": econ.get(
            "portfolio_volatility_capital_basis"),
        "concentration": econ.get("concentration"),
        "largest_position": econ.get("largest_position"),
        "cash_weight": econ.get("cash_weight"),
        "mandatory_obligations_remaining": stats.get(
            "mandatory_obligations_remaining"),
        "obligations_remaining": list(stats.get("obligations_remaining") or []),
        "is_defer": False,
        "expected_return": econ.get("expected_return"),
        "expected_return_state": econ.get("expected_return_state"),
        "score_is_a_percentile_not_a_return": True,
        "score_converted_to_dollars": False,
        "score_basis": econ.get("score_basis"),
        "score_excludes_uninvested_capital": econ.get(
            "score_excludes_uninvested_capital"),
        "invested_weight": econ.get("invested_weight"),
        "evidence_cautions": [],
        "evidence_caution_codes": [],
        "solved_under_ruling": (successor.get("derived_under") or {}).get("ruling_id"),
        "binding_limit": (successor.get("derived_under") or {}).get("binding_limit"),
    }
    options = [o for o in (block.get("options") or [])
               if o.get("target") != option["target"]] + [option]
    block["options"] = options
    block["option_order"] = list(block.get("option_order") or []) + [option["target"]]
    block["selectable_targets"] = sorted(
        o["target"] for o in options if o.get("selectable"))
    block["policy_compliant_successor_offered"] = bool(option["selectable"])
    gov["target_selection"] = block
    return gov


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
        # R83 — say WHY there is nothing to review. "read state NOT_RUN" was the only
        # reason this route could ever give, and on 2026-09-28 it was false: the owner
        # had built a complete target and withheld it on a measured portfolio limit.
        wh = (payload or {}).get("governed_withheld_outcome") or {}
        if wh:
            return _envelope(
                status=STATUS_NO_PROPOSAL, generated_at=generated_at,
                proposal_payload=payload,
                message=(
                    "There is no reallocation proposal to review because the governed "
                    "cycle for this session RULED: it built a complete alternative "
                    "portfolio over %s holdings, re-optimised it under the breached "
                    "limit and WITHHELD it (%s%s). That is a completed governed "
                    "decision, so no artifact is persisted, no target is selectable "
                    "and no approval exists to give. Running the cycle again would "
                    "reach the same verdict; the portfolio limit itself is what needs "
                    "review. Nothing is fabricated."
                    % (wh.get("proposed_holding_count")
                       if wh.get("proposed_holding_count") is not None else "the eligible",
                       ", ".join(wh.get("withheld_codes")
                                 or wh.get("withheld_reasons") or [])
                       or "a governed portfolio limit",
                       (" — %s" % ", ".join(wh.get("withheld_breaching_tickers") or []))
                       if wh.get("withheld_breaching_tickers") else "")))
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
        workflow_state=workflow_state,
        proposal_hash=identity.get("proposal_hash"), decision_dir=decision_dir)
    # R69.2 - the three implementable target representations. The review hash is
    # computed ONCE and travels into both the blocks (which bind it) and the
    # envelope, so the identity a selection freezes is provably the identity the
    # operator was served.
    rhash = review_hash(review)
    selected_targets = _selected_targets(
        proposal=proposal, review=review, identity=identity,
        proposal_id=art_meta.get("proposal_id"), review_hash_value=rhash,
        active_book_id=book_id, eligible_market_date=eligible)
    # R82.1 - the successor target an AUTHORITATIVE ruling requires. Built from the
    # same proposal and the same inputs, by the same repair owner, against the cap
    # the ruling made binding. ``None`` in every ordinary case, including
    # ACCEPT_AS_IS: a ruling that reopens no obligation builds no target.
    successor = _policy_compliant_successor(
        ruling=governance.get("risk_policy_ruling"),
        selection=governance.get("selection"),
        proposal=proposal, hoc_assessment=hoc_assessment,
        outcome_evidence=outcome_evidence, aligned_returns=aligned_returns,
        read_state=read_state, identity=identity,
        proposal_id=art_meta.get("proposal_id"), review_hash_value=rhash,
        active_book_id=book_id, eligible_market_date=eligible)
    if successor and successor.get("implementation"):
        # Offered through the EXISTING selection lane: one more entry in the block
        # the governed write gate already reads, and one more implementable
        # representation in the map it already resolves a selection against.
        selected_targets = dict(selected_targets)
        selected_targets[successor["target"]] = successor["implementation"]
        governance = _offer_successor(governance=governance, successor=successor,
                                     actionable=bool(governance.get("actionable")))
    return _envelope(
        status=STATUS_OK, generated_at=generated_at, proposal_payload=payload,
        review=review, governance=governance,
        selected_targets=selected_targets, review_hash_value=rhash,
        policy_compliant_successor=successor,
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
    "SUCCESSOR_SOLVED", "SUCCESSOR_INFEASIBLE", "SUCCESSOR_RISK_UNMEASURED",
    "SUCCESSOR_NO_BINDING_LIMIT", "SUCCESSOR_STATE_VOCAB",
]
