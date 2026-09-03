r"""Stage 18 — Manual Portfolio-Decision owner (materiality + durable decision ledger).

This is the ONE canonical owner of the *manual portfolio-reallocation decision* that
sits between the review-only Reallocation Proposal (Slice 7, ``api.reallocation_proposal``)
and any future controlled paper-order plan. It closes the "decision" gap that Stage 18
identified: the operational Daily Cycle can complete while a materially-actionable
reallocation proposal (exits / adds / replacements / nontrivial turnover) sits unreviewed
and the active paper book never changes.

What this module OWNS
  1. MATERIALITY — a deterministic verdict, derived ONLY from the immutable proposal's
     own action semantics (never from UI JavaScript, never from realized P&L): a proposal
     is materially actionable when it contains at least one genuine capital-allocation
     change (EXIT / ADD / REPLACE / INCREASE / REDUCE). The engine already gates every
     INCREASE / REDUCE by its own ``material_weight_delta``, so a non-zero change count is
     the proposal's own materiality signal.
  2. A durable, append-only, IDEMPOTENT manual-decision ledger binding an operator
     decision (APPROVE_FOR_PAPER_REBALANCE / REJECT / HOLD) to the EXACT immutable
     proposal: ``proposal_id``, ``proposal_hash`` and the five bound input hashes
     (``portfolio_state_hash``, ``hoc_assessment_hash``, ``universe_scoring_hash``,
     ``universe_input_contract_hash``, ``eligible_market_date`` + ``active_book_id``).
     A decision recorded against one proposal can NEVER be presented as a decision on a
     changed portfolio: if any bound state changed, the decision state becomes
     ``STALE_PROPOSAL_REVIEW_REQUIRED`` and a fresh review is required.
  3. The SEPARATE authoritative portfolio-decision *review state* (a lane distinct from
     the operational ``overall_state`` and from model governance), so a completed Daily
     Close is never conflated with "no capital-redeployment decision to make".
  4. A READ-ONLY paper-order-PLAN PREVIEW derived from an APPROVED proposal's own
     allocations (a deterministic projection — it computes no new target and, critically,
     WRITES NOTHING: no order, no fill, no holding, no cash, no NAV).

What this module NEVER does
  * It creates NO order, NO fill, NO target and mutates NO holding / cash / NAV.
  * It runs NO provider / prediction / research engine (it reads the immutable proposal
    artifact + portfolio state only).
  * It NEVER approves automatically and NEVER promotes / recalibrates a model.
  * Recording a decision requires an explicit manual confirmation token; an identical
    decision on the same proposal is idempotent (no duplicate record).

The decision ledger lives under its own research/decision-evidence root
(``PAPER_TRADER_PORTFOLIO_DECISION_DIR``) — NEVER the operational paper-desk ledger root.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from paper_trader.api import reallocation_proposal as realloc
from paper_trader.engine import constrained_reallocation as _cr

PHASE = "STAGE18"
OWNER = "api.portfolio_decision"

# --- Manual-decision vocabulary -------------------------------------------------- #
DECISION_APPROVE = "APPROVE_FOR_PAPER_REBALANCE"
DECISION_REJECT = "REJECT"
DECISION_HOLD = "HOLD"
DECISION_VOCAB = (DECISION_APPROVE, DECISION_REJECT, DECISION_HOLD)

# Explicit manual confirmation token required to record ANY decision.
CONFIRM_TOKEN = "CONFIRM_PORTFOLIO_REBALANCE_DECISION"

# --- Separate portfolio-decision review-state vocabulary (a lane of its own; it
#     NEVER enters the operational OVERALL_STATES and never gates the Daily Close) --- #
PDS_NO_ACTIVE_BOOK = "PORTFOLIO_DECISION_NO_ACTIVE_BOOK"
PDS_NO_PROPOSAL = "PORTFOLIO_DECISION_NO_PROPOSAL"
PDS_NO_MATERIAL_CHANGE = "NO_MATERIAL_CHANGE"
PDS_REVIEW_REQUIRED = "PROPOSAL_REVIEW_REQUIRED"
PDS_APPROVED = "PROPOSAL_APPROVED"
PDS_REJECTED = "PROPOSAL_REJECTED"
PDS_HELD = "PROPOSAL_HELD"
PDS_STALE = "STALE_PROPOSAL_REVIEW_REQUIRED"
#: Release 29.3 — a COMPLETE candidate target was built and is fully visible, but a
#: portfolio-level limit that only the complete target can settle (turnover budget /
#: concentration / sector concentration / post-change risk) is breached, so the change
#: is WITHHELD. This is materially different from "no proposal yet": deterioration was
#: found and a target was constructed; it simply did not clear the portfolio gates.
#: Never approvable, never executable, and it is NOT outstanding operator work.
PDS_CHANGE_WITHHELD = "CHANGE_CANDIDATE_WITHHELD"
#: Release 47 — a complete FEASIBLE alternative target exists and is fully visible,
#: but its expected improvement does not justify what switching to it would cost. The
#: system has taken the decision to keep the current book; this is an ECONOMIC
#: conclusion about a computed alternative, and it is emphatically NOT the old
#: "a constraint blocked us" state, nor outstanding operator work. Not approvable:
#: rebalancing anyway would be trading because a day passed.
PDS_HOLD_CURRENT_BOOK = "HOLD_CURRENT_BOOK"
#: R54.2.3.2 — a NEWER authoritative governed decision (a later session's governed
#: verdict, or the same session's authoritative assessment concluding from newer
#: evidence) stands, and it does not request/endorse this proposal. The proposal
#: remains immutable, history-visible evidence; it is no longer current, no longer
#: reviewable as outstanding work, and NEVER approvable. This is distinct from
#: PDS_STALE (the proposal changed under the operator mid-review — re-review it):
#: there is nothing to re-review here, because the newer decision already answered
#: the portfolio question.
PDS_SUPERSEDED = "PROPOSAL_SUPERSEDED_BY_NEWER_DECISION"
PDS_UNAVAILABLE = "PORTFOLIO_DECISION_UNAVAILABLE"
DECISION_STATE_VOCAB = (
    PDS_NO_ACTIVE_BOOK, PDS_NO_PROPOSAL, PDS_NO_MATERIAL_CHANGE, PDS_REVIEW_REQUIRED,
    PDS_APPROVED, PDS_REJECTED, PDS_HELD, PDS_STALE, PDS_CHANGE_WITHHELD,
    PDS_HOLD_CURRENT_BOOK, PDS_SUPERSEDED, PDS_UNAVAILABLE)
#: The ONLY states in which any surface may expose an approvable proposal action.
APPROVABLE_DECISION_STATES = (PDS_REVIEW_REQUIRED, PDS_HELD)

# Structural (membership) vs resize action tokens (mirror engine.reallocation_proposal).
_MEMBERSHIP_ACTIONS = ("EXIT", "ADD", "REPLACE_IN", "REPLACE_OUT")
_RESIZE_ACTIONS = ("INCREASE", "REDUCE")

# --- Ledger root (a decision-evidence root, NEVER the operational desk ledger) ----- #
DECISION_DIR_ENV = "PAPER_TRADER_PORTFOLIO_DECISION_DIR"
_DEFAULT_DECISION_DIR = Path(r"D:\Stock_Prediction_app_data\portfolio_decisions")
_RECORDS_FILE = "decisions.json"
_INDEX_FILE = "index.json"


# --------------------------------------------------------------------------- #
# io helpers (same atomic pattern as api.reallocation_proposal)
# --------------------------------------------------------------------------- #
def _now(now: Optional[datetime]) -> datetime:
    return now or datetime.now(timezone.utc)


def _now_iso(now: Optional[datetime]) -> str:
    return _now(now).astimezone(timezone.utc).isoformat()


def _decision_dir(decision_dir=None) -> Path:
    if decision_dir is not None:
        return Path(decision_dir)
    env = os.environ.get(DECISION_DIR_ENV)
    return Path(env) if env else _DEFAULT_DECISION_DIR


def _records_path(decision_dir=None) -> Path:
    return _decision_dir(decision_dir) / _RECORDS_FILE


def _index_path(decision_dir=None) -> Path:
    return _decision_dir(decision_dir) / _INDEX_FILE


def _atomic_write_json(path: Path, payload: Any) -> None:
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


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _index_key(active_book_id: Optional[str], eligible_market_date: Optional[str]) -> str:
    return "%s|%s" % (active_book_id or "?", eligible_market_date or "?")


# --------------------------------------------------------------------------- #
# Materiality — derived ONLY from proposal action semantics
# --------------------------------------------------------------------------- #
def _counts_from_summary(proposal_summary: dict) -> dict:
    return dict((proposal_summary or {}).get("reallocation_action_counts") or {})


def assess_materiality(proposal_summary: dict) -> dict:
    """Deterministic materiality verdict from the proposal's own action counts.

    ``material`` is True iff the proposal contains at least one genuine capital-allocation
    change. Structural membership changes (EXIT / ADD / REPLACE) and engine-gated resizes
    (INCREASE / REDUCE — each already past ``material_weight_delta``) both count. Realized
    P&L is NEVER an input here.
    """
    counts = _counts_from_summary(proposal_summary)

    def _c(k):
        try:
            return int(counts.get(k, 0) or 0)
        except (TypeError, ValueError):
            return 0

    membership = sum(_c(a) for a in _MEMBERSHIP_ACTIONS)
    resize = sum(_c(a) for a in _RESIZE_ACTIONS)
    turnover = (proposal_summary or {}).get("reallocation_one_way_turnover")
    material = bool(membership > 0 or resize > 0)
    return {
        "material": material,
        "membership_change_count": membership,
        "resize_change_count": resize,
        "one_way_turnover": turnover,
        "action_counts": counts,
        "basis": "PROPOSAL_ACTION_SEMANTICS",
        "note": ("Materiality is derived from the immutable proposal's action counts "
                 "(EXIT/ADD/REPLACE membership + engine-gated INCREASE/REDUCE resizes); "
                 "it is never derived from realized P&L or from UI code."),
    }


# --------------------------------------------------------------------------- #
# R54.2.3.2 — PROPOSAL SUPERSESSION BY A NEWER AUTHORITATIVE DECISION.
#
# The live 2026-09-02 defect: a live event cycle produced a 28-change proposal at
# 23:38Z from reassessment evidence that the governed Daily Research Cycle then
# SUPERSEDED at 23:51Z with an authoritative CURRENT_NO_CHANGE conclusion (manifest
# drc_2026-09-02_15abfb01856f: reallocation_proposal_state NOT_REQUIRED). The
# reassessment store recorded the version supersession; the proposal index still
# pointed at the stale artifact, and every "current proposal" read presented it as
# reviewable/approvable — REALLOCATE — 28 POSITIONS CHANGE beside "No change is
# proposed". The authority rule this block owns:
#
#     newer governed completed-session decision
#         > older governed completed-session decision
#         > any older proposal awaiting manual review
#
# and a NON-governed / governance-withheld intraday research result NEVER supersedes
# a governed decision (that is the R54.1 direction, unchanged). There is exactly ONE
# supersession calculation, and it lives here in the canonical decision owner.
# --------------------------------------------------------------------------- #
SUPERSESSION_OWNER = OWNER
#: The assessment decisions that are CONCLUSIVE portfolio verdicts able to supersede
#: a standing proposal. Blocked / not-run / manual-adjudication states are questions,
#: not decisions, and never tear down reviewable work (fail-closed toward review).
SUPERSEDING_ASSESSMENT_DECISIONS = ("CURRENT_NO_CHANGE", "PROPOSAL_READY")
# Supersession reason codes (`superseded` True) / non-supersession reasons (False).
SUP_NEWER_SESSION_DECISION = "NEWER_SESSION_GOVERNED_DECISION"
SUP_NO_CHANGE_DECISION = "SESSION_DECISION_IS_NO_CHANGE"
SUP_NEWER_EVIDENCE_REQUESTED_FRESH_PROPOSAL = "NEWER_EVIDENCE_REQUESTED_FRESH_PROPOSAL"
SUP_NOT_SUPERSEDED_CURRENT = "PROPOSAL_BOUND_TO_STANDING_ASSESSMENT"
SUP_NO_PROPOSAL = "NO_PROPOSAL_TO_SUPERSEDE"
SUP_NO_ASSESSMENT = "NO_AUTHORITATIVE_ASSESSMENT_OBSERVED"
SUP_AUTHORITY_UNPROVEN = "ASSESSMENT_AUTHORITY_UNPROVEN"
SUP_ASSESSMENT_NOT_CONCLUSIVE = "ASSESSMENT_DECISION_NOT_CONCLUSIVE"
SUP_ASSESSMENT_OLDER = "ASSESSMENT_OLDER_THAN_PROPOSAL"
SUP_DIRECTION_UNPROVEN = "SUPERSESSION_DIRECTION_UNPROVEN"


def assess_proposal_supersession(*, proposal_summary: Optional[dict],
                                 assessment: Optional[dict]) -> dict:
    """THE one supersession calculation: is the current proposal outranked by a
    newer authoritative decision? Pure; no io; fail-closed in BOTH directions.

    ``assessment`` is the AUTHORITATIVE assessment view the caller resolved (the
    reassessment store's version-chain head, plus proof of decision authority):
    ``available / decision / eligible_market_date / reassessment_hash /
    artifact_id / generated_at / hoc_assessment_hash (the assessment's OWN
    evidence) / is_governed / governed_manifest_run_id / governed_provenance``.

    ``superseded`` becomes True ONLY when every link is proven:
      * a proposal exists;
      * an authoritative assessment exists AND ``is_governed`` is True — a
        non-governed or governance-withheld intraday result never supersedes;
      * the assessment's decision is a conclusive verdict
        (:data:`SUPERSEDING_ASSESSMENT_DECISIONS`);
      * the direction is newer-onto-older: a LATER session always supersedes; the
        SAME session supersedes when its authoritative conclusion is
        CURRENT_NO_CHANGE (the session's decision requests no proposal), or when
        it requested a proposal from provably different evidence and is not older
        than the standing artifact. An assessment for an EARLIER session never
        supersedes anything.
    Anything unprovable → NOT superseded (the standing review keeps its status).
    """
    summ = proposal_summary or {}
    a = assessment or {}
    base = {
        "owner": SUPERSESSION_OWNER,
        "superseded": False,
        "reason": None,
        "proposal_id": summ.get("reallocation_proposal_id"),
        "proposal_hash": summ.get("reallocation_proposal_hash"),
        "proposal_session": summ.get("reallocation_bound_eligible_market_date"),
        "proposal_bound_hoc_assessment_hash": summ.get(
            "reallocation_bound_hoc_assessment_hash"),
        "superseded_by": None,
    }
    if not summ.get("reallocation_proposal_available"):
        return {**base, "reason": SUP_NO_PROPOSAL}
    if not a or not a.get("decision"):
        return {**base, "reason": SUP_NO_ASSESSMENT}
    if a.get("is_governed") is not True:
        return {**base, "reason": SUP_AUTHORITY_UNPROVEN}
    decision = str(a.get("decision"))
    if decision not in SUPERSEDING_ASSESSMENT_DECISIONS:
        return {**base, "reason": SUP_ASSESSMENT_NOT_CONCLUSIVE,
                "assessment_decision": decision}

    superseded_by = {
        "kind": "GOVERNED_ASSESSMENT",
        "decision": decision,
        "artifact_id": a.get("artifact_id"),
        "reassessment_hash": a.get("reassessment_hash"),
        "session": (str(a.get("eligible_market_date"))[:10]
                    if a.get("eligible_market_date") else None),
        "decided_at": a.get("generated_at"),
        "governed_manifest_run_id": a.get("governed_manifest_run_id"),
        "governed_provenance": a.get("governed_provenance"),
        "owner": "api.portfolio_reassessment (adjudicated by the governed manifest "
                 "/ governed decision lane)",
    }
    a_session = superseded_by["session"]
    p_session = (str(base["proposal_session"])[:10]
                 if base["proposal_session"] else None)
    if a_session and p_session and a_session < p_session:
        return {**base, "reason": SUP_ASSESSMENT_OLDER}
    if a_session and p_session and a_session > p_session:
        return {**base, "superseded": True, "reason": SUP_NEWER_SESSION_DECISION,
                "superseded_by": superseded_by}
    # Same session (or a session side unknown — treated as the same-session
    # comparison, which requires an evidence/decision proof to supersede).
    if decision == "CURRENT_NO_CHANGE":
        # The session's authoritative conclusion requests NO proposal; whatever
        # artifact stands at the proposal key is not endorsed by the decision of
        # record. Timestamps are not required: the store head IS the session's
        # authoritative conclusion by the R54.2 version-chain contract.
        return {**base, "superseded": True, "reason": SUP_NO_CHANGE_DECISION,
                "superseded_by": superseded_by}
    # decision == PROPOSAL_READY: the assessment requested a proposal. If the
    # standing proposal is bound to the SAME evidence, it IS the requested one.
    own = a.get("hoc_assessment_hash")
    bound = base["proposal_bound_hoc_assessment_hash"]
    if own is None or bound is None:
        return {**base, "reason": SUP_DIRECTION_UNPROVEN}
    if str(own) == str(bound):
        return {**base, "reason": SUP_NOT_SUPERSEDED_CURRENT}
    # Different evidence requested a FRESH proposal. Supersede only when the
    # assessment is not provably OLDER than the standing artifact (an unpersisted
    # or refused newer assessment must never be outranked by inference).
    a_at = str(a.get("generated_at") or "")
    p_at = str(summ.get("reallocation_proposal_generated_at") or "")
    if a_at and p_at and a_at < p_at:
        return {**base, "reason": SUP_ASSESSMENT_OLDER}
    return {**base, "superseded": True,
            "reason": SUP_NEWER_EVIDENCE_REQUESTED_FRESH_PROPOSAL,
            "superseded_by": superseded_by}


def load_decision_supersession(*, active_book_id: Optional[str],
                               proposal_summary: Optional[dict],
                               reassessment_dir=None, drc_dir=None,
                               decision_dir=None,
                               assessment: Optional[dict] = None) -> dict:
    """Resolve the authoritative-assessment view from the stores and run THE one
    supersession calculation. Bounded, read-only, degrade-safe.

    Reads (all small immutable-store reads; no engine, no provider, no write):
      1. the reassessment store's newest pointer for the book (R54.2 head);
      2. the governed Daily-Research-Cycle manifest for that session — decision
         authority proof #1 (``governed`` and it binds the head's hash);
      3. the persisted governed intraday decision record — authority proof #2
         (an R54.1 gate-passed record binding the head's hash).
    A hermetic caller supplies ``assessment`` (or the explicit dirs) and never
    touches a production store. Unresolvable authority → NOT superseded.
    """
    if assessment is None:
        ptr = None
        try:
            from paper_trader.api import portfolio_reassessment as _prs
            ptr = _prs.load_latest_assessment_pointer(
                active_book_id=active_book_id, reassessment_dir=reassessment_dir)
        except Exception:  # noqa: BLE001 - a read must never crash the caller
            ptr = None
        if ptr:
            session = ptr.get("eligible_market_date")
            head_hash = ptr.get("reassessment_hash")
            is_governed, run_id, provenance = None, None, None
            try:
                from paper_trader.api import daily_research_cycle as _drc
                ref = _drc.load_governed_manifest_reference(
                    eligible_market_date=session, drc_dir=drc_dir)
            except Exception:  # noqa: BLE001
                ref = None
            if ref and ref.get("governed") and head_hash \
                    and str(ref.get("portfolio_reassessment_hash")) == str(head_hash):
                is_governed = True
                run_id = ref.get("run_id")
                provenance = PROV_GOVERNED_DAILY_CYCLE
            else:
                gov = load_governed_decision_record(
                    active_book_id=active_book_id, decision_dir=decision_dir)
                if gov and head_hash and str(
                        (gov.get("identity") or {}).get("reassessment_hash")) \
                        == str(head_hash):
                    is_governed = True
                    run_id = gov.get("record_id")
                    provenance = gov.get("provenance")
            assessment = {
                "available": True,
                "decision": ptr.get("decision"),
                "eligible_market_date": session,
                "reassessment_hash": head_hash,
                "artifact_id": ptr.get("artifact_id"),
                "generated_at": ptr.get("generated_at"),
                "hoc_assessment_hash": ptr.get("hoc_assessment_hash"),
                "is_governed": is_governed,
                "governed_manifest_run_id": run_id,
                "governed_provenance": provenance,
            }
    return assess_proposal_supersession(proposal_summary=proposal_summary,
                                        assessment=assessment)


# --------------------------------------------------------------------------- #
# Binding — the exact immutable identity a decision is recorded against
# --------------------------------------------------------------------------- #
def _binding_from_artifact(artifact: dict) -> dict:
    art = artifact or {}
    ident = art.get("identity") or {}
    ic = art.get("input_contract") or {}
    prop = art.get("proposal") or {}
    return {
        "proposal_id": art.get("proposal_id"),
        "proposal_hash": ident.get("proposal_hash") or prop.get("proposal_hash"),
        "eligible_market_date": ident.get("eligible_market_date")
        or ic.get("eligible_market_date"),
        "active_book_id": ident.get("active_book_id") or ic.get("active_book_id"),
        "portfolio_state_hash": ident.get("portfolio_state_hash")
        or ic.get("portfolio_state_hash"),
        # Stage 19.1 — the corporate-action registry state this proposal was computed
        # against. Absent on artifacts written before the contract existed (== empty).
        "corporate_actions_hash": ident.get("corporate_actions_hash")
        or ic.get("corporate_actions_hash"),
        "hoc_assessment_hash": ident.get("hoc_assessment_hash")
        or ic.get("hoc_assessment_hash"),
        "universe_scoring_hash": ident.get("universe_scoring_hash")
        or ic.get("universe_scoring_hash"),
        "universe_input_contract_hash": ic.get("universe_input_contract_hash"),
        "allocation_policy_version": ident.get("allocation_policy_version"),
    }


# --------------------------------------------------------------------------- #
# Decision ledger — append-only, idempotent read/write
# --------------------------------------------------------------------------- #
def _latest_pointer(active_book_id, eligible_market_date, decision_dir=None) -> Optional[dict]:
    index = _load_json(_index_path(decision_dir)) or {}
    return index.get(_index_key(active_book_id, eligible_market_date))


def load_decision_record(*, active_book_id: Optional[str],
                         eligible_market_date: Optional[str],
                         decision_dir=None) -> Optional[dict]:
    """The latest recorded decision for an exact (active book, eligible date). PURE
    reader; returns ``None`` when no decision has been recorded. Never raises."""
    try:
        ptr = _latest_pointer(active_book_id, eligible_market_date, decision_dir)
        if not ptr:
            return None
        rid = ptr.get("record_id")
        records = _load_json(_records_path(decision_dir)) or []
        for rec in reversed(records):
            if rec.get("record_id") == rid:
                return rec
        # fall back to pointer's embedded snapshot if the record list was trimmed
        return ptr.get("record")
    except Exception:  # noqa: BLE001 - a pure read must never crash the caller
        return None


def record_decision(*, decision: str, confirm: Optional[str],
                    expected_proposal_hash: Optional[str] = None,
                    active_book_id: Optional[str] = None,
                    eligible_market_date: Optional[str] = None,
                    artifact: Optional[dict] = None,
                    proposal_summary: Optional[dict] = None,
                    actor: Optional[str] = None,
                    decision_dir=None, reallocation_dir=None,
                    reassessment_dir=None, drc_dir=None,
                    supersession: Optional[dict] = None,
                    now: Optional[datetime] = None,
                    portfolio_state: Optional[dict] = None,
                    portfolio_state_loader: Optional[Callable] = None) -> dict:
    """Record a durable manual portfolio-reallocation decision, bound to the EXACT
    current immutable proposal. Idempotent: an identical decision on the same proposal
    hash reuses the existing record (no duplicate). A different decision on the same
    proposal REVISES (a new immutable record; the pointer advances; history preserved).

    Safety gates (each returns a NOT_RECORDED payload and writes nothing):
      * ``confirm`` must equal :data:`CONFIRM_TOKEN`;
      * ``decision`` must be in :data:`DECISION_VOCAB`;
      * a current proposal must exist for the active book + eligible session;
      * the proposal must be materially actionable (nothing to decide otherwise);
      * ``expected_proposal_hash`` (the proposal the operator reviewed) must equal the
        server's CURRENT proposal hash — otherwise ``STALE_PROPOSAL_REVIEW_REQUIRED``
        (a stale proposal can never be approved against a changed portfolio).
    """
    base = {"owner": OWNER, "phase": PHASE, "recorded": False,
            "created_orders": False, "created_fills": False, "changed_holdings": False,
            "changed_cash": False, "changed_nav": False, "paper_only": True,
            "manual_review": True, "automation_off": True}

    if decision not in DECISION_VOCAB:
        return {**base, "status": "INVALID_DECISION",
                "message": "decision must be one of %s" % (DECISION_VOCAB,),
                "decision_vocabulary": list(DECISION_VOCAB)}
    if confirm != CONFIRM_TOKEN:
        return {**base, "status": "DECISION_CONFIRMATION_REQUIRED",
                "message": "Explicit manual confirmation required.",
                "confirm_required_token": CONFIRM_TOKEN}

    # Resolve the current immutable proposal artifact for the active book + eligible date.
    # R54.2.3.2 — remember whether THIS call resolved the proposal itself (the live
    # endpoint path): that is the path that must recompute supersession server-side.
    _server_resolved_proposal = artifact is None
    if artifact is None:
        if active_book_id is None or eligible_market_date is None:
            try:
                ps = portfolio_state if portfolio_state is not None else (
                    (portfolio_state_loader or _default_portfolio_state_loader)())
            except Exception as exc:  # noqa: BLE001
                return {**base, "status": "PORTFOLIO_STATE_UNAVAILABLE",
                        "message": str(exc)[:160]}
            active_book_id = active_book_id or (ps.get("active_book") or {}).get("book_id")
            eligible_market_date = eligible_market_date or (
                ps.get("dates") or {}).get("eligible_market_date")
        if not active_book_id:
            return {**base, "status": PDS_NO_ACTIVE_BOOK,
                    "message": "No active operational book."}
        artifact = realloc.load_latest_artifact(
            active_book_id=active_book_id, eligible_market_date=eligible_market_date,
            reallocation_dir=reallocation_dir)
    if not artifact:
        return {**base, "status": PDS_NO_PROPOSAL,
                "message": "No reallocation proposal exists for the active book / "
                           "eligible session. Run the Daily Research Cycle first."}

    binding = _binding_from_artifact(artifact)
    current_hash = binding.get("proposal_hash")
    summ = proposal_summary or realloc.load_proposal_summary(
        active_book_id=binding.get("active_book_id"),
        eligible_market_date=binding.get("eligible_market_date"),
        artifact=artifact, reallocation_dir=reallocation_dir)
    # R54.2.3.2 fail-closed guard — SERVER-ENFORCED: a proposal superseded by a newer
    # authoritative governed decision can never be approved (or rejected/held — there
    # is no current decision to record on it). On the live endpoint path (this call
    # resolved the proposal itself) — or when the caller supplies the sibling store
    # roots — the verdict is recomputed HERE by the ONE calculation, so a direct
    # endpoint call can never slip past a browser-side rendering. A hermetic caller
    # that injected its whole world is judged on the verdict that world carries.
    sup = supersession
    if sup is None and (summ.get("reallocation_proposal_supersession") is not None
                        or summ.get("reallocation_proposal_superseded") is not None):
        sup = (summ.get("reallocation_proposal_supersession")
               or {"superseded": bool(summ.get("reallocation_proposal_superseded"))})
    if sup is None and (_server_resolved_proposal or reassessment_dir is not None
                        or drc_dir is not None):
        sup = load_decision_supersession(
            active_book_id=binding.get("active_book_id"),
            proposal_summary=summ, reassessment_dir=reassessment_dir,
            drc_dir=drc_dir, decision_dir=decision_dir)
    if sup and sup.get("superseded"):
        by = sup.get("superseded_by") or {}
        return {**base, "status": PDS_SUPERSEDED,
                "message": ("This proposal was superseded by a newer authoritative "
                            "decision (%s for session %s, %s). It remains visible "
                            "as history and can no longer be reviewed as current "
                            "or approved. No decision was recorded."
                            % (by.get("decision") or "governed decision",
                               by.get("session") or "?",
                               by.get("artifact_id")
                               or by.get("governed_manifest_run_id") or "id n/a")),
                "supersession": dict(sup),
                "superseded_by": by,
                "current_proposal_hash": current_hash, "binding": binding}

    # Release 29.3 fail-closed guard: a complete target the proposal owner WITHHELD can
    # never be approved. It is reviewable evidence of a rejected change, not a proposal.
    if summ.get("reallocation_proposal_withheld"):
        return {**base, "status": PDS_CHANGE_WITHHELD,
                "message": ("The complete candidate target did not clear the portfolio-"
                            "level limits owned by engine.reallocation_proposal (%s); the "
                            "change is withheld and cannot be approved."
                            % (", ".join(summ.get("reallocation_withheld_reasons") or [])
                               or "portfolio limit breach")),
                "withheld_reasons": list(summ.get("reallocation_withheld_reasons") or []),
                "binding": _binding_from_artifact(artifact)}

    # Release 47 fail-closed guard: the proposal owner already decided that the
    # feasible alternative is not worth what switching costs. Approving it anyway
    # would be trading because a day passed, which is exactly the behaviour the
    # switching hurdle exists to prevent. The refusal names the ECONOMICS, so it can
    # never be mistaken for the data/constraint blocker above.
    if summ.get("reallocation_outcome") == _cr.OUTCOME_HOLD_CURRENT_BOOK:
        return {**base, "status": PDS_HOLD_CURRENT_BOOK,
                "message": ("A complete feasible alternative target exists and was "
                            "priced, but its expected improvement does not clear the "
                            "switching hurdle after transition cost (%s). The "
                            "current book is the decision; there is nothing to "
                            "approve."
                            % (", ".join(summ.get(
                                "reallocation_outcome_reason_codes") or [])
                               or "below switching hurdle")),
                "reallocation_outcome": _cr.OUTCOME_HOLD_CURRENT_BOOK,
                "switching_hurdle": summ.get("reallocation_switching_hurdle"),
                "feasible_target_exists": bool(
                    summ.get("reallocation_feasible_target_exists")),
                "binding": binding}

    materiality = assess_materiality(summ)
    if not materiality["material"]:
        return {**base, "status": PDS_NO_MATERIAL_CHANGE,
                "message": "The current proposal contains no material capital-allocation "
                           "change; there is nothing to approve.",
                "materiality": materiality, "binding": binding}

    # Stale guard: the operator must be approving the proposal they actually reviewed.
    if expected_proposal_hash is not None and expected_proposal_hash != current_hash:
        return {**base, "status": PDS_STALE,
                "message": "The proposal changed since it was reviewed; a fresh review is "
                           "required before a decision can be recorded.",
                "expected_proposal_hash": expected_proposal_hash,
                "current_proposal_hash": current_hash, "binding": binding}

    # Stage 19.1 corporate-action guard: a proposal computed BEFORE a corporate action was
    # registered describes economic holdings that no longer exist. Its proposal_hash is
    # unchanged (the artifact is immutable), so the hash check above cannot catch it — the
    # registry fingerprint must. Backend-enforced: it can never be approved.
    ca_stale = realloc.corporate_action_staleness(
        artifact=artifact, active_book_id=binding.get("active_book_id"))
    if ca_stale.get("stale"):
        return {**base, "status": PDS_STALE,
                "message": ("A corporate action has been registered since this proposal was "
                            "produced, so it was computed against holdings that no longer "
                            "describe the current portfolio. It cannot be approved. Run the "
                            "Daily Research Cycle to produce a fresh proposal."),
                "stale_reason": ca_stale.get("reason"),
                "corporate_action_staleness": ca_stale,
                "current_proposal_hash": current_hash, "binding": binding}

    # Idempotency: identical decision on the same proposal hash → reuse existing record.
    existing = load_decision_record(active_book_id=binding["active_book_id"],
                                    eligible_market_date=binding["eligible_market_date"],
                                    decision_dir=decision_dir)
    if existing and existing.get("proposal_hash") == current_hash \
            and existing.get("decision") == decision:
        return {**base, "status": "REUSED_EXISTING", "recorded": True, "reused": True,
                "revised": False, "record": existing, "binding": binding,
                "materiality": materiality}

    revised = bool(existing and existing.get("proposal_hash") == current_hash
                   and existing.get("decision") != decision)
    ts = _now_iso(now)
    record_id = "pdec_%s_%s_%s" % (
        binding.get("eligible_market_date") or "nodate",
        binding.get("active_book_id") or "book",
        (current_hash or "")[:12])
    # A revision of an existing decision on the SAME proposal gets a distinct suffix so
    # both immutable records are preserved.
    if revised:
        record_id = record_id + "_r%d" % (int((existing or {}).get("revision", 0)) + 1)

    record = {
        "record_id": record_id,
        "owner": OWNER,
        "decision": decision,
        "recorded_at": ts,
        "actor": actor or "operator",
        "revision": (int((existing or {}).get("revision", 0)) + 1) if revised else 0,
        "supersedes_record_id": existing.get("record_id") if revised else None,
        "proposal_id": binding.get("proposal_id"),
        "proposal_hash": current_hash,
        "binding": binding,
        "materiality": {k: materiality[k] for k in
                        ("material", "membership_change_count", "resize_change_count",
                         "one_way_turnover")},
        "confirm_token": CONFIRM_TOKEN,
    }

    # Append-only write: never rewrite a prior record; only append + advance the pointer.
    records = _load_json(_records_path(decision_dir)) or []
    if not isinstance(records, list):
        records = []
    records.append(record)
    _atomic_write_json(_records_path(decision_dir), records)
    index = _load_json(_index_path(decision_dir)) or {}
    index[_index_key(binding["active_book_id"], binding["eligible_market_date"])] = {
        "record_id": record_id, "decision": decision, "proposal_hash": current_hash,
        "proposal_id": binding.get("proposal_id"), "recorded_at": ts, "record": record}
    _atomic_write_json(_index_path(decision_dir), index)
    return {**base, "status": ("REVISED" if revised else "CREATED"), "recorded": True,
            "reused": False, "revised": revised, "record": record, "binding": binding,
            "materiality": materiality}


# --------------------------------------------------------------------------- #
# Decision-state derivation (the separate portfolio-decision review lane)
# --------------------------------------------------------------------------- #
_STATE_META = {
    PDS_NO_ACTIVE_BOOK: ("No active book", "INFO"),
    PDS_NO_PROPOSAL: ("No proposal yet", "INFO"),
    PDS_NO_MATERIAL_CHANGE: ("No material change", "SUCCESS"),
    PDS_REVIEW_REQUIRED: ("Proposal awaiting manual review", "ATTENTION"),
    PDS_APPROVED: ("Approved for paper rebalance", "INFO"),
    PDS_REJECTED: ("Proposal rejected", "INFO"),
    PDS_HELD: ("Proposal held / deferred", "ATTENTION"),
    PDS_STALE: ("Proposal superseded — fresh review required", "ATTENTION"),
    PDS_CHANGE_WITHHELD: ("Portfolio change withheld", "ATTENTION"),
    PDS_HOLD_CURRENT_BOOK: ("Hold the current book", "SUCCESS"),
    PDS_SUPERSEDED: ("Proposal superseded by a newer decision", "INFO"),
    PDS_UNAVAILABLE: ("Portfolio-decision state unavailable", "ATTENTION"),
}
_DECISION_TO_STATE = {DECISION_APPROVE: PDS_APPROVED, DECISION_REJECT: PDS_REJECTED,
                      DECISION_HOLD: PDS_HELD}


def derive_decision_state(*, has_active_book: bool, proposal_summary: dict,
                          decision_record: Optional[dict]) -> dict:
    """Compose the SEPARATE portfolio-decision review state from (a) the current proposal
    and (b) the latest recorded decision. Pure; no io."""
    summ = proposal_summary or {}
    available = bool(summ.get("reallocation_proposal_available"))
    current_hash = summ.get("reallocation_proposal_hash")
    materiality = assess_materiality(summ)

    # Stage 19.1: a proposal produced before a registered corporate action is stale
    # regardless of any recorded decision — it describes holdings that no longer exist.
    ca_stale = bool(summ.get("reallocation_proposal_stale"))
    # R54.2.3.2: a NEWER authoritative governed decision supersedes the proposal.
    # The verdict is computed ONCE by assess_proposal_supersession and travels with
    # the summary; this derivation consumes it and re-decides nothing.
    supersession = dict(summ.get("reallocation_proposal_supersession") or {})
    superseded = bool(summ.get("reallocation_proposal_superseded")
                      or supersession.get("superseded"))
    # Release 29.3: the complete-target owner withheld the change. A withheld target is
    # reviewable evidence, never an approvable proposal, so it can never reach the
    # manual-review branch below. Fail closed: it outranks materiality.
    withheld = bool(summ.get("reallocation_proposal_withheld"))
    # Release 47: the proposal owner's own authoritative outcome. HOLD_CURRENT_BOOK
    # means a feasible alternative WAS computed and priced and is simply not worth
    # paying for. It is rendered as its own state so an operator is never shown
    # "blocked" for what is actually a considered economic decision.
    outcome = summ.get("reallocation_outcome")
    hold_current_book = bool(outcome == _cr.OUTCOME_HOLD_CURRENT_BOOK)

    if not has_active_book:
        state = PDS_NO_ACTIVE_BOOK
    elif not available:
        state = PDS_NO_PROPOSAL
    elif ca_stale:
        state = PDS_STALE
    elif superseded:
        # R54.2.3.2 — outranks review, hold, withhold AND a recorded decision: a
        # newer authoritative decision has answered the portfolio question, so the
        # proposal (and any decision recorded on it) is history, never current
        # outstanding work. The records themselves stay immutable and visible.
        state = PDS_SUPERSEDED
    elif withheld:
        state = PDS_CHANGE_WITHHELD
    elif not materiality["material"]:
        state = PDS_NO_MATERIAL_CHANGE
    elif hold_current_book:
        state = PDS_HOLD_CURRENT_BOOK
    else:
        rec = decision_record or None
        if rec and rec.get("proposal_hash") == current_hash:
            state = _DECISION_TO_STATE.get(rec.get("decision"), PDS_REVIEW_REQUIRED)
        elif rec and rec.get("proposal_hash") and rec.get("proposal_hash") != current_hash:
            # A prior decision exists but for a DIFFERENT (now superseded) proposal.
            state = PDS_STALE
        else:
            state = PDS_REVIEW_REQUIRED

    label, severity = _STATE_META[state]
    requires_review = state in (PDS_REVIEW_REQUIRED, PDS_STALE, PDS_HELD)
    # R54.2.3.2 — a superseded proposal's economics are HISTORY, never current
    # decision work. The top-level fields a surface reads as "the current
    # proposal's turnover/cost/improvement" go quiet (the immutable artifact and
    # the supersession block keep the numbers), and the published materiality
    # reflects CURRENT outstanding work: none.
    if superseded:
        published_materiality = {
            **materiality, "material": False, "action_counts": {},
            "membership_change_count": 0, "resize_change_count": 0,
            "superseded_note": ("The proposal's own action counts remain on its "
                                "immutable artifact and in the supersession "
                                "block; a newer authoritative decision stands, "
                                "so there is no current change to act on."),
        }
    else:
        published_materiality = materiality
    return {
        "portfolio_decision_state": state,
        "portfolio_decision_state_vocabulary": list(DECISION_STATE_VOCAB),
        "label": label,
        "severity": severity,
        "requires_manual_review": requires_review,
        "material": published_materiality["material"],
        "materiality": published_materiality,
        "proposal_available": available,
        "proposal_hash": current_hash,
        "proposal_id": summ.get("reallocation_proposal_id"),
        "proposal_state": summ.get("reallocation_proposal_state"),
        "proposed_holding_count": (None if superseded else summ.get(
            "reallocation_proposed_holding_count")),
        "one_way_turnover": (None if superseded else summ.get(
            "reallocation_one_way_turnover")),
        "estimated_transaction_cost": (None if superseded else summ.get(
            "reallocation_estimated_transaction_cost")),
        "score_improvement_net_of_cost": (None if superseded else summ.get(
            "reallocation_score_improvement_net_of_cost")),
        # R54.2.3.2 — the supersession verdict, rendered verbatim everywhere.
        "proposal_superseded": superseded,
        "supersession": (supersession or None),
        "superseded_by": supersession.get("superseded_by"),
        "superseded_proposal": ({
            "proposal_id": summ.get("reallocation_proposal_id"),
            "proposal_hash": current_hash,
            "one_way_turnover": summ.get("reallocation_one_way_turnover"),
            "estimated_transaction_cost": summ.get(
                "reallocation_estimated_transaction_cost"),
            "score_improvement_net_of_cost": summ.get(
                "reallocation_score_improvement_net_of_cost"),
            "action_counts": dict(materiality.get("action_counts") or {}),
            "history_only": True,
        } if superseded else None),
        "data_gaps": summ.get("reallocation_data_gaps") or [],
        "decision": (decision_record or {}).get("decision"),
        "decision_bound_proposal_hash": (decision_record or {}).get("proposal_hash"),
        "decision_recorded_at": (decision_record or {}).get("recorded_at"),
        "decision_is_current": bool(decision_record
                                    and decision_record.get("proposal_hash") == current_hash
                                    and not ca_stale and not superseded),
        # Stage 19.1 — the explicit approvability contract (backend-enforced; the UI
        # renders it and never decides it).
        "corporate_action_stale": ca_stale,
        "corporate_action_stale_reason": summ.get("reallocation_proposal_stale_reason"),
        # Release 29.3 — the complete-target withhold verdict, rendered verbatim.
        "change_withheld": withheld,
        "withheld_reasons": list(summ.get("reallocation_withheld_reasons") or []),
        # Release 47 — the authoritative outcome and what the constraints did, both
        # rendered verbatim from the proposal owner. No surface re-derives them.
        "reallocation_outcome": outcome,
        "reallocation_outcome_vocabulary": list(_cr.OUTCOME_VOCAB),
        "reallocation_outcome_headline": summ.get("reallocation_outcome_headline"),
        "reallocation_outcome_reason_codes": list(
            summ.get("reallocation_outcome_reason_codes") or []),
        "hold_current_book": hold_current_book,
        "feasible_target_exists": bool(
            summ.get("reallocation_feasible_target_exists")),
        "constraints_that_reshaped": list(
            summ.get("reallocation_constraints_reshaped") or []),
        "constraint_reoptimized": bool(
            summ.get("reallocation_constraint_reoptimized")),
        "switching_hurdle": summ.get("reallocation_switching_hurdle"),
        "clears_switching_hurdle": summ.get("reallocation_clears_switching_hurdle"),
        "approvable": bool(available and materiality["material"] and not ca_stale
                           and not superseded and not withheld
                           and not hold_current_book),
        "owner": OWNER,
        "confirm_required_token": CONFIRM_TOKEN,
        "decision_vocabulary": list(DECISION_VOCAB),
    }


# --------------------------------------------------------------------------- #
# READ-ONLY paper-order-PLAN PREVIEW from an APPROVED proposal (writes nothing)
# --------------------------------------------------------------------------- #
def build_order_plan_preview(*, artifact: dict) -> dict:
    """Deterministic, READ-ONLY projection of the approved proposal's own allocations into
    a paper-order plan shape (SELL/REDUCE vs BUY/ADD/INCREASE, proceeds, purchases, cost,
    residual cash). It computes no new target and writes nothing — it is a preview of what
    a future controlled paper-order slice would reconcile. Whole-share rounding is reported
    as a documented residual, never silently applied to a live book here."""
    art = artifact or {}
    prop = art.get("proposal") or {}
    allocs = prop.get("allocations") or []
    turnover = prop.get("turnover") or {}
    portfolio = prop.get("portfolio") or {}

    sells, buys = [], []
    for a in allocs:
        action = a.get("action")
        cap = a.get("capital_change")
        row = {"ticker": a.get("ticker"), "action": action, "sector": a.get("sector"),
               "current_weight": a.get("current_weight"),
               "proposed_weight": a.get("proposed_weight"),
               "delta_weight": a.get("delta_weight"), "capital_change": cap,
               "current_market_value": a.get("current_market_value"),
               "proposed_market_value": a.get("proposed_market_value"),
               "rank": a.get("rank")}
        if cap is None:
            continue
        if cap < 0 or action in ("EXIT", "REDUCE", "REPLACE_OUT"):
            row["side"] = "SELL"
            sells.append(row)
        elif cap > 0 or action in ("ADD", "INCREASE", "REPLACE_IN"):
            row["side"] = "BUY"
            buys.append(row)

    est_proceeds = turnover.get("gross_sells")
    est_purchases = turnover.get("gross_buys")
    est_cost = turnover.get("estimated_transaction_cost")
    current_cash = portfolio.get("current_cash")
    proposed_cash = portfolio.get("proposed_cash")
    return {
        "preview_only": True,
        "creates_orders": False,
        "wrote_to_ledger": False,
        "owner": OWNER,
        "derived_from_proposal_id": art.get("proposal_id"),
        "derived_from_proposal_hash": (prop.get("proposal_hash")
                                       or (art.get("identity") or {}).get("proposal_hash")),
        "sell_orders": sorted(sells, key=lambda r: (r.get("capital_change") or 0)),
        "buy_orders": sorted(buys, key=lambda r: -(r.get("capital_change") or 0)),
        "sell_count": len(sells),
        "buy_count": len(buys),
        "estimated_proceeds": est_proceeds,
        "estimated_purchases": est_purchases,
        "estimated_transaction_cost": est_cost,
        "current_cash": current_cash,
        "proposed_cash": proposed_cash,
        "one_way_turnover": turnover.get("one_way_turnover"),
        "whole_share_policy_note": ("This preview reports proposal capital deltas; a future "
                                    "controlled paper-order slice reconciles whole shares, "
                                    "minimum-order and residual cash against the desk. No "
                                    "order is created here."),
        "execution_note": ("Execution, when a future slice enables it, is the EXISTING "
                           "Paper Desk NEXT_CLOSE path (no same-close hindsight fill); this "
                           "module never creates a second execution engine."),
    }


# --------------------------------------------------------------------------- #
# Default loaders (injectable seams)
# --------------------------------------------------------------------------- #
def _default_portfolio_state_loader() -> dict:
    from paper_trader.api import portfolio_state as ps
    return ps.load_portfolio_state()


# --------------------------------------------------------------------------- #
# GET read contract — /v1/operations/portfolio-decision
# --------------------------------------------------------------------------- #
def _safety() -> dict:
    return {
        "read_only": True, "preview_only": True, "manual_review": True,
        "paper_only": True, "automation_off": True,
        "created_orders": False, "created_fills": False, "created_order_plan": False,
        "created_target": False, "changed_holdings": False, "changed_cash": False,
        "changed_nav": False, "wrote_to_ledger": False, "wrote_to_database": False,
        "called_provider": False, "called_prediction": False,
        "automatic_promotion_allowed": False, "promoted_model": False,
        "recalibrated_model": False, "broker_enabled": False, "live_orders_enabled": False,
        "safety_badges": ["READ ONLY", "PREVIEW ONLY", "MANUAL REVIEW", "NO ORDERS",
                          "NO BROKER", "AUTOMATION OFF"],
    }


def _safe_supersession(**kwargs) -> Optional[dict]:
    """R54.2.3.2 — degrade-safe wrapper: an unreadable store never crashes a read
    and never fabricates a verdict (None means 'no verdict resolved')."""
    try:
        return load_decision_supersession(**kwargs)
    except Exception:  # noqa: BLE001 - a pure read must never crash the caller
        return None


def load_portfolio_decision(*, portfolio_state: Optional[dict] = None,
                            proposal_summary: Optional[dict] = None,
                            artifact: Optional[dict] = None,
                            decision_record: Optional[dict] = None,
                            decision_dir=None, reallocation_dir=None,
                            reassessment_dir=None, drc_dir=None,
                            supersession: Optional[dict] = None,
                            now: Optional[datetime] = None,
                            portfolio_state_loader: Optional[Callable] = None) -> dict:
    """The read contract. READ-ONLY: reads the immutable proposal summary + the latest
    recorded decision and composes the separate portfolio-decision review lane (plus a
    read-only order-plan preview when the current proposal is APPROVED). Degrade-safe."""
    generated_at = _now_iso(now)
    try:
        ps = portfolio_state if portfolio_state is not None else (
            (portfolio_state_loader or _default_portfolio_state_loader)())
    except Exception as exc:  # noqa: BLE001
        return {"phase": PHASE, "owner": OWNER, "status": "UNAVAILABLE",
                "generated_at": generated_at,
                "portfolio_decision_state": PDS_UNAVAILABLE,
                "message": "Portfolio state unavailable: %s" % str(exc)[:160],
                **_safety()}

    ab = (ps or {}).get("active_book") or {}
    active_book_id = ab.get("book_id")
    eligible = ((ps or {}).get("dates") or {}).get("eligible_market_date")

    _loaded_summary_default = proposal_summary is None and reallocation_dir is None
    if proposal_summary is None:
        proposal_summary = realloc.load_proposal_summary(
            active_book_id=active_book_id, eligible_market_date=eligible,
            artifact=artifact, reallocation_dir=reallocation_dir)
    if decision_record is None:
        decision_record = load_decision_record(
            active_book_id=active_book_id, eligible_market_date=eligible,
            decision_dir=decision_dir)

    # R54.2.3.2 — resolve the supersession verdict ONCE (unless the composition or a
    # hermetic caller already supplied it via ``supersession`` or summary fields) and
    # attach it to the summary the lane derivation consumes, so this read can never
    # present a proposal a newer authoritative decision has superseded as current.
    # The default resolution runs on the PRODUCTION-DEFAULT read (the live GET
    # path) or when the caller supplied the sibling store roots; an injected
    # hermetic summary/world without verdict fields stays a constructed world.
    if proposal_summary.get("reallocation_proposal_superseded") is None \
            and "reallocation_proposal_supersession" not in proposal_summary:
        sup = supersession
        if sup is None and (_loaded_summary_default or reassessment_dir is not None
                            or drc_dir is not None):
            sup = _safe_supersession(
                active_book_id=active_book_id, proposal_summary=proposal_summary,
                reassessment_dir=reassessment_dir, drc_dir=drc_dir,
                decision_dir=decision_dir)
        if sup is not None:
            proposal_summary = {**proposal_summary,
                                "reallocation_proposal_superseded": bool(
                                    sup.get("superseded")),
                                "reallocation_proposal_supersession": dict(sup)}

    lane = derive_decision_state(has_active_book=bool(active_book_id),
                                 proposal_summary=proposal_summary,
                                 decision_record=decision_record)

    order_plan_preview = None
    if lane["portfolio_decision_state"] == PDS_APPROVED:
        art = artifact if artifact is not None else realloc.load_latest_artifact(
            active_book_id=active_book_id, eligible_market_date=eligible,
            reallocation_dir=reallocation_dir)
        if art:
            order_plan_preview = build_order_plan_preview(artifact=art)

    return {
        "phase": PHASE, "owner": OWNER, "status": "OK", "generated_at": generated_at,
        "active_book_id": active_book_id, "active_book_label": ab.get("book_label"),
        "eligible_market_date": eligible,
        **lane,
        "order_plan_preview": order_plan_preview,
        "sole_decision_path": "POST /v1/operations/portfolio-decision/record",
        "regenerate_proposal_path": "POST /v1/operations/daily-research-cycle/run",
        **_safety(),
    }


# =========================================================================== #
# R54.1 — THE ONE GOVERNED INTRADAY DECISION GATE
# =========================================================================== #
r"""Why this lives HERE, inside the decision owner.

Before R54.1 the live event path (``api.event_signal_refresh`` -> the canonical
HOC / reassessment / target owners) could produce, intraday, a COMPLETE priced
answer to the portfolio question — and that answer's provenance was permanently
``LIVE_PRE_DRC_SIGNAL`` because ``governed_research_evidence_current`` is true
only for a validated Daily-Research-Cycle run manifest (Release 29.5). The
system could therefore KNOW at 13:42 that the portfolio had been reassessed
against new information and that the priced conclusion was HOLD or CHANGE,
while the AUTHORITATIVE recommendation on every surface remained the previous
DRC-governed decision. Safe, but not an active manager.

R54.1 closes exactly that gap and nothing else. It adds ONE gate answering ONE
question:

    "Is this intraday reassessment sufficiently complete, fresh, point-in-time
     bound and internally consistent that it can REPLACE the prior governed
     portfolio decision as the latest authoritative RECOMMENDATION?"

That question is emphatically NOT "should the portfolio trade?". The answer to
the second question is still, always, NO: a governed CHANGE is a recommendation
that requires the same manual review, the same approval token and the same
Stage-19 order-plan confirmation as before. This module creates no order, no
fill, no approval, no target and no model promotion, and it never advances the
operational close mark.

OWNERSHIP. The gate is code inside the CANONICAL DECISION OWNER. There is no
second governance framework, no second decision engine and no second economics:
every threshold, hurdle, outcome and constraint verdict is READ VERBATIM from
the owner that decided it (``engine.constrained_reallocation`` via
``api.reallocation_proposal``; ``api.portfolio_reassessment``;
``api.holding_opportunity_cost``; ``api.universe_scoring``;
``api.workflow_state``). The gate decides ADMISSIBILITY, never economics.

STORAGE. Governed decisions are appended to the GOVERNED LANE of this owner's
own ledger root (``governed_decisions.json`` + ``governed_index.json``),
alongside — never mixed into — the manual operator-decision lane
(``decisions.json``). They are two different objects: the manual lane records
what the OPERATOR decided about an approvable proposal; the governed lane
records which RECOMMENDATION is currently authoritative and where it came from.
Writing a governed record into the manual pointer index would make
``load_decision_record`` return a system record where a caller expects an
operator record, and ``derive_decision_state`` would then demand review of a
question the system had already settled. Same owner, same root, same
append-only atomic writer, two lanes.

IMMUTABILITY. A recorded governed decision is never rewritten. A newer decision
SUPERSEDES it by appending a record that names it in ``supersedes_decision_id``.
"""

GOVERNANCE_GATE_VERSION = "intraday_decision_governance.v1"
#: There is exactly ONE intraday-governance owner, and it is this module.
GOVERNANCE_GATE_OWNER = OWNER

# --- Provenance: WHERE an authoritative decision came from ------------------ #
#: A validated Daily-Research-Cycle run manifest produced it (Release 29.5).
PROV_GOVERNED_DAILY_CYCLE = "GOVERNED_DAILY_CYCLE"
#: The live intraday chain produced it AND it passed this module's gate.
PROV_GOVERNED_INTRADAY = "GOVERNED_INTRADAY"
#: Real, current, displayable live signal state that has NOT been governed. It
#: is never the authoritative decision. (Mirrors api.workflow_state's literal.)
PROV_LIVE_PRE_DRC_SIGNAL = "LIVE_PRE_DRC_SIGNAL"
GOVERNED_PROVENANCE_VOCAB = (PROV_GOVERNED_DAILY_CYCLE, PROV_GOVERNED_INTRADAY)
DECISION_PROVENANCE_VOCAB = (PROV_GOVERNED_DAILY_CYCLE, PROV_GOVERNED_INTRADAY,
                             PROV_LIVE_PRE_DRC_SIGNAL)
#: Deterministic tie-break ONLY (used when two governed decisions carry the
#: identical decision timestamp): the session-terminal governed cycle outranks an
#: intraday promotion. It never reorders decisions that differ in time.
_PROVENANCE_RANK = {PROV_GOVERNED_DAILY_CYCLE: 2, PROV_GOVERNED_INTRADAY: 1}

# --- Gate verdicts ---------------------------------------------------------- #
GATE_ELIGIBLE = "GOVERNED_INTRADAY_DECISION_ELIGIBLE"
GATE_WITHHELD = "INTRADAY_DECISION_WITHHELD"
GATE_VERDICT_VOCAB = (GATE_ELIGIBLE, GATE_WITHHELD)

# --- The two governed decisions. BOTH are real decisions. ------------------- #
#: A complete feasible alternative was priced and is not worth what switching
#: costs. Holding IS the decision. (Same word the R47 kernel and the manual lane
#: already use — no second vocabulary.)
GD_HOLD_CURRENT_BOOK = PDS_HOLD_CURRENT_BOOK
#: A complete feasible target clears the switching hurdle. This updates the
#: authoritative RECOMMENDATION; it approves and executes nothing.
GD_CHANGE_RECOMMENDED = "CHANGE_RECOMMENDED"
#: R54.2.3.2 — the reassessment owner's own word, reused (never re-spelled): the
#: governed cycle concluded the current portfolio remains the best use of capital
#: and requested NO proposal. It is a real decision (distinct from HOLD_CURRENT_BOOK,
#: where a feasible alternative WAS priced and rejected on its economics) and it
#: carries no manual-review obligation.
GD_NO_CHANGE = "CURRENT_NO_CHANGE"
GOVERNED_DECISION_VOCAB = (GD_HOLD_CURRENT_BOOK, GD_CHANGE_RECOMMENDED,
                           GD_NO_CHANGE)

#: Position-level recommendation words, read verbatim from the proposal owner's
#: own action vocabulary. This module maps nothing and invents nothing.
POSITION_RECOMMENDATION_VOCAB = ("HOLD", "REDUCE", "EXIT", "REPLACE_OUT",
                                 "REPLACE_IN", "ADD", "INCREASE")

#: Recording a governed decision is a SYSTEM action derived from a passed gate,
#: not an operator approval — it carries its own token so it can never be
#: confused with, or satisfy, the manual approval token above.
GOVERNED_DECISION_CONFIRM_TOKEN = "CONFIRM_GOVERNED_INTRADAY_DECISION"

# --- Withheld-reason taxonomy (Phase J). Canonical codes are REUSED. -------- #
WR_NO_ACTIVE_BOOK = PDS_NO_ACTIVE_BOOK
WR_PORTFOLIO_IDENTITY_STALE = "PORTFOLIO_IDENTITY_STALE"
WR_MARKET_DATA_STALE = "MARKET_DATA_STALE"
#: The book's OWN eligible session is not confirmed by owned data. Reused
#: verbatim from api.workflow_state's blocker code — one spelling, one meaning.
WR_OWNED_DATA_NOT_CONFIRMED = "OWNED_DATA_NOT_CONFIRMED"
WR_POINT_IN_TIME = "POINT_IN_TIME_INTEGRITY_FAILURE"
WR_RANKING_IDENTITY = "RANKING_IDENTITY_MISMATCH"
WR_HOC_IDENTITY = "HOC_IDENTITY_MISMATCH"
#: Release 54.3 — the opportunity-cost assessment this candidate depends on was
#: computed but never became an immutable artifact. A hash that cannot be produced
#: as evidence is not evidence, so a governed decision may never stand on it.
WR_HOC_NOT_PERSISTED = "HOC_ARTIFACT_NOT_PERSISTED"
#: Release 54.3 — an artifact IS named, but what the store holds under that id is
#: not the assessment this candidate claims (different hash, book or session).
WR_HOC_ARTIFACT_MISMATCH = "HOC_ARTIFACT_IDENTITY_MISMATCH"
WR_REASSESSMENT_IDENTITY = "REASSESSMENT_IDENTITY_MISMATCH"
WR_TARGET_IDENTITY = "TARGET_IDENTITY_MISMATCH"
WR_SWITCHING_ECONOMICS = "SWITCHING_ECONOMICS_INCOMPLETE"
#: The target owner's own third outcome — reused, never re-spelled.
WR_TRUE_BLOCKER = "TRUE_BLOCKER"
#: The complete target breached a mandatory portfolio limit. Canonical decision
#: state, reused as the reason a governed promotion is refused.
WR_CHANGE_WITHHELD = PDS_CHANGE_WITHHELD
WR_SUPERSEDED = "SUPERSEDED_BY_NEWER_DECISION"
WR_DUPLICATE = "DUPLICATE_CANDIDATE"
WR_EXECUTION_PRECEDENCE = "EXECUTION_PRECEDENCE"
WR_EVIDENCE_INCOMPLETE = "CANDIDATE_EVIDENCE_INCOMPLETE"
WITHHELD_REASON_VOCAB = (
    WR_NO_ACTIVE_BOOK, WR_PORTFOLIO_IDENTITY_STALE, WR_MARKET_DATA_STALE,
    WR_OWNED_DATA_NOT_CONFIRMED, WR_POINT_IN_TIME, WR_RANKING_IDENTITY,
    WR_HOC_IDENTITY, WR_HOC_NOT_PERSISTED, WR_HOC_ARTIFACT_MISMATCH,
    WR_REASSESSMENT_IDENTITY, WR_TARGET_IDENTITY,
    WR_SWITCHING_ECONOMICS, WR_TRUE_BLOCKER, WR_CHANGE_WITHHELD,
    WR_SUPERSEDED, WR_DUPLICATE, WR_EXECUTION_PRECEDENCE,
    WR_EVIDENCE_INCOMPLETE)

#: The governed lane of this owner's ledger (see the module note above).
_GOVERNED_RECORDS_FILE = "governed_decisions.json"
_GOVERNED_INDEX_FILE = "governed_index.json"

#: The event-cycle states that can carry a promotable candidate. A duplicate
#: trigger is the anti-churn refusal itself and is never promoted.
_PROMOTABLE_CYCLE_STATES = ("PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW",
                            "REASSESSED_NO_CHANGE")
#: The R47 outcomes that are CONCLUSIVE portfolio answers.
_OUTCOME_PROPOSAL_READY = _cr.OUTCOME_PROPOSAL_READY
_OUTCOME_HOLD = _cr.OUTCOME_HOLD_CURRENT_BOOK
_OUTCOME_TRUE_BLOCKER = _cr.OUTCOME_TRUE_BLOCKER

#: Reassessment states whose OWN word is "the inputs were not good enough".
_REASSESS_BLOCKED_DATA = "BLOCKED_DATA"
_REASSESS_BLOCKED_EVIDENCE = "BLOCKED_EVIDENCE"

#: The zero-base proof this gate BINDS (it never re-derives it): the target
#: kernel gives a current holding no investment privilege beyond a priced
#: transition cost. See engine.constrained_reallocation.INCUMBENCY_POLICY.
ZERO_BASE_INCUMBENCY_POLICY = _cr.INCUMBENCY_POLICY


def _governed_records_path(decision_dir=None) -> Path:
    return _decision_dir(decision_dir) / _GOVERNED_RECORDS_FILE


def _governed_index_path(decision_dir=None) -> Path:
    return _decision_dir(decision_dir) / _GOVERNED_INDEX_FILE


def candidate_identity_hash(identity: dict) -> str:
    """The deterministic identity of a governed-decision CANDIDATE.

    Covers the EVIDENCE only — active book, eligible session, portfolio /
    economic / corporate-action state, ranking identity, HOC, reassessment,
    target and the target owner's outcome. It deliberately excludes the event
    cycle's run id, its wall clock and the materiality trigger fingerprint:
    two different triggers that reach the SAME conclusion from the SAME evidence
    are the same decision, and re-deciding it would be churn dressed as
    governance. The trigger fingerprint is still BOUND into the record (it is
    part of the provenance an auditor needs); it is simply not part of identity.
    """
    blob = json.dumps(identity or {}, sort_keys=True, ensure_ascii=False,
                      default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def _governed_safety() -> dict:
    """Structural safety of the governed lane. Every one of these is a property
    of the code, not a runtime preference: this module has no order, fill,
    approval, promotion, sleeve-activation, close or scheduler path at all."""
    return {
        "paper_only": True,
        "manual_review_required_for_change": True,
        "automation_enabled": False,
        "broker_enabled": False,
        "created_orders": False,
        "created_order_plan": False,
        "created_fills": False,
        "approved_anything": False,
        "automatic_approval_allowed": False,
        "promoted_model": False,
        "automatic_model_promotion_allowed": False,
        "activated_sleeve": False,
        "automatic_sleeve_activation_allowed": False,
        "changed_holdings": False,
        "changed_cash": False,
        "changed_nav": False,
        "ran_daily_close": False,
        "advances_operational_mark": False,
        "operational_mark_advanced_only_by": "api.daily_close",
        "rewrote_history": False,
        "safety_badges": ["PREVIEW ONLY", "MANUAL REVIEW", "NO ORDERS",
                          "ORDERS DISABLED", "AUTOMATION OFF"],
    }


def _check(group: str, name: str, passed: bool, owner: str, detail: str,
           reason_code: Optional[str] = None) -> dict:
    return {"group": group, "check": name, "passed": bool(passed),
            "owner": owner, "detail": detail,
            "reason_code": (None if passed else reason_code)}


#: The evidence fields EVERY governed decision carries — a persisted intraday
#: record and a projected daily-cycle decision alike. Two decisions agreeing on
#: all five describe the same conclusion from the same evidence, whatever else
#: their fuller identities record.
_CORE_EVIDENCE_KEYS = ("active_book_id", "eligible_market_session",
                       "reassessment_hash", "proposal_hash", "target_outcome")


def _core_evidence(identity: Optional[dict]) -> tuple:
    return tuple((identity or {}).get(k) for k in _CORE_EVIDENCE_KEYS)


def _eq_when_known(a: Any, b: Any) -> Optional[bool]:
    """True/False only when BOTH sides are known; None means "not comparable".

    A missing identity is never silently treated as a match — the caller decides
    whether "not comparable" is admissible for that particular binding.
    """
    if a is None or b is None:
        return None
    return str(a) == str(b)


# --------------------------------------------------------------------------- #
# The CANDIDATE — assembled from owner payloads, never computed here
# --------------------------------------------------------------------------- #
def build_intraday_candidate(*, portfolio_state: Optional[dict],
                             event_cycle: Optional[dict],
                             reassessment: Optional[dict],
                             proposal_summary: Optional[dict],
                             constrained: Optional[dict] = None,
                             scoring_identity: Optional[dict] = None,
                             workflow: Optional[dict] = None,
                             hoc_binding: Optional[dict] = None,
                             observation_received_at: Any = None,
                             now: Optional[datetime] = None) -> dict:
    """Assemble ONE governed-decision candidate out of the owners' own payloads.

    Pure and io-free. Every field is copied verbatim from the owner named beside
    it; nothing is derived, averaged, defaulted or re-decided. ``event_cycle`` is
    ``api.event_signal_refresh``'s ``last_run_summary`` (the store owner's own
    summary of its persisted run payload).

    Release 54.3 — ``hoc_binding`` is
    ``api.holding_opportunity_cost.resolve_binding``'s answer to "does the exact
    opportunity-cost artifact this evidence claims actually exist on disk?". The io
    belongs to the artifact's owner; this function only records the answer, and the
    gate only reads it. When no binding is supplied the retrievability fields
    resolve to False and the gate fails closed, which is the correct behaviour for
    a dependency nobody was able to prove.
    """
    ps = portfolio_state or {}
    ev = event_cycle or {}
    rs = reassessment or {}
    summ = proposal_summary or {}
    con = constrained or {}
    sc = scoring_identity or {}
    wf = workflow or {}
    op = wf.get("operational_state") or {}
    rcs = wf.get("research_cycle_state") or {}

    book = (ps.get("active_book") or {})
    active_book_id = book.get("book_id") or ev.get("active_book_id")
    eligible = ((ps.get("dates") or {}).get("eligible_market_date")
                or ev.get("eligible_market_date"))
    # ``proposal_binding`` is the reassessment owner's OWN published identity map
    # — "the provenance a proposal generated by this reassessment MUST carry".
    # It is the authoritative source here; the artifact identity and the free
    # provenance block are read only where it is silent.
    prov = dict(rs.get("proposal_binding") or {})
    for fallback in ((rs.get("artifact") or {}).get("identity") or {},
                     rs.get("provenance") or {}):
        for k, v in fallback.items():
            if prov.get(k) is None and v is not None:
                prov[k] = v

    econ = con.get("switching_economics") or {}
    outcome = con.get("outcome") or summ.get("reallocation_outcome")

    # R54.3 — the opportunity-cost binding. Preference order is the strongest
    # available proof first: an explicitly RESOLVED binding (the owner actually
    # opened the artifact), then the cycle's own published persistence outcome,
    # then the reassessment's recorded dependency. Nothing is defaulted to True.
    hb = dict(hoc_binding or {})
    if not hb:
        hb = dict((event_cycle or {}).get("hoc_binding") or {})
    hoc_artifact_id = (hb.get("hoc_artifact_id") or ev.get("hoc_artifact_id")
                       or prov.get("hoc_artifact_id"))
    hoc_persisted = hb.get("hoc_persisted")
    if hoc_persisted is None:
        hoc_persisted = (event_cycle or {}).get("hoc_persisted")
    if hoc_persisted is None:
        hoc_persisted = prov.get("hoc_persisted")
    hoc_evidence_hash = (hb.get("hoc_assessment_evidence_hash")
                         or (event_cycle or {}).get("hoc_assessment_evidence_hash")
                         or prov.get("hoc_assessment_evidence_hash"))

    identity = {
        "active_book_id": active_book_id,
        "eligible_market_session": eligible,
        # The hashes the EVIDENCE was built against — the reassessment owner's
        # own bound portfolio-state hash and the hash the event cycle bound —
        # NOT a re-read of the live state. The gate's job is precisely to prove
        # those still describe the portfolio that exists now.
        "portfolio_state_hash": (prov.get("portfolio_state_hash")
                                 or ps.get("state_hash")),
        "economic_state_hash": (ev.get("portfolio_state_hash")
                                or ps.get("economic_state_hash")),
        "corporate_actions_hash": (summ.get("reallocation_corporate_actions_hash")
                                   or prov.get("corporate_actions_hash")),
        "universe_scoring_hash": prov.get("universe_scoring_hash"),
        "universe_input_contract_hash": prov.get("universe_input_contract_hash"),
        "ranking_basis_date": sc.get("ranking_date"),
        "hoc_assessment_hash": (ev.get("hoc_assessment_hash")
                                or prov.get("hoc_assessment_hash")),
        # R54.3 — the EXACT immutable opportunity-cost version this decision
        # stands on. It is part of IDENTITY (not merely evidence) because a
        # governed decision built on a different HOC version is a different
        # decision, however identical everything downstream of it looks.
        "hoc_artifact_id": hoc_artifact_id,
        "hoc_assessment_evidence_hash": hoc_evidence_hash,
        "reassessment_id": rs.get("reassessment_id") or prov.get("reassessment_id"),
        "reassessment_hash": (rs.get("reassessment_hash")
                              or prov.get("reassessment_hash")),
        "proposal_id": summ.get("reallocation_proposal_id"),
        "proposal_hash": summ.get("reallocation_proposal_hash"),
        "target_outcome": outcome,
    }
    ident_hash = candidate_identity_hash(identity)

    if outcome == _OUTCOME_PROPOSAL_READY:
        decision = GD_CHANGE_RECOMMENDED
    elif outcome == _OUTCOME_HOLD:
        decision = GD_HOLD_CURRENT_BOOK
    else:
        decision = None

    # Position-level recommendations are the proposal owner's OWN allocation
    # actions, verbatim. A governed HOLD deliberately carries none: the target
    # that was priced is precisely the one the system decided NOT to take, so
    # publishing its legs as recommendations would invert the decision.
    recommendations: list[dict] = []
    if decision == GD_CHANGE_RECOMMENDED:
        for a in ((con.get("best_feasible_target") or {}).get("allocations") or []):
            act = a.get("action")
            if act in ("HOLD", None):
                continue
            recommendations.append({
                "ticker": a.get("ticker"),
                "recommendation": act,
                "current_weight": a.get("current_weight"),
                "proposed_weight": a.get("proposed_weight"),
                "delta_weight": a.get("delta_weight"),
                "capital_change": a.get("capital_change"),
                "owner": "api.reallocation_proposal",
            })

    return {
        "owner": GOVERNANCE_GATE_OWNER,
        "gate_version": GOVERNANCE_GATE_VERSION,
        "candidate_identity_hash": ident_hash,
        "candidate_id": "gcand_%s_%s_%s" % (eligible or "nodate",
                                            active_book_id or "book",
                                            ident_hash[:12]),
        "identity": identity,
        "decision": decision,
        "decision_vocabulary": list(GOVERNED_DECISION_VOCAB),
        "position_recommendations": recommendations,
        "position_recommendation_vocabulary": list(POSITION_RECOMMENDATION_VOCAB),
        "position_recommendation_note": (
            "A governed HOLD carries no position recommendations: the priced "
            "target is the alternative the system decided NOT to take."
            if decision == GD_HOLD_CURRENT_BOOK else
            "Read verbatim from the proposal owner's own allocation actions."),
        "switching_economics": dict(econ),
        "evidence": {
            "event_cycle_run_id": ev.get("run_id"),
            "event_cycle_state": ev.get("state"),
            "event_cycle_started_at": ev.get("generated_at"),
            "event_cycle_completed_at": ev.get("completed_at"),
            "materiality_change_level": ev.get("materiality_change_level"),
            "materiality_trigger_fingerprint": ev.get(
                "materiality_trigger_fingerprint"),
            "reassessment_ran": ev.get("reassessment_ran"),
            "proposal_built": ev.get("proposal_built"),
            "hoc_holdings_reviewed": ev.get("hoc_holdings_reviewed"),
            # R54.3 — the retrievability facts, resolved by the artifact's own
            # owner. The gate reads them; it opens no store of its own.
            "hoc_persisted": hoc_persisted,
            "hoc_persistence_status": (hb.get("hoc_persistence_status")
                                       or ev.get("hoc_persistence_status")
                                       or (event_cycle or {}).get(
                                           "hoc_persistence_status")),
            "hoc_artifact_retrievable": hb.get("hoc_artifact_retrievable"),
            "hoc_artifact_identity_matches": hb.get("hoc_artifact_identity_matches"),
            "hoc_binding_detail": hb.get("hoc_binding_detail"),
            "hoc_binding_owner": (hb.get("hoc_binding_resolved_by")
                                  or hb.get("hoc_owner")),
            "reassessment_bound_hoc_artifact_id": prov.get("hoc_artifact_id"),
            "reassessment_bound_hoc_persisted": prov.get("hoc_persisted"),
            "cycle_holdings": ev.get("holdings"),
            "cycle_portfolio_state_hash": ev.get("portfolio_state_hash"),
            "cycle_blocker_codes": ev.get("blocker_codes") or [],
            "proposal_data_gaps": list(summ.get("reallocation_data_gaps") or []),
            "reassessment_state": rs.get("state"),
            "artifact_class": rcs.get("opportunity_cost_artifact_class"),
            "producer_owner": rcs.get("opportunity_cost_producer_owner"),
            "governed_daily_cycle_evidence_current": bool(
                rcs.get("governed_research_evidence_current")),
            # The OPERATIONAL clock, recorded — never advanced, never fabricated.
            # Read from api.workflow_state's operational block when the caller
            # supplies it, else from api.portfolio_state's own dates block (the
            # same two owners, one of which is always present).
            "operational_mark_date": (op.get("desk_mark_date")
                                      or op.get("valuation_date")
                                      or (ps.get("dates") or {}).get("desk_mark_date")
                                      or (ps.get("dates") or {}).get("valuation_date")),
            "latest_completed_close_date": (
                op.get("latest_completed_close_date")
                or (ps.get("dates") or {}).get("latest_daily_close_date")),
            "operational_close_valid": op.get("operational_close_valid"),
            "operational_mark_source": ("api.workflow_state.operational_state"
                                        if op else "api.portfolio_state.dates"),
            "operational_eligible_session": op.get("eligible_market_date")
            or eligible,
            "eligible_session_already_processed": op.get(
                "eligible_session_already_processed"),
            "expected_session_owned_data_confirmed": (
                wf.get("overall_state") != "WAITING_FOR_OWNED_DATA"
                if wf.get("overall_state") is not None else None),
            "expected_session_note": (
                "The workflow's WAITING_FOR_OWNED_DATA state concerns the NEXT "
                "expected completed session and the OPERATIONAL CLOSE clock. It "
                "is recorded here, never consumed as intraday decision evidence "
                "and never cleared by this module."),
        },
        "zero_base": {
            "incumbency_policy": ZERO_BASE_INCUMBENCY_POLICY,
            "current_holdings_privileged": bool(
                (con.get("multi_asset") or {}).get("current_holdings_privileged")),
            "ideal_target_owner": ((con.get("ideal_target") or {})
                                   .get("zero_base_owner")),
            "target_engine_owner": con.get("calculation_owner"),
            "note": ("The target owner answers the zero-base question; a held "
                     "name's ONLY advantage is the priced transition cost."),
        },
        # Phase-G inputs. The stage clock belongs to the cycle owner; the two
        # governance stamps are added by the gate and the writer, and the ONE
        # latency measurement is composed by api.event_signal_refresh.
        "latency_inputs": {
            "stage_timestamps": dict(ev.get("stage_timestamps") or {}),
            "event_cycle_started_at": ev.get("generated_at"),
            "observation_received_at": observation_received_at,
            "cycle_duration_seconds": ev.get("cycle_duration_seconds"),
            "oldest_event_to_reassessment_seconds": ev.get(
                "oldest_event_to_reassessment_seconds"),
            "measurement_owner": "api.event_signal_refresh",
        },
        "decided_at": _now_iso(now),
        "provenance": PROV_GOVERNED_INTRADAY,
        "manual_review_required": bool(decision == GD_CHANGE_RECOMMENDED),
        "safety": _governed_safety(),
    }


# --------------------------------------------------------------------------- #
# THE GATE. Admissibility only — it decides no economics of its own.
# --------------------------------------------------------------------------- #
def evaluate_intraday_governance(*, candidate: Optional[dict],
                                 portfolio_state: Optional[dict] = None,
                                 event_cycle: Optional[dict] = None,
                                 reassessment: Optional[dict] = None,
                                 proposal_summary: Optional[dict] = None,
                                 constrained: Optional[dict] = None,
                                 workflow: Optional[dict] = None,
                                 scoring_identity: Optional[dict] = None,
                                 rebalance: Optional[dict] = None,
                                 current_governed: Optional[dict] = None) -> dict:
    """Run every mandatory condition over ONE candidate. Pure; no io.

    Returns ``GOVERNED_INTRADAY_DECISION_ELIGIBLE`` only when EVERY check passes.
    Otherwise ``INTRADAY_DECISION_WITHHELD`` with explicit, classified reasons —
    never a generic BLOCKED.
    """
    cand = candidate or {}
    ident = cand.get("identity") or {}
    ev = cand.get("evidence") or {}
    ps = portfolio_state or {}
    rs = reassessment or {}
    summ = proposal_summary or {}
    con = constrained or {}
    sc = scoring_identity or {}
    wf = workflow or {}
    op = wf.get("operational_state") or {}
    econ = cand.get("switching_economics") or {}
    checks: list[dict] = []

    # --- A. PORTFOLIO IDENTITY --------------------------------------------- #
    book_id = ident.get("active_book_id")
    checks.append(_check(
        "PORTFOLIO_IDENTITY", "ACTIVE_BOOK_PRESENT", bool(book_id),
        "api.portfolio_state",
        "active book = %s" % (book_id or "NONE"), WR_NO_ACTIVE_BOOK))

    cycle_book = (event_cycle or {}).get("active_book_id")
    same_book = _eq_when_known(cycle_book, book_id)
    checks.append(_check(
        "PORTFOLIO_IDENTITY", "ACTIVE_BOOK_UNCHANGED", same_book is not False,
        "api.event_signal_refresh",
        "cycle book %s vs current %s" % (cycle_book, book_id),
        WR_PORTFOLIO_IDENTITY_STALE))

    live_ps_hash = ps.get("state_hash")
    live_econ_hash = ps.get("economic_state_hash")
    bound_ps_hash = ident.get("portfolio_state_hash")
    checks.append(_check(
        "PORTFOLIO_IDENTITY", "PORTFOLIO_STATE_HASH_BOUND",
        bound_ps_hash is not None, "api.portfolio_state",
        "evidence bound %s (current document hash %s)"
        % (bound_ps_hash, live_ps_hash), WR_PORTFOLIO_IDENTITY_STALE))

    # "Is the evidence still describing the CURRENT portfolio?" is answered by
    # the reassessment owner's Stage-21 economic-currency contract, NOT by
    # comparing raw ``state_hash`` values. ``state_hash`` covers the whole
    # portfolio-state DOCUMENT, which embeds the assessment's own output — so
    # comparing it would mark every fresh assessment stale the moment research
    # ran (exactly the fabrication Stage 21 exists to prevent). The economic
    # fingerprint covers holdings / cash / NAV / orders / fills / corporate
    # actions and structurally excludes research outputs.
    try:
        from paper_trader.api import portfolio_reassessment as _prs_ec
        currency = _prs_ec.economic_currency(artifact=rs.get("artifact"),
                                             portfolio_state=ps)
    except Exception as exc:  # noqa: BLE001 - a gate read must never crash
        currency = {"state": "UNVERIFIABLE", "reason": str(exc)[:120]}
    cur_state = currency.get("state")
    checks.append(_check(
        "PORTFOLIO_IDENTITY", "ECONOMIC_PORTFOLIO_STILL_CURRENT",
        cur_state == "CURRENT", "api.portfolio_reassessment.economic_currency",
        "%s (%s)" % (cur_state, currency.get("reason") or "economic fingerprint "
                     "unchanged since the assessment"),
        # SUPERSEDED is staleness; UNVERIFIABLE is NOT — the owner refuses to
        # infer it, and so does this gate. A promotion still fails closed,
        # because it cannot be PROVEN the evidence describes the book.
        WR_PORTFOLIO_IDENTITY_STALE if cur_state == "SUPERSEDED"
        else WR_EVIDENCE_INCOMPLETE))

    # The event cycle binds economic_state_hash when the state owner publishes
    # one, else the plain state hash — so a match against EITHER is honest.
    bound_econ_hash = ident.get("economic_state_hash")
    econ_ok = (bound_econ_hash is not None
               and str(bound_econ_hash) in {str(live_ps_hash), str(live_econ_hash)})
    checks.append(_check(
        "PORTFOLIO_IDENTITY", "ECONOMIC_STATE_HASH_BOUND", econ_ok,
        "api.portfolio_state",
        "cycle bound %s vs current economic %s / state %s"
        % (bound_econ_hash, live_econ_hash, live_ps_hash),
        WR_PORTFOLIO_IDENTITY_STALE))

    held_now = sorted({str(p.get("ticker")).upper()
                       for p in (ps.get("positions") or []) if p.get("ticker")})
    cycle_held = sorted(str(t).upper() for t in (ev.get("cycle_holdings") or []))
    holdings_ok = (not cycle_held) or cycle_held == held_now
    checks.append(_check(
        "PORTFOLIO_IDENTITY", "HOLDINGS_RECONCILE", holdings_ok,
        "api.portfolio_state",
        "%d held now, %d in the cycle" % (len(held_now), len(cycle_held)),
        WR_PORTFOLIO_IDENTITY_STALE))

    cap = ps.get("capital") or {}
    cash_nav_ok = (cap.get("nav") is not None and cap.get("cash") is not None)
    checks.append(_check(
        "PORTFOLIO_IDENTITY", "CASH_AND_NAV_RECONCILE", cash_nav_ok,
        "api.operational_book -> desk.book_nav",
        "nav=%s cash=%s" % (cap.get("nav"), cap.get("cash")),
        WR_PORTFOLIO_IDENTITY_STALE))

    ca_ok = not bool(summ.get("reallocation_proposal_stale"))
    checks.append(_check(
        "PORTFOLIO_IDENTITY", "CORPORATE_ACTION_REGISTRY_CURRENT", ca_ok,
        "api.corporate_actions via api.reallocation_proposal",
        summ.get("reallocation_proposal_stale_reason") or "registry fingerprint current",
        WR_PORTFOLIO_IDENTITY_STALE))

    # --- B. MARKET / DATA FRESHNESS ----------------------------------------- #
    # The BOOK's own session must be owned-confirmed and validly closed. This is
    # the OWNED_DATA_NOT_CONFIRMED rule, applied to the session the candidate is
    # actually built on. It is NOT the workflow's forward-looking wait for the
    # NEXT expected session — that concerns the operational close clock, is
    # recorded in the candidate's evidence, and is never cleared here.
    mark = ev.get("operational_mark_date")
    closed = ev.get("latest_completed_close_date")
    elig = ident.get("eligible_market_session")
    owned_ok = bool(
        elig and mark and closed
        and str(mark)[:10] >= str(elig)[:10]
        and str(closed)[:10] >= str(elig)[:10]
        and ev.get("operational_close_valid") is not False)
    checks.append(_check(
        "MARKET_DATA_FRESHNESS", "BOOK_SESSION_OWNED_CONFIRMED", owned_ok,
        "api.daily_close (close validity) + engine.market_session (eligibility)",
        "operational mark %s, latest completed close %s, eligible session %s, "
        "close_valid=%s (source %s)"
        % (mark, closed, elig, ev.get("operational_close_valid"),
           ev.get("operational_mark_source")),
        WR_OWNED_DATA_NOT_CONFIRMED))

    gaps = list(ev.get("proposal_data_gaps") or [])
    reassess_state = str(rs.get("state") or ev.get("reassessment_state") or "")
    data_ok = (not gaps) and reassess_state != _REASSESS_BLOCKED_DATA
    checks.append(_check(
        "MARKET_DATA_FRESHNESS", "NO_TRUE_DATA_GAP", data_ok,
        "api.reallocation_proposal + api.portfolio_reassessment",
        "data gaps=%s reassessment=%s" % (gaps or "none", reassess_state or "?"),
        WR_MARKET_DATA_STALE))

    evidence_ok = reassess_state != _REASSESS_BLOCKED_EVIDENCE
    checks.append(_check(
        "MARKET_DATA_FRESHNESS", "EVIDENCE_NOT_BLOCKED", evidence_ok,
        "api.portfolio_reassessment",
        "reassessment state %s" % (reassess_state or "?"), WR_EVIDENCE_INCOMPLETE)
    )

    # Point-in-time: nothing may be dated AFTER the session it claims to describe,
    # and every artifact must describe the SAME session.
    ranking_date = ident.get("ranking_basis_date") or sc.get("ranking_date")
    pit_future = bool(ranking_date and elig and str(ranking_date)[:10] > str(elig)[:10])
    sessions = {str(x)[:10] for x in (
        elig, summ.get("reallocation_bound_eligible_market_date"),
        rs.get("eligible_market_date"),
        (rs.get("proposal_binding") or {}).get("eligible_market_date"),
        (event_cycle or {}).get("eligible_market_date"))
        if x}
    pit_ok = (not pit_future) and len(sessions) <= 1
    checks.append(_check(
        "MARKET_DATA_FRESHNESS", "POINT_IN_TIME_INTEGRITY", pit_ok,
        "api.universe_scoring + api.portfolio_reassessment + api.reallocation_proposal",
        "ranking basis %s; sessions bound=%s" % (ranking_date, sorted(sessions)),
        WR_POINT_IN_TIME))

    # --- C. SIGNAL / RANKING IDENTITY --------------------------------------- #
    ranking_bound = bool(ident.get("universe_input_contract_hash")
                         or ident.get("universe_scoring_hash"))
    checks.append(_check(
        "SIGNAL_RANKING_IDENTITY", "RANKING_IDENTITY_BOUND", ranking_bound,
        "api.universe_scoring",
        "input_contract=%s scoring=%s" % (ident.get("universe_input_contract_hash"),
                                          ident.get("universe_scoring_hash")),
        WR_RANKING_IDENTITY))
    checks.append(_check(
        "SIGNAL_RANKING_IDENTITY", "RANKING_BASIS_DATE_EXPLICIT",
        bool(ranking_date), "api.universe_scoring",
        "ranking basis date %s (owned model-input as-of date, never wall clock)"
        % ranking_date, WR_RANKING_IDENTITY))
    live_ic = _eq_when_known(sc.get("input_contract_hash"),
                             ident.get("universe_input_contract_hash"))
    checks.append(_check(
        "SIGNAL_RANKING_IDENTITY", "RANKING_IDENTITY_UNCHANGED",
        live_ic is not False, "api.universe_scoring",
        "live input contract %s vs bound %s"
        % (sc.get("input_contract_hash"), ident.get("universe_input_contract_hash")),
        WR_RANKING_IDENTITY))

    # --- D. HOLDING OPPORTUNITY COST IDENTITY ------------------------------- #
    hoc_hash = ident.get("hoc_assessment_hash")
    checks.append(_check(
        "HOC_IDENTITY", "HOC_ASSESSMENT_HASH_BOUND", bool(hoc_hash),
        "api.holding_opportunity_cost", "assessment hash %s" % hoc_hash,
        WR_HOC_IDENTITY))
    hoc_vs_target = _eq_when_known(
        summ.get("reallocation_bound_hoc_assessment_hash"), hoc_hash)
    checks.append(_check(
        "HOC_IDENTITY", "TARGET_BOUND_TO_SAME_HOC", hoc_vs_target is not False,
        "api.reallocation_proposal",
        "target-bound HOC %s vs candidate %s"
        % (summ.get("reallocation_bound_hoc_assessment_hash"), hoc_hash),
        WR_HOC_IDENTITY))
    reviewed = ev.get("hoc_holdings_reviewed")
    all_reviewed = (reviewed is None or not held_now
                    or int(reviewed) >= len(held_now))
    checks.append(_check(
        "HOC_IDENTITY", "EVERY_HOLDING_ASSESSED", all_reviewed,
        "api.holding_opportunity_cost",
        "%s of %d holdings reviewed" % (reviewed, len(held_now)), WR_HOC_IDENTITY))

    # Release 54.3 — THE OPPORTUNITY-COST DEPENDENCY MUST BE PRODUCIBLE AS EVIDENCE.
    #
    # Everything above compares HASHES. A hash proves two payloads agree; it proves
    # nothing about whether either still EXISTS. Before R54.3 that was the whole
    # gap: the opportunity-cost owner refused a second same-session write, so every
    # intraday cycle after the first computed a perfectly real assessment that lived
    # only in memory, the reassessment persisted its transient hash as a dependency,
    # and these checks passed on a chain whose first link could never be retrieved.
    # A governed decision that cannot produce its own evidence is not governed.
    #
    # Absence is inadmissible here, deliberately. "Not comparable" is admissible for
    # a binding that MIGHT legitimately be unknown; it is not admissible for the
    # question "does this artifact exist?", where the only honest answers are a
    # proof and a refusal.
    hoc_artifact_id = ident.get("hoc_artifact_id")
    checks.append(_check(
        "HOC_IDENTITY", "HOC_ARTIFACT_ID_BOUND", bool(hoc_artifact_id),
        "api.holding_opportunity_cost",
        "artifact id %s (persistence=%s)" % (hoc_artifact_id or "NONE",
                                             ev.get("hoc_persistence_status")),
        WR_HOC_NOT_PERSISTED))
    checks.append(_check(
        "HOC_IDENTITY", "HOC_ASSESSMENT_WAS_PERSISTED",
        ev.get("hoc_persisted") is True, "api.holding_opportunity_cost",
        "persistence status %s; persisted=%s"
        % (ev.get("hoc_persistence_status") or "UNRECORDED", ev.get("hoc_persisted")),
        WR_HOC_NOT_PERSISTED))
    checks.append(_check(
        "HOC_IDENTITY", "HOC_ARTIFACT_RETRIEVABLE",
        ev.get("hoc_artifact_retrievable") is True,
        "api.holding_opportunity_cost.resolve_binding",
        ev.get("hoc_binding_detail") or "no retrievability proof was supplied",
        WR_HOC_NOT_PERSISTED))
    checks.append(_check(
        "HOC_IDENTITY", "HOC_ARTIFACT_IDENTITY_MATCHES",
        ev.get("hoc_artifact_identity_matches") is True,
        "api.holding_opportunity_cost.resolve_binding",
        "stored artifact must carry the claimed assessment hash, book and session "
        "(%s)" % (ev.get("hoc_binding_detail") or "unproven"),
        WR_HOC_ARTIFACT_MISMATCH))
    # The reassessment is the link that CLAIMS the dependency, so its recorded
    # binding must be the same artifact this candidate stands on.
    reas_hoc = ev.get("reassessment_bound_hoc_artifact_id")
    reas_hoc_ok = _eq_when_known(reas_hoc, hoc_artifact_id)
    checks.append(_check(
        "HOC_IDENTITY", "REASSESSMENT_BOUND_TO_THE_SAME_HOC_ARTIFACT",
        reas_hoc_ok is not False, "api.portfolio_reassessment",
        "reassessment bound %s vs candidate %s" % (reas_hoc, hoc_artifact_id),
        WR_HOC_ARTIFACT_MISMATCH))
    checks.append(_check(
        "HOC_IDENTITY", "REASSESSMENT_DEPENDENCY_IS_NOT_TRANSIENT",
        ev.get("reassessment_bound_hoc_persisted") is not False,
        "api.portfolio_reassessment",
        "the reassessment recorded hoc_persisted=%s"
        % ev.get("reassessment_bound_hoc_persisted"), WR_HOC_NOT_PERSISTED))
    hoc_ev_hash = ident.get("hoc_assessment_evidence_hash")
    checks.append(_check(
        "HOC_IDENTITY", "HOC_EVIDENCE_IDENTITY_BOUND", bool(hoc_ev_hash),
        "api.holding_opportunity_cost",
        "assessment evidence hash %s" % (hoc_ev_hash or "NONE"),
        WR_HOC_IDENTITY))

    # --- E. PORTFOLIO REASSESSMENT IDENTITY --------------------------------- #
    ra_hash = ident.get("reassessment_hash")
    checks.append(_check(
        "REASSESSMENT_IDENTITY", "REASSESSMENT_HASH_BOUND", bool(ra_hash),
        "api.portfolio_reassessment", "reassessment hash %s" % ra_hash,
        WR_REASSESSMENT_IDENTITY))
    cycle_ra = _eq_when_known((event_cycle or {}).get("reassessment_hash"), ra_hash)
    # R54.2 — the cycle's conclusion must ALSO have become an immutable artifact.
    # A refused write (CONFLICT_REJECTED / REJECTED_INCONSISTENT_IDENTITY) leaves a
    # live conclusion with no evidence standing behind it, and an unpersisted
    # assessment is never governable however current it looks. This TIGHTENS the
    # rule inside the same check; it does not relax the hash comparison.
    ran = (event_cycle or {}).get("reassessment_ran")
    persisted = (event_cycle or {}).get("reassessment_persisted")
    persisted_ok = not (bool(ran) and persisted is False)
    checks.append(_check(
        "REASSESSMENT_IDENTITY", "CYCLE_REASSESSMENT_IS_THE_CANDIDATE",
        cycle_ra is not False and persisted_ok, "api.event_signal_refresh",
        "cycle %s vs candidate %s (persistence=%s, artifact=%s)"
        % ((event_cycle or {}).get("reassessment_hash"), ra_hash,
           (event_cycle or {}).get("reassessment_persistence_status"),
           (event_cycle or {}).get("reassessment_id")),
        WR_REASSESSMENT_IDENTITY))
    checks.append(_check(
        "REASSESSMENT_IDENTITY", "MATERIALITY_TRIGGER_BOUND",
        bool(ev.get("materiality_trigger_fingerprint")),
        "engine.event_materiality",
        "trigger fingerprint %s" % ev.get("materiality_trigger_fingerprint"),
        WR_EVIDENCE_INCOMPLETE))

    # --- F. TARGET / PROPOSAL IDENTITY -------------------------------------- #
    outcome = ident.get("target_outcome")
    if outcome == _OUTCOME_TRUE_BLOCKER:
        conclusive, conclusive_reason = False, WR_TRUE_BLOCKER
    elif summ.get("reallocation_proposal_withheld"):
        conclusive, conclusive_reason = False, WR_CHANGE_WITHHELD
    elif outcome in (_OUTCOME_PROPOSAL_READY, _OUTCOME_HOLD):
        conclusive, conclusive_reason = True, None
    else:
        conclusive, conclusive_reason = False, WR_EVIDENCE_INCOMPLETE
    checks.append(_check(
        "TARGET_IDENTITY", "CONCLUSIVE_PRICED_OUTCOME", conclusive,
        "api.reallocation_proposal (engine.constrained_reallocation)",
        "outcome %s; withheld=%s" % (outcome or "NONE",
                                     bool(summ.get("reallocation_proposal_withheld"))),
        conclusive_reason))

    checks.append(_check(
        "TARGET_IDENTITY", "TARGET_HASH_BOUND", bool(ident.get("proposal_hash")),
        "api.reallocation_proposal", "proposal hash %s" % ident.get("proposal_hash"),
        WR_TARGET_IDENTITY))
    target_book = _eq_when_known(summ.get("reallocation_bound_active_book_id"), book_id)
    checks.append(_check(
        "TARGET_IDENTITY", "TARGET_BOUND_TO_ACTIVE_BOOK", target_book is not False,
        "api.reallocation_proposal",
        "target book %s" % summ.get("reallocation_bound_active_book_id"),
        WR_TARGET_IDENTITY))
    checks.append(_check(
        "TARGET_IDENTITY", "FEASIBLE_TARGET_WAS_COMPUTED",
        bool(summ.get("reallocation_feasible_target_exists")
             or con.get("feasible_target_exists")),
        "engine.constrained_reallocation",
        "feasible target exists = %s" % summ.get("reallocation_feasible_target_exists"),
        WR_TARGET_IDENTITY))

    # --- G. CHURN / ECONOMIC CONTROLS (bound, never re-decided) ------------- #
    required_econ = ("switching_hurdle", "clears_switching_hurdle",
                     "one_way_turnover", "estimated_transaction_cost",
                     "concentration_before", "concentration_after",
                     "score_improvement_net_of_cost")
    missing_econ = [k for k in required_econ if econ.get(k) is None]
    checks.append(_check(
        "ECONOMIC_CONTROLS", "SWITCHING_ECONOMICS_COMPLETE", not missing_econ,
        "engine.constrained_reallocation.switching_economics",
        "missing: %s" % (missing_econ or "none"), WR_SWITCHING_ECONOMICS))
    checks.append(_check(
        "ECONOMIC_CONTROLS", "RISK_BEFORE_AND_AFTER_PRICED",
        ("portfolio_volatility_before" in econ and "portfolio_volatility_after" in econ),
        "engine.constrained_reallocation",
        "volatility before/after published by the target owner",
        WR_SWITCHING_ECONOMICS))
    checks.append(_check(
        "ECONOMIC_CONTROLS", "TURNOVER_BUDGET_EVALUATED",
        bool((con.get("constraint_inventory") or {}).get("constraints")
             or econ.get("one_way_turnover") is not None),
        "engine.constrained_reallocation",
        "one-way turnover %s" % econ.get("one_way_turnover"), WR_SWITCHING_ECONOMICS))
    checks.append(_check(
        "ECONOMIC_CONTROLS", "ZERO_BASE_INCUMBENCY_POLICY_INTACT",
        ((cand.get("zero_base") or {}).get("incumbency_policy")
         == ZERO_BASE_INCUMBENCY_POLICY
         and not (cand.get("zero_base") or {}).get("current_holdings_privileged")),
        "engine.constrained_reallocation",
        "incumbency policy %s" % (cand.get("zero_base") or {}).get("incumbency_policy"),
        WR_SWITCHING_ECONOMICS))
    not_duplicate_trigger = (ev.get("event_cycle_state")
                             != "DUPLICATE_TRIGGER_SUPPRESSED")
    checks.append(_check(
        "ECONOMIC_CONTROLS", "ANTI_CHURN_TRIGGER_NOT_SUPPRESSED",
        not_duplicate_trigger, "engine.event_materiality",
        "cycle state %s" % ev.get("event_cycle_state"), WR_DUPLICATE))
    checks.append(_check(
        "ECONOMIC_CONTROLS", "CYCLE_REACHED_A_PORTFOLIO_ANSWER",
        bool(ev.get("event_cycle_state") in _PROMOTABLE_CYCLE_STATES
             and ev.get("reassessment_ran")),
        "api.event_signal_refresh",
        "cycle state %s; reassessment_ran=%s"
        % (ev.get("event_cycle_state"), ev.get("reassessment_ran")),
        WR_EVIDENCE_INCOMPLETE))
    checks.append(_check(
        "ECONOMIC_CONTROLS", "CYCLE_NOT_BLOCKED",
        not (ev.get("cycle_blocker_codes") or []), "api.event_signal_refresh",
        "cycle blockers %s" % (ev.get("cycle_blocker_codes") or "none"),
        WR_TRUE_BLOCKER))

    # --- H. CONCURRENCY / SUPERSESSION -------------------------------------- #
    cur = current_governed or {}
    # Duplicate by full evidence identity OR by the CORE evidence a projected
    # daily-cycle decision can also carry. Without the second test an intraday
    # candidate built from exactly the DRC's own reassessment + target would be
    # promoted as a "new" governed decision that says what the DRC already
    # said — a redundant record, which is the churn idempotency exists to stop.
    dup = bool(cur.get("candidate_identity_hash")
               and cur.get("candidate_identity_hash")
               == cand.get("candidate_identity_hash"))
    if not dup:
        core = _core_evidence(ident)
        dup = bool(all(v is not None for v in core)
                   and core == _core_evidence(cur.get("identity") or {}))
    checks.append(_check(
        "CONCURRENCY", "CANDIDATE_ADDS_NEW_EVIDENCE", not dup,
        GOVERNANCE_GATE_OWNER,
        ("identical evidence identity to the standing governed decision %s"
         % cur.get("record_id")) if dup else "evidence identity is new",
        WR_DUPLICATE))
    newer_exists = bool(
        cur and not dup
        and governed_decision_ordering_key(cur)
        >= governed_decision_ordering_key(cand))
    checks.append(_check(
        "CONCURRENCY", "NOT_SUPERSEDED_BY_A_NEWER_DECISION", not newer_exists,
        GOVERNANCE_GATE_OWNER,
        "standing governed decision %s at %s"
        % (cur.get("record_id"), cur.get("decided_at")), WR_SUPERSEDED))
    # Execution precedence is decided by api.rebalance_execution and already
    # published by the reassessment read; recompute it through its owner only
    # when the caller supplied a rebalance state the read did not see.
    prec = dict(rs.get("execution_precedence") or {})
    if rebalance is not None or not prec:
        try:
            from paper_trader.api import portfolio_reassessment as _prs
            prec = _prs.execution_precedence(
                rebalance_state=(rebalance or {}).get("rebalance_state"),
                pending_orders=op.get("pending_orders"))
        except Exception:  # noqa: BLE001 - a gate read must never crash
            prec = {"execution_active": bool((op.get("pending_orders") or 0) > 0)}
    checks.append(_check(
        "CONCURRENCY", "NO_EXECUTION_HOLDS_PRECEDENCE",
        not prec.get("execution_active"), "api.rebalance_execution",
        prec.get("reason") or "no controlled paper rebalance in flight",
        WR_EXECUTION_PRECEDENCE))
    checks.append(_check(
        "CONCURRENCY", "CANDIDATE_IDENTITY_IS_DETERMINISTIC",
        bool(cand.get("candidate_identity_hash")) and bool(cand.get("decided_at")),
        GOVERNANCE_GATE_OWNER,
        "identity %s at %s" % (cand.get("candidate_identity_hash"),
                               cand.get("decided_at")), WR_EVIDENCE_INCOMPLETE))

    # --- I. SAFETY (structural; these are properties of the code) ----------- #
    safety = cand.get("safety") or _governed_safety()
    checks.append(_check(
        "SAFETY", "MANUAL_REVIEW_REQUIRED_FOR_CHANGE",
        (cand.get("decision") != GD_CHANGE_RECOMMENDED
         or bool(cand.get("manual_review_required"))),
        GOVERNANCE_GATE_OWNER, "a governed CHANGE is a recommendation only",
        WR_EVIDENCE_INCOMPLETE))
    checks.append(_check(
        "SAFETY", "NO_AUTOMATION_NO_APPROVAL_NO_PROMOTION",
        not (safety.get("automation_enabled") or safety.get("broker_enabled")
             or safety.get("approved_anything")
             or safety.get("automatic_approval_allowed")
             or safety.get("promoted_model")
             or safety.get("activated_sleeve")),
        GOVERNANCE_GATE_OWNER, "structural safety intact", WR_EVIDENCE_INCOMPLETE))

    failed = [c for c in checks if not c["passed"]]
    reasons: list[dict] = []
    seen: set = set()
    for c in failed:
        code = c.get("reason_code") or WR_EVIDENCE_INCOMPLETE
        if code in seen:
            continue
        seen.add(code)
        reasons.append({"code": code, "check": c["check"], "group": c["group"],
                        "owner": c["owner"], "detail": c["detail"]})

    eligible_verdict = not failed
    return {
        "owner": GOVERNANCE_GATE_OWNER,
        "gate_version": GOVERNANCE_GATE_VERSION,
        "verdict": GATE_ELIGIBLE if eligible_verdict else GATE_WITHHELD,
        "verdict_vocabulary": list(GATE_VERDICT_VOCAB),
        "eligible": eligible_verdict,
        "candidate_id": cand.get("candidate_id"),
        "candidate_identity_hash": cand.get("candidate_identity_hash"),
        "candidate_decision": cand.get("decision"),
        "duplicate_of_standing_decision": dup,
        "withheld_reasons": reasons,
        "withheld_reason_codes": [r["code"] for r in reasons],
        "withheld_reason_vocabulary": list(WITHHELD_REASON_VOCAB),
        "failing_checks": [c["check"] for c in failed],
        "checks": checks,
        "checks_passed": len(checks) - len(failed),
        "checks_total": len(checks),
        "evaluated_at": _now_iso(None),
        "economics_owner": "engine.constrained_reallocation",
        "gate_decides_economics": False,
        "safety": _governed_safety(),
    }


# --------------------------------------------------------------------------- #
# Supersession — ONE deterministic ordering, used by the gate AND the read
# --------------------------------------------------------------------------- #
def governed_decision_ordering_key(record: Optional[dict]) -> tuple:
    """The total order over governed portfolio decisions.

    ``(eligible session, decision timestamp, provenance rank, identity hash)``.
    A later session always outranks an earlier one; within a session the later
    decision timestamp wins; a tie on BOTH is broken by provenance (the
    session-terminal governed cycle outranks an intraday promotion) and finally
    by identity hash, so the order is total and reproducible. A stale or older
    assessment can therefore never supersede a newer governed decision.
    """
    r = record or {}
    ident = r.get("identity") or {}
    session = str(r.get("eligible_market_session")
                  or ident.get("eligible_market_session") or "")[:10]
    stamp = _parse_iso(r.get("decided_at"))
    rank = _PROVENANCE_RANK.get(r.get("provenance"), 0)
    ident_hash = str(r.get("candidate_identity_hash") or "")
    return (session, stamp, rank, ident_hash)


def _parse_iso(value: Any) -> str:
    """A sortable normalisation of an owner-stamped ISO timestamp.

    String comparison is exact for the ISO-8601 UTC stamps every owner in this
    system writes; an absent stamp sorts before every real one instead of
    raising or being given a fabricated value.
    """
    if not value:
        return ""
    return str(value).replace("Z", "+00:00")


# --------------------------------------------------------------------------- #
# Governed-lane persistence (append-only, idempotent, never rewritten)
# --------------------------------------------------------------------------- #
def load_governed_decision_record(*, active_book_id: Optional[str] = None,
                                  decision_dir=None) -> Optional[dict]:
    """The latest PERSISTED governed decision for a book (or overall). Pure
    reader; never raises, never writes."""
    try:
        index = _load_json(_governed_index_path(decision_dir)) or {}
        rows = [v for k, v in index.items()
                if active_book_id is None or str(k) == str(active_book_id)]
        if not rows:
            return None
        best = max(rows, key=lambda r: governed_decision_ordering_key(
            r.get("record") or r))
        rec = best.get("record")
        if rec:
            return rec
        records = _load_json(_governed_records_path(decision_dir)) or []
        for r in reversed(records):
            if r.get("record_id") == best.get("record_id"):
                return r
        return None
    except Exception:  # noqa: BLE001 - a pure read must never crash the caller
        return None


def record_governed_decision(*, candidate: dict, gate: dict,
                             provenance: str = PROV_GOVERNED_INTRADAY,
                             confirm: Optional[str] = None,
                             decision_dir=None, actor: Optional[str] = None,
                             now: Optional[datetime] = None) -> dict:
    """Append ONE governed portfolio decision. Writes nothing else, ever.

    Fail-closed and idempotent:
      * the gate must have returned ``GOVERNED_INTRADAY_DECISION_ELIGIBLE``;
      * ``confirm`` must equal :data:`GOVERNED_DECISION_CONFIRM_TOKEN` (a system
        token, deliberately NOT the operator approval token);
      * a candidate whose evidence identity already stands is REUSED, never
        duplicated;
      * a candidate that does not strictly outrank the standing decision is
        refused with ``SUPERSEDED_BY_NEWER_DECISION``;
      * the prior record is NEVER mutated — supersession is an append that names
        it in ``supersedes_decision_id``.

    It creates no order, no fill, no order plan, no approval and no model
    promotion, and it never advances the operational close mark.
    """
    base = {"owner": GOVERNANCE_GATE_OWNER, "recorded": False,
            "safety": _governed_safety()}
    if confirm != GOVERNED_DECISION_CONFIRM_TOKEN:
        return {**base, "status": "GOVERNED_DECISION_CONFIRMATION_REQUIRED",
                "confirm_required_token": GOVERNED_DECISION_CONFIRM_TOKEN,
                "message": ("Recording a governed decision requires the system "
                            "confirmation token.")}
    if provenance not in GOVERNED_PROVENANCE_VOCAB:
        return {**base, "status": "INVALID_PROVENANCE",
                "provenance_vocabulary": list(GOVERNED_PROVENANCE_VOCAB),
                "message": "provenance must be one of %s"
                           % (GOVERNED_PROVENANCE_VOCAB,)}
    if not (gate or {}).get("eligible"):
        return {**base, "status": GATE_WITHHELD,
                "withheld_reasons": list((gate or {}).get("withheld_reasons") or []),
                "withheld_reason_codes": list(
                    (gate or {}).get("withheld_reason_codes") or []),
                "message": ("The intraday governance gate withheld this "
                            "candidate; no governed decision was recorded.")}
    decision = (candidate or {}).get("decision")
    if decision not in GOVERNED_DECISION_VOCAB:
        return {**base, "status": "INVALID_DECISION",
                "decision_vocabulary": list(GOVERNED_DECISION_VOCAB),
                "message": "a governed decision must be one of %s"
                           % (GOVERNED_DECISION_VOCAB,)}

    ident = (candidate or {}).get("identity") or {}
    book = ident.get("active_book_id")
    session = ident.get("eligible_market_session")
    ident_hash = candidate.get("candidate_identity_hash")
    existing = load_governed_decision_record(active_book_id=book,
                                             decision_dir=decision_dir)

    if existing and existing.get("candidate_identity_hash") == ident_hash:
        return {**base, "status": "REUSED_EXISTING", "recorded": True,
                "idempotent": True, "idempotent_reason": WR_DUPLICATE,
                "record": existing,
                "message": ("The identical evidence identity is already the "
                            "standing governed decision.")}

    ts = _now_iso(now)
    proposed = dict(candidate)
    proposed["provenance"] = provenance
    proposed["decided_at"] = candidate.get("decided_at") or ts
    proposed["eligible_market_session"] = session
    # ONE latency measurement, composed by its owner now that BOTH governance
    # stamps exist. A missing stage stamp is named, never invented.
    li = candidate.get("latency_inputs") or {}
    try:
        from paper_trader.api import event_signal_refresh as _esr
        latency = _esr.measure_decision_latency(
            stage_timestamps=li.get("stage_timestamps"),
            event_cycle_started_at=li.get("event_cycle_started_at"),
            observation_received_at=li.get("observation_received_at"),
            governance_gate_completed_at=(gate or {}).get("evaluated_at"),
            governed_decision_persisted_at=ts)
    except Exception as exc:  # noqa: BLE001 - observability never blocks a decision
        latency = {"latency_measurement_complete": False,
                   "measurement_unavailable": str(exc)[:160]}
    if existing and governed_decision_ordering_key(existing) >= \
            governed_decision_ordering_key(proposed):
        return {**base, "status": WR_SUPERSEDED,
                "standing_decision_id": existing.get("record_id"),
                "standing_decided_at": existing.get("decided_at"),
                "message": ("A newer governed decision already stands; a stale "
                            "or older candidate can never supersede it.")}

    record = {
        "record_id": "gdec_%s_%s_%s" % (session or "nodate", book or "book",
                                        str(ident_hash or "")[:12]),
        "record_kind": "GOVERNED_PORTFOLIO_DECISION",
        "owner": GOVERNANCE_GATE_OWNER,
        "schema_version": GOVERNANCE_GATE_VERSION,
        "provenance": provenance,
        "provenance_vocabulary": list(GOVERNED_PROVENANCE_VOCAB),
        "decision": decision,
        "decision_vocabulary": list(GOVERNED_DECISION_VOCAB),
        "decided_at": proposed["decided_at"],
        "recorded_at": ts,
        "actor": actor or GOVERNANCE_GATE_OWNER,
        "active_book_id": book,
        "eligible_market_session": session,
        "candidate_id": candidate.get("candidate_id"),
        "candidate_identity_hash": ident_hash,
        "identity": dict(ident),
        "evidence_provenance": dict(candidate.get("evidence") or {}),
        "switching_economics": dict(candidate.get("switching_economics") or {}),
        "position_recommendations": list(
            candidate.get("position_recommendations") or []),
        "zero_base": dict(candidate.get("zero_base") or {}),
        "manual_review_required": bool(decision == GD_CHANGE_RECOMMENDED),
        "approval_required_token": CONFIRM_TOKEN,
        "approval_path": "POST /v1/operations/portfolio-decision/record",
        "supersedes_decision_id": (existing or {}).get("record_id"),
        "supersedes_decided_at": (existing or {}).get("decided_at"),
        "gate": {
            "verdict": gate.get("verdict"),
            "gate_version": gate.get("gate_version"),
            "checks_passed": gate.get("checks_passed"),
            "checks_total": gate.get("checks_total"),
            "evaluated_at": gate.get("evaluated_at"),
        },
        "latency": latency,
        "safety": _governed_safety(),
    }

    records = _load_json(_governed_records_path(decision_dir)) or []
    if not isinstance(records, list):
        records = []
    records.append(record)              # append-only; nothing above is rewritten
    _atomic_write_json(_governed_records_path(decision_dir), records)
    index = _load_json(_governed_index_path(decision_dir)) or {}
    index[str(book or "?")] = {"record_id": record["record_id"],
                               "decision": decision, "provenance": provenance,
                               "decided_at": record["decided_at"],
                               "record": record}
    _atomic_write_json(_governed_index_path(decision_dir), index)
    return {**base, "status": "CREATED", "recorded": True, "idempotent": False,
            "record": record, "superseded_record_id": (existing or {}).get("record_id")}


# --------------------------------------------------------------------------- #
# THE composed entry point — the ONE call the live event cycle delegates to
# --------------------------------------------------------------------------- #
def _snapshot_section(name: str) -> Optional[dict]:
    from paper_trader.api import decision_snapshot as snap
    return snap.section(name)


def govern_latest_intraday_assessment(
        *, confirm: Optional[str] = None,
        portfolio_state: Optional[dict] = None,
        event_cycle: Optional[dict] = None,
        reassessment: Optional[dict] = None,
        proposal_summary: Optional[dict] = None,
        constrained: Optional[dict] = None,
        workflow: Optional[dict] = None,
        scoring_identity: Optional[dict] = None,
        rebalance: Optional[dict] = None,
        hoc_binding: Optional[dict] = None,
        observation_received_at: Any = None,
        decision_dir=None, reallocation_dir=None, hoc_dir=None,
        loaders: Optional[dict] = None,
        now: Optional[datetime] = None) -> dict:
    """Build the candidate, run the gate, and persist ONLY if it passes.

    This is the ONE governed-promotion path. It is token-gated exactly like the
    event cycle it is called from, every owner read is an injectable seam (so a
    test never touches a production store), and a failure in any single owner
    degrades to a WITHHELD verdict rather than a crash or a fabricated decision.

    It performs no approval, creates no order/fill/order-plan, promotes no
    model, activates no sleeve, runs no close and advances no operational mark.
    """
    if confirm != GOVERNED_DECISION_CONFIRM_TOKEN:
        return {"owner": GOVERNANCE_GATE_OWNER, "recorded": False,
                "status": "GOVERNED_DECISION_CONFIRMATION_REQUIRED",
                "confirm_required_token": GOVERNED_DECISION_CONFIRM_TOKEN,
                "safety": _governed_safety()}

    lds = dict(loaders or {})
    warnings: list[str] = []

    def _get(name: str, supplied: Any, default_fn: Callable) -> Any:
        if supplied is not None:
            return supplied
        fn = lds.get(name, default_fn)
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - one owner failing never crashes
            warnings.append("%s unavailable: %s" % (name, str(exc)[:160]))
            return None

    ps = _get("portfolio_state", portfolio_state, _default_portfolio_state_loader)
    wf = _get("workflow", workflow, lambda: _snapshot_section("workflow"))

    def _load_event_cycle():
        from paper_trader.api import event_signal_refresh as esr
        return (esr.load_event_signal_refresh_status(portfolio_state=ps)
                or {}).get("last_run_summary")

    def _load_reassessment():
        from paper_trader.api import portfolio_reassessment as prs
        return prs.load_portfolio_reassessment(portfolio_state=ps)

    def _load_summary():
        ab = ((ps or {}).get("active_book") or {}).get("book_id")
        el = ((ps or {}).get("dates") or {}).get("eligible_market_date")
        return realloc.load_proposal_summary(
            active_book_id=ab, eligible_market_date=el,
            reallocation_dir=reallocation_dir)

    def _load_constrained():
        return realloc.load_constrained_reallocation(
            portfolio_state=ps, reallocation_dir=reallocation_dir,
            decision_dir=decision_dir)

    def _load_scoring_identity():
        from paper_trader.api import universe_scoring as us
        return us.canonical_identity(us.build_universe_scoring())

    ev = _get("event_cycle", event_cycle, _load_event_cycle)
    rs = _get("reassessment", reassessment, _load_reassessment)
    summ = _get("proposal_summary", proposal_summary, _load_summary)
    con = _get("constrained", constrained, _load_constrained)
    sc = _get("scoring_identity", scoring_identity, _load_scoring_identity)

    def _load_hoc_binding():
        """R54.3 — PROVE the opportunity-cost dependency exists, through its owner.

        The gate is pure, so the one read that can answer "is this artifact
        retrievable?" happens here and is handed to it as a fact. The lookup is by
        EXACT id — never "latest for the session" — so an older candidate can never
        be validated by a newer artifact that merely happens to share its session.
        """
        from paper_trader.api import holding_opportunity_cost as hocm
        claimed = {
            "hoc_artifact_id": ((ev or {}).get("hoc_artifact_id")
                                or ((rs or {}).get("proposal_binding") or {}).get(
                                    "hoc_artifact_id")),
            "hoc_assessment_hash": (ev or {}).get("hoc_assessment_hash"),
            "hoc_assessment_evidence_hash": (ev or {}).get(
                "hoc_assessment_evidence_hash"),
            "hoc_persistence_status": (ev or {}).get("hoc_persistence_status"),
            "hoc_persisted": (ev or {}).get("hoc_persisted"),
            "hoc_active_book_id": ((ps or {}).get("active_book") or {}).get("book_id"),
            "hoc_eligible_market_date": ((ps or {}).get("dates") or {}).get(
                "eligible_market_date"),
        }
        return hocm.resolve_binding(
            binding=claimed,
            active_book_id=claimed["hoc_active_book_id"],
            eligible_market_date=claimed["hoc_eligible_market_date"],
            hoc_dir=hoc_dir)

    hb = _get("hoc_binding", hoc_binding, _load_hoc_binding)

    candidate = build_intraday_candidate(
        portfolio_state=ps, event_cycle=ev, reassessment=rs,
        proposal_summary=summ, constrained=con, scoring_identity=sc,
        workflow=wf, hoc_binding=hb,
        observation_received_at=observation_received_at, now=now)
    # The STANDING authority is the later of the persisted governed record and
    # the projected DRC-governed decision, under the ONE ordering — the same
    # resolution the read performs. Comparing against only the persisted record
    # would let an intraday candidate supersede a NEWER daily-cycle decision.
    persisted_standing = load_governed_decision_record(
        active_book_id=(candidate.get("identity") or {}).get("active_book_id"),
        decision_dir=decision_dir)
    projected_standing = project_governed_daily_cycle_decision(
        workflow=wf, reassessment=rs, proposal_summary=summ, constrained=con)
    standing_rows = [r for r in (persisted_standing, projected_standing)
                     if r and r.get("decision")]
    standing = (max(standing_rows, key=governed_decision_ordering_key)
                if standing_rows else None)
    gate = evaluate_intraday_governance(
        candidate=candidate, portfolio_state=ps, event_cycle=ev,
        reassessment=rs, proposal_summary=summ, constrained=con, workflow=wf,
        scoring_identity=sc, rebalance=rebalance, current_governed=standing)

    persisted = None
    if gate.get("eligible"):
        persisted = record_governed_decision(
            candidate=candidate, gate=gate,
            provenance=PROV_GOVERNED_INTRADAY,
            confirm=GOVERNED_DECISION_CONFIRM_TOKEN,
            decision_dir=decision_dir, now=now)
    return {
        "owner": GOVERNANCE_GATE_OWNER,
        "gate_version": GOVERNANCE_GATE_VERSION,
        "verdict": gate.get("verdict"),
        "eligible": bool(gate.get("eligible")),
        "recorded": bool((persisted or {}).get("recorded")),
        "record": (persisted or {}).get("record"),
        "persist_status": (persisted or {}).get("status"),
        "candidate": candidate,
        "gate": gate,
        "standing_decision_id": (standing or {}).get("record_id"),
        "warnings": warnings,
        "safety": _governed_safety(),
    }


# --------------------------------------------------------------------------- #
# The governed READ — which recommendation is authoritative RIGHT NOW
# --------------------------------------------------------------------------- #
def project_governed_daily_cycle_decision(*, workflow: Optional[dict],
                                          reassessment: Optional[dict],
                                          proposal_summary: Optional[dict],
                                          constrained: Optional[dict] = None
                                          ) -> Optional[dict]:
    """The DRC-governed decision, PROJECTED into the governed-decision shape.

    Release 29.5 already declares when a decision is governed by the Daily
    Research Cycle (``governed_research_evidence_current`` — a validated run
    manifest). That decision is not written into this lane's ledger, so it is
    projected here — verbatim, marked ``persisted: False`` — purely so the ONE
    ordering function can compare it with an intraday promotion. Nothing is
    decided, re-derived or written.
    """
    wf = workflow or {}
    rcs = wf.get("research_cycle_state") or {}
    if not rcs.get("governed_research_evidence_current"):
        return None
    rs = reassessment or {}
    summ = proposal_summary or {}
    con = constrained or {}
    # R54.2.3.2 — the projected decision is the GOVERNED ASSESSMENT'S OWN verdict
    # first. Before this fix the decision word was read from the standing proposal's
    # outcome, so on 2026-09-02 the projection stamped the governed CURRENT_NO_CHANGE
    # assessment's own timestamp (23:51:50Z) onto CHANGE_RECOMMENDED taken from a
    # stale event-cycle proposal the governed manifest recorded as NOT_REQUIRED.
    # The proposal outcome is consulted ONLY when the assessment requested it and
    # the standing proposal is bound to that assessment's evidence — proven by the
    # ONE supersession calculation, never re-derived here.
    rs_state = (rs.get("state") or rs.get("reassessment_state")
                or ((rs.get("reassessment") or {}).get("reassessment_state")
                    if isinstance(rs.get("reassessment"), dict) else None))
    rs_ident = ((rs.get("artifact") or {}).get("identity")
                if isinstance(rs.get("artifact"), dict) else None) or {}
    sup = assess_proposal_supersession(
        proposal_summary=summ,
        assessment={
            "available": True,
            "decision": rs_state,
            "eligible_market_date": rs.get("eligible_market_date"),
            "reassessment_hash": rs.get("reassessment_hash"),
            "artifact_id": (rs.get("artifact") or {}).get("reassessment_id")
            if isinstance(rs.get("artifact"), dict) else None,
            "generated_at": (rs.get("artifact") or {}).get("generated_at")
            if isinstance(rs.get("artifact"), dict) else None,
            "hoc_assessment_hash": rs_ident.get("hoc_assessment_hash"),
            # The projection exists only under governed_research_evidence_current,
            # so the assessment it projects IS the governed evidence.
            "is_governed": True,
            "governed_manifest_run_id": rcs.get("governed_manifest_run_id"),
            "governed_provenance": PROV_GOVERNED_DAILY_CYCLE,
        })
    outcome = con.get("outcome") or summ.get("reallocation_outcome")
    if str(rs_state or "") == "CURRENT_NO_CHANGE":
        decision = GD_NO_CHANGE
    elif sup.get("superseded"):
        # The standing proposal is not this assessment's; its outcome projects
        # nothing. Fail closed rather than fabricate a decision word.
        decision = None
    elif outcome == _OUTCOME_PROPOSAL_READY:
        decision = GD_CHANGE_RECOMMENDED
    elif outcome == _OUTCOME_HOLD:
        decision = GD_HOLD_CURRENT_BOOK
    else:
        decision = None
    stamp = (rs.get("artifact") or {}).get("generated_at")
    book_id = ((rs.get("active_book") or {}).get("book_id")
               or (rs.get("proposal_binding") or {}).get("active_book_id"))
    # R54.2.3.2 — a superseded (or unrequested) proposal's hash/outcome never
    # enter the governed identity: the manifest recorded no proposal for this
    # decision, and binding a stale artifact here would launder it back in.
    binds_proposal = bool(decision in (GD_CHANGE_RECOMMENDED, GD_HOLD_CURRENT_BOOK)
                          and not sup.get("superseded"))
    ident = {
        "active_book_id": book_id,
        "eligible_market_session": rs.get("eligible_market_date"),
        "reassessment_hash": rs.get("reassessment_hash"),
        "proposal_hash": ((summ.get("reallocation_proposal_hash")
                           or (wf.get("portfolio_decision_state")
                               or {}).get("proposal_hash"))
                          if binds_proposal else None),
        "target_outcome": outcome if binds_proposal else None,
    }
    return {
        "record_id": "drc_governed_%s" % (rcs.get("governed_manifest_run_id")
                                          or "run"),
        "record_kind": "GOVERNED_PORTFOLIO_DECISION",
        "owner": "api.daily_research_cycle (manifest) via %s" % GOVERNANCE_GATE_OWNER,
        "provenance": PROV_GOVERNED_DAILY_CYCLE,
        "decision": decision,
        "decided_at": stamp,
        "eligible_market_session": rs.get("eligible_market_date"),
        "active_book_id": book_id,
        "candidate_identity_hash": candidate_identity_hash(ident),
        "identity": ident,
        "governed_manifest_run_id": rcs.get("governed_manifest_run_id"),
        "persisted": False,
        "projected": True,
        "projection_note": ("Projected from the Release-29.5 governed-evidence "
                            "contract so the ONE ordering function can compare "
                            "it with an intraday promotion. Not a ledger row."),
        "manual_review_required": bool(decision == GD_CHANGE_RECOMMENDED),
        "safety": _governed_safety(),
    }


def load_governed_portfolio_decision(*, workflow: Optional[dict] = None,
                                     reassessment: Optional[dict] = None,
                                     proposal_summary: Optional[dict] = None,
                                     constrained: Optional[dict] = None,
                                     active_book_id: Optional[str] = None,
                                     decision_dir=None) -> dict:
    """THE authoritative governed portfolio decision right now.

    The later of (a) the newest PERSISTED governed record for the book and
    (b) the projected DRC-governed decision, under the ONE ordering function.
    A non-governed live signal is never a candidate here: it cannot enter this
    lane without passing the gate, and the read says so explicitly.
    """
    persisted = load_governed_decision_record(active_book_id=active_book_id,
                                              decision_dir=decision_dir)
    projected = project_governed_daily_cycle_decision(
        workflow=workflow, reassessment=reassessment,
        proposal_summary=proposal_summary, constrained=constrained)
    candidates = [r for r in (persisted, projected) if r and r.get("decision")]
    latest = (max(candidates, key=governed_decision_ordering_key)
              if candidates else None)
    return {
        "owner": GOVERNANCE_GATE_OWNER,
        "gate_version": GOVERNANCE_GATE_VERSION,
        "available": bool(latest),
        "decision": (latest or {}).get("decision"),
        "decision_vocabulary": list(GOVERNED_DECISION_VOCAB),
        "provenance": (latest or {}).get("provenance"),
        "provenance_vocabulary": list(DECISION_PROVENANCE_VOCAB),
        "decided_at": (latest or {}).get("decided_at"),
        "record_id": (latest or {}).get("record_id"),
        "eligible_market_session": (latest or {}).get("eligible_market_session"),
        "identity": (latest or {}).get("identity") or {},
        "supersedes_decision_id": (latest or {}).get("supersedes_decision_id"),
        "manual_review_required": (latest or {}).get("manual_review_required"),
        "position_recommendations": list(
            (latest or {}).get("position_recommendations") or []),
        "switching_economics": dict((latest or {}).get("switching_economics") or {}),
        "evidence_provenance": dict((latest or {}).get("evidence_provenance") or {}),
        "latency": dict((latest or {}).get("latency") or {}),
        "zero_base": dict((latest or {}).get("zero_base") or {}),
        "gate": dict((latest or {}).get("gate") or {}),
        "persisted": bool((latest or {}).get("persisted", True)),
        "persisted_record_present": bool(persisted),
        "projected_daily_cycle_present": bool(projected),
        "live_signal_is_never_authoritative": True,
        "non_governed_provenance": PROV_LIVE_PRE_DRC_SIGNAL,
        "approval_required_token": CONFIRM_TOKEN,
        "safety": _governed_safety(),
    }


# --------------------------------------------------------------------------- #
# R54.2.3.2 — THE canonical decision-authority selector (Phase B).
# --------------------------------------------------------------------------- #
#: The one explicit authority order, stated once and echoed verbatim by surfaces.
DECISION_AUTHORITY_ORDER = (
    "1. newer governed completed-session decision",
    "2. older governed completed-session decision",
    "3. older proposal awaiting manual review",
    "A governed intraday decision participates only through the R54.1 gate + the "
    "one ordering function; a non-governed / governance-withheld intraday research "
    "result never supersedes an authoritative governed decision.",
)


def resolve_decision_authority(*, assessment: Optional[dict],
                               proposal_summary: Optional[dict],
                               supersession: Optional[dict] = None,
                               governed_decision: Optional[dict] = None,
                               decision_record: Optional[dict] = None) -> dict:
    """Answer, from already-resolved owner views, WHICH decision is authoritative
    right now and WHICH proposal (if any) is currently reviewable. Pure; no io;
    computes no economics and re-decides nothing — the supersession verdict is the
    ONE calculation's output, passed in verbatim.

    ``assessment`` is the same authoritative-assessment view the supersession
    calculation consumed. ``governed_decision`` (optional) is the resolved
    :func:`load_governed_portfolio_decision` answer; when it is newer than the
    assessment under the one ordering it is the authority.
    """
    a = assessment or {}
    summ = proposal_summary or {}
    sup = supersession or {}
    gov = governed_decision or {}
    superseded = bool(sup.get("superseded")
                      or summ.get("reallocation_proposal_superseded"))

    # The authoritative decision: the governed lane's resolved answer when it is
    # available; else the governed assessment of record; else nothing provable.
    authority_id, authority_session, authority_type, authority_owner = (
        None, None, None, None)
    if gov.get("available") and gov.get("decision"):
        authority_id = gov.get("record_id")
        authority_session = gov.get("eligible_market_session")
        authority_type = gov.get("decision")
        authority_owner = gov.get("owner") or GOVERNANCE_GATE_OWNER
    elif a.get("is_governed") is True and a.get("decision"):
        authority_id = a.get("artifact_id") or a.get("governed_manifest_run_id")
        authority_session = (str(a.get("eligible_market_date"))[:10]
                             if a.get("eligible_market_date") else None)
        authority_type = a.get("decision")
        authority_owner = "api.portfolio_reassessment (governed manifest)"

    reviewable = bool(
        summ.get("reallocation_proposal_available")
        and not superseded
        and not summ.get("reallocation_proposal_stale")
        and not summ.get("reallocation_proposal_withheld")
        and summ.get("reallocation_outcome") != _OUTCOME_HOLD
        and (summ.get("reallocation_proposal_approvable")
             in (True, None)))
    superseded_ids = [pid for pid in (
        (sup.get("proposal_id") if superseded else None),) if pid]
    return {
        "owner": OWNER,
        "authority_order": list(DECISION_AUTHORITY_ORDER),
        "current_authoritative_decision_id": authority_id,
        "current_authoritative_session": authority_session,
        "current_authoritative_decision_type": authority_type,
        "current_authoritative_decision_owner": authority_owner,
        "current_reviewable_proposal_id": (
            summ.get("reallocation_proposal_id") if reviewable else None),
        "superseded_proposal_ids": superseded_ids,
        "supersession_reason": (sup.get("reason") if superseded else None),
        "superseded_by": (sup.get("superseded_by") if superseded else None),
        "decision_record_present": bool(decision_record),
        "authority_provable": bool(authority_id),
        "note": ("No governed decision was observable on this read; the standing "
                 "review state is unchanged (fail-closed)."
                 if not authority_id else None),
    }


__all__ = [
    "PHASE", "OWNER", "DECISION_APPROVE", "DECISION_REJECT", "DECISION_HOLD",
    "DECISION_VOCAB", "CONFIRM_TOKEN", "DECISION_STATE_VOCAB",
    "PDS_NO_ACTIVE_BOOK", "PDS_NO_PROPOSAL", "PDS_NO_MATERIAL_CHANGE",
    "PDS_REVIEW_REQUIRED", "PDS_APPROVED", "PDS_REJECTED", "PDS_HELD", "PDS_STALE",
    "PDS_CHANGE_WITHHELD", "PDS_HOLD_CURRENT_BOOK", "APPROVABLE_DECISION_STATES",
    "PDS_UNAVAILABLE", "assess_materiality", "record_decision", "load_decision_record",
    "derive_decision_state", "build_order_plan_preview", "load_portfolio_decision",
    "DECISION_DIR_ENV",
    # --- R54.2.3.2 — decision-over-proposal supersession + authority selector --- #
    "PDS_SUPERSEDED", "SUPERSESSION_OWNER", "SUPERSEDING_ASSESSMENT_DECISIONS",
    "assess_proposal_supersession", "load_decision_supersession",
    "resolve_decision_authority", "DECISION_AUTHORITY_ORDER", "GD_NO_CHANGE",
    # --- R54.1 governed intraday decision lane (this module is the ONE owner) --- #
    "GOVERNANCE_GATE_VERSION", "GOVERNANCE_GATE_OWNER",
    "PROV_GOVERNED_DAILY_CYCLE", "PROV_GOVERNED_INTRADAY",
    "PROV_LIVE_PRE_DRC_SIGNAL", "GOVERNED_PROVENANCE_VOCAB",
    "DECISION_PROVENANCE_VOCAB", "GATE_ELIGIBLE", "GATE_WITHHELD",
    "GATE_VERDICT_VOCAB", "GD_HOLD_CURRENT_BOOK", "GD_CHANGE_RECOMMENDED",
    "GOVERNED_DECISION_VOCAB", "WITHHELD_REASON_VOCAB",
    "POSITION_RECOMMENDATION_VOCAB", "GOVERNED_DECISION_CONFIRM_TOKEN",
    "build_intraday_candidate", "evaluate_intraday_governance",
    "record_governed_decision", "load_governed_decision_record",
    "governed_decision_ordering_key", "project_governed_daily_cycle_decision",
    "load_governed_portfolio_decision", "candidate_identity_hash",
    "govern_latest_intraday_assessment", "ZERO_BASE_INCUMBENCY_POLICY",
]
