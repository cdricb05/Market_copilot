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
import secrets
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from paper_trader.api import reallocation_proposal as realloc
from paper_trader.engine import constrained_reallocation as _cr
from paper_trader.engine import holding_opportunity_cost as _hoc
from paper_trader.engine import selected_target as _st

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
#: R63 — the proposal is intact and reviewable, but it is bound to a session that
#: is no longer the latest the operational workflow is ready for. It remains
#: immutable historical evidence and stays fully READABLE; it is simply no longer
#: a decision anyone may act on with live capital. Distinct from PDS_STALE (the
#: proposal changed under the operator) and from PDS_SUPERSEDED (a newer governed
#: decision already answered the question): here nothing changed and nothing
#: answered it — the world simply moved on to a later session.
PDS_SESSION_STALE = "PROPOSAL_SESSION_STALE"
#: R63 — the operator's governed selection for this session is CURRENT (keep the
#: book). There is no target to approve; the no-change decision is the outcome.
PDS_SELECTION_IS_NO_CHANGE = "SELECTED_TARGET_IS_NO_CHANGE"
#: R69.2 — a NEW approval must name the target it approves. Before this release an
#: APPROVE posted without a governed selection was recorded, and the order-plan
#: owner then built the standing FULL TARGET for it: a missing choice silently
#: became a choice. The proposal is unchanged and still fully readable; only an
#: unattributed approval is refused. Decisions recorded BEFORE this release are
#: untouched and stay readable, re-recordable and executable exactly as they were.
PDS_TARGET_SELECTION_REQUIRED = "TARGET_SELECTION_REQUIRED"
#: R69.2 — the operator selected a target that carries no implementable book (for
#: example a minimum repair whose risk the review could never measure). It is
#: refused HERE, before any approval exists, and the exact reason the selected
#: target cannot be implemented is named. No approval, order plan or order is
#: created, and no other target is substituted for the one that was chosen.
PDS_SELECTED_TARGET_NOT_IMPLEMENTABLE = "SELECTED_TARGET_NOT_IMPLEMENTABLE"
#: R63 live integration — the proposal's own target still leaves a repair
#: obligation open that engine.holding_opportunity_cost already RULED, or it
#: predates the contract and carries no verdict at all. Either way the target
#: may not be approved: approving it would commit capital to a book the system
#: has itself declared non-compliant. The proposal stays immutable and fully
#: readable; only approval is refused. Distinct from PDS_CHANGE_WITHHELD (the
#: kernel could build no compliant target) — here a target exists and is simply
#: not one the owner's rules permit.
PDS_REPAIR_OBLIGATIONS_OPEN = _hoc.OBLIGATIONS_UNRESOLVED
#: R69.5 — the selected target clears its own per-name risk-contribution cap only
#: because that cap ROSE when the covariance universe shrank. Against the limit the
#: CURRENT book was actually judged against it does not comply, and on 2026-09-23
#: two of the four offending names are ones the target itself created. That is a
#: RISK-POLICY question, not a constraint failure: the declared 3/N policy is
#: correctly applied and this gate changes no threshold, grants no exception and
#: approves nothing. It withholds approval until an operator rules on the policy and
#: binds that ruling to the exact instruments, the exact reference limit and the
#: exact frozen book. REJECT and HOLD stay available throughout.
PDS_RISK_POLICY_REVIEW_REQUIRED = "SELECTED_TARGET_REQUIRES_RISK_POLICY_REVIEW"
#: R82 — the operator RULED, and the ruling is JUDGE_AGAINST_THE_BEFORE_UNIVERSE.
#: The cap the breach was raised against is now the binding cap for this frozen
#: book, and the target does not satisfy it: the risk-contribution obligations that
#: the shrinking covariance universe discharged are OPEN again. This is a different
#: state from PDS_RISK_POLICY_REVIEW_REQUIRED, which means "nobody has ruled yet" —
#: here the ruling exists and the target fails it, so no acknowledgement can clear
#: it. The proposal stays immutable and fully readable; REJECT and HOLD stay
#: available; nothing is written and no threshold moved.
PDS_REFERENCE_LIMIT_BREACH_RULED = "SELECTED_TARGET_BREACHES_THE_RULED_REFERENCE_LIMIT"
PDS_UNAVAILABLE = "PORTFOLIO_DECISION_UNAVAILABLE"
DECISION_STATE_VOCAB = (
    PDS_NO_ACTIVE_BOOK, PDS_NO_PROPOSAL, PDS_NO_MATERIAL_CHANGE, PDS_REVIEW_REQUIRED,
    PDS_APPROVED, PDS_REJECTED, PDS_HELD, PDS_STALE, PDS_CHANGE_WITHHELD,
    PDS_HOLD_CURRENT_BOOK, PDS_SUPERSEDED, PDS_SESSION_STALE,
    PDS_SELECTION_IS_NO_CHANGE, PDS_REPAIR_OBLIGATIONS_OPEN,
    PDS_TARGET_SELECTION_REQUIRED, PDS_SELECTED_TARGET_NOT_IMPLEMENTABLE,
    PDS_RISK_POLICY_REVIEW_REQUIRED, PDS_REFERENCE_LIMIT_BREACH_RULED,
    PDS_UNAVAILABLE)

# --------------------------------------------------------------------------- #
# R69.5 — the RISK-POLICY ACKNOWLEDGEMENT
#
# The token is re-exported from the kernel that defines the state, never
# re-spelled: one vocabulary, one owner. A ruling is refused unless it names the
# same instruments, the same reference limit and the same frozen book the operator
# was shown — so a selection revised underneath a ruling makes that ruling stale
# and fails the approval closed, exactly as a revised selection does.
# --------------------------------------------------------------------------- #
RISK_POLICY_ACK_TOKEN = _st.POLICY_REVIEW_ACK_TOKEN
#: Why an acknowledgement was not accepted. Structured; never free text.
ACK_MISSING = "RISK_POLICY_ACKNOWLEDGEMENT_MISSING"
ACK_BAD_TOKEN = "RISK_POLICY_ACKNOWLEDGEMENT_TOKEN_INVALID"
ACK_WRONG_BOOK = "RISK_POLICY_ACKNOWLEDGEMENT_BOUND_TO_A_DIFFERENT_BOOK"
ACK_WRONG_LIMIT = "RISK_POLICY_ACKNOWLEDGEMENT_REFERENCE_LIMIT_MISMATCH"
ACK_WRONG_INSTRUMENTS = "RISK_POLICY_ACKNOWLEDGEMENT_INSTRUMENTS_MISMATCH"
ACK_NOT_PUBLISHED = "RISK_POLICY_REVIEW_NOT_PUBLISHED_BY_SELECTION"
ACK_REASON_VOCAB = (ACK_MISSING, ACK_BAD_TOKEN, ACK_WRONG_BOOK, ACK_WRONG_LIMIT,
                    ACK_WRONG_INSTRUMENTS, ACK_NOT_PUBLISHED)
#: The ONLY states in which any surface may expose an approvable proposal action.
APPROVABLE_DECISION_STATES = (PDS_REVIEW_REQUIRED, PDS_HELD)

# --------------------------------------------------------------------------- #
# R63 — DECISION FRESHNESS
#
# A persisted proposal does not become actionable merely by continuing to exist.
# The 2026-09-18 proposal survived the 2026-09-21 close: it is still perfectly
# readable evidence, and it is no longer a decision about the current book.
#
# NO second calendar, session authority or clock is introduced here. The latest
# eligible session is asked of the ONE owner that already computes it
# (``api.workflow_state``, which composes ``engine.market_session`` through
# ``api.data_freshness``); this module only COMPARES two dates it is given.
# --------------------------------------------------------------------------- #
FRESHNESS_CURRENT = "CURRENT"
FRESHNESS_STALE = "STALE"
FRESHNESS_UNVERIFIABLE = "UNVERIFIABLE"
FRESHNESS_VOCAB = (FRESHNESS_CURRENT, FRESHNESS_STALE, FRESHNESS_UNVERIFIABLE)

#: The operator action a stale decision points at. Reuses the existing operator
#: action token owned by ``api.workflow_state`` (``OP_ACTION_RUN_CYCLE``) rather
#: than coining a new one.
NEXT_ACTION_RUN_PORTFOLIO_CYCLE = "RUN_PORTFOLIO_CYCLE"
SESSION_AUTHORITY_OWNER = "api.workflow_state"
SESSION_CALENDAR_OWNER = "engine.market_session"

# --------------------------------------------------------------------------- #
# R82.2 — THE NEXT REQUIRED ACTION, spelled ONCE.
#
# Until this release the two literals below marked with "(R82/R82.1)" were typed
# inline inside ``record_decision``'s refusal payloads and existed nowhere a read
# surface could find them, while the browser invented its own answer from
# ``actionable && a selection exists``. On 2026-09-28 the live panel therefore said
# NEXT REQUIRED ACTION = APPROVE SELECTED TARGET and offered an armed APPROVE
# MINIMUM REPAIR control in the same paint as APPROVAL WITHHELD and "Step 4 — RISK
# POLICY DECISION (yours to make)".
#
# These are that vocabulary. Every one of them is a word the WRITE path already
# uses or would use, and the read projection below hands the same word to the
# screen, so a surface can name the operator's next act without holding a rule.
# --------------------------------------------------------------------------- #
#: No governed selection exists for this session yet.
NEXT_ACTION_SELECT_A_TARGET = "SELECT_A_TARGET"
#: A selection exists and it is bound to a proposal this read no longer serves.
NEXT_ACTION_SELECT_AGAINST_THE_CURRENT_REVIEW = (
    "SELECT_A_TARGET_AGAINST_THE_CURRENT_REVIEW")
#: (R69.2) the selected target carries no implementable book.
NEXT_ACTION_SELECT_AN_IMPLEMENTABLE_TARGET = "SELECT_AN_IMPLEMENTABLE_TARGET"
#: The governed selection is CURRENT: there is no target to approve.
NEXT_ACTION_RECORD_THE_NO_CHANGE_DECISION = "RECORD_THE_NO_CHANGE_DECISION"
#: (R82) the frozen book owes an operator risk-policy ruling that no AUTHORITATIVE
#: record answers. Approval is withheld until one exists.
NEXT_ACTION_RECORD_RISK_POLICY_RULING = "RECORD_RISK_POLICY_RULING"
#: (R82.1) a verified ruling refuses this frozen book, and the governed review has
#: solved the compliant successor the operator should review and select instead.
NEXT_ACTION_REVIEW_THE_POLICY_COMPLIANT_SUCCESSOR = (
    "REVIEW_AND_SELECT_THE_POLICY_COMPLIANT_SUCCESSOR_TARGET")
#: Every gate is clear. This is the ONLY word that may arm an approval affordance.
NEXT_ACTION_APPROVE_SELECTED_TARGET = "APPROVE_SELECTED_TARGET"
NEXT_ACTION_VOCAB = (
    NEXT_ACTION_RUN_PORTFOLIO_CYCLE, NEXT_ACTION_SELECT_A_TARGET,
    NEXT_ACTION_SELECT_AGAINST_THE_CURRENT_REVIEW,
    NEXT_ACTION_SELECT_AN_IMPLEMENTABLE_TARGET,
    NEXT_ACTION_RECORD_THE_NO_CHANGE_DECISION,
    NEXT_ACTION_RECORD_RISK_POLICY_RULING,
    NEXT_ACTION_REVIEW_THE_POLICY_COMPLIANT_SUCCESSOR,
    NEXT_ACTION_APPROVE_SELECTED_TARGET)
#: The operator-facing wording of each action, owned HERE. A browser that turned a
#: code into prose would be holding an interpretation of workflow state, which is
#: exactly what this release removes from it.
NEXT_ACTION_LABELS = {
    NEXT_ACTION_RUN_PORTFOLIO_CYCLE: "Run portfolio cycle",
    NEXT_ACTION_SELECT_A_TARGET: "Select a target",
    NEXT_ACTION_SELECT_AGAINST_THE_CURRENT_REVIEW: (
        "Select a target against the current review"),
    NEXT_ACTION_SELECT_AN_IMPLEMENTABLE_TARGET: "Select an implementable target",
    NEXT_ACTION_RECORD_THE_NO_CHANGE_DECISION: "Record the no-change decision",
    NEXT_ACTION_RECORD_RISK_POLICY_RULING: "Risk policy decision",
    NEXT_ACTION_REVIEW_THE_POLICY_COMPLIANT_SUCCESSOR: (
        "Review and select the policy-compliant successor"),
    NEXT_ACTION_APPROVE_SELECTED_TARGET: "Approve selected target",
}


def latest_eligible_session(*, workflow_state: Optional[dict] = None,
                            loader: Optional[Callable] = None) -> Optional[str]:
    """THE latest session the operational workflow is ready to act on, or None.

    Delegated, never derived. ``action_session_market_date`` is the value the
    workflow-state owner already publishes for exactly this question: during a
    catch-up it is the OLDEST unclosed completed session (the one the operator
    must actually run), and otherwise the latest eligible session. Returns None
    when the owner cannot answer, so the caller can fail closed on its own terms.
    """
    ws = workflow_state
    if ws is None:
        try:
            if loader is None:
                from paper_trader.api import workflow_state as _ws  # lazy: cycle
                loader = _ws.load_workflow_state
            ws = loader()
        except Exception:  # noqa: BLE001 - never let a read break a decision path
            return None
    return ((ws or {}).get("action_session_market_date")
            or (ws or {}).get("eligible_market_date") or None)


def decision_freshness(*, bound_session: Optional[str],
                       latest_session: Optional[str]) -> dict:
    """Is a decision bound to ``bound_session`` still actionable? PURE date compare.

    Fails closed in both directions: an unknown session on either side is
    UNVERIFIABLE and is NOT actionable, because "we could not tell" must never
    read as "yes".
    """
    b = (bound_session or "").strip() or None
    l = (latest_session or "").strip() or None
    if b is None or l is None:
        state = FRESHNESS_UNVERIFIABLE
    elif b == l:
        state = FRESHNESS_CURRENT
    elif b < l:
        state = FRESHNESS_STALE
    else:
        # The bound session is AHEAD of what the workflow says is actionable. That
        # is not freshness, it is an inconsistency, and it is never actionable.
        state = FRESHNESS_UNVERIFIABLE
    actionable = state == FRESHNESS_CURRENT
    return {
        "state": state,
        "vocabulary": list(FRESHNESS_VOCAB),
        "bound_session": b,
        "latest_eligible_session": l,
        "actionable": actionable,
        "target_selection_allowed": actionable,
        "approval_allowed": actionable,
        "order_plan_confirmation_allowed": actionable,
        "next_required_action": (None if actionable
                                 else NEXT_ACTION_RUN_PORTFOLIO_CYCLE),
        "session_authority_owner": SESSION_AUTHORITY_OWNER,
        "session_calendar_owner": SESSION_CALENDAR_OWNER,
        "readable_as_history": True,
        "immutable": True,
        "detail": (
            "The proposal's session is the latest the workflow is ready to act on."
            if state == FRESHNESS_CURRENT else
            ("This decision is bound to session %s, but the latest eligible "
             "session is %s. It remains immutable, readable historical evidence "
             "and can no longer be selected, approved or confirmed. Run the "
             "portfolio cycle for the current session to produce a fresh "
             "decision." % (b, l)) if state == FRESHNESS_STALE else
            ("The bound session (%s) could not be reconciled with the latest "
             "eligible session (%s), so no action is permitted." % (b, l))),
    }

# Structural (membership) vs resize action tokens (mirror engine.reallocation_proposal).
_MEMBERSHIP_ACTIONS = ("EXIT", "ADD", "REPLACE_IN", "REPLACE_OUT")
_RESIZE_ACTIONS = ("INCREASE", "REDUCE")

# --- Ledger root (a decision-evidence root, NEVER the operational desk ledger) ----- #
DECISION_DIR_ENV = "PAPER_TRADER_PORTFOLIO_DECISION_DIR"
_DEFAULT_DECISION_DIR = Path(r"D:\Stock_Prediction_app_data\portfolio_decisions")
_RECORDS_FILE = "decisions.json"
_INDEX_FILE = "index.json"
#: R63 — target selections live in the SAME governance ledger root as the
#: decisions they precede. They are a governance artifact, never a second
#: proposal store: a selection references the immutable proposal, it never
#: replaces it and it holds no target this system did not already compute.
_SELECTIONS_FILE = "target_selections.json"
_SELECTION_INDEX_FILE = "target_selection_index.json"


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


def _selections_path(decision_dir=None) -> Path:
    return _decision_dir(decision_dir) / _SELECTIONS_FILE


def _selection_index_path(decision_dir=None) -> Path:
    return _decision_dir(decision_dir) / _SELECTION_INDEX_FILE


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
                    portfolio_state_loader: Optional[Callable] = None,
                    expected_selection_id: Optional[str] = None,
                    expected_selected_target: Optional[str] = None,
                    risk_policy_acknowledgement: Optional[dict] = None,
                    latest_session: Optional[str] = None,
                    workflow_state: Optional[dict] = None,
                    enforce_session_freshness: bool = True) -> dict:
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
        (a stale proposal can never be approved against a changed portfolio);
      * R69.5 — when the selected target satisfies its own per-name risk-contribution
        cap only because that cap ROSE with a shrinking covariance universe,
        ``risk_policy_acknowledgement`` must carry an operator ruling bound to the
        exact frozen book, reference limit and instruments, or the approval is
        withheld with ``SELECTED_TARGET_REQUIRES_RISK_POLICY_REVIEW``. The ruling
        authorises THAT target only: it changes no threshold and grants no standing
        exception.
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

    # --- R63 session-freshness gate: fail closed ------------------------------ #
    # A proposal does not stay actionable merely by remaining persisted. Once a
    # later session is the one the workflow is ready to act on, this proposal is
    # historical evidence: still readable, never approvable. It is NOT rewritten,
    # regenerated, rejected or superseded to satisfy this gate; only the ability
    # to ACT on it expires.
    #
    # It sits AFTER the structural and economic guards above deliberately. Those
    # refusals - withheld, hold-current-book, immaterial - are properties of the
    # proposal ITSELF and are true in every session, so they are the more
    # specific and more useful answer. Safety is identical either way: every one
    # of these paths refuses and writes nothing. Nothing can reach a write
    # without passing this gate.
    freshness = decision_freshness(
        bound_session=binding.get("eligible_market_date"),
        latest_session=(latest_session if latest_session is not None
                        else (latest_eligible_session(workflow_state=workflow_state)
                              if enforce_session_freshness else
                              binding.get("eligible_market_date"))))
    if enforce_session_freshness and not freshness["approval_allowed"]:
        return {**base, "status": PDS_SESSION_STALE, "freshness": freshness,
                "binding": binding, "current_proposal_hash": current_hash,
                "next_required_action": freshness["next_required_action"],
                "message": freshness["detail"]}

    # --- R63 live integration: a non-compliant target can never be APPROVED --- #
    # The reviewability invariant has to bind on the WRITE path, not only in the
    # review projection. The selection gate below refuses a non-selectable
    # FULL_TARGET, but it only engages once a selection EXISTS: a caller that
    # posts this endpoint without selecting anything skipped it entirely and
    # approved the standing target. On 2026-09-22 that standing target retained
    # VLO and halved LH against an EXIT_TO_ZERO ruling, and this gate recorded
    # the approval.
    #
    # The verdict is the proposal kernel's own, read back off the immutable
    # artifact - no second classifier, and no obligation is re-derived here.
    # Scoped to APPROVE: REJECT and HOLD stay available, because refusing those
    # too would leave the operator no way to record a judgement on a proposal
    # they cannot approve.
    if decision == DECISION_APPROVE:
        repair = realloc.kernel.mandatory_repair_read_verdict(
            (artifact or {}).get("proposal") or {})
        if not repair["reviewable"]:
            return {**base, "status": PDS_REPAIR_OBLIGATIONS_OPEN,
                    "binding": binding, "current_proposal_hash": current_hash,
                    "mandatory_repair_verdict": repair,
                    "obligations_open": list(repair["instruments"]),
                    "next_required_action": "RUN_PORTFOLIO_CYCLE",
                    "message": (
                        "This target cannot be approved: %s Approving it would "
                        "commit capital to a book the system has itself ruled "
                        "non-compliant. The proposal is unchanged and remains "
                        "readable; select the minimum repair, or run the "
                        "portfolio cycle for a target that resolves it."
                        % repair["detail"])}

    # --- R63: approval must consume EXACTLY the governed selection ------------ #
    # When the operator has selected a target, the approval gate approves THAT
    # target. It may never fall back to the standing full target, because the
    # operator would then have approved something they explicitly did not choose.
    selection = load_target_selection(
        active_book_id=binding.get("active_book_id"),
        eligible_market_date=binding.get("eligible_market_date"),
        decision_dir=decision_dir)
    # The decision already on file for this exact (book, session). Read here so the
    # gates below can tell a NEW approval from a replay of one recorded earlier.
    prior_decision = load_decision_record(
        active_book_id=binding.get("active_book_id"),
        eligible_market_date=binding.get("eligible_market_date"),
        decision_dir=decision_dir)
    replaying_prior = bool(prior_decision
                           and prior_decision.get("proposal_hash") == current_hash
                           and prior_decision.get("decision") == decision)
    if decision == DECISION_APPROVE and selection is not None:
        sb = selection.get("binding") or {}
        mismatches = [
            (name, exp, act) for name, exp, act in (
                ("proposal_hash", sb.get("proposal_hash"), current_hash),
                ("selection_id", expected_selection_id or sb.get("selection_id")
                 or selection.get("selection_id"), selection.get("selection_id")),
                # R69.2 - the operator's browser says which target it was looking
                # at. If the governed ledger holds a different one, the selection
                # was revised under them and this approval would attach to a book
                # they never saw.
                ("selected_target", expected_selected_target,
                 selection.get("selected_target")),
            ) if exp is not None and act is not None and exp != act]
        if mismatches:
            return {**base, "status": PDS_STALE, "binding": binding,
                    "selection": selection, "mismatches": mismatches,
                    "current_proposal_hash": current_hash,
                    "message": ("The governed target selection does not match the "
                                "proposal being approved (%s). Re-select a target "
                                "against the current review before approving."
                                % ", ".join(m[0] for m in mismatches))}
        if selection.get("selected_target") == TARGET_CURRENT:
            return {**base, "status": PDS_SELECTION_IS_NO_CHANGE, "binding": binding,
                    "selection": selection,
                    "message": ("The governed selection for this session is CURRENT "
                                "(keep the book unchanged). There is no target to "
                                "approve; record the no-change decision instead.")}
        # --- R69.2: a target with no implementable book is refused HERE --------- #
        # Before approval, not after it, and never by offering another target in
        # its place. The reason is the one engine.selected_target named.
        impl_verdict = recorded_selection_implementability(selection)
        if not impl_verdict["implementable"] and not replaying_prior:
            reason = impl_verdict["reason"]
            return {**base, "status": PDS_SELECTED_TARGET_NOT_IMPLEMENTABLE,
                    "binding": binding, "selection": selection,
                    "selected_target": selection.get("selected_target"),
                    "not_implementable_reason": reason,
                    "not_implementable_detail": impl_verdict["detail"],
                    "not_implementable_vocabulary": list(
                        _st.NOT_IMPLEMENTABLE_VOCAB),
                    "current_proposal_hash": current_hash,
                    "next_required_action": "SELECT_AN_IMPLEMENTABLE_TARGET",
                    "message": (
                        "The governed selection for this session is %s, and it "
                        "carries no implementable book (%s). Approving it would "
                        "record a decision no order plan could ever honour, and "
                        "this gate will not substitute a different target for the "
                        "one that was chosen. Nothing was written. Select a target "
                        "the review publishes as implementable, or run the "
                        "portfolio cycle for a fresh proposal."
                        % (selection.get("selected_target"),
                           reason or "reason not published by the review"))}
        # --- R69.5: a cap that cleared itself is a POLICY question ------------- #
        # The target below is constraint-VALID: its own 3/N cap reports zero
        # breaches, every mandatory obligation is discharged, and the declared
        # policy was applied correctly on both sides. It is nonetheless not the
        # same book the current portfolio was judged as. On 2026-09-23 the full
        # target carries four names above the 12% limit the current book was held
        # to — 54.6% of portfolio risk on 14.8% of NAV — and two of the four are
        # concentrations the target itself created (ALAB raised 1.84% -> 3.21%,
        # SNDK added at 2.88%), which no denominator argument reaches.
        #
        # This gate does not change the cap, grant an exception, judge the target
        # or prefer another one. It withholds APPROVAL until an operator rules on
        # the policy, and binds that ruling to the exact frozen book so a selection
        # revised underneath it makes it stale rather than silently portable.
        # Scoped to APPROVE and never to a replay, for the same reason every gate
        # above is: REJECT and HOLD must stay available on a target that cannot be
        # approved, and a decision recorded earlier stays re-recordable as it was.
        policy_review = selection_policy_review(selection)
        # --- R82: the operator already RULED, and the ruling refuses this book -- #
        # This check runs BEFORE the acknowledgement below on purpose. A recorded
        # JUDGE_AGAINST_THE_BEFORE_UNIVERSE says the cap the breach was raised
        # against is the cap this frozen book is judged by; an acknowledgement that
        # the same book breaches that cap cannot then clear it, or the ruling would
        # be advisory. The refusal is substantive, not procedural: the ruling exists
        # and the target fails it. Nothing is written, no threshold moved, no
        # exception was granted, and REJECT / HOLD stay available.
        ruled = risk_policy_ruling_state(selection=selection,
                                         decision_dir=decision_dir)
        # R82.2 — the ORDER and the words of the two risk-policy refusals below are
        # no longer typed here. They come from risk_policy_approval_gate, which the
        # READ projection consumes too, so the panel can never present approval as
        # available in a state this gate would refuse.
        policy_gate = risk_policy_approval_gate(policy_review=policy_review,
                                                ruling_state=ruled)
        if policy_gate["status"] == PDS_REFERENCE_LIMIT_BREACH_RULED \
                and not replaying_prior:
            return {**base, "status": PDS_REFERENCE_LIMIT_BREACH_RULED,
                    "binding": binding, "selection": selection,
                    "selected_target": selection.get("selected_target"),
                    "risk_policy_review": policy_review,
                    "risk_policy_ruling": ruled,
                    "risk_policy_owner": _st.RISK_POLICY_OWNER,
                    "declared_policy_changed": False,
                    "exception_granted": False,
                    "current_proposal_hash": current_hash,
                    # R82.1 — this used to say SELECT_A_COMPLIANT_TARGET_OR_REJECT
                    # while no compliant target existed anywhere in the system. The
                    # governed review now SOLVES one against the ruled cap and
                    # publishes it as %s, so the next action names something the
                    # operator can actually do.
                    "next_required_action": policy_gate["next_required_action"],
                    "approval_gate": policy_gate,
                    "policy_compliant_successor_target": (
                        TARGET_POLICY_COMPLIANT_REPAIR),
                    "manual_review_reference": _st.POLICY_REVIEW_REFERENCE_DOC,
                    "message": (
                        "The governed risk-policy ruling on record for this frozen "
                        "book is %s, so the binding per-name cap is %s. %s Nothing "
                        "was written, no threshold moved and no exception was "
                        "granted. The proposal decision review re-solves the repair "
                        "against the ruled cap and publishes the result as the %s "
                        "target: review it, select it, and approve that selection at "
                        "this gate. Reject and hold remain available."
                        % (ruled.get("ruling"), ruled.get("binding_limit"),
                           ruled.get("detail") or "",
                           TARGET_POLICY_COMPLIANT_REPAIR))}
        # --- R82.1: a VERIFIED ruling is the answer the review was waiting for --- #
        # R69.5 withheld approval "until an operator rules on the policy" and then
        # accepted only a per-request acknowledgement. A durable ruling recorded
        # through the governed operator review, bound to this exact frozen book, is
        # a stronger record of the same decision, so it satisfies the review. It
        # does NOT approve anything: the operator still has to record the decision
        # at this gate with its own confirmation token, and every other gate below
        # still runs. An UNVERIFIED ruling satisfies nothing — the ack path stands.
        if policy_gate["status"] == PDS_RISK_POLICY_REVIEW_REQUIRED \
                and not replaying_prior:
            ack_verdict = validate_risk_policy_acknowledgement(
                policy_review=policy_review,
                acknowledgement=risk_policy_acknowledgement)
            if not ack_verdict["accepted"]:
                return {**base, "status": PDS_RISK_POLICY_REVIEW_REQUIRED,
                        "binding": binding, "selection": selection,
                        "selected_target": selection.get("selected_target"),
                        "risk_policy_review": policy_review,
                        "risk_policy_acknowledgement": ack_verdict,
                        "risk_policy_owner": _st.RISK_POLICY_OWNER,
                        "declared_policy_changed": False,
                        "exception_granted": False,
                        "current_proposal_hash": current_hash,
                        "next_required_action": policy_gate[
                            "next_required_action"],
                        "approval_gate": policy_gate,
                        "manual_review_reference": _st.POLICY_REVIEW_REFERENCE_DOC,
                        "message": (
                            "The selected %s is valid against its OWN per-name risk "
                            "limit and is not valid against the limit the current "
                            "book was judged against. %s Approval is withheld, not "
                            "refused: nothing was written, no threshold moved, no "
                            "exception was granted and the target is unchanged. "
                            "Record the risk-policy ruling (%s), or select a target "
                            "that complies with the reference limit, or reject / "
                            "hold this proposal — all three remain available."
                            % (selection.get("selected_target"),
                               policy_review.get("detail") or "",
                               ack_verdict["reason"]))}

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

    # --- R69.2: a NEW approval must name the target it approves ---------------- #
    # An APPROVE with no governed selection used to be recorded, and the order-plan
    # owner then built the standing FULL TARGET for it. A missing choice became a
    # choice, silently.
    #
    # It sits HERE, as the last gate before the write, for the same reason the
    # session-freshness gate sits where it does: every refusal above is a property
    # of the PROPOSAL itself - superseded, withheld, below the hurdle, immaterial,
    # stale, corporate-action stale - and each of those is the more specific and
    # more useful answer. Safety is identical either way, because all of them write
    # nothing; nothing can reach a write without passing this.
    #
    # Scoped to APPROVE, and never to a replay: REJECT and HOLD stay available so
    # an operator can always record a judgement on a proposal they cannot approve,
    # and a decision recorded before this release stays readable and idempotently
    # re-recordable exactly as it was.
    if decision == DECISION_APPROVE and selection is None and not replaying_prior:
        return {**base, "status": PDS_TARGET_SELECTION_REQUIRED, "binding": binding,
                "current_proposal_hash": current_hash,
                "target_vocabulary": list(TARGET_VOCAB),
                "selection_confirm_token": SELECTION_CONFIRM_TOKEN,
                "next_required_action": "SELECT_TARGET",
                "selection_route": "POST /v1/operations/portfolio-decision/select-target",
                "message": (
                    "No governed target selection exists for this session, so there "
                    "is nothing to approve BY NAME. An approval recorded without one "
                    "used to be executed as the standing full target, which is not a "
                    "choice the operator made. Select CURRENT, MINIMUM_REPAIR or "
                    "FULL_TARGET first; the approval then binds exactly that target. "
                    "Nothing was written and the proposal is unchanged.")}

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

    # R82 — read the standing ruling ONCE, so the record cannot carry an id from one
    # read and a block from another if the store moved between them.
    ruling_at_decision = (risk_policy_ruling_state(selection=selection,
                                                   decision_dir=decision_dir)
                          if selection is not None else None)

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
        # --- R69.2: WHICH target this decision approves ------------------------- #
        # A decision record used to say only "APPROVE", and the order-plan owner
        # inferred the target from whatever selection happened to be on file at
        # the moment it was asked - so a selection revised after the approval
        # silently changed what had been approved. The approved target is now part
        # of the immutable decision itself. A record written before this release
        # carries None here, which is what makes a legacy approval identifiable
        # rather than merely indistinguishable.
        "selected_target": (selection or {}).get("selected_target"),
        "selection_id": (selection or {}).get("selection_id"),
        "selected_target_hash": ((selection or {}).get("binding")
                                 or {}).get("selected_target_hash"),
        "selected_target_implementation_hash": (
            (selection or {}).get("selected_target_implementation_hash")),
        "selected_target_implementable": (
            None if selection is None
            else bool(recorded_selection_implementability(selection)["implementable"])),
        "target_selection_owner": OWNER,
        "target_binding_contract": "R69.2_SELECTED_TARGET_BOUND_TO_DECISION",
        # --- R69.5: the risk-policy ruling this approval rests on --------------- #
        # None when the target complied with the reference limit and no ruling was
        # ever needed — which is what makes an approval that DID need one findable
        # for ever, rather than merely indistinguishable from one that did not.
        "risk_policy_review": (
            selection_policy_review(selection) if selection is not None else None),
        "risk_policy_acknowledgement": (
            dict(risk_policy_acknowledgement)
            if isinstance(risk_policy_acknowledgement, dict) else None),
        "risk_policy_contract": "R69.5_REFERENCE_LIMIT_RULING_BOUND_TO_THE_FROZEN_BOOK",
        # --- R82: the DURABLE governed ruling this decision was taken under ------ #
        # The acknowledgement above lives only in the approve request. The ruling is
        # an artifact of its own, so a decision records WHICH ruling was standing at
        # the moment it was taken — including ``NO_RULING_ON_RECORD``, which is the
        # honest answer for every decision recorded before this lane existed.
        "risk_policy_ruling_id": (ruling_at_decision or {}).get("ruling_id"),
        "risk_policy_ruling": ruling_at_decision,
        "risk_policy_ruling_contract": "R82_RULING_BOUND_TO_ONE_PROPOSAL_AND_TARGET",
        "declared_risk_policy_changed": False,
        "standing_risk_policy_exception_granted": False,
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
# R63 — GOVERNED TARGET SELECTION (between REVIEW and APPROVE)
#
# The R62 review can say "the minimum repair is the better review path", but
# until now the approval path only knew ONE target: the standing proposal's full
# target. An operator who agreed with the review had no way to act on it.
#
# This adds exactly ONE governed step. It is NOT an approval, NOT an order plan
# and NOT an optimisation: the three targets all come from the review, which
# derived them from the immutable proposal. A selection RECORDS WHICH ONE the
# operator wants to put in front of the existing Approve gate, and binds every
# identity that makes that choice meaningful, so a later approval cannot silently
# consume a different target than the one that was chosen.
#
# The three manual gates stay independent and in order:
#   Review -> SELECT TARGET -> Approve -> Confirm order plan -> next close
# --------------------------------------------------------------------------- #
TARGET_CURRENT = "CURRENT"
TARGET_MINIMUM_REPAIR = "MINIMUM_REPAIR"
TARGET_FULL_TARGET = "FULL_TARGET"
#: R82.1 — the SUCCESSOR target a governed JUDGE ruling produces: the same minimum
#: repair, re-solved by the same owner against the cap the ruling makes binding. It
#: is selectable ONLY while the read seam publishes one, which happens only under an
#: authoritative ruling that reopened an obligation — so it can never be selected on
#: a book nobody ruled on. Everything downstream of the selection is unchanged:
#: ``api.rebalance_execution.resolve_target`` already builds any non-full target
#: from the book frozen with the selection.
TARGET_POLICY_COMPLIANT_REPAIR = "POLICY_COMPLIANT_REPAIR"
TARGET_VOCAB = (TARGET_CURRENT, TARGET_MINIMUM_REPAIR, TARGET_FULL_TARGET,
                TARGET_POLICY_COMPLIANT_REPAIR)

#: A selection is an explicit operator act and carries its own token, distinct
#: from the approval token so neither can ever be replayed as the other.
SELECTION_CONFIRM_TOKEN = "CONFIRM_PORTFOLIO_TARGET_SELECTION"

TS_CREATED = "CREATED"
TS_REUSED = "REUSED_EXISTING"
TS_REVISED = "REVISED"
TS_NOT_RECORDED = "NOT_RECORDED"
TS_NOT_SELECTABLE = "TARGET_NOT_SELECTABLE"
TS_STALE = "STALE_REVIEW_SELECTION_REFUSED"
TS_SESSION_STALE = PDS_SESSION_STALE
TS_NO_REVIEW = "NO_REVIEW_AVAILABLE"
SELECTION_STATUS_VOCAB = (TS_CREATED, TS_REUSED, TS_REVISED, TS_NOT_RECORDED,
                          TS_NOT_SELECTABLE, TS_STALE, TS_SESSION_STALE, TS_NO_REVIEW)


def load_target_selection(*, active_book_id: Optional[str],
                          eligible_market_date: Optional[str],
                          decision_dir=None) -> Optional[dict]:
    """The latest governed target selection for an exact (book, session), or None.
    PURE reader; never raises."""
    try:
        index = _load_json(_selection_index_path(decision_dir)) or {}
        ptr = index.get(_index_key(active_book_id, eligible_market_date))
        if not ptr:
            return None
        sid = ptr.get("selection_id")
        rows = _load_json(_selections_path(decision_dir)) or []
        for rec in reversed(rows):
            if rec.get("selection_id") == sid:
                return rec
        return ptr.get("record")
    except Exception:  # noqa: BLE001 - a pure read must never crash the caller
        return None


def _selection_binding(*, review_envelope: dict, option: dict,
                       target: str, implementation: Optional[dict] = None) -> dict:
    """Every identity a selection must bind, read from the review envelope.

    If any of these moves, the selection no longer describes the world it was made
    in and the approval below refuses it.
    """
    rev = (review_envelope or {}).get("review") or {}
    ident = rev.get("reviewed_proposal") or {}
    inputs = (review_envelope or {}).get("inputs") or {}
    return {
        "proposal_id": (review_envelope or {}).get("proposal_id"),
        "proposal_hash": (review_envelope or {}).get("proposal_hash")
                         or ident.get("proposal_hash"),
        "review_hash": (review_envelope or {}).get("review_hash"),
        "hoc_assessment_hash": (inputs.get("hoc_assessment_hash_used")
                                or inputs.get("hoc_assessment_hash_bound_by_proposal")
                                or ident.get("hoc_assessment_hash")),
        "eligible_market_date": (ident.get("eligible_market_date")
                                 or (review_envelope or {}).get("eligible_market_date")),
        "active_book_id": (ident.get("active_book_id")
                           or ((review_envelope or {}).get("active_book") or {}).get("id")),
        "portfolio_state_hash": ident.get("portfolio_state_hash"),
        "corporate_actions_hash": ident.get("corporate_actions_hash"),
        "universe_scoring_hash": ident.get("universe_scoring_hash"),
        "proposal_read_state": (review_envelope or {}).get("proposal_read_state"),
        "review_verdict": (rev.get("review_verdict") or {}).get("verdict"),
        "selected_target": target,
        # The identity of the TARGET itself, so an approval can prove it is
        # approving the same weights the operator saw.
        "selected_target_hash": _target_hash(option),
        # R69.2 — the identity of the BOOK, not only of the headline economics.
        # ``selected_target_hash`` above proves the operator saw the same summary;
        # this proves they get the same weights. A MINIMUM_REPAIR approval that
        # produced the FULL TARGET's orders had a perfectly valid economics hash.
        "selected_target_implementation_hash": (implementation or {}).get(
            "selected_target_implementation_hash"),
    }


def _implementation_for(review_envelope: Optional[dict], target: str) -> Optional[dict]:
    """The frozen, implementable representation of ONE target, from the envelope.

    Read, never built here: ``engine.selected_target`` owns it and
    ``api.proposal_decision_review`` composes it. An envelope that predates R69.2
    carries none, and this returns None rather than fabricating a book.
    """
    blocks = (review_envelope or {}).get("selected_targets") or {}
    got = blocks.get(target)
    return dict(got) if isinstance(got, dict) and got else None


def selection_implementability(*, target: str,
                               implementation: Optional[dict]) -> dict:
    """Can an order plan ever be built for this selection? (verdict, reason).

    It agrees with ``api.rebalance_execution.resolve_target`` BY CONSTRUCTION,
    because the one case it decides without a frozen book is the one that owner
    also decides without one: the FULL TARGET is implementable from the proposal
    artifact's own allocations, which is where it has always come from. Every
    other target needs the book frozen at selection time, and says so when it has
    none rather than letting the approval gate discover it later.
    """
    if implementation is not None:
        return {"implementable": bool(implementation.get("implementable")),
                "reason": implementation.get("not_implementable_reason"),
                "detail": implementation.get("not_implementable_detail"),
                "authority": _st.CALCULATION_OWNER}
    if target == TARGET_FULL_TARGET:
        return {"implementable": True, "reason": None,
                "detail": ("The full target is implemented from the proposal "
                           "artifact's own allocations, exactly as it always has "
                           "been. This selection froze no separate book and needs "
                           "none."),
                "authority": "api.reallocation_proposal (artifact allocations)"}
    if target == TARGET_CURRENT:
        return {"implementable": False, "reason": _st.NOT_IMPLEMENTABLE_NO_CHANGE,
                "detail": ("This target IS the current book. There is nothing to "
                           "buy, nothing to sell and no order plan to build; the "
                           "decision is to keep the book unchanged."),
                "authority": _st.CALCULATION_OWNER}
    return {"implementable": False, "reason": _st.NOT_IMPLEMENTABLE_NO_WEIGHTS,
            "detail": ("The review this selection was made against published no "
                       "implementable book for %s, so its exact weights were never "
                       "frozen and no order plan can be built from it. Re-select "
                       "the target against the current review." % target),
            "authority": _st.CALCULATION_OWNER}


def recorded_selection_implementability(selection: Optional[dict]) -> dict:
    """The same verdict, for a selection READ BACK from the governed ledger.

    A record written before R69.2 carries neither the flag nor the book, so the
    verdict is re-derived from what it does carry - its target - rather than read
    as a missing ``False``. Getting that wrong would refuse to approve a perfectly
    implementable full-target selection recorded last week.
    """
    sel = selection or {}
    if sel.get("implementable") is not None and sel.get("implementation_available"):
        return {"implementable": bool(sel.get("implementable")),
                "reason": sel.get("not_implementable_reason"),
                "detail": sel.get("not_implementable_detail"),
                "authority": sel.get("implementability_authority")
                             or _st.CALCULATION_OWNER}
    return selection_implementability(
        target=sel.get("selected_target"),
        implementation=sel.get("selected_target_implementation") or None)


# --------------------------------------------------------------------------- #
# R69.5 — the risk-policy review a selection carries, and the ruling that clears it
# --------------------------------------------------------------------------- #
def selection_policy_review(selection: Optional[dict]) -> dict:
    """Does the FROZEN book in this selection owe a manual risk-policy ruling?

    Read off the selection, never recomputed from the live review: the operator is
    approving the book they froze, so the question must be asked of THAT book. A
    selection recorded before R69.5 published no verdict at all, and this reports
    that as UNPUBLISHED rather than as a quiet ``False`` — the whole defect R69.1
    documented is a policy effect nobody could see, and inferring "fine" from
    silence would reproduce it one layer down. Re-selecting the same target against
    the current review publishes the block and leaves the frozen book's identity
    hash unchanged, because the hash covers the weights, rows and economics and this
    verdict changes none of them.
    """
    sel = selection or {}
    impl = sel.get("selected_target_implementation") or {}
    has_book = bool(isinstance(impl, dict) and impl)
    review = ((impl.get("risk_contribution") or {}).get("policy_review")
              if has_book else None)
    if not has_book:
        # A selection recorded before R69.2 froze no book at all. R69.2 already
        # rules on that shape - the minimum repair is refused as unimplementable,
        # the full target is implemented from the artifact's own allocations as it
        # always was - and this gate adds no second refusal to it: there is no
        # frozen book here to judge against any limit, and refusing on an absence
        # this release created would break a path R69.2 deliberately preserved.
        # In production such a selection belongs to an earlier session, so the R63
        # session-freshness gate has already made it unapprovable.
        return {"published": False, "required": False, "frozen_book_present": False,
                "reason": None, "state": None, "instruments": [],
                "reference_limit": None, "governed_limit": None,
                "implementation_hash": None,
                "detail": ("This selection froze no book (it predates R69.2), so "
                           "there is no target representation to judge against the "
                           "reference limit. Its approval path is unchanged by "
                           "R69.5."),
                "owner": _st.CALCULATION_OWNER}
    if not isinstance(review, dict) or not review:
        return {"published": False, "required": True, "frozen_book_present": True,
                "reason": ACK_NOT_PUBLISHED,
                "state": None, "instruments": [], "reference_limit": None,
                "governed_limit": None,
                "implementation_hash": sel.get("selected_target_implementation_hash"),
                "detail": (
                    "This selection froze a book before the risk-policy review state "
                    "existed, so whether that book complies with the limit the "
                    "current portfolio was judged against was never published. It is "
                    "not asserted to be compliant and it is not asserted to be in "
                    "breach. Select the same target again against the current review "
                    "— the verdict is then part of the frozen book, and the target's "
                    "identity hash does not move, because that hash covers the "
                    "weights, the rows and the economics and this verdict changes "
                    "none of them."),
                "owner": _st.CALCULATION_OWNER}
    return {"published": True, "required": bool(review.get("required")),
            "frozen_book_present": True,
            "reason": None, "state": review.get("state"),
            "instruments": sorted(review.get("instruments") or []),
            "reference_limit": review.get("reference_limit"),
            "governed_limit": review.get("governed_limit"),
            "implementation_hash": sel.get("selected_target_implementation_hash"),
            "decision_required": review.get("decision_required"),
            "options": list(review.get("options") or []),
            "manual_review_reference": review.get("manual_review_reference"),
            "detail": review.get("decision_required"),
            "owner": review.get("owner") or _st.CALCULATION_OWNER}


def validate_risk_policy_acknowledgement(*, policy_review: dict,
                                         acknowledgement: Optional[dict]) -> dict:
    """Is this ruling a ruling on THIS book, THIS limit and THESE instruments?

    Fail-closed on every axis. A ruling that names a different reference limit, a
    different instrument set or a different frozen book is not a weaker ruling — it
    is a ruling about something else, and accepting it would let a policy decision
    taken on one target authorise another.
    """
    ack = acknowledgement if isinstance(acknowledgement, dict) else None
    if not ack:
        return {"accepted": False, "reason": ACK_MISSING,
                "reason_vocabulary": list(ACK_REASON_VOCAB)}
    if ack.get("token") != RISK_POLICY_ACK_TOKEN:
        return {"accepted": False, "reason": ACK_BAD_TOKEN,
                "reason_vocabulary": list(ACK_REASON_VOCAB),
                "required_token": RISK_POLICY_ACK_TOKEN}
    want_hash = policy_review.get("implementation_hash")
    got_hash = ack.get("selected_target_implementation_hash")
    if want_hash is not None and got_hash != want_hash:
        return {"accepted": False, "reason": ACK_WRONG_BOOK,
                "reason_vocabulary": list(ACK_REASON_VOCAB),
                "expected_selected_target_implementation_hash": want_hash,
                "acknowledged_selected_target_implementation_hash": got_hash}
    want_limit = policy_review.get("reference_limit")
    got_limit = ack.get("reference_limit")
    if want_limit is not None and (
            got_limit is None or abs(float(got_limit) - float(want_limit)) > 1.0e-9):
        return {"accepted": False, "reason": ACK_WRONG_LIMIT,
                "reason_vocabulary": list(ACK_REASON_VOCAB),
                "expected_reference_limit": want_limit,
                "acknowledged_reference_limit": got_limit}
    want_names = sorted(policy_review.get("instruments") or [])
    got_names = sorted(ack.get("instruments") or [])
    if got_names != want_names:
        return {"accepted": False, "reason": ACK_WRONG_INSTRUMENTS,
                "reason_vocabulary": list(ACK_REASON_VOCAB),
                "expected_instruments": want_names,
                "acknowledged_instruments": got_names}
    return {"accepted": True, "reason": None,
            "reason_vocabulary": list(ACK_REASON_VOCAB),
            "token": RISK_POLICY_ACK_TOKEN,
            "reference_limit": want_limit, "instruments": want_names,
            "selected_target_implementation_hash": want_hash,
            "ruling": ack.get("ruling"),
            "ruled_by": ack.get("ruled_by"),
            "changes_the_declared_policy": False,
            "grants_a_standing_exception": False}


def _target_hash(option: Optional[dict]) -> Optional[str]:
    """A stable identity for ONE selected target's economically meaningful facts."""
    if not option:
        return None
    try:
        return _cr.stable_hash({
            "target": option.get("target"),
            "positions": option.get("positions"),
            "changes": option.get("changes"),
            "one_way_turnover": option.get("one_way_turnover"),
            "estimated_cost": option.get("estimated_cost"),
            "score": option.get("score"),
            "cash_weight": option.get("cash_weight"),
            "concentration": option.get("concentration"),
            "obligations_remaining": option.get("mandatory_obligations_remaining"),
        })
    except Exception:  # noqa: BLE001
        return None


def record_target_selection(*, target: str, confirm: Optional[str],
                            review_envelope: Optional[dict] = None,
                            expected_proposal_hash: Optional[str] = None,
                            expected_review_hash: Optional[str] = None,
                            expected_hoc_assessment_hash: Optional[str] = None,
                            actor: Optional[str] = None,
                            decision_dir=None,
                            latest_session: Optional[str] = None,
                            workflow_state: Optional[dict] = None,
                            enforce_session_freshness: bool = True,
                            now: Optional[datetime] = None) -> dict:
    """Record WHICH reviewed target the operator wants to take to the Approve gate.

    Selection is NOT approval. It creates no order plan, no order and no fill, and
    it moves no capital. It is idempotent: selecting the same target against the
    same identities returns the same governed outcome and writes no second
    artifact. A CONFLICTING selection is an explicit revision - a new immutable
    record, pointer advanced, history preserved.

    Fails closed on every identity that could have moved underneath the operator
    (proposal, review, opportunity-cost assessment) and on session freshness.
    """
    ts = _now_iso(now)
    base = {"owner": OWNER, "phase": "R63", "recorded": False, "reused": False,
            "revised": False, "selected": False, "evaluated_at": ts,
            "target_vocabulary": list(TARGET_VOCAB),
            "status_vocabulary": list(SELECTION_STATUS_VOCAB),
            "is_an_approval": False, "approves_proposal": False,
            "creates_order_plan": False, "creates_orders": False,
            "creates_fills": False, "executes": False, "deploys_capital": False,
            "mutates_proposal": False, "decided_by_llm": False,
            "manual_approval_still_required": True}

    if confirm != SELECTION_CONFIRM_TOKEN:
        return {**base, "status": TS_NOT_RECORDED,
                "message": ("Target selection requires the explicit confirmation "
                            "token %s." % SELECTION_CONFIRM_TOKEN)}
    if target not in TARGET_VOCAB:
        return {**base, "status": TS_NOT_RECORDED,
                "message": ("Unknown target %r. One of %s is required."
                            % (target, ", ".join(TARGET_VOCAB)))}

    env = review_envelope or {}
    rev = env.get("review") or {}
    # R69.1 - read the RECONCILED option list, not the kernel's raw one.
    #
    # The review envelope carries the same three options twice. The kernel copy
    # under ``review.target_selection`` holds no clock: it rules on obligations
    # and economics, so it reports ``selectable: True`` even for a proposal bound
    # to a session the workflow has long moved past. The freshness-reconciled copy
    # is published at the top level (and under ``governance``) and is the one the
    # browser renders.
    #
    # This gate read the kernel copy, so "backend-decided selectability" was
    # decided by the one copy that cannot see the session. Nothing unsafe reached
    # a write - the session-freshness guard below runs FIRST and fails closed -
    # but a guard that disagrees with the surface it guards is a defect waiting
    # for the day the order changes. It now reads what the operator was shown.
    selection_block = (env.get("target_selection")
                       or (env.get("governance") or {}).get("target_selection")
                       or rev.get("target_selection") or {})
    options = {o.get("target"): o for o in (selection_block.get("options") or [])}
    if not options:
        return {**base, "status": TS_NO_REVIEW,
                "message": ("No proposal decision review is available, so there is "
                            "nothing to select. Run the portfolio cycle first.")}

    # R69.2 - the complete book this target describes, exactly as the review the
    # operator was reading published it. It is FROZEN into the record below, so the
    # order-plan owner later implements the weights that were on screen rather than
    # re-deriving a target from a projection that has since moved.
    implementation = _implementation_for(env, target)
    implementability = selection_implementability(target=target,
                                                  implementation=implementation)
    binding = _selection_binding(review_envelope=env, option=options.get(target),
                                 target=target, implementation=implementation)

    # --- session freshness: a persisted proposal is not actionable forever ----- #
    freshness = decision_freshness(
        bound_session=binding.get("eligible_market_date"),
        latest_session=(latest_session if latest_session is not None
                        else (latest_eligible_session(workflow_state=workflow_state)
                              if enforce_session_freshness else
                              binding.get("eligible_market_date"))))
    if enforce_session_freshness and not freshness["target_selection_allowed"]:
        return {**base, "status": TS_SESSION_STALE, "freshness": freshness,
                "binding": binding,
                "next_required_action": freshness["next_required_action"],
                "message": freshness["detail"]}

    # --- stale-identity guards: fail closed, write nothing --------------------- #
    for label, expected, actual, code in (
            ("proposal", expected_proposal_hash, binding.get("proposal_hash"),
             "PROPOSAL_HASH_MISMATCH"),
            ("review", expected_review_hash, binding.get("review_hash"),
             "REVIEW_HASH_MISMATCH"),
            ("opportunity-cost assessment", expected_hoc_assessment_hash,
             binding.get("hoc_assessment_hash"), "HOC_ASSESSMENT_HASH_MISMATCH")):
        if expected is not None and expected != actual:
            return {**base, "status": TS_STALE, "binding": binding,
                    "reason_code": code, "expected": expected, "actual": actual,
                    "freshness": freshness,
                    "message": ("The %s changed since it was reviewed, so this "
                                "selection would bind evidence the operator never "
                                "saw. Re-review before selecting." % label)}

    option = options.get(target) or {}
    if not option.get("selectable"):
        return {**base, "status": TS_NOT_SELECTABLE, "binding": binding,
                "target": target, "freshness": freshness,
                "blockers": list(option.get("blockers") or []),
                "blocker_codes": list(option.get("blocker_codes") or []),
                "message": ("%s cannot be selected: %s"
                            % (target,
                               "; ".join(b.get("detail") or b.get("code") or ""
                                         for b in (option.get("blockers") or []))
                               or "the backend marked it not selectable."))}

    existing = load_target_selection(
        active_book_id=binding.get("active_book_id"),
        eligible_market_date=binding.get("eligible_market_date"),
        decision_dir=decision_dir)

    # Idempotent: the same target against the same identities is the same governed
    # outcome. No duplicate artifact is written.
    #
    # R69.2 - with ONE exception, and it is not a loophole. A selection recorded
    # before this release froze the target's economics but not its weights, so it
    # carries no implementable book and no order plan can ever be built from it.
    # Re-selecting the same target against the same evidence, now that the book
    # IS available, is a genuine governed revision rather than a replay: the
    # record gains the weights it should always have carried, and the superseded
    # one is preserved exactly as every other revision is. The reverse never
    # happens - an implementation is never dropped from a record that has one.
    _has_impl = bool((existing or {}).get("selected_target_implementation"))
    _gains_impl = bool(implementation) and not _has_impl
    if existing and existing.get("selected_target") == target \
            and (existing.get("binding") or {}).get("proposal_hash") == binding.get("proposal_hash") \
            and (existing.get("binding") or {}).get("review_hash") == binding.get("review_hash") \
            and not _gains_impl:
        prior = recorded_selection_implementability(existing)
        return {**base, "status": TS_REUSED, "recorded": True, "reused": True,
                "selected": True, "record": existing, "binding": binding,
                "target": target, "freshness": freshness,
                "implementable": prior["implementable"],
                "not_implementable_reason": prior["reason"],
                "not_implementable_detail": prior["detail"],
                "implementation_available": _has_impl}

    revised = bool(existing)
    selection_id = "psel_%s_%s_%s_%s" % (
        binding.get("eligible_market_date") or "nodate",
        binding.get("active_book_id") or "book",
        target.lower(),
        (binding.get("proposal_hash") or "")[:12])
    if revised:
        selection_id += "_r%d" % (int((existing or {}).get("revision", 0)) + 1)

    record = {
        "selection_id": selection_id,
        "owner": OWNER,
        "phase": "R63",
        "artifact_kind": "proposal_review_selection",
        "artifact_doc": ("A GOVERNANCE artifact. It references the immutable "
                         "proposal and the review that adjudicated it; it "
                         "replaces neither and computes no target of its own."),
        "selected_target": target,
        "selected_target_label": option.get("label"),
        "selected_at": ts,
        "actor": actor or "operator",
        "revision": (int((existing or {}).get("revision", 0)) + 1) if revised else 0,
        "supersedes_selection_id": (existing or {}).get("selection_id") if revised else None,
        "binding": binding,
        # The economics the operator was shown AT selection time, frozen with the
        # choice so the approval gate can prove what was agreed to.
        "selected_target_economics": {
            k: option.get(k) for k in (
                "positions", "changes", "one_way_turnover", "estimated_cost",
                "score", "score_improvement_net_of_cost", "portfolio_volatility",
                "portfolio_volatility_capital_basis", "concentration",
                "largest_position", "cash_weight",
                "mandatory_obligations_remaining")},
        # R69.2 — THE SELECTED TARGET ITSELF, frozen with the choice.
        #
        # Until this release a selection froze the target's ECONOMICS and nothing
        # else. The weights existed only inside the review projection, which is
        # recomputed on every read and persisted nowhere, so the approval path had
        # no target to consume and the order-plan owner fell back to the one list
        # it did hold: the artifact's full target. The operator chose 14 names and
        # 21% turnover and was handed 20 names and 35%.
        #
        # The complete book now travels with the selection: every weight, every
        # allocation row, every economic and the before/after risk-contribution
        # comparison, all read verbatim from the review that was on screen. This
        # record is immutable and append-only; a different choice is a new record.
        "selected_target_implementation": implementation,
        "selected_target_implementation_hash": (implementation or {}).get(
            "selected_target_implementation_hash"),
        "implementable": bool(implementability["implementable"]),
        "not_implementable_reason": implementability["reason"],
        "not_implementable_detail": implementability["detail"],
        "implementability_authority": implementability["authority"],
        "implementation_available": implementation is not None,
        "mandatory_obligations_remaining": list(option.get("obligations_remaining") or []),
        "review_verdict": binding.get("review_verdict"),
        "review_recommended_target": selection_block.get("recommended_target"),
        "followed_recommendation": bool(
            selection_block.get("recommended_target") == target),
        "is_defer": bool(option.get("is_defer")),
        "freshness": freshness,
        "confirm_token": SELECTION_CONFIRM_TOKEN,
        "is_an_approval": False,
        "creates_order_plan": False,
        "creates_orders": False,
    }

    rows = _load_json(_selections_path(decision_dir)) or []
    if not isinstance(rows, list):
        rows = []
    rows.append(record)
    _atomic_write_json(_selections_path(decision_dir), rows)
    index = _load_json(_selection_index_path(decision_dir)) or {}
    index[_index_key(binding.get("active_book_id"),
                     binding.get("eligible_market_date"))] = {
        "selection_id": selection_id, "selected_target": target,
        "proposal_hash": binding.get("proposal_hash"),
        "review_hash": binding.get("review_hash"),
        "selected_target_implementation_hash": binding.get(
            "selected_target_implementation_hash"),
        "implementable": bool(record.get("implementable")),
        "selected_at": ts, "record": record}
    _atomic_write_json(_selection_index_path(decision_dir), index)
    return {**base, "status": (TS_REVISED if revised else TS_CREATED),
            "recorded": True, "selected": True, "revised": revised,
            "record": record, "binding": binding, "target": target,
            "freshness": freshness,
            # R69.2 - the surface learns whether the thing it just recorded can
            # actually be implemented WITHOUT loading the frozen book.
            "implementable": bool(record.get("implementable")),
            "not_implementable_reason": record.get("not_implementable_reason"),
            "not_implementable_detail": record.get("not_implementable_detail"),
            "implementation_available": implementation is not None,
            "selected_target_implementation_hash": binding.get(
                "selected_target_implementation_hash"),
            "position_count": (implementation or {}).get("position_count"),
            "selected_target_owner": _st.CALCULATION_OWNER}


# --------------------------------------------------------------------------- #
# R82 — THE GOVERNED RISK-POLICY RULING
#
# R69.5 withheld approval at SELECTED_TARGET_REQUIRES_RISK_POLICY_REVIEW and left
# one door through it: a per-request acknowledgement bound to the frozen book,
# which on acceptance let the approval through. Two consequences followed.
#
#   1. Only ACCEPT_AS_IS was reachable. The second course R69.1 put to the operator
#      — judge the cap against the universe the breach was RAISED against — had no
#      recordable form, because the only outcome available unblocks the very
#      approval that ruling refuses.
#   2. No ruling was DURABLE. The ack lived inside the approval request, so a book
#      nobody approved carried no ruling at all and the question re-opened on every
#      read.
#
# This lane records the ruling itself: an immutable, append-only governed artifact
# bound to ONE proposal, ONE selection and ONE frozen book, written through the
# same store root, the same atomic writer and the same revision idiom as every
# other governed record here. It is NOT an approval and it is NOT a second
# decision authority — ``record_decision`` remains the only writer of a portfolio
# decision, and this lane can only ever make an approval LESS available:
# ACCEPT_AS_IS unblocks nothing, and the R69.5 acknowledgement gate stands exactly
# as it stood.
# --------------------------------------------------------------------------- #
#: Re-exported from the kernel that defines them. One vocabulary, one owner.
RULING_VOCAB = _st.RULING_VOCAB
RULING_AVAILABLE = _st.RULING_AVAILABLE
RULING_ACCEPT_AS_IS = _st.RULING_ACCEPT_AS_IS
RULING_JUDGE_AGAINST_THE_BEFORE_UNIVERSE = _st.RULING_JUDGE_AGAINST_THE_BEFORE_UNIVERSE
RULING_ADD_AN_ABSOLUTE_COMPANION_FLOOR = _st.RULING_ADD_AN_ABSOLUTE_COMPANION_FLOOR
#: Distinct from the approval token AND from the selection token, so no one of the
#: three can ever be replayed as another.
RULING_CONFIRM_TOKEN = "CONFIRM_RISK_POLICY_RULING"

RULING_RECORDED = "RULING_RECORDED"
#: R82.1 — written, but with NO governed effect, because its provenance did not
#: verify. Never collapsed into RULING_RECORDED: a caller that cannot tell the two
#: apart would report an unverified artifact as a recorded operator decision.
RULING_RECORDED_UNVERIFIED = "RULING_RECORDED_WITHOUT_VERIFIED_OPERATOR_PROVENANCE"
RULING_REUSED = "RULING_REUSED_EXISTING"
RULING_REVISED = "RULING_REVISED"
RULING_NOT_RECORDED = "RULING_NOT_RECORDED"
RULING_NOT_REQUIRED = "NO_RISK_POLICY_REVIEW_TO_RULE_ON"
RULING_NOT_AVAILABLE = "RULING_NOT_AVAILABLE_IN_THIS_RELEASE"
RULING_UNKNOWN = "RULING_NOT_IN_VOCABULARY"
RULING_NO_SELECTION = "NO_GOVERNED_SELECTION_TO_RULE_ON"
RULING_WRONG_BOOK = "RULING_BOUND_TO_A_DIFFERENT_BOOK"
RULING_WRONG_LIMIT = "RULING_REFERENCE_LIMIT_MISMATCH"
RULING_WRONG_INSTRUMENTS = "RULING_INSTRUMENTS_MISMATCH"
RULING_WRONG_SELECTION = "RULING_BOUND_TO_A_DIFFERENT_SELECTION"
RULING_NOT_PUBLISHED = ACK_NOT_PUBLISHED
RULING_STATUS_VOCAB = (
    RULING_RECORDED, RULING_RECORDED_UNVERIFIED, RULING_REUSED, RULING_REVISED,
    RULING_NOT_RECORDED,
    RULING_NOT_REQUIRED, RULING_NOT_AVAILABLE, RULING_UNKNOWN,
    RULING_NO_SELECTION, RULING_WRONG_BOOK, RULING_WRONG_LIMIT,
    RULING_WRONG_INSTRUMENTS, RULING_WRONG_SELECTION, RULING_NOT_PUBLISHED)

_RULINGS_FILE = "risk_policy_rulings.json"
_RULING_INDEX_FILE = "risk_policy_ruling_index.json"

# --------------------------------------------------------------------------- #
# R82.1 — RULING PROVENANCE: who actually made this ruling?
#
# R82 shipped a governed ruling store whose ``ruled_by`` is a free string the
# CALLER supplies. A record written by a script with ``actor="operator"`` is then
# byte-indistinguishable from one a human confirmed on a screen, and the store
# cannot tell an authoritative human ruling from a development artifact. R82's own
# live record is exactly that: it was written by a direct API call during
# implementation and it says ``ruled_by: operator``.
#
# The correction is general and special-cases no record. A ruling now carries a
# PROVENANCE block, the writer DERIVES the channel from evidence rather than
# accepting the caller's word for it, and a ruling whose provenance is not verified
# has NO governed effect: it is readable audit evidence, it binds nothing, and
# approval stays withheld exactly as it would with no ruling at all. History is
# never rewritten — a later verified ruling supersedes an unverified one through
# the ordinary revision chain.
#
# WHAT THE VERIFIED CHANNEL PROVES, precisely, and nothing more: the submission
# came from a client that had loaded THIS backend process's governed review panel
# for THIS exact frozen book, echoed the single-use token that read minted, and
# asserted the operator confirmation. No server-side check at this layer can prove
# a human moved a mouse, and the record does not claim it does. What it does prove
# is what the defect needed: a ruling can no longer be conjured by calling the
# writer directly, because the token is never derivable from the store, the
# vocabulary or the request — only from a governed read.
# --------------------------------------------------------------------------- #
#: Derived, never asserted by the caller.
RULING_CHANNEL_OPERATOR_UI = "OPERATOR_UI_CONFIRMED"
RULING_CHANNEL_API_DIRECT = "API_DIRECT_CALL"
RULING_CHANNEL_ABSENT = "PROVENANCE_NOT_RECORDED"
RULING_CHANNEL_VOCAB = (RULING_CHANNEL_OPERATOR_UI, RULING_CHANNEL_API_DIRECT,
                        RULING_CHANNEL_ABSENT)
#: The ONE channel that carries governed authority.
RULING_CHANNEL_TRUSTED = (RULING_CHANNEL_OPERATOR_UI,)
#: What ``ruled_by`` says when the caller named nobody. R82 defaulted it to
#: "operator", which is how a development call came to be labelled as an operator's
#: decision. Narration now defaults to silence.
RULING_ACTOR_UNATTRIBUTED = "ACTOR_NOT_SUPPLIED"

#: What a ruling may be USED for. A record is always readable; only an
#: authoritative one governs anything.
RULING_USE_AUTHORITATIVE = "AUTHORITATIVE_OPERATOR_RULING"
RULING_USE_UNVERIFIED = "UNVERIFIED_REQUIRES_OPERATOR_CONFIRMATION"
RULING_USE_VOCAB = (RULING_USE_AUTHORITATIVE, RULING_USE_UNVERIFIED)

#: Why a provenance did not verify. Structured, never free text.
PROV_OK = None
PROV_MISSING = "NO_PROVENANCE_BLOCK_ON_THE_RECORD"
PROV_NO_TOKEN = "NO_UI_SUBMISSION_TOKEN_PRESENTED"
PROV_TOKEN_MISMATCH = "UI_SUBMISSION_TOKEN_DOES_NOT_MATCH_THIS_FROZEN_BOOK"
#: R82.1.1 - the ceremony verdicts. Each names a DIFFERENT failure, because
#: "unknown", "already spent" and "bound to another book" are not the same event and
#: an operator debugging a refused ruling needs to be told which one happened.
PROV_NO_CONFIRMATION = "NO_GOVERNED_OPERATOR_CONFIRMATION_PRESENTED"
PROV_CONFIRMATION_UNKNOWN = "OPERATOR_CONFIRMATION_WAS_NEVER_ISSUED_BY_THIS_BACKEND"
PROV_CONFIRMATION_REPLAYED = "OPERATOR_CONFIRMATION_ALREADY_CONSUMED"
PROV_CONFIRMATION_EXPIRED = "OPERATOR_CONFIRMATION_EXPIRED"
PROV_CONFIRMATION_WRONG_BOOK = (
    "OPERATOR_CONFIRMATION_BOUND_TO_A_DIFFERENT_FROZEN_BOOK")
PROV_CONFIRMATION_WRONG_RULING = "OPERATOR_CONFIRMATION_BOUND_TO_A_DIFFERENT_RULING"
PROV_REASON_VOCAB = (PROV_MISSING, PROV_NO_TOKEN, PROV_TOKEN_MISMATCH,
                     PROV_NO_CONFIRMATION, PROV_CONFIRMATION_UNKNOWN,
                     PROV_CONFIRMATION_REPLAYED, PROV_CONFIRMATION_EXPIRED,
                     PROV_CONFIRMATION_WRONG_BOOK, PROV_CONFIRMATION_WRONG_RULING)

#: A per-PROCESS secret. It is minted at import and never persisted, so a
#: submission token cannot be forged from the store, from the repository or from a
#: previous run - only obtained from a governed read of this live process.
_UI_SUBMISSION_SECRET = secrets.token_hex(32)

# --------------------------------------------------------------------------- #
# R82.1.1 - THE OPERATOR CONFIRMATION CEREMONY
#
# R82.1 derived the channel from a book-bound submission token plus a
# ``confirmed_in_ui`` boolean. The token is a PURE FUNCTION of the frozen identity,
# so any in-process caller could mint one by calling ``ruling_submission_token``,
# and the boolean is a caller's claim. Token + claim was therefore not evidence of a
# confirmation ceremony: it was evidence of knowing the book.
#
# The confirmation is now an ARTIFACT THIS PROCESS ISSUES and NOBODY CAN DERIVE. It
# is minted only by ``open_ruling_confirmation``, lives only in this process's
# memory, is never persisted, never returned by a read, and is spent the first time
# it is presented. The ledger - not a boolean, not a surface string, not an actor
# name - is what says a governed confirmation happened.
#
# WHAT THE VERIFIED CHANNEL PROVES, precisely, and nothing more: the ruling
# submission consumed a single-use confirmation that THIS backend process issued,
# for THIS exact frozen book and THIS exact ruling, in a ceremony that required the
# book token a governed read had minted and the typed confirmation phrase, and that
# had not expired and had never been spent. It does NOT prove a human moved a mouse,
# and it does not try to: a client that performs every act of the ceremony is
# treated as the operator, because that is what the ceremony IS. What it does prove
# is exactly what the defect needed - a ruling can no longer be made authoritative
# by obtaining the served token and asserting a boolean, because the confirmation is
# not derivable from the store, the repository, the vocabulary, the request, or any
# function a caller can call without performing the ceremony itself.
# --------------------------------------------------------------------------- #
#: Short-lived on purpose: a confirmation is opened by the act of ruling and spent
#: moments later. A window long enough to leave lying around is a window long enough
#: to be reused by something other than the act that opened it.
RULING_CONFIRMATION_TTL_SECONDS = 180
#: The ledger is bounded. A caller that opens ceremonies it never spends cannot grow
#: this process's memory without limit; the oldest unspent entries fall out first.
_RULING_CONFIRMATION_MAX = 64
#: PROCESS-LOCAL and never persisted. A restart invalidates every open ceremony,
#: which is the fail-closed answer: evidence that outlived the process that issued
#: it would be evidence of nothing.
_RULING_CONFIRMATIONS: dict = {}
_RULING_CONFIRMATION_LOCK = threading.Lock()

#: Why a ceremony was refused before any confirmation was issued.
CEREMONY_OK = None
CEREMONY_NO_BOOK = "NO_FROZEN_BOOK_TO_CONFIRM_AGAINST"
CEREMONY_NO_PHRASE = "CONFIRMATION_PHRASE_NOT_PRESENTED"
CEREMONY_RULING_UNKNOWN = "RULING_NOT_IN_VOCABULARY"
CEREMONY_RULING_UNAVAILABLE = "RULING_NOT_AVAILABLE_IN_THIS_RELEASE"
CEREMONY_REFUSAL_VOCAB = (CEREMONY_NO_BOOK, CEREMONY_NO_PHRASE,
                          PROV_NO_TOKEN, PROV_TOKEN_MISMATCH,
                          CEREMONY_RULING_UNKNOWN, CEREMONY_RULING_UNAVAILABLE)
#: Where the ceremony is performed. Published beside the ruling route so a screen
#: never has to hold a copy of either path.
RULING_CONFIRMATION_ROUTE = (
    "/v1/operations/portfolio-decision/risk-policy-ruling/confirmation")


def ruling_submission_token(*, selection_id: Optional[str],
                            selected_target_implementation_hash: Optional[str],
                            reference_limit: Optional[float]) -> Optional[str]:
    """The book-bound token a governed READ mints, and what it is NOT.

    It proves that its holder read THIS live process's governed review of THIS exact
    frozen book: it is salted with a per-process secret, so it cannot be derived
    from the store, the repository or a previous run.

    It is NOT, on its own, evidence of an operator decision - R82.1 treated it as
    half of one, and that was the defect. It is a PRECONDITION of opening the
    confirmation ceremony (``open_ruling_confirmation``) and nothing more. Returns
    None when there is no frozen book to bind, in which case no ruling is
    submittable and the write surface offers none.
    """
    if not selection_id or not selected_target_implementation_hash:
        return None
    payload = "|".join((
        _UI_SUBMISSION_SECRET, "R82.1-RULING",
        str(selection_id), str(selected_target_implementation_hash),
        ("" if reference_limit is None else repr(round(float(reference_limit), 10))),
    ))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:40]


def _confirmation_key(confirmation: Optional[str]) -> Optional[str]:
    """Ledger entries are keyed by the HASH of the confirmation, never by it.

    Nothing that can be presented as evidence is held in the structure a diagnostic
    dump, a traceback or a repr would print.
    """
    if not isinstance(confirmation, str) or not confirmation:
        return None
    return hashlib.sha256(confirmation.encode("utf-8")).hexdigest()


def _same_reference_limit(a: Optional[float], b: Optional[float]) -> bool:
    if a is None or b is None:
        return a is None and b is None
    try:
        return abs(float(a) - float(b)) <= 1.0e-9
    except (TypeError, ValueError):
        return False


def _sweep_ruling_confirmations(mono: float) -> None:
    """Drop everything past its window. The caller holds the lock."""
    dead = [k for k, v in _RULING_CONFIRMATIONS.items()
            if mono >= v.get("expires_mono", 0.0)]
    for k in dead:
        _RULING_CONFIRMATIONS.pop(k, None)
    while len(_RULING_CONFIRMATIONS) > _RULING_CONFIRMATION_MAX:
        oldest = min(_RULING_CONFIRMATIONS.items(),
                     key=lambda kv: kv[1].get("issued_mono", 0.0))[0]
        _RULING_CONFIRMATIONS.pop(oldest, None)


def open_ruling_confirmation(*, ruling: Optional[str],
                             confirm: Optional[str],
                             submission_token: Optional[str],
                             selection_id: Optional[str],
                             selected_target_implementation_hash: Optional[str],
                             reference_limit: Optional[float],
                             surface: Optional[str] = None,
                             actor: Optional[str] = None,
                             now: Optional[datetime] = None) -> dict:
    """ACT ONE of the governed ceremony: issue ONE single-use operator confirmation.

    This is the ONLY place a confirmation comes into existence. It is minted here,
    held in this process's memory, and returned to the caller exactly once - it is
    never stored, never re-derivable and never published by any read.

    The ceremony refuses unless the caller presents, together:

      * the book token a governed READ of this live process minted for this exact
        frozen book - so a caller that never read the governed review cannot open a
        ceremony about it;
      * the typed confirmation phrase - the same explicit act the write demands; and
      * ONE ruling from the available vocabulary - so the confirmation is bound to
        the CHOICE, and cannot be carried to a different one.

    It records nothing durable, approves nothing and rules on nothing: opening a
    ceremony is not a decision, and abandoning one leaves no artifact behind.
    """
    ts = _now_iso(now)
    base = {"owner": OWNER, "phase": "R82.1.1", "issued": False, "refused": True,
            "confirmation": None, "confirmation_id": None,
            "ttl_seconds": RULING_CONFIRMATION_TTL_SECONDS,
            "confirm_token": RULING_CONFIRM_TOKEN,
            "route": RULING_ROUTE,
            "confirmation_route": RULING_CONFIRMATION_ROUTE,
            "refusal_vocabulary": list(CEREMONY_REFUSAL_VOCAB),
            "issued_at": ts,
            "is_an_approval": False, "approves_proposal": False,
            "records_a_ruling": False, "records_a_portfolio_decision": False,
            "creates_order_plan": False, "creates_orders": False,
            "creates_fills": False, "executes": False, "deploys_capital": False,
            "changes_declared_policy": False,
            "manual_approval_still_required": True}

    want_token = ruling_submission_token(
        selection_id=selection_id,
        selected_target_implementation_hash=selected_target_implementation_hash,
        reference_limit=reference_limit)
    if not want_token:
        return {**base, "reason": CEREMONY_NO_BOOK,
                "message": ("There is no frozen book to confirm a ruling against, "
                            "so no operator confirmation was issued.")}
    if confirm != RULING_CONFIRM_TOKEN:
        return {**base, "reason": CEREMONY_NO_PHRASE,
                "message": ("The governed confirmation requires the explicit "
                            "phrase %s." % RULING_CONFIRM_TOKEN)}
    if not submission_token:
        return {**base, "reason": PROV_NO_TOKEN,
                "message": ("A confirmation is issued only to a client that has "
                            "read this backend's governed review of this frozen "
                            "book and echoes the token that read minted.")}
    if not secrets.compare_digest(str(submission_token), want_token):
        return {**base, "reason": PROV_TOKEN_MISMATCH,
                "message": ("The submission token names a different frozen book "
                            "than the one this confirmation would bind. Re-read the "
                            "governed review and confirm again.")}
    effect = _st.ruling_effect(ruling)
    if not effect["known"]:
        return {**base, "reason": CEREMONY_RULING_UNKNOWN, "ruling": ruling,
                "message": effect["detail"]}
    if not effect["available"]:
        return {**base, "reason": CEREMONY_RULING_UNAVAILABLE, "ruling": ruling,
                "message": effect["detail"]}

    confirmation = secrets.token_urlsafe(32)
    confirmation_id = "rconf_%s" % secrets.token_hex(8)
    mono = time.monotonic()
    entry = {
        "confirmation_id": confirmation_id,
        "ruling": ruling,
        "selection_id": selection_id,
        "selected_target_implementation_hash": selected_target_implementation_hash,
        "reference_limit": reference_limit,
        "issued_at": ts,
        "issued_mono": mono,
        "expires_mono": mono + float(RULING_CONFIRMATION_TTL_SECONDS),
        "consumed": False,
        "consumed_at": None,
        "consumed_outcome": None,
        "surface": surface,
        "actor": actor,
    }
    with _RULING_CONFIRMATION_LOCK:
        _sweep_ruling_confirmations(mono)
        _RULING_CONFIRMATIONS[_confirmation_key(confirmation)] = entry
    return {**base, "issued": True, "refused": False, "reason": CEREMONY_OK,
            "confirmation": confirmation,
            "confirmation_id": confirmation_id,
            "ruling": ruling,
            "expires_in_seconds": RULING_CONFIRMATION_TTL_SECONDS,
            "single_use": True,
            "issuer": OWNER,
            "binds": {
                "ruling": ruling,
                "selection_id": selection_id,
                "selected_target_implementation_hash": (
                    selected_target_implementation_hash),
                "reference_limit": reference_limit,
            },
            "message": ("A single-use operator confirmation was issued for this "
                        "ruling on this frozen book. It is spent by the ruling "
                        "submission, expires in %ds, and confirms nothing on its "
                        "own." % RULING_CONFIRMATION_TTL_SECONDS)}


def consume_ruling_confirmation(*, confirmation: Optional[str],
                                ruling: Optional[str],
                                selection_id: Optional[str],
                                selected_target_implementation_hash: Optional[str],
                                reference_limit: Optional[float],
                                now: Optional[datetime] = None) -> dict:
    """ACT TWO: SPEND the confirmation, and say what spending it proved.

    The entry is marked consumed the moment it is presented, BEFORE its bindings are
    judged. That ordering is deliberate: a confirmation presented against the wrong
    book or the wrong ruling is spent by that attempt and cannot be re-aimed at the
    right one. A single-use credential that survives a failed use is not single-use.

    Pure with respect to the durable store - it reads and writes nothing on disk.
    """
    ts = _now_iso(now)
    out = {"verified": False, "reason": PROV_NO_CONFIRMATION,
           "presented": bool(confirmation), "consumed": False,
           "confirmation_id": None, "issued_at": None, "consumed_at": None,
           "ttl_seconds": RULING_CONFIRMATION_TTL_SECONDS,
           "issuer": OWNER, "single_use": True,
           "reason_vocabulary": list(PROV_REASON_VOCAB)}
    key = _confirmation_key(confirmation)
    if not key:
        return out
    mono = time.monotonic()
    with _RULING_CONFIRMATION_LOCK:
        # Look up BEFORE sweeping, so an entry that is merely past its window is
        # refused as EXPIRED rather than as UNKNOWN. The two are different events and
        # an operator whose confirmation timed out is owed the one that happened.
        entry = _RULING_CONFIRMATIONS.get(key)
        if entry is None:
            _sweep_ruling_confirmations(mono)
            # Either never issued, or issued and already spent long enough ago to
            # have fallen out of the window. Both are refusals; neither is a hint.
            return {**out, "reason": PROV_CONFIRMATION_UNKNOWN}
        if entry.get("consumed"):
            return {**out, "reason": PROV_CONFIRMATION_REPLAYED,
                    "confirmation_id": entry.get("confirmation_id"),
                    "issued_at": entry.get("issued_at"),
                    "consumed_at": entry.get("consumed_at"),
                    "first_use_outcome": entry.get("consumed_outcome")}
        if mono >= entry.get("expires_mono", 0.0):
            entry["consumed"] = True
            entry["consumed_at"] = ts
            entry["consumed_outcome"] = PROV_CONFIRMATION_EXPIRED
            _sweep_ruling_confirmations(mono)
            return {**out, "reason": PROV_CONFIRMATION_EXPIRED, "consumed": True,
                    "confirmation_id": entry.get("confirmation_id"),
                    "issued_at": entry.get("issued_at"), "consumed_at": ts}
        # Spend it first, judge it second.
        entry["consumed"] = True
        entry["consumed_at"] = ts
        same_book = (
            entry.get("selection_id") == selection_id
            and entry.get("selected_target_implementation_hash")
            == selected_target_implementation_hash
            and _same_reference_limit(entry.get("reference_limit"),
                                      reference_limit))
        if not same_book:
            reason = PROV_CONFIRMATION_WRONG_BOOK
        elif entry.get("ruling") != ruling:
            reason = PROV_CONFIRMATION_WRONG_RULING
        else:
            reason = PROV_OK
        entry["consumed_outcome"] = reason
        return {**out, "verified": reason is PROV_OK, "reason": reason,
                "consumed": True,
                "confirmation_id": entry.get("confirmation_id"),
                "issued_at": entry.get("issued_at"), "consumed_at": ts,
                "bound_ruling": entry.get("ruling"),
                "bound_selection_id": entry.get("selection_id"),
                "bound_selected_target_implementation_hash": entry.get(
                    "selected_target_implementation_hash"),
                "bound_reference_limit": entry.get("reference_limit"),
                "ceremony_surface": entry.get("surface"),
                "ceremony_actor": entry.get("actor")}


def ruling_provenance(*, submission_token: Optional[str] = None,
                      confirmed_in_ui: Optional[bool] = None,
                      consumption: Optional[dict] = None,
                      ruling: Optional[str] = None,
                      selection_id: Optional[str] = None,
                      selected_target_implementation_hash: Optional[str] = None,
                      reference_limit: Optional[float] = None,
                      surface: Optional[str] = None,
                      actor: Optional[str] = None) -> dict:
    """DERIVE the provenance of one ruling submission from its evidence.

    PURE. It spends nothing: ``consumption`` is the verdict
    ``consume_ruling_confirmation`` already returned, and a caller that presents no
    consumed confirmation gets an UNVERIFIED provenance by construction. That is why
    every argument defaults to None - the absence of evidence is the ordinary case,
    and it has to fail closed without anyone remembering to make it.

    The channel is decided here and is never read from the request. ``actor``,
    ``surface`` and ``confirmed_in_ui`` are recorded as what the caller CLAIMED and
    weigh nothing: a caller that could name its own channel - or be believed about
    the screen it was sitting at - would reproduce the very defect this closes.
    """
    want = ruling_submission_token(
        selection_id=selection_id,
        selected_target_implementation_hash=selected_target_implementation_hash,
        reference_limit=reference_limit)
    token_bound = bool(submission_token and want
                       and secrets.compare_digest(str(submission_token), want))
    cons = consumption if isinstance(consumption, dict) else {}
    ceremony_ok = cons.get("verified") is True
    if not ceremony_ok:
        reason = cons.get("reason") or PROV_NO_CONFIRMATION
    elif not submission_token:
        reason = PROV_NO_TOKEN
    elif not token_bound:
        reason = PROV_TOKEN_MISMATCH
    else:
        reason = PROV_OK
    verified = reason is PROV_OK
    return {
        "channel": (RULING_CHANNEL_OPERATOR_UI if verified
                    else RULING_CHANNEL_API_DIRECT),
        "channel_vocabulary": list(RULING_CHANNEL_VOCAB),
        "channel_derived_by": OWNER,
        "channel_asserted_by_caller": False,
        "verified": verified,
        "reason": reason,
        "reason_vocabulary": list(PROV_REASON_VOCAB),
        # --- the CEREMONY: the only evidence that carries any weight here ------- #
        "ceremony": "R82.1.1_SINGLE_USE_OPERATOR_CONFIRMATION",
        "ceremony_issuer": OWNER,
        "ceremony_route": RULING_CONFIRMATION_ROUTE,
        "ceremony_verified": ceremony_ok,
        "ceremony_reason": cons.get("reason") or PROV_NO_CONFIRMATION,
        "confirmation_presented": bool(cons.get("presented")),
        "confirmation_consumed": bool(cons.get("consumed")),
        "confirmation_id": cons.get("confirmation_id"),
        "confirmation_issued_at": cons.get("issued_at"),
        "confirmation_consumed_at": cons.get("consumed_at"),
        "confirmation_is_single_use": True,
        "confirmation_is_derivable_by_a_caller": False,
        "confirmation_ttl_seconds": RULING_CONFIRMATION_TTL_SECONDS,
        # --- CLAIMS: recorded, never weighed ------------------------------------ #
        "operator_confirmation_asserted": confirmed_in_ui is True,
        "asserted_confirmation_is_evidence": False,
        "surface_is_evidence": False,
        "actor_is_evidence": False,
        "submission_token_presented": bool(submission_token),
        "submission_token_bound_to_this_book": token_bound,
        "submission_token_alone_is_evidence": False,
        "bound_ruling": ruling,
        "bound_selection_id": selection_id,
        "bound_selected_target_implementation_hash": (
            selected_target_implementation_hash),
        "bound_reference_limit": reference_limit,
        "surface": surface,
        "actor": actor,
        "operational_use": (RULING_USE_AUTHORITATIVE if verified
                            else RULING_USE_UNVERIFIED),
        "proves": (
            "This submission spent a single-use operator confirmation that this "
            "backend process issued for this exact frozen book and this exact "
            "ruling, in a ceremony that required the token a governed read of this "
            "process had minted and the typed confirmation phrase. The confirmation "
            "had not expired and had never been spent before."
            if verified else
            "Nothing. This submission spent no operator confirmation issued by this "
            "backend for this ruling on this frozen book."),
        "does_not_prove": (
            "That a specific human being pressed a key. No check at this layer can, "
            "and this record does not claim it: a client that performs every act of "
            "the ceremony is treated as the operator. What it does exclude is a "
            "caller becoming authoritative by obtaining the served token and "
            "asserting a confirmation, which is what R82.1 allowed."),
    }


def ruling_provenance_state(record: Optional[dict]) -> dict:
    """Is the ruling on THIS record authoritative for operational use?

    Fails closed on absence, which is the whole point: every ruling written before
    R82.1 carries no provenance block at all, so none of them governs anything. The
    record stays readable audit evidence either way. No ruling id is special-cased
    anywhere in this function.
    """
    rec = record if isinstance(record, dict) else {}
    prov = rec.get("provenance")
    if not isinstance(prov, dict) or not prov:
        return {
            "verified": False, "channel": RULING_CHANNEL_ABSENT,
            "reason": PROV_MISSING,
            "operational_use": RULING_USE_UNVERIFIED,
            "channel_vocabulary": list(RULING_CHANNEL_VOCAB),
            "use_vocabulary": list(RULING_USE_VOCAB),
            "readable_as_audit_evidence": True,
            "supersedable_by_a_verified_ruling": True,
            "detail": (
                "This ruling carries no provenance block, so there is no evidence it "
                "was recorded by an operator through the governed review screen. It "
                "is kept as readable audit evidence and binds nothing: the binding "
                "cap is unchanged by it and approval stays withheld until a ruling "
                "with verified operator provenance is recorded. Recording one "
                "supersedes this record through the normal governed revision path; "
                "nothing is deleted or rewritten."),
        }
    verified = (prov.get("verified") is True
                and prov.get("channel") in RULING_CHANNEL_TRUSTED)
    return {
        "verified": bool(verified),
        "channel": prov.get("channel"),
        "reason": (None if verified else (prov.get("reason") or PROV_MISSING)),
        "operational_use": (RULING_USE_AUTHORITATIVE if verified
                            else RULING_USE_UNVERIFIED),
        "channel_vocabulary": list(RULING_CHANNEL_VOCAB),
        "use_vocabulary": list(RULING_USE_VOCAB),
        "readable_as_audit_evidence": True,
        "supersedable_by_a_verified_ruling": not verified,
        "surface": prov.get("surface"),
        "operator_confirmation_asserted": prov.get(
            "operator_confirmation_asserted"),
        "detail": (
            "Recorded by an operator through the governed review screen for this "
            "exact frozen book." if verified else
            "This ruling was not submitted through the governed operator review of "
            "this frozen book (%s), so it binds nothing and is kept as readable "
            "audit evidence." % (prov.get("reason") or PROV_MISSING)),
    }


def _rulings_path(decision_dir=None) -> Path:
    return _decision_dir(decision_dir) / _RULINGS_FILE


def _ruling_index_path(decision_dir=None) -> Path:
    return _decision_dir(decision_dir) / _RULING_INDEX_FILE


def load_risk_policy_ruling(*, active_book_id: Optional[str],
                            eligible_market_date: Optional[str],
                            selected_target_implementation_hash: Optional[str] = None,
                            decision_dir=None) -> Optional[dict]:
    """The latest governed risk-policy ruling for an exact (book, session), or None.

    When ``selected_target_implementation_hash`` is given, a ruling recorded against
    a DIFFERENT frozen book is not returned. That is the whole point of binding it:
    a ruling is about one book, so a selection revised underneath it leaves the new
    book unruled rather than inheriting a ruling nobody made about it.

    PURE reader; never raises.
    """
    try:
        index = _load_json(_ruling_index_path(decision_dir)) or {}
        ptr = index.get(_index_key(active_book_id, eligible_market_date))
        if not ptr:
            return None
        rid = ptr.get("ruling_id")
        rows = _load_json(_rulings_path(decision_dir)) or []
        rec = None
        for r in reversed(rows):
            if r.get("ruling_id") == rid:
                rec = r
                break
        if rec is None:
            rec = ptr.get("record")
        if rec is None:
            return None
        if (selected_target_implementation_hash is not None
                and rec.get("selected_target_implementation_hash")
                != selected_target_implementation_hash):
            return None
        return rec
    except Exception:  # noqa: BLE001 - a pure read must never crash the caller
        return None


def record_risk_policy_ruling(*, ruling: Optional[str], confirm: Optional[str],
                              selection: Optional[dict] = None,
                              expected_selection_id: Optional[str] = None,
                              expected_selected_target_implementation_hash:
                                  Optional[str] = None,
                              expected_reference_limit: Optional[float] = None,
                              instruments: Optional[list] = None,
                              actor: Optional[str] = None,
                              submission_token: Optional[str] = None,
                              confirmed_in_ui: Optional[bool] = None,
                              operator_confirmation: Optional[str] = None,
                              surface: Optional[str] = None,
                              decision_dir=None,
                              now: Optional[datetime] = None) -> dict:
    """Record ONE durable governed ruling on the moving-denominator policy question.

    It is NOT an approval: it records no decision, builds no order plan, creates no
    order or fill, moves no capital, mutates no proposal and does not touch the
    frozen selection record or its ``selected_target_implementation_hash``. It also
    changes no declared threshold and grants no standing exception — the 3/N policy
    stays exactly as ``engine.holding_opportunity_cost`` declares it, and the ruling
    binds only the ONE proposal and target it names.

    Fails closed on every identity that could have moved underneath the operator
    (selection id, frozen book hash, reference limit, instrument set), for the same
    reason the acknowledgement does: a ruling that names a different book is not a
    weaker ruling, it is a ruling about something else.

    Idempotent — the same ruling against the same identities writes no second
    artifact; a DIFFERENT ruling on the same book is preserved as an auditable
    revision with the superseded id retained.

    R82.1 — every record carries a DERIVED provenance block, and only a ruling whose
    provenance verifies has governed effect. R82.1.1 — the evidence is
    ``operator_confirmation``: a single-use confirmation this backend issued through
    ``open_ruling_confirmation`` for this exact frozen book and this exact ruling,
    which is SPENT here and cannot be spent twice. ``submission_token`` binds the
    book; ``confirmed_in_ui``, ``surface`` and ``actor`` are recorded as claims and
    weigh nothing. Without a confirmation the ruling is still written (so an
    attempted ruling is auditable) and marked ``UNVERIFIED``, which binds nothing and
    leaves approval withheld. The channel is derived here from that evidence — a
    caller can neither declare its own provenance nor mint its own confirmation.
    """
    ts = _now_iso(now)
    base = {"owner": OWNER, "phase": "R82", "recorded": False, "reused": False,
            "revised": False, "evaluated_at": ts,
            "ruling_vocabulary": list(RULING_VOCAB),
            "available_rulings": list(RULING_AVAILABLE),
            "status_vocabulary": list(RULING_STATUS_VOCAB),
            "confirm_token": RULING_CONFIRM_TOKEN,
            "scope": _st.RULING_SCOPE,
            "provenance_channel_vocabulary": list(RULING_CHANNEL_VOCAB),
            "provenance_use_vocabulary": list(RULING_USE_VOCAB),
            "is_an_approval": False, "approves_proposal": False,
            "records_a_portfolio_decision": False,
            "creates_order_plan": False, "creates_orders": False,
            "creates_fills": False, "executes": False, "deploys_capital": False,
            "mutates_proposal": False, "mutates_selection": False,
            "changes_declared_policy": False,
            "grants_standing_exception": False,
            "creates_absolute_companion_floor": False,
            "unblocks_approval": False,
            "decided_by_llm": False,
            "manual_approval_still_required": True}

    if confirm != RULING_CONFIRM_TOKEN:
        return {**base, "status": RULING_NOT_RECORDED,
                "message": ("A risk-policy ruling requires the explicit "
                            "confirmation token %s." % RULING_CONFIRM_TOKEN)}

    effect = _st.ruling_effect(ruling)
    if not effect["known"]:
        return {**base, "status": RULING_UNKNOWN, "ruling": ruling,
                "message": effect["detail"]}
    if not effect["available"]:
        return {**base, "status": RULING_NOT_AVAILABLE, "ruling": ruling,
                "reason": effect["reason"], "message": effect["detail"]}

    if not selection:
        return {**base, "status": RULING_NO_SELECTION, "ruling": ruling,
                "message": ("There is no governed target selection for this book "
                            "and session, so there is no frozen target to rule on. "
                            "Select a target first.")}

    review = selection_policy_review(selection)
    if not review.get("published"):
        return {**base, "status": RULING_NOT_PUBLISHED, "ruling": ruling,
                "risk_policy_review": review,
                "message": review.get("detail") or (
                    "This selection published no risk-policy verdict, so there is "
                    "nothing a ruling could bind.")}
    if not review.get("required"):
        return {**base, "status": RULING_NOT_REQUIRED, "ruling": ruling,
                "risk_policy_review": review,
                "message": ("This target complies with the limit the current book "
                            "was judged against, so no risk-policy ruling stands "
                            "between it and the existing approval gates. Nothing "
                            "was written.")}

    # --- bound identity: the same book, cap and names the operator was shown ---- #
    sid = selection.get("selection_id")
    if expected_selection_id is not None and expected_selection_id != sid:
        return {**base, "status": RULING_WRONG_SELECTION, "ruling": ruling,
                "expected_selection_id": expected_selection_id,
                "actual_selection_id": sid,
                "message": ("The governed selection changed since it was reviewed, "
                            "so this ruling would bind a target the operator never "
                            "saw. Re-review before ruling.")}
    want_hash = review.get("implementation_hash")
    got_hash = expected_selected_target_implementation_hash
    if got_hash is not None and got_hash != want_hash:
        return {**base, "status": RULING_WRONG_BOOK, "ruling": ruling,
                "expected_selected_target_implementation_hash": want_hash,
                "ruled_selected_target_implementation_hash": got_hash,
                "message": ("This ruling names a different frozen book than the "
                            "selection holds. A ruling is about one book; it is "
                            "refused rather than carried to another.")}
    want_limit = review.get("reference_limit")
    if expected_reference_limit is not None and want_limit is not None and (
            abs(float(expected_reference_limit) - float(want_limit)) > 1.0e-9):
        return {**base, "status": RULING_WRONG_LIMIT, "ruling": ruling,
                "expected_reference_limit": want_limit,
                "ruled_reference_limit": expected_reference_limit,
                "message": ("This ruling names a different reference limit than "
                            "the one this book was judged against.")}
    want_names = sorted(review.get("instruments") or [])
    if instruments is not None and sorted(instruments) != want_names:
        return {**base, "status": RULING_WRONG_INSTRUMENTS, "ruling": ruling,
                "expected_instruments": want_names,
                "ruled_instruments": sorted(instruments or []),
                "message": ("This ruling names a different instrument set than the "
                            "one in breach against the reference limit.")}

    impl = selection.get("selected_target_implementation") or {}
    binding_block = selection.get("binding") or {}
    derived = _st.apply_policy_ruling(
        risk_contribution=(impl.get("risk_contribution") or {}), ruling=ruling)

    # --- provenance: DERIVED from evidence, never taken from the caller --------- #
    # The confirmation is SPENT here, after every identity check above has passed,
    # so a refused ruling never burns the operator's ceremony, and a ruling that
    # reaches this line can never be re-submitted with the same evidence.
    consumption = consume_ruling_confirmation(
        confirmation=operator_confirmation, ruling=ruling, selection_id=sid,
        selected_target_implementation_hash=want_hash,
        reference_limit=want_limit, now=now)
    provenance = ruling_provenance(
        submission_token=submission_token, confirmed_in_ui=confirmed_in_ui,
        consumption=consumption, ruling=ruling,
        selection_id=sid, selected_target_implementation_hash=want_hash,
        reference_limit=want_limit, surface=surface, actor=actor)
    verified = bool(provenance["verified"])

    book_id = binding_block.get("active_book_id")
    session = binding_block.get("eligible_market_date")
    existing = load_risk_policy_ruling(active_book_id=book_id,
                                       eligible_market_date=session,
                                       decision_dir=decision_dir)
    same_book = bool(existing and existing.get(
        "selected_target_implementation_hash") == want_hash)
    # Reuse requires the same ruling AND the same authority. An unverified record
    # must never absorb a later verified ruling into itself — the verified one has
    # to reach the store, or the operator's actual decision would be silently
    # answered with "already on record" by an artifact that governs nothing.
    existing_verified = (ruling_provenance_state(existing)["verified"]
                         if existing else False)
    if (existing and same_book and existing.get("ruling") == ruling
            and existing_verified == verified):
        return {**base, "status": RULING_REUSED, "recorded": True, "reused": True,
                "ruling": ruling, "record": existing,
                "risk_policy_review": review, "derived": derived,
                "provenance": provenance,
                "provenance_verified": verified,
                "provenance_state": ruling_provenance_state(existing),
                "message": ("This ruling is already on record against this exact "
                            "frozen book. No second artifact was written.")}

    revised = bool(existing and same_book)
    ruling_id = "prul_%s_%s_%s_%s" % (
        session or "nodate", book_id or "book",
        str(selection.get("selected_target") or "target").lower(),
        str(want_hash or "")[:12])
    if revised:
        ruling_id += "_r%d" % (int((existing or {}).get("revision", 0)) + 1)

    record = {
        "ruling_id": ruling_id,
        "owner": OWNER,
        "phase": "R82",
        "artifact_kind": "risk_policy_ruling",
        "artifact_doc": (
            "A GOVERNANCE artifact. It records an operator's ruling on the "
            "moving-denominator risk-contribution question for ONE frozen "
            "proposal and ONE selected target. It is not an approval, it changes "
            "no declared threshold and it grants no standing exception."),
        "ruling": ruling,
        "ruling_label": effect["detail"],
        "binds": effect["binds"],
        "ruled_at": ts,
        # R82.1 — the ACTOR as the caller named them, which is narration, and the
        # PROVENANCE, which is evidence. They are separate fields because R82 had
        # only the first and a record that said "operator" could not be trusted to
        # mean one. The default is no longer "operator": a caller that names nobody
        # is now recorded as naming nobody, rather than as the portfolio operator.
        "ruled_by": actor or RULING_ACTOR_UNATTRIBUTED,
        "ruled_by_is_evidence": False,
        "provenance": provenance,
        "provenance_verified": verified,
        "provenance_contract": (
            "R82.1.1_RULING_AUTHORITY_REQUIRES_A_SPENT_GOVERNED_OPERATOR_"
            "CONFIRMATION"),
        "governs_this_book": verified,
        "revision": (int((existing or {}).get("revision", 0)) + 1) if revised else 0,
        "supersedes_ruling_id": (existing or {}).get("ruling_id") if revised else None,
        # --- the exact identity this ruling is about --------------------------- #
        "active_book_id": book_id,
        "eligible_market_date": session,
        "proposal_id": binding_block.get("proposal_id"),
        "proposal_hash": binding_block.get("proposal_hash"),
        "review_hash": binding_block.get("review_hash"),
        "selection_id": sid,
        "selected_target": selection.get("selected_target"),
        "selected_target_implementation_hash": want_hash,
        "reference_limit": want_limit,
        "governed_limit": review.get("governed_limit"),
        "instruments": want_names,
        "risk_policy_review_state": review.get("state"),
        "policy_owner": _st.RISK_POLICY_OWNER,
        # --- what the ruling MEANS for this book, derived by the kernel -------- #
        "derived": derived,
        "binding_limit": derived.get("binding_limit"),
        "binding_limit_source": derived.get("binding_limit_source"),
        "reopened_obligations": derived.get("reopened_obligations"),
        "reopened_obligation_count": derived.get("reopened_obligation_count"),
        "approval_blocked_by_this_ruling": derived.get(
            "approval_blocked_by_this_ruling"),
        "scope": _st.RULING_SCOPE,
        "confirm_token": RULING_CONFIRM_TOKEN,
        "manual_review_reference": _st.POLICY_REVIEW_REFERENCE_DOC,
        "is_an_approval": False,
        "changes_declared_policy": False,
        "grants_standing_exception": False,
        "creates_absolute_companion_floor": False,
        "unblocks_approval": False,
        "creates_order_plan": False,
        "creates_orders": False,
    }

    rows = _load_json(_rulings_path(decision_dir)) or []
    if not isinstance(rows, list):
        rows = []
    rows.append(record)
    _atomic_write_json(_rulings_path(decision_dir), rows)
    index = _load_json(_ruling_index_path(decision_dir)) or {}
    index[_index_key(book_id, session)] = {
        "ruling_id": ruling_id, "ruling": ruling,
        "selection_id": sid,
        "selected_target": selection.get("selected_target"),
        "selected_target_implementation_hash": want_hash,
        "reference_limit": want_limit,
        "binding_limit": derived.get("binding_limit"),
        "approval_blocked_by_this_ruling": derived.get(
            "approval_blocked_by_this_ruling"),
        "provenance_verified": verified,
        "ruled_at": ts, "record": record}
    _atomic_write_json(_ruling_index_path(decision_dir), index)
    # An unverified write reports UNVERIFIED whether or not it revised something:
    # "REVISED" on its own would read as a governed decision replacing another.
    status = (RULING_RECORDED_UNVERIFIED if not verified
              else (RULING_REVISED if revised else RULING_RECORDED))
    return {**base, "status": status,
            "recorded": True, "revised": revised, "ruling": ruling,
            "ruling_id": ruling_id, "record": record,
            "risk_policy_review": review, "derived": derived,
            "provenance": provenance, "provenance_verified": verified,
            "provenance_state": ruling_provenance_state(record),
            "governs_this_book": verified,
            "message": (derived.get("detail") if verified else
                        ("The ruling was written as audit evidence and GOVERNS "
                         "NOTHING: %s. Approval stays withheld exactly as it was, "
                         "the binding cap is unchanged, and a ruling recorded "
                         "through the governed operator review supersedes this "
                         "record." % provenance["reason"]))}


def risk_policy_ruling_state(*, selection: Optional[dict],
                             ruling_record: Optional[dict] = None,
                             decision_dir=None) -> dict:
    """The ruled state ONE frozen selection carries, for a READ surface.

    Composed, never recomputed: the ruling is read from the governed store and its
    meaning from ``engine.selected_target.apply_policy_ruling`` over the frozen
    book's own risk block. The frozen artifact is not modified — this is a layer
    published beside it, so ``selected_target_implementation_hash`` never moves.

    ``UNRULED`` is the honest state for a book nobody has ruled on, and it is NOT
    read as permission: the R69.5 gate continues to withhold approval there.

    R82.1 — a ruling with UNVERIFIED provenance reaches the honest FOURTH state:
    the record is published so the operator can see it, and it binds nothing. It
    neither blocks approval (that would let a development artifact govern) nor
    clears the policy review (that would let one grant). Approval therefore stays
    withheld exactly where it was, which is the only fail-closed answer.
    """
    sel = selection or {}
    impl = sel.get("selected_target_implementation") or {}
    review = selection_policy_review(sel)
    want_hash = review.get("implementation_hash")
    binding_block = sel.get("binding") or {}
    rec = ruling_record
    if rec is None and sel:
        rec = load_risk_policy_ruling(
            active_book_id=binding_block.get("active_book_id"),
            eligible_market_date=binding_block.get("eligible_market_date"),
            selected_target_implementation_hash=want_hash,
            decision_dir=decision_dir)
    out = {
        "owner": OWNER,
        "policy_owner": _st.RISK_POLICY_OWNER,
        "calculation_owner": _st.CALCULATION_OWNER,
        "ruling_vocabulary": list(RULING_VOCAB),
        "available_rulings": list(RULING_AVAILABLE),
        "unavailable_rulings": list(_st.RULING_NOT_AVAILABLE_THIS_RELEASE),
        "unavailable_reason": _st.RULING_UNAVAILABLE_REASON,
        "confirm_token": RULING_CONFIRM_TOKEN,
        "risk_policy_review": review,
        "scope": _st.RULING_SCOPE,
        "state_vocabulary": list(_st.RULED_STATE_VOCAB),
        "declared_policy_changed": False,
        "standing_exception_granted": False,
        "absolute_companion_floor_created": False,
        "target_approved_here": False,
        "provenance_channel_vocabulary": list(RULING_CHANNEL_VOCAB),
        "provenance_use_vocabulary": list(RULING_USE_VOCAB),
        "manual_review_reference": _st.POLICY_REVIEW_REFERENCE_DOC,
    }
    if not rec:
        return {**out, "ruled": False, "ruling": None, "ruling_id": None,
                "state": _st.RULED_UNRULED,
                "authoritative": False,
                "provenance_state": None,
                "satisfies_the_policy_review": False,
                "approval_blocked_by_the_ruling": False,
                "binding_limit": None, "binding_limit_source": None,
                "reopened_obligations": [], "reopened_obligation_count": 0,
                "treatment": [], "derived": None,
                "detail": ("No risk-policy ruling is on record for this frozen "
                           "book. That is not permission: approval stays withheld "
                           "at %s until one is recorded."
                           % PDS_RISK_POLICY_REVIEW_REQUIRED)}
    derived = _st.apply_policy_ruling(
        risk_contribution=(impl.get("risk_contribution") or {}),
        ruling=rec.get("ruling"))
    prov = ruling_provenance_state(rec)
    if not prov["verified"]:
        # Present, readable, and without authority. Every governed consequence is
        # withheld: no binding cap, no reopened obligation, and no satisfaction of
        # the policy review the R69.5 gate is still waiting on.
        return {**out, "ruled": True, "authoritative": False,
                "ruling": rec.get("ruling"),
                "ruling_id": rec.get("ruling_id"),
                "ruled_by": rec.get("ruled_by"),
                "ruled_at": rec.get("ruled_at"),
                "revision": rec.get("revision"),
                "selection_id": rec.get("selection_id"),
                "selected_target": rec.get("selected_target"),
                "selected_target_implementation_hash": rec.get(
                    "selected_target_implementation_hash"),
                "proposal_id": rec.get("proposal_id"),
                "instruments": list(rec.get("instruments") or []),
                "reference_limit": rec.get("reference_limit"),
                "governed_limit": rec.get("governed_limit"),
                "state": _st.RULED_UNVERIFIED,
                "provenance_state": prov,
                "satisfies_the_policy_review": False,
                "approval_blocked_by_the_ruling": False,
                "binding_limit": None, "binding_limit_source": None,
                "reopened_obligations": [], "reopened_obligation_count": 0,
                "treatment": [],
                "derived": None,
                # What it WOULD mean if an operator confirmed it, kept under a name
                # no governed consumer reads as a verdict.
                "derived_if_confirmed": derived,
                "detail": prov["detail"] + (
                    " Approval stays withheld at %s until a ruling with verified "
                    "operator provenance is recorded through the review screen."
                    % PDS_RISK_POLICY_REVIEW_REQUIRED)}
    return {**out, "ruled": True, "authoritative": True,
            "provenance_state": prov,
            # A verified ruling ANSWERS the R69.5 policy review. Whether it then
            # permits an approval is a different question, decided by the binding
            # cap below and by every other gate.
            "satisfies_the_policy_review": not bool(
                derived.get("approval_blocked_by_this_ruling")),
            "ruling": rec.get("ruling"),
            "ruling_id": rec.get("ruling_id"),
            "ruled_by": rec.get("ruled_by"),
            "ruled_at": rec.get("ruled_at"),
            "revision": rec.get("revision"),
            "supersedes_ruling_id": rec.get("supersedes_ruling_id"),
            "selection_id": rec.get("selection_id"),
            "selected_target": rec.get("selected_target"),
            "selected_target_implementation_hash": rec.get(
                "selected_target_implementation_hash"),
            "proposal_id": rec.get("proposal_id"),
            "proposal_hash": rec.get("proposal_hash"),
            "instruments": list(rec.get("instruments") or []),
            "reference_limit": rec.get("reference_limit"),
            "governed_limit": rec.get("governed_limit"),
            "state": derived.get("state"),
            "binds": derived.get("binds"),
            "binding_limit": derived.get("binding_limit"),
            "binding_limit_source": derived.get("binding_limit_source"),
            "reopened_obligations": list(derived.get("reopened_obligations") or []),
            "reopened_obligation_count": derived.get("reopened_obligation_count"),
            "obligations_open_on_the_governed_limit": derived.get(
                "obligations_open_on_the_governed_limit"),
            "instruments_in_breach_of_the_binding_limit": list(
                derived.get("instruments_in_breach_of_the_binding_limit") or []),
            "complies_with_the_binding_limit": derived.get(
                "complies_with_the_binding_limit"),
            "approval_blocked_by_the_ruling": bool(
                derived.get("approval_blocked_by_this_ruling")),
            "treatment": list(derived.get("treatment") or []),
            "derived": derived,
            "detail": derived.get("detail")}


# --------------------------------------------------------------------------- #
# R82.1 — THE OPERATOR DECISION CONTRACT for a write surface.
#
# R69.5 printed three English sentences and no control; R82 stated a ruling once it
# existed and still offered no control. Neither published what a screen needs to
# BUILD one: which options exist, which are available, what each does, what the
# submission must bind, and the tokens that make it governed.
#
# This is that contract. It is a pure composition over the frozen selection and the
# governed store: the backend remains the authority for the vocabulary and for every
# validation, and the browser holds no ruling knowledge of its own.
# --------------------------------------------------------------------------- #
RULING_ROUTE = "/v1/operations/portfolio-decision/risk-policy-ruling"
#: Where the operator stands on the risk-policy question for this frozen book.
RPD_NOT_APPLICABLE = "NO_RISK_POLICY_QUESTION_ON_THIS_BOOK"
RPD_REQUIRED = "OPERATOR_RULING_REQUIRED"
RPD_REQUIRED_PRIOR_UNVERIFIED = "OPERATOR_RULING_REQUIRED_PRIOR_RECORD_UNVERIFIED"
RPD_TAKEN = "OPERATOR_RULING_ON_RECORD"
RPD_STATE_VOCAB = (RPD_NOT_APPLICABLE, RPD_REQUIRED,
                   RPD_REQUIRED_PRIOR_UNVERIFIED, RPD_TAKEN)


def risk_policy_decision(*, selection: Optional[dict],
                         ruling_state: Optional[dict] = None,
                         decision_dir=None) -> dict:
    """What the operator must decide about this frozen book, and how to submit it.

    ``required`` is True only while the book owes a ruling that no AUTHORITATIVE
    record answers. A prior UNVERIFIED record does not answer it and is disclosed
    beside the controls rather than standing in for a decision nobody made.

    Pure read. It records nothing, approves nothing and mints no durable state: the
    submission token is re-derived from the frozen identity on every read.
    """
    sel = selection or {}
    ruled = (ruling_state if ruling_state is not None
             else risk_policy_ruling_state(selection=sel, decision_dir=decision_dir))
    review = ruled.get("risk_policy_review") or selection_policy_review(sel)
    binding_block = sel.get("binding") or {}
    impl_hash = review.get("implementation_hash")
    authoritative = bool(ruled.get("authoritative"))
    owed = bool(sel) and bool(review.get("required"))
    prior_unverified = bool(ruled.get("ruled")) and not authoritative

    if not owed:
        state, required = RPD_NOT_APPLICABLE, False
    elif authoritative:
        state, required = RPD_TAKEN, False
    elif prior_unverified:
        state, required = RPD_REQUIRED_PRIOR_UNVERIFIED, True
    else:
        state, required = RPD_REQUIRED, True

    token = (ruling_submission_token(
        selection_id=sel.get("selection_id"),
        selected_target_implementation_hash=impl_hash,
        reference_limit=review.get("reference_limit")) if required else None)
    return {
        "owner": OWNER,
        "policy_owner": _st.RISK_POLICY_OWNER,
        "state": state,
        "state_vocabulary": list(RPD_STATE_VOCAB),
        "required": required,
        "route": RULING_ROUTE,
        "method": "POST",
        "confirm_token": RULING_CONFIRM_TOKEN,
        "submission_token": token,
        "submission_token_doc": (
            "Echo this verbatim. It is minted per read and bound to this exact "
            "frozen book. It is NOT evidence of a decision on its own (R82.1 "
            "treated it as half of one): it is what lets you OPEN the operator "
            "confirmation ceremony below."),
        "operator_confirmation_required": True,
        # R82.1.1 — the ceremony. A ruling is authoritative only if it spends a
        # confirmation this backend issued here; nothing published by this read can
        # substitute for one, which is why no confirmation appears in it.
        "confirmation_route": RULING_CONFIRMATION_ROUTE,
        "confirmation_method": "POST",
        "confirmation_ttl_seconds": RULING_CONFIRMATION_TTL_SECONDS,
        "confirmation_is_single_use": True,
        "confirmation_ceremony": "R82.1.1_SINGLE_USE_OPERATOR_CONFIRMATION",
        "confirmation_doc": (
            "POST the chosen ruling, the confirmation phrase and the submission "
            "token to the confirmation route, then send the confirmation it returns "
            "with the ruling. The confirmation is issued by %s, is bound to that one "
            "ruling on this one frozen book, expires in %ds and is spent the first "
            "time it is presented. A ruling submitted without one is kept as "
            "readable audit evidence and governs nothing." % (
                OWNER, RULING_CONFIRMATION_TTL_SECONDS)),
        # The vocabulary and its availability are the KERNEL's, published so a
        # screen can render the choice without holding a copy of the rule.
        "options": _st.ruling_options(),
        "available_rulings": list(RULING_AVAILABLE),
        "unavailable_rulings": list(_st.RULING_NOT_AVAILABLE_THIS_RELEASE),
        "unavailable_reason": _st.RULING_UNAVAILABLE_REASON,
        "scope": _st.RULING_SCOPE,
        # Everything the submission must name, and everything the screen must show
        # the operator BEFORE they choose.
        "binds": {
            "proposal_id": binding_block.get("proposal_id"),
            "proposal_hash": binding_block.get("proposal_hash"),
            "selection_id": sel.get("selection_id"),
            "selected_target": sel.get("selected_target"),
            "selected_target_implementation_hash": impl_hash,
            "reference_limit": review.get("reference_limit"),
            "governed_limit": review.get("governed_limit"),
            "instruments": list(review.get("instruments") or []),
        },
        "prior_unverified_ruling": ({
            "ruling_id": ruled.get("ruling_id"),
            "ruling": ruled.get("ruling"),
            "ruled_at": ruled.get("ruled_at"),
            "ruled_by": ruled.get("ruled_by"),
            "provenance_state": ruled.get("provenance_state"),
            "governs_anything": False,
            "detail": ruled.get("detail"),
        } if prior_unverified else None),
        "risk_policy_review": review,
        "approves_nothing": True,
        "creates_order_plan": False,
        "creates_orders": False,
        "changes_declared_policy": False,
        "manual_approval_still_required": True,
        "manual_review_reference": _st.POLICY_REVIEW_REFERENCE_DOC,
        "detail": {
            RPD_NOT_APPLICABLE: (
                "This frozen book raises no risk-policy question, so there is "
                "nothing to rule on."),
            RPD_REQUIRED: (
                "Approval is withheld until you record which policy governs this "
                "frozen book. Recording a ruling approves nothing."),
            RPD_REQUIRED_PRIOR_UNVERIFIED: (
                "A prior ruling is on record for this book and its provenance was "
                "never verified, so it binds nothing and approval stays withheld. "
                "Your ruling supersedes it through the normal governed revision "
                "path; the earlier record is kept as audit evidence."),
            RPD_TAKEN: (
                "An operator ruling is on record for this frozen book. It is stated "
                "with the target; there is nothing further to decide here."),
        }[state],
    }


# =========================================================================== #
# R82.2 — THE APPROVAL GATE, PUBLISHED ON A READ.
#
# The authority this publishes is not new. ``record_decision`` has ruled on exactly
# this question since R63, and R69.2/R69.5/R82/R82.1 each added a term to it. What
# never existed was a READ of that ruling, so every surface that needed to know
# whether approval was the operator's current act had to guess — and the browser's
# guess was ``the session is actionable and a target is selected``.
#
# On 2026-09-28, against the frozen 2026-09-25 MINIMUM_REPAIR, that guess produced a
# panel which said APPROVAL WITHHELD in Step 3, asked for a RISK POLICY DECISION in
# Step 4, and in the same paint reported NEXT REQUIRED ACTION = APPROVE SELECTED
# TARGET beside an armed APPROVE MINIMUM REPAIR button.
#
# The two functions below are that read. They are PURE COMPOSITIONS over verdicts
# the existing owners already produced:
#
#   * ``selection_policy_review``    — does this frozen book owe a ruling?
#   * ``risk_policy_ruling_state``   — does a ruling answer it, and does the ruling
#                                      itself refuse the book?
#   * ``decision_freshness``         — is the bound session still actionable?
#   * ``recorded_selection_implementability`` — can this book become an order plan?
#
# No cap, share, weight, excess, turnover or cost is derived here, no threshold is
# read, and nothing is written. There is no second gate order either: the WRITE path
# consumes ``risk_policy_approval_gate`` for its own two risk-policy refusals, so the
# read and the write cannot drift into disagreement by editing one of them.
# =========================================================================== #
#: The gate's own verdict word when nothing withholds approval. It is the canonical
#: manual-review state, reused: a proposal whose every gate is clear is a proposal
#: awaiting the operator's manual decision, which is what that state has always meant.
AG_AVAILABLE = PDS_REVIEW_REQUIRED
#: Ordered, most-specific-first, and identical to the order ``record_decision``
#: evaluates. Published so a surface can state WHERE in the gate it is standing.
APPROVAL_GATE_STATUS_VOCAB = (
    AG_AVAILABLE, PDS_SESSION_STALE, PDS_TARGET_SELECTION_REQUIRED, PDS_STALE,
    PDS_SELECTION_IS_NO_CHANGE, PDS_SELECTED_TARGET_NOT_IMPLEMENTABLE,
    PDS_REFERENCE_LIMIT_BREACH_RULED, PDS_RISK_POLICY_REVIEW_REQUIRED)


def risk_policy_approval_gate(*, policy_review: Optional[dict],
                              ruling_state: Optional[dict]) -> dict:
    """Does the RISK-POLICY layer withhold approval of this frozen book, and why?

    ONE ordered answer, consumed by BOTH the approval write path in
    :func:`record_decision` and the read projection in
    :func:`selected_target_approval_gate`. Pure; no io; decides no economics.

    The order matters and is the write path's own. A recorded
    ``JUDGE_AGAINST_THE_BEFORE_UNIVERSE`` is a SUBSTANTIVE refusal - the ruling
    exists and the target fails it - so it outranks "nobody has ruled yet", which is
    a procedural one. Reversing them would let an acknowledgement clear a breach an
    operator had already ruled binding.

    An UNVERIFIED ruling reaches neither branch as an answer: R82.1 publishes
    ``satisfies_the_policy_review = False`` and
    ``approval_blocked_by_the_ruling = False`` for it, so it falls to the second
    branch exactly as an unruled book does. That is the only fail-closed reading: it
    neither governs nor grants.
    """
    review = policy_review or {}
    ruled = ruling_state or {}
    owed = bool(review.get("required"))
    answered = bool(ruled.get("satisfies_the_policy_review"))
    if ruled.get("approval_blocked_by_the_ruling"):
        return {
            "withholds_approval": True,
            "status": PDS_REFERENCE_LIMIT_BREACH_RULED,
            "next_required_action": (
                NEXT_ACTION_REVIEW_THE_POLICY_COMPLIANT_SUCCESSOR),
            "policy_compliant_successor_target": TARGET_POLICY_COMPLIANT_REPAIR,
            "ruling_owed": owed, "ruling_answers_the_review": answered,
            "prior_ruling_unverified": bool(ruled.get("ruled")
                                            and not ruled.get("authoritative")),
            "policy_owner": _st.RISK_POLICY_OWNER, "owner": OWNER,
            "detail": (
                "The governed risk-policy ruling on record for this frozen book "
                "refuses it: %s Approval is withheld. The governed review solves "
                "the compliant successor and publishes it as %s; review it, select "
                "it, and approve that selection instead. Reject and hold remain "
                "available." % (ruled.get("detail") or "",
                                TARGET_POLICY_COMPLIANT_REPAIR)),
        }
    if owed and not answered:
        return {
            "withholds_approval": True,
            "status": PDS_RISK_POLICY_REVIEW_REQUIRED,
            "next_required_action": NEXT_ACTION_RECORD_RISK_POLICY_RULING,
            "policy_compliant_successor_target": None,
            "ruling_owed": True, "ruling_answers_the_review": False,
            "prior_ruling_unverified": bool(ruled.get("ruled")
                                            and not ruled.get("authoritative")),
            "policy_owner": _st.RISK_POLICY_OWNER, "owner": OWNER,
            "detail": (
                "This frozen book owes an operator risk-policy ruling and no "
                "ruling with verified operator provenance answers it, so approval "
                "is withheld. Recording a ruling approves nothing; approval stays "
                "a separate manual act at its own gate."),
        }
    return {
        "withholds_approval": False, "status": None,
        "next_required_action": None,
        "policy_compliant_successor_target": None,
        "ruling_owed": owed, "ruling_answers_the_review": answered,
        "prior_ruling_unverified": False,
        "policy_owner": _st.RISK_POLICY_OWNER, "owner": OWNER,
        "detail": ("The risk-policy layer withholds nothing about this frozen "
                   "book." if not owed else
                   "A ruling with verified operator provenance answers the "
                   "risk-policy review for this frozen book. It is not an "
                   "approval: every ordinary approval gate still applies."),
    }


def selected_target_approval_gate(*, selection: Optional[dict],
                                  freshness: Optional[dict] = None,
                                  ruling_state: Optional[dict] = None,
                                  policy_review: Optional[dict] = None,
                                  current_proposal_hash: Optional[str] = None,
                                  decision_dir=None) -> dict:
    """Is approval the operator's CURRENT act on this frozen selection, or not?

    The read counterpart of the APPROVE branch of :func:`record_decision`, evaluated
    in that branch's own order. It is the ONE thing a surface must consume before it
    renders an approval affordance or names a next required action: a control that
    leads to a refusal three gates later is exactly the misleading Approve button
    this project forbids.

    Fail-closed on identity. When ``current_proposal_hash`` is supplied and the
    selection is bound to a different proposal, approval is unavailable - the write
    path refuses that approval as ``PROPOSAL_STALE``, and a read that offered it
    would be advertising a decision on a book nobody reviewed.

    It writes nothing, approves nothing, creates no order plan and moves no
    threshold. ``decision_dir`` is only ever used to READ the governed ruling.
    """
    sel = selection or None
    fresh = freshness or None
    review = (policy_review if policy_review is not None
              else (selection_policy_review(sel) if sel else None))
    ruled = ruling_state
    if ruled is None and sel:
        ruled = risk_policy_ruling_state(selection=sel, decision_dir=decision_dir)
    policy = risk_policy_approval_gate(policy_review=review, ruling_state=ruled)

    selected_target = (sel or {}).get("selected_target")
    bound_hash = ((sel or {}).get("binding") or {}).get("proposal_hash")
    impl_verdict = recorded_selection_implementability(sel) if sel else None

    def _out(status, action, detail, **extra) -> dict:
        return {
            "owner": OWNER,
            "gate_owner": OWNER,
            "available": status == AG_AVAILABLE,
            "status": status,
            "status_vocabulary": list(APPROVAL_GATE_STATUS_VOCAB),
            "next_required_action": action,
            "next_required_action_label": NEXT_ACTION_LABELS.get(action),
            "next_required_action_vocabulary": list(NEXT_ACTION_VOCAB),
            # What a surface must say where it would otherwise have offered the
            # approval it is not offering. Published so the words are the gate's.
            "approval_state_label": ("APPROVAL AVAILABLE"
                                     if status == AG_AVAILABLE
                                     else "APPROVAL WITHHELD"),
            "approval_withheld_because": (None if status == AG_AVAILABLE else status),
            "detail": detail,
            "selected_target": selected_target,
            "selection_id": (sel or {}).get("selection_id"),
            "selected_target_implementation_hash": (
                (sel or {}).get("selected_target_implementation_hash")),
            "risk_policy": policy,
            "risk_policy_withholds_approval": bool(policy["withholds_approval"]),
            "session_actionable": (None if not fresh
                                   else bool(fresh.get("approval_allowed"))),
            "confirm_required_token": CONFIRM_TOKEN,
            "approval_route": "POST /v1/operations/portfolio-decision/record",
            "reject_and_hold_remain_available": True,
            "is_an_approval": False, "approves_anything": False,
            "creates_order_plan": False, "creates_orders": False,
            "creates_fills": False, "writes_nothing": True,
            **extra,
        }

    # 1. Session freshness. First, exactly as on the write path: a proposal bound to
    #    a session the workflow has moved past is evidence, never a live decision.
    if fresh is not None and not fresh.get("approval_allowed"):
        return _out(PDS_SESSION_STALE,
                    (fresh.get("next_required_action")
                     or NEXT_ACTION_RUN_PORTFOLIO_CYCLE),
                    (fresh.get("detail")
                     or "This proposal is bound to a session the workflow has "
                        "moved past. It stays fully readable and is no longer a "
                        "decision anyone may act on."))
    # 2. A NEW approval must name the target it approves (R69.2).
    if not sel:
        return _out(PDS_TARGET_SELECTION_REQUIRED, NEXT_ACTION_SELECT_A_TARGET,
                    "No governed target selection exists for this session, so "
                    "there is nothing to approve. Select a target first; "
                    "selecting is not approving.")
    # 3. Identity. Fail closed on a selection bound to another proposal.
    if (current_proposal_hash is not None and bound_hash is not None
            and bound_hash != current_proposal_hash):
        return _out(PDS_STALE, NEXT_ACTION_SELECT_AGAINST_THE_CURRENT_REVIEW,
                    "The governed selection on file is bound to a different "
                    "proposal than the one being read, so it cannot be approved "
                    "here. Nothing is written and the records stay immutable; "
                    "select a target against the current review.",
                    selection_bound_proposal_hash=bound_hash,
                    current_proposal_hash=current_proposal_hash)
    # 4. CURRENT is a no-change decision, not a target.
    if selected_target == TARGET_CURRENT:
        return _out(PDS_SELECTION_IS_NO_CHANGE,
                    NEXT_ACTION_RECORD_THE_NO_CHANGE_DECISION,
                    "The governed selection for this session is CURRENT (keep the "
                    "book unchanged). There is no target to approve; the no-change "
                    "decision is the outcome.")
    # 5. A target with no implementable book can never become an order plan (R69.2).
    if impl_verdict and not impl_verdict.get("implementable"):
        return _out(PDS_SELECTED_TARGET_NOT_IMPLEMENTABLE,
                    NEXT_ACTION_SELECT_AN_IMPLEMENTABLE_TARGET,
                    impl_verdict.get("detail")
                    or "This selection carries no implementable book, so no order "
                       "plan could ever honour it.",
                    not_implementable_reason=impl_verdict.get("reason"))
    # 6 and 7. The risk-policy layer, in its own owner's order.
    if policy["withholds_approval"]:
        return _out(policy["status"], policy["next_required_action"],
                    policy["detail"],
                    policy_compliant_successor_target=policy[
                        "policy_compliant_successor_target"])
    # 8. Every gate this owner enforces is clear. Approval is the operator's act,
    #    and it is still a MANUAL one that creates no order and no fill.
    return _out(AG_AVAILABLE, NEXT_ACTION_APPROVE_SELECTED_TARGET,
                "Every gate this owner enforces is clear for the frozen book on "
                "file. Approval remains a manual operator act: it records the "
                "decision and creates no order plan, order or fill.")


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


def _session_blocks_approval(freshness: Optional[dict]) -> bool:
    """True only when a freshness verdict was SUPPLIED and it refuses approval."""
    if not freshness:
        return False
    return not bool(freshness.get("approval_allowed"))


def _gate_blocks_approval(approval_gate: Optional[dict]) -> bool:
    """R82.2 — True only when an approval-gate verdict was SUPPLIED and it withholds.

    Deliberately the same contract ``_session_blocks_approval`` gives the session
    term beside it: an absent gate is "no information", not "no". A caller that
    supplies none sees the pre-R82.2 answer, and the write path fails closed on its
    own regardless.
    """
    if not approval_gate:
        return False
    return not bool(approval_gate.get("available"))


def derive_decision_state(*, has_active_book: bool, proposal_summary: dict,
                          decision_record: Optional[dict],
                          freshness: Optional[dict] = None,
                          approval_gate: Optional[dict] = None) -> dict:
    """Compose the SEPARATE portfolio-decision review state from (a) the current proposal
    and (b) the latest recorded decision. Pure; no io.

    R63: ``freshness`` is the session verdict from :func:`decision_freshness`. When
    it says the bound session is no longer actionable this read reports
    ``approvable = False``, because the write path refuses it. A read model that
    advertised an approvable proposal the backend would then refuse is exactly the
    kind of hidden state this project treats as a defect - a surface would render
    an Approve affordance that cannot succeed."""
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
    # R83 — A WITHHELD VERDICT OUTRANKS "NO PROPOSAL".
    #
    # ``available`` means one thing: a PERSISTED proposal artifact exists. A withheld
    # complete target never has one, by the proposal owner's own persistence contract —
    # so PDS_CHANGE_WITHHELD sat behind a term that a withheld verdict can never
    # satisfy, and every withheld session collapsed to PDS_NO_PROPOSAL. That is the
    # same unreachable-state defect as the read state one layer below, and it is what
    # kept the reassessment card offering REVIEW PORTFOLIO PROPOSAL over a change the
    # governed owner had already refused (its suppression keys on this very state).
    #
    # A withholding is a POSITIVE governed answer, not an absence. The branch is
    # deliberately narrowed to ``and not available`` so it adds exactly ONE newly
    # reachable path and leaves every pre-existing precedence — stale, superseded, the
    # persisted-artifact withhold below — byte-identical. It can only ever REMOVE
    # approvability: PDS_CHANGE_WITHHELD is absent from APPROVABLE_DECISION_STATES, so
    # nothing becomes approvable by reaching it.
    elif withheld and not available:
        state = PDS_CHANGE_WITHHELD
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
        # R63 live integration — the mandatory-repair verdict, carried on the lane
        # so the cockpit cards and the review screen cannot disagree about the
        # SAME proposal. Rendered verbatim from the proposal owner's summary.
        "full_target_reviewable": bool(
            summ.get("reallocation_full_target_reviewable")),
        "mandatory_repair_code": summ.get("reallocation_mandatory_repair_code"),
        "mandatory_obligations_open": list(
            summ.get("reallocation_mandatory_obligations_open") or []),
        "mandatory_repair_detail": summ.get("reallocation_mandatory_repair_detail"),
        # R63 - session freshness can only ever REMOVE approvability. An unknown
        # verdict is treated as "no information" here (the write path still fails
        # closed on it), so a caller that supplies no session sees the pre-R63
        # answer rather than a silent False.
        # R63 live integration adds the obligation term here too, so the state
        # that drives the Approve control agrees with the write path that would
        # refuse it, and with the review screen that already refused the target.
        # Like the session term beside it, only an explicit False removes
        # approvability: None is "the gate supplied no verdict", not "no", and
        # the write path is what fails closed on an unverifiable proposal.
        # R82.2 adds the SELECTED-TARGET approval gate on the same terms: a surface
        # that renders an Approve affordance from this field must not offer one while
        # the risk-policy review (or any other gate the write path enforces on the
        # frozen selection) withholds it.
        "approvable": bool(available and materiality["material"] and not ca_stale
                           and not superseded and not withheld
                           and not hold_current_book
                           and summ.get(
                               "reallocation_full_target_reviewable") is not False
                           and not _session_blocks_approval(freshness)
                           and not _gate_blocks_approval(approval_gate)),
        "freshness": dict(freshness or {}),
        "session_actionable": (None if not freshness
                               else bool(freshness.get("approval_allowed"))),
        # R82.2 — the gate's own word wins, because it is the more specific answer:
        # it knows about the frozen selection and the risk-policy ruling, and the
        # session term is already one of its own inputs.
        "approval_gate": (dict(approval_gate) if approval_gate else None),
        "approval_gate_owner": OWNER,
        "next_required_action": ((approval_gate or {}).get("next_required_action")
                                 or (freshness or {}).get("next_required_action")),
        "next_required_action_vocabulary": list(NEXT_ACTION_VOCAB),
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
                            portfolio_state_loader: Optional[Callable] = None,
                            latest_session: Optional[str] = None,
                            workflow_state: Optional[dict] = None,
                            enforce_session_freshness: bool = True) -> dict:
    """The read contract. READ-ONLY: reads the immutable proposal summary + the latest
    recorded decision and composes the separate portfolio-decision review lane (plus a
    read-only order-plan preview when the current proposal is APPROVED). Degrade-safe.

    R63: publishes the session-freshness verdict and withholds ``approvable`` when
    the bound session is no longer the one the workflow is ready to act on, so no
    surface can offer an Approve the write path would refuse."""
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

    # R63 — the session verdict this read publishes, resolved on the same terms as
    # the supersession block above: the PRODUCTION-DEFAULT read resolves it from
    # the canonical session owner, while an injected hermetic world stays a
    # constructed world unless the caller states its session. It can only ever
    # REMOVE approvability, and the write path fails closed independently.
    fresh = None
    if latest_session is not None or workflow_state is not None:
        fresh = decision_freshness(
            bound_session=eligible,
            latest_session=(latest_session if latest_session is not None
                            else latest_eligible_session(
                                workflow_state=workflow_state)))
    elif enforce_session_freshness and _loaded_summary_default:
        fresh = decision_freshness(
            bound_session=eligible, latest_session=latest_eligible_session())

    # R82.2 — the SELECTED-TARGET approval gate, resolved on the same terms as the
    # freshness verdict above: the PRODUCTION-DEFAULT read asks the gate, while an
    # injected hermetic world stays a constructed world. Like freshness it can only
    # ever REMOVE approvability, and the write path fails closed independently.
    #
    # Without it this lane reported `approvable: True` for the frozen 2026-09-25
    # MINIMUM_REPAIR while api.portfolio_decision's own write path was refusing that
    # approval at SELECTED_TARGET_REQUIRES_RISK_POLICY_REVIEW.
    gate = None
    if _loaded_summary_default and active_book_id:
        try:
            gate = selected_target_approval_gate(
                selection=load_target_selection(
                    active_book_id=active_book_id, eligible_market_date=eligible,
                    decision_dir=decision_dir),
                freshness=fresh,
                current_proposal_hash=proposal_summary.get(
                    "reallocation_proposal_hash"),
                decision_dir=decision_dir)
        except Exception:  # noqa: BLE001 - degrade-safe: a read never crashes here
            gate = None

    lane = derive_decision_state(has_active_book=bool(active_book_id),
                                 proposal_summary=proposal_summary,
                                 decision_record=decision_record,
                                 freshness=fresh, approval_gate=gate)

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
#: R54.4 — the operator-facing name of each PRODUCER. A surface states which
#: lane produced the standing decision; it never infers authority from the lane.
_PRODUCER_LABELS = {PROV_GOVERNED_DAILY_CYCLE: "Daily DRC",
                    PROV_GOVERNED_INTRADAY: "Governed intraday event"}

# --- Gate verdicts ---------------------------------------------------------- #
GATE_ELIGIBLE = "GOVERNED_INTRADAY_DECISION_ELIGIBLE"
GATE_WITHHELD = "INTRADAY_DECISION_WITHHELD"
#: R54.4 — the DAILY producer's own verdict words. The two gates ask different
#: questions of different evidence, so a daily row must not be stamped with the
#: intraday verdict literal: a reader inspecting a governed record has to be able
#: to see WHICH gate admitted it. The eligibility BOOLEAN is the shared contract
#: the writer actually enforces; these are the honest labels beside it.
DAILY_GATE_ELIGIBLE = "GOVERNED_DAILY_DECISION_ELIGIBLE"
DAILY_GATE_WITHHELD = "DAILY_DECISION_WITHHELD"
GATE_VERDICT_VOCAB = (GATE_ELIGIBLE, GATE_WITHHELD,
                      DAILY_GATE_ELIGIBLE, DAILY_GATE_WITHHELD)

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
#: Release 62.1.1 — the INTRADAY lane's own designed no-op, named.
#:
#: The intraday producer contract (see :func:`build_intraday_candidate`)
#: deliberately promotes only on a PRICED R47 outcome: concluding
#: ``CURRENT_NO_CHANGE`` for a SESSION is the session-terminal daily producer's
#: prerogative, so an intraday cycle whose reassessment concluded no change
#: carries no governed decision at all. That withholding is CORRECT and stays.
#:
#: What was wrong was the WORDS. With no target to inspect, the target and
#: economics checks all failed, and the operator was shown
#: ``TARGET_IDENTITY_MISMATCH`` + ``CANDIDATE_EVIDENCE_INCOMPLETE`` +
#: ``SWITCHING_ECONOMICS_INCOMPLETE`` — three defect reports for a chain with no
#: defect in it, on 2026-09-08 against a candidate whose evidence identity
#: matched the standing governed decision exactly. This code says the true
#: thing, once, and the checks that need a target are NOT_APPLICABLE instead of
#: failed. The verdict is unchanged: the cycle is still withheld.
WR_INTRADAY_NO_PRICED_TARGET = "INTRADAY_CYCLE_REACHED_NO_PRICED_TARGET"
#: Release 54.4 — the DAILY producer's own admissibility failure: the Daily
#: Research Cycle manifest this candidate claims is absent, non-terminal, or is
#: not the governed manifest of record for that session (Release 29.5). It is a
#: distinct concept from every intraday code above — reusing one of those would
#: describe an intraday condition that was never evaluated.
WR_DAILY_MANIFEST_NOT_GOVERNED = "DAILY_MANIFEST_NOT_GOVERNED"
WITHHELD_REASON_VOCAB = (
    WR_NO_ACTIVE_BOOK, WR_PORTFOLIO_IDENTITY_STALE, WR_MARKET_DATA_STALE,
    WR_OWNED_DATA_NOT_CONFIRMED, WR_POINT_IN_TIME, WR_RANKING_IDENTITY,
    WR_HOC_IDENTITY, WR_HOC_NOT_PERSISTED, WR_HOC_ARTIFACT_MISMATCH,
    WR_REASSESSMENT_IDENTITY, WR_TARGET_IDENTITY,
    WR_SWITCHING_ECONOMICS, WR_TRUE_BLOCKER, WR_CHANGE_WITHHELD,
    WR_SUPERSEDED, WR_DUPLICATE, WR_EXECUTION_PRECEDENCE,
    WR_EVIDENCE_INCOMPLETE, WR_INTRADAY_NO_PRICED_TARGET,
    WR_DAILY_MANIFEST_NOT_GOVERNED)

#: R62.1.1 — reason codes that describe a lane's DESIGNED behaviour rather than
#: a defect in the evidence. Named as a set so an operator surface can render a
#: correct no-op differently from a broken chain without classifying anything.
NON_DEFECT_WITHHELD_REASON_CODES = (WR_INTRADAY_NO_PRICED_TARGET, WR_DUPLICATE)

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


# --------------------------------------------------------------------------- #
# Release 62.1.1 — a check has THREE dispositions, not two.
#
# A condition that cannot apply to the lane being evaluated is not a failure and
# is not a pass. Before this release there was no way to say so, so every check
# that needs a priced target FAILED on a cycle that legitimately produced none —
# and the operator read three defect codes for a chain with no defect in it.
#
# NOT_APPLICABLE is never inferred from an absent value: only an explicit lane
# declaration selects it (see :func:`_no_priced_target_lane`), and a check that
# is applicable and unproven still FAILS and still withholds.
# --------------------------------------------------------------------------- #
CHECK_PASSED = "PASSED"
CHECK_FAILED = "FAILED"
CHECK_NOT_APPLICABLE = "NOT_APPLICABLE_TO_THIS_LANE"
CHECK_DISPOSITION_VOCAB = (CHECK_PASSED, CHECK_FAILED, CHECK_NOT_APPLICABLE)


def _check(group: str, name: str, passed: bool, owner: str, detail: str,
           reason_code: Optional[str] = None,
           applicable: bool = True,
           not_applicable_because: Optional[str] = None) -> dict:
    applicable = bool(applicable)
    if not applicable:
        disposition = CHECK_NOT_APPLICABLE
    else:
        disposition = CHECK_PASSED if passed else CHECK_FAILED
    return {"group": group, "check": name,
            "passed": bool(passed) if applicable else None,
            "applicable": applicable,
            "disposition": disposition,
            "disposition_vocabulary": list(CHECK_DISPOSITION_VOCAB),
            "not_applicable_because": (None if applicable
                                       else not_applicable_because),
            "owner": owner, "detail": detail,
            "reason_code": (None if (passed or not applicable) else reason_code)}


def _gate_counts(checks: list) -> dict:
    """The arithmetic of a gate result, and a NON-VACUOUS proof that it closes.

    Every count is tallied INDEPENDENTLY from the checks themselves - passed,
    failed and not-applicable are three separate scans - and the two identities

        applicable = passed + failed
        total      = applicable + not_applicable

    are then ASSERTED over those independent tallies rather than derived by
    subtraction. Deriving one count from another would make the flag true by
    construction and prove nothing; tallying separately is what makes a check
    that is neither passed, failed nor excused - or one quietly counted twice -
    show up as ``counts_are_closed: False``.
    """
    total = len(checks)
    na = sum(1 for c in checks if not c.get("applicable", True))
    passed = sum(1 for c in checks
                 if c.get("applicable", True) and c.get("passed"))
    failed = sum(1 for c in checks
                 if c.get("applicable", True) and not c.get("passed"))
    applicable = sum(1 for c in checks if c.get("applicable", True))
    return {"checks_total": total,
            "checks_applicable": applicable,
            "checks_passed": passed,
            "checks_failed": failed,
            "checks_not_applicable": na,
            "check_disposition_vocabulary": list(CHECK_DISPOSITION_VOCAB),
            "counts_are_closed": (passed + failed == applicable
                                  and applicable + na == total)}


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
# R54.4 — the ONE governed-decision IDENTITY and DECISION-WORD contract.
#
# Both producers (the session-terminal Daily Research Cycle and the live
# intraday event cycle) describe the same business concept, so they must spell
# its identity and its conclusion the SAME way. These helpers are the single
# spelling: two producers that observed the same evidence therefore compute the
# same ``candidate_identity_hash`` and the same decision word by construction,
# which is what makes the writer's duplicate-detection meaningful across lanes.
# --------------------------------------------------------------------------- #
def _merge_reassessment_provenance(reassessment: Optional[dict]) -> dict:
    """The reassessment owner's OWN published identity map for its evidence.

    ``proposal_binding`` is authoritative ("the provenance a proposal generated
    by this reassessment MUST carry"); the artifact identity and the free
    provenance block are read only where it is silent. Nothing is derived.
    """
    rs = reassessment or {}
    prov = dict(rs.get("proposal_binding") or {})
    for fallback in ((rs.get("artifact") or {}).get("identity") or {},
                     rs.get("provenance") or {}):
        for k, v in (fallback or {}).items():
            if prov.get(k) is None and v is not None:
                prov[k] = v
    return prov


def _governed_identity(*, portfolio_state: Optional[dict],
                       event_cycle: Optional[dict],
                       reassessment: Optional[dict],
                       proposal_summary: Optional[dict],
                       provenance_map: dict,
                       scoring_identity: Optional[dict],
                       outcome: Any,
                       hoc_artifact_id: Any,
                       hoc_evidence_hash: Any) -> dict:
    """THE evidence identity of a governed portfolio decision.

    Covers the EVIDENCE only. It deliberately excludes every producer-specific
    accident — the event cycle's run id, the DRC run id, wall clocks and the
    materiality trigger fingerprint — because two producers that reach the same
    conclusion from the same evidence made the SAME decision, and re-deciding it
    would be churn dressed as governance. ``event_cycle`` is simply absent for
    the daily producer; each field then falls through to the same owner the
    intraday path would have used as its fallback.
    """
    ps = portfolio_state or {}
    ev = event_cycle or {}
    rs = reassessment or {}
    summ = proposal_summary or {}
    prov = provenance_map or {}
    sc = scoring_identity or {}
    return {
        "active_book_id": ((ps.get("active_book") or {}).get("book_id")
                           or ev.get("active_book_id")),
        "eligible_market_session": ((ps.get("dates") or {}).get(
            "eligible_market_date") or ev.get("eligible_market_date")),
        # The hashes the EVIDENCE was built against — the reassessment owner's
        # own bound portfolio-state hash and the hash the producer bound — NOT a
        # re-read of the live state. The gate's job is precisely to prove those
        # still describe the portfolio that exists now.
        "portfolio_state_hash": (prov.get("portfolio_state_hash")
                                 or ps.get("state_hash")),
        # R54.4 — the reassessment's OWN bound economic identity is the final
        # fallback, so a producer that performs no live portfolio read (the
        # daily one) still records the same economic axis the intraday producer
        # would have. Without it the two lanes could describe identical evidence
        # with different identities and fail to collapse to one decision.
        "economic_state_hash": (ev.get("portfolio_state_hash")
                                or ps.get("economic_state_hash")
                                or prov.get("economic_state_hash")),
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


def _reassessment_state_word(reassessment: Optional[dict]) -> Optional[str]:
    """The reassessment owner's own verdict word, wherever it published it."""
    rs = reassessment or {}
    inner = rs.get("reassessment")
    return (rs.get("state") or rs.get("reassessment_state") or rs.get("decision")
            or (inner.get("reassessment_state") if isinstance(inner, dict)
                else None))


def _governed_decision_word(*, reassessment_state: Any,
                            outcome: Any) -> Optional[str]:
    """The ONE mapping from the owners' own words to a governed decision word.

    ``CURRENT_NO_CHANGE`` is the reassessment owner's conclusion that the
    current portfolio remains the best use of capital and NO proposal was
    requested; it outranks the target outcome because in that case there is no
    target to speak for. Otherwise the target owner's own R47 outcome decides.
    ``None`` means "not a conclusive portfolio answer" — never a default.
    """
    if str(reassessment_state or "") == "CURRENT_NO_CHANGE":
        return GD_NO_CHANGE
    if outcome == _OUTCOME_PROPOSAL_READY:
        return GD_CHANGE_RECOMMENDED
    if outcome == _OUTCOME_HOLD:
        return GD_HOLD_CURRENT_BOOK
    return None


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
                             observation_provenance: Any = None,
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
    # provenance block are read only where it is silent. R54.4 — ONE spelling,
    # shared verbatim with the daily producer.
    prov = _merge_reassessment_provenance(rs)

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

    # R54.4 — the ONE evidence-identity contract, shared verbatim with the daily
    # producer so identical evidence yields an identical hash in either lane.
    identity = _governed_identity(
        portfolio_state=ps, event_cycle=ev, reassessment=rs,
        proposal_summary=summ, provenance_map=prov, scoring_identity=sc,
        outcome=outcome, hoc_artifact_id=hoc_artifact_id,
        hoc_evidence_hash=hoc_evidence_hash)
    ident_hash = candidate_identity_hash(identity)

    # The INTRADAY producer contract deliberately promotes only on a PRICED R47
    # outcome. It does NOT map the reassessment owner's CURRENT_NO_CHANGE word to
    # a governed decision: intraday "nothing to do" is the absence of a new
    # authoritative answer, not a new one. Concluding CURRENT_NO_CHANGE for a
    # SESSION is the session-terminal daily producer's prerogative — see
    # ``_governed_decision_word`` and ``build_daily_cycle_candidate``.
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
            # R55.2 — WHERE the observation stamp came from. Copied verbatim
            # from the caller, which is the only party that knows; the latency
            # owner labels the interval a pipeline latency only for an
            # observation THIS cycle admitted, and an age otherwise.
            "observation_provenance": observation_provenance,
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
# Release 62.1.1 — THE NO-PRICED-TARGET LANE
# --------------------------------------------------------------------------- #
#: The reassessment owner's own word for "the current portfolio remains the best
#: use of capital and NO target was requested". Spelled once, read from the
#: owner's payload, never inferred from an absent artifact.
_REASSESS_CURRENT_NO_CHANGE = "CURRENT_NO_CHANGE"


def _no_priced_target_lane(*, candidate: dict, reassessment: Optional[dict],
                           evidence: dict, identity: dict) -> dict:
    """Did this intraday cycle legitimately produce NO priced target?

    FAIL-CLOSED, and deliberately conjunctive. Every one of the three facts
    below is a POSITIVE declaration by an OWNER; not one of them is "the
    artifact is missing". A cycle whose target failed to compute, or whose
    reassessment was BLOCKED_DATA or BLOCKED_EVIDENCE, matches none of them - so
    every target and economics check stays applicable and still fails exactly as
    it did before.

        1. the REASSESSMENT owner concluded CURRENT_NO_CHANGE;
        2. the CYCLE owner recorded ``proposal_built: False``;
        3. the CYCLE owner recorded that a reassessment DID run (so this is a
           completed conclusion, not an abandoned chain).

    A BOUND PROPOSAL HASH is deliberately NOT one of them. A cycle that asked
    for no target and bound one anyway is precisely the stale-artifact
    substitution this release exists to catch, and it must be caught INSIDE this
    lane by ``PROPOSAL_BINDING_CONSISTENT`` - as a named TARGET_IDENTITY_MISMATCH
    - rather than quietly routed back into the priced lane where a bound hash
    satisfies ``TARGET_HASH_BOUND`` and the substitution passes unremarked.
    """
    rs = reassessment or {}
    state = str(rs.get("state") or evidence.get("reassessment_state") or "")
    reasons = {
        "reassessment_concluded_no_change": state == _REASSESS_CURRENT_NO_CHANGE,
        "cycle_built_no_proposal": evidence.get("proposal_built") is False,
        "cycle_ran_a_reassessment": evidence.get("reassessment_ran") is True,
    }
    active = all(reasons.values())
    return {
        "no_priced_target_lane": active,
        "lane_facts": reasons,
        "proposal_hash_bound": bool(identity.get("proposal_hash")),
        "reassessment_state": state or None,
        "because": (
            "the reassessment owner concluded %s and the cycle owner recorded "
            "that it built no proposal, so there is no priced target for the "
            "target and economics checks to inspect. The intraday producer "
            "contract promotes only on a priced R47 outcome - a session's "
            "no-change conclusion belongs to the daily producer - so this "
            "cycle is still WITHHELD, for that reason and not for a defect."
            % _REASSESS_CURRENT_NO_CHANGE) if active else
            ("a priced target is expected of this cycle; every target and "
             "economics condition is applicable and unproven ones fail"),
        "verdict_is_unchanged_by_this_classification": True,
        "owner": GOVERNANCE_GATE_OWNER,
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

    # R62.1.1 — WHICH LANE is being evaluated, decided from four positive owner
    # declarations before a single condition is scored. Nothing below infers it.
    lane = _no_priced_target_lane(candidate=cand, reassessment=rs,
                                  evidence=ev, identity=ident)
    _target_applies = not lane["no_priced_target_lane"]
    _lane_note = lane["because"]

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
    # R62.1.1 — the intraday lane's own designed no-op, stated ONCE as its own
    # applicable, failing condition. It is what actually withholds a
    # CURRENT_NO_CHANGE cycle, and saying it here is what lets the seven
    # target/economics conditions below stop reporting a defect that is not one.
    checks.append(_check(
        "TARGET_IDENTITY", "INTRADAY_CANDIDATE_HAS_A_PRICED_TARGET",
        bool(cand.get("decision")), GOVERNANCE_GATE_OWNER,
        "candidate decision %s; %s" % (cand.get("decision") or "NONE", _lane_note),
        WR_INTRADAY_NO_PRICED_TARGET))
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
        conclusive_reason,
        applicable=_target_applies, not_applicable_because=_lane_note))

    checks.append(_check(
        "TARGET_IDENTITY", "TARGET_HASH_BOUND", bool(ident.get("proposal_hash")),
        "api.reallocation_proposal", "proposal hash %s" % ident.get("proposal_hash"),
        WR_TARGET_IDENTITY,
        applicable=_target_applies, not_applicable_because=_lane_note))
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
        WR_TARGET_IDENTITY,
        applicable=_target_applies, not_applicable_because=_lane_note))
    # R62.1.1 — the DAILY gate's binding rule, applied to the intraday lane so
    # both producers spell it the same way: a cycle that requested no target
    # must bind NO proposal. Binding one would launder a stale artifact into a
    # conclusion that never asked for it, and THAT is a real identity mismatch.
    # It replaces TARGET_HASH_BOUND in the no-target lane, so in every lane
    # exactly one of the two is applicable and the binding is never unchecked.
    checks.append(_check(
        "TARGET_IDENTITY", "PROPOSAL_BINDING_CONSISTENT",
        not ident.get("proposal_hash"), "api.reallocation_proposal",
        "a no-target cycle binds proposal %s" % (ident.get("proposal_hash"),),
        WR_TARGET_IDENTITY,
        applicable=not _target_applies,
        not_applicable_because=("a priced-target cycle MUST bind its proposal; "
                                "TARGET_HASH_BOUND is the applicable rule")))

    # --- G. CHURN / ECONOMIC CONTROLS (bound, never re-decided) ------------- #
    required_econ = ("switching_hurdle", "clears_switching_hurdle",
                     "one_way_turnover", "estimated_transaction_cost",
                     "concentration_before", "concentration_after",
                     "score_improvement_net_of_cost")
    missing_econ = [k for k in required_econ if econ.get(k) is None]
    # R62.1.1 — switching ECONOMICS price a switch. With no priced target there
    # is no switch to price, so these four are NOT_APPLICABLE to the no-target
    # lane rather than four more defect reports about a chain with no defect.
    checks.append(_check(
        "ECONOMIC_CONTROLS", "SWITCHING_ECONOMICS_COMPLETE", not missing_econ,
        "engine.constrained_reallocation.switching_economics",
        "missing: %s" % (missing_econ or "none"), WR_SWITCHING_ECONOMICS,
        applicable=_target_applies, not_applicable_because=_lane_note))
    checks.append(_check(
        "ECONOMIC_CONTROLS", "RISK_BEFORE_AND_AFTER_PRICED",
        ("portfolio_volatility_before" in econ and "portfolio_volatility_after" in econ),
        "engine.constrained_reallocation",
        "volatility before/after published by the target owner",
        WR_SWITCHING_ECONOMICS,
        applicable=_target_applies, not_applicable_because=_lane_note))
    checks.append(_check(
        "ECONOMIC_CONTROLS", "TURNOVER_BUDGET_EVALUATED",
        bool((con.get("constraint_inventory") or {}).get("constraints")
             or econ.get("one_way_turnover") is not None),
        "engine.constrained_reallocation",
        "one-way turnover %s" % econ.get("one_way_turnover"), WR_SWITCHING_ECONOMICS,
        applicable=_target_applies, not_applicable_because=_lane_note))
    checks.append(_check(
        "ECONOMIC_CONTROLS", "ZERO_BASE_INCUMBENCY_POLICY_INTACT",
        ((cand.get("zero_base") or {}).get("incumbency_policy")
         == ZERO_BASE_INCUMBENCY_POLICY
         and not (cand.get("zero_base") or {}).get("current_holdings_privileged")),
        "engine.constrained_reallocation",
        "incumbency policy %s" % (cand.get("zero_base") or {}).get("incumbency_policy"),
        WR_SWITCHING_ECONOMICS,
        applicable=_target_applies, not_applicable_because=_lane_note))
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

    failed = [c for c in checks if c["applicable"] and not c["passed"]]
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
    counts = _gate_counts(checks)
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
        # R62.1.1 — WHICH LANE this cycle was evaluated as, and why. A
        # classification, never a verdict: the lane changes which conditions
        # APPLY, and never whether an applicable one passed.
        "evaluation_lane": lane,
        "withheld_reasons": reasons,
        "withheld_reason_codes": [r["code"] for r in reasons],
        "withheld_reason_vocabulary": list(WITHHELD_REASON_VOCAB),
        "failing_checks": [c["check"] for c in failed],
        "not_applicable_checks": [c["check"] for c in checks
                                  if not c["applicable"]],
        "checks": checks,
        **counts,
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


def load_persisted_daily_decision(*, active_book_id: Optional[str],
                                  eligible_market_session: Optional[str],
                                  decision_dir=None) -> Optional[dict]:
    """R54.4 — the persisted DAILY governed row for one book and session.

    Answers exactly one question: "did the daily producer already write a real
    ledger row for this session?". It is what retires the legacy read-time
    projection. Pure reader; never raises, never writes; ``None`` when the
    session predates R54.4 or the daily write was withheld.
    """
    if not eligible_market_session:
        return None
    want = str(eligible_market_session)[:10]
    try:
        records = _load_json(_governed_records_path(decision_dir)) or []
        if not isinstance(records, list):
            return None
        hits = [
            r for r in records
            if r.get("provenance") == PROV_GOVERNED_DAILY_CYCLE
            and str(r.get("eligible_market_session")
                    or (r.get("identity") or {}).get(
                        "eligible_market_session") or "")[:10] == want
            and (active_book_id is None
                 or str(r.get("active_book_id")
                        or (r.get("identity") or {}).get("active_book_id")
                        or "") == str(active_book_id))
        ]
        if not hits:
            return None
        return max(hits, key=governed_decision_ordering_key)
    except Exception:  # noqa: BLE001 - a pure read must never crash the caller
        return None


#: Release 61 — why the legacy projection may not stand as an authority the
#: governance gate compares a candidate against.
#:
#: ``project_governed_daily_cycle_decision`` builds its identity from the
#: reassessment and proposal it is HANDED. The intraday gate hands it the very
#: reassessment and proposal the candidate under evaluation is built from, so
#: the projection's core evidence equalled the candidate's BY CONSTRUCTION and
#: ``CANDIDATE_ADDS_NEW_EVIDENCE`` could never pass: every genuinely new
#: evidence-bearing intraday candidate was refused as ``DUPLICATE_CANDIDATE``.
#:
#: The R54.4 rule that prevents this already exists and the READ has always
#: applied it — a real daily ledger row RETIRES the projection for that session,
#: because two descriptions of one decision must never both be candidates for
#: authority. The gate declared parity with that read in a comment and did not
#: perform it. This resolver is that rule, in ONE place, used by both.
PROJECTION_RETIRED_BY_LEDGER_ROW = "LEGACY_DAILY_PROJECTION_RETIRED_BY_LEDGER_ROW"


def resolve_standing_governed_decision(*, persisted: Optional[dict],
                                       projected: Optional[dict],
                                       decision_dir=None) -> dict:
    """THE standing governed authority, and what it was chosen over.

    Pure selection over two candidate descriptions under the ONE ordering
    function, with the R54.4 retirement applied first. Reads only the governed
    ledger (through :func:`load_persisted_daily_decision`); writes nothing.
    """
    projection_suppressed = False
    if projected and projected.get("decision"):
        pid = projected.get("identity") or {}
        row = load_persisted_daily_decision(
            active_book_id=(projected.get("active_book_id")
                            or pid.get("active_book_id")),
            eligible_market_session=projected.get("eligible_market_session"),
            decision_dir=decision_dir)
        if row:
            projection_suppressed = True
            projected = None
    rows = [r for r in (persisted, projected) if r and r.get("decision")]
    standing = (max(rows, key=governed_decision_ordering_key) if rows else None)
    return {
        "standing": standing,
        "persisted_record_present": bool(persisted),
        "projected_daily_cycle_present": bool(projected),
        "legacy_daily_projection_suppressed": projection_suppressed,
        "suppression_reason": (PROJECTION_RETIRED_BY_LEDGER_ROW
                               if projection_suppressed else None),
        "resolved_by": GOVERNANCE_GATE_OWNER,
    }


#: R61 — the endpoints that only the INTRADAY lane can own. A producer that
#: declares ``intraday_latency_applicable: False`` never had an observation to
#: receive or an event cycle to start, so an absent stamp there is a fact about
#: the lane, not a broken chain.
INTRADAY_ONLY_LATENCY_STAGES = ("observation_received_at",
                                "event_cycle_started_at")

#: R62.1.1 — the R61 statement, COMPLETED. Every endpoint above is an EVENT
#: CYCLE concept, and so are these three: ``api.event_signal_refresh``'s own
#: ``stage_step_map`` resolves each of them to a step of an event cycle
#: (REFRESH_AFFECTED_INPUTS, PORTFOLIO_REASSESSMENT, REALLOCATION_PROPOSAL). A
#: session-terminal daily decision runs no event cycle, so it has none of them —
#: exactly as it has no observation and no cycle start. R61 named two of the
#: five and left three behind, which is why the Sep-8 daily record's own
#: ``interval_dispositions`` said MISSING for three intervals while its
#: ``missing_measurements`` was empty: two halves of one true statement,
#: disagreeing. The daily lane's OWN latency (governance gate -> persistence) is
#: unaffected and is still MEASURED on its own stamps.
#:
#: This is not a licence to excuse a stage that ran. ``measure_decision_latency``
#: excuses only UNSTAMPED endpoints, and an INTRADAY decision never reaches this
#: list at all: it does own every endpoint, so an absent stamp there is a real
#: gap and stays MISSING.
EVENT_CYCLE_ONLY_LATENCY_STAGES = ("signal_refresh_completed_at",
                                   "reassessment_completed_at",
                                   "target_completed_at")

#: The full set a producer with no event cycle structurally never had.
DAILY_LANE_ABSENT_LATENCY_STAGES = (INTRADAY_ONLY_LATENCY_STAGES
                                    + EVENT_CYCLE_ONLY_LATENCY_STAGES)


def _not_required_latency_stages(latency_inputs: Optional[dict]) -> list:
    """Which latency endpoints the PRODUCER proved it legitimately never ran.

    Only the producer may excuse a stage (the R55.1 rule), and this reads that
    declaration rather than inferring one: a producer that says nothing excuses
    nothing, and every unstamped endpoint stays MISSING.
    """
    li = latency_inputs or {}
    if li.get("intraday_latency_applicable") is False:
        return list(DAILY_LANE_ABSENT_LATENCY_STAGES)
    return []


def record_governed_decision(*, candidate: dict, gate: dict,
                             provenance: str = PROV_GOVERNED_INTRADAY,
                             confirm: Optional[str] = None,
                             decision_dir=None, actor: Optional[str] = None,
                             now: Optional[datetime] = None) -> dict:
    """Append ONE governed portfolio decision. Writes nothing else, ever.

    R54.4 — this is the SINGLE writer for BOTH producers. The daily
    session-terminal cycle and the live intraday cycle each run their own
    admissibility gate and then persist here; ``provenance`` is the only thing
    that distinguishes their rows. There is no second writer, ledger or ordering.

    Fail-closed and idempotent:
      * the gate must have declared the candidate eligible (the intraday gate's
        ``GOVERNED_INTRADAY_DECISION_ELIGIBLE`` or the daily gate's
        ``GOVERNED_DAILY_DECISION_ELIGIBLE``);
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
        # Echo the refusing gate's OWN verdict word so a daily refusal is not
        # reported with the intraday literal.
        return {**base, "status": (gate or {}).get("verdict") or GATE_WITHHELD,
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
            observation_provenance=li.get("observation_provenance"),
            event_cycle_processing_seconds=li.get("cycle_duration_seconds"),
            governance_gate_completed_at=(gate or {}).get("evaluated_at"),
            governed_decision_persisted_at=ts,
            # R61 — the PRODUCER's own declaration reaches the latency owner.
            # The daily lane already said ``intraday_latency_applicable:
            # False`` — it processes no observation and runs no event cycle, so
            # those two endpoints do not exist for it — but the declaration was
            # never passed on, so both came back MISSING and the Active Manager
            # acceptance read 9/10 with LATENCY MISSING against a decision that
            # structurally never had them. This is NOT a backfill: no timestamp
            # is invented, and a stage that DID stamp is still measured on its
            # own evidence (``measure_decision_latency`` excuses only unstamped
            # endpoints). An intraday decision, which does own both, is
            # unaffected and still reports MISSING when either is absent.
            not_required_stages=_not_required_latency_stages(li))
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
        observation_provenance: Any = None,
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
        observation_received_at=observation_received_at,
        observation_provenance=observation_provenance, now=now)
    # The STANDING authority is the later of the persisted governed record and
    # the projected DRC-governed decision, under the ONE ordering — the same
    # resolution the read performs. Comparing against only the persisted record
    # would let an intraday candidate supersede a NEWER daily-cycle decision.
    persisted_standing = load_governed_decision_record(
        active_book_id=(candidate.get("identity") or {}).get("active_book_id"),
        decision_dir=decision_dir)
    projected_standing = project_governed_daily_cycle_decision(
        workflow=wf, reassessment=rs, proposal_summary=summ, constrained=con)
    # R61 — resolved through the ONE resolver, so the gate performs the same
    # R54.4 retirement the read performs instead of merely claiming to. The
    # projection is built from THIS candidate's own reassessment and proposal;
    # left standing it is a self-comparison no new evidence can ever beat.
    standing_resolution = resolve_standing_governed_decision(
        persisted=persisted_standing, projected=projected_standing,
        decision_dir=decision_dir)
    standing = standing_resolution["standing"]
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
        # R61 — WHICH description of the standing decision the gate compared
        # against, and whether the legacy projection was retired by a real
        # ledger row. An operator reading DUPLICATE_CANDIDATE needs to know what
        # the candidate was said to duplicate.
        "standing_decision_resolution": {
            k: v for k, v in standing_resolution.items() if k != "standing"},
        "warnings": warnings,
        "safety": _governed_safety(),
    }


# =========================================================================== #
# R54.4 — THE DAILY PRODUCER CONTRACT
# =========================================================================== #
r"""Why the Daily Research Cycle stopped being its own decision owner.

Before R54.4 there was ONE business concept — the governed portfolio decision —
with TWO persistence realities:

  * INTRADAY decisions were APPENDED, by this module, to an immutable governed
    ledger with an identity hash, a supersession lineage and a gate record.
  * The DAILY session-terminal decision was never written down at all. It lived
    only inside ``api.daily_research_cycle``'s run manifest and was RE-DERIVED
    at every single read by ``project_governed_daily_cycle_decision`` from three
    separately mutable inputs (the workflow's research-cycle state, the current
    reassessment and the current proposal summary).

That asymmetry is the defect. A decision that is recomputed on read is not a
decision the system ever MADE — it is a decision the system keeps re-making,
and it silently changes retroactively whenever any upstream input moves. It has
no record id, so nothing can name it in ``supersedes_decision_id``; the intraday
writer had to rebuild the projection just to discover what it was superseding.

R54.4 makes the daily cycle a PRODUCER, exactly like the intraday cycle: it
computes the research and the evidence, then DELEGATES the governed write to
this module. Producer is not authority. There is now one writer
(:func:`record_governed_decision`), one ledger, one ordering and one history,
and ``provenance`` records which lane produced each row.

The projection survives ONLY as a read-only LEGACY compatibility shim for
sessions that completed before this release and therefore have no ledger row.
It is suppressed the moment a persisted daily record exists for that session —
see :func:`load_governed_portfolio_decision`. No history is rewritten and no
historical row is fabricated.
"""

# --------------------------------------------------------------------------- #
# RELEASE 55.1 — THE TERMINAL INTRADAY GOVERNANCE DISPOSITION.
#
# Every event cycle relevant to governance must end with an OWNER-ISSUED
# terminal disposition. Before this release the gate recorded a verdict only
# when it was actually invoked, and every other outcome — including the correct
# and extremely common one, "no candidate existed, so no verdict was required" —
# reached the operator as an absence. The acceptance contract could not tell
# "the gate declined to promote" from "the gate never ran", so it reported
# GOVERNANCE MISSING on a chain that had in fact completed successfully.
#
# This classifier lives in the ONE governance authority because the question it
# answers — WAS A GOVERNANCE VERDICT REQUIRED, AND WHAT WAS IT — is this
# module's question. It is a pure classification of facts the gate and the
# cycle owner ALREADY recorded. It runs no gate, reaches no verdict of its own,
# writes nothing, opens no store, creates no ledger and manufactures no
# timestamp. NOT_REQUIRED is a valid terminal disposition; it is never turned
# into a fake evaluation, and no governed row is ever written to make an
# acceptance row green.
# --------------------------------------------------------------------------- #
#: A governance verdict was not required: the cycle terminated before a
#: portfolio reassessment candidate could exist. A successful terminal no-op.
GOV_DISP_NOT_REQUIRED = "NOT_REQUIRED_NO_NEW_INFORMATION"
#: The gate evaluated a real candidate and concluded no promotion was warranted.
GOV_DISP_EVALUATED_NO_PROMOTION = "EVALUATED_NO_PROMOTION"
#: The gate evaluated a candidate and withheld it; the exact reasons travel.
GOV_DISP_WITHHELD = "WITHHELD"
#: The gate promoted the candidate to a governed decision through the ONE writer.
GOV_DISP_PROMOTED = "PROMOTED"
#: The system cannot prove what happened. Never inferred away, never excused.
GOV_DISP_INCOMPLETE = "INCOMPLETE"

GOVERNANCE_DISPOSITION_VOCAB = (
    GOV_DISP_NOT_REQUIRED, GOV_DISP_EVALUATED_NO_PROMOTION, GOV_DISP_WITHHELD,
    GOV_DISP_PROMOTED, GOV_DISP_INCOMPLETE,
)

#: The four dispositions that PROVE what happened. INCOMPLETE deliberately is
#: not among them: acceptance stays fail-closed on an unproven cycle.
GOVERNANCE_TERMINAL_DISPOSITIONS = (
    GOV_DISP_NOT_REQUIRED, GOV_DISP_EVALUATED_NO_PROMOTION, GOV_DISP_WITHHELD,
    GOV_DISP_PROMOTED,
)

#: Why the disposition is what it is. One code per provable cause.
GOV_REASON_NO_CANDIDATE = "NO_REASSESSMENT_CANDIDATE_PRODUCED"
GOV_REASON_PROMOTED = "CANDIDATE_PROMOTED_TO_GOVERNED_DECISION"
GOV_REASON_WITHHELD = "GATE_WITHHELD_CANDIDATE"
GOV_REASON_NO_PROMOTION = "GATE_EVALUATED_AND_DID_NOT_PROMOTE"
GOV_REASON_GATE_NOT_INVOKED = "GATE_NOT_INVOKED_AFTER_REASSESSMENT"
GOV_REASON_GATE_SILENT = "GATE_INVOKED_WITHOUT_RECORDED_VERDICT"
GOV_REASON_NO_CYCLE = "NO_EVENT_CYCLE_RECORDED"
GOV_REASON_UNPROVEN = "CYCLE_RECORDS_NO_TERMINAL_GOVERNANCE_FACTS"

GOVERNANCE_REASON_VOCAB = (
    GOV_REASON_NO_CANDIDATE, GOV_REASON_PROMOTED, GOV_REASON_WITHHELD,
    GOV_REASON_NO_PROMOTION, GOV_REASON_GATE_NOT_INVOKED, GOV_REASON_GATE_SILENT,
    GOV_REASON_NO_CYCLE, GOV_REASON_UNPROVEN,
)

#: The gate's own invocation rule, stated once here by the module that owns it.
#: ``api.event_signal_refresh`` calls the gate if and only if the cycle produced
#: a reassessment candidate, so "no candidate" is a governance answer, not a gap.
INTRADAY_GATE_INVOCATION_CONTRACT = (
    "api.event_signal_refresh invokes the R54.1 governed-decision gate if and "
    "only if the event cycle produced a portfolio reassessment candidate. A "
    "cycle that terminated at NO_NEW_INFORMATION, INFORMATION_NOT_MATERIAL or "
    "DUPLICATE_TRIGGER_SUPPRESSED therefore required no governance verdict, and "
    "its absence is the contract being honoured rather than a stage that failed."
)

_GOV_REASON_DETAIL = {
    GOV_REASON_NO_CANDIDATE: (
        "The materiality gate concluded the portfolio question did not need "
        "asking, so no reassessment candidate existed for governance to rule on."),
    GOV_REASON_PROMOTED: (
        "The gate admitted the candidate and the ONE governed writer persisted "
        "it as the latest governed portfolio decision."),
    GOV_REASON_WITHHELD: (
        "The gate evaluated the candidate and withheld governance; the failing "
        "checks and reason codes are carried verbatim."),
    GOV_REASON_NO_PROMOTION: (
        "The gate evaluated the candidate and did not promote it. The standing "
        "governed decision remains authoritative."),
    GOV_REASON_GATE_NOT_INVOKED: (
        "This cycle produced a reassessment candidate but never reached the "
        "governance gate, so no verdict exists to report. The chain is "
        "incomplete: what the gate would have concluded is unproven."),
    GOV_REASON_GATE_SILENT: (
        "This cycle reached the governance gate but recorded no verdict, so "
        "what the gate concluded is unproven."),
    GOV_REASON_NO_CYCLE: (
        "No event cycle is recorded, so there is nothing to govern and nothing "
        "to prove."),
    GOV_REASON_UNPROVEN: (
        "The cycle records no terminal governance facts, so whether a verdict "
        "was required cannot be established."),
}


def classify_intraday_governance(*, event_cycle: Optional[dict]) -> dict:
    """THE terminal governance disposition of ONE event cycle.

    Pure classification of what the gate and the cycle owner already recorded:
    the gate's own verdict block (written by :func:`govern_latest_intraday_
    assessment` into the cycle payload) and the cycle owner's own
    ``state`` / ``reassessment_ran`` / ``governance_gate_invoked`` facts.

    Fail-closed by construction. Only two things can yield a NOT_REQUIRED
    disposition — a cycle state in which no candidate can exist AND an explicit
    ``reassessment_ran is False``. Everything unproven is INCOMPLETE, which
    acceptance reports as MISSING. This function writes nothing, evaluates no
    gate rule, and stamps no clock.
    """
    cyc = event_cycle or {}
    gd = cyc.get("governed_decision") or {}
    state = str(cyc.get("state") or "") or None
    ran = cyc.get("reassessment_ran")
    invoked = cyc.get("governance_gate_invoked")
    has_cycle = bool(cyc.get("run_id") or state)

    # The gate's own recorded verdict always wins: it is the owner speaking.
    withheld = list(gd.get("withheld_reason_codes") or [])
    failing = list(gd.get("failing_checks") or [])
    if gd.get("recorded"):
        disposition, reason = GOV_DISP_PROMOTED, GOV_REASON_PROMOTED
    elif gd.get("evaluated"):
        if withheld or failing:
            disposition, reason = GOV_DISP_WITHHELD, GOV_REASON_WITHHELD
        else:
            disposition, reason = (GOV_DISP_EVALUATED_NO_PROMOTION,
                                   GOV_REASON_NO_PROMOTION)
    elif not has_cycle:
        disposition, reason = GOV_DISP_INCOMPLETE, GOV_REASON_NO_CYCLE
    elif state in _no_candidate_cycle_states() and ran is False:
        disposition, reason = GOV_DISP_NOT_REQUIRED, GOV_REASON_NO_CANDIDATE
    elif ran is True:
        # A candidate existed, so a verdict WAS required. Name which of the two
        # provable causes left it absent, rather than reporting a bare gap.
        reason = (GOV_REASON_GATE_SILENT if invoked
                  else GOV_REASON_GATE_NOT_INVOKED)
        disposition = GOV_DISP_INCOMPLETE
    else:
        disposition, reason = GOV_DISP_INCOMPLETE, GOV_REASON_UNPROVEN

    required = (None if disposition == GOV_DISP_INCOMPLETE
                and reason in (GOV_REASON_NO_CYCLE, GOV_REASON_UNPROVEN)
                else disposition != GOV_DISP_NOT_REQUIRED)
    return {
        "schema_version": "intraday_governance_disposition.v1",
        "phase": "R55.1",
        "owner": GOVERNANCE_GATE_OWNER,
        "gate_version": GOVERNANCE_GATE_VERSION,
        "disposition": disposition,
        "disposition_vocabulary": list(GOVERNANCE_DISPOSITION_VOCAB),
        "terminal_dispositions": list(GOVERNANCE_TERMINAL_DISPOSITIONS),
        "terminal": disposition in GOVERNANCE_TERMINAL_DISPOSITIONS,
        "required": required,
        "evaluated": bool(gd.get("evaluated")),
        "gate_invoked_by_cycle": invoked,
        "reason": reason,
        "reason_vocabulary": list(GOVERNANCE_REASON_VOCAB),
        "reason_detail": _GOV_REASON_DETAIL.get(reason),
        "event_cycle_run_id": cyc.get("run_id"),
        "event_cycle_state": state,
        "reassessment_ran": ran,
        "candidate_reassessment_id": cyc.get("reassessment_id"),
        "candidate_reassessment_hash": cyc.get("reassessment_hash"),
        "candidate_identity_hash": gd.get("candidate_identity_hash"),
        "candidate_decision": gd.get("decision"),
        "verdict": gd.get("verdict"),
        "promoted_to_governed": bool(gd.get("recorded")),
        "promotion_decision_id": gd.get("record_id"),
        "withheld_reason_codes": withheld,
        "failing_checks": failing,
        # Only a stamp an owner actually recorded. Never manufactured, and
        # deliberately absent for a cycle that required no verdict.
        "at": (cyc.get("generated_at")
               if disposition in GOVERNANCE_TERMINAL_DISPOSITIONS else None),
        "invocation_contract": INTRADAY_GATE_INVOCATION_CONTRACT,
        "decided_here": False,
        "classifies_only": True,
        "recomputes_nothing": True,
        "writes_nothing": True,
        "creates_no_governed_row": True,
        "safety": _governed_safety(),
    }


def _no_candidate_cycle_states() -> tuple:
    """The cycle owner's OWN list of states in which no candidate can exist.

    Read from ``api.event_signal_refresh`` so the two modules can never drift
    into disagreeing about what a terminal no-op cycle is. A missing owner
    degrades to an empty tuple, which makes every cycle unproven — fail-closed,
    never fail-open.
    """
    try:
        from paper_trader.api import event_signal_refresh as esr
        return tuple(esr.NO_CANDIDATE_CYCLE_STATES)
    except Exception:  # noqa: BLE001 — an unreadable owner proves nothing
        return ()


# --------------------------------------------------------------------------- #
# RELEASE 55.1 — HOW THE AUTHORITATIVE GOVERNED DECISION IS HELD.
#
# ``persisted`` alone was reported next to a real ``record_id``, which read as a
# contradiction. It was in fact truthful: a session that closed BEFORE R54.4
# has no ledger row and is served by the read-time legacy projection. These
# tokens make the same fact self-describing, and separate "is a ledger row"
# from "is retrievable through the canonical owner right now".
# --------------------------------------------------------------------------- #
#: A real row in the ONE governed ledger, written by the ONE governed writer.
DECISION_PERSISTENCE_LEDGER_ROW = "LEDGER_ROW"
#: Retrievable and authoritative, but reconstructed at read time from the
#: Release-29.5 governed-evidence contract because the session predates R54.4.
DECISION_PERSISTENCE_LEGACY_PROJECTION = "LEGACY_COMPATIBILITY_PROJECTION"
#: R55.2.2 — retrievable, authoritative, and NOT a ledger row for a session whose
#: daily cycle ran under the DELEGATING producer contract. The decision was
#: genuinely reached; the governed write did not complete. Distinguished from the
#: legitimate legacy projection above because this one is a real defect.
DECISION_PERSISTENCE_UNPERSISTED = "POST_CUTOVER_NOT_PERSISTED"
#: No governed decision at all.
DECISION_PERSISTENCE_ABSENT = "ABSENT"
DECISION_PERSISTENCE_VOCAB = (DECISION_PERSISTENCE_LEDGER_ROW,
                              DECISION_PERSISTENCE_LEGACY_PROJECTION,
                              DECISION_PERSISTENCE_UNPERSISTED,
                              DECISION_PERSISTENCE_ABSENT)

#: R55.2.2 — the ONE blocker code a post-cutover unpersisted governed daily
#: decision raises. Named here, by the decision owner, so no surface invents it.
GOVERNED_DAILY_NOT_PERSISTED_BLOCKER = "GOVERNED_DAILY_DECISION_NOT_PERSISTED"

# --------------------------------------------------------------------------- #
# R55.2.2 — THE CUTOVER, stated as recorded provenance rather than a clock read.
#
# A session is POST-CUTOVER when its own daily manifest declares that the
# producer delegates its governed terminal decision
# (``research_cycle_state.governed_decision_delegation``, written by
# ``api.daily_research_cycle`` since R55.2.2). That declaration is authoritative
# and needs no fallback.
#
# Manifests persisted BEFORE that declaration existed cannot answer for
# themselves, so exactly one recorded fact resolves them: the release at which
# the daily producer began delegating, and the first eligible market session
# whose cycle ran after it. Both are historical facts of this repository — commit
# c0df3b1 (R54.4) landed 2026-09-03T16:14:04Z, the 2026-09-02 cycle completed
# 2026-09-02T23:51:52Z under the previous contract, and the 2026-09-03 cycle
# started 2026-09-04T01:59:33Z under the new one. The boundary is therefore
# fixed, auditable and independent of "today"; it never moves, and it becomes
# inert once no readable session predates the declaration.
# --------------------------------------------------------------------------- #
GOVERNED_DAILY_WRITE_CUTOVER_RELEASE = "c0df3b1"
GOVERNED_DAILY_WRITE_CUTOVER_SESSION = "2026-09-03"
GOVERNED_DAILY_WRITE_CUTOVER_BASIS = (
    "PRODUCER_DECLARATION (the manifest's own governed_decision_delegation), "
    "falling back for pre-declaration manifests to the recorded release "
    "boundary: R54.4 / %s, first delegating session %s."
    % (GOVERNED_DAILY_WRITE_CUTOVER_RELEASE, GOVERNED_DAILY_WRITE_CUTOVER_SESSION))


def governed_daily_write_expected(*, eligible_market_session: Optional[str] = None,
                                  delegation: Optional[dict] = None) -> dict:
    """Was this session's daily cycle EXPECTED to write a governed ledger row?

    Pure; no io; no clock. The producer's own declaration decides it when the
    manifest carries one, and the recorded release boundary resolves manifests
    written before the declaration existed. A session that cannot be dated at all
    is treated as PRE-cutover — the conservative answer, because inventing an
    expectation would turn unknown history into a fabricated defect.
    """
    if isinstance(delegation, dict) and delegation.get("delegates_terminal_decision"):
        return {"expected_ledger_row": True,
                "cutover_basis": "PRODUCER_DECLARATION",
                "producer_declaration": dict(delegation)}
    session = str(eligible_market_session)[:10] if eligible_market_session else None
    if session:
        return {"expected_ledger_row": session >= GOVERNED_DAILY_WRITE_CUTOVER_SESSION,
                "cutover_basis": "RECORDED_RELEASE_BOUNDARY",
                "cutover_release": GOVERNED_DAILY_WRITE_CUTOVER_RELEASE,
                "cutover_session": GOVERNED_DAILY_WRITE_CUTOVER_SESSION}
    return {"expected_ledger_row": False, "cutover_basis": "SESSION_UNKNOWN",
            "cutover_session": GOVERNED_DAILY_WRITE_CUTOVER_SESSION}


_PERSISTENCE_DETAIL = {
    DECISION_PERSISTENCE_LEDGER_ROW: (
        "A row in the one governed decision ledger, written by the one governed "
        "writer in api.portfolio_decision."),
    DECISION_PERSISTENCE_LEGACY_PROJECTION: (
        "Authoritative and retrievable, but not a ledger row: this session "
        "completed before R54.4 made the daily cycle delegate its governed "
        "write, so the decision is reconstructed at read time from the "
        "Release-29.5 governed-evidence contract. History is not rewritten and "
        "the row is never backfilled; the next governed cycle writes a real one."),
    DECISION_PERSISTENCE_UNPERSISTED: (
        "Authoritative and retrievable, but NOT a ledger row for a session whose "
        "daily cycle ran under the delegating producer contract, so a governed "
        "row was expected. The decision was genuinely reached; the governed "
        "write did not complete. The gap is PRESERVED, never backfilled — the "
        "repair is that the next governed cycle writes a real row."),
    DECISION_PERSISTENCE_ABSENT: "No governed portfolio decision exists.",
}


def classify_decision_persistence(*, record: Optional[dict],
                                  available: Optional[bool] = None) -> dict:
    """How the authoritative governed decision is currently held.

    Pure classification of the record this module just resolved. Retrievability
    is derived from the canonical owner's own read — never from a UI, a browser
    or a file probe by a caller.

    R55.2.2 — a read-time projection is no longer one thing. For a session that
    predates the delegating producer it is a legitimate compatibility shim; for
    one that ran under it, the same shape is a missing governed write, and
    reporting both as ``LEGACY_COMPATIBILITY_PROJECTION`` is exactly what let a
    genuinely new session pass acceptance while its ledger row was absent.
    """
    rec = record or {}
    has = bool(rec.get("decision")) if available is None else bool(available)
    projected = bool(rec.get("legacy_compatibility_projection")
                     or rec.get("projected"))
    expectation = governed_daily_write_expected(
        eligible_market_session=rec.get("eligible_market_session"),
        delegation=rec.get("producer_governed_write_delegation"))
    if not has:
        status = DECISION_PERSISTENCE_ABSENT
    elif not projected:
        status = DECISION_PERSISTENCE_LEDGER_ROW
    elif expectation["expected_ledger_row"]:
        status = DECISION_PERSISTENCE_UNPERSISTED
    else:
        status = DECISION_PERSISTENCE_LEGACY_PROJECTION
    return {
        "persistence_status": status,
        "persistence_status_vocabulary": list(DECISION_PERSISTENCE_VOCAB),
        "persistence_detail": _PERSISTENCE_DETAIL.get(status),
        "is_ledger_row": status == DECISION_PERSISTENCE_LEDGER_ROW,
        # The decision IS retrievable through this owner in every state but
        # ABSENT. This is what "persisted" was being misread as.
        "retrievable_through_owner": status != DECISION_PERSISTENCE_ABSENT,
        "retrievability_owner": GOVERNANCE_GATE_OWNER,
        "backfilled": False,
        "history_rewritten": False,
        # R55.2.2 — the cutover answer, published beside the status so every
        # surface reads ONE owner's verdict instead of re-deriving it.
        "expected_ledger_row": bool(has and expectation["expected_ledger_row"]),
        "cutover_basis": expectation.get("cutover_basis"),
        "cutover_contract": GOVERNED_DAILY_WRITE_CUTOVER_BASIS,
        "persistence_blocker": (GOVERNED_DAILY_NOT_PERSISTED_BLOCKER
                                if status == DECISION_PERSISTENCE_UNPERSISTED
                                else None),
        "historical_gap_preserved": status == DECISION_PERSISTENCE_UNPERSISTED,
    }


#: The daily producer's contract version, recorded on every daily row.
DAILY_PRODUCER_CONTRACT_VERSION = "daily_cycle_decision_producer.v1"

#: ``api.daily_research_cycle``'s OWN terminal-complete words, reused verbatim.
#: Only a manifest in one of these states is governed evidence (Release 29.5).
DAILY_TERMINAL_COMPLETE_STATES = ("COMPLETE", "COMPLETE_WITH_EVIDENCE_GAP")


def build_daily_cycle_candidate(*, portfolio_state: Optional[dict],
                                drc_manifest: Optional[dict],
                                reassessment: Optional[dict],
                                proposal_summary: Optional[dict],
                                constrained: Optional[dict] = None,
                                scoring_identity: Optional[dict] = None,
                                hoc_binding: Optional[dict] = None,
                                now: Optional[datetime] = None) -> dict:
    """Assemble ONE governed-decision candidate from the DAILY producer's output.

    Pure and io-free, and deliberately the same shape the intraday producer
    emits: it shares the identity contract (:func:`_governed_identity`) and the
    decision-word contract (:func:`_governed_decision_word`) verbatim, so the
    same evidence observed by either lane yields the same
    ``candidate_identity_hash`` and the same conclusion. That is what lets the
    ONE writer recognise a daily and an intraday candidate as the SAME decision
    instead of appending two authorities for one session.

    ``drc_manifest`` is ``api.daily_research_cycle``'s own run record — the
    producer hands over its evidence rather than this module reaching into the
    research store, so the read path gains no dependency on the cycle owner.
    """
    ps = portfolio_state or {}
    man = drc_manifest or {}
    rs = reassessment or {}
    summ = proposal_summary or {}
    con = constrained or {}
    prov = _merge_reassessment_provenance(rs)
    hb = dict(hoc_binding or {})

    outcome = con.get("outcome") or summ.get("reallocation_outcome")
    rs_state = _reassessment_state_word(rs) or man.get(
        "portfolio_reassessment_state")
    decision = _governed_decision_word(reassessment_state=rs_state,
                                       outcome=outcome)

    # R54.3 parity — the EXACT immutable opportunity-cost version. Strongest
    # available proof first; nothing is defaulted to present.
    hoc_artifact_id = (hb.get("hoc_artifact_id")
                       or man.get("opportunity_cost_artifact_id")
                       or prov.get("hoc_artifact_id"))
    hoc_evidence_hash = (hb.get("hoc_assessment_evidence_hash")
                         or prov.get("hoc_assessment_evidence_hash"))
    hoc_persisted = hb.get("hoc_persisted")
    if hoc_persisted is None:
        hoc_persisted = prov.get("hoc_persisted")

    identity = _governed_identity(
        portfolio_state=ps, event_cycle=None, reassessment=rs,
        proposal_summary=summ, provenance_map=prov,
        scoring_identity=scoring_identity, outcome=outcome,
        hoc_artifact_id=hoc_artifact_id, hoc_evidence_hash=hoc_evidence_hash)
    # A session-terminal daily decision belongs to the SESSION AND BOOK THE RUN
    # WAS FOR, which the manifest recorded — not to whatever session a live
    # portfolio-state read happens to be on when the write is made. The manifest
    # is therefore the anchor of record here, and the live state is the fallback.
    # (In the normal case they are the same two facts; when they are not, the
    # run's own session is the honest answer and the evidence checks below prove
    # the reassessment belongs to it.)
    if man.get("active_book_id"):
        identity["active_book_id"] = man.get("active_book_id")
    if man.get("eligible_market_date"):
        identity["eligible_market_session"] = man.get("eligible_market_date")
    if identity.get("hoc_assessment_hash") is None:
        identity["hoc_assessment_hash"] = man.get(
            "opportunity_cost_assessment_hash")
    if identity.get("reassessment_hash") is None:
        identity["reassessment_hash"] = man.get("portfolio_reassessment_hash")
    if identity.get("reassessment_id") is None:
        identity["reassessment_id"] = man.get("portfolio_reassessment_id")
    # R54.2.3.2 parity, and the reason the DAILY producer reads the MANIFEST
    # rather than the live proposal key: an UNREQUESTED proposal's hash and
    # outcome must never enter the governed identity. The manifest is the run's
    # own record of which proposal (if any) it built; whatever artifact happens
    # to sit at the live key may belong to an entirely different cycle, and
    # binding it here would launder a stale target into a decision that never
    # asked for one. A CURRENT_NO_CHANGE decision requested no target at all.
    man_proposal_hash = man.get("reallocation_proposal_hash") or None
    man_proposal_id = man.get("reallocation_proposal_id") or None
    binds_proposal = decision in (GD_CHANGE_RECOMMENDED, GD_HOLD_CURRENT_BOOK)
    identity["proposal_hash"] = man_proposal_hash if binds_proposal else None
    identity["proposal_id"] = man_proposal_id if binds_proposal else None
    identity["target_outcome"] = outcome if binds_proposal else None
    ident_hash = candidate_identity_hash(identity)

    book = identity.get("active_book_id")
    eligible = identity.get("eligible_market_session")

    # Position-level recommendations are the proposal owner's OWN actions,
    # verbatim. A governed HOLD / CURRENT_NO_CHANGE carries none: in the first
    # case the priced target is precisely the one the system declined, and in
    # the second no target was ever requested.
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

    # The decision instant is the EVIDENCE's own stamp, never this process's
    # wall clock: no arbitrary clock race may decide capital authority.
    decided_at = ((rs.get("artifact") or {}).get("generated_at")
                  if isinstance(rs.get("artifact"), dict) else None)
    decided_at = decided_at or man.get("completed_at") or _now_iso(now)

    return {
        "owner": GOVERNANCE_GATE_OWNER,
        "gate_version": GOVERNANCE_GATE_VERSION,
        "producer_contract_version": DAILY_PRODUCER_CONTRACT_VERSION,
        "candidate_identity_hash": ident_hash,
        "candidate_id": "gcand_%s_%s_%s" % (eligible or "nodate",
                                            book or "book", ident_hash[:12]),
        "identity": identity,
        "decision": decision,
        "decision_vocabulary": list(GOVERNED_DECISION_VOCAB),
        "position_recommendations": recommendations,
        "position_recommendation_vocabulary": list(POSITION_RECOMMENDATION_VOCAB),
        "position_recommendation_note": (
            "A governed HOLD or CURRENT_NO_CHANGE carries no position "
            "recommendations." if decision != GD_CHANGE_RECOMMENDED else
            "Read verbatim from the proposal owner's own allocation actions."),
        "switching_economics": dict(con.get("switching_economics") or {}),
        "evidence": {
            "daily_cycle_run_id": man.get("run_id"),
            "daily_cycle_state": man.get("state"),
            "daily_cycle_completed_at": man.get("completed_at"),
            "daily_cycle_session_contract_hash": man.get("session_contract_hash"),
            "daily_cycle_input_contract_hash": man.get("input_contract_hash"),
            "manifest_reassessment_id": man.get("portfolio_reassessment_id"),
            "manifest_reassessment_hash": man.get("portfolio_reassessment_hash"),
            "manifest_reassessment_state": man.get("portfolio_reassessment_state"),
            "manifest_proposal_id": man.get("reallocation_proposal_id"),
            "manifest_proposal_hash": man.get("reallocation_proposal_hash"),
            "manifest_proposal_state": man.get("reallocation_proposal_state"),
            "manifest_hoc_artifact_id": man.get("opportunity_cost_artifact_id"),
            "manifest_hoc_assessment_hash": man.get(
                "opportunity_cost_assessment_hash"),
            "hoc_persisted": hoc_persisted,
            "hoc_artifact_retrievable": hb.get("hoc_artifact_retrievable"),
            "hoc_artifact_identity_matches": hb.get("hoc_artifact_identity_matches"),
            "hoc_binding_detail": hb.get("hoc_binding_detail"),
            "hoc_binding_owner": (hb.get("hoc_binding_resolved_by")
                                  or hb.get("hoc_owner")),
            "reassessment_state": rs_state,
            "proposal_data_gaps": list(summ.get("reallocation_data_gaps") or []),
            "governed_daily_cycle_evidence_current": bool(
                str(man.get("state") or "") in DAILY_TERMINAL_COMPLETE_STATES),
            "producer_owner": "api.daily_research_cycle",
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
        # The daily lane measures no intraday latency: there is no observation
        # -> decision race to measure. The field is named, never invented.
        "latency_inputs": {"measurement_owner": "api.daily_research_cycle",
                           "intraday_latency_applicable": False},
        "decided_at": decided_at,
        "provenance": PROV_GOVERNED_DAILY_CYCLE,
        "manual_review_required": bool(decision == GD_CHANGE_RECOMMENDED),
        "safety": _governed_safety(),
    }


def evaluate_daily_cycle_governance(*, candidate: Optional[dict],
                                    drc_manifest: Optional[dict],
                                    portfolio_state: Optional[dict] = None,
                                    reassessment: Optional[dict] = None,
                                    proposal_summary: Optional[dict] = None,
                                    constrained: Optional[dict] = None,
                                    current_governed: Optional[dict] = None
                                    ) -> dict:
    """The DAILY admissibility gate. Same machinery, same vocabulary, same
    verdict shape as the intraday gate — a different QUESTION.

    The intraday gate asks "is this live reassessment complete, fresh and bound
    tightly enough to replace the standing recommendation?". The daily gate asks
    the session-terminal question instead: "is this a VALIDATED terminal-COMPLETE
    Daily-Research-Cycle manifest whose bound reassessment and opportunity-cost
    artifacts actually exist and actually belong to it?".

    It is not a second governance framework: it decides ADMISSIBILITY only,
    computes no economics, and reuses the canonical withheld taxonomy. It is
    fail-closed — anything it cannot prove withholds the write.
    """
    cand = candidate or {}
    man = drc_manifest or {}
    ident = cand.get("identity") or {}
    rs = reassessment or {}
    summ = proposal_summary or {}
    con = constrained or {}
    ev = cand.get("evidence") or {}
    checks: list[dict] = []

    # --- The governed manifest of record --------------------------------- #
    state = str(man.get("state") or "")
    checks.append(_check(
        "DAILY_MANIFEST", "MANIFEST_PRESENT", bool(man),
        "api.daily_research_cycle",
        "manifest %s" % (man.get("run_id") or "NONE"),
        WR_DAILY_MANIFEST_NOT_GOVERNED))
    checks.append(_check(
        "DAILY_MANIFEST", "MANIFEST_TERMINAL_COMPLETE",
        state in DAILY_TERMINAL_COMPLETE_STATES, "api.daily_research_cycle",
        "manifest state = %s (terminal-complete = %s)"
        % (state or "NONE", list(DAILY_TERMINAL_COMPLETE_STATES)),
        WR_DAILY_MANIFEST_NOT_GOVERNED))
    checks.append(_check(
        "DAILY_MANIFEST", "MANIFEST_RUN_IDENTIFIED", bool(man.get("run_id")),
        "api.daily_research_cycle", "run_id = %s" % (man.get("run_id") or "NONE"),
        WR_DAILY_MANIFEST_NOT_GOVERNED))
    # The candidate is anchored to the manifest's session, so comparing the two
    # would be tautological. The binding that actually has to hold is the
    # CROSS-OWNER one: the reassessment this decision stands on must belong to
    # the session the run was for.
    checks.append(_check(
        "DAILY_MANIFEST", "MANIFEST_SESSION_MATCHES_EVIDENCE",
        _eq_when_known(str(man.get("eligible_market_date") or "")[:10] or None,
                       str(rs.get("eligible_market_date") or "")[:10]
                       or None) is not False,
        "api.portfolio_reassessment",
        "manifest session %s vs reassessment session %s"
        % (man.get("eligible_market_date"), rs.get("eligible_market_date")),
        WR_DAILY_MANIFEST_NOT_GOVERNED))
    checks.append(_check(
        "DAILY_MANIFEST", "MANIFEST_BOOK_MATCHES_CANDIDATE",
        _eq_when_known(man.get("active_book_id"),
                       ident.get("active_book_id")) is not False,
        "api.daily_research_cycle",
        "manifest book %s vs candidate %s"
        % (man.get("active_book_id"), ident.get("active_book_id")),
        WR_PORTFOLIO_IDENTITY_STALE))

    # --- Portfolio identity ---------------------------------------------- #
    checks.append(_check(
        "PORTFOLIO_IDENTITY", "ACTIVE_BOOK_PRESENT",
        bool(ident.get("active_book_id")), "api.portfolio_state",
        "active book = %s" % (ident.get("active_book_id") or "NONE"),
        WR_NO_ACTIVE_BOOK))
    checks.append(_check(
        "PORTFOLIO_IDENTITY", "ELIGIBLE_SESSION_PRESENT",
        bool(ident.get("eligible_market_session")), "api.portfolio_state",
        "eligible session = %s"
        % (ident.get("eligible_market_session") or "NONE"),
        WR_EVIDENCE_INCOMPLETE))

    # --- The reassessment this decision stands on ------------------------- #
    checks.append(_check(
        "EVIDENCE", "REASSESSMENT_BOUND",
        bool(ident.get("reassessment_hash")) and bool(ident.get("reassessment_id")),
        "api.portfolio_reassessment",
        "reassessment %s / %s" % (ident.get("reassessment_id"),
                                  ident.get("reassessment_hash")),
        WR_REASSESSMENT_IDENTITY))
    checks.append(_check(
        "EVIDENCE", "REASSESSMENT_MATCHES_MANIFEST",
        _eq_when_known(man.get("portfolio_reassessment_hash"),
                       ident.get("reassessment_hash")) is not False,
        "api.daily_research_cycle",
        "manifest reassessment %s vs candidate %s"
        % (man.get("portfolio_reassessment_hash"),
           ident.get("reassessment_hash")),
        WR_REASSESSMENT_IDENTITY))

    # --- R54.3 parity: the EXACT opportunity-cost artifact ---------------- #
    checks.append(_check(
        "EVIDENCE", "HOC_ARTIFACT_BOUND", bool(ident.get("hoc_artifact_id")),
        "api.holding_opportunity_cost",
        "hoc artifact = %s" % (ident.get("hoc_artifact_id") or "NONE"),
        WR_HOC_NOT_PERSISTED))
    checks.append(_check(
        "EVIDENCE", "HOC_ARTIFACT_RETRIEVABLE",
        ev.get("hoc_artifact_retrievable") is True,
        "api.holding_opportunity_cost.resolve_binding",
        "retrievable = %s (%s)" % (ev.get("hoc_artifact_retrievable"),
                                   ev.get("hoc_binding_detail") or "no detail"),
        WR_HOC_NOT_PERSISTED))
    checks.append(_check(
        "EVIDENCE", "HOC_ARTIFACT_IDENTITY_MATCHES",
        ev.get("hoc_artifact_identity_matches") is not False,
        "api.holding_opportunity_cost.resolve_binding",
        "identity matches = %s" % (ev.get("hoc_artifact_identity_matches"),),
        WR_HOC_ARTIFACT_MISMATCH))
    checks.append(_check(
        "EVIDENCE", "HOC_MATCHES_MANIFEST",
        _eq_when_known(man.get("opportunity_cost_assessment_hash"),
                       ident.get("hoc_assessment_hash")) is not False,
        "api.daily_research_cycle",
        "manifest hoc %s vs candidate %s"
        % (man.get("opportunity_cost_assessment_hash"),
           ident.get("hoc_assessment_hash")),
        WR_HOC_IDENTITY))

    # --- The conclusion --------------------------------------------------- #
    decision = cand.get("decision")
    checks.append(_check(
        "DECISION", "DECISION_IS_CONCLUSIVE",
        decision in GOVERNED_DECISION_VOCAB, GOVERNANCE_GATE_OWNER,
        "decision = %s" % (decision or "NONE"), WR_EVIDENCE_INCOMPLETE))
    outcome = ident.get("target_outcome")
    checks.append(_check(
        "DECISION", "NOT_A_TRUE_BLOCKER", outcome != _OUTCOME_TRUE_BLOCKER,
        "engine.constrained_reallocation", "target outcome = %s" % (outcome,),
        WR_TRUE_BLOCKER))
    # A CHANGE must name the exact proposal it recommends; a HOLD or
    # CURRENT_NO_CHANGE must bind none — binding a stale artifact to a decision
    # that did not ask for one is precisely how R54.2.3.2's defect was launched.
    if decision == GD_CHANGE_RECOMMENDED:
        # A CHANGE must name a proposal, and it must be THIS RUN's proposal.
        proposal_ok = (bool(ident.get("proposal_hash"))
                       and _eq_when_known(man.get("reallocation_proposal_hash"),
                                          ident.get("proposal_hash")) is not False)
        detail = ("CHANGE binds proposal %s (manifest %s)"
                  % (ident.get("proposal_hash"),
                     man.get("reallocation_proposal_hash")))
    elif decision == GD_HOLD_CURRENT_BOOK:
        # A HOLD priced a feasible alternative and declined it, so a bound
        # proposal is legitimate — but it must still be the run's own.
        proposal_ok = _eq_when_known(man.get("reallocation_proposal_hash") or None,
                                     ident.get("proposal_hash")) is not False
        detail = ("HOLD binds the priced-and-declined proposal %s (manifest %s)"
                  % (ident.get("proposal_hash"),
                     man.get("reallocation_proposal_hash")))
    else:
        # CURRENT_NO_CHANGE requested no target; binding one would launder a
        # stale artifact into a decision that never asked for it.
        proposal_ok = not ident.get("proposal_hash")
        detail = ("%s binds no proposal (bound = %s)"
                  % (decision, ident.get("proposal_hash")))
    checks.append(_check(
        "DECISION", "PROPOSAL_BINDING_CONSISTENT", proposal_ok,
        "api.reallocation_proposal", detail, WR_TARGET_IDENTITY))

    # --- Supersession: never overwrite a newer authority ------------------ #
    standing = current_governed or {}
    dup = bool(standing and standing.get("candidate_identity_hash")
               == cand.get("candidate_identity_hash"))
    outranks = True
    if standing and standing.get("decision") and not dup:
        outranks = (governed_decision_ordering_key(cand)
                    > governed_decision_ordering_key(standing))
    checks.append(_check(
        "SUPERSESSION", "STRICTLY_OUTRANKS_STANDING_DECISION",
        dup or outranks, GOVERNANCE_GATE_OWNER,
        "standing = %s @ %s (duplicate = %s)"
        % (standing.get("record_id"), standing.get("decided_at"), dup),
        WR_SUPERSEDED))

    # --- Structural safety ------------------------------------------------ #
    safety = cand.get("safety") or {}
    checks.append(_check(
        "SAFETY", "CHANGE_IS_RECOMMENDATION_ONLY",
        (decision != GD_CHANGE_RECOMMENDED
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

    failed = [c for c in checks if c["applicable"] and not c["passed"]]
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
    counts = _gate_counts(checks)
    return {
        "owner": GOVERNANCE_GATE_OWNER,
        "gate_version": GOVERNANCE_GATE_VERSION,
        "producer_contract_version": DAILY_PRODUCER_CONTRACT_VERSION,
        "producer": PROV_GOVERNED_DAILY_CYCLE,
        "verdict": (DAILY_GATE_ELIGIBLE if eligible_verdict
                    else DAILY_GATE_WITHHELD),
        "verdict_vocabulary": list(GATE_VERDICT_VOCAB),
        "eligible": eligible_verdict,
        "candidate_id": cand.get("candidate_id"),
        "candidate_identity_hash": cand.get("candidate_identity_hash"),
        "candidate_decision": decision,
        "duplicate_of_standing_decision": dup,
        "withheld_reasons": reasons,
        "withheld_reason_codes": [r["code"] for r in reasons],
        "withheld_reason_vocabulary": list(WITHHELD_REASON_VOCAB),
        "failing_checks": [c["check"] for c in failed],
        "not_applicable_checks": [c["check"] for c in checks
                                  if not c["applicable"]],
        "checks": checks,
        **counts,
        "evaluated_at": _now_iso(None),
        "economics_owner": "engine.constrained_reallocation",
        "gate_decides_economics": False,
        "safety": _governed_safety(),
    }


def govern_daily_cycle_decision(*, confirm: Optional[str] = None,
                                drc_manifest: Optional[dict] = None,
                                portfolio_state: Optional[dict] = None,
                                reassessment: Optional[dict] = None,
                                proposal_summary: Optional[dict] = None,
                                constrained: Optional[dict] = None,
                                scoring_identity: Optional[dict] = None,
                                hoc_binding: Optional[dict] = None,
                                decision_dir=None, reallocation_dir=None,
                                hoc_dir=None, reassessment_dir=None,
                                loaders: Optional[dict] = None,
                                now: Optional[datetime] = None) -> dict:
    """THE call ``api.daily_research_cycle`` delegates its governed write to.

    Mirror image of :func:`govern_latest_intraday_assessment`: build the
    candidate, run the DAILY gate, and persist through the ONE writer only if it
    passes. Every owner read is an injectable seam, and a failure in any single
    owner degrades to a WITHHELD verdict rather than a crash or a fabricated
    decision.

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

    # The DAILY producer deliberately does NOT re-read live portfolio state.
    # Its evidence is the terminal manifest plus the immutable reassessment the
    # run bound; the anchors come from the manifest and the identity hashes come
    # from the reassessment's own binding. Re-reading the live document here
    # would make a session-terminal decision depend on whatever the portfolio
    # looks like at write time — and, in a hermetic run, would reach a
    # production store the caller had explicitly pinned away from.
    ps = portfolio_state
    man = drc_manifest or {}

    def _book() -> Any:
        return (((ps or {}).get("active_book") or {}).get("book_id")
                or man.get("active_book_id"))

    def _session() -> Any:
        return (((ps or {}).get("dates") or {}).get("eligible_market_date")
                or man.get("eligible_market_date"))

    def _anchor() -> dict:
        """The book + session this run decided for, taken from its manifest.

        Handed to the owner reads below so they resolve THIS session's artifacts
        without each independently re-loading the live portfolio-state document
        (a full desk-ledger replay). That read is both expensive and, in a
        hermetic run, a fall-through to exactly the production store the caller
        pinned away from. Whatever the reads return is still validated against
        the manifest by the gate, so the anchor grants nothing.
        """
        return {"active_book": {"book_id": _book()},
                "dates": {"eligible_market_date": _session()}}

    def _load_reassessment():
        # The reassessment store is an injectable seam: a hermetic caller that
        # passes a research root must never fall through to the production one.
        from paper_trader.api import portfolio_reassessment as prs
        return prs.load_portfolio_reassessment(
            portfolio_state=ps if ps is not None else _anchor(),
            reassessment_dir=reassessment_dir)

    def _load_summary():
        return realloc.load_proposal_summary(
            active_book_id=_book(), eligible_market_date=_session(),
            reallocation_dir=reallocation_dir)

    def _load_constrained():
        return realloc.load_constrained_reallocation(
            portfolio_state=ps if ps is not None else _anchor(),
            reallocation_dir=reallocation_dir, decision_dir=decision_dir)

    rs = _get("reassessment", reassessment, _load_reassessment)
    summ = _get("proposal_summary", proposal_summary, _load_summary)
    # Transition economics belong to a PROPOSAL. A run whose manifest names none
    # built no target, so there is nothing to price and the (expensive) target
    # read is not performed — a CURRENT_NO_CHANGE decision must not carry, or
    # wait for, the economics of a transition that was never proposed.
    _built_a_proposal = bool(man.get("reallocation_proposal_hash")
                             or man.get("reallocation_proposal_id"))
    if constrained is not None or "constrained" in lds or _built_a_proposal:
        con = _get("constrained", constrained, _load_constrained)
    else:
        con = None
    # Like the portfolio state above, the scoring identity is HANDED OVER by the
    # producer (the run computed it) and never re-derived here: rebuilding it
    # would re-run the scoring engine against the canonical universe store —
    # slow in production, and in a hermetic run a read of exactly the store the
    # caller pinned away from. A producer that supplies none records none.
    sc = scoring_identity

    def _load_hoc_binding():
        """R54.3 parity — PROVE the opportunity-cost dependency through its owner.

        Lookup is by EXACT id, never "latest for the session", so a stale
        candidate can never be validated by a newer artifact that merely shares
        its session.
        """
        from paper_trader.api import holding_opportunity_cost as hocm
        prov = _merge_reassessment_provenance(rs)
        claimed = {
            "hoc_artifact_id": (man.get("opportunity_cost_artifact_id")
                                or prov.get("hoc_artifact_id")),
            "hoc_assessment_hash": (man.get("opportunity_cost_assessment_hash")
                                    or prov.get("hoc_assessment_hash")),
            "hoc_assessment_evidence_hash": prov.get(
                "hoc_assessment_evidence_hash"),
            "hoc_persisted": prov.get("hoc_persisted"),
            "hoc_active_book_id": _book(),
            "hoc_eligible_market_date": _session(),
        }
        return hocm.resolve_binding(
            binding=claimed, active_book_id=claimed["hoc_active_book_id"],
            eligible_market_date=claimed["hoc_eligible_market_date"],
            hoc_dir=hoc_dir)

    hb = _get("hoc_binding", hoc_binding, _load_hoc_binding)

    candidate = build_daily_cycle_candidate(
        portfolio_state=ps, drc_manifest=man, reassessment=rs,
        proposal_summary=summ, constrained=con, scoring_identity=sc,
        hoc_binding=hb, now=now)
    # The STANDING authority is the persisted ledger — the ONE history. The
    # legacy projection is NOT consulted here: it describes a session that was
    # never written, and this call is precisely what writes one.
    standing = load_governed_decision_record(
        active_book_id=(candidate.get("identity") or {}).get("active_book_id"),
        decision_dir=decision_dir)
    gate = evaluate_daily_cycle_governance(
        candidate=candidate, drc_manifest=man, portfolio_state=ps,
        reassessment=rs, proposal_summary=summ, constrained=con,
        current_governed=standing)

    persisted = None
    if gate.get("eligible"):
        persisted = record_governed_decision(
            candidate=candidate, gate=gate,
            provenance=PROV_GOVERNED_DAILY_CYCLE,
            confirm=GOVERNED_DECISION_CONFIRM_TOKEN,
            decision_dir=decision_dir, now=now)
    return {
        "owner": GOVERNANCE_GATE_OWNER,
        "gate_version": GOVERNANCE_GATE_VERSION,
        "producer": PROV_GOVERNED_DAILY_CYCLE,
        "producer_contract_version": DAILY_PRODUCER_CONTRACT_VERSION,
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
    """LEGACY READ-ONLY compatibility projection (see R54.4).

    Release 29.5 declares when a decision is governed by the Daily Research
    Cycle (``governed_research_evidence_current`` — a validated run manifest).
    Before R54.4 that decision was never written into this lane's ledger, so it
    had to be projected here — verbatim, marked ``persisted: False`` — purely so
    the ONE ordering function could compare it with an intraday promotion.

    Since R54.4 the daily cycle DELEGATES its governed write to this module
    (:func:`govern_daily_cycle_decision`), so a session run under the current
    runtime has a real ledger row. This projection therefore exists only to keep
    sessions that completed BEFORE R54.4 readable, and
    :func:`load_governed_portfolio_decision` suppresses it as soon as a
    persisted daily record exists for the same book and session. It writes
    nothing, fabricates no historical row and rewrites no history.
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
    if sup.get("superseded") and str(rs_state or "") != "CURRENT_NO_CHANGE":
        # The standing proposal is not this assessment's; its outcome projects
        # nothing. Fail closed rather than fabricate a decision word.
        decision = None
    else:
        # R54.4 — the SAME decision-word contract the daily producer writes with,
        # so the legacy projection and a persisted daily row can never disagree.
        decision = _governed_decision_word(reassessment_state=rs_state,
                                           outcome=outcome)
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
        # R54.4 — this row is a READ-ONLY shim, not a ledger row. It is retired
        # for any session the daily producer has since written.
        "legacy_compatibility_projection": True,
        # R55.2.2 — the PRODUCER'S OWN declaration, carried verbatim from the
        # manifest that this projection describes. It is what tells
        # ``classify_decision_persistence`` whether the absence of a ledger row
        # is legitimate history or a missing governed write, so the two are never
        # again reported with one word.
        "producer_governed_write_delegation": rcs.get(
            "governed_decision_delegation"),
        "projection_note": ("Projected from the Release-29.5 governed-evidence "
                            "contract for a session with no governed ledger row. "
                            "Legitimate for a session that predates the "
                            "delegating producer; a missing governed write for "
                            "one that ran under it. Not a ledger row, never "
                            "backfilled, and suppressed once a persisted daily "
                            "record exists."),
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
    # R54.4 — the legacy projection is a compatibility shim for sessions that
    # completed before the daily cycle delegated its write. The moment a real
    # daily ledger row exists for that book and session, the row IS the decision
    # and the projection is retired: two descriptions of one decision must never
    # both be candidates for authority.
    # R61 — the retirement rule lives in ONE function, shared verbatim with the
    # governance gate (see ``resolve_standing_governed_decision``).
    resolution = resolve_standing_governed_decision(
        persisted=persisted, projected=projected, decision_dir=decision_dir)
    projection_suppressed = resolution["legacy_daily_projection_suppressed"]
    projected = None if projection_suppressed else projected
    latest = resolution["standing"]
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
        # R55.1 — ``persisted: false`` beside a real record_id read as a defect.
        # It is truthful, and this says WHY in the owner's own words: whether the
        # decision is a ledger row or the read-time legacy projection, and that
        # it is retrievable through this owner either way.
        **classify_decision_persistence(record=latest, available=bool(latest)),
        "persisted_record_present": bool(persisted),
        "projected_daily_cycle_present": bool(projected),
        # R54.4 — true when a real daily ledger row retired the legacy
        # projection for that session (the forward-going state).
        "legacy_daily_projection_suppressed": projection_suppressed,
        "legacy_projection_note": (
            "The daily cycle delegates its governed write since R54.4; the "
            "read-time projection survives only for sessions completed before "
            "that release and is suppressed once a ledger row exists."),
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
    # R54.4 — DAILY and INTRADAY are PRODUCERS of one governed decision, never
    # competing authorities. Both write through the same writer and are ordered
    # by the same key: (eligible session, decided_at, provenance rank, identity
    # hash). Neither lane wins by being a lane.
    "4. producer (DAILY_DRC vs INTRADAY_EVENT) is provenance, never authority: "
    "both are ordered by (eligible session, decision timestamp, provenance rank, "
    "identity hash) under the ONE ordering function.",
    "5. on an EXACT tie of session AND decision timestamp, the session-terminal "
    "GOVERNED_DAILY_CYCLE outranks a GOVERNED_INTRADAY promotion, because the "
    "session-terminal cycle's evidence base strictly contains the intraday "
    "cycle's (full scoring refresh, opportunity cost, reassessment, proposal and "
    "forward evidence, versus a bounded event-driven reassessment). This is a "
    "deterministic TIE-BREAK only; it never reorders decisions that differ in "
    "time, so a later intraday decision still outranks an earlier daily one.",
    "6. identical evidence identity in either lane is the SAME decision: it is "
    "reused, never appended twice, so a daily and an intraday producer can never "
    "create two authorities for one body of evidence.",
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
        # R54.4 — WHICH PRODUCER produced the standing decision. Provenance is
        # reported truthfully and is never authority: the ordering above decides
        # that. Read verbatim from the governed lane; never re-derived here.
        "current_authoritative_decision_producer": gov.get("provenance"),
        "producer_vocabulary": list(GOVERNED_PROVENANCE_VOCAB),
        "producer_label": _PRODUCER_LABELS.get(gov.get("provenance")),
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
    # R62.1.1 — three check dispositions, the designed-no-op reason code and
    # the lane classification that selects between them.
    "CHECK_PASSED", "CHECK_FAILED", "CHECK_NOT_APPLICABLE",
    "CHECK_DISPOSITION_VOCAB", "WR_INTRADAY_NO_PRICED_TARGET",
    "NON_DEFECT_WITHHELD_REASON_CODES",
    "POSITION_RECOMMENDATION_VOCAB", "GOVERNED_DECISION_CONFIRM_TOKEN",
    "build_intraday_candidate", "evaluate_intraday_governance",
    "record_governed_decision", "load_governed_decision_record",
    "governed_decision_ordering_key", "project_governed_daily_cycle_decision",
    "load_governed_portfolio_decision", "candidate_identity_hash",
    "govern_latest_intraday_assessment", "ZERO_BASE_INCUMBENCY_POLICY",
    # R54.4 — the DAILY producer contract + the ledger reader that retires the
    # legacy read-time projection.
    "build_daily_cycle_candidate", "evaluate_daily_cycle_governance",
    "govern_daily_cycle_decision", "load_persisted_daily_decision",
    "DAILY_PRODUCER_CONTRACT_VERSION", "DAILY_TERMINAL_COMPLETE_STATES",
    "WR_DAILY_MANIFEST_NOT_GOVERNED", "DAILY_GATE_ELIGIBLE",
    "DAILY_GATE_WITHHELD",
    # R55.1 — the terminal governance disposition + how the decision is held.
    "GOV_DISP_NOT_REQUIRED", "GOV_DISP_EVALUATED_NO_PROMOTION",
    "GOV_DISP_WITHHELD", "GOV_DISP_PROMOTED", "GOV_DISP_INCOMPLETE",
    "GOVERNANCE_DISPOSITION_VOCAB", "GOVERNANCE_TERMINAL_DISPOSITIONS",
    "GOVERNANCE_REASON_VOCAB", "INTRADAY_GATE_INVOCATION_CONTRACT",
    "classify_intraday_governance", "classify_decision_persistence",
    "DECISION_PERSISTENCE_VOCAB", "DECISION_PERSISTENCE_LEDGER_ROW",
    "DECISION_PERSISTENCE_LEGACY_PROJECTION", "DECISION_PERSISTENCE_ABSENT",
    # R55.2.2 — the post-cutover persistence contract.
    "DECISION_PERSISTENCE_UNPERSISTED", "GOVERNED_DAILY_NOT_PERSISTED_BLOCKER",
    "GOVERNED_DAILY_WRITE_CUTOVER_RELEASE", "GOVERNED_DAILY_WRITE_CUTOVER_SESSION",
    "GOVERNED_DAILY_WRITE_CUTOVER_BASIS", "governed_daily_write_expected",
    # R61 — the ONE standing-authority resolver, shared by the gate and the read.
    "resolve_standing_governed_decision", "PROJECTION_RETIRED_BY_LEDGER_ROW",
    "INTRADAY_ONLY_LATENCY_STAGES",
    # R82.2 — the ONE approval gate, published on a read. The write path in
    # record_decision consumes risk_policy_approval_gate for its own two
    # risk-policy refusals, so no surface can present approval as available in a
    # state this owner would refuse.
    "risk_policy_approval_gate", "selected_target_approval_gate",
    "AG_AVAILABLE", "APPROVAL_GATE_STATUS_VOCAB", "NEXT_ACTION_VOCAB",
    "NEXT_ACTION_LABELS",
    "NEXT_ACTION_RUN_PORTFOLIO_CYCLE", "NEXT_ACTION_SELECT_A_TARGET",
    "NEXT_ACTION_SELECT_AGAINST_THE_CURRENT_REVIEW",
    "NEXT_ACTION_SELECT_AN_IMPLEMENTABLE_TARGET",
    "NEXT_ACTION_RECORD_THE_NO_CHANGE_DECISION",
    "NEXT_ACTION_RECORD_RISK_POLICY_RULING",
    "NEXT_ACTION_REVIEW_THE_POLICY_COMPLIANT_SUCCESSOR",
    "NEXT_ACTION_APPROVE_SELECTED_TARGET",
]
