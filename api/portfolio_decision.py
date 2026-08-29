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
PDS_UNAVAILABLE = "PORTFOLIO_DECISION_UNAVAILABLE"
DECISION_STATE_VOCAB = (
    PDS_NO_ACTIVE_BOOK, PDS_NO_PROPOSAL, PDS_NO_MATERIAL_CHANGE, PDS_REVIEW_REQUIRED,
    PDS_APPROVED, PDS_REJECTED, PDS_HELD, PDS_STALE, PDS_CHANGE_WITHHELD,
    PDS_HOLD_CURRENT_BOOK, PDS_UNAVAILABLE)
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
    return {
        "portfolio_decision_state": state,
        "portfolio_decision_state_vocabulary": list(DECISION_STATE_VOCAB),
        "label": label,
        "severity": severity,
        "requires_manual_review": requires_review,
        "material": materiality["material"],
        "materiality": materiality,
        "proposal_available": available,
        "proposal_hash": current_hash,
        "proposal_id": summ.get("reallocation_proposal_id"),
        "proposal_state": summ.get("reallocation_proposal_state"),
        "proposed_holding_count": summ.get("reallocation_proposed_holding_count"),
        "one_way_turnover": summ.get("reallocation_one_way_turnover"),
        "estimated_transaction_cost": summ.get("reallocation_estimated_transaction_cost"),
        "score_improvement_net_of_cost": summ.get(
            "reallocation_score_improvement_net_of_cost"),
        "data_gaps": summ.get("reallocation_data_gaps") or [],
        "decision": (decision_record or {}).get("decision"),
        "decision_bound_proposal_hash": (decision_record or {}).get("proposal_hash"),
        "decision_recorded_at": (decision_record or {}).get("recorded_at"),
        "decision_is_current": bool(decision_record
                                    and decision_record.get("proposal_hash") == current_hash
                                    and not ca_stale),
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
                           and not withheld and not hold_current_book),
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


def load_portfolio_decision(*, portfolio_state: Optional[dict] = None,
                            proposal_summary: Optional[dict] = None,
                            artifact: Optional[dict] = None,
                            decision_record: Optional[dict] = None,
                            decision_dir=None, reallocation_dir=None,
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

    if proposal_summary is None:
        proposal_summary = realloc.load_proposal_summary(
            active_book_id=active_book_id, eligible_market_date=eligible,
            artifact=artifact, reallocation_dir=reallocation_dir)
    if decision_record is None:
        decision_record = load_decision_record(
            active_book_id=active_book_id, eligible_market_date=eligible,
            decision_dir=decision_dir)

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


__all__ = [
    "PHASE", "OWNER", "DECISION_APPROVE", "DECISION_REJECT", "DECISION_HOLD",
    "DECISION_VOCAB", "CONFIRM_TOKEN", "DECISION_STATE_VOCAB",
    "PDS_NO_ACTIVE_BOOK", "PDS_NO_PROPOSAL", "PDS_NO_MATERIAL_CHANGE",
    "PDS_REVIEW_REQUIRED", "PDS_APPROVED", "PDS_REJECTED", "PDS_HELD", "PDS_STALE",
    "PDS_CHANGE_WITHHELD", "PDS_HOLD_CURRENT_BOOK", "APPROVABLE_DECISION_STATES",
    "PDS_UNAVAILABLE", "assess_materiality", "record_decision", "load_decision_record",
    "derive_decision_state", "build_order_plan_preview", "load_portfolio_decision",
    "DECISION_DIR_ENV",
]
