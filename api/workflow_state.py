"""api/workflow_state.py — Slice 2 canonical workflow / operator-state contract.

READ-ONLY composition service (Consolidation Roadmap Slice 2; TARGET_ARCHITECTURE
context "Workflow / gate state"). It is the ONE authoritative owner of the
*combined operator interpretation*: the overall workflow state, the current task,
the single primary next action, its severity, the queued follow-up actions, the
portfolio-assessment currency, the blocking conditions, the completed-state
summary, and a deterministic cross-surface consistency verdict.

It **composes** existing authoritative read models and does NOT recompute their
business logic (Principle 1 / Principle 6). It owns no persistent state, performs
NO provider network call, NO prediction call, NO Daily Close, NO research refresh,
NO portfolio reassessment, and NO write of any kind (file, ledger, database,
snapshot, order, signal, decision, fill, cache).

Domain facts stay with their existing owners — this service only *interprets*
them into one operator view:

  * market session / freshness      → ``api.data_freshness`` (built on
    ``engine.market_session``) — session status, operational + research dates,
    active-book identity, readiness flags, cross-surface consistency (Slice 1).
  * portfolio assessment / gate     → ``api.daily_action_gate`` — the outcome /
    target-state and the monthly review clock (never re-derived here).
  * operational daily close         → ``api.daily_close.load_close_progress``
    (PROBE-FREE) — latest completed close date + final status.
  * active operational book         → ``api.operational_book`` — book identity,
    NAV / cash / holdings, pending paper orders, lifecycle.
  * forward evidence                → ``api.forward_prediction_skill`` — latest
    TRUE_FORWARD snapshot + evidence state (gaps documented, never fabricated).
  * target readiness                → ``api.alpha_target`` — target market date.

Decision-currency defect repaired (Workstream F): an older Daily Action Gate
"NO ACTION TODAY" result is NEVER presented as a current "today" conclusion. The
historical result is preserved (dated) in ``completed_summary``; when it is stale
or overdue relative to the latest eligible completed session the workflow says
"Portfolio reassessment is due." No new assessment is run here.

Separation of concerns (Architecture Decision D-7): research/model staleness or a
documented forward-evidence gap is ATTENTION-level and NEVER downgrades a valid,
completed operational close to an operational failure.

Slice 2 scope: this service INTERPRETS state only. It performs no workflow action.
The Persistent Daily Research Cycle and portfolio reassessment (Slice 3+) are
described / routed here but not executed.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Optional

from paper_trader.api import data_freshness as df
from paper_trader.engine import data_gap_taxonomy as gaptax
from paper_trader.engine import market_session as msession
from paper_trader.engine import normal_cycle as ncycle

PHASE = "29C-Slice2"
#: Stage 22 — the canonical NORMAL DAILY PORTFOLIO CYCLE contract this owner projects
#: the decided state onto. The sequence itself lives in the pure kernel
#: ``engine.normal_cycle``; nothing here re-derives it.
NORMAL_CYCLE_OWNER = "engine.normal_cycle"
DATA_GAP_TAXONOMY_OWNER = "engine.data_gap_taxonomy"
#: This module — the ONE owner of the composed operator state.
WORKFLOW_STATE_OWNER = "api.workflow_state"

# --------------------------------------------------------------------------- #
# Frozen overall workflow-state vocabulary (part of the tested contract,
# Workstream D). Deterministic priority policy in ``_decide_overall`` (E).
# --------------------------------------------------------------------------- #
WAITING_FOR_SESSION_CLOSE = "WAITING_FOR_SESSION_CLOSE"
WAITING_FOR_OWNED_DATA = "WAITING_FOR_OWNED_DATA"
RESEARCH_CYCLE_REQUIRED = "RESEARCH_CYCLE_REQUIRED"
RESEARCH_CYCLE_RUNNING = "RESEARCH_CYCLE_RUNNING"      # Slice 3 (a cycle is in flight)
RESEARCH_CYCLE_BLOCKED = "RESEARCH_CYCLE_BLOCKED"      # Slice 3 (e.g. monthly emitter)
PORTFOLIO_REASSESSMENT_REQUIRED = "PORTFOLIO_REASSESSMENT_REQUIRED"
READY_FOR_DAILY_CLOSE = "READY_FOR_DAILY_CLOSE"
DAILY_CYCLE_COMPLETE = "DAILY_CYCLE_COMPLETE"
DAILY_CYCLE_COMPLETE_EVIDENCE_GAP = "DAILY_CYCLE_COMPLETE_EVIDENCE_GAP"
MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"
INCONSISTENT_STATE = "INCONSISTENT_STATE"

OVERALL_STATES = (
    INCONSISTENT_STATE,
    WAITING_FOR_SESSION_CLOSE,
    WAITING_FOR_OWNED_DATA,
    RESEARCH_CYCLE_RUNNING,
    RESEARCH_CYCLE_BLOCKED,
    RESEARCH_CYCLE_REQUIRED,
    PORTFOLIO_REASSESSMENT_REQUIRED,
    READY_FOR_DAILY_CLOSE,
    MANUAL_REVIEW_REQUIRED,
    DAILY_CYCLE_COMPLETE_EVIDENCE_GAP,
    DAILY_CYCLE_COMPLETE,
)

# Daily Research Cycle state mirror (a FROZEN subset of api.daily_research_cycle's
# tested vocabulary). Kept as literals so this module stays importable/pure without
# api.daily_research_cycle; the production path reads the real status via
# daily_research_cycle.load_daily_research_cycle_status.
_DRC_RUNNING_STATES = frozenset({
    "PLANNING", "REFRESHING_REQUIRED_INPUTS", "VALIDATING_INPUT_ALIGNMENT",
    "SCORING_UNIVERSE", "PREPARING_TARGET", "CAPTURING_FORWARD_EVIDENCE",
    "RUNNING_PORTFOLIO_ASSESSMENT", "RUN_IN_PROGRESS"})
_DRC_BLOCKED_STATES = frozenset({"BLOCKED", "FAILED"})
_DRC_COMPLETE_STATES = frozenset({"COMPLETE", "COMPLETE_WITH_EVIDENCE_GAP"})
_DRC_EXECUTE_TOKEN = "RUN_DAILY_RESEARCH_CYCLE"

# Frozen assessment-currency vocabulary (Workstream D / decision currency F).
ASSESS_CURRENT = "CURRENT"
ASSESS_STALE = "STALE"
ASSESS_DUE = "DUE"
ASSESS_OVERDUE = "OVERDUE"
ASSESS_MISSING = "MISSING"
ASSESS_INCONSISTENT = "INCONSISTENT"
ASSESSMENT_STATUSES = (ASSESS_CURRENT, ASSESS_STALE, ASSESS_DUE, ASSESS_OVERDUE,
                       ASSESS_MISSING, ASSESS_INCONSISTENT)
# The assessment states that require a fresh reassessment (not current).
_ASSESS_NEEDS_ACTION = frozenset({ASSESS_STALE, ASSESS_DUE, ASSESS_OVERDUE,
                                  ASSESS_MISSING})

# Frozen action-severity vocabulary.
SEV_INFO = "INFO"
SEV_SUCCESS = "SUCCESS"
SEV_ATTENTION = "ATTENTION"
SEV_BLOCKED = "BLOCKED"
SEV_ERROR = "ERROR"
ACTION_SEVERITIES = (SEV_INFO, SEV_SUCCESS, SEV_ATTENTION, SEV_BLOCKED, SEV_ERROR)

# Cross-surface consistency vocabulary (shared with data_freshness Slice 1).
CONSISTENT = "CONSISTENT"
INCONSISTENT = "INCONSISTENT"
UNKNOWN = "UNKNOWN"
CONSISTENCY_VOCAB = (CONSISTENT, INCONSISTENT, UNKNOWN)

# Valid existing UI routes (Workstream I / G). Every action destination is one of
# these six sidebar routes (data-route tokens), so the UI can navigate with the
# existing ``navigateToRoute`` helper and never invents a dead route.
DEST_COMMAND_CENTER = "command-center"
DEST_PORTFOLIO = "portfolio"
DEST_DAILY_WORKFLOW = "daily-workflow"
DEST_PORTFOLIO_MANAGER = "portfolio-manager"
DEST_MODEL_TARGET = "multi-horizon"
DEST_RESEARCH_AUDIT = "research-audit"
VALID_DESTINATIONS = (DEST_COMMAND_CENTER, DEST_PORTFOLIO, DEST_DAILY_WORKFLOW,
                      DEST_PORTFOLIO_MANAGER, DEST_MODEL_TARGET, DEST_RESEARCH_AUDIT)

# Frozen action codes (one per overall state + queued follow-ups).
ACTION_INSPECT_INCONSISTENCY = "INSPECT_STATE_INCONSISTENCY"
ACTION_WAIT_FOR_SESSION_CLOSE = "WAIT_FOR_SESSION_CLOSE"
ACTION_WAIT_FOR_OWNED_DATA = "WAIT_FOR_OR_REFRESH_OWNED_DATA"
ACTION_RUN_RESEARCH_CYCLE = "RUN_DAILY_RESEARCH_CYCLE"
ACTION_MONITOR_RESEARCH_CYCLE = "MONITOR_DAILY_RESEARCH_CYCLE"
ACTION_RESOLVE_RESEARCH_BLOCKER = "RESOLVE_RESEARCH_CYCLE_BLOCKER"
ACTION_RUN_PORTFOLIO_REASSESSMENT = "RUN_PORTFOLIO_REASSESSMENT"
ACTION_RUN_DAILY_CLOSE = "RUN_DAILY_CLOSE"
ACTION_REVIEW_EVIDENCE_GAP = "REVIEW_EVIDENCE_GAP"
ACTION_MANUAL_REVIEW = "MANUAL_PORTFOLIO_REVIEW"
ACTION_MONITOR = "MONITOR_PORTFOLIO"
ACTION_REVIEW_PENDING_ORDERS = "REVIEW_PENDING_PAPER_ORDERS"
# Phase 29G.2: after a completed Holding Opportunity-Cost assessment exists, the operator
# REVIEWS it (read-only) before the Daily Close. This is a review/routing action only —
# it is NEVER a separate reassessment/proposal/rebalance/order execution control.
ACTION_REVIEW_HOC = "REVIEW_HOLDING_OPPORTUNITY_COST"
# Phase 29H (Slice 7): after a reallocation proposal exists, the operator REVIEWS it
# (read-only, manual review). Like ACTION_REVIEW_HOC this is a review/routing action
# only — NEVER an apply/rebalance/confirm-target/order-execution control; the Daily
# Close remains a separate operational action governed by its own requirements.
ACTION_REVIEW_REALLOCATION = "REVIEW_REALLOCATION_PROPOSAL"

# --------------------------------------------------------------------------- #
# Canonical execution routing (Operator Action Integrity). The machine-readable
# executor identity for the THREE real primary executions. The UI's ONE shared
# dispatcher (dispatchCanonicalPrimaryAction) maps these to the EXISTING
# execution owners — never a second writer, never a client-side priority engine:
#   PAPER_DESK_REFRESH    → POST /v1/paper-desk/refresh
#                           (api.paper_trading_desk — the ONE owned-EOD desk-mark writer)
#   DAILY_RESEARCH_CYCLE  → POST /v1/operations/daily-research-cycle/run
#                           (api.daily_research_cycle — the sole research execution path)
#   DAILY_CLOSE           → POST /v1/operations/daily-close/execute
#                           (api.daily_close — the ONLY close write path)
# Paths/tokens are kept as literals so this module stays importable/pure without
# the owner modules (same pattern as _DRC_EXECUTE_TOKEN); the contract tests
# assert they equal the owners' constants and the real app routes.
EXEC_PAPER_DESK_REFRESH = "PAPER_DESK_REFRESH"
EXEC_DAILY_RESEARCH_CYCLE = "DAILY_RESEARCH_CYCLE"
EXEC_DAILY_CLOSE = "DAILY_CLOSE"
EXECUTION_KINDS = (EXEC_PAPER_DESK_REFRESH, EXEC_DAILY_RESEARCH_CYCLE,
                   EXEC_DAILY_CLOSE)

# Stage 19.3 — NORMAL-PATH vs MAINTENANCE execution kinds.
#
# The Paper Desk refresh remains the canonical LOW-LEVEL owned-mark / NEXT_CLOSE
# settlement owner and keeps its endpoint, but it is no longer part of the normal
# operator workflow: the Daily Close composes it. Exposing it as a canonical
# primary_action created a SECOND post-close orchestration path (refresh the desk,
# then run the close) for one transition. It is therefore classified MAINTENANCE:
# reachable only from an explicitly secondary Advanced / Recovery surface, never
# promoted as the operator's next action, never rendered beside the normal CTA.
MAINTENANCE_EXECUTION_KINDS = (EXEC_PAPER_DESK_REFRESH,)
NORMAL_PATH_EXECUTION_KINDS = (EXEC_DAILY_RESEARCH_CYCLE, EXEC_DAILY_CLOSE)


def assert_primary_action_contract(primary: dict) -> dict:
    """Fail CLOSED if a maintenance/recovery executor is promoted to the canonical
    primary action. Returns the same dict so callers can wrap it inline. This is the
    runtime half of the Stage 19.3 guard (the static half lives in
    ``scripts/audit_architecture.py``)."""
    kind = (primary or {}).get("execution_kind")
    if kind in MAINTENANCE_EXECUTION_KINDS:
        raise AssertionError(
            "%s is a maintenance/recovery executor and must never be the canonical "
            "primary action; the Daily Close composes it." % kind)
    return primary
EXECUTION_CONTRACTS = {
    EXEC_PAPER_DESK_REFRESH: {
        "method": "POST", "path": "/v1/paper-desk/refresh",
        "confirmation_field": "confirm",
        "confirmation_token": "CONFIRM_PAPER_DESK_REFRESH"},
    EXEC_DAILY_RESEARCH_CYCLE: {
        "method": "POST", "path": "/v1/operations/daily-research-cycle/run",
        "confirmation_field": "confirmation",
        "confirmation_token": _DRC_EXECUTE_TOKEN},
    EXEC_DAILY_CLOSE: {
        "method": "POST", "path": "/v1/operations/daily-close/execute",
        "confirmation_field": "confirmation",
        "confirmation_token": "CONFIRM_ALPHA_DAILY_CLOSE"},
}

# --------------------------------------------------------------------------- #
# Release 48 — ONE OPERATOR CONCEPT FOR THE NORMAL-PATH MUTATIONS.
#
# The overall-state machine still decides WHICH owner must run (the Daily Close
# or the Daily Research Cycle — the two normal-path mutations above, unchanged),
# and every state-level ``primary_action`` still names that owner verbatim for
# the audit/advanced surfaces and the existing contracts. What Release 48 changes
# is the OPERATOR PRESENTATION: the canonical operator command now presents the
# ONE concept "Run the portfolio cycle" whenever a normal-path mutation is due,
# and the orchestration owner (``api.portfolio_cycle``) sequences the same
# authoritative owners with their own tokens. The operator no longer carries the
# close-before-research order in their head; the workflow owner walks it.
#
# The underlying decided step travels beside the presented action
# (``cycle_underlying_kind`` / ``cycle_underlying_label``), so nothing is hidden
# — the presentation is simplified, the decision is not.
# --------------------------------------------------------------------------- #
EXEC_PORTFOLIO_CYCLE = "PORTFOLIO_CYCLE"
PORTFOLIO_CYCLE_CONFIRMATION = "RUN_PORTFOLIO_CYCLE"
PORTFOLIO_CYCLE_OWNER = "api.portfolio_cycle"
PORTFOLIO_CYCLE_LABEL = "Run the portfolio cycle"
PORTFOLIO_CYCLE_EXECUTION_CONTRACT = {
    "method": "POST", "path": "/v1/operations/portfolio-cycle/run",
    "confirmation_field": "confirmation",
    "confirmation_token": PORTFOLIO_CYCLE_CONFIRMATION,
}

# Release 29.4 — CLOSE VALIDITY IS NOT DECIDED HERE.
#
# This module used to keep a private LITERAL COPY of "which close statuses mean the
# session was processed". Release 29.3 renamed the drift token and migrated it on read;
# this copy kept the old spelling, so the real, complete 2026-08-17 close stopped being
# recognised, the eligible session read as UNCLOSED, and on 2026-08-18 08:31 ET — with
# the session still open — RUN_DAILY_CLOSE was offered for an already-processed session.
#
# The predicate now belongs to the module that WRITES the token (api.daily_close). The
# fallback below exists only so this module stays importable in a pure context; it is a
# copy of the owner's set and `scripts/audit_architecture.py` fails the build if it ever
# stops matching, so the two can no longer drift silently.
CLOSE_VALIDITY_OWNER = "api.daily_close"
#: WHICH completed session is eligible to work on is decided by the market-session
#: domain and reaches this module through api.data_freshness. The workflow owner never
#: recomputes a market date, a trading calendar or a session cutoff of its own.
SESSION_ELIGIBILITY_OWNER = "engine.market_session"
_CLOSE_COMPLETE_FALLBACK = frozenset({
    "DAILY_CLOSE_COMPLETE_HOLD", "DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT",
    "PAPER_ORDERS_SUBMITTED", "INITIAL_BASELINE_RECORDED", "ALREADY_PROCESSED"})
_CLOSE_FAILED_STATUSES = frozenset({"DATA_BLOCKED", "EXECUTION_ERROR"})

# Daily Action Gate target_state that demands a manual approval (material change).
_GATE_APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
# Gate outcome code for a no-change conclusion (preserved historically, never
# re-presented as a stale "today" verdict — Workstream F).
_GATE_OUTCOME_NO_ACTION = "NO_ACTION_TODAY"

SAFETY_BADGES = ["READ ONLY", "NO PROVIDER CALL", "NO ORDERS", "AUTOMATION OFF",
                 "MANUAL REVIEW", "PREVIEW ONLY"]

# Deterministic clock seam (tests / explicit callers).
NOW_ENV = "PAPER_TRADER_WORKFLOW_STATE_NOW"
_now_override: Optional[datetime] = None


def _now() -> datetime:
    if _now_override is not None:
        return _now_override
    raw = os.environ.get(NOW_ENV)
    if raw:
        try:
            parsed = datetime.fromisoformat(raw)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(tz=timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _coerce_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _iso(d: Optional[date]) -> Optional[str]:
    return d.isoformat() if d else None


def _business_days_between(a: Optional[date], b: Optional[date]) -> Optional[int]:
    if a is None or b is None:
        return None
    lo, hi = (a, b) if a <= b else (b, a)
    n, cur = 0, lo
    while cur < hi:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            n += 1
    return n


def _op_book(operational: Optional[dict]) -> dict:
    """Unwrap the active operational-book payload nested under ``operational_book``
    by ``operational_book.load_operational_book`` (a book dict injected directly in
    tests is returned unchanged)."""
    op = operational or {}
    inner = op.get("operational_book")
    return inner if isinstance(inner, dict) else op


def _safe(loader: Callable, warnings: list, label: str) -> Any:
    try:
        return loader()
    except Exception as exc:  # noqa: BLE001
        warnings.append("%s unavailable: %s" % (label, str(exc)[:160]))
        return None


# --------------------------------------------------------------------------- #
# Pure assessment-currency classifier (Workstream F). Deterministic over injected
# dates. This repairs the "older assessment presented as today" defect: a stale or
# overdue assessment is classified, never re-labelled as a current conclusion.
# --------------------------------------------------------------------------- #
def classify_assessment(*, assessment_date: Any, eligible_date: Any,
                        current_reference_date: Any, next_review_date: Any,
                        review_due: bool) -> dict[str, Any]:
    """Classify how current the latest portfolio assessment is.

    Returns ``status`` (frozen vocabulary), ``review_overdue`` and
    ``current_for_eligible_session``. Precedence: MISSING > INCONSISTENT >
    OVERDUE > STALE > DUE > CURRENT.
    """
    a = _coerce_date(assessment_date)
    elig = _coerce_date(eligible_date)
    ref = _coerce_date(current_reference_date)
    nr = _coerce_date(next_review_date)

    review_overdue = bool(nr is not None and ref is not None and ref > nr)
    review_due_flag = bool(review_due) or bool(
        nr is not None and ref is not None and ref >= nr)
    current_for_eligible = bool(a is not None and elig is not None and a == elig)

    if a is None:
        status = ASSESS_MISSING
    elif elig is not None and a > elig:
        status = ASSESS_INCONSISTENT
    elif review_overdue:
        status = ASSESS_OVERDUE
    elif elig is not None and a < elig:
        status = ASSESS_STALE
    elif review_due_flag:
        status = ASSESS_DUE
    else:
        status = ASSESS_CURRENT

    return {"status": status, "review_overdue": review_overdue,
            "review_due": review_due_flag,
            "current_for_eligible_session": current_for_eligible,
            "assessment_age_sessions": _business_days_between(a, elig)}


# --------------------------------------------------------------------------- #
# Canonical presentation contract (Workstream B / D / F). The backend GENERATES
# the operator-facing text so the UI never turns raw dates/booleans into a
# conclusion. An older no-change assessment is presented as a DATED historical
# result with its canonical currency; "today" wording is allowed ONLY when the
# assessment is current. The evidence presentation separates the still-open
# current session from the latest completed close's documented (attention-level)
# forward-evidence gap. No new assessment or evidence is computed here.
# --------------------------------------------------------------------------- #
_CURRENCY_LABELS = {
    ASSESS_CURRENT: "Current", ASSESS_STALE: "Stale", ASSESS_DUE: "Due",
    ASSESS_OVERDUE: "Overdue", ASSESS_MISSING: "Missing",
    ASSESS_INCONSISTENT: "Inconsistent",
}
_CURRENCY_SEVERITY = {
    ASSESS_CURRENT: SEV_SUCCESS, ASSESS_STALE: SEV_ATTENTION, ASSESS_DUE: SEV_ATTENTION,
    ASSESS_OVERDUE: SEV_ATTENTION, ASSESS_MISSING: SEV_ATTENTION,
    ASSESS_INCONSISTENT: SEV_ERROR,
}
_PROPOSAL_OUTCOMES = frozenset({
    "PORTFOLIO_CHANGES_PROPOSED", "PROPOSAL", "PROPOSAL_READY",
    "REBALANCE_PROPOSAL_READY", "APPROVAL_REQUIRED", "PROPOSED"})

# --------------------------------------------------------------------------- #
# Phase 29G.2 residual hard cutover — the ONE primary portfolio-decision concept is
# the Holding Opportunity-Cost Review (Slice 6). The legacy daily-action-gate rank-
# membership comparison is reclassified compatibility-only and is NEVER a primary
# decision. Before the first production HOC artifact, the canonical operator state is
# HOLDING_OPPORTUNITY_COST_NOT_RUN (a compatibility comparison must never set the
# canonical operator state to PROPOSAL_READY / REBALANCE_PROPOSAL_READY /
# PORTFOLIO_CHANGES_PROPOSED).
# --------------------------------------------------------------------------- #
HOC_CANONICAL_OWNER = "api.holding_opportunity_cost"
HOC_PRIMARY_TITLE = "HOLDING OPPORTUNITY-COST REVIEW"
LEGACY_COMPARISON_TITLE = "LEGACY MEMBERSHIP-COMPARISON SUMMARY — COMPATIBILITY ONLY"

# Canonical operator-state tokens (the ONE portfolio-decision state every surface reads).
COS_NOT_RUN = "HOLDING_OPPORTUNITY_COST_NOT_RUN"
COS_AVAILABLE = "HOLDING_OPPORTUNITY_COST_AVAILABLE"
COS_DEGRADED = "HOLDING_OPPORTUNITY_COST_DEGRADED"
COS_BLOCKED = "HOLDING_OPPORTUNITY_COST_BLOCKED"
COS_NO_ACTIVE_BOOK = "HOLDING_OPPORTUNITY_COST_NO_ACTIVE_BOOK"
COS_UNAVAILABLE = "HOLDING_OPPORTUNITY_COST_UNAVAILABLE"
CANONICAL_OPERATOR_STATES = (COS_NOT_RUN, COS_AVAILABLE, COS_DEGRADED, COS_BLOCKED,
                             COS_NO_ACTIVE_BOOK, COS_UNAVAILABLE)

# Slice 7 (Phase 29H) reallocation-proposal operator-state tokens (a SEPARATE,
# informational review state — it never enters the frozen OVERALL_STATES vocabulary
# and never gates the Daily Close).
RP_TITLE = "REALLOCATION PROPOSAL — MANUAL REVIEW REQUIRED"
RP_CANONICAL_OWNER = "api.reallocation_proposal"
RPS_NOT_RUN = "REALLOCATION_PROPOSAL_NOT_RUN"
RPS_READY = "REALLOCATION_PROPOSAL_READY"
RPS_DEGRADED = "REALLOCATION_PROPOSAL_DEGRADED"
RPS_BLOCKED = "REALLOCATION_PROPOSAL_BLOCKED"
#: Release 29.3 — a COMPLETE candidate target exists and is reviewable, but a
#: portfolio-level limit only the complete target can settle (turnover / concentration /
#: sector / risk) is breached, so the change is WITHHELD. Never approvable.
RPS_WITHHELD = "REALLOCATION_PROPOSAL_WITHHELD"
RPS_NO_ACTIVE_BOOK = "REALLOCATION_PROPOSAL_NO_ACTIVE_BOOK"
RPS_UNAVAILABLE = "REALLOCATION_PROPOSAL_UNAVAILABLE"
REALLOCATION_OPERATOR_STATES = (RPS_NOT_RUN, RPS_READY, RPS_DEGRADED, RPS_BLOCKED,
                                RPS_WITHHELD, RPS_NO_ACTIVE_BOOK, RPS_UNAVAILABLE)
#: The ONLY reallocation operator states in which a proposal may be reviewed/approved.
REALLOCATION_APPROVABLE_STATES = (RPS_READY, RPS_DEGRADED)
# Reallocation read-summary states (mirror of api.reallocation_proposal READ_STATE_VOCAB).
_RP_READY = "READY"
_RP_DEGRADED = "DEGRADED"
_RP_BLOCKED = "BLOCKED"
_RP_NO_ACTIVE_BOOK = "NO_ACTIVE_BOOK"
_RP_NOT_RUN = "NOT_RUN"
_RP_UNAVAILABLE = "UNAVAILABLE"
_RP_WITHHELD = "WITHHELD"

# HOC read-summary states (mirror of api.holding_opportunity_cost READ_STATE_VOCAB) —
# kept as literals so this module stays importable/pure without importing the owner.
_HOC_READY = "READY"
_HOC_DEGRADED = "DEGRADED"
_HOC_BLOCKED = "BLOCKED"
_HOC_NO_ACTIVE_BOOK = "NO_ACTIVE_BOOK"
_HOC_NOT_RUN = "NOT_RUN"
_HOC_UNAVAILABLE = "UNAVAILABLE"

# --------------------------------------------------------------------------- #
# Stage 18 — model-recalibration REVIEW trigger vocabulary (a SEPARATE lane from
# the portfolio decision; never a retrainer, never automatic). A model REVIEW is
# raised ONLY when a SIGNAL / RANKING degradation flag is *actionable* (evidence
# sufficient) — never from a portfolio performance symptom (negative excess,
# drawdown, cost-adjusted alpha) and never from realized P&L. Champion recalibration
# stays a manually-approved controlled study; nothing here promotes or retrains.
# --------------------------------------------------------------------------- #
MODEL_HEALTHY = "MODEL_HEALTHY"
MODEL_RECALIBRATION_REVIEW = "MODEL_RECALIBRATION_REVIEW"
MODEL_REVIEW_STATES = (MODEL_HEALTHY, MODEL_RECALIBRATION_REVIEW)
# The ONLY forward-evidence research-flag category that reflects signal/ranking
# degradation (all other flags are portfolio symptoms — observation-only by design).
_SIGNAL_DEGRADATION_FLAGS = frozenset({"RANK_IC_DEGRADATION"})

# Stage 18 — separate portfolio-decision review lane action code (a REVIEW/routing
# action only; the durable decision is recorded by api.portfolio_decision, never here).
ACTION_REVIEW_PROPOSED_PORTFOLIO = "REVIEW_PROPOSED_PORTFOLIO"

# --------------------------------------------------------------------------- #
# Stage 20 — the canonical PORTFOLIO-REASSESSMENT lane (Workstream I).
#
# The workflow owner EXPOSES the reassessment decision; it never computes it. Every
# value below is read verbatim from api.portfolio_reassessment (which itself only
# aggregates the Slice-6 assessment through the pure Stage-20 kernel). There is
# deliberately NO second economic gate here and NO second operator command bar: the
# reassessment contributes at most ONE routing action, and it is suppressed entirely
# while a Stage-19 execution is in flight.
# --------------------------------------------------------------------------- #
PRS_TITLE = "ACTIVE PORTFOLIO ASSESSMENT"
PRS_CANONICAL_OWNER = "api.portfolio_reassessment"
PRS_NOT_RUN = "NOT_RUN"
PRS_CHANGE_CANDIDATE = "CHANGE_CANDIDATE"
#: Policy defaults mirrored from engine.portfolio_reassessment.default_policy() ONLY so
#: a pre-29.3 artifact (which did not carry them) still renders a complete decision
#: object. A live artifact always supplies its own values and they win.
_PRS_NET_HURDLE_DEFAULT = 0.05
_PRS_TURNOVER_BUDGET_DEFAULT = 0.35
PRS_PROPOSAL_READY = "PROPOSAL_READY"
#: The canonical portfolio-decision tokens this module cross-checks (mirrored from
#: api.portfolio_decision as literals so the module stays importable/pure).
PDS_NO_PROPOSAL = "PORTFOLIO_DECISION_NO_PROPOSAL"
PDS_CHANGE_WITHHELD = "CHANGE_CANDIDATE_WITHHELD"
PDS_REVIEW_REQUIRED = "PROPOSAL_REVIEW_REQUIRED"
#: Release 47 — a complete FEASIBLE alternative was computed and priced and is simply
#: not worth what switching would cost. It is a decision, not a blocker and not a
#: withhold, and the operator surface must say so.
PDS_HOLD_CURRENT_BOOK = "HOLD_CURRENT_BOOK"
#: Release 47 — the proposal owner's three authoritative outcomes, mirrored as
#: literals so this module stays importable and pure. It renders them; it never
#: decides them.
RO_PROPOSAL_READY = "PROPOSAL_READY"
RO_HOLD_CURRENT_BOOK = "HOLD_CURRENT_BOOK"
RO_TRUE_BLOCKER = "TRUE_BLOCKER"
REALLOCATION_OUTCOME_VOCABULARY = (RO_PROPOSAL_READY, RO_HOLD_CURRENT_BOOK,
                                   RO_TRUE_BLOCKER)
#: Operator-facing reassessment states (the kernel vocabulary plus the read states).
PRS_OPERATOR_STATES = (
    "NOT_READY", "CURRENT_NO_CHANGE", "CHANGE_CANDIDATE", "PROPOSAL_READY",
    "BLOCKED_DATA", "BLOCKED_EVIDENCE", "MANUAL_REVIEW_REQUIRED", PRS_NOT_RUN,
    "UNAVAILABLE", "STALE_CORPORATE_ACTION_REVIEW_REQUIRED")
ACTION_REVIEW_PORTFOLIO_PROPOSAL = "REVIEW_PORTFOLIO_PROPOSAL"


def _historical_result_text(outcome: Any, recommendation: Any) -> str:
    """Human, DATE-FREE statement of what the legacy membership comparison concluded.
    The date is added by the presentation headline, never baked in here. This is the
    LEGACY rank-membership comparison summary, never the primary portfolio decision."""
    if outcome == _GATE_OUTCOME_NO_ACTION:
        return "The current holdings matched the ranked names."
    if outcome in _PROPOSAL_OUTCOMES:
        return "The legacy membership comparison identified changes (compatibility only)."
    if recommendation:
        return str(recommendation)
    if outcome:
        return str(outcome).replace("_", " ").capitalize() + "."
    return "No legacy membership-comparison result is available."


def build_assessment_presentation(*, assessment_status: str, assessment_date: Any,
                                  outcome: Any, recommendation: Any,
                                  current_for_eligible: bool,
                                  membership_add_count: int = 0,
                                  membership_remove_count: int = 0,
                                  membership_resize_count: int = 0) -> dict[str, Any]:
    """LEGACY membership-comparison presentation (Phase 29G.2 compatibility-only).

    This is the read-only compatibility framing of the daily-action-gate rank-membership
    comparison. It is NEVER the primary portfolio-decision card (that is the Holding
    Opportunity-Cost Review — ``build_holding_opportunity_cost_presentation``). The dated
    historical currency is preserved for audit; the title/classification make the
    compatibility-only, no-decision-authority status explicit. "today" wording is
    permitted only when the comparison is current."""
    status = assessment_status
    is_current = (status == ASSESS_CURRENT)
    date_txt = _iso(_coerce_date(assessment_date)) or (
        str(assessment_date) if assessment_date else None)
    hist = _historical_result_text(outcome, recommendation)
    title = LEGACY_COMPARISON_TITLE
    title_with_date = ("%s — %s" % (title, date_txt)) if date_txt else title

    if status == ASSESS_MISSING:
        headline = "No legacy membership comparison has been recorded yet."
    elif date_txt:
        headline = hist.rstrip(".") + " on %s." % date_txt
    else:
        headline = hist

    if status == ASSESS_INCONSISTENT:
        explanation = ("The recorded comparison is dated after the latest eligible "
                       "completed session; reconcile the inconsistency before acting.")
        follow_up = "Inspect the state inconsistency."
    elif is_current:
        explanation = ("Read-only compatibility view. The canonical portfolio decision "
                       "is the Holding Opportunity-Cost Review.")
        follow_up = None
    else:
        explanation = ("Read-only compatibility view. The canonical portfolio decision "
                       "is the Holding Opportunity-Cost Review.")
        follow_up = None

    total = int(membership_add_count) + int(membership_remove_count) + int(membership_resize_count)
    return {
        "title": title,
        "title_with_date": title_with_date,
        "assessment_date": date_txt,
        "historical_result": hist,
        "recommendation": (str(recommendation) if recommendation else None),
        "currency_status": status,
        "currency_label": _CURRENCY_LABELS.get(status, status),
        "badge": status,
        "headline": headline,
        "explanation": explanation,
        "severity": _CURRENCY_SEVERITY.get(status, SEV_ATTENTION),
        "follow_up": follow_up,
        "may_be_called_current": is_current,
        "today_wording_allowed": is_current,
        "current_for_eligible_session": bool(current_for_eligible),
        # Phase 29G.2 explicit compatibility classification (never a primary decision).
        "compatibility_only": True,
        "decision_authority": "NONE",
        "execution_available": False,
        "is_portfolio_proposal": False,
        "creates_orders": False,
        "canonical_decision_owner": HOC_CANONICAL_OWNER,
        "legacy_membership_comparison": True,
        "membership_add_count": int(membership_add_count),
        "membership_remove_count": int(membership_remove_count),
        "membership_resize_count": int(membership_resize_count),
        "membership_change_count": total,
        "view_action_label": "View Legacy Membership Comparison",
    }


# HOC read-summary state → (canonical operator state, badge label, severity).
_HOC_STATE_MAP = {
    _HOC_READY: (COS_AVAILABLE, "AVAILABLE", SEV_SUCCESS),
    _HOC_DEGRADED: (COS_DEGRADED, "AVAILABLE — DATA GAPS", SEV_ATTENTION),
    _HOC_BLOCKED: (COS_BLOCKED, "BLOCKED", SEV_BLOCKED),
    _HOC_NO_ACTIVE_BOOK: (COS_NO_ACTIVE_BOOK, "NO ACTIVE BOOK", SEV_ATTENTION),
    _HOC_NOT_RUN: (COS_NOT_RUN, "NOT_RUN", SEV_ATTENTION),
    _HOC_UNAVAILABLE: (COS_UNAVAILABLE, "UNAVAILABLE", SEV_ATTENTION),
}


def build_holding_opportunity_cost_presentation(
        *, state: Any, available: bool, eligible_date: Any,
        active_book_label: Any = None, recommendation_counts: Optional[dict] = None,
        assessment_hash: Any = None, data_gaps: Optional[list] = None) -> dict[str, Any]:
    """The ONE PRIMARY portfolio-decision presentation (Phase 29G.2): the Holding
    Opportunity-Cost Review. Rendered on every operator surface (Command Center, Daily
    Workflow, Portfolio Manager). Read-only, review-only: no target weights, no orders,
    no confirmation. Before the first production artifact it presents NOT_RUN with
    recommendations "NONE YET" — never fabricated HOLD/REDUCE/EXIT/REPLACE/ADD counts."""
    hoc_state = str(state) if state else _HOC_NOT_RUN
    if hoc_state not in _HOC_STATE_MAP:
        hoc_state = _HOC_NOT_RUN if not available else _HOC_READY
    canonical_state, badge, severity = _HOC_STATE_MAP[hoc_state]
    date_txt = _iso(_coerce_date(eligible_date)) or (
        str(eligible_date) if eligible_date else None)
    title = HOC_PRIMARY_TITLE
    title_with_date = ("%s — %s" % (title, date_txt)) if date_txt else title
    counts = recommendation_counts or {}
    gaps = list(data_gaps or [])

    has_assessment = bool(available and hoc_state in (_HOC_READY, _HOC_DEGRADED))
    if not has_assessment:
        headline = ("No Holding Opportunity-Cost assessment has been produced for the "
                    "current active book and eligible session yet.")
        explanation = ("The first production assessment will be generated by the next "
                       "completed Daily Research Cycle. Recommendations: NONE YET. "
                       "Read-only, review-only: no target weights, no orders.")
        recommendations_label = "NONE YET"
    else:
        def _c(k):
            try:
                return int(counts.get(k, 0) or 0)
            except (TypeError, ValueError):
                return 0
        recommendations_label = (
            "%d HOLD / %d REDUCE / %d EXIT / %d REPLACE / %d ADD"
            % (_c("HOLD"), _c("REDUCE"), _c("EXIT"), _c("REPLACE"), _c("ADD")))
        gap_txt = (" · %d data gap(s) documented" % len(gaps)) if gaps else ""
        headline = ("Holding Opportunity-Cost assessment%s: %s.%s"
                    % ((" — %s" % date_txt) if date_txt else "",
                       recommendations_label, gap_txt))
        explanation = ("Review-only per-holding opportunity-cost assessment produced by "
                       "the Daily Research Cycle. No target weights, no orders, no "
                       "confirmation. It feeds the Reallocation Proposal engine "
                       "(Slice 7), which is review-only and creates no order or target.")

    return {
        "title": title,
        "title_with_date": title_with_date,
        "badge": badge,
        "currency_status": badge,
        "headline": headline,
        "explanation": explanation,
        "severity": severity,
        "state": hoc_state,
        "canonical_operator_state": canonical_state,
        "available": bool(has_assessment),
        "has_assessment": has_assessment,
        "eligible_market_date": date_txt,
        "active_book_label": active_book_label,
        "recommendation_counts": dict(counts) if has_assessment else {},
        "recommendations_label": recommendations_label,
        "assessment_hash": assessment_hash if has_assessment else None,
        "data_gaps": gaps if has_assessment else [],
        "is_primary_decision": True,
        "decision_authority": "REVIEW_ONLY",
        "execution_available": False,
        "creates_orders": False,
        "canonical_decision_owner": HOC_CANONICAL_OWNER,
        "sole_execution_path": "POST /v1/operations/daily-research-cycle/run",
    }


# Reallocation read-summary state → (canonical operator state, badge, severity).
_RP_STATE_MAP = {
    _RP_READY: (RPS_READY, "READY — MANUAL REVIEW", SEV_INFO),
    _RP_DEGRADED: (RPS_DEGRADED, "READY — DATA GAPS", SEV_ATTENTION),
    _RP_BLOCKED: (RPS_BLOCKED, "BLOCKED", SEV_BLOCKED),
    _RP_NO_ACTIVE_BOOK: (RPS_NO_ACTIVE_BOOK, "NO ACTIVE BOOK", SEV_ATTENTION),
    _RP_NOT_RUN: (RPS_NOT_RUN, "NOT_RUN", SEV_INFO),
    _RP_WITHHELD: (RPS_WITHHELD, "WITHHELD — PORTFOLIO LIMITS", SEV_ATTENTION),
    _RP_UNAVAILABLE: (RPS_UNAVAILABLE, "UNAVAILABLE", SEV_ATTENTION),
}


#: Release 46.6.2 — reassessment states that mean the Stage-20 ECONOMIC GATE
#: examined this session and did NOT clear. On those, the Daily Research Cycle
#: deliberately SKIPS ``BUILD_REALLOCATION_PROPOSAL`` and records why; a
#: completed cycle then legitimately leaves no proposal behind.
_RP_GATE_DID_NOT_CLEAR = ("MANUAL_REVIEW_REQUIRED", "CURRENT_NO_CHANGE",
                          "BLOCKED_DATA", "BLOCKED_EVIDENCE",
                          "STALE_CORPORATE_ACTION_REVIEW_REQUIRED")


def build_reallocation_proposal_presentation(
        *, state: Any, available: bool, eligible_date: Any,
        action_counts: Optional[dict] = None, proposal_hash: Any = None,
        proposed_holding_count: Any = None, score_improvement: Any = None,
        one_way_turnover: Any = None, estimated_transaction_cost: Any = None,
        data_gaps: Optional[list] = None,
        cycle_complete: bool = False, cycle_run_id: Any = None,
        reassessment_state: Any = None) -> dict[str, Any]:
    """Slice 7 (Phase 29H) reallocation-proposal presentation, rendered VERBATIM by the
    UI (like the HOC presentation). Review-only, manual-review: it exposes the proposal
    state as INFORMATION and NEVER as an order/apply/confirm requirement. It never gates
    the Daily Close.

    Release 46.6.2. NOT_RUN carried one sentence for two different situations,
    and on 2026-08-28 it told the operator the wrong one. The governed cycle
    ``drc_2026-08-28_5f619736c4ba`` COMPLETED - fourteen steps, no failures -
    and its ``BUILD_REALLOCATION_PROPOSAL`` step was deliberately SKIPPED with
    the reason recorded on the manifest: "The portfolio-level economic gate did
    not clear (MANUAL_REVIEW_REQUIRED); no proposal is built and no capital is
    redeployed." The card nonetheless said the first proposal would come from
    "the next completed Daily Research Cycle" and pointed at
    ``POST /v1/operations/daily-research-cycle/run``. Running it again would
    have produced the same skip, because the constraint is a portfolio fact and
    not a missing cycle.

    So the two cases are now separated by the cycle's own state: no completed
    cycle yet (run one) versus a completed cycle whose economic gate withheld
    the proposal (adjudicate the constraint). Nothing about the gate, the
    proposal or the safety posture changes - only which true sentence is shown.
    """
    rp_state = str(state) if state else _RP_NOT_RUN
    if rp_state not in _RP_STATE_MAP:
        rp_state = _RP_NOT_RUN if not available else _RP_READY
    canonical_state, badge, severity = _RP_STATE_MAP[rp_state]
    date_txt = _iso(_coerce_date(eligible_date)) or (
        str(eligible_date) if eligible_date else None)
    title = RP_TITLE
    title_with_date = ("%s — %s" % (title, date_txt)) if date_txt else title
    counts = action_counts or {}
    gaps = list(data_gaps or [])
    has_proposal = bool(available and rp_state in (_RP_READY, _RP_DEGRADED))
    ra_state = str(reassessment_state) if reassessment_state else None
    gate_withheld = bool(cycle_complete and ra_state in _RP_GATE_DID_NOT_CLEAR)

    if not has_proposal and gate_withheld:
        headline = ("The Daily Research Cycle for %s COMPLETED and produced no "
                    "reallocation proposal: the portfolio-level economic gate "
                    "did not clear (%s)." % (date_txt or "this session",
                                             ra_state))
        explanation = ("This is a decision, not a gap. The cycle ran its "
                       "reallocation step and skipped it on purpose, so "
                       "running the Daily Research Cycle again would skip it "
                       "again. What is outstanding is the portfolio "
                       "constraint itself, which requires human adjudication "
                       "in the Active Portfolio Assessment. Review-only: no "
                       "orders, no target, no capital redeployed.")
        summary_label = "NONE — ECONOMIC GATE WITHHELD"
    elif not has_proposal:
        headline = ("No reallocation proposal has been produced for the current active "
                    "book and eligible session yet.")
        explanation = ("The first proposal will be generated by the next completed Daily "
                       "Research Cycle (after the Holding Opportunity-Cost step). "
                       "Review-only: manual review required, no orders, no target.")
        summary_label = "NONE YET"
    else:
        def _c(k):
            try:
                return int(counts.get(k, 0) or 0)
            except (TypeError, ValueError):
                return 0
        summary_label = ("%d retain / %d increase / %d reduce / %d exit / %d add / "
                         "%d replace"
                         % (_c("RETAIN"), _c("INCREASE"), _c("REDUCE"), _c("EXIT"),
                            _c("ADD"), _c("REPLACE_IN")))
        gap_txt = (" · %d data gap(s)" % len(gaps)) if gaps else ""
        headline = ("Reallocation proposal%s: %s proposed holdings, %s.%s"
                    % ((" — %s" % date_txt) if date_txt else "",
                       proposed_holding_count, summary_label, gap_txt))
        explanation = ("Review-only proposed target portfolio produced by the Daily "
                       "Research Cycle. Manual review is mandatory. It confirms no "
                       "operational or alpha target, creates no order/fill and changes "
                       "no holding/cash/NAV. The Daily Close is a separate action.")

    return {
        "title": title,
        "title_with_date": title_with_date,
        "badge": badge,
        "headline": headline,
        "explanation": explanation,
        "severity": severity,
        "state": rp_state,
        "canonical_operator_state": canonical_state,
        "available": bool(has_proposal),
        "has_proposal": has_proposal,
        "eligible_market_date": date_txt,
        "action_counts": dict(counts) if has_proposal else {},
        "summary_label": summary_label,
        "proposal_hash": proposal_hash if has_proposal else None,
        "proposed_holding_count": proposed_holding_count if has_proposal else None,
        "score_improvement": score_improvement if has_proposal else None,
        "one_way_turnover": one_way_turnover if has_proposal else None,
        "estimated_transaction_cost": estimated_transaction_cost if has_proposal else None,
        "data_gaps": gaps if has_proposal else [],
        "is_primary_decision": False,
        "decision_authority": "REVIEW_ONLY",
        "execution_available": False,
        "creates_orders": False,
        "review_only": True,
        "canonical_owner": RP_CANONICAL_OWNER,
        # Release 46.6.2 - the route that BUILDS a proposal is still the DRC,
        # but it is not the operator's next action once a completed cycle has
        # already withheld one. Both facts are stated, apart.
        "sole_execution_path": "POST /v1/operations/daily-research-cycle/run",
        "governed_cycle_complete_for_session": bool(cycle_complete),
        "governed_cycle_run_id": cycle_run_id,
        "reassessment_state": ra_state,
        "economic_gate_withheld_the_proposal": gate_withheld,
        "running_the_cycle_again_would_change_nothing": gate_withheld,
        "outstanding_action": ("MANUAL_PORTFOLIO_CONSTRAINT_REVIEW"
                               if gate_withheld else
                               ("RUN_DAILY_RESEARCH_CYCLE" if not has_proposal
                                else "REVIEW_THE_PROPOSAL")),
        "outstanding_action_owner": (PRS_CANONICAL_OWNER if gate_withheld
                                     else RP_CANONICAL_OWNER),
    }


def build_evidence_presentation(*, operational_close_valid: bool, latest_close_date: Any,
                                evidence_gap: bool, active_book_snapshot_present: bool,
                                current_session_open: bool) -> dict[str, Any]:
    """Presentation that keeps the still-open CURRENT session distinct from the
    latest completed close's evidence state (Workstream F). A documented gap on a
    valid completed close is ATTENTION-level and never an operational failure.

    ``current_session_open`` means today's market session has not yet closed/been
    processed — so no close or evidence result exists for IT yet (this is separate
    from the already-completed close, which may carry a documented gap)."""
    close_txt = _iso(_coerce_date(latest_close_date)) or (
        str(latest_close_date) if latest_close_date else None)

    if current_session_open:
        cur = {"state": "NO_RESULT_YET", "label": "No result yet",
               "explanation": "No close or evidence result exists yet for the "
                              "still-open current session."}
    else:
        cur = {"state": "SESSION_PROCESSED", "label": "Session processed",
               "explanation": "The current market session has been processed."}

    if not operational_close_valid:
        # Release 29.4 — "no completed close has EVER been recorded" and "the most recent
        # close attempt did not complete" are different facts, and the live 2026-08-18
        # payload asserted the first while a valid 2026-08-17 close with 6/6 forward
        # snapshots sat in the journal. A recorded date means a close ran: say what is
        # actually true about it and never erase it from the operator's evidence.
        if close_txt:
            comp = {"state": "NOT_COMPLETED", "label": "Last close attempt incomplete",
                    "explanation": "The most recent close attempt (%s) did not complete. "
                                   "No completed operational close has been recorded "
                                   "since." % close_txt}
        else:
            comp = {"state": "NONE", "label": "No completed close",
                    "explanation": "No completed operational close has been recorded yet."}
    elif evidence_gap:
        comp = {"state": "VALID_WITH_DOCUMENTED_GAP",
                "label": "Valid — documented forward-evidence gap",
                "explanation": "The latest completed close (%s) remains valid and has a "
                               "documented forward-evidence gap." % (close_txt or "n/a")}
    else:
        comp = {"state": "VALID", "label": "Valid — forward evidence captured",
                "explanation": "The latest completed close (%s) is valid with forward "
                               "evidence captured." % (close_txt or "n/a")}

    return {
        "current_session": cur,
        "latest_completed_close": comp,
        "documented_gap": bool(evidence_gap),
        "documented_gap_label": ("Documented forward-evidence gap" if evidence_gap
                                 else "No documented forward-evidence gap"),
        "severity": (SEV_ATTENTION if evidence_gap else SEV_INFO),
        "explanation": ("%s %s" % (cur["explanation"], comp["explanation"])).strip(),
    }


# --------------------------------------------------------------------------- #
# Stage 22 (Workstream B) — STALE-EVIDENCE PRESENTATION.
#
# BLOCKED_EVIDENCE keeps its meaning exactly: stale evidence MUST NOT drive a
# portfolio change, and it never will. What Stage 22 changes is the INFORMATION
# HIERARCHY, not the truth. Two very different situations were rendered identically
# as a large red card:
#
#   SYSTEM BLOCKER          the operator has to fix something NOW; the cycle cannot
#                           continue until they do.
#   EXPECTED STALE EVIDENCE the existing assessment is intentionally non-actionable
#                           because a newer portfolio or session exists and the NEXT
#                           canonical cycle will supersede it. Nothing is broken and
#                           nothing is required.
#
# Before the session closes — when the authoritative workflow says
# WAITING_FOR_SESSION_CLOSE and there is genuinely nothing to do — an expected stale
# assessment is DEMOTED to evidence/history: still fully visible for audit, never
# competing with the "no action required" state, never dressed as an incident.
# Nothing is hidden, no history is rewritten and no validity rule changes.
# --------------------------------------------------------------------------- #
EVIDENCE_SYSTEM_BLOCKER = "SYSTEM_BLOCKER"
EVIDENCE_EXPECTED_STALE = "EXPECTED_STALE_EVIDENCE"
EVIDENCE_CURRENT = "CURRENT_EVIDENCE"
EVIDENCE_CLASSES = (EVIDENCE_SYSTEM_BLOCKER, EVIDENCE_EXPECTED_STALE, EVIDENCE_CURRENT)

#: Presentation rank. PRIMARY competes for the operator's attention; EVIDENCE and
#: HISTORY never do.
PRESENT_PRIMARY = "PRIMARY"
PRESENT_EVIDENCE = "EVIDENCE"
PRESENT_HISTORY = "HISTORY"
PRESENTATION_CLASSES = (PRESENT_PRIMARY, PRESENT_EVIDENCE, PRESENT_HISTORY)

#: Reassessment / assessment states that fail closed on evidence.
_BLOCKED_EVIDENCE_STATES = frozenset({
    "BLOCKED_EVIDENCE", "STALE_CORPORATE_ACTION_REVIEW_REQUIRED"})
#: Reassessment states that fail closed on a missing/unusable INPUT — the operator has
#: to restore the named source, so these are never "expected".
_BLOCKED_DATA_STATES = frozenset({"BLOCKED_DATA"})

#: Blocker codes whose ONLY resolution is the next canonical cycle. Their presence
#: means the evidence is superseded by design, not that the system is broken.
_SUPERSEDABLE_BLOCKER_CODES = frozenset({
    "STALE_CORPORATE_ACTION_EVIDENCE",
    "PORTFOLIO_STATE_CHANGED_SINCE_ASSESSMENT",
    "ASSESSMENT_ELIGIBLE_DATE_MISMATCH",
    "HOLDING_OPPORTUNITY_COST_NOT_RUN",
})


def blocker_codes_of(blockers: Any) -> list[str]:
    """The sorted blocker codes of a reassessment blocker list (dicts or bare codes)."""
    return sorted({(b.get("code") if isinstance(b, dict) else str(b))
                   for b in (blockers or []) if b})


def _blocked_kind(reassessment_state: Any, blockers: Any) -> tuple[bool, bool, bool]:
    """``(is_evidence_blocked, is_data_blocked, all_causes_supersedable)`` — the ONE
    rule that decides both the evidence classification and whether the blocked
    reassessment suspends the normal cycle. Factored so those two can never disagree."""
    state = str(reassessment_state or "")
    codes = blocker_codes_of(blockers)
    return (state in _BLOCKED_EVIDENCE_STATES,
            state in _BLOCKED_DATA_STATES,
            bool(codes) and all(c in _SUPERSEDABLE_BLOCKER_CODES for c in codes))


def reassessment_blocks_cycle(*, reassessment_state: Any, blockers: Any = None) -> bool:
    """Stage 22.1 — does this reassessment leave the cycle WITHOUT a portfolio verdict?

    True when the reassessment failed closed on a missing/unusable INPUT (BLOCKED_DATA),
    or on evidence whose cause is NOT resolved simply by running the next canonical
    cycle. Both mean no portfolio-level economic verdict exists for the eligible
    session, so "no change / monitor" would be a fabricated conclusion.

    False for an EXPECTED stale assessment (every named cause is superseded by the next
    Daily Research Cycle) — nothing is broken there and nothing is required, which is
    exactly the Stage-22 Workstream-B distinction this reuses rather than re-deriving.
    """
    is_evidence, is_data, all_supersedable = _blocked_kind(reassessment_state, blockers)
    return bool(is_data or (is_evidence and not all_supersedable))


# --------------------------------------------------------------------------- #
# Release 46.2 — THE canonical PORTFOLIO-ATTENTION rule.
#
# WHY THIS EXISTS
# ---------------
# The governed 2026-08-25 Daily Research Cycle produced a portfolio reassessment of
# ``MANUAL_REVIEW_REQUIRED`` naming seven hard-constraint breaches on retained names
# (ABNB / CVS / DXCM / EXPE / ITW / LH sector weight, AMD risk contribution). The
# canonical portfolio decision correctly read BLOCKED and said, in the operator's own
# payload, that "a holding breaches a hard portfolio constraint that requires human
# adjudication". The SAME payload reported overall_state = DAILY_CYCLE_COMPLETE,
# current_task = "Monitor the portfolio." and no_action_required = true.
#
# It was not a wording bug. This module composed ``manual_review_required`` from ONE
# signal — the reassessment's ``proposal_required`` — and a constraint breach
# deliberately proposes nothing (there is no change to propose; a person must decide).
# The state was equally not "blocked": ``reassessment_blocks_cycle`` above answers a
# different question (did the reassessment reach a verdict at all?) and a constraint
# breach DID reach one. Falling through both gates, it landed in the terminal region.
#
# THE DISTINCTION THIS ENCODES
# ----------------------------
#   MUTATION   a normal-path write is offered (Daily Close / Daily Research Cycle).
#              Never produced here: a reassessment never opens a write gate.
#   REVIEW     a human must adjudicate. It creates NO order, NO proposal and NO
#              portfolio change, requires no rebalance approval, leaves automation
#              OFF and stays paper-only. Reviewing IS an action, so it can never be
#              reported as "no action required".
#   NONE       the economic gate cleared and nothing should change.
#
# This is the ONE place that decision is made. ``manual_review_required`` (which
# drives the overall state), the primary-action wording, and the cross-surface
# invariant below all read this same verdict, so they cannot disagree.
# --------------------------------------------------------------------------- #
ATTENTION_MUTATION = "MUTATION"
ATTENTION_REVIEW = "REVIEW"
ATTENTION_NONE = "NONE"
PORTFOLIO_ATTENTION_KINDS = (ATTENTION_MUTATION, ATTENTION_REVIEW, ATTENTION_NONE)

#: Review reasons, in precedence order.
REVIEW_REASON_CONSTRAINT_BREACH = "PORTFOLIO_CONSTRAINT_BREACH"
REVIEW_REASON_PROPOSAL = "PORTFOLIO_PROPOSAL"

#: The canonical operator actions for each review reason. Both already exist in the
#: reassessment owner's own vocabulary (api.portfolio_reassessment._PRESENTATION);
#: they are mirrored here as literals so this module stays import-pure.
ACTION_REVIEW_PORTFOLIO_CONSTRAINT_BREACH = "REVIEW_PORTFOLIO_CONSTRAINT_BREACH"

#: Reassessment states that require HUMAN ADJUDICATION while proposing no change.
_PRS_REVIEW_REQUIRED_STATES = frozenset({"MANUAL_REVIEW_REQUIRED"})

#: Per-holding hard-constraint blocker codes, mirrored from
#: engine.portfolio_reassessment (which emits them as ``"<TICKER>:<CODE>"``).
CONSTRAINT_BREACH_CODES = ("NAME_WEIGHT_BREACH", "SECTOR_WEIGHT_BREACH",
                           "RISK_CONTRIBUTION_BREACH")


def constraint_breaches_of(blockers: Any) -> list[str]:
    """The ``TICKER:CODE`` hard-constraint breaches in a reassessment blocker list."""
    return [c for c in blocker_codes_of(blockers)
            if any(c.endswith(":" + code) or c == code
                   for code in CONSTRAINT_BREACH_CODES)]


def portfolio_attention(*, reassessment_state: Any, proposal_required: bool,
                        blockers: Any = None,
                        execution_active: bool = False,
                        held_name_breaches: Any = None) -> dict[str, Any]:
    """THE portfolio-attention verdict: MUTATION, REVIEW or NONE.

    Pure. Reads the reassessment owner's already-decided state and its own blocker
    list; computes no economics, opens no gate and never turns a review into a change.

    ``execution_active`` applies the unchanged Stage-19 precedence: while paper orders
    from a confirmed plan are still working, a newly produced PROPOSAL must not compete
    with the execution lifecycle. A hard-constraint breach is NOT a proposal — it is a
    named condition of the book the operator has to adjudicate either way — so it is
    reported regardless, exactly as ``reassessment_blocks_cycle`` is.
    """
    state = str(reassessment_state or "")
    # Release 47 — a per-holding cap breach is no longer a BLOCKER (it asks for a
    # target instead), so it no longer arrives through ``blockers``. It is still the
    # operator's business to see WHICH names breached, so the reassessment owner's
    # own published list is accepted here and unioned with any legacy blocker form.
    # Reading it changes nothing: the verdict below still keys off the state.
    breaches = sorted(set(constraint_breaches_of(blockers))
                      | {str(b) for b in (held_name_breaches or []) if b})
    constraint_review = state in _PRS_REVIEW_REQUIRED_STATES
    proposal_review = bool(proposal_required) and not bool(execution_active)

    if constraint_review:
        reason, action = (REVIEW_REASON_CONSTRAINT_BREACH,
                          ACTION_REVIEW_PORTFOLIO_CONSTRAINT_BREACH)
    elif proposal_review:
        reason, action = (REVIEW_REASON_PROPOSAL, ACTION_REVIEW_PORTFOLIO_PROPOSAL)
    else:
        reason, action = None, None

    review = bool(constraint_review or proposal_review)
    return {
        "attention_kind": ATTENTION_REVIEW if review else ATTENTION_NONE,
        "attention_vocabulary": list(PORTFOLIO_ATTENTION_KINDS),
        "review_required": review,
        "review_reason": reason,
        "operator_action": action,
        "reassessment_state": state or None,
        "constraint_breaches": breaches,
        "constraint_breach_count": len(breaches),
        "proposal_required": bool(proposal_required),
        "proposal_review_suppressed_by_execution": bool(
            proposal_required and execution_active),
        # What a REVIEW is not. Stated on the object so no surface has to assume it.
        "creates_orders": False,
        "creates_proposal": False,
        "mutates_portfolio": False,
        "requires_rebalance_approval": False,
        "automation_off": True,
        "paper_only": True,
        "is_a_mutation": False,
        "calculation_owner": WORKFLOW_STATE_OWNER,
        "reassessment_owner": PRS_CANONICAL_OWNER,
    }


#: Overall states that assert the eligible session needs nothing from the operator.
#: A portfolio-attention REVIEW may never coexist with one of these.
_NO_ATTENTION_OVERALL_STATES = frozenset({DAILY_CYCLE_COMPLETE,
                                          DAILY_CYCLE_COMPLETE_EVIDENCE_GAP,
                                          WAITING_FOR_SESSION_CLOSE})


def portfolio_attention_violations(*, attention: dict, overall: Any,
                                   cycle_no_action_required: Any,
                                   cycle_review_required: Any = None,
                                   primary_action_code: Any = None) -> list[dict]:
    """THE strict cross-surface invariant (Release 46.2).

    If the canonical portfolio attention requires human review, then the overall
    workflow may NOT report a completion state and the normal cycle may NOT report
    ``no_action_required``. Any surface that manages to do both is INCONSISTENT — the
    exact contradiction the governed 2026-08-25 cycle shipped.
    """
    if not (attention or {}).get("review_required"):
        return []
    ov = str(overall)
    common = {"concept": "portfolio_attention",
              "overall_state": ov,
              "reassessment_state": attention.get("reassessment_state"),
              "review_reason": attention.get("review_reason"),
              "constraint_breaches": attention.get("constraint_breaches"),
              "authoritative_owners": [PRS_CANONICAL_OWNER, WORKFLOW_STATE_OWNER],
              "surface": "workflow_state"}
    out: list[dict] = []
    if ov in _NO_ATTENTION_OVERALL_STATES:
        out.append({**common, "code": "PORTFOLIO_ATTENTION_CONTRADICTION",
                    "detail": ("the canonical portfolio reassessment requires human "
                               "review while the overall workflow state asserts the "
                               "eligible session is complete")})
    if bool(cycle_no_action_required):
        out.append({**common, "code": "PORTFOLIO_ATTENTION_NO_ACTION_CONTRADICTION",
                    "detail": ("the canonical portfolio reassessment requires human "
                               "review while the normal cycle reports "
                               "no_action_required")})
    if cycle_review_required is not None and not bool(cycle_review_required):
        out.append({**common, "code": "PORTFOLIO_ATTENTION_REVIEW_GATE_CLOSED",
                    "detail": ("the canonical portfolio reassessment requires human "
                               "review while the normal cycle opened no review gate"),
                    "primary_action": primary_action_code})
    return out


def build_evidence_classification(*, reassessment_state: Any, overall: str,
                                  blockers: Any = None,
                                  eligible_date: Any = None,
                                  next_cycle_action: Any = None) -> dict[str, Any]:
    """Classify a blocked/stale assessment as a SYSTEM BLOCKER or EXPECTED STALE.

    Fail-closed semantics are untouched: ``blocks_portfolio_action`` is True for every
    blocked/stale state regardless of classification. Only the presentation rank moves.
    """
    state = str(reassessment_state or "")
    codes = blocker_codes_of(blockers)
    is_evidence_blocked, is_data_blocked, all_supersedable = _blocked_kind(state, blockers)
    blocked = bool(is_evidence_blocked or is_data_blocked)

    if not blocked:
        return {
            "classification": EVIDENCE_CURRENT,
            "presentation_class": PRESENT_PRIMARY,
            "demoted": False,
            "competes_with_primary_action": True,
            "is_operational_incident": False,
            "requires_operator_fix": False,
            "blocks_portfolio_action": False,
            "severity": SEV_INFO,
            "state": state or None,
            "blocker_codes": codes,
            "headline": None,
            "explanation": None,
            "superseded_by": None,
            "audit_visible": True,
            "history_rewritten": False,
            "validity_rules_changed": False,
        }

    # An evidence block whose every named cause is resolved by the next canonical cycle,
    # while the authoritative workflow reports a passive or normal-cycle state, is
    # EXPECTED. A data block, an unknown cause, or a workflow already in recovery is a
    # SYSTEM BLOCKER the operator must act on.
    workflow_healthy = overall not in (INCONSISTENT_STATE, RESEARCH_CYCLE_BLOCKED)
    expected = bool(is_evidence_blocked and all_supersedable and workflow_healthy)

    if expected:
        passive = overall in _PASSIVE_STATES
        return {
            "classification": EVIDENCE_EXPECTED_STALE,
            "presentation_class": (PRESENT_HISTORY if passive else PRESENT_EVIDENCE),
            "demoted": True,
            "competes_with_primary_action": False,
            "is_operational_incident": False,
            "requires_operator_fix": False,
            "blocks_portfolio_action": True,
            "severity": SEV_INFO,
            "state": state,
            "blocker_codes": codes,
            "headline": ("Superseded assessment — kept as evidence, not an incident."),
            "explanation": (
                "This assessment is intentionally non-actionable: the portfolio or the "
                "session moved on after it was produced, so it correctly cannot drive a "
                "portfolio change. Nothing is broken and nothing is required of you — "
                "%s will replace it. The record is preserved unchanged for audit."
                % (str(next_cycle_action) if next_cycle_action
                   else "the next Daily Research Cycle")),
            "superseded_by": (str(next_cycle_action) if next_cycle_action
                              else "the next Daily Research Cycle"),
            "audit_visible": True,
            "history_rewritten": False,
            "validity_rules_changed": False,
        }

    return {
        "classification": EVIDENCE_SYSTEM_BLOCKER,
        "presentation_class": PRESENT_PRIMARY,
        "demoted": False,
        "competes_with_primary_action": True,
        "is_operational_incident": True,
        "requires_operator_fix": True,
        "blocks_portfolio_action": True,
        "severity": SEV_BLOCKED,
        "state": state,
        "blocker_codes": codes,
        "headline": "Assessment blocked — the named cause must be resolved.",
        "explanation": (
            "The portfolio assessment for %s cannot be produced until the named "
            "blocker is resolved: %s. No portfolio change may be driven from blocked "
            "evidence."
            % (eligible_date or "the eligible session",
               ", ".join(codes) if codes else "cause not reported")),
        "superseded_by": None,
        "audit_visible": True,
        "history_rewritten": False,
        "validity_rules_changed": False,
    }


# --------------------------------------------------------------------------- #
# Stage 22 (Workstream E) — FRESH ASSESSMENT / PROPOSAL BINDING.
#
# After the Daily Research Cycle completes, a fresh assessment must be provably bound
# to the session and the portfolio it describes, and a proposal must be bound to that
# exact assessment. Every check below fails CLOSED — but EXACTLY ONCE: the verdict is
# computed here and every surface reads it, so a single broken binding is stated one
# time rather than restated as four separate red cards.
#
# UNVERIFIABLE is never treated as broken. An artifact that recorded no fingerprint
# cannot prove currency either way, and inferring staleness from a missing value is
# fabrication (the same rule api.portfolio_reassessment.economic_currency applies).
# --------------------------------------------------------------------------- #
BINDING_CURRENT = "BOUND_AND_CURRENT"
BINDING_STALE = "BINDING_BROKEN"
BINDING_UNVERIFIABLE = "BINDING_UNVERIFIABLE"
BINDING_ABSENT = "NO_ASSESSMENT"
BINDING_STATES = (BINDING_CURRENT, BINDING_STALE, BINDING_UNVERIFIABLE, BINDING_ABSENT)


def build_assessment_binding(*, eligible_date: Any, active_book_id: Any,
                             hoc_available: bool, hoc_bound_date: Any,
                             hoc_bound_book: Any, hoc_assessment_hash: Any,
                             hoc_stale: bool = False, hoc_stale_reason: Any = None,
                             proposal_available: bool = False,
                             proposal_bound_assessment_hash: Any = None,
                             proposal_bound_date: Any = None,
                             proposal_hash: Any = None) -> dict[str, Any]:
    """One fail-closed verdict on assessment currency and proposal binding."""
    elig = _iso(_coerce_date(eligible_date)) or (
        str(eligible_date) if eligible_date else None)
    checks: list[dict[str, Any]] = []

    def check(name, *, ok: Optional[bool], expected, actual, detail):
        checks.append({"check": name, "passed": ok, "verifiable": ok is not None,
                       "expected": expected, "actual": actual, "detail": detail})

    if not hoc_available:
        return {
            "state": BINDING_ABSENT,
            "bound_and_current": False,
            "fails_closed": True,
            "checks": checks,
            "failed_checks": [],
            "unverifiable_checks": [],
            "reason": "NO_ASSESSMENT_FOR_ELIGIBLE_SESSION",
            "explanation": ("No Holding Opportunity-Cost assessment exists for the "
                            "eligible session %s, so there is nothing to bind. The "
                            "Daily Research Cycle produces one." % (elig or "n/a")),
            "eligible_market_date": elig,
            "assessment_hash": None,
            "proposal_hash": None,
            "proposal_bound_to_current_assessment": False,
            "stated_once": True,
        }

    bound_date = _iso(_coerce_date(hoc_bound_date)) or (
        str(hoc_bound_date) if hoc_bound_date else None)
    check("assessment_market_date_equals_eligible_session",
          ok=(None if (bound_date is None or elig is None) else bound_date == elig),
          expected=elig, actual=bound_date,
          detail="The assessment must cover the latest eligible completed session.")
    check("assessment_bound_to_active_book",
          ok=(None if (not hoc_bound_book or not active_book_id)
              else str(hoc_bound_book) == str(active_book_id)),
          expected=active_book_id, actual=hoc_bound_book,
          detail="The assessment must describe the CURRENT active operational book.")
    check("corporate_action_registry_unchanged",
          ok=(not bool(hoc_stale)),
          expected="registry fingerprint unchanged since the assessment",
          actual=(hoc_stale_reason or "unchanged"),
          detail="A corporate action registered afterwards invalidates the evidence.")

    if proposal_available:
        check("proposal_binds_to_current_assessment",
              ok=(None if (not proposal_bound_assessment_hash or not hoc_assessment_hash)
                  else str(proposal_bound_assessment_hash) == str(hoc_assessment_hash)),
              expected=hoc_assessment_hash, actual=proposal_bound_assessment_hash,
              detail="A proposal may only be reviewed against the assessment it was "
                     "derived from.")
        p_date = _iso(_coerce_date(proposal_bound_date)) or (
            str(proposal_bound_date) if proposal_bound_date else None)
        check("proposal_covers_eligible_session",
              ok=(None if (p_date is None or elig is None) else p_date == elig),
              expected=elig, actual=p_date,
              detail="The proposal must cover the same eligible completed session.")

    failed = [c["check"] for c in checks if c["passed"] is False]
    unverifiable = [c["check"] for c in checks if c["passed"] is None]
    if failed:
        state, ok = BINDING_STALE, False
        explanation = (
            "The current assessment no longer describes what it claims to: %s. It "
            "remains readable as immutable evidence, but it may not drive a portfolio "
            "change. The next Daily Research Cycle reassesses against the current "
            "state." % ", ".join(failed))
    elif unverifiable:
        state, ok = BINDING_UNVERIFIABLE, False
        explanation = (
            "The assessment predates part of the binding contract (%s), so whether it "
            "still describes the current portfolio cannot be proven either way. "
            "Nothing is inferred from the missing value." % ", ".join(unverifiable))
    else:
        state, ok = BINDING_CURRENT, True
        explanation = ("The assessment covers the eligible session %s, describes the "
                       "current active book and the current corporate-action registry"
                       "%s." % (elig or "n/a",
                                ", and the proposal binds to it"
                                if proposal_available else ""))
    prop_bound = bool(
        proposal_available and not failed
        and proposal_bound_assessment_hash and hoc_assessment_hash
        and str(proposal_bound_assessment_hash) == str(hoc_assessment_hash))
    return {
        "state": state,
        "bound_and_current": ok,
        "fails_closed": True,
        "checks": checks,
        "failed_checks": failed,
        "unverifiable_checks": unverifiable,
        "reason": (failed[0] if failed else (unverifiable[0] if unverifiable else None)),
        "explanation": explanation,
        "eligible_market_date": elig,
        "assessment_hash": hoc_assessment_hash,
        "proposal_hash": proposal_hash,
        "proposal_bound_to_current_assessment": prop_bound,
        # The verdict is computed ONCE here; surfaces render it and never re-derive it,
        # so a single broken binding is reported exactly once.
        "stated_once": True,
    }


# --------------------------------------------------------------------------- #
# Stage 19.3 — OPERATOR COMMAND (Workstream F). The ONE compact answer to
# "WHAT DO I DO NOW?", generated by the backend so every surface renders the SAME
# five fields instead of re-interpreting raw state. It is a projection of the
# already-decided overall state + canonical primary action — it decides nothing,
# re-derives nothing and writes nothing. Every operator page (Today, Portfolio,
# Portfolio Manager, Daily Workflow, the right action rail) MIRRORS this block.
# --------------------------------------------------------------------------- #
#: Stage 22 — ONE vocabulary. This is a complete sentence because every surface renders
#: it as prose (the command bar's no-action line and the right rail's next-action line).
#: The UI holds no literal of its own; it renders this value verbatim.
NO_ACTION_TEXT = "No action required right now."

#: Overall states in which NO normal-path workflow mutation may be offered. The
#: operator is waiting or reviewing; any execute-looking control here is a defect.
_PASSIVE_STATES = frozenset({
    WAITING_FOR_SESSION_CLOSE, RESEARCH_CYCLE_RUNNING, RESEARCH_CYCLE_BLOCKED,
    PORTFOLIO_REASSESSMENT_REQUIRED, MANUAL_REVIEW_REQUIRED,
    DAILY_CYCLE_COMPLETE, DAILY_CYCLE_COMPLETE_EVIDENCE_GAP})


def build_operator_command(*, overall: str, primary: dict,
                           pending_orders: int = 0,
                           eligible_date: Any = None,
                           latest_close_date: Any = None,
                           cycle: Optional[dict] = None) -> dict[str, Any]:
    """Project the canonical state + primary action into the ONE operator command bar.

    ``primary_action_available`` is the single authority for whether a normal-path
    mutation CTA may render anywhere on any page. When it is False the surfaces show
    the state, the task and ``next_text`` — and NO enabled workflow execution control.
    """
    elig = _iso(_coerce_date(eligible_date)) or (
        str(eligible_date) if eligible_date else None)
    closed = _iso(_coerce_date(latest_close_date)) or (
        str(latest_close_date) if latest_close_date else None)
    kind = primary.get("execution_kind")
    # A maintenance executor can never become the operator's promoted action.
    executable = bool(primary.get("execution_available")
                      and kind in NORMAL_PATH_EXECUTION_KINDS)
    passive = overall in _PASSIVE_STATES or not executable

    if executable:
        # Release 48 — the operator confirms the ONE cycle token; the composed
        # owners' own tokens are supplied by the orchestration owner.
        next_text = "Confirm to run — %s" % PORTFOLIO_CYCLE_CONFIRMATION
    elif overall == WAITING_FOR_SESSION_CLOSE:
        next_text = NO_ACTION_TEXT
    elif overall in (DAILY_CYCLE_COMPLETE, DAILY_CYCLE_COMPLETE_EVIDENCE_GAP):
        next_text = NO_ACTION_TEXT
    else:
        next_text = primary.get("label") or NO_ACTION_TEXT

    supporting = None
    if executable and kind == EXEC_DAILY_CLOSE:
        supporting = (
            "This Daily Close will refresh owned marks and settle eligible NEXT_CLOSE "
            "paper orders before reassessing the portfolio.")
        if pending_orders > 0:
            supporting += (" %d paper order(s) are currently working."
                           % int(pending_orders))
        supporting += (
            " The portfolio cycle runs it first, continues with the Daily Research "
            "Cycle when the workflow requires it, and stops at the governed "
            "portfolio decision — nothing is approved or executed automatically.")
    elif executable and kind == EXEC_DAILY_RESEARCH_CYCLE:
        supporting = (
            "The portfolio cycle runs the Daily Research Cycle — signal refresh, "
            "opportunity-cost assessment, portfolio reassessment and, when one is "
            "justified, a review-only reallocation proposal — and stops at the "
            "governed portfolio decision. Nothing is approved or executed "
            "automatically.")
    elif pending_orders > 0 and passive:
        supporting = ("%d paper order(s) are working and settle at the next eligible "
                      "completed close — monitoring only, no action required."
                      % int(pending_orders))

    # Stage 22 (Workstream G): the operator must be able to read the whole state in
    # about five seconds — WHAT IS HAPPENING NOW / WHAT DO I NEED TO DO / WHY / WHAT
    # HAPPENS AFTER THAT. The first three already existed; the fourth is supplied by
    # the canonical cycle contract so no surface writes its own "and then…" copy.
    cyc = cycle or {}
    return {
        # -- the five operator fields -------------------------------------- #
        "state": overall,
        "state_label": str(overall).replace("_", " "),
        "task": primary.get("current_task"),
        "why": primary.get("explanation"),
        "next_text": next_text,
        "supporting_text": supporting,
        # -- the canonical cycle position (Workstream A / G) ----------------- #
        "now_text": cyc.get("now_text"),
        "after_text": cyc.get("after_text"),
        "cycle_stage": cyc.get("current_stage"),
        "cycle_stage_label": cyc.get("current_stage_label"),
        "cycle_stage_ordinal": cyc.get("current_stage_ordinal"),
        "cycle_next_stage": cyc.get("next_stage"),
        "cycle_next_stage_label": cyc.get("next_stage_label"),
        "cycle_stage_count": len(ncycle.STAGE_SEQUENCE),
        # -- the ONE primary action (or none) ------------------------------- #
        # Release 48: whenever a normal-path mutation is due, the operator sees
        # ONE concept — the portfolio cycle — executed through the ONE
        # orchestration owner. The decided underlying step travels beside it.
        "primary_action_available": executable,
        "primary_action_label": PORTFOLIO_CYCLE_LABEL if executable else None,
        "primary_action_code": ("RUN_PORTFOLIO_CYCLE" if executable else None),
        "primary_action_kind": EXEC_PORTFOLIO_CYCLE if executable else None,
        "confirmation_required": (PORTFOLIO_CYCLE_CONFIRMATION
                                  if executable else None),
        "primary_action_owner": PORTFOLIO_CYCLE_OWNER if executable else None,
        "primary_action_execution_contract": (
            dict(PORTFOLIO_CYCLE_EXECUTION_CONTRACT) if executable else None),
        "cycle_underlying_kind": kind if executable else None,
        "cycle_underlying_label": primary.get("label") if executable else None,
        "cycle_underlying_action_code": (primary.get("action_code")
                                         if executable else None),
        "destination": primary.get("destination"),
        "focus": primary.get("focus"),
        "severity": primary.get("severity"),
        # -- hard UX invariants every surface obeys verbatim ---------------- #
        "passive": passive,
        "mutation_controls_allowed": executable,
        "maintenance_execution_kinds": list(MAINTENANCE_EXECUTION_KINDS),
        "eligible_market_date": elig,
        "latest_completed_close_date": closed,
        "note": ("One canonical operator command. Every page mirrors this block and "
                 "renders at most one normal-path workflow action; recovery and "
                 "diagnostic controls live in a separate collapsed area."),
    }


def build_daily_close_gate(overall: str, *, eligible_date: Any = None,
                           latest_close_date: Any = None) -> dict[str, Any]:
    """Operator Action Integrity (Defect 3): the ONE canonical Daily-Close
    availability verdict for every SECONDARY close surface. Only the canonical
    READY_FOR_DAILY_CLOSE primary action may expose an executable Daily Close
    control; in every other overall state a secondary panel shows the passive
    status below and NEVER an executable-looking close button. This is pure
    presentation over the already-decided overall state — no re-derivation, no
    second priority engine, no write. An unknown state fails CLOSED."""
    elig = _iso(_coerce_date(eligible_date)) or (
        str(eligible_date) if eligible_date else None)
    closed = _iso(_coerce_date(latest_close_date)) or (
        str(latest_close_date) if latest_close_date else None)
    # Stage 19.3: WAITING_FOR_OWNED_DATA now ALSO promotes the canonical Daily Close
    # (it is the owner that advances owned marks through the Paper Desk), so the close
    # control is legitimately available in both post-close states. Every other state
    # stays passive, and an unknown state still fails CLOSED.
    allowed = overall in (READY_FOR_DAILY_CLOSE, WAITING_FOR_OWNED_DATA)
    if allowed:
        passive, badge = None, None
    elif overall in (DAILY_CYCLE_COMPLETE, DAILY_CYCLE_COMPLETE_EVIDENCE_GAP,
                     MANUAL_REVIEW_REQUIRED):
        passive = ("Daily Close complete for %s."
                   % (closed or elig or "the eligible session"))
        badge = "COMPLETE"
    elif overall == WAITING_FOR_SESSION_CLOSE:
        passive = "Daily Close waiting for the next market session to close."
        badge = "WAITING"
    elif overall in (RESEARCH_CYCLE_REQUIRED, PORTFOLIO_REASSESSMENT_REQUIRED):
        # Stage 22: under the canonical cycle these states are only reachable AFTER the
        # eligible session's close is complete (an unclosed session is READY_FOR_DAILY_
        # CLOSE by P3.7). The old wording — "Daily Close waiting for the Daily Research
        # Cycle" — inverted the actual dependency and contradicted the cycle contract
        # every surface now reads.
        passive = ("Daily Close complete for %s; the Daily Research Cycle is next."
                   % (closed or elig or "the eligible session"))
        badge = "COMPLETE"
    elif overall == RESEARCH_CYCLE_RUNNING:
        # A run is in flight. This state IS reachable with the close still pending, so
        # the honest statement is about precedence, not about completion.
        passive = ("Daily Close is not the current action — a Daily Research Cycle run "
                   "is in progress.")
        badge = "WAITING"
    elif overall == RESEARCH_CYCLE_BLOCKED:
        passive = "Daily Close unavailable — resolve the named research blocker first."
        badge = "BLOCKED"
    else:  # INCONSISTENT_STATE and any unknown state
        passive = "Daily Close unavailable — resolve the state inconsistency first."
        badge = "BLOCKED"
    return {"execution_allowed": allowed, "overall_state": overall,
            "passive_status": passive, "passive_badge": badge}


# --------------------------------------------------------------------------- #
# Stage 18 — model-recalibration REVIEW derivation (separate lane). A model REVIEW
# is raised ONLY when a signal/ranking degradation research flag is *actionable*
# (evidence sufficient). Performance symptoms (negative excess, drawdown, cost-
# adjusted alpha) and realized P&L NEVER trigger it. Never a retrainer/promoter.
# --------------------------------------------------------------------------- #
def _derive_model_review(forward_status: Optional[dict]) -> dict[str, Any]:
    flags = list((forward_status or {}).get("research_flags") or [])
    triggering = [f for f in flags
                  if isinstance(f, dict) and f.get("flag") in _SIGNAL_DEGRADATION_FLAGS
                  and bool(f.get("actionable"))]
    # Observation-only signal-degradation flags (present but not yet actionable — thin
    # evidence) are surfaced as the missing-evidence reason, never as a trigger.
    watching = [f for f in flags
                if isinstance(f, dict) and f.get("flag") in _SIGNAL_DEGRADATION_FLAGS
                and not bool(f.get("actionable"))]
    insufficient = [f for f in flags
                    if isinstance(f, dict) and f.get("flag") == "INSUFFICIENT_SAMPLE"]
    state = MODEL_RECALIBRATION_REVIEW if triggering else MODEL_HEALTHY
    reasons = [{"flag": f.get("flag"), "metric": f.get("metric"), "value": f.get("value"),
                "threshold": f.get("threshold")} for f in triggering]
    missing = None
    if state == MODEL_HEALTHY and (watching or insufficient):
        src = (insufficient or watching)[0]
        missing = ("Signal-degradation evidence is not yet actionable: %s (%s)."
                   % (src.get("threshold") or "evidence below interpretation floor",
                      src.get("flag")))
    return {
        "model_review_state": state,
        "model_review_state_vocabulary": list(MODEL_REVIEW_STATES),
        "model_review_required": bool(triggering),
        "review_only": True,
        "automatic_retraining_allowed": False,
        "automatic_promotion_allowed": False,
        "triggering_flags": reasons,
        "observation_only_signal_flags": [f.get("flag") for f in watching],
        "missing_evidence": missing,
        "basis": ("A model recalibration REVIEW is raised only by an actionable "
                  "signal/ranking degradation flag; portfolio performance weakness and "
                  "realized P&L never trigger it."),
    }


# --------------------------------------------------------------------------- #
# Stage 18 — the backend-composed "Today" hero (three explicit lanes rendered
# verbatim by the UI). This does NOT mutate overall_state / primary_action: it
# composes the operational, portfolio-decision and model lanes into the single
# headline + CTA the operator sees, so a completed operational close is never
# conflated with "no capital-redeployment decision to make". When the operational
# lane still needs action the hero mirrors the operational primary action; only
# when the operational cycle is complete does the portfolio / model lane surface.
# --------------------------------------------------------------------------- #
_OPERATIONAL_COMPLETE_STATES = frozenset({DAILY_CYCLE_COMPLETE,
                                          DAILY_CYCLE_COMPLETE_EVIDENCE_GAP})
_PD_REVIEW_STATES = frozenset({"PROPOSAL_REVIEW_REQUIRED",
                               "STALE_PROPOSAL_REVIEW_REQUIRED", "PROPOSAL_HELD"})


def _build_today_hero(*, overall: str, primary: dict, eligible_date: Any,
                      operational_close_valid: bool, latest_close_date: Any,
                      portfolio_decision_lane: dict, model_review: dict) -> dict[str, Any]:
    close_txt = _iso(_coerce_date(latest_close_date)) or (
        str(latest_close_date) if latest_close_date else None)
    operational_lane = {
        "lane": "OPERATIONAL",
        "state": overall,
        "headline": primary.get("headline"),
        "detail": primary.get("explanation"),
        "severity": primary.get("severity"),
        "complete": overall in _OPERATIONAL_COMPLETE_STATES,
    }
    pd_state = portfolio_decision_lane.get("portfolio_decision_state")
    portfolio_lane = {
        "lane": "PORTFOLIO",
        "state": pd_state,
        "label": portfolio_decision_lane.get("label"),
        "requires_manual_review": bool(portfolio_decision_lane.get("requires_manual_review")),
        "material": bool(portfolio_decision_lane.get("material")),
        "one_way_turnover": portfolio_decision_lane.get("one_way_turnover"),
        "proposed_holding_count": portfolio_decision_lane.get("proposed_holding_count"),
        "decision": portfolio_decision_lane.get("decision"),
    }
    model_lane = {
        "lane": "MODEL",
        "state": model_review.get("model_review_state"),
        "review_required": bool(model_review.get("model_review_required")),
    }

    # Choose the single hero focus. The operational lane wins while it still needs
    # action; once complete, an unreviewed material portfolio proposal wins over
    # plain monitoring; a model review is surfaced but never outranks the portfolio
    # decision (both are shown as lanes regardless).
    if overall not in _OPERATIONAL_COMPLETE_STATES:
        focus_lane = "OPERATIONAL"
        headline = primary.get("headline")
        detail = primary.get("explanation")
        severity = primary.get("severity")
        cta_label = primary.get("label")
        cta_action = primary.get("action_code")
        cta_destination = primary.get("destination")
        cta_focus = primary.get("focus")
    elif pd_state in _PD_REVIEW_STATES:
        focus_lane = "PORTFOLIO"
        _verb = ("awaiting manual review" if pd_state == "PROPOSAL_REVIEW_REQUIRED"
                 else ("changed since review — re-review required"
                       if pd_state == "STALE_PROPOSAL_REVIEW_REQUIRED"
                       else "held / deferred — decide to proceed"))
        headline = ("Daily close complete for %s — portfolio proposal %s."
                    % (close_txt or (eligible_date or "the eligible session"), _verb))
        detail = ("The operational Daily Close is complete and valid, but the current "
                  "reallocation proposal contains material capital-allocation changes "
                  "(%s proposed holdings, %s one-way turnover) that have not been "
                  "approved, rejected or deferred. Manual review is required — reviewing "
                  "creates no order."
                  % (portfolio_decision_lane.get("proposed_holding_count"),
                     _fmt_pct(portfolio_decision_lane.get("one_way_turnover"))))
        severity = SEV_ATTENTION
        cta_label = "Review proposed portfolio"
        cta_action = ACTION_REVIEW_PROPOSED_PORTFOLIO
        cta_destination = DEST_PORTFOLIO_MANAGER
        cta_focus = "realloc-card"
    elif model_review.get("model_review_state") == MODEL_RECALIBRATION_REVIEW:
        focus_lane = "MODEL"
        headline = ("Daily close complete for %s — champion recalibration review due."
                    % (close_txt or (eligible_date or "the eligible session")))
        detail = ("The operational cycle is complete and no portfolio change is pending, "
                  "but a signal/ranking degradation review threshold has been met. "
                  "Recalibration is a manually-approved controlled study — nothing is "
                  "retrained or promoted automatically.")
        severity = SEV_ATTENTION
        cta_label = "Review model evidence"
        cta_action = "REVIEW_MODEL_RECALIBRATION"
        cta_destination = DEST_RESEARCH_AUDIT
        cta_focus = None
    else:
        focus_lane = "OPERATIONAL"
        headline = primary.get("headline")
        detail = primary.get("explanation")
        severity = primary.get("severity")
        cta_label = primary.get("label")
        cta_action = primary.get("action_code")
        cta_destination = primary.get("destination")
        cta_focus = primary.get("focus")

    return {
        "focus_lane": focus_lane,
        "headline": headline,
        "detail": detail,
        "severity": severity,
        "cta_label": cta_label,
        "cta_action_code": cta_action,
        "cta_destination": cta_destination,
        "cta_focus": cta_focus,
        # The CTA is a REVIEW/routing control only; the portfolio decision itself is
        # recorded by the dedicated owner under its own confirmation token.
        "cta_execution_available": False,
        "cta_creates_orders": False,
        "operational_lane": operational_lane,
        "portfolio_lane": portfolio_lane,
        "model_lane": model_lane,
        "lanes_are_independent": True,
        "note": ("Operational close, portfolio decision and model governance are three "
                 "independent lanes. A completed close never implies the portfolio "
                 "proposal was reviewed or that a model review is or is not due."),
    }


def _fmt_pct(x: Any) -> str:
    try:
        return "%.1f%%" % (float(x) * 100.0)
    except (TypeError, ValueError):
        return "n/a"


# --------------------------------------------------------------------------- #
# Deterministic priority policy (Workstream E). Returns the single overall state.
# First matching condition wins; the exact precedence is documented inline and in
# docs/ARCHITECTURE_DECISIONS.md (D-12).
# --------------------------------------------------------------------------- #
def _decide_overall(*, inconsistent: bool, session_status: str,
                    has_confirmed_eligible: bool, eligible_session_closed: bool,
                    owned_data_lag: bool, research_current: bool,
                    assessment_status: str, manual_review_required: bool,
                    evidence_gap: bool, cycle_running: bool = False,
                    cycle_blocked: bool = False, cycle_inconsistent: bool = False,
                    cycle_complete: bool = False, hoc_current: bool = False,
                    research_cycle_due_after_close: bool = False,
                    reassessment_blocked: bool = False) -> str:
    # P1 — an inconsistent authoritative state takes highest priority. Phase 29G.3: a Daily
    #      Research Cycle status of INCONSISTENT (e.g. terminal downstream artifacts exist but
    #      the run manifest is missing → a safe idempotent recovery is required) is a genuine
    #      inconsistency and is surfaced here — never masked as NOT_STARTED / "assessment not
    #      run".
    if inconsistent or session_status == msession.INCONSISTENT_FUTURE_DATA \
            or assessment_status == ASSESS_INCONSISTENT or cycle_inconsistent:
        return INCONSISTENT_STATE

    # P2 — the current market session has not closed AND the latest eligible
    #      completed session is already fully processed → nothing to do but wait.
    #
    #      RELEASE 46.2 REPAIR. This gate asserts "nothing to do but wait", and that
    #      claim is false while a canonical portfolio review is outstanding. R46.2
    #      lifted a required review above the two COMPLETION states but left it below
    #      this one, so the identical contradiction survived at a different hour: after
    #      the 2026-08-25 cycle the operator was told at 09:48 the next morning to
    #      "wait for the current market session to close" and that no action was
    #      required, while the same payload named seven holdings breaching a hard
    #      constraint that "requires human adjudication". The payload even declared
    #      itself INCONSISTENT — the R46.2 invariant fired and nothing above it moved.
    #
    #      A hard-constraint breach is a standing condition of the BOOK, not a step of
    #      the daily cycle: it is review-only, it needs no fresh data and it can be
    #      adjudicated at any hour, so a clock that has not yet struck 16:00 is no
    #      reason to hide it. This does NOT let a review outrank work: every gate below
    #      that NAMES something to do (an unconfirmed session, an in-flight or blocked
    #      cycle, a due Daily Close, a due research cycle, a due reassessment) still
    #      outranks it, because those states never claim nothing is outstanding.
    #      Precisely the three states that DO make that claim — this one and the two
    #      completion states — are the ones a required review now outranks, which is
    #      exactly the set the cross-surface invariant names.
    if session_status == msession.BEFORE_SESSION_CLOSE and eligible_session_closed \
            and not manual_review_required:
        return WAITING_FOR_SESSION_CLOSE

    # P3 — the expected session is not confirmed by owned data (or no confirmed
    #      session exists) → wait for / refresh owned market data. INVARIANT
    #      (Phase 29D.1 live-acceptance): unresolved current-session owned data
    #      ALWAYS outranks any research-cycle blocker below — a research blocker on
    #      the PRIOR session must never be surfaced while the expected session is
    #      still unconfirmed. This precedes P3.5/P3.6 by construction.
    if (not has_confirmed_eligible) or owned_data_lag:
        return WAITING_FOR_OWNED_DATA

    # From here a confirmed eligible completed session exists to work on. The
    # daily-cycle progression gates (P4–P6) run against that eligible session even
    # while today's session is still forming (before its close).

    # P3.5 — a Persistent Daily Research Cycle run is in flight (Slice 3).
    if cycle_running:
        return RESEARCH_CYCLE_RUNNING
    # P3.6 — the Daily Research Cycle is blocked (e.g. a due monthly momentum input
    #        with no wired emitter). More specific than P4, but strictly BELOW P3:
    #        it is only reachable once owned data has confirmed the eligible session.
    if cycle_blocked:
        return RESEARCH_CYCLE_BLOCKED

    # P3.7 — STAGE 22 CLOSE PRECEDENCE. A confirmed eligible completed session whose
    #        operational Daily Close is NOT complete has exactly ONE canonical next
    #        action: the Daily Close. Before Stage 22 a stale research input could
    #        promote the Daily Research Cycle ahead of an unclosed session, so the same
    #        session had two legal orderings (close-then-research in the live flow,
    #        research-then-close whenever owned marks had advanced without a close).
    #        Two legal orderings is exactly the "alternate path" the canonical normal
    #        cycle forbids: the close is what advances owned marks, settles NEXT_CLOSE
    #        paper orders and records NAV, so research produced ahead of it describes a
    #        portfolio that is about to change. A run already IN FLIGHT (P3.5) or BLOCKED
    #        (P3.6) still outranks this — an in-progress cycle is never interrupted and a
    #        blocked one names a fix the operator must make first.
    if not eligible_session_closed:
        return READY_FOR_DAILY_CLOSE

    # P4 — required research inputs are stale or missing → run the research cycle.
    if not research_current:
        return RESEARCH_CYCLE_REQUIRED

    # P4.5 — STAGE 22 POST-CLOSE RESEARCH REQUIREMENT (Workstream D). The Daily Close
    #        composes the owned-mark refresh and the model-input refresh, so a completed
    #        close can leave every REQUIRED signal input current while no Holding
    #        Opportunity-Cost assessment exists for the session it just closed. The
    #        operator was then told "monitor / no action required" for a session that had
    #        never been reassessed. A completed close therefore makes the Daily Research
    #        Cycle DUE until an assessment bound to that same eligible session exists.
    #        The caller passes this only when the HOC contract is actually observable —
    #        an absent contract is UNVERIFIABLE and never inferred as "not run".
    if research_cycle_due_after_close:
        return RESEARCH_CYCLE_REQUIRED

    # P5 — research current but the portfolio assessment is missing/due/overdue/
    #      stale → run a portfolio reassessment. Phase 29G.3 (Workstream G): the portfolio
    #      reassessment is PRODUCED by the Daily Research Cycle's Holding Opportunity-Cost
    #      step. When that cycle has COMPLETED for the eligible session AND a current HOC
    #      assessment exists, the reassessment is SATISFIED even if the legacy daily-action-
    #      gate assessment date lags the still-pending Daily Close — the operator proceeds to
    #      review the Holding Opportunity-Cost assessment and then run the Daily Close. There
    #      is NO separate reassessment control.
    reassessment_satisfied = bool(cycle_complete and hoc_current)
    if assessment_status in _ASSESS_NEEDS_ACTION and not reassessment_satisfied:
        return PORTFOLIO_REASSESSMENT_REQUIRED

    # (The former P6 "close is not complete" gate now runs as P3.7 above, so that an
    #  unclosed eligible session can never be overtaken by a research/reassessment
    #  action. Reaching this point always means the close IS complete.)

    # P6 — STAGE 22.1 BLOCKED PORTFOLIO DECISION. The Daily Close and the Daily Research
    #      Cycle both completed for the eligible session and an opportunity-cost
    #      assessment exists, yet the canonical portfolio reassessment reached NO
    #      portfolio-level economic verdict: it failed closed on a missing/unusable input
    #      (BLOCKED_DATA) or on evidence whose named cause the next cycle will not clear.
    #
    #      Before Stage 22.1 nothing here consulted the reassessment STATE — only
    #      ``proposal_required``, which a blocked reassessment deliberately leaves False.
    #      The blocked session therefore fell straight through to DAILY_CYCLE_COMPLETE /
    #      MONITOR_PORTFOLIO, and the operator was shown "no portfolio change requires
    #      review" while the SAME payload reported SYSTEM_BLOCKER / BLOCKED_DATA. That is
    #      the difference between "the economic gate cleared and nothing should change"
    #      and "no verdict could be reached" — presenting the second as the first is a
    #      fabricated conclusion, so it is now stated as the named blocker it is.
    #
    #      This sits BELOW every input/ordering gate above (an unconfirmed session, an
    #      in-flight or input-blocked cycle, an unclosed session and a due research cycle
    #      all name an earlier fix) and ABOVE the terminal region, so no completion state
    #      can outrank it. It opens NO mutation gate: RESEARCH_CYCLE_BLOCKED maps to the
    #      RECOVERY stage, where the normal cycle offers nothing to execute and the
    #      controlled rebalance stays unavailable.
    if reassessment_blocked:
        return RESEARCH_CYCLE_BLOCKED

    # Terminal region — the eligible session is fully processed. Refinement order
    # (documented precedence 7/8/9): a material-risk / manual-review gate is
    # surfaced ahead of a plain or evidence-gapped completion (P7 > P8 > P9).
    # Release 46.2 repair: ``manual_review_required`` ALSO suppresses P2 above, so the
    # review outranks every "nothing is outstanding" claim the state machine can make,
    # not merely the two it can make after the close.
    if manual_review_required:
        return MANUAL_REVIEW_REQUIRED
    if evidence_gap:
        return DAILY_CYCLE_COMPLETE_EVIDENCE_GAP
    return DAILY_CYCLE_COMPLETE


# --------------------------------------------------------------------------- #
# Primary-action + current-task presentation per overall state (Workstream C.9/10).
# Slice-2 descriptive/routing actions are explicitly NOT executed here; Slice-3
# actions are labelled not-yet-implemented (execution_available=False).
# --------------------------------------------------------------------------- #
def _primary_action(overall: str, ctx: dict) -> dict[str, Any]:
    elig = ctx.get("eligible_date")
    session_reason = ctx.get("session_operator_action") or ""

    if overall == INCONSISTENT_STATE:
        return {"action_code": ACTION_INSPECT_INCONSISTENCY,
                "label": "Inspect the state inconsistency",
                "explanation": "Authoritative surfaces disagree; reconcile the "
                               "named consistency violations before operating.",
                "severity": SEV_ERROR, "destination": DEST_RESEARCH_AUDIT,
                "safe_to_execute": True, "execution_available": True,
                "manual_confirmation_required": False, "slice3_pending": False,
                "current_task": "Inspect and reconcile the state inconsistency.",
                "headline": "State inconsistency — inspect before operating."}

    if overall == WAITING_FOR_SESSION_CLOSE:
        return {"action_code": ACTION_WAIT_FOR_SESSION_CLOSE,
                "label": "Wait for the market session to close",
                "explanation": session_reason or (
                    "The latest eligible session (%s) is fully processed; the "
                    "current session is still open." % (elig or "n/a")),
                "severity": SEV_INFO, "destination": DEST_COMMAND_CENTER,
                "safe_to_execute": True, "execution_available": False,
                "manual_confirmation_required": False, "slice3_pending": False,
                "current_task": "Wait for the current market session to close.",
                "headline": "Waiting for the current session to close."}

    if overall == WAITING_FOR_OWNED_DATA:
        # Stage 19.3 — SINGLE POST-CLOSE ORCHESTRATION PATH. This state used to promote
        # the standalone Paper Desk refresh (POST /v1/paper-desk/refresh) to the canonical
        # primary action, which made the normal post-close workflow a TWO-step operator
        # sequence: refresh the desk, then run the Daily Close. The Daily Close already
        # COMPOSES the same Paper Desk owner (desk.refresh_desk -> owned EOD marks +
        # NEXT_CLOSE settlement + performance) and then advances the model inputs and
        # reassesses the portfolio, so it is a strict superset. The canonical primary
        # action is therefore the Daily Close in EVERY normal post-close state; the raw
        # desk refresh survives only as an explicit maintenance/recovery capability and
        # is never a ``primary_action`` (asserted by ``assert_primary_action_contract``).
        # The close revalidates provider readiness server-side and writes nothing when the
        # owned session is genuinely unpublished — fail-closed, not manufactured.
        return {"action_code": ACTION_WAIT_FOR_OWNED_DATA,
                "label": "Run the Daily Close",
                "explanation": (
                    "The expected completed session is not yet confirmed by owned market "
                    "data. The Daily Close refreshes owned marks through the Paper Desk, "
                    "settles eligible NEXT_CLOSE paper orders and then reassesses the "
                    "portfolio — no separate desk refresh is required. If the owned "
                    "provider has not published the session yet the close writes nothing "
                    "and reports that it is still waiting."
                    + ((" " + session_reason) if session_reason else "")),
                "severity": SEV_ATTENTION, "destination": DEST_DAILY_WORKFLOW,
                "safe_to_execute": False, "execution_available": True,
                "manual_confirmation_required": True, "slice3_pending": False,
                "confirmation_required": EXECUTION_CONTRACTS[
                    EXEC_DAILY_CLOSE]["confirmation_token"],
                "execution_kind": EXEC_DAILY_CLOSE,
                "current_task": "Run the Daily Close for the completed market session.",
                "headline": "Process the latest completed market close."}

    if overall == RESEARCH_CYCLE_REQUIRED:
        # Stage 22 — the SAME canonical action, stated for the reason that actually
        # raised it. The post-close requirement is not "inputs are stale": every input
        # can be current while the session that was just closed has never been
        # reassessed, and telling the operator otherwise sends them looking for a stale
        # input that does not exist.
        if ctx.get("research_cycle_due_after_close"):
            # Release 29.5 — the requirement has TWO shapes now, and naming the wrong one
            # sends the operator hunting for something that is plainly on screen. When
            # continuous collection has already refreshed a live assessment, saying "no
            # assessment has been produced" contradicts the counts rendered beside it; the
            # true reason is that no GOVERNED cycle has run for the session.
            if ctx.get("live_pre_drc_signal_present"):
                explanation = (
                    "The Daily Close for %s is complete. A live opportunity-cost "
                    "assessment already exists for that session, refreshed by continuous "
                    "information collection — it is current signal context, not the "
                    "governed daily-cycle result. The Daily Research Cycle is the sole "
                    "path that produces the governed outcome: it refreshes every required "
                    "signal, reassesses the portfolio against complete evidence and "
                    "records the run manifest. Nothing is approved or executed by running "
                    "it." % (elig or "the eligible session"))
            else:
                explanation = (
                    "The Daily Close for %s is complete, but no Holding Opportunity-Cost "
                    "assessment has been produced for that session yet. The Daily Research "
                    "Cycle is the sole path that refreshes the signals, assesses every "
                    "holding's opportunity cost, reassesses the portfolio and produces a "
                    "reallocation proposal when one is justified. Nothing is approved or "
                    "executed by running it." % (elig or "the eligible session"))
        else:
            explanation = ("Required research inputs are stale or missing for the "
                           "latest eligible session. The canonical Persistent Daily "
                           "Research Cycle (Slice 3) refreshes every required input "
                           "through its authoritative owner, scores the universe, "
                           "prepares the target and captures immutable evidence.")
        # The task line and the headline name the SAME reason the explanation does. A
        # post-close cycle whose inputs are all current was still told to "refresh the
        # stale inputs" — a sentence the operator cannot act on, because there is no
        # stale input to find.
        if ctx.get("research_cycle_due_after_close"):
            task = ("Run the Daily Research Cycle to produce the governed assessment "
                    "for %s." % (elig or "the eligible session"))
            headline = ("Daily Close complete — run the Daily Research Cycle.")
        else:
            task = "Run the Daily Research Cycle to refresh the stale inputs."
            headline = "Research inputs are stale — run the Daily Research Cycle."
        return {"action_code": ACTION_RUN_RESEARCH_CYCLE,
                "label": "Run the Daily Research Cycle",
                "explanation": explanation,
                "severity": SEV_ATTENTION, "destination": DEST_DAILY_WORKFLOW,
                "safe_to_execute": False, "execution_available": True,
                "manual_confirmation_required": True, "slice3_pending": False,
                "confirmation_required": _DRC_EXECUTE_TOKEN,
                "execution_kind": EXEC_DAILY_RESEARCH_CYCLE,
                "current_task": task,
                "headline": headline}

    if overall == RESEARCH_CYCLE_RUNNING:
        return {"action_code": ACTION_MONITOR_RESEARCH_CYCLE,
                "label": "Daily Research Cycle running",
                "explanation": "A Persistent Daily Research Cycle run is in progress "
                               "for the eligible session; watch its progress and do not "
                               "start it again.",
                "severity": SEV_INFO, "destination": DEST_DAILY_WORKFLOW,
                "safe_to_execute": True, "execution_available": False,
                "manual_confirmation_required": False, "slice3_pending": False,
                "current_task": "Monitor the running Daily Research Cycle.",
                "headline": "Daily Research Cycle is running."}

    if overall == RESEARCH_CYCLE_BLOCKED:
        # Stage 22.1 — the SAME blocked-cycle state has two named causes, and the
        # operator is told which one applies rather than a generic "a required input".
        prs_codes = list(ctx.get("reassessment_blocker_codes") or [])
        if ctx.get("reassessment_blocked"):
            named = ", ".join(prs_codes) if prs_codes else "cause not reported"
            return {"action_code": ACTION_RESOLVE_RESEARCH_BLOCKER,
                    "label": "Resolve the blocked portfolio reassessment",
                    "explanation": (
                        "The Daily Close and the Daily Research Cycle completed for %s, "
                        "but the portfolio reassessment reached NO portfolio-level "
                        "verdict: %s. There is no proposal and no 'no change' "
                        "conclusion — neither may be inferred from blocked evidence. "
                        "Repair the named cause, then run the Daily Research Cycle "
                        "again to reassess the portfolio against complete evidence."
                        % (elig or "the eligible session", named)),
                    "severity": SEV_BLOCKED, "destination": DEST_DAILY_WORKFLOW,
                    "safe_to_execute": True, "execution_available": False,
                    "manual_confirmation_required": False, "slice3_pending": False,
                    "current_task": "Resolve the named portfolio-reassessment blocker.",
                    "headline": ("Portfolio reassessment blocked for %s — no portfolio "
                                 "verdict was reached." % (elig or "the eligible session"))}
        return {"action_code": ACTION_RESOLVE_RESEARCH_BLOCKER,
                "label": "Resolve the Daily Research Cycle blocker",
                "explanation": "The Daily Research Cycle is blocked (a required research "
                               "input cannot be refreshed automatically — e.g. the frozen "
                               "monthly momentum input is due and has no safe emitter). "
                               "The exact source and required action are named in the "
                               "cycle status; the monthly input is never approximated.",
                "severity": SEV_BLOCKED, "destination": DEST_DAILY_WORKFLOW,
                "safe_to_execute": True, "execution_available": False,
                "manual_confirmation_required": False, "slice3_pending": False,
                "current_task": "Resolve the named Daily Research Cycle blocker.",
                "headline": "Daily Research Cycle is blocked — resolve the named input."}

    if overall == PORTFOLIO_REASSESSMENT_REQUIRED:
        # Phase 29G.1 hard cutover: Slice 3 (the Daily Research Cycle) and Slice 6 (the
        # Holding Opportunity-Cost engine) are IMPLEMENTED. The portfolio reassessment is
        # produced inside the Daily Research Cycle (its ASSESS_HOLDING_OPPORTUNITY_COST
        # step) — there is NO separate reassessment execution control, and the obsolete
        # Slice-2 placeholder label is gone. This action is descriptive/routing only
        # (execution_available False); the sole execution path is the DRC run action.
        return {"action_code": ACTION_RUN_PORTFOLIO_REASSESSMENT,
                "label": "Reassess the portfolio by running the Daily Research Cycle",
                "explanation": "Research inputs are current, but the portfolio Holding "
                               "Opportunity-Cost assessment is not current for the latest "
                               "eligible session. The reassessment is produced by the "
                               "canonical Daily Research Cycle (its "
                               "ASSESS_HOLDING_OPPORTUNITY_COST step) — the sole execution "
                               "path (POST /v1/operations/daily-research-cycle/run). There "
                               "is no separate reassessment control: run the Daily Research "
                               "Cycle from the Daily Workflow, then review the Holding "
                               "Opportunity-Cost assessment.",
                "severity": SEV_ATTENTION, "destination": DEST_DAILY_WORKFLOW,
                "safe_to_execute": True, "execution_available": False,
                "manual_confirmation_required": False, "slice3_pending": False,
                "current_task": "Run the Daily Research Cycle to reassess the portfolio.",
                "headline": "Portfolio reassessment is due — run the Daily Research Cycle."}

    if overall == READY_FOR_DAILY_CLOSE:
        return {"action_code": ACTION_RUN_DAILY_CLOSE,
                "label": "Run the Daily Close",
                "explanation": "Research and the portfolio assessment are current for "
                               "the latest eligible session; the operational Daily "
                               "Close has not been completed for it.",
                "severity": SEV_ATTENTION, "destination": DEST_DAILY_WORKFLOW,
                "safe_to_execute": False, "execution_available": True,
                "manual_confirmation_required": True, "slice3_pending": False,
                "confirmation_required": EXECUTION_CONTRACTS[
                    EXEC_DAILY_CLOSE]["confirmation_token"],
                "execution_kind": EXEC_DAILY_CLOSE,
                "current_task": "Run the operational Daily Close.",
                "headline": "Ready for the Daily Close."}

    if overall == MANUAL_REVIEW_REQUIRED:
        return {"action_code": ACTION_MANUAL_REVIEW,
                "label": "Manually review the proposed change",
                "explanation": "A material change / review gate requires manual "
                               "review before the target can be confirmed. Nothing is "
                               "executed automatically.",
                "severity": SEV_ATTENTION, "destination": DEST_PORTFOLIO_MANAGER,
                "safe_to_execute": True, "execution_available": False,
                "manual_confirmation_required": True, "slice3_pending": False,
                "current_task": "Manually review the proposed portfolio change.",
                "headline": "Manual review required."}

    if overall == DAILY_CYCLE_COMPLETE_EVIDENCE_GAP:
        return {"action_code": ACTION_REVIEW_EVIDENCE_GAP,
                "label": "Review the documented forward-evidence gap",
                "explanation": "The operational Daily Close for %s is complete and "
                               "valid; a forward-evidence (TRUE_FORWARD) gap is "
                               "documented and remains attention-level, not an "
                               "operational failure." % (elig or "the eligible session"),
                "severity": SEV_ATTENTION, "destination": DEST_RESEARCH_AUDIT,
                "safe_to_execute": True, "execution_available": False,
                "manual_confirmation_required": False, "slice3_pending": False,
                "current_task": "Review the documented forward-evidence gap.",
                "headline": "Daily cycle complete — forward-evidence gap documented."}

    # DAILY_CYCLE_COMPLETE
    return {"action_code": ACTION_MONITOR,
            "label": "Monitor the portfolio",
            "explanation": "The daily cycle is complete for %s with forward evidence "
                           "captured. No action is required for the latest eligible "
                           "session." % (elig or "the eligible session"),
            "severity": SEV_SUCCESS, "destination": DEST_PORTFOLIO,
            "safe_to_execute": True, "execution_available": False,
            "manual_confirmation_required": False, "slice3_pending": False,
            "current_task": "Monitor the portfolio.",
            "headline": "Daily cycle complete for %s." % (elig or "the eligible session")}


# --------------------------------------------------------------------------- #
# Queued follow-up actions (Workstream C.12). ALL applicable follow-ups are
# reported (none hidden); the promoted primary is excluded from the queue.
# --------------------------------------------------------------------------- #
def _queued_actions(*, primary_code: str, research_current: bool,
                    assessment_status: str, eligible_session_closed: bool,
                    has_confirmed_eligible: bool, evidence_gap: bool,
                    manual_review_required: bool, pending_orders: int,
                    cycle_active: bool = False, hoc_available: bool = False,
                    reallocation_available: bool = False,
                    reassessment_satisfied: bool = False,
                    reassessment_proposal_required: bool = False,
                    reassessment_execution_active: bool = False) -> list[dict]:
    q: list[dict[str, Any]] = []

    def add(action_code, label, severity, destination, reason, *,
            execution_available=False, safe_to_execute=True, slice3_pending=False,
            focus=None):
        if action_code == primary_code:
            return
        # ``focus`` is an OPTIONAL in-tab subsection token (never a new route): the
        # destination stays one of the six canonical sidebar routes, and ``focus``
        # deep-links to the exact review surface inside it (e.g. the Reallocation
        # Proposal card), so a review action lands directly on its subject rather than
        # the top of a broad tab. Navigation only — never an execute/confirm/order path.
        q.append({"action_code": action_code, "label": label, "severity": severity,
                  "destination": destination, "reason": reason,
                  "execution_available": execution_available,
                  "safe_to_execute": safe_to_execute, "slice3_pending": slice3_pending,
                  "focus": focus})

    if not research_current and not cycle_active:
        add(ACTION_RUN_RESEARCH_CYCLE, "Run the Daily Research Cycle",
            SEV_ATTENTION, DEST_DAILY_WORKFLOW,
            "Required research inputs are stale or missing.",
            execution_available=True, safe_to_execute=False)
    if assessment_status in _ASSESS_NEEDS_ACTION and not reassessment_satisfied:
        # Phase 29G.1: the reassessment is produced by the Daily Research Cycle (the sole
        # execution path), not a separate obsolete placeholder control. Phase 29G.3: once
        # the cycle has completed for the eligible session and a current Holding
        # Opportunity-Cost assessment exists, the reassessment is SATISFIED and no
        # reassessment follow-up is queued (the operator reviews the HOC assessment instead).
        add(ACTION_RUN_PORTFOLIO_REASSESSMENT,
            "Reassess the portfolio by running the Daily Research Cycle",
            SEV_ATTENTION, DEST_DAILY_WORKFLOW,
            "The portfolio Holding Opportunity-Cost assessment is %s for the latest "
            "eligible session; it is produced by the Daily Research Cycle (the sole "
            "execution path), not a separate reassessment control."
            % assessment_status.lower(), slice3_pending=False)
    if hoc_available:
        # After a completed Holding Opportunity-Cost assessment exists, the operator
        # REVIEWS it (read-only) before the Daily Close. Review/routing only — never a
        # separate reassessment / proposal / rebalance / order execution control.
        add(ACTION_REVIEW_HOC, "Review the Holding Opportunity-Cost assessment",
            SEV_INFO, DEST_PORTFOLIO_MANAGER,
            "A Holding Opportunity-Cost assessment is available for the eligible session; "
            "review it (read-only) before the Daily Close. No orders, no confirmation.",
            focus="opportunity-cost")
    if reassessment_proposal_required and not reassessment_execution_active:
        # Stage 20: the reassessment cleared the portfolio-level economic gate, so a
        # reviewable proposal exists. Review/routing ONLY — never an apply / approve /
        # rebalance / order control (those remain the Stage-18 and Stage-19 gates). While
        # a Stage-19 execution is in flight this action is suppressed entirely so the
        # pending execution keeps the operator's undivided attention.
        add(ACTION_REVIEW_PORTFOLIO_PROPOSAL, "Review the proposed portfolio change",
            SEV_ATTENTION, DEST_PORTFOLIO_MANAGER,
            "The portfolio reassessment found a change that is economically justified "
            "after switching cost, turnover, concentration and churn controls. Review it "
            "(read-only, manual review) — nothing is approved or executed automatically.",
            focus="reassessment")
    if reallocation_available:
        # Slice 7 (Phase 29H): after a reallocation proposal exists the operator REVIEWS
        # it (read-only, manual review). Review/routing only — NEVER an apply / rebalance
        # / confirm-target / order-execution control. It never gates the Daily Close.
        add(ACTION_REVIEW_REALLOCATION, "Review the reallocation proposal",
            SEV_INFO, DEST_PORTFOLIO_MANAGER,
            "A reallocation proposal is available for the eligible session; review it "
            "(read-only, manual review) — no orders, no confirmation, no target change. "
            "The Daily Close remains a separate action.",
            focus="reallocation")
    if has_confirmed_eligible and not eligible_session_closed:
        add(ACTION_RUN_DAILY_CLOSE, "Run the Daily Close", SEV_ATTENTION,
            DEST_DAILY_WORKFLOW,
            "The operational Daily Close is not complete for the eligible session.",
            execution_available=True, safe_to_execute=False)
    if manual_review_required:
        add(ACTION_MANUAL_REVIEW, "Manually review the proposed change",
            SEV_ATTENTION, DEST_PORTFOLIO_MANAGER,
            "A material change / review gate requires manual review.")
    if evidence_gap:
        add(ACTION_REVIEW_EVIDENCE_GAP, "Review the documented forward-evidence gap",
            SEV_ATTENTION, DEST_RESEARCH_AUDIT,
            "A forward-evidence gap is documented for a completed close.")
    if pending_orders and pending_orders > 0:
        add(ACTION_REVIEW_PENDING_ORDERS, "Review pending paper orders",
            SEV_ATTENTION, DEST_PORTFOLIO,
            "%d pending paper order(s) await manual review." % int(pending_orders))
    return q


# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
# Release 29.3 — the ONE canonical portfolio-decision object.
#
# Composed (never recomputed) from the three canonical owners so a downstream consumer
# (Release 30 / Telegram) reads exactly ONE state and one human sentence. It carries no
# economics of its own: every number is copied verbatim from the owner that produced it.
# --------------------------------------------------------------------------- #
CPD_NO_CHANGE = "NO_CHANGE"
CPD_CHANGE_WITHHELD = "CHANGE_CANDIDATE_WITHHELD"
CPD_REVIEW_REQUIRED = "PROPOSAL_REVIEW_REQUIRED"
CPD_DECISION_RECORDED = "DECISION_RECORDED"
CPD_BLOCKED = "BLOCKED"
CPD_NOT_RUN = "NOT_RUN"
#: Release 47 — a complete FEASIBLE alternative target exists, was priced against the
#: current book and did not clear the switching hurdle. Distinct from
#: CPD_CHANGE_WITHHELD (a portfolio limit the target could not satisfy at all) and
#: from CPD_BLOCKED (no trustworthy decision exists): here a decision WAS taken, and
#: the decision is to keep the current book.
CPD_HOLD_CURRENT_BOOK = "HOLD_CURRENT_BOOK"
CANONICAL_PORTFOLIO_DECISION_STATES = (
    CPD_NOT_RUN, CPD_NO_CHANGE, CPD_CHANGE_WITHHELD, CPD_HOLD_CURRENT_BOOK,
    CPD_REVIEW_REQUIRED, CPD_DECISION_RECORDED, CPD_BLOCKED)
#: The ONLY canonical states in which an operator action on a proposal exists.
CANONICAL_ACTIONABLE_DECISION_STATES = (CPD_REVIEW_REQUIRED,)

#: Release 29.5 — WHERE a composed decision came from. Not a decision state: the same
#: verdict can be reached from governed daily-cycle evidence or from live signal state
#: that continuous collection refreshed between cycles, and the operator is entitled to
#: know which. It never changes the verdict, the economics or what is approvable.
DECISION_PROVENANCE_GOVERNED = "GOVERNED_DAILY_CYCLE"
DECISION_PROVENANCE_LIVE_PRE_DRC = "LIVE_PRE_DRC_SIGNAL"
DECISION_PROVENANCE_VOCABULARY = (DECISION_PROVENANCE_GOVERNED,
                                  DECISION_PROVENANCE_LIVE_PRE_DRC)


def build_canonical_portfolio_decision(*, reassessment_summary: dict,
                                       reallocation_operator_state: Any,
                                       portfolio_decision_lane: dict,
                                       attention_count: Any,
                                       eligible_date: Any,
                                       governed_research_evidence_current: bool = True) -> dict:
    """Compose the ONE unambiguous portfolio-decision object.

    Pure composition over authoritative owners. Precedence is deterministic:
    a recorded decision > an approvable proposal awaiting review > a withheld complete
    target > a reassessment blocker > no change > not run.

    Release 29.5 — ``governed_research_evidence_current`` says whether the governed Daily
    Research Cycle has actually run for this session. When it has not, the composed
    decision is still a truthful reading of the CURRENT reassessment, but it is LIVE
    signal state rather than the governed daily-cycle verdict, and it is labelled as such.
    The decision's own precedence, states and economics are untouched: this adds a
    provenance label, it does not re-decide anything.
    """
    prs = reassessment_summary or {}
    lane = portfolio_decision_lane or {}
    prs_state = prs.get("reassessment_state") or PRS_NOT_RUN
    pd_state = lane.get("portfolio_decision_state")
    mex = list(prs.get("mandatory_exit_tickers") or [])
    mex_policy = prs.get("mandatory_exit_policy") or {}
    blockers = list(prs.get("blockers") or [])
    withheld_reasons = list(lane.get("withheld_reasons") or [])
    # Release 47 — read (never re-derive) the proposal owner's authoritative outcome.
    reallocation_outcome = lane.get("reallocation_outcome")
    hold_current_book = bool(lane.get("hold_current_book")
                             or reallocation_outcome == RO_HOLD_CURRENT_BOOK)
    constraints_reshaped = list(lane.get("constraints_that_reshaped") or [])

    if pd_state in ("PROPOSAL_APPROVED", "PROPOSAL_REJECTED"):
        state = CPD_DECISION_RECORDED
    elif lane.get("requires_manual_review") and reallocation_operator_state in             REALLOCATION_APPROVABLE_STATES:
        state = CPD_REVIEW_REQUIRED
    # Release 47: an ECONOMIC hold outranks the withhold branch. A feasible target
    # that was computed, priced and rejected on its merits must never be presented as
    # "withheld by a constraint" - that is the exact confusion this release removes.
    elif pd_state == PDS_HOLD_CURRENT_BOOK or hold_current_book:
        state = CPD_HOLD_CURRENT_BOOK
    elif pd_state == PDS_CHANGE_WITHHELD or reallocation_operator_state == RPS_WITHHELD:
        state = CPD_CHANGE_WITHHELD
    elif prs_state in ("BLOCKED_DATA", "BLOCKED_EVIDENCE", "MANUAL_REVIEW_REQUIRED"):
        state = CPD_BLOCKED
    elif prs_state == PRS_CHANGE_CANDIDATE:
        # Deterioration was found, but the reassessment did not ask for a target. That is
        # a WITHHELD change, never "no change recommended".
        state = CPD_CHANGE_WITHHELD
    elif prs_state == "CURRENT_NO_CHANGE":
        state = CPD_NO_CHANGE
    elif prs_state in (PRS_NOT_RUN, "NOT_READY", "UNAVAILABLE"):
        state = CPD_NOT_RUN
    else:
        state = CPD_NOT_RUN

    reasons = withheld_reasons or [b for b in blockers]
    # A reassessment that CLEARED the gate but whose proposal the owner has not produced
    # is not "no decision yet" — it is a requested proposal that is still missing. Say so
    # precisely so a downstream consumer never reads it as "nothing to do".
    proposal_requested_not_produced = bool(
        state == CPD_NOT_RUN and prs_state == PRS_PROPOSAL_READY
        and reallocation_operator_state not in REALLOCATION_APPROVABLE_STATES)
    headline = {
        CPD_NO_CHANGE: "NO PORTFOLIO CHANGE REQUIRED",
        CPD_CHANGE_WITHHELD: "PORTFOLIO CHANGE WITHHELD",
        CPD_HOLD_CURRENT_BOOK: "HOLD THE CURRENT BOOK",
        CPD_REVIEW_REQUIRED: "PORTFOLIO PROPOSAL — MANUAL REVIEW REQUIRED",
        CPD_DECISION_RECORDED: "PORTFOLIO DECISION RECORDED",
        CPD_BLOCKED: "PORTFOLIO DECISION BLOCKED",
        CPD_NOT_RUN: "NO PORTFOLIO DECISION YET",
    }[state]
    if proposal_requested_not_produced:
        headline = "PORTFOLIO PROPOSAL REQUESTED — NOT YET PRODUCED"

    # Release 46.6 - when there is no proposal and the AUTHORITATIVE reason is
    # known, say the reason. The Proposed Portfolio surface printed "NO
    # PROPOSAL YET" over a portfolio whose reassessment was BLOCKED on named
    # constraint breaches, which reads as "nothing has happened" when what
    # actually happened is that something needs a person.
    no_proposal_headline, no_proposal_reason = None, None
    if state == CPD_BLOCKED:
        no_proposal_headline = "NO PROPOSAL — PORTFOLIO CONSTRAINT REVIEW REQUIRED"
        no_proposal_reason = (
            "the portfolio reassessment is %s and named %d blocker(s): %s. A "
            "proposal is not produced while a blocker stands; this is a "
            "review, not an outage."
            % (prs_state, len(blockers), ", ".join(blockers[:8]) or "none listed"))
    elif state == CPD_HOLD_CURRENT_BOOK:
        no_proposal_headline = "NO PROPOSAL — HOLD THE CURRENT BOOK"
        no_proposal_reason = (
            "a complete FEASIBLE alternative portfolio was computed%s and priced "
            "against the current book; its expected improvement does not justify "
            "what switching would cost (%s). Holding is the decision, not a "
            "constraint blocking one."
            % (" after %s reshaped it" % ", ".join(constraints_reshaped[:4])
               if constraints_reshaped else "",
               ", ".join(lane.get("reallocation_outcome_reason_codes") or [])
               or "below the switching hurdle"))
    elif state == CPD_CHANGE_WITHHELD:
        no_proposal_headline = "NO PROPOSAL — PORTFOLIO CHANGE WITHHELD"
        no_proposal_reason = (
            "deterioration was found and no change cleared the economic "
            "hurdle: %s" % (", ".join(reasons[:8]) or "no reason recorded"))
    elif proposal_requested_not_produced:
        no_proposal_headline = "NO PROPOSAL — REQUESTED BUT NOT YET PRODUCED"
        no_proposal_reason = (
            "the reassessment cleared its gate and asked for a proposal; the "
            "proposal owner has not produced one for this session yet")
    elif state == CPD_NO_CHANGE:
        no_proposal_headline = "NO PROPOSAL — NO PORTFOLIO CHANGE REQUIRED"
        no_proposal_reason = (
            "the reassessment ran and found the current holdings remain the "
            "best available use of capital")

    return {
        "state": state,
        "state_vocabulary": list(CANONICAL_PORTFOLIO_DECISION_STATES),
        "headline": headline,
        "no_proposal_headline": no_proposal_headline,
        "no_proposal_reason": no_proposal_reason,
        "no_proposal_reason_is_authoritative": bool(no_proposal_reason),
        "eligible_market_date": eligible_date,
        # The human sentence is produced by the reassessment owner; never re-worded here.
        "explanation": prs.get("explanation"),
        "holding_attention_count": attention_count,
        "holding_attention_scope": "INDIVIDUAL_HOLDING_REVIEW_SIGNAL",
        "portfolio_scope": "WHOLE_PORTFOLIO_ECONOMIC_VERDICT",
        "scopes_are_different_questions": True,
        # Release 46.6 - the two counts an operator sees on different cards,
        # under names that say which is which. One card read "15 holdings need
        # attention" while another read "holdings needing attention = 0"; both
        # were right and the labels made them look like a contradiction.
        "signal_level_holdings_under_review": attention_count,
        # ``decision`` is a dict on the full reassessment artifact and a plain
        # state STRING on the summary the workflow composer passes here, so
        # the nested read is guarded rather than assumed.
        "actionable_holdings_after_portfolio_gate": (
            prs.get("actionable_holding_count")
            if prs.get("actionable_holding_count") is not None
            else (prs.get("decision") or {}).get("actionable_holding_count")
            if isinstance(prs.get("decision"), dict) else None),
        "attention_counts_are_different_questions": (
            "SIGNAL-LEVEL HOLDINGS UNDER REVIEW counts individual holdings "
            "whose own economics flagged them. ACTIONABLE HOLDINGS AFTER THE "
            "PORTFOLIO GATE counts what survived churn control, concentration "
            "and the turnover budget. A non-zero first number with a zero "
            "second one is the NORMAL state, not a contradiction."),
        "reassessment_state": prs_state,
        "reassessment_owner": PRS_CANONICAL_OWNER,
        "reassessment_hash": prs.get("reassessment_hash"),
        "proposal_state": reallocation_operator_state,
        "proposal_owner": RP_CANONICAL_OWNER,
        "proposal_hash": lane.get("proposal_hash"),
        "decision_state": pd_state,
        "decision_owner": "api.portfolio_decision",
        "withheld_reasons": reasons,
        # --- Release 47 — rendered verbatim from the proposal owner --------- #
        "reallocation_outcome": reallocation_outcome,
        "reallocation_outcome_vocabulary": list(REALLOCATION_OUTCOME_VOCABULARY),
        "hold_current_book": hold_current_book,
        "feasible_target_exists": bool(lane.get("feasible_target_exists")),
        "constraints_that_reshaped": constraints_reshaped,
        "constraint_reoptimized": bool(lane.get("constraint_reoptimized")),
        "switching_hurdle": lane.get("switching_hurdle"),
        "clears_switching_hurdle": lane.get("clears_switching_hurdle"),
        "constraint_philosophy": (
            "A normal portfolio constraint reshapes the solution; it does not "
            "freeze the portfolio. Only a declared true blocker stops a decision."),
        "expected_net_improvement": prs.get("expected_net_improvement"),
        "net_improvement_hurdle": prs.get("net_improvement_hurdle"),
        "expected_one_way_turnover": prs.get("expected_one_way_turnover"),
        "turnover_budget": prs.get("turnover_budget"),
        "expected_transaction_cost_usd": prs.get("expected_transaction_cost_usd"),
        "mandatory_exit_tickers": mex,
        "mandatory_exit_obligation": mex_policy.get("obligation") or "NONE",
        "mandatory_exit_statement": mex_policy.get("statement"),
        "proposal_requested_not_produced": proposal_requested_not_produced,
        # --- Release 29.5 — is this the GOVERNED daily-cycle verdict, or live signal? --- #
        "decision_provenance": (DECISION_PROVENANCE_GOVERNED
                                if governed_research_evidence_current
                                else DECISION_PROVENANCE_LIVE_PRE_DRC),
        "is_governed_daily_cycle_decision": bool(governed_research_evidence_current),
        "provenance_label": (None if governed_research_evidence_current
                             else "CURRENT LIVE ASSESSMENT — GOVERNED DAILY CYCLE PENDING"),
        "provenance_detail": (
            None if governed_research_evidence_current else
            "Continuous collection has refreshed this assessment since the last governed "
            "Daily Research Cycle. It is current signal information, not the governed "
            "daily-cycle decision; run the Daily Research Cycle to produce that."),
        "operator_action_available": bool(state in CANONICAL_ACTIONABLE_DECISION_STATES),
        "approvable": bool(lane.get("approvable")),
        "creates_orders": False,
        "automation_off": True,
        "manual_review_only": True,
    }


# --------------------------------------------------------------------------- #
# Release 29.3 — SEMANTIC decision-integrity invariants.
#
# The pre-29.3 validator compared only DATES, so the live 2026-08-17 payload reported
# CONSISTENT while simultaneously claiming PROPOSAL_READY (legacy gate, Daily Close and
# DRC summaries) and REALLOCATION_PROPOSAL_NOT_RUN / PORTFOLIO_DECISION_NO_PROPOSAL
# (canonical owners). These invariants COMPARE AUTHORITATIVE OWNERS; they never
# recompute an owner's economics, so no decision calculation is duplicated.
# --------------------------------------------------------------------------- #
#: Tokens that assert, in operator vocabulary, that a reviewable portfolio proposal
#: exists. Only the canonical proposal owner is entitled to make that claim.
PROPOSAL_CLAIM_TOKENS = frozenset({
    "PROPOSAL_READY", "REBALANCE_PROPOSAL_READY", "PORTFOLIO_CHANGES_PROPOSED",
    "PROPOSAL", "APPROVAL_REQUIRED", "PROPOSED"})
#: Operator phrases that assert the same thing in prose.
PROPOSAL_CLAIM_PHRASES = ("PORTFOLIO CHANGES PROPOSED", "PROPOSAL READY",
                          "REBALANCE PROPOSAL READY", "CHANGES PROPOSED")

SEMANTIC_VIOLATION_CODES = (
    "PROPOSAL_CLAIMED_WITHOUT_PROPOSAL_OWNER",
    "PROPOSAL_REQUIRED_CONTRADICTS_PORTFOLIO_DECISION",
    "APPROVABLE_WITHOUT_CANONICAL_PROPOSAL",
    "PROPOSAL_NOT_BOUND_TO_CURRENT_REASSESSMENT",
    "WITHHELD_PROPOSAL_EXPOSED_AS_APPROVABLE",
    "MANDATORY_EXIT_PRESENTED_AS_EXECUTABLE_OBLIGATION",
)

#: Release 29.4 — SESSION-AUTHORITY violation codes. These compare the market-session
#: owner and the Daily Close owner against what the composed payload OFFERS. They
#: recompute neither owner's answer; they only detect a composition that contradicts one.
SESSION_AUTHORITY_VIOLATION_CODES = (
    "DAILY_CLOSE_OFFERED_FOR_ALREADY_PROCESSED_SESSION",
    "COMPLETED_CLOSE_REPORTED_INVALID",
    "COMPLETED_CLOSE_HIDDEN_FROM_EVIDENCE",
)


def check_session_authority(*, session_status: Any, eligible_market_date: Any,
                            expected_completed_market_date: Any,
                            latest_completed_close_date: Any,
                            operational_close_valid: bool,
                            latest_close_status: Any,
                            daily_close_execution_allowed: bool,
                            evidence_completed_close_state: Any) -> list[dict]:
    """Release 29.4 — the workflow may never contradict the two session owners.

    ``engine.market_session`` owns WHICH completed session is eligible; ``api.daily_close``
    owns whether that session was operationally processed. This is pure: it asks the two
    owners' published answers whether the composed payload is telling the operator
    something neither of them said.
    """
    violations: list[dict] = []
    elig = _coerce_date(eligible_market_date)
    expected = _coerce_date(expected_completed_market_date)
    closed = _coerce_date(latest_completed_close_date)
    already_processed = bool(operational_close_valid and elig and closed and closed >= elig)
    # Is there a NEWER session for a close to work toward? Once the post-close cutoff
    # passes, the market-session owner advances the EXPECTED date even while the owned
    # provider has not published yet — and in that state the Daily Close is precisely the
    # mechanism that advances owned marks (Stage 19.3), so offering it is correct. Before
    # the cutoff nothing newer is expected, and offering it is the Release 29.4 defect.
    new_session_expected = bool(expected and closed and expected > closed)

    # S1. THE RELEASE 29.4 DEFECT. A Daily Close may never be offered for a session the
    #     close owner already processed when no newer session is expected to exist yet.
    if already_processed and not new_session_expected and daily_close_execution_allowed:
        violations.append({
            "code": "DAILY_CLOSE_OFFERED_FOR_ALREADY_PROCESSED_SESSION",
            "concept": "session_authority",
            "value_session_status": session_status,
            "value_eligible_market_date": _iso(elig),
            "value_expected_completed_market_date": _iso(expected),
            "value_latest_completed_close_date": _iso(closed),
            "authoritative_owners": [SESSION_ELIGIBILITY_OWNER, CLOSE_VALIDITY_OWNER],
            "surface": "workflow_state"})

    # S2. A recorded COMPLETED close may never be reported as an invalid one. Only the
    #     close owner's vocabulary decides this — never a portfolio finding.
    if closed is not None and not operational_close_valid and _completed_close_token(
            latest_close_status):
        violations.append({
            "code": "COMPLETED_CLOSE_REPORTED_INVALID",
            "concept": "close_validity",
            "value_latest_close_status": latest_close_status,
            "value_latest_completed_close_date": _iso(closed),
            "authoritative_owners": [CLOSE_VALIDITY_OWNER],
            "surface": "workflow_state"})

    # S3. A valid completed close may never be presented as "no completed close exists".
    if operational_close_valid and closed is not None and \
            str(evidence_completed_close_state) == "NONE":
        violations.append({
            "code": "COMPLETED_CLOSE_HIDDEN_FROM_EVIDENCE",
            "concept": "evidence_presentation",
            "value_latest_completed_close_date": _iso(closed),
            "authoritative_owners": [CLOSE_VALIDITY_OWNER, "api.workflow_state"],
            "surface": "workflow_state"})
    return violations


def _completed_close_token(status) -> bool:
    """True when ``status`` is one the Daily Close owner classifies as COMPLETED."""
    if status is None:
        return False
    return _normalize_close_status(status) in _close_complete_statuses()


def _normalize_close_status(status):
    """Delegate to the Daily Close owner's Release-29.3 vocabulary normalisation.

    Lazy import keeps ``api.workflow_state`` acyclic. Degrade-safe: if the owner cannot
    be imported the raw value is returned unchanged and the semantic invariants below
    still catch any proposal claim it carries.
    """
    if status is None:
        return None
    try:
        from paper_trader.api import daily_close as _dc
        return _dc.normalize_close_status(status)
    except Exception:  # noqa: BLE001 - a pure read must never crash the composition
        return status


def _close_complete_statuses() -> frozenset:
    """The Daily Close owner's completed-close vocabulary (never a local literal)."""
    try:
        from paper_trader.api import daily_close as _dc
        return _dc.completed_close_statuses()
    except Exception:  # noqa: BLE001 - degrade to the audited fallback, never crash
        return _CLOSE_COMPLETE_FALLBACK


def _is_operational_close_complete(progress) -> bool:
    """Release 29.4 — ask the Daily Close owner whether its close COMPLETED.

    The workflow owner composes; it does not re-decide another lane's verdict. This is a
    question about OPERATIONAL COMPLETION only: whether the close ran to the end and
    recorded its work. What the portfolio lane later concluded about membership drift, a
    reallocation proposal or a Holding Opportunity-Cost verdict is a separate lane and
    can never reopen a close that already happened.
    """
    try:
        from paper_trader.api import daily_close as _dc
        return bool(_dc.is_operational_close_complete(progress))
    except Exception:  # noqa: BLE001 - degrade to the audited fallback, never crash
        p = progress if isinstance(progress, dict) else {}
        return bool(p.get("done") and _normalize_close_status(
            p.get("final_close_status")) in _CLOSE_COMPLETE_FALLBACK)


def _claims_proposal(value) -> bool:
    """True when a surfaced value asserts a reviewable proposal exists."""
    if not isinstance(value, str):
        return False
    token = value.strip().upper()
    if token in PROPOSAL_CLAIM_TOKENS:
        return True
    return any(phrase in token for phrase in PROPOSAL_CLAIM_PHRASES)


def check_decision_semantics(*, reallocation_operator_state: Any,
                             reallocation_approvable: bool,
                             reassessment_state: Any,
                             reassessment_proposal_required: bool,
                             portfolio_decision_state: Any,
                             portfolio_decision_requires_review: bool,
                             portfolio_decision_approvable: bool,
                             proposal_bound_reassessment_hash: Any,
                             current_reassessment_hash: Any,
                             mandatory_exit_tickers: Any,
                             mandatory_exit_obligation: Any,
                             summary_claims: Optional[dict] = None,
                             reallocation_outcome: Any = None,
                             feasible_target_exists: Any = None,
                             constraints_that_reshaped: Any = None) -> list[dict]:
    """Compare the authoritative owners and return every SEMANTIC contradiction.

    Pure; no io, no recomputation of any owner's economics. ``summary_claims`` maps a
    surfaced field path -> its value, so a summary that republishes a proposal claim is
    caught at the exact field that produced it.
    """
    violations: list[dict] = []
    proposal_exists = reallocation_operator_state in REALLOCATION_APPROVABLE_STATES

    # I1. No surface may claim a proposal the canonical proposal owner has not produced.
    if not proposal_exists:
        for path, value in sorted((summary_claims or {}).items()):
            if _claims_proposal(value):
                violations.append({
                    "code": "PROPOSAL_CLAIMED_WITHOUT_PROPOSAL_OWNER",
                    "concept": "portfolio_proposal", "field": path, "value": value,
                    "authoritative_owners": [RP_CANONICAL_OWNER, "api.workflow_state"],
                    "canonical_proposal_state": reallocation_operator_state,
                    "surface": "workflow_state"})

    # I2. A reassessment that did not request a proposal cannot yield a decision lane
    #     that requires proposal review.
    if (reassessment_state == PRS_CHANGE_CANDIDATE
            and not reassessment_proposal_required
            and portfolio_decision_requires_review):
        violations.append({
            "code": "PROPOSAL_REQUIRED_CONTRADICTS_PORTFOLIO_DECISION",
            "concept": "portfolio_decision",
            "value_reassessment": reassessment_state,
            "value_portfolio_decision": portfolio_decision_state,
            "authoritative_owners": ["api.portfolio_reassessment",
                                     "api.portfolio_decision"],
            "surface": "workflow_state"})

    # I3. A NO_PROPOSAL decision may never expose an approvable proposal.
    if portfolio_decision_state == PDS_NO_PROPOSAL and (
            portfolio_decision_approvable or reallocation_approvable):
        violations.append({
            "code": "APPROVABLE_WITHOUT_CANONICAL_PROPOSAL",
            "concept": "portfolio_decision",
            "value_portfolio_decision": portfolio_decision_state,
            "authoritative_owners": ["api.portfolio_decision", RP_CANONICAL_OWNER],
            "surface": "workflow_state"})

    # I4. A current proposal must bind to the current eligible-session reassessment.
    if proposal_exists and current_reassessment_hash and proposal_bound_reassessment_hash             and proposal_bound_reassessment_hash != current_reassessment_hash:
        violations.append({
            "code": "PROPOSAL_NOT_BOUND_TO_CURRENT_REASSESSMENT",
            "concept": "proposal_binding",
            "value_proposal_bound_hoc_assessment": proposal_bound_reassessment_hash,
            "value_reassessment_bound_hoc_assessment": current_reassessment_hash,
            "authoritative_owners": [RP_CANONICAL_OWNER, "api.portfolio_reassessment"],
            "surface": "workflow_state"})

    # I5. A WITHHELD complete target is never approvable.
    if reallocation_operator_state == RPS_WITHHELD and (
            reallocation_approvable or portfolio_decision_approvable):
        violations.append({
            "code": "WITHHELD_PROPOSAL_EXPOSED_AS_APPROVABLE",
            "concept": "portfolio_proposal",
            "value_proposal_state": reallocation_operator_state,
            "authoritative_owners": [RP_CANONICAL_OWNER, "api.portfolio_decision"],
            "surface": "workflow_state"})

    # I7 (Release 47). A reshaping constraint may never be presented as a blocker.
    #     This is the release's central invariant expressed as a contradiction: if a
    #     feasible target exists, the operator surface may not say the portfolio is
    #     blocked, and a named cap may not be the reason it claims.
    if (feasible_target_exists and reallocation_outcome == RO_TRUE_BLOCKER):
        violations.append({
            "code": "BLOCKED_WHILE_FEASIBLE_TARGET_EXISTS",
            "concept": "portfolio_proposal",
            "value_outcome": reallocation_outcome,
            "value_constraints_that_reshaped": list(constraints_that_reshaped or []),
            "authoritative_owners": [RP_CANONICAL_OWNER,
                                     "engine.constrained_reallocation"],
            "surface": "workflow_state"})

    # I8 (Release 47). HOLD_CURRENT_BOOK is a taken decision, never an approvable one.
    if reallocation_outcome == RO_HOLD_CURRENT_BOOK and (
            reallocation_approvable or portfolio_decision_approvable):
        violations.append({
            "code": "HOLD_CURRENT_BOOK_EXPOSED_AS_APPROVABLE",
            "concept": "portfolio_decision",
            "value_outcome": reallocation_outcome,
            "authoritative_owners": [RP_CANONICAL_OWNER, "api.portfolio_decision"],
            "surface": "workflow_state"})

    # I6. An eligibility exit is REQUIRED_IF_REALLOCATION_PROCEEDS, never an executable
    #     obligation while no approvable proposal exists.
    if (mandatory_exit_tickers and not proposal_exists
            and mandatory_exit_obligation not in (None, "NONE",
                                                  "REQUIRED_IF_REALLOCATION_PROCEEDS")):
        violations.append({
            "code": "MANDATORY_EXIT_PRESENTED_AS_EXECUTABLE_OBLIGATION",
            "concept": "mandatory_eligibility_exit",
            "value_obligation": mandatory_exit_obligation,
            "tickers": list(mandatory_exit_tickers),
            "authoritative_owners": ["engine.portfolio_reassessment",
                                     RP_CANONICAL_OWNER],
            "surface": "workflow_state"})
    return violations


# Cross-surface consistency validator (Workstream K). Read-only; no provider call.
# Inherits data_freshness's violations and adds workflow-level owner cross-checks;
# staleness/overdue are NORMAL states and are NOT consistency violations.
# --------------------------------------------------------------------------- #
def _build_consistency(*, freshness: dict, gate: Optional[dict],
                       close_progress: Optional[dict], operational: Optional[dict],
                       forward_status: Optional[dict], target_readiness: Optional[dict],
                       assessment_status: str, latest_assessment_date: Any,
                       eligible_date: Any, latest_close_date: Any,
                       overall: str, primary_code: str,
                       pending_orders: int) -> tuple[str, list]:
    violations: list[dict[str, Any]] = []
    saw_unknown = False

    # Inherit the Slice-1 freshness consistency verdict (active book, operational
    # dates, target date vs the active book) verbatim so no conflict is hidden.
    fresh_status = freshness.get("consistency_status")
    for v in (freshness.get("consistency_violations") or []):
        violations.append({**v, "surface": "data_freshness"})
    if fresh_status == UNKNOWN:
        saw_unknown = True

    def _cmp(concept, a_val, b_val, owner_a, owner_b, code):
        nonlocal saw_unknown
        av, bv = df._coerce_iso(a_val), df._coerce_iso(b_val)
        if av is None or bv is None:
            saw_unknown = True
            return
        if av != bv:
            violations.append({"code": code, "concept": concept,
                               "value_%s" % owner_a: av, "value_%s" % owner_b: bv,
                               "authoritative_owners": [owner_a, owner_b],
                               "surface": "workflow_state"})

    # Latest completed close matches the probe-free Daily Close progress.
    _cmp("latest_completed_close", latest_close_date,
         (close_progress or {}).get("market_date"),
         "workflow_state", "daily_close", "CLOSE_DATE_MISMATCH")
    # Latest assessment date matches the Daily Action Gate.
    _cmp("latest_assessment", latest_assessment_date,
         (gate or {}).get("latest_completed_market_date"),
         "workflow_state", "daily_action_gate", "ASSESSMENT_DATE_MISMATCH")
    # Target date matches alpha-target readiness (when the readiness model loaded).
    if target_readiness is not None:
        tr_dates = (target_readiness.get("dates") or {})
        _cmp("target_calculation",
             _row(freshness, "target_calculation"),
             tr_dates.get("alpha_market_date"),
             "data_freshness", "alpha_target", "TARGET_READINESS_MISMATCH")
    # Latest evidence date matches the Forward Prediction Skill.
    _cmp("latest_true_forward", _row(freshness, "latest_true_forward"),
         (forward_status or {}).get("latest_snapshot_date"),
         "data_freshness", "forward_prediction_skill", "EVIDENCE_DATE_MISMATCH")

    # An assessment dated AFTER the eligible session is an integrity fault.
    a = _coerce_date(latest_assessment_date)
    elig = _coerce_date(eligible_date)
    if a is not None and elig is not None and a > elig:
        violations.append({"code": "ASSESSMENT_AHEAD_OF_ELIGIBLE_SESSION",
                           "concept": "portfolio_assessment",
                           "value_assessment": a.isoformat(),
                           "value_eligible": elig.isoformat(),
                           "authoritative_owners": ["daily_action_gate",
                                                    "engine.market_session"],
                           "surface": "workflow_state"})

    # Structural self-checks — the primary action must follow the frozen policy,
    # and a stale/overdue assessment must never be presented as "today".
    if _EXPECTED_ACTION_FOR.get(overall) not in (None, primary_code):
        violations.append({"code": "PRIMARY_ACTION_POLICY_MISMATCH",
                           "concept": "primary_action", "overall_state": overall,
                           "primary_action": primary_code, "surface": "workflow_state"})

    if violations:
        return INCONSISTENT, violations
    if saw_unknown:
        return UNKNOWN, violations
    return CONSISTENT, violations


def _row(freshness: dict, source_id: str) -> Any:
    for r in (freshness.get("source_freshness") or []):
        if r.get("source_id") == source_id:
            return r.get("as_of_date")
    return None


# Structural map used by the consistency self-check: which primary action code a
# given overall state MUST produce (Workstream N.4 / K).
_EXPECTED_ACTION_FOR = {
    INCONSISTENT_STATE: ACTION_INSPECT_INCONSISTENCY,
    WAITING_FOR_SESSION_CLOSE: ACTION_WAIT_FOR_SESSION_CLOSE,
    WAITING_FOR_OWNED_DATA: ACTION_WAIT_FOR_OWNED_DATA,
    RESEARCH_CYCLE_REQUIRED: ACTION_RUN_RESEARCH_CYCLE,
    RESEARCH_CYCLE_RUNNING: ACTION_MONITOR_RESEARCH_CYCLE,
    RESEARCH_CYCLE_BLOCKED: ACTION_RESOLVE_RESEARCH_BLOCKER,
    PORTFOLIO_REASSESSMENT_REQUIRED: ACTION_RUN_PORTFOLIO_REASSESSMENT,
    READY_FOR_DAILY_CLOSE: ACTION_RUN_DAILY_CLOSE,
    MANUAL_REVIEW_REQUIRED: ACTION_MANUAL_REVIEW,
    DAILY_CYCLE_COMPLETE_EVIDENCE_GAP: ACTION_REVIEW_EVIDENCE_GAP,
    DAILY_CYCLE_COMPLETE: ACTION_MONITOR,
}


# --------------------------------------------------------------------------- #
# Public entry point.
# --------------------------------------------------------------------------- #
def load_workflow_state(
    *,
    now: Optional[datetime] = None,
    reference_today: Any = None,
    close_cutoff_et: Any = msession.DEFAULT_CLOSE_CUTOFF_ET,
    freshness: Optional[dict] = None,
    gate: Optional[dict] = None,
    close_progress: Optional[dict] = None,
    operational: Optional[dict] = None,
    inputs: Optional[dict] = None,
    daily_status: Optional[dict] = None,
    desk_marks: Optional[dict] = None,
    forward_status: Optional[dict] = None,
    target_readiness: Optional[dict] = None,
    research_cycle: Optional[dict] = None,
    reassessment_summary: Optional[dict] = None,
    decision_record: Optional[dict] = None,
    date_overrides: Optional[dict] = None,
    active_book_override: Any = None,
) -> dict[str, Any]:
    """Return the canonical workflow / operator-state contract (read-only).

    Clock: pass ``now`` (datetime) or ``reference_today`` (offline date). Every
    read model may be injected for deterministic tests; otherwise the existing
    authoritative loaders are called degrade-safely (each failure becomes a
    ``warnings[]`` entry, never an exception):

      * ``operational``  → ``operational_book.load_operational_book`` (active book)
      * ``close_progress`` → ``daily_close.load_close_progress`` (PROBE-FREE)
      * ``gate``         → ``daily_action_gate.load_daily_action_gate``
      * ``forward_status`` → ``forward_prediction_skill.load_prediction_skill``
      * ``target_readiness`` → ``alpha_target.load_readiness``
      * ``inputs``/``daily_status``/``desk_marks`` → the model-input / research
        loaders (shared with ``data_freshness`` so nothing is loaded twice)
      * ``reassessment_summary`` → ``portfolio_reassessment.load_reassessment_summary``
      * ``decision_record`` → ``portfolio_decision.load_decision_record``

    Stage 22: the last two are injectable for the same reason as every other read model
    above — a hermetic scenario must be able to bind EVERY seam. Without them the
    hermetic acceptance harness composed a synthetic world whose reassessment lane was
    silently read from the operator's REAL artifact store, so a scenario could "pass"
    while describing state that came from live evidence.
    """
    warnings: list[str] = []

    # --- Load the raw domain read models ONCE (degrade-safe), unless injected. -- #
    if operational is None:
        operational = _safe(lambda: _import_operational().load_operational_book(),
                            warnings, "Active operational book") or {}
    if close_progress is None:
        # PROBE-FREE: load_close_progress reads the close progress/journal file
        # only (load_daily_close would run the owned-EOD provider probe — forbidden).
        close_progress = _safe(lambda: _import_daily_close().load_close_progress(),
                               warnings, "Daily Close progress")
    if forward_status is None:
        forward_status = _safe(lambda: _import_fps().load_prediction_skill(),
                               warnings, "Forward-prediction skill status")
    if inputs is None:
        inputs = _safe(lambda: _import_engine().load_inputs(),
                       warnings, "Owned model inputs") or {}
    if daily_status is None:
        daily_status = _safe(
            lambda: _import_daily_refresh().load_current_alpha_daily_status(),
            warnings, "Champion research daily status") or {}
    if desk_marks is None:
        desk_marks = _safe(lambda: _import_desk().read_marks(),
                           warnings, "Desk marks") or {}
    if gate is None:
        # Read-only owned-data gate (no provider network, no prediction tunnel).
        gate = _safe(lambda: _import_gate().load_daily_action_gate(),
                     warnings, "Daily Action Gate")
    if target_readiness is None:
        target_readiness = _safe(lambda: _import_target().load_readiness(),
                                 warnings, "Alpha-target readiness")

    # --- Compose the Slice-1 freshness contract from the SAME read models. ----- #
    if freshness is None:
        freshness = _safe(
            lambda: df.load_data_freshness(
                now=now, reference_today=reference_today, close_cutoff_et=close_cutoff_et,
                operational=operational, inputs=inputs, daily_status=daily_status,
                desk_marks=desk_marks, daily_close_status=close_progress,
                forward_status=forward_status, date_overrides=date_overrides,
                active_book_override=active_book_override),
            warnings, "Data freshness") or {}
    for w in (freshness.get("warnings") or []):
        if w not in warnings:
            warnings.append(w)

    # --- Compose the Slice-3 Daily Research Cycle status (read-only) from the SAME
    #     freshness contract so nothing is loaded twice; degrade-safe. --------- #
    if research_cycle is None:
        research_cycle = _safe(
            lambda: _import_drc().load_daily_research_cycle_status(freshness=freshness),
            warnings, "Daily Research Cycle status") or {}

    session = freshness.get("market_session") or {}
    session_status = session.get("session_status") or msession.NO_CONFIRMED_DATA
    eligible_date = freshness.get("eligible_market_date")
    expected_date = freshness.get("expected_completed_market_date")
    calendar_date = (session.get("evaluated_at_et") or "")[:10] or None
    current_reference_date = calendar_date or expected_date or eligible_date

    # --- Operational facts (active book + close). ------------------------------ #
    ob = _op_book(operational)
    close_market_date = (close_progress or {}).get("market_date")
    # Release 29.3 — the Daily Close owner normalises its own persisted vocabulary; the
    # workflow owner re-applies it defensively so an injected/legacy progress document
    # can never republish a token that claims a portfolio proposal the close never owned.
    close_final_status = _normalize_close_status(
        (close_progress or {}).get("final_close_status"))
    close_status = close_final_status or _normalize_close_status(
        (close_progress or {}).get("status"))
    close_done = bool((close_progress or {}).get("done"))
    # Release 29.4 — the DAILY CLOSE OWNER answers this, not a local status mirror.
    operational_close_valid = _is_operational_close_complete(close_progress)
    latest_close_date = close_market_date
    close_failed = bool(close_final_status in _CLOSE_FAILED_STATUSES)

    elig_d = _coerce_date(eligible_date)
    close_d = _coerce_date(latest_close_date)
    has_confirmed_eligible = elig_d is not None
    # SESSION AUTHORITY (Release 29.4). Two independent owners answer two different
    # questions and this line composes them without re-deciding either:
    #   engine.market_session  -> WHICH completed session is eligible to work on
    #   api.daily_close        -> whether THAT session was operationally processed
    # The portfolio lane appears nowhere in this predicate, so no portfolio finding can
    # reopen a recorded close or make an already-processed session look runnable.
    eligible_session_closed = bool(
        operational_close_valid and elig_d is not None and close_d is not None
        and close_d >= elig_d)
    #: The Release 29.4 invariant, computed from the SAME two owners: a Daily Close must
    #: never be offered for a session the close owner already processed.
    eligible_session_already_processed = eligible_session_closed

    pending_orders = int(ob.get("pending_order_count") or 0)

    # --- Research facts (from the freshness rows / research owners). ----------- #
    research_rows = [r for r in (freshness.get("source_freshness") or [])
                     if r.get("kind") == df.KIND_RESEARCH
                     and r.get("required_for_signal_refresh")]
    stale_source_ids = sorted(r["source_id"] for r in research_rows
                              if r.get("status") not in df._SATISFIED)
    missing_source_ids = sorted(r["source_id"] for r in research_rows
                                if r.get("status") == df.MISSING)
    research_current = not stale_source_ids
    research_cycle_required = not research_current

    # --- Daily Research Cycle facts (Slice 3). --------------------------------- #
    drc_state = (research_cycle or {}).get("state")
    cycle_running = drc_state in _DRC_RUNNING_STATES
    cycle_blocked = drc_state in _DRC_BLOCKED_STATES
    cycle_complete = drc_state in _DRC_COMPLETE_STATES
    # Phase 29G.3: a DRC status of INCONSISTENT means a safe idempotent recovery is required
    # (e.g. terminal downstream artifacts without a run manifest); it is a genuine
    # inconsistency, never masked as NOT_STARTED / "assessment not run".
    cycle_inconsistent = (drc_state == INCONSISTENT)
    drc_blockers = list((research_cycle or {}).get("blockers") or [])

    # --- Assessment currency (Workstream F). ----------------------------------- #
    latest_assessment_date = (gate or {}).get("latest_completed_market_date")
    # Release 29.3 — these two fields USED to be the legacy rank-membership gate's
    # outcome/headline, which said "PROPOSAL_READY" / "PORTFOLIO CHANGES PROPOSED —
    # MANUAL REVIEW REQUIRED" while api.reallocation_proposal reported NOT_RUN. They are
    # now explicitly scoped as the legacy comparison; the canonical portfolio verdict is
    # attached separately below from its real owners.
    legacy_membership_result = (gate or {}).get("outcome")
    legacy_membership_recommendation = ((gate or {}).get("headline")
                                        or (gate or {}).get("explanation"))
    latest_assessment_result = legacy_membership_result
    latest_assessment_recommendation = legacy_membership_recommendation
    next_review_date = (gate or {}).get("next_scheduled_full_review")
    gate_review_due = bool((gate or {}).get("scheduled_review_due"))
    gate_target_state = (gate or {}).get("target_state")

    ac = classify_assessment(assessment_date=latest_assessment_date,
                             eligible_date=eligible_date,
                             current_reference_date=current_reference_date,
                             next_review_date=next_review_date,
                             review_due=gate_review_due)
    assessment_status = ac["status"]

    # Release 29.3: manual review is required when the CANONICAL portfolio-decision
    # owner says so — never because a rank-membership diff exists. Resolved after the
    # portfolio-decision lane is composed (see ``manual_review_required`` below).
    manual_review_required = False

    # --- Stage 20 (Workstream I): the canonical PORTFOLIO-REASSESSMENT lane. -------- #
    #     A PURE ARTIFACT READ of the immutable reassessment for the active book +
    #     eligible session (api.portfolio_reassessment.load_reassessment_summary). The
    #     workflow owner computes NO economics of its own: the decision, the expected net
    #     improvement, the turnover, the blockers and the operator wording all come from
    #     the ONE canonical owner. Degrade-safe: a read failure leaves the lane NOT_RUN.
    prs_book_id = (freshness.get("active_book") or {}).get("active_book_id")
    if reassessment_summary is None:
        reassessment_summary = _safe(
            lambda: _import_reassessment().load_reassessment_summary(
                active_book_id=prs_book_id, eligible_market_date=eligible_date),
            warnings, "Portfolio reassessment summary") or {}
    reassessment_state = (reassessment_summary.get("reassessment_state")
                          or PRS_NOT_RUN)
    reassessment_proposal_required = bool(reassessment_summary.get("proposal_required"))
    # Stage-19 PRECEDENCE. A reassessment is EVIDENCE; a confirmed order plan is a
    # COMMITMENT. While paper orders from a confirmed plan are still awaiting their
    # NEXT_CLOSE settlement, a newly produced proposal must never overwrite, obscure or
    # compete with the execution lifecycle — so it does NOT raise the manual-review gate
    # and does NOT become the operator's primary action. It stays fully readable as
    # evidence in its own lane.
    reassessment_execution = _safe(
        lambda: _import_reassessment().execution_precedence(
            rebalance_state=None, pending_orders=pending_orders),
        warnings, "Reassessment execution precedence") or {
            "execution_active": bool(pending_orders), "reassessment_outranked": bool(pending_orders)}
    # Release 46.2 — the ONE portfolio-attention verdict. Both review reasons (an
    # economically justified PROPOSAL, and a hard-constraint breach requiring human
    # ADJUDICATION) are resolved here, by the single factored rule, so the overall
    # state, the primary action and the cross-surface invariant cannot disagree.
    # Before R46.2 only the proposal reason existed, and a constraint breach — which
    # proposes nothing by design — fell through every gate into DAILY_CYCLE_COMPLETE.
    portfolio_attention_verdict = portfolio_attention(
        reassessment_state=reassessment_state,
        proposal_required=reassessment_proposal_required,
        blockers=list(reassessment_summary.get("blockers") or []),
        execution_active=bool(reassessment_execution.get("execution_active")),
        held_name_breaches=list(
            reassessment_summary.get("held_name_constraint_breaches") or []))
    attention_reason = portfolio_attention_verdict["review_reason"]
    reassessment_manual_review = bool(attention_reason == REVIEW_REASON_PROPOSAL)
    reassessment_constraint_review = bool(
        attention_reason == REVIEW_REASON_CONSTRAINT_BREACH)
    manual_review_required = bool(manual_review_required
                                  or portfolio_attention_verdict["review_required"])
    # Stage 22.1 — did the reassessment reach a portfolio-level verdict at all? A blocked
    # reassessment leaves ``proposal_required`` False by design, which is indistinguishable
    # from "no change is justified" unless the STATE itself is consulted. It is consulted
    # here, through the ONE factored rule the evidence classifier also uses, so the two can
    # never disagree. NOT suppressed by an in-flight execution: Stage-19 precedence exists
    # to stop a competing PROPOSAL from outranking a commitment, and this is not a proposal
    # — it is a named blocker the operator has to repair either way.
    reassessment_blockers = list(reassessment_summary.get("blockers") or [])
    reassessment_blocker_codes = blocker_codes_of(reassessment_blockers)
    reassessment_blocked = reassessment_blocks_cycle(
        reassessment_state=reassessment_state, blockers=reassessment_blockers)

    # --- Holding Opportunity-Cost summary (Phase 29G.2 — the canonical primary
    #     portfolio decision). The gate is the ONE owner that delegates to the HOC
    #     read-summary (api.holding_opportunity_cost.load_assessment_summary) for the
    #     active book + eligible session, so the workflow owner reads the HOC state from
    #     the gate's already-loaded opportunity_cost_* fields — one documented shared
    #     state path (no second HOC loader, no recomputation). ---------------------- #
    hoc_available = bool((gate or {}).get("opportunity_cost_available"))
    hoc_state = (gate or {}).get("opportunity_cost_state") or _HOC_NOT_RUN
    hoc_counts = (gate or {}).get("opportunity_cost_recommendation_counts") or {}
    hoc_hash = (gate or {}).get("opportunity_cost_assessment_hash")
    hoc_gaps = (gate or {}).get("opportunity_cost_data_gaps") or []
    # Release 29.5 — the artifact owner's provenance classification, carried through the
    # SAME shared gate path as every other opportunity-cost field. This module reads the
    # classification; it never derives one (an audit invariant).
    hoc_artifact_class = (gate or {}).get("opportunity_cost_artifact_class")
    hoc_producer_owner = (gate or {}).get("opportunity_cost_producer_owner")
    hoc_claims_drc_terminal = bool((gate or {}).get("opportunity_cost_claims_drc_terminal"))

    # --- Slice 7 (Phase 29H) reallocation-proposal state (same shared gate path;
    #     the gate delegates to api.reallocation_proposal.load_proposal_summary). The
    #     workflow owner only EXPOSES the proposal state as an informational review —
    #     it never creates a proposal and never turns it into an order/close requirement.
    rp_available = bool((gate or {}).get("reallocation_proposal_available"))
    rp_state = (gate or {}).get("reallocation_proposal_state") or _RP_NOT_RUN
    rp_counts = (gate or {}).get("reallocation_action_counts") or {}
    rp_hash = (gate or {}).get("reallocation_proposal_hash")
    rp_gaps = (gate or {}).get("reallocation_data_gaps") or []

    # --- Evidence facts (documented gap, never fabricated). -------------------- #
    latest_snapshot_date = (forward_status or {}).get("latest_snapshot_date")
    snap_d = _coerce_date(latest_snapshot_date)
    active_book_snapshot_present = bool(
        snap_d is not None and close_d is not None and snap_d >= close_d)
    evidence_gap = bool(operational_close_valid and not active_book_snapshot_present)

    owned_data_lag = session_status in (msession.WAITING_FOR_OWNED_DATA,
                                        msession.NO_CONFIRMED_DATA)

    # --- Overall workflow state (deterministic priority policy). --------------- #
    fresh_consistency = freshness.get("consistency_status")
    inconsistent_inputs = (fresh_consistency == INCONSISTENT)
    reassessment_satisfied = bool(cycle_complete and hoc_available)
    # Stage 22 (Workstream D) — the POST-CLOSE RESEARCH REQUIREMENT. A completed Daily
    # Close for the eligible session makes the Daily Research Cycle DUE until a Holding
    # Opportunity-Cost assessment exists for that same session. The requirement applies
    # ONLY when the HOC contract is actually observable on the gate: an absent contract
    # is UNVERIFIABLE, and inferring "not run" from a missing key would fabricate a
    # requirement (the rule api.portfolio_reassessment.economic_currency already uses).
    hoc_contract_observable = bool(
        gate is not None
        and ("opportunity_cost_available" in gate or "opportunity_cost_state" in gate))
    # Release 29.5 — GOVERNED EVIDENCE vs LIVE SIGNAL STATE. The rule above asked only
    # whether an opportunity-cost artifact EXISTS. Since Release 28/29 that question no
    # longer answers "did the governed cycle run for this session?": continuous collection
    # triggers the incremental event refresh many times a day, and it writes a perfectly
    # real artifact through the same canonical owner. A completed close plus one of those
    # live artifacts therefore reported the daily cycle as satisfied and dropped the
    # operator into the terminal region without the governed cycle ever having run.
    #
    # The governed answer belongs to the cycle owner, which holds the run manifest. It is
    # READ here (api.daily_research_cycle: governed_research_evidence_current), never
    # re-derived from the artifact — this module classifies no provenance of its own.
    # Degrade-safe: when the DRC contract is not observable, the old artifact-existence
    # rule still applies, so an unreadable cycle status can never fabricate a requirement.
    drc_contract_observable = bool(
        isinstance(research_cycle, dict) and research_cycle.get("state"))
    # The explicit contract field when the cycle owner publishes it; otherwise its
    # manifest-backed COMPLETE state, which is the same answer stated less precisely. The
    # fallback keeps an older/partial status contract behaving exactly as before.
    governed_research_evidence_current = bool(
        research_cycle.get("governed_research_evidence_current", cycle_complete)
        if drc_contract_observable else False)
    research_cycle_due_after_close = bool(
        eligible_session_closed
        and ((hoc_contract_observable and not hoc_available)
             or (drc_contract_observable and not governed_research_evidence_current)))
    # Current signal information exists, but the governed cycle has not produced it. This
    # is the state the operator must be able to SEE without it being mistaken for a
    # completed daily cycle — the whole point of the two-class distinction.
    live_pre_drc_signal_present = bool(hoc_available
                                       and not governed_research_evidence_current)
    overall = _decide_overall(
        inconsistent=inconsistent_inputs, session_status=session_status,
        has_confirmed_eligible=has_confirmed_eligible,
        eligible_session_closed=eligible_session_closed,
        owned_data_lag=owned_data_lag, research_current=research_current,
        assessment_status=assessment_status,
        manual_review_required=manual_review_required, evidence_gap=evidence_gap,
        cycle_running=cycle_running, cycle_blocked=cycle_blocked,
        cycle_inconsistent=cycle_inconsistent, cycle_complete=cycle_complete,
        hoc_current=hoc_available,
        research_cycle_due_after_close=research_cycle_due_after_close,
        reassessment_blocked=reassessment_blocked)

    primary = assert_primary_action_contract(_primary_action(overall, {
        "eligible_date": eligible_date,
        "research_cycle_due_after_close": research_cycle_due_after_close,
        "live_pre_drc_signal_present": live_pre_drc_signal_present,
        "reassessment_blocked": reassessment_blocked,
        "reassessment_blocker_codes": reassessment_blocker_codes,
        "session_operator_action": session.get("operator_action")}))
    primary_code = primary["action_code"]
    # Stage 19.3 — when the promoted action is the Daily Close and paper orders are
    # working, the operator is told (in the SAME sentence) that the close also settles
    # them. Pure presentation over the pending count the operational owner reported.
    if primary.get("execution_kind") == EXEC_DAILY_CLOSE and pending_orders > 0:
        primary = dict(primary, explanation=(
            "%s %d NEXT_CLOSE paper order(s) are working; this Daily Close settles the "
            "eligible ones through the Paper Desk before reassessing the portfolio."
            % (primary.get("explanation") or "", int(pending_orders))).strip())
    # Stage 20 — when the manual-review gate was raised by the PORTFOLIO REASSESSMENT
    # (not by the legacy target gate), the single primary action is named for what it
    # actually is and carries the reassessment's own canonical numbers. Pure presentation
    # over values the reassessment owner computed: no second action is added, and the
    # action code / contract are unchanged (still ONE primary action).
    if overall == MANUAL_REVIEW_REQUIRED and reassessment_manual_review:
        primary = dict(
            primary,
            label="Review the proposed portfolio change",
            headline="A portfolio change is economically justified — review it.",
            current_task="Review the proposed portfolio change.",
            destination=DEST_PORTFOLIO_MANAGER,
            focus="reassessment",
            explanation=(
                "The portfolio reassessment for %s found a change that clears the "
                "portfolio-level economic gate after switching cost, turnover, "
                "concentration and churn controls (expected net improvement %s score "
                "points; expected one-way turnover %s). Review the proposal — nothing is "
                "approved, no order plan is confirmed and no order is created "
                "automatically."
                % (eligible_date or "the eligible session",
                   reassessment_summary.get("expected_net_improvement"),
                   reassessment_summary.get("expected_one_way_turnover"))))
        primary = assert_primary_action_contract(primary)
    # Release 46.2 — the OTHER manual-review reason: a retained holding breaches a hard
    # portfolio constraint. The action code stays ACTION_MANUAL_REVIEW (one primary
    # action, and ``_EXPECTED_ACTION_FOR`` still matches); only the wording and the
    # named operator action are specialised, from the reassessment owner's own
    # vocabulary. It offers nothing to execute: REVIEWING a breach creates no order, no
    # proposal and no portfolio change, and the constraint is NOT auto-corrected.
    elif overall == MANUAL_REVIEW_REQUIRED and reassessment_constraint_review:
        breaches = list(portfolio_attention_verdict["constraint_breaches"])
        primary = dict(
            primary,
            label="Review the portfolio constraint breach",
            headline="A holding breaches a hard portfolio constraint — adjudicate it.",
            current_task="Review the portfolio constraint breach.",
            destination=DEST_PORTFOLIO_MANAGER,
            focus="reassessment",
            severity=SEV_ATTENTION,
            execution_available=False,
            operator_action=ACTION_REVIEW_PORTFOLIO_CONSTRAINT_BREACH,
            constraint_breaches=breaches,
            constraint_breach_count=len(breaches),
            explanation=(
                "%s Nothing is proposed and nothing is corrected automatically: "
                "reviewing creates no order, no proposal and no portfolio change, "
                "requires no rebalance approval, and leaves automation OFF. The "
                "breach stays on the record until a person adjudicates it."
                % (reassessment_summary.get("explanation")
                   or ("The portfolio reassessment for %s found %d hard constraint "
                       "breach(es) on retained holdings (%s)."
                       % (eligible_date or "the eligible session", len(breaches),
                          ", ".join(breaches) or "see the reassessment blockers")))))
        primary = assert_primary_action_contract(primary)

    queued = _queued_actions(
        primary_code=primary_code, research_current=research_current,
        assessment_status=assessment_status,
        eligible_session_closed=eligible_session_closed,
        has_confirmed_eligible=has_confirmed_eligible, evidence_gap=evidence_gap,
        manual_review_required=manual_review_required, pending_orders=pending_orders,
        cycle_active=(cycle_running or cycle_blocked), hoc_available=hoc_available,
        reallocation_available=rp_available,
        reassessment_satisfied=reassessment_satisfied,
        reassessment_proposal_required=reassessment_proposal_required,
        reassessment_execution_active=bool(
            reassessment_execution.get("execution_active")))

    # --- Blockers + warnings (Workstream C.12/13). ----------------------------- #
    blockers: list[dict[str, Any]] = []
    if cycle_inconsistent:
        # Phase 29G.3 recovery: surface the DRC's exact reason (e.g. terminal downstream
        # artifacts without a run manifest) and the safe idempotent recovery action — never
        # a "the assessment has not run" / NOT_STARTED message.
        for b in drc_blockers:
            blockers.append({**b, "surface": "daily_research_cycle",
                             "recovery_required": True})
        if not drc_blockers:
            blockers.append({"code": "DAILY_RESEARCH_CYCLE_INCONSISTENT",
                             "surface": "daily_research_cycle", "recovery_required": True,
                             "detail": "The Daily Research Cycle status is INCONSISTENT; a "
                                       "safe idempotent recovery is required."})
    if reassessment_blocked:
        # Stage 22.1 — the named cause of the missing portfolio verdict, carried verbatim
        # from the ONE reassessment owner. Reported whenever the reassessment is blocked,
        # not only when it won the priority policy, so an earlier-ranked action (e.g. an
        # unconfirmed session) never hides why the portfolio decision is unavailable.
        for b in reassessment_blockers:
            row = dict(b) if isinstance(b, dict) else {"code": str(b)}
            blockers.append({**row, "surface": "portfolio_reassessment",
                             "reassessment_state": reassessment_state,
                             "blocks_portfolio_decision": True,
                             "recovery_required": True})
        if not reassessment_blockers:
            blockers.append({"code": "PORTFOLIO_REASSESSMENT_%s" % reassessment_state,
                             "surface": "portfolio_reassessment",
                             "reassessment_state": reassessment_state,
                             "blocks_portfolio_decision": True,
                             "recovery_required": True,
                             "detail": "The portfolio reassessment reached no "
                                       "portfolio-level verdict and named no cause."})
    if overall == WAITING_FOR_OWNED_DATA:
        blockers.append({"code": "OWNED_DATA_NOT_CONFIRMED",
                         "detail": session.get("reason") or "Owned market data is "
                         "not confirmed for the expected session."})
    if close_failed:
        blockers.append({"code": "DAILY_CLOSE_FAILED",
                         "detail": "The operational Daily Close reported %s."
                         % close_status})
    for sid in stale_source_ids:
        blockers.append({"code": "RESEARCH_INPUT_STALE", "source_id": sid})

    if operational_close_valid and (research_cycle_required
                                    or assessment_status in _ASSESS_NEEDS_ACTION):
        warnings.append(
            "The completed operational close at %s remains valid; research/"
            "assessment staleness is separate (D-7)." % (latest_close_date or "n/a"))
    if evidence_gap:
        warnings.append(
            "A forward-evidence (TRUE_FORWARD) gap is documented for %s; it is "
            "attention-level and does not invalidate the completed close."
            % (latest_close_date or "the completed close"))

    # --- Assessment/preserved-history summary (Workstream F rule 3/5). --------- #
    preserved_statement = None
    if latest_assessment_result == _GATE_OUTCOME_NO_ACTION and latest_assessment_date:
        preserved_statement = (
            "No portfolio change was required as of the %s assessment."
            % latest_assessment_date)
        if assessment_status != ASSESS_CURRENT:
            preserved_statement += " That assessment is now %s; a fresh reassessment " \
                                   "is due." % assessment_status.lower()

    completed_summary = {
        "latest_completed_close": {
            "market_date": latest_close_date, "status": close_status,
            "valid": operational_close_valid},
        "latest_portfolio_assessment": {
            "market_date": latest_assessment_date,
            # Release 29.3 — scoped to what it actually is (see portfolio_assessment_state).
            "result": latest_assessment_result,
            "recommendation": latest_assessment_recommendation,
            "scope": "LEGACY_RANK_MEMBERSHIP_COMPARISON",
            "owner": "api.daily_action_gate",
            "is_portfolio_proposal": False,
            "preserved_statement": preserved_statement,
            "current_for_eligible_session": ac["current_for_eligible_session"]},
        "latest_research_refresh": {"market_date": _row(freshness, "price_score_refresh")},
        "latest_evidence_snapshot": {"market_date": latest_snapshot_date},
    }

    # --- Canonical presentation contract (Workstream B/D — Phase 29G.2 cutover). --- #
    # The ONE PRIMARY portfolio-decision presentation is the Holding Opportunity-Cost
    # Review; it is what every operator surface (Command Center / Daily Workflow /
    # Portfolio Manager) renders as ``assessment_presentation``. Before the first
    # production HOC artifact it presents NOT_RUN with recommendations "NONE YET" (no
    # fabricated counts). The legacy daily-action-gate rank-membership comparison is
    # reclassified compatibility-only under ``legacy_membership_comparison`` and is NEVER
    # a primary decision. The UI renders these values verbatim (never derives them).
    hoc_membership_add = len((gate or {}).get("proposed_additions") or [])
    hoc_membership_remove = len((gate or {}).get("proposed_removals") or [])
    hoc_membership_resize = len((gate or {}).get("proposed_resizes") or [])
    holding_opportunity_cost_presentation = build_holding_opportunity_cost_presentation(
        state=hoc_state, available=hoc_available, eligible_date=eligible_date,
        active_book_label=(freshness.get("active_book") or {}).get("active_book_name"),
        recommendation_counts=hoc_counts, assessment_hash=hoc_hash, data_gaps=hoc_gaps)
    canonical_operator_state = holding_opportunity_cost_presentation["canonical_operator_state"]
    # Slice 7 (Phase 29H) reallocation-proposal presentation, rendered verbatim by the UI
    # as an informational review card (never a primary decision, never an order/close gate).
    reallocation_proposal_presentation = build_reallocation_proposal_presentation(
        state=rp_state, available=rp_available, eligible_date=eligible_date,
        action_counts=rp_counts, proposal_hash=rp_hash,
        proposed_holding_count=(gate or {}).get("reallocation_proposed_holding_count"),
        score_improvement=(gate or {}).get("reallocation_score_improvement"),
        one_way_turnover=(gate or {}).get("reallocation_one_way_turnover"),
        estimated_transaction_cost=(gate or {}).get("reallocation_estimated_transaction_cost"),
        data_gaps=rp_gaps,
        # Release 46.6.2 - a completed cycle that WITHHELD a proposal must not
        # be presented as a cycle that has not run.
        cycle_complete=cycle_complete,
        cycle_run_id=(research_cycle or {}).get("run_id"),
        reassessment_state=reassessment_state)
    reallocation_operator_state = reallocation_proposal_presentation["canonical_operator_state"]
    # Stage 20: the ACTIVE PORTFOLIO ASSESSMENT presentation. Built by the canonical
    # reassessment owner (never here) so the operator wording, the single action and the
    # Stage-19 precedence suppression all have exactly one source. The UI renders it
    # verbatim and derives nothing.
    reassessment_presentation = _safe(
        lambda: _import_reassessment().build_presentation(
            state=reassessment_state,
            reassessment={"decision": {
                "holdings_evaluated": reassessment_summary.get("holdings_evaluated"),
                "expected_net_improvement": reassessment_summary.get(
                    "expected_net_improvement"),
                "expected_one_way_turnover": reassessment_summary.get(
                    "expected_one_way_turnover"),
                "expected_transaction_cost_usd": reassessment_summary.get(
                    "expected_transaction_cost_usd"),
                "blockers": reassessment_summary.get("blockers") or []},
                "attention": {"count": reassessment_summary.get("attention_count") or 0},
                "explanation": reassessment_summary.get("explanation")},
            execution=reassessment_execution),
        warnings, "Portfolio reassessment presentation") or {
            "title": PRS_TITLE, "state": reassessment_state,
            "operator_state": PRS_NOT_RUN, "primary_action": None}
    reassessment_lane = {
        "title": PRS_TITLE,
        "canonical_owner": PRS_CANONICAL_OWNER,
        "calculation_owner": "engine.portfolio_reassessment",
        "state": reassessment_state,
        "state_vocabulary": list(PRS_OPERATOR_STATES),
        "available": bool(reassessment_summary.get("reassessment_available")),
        "reassessment_id": reassessment_summary.get("reassessment_id"),
        "reassessment_hash": reassessment_summary.get("reassessment_hash"),
        "reassessment_date": reassessment_summary.get("reassessment_date"),
        "decision": reassessment_summary.get("decision") or PRS_NOT_RUN,
        "proposal_required": reassessment_proposal_required,
        "holdings_evaluated": reassessment_summary.get("holdings_evaluated") or 0,
        "attention_count": reassessment_summary.get("attention_count") or 0,
        "expected_net_improvement": reassessment_summary.get("expected_net_improvement"),
        "expected_one_way_turnover": reassessment_summary.get("expected_one_way_turnover"),
        "expected_transaction_cost_usd": reassessment_summary.get(
            "expected_transaction_cost_usd"),
        "blockers": reassessment_summary.get("blockers") or [],
        "reason_codes": reassessment_summary.get("reason_codes") or [],
        "explanation": reassessment_summary.get("explanation"),
        "policy_version": reassessment_summary.get("policy_version"),
        # Stage-19 precedence: while an execution is in flight the reassessment is
        # EVIDENCE ONLY and contributes no action.
        "execution_precedence": reassessment_execution,
        "raises_manual_review": reassessment_manual_review,
        # Release 46.2 — the ONE portfolio-attention verdict, carried verbatim so every
        # surface renders the same interpretation and none derives one of its own.
        "portfolio_attention": portfolio_attention_verdict,
        "raises_constraint_breach_review": reassessment_constraint_review,
        "constraint_breaches": portfolio_attention_verdict["constraint_breaches"],
        "review_only": True,
        "creates_orders": False,
        "sole_execution_path": "POST /v1/operations/daily-research-cycle/run",
    }
    # The PRIMARY card payload is the HOC presentation (owned by the UI's
    # renderWorkflowState canonical nodes) — it never carries the legacy "LATEST
    # PORTFOLIO ASSESSMENT" / "PROPOSAL READY" / "PORTFOLIO CHANGES PROPOSED" wording.
    assessment_presentation = holding_opportunity_cost_presentation
    legacy_membership_comparison = build_assessment_presentation(
        assessment_status=assessment_status, assessment_date=latest_assessment_date,
        outcome=latest_assessment_result, recommendation=latest_assessment_recommendation,
        current_for_eligible=ac["current_for_eligible_session"],
        membership_add_count=hoc_membership_add,
        membership_remove_count=hoc_membership_remove,
        membership_resize_count=hoc_membership_resize)
    evidence_presentation = build_evidence_presentation(
        operational_close_valid=operational_close_valid, latest_close_date=latest_close_date,
        evidence_gap=evidence_gap, active_book_snapshot_present=active_book_snapshot_present,
        current_session_open=(session_status == msession.BEFORE_SESSION_CLOSE))

    # --- Model governance (read-only; promotion always manual). ---------------- #
    fps_active = (forward_status or {}).get("active_book") or {}
    fps_shadows = (forward_status or {}).get("shadow_books") or []
    model_governance = {
        "champion": fps_active.get("model_id") or fps_active.get("book_id"),
        "champion_label": fps_active.get("label"),
        "challenger": (fps_shadows[0].get("model_id") if fps_shadows
                       and isinstance(fps_shadows[0], dict) else None),
        "latest_revalidation_date": None,
        "revalidation_status": "NO_REVALIDATION_DUE",
        "revalidation_due": False,
        "manual_review_required": bool(manual_review_required),
        "automatic_promotion_allowed": False,
    }
    # Stage 18 (Workstream J): the canonical model-recalibration REVIEW trigger — a
    # SEPARATE lane from the portfolio decision, wired to the forward-evidence
    # signal-degradation flags (never a retrainer, never automatic, never from P&L).
    model_review = _derive_model_review(forward_status)
    model_governance["model_review_state"] = model_review["model_review_state"]
    model_governance["model_review_required"] = model_review["model_review_required"]
    model_governance["model_review_triggering_flags"] = model_review["triggering_flags"]
    model_governance["model_review_missing_evidence"] = model_review["missing_evidence"]

    # --- Stage 18 (Workstream C/D/E): the SEPARATE portfolio-decision review lane.
    #     Composed from the immutable reallocation-proposal summary (already loaded via
    #     the shared gate path) + the durable manual decision recorded by the dedicated
    #     owner (api.portfolio_decision). It NEVER enters OVERALL_STATES, NEVER gates the
    #     Daily Close, and creates no order. Degrade-safe: any read failure leaves the
    #     lane at its no-proposal default and never raises. --------------------------- #
    rp_summary = {
        "reallocation_proposal_available": rp_available,
        "reallocation_proposal_state": rp_state,
        "reallocation_proposal_hash": rp_hash,
        "reallocation_proposal_id": (gate or {}).get("reallocation_proposal_id"),
        "reallocation_action_counts": rp_counts,
        "reallocation_proposed_holding_count": (gate or {}).get(
            "reallocation_proposed_holding_count"),
        "reallocation_one_way_turnover": (gate or {}).get("reallocation_one_way_turnover"),
        "reallocation_estimated_transaction_cost": (gate or {}).get(
            "reallocation_estimated_transaction_cost"),
        "reallocation_score_improvement_net_of_cost": (gate or {}).get(
            "reallocation_score_improvement_net_of_cost"),
        "reallocation_data_gaps": rp_gaps,
        # Release 29.3 — the complete-target withhold verdict travels with the summary so
        # the decision owner can never present a withheld target as approvable.
        "reallocation_proposal_withheld": bool(
            (gate or {}).get("reallocation_proposal_withheld")
            or rp_state == _RP_WITHHELD),
        "reallocation_withheld_reasons": list(
            (gate or {}).get("reallocation_withheld_reasons") or []),
        "reallocation_proposal_stale": bool((gate or {}).get("reallocation_proposal_stale")),
        "reallocation_proposal_stale_reason": (gate or {}).get(
            "reallocation_proposal_stale_reason"),
        # Release 47 — the outcome, the reshaping ledger and the switching hurdle
        # travel with the summary, so the decision owner and every surface below it
        # read ONE authoritative verdict instead of inferring one from a state name.
        "reallocation_outcome": (gate or {}).get("reallocation_outcome"),
        "reallocation_outcome_vocabulary": list(REALLOCATION_OUTCOME_VOCABULARY),
        "reallocation_outcome_headline": (gate or {}).get(
            "reallocation_outcome_headline"),
        "reallocation_outcome_reason_codes": list(
            (gate or {}).get("reallocation_outcome_reason_codes") or []),
        "reallocation_constraints_reshaped": list(
            (gate or {}).get("reallocation_constraints_reshaped") or []),
        "reallocation_constraint_reoptimized": bool(
            (gate or {}).get("reallocation_constraint_reoptimized")),
        "reallocation_feasible_target_exists": bool(
            (gate or {}).get("reallocation_feasible_target_exists")),
        "reallocation_switching_hurdle": (gate or {}).get(
            "reallocation_switching_hurdle"),
        "reallocation_clears_switching_hurdle": (gate or {}).get(
            "reallocation_clears_switching_hurdle"),
    }
    active_book_id = (freshness.get("active_book") or {}).get("active_book_id")
    if decision_record is None:
        decision_record = _safe(
            lambda: _import_portfolio_decision().load_decision_record(
                active_book_id=active_book_id, eligible_market_date=eligible_date),
            warnings, "Portfolio decision record")
    portfolio_decision_lane = _safe(
        lambda: _import_portfolio_decision().derive_decision_state(
            has_active_book=bool(active_book_id), proposal_summary=rp_summary,
            decision_record=decision_record),
        warnings, "Portfolio decision lane") or {
            "portfolio_decision_state": "PORTFOLIO_DECISION_UNAVAILABLE",
            "requires_manual_review": False, "material": False}

    # --- Stage 22 (Workstream A / G): the canonical NORMAL CYCLE. The workflow owner
    #     PROJECTS its already-decided state onto the pure cycle kernel; the kernel owns
    #     the sequence, the per-stage gates and the four operator answers. The kernel
    #     also enforces the single-primary-mutation invariant, so a composition that
    #     somehow opened two gates raises here instead of reaching a browser. ------- #
    normal_cycle = ncycle.build_cycle_view(
        overall=overall,
        current_task=primary.get("current_task"),
        why=primary.get("explanation"),
        action_available=bool(primary.get("execution_available")
                              and primary.get("execution_kind")
                              in NORMAL_PATH_EXECUTION_KINDS),
        action_label=primary.get("label"),
        no_action_text=NO_ACTION_TEXT,
        eligible_market_date=eligible_date,
        latest_completed_close_date=latest_close_date,
        execution_active=bool(reassessment_execution.get("execution_active")),
        # Release 46.2 repair — the ONE attention verdict, handed to the kernel instead
        # of letting it infer a second one from the overall state. The kernel's own
        # derivation is only correct while the review IS the overall state; an
        # outstanding review that a due close or a stale input legitimately outranks was
        # invisible to it, so the cycle reported "no action required" over a live
        # adjudication. The verdict has already applied Stage-19 execution precedence.
        review_required=portfolio_attention_verdict["review_required"],
        blockers=[b.get("code") for b in blockers if isinstance(b, dict)],
        # Stage 22.1 — the stages this owner has authoritatively observed COMPLETE for
        # the eligible session. Wording/status only: it opens no gate and changes no
        # mutation count. It exists so a suspended cycle still reports the valid work it
        # recorded (the Daily Close that ran, the signal refresh that completed) instead
        # of blanking every stage into "unavailable".
        completed_stages=(
            ([ncycle.STAGE_DAILY_CLOSE] if eligible_session_closed else [])
            + ([ncycle.STAGE_DAILY_RESEARCH_CYCLE]
               if (cycle_complete and hoc_available) else [])))

    # --- Stage 22 (Workstream B): is the blocked/stale assessment a SYSTEM BLOCKER the
    #     operator must fix now, or EXPECTED stale evidence the next canonical cycle
    #     supersedes? Fail-closed semantics are unchanged either way. ------------- #
    evidence_classification = build_evidence_classification(
        reassessment_state=reassessment_state, overall=overall,
        blockers=(reassessment_summary.get("blockers") or []),
        eligible_date=eligible_date,
        # The assessment is ALWAYS replaced by the Daily Research Cycle — that is the
        # sole path that produces one. Naming the next cycle STAGE here would tell the
        # operator the Daily Close replaces the assessment, which it does not.
        next_cycle_action="the next Daily Research Cycle")

    # --- Stage 22 (Workstream E): ONE fail-closed binding verdict over the fresh
    #     assessment and the proposal derived from it. ---------------------------- #
    assessment_binding = build_assessment_binding(
        eligible_date=eligible_date, active_book_id=active_book_id,
        hoc_available=hoc_available,
        hoc_bound_date=(gate or {}).get("opportunity_cost_bound_eligible_market_date"),
        hoc_bound_book=(gate or {}).get("opportunity_cost_bound_active_book_id"),
        hoc_assessment_hash=hoc_hash,
        hoc_stale=bool((gate or {}).get("opportunity_cost_stale")),
        hoc_stale_reason=(gate or {}).get("opportunity_cost_stale_reason"),
        proposal_available=rp_available,
        proposal_bound_assessment_hash=(gate or {}).get(
            "reallocation_bound_hoc_assessment_hash"),
        proposal_bound_date=(gate or {}).get("reallocation_bound_eligible_market_date"),
        proposal_hash=rp_hash)

    # --- Stage 22 (Workstream C): the machine-readable gap taxonomy, read verbatim
    #     from the HOC owner through the SAME shared gate path. The workflow owner
    #     consumes the ``blocking`` classification and never parses a gap string. --- #
    gap_taxonomy = ((gate or {}).get("opportunity_cost_data_gap_taxonomy")
                    or gaptax.summarize([], eligible_market_date=eligible_date))

    today_hero = _build_today_hero(
        overall=overall, primary=primary, eligible_date=eligible_date,
        operational_close_valid=operational_close_valid, latest_close_date=latest_close_date,
        portfolio_decision_lane=portfolio_decision_lane, model_review=model_review)

    # --- Cross-surface consistency (Workstream K). ----------------------------- #
    consistency_status, consistency_violations = _build_consistency(
        freshness=freshness, gate=gate, close_progress=close_progress,
        operational=operational, forward_status=forward_status,
        target_readiness=target_readiness, assessment_status=assessment_status,
        latest_assessment_date=latest_assessment_date, eligible_date=eligible_date,
        latest_close_date=latest_close_date, overall=overall,
        primary_code=primary_code, pending_orders=pending_orders)

    # --- Release 29.3: the ONE canonical portfolio-decision object. ------------- #
    canonical_portfolio_decision = build_canonical_portfolio_decision(
        reassessment_summary={**reassessment_summary,
                              "net_improvement_hurdle": (
                                  reassessment_summary.get("net_improvement_hurdle")
                                  or _PRS_NET_HURDLE_DEFAULT),
                              "turnover_budget": (
                                  reassessment_summary.get("turnover_budget")
                                  or _PRS_TURNOVER_BUDGET_DEFAULT)},
        reallocation_operator_state=reallocation_operator_state,
        portfolio_decision_lane=portfolio_decision_lane,
        attention_count=reassessment_summary.get("attention_count"),
        eligible_date=eligible_date,
        governed_research_evidence_current=governed_research_evidence_current)

    # --- Release 29.3: SEMANTIC decision-integrity invariants. ------------------ #
    # Every field below is read verbatim from its canonical owner; the check compares
    # owners and recomputes none of their economics.
    _mex_policy = (reassessment_summary.get("mandatory_exit_policy") or {})
    semantic_violations = check_decision_semantics(
        reallocation_operator_state=reallocation_operator_state,
        reallocation_approvable=bool(
            reallocation_operator_state in REALLOCATION_APPROVABLE_STATES
            and not rp_summary.get("reallocation_proposal_withheld")
            and not rp_summary.get("reallocation_proposal_stale")),
        reassessment_state=reassessment_state,
        reassessment_proposal_required=reassessment_proposal_required,
        portfolio_decision_state=portfolio_decision_lane.get("portfolio_decision_state"),
        portfolio_decision_requires_review=bool(
            portfolio_decision_lane.get("requires_manual_review")),
        portfolio_decision_approvable=bool(portfolio_decision_lane.get("approvable")),
        # The proposal and the reassessment are both bound to the SAME immutable HOC
        # assessment, so that hash is the provable binding between them. A proposal
        # carrying a different one was produced from different evidence.
        proposal_bound_reassessment_hash=(gate or {}).get(
            "reallocation_bound_hoc_assessment_hash"),
        current_reassessment_hash=reassessment_summary.get("hoc_assessment_hash"),
        mandatory_exit_tickers=(reassessment_summary.get("mandatory_exit_tickers") or []),
        mandatory_exit_obligation=_mex_policy.get("obligation"),
        # Release 47 — the outcome and the reshaping ledger, read from the proposal
        # owner's summary. The validator compares them; it never re-derives them.
        reallocation_outcome=rp_summary.get("reallocation_outcome"),
        feasible_target_exists=bool(
            rp_summary.get("reallocation_feasible_target_exists")),
        constraints_that_reshaped=list(
            rp_summary.get("reallocation_constraints_reshaped") or []),
        summary_claims={
            "operational_state.latest_close_status": close_status,
            "research_cycle_state.assessment_status":
                (research_cycle or {}).get("assessment_status"),
            "portfolio_assessment_state.latest_assessment_result":
                latest_assessment_result,
            "portfolio_assessment_state.latest_assessment_recommendation":
                latest_assessment_recommendation,
            "portfolio_assessment_state.gate_target_state": gate_target_state,
            "completed_summary.latest_completed_close.status": close_status,
            "completed_summary.latest_portfolio_assessment.result":
                latest_assessment_result,
            "completed_summary.latest_portfolio_assessment.recommendation":
                latest_assessment_recommendation,
            "portfolio_assessment_state.portfolio_decision_state":
                portfolio_decision_lane.get("portfolio_decision_state"),
        })
    # Release 29.4 — SESSION AUTHORITY. The composed payload may never offer a Daily
    # Close for a session the close owner already processed, report a completed close as
    # invalid, or hide one from the evidence presentation.
    _dc_gate = build_daily_close_gate(overall, eligible_date=eligible_date,
                                      latest_close_date=latest_close_date)
    session_violations = check_session_authority(
        session_status=session_status,
        eligible_market_date=eligible_date,
        expected_completed_market_date=expected_date,
        latest_completed_close_date=latest_close_date,
        operational_close_valid=operational_close_valid,
        latest_close_status=close_status,
        daily_close_execution_allowed=bool(_dc_gate.get("execution_allowed")),
        evidence_completed_close_state=(
            (evidence_presentation.get("latest_completed_close") or {}).get("state")))
    if session_violations:
        consistency_violations = list(consistency_violations) + session_violations
        consistency_status = INCONSISTENT
    if semantic_violations:
        consistency_violations = list(consistency_violations) + semantic_violations
        consistency_status = INCONSISTENT
    # Release 46.2 — THE PORTFOLIO-ATTENTION INVARIANT. The composed payload may never
    # assert that the eligible session needs nothing while the canonical reassessment
    # requires human adjudication. This is checked against the SAME normal-cycle view
    # the operator is shown, so a regression here fails on the payload rather than in a
    # browser.
    attention_violations = portfolio_attention_violations(
        attention=portfolio_attention_verdict, overall=overall,
        cycle_no_action_required=normal_cycle.get("no_action_required"),
        cycle_review_required=normal_cycle.get("review_required"),
        primary_action_code=primary_code)
    if attention_violations:
        consistency_violations = list(consistency_violations) + attention_violations
        consistency_status = INCONSISTENT
    if consistency_status == INCONSISTENT:
        warnings.append("Cross-surface consistency check found %d violation(s); see "
                        "consistency_violations." % len(consistency_violations))

    # De-duplicate warnings, preserving order.
    seen: set[str] = set()
    uniq_warnings = [w for w in warnings if not (w in seen or seen.add(w))]

    return {
        "status": "OK",
        "phase": PHASE,
        "evaluated_at": _now_iso(),
        "overall_state": overall,
        "overall_state_vocabulary": list(OVERALL_STATES),
        "current_task": primary["current_task"],
        "headline": primary["headline"],
        "primary_action": {
            "action_code": primary["action_code"], "label": primary["label"],
            "explanation": primary["explanation"], "severity": primary["severity"],
            "destination": primary["destination"], "focus": primary.get("focus"),
            "safe_to_execute": primary["safe_to_execute"],
            "execution_available": primary["execution_available"],
            "manual_confirmation_required": primary["manual_confirmation_required"],
            "slice3_pending": primary.get("slice3_pending", False),
            "confirmation_required": primary.get("confirmation_required"),
            # Operator Action Integrity: the machine-readable executor identity
            # (one of EXECUTION_KINDS) + its frozen contract, present EXACTLY for
            # the three real executions; the UI dispatcher maps it to the
            # EXISTING owner and never invents a second writer.
            "execution_kind": primary.get("execution_kind"),
            "execution_contract": (
                dict(EXECUTION_CONTRACTS[primary["execution_kind"]])
                if primary.get("execution_kind") in EXECUTION_CONTRACTS else None)},
        # Stage 19.3 (Workstream F): the ONE operator command every page mirrors.
        # Stage 22 (Workstream G): it also carries the canonical cycle position and the
        # "what happens after that" answer, so no surface writes its own.
        "operator_command": build_operator_command(
            overall=overall, primary=primary, pending_orders=pending_orders,
            eligible_date=eligible_date, latest_close_date=latest_close_date,
            cycle=normal_cycle),
        # Stage 22 (Workstream A): the canonical NORMAL DAILY PORTFOLIO CYCLE — the
        # ordered stages, where the operator is in them, what happens next, and the ONE
        # per-stage gate every surface obeys. There is no alternate path.
        "normal_cycle": normal_cycle,
        "normal_cycle_owner": NORMAL_CYCLE_OWNER,
        "normal_cycle_stage": normal_cycle["current_stage"],
        "normal_cycle_stage_gates": normal_cycle["stage_gates"],
        # Release 46.2 (Objective A): the ONE portfolio-attention verdict — MUTATION,
        # REVIEW or NONE — promoted to the top level so the Today hero, the Portfolio
        # page and the Active Manager all read the identical interpretation instead of
        # each inferring "is there anything to do?" from a different field.
        "portfolio_attention": portfolio_attention_verdict,
        "portfolio_attention_owner": WORKFLOW_STATE_OWNER,
        "portfolio_attention_kind": portfolio_attention_verdict["attention_kind"],
        "portfolio_review_required": portfolio_attention_verdict["review_required"],
        # Stage 22 (Workstream B): SYSTEM BLOCKER vs EXPECTED STALE EVIDENCE. The
        # fail-closed rule is unchanged; only the information hierarchy moves.
        "evidence_classification": evidence_classification,
        "evidence_class_vocabulary": list(EVIDENCE_CLASSES),
        "presentation_class_vocabulary": list(PRESENTATION_CLASSES),
        # Stage 22 (Workstream E): ONE fail-closed assessment / proposal binding verdict.
        "assessment_binding": assessment_binding,
        "assessment_binding_state_vocabulary": list(BINDING_STATES),
        # Stage 22 (Workstream C): the machine-readable data-gap taxonomy.
        "data_gap_taxonomy": gap_taxonomy,
        "data_gap_taxonomy_owner": DATA_GAP_TAXONOMY_OWNER,
        "blocking_data_gap_count": gap_taxonomy.get("blocking_gap_count", 0),
        # Operator Action Integrity (Defect 3): the canonical Daily-Close
        # availability verdict every secondary close surface obeys verbatim.
        "daily_close_gate": _dc_gate,
        "queued_actions": queued,
        "blockers": blockers,
        "warnings": uniq_warnings,
        "current_session": {
            "calendar_date": calendar_date,
            "expected_completed_market_date": expected_date,
            "latest_eligible_completed_market_date": eligible_date,
            "session_status": session_status,
            "session_close_cutoff": session.get("close_cutoff_et"),
            "owned_data_confirmation_date": session.get("latest_confirmed_owned_data_date"),
        },
        "operational_state": {
            "active_book_id": (freshness.get("active_book") or {}).get("active_book_id"),
            "active_book_name": (freshness.get("active_book") or {}).get("active_book_name"),
            "book_status": ob.get("current_status"),
            "valuation_date": _row(freshness, "operational_valuation"),
            "desk_mark_date": _row(freshness, "desk_marks"),
            "benchmark_date": _row(freshness, "benchmark"),
            "nav": ob.get("nav"),
            "cash": ob.get("cash"),
            "holdings_count": ob.get("holdings_count"),
            "pending_orders": pending_orders,
            "latest_completed_close_date": latest_close_date,
            "latest_close_status": close_status,
            "operational_close_valid": operational_close_valid,
            "operational_consistency_status": fresh_consistency,
            # --- Release 29.4 SESSION AUTHORITY (two owners, no re-derivation) ---- #
            "close_validity_owner": CLOSE_VALIDITY_OWNER,
            "close_validity_policy": "OPERATIONAL_COMPLETION_ONLY",
            "close_run_completed": close_done,
            "session_eligibility_owner": SESSION_ELIGIBILITY_OWNER,
            "eligible_market_date": eligible_date,
            "eligible_session_closed": eligible_session_closed,
            "eligible_session_already_processed": eligible_session_already_processed,
            "close_validity_independent_of_portfolio_outcome": True,
        },
        "research_state": {
            "latest_price_score_refresh_date": _row(freshness, "price_score_refresh"),
            "monthly_momentum_input_date": _row(freshness, "momentum_monthly"),
            "fundamental_data_date": _row(freshness, "fundamental_quarterly"),
            "champion_research_mark_date": _row(freshness, "champion_research_mark"),
            "latest_daily_research_run_date": (daily_status or {}).get("latest_valid_mark_date"),
            "daily_research_run_status": (daily_status or {}).get("status"),
            "target_calculation_date": _row(freshness, "target_calculation"),
            "latest_true_forward_snapshot_date": latest_snapshot_date,
            "research_inputs_current": research_current,
            "research_cycle_required": research_cycle_required,
            "stale_source_ids": stale_source_ids,
            "missing_source_ids": missing_source_ids,
        },
        "research_cycle_state": {
            "state": drc_state,
            "executable": bool((research_cycle or {}).get("executable")),
            "eligible_market_date": (research_cycle or {}).get("eligible_market_date"),
            "run_id": (research_cycle or {}).get("run_id"),
            "current_step": (research_cycle or {}).get("current_step"),
            "completed_steps": (research_cycle or {}).get("completed_steps"),
            "required_stale_inputs": ((research_cycle or {}).get("input_plan")
                                      or {}).get("required_stale_inputs"),
            "blockers": (research_cycle or {}).get("blockers"),
            "top25_count": (len((research_cycle or {}).get("top25") or [])
                            if (research_cycle or {}).get("top25") is not None else None),
            "top50_count": (len((research_cycle or {}).get("top50") or [])
                            if (research_cycle or {}).get("top50") is not None else None),
            "target_status": (research_cycle or {}).get("target_status"),
            "evidence_status": (research_cycle or {}).get("evidence_status"),
            # Release 29.3 — the DRC reports ORCHESTRATION semantics. Its
            # ``assessment_status`` is the LEGACY rank-membership comparison outcome and
            # is scoped as such; the canonical portfolio verdict is carried alongside it,
            # read verbatim from api.portfolio_reassessment / api.reallocation_proposal.
            "assessment_status": (research_cycle or {}).get("assessment_status"),
            "assessment_status_scope": "LEGACY_RANK_MEMBERSHIP_COMPARISON",
            "assessment_status_owner": "api.daily_action_gate",
            "portfolio_reassessment_state": reassessment_state,
            "portfolio_reassessment_owner": PRS_CANONICAL_OWNER,
            "reallocation_operator_state": reallocation_operator_state,
            "reallocation_proposal_owner": RP_CANONICAL_OWNER,
            "confirmation_required": _DRC_EXECUTE_TOKEN,
            "cycle_running": cycle_running,
            "cycle_blocked": cycle_blocked,
            "cycle_complete": cycle_complete,
            # --- Release 29.5 — the provenance contract, read from its owners --------- #
            # Two classes of downstream artifact are legitimate. Class 1 (LIVE_PRE_DRC_
            # SIGNAL) is produced by the Release 28 event refresh that Release 29
            # continuous collection triggers; it is real, current and displayable, and it
            # proves nothing about this cycle. Class 2 (GOVERNED_DRC_TERMINAL) is bound to
            # a run manifest. Only Class 2 satisfies the governed daily cycle.
            "governed_research_evidence_current": governed_research_evidence_current,
            "governed_evidence_owner": "api.daily_research_cycle",
            "governed_manifest_run_id": (research_cycle or {}).get("governed_manifest_run_id"),
            "opportunity_cost_artifact_class": hoc_artifact_class,
            "opportunity_cost_producer_owner": hoc_producer_owner,
            "opportunity_cost_claims_drc_terminal": hoc_claims_drc_terminal,
            "opportunity_cost_proves_drc_complete": False,
            "live_pre_drc_signal_present": live_pre_drc_signal_present,
            "provenance_note": (
                "A Holding Opportunity-Cost artifact can exist because the governed Daily "
                "Research Cycle produced it, or because continuous collection triggered an "
                "incremental signal refresh between cycles. Only a run manifest proves the "
                "governed cycle ran; an artifact never does."),
        },
        "portfolio_assessment_state": {
            "latest_assessment_date": latest_assessment_date,
            # Release 29.3 — the four legacy fields below describe the compatibility-only
            # rank-membership comparison owned by api.daily_action_gate. Before 29.3 they
            # published "PROPOSAL_READY" / "PORTFOLIO CHANGES PROPOSED — MANUAL REVIEW
            # REQUIRED" while the canonical proposal owner reported NOT_RUN. They are now
            # explicitly scoped, and the CANONICAL portfolio verdict sits beside them.
            "latest_assessment_result": latest_assessment_result,
            "latest_assessment_recommendation": latest_assessment_recommendation,
            "latest_assessment_scope": "LEGACY_RANK_MEMBERSHIP_COMPARISON",
            "latest_assessment_owner": "api.daily_action_gate",
            "latest_assessment_is_portfolio_proposal": False,
            "latest_assessment_current_for_eligible_session": ac["current_for_eligible_session"],
            "next_scheduled_review_date": next_review_date,
            "review_due": ac["review_due"],
            "review_overdue": ac["review_overdue"],
            "assessment_age_sessions": ac["assessment_age_sessions"],
            "assessment_status": assessment_status,
            "gate_target_state": gate_target_state,
            # --- the ONE canonical portfolio verdict (read verbatim from its owners) --- #
            "portfolio_reassessment_state": reassessment_state,
            "portfolio_reassessment_owner": PRS_CANONICAL_OWNER,
            "portfolio_proposal_required": reassessment_proposal_required,
            "reallocation_operator_state": reallocation_operator_state,
            "reallocation_proposal_owner": RP_CANONICAL_OWNER,
            "portfolio_decision_state": portfolio_decision_lane.get(
                "portfolio_decision_state"),
            "portfolio_decision_owner": "api.portfolio_decision",
            "portfolio_decision_approvable": bool(
                portfolio_decision_lane.get("approvable")),
        },
        "evidence_state": {
            "latest_snapshot_date": latest_snapshot_date,
            "required_snapshot_count": 1 if operational_close_valid else 0,
            "captured_snapshot_count": (forward_status or {}).get("snapshot_count"),
            "missing_snapshot_count": (0 if active_book_snapshot_present
                                       else (1 if operational_close_valid else 0)),
            "active_book_snapshot_present": active_book_snapshot_present,
            "evidence_status": ((forward_status or {}).get("evidence_state")
                                or (forward_status or {}).get("status")),
            "documented_gap": evidence_gap,
            "gap_severity": (SEV_ATTENTION if evidence_gap else SEV_INFO),
            "recovery_classification": (forward_status or {}).get("interpretation"),
        },
        "model_governance_state": model_governance,
        "model_review_state": model_review["model_review_state"],
        "model_review": model_review,
        # Stage 18 (Workstream C/D/E/K): the SEPARATE portfolio-decision review lane and
        # the backend-composed three-lane "Today" hero. These add to — and never mutate —
        # overall_state / primary_action: a completed operational close is never rendered
        # as "no action required" while a material reallocation proposal awaits review.
        "portfolio_decision_state": portfolio_decision_lane,
        "today_hero": today_hero,
        # Phase 29G.2: the PRIMARY portfolio-decision presentation is the Holding
        # Opportunity-Cost Review (assessment_presentation is an alias of it, retained so
        # the UI's canonical-node owner keeps painting the same DOM). The legacy rank-
        # membership comparison is compatibility-only and never a primary decision.
        "assessment_presentation": assessment_presentation,
        "holding_opportunity_cost_presentation": holding_opportunity_cost_presentation,
        "canonical_operator_state": canonical_operator_state,
        "canonical_operator_state_vocabulary": list(CANONICAL_OPERATOR_STATES),
        "canonical_decision_owner": HOC_CANONICAL_OWNER,
        # Slice 7 (Phase 29H) reallocation proposal — informational review state (a
        # SEPARATE vocabulary; never part of OVERALL_STATES; never an order/close gate).
        "reallocation_proposal_presentation": reallocation_proposal_presentation,
        "reallocation_operator_state": reallocation_operator_state,
        "reallocation_operator_state_vocabulary": list(REALLOCATION_OPERATOR_STATES),
        "reallocation_proposal_owner": RP_CANONICAL_OWNER,
        # Stage 20 — the ACTIVE PORTFOLIO ASSESSMENT lane. A SEPARATE vocabulary that is
        # never part of OVERALL_STATES; it contributes at most ONE routing action and is
        # fully suppressed (evidence only) while a Stage-19 execution is in flight.
        "portfolio_reassessment": reassessment_lane,
        "portfolio_reassessment_presentation": reassessment_presentation,
        "portfolio_reassessment_state": reassessment_state,
        "portfolio_reassessment_state_vocabulary": list(PRS_OPERATOR_STATES),
        "portfolio_reassessment_owner": PRS_CANONICAL_OWNER,
        "portfolio_reassessment_execution_precedence": reassessment_execution,
        "legacy_membership_comparison": legacy_membership_comparison,
        "evidence_presentation": evidence_presentation,
        "completed_summary": completed_summary,
        "canonical_portfolio_decision": canonical_portfolio_decision,
        "consistency_status": consistency_status,
        "consistency_violations": consistency_violations,
        "safety": {
            "read_only": True,
            "wrote_to_database": False,
            "wrote_to_ledger": False,
            "called_provider": False,
            "called_prediction": False,
            "ran_daily_close": False,
            "ran_research_refresh": False,
            "ran_portfolio_reassessment": False,
            "created_orders": False,
            "created_signals": False,
            "created_trade_decisions": False,
            "created_fills": False,
            "created_proposals": False,
            "promoted_model": False,
            "automatic_promotion_allowed": False,
            "paper_only": True,
            "automation_off": True,
            "manual_review": True,
            "safety_badges": list(SAFETY_BADGES),
        },
        "provenance": {
            "phase": PHASE,
            "workflow_state_owner": "api.workflow_state",
            "composed_read_models": [
                "data_freshness.load_data_freshness (engine.market_session)",
                "daily_action_gate.load_daily_action_gate",
                "daily_close.load_close_progress",
                "operational_book.load_operational_book",
                "forward_prediction_skill.load_prediction_skill",
                "alpha_target.load_readiness",
            ],
            "note": "Read-only composition of the combined operator interpretation; "
                    "domain facts stay with their owners and are never recomputed. No "
                    "provider/prediction call, no Daily Close, no research refresh, no "
                    "reassessment, no write. Slice 2 performs no workflow action.",
        },
    }


# --------------------------------------------------------------------------- #
# Lazy import seams (avoid an api.app import cycle; each is monkeypatchable).
# --------------------------------------------------------------------------- #
def _import_operational():
    from paper_trader.api import operational_book
    return operational_book


def _import_daily_close():
    from paper_trader.api import daily_close
    return daily_close


def _import_fps():
    from paper_trader.api import forward_prediction_skill
    return forward_prediction_skill


def _import_engine():
    from paper_trader.api import multi_horizon_engine
    return multi_horizon_engine


def _import_daily_refresh():
    from paper_trader.api import current_alpha_daily_refresh
    return current_alpha_daily_refresh


def _import_desk():
    from paper_trader.api import paper_trading_desk
    return paper_trading_desk


def _import_gate():
    from paper_trader.api import daily_action_gate
    return daily_action_gate


def _import_target():
    from paper_trader.api import alpha_target
    return alpha_target


def _import_drc():
    from paper_trader.api import daily_research_cycle
    return daily_research_cycle


def _import_portfolio_decision():
    from paper_trader.api import portfolio_decision
    return portfolio_decision


def _import_reassessment():
    from paper_trader.api import portfolio_reassessment
    return portfolio_reassessment


__all__ = [
    "PHASE",
    "WAITING_FOR_SESSION_CLOSE", "WAITING_FOR_OWNED_DATA", "RESEARCH_CYCLE_REQUIRED",
    "RESEARCH_CYCLE_RUNNING", "RESEARCH_CYCLE_BLOCKED",
    "PORTFOLIO_REASSESSMENT_REQUIRED", "READY_FOR_DAILY_CLOSE", "DAILY_CYCLE_COMPLETE",
    "DAILY_CYCLE_COMPLETE_EVIDENCE_GAP", "MANUAL_REVIEW_REQUIRED", "INCONSISTENT_STATE",
    "OVERALL_STATES",
    "ASSESS_CURRENT", "ASSESS_STALE", "ASSESS_DUE", "ASSESS_OVERDUE",
    "ASSESS_MISSING", "ASSESS_INCONSISTENT", "ASSESSMENT_STATUSES",
    "SEV_INFO", "SEV_SUCCESS", "SEV_ATTENTION", "SEV_BLOCKED", "SEV_ERROR",
    "ACTION_SEVERITIES",
    "CONSISTENT", "INCONSISTENT", "UNKNOWN", "CONSISTENCY_VOCAB",
    "VALID_DESTINATIONS",
    "classify_assessment", "build_assessment_presentation",
    "build_holding_opportunity_cost_presentation",
    "build_evidence_presentation", "load_workflow_state",
    # Release 46.2 — the canonical portfolio-attention rule and its invariant.
    "WORKFLOW_STATE_OWNER", "ATTENTION_MUTATION", "ATTENTION_REVIEW", "ATTENTION_NONE",
    "PORTFOLIO_ATTENTION_KINDS", "REVIEW_REASON_CONSTRAINT_BREACH",
    "REVIEW_REASON_PROPOSAL", "ACTION_REVIEW_PORTFOLIO_CONSTRAINT_BREACH",
    "CONSTRAINT_BREACH_CODES", "constraint_breaches_of", "portfolio_attention",
    "portfolio_attention_violations",
    # Stage 22 — canonical normal cycle, stale-evidence hierarchy, binding verdict.
    "NORMAL_CYCLE_OWNER", "DATA_GAP_TAXONOMY_OWNER", "NO_ACTION_TEXT",
    "EVIDENCE_SYSTEM_BLOCKER", "EVIDENCE_EXPECTED_STALE", "EVIDENCE_CURRENT",
    "EVIDENCE_CLASSES", "PRESENT_PRIMARY", "PRESENT_EVIDENCE", "PRESENT_HISTORY",
    "PRESENTATION_CLASSES", "build_evidence_classification",
    "BINDING_CURRENT", "BINDING_STALE", "BINDING_UNVERIFIABLE", "BINDING_ABSENT",
    "BINDING_STATES", "build_assessment_binding",
    "HOC_CANONICAL_OWNER", "HOC_PRIMARY_TITLE", "LEGACY_COMPARISON_TITLE",
    "COS_NOT_RUN", "COS_AVAILABLE", "COS_DEGRADED", "COS_BLOCKED",
    "COS_NO_ACTIVE_BOOK", "COS_UNAVAILABLE", "CANONICAL_OPERATOR_STATES",
    "MODEL_HEALTHY", "MODEL_RECALIBRATION_REVIEW", "MODEL_REVIEW_STATES",
    "ACTION_REVIEW_PROPOSED_PORTFOLIO",
    "_derive_model_review", "_build_today_hero",
]
