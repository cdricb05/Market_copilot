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
from paper_trader.engine import market_session as msession

PHASE = "29C-Slice2"

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

# Daily-close status vocabulary mirror (a FROZEN subset of api.daily_close's
# tested contract). Kept as literals so this module stays importable/pure without
# api.daily_close; the production path reads the real status via load_close_progress.
_CLOSE_COMPLETE_STATUSES = frozenset({
    "DAILY_CLOSE_COMPLETE_HOLD", "REBALANCE_PROPOSAL_READY",
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
    "PORTFOLIO_CHANGES_PROPOSED", "PROPOSAL", "REBALANCE_PROPOSAL_READY",
    "APPROVAL_REQUIRED", "PROPOSED"})


def _historical_result_text(outcome: Any, recommendation: Any) -> str:
    """Human, DATE-FREE statement of what the latest assessment concluded. The
    date is added by the presentation headline, never baked in here."""
    if outcome == _GATE_OUTCOME_NO_ACTION:
        return "No portfolio change was recommended."
    if outcome in _PROPOSAL_OUTCOMES:
        return "Portfolio changes were proposed."
    if recommendation:
        return str(recommendation)
    if outcome:
        return str(outcome).replace("_", " ").capitalize() + "."
    return "No portfolio assessment result is available."


def build_assessment_presentation(*, assessment_status: str, assessment_date: Any,
                                  outcome: Any, recommendation: Any,
                                  current_for_eligible: bool) -> dict[str, Any]:
    """Presentation for the latest portfolio assessment (Workstream B/D).

    The historical no-change result is preserved and DATED; its canonical currency
    is surfaced; "today" wording is permitted only when the assessment is current.
    """
    status = assessment_status
    is_current = (status == ASSESS_CURRENT)
    date_txt = _iso(_coerce_date(assessment_date)) or (
        str(assessment_date) if assessment_date else None)
    hist = _historical_result_text(outcome, recommendation)
    title = "LATEST PORTFOLIO ASSESSMENT"
    title_with_date = ("%s — %s" % (title, date_txt)) if date_txt else title

    if status == ASSESS_MISSING:
        headline = "No portfolio assessment has been recorded yet."
    elif date_txt:
        headline = hist.rstrip(".") + " on %s." % date_txt
    else:
        headline = hist

    if status == ASSESS_INCONSISTENT:
        explanation = ("The recorded assessment is dated after the latest eligible "
                       "completed session; reconcile the inconsistency before acting.")
        follow_up = "Inspect the state inconsistency."
    elif is_current:
        explanation = "This assessment is current for the latest completed session."
        follow_up = None
    else:
        explanation = "A new portfolio reassessment is required."
        follow_up = "A new portfolio reassessment is required."

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
# Deterministic priority policy (Workstream E). Returns the single overall state.
# First matching condition wins; the exact precedence is documented inline and in
# docs/ARCHITECTURE_DECISIONS.md (D-12).
# --------------------------------------------------------------------------- #
def _decide_overall(*, inconsistent: bool, session_status: str,
                    has_confirmed_eligible: bool, eligible_session_closed: bool,
                    owned_data_lag: bool, research_current: bool,
                    assessment_status: str, manual_review_required: bool,
                    evidence_gap: bool, cycle_running: bool = False,
                    cycle_blocked: bool = False) -> str:
    # P1 — an inconsistent authoritative state takes highest priority.
    if inconsistent or session_status == msession.INCONSISTENT_FUTURE_DATA \
            or assessment_status == ASSESS_INCONSISTENT:
        return INCONSISTENT_STATE

    # P2 — the current market session has not closed AND the latest eligible
    #      completed session is already fully processed → nothing to do but wait.
    if session_status == msession.BEFORE_SESSION_CLOSE and eligible_session_closed:
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

    # P4 — required research inputs are stale or missing → run the research cycle.
    if not research_current:
        return RESEARCH_CYCLE_REQUIRED

    # P5 — research current but the portfolio assessment is missing/due/overdue/
    #      stale → run a portfolio reassessment.
    if assessment_status in _ASSESS_NEEDS_ACTION:
        return PORTFOLIO_REASSESSMENT_REQUIRED

    # P6 — research and assessment current but the eligible session's close is
    #      not yet complete → run the Daily Close.
    if not eligible_session_closed:
        return READY_FOR_DAILY_CLOSE

    # Terminal region — the eligible session is fully processed. Refinement order
    # (documented precedence 7/8/9): a material-risk / manual-review gate is
    # surfaced ahead of a plain or evidence-gapped completion (P7 > P8 > P9).
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
        return {"action_code": ACTION_WAIT_FOR_OWNED_DATA,
                "label": "Wait for or refresh owned market data",
                "explanation": session_reason or (
                    "The expected completed session is not yet confirmed by owned "
                    "market data."),
                "severity": SEV_ATTENTION, "destination": DEST_COMMAND_CENTER,
                "safe_to_execute": True, "execution_available": False,
                "manual_confirmation_required": False, "slice3_pending": False,
                "current_task": "Wait for / refresh the owned market data.",
                "headline": "Waiting for owned market data confirmation."}

    if overall == RESEARCH_CYCLE_REQUIRED:
        return {"action_code": ACTION_RUN_RESEARCH_CYCLE,
                "label": "Run the Daily Research Cycle",
                "explanation": "Required research inputs are stale or missing for the "
                               "latest eligible session. The canonical Persistent Daily "
                               "Research Cycle (Slice 3) refreshes every required input "
                               "through its authoritative owner, scores the universe, "
                               "prepares the target and captures immutable evidence.",
                "severity": SEV_ATTENTION, "destination": DEST_DAILY_WORKFLOW,
                "safe_to_execute": False, "execution_available": True,
                "manual_confirmation_required": True, "slice3_pending": False,
                "confirmation_required": _DRC_EXECUTE_TOKEN,
                "current_task": "Run the Daily Research Cycle to refresh the stale inputs.",
                "headline": "Research inputs are stale — run the Daily Research Cycle."}

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
                    cycle_active: bool = False) -> list[dict]:
    q: list[dict[str, Any]] = []

    def add(action_code, label, severity, destination, reason, *,
            execution_available=False, safe_to_execute=True, slice3_pending=False):
        if action_code == primary_code:
            return
        q.append({"action_code": action_code, "label": label, "severity": severity,
                  "destination": destination, "reason": reason,
                  "execution_available": execution_available,
                  "safe_to_execute": safe_to_execute, "slice3_pending": slice3_pending})

    if not research_current and not cycle_active:
        add(ACTION_RUN_RESEARCH_CYCLE, "Run the Daily Research Cycle",
            SEV_ATTENTION, DEST_DAILY_WORKFLOW,
            "Required research inputs are stale or missing.",
            execution_available=True, safe_to_execute=False)
    if assessment_status in _ASSESS_NEEDS_ACTION:
        # Phase 29G.1: the reassessment is produced by the Daily Research Cycle (the sole
        # execution path), not a separate obsolete placeholder control.
        add(ACTION_RUN_PORTFOLIO_REASSESSMENT,
            "Reassess the portfolio by running the Daily Research Cycle",
            SEV_ATTENTION, DEST_DAILY_WORKFLOW,
            "The portfolio Holding Opportunity-Cost assessment is %s for the latest "
            "eligible session; it is produced by the Daily Research Cycle (the sole "
            "execution path), not a separate reassessment control."
            % assessment_status.lower(), slice3_pending=False)
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
    close_final_status = (close_progress or {}).get("final_close_status")
    close_status = close_final_status or (close_progress or {}).get("status")
    close_done = bool((close_progress or {}).get("done"))
    operational_close_valid = bool(
        close_done and close_final_status in _CLOSE_COMPLETE_STATUSES)
    latest_close_date = close_market_date if operational_close_valid else close_market_date
    close_failed = bool(close_final_status in _CLOSE_FAILED_STATUSES)

    elig_d = _coerce_date(eligible_date)
    close_d = _coerce_date(latest_close_date)
    has_confirmed_eligible = elig_d is not None
    eligible_session_closed = bool(
        operational_close_valid and elig_d is not None and close_d is not None
        and close_d >= elig_d)

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

    # --- Assessment currency (Workstream F). ----------------------------------- #
    latest_assessment_date = (gate or {}).get("latest_completed_market_date")
    latest_assessment_result = (gate or {}).get("outcome")
    latest_assessment_recommendation = ((gate or {}).get("headline")
                                        or (gate or {}).get("explanation"))
    next_review_date = (gate or {}).get("next_scheduled_full_review")
    gate_review_due = bool((gate or {}).get("scheduled_review_due"))
    gate_target_state = (gate or {}).get("target_state")

    ac = classify_assessment(assessment_date=latest_assessment_date,
                             eligible_date=eligible_date,
                             current_reference_date=current_reference_date,
                             next_review_date=next_review_date,
                             review_due=gate_review_due)
    assessment_status = ac["status"]

    manual_review_required = bool(gate_target_state == _GATE_APPROVAL_REQUIRED)

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
    overall = _decide_overall(
        inconsistent=inconsistent_inputs, session_status=session_status,
        has_confirmed_eligible=has_confirmed_eligible,
        eligible_session_closed=eligible_session_closed,
        owned_data_lag=owned_data_lag, research_current=research_current,
        assessment_status=assessment_status,
        manual_review_required=manual_review_required, evidence_gap=evidence_gap,
        cycle_running=cycle_running, cycle_blocked=cycle_blocked)

    primary = _primary_action(overall, {
        "eligible_date": eligible_date,
        "session_operator_action": session.get("operator_action")})
    primary_code = primary["action_code"]

    queued = _queued_actions(
        primary_code=primary_code, research_current=research_current,
        assessment_status=assessment_status,
        eligible_session_closed=eligible_session_closed,
        has_confirmed_eligible=has_confirmed_eligible, evidence_gap=evidence_gap,
        manual_review_required=manual_review_required, pending_orders=pending_orders,
        cycle_active=(cycle_running or cycle_blocked))

    # --- Blockers + warnings (Workstream C.12/13). ----------------------------- #
    blockers: list[dict[str, Any]] = []
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
            "result": latest_assessment_result,
            "recommendation": latest_assessment_recommendation,
            "preserved_statement": preserved_statement,
            "current_for_eligible_session": ac["current_for_eligible_session"]},
        "latest_research_refresh": {"market_date": _row(freshness, "price_score_refresh")},
        "latest_evidence_snapshot": {"market_date": latest_snapshot_date},
    }

    # --- Canonical presentation contract (Workstream B/D/F). ------------------- #
    # Backend-generated operator text so the UI renders (never derives) the primary
    # interpretation: an older no-change assessment is a DATED historical result
    # with its currency; the still-open session is kept distinct from the completed
    # close's documented (attention) forward-evidence gap.
    assessment_presentation = build_assessment_presentation(
        assessment_status=assessment_status, assessment_date=latest_assessment_date,
        outcome=latest_assessment_result, recommendation=latest_assessment_recommendation,
        current_for_eligible=ac["current_for_eligible_session"])
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

    # --- Cross-surface consistency (Workstream K). ----------------------------- #
    consistency_status, consistency_violations = _build_consistency(
        freshness=freshness, gate=gate, close_progress=close_progress,
        operational=operational, forward_status=forward_status,
        target_readiness=target_readiness, assessment_status=assessment_status,
        latest_assessment_date=latest_assessment_date, eligible_date=eligible_date,
        latest_close_date=latest_close_date, overall=overall,
        primary_code=primary_code, pending_orders=pending_orders)
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
            "destination": primary["destination"],
            "safe_to_execute": primary["safe_to_execute"],
            "execution_available": primary["execution_available"],
            "manual_confirmation_required": primary["manual_confirmation_required"],
            "slice3_pending": primary.get("slice3_pending", False),
            "confirmation_required": primary.get("confirmation_required")},
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
            "assessment_status": (research_cycle or {}).get("assessment_status"),
            "confirmation_required": _DRC_EXECUTE_TOKEN,
            "cycle_running": cycle_running,
            "cycle_blocked": cycle_blocked,
            "cycle_complete": cycle_complete,
        },
        "portfolio_assessment_state": {
            "latest_assessment_date": latest_assessment_date,
            "latest_assessment_result": latest_assessment_result,
            "latest_assessment_recommendation": latest_assessment_recommendation,
            "latest_assessment_current_for_eligible_session": ac["current_for_eligible_session"],
            "next_scheduled_review_date": next_review_date,
            "review_due": ac["review_due"],
            "review_overdue": ac["review_overdue"],
            "assessment_age_sessions": ac["assessment_age_sessions"],
            "assessment_status": assessment_status,
            "gate_target_state": gate_target_state,
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
        "assessment_presentation": assessment_presentation,
        "evidence_presentation": evidence_presentation,
        "completed_summary": completed_summary,
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
    "build_evidence_presentation", "load_workflow_state",
]
