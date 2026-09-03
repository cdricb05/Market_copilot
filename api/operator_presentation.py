"""Release 49 — the ONE reconciled operator presentation.

The normal operator UI used to render many raw subsystem states independently
(``MANUAL_REVIEW_REQUIRED`` beside ``PORTFOLIO CONSTRAINT BREACH`` beside
``STATE NOT_RUN`` beside ``NO PROPOSAL YET - RUN THE DAILY RESEARCH CYCLE``
beside ``REBALANCE_NO_PROPOSAL``), each true of its own owner and none of them an
operator answer. This module is the ONE read-only place where those authoritative
states are RECONCILED into one operator truth:

    WHAT IS HAPPENING?      -> system_readiness
    WHAT SHOULD WE OWN?     -> portfolio_decision + decision_summary
    WHAT DO I NEED TO DO?   -> next_action (at most ONE)

It is a PRESENTATION owner and nothing else. It CONSUMES the canonical owners —
``api.workflow_state`` (overall state, operator command, canonical portfolio
decision, portfolio-decision lane, reassessment lane, operational state),
``api.reallocation_proposal.load_constrained_reallocation`` (Release-47 outcome,
best feasible target, switching economics, approval + execution state),
``api.daily_close`` (the P&L block), ``api.material_information`` (what arrived),
``api.portfolio_decision_outcome`` (forward evidence of executed decisions) and
``api.information_collection`` (collection lifecycle) — and it RECOMPUTES NONE of
them. No NAV, no target, no portfolio decision, no constraint, no HOC, no proposal,
no execution state and no research verdict is derived here: every number and every
verdict is read verbatim from the owner that decided it, and this module only
chooses which of the already-decided facts the operator sees first and in what
words.

Two hard rules the reconciliation obeys:

* A HISTORICAL session is never rerun, backfilled or rewritten. When the latest
  completed session was decided under the prior workflow (a per-holding cap breach
  recorded as a manual-review blocker, no governed Release-47 target for the
  session), the presentation says so — ``historical_context.historical`` — and
  names the next ELIGIBLE action (the next portfolio cycle after the next market
  close). It never fabricates a target and never tells the operator to rerun an
  immutable session.
* A collection-infrastructure problem is stated as DEGRADED unless the owners say
  it blocks the portfolio decision. A red infrastructure chip is not a portfolio
  blocker.

Read-only. No write, no provider call, no prediction call, no order, no approval.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Callable, Optional

PHASE = "R49"
OWNER = "api.operator_presentation"
SCHEMA_VERSION = "operator_presentation.v1"
ROUTE = "/v1/operations/operator-presentation"

# --------------------------------------------------------------------------- #
# Vocabularies (frozen; every consumer renders these words and nothing else).
# --------------------------------------------------------------------------- #
SYSTEM_READY = "READY"
SYSTEM_DEGRADED = "DEGRADED"
SYSTEM_BLOCKED = "BLOCKED"
SYSTEM_READINESS_VOCABULARY = (SYSTEM_READY, SYSTEM_DEGRADED, SYSTEM_BLOCKED)

PD_CYCLE_REQUIRED = "CYCLE_REQUIRED"
PD_REALLOCATE = "REALLOCATE"
PD_HOLD = "HOLD"
PD_BLOCKED = "BLOCKED"
PD_AWAITING_APPROVAL = "AWAITING_APPROVAL"
PD_AWAITING_CONFIRMATION = "AWAITING_CONFIRMATION"
PD_AWAITING_NEXT_CLOSE = "AWAITING_NEXT_CLOSE"
PD_OUTCOME_ACCRUING = "OUTCOME_ACCRUING"
PORTFOLIO_DECISION_VOCABULARY = (
    PD_CYCLE_REQUIRED, PD_REALLOCATE, PD_HOLD, PD_BLOCKED, PD_AWAITING_APPROVAL,
    PD_AWAITING_CONFIRMATION, PD_AWAITING_NEXT_CLOSE, PD_OUTCOME_ACCRUING)
PORTFOLIO_DECISION_LABELS = {
    PD_CYCLE_REQUIRED: "Cycle required",
    PD_REALLOCATE: "Reallocate",
    PD_HOLD: "Hold",
    PD_BLOCKED: "Blocked",
    PD_AWAITING_APPROVAL: "Awaiting approval",
    PD_AWAITING_CONFIRMATION: "Awaiting confirmation",
    PD_AWAITING_NEXT_CLOSE: "Awaiting next close",
    PD_OUTCOME_ACCRUING: "Outcome accruing",
}

NA_PORTFOLIO_CYCLE = "PORTFOLIO_CYCLE"        # executes through the ONE dispatcher
NA_REVIEW_REALLOCATION = "REVIEW_REALLOCATION"
NA_REVIEW_ORDER_PLAN = "REVIEW_ORDER_PLAN"
NA_REVIEW_BLOCKER = "REVIEW_BLOCKER"
NA_WAIT = "WAIT"
NA_NONE = "NONE"
NEXT_ACTION_VOCABULARY = (NA_PORTFOLIO_CYCLE, NA_REVIEW_REALLOCATION,
                          NA_REVIEW_ORDER_PLAN, NA_REVIEW_BLOCKER, NA_WAIT, NA_NONE)
NEXT_ACTION_LABELS = {
    NA_PORTFOLIO_CYCLE: "Run portfolio cycle",
    NA_REVIEW_REALLOCATION: "Review reallocation",
    NA_REVIEW_ORDER_PLAN: "Review order plan",
    NA_REVIEW_BLOCKER: "Review blocker",
}
#: The only next-action kind that EXECUTES anything, and it executes through the
#: Release-48 orchestration owner's own contract, supplied verbatim by the workflow
#: owner. Every other kind is navigation.
EXECUTING_NEXT_ACTION_KINDS = frozenset({NA_PORTFOLIO_CYCLE})

GOV_REVIEW = "REVIEW"
GOV_APPROVE = "APPROVE"
GOV_CONFIRM = "CONFIRM"
GOV_AWAIT_NEXT_CLOSE = "AWAIT_NEXT_CLOSE"
GOV_EXECUTED = "EXECUTED"
GOV_OUTCOME_ACCRUING = "OUTCOME_ACCRUING"
GOVERNANCE_SEQUENCE = (GOV_REVIEW, GOV_APPROVE, GOV_CONFIRM, GOV_AWAIT_NEXT_CLOSE,
                       GOV_EXECUTED, GOV_OUTCOME_ACCRUING)
GOVERNANCE_LABELS = {
    GOV_REVIEW: "Review", GOV_APPROVE: "Approve", GOV_CONFIRM: "Confirm order plan",
    GOV_AWAIT_NEXT_CLOSE: "Await next close", GOV_EXECUTED: "Executed",
    GOV_OUTCOME_ACCRUING: "Outcome accruing",
}
GOVERNANCE_OWNERS = {
    GOV_REVIEW: "api.reallocation_proposal",
    GOV_APPROVE: "api.portfolio_decision",
    GOV_CONFIRM: "api.rebalance_execution",
    GOV_AWAIT_NEXT_CLOSE: "api.daily_close (NEXT_CLOSE settlement through the desk owner)",
    GOV_EXECUTED: "api.rebalance_execution",
    GOV_OUTCOME_ACCRUING: "api.portfolio_decision_outcome",
}
STEP_DONE, STEP_CURRENT, STEP_UPCOMING = "DONE", "CURRENT", "UPCOMING"

SAFETY_MODE_LINE = "PAPER · MANUAL APPROVAL · AUTOMATION OFF"

#: Owner state strings this module READS (never writes, never re-derives). They are
#: named here once so a renamed owner constant fails loudly in the tests.
_CPD_NOT_RUN = "NOT_RUN"
_CPD_NO_CHANGE = "NO_CHANGE"
_CPD_WITHHELD = "CHANGE_CANDIDATE_WITHHELD"
_CPD_HOLD = "HOLD_CURRENT_BOOK"
_CPD_REVIEW = "PROPOSAL_REVIEW_REQUIRED"
_CPD_RECORDED = "DECISION_RECORDED"
_CPD_BLOCKED = "BLOCKED"
_PDS_APPROVED = "PROPOSAL_APPROVED"
_PDS_REJECTED = "PROPOSAL_REJECTED"
_PDS_HELD = "PROPOSAL_HELD"
_PDS_STALE = "STALE_PROPOSAL_REVIEW_REQUIRED"
_PDS_HOLD = "HOLD_CURRENT_BOOK"
#: R54.2.3.2 — a newer authoritative decision superseded the proposal (history only).
_PDS_SUPERSEDED = "PROPOSAL_SUPERSEDED_BY_NEWER_DECISION"
_RB_PLAN_REVIEW_REQUIRED = "PROPOSAL_APPROVED_ORDER_PLAN_REVIEW_REQUIRED"
_RB_PLAN_CONFIRMED = "ORDER_PLAN_CONFIRMED_PAPER_EXECUTION_PENDING"
_RB_EXECUTED = "PAPER_EXECUTED_RECONCILED"
_RB_BLOCKED = frozenset({"ORDER_PLAN_BLOCKED_MISSING_OWNED_MARKS",
                         "ORDER_PLAN_BLOCKED_INCOMPLETE_TARGET"})
_RO_PROPOSAL_READY = "PROPOSAL_READY"
_RO_HOLD = "HOLD_CURRENT_BOOK"
_RO_TRUE_BLOCKER = "TRUE_BLOCKER"
_WF_INCONSISTENT = "INCONSISTENT_STATE"
_WF_WAITING_CLOSE = "WAITING_FOR_SESSION_CLOSE"
_WF_CYCLE_RUNNING = "RESEARCH_CYCLE_RUNNING"
_WF_CYCLE_BLOCKED = "RESEARCH_CYCLE_BLOCKED"
_COLLECTION_RUNNING = "RUNNING"

SOURCE_OWNERS = {
    "workflow_state": "api.workflow_state",
    "constrained_reallocation": "api.reallocation_proposal",
    "daily_close": "api.daily_close",
    "material_information": "api.material_information",
    "decision_outcomes": "api.portfolio_decision_outcome",
    "information_collection": "api.information_collection",
    # Release 50 - the ONE capital pool (allocation by asset class, verbatim).
    "capital_pool": "api.capital_pool",
}


def _now_iso(now: Optional[datetime] = None) -> str:
    return (now or datetime.now(timezone.utc)).isoformat()


def _d(x: Any) -> dict:
    return x if isinstance(x, dict) else {}


def _l(x: Any) -> list:
    return list(x) if isinstance(x, (list, tuple)) else []


def _num(x: Any) -> Optional[float]:
    try:
        if x is None or isinstance(x, bool):
            return None
        v = float(x)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def _int(x: Any) -> int:
    v = _num(x)
    return int(v) if v is not None else 0


def _count_actions(action_counts: dict, allocations: list) -> dict:
    """The owner's own action tally when it published one; otherwise the plain
    count of the owner's allocation rows by their published action label. A count
    of published rows is presentation, not a portfolio decision."""
    counts = {k: _int(v) for k, v in _d(action_counts).items()}
    if not counts and allocations:
        for row in allocations:
            act = str(_d(row).get("action") or "")
            if act:
                counts[act] = counts.get(act, 0) + 1
    return counts


# --------------------------------------------------------------------------- #
# System readiness — READY / DEGRADED / BLOCKED, with every degraded item saying
# whether it blocks the portfolio decision (the owners decide; this reads).
# --------------------------------------------------------------------------- #
def _blocker_text(b: Any, *, short: bool = False) -> str:
    """One human sentence for a workflow blocker row — never a dict repr.

    Release 54.2.2. The readiness summary is read by a person; a Python literal is
    not a reason. Prefers the owner's own ``detail`` sentence, names the source it
    applies to, and falls back to the code alone rather than to ``str(dict)``.
    ``short`` drops the detail, for the one-line summary that joins three of these.
    """
    row = _d(b) if isinstance(b, dict) else None
    if row is None:
        return str(b)
    code = str(row.get("code") or "").strip()
    src = str(row.get("source_id") or "").strip()
    detail = str(row.get("detail") or "").strip()
    head = ("%s (%s)" % (code, src)) if (code and src) else (code or src)
    if short:
        return head or (detail.split(".")[0] if detail else "unspecified blocker")
    if head and detail:
        return "%s — %s" % (head, detail)
    return head or detail or "unspecified blocker"


def _reason_line(reasons: list, *, limit: int = 3, each: int = 90) -> str:
    """The one-line readiness summary: the first few reasons, each trimmed to its
    leading clause. The full sentences stay in ``blocking_reasons`` /
    ``degraded_reasons``, which is where a reader goes for the whole story — a
    summary that carries three paragraphs is not a summary (Release 54.2.2)."""
    out = []
    for r in list(reasons)[:limit]:
        s = str(r).strip()
        head = s.split(" — ")[0].strip() if " — " in s else s
        out.append(head if len(head) <= each else (head[:each - 1].rstrip() + "…"))
    return "; ".join(out)


def _system_readiness(wf: dict, collection: Optional[dict],
                      recovery: Optional[dict] = None,
                      obligation: Optional[dict] = None) -> dict:
    os_ = _d(wf.get("operational_state"))
    ev = _d(wf.get("evidence_state"))
    evc = _d(wf.get("evidence_classification"))
    gaps = _d(wf.get("data_gap_taxonomy"))
    mrev = _d(wf.get("model_review"))
    overall = wf.get("overall_state")
    blockers = _l(wf.get("blockers"))
    items: list[dict] = []
    blocking: list[str] = []
    degraded: list[str] = []

    def item(key, label, value, state, detail=None, blocks=False):
        items.append({"key": key, "label": label, "value": value, "state": state,
                      "detail": detail, "blocks_portfolio_decision": bool(blocks)})

    workflow_available = bool(wf.get("status") == "OK" and overall)
    item("service", "Service", "ready" if workflow_available else "unavailable",
         "ok" if workflow_available else "blocked",
         None if workflow_available else "The canonical workflow state did not load.",
         blocks=not workflow_available)
    if not workflow_available:
        blocking.append("workflow state unavailable")

    rec = _d(recovery)
    governed_session = os_.get("eligible_market_date") or wf.get("eligible_market_date")
    # Release 54.2.1 (Phase J.4) — FRESHNESS IS ALWAYS RELATIVE TO A SESSION. "Fresh"
    # on its own was read as "the system is current", which is a different claim: the
    # data can be complete for the governed 2026-08-31 session while 2026-09-01 has
    # never been closed. The session is therefore named in the value, and the
    # outstanding session is a row of its own rather than an absence the operator has
    # to notice. No freshness CALCULATION changes here — only what it is called.
    item("eligible_session", "Eligible session (governed)", governed_session, "ok",
         ("Fresh for the governed session — %s. A newer completed session may still "
          "be outstanding; see Session recovery." % governed_session)
         if rec.get("active") else
         ("Fresh for the governed session — %s." % governed_session
          if governed_session else None))
    close_valid = bool(os_.get("operational_close_valid"))
    item("operational_mark", "Operational mark", os_.get("valuation_date"),
         "ok" if close_valid else "degraded",
         None if close_valid else "The latest operational close is not marked valid.")
    if not close_valid and workflow_available:
        degraded.append("operational close not valid")
    if rec.get("available"):
        _r_state = str(rec.get("state") or "")
        item("session_recovery", "Session recovery",
             (rec.get("recovery_session_display") or rec.get("recovery_session")
              or "not required") if rec.get("active") else "not required",
             ("blocked" if _r_state in ("CATCH_UP_BLOCKED",
                                        "CATCH_UP_WAITING_FOR_OWNED_DATA")
              else "degraded" if rec.get("active") else "ok"),
             rec.get("summary"),
             # A missed session is WORK, not an incident: it never blocks the
             # portfolio decision surface, it names the one action that clears it.
             blocks=False)
        if _r_state == "CATCH_UP_REQUIRED":
            degraded.append("a completed session (%s) has not been closed"
                            % rec.get("recovery_session"))
        elif _r_state in ("CATCH_UP_WAITING_FOR_OWNED_DATA", "CATCH_UP_BLOCKED"):
            degraded.append("session recovery %s (%s)"
                            % (_r_state.lower().replace("_", " "),
                               rec.get("recovery_session")))
    # Release 54.2.2 — the GOVERNED RESEARCH clock, as its own row. The operational
    # close and the governed research cycle are different questions about the same
    # session, and the readiness list previously answered only the first. Like the
    # recovery row, outstanding governed research is WORK, not an incident: it never
    # blocks the portfolio-decision surface and it never invalidates the book.
    ob = _d(obligation)
    if ob.get("available"):
        # ``_governed_research`` publishes the workflow owner's obligation under
        # ``state``; the raw workflow block spells it ``research_obligation_state``.
        # Both are accepted so this row is correct whichever is handed in.
        _o_state = str(ob.get("state") or ob.get("research_obligation_state") or "")
        _o_session = ob.get("outstanding_research_session")
        item("governed_research", "Governed research",
             (_o_session or "outstanding") if _o_session else "current",
             ("blocked" if _o_state == "RESEARCH_OBLIGATION_BLOCKED"
              else "degraded" if _o_state in ("RESEARCH_OBLIGATION_OUTSTANDING",
                                              "RESEARCH_OBLIGATION_EVIDENCE_GAP")
              else "ok"),
             ob.get("summary"), blocks=False)
        if _o_state == "RESEARCH_OBLIGATION_OUTSTANDING":
            degraded.append("governed research for %s has not run" % _o_session)
        elif _o_state == "RESEARCH_OBLIGATION_BLOCKED":
            degraded.append("governed research for %s is blocked by a named input"
                            % _o_session)
        elif _o_state == "RESEARCH_OBLIGATION_EVIDENCE_GAP":
            degraded.append("governed research for %s is a documented gap" % _o_session)
    item("nav", "NAV", _num(os_.get("nav")), "ok")

    if overall == _WF_INCONSISTENT:
        blocking.append("authoritative surfaces disagree")
    # Release 54.2.2 — READ THE OWNER'S SEVERITY; DO NOT INVENT ONE.
    #
    # This loop used to be ``blocking.append(str(b))`` over every workflow blocker.
    # Two consequences, both visible to the operator on 2026-09-02: the whole service
    # was declared BLOCKED because two RESEARCH inputs were stale — beside warnings
    # from the same payload stating in plain English that the completed close remained
    # valid — and the reason was rendered as a Python dict repr,
    # "{'code': 'RESEARCH_INPUT_STALE', 'source_id': 'momentum_monthly'}".
    #
    # The workflow owner now states each blocker's severity, scope and whether it
    # blocks the portfolio decision. A row that does not block it is ATTENTION on its
    # own lane, never a red service-wide banner, and every row is rendered as its code
    # plus the owner's sentence. Degrade-safe: a row with no severity (an older
    # payload) keeps the previous blocking behaviour.
    for b in blockers:
        row = _d(b) if isinstance(b, dict) else {}
        sev = row.get("severity")
        non_blocking = bool(row) and (row.get("blocks_portfolio_decision") is False
                                      and sev in (None, "ATTENTION", "INFO"))
        (degraded if non_blocking else blocking).append(_blocker_text(b))
    if bool(gaps.get("has_blocking_gap")):
        blocking.append("blocking data gap: %s"
                        % ", ".join(str(t) for t in _l(gaps.get("affected_tickers"))))
    if bool(evc.get("is_operational_incident")) or bool(evc.get("blocks_portfolio_action")):
        blocking.append("operational evidence incident")

    # Collection — decided by api.information_collection; presented as DEGRADED and
    # explicitly NON-BLOCKING unless the workflow owner itself named it a blocker.
    if collection is not None:
        svc = _d(collection.get("service"))
        rec = _d(collection.get("recovery"))
        hd = _d(collection.get("headline"))
        svc_state = svc.get("service_state")
        running = svc_state == _COLLECTION_RUNNING
        item("collection", "Collection",
             "running" if running else str(svc_state or "unknown").lower(),
             "ok" if running else "degraded",
             None if running else (
                 (rec.get("why") or hd.get("detail") or "Collection is not running.")
                 + " The portfolio decision is unaffected; recovery is an explicit "
                   "operator action under System · Audit."),
             blocks=False)
        if not running:
            degraded.append("collection %s" % str(svc_state or "unknown").lower())
    else:
        item("collection", "Collection", "not checked", "degraded",
             "The collection state did not load.", blocks=False)
        degraded.append("collection state unavailable")

    # Research / evidence health — a documented gap is attention, never a blocker.
    gap = bool(ev.get("documented_gap"))
    review_due = bool(mrev.get("model_review_required"))
    if gap:
        r_state, r_value, r_detail = "degraded", "evidence gap", ev.get("recovery_classification")
        degraded.append("forward-evidence gap documented")
    elif review_due:
        r_state, r_value, r_detail = "degraded", "model review due", \
            "A model-recalibration review threshold has been met (review only)."
        degraded.append("model review due")
    else:
        r_state, r_value, r_detail = "ok", "healthy", None
    item("research", "Research", r_value, r_state, r_detail, blocks=False)

    if blocking:
        state = SYSTEM_BLOCKED
    elif degraded:
        state = SYSTEM_DEGRADED
    else:
        state = SYSTEM_READY
    if state == SYSTEM_BLOCKED:
        summary = "Blocked — " + _reason_line(blocking)
    elif state == SYSTEM_DEGRADED:
        # Release 54.2.2 — a DEGRADED service whose operational book is valid says so.
        # "The portfolio decision remains valid" was the right sentence for a stale
        # collection lane and the wrong one for an incomplete governed-research lane,
        # where the honest claim is about the BOOK, not the decision.
        summary = ("Degraded — " + _reason_line(degraded)
                   + (". The operational book remains valid."
                      if bool(os_.get("operational_close_valid"))
                      else ". The portfolio decision remains valid."))
    else:
        summary = "Ready"
    return {
        "state": state,
        "state_vocabulary": list(SYSTEM_READINESS_VOCABULARY),
        "summary": summary,
        "items": items,
        "blocking_reasons": blocking,
        "degraded_reasons": degraded,
        "degraded_blocks_portfolio_decision": False if not blocking else True,
        "portfolio_decision_remains_valid": not blocking,
        # Release 54.2.2 — stated separately, because they are separate questions and
        # the operator was reading one as the other. Both come from their owners.
        "operational_book_valid": bool(os_.get("operational_close_valid")),
        "governed_research_current": bool(ob.get("governed_research_current"))
        if ob.get("available") else None,
        "severity_owner": wf.get("blocker_severity_owner") or "api.workflow_state",
    }


# --------------------------------------------------------------------------- #
# Release 54.2.1 — MISSED-SESSION RECOVERY, presented.
#
# The obligation itself is decided by ``api.workflow_state`` and read verbatim. What
# this owner adds is the one fact the workflow owner deliberately cannot have: it is
# PROBE-FREE, so it can only say what the persisted owned marks confirm. The Daily
# Close owner DOES probe the owned provider, and its payload already travels into this
# module, so the provider's own answer is presented BESIDE the obligation instead of
# the operator having to guess whether the missed session can actually be closed.
#
# Nothing is recomputed here: the obligation comes from one owner, the provider
# readiness from the other, and a disagreement is shown rather than resolved.
# --------------------------------------------------------------------------- #
_RECOVERY_HEADLINES = {
    "CATCH_UP_REQUIRED": "CATCH UP REQUIRED",
    "CATCH_UP_WAITING_FOR_OWNED_DATA": "CATCH UP WAITING FOR OWNED DATA",
    "CATCH_UP_BLOCKED": "CATCH UP BLOCKED",
}


def _month_day(iso: Any) -> Optional[str]:
    """"2026-09-01" -> "Sep 1, 2026" (backend-owned; no client date arithmetic)."""
    try:
        d = date.fromisoformat(str(iso)[:10])
    except (TypeError, ValueError):
        return None
    return "%s %d, %d" % (_MONTHS[d.month - 1], d.day, d.year)


_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def _session_recovery(wf: dict, daily_close: dict) -> dict:
    """The operator-facing catch-up panel: the workflow owner's obligation plus the
    close owner's provider answer. Pure projection of two owners' payloads."""
    rec = _d(wf.get("session_recovery"))
    state = rec.get("recovery_state")
    session = rec.get("recovery_session")
    provider = _d(daily_close.get("provider_readiness")) \
        or _d(rec.get("owned_provider_coverage"))
    prov_latest = provider.get("provider_latest_date")
    prov_ready = provider.get("ready")
    # Release 54.2.3.1 — the workflow owner now composes the close owner's provider
    # answer into its OWN recovery verdict, so this panel reads that published
    # verdict verbatim. The inline date comparison survives ONLY as a degrade
    # fallback for a payload predating the reconciliation; it can no longer
    # contradict the headline beside it, because the same verdict decided both.
    provider_covers_session = rec.get("provider_covers_recovery_session")
    if provider_covers_session is None:
        # The close owner probed for ITS expected session; its answer only settles
        # OUR session when the published date actually reaches it. Never inferred.
        provider_covers_session = bool(
            session and prov_latest and str(prov_latest)[:10] >= str(session)[:10])
    provider_covers_session = bool(provider_covers_session)
    if provider_covers_session:
        owned_line, owned_state = "READY", "ok"
    elif (prov_latest and session
          and str(prov_latest)[:10] >= str(session)[:10]):
        # The provider HAS published the session but the close owner's coverage
        # verdict is still negative (an incomplete valuation / decision scope) —
        # saying "not published" here would misname the cause.
        owned_line, owned_state = ("PUBLISHED BUT NOT COVERABLE (market-data "
                                   "scope incomplete)"), "blocked"
    elif prov_latest:
        owned_line, owned_state = ("NOT PUBLISHED (owned provider is current "
                                   "through %s)" % prov_latest), "blocked"
    elif rec.get("recovery_data_state") == "CONFIRMED":
        owned_line, owned_state = "READY (already in the owned desk marks)", "ok"
    else:
        owned_line, owned_state = ("UNVERIFIED — the Daily Close revalidates the "
                                   "owned provider and writes nothing if the session "
                                   "is unpublished"), "degraded"
    active = bool(rec.get("catch_up_required")) or state == "CATCH_UP_BLOCKED"
    return {
        "available": bool(rec),
        "active": active,
        "state": state,
        "state_vocabulary": _l(rec.get("recovery_state_vocabulary")),
        "headline": _RECOVERY_HEADLINES.get(str(state)) if active else None,
        "recovery_session": session,
        "recovery_session_display": _month_day(session),
        "detail": (("%s was not closed." % (_month_day(session) or session))
                   if active and session else None),
        "missed_completed_sessions": _l(rec.get("missed_completed_sessions")),
        "missed_completed_session_count": rec.get("missed_completed_session_count"),
        "last_closed_session": rec.get("last_closed_session"),
        "owned_data_line": owned_line if active else None,
        "owned_data_state": owned_state if active else None,
        "owned_provider_latest_date": prov_latest,
        "owned_provider_ready_for_its_expected_session": prov_ready,
        "provider_covers_recovery_session": provider_covers_session,
        "blockers": _l(rec.get("recovery_blockers")),
        "summary": rec.get("summary"),
        # The action is the SAME canonical portfolio cycle — never a backfill or
        # force-close control, and the operator never supplies a date.
        "next_action_kind": (NA_PORTFOLIO_CYCLE if state == "CATCH_UP_REQUIRED"
                             else NA_REVIEW_BLOCKER if state == "CATCH_UP_BLOCKED"
                             else NA_WAIT if state == "CATCH_UP_WAITING_FOR_OWNED_DATA"
                             else NA_NONE),
        "next_action_label": ("Run the Portfolio Cycle"
                              if state == "CATCH_UP_REQUIRED" else
                              "No action is currently safe"
                              if state in ("CATCH_UP_WAITING_FOR_OWNED_DATA",
                                           "CATCH_UP_BLOCKED") else None),
        "backfill_control_offered": False,
        "force_close_control_offered": False,
        "operator_supplies_no_date": True,
        "owners": {"obligation": "api.workflow_state",
                   "calendar": "engine.market_session",
                   "provider_readiness": "api.daily_close"},
    }


# --------------------------------------------------------------------------- #
# Historical / pre-R47 reconciliation.
# --------------------------------------------------------------------------- #
def _historical_context(wf: dict, constrained: dict) -> dict:
    cpd = _d(wf.get("canonical_portfolio_decision"))
    rpp = _d(wf.get("reallocation_proposal_presentation"))
    inv = _d(constrained.get("constraint_inventory"))
    session = cpd.get("eligible_market_date") or wf.get("eligible_market_date") \
        or _d(wf.get("operational_state")).get("eligible_market_date")
    blockers = [str(b) for b in _l(cpd.get("withheld_reasons"))]
    true_blocker_codes = {str(c) for c in _l(inv.get("true_blocker_codes"))}
    codes = {b.split(":")[-1] for b in blockers}
    governed_cycle_complete = bool(rpp.get("governed_cycle_complete_for_session"))
    gate_withheld = bool(rpp.get("economic_gate_withheld_the_proposal"))
    feasible = bool(cpd.get("feasible_target_exists")) or bool(
        constrained.get("feasible_target_exists"))
    outcome = constrained.get("outcome")
    no_governed_target = (outcome is None) and not feasible
    # The prior workflow recorded a per-holding CAP breach as a manual-review
    # blocker. Under the current workflow (Release 47 §7b) such a code is a
    # reshaping constraint, never a true blocker. A blocked decision whose every
    # blocker is outside the owner's OWN true-blocker vocabulary, on a session the
    # governed cycle completed without a target, is therefore a historical artifact.
    blockers_are_prior_workflow = bool(codes) and bool(true_blocker_codes) \
        and not (codes & true_blocker_codes)
    historical = (cpd.get("state") == _CPD_BLOCKED and governed_cycle_complete
                  and gate_withheld and no_governed_target
                  and blockers_are_prior_workflow)
    tickers = sorted({b.split(":")[0] for b in blockers if ":" in b})
    if historical:
        explanation = (
            "The %s session was completed under the prior decision workflow. A "
            "portfolio constraint breach was recorded (%d holding%s: %s). No governed "
            "reallocation target exists for that historical session, and the "
            "historical record will not be rewritten."
            % (session or "latest completed", len(tickers), "" if len(tickers) == 1 else "s",
               ", ".join(tickers) or "none named"))
    else:
        explanation = None
    return {
        "historical": bool(historical),
        "session_date": session,
        "recorded_under": ("PRIOR_DECISION_WORKFLOW" if historical
                           else "CURRENT_GOVERNED_WORKFLOW"),
        "explanation": explanation,
        "governed_target_exists": bool(feasible),
        "governed_cycle_complete_for_session": governed_cycle_complete,
        "breach_tickers": tickers,
        "blocker_codes": sorted(codes),
        "blockers_outside_true_blocker_vocabulary": blockers_are_prior_workflow,
        "next_eligible_action": (
            "Run the portfolio cycle after the next eligible market close."),
        "history_rewritten": False,
        "proposal_fabricated": False,
        "rerun_of_historical_session_instructed": False,
    }


# --------------------------------------------------------------------------- #
# The portfolio decision — ONE state, ONE headline, ONE explanation, ONE action.
# --------------------------------------------------------------------------- #
def _next_action(kind: str, *, available: bool, label: Optional[str] = None,
                 destination: Optional[str] = None, execution_contract=None,
                 confirmation: Optional[str] = None, owner: Optional[str] = None,
                 note: Optional[str] = None) -> dict:
    executes = kind in EXECUTING_NEXT_ACTION_KINDS and bool(available)
    return {
        "kind": kind,
        "kind_vocabulary": list(NEXT_ACTION_VOCABULARY),
        "label": label if label is not None else NEXT_ACTION_LABELS.get(kind),
        "available": bool(available),
        "executes": executes,
        "navigates": bool(available) and not executes and kind not in (NA_WAIT, NA_NONE),
        "destination": destination,
        "execution_contract": (dict(execution_contract)
                               if executes and isinstance(execution_contract, dict)
                               else None),
        "confirmation_required": confirmation if executes else None,
        "owner": owner,
        "note": note,
        "creates_orders": False,
        "approves_anything": False,
    }


def _governance(decision: str) -> dict:
    current = {
        PD_REALLOCATE: GOV_REVIEW,
        PD_AWAITING_APPROVAL: GOV_APPROVE,
        PD_AWAITING_CONFIRMATION: GOV_CONFIRM,
        PD_AWAITING_NEXT_CLOSE: GOV_AWAIT_NEXT_CLOSE,
        PD_OUTCOME_ACCRUING: GOV_OUTCOME_ACCRUING,
    }.get(decision)
    steps = []
    passed = current is not None
    for s in GOVERNANCE_SEQUENCE:
        if current is None:
            status = STEP_UPCOMING
        elif s == current:
            status = STEP_CURRENT
            passed = False
        elif passed:
            status = STEP_DONE
        else:
            status = STEP_UPCOMING
        steps.append({"step": s, "label": GOVERNANCE_LABELS[s], "status": status,
                      "owner": GOVERNANCE_OWNERS[s], "manual": s in (GOV_REVIEW, GOV_APPROVE, GOV_CONFIRM)})
    # OUTCOME_ACCRUING implies EXECUTED is done.
    if current == GOV_OUTCOME_ACCRUING:
        for st in steps:
            if st["step"] == GOV_EXECUTED:
                st["status"] = STEP_DONE
    return {
        "sequence": list(GOVERNANCE_SEQUENCE),
        "current_step": current,
        "steps": steps,
        "manual_gates": [GOV_APPROVE, GOV_CONFIRM],
        "note": ("Two independent manual gates, both backend-enforced. Nothing on "
                 "the presentation surface approves, confirms, executes or creates "
                 "an order."),
    }


def _portfolio_decision(wf: dict, constrained: dict, outcomes: dict,
                        historical: dict) -> dict:
    cmd = _d(wf.get("operator_command"))
    pa = _d(wf.get("primary_action"))
    cpd = _d(wf.get("canonical_portfolio_decision"))
    lane = _d(wf.get("portfolio_decision_state"))
    prs = _d(wf.get("portfolio_reassessment"))
    exec_prec = _d(wf.get("portfolio_reassessment_execution_precedence"))
    os_ = _d(wf.get("operational_state"))
    execution = _d(constrained.get("execution"))
    sw = _d(constrained.get("switching_economics"))
    overall = wf.get("overall_state")
    cpd_state = cpd.get("state")
    pd_state = lane.get("portfolio_decision_state")
    rb_state = execution.get("rebalance_state") or exec_prec.get("rebalance_state")
    outcome = constrained.get("outcome")
    pending = _int(os_.get("pending_orders")) or _int(exec_prec.get("pending_orders"))
    session = cpd.get("eligible_market_date") or os_.get("eligible_market_date")
    # R54.2.3.2 — a SUPERSEDED proposal's rows are history, never the current change:
    # counting its published allocations back in through the fallback would resurrect
    # the "REALLOCATE — 28 POSITIONS CHANGE" hero the supersession just retired.
    _superseded = bool(pd_state == _PDS_SUPERSEDED or cpd.get("proposal_superseded"))
    counts = _count_actions(_d(_d(lane.get("materiality")).get("action_counts")),
                            [] if _superseded else
                            _l(_d(constrained.get("best_feasible_target")).get("allocations")))
    changing = sum(v for k, v in counts.items() if k != "RETAIN")

    eyebrow = None
    tone = "info"
    blocked_detail = None
    reason_codes: list = []

    # 1. An execution in flight outranks every decision surface (Stage 19 precedence).
    if rb_state == _RB_PLAN_CONFIRMED or (bool(exec_prec.get("execution_active")) and pending > 0):
        state = PD_AWAITING_NEXT_CLOSE
        headline = "AWAITING NEXT CLOSE" + (" — %d PAPER ORDER%s WORKING" % (pending, "" if pending == 1 else "S")
                                             if pending else "")
        explanation = ("The approved order plan is confirmed. Paper orders fill at the first "
                       "completed owned close strictly after approval (NEXT_CLOSE — no "
                       "same-close hindsight fill); the next Daily Close settles them.")
        action = _next_action(NA_WAIT, available=False,
                              label="Wait for the next completed close")
        tone = "info"
    elif rb_state == _RB_EXECUTED:
        state = PD_OUTCOME_ACCRUING
        headline = "EXECUTED — OUTCOME ACCRUING"
        explanation = ("The governed rebalance executed and reconciled. Its forward "
                       "evidence against the frozen hold counterfactual accrues at each "
                       "completed close; nothing further is required.")
        action = _next_action(NA_NONE, available=False)
        tone = "ok"
    elif rb_state in _RB_BLOCKED:
        state = PD_BLOCKED
        headline = "BLOCKED — ORDER PLAN CANNOT BE BUILT"
        explanation = execution.get("message") or "The approved target cannot be faithfully executed."
        reason_codes = [str(r) for r in _l(execution.get("blocked_reasons"))]
        blocked_detail = {
            "why": explanation,
            "cannot_trust": "The order plan for the approved target; no paper order may be created from it.",
            "resolves": ("Restore the missing owned marks / complete the target, then review "
                         "the order plan again. Nothing is executed automatically."),
        }
        action = _next_action(NA_REVIEW_ORDER_PLAN, available=True,
                              destination="portfolio-manager/reallocation")
        tone = "bad"
    elif rb_state == _RB_PLAN_REVIEW_REQUIRED or pd_state == _PDS_APPROVED:
        state = PD_AWAITING_CONFIRMATION
        headline = "APPROVED — REVIEW ORDER PLAN"
        explanation = ("The reallocation is approved (manual gate 1). Its deterministic "
                       "order plan awaits the second manual confirmation before any paper "
                       "order is created.")
        action = _next_action(NA_REVIEW_ORDER_PLAN, available=True,
                              destination="portfolio-manager/reallocation")
        tone = "warn"
    # 2. A normal-path mutation is due: the ONE Release-48 concept, verbatim.
    elif bool(cmd.get("primary_action_available")) and cmd.get("primary_action_kind") == NA_PORTFOLIO_CYCLE:
        state = PD_CYCLE_REQUIRED
        headline = "RUN THE PORTFOLIO CYCLE"
        explanation = cmd.get("supporting_text") or cmd.get("why") or pa.get("explanation") \
            or "The eligible session is ready to be processed."
        action = _next_action(
            NA_PORTFOLIO_CYCLE, available=True,
            label=cmd.get("primary_action_label") or NEXT_ACTION_LABELS[NA_PORTFOLIO_CYCLE],
            execution_contract=cmd.get("primary_action_execution_contract"),
            confirmation=cmd.get("confirmation_required"),
            owner=cmd.get("primary_action_owner"),
            note="Executes through the canonical dispatcher; stops at the governed portfolio decision.")
        tone = "info"
    elif overall == _WF_CYCLE_RUNNING:
        state = PD_CYCLE_REQUIRED
        headline = "PORTFOLIO CYCLE RUNNING"
        explanation = pa.get("explanation") or "A Daily Research Cycle run is in progress."
        action = _next_action(NA_WAIT, available=False, label="Wait for the running cycle")
        tone = "info"
    elif overall in (_WF_CYCLE_BLOCKED, _WF_INCONSISTENT):
        state = PD_BLOCKED
        headline = "BLOCKED"
        explanation = pa.get("explanation") or cmd.get("why") or "Recovery is required."
        reason_codes = [str(b) for b in _l(wf.get("blockers"))] or \
            [str(c) for c in _l(_d(wf.get("evidence_classification")).get("blocker_codes"))]
        blocked_detail = {
            "why": explanation,
            "cannot_trust": ("The daily cycle cannot advance, so no portfolio decision for "
                             "this session can be trusted."),
            "resolves": pa.get("label") or "Resolve the named blocker.",
        }
        action = _next_action(NA_REVIEW_BLOCKER, available=True,
                              destination="system-audit/diagnostics")
        tone = "bad"
    # 3. The governed decision, read from its owners in their own precedence.
    #    Track B (decision consistency): the constrained owner's HOLD_CURRENT_BOOK
    #    outcome is read FIRST. It is the highest decision authority in this region
    #    (constrained outcome -> decision lane -> composed workflow object), and a
    #    review state any downstream surface reconstructed from "a proposal exists"
    #    must never outrank it — on 2026-08-31 exactly that reconstruction presented
    #    a governed economic HOLD as "REALLOCATE — 27 POSITIONS CHANGE".
    elif cpd_state == _CPD_HOLD or outcome == _RO_HOLD or pd_state == _PDS_HOLD:
        state = PD_HOLD
        headline = "HOLD CURRENT PORTFOLIO"
        explanation = cpd.get("no_proposal_reason") or (
            "A feasible alternative exists, but the expected improvement after "
            "transaction costs and turnover does not clear the switching hurdle.")
        action = _next_action(NA_NONE, available=False)
        tone = "ok"
    elif cpd_state == _CPD_REVIEW or (outcome == _RO_PROPOSAL_READY and bool(lane.get("requires_manual_review"))):
        state = PD_REALLOCATE
        headline = "REALLOCATE" + (" — %d POSITION%s CHANGE" % (changing, "" if changing == 1 else "S")
                                   if changing else "")
        explanation = cpd.get("no_proposal_reason") or cpd.get("explanation") or (
            "A feasible target exists and its expected improvement clears the "
            "switching hurdle after cost, risk, liquidity and turnover.")
        action = _next_action(NA_REVIEW_REALLOCATION, available=True,
                              destination="portfolio-manager/reallocation")
        tone = "warn"
    elif pd_state in (_PDS_HELD, _PDS_STALE):
        state = PD_AWAITING_APPROVAL
        headline = ("PROPOSAL HELD — DECIDE TO PROCEED" if pd_state == _PDS_HELD
                    else "PROPOSAL CHANGED — RE-REVIEW REQUIRED")
        explanation = cpd.get("explanation") or (
            "The proposal awaits the manual approval decision.")
        action = _next_action(NA_REVIEW_REALLOCATION, available=True,
                              destination="portfolio-manager/reallocation")
        tone = "warn"
    elif cpd_state in (_CPD_NO_CHANGE, _CPD_WITHHELD) or (cpd_state == _CPD_RECORDED and pd_state == _PDS_REJECTED):
        state = PD_HOLD
        headline = "HOLD CURRENT PORTFOLIO"
        if cpd_state == _CPD_WITHHELD:
            explanation = cpd.get("no_proposal_reason") or (
                "Deterioration was found, but no change cleared the economic hurdle.")
        elif cpd_state == _CPD_RECORDED:
            explanation = "The proposal was rejected on manual review; the current book stands."
        else:
            explanation = cpd.get("no_proposal_reason") or (
                "The reassessment found the current holdings remain the best available use of capital.")
        action = _next_action(NA_NONE, available=False)
        tone = "ok"
    elif cpd_state == _CPD_BLOCKED or outcome == _RO_TRUE_BLOCKER:
        state = PD_BLOCKED
        reason_codes = [str(r) for r in _l(cpd.get("withheld_reasons"))] or \
            [str(r) for r in _l(_d(constrained.get("reallocation_outcome")).get("reason_codes"))]
        if historical.get("historical"):
            eyebrow = "HISTORICAL DECISION"
            headline = "HISTORICAL DECISION — %s" % (historical.get("session_date") or session or "")
            explanation = historical.get("explanation")
            blocked_detail = {
                "why": explanation,
                "cannot_trust": ("The recorded breach list is a per-holding review signal from "
                                 "the prior workflow; it is not an executable target, and no "
                                 "target was solved for this session."),
                "resolves": historical.get("next_eligible_action"),
            }
            action = _next_action(NA_WAIT, available=False,
                                  label=historical.get("next_eligible_action"),
                                  note="A historical session is never rerun.")
            tone = "warn"
        else:
            headline = "BLOCKED"
            explanation = cpd.get("explanation") or cpd.get("no_proposal_reason") or (
                "No trustworthy portfolio decision can be made.")
            blocked_detail = {
                "why": explanation,
                "cannot_trust": ("The portfolio decision for %s; no target can be reviewed or "
                                 "approved from it." % (session or "this session")),
                "resolves": ("Resolve the named blocker; the next portfolio cycle re-solves the "
                             "target under the constraint-respecting workflow."),
            }
            action = _next_action(NA_REVIEW_BLOCKER, available=True,
                                  destination="portfolio-manager/overview")
            tone = "bad"
    elif cpd_state == _CPD_NOT_RUN or cpd_state is None:
        state = PD_CYCLE_REQUIRED
        if overall == _WF_WAITING_CLOSE:
            headline = "WAITING FOR THE SESSION TO CLOSE"
            explanation = pa.get("explanation") or (
                "The latest eligible session is fully processed; the current session is still open.")
            action = _next_action(NA_WAIT, available=False,
                                  label="Run the portfolio cycle after the session closes")
        else:
            headline = "NO PORTFOLIO DECISION YET"
            explanation = cmd.get("why") or pa.get("explanation") or (
                "No governed portfolio decision exists for the eligible session.")
            action = _next_action(NA_WAIT, available=False,
                                  label=cmd.get("next_text") or "Wait for the portfolio cycle")
        tone = "info"
    else:
        # Fail closed: an unrecognised owner state is never presented as HOLD.
        state = PD_BLOCKED
        headline = "BLOCKED — STATE NOT RECOGNISED"
        explanation = ("The canonical portfolio decision reported a state this presentation "
                       "does not recognise; see the raw states under Audit.")
        blocked_detail = {"why": explanation,
                          "cannot_trust": "The reconciled presentation.",
                          "resolves": "Inspect the raw owner states under System · Audit."}
        action = _next_action(NA_REVIEW_BLOCKER, available=True,
                              destination="system-audit/diagnostics")
        tone = "bad"

    return {
        "state": state,
        "state_vocabulary": list(PORTFOLIO_DECISION_VOCABULARY),
        "state_label": PORTFOLIO_DECISION_LABELS[state],
        "eyebrow": eyebrow,
        "headline": headline,
        "explanation": explanation,
        "tone": tone,
        "eligible_market_date": session,
        "next_action": action,
        "blocked_detail": blocked_detail,
        "reason_codes": reason_codes,
        "positions_changing": changing,
        "governance": _governance(state),
        "manual_review_only": True,
        "creates_orders": False,
        "automation_off": True,
        "owners": {
            "overall_state": "api.workflow_state",
            "operator_command": "api.workflow_state.build_operator_command",
            "canonical_portfolio_decision": "api.workflow_state.build_canonical_portfolio_decision",
            "portfolio_decision_lane": "api.portfolio_decision",
            "reallocation_outcome": "engine.constrained_reallocation via api.reallocation_proposal",
            "execution_state": "api.rebalance_execution",
            "decision_outcomes": "api.portfolio_decision_outcome",
        },
    }


# --------------------------------------------------------------------------- #
# Snapshot, economics, attention, outcome — every number read verbatim.
# --------------------------------------------------------------------------- #
#: Release 54.2.1 (Phase J.1) — the daily P&L is ALWAYS a closed-session figure. It is
#: the change in NAV between the last two recorded operational marks, so calling it
#: "Today" is wrong on every morning before that session's close has run — and it was
#: read as "today" on 2026-09-02 while the number belonged to 2026-08-31. The label is
#: decided HERE, from backend session metadata (the valuation date the close owner
#: recorded vs. the calendar date the session owner reports), so no client performs
#: date arithmetic to work out what the number means.
PNL_LABEL_TODAY = "TODAY"
PNL_LABEL_LAST_CLOSED = "LAST CLOSED SESSION"


def _daily_pnl_label(wf: dict, pnl: dict) -> dict:
    valuation = pnl.get("valuation_date") or _d(wf.get("operational_state")).get(
        "valuation_date")
    calendar = _d(wf.get("current_session")).get("calendar_date")
    v = str(valuation)[:10] if valuation else None
    c = str(calendar)[:10] if calendar else None
    is_today = bool(v and c and v == c)
    return {
        "daily_pnl_session_date": v,
        "daily_pnl_session_display": _month_day(v),
        "daily_pnl_period_label": (PNL_LABEL_TODAY if is_today
                                   else PNL_LABEL_LAST_CLOSED if v else None),
        "daily_pnl_period_label_short": (PNL_LABEL_TODAY if is_today
                                         else (_month_day(v) or "").upper() or None),
        "daily_pnl_is_current_calendar_day": is_today,
        "daily_pnl_label_owner": OWNER,
        "daily_pnl_label_basis": ("api.daily_close.pnl.valuation_date vs "
                                  "api.workflow_state.current_session.calendar_date"),
    }


def _portfolio_snapshot(wf: dict, daily_close: dict, capital_pool: Optional[dict] = None) -> dict:
    os_ = _d(wf.get("operational_state"))
    pnl = _d(daily_close.get("pnl"))
    cpool = _d(capital_pool)
    # Release 50 - the allocation by asset class is the capital-pool owner's, read
    # verbatim; only asset classes that carry weight are present (never "FX 0%").
    allocation = [{"asset_class": k, "label": _d(cpool.get("allocation_labels")).get(k, k),
                   "weight": _num(v)}
                  for k, v in _d(cpool.get("allocation")).items() if _num(v) is not None]
    return {
        "allocation": allocation,
        "allocation_available": bool(allocation),
        "asset_classes_present": [a["asset_class"] for a in allocation],
        "collateral": _num(cpool.get("collateral")),
        "available_capital": _num(cpool.get("available_capital")),
        "gross_exposure": _num(cpool.get("gross_exposure")),
        "non_equity_positions": _int(cpool.get("non_equity_position_count")),
        "capital_pool_owner": cpool.get("owner") or SOURCE_OWNERS["capital_pool"],
        "book_id": os_.get("active_book_id"),
        "book_label": os_.get("active_book_name"),
        "book_status": os_.get("book_status"),
        "valuation_date": os_.get("valuation_date") or pnl.get("valuation_date"),
        "nav": _num(os_.get("nav")) if _num(os_.get("nav")) is not None else _num(pnl.get("nav")),
        "cash": _num(os_.get("cash")) if _num(os_.get("cash")) is not None else _num(pnl.get("cash")),
        "invested_value": _num(pnl.get("invested_value")),
        "positions": _int(os_.get("holdings_count")) if os_.get("holdings_count") is not None else None,
        "pending_orders": _int(os_.get("pending_orders")),
        "daily_pnl": _num(pnl.get("daily_pnl")),
        "daily_return_pct": _num(pnl.get("daily_return_pct")),
        "daily_pnl_available": bool(pnl.get("daily_pnl_available", pnl.get("daily_pnl") is not None)),
        "daily_pnl_note": pnl.get("daily_pnl_note"),
        **_daily_pnl_label(wf, pnl),
        "cumulative_pnl": _num(pnl.get("cumulative_pnl")),
        "cumulative_return_pct": _num(pnl.get("cumulative_return_pct")),
        "benchmark_cumulative_return_pct": _num(pnl.get("spy_cumulative_return_pct")),
        "excess_return_pct": _num(pnl.get("excess_return_pct")),
        "drawdown_pct": _num(pnl.get("drawdown_pct")),
        "starting_capital": _num(pnl.get("starting_capital")),
        "basis": pnl.get("basis"),
        "basis_label": pnl.get("label"),
        "benchmark": "SPY",
        "owners": {"nav_cash_positions": "api.operational_book via api.workflow_state",
                   "pnl": "api.daily_close"},
    }


def _decision_summary(wf: dict, constrained: dict) -> dict:
    cpd = _d(wf.get("canonical_portfolio_decision"))
    lane = _d(wf.get("portfolio_decision_state"))
    best = _d(constrained.get("best_feasible_target"))
    sw = _d(constrained.get("switching_economics"))
    trn = _d(constrained.get("turnover"))
    allocations = _l(best.get("allocations"))
    counts = _count_actions(_d(_d(lane.get("materiality")).get("action_counts")), allocations)
    feasible = bool(cpd.get("feasible_target_exists")) or bool(constrained.get("feasible_target_exists"))
    net = _num(sw.get("score_improvement_net_of_cost"))
    if net is None:
        net = _num(cpd.get("expected_net_improvement"))
    hurdle = _num(sw.get("switching_hurdle"))
    if hurdle is None:
        hurdle = _num(cpd.get("net_improvement_hurdle"))
    turnover = _num(sw.get("one_way_turnover"))
    if turnover is None:
        turnover = _num(trn.get("one_way_turnover"))
    if turnover is None:
        turnover = _num(cpd.get("expected_one_way_turnover"))
    cost = _num(sw.get("estimated_transaction_cost"))
    if cost is None:
        cost = _num(trn.get("estimated_transaction_cost"))
    if cost is None:
        cost = _num(cpd.get("expected_transaction_cost_usd"))
    replacements = counts.get("REPLACE_OUT", 0) or counts.get("REPLACE", 0)
    # Release 54.2.1 (Phase J.2) — WHAT THIS TARGET IS. The reallocation page rendered
    # EXIT / REDUCE / ADD / INCREASE counts at full prominence while the authoritative
    # decision was HOLD CURRENT PORTFOLIO, so the best FEASIBLE alternative read as the
    # RECOMMENDED one. It is not: it is the candidate the switching hurdle rejected.
    # The analysis stays (it is exactly what makes HOLD explainable) and is framed for
    # what it is, from the two owners' own verdicts — no count is hidden or recomputed.
    clears = (sw.get("clears_switching_hurdle")
              if sw.get("clears_switching_hurdle") is not None
              else cpd.get("clears_switching_hurdle"))
    authoritative_state = str(cpd.get("state") or "")
    is_hold = authoritative_state in ("HOLD_CURRENT_BOOK", "NO_CHANGE",
                                      "CHANGE_CANDIDATE_WITHHELD")
    rejected = bool(feasible and (clears is False or is_hold))
    # R54.2.3.2 — the decision-supersession verdict, read verbatim (never derived
    # here): the analysis stays visible as history, framed as exactly that.
    superseded = bool(cpd.get("proposal_superseded") or constrained.get("superseded")
                      or lane.get("proposal_superseded"))
    superseded_by = (cpd.get("superseded_by") or constrained.get("superseded_by")
                     or lane.get("superseded_by") or None)
    return {
        "superseded": superseded,
        "superseded_by": superseded_by,
        "superseded_banner": (
            ("SUPERSEDED — a newer authoritative decision (%s for %s) stands; "
             "this proposal is history only and cannot be reviewed or approved."
             % ((superseded_by or {}).get("decision") or "governed decision",
                (superseded_by or {}).get("session") or "a later session"))
            if superseded else None),
        "available": feasible,
        "feasible_target_exists": feasible,
        "outcome": constrained.get("outcome"),
        # --- what the operator is looking at (never a recommendation on its own) --
        "target_class": ("SUPERSEDED_HISTORY_ONLY" if superseded
                         else "REJECTED_FEASIBLE_ALTERNATIVE" if rejected
                         else "PROPOSED_PORTFOLIO_CHANGE" if feasible
                         else "NO_FEASIBLE_TARGET"),
        "target_class_label": ("SUPERSEDED — HISTORY ONLY" if superseded
                               else "REJECTED FEASIBLE ALTERNATIVE" if rejected
                               else "PROPOSED PORTFOLIO CHANGE" if feasible
                               else "NO FEASIBLE TARGET"),
        "is_recommended_portfolio": bool(feasible and not rejected and not superseded),
        "not_recommended_banner": ("NOT THE RECOMMENDED PORTFOLIO — the switching "
                                   "hurdle was not cleared; the authoritative "
                                   "decision is to hold the current portfolio."
                                   if rejected else None),
        "authoritative_decision_state": authoritative_state or None,
        "authoritative_decision_owner": "api.portfolio_decision via api.workflow_state",
        "renders_approval_cta": bool(feasible and not rejected and not superseded),
        "outcome_vocabulary": list(_l(constrained.get("outcome_vocabulary"))),
        "exits": counts.get("EXIT", 0),
        "reductions": counts.get("REDUCE", 0),
        "replacements": replacements,
        "additions": counts.get("ADD", 0),
        "increases": counts.get("INCREASE", 0),
        "retained": counts.get("RETAIN", 0),
        "positions_changing": sum(v for k, v in counts.items() if k != "RETAIN"),
        "action_counts": counts,
        "turnover": turnover,
        "turnover_budget": _num(cpd.get("turnover_budget")),
        "estimated_cost": cost,
        "net_improvement": net,
        "switching_hurdle": hurdle,
        "clears_switching_hurdle": clears,
        "score_before": _num(sw.get("score_before")),
        "score_after": _num(sw.get("score_after")),
        "risk_before": _num(sw.get("portfolio_volatility_before")),
        "risk_after": _num(sw.get("portfolio_volatility_after")),
        "concentration_before": _num(sw.get("concentration_before")),
        "concentration_after": _num(sw.get("concentration_after")),
        "cash_before": _num(sw.get("cash_weight_before")),
        "cash_after": _num(sw.get("cash_weight_after")),
        "positions_before": sw.get("position_count_before"),
        "positions_after": sw.get("position_count_after"),
        "target_position_count": best.get("position_count"),
        "target_cash_weight": _num(best.get("cash_weight")),
        "constraints_satisfied": _d(best.get("constraints")).get("all_ok"),
        "constraints_that_reshaped": [str(c) for c in _l(cpd.get("constraints_that_reshaped"))
                                      or _l(constrained.get("constraints_that_reshaped"))],
        "constraint_reoptimized": bool(cpd.get("constraint_reoptimized")
                                       or constrained.get("constraint_reoptimization_applied")),
        "mandatory_exits": [str(t) for t in _l(sw.get("mandatory_exits"))
                            or _l(cpd.get("mandatory_exit_tickers"))],
        "expected_return_state": sw.get("expected_return_state") or "NOT_CALIBRATED",
        "improvement_units": "combined percentile score points (never a dollar forecast)",
        "owners": {"counts": "api.portfolio_decision (materiality) / api.reallocation_proposal (allocations)",
                   "economics": "engine.constrained_reallocation via api.reallocation_proposal",
                   "hurdle_and_budget": "api.workflow_state.canonical_portfolio_decision"},
    }


_RELEVANCE = {
    "OPERATIONAL_ALPHA": "operational signal input",
    "OPERATIONAL_RISK": "operational risk input",
    "EVENT_TRIGGER_ONLY": "reassessment trigger only",
    "RESEARCH_ALPHA": "research evidence only",
    "OBSERVABILITY_ONLY": "observability only",
    "BLOCKED": "blocked source",
}


def _alerts_summary(mi: Optional[dict], top_n: int = 3) -> dict:
    mi = _d(mi)
    rows = [_d(r) for r in _l(mi.get("rows"))]

    def rank(r: dict):
        return (0 if r.get("held") else 1,
                0 if r.get("hoc_affected") else 1,
                0 if r.get("risk_affected") else 1)

    ordered = sorted(rows, key=rank)   # stable: the owner's recency order survives
    items = []
    for r in ordered[:top_n]:
        held = bool(r.get("held"))
        rec = r.get("hoc_recommendation")
        auth = str(r.get("signal_authority") or "")
        if held and rec:
            relevance = "Held · review signal %s" % rec
        elif held:
            relevance = "Held · no signal change"
        elif r.get("ticker"):
            relevance = "Not held · " + _RELEVANCE.get(auth, "event")
        else:
            relevance = "Market-wide · " + _RELEVANCE.get(auth, "event")
        desc = str(r.get("what_changed") or r.get("source_title") or r.get("event_type") or "")
        items.append({
            "event_id": r.get("event_id"),
            "ticker": r.get("ticker"),
            "held": held,
            "description": desc[:120],
            "relevance": relevance,
            "hoc_recommendation": rec,
            "event_type": r.get("event_type"),
            "signal_authority": auth or None,     # raw, for audit — never rendered as prose
            "timestamp": r.get("timestamp"),
            "source_url": (r.get("source_url")
                           if r.get("source_url_state") == "CANONICAL_SOURCE_URL" else None),
        })
    return {
        "available": bool(mi) and mi.get("state") in ("READY", "NO_MATERIAL_INFORMATION"),
        "state": mi.get("state"),
        "count": _int(mi.get("total_material_events")),
        "portfolio_relevant_count": _int(mi.get("material_events_affecting_holdings")),
        "affected_holdings": [str(t) for t in _l(mi.get("affected_holdings"))],
        "rows_available": len(rows),
        "top_items": items,
        "top_n": top_n,
        "detail_location": "system-audit/diagnostics",
        "owner": mi.get("composition_owner") or SOURCE_OWNERS["material_information"],
        "authority_owner": mi.get("authority_policy_owner"),
    }


def _decision_outcome(outcomes: Optional[dict]) -> dict:
    o = _d(outcomes)
    measured = _int(o.get("measured_count"))
    return {
        "available": measured > 0,
        "decision_count": _int(o.get("decision_count")),
        "measured_count": measured,
        "pending_count": _int(o.get("pending_count")),
        "cumulative_incremental_pnl": _num(o.get("cumulative_incremental_pnl")),
        "cumulative_transaction_cost": _num(o.get("cumulative_transaction_cost")),
        "verdict_counts": _d(o.get("verdict_counts")),
        "improvement_basis": o.get("improvement_basis"),
        "owner": o.get("owner") or SOURCE_OWNERS["decision_outcomes"],
    }


# --------------------------------------------------------------------------- #
# Release 54.2.2 — GOVERNED RESEARCH, presented (Phase J).
#
# Today showed "Wait for the market session to close" while a completed session's
# governed research had never run, and simultaneously painted the whole service red
# because two research inputs were stale. Both halves were wrong in the same way:
# the operational lane and the governed-research lane were being read as one thing.
#
# This block states them as three plain lines the operator can act on — the book,
# the governed research, and the forward-evidence gap — plus the ONE next action.
# Every value is the workflow owner's; this owner only decides how to say it.
# --------------------------------------------------------------------------- #
def _usd(x: Any) -> str:
    """Backend-formatted USD. The client performs no arithmetic and no formatting."""
    v = _num(x)
    return "n/a" if v is None else ("$%s" % format(round(v, 2), ",.2f"))


_GR_NEXT_RESUME = "RESUME_PORTFOLIO_CYCLE"
_GR_NEXT_RESOLVE = "RESOLVE_NAMED_RESEARCH_BLOCKER"
_GR_NEXT_MONITOR = "MONITOR_RUNNING_CYCLE"
_GR_NEXT_NONE = "NONE"


def _governed_research(wf: dict, daily_close: dict) -> dict:
    ob = _d(wf.get("research_obligation"))
    os_ = _d(wf.get("operational_state"))
    ev = _d(wf.get("evidence_state"))
    dc = _d(daily_close)
    if not ob:
        return {"available": False, "active": False,
                "owner": "api.workflow_state", "computed_here": False,
                "detail": ("The workflow owner did not publish a post-close research "
                           "obligation; nothing is inferred in its place.")}
    state = str(ob.get("research_obligation_state") or "")
    session = ob.get("outstanding_research_session")
    closed = ob.get("latest_closed_session")
    close_valid = bool(os_.get("operational_close_valid"))
    blockers = _l(ob.get("true_blockers"))
    stale_ids = [str(s) for s in _l(ob.get("stale_input_ids"))]

    # Line 1 — the operational book. It is VALID or it is not; a research lane never
    # decides that, and this line says so explicitly.
    book_line = ("%s close complete — NAV %s — VALID"
                 % (_month_day(closed) or (closed or "the latest session"),
                    _usd(_num(os_.get("nav")))) if close_valid else
                 "The latest operational close is not marked valid.")
    # Line 2 — the governed research for that same session.
    if state == "NO_RESEARCH_OBLIGATION":
        research_line = ("%s governed research complete"
                         % (_month_day(ob.get("latest_governed_research_session"))
                            or "Latest session"))
    elif state == "RESEARCH_OBLIGATION_EVIDENCE_GAP":
        research_line = ("%s governed research cannot be reconstructed — documented gap"
                         % (_month_day(session) or session))
    else:
        research_line = ("%s incomplete — %d research input%s require%s resolution"
                         % (_month_day(session) or session, len(stale_ids),
                            "" if len(stale_ids) == 1 else "s",
                            "s" if len(stale_ids) == 1 else ""))
    # Line 3 — the forward-evidence gap, which is a THIRD thing again: historical,
    # unrecoverable, and explicitly not a reason to distrust the close.
    fe_gap = bool(ev.get("documented_gap")) or bool(
        _d(dc.get("forward_evidence")).get("status") == "FORWARD_EVIDENCE_BLOCKED")
    fe_line = None
    if fe_gap:
        _fe = _d(dc.get("forward_evidence"))
        fe_line = ("%s historical snapshot gap documented — %s — does not invalidate "
                   "the close"
                   % (_month_day(_fe.get("market_date") or closed)
                      or (closed or "the closed session"),
                      "not recoverable"
                      if str(_fe.get("recovery_classification") or "")
                      == "EVIDENCE_GAP_MUST_REMAIN" else "attention only"))

    if state == "RESEARCH_OBLIGATION_OUTSTANDING":
        nxt, next_label = _GR_NEXT_RESUME, "Resume the portfolio cycle"
        if str(ob.get("next_action") or "") == "MONITOR_RUNNING_CYCLE":
            nxt, next_label = _GR_NEXT_MONITOR, "A cycle run is already in progress"
    elif state == "RESEARCH_OBLIGATION_BLOCKED":
        nxt = _GR_NEXT_RESOLVE
        _named = ", ".join(str(_d(b).get("source_id") or "") for b in blockers) \
            or "the named research blocker"
        next_label = "Resolve %s, then resume the portfolio cycle" % _named
    elif state == "RESEARCH_OBLIGATION_EVIDENCE_GAP":
        nxt, next_label = _GR_NEXT_NONE, "No safe recovery exists — the gap is documented"
    else:
        nxt, next_label = _GR_NEXT_NONE, None
    return {
        "available": True,
        "active": bool(ob.get("obligation_outstanding")),
        "owner": "api.workflow_state",
        "classification_owner": ob.get("classification_owner"),
        "computed_here": False,
        "state": state,
        "state_vocabulary": _l(ob.get("state_vocabulary")),
        "outstanding_research_session": session,
        "outstanding_research_session_display": _month_day(session),
        "operational_book_line": book_line,
        "operational_book_valid": close_valid,
        "governed_research_line": research_line,
        "forward_evidence_line": fe_line,
        "forward_evidence_gap": fe_gap,
        "forward_evidence_gap_invalidates_close": False,
        "latest_closed_session": closed,
        "latest_governed_research_session": ob.get("latest_governed_research_session"),
        "latest_governed_decision_session": ob.get("latest_governed_decision_session"),
        "decision_rests_on_governed_research": ob.get(
            "decision_rests_on_governed_research"),
        "stale_input_ids": stale_ids,
        "input_classification": _l(ob.get("input_classification")),
        "safely_recoverable_input_ids": _l(ob.get("safely_recoverable_input_ids")),
        "true_blockers": blockers,
        "next_action_kind": nxt,
        "next_action_label": next_label,
        "summary": ob.get("summary"),
        # This panel offers no execute control of its own: recovery resumes through
        # the ONE portfolio cycle, and there is no research backfill button.
        "backfill_control_offered": False,
        "research_specific_route": None,
        "operator_supplies_no_date": True,
    }


# --------------------------------------------------------------------------- #
# Release 54.2.2 — ATTRIBUTION AVAILABILITY, presented (Phase M).
#
# On 2026-09-01 every holding was published with pnl_contribution 0.0 while the
# recorded NAV had moved -$1,206.59, and the surface showed those zeros as a
# decomposition. "Every holding contributed nothing" is a claim about the market;
# "this decomposition could not be computed" is a claim about the data, and only
# the second one was true. The reconciliation owner now decides which of the two
# the operator is told, and NAV / P&L keep their own owner and stay visible.
# --------------------------------------------------------------------------- #
ATTRIBUTION_UNAVAILABLE_HEADLINE = "ATTRIBUTION UNAVAILABLE — NAV RECONCILIATION FAILED"


def _attribution(daily_close: dict) -> dict:
    a = _d(_d(daily_close).get("attribution"))
    if not a:
        return {"available": False, "reconciles": None,
                "owner": SOURCE_OWNERS.get("daily_close", "api.daily_close"),
                "headline": None, "detail": None,
                "shows_zero_contributors": False}
    reconciles = a.get("reconciles")
    available = bool(a.get("available"))
    return {
        "available": available,
        "decomposition_trustworthy": bool(a.get("decomposition_trustworthy", available)),
        "reconciles": bool(reconciles) if reconciles is not None else None,
        "status": a.get("attribution_status"),
        "attribution_date": a.get("attribution_date"),
        "prior_date": a.get("prior_date"),
        "residual": _num(a.get("reconciliation_residual")),
        "position_contribution_sum": _num(a.get("position_contribution_sum")),
        "market_movement_pnl": _num(a.get("market_movement_pnl")),
        "stale_mark_tickers": _l(_d(a.get("mark_source")).get("stale_mark_tickers")),
        "headline": (None if available else ATTRIBUTION_UNAVAILABLE_HEADLINE),
        "detail": (None if available else (
            a.get("unavailable_reason") or a.get("reconciliation_diagnostic"))),
        "diagnostic": a.get("reconciliation_diagnostic"),
        "diagnostic_location": "system-audit/diagnostics",
        # The two facts the operator must not confuse. Total P&L validity belongs to
        # the NAV owner; only the DECOMPOSITION is withheld here.
        "total_pnl_remains_valid": True,
        "total_pnl_owner": "api.daily_close",
        "shows_zero_contributors": False,
        "owner": "api.forward_evidence (reconciliation contract)",
    }


def _research_health(wf: dict) -> dict:
    ev = _d(wf.get("evidence_state"))
    mrev = _d(wf.get("model_review"))
    gap = bool(ev.get("documented_gap"))
    return {
        "evidence_status": ev.get("evidence_status"),
        "documented_gap": gap,
        "gap_severity": ev.get("gap_severity"),
        "model_review_state": mrev.get("model_review_state"),
        "model_review_required": bool(mrev.get("model_review_required")),
        "label": ("Evidence gap" if gap else
                  ("Model review due" if mrev.get("model_review_required") else "Healthy")),
        "destination": "research",
    }


# --------------------------------------------------------------------------- #
# The builder (pure) and the loader (composition, parallel, degrade-safe).
# --------------------------------------------------------------------------- #
def build_operator_presentation(*, workflow: Optional[dict],
                                constrained: Optional[dict] = None,
                                daily_close: Optional[dict] = None,
                                material_information: Optional[dict] = None,
                                decision_outcomes: Optional[dict] = None,
                                information_collection: Optional[dict] = None,
                                warnings: Optional[list] = None,
                                now: Optional[datetime] = None,
                                capital_pool: Optional[dict] = None) -> dict:
    """Reconcile the authoritative owner payloads into ONE operator presentation.

    Pure: no I/O, no owner call, no recomputation. A missing owner degrades to an
    honest gap (``sources[<name>].available = False``) and never to a fabricated value.
    """
    wf = _d(workflow)
    cn = _d(constrained)
    dc = _d(daily_close)
    warns = list(warnings or [])
    historical = _historical_context(wf, cn)
    decision = _portfolio_decision(wf, cn, _d(decision_outcomes), historical)
    recovery = _session_recovery(wf, dc)
    governed_research = _governed_research(wf, dc)
    attribution = _attribution(dc)
    system = _system_readiness(
        wf, information_collection if isinstance(information_collection, dict) else None,
        recovery, governed_research)
    snapshot = _portfolio_snapshot(wf, dc, capital_pool)
    summary = _decision_summary(wf, cn)
    alerts = _alerts_summary(material_information)
    outcome = _decision_outcome(decision_outcomes)
    cmd = _d(wf.get("operator_command"))
    os_ = _d(wf.get("operational_state"))
    raw_states = {
        "overall_state": wf.get("overall_state"),
        "operator_command_state": cmd.get("state"),
        "canonical_portfolio_decision_state": _d(wf.get("canonical_portfolio_decision")).get("state"),
        "reassessment_state": _d(wf.get("portfolio_reassessment")).get("state"),
        "proposal_state": _d(wf.get("reallocation_proposal_presentation")).get("state"),
        "reallocation_operator_state": wf.get("reallocation_operator_state"),
        "portfolio_decision_state": _d(wf.get("portfolio_decision_state")).get("portfolio_decision_state"),
        "reallocation_outcome": cn.get("outcome"),
        "constrained_state": cn.get("state"),
        "rebalance_state": _d(cn.get("execution")).get("rebalance_state"),
        "close_status": os_.get("latest_close_status"),
        "collection_service_state": _d(_d(information_collection).get("service")).get("service_state"),
        "note": "Raw owner states, for Audit / Advanced only. Normal surfaces render the reconciled fields.",
        # Release 54.2.1 (Phase J.3) — a raw upstream token is NOT a decision, and Audit
        # rendered them as if it were: "PORTFOLIO PROPOSAL READY" with a "REVIEW
        # PORTFOLIO PROPOSAL" CTA beside an authoritative decision of HOLD CURRENT
        # PORTFOLIO. PROPOSAL_READY means "a feasible alternative was constructed", the
        # input to the governed gate — not its verdict. These flags travel WITH the
        # values so no surface can render them as an action.
        "label": "RAW / NON-AUTHORITATIVE DIAGNOSTIC STATE",
        "authoritative": False,
        "actionable": False,
        "renders_cta": False,
        "authoritative_decision_state": _d(
            wf.get("canonical_portfolio_decision")).get("state"),
        "authoritative_decision_label": decision.get("label"),
        "authoritative_decision_owner": SOURCE_OWNERS["workflow_state"],
        "manual_review_required": bool(_d(wf.get("portfolio_attention")).get(
            "review_required")),
        "disagrees_with_authoritative_decision": bool(
            str(_d(wf.get("reallocation_proposal_presentation")).get("state") or "")
            .endswith("PROPOSAL_READY")
            and not _d(wf.get("portfolio_attention")).get("review_required")),
        "disagreement_note": (
            "A raw PROPOSAL_READY / PROPOSAL state describes an upstream artifact, "
            "never a governed portfolio change. When the governed decision is HOLD "
            "and no manual review is required, no proposal review is outstanding and "
            "no review CTA may be rendered from these values."),
    }
    sources = {
        "workflow_state": {"available": bool(wf), "owner": SOURCE_OWNERS["workflow_state"]},
        "constrained_reallocation": {"available": bool(cn), "owner": SOURCE_OWNERS["constrained_reallocation"]},
        "daily_close": {"available": bool(dc), "owner": SOURCE_OWNERS["daily_close"]},
        "material_information": {"available": bool(material_information), "owner": SOURCE_OWNERS["material_information"]},
        "decision_outcomes": {"available": bool(decision_outcomes), "owner": SOURCE_OWNERS["decision_outcomes"]},
        "information_collection": {"available": bool(information_collection), "owner": SOURCE_OWNERS["information_collection"]},
        "capital_pool": {"available": bool(capital_pool), "owner": SOURCE_OWNERS["capital_pool"]},
    }
    safety_badges: list[str] = []
    for src in (wf, cn, dc):
        for b in _l(_d(src.get("safety")).get("safety_badges")) + _l(src.get("safety_badges")):
            if b not in safety_badges:
                safety_badges.append(str(b))
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "owner": OWNER,
        "status": "OK" if wf else "DEGRADED",
        "as_of": _now_iso(now),
        "eligible_market_date": (_d(wf.get("canonical_portfolio_decision")).get("eligible_market_date")
                                 or os_.get("eligible_market_date") or cmd.get("eligible_market_date")),
        "latest_completed_close_date": os_.get("latest_completed_close_date")
                                       or cmd.get("latest_completed_close_date"),
        "system_readiness": system,
        # Release 54.2.1 — the missed-completed-session panel every operator surface
        # renders verbatim. The obligation is api.workflow_state's; the provider
        # readiness beside it is api.daily_close's; this module decides neither.
        "session_recovery": recovery,
        # Release 54.2.2 — the post-close governed-research panel, and the
        # attribution availability verdict every P&L surface obeys.
        "governed_research": governed_research,
        "attribution": attribution,
        "portfolio_decision": decision,
        "headline": decision["headline"],
        "explanation": decision["explanation"],
        "next_action": decision["next_action"],
        "portfolio_snapshot": snapshot,
        "decision_summary": summary,
        "alerts_summary": alerts,
        "decision_outcome": outcome,
        "research_health": _research_health(wf),
        "historical_context": historical,
        "safety": {
            "mode_line": SAFETY_MODE_LINE,
            "paper_only": True,
            "manual_approval": True,
            "automation_off": True,
            "creates_orders": False,
            "creates_fills": False,
            "approves_proposals": False,
            "confirms_order_plans": False,
            "broker": "NONE",
            "badges": safety_badges,
            "detail_location": "Portfolio › Audit & Details and System · Audit",
        },
        "raw_states": raw_states,
        "sources": sources,
        "warnings": warns,
        "recomputes_nothing": True,
        "recomputed_concepts": [],
        "consumed_owners": dict(SOURCE_OWNERS),
        "read_only": True,
        "wrote_to_database": False,
        "wrote_to_ledger": False,
        "called_provider": False,
        "called_prediction": False,
        "note": ("ONE reconciled operator presentation. It consumes the authoritative "
                 "owners and translates their already-decided states into one operator "
                 "truth; it never recomputes NAV, a target, a portfolio decision, a "
                 "constraint, an HOC assessment, a proposal, an execution state or a "
                 "research verdict, never rewrites history and never fabricates a proposal."),
    }


def _accepts(fn: Callable, name: str) -> bool:
    try:
        import inspect
        return name in inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False


def owner_loaders(*, portfolio_state: Optional[dict] = None) -> dict[str, Callable[[], dict]]:
    """The owners, composed — never forked.

    Every owner that accepts an injected ``portfolio_state`` receives the ONE
    portfolio state loaded here, so the presentation costs one portfolio-state
    read instead of five (the read models are otherwise identical). The
    collection lifecycle is read through the owner's own
    ``resolve_service_lifecycle`` verdict (RUNNING / STOPPED / DEGRADED /
    NEVER_STARTED) — the same function the full collection read model uses —
    without the per-source health scan the presentation does not render.

    Release 50 - the decision snapshot passes the ONE portfolio state it already
    composed (``portfolio_state=``) and reuses these loaders for the owners it
    fans out, so no second read-model composition exists.
    """
    # Imported lazily so the pure builder stays importable without the owners.
    from paper_trader.api import workflow_state as _ws
    from paper_trader.api import reallocation_proposal as _rp
    from paper_trader.api import daily_close as _dc
    from paper_trader.api import material_information as _mi
    from paper_trader.api import portfolio_decision_outcome as _pdo
    from paper_trader.api import information_collection as _ic
    from paper_trader.api import portfolio_state as _pst
    from paper_trader.api import portfolio_decision as _pdm
    from paper_trader.api import holding_opportunity_cost as _hoc
    from paper_trader.api import event_signal_refresh as _esr
    from paper_trader.api import return_forecast as _rfc
    from paper_trader.api import capital_pool as _cpool

    cache: dict[str, Any] = {}
    if portfolio_state is not None:
        cache["ps"] = portfolio_state

    def _ps() -> Optional[dict]:
        if "ps" not in cache:
            try:
                cache["ps"] = _pst.load_portfolio_state()
            except Exception:  # noqa: BLE001 - owners fall back to their own read
                cache["ps"] = None
        return cache["ps"]

    def _with_ps(fn: Callable, **kw):
        ps = _ps()
        if ps is not None and _accepts(fn, "portfolio_state"):
            kw["portfolio_state"] = ps
        return fn(**kw)

    def _material() -> dict:
        # The owner's own composition (``load_material_information``) re-reads the
        # portfolio state inside each composed owner; this passes the ONE state in
        # and calls the owner's own pure ``build`` — identical output, one read.
        try:
            ev = _with_ps(_esr.load_event_signal_refresh_status)
            hoc = _with_ps(_hoc.load_holding_opportunity_cost)
            dec = _with_ps(_pdm.load_portfolio_decision)
            fsum = _rfc.summary()
            return _mi.build(event_refresh=ev, hoc=hoc, decision=dec,
                             forecast_summary=fsum, limit=12)
        except Exception:  # noqa: BLE001 - the owner's own degrade path
            return _mi.load_material_information(limit=12)

    def _collection() -> dict:
        state = _ic.load_service_state()
        try:
            lock = _ic._read_json(_ic._lock_path()) or None
        except Exception:  # noqa: BLE001
            lock = None
        lc = _ic.resolve_service_lifecycle(state, lock, datetime.now(timezone.utc))
        return {"service": lc,
                "recovery": {"why": lc.get("reason")},
                "headline": {"detail": lc.get("reason")},
                "lifecycle_owner": "api.information_collection.resolve_service_lifecycle",
                "scope": "SERVICE_LIFECYCLE_ONLY"}

    def _daily_close() -> Optional[dict]:
        # Loaded once per composition: the workflow loader consumes its provider
        # answer, and the presentation renders the same payload — ONE probe.
        if "dc" not in cache:
            try:
                cache["dc"] = _dc.load_daily_close()
            except Exception:  # noqa: BLE001 - the workflow then fails closed
                cache["dc"] = None
        return cache["dc"]

    def _workflow() -> dict:
        # Release 54.2.3.1 — the probe-free workflow owner receives the close
        # owner's already-probed provider answer, exactly as the decision
        # snapshot supplies it. A missing daily-close payload degrades to no
        # answer, which the workflow fails closed on.
        dc_payload = _daily_close() or {}
        return _ws.load_workflow_state(
            provider_readiness=dc_payload.get("provider_readiness"),
            market_data_scope=dc_payload.get("market_data_scope"))

    return {
        "workflow": _workflow,
        "constrained": lambda: _with_ps(_rp.load_constrained_reallocation),
        "daily_close": lambda: _daily_close() or {},
        "material_information": _material,
        "decision_outcomes": lambda: _pdo.load_portfolio_decision_outcomes(),
        "information_collection": _collection,
        "capital_pool": lambda: _cpool.load_capital_pool(portfolio_state=_ps()),
    }


def _default_loaders() -> dict[str, Callable[[], dict]]:
    return owner_loaders()


def load_operator_presentation(*, workflow: Optional[dict] = None,
                               constrained: Optional[dict] = None,
                               daily_close: Optional[dict] = None,
                               material_information: Optional[dict] = None,
                               decision_outcomes: Optional[dict] = None,
                               information_collection: Optional[dict] = None,
                               loaders: Optional[dict] = None,
                               now: Optional[datetime] = None,
                               capital_pool: Optional[dict] = None) -> dict:
    """Compose the owners and build.

    READ-ONLY. Every owner is a GET-level read model; a failing owner degrades to a
    warning and an honest ``sources[<name>].available = False`` — never a crash and
    never a fabricated value.
    """
    supplied = {
        "workflow": workflow, "constrained": constrained, "daily_close": daily_close,
        "material_information": material_information,
        "decision_outcomes": decision_outcomes,
        "information_collection": information_collection,
        "capital_pool": capital_pool,
    }
    warnings: list[str] = []
    missing = [k for k, v in supplied.items() if v is None]
    if missing:
        try:
            lds = dict(loaders) if loaders else _default_loaders()
        except Exception as exc:  # noqa: BLE001 - owners unavailable: degrade honestly
            lds = {}
            warnings.append("owner loaders unavailable: %s" % str(exc)[:160])
        # Serial on purpose: the read models are CPU-bound under the interpreter
        # lock, so threads buy nothing; the ONE shared portfolio-state read (see
        # ``_default_loaders``) is what makes the composition cheap.
        for k in missing:
            fn = lds.get(k)
            if fn is None:
                warnings.append("%s: no loader" % k)
                continue
            try:
                supplied[k] = fn()
            except Exception as exc:  # noqa: BLE001
                supplied[k] = None
                warnings.append("%s unavailable: %s" % (k, str(exc)[:160]))
    return build_operator_presentation(
        workflow=supplied["workflow"], constrained=supplied["constrained"],
        daily_close=supplied["daily_close"],
        material_information=supplied["material_information"],
        decision_outcomes=supplied["decision_outcomes"],
        information_collection=supplied["information_collection"],
        capital_pool=supplied["capital_pool"],
        warnings=warnings, now=now)
