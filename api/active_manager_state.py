r"""api/active_manager_state.py — R54 Slice 1: THE ONE Active Manager Operating State.

WHY THIS EXISTS
---------------
Every fact the operator needs already has a canonical owner, but before R54 no
single response answered the active-manager questions in one place: what is the
operational book state, what is the latest live/intraday research state, what
just happened, did arriving information cause a signal refresh, was the universe
rescored, was the portfolio reassessed and why, what is the decision, does a
target/proposal exist, is manual action needed, what is stale, and what happens
next. The operator had to infer the answer across Today, Portfolio,
Reallocation, Markets, Research and System · Audit.

WHAT THIS IS
------------
A READ-ONLY COMPOSITION / PROJECTION over the existing canonical owners — never
a second business-calculation engine. Every number and every verdict is read
VERBATIM from the owner that decided it:

    operational book / NAV / cash / holdings   api.portfolio_state (one NAV:
                                               api.operational_book -> desk.book_nav)
    session / dates / freshness words          api.workflow_state (built on
                                               api.data_freshness / engine.market_session)
    workflow interpretation / next action      api.workflow_state (the ONE owner)
    collection lifecycle                       api.information_collection
                                               .resolve_service_lifecycle
    live event / signal-refresh state          api.event_signal_refresh (read contract)
    universe scoring identity                  api.universe_scoring.canonical_identity
    portfolio reassessment                     api.portfolio_reassessment (read contract)
    governed target / switching economics      api.reallocation_proposal
                                               .load_constrained_reallocation (R47)
    approval lane                              api.portfolio_decision (via the R47 read)
    order-plan / execution state               api.rebalance_execution (snapshot section)
    forward evidence / model review            api.workflow_state (evidence_state /
                                               model_governance_state / model_review)
    research runtime health                    api.research_runtime (R52)
    intraday prospective emission (R53.1)      api.research_runtime
                                               .load_intraday_emission_status

THE TIME-STATE DISTINCTION (the R54 contract)
---------------------------------------------
The payload keeps two clocks explicitly and permanently separate:

    OPERATIONAL BOOK STATE — the latest closed ELIGIBLE market session, the
    official operational mark, holdings, cash, NAV. This may legitimately remain
    the PREVIOUS session's date during a live trading day.

    LIVE / INTRADAY RESEARCH STATE — the latest information observation, the
    latest material event, the latest incremental event cycle, the latest
    scoring identity, the latest TRUE_FORWARD snapshot. These may be newer than
    the operational mark at any moment.

An intraday observation NEVER becomes an operational close mark here: this
module composes the two families side by side and can never write either. Only
the Daily Close owner advances the operational mark, and this module has no
write path of any kind.

WHAT THE PROJECTION MAY DO (and nothing more)
---------------------------------------------
* copy an owner's published field verbatim;
* SELECT among owner-published rows (e.g. the newest owner-stamped event
  timestamp in an owner-bounded list — a selection, never a fabricated stamp);
* COUNT rows in an owner-bounded list against an owner-published timestamp
  (``material_events_since_last_reassessment`` states its bounded basis);
* list a component as stale/missing ONLY when that component's OWN state token
  says so (NOT_RUN / UNAVAILABLE / STALE / DEGRADED / MISSING vocabularies are
  each owner's, never re-derived from wall-clock arithmetic here).

It computes NO NAV, NO opportunity cost, NO target, NO proposal, NO ranking, NO
freshness verdict, NO recommendation and NO priority of its own. READ-ONLY: no
write, no provider call, no prediction call, no order, no approval, no
scheduler. Consumed by the Today surface through exactly ONE UI loader.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Optional

PHASE = "R54"
OWNER = "api.active_manager_state"
SCHEMA_VERSION = "active_manager_state.v1"
ROUTE = "/v1/operations/active-manager-state"

#: Components of the composed state, in presentation order. Each is either
#: PRESENT or listed in ``stale_components`` with its owner's own words.
COMPONENTS = (
    "operational_book", "live_information", "signal_state",
    "portfolio_reassessment", "target_proposal", "research_governance",
    "execution_safety", "operator_guidance",
    # R54.1 — the two decision lanes, permanently separate, plus the gate that
    # is the ONLY bridge between them and the measured latency of that bridge.
    "latest_live_intraday_assessment", "latest_governed_portfolio_decision",
    "intraday_governance", "decision_latency",
    # R54.2.4 — LANE B: the live/intraday reassessment as one first-class,
    # self-explaining answer (composed from the components above; no new owner).
    "live_reassessment_lane",
)

#: The authoritative owner of every composed component (part of the tested
#: contract; a renamed owner fails loudly in the release tests).
COMPONENT_OWNERS = {
    "operational_book": "api.portfolio_state (api.operational_book -> desk.book_nav)",
    "live_information": "api.information_collection + api.event_signal_refresh",
    "signal_state": "api.event_signal_refresh + api.universe_scoring + api.workflow_state",
    "portfolio_reassessment": "api.portfolio_reassessment",
    "target_proposal": "api.reallocation_proposal (engine.constrained_reallocation)",
    "research_governance": "api.workflow_state + api.universe_scoring + api.research_runtime",
    "execution_safety": "api.portfolio_decision + api.rebalance_execution + api.workflow_state",
    "operator_guidance": "api.workflow_state",
    "latest_live_intraday_assessment": "api.event_signal_refresh",
    "latest_governed_portfolio_decision": "api.portfolio_decision",
    "intraday_governance": "api.portfolio_decision",
    "decision_latency": "api.event_signal_refresh",
    "live_reassessment_lane": "api.active_manager_state",
}

SAFETY_BADGES = ["READ ONLY", "PREVIEW ONLY", "NO ORDERS", "ORDERS DISABLED",
                 "AUTOMATION OFF", "MANUAL REVIEW"]

TIME_STATE_STATEMENT = (
    "The OPERATIONAL BOOK is marked to the latest closed eligible market "
    "session and may legitimately trail the live research clock during a "
    "trading day. LIVE / INTRADAY RESEARCH state (observations, event cycles, "
    "scoring, prospective emissions) may be newer at any moment. The two clocks "
    "are never conflated: an intraday observation never becomes an operational "
    "close mark, and only the Daily Close owner advances the operational mark.")

#: R54 finalization — the DECISION AUTHORITY LADDER. Five distinct concepts the
#: operator must never see collapsed into one word. Structural architecture
#: statement (like TIME_STATE_STATEMENT); every VALUE beside it is projected
#: verbatim from the owner named on its row.
DECISION_AUTHORITY_STATEMENT = (
    "Five distinct concepts, five owners, one ladder. A LIVE INTRADAY "
    "ASSESSMENT (incremental event cycle, api.event_signal_refresh) is current "
    "signal context and never advances the governed decision or the "
    "operational clock. A GOVERNED PORTFOLIO REASSESSMENT "
    "(api.portfolio_reassessment) is the persisted decision evidence for the "
    "eligible session. The GOVERNED TARGET (api.reallocation_proposal) is the "
    "priced complete-portfolio answer, whose own outcome may be to HOLD. A "
    "MANUAL-REVIEW CANDIDATE exists only when the decision owner "
    "(api.portfolio_decision) says review is required. An APPROVED DECISION "
    "exists only after the operator records it manually. A portfolio may be "
    "reassessed many times per trading day; it is never rebalanced merely "
    "because it was reassessed.")

#: What the event cycle's PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW token asserts —
#: and what it does NOT. The token records that the cycle asked the canonical
#: owners to build a complete priced target ARTIFACT for review; whether that
#: artifact recommends any change is stated only by the target owner's own
#: outcome (``target_proposal.target_state``), and the decision is settled only
#: by the portfolio-decision owner.
EVENT_CYCLE_PROPOSAL_NOTE = (
    "PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW records that a complete priced "
    "target artifact was built for review by the canonical owners. It does "
    "not, by itself, recommend a portfolio change: the target owner's own "
    "outcome states whether the change clears the switching hurdle, and the "
    "governed decision belongs to api.portfolio_decision.")

# --------------------------------------------------------------------------- #
# RELEASE 55 — the two component surfaces, and why a row moves between them.
# --------------------------------------------------------------------------- #
#: The operator's STALE / MISSING list. A row here asserts a real problem.
STALE_SURFACE = "OPERATOR_STALE_MISSING"
#: The AUDIT / ADVANCED advisory list. True, retained, and NOT a problem.
ADVISORY_SURFACE = "AUDIT_ADVANCED_ADVISORY"

#: Why the legacy scheduled-review row is advisory rather than an operator
#: problem. Quoted on the audit surface so the demotion is self-explaining and
#: can never be mistaken for a hidden blocker.
LEGACY_SCHEDULE_ADVISORY_REASON = (
    "The portfolio reassessment IS current for the eligible session. The only "
    "thing that moved its status off CURRENT is the LEGACY monthly "
    "scheduled-review checkpoint clock, whose own owner (api.operational_book) "
    "declares it is the floor for MODEL RECALIBRATION and explicitly not the "
    "governing portfolio-reassessment cadence "
    "(review_is_the_governing_portfolio_cadence=False). A legacy clock that the "
    "authoritative current reassessment contradicts is a compatibility "
    "observation, not an operator problem, so it is retained here in full and "
    "kept off the normal operator surface.")

#: The THREE questions the operator's first screen must answer, in order. The
#: composed answers travel under these exact keys so no surface invents a fourth
#: question or reorders the three.
OPERATOR_ANSWER_QUESTIONS = (
    "WHAT IS THE CURRENT AUTHORITATIVE PORTFOLIO DECISION?",
    "WHAT HAS CHANGED SINCE THAT DECISION?",
    "WHAT SHOULD THE OPERATOR DO NOW?",
)


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _parse_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    s = str(value).replace("Z", "+00:00")
    for candidate in (s, s[:19], s[:10]):
        try:
            dt = datetime.fromisoformat(candidate)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _newest(stamps: list) -> Optional[str]:
    """The newest owner-published timestamp among ``stamps`` (a SELECTION over
    values the owners stamped; never a fabricated reading of this module's own
    clock)."""
    best: Optional[datetime] = None
    best_raw: Optional[str] = None
    for raw in stamps or []:
        dt = _parse_dt(raw)
        if dt is not None and (best is None or dt > best):
            best, best_raw = dt, str(raw)
    return best_raw


def _safety() -> dict:
    return {
        "read_only": True, "performed_write": False, "wrote_to_ledger": False,
        "called_provider": False, "called_prediction": False,
        "ran_daily_close": False, "ran_research_refresh": False,
        "ran_portfolio_reassessment": False, "created_orders": False,
        "created_proposals": False, "approved_anything": False,
        "promoted_model": False, "automation_enabled": False,
        "broker_enabled": False, "paper_only": True, "manual_review": True,
        "safety_badges": list(SAFETY_BADGES),
    }


# --------------------------------------------------------------------------- #
# Block builders. Each is a verbatim projection of ONE owner family; a missing
# owner degrades to {"available": False} and a stale_components row.
# --------------------------------------------------------------------------- #
def _operational_book_block(portfolio_state: Optional[dict],
                            workflow: Optional[dict]) -> dict:
    ps = portfolio_state or {}
    op = (workflow or {}).get("operational_state") or {}
    dates = ps.get("dates") or {}
    capital = ps.get("capital") or {}
    book = ps.get("active_book") or {}
    return {
        "available": bool(ps),
        "state": ps.get("state"),
        "active_book_id": book.get("book_id") or op.get("active_book_id"),
        "active_book_name": book.get("book_name") or op.get("active_book_name"),
        "eligible_market_session": (dates.get("eligible_market_date")
                                    or op.get("eligible_market_date")),
        "operational_mark_date": op.get("desk_mark_date") or op.get("valuation_date"),
        "valuation_date": op.get("valuation_date"),
        "latest_completed_close_date": op.get("latest_completed_close_date"),
        "latest_close_status": op.get("latest_close_status"),
        "operational_close_valid": op.get("operational_close_valid"),
        "nav": capital.get("nav") if capital else op.get("nav"),
        "cash": capital.get("cash") if capital else op.get("cash"),
        "holdings_count": (len(ps.get("positions") or [])
                           if ps.get("positions") is not None
                           else op.get("holdings_count")),
        "pending_paper_orders": op.get("pending_orders"),
        "portfolio_state_hash": ps.get("state_hash"),
        "economic_state_hash": ps.get("economic_state_hash"),
        "book_freshness": op.get("operational_consistency_status"),
        "owner": COMPONENT_OWNERS["operational_book"],
    }


def _merged_last_run(ev: dict) -> dict:
    """The last event cycle's facts: the owner's ``last_run_summary`` (built by
    api.event_signal_refresh from its own persisted run payload) layered over
    the ``last_run`` pointer. A merge of two projections of the SAME owner
    artifact — never a computation."""
    merged = dict(ev.get("last_run") or {})
    merged.update({k: v for k, v in (ev.get("last_run_summary") or {}).items()
                   if v is not None})
    return merged


def _live_information_block(information_collection: Optional[dict],
                            event_refresh: Optional[dict],
                            reassessment: Optional[dict]) -> dict:
    ic = information_collection or {}
    svc = ic.get("service") or ic  # snapshot section wraps the lifecycle verdict
    ev = event_refresh or {}
    last_run = _merged_last_run(ev)
    recent = ev.get("recent_events") or []
    material = ev.get("material_events") or []
    # Owner-stamped event timestamps only. ``recent_events`` is owner-bounded, so
    # the count below states its bounded basis instead of claiming completeness.
    last_observation_at = _newest(
        [e.get("ingested_at") or e.get("published_at") for e in recent])
    last_material_at = _newest(
        [e.get("ingested_at") or e.get("published_at") for e in material])
    reassessed_at = ((reassessment or {}).get("artifact") or {}).get("generated_at")
    since = None
    if reassessed_at:
        anchor = _parse_dt(reassessed_at)
        if anchor is not None:
            since = sum(
                1 for e in material
                if (_parse_dt(e.get("ingested_at") or e.get("published_at"))
                    or anchor) > anchor)
    return {
        "available": bool(svc),
        "collection_running": (svc.get("service_state") == "RUNNING"),
        "collection_service_state": svc.get("service_state"),
        "worker_activity": svc.get("worker_activity"),
        "collection_reason": svc.get("reason"),
        "last_event_cycle": {
            "run_id": last_run.get("run_id"),
            "state": last_run.get("state") or ev.get("state"),
            "generated_at": last_run.get("generated_at"),
            # R54 finalization — the cycle owner's own recorded facts, so the
            # terminal token can never be read as more than it asserts: the
            # token records ARTIFACT EXISTENCE, never a recommended change.
            "reassessment_ran": last_run.get("reassessment_ran"),
            "proposal_built": last_run.get("proposal_built"),
            # Owner-summary flat keys first; the full run payload's nested
            # shape as fallback (both are the same owner artifact).
            "materiality_change_level": (
                last_run.get("materiality_change_level")
                if last_run.get("materiality_change_level") is not None
                else (last_run.get("materiality") or {}).get("change_level")),
            "reassessment_state": (
                last_run.get("reassessment_state")
                or (last_run.get("portfolio_reassessment")
                    or {}).get("reassessment_state")),
            "proposal_state": (
                last_run.get("proposal_state")
                or (last_run.get("target_portfolio")
                    or {}).get("proposal_state")),
            # R54.2 — WHICH immutable reassessment artifact this cycle's
            # conclusion became. A refused write leaves ``persisted`` False and
            # no id, and the operator sees that rather than a bare hash with no
            # evidence behind it.
            "reassessment_hash": (
                last_run.get("reassessment_hash")
                or (last_run.get("portfolio_reassessment")
                    or {}).get("reassessment_hash")),
            "reassessment_id": last_run.get("reassessment_id"),
            "reassessment_persisted": last_run.get("reassessment_persisted"),
            "reassessment_persistence_status": last_run.get(
                "reassessment_persistence_status"),
            "state_note": EVENT_CYCLE_PROPOSAL_NOTE,
            "advances_governed_decision": False,
            "advances_operational_mark": False,
            # R54.1 — what the ONE gate owner (api.portfolio_decision) concluded
            # about THIS cycle's candidate, recorded by that owner into the
            # cycle's own run payload at decision time.
            "governed_decision": last_run.get("governed_decision"),
            # R55.1 — the cycle owner's proof of whether it REACHED the gate.
            # Without it, "the gate declined" and "the gate was never called"
            # are the same absence, and the operator cannot tell a completed
            # chain from a broken one.
            "governance_gate_invoked": last_run.get("governance_gate_invoked"),
            "stage_timestamps": last_run.get("stage_timestamps"),
        },
        "last_observation_at": last_observation_at,
        "last_material_event_at": last_material_at,
        "material_event_count": ev.get("material_event_count"),
        "material_events_since_last_reassessment": since,
        "since_basis": ("owner-bounded recent material events newer than the "
                        "persisted reassessment artifact stamp"
                        if since is not None else None),
        "affected_current_holdings": ev.get("affected_holdings"),
        "owner": COMPONENT_OWNERS["live_information"],
    }


def _signal_state_block(event_refresh: Optional[dict], scoring: Optional[dict],
                        workflow: Optional[dict],
                        intraday_emission: Optional[dict] = None) -> dict:
    ev = event_refresh or {}
    last_run = _merged_last_run(ev)
    rs = (workflow or {}).get("research_state") or {}
    evid = (workflow or {}).get("evidence_state") or {}
    sc = scoring or {}
    ie = intraday_emission or {}
    rank_deltas = last_run.get("rank_deltas") or {}
    return {
        "available": bool(ev or sc or rs),
        "last_signal_refresh_at": last_run.get("generated_at"),
        "last_signal_refresh_state": last_run.get("state") or ev.get("state"),
        "latest_price_score_refresh_date": rs.get("latest_price_score_refresh_date"),
        "last_prediction_emission_date": evid.get("latest_snapshot_date"),
        "last_scoring_ranking_date": sc.get("ranking_date"),
        "scored_universe_count": sc.get("scored_count"),
        "ranking_snapshot_id": sc.get("input_contract_hash"),
        "scoring_status": sc.get("status"),
        # R54 finalization — scoring SCOPE and BASIS made explicit so a
        # ranking dated to the last closed session is never read as "nothing
        # was scored today". api.universe_scoring recomputes the FULL eligible
        # universe on every build (there is no partial scorer), and its
        # ranking_date is the owned model-input as-of date — the point-in-time
        # DATA basis, never the wall-clock recompute time.
        "scoring_basis": {
            "scope": "FULL_UNIVERSE_RECOMPUTE",
            "basis": ("ranking_date is the owned model-input as-of date "
                      "(point-in-time data basis), not the wall-clock "
                      "recompute time; the event cycle recomputes it through "
                      "the one owner whenever an input is invalidated"),
            "owner": "api.universe_scoring",
        },
        "last_full_universe_scoring": {
            "ranking_basis_date": sc.get("ranking_date"),
            "scored_count": sc.get("scored_count"),
            "input_contract_hash": sc.get("input_contract_hash"),
            "status": sc.get("status"),
        },
        "last_incremental_signal_refresh": {
            "at": last_run.get("generated_at"),
            "state": last_run.get("state"),
            "calculations_refreshed": last_run.get("calculations_refreshed"),
            "affected_names_refreshed": last_run.get("affected_entities"),
            # Preferred: the cycle owner's own count from its persisted run
            # payload; fallback: a count of the same owner-bounded row list.
            "held_rank_delta_rows": (
                last_run.get("held_rank_delta_rows")
                if last_run.get("held_rank_delta_rows") is not None
                else (len(rank_deltas.get("rows") or [])
                      if rank_deltas else None)),
            "prior_ranking_available": (
                last_run.get("prior_ranking_available")
                if last_run.get("prior_ranking_available") is not None
                else rank_deltas.get("prior_available")),
        },
        # The facts no owner persists — declared, never fabricated.
        "not_persisted_facts": [
            ("affected_names_rescored — api.universe_scoring recomputes the "
             "full universe; per-name partial rescoring is not a persisted "
             "concept"),
            ("latest_rank_change_timestamp — rank deltas are measured per "
             "event cycle against the prior persisted snapshot, not stamped "
             "as a change series"),
        ],
        # R54 finalization — TWO DISTINCT forward-evidence identities. The
        # daily governed TRUE_FORWARD bundle (api.forward_prediction_skill,
        # captured by the Daily Close) and the R53.1 intraday prospective
        # emission (research ledger, evidence_class PROSPECTIVE_INTRADAY) are
        # never summed and never interchanged.
        "latest_governed_true_forward_date": evid.get("latest_snapshot_date"),
        "governed_true_forward_owner": "api.forward_prediction_skill",
        "latest_intraday_prospective_emission": (dict(ie) if ie else None),
        "signal_freshness": {
            "research_inputs_current": rs.get("research_inputs_current"),
            "stale_source_ids": rs.get("stale_source_ids"),
            "missing_source_ids": rs.get("missing_source_ids"),
        },
        "owner": COMPONENT_OWNERS["signal_state"],
    }


def _reassessment_block(reassessment: Optional[dict], workflow: Optional[dict],
                        event_refresh: Optional[dict]) -> dict:
    r = reassessment or {}
    dec = r.get("decision") or {}
    lane = (workflow or {}).get("portfolio_reassessment") or {}
    rcs = (workflow or {}).get("research_cycle_state") or {}
    pas = (workflow or {}).get("portfolio_assessment_state") or {}
    hocp = (workflow or {}).get("holding_opportunity_cost_presentation") or {}
    # R54 finalization — the Track-B SETTLED-AWARE presentation built by the
    # reassessment owner WITH the canonical decision lane
    # (api.workflow_state passes decision_lane into build_presentation). A raw
    # kernel state of PROPOSAL_READY honestly records that the reassessment
    # ASKED for a target; when the decision owner has already settled the
    # question (e.g. HOLD_CURRENT_BOOK) the presentation's operator_state says
    # PORTFOLIO_DECISION_SETTLED — and THAT is what the operator must read.
    pres = (workflow or {}).get("portfolio_reassessment_presentation") or {}
    last_run = _merged_last_run(event_refresh or {})
    alternatives = r.get("strongest_alternatives") or []
    best = alternatives[0] if alternatives else None
    # Trigger provenance is read from its owners: the artifact class decided by
    # api.holding_opportunity_cost (via the workflow read) says WHICH path
    # produced the current artifacts; the event cycle's own recorded state says
    # what the last incremental trigger concluded. Nothing is inferred here.
    artifact_class = rcs.get("opportunity_cost_artifact_class")
    trigger = {
        "artifact_class": artifact_class,
        "producer_owner": rcs.get("opportunity_cost_producer_owner"),
        "governed_daily_cycle": bool(rcs.get("governed_research_evidence_current")),
        "last_event_cycle_state": last_run.get("state"),
    }
    return {
        "available": bool(r),
        "state": r.get("state"),
        "operator_state": pres.get("operator_state") or None,
        "decision_settled": pres.get("decision_settled"),
        "settled_decision_state": pres.get("settled_decision_state"),
        "settled_decision_owner": pres.get("settled_decision_owner"),
        "operator_task": pres.get("task"),
        "operator_next_action": pres.get("next_action"),
        "presentation_owner": ("api.portfolio_reassessment.build_presentation "
                               "(settled-aware, via api.workflow_state)"),
        "last_reassessment_at": (r.get("artifact") or {}).get("generated_at"),
        "reassessment_session": r.get("eligible_market_date"),
        "reassessment_id": r.get("reassessment_id"),
        "reassessment_hash": r.get("reassessment_hash"),
        "reassessment_trigger": trigger,
        "holdings_evaluated": dec.get("holdings_evaluated"),
        "alternatives_evaluated": len(alternatives) or None,
        "current_decision": dec.get("reassessment_state") or r.get("state"),
        "decision_reason": r.get("explanation"),
        "expected_net_improvement": dec.get("expected_net_improvement"),
        "net_improvement_hurdle": dec.get("net_improvement_hurdle"),
        "expected_one_way_turnover": dec.get("expected_one_way_turnover"),
        "expected_transaction_cost_usd": dec.get("expected_transaction_cost_usd"),
        "hoc_summary": {
            "state": hocp.get("state"),
            "recommendation_counts": hocp.get("recommendation_counts"),
            "assessment_hash": hocp.get("assessment_hash"),
        },
        "best_replacement_summary": best,
        "reassessment_freshness": pas.get("assessment_status"),
        # R54 finalization — the owner facts BEHIND the currency verdict, so an
        # OVERDUE badge can never be misread as "the eligible session was not
        # reassessed": the classification and every input to it belong to
        # api.workflow_state.classify_assessment (over the legacy gate's
        # scheduled-review clock), and live event cycles never advance it.
        "reassessment_freshness_detail": {
            "current_for_eligible_session": pas.get(
                "latest_assessment_current_for_eligible_session"),
            "assessment_age_sessions": pas.get("assessment_age_sessions"),
            "next_scheduled_review_date": pas.get("next_scheduled_review_date"),
            "review_due": pas.get("review_due"),
            "review_overdue": pas.get("review_overdue"),
            "currency_owner": "api.workflow_state.classify_assessment",
            "schedule_owner": ("api.daily_action_gate "
                               "(legacy scheduled-review clock)"),
            "advanced_by_live_event_cycles": False,
            # Release 55 — the AUTHORITY BOUNDARY between the two clocks, carried
            # verbatim from the currency owner. ``schedule_decided_status`` is the
            # one field a surface needs to know whether a non-CURRENT token names
            # a real portfolio obligation or only the model-recalibration
            # checkpoint. Absent (pre-R55 payload) means "not stated", never "no".
            "status_decided_by": pas.get("assessment_status_decided_by"),
            "schedule_decided_status": pas.get(
                "schedule_decided_assessment_status"),
            "schedule_governs_portfolio_cadence": pas.get(
                "scheduled_review_governs_portfolio_cadence"),
            "schedule_scope": pas.get("scheduled_review_scope"),
            "schedule_scope_owner": pas.get("scheduled_review_scope_owner"),
            "schedule_scope_note": pas.get("scheduled_review_scope_note"),
            "schedule_is_compatibility_only": pas.get(
                "scheduled_review_is_compatibility_only"),
            "portfolio_reassessment_cadence": pas.get(
                "portfolio_reassessment_cadence"),
        },
        "proposal_required": dec.get("proposal_required"),
        "workflow_lane_state": lane.get("state") or lane.get("operator_state"),
        "owner": COMPONENT_OWNERS["portfolio_reassessment"],
    }


def _target_proposal_block(constrained: Optional[dict],
                           workflow: Optional[dict]) -> dict:
    c = constrained or {}
    econ = c.get("switching_economics") or {}
    best = c.get("best_feasible_target") or {}
    approval = c.get("approval") or {}
    risk = c.get("risk") or {}
    return {
        "available": bool(c),
        "target_state": c.get("outcome") or c.get("outcome_state"),
        "outcome_vocabulary": c.get("outcome_vocabulary"),
        "feasible_target_exists": c.get("feasible_target_exists"),
        "proposal_state": ((workflow or {}).get("reallocation_operator_state")
                           or c.get("outcome_state")),
        "approval_state": approval.get("portfolio_decision_state"),
        "expected_improvement": (econ.get("expected_net_improvement")
                                 if "expected_net_improvement" in econ
                                 else econ.get("net_improvement")),
        "switching_hurdle": (econ.get("net_improvement_hurdle")
                             or econ.get("switching_hurdle")),
        "clears_hurdle": (econ.get("clears_hurdle")
                          if "clears_hurdle" in econ
                          else econ.get("clears_switching_hurdle")),
        "turnover": c.get("turnover"),
        "estimated_cost": (econ.get("expected_transaction_cost_usd")
                           or econ.get("estimated_cost_usd")),
        "risk": risk,
        "target_position_count": best.get("position_count"),
        "target_snapshot_id": ((workflow or {}).get("portfolio_decision_state")
                               or {}).get("proposal_hash"),
        "decision_provenance": ((workflow or {}).get("portfolio_decision_state")
                                or {}).get("decision_provenance"),
        "owner": COMPONENT_OWNERS["target_proposal"],
    }


def _live_intraday_assessment_block(live_information: dict,
                                    reassessment: dict) -> dict:
    """R54.1 — the LIVE intraday assessment, alone and clearly labelled.

    This is current signal state produced by ``api.event_signal_refresh``. It is
    real, it is displayable, and it is NOT the authoritative decision: only a
    candidate that passes the intraday governance gate owned by
    ``api.portfolio_decision`` may become that.
    """
    cycle = live_information.get("last_event_cycle") or {}
    return {
        "available": bool(cycle.get("run_id") or cycle.get("state")),
        "state": cycle.get("state"),
        "at": cycle.get("generated_at"),
        "run_id": cycle.get("run_id"),
        "reassessment_ran": cycle.get("reassessment_ran"),
        "proposal_built": cycle.get("proposal_built"),
        "materiality_change_level": cycle.get("materiality_change_level"),
        "reassessment_state": cycle.get("reassessment_state"),
        "proposal_state": cycle.get("proposal_state"),
        # R54.2 — the immutable artifact behind this live conclusion, projected
        # verbatim from the cycle owner. Never re-derived here.
        "reassessment_hash": cycle.get("reassessment_hash"),
        "reassessment_id": cycle.get("reassessment_id"),
        "reassessment_persisted": cycle.get("reassessment_persisted"),
        "reassessment_persistence_status": cycle.get(
            "reassessment_persistence_status"),
        "last_material_event_at": live_information.get("last_material_event_at"),
        "reassessment_operator_state": reassessment.get("operator_state"),
        "provenance": "LIVE_PRE_DRC_SIGNAL",
        "is_authoritative_decision": False,
        "advances_governed_decision": False,
        "advances_operational_mark": False,
        "state_note": EVENT_CYCLE_PROPOSAL_NOTE,
        "owner": COMPONENT_OWNERS["latest_live_intraday_assessment"],
    }


def _governed_decision_block(governed_decision: Optional[dict]) -> dict:
    """R54.1 — the LATEST GOVERNED portfolio decision, verbatim from the ONE
    decision owner. Every value here was decided by ``api.portfolio_decision``;
    this module selects nothing and re-derives nothing."""
    g = governed_decision or {}
    econ = g.get("switching_economics") or {}
    ev = g.get("evidence_provenance") or {}
    return {
        "available": bool(g.get("available")),
        "decision": g.get("decision"),
        "decision_vocabulary": g.get("decision_vocabulary"),
        "timestamp": g.get("decided_at"),
        "provenance": g.get("provenance"),
        "provenance_vocabulary": g.get("provenance_vocabulary"),
        "record_id": g.get("record_id"),
        "eligible_market_session": g.get("eligible_market_session"),
        "trigger": {
            "event_cycle_run_id": ev.get("event_cycle_run_id"),
            "event_cycle_state": ev.get("event_cycle_state"),
            "materiality_change_level": ev.get("materiality_change_level"),
            "materiality_trigger_fingerprint": ev.get(
                "materiality_trigger_fingerprint"),
        },
        "holdings_reviewed": ev.get("hoc_holdings_reviewed"),
        "alternatives_reviewed": len(g.get("position_recommendations") or []) or None,
        "position_recommendations": list(g.get("position_recommendations") or []),
        "switching_economics": {
            "expected_net_improvement": econ.get("score_improvement_net_of_cost"),
            "switching_hurdle": econ.get("switching_hurdle"),
            "clears_switching_hurdle": econ.get("clears_switching_hurdle"),
            "one_way_turnover": econ.get("one_way_turnover"),
            "estimated_transaction_cost": econ.get("estimated_transaction_cost"),
            "concentration_before": econ.get("concentration_before"),
            "concentration_after": econ.get("concentration_after"),
            "portfolio_volatility_before": econ.get("portfolio_volatility_before"),
            "portfolio_volatility_after": econ.get("portfolio_volatility_after"),
            "owner": "engine.constrained_reallocation",
        },
        "manual_review_required": g.get("manual_review_required"),
        "supersedes_decision_id": g.get("supersedes_decision_id"),
        "governing_evidence_identity": g.get("identity") or {},
        "zero_base": g.get("zero_base") or {},
        "gate": g.get("gate") or {},
        "persisted": g.get("persisted"),
        # R55.1 — the decision owner's own account of HOW the decision is held.
        # ``persisted: false`` beside a real record_id was truthful but read as a
        # contradiction; these say which of the two live states it is, and that
        # the decision is retrievable through the canonical owner either way.
        "persistence_status": g.get("persistence_status"),
        "persistence_status_vocabulary": list(
            g.get("persistence_status_vocabulary") or []),
        "persistence_detail": g.get("persistence_detail"),
        "is_ledger_row": g.get("is_ledger_row"),
        "retrievable_through_owner": g.get("retrievable_through_owner"),
        "retrievability_owner": g.get("retrievability_owner"),
        "backfilled": g.get("backfilled"),
        "approval_required_token": g.get("approval_required_token"),
        "creates_orders": False,
        "approves_anything": False,
        "advances_operational_mark": False,
        "owner": COMPONENT_OWNERS["latest_governed_portfolio_decision"],
    }


def _classify_governance(cycle: Optional[dict]) -> dict:
    """Ask the GOVERNANCE OWNER for this cycle's terminal disposition.

    Release 55.1 delegates rather than inferring here: whether a governance
    verdict was required, and what it was, is ``api.portfolio_decision``'s
    question. An unavailable owner degrades to an empty dict, which leaves the
    disposition unproven and acceptance MISSING — fail-closed, never fail-open.
    """
    try:
        from paper_trader.api import portfolio_decision as pdec
        return pdec.classify_intraday_governance(event_cycle=cycle or {}) or {}
    except Exception:  # noqa: BLE001 — an unreadable owner proves nothing
        return {}


def _intraday_governance_block(live_information: dict,
                               governed_decision: Optional[dict]) -> dict:
    """R54.1 — why the latest live candidate did or did not become governed.

    Read verbatim from the record the decision owner wrote into the event
    cycle's own run payload at decision time. The operator can therefore see a
    live signal AND the exact classified reasons it was not promoted.

    RELEASE 55.1 — the block now also carries that owner's TERMINAL DISPOSITION
    for the cycle. Before it did, a cycle that correctly required no verdict was
    indistinguishable from one whose gate never ran: both surfaced as an absent
    verdict, and acceptance called both MISSING.
    """
    cycle = live_information.get("last_event_cycle") or {}
    gd = cycle.get("governed_decision") or {}
    disp = _classify_governance(cycle)
    return {
        # R55.1 — a proven terminal disposition IS an available governance
        # answer, even when the gate correctly never needed to run.
        "available": bool(gd) or bool(disp.get("terminal")),
        "disposition": disp.get("disposition"),
        "disposition_vocabulary": list(disp.get("disposition_vocabulary") or []),
        "terminal": disp.get("terminal"),
        "required": disp.get("required"),
        "reason": disp.get("reason"),
        "reason_detail": disp.get("reason_detail"),
        "gate_invoked_by_cycle": disp.get("gate_invoked_by_cycle"),
        "event_cycle_run_id": disp.get("event_cycle_run_id"),
        "event_cycle_state": disp.get("event_cycle_state"),
        "candidate_reassessment_id": disp.get("candidate_reassessment_id"),
        "disposition_at": disp.get("at"),
        "disposition_owner": disp.get("owner"),
        "invocation_contract": disp.get("invocation_contract"),
        "gate_owner": "api.portfolio_decision",
        "evaluated": gd.get("evaluated"),
        "verdict": gd.get("verdict"),
        "candidate_decision": gd.get("decision"),
        "candidate_identity_hash": gd.get("candidate_identity_hash"),
        "promoted_to_governed": bool(gd.get("recorded")),
        "governed_record_id": gd.get("record_id"),
        "withheld_reason_codes": list(gd.get("withheld_reason_codes") or []),
        "failing_checks": list(gd.get("failing_checks") or []),
        "standing_governed_decision": (governed_decision or {}).get("record_id"),
        "manual_review_required_for_change": True,
        "created_orders": False,
        "approved_anything": False,
        "advances_operational_mark": False,
        "owner": COMPONENT_OWNERS["intraday_governance"],
    }


# --------------------------------------------------------------------------- #
# R54.2.4 (Defects 7 + 8) — LANE B: the latest LIVE / INTRADAY reassessment as a
# first-class, self-explaining operator answer. ONE composed projection over the
# owners' already-recorded facts (the event cycle's own run payload, the R54.1
# gate verdict the decision owner wrote into it, and the reassessment head) —
# nothing here re-evaluates the gate, re-scores anything, or advances authority.
# --------------------------------------------------------------------------- #
LANE_CONCLUSION_HOLD = "HOLD"
LANE_CONCLUSION_CHANGE = "CHANGE"
LANE_CONCLUSION_PROPOSAL_AVAILABLE = "PROPOSAL_AVAILABLE"
LANE_CONCLUSION_NOT_MATERIAL = "INFORMATION_NOT_MATERIAL"
#: R55.1 — the signal refresh completed and the materiality gate concluded no
#: portfolio reassessment was needed. A successful terminal no-op with a real
#: name. Before this token such a cycle fell through to UNKNOWN, so a healthy
#: no-op was displayed as "Latest reassessment -> UNKNOWN": a reassessment that
#: never ran, presented as one whose conclusion could not be read.
LANE_CONCLUSION_NO_REASSESSMENT = "NO_REASSESSMENT_REQUIRED"
LANE_CONCLUSION_UNKNOWN = "UNKNOWN"
LANE_CONCLUSION_VOCAB = (LANE_CONCLUSION_HOLD, LANE_CONCLUSION_CHANGE,
                         LANE_CONCLUSION_PROPOSAL_AVAILABLE,
                         LANE_CONCLUSION_NOT_MATERIAL,
                         LANE_CONCLUSION_NO_REASSESSMENT,
                         LANE_CONCLUSION_UNKNOWN)

#: The conclusions that mean NO NEW PORTFOLIO REASSESSMENT EXISTS for this
#: cycle. Nothing downstream may present a reassessment conclusion for them.
LANE_NO_REASSESSMENT_CONCLUSIONS = (LANE_CONCLUSION_NOT_MATERIAL,
                                    LANE_CONCLUSION_NO_REASSESSMENT)

LANE_GOV_GOVERNED = "GOVERNED"
LANE_GOV_WITHHELD = "WITHHELD"
LANE_GOV_ELIGIBLE = "ELIGIBLE"
LANE_GOV_NOT_REQUIRED = "NOT_REQUIRED"
#: R55.1 — the gate ran and concluded no promotion was warranted. Distinct from
#: ELIGIBLE, which now means only "a candidate exists and the gate has not
#: spoken" — an unproven state, never a successful one.
LANE_GOV_EVALUATED_NO_PROMOTION = "EVALUATED_NO_PROMOTION"
LANE_GOV_UNKNOWN = "UNKNOWN"
LANE_GOVERNANCE_VOCAB = (LANE_GOV_GOVERNED, LANE_GOV_WITHHELD, LANE_GOV_ELIGIBLE,
                         LANE_GOV_NOT_REQUIRED, LANE_GOV_EVALUATED_NO_PROMOTION,
                         LANE_GOV_UNKNOWN)

#: R55.1 — the ONE map from the governance owner's terminal disposition to this
#: lane's display vocabulary. The lane no longer decides its own governance
#: word: ``api.portfolio_decision`` decides, and this only renames.
GOVERNANCE_DISPOSITION_TO_LANE = {
    "PROMOTED": LANE_GOV_GOVERNED,
    "WITHHELD": LANE_GOV_WITHHELD,
    "EVALUATED_NO_PROMOTION": LANE_GOV_EVALUATED_NO_PROMOTION,
    "NOT_REQUIRED_NO_NEW_INFORMATION": LANE_GOV_NOT_REQUIRED,
    "INCOMPLETE": LANE_GOV_UNKNOWN,
}


# --------------------------------------------------------------------------- #
# R55.1 — THE OPERATOR SENTENCES FOR LANE B, composed by the backend.
#
# The UI must not decide whether a reassessment occurred, nor what a cycle
# state means. It renders these three strings. Each is a total function of the
# owner tokens above, so a state nobody anticipated still produces an honest
# sentence rather than a blank or an invented one.
# --------------------------------------------------------------------------- #
_LANE_HEADLINES = {
    LANE_CONCLUSION_NO_REASSESSMENT: "Latest signal refresh: no new information",
    LANE_CONCLUSION_NOT_MATERIAL: (
        "Latest signal refresh: new information, not material to the portfolio"),
    LANE_CONCLUSION_HOLD: "Latest live reassessment: no portfolio change",
    LANE_CONCLUSION_CHANGE: "Latest live reassessment: change indicated",
    LANE_CONCLUSION_PROPOSAL_AVAILABLE: (
        "Latest live reassessment: target portfolio built for manual review"),
}

_LANE_REASSESSMENT_SUMMARIES = {
    LANE_CONCLUSION_NO_REASSESSMENT: "No new portfolio reassessment was required",
    LANE_CONCLUSION_NOT_MATERIAL: "No new portfolio reassessment was required",
    LANE_CONCLUSION_HOLD: "A portfolio reassessment ran and concluded no change",
    LANE_CONCLUSION_CHANGE: "A portfolio reassessment ran and indicated a change",
    LANE_CONCLUSION_PROPOSAL_AVAILABLE: (
        "A portfolio reassessment ran and produced a target for manual review"),
}


def _lane_headline(conclusion: str, cycle_state: Any) -> str:
    """One sentence naming what the latest live cycle actually was."""
    known = _LANE_HEADLINES.get(conclusion)
    if known:
        return known
    return ("Latest live cycle: outcome could not be read (%s)"
            % (cycle_state or "no state recorded"))


def _lane_reassessment_summary(conclusion: str, ran: bool) -> str:
    """One sentence about the REASSESSMENT — never a conclusion for a
    reassessment that did not run."""
    known = _LANE_REASSESSMENT_SUMMARIES.get(conclusion)
    if known:
        return known
    return ("A portfolio reassessment ran but its conclusion was not recorded"
            if ran else
            "Whether a portfolio reassessment was required could not be read")


def _lane_governed_summary(promoted: bool, record_id: Any) -> str:
    """One sentence about the GOVERNED decision's standing after this cycle."""
    if promoted:
        return ("This cycle became the latest governed portfolio decision (%s)"
                % (record_id or "record id not recorded"))
    return "Standing governed decision unchanged"


def _live_reassessment_lane_block(*, live_information: dict, signal_state: dict,
                                  reassessment: dict, workflow: Optional[dict],
                                  governed_decision: Optional[dict]) -> dict:
    cycle = live_information.get("last_event_cycle") or {}
    gd = cycle.get("governed_decision") or {}
    cycle_state = str(cycle.get("state") or "")
    reassess_state = str(cycle.get("reassessment_state") or "")

    # WHAT the live run concluded — from the cycle owner's own recorded tokens.
    #
    # R55.1 — a cycle that the owner recorded as having run NO reassessment can
    # never carry a reassessment conclusion. It is named for what it is, and
    # never falls through to UNKNOWN: "no new information" is an answer, and
    # UNKNOWN must stay reserved for a cycle whose outcome cannot be read.
    ran = cycle.get("reassessment_ran")
    if cycle_state == "INFORMATION_NOT_MATERIAL":
        conclusion = LANE_CONCLUSION_NOT_MATERIAL
    elif ran is False:
        conclusion = LANE_CONCLUSION_NO_REASSESSMENT
    elif reassess_state == "CURRENT_NO_CHANGE":
        conclusion = LANE_CONCLUSION_HOLD
    elif reassess_state == "PROPOSAL_READY":
        conclusion = (LANE_CONCLUSION_PROPOSAL_AVAILABLE
                      if cycle.get("proposal_built")
                      else LANE_CONCLUSION_CHANGE)
    else:
        conclusion = LANE_CONCLUSION_UNKNOWN
    reassessment_ran_here = conclusion not in LANE_NO_REASSESSMENT_CONCLUSIONS

    # WHETHER governance passed — R55.1 asks the GOVERNANCE OWNER for its
    # terminal disposition and only renames it for display. This lane holds no
    # governance rule of its own: before R55.1 it inferred NOT_REQUIRED here,
    # which is the right answer reached by the wrong module, and it had no way
    # at all to express "the gate ran and did not promote".
    disposition = _classify_governance(cycle)
    governance = GOVERNANCE_DISPOSITION_TO_LANE.get(
        disposition.get("disposition"), LANE_GOV_UNKNOWN)
    governance_note = (disposition.get("reason_detail")
                       or "The cycle payload records no governance facts.")
    if governance == LANE_GOV_GOVERNED and gd.get("record_id"):
        governance_note = "%s Record %s." % (governance_note, gd.get("record_id"))

    promoted = bool(gd.get("recorded"))
    attention = ((workflow or {}).get("portfolio_attention") or {})

    # The lane's own economics: the reassessment head's release-set estimate,
    # and ONLY when it is provably this cycle's artifact (ids match). Scope is
    # named; a non-matching head yields an honest None, never a borrowed number.
    same_artifact = bool(
        cycle.get("reassessment_id")
        and cycle.get("reassessment_id") == reassessment.get("reassessment_id"))
    economics = ({
        "scope": "HOC_RELEASE_SET_ESTIMATE",
        "scope_note": ("Pre-proposal, non-binding release-set estimate from the "
                       "reassessment artifact this cycle produced."),
        "expected_net_improvement": reassessment.get("expected_net_improvement"),
        "net_improvement_hurdle": reassessment.get("net_improvement_hurdle"),
        "expected_one_way_turnover": reassessment.get("expected_one_way_turnover"),
        "expected_transaction_cost_usd": reassessment.get(
            "expected_transaction_cost_usd"),
        "owner": "engine.portfolio_reassessment via api.portfolio_reassessment",
    } if same_artifact else None)

    return {
        "available": bool(cycle.get("run_id") or cycle_state),
        "lane": "LIVE_INTRADAY_REASSESSMENT",
        "lane_label": "LATEST LIVE / INTRADAY REASSESSMENT",
        "provenance": "LIVE_PRE_DRC_SIGNAL",
        "run_id": cycle.get("run_id"),
        "at": cycle.get("generated_at"),
        "trigger": cycle.get("materiality_change_level"),
        "trigger_owner": "engine.event_materiality via api.event_signal_refresh",
        "material_event_count": live_information.get("material_event_count"),
        "material_events_since_last_reassessment": live_information.get(
            "material_events_since_last_reassessment"),
        "affected_holdings": live_information.get("affected_current_holdings"),
        "last_material_event_at": live_information.get("last_material_event_at"),
        "scoring_basis_date": signal_state.get("last_scoring_ranking_date"),
        "hoc_completed_at": (cycle.get("stage_timestamps")
                             or {}).get("hoc_completed_at"),
        "reassessment_id": cycle.get("reassessment_id"),
        "reassessment_hash": cycle.get("reassessment_hash"),
        "reassessment_persisted": cycle.get("reassessment_persisted"),
        "reassessment_persistence_status": cycle.get(
            "reassessment_persistence_status"),
        "candidate_conclusion": conclusion,
        "candidate_conclusion_vocabulary": list(LANE_CONCLUSION_VOCAB),
        # R55.1 — THE backend's answer to "did a portfolio reassessment happen
        # on this cycle?". The UI reads this flag; it never derives the answer
        # from a state token, a missing hash or an empty conclusion.
        "reassessment_ran": reassessment_ran_here,
        "reassessment_ran_owner": "api.event_signal_refresh (materiality gate)",
        # R55.1 — the three sentences the operator surface renders VERBATIM.
        # Composed here, by the owner of the composition, so the presentation
        # layer performs no interpretation of its own.
        "headline": _lane_headline(conclusion, cycle_state),
        "reassessment_summary": _lane_reassessment_summary(
            conclusion, reassessment_ran_here),
        "governed_summary": _lane_governed_summary(promoted, gd.get("record_id")),
        "economics": economics,
        "governance_state": governance,
        "governance_state_vocabulary": list(LANE_GOVERNANCE_VOCAB),
        "governance_note": governance_note,
        # R55.1 — the governance owner's own terminal disposition, carried
        # verbatim beside the lane's display rename of it.
        "governance_disposition": disposition.get("disposition"),
        "governance_disposition_terminal": disposition.get("terminal"),
        "governance_required": disposition.get("required"),
        "governance_reason": disposition.get("reason"),
        "governance_disposition_owner": disposition.get("owner"),
        "governance_withheld_reason_codes": list(
            gd.get("withheld_reason_codes") or []),
        "governance_failing_checks": list(gd.get("failing_checks") or []),
        "governed_record_id": gd.get("record_id"),
        "promoted_to_governed": promoted,
        # A live result WITHOUT a recorded governed promotion never supersedes
        # the standing decision — the R54.2.3.2 authority rule, echoed.
        "supersedes_standing_decision": promoted,
        "standing_governed_decision_id": (governed_decision or {}).get(
            "record_id"),
        "manual_review_available": bool(attention.get("review_required")),
        "is_authoritative_decision": promoted,
        "advances_operational_mark": False,
        "creates_orders": False,
        "approves_anything": False,
        "gate_owner": "api.portfolio_decision",
        "owner": OWNER,
        "composed_from": ["api.event_signal_refresh",
                          "api.portfolio_decision (gate record on the cycle "
                          "payload)", "api.portfolio_reassessment"],
        "note": ("Lane B — the live/intraday research lane. It never outranks "
                 "the governed portfolio decision (Lane A) unless the R54.1 "
                 "gate recorded a governed promotion, and it never creates an "
                 "order or an approval."),
    }


def _stages_not_required(cycle: Optional[dict]) -> list:
    """Ask the CYCLE OWNER which decision-chain stages it legitimately skipped.

    Release 55.1 refuses to decide this here: only ``api.event_signal_refresh``
    ran the materiality gate, so only it can say a stage was not required. An
    unavailable owner excuses nothing, so every unstamped stage stays MISSING.
    """
    try:
        from paper_trader.api import event_signal_refresh as esr
        return list(esr.stages_not_required(cycle or {}) or [])
    except Exception:  # noqa: BLE001 — an unreadable owner excuses nothing
        return []


def _measure_latency(**kwargs) -> Optional[dict]:
    """Call the LATENCY OWNER's own measurement function.

    Release 55 delegates rather than subtracting timestamps here: the intervals,
    the missing-endpoint naming and the completeness verdict all stay with
    ``api.event_signal_refresh.measure_decision_latency``. An unavailable owner
    module degrades to None and the block reports MISSING, exactly as it would
    for an unstamped stage.
    """
    try:
        from paper_trader.api import event_signal_refresh as esr
        return esr.measure_decision_latency(**kwargs)
    except Exception:  # noqa: BLE001 — a missing owner is MISSING, never invented
        return None


def _decision_latency_block(governed_decision: Optional[dict],
                            live_information: dict) -> dict:
    """R54.1 — measured, never modelled. Every value is the latency owner's
    (``api.event_signal_refresh``) own measurement over persisted stamps; a
    stage that persists no timestamp is NAMED, never filled in.

    RELEASE 55 — MEASURE THE CHAIN THAT ACTUALLY RAN.

    Before R55 this block read the latency record that a GOVERNED INTRADAY
    PROMOTION carries. That record exists only when the R54.1 gate promoted a
    cycle, so on 2026-09-03 — twenty material events, a full-universe rescore, a
    completed opportunity-cost assessment and a completed reassessment, none of
    it promoted because the conclusion was HOLD — the whole block reported
    ``available: false`` and the operator could not see where any of the time
    went, even though ``api.event_signal_refresh`` had persisted four real stage
    timestamps on the very same payload.

    The repair adds no measurement of its own: when no governed record carries a
    latency, this block asks the SAME owner function to measure the SAME
    persisted stamps. The governed endpoint is genuinely absent in that case, so
    ``reassessment_to_governed_seconds`` and ``observation_to_governed_seconds``
    stay None and ``governed_decision_persisted_at`` is named in
    ``missing_measurements``. Nothing is manufactured; a promoted cycle's own
    record still wins outright.
    """
    lat = (governed_decision or {}).get("latency") or {}
    cycle = live_information.get("last_event_cycle") or {}
    basis = "GOVERNED_DECISION_LATENCY_RECORD"
    if not lat:
        stamps = cycle.get("stage_timestamps") or {}
        # The observation stamp is the event fabric's own; the newest material
        # observation the live block already SELECTED (never a clock read here).
        observed = (live_information.get("last_material_event_at")
                    or live_information.get("last_observation_at"))
        if stamps or observed:
            measured = _measure_latency(
                stage_timestamps=stamps,
                event_cycle_started_at=cycle.get("generated_at"),
                observation_received_at=observed,
                governance_gate_completed_at=None,
                governed_decision_persisted_at=None,
                # R55.1 — the CYCLE owner names which stages it legitimately
                # never ran, so a correct terminal no-op is not reported as a
                # broken chain. Only the owner may excuse a stage.
                not_required_stages=_stages_not_required(cycle))
            if measured:
                lat = measured
                basis = "LIVE_EVENT_CYCLE_STAGE_TIMESTAMPS"
    return {
        "available": bool(lat),
        "measurement_owner": "api.event_signal_refresh",
        "measurement_basis": basis if lat else None,
        "measurement_basis_vocabulary": [
            "GOVERNED_DECISION_LATENCY_RECORD",
            "LIVE_EVENT_CYCLE_STAGE_TIMESTAMPS"],
        "timestamps": lat.get("timestamps") or {},
        "observation_to_signal_seconds": lat.get("observation_to_signal_seconds"),
        "signal_to_reassessment_seconds": lat.get("signal_to_reassessment_seconds"),
        "reassessment_to_governed_seconds": lat.get(
            "reassessment_to_governed_seconds"),
        "observation_to_governed_seconds": lat.get(
            "observation_to_governed_seconds"),
        "latency_measurement_complete": lat.get("latency_measurement_complete"),
        "missing_measurements": list(lat.get("missing_measurements") or []),
        # R55.1 — NOT_REQUIRED is not MISSING. A stage an owner proved this
        # cycle never needed is named separately and never zero-filled.
        "not_required_measurements": list(
            lat.get("not_required_measurements") or []),
        "stage_dispositions": lat.get("stage_dispositions") or {},
        "interval_dispositions": lat.get("interval_dispositions") or {},
        "disposition_vocabulary": list(lat.get("disposition_vocabulary") or []),
        "never_zero_fills_an_unexecuted_stage": True,
        "last_event_cycle_at": cycle.get("generated_at"),
        "computed_here": False,
        "owner": COMPONENT_OWNERS["decision_latency"],
    }


def _research_governance_block(workflow: Optional[dict], scoring: Optional[dict],
                               runtime_health: Optional[dict]) -> dict:
    w = workflow or {}
    sc = scoring or {}
    rt = runtime_health or {}
    return {
        "available": bool(w or sc or rt),
        "active_model": sc.get("primary_model_id"),
        "champion_model_id": sc.get("champion_model_id"),
        "challenger_model_ids": sc.get("challenger_model_ids"),
        "model_governance_state": w.get("model_governance_state"),
        "model_review_state": w.get("model_review_state"),
        "forward_evidence_state": w.get("evidence_state"),
        "recalibration_state": (w.get("model_review") or {}).get(
            "model_review_state") or w.get("model_review_state"),
        "automatic_promotion_allowed": False,
        "research_runtime": {
            "state": rt.get("state"),
            "owner": rt.get("owner"),
            "n_runs_total": rt.get("n_runs_total"),
        },
        "owner": COMPONENT_OWNERS["research_governance"],
    }


def _execution_safety_block(constrained: Optional[dict], rebalance: Optional[dict],
                            workflow: Optional[dict]) -> dict:
    approval = (constrained or {}).get("approval") or {}
    execution = (constrained or {}).get("execution") or {}
    primary = (workflow or {}).get("primary_action") or {}
    op = (workflow or {}).get("operational_state") or {}
    return {
        "available": bool(constrained or rebalance or workflow),
        "manual_review_required": approval.get("requires_manual_review"),
        "manual_approval_required": True,
        "approval_state": approval.get("portfolio_decision_state"),
        "order_plan_state": ((rebalance or {}).get("rebalance_state")
                             or execution.get("rebalance_state")),
        "execution_active": execution.get("execution_active"),
        "pending_paper_orders": op.get("pending_orders"),
        "execution_available": primary.get("execution_available"),
        "automation_enabled": False,
        "broker_enabled": False,
        "order_routes_exist": False,
        "owner": COMPONENT_OWNERS["execution_safety"],
    }


def _operator_guidance_block(workflow: Optional[dict]) -> dict:
    w = workflow or {}
    primary = w.get("primary_action") or {}
    return {
        "available": bool(w),
        "overall_state": w.get("overall_state"),
        "current_task": w.get("current_task"),
        "headline": w.get("headline"),
        "next_action": primary,
        "operator_command": w.get("operator_command"),
        # Release 55 — the ONE operator action contract, projected verbatim from
        # its owner (api.workflow_state.build_operator_action). Absent on a
        # pre-R55 workflow payload; nothing is substituted in its place.
        "operator_action": w.get("operator_action"),
        "portfolio_attention": w.get("portfolio_attention"),
        "canonical_portfolio_decision": w.get("canonical_portfolio_decision"),
        "queued_actions": w.get("queued_actions"),
        "blocking_reasons": w.get("blockers"),
        "warnings": w.get("warnings"),
        "consistency_status": w.get("consistency_status"),
        # Release 54.2.1 — the missed-completed-session (catch-up) obligation, read
        # VERBATIM from the workflow owner. This module computes no session date of
        # its own: if it did, a second catch-up owner would exist and the two could
        # disagree about which morning the operator owes a close.
        "session_recovery": _session_recovery_block(workflow),
        # Release 54.2.2 — the post-close governed-research obligation, read VERBATIM
        # from the same workflow owner. A completed close does not settle it, and this
        # module does not decide it: a second opinion about whether governed research
        # is owed would be a second workflow owner.
        "research_obligation": _research_obligation_block(workflow),
        "owner": COMPONENT_OWNERS["operator_guidance"],
    }


#: Release 54.2.1 — the recovery fields this projection republishes, in the workflow
#: owner's OWN spelling. Delegation is the whole point: no default is invented for a
#: missing key, and an absent workflow payload yields an explicit UNAVAILABLE row.
_RECOVERY_FIELDS = ("recovery_state", "catch_up_required", "last_closed_session",
                    "missed_completed_sessions", "missed_completed_session_count",
                    "recovery_session", "current_open_or_next_session",
                    "recovery_data_state", "recovery_data_ready",
                    "recovery_blockers", "next_action", "summary",
                    "recovery_state_vocabulary", "skipped_non_sessions",
                    "operator_supplies_no_date", "orchestration_path",
                    "recovery_specific_route", "oldest_first")


def _session_recovery_block(workflow: Optional[dict]) -> dict:
    """Project the workflow owner's catch-up contract read-only (no derivation)."""
    rec = (workflow or {}).get("session_recovery")
    if not isinstance(rec, dict) or not rec:
        return {"available": False, "recovery_state": "UNAVAILABLE",
                "owner": "api.workflow_state", "delegated": True,
                "computed_here": False,
                "detail": ("The workflow owner did not publish a session-recovery "
                           "contract; nothing is inferred in its place.")}
    out = {"available": True, "owner": rec.get("owner") or "api.workflow_state",
           "calendar_owner": rec.get("calendar_owner"),
           "close_owner": rec.get("close_owner"),
           "delegated": True, "computed_here": False}
    for key in _RECOVERY_FIELDS:
        out[key] = rec.get(key)
    return out


#: Release 54.2.2 — the post-close governed-research obligation fields this projection
#: republishes, in the workflow owner's OWN spelling. Same delegation rule as the
#: recovery block above: nothing is defaulted, nothing is recomputed.
_OBLIGATION_FIELDS = (
    "research_obligation_state", "state_vocabulary", "obligation_outstanding",
    "latest_closed_session", "latest_governed_research_session",
    "latest_governed_decision_session", "outstanding_research_session",
    "operational_close_valid", "governed_research_current",
    "governed_decision_state", "decision_rests_on_governed_research",
    "stale_input_ids", "input_classification", "safely_recoverable_input_ids",
    "unrecoverable_gap_ids", "true_blockers", "safe_work_remains",
    "next_action", "summary", "orchestration_path", "research_specific_route",
    "operator_supplies_no_date", "repeats_the_completed_close",
    "invalidates_operational_close", "documented_forward_evidence_gap",
    "forward_evidence_gap_invalidates_close")


def _research_obligation_block(workflow: Optional[dict]) -> dict:
    """Project the workflow owner's post-close research obligation read-only."""
    ob = (workflow or {}).get("research_obligation")
    if not isinstance(ob, dict) or not ob:
        return {"available": False, "research_obligation_state": "UNAVAILABLE",
                "owner": "api.workflow_state", "delegated": True,
                "computed_here": False,
                "detail": ("The workflow owner did not publish a post-close research "
                           "obligation; nothing is inferred in its place.")}
    out = {"available": True, "owner": ob.get("owner") or "api.workflow_state",
           "classification_owner": ob.get("classification_owner"),
           "delegated": True, "computed_here": False}
    for key in _OBLIGATION_FIELDS:
        out[key] = ob.get(key)
    return out


# --------------------------------------------------------------------------- #
# Stale / missing components — each verdict quotes the OWNER's own state token.
# --------------------------------------------------------------------------- #
_REASSESS_NOT_READY = frozenset({
    "NOT_RUN", "UNAVAILABLE", "STALE_CORPORATE_ACTION_REVIEW_REQUIRED"})
_ASSESS_NEEDS_ACTION = frozenset({"STALE", "DUE", "OVERDUE", "MISSING"})


def _stale_components(*, operational_book: dict, live_information: dict,
                      signal_state: dict, reassessment: dict,
                      target_proposal: dict, research_governance: dict) -> tuple:
    """Return ``(stale_components, advisory_components)``.

    RELEASE 55 — WHAT BELONGS ON A NORMAL OPERATOR SURFACE.

    ``stale_components`` is the operator's STALE / MISSING list: a row here
    asserts that something the operator depends on is not in the state it should
    be in. ``advisory_components`` is the AUDIT-ONLY list: a compatibility
    observation that is TRUE but is not an operator problem, retained in full so
    nothing is hidden and no history is rewritten.

    The R55 defect this split repairs: a portfolio reassessment that was current
    for the eligible session (age 0 sessions) appeared in the STALE / MISSING
    list because the LEGACY monthly scheduled-review checkpoint clock had
    passed. That clock's own owner declares it governs model recalibration, not
    the portfolio cadence, so the row was never an operator problem — and a
    legacy clock contradicted by the authoritative current reassessment must
    never compete with it on a normal surface.
    """
    out: list[dict] = []
    advisory: list[dict] = []

    def _add(component: str, owner_state: Any, detail: Any = None,
             display_label: Any = None, *, advisory_only: bool = False,
             advisory_reason: Any = None):
        # R54.2.4 — ``display_label`` is what a normal surface PRINTS; the raw
        # owner_state token stays beside it for Audit. When absent, surfaces
        # fall back to "<component> (<owner_state>)" exactly as before.
        row = {"component": component,
               "owner": COMPONENT_OWNERS.get(component),
               "owner_state": owner_state,
               "display_label": display_label,
               "detail": detail}
        if advisory_only:
            row["advisory_only"] = True
            row["advisory_reason"] = advisory_reason
            row["is_operator_problem"] = False
            row["surface"] = ADVISORY_SURFACE
            advisory.append(row)
        else:
            row["is_operator_problem"] = True
            row["surface"] = STALE_SURFACE
            out.append(row)

    if not operational_book.get("available"):
        _add("operational_book", "MISSING")
    if not live_information.get("available"):
        _add("live_information", "MISSING")
    elif not live_information.get("collection_running"):
        _add("live_information", live_information.get("collection_service_state"),
             live_information.get("collection_reason"))
    if not signal_state.get("available"):
        _add("signal_state", "MISSING")
    elif (signal_state.get("scoring_status")
          and signal_state.get("scoring_status") != "UNIVERSE_SCORING_READY"):
        _add("signal_state", signal_state.get("scoring_status"))
    if not reassessment.get("available"):
        _add("portfolio_reassessment", "MISSING")
    else:
        if str(reassessment.get("state") or "") in _REASSESS_NOT_READY:
            _add("portfolio_reassessment", reassessment.get("state"))
        elif str(reassessment.get("reassessment_freshness") or "") in _ASSESS_NEEDS_ACTION:
            # Detail composed from the currency owner's OWN inputs so the
            # verdict is self-explaining (e.g. OVERDUE can mean "the legacy
            # scheduled-review date passed" while the assessment is still
            # current for the eligible session).
            fd = reassessment.get("reassessment_freshness_detail") or {}
            parts = ["assessment currency decided by api.workflow_state"]
            if fd.get("current_for_eligible_session") is True:
                parts.append("assessment is current for the eligible session")
            if fd.get("review_overdue") and fd.get("next_scheduled_review_date"):
                parts.append("scheduled review date %s has passed (legacy "
                             "api.daily_action_gate clock)"
                             % fd.get("next_scheduled_review_date"))
            # R54.2.4 (Defect 6) — a reassessment that IS current for the
            # eligible session must never be printed "Portfolio reassessment
            # (OVERDUE)": the OVERDUE token records only that the LEGACY
            # scheduled-review clock (api.daily_action_gate) has passed. The
            # obligation stays visible, under its truthful name.
            display = None
            if (fd.get("current_for_eligible_session") is True
                    and fd.get("review_overdue")):
                display = ("Scheduled full review due — legacy "
                           "api.daily_action_gate clock; the portfolio "
                           "reassessment itself is current for the eligible "
                           "session")
            # RELEASE 55 — R54.2.4 made the label truthful; it left the row in the
            # operator's STALE / MISSING list, so a truthful sentence still read as
            # a problem. The row is DEMOTED to the audit-only advisory list when,
            # and only when, BOTH of the currency owner's own facts hold:
            #
            #   * the assessment is current for the eligible session, and
            #   * the ONLY thing that moved the token off CURRENT was the legacy
            #     schedule, which its owner declares does not govern the
            #     portfolio cadence (``schedule_decided_status`` is True, or the
            #     owner stated ``schedule_is_compatibility_only``).
            #
            # A genuinely STALE / MISSING / INCONSISTENT assessment, or a token
            # this module cannot attribute to the legacy clock, still lands in the
            # operator list unchanged: silence is never read as a repair.
            legacy_only = bool(
                fd.get("current_for_eligible_session") is True
                and fd.get("review_overdue")
                and (fd.get("schedule_decided_status") is True
                     or fd.get("schedule_is_compatibility_only") is True))
            _add("portfolio_reassessment", reassessment.get("reassessment_freshness"),
                 "; ".join(parts), display_label=display,
                 advisory_only=legacy_only,
                 advisory_reason=(LEGACY_SCHEDULE_ADVISORY_REASON
                                  if legacy_only else None))
    if not target_proposal.get("available"):
        _add("target_proposal", "MISSING")
    if not research_governance.get("available"):
        _add("research_governance", "MISSING")
    elif (research_governance.get("research_runtime") or {}).get("state") in (
            None, "RUNTIME_NEVER_RAN"):
        _add("research_governance",
             (research_governance.get("research_runtime") or {}).get("state")
             or "RESEARCH_RUNTIME_UNKNOWN",
             "R52 research runtime health")
    return out, advisory


# --------------------------------------------------------------------------- #
# RELEASE 55 — THE THREE OPERATOR ANSWERS.
#
# The operator's first screen must answer three questions and nothing else:
#
#   1  WHAT IS THE CURRENT AUTHORITATIVE PORTFOLIO DECISION?
#   2  WHAT HAS CHANGED SINCE THAT DECISION?
#   3  WHAT SHOULD THE OPERATOR DO NOW?
#
# Before R55 the Today page published the raw material instead of the answers,
# and the operator had to reconcile the governed decision, the last portfolio
# reassessment, the latest live/intraday reassessment, two clocks, the
# session-close status and a legacy review clock to work out which of the three
# they were even looking at.
#
# This block composes the answers ONCE, here, from facts the owners already
# decided. It resolves no economics, selects no authority, orders nothing and
# writes nothing: answer 1 is the decision owner's own verdict, answer 2 is the
# live lane's own record, answer 3 is the workflow owner's own operator action.
# Every identity (run id, artifact id, hash, UTC stamp, owner name) stays in the
# payload for Audit / Advanced — the answers are what a NORMAL surface prints.
# --------------------------------------------------------------------------- #
def _operator_display_time(value: Any, *, with_date: bool = True):
    """The Eastern-time spelling of an owner's stamp, from the CLOCK OWNER.

    ``engine.market_session.format_operator_timestamp`` owns the Eastern clock;
    this module neither converts a timezone nor reads one. An unavailable owner
    or an unparseable stamp degrades to None, and the surface then shows the raw
    identity from Audit rather than a guessed local time.
    """
    try:
        from paper_trader.engine import market_session as msession
        return msession.format_operator_timestamp(value, with_date=with_date)
    except Exception:  # noqa: BLE001 — a missing clock owner prints nothing
        return None


def _operator_answer_block(*, governed: dict, canonical: Optional[dict],
                           lane: dict, live_information: dict,
                           operational_book: dict,
                           operator_guidance: dict) -> dict:
    canon = canonical or {}
    action = operator_guidance.get("operator_action") or {}
    cmd = operator_guidance.get("operator_command") or {}

    # ---- ANSWER 1 — the current authoritative decision -------------------- #
    # The headline is the decision owner's own sentence. When the governed lane
    # published no decision at all this stays UNAVAILABLE rather than borrowing
    # the research lane's conclusion.
    decision_available = bool(governed.get("available") or canon)
    decision = {
        "question": OPERATOR_ANSWER_QUESTIONS[0],
        "available": decision_available,
        "headline": canon.get("headline"),
        "explanation": canon.get("explanation") or canon.get("no_proposal_reason"),
        "decision": governed.get("decision") or canon.get("state"),
        "decision_state": canon.get("state"),
        "session": (governed.get("eligible_market_session")
                    or canon.get("eligible_market_date")),
        "session_display": _operator_display_time(
            governed.get("eligible_market_session")
            or canon.get("eligible_market_date")),
        "decided_at": governed.get("timestamp"),
        "decided_at_display": _operator_display_time(governed.get("timestamp")),
        "provenance": governed.get("provenance"),
        "record_id": governed.get("record_id"),
        "persisted": governed.get("persisted"),
        # R55.1 — how the decision is HELD, in the decision owner's own words,
        # so ``persisted: false`` beside a real record id can never again read
        # as a contradiction on any surface.
        "persistence_status": governed.get("persistence_status"),
        "persistence_detail": governed.get("persistence_detail"),
        "is_ledger_row": governed.get("is_ledger_row"),
        "retrievable_through_owner": governed.get("retrievable_through_owner"),
        "manual_review_required": governed.get("manual_review_required"),
        # The operational facts the decision was taken against — the book's own.
        "operational_mark_date": operational_book.get("operational_mark_date"),
        "operational_mark_display": _operator_display_time(
            operational_book.get("operational_mark_date")),
        "nav": operational_book.get("nav"),
        "is_authoritative": True,
        "authority_owner": COMPONENT_OWNERS["latest_governed_portfolio_decision"],
        "presentation_owner": "api.workflow_state.build_canonical_portfolio_decision",
    }

    # ---- ANSWER 2 — what changed since that decision ---------------------- #
    # The live/intraday lane's OWN record. The two facts an operator most often
    # misreads are stated explicitly: a reassessment that concluded HOLD is not a
    # decision, and material information about names the book does not hold is a
    # legitimate reason to re-underwrite the portfolio, not an anomaly.
    events = live_information.get("material_event_count")
    affected = list(lane.get("affected_holdings") or [])
    # R55.1 — DID A REASSESSMENT RUN? The lane owner answers; this composition
    # only falls back to the conclusion vocabulary when an older payload omits
    # the flag. Everything reassessment-shaped below is gated on this, so a
    # cycle that ran no reassessment can never present a reassessment time or a
    # reassessment conclusion — which is how "NO_NEW_INFORMATION" used to reach
    # the operator as "Latest reassessment ... Conclusion: UNKNOWN".
    conclusion = lane.get("candidate_conclusion")
    ran = (bool(lane.get("reassessment_ran"))
           if lane.get("reassessment_ran") is not None
           else conclusion not in LANE_NO_REASSESSMENT_CONCLUSIONS)
    changed = {
        "question": OPERATOR_ANSWER_QUESTIONS[1],
        "available": bool(lane.get("available")),
        "material_events_evaluated": events,
        "material_events_since_decision": live_information.get(
            "material_events_since_last_reassessment"),
        "affected_current_holdings": affected,
        "affected_current_holdings_count": len(affected),
        "last_material_event_at": lane.get("last_material_event_at"),
        "last_material_event_display": _operator_display_time(
            lane.get("last_material_event_at")),
        # The CYCLE's own clock, named for what it is. A signal refresh that
        # produced no reassessment still has a time; a reassessment does not.
        "latest_cycle_at": lane.get("at"),
        "latest_cycle_display": _operator_display_time(lane.get("at")),
        "reassessment_ran": ran,
        "headline": lane.get("headline"),
        "reassessment_summary": lane.get("reassessment_summary"),
        "governed_summary": lane.get("governed_summary"),
        "latest_reassessment_at": lane.get("at") if ran else None,
        "latest_reassessment_display": (_operator_display_time(lane.get("at"))
                                        if ran else None),
        "latest_reassessment_trigger": lane.get("trigger"),
        "latest_reassessment_conclusion": conclusion if ran else None,
        "scoring_basis_date": lane.get("scoring_basis_date"),
        "scoring_basis_display": _operator_display_time(
            lane.get("scoring_basis_date")),
        "governance_state": lane.get("governance_state"),
        "supersedes_standing_decision": lane.get("supersedes_standing_decision"),
        "changes_the_authoritative_decision": bool(
            lane.get("promoted_to_governed")),
        "lane": lane.get("lane"),
        "lane_label": lane.get("lane_label"),
        "is_authoritative": False,
        "why_non_held_events_matter": (
            "Material information about an asset the book does not hold can "
            "still change the opportunity cost of what it does hold, so it "
            "legitimately triggers a portfolio reassessment. Zero affected "
            "holdings is a normal outcome, not a failure."),
        "why_this_is_not_the_decision": (
            "This is the live research lane. It re-underwrites the portfolio "
            "continuously and never becomes the authoritative decision unless "
            "the governance gate promotes it."),
        "owner": lane.get("owner"),
    }

    # ---- ANSWER 3 — what the operator should do now ----------------------- #
    do_now = {
        "question": OPERATOR_ANSWER_QUESTIONS[2],
        "available": bool(action),
        "action": action.get("action"),
        "action_label": action.get("action_label"),
        "action_detail": action.get("action_detail"),
        "why": action.get("why"),
        "requires_operator_work": action.get("requires_operator_work"),
        "executes": action.get("executes"),
        "execution_label": action.get("execution_label"),
        "confirmation_required": action.get("confirmation_required"),
        "destination": action.get("destination"),
        "focus": action.get("focus"),
        "severity": action.get("severity"),
        "blocking_reason": action.get("blocking_reason"),
        "priority_rank": action.get("priority_rank"),
        "action_vocabulary": action.get("action_vocabulary"),
        "supporting_text": cmd.get("supporting_text"),
        "after_text": cmd.get("after_text"),
        "creates_orders": False,
        "approves_anything": False,
        "automation_enabled": False,
        "owner": action.get("owner"),
        "priority_owner": action.get("priority_owner"),
    }

    return {
        "schema_version": "operator_answer.v1",
        "questions": list(OPERATOR_ANSWER_QUESTIONS),
        "current_decision": decision,
        "what_changed_since": changed,
        "what_to_do_now": do_now,
        # The two lanes stay named and stay separate on the operator's own
        # screen: the research lane can never masquerade as the governed answer.
        "lanes_are_distinct": True,
        "governed_lane_owner": COMPONENT_OWNERS[
            "latest_governed_portfolio_decision"],
        "research_lane_owner": COMPONENT_OWNERS["latest_live_intraday_assessment"],
        "identities_live_in_audit": True,
        "composed_here": True,
        "recomputes_nothing": True,
        "owner": OWNER,
    }


# --------------------------------------------------------------------------- #
# RELEASE 55 — THE ACTIVE MANAGER ACCEPTANCE CONTRACT.
#
# A deterministic, read-only checklist over the composed state: one row per
# stage of the chain, each quoting the owner that decided it. It exists so an
# operator (or a release gate) can prove the Active Manager ran end to end
# without opening nine surfaces, and so a MISSING fact is reported as MISSING
# instead of being inferred from a neighbouring one.
#
# It is a pure function of an already-composed payload: no loader, no owner
# call, no arithmetic, no verdict of its own beyond PRESENT / MISSING.
# --------------------------------------------------------------------------- #
ACCEPTANCE_ROWS = (
    "COLLECTION", "SIGNAL", "SCORING", "HOC", "REASSESSMENT", "GOVERNANCE",
    "GOVERNED_DECISION", "OPERATIONAL_BOOK", "NEXT_ACTION", "LATENCY",
)
ACCEPTANCE_PRESENT = "PRESENT"
ACCEPTANCE_MISSING = "MISSING"


def build_acceptance_contract(state: Optional[dict]) -> dict:
    """The R55 read-only acceptance view over a composed active-manager state.

    ``state`` is the payload ``build_active_manager_state`` returns (or the same
    JSON fetched from the route). Every row's ``value`` is copied; ``status`` is
    PRESENT only when the row's own key fact exists. Nothing is manufactured: a
    stage that persisted nothing stays MISSING and says which owner owed it.
    """
    s = state or {}
    ob = s.get("operational_book") or {}
    li = s.get("live_information") or {}
    sg = s.get("signal_state") or {}
    rs = s.get("portfolio_reassessment") or {}
    lane = s.get("live_reassessment_lane") or {}
    gov = s.get("intraday_governance") or {}
    gd = s.get("latest_governed_portfolio_decision") or {}
    lat = s.get("decision_latency") or {}
    guid = s.get("operator_guidance") or {}
    action = guid.get("operator_action") or {}
    cycle = li.get("last_event_cycle") or {}
    stamps = cycle.get("stage_timestamps") or {}
    owners = s.get("component_owners") or dict(COMPONENT_OWNERS)

    def _row(row: str, key_fact: Any, owner: Any, **values) -> dict:
        return {"row": row,
                "status": (ACCEPTANCE_PRESENT if key_fact not in (None, "", [])
                           else ACCEPTANCE_MISSING),
                "owner": owner, **values}

    rows = [
        _row("COLLECTION", li.get("last_observation_at"),
             owners.get("live_information"),
             service_state=li.get("collection_service_state"),
             running=li.get("collection_running"),
             last_observation_at=li.get("last_observation_at"),
             worker_activity=li.get("worker_activity")),
        _row("SIGNAL", li.get("last_material_event_at"),
             owners.get("live_information"),
             last_material_event_at=li.get("last_material_event_at"),
             material_event_count=li.get("material_event_count"),
             last_signal_refresh_at=sg.get("last_signal_refresh_at"),
             last_signal_refresh_state=sg.get("last_signal_refresh_state")),
        _row("SCORING", sg.get("last_scoring_ranking_date"),
             owners.get("signal_state"),
             scoring_basis_date=sg.get("last_scoring_ranking_date"),
             scope=(sg.get("scoring_basis") or {}).get("scope"),
             scored_universe_count=sg.get("scored_universe_count"),
             ranking_snapshot_id=sg.get("ranking_snapshot_id"),
             scoring_status=sg.get("scoring_status")),
        _row("HOC", (rs.get("hoc_summary") or {}).get("assessment_hash"),
             "api.holding_opportunity_cost",
             state=(rs.get("hoc_summary") or {}).get("state"),
             assessment_hash=(rs.get("hoc_summary") or {}).get("assessment_hash"),
             completed_at=lane.get("hoc_completed_at") or stamps.get(
                 "hoc_completed_at")),
        _row("REASSESSMENT", rs.get("reassessment_id"),
             owners.get("portfolio_reassessment"),
             reassessment_id=rs.get("reassessment_id"),
             reassessment_hash=rs.get("reassessment_hash"),
             session=rs.get("reassessment_session"),
             at=rs.get("last_reassessment_at"),
             result=rs.get("current_decision"),
             latest_live_conclusion=lane.get("candidate_conclusion"),
             # R55.1 — did a reassessment run on the latest live cycle at all?
             # The row states it, so no reader has to infer it from a token.
             latest_live_reassessment_ran=lane.get("reassessment_ran"),
             latest_live_summary=lane.get("reassessment_summary"),
             latest_live_persisted=lane.get("reassessment_persisted"),
             latest_live_persistence_status=lane.get(
                 "reassessment_persistence_status")),
        # R55.1 — GOVERNANCE is PRESENT when the governance OWNER proves a
        # terminal disposition, which includes proving that no verdict was
        # required. It stays MISSING for an INCOMPLETE cycle: MISSING means the
        # system cannot prove what happened, and that remains fail-closed.
        _row("GOVERNANCE",
             gov.get("disposition") if gov.get("terminal") else None,
             gov.get("disposition_owner") or owners.get("intraday_governance"),
             disposition=gov.get("disposition"),
             terminal=gov.get("terminal"),
             required=gov.get("required"),
             gate_evaluated=gov.get("evaluated"),
             gate_invoked_by_cycle=gov.get("gate_invoked_by_cycle"),
             reason=gov.get("reason"),
             reason_detail=gov.get("reason_detail"),
             event_cycle_run_id=gov.get("event_cycle_run_id"),
             event_cycle_state=gov.get("event_cycle_state"),
             candidate_reassessment_id=gov.get("candidate_reassessment_id"),
             promotion_decision_id=gov.get("governed_record_id"),
             at=gov.get("disposition_at"),
             verdict=gov.get("verdict"),
             lane_governance_state=lane.get("governance_state"),
             promoted_to_governed=lane.get("promoted_to_governed"),
             withheld_reason_codes=list(gov.get("withheld_reason_codes") or []),
             failing_checks=list(gov.get("failing_checks") or [])),
        _row("GOVERNED_DECISION", gd.get("decision"),
             owners.get("latest_governed_portfolio_decision"),
             decision=gd.get("decision"), provenance=gd.get("provenance"),
             session=gd.get("eligible_market_session"),
             record_id=gd.get("record_id"), decided_at=gd.get("timestamp"),
             persisted=gd.get("persisted"),
             # R55.1 — self-describing persistence: a legacy projection is not
             # a ledger row, and both are retrievable through the one owner.
             persistence_status=gd.get("persistence_status"),
             persistence_detail=gd.get("persistence_detail"),
             is_ledger_row=gd.get("is_ledger_row"),
             retrievable_through_owner=gd.get("retrievable_through_owner"),
             supersedes_decision_id=gd.get("supersedes_decision_id")),
        _row("OPERATIONAL_BOOK", ob.get("operational_mark_date"),
             owners.get("operational_book"),
             latest_completed_close_date=ob.get("latest_completed_close_date"),
             operational_mark_date=ob.get("operational_mark_date"),
             eligible_market_session=ob.get("eligible_market_session"),
             nav=ob.get("nav"), cash=ob.get("cash"),
             holdings_count=ob.get("holdings_count"),
             close_valid=ob.get("operational_close_valid")),
        _row("NEXT_ACTION", action.get("action"), owners.get("operator_guidance"),
             action=action.get("action"), label=action.get("action_label"),
             overall_state=guid.get("overall_state"),
             requires_operator_work=action.get("requires_operator_work"),
             priority_rank=action.get("priority_rank"),
             executes=action.get("executes")),
        _row("LATENCY", lat.get("observation_to_signal_seconds"),
             lat.get("measurement_owner") or owners.get("decision_latency"),
             measurement_basis=lat.get("measurement_basis"),
             observation_to_signal_seconds=lat.get(
                 "observation_to_signal_seconds"),
             signal_to_reassessment_seconds=lat.get(
                 "signal_to_reassessment_seconds"),
             reassessment_to_governed_seconds=lat.get(
                 "reassessment_to_governed_seconds"),
             observation_to_governed_seconds=lat.get(
                 "observation_to_governed_seconds"),
             measurement_complete=lat.get("latency_measurement_complete"),
             missing_measurements=list(lat.get("missing_measurements") or []),
             # R55.1 — stages an owner proved this cycle never needed. Named
             # apart from MISSING so a correct no-op does not read as a fault.
             not_required_measurements=list(
                 lat.get("not_required_measurements") or []),
             interval_dispositions=lat.get("interval_dispositions") or {}),
    ]
    missing = [r["row"] for r in rows if r["status"] == ACCEPTANCE_MISSING]
    return {
        "schema_version": "active_manager_acceptance.v1",
        "phase": "R55",
        "owner": OWNER,
        "row_vocabulary": list(ACCEPTANCE_ROWS),
        "status_vocabulary": [ACCEPTANCE_PRESENT, ACCEPTANCE_MISSING],
        "rows": rows,
        "present_count": len(rows) - len(missing),
        "missing_rows": missing,
        "complete": not missing,
        "generated_at": s.get("generated_at"),
        "read_only": True,
        "recomputes_nothing": True,
        "manufactures_no_timestamp": True,
        "note": ("Deterministic acceptance view over the composed state. A row "
                 "is MISSING when its owner persisted nothing; a missing fact "
                 "is never inferred from a neighbouring stage."),
    }


# --------------------------------------------------------------------------- #
# THE builder (pure projection; every input injectable for tests).
# --------------------------------------------------------------------------- #
def build_active_manager_state(*, workflow: Optional[dict] = None,
                               portfolio_state: Optional[dict] = None,
                               constrained: Optional[dict] = None,
                               information_collection: Optional[dict] = None,
                               rebalance: Optional[dict] = None,
                               event_refresh: Optional[dict] = None,
                               reassessment: Optional[dict] = None,
                               scoring: Optional[dict] = None,
                               runtime_health: Optional[dict] = None,
                               intraday_emission: Optional[dict] = None,
                               governed_decision: Optional[dict] = None,
                               warnings: Optional[list] = None) -> dict:
    """Compose the ONE Active Manager Operating State from the owners' payloads.

    Pure projection: no loader runs here, no owner value is recomputed, and a
    missing owner degrades to an explicit stale/missing row — never a fabricated
    value and never a crash.
    """
    warn = list(warnings or [])
    operational_book = _operational_book_block(portfolio_state, workflow)
    live_information = _live_information_block(information_collection,
                                               event_refresh, reassessment)
    signal_state = _signal_state_block(event_refresh, scoring, workflow,
                                       intraday_emission)
    reassessment_block = _reassessment_block(reassessment, workflow, event_refresh)
    target_proposal = _target_proposal_block(constrained, workflow)
    research_governance = _research_governance_block(workflow, scoring,
                                                     runtime_health)
    execution_safety = _execution_safety_block(constrained, rebalance, workflow)
    operator_guidance = _operator_guidance_block(workflow)
    live_intraday = _live_intraday_assessment_block(live_information,
                                                    reassessment_block)
    governed_block = _governed_decision_block(governed_decision)
    intraday_governance = _intraday_governance_block(live_information,
                                                     governed_decision)
    live_reassessment_lane = _live_reassessment_lane_block(
        live_information=live_information, signal_state=signal_state,
        reassessment=reassessment_block, workflow=workflow,
        governed_decision=governed_block)
    decision_latency = _decision_latency_block(governed_decision,
                                               live_information)
    # R55 — the operator's STALE / MISSING list and the AUDIT-ONLY advisory list
    # are now two lists, because they answer two different questions.
    stale, advisory = _stale_components(
        operational_book=operational_book, live_information=live_information,
        signal_state=signal_state, reassessment=reassessment_block,
        target_proposal=target_proposal, research_governance=research_governance)
    # R55 — the THREE operator answers, composed once from the owners above.
    operator_answer = _operator_answer_block(
        governed=governed_block,
        canonical=(workflow or {}).get("canonical_portfolio_decision"),
        lane=live_reassessment_lane, live_information=live_information,
        operational_book=operational_book, operator_guidance=operator_guidance)

    time_state = {
        "distinct": True,
        "statement": TIME_STATE_STATEMENT,
        "operational": {
            "eligible_market_session": operational_book.get("eligible_market_session"),
            "operational_mark_date": operational_book.get("operational_mark_date"),
            "latest_completed_close_date": operational_book.get(
                "latest_completed_close_date"),
            # Release 54.2.1 — the operational clock has a THIRD fact beside the
            # eligible session and the mark: whether a completed session is still
            # owed. Delegated, never recomputed.
            "session_recovery_state": (operator_guidance.get("session_recovery")
                                       or {}).get("recovery_state"),
            "recovery_session": (operator_guidance.get("session_recovery")
                                 or {}).get("recovery_session"),
            "owner": COMPONENT_OWNERS["operational_book"],
        },
        # Release 54.2.2 — THE THREE CLOCKS STATED AS THREE CLOCKS. The operational
        # close, the governed research cycle and the governed portfolio decision
        # advance independently and may legitimately differ; before this release a
        # completed close was read as though it settled all three. Every value is the
        # workflow owner's, republished — none is computed here.
        "governed_research": {
            "research_obligation_state": (operator_guidance.get("research_obligation")
                                          or {}).get("research_obligation_state"),
            "latest_closed_session": (operator_guidance.get("research_obligation")
                                      or {}).get("latest_closed_session"),
            "latest_governed_research_session": (
                operator_guidance.get("research_obligation")
                or {}).get("latest_governed_research_session"),
            "latest_governed_decision_session": (
                operator_guidance.get("research_obligation")
                or {}).get("latest_governed_decision_session"),
            "outstanding_research_session": (
                operator_guidance.get("research_obligation")
                or {}).get("outstanding_research_session"),
            "governed_research_current": (operator_guidance.get("research_obligation")
                                          or {}).get("governed_research_current"),
            "decision_rests_on_governed_research": (
                operator_guidance.get("research_obligation")
                or {}).get("decision_rests_on_governed_research"),
            "invalidates_operational_close": (
                operator_guidance.get("research_obligation")
                or {}).get("invalidates_operational_close"),
            "owner": "api.workflow_state",
            "computed_here": False,
        },
        "live_research": {
            "last_observation_at": live_information.get("last_observation_at"),
            "last_material_event_at": live_information.get("last_material_event_at"),
            "last_event_cycle_at": (live_information.get("last_event_cycle")
                                    or {}).get("generated_at"),
            "last_scoring_ranking_date": signal_state.get("last_scoring_ranking_date"),
            "last_prediction_emission_date": signal_state.get(
                "last_prediction_emission_date"),
            "last_intraday_prospective_emission_at": (
                ((signal_state.get("latest_intraday_prospective_emission")
                  or {}).get("last_emission") or {}).get("emitted_at_utc")),
            "collection_service_state": live_information.get(
                "collection_service_state"),
            "owner": COMPONENT_OWNERS["live_information"],
        },
        "operational_mark_advanced_only_by": "api.daily_close",
    }

    # R54 finalization — THE DECISION AUTHORITY LADDER. Each rung carries the
    # owner's own current value verbatim beside the owner's name, so the five
    # concepts can never be collapsed into one word on any surface.
    last_cycle = live_information.get("last_event_cycle") or {}
    approval = (constrained or {}).get("approval") or {}
    decision_authority = {
        "statement": DECISION_AUTHORITY_STATEMENT,
        "live_intraday_assessment": {
            "state": last_cycle.get("state"),
            "at": last_cycle.get("generated_at"),
            "reassessment_ran": last_cycle.get("reassessment_ran"),
            "proposal_built": last_cycle.get("proposal_built"),
            "state_note": EVENT_CYCLE_PROPOSAL_NOTE,
            "advances_governed_decision": False,
            "advances_operational_mark": False,
            "owner": "api.event_signal_refresh",
        },
        "governed_portfolio_reassessment": {
            "state": reassessment_block.get("state"),
            "operator_state": reassessment_block.get("operator_state"),
            "decision_settled": reassessment_block.get("decision_settled"),
            "at": reassessment_block.get("last_reassessment_at"),
            "artifact_class": (reassessment_block.get("reassessment_trigger")
                               or {}).get("artifact_class"),
            "owner": "api.portfolio_reassessment",
        },
        "governed_target": {
            "state": target_proposal.get("target_state"),
            "clears_hurdle": target_proposal.get("clears_hurdle"),
            "owner": "api.reallocation_proposal",
        },
        "manual_review_candidate": {
            "required": approval.get("requires_manual_review"),
            "owner": "api.portfolio_decision",
        },
        "approved_decision": {
            "state": approval.get("portfolio_decision_state"),
            "recorded_only_by_manual_confirmation": True,
            "owner": "api.portfolio_decision",
        },
        "canonical_current_decision": (
            (workflow or {}).get("canonical_portfolio_decision")),
        # R54.2.3.2 — THE canonical decision-authority selector, echoed verbatim
        # from its owner (api.portfolio_decision via api.workflow_state): which
        # decision is authoritative right now, which proposal (if any) is
        # currently reviewable, and what was superseded.
        "authoritative_selector": (workflow or {}).get("decision_authority"),
        # R54.1 — the ladder's missing rung, now real: the ONE gate that may
        # promote a complete live intraday assessment into the governed lane,
        # and the governed decision that results. Promotion updates the
        # authoritative RECOMMENDATION only; approval and execution are
        # unchanged, manual, and still belong to the operator.
        "intraday_governance_gate": {
            "owner": "api.portfolio_decision",
            "verdict": intraday_governance.get("verdict"),
            "promoted_to_governed": intraday_governance.get(
                "promoted_to_governed"),
            "withheld_reason_codes": intraday_governance.get(
                "withheld_reason_codes"),
            "promotion_changes_recommendation_only": True,
            "promotion_approves_nothing": True,
            "promotion_creates_no_order": True,
            "promotion_advances_operational_mark": False,
        },
        "latest_governed_portfolio_decision": {
            "decision": governed_block.get("decision"),
            "provenance": governed_block.get("provenance"),
            "at": governed_block.get("timestamp"),
            "record_id": governed_block.get("record_id"),
            "supersedes_decision_id": governed_block.get(
                "supersedes_decision_id"),
            "owner": "api.portfolio_decision",
        },
    }

    payload = {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "owner": OWNER,
        "route": ROUTE,
        "generated_at": _now_iso(),
        "components": list(COMPONENTS),
        "component_owners": dict(COMPONENT_OWNERS),
        # Release 55 — THE THREE OPERATOR ANSWERS, first in the payload because
        # they are first on the screen. Everything below them is the evidence.
        "operator_answer": operator_answer,
        "time_state": time_state,
        "decision_authority": decision_authority,
        "operational_book": operational_book,
        "live_information": live_information,
        "signal_state": signal_state,
        "portfolio_reassessment": reassessment_block,
        "target_proposal": target_proposal,
        "research_governance": research_governance,
        "execution_safety": execution_safety,
        "operator_guidance": operator_guidance,
        # Release 54.2.1 — promoted to the top level so a surface never has to dig
        # for "is a completed session still owed?". Identical object, one owner.
        "session_recovery": operator_guidance.get("session_recovery"),
        # Release 54.2.2 — promoted for the same reason: a surface must never have to
        # dig for "is governed research still owed for the session we closed?".
        "research_obligation": operator_guidance.get("research_obligation"),
        "latest_live_intraday_assessment": live_intraday,
        # R54.2.4 — LANE B: the first-class live/intraday reassessment answer
        # (when, why, what changed, what it concluded, whether governance
        # passed, the exact withheld reason, and whether it supersedes the
        # standing decision). Composed once HERE; the UI renders it verbatim.
        "live_reassessment_lane": live_reassessment_lane,
        "latest_governed_portfolio_decision": governed_block,
        "intraday_governance": intraday_governance,
        "decision_latency": decision_latency,
        "stale_components": stale,
        "stale_component_count": len(stale),
        # Release 55 — the AUDIT / ADVANCED advisory list: observations that are
        # TRUE and are NOT operator problems. Retained in full (nothing deleted,
        # no history rewritten) and kept off the normal operator surface.
        "advisory_components": advisory,
        "advisory_component_count": len(advisory),
        "component_surfaces": {"stale": STALE_SURFACE,
                               "advisory": ADVISORY_SURFACE},
        "warnings": warn,
        "read_only": True,
        "business_calculation_owner": False,
        "recomputes_nothing": True,
        "safety": _safety(),
        "provenance": {
            "owner": OWNER,
            "composition_only": True,
            "note": ("Read-only projection over the canonical owners. Domain "
                     "facts stay with their owners and are never recomputed; "
                     "a disagreeing surface means this module has a bug."),
        },
    }
    # Release 55 — the acceptance contract is a pure function of the payload
    # above, so it is attached LAST and can never influence what it reports.
    payload["acceptance"] = build_acceptance_contract(payload)
    return payload


# --------------------------------------------------------------------------- #
# Production loader. Decision-side sections come from the ONE Release-50
# decision snapshot (one composition per identity); live research reads run
# fresh because the event fabric is deliberately outside the snapshot identity.
# --------------------------------------------------------------------------- #
def load_active_manager_state(*, loaders: Optional[dict] = None) -> dict:
    warnings: list[str] = []

    def _get(name: str, fn: Callable):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - one owner failing never hides the rest
            warnings.append("%s unavailable: %s" % (name, str(exc)[:160]))
            return None

    lds = dict(loaders or {})

    def _section(name: str) -> Callable:
        def _load():
            from paper_trader.api import decision_snapshot as snap
            return snap.section(name)
        return lds.get(name, _load)

    workflow = _get("workflow", _section("workflow"))
    portfolio_state = _get("portfolio_state", _section("portfolio_state"))
    constrained = _get("constrained", _section("constrained"))
    information_collection = _get("information_collection",
                                  _section("information_collection"))
    rebalance = _get("rebalance", _section("rebalance"))

    def _event_refresh():
        from paper_trader.api import event_signal_refresh as esr
        return esr.load_event_signal_refresh_status(
            portfolio_state=portfolio_state)

    def _reassessment():
        from paper_trader.api import portfolio_reassessment as prs
        return prs.load_portfolio_reassessment(portfolio_state=portfolio_state)

    def _scoring():
        from paper_trader.api import universe_scoring as us
        built = us.build_universe_scoring()
        ident = us.canonical_identity(built)
        # identity + counts only; the ranking rows stay with their owner's route
        return {
            "primary_model_id": ident.get("primary_model_id"),
            "champion_model_id": us.CHAMPION_MODEL_ID,
            "challenger_model_ids": list(us.CHALLENGER_MODEL_IDS),
            "ranking_date": ident.get("ranking_date"),
            "input_contract_hash": ident.get("input_contract_hash"),
            "scored_count": built.get("scored_count"),
            "status": ident.get("status"),
        }

    def _runtime_health():
        from paper_trader.api import research_runtime as rr
        return rr.load_runtime_health()

    def _intraday_emission():
        from paper_trader.api import research_runtime as rr
        return rr.load_intraday_emission_status()

    def _governed_decision():
        # R54.1 — the authoritative governed decision, decided entirely by the
        # ONE decision owner. This module reads it; it never resolves, orders
        # or supersedes a decision itself.
        from paper_trader.api import portfolio_decision as pdec
        ab = ((portfolio_state or {}).get("active_book") or {}).get("book_id")
        return pdec.load_governed_portfolio_decision(
            workflow=workflow, reassessment=reassessment,
            proposal_summary=None, constrained=constrained, active_book_id=ab)

    event_refresh = _get("event_refresh", lds.get("event_refresh", _event_refresh))
    reassessment = _get("reassessment", lds.get("reassessment", _reassessment))
    scoring = _get("scoring", lds.get("scoring", _scoring))
    runtime_health = _get("runtime_health",
                          lds.get("runtime_health", _runtime_health))
    intraday_emission = _get("intraday_emission",
                             lds.get("intraday_emission", _intraday_emission))
    governed_decision = _get("governed_decision",
                             lds.get("governed_decision", _governed_decision))

    return build_active_manager_state(
        workflow=workflow, portfolio_state=portfolio_state,
        constrained=constrained, information_collection=information_collection,
        rebalance=rebalance, event_refresh=event_refresh,
        reassessment=reassessment, scoring=scoring,
        runtime_health=runtime_health, intraday_emission=intraday_emission,
        governed_decision=governed_decision, warnings=warnings)


__all__ = [
    "PHASE", "OWNER", "SCHEMA_VERSION", "ROUTE", "COMPONENTS",
    "COMPONENT_OWNERS", "SAFETY_BADGES", "TIME_STATE_STATEMENT",
    "DECISION_AUTHORITY_STATEMENT", "EVENT_CYCLE_PROPOSAL_NOTE",
    # Release 55 — the two component surfaces, the three operator answers and
    # the deterministic acceptance contract.
    "STALE_SURFACE", "ADVISORY_SURFACE", "LEGACY_SCHEDULE_ADVISORY_REASON",
    "OPERATOR_ANSWER_QUESTIONS", "ACCEPTANCE_ROWS", "ACCEPTANCE_PRESENT",
    "ACCEPTANCE_MISSING", "build_acceptance_contract",
    # Release 55.1 — no-op semantics + the owner-issued governance disposition.
    "LANE_CONCLUSION_NO_REASSESSMENT", "LANE_NO_REASSESSMENT_CONCLUSIONS",
    "LANE_CONCLUSION_VOCAB", "LANE_GOVERNANCE_VOCAB",
    "LANE_GOV_EVALUATED_NO_PROMOTION", "GOVERNANCE_DISPOSITION_TO_LANE",
    "build_active_manager_state", "load_active_manager_state",
]
