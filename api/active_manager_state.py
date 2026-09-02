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
        "approval_required_token": g.get("approval_required_token"),
        "creates_orders": False,
        "approves_anything": False,
        "advances_operational_mark": False,
        "owner": COMPONENT_OWNERS["latest_governed_portfolio_decision"],
    }


def _intraday_governance_block(live_information: dict,
                               governed_decision: Optional[dict]) -> dict:
    """R54.1 — why the latest live candidate did or did not become governed.

    Read verbatim from the record the decision owner wrote into the event
    cycle's own run payload at decision time. The operator can therefore see a
    live signal AND the exact classified reasons it was not promoted."""
    cycle = live_information.get("last_event_cycle") or {}
    gd = cycle.get("governed_decision") or {}
    return {
        "available": bool(gd),
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


def _decision_latency_block(governed_decision: Optional[dict],
                            live_information: dict) -> dict:
    """R54.1 — measured, never modelled. Every value is the latency owner's
    (``api.event_signal_refresh``) own measurement over persisted stamps; a
    stage that persists no timestamp is NAMED, never filled in."""
    lat = (governed_decision or {}).get("latency") or {}
    cycle = live_information.get("last_event_cycle") or {}
    return {
        "available": bool(lat),
        "measurement_owner": "api.event_signal_refresh",
        "timestamps": lat.get("timestamps") or {},
        "observation_to_signal_seconds": lat.get("observation_to_signal_seconds"),
        "signal_to_reassessment_seconds": lat.get("signal_to_reassessment_seconds"),
        "reassessment_to_governed_seconds": lat.get(
            "reassessment_to_governed_seconds"),
        "observation_to_governed_seconds": lat.get(
            "observation_to_governed_seconds"),
        "latency_measurement_complete": lat.get("latency_measurement_complete"),
        "missing_measurements": list(lat.get("missing_measurements") or []),
        "last_event_cycle_at": cycle.get("generated_at"),
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
        "portfolio_attention": w.get("portfolio_attention"),
        "canonical_portfolio_decision": w.get("canonical_portfolio_decision"),
        "queued_actions": w.get("queued_actions"),
        "blocking_reasons": w.get("blockers"),
        "warnings": w.get("warnings"),
        "consistency_status": w.get("consistency_status"),
        "owner": COMPONENT_OWNERS["operator_guidance"],
    }


# --------------------------------------------------------------------------- #
# Stale / missing components — each verdict quotes the OWNER's own state token.
# --------------------------------------------------------------------------- #
_REASSESS_NOT_READY = frozenset({
    "NOT_RUN", "UNAVAILABLE", "STALE_CORPORATE_ACTION_REVIEW_REQUIRED"})
_ASSESS_NEEDS_ACTION = frozenset({"STALE", "DUE", "OVERDUE", "MISSING"})


def _stale_components(*, operational_book: dict, live_information: dict,
                      signal_state: dict, reassessment: dict,
                      target_proposal: dict, research_governance: dict) -> list:
    out: list[dict] = []

    def _add(component: str, owner_state: Any, detail: Any = None):
        out.append({"component": component,
                    "owner": COMPONENT_OWNERS.get(component),
                    "owner_state": owner_state,
                    "detail": detail})

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
            _add("portfolio_reassessment", reassessment.get("reassessment_freshness"),
                 "; ".join(parts))
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
    return out


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
    decision_latency = _decision_latency_block(governed_decision,
                                               live_information)
    stale = _stale_components(
        operational_book=operational_book, live_information=live_information,
        signal_state=signal_state, reassessment=reassessment_block,
        target_proposal=target_proposal, research_governance=research_governance)

    time_state = {
        "distinct": True,
        "statement": TIME_STATE_STATEMENT,
        "operational": {
            "eligible_market_session": operational_book.get("eligible_market_session"),
            "operational_mark_date": operational_book.get("operational_mark_date"),
            "latest_completed_close_date": operational_book.get(
                "latest_completed_close_date"),
            "owner": COMPONENT_OWNERS["operational_book"],
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

    return {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "owner": OWNER,
        "route": ROUTE,
        "generated_at": _now_iso(),
        "components": list(COMPONENTS),
        "component_owners": dict(COMPONENT_OWNERS),
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
        "latest_live_intraday_assessment": live_intraday,
        "latest_governed_portfolio_decision": governed_block,
        "intraday_governance": intraday_governance,
        "decision_latency": decision_latency,
        "stale_components": stale,
        "stale_component_count": len(stale),
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
    "build_active_manager_state", "load_active_manager_state",
]
