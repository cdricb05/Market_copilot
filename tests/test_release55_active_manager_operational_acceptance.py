"""Release 55 — ACTIVE MANAGER OPERATIONAL ACCEPTANCE & OPERATOR CLARITY.

What these tests prove:

  * ONE OPERATOR ACTION CONTRACT (Phase B). ``api.workflow_state`` publishes a
    frozen seven-code operator-action vocabulary with ONE authoritative priority
    order, mapped TOTALLY from the eleven internal overall states that
    ``_decide_overall`` already selected. The contract re-runs no policy, adds no
    execution path, fails CLOSED to BLOCKED on an unknown state, and refines
    exactly one state (a post-close research obligation RESUMES rather than
    restarts) from an obligation an owner already published.

  * THE LEGACY CLOCK NO LONGER COMPETES (Phase D). The monthly scheduled-review
    checkpoint belongs to ``api.operational_book``, which declares it the floor
    for MODEL RECALIBRATION and explicitly NOT the governing portfolio cadence.
    ``api.daily_action_gate`` now forwards that declaration verbatim beside the
    date; ``api.workflow_state.classify_assessment`` consumes it and stops
    letting a non-governing schedule decide the PORTFOLIO-ASSESSMENT status; and
    ``api.active_manager_state`` demotes the resulting row from the operator's
    STALE / MISSING list to an AUDIT-ONLY advisory. Nothing is hidden: the
    ``review_due`` / ``review_overdue`` facts and the full advisory row are
    retained, a genuinely STALE / MISSING assessment is untouched, and silence
    (no declaration) preserves the pre-R55 behaviour exactly.

  * THREE OPERATOR ANSWERS (Phase C). ``operator_answer`` composes, once, the
    only three questions the first screen may answer, with the GOVERNED lane and
    the LIVE / INTRADAY RESEARCH lane permanently distinct — the research lane is
    never authoritative unless the backend itself says the gate promoted it.
    Operator-facing times are spelled in Eastern by the CLOCK OWNER
    (``engine.market_session``), never by a browser and never by this module.

  * A DETERMINISTIC ACCEPTANCE CONTRACT (Phase E). Ten named rows, each quoting
    the owner that decided it, PRESENT only when that owner's key fact exists.
    Decision latency is measured by the LATENCY OWNER
    (``api.event_signal_refresh.measure_decision_latency``) over persisted stage
    timestamps; a stage that persisted no stamp stays MISSING and no interval
    that depends on it is computed.

  * THE UI DERIVES NOTHING (Phase C). The R55 region escapes and places backend
    strings, picks a CSS tone from a backend token, and contains no timezone
    conversion, no arithmetic, no hash comparison, no fetch, no execution or
    approval control, no ``alert()`` and no ``confirm()``. The three answers
    precede the command center; the diagnostic three-clock strip moves inside the
    Today Advanced disclosure and loses nothing.

  * SAFETY. Every new path is read-only: no write, no order, no fill, no
    approval, no model promotion, no sleeve activation, no scheduler, no
    provider or prediction call.

Hermetic: every test drives pure functions over injected payloads. No route is
called, no store is opened, no backend is started and nothing is written.
"""
from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

from paper_trader.api import active_manager_state as ams
from paper_trader.api import daily_action_gate as dag
from paper_trader.api import event_signal_refresh as esr
from paper_trader.api import workflow_state as ws
from paper_trader.engine import market_session as msession

REPO = Path(__file__).resolve().parents[1]
AMS_SRC = (REPO / "api" / "active_manager_state.py").read_text(
    encoding="utf-8", errors="replace")
WS_SRC = (REPO / "api" / "workflow_state.py").read_text(
    encoding="utf-8", errors="replace")
DAG_SRC = (REPO / "api" / "daily_action_gate.py").read_text(
    encoding="utf-8", errors="replace")
UI = (REPO / "api" / "ui" / "index.html").read_text(
    encoding="utf-8", errors="replace")


# --------------------------------------------------------------------------- #
# Fixtures. The live 2026-09-03 numbers are used deliberately: this slice was
# raised by that session's operator experience, so the regression is anchored to
# the exact state that produced it.
# --------------------------------------------------------------------------- #
LIVE_SESSION = "2026-09-02"
LIVE_TODAY = "2026-09-03"
LIVE_CHECKPOINT = "2026-08-01"

#: The review clock's OWN scope declaration, exactly as api.operational_book
#: publishes it (Release 46.6).
BOOK_REVIEW_SCOPE = {
    "review_scope": "SCHEDULED_MODEL_RECALIBRATION_CHECKPOINT",
    "review_cadence": "MONTHLY",
    "review_is_the_governing_portfolio_cadence": False,
    "portfolio_reassessment_cadence": "AFTER_EVERY_MATERIAL_SIGNAL_REFRESH",
    "portfolio_reassessment_owner": "engine.portfolio_reassessment",
    "review_scope_note": "model recalibration floor, not the portfolio cadence",
}


def _classify_args(**over) -> dict:
    args = dict(assessment_date=LIVE_SESSION, eligible_date=LIVE_SESSION,
                current_reference_date=LIVE_TODAY,
                next_review_date=LIVE_CHECKPOINT, review_due=True)
    args.update(over)
    return args


def _freshness_detail(**over) -> dict:
    fd = {"current_for_eligible_session": True,
          "assessment_age_sessions": 0,
          "next_scheduled_review_date": LIVE_CHECKPOINT,
          "review_due": True, "review_overdue": True,
          "currency_owner": ws.ASSESSMENT_CURRENCY_OWNER,
          "schedule_decided_status": True,
          "schedule_is_compatibility_only": True}
    fd.update(over)
    return fd


def _reassessment_block(**over) -> dict:
    block = {"available": True, "state": "CURRENT_NO_CHANGE",
             "reassessment_freshness": "OVERDUE",
             "reassessment_freshness_detail": _freshness_detail()}
    block.update(over)
    return block


def _stale_kwargs(**over) -> dict:
    kw = dict(operational_book={"available": True},
              live_information={"available": True, "collection_running": True},
              signal_state={"available": True,
                            "scoring_status": "UNIVERSE_SCORING_READY"},
              reassessment=_reassessment_block(),
              target_proposal={"available": True},
              research_governance={"available": True,
                                   "research_runtime": {"state": "HEALTHY"}})
    kw.update(over)
    return kw


#: The live event cycle's persisted stage timestamps (api.event_signal_refresh).
LIVE_STAGE_STAMPS = {
    "signal_refresh_completed_at": "2026-09-03T16:28:42.462667+00:00",
    "scoring_completed_at": "2026-09-03T16:28:42.468987+00:00",
    "hoc_completed_at": "2026-09-03T16:28:48.773940+00:00",
    "reassessment_completed_at": "2026-09-03T16:28:50.940532+00:00",
    "target_completed_at": None,
}


def _live_information(**over) -> dict:
    li = {"available": True, "collection_running": True,
          "collection_service_state": "RUNNING", "worker_activity": "IDLE",
          "last_observation_at": "2026-09-03T15:55:08.827043+00:00",
          "last_material_event_at": "2026-09-03T15:55:08.827043+00:00",
          "material_event_count": 20,
          "material_events_since_last_reassessment": 18,
          "last_event_cycle": {
              "run_id": "evt_34dbe756217df98f",
              "state": "REASSESSED_NO_CHANGE",
              "generated_at": "2026-09-03T16:26:31.784900+00:00",
              "stage_timestamps": dict(LIVE_STAGE_STAMPS)}}
    li.update(over)
    return li


def _lane(**over) -> dict:
    lane = {"available": True, "lane": "LIVE_INTRADAY_REASSESSMENT",
            "lane_label": "LATEST LIVE / INTRADAY REASSESSMENT",
            "at": "2026-09-03T16:26:31.784900+00:00",
            "trigger": "MATERIAL_SIGNAL_CHANGED",
            "last_material_event_at": "2026-09-03T15:55:08.827043+00:00",
            "scoring_basis_date": LIVE_SESSION,
            "hoc_completed_at": "2026-09-03T16:28:48.773940+00:00",
            "candidate_conclusion": "HOLD", "governance_state": "ELIGIBLE",
            "affected_holdings": [], "promoted_to_governed": False,
            "supersedes_standing_decision": False,
            "owner": "api.active_manager_state"}
    lane.update(over)
    return lane


def _governed(**over) -> dict:
    gd = {"available": True, "decision": "CURRENT_NO_CHANGE",
          "timestamp": "2026-09-02T23:51:50.475243Z",
          "provenance": "GOVERNED_DAILY_CYCLE",
          "record_id": "drc_governed_drc_2026-09-02_15abfb01856f",
          "eligible_market_session": LIVE_SESSION,
          "manual_review_required": False, "persisted": False,
          "owner": "api.portfolio_decision"}
    gd.update(over)
    return gd


def _canonical(**over) -> dict:
    c = {"state": "NO_CHANGE", "headline": "NO PORTFOLIO CHANGE REQUIRED",
         "explanation": "The current portfolio remains the best use of capital.",
         "eligible_market_date": LIVE_SESSION}
    c.update(over)
    return c


def _command(**over) -> dict:
    cmd = {"state": "WAITING_FOR_SESSION_CLOSE",
           "task": "Wait for the current market session to close.",
           "why": "Wait for the session close cutoff before running the cycle.",
           "primary_action_available": False, "primary_action_label": None,
           "primary_action_kind": None, "primary_action_owner": None,
           "primary_action_execution_contract": None,
           "confirmation_required": None, "destination": "command-center",
           "severity": "INFO",
           "portfolio_cycle_blocking_reason": "Waiting for the session to close.",
           "after_text": "When the session closes, the Daily Close is the action."}
    cmd.update(over)
    return cmd


def _answer(**over) -> dict:
    kw = dict(governed=_governed(), canonical=_canonical(), lane=_lane(),
              live_information=_live_information(),
              operational_book={"operational_mark_date": LIVE_SESSION,
                                "nav": 97934.33},
              operator_guidance={
                  "operator_command": _command(),
                  "operator_action": ws.build_operator_action(
                      overall=ws.WAITING_FOR_SESSION_CLOSE,
                      command=_command())})
    kw.update(over)
    return ams._operator_answer_block(**kw)


# =========================================================================== #
# PHASE B — THE ONE OPERATOR ACTION CONTRACT
# =========================================================================== #
def test_operator_action_vocabulary_is_the_frozen_seven():
    assert set(ws.OPERATOR_ACTIONS) == {
        "MONITOR_PORTFOLIO", "REVIEW_PORTFOLIO_PROPOSAL", "RUN_PORTFOLIO_CYCLE",
        "WAIT_FOR_OWNED_DATA", "RESUME_RESEARCH_CYCLE",
        "WAIT_FOR_SESSION_CLOSE", "BLOCKED"}
    assert len(ws.OPERATOR_ACTIONS) == 7


def test_priority_order_is_one_total_ordering_without_duplicates():
    assert len(ws.OPERATOR_ACTION_PRIORITY) == len(set(ws.OPERATOR_ACTION_PRIORITY))
    assert set(ws.OPERATOR_ACTION_PRIORITY) == set(ws.OPERATOR_ACTIONS)
    # BLOCKED outranks everything; the two "nothing is outstanding" claims rank
    # last, so a real instruction can never be presented below them.
    assert ws.OPERATOR_ACTION_PRIORITY[0] == ws.OP_ACTION_BLOCKED
    assert ws.OPERATOR_ACTION_PRIORITY[-1] == ws.OP_ACTION_MONITOR
    for passive in (ws.OP_ACTION_MONITOR, ws.OP_ACTION_WAIT_SESSION_CLOSE):
        for work in (ws.OP_ACTION_BLOCKED, ws.OP_ACTION_RUN_CYCLE,
                     ws.OP_ACTION_RESUME_RESEARCH, ws.OP_ACTION_REVIEW_PROPOSAL):
            assert (ws.OPERATOR_ACTION_PRIORITY.index(work)
                    < ws.OPERATOR_ACTION_PRIORITY.index(passive))


def test_every_overall_state_maps_to_exactly_one_operator_action():
    """Total mapping: no internal state may fall through to an invented action."""
    assert set(ws.OPERATOR_ACTION_BY_OVERALL) == set(ws.OVERALL_STATES)
    for state in ws.OVERALL_STATES:
        out = ws.build_operator_action(overall=state)
        assert out["action"] in ws.OPERATOR_ACTIONS, state
        assert out["failed_closed_on_unknown_state"] is False, state
        assert out["priority_rank"] == ws.OPERATOR_ACTION_PRIORITY.index(
            out["action"]), state


@pytest.mark.parametrize("state,expected", [
    ("INCONSISTENT_STATE", "BLOCKED"),
    ("RESEARCH_CYCLE_BLOCKED", "BLOCKED"),
    ("WAITING_FOR_OWNED_DATA", "WAIT_FOR_OWNED_DATA"),
    ("READY_FOR_DAILY_CLOSE", "RUN_PORTFOLIO_CYCLE"),
    ("PORTFOLIO_REASSESSMENT_REQUIRED", "RUN_PORTFOLIO_CYCLE"),
    ("MANUAL_REVIEW_REQUIRED", "REVIEW_PORTFOLIO_PROPOSAL"),
    ("WAITING_FOR_SESSION_CLOSE", "WAIT_FOR_SESSION_CLOSE"),
    ("DAILY_CYCLE_COMPLETE", "MONITOR_PORTFOLIO"),
    ("DAILY_CYCLE_COMPLETE_EVIDENCE_GAP", "MONITOR_PORTFOLIO"),
    ("RESEARCH_CYCLE_RUNNING", "MONITOR_PORTFOLIO"),
])
def test_state_to_action_projection(state, expected):
    assert ws.build_operator_action(overall=state)["action"] == expected


def test_unknown_overall_state_fails_closed_to_blocked():
    """A state this contract does not know must never get a reassuring action."""
    for bogus in ("", None, "TOTALLY_FINE", "DAILY_CYCLE_COMPLETE_XX"):
        out = ws.build_operator_action(overall=bogus)
        assert out["action"] == ws.OP_ACTION_BLOCKED
        assert out["failed_closed_on_unknown_state"] is True
        assert out["requires_operator_work"] is True


def test_post_close_research_obligation_resumes_instead_of_restarting():
    out = ws.build_operator_action(
        overall=ws.RESEARCH_CYCLE_REQUIRED,
        research_obligation={"obligation_outstanding": True,
                             "operational_close_valid": True,
                             "repeats_the_completed_close": False})
    assert out["action"] == ws.OP_ACTION_RESUME_RESEARCH
    assert out["resumes_research_without_repeating_close"] is True


def test_research_required_without_an_obligation_runs_the_cycle():
    out = ws.build_operator_action(overall=ws.RESEARCH_CYCLE_REQUIRED)
    assert out["action"] == ws.OP_ACTION_RUN_CYCLE
    assert out["resumes_research_without_repeating_close"] is False


@pytest.mark.parametrize("obligation,recovery", [
    # an obligation whose close is NOT valid is not a safe resume
    ({"obligation_outstanding": True, "operational_close_valid": False}, {}),
    # a cycle that would repeat the completed close is not a resume
    ({"obligation_outstanding": True, "operational_close_valid": True,
      "repeats_the_completed_close": True}, {}),
    # an outstanding catch-up outranks a resume: the close comes first
    ({"obligation_outstanding": True, "operational_close_valid": True},
     {"catch_up_required": True}),
])
def test_resume_refinement_is_withheld_unless_every_owner_fact_holds(
        obligation, recovery):
    out = ws.build_operator_action(overall=ws.RESEARCH_CYCLE_REQUIRED,
                                   research_obligation=obligation,
                                   session_recovery=recovery)
    assert out["action"] == ws.OP_ACTION_RUN_CYCLE
    assert out["resumes_research_without_repeating_close"] is False


def test_passive_actions_never_claim_operator_work():
    for state, passive in (("DAILY_CYCLE_COMPLETE", True),
                           ("WAITING_FOR_SESSION_CLOSE", True),
                           ("WAITING_FOR_OWNED_DATA", True),
                           ("READY_FOR_DAILY_CLOSE", False),
                           ("MANUAL_REVIEW_REQUIRED", False),
                           ("INCONSISTENT_STATE", False)):
        out = ws.build_operator_action(overall=state)
        assert out["is_passive"] is passive, state
        assert out["requires_operator_work"] is (not passive), state


def test_execution_fields_are_copies_of_the_one_command_never_invented():
    """The contract may not enable a control the command has not authorised."""
    withheld = ws.build_operator_action(
        overall=ws.READY_FOR_DAILY_CLOSE, command=_command())
    assert withheld["executes"] is False
    assert withheld["confirmation_required"] is None
    assert withheld["execution_contract"] is None

    authorised = ws.build_operator_action(
        overall=ws.READY_FOR_DAILY_CLOSE,
        command=_command(primary_action_available=True,
                         primary_action_label=ws.PORTFOLIO_CYCLE_LABEL,
                         primary_action_kind=ws.EXEC_PORTFOLIO_CYCLE,
                         primary_action_owner=ws.PORTFOLIO_CYCLE_OWNER,
                         confirmation_required=ws.PORTFOLIO_CYCLE_CONFIRMATION,
                         primary_action_execution_contract=dict(
                             ws.PORTFOLIO_CYCLE_EXECUTION_CONTRACT)))
    assert authorised["executes"] is True
    assert authorised["execution_label"] == ws.PORTFOLIO_CYCLE_LABEL
    assert authorised["execution_kind"] == ws.EXEC_PORTFOLIO_CYCLE
    assert authorised["confirmation_required"] == ws.PORTFOLIO_CYCLE_CONFIRMATION
    assert (authorised["execution_contract"]
            == dict(ws.PORTFOLIO_CYCLE_EXECUTION_CONTRACT))


def test_blocked_action_quotes_the_blockers_own_words():
    out = ws.build_operator_action(
        overall=ws.RESEARCH_CYCLE_BLOCKED,
        blockers=[{"code": "MONTHLY_INPUT_DUE",
                   "detail": "The monthly momentum input has no wired emitter."}],
        command=_command(why="something generic"))
    assert out["action"] == ws.OP_ACTION_BLOCKED
    assert out["why"] == "The monthly momentum input has no wired emitter."


def test_operator_action_is_safe_by_construction():
    for state in ws.OVERALL_STATES:
        out = ws.build_operator_action(overall=state)
        assert out["creates_orders"] is False
        assert out["approves_anything"] is False
        assert out["advances_operational_mark"] is False
        assert out["automation_enabled"] is False
        assert out["derived_in_ui"] is False
        assert out["owner"] == ws.WORKFLOW_STATE_OWNER
        assert out["priority_owner"] == "api.workflow_state._decide_overall"


def test_the_action_contract_reruns_no_priority_policy():
    """The projection must read the decided state, never re-decide it.

    The docstring is allowed to NAME the priority owner (it must, so the
    boundary is documented); the executable body may not reach for any input
    ``_decide_overall`` weighs.
    """
    src = inspect.getsource(ws.build_operator_action)
    body = src.replace(inspect.getdoc(ws.build_operator_action) or "", "")
    for forbidden in ("_decide_overall(", "session_status", "eligible_date",
                      "_coerce_date", "date.today", "datetime."):
        assert forbidden not in body, forbidden


def test_only_one_module_defines_the_operator_action_contract():
    hits = sorted(
        p.relative_to(REPO).as_posix()
        for p in (REPO / "api").glob("*.py")
        if "def build_operator_action(" in p.read_text(encoding="utf-8",
                                                       errors="replace"))
    assert hits == ["api/workflow_state.py"], hits


# =========================================================================== #
# PHASE D — THE LEGACY SCHEDULED-REVIEW CLOCK NO LONGER COMPETES
# =========================================================================== #
def test_review_clock_owner_still_declares_it_is_not_the_portfolio_cadence():
    """The declaration R55 consumes is the clock owner's, not R55's invention."""
    ob_src = (REPO / "api" / "operational_book.py").read_text(
        encoding="utf-8", errors="replace")
    assert '"review_scope": "SCHEDULED_MODEL_RECALIBRATION_CHECKPOINT"' in ob_src
    assert '"review_is_the_governing_portfolio_cadence": False' in ob_src


def test_gate_forwards_the_scope_declaration_verbatim():
    out = dag.evaluate_daily_action_gate(
        holdings={}, target={}, next_scheduled_full_review=LIVE_CHECKPOINT,
        scheduled_review_due=True, scheduled_review_scope=BOOK_REVIEW_SCOPE)
    scope = out["scheduled_review_scope"]
    assert scope["available"] is True
    assert scope["scope_owner"] == dag.REVIEW_SCOPE_OWNER == "api.operational_book"
    assert scope["decided_here"] is False
    for key in dag.REVIEW_SCOPE_FIELDS:
        assert scope[key] == BOOK_REVIEW_SCOPE[key], key


def test_gate_never_invents_a_scope_when_the_owner_published_none():
    out = dag.evaluate_daily_action_gate(
        holdings={}, target={}, next_scheduled_full_review=LIVE_CHECKPOINT,
        scheduled_review_due=True)
    scope = out["scheduled_review_scope"]
    assert scope["available"] is False
    for key in dag.REVIEW_SCOPE_FIELDS:
        assert scope[key] is None, key


def test_gate_date_bundle_carries_the_scope_beside_the_date():
    out = dag.evaluate_daily_action_gate(
        holdings={}, target={}, next_scheduled_full_review=LIVE_CHECKPOINT,
        scheduled_review_due=True, scheduled_review_scope=BOOK_REVIEW_SCOPE)
    assert out["scheduled_review_scope"]["review_scope"] == (
        "SCHEDULED_MODEL_RECALIBRATION_CHECKPOINT")


def test_classify_assessment_default_preserves_pre_r55_behaviour():
    """Silence is never read as a repair: an unstated scope keeps OVERDUE."""
    out = ws.classify_assessment(**_classify_args())
    assert out["status"] == ws.ASSESS_OVERDUE
    assert out["schedule_decided_status"] is True
    assert out["status_decided_by"] == ws.SCHEDULE_CLOCK_OWNER
    for silence in (None, True):
        again = ws.classify_assessment(
            **_classify_args(schedule_governs_portfolio_cadence=silence))
        assert again["status"] == ws.ASSESS_OVERDUE, silence


def test_a_non_governing_schedule_no_longer_decides_the_assessment_status():
    """THE R55 defect, on the exact live 2026-09-03 dates."""
    out = ws.classify_assessment(
        **_classify_args(schedule_governs_portfolio_cadence=False))
    assert out["status"] == ws.ASSESS_CURRENT
    assert out["status_decided_by"] == ws.ASSESSMENT_CURRENCY_OWNER
    assert out["schedule_decided_status"] is False
    # The recalibration obligation is SCOPED, never hidden.
    assert out["review_overdue"] is True
    assert out["review_due"] is True
    assert out["current_for_eligible_session"] is True
    assert out["assessment_age_sessions"] == 0


@pytest.mark.parametrize("assessment_date,expected", [
    # a genuinely older assessment is still STALE
    ("2026-08-28", ws.ASSESS_STALE),
    # an assessment newer than the eligible session is still INCONSISTENT
    ("2026-09-04", ws.ASSESS_INCONSISTENT),
    # no assessment at all is still MISSING
    (None, ws.ASSESS_MISSING),
])
def test_the_demotion_never_repairs_a_real_currency_problem(assessment_date,
                                                            expected):
    out = ws.classify_assessment(
        **_classify_args(assessment_date=assessment_date,
                         schedule_governs_portfolio_cadence=False))
    assert out["status"] == expected


def test_missing_and_inconsistent_still_outrank_the_schedule():
    """Precedence above the schedule is untouched by the demotion."""
    for date_, expected in ((None, ws.ASSESS_MISSING),
                            ("2026-09-04", ws.ASSESS_INCONSISTENT)):
        legacy = ws.classify_assessment(**_classify_args(assessment_date=date_))
        assert legacy["status"] == expected


def test_stale_component_row_is_demoted_to_the_audit_advisory_surface():
    stale, advisory = ams._stale_components(**_stale_kwargs())
    assert [r["component"] for r in stale] == []
    assert [r["component"] for r in advisory] == ["portfolio_reassessment"]
    row = advisory[0]
    assert row["advisory_only"] is True
    assert row["is_operator_problem"] is False
    assert row["surface"] == ams.ADVISORY_SURFACE
    # Nothing is deleted: the owner's raw token, the truthful label, the
    # self-explaining detail AND the demotion reason are all retained.
    assert row["owner_state"] == "OVERDUE"
    assert "Scheduled full review due" in row["display_label"]
    assert "current for the eligible session" in row["detail"]
    assert row["advisory_reason"] == ams.LEGACY_SCHEDULE_ADVISORY_REASON
    assert "MODEL RECALIBRATION" in row["advisory_reason"]


@pytest.mark.parametrize("detail_over", [
    # the assessment is NOT current for the session -> a real operator problem
    {"current_for_eligible_session": False},
    # the schedule did not decide the token -> this module cannot attribute it
    {"schedule_decided_status": False, "schedule_is_compatibility_only": None},
    # the owner never stated the review was overdue
    {"review_overdue": False},
])
def test_a_row_the_legacy_clock_cannot_explain_stays_an_operator_problem(
        detail_over):
    stale, advisory = ams._stale_components(**_stale_kwargs(
        reassessment=_reassessment_block(
            reassessment_freshness_detail=_freshness_detail(**detail_over))))
    assert [r["component"] for r in stale] == ["portfolio_reassessment"]
    assert advisory == []
    assert stale[0]["is_operator_problem"] is True
    assert stale[0]["surface"] == ams.STALE_SURFACE


def test_a_genuinely_stale_assessment_state_is_never_demoted():
    """A NOT_RUN / MISSING assessment is a different code path and stays loud."""
    for state in ("NOT_RUN", "UNAVAILABLE",
                  "STALE_CORPORATE_ACTION_REVIEW_REQUIRED"):
        stale, advisory = ams._stale_components(**_stale_kwargs(
            reassessment=_reassessment_block(state=state)))
        assert [r["component"] for r in stale] == ["portfolio_reassessment"], state
        assert advisory == [], state


def test_a_missing_component_is_never_demoted():
    stale, advisory = ams._stale_components(**_stale_kwargs(
        operational_book={"available": False},
        live_information={"available": False},
        reassessment={"available": False}))
    assert {r["component"] for r in stale} == {
        "operational_book", "live_information", "portfolio_reassessment"}
    assert advisory == []


def test_composed_state_publishes_both_surfaces_and_their_counts():
    state = ams.build_active_manager_state()
    assert "stale_components" in state and "advisory_components" in state
    assert state["stale_component_count"] == len(state["stale_components"])
    assert state["advisory_component_count"] == len(state["advisory_components"])
    assert state["component_surfaces"] == {"stale": ams.STALE_SURFACE,
                                           "advisory": ams.ADVISORY_SURFACE}


def test_the_authority_boundary_travels_in_the_freshness_detail():
    fd_keys = ("status_decided_by", "schedule_decided_status",
               "schedule_governs_portfolio_cadence", "schedule_scope",
               "schedule_is_compatibility_only",
               "portfolio_reassessment_cadence")
    for key in fd_keys:
        assert '"%s"' % key in AMS_SRC, key


def test_workflow_payload_names_which_clock_decided_the_status():
    for key in ("assessment_status_decided_by", "assessment_currency_owner",
                "schedule_decided_assessment_status",
                "scheduled_review_governs_portfolio_cadence",
                "scheduled_review_is_compatibility_only",
                "scheduled_review_scope"):
        assert '"%s"' % key in WS_SRC, key


def test_workflow_consumes_the_gates_declaration_and_invents_none():
    assert 'review_scope = (gate or {}).get("scheduled_review_scope")' in WS_SRC
    assert ('schedule_governs_portfolio_cadence=schedule_governs_portfolio_cadence'
            in WS_SRC)
    # The workflow owner must not hard-code the scope it is supposed to read.
    assert 'SCHEDULED_MODEL_RECALIBRATION_CHECKPOINT"' not in WS_SRC.replace(
        'SCHEDULE_CLOCK_SCOPE = "SCHEDULED_MODEL_RECALIBRATION_CHECKPOINT"', "")


def test_only_the_review_clock_owner_declares_the_scope():
    """No second module may decide what the scheduled-review date governs."""
    hits = sorted(
        p.relative_to(REPO).as_posix()
        for p in list((REPO / "api").glob("*.py")) + list((REPO / "engine").glob("*.py"))
        if '"review_is_the_governing_portfolio_cadence": False' in p.read_text(
            encoding="utf-8", errors="replace"))
    assert hits == ["api/operational_book.py"], hits


# =========================================================================== #
# PHASE C — THE THREE OPERATOR ANSWERS
# =========================================================================== #
def test_the_three_questions_are_frozen_and_ordered():
    assert len(ams.OPERATOR_ANSWER_QUESTIONS) == 3
    assert ams.OPERATOR_ANSWER_QUESTIONS[0].startswith(
        "WHAT IS THE CURRENT AUTHORITATIVE PORTFOLIO DECISION")
    assert ams.OPERATOR_ANSWER_QUESTIONS[1].startswith("WHAT HAS CHANGED SINCE")
    assert ams.OPERATOR_ANSWER_QUESTIONS[2].startswith("WHAT SHOULD THE OPERATOR DO")
    ans = _answer()
    assert ans["questions"] == list(ams.OPERATOR_ANSWER_QUESTIONS)
    assert list(ans)[2:5] == ["current_decision", "what_changed_since",
                              "what_to_do_now"]


def test_answer_one_is_the_governed_decision_owners_own_verdict():
    dec = _answer()["current_decision"]
    assert dec["available"] is True
    assert dec["headline"] == "NO PORTFOLIO CHANGE REQUIRED"
    assert dec["decision"] == "CURRENT_NO_CHANGE"
    assert dec["provenance"] == "GOVERNED_DAILY_CYCLE"
    assert dec["session"] == LIVE_SESSION
    assert dec["record_id"] == "drc_governed_drc_2026-09-02_15abfb01856f"
    assert dec["is_authoritative"] is True
    assert dec["authority_owner"] == "api.portfolio_decision"
    # A projection: the persistence fact passes through as the owner set it.
    assert dec["persisted"] is False


def test_answer_two_is_the_research_lane_and_is_never_authoritative():
    chg = _answer()["what_changed_since"]
    assert chg["available"] is True
    assert chg["is_authoritative"] is False
    assert chg["changes_the_authoritative_decision"] is False
    assert chg["material_events_evaluated"] == 20
    assert chg["affected_current_holdings_count"] == 0
    assert chg["latest_reassessment_conclusion"] == "HOLD"
    assert chg["supersedes_standing_decision"] is False
    assert "never becomes the authoritative decision" in chg[
        "why_this_is_not_the_decision"]


def test_zero_affected_holdings_is_explained_as_normal_not_as_a_failure():
    chg = _answer()["what_changed_since"]
    assert "opportunity cost" in chg["why_non_held_events_matter"]
    assert "normal outcome, not a failure" in chg["why_non_held_events_matter"]


def test_answer_two_only_changes_the_decision_when_the_gate_promoted_it():
    chg = _answer(lane=_lane(promoted_to_governed=True,
                             supersedes_standing_decision=True,
                             governance_state="GOVERNED"))["what_changed_since"]
    assert chg["changes_the_authoritative_decision"] is True
    assert chg["supersedes_standing_decision"] is True
    # Even then the lane itself is not the authority: the decision owner is.
    assert chg["is_authoritative"] is False


def test_answer_three_is_the_workflow_owners_action_verbatim():
    act = _answer()["what_to_do_now"]
    assert act["action"] == "WAIT_FOR_SESSION_CLOSE"
    assert act["requires_operator_work"] is False
    assert act["executes"] is False
    assert act["priority_owner"] == "api.workflow_state._decide_overall"
    assert act["action_vocabulary"] == list(ws.OPERATOR_ACTIONS)
    assert act["creates_orders"] is False
    assert act["approves_anything"] is False


def test_the_answer_block_declares_the_two_lanes_distinct():
    ans = _answer()
    assert ans["lanes_are_distinct"] is True
    assert ans["governed_lane_owner"] == "api.portfolio_decision"
    assert ans["research_lane_owner"] == "api.event_signal_refresh"
    assert ans["recomputes_nothing"] is True
    assert ans["identities_live_in_audit"] is True


def test_operator_times_are_eastern_and_come_from_the_clock_owner():
    assert msession.format_operator_timestamp(
        "2026-09-03T16:26:31.784900+00:00") == "Sep 3, 12:26 PM ET"
    assert msession.format_operator_timestamp(
        "2026-09-03T16:26:31.784900+00:00", with_date=False) == "12:26 PM ET"
    # A market DATE never gets a fabricated clock time.
    assert msession.format_operator_timestamp("2026-09-02") == "Sep 2, 2026"
    # Midnight and noon render as 12, not 0.
    assert msession.format_operator_timestamp(
        "2026-09-03T16:00:00+00:00") == "Sep 3, 12:00 PM ET"
    assert msession.format_operator_timestamp(
        "2026-09-03T04:30:00+00:00") == "Sep 3, 12:30 AM ET"


def test_a_missing_or_unusable_stamp_never_becomes_now():
    for bad in (None, "", "not-a-time", "2026-13-45"):
        assert msession.format_operator_timestamp(bad) is None
        assert ams._operator_display_time(bad) is None
    dec = _answer(governed=_governed(timestamp=None))["current_decision"]
    assert dec["decided_at_display"] is None


def test_display_strings_appear_beside_their_raw_identities():
    ans = _answer()
    dec, chg = ans["current_decision"], ans["what_changed_since"]
    assert dec["decided_at_display"] == "Sep 2, 7:51 PM ET"
    assert dec["decided_at"] == "2026-09-02T23:51:50.475243Z"
    assert chg["latest_reassessment_display"] == "Sep 3, 12:26 PM ET"
    assert chg["latest_reassessment_at"] == "2026-09-03T16:26:31.784900+00:00"


def test_the_answer_block_is_composed_in_exactly_one_module():
    hits = sorted(
        p.relative_to(REPO).as_posix()
        for p in (REPO / "api").glob("*.py")
        if "def _operator_answer_block(" in p.read_text(encoding="utf-8",
                                                        errors="replace"))
    assert hits == ["api/active_manager_state.py"], hits


def test_the_composed_state_publishes_the_answers():
    state = ams.build_active_manager_state()
    assert "operator_answer" in state
    assert state["operator_answer"]["schema_version"] == "operator_answer.v1"


# =========================================================================== #
# PHASE E — THE ACCEPTANCE CONTRACT AND MEASURED LATENCY
# =========================================================================== #
def test_acceptance_rows_are_the_frozen_ten():
    assert ams.ACCEPTANCE_ROWS == (
        "COLLECTION", "SIGNAL", "SCORING", "HOC", "REASSESSMENT", "GOVERNANCE",
        "GOVERNED_DECISION", "OPERATIONAL_BOOK", "NEXT_ACTION", "LATENCY")
    acc = ams.build_acceptance_contract({})
    assert [r["row"] for r in acc["rows"]] == list(ams.ACCEPTANCE_ROWS)
    assert acc["row_vocabulary"] == list(ams.ACCEPTANCE_ROWS)


def test_an_empty_state_reports_every_row_missing_and_invents_nothing():
    acc = ams.build_acceptance_contract({})
    assert acc["missing_rows"] == list(ams.ACCEPTANCE_ROWS)
    assert acc["present_count"] == 0
    assert acc["complete"] is False
    assert acc["manufactures_no_timestamp"] is True
    assert acc["read_only"] is True
    for row in acc["rows"]:
        assert row["status"] == ams.ACCEPTANCE_MISSING
        assert row["owner"], row["row"]


def test_acceptance_rows_quote_the_owner_that_decided_them():
    acc = ams.build_acceptance_contract({})
    by_row = {r["row"]: r["owner"] for r in acc["rows"]}
    assert by_row["HOC"] == "api.holding_opportunity_cost"
    assert by_row["REASSESSMENT"] == "api.portfolio_reassessment"
    assert by_row["GOVERNANCE"] == "api.portfolio_decision"
    assert by_row["GOVERNED_DECISION"] == "api.portfolio_decision"
    assert by_row["NEXT_ACTION"] == "api.workflow_state"
    assert by_row["LATENCY"] == "api.event_signal_refresh"


def test_acceptance_reports_present_when_the_owner_persisted_the_fact():
    state = {
        "live_information": _live_information(),
        "signal_state": {"last_scoring_ranking_date": LIVE_SESSION,
                         "scored_universe_count": 234,
                         "scoring_basis": {"scope": "FULL_UNIVERSE_RECOMPUTE"},
                         "scoring_status": "UNIVERSE_SCORING_READY"},
        "portfolio_reassessment": {
            "reassessment_id": "prs_2026-09-02_alpha_paper_book_1_029df5cdcda5",
            "reassessment_session": LIVE_SESSION,
            "current_decision": "CURRENT_NO_CHANGE",
            "hoc_summary": {"state": "READY", "assessment_hash": "702c599e"}},
        "live_reassessment_lane": _lane(),
        # R55.1 — the GOVERNANCE row's key fact is now the governance OWNER's
        # TERMINAL DISPOSITION, not a bare verdict string. A gate that promoted
        # a candidate publishes both, so this fixture carries both.
        "intraday_governance": {"verdict": "GOVERNED", "evaluated": True,
                                "disposition": "PROMOTED", "terminal": True,
                                "required": True,
                                "reason": "CANDIDATE_PROMOTED_TO_GOVERNED_DECISION",
                                "disposition_owner": "api.portfolio_decision"},
        "latest_governed_portfolio_decision": _governed(),
        "operational_book": {"operational_mark_date": LIVE_SESSION,
                             "nav": 97934.33, "holdings_count": 25},
        "operator_guidance": {"overall_state": "WAITING_FOR_SESSION_CLOSE",
                              "operator_action": ws.build_operator_action(
                                  overall=ws.WAITING_FOR_SESSION_CLOSE)},
        "decision_latency": {"observation_to_signal_seconds": 2013.6,
                             "measurement_owner": "api.event_signal_refresh"},
    }
    acc = ams.build_acceptance_contract(state)
    assert acc["missing_rows"] == []
    assert acc["present_count"] == 10
    assert acc["complete"] is True


def test_an_unevaluated_governance_gate_is_reported_missing_not_inferred():
    """The live 2026-09-03 gap: a lane may be ELIGIBLE with no gate verdict."""
    state = {"live_reassessment_lane": _lane(governance_state="ELIGIBLE"),
             "intraday_governance": {"evaluated": None, "verdict": None}}
    row = next(r for r in ams.build_acceptance_contract(state)["rows"]
               if r["row"] == "GOVERNANCE")
    assert row["status"] == ams.ACCEPTANCE_MISSING
    assert row["gate_evaluated"] is None
    assert row["verdict"] is None
    # The lane's own inference is reported BESIDE the missing verdict, never
    # substituted for it.
    assert row["lane_governance_state"] == "ELIGIBLE"


def test_latency_is_measured_by_the_owner_over_persisted_stage_stamps():
    """R55: a cycle that ran but was not promoted still yields real latency."""
    lat = ams._decision_latency_block(None, _live_information())
    assert lat["available"] is True
    assert lat["measurement_basis"] == "LIVE_EVENT_CYCLE_STAGE_TIMESTAMPS"
    assert lat["measurement_owner"] == "api.event_signal_refresh"
    assert lat["computed_here"] is False
    # observation 15:55:08.827 -> signal refresh 16:28:42.462
    assert lat["observation_to_signal_seconds"] == pytest.approx(2013.6, abs=0.2)
    # signal refresh 16:28:42.462 -> reassessment 16:28:50.940
    assert lat["signal_to_reassessment_seconds"] == pytest.approx(8.5, abs=0.2)


def test_the_governed_endpoints_stay_missing_without_a_governed_decision():
    lat = ams._decision_latency_block(None, _live_information())
    assert lat["reassessment_to_governed_seconds"] is None
    assert lat["observation_to_governed_seconds"] is None
    assert lat["latency_measurement_complete"] is False
    assert "governed_decision_persisted_at" in lat["missing_measurements"]


def test_a_promoted_cycles_own_latency_record_still_wins():
    record = {"latency": {"timestamps": {"x": "y"},
                          "observation_to_signal_seconds": 1.0,
                          "signal_to_reassessment_seconds": 2.0,
                          "reassessment_to_governed_seconds": 3.0,
                          "observation_to_governed_seconds": 6.0,
                          "latency_measurement_complete": True,
                          "missing_measurements": []}}
    lat = ams._decision_latency_block(record, _live_information())
    assert lat["measurement_basis"] == "GOVERNED_DECISION_LATENCY_RECORD"
    assert lat["observation_to_governed_seconds"] == 6.0
    assert lat["latency_measurement_complete"] is True


def test_latency_without_any_stamp_reports_unavailable_not_zero():
    lat = ams._decision_latency_block(None, {"available": True})
    assert lat["available"] is False
    assert lat["measurement_basis"] is None
    for key in ("observation_to_signal_seconds", "signal_to_reassessment_seconds",
                "reassessment_to_governed_seconds",
                "observation_to_governed_seconds"):
        assert lat[key] is None, key


def test_the_latency_measurement_function_is_the_owners_not_a_second_copy():
    assert callable(esr.measure_decision_latency)
    src = inspect.getsource(ams._decision_latency_block)
    # No timestamp arithmetic in the projection module.
    assert "total_seconds" not in src
    assert "fromisoformat" not in src
    assert "_measure_latency(" in src
    helper = inspect.getsource(ams._measure_latency)
    assert "esr.measure_decision_latency" in helper


def test_the_acceptance_contract_is_defined_in_exactly_one_module():
    hits = sorted(
        p.relative_to(REPO).as_posix()
        for p in (REPO / "api").glob("*.py")
        if "def build_acceptance_contract(" in p.read_text(encoding="utf-8",
                                                           errors="replace"))
    assert hits == ["api/active_manager_state.py"], hits


def test_the_composed_state_attaches_its_own_acceptance_view():
    state = ams.build_active_manager_state()
    assert state["acceptance"]["schema_version"] == "active_manager_acceptance.v1"
    assert state["acceptance"]["generated_at"] == state["generated_at"]


# =========================================================================== #
# PHASE C (UI) — THE BROWSER DERIVES NOTHING
# =========================================================================== #
def _r55_region() -> str:
    start = UI.find("/* R55_REGION_START */")
    end = UI.find("/* R55_REGION_END */")
    assert 0 <= start < end, "the R55 UI region markers are missing"
    return UI[start:end]


def test_the_three_answers_have_one_container_and_come_first():
    ans = UI.find('<div id="today-operator-answer"')
    cc = UI.find('<div id="today-command-center"')
    assert ans > 0 and cc > 0
    assert ans < cc, "the three answers must precede the command center"
    assert UI.count('id="today-operator-answer"') == 1


def test_the_panel_hides_inline_so_the_renderer_can_reveal_it():
    """Regression: a stylesheet ``display:none`` survives ``style.display=''``.

    The panel must hide through the INLINE style every other Today panel uses,
    or the renderer paints a correct panel that nobody can see.
    """
    node = UI[UI.find('<div id="today-operator-answer"'):]
    node = node[:node.find(">") + 1]
    assert 'style="display:none;"' in node
    rule_at = UI.find("#today-operator-answer {")
    assert rule_at > 0
    rule = UI[rule_at:UI.find("}", rule_at)]
    assert "display: none" not in rule and "display:none" not in rule


def test_the_ui_renders_each_of_the_three_answers_from_the_backend_block():
    region = _r55_region()
    for token in ("d.operator_answer", "a.current_decision",
                  "a.what_changed_since", "a.what_to_do_now"):
        assert token in region, token
    for token in ("dec.headline", "dec.provenance", "dec.decided_at_display",
                  "chg.material_events_evaluated",
                  "chg.affected_current_holdings_count",
                  "chg.latest_reassessment_display",
                  "chg.latest_reassessment_conclusion",
                  "act.action_label", "act.action_detail"):
        assert token in region, token


def test_the_ui_region_derives_nothing():
    region = _r55_region()
    derivation = sorted(set(
        re.findall(r"Math\.\w+\(", region)
        + re.findall(r"\.reduce\(", region)
        + re.findall(r"\.sort\(", region)
        + re.findall(r"new Date\(", region)
        + re.findall(r"Date\.now\(", region)
        + re.findall(r"toLocale\w*\(", region)
        + re.findall(r"getTimezoneOffset", region)
        + re.findall(r"_hash\s*[!=]==?", region)
        + re.findall(r"\bfetch\(", region)))
    assert derivation == [], derivation


def test_the_ui_region_holds_no_execution_or_approval_control():
    region = _r55_region()
    forbidden = sorted(set(
        re.findall(r"dispatchCanonicalPrimaryAction", region)
        + re.findall(r"CONFIRM_[A-Z_]+", region)
        + re.findall(r"\balert\(", region)
        + re.findall(r"\bconfirm\(", region)
        + re.findall(r"createOrder\w*", region)
        + re.findall(r"approve\w*\(", region)))
    assert forbidden == [], forbidden


def test_the_research_lane_can_never_masquerade_as_the_governed_decision():
    region = _r55_region()
    assert "RESEARCH LANE" in region
    assert "chg.changes_the_authoritative_decision" in region
    assert 'data-authoritative=' in region
    # The governed badge is only reachable through the backend's own promotion
    # verdict, never through a UI condition on a hash or a timestamp.
    promoted = region[region.find("var laneBadge"):]
    assert "chg.changes_the_authoritative_decision" in promoted[:200]


def test_the_diagnostic_strip_moved_inside_the_today_advanced_disclosure():
    adv = UI.find('<details id="today-advanced"')
    assert adv > 0
    adv_end = UI.find("</details>", adv)
    ops = UI.find('<div id="today-operating-state"')
    assert adv < ops < adv_end, "the operating-state strip must be in Advanced"
    for hosted in ('id="today-advisory"', 'id="today-acceptance"'):
        at = UI.find(hosted)
        assert adv < at < adv_end, hosted


def test_the_advanced_disclosure_keeps_every_diagnostic():
    """Demotion is not deletion: the R54 strip and Lane B are still rendered."""
    assert "function renderActiveManagerState(" in UI
    assert "_amsLaneBHtml" in UI
    assert "live_reassessment_lane" in UI
    region = _r55_region()
    assert "d.advisory_components" in region
    assert "d.acceptance" in region
    assert "lat.observation_to_signal_seconds" in region
    assert "lat.missing_measurements" in region


def test_the_advisory_rows_are_marked_as_not_an_operator_problem():
    region = _r55_region()
    assert 'data-operator-problem="0"' in region
    assert "r.advisory_reason" in region
    assert "not an operator problem" in region


def test_one_loader_still_owns_the_active_manager_read():
    assert UI.count("_opFetch('/v1/operations/active-manager-state')") == 1
    region = _r55_region()
    assert "/v1/operations/" not in region, "the R55 region must not fetch"


def test_the_renderers_are_invoked_from_the_one_composed_render():
    body = UI[UI.find("function renderActiveManagerState("):]
    head = body[:body.find("/* R54_REGION_END */")]
    for fn in ("_r55RenderOperatorAnswer", "_r55RenderAdvisory",
               "_r55RenderAcceptance"):
        assert fn in head, fn


def test_no_browser_dialog_anywhere_in_the_ui():
    assert re.search(r"[^.\w]alert\(", UI) is None
    assert re.search(r"[^.\w]confirm\(", UI) is None


# =========================================================================== #
# SAFETY — every new path is read-only
# =========================================================================== #
def test_the_composition_owner_still_declares_itself_read_only():
    state = ams.build_active_manager_state()
    assert state["read_only"] is True
    assert state["recomputes_nothing"] is True
    assert state["business_calculation_owner"] is False
    safety = state["safety"]
    for flag in ("performed_write", "wrote_to_ledger", "called_provider",
                 "called_prediction", "ran_daily_close", "ran_research_refresh",
                 "ran_portfolio_reassessment", "created_orders",
                 "created_proposals", "approved_anything", "promoted_model",
                 "automation_enabled", "broker_enabled"):
        assert safety[flag] is False, flag


def test_no_new_write_path_reaches_the_r55_surfaces():
    for name, src in (("active_manager_state", AMS_SRC),
                      ("workflow_state", WS_SRC)):
        for forbidden in ("requests.post", "httpx.post", "session.commit",
                          "record_decision(", "execute_daily_close(",
                          "promote_model(", "activate_sleeve("):
            assert forbidden not in src, "%s: %s" % (name, forbidden)


def test_the_r55_owners_reach_no_scheduler():
    for src in (AMS_SRC, WS_SRC, DAG_SRC):
        for forbidden in ("schtasks", "Register-ScheduledTask", "APScheduler",
                          "threading.Timer"):
            assert forbidden not in src, forbidden
