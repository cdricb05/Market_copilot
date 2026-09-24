r"""R69.3 - a Daily Close that is RUNNING must never read as a Daily Close that is DUE.

THE LIVE DEFECT THIS PINS (2026-09-24). The operator started the Portfolio Cycle. Run
``dcr_2026-09-23_alpha_paper_book_1_20260924T145906`` began at 14:59:06Z, refreshed owned
marks and appended the 2026-09-23 performance mark (NAV 97,973.38) at 14:59:15Z, spent
42 minutes inside stage 3 of 9 rebuilding the model-input universe from the provider, and
appended its close journal row at 15:41:56Z. Throughout those 42 minutes:

  * ``api.daily_close`` never read its OWN run record on the GET path, so it resolved
    ``DAILY_CLOSE_DUE`` (marks at 2026-09-23, journal still ending 2026-09-22) with
    ``primary_action.enabled: true`` and ``safe_to_rerun_close: true``;
  * ``api.workflow_state`` composed ``READY_FOR_DAILY_CLOSE``, an executable
    ``RUN_DAILY_CLOSE`` primary action, ``daily_close_gate.execution_allowed: true`` and
    ``operator_action_code: RUN_PORTFOLIO_CYCLE``;
  * ``api.data_freshness`` published "the completed close at 2026-09-23 remains valid"
    from the IN-FLIGHT run's bound session;
  * the RUN_ID appeared on NO operator surface, because the progress poll lived inside
    the browser closure of the POST;
  * ``updated_at`` was stamped only at stage boundaries, so at 15:44:15Z the 45-minute
    cutoff would have reported the healthy, actively-working run as
    ``RUN_FAILED_RECOVERABLE`` with ``safe_retry_allowed: true``.

Nothing was corrupted - the single-flight lock refuses a duplicate POST and writes
nothing, and the append-only journal and performance ledgers each hold exactly ONE
2026-09-23 row. The defect is the PROJECTION, and these are its regressions.

Every test is hermetic: its own desk directory, its own progress document, pure
resolvers. No live endpoint, no provider, no ledger, no close run, no selection, no
approval, no order confirmation.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from paper_trader.api import daily_close as dc
from paper_trader.api import data_freshness as df
from paper_trader.api import portfolio_cycle as pcycle
from paper_trader.api import workflow_state as ws

BOOK = "alpha_paper_book_1"
SESSION = "2026-09-23"
PRIOR = "2026-09-22"
RUN_ID = "dcr_2026-09-23_alpha_paper_book_1_20260924T145906"


def _now(offset_seconds: float = 0.0) -> str:
    return (datetime.now(tz=timezone.utc)
            + timedelta(seconds=offset_seconds)).isoformat()


def _progress_doc(*, stage: str = "RECALCULATE_DECISION_UNIVERSE",
                  updated_offset: float = -10.0,
                  heartbeat: bool = True,
                  journal_row_id=None,
                  running: bool = True) -> dict:
    """The REAL shape ``_CloseProgress`` writes (field-for-field as the live document)."""
    stages = []
    reached = False
    for key, label in dc.CLOSE_STAGES:
        if key == stage:
            status, reached = "current", True
        elif not reached:
            status = "done"
        else:
            status = "pending"
        stages.append({"key": key, "label": label, "status": status})
    doc = {
        "phase": "28B.2",
        "schema_version": dc.CLOSE_RUN_SCHEMA_VERSION,
        "run_id": RUN_ID,
        "idempotency_key": "%s|%s" % (BOOK, SESSION),
        "book_id": BOOK,
        "requested_by": "portfolio_cycle:manual_ui",
        "outcome": dc.RUN_RUNNING if running else dc.RUN_COMPLETED,
        "running": running,
        "done": not running,
        "market_date": SESSION,
        "evaluation_date": "2026-09-24",
        "started_at": _now(-2600.0),
        "updated_at": _now(updated_offset),
        "completed_at": None,
        "stage": stage,
        "stage_label": dict(dc.CLOSE_STAGES)[stage],
        "stage_changed_at": _now(-2560.0),
        "stages": stages,
        "completed_steps": [s["key"] for s in stages if s["status"] == "done"],
        "writes_occurred": True,
        "blocker": None, "failure": None, "settlement": None,
        "journal_row_id": journal_row_id,
        "warning": None,
        "final_close_status": None, "final_evidence_status": None,
    }
    if heartbeat:
        doc["heartbeat_interval_seconds"] = dc._HEARTBEAT_SECONDS
        doc["heartbeat_owner"] = "api.daily_close._CloseProgress"
        doc["pid"] = 4242
    return doc


def _write_progress(desk_dir: Path, doc: dict) -> None:
    desk_dir.mkdir(parents=True, exist_ok=True)
    (desk_dir / dc.CLOSE_PROGRESS_FILE).write_text(json.dumps(doc), encoding="utf-8")


# --------------------------------------------------------------------------- #
# 1. THE CLOSE OWNER
# --------------------------------------------------------------------------- #
def test_running_status_is_in_the_one_vocabulary_and_is_neither_processed_nor_rerunnable():
    assert dc.CLOSE_RUNNING in dc.ALL_CLOSE_STATUSES
    assert dc.CLOSE_RUNNING in dc._PRESENTATION, "every status must have operator copy"
    assert dc.CLOSE_RUNNING not in dc._CLOSE_PROCESSED_STATUSES
    assert dc.CLOSE_RUNNING not in dc._CLOSE_RERUNNABLE_STATUSES
    assert dc.CLOSE_RUNNING not in dc._RUNNABLE
    assert dc.CLOSE_RUNNING in dc._DISABLED_PRIMARY
    # It is NOT the duplicate-POST result token; the two answer different questions.
    assert dc.CLOSE_RUNNING != dc.CLOSE_IN_PROGRESS


def test_a_completed_close_claim_can_never_be_built_from_the_running_status():
    assert not dc.is_completed_close_status(dc.CLOSE_RUNNING)
    assert not dc.is_operational_close_complete(_progress_doc())
    # Even once the journal row has landed mid-run, the RUN has not completed.
    assert not dc.is_operational_close_complete(
        _progress_doc(stage="CAPTURE_MATURITY_PRICES",
                      journal_row_id="%s|%s" % (BOOK, SESSION)))


def test_resolver_returns_running_and_outranks_the_due_close_it_used_to_offer():
    """The exact live inputs: marks at 2026-09-23, journal still ending 2026-09-22."""
    kwargs = dict(initialized=True, book_active=True, pending_orders=0,
                  latest_eligible=SESSION, last_processed_date=PRIOR,
                  processed_decision_for_latest=None, forward_tracking=True)
    assert dc.resolve_daily_close_status(**kwargs) == dc.CLOSE_DUE      # the defect
    assert dc.resolve_daily_close_status(close_run_in_flight=True,
                                         **kwargs) == dc.CLOSE_RUNNING  # the repair


@pytest.mark.parametrize("extra", [
    dict(pending_orders=3),
    dict(processed_decision_for_latest=dc.DECISION_HOLD),
    dict(provider_ready=False),
    dict(valuation_complete=False),
    dict(baseline_required=True),
    dict(within_trading_day=True),
    dict(initialized=False, book_active=False),
])
def test_a_run_in_flight_outranks_every_other_resolver_branch(extra):
    """No other fact may reinstate an offerable close while the lock is held."""
    kwargs = dict(initialized=True, book_active=True, pending_orders=0,
                  latest_eligible=SESSION, last_processed_date=PRIOR,
                  processed_decision_for_latest=None, forward_tracking=True)
    kwargs.update(extra)
    assert dc.resolve_daily_close_status(close_run_in_flight=True,
                                         **kwargs) == dc.CLOSE_RUNNING


def test_primary_action_for_a_running_close_is_disabled_and_runs_no_close():
    pa = dc._primary_action(dc.CLOSE_RUNNING, book_active=True)
    assert pa["enabled"] is False
    assert pa["runs_daily_close"] is False


def test_active_close_run_projects_the_identity_and_stage_position():
    run = dc.active_close_run(_progress_doc())
    assert run is not None
    assert run["in_flight"] is True
    assert run["run_id"] == RUN_ID
    assert run["market_date"] == SESSION
    assert run["stage"] == "RECALCULATE_DECISION_UNIVERSE"
    assert run["stage_ordinal"] == 3
    assert run["stage_count"] == len(dc.CLOSE_STAGES) == 9
    assert run["requested_by"] == "portfolio_cycle:manual_ui"
    assert run["duplicate_submission_status"] == dc.CLOSE_IN_PROGRESS
    # Mid-run, before RECORD_DECISION, no completed close exists for the session.
    assert run["recorded_close_for_bound_session"] is False


def test_active_close_run_is_none_when_no_run_is_live():
    assert dc.active_close_run(None) is None
    assert dc.active_close_run({}) is None
    assert dc.active_close_run(_progress_doc(running=False)) is None


# --------------------------------------------------------------------------- #
# 2. A LONG STAGE IS NOT A DEAD PROCESS (the heartbeat)
# --------------------------------------------------------------------------- #
def test_a_long_stage_with_a_fresh_heartbeat_reads_as_running_not_recoverable(tmp_path):
    """The 2026-09-24 near-miss: 42 minutes in one stage, 2 minutes short of the cutoff."""
    d = tmp_path / "desk"
    doc = _progress_doc(updated_offset=-20.0)          # heartbeat 20s ago
    doc["stage_changed_at"] = _now(-2560.0)            # stage current for ~43 minutes
    _write_progress(d, doc)
    p = dc.load_close_progress(desk_dir=d)
    assert p["outcome"] == dc.RUN_RUNNING
    assert p["running"] is True and p["stale"] is False
    assert p["safe_retry_allowed"] is False
    assert p["liveness_basis"] == "HEARTBEAT"
    assert p["stage_age_seconds"] > 2400, "the STAGE is long"
    assert p["heartbeat_age_seconds"] < 60, "the PROCESS is alive"
    assert p["long_stage_is_not_a_failure"] is True


def test_a_heartbeat_that_stopped_is_a_dead_run_on_a_short_window(tmp_path):
    """A restart that kills a run stops the liveness claim in minutes, not 45."""
    d = tmp_path / "desk"
    _write_progress(d, _progress_doc(
        updated_offset=-(dc._HEARTBEAT_LIVENESS_SECONDS + 60)))
    p = dc.load_close_progress(desk_dir=d)
    assert p["running"] is False and p["stale"] is True
    assert p["outcome"] == dc.RUN_FAILED_RECOVERABLE
    assert p["safe_retry_allowed"] is True
    assert dc.active_close_run(p) is None


def test_a_legacy_document_without_a_heartbeat_keeps_the_45_minute_rule(tmp_path):
    """No stored document and no injected shape changes meaning."""
    d = tmp_path / "desk"
    _write_progress(d, _progress_doc(heartbeat=False, updated_offset=-1800.0))
    p = dc.load_close_progress(desk_dir=d)
    assert p["liveness_basis"] == "LEGACY_UPDATED_AT"
    assert p["liveness_window_seconds"] == dc._PROGRESS_STALE_MINUTES * 60.0
    assert p["running"] is True, "30 minutes is inside the legacy 45-minute window"


def test_the_stage_writer_stamps_liveness_and_stage_duration_separately(tmp_path):
    d = tmp_path / "desk"
    prog = dc._CloseProgress(d, market_date=SESSION, evaluation_date="2026-09-24",
                             book_id=BOOK, requested_by="pytest")
    try:
        assert prog._doc["heartbeat_interval_seconds"] == dc._HEARTBEAT_SECONDS
        prog.stage("VALUE_HOLDINGS")
        doc = json.loads((d / dc.CLOSE_PROGRESS_FILE).read_text(encoding="utf-8"))
        assert doc["stage"] == "VALUE_HOLDINGS"
        assert doc["stage_changed_at"], "stage duration has its own field"
        assert doc["updated_at"], "liveness has its own field"
    finally:
        prog.stop_heartbeat()
    assert dc._ACTIVE_PROGRESS is None, "the heartbeat must be deregistered"


def test_finalize_stops_the_heartbeat_so_a_finished_run_cannot_read_as_live(tmp_path):
    """A leaked pulse would keep stamping ``updated_at`` on a COMPLETED run, and this
    module's own liveness rule would then report a finished close as still executing."""
    d = tmp_path / "desk"
    prog = dc._CloseProgress(d, market_date=SESSION, evaluation_date="2026-09-24",
                             book_id=BOOK, requested_by="pytest")
    assert dc._ACTIVE_PROGRESS is prog
    prog.stage("VALUE_HOLDINGS")
    dc._progress_finalize(d, {"close_status": dc.CLOSE_COMPLETE_HOLD,
                              "forward_evidence_status": "FORWARD_EVIDENCE_COMPLETE",
                              "performed_write": True})
    assert dc._ACTIVE_PROGRESS is None, "finalize must deregister the heartbeat"
    assert prog._stop.is_set(), "the pulse must be stopped"
    p = dc.load_close_progress(desk_dir=d)
    assert p["running"] is False and p["done"] is True
    assert p["outcome"] == dc.RUN_COMPLETED
    assert dc.active_close_run(p) is None, "a finished run is never in flight"
    assert dc.is_operational_close_complete(p) is True


# --------------------------------------------------------------------------- #
# 3. THE WORKFLOW OWNER
# --------------------------------------------------------------------------- #
def test_the_workflow_vocabulary_is_total_over_the_new_state():
    assert ws.DAILY_CLOSE_RUNNING in ws.OVERALL_STATES
    assert ws.DAILY_CLOSE_RUNNING in ws.OPERATOR_ACTION_BY_OVERALL
    assert ws.DAILY_CLOSE_RUNNING in ws._EXPECTED_ACTION_FOR
    assert ws.DAILY_CLOSE_RUNNING in ws._PASSIVE_STATES
    # The research cycle already had this rule; the close now shares it.
    assert (ws.OPERATOR_ACTION_BY_OVERALL[ws.DAILY_CLOSE_RUNNING]
            == ws.OPERATOR_ACTION_BY_OVERALL[ws.RESEARCH_CYCLE_RUNNING]
            == ws.OP_ACTION_MONITOR)
    assert ws.OPERATOR_ACTION_BY_OVERALL[ws.DAILY_CLOSE_RUNNING] != ws.OP_ACTION_RUN_CYCLE


def test_a_run_in_flight_outranks_every_other_priority_gate_including_inconsistency():
    """A mid-run reading is transient by construction: marks have advanced, the journal
    row has not. INCONSISTENT_STATE there would send the operator to recovery."""
    hostile = dict(inconsistent=True, session_status="INCONSISTENT_FUTURE_DATA",
                   has_confirmed_eligible=False, eligible_session_closed=False,
                   owned_data_lag=True, research_current=False,
                   assessment_status=ws.ASSESS_INCONSISTENT,
                   manual_review_required=True, evidence_gap=True,
                   cycle_running=True, cycle_blocked=True, cycle_inconsistent=True,
                   catch_up_required=True, research_obligation_outstanding=True)
    assert ws._decide_overall(**hostile) == ws.INCONSISTENT_STATE
    assert ws._decide_overall(close_run_in_flight=True,
                              **hostile) == ws.DAILY_CLOSE_RUNNING


def test_the_primary_action_is_passive_and_names_the_run():
    pa = ws._primary_action(ws.DAILY_CLOSE_RUNNING, {
        "eligible_date": SESSION,
        "close_run": dc.active_close_run(_progress_doc())})
    assert pa["action_code"] == ws.ACTION_MONITOR_DAILY_CLOSE
    assert pa["execution_available"] is False
    assert pa.get("execution_kind") is None, "no executable close while one is running"
    assert RUN_ID in pa["explanation"]
    assert "stage 3 of 9" in pa["explanation"]
    assert ws._EXPECTED_ACTION_FOR[ws.DAILY_CLOSE_RUNNING] == pa["action_code"]


def test_every_secondary_close_surface_is_forbidden_from_offering_a_close():
    gate = ws.build_daily_close_gate(ws.DAILY_CLOSE_RUNNING, eligible_date=SESSION,
                                     latest_close_date=SESSION,
                                     provider_confirms_owed_session=True)
    assert gate["execution_allowed"] is False, \
        "a provider-ready session must NOT reopen the close while a run holds the lock"
    assert gate["passive_badge"] == "RUNNING"
    assert "do not start another" in gate["passive_status"]


def test_an_in_flight_read_is_not_reported_as_a_failed_close():
    """R69's lesson, on the close lane: 'did not complete' is a failure statement."""
    run = dc.active_close_run(_progress_doc())
    without = ws.build_evidence_presentation(
        operational_close_valid=False, latest_close_date=SESSION, evidence_gap=False,
        active_book_snapshot_present=False, current_session_open=False)
    assert without["latest_completed_close"]["state"] == "NOT_COMPLETED"
    assert "did not complete" in without["latest_completed_close"]["explanation"]

    with_run = ws.build_evidence_presentation(
        operational_close_valid=False, latest_close_date=SESSION, evidence_gap=False,
        active_book_snapshot_present=False, current_session_open=False, close_run=run)
    comp = with_run["latest_completed_close"]
    assert comp["state"] == "IN_PROGRESS"
    assert "did not complete" not in comp["explanation"]
    assert "has not failed" in comp["explanation"]
    assert comp["run_id"] == RUN_ID


def test_research_obligation_says_not_yet_determinable_rather_than_nothing_owed():
    common = dict(latest_completed_close_date=None, operational_close_valid=False,
                  eligible_market_date=SESSION, research_current=False,
                  stale_input_ids=["price_score_refresh"])
    quiet = ws.build_research_obligation(**common)
    assert quiet["research_obligation_state"] == ws.NO_RESEARCH_OBLIGATION
    assert quiet["obligation_determinable"] is True

    during = ws.build_research_obligation(close_run_in_flight=True, **common)
    assert during["research_obligation_state"] == ws.NO_RESEARCH_OBLIGATION, \
        "an in-flight close creates no obligation it has not yet produced"
    assert during["close_run_in_flight"] is True
    assert during["obligation_determinable"] is False
    assert "Not yet determinable" in during["summary"]
    assert "no completed operational close is outstanding" not in during["summary"].lower()


# --------------------------------------------------------------------------- #
# 4. THE ORCHESTRATOR
# --------------------------------------------------------------------------- #
def test_the_portfolio_cycle_refuses_by_name_and_never_as_recovery_required():
    wf = {"overall_state": ws.DAILY_CLOSE_RUNNING,
          "primary_action": ws._primary_action(ws.DAILY_CLOSE_RUNNING, {
              "eligible_date": SESSION,
              "close_run": dc.active_close_run(_progress_doc())}),
          "close_run": dc.active_close_run(_progress_doc())}
    plan = pcycle.plan_next_step(wf)
    assert plan["step"] is None, "no owner is invoked"
    assert plan["stop_reason"] == pcycle.STOP_CYCLE_ALREADY_RUNNING
    assert plan["stop_reason"] != pcycle.STOP_RECOVERY_REQUIRED
    assert RUN_ID in plan["reason"]
    assert "stage 3 of 9" in plan["reason"]


def test_repeated_portfolio_cycle_invocation_stays_a_refusal():
    """Pressing the button again must keep refusing, identically, forever."""
    wf = {"overall_state": ws.DAILY_CLOSE_RUNNING, "primary_action": {},
          "close_run": dc.active_close_run(_progress_doc())}
    reasons = {pcycle.plan_next_step(wf)["stop_reason"] for _ in range(5)}
    assert reasons == {pcycle.STOP_CYCLE_ALREADY_RUNNING}


# --------------------------------------------------------------------------- #
# 5. NO SURFACE MAY CALL AN IN-FLIGHT SESSION A COMPLETED CLOSE
# --------------------------------------------------------------------------- #
def test_the_close_owner_publishes_the_bound_session_and_the_completed_one_separately(tmp_path):
    """``market_date`` is the session a run is BOUND to. The most recent session a close
    actually COMPLETED is a different date for the whole duration of that run."""
    d = tmp_path / "desk"
    _write_progress(d, _progress_doc())
    (d / dc.DAILY_CLOSE_JOURNAL_FILE).write_text(json.dumps({
        "phase": "t", "ledger": "daily_close", "append_only": True,
        "rows": [{"seq": 1, "event": dc.DAILY_CLOSE_EVENT, "book_id": BOOK,
                  "market_date": PRIOR,
                  "close_status": dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT,
                  "decision": dc.DECISION_MEMBERSHIP_DRIFT}]}), encoding="utf-8")
    p = dc.load_close_progress(desk_dir=d)
    assert p["market_date"] == SESSION, "the run is bound to 2026-09-23"
    assert p["last_completed_close_date"] == PRIOR, "but 2026-09-22 is the last COMPLETED"
    assert p["market_date_is_bound_session"] is True


def test_data_freshness_does_not_publish_an_in_flight_session_as_a_completed_close():
    live = _progress_doc()
    live["last_completed_close_date"] = PRIOR
    assert df._close_run_in_flight(live) is True
    dates = df._extract_dates(operational={}, inputs={}, daily_close_status=live,
                              desk_marks=None, daily_status=None, forward_status=None,
                              overrides=None)
    assert dates["daily_close_date"] == PRIOR, \
        "the bound session of a running run is not a completed close"

    finished = _progress_doc(running=False)
    finished["last_completed_close_date"] = PRIOR
    dates2 = df._extract_dates(operational={}, inputs={}, daily_close_status=finished,
                               desk_marks=None, daily_status=None, forward_status=None,
                               overrides=None)
    assert dates2["daily_close_date"] == SESSION


def test_the_freshness_row_and_its_authority_agree_for_the_whole_run():
    """Blanking the row, or comparing the COMPLETED date against the session the run is
    merely BOUND to, would each raise a false alarm for the duration of a healthy run.
    The row and the value the consistency check compares it against must be one fact."""
    live = _progress_doc()
    live["last_completed_close_date"] = PRIOR
    row_value = df._extract_dates(
        operational={}, inputs={}, daily_close_status=live, desk_marks=None,
        daily_status=None, forward_status=None, overrides=None)["daily_close_date"]
    authority = (live.get("last_completed_close_date")
                 if df._close_run_in_flight(live) else live.get("market_date"))
    assert row_value is not None, "the row must never be blanked by a running close"
    assert row_value == authority == PRIOR, \
        "freshness row and consistency authority must be the SAME completed-close fact"


def test_an_injected_status_shape_without_a_run_record_is_unchanged():
    """Callers that pass a status dict (not a run record) keep their exact meaning."""
    shape = {"market_date": SESSION, "last_processed_market_date": PRIOR}
    assert df._close_run_in_flight(shape) is False
    dates = df._extract_dates(operational={}, inputs={}, daily_close_status=shape,
                              desk_marks=None, daily_status=None, forward_status=None,
                              overrides=None)
    assert dates["daily_close_date"] == SESSION


# --------------------------------------------------------------------------- #
# 6. SAFETY - this slice writes nothing and decides nothing
# --------------------------------------------------------------------------- #
def test_reading_a_running_close_writes_nothing_and_creates_no_duplicate(tmp_path):
    d = tmp_path / "desk"
    _write_progress(d, _progress_doc())
    path = d / dc.CLOSE_PROGRESS_FILE
    before = path.read_bytes()
    names_before = sorted(q.name for q in d.iterdir())
    for _ in range(3):
        p = dc.load_close_progress(desk_dir=d)
        run = dc.active_close_run(p)
        assert run["run_id"] == RUN_ID
    assert path.read_bytes() == before, "a read may never mutate the run record"
    assert sorted(q.name for q in d.iterdir()) == names_before, \
        "no journal, ledger, mark, proposal or evidence file is created by a read"


def test_the_running_projection_approves_confirms_and_selects_nothing():
    run = dc.active_close_run(_progress_doc())
    blob = json.dumps(run).lower()
    for forbidden in ("approved", "confirmed_order_plan", "selection_id",
                      "created_orders\": true", "created_fills"):
        assert forbidden not in blob
    pa = ws._primary_action(ws.DAILY_CLOSE_RUNNING,
                            {"eligible_date": SESSION, "close_run": run})
    assert pa["execution_available"] is False
    assert pa["manual_confirmation_required"] is False
