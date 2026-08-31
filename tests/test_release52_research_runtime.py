"""Release 52 - the persistent prospective research runtime.

What is locked shut here:

* **Timing is derived, never invented.** The emission policy protects the
  next entry slot from stale-input burn on weekdays, fails open to a LEGAL
  stale emission at the final retry, and treats weekends as duplicate-safe.
  Month-end and weekly cadences come from the canonical lane predicates.
* **A forfeited opportunity is first-class state.** The ledger is
  append-only, chain-hashed with the canonical desk primitives, idempotent
  on (lane, scope, decision date), refuses any row that does not refuse
  backfill, and its sweep detects missed batch slots, missed month-ends and
  mirrors the continuation owner's recorded refusals - exactly once each.
* **One runtime path, one instance.** A second concurrent invocation is
  refused and reported; a stale lock (dead pid) is reclaimed; a broken
  evidence chain fails the run CLOSED before anything is written.
* **The frontier refresh can surface PROMOTION_READY and can approve
  nothing.** A state transition is recorded exactly once.
* **The scheduler deliverables follow the estate's contracts.** No exit
  statements, one task, explicit migration, and a validator that reports an
  absent or malformed task as a blocker.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import subprocess
from pathlib import Path

import pytest

from alpha_agent import r46 as R46
from alpha_agent import r52 as R52
from alpha_agent.r46 import adopted_forward as AF
from alpha_agent.r46 import challengers as CH
from alpha_agent.r46 import clock as CK
from alpha_agent.r46 import contract as C
from alpha_agent.r46 import lanes as LN
from alpha_agent.r46 import ledger as LG
from alpha_agent.r46 import registry as RG
from alpha_agent.r46 import runlock as RL
from alpha_agent.r52 import forfeiture as FF
from alpha_agent.r52 import frontier_refresh as FR
from alpha_agent.r52 import runtime as RT
from alpha_agent.r52 import timing_contract as TC
from alpha_agent.r52 import velocity_ops as VO

REPO = Path(__file__).resolve().parents[1]

TEST_CAMPAIGN = "r52_pytest_campaign"


def _utc(y, m, d, hh=0, mm=0):
    return dt.datetime(y, m, d, hh, mm, tzinfo=dt.timezone.utc)


def _pin_owned_session(monkeypatch, d):
    """Pin the owned-data freshness probe for a hermetic runtime test.

    ``evaluate_emission_policy`` documents ``last_session`` as the injectable
    seam, and the runtime reaches it through the ONE canonical probe
    ``TC.owned_last_session`` - which in production reads the LIVE owned
    Norgate store. A hermetic test must therefore pin BOTH the instant AND
    this probe: with only the instant pinned, the verdict flips the moment
    the real nightly refresh prints the pinned day's bar (observed
    2026-08-31 - the Monday-morning suppression case became EMIT_OK_FRESH
    once pytest ran after the evening data delivery). The wall clock and the
    live store are never allowed to decide a pinned scenario.
    """
    monkeypatch.setattr(TC, "owned_last_session", lambda: d)


@pytest.fixture()
def sandbox(tmp_path, monkeypatch):
    """Every R46 and R52 write goes to a temp root. Production is untouched.

    The repo is importable both as top-level packages (``alpha_agent``) and
    as ``paper_trader.alpha_agent`` (the API modules use the second form), so
    BOTH module instances are pointed at the temp root - otherwise a read
    through the API would silently reach the production store.
    """
    from paper_trader.alpha_agent import r46 as PT_R46
    from paper_trader.alpha_agent import r52 as PT_R52
    monkeypatch.setattr(R46, "RESEARCH_ROOT", tmp_path / "r46root")
    monkeypatch.setattr(R52, "RUNTIME_ROOT", tmp_path / "r52root")
    monkeypatch.setattr(PT_R46, "RESEARCH_ROOT", tmp_path / "r46root")
    monkeypatch.setattr(PT_R52, "RUNTIME_ROOT", tmp_path / "r52root")
    monkeypatch.setenv("PAPER_TRADER_ACCEPTANCE_MODE", "1")
    return tmp_path


# =========================================================================== #
# PART A - the derived timing contract (scenarios A, H, I, J)
# =========================================================================== #
def test_weekday_morning_emission_is_suppressed_to_protect_the_slot():
    # Monday 2026-08-31 09:00 ET, Friday's session is the freshest owned bar.
    p = TC.evaluate_emission_policy(_utc(2026, 8, 31, 13, 0),
                                    last_session=dt.date(2026, 8, 28))
    assert p["emit"] is False
    assert p["mode"] == TC.EMIT_SUPPRESSED_DATA_PENDING
    assert p["entry_session_date"] == "2026-09-01"


def test_post_refresh_evening_emission_is_fresh_and_allowed():
    # Monday 18:00 ET with Monday's bars printed.
    p = TC.evaluate_emission_policy(_utc(2026, 8, 31, 22, 0),
                                    last_session=dt.date(2026, 8, 31))
    assert p["emit"] is True
    assert p["mode"] == TC.EMIT_OK_FRESH


def test_final_retry_fails_open_to_a_legal_stale_emission():
    # Monday 21:45 ET (01:45Z Tuesday) with the data path still stale.
    p = TC.evaluate_emission_policy(_utc(2026, 9, 1, 1, 45),
                                    last_session=dt.date(2026, 8, 28))
    assert p["emit"] is True
    assert p["mode"] == TC.EMIT_OK_STALE_FINAL
    # The eastern date is still Monday, so the slot is still Tuesday's.
    assert p["entry_session_date"] == "2026-09-01"


def test_weekend_invocation_is_duplicate_safe_and_allowed():
    p = TC.evaluate_emission_policy(_utc(2026, 8, 30, 21, 0),
                                    last_session=dt.date(2026, 8, 28))
    assert p["emit"] is True
    assert p["mode"] == TC.EMIT_OK_WEEKEND


def test_emission_never_precedes_its_own_outcome_window():
    for when in (_utc(2026, 8, 31, 13), _utc(2026, 8, 31, 22),
                 _utc(2026, 9, 1, 1, 45), _utc(2026, 8, 30, 21)):
        p = TC.evaluate_emission_policy(when,
                                        last_session=dt.date(2026, 8, 28))
        assert CK.iso(when) < p["slot_cutoff_utc"]


def test_month_end_predicate_resolves_2026_08_31_as_the_decision_date():
    d = LN.due_month_end(dt.date(2026, 8, 31))
    assert d["due"] is True
    early = LN.due_month_end(dt.date(2026, 8, 28))
    assert early["due"] is False
    assert early["next_decision_date"] == "2026-08-31"


def test_weekly_predicate_is_a_call_cadence_not_a_decision_grid():
    fri = LN.due_weekly_friday(dt.date(2026, 9, 4))
    assert fri["due"] is True
    tue = LN.due_weekly_friday(dt.date(2026, 9, 1))
    assert tue["due"] is False
    assert tue["next_decision_date"] is None
    assert tue["next_decision_date_source"] == LN.CALL_CADENCE_ONLY


def test_timing_contract_is_derived_from_the_owners(sandbox):
    body = TC.build(_utc(2026, 8, 31, 13, 0))
    assert body["scheduler_is_not_a_timing_authority"] is True
    assert body["backfill_after_window_open"] == "REFUSED_ALWAYS"
    lanes = {r["lane_id"]: r for r in body["lanes"]}
    assert TC.DAILY_BATCH_LANE in lanes
    assert TC.OUTCOME_SCORING_LANE in lanes
    for lane in LN.registry():
        assert lane.lane_id in lanes
    assert (sandbox / "r52root" / TC.ARTIFACT).exists()


def test_invocation_plan_has_a_sweep_and_a_fail_open_final_retry():
    purposes = [t["purpose"] for t in TC.INVOCATION_PLAN]
    assert "SCORING_AND_FORFEITURE_SWEEP" in purposes
    assert "FINAL_RETRY_FAIL_OPEN" in purposes
    assert len({t["local_time"] for t in TC.INVOCATION_PLAN}) == len(
        TC.INVOCATION_PLAN)


# =========================================================================== #
# PART B - the forfeiture ledger (scenarios B, C, E, K)
# =========================================================================== #
def _no_preds(monkeypatch):
    monkeypatch.setattr(LG, "predictions", lambda cid=None: [])
    monkeypatch.setattr(RG, "load", lambda cid=None: {"challengers": [
        {"challenger_id": "x", "state": "FORWARD_PENDING"}]})


def test_missed_batch_slots_are_forfeited_once_and_only_once(
        sandbox, monkeypatch):
    _no_preds(monkeypatch)
    monkeypatch.setattr(AF, "predictions", lambda cid=None: [])
    now = _utc(2026, 9, 2, 13, 0)          # Wed 09:00 ET
    first = FF.sweep(now, scheduler_state="TEST")
    # Sep 1 and Sep 2 entry windows are both open with zero rows on record.
    batch = [r for r in first["rows"] if r["lane_id"] == "r46_daily_batch"]
    assert {r["decision_date"] for r in batch} == {"2026-09-01", "2026-09-02"}
    assert all(r["backfill_refused"] is True for r in first["rows"])
    again = FF.sweep(now, scheduler_state="TEST")
    assert again["n_appended"] == 0
    assert again["n_total_forfeitures"] == first["n_total_forfeitures"]
    assert FF.verify()["all_intact"] is True


def test_a_window_not_yet_open_is_never_forfeited(sandbox, monkeypatch):
    _no_preds(monkeypatch)
    monkeypatch.setattr(AF, "predictions", lambda cid=None: [])
    # Monday 2026-08-31 09:00 ET: no window has opened since accountability.
    body = FF.sweep(_utc(2026, 8, 31, 13, 0), scheduler_state="TEST")
    assert [r for r in body["rows"]
            if r["lane_id"] == "r46_daily_batch"] == []


def test_a_used_slot_is_not_forfeited(sandbox, monkeypatch):
    monkeypatch.setattr(LG, "predictions", lambda cid=None: [
        {"effective_as_of": "2026-09-01"}])
    monkeypatch.setattr(RG, "load", lambda cid=None: {"challengers": []})
    monkeypatch.setattr(AF, "predictions", lambda cid=None: [])
    body = FF.sweep(_utc(2026, 9, 1, 13, 0), scheduler_state="TEST")
    assert [r for r in body["rows"]
            if r["lane_id"] == "r46_daily_batch"] == []


def test_month_end_lanes_forfeit_per_shadow_when_nothing_was_recorded(
        sandbox, monkeypatch):
    _no_preds(monkeypatch)
    monkeypatch.setattr(AF, "predictions", lambda cid=None: [])
    body = FF.sweep(_utc(2026, 9, 1, 13, 0), scheduler_state="TEST")
    me = [r for r in body["rows"] if r["lane_id"].endswith("fut_month_end")]
    assert {r["decision_date"] for r in me} == {"2026-08-31"}
    assert {r["challenger_scope"] for r in me} == {
        "shadow_wide_xs", "shadow_carry_rule_xs",
        "shadow_intl_rates_carry_rv", "shadow_slot5_c39_fad367467c79"}


def test_an_emitted_month_end_is_not_forfeited(sandbox, monkeypatch):
    _no_preds(monkeypatch)
    rows = [{"adopted_challenger_id": s, "decision_date": "2026-08-31"}
            for s in ("shadow_wide_xs", "shadow_carry_rule_xs",
                      "shadow_intl_rates_carry_rv",
                      "shadow_slot5_c39_fad367467c79")]
    monkeypatch.setattr(AF, "predictions", lambda cid=None: rows)
    body = FF.sweep(_utc(2026, 9, 1, 13, 0), scheduler_state="TEST")
    assert [r for r in body["rows"]
            if r["lane_id"].endswith("fut_month_end")] == []


def test_recorded_refusals_are_mirrored_verbatim_exactly_once(
        sandbox, monkeypatch):
    _no_preds(monkeypatch)
    monkeypatch.setattr(AF, "predictions", lambda cid=None: [])
    art = {
        "refused_decision_dates": {"r39_vx_weekly": [{
            "decision_date": "2026-08-25", "shadow_id": "shadow_vx_carry_ts",
            "outcome_window_start_utc": "2026-08-26T04:00:00Z",
            "emission_attempted_utc": "2026-08-30T21:50:38Z",
            "hours_late": 113.844, "reason": "OUTCOME_WINDOW_ALREADY_OPEN"}]},
        "emission_feasibility": {"r39_vx_weekly": "STRUCTURALLY_LATE"},
    }
    d = R46.campaign_dir()
    (d / AF.ARTIFACT).write_text(json.dumps(art), encoding="utf-8")
    body = FF.sweep(_utc(2026, 8, 31, 13, 0), scheduler_state="TEST")
    vx = [r for r in body["rows"] if r["lane_id"] == "r39_vx_weekly"]
    assert len(vx) == 1
    assert vx[0]["reason"] == FF.REASON_STRUCTURAL
    assert vx[0]["backfill_refused"] is True
    assert vx[0]["evidence"]["hours_late"] == 113.844
    again = FF.sweep(_utc(2026, 8, 31, 14, 0), scheduler_state="TEST")
    assert len([r for r in again["rows"]
                if r["lane_id"] == "r39_vx_weekly"]) == 1


def test_the_ledger_refuses_a_row_that_does_not_refuse_backfill(sandbox):
    with pytest.raises(ValueError):
        FF.append([{"lane_id": "x", "challenger_scope": "y",
                    "decision_date": "2026-08-31", "reason": "r",
                    "backfill_refused": False}])


# =========================================================================== #
# PART C - the runtime lock and the one runtime path (scenarios F, G, J)
# =========================================================================== #
def test_lock_is_exclusive_reclaims_dead_holders_and_reports(sandbox):
    path = R52.runtime_dir() / "test.lock"
    r1 = RL.acquire_path(path, "one", wait_s=0)
    assert r1["acquired"] is True
    with pytest.raises(RL.AdvanceLockBusy):
        RL.acquire_path(path, "two", wait_s=0)
    assert RL.release_path(path, "one") is True
    # a dead pid's lock is reclaimed immediately
    path.write_text(json.dumps({"holder": "ghost", "pid": 999999999}),
                    encoding="utf-8")
    r2 = RL.acquire_path(path, "three", wait_s=0)
    assert r2["acquired"] is True
    assert r2["reclaimed_stale"]["previous_holder"] == "ghost"
    RL.release_path(path, "three")


def test_release_never_removes_someone_elses_lock(sandbox):
    path = R52.runtime_dir() / "test2.lock"
    RL.acquire_path(path, "owner", wait_s=0)
    assert RL.release_path(path, "not_owner") is False
    assert path.exists()
    assert RL.release_path(path, "owner") is True


def _stub_advance(monkeypatch, calls):
    from alpha_agent.r46 import advance as AD

    def fake(campaign_id, **kw):
        calls.append(kw)
        return {"state": AD.STATE_ADVANCED, "available": True,
                "tournament_outcomes_scored": 1,
                "tournament_predictions_emitted": 2,
                "tournament_forward_evidence_count": 10,
                "tournament_challengers_active": 5,
                "pending_predictions": 3, "n_stage_failures": 0,
                "emission": {"n_offered": 2, "n_duplicates_skipped": 0},
                "evidence_velocity": {}, "shadow_pnl": {}, "lanes": {},
                "ledger_chain_intact": True, "pnl_as_of": "2026-08-28"}
    monkeypatch.setattr(AD, "advance", fake)


def _stub_frontier_inputs(monkeypatch):
    monkeypatch.setattr(FR, "_declared_sleeves", lambda: [])
    monkeypatch.setattr(FR, "_unit_economics", lambda sleeves: {})
    monkeypatch.setattr(FR, "_nav_usd", lambda: 100000.0)


def test_runtime_cycle_runs_every_stage_and_writes_health(
        sandbox, monkeypatch):
    calls = []
    _stub_advance(monkeypatch, calls)
    _stub_frontier_inputs(monkeypatch)
    monkeypatch.setattr(AF, "predictions", lambda cid=None: [])
    monkeypatch.setattr(LG, "predictions", lambda cid=None: [])
    monkeypatch.setattr(RG, "load", lambda cid=None: {"challengers": []})
    # Monday 09:00 ET with Friday the freshest OWNED bar - both pinned.
    _pin_owned_session(monkeypatch, dt.date(2026, 8, 28))
    body = RT.research_runtime_cycle(_utc(2026, 8, 31, 13, 0),
                                     trigger="PYTEST")
    assert body["state"] in (RT.RUN_COMPLETED,
                             RT.RUN_COMPLETED_WITH_FAILURES)
    names = [s["stage"] for s in body["stages"]]
    for stage in ("runtime_lock", "timing_contract", "chain_integrity",
                  "tournament_advance", "forfeiture_sweep",
                  "velocity_operational", "promotion_frontier"):
        assert stage in names
    assert len(calls) == 1
    # weekday morning: the batch emission must have been suppressed
    assert calls[0]["emit_batch"] is False
    by = {s["stage"]: s for s in body["stages"]}
    assert by["tournament_advance"]["emit_batch_requested"] is False
    assert (by["tournament_advance"]["emission_mode"]
            == TC.EMIT_SUPPRESSED_DATA_PENDING)
    # the runtime's frozen safety facts (research only, never operational)
    assert body["calls_portfolio_cycle"] is False
    assert body["runs_daily_close"] is False
    assert body["promotes_models"] is False
    assert body["backfills"] is False
    health = RT.load_health()
    assert health["runtime_state"] == body["state"]
    assert health["forward_chain_integrity"] is True
    assert health["next_expected_invocation"]["local_time"] in {
        t["local_time"] for t in TC.INVOCATION_PLAN}
    runs = RT.load_runs()
    assert runs["n_runs_total"] == 1


def test_runtime_emits_when_a_legal_emission_is_due(sandbox, monkeypatch):
    """The other half of the pinned pair: post-refresh Monday evening with
    Monday's OWNED bar printed - the policy grants the slot and the runtime
    passes ``emit_batch=True`` to the one advance."""
    calls = []
    _stub_advance(monkeypatch, calls)
    _stub_frontier_inputs(monkeypatch)
    monkeypatch.setattr(AF, "predictions", lambda cid=None: [])
    monkeypatch.setattr(LG, "predictions", lambda cid=None: [])
    monkeypatch.setattr(RG, "load", lambda cid=None: {"challengers": []})
    _pin_owned_session(monkeypatch, dt.date(2026, 8, 31))
    body = RT.research_runtime_cycle(_utc(2026, 8, 31, 22, 0),
                                     trigger="PYTEST")
    assert len(calls) == 1
    assert calls[0]["emit_batch"] is True
    by = {s["stage"]: s for s in body["stages"]}
    assert by["tournament_advance"]["emit_batch_requested"] is True
    assert by["tournament_advance"]["emission_mode"] == TC.EMIT_OK_FRESH


def test_runtime_verdict_is_pinned_not_wall_clock(sandbox, monkeypatch):
    """A hermetic scenario's verdict may depend ONLY on its pinned instant
    and pinned owned-data probe - never on when pytest happens to run.

    The machine wall clock is moved to an absurd future instant; the pinned
    Monday-morning suppression verdict must not move with it. (This is the
    defect class that broke the runtime-cycle test on 2026-08-31: the live
    probe made the verdict a function of the operator's pytest start time
    relative to the nightly data refresh.)
    """
    calls = []
    _stub_advance(monkeypatch, calls)
    _stub_frontier_inputs(monkeypatch)
    monkeypatch.setattr(AF, "predictions", lambda cid=None: [])
    monkeypatch.setattr(LG, "predictions", lambda cid=None: [])
    monkeypatch.setattr(RG, "load", lambda cid=None: {"challengers": []})
    _pin_owned_session(monkeypatch, dt.date(2026, 8, 28))
    monkeypatch.setattr(CK, "now_utc", lambda: _utc(2031, 1, 1, 12, 0))
    body = RT.research_runtime_cycle(_utc(2026, 8, 31, 13, 0),
                                     trigger="PYTEST")
    assert calls[0]["emit_batch"] is False
    by = {s["stage"]: s for s in body["stages"]}
    assert (by["tournament_advance"]["emission_mode"]
            == TC.EMIT_SUPPRESSED_DATA_PENDING)
    # and the pure policy function itself is clock-free given its inputs
    p = TC.evaluate_emission_policy(_utc(2026, 8, 31, 13, 0),
                                    last_session=dt.date(2026, 8, 28))
    assert p["emit"] is False
    assert p["mode"] == TC.EMIT_SUPPRESSED_DATA_PENDING


def test_second_concurrent_runtime_instance_is_refused(sandbox, monkeypatch):
    calls = []
    _stub_advance(monkeypatch, calls)
    _stub_frontier_inputs(monkeypatch)
    lock = R52.runtime_dir() / RT.RUNTIME_LOCK_NAME
    RL.acquire_path(lock, "someone_else", wait_s=0)
    try:
        body = RT.research_runtime_cycle(_utc(2026, 8, 31, 13, 0),
                                         trigger="PYTEST")
        assert body["state"] == RT.RUN_REFUSED_CONCURRENT
        assert calls == []
    finally:
        # someone_else's lock survives the refused run
        assert lock.exists()
        lock.unlink()


def test_a_broken_chain_fails_the_run_closed_before_any_write(
        sandbox, monkeypatch):
    from alpha_agent.r46 import advance as AD

    def must_not_run(*a, **k):
        raise AssertionError("advance ran after a failed integrity check")
    monkeypatch.setattr(AD, "advance", must_not_run)
    _pin_owned_session(monkeypatch, dt.date(2026, 8, 28))
    monkeypatch.setattr(RT, "_chains_ok",
                        lambda: {"all_intact": False, "chains": {}})
    body = RT.research_runtime_cycle(_utc(2026, 8, 31, 13, 0),
                                     trigger="PYTEST")
    assert body["state"] == RT.RUN_FAILED_INTEGRITY
    assert body["fail_closed"] is True
    assert body["nothing_was_written"] is True


def test_one_failing_stage_does_not_stop_the_others(sandbox, monkeypatch):
    calls = []
    _stub_advance(monkeypatch, calls)
    _stub_frontier_inputs(monkeypatch)

    def boom(*a, **k):
        raise RuntimeError("lane source down")
    monkeypatch.setattr(FF, "sweep", boom)
    monkeypatch.setattr(AF, "predictions", lambda cid=None: [])
    monkeypatch.setattr(LG, "predictions", lambda cid=None: [])
    monkeypatch.setattr(RG, "load", lambda cid=None: {"challengers": []})
    _pin_owned_session(monkeypatch, dt.date(2026, 8, 28))
    body = RT.research_runtime_cycle(_utc(2026, 8, 31, 13, 0),
                                     trigger="PYTEST")
    assert body["state"] == RT.RUN_COMPLETED_WITH_FAILURES
    by = {s["stage"]: s for s in body["stages"]}
    assert by["forfeiture_sweep"]["state"] == RT.FAILED_RETRYABLE
    assert by["tournament_advance"]["state"] == RT.SUCCESS
    assert by["promotion_frontier"]["state"] == RT.SUCCESS


def test_runtime_never_backdates_and_never_forces_emission(sandbox,
                                                           monkeypatch):
    calls = []
    _stub_advance(monkeypatch, calls)
    _stub_frontier_inputs(monkeypatch)
    monkeypatch.setattr(AF, "predictions", lambda cid=None: [])
    monkeypatch.setattr(LG, "predictions", lambda cid=None: [])
    monkeypatch.setattr(RG, "load", lambda cid=None: {"challengers": []})
    # pinned FRESH: the policy would grant the slot, the override still wins
    _pin_owned_session(monkeypatch, dt.date(2026, 8, 31))
    RT.research_runtime_cycle(_utc(2026, 8, 31, 22, 0), trigger="PYTEST",
                              emit_override="NEVER")
    assert calls[0]["emit_batch"] is False


# =========================================================================== #
# PART D - emission through the canonical door (scenarios A, B, N, O)
# =========================================================================== #
def test_r52_challengers_register_emit_and_deduplicate(sandbox):
    reg = RG.register(TEST_CAMPAIGN, specs=list(CH.R52_SPECS))
    assert reg["retune_free"] is True
    ids = {c["challenger_id"] for c in reg["challengers"]}
    assert ids == {"r52_eqidx_xs_rel_mom_12_1", "r52_rates_copper_gold_lead"}
    from alpha_agent.r46 import emit as EM
    first = EM.emit(TEST_CAMPAIGN, reg, _utc(2026, 8, 31, 22, 0),
                    specs=list(CH.R52_SPECS))
    assert first["state"] == "EMITTED"
    assert first["n_appended"] >= 1
    assert first["entry_session_date"] == "2026-09-01"
    again = EM.emit(TEST_CAMPAIGN, reg, _utc(2026, 8, 31, 22, 30),
                    specs=list(CH.R52_SPECS))
    assert again["n_appended"] == 0
    assert again["n_duplicates_skipped"] == first["n_appended"]
    assert LG.verify(TEST_CAMPAIGN)["all_intact"] is True


def test_a_retuned_r52_spec_is_detected_never_absorbed(sandbox):
    RG.register(TEST_CAMPAIGN, specs=list(CH.R52_SPECS))
    tampered = [dict(s) for s in CH.R52_SPECS]
    tampered[0] = dict(tampered[0],
                       parameters=dict(tampered[0]["parameters"],
                                       formation_days=126))
    reg2 = RG.register(TEST_CAMPAIGN, specs=tampered)
    assert reg2["retune_free"] is False
    assert any(r["challenger_id"] == "r52_eqidx_xs_rel_mom_12_1"
               for r in reg2["retunes_detected"])


def test_every_frozen_spec_in_the_field_is_internally_consistent():
    ids = [s["challenger_id"] for s in CH.ALL_SPECS]
    assert len(ids) == len(set(ids)), "duplicate challenger_id"
    for s in CH.ALL_SPECS:
        assert s["parameters_were_searched"] is False
        assert s["promotion_allowed"] is False
        assert CH.spec_hash(s)


def test_r52_specs_declare_independence_and_overlap():
    for s in CH.R52_SPECS:
        assert s["cohort"] == CH.R52_COHORT
        assert s["dependence_cluster"]
        assert s["information_family"]
        assert s["economic_overlap_with"]
        assert s["overlap_note"]
    clusters = {s["dependence_cluster"] for s in CH.R52_SPECS}
    assert clusters == {"EQIDX_XS_PRICE", "RATES_CROSS_ASSET"}


def test_declined_hypotheses_are_recorded_decisions_not_silences():
    assert len(CH.R52_DECLINED) >= 5
    for reason in CH.R52_DECLINED.values():
        assert len(reason) > 60


def test_outcome_ledger_scores_a_prediction_exactly_once(sandbox):
    row = {"prediction_id": "p1", "challenger_id": "c", "horizon": 5,
           "scored_at_utc": "2026-09-08T00:00:00Z",
           "realised_net_return": 0.01,
           "forward_evidence_type": C.TRUE_FORWARD}
    first = LG.append_outcomes([row], TEST_CAMPAIGN)
    assert first["n_appended"] == 1
    again = LG.append_outcomes([dict(row)], TEST_CAMPAIGN)
    assert again["n_appended"] == 0
    assert again["n_duplicates_skipped"] == 1


# =========================================================================== #
# PART E - the frontier refresh (scenario Q)
# =========================================================================== #
def _frontier_fixture_files(campaign_dir_path):
    lb = {"rows": [{
        "challenger_id": "r52_eqidx_xs_rel_mom_12_1",
        "asset_class": "EQUITY_INDEX", "state": "FORWARD_CONFIRMED",
        "effective_independent": 30.0, "horizon": 20,
        "forward_predictions_emitted": 100,
        "forward_predictions_matured": 80}]}
    (campaign_dir_path / FR.LEADERBOARD_ARTIFACT).write_text(
        json.dumps(lb), encoding="utf-8")
    (campaign_dir_path / FR.VELOCITY_ARTIFACT).write_text(
        json.dumps({}), encoding="utf-8")
    (campaign_dir_path / FR.VERDICTS_ARTIFACT).write_text(
        json.dumps({"rows": []}), encoding="utf-8")
    (campaign_dir_path / FR.CONTINUATION_ARTIFACT).write_text(
        json.dumps({"lane_results": {}}), encoding="utf-8")


def test_promotion_ready_is_surfaced_and_never_approved(sandbox):
    _frontier_fixture_files(R46.campaign_dir())
    sleeve = {"sleeve_id": "sleeve_equity_index_futures",
              "declared_capabilities": {"LIQUIDITY_SUPPORTED": True},
              "r50_activation_attempt": {}}
    body = FR.refresh(_utc(2026, 8, 31, 13, 0), sleeves=[sleeve],
                      unit_economics={"sleeve_equity_index_futures": {
                          "smallest_unit_notional_usd": 5000.0,
                          "smallest_unit_symbol": "&MES",
                          "margin_usd": 500.0}},
                      nav_usd=100000.0)
    assert body["promotion_ready_count"] == 1
    assert body["promotion_ready"] == ["sleeve_equity_index_futures"]
    assert body["automatic_promotion_performed"] is False
    assert body["frontier"]["manual_approval_required"] is True
    # the transition is recorded exactly once
    assert len(body["packet_state_transitions"]) >= 1
    again = FR.refresh(_utc(2026, 8, 31, 14, 0), sleeves=[sleeve],
                       unit_economics={"sleeve_equity_index_futures": {
                           "smallest_unit_notional_usd": 5000.0,
                           "smallest_unit_symbol": "&MES",
                           "margin_usd": 500.0}},
                       nav_usd=100000.0)
    assert again["packet_state_transitions"] == []


def test_r52_package_has_no_approval_writer():
    for name in ("__init__", "runtime", "forfeiture", "frontier_refresh",
                 "timing_contract", "velocity_ops"):
        src = (REPO / "alpha_agent" / "r52" / (name + ".py")).read_text(
            encoding="utf-8")
        assert "model_approval_state" not in src.replace(
            "``model_approval_state``", "")
        assert "capital_eligible" not in src.replace(
            "``capital_eligible``", "")
        assert "record_decision" not in src
        assert "approvals=" not in src


def test_r52_package_never_reaches_the_operational_path():
    forbidden = (
        r"from paper_trader\.api import daily_close",
        r"from paper_trader\.api import rebalance_execution",
        r"from paper_trader\.api import portfolio_decision\b",
        r"from paper_trader\.engine import normal_cycle",
        r"portfolio-cycle/run",
        r"\brequests\.",
        r"127\.0\.0\.1:8001",
    )
    for p in (REPO / "alpha_agent" / "r52").glob("*.py"):
        src = p.read_text(encoding="utf-8")
        for pat in forbidden:
            assert not re.search(pat, src), "%s in %s" % (pat, p.name)
    runner = (REPO / "scripts" / "run_research_runtime.py").read_text(
        encoding="utf-8")
    for pat in forbidden:
        assert not re.search(pat, runner), "%s in runner" % pat


# =========================================================================== #
# PART F - operational velocity (the two bottlenecks)
# =========================================================================== #
def test_velocity_split_names_runtime_loss_apart_from_calendar(
        sandbox, monkeypatch):
    monkeypatch.setattr(LG, "predictions", lambda cid=None: [
        {"effective_as_of": "2026-09-01"}, {"effective_as_of": "2026-09-01"}])
    FF.append([{"lane_id": "r46_daily_batch",
                "challenger_scope": FF.SCOPE_BATCH,
                "decision_date": "2026-09-02",
                "reason": FF.REASON_RUNTIME_NOT_INVOKED,
                "observed_invocation_utc": "2026-09-03T12:00:00Z",
                "legal_emission_start": "any",
                "legal_emission_cutoff_utc": "2026-09-02T04:00:00Z",
                "upstream_data_state": "UNKNOWN_AT_SWEEP_TIME",
                "scheduler_state": "TEST", "outcome_window_already_open": True,
                "backfill_refused": True, "n_cells_lost": 36,
                "evidence": {}, "source": "test",
                "calculation_owner": FF.CALCULATION_OWNER}])
    body = VO.build(_utc(2026, 9, 3, 12, 0))
    assert body["forfeited_cells_total"] == 36
    assert body["evidence_loss_due_to_runtime"]["n_cells"] == 36
    week = next(w for w in body["weekly"] if w["week"] == "2026-W36")
    assert week["predictions_emitted"] == 2
    assert week["batch_slots_used"] == 1
    assert week["cells_lost"] == 36
    assert "SCIENTIFICALLY_SLOW" in body["the_two_bottlenecks"]
    assert "OPERATIONALLY_MISSED" in body["the_two_bottlenecks"]


# =========================================================================== #
# PART G - the scheduler deliverables (scenarios L, M) and the entrypoint
# =========================================================================== #
def _script(name: str) -> str:
    return (REPO / "scripts" / name).read_text(encoding="utf-8")


def test_install_script_is_explicit_idempotent_and_exit_free():
    src = _script("install_research_runtime_task.ps1")
    assert "PaperTrader-ResearchRuntime" in src
    assert "R52_TASK_UNCHANGED" in src
    assert "R52_TASK_INSTALL_BLOCKED" in src
    assert "-Force" in src and "explicit migration" in src
    assert "StartWhenAvailable" in src
    assert "IgnoreNew" in src
    assert "run_research_runtime.py" in src
    for t in ("08:15", "17:45", "19:45", "21:45"):
        assert t in src
    assert re.search(r"^\s*exit\b", src, re.MULTILINE) is None


def test_validator_reports_an_absent_task_as_scheduler_incomplete():
    src = _script("validate_research_runtime_task.ps1")
    assert "R52_SCHEDULER_INCOMPLETE" in src
    assert "R52_TASK_INVALID" in src
    assert "R52_TASK_VALID" in src
    assert "missing trigger times" in src
    assert "StartWhenAvailable" in src
    assert re.search(r"^\s*exit\b", src, re.MULTILINE) is None


def test_disable_script_disables_only_and_deletes_nothing():
    src = _script("disable_research_runtime_task.ps1")
    assert "Disable-ScheduledTask" in src
    assert "Unregister-ScheduledTask" not in src
    assert "Remove-Item" not in src
    assert "PaperTrader-ResearchRuntime" in src
    assert re.search(r"^\s*exit\b", src, re.MULTILINE) is None


def test_one_shot_script_calls_the_same_entrypoint_as_the_task():
    src = _script("run_research_runtime_once.ps1")
    assert "run_research_runtime.py" in src
    assert "R52RuntimeOnceResult" in src
    assert re.search(r"^\s*exit\b", src, re.MULTILINE) is None


def test_python_entrypoint_owns_no_timing_and_prints_one_token():
    src = (REPO / "scripts" / "run_research_runtime.py").read_text(
        encoding="utf-8")
    assert "research_runtime_cycle" in src
    for token in ("RESEARCH_RUNTIME_OK", "RESEARCH_RUNTIME_REFUSED",
                  "RESEARCH_RUNTIME_INTEGRITY_FAILED",
                  "RESEARCH_RUNTIME_FAILED"):
        assert token in src
    # no timing rule of its own: no wall-clock comparisons, no calendars
    assert "17:45" not in src and "08:15" not in src
    assert "while True" not in src


# =========================================================================== #
# PART H - the API read model
# =========================================================================== #
def test_runtime_health_route_is_read_only_and_reports_never_ran(
        sandbox):
    from api import research_runtime as ARR
    body = ARR.load_runtime_health()
    assert body["state"] == "RUNTIME_NEVER_RAN"
    assert body["research_only"] is True


def test_runtime_health_route_reports_a_completed_run(sandbox, monkeypatch):
    calls = []
    _stub_advance(monkeypatch, calls)
    _stub_frontier_inputs(monkeypatch)
    monkeypatch.setattr(AF, "predictions", lambda cid=None: [])
    monkeypatch.setattr(LG, "predictions", lambda cid=None: [])
    monkeypatch.setattr(RG, "load", lambda cid=None: {"challengers": []})
    RT.research_runtime_cycle(_utc(2026, 8, 31, 13, 0), trigger="PYTEST")
    from api import research_runtime as ARR
    body = ARR.load_runtime_health()
    assert body["state"] in (RT.RUN_COMPLETED, RT.RUN_COMPLETED_WITH_FAILURES)
    assert body["safety"]["calls_portfolio_cycle"] is False
    assert body["safety"]["backfills_forward_rows"] is False
    assert body["runtime_health"]["forward_chain_integrity"] is True


# =========================================================================== #
# PART I - installer principal idempotency (the R52 correction)
# =========================================================================== #
# The original installer decided "identical" from action + trigger times
# alone, so an existing Interactive task was treated as identical to a
# requested S4U task and R52_TASK_UNCHANGED fired before -Force could
# migrate the principal. Task equivalence must include the principal.
# These tests drive the REAL PowerShell decision logic through the
# installer's hermetic -DecisionProbe mode and the validator's
# -PrincipalProbe mode: no scheduler, no task, no process is touched.

_PS = ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File"]
_INSTALLER = REPO / "scripts" / "install_research_runtime_task.ps1"
_VALIDATOR = REPO / "scripts" / "validate_research_runtime_task.ps1"


def _task_snapshot(logon, *, times=("08:15", "17:45", "19:45", "21:45"),
                   execute=None, multiple_instances="IgnoreNew"):
    """An existing-task snapshot matching the installer's desired definition
    in every field except those a test overrides."""
    execute = execute or r"C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe"
    runtime = r"C:\Users\binis\paper_trader\scripts\run_research_runtime.py"
    return {
        "TaskName": "PaperTrader-ResearchRuntime",
        "State": "Ready",
        "Enabled": True,
        "Action": {"Execute": execute,
                   "Arguments": f'"{runtime}" --trigger SCHEDULED',
                   "WorkingDirectory": r"C:\Users\binis\paper_trader"},
        "Triggers": [{"Type": "MSFT_TaskDailyTrigger",
                      "StartBoundary": f"2026-08-31T{t}:00",
                      "Enabled": True} for t in times],
        "Principal": {"UserId": os.environ.get("USERNAME", "binis"),
                      "LogonType": logon, "RunLevel": "Limited"},
        "Settings": {"StartWhenAvailable": True,
                     "MultipleInstances": multiple_instances,
                     "ExecutionTimeLimit": "PT2H", "RestartCount": 2,
                     "RestartInterval": "PT10M", "WakeToRun": True,
                     "DisallowStartIfOnBatteries": False},
    }


def _probe_decision(tmp_path, snapshot, *extra_args):
    if snapshot is None:
        target = "ABSENT"
    else:
        probe = tmp_path / "r52_probe_snapshot.json"
        probe.write_text(json.dumps(snapshot), encoding="utf-8")
        target = str(probe)
    out = subprocess.run(
        _PS + [str(_INSTALLER), "-DecisionProbe", target, *extra_args],
        capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def _mismatches(decision):
    mm = decision.get("mismatches")
    if mm is None:
        return []
    return [mm] if isinstance(mm, str) else list(mm)


def test_existing_interactive_requested_s4u_without_force_is_blocked(tmp_path):
    d = _probe_decision(tmp_path, _task_snapshot("Interactive"),
                        "-PreferredLogonType", "S4U")
    assert d["decision"] == "BLOCKED_PRINCIPAL"
    assert any("Principal.LogonType=Interactive, requested=S4U" in m
               for m in _mismatches(d))


def test_existing_interactive_requested_s4u_with_force_migrates(tmp_path):
    d = _probe_decision(tmp_path, _task_snapshot("Interactive"),
                        "-PreferredLogonType", "S4U", "-Force")
    assert d["decision"] == "MIGRATE"
    assert any("Principal.LogonType" in m for m in _mismatches(d))


def test_existing_s4u_requested_s4u_is_unchanged(tmp_path):
    d = _probe_decision(tmp_path, _task_snapshot("S4U"),
                        "-PreferredLogonType", "S4U")
    assert d["decision"] == "UNCHANGED"
    assert not _mismatches(d)


def test_absent_task_decides_fresh_install(tmp_path):
    d = _probe_decision(tmp_path, None, "-PreferredLogonType", "S4U")
    assert d["decision"] == "INSTALL"


def test_action_trigger_and_settings_mismatches_are_still_detected(tmp_path):
    d = _probe_decision(
        tmp_path, _task_snapshot("S4U", times=("08:15", "17:45", "19:45")))
    assert d["decision"] == "BLOCKED_DEFINITION"
    assert any("trigger times" in m for m in _mismatches(d))

    d = _probe_decision(
        tmp_path, _task_snapshot("S4U", execute=r"C:\Windows\py.exe"))
    assert d["decision"] == "BLOCKED_DEFINITION"
    assert any("Action.Execute" in m for m in _mismatches(d))

    d = _probe_decision(
        tmp_path, _task_snapshot("S4U", multiple_instances="Parallel"))
    assert d["decision"] == "BLOCKED_DEFINITION"
    assert any("MultipleInstances" in m for m in _mismatches(d))


def test_validator_principal_probe_rejects_interactive():
    out = subprocess.run(
        _PS + [str(_VALIDATOR), "-PrincipalProbe", "Interactive"],
        capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    assert "R52_PRINCIPAL_REJECTED" in out.stdout
    assert "R52_PRINCIPAL_ACCEPTED" not in out.stdout


def test_validator_principal_probe_accepts_s4u():
    out = subprocess.run(
        _PS + [str(_VALIDATOR), "-PrincipalProbe", "S4U"],
        capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    assert "R52_PRINCIPAL_ACCEPTED" in out.stdout
    assert "R52_PRINCIPAL_REJECTED" not in out.stdout


def test_scripts_carry_the_principal_contract_and_stay_exit_free():
    inst = _script("install_research_runtime_task.ps1")
    assert "Principal.LogonType" in inst
    assert "explicit -Force migration required" in inst
    assert "R52_TASK_MIGRATED" in inst
    # migration never silently falls back to Interactive
    assert "requested logon type ONLY" in inst
    assert re.search(r"^\s*exit\b", inst, re.MULTILINE) is None
    val = _script("validate_research_runtime_task.ps1")
    assert "logged-out-capable" in val
    assert "R52_PRINCIPAL_REJECTED" in val and "R52_PRINCIPAL_ACCEPTED" in val
    assert re.search(r"^\s*exit\b", val, re.MULTILINE) is None
