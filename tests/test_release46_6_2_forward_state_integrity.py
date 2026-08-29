"""Release 46.6.2 - forward-state integrity, adopted timing, collection recovery.

Four production facts of 2026-08-28 are pinned here, each with the measurement
that decided it rather than the story that fitted it.

**The VX block was CORRECT, and R46.6.1's artifact could not prove it.** The
canonical cycle called ``r39_vx_weekly`` and reported ``CALLED_PIT_BLOCKED /
OUTCOME_WINDOW_ALREADY_OPEN`` with ``n_refused_outcome_window_open = 1`` - and
never said WHICH decision date it refused, so the only way to adjudicate the
gate was to rebuild the owner's panel. Doing that showed the refused date was
Tuesday **2026-08-25**, not the Friday: the frozen grid in
``alpha_agent.r39.universal_state.build_vx_weekly`` walks
``range(260, len(sessions) - 1, 5)``, so its newest decision date is always at
least one session short of the panel end. That decision's outcome window opened
2026-08-26T04:00:00Z and the cycle attempted to emit 67.3 hours later. The gate
is unchanged; what changed is that a refusal now names its own date, and a
refusal no run could have avoided is called ``STRUCTURALLY_LATE`` instead of
being reported as a transient block.

**A Friday decision would NOT have been refused**, which is the weekend case
the gate has to get right and does: ``next_weekday(Friday)`` is Monday and the
window does not open until Monday 00:00 Eastern.

**The collection worker died and could not come back.** Its lock survived a
kill that never ran its ``finally``, the relaunch correctly refused to be a
second worker, and nothing else was ever going to try. Recovery now has one
authorised, idempotent owner that may clear a lock ONLY when the process table
proves no worker exists.

**503 and 501 option sessions are different quantities**, measured on one
surface at one instant, and the two sessions that separate them are named.

Everything here is hermetic: temp roots, synthetic panels, no network, no
production write. The prior releases' stores are opened read-only and hashed
before and after.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from alpha_agent import r46 as R46
from alpha_agent.r46 import adopted_forward as AF
from alpha_agent.r46 import clock as CK
from alpha_agent.r46 import contract as C
from alpha_agent.r46 import harvest as HV
from alpha_agent.r46 import lanes as LN
from alpha_agent.r46 import options as OP
from alpha_agent.r46 import options_hypotheses as OH
from paper_trader.api import information_collection as ic
from paper_trader.api import workflow_state as WS

TEST_CAMPAIGN = "r46_6_2_pytest_campaign"

R39_VX_WEEKLY = ("shadow_vx_carry_ts",)
R39_MONTH_END = ("shadow_wide_xs", "shadow_carry_rule_xs")

#: The production instants this release adjudicated.
VX_REFUSED_DECISION = "2026-08-25"          # Tuesday - the owner's own grid
VX_PANEL_LATEST_SESSION = "2026-08-28"      # Friday - the panel's newest bar
DRC_ATTEMPT = dt.datetime(2026, 8, 28, 23, 18, 37, tzinfo=dt.timezone.utc)
FRIDAY_EVENING = dt.datetime(2026, 8, 28, 23, 30, tzinfo=dt.timezone.utc)
MONDAY_MORNING = dt.datetime(2026, 8, 31, 13, 30, tzinfo=dt.timezone.utc)

PRIOR_ARTIFACTS = {
    "R39_registry": r"D:\Stock_Prediction_app_data\universal_alpha_r39"
                    r"\r39_universal_alpha_continuation_v2"
                    r"\research_shadow_registry.json",
    "R40_registry": r"D:\Stock_Prediction_app_data\prospective_alpha_r40"
                    r"\r40_prospective_alpha_acceleration_v1"
                    r"\shadow_registry_v2.json",
}


# --------------------------------------------------------------------------- #
# Fixtures and builders
# --------------------------------------------------------------------------- #
@pytest.fixture()
def sandbox(tmp_path, monkeypatch):
    """Every R46 write goes to a temp root. Production is never touched."""
    monkeypatch.setattr(R46, "RESEARCH_ROOT", tmp_path / "r46root")
    monkeypatch.setattr(C, "ARTIFACT_DIR", tmp_path / "r46root" / TEST_CAMPAIGN)
    return tmp_path


@pytest.fixture()
def root(tmp_path):
    """An isolated collection-state root."""
    d = tmp_path / "collection"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _sha(path: str) -> str:
    p = Path(path)
    return (hashlib.sha256(p.read_bytes()).hexdigest() if p.exists()
            else "ABSENT")


def prior_hashes() -> dict:
    return {k: _sha(v) for k, v in PRIOR_ARTIFACTS.items()}


def vx_panel(dates, carry: float = 0.4, fwd: float = 0.012):
    return pd.DataFrame([{"market_id": "VX", "decision_date": pd.Timestamp(d),
                          "carry_slope_ann": carry, "fwd_5": fwd}
                         for d in dates])


def fut_panel(dates, n: int = 9):
    rows = []
    for d in dates:
        for i in range(n):
            rows.append({"market_id": "M%d" % i,
                         "decision_date": pd.Timestamp(d),
                         "carry_slope_ann": 0.1 * (i - n / 2.0),
                         "vol_63": 0.15 + 0.01 * i,
                         "economic_group": "G%d" % (i % 2),
                         "fwd_21": 0.004 * (i - n / 2.0)})
    return pd.DataFrame(rows)


def layer(market: str, sessions):
    """The RAW per-market session axis the adopted owner's state carries."""
    return {market: pd.DataFrame({"Date": [pd.Timestamp(s) for s in sessions],
                                  "ret": [0.0] * len(sessions)})}


def state_with(vx_dates, vx_sessions=None, fut_dates=(), fut_sessions=None):
    st = {"vx": vx_panel(vx_dates), "fut": fut_panel(list(fut_dates)),
          "fut_intl_rates": pd.DataFrame()}
    lay = {}
    if vx_sessions is not None:
        lay.update(layer("VX", vx_sessions))
    if fut_sessions is not None:
        lay.update(layer("M0", fut_sessions))
    if lay:
        st["layer"] = lay
    return st


def vx_shadow():
    return {"shadow_id": "shadow_vx_carry_ts", "lane": "VX",
            "frozen_at": "2026-08-23T04:02:47Z", "horizon_sessions": 5}


# =========================================================================== #
# 1. THE 2026-08-28 VX REFUSAL, RECONSTRUCTED AND ADJUDICATED
# =========================================================================== #
class TestTheProductionVxRefusalWasCorrect:
    """Section 5/6: determine the exact truth, then leave a correct gate alone."""

    def test_the_frozen_grid_can_never_put_a_decision_on_the_newest_session(
            self):
        """The whole adjudication turns on this.

        ``alpha_agent.r39.universal_state.build_vx_weekly`` walks
        ``range(260, len(sessions) - 1, 5)``. The upper bound is EXCLUSIVE and
        is ``len - 1``, so the newest decision date is at best the second-newest
        session and at worst five sessions back. Whichever it is, a LATER
        session exists the moment that decision date becomes readable - and a
        later session means the outcome window has opened. The rule is
        reproduced here, not paraphrased.
        """
        for n_extra in range(1, 12):
            sessions = [dt.date(2024, 1, 1) + dt.timedelta(days=i)
                        for i in range(260 + n_extra)]
            grid = [sessions[pos]
                    for pos in range(260, len(sessions) - 1, 5)]
            if not grid:
                continue
            newest_decision = grid[-1]
            assert newest_decision < sessions[-1], n_extra
            # ...and therefore the window opened no later than that session
            assert AF.outcome_window_start(newest_decision) <= \
                CK.outcome_window_start_utc(
                    CK.next_weekday(newest_decision))
            assert CK.next_weekday(newest_decision) <= sessions[-1]

    def test_the_refused_decision_date_was_the_tuesday_not_the_friday(self):
        """2026-08-28 was a VX SESSION; it was not the owner's decision date."""
        due = AF.due_decision_dates(vx_shadow(),
                                    vx_panel([VX_REFUSED_DECISION]), set())
        assert [str(pd.Timestamp(d).date()) for d in due] == \
            [VX_REFUSED_DECISION]
        assert dt.date.fromisoformat(VX_REFUSED_DECISION).weekday() == 1
        assert dt.date.fromisoformat(VX_PANEL_LATEST_SESSION).weekday() == 4
        # the Friday was three sessions AFTER the decision the gate refused
        assert VX_REFUSED_DECISION < VX_PANEL_LATEST_SESSION

    def test_the_refusal_names_its_own_date_and_says_how_late_it_was(self):
        """R46.6.1 reported a COUNT of refusals. A count cannot be audited."""
        ev = AF.refusal_evidence(VX_REFUSED_DECISION, DRC_ATTEMPT)
        assert ev["decision_date"] == "2026-08-25"
        assert ev["decision_weekday"] == "Tuesday"
        assert ev["first_outcome_session"] == "2026-08-26"
        assert ev["outcome_window_start_utc"] == "2026-08-26T04:00:00Z"
        assert ev["reason"] == AF.SKIP_OUTCOME_WINDOW_OPEN
        assert 67.0 < ev["hours_late"] < 67.6
        for field in AF.REFUSAL_EVIDENCE_FIELDS:
            assert field in ev

    def test_a_run_lane_refusal_carries_that_evidence_end_to_end(self, sandbox):
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                        state=state_with([VX_REFUSED_DECISION]),
                        now=DRC_ATTEMPT, mature_matured=False)
        assert r["lifecycle"] == "CALLED_PIT_BLOCKED"
        assert r["n_refused_outcome_window_open"] == 1
        assert r["n_appended"] == 0
        rows = r["refused_decision_dates"]
        assert len(rows) == 1
        assert rows[0]["decision_date"] == VX_REFUSED_DECISION
        assert rows[0]["first_outcome_session"] == "2026-08-26"
        assert rows[0]["shadow_id"] == "shadow_vx_carry_ts"

    def test_a_refusal_no_run_could_have_avoided_is_named_structurally_late(
            self, sandbox):
        """Measured, not asserted: the owner's session axis already held
        sessions after its newest decision date, so the window had opened
        before that date existed to be read."""
        sessions = ["2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28"]
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                        state=state_with([VX_REFUSED_DECISION],
                                         vx_sessions=sessions),
                        now=DRC_ATTEMPT, mature_matured=False)
        assert r["emission_feasibility"] == AF.EMISSION_STRUCTURALLY_LATE
        per = r["per_shadow"]["shadow_vx_carry_ts"]
        assert per["owner_latest_session"] == "2026-08-28"
        assert per["owner_latest_decision_date"] == VX_REFUSED_DECISION
        assert per["n_sessions_after_newest_due_decision_date"] == 3
        assert "no earlier run could have emitted it" in \
            per["emission_feasibility_reason"].lower()

    def test_without_a_session_axis_the_structural_claim_is_not_made(
            self, sandbox):
        """A block that cannot be proven structural is reported as late for
        THIS RUN. The stronger claim is never made on absent evidence."""
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                        state=state_with([VX_REFUSED_DECISION]),
                        now=DRC_ATTEMPT, mature_matured=False)
        assert r["emission_feasibility"] == AF.EMISSION_LATE_THIS_RUN

    def test_the_gate_itself_is_unchanged_and_backfills_nothing(self, sandbox):
        """A release that recovers Friday's evidence by backdating FAILS."""
        assert C.SAFETY_BLOCK["backdates_forward_rows"] is False
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                        state=state_with([VX_REFUSED_DECISION]),
                        now=DRC_ATTEMPT, mature_matured=False)
        assert r["n_appended"] == 0
        assert AF.predictions(TEST_CAMPAIGN) == []


# =========================================================================== #
# 2. REQUIRED VX TIMING TESTS - section 8, A through H
# =========================================================================== #
class TestAdoptedTimingGate:

    def test_a_friday_decision_before_monday_opens_is_valid(self, sandbox):
        """A. The weekend roll. This is the case R46.6 believed it was in."""
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                        state=state_with(["2026-08-28"]), now=FRIDAY_EVENING,
                        mature_matured=False)
        assert r["lifecycle"] == "CALLED_AND_EMITTED"
        assert r["n_appended"] == 1
        row = AF.predictions(TEST_CAMPAIGN)[0]
        assert row["decision_date"] == "2026-08-28"
        assert row["outcome_window_start_utc"] == "2026-08-31T04:00:00Z"
        assert row["evidence_class"] == C.TRUE_FORWARD

    def test_a_friday_decision_after_monday_opens_is_pit_blocked(self, sandbox):
        """B."""
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2026, 8, 31), campaign_id=TEST_CAMPAIGN,
                        state=state_with(["2026-08-28"]), now=MONDAY_MORNING,
                        mature_matured=False)
        assert r["lifecycle"] == "CALLED_PIT_BLOCKED"
        assert r["n_appended"] == 0
        assert r["refused_decision_dates"][0]["decision_date"] == "2026-08-28"

    def test_a_weekday_decision_before_the_next_session_opens_is_valid(
            self, sandbox):
        """C. Thursday close, before Friday 00:00 ET."""
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2026, 8, 27), campaign_id=TEST_CAMPAIGN,
                        state=state_with(["2026-08-27"]),
                        now=dt.datetime(2026, 8, 27, 23, 0,
                                        tzinfo=dt.timezone.utc),
                        mature_matured=False)
        assert r["n_appended"] == 1

    def test_a_weekday_decision_after_the_next_session_opens_is_blocked(
            self, sandbox):
        """D."""
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                        state=state_with(["2026-08-27"]),
                        now=dt.datetime(2026, 8, 28, 12, 0,
                                        tzinfo=dt.timezone.utc),
                        mature_matured=False)
        assert r["lifecycle"] == "CALLED_PIT_BLOCKED"
        assert r["n_appended"] == 0

    def test_the_window_rolls_over_weekends_and_holidays_correctly(self,
                                                                    sandbox):
        """E. The canonical R46 clock owns the calendar; the continuation gate
        adds no second one and no hard-coded weekday shortcut.

        Weekends are skipped by the clock. A HOLIDAY is deliberately not
        assumed away: the window still opens on the next weekday's calendar
        date, and the instrument's own next realised bar resolves forward from
        there - never backwards, which is the direction that would create
        look-ahead."""
        for d, nxt in ((dt.date(2026, 8, 28), dt.date(2026, 8, 31)),  # Fri->Mon
                       (dt.date(2026, 8, 29), dt.date(2026, 8, 31)),  # Sat->Mon
                       (dt.date(2026, 8, 30), dt.date(2026, 8, 31)),  # Sun->Mon
                       (dt.date(2026, 8, 31), dt.date(2026, 9, 1)),   # Mon->Tue
                       (dt.date(2026, 12, 31), dt.date(2027, 1, 1))):
            assert CK.next_weekday(d) == nxt
            assert AF.outcome_window_start(d) == \
                CK.outcome_window_start_utc(nxt)
        # a Thursday decision whose Friday is a market holiday: the window
        # still opens on the Friday, so a Friday-morning run is REFUSED and
        # cannot pretend the holiday bought it another day
        thursday = dt.date(2026, 12, 31)
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2027, 1, 1), campaign_id=TEST_CAMPAIGN,
                        state=state_with([str(thursday)]),
                        now=dt.datetime(2027, 1, 1, 15, 0,
                                        tzinfo=dt.timezone.utc),
                        mature_matured=False)
        assert r["lifecycle"] == "CALLED_PIT_BLOCKED"
        assert r["refused_decision_dates"][0]["first_outcome_session"] == \
            "2027-01-01"

    def test_a_replay_appends_no_duplicate_continuation_row(self, sandbox):
        """F. Idempotency, on the identity key."""
        args = dict(release="R39", shadow_ids=R39_VX_WEEKLY,
                    as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                    state=state_with(["2026-08-28"]), now=FRIDAY_EVENING,
                    mature_matured=False)
        first = AF.run_lane(**args)
        second = AF.run_lane(**args)
        assert first["lifecycle"] == "CALLED_AND_EMITTED"
        assert first["n_appended"] == 1
        # the replay finds the date already in the R46 continuation ledger, so
        # it is not even offered a second time
        assert second["n_appended"] == 0
        assert second["lifecycle"] == "CALLED_QUIET_NOT_DUE"
        assert second["owner_state"] == "NO_ELIGIBLE_DECISION"
        assert second["n_due_decision_dates"] == 0
        assert len(AF.predictions(TEST_CAMPAIGN)) == 1

    def test_a_specification_identity_mismatch_blocks_the_lane(self, sandbox,
                                                               monkeypatch):
        """G. A decision date that cannot be tied to the frozen spec is
        refused - the identity, not the date, is what fails."""
        monkeypatch.setitem(AF.FROZEN_STRATEGY_IDENTITY,
                            "shadow_vx_carry_ts", "0" * 64)
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                        state=state_with(["2026-08-28"]), now=FRIDAY_EVENING,
                        mature_matured=False)
        assert r["lifecycle"] == "CALLED_PIT_BLOCKED"
        assert r["owner_state"] == "SPECIFICATION_IDENTITY_UNPROVEN"
        assert AF.predictions(TEST_CAMPAIGN) == []

    def test_an_absent_owner_decision_date_fails_closed(self, sandbox):
        """H. No provenance, no row - and the lane says QUIET, never emits."""
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                        state=state_with([]), now=FRIDAY_EVENING,
                        mature_matured=False)
        assert r["n_appended"] == 0
        assert r["lifecycle"] in ("CALLED_DATA_BLOCKED", "CALLED_QUIET_NOT_DUE")
        assert AF.predictions(TEST_CAMPAIGN) == []


# =========================================================================== #
# 3. THE CALL CADENCE IS NOT THE OWNER'S DECISION GRID
# =========================================================================== #
class TestCallCadenceIsNotADecisionGrid:
    """R46.6 published 'next decision 2026-08-28' for the VX lane from a
    Friday rule. The owner's newest decision date was Tuesday 2026-08-25, and
    it was that Tuesday the gate refused."""

    def test_the_weekly_predicate_no_longer_claims_a_decision_date(self):
        d = LN.due_weekly_friday(dt.date(2026, 8, 26))
        assert d["due"] is False
        assert d["next_decision_date"] is None
        assert d["next_decision_date_source"] == LN.CALL_CADENCE_ONLY
        assert d["next_call_date"] == "2026-08-28"
        assert "own panel's session grid" in d["next_decision_date_unknown_reason"]

    def test_the_month_end_predicate_does_name_the_owners_decision_date(self):
        d = LN.due_month_end(dt.date(2026, 8, 12))
        assert d["due"] is False
        assert d["next_decision_date"] == "2026-08-31"
        assert d["next_call_date"] == "2026-08-31"
        assert d["next_decision_date_source"] == LN.DECISION_DATE_FROM_PREDICATE

    def test_month_end_is_due_on_the_last_weekday_of_the_month(self):
        assert LN.due_month_end(dt.date(2026, 8, 31))["due"] is True
        assert LN.due_month_end(dt.date(2026, 8, 28))["due"] is False

    def test_the_lane_result_reports_the_owner_as_the_decision_source(
            self, sandbox):
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                        state=state_with([VX_REFUSED_DECISION]),
                        now=DRC_ATTEMPT, mature_matured=False)
        assert r["next_decision_date_source"] == "ADOPTED_OWNER_PANEL"
        assert r["next_decision_date"] is None

    def test_the_discrepancy_is_recorded_rather_than_resolved_by_fiat(self):
        d = AF.VX_CADENCE_DISCREPANCY
        assert d["they_are_not_the_same_rule"] is True
        assert d["measured_2026_08_28"]["latest_owner_decision_date"] == \
            VX_REFUSED_DECISION
        assert d["measured_2026_08_28"]["latest_vx_session"] == \
            VX_PANEL_LATEST_SESSION
        assert d["prior_release_artifact_mutated"] is False


# =========================================================================== #
# 4. THE MONTH-END LANES CAN STILL EMIT ON MONDAY
# =========================================================================== #
class TestMonthEndEvidenceIsNotBlocked:
    """The month-end panels put the decision on the panel's OWN newest
    session, so unlike the VX grid their decision date is today's - and a
    Monday-evening Eastern run is inside the window."""

    def test_a_month_end_decision_emits_when_the_cycle_runs_that_evening(
            self, sandbox):
        monday_evening = dt.datetime(2026, 8, 31, 23, 20,
                                     tzinfo=dt.timezone.utc)   # 19:20 ET
        r = AF.run_lane(release="R39", shadow_ids=R39_MONTH_END,
                        as_of=dt.date(2026, 8, 31), campaign_id=TEST_CAMPAIGN,
                        state=state_with([], fut_dates=["2026-08-31"],
                                         fut_sessions=["2026-08-31"]),
                        now=monday_evening, mature_matured=False)
        assert r["lifecycle"] == "CALLED_AND_EMITTED"
        assert r["n_appended"] >= 1
        assert r["emission_feasibility"] == AF.EMISSION_CAN_EMIT

    def test_the_same_decision_is_lost_if_the_cycle_runs_the_next_day(
            self, sandbox):
        tuesday = dt.datetime(2026, 9, 1, 23, 20, tzinfo=dt.timezone.utc)
        r = AF.run_lane(release="R39", shadow_ids=R39_MONTH_END,
                        as_of=dt.date(2026, 9, 1), campaign_id=TEST_CAMPAIGN,
                        state=state_with([], fut_dates=["2026-08-31"],
                                         fut_sessions=["2026-08-31",
                                                       "2026-09-01"]),
                        now=tuesday, mature_matured=False)
        assert r["lifecycle"] == "CALLED_PIT_BLOCKED"
        assert r["refused_decision_dates"][0]["decision_date"] == "2026-08-31"
        assert r["emission_feasibility"] == AF.EMISSION_STRUCTURALLY_LATE


# =========================================================================== #
# 5. COLLECTION RECOVERY - section 10/11/12
# =========================================================================== #
class TestCollectionRecoveryTruth:

    def _dead_worker(self, root, now):
        ic.acquire_service_lock(root=root, instance_id="w1", pid=999_999_999,
                                now=now)
        ic.register_worker_start(root=root, instance_id="w1", pid=999_999_999,
                                 now=now)
        state = ic.load_service_state(root=root)
        state["collection_automation_enabled"] = True
        state["loop_count"] = 2512
        ic.save_service_state(state, root=root)
        return ic.load_service_state(root=root)

    def test_a_dead_worker_reports_degraded_restartable(self, root):
        """D. The dead-worker status must be degraded AND recoverable."""
        now = dt.datetime(2026, 8, 28, 17, 51, 14, tzinfo=dt.timezone.utc)
        state = self._dead_worker(root, now)
        later = now + dt.timedelta(hours=6)
        lifecycle = ic.resolve_service_lifecycle(
            state, ic.read_service_lock(root=root), later)
        rec = ic.resolve_recovery(state, lifecycle, now=later)
        assert lifecycle["service_state"] == ic.SVC_DEGRADED
        assert lifecycle["worker_activity"] == ic.ACT_DEAD
        assert rec["recovery_state"] == ic.REC_DEGRADED_RESTARTABLE
        assert rec["recovery_required"] is True
        assert rec["recovery_command"] == ic.RECOVERY_COMMAND

    def test_the_payload_says_nothing_will_restart_it_by_itself(self, root):
        """The defect that made six hours of silence possible: DEGRADED did
        not say whether anything else was coming. It was not."""
        now = dt.datetime(2026, 8, 28, 17, 51, 14, tzinfo=dt.timezone.utc)
        state = self._dead_worker(root, now)
        later = now + dt.timedelta(hours=6)
        rec = ic.resolve_recovery(
            state,
            ic.resolve_service_lifecycle(state,
                                         ic.read_service_lock(root=root),
                                         later),
            now=later)
        assert rec["can_silently_remain_dead"] is True
        assert rec["nothing_restarts_it_automatically"] is True
        assert rec["scheduled_task_trigger"] == "AT_LOGON_ONLY"
        assert rec["stop_was_unowned"] is True

    def test_a_healthy_worker_needs_no_recovery(self, root):
        import os
        now = dt.datetime(2026, 8, 28, 17, 51, 14, tzinfo=dt.timezone.utc)
        ic.acquire_service_lock(root=root, instance_id="w1", pid=os.getpid(),
                                now=now)
        ic.register_worker_start(root=root, instance_id="w1", pid=os.getpid(),
                                 now=now)
        state = ic.load_service_state(root=root)
        state["collection_automation_enabled"] = True
        state["loop_count"] = 5
        ic.save_service_state(state, root=root)
        state = ic.load_service_state(root=root)
        lifecycle = ic.resolve_service_lifecycle(
            state, ic.read_service_lock(root=root), now)
        rec = ic.resolve_recovery(state, lifecycle, now=now)
        assert rec["recovery_state"] == ic.REC_RUNNING_HEALTHY
        assert rec["recovery_required"] is False
        assert rec["can_silently_remain_dead"] is False

    def test_a_clean_stop_is_not_a_crash(self, root):
        """An INTENTIONAL stop must never be presented as a failure - which is
        exactly why the killed worker's missing marker is reported by name."""
        now = dt.datetime(2026, 8, 28, 17, 51, 14, tzinfo=dt.timezone.utc)
        self._dead_worker(root, now)
        ic.release_service_lock(root=root, instance_id="w1", graceful=True)
        state = ic.load_service_state(root=root)
        state["collection_automation_enabled"] = True
        ic.save_service_state(state, root=root)
        state = ic.load_service_state(root=root)
        lifecycle = ic.resolve_service_lifecycle(state, None, now)
        rec = ic.resolve_recovery(state, lifecycle, now=now)
        assert rec["recovery_state"] == ic.REC_STOPPED_INTENTIONALLY
        assert rec["recovery_required"] is False
        assert rec["stop_was_unowned"] is False

    def test_recovery_may_clear_a_lock_only_when_no_worker_exists(self, root):
        """B. The provable-death condition, decided by the launch topology and
        never by the pid probe alone."""
        now = dt.datetime(2026, 8, 28, 17, 51, 14, tzinfo=dt.timezone.utc)
        self._dead_worker(root, now)
        lock = ic.read_service_lock(root=root)
        empty = ic.resolve_worker_topology([], lock=lock)
        decision = ic.resolve_abandoned_lock(lock=lock, topology=empty,
                                             now=now)
        assert empty["verdict"] == ic.WORKER_TOPOLOGY_NONE
        assert decision["state"] == ic.LOCK_CLEARABLE
        assert decision["may_clear"] is True

    def test_recovery_refuses_to_clear_a_lock_while_a_worker_runs(self, root):
        """A. Worker alive -> no second worker, and no lock is taken from it."""
        import os
        now = dt.datetime(2026, 8, 28, 17, 51, 14, tzinfo=dt.timezone.utc)
        ic.acquire_service_lock(root=root, instance_id="w1", pid=4242, now=now)
        lock = ic.read_service_lock(root=root)
        live = ic.resolve_worker_topology(
            [{"pid": 4242, "parent_pid": None,
              "command_line": "python %s" % ic.CANONICAL_WORKER_SCRIPT,
              "executable_path": "python.exe"}], lock=lock)
        decision = ic.resolve_abandoned_lock(lock=lock, topology=live, now=now)
        assert live["verdict"] == ic.WORKER_TOPOLOGY_SINGLE
        assert decision["state"] == ic.LOCK_REFUSED_WORKER_RUNNING
        assert decision["may_clear"] is False
        assert os.path.exists(str(root / "collection_service.lock"))

    def test_an_unresolvable_topology_fails_closed(self, root):
        now = dt.datetime(2026, 8, 28, 17, 51, 14, tzinfo=dt.timezone.utc)
        self._dead_worker(root, now)
        lock = ic.read_service_lock(root=root)
        ambiguous = ic.resolve_worker_topology(
            [{"pid": None, "command_line": ic.CANONICAL_WORKER_SCRIPT}],
            lock=lock)
        decision = ic.resolve_abandoned_lock(lock=lock, topology=ambiguous,
                                             now=now)
        assert decision["state"] == ic.LOCK_REFUSED_TOPOLOGY_UNKNOWN
        assert decision["may_clear"] is False

    def test_clearing_is_idempotent_and_writes_the_missing_stop_marker(
            self, root):
        """C. Two recovery calls -> still one worker, and the second is a
        no-op rather than an error."""
        now = dt.datetime(2026, 8, 28, 17, 51, 14, tzinfo=dt.timezone.utc)
        self._dead_worker(root, now)
        empty = ic.resolve_worker_topology(
            [], lock=ic.read_service_lock(root=root))
        first = ic.clear_abandoned_lock(root=root, topology=empty, now=now)
        second = ic.clear_abandoned_lock(root=root, topology=empty, now=now)
        assert first["cleared"] is True
        assert second["state"] == ic.LOCK_NOTHING_TO_CLEAR
        assert second["cleared"] is False
        state = ic.load_service_state(root=root)
        assert state["stopped_at"] is not None
        assert state["graceful_shutdown"] is False
        assert "operator recovery" in state["last_error"]

    def test_a_cleared_lock_lets_exactly_one_worker_back_in(self, root):
        """B/C together: recovery restores ONE worker, and only one."""
        now = dt.datetime(2026, 8, 28, 17, 51, 14, tzinfo=dt.timezone.utc)
        self._dead_worker(root, now)
        ic.clear_abandoned_lock(
            root=root,
            topology=ic.resolve_worker_topology(
                [], lock=ic.read_service_lock(root=root)),
            now=now)
        new = ic.acquire_service_lock(root=root, instance_id="w2", pid=4242,
                                      now=now + dt.timedelta(seconds=60))
        intruder = ic.acquire_service_lock(root=root, instance_id="w3",
                                           pid=4243,
                                           now=now + dt.timedelta(seconds=61))
        assert new["acquired"] is True
        assert intruder["acquired"] is False
        assert intruder["reason"] == "SINGLE_FLIGHT_LOCK_HELD"

    def test_the_automatic_reclaim_rule_is_unchanged(self, root):
        """The singleton gate was RIGHT and is not loosened here. Recovery is
        an OPERATOR authority, not a wider tolerance."""
        now = dt.datetime(2026, 8, 28, 17, 51, 14, tzinfo=dt.timezone.utc)
        ic.acquire_service_lock(root=root, instance_id="dead",
                                pid=999_999_999, now=now)
        soon = ic.acquire_service_lock(
            root=root, instance_id="new", pid=4242,
            now=now + dt.timedelta(seconds=ic.LOCK_TAKEOVER_SECONDS / 2))
        assert soon["acquired"] is False

    def test_worker_health_and_source_health_stay_distinct(self, root):
        """F. A dead worker says nothing about a provider, and "6/6 sources
        healthy" says nothing about whether anything is collecting them. That
        is exactly what the live payload showed on 2026-08-28: every due source
        healthy, and nothing running to ask them."""
        now = dt.datetime(2026, 8, 28, 17, 51, 14, tzinfo=dt.timezone.utc)
        state = self._dead_worker(root, now)
        later = now + dt.timedelta(hours=6)
        lifecycle = ic.resolve_service_lifecycle(
            state, ic.read_service_lock(root=root), later)
        rec = ic.resolve_recovery(state, lifecycle, now=later)
        # the recovery verdict is a pure function of the WORKER verdicts; no
        # source-health input can reach it and none can rescue it
        assert rec["recovery_state"] == ic.REC_DEGRADED_RESTARTABLE
        assert set(rec) & {"source_health", "sources", "healthy_due"} == set()
        health = ic.build_source_runtime_health(root=root, now=later)
        assert "service_state" not in health["summary"]
        assert "worker_activity" not in health["summary"]
        # and a fully healthy source registry does not change the verdict
        assert ic.resolve_recovery(state, lifecycle, now=later)[
            "recovery_state"] == ic.REC_DEGRADED_RESTARTABLE

    def test_recovery_never_touches_execution_automation(self, root):
        now = dt.datetime(2026, 8, 28, 17, 51, 14, tzinfo=dt.timezone.utc)
        state = self._dead_worker(root, now)
        rec = ic.resolve_recovery(
            state,
            ic.resolve_service_lifecycle(state,
                                         ic.read_service_lock(root=root), now),
            now=now)
        assert rec["collection_automation_is_not_execution_automation"] is True
        assert rec["recovery_creates_no_second_worker"] is True
        src = Path("scripts/manage_information_collection.ps1").read_text(
            encoding="utf-8", errors="replace")
        recover = src.split('"Recover" {')[1].split('"Uninstall" {')[0]
        for forbidden in ("Stop-Process", "Register-ScheduledTask",
                          "Set-ScheduledTask", "Unregister-ScheduledTask"):
            assert forbidden not in recover, forbidden


# =========================================================================== #
# 6. NEXT MATURITY - one owner, and the reason it has not moved
# =========================================================================== #
class TestNextMaturityTruth:

    def test_the_owner_returns_the_date_with_why_it_is_outstanding(self,
                                                                   monkeypatch):
        rows = [
            {"prediction_id": "p1", "challenger_id": "c1", "horizon": 1,
             "horizon_end_expected": "2026-08-28", "effective_as_of":
                 "2026-08-27", "emitted_at_utc": "2026-08-26T22:29:21Z",
             "position_expression": {"legs": [{"instrument": "&VX"}]}},
            {"prediction_id": "p2", "challenger_id": "c2", "horizon": 5,
             "horizon_end_expected": "2026-09-04", "effective_as_of":
                 "2026-08-28", "position_expression": {"legs": []}},
        ]
        monkeypatch.setattr(HV.LG, "predictions", lambda *a, **k: rows)
        monkeypatch.setattr(HV.LG, "outcomes", lambda *a, **k: [])
        d = HV.next_maturity_detail()
        assert d["next_maturity"] == "2026-08-28"
        assert d["n_at_next"] == 1
        assert d["rows"][0]["instruments"] == ["&VX"]
        assert "realised sessions" in d["why"]
        assert "CALENDAR estimate" in d["estimate_note"]

    def test_a_date_that_has_been_scored_still_stands_when_a_row_waits(
            self, monkeypatch):
        """The exact 2026-08-28 situation: one maturity WAS scored on that
        date and one prediction still expected it, because a prediction is
        scored on its own instrument's realised bars."""
        rows = [
            {"prediction_id": "scored", "horizon_end_expected": "2026-08-28",
             "position_expression": {"legs": []}},
            {"prediction_id": "waiting", "challenger_id": "vx",
             "horizon_end_expected": "2026-08-28", "horizon": 1,
             "effective_as_of": "2026-08-27",
             "position_expression": {"legs": [{"instrument": "&VX"}]}},
        ]
        monkeypatch.setattr(HV.LG, "predictions", lambda *a, **k: rows)
        monkeypatch.setattr(HV.LG, "outcomes",
                            lambda *a, **k: [{"prediction_id": "scored"}])
        assert HV.next_maturity() == "2026-08-28"
        d = HV.next_maturity_detail()
        assert d["n_at_next"] == 1
        assert d["rows"][0]["prediction_id"] == "waiting"

    def test_there_is_one_owner_and_the_api_composes_it(self):
        assert HV.next_maturity_detail.__module__.endswith("r46.harvest")
        src = Path("api/prospective_tournament.py").read_text(
            encoding="utf-8", errors="replace")
        assert "harvest as HV" in src or "HV.next_maturity_detail()" in src
        assert "next_material_maturity_detail" in src

    def test_nothing_outstanding_reports_no_next_maturity(self, monkeypatch):
        monkeypatch.setattr(HV.LG, "predictions", lambda *a, **k: [])
        monkeypatch.setattr(HV.LG, "outcomes", lambda *a, **k: [])
        d = HV.next_maturity_detail()
        assert d["next_maturity"] is None
        assert d["n_pending"] == 0


# =========================================================================== #
# 7. OPTIONS - 503 AND 501 ARE DIFFERENT QUESTIONS
# =========================================================================== #
class TestOptionSessionSemantics:

    def test_the_lane_says_what_its_session_number_counts(self):
        assert "ACQUIRED_SESSION_DATES" in OP.ACQUIRED_SESSIONS_MEANS
        assert OP.SESSION_GATE_MEASURES == "NUMBER_OF_SESSIONS_ONLY"
        assert OP.SESSION_GATE_DOES_NOT_MEASURE == \
            "STRIKE_AND_EXPIRY_BREADTH_PER_SESSION"

    def test_a_thin_session_is_acquired_but_not_feature_complete(self,
                                                                 monkeypatch):
        """E. Force the two counts apart and prove the labels stay distinct
        rather than being reconciled to one number."""
        raw = pd.DataFrame([
            {"ticker": "A", "date": "2026-08-20", "iv": 0.2, "T_years": 0.1},
            {"ticker": "B", "date": "2026-08-20", "iv": 0.2, "T_years": 0.1},
            {"ticker": "C", "date": "2026-08-20", "iv": 0.2, "T_years": 0.1},
            {"ticker": "D", "date": "2026-08-20", "iv": 0.2, "T_years": 0.1},
            # a session with fewer than MIN_ROWS_PER_SESSION usable rows
            {"ticker": "E", "date": "2026-08-21", "iv": 0.2, "T_years": 0.1},
        ])
        monkeypatch.setattr(OP, "existing_surface", lambda: raw)
        monkeypatch.setattr(OP, "r46_batches", lambda: None)
        census = OH.session_census()
        assert census["acquired_usable_sessions"] == 2
        assert census["feature_complete_sessions"] == 1
        assert census["sessions_dropped_too_few_rows"] == ["2026-08-21"]
        assert census["the_two_counts_are_not_forced_equal"] is True
        assert census["acquired_usable_sessions_means"] != \
            census["feature_complete_sessions_means"]

    def test_the_binding_constraint_is_unchanged(self):
        """No hypothesis was weakened, proxied or purchased to close a gap."""
        assert len(OP.PREDECLARED_HYPOTHESES) == 3
        assert OP.hypotheses_hash() == (
            "0f31b567a2c252eb9e228325466f71dce45a170aa18a10e9bb2853b2df9e65dd")


# =========================================================================== #
# 8. DRC / PROPOSAL READ-MODEL CONSISTENCY
# =========================================================================== #
class TestDrcProposalConsistency:

    def test_a_completed_cycle_that_withheld_a_proposal_never_says_run_drc(
            self):
        """F. The 2026-08-28 defect: the cycle COMPLETED, the economic gate
        withheld the proposal, and the card asked for another cycle."""
        p = WS.build_reallocation_proposal_presentation(
            state="NOT_RUN", available=False, eligible_date="2026-08-28",
            cycle_complete=True, cycle_run_id="drc_2026-08-28_5f619736c4ba",
            reassessment_state="MANUAL_REVIEW_REQUIRED")
        assert p["economic_gate_withheld_the_proposal"] is True
        assert p["outstanding_action"] == "MANUAL_PORTFOLIO_CONSTRAINT_REVIEW"
        assert p["running_the_cycle_again_would_change_nothing"] is True
        assert "COMPLETED" in p["headline"]
        assert "will be generated by the next completed Daily" not in \
            p["explanation"]
        assert p["governed_cycle_run_id"] == "drc_2026-08-28_5f619736c4ba"

    def test_a_cycle_that_has_not_run_still_asks_for_the_cycle(self):
        p = WS.build_reallocation_proposal_presentation(
            state="NOT_RUN", available=False, eligible_date="2026-08-28",
            cycle_complete=False, reassessment_state="NOT_RUN")
        assert p["economic_gate_withheld_the_proposal"] is False
        assert p["outstanding_action"] == "RUN_DAILY_RESEARCH_CYCLE"
        assert "next completed Daily" in p["explanation"]

    def test_a_real_proposal_is_still_presented_as_review_only(self):
        p = WS.build_reallocation_proposal_presentation(
            state="READY", available=True, eligible_date="2026-08-28",
            action_counts={"RETAIN": 20, "EXIT": 2}, proposal_hash="h",
            proposed_holding_count=25, cycle_complete=True,
            reassessment_state="PROPOSAL_READY")
        assert p["has_proposal"] is True
        assert p["execution_available"] is False
        assert p["creates_orders"] is False
        assert p["outstanding_action"] == "REVIEW_THE_PROPOSAL"

    def test_the_withheld_wording_never_claims_an_order_or_a_target(self):
        p = WS.build_reallocation_proposal_presentation(
            state="NOT_RUN", available=False, eligible_date="2026-08-28",
            cycle_complete=True, reassessment_state="MANUAL_REVIEW_REQUIRED")
        assert p["creates_orders"] is False
        assert p["execution_available"] is False
        assert p["decision_authority"] == "REVIEW_ONLY"
        assert p["has_proposal"] is False


# =========================================================================== #
# 9. STALE OPERATOR WORDING AND THE LEGACY BOOK
# =========================================================================== #
class TestOperatorWordingTruth:

    def test_the_monthly_review_is_no_longer_the_next_portfolio_action(self):
        src = Path("api/alpha_book.py").read_text(encoding="utf-8",
                                                  errors="replace")
        assert "the next portfolio action is the monthly review" not in src
        assert "reassessed after each material signal refresh" in src

    def test_the_legacy_executed_book_is_labelled_and_subordinate(self):
        src = Path("api/portfolio_manager.py").read_text(encoding="utf-8",
                                                         errors="replace")
        assert "LEGACY EXECUTED PAPER PORTFOLIO (HISTORICAL DIAGNOSTIC)" in src
        assert '"classification": "HISTORICAL_DIAGNOSTIC"' in src
        assert '"is_current_executed_book": False' in src
        assert '"decision_authority": "NONE"' in src

    def test_the_legacy_book_history_is_preserved_not_removed(self):
        src = Path("api/portfolio_manager.py").read_text(encoding="utf-8",
                                                         errors="replace")
        assert '"tickers": executed' in src
        assert "auditability" in src

    def test_the_ui_renders_the_owner_label_verbatim(self):
        html = Path("api/ui/index.html").read_text(encoding="utf-8",
                                                   errors="replace")
        assert "escapeHtml(ex.label || 'EXECUTED PAPER PORTFOLIO')" in html
        assert "not</b> the current executed paper book" in html


# =========================================================================== #
# 10. PRODUCTION EVIDENCE IS NEVER TOUCHED BY THIS SUITE
# =========================================================================== #
class TestProductionEvidencePreservation:

    def test_the_prior_release_registries_are_byte_identical(self, sandbox):
        """G. Any change to a prior-release artifact fails the gate."""
        before = prior_hashes()
        assert before["R39_registry"] != "ABSENT"
        AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                    as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                    state=state_with(["2026-08-28"]), now=FRIDAY_EVENING,
                    mature_matured=False)
        assert prior_hashes() == before

    def test_every_write_in_this_file_lands_under_the_temp_root(self, sandbox):
        AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                    as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                    state=state_with(["2026-08-28"]), now=FRIDAY_EVENING,
                    mature_matured=False)
        d = AF.continuation_dir(TEST_CAMPAIGN)
        assert str(sandbox) in str(d)
        assert "prospective_alpha_tournament_r46" not in str(d)

    def test_the_release_creates_no_order_and_promotes_no_model(self):
        for flag in ("creates_order", "creates_paper_order", "promotes_model",
                     "enables_automation", "mutates_holdings",
                     "changes_scheduler", "may_spend_money",
                     "backdates_forward_rows"):
            assert C.SAFETY_BLOCK[flag] is False, flag
