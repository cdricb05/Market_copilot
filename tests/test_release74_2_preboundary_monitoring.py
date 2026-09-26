r"""Release 74.2 - a decision boundary must be warned about BEFORE it passes.

THE DEFECT THIS LOCKS OUT
-------------------------
R68 gave every registration a producer. R72 gave the estate a permanent LOSS
ledger, and it worked: it counted eleven boundaries that passed with no frozen
decision. But every state in that ledger is a post mortem. The earliest instant
``PERMANENT_MISS_NOT_RECORDED_AS_A_FORFEITURE`` can be true is after the
opportunity is already gone.

The live consequence, read from the runtime journal on 2026-09-26: the next-open
SPY challenger missed NINE CONSECUTIVE boundaries, 2026-09-15 to 2026-09-25. On
each of those days the producer ran, reported ``DATA_BLOCKED`` with the exact
blocker ("the historical vendor has not published session S-1 yet"), and then
reported ``FORFEITED`` about an hour later. The second miss was fully predictable
from the first. Nothing anywhere turned that into a warning while a window was
still open, and nothing distinguished a nine-day structural failure from nine
unrelated accidents.

Every test below fails if that prospective half is removed, weakened, or allowed
to compute its verdict from facts the heartbeat's allow-list has dropped - the
drop that has now happened three times (R68 ``next_boundaries``, R72
``missed_boundaries``, R74.2 ``publication``).

NOTHING HERE TOUCHES A LIVE STORE. Every assertion runs on injected fixtures:
:func:`producer_coverage` takes ``accrual_by_identity`` and ``runs``, and the
readiness functions are pure.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from paper_trader.api import forward_producer_health as H


NOW = datetime(2026, 9, 26, 16, 0, tzinfo=timezone.utc)

#: A challenger the producer declaration actually knows. A made-up id is
#: correctly short-circuited to R_NO_PRODUCER before any input or boundary
#: test runs, so using one would test the orphan path by accident.
PRODUCED = "REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1"


def _beat(**kw):
    """A producer heartbeat as :func:`heartbeat` publishes one."""
    base = {
        "ran": True,
        "last_run_id": "run-1",
        "last_run_started_utc": (NOW - timedelta(minutes=15)).isoformat(),
        "last_stage_state": "NOT_DUE",
        "last_advance_state": None,
        "last_detail": None,
        "blocked_on": None,
        "next_boundaries": [],
        "missed_boundaries": [],
        "publication": None,
        "entry_session": None,
        "information_session": None,
        "entry_state": None,
        "forward_panel_last_session": "2026-09-25",
        "declared_grid_owner": "a.declared.grid.owner",
    }
    base.update(kw)
    return base


def _accrual(challenger_id, **kw):
    base = {
        "challenger_id": challenger_id,
        "asset_class": "US_ETF",
        "cadence_sessions": 5,
        "horizon_sessions": 5,
        "predictions_emitted": 0,
        "matured_observations": 0,
        "pending_observations": 0,
        "forfeitures": 0,
        "last_emission_session": None,
        "next_eligible_observation_session": None,
        "current_accrual_state": "NOT_DUE",
        "latest_blocker": None,
        "registration_session": "2026-09-12",
    }
    base.update(kw)
    return base


# --------------------------------------------------------------------------- #
# 1. THE ALLOW-LIST. The verdict is computed from journalled facts, so a field
#    the heartbeat drops is a verdict computed from None that reads as healthy.
# --------------------------------------------------------------------------- #
READINESS_INPUTS = ("publication", "entry_session", "information_session",
                    "entry_state", "next_boundaries", "missed_boundaries",
                    "forward_panel_last_session", "blocked_on")


def test_heartbeat_carries_every_fact_the_readiness_verdict_needs():
    """The fourth occurrence of this drop must fail here, not in production.

    The producers have journalled ``publication``, ``entry_session``,
    ``information_session`` and ``entry_state`` since R62.3.3. The health
    module's extraction dropped all four, so the only component that asks "are
    the inputs for the next decision in hand?" could not see the answer sitting
    on disk while nine boundaries were lost.
    """
    stage = "next_open_prospective_decision"
    runs = {"runs": [{
        "run_id": "r1", "started_utc": NOW.isoformat(),
        "stages": [{
            "stage": stage, "state": "DATA_BLOCKED",
            "advance_state": "NEXT_OPEN_AWAITING_SOURCE_PUBLICATION",
            "publication": "NOT_PUBLISHED",
            "entry_session": "2026-09-28",
            "information_session": "2026-09-25",
            "entry_state": "AWAITING_SOURCE_PUBLICATION",
            "blocked_on": "the historical vendor has not published session "
                          "2026-09-25 yet",
            "blocked_owner": "the vendor",
            "declared_grid_owner": "grid.owner",
            "next_boundaries": ["2026-09-28"],
            "missed_boundaries": ["2026-09-25"],
            "forward_panel_last_session": "2026-09-25",
        }]}]}
    beat = H.heartbeat(runs=runs)["stages"][stage]
    for field in READINESS_INPUTS:
        assert field in beat, (
            "heartbeat() dropped %r, which the readiness verdict is computed "
            "from. This is the same allow-list defect as R68 and R72." % field)
    assert beat["publication"] == "NOT_PUBLISHED"
    assert beat["information_session"] == "2026-09-25"
    assert beat["entry_session"] == "2026-09-28"


def test_lifecycle_republishes_the_readiness_inputs():
    """A reader given a verdict and not its inputs cannot check it."""
    beat = _beat(publication="PUBLISHED", information_session="2026-09-25",
                 entry_session="2026-09-28", next_boundaries=["2026-09-28"])
    row = H.lifecycle_state(_accrual(PRODUCED), beat=beat)
    hb = row["producer_heartbeat"]
    for field in ("publication", "entry_session", "information_session",
                  "entry_state", "declared_grid_owner"):
        assert field in hb, "lifecycle dropped %r from the heartbeat" % field


# --------------------------------------------------------------------------- #
# 2. INPUT READINESS. Silence is never confirmation.
# --------------------------------------------------------------------------- #
def test_a_producer_that_declares_no_publication_state_is_not_reported_present():
    out = H.input_readiness(_beat())
    assert out["input_state"] == H.I_NOT_DECLARED
    assert out["input_state"] != H.I_PRESENT


def test_published_is_inputs_present_and_a_blocker_is_inputs_missing():
    assert H.input_readiness(
        _beat(publication="PUBLISHED"))["input_state"] == H.I_PRESENT
    missing = H.input_readiness(_beat(
        publication="NOT_PUBLISHED", last_stage_state="DATA_BLOCKED",
        blocked_on="the historical vendor has not published session X yet"))
    assert missing["input_state"] == H.I_MISSING
    assert "vendor" in missing["why"]


def test_a_blocker_outranks_a_stale_published_flag():
    """The blocker is the live fact; a leftover PUBLISHED must not mask it."""
    out = H.input_readiness(_beat(publication="PUBLISHED",
                                  blocked_on="waiting on session X"))
    assert out["input_state"] == H.I_MISSING


# --------------------------------------------------------------------------- #
# 3. THE WARNING ITSELF - raised BEFORE the boundary, not after.
# --------------------------------------------------------------------------- #
def test_an_imminent_boundary_with_missing_inputs_warns_before_it_passes():
    """The warning the nine SPY misses never produced."""
    beat = _beat(next_boundaries=["2026-09-28"], last_stage_state="DATA_BLOCKED",
                 publication="NOT_PUBLISHED",
                 blocked_on="the historical vendor has not published session "
                            "2026-09-25 yet")
    out = H.preboundary_readiness(_accrual(PRODUCED), beat, now=NOW)
    assert out["readiness"] == H.R_INPUT_LATE
    assert out["severity"] == H.SEV_WARN
    assert out["is_actionable_now"] is True
    assert out["asked_before_the_boundary"] is True, (
        "the whole point is that this is asked while the window is still open")
    assert out["boundary_is_imminent"] is True
    assert out["calendar_days_until_boundary"] == 2
    assert "vendor" in out["why"]


def test_a_distant_boundary_with_missing_inputs_is_not_yet_a_warning():
    """Most of these resolve. Crying wolf about them would drown the real one."""
    beat = _beat(next_boundaries=["2026-10-20"], last_stage_state="DATA_BLOCKED",
                 blocked_on="not published yet")
    out = H.preboundary_readiness(_accrual(PRODUCED), beat, now=NOW)
    assert out["readiness"] == H.R_INPUT_PENDING
    assert out["severity"] == H.SEV_OK
    assert out["is_actionable_now"] is False


def test_a_stale_producer_before_an_imminent_boundary_is_a_defect():
    """A dead producer and a late vendor have different owners."""
    beat = _beat(
        next_boundaries=["2026-09-28"], publication="PUBLISHED",
        last_run_started_utc=(NOW - timedelta(hours=30)).isoformat())
    out = H.preboundary_readiness(_accrual(PRODUCED), beat, now=NOW)
    assert out["readiness"] == H.R_PRODUCER_STALE
    assert out["severity"] == H.SEV_DEFECT
    assert out["producer_is_stale"] is True
    assert out["producer_hours_since_last_run"] > H.PRODUCER_STALE_AFTER_HOURS


def test_everything_in_hand_is_ready_and_raises_nothing():
    beat = _beat(next_boundaries=["2026-09-28"], publication="PUBLISHED")
    out = H.preboundary_readiness(_accrual(PRODUCED), beat, now=NOW)
    assert out["readiness"] == H.R_READY
    assert out["is_actionable_now"] is False


def test_no_boundary_declared_is_not_a_warning():
    out = H.preboundary_readiness(_accrual(PRODUCED), _beat(), now=NOW)
    assert out["readiness"] == H.R_NO_BOUNDARY
    assert out["is_actionable_now"] is False


# --------------------------------------------------------------------------- #
# 4. CHRONIC vs RECOVERED - the distinction that turns nine accidents into one
#    structural fact, and must NOT mislabel a producer that recovered.
# --------------------------------------------------------------------------- #
def test_nine_consecutive_misses_with_no_emission_since_is_chronic():
    """The live SPY case, exactly as the journal recorded it."""
    missed = ["2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18",
              "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24",
              "2026-09-25"]
    beat = _beat(next_boundaries=["2026-09-28"], missed_boundaries=missed,
                 publication="PUBLISHED", information_session="2026-09-25",
                 entry_session="2026-09-28")
    acc = _accrual("REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1",
                   last_emission_session=None)
    ch = H.chronic_miss(acc, beat)
    assert ch["is_chronic"] is True
    assert ch["n_consecutive_missed_boundaries"] == 9
    assert ch["recovered_after_the_newest_miss"] is False

    out = H.preboundary_readiness(acc, beat, now=NOW)
    assert out["readiness"] == H.R_CHRONIC
    assert out["is_actionable_now"] is True
    # The structural verdict must NOT claim the next boundary is unreachable
    # when its inputs are already in hand: that would be a false alarm about a
    # boundary the estate can still meet.
    assert out["next_boundary_inputs_in_hand"] is True
    assert "REACHABLE" in out["why"]
    # It must name the FIT as the failure, never the code.
    assert "publication latency" in out["why"]


def test_a_producer_that_emitted_after_its_miss_is_recovered_not_chronic():
    """FX carry missed 2026-09-15 and emitted 2026-09-22. Not a chronic fault."""
    beat = _beat(next_boundaries=["2026-09-29"],
                 missed_boundaries=["2026-09-15"])
    acc = _accrual("ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7",
                   last_emission_session="2026-09-22", predictions_emitted=1)
    ch = H.chronic_miss(acc, beat)
    assert ch["is_chronic"] is False
    assert ch["recovered_after_the_newest_miss"] is True


def test_a_long_run_of_misses_is_recovered_once_an_emission_postdates_them():
    missed = ["2026-09-15", "2026-09-16", "2026-09-17"]
    acc = _accrual("X", last_emission_session="2026-09-18")
    ch = H.chronic_miss(acc, _beat(missed_boundaries=missed))
    assert ch["is_chronic"] is False, (
        "an emission at or after the newest miss proves the producer can still "
        "meet a boundary; the run is over")


def test_below_the_threshold_is_not_yet_structural():
    acc = _accrual("X", last_emission_session=None)
    ch = H.chronic_miss(acc, _beat(missed_boundaries=["2026-09-21"]))
    assert ch["is_chronic"] is False
    assert ch["threshold"] == H.CHRONIC_CONSECUTIVE_MISSES


# --------------------------------------------------------------------------- #
# 5. NO SECOND CALENDAR. A class whose sessions this estate does not own gets
#    calendar days, never a fabricated weekday session count.
# --------------------------------------------------------------------------- #
def test_a_non_exchange_class_gets_no_invented_session_distance():
    beat = _beat(next_boundaries=["2026-10-20"], publication="PUBLISHED")
    out = H.preboundary_readiness(
        _accrual("ALPHA_RECOVERY_FUTURES_TS_TREND_H21_V1",
                 asset_class="MULTI_ASSET_FUTURES"), beat, now=NOW)
    assert out["eligible_sessions_until_boundary"] is None
    assert out["session_distance_owner"] is None
    assert out["calendar_days_until_boundary"] == 24
    assert "realised bar calendar" in str(out["observation_calendar_owner"])


def test_the_boundary_always_comes_from_the_producers_own_grid():
    beat = _beat(next_boundaries=["2026-09-30", "2026-09-28", "2026-10-01"],
                 publication="PUBLISHED")
    out = H.preboundary_readiness(_accrual(PRODUCED), beat, now=NOW)
    assert out["next_boundary"] == "2026-09-28", "the SOONEST boundary wins"
    assert out["next_boundary_source"] == "PRODUCER_DECLARED_GRID"


# --------------------------------------------------------------------------- #
# 6. THE AGGREGATE LEDGER, and its separation from the retrospective one.
# --------------------------------------------------------------------------- #
def _live_shaped_fixture():
    """The eight live registrations, shaped as the projection and journal hold
    them on 2026-09-26. Injected, never read from a store."""
    stage_no = "next_open_prospective_decision"
    stage_fx = "fx_carry_cadence_prospective_decision"
    stage_r58 = "r58_cadence_prospective_decision"
    proj = {
        "h_spy_next": _accrual("REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1"),
        "h_spy_old": _accrual("REVERSED_SPY_PUT_CALL_SKEW_H5"),
        "h_fx": _accrual("ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7",
                         asset_class="FX_FUTURES", predictions_emitted=1,
                         last_emission_session="2026-09-22"),
        "h_r58": _accrual("R58_FCF_PURE_V1", asset_class="US_EQUITY",
                          cadence_sessions=21, horizon_sessions=21,
                          predictions_emitted=1,
                          last_emission_session="2026-09-10"),
    }
    runs = {"runs": [{
        "run_id": "r1", "started_utc": (NOW - timedelta(minutes=15)).isoformat(),
        "stages": [
            {"stage": stage_no, "state": "NOT_DUE",
             "advance_state": "NEXT_OPEN_AWAITING_DECISION_WINDOW",
             "publication": "PUBLISHED", "entry_session": "2026-09-28",
             "information_session": "2026-09-25",
             "next_boundaries": ["2026-09-28", "2026-09-29"],
             "missed_boundaries": ["2026-09-15", "2026-09-16", "2026-09-17",
                                   "2026-09-18", "2026-09-21", "2026-09-22",
                                   "2026-09-23", "2026-09-24", "2026-09-25"]},
            {"stage": stage_fx, "state": "NOT_DUE",
             "advance_state": "FX_CADENCE_OUTSIDE_A_DECISION_WINDOW",
             "next_boundaries": ["2026-09-29"],
             "missed_boundaries": ["2026-09-15"]},
            {"stage": stage_r58, "state": "NOT_DUE",
             "advance_state": "R58_BOUNDARY_BEYOND_THE_FREEZE_LEAD",
             "next_boundaries": ["2026-10-09"], "missed_boundaries": []},
        ]}]}
    return proj, runs


def test_the_warning_ledger_names_only_the_registration_that_cannot_comply():
    proj, runs = _live_shaped_fixture()
    cov = H.producer_coverage(accrual_by_identity=proj, runs=runs)
    ids = [w["challenger_id"] for w in cov["preboundary_warnings"]]
    assert ids == ["REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1"], (
        "the FX carry recovery and the superseded record are not warnings")
    assert cov["n_preboundary_warnings"] == 1
    assert cov["every_next_boundary_is_reachable"] is False
    w = cov["preboundary_warnings"][0]
    # ACTIONABLE means: which boundary, how long is left, and whose problem.
    assert w["next_boundary"] == "2026-09-28"
    assert w["calendar_days_until_boundary"] == 2
    assert w["producer_owner"]
    assert w["readiness"] == H.R_CHRONIC


def test_a_correctly_blocked_strategy_stays_blocked_and_is_never_emitted_for():
    """Monitoring must not become a repair. Nothing here writes or emits."""
    proj, runs = _live_shaped_fixture()
    cov = H.producer_coverage(accrual_by_identity=proj, runs=runs)
    assert cov["writes_nothing"] is True
    assert cov["emits_no_prediction"] is True
    assert cov["read_only"] is True
    for row in cov["registrations"]:
        pr = row["preboundary_readiness"]
        assert pr["emits_no_prediction"] is True
        assert pr["changes_no_decision_contract"] is True
    # The permanent-loss ledger is untouched by the new prospective one.
    assert cov["n_permanent_misses_declared_by_producers"] == 10
    assert cov["n_permanent_misses_unrecorded"] == 10


def test_the_prospective_and_retrospective_ledgers_stay_separate():
    """Merging them would let "about to be lost" read as "already lost"."""
    proj, runs = _live_shaped_fixture()
    cov = H.producer_coverage(accrual_by_identity=proj, runs=runs)
    assert "preboundary_warnings" in cov
    assert "permanent_misses_by_challenger" in cov
    assert cov["n_preboundary_warnings"] != cov[
        "n_permanent_misses_unrecorded"], (
        "one counts registrations at risk, the other counts boundaries lost")


def test_the_headline_states_the_warning_so_it_cannot_be_missed():
    proj, runs = _live_shaped_fixture()
    cov = H.producer_coverage(accrual_by_identity=proj, runs=runs)
    assert "CANNOT MEET" in cov["headline"]
    assert "REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1" in cov["headline"]


def test_a_clean_estate_says_so_explicitly():
    """An absent warning must be an ASSERTION of reachability, not a silence."""
    proj = {"h": _accrual("R58_FCF_PURE_V1", asset_class="US_EQUITY",
                          predictions_emitted=1,
                          last_emission_session="2026-09-10")}
    runs = {"runs": [{
        "run_id": "r1", "started_utc": (NOW - timedelta(minutes=5)).isoformat(),
        "stages": [{"stage": "r58_cadence_prospective_decision",
                    "state": "NOT_DUE",
                    "next_boundaries": ["2026-10-09"],
                    "missed_boundaries": []}]}]}
    cov = H.producer_coverage(accrual_by_identity=proj, runs=runs)
    assert cov["n_preboundary_warnings"] == 0
    assert cov["every_next_boundary_is_reachable"] is True
    assert "Every next boundary is reachable" in cov["headline"]


# --------------------------------------------------------------------------- #
# 7. AN ORPHAN IS SEEN PROSPECTIVELY TOO.
# --------------------------------------------------------------------------- #
def test_a_boundary_with_no_producer_is_a_prospective_defect():
    beat = None
    acc = _accrual("A_CHALLENGER_NOBODY_DECLARED")
    out = H.preboundary_readiness(acc, beat, now=NOW)
    assert out["readiness"] == H.R_NO_PRODUCER
    assert out["severity"] == H.SEV_DEFECT


def test_a_declared_superseded_record_is_never_warned_about():
    acc = _accrual("REVERSED_SPY_PUT_CALL_SKEW_H5")
    out = H.preboundary_readiness(acc, None, now=NOW)
    assert out["readiness"] == H.R_NOT_EXPECTED
    assert out["is_actionable_now"] is False


# --------------------------------------------------------------------------- #
# 8. THE VOCABULARY IS CLOSED AND ITS SEVERITIES ARE TOTAL.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("state", H.READINESS_STATES)
def test_every_readiness_state_has_a_severity(state):
    assert state in H.READINESS_SEVERITY
    assert H.READINESS_SEVERITY[state] in (H.SEV_OK, H.SEV_WARN, H.SEV_DEFECT)


def test_every_actionable_state_is_a_warning_or_a_defect():
    for state in H.ACTIONABLE_READINESS_STATES:
        assert H.READINESS_SEVERITY[state] in (H.SEV_WARN, H.SEV_DEFECT)
    for state in H.READINESS_STATES:
        if H.READINESS_SEVERITY[state] in (H.SEV_WARN, H.SEV_DEFECT):
            assert state in H.ACTIONABLE_READINESS_STATES, (
                "%s is a warning nobody is told to act on" % state)


# --------------------------------------------------------------------------- #
# 9. THE RUNTIME RAISES IT - so the warning is durable before the deadline
#    rather than only computed when somebody happens to ask.
# --------------------------------------------------------------------------- #
def test_the_runtime_journals_the_warning_and_delegates_the_verdict():
    from pathlib import Path
    src = Path(__file__).resolve().parents[1] / "alpha_agent" / "r52" / \
        "runtime.py"
    text = src.read_text(encoding="utf-8")
    assert '"forward_preboundary_monitor"' in text, (
        "a warning computed only at read time is not raised before a deadline")
    assert "FPH.producer_coverage()" in text, (
        "the runtime must DELEGATE the verdict, not keep a second rule")
    # It must run ABOVE the efficiency gate. A boundary crosses into the warning
    # lead as time passes with no input changing, which is exactly the condition
    # under which the gate skips - so a warning the gate can hold back is not a
    # warning at all.
    assert text.index('"forward_preboundary_monitor"') < text.index(
        "gate = EL.decide("), (
        "a pre-boundary warning that the maturation gate can suppress would be "
        "absent on precisely the cycles where a deadline moves into range")
    # ...and BELOW the producers, whose heartbeat it reads for this cycle.
    assert text.index('"forward_preboundary_monitor"') > text.index(
        '"r58_cadence_prospective_decision"'), (
        "the verdict is computed from the producers' own journalled boundary "
        "grid, publication state and local panel session")


# --------------------------------------------------------------------------- #
# 10. THE TWO INPUT LEGS. A vendor that delivered and a local panel that did
#     not are different facts with different owners, and they came apart in the
#     live estate for four consecutive days.
# --------------------------------------------------------------------------- #
def test_a_published_vendor_with_a_lagging_local_panel_is_inputs_missing():
    """The exact live state on 2026-09-25T18:10Z.

    ``publication=PUBLISHED`` for information session 2026-09-24 while
    ``forward_panel_last_session`` was still 2026-09-21. A reader that stopped at
    the vendor leg would have called these inputs present, and the producer would
    still have had nothing to score.
    """
    out = H.input_readiness(_beat(
        publication="PUBLISHED", information_session="2026-09-24",
        forward_panel_last_session="2026-09-21"))
    assert out["input_state"] == H.I_MISSING, (
        "the local panel is what the signal is computed from; a published "
        "vendor does not put a session in the producer's hands")
    assert out["short_leg"] == H.L_LEG_LOCAL
    assert out["local_panel_reaches_the_information_session"] is False
    # The warning must name the LOCAL owner, not the vendor that delivered.
    assert "local acquisition" in out["why"]
    assert "not the" in out["why"]


def test_the_local_leg_satisfied_reports_inputs_present():
    out = H.input_readiness(_beat(
        publication="PUBLISHED", information_session="2026-09-25",
        forward_panel_last_session="2026-09-25"))
    assert out["input_state"] == H.I_PRESENT
    assert out["short_leg"] is None
    assert out["local_panel_reaches_the_information_session"] is True


def test_an_unpublished_vendor_names_the_vendor_leg():
    out = H.input_readiness(_beat(
        publication="NOT_PUBLISHED", last_stage_state="DATA_BLOCKED",
        blocked_on="the historical vendor has not published session X yet",
        information_session="2026-09-25",
        forward_panel_last_session="2026-09-21"))
    assert out["input_state"] == H.I_MISSING
    assert out["short_leg"] == H.L_LEG_VENDOR, (
        "when the vendor has not served the session, the local lag is a "
        "consequence and not the binding leg")


def test_a_lagging_local_panel_before_an_imminent_boundary_warns():
    """The warning that would have fired on 2026-09-16 instead of after nine."""
    beat = _beat(next_boundaries=["2026-09-28"], publication="PUBLISHED",
                 information_session="2026-09-25",
                 forward_panel_last_session="2026-09-21")
    out = H.preboundary_readiness(_accrual(PRODUCED), beat, now=NOW)
    assert out["readiness"] == H.R_INPUT_LATE
    assert out["is_actionable_now"] is True
    assert out["input_readiness"]["short_leg"] == H.L_LEG_LOCAL


def test_the_warning_ledger_carries_the_short_leg():
    beat_stage = "next_open_prospective_decision"
    proj = {"h": _accrual(PRODUCED)}
    runs = {"runs": [{
        "run_id": "r1", "started_utc": (NOW - timedelta(minutes=5)).isoformat(),
        "stages": [{"stage": beat_stage, "state": "NOT_DUE",
                    "publication": "PUBLISHED",
                    "information_session": "2026-09-25",
                    "forward_panel_last_session": "2026-09-21",
                    "next_boundaries": ["2026-09-28"],
                    "missed_boundaries": []}]}]}
    cov = H.producer_coverage(accrual_by_identity=proj, runs=runs)
    assert cov["n_preboundary_warnings"] == 1
    assert cov["preboundary_warnings"][0]["short_leg"] == H.L_LEG_LOCAL
