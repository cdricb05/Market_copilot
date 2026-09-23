r"""Release 68 - THE FORWARD EVIDENCE REPAIR, PROVED AS A CHAIN.

R67 measured that four registered challengers had no code path able to produce a
second decision, and stopped there. This suite proves the path R68 built, and
proves it end to end rather than function by function - because every one of the
four was made of functions that worked individually and a chain that did not
exist.

THE CHAIN, PROVED FOR EACH AFFECTED STRATEGY
    registered specification
      -> eligible decision boundary          (on the exchange calendar)
      -> current PIT inputs                  (a panel that has caught up)
      -> originating-owner signal calculation
      -> immutable frozen decision           (first write wins, window-bounded)
      -> canonical evidence emission
      -> pending observation
      -> genuine future maturation           (no hindsight)

WHAT IS SIMULATED AND WHAT IS NOT, STATED PLAINLY
    The scorer is the ONE part of the chain these tests do not drive with real
    vendor data: ``alpha_agent.r58.challengers.build`` needs a ~1,900-symbol
    Norgate panel, which no hermetic test may open. It is proved separately and
    for real - ``test_the_live_scorer_produces_a_book_for_every_challenger``
    runs it against the live forward panel and is skipped when that panel is
    absent. Everywhere else the book is injected, so what these tests prove is
    the CHAIN, which is exactly what was broken.

Every test is hermetic: the registry, the accrual store, the R58 research root
and the R68 root are redirected into pytest temp roots. Every test pins its own
clock - a suite whose verdicts move with the wall clock passes today and fails
tomorrow for no reason anybody can find. Nothing here promotes a model,
allocates capital, changes a holding or creates an order.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

from paper_trader.alpha_agent import r58 as R58                          # noqa: E402
from paper_trader.alpha_agent import r68 as R68                          # noqa: E402
from paper_trader.alpha_agent.alpha_recovery import (                    # noqa: E402
    prospective_decision as PD)
from paper_trader.alpha_agent.r52 import runtime as R52RT                # noqa: E402
from paper_trader.alpha_agent.r67 import forward_producer as R67FP       # noqa: E402
from paper_trader.alpha_agent.r68 import r58_cadence_runtime as R58R     # noqa: E402
from paper_trader.api import canonical_forward_accrual as CFA            # noqa: E402
from paper_trader.api import forward_challenger_registry as FCR          # noqa: E402
from paper_trader.api import forward_producer_health as FPH              # noqa: E402

CHALLENGERS = ("R58_SHORT_VOLUME_PRESSURE_V1", "R58_DISCLOSURE_INTENSITY_V1",
               "R58_FUND_MOMENTUM_VETO_V1", "R58_FCF_PURE_V1")
CHALLENGER = "R58_DISCLOSURE_INTENSITY_V1"
FREEZE_SESSION = "2026-09-03"
REGISTERED_ON = "2026-09-09"
FIRST_DECISION = "2026-09-10"
#: 21 and 42 eligible NYSE sessions after the first decision session. Asserted
#: against the calendar in ``test_the_cadence_grid_is_the_registrars_own``
#: rather than trusted, so a calendar change fails loudly here first.
SECOND_DECISION = "2026-10-09"
THIRD_DECISION = "2026-11-09"
RECORD_HASH = "e9ac82897fa0547d" * 4

BOOK_1 = {"AAA": 0.25, "BBB": 0.25, "CCC": 0.25, "DDD": 0.25}
BOOK_2 = {"AAA": 0.5, "CCC": 0.5}
BOOK_3 = {"BBB": 0.5, "DDD": 0.5}

TICKERS = ("AAA", "BBB", "CCC", "DDD")


# =========================================================================== #
# Fixtures
# =========================================================================== #
@pytest.fixture()
def stores(tmp_path, monkeypatch):
    monkeypatch.setenv(FCR.REGISTRY_DIR_ENV, str(tmp_path / "registry"))
    monkeypatch.setenv(CFA.STORE_DIR_ENV, str(tmp_path / "accrual"))
    monkeypatch.setenv(R58.RESEARCH_ROOT_ENV, str(tmp_path / "r58"))
    monkeypatch.setenv(R68.RESEARCH_ROOT_ENV, str(tmp_path / "r68"))
    return {"r58": tmp_path / "r58", "r68": tmp_path / "r68",
            "accrual": tmp_path / "accrual"}


def write_frozen_decision(root, challenger_id=CHALLENGER, *,
                          session=FREEZE_SESSION, weights=None,
                          record_hash=RECORD_HASH, cadence=21, horizon=21,
                          bps=12.5):
    """One adoption freeze, shaped exactly as ``r58.challengers.freeze`` writes it."""
    d = Path(root) / "challengers"
    d.mkdir(parents=True, exist_ok=True)
    rec = {
        "challenger_id": challenger_id,
        "eligible_session": session,
        "weights": dict(weights if weights is not None else BOOK_1),
        "record_hash": record_hash,
        "spec_hash": "spec_%s" % challenger_id,
        "weights_hash": "weights_%s" % challenger_id,
        "spec": {"role": "CONTROL" if challenger_id.endswith("FCF_PURE_V1")
                 else "CANDIDATE"},
        "construction": {"type": "LONG_ONLY_EQUAL_WEIGHT_TOP_N", "top_n": 50,
                         "cash_weight": 0.0,
                         "rebalance_cadence_sessions": cadence,
                         "evaluation_horizon_sessions": horizon},
        "cost_policy": {"bps_per_side": bps,
                        "charged_to": "strategy AND benchmark symmetrically"},
        "benchmark": "equal weight of the eligible universe at inception",
        "inception_rule": ("signal uses information available through the close "
                           "of %s; the position is effective at the NEXT close; "
                           "forward evidence begins strictly after inception and "
                           "is NEVER back-filled" % session),
    }
    (d / ("%s.json" % challenger_id)).write_text(
        json.dumps(rec, indent=1, sort_keys=True), encoding="utf-8")
    return rec


def identity(challenger_id=CHALLENGER, *, record_hash=RECORD_HASH):
    return {
        "challenger_id": challenger_id,
        "freeze_id": "H_r68test_%s" % challenger_id[-8:],
        "freeze_record_hash": record_hash,
        "model_spec_hash": "model_%s" % challenger_id,
        "feature_snapshot_hash": "r58_challenger_freeze",
        "asset_class": "US_EQUITY",
        "release": "R58",
        "model_family": "RANK_TOPN",
        "horizon_sessions": 21,
        "instrument_scope": [],
        "inception": FREEZE_SESSION,
        "identity_hash": "ih_%s" % challenger_id,
    }


ACTIVE = {"lifecycle_state": "ACTIVE", "adoptable": True,
          "never_resurrectable": False, "evidence": "NO_PERSISTED_FACT"}


def register(challenger_id=CHALLENGER, **kw):
    out = FCR.register_forward_challenger(
        identity=identity(challenger_id, **kw),
        observation_clock_starts=REGISTERED_ON,
        challenger_class="FORWARD_SIGNAL_CANONICAL", lifecycle=dict(ACTIVE))
    assert out["outcome"] in (FCR.REGISTERED, FCR.ALREADY_REGISTERED), out
    return out["registration"]


def sessions_upto(last):
    """Real NYSE sessions from before registration through ``last``."""
    from datetime import date, timedelta

    from paper_trader.engine import exchange_calendar as EC
    out, cur = [], date(2026, 9, 1)
    end = date.fromisoformat(last)
    while cur <= end:
        if EC.is_supported(cur) and not EC.is_non_session(cur):
            out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def panel(last, *, missing=(), tickers=TICKERS):
    ss = sessions_upto(last)
    return {"series": {tk: {"dates": list(ss),
                            "adj": [100.0 + 0.5 * (i + k)
                                    for i in range(len(ss))]}
                       for k, tk in enumerate(tickers) if tk not in missing}}


def arm(stores, challenger_id=CHALLENGER, **kw):
    """Register one challenger and declare its producer policy."""
    write_frozen_decision(stores["r58"], challenger_id, **kw)
    reg = register(challenger_id)
    R58R.declare_policies(now="2026-09-09T12:00:00+00:00")
    return reg


def freeze_for(challenger_id, session, weights, *, now, info=None):
    """Drive the producer's freeze with an INJECTED book (see module docstring)."""
    reg = [r for r in R58R.registrations()
           if r.get("challenger_id") == challenger_id][0]
    return R58R._freeze(
        reg, boundary=session,
        information_session=info or "2026-10-08",
        weights=dict(weights),
        scored={"universe_session": info or "2026-10-08",
                "universe_identity_hash": "uni_%s" % session, "n_eligible": 500},
        now=now)


def accrue(*, today, panel_last, execute=True):
    return CFA.advance_canonical_forward_accrual(
        price_panel=panel(panel_last), today=today, execute=execute)


def row(out, challenger_id=CHALLENGER):
    return [r for r in out["challengers"]
            if r["challenger_id"] == challenger_id][0]


# =========================================================================== #
# 1. THE BOUNDARY - the registrar's own grid, on the registrar's own calendar
# =========================================================================== #
def test_the_cadence_grid_is_the_registrars_own(stores):
    """The 2nd and 3rd boundaries are 21 and 42 eligible sessions from the 1st.

    Asserted against the authoritative calendar rather than hard-coded, so this
    test fails first if the calendar or the cadence ever moves.
    """
    reg = arm(stores)
    assert CFA.first_decision_session(reg) == FIRST_DECISION
    b2 = CFA.cadence_boundary_after(reg, cadence_sessions=21,
                                    after=FIRST_DECISION)
    b3 = CFA.cadence_boundary_after(reg, cadence_sessions=21, after=b2)
    assert b2 == SECOND_DECISION, b2
    assert b3 == THIRD_DECISION, b3
    ss = sessions_upto(b3)
    assert ss.index(b2) - ss.index(FIRST_DECISION) == 21
    assert ss.index(b3) - ss.index(b2) == 21


def test_the_producer_reports_the_next_boundary_and_does_not_freeze_early(stores):
    """Armed is a state, not an emission. The first boundary needs no decision."""
    reg = arm(stores)
    nb = R58R.next_boundary(reg, now="2026-09-23T19:00:00+00:00")
    # The FIRST boundary is governed by the book adopted at registration, so the
    # producer must not offer to decide it - that would be a second book for a
    # session whose book already exists.
    assert nb["boundary"] == SECOND_DECISION
    assert nb["window_state"] == "EMISSION_WINDOW_OPEN"
    assert nb["missed_boundaries"] == []
    assert PD.load_decision(CHALLENGER, FIRST_DECISION,
                            root=R58R.decision_root()) is None


def test_the_emission_boundary_is_not_loosened_by_declaring_it(stores):
    """Declaring PRIOR_SESSION_ONLY resolves to what an ABSENT policy resolved to.

    The whole safety argument for continuing these four identities rests on
    this: the producer declares the rule they were always judged by, and
    declaring it changes nothing.
    """
    reg = arm(stores)
    declared = CFA.emission_policy(reg)
    from paper_trader.engine import forward_emission_window as EW
    absent = EW.normalise_policy(None)
    assert declared["boundary"] == EW.BOUNDARY_PRIOR_SESSION
    assert (EW.window_bounds(declared, SECOND_DECISION)["closes_at"]
            == EW.window_bounds(absent, SECOND_DECISION)["closes_at"])
    # And the execution contract must not move the decision session.
    assert CFA.execution_offset_sessions(reg)["offset_sessions"] == 0


# =========================================================================== #
# 2. THE CHAIN - first, second and third successive decisions
# =========================================================================== #
def test_the_first_decision_emits_from_the_adopted_book(stores):
    reg = arm(stores)
    out = accrue(today="2026-09-09", panel_last="2026-09-09")
    r = row(out)
    assert r["predictions_emitted"] == 1
    assert r["last_emission_session"] == FIRST_DECISION
    assert r["matured_observations"] == 0


def test_the_second_and_third_cadence_decisions_accrue(stores):
    """The repair, stated as one test: a second and third observation exist."""
    arm(stores)
    accrue(today="2026-09-09", panel_last="2026-09-09")

    # Boundary 2: the producer freezes BEFORE the session, the accrual emits.
    res = freeze_for(CHALLENGER, SECOND_DECISION, BOOK_2,
                     now="2026-10-08T20:00:00+00:00", info="2026-10-08")
    assert res["state"] == R58R.ST_FROZEN, res
    out = accrue(today="2026-10-08", panel_last="2026-10-08")
    r = row(out)
    assert r["predictions_emitted"] == 2, r
    assert r["last_emission_session"] == SECOND_DECISION

    # Boundary 3.
    res = freeze_for(CHALLENGER, THIRD_DECISION, BOOK_3,
                     now="2026-11-06T20:00:00+00:00", info="2026-11-06")
    assert res["state"] == R58R.ST_FROZEN, res
    out = accrue(today="2026-11-06", panel_last="2026-11-06")
    r = row(out)
    assert r["predictions_emitted"] == 3, r
    assert r["last_emission_session"] == THIRD_DECISION
    # And the first one has MATURED on the way - genuine, not hindsight.
    assert r["matured_observations"] >= 1, r


def test_each_emission_carries_its_own_frozen_book(stores):
    """A per-session decision governs its own session, not the adopted book."""
    arm(stores)
    accrue(today="2026-09-09", panel_last="2026-09-09")
    freeze_for(CHALLENGER, SECOND_DECISION, BOOK_2,
               now="2026-10-08T20:00:00+00:00")
    accrue(today="2026-10-08", panel_last="2026-10-08")
    ident = identity()["identity_hash"]
    ems = {str(e["decision_session"]): e
           for e in CFA.load_emissions(ident)}
    assert set(ems) == {FIRST_DECISION, SECOND_DECISION}
    w1 = (ems[FIRST_DECISION].get("prediction") or {}).get("weights") or {}
    w2 = (ems[SECOND_DECISION].get("prediction") or {}).get("weights") or {}
    assert set(w1) == set(BOOK_1)
    assert set(w2) == set(BOOK_2)


def test_the_registrars_hash_binding_survives_a_per_session_decision(stores):
    """A per-session decision reports the ADOPTION freeze's hash, not its own.

    This is what keeps the registrar's integrity binding intact. If the decision
    reported its own hash the registration would refuse every book it produced.
    """
    reg = arm(stores)
    freeze_for(CHALLENGER, SECOND_DECISION, BOOK_2,
               now="2026-10-08T20:00:00+00:00")
    book = CFA.resolve_frozen_decision(reg, SECOND_DECISION)
    assert book["resolved"] is True, book
    assert book["record_hash"] == RECORD_HASH
    assert book["decision_record_hash"] != RECORD_HASH
    assert book["per_session_decision"] is True


def test_cost_and_benchmark_come_from_the_frozen_policy(stores):
    arm(stores, bps=12.5)
    freeze_for(CHALLENGER, SECOND_DECISION, BOOK_2,
               now="2026-10-08T20:00:00+00:00")
    rec = PD.load_decision(CHALLENGER, SECOND_DECISION,
                           root=R58R.decision_root())
    assert rec["cost_policy"]["bps_per_side"] == 12.5
    assert rec["evaluation_horizon_sessions"] == 21
    assert rec["rebalance_cadence_sessions"] == 21


# =========================================================================== #
# 3. THE REFUSALS
# =========================================================================== #
def test_a_duplicate_invocation_is_idempotent(stores):
    arm(stores)
    a = freeze_for(CHALLENGER, SECOND_DECISION, BOOK_2,
                   now="2026-10-08T20:00:00+00:00")
    b = freeze_for(CHALLENGER, SECOND_DECISION, BOOK_2,
                   now="2026-10-08T20:05:00+00:00")
    assert a["state"] == R58R.ST_FROZEN
    assert b["state"] == R58R.ST_ALREADY_FROZEN
    assert a["decision_identity_hash"] == b["decision_identity_hash"]


def test_a_conflicting_second_freeze_is_refused_and_the_held_record_stands(stores):
    """First write wins. A different book for the same session never overwrites."""
    arm(stores)
    freeze_for(CHALLENGER, SECOND_DECISION, BOOK_2,
               now="2026-10-08T20:00:00+00:00")
    bad = freeze_for(CHALLENGER, SECOND_DECISION, BOOK_3,
                     now="2026-10-08T21:00:00+00:00")
    assert bad["state"] == R58R.ST_BLOCKED
    assert bad["freeze_outcome"] == PD.REFUSED_CONFLICT
    held = PD.load_decision(CHALLENGER, SECOND_DECISION,
                            root=R58R.decision_root())
    assert set(held["weights"]) == set(BOOK_2)


def test_a_crash_before_the_accrual_recovers_on_the_next_run(stores):
    """The decision is on disk; a later run emits it. No state lives in memory."""
    arm(stores)
    accrue(today="2026-09-09", panel_last="2026-09-09")
    freeze_for(CHALLENGER, SECOND_DECISION, BOOK_2,
               now="2026-10-08T20:00:00+00:00")
    # The "crash": the accrual never ran on the freeze day. It runs later, while
    # the window is still open, and the observation is not lost.
    out = accrue(today="2026-10-08", panel_last="2026-10-08")
    assert row(out)["predictions_emitted"] == 2


def test_a_freeze_after_the_window_shut_is_refused_and_never_backfilled(stores):
    arm(stores)
    late = freeze_for(CHALLENGER, SECOND_DECISION, BOOK_2,
                      now="2026-10-09T13:00:00+00:00")
    assert late["state"] == R58R.ST_MISSED
    assert late["freeze_outcome"] == PD.REFUSED_TOO_LATE
    assert PD.load_decision(CHALLENGER, SECOND_DECISION,
                            root=R58R.decision_root()) is None


def test_a_missed_boundary_is_reported_not_repaired(stores):
    """A boundary that passed undecided is MISSED, and the producer moves on."""
    reg = arm(stores)
    nb = R58R.next_boundary(reg, now="2026-10-20T12:00:00+00:00")
    assert [m["decision_session"] for m in nb["missed_boundaries"]] \
        == [SECOND_DECISION]
    assert nb["boundary"] == THIRD_DECISION


def test_a_missing_original_freeze_declares_no_policy(stores):
    """No adoption record, no policy. A producer never invents an identity."""
    register(CHALLENGER)            # registered, but the R58 artifact is absent
    res = R58R.declare_policies(now="2026-09-09T12:00:00+00:00")
    assert res[CHALLENGER]["outcome"] == "REFUSED_NO_FROZEN_RECORD"
    assert PD.load_policy(CHALLENGER, root=R58R.decision_root()) is None


def test_a_changed_original_freeze_blocks_the_accrual_rather_than_repairing_it(stores):
    """A book whose hash is not the registered one is refused, never repaired."""
    reg = arm(stores)
    write_frozen_decision(stores["r58"], CHALLENGER, record_hash="0" * 64)
    book = CFA.resolve_frozen_decision(reg, None)
    assert book["resolved"] is False
    assert book["reason"] == CFA.INTEGRITY_HASH_MISMATCH


def test_missing_provider_data_blocks_rather_than_scores(stores, monkeypatch):
    """A panel that has not caught up is a WAIT, never an older decision."""
    arm(stores)
    monkeypatch.setattr(R58R, "forward_panel_last_session", lambda: "2026-09-22")
    out = R58R.advance(now="2026-10-08T20:00:00+00:00", allow_refresh=False)
    assert out["state"] in (R58R.ST_AWAITING_DATA, R58R.ST_BEFORE_LEAD), out
    assert not any(r.get("frozen") for r in out["challengers"])


def test_an_absent_panel_never_scores(stores, monkeypatch):
    arm(stores)
    monkeypatch.setattr(R58R, "forward_panel_last_session", lambda: None)
    out = R58R.advance(now="2026-10-08T20:00:00+00:00", allow_refresh=False)
    assert out["state"] == R58R.ST_AWAITING_DATA, out
    assert out["challengers"][0]["blocked_on"] == "FORWARD_PANEL_ABSENT"


def test_a_panel_that_holds_the_decision_session_is_refused(stores, monkeypatch):
    """Scoring on the very close the book is entered at is look-ahead."""
    arm(stores)
    monkeypatch.setattr(R58R, "forward_panel_last_session",
                        lambda: SECOND_DECISION)
    out = R58R.advance(now="2026-10-08T20:00:00+00:00", allow_refresh=False)
    assert out["state"] == R58R.ST_BLOCKED, out
    assert out["challengers"][0]["blocked_on"] == "PANEL_HOLDS_THE_DECISION_SESSION"


def test_an_empty_book_is_refused_rather_than_frozen(stores):
    arm(stores)
    res = freeze_for(CHALLENGER, SECOND_DECISION, {},
                     now="2026-10-08T20:00:00+00:00")
    assert res["state"] == R58R.ST_BLOCKED
    assert res["freeze_outcome"] == PD.REFUSED_MALFORMED


def test_no_decision_is_ever_frozen_for_a_past_session(stores):
    """Every session before the producer existed stays undecided, forever."""
    arm(stores)
    for past in ("2026-09-04", "2026-09-08", FIRST_DECISION, "2026-09-15"):
        assert PD.load_decision(CHALLENGER, past,
                                root=R58R.decision_root()) is None


# =========================================================================== #
# 4. ALL FOUR STRATEGIES, NOT JUST ONE
# =========================================================================== #
@pytest.mark.parametrize("cid", CHALLENGERS)
def test_every_affected_strategy_has_the_whole_chain(stores, cid):
    """The chain, proved separately for each of the four R58 registrations."""
    arm(stores, cid)
    reg = [r for r in R58R.registrations() if r["challenger_id"] == cid][0]

    # registered specification -> eligible boundary
    nb = R58R.next_boundary(reg, now="2026-09-23T19:00:00+00:00")
    assert nb["boundary"] == SECOND_DECISION

    # -> immutable frozen decision
    res = freeze_for(cid, SECOND_DECISION, BOOK_2,
                     now="2026-10-08T20:00:00+00:00")
    assert res["state"] == R58R.ST_FROZEN, res

    # -> canonical evidence emission -> pending observation
    out = CFA.advance_canonical_forward_accrual(
        price_panel=panel("2026-10-08"), today="2026-10-08", execute=True)
    r = row(out, cid)
    assert r["predictions_emitted"] >= 1, r
    assert r["pending_observations"] >= 1

    # -> genuine future maturation, and NOT before its horizon
    assert r["matured_observations"] == 0
    later = CFA.advance_canonical_forward_accrual(
        price_panel=panel("2026-11-20"), today="2026-11-20", execute=True)
    assert row(later, cid)["matured_observations"] >= 1


# =========================================================================== #
# 5. THE INVARIANT THAT MAKES A SILENT ORPHAN IMPOSSIBLE
# =========================================================================== #
def test_every_declared_producer_stage_exists_in_the_runtime():
    """A renamed or deleted stage fails the build, not the forward book."""
    import inspect
    src = inspect.getsource(R52RT.research_runtime_cycle)
    for stage in FPH.RUNTIME_PRODUCER_STAGES:
        assert '"%s"' % stage in src, (
            "%s is declared a producer stage and does not appear in the runtime"
            % stage)


def test_the_r58_stage_runs_before_the_accrual_stage():
    """A decision frozen this cycle must be scored in the SAME cycle."""
    import inspect
    src = inspect.getsource(R52RT.research_runtime_cycle)
    assert (src.index('"r58_cadence_prospective_decision"')
            < src.index('"canonical_forward_accrual"'))


def test_a_registration_alone_is_never_described_as_accruing():
    """ACCRUING is unreachable without a live producer, by construction."""
    orphan = {"challenger_id": "SOMETHING_NOBODY_DECLARED",
              "predictions_emitted": 5, "matured_observations": 2}
    st = FPH.lifecycle_state(orphan)
    assert st["lifecycle"] == FPH.L_NOT_ARMED
    assert st["is_a_defect"] is True
    assert st["reason"] == FPH.UNDECLARED


def test_an_undeclared_registration_is_a_failure_not_a_default():
    assert FPH.producer_for("A_NAME_NOBODY_DECLARED")["is_a_defect"] is True
    assert FPH.producer_for("R58_FCF_PURE_V1")["is_a_defect"] is False


def test_the_producer_lifecycle_separates_the_eight_states():
    """Each state is reachable and none collapses into another."""
    base = {"challenger_id": "R58_FCF_PURE_V1", "predictions_emitted": 1}
    live = {"last_stage_state": "SUCCESS"}
    assert FPH.lifecycle_state(base, beat=live)["lifecycle"] == FPH.L_ACCRUING
    assert FPH.lifecycle_state({**base, "predictions_emitted": 0},
                               beat=live)["lifecycle"] == FPH.L_ARMED
    assert FPH.lifecycle_state(
        base, beat={"last_stage_state": "DATA_BLOCKED"}
    )["lifecycle"] == FPH.L_AWAITING_DATA
    assert FPH.lifecycle_state(
        base, beat={"last_stage_state": "FORFEITED"}
    )["lifecycle"] == FPH.L_MISSED_GAP
    assert FPH.lifecycle_state(
        base, beat={"last_stage_state": "FAILED_RETRYABLE"}
    )["lifecycle"] == FPH.L_PRODUCER_FAILED
    assert FPH.lifecycle_state(
        {"challenger_id": "REVERSED_SPY_PUT_CALL_SKEW_H5"}
    )["lifecycle"] == FPH.L_SUPERSEDED
    assert FPH.lifecycle_state(
        {**base, "lifecycle_state": "RETIRED"}, beat=live
    )["lifecycle"] == FPH.L_RETIRED


def test_the_heartbeat_reports_every_declared_stage():
    beat = FPH.heartbeat(runs={"runs": [{
        "run_id": "r1", "started_utc": "2026-09-23T14:50:30Z",
        "stages": [{"stage": "r58_cadence_prospective_decision",
                    "state": "NOT_DUE", "advance_state": "R58_ARMED"}]}]})
    assert set(beat["stages"]) == set(FPH.RUNTIME_PRODUCER_STAGES)
    assert beat["stages"]["r58_cadence_prospective_decision"]["ran"] is True
    # A declared stage that has never run is reported as such, not omitted.
    assert beat["stages"]["fx_carry_cadence_prospective_decision"]["ran"] is False


def test_r67_and_the_health_owner_read_ONE_producer_declaration():
    """Two lists that can disagree is how the orphan survived. There is one."""
    assert R67FP.RUNTIME_PRODUCER_STAGES is FPH.RUNTIME_PRODUCER_STAGES
    assert R67FP.KNOWN_UNPRODUCED is FPH.KNOWN_UNPRODUCED


def test_r67s_four_orphans_now_have_an_executable_path():
    """The R67 finding, retested as the SUCCESSFUL state rather than the defect.

    R67's suite asserted these four had NO producer. That assertion was true and
    is now the wrong test: it would pass again the day somebody unwired them.
    """
    for cid in CHALLENGERS:
        p = FPH.producer_for(cid)
        assert p["producer_state"] == FPH.P_LIVE, (cid, p)
        assert p["producer_stage"] == "r58_cadence_prospective_decision"
        assert p["producer_owner"] == "alpha_agent.r68.r58_cadence_runtime"
        assert p["is_a_defect"] is False
        assert cid not in FPH.KNOWN_UNPRODUCED


def test_the_capital_floor_is_now_reachable_for_all_four():
    """R67 computed UNREACHABLE_NO_CADENCE_PRODUCER for these four. Not any more."""
    for cid in CHALLENGERS:
        out = R67FP.time_to_capital_floor(
            {"challenger_id": cid, "cadence_sessions": 21,
             "horizon_sessions": 21, "matured_observations": 0})
        assert out["floor_verdict"] == "REACHABLE_ON_CADENCE", (cid, out)
        assert out["floor_reachable"] is True
        assert out["sessions_to_floor"] == 60 * 21 + 21


# =========================================================================== #
# 6. SAFETY - what this release may not do
# =========================================================================== #
FORBIDDEN = ("create_order", "create_fill", "submit_order", "place_order",
             "promote_champion", "promote_model", "activate_sleeve",
             "allocate_capital", "approve_proposal", "run_daily_close")


@pytest.mark.parametrize("rel", [
    "alpha_agent/r68/r58_cadence_runtime.py",
    "alpha_agent/r68/__init__.py",
    "api/forward_producer_health.py",
    "scripts/run_r58_forward_panel_refresh.py",
])
def test_no_execution_or_promotion_path(rel):
    src = (REPO / rel).read_text(encoding="utf-8")
    assert not [t for t in FORBIDDEN if t + "(" in src], rel


def test_the_producer_never_writes_the_frozen_research_panel():
    """The R57 panel is the frozen substrate of every settled result."""
    src = (REPO / "scripts/run_r58_forward_panel_refresh.py").read_text(
        encoding="utf-8")
    assert "DEFAULT_RESEARCH_ROOT" in src and "refuses" in src
    rt = (REPO / "alpha_agent/r68/r58_cadence_runtime.py").read_text(
        encoding="utf-8")
    assert "env[R57_ROOT_ENV] = str(panel_root())" in rt


def test_the_producer_contains_no_signal_implementation():
    """Every score is the originating owner's. No formula lives here."""
    src = (REPO / "alpha_agent/r68/r58_cadence_runtime.py").read_text(
        encoding="utf-8")
    assert "CH.build(" in src
    for tok in ("def xs_rank", "def xs_z", "np.argsort", "nanmedian",
                "def _book_from_scores"):
        assert tok not in src, tok


def test_the_activation_decision_is_recorded_with_its_reversal(stores):
    arm(stores)
    d = R58R.activation_decision()
    assert d["DECISION"] == "CONTINUE_THE_EXISTING_FOUR_IDENTITIES"
    assert len(d["why_continuation_is_faithful"]) >= 5
    assert "_PER_SESSION_DECISION_ROOTS" in d["how_to_reverse_this_in_one_step"]
    assert len(d["registrations"]) == 1
    assert d["registrations"][0]["frozen_construction_declares_cadence"] == 21


# =========================================================================== #
# 7. THE LIVE SCORER - real vendor data, skipped when it is absent
# =========================================================================== #
@pytest.mark.filterwarnings(
    # r57.engine.eligibility takes a nanmedian of 63 sessions of dollar volume
    # per symbol. A name that was already delisted at the decision index has no
    # volume in that window, so the slice is all-NaN and numpy says so. The very
    # next line is ``ok &= np.isfinite(med) & ...``, which excludes exactly those
    # names - the warning is the eligibility rule working. It is ignored HERE
    # and nowhere else, so the same warning anywhere new still fails the suite.
    "ignore:All-NaN slice encountered:RuntimeWarning")
def test_the_live_scorer_produces_a_book_for_every_challenger():
    """The one part the hermetic chain tests inject, proved for real.

    Skipped rather than faked when the forward panel has not been built, because
    a test that silently passes on absent data is how a stale panel goes
    unnoticed for eight days.
    """
    last = R58R.forward_panel_last_session()
    if not last:
        pytest.skip("the R58 forward panel has not been built on this machine")
    scored = R58R.score_books(last)
    assert scored["n_eligible"] > 100, scored["n_eligible"]
    for cid in CHALLENGERS:
        book = scored["books"][cid]
        assert book["n_held"] > 0, cid
        assert abs(sum(book["weights"].values()) - 1.0) < 1e-6, cid
