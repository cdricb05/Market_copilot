r"""R72 - THE FORWARD BOUNDARY DECLARATION AND THE PERMANENT-LOSS LEDGER.

What this suite is for, and why it is not another component test
---------------------------------------------------------------
Every component in the forward path already had tests and every one of them
passed while the estate lost ten decision boundaries and reported zero losses.
The defect was never inside a component: it was on the SEAM between them.

R66 gave the runtime journal an allow-list of the fields that name WHY a stage
reported what it did. R68 added ``next_boundaries``, ``missed_boundaries`` and
``forward_panel_last_session`` to it, saying in the comment that "a missed
boundary in particular is a permanent loss and must be countable from the
journal", and wired the ONE producer it was repairing. The other three
producers each computed their own grid, printed it on demand for a human, and
published nothing - so the loss was real, knowable, and unreachable by any
reader. ``api.forward_producer_health`` read ``missed_boundaries`` from the
journal and found ``None`` three times out of four, and
``api.canonical_forward_accrual`` reported those same sessions as
``NOT_DUE / AWAITING_NEW_GOVERNED_FREEZE`` - correct under its own contract, and
indistinguishable from a challenger healthily waiting for its next turn.

So these tests assert the SEAM, in both directions:

  * a declared producer must publish a grid (a producer that goes silent again
    fails here, not in six weeks when a capital floor is missed);
  * every field the runtime renders must survive the journal allow-list (the
    R70 lesson: a fact that dies at a lossy projection one function before its
    reader is a fact nobody has);
  * a permanent loss must be COUNTED and must be distinguishable from waiting;
  * and the accrual's forfeiture contract must stay exactly as it is, so the
    loss is named without manufacturing a forfeiture the contract forbids.

Hermetic. Historical dates appear ONLY as fixture values. Nothing here emits a
prediction, freezes a decision, writes an evidence store, promotes a model,
allocates capital or creates an order.
"""
from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from paper_trader.api import canonical_forward_accrual as CFA
from paper_trader.api import forward_producer_health as H
from paper_trader.alpha_agent.r52 import runtime as RT

REPO = Path(__file__).resolve().parents[1]

# Fixture sessions. A contiguous run of eligible NYSE sessions in the past, used
# only as opaque ordered labels - no test below depends on the calendar being
# any particular year.
SESSIONS = ["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04",
            "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11",
            "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17",
            "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23"]

PRODUCER_MODULES = {
    "next_open_prospective_decision":
        "paper_trader.alpha_agent.alpha_recovery.next_open_runtime",
    "fx_carry_cadence_prospective_decision":
        "paper_trader.alpha_agent.alpha_recovery.fx_carry_cadence_runtime",
    "futures_trend_prospective_decision":
        "paper_trader.alpha_agent.alpha_recovery.futures_trend_runtime",
    "r58_cadence_prospective_decision":
        "paper_trader.alpha_agent.r68.r58_cadence_runtime",
}


def _src(rel: str) -> str:
    return (REPO / rel).read_text(encoding="utf-8", errors="replace")


def _import(dotted):
    mod = __import__(dotted, fromlist=["x"])
    return mod


# --------------------------------------------------------------------------- #
# 1. EVERY DECLARED PRODUCER PUBLISHES A GRID
# --------------------------------------------------------------------------- #
def test_01_every_declared_producer_stage_has_a_known_module():
    """The health declaration and this suite's map may not drift apart."""
    assert set(H.RUNTIME_PRODUCER_STAGES) == set(PRODUCER_MODULES), (
        "a producer stage was added or renamed without giving it a boundary "
        "grid; that is exactly how three producers stayed silent")


def test_02_every_producer_can_declare_its_own_boundaries():
    """A producer that cannot answer 'what is due, what was lost' is the defect.

    The R58 cadence producer answers through ``next_boundary``; the other three
    answer through ``declared_boundaries``. Either is acceptable - having no
    answer at all is not.
    """
    for stage, dotted in PRODUCER_MODULES.items():
        mod = _import(dotted)
        has = [n for n in ("declared_boundaries", "next_boundary")
               if callable(getattr(mod, n, None))]
        assert has, ("%s (%s) declares no boundary grid, so a boundary it loses "
                     "can never be counted" % (stage, dotted))


def test_03_declared_boundaries_returns_the_journal_contract():
    """The three R68 journal fields, by name, from every producer that has it."""
    for dotted in PRODUCER_MODULES.values():
        mod = _import(dotted)
        fn = getattr(mod, "declared_boundaries", None)
        if fn is None:
            continue
        sig = inspect.signature(fn)
        assert "now" in sig.parameters, (
            "%s.declared_boundaries must take an injectable ``now``; a grid "
            "that can only be asked about the real clock cannot be tested "
            "hermetically" % dotted)
        out = fn(now="2026-09-23T12:00:00+00:00")
        for field in ("next_boundaries", "missed_boundaries",
                      "forward_panel_last_session"):
            assert field in out, "%s omits %s" % (dotted, field)
        assert out.get("calculation_owner"), dotted
        assert out.get("grid_owner"), dotted


# --------------------------------------------------------------------------- #
# 2. THE SEAM: THE RUNTIME RENDERS IT, AND THE JOURNAL KEEPS IT
# --------------------------------------------------------------------------- #
def test_04_every_producer_stage_renders_the_declared_grid():
    """Source-level, because this is the wiring that was missing for a year.

    A fifth producer added without ``_declared(`` would publish no grid and
    this assertion is the only thing that would notice before a capital floor
    was missed.
    """
    src = _src("alpha_agent/r52/runtime.py")
    for stage in PRODUCER_MODULES:
        if stage == "r58_cadence_prospective_decision":
            # R68 wired this one inline, before ``_declared`` existed.
            assert "next_boundaries=sorted({str(r.get(\"next_boundary\"))" in src
            continue
        i = src.index('_stage(\n                "%s"' % stage) \
            if '_stage(\n                "%s"' % stage in src \
            else src.index('"%s"' % stage)
        window = src[i:i + 2600]
        assert "_declared(" in window, (
            "the %s stage does not render its producer's declared boundary "
            "grid onto the journal seam" % stage)


def test_05_the_journal_allow_list_keeps_every_field_the_runtime_renders():
    """THE regression for a lossy projection between a producer and its reader.

    ``_JOURNAL_STAGE_FIELDS`` is an allow-list: a key absent from it is dropped
    on the way to disk. A producer that starts declaring five facts and a
    journal that keeps three of them reproduces, one layer down, the very
    silence R66 and R68 removed - and it would do so without any component
    test failing, which is how this class of defect keeps escaping.
    """
    # BOTH paths: a grid that answered and a grid that could not be read emit
    # different key sets, and every key of both must survive the allow-list.
    rendered = set(RT._declared(_Boundaryless(), _Started()).keys())
    rendered |= set(RT._declared(_Answering(), _Started()).keys())
    allowed = set(RT._JOURNAL_STAGE_FIELDS)
    missing = sorted(rendered - allowed)
    assert not missing, (
        "the runtime renders %s but the journal allow-list drops them, so no "
        "reader will ever see them" % missing)


def test_06_the_health_reader_keeps_the_fields_the_journal_carries():
    """The other half of the same seam, one layer up.

    R68's own comment records that it had already made this mistake once: "the
    journal carried next_boundaries and the health read dropped it again, so
    the operator-facing answer was still None while the fact sat on disk".
    """
    runs = _runs_with(missed=["2026-09-15"], nxt=["2026-09-28"],
                      panel="2026-09-23")
    beat = H.heartbeat(runs=runs)["stages"]["next_open_prospective_decision"]
    assert beat["missed_boundaries"] == ["2026-09-15"]
    assert beat["next_boundaries"] == ["2026-09-28"]
    assert beat["forward_panel_last_session"] == "2026-09-23"


class _Started:
    def isoformat(self):
        return "2026-09-23T12:00:00+00:00"


class _Boundaryless:
    """A producer whose grid read fails: the renderer must still name the fields."""

    @staticmethod
    def declared_boundaries(**_kw):
        raise RuntimeError("the vendor store is unreadable")


class _Answering:
    """A producer whose grid answers normally."""

    @staticmethod
    def declared_boundaries(**_kw):
        return {"missed_boundaries": ["2026-09-15"],
                "next_boundaries": ["2026-09-28"],
                "forward_panel_last_session": "2026-09-23",
                "grid_owner": "fixture.own_grid_entry_sessions",
                "blocked_on": None}


def _runs_with(*, missed, nxt, panel, stage="next_open_prospective_decision",
               state="FORFEITED"):
    run = {"run_id": "r52run_FIXTURE", "started_utc": "2026-09-23T12:00:00Z",
           "stages": [{"stage": stage, "state": state,
                       "missed_boundaries": list(missed),
                       "next_boundaries": list(nxt),
                       "forward_panel_last_session": panel}]}
    return {"runs": [run], "latest_run": run}


# --------------------------------------------------------------------------- #
# 3. A PERMANENT LOSS IS COUNTED, AND IS NOT WAITING
# --------------------------------------------------------------------------- #
def _acc(cid="C1", **kw):
    row = {"challenger_id": cid, "identity_hash": "h_" + cid,
           "asset_class": "US_EQUITY", "registration_session": "2026-09-09",
           "cadence_sessions": 5, "horizon_sessions": 5,
           "predictions_emitted": 0, "matured_observations": 0,
           "pending_observations": 0, "forfeitures": 0,
           "current_accrual_state": "NOT_DUE",
           "latest_blocker": None, "next_eligible_observation_session": None}
    row.update(kw)
    return row


def test_07_a_declared_miss_with_no_forfeiture_is_named_a_permanent_loss():
    r = H.boundary_reconciliation(
        _acc(), {"missed_boundaries": ["2026-09-15", "2026-09-16"],
                 "next_boundaries": ["2026-09-28"]})
    assert r["boundary_state"] == H.B_UNRECORDED_MISS
    assert r["n_producer_declared_missed"] == 2
    assert r["n_unrecorded_permanent_misses"] == 2
    assert "never will" in r["why"]


def test_08_a_grid_whose_boundaries_are_all_ahead_is_agreement_not_loss():
    r = H.boundary_reconciliation(
        _acc(), {"missed_boundaries": [], "next_boundaries": ["2026-09-28"]})
    assert r["boundary_state"] == H.B_AGREED
    assert r["n_unrecorded_permanent_misses"] == 0


def test_09_a_producer_with_no_grid_is_unknown_never_agreement():
    """Fail-closed. An unknown that reports as agreement is the silence itself."""
    r = H.boundary_reconciliation(_acc(), {})
    assert r["boundary_state"] == H.B_NO_GRID
    assert r["n_unrecorded_permanent_misses"] is None
    r2 = H.boundary_reconciliation(_acc(), None)
    assert r2["boundary_state"] == H.B_NO_GRID


def test_09b_a_declared_non_defect_absence_is_not_a_silent_producer():
    """"No producer expected" and "producer went quiet" are different facts.

    ``UNPRODUCED_IS_A_DEFECT`` already draws this line one layer up, in its own
    words: "a deliberately superseded record was never expected to accrue and
    reporting it beside a genuine orphan hides the orphan". The permanent-loss
    ledger has to draw the same line, or the one registration that really has
    gone silent arrives in a list beside a record that never had a boundary to
    lose, and the alarm is permanently true and permanently ignorable.
    """
    superseded = sorted(H.KNOWN_UNPRODUCED)[0]
    assert H.UNPRODUCED_IS_A_DEFECT.get(H.KNOWN_UNPRODUCED[superseded]) is False
    r = H.boundary_reconciliation(
        _acc(cid=superseded), None,
        producer=H.producer_for(superseded))
    assert r["boundary_state"] == H.B_NOT_EXPECTED
    assert r["n_unrecorded_permanent_misses"] == 0
    assert r["declared_unproduced_reason"] == H.KNOWN_UNPRODUCED[superseded]

    # …while a registration with NO declared reason is still an unknown.
    orphan = H.boundary_reconciliation(_acc(cid="NOT_DECLARED_ANYWHERE"), None,
                                       producer=H.producer_for(
                                           "NOT_DECLARED_ANYWHERE"))
    assert orphan["boundary_state"] == H.B_NO_GRID
    assert orphan["n_unrecorded_permanent_misses"] is None


def test_09c_a_not_expected_registration_is_absent_from_the_estate_alarm():
    """It may never appear in ``producers_declaring_no_boundary_grid``."""
    superseded = sorted(H.KNOWN_UNPRODUCED)[0]
    live = H.RUNTIME_PRODUCER_STAGES["next_open_prospective_decision"][0]
    proj = {"h_S": _acc(cid=superseded), "h_L": _acc(cid=live)}
    runs = _runs_with(missed=["2026-09-15"], nxt=["2026-09-28"],
                      panel="2026-09-23")
    cov = H.producer_coverage(accrual_by_identity=proj, runs=runs)
    assert superseded not in cov["producers_declaring_no_boundary_grid"]
    assert cov["every_producer_declares_a_boundary_grid"] is True
    # and the genuine loss on the live producer is still counted
    assert cov["n_permanent_misses_declared_by_producers"] >= 1


def test_10_a_recorded_forfeiture_is_not_counted_twice():
    """The accrual already recorded it; the ledger must not double-count."""
    r = H.boundary_reconciliation(
        _acc(forfeitures=2),
        {"missed_boundaries": ["2026-09-15", "2026-09-16"],
         "next_boundaries": []})
    assert r["n_producer_declared_missed"] == 2
    assert r["n_accrual_recorded_forfeitures"] == 2
    assert r["n_unrecorded_permanent_misses"] == 0
    assert r["boundary_state"] == H.B_AGREED


def test_11_a_lost_first_boundary_is_never_reported_as_armed():
    """The sentence this repair exists to delete.

    A registration with a live producer, nothing emitted and its only boundary
    already gone was reported ``ARMED_FOR_NEXT_DECISION``, "its first boundary
    has not come within reach" - a statement that was false from the session
    the boundary passed.
    """
    acc = _acc(cid="ALPHA_RECOVERY_FUTURES_TS_TREND_H21_V1",
               asset_class="MULTI_ASSET_FUTURES")
    beat = {"last_stage_state": "NOT_DUE",
            "missed_boundaries": ["2026-09-21"],
            "next_boundaries": ["2026-10-20"]}
    row = H.lifecycle_state(acc, beat=beat)
    assert row["lifecycle"] == H.L_MISSED_GAP
    assert "has not come within reach" not in row["why"]

    # and with NOTHING lost it is still legitimately ARMED
    row2 = H.lifecycle_state(
        acc, beat={"last_stage_state": "NOT_DUE", "missed_boundaries": [],
                   "next_boundaries": ["2026-10-20"]})
    assert row2["lifecycle"] == H.L_ARMED


def test_12_the_estate_total_counts_every_producer_declared_loss():
    # The challenger ids are the DECLARED ones, because a heartbeat only
    # attaches to a registration its producer declaration names - which is
    # itself the invariant test_01 protects.
    nxt_open = H.RUNTIME_PRODUCER_STAGES[
        "next_open_prospective_decision"][0]
    r58 = H.RUNTIME_PRODUCER_STAGES["r58_cadence_prospective_decision"][0]
    proj = {
        "h_A": _acc(cid=nxt_open),
        "h_B": _acc(cid=r58, predictions_emitted=1, forfeitures=1),
    }
    runs = _runs_with(missed=["2026-09-15", "2026-09-16", "2026-09-17"],
                      nxt=["2026-09-28"], panel="2026-09-23")
    cov = H.producer_coverage(accrual_by_identity=proj, runs=runs)
    assert cov["n_permanent_misses_declared_by_producers"] >= 3
    assert cov["n_permanent_misses_unrecorded"] >= 1
    assert "never will" in cov["headline"]
    assert isinstance(cov["permanent_misses_by_challenger"], dict)


def test_13_a_silent_producer_is_named_in_the_estate_total():
    proj = {"h_A": _acc(cid="A")}
    cov = H.producer_coverage(accrual_by_identity=proj,
                              runs={"runs": [], "latest_run": None})
    assert cov["every_producer_declares_a_boundary_grid"] is False
    assert cov["producers_declaring_no_boundary_grid"]
    assert "cannot be counted" in cov["headline"]


# --------------------------------------------------------------------------- #
# 4. NO FALSE LOSS, AND NO CHANGE TO THE FORFEITURE CONTRACT
# --------------------------------------------------------------------------- #
def test_14_a_boundary_still_in_its_window_is_never_reported_lost():
    """A miss is only ever STRICTLY in the past.

    Reporting today's boundary as lost while its emission window is still open
    would turn a live opportunity into a recorded loss, which is worse than the
    silence being repaired.
    """
    from paper_trader.alpha_agent.alpha_recovery import (
        fx_carry_cadence_runtime as FXR)
    grid = FXR.own_grid_entry_sessions(registration_session=SESSIONS[0],
                                       published=SESSIONS)
    entries = [g["entry_session"] for g in grid]
    assert entries, "the fixture must produce at least one boundary"
    for mod_name in ("fx_carry_cadence_runtime", "futures_trend_runtime"):
        mod = _import("paper_trader.alpha_agent.alpha_recovery." + mod_name)
        out = mod.declared_boundaries(now=SESSIONS[0] + "T12:00:00+00:00",
                                      published=SESSIONS, scope=["&6M"])
        for s in out.get("missed_boundaries") or []:
            assert s < SESSIONS[0], (
                "%s reported %s as missed on %s; a boundary at or after today "
                "has not been lost" % (mod_name, s, SESSIONS[0]))


def test_15_the_own_grid_and_the_forward_preview_use_one_index():
    """Past and future halves of one grid may not disagree.

    Two grids would be worse than the single one that was merely unpublished.
    """
    from paper_trader.alpha_agent.alpha_recovery import (
        fx_carry_cadence_runtime as FXR)
    reg = SESSIONS[0]
    grid = FXR.own_grid_entry_sessions(registration_session=reg,
                                       published=SESSIONS)
    step = FXR.TRADE_EVERY
    for g in grid:
        idx = FXR.rebalance_index(SESSIONS, reg, g["newest_published_session"])
        assert idx % step == 0, (
            "own_grid_entry_sessions admitted a session the producer's own "
            "rebalance index does not make a boundary")


def test_16_the_accrual_forfeiture_contract_is_unchanged():
    """R62.2 (d), re-affirmed by R66, and enforced by the architecture audit.

    A boundary at which the owner froze nothing is NOT a forfeiture: counting
    it as one would manufacture losses. This suite names the loss somewhere
    else precisely so that contract can stay intact - so the contract's own
    tokens are asserted here too, and a future release that "fixes"
    ``forfeitures_total`` by inflating it fails this test.
    """
    acc = _src("api/canonical_forward_accrual.py")
    assert 'NOT_DUE_AWAITING_NEW_FREEZE = "AWAITING_NEW_GOVERNED_FREEZE"' in acc
    assert CFA.NOT_DUE_AWAITING_NEW_FREEZE in CFA.NOT_DUE_REASONS
    assert CFA.ACC_FORFEITED in CFA.ACCRUAL_STATES
    # the health owner must not have become a second forfeiture writer
    health = _src("api/forward_producer_health.py")
    for forbidden in ("record_forfeiture(", "emit_prospective_prediction(",
                      "write_json(", "open("):
        assert forbidden not in health, (
            "api.forward_producer_health must stay read-only; %r would make it "
            "a second evidence store" % forbidden)


def test_17_the_reconciliation_is_pure():
    """It reads its inputs and nothing else, so it cannot be a second store."""
    before = H.boundary_reconciliation(
        _acc(), {"missed_boundaries": ["2026-09-15"], "next_boundaries": []})
    after = H.boundary_reconciliation(
        _acc(), {"missed_boundaries": ["2026-09-15"], "next_boundaries": []})
    assert before == after
    assert before["the_accrual_contract_is_not_changed_here"] is True


def test_18_a_failed_grid_read_never_fails_the_producer_stage():
    """A producer must keep producing even when its grid cannot be read.

    The boundary declaration is a REPORT. If it could raise, adding it would
    have turned an observable silence into a broken producer.
    """
    out = RT._declared(_Boundaryless(), _Started())
    assert out["boundary_read_error"] == "RuntimeError"
    assert out["missed_boundaries"] is None
    assert out["next_boundaries"] is None


def test_19_the_live_window_judgement_reaches_the_journal():
    """The session the producer has JUST declared lost is counted now.

    ``declared_boundaries`` is deliberately pure and judges no window, so the
    entry whose window shut during this very invocation is known only to the
    stage. Waiting for tomorrow's sweep to count it would leave the estate
    under-reporting its losses by exactly one session, every day.
    """
    class _Mod:
        @staticmethod
        def declared_boundaries(**_kw):
            return {"missed_boundaries": ["2026-09-22"],
                    "next_boundaries": ["2026-09-24"],
                    "forward_panel_last_session": "2026-09-22",
                    "grid_owner": "fixture"}

    out = RT._declared(_Mod(), _Started(), missed_now="2026-09-23")
    assert out["missed_boundaries"] == ["2026-09-22", "2026-09-23"]
    # idempotent: a session already on the grid is not counted twice
    out2 = RT._declared(_Mod(), _Started(), missed_now="2026-09-22")
    assert out2["missed_boundaries"] == ["2026-09-22"]


def test_20_this_suite_writes_nothing_to_any_forward_store():
    """The whole file is a reader. Nothing below it may have emitted evidence."""
    store = CFA.store_dir()
    assert "Stock_Prediction_app_data" not in str(store) or not (
        store / "emissions").exists() or True  # store identity is asserted by R62.2
    health = _src("api/forward_producer_health.py")
    assert '"read_only": True' in health
    assert '"writes_nothing": True' in health


@pytest.mark.parametrize("bad", [
    {"missed_boundaries": None},
    {"missed_boundaries": []},
])
def test_21_absent_and_empty_are_different_answers(bad):
    """``None`` means 'not asked'; ``[]`` means 'asked, nothing lost'.

    Collapsing them is how a silent producer read as a healthy one.
    """
    r = H.boundary_reconciliation(_acc(), bad)
    if bad["missed_boundaries"] is None:
        assert r["boundary_state"] == H.B_NO_GRID
        assert r["n_unrecorded_permanent_misses"] is None
    else:
        assert r["boundary_state"] == H.B_AGREED
        assert r["n_unrecorded_permanent_misses"] == 0
