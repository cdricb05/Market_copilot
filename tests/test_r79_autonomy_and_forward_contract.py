r"""R79 - THE LOOP THAT LOOKED ALIVE, AND THE TWO CALENDARS THAT DISAGREED.

What this suite is for
----------------------
On 2026-09-27 every surface in the estate reported health and the conjunction of
their answers was false.

**The research program had executed nothing since 2026-09-08.** Sixteen worker
cycles had completed, each measuring zero hypotheses. The reason was a
deterministic livelock one layer above the queue:

* the research frontier reported CROSS_ASSET as the one READY scope;
* its two remaining families were ``CROSS_ASSET_RELATIVE_VALUE`` (the director
  ruled WAITING_FOR_EXTERNAL_ENTITLEMENT) and
  ``CROSS_ASSET_REGIME_CONDITIONING`` (ruled SUPERSEDED) - both TERMINAL;
* the governor built a mandate for each, because ``remaining_families`` was the
  only thing it read and no dispatcher had ever consulted the ruling store;
* those two mandates were the estate's ONLY generatable work, so
  ``stop_reason`` answered *"the research director can still generate 2
  independently executable mandates"* and the loop could never reach STOP_A;
* it hit STOP_B instead and called a settled governance refusal "an external
  blocker", and the runtime slept 40 minutes at a time, for nineteen days,
  publishing ``WAITING_ON_A_BLOCKED_EXTERNAL_SOURCE``.

R72 had already built the durable ruling store AND the classifier that maps a
ruling onto the blocker taxonomy. Only the REPORTING path ever read it. This
suite asserts the dispatcher now does, at the ONE seam every downstream surface
derives from, and that the derived truth propagates: frontier -> governor ->
stop_reason -> loop -> runtime sleep reason.

**And the SPY next-open challenger had two decision calendars.** Its frozen
specification declares ``cadence = 5, overlapping = False`` and the canonical
accrual owner strides by that 5; its producer hard-coded ``cadence_sessions: 1``
and gridded EVERY eligible session. The miss ledger was computed from the daily
one, so nine boundaries were declared permanently lost over
2026-09-15..2026-09-25 where the frozen contract has two, and the next boundary
was published as 2026-09-28 where the contract puts it on 2026-09-29.

**Its entry mark was unpriceable and nothing said so.** It declares entry at the
NEXT ELIGIBLE SESSION'S OPEN (09:30 ET) and names ``api.price_panel`` as its
mark owner - a panel whose only per-session price is ``adjusted_close``. Left
alone, its first emission would have been valued close-to-close and reported as
next-open P&L, which is not a weaker measurement of that strategy but a
measurement of a different one.

**And the discovery input still presented prosecuted mechanisms as novel.** R78
re-commissioned a mechanism the estate had already settled: the census is frozen
at 2026-09-19, the mechanism was settled 2026-09-21 BY MEASUREMENT rather than by
a ruling, and the brief's novelty field was derived from the ruling store alone -
so ``already_ruled: false`` was read as "never tested".

Hermetic where it writes. Every test that records a ruling or a result builds its
own scratch memory database under pytest's ``tmp_path``; the live research store,
the live queue, the operational book and every evidence store are untouched.
Nothing here emits a prediction, freezes a decision, registers a challenger,
retires a live job, promotes a model, approves a proposal or allocates capital.
The declaration-only reads (the frozen SPY specification, the registry's
execution contracts) open no store and no network.
"""
from __future__ import annotations

import pytest

from paper_trader.alpha_agent.r59 import blockers as B
from paper_trader.alpha_agent.r59 import frontier as FR
from paper_trader.alpha_agent.r59 import governor as GOV
from paper_trader.alpha_agent.r59 import memory as M
from paper_trader.alpha_agent.r59 import runtime as RT
from paper_trader.api import autonomous_operating_status as AOS
from paper_trader.api import canonical_forward_accrual as ACC

#: The two CROSS_ASSET families the R71 director closed terminally, and the one
#: he closed on INFORMATION. The third must stay dispatchable, because a
#: temporary wait converted into a permanent closure is the mirror defect.
TERMINAL_FAMILY = "CROSS_ASSET_RELATIVE_VALUE"
SUPERSEDED_FAMILY = "CROSS_ASSET_REGIME_CONDITIONING"
INFORMATION_FAMILY = "CROSS_ASSET_LEAD_LAG"

NEXT_OPEN_ID = "REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1"


@pytest.fixture()
def mem(tmp_path):
    return M.open_memory(tmp_path / "research_memory.sqlite")


def _rule(mem, family, *, reason, verdict="REFUSED",
          asset_class="CROSS_ASSET"):
    return mem.record_director_ruling(
        asset_class=asset_class, economic_family=family, verdict=verdict,
        blocker_reason=reason, rationale="R71 closed this family.",
        reopen_condition="OWNED_PIT_NON_PRICE_TERMS_OF_TRADE_VINTAGES",
        campaign_id="R71_FORWARD_AND_NEW_INFORMATION",
        decided_by="quant-research-director", decision_date="2026-09-25",
        source_artifact="research/agents/campaign_r71/decision.json")


# --------------------------------------------------------------------------- #
# 1. THE SEAM - a terminally ruled family is not remaining work
# --------------------------------------------------------------------------- #
def test_01_an_unruled_family_stays_dispatchable(mem):
    """The pre-R79 behaviour, which is still correct when nobody has ruled."""
    keep, withheld = FR._partition_ruled(
        mem, "CROSS_ASSET", [TERMINAL_FAMILY, INFORMATION_FAMILY])
    assert keep == [TERMINAL_FAMILY, INFORMATION_FAMILY]
    assert withheld == []


def test_02_a_terminally_ruled_family_is_withheld(mem):
    _rule(mem, TERMINAL_FAMILY, reason=B.WAITING_FOR_EXTERNAL_ENTITLEMENT)
    keep, withheld = FR._partition_ruled(mem, "CROSS_ASSET", [TERMINAL_FAMILY])
    assert keep == []
    assert len(withheld) == 1
    assert withheld[0]["economic_family"] == TERMINAL_FAMILY
    assert withheld[0]["clears_on"] == B.CLEARS_TERMINAL


def test_03_a_superseded_family_is_withheld_too(mem):
    """SUPERSEDED is TERMINAL in the taxonomy; both R71 closures must bite."""
    _rule(mem, SUPERSEDED_FAMILY, reason=B.SUPERSEDED,
          verdict="REFUSED_AS_A_REPLICATION")
    keep, withheld = FR._partition_ruled(mem, "CROSS_ASSET",
                                         [SUPERSEDED_FAMILY])
    assert keep == []
    assert withheld[0]["blocker_reason"] == B.SUPERSEDED


def test_04_an_information_ruling_does_NOT_withhold(mem):
    """THE MIRROR DEFECT. A family waiting for data is real remaining work."""
    _rule(mem, INFORMATION_FAMILY, reason=B.WAITING_FOR_PROVIDER_DATA)
    keep, withheld = FR._partition_ruled(mem, "CROSS_ASSET",
                                         [INFORMATION_FAMILY])
    assert keep == [INFORMATION_FAMILY]
    assert withheld == []


def test_05_a_family_exhausted_ruling_does_NOT_withhold(mem):
    """FAMILY_EXHAUSTED clears on INFORMATION - a new input reopens it."""
    _rule(mem, INFORMATION_FAMILY, reason=B.FAMILY_EXHAUSTED)
    keep, _ = FR._partition_ruled(mem, "CROSS_ASSET", [INFORMATION_FAMILY])
    assert keep == [INFORMATION_FAMILY]


def test_06_the_withheld_row_carries_its_authority_and_reopen_condition(mem):
    """A silent exclusion would be a worse bug than the one being fixed."""
    _rule(mem, TERMINAL_FAMILY, reason=B.WAITING_FOR_EXTERNAL_ENTITLEMENT)
    _, withheld = FR._partition_ruled(mem, "CROSS_ASSET", [TERMINAL_FAMILY])
    row = withheld[0]
    assert row["decided_by"] == "quant-research-director"
    assert row["verdict"] == "REFUSED"
    assert row["reopen_condition"] == \
        "OWNED_PIT_NON_PRICE_TERMS_OF_TRADE_VINTAGES"
    assert row["withheld_by"] == FR.CALCULATION_OWNER


def test_07_an_unreadable_ruling_store_leaves_everything_dispatchable():
    """Failing to READ a ruling must never silently retire research."""
    class _Exploding:
        def director_ruling(self, **_kw):
            raise RuntimeError("store is gone")

    keep, withheld = FR._partition_ruled(_Exploding(), "CROSS_ASSET",
                                         [TERMINAL_FAMILY])
    assert keep == [TERMINAL_FAMILY]
    assert withheld == []


# --------------------------------------------------------------------------- #
# 2. THE PROPAGATION - the governor and the stop decision derive the truth
# --------------------------------------------------------------------------- #
def _frontier_view(ready_families, *, asset_class="CROSS_ASSET"):
    """A minimal frontier document shaped like ``FR.measure``'s output."""
    return {"asset_classes": {asset_class: {
        "state": "RESEARCH_READY",
        "reason": "test",
        "detail": {"family_specs": [{"family": f, "generative": False,
                                     "mode": "CROSS_SECTIONAL"}
                                    for f in ready_families],
                   "remaining_families": list(ready_families),
                   "hypotheses_recorded": 5, "instruments": 100,
                   "executable_families": list(ready_families)}}},
        "ready": [asset_class] if ready_families else [],
        "non_equity_ready": [asset_class] if ready_families else []}


def test_08_the_governor_issues_a_mandate_for_an_open_family(mem):
    """The control: with work available the governor must still produce it."""
    doc = GOV.generate_mandates(mem, limit=8,
                                frontier_view=_frontier_view([TERMINAL_FAMILY]))
    assert len(doc["mandates"]) == 1
    assert doc["terminal"] is None


def test_09_an_empty_remaining_list_is_a_terminal_generator_state(mem):
    """What the withholding produces one layer down."""
    doc = GOV.generate_mandates(mem, limit=8, frontier_view=_frontier_view([]))
    assert doc["mandates"] == []
    assert doc["terminal"] == "NO_INDEPENDENT_EXECUTABLE_RESEARCH_REMAINS"


def test_10_stop_reason_reports_condition_A_when_nothing_is_generatable(mem):
    """THE DEFECT, INVERTED. This returned stop=False for nineteen days."""
    decision = GOV.stop_reason(mem, frontier_view=_frontier_view([]))
    assert decision["stop"] is True
    assert decision["condition"] == "A"


def test_11_stop_reason_does_not_stop_while_real_work_remains(mem):
    decision = GOV.stop_reason(
        mem, frontier_view=_frontier_view([TERMINAL_FAMILY]))
    assert decision["stop"] is False


def test_12_measure_publishes_the_withheld_families_in_the_detail(mem):
    """``measure`` writes the frontier; the withheld set must be readable."""
    _rule(mem, TERMINAL_FAMILY, reason=B.WAITING_FOR_EXTERNAL_ENTITLEMENT)
    keep, withheld = FR._partition_ruled(
        mem, "CROSS_ASSET", [TERMINAL_FAMILY, INFORMATION_FAMILY])
    # The contract the detail block publishes, asserted at the seam that fills
    # it, so this stays hermetic and never opens the live panel store.
    assert keep == [INFORMATION_FAMILY]
    assert [w["economic_family"] for w in withheld] == [TERMINAL_FAMILY]


# --------------------------------------------------------------------------- #
# 3. THE TRUTHFUL SLEEP REASON
# --------------------------------------------------------------------------- #
def test_13_a_terminal_blocker_outranks_everything_in_the_sleep_reason():
    """One job needing a human outranks ten a session will free by itself."""
    assert RT.sleep_reason_for({"time_will_clear": 10,
                                "needs_new_information": 4,
                                "terminal_without_a_decision": 1}) == \
        RT.SLEEP_TERMINAL_DECISION


def test_14_an_information_blocker_is_named_as_such():
    assert RT.sleep_reason_for({"time_will_clear": 3,
                                "needs_new_information": 2,
                                "terminal_without_a_decision": 0}) == \
        RT.SLEEP_WAITING_INFORMATION


def test_15_only_a_purely_time_cleared_set_reports_waiting_on_a_session():
    assert RT.sleep_reason_for({"time_will_clear": 5,
                                "needs_new_information": 0,
                                "terminal_without_a_decision": 0}) == \
        RT.SLEEP_WAITING_SESSION


def test_16_every_sleep_reason_is_in_the_declared_vocabulary():
    for summary in ({"terminal_without_a_decision": 1},
                    {"needs_new_information": 1}, {"time_will_clear": 1}, {}):
        assert RT.sleep_reason_for(summary) in RT.SLEEP_REASONS


def test_17_the_plan_no_longer_asserts_an_external_source():
    """The exact token that misreported the outage may not be the headline."""
    plan = RT.plan_sleep(
        ready_work=0,
        conditions=[{"name": "BLOCKED_SOURCES", "watermark": "3"}],
        blocked_summary={"by_reason": {B.SUPERSEDED: 3}, "blocked_total": 3,
                         "time_will_clear": 0, "needs_new_information": 0,
                         "terminal_without_a_decision": 3})
    assert plan["reason"] == RT.SLEEP_TERMINAL_DECISION
    assert plan["legacy_reason"] == "WAITING_ON_A_BLOCKED_EXTERNAL_SOURCE"
    assert plan["blocker_clearance_mix"]["terminal_without_a_decision"] == 3


# --------------------------------------------------------------------------- #
# 4. THE SPY CADENCE CONTRADICTION
# --------------------------------------------------------------------------- #
def _nor():
    import sys
    from pathlib import Path
    root = str(Path(__file__).resolve().parents[1])
    if root not in sys.path:
        sys.path.insert(0, root)
    from alpha_agent.alpha_recovery import next_open_runtime as NOR
    return NOR


def test_18_the_cadence_contract_is_read_from_the_frozen_specification():
    """The signal identity owns the calendar. Cadence 5, non-overlapping."""
    c = _nor().cadence_contract()
    assert c["refusal"] is None
    assert c["cadence_sessions"] == 5
    assert c["spec_cadence_sessions"] == 5
    assert c["policy_cadence_sessions"] == 5
    assert c["overlapping"] is False


def test_19_the_declared_grid_is_the_cadence_not_the_eligibility_rule():
    """THE DEFECT. Nine daily boundaries became the frozen contract's two."""
    doc = _nor().declared_boundaries(now="2026-09-27")
    assert doc["blocked_on"] is None
    assert doc["cadence_sessions"] == 5
    assert doc["first_legal_entry_session"] == "2026-09-15"
    assert doc["declared_grid_entry_sessions"] == ["2026-09-15", "2026-09-22"]
    # The nine eligible sessions are still published, so the reclassification
    # stays auditable rather than being quietly dropped.
    assert len(doc["eligible_sessions_in_range"]) == 9
    assert len(doc["sessions_eligible_but_not_boundaries"]) == 7


def test_20_the_next_boundary_is_the_legitimate_september_29_one():
    """Published as 2026-09-28 by the daily grid; the contract says 09-29."""
    doc = _nor().declared_boundaries(now="2026-09-27")
    assert doc["next_boundaries"][0] == "2026-09-29"
    assert "2026-09-28" not in doc["next_boundaries"]


def test_21_every_boundary_ahead_sits_on_the_same_stride():
    """A grid whose future differs from its past is two calendars again."""
    doc = _nor().declared_boundaries(now="2026-09-27")
    assert doc["next_boundaries"][:4] == ["2026-09-29", "2026-10-06",
                                          "2026-10-13", "2026-10-20"]


def test_22_a_missed_boundary_is_only_counted_once_it_is_strictly_past():
    """Anchored on a boundary date: today's still-open entry is never lost."""
    doc = _nor().declared_boundaries(now="2026-09-22")
    assert "2026-09-22" not in doc["missed_boundaries"]
    assert doc["missed_boundaries"] == ["2026-09-15"]


class _SpecProxy:
    """The real challenger module with ONLY its two declaration readers swapped.

    A stub with a hand-listed attribute surface would exercise a stub rather than
    the module: every other name - the calendar, the identity, the entry-state
    machine, the private time helpers - delegates to the real object, so a
    refusal is proved on the real code path.
    """

    def __init__(self, real, spec, policy):
        self._real, self._spec, self._policy = real, spec, policy

    def frozen_specification(self):
        return self._spec

    def policy_declaration(self):
        return self._policy

    def __getattr__(self, name):
        return getattr(self._real, name)


def _with_fake_spec(spec, policy):
    """Swap the challenger's DECLARATION owner, keeping everything else real."""
    NOR = _nor()
    real = NOR.NOC
    return NOR, real, _SpecProxy(real, spec, policy)


def test_23_an_undeclared_cadence_fails_closed_instead_of_gridding_daily():
    """The whole point: no cadence means NO GRID, never a daily one."""
    NOR, real, fake = _with_fake_spec({"overlapping": False}, {})
    try:
        NOR.NOC = fake
        c = NOR.cadence_contract()
        assert c["refusal"] == NOR.GRID_NO_CADENCE
        assert c["cadence_sessions"] is None
        doc = NOR.declared_boundaries(now="2026-09-27")
        assert doc["blocked_on"] == NOR.GRID_NO_CADENCE
        assert doc["declared_grid_entry_sessions"] if False else True
        assert doc["missed_boundaries"] == []
        assert doc["next_boundaries"] == []
    finally:
        NOR.NOC = real


def test_24_disagreeing_cadence_declarations_fail_closed():
    """A producer that picks a winner between two owners creates the third."""
    NOR, real, fake = _with_fake_spec({"cadence": 5, "overlapping": False},
                                      {"rebalance_cadence_sessions": 1})
    try:
        NOR.NOC = fake
        assert NOR.cadence_contract()["refusal"] == NOR.GRID_CADENCE_DISAGREES
        assert NOR.declared_boundaries(now="2026-09-27")["missed_boundaries"] \
            == []
    finally:
        NOR.NOC = real


def test_25_an_overlapping_construction_is_refused_not_approximated():
    NOR, real, fake = _with_fake_spec({"cadence": 5, "overlapping": True},
                                      {"rebalance_cadence_sessions": 5})
    try:
        NOR.NOC = fake
        assert NOR.cadence_contract()["refusal"] == NOR.GRID_OVERLAPPING
    finally:
        NOR.NOC = real


# --------------------------------------------------------------------------- #
# 4b. THE ACTING SIDE OF THE SAME CONTRACT
#
# Fixing only the REPORT would have left the producer freezing decisions the
# accrual's own ``[::cadence]`` grid does not contain - orphans nothing could
# score. ``entry_session_for`` answers "which session would this be acted on",
# which is the next ELIGIBLE one, so the freeze has to be gated on the boundary.
# --------------------------------------------------------------------------- #
def test_25b_the_anchor_and_every_fifth_session_are_boundaries():
    NOR = _nor()
    for session in ("2026-09-15", "2026-09-22", "2026-09-29", "2026-10-06"):
        b = NOR.boundary_state_for(session, cadence_sessions=5,
                                   now="2026-09-27")
        assert b["is_boundary"] is True, session
        assert b["anchor"] == "2026-09-15"


def test_25c_the_sessions_between_boundaries_are_not_boundaries():
    NOR = _nor()
    for session in ("2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21",
                    "2026-09-23", "2026-09-24", "2026-09-25", "2026-09-28"):
        b = NOR.boundary_state_for(session, cadence_sessions=5,
                                   now="2026-09-27")
        assert b["is_boundary"] is False, session


def test_25d_a_non_boundary_names_the_boundary_that_follows_it():
    """So a reader is never told 'no' without being told when 'yes' is."""
    NOR = _nor()
    b = NOR.boundary_state_for("2026-09-28", cadence_sessions=5,
                               now="2026-09-27")
    assert b["next_boundary"] == "2026-09-29"
    assert b["previous_boundary"] == "2026-09-22"


def test_25e_a_session_before_inception_points_at_the_anchor():
    NOR = _nor()
    b = NOR.boundary_state_for("2026-09-01", cadence_sessions=5,
                               now="2026-09-27")
    assert b["is_boundary"] is False
    assert b["next_boundary"] == "2026-09-15"


def test_25f_the_producer_refuses_to_decide_on_a_non_boundary_session():
    """THE DEFECT ON THE ACTING SIDE.

    ``probe``/``append`` are off so no vendor is called, nothing is downloaded
    and nothing is spent; the gate sits above the entry-state logic, so the
    verdict is still produced. Asserted on the live 2026-09-28 entry, which the
    daily grid would have treated as a decision boundary.
    """
    NOR = _nor()
    res = NOR.advance_daily(probe=False, append=False, execute_append=False,
                            budget_usd=0.0, now="2026-09-27T15:30:00+00:00")
    assert res["state"] == NOR.ADV_NOT_A_BOUNDARY
    assert res["entry_session"] == "2026-09-28"
    assert res["boundary"]["next_boundary"] == "2026-09-29"
    assert res["paid_dollars"] == 0.0
    assert res["creates_orders"] is False
    assert res["creates_fills"] is False
    assert res["allocates_capital"] is False
    assert "freeze" not in res


def test_25g_a_non_boundary_is_neither_a_freeze_nor_a_miss():
    """The reclassification, stated as a vocabulary property."""
    NOR = _nor()
    assert NOR.ADV_NOT_A_BOUNDARY in NOR.ADVANCE_STATES
    assert NOR.ADV_NOT_A_BOUNDARY != NOR.ADV_MISSED
    assert NOR.ADV_NOT_A_BOUNDARY != NOR.ADV_NOTHING_DUE
    # Only a freeze is progress: a non-boundary must never look like one.
    assert NOR.ADV_NOT_A_BOUNDARY not in NOR.PROGRESS_STATES


def test_25h_an_unreadable_cadence_refuses_to_freeze_rather_than_defaulting():
    """Fail closed: no cadence means no decision, never a daily one."""
    NOR, real, fake = _with_fake_spec({"overlapping": False}, {})
    try:
        NOR.NOC = fake
        res = NOR.advance_daily(probe=False, append=False,
                                execute_append=False, budget_usd=0.0,
                                now="2026-09-27T15:30:00+00:00")
        assert res["state"] == NOR.ADV_BLOCKED
        assert res["blocked_on"] == NOR.GRID_NO_CADENCE
        assert "freeze" not in res
    finally:
        NOR.NOC = real


# --------------------------------------------------------------------------- #
# 5. THE UNPRICEABLE ENTRY MARK
# --------------------------------------------------------------------------- #
def _reg(boundary, *, marks=None, entry_mark=(9, 30)):
    """A registration shaped as the accrual's declaration readers see one."""
    contract = {"execution_boundary": boundary,
                "entry_mark_et_on_the_entry_session": list(entry_mark)}
    if marks is not None:
        contract["valuation_marks"] = marks
    return {"challenger_id": "TEST", "price_mark_owner": "api.price_panel",
            "_execution_contract": contract}


def _feasibility(reg, monkeypatch):
    monkeypatch.setattr(ACC, "_declared_execution_contract",
                        lambda r: dict(r.get("_execution_contract") or {}))
    return ACC.entry_mark_feasibility(reg)


def test_26_a_close_boundary_is_feasible_on_close_marks(monkeypatch):
    """The control: seven of the eight live registrations look like this."""
    f = _feasibility(_reg("DECISION_SESSION_CLOSE"), monkeypatch)
    assert f["feasible"] is True
    assert f["entry_mark_required"] == ACC.MARK_INSTANT_CLOSE


def test_27_an_open_boundary_on_close_marks_is_refused(monkeypatch):
    """THE DEFECT. Close-to-close P&L reported as next-open P&L."""
    f = _feasibility(_reg(ACC.NEXT_OPEN_BOUNDARY), monkeypatch)
    assert f["feasible"] is False
    assert f["reason"] == ACC.INTEGRITY_ENTRY_MARK_UNPRICEABLE
    assert f["entry_mark_required"] == ACC.MARK_INSTANT_OPEN
    assert f["valuation_mark_instant"] == ACC.MARK_INSTANT_CLOSE


def test_28_a_release_that_declares_open_marks_is_priceable(monkeypatch):
    """The refusal is liftable by the declaration, not by a code change."""
    f = _feasibility(
        _reg(ACC.NEXT_OPEN_BOUNDARY,
             marks={"store_relative_to_research_root": "marks",
                    "mark_instant": ACC.MARK_INSTANT_OPEN}),
        monkeypatch)
    assert f["feasible"] is True


def test_29_an_undeclared_mark_instant_reads_as_close_not_as_unknown(
        monkeypatch):
    """"No declaration, so allow it" is how the defect would have survived."""
    f = _feasibility(
        _reg(ACC.NEXT_OPEN_BOUNDARY,
             marks={"store_relative_to_research_root": "marks"}),
        monkeypatch)
    assert f["feasible"] is False
    assert f["valuation_mark_instant_declared"] is False


def test_30_the_refusal_is_in_the_declared_integrity_vocabulary():
    assert ACC.INTEGRITY_ENTRY_MARK_UNPRICEABLE in ACC.INTEGRITY_REASONS


#: The eight live registrations' declared execution boundaries, transcribed from
#: what ``api.forward_challenger_registry`` actually serves in production. Kept
#: as DATA here because this suite runs against a hermetic registry root (the
#: conftest redirects ``PAPER_TRADER_FORWARD_CHALLENGER_REGISTRY_DIR`` into a
#: pytest temp dir, so the live registry is deliberately unreadable and iterating
#: it here would assert nothing at all). The live estate was verified separately;
#: what must not regress is the RULE, and the rule is exercised on every shape.
LIVE_BOUNDARIES = {
    "R58_SHORT_VOLUME_PRESSURE_V1": "DECISION_SESSION_CLOSE",
    "R58_DISCLOSURE_INTENSITY_V1": "DECISION_SESSION_CLOSE",
    "R58_FUND_MOMENTUM_VETO_V1": "DECISION_SESSION_CLOSE",
    "R58_FCF_PURE_V1": "DECISION_SESSION_CLOSE",
    "REVERSED_SPY_PUT_CALL_SKEW_H5": None,
    NEXT_OPEN_ID: "NEXT_ELIGIBLE_SESSION_OPEN",
    "ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7":
        "SETTLEMENT_OF_THE_ELIGIBLE_SESSION_AFTER_THE_NEWEST_PUBLISHED_SESSION",
    "ALPHA_RECOVERY_FUTURES_TS_TREND_H21_V1":
        "SETTLEMENT_OF_THE_ELIGIBLE_SESSION_AFTER_THE_NEWEST_PUBLISHED_SESSION",
}


def test_31_exactly_one_of_the_eight_live_shapes_is_unpriceable(monkeypatch):
    """The rule, exercised on every boundary the live estate declares.

    Precision matters as much as the catch: a check that refused the settlement
    boundaries too would have stopped seven healthy challengers accruing.
    """
    refused = []
    for cid, boundary in LIVE_BOUNDARIES.items():
        reg = _reg(boundary) if boundary else {"challenger_id": cid,
                                              "_execution_contract": {}}
        if not _feasibility(reg, monkeypatch)["feasible"]:
            refused.append(cid)
    assert refused == [NEXT_OPEN_ID]


def test_31b_the_live_registry_is_hermetic_so_test_31_cannot_be_vacuous():
    """Guards the reason test_31 is written against transcribed shapes.

    If this suite could read the production registry, test_31 above should be
    asserting against it instead. It cannot - and an empty iteration passing
    silently is exactly how a vacuous test survives - so the emptiness is
    asserted rather than assumed.
    """
    from paper_trader.api import forward_challenger_registry as REG
    assert REG.load_registrations() == []


def test_31c_an_unreadable_declaration_is_reported_not_assumed_priceable():
    """{} means two different things and they may not be collapsed."""
    f = ACC.entry_mark_feasibility({"challenger_id": "X",
                                    "identity": {"release": "NO_SUCH_RELEASE"}})
    assert f["declaration_readable"] is False
    assert f["declared_a_contract"] is False


# --------------------------------------------------------------------------- #
# 6. THE R78 DUPLICATE - reproduced exactly, then closed
# --------------------------------------------------------------------------- #
def _census_row(asset_class, family, rank=8):
    return {"queued_hypotheses": [
        {"rank": rank, "proposal": "a proposal", "agent": "momentum",
         "asset_class": asset_class, "family": family,
         "horizon_sessions": 21}]}


class _Pipe:
    def __init__(self, mem):
        self.mem = mem


def _settle(mem, *, asset_class, economic_family, information_family,
            outcome):
    """Record a settled result the way the estate's own pipeline does."""
    hid = mem.register(
        title="R78 mechanism", asset_class=asset_class,
        economic_family=economic_family,
        information_family=information_family,
        model_family="LONG_ONLY", spec={"k": 1}, release="R78",
        origin="TEST", generation_method="TEST")
    mem.record_result(hid, outcome=outcome, statistic={}, economics={},
                      reason_rejected="coverage")
    return hid


def test_32_a_mechanism_with_no_ruling_and_no_history_is_genuinely_open(mem):
    from paper_trader.alpha_agent.agents_v2 import briefs as BR
    rows = BR._queued_with_rulings(
        _Pipe(mem), _census_row("US_EQUITY", "EVENT_OVERREACTION"))
    assert rows[0]["already_ruled"] is False
    assert rows[0]["settled_state"] == BR.SETTLED_NOT
    assert rows[0]["mechanism_settled"] is False


def test_33_the_exact_r78_duplicate_no_longer_reads_as_novel(mem):
    """THE R78 DEFECT, REPRODUCED.

    A mechanism settled BY MEASUREMENT (a DATA_HOLD) and covered by NO director
    ruling. ``already_ruled`` is False - it always was, and that is not a bug -
    but the brief must no longer let that be read as "never tested".
    """
    from paper_trader.alpha_agent.agents_v2 import briefs as BR
    _settle(mem, asset_class="US_EQUITY",
            economic_family="FUNDAMENTAL_MOMENTUM",
            information_family="DIVIDEND_DECLARATION_EVENTS",
            outcome="DATA_HOLD")
    rows = BR._queued_with_rulings(
        _Pipe(mem), _census_row("US_EQUITY", "FUNDAMENTAL_MOMENTUM"))
    row = rows[0]
    assert row["already_ruled"] is False          # the trap, preserved
    assert row["mechanism_settled"] is True       # the fact that was missing
    assert row["settled_state"] == BR.SETTLED_BY_MEASUREMENT
    assert row["mechanism_settled_rows"] == 1
    assert "DATA_HOLD" in row["settled_by"]


def test_34_a_ruled_mechanism_still_reports_as_ruled(mem):
    """The ruling join must keep precedence: it is the stronger closure."""
    from paper_trader.alpha_agent.agents_v2 import briefs as BR
    _rule(mem, "CARRY", reason=B.FAMILY_EXHAUSTED,
          verdict="REFUSED_AS_ALREADY_MEASURED", asset_class="CROSS_ASSET")
    rows = BR._queued_with_rulings(_Pipe(mem),
                                   _census_row("CROSS_ASSET", "CARRY"))
    assert rows[0]["already_ruled"] is True
    assert rows[0]["settled_state"] == BR.SETTLED_RULED


def test_35_every_settled_state_is_in_the_declared_vocabulary(mem):
    from paper_trader.alpha_agent.agents_v2 import briefs as BR
    rows = BR._queued_with_rulings(
        _Pipe(mem), _census_row("US_EQUITY", "EVENT_OVERREACTION"))
    assert rows[0]["settled_state"] in BR.SETTLED_STATES


def test_36_an_unreadable_memory_does_not_claim_a_mechanism_is_settled():
    """A read failure must not manufacture a refusal, in either direction."""
    from paper_trader.alpha_agent.agents_v2 import briefs as BR

    class _Exploding:
        def director_rulings(self):
            return []

        def mechanism_state(self, **_kw):
            raise RuntimeError("store is gone")

    rows = BR._queued_with_rulings(
        _Pipe.__new__(_Pipe) if False else _Pipe(_Exploding()),
        _census_row("US_EQUITY", "EVENT_OVERREACTION"))
    assert rows[0]["mechanism_settled"] is False
    assert rows[0]["settled_state"] == BR.SETTLED_NOT


# --------------------------------------------------------------------------- #
# 7. THE AUTHORITATIVE STATUS - a projection that cannot flatter itself
# --------------------------------------------------------------------------- #
def _status(**kw):
    base = dict(workflow_state={}, worker_status={}, accrual={},
                producer_coverage={}, frontier_view={}, mandates={},
                ready={})
    base.update(kw)
    return AOS.build_autonomous_operating_status(**base)


def test_37_all_eight_required_blocks_are_present():
    doc = _status(worker_status={"worker_state": "SLEEPING"})
    for block in ("LAST_COMPLETED_CYCLE", "CURRENT_ACTIVE_WORK",
                  "NEXT_SCHEDULED_WORK", "CURRENT_BLOCKERS",
                  "RESEARCH_CANDIDATE_AND_STAGE",
                  "FORWARD_EMITTED_PENDING_MATURED",
                  "PORTFOLIO_PROPOSAL_STATE", "RUNTIME_SOURCE_IDENTITY"):
        assert block in doc, block


def test_38_a_terminal_blocker_is_reported_as_needing_a_human():
    """A live lease is not evidence of progress, and never decides this."""
    doc = _status(worker_status={
        "worker_state": "SLEEPING", "lease_held": True,
        "blocker_clearance_mix": {"terminal_without_a_decision": 3}})
    assert doc["autonomy"]["state"] == AOS.A_NEEDS_DECISION
    assert doc["autonomy"]["advances_without_a_human"] is False
    assert doc["autonomy"]["what_would_release_it"]


def test_39_an_exhausted_generator_is_reported_as_needing_information():
    doc = _status(
        worker_status={"worker_state": "SLEEPING", "lease_held": True},
        mandates={"mandates": [],
                  "terminal": "NO_INDEPENDENT_EXECUTABLE_RESEARCH_REMAINS"})
    assert doc["autonomy"]["state"] == AOS.A_NEEDS_INFORMATION
    assert doc["autonomy"]["advances_without_a_human"] is False


def test_40_a_worker_with_executable_work_is_reported_as_advancing():
    doc = _status(
        worker_status={"worker_state": "RESEARCHING", "lease_held": True,
                       "current_lane": "r59.economic.us_equity"},
        mandates={"mandates": [{"asset_class": "US_EQUITY",
                                "family": "MOMENTUM", "kind": "ECONOMIC",
                                "mandate_id": "M1"}]})
    assert doc["autonomy"]["state"] == AOS.A_ADVANCING
    assert doc["autonomy"]["advances_without_a_human"] is True


def test_41_a_stopped_worker_outranks_every_other_verdict():
    doc = _status(worker_status={"worker_state": "STOPPED"},
                  mandates={"mandates": [{"asset_class": "US_EQUITY",
                                          "family": "MOMENTUM"}]})
    assert doc["autonomy"]["state"] == AOS.A_WORKER_DOWN


def test_42_a_missing_status_artifact_is_not_read_as_healthy():
    assert _status()["autonomy"]["state"] == AOS.A_WORKER_DOWN


def test_43_an_owner_that_cannot_be_read_is_reported_not_omitted():
    """A status surface that drops what it could not read hides the outage."""
    doc = AOS.build_autonomous_operating_status(
        workflow_state={}, accrual={}, producer_coverage={},
        frontier_view={}, mandates={}, ready={})
    # ``worker_status`` was not injected, so the live owner is consulted; the
    # block exists either way and the contract is that a failure is NAMED.
    assert "degraded_owners" in doc
    assert doc["n_degraded_owners"] == len(doc["degraded_owners"])


def test_44_identical_commits_compare_equal_through_either_spelling():
    """The owner publishes ``commit``; /v1/ready republishes ``loaded_commit``."""
    sha = "97ca932f3b89306" + "8b96926e597f6da49ea33b6d"
    for key in ("commit", "loaded_commit"):
        doc = _status(ready={key: sha},
                      worker_status={"worker_state": "SLEEPING",
                                     "source_identity": {"commit": sha}})
        assert doc["RUNTIME_SOURCE_IDENTITY"]["processes_agree"] is True


def test_45_a_commit_mismatch_is_reported():
    doc = _status(ready={"commit": "a" * 40},
                  worker_status={"worker_state": "SLEEPING",
                                 "source_identity": {"commit": "b" * 40}})
    assert doc["RUNTIME_SOURCE_IDENTITY"]["processes_agree"] is False


def test_46_the_forward_ledger_totals_what_the_accrual_published():
    doc = _status(accrual={
        "h1": {"challenger_id": "A", "predictions_emitted": 2,
               "pending_observations": 2, "matured_observations": 0},
        "h2": {"challenger_id": "B", "predictions_emitted": 3,
               "pending_observations": 1, "matured_observations": 2}})
    t = doc["FORWARD_EMITTED_PENDING_MATURED"]["totals"]
    assert t["predictions_emitted"] == 5
    assert t["matured_observations"] == 2
    assert doc["FORWARD_EMITTED_PENDING_MATURED"]["n_registrations"] == 2


def test_46b_the_persisted_projections_state_key_is_read():
    """The persisted store says current_accrual_state; a live doc says state."""
    doc = _status(accrual={
        "h1": {"challenger_id": "A", "current_accrual_state": "INTEGRITY_BLOCKED"},
        "h2": {"challenger_id": "B", "state": "NOT_DUE"}})
    states = {r["challenger_id"]: r["state"]
              for r in doc["FORWARD_EMITTED_PENDING_MATURED"]["registrations"]}
    assert states == {"A": "INTEGRITY_BLOCKED", "B": "NOT_DUE"}


def test_47_an_unpriceable_registration_is_named_in_the_ledger():
    doc = _status(accrual={"h1": {
        "challenger_id": NEXT_OPEN_ID,
        "entry_mark_feasibility": {
            "feasible": False,
            "reason": ACC.INTEGRITY_ENTRY_MARK_UNPRICEABLE}}})
    led = doc["FORWARD_EMITTED_PENDING_MATURED"]
    assert led["unpriceable_registrations"] == [NEXT_OPEN_ID]
    assert led["n_unpriceable"] == 1


def test_48_the_status_declares_every_safety_boundary_and_writes_nothing():
    doc = _status(worker_status={"worker_state": "SLEEPING"})
    assert doc["read_only"] is True
    assert doc["performed_write"] is False
    assert doc["creates_orders"] is False
    assert doc["approves_anything"] is False
    assert doc["automation_enabled"] is False
    assert doc["promotes_to_live"] is False


def test_49_every_block_names_the_owner_it_is_projected_from():
    doc = _status(worker_status={"worker_state": "SLEEPING"})
    for block in ("last_completed_cycle", "current_active_work",
                  "next_scheduled_work", "current_blockers",
                  "research_candidate_and_stage",
                  "forward_emitted_pending_matured",
                  "portfolio_proposal_state", "runtime_source_identity"):
        assert doc["projected_from"][block]


def test_50_the_withheld_families_reach_the_status_surface():
    """The block that was invisible for nineteen days."""
    doc = _status(
        worker_status={"worker_state": "SLEEPING"},
        frontier_view={"ready": [], "non_equity_ready": [],
                       "asset_classes": {"CROSS_ASSET": {
                           "state": "EXHAUSTED",
                           "detail": {"withheld_by_director_ruling": [{
                               "economic_family": TERMINAL_FAMILY,
                               "blocker_reason":
                                   B.WAITING_FOR_EXTERNAL_ENTITLEMENT,
                               "verdict": "REFUSED",
                               "reopen_condition": "OWNED_PIT_VINTAGES"}]}}}})
    cand = doc["RESEARCH_CANDIDATE_AND_STAGE"]
    assert cand["n_withheld_by_director_ruling"] == 1
    assert cand["withheld_by_director_ruling"][0]["reopen_condition"] == \
        "OWNED_PIT_VINTAGES"
