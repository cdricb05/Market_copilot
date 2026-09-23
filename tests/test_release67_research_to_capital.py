"""Release 67 - the research-to-capital projection.

These tests defend four things R67 established, in the order they matter:

  1. The FORWARD PRODUCER reconciliation. Four registered challengers can never
     reach the capital gate because nothing re-scores them. The arithmetic and
     the producer map are both pinned, and the map is asserted against the
     runtime SOURCE so a renamed stage fails the build instead of silently
     turning a live producer into a phantom one.

  2. The CAPITAL FEASIBILITY provenance rule. An authorised policy is evaluated
     BEFORE an exchange constraint and both are evaluated before any convention.
     R66's own eleven structures are the fixture: every one is refused by the
     long-only policy, including the one R66 reported as nearly holdable.

  3. The STRATEGY INVENTORY's anti-resurrection screen, including the
     VALIDATION_HOLE case, which is the shape most likely to be argued back to
     life and which the estate's single strongest record actually has.

  4. That the whole package stays READ-ONLY.

Network-free and provider-free. They read the live owners, so they assert
INVARIANTS and RELATIONSHIPS rather than frozen numbers that a new session
would break - except where a number IS the finding, which is stated at the
assertion.
"""
from __future__ import annotations

import inspect

import pytest

from paper_trader.alpha_agent import r67
from paper_trader.alpha_agent.r67 import capital_feasibility as CF
from paper_trader.alpha_agent.r67 import cycle as CY
from paper_trader.alpha_agent.r67 import forward_producer as FP
from paper_trader.alpha_agent.r67 import strategy_inventory as SI


# --------------------------------------------------------------------------- #
# 1. Forward producer
# --------------------------------------------------------------------------- #
def test_01_every_declared_producer_stage_exists_in_the_runtime():
    """A stage named as a live producer must actually be in the runtime.

    This is the test that stops the reconciliation rotting. If someone renames
    ``fx_carry_cadence_prospective_decision``, the map would keep reporting that
    challenger as ACCRUING while nothing produced it - which is precisely the
    failure the whole module exists to detect, committed by the detector.
    """
    from paper_trader.alpha_agent.r52 import runtime as RT
    src = inspect.getsource(RT.research_runtime_cycle)
    for stage in FP.RUNTIME_PRODUCER_STAGES:
        assert stage in src, (
            "%s is declared a live cadence producer but no such stage exists in "
            "alpha_agent.r52.runtime.research_runtime_cycle" % stage)


def test_02_the_r58_freeze_producer_exists_and_is_uncalled():
    """The finding in one assertion: the re-scorer is real, and orphaned.

    ``freeze`` existing is what makes this a WIRING defect rather than a missing
    capability, and that distinction is the difference between a day of work and
    a release.
    """
    from paper_trader.alpha_agent.r58 import challengers as C58
    assert callable(C58.freeze)
    assert C58.CADENCE == 21, (
        "the R58 challengers declare a 21-session cadence; the orphan finding's "
        "arithmetic depends on it")
    # No challenger may be BOTH declared unproduced and declared produced. R68
    # moved the four R58 ids from the first list to the second, and this asserts
    # the move was complete rather than additive.
    produced = {c for ids in FP.RUNTIME_PRODUCER_STAGES.values() for c in ids}
    for cid in FP.KNOWN_UNPRODUCED:
        assert cid not in produced, (
            "%s cannot be both orphaned and produced" % cid)
    assert "R58_FCF_PURE_V1" in produced, (
        "R68 wired the R58 cadence producer; this is the assertion that fails "
        "if it is ever unwired")


def test_03_an_orphan_has_no_floor_and_says_why():
    """The ORPHAN ARITHMETIC, on a challenger that is one.

    R68 NOTE. This test was written against ``R58_FCF_PURE_V1``, which was an
    orphan when R67 measured it and is not one now: R68 wired
    ``alpha_agent.r68.r58_cadence_runtime`` as its producer. Re-pointing the
    test at a name that is still unproduced keeps the ARITHMETIC under test -
    which is what this test is for - while
    ``tests/test_release68_forward_evidence_repair.py`` asserts the four R58
    challengers now reach REACHABLE_ON_CADENCE.

    The distinction matters: left as it was, this test would have gone green
    again the day somebody unwired them, which is the defect it exists to
    prevent.
    """
    acc = {"challenger_id": "A_CHALLENGER_WITH_NO_DECLARED_PRODUCER",
           "cadence_sessions": 21, "horizon_sessions": 21,
           "matured_observations": 0, "predictions_emitted": 1}
    r = FP.time_to_capital_floor(acc)
    assert r["producer_state"] != FP.P_ACCRUING
    assert r["floor_reachable"] is False
    assert r["sessions_to_floor"] is None
    assert r["floor_verdict"] == "UNREACHABLE_NO_CADENCE_PRODUCER"
    assert "forfeiture" in r["floor_explanation"].lower(), (
        "the explanation must say this is NOT a forfeiture - R66 settled that "
        "contract and a reader must not be left to guess")


def test_03b_the_four_r58_challengers_are_no_longer_orphans():
    """The SUCCESSFUL state, tested where the defect used to be asserted.

    R67's own finding was that these four could never reach the capital floor at
    any date. R68 repaired it, and this is the assertion that now fails if the
    repair is ever undone.
    """
    for cid in ("R58_SHORT_VOLUME_PRESSURE_V1", "R58_DISCLOSURE_INTENSITY_V1",
                "R58_FUND_MOMENTUM_VETO_V1", "R58_FCF_PURE_V1"):
        r = FP.time_to_capital_floor(
            {"challenger_id": cid, "cadence_sessions": 21,
             "horizon_sessions": 21, "matured_observations": 0})
        assert r["producer_state"] == FP.P_ACCRUING, (cid, r)
        assert r["floor_verdict"] == "REACHABLE_ON_CADENCE", (cid, r)
        assert r["sessions_to_floor"] == 60 * 21 + 21


def test_04_the_floor_arithmetic_is_cadence_times_observations_plus_horizon():
    """60 observations at one per cadence, plus one horizon to mature the last."""
    acc = {"challenger_id": "ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7",
           "cadence_sessions": 5, "horizon_sessions": 5,
           "matured_observations": 0, "predictions_emitted": 1}
    r = FP.time_to_capital_floor(acc)
    assert r["producer_state"] == FP.P_ACCRUING
    assert r["floor_reachable"] is True
    assert r["sessions_to_floor"] == r["min_raw_matured_required"] * 5 + 5
    assert r["sessions_to_floor"] == 305, (
        "this number IS the finding: the soonest ANY sleeve can be funded")


def test_05_the_gate_floor_is_the_gates_own_and_is_never_redeclared():
    """R67 owns no threshold. A local copy would drift from the real gate."""
    from paper_trader.api import capital_eligibility_gate as G
    src = inspect.getsource(FP)
    assert "gate_thresholds" in src
    assert G.gate_thresholds(5)["min_raw_matured"] == \
        FP._gate_floor(5)["min_raw_matured"]


def test_06_reconcile_partitions_the_book_without_losing_anyone():
    r = FP.reconcile()
    assert r["n_registered"] == len(r["rows"])
    assert (r["n_with_live_producer"] + r["n_without_producer"]
            + r["n_producer_undetermined"]) == r["n_registered"]
    assert r["n_orphaned_defect"] + r["n_without_producer_by_design"] == \
        r["n_without_producer"]
    assert r["n_producer_undetermined"] == 0, (
        "a registered challenger outside the declared producer map cannot have "
        "its floor stated; extend the map")


def test_07_a_by_design_absence_is_not_counted_as_a_defect(monkeypatch):
    """An absence without a producer is not automatically a defect.

    The partition this tests is R67's central point and it is unchanged: a
    DELIBERATELY superseded record and a genuine orphan are different things and
    reporting them as one number hides the orphan.

    R68 NOTE. The orphan in this fixture was ``R58_FCF_PURE_V1`` and is now a
    name with no declared producer at all, because the R58 four have one. What
    is under test is the PARTITION, not which challengers happened to fail in
    September.

    Both the projection AND the declaration are injected, because after R68 the
    estate contains no orphan to point at - and a partition test that needs one
    to exist would become untestable exactly when the system is healthy.
    """
    monkeypatch.setitem(FP.KNOWN_UNPRODUCED, "AN_ORPHAN_FOR_THIS_TEST",
                        "A_PRODUCER_WAS_INTENDED_AND_NEVER_WIRED")
    monkeypatch.setitem(FP.UNPRODUCED_IS_A_DEFECT,
                        "A_PRODUCER_WAS_INTENDED_AND_NEVER_WIRED", True)
    proj = {
        "h1": {"challenger_id": "AN_ORPHAN_FOR_THIS_TEST",
               "cadence_sessions": 21, "horizon_sessions": 21,
               "matured_observations": 0},
        "h2": {"challenger_id": "REVERSED_SPY_PUT_CALL_SKEW_H5",
               "cadence_sessions": 5, "horizon_sessions": 5,
               "matured_observations": 0},
        "h3": {"challenger_id": "ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7",
               "cadence_sessions": 5, "horizon_sessions": 5,
               "matured_observations": 0},
        "h4": {"challenger_id": "R58_FCF_PURE_V1", "cadence_sessions": 21,
               "horizon_sessions": 21, "matured_observations": 0},
    }
    r = FP.reconcile(accrual_by_identity=proj)
    assert r["n_without_producer"] == 2
    assert r["n_orphaned_defect"] == 1
    assert r["n_without_producer_by_design"] == 1
    assert r["orphaned_challenger_ids"] == ["AN_ORPHAN_FOR_THIS_TEST"]
    assert "REVERSED_SPY_PUT_CALL_SKEW_H5" not in r["orphaned_challenger_ids"]
    # And the repaired one is counted with the live producers, not the absences.
    assert r["n_with_live_producer"] == 2


def test_07b_a_racy_read_is_reported_not_believed(monkeypatch):
    """An empty projection during a worker write must NOT read as an empty book.

    MEASURED: a test run landed mid-write and reconciled 0 registrations while
    8 were on disk. Reporting that as "nothing is registered" would be a false
    statement about the forward book manufactured by a file-system race.
    """
    from paper_trader.api import canonical_forward_accrual as CFA
    monkeypatch.setattr(CFA, "load_accrual_projection", lambda **kw: {})
    monkeypatch.setattr(CFA, "load_accrual_projection_artifact",
                        lambda **kw: {"n_registered": 8})
    r = FP.reconcile()
    assert r["projection_is_trustworthy"] is False
    assert r["projection_read_problem"]
    assert "COULD NOT BE READ RELIABLY" in r["headline"]
    assert r["projection_n_registered_expected"] == 8


def test_07c_an_absent_artifact_is_an_empty_book_not_a_failed_read():
    """The distinction the guard exists to preserve, asserted in both directions.

    ``tests/conftest.py`` deliberately redirects the accrual store to an empty
    temp root so no test can write a real emission into the live store, so under
    pytest the forward book IS legitimately empty. That must read as an empty
    book - NOT as a failed read - while an artifact that is present and yields
    nothing must read as a failed read. Same zero, opposite meanings.
    """
    r = FP.reconcile()
    if not FP._projection_file_exists():
        assert r["n_registered"] == 0
        assert r["projection_is_trustworthy"] is True, (
            "a MISSING artifact legitimately means no accrual run has happened")
        assert r["projection_read_problem"] is None
    elif r["projection_is_trustworthy"]:
        assert r["projection_read_problem"] is None
        assert r["n_registered"] == r["projection_n_registered_expected"]
    else:                      # the live worker was mid-write; that is the point
        assert "COULD NOT BE READ RELIABLY" in r["headline"]


def test_07d_a_present_artifact_that_reads_as_nothing_is_a_failed_read(monkeypatch):
    """The case that slipped through the first version of the guard.

    A mid-write read can fail BOTH the identity map and the n_registered
    roll-up, leaving expected=None and len=0 - two numbers that AGREE, and which
    the first cross-check therefore passed as a confidently empty book.
    """
    from paper_trader.api import canonical_forward_accrual as CFA
    monkeypatch.setattr(CFA, "load_accrual_projection", lambda **kw: {})
    monkeypatch.setattr(CFA, "load_accrual_projection_artifact", lambda **kw: {})
    monkeypatch.setattr(FP, "_projection_file_exists", lambda: True)
    r = FP.reconcile()
    assert r["projection_is_trustworthy"] is False
    assert "FAILED READ" in r["projection_read_problem"]


# --------------------------------------------------------------------------- #
# 2. Capital feasibility provenance
# --------------------------------------------------------------------------- #
def test_08_only_policy_and_exchange_bind():
    assert set(CF.BINDING_OWNERS) == {CF.OWNER_POLICY, CF.OWNER_EXCHANGE}
    for name, c in CF.CONSTRAINT_PROVENANCE.items():
        assert c["owner"] in CF.CONSTRAINT_OWNERS
        assert c["binds"] is (c["owner"] in CF.BINDING_OWNERS), (
            "%s claims binds=%s under owner %s" % (name, c["binds"], c["owner"]))


def test_09_the_r66_gross_notional_threshold_is_recorded_as_a_convention():
    """R66 reported it as structural. It is a number R66 declared for itself."""
    c = CF.CONSTRAINT_PROVENANCE["MAX_GROSS_NOTIONAL_OVER_NAV"]
    assert c["owner"] == CF.OWNER_R66
    assert c["binds"] is False


def test_10_the_long_only_policy_is_the_one_r66_never_checked():
    c = CF.CONSTRAINT_PROVENANCE["LONG_ONLY_BOOK"]
    assert c["owner"] == CF.OWNER_POLICY
    assert c["binds"] is True
    assert c["r66_checked_it"] is False


def test_11_authorised_policy_is_read_from_its_owners_not_declared():
    pol = CF.authorised_policy()
    assert pol["long_only"] is True
    assert pol["short_exposure_supported"] is False
    assert pol["short_leg_rule"] == "SHORT_LEG_NOT_EXPRESSIBLE_LONG_ONLY_BOOK"
    assert pol["nav_usd"] and pol["nav_usd"] > 0


def test_12_a_short_leg_is_refused_before_notional_is_consulted():
    """Order of evaluation is the correction. A structure the book may not hold
    must not receive a notional verdict that invites a cash-policy remedy."""
    pol = {"short_exposure_supported": False, "long_only": True,
           "short_leg_rule": "SHORT_LEG_NOT_EXPRESSIBLE_LONG_ONLY_BOOK",
           "nav_usd": 99127.48, "free_cash_usd": 4482.71}
    r = CF.assess_structure(
        legs=[{"symbol": "ZC", "side": "LONG", "notional_usd": 1.0,
               "initial_margin_usd": 1.0},
              {"symbol": "ZW", "side": "SHORT", "notional_usd": 1.0,
               "initial_margin_usd": 1.0}],
        policy=pol, label="FEED_ZC_ZW")
    assert r["verdict"] == CF.V_NOT_EXPRESSIBLE
    assert r["binding_constraint_owner"] == CF.OWNER_POLICY
    assert "cash policy" in r["what_would_change_it"].lower()


def test_13_tiny_margin_does_not_rescue_a_short_leg():
    """R66's single survivor missed by $82 of margin and was reported as
    holdable at a 10% cash policy. No cash policy makes a short leg
    expressible, and that is the substantive correction."""
    pol = {"short_exposure_supported": False, "long_only": True,
           "short_leg_rule": "SHORT_LEG_NOT_EXPRESSIBLE_LONG_ONLY_BOOK",
           "nav_usd": 99127.48, "free_cash_usd": 99000.0}
    r = CF.assess_structure(
        legs=[{"symbol": "ZC", "side": "LONG", "notional_usd": 100.0,
               "initial_margin_usd": 1.0},
              {"symbol": "ZW", "side": "SHORT", "notional_usd": 100.0,
               "initial_margin_usd": 1.0}],
        policy=pol)
    assert r["verdict"] == CF.V_NOT_EXPRESSIBLE


def test_14_a_long_only_structure_reaches_the_exchange_tests():
    pol = {"short_exposure_supported": False, "long_only": True,
           "short_leg_rule": "X", "nav_usd": 100000.0, "free_cash_usd": 5000.0}
    ok = CF.assess_structure(
        legs=[{"symbol": "ZC", "side": "LONG", "notional_usd": 50000.0,
               "initial_margin_usd": 2000.0}], policy=pol)
    assert ok["verdict"] == CF.V_IMPLEMENTABLE
    assert "separate gate" in ok["eligibility_note"] or \
        "capital_eligibility_gate" in ok["eligibility_note"], (
        "holdable must never be mistaken for fundable")

    broke = CF.assess_structure(
        legs=[{"symbol": "ZC", "side": "LONG", "notional_usd": 50000.0,
               "initial_margin_usd": 9000.0}], policy=pol)
    assert broke["verdict"] == CF.V_MARGIN_SHORT
    assert broke["binding_constraint_owner"] == CF.OWNER_EXCHANGE


def test_15_sensitivity_is_never_a_verdict():
    s = CF.sensitivity({"gross_notional_over_nav": 42.4,
                        "initial_margin_usd": 1000.0, "free_cash_usd": 4482.71})
    assert s["is_a_verdict"] is False
    assert s["adopted"] is False
    assert s["requires_human_approval_to_adopt"] is True


def test_16_a_volatility_hedge_is_not_called_a_dv01_hedge():
    c = CF.CONSTRAINT_PROVENANCE["VOL_MATCHED_HEDGE_RATIO_AS_DV01_PROXY"]
    assert c["owner"] == CF.OWNER_RESEARCH and c["binds"] is False
    assert "not a DV01 hedge" in c["note"]


# --------------------------------------------------------------------------- #
# 3. Strategy inventory
# --------------------------------------------------------------------------- #
def test_17_validation_hole_is_distinguished_from_a_lockbox_only_artifact():
    """Strong D, flat V, strong L is its OWN verdict.

    Two good layers out of three reads as reassuring and is not: validation is
    the designated out-of-sample window and skipping it is the failure.
    """
    layer = {"discovery": {"t_net_excess": 3.78},
             "validation": {"t_net_excess": 0.09},
             "lockbox": {"t_net_excess": 4.74}}
    r = SI._resurrection_risk(layer, 4.74)
    assert r["resurrection_risk"] == SI.RR_VALIDATION_HOLE
    assert r["may_be_cited_as_prior_plausibility"] is False

    lockbox_only = {"discovery": {"t_net_excess": -0.04},
                    "validation": {"t_net_excess": 0.31},
                    "lockbox": {"t_net_excess": 2.89}}
    r2 = SI._resurrection_risk(lockbox_only, 2.89)
    assert r2["resurrection_risk"] == SI.RR_SUSPECT
    assert r2["may_be_cited_as_prior_plausibility"] is False


def test_18_a_weak_lockbox_is_not_flagged_at_all():
    layer = {"discovery": {"t_net_excess": 0.2},
             "validation": {"t_net_excess": 0.1},
             "lockbox": {"t_net_excess": 1.1}}
    assert SI._resurrection_risk(layer, 1.1)["resurrection_risk"] == SI.RR_NONE


def test_19_counts_describe_the_estate_not_the_page():
    """A header that shrinks with a display limit misreports the estate."""
    full = SI.build()
    page = SI.build(limit=5)
    assert page["n_mechanisms"] == full["n_mechanisms"]
    assert page["n_mechanisms_flagged_regime_artifact"] == \
        full["n_mechanisms_flagged_regime_artifact"]
    assert page["rows_returned"] == 5
    assert page["rows_truncated_by_limit"] is True
    assert full["rows_truncated_by_limit"] is False


def test_20_the_estate_has_never_had_a_qualified_survivor():
    """If this ever fails, it is the best news in the project's history and the
    inventory's every 'next action' must be revisited."""
    inv = SI.build(limit=1)
    assert inv["estate"]["qualified_survivors"] == 0
    assert "QUALIFIED" not in (inv["estate"]["by_outcome"] or {})


def test_21_deduplication_is_by_mechanism_not_by_experiment_name():
    inv = SI.build(limit=None)
    assert inv["deduplication_unit"].startswith("ECONOMIC_MECHANISM")
    assert inv["n_mechanisms"] < inv["estate"]["hypotheses_settled"], (
        "8k experiment names must collapse to a far smaller set of mechanisms "
        "or the inventory is not an inventory")
    assert sum(r["n_experiments"] for r in inv["rows"]) == \
        inv["estate"]["hypotheses_settled"]


def test_22_every_mechanism_gets_exactly_one_next_action():
    inv = SI.build(limit=None)
    for r in inv["rows"]:
        assert r["next_action"] in SI.NEXT_ACTIONS
        assert r["next_action_detail"]


def test_23_an_orphaned_forward_registration_drives_the_next_action():
    r = SI._next_action(
        outcomes={"FORWARD_FROZEN": 1}, forward_ids=["R58_FCF_PURE_V1"],
        producer_rows={"R58_FCF_PURE_V1": {
            "producer_state": "NO_CADENCE_PRODUCER", "is_a_defect": True}},
        frontier_state="EXHAUSTED")
    assert r["next_action"] == SI.NA_WIRE_PRODUCER


# --------------------------------------------------------------------------- #
# 4. The pass, and read-only safety
# --------------------------------------------------------------------------- #
def test_24_the_cycle_owns_no_clock_and_starts_no_worker():
    b = CY.run(execute=False, inventory_limit=3)
    assert b["owns_no_clock"] is True
    assert b["starts_no_worker"] is True
    assert b["serves_operating_cycle"] == "PORTFOLIO_REASSESSMENT"
    assert "MODEL_RECALIBRATION" in b["does_not_serve"], (
        "a research pass having run is never a reason to recalibrate")


def test_25_the_cycle_completes_and_carries_all_three_projections():
    b = CY.run(execute=False, inventory_limit=3)
    assert b["state"] in CY.CYCLE_STATES
    assert b["problems"] == [], b["problems"]
    assert b["forward_producer"] and b["strategy_inventory"] and b["authorised_policy"]
    assert b["headline"]


def test_26_execute_false_writes_nothing(tmp_path):
    b = CY.run(root=tmp_path, execute=False, inventory_limit=3)
    assert "artifact_path" not in b
    assert not list(tmp_path.iterdir())


def test_27_execute_true_writes_exactly_one_artifact(tmp_path):
    b = CY.run(root=tmp_path, execute=True, inventory_limit=3)
    written = list(tmp_path.iterdir())
    assert len(written) == 1 and written[0].name == CY.ARTIFACT_NAME
    assert b["artifact_path"] == str(written[0])


def test_28_the_package_declares_itself_read_only():
    for k in ("creates_orders", "creates_fills", "mutates_holdings",
              "mutates_operational_store", "promotes_model", "activates_sleeve",
              "charges_search_burden", "emits_forward_predictions"):
        assert r67.SAFETY[k] is False, k
    for mod in (FP, SI, CF, CY):
        src = inspect.getsource(mod)
        assert "open_memory()" not in src, (
            "%s must use the READ-ONLY research memory handle" % mod.__name__)
        assert "execute=True" not in src or mod is CY, (
            "%s must not advance a writing owner" % mod.__name__)


@pytest.mark.parametrize("mod", [FP, SI, CF])
def test_29_no_projection_emits_a_prediction_or_registers_anything(mod):
    src = inspect.getsource(mod)
    for forbidden in ("emit_prospective_prediction", "record_forfeiture",
                      "register_forward_challenger", "persist_accrual_projection",
                      "set_frontier", "record_result", "register("):
        assert forbidden not in src, (
            "%s calls %s - the projections write nothing"
            % (mod.__name__, forbidden))
